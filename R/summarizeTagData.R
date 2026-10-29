#######################################################################################################
# Summarise processed tag deployments #################################################################
#######################################################################################################

#' Summarise processed tag deployments
#'
#' @description
#' Builds a deployment-level summary table from processed archival tag data, with one row per
#' deployment: how long the tag recorded and at what rate, and the headline figures for depth,
#' temperature, activity, swimming and diving.
#'
#' Descriptive fields are taken from each tag's own metadata and need not be supplied again. External
#' per-animal covariates the tag never recorded, such as sex or size, are attached through
#' `extra.metadata`; video coverage, dive annotations and the deployment-exclusion log are folded in
#' from their own arguments where available.
#'
#' The result is a typed data frame meant for analysis - numeric columns stay numeric and datetimes stay
#' `POSIXct`. Use [format.nautilus_summary()] for a display-ready version to inspect or export.
#'
#' @param data Processed deployments, in any of the forms used across the pipeline: a list of datasets
#'   (one per individual), a single aggregated table with an `ID` column, or a character vector of
#'   `.rds` file paths, read one deployment at a time. The output of [processTagData()], optionally
#'   after [calculateTailBeats()], is expected.
#' @param extra.metadata Optional data frame of external per-animal covariates, such as `sex` or
#'   `size`, carrying an `ID` column. Several rows for one ID are collapsed by joining the distinct
#'   values with `"/"`; for a numeric covariate that means conflicting values become `NA`, with a
#'   warning, so supply one value per ID. Fields already held in the tag metadata are filled
#'   automatically and need not be repeated here. For total video duration use `video.metadata`, which
#'   totals the per-file table for you. Default `NULL`.
#' @param deployments Optional `nautilus_deployments` object from [checkDeploymentMetadata()]. When
#'   supplied, the summary is completed into the full study roster: every deployment gets a row, and
#'   `status` marks each `"included"` (processed data present) or `"excluded"` (in the roster but absent
#'   from `data`). `processing_status` separately identifies a retained deployment whose processing
#'   was partial; it does not change its inclusion status. Supply the roster when reporting a study,
#'   so the table accounts for every animal tagged rather than only those whose data survived.
#'   Default `NULL`.
#' @param metadata Which deployment metadata to include, as a keyword or a vector of names.
#'   `"standard"` (default) adds every biometric trait the cohort carries plus the tagging date and
#'   coordinates; `"none"` reproduces the bare table; `"all"` adds the pop-up date and coordinates, the
#'   deployment type, and the package, logger and axis-configuration identifiers. Alternatively name the
#'   fields and traits you want - `c("sex", "deploy_lon", "deploy_lat")` - and they are emitted in the
#'   package's canonical order, so two calls requesting the same set still bind together. A named trait
#'   is given a column even where no deployment carries it, with a warning, which is how a cohort that
#'   recorded different traits can still be combined. Fields not carried onto the tag object by
#'   [importTagData()], such as `recovery_datetime`, are deliberately not offered. A field requested
#'   here can also group the rendered table, via
#'   \code{\link[=format.nautilus_summary]{format}(x, group.by = )}.
#' @param video.metadata Optional table from [getVideoMetadata()], one row per video file with `ID` and
#'   `duration` in seconds. Adds `video_duration_h`, the total footage per deployment. A deployment with
#'   no entry gets `NA`, never `0`: [getVideoMetadata()] omits folders holding no video, so absence
#'   means no footage was found, which is a different claim from a camera that ran for zero hours. This
#'   is total footage and routinely exceeds the retained record, since a camera started on deck films
#'   either side of the deployment. Default `NULL`.
#' @param exclusions Optional deployment-exclusion log: the path to the shared `exclusions.csv`, or the
#'   table itself. Written by every stage that can drop a deployment ([importTagData()],
#'   [filterDeploymentData()], [regularizeTimeSeries()], [applyAxisMapping()] and [processTagData()]),
#'   it supplies the `status_reason` of each deployment missing from `data`, and fills the record window
#'   of one that was detected and then rejected as too short. Where a deployment appears under several
#'   stages, the earliest in pipeline order wins - that is where it left. Default `NULL`.
#' @param error.stat Which error statistic the display-only population row shows: `"sd"` (standard
#'   deviation, the default) describes the spread across deployments, `"se"` (standard error) how well
#'   their mean is pinned down. They answer different questions, and the first is usually what a reader
#'   of a cohort table wants.
#' @param tbf.method Which tail-beat method to summarise, `"peaks"` or `"wavelet"`. `NULL` (default)
#'   resolves it per deployment from whichever `tbf_hz_*` columns carry values, with the package's
#'   documented order breaking a tie. The method actually used is reported in `tbf_method`, so a cohort
#'   pooled from deployments that ran different methods stays visible rather than silently blended.
#'   Even when a method is specified, a deployment with no estimates from either method retains its
#'   other metrics and reports `NA` for `tbf_mean` and `tbf_method`. A deployment with estimates only
#'   from the other method raises an error. See [tailBeatColumn()].
#' @param verbose How much detail to print: `0`/`"quiet"`, `1`/`"normal"` (header and summary), or
#'   `2`/`"detailed"` (default), which adds per-metric coverage across the processed deployments and a
#'   progress bar while the tags are read.
#'
#' @return A `nautilus_summary` data frame, one row per deployment. Columns are grouped so the table
#'   reads as a narrative - which deployment, on which animal, with which tag, put out where and when,
#'   what record came back and whether it was kept, how much data there is, what the animal experienced,
#'   and what it did:
#'
#'   \itemize{
#'     \item **identity** - `id`, and `animal_id` where the roster carries it. One individual can be
#'       tagged more than once, so the two are not the same identifier.
#'     \item **animal** - the biometric traits the cohort carries, followed by any `extra.metadata`
#'       covariates. Both describe the animal, so they sit together.
#'     \item **tag** - `tag_model`, `tag_type`, `attachment_site`, `paddle_wheel`.
#'     \item **deployment** - the metadata fields `metadata` asked for, in the package's canonical
#'       order.
#'     \item **record** - `record_start`, `record_end`, `record_duration_h`, then `status`,
#'       `status_reason` and `processing_status`. A partial processing status means the deployment
#'       was retained but an expected stream could not support its derived metrics.
#'     \item **coverage** - `n_samples`, `sampling_hz`, `n_positions`, `video_duration_h`.
#'     \item **habitat** - `depth_mean`, `depth_max`, `temp_mean`, `temp_min`, `temp_max`.
#'     \item **movement** - `vedba_mean`, `odba_mean`, `tbf_mean`, `tbf_method`, `pct_swimming`,
#'       `speed_mean`, `speed_max`, `descent_rate_max`, `ascent_rate_max`.
#'     \item **dives** - `n_dives`, `dive_duration_median_min`, `dive_duration_max_min`,
#'       `dive_depth_median_m`, `dive_depth_max_m`, `dives_incomplete`, `dives_truncated`,
#'       `dives_gapped`.
#'   }
#'
#'   A column the package does not recognise trails the block it follows rather than disappearing, so a
#'   column added later still appears. Only the columns the data and arguments support are present.
#'
#'   `deploy_datetime` and `record_start` are both reported and are not the same quantity: the first is
#'   what the metadata says, the second what the data says, whether detected by [filterDeploymentData()]
#'   or supplied through its `custom.deployment.times`. The sign of the difference is informative - a
#'   record starting before the recorded tagging time means the tag was already logging. For a
#'   deployment that was never recovered, `deploy_datetime` is the only date there is.
#'
#' @details
#' ## The study roster and excluded deployments
#'
#' With `deployments`, the summary describes the whole study rather than only the deployments that
#' produced data. An excluded deployment keeps whatever identity and deployment metadata the roster
#' holds, and every metric derived from the processed data is `NA`. Supplying `exclusions` as well fills
#' `status_reason`, and where a deployment was detected and then rejected as too short, its record
#' window too - so a row that would otherwise be empty still says what was found and why it was set
#' aside.
#'
#' ## Dive metrics
#'
#' A deployment carrying dive annotations from [detectDives()] gains a deployment-level dive summary,
#' reduced by the same routine [diveMetrics()] uses, so the per-dive and per-deployment views cannot
#' disagree. In a mixed cohort, deployments without annotations take `NA` for the dive columns.
#'
#' `dives_incomplete` counts dives whose extent was limited by the record rather than by the animal.
#' They are further split into `dives_truncated`, where the record began or ended mid-dive, and
#' `dives_gapped`, where missing samples interrupted one. A dive can be both, so the two do not sum to
#' `dives_incomplete`.
#'
#' ## Formatting and export
#'
#' The returned values are analysis-ready. The `print` method and [format.nautilus_summary()] render
#' the display version, which adds a cohort `mean +/- error` row where more than one deployment is
#' present, and can order the rows by any column and group them by any categorical one. Both are
#' formatting arguments, so a table can be re-ordered or re-grouped without summarising the tags again:
#' `format(x, order.by = "-depth_max", group.by = "status")`.
#'
#' `format()` writes ASCII by default: the table reaches a CSV far more often than a terminal, and a
#' spreadsheet opening a UTF-8 file with no byte-order mark will guess the encoding and can turn a
#' degree sign into mojibake. Pass `symbols = "unicode"` where the consumer handles UTF-8 -
#' `knitr::kable()`, `flextable`, a paste into a manuscript.
#'
#' @seealso [processTagData()] and [calculateTailBeats()] for the steps that produce the input;
#'   [detectDives()] and [diveMetrics()] for the dive annotations; [getVideoMetadata()] for the video
#'   table; [checkDeploymentMetadata()] for the study roster; [metadataColumns()] for the metadata
#'   vocabulary; [format.nautilus_summary()] for the display-ready table.
#'
#' @examples
#' \dontrun{
#' # One row per deployment, from processed and tail-beat-annotated tags.
#' tag <- calculateTailBeats(processTagData(oriented))
#' summary <- summarizeTagData(tag)
#'
#' # Complete the study roster, attach external covariates, and explain every exclusion.
#' summary <- summarizeTagData(list.files("./tailbeats", full.names = TRUE),
#'                             deployments = deployments,
#'                             extra.metadata = animal_metadata[, c("ID", "sex", "size")],
#'                             exclusions = "./data interim/exclusions.csv")
#'
#' # A formatted table for a report, grouped by sex.
#' format(summary, style = "report", group.by = "sex")
#' }
#' @export
summarizeTagData <- function(data,
                             extra.metadata = NULL,
                             deployments = NULL,
                             metadata = "standard",
                             video.metadata = NULL,
                             exclusions = NULL,
                             error.stat = "sd",
                             tbf.method = NULL,
                             verbose = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  error.stat <- tolower(error.stat)
  .assert_choice(error.stat, "error.stat", c("sd", "se"))
  .assert_string(tbf.method, "tbf.method", null_ok = TRUE)
  if (!is.null(tbf.method)) .assert_choice(tbf.method, "tbf.method", .tbBackends())
  if (!is.null(deployments)) {
    if (!inherits(deployments, "nautilus_deployments"))
      .abort("{.arg deployments} must be a {.cls nautilus_deployments} object from {.fn checkDeploymentMetadata}.")
    .assert_columns(deployments, "id", "deployments")
  }
  if (!is.null(extra.metadata)) .assert_columns(extra.metadata, "ID", "extra.metadata")
  covariates <- character(0)

  # resolve input: a list of processed datasets, a single aggregated data.frame (split by ID), or a
  # character vector of .rds file paths (loaded lazily) - consistent with the rest of the pipeline.
  meta_req <- .summaryResolveMetadata(metadata)
  .assert_video_metadata(video.metadata)
  r <- .resolveInput(data, id.col = "ID")

  .log_header(lvl, "summarizeTagData", "Summarising the deployments",
              bullets = sprintf("Input: %d tag%s", r$n, if (r$n != 1) "s" else ""))

  # per-deployment summaries (the same engine that backs summary.nautilus_tag). Empty/malformed deployments
  # come back NULL (.summarize warns per case); report the omissions here rather than letting them vanish.
  pb      <- .log_progress_start(lvl, r$n, "Summarising")           # live bar at detailed verbosity (lvl >= 2)
  parts   <- lapply(seq_len(r$n), function(i) {
    .log_progress_step(pb); .summarize(r$get(i), tbf.method, meta_req)
  })
  .log_progress_done(pb)
  dropped <- vapply(parts, is.null, logical(1))
  if (any(dropped))
    warning(sprintf("summarizeTagData: %d of %d deployment(s) were empty or malformed and omitted: %s",
                    sum(dropped), r$n, paste(r$ids[dropped], collapse = ", ")), call. = FALSE)
  # fill: the dive block exists only for deployments annotated by detectDives(), so a mixed cohort has
  # ragged columns - the deployments without it come back NA, as for any absent metric
  summary_table <- if (any(!dropped)) as.data.frame(data.table::rbindlist(parts[!dropped], fill = TRUE))
                   else .summaryTemplate()
  rownames(summary_table) <- NULL
  summary_table <- .summaryOrderMeta(summary_table, meta_req)

  # optionally complete the study roster: every deployment in `deployments` gets a row - processed ones
  # carry full metrics + status "included"; the rest get NA metrics, identity filled from the QC roster,
  # and status "excluded" (works on an empty summary too, so an all-excluded roster is still produced).
  # Done BEFORE the covariate join so external covariates attach to every row.
  if (!is.null(deployments)) summary_table <- .completeRoster(summary_table, deployments, meta_req)
  summary_table <- .attachVideoDuration(summary_table, video.metadata)
  summary_table <- .attachExclusions(summary_table, exclusions)

  # optional join of EXTERNAL per-animal covariates (those not already in the tag metadata)
  if (!is.null(extra.metadata) && nrow(summary_table) > 0) {
    em <- as.data.frame(extra.metadata)
    cov_cols <- setdiff(names(em), "ID")
    if (length(cov_cols)) {
      # covariates cannot overwrite summary fields; reserve "status" too (only present once a roster completes,
      # but it is a structural column - a covariate named "status" would silently break the roster split below)
      clash <- intersect(cov_cols, union(names(summary_table), "status"))
      if (length(clash))
        .abort(c("{.arg extra.metadata} column(s) {.val {clash}} clash with reserved summary columns.",
                 "i" = "Rename the covariate column(s) before passing."))
      is_num <- vapply(em[, cov_cols, drop = FALSE], is.numeric, logical(1))
      # collapse multiple rows per ID (distinct values joined by "/")
      agg <- stats::aggregate(em[, cov_cols, drop = FALSE], by = list(id = as.character(em[["ID"]])),
                              FUN = function(v) paste(unique(v[!is.na(v)]), collapse = "/"), drop = FALSE)
      agg[agg == ""] <- NA
      for (cc in cov_cols[is_num]) {
        if (any(grepl("/", agg[[cc]]), na.rm = TRUE))
          warning(sprintf("summarizeTagData: numeric covariate '%s' has multiple values for some ID(s); those become NA (provide one value per ID).", cc), call. = FALSE)
        agg[[cc]] <- .asNumericSafe(agg[[cc]])
      }
      if (!length(intersect(agg$id, summary_table$id)))
        warning("summarizeTagData: no 'extra.metadata' ID matches any deployment - covariates attached as all-NA (check the ID column values/type).", call. = FALSE)
      ord_ids <- summary_table$id                                   # base merge() reorders rows; restore roster order
      summary_table <- merge(summary_table, agg, by = "id", all.x = TRUE, sort = FALSE)
      summary_table <- summary_table[match(ord_ids, summary_table$id), , drop = FALSE]
      rownames(summary_table) <- NULL
      covariates <- cov_cols            # seated with the traits below: both describe the animal
    }
  }

  # One ordering pass at the end, once every block exists. `status`, `status_reason` and
  # `video_duration_h` are attached by steps that run after the first pass, and seating them where they
  # were appended is how `status_reason` ended up thirty columns from `status`.
  summary_table <- .summaryOrderMeta(summary_table, meta_req, covariates)

  if (lvl >= 1L) {
    .log_summary(lvl)
    .summaryReport(lvl, summary_table, has_roster = !is.null(deployments))
    .log_runtime(lvl, start.time)
  }

  .newSummary(summary_table, error.stat)
}
