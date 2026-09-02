#######################################################################################################
# Summarize Tag Data ##################################################################################
#######################################################################################################

#' Summarise processed tag deployments
#'
#' @description
#' Every study built on archival tags needs the same table: one row per deployment, giving how long the
#' tag recorded, at what rate, and the headline figures for depth, temperature, activity and swimming.
#' Assembling it by hand across a fleet is tedious and easy to get subtly wrong, particularly where
#' deployments differ in which processing steps they have been through.
#'
#' This function builds that table. The descriptive fields come from each tag's own metadata, so they
#' need not be supplied again, and an optional `extra.metadata` table attaches per-animal covariates
#' such as sex or size that the tag never recorded.
#'
#' The returned object is a **typed** data frame (numeric columns stay numeric, datetimes stay
#' POSIXct), meant for downstream analysis. Its `print` method renders a formatted table and, for more
#' than one deployment, appends a population `mean +/- error` row for display only. To export that same
#' formatted table (for a report or paper), call `format()` on the result and write it out, e.g.
#' `write.csv(format(summary), "summary.csv", row.names = FALSE)`.
#'
#' @param data Processed deployments, in any of the forms used across the pipeline: a list of datasets
#'   (one per individual), a single aggregated data.table/data.frame with an `ID` column, or a character
#'   vector of `.rds` file paths (loaded lazily, one per deployment). The output of [processTagData()]
#'   (optionally after [calculateTailBeats()]) is expected.
#' @param extra.metadata Optional data frame of external per-animal covariates to attach (e.g. `sex`,
#'   `size`). Must contain an `ID` column. Multiple rows per ID are collapsed (distinct values joined
#'   by "/"). Fields already captured in the tag metadata (tag model/type, attachment site, sampling
#'   rate) are filled automatically and need not be provided here. This is also the route for any
#'   external per-deployment metric, e.g. total video duration from [getVideoMetadata()]:
#'   Default `NULL`. For total video duration use `video.metadata` instead, which totals the per-file
#'   table for you.
#' @param deployments Optional `nautilus_deployments` object from [checkDeploymentMetadata()]. When
#'   supplied, the summary is completed into the full study roster: every deployment in `deployments`
#'   gets a row, and a `status` column marks each as `"included"` (processed data present, full metrics)
#'   or `"excluded"` (in the roster but absent from `data` - NA metrics, identity filled from the
#'   roster). The reason for exclusion is not inferred here; see [issues()]. Default `NULL`. Supply it
#'   when reporting a study, so the table accounts for every animal tagged rather than only those whose
#'   data survived.
#' @param metadata Which deployment metadata to include, as a single keyword or a vector of names:
#'   `"standard"` (default) adds every biometric trait the cohort carries plus the tagging date and
#'   coordinates; `"none"` reproduces the bare table; `"all"` adds the pop-up date and coordinates, the
#'   deployment type, and the package, logger and axis-configuration identifiers. Alternatively name the
#'   fields and traits you want - `c("sex", "deploy_lon", "deploy_lat")` - and they are emitted in the
#'   package's canonical order, so two calls requesting the same set still bind together. A named trait
#'   is given a column even where no deployment carries it (with a warning), which is how a cohort that
#'   recorded different traits can still be combined. Fields not carried onto the tag object by
#'   [importTagData()], such as `recovery_datetime`, are deliberately not offered. A field requested
#'   here can be used to group the rendered table, via
#'   \code{\link[=format.nautilus_summary]{format}(x, group.by = )}.
#' @param video.metadata Optional table from [getVideoMetadata()] - one row per video file, with `ID`
#'   and `duration` (seconds). Adds `video_duration_h`, the total footage per deployment. The totalling
#'   happens here because that table has several rows per deployment, which the generic covariate join
#'   would collapse to a string. A deployment with no entry gets `NA`, never `0`: [getVideoMetadata()]
#'   drops folders holding no video, so absence means "no footage found", which is a different claim
#'   from "the camera ran for zero hours". This is TOTAL footage and routinely exceeds the retained
#'   record - a camera started on deck records either side of the deployment. Default `NULL`.
#' @param exclusions Optional deployment-exclusion log - the path to the shared `exclusions.csv`, or
#'   the table itself. Written by every stage that can drop a deployment ([importTagData()],
#'   [filterDeploymentData()], [regularizeTimeSeries()], [applyAxisMapping()] and [processTagData()]),
#'   it supplies the `status_reason` of each deployment that is missing from `data`, and fills the
#'   record window of one that was detected and then rejected as too short. Where a deployment appears
#'   under several stages, the earliest in pipeline order wins - that is where it left. Default `NULL`.

#' @param tbf.method Which tail-beat backend to summarise, e.g. `"wavelet"`. `NULL` (default) resolves it
#'   per deployment from the data -- whichever `tbf_hz_*` columns carry values, with the package's
#'   documented order (`peaks`, then `wavelet`) breaking a tie. The backend actually used is reported as
#'   `tbf_method`, so a cohort pooled from deployments that ran different backends stays visible rather
#'   than silently blended. See [tailBeatColumn()].
#' @param error.stat Which error statistic the display-only population row shows: `"sd"` (standard
#'   deviation, the default) describes the spread across deployments, while `"se"` (standard error)
#'   describes how well their mean is pinned down. They answer different questions, and the first is
#'   usually what a reader of a cohort table wants.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet" (silent), `TRUE`/`1`/"normal" (header + summary), or
#'   `2`/"detailed" (default): additionally reports per-metric coverage across the processed deployments
#'   and shows a live progress bar while the tags are read (cli auto-hides it for fast runs).
#'
#' @return A `nautilus_summary` data frame, one row per deployment, with (where available) the columns:
#' \itemize{
#'   \item **id**, **animal_id**: the deployment, and the animal that carried it - one individual can
#'     be tagged more than once, so the two are not the same identifier. `animal_id` appears when it was
#'     mapped with `metadataColumns(animal_id = )`, and is completed from the roster for a deployment
#'     whose tag predates that mapping.
#'   \item **tag_model**, **tag_type**, **attachment_site**, **paddle_wheel**: the tag. `paddle_wheel`
#'     sits here rather than with the metrics because it is what says whether the speed columns mean
#'     anything.
#'   \item **record_start**, **record_end**, **record_duration_h**, **n_samples**: the
#'     recorded data span - the true on-animal window once the data have been deployment-filtered.
#'   \item **sampling_hz**: original sampling rate (Hz).
#'   \item **depth_mean**, **depth_max** (m); **temp_mean**, **temp_min**, **temp_max** (degrees Celsius).
#'   \item **vedba_mean**, **odba_mean** (g): mean activity (dynamic body acceleration).
#'   \item **tbf_method**: which tail-beat backend `tbf_mean` came from (`"peaks"`/`"wavelet"`).
#'   \item **tbf_mean** (Hz): mean tail-beat frequency over beating samples; **pct_swimming**
#'     (%): share of time actively swimming - both present only after [calculateTailBeats()].
#'   \item **paddle_wheel** (logical): whether the tag carried a paddle wheel (disambiguates the speed columns).
#'   \item **speed_mean**, **speed_max** (m/s): paddle-wheel speed, when available.
#'   \item **descent_rate_max**, **ascent_rate_max** (m/s): fastest vertical speeds (descent positive).
#'   \item **n_positions**: total number of position fixes within the record span, when available.
#'   \item **status**: `"included"`/`"excluded"` - only when `deployments` is supplied (see that
#'     argument); **status_reason**, the rule that set a deployment aside, when `exclusions` is supplied.
#'   \item the **metadata block** selected by `metadata`: biometric traits under the names they were
#'     declared with at import, then `deploy_datetime`, `deploy_lon`, `deploy_lat` and, with
#'     `metadata = "all"`, the pop-up and identifier fields. These are filled from each tag's own
#'     metadata, and from the roster for a deployment whose data never arrived - so a tag that was
#'     never recovered still reports who was tagged, when and where.
#'   \item **video_duration_h**: total video recorded (h), when `video.metadata` is supplied.
#'   \item **n_dives**; **dive_duration_median_min**, **dive_duration_max_min** (min);
#'     **dive_depth_median_m**, **dive_depth_max_m** (m, per-dive maximum depths);
#'     **dives_incomplete**, **dives_truncated**, **dives_gapped**: the dive block -
#'     present only for deployments annotated by [detectDives()], and computed with the same reducer
#'     [diveMetrics()] uses (see Details).
#' }
#'
#' @details
#' ## The dive block
#'
#' Where the data carry a `dive_id` column, the summary gains a per-deployment dive block. Its numbers
#' are not recomputed here: the deployment is reduced by the same engine that sits behind
#' [diveMetrics()], so a dive count or median duration quoted from the summary is by construction the
#' one the per-dive table gives. It reads the canonical `datetime` and `depth` columns, which is what
#' the pipeline produces and what this block requires.
#'
#' One figure is not quite what it appears: the printed cohort line reports a median of per-deployment
#' medians, not a pooled median over every dive. The per-deployment columns are exact, and a pooled
#' figure is one [diveMetrics()] call away.
#'
#' `dives_incomplete` counts the dives whose extent was set by the record rather than by the animal,
#' split into `dives_truncated`, where the tag started or stopped mid-dive, and `dives_gapped`, where
#' an interruption in time or a depth channel that went dark bounds the dive. A dive can be both.
#' Deployments that have not been through [detectDives()] simply have no dive columns, and in a mixed
#' cohort they come back `NA`, as for any absent metric.
#'
#' @details
#' ## What a deployment that produced no data still reports
#'
#' A tag that was never recovered, or one set aside during filtering, is not a deployment nothing is
#' known about. Its row is completed from the roster through the same field declaration the processed
#' rows are read through, so it carries the same identity, trait and tagging columns rather than a bare
#' identifier. Supply `exclusions` as well and a deployment rejected for being too short also reports
#' the window that was detected before it was rejected, which is the measurement the decision rested on.
#'
#' ## The column order
#'
#' Columns are grouped so the table reads as a narrative: which deployment, on which animal, with which
#' tag, put out where and when, what record came back and whether it was kept, how much data there is,
#' what the animal experienced, and what it did. `status` and `status_reason` close the record block
#' together, because the reason usually explains a short or absent record. Traits and any
#' `extra.metadata` covariates sit together, since both describe the animal. Anything the package does
#' not recognise trails the block it follows rather than disappearing.
#'
#' `deploy_datetime` and `record_start` are both reported and are not the same quantity: the first is
#' what the metadata says, the second is what the data says, whether detected by
#' [filterDeploymentData()] or supplied through its `custom.deployment.times`. Across a 52-deployment
#' cohort only 9 agreed to the minute and 5 differed by more than an hour, and the sign of the
#' difference is informative - a record starting BEFORE the recorded tagging time means the tag was
#' already logging. For a deployment that was never recovered, `deploy_datetime` is the only date there
#' is.
#'
#' ## Exporting the table
#'
#' `format()` renders the publication version, and its output is ASCII by default: the table is written
#' to a CSV far more often than it is read in a terminal, and a spreadsheet opening a UTF-8 file with no
#' byte-order mark will guess the encoding and can turn a degree sign into mojibake. Pass
#' `symbols = "unicode"` for the typographic forms where the consumer handles UTF-8 - `knitr::kable()`,
#' `flextable`, a paste into a manuscript.
#'
#' @seealso [processTagData()], [filterDeploymentData()], [detectDives()], [diveMetrics()],
#'   [getVideoMetadata()], [metadataColumns()].
#' @examples
#' \dontrun{
#' # One row per deployment from processed (and tail-beat-annotated) tags.
#' tag <- calculateTailBeats(processTagData(oriented))
#' summarizeTagData(tag)
#'
#' # Complete the study roster and attach external per-animal covariates.
#' summarizeTagData(list.files("./tailbeats", full.names = TRUE),
#'                  deployments = deployments,
#'                  extra.metadata = animal_metadata[, c("ID", "sex", "size")])
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


#' Render the SUMMARY block as short, titled sections rather than one dense line per topic.
#'
#' The information is unchanged and so is its verbosity gating - what moves is the layout. A single line
#' carrying six coverage counts is quick to write and slow to read; the same six as an aligned column
#' under a heading can be scanned without parsing prose. Sections appear only when they have something
#' to say, so a run without a roster, without dives or without exclusions simply omits them.
#' @param lvl Resolved verbosity.
#' @param st The finished summary table.
#' @param has_roster Whether `deployments` completed the roster, which is what makes the
#'   included/excluded split meaningful.
#' @keywords internal
#' @noRd
.summaryReport <- function(lvl, st, has_roster) {
  rostered <- has_roster && "status" %in% names(st)
  incl     <- if (rostered) st[st$status == "included", , drop = FALSE] else st
  n        <- nrow(incl)
  tick     <- cli::col_green(cli::symbol$tick)
  cross    <- cli::col_red(cli::symbol$cross)

  ## Deployments -----------------------------------------------------------------------------------
  .log_section(lvl, "Deployments")
  if (rostered) {
    n_excl <- nrow(st) - n
    # "Total", not "Roster": under a "Deployments" heading the plain word needs no explaining, and
    # the roster is a concept from the `deployments` argument rather than one the reader arrives with.
    .log_rows(lvl, c(Total = nrow(st), Included = n, Excluded = n_excl),
              symbols = c(tick, tick, if (n_excl > 0) cross else tick))
  } else {
    .log_rows(lvl, stats::setNames(n, if (n == 1) "Tag summarised" else "Tags summarised"),
              symbols = tick)
  }

  ## Data availability -----------------------------------------------------------------------------
  # detailed-only, as before: a completeness check for spotting the deployments that never had
  # calculateTailBeats() run, or that carry no magnetometer or positions
  if (lvl >= 2L && n > 0) {
    cnt  <- function(col) if (col %in% names(incl)) sum(is.finite(incl[[col]])) else 0L
    p    <- if ("n_positions" %in% names(incl)) incl$n_positions else rep(NA_real_, n)
    rows <- c(Depth = cnt("depth_max"), Temperature = cnt("temp_max"), Activity = cnt("vedba_mean"),
              `Tail-beats` = cnt("tbf_mean"), Paddle = cnt("speed_mean"),
              Positions = sum(is.finite(p) & p > 0))
    .log_section(lvl, sprintf("Data availability (%d %s)", n, if (rostered) "included" else "processed"),
                 min_level = 2L)
    .log_rows(lvl, rows, min_level = 2L)
  }

  ## Dives -----------------------------------------------------------------------------------------
  if (lvl >= 2L && "n_dives" %in% names(incl)) .summaryDiveSection(lvl, incl)

  ## Excluded deployments --------------------------------------------------------------------------
  if (lvl >= 2L && rostered) {
    ex <- st$id[st$status == "excluded"]
    if (length(ex)) {
      .log_section(lvl, "Excluded deployments", min_level = 2L)
      shown <- if (length(ex) > 12L) paste0(paste(utils::head(ex, 12L), collapse = ", "),
                                            sprintf(", ... (+%d more)", length(ex) - 12L))
               else paste(ex, collapse = ", ")
      .log_block(lvl, shown, min_level = 2L)
    }
  }
  cli::cli_text("")
  invisible()
}


#' The dive section of the SUMMARY block, present once detectDives() has annotated any deployment.
#' @keywords internal
#' @noRd
.summaryDiveSection <- function(lvl, incl) {
  nd    <- incl$n_dives
  have  <- is.finite(nd)                                  # the deployments that carry the block
  n_dep <- sum(have)
  if (!n_dep) return(invisible())
  tot <- sum(nd[have])

  if (tot == 0) {
    .log_section(lvl, "Dives", min_level = 2L)
    .log_block(lvl, sprintf("None detected in %d annotated deployment%s.",
                            n_dep, if (n_dep != 1) "s" else ""), min_level = 2L)
    return(invisible())
  }

  # "across N deployments" counts the ones that HAVE dives, not the ones that were annotated - a cohort
  # where 3 of 10 tags dived otherwise reads "across 10 deployments"
  n_with <- sum(nd[have] > 0)
  # no pooled dive list exists here, so the typical duration is a median of per-deployment medians. Said
  # plainly, because it is the kind of number that gets copied into a methods section.
  med  <- stats::median(incl$dive_duration_median_min[have], na.rm = TRUE)
  rows <- c(Total = format(tot, big.mark = ","),
            `With dives` = format(n_with),
            `Typical duration` = sprintf("%.1f min (median of deployment medians)", med),
            Longest = sprintf("%.1f min", max(incl$dive_duration_max_min[have], na.rm = TRUE)),
            Deepest = sprintf("%.1f m", max(incl$dive_depth_max_m[have], na.rm = TRUE)))

  n_inc <- sum(incl$dives_incomplete[have], na.rm = TRUE)
  if (n_inc > 0) {
    n_tr <- sum(incl$dives_truncated[have], na.rm = TRUE)
    n_gp <- sum(incl$dives_gapped[have], na.rm = TRUE)
    # the two causes overlap, so they can sum past the total. Say so when they actually do, rather than
    # let "2 incomplete: 2 truncated, 1 gap-interrupted" read as an arithmetic error.
    rows <- c(rows, Incomplete = sprintf("%s (%s truncated, %s gap-interrupted%s)",
                                         format(n_inc, big.mark = ","), format(n_tr, big.mark = ","),
                                         format(n_gp, big.mark = ","),
                                         if (n_tr + n_gp > n_inc) "; a dive can be both" else ""))
  }
  .log_section(lvl, "Dives", min_level = 2L)
  .log_rows(lvl, rows, min_level = 2L)
  invisible()
}


################################################################################
# Per-deployment summary (internal; reused by summary.nautilus_tag) ############
################################################################################

#' One-row, typed summary for a single deployment.
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.summarize <- function(data_individual, tbf.method = NULL,
                       meta.req = list(fields = character(0), traits = character(0))) {

  if (is.null(data_individual) || nrow(data_individual) == 0) return(NULL)
  dt <- data_individual

  # structural validation: skip a malformed deployment with a clear warning rather than silently producing
  # Inf/garbage (missing / non-POSIXct datetime) or mixing animals (several IDs collapsed into one row)
  if (!"ID" %in% names(dt)) { warning("summarizeTagData: a deployment has no 'ID' column; skipped.", call. = FALSE); return(NULL) }
  ids <- unique(dt[["ID"]]); ids <- ids[!is.na(ids)]
  if (!length(ids)) { warning("summarizeTagData: a deployment has no non-missing 'ID'; skipped.", call. = FALSE); return(NULL) }
  id <- as.character(ids[1])
  if (length(ids) > 1L) {
    warning(sprintf("summarizeTagData: a deployment table contains %d IDs (%s); it must be one animal per deployment - skipped.",
                    length(ids), paste(utils::head(ids, 4L), collapse = ", ")), call. = FALSE)
    return(NULL)
  }
  if (!"datetime" %in% names(dt) || !inherits(dt[["datetime"]], "POSIXt") || all(is.na(dt[["datetime"]]))) {
    warning(sprintf("summarizeTagData: deployment '%s' has no valid POSIXct 'datetime' column; skipped.", id), call. = FALSE)
    return(NULL)
  }

  meta <- .getMeta(.ensureMeta(dt))

  # scalar coercions tolerant of NULL / NA / wrong-length metadata fields
  s_chr <- function(v) { v <- v %||% NA_character_; if (length(v) != 1) NA_character_ else as.character(v) }
  s_num <- function(v) { v <- .asNumericSafe(v %||% NA_real_); if (length(v) != 1) NA_real_ else v }
  s_lgl <- function(v) {                                   # tolerant logical (handles 0/1, "TRUE"/"yes", NA)
    v <- v %||% NA; if (length(v) != 1 || is.na(v)) return(NA)
    if (is.character(v)) tolower(v) %in% c("true", "1", "yes", "y", "t") else as.logical(v)
  }
  # statistic over a (possibly absent / all-NA) numeric column -> NA when unavailable
  cstat <- function(col, fun) {
    if (!col %in% names(dt)) return(NA_real_)
    v <- dt[[col]]; v <- v[is.finite(v)]
    if (!length(v)) return(NA_real_)
    as.numeric(fun(v))
  }
  hrs  <- function(a, b) as.numeric(difftime(b, a, units = "hours"))

  # the deployment window is the data's own datetime range. After filterDeploymentData() this is the
  # TRUE on-animal recording period; the broad metadata deploy/popup window (which includes pre- and
  # post-deployment drift) is intentionally NOT duplicated here - read it from tagMetadata() if needed.
  record_start <- min(dt[["datetime"]], na.rm = TRUE)
  record_end   <- max(dt[["datetime"]], na.rm = TRUE)
  # position fixes: TOTAL count within the record span, from the canonical complete record
  # (meta$ancillary$positions); post-deployment drift fixes are excluded here (the full record is in
  # tagMetadata()$ancillary$positions). NA when the tag carries no position stream at all.
  has_positions <- !is.null(.getMeta(dt)$ancillary$positions)
  pos <- .tagPositions(dt)
  if (nrow(pos))
    pos <- pos[!is.na(pos$datetime) & pos$datetime >= record_start & pos$datetime <= record_end, , drop = FALSE]
  n_positions <- if (has_positions) nrow(pos) else NA_integer_
  vv_min <- cstat("vertical_velocity", min)               # most negative vertical speed = fastest ascent
  # Tail-beat metrics (NA unless calculateTailBeats() has run). The estimate columns are named after the
  # backend that produced them, so the one to report is resolved from the data - which tbf_hz_* columns
  # actually carry values - and never from the metadata, which does not survive rbind, a CSV round trip
  # or dplyr::mutate. The resolved backend is reported alongside the value as `tbf_method`, so a cohort
  # pooled from deployments that used different backends is visible rather than silently blended.
  tbf_col    <- .tbfResolve(dt, "hz", method = tbf.method)
  tbf_method <- .tbfMethodOf(tbf_col)
  pct_swimming <- if ("tbf_swimming" %in% names(dt)) {
    sw <- dt[["tbf_swimming"]]; sw <- sw[!is.na(sw)]
    if (length(sw)) 100 * mean(as.numeric(sw)) else NA_real_
  } else NA_real_

  out <- data.frame(
    id                    = id,
    animal_id             = s_chr(meta$animal_id),
    tag_model             = s_chr(meta$tag$model),
    tag_type              = s_chr(meta$tag$type),
    attachment_site       = s_chr(meta$deployment$attachment_site),
    record_start          = record_start,
    record_end            = record_end,
    record_duration_h     = hrs(record_start, record_end),
    n_samples             = nrow(dt),
    sampling_hz           = s_num(meta$sensors$sampling_hz_original),
    depth_mean            = cstat("depth", mean),
    depth_max             = cstat("depth", max),
    temp_mean             = cstat("temp", mean),
    temp_min              = cstat("temp", min),
    temp_max              = cstat("temp", max),
    vedba_mean            = cstat("vedba", mean),
    odba_mean             = cstat("odba", mean),
    tbf_mean              = if (is.null(tbf_col)) NA_real_ else cstat(tbf_col, mean),
    tbf_method            = tbf_method,
    pct_swimming          = pct_swimming,
    paddle_wheel          = s_lgl(meta$tag$paddle_wheel),
    speed_mean            = cstat("paddle_speed", mean),
    speed_max             = cstat("paddle_speed", max),
    descent_rate_max      = cstat("vertical_velocity", max),
    ascent_rate_max       = if (is.na(vv_min)) NA_real_ else -vv_min,
    n_positions           = n_positions,
    stringsAsFactors      = FALSE,
    check.names           = FALSE
  )

  # Requested deployment metadata and biometric traits, read through the shared declaration so a
  # processed deployment and a roster-only one are filled from the same list of fields.
  mb <- .summaryMetaRow(meta, meta.req$fields, meta.req$traits)
  if (length(mb))
    out <- cbind(out, as.data.frame(mb, stringsAsFactors = FALSE, check.names = FALSE))

  # optional dive block, appended only for data that have been through detectDives(). A non-numeric
  # dive_id is not a dive annotation, and is left out rather than guessed at (same tolerance as cstat()).
  if (all(c("dive_id", "dive_phase", "depth") %in% names(dt)) && is.numeric(dt[["dive_id"]]))
    out <- cbind(out, .summarizeDives(dt, id))
  out
}


#' Per-deployment dive block for the summary table.
#'
#' The statistics are NOT recomputed here: the deployment is reduced with `.diveMetricsOne()`, the
#' same engine [diveMetrics()] calls, so a dive count or a median duration quoted from the
#' summary is by construction the one the per-dive table gives. Reimplementing them is how the two
#' functions would drift apart and disagree in a publication.
#'
#' The reduction is pinned to the canonical `datetime` / `depth` columns, which is what everything
#' upstream of here produces. A caller who ran [diveMetrics()] against differently named
#' columns would get a table this block does not match - the guard below requires the canonical names
#' to be present, so the mismatch cannot pass silently as a wrong number.
#'
#' A deployment whose `dive_id` is 0 throughout yields a 0-row reduction, hence `n_dives = 0` and NA
#' statistics - a clean "no dives detected" row rather than an error. The censoring counts stay 0 there,
#' since counting over an empty table is unambiguous.
#' @keywords internal
#' @noRd
.summarizeDives <- function(dt, id) {
  dm <- .diveMetricsOne(dt, id, "datetime", "depth",
                        variables = NULL, circular.variables = NULL,
                        statistics = "mean", by.phase = FALSE)
  # statistic over a per-dive column -> NA when there are no dives (or no usable values)
  dstat <- function(col, fun) {
    v <- dm[[col]]; v <- v[is.finite(v)]
    if (!length(v)) return(NA_real_)
    as.numeric(fun(v))
  }
  data.frame(
    n_dives                  = nrow(dm),
    dive_duration_median_min = dstat("duration_s", stats::median) / 60,
    dive_duration_max_min    = dstat("duration_s", max) / 60,
    dive_depth_median_m      = dstat("max_depth_m", stats::median),
    dive_depth_max_m         = dstat("max_depth_m", max),
    # the record, not the animal, set the extent of these dives (a dive can be both truncated and gapped)
    dives_incomplete         = sum(!dm$complete, na.rm = TRUE),
    dives_truncated          = sum(dm$truncated_start | dm$truncated_end, na.rm = TRUE),
    dives_gapped             = sum(dm$n_gaps > 0, na.rm = TRUE),
    stringsAsFactors         = FALSE,
    check.names              = FALSE
  )
}


#' Complete the study roster: add a row for every deployment in `deployments`, with a status flag.
#'
#' Processed deployments keep their full metrics (status "included"); deployments present in the QC
#' roster but absent from the processed data get NA metrics, their identity filled from the roster, and
#' status "excluded". `status` is intentionally coarse: summarizeTagData cannot reliably attribute WHY a
#' deployment is absent (a QC error, a sensor exclusion, or a manual drop), so the detailed reasons stay
#' in issues(). The `nautilus_deployments` table is normalised to canonical role names, so identity is
#' read straight off it (optional roles such as tag_type/attachment_site/paddle_wheel may be absent).
#' @keywords internal
#' @noRd
.completeRoster <- function(summary_table, deployments,
                            meta.req = list(fields = character(0), traits = character(0))) {
  rid  <- as.character(deployments[["id"]])
  pick <- function(col) if (col %in% names(deployments)) deployments[[col]] else rep(NA, length(rid))
  lgl  <- function(v) {                          # NA-preserving (unknown paddle stays NA, not silently FALSE)
    if (is.character(v)) { out <- tolower(v) %in% c("true", "1", "yes", "y", "t"); out[is.na(v)] <- NA; out }
    else as.logical(v)
  }

  roster_traits <- if (length(meta.req$traits) == 1L && is.na(meta.req$traits))
    attr(deployments, "nautilus.columns")$traits %||% character(0) else meta.req$traits
  summary_table$status <- rep("included", nrow(summary_table))   # length-safe on an empty (all-excluded) table
  missing_ids <- setdiff(rid, summary_table$id)
  if (length(missing_ids)) {
    add <- summary_table[rep(NA_integer_, length(missing_ids)), , drop = FALSE]   # NA rows, types preserved
    rownames(add) <- NULL
    m <- match(missing_ids, rid)
    add$id <- missing_ids
    if ("animal_id" %in% names(add))       add$animal_id       <- as.character(pick("animal_id"))[m]
    if ("tag_model" %in% names(add))       add$tag_model       <- as.character(pick("tag_model"))[m]
    if ("tag_type" %in% names(add))        add$tag_type        <- as.character(pick("tag_type"))[m]
    if ("attachment_site" %in% names(add)) add$attachment_site <- as.character(pick("attachment_site"))[m]
    if ("paddle_wheel" %in% names(add))    add$paddle_wheel    <- lgl(pick("paddle_wheel"))[m]

    # The metadata a deployment came with does not stop being true because its data never arrived. A
    # tag that was never recovered has no record, no depth and no dive - but it was still attached to a
    # known animal, on a known date, at a known place, and the roster is holding all of it. Filled from
    # the SAME field declaration the processed rows are read through, so the two cannot drift.
    spec <- .summaryMetaFields()
    for (f in intersect(meta.req$fields, names(add))) {
      src <- spec[[f]]$roster
      if (!src %in% names(deployments)) next
      val <- deployments[[src]][m]
      add[[f]] <- switch(spec[[f]]$type,
        # pick() yields a logical NA vector for an absent role, and assigning that into a POSIXct
        # column silently retypes the whole column, so datetimes are only taken when genuinely POSIXt
        time = if (inherits(deployments[[src]], "POSIXt")) val else add[[f]],
        num  = .asNumericSafe(val),
        as.character(val))
    }
    # Traits keep the names they were declared with at import, so the roster carries them verbatim.
    # When the caller asked for "whatever traits exist" and NO processed deployment carried any - every
    # tag excluded, or a cohort imported without biometrics - the roster is the only source, so its
    # non-role columns supply both the names and the values. metadataColumns() IS the role vocabulary,
    # which is what lets a trait be told from a mapped field without a second list to keep in step.
    tr <- setdiff(names(add), c(names(.summaryTemplate()), .summaryDiveCols(), meta.req$fields, "status"))
    want <- meta.req$traits
    if (length(want) == 1L && is.na(want)) {
      # The traits the caller DECLARED at import, which checkDeploymentMetadata() records on the roster.
      # Taking every non-role column instead swept the whole workbook in - and only down this path, so a
      # study column appeared on excluded rows and was NA on every processed one.
      tr <- union(tr, attr(deployments, "nautilus.columns")$traits %||% character(0))
    } else tr <- union(tr, want)
    for (tn in intersect(tr, names(deployments))) {
      val <- deployments[[tn]][m]
      if (!tn %in% names(add)) add[[tn]] <- rep(if (is.numeric(val)) NA_real_ else NA, nrow(add))
      add[[tn]] <- if (is.numeric(add[[tn]]) || is.numeric(val)) .asNumericSafe(val) else as.character(val)
      if (!tn %in% names(summary_table)) summary_table[[tn]] <- rep(NA, nrow(summary_table))
    }
    add <- add[, union(names(summary_table), names(add)), drop = FALSE]
    add$status <- "excluded"
    summary_table <- rbind(summary_table, add)
  }

  # COMPLETE, never override. A processed deployment's own metadata stays authoritative - it is what the
  # processing actually used - but where the tag carries nothing the roster may still know, and leaving
  # the cell empty recreates the very asymmetry this function exists to remove: a column populated on
  # excluded rows and blank on included ones. Bites hardest on a role added after a cohort was imported,
  # like `animal_id`, which would otherwise need a full re-import to appear.
  fillable <- intersect(c("animal_id", "tag_model", "tag_type", "attachment_site",
                          meta.req$fields, roster_traits),
                        intersect(names(summary_table), names(deployments)))
  if (length(fillable)) {
    mm <- match(summary_table$id, rid)
    for (cc in fillable) {
      gap <- is.na(summary_table[[cc]]) & !is.na(mm)
      if (!any(gap)) next
      val <- deployments[[cc]][mm[gap]]
      summary_table[[cc]][gap] <- if (inherits(summary_table[[cc]], "POSIXt")) {
        if (inherits(deployments[[cc]], "POSIXt")) val else next
      } else if (is.numeric(summary_table[[cc]])) .asNumericSafe(val) else as.character(val)
    }
  }

  # order by the roster (deployment order); any processed ids not in the roster trail at the end
  ord <- c(match(rid[rid %in% summary_table$id], summary_table$id),
           which(!summary_table$id %in% rid))
  summary_table <- summary_table[ord, , drop = FALSE]
  rownames(summary_table) <- NULL

  summary_table
}


################################################################################
# nautilus_summary class + print method ########################################
################################################################################

#' A 0-row data frame carrying the exact column schema (and types) that `.summarize` produces, used to
#' seed an empty summary so the typed contract and roster completion still work when no deployment yields a
#' row. Keep in sync with the `data.frame(...)` at the end of `.summarize`.
#' @keywords internal
#' @noRd
.summaryTemplate <- function() {
  ps <- as.POSIXct(character(0), tz = "UTC")
  data.frame(id = character(0), animal_id = character(0), tag_model = character(0), tag_type = character(0), attachment_site = character(0),
             record_start = ps, record_end = ps, record_duration_h = numeric(0), n_samples = integer(0),
             sampling_hz = numeric(0), depth_mean = numeric(0), depth_max = numeric(0), temp_mean = numeric(0),
             temp_min = numeric(0), temp_max = numeric(0), vedba_mean = numeric(0), odba_mean = numeric(0),
             tbf_mean = numeric(0), tbf_method = character(0), pct_swimming = numeric(0),
             paddle_wheel = logical(0), speed_mean = numeric(0),
             speed_max = numeric(0), descent_rate_max = numeric(0), ascent_rate_max = numeric(0),
             n_positions = integer(0), stringsAsFactors = FALSE, check.names = FALSE)
}

#' Construct a nautilus_summary (a typed per-deployment summary data frame).
#' @keywords internal
#' @noRd
.newSummary <- function(df, error.stat = "sd") {
  if (is.null(df)) df <- data.frame()
  attr(df, "error.stat") <- error.stat
  class(df) <- c("nautilus_summary", "data.frame")
  df
}


#' The grouping column as an ordered factor, with the missing rows kept as their own trailing group.
#'
#' A deployment whose grouping value is unknown is a fact about the cohort, not a row to drop, so it
#' becomes an explicit "(missing)" group placed last. A factor keeps the level order it was given -
#' that is usually why it is a factor - and anything else sorts naturally; unused levels are dropped so
#' an empty group never renders.
#' @keywords internal
#' @noRd
.summaryGroupFactor <- function(v, missing.label = "(missing)") {
  na <- is.na(v)
  lv <- if (is.factor(v)) levels(droplevels(v[!na])) else sort(unique(as.character(v[!na])))
  ch <- as.character(v); ch[na] <- missing.label
  factor(ch, levels = c(lv, if (any(na)) missing.label))
}

#' One `mean +/- error` footer row over a set of deployments.
#'
#' Shared by the ungrouped footer and the per-group ones, so the two cannot drift apart. Where the error
#' is not finite - a group of one, or a column with a single non-missing value - the cell carries the
#' mean alone rather than a mean beside an empty interval.
#' @keywords internal
#' @noRd
.summaryFooterRow <- function(sub, agg_cols, prec_of, errfun, pm, err_stat, cols) {
  foot <- stats::setNames(rep(NA_character_, length(cols)), cols)
  for (nm in agg_cols) {
    m <- mean(sub[[nm]], na.rm = TRUE)
    if (is.finite(m)) {
      e <- errfun(sub[[nm]])
      foot[nm] <- if (is.finite(e))
        sprintf(paste0("%.", prec_of(nm), "f ", pm, " %.", prec_of(nm), "f"), m, e)
      else sprintf(paste0("%.", prec_of(nm), "f"), m)
    }
  }
  foot[["id"]] <- paste0("mean ", pm, " ", err_stat)
  as.data.frame(as.list(foot), stringsAsFactors = FALSE, check.names = FALSE)
}

#' Format a nautilus_summary as a display-ready character data frame
#'
#' Returns the same formatted table the print method renders - fixed per-metric precision, datetimes as
#' `dd/Mon/YYYY HH:MM`, missing values as "-", and (optionally) the display-only population
#' `mean +/- error` row - but as a STRUCTURED character data frame, so the formatted summary can be
#' exported directly for reporting, e.g. `write.csv(format(summary), "summary.csv", row.names = FALSE)`.
#'
#' With `group.by`, rows are ordered by a categorical column and each group gains its own
#' `mean +/- error` row, which is the quickest way to compare analysed against excluded deployments, or
#' one class of animal against another. The result stays a rectangle of one row per deployment plus one
#' footer per group: the blank line that separates groups on the console is inserted when printing, so
#' an exported file never carries an empty record.
#' @param x A `nautilus_summary` object.
#' @param style Column-name style: `"internal"` (default) keeps the programmatic snake_case names used
#'   throughout the API; `"report"` relabels the columns with human-readable, publication-ready headers
#'   (e.g. `depth_max` -> "Max depth (m)") for a report or paper; `"concise"` uses the same publication
#'   headers but abbreviated for width-constrained tables (e.g. `record_start` -> "Start",
#'   `attachment_site` -> "Attach. site", `tbf_mean` -> "Mean TBF (Hz)"). Values and layout are identical
#'   across styles - only the column names differ. Covariate columns from `extra.metadata` keep their own
#'   names (lightly prettified).
#' @param datetime.format Character. The \code{\link[base]{strftime}} format for the `record_start` /
#'   `record_end` datetime columns. Default `"%d/%b/%Y %H:%M"` (e.g. `01/Jan/2020 00:00`).
#' @param include.summary.row Logical. Append the display-only population `mean +/- error` row (only
#'   meaningful with more than one deployment). Default `TRUE` (matches the console). Set `FALSE` for a
#'   pure per-deployment table.
#'
#'   Numeric biometric traits and `extra.metadata` covariates are averaged in this row like any other
#'   metric. If a column should not be treated as a continuous variable - an identifier, a code, a year -
#'   supply it as character or factor rather than numeric, and it is left out of the row.
#' @param decimals Optional per-column override of the display precision, as a named numeric vector of
#'   decimal places - `c(video_duration_h = 1, depth_max = 0)`. Merged OVER the built-in precision, so
#'   naming one column leaves every other exactly as it was, and both the value and the
#'   `mean +/- error` row follow the override. Named by the INTERNAL column names (the ones
#'   `style = "internal"` shows and `@return` lists), not by the rendered headers: a header belongs to a
#'   style - `video_duration_h` reads "Video recorded (h)" under `report` and "Video (h)" under
#'   `concise` - so an override keyed on one would quietly stop applying when the style changed. Pass a
#'   header by mistake and the error names the column to use instead. Default `NULL`.
#' @param group.by Optional column name to group the rendering by - `"status"` to compare analysed
#'   against excluded deployments, or any categorical column in the table, such as a trait requested
#'   through `summarizeTagData(metadata = )`. Rows are ordered by group and each group is given its own
#'   `mean +/- error` row. Default `FALSE`, which renders the table ungrouped.
#'
#'   Grouping changes only the presentation: the `nautilus_summary` itself is one row per deployment
#'   either way. Named by the INTERNAL column name, as `decimals` is, so an override keeps working when
#'   the style changes. The returned table stays rectangular - the blank line between groups belongs to
#'   the console rendering, not to the exported object.
#' @param symbols Whether the rendered table may use typographic symbols: `"ascii"` (default) writes
#'   `+/-`, `deg C` and `m/s`; `"unicode"` writes the plus-minus, degree and superscript forms. ASCII is
#'   the default because this table is usually written to a file, and a spreadsheet opening a UTF-8 CSV
#'   with no byte-order mark guesses the encoding - on macOS it guesses MacRoman and renders the degree
#'   sign as two characters. It also keeps the column names typeable and indexable from a script.
#' @param ... Unused.
#' @return A character `data.frame` - the formatted table (0-row for an empty summary).
#' @exportS3Method format nautilus_summary

format.nautilus_summary <- function(x, style = c("internal", "report", "concise"),
                                    datetime.format = "%d/%b/%Y %H:%M", include.summary.row = TRUE,
                                    symbols = c("ascii", "unicode"), decimals = NULL,
                                    group.by = FALSE, ...) {
  style <- match.arg(style)
  symbols <- match.arg(symbols)
  .assert_string(datetime.format, "datetime.format")
  df <- as.data.frame(x)
  if (nrow(df) == 0 || ncol(df) == 0) return(data.frame())

  ## ---- grouping: order the rows, and remember which group each one belongs to --------------------
  # Ordering happens here, before the display table is built, so the character rows and the numeric
  # rows the footers are computed from stay in step.
  grp <- NULL
  if (!isFALSE(group.by)) {
    .assert_string(group.by, "group.by")
    if (!group.by %in% names(df))
      .abort(c("{.arg group.by} does not name a column of the summary: {.val {group.by}}.",
               "i" = "Available columns: {.val {names(df)}}."))
    grp <- .summaryGroupFactor(df[[group.by]])
    ord <- order(as.integer(grp), seq_along(grp))     # stable: incoming order kept within a group
    df <- df[ord, , drop = FALSE]; grp <- grp[ord]
  }

  dec <- .assert_decimals(decimals, df, style)
  err_stat <- attr(x, "error.stat") %||% "sd"
  # ASCII by default, and NOT gated on cli::is_utf8_output(). That gate asks whether the TERMINAL can
  # render a glyph, which is the right question for print() and the wrong one for a value headed to a
  # file: the same script, data and package version wrote different CSV bytes depending on whether the
  # session had a UTF-8 locale, which is not reproducible output. It also gated only the plus-minus, so
  # a non-UTF-8 terminal still received a degree sign in the header next to a "+/-" in the body.
  pm <- if (identical(symbols, "unicode")) "\u00b1" else "+/-"
  errfun <- if (err_stat == "se") function(v) stats::sd(v, na.rm = TRUE) / sqrt(sum(is.finite(v)))
            else function(v) stats::sd(v, na.rm = TRUE)

  # fixed, predictable display precision per metric (default 2 dp for anything unlisted)
  prec_map <- c(record_duration_h = 1, sampling_hz = 0,
                deploy_lon = 4, deploy_lat = 4, popup_lon = 4, popup_lat = 4, video_duration_h = 2,
                depth_mean = 1, depth_max = 1, temp_mean = 1, temp_min = 1, temp_max = 1,
                vedba_mean = 3, odba_mean = 3, tbf_mean = 2, pct_swimming = 1, speed_mean = 2, speed_max = 2,
                descent_rate_max = 2, ascent_rate_max = 2, n_samples = 0, n_positions = 0,
                n_dives = 0, dive_duration_median_min = 1, dive_duration_max_min = 1,
                dive_depth_median_m = 1, dive_depth_max_m = 1,
                dives_incomplete = 0, dives_truncated = 0, dives_gapped = 0)
  # User overrides MERGE over the defaults rather than replacing them, so naming one column leaves every
  # other column - and the 2 dp fallback for traits and covariates - exactly as it was.
  prec_map[names(dec)] <- dec
  prec_of <- function(nm) { p <- unname(prec_map[nm]); if (is.na(p)) 2L else as.integer(p) }

  num_cols <- names(df)[vapply(df, is.numeric, logical(1))]
  # columns where a cross-individual mean is meaningless: the per-tag acquisition constant, and the
  # tagging coordinates - averaging those gives the centroid of the study area, which is a real
  # quantity but not the one a "mean +/- sd" row in a deployment table is read as.
  agg_cols <- setdiff(num_cols, c("sampling_hz", "deploy_lon", "deploy_lat", "popup_lon", "popup_lat"))

  # character display table: datetimes formatted, numerics rounded to their precision
  disp <- as.data.frame(lapply(names(df), function(nm) {
    col <- df[[nm]]
    if (inherits(col, "POSIXt")) format(col, datetime.format, tz = "UTC")
    else if (nm %in% num_cols) ifelse(is.na(col), NA_character_, sprintf(paste0("%.", prec_of(nm), "f"), col))
    else as.character(col)
  }), stringsAsFactors = FALSE)
  names(disp) <- names(df)

  # Display-only mean +/- error footers. Ungrouped, one over the whole cohort, and only where there is
  # more than one deployment to average. Grouped, one per group - including a group of one, which shows
  # its mean alone: dropping it would make the groups look inconsistent rather than sparse.
  row_grp <- if (is.null(grp)) NULL else as.character(grp)
  if (isTRUE(include.summary.row)) {
    if (is.null(grp)) {
      if (nrow(df) > 1)
        disp <- rbind(disp, .summaryFooterRow(df, agg_cols, prec_of, errfun, pm, err_stat, names(df)))
    } else {
      out <- disp[0, , drop = FALSE]; tag <- character(0)
      for (lv in levels(grp)) {
        idx <- which(grp == lv)
        if (!length(idx)) next
        f <- .summaryFooterRow(df[idx, , drop = FALSE], agg_cols, prec_of, errfun, pm, err_stat,
                               names(df))
        # the footer carries its own group value, so an exported table stays self-describing: footers
        # are identifiable by the id cell and attributable by the grouping column, with no parsing
        if (!identical(group.by, "id")) f[[group.by]] <- lv
        out <- rbind(out, disp[idx, , drop = FALSE], f)
        tag <- c(tag, rep(lv, length(idx) + 1L))
      }
      disp <- out; row_grp <- tag
    }
  }
  rownames(disp) <- NULL
  disp[is.na(disp)] <- "-"
  if (style != "internal") names(disp) <- .summaryHeaders(names(disp), style)
  if (identical(symbols, "ascii")) names(disp) <- .foldSymbols(names(disp))
  # Which group each OUTPUT row belongs to, for print() to break on. An attribute rather than a blank
  # separator row: `format()` is the documented export route, and an empty record reads as a malformed
  # row in a spreadsheet or on re-import. Attributes do not reach write.csv() at all.
  if (!is.null(row_grp)) attr(disp, "summary.groups") <- row_grp
  disp
}


#' Replace the typographic symbols a header may carry with ASCII spellings.
#'
#' Applied to headers on the way out of `format()` rather than kept out of the dictionaries, so the
#' dictionaries stay readable and one function owns every substitution. A degree sign in a column NAME
#' is the worst of these: besides surviving no encoding guess, it makes the column awkward to reference
#' - `x[["Mean temp. (deg C)"]]` can be typed on any keyboard and pasted into a script that must remain
#' ASCII, the degree-sign spelling cannot.
#' @keywords internal
#' @noRd
.foldSymbols <- function(s) {
  s <- gsub("\u00b0C", "deg C", s, fixed = TRUE)
  s <- gsub("m s\u207b\u00b9", "m/s", s, fixed = TRUE)
  s <- gsub("(\u00b0)", "(deg)", s, fixed = TRUE)   # a bare unit, not a value: no leading space
  s <- gsub("\u00b0", " deg", s, fixed = TRUE)
  s <- gsub("\u00b1", "+/-", s, fixed = TRUE)
  s <- gsub("\u207b\u00b9", "^-1", s, fixed = TRUE)
  s
}


#' The deployment-metadata fields the summary can surface, declared ONCE.
#'
#' Each entry says where the field lives on a tag's own metadata AND which column of a
#' `nautilus_deployments` roster carries the same thing. Both readers walk this list, which is what
#' makes a deployment that never produced data get exactly the columns a processed one gets: the
#' alternative - two hand-written lists - is why a non-recovered tag used to come back with four fields
#' out of thirty and no tagging date, when the roster had known it all along.
#'
#' `recovery_datetime` and `tag_format` are deliberately absent: `metadataColumns()` maps them, but
#' nothing carries them onto the tag object, so they can only ever be roster-side and would be NA for
#' every processed deployment.
#' @keywords internal
#' @noRd
.summaryMetaFields <- function() list(
  deploy_datetime = list(get = function(m) m$deployment$datetime,        roster = "deploy_datetime", type = "time"),
  deploy_site     = list(get = function(m) m$deployment$site,            roster = "deploy_site",     type = "chr"),
  deploy_lon      = list(get = function(m) m$deployment$lon,             roster = "deploy_lon",      type = "num"),
  deploy_lat      = list(get = function(m) m$deployment$lat,             roster = "deploy_lat",      type = "num"),
  popup_datetime  = list(get = function(m) m$deployment$popup_datetime,  roster = "popup_datetime",  type = "time"),
  popup_lon       = list(get = function(m) m$deployment$popup_lon,       roster = "popup_lon",       type = "num"),
  popup_lat       = list(get = function(m) m$deployment$popup_lat,       roster = "popup_lat",       type = "num"),
  deployment_type = list(get = function(m) m$deployment$deployment_type, roster = "deployment_type", type = "chr"),
  attachment_site = list(get = function(m) m$deployment$attachment_site, roster = "attachment_site", type = "chr"),
  package_id      = list(get = function(m) m$tag$package_id,             roster = "package_id",      type = "chr"),
  logger_id       = list(get = function(m) m$tag$logger_id,              roster = "logger_id",       type = "chr"),
  axis_config     = list(get = function(m) m$tag$axis_config,            roster = "axis_config",     type = "chr"))

#' The keyword shorthands for `metadata =`.
#' @keywords internal
#' @noRd
.summaryMetaSets <- function() list(
  none     = character(0),
  standard = c("deploy_datetime", "deploy_site", "deploy_lon", "deploy_lat"),
  all      = setdiff(names(.summaryMetaFields()), "attachment_site"))   # already in the identity block

#' Resolve `metadata =` into the fields and traits to emit.
#'
#' Returns `traits = NA_character_` for "every trait the cohort happens to carry" (the keyword forms)
#' and an explicit character vector when the caller named them, which is the escape hatch that pins the
#' schema across cohorts that recorded different traits.
#' @keywords internal
#' @noRd
.summaryResolveMetadata <- function(metadata) {
  if (!is.character(metadata) || !length(metadata) || anyNA(metadata))
    .abort(c("{.arg metadata} must be a character vector with no {.val NA}.",
             "i" = "One of {.val none}, {.val standard}, {.val all}, or field/trait names."))
  sets  <- .summaryMetaSets()
  known <- names(.summaryMetaFields())
  if (length(metadata) == 1L && metadata %in% names(sets))
    return(list(fields = sets[[metadata]],
                traits = if (identical(metadata, "none")) character(0) else NA_character_))
  hit <- intersect(metadata, names(sets))
  if (length(hit))
    .abort(c("{.arg metadata} mixes the keyword{?s} {.val {hit}} with field names.",
             "i" = "Pass a single keyword, or list the fields and traits you want."))
  # anything not a known field is taken to name a trait - traits are user-defined at import, so the
  # package cannot hold a vocabulary for them
  list(fields = known[known %in% metadata], traits = setdiff(metadata, known))
}

#' Read the requested metadata fields off one tag's metadata.
#' @keywords internal
#' @noRd
.summaryMetaRow <- function(meta, fields, traits) {
  spec <- .summaryMetaFields()
  cast <- function(v, type) switch(type,
    time = { v <- v %||% NA; if (length(v) != 1 || !inherits(v, "POSIXt")) .POSIXct(NA_real_, tz = "UTC") else v },
    num  = { v <- .asNumericSafe(v %||% NA_real_); if (length(v) != 1) NA_real_ else v },
    { v <- v %||% NA_character_; if (length(v) != 1) NA_character_ else as.character(v) })
  out <- lapply(fields, function(f) cast(spec[[f]]$get(meta), spec[[f]]$type))
  names(out) <- fields
  bio <- meta$biometrics %||% list()
  # Only traits this tag actually carries. A requested-but-absent one is NOT filled in here: leaving the
  # column off makes "no deployment carried it" visible once, after binding, where it can be reported -
  # emitting NA per row instead made a typo indistinguishable from a trait nobody recorded.
  keep <- if (length(traits) == 1L && is.na(traits)) names(bio) else intersect(traits, names(bio))
  for (tn in keep) {
    v <- bio[[tn]]
    if (is.null(v) || length(v) != 1) next
    out[[tn]] <- if (is.numeric(v) || is.logical(v)) v else as.character(v)
  }
  out
}


#' Publication-ready column headers for a nautilus_summary (used by format(style = "report"/"concise")).
#'
#' Maps the internal snake_case names to human-readable headers with units - full ("report") or
#' abbreviated ("concise", for width-constrained tables). Unmapped columns (e.g. `extra.metadata`
#' covariates) fall back to a light "Sentence case" prettifier, so covariates that are already readable
#' pass through largely unchanged.
#' @keywords internal
#' @noRd
.summaryHeaders <- function(cols, style = "report") {
  report <- c(
    id = "ID", animal_id = "Animal ID",
    tag_model = "Tag model", tag_type = "Tag type", attachment_site = "Attachment site",
    status = "Status", status_reason = "Exclusion reason",
    deploy_datetime = "Tagging date", deploy_site = "Tagging site",
    deploy_lon = "Tagging longitude (\u00b0)",
    deploy_lat = "Tagging latitude (\u00b0)", popup_datetime = "Pop-up date",
    popup_lon = "Pop-up longitude (\u00b0)", popup_lat = "Pop-up latitude (\u00b0)",
    deployment_type = "Deployment type", package_id = "Package ID", logger_id = "Logger ID",
    axis_config = "Axis configuration", video_duration_h = "Video recorded (h)",
    record_start = "Record start", record_end = "Record end",
    record_duration_h = "Recording duration (h)", n_samples = "Samples (n)", sampling_hz = "Sampling rate (Hz)",
    depth_mean = "Mean depth (m)", depth_max = "Max depth (m)",
    temp_mean = "Mean temp. (\u00b0C)", temp_min = "Min temp. (\u00b0C)", temp_max = "Max temp. (\u00b0C)",
    vedba_mean = "Mean VeDBA (g)", odba_mean = "Mean ODBA (g)",
    tbf_mean = "Mean tail-beat freq. (Hz)", tbf_method = "TBF backend", pct_swimming = "Time swimming (%)", paddle_wheel = "Paddle wheel",
    speed_mean = "Mean speed (m/s)", speed_max = "Max speed (m/s)",
    descent_rate_max = "Max descent rate (m/s)", ascent_rate_max = "Max ascent rate (m/s)",
    n_positions = "Positions (n)",
    n_dives = "Dives (n)", dive_duration_median_min = "Median dive duration (min)",
    dive_duration_max_min = "Max dive duration (min)", dive_depth_median_m = "Median dive depth (m)",
    dive_depth_max_m = "Max dive depth (m)", dives_incomplete = "Incomplete dives (n)",
    dives_truncated = "Boundary-truncated dives (n)", dives_gapped = "Gap-interrupted dives (n)")
  concise <- c(
    id = "ID", animal_id = "Animal",
    tag_model = "Tag model", tag_type = "Tag type", attachment_site = "Attach. site",
    status = "Status", status_reason = "Reason",
    deploy_datetime = "Tagged", deploy_site = "Site",
    deploy_lon = "Lon (\u00b0)", deploy_lat = "Lat (\u00b0)",
    popup_datetime = "Pop-up", popup_lon = "Pop-up lon (\u00b0)", popup_lat = "Pop-up lat (\u00b0)",
    deployment_type = "Deploy. type", package_id = "Package", logger_id = "Logger",
    axis_config = "Axis config", video_duration_h = "Video (h)",
    record_start = "Rec. start", record_end = "Rec. end",
    record_duration_h = "Duration (h)", n_samples = "Samples (n)", sampling_hz = "Rate (Hz)",
    depth_mean = "Mean depth (m)", depth_max = "Max depth (m)",
    temp_mean = "Mean temp. (\u00b0C)", temp_min = "Min temp. (\u00b0C)", temp_max = "Max temp. (\u00b0C)",
    vedba_mean = "Mean VeDBA (g)", odba_mean = "Mean ODBA (g)",
    tbf_mean = "Mean TBF (Hz)", tbf_method = "TBF backend", pct_swimming = "Swimming (%)", paddle_wheel = "Paddle wheel",
    speed_mean = "Mean speed (m s\u207b\u00b9)", speed_max = "Max speed (m s\u207b\u00b9)",
    descent_rate_max = "Max descent (m s\u207b\u00b9)", ascent_rate_max = "Max ascent (m s\u207b\u00b9)",
    n_positions = "Positions (n)",
    n_dives = "Dives (n)", dive_duration_median_min = "Med. dive dur. (min)",
    dive_duration_max_min = "Max dive dur. (min)", dive_depth_median_m = "Med. dive depth (m)",
    dive_depth_max_m = "Max dive depth (m)", dives_incomplete = "Incompl. dives (n)",
    dives_truncated = "Truncated dives (n)", dives_gapped = "Gapped dives (n)")
  dict <- if (identical(style, "concise")) concise else report
  out <- unname(dict[cols])
  miss <- is.na(out)
  if (any(miss)) out[miss] <- vapply(cols[miss], function(s) {
    s <- gsub("_", " ", s); paste0(toupper(substring(s, 1, 1)), substring(s, 2))
  }, character(1))
  out
}


#' Print method for a nautilus_summary
#'
#' Renders the formatted table (via \code{\link[=format.nautilus_summary]{format}}) plus a one-line
#' banner; for more than one deployment the formatted table carries a display-only `mean +/- error` row.
#' The underlying object stays numeric/POSIXct - this only affects what is shown. To export the formatted
#' table, use `format(x)` (e.g. `write.csv(format(x), file, row.names = FALSE)`).
#' @param x A `nautilus_summary` object.
#' @param ... Passed to \code{\link[=format.nautilus_summary]{format}}, so the display can be tuned in
#'   place - `print(x, decimals = c(video_duration_h = 1))` - before settling on an export.
#' @return `x`, invisibly. Called for the printed output.
#' @exportS3Method print nautilus_summary

print.nautilus_summary <- function(x, ...) {
  df <- as.data.frame(x)
  if (nrow(df) == 0) { cat("<nautilus_summary> 0 deployments\n"); return(invisible(x)) }
  # The console IS the right place to ask what the terminal can render - and to ask it ONCE, for the
  # banner and the table together. Gating only the banner left a "+/-" beside a header still carrying a
  # degree sign on a terminal cli had just declared incapable of UTF-8.
  uni <- cli::is_utf8_output()
  pm <- if (uni) "\u00b1" else "+/-"
  err_stat <- attr(x, "error.stat") %||% "sd"
  fmt <- format(x, symbols = if (uni) "unicode" else "ascii", ...)
  rg  <- attr(fmt, "summary.groups")
  gb  <- list(...)$group.by

  banner <- if (!is.null(rg) && !is.null(gb))
    sprintf(" (grouped by %s; one mean %s %s row per group)", gb, pm, err_stat)
  else if (nrow(df) > 1) sprintf(" (final row: mean %s %s)", pm, err_stat) else ""
  cat(sprintf("<nautilus_summary> %d deployment%s%s\n", nrow(df), if (nrow(df) != 1) "s" else "",
              banner))

  # The blank line between groups is inserted HERE, at render time, rather than returned by format():
  # a reader wants the break, an exported file does not want an empty record.
  if (!is.null(rg) && length(rg) == nrow(fmt) && length(unique(rg)) > 1L) {
    txt <- utils::capture.output(print(fmt, row.names = FALSE))
    hdr <- length(txt) - nrow(fmt)                       # header lines precede the body
    brk <- which(rg[-length(rg)] != rg[-1])              # last row of each group but the final one
    for (i in rev(brk)) txt <- append(txt, "", after = hdr + i)
    writeLines(txt)                                      # terminates each line, and adds none
  } else {
    print(fmt, row.names = FALSE)
  }
  invisible(x)
}


#######################################################################################################
#######################################################################################################
#######################################################################################################


#' The dive block's column names, in the documented order. Declared once so the block can be told apart
#' from a biometric trait, both of which are absent from the base template.
#' @keywords internal
#' @noRd
.summaryDiveCols <- function()
  c("n_dives", "dive_duration_median_min", "dive_duration_max_min", "dive_depth_median_m",
    "dive_depth_max_m", "dives_incomplete", "dives_truncated", "dives_gapped")


#' The order the summary table is presented in, declared as blocks.
#'
#' A deployment table is read as a narrative - which deployment, on which animal, with which tag, put
#' out where and when, what record came back and whether it was kept, how much data there is, what the
#' animal experienced, what it did - and the columns are ordered to follow it. Declared here rather
#' than assembled incrementally, because the order previously fell out of the sequence in which blocks
#' happened to be cbind-ed and appended, which put `status_reason` thirty columns from `status` and the
#' external covariates ahead of the animal they describe.
#'
#' Anything not named here keeps its relative order and trails the block it follows, so a column added
#' later appears rather than disappearing.
#' @keywords internal
#' @noRd
.summaryBlocks <- function() list(
  identity = c("id", "animal_id"),
  animal   = character(0),                    # declared traits + extra.metadata covariates, filled in
  tag      = c("tag_model", "tag_type", "attachment_site", "paddle_wheel"),
  deploy   = character(0),                    # the requested metadata fields, in vocabulary order
  record   = c("record_start", "record_end", "record_duration_h", "status", "status_reason"),
  coverage = c("n_samples", "sampling_hz", "n_positions", "video_duration_h"),
  habitat  = c("depth_mean", "depth_max", "temp_mean", "temp_min", "temp_max"),
  movement = c("vedba_mean", "odba_mean", "tbf_mean", "tbf_method", "pct_swimming",
               "speed_mean", "speed_max", "descent_rate_max", "ascent_rate_max"),
  dives    = .summaryDiveCols())


#' Put every column in its block, materialising the ones an argument asked for.
#'
#' `rbindlist(fill = TRUE)` unions the columns of a ragged cohort, so a trait first seen on deployment
#' seventeen lands wherever it lands, and two calls on the same animals must not produce differently
#' ordered tables. Explicitly named traits are also MATERIALISED when no deployment carried them, which
#' is what lets two cohorts that recorded different traits still bind together.
#' @keywords internal
#' @noRd
.summaryOrderMeta <- function(tbl, meta.req, covariates = character(0)) {
  fields <- meta.req$fields
  traits <- meta.req$traits
  blocks <- .summaryBlocks()
  known  <- unlist(blocks, use.names = FALSE)
  if (length(traits) == 1L && is.na(traits)) {
    # Whatever the cohort carried. Inferred by exclusion, so everything the summary itself produces has
    # to be excluded - the declared blocks AND the metadata fields, which are not traits.
    traits <- setdiff(names(tbl), c(known, fields, covariates))
  } else {
    # Named but carried by no deployment. The column is still created - that is the whole point of
    # naming traits explicitly - but a silent all-NA column is indistinguishable from a typo, and the
    # package cannot hold a vocabulary for user-declared traits to check the spelling against.
    absent <- setdiff(traits, names(tbl))
    for (tn in absent) tbl[[tn]] <- NA
    if (length(absent))
      warning(sprintf("summarizeTagData: no deployment carries the requested trait(s) %s - column(s) added as all-NA (check the spelling, or metadataColumns(traits = ) at import).",
                      paste(sprintf("'%s'", absent), collapse = ", ")), call. = FALSE)
  }
  # Requested FIELDS are materialised whether or not any deployment carried them, so column presence
  # follows the argument and nothing else - the fixed-schema promise. Without this an all-excluded
  # cohort came back with no metadata columns for .completeRoster() to fill.
  spec <- .summaryMetaFields()
  for (f in setdiff(fields, names(tbl)))
    tbl[[f]] <- switch(spec[[f]]$type,
                       time = .POSIXct(rep(NA_real_, nrow(tbl)), tz = "UTC"),
                       num  = rep(NA_real_, nrow(tbl)),
                       rep(NA_character_, nrow(tbl)))

  blocks$animal <- c(traits, covariates)
  blocks$deploy <- fields
  ord <- unlist(blocks, use.names = FALSE)
  ord <- intersect(ord, names(tbl))
  tbl[, c(ord, setdiff(names(tbl), ord)), drop = FALSE]
}


#' Validate a `video.metadata` table (the getVideoMetadata() contract this function relies on).
#' @keywords internal
#' @noRd
.assert_video_metadata <- function(v) {
  if (is.null(v)) return(invisible(NULL))
  if (!is.data.frame(v))
    .abort("{.arg video.metadata} must be a data frame as returned by {.fn getVideoMetadata}.")
  miss <- setdiff(c("ID", "duration"), names(v))
  if (length(miss))
    .abort(c("{.arg video.metadata} is missing the column{?s} {.field {miss}}.",
             "i" = "Pass the table {.fn getVideoMetadata} returns, unaggregated."))
  if (!is.numeric(v[["duration"]]))
    .abort("{.field duration} in {.arg video.metadata} must be numeric (seconds).")
  invisible(NULL)
}


#' Total recorded video per deployment, in hours.
#'
#' getVideoMetadata() returns one row per FILE, so the totalling belongs here rather than in the
#' caller's script: handed the raw table, the generic covariate join would collapse the several
#' durations of one deployment into the string "403.4/404.1/404.7" and then turn it into NA.
#'
#' NA, never 0, for a deployment with no entry. A folder holding no video is dropped by
#' getVideoMetadata() entirely, so absence in this table means "no footage was found", which is not the
#' same claim as "the camera ran for zero hours", and only one of those is safe to average.
#'
#' This is TOTAL footage, which routinely exceeds the retained record window - a camera started on deck
#' keeps recording either side of the deployment (measured on PIN_CAM_06: 0.90 h of video against a
#' 0.54 h overlap). It answers "how much video is there", not "how much video covers analysed data".
#' @keywords internal
#' @noRd
.attachVideoDuration <- function(tbl, video.metadata) {
  if (is.null(video.metadata) || !nrow(tbl)) return(tbl)
  v <- as.data.frame(video.metadata)
  ids <- as.character(v[["ID"]])
  secs <- .asNumericSafe(v[["duration"]])
  ok <- !is.na(ids) & is.finite(secs)
  tot <- if (any(ok)) tapply(secs[ok], ids[ok], sum) else numeric(0)
  tbl$video_duration_h <- unname(as.numeric(tot[match(tbl$id, names(tot))]) / 3600)
  if (!any(tbl$id %in% names(tot)))
    warning("summarizeTagData: no 'video.metadata' ID matches any deployment - video_duration_h is all NA (check the ID values).",
            call. = FALSE)
  tbl
}


#' Fill the record window of a deployment that filterDeploymentData() detected and then rejected.
#'
#' A deployment excluded for being shorter than `min.deployment.hours` is not a deployment nothing is
#' known about: a window WAS found, and the reason it was dropped is precisely that the window was
#' short. Reporting it as a bare "excluded" with no times throws away the one number that justifies the
#' decision. So its `record_start`, `record_end` and `record_duration_h` are filled from the exclusions
#' table, and `status_reason` says which rule set it aside.
#'
#' Only ever writes into rows whose metrics are absent - a deployment that survived filtering owns its
#' record window, and an exclusions table left over from an earlier run must not overwrite it.
#' @keywords internal
#' @noRd
.attachExclusions <- function(tbl, exclusions) {
  if (is.null(exclusions) || !nrow(tbl)) return(tbl)
  # One row per deployment: where it left the pipeline. A deployment carrying rows from several stages
  # left at the earliest of them - the later ones are decisions taken in an older run, before an
  # upstream stage started excluding it.
  ex <- .exclusionsResolve(.exclusionsRead(exclusions))
  if (is.null(ex) || !nrow(ex)) return(tbl)

  # The log describes the data products on disk, and running a stage refreshes both together - so a row
  # left by a stage that has not re-run still describes the data that stage last wrote. The one genuine
  # conflict is a deployment that is excluded AND present in the data handed in, which means the two
  # came from different runs.
  present <- intersect(tbl$id[!is.na(tbl$record_start)], ex$id)
  if (length(present))
    warning(sprintf(paste0("summarizeTagData: %d deployment%s excluded in the log but present in the ",
                           "data (%s). The log and the data are from different runs."),
                    length(present), if (length(present) != 1L) "s are" else " is",
                    paste(utils::head(present, 5), collapse = ", ")), call. = FALSE)

  m <- match(tbl$id, as.character(ex$id))
  hit <- !is.na(m)
  if (!any(hit)) {
    warning("summarizeTagData: no 'exclusions' ID matches any deployment - nothing filled (check the ID values).",
            call. = FALSE)
    return(tbl)
  }
  if (!"status_reason" %in% names(tbl)) tbl$status_reason <- NA_character_
  tbl$status_reason[hit] <- as.character(ex$reason)[m[hit]]

  # never overwrite a deployment that has its own record: only a row with no window gets one
  fill <- hit & is.na(tbl$record_start)
  if (any(fill) && all(c("window_start", "window_end") %in% names(ex))) {
    tbl$record_start[fill] <- ex$window_start[m[fill]]
    tbl$record_end[fill]   <- ex$window_end[m[fill]]
    tbl$record_duration_h[fill] <- ex$window_hours[m[fill]]
  }
  tbl
}


#' Validate `decimals =` and return it as a named integer vector (empty when nothing was asked for).
#'
#' Keyed on the INTERNAL column names, never the rendered headers, because a header is a property of
#' the style: `video_duration_h` reads "Video recorded (h)" under `report` and "Video (h)" under
#' `concise`, and `symbols = "ascii"` rewrites others again. An override keyed on what the table happens
#' to be called would silently stop applying the moment the style changed - not error, just quietly do
#' nothing, which is the worst way for a formatting argument to fail.
#'
#' That costs the caller some ergonomics, since the header is what they are looking at when they decide
#' a column needs another decimal place. So an unrecognised name is resolved BACK through the header
#' dictionaries: ask for `"Video (h)"` and the error names `video_duration_h` rather than listing thirty
#' valid columns.
#' @param df The table being formatted, so a name is checked against the columns that actually exist.
#' @param style The style being rendered, for the header-to-column suggestion.
#' @keywords internal
#' @noRd
.assert_decimals <- function(decimals, df, style) {
  if (is.null(decimals)) return(stats::setNames(integer(0), character(0)))
  if (!is.numeric(decimals) || !length(decimals))
    .abort(c("{.arg decimals} must be a named numeric vector, e.g. {.code c(video_duration_h = 1)}.",
             "i" = "Use the column names {.code format(x, style = \"internal\")} shows."))
  nms <- names(decimals)
  if (is.null(nms) || any(!nzchar(nms)) || anyNA(nms))
    .abort(c("Every element of {.arg decimals} must be named after a column.",
             "i" = "e.g. {.code decimals = c(video_duration_h = 1, depth_max = 0)}"))
  if (anyNA(decimals) || any(!is.finite(decimals)) || any(decimals < 0) || any(decimals != trunc(decimals)))
    .abort("{.arg decimals} must be whole numbers of decimal places, zero or more.")
  if (anyDuplicated(nms))
    .abort("{.arg decimals} names the column{?s} {.field {unique(nms[duplicated(nms)])}} more than once.")

  unknown <- setdiff(nms, names(df))
  if (length(unknown)) {
    # a header the caller read off the formatted table, mapped back to the column it came from
    hits <- unlist(lapply(c("report", "concise"), function(st) {
      h <- .foldSymbols(.summaryHeaders(names(df), st))
      stats::setNames(names(df), h)[intersect(unknown, h)]
    }))
    # the two styles share many headers verbatim ("Max depth (m)"), so the same suggestion arrives twice
    hits <- hits[!duplicated(names(hits))]
    hint <- if (length(hits))
      c("i" = "{.val {names(hits)}} {?is/are} a display header; use {.field {unname(hits)}}.")
    else
      c("i" = "Column names are the ones {.code format(x, style = \"internal\")} shows.")
    .abort(c("{.arg decimals} names {cli::qty(length(unknown))}column{?s} not in this summary: {.val {unknown}}.",
             hint))
  }
  bad <- nms[!vapply(df[nms], is.numeric, logical(1))]
  if (length(bad))
    .abort(c("{.arg decimals} sets a decimal place on the non-numeric column{?s} {.field {bad}}.",
             "i" = "Decimal places apply to numeric columns only."))
  stats::setNames(as.integer(decimals), nms)
}
