#######################################################################################################
# Summarize Tag Data ##################################################################################
#######################################################################################################

#' Summarise processed tag deployments
#'
#' @description
#' Builds a tidy, one-row-per-deployment summary of processed archival-tag data: the recording window,
#' key acquisition metadata (sampling rate, tag model), and headline environmental and kinematic metrics
#' (depth, temperature, activity, tail-beats, speed, vertical speed).
#' Descriptive fields are read directly from the consolidated tag metadata, so they do not need to be
#' supplied again; an optional `extra.metadata` table can attach external per-animal covariates (sex,
#' size, ...) that are not part of the tag metadata.
#'
#' The returned object is a **typed** data frame (numeric columns stay numeric, datetimes stay
#' POSIXct), meant for downstream analysis. Its `print` method renders a formatted table and, for more
#' than one deployment, appends a population `mean ± error` row for display only. To export that same
#' formatted table (for a report or paper), call `format()` on the result and write it out, e.g.
#' `write.csv(format(summary), "summary.csv", row.names = FALSE)`.
#'
#' @param data Processed deployments, in any of the forms used across the pipeline: a list of datasets
#'   (one per individual), a single aggregated data.table/data.frame with an `ID` column, or a character
#'   vector of `.rds` file paths (loaded lazily, one per deployment). The output of \link{processTagData}
#'   (optionally after \link{calculateTailBeats}) is expected.
#' @param extra.metadata Optional data frame of EXTERNAL per-animal covariates to attach (e.g. `sex`,
#'   `size`). Must contain an `ID` column. Multiple rows per ID are collapsed (distinct values joined
#'   by "/"). Fields already captured in the tag metadata (tag model/type, attachment site, sampling
#'   rate) are filled automatically and need not be provided here. This is also the route for any
#'   external per-deployment metric, e.g. total video duration from \link{getVideoMetadata}:
#'   `v <- aggregate(duration ~ ID, getVideoMetadata(...), sum); v$duration <- v$duration / 3600` and
#'   pass `v` (renamed) as a covariate. Default `NULL`.
#' @param deployments Optional `nautilus_deployments` object from \link{qcDeploymentMetadata}. When
#'   supplied, the summary is completed into the full study roster: every deployment in `deployments`
#'   gets a row, and a `status` column marks each as `"included"` (processed data present, full metrics)
#'   or `"excluded"` (in the roster but absent from `data` - NA metrics, identity filled from the
#'   roster). The reason for exclusion is not inferred here; see \link{qcIssues}. Default `NULL`.
#' @param error.stat Error statistic for the display-only population row: `"sd"` (standard deviation,
#'   default) or `"se"` (standard error).
#' @param verbose Verbosity: `FALSE`/`0`/"quiet" (silent), `TRUE`/`1`/"normal" (header + summary), or
#'   `2`/"detailed" (default): additionally reports per-metric coverage across the processed deployments
#'   and shows a live progress bar while the tags are read (cli auto-hides it for fast runs).
#'
#' @return A `nautilus_summary` data frame, one row per deployment, with (where available) the columns:
#' \itemize{
#'   \item \strong{id}, \strong{tag_model}, \strong{tag_type}, \strong{attachment_site}: identity and tag metadata.
#'   \item \strong{record_start}, \strong{record_end}, \strong{record_duration_h}, \strong{n_samples}: the
#'     recorded data span - the true on-animal window once the data have been deployment-filtered.
#'   \item \strong{sampling_hz}: original sampling rate (Hz).
#'   \item \strong{depth_mean}, \strong{depth_max} (m); \strong{temp_mean}, \strong{temp_min}, \strong{temp_max} (°C).
#'   \item \strong{vedba_mean}, \strong{odba_mean} (g): mean activity (dynamic body acceleration).
#'   \item \strong{tbf_mean} (Hz): mean tail-beat frequency over beating samples; \strong{pct_swimming}
#'     (\%): share of time actively swimming - both present only after \link{calculateTailBeats}.
#'   \item \strong{paddle_wheel} (logical): whether the tag carried a paddle wheel (disambiguates the speed columns).
#'   \item \strong{speed_mean}, \strong{speed_max} (m/s): paddle-wheel speed, when available.
#'   \item \strong{descent_rate_max}, \strong{ascent_rate_max} (m/s): fastest vertical speeds (descent positive).
#'   \item \strong{n_positions}: total number of position fixes within the record span, when available.
#'   \item \strong{status}: `"included"`/`"excluded"` - only when `deployments` is supplied (see that argument).
#' }
#'
#' @seealso \link{processTagData}, \link{filterDeploymentData}.
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
                             error.stat = "sd",
                             verbose = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  error.stat <- tolower(error.stat)
  .assert_choice(error.stat, "error.stat", c("sd", "se"))
  if (!is.null(deployments)) {
    if (!inherits(deployments, "nautilus_deployments"))
      .abort("{.arg deployments} must be a {.cls nautilus_deployments} object from {.fn qcDeploymentMetadata}.")
    .assert_columns(deployments, "id", "deployments")
  }
  if (!is.null(extra.metadata)) .assert_columns(extra.metadata, "ID", "extra.metadata")

  # resolve input: a list of processed datasets, a single aggregated data.frame (split by ID), or a
  # character vector of .rds file paths (loaded lazily) - consistent with the rest of the pipeline.
  r <- .resolveInput(data, id.col = "ID")

  .log_header(lvl, "summarizeTagData", "Summarising the deployments",
              bullets = sprintf("Input: %d tag%s", r$n, if (r$n != 1) "s" else ""))

  # per-deployment summaries (the same engine that backs summary.nautilus_tag). Empty/malformed deployments
  # come back NULL (.summarize warns per case); report the omissions here rather than letting them vanish.
  pb      <- .log_progress_start(lvl, r$n, "Summarising")           # live bar at detailed verbosity (lvl >= 2)
  parts   <- lapply(seq_len(r$n), function(i) { .log_progress_step(pb); .summarize(r$get(i)) })
  .log_progress_done(pb)
  dropped <- vapply(parts, is.null, logical(1))
  if (any(dropped))
    warning(sprintf("summarizeTagData: %d of %d deployment(s) were empty or malformed and omitted: %s",
                    sum(dropped), r$n, paste(r$ids[dropped], collapse = ", ")), call. = FALSE)
  summary_table <- if (any(!dropped)) do.call(rbind, parts[!dropped]) else .summaryTemplate()
  rownames(summary_table) <- NULL

  # optionally complete the study roster: every deployment in `deployments` gets a row - processed ones
  # carry full metrics + status "included"; the rest get NA metrics, identity filled from the QC roster,
  # and status "excluded" (works on an empty summary too, so an all-excluded roster is still produced).
  # Done BEFORE the covariate join so external covariates attach to every row.
  if (!is.null(deployments)) summary_table <- .completeRoster(summary_table, deployments)

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
        agg[[cc]] <- suppressWarnings(as.numeric(agg[[cc]]))
      }
      if (!length(intersect(agg$id, summary_table$id)))
        warning("summarizeTagData: no 'extra.metadata' ID matches any deployment - covariates attached as all-NA (check the ID column values/type).", call. = FALSE)
      ord_ids <- summary_table$id                                   # base merge() reorders rows; restore roster order
      summary_table <- merge(summary_table, agg, by = "id", all.x = TRUE, sort = FALSE)
      summary_table <- summary_table[match(ord_ids, summary_table$id), , drop = FALSE]
      rownames(summary_table) <- NULL
      summary_table <- summary_table[, c("id", cov_cols, setdiff(names(summary_table), c("id", cov_cols)))]
    }
  }

  if (lvl >= 1L) {
    .log_summary(lvl)
    # detailed: per-metric coverage across the processed deployments (+ the excluded ids, if a roster)
    if (lvl >= 2L) .summaryCoverageDetail(lvl, summary_table)
    if (!is.null(deployments) && "status" %in% names(summary_table)) {
      n_excl <- sum(summary_table$status == "excluded")
      .log_done(lvl, nrow(summary_table), " deployment", if (nrow(summary_table) != 1) "s",
                " in roster (", nrow(summary_table) - n_excl, " included, ", n_excl, " excluded)")
    } else {
      n_ok <- nrow(summary_table)
      .log_done(lvl, n_ok, " tag", if (n_ok != 1) "s", " summarised")
    }
    .log_runtime(lvl, start.time)
  }

  .newSummary(summary_table, error.stat)
}


#' Detailed-verbose diagnostic: how many processed deployments carry each optional metric group, plus
#' the excluded ids when the roster was completed. A cheap data-completeness check (e.g. spotting the
#' deployments that never had calculateTailBeats() run, or lack a magnetometer / positions).
#' @keywords internal
#' @noRd
.summaryCoverageDetail <- function(lvl, st) {
  incl <- if ("status" %in% names(st)) st[st$status == "included", , drop = FALSE] else st
  n <- nrow(incl)
  if (n == 0) return(invisible())
  cnt <- function(col) if (col %in% names(incl)) sum(is.finite(incl[[col]])) else 0L
  p <- if ("n_positions" %in% names(incl)) incl$n_positions else rep(NA_real_, n)
  npos <- sum(is.finite(p) & p > 0)
  sep <- if (cli::is_utf8_output()) " \u00b7 " else " | "     # ASCII fallback on non-UTF-8 consoles
  parts <- sprintf(c("depth %d", "temp %d", "activity %d", "tail-beats %d", "paddle %d", "positions %d"),
                   c(cnt("depth_max"), cnt("temp_max"), cnt("vedba_mean"), cnt("tbf_mean"), cnt("speed_mean"), npos))
  .log_detail(lvl, sprintf("metric coverage (of %d processed): %s", n, paste(parts, collapse = sep)))
  if ("status" %in% names(st)) {
    ex <- st$id[st$status == "excluded"]
    if (length(ex))
      .log_detail(lvl, sprintf("excluded (%d): %s", length(ex),
                               if (length(ex) > 12) paste0(paste(utils::head(ex, 12), collapse = ", "), sprintf(", ... (+%d more)", length(ex) - 12))
                               else paste(ex, collapse = ", ")))
  }
  invisible()
}


################################################################################
# Per-deployment summary (internal; reused by summary.nautilus_tag) ############
################################################################################

#' One-row, typed summary for a single deployment.
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.summarize <- function(data_individual) {

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
  s_num <- function(v) { v <- suppressWarnings(as.numeric(v %||% NA_real_)); if (length(v) != 1) NA_real_ else v }
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
  # tail-beat metrics (NA unless calculateTailBeats() has run). tbf_hz is NA while not beating, so its
  # mean is already over beating samples; pct_swimming is the share of time actively swimming.
  pct_swimming <- if ("tbf_swimming" %in% names(dt)) {
    sw <- dt[["tbf_swimming"]]; sw <- sw[!is.na(sw)]
    if (length(sw)) 100 * mean(as.numeric(sw)) else NA_real_
  } else NA_real_

  data.frame(
    id                    = id,
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
    tbf_mean              = cstat("tbf_hz", mean),
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
}


#' Complete the study roster: add a row for every deployment in `deployments`, with a status flag.
#'
#' Processed deployments keep their full metrics (status "included"); deployments present in the QC
#' roster but absent from the processed data get NA metrics, their identity filled from the roster, and
#' status "excluded". `status` is intentionally coarse: summarizeTagData cannot reliably attribute WHY a
#' deployment is absent (a QC error, a sensor exclusion, or a manual drop), so the detailed reasons stay
#' in qcIssues(). The `nautilus_deployments` table is normalised to canonical role names, so identity is
#' read straight off it (optional roles such as tag_type/attachment_site/paddle_wheel may be absent).
#' @keywords internal
#' @noRd
.completeRoster <- function(summary_table, deployments) {
  rid  <- as.character(deployments[["id"]])
  pick <- function(col) if (col %in% names(deployments)) deployments[[col]] else rep(NA, length(rid))
  lgl  <- function(v) {                          # NA-preserving (unknown paddle stays NA, not silently FALSE)
    if (is.character(v)) { out <- tolower(v) %in% c("true", "1", "yes", "y", "t"); out[is.na(v)] <- NA; out }
    else as.logical(v)
  }

  summary_table$status <- rep("included", nrow(summary_table))   # length-safe on an empty (all-excluded) table
  missing_ids <- setdiff(rid, summary_table$id)
  if (length(missing_ids)) {
    add <- summary_table[rep(NA_integer_, length(missing_ids)), , drop = FALSE]   # NA rows, types preserved
    rownames(add) <- NULL
    m <- match(missing_ids, rid)
    add$id <- missing_ids
    if ("tag_model" %in% names(add))       add$tag_model       <- as.character(pick("tag_model"))[m]
    if ("tag_type" %in% names(add))        add$tag_type        <- as.character(pick("tag_type"))[m]
    if ("attachment_site" %in% names(add)) add$attachment_site <- as.character(pick("attachment_site"))[m]
    if ("paddle_wheel" %in% names(add))    add$paddle_wheel    <- lgl(pick("paddle_wheel"))[m]
    add$status <- "excluded"
    summary_table <- rbind(summary_table, add)
  }

  # order by the roster (deployment order); any processed ids not in the roster trail at the end
  ord <- c(match(rid[rid %in% summary_table$id], summary_table$id),
           which(!summary_table$id %in% rid))
  summary_table <- summary_table[ord, , drop = FALSE]
  rownames(summary_table) <- NULL

  # place `status` right after the identity block
  ident <- intersect(c("id", "tag_model", "tag_type", "attachment_site"), names(summary_table))
  rest  <- setdiff(names(summary_table), c(ident, "status"))
  summary_table[, c(ident, "status", rest), drop = FALSE]
}


################################################################################
# nautilus_summary class + print method ########################################
################################################################################

#' A 0-row data frame carrying the exact column schema (and types) that \code{.summarize} produces, used to
#' seed an empty summary so the typed contract and roster completion still work when no deployment yields a
#' row. Keep in sync with the \code{data.frame(...)} at the end of \code{.summarize}.
#' @keywords internal
#' @noRd
.summaryTemplate <- function() {
  ps <- as.POSIXct(character(0), tz = "UTC")
  data.frame(id = character(0), tag_model = character(0), tag_type = character(0), attachment_site = character(0),
             record_start = ps, record_end = ps, record_duration_h = numeric(0), n_samples = integer(0),
             sampling_hz = numeric(0), depth_mean = numeric(0), depth_max = numeric(0), temp_mean = numeric(0),
             temp_min = numeric(0), temp_max = numeric(0), vedba_mean = numeric(0), odba_mean = numeric(0),
             tbf_mean = numeric(0), pct_swimming = numeric(0), paddle_wheel = logical(0), speed_mean = numeric(0),
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


#' Format a nautilus_summary as a display-ready character data frame
#'
#' Returns the same formatted table the print method renders - fixed per-metric precision, datetimes as
#' `dd/Mon/YYYY HH:MM`, missing values as "-", and (optionally) the display-only population
#' `mean +/- error` row - but as a STRUCTURED character data frame, so the formatted summary can be
#' exported directly for reporting, e.g. `write.csv(format(summary), "summary.csv", row.names = FALSE)`.
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
#' @param ... Unused.
#' @return A character `data.frame` - the formatted table (0-row for an empty summary).
#' @exportS3Method format nautilus_summary

format.nautilus_summary <- function(x, style = c("internal", "report", "concise"),
                                    datetime.format = "%d/%b/%Y %H:%M", include.summary.row = TRUE, ...) {
  style <- match.arg(style)
  .assert_string(datetime.format, "datetime.format")
  df <- as.data.frame(x)
  if (nrow(df) == 0 || ncol(df) == 0) return(data.frame())

  err_stat <- attr(x, "error.stat") %||% "sd"
  pm <- if (cli::is_utf8_output()) "\u00b1" else "+/-"           # locale-safe plus-minus marker
  errfun <- if (err_stat == "se") function(v) stats::sd(v, na.rm = TRUE) / sqrt(sum(is.finite(v)))
            else function(v) stats::sd(v, na.rm = TRUE)

  # fixed, predictable display precision per metric (default 2 dp for anything unlisted)
  prec_map <- c(record_duration_h = 1, sampling_hz = 0,
                depth_mean = 1, depth_max = 1, temp_mean = 1, temp_min = 1, temp_max = 1,
                vedba_mean = 3, odba_mean = 3, tbf_mean = 2, pct_swimming = 1, speed_mean = 2, speed_max = 2,
                descent_rate_max = 2, ascent_rate_max = 2, n_samples = 0, n_positions = 0)
  prec_of <- function(nm) { p <- unname(prec_map[nm]); if (is.na(p)) 2L else as.integer(p) }

  num_cols <- names(df)[vapply(df, is.numeric, logical(1))]
  # columns where a cross-individual mean is meaningless (the per-tag acquisition constant)
  agg_cols <- setdiff(num_cols, "sampling_hz")

  # character display table: datetimes formatted, numerics rounded to their precision
  disp <- as.data.frame(lapply(names(df), function(nm) {
    col <- df[[nm]]
    if (inherits(col, "POSIXt")) format(col, datetime.format, tz = "UTC")
    else if (nm %in% num_cols) ifelse(is.na(col), NA_character_, sprintf(paste0("%.", prec_of(nm), "f"), col))
    else as.character(col)
  }), stringsAsFactors = FALSE)
  names(disp) <- names(df)

  # display-only population mean +/- error footer (only meaningful with more than one deployment)
  if (isTRUE(include.summary.row) && nrow(df) > 1) {
    foot <- stats::setNames(rep(NA_character_, ncol(df)), names(df))
    for (nm in agg_cols) {
      m <- mean(df[[nm]], na.rm = TRUE)
      if (is.finite(m)) {
        e <- errfun(df[[nm]])
        foot[nm] <- if (is.finite(e)) sprintf(paste0("%.", prec_of(nm), "f ", pm, " %.", prec_of(nm), "f"), m, e)
                    else sprintf(paste0("%.", prec_of(nm), "f"), m)
      }
    }
    foot[["id"]] <- paste0("mean ", pm, " ", err_stat)
    disp <- rbind(disp, as.data.frame(as.list(foot), stringsAsFactors = FALSE, check.names = FALSE))
  }
  disp[is.na(disp)] <- "-"
  if (style != "internal") names(disp) <- .summaryHeaders(names(disp), style)
  disp
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
    id = "ID", tag_model = "Tag model", tag_type = "Tag type", attachment_site = "Attachment site",
    status = "Status", record_start = "Record start", record_end = "Record end",
    record_duration_h = "Recording duration (h)", n_samples = "Samples (n)", sampling_hz = "Sampling rate (Hz)",
    depth_mean = "Mean depth (m)", depth_max = "Max depth (m)",
    temp_mean = "Mean temp. (\u00b0C)", temp_min = "Min temp. (\u00b0C)", temp_max = "Max temp. (\u00b0C)",
    vedba_mean = "Mean VeDBA (g)", odba_mean = "Mean ODBA (g)",
    tbf_mean = "Mean tail-beat freq. (Hz)", pct_swimming = "Time swimming (%)", paddle_wheel = "Paddle wheel",
    speed_mean = "Mean speed (m/s)", speed_max = "Max speed (m/s)",
    descent_rate_max = "Max descent rate (m/s)", ascent_rate_max = "Max ascent rate (m/s)",
    n_positions = "Positions (n)")
  concise <- c(
    id = "ID", tag_model = "Tag model", tag_type = "Tag type", attachment_site = "Attach. site",
    status = "Status", record_start = "Start", record_end = "End",
    record_duration_h = "Duration (h)", n_samples = "Samples (n)", sampling_hz = "Rate (Hz)",
    depth_mean = "Mean depth (m)", depth_max = "Max depth (m)",
    temp_mean = "Mean temp. (\u00b0C)", temp_min = "Min temp. (\u00b0C)", temp_max = "Max temp. (\u00b0C)",
    vedba_mean = "Mean VeDBA (g)", odba_mean = "Mean ODBA (g)",
    tbf_mean = "Mean TBF (Hz)", pct_swimming = "Swimming (%)", paddle_wheel = "Paddle wheel",
    speed_mean = "Mean speed (m s\u207b\u00b9)", speed_max = "Max speed (m s\u207b\u00b9)",
    descent_rate_max = "Max descent (m s\u207b\u00b9)", ascent_rate_max = "Max ascent (m s\u207b\u00b9)",
    n_positions = "Positions (n)")
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
#' @param ... Unused.
#' @exportS3Method print nautilus_summary

print.nautilus_summary <- function(x, ...) {
  df <- as.data.frame(x)
  if (nrow(df) == 0) { cat("<nautilus_summary> 0 deployments\n"); return(invisible(x)) }
  pm <- if (cli::is_utf8_output()) "\u00b1" else "+/-"
  err_stat <- attr(x, "error.stat") %||% "sd"
  cat(sprintf("<nautilus_summary> %d deployment%s%s\n", nrow(df), if (nrow(df) != 1) "s" else "",
              if (nrow(df) > 1) sprintf(" (final row: mean %s %s)", pm, err_stat) else ""))
  print(format(x), row.names = FALSE)
  invisible(x)
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
