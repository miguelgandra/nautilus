#######################################################################################################
# Per-deployment processing summary ###################################################################
#######################################################################################################

#' Per-deployment processing summary from processTagData() provenance
#'
#' @description
#' Projects the provenance that \code{\link{processTagData}} records into each object - its
#' `processTagData` and `depth_drift` audit-trail entries - into a tidy, one-row-per-deployment table.
#' This is the scannable cross-deployment view of *what the pipeline did* (mounting-offset corrections,
#' magnetometer calibration, depth-drift outcome, downsampling), complementing \code{\link{summarizeTagData}},
#' which summarises *what the animal did*.
#'
#' Because it reads stored metadata rather than recomputing anything, it works on the in-memory objects
#' returned by \code{\link{processTagData}} OR on the saved `.rds` files at any later time, and never
#' re-runs processing. It is therefore independent of \code{processTagData}'s `return.data` argument.
#'
#' @param data Processed nautilus tag(s): a named list of tag objects, a single tag object, a character
#'   vector of `.rds` file paths, or a directory containing processed `.rds` files.
#' @param id.col Name of the ID column used only when `data` is a single combined data.frame that must be
#'   split by deployment. Default `"ID"`.
#'
#' @return A `nautilus_processing_summary` (a `data.frame` subclass), one row per deployment, with typed
#'   columns: `id`, `tag_model`, `algorithm` (orientation estimator), `median_pitch` / `median_roll`
#'   (deg), `pitch_offset` / `pitch_r2` / `roll_offset` (the applied mounting-offset corrections),
#'   `coreg_corr` (accelerometer<->gyroscope co-registration correlation from \code{\link{applyAxisMapping}};
#'   NA without a gyroscope), `hard_iron_uT`, `heading_conf` (stored magnetometer heading confidence
#'   `"high"`/`"medium"`/`"low"` from \code{\link{calibrateMagnetometer}}; NA if none stored), `mag_radcv`
#'   (calibrated-radius CV) and `mag_dip_resid` (IGRF dip residual, deg), `declination` (deg),
#'   `drift_status`, `drift_offset_m` (max |offset| applied), `drift_residual_m`, `drift_anchors`,
#'   `hz_in` / `hz_out` (sampling rates), `n_in` / `n_out` (row counts), and `flag` (comma-separated
#'   orientation anomalies: `"pitch"`, `"roll"`, or `""`). A tag not processed by
#'   \code{\link{processTagData}} yields a row of `NA`s.
#' @seealso \code{\link{processTagData}}, \code{\link{summarizeTagData}}, \code{\link{processingHistory}}
#' @examples
#' \dontrun{
#' processed <- processTagData(tags)
#' processingSummary(processed)                       # from the in-memory list
#' processingSummary("./data interim/processed")      # or from the saved files, any time later
#' }
#' @export
processingSummary <- function(data, id.col = "ID") {
  .assert_string(id.col, "id.col")
  # a single directory -> its .rds files
  if (is.character(data) && length(data) == 1L && dir.exists(data)) {
    data <- list.files(data, pattern = "\\.rds$", full.names = TRUE, ignore.case = TRUE)
    if (!length(data)) .abort("No {.file .rds} files found in the supplied directory.")
  }
  # a single tag object -> wrap it, so its metadata is not lost to a data.frame split in .resolveInput
  if (!is.null(.getMeta(data))) data <- list(data)
  r <- .resolveInput(data, id.col = id.col)
  rows <- if (r$n == 0L) list() else lapply(seq_len(r$n), function(i) .processingSummaryRow(r$get(i), r$ids[i]))
  out <- do.call(rbind, c(list(.processingSummaryTemplate()), rows))
  rownames(out) <- NULL
  class(out) <- c("nautilus_processing_summary", "data.frame")
  out
}


#' Typed 0-row template defining the processing-summary columns (single source of the schema).
#' @keywords internal
#' @noRd
.processingSummaryTemplate <- function() {
  data.frame(id = character(0), tag_model = character(0), algorithm = character(0),
             median_pitch = numeric(0), median_roll = numeric(0),
             pitch_offset = numeric(0), pitch_r2 = numeric(0), roll_offset = numeric(0),
             coreg_corr = numeric(0),
             hard_iron_uT = numeric(0), heading_conf = character(0),
             mag_radcv = numeric(0), mag_dip_resid = numeric(0), declination = numeric(0),
             drift_status = character(0), drift_offset_m = numeric(0),
             drift_residual_m = numeric(0), drift_anchors = integer(0),
             hz_in = numeric(0), hz_out = numeric(0),
             n_in = integer(0), n_out = integer(0), flag = character(0),
             stringsAsFactors = FALSE)
}


#' Build one processing-summary row from a tag's stored metadata.
#' @keywords internal
#' @noRd
.processingSummaryRow <- function(x, fallback_id = NA_character_) {
  meta <- .getMeta(x)
  pr   <- .lastProcessingRecord(meta, "processTagData")
  dd   <- .lastProcessingRecord(meta, "depth_drift")
  num  <- function(v) if (is.null(v) || length(v) != 1L) NA_real_    else as.numeric(v)
  chr  <- function(v) if (is.null(v) || length(v) != 1L) NA_character_ else as.character(v)
  intg <- function(v) if (is.null(v) || length(v) != 1L) NA_integer_  else as.integer(v)
  # magnetometer QC accessor: prefer the APPLIED fit's value, fall back to the PROPOSED estimate's
  mqc  <- function(mcal, field) {
    a <- mcal$qc[[field]]; if (!is.null(a) && length(a) == 1L && !is.na(a)) return(a)
    if (!is.null(mcal$proposed)) mcal$proposed$qc[[field]] else NULL
  }
  off   <- if (!is.null(dd)) dd$outcome$offset_range_m else NULL
  flags <- c(if (isTRUE(pr$pitch_anomaly_detected)) "pitch",
             if (isTRUE(pr$roll_anomaly_detected))  "roll")
  data.frame(
    id               = chr(meta$id %||% fallback_id),
    tag_model        = chr(meta$tag$model),
    algorithm        = chr(pr$orientation_algorithm),
    median_pitch     = num(pr$median_pitch_deg),
    median_roll      = num(pr$median_roll_deg),
    pitch_offset     = num(pr$pitch_offset_deg),
    pitch_r2         = num(pr$pitch_offset_r2),
    roll_offset      = num(pr$roll_offset_deg),
    coreg_corr       = num(meta$axis_mapping$coreg_corr),        # accel<->gyro co-registration (Phase 2)
    hard_iron_uT     = num(pr$hard_iron_offset_uT),
    # magnetometer heading QC: the APPLIED fit's QC, falling back to the PROPOSED estimate's (a well-covered
    # deployment that was not yet run through processTagData still reports its proposed confidence)
    heading_conf     = chr(mqc(meta$mag_calibration, "confidence")),
    mag_radcv        = num(mqc(meta$mag_calibration, "radcv")),
    mag_dip_resid    = num(mqc(meta$mag_calibration, "igrf_residual")),
    declination      = num(pr$magnetic_declination),
    drift_status     = chr(dd$status %||% "none"),
    drift_offset_m   = if (!is.null(off) && length(off) == 2L) max(abs(off)) else NA_real_,
    drift_residual_m = num(if (!is.null(dd)) dd$outcome$residual_m else NULL),
    drift_anchors    = intg(if (!is.null(dd)) dd$n_anchors else NULL),
    hz_in            = num(meta$sensors$sampling_hz_original),
    hz_out           = num(meta$sensors$sampling_hz_processed),
    n_in             = intg(pr$n_input),
    n_out            = intg(pr$n_output),
    flag             = if (length(flags)) paste(flags, collapse = ",") else "",
    stringsAsFactors = FALSE)
}


#' The last audit-trail record for a given step (or NULL). Last wins if a step ran more than once.
#' @keywords internal
#' @noRd
.lastProcessingRecord <- function(meta, step) {
  recs <- meta$processing
  if (is.null(recs) || !length(recs)) return(NULL)
  hit <- NULL
  for (rec in recs) if (identical(rec$step, step)) hit <- rec
  hit
}


#' @exportS3Method print nautilus_processing_summary
print.nautilus_processing_summary <- function(x, ...) {
  df <- as.data.frame(x)
  if (!nrow(df)) { cat("# processing summary: 0 deployments\n"); return(invisible(x)) }
  disp <- df
  for (nm in names(disp)) if (is.numeric(disp[[nm]])) disp[[nm]] <- round(disp[[nm]], 2)
  cat(sprintf("# processing summary: %d deployment%s\n", nrow(df), if (nrow(df) != 1L) "s" else ""))
  print(disp, row.names = FALSE, ...)
  invisible(x)
}
