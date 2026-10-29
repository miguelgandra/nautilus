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
#'   (deg), `pitch_offset` / `pitch_r2` / `roll_offset` (the applied mounting-offset corrections;
#'   `roll_offset` is NA when the estimate exceeded `mount.roll.max` and was therefore not subtracted -
#'   the estimate itself is kept in the processing record as `roll_offset_estimate_deg`),
#'   `coreg_corr` (accelerometer<->gyroscope co-registration correlation from \code{\link{applyAxisMapping}};
#'   NA without a gyroscope), `hard_iron_uT`, `heading_conf` (stored magnetometer heading confidence
#'   `"high"`/`"medium"`/`"low"` from \code{\link{calibrateMagnetometer}}; NA if none stored), `mag_radcv`
#'   (calibrated-radius CV) and `mag_dip_resid` (IGRF dip residual, deg), `declination` (deg),
#'   `paddle_acceptance` (percentage of paddle-frequency windows accepted) and `paddle_failure` (the
#'   dominant rejection reason),
#'   `drift_status`, `drift_offset_m` (max |offset| applied), `drift_residual_m`, `drift_anchors`,
#'   `hz_in` / `hz_out` (sampling rates), `n_in` / `n_out` (row counts), and `flag` (comma-separated
#'   orientation anomalies: `"pitch"` (unusual median pitch), `"mount"` (unusual estimated mounting roll,
#'   whether or not it was corrected), `"roll"` (roll left over after the correction), or `""`).
#'   A tag not processed by
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
