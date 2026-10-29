#' Magnetometer-calibration switches for processTagData()
#'
#' @description
#' Groups the magnetometer-calibration switches of [processTagData()] into one object. These decide
#' whether the magnetometer is corrected for the tag's own iron before heading is computed, and whether
#' a calibration already estimated by [calibrateMagnetometer()] is used in preference to one fitted on
#' the spot. The mounting offset corrections and the orientation-estimator tuning live in
#' [orientationControl()].
#'
#' @param hard.iron Logical; whether to apply a supported hard-iron centre correction
#'   (default \code{TRUE}). This subtracts the additive magnetic offset.
#' @param soft.iron Logical; whether to apply a supported soft-iron matrix
#'   (default \code{TRUE}). Full ellipsoid fits can include cross-axis terms; a constrained planar
#'   fallback does not estimate directional distortion. Confidence gates still apply.
#' @param use.stored Whether to prefer a calibration already stored in the metadata by
#'   [calibrateMagnetometer()], such as a fit pooled across every deployment of one tag (default
#'   `TRUE`). Reuse requires both correction switches, high or medium confidence, a finite centre,
#'   a stored matrix and matching axis metadata. Otherwise an inline per-deployment estimate is
#'   considered and is itself confidence-gated. Set \code{FALSE} to bypass stored proposals.
#'   A calibration already marked as applied is not applied or estimated again.
#'
#' @return A validated `nautilus_calibration` object for the `calibration` argument of
#'   [processTagData()].
#'
#' @seealso [processTagData()] for the function that consumes it; [calibrateMagnetometer()] for the
#'   stored calibration it can draw on; [orientationControl()] and [smoothingControl()] for the other
#'   processing settings.
#'
#' @examples
#' calibrationControl(soft.iron = FALSE)
#' @export
calibrationControl <- function(hard.iron = TRUE, soft.iron = TRUE, use.stored = TRUE) {
  flags <- list(hard.iron = hard.iron, soft.iron = soft.iron, use.stored = use.stored)
  for (nm in names(flags)) .assert_flag(flags[[nm]], paste0("calibration$", nm))
  structure(flags, class = "nautilus_calibration")
}
