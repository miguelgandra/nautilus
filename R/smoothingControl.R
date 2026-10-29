#######################################################################################################
# Control objects for processTagData() ################################################################
#######################################################################################################

#' Smoothing windows for processTagData()
#'
#' @description
#' Groups the smoothing-window arguments of \code{\link{processTagData}} into one object, so the main
#' call stays uncluttered. Each value is a window length in seconds. `NULL` disables optional
#' post-smoothing; for `depth` it selects the minimum local slope window (see below).
#'
#' @param static Window (s) setting the static/gravity separation: the acceleration is split into a
#'   static (gravity/posture) and a dynamic (motion) part with a zero-phase Butterworth high-pass whose
#'   -3 dB cutoff is the equivalent of this window (~`0.76 / static` Hz; the default 3 s gives ~0.25 Hz).
#'   The dynamic part underlies VeDBA/ODBA and surge/sway/heave, the static part the orientation
#'   pitch/roll -- so this is not a cosmetic post-smoother: it defines what counts as "dynamic", must be
#'   `> 0`, and cannot be disabled. Default 3.
#' @param orientation Window (s) for orientation metrics (roll, pitch, heading; circular mean). Default 1.
#' @param dba Window (s) for VeDBA/ODBA smoothing, applied as a zero-phase Butterworth low-pass whose
#'   -3 dB cutoff is the equivalent of this window (~`0.44 / dba` Hz; the default 2 s gives ~0.22 Hz).
#'   `NULL` disables the smoothing. Default 2.
#' @param depth Centred local linear regression window (s) for vertical velocity. Default 5.
#'   Slopes are fitted directly to corrected depth against actual timestamps, without a preceding
#'   depth smoother. The sample radius is the larger of one and the whole number of sampling
#'   intervals in half the requested window. `NULL` or zero selects the minimum three-point
#'   stencil. Fits truncate at segment edges and require at least three observations. Missing
#'   depth/time, non-increasing timestamps and gaps longer than 1.5 sampling intervals split
#'   segments. The stored `depth` channel remains drift-corrected and unsmoothed.
#' @param vertical Centred rolling-mean window (s) applied to the fitted vertical velocity.
#'   Default 1; `NULL` or zero disables it. Applied independently within continuous segments;
#'   incomplete windows at edges or next to missing rates remain `NA`.
#'   Paddle-wheel speed is not smoothed here: that window belongs to
#'   \code{\link{calculatePaddleSpeed}}, which turns the recorded paddle frequency into a speed.
#' @return A validated `nautilus_smoothing` object for the `smoothing` argument of \code{\link{processTagData}}.
#' @seealso \code{\link{processTagData}}, \code{\link{calibrationControl}}
#' @examples
#' smoothingControl(depth = 15, dba = NULL)   # 15 s depth window; disable DBA post-smoothing
#' @export
smoothingControl <- function(static = 3, orientation = 1, dba = 2, depth = 5, vertical = 1) {
  fields <- list(static = static, orientation = orientation, dba = dba, depth = depth, vertical = vertical)
  for (nm in names(fields)) if (!is.null(fields[[nm]])) .assert_number(fields[[nm]], paste0("smoothing$", nm), min = 0)
  if (is.null(fields$static) || fields$static <= 0)
    .abort("{.arg smoothing$static} must be a positive number (the gravity-separation window cannot be disabled).")
  structure(fields, class = "nautilus_smoothing")
}
