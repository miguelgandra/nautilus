#' Orientation-estimation tuning for processTagData()
#'
#' @description
#' Groups the specialised orientation settings of [processTagData()] into one object, leaving the
#' primary choice - the estimator itself - as that function's top-level `orientation.algorithm`
#' argument.
#'
#' Two distinct things are tuned here. The first is the estimator: how much the Madgwick filter trusts
#' the accelerometer against the gyroscope, and how paddle-wheel contamination is removed before heading
#' is computed. The second is the mounting geometry: a tag is never attached perfectly level, and the
#' resulting constant pitch and roll offsets are indistinguishable from the animal's own posture unless
#' they are estimated and removed.
#'
#' @param madgwick.beta The Madgwick filter's gain, which sets how much it trusts the accelerometer
#'   against the gyroscope (default `0.02`). Raise it if the estimated orientation drifts over long
#'   stretches; lower it if the orientation is jittery during vigorous swimming. Used only when
#'   `orientation.algorithm = "madgwick"`.
#' @param correct.pitch Whether to estimate and subtract the mounting pitch offset (default `TRUE`),
#'   taken from where the pitch-against-vertical-velocity relationship crosses zero. Disable it if you
#'   know the tag was mounted level and want the raw posture.
#' @param correct.roll Whether to estimate and subtract the mounting roll offset (default `TRUE`), taken
#'   as the median roll over level swimming.
#' @param pitch.offset.min.r2 Minimum R-squared of the pitch-against-vertical-velocity fit required
#'   before the pitch offset is subtracted (default `0.1`). Below this the fitted offset is really just
#'   the mean pitch, and subtracting it would strip out genuine posture, so the correction is skipped.
#' @param mount.roll.max Largest mounting roll offset, in degrees, that `correct.roll` will still
#'   subtract (default `60`). This is a plausibility gate rather than an alarm: beyond it, a large
#'   estimate more likely means the body frame is wrong than that the tag was clamped that far round, so
#'   the offset is recorded but left in place. It is deliberately wider than `warning.threshold`, because
#'   a steeply rolled clamp is a real mounting geometry - a left-side and a right-side attachment are
#'   mirror images - and such a deployment should be both corrected and flagged. Lower it to be stricter
#'   about what may be absorbed into the mount. Roll only: a large pitch offset would mean the tag points
#'   along the body rather than across it, which is not a normal mounting geometry, so the pitch
#'   correction stays capped by `warning.threshold`.
#' @param warning.threshold Threshold in degrees above which an orientation warning is raised (default
#'   `45`), for three independent checks: an unusual median absolute pitch, an unusual estimated mounting
#'   roll, raised whether or not the correction was applied, and an unusual median absolute roll left
#'   over after correction. It also caps the pitch offset correction. Lower it, to `35` say, to hear
#'   about moderately rolled mounts as well; it changes what is reported, not what is corrected.
#' @param heading.denoise How to suppress paddle-wheel contamination of the magnetometer before heading
#'   is computed. A spinning paddle magnet adds a large, fast oscillation to the field; because it adds
#'   to the field vector and averages to zero over a rotation, a centred running mean of the
#'   magnetometer vector removes it while preserving the slow, orientation-driven variation. `"auto"`
#'   (default) detects the paddle and derives one stable window per deployment from its rotation rate;
#'   `"manual"` always applies `heading.denoise.window`; `"off"` disables it. Where the paddle turns too
#'   slowly to be separated from the animal's own turning, no window can help and a warning is raised -
#'   use a gyroscope-based orientation estimator instead.
#' @param heading.denoise.window Smoothing window in seconds used when `heading.denoise = "manual"`
#'   (default `3`). It should span several paddle rotations but stay well short of the animal's turning
#'   timescale.
#'
#' @return A validated `nautilus_orientation` object for the `orientation` argument of
#'   [processTagData()].
#'
#' @seealso [processTagData()] for the function that consumes it; [calibrationControl()] and
#'   [smoothingControl()] for the other processing settings.
#'
#' @examples
#' orientationControl(correct.roll = FALSE)     # skip the roll-offset correction
#' orientationControl(madgwick.beta = 0.05)     # stronger Madgwick gain
#' orientationControl(heading.denoise = "manual", heading.denoise.window = 2)
#' @references
#' Madgwick SOH, Harrison AJL, Vaidyanathan R (2011) Estimation of IMU and MARG orientation using a
#' gradient descent algorithm. *IEEE International Conference on Rehabilitation Robotics*. 1-7.
#' \doi{10.1109/ICORR.2011.5975346}
#' @export
orientationControl <- function(madgwick.beta = 0.02, correct.pitch = TRUE, correct.roll = TRUE,
                               pitch.offset.min.r2 = 0.1, mount.roll.max = 60, warning.threshold = 45,
                               heading.denoise = c("auto", "manual", "off"),
                               heading.denoise.window = 3) {
  heading.denoise <- match.arg(heading.denoise)
  .assert_number(madgwick.beta, "orientation$madgwick.beta", min = 0)
  .assert_flag(correct.pitch, "orientation$correct.pitch")
  .assert_flag(correct.roll, "orientation$correct.roll")
  .assert_number(pitch.offset.min.r2, "orientation$pitch.offset.min.r2", min = 0, max = 1)
  .assert_number(mount.roll.max, "orientation$mount.roll.max", min = 0, max = 180)
  .assert_number(warning.threshold, "orientation$warning.threshold", min = 0)
  .assert_number(heading.denoise.window, "orientation$heading.denoise.window", min = 0)
  structure(list(madgwick.beta = madgwick.beta, correct.pitch = correct.pitch, correct.roll = correct.roll,
                 pitch.offset.min.r2 = pitch.offset.min.r2, mount.roll.max = mount.roll.max,
                 warning.threshold = warning.threshold,
                 heading.denoise = heading.denoise, heading.denoise.window = heading.denoise.window),
            class = "nautilus_orientation")
}
