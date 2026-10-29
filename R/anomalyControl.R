#' Anomaly-detection settings for one sensor channel
#'
#' @description
#' What counts as an impossible jump depends entirely on the channel. Depth can change by metres per
#' second during a dive; temperature cannot change by degrees per second anywhere in the ocean. A single
#' threshold across channels would either miss real faults or flag ordinary behaviour.
#'
#' `anomalyControl()` describes one channel, so [checkSensorQuality()] can screen several in a single
#' call, each judged on its own terms.
#'
#' @param rate.threshold How fast the channel can plausibly change, in units per second. A
#'   sample-to-sample change beyond this is treated as a spike rather than a measurement. Set it from
#'   what the animal and the environment allow, with some headroom: too low and normal behaviour is
#'   flagged, too high and real spikes survive into your analysis. Required.
#' @param sensor.resolution The smallest change the channel can express, in its measurement units.
#'   Used in the resolution gate preceding the rate test. The current gate is sampling-interval
#'   dependent; see [checkSensorQuality()] for its criterion and limitations.
#'
#'   Required, with no default, because resolution is a property of a particular instrument and channel
#'   and the package has no basis for guessing it: a value suited to depth in metres is an order of
#'   magnitude too coarse for temperature in degrees. Take it from the tag's specification, or from the
#'   smallest non-zero difference between consecutive raw readings.
#' @param sensor.accuracy.fixed,sensor.accuracy.percent The sensor's stated accuracy, as a fixed value in
#'   the channel's units or as a percentage of the reading. Supply at most one. Retained in this control
#'   object but not used by the detector or copied into the processing-history entry. Defaults `NULL`.
#' @param outlier.window How close together, in minutes, outliers must fall to be treated as one
#'   malfunction period rather than as separate spikes. Default 5. Widen it where a failing sensor
#'   glitches intermittently over a longer stretch.
#' @param stall.threshold Minimum duration of identical, strictly positive readings flagged as a stall,
#'   in minutes (default \code{5}), evaluated using the nominal sampling frequency. Zero and negative
#'   constant readings are not flagged. Raise it for channels that legitimately remain constant.
#' @return A validated `nautilus_anomaly` object, for one entry of [checkSensorQuality()]'s `sensors`
#'   argument.
#' @seealso [checkSensorQuality()]
#' @examples
#' anomalyControl(rate.threshold = 7, sensor.resolution = 0.5, sensor.accuracy.percent = 1)
#' @export
anomalyControl <- function(rate.threshold,
                           sensor.resolution,
                           sensor.accuracy.fixed = NULL,
                           sensor.accuracy.percent = NULL,
                           outlier.window = 5,
                           stall.threshold = 5) {
  .assert_number(rate.threshold, "rate.threshold", min = 0)
  .assert_number(sensor.resolution, "sensor.resolution", min = 0)
  .assert_number(outlier.window, "outlier.window", min = 0)
  .assert_number(stall.threshold, "stall.threshold", min = 0)
  if (!is.null(sensor.accuracy.fixed) && !is.null(sensor.accuracy.percent))
    .abort("Provide only one of {.arg sensor.accuracy.fixed} or {.arg sensor.accuracy.percent}, not both.")
  if (!is.null(sensor.accuracy.fixed))   .assert_number(sensor.accuracy.fixed, "sensor.accuracy.fixed", min = 0)
  if (!is.null(sensor.accuracy.percent)) .assert_number(sensor.accuracy.percent, "sensor.accuracy.percent", min = 0, max = 100)
  structure(list(rate.threshold = rate.threshold, sensor.resolution = sensor.resolution,
                 sensor.accuracy.fixed = sensor.accuracy.fixed, sensor.accuracy.percent = sensor.accuracy.percent,
                 outlier.window = outlier.window, stall.threshold = stall.threshold),
            class = "nautilus_anomaly")
}
