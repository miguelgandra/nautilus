#' Paddle-wheel frequency estimation settings for processTagData()
#'
#' @description
#' Groups the settings used by [processTagData()] to recover paddle-wheel rotation frequency from the
#' magnetometer. The estimator works in overlapping windows, reports a frequency only when the strongest
#' in-band component is an interior spectral peak, and requires that peak to stand above the spectral
#' background. This prevents broadband noise and sampling-boundary artefacts from becoming precise-looking
#' rotation rates.
#'
#' @param window.size Length of each spectral window in seconds (default `5`). Longer windows improve
#'   frequency resolution but follow changes in rotation rate more slowly.
#' @param step.size Time between successive estimates in seconds (default `1`). It must not exceed
#'   `window.size`.
#' @param min.freq.Hz Lower frequency searched, in hertz (default `0.1`). It must be positive.
#' @param max.freq.Hz Optional upper frequency searched, in hertz. `NULL` (default) uses the Nyquist-safe
#'   limit set by `nyquist.guard`; an explicit value can narrow the band but cannot bypass that guard.
#' @param nyquist.guard Fraction of the Nyquist frequency retained as a safe analysis band (default
#'   `0.9`). It must lie strictly between zero and one. Power whose dominant peak lies above this limit
#'   is treated as a sampling-boundary artefact rather than reassigned to a lower-frequency peak. This
#'   guard rejects boundary peaks but cannot recover frequencies that were already aliased during
#'   recording; the sensor sampling rate must still be high enough for the physical rotation range.
#' @param min.prominence Minimum ratio of peak power to median in-band background power (default `20`).
#'   A value of zero disables this quality gate, but the interior-peak and Nyquist guards still apply.
#' @param max.interp.gap Longest run of rejected estimates, in seconds, that may be filled between two
#'   accepted estimates (default `2`). `NULL` or zero leaves every rejected window missing. Leading and
#'   trailing gaps are never extrapolated.
#'
#' @return A validated `nautilus_paddle_frequency` object for the `paddle` argument of
#'   [processTagData()].
#'
#' @seealso [processTagData()] for frequency extraction and [calculatePaddleSpeed()] for the separate
#'   calibration step that converts frequency to speed.
#'
#' @examples
#' paddleFrequencyControl(max.freq.Hz = 35, min.prominence = 15)
#' paddleFrequencyControl(max.interp.gap = NULL)  # retain every rejected interval as missing
#' @export
paddleFrequencyControl <- function(window.size = 5,
                                   step.size = 1,
                                   min.freq.Hz = 0.1,
                                   max.freq.Hz = NULL,
                                   nyquist.guard = 0.9,
                                   min.prominence = 20,
                                   max.interp.gap = 2) {
  .assert_number(window.size, "paddle$window.size", min = 0)
  .assert_number(step.size, "paddle$step.size", min = 0)
  .assert_number(min.freq.Hz, "paddle$min.freq.Hz", min = 0)
  .assert_number(max.freq.Hz, "paddle$max.freq.Hz", min = 0, null_ok = TRUE)
  .assert_number(nyquist.guard, "paddle$nyquist.guard", min = 0)
  .assert_number(min.prominence, "paddle$min.prominence", min = 0)
  .assert_number(max.interp.gap, "paddle$max.interp.gap", min = 0, null_ok = TRUE)

  if (window.size <= 0) .abort("{.arg paddle$window.size} must be greater than zero.")
  if (step.size <= 0) .abort("{.arg paddle$step.size} must be greater than zero.")
  if (step.size > window.size)
    .abort("{.arg paddle$step.size} must not exceed {.arg paddle$window.size}.")
  if (min.freq.Hz <= 0) .abort("{.arg paddle$min.freq.Hz} must be greater than zero.")
  if (!is.null(max.freq.Hz) && max.freq.Hz <= min.freq.Hz)
    .abort("{.arg paddle$max.freq.Hz} must be greater than {.arg paddle$min.freq.Hz}.")
  if (nyquist.guard <= 0 || nyquist.guard >= 1)
    .abort("{.arg paddle$nyquist.guard} must lie strictly between zero and one.")

  structure(list(window.size = window.size, step.size = step.size,
                 min.freq.Hz = min.freq.Hz, max.freq.Hz = max.freq.Hz,
                 nyquist.guard = nyquist.guard, min.prominence = min.prominence,
                 max.interp.gap = max.interp.gap),
            class = "nautilus_paddle_frequency")
}
