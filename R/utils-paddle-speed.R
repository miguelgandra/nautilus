#######################################################################################################
# Paddle-wheel frequency estimation ##################################################################
#######################################################################################################

#' Estimate paddle-wheel rotation frequency with windowed spectra
#'
#' The strongest component is accepted only when it lies below the Nyquist guard, is an interior local
#' maximum of the analysis band, and clears a power-to-median-background prominence threshold. Rejected
#' windows remain missing except for short, bounded gaps between accepted estimates.
#'
#' @param mz Numeric magnetometer channel containing the paddle-wheel oscillation.
#' @param sampling.rate Sampling rate in hertz.
#' @param window.size,step.size Spectral-window length and step in seconds.
#' @param min.freq.Hz,max.freq.Hz Analysis-band limits. `NULL` for `max.freq.Hz` uses the Nyquist guard.
#' @param nyquist.guard Fraction of Nyquist retained as the safe upper band.
#' @param min.prominence Required peak power / median background power ratio.
#' @param max.interp.gap Longest rejected interval in seconds that may be interpolated; `NULL` disables
#'   interpolation.
#' @param calibration.slope Optional zero-intercept speed-calibration slope. The production pipeline
#'   leaves this `NULL` and performs calibration in [calculatePaddleSpeed()].
#' @param smooth.window Optional post-estimation smoothing window in seconds. Retained for internal
#'   compatibility; the production pipeline smooths only in [calculatePaddleSpeed()].
#' @param quality.check Retained for internal compatibility. Quality is always evaluated because it is
#'   part of the acceptance rule; setting this to `FALSE` only omits the full-length prominence vector.
#' @param verbose Whether to report the number of windows being processed.
#'
#' @return A list containing full-length `freq`, optional `speed`, optional `peak.prominence`, and `qc`.
#'   `qc` contains the compact acceptance summary plus the window-level results used to derive it. The
#'   window table is internal diagnostic state and is not appended to processed tag data.
#' @keywords internal
#' @noRd
.getPaddleSpeed <- function(mz,
                            sampling.rate,
                            window.size = 5,
                            step.size = 1,
                            min.freq.Hz = 0.1,
                            max.freq.Hz = NULL,
                            nyquist.guard = 0.9,
                            min.prominence = 20,
                            max.interp.gap = 2,
                            calibration.slope = NULL,
                            smooth.window = NULL,
                            quality.check = TRUE,
                            verbose = FALSE) {
  if (!is.numeric(mz)) stop("`mz` must be a numeric vector.")
  if (length(mz) < 2L) stop("`mz` must have length > 1 to perform meaningful analysis.")
  if (!is.numeric(sampling.rate) || length(sampling.rate) != 1L ||
      !is.finite(sampling.rate) || sampling.rate <= 0)
    stop("`sampling.rate` must be a single positive number.")
  if (!is.numeric(window.size) || length(window.size) != 1L ||
      !is.finite(window.size) || window.size <= 0)
    stop("`window.size` must be a single positive number.")
  if (!is.numeric(step.size) || length(step.size) != 1L || !is.finite(step.size) || step.size <= 0)
    stop("`step.size` must be a single positive number.")
  if (step.size > window.size) stop("`step.size` must not exceed `window.size`.")
  if (!is.numeric(min.freq.Hz) || length(min.freq.Hz) != 1L ||
      !is.finite(min.freq.Hz) || min.freq.Hz <= 0)
    stop("`min.freq.Hz` must be a single positive number.")
  if (!is.null(max.freq.Hz) && (!is.numeric(max.freq.Hz) || length(max.freq.Hz) != 1L ||
                                !is.finite(max.freq.Hz) || max.freq.Hz <= min.freq.Hz))
    stop("`max.freq.Hz` must be NULL or a single number greater than `min.freq.Hz`.")
  if (!is.numeric(nyquist.guard) || length(nyquist.guard) != 1L || !is.finite(nyquist.guard) ||
      nyquist.guard <= 0 || nyquist.guard >= 1)
    stop("`nyquist.guard` must lie strictly between zero and one.")
  if (!is.numeric(min.prominence) || length(min.prominence) != 1L ||
      !is.finite(min.prominence) || min.prominence < 0)
    stop("`min.prominence` must be a single non-negative number.")
  if (!is.null(max.interp.gap) && (!is.numeric(max.interp.gap) || length(max.interp.gap) != 1L ||
                                   !is.finite(max.interp.gap) || max.interp.gap < 0))
    stop("`max.interp.gap` must be NULL or a single non-negative number.")
  if (!is.null(calibration.slope) && (!is.numeric(calibration.slope) ||
                                      length(calibration.slope) != 1L ||
                                      !is.finite(calibration.slope)))
    stop("`calibration.slope` must be a single finite number, or NULL for frequency only.")
  if (!is.null(smooth.window) && (!is.numeric(smooth.window) || length(smooth.window) != 1L ||
                                  !is.finite(smooth.window) || smooth.window <= 0))
    stop("`smooth.window` must be a single positive number if supplied.")

  n <- length(mz)
  win_len <- min(n, max(4L, round(window.size * sampling.rate)))
  step_len <- max(1L, round(step.size * sampling.rate))
  starts <- seq.int(1L, n - win_len + 1L, by = step_len)
  centres <- starts + floor((win_len - 1L) / 2L)
  freq_axis <- seq.int(0L, floor(win_len / 2L)) * sampling.rate / win_len
  nyquist <- sampling.rate / 2
  safe_upper <- min(max.freq.Hz %||% Inf, nyquist.guard * nyquist)

  empty_result <- function(status) {
    windows <- data.frame(index = centres, freq_hz = NA_real_, prominence = NA_real_,
                          accepted = FALSE, status = status, stringsAsFactors = FALSE)
    qc <- .paddleFrequencyQC(windows, min.freq.Hz, safe_upper, nyquist)
    list(freq = rep(NA_real_, n), speed = if (is.null(calibration.slope)) NULL else rep(NA_real_, n),
         peak.prominence = if (isTRUE(quality.check)) rep(NA_real_, n) else NULL, qc = qc)
  }
  if (!is.finite(safe_upper) || safe_upper <= min.freq.Hz ||
      sum(freq_axis >= min.freq.Hz & freq_axis <= safe_upper) < 3L)
    return(empty_result("sampling_rate_too_low"))

  broad_idx <- which(freq_axis >= min.freq.Hz & freq_axis <= nyquist)
  band_idx <- which(freq_axis >= min.freq.Hz & freq_axis <= safe_upper)
  taper <- 0.5 * (1 - cos(2 * pi * seq.int(0L, win_len - 1L) / (win_len - 1L)))

  estimate_one <- function(i) {
    segment <- mz[i:(i + win_len - 1L)]
    if (any(!is.finite(segment)))
      return(list(freq = NA_real_, prominence = NA_real_, status = "missing_data"))
    spread <- stats::sd(segment)
    if (!is.finite(spread) || spread < sqrt(.Machine$double.eps))
      return(list(freq = NA_real_, prominence = NA_real_, status = "constant_signal"))

    # Linear detrending reduces leakage without the NA/recycling failure mode of predict(lm(...)).
    tt <- seq_along(segment)
    segment <- stats::.lm.fit(cbind(1, tt), segment)$residuals
    power <- Mod(stats::fft(segment * taper))[seq_along(freq_axis)]^2
    if (!any(is.finite(power[band_idx]) & power[band_idx] > 0))
      return(list(freq = NA_real_, prominence = NA_real_, status = "no_spectral_power"))

    # Do not quietly choose a weaker, apparently valid peak when the spectrum is actually dominated by
    # the region nearest Nyquist. That is the precise route that produced the 25/50-Hz speed artefacts.
    broad_peak <- broad_idx[which.max(power[broad_idx])]
    if (is.finite(power[broad_peak]) && freq_axis[broad_peak] > nyquist.guard * nyquist)
      return(list(freq = NA_real_, prominence = NA_real_, status = "nyquist_guard"))

    band_power <- power[band_idx]
    peak_pos <- which.max(band_power)
    peak_idx <- band_idx[peak_pos]
    if (peak_pos == 1L || peak_pos == length(band_idx))
      return(list(freq = NA_real_, prominence = NA_real_, status = "band_edge"))
    if (!(band_power[peak_pos] > band_power[peak_pos - 1L] &&
          band_power[peak_pos] > band_power[peak_pos + 1L]))
      return(list(freq = NA_real_, prominence = NA_real_, status = "not_local_maximum"))

    # Exclude the adjacent leakage bins from the background but use the robust median of everything
    # else. Unlike the former magnitude/mean score this has a stable noise baseline and is not diluted
    # by a few strong side lobes.
    omit <- unique(pmax(1L, pmin(length(band_power), peak_pos + (-1L:1L))))
    background <- stats::median(band_power[-omit], na.rm = TRUE)
    peak_power <- band_power[peak_pos]
    prominence <- if (is.finite(background) && background > 0) peak_power / background
                  else if (is.finite(peak_power) && peak_power > 0) Inf else NA_real_
    if (is.na(prominence))
      return(list(freq = NA_real_, prominence = NA_real_, status = "no_background"))
    if (prominence < min.prominence)
      return(list(freq = NA_real_, prominence = prominence, status = "low_prominence"))

    list(freq = freq_axis[peak_idx], prominence = prominence, status = "accepted")
  }

  if (isTRUE(verbose)) message("Processing ", length(starts), " windows...")
  estimates <- lapply(starts, estimate_one)
  windows <- data.frame(
    index = centres,
    freq_hz = vapply(estimates, `[[`, numeric(1), "freq"),
    prominence = vapply(estimates, `[[`, numeric(1), "prominence"),
    status = vapply(estimates, `[[`, character(1), "status"),
    stringsAsFactors = FALSE)
  windows$accepted <- windows$status == "accepted"

  freq_full <- .paddleExpandWindows(windows$freq_hz, centres, n, step.size, max.interp.gap)
  prom_values <- ifelse(windows$accepted, windows$prominence, NA_real_)
  prom_full <- .paddleExpandWindows(prom_values, centres, n, step.size, max.interp.gap)

  if (!is.null(smooth.window)) {
    k <- max(3L, round(smooth.window * sampling.rate))
    if (k <= n)
      freq_full <- data.table::frollmean(freq_full, n = k, fill = NA_real_, align = "center")
  }

  list(freq = freq_full,
       speed = if (is.null(calibration.slope)) NULL else freq_full * calibration.slope,
       peak.prominence = if (isTRUE(quality.check)) prom_full else NULL,
       qc = .paddleFrequencyQC(windows, min.freq.Hz, safe_upper, nyquist))
}


#' Expand window-centre estimates without bridging long rejected intervals
#' @keywords internal
#' @noRd
.paddleExpandWindows <- function(values, centres, n, step.size, max.interp.gap) {
  values <- as.numeric(values)
  if (!is.null(max.interp.gap) && max.interp.gap > 0 && any(is.finite(values))) {
    maxgap <- floor(max.interp.gap / step.size + sqrt(.Machine$double.eps))
    if (maxgap > 0L) values <- zoo::na.approx(values, x = centres, maxgap = maxgap, na.rm = FALSE)
  }

  out <- rep(NA_real_, n)
  finite <- is.finite(values)
  if (!any(finite)) return(out)
  runs <- rle(finite)
  ends <- cumsum(runs$lengths)
  starts <- ends - runs$lengths + 1L
  for (j in which(runs$values)) {
    ix <- starts[j]:ends[j]
    if (length(ix) == 1L) {
      out[centres[ix]] <- values[ix]
    } else {
      target <- seq.int(centres[ix[1]], centres[ix[length(ix)]])
      out[target] <- stats::approx(centres[ix], values[ix], xout = target, rule = 1)$y
    }
  }
  out
}


#' Summarise paddle-frequency window acceptance
#' @keywords internal
#' @noRd
.paddleFrequencyQC <- function(windows, min.freq.Hz, max.freq.Hz, nyquist) {
  n <- nrow(windows)
  accepted <- sum(windows$accepted)
  rejected <- windows$status[!windows$accepted]
  counts <- sort(table(rejected), decreasing = TRUE)
  dominant <- if (length(counts)) names(counts)[1] else "none"
  list(n_windows = n,
       n_accepted = accepted,
       acceptance_pct = if (n) 100 * accepted / n else NA_real_,
       dominant_failure = dominant,
       band_hz = c(min = min.freq.Hz, max = max.freq.Hz),
       nyquist_hz = nyquist,
       failure_counts = counts,
       windows = windows)
}
