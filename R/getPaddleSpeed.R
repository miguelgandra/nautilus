#' Estimate Paddle Wheel Frequency (Optimized Block-wise FFT)
#'
#' Efficient paddle wheel frequency estimation using optimized FFT processing with
#' improved spectral analysis. The function processes the magnetometer's z-axis
#' data in overlapping windows, applies Fast Fourier Transform (FFT) to identify
#' dominant frequencies within a specified range, and converts these frequencies
#' to animal swimming speeds using a provided calibration slope (assuming a zero y-intercept).
#'
#' @param mz Numeric vector of magnetometer z-axis values. Missing values (`NA`)
#'   are not explicitly handled and may cause errors; consider pre-processing `mz`
#'   to remove or impute `NA`s if they are present.
#' @param sampling.rate Sampling rate of the `mz` data in Hz (e.g., 100). Must be positive.
#' @param window.size Size of the FFT window in seconds (default: 5). Must be positive.
#' @param step.size Step size for overlapping windows in seconds (default: 1). Must be positive.
#' @param min.freq.Hz Minimum frequency (Hz) to consider for peak detection (default: 0.1).
#'   This should be chosen based on the expected minimum paddle wheel rotation frequency.
#' @param max.freq.Hz Maximum frequency (Hz) to consider for peak detection (default: 100).
#'   This should be chosen based on the expected maximum paddle wheel rotation frequency.
#' @param calibration.slope Numeric value representing the slope of the calibration line
#'   (speed = slope * frequency). (default: 0.25).
#' @param smooth.window Smoothing window in seconds for the final frequency and
#'   speed estimates (default: NULL). If provided, a rolling mean is applied.
#' @param quality.check Logical; if TRUE, calculates and returns a quality metric
#'   for each frequency estimate, indicating the prominence of the detected peak
#'   relative to other frequencies within the search band (default: TRUE).
#' @param verbose Logical; if TRUE, prints progress messages during execution (default: FALSE).
#'
#' @return A list with:
#' \itemize{
#'   \item **freq**: Numeric vector of estimated paddle wheel frequencies (Hz),
#'     interpolated to the full length of `mz`. `NA` values will be present where
#'     a frequency could not be reliably estimated.
#'   \item **speed**: Numeric vector of estimated animal swimming speeds (m/s),
#'     derived from `freq` using `calibration.slope`.
#'   \item **peak.prominence**: Numeric vector representing the quality of each
#'     frequency estimate (ratio of peak power to maximum power in the valid
#'     frequency range). Only included if `quality.check` is TRUE. `NA` values
#'     will be present where a quality could not be reliably estimated.
#' }
#' @importFrom stats fft lm predict sd
#' @importFrom zoo na.approx zoo
#' @importFrom data.table frollmean

.getPaddleSpeed <- function(mz,
                            sampling.rate,
                            window.size = 5,
                            step.size = 1,
                            min.freq.Hz = 0.1,
                            max.freq.Hz = 100,
                            calibration.slope,
                            smooth.window = NULL,
                            quality.check = TRUE,
                            verbose = FALSE) {


  # --- Input Validation ---
  if (!is.numeric(mz)) stop("`mz` must be a numeric vector.")
  if (length(mz) < 2) stop("`mz` must have length > 1 to perform meaningful analysis.")
  if (sampling.rate <= 0) stop("`sampling.rate` must be positive.")
  if (window.size <= 0) stop("`window.size` must be positive.")
  if (step.size <= 0) stop("`step.size` must be positive.")
  if (min.freq.Hz >= max.freq.Hz) stop("`min.freq.Hz` must be less than `max.freq.Hz`.")
  if (!is.null(smooth.window) && smooth.window <= 0) stop("`smooth.window` must be positive if provided.")

  # Validate calibration.slope
  if (!is.numeric(calibration.slope) || length(calibration.slope) != 1) {
    stop("`calibration.slope` must be a single numeric value.")
  }

  n <- length(mz)
  win_len <- round(window.size * sampling.rate)
  step_len <- round(step.size * sampling.rate)

  # Ensure window isn't longer than data
  if (win_len > n) {
    warning("`window.size` is larger than the data length. Adjusting `window.size` to full data length.")
    win_len <- n
  }
  starts <- seq(1, n - win_len + 1, by = step_len)

  # Pre-calculate frequency axis once
  freq_axis <- (0:(floor(win_len/2))) * sampling.rate / win_len

  # --- Internal Function for Frequency Estimation ---
  freq_estimates_single_window <- function(i) {
    segment <- mz[i:(i + win_len - 1)]

    # Handle segments with constant values (no variance) to avoid errors in lm() or meaningless FFT
    if (sd(segment, na.rm = TRUE) < .Machine$double.eps) {
      return(list(freq = NA_real_, quality = NA_real_))
    }

    # Remove linear trend to reduce spectral leakage (end effects)
    segment <- segment - stats::predict(lm(segment ~ seq_along(segment)))

    # Apply Hanning window to reduce side lobes and improve frequency resolution
    window <- 0.5 * (1 - cos(2*pi*seq(0, 1, length.out = win_len)))
    segment <- segment * window

    # Compute FFT and take the magnitude of the single-sided spectrum
    spec <- abs(stats::fft(segment))[1:(floor(win_len/2) + 1)]

    # Identify valid frequency range based on min.freq.Hz and max.freq.Hz
    valid_indices <- which(freq_axis >= min.freq.Hz & freq_axis <= max.freq.Hz)

    # If no valid frequencies or all spectral values are NA/zero in the valid range
    if (length(valid_indices) == 0 || all(is.na(spec[valid_indices])) || all(spec[valid_indices] == 0)) {
      return(list(freq = NA_real_, quality = NA_real_))
    }

    # Find the index of the maximum power within the valid frequency range
    max_idx_in_valid <- valid_indices[which.max(spec[valid_indices])]

    # Calculate quality as the ratio of peak power to the maximum power within the valid range
    peak_power <- spec[max_idx_in_valid]
    max_power_in_valid <- max(spec[valid_indices])

    list(freq = freq_axis[max_idx_in_valid],
         quality = peak_power / max_power_in_valid)
  }

  if (verbose) message("Processing ", length(starts), " windows...")

  # --- Sequential Processing (Parallel option removed) ---
  results <- lapply(starts, freq_estimates_single_window)


  # Extract frequencies and qualities from results
  freqs <- sapply(results, `[[`, "freq")
  qualities <- if (quality.check) sapply(results, `[[`, "quality") else NULL
  time_index <- starts + floor(win_len / 2) # Time index corresponds to the center of each window

  # --- Interpolation to Full Length ---
  freq_full <- zoo::na.approx(zoo::zoo(freqs, time_index), xout = seq_len(n), na.rm = FALSE)
  freq_full <- as.numeric(freq_full)

  # --- Smoothing ---
  if (!is.null(smooth.window)) {
    k <- max(3, round(smooth.window * sampling.rate))
    if (k > length(freq_full)) {
      warning("`smooth.window` results in a window larger than the data. No smoothing applied to frequency.")
    } else {
      freq_full <- data.table::frollmean(freq_full, n = k, fill = NA, align = "center")
    }
  }

  # --- Calculate Speed using the Calibration Slope ---
  # Apply the linear calibration: speed = slope * frequency (intercept is 0)
  speed_full <- freq_full * calibration.slope

  # --- Return Results ---
  return_list <- list(
    freq = freq_full,
    speed = speed_full
  )

  if (quality.check) {
    qual_full <- zoo::na.approx(zoo::zoo(qualities, time_index), xout = seq_len(n), na.rm = FALSE)
    return_list$peak.prominence <- as.numeric(qual_full)

    if (!is.null(smooth.window)) {
      k_qual <- max(3, round(smooth.window * sampling.rate))
      if (k_qual > length(qual_full)) {
        warning("`smooth.window` results in a window larger than the data. No smoothing applied to quality.")
      } else {
        return_list$peak.prominence <- data.table::frollmean(return_list$peak.prominence, n = k_qual, fill = NA, align = "center")
      }
    }
  }

  return(return_list)
}
