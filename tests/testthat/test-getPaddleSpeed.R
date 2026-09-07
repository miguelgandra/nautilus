# Tests for .getPaddleSpeed (block-wise FFT frequency estimation).
# Covers the Increment 3 fixes:
#   C5 - peak.prominence used to be identically 1 (peak / max(band) == 1)
#   C6 - NA input silently misaligned the detrend subtraction

make_signal <- function(freq = 1, sr = 10, dur = 60, noise = 0) {
  t <- seq(0, dur, by = 1 / sr)
  sin(2 * pi * freq * t) + if (noise > 0) stats::rnorm(length(t), sd = noise) else 0
}

test_that(".getPaddleSpeed recovers a known frequency", {
  sr <- 10
  mz <- make_signal(freq = 1, sr = sr)
  res <- .getPaddleSpeed(mz, sampling.rate = sr, window.size = 5, step.size = 1,
                         min.freq.Hz = 0.2, max.freq.Hz = 4, calibration.slope = 0.25)
  expect_named(res, c("freq", "speed", "peak.prominence", "qc"))
  expect_length(res$freq, length(mz))
  expect_equal(stats::median(res$freq, na.rm = TRUE), 1, tolerance = 0.2)
  expect_equal(res$speed, res$freq * 0.25)
})

test_that("peak.prominence is a real ratio, not identically 1 (C5)", {
  sr <- 10
  set.seed(1)
  mz <- make_signal(freq = 1, sr = sr, noise = 0.05)
  res <- .getPaddleSpeed(mz, sampling.rate = sr, window.size = 5, step.size = 1,
                         min.freq.Hz = 0.2, max.freq.Hz = 4, calibration.slope = 0.25)
  pp <- res$peak.prominence[!is.na(res$peak.prominence)]
  expect_gt(length(pp), 0)
  expect_false(all(pp == 1))   # the bug forced every value to exactly 1
  expect_gt(stats::median(pp), 1)  # a clear peak should stand above the background
})

test_that(".getPaddleSpeed tolerates NA without error or misalignment (C6)", {
  sr <- 10
  mz <- make_signal(freq = 1, sr = sr)
  mz[c(50, 51, 200)] <- NA
  expect_no_error(
    res <- .getPaddleSpeed(mz, sampling.rate = sr, window.size = 5, step.size = 1,
                           min.freq.Hz = 0.2, max.freq.Hz = 4, calibration.slope = 0.25)
  )
  expect_length(res$freq, length(mz))
  # estimate should still be sensible where windows are clean
  expect_equal(stats::median(res$freq, na.rm = TRUE), 1, tolerance = 0.2)
})

test_that("white noise is not promoted to a continuous frequency track", {
  set.seed(2)
  res <- .getPaddleSpeed(stats::rnorm(60000), sampling.rate = 100,
                         min.prominence = 20)
  expect_lt(res$qc$acceptance_pct, 5)
  expect_identical(res$qc$dominant_failure, "low_prominence")
  expect_gt(mean(is.na(res$freq)), 0.95)
})

test_that("a Nyquist-frequency component is rejected instead of becoming a speed", {
  mz <- (-1)^(seq_len(6000))                         # exactly 50 Hz at a 100-Hz sample rate
  res <- .getPaddleSpeed(mz, sampling.rate = 100)
  expect_true(all(is.na(res$freq)))
  expect_identical(res$qc$dominant_failure, "nyquist_guard")
  expect_equal(unname(res$qc$failure_counts["nyquist_guard"]), res$qc$n_windows)
})

test_that("a peak on an analysis-band edge is not treated as an interior maximum", {
  sr <- 20
  mz <- make_signal(freq = 1, sr = sr)
  res <- .getPaddleSpeed(mz, sampling.rate = sr, min.freq.Hz = 1, max.freq.Hz = 8,
                         min.prominence = 0)
  expect_true(all(is.na(res$freq)))
  expect_identical(res$qc$dominant_failure, "band_edge")
})

test_that("window interpolation fills only gaps no longer than max.interp.gap", {
  centres <- seq(10, 90, by = 10)
  short <- c(1, 1, NA, NA, 1, 1, 1, 1, 1)
  long  <- c(1, 1, NA, NA, NA, 1, 1, 1, 1)
  a <- nautilus:::.paddleExpandWindows(short, centres, 100, step.size = 1,
                                       max.interp.gap = 2)
  b <- nautilus:::.paddleExpandWindows(long, centres, 100, step.size = 1,
                                       max.interp.gap = 2)
  expect_true(all(is.finite(a[30:40])))
  expect_true(all(is.na(b[30:50])))
  expect_true(all(is.na(a[1:9])))                 # never extrapolate beyond accepted centres
})

test_that("all-missing and constant windows fail cleanly", {
  a <- .getPaddleSpeed(rep(NA_real_, 1000), sampling.rate = 20)
  b <- .getPaddleSpeed(rep(3, 1000), sampling.rate = 20)
  expect_true(all(is.na(a$freq)))
  expect_identical(a$qc$dominant_failure, "missing_data")
  expect_true(all(is.na(b$freq)))
  expect_identical(b$qc$dominant_failure, "constant_signal")
})

test_that("paddleFrequencyControl validates the spectral contract", {
  ctrl <- paddleFrequencyControl()
  expect_s3_class(ctrl, "nautilus_paddle_frequency")
  expect_equal(ctrl$nyquist.guard, 0.9)
  expect_equal(ctrl$min.prominence, 20)
  expect_equal(ctrl$max.interp.gap, 2)
  expect_error(paddleFrequencyControl(nyquist.guard = 1), "strictly between")
  expect_error(paddleFrequencyControl(step.size = 6, window.size = 5), "must not exceed")
  expect_error(paddleFrequencyControl(max.freq.Hz = 0.05), "must be greater")
})
