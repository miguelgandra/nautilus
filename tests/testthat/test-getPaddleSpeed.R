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
  expect_named(res, c("freq", "speed", "peak.prominence"))
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
