# Tests for the zero-phase Butterworth filter primitives that replaced the boxcar moving-average
# filters in processTagData() (task #73): .filtfiltCorner() corner pre-compensation and .filterSegments()
# segment-aware high/low/band filtering, plus the .bandpassSegments() wrapper.

# helper: realised amplitude gain of a filter at a single frequency, over the settled interior
.gain_at <- function(f, type, target, order = 2, fs = 20, n = 24000) {
  t  <- seq_len(n) / fs
  fc <- nautilus:::.filtfiltCorner(target, order, type)
  x  <- sin(2 * pi * f * t)
  y  <- nautilus:::.filterSegments(x, fs, fc, type = type, order = order)
  ii <- round(0.2 * n):round(0.8 * n)
  max(abs(y[ii])) / max(abs(x[ii]))
}

# ---- .filtfiltCorner: type-aware reciprocal ----------------------------------------------

test_that(".filtfiltCorner is the reciprocal for high vs low, and matches the filtfilt algebra", {
  k <- (sqrt(2) - 1)^(1 / (2 * 2))                       # order 2
  expect_equal(nautilus:::.filtfiltCorner(0.25, 2, "low"),  0.25 / k)
  expect_equal(nautilus:::.filtfiltCorner(0.25, 2, "high"), 0.25 * k)
  # high and low corners are reciprocal about the target
  lo <- nautilus:::.filtfiltCorner(0.25, 2, "low")
  hi <- nautilus:::.filtfiltCorner(0.25, 2, "high")
  expect_equal(lo * hi, 0.25^2)
  expect_equal(nautilus:::.filtfiltCorner(0.25, 2), nautilus:::.filtfiltCorner(0.25, 2, "low"))  # default
})

test_that(".filtfiltCorner places the effective -3 dB at the target for BOTH types", {
  # after filtfilt (which squares |H|), the -3 dB point (gain 1/sqrt(2)) must land at the target
  expect_equal(.gain_at(0.2518, "high", 0.2518), 1 / sqrt(2), tolerance = 0.02)
  expect_equal(.gain_at(0.2215, "low",  0.2215), 1 / sqrt(2), tolerance = 0.02)
  # a naive (non-type-aware) corner would be a half-octave off: prove the high-pass is NOT ~0.707
  # at the target if the LOW correction were (wrongly) used
  wrong <- local({
    fc <- 0.2518 / (sqrt(2) - 1)^(1 / 4)                 # low-style corner on a high-pass
    t <- seq_len(24000) / 20; x <- sin(2 * pi * 0.2518 * t)
    y <- nautilus:::.filterSegments(x, 20, fc, type = "high", order = 2)
    ii <- 4800:19200; max(abs(y[ii])) / max(abs(x[ii]))
  })
  expect_true(wrong < 0.5)                               # badly attenuated -> the reciprocal matters
})

# ---- .filterSegments: frequency selectivity + DC handling --------------------------------

test_that("high-pass rejects DC and low frequencies, passes high frequencies", {
  expect_lt(.gain_at(0.05, "high", 0.25), 0.15)          # well below cutoff -> rejected
  expect_gt(.gain_at(1.00, "high", 0.25), 0.95)          # well above cutoff -> passed
  # a pure DC (gravity) input high-passes to ~0
  x <- rep(1.03, 4000)
  y <- nautilus:::.filterSegments(x, 20, nautilus:::.filtfiltCorner(0.25, 2, "high"), type = "high")
  expect_lt(max(abs(y)), 1e-6)
})

test_that("low-pass preserves the DC level and rejects high frequencies", {
  # a pure constant is preserved to machine precision (seg - mean == 0, re-add restores it exactly):
  # this is the property that keeps a smoothed energy proxy at its true magnitude
  xc <- rep(0.37, 4000)
  yc <- nautilus:::.filterSegments(xc, 20, nautilus:::.filtfiltCorner(0.22, 2, "low"), type = "low")
  expect_equal(yc, xc, tolerance = 1e-9)
  # DC + slow tone: the mean level survives (loose tol: the tone leaves a small edge residual)
  t <- seq_len(24000) / 20
  x <- 5 + sin(2 * pi * 0.05 * t)
  y <- nautilus:::.filterSegments(x, 20, nautilus:::.filtfiltCorner(0.22, 2, "low"), type = "low")
  expect_equal(mean(y, na.rm = TRUE), 5, tolerance = 0.01)
  expect_gt(.gain_at(0.05, "low", 0.22), 0.95)           # slow content passed
  expect_lt(.gain_at(1.00, "low", 0.22), 0.05)           # fast content rejected
})

test_that("static + dynamic == raw exactly (complementary split)", {
  set.seed(1); x <- cumsum(rnorm(5000)) / 50 + sin(2 * pi * 0.4 * seq_len(5000) / 20)
  dyn <- nautilus:::.filterSegments(x, 20, nautilus:::.filtfiltCorner(0.25, 2, "high"), type = "high")
  stat <- x - dyn
  expect_equal(stat + dyn, x)
})

# ---- NA / gap handling -------------------------------------------------------------------

test_that("all-NA input returns all-NA and never errors", {
  x <- rep(NA_real_, 500)
  y <- nautilus:::.filterSegments(x, 20, nautilus:::.filtfiltCorner(0.25, 2, "high"), type = "high")
  expect_true(all(is.na(y)))
  expect_length(y, 500)
})

test_that("gaps are respected: each finite run is filtered independently, runs shorter than min.seg are NA", {
  fs <- 20
  seg <- sin(2 * pi * 0.5 * seq_len(2000) / fs)
  x <- c(seg, rep(NA_real_, 50), seg)                    # two long runs split by a gap
  y <- nautilus:::.filterSegments(x, fs, nautilus:::.filtfiltCorner(0.25, 2, "high"), type = "high")
  expect_true(all(is.na(y[2001:2050])))                  # the gap stays NA
  expect_true(all(is.finite(y[1:2000])))                 # long runs filtered
  expect_true(all(is.finite(y[2051:4050])))
  # a run shorter than min.seg (round(fs) = 20) yields NA (cannot reliably filter a sub-second run)
  xs <- c(rep(NA_real_, 30), sin(2 * pi * 0.5 * seq_len(10) / fs), rep(NA_real_, 30))
  ys <- nautilus:::.filterSegments(xs, fs, nautilus:::.filtfiltCorner(0.25, 2, "high"), type = "high")
  expect_true(all(is.na(ys)))
})

# ---- .bandpassSegments wrapper -----------------------------------------------------------

test_that(".bandpassSegments passes an in-band tone and rejects out-of-band tones", {
  fs <- 20; t <- seq_len(12000) / fs
  x_in  <- sin(2 * pi * 0.8 * t)                          # inside 0.3-1.5
  x_lo  <- sin(2 * pi * 0.05 * t)                         # below band
  x_hi  <- sin(2 * pi * 5.0  * t)                          # above band
  amp <- function(y) { ii <- 2400:9600; max(abs(y[ii])) }
  expect_gt(amp(nautilus:::.bandpassSegments(x_in, fs, 0.3, 1.5, order = 4)), 0.9)
  expect_lt(amp(nautilus:::.bandpassSegments(x_lo, fs, 0.3, 1.5, order = 4)), 0.1)
  expect_lt(amp(nautilus:::.bandpassSegments(x_hi, fs, 0.3, 1.5, order = 4)), 0.1)
})
