# Tests for the Morlet CWT primitive (R/utils-cwt.R). Each pins a defect that is invisible without a
# known ground truth, and that the WaveletComp-based implementation it replaces actually exhibited.

.tone <- function(f0 = 1, fs = 20, dur = 300, amp = 1, noise = 0, seed = 1) {
  set.seed(seed)
  t <- seq(0, dur, by = 1 / fs)
  amp * sin(2 * pi * f0 * t) + stats::rnorm(length(t), 0, noise)
}

test_that(".cwtRidge recovers a known frequency without the scale-convention bias", {
  # Mapping the |W|^2 ridge through the wrong Fourier factor costs a silent ~1.4% at EVERY frequency,
  # which no amount of eyeballing a spectrogram would reveal.
  for (f0 in c(0.237, 0.5, 1.0, 2.0)) {
    r <- .cwtRidge(.tone(f0 = f0), fs = 20, min.freq = 0.1, max.freq = 3)
    expect_equal(stats::median(r$freq, na.rm = TRUE), f0, tolerance = 0.005)
  }
})

test_that(".cwtRidge recovers absolute amplitude, and it scales with the signal", {
  # The WaveletComp path could not do this at all: it standardised the series, so power was identical
  # across a 1000x amplitude change and no amplitude was recoverable.
  for (a in c(0.01, 1, 100)) {
    r <- .cwtRidge(.tone(f0 = 0.5, amp = a), fs = 20, min.freq = 0.1, max.freq = 3)
    expect_equal(stats::median(r$amp, na.rm = TRUE) / a, 1, tolerance = 0.02)
  }
})

test_that(".cwtRidge is invariant to the batch size", {
  # The single test that would have caught the per-batch standardisation defect: a glide-dominated
  # batch had its noise renormalised to unit variance and fabricated beats, so a memory knob moved the
  # science. Hostile input on purpose: a loud stretch and a near-silent one.
  fs <- 20
  x <- .tone(f0 = 0.8, fs = fs, dur = 600, noise = 0.02)
  t <- seq(0, 600, by = 1 / fs)
  x[t > 200 & t < 400] <- stats::rnorm(sum(t > 200 & t < 400), 0, 0.02)   # a silent stretch
  a <- .cwtRidge(x, fs, 0.1, 3, batch.s = 900)                            # one batch
  b <- .cwtRidge(x, fs, 0.1, 3, batch.s = 120)                            # several
  expect_identical(is.na(a$freq), is.na(b$freq))
  expect_lt(max(abs(a$freq - b$freq), na.rm = TRUE), 1e-8)                # floating point only
})

test_that(".cwtRidge resolves frequency below the scale grid", {
  # Without sub-bin interpolation the output is quantised to ~79 values over this band (~3.5% steps),
  # which is the size of the errors being measured.
  r <- .cwtRidge(.tone(f0 = 0.6, dur = 600), fs = 20, min.freq = 0.1, max.freq = 3)
  expect_gt(length(unique(r$freq[!is.na(r$freq)])), 100)
})

test_that(".cwtRidge masks the record ends via a cone of influence, and only the ends", {
  r <- .cwtRidge(.tone(f0 = 0.5, dur = 300), fs = 20, min.freq = 0.1, max.freq = 3)
  expect_true(is.na(r$freq[1]))                          # ends are edge-contaminated
  expect_true(is.na(r$freq[length(r$freq)]))
  expect_false(is.na(r$freq[length(r$freq) %/% 2]))      # the interior is not
  expect_lt(r$meta$pct_masked_coi, 40)                   # a units error here masked ~100x too much
})

test_that(".cwtRidge tolerates NA and reports nothing where it cannot", {
  x <- .tone(f0 = 0.5)
  x[c(50:60, 500:520)] <- NA_real_
  r <- .cwtRidge(x, fs = 20, min.freq = 0.1, max.freq = 3)
  expect_true(all(is.na(r$freq[c(50:60, 500:520)])))
  expect_equal(stats::median(r$freq, na.rm = TRUE), 0.5, tolerance = 0.01)
  expect_equal(.cwtRidge(rep(NA_real_, 100), 20, 0.1, 3)$freq, rep(NA_real_, 100))
})

test_that(".cwtRidge builds a spectrogram only when asked, within its column budget", {
  x <- .tone(f0 = 0.5, dur = 300)
  expect_null(.cwtRidge(x, 20, 0.1, 3)$meta$spectrogram)
  s <- .cwtRidge(x, 20, 0.1, 3, spectrogram = TRUE, spec.max.cols = 500)$meta$spectrogram
  expect_lte(ncol(s$power), 500)
  expect_equal(nrow(s$power), length(s$freqs))
})

# ---- prominence floor -------------------------------------------------------------------------------
# A per-column argmax always returns something. These tests pin the behaviour that separates "there is a
# resolvable oscillation here" from "there is only background", which is what the floor exists to do.

.rednoise <- function(n, seed = 42) {
  set.seed(seed)
  x <- cumsum(rnorm(n))
  x <- x - as.numeric(stats::filter(x, rep(1 / 401, 401), sides = 2))
  x[is.na(x)] <- 0
  x / stats::sd(x)
}

test_that("the floor withholds a frequency for noise with no oscillation, but keeps a real beat", {
  fs <- 20; n <- 20000L
  noise <- .rednoise(n)
  r_noise <- .cwtRidge(noise, fs, 0.09, 2.75)
  expect_gt(r_noise$meta$pct_masked_prominence, 50)            # most of a beat-free record is withheld
  t <- seq_len(n) / fs
  r_beat <- .cwtRidge(4 * sin(2 * pi * 0.5 * t) + noise, fs, 0.09, 2.75)
  expect_lt(r_beat$meta$pct_masked_prominence, 5)              # a clear beat is not touched
  expect_equal(stats::median(r_beat$freq, na.rm = TRUE), 0.5, tolerance = 0.05)
})

test_that("prominence separates noise from signal, and prominence = 0 restores the old behaviour", {
  fs <- 20; n <- 20000L
  noise <- .rednoise(n)
  t <- seq_len(n) / fs
  expect_lt(stats::median(.cwtRidge(noise, fs, 0.09, 2.75)$prominence, na.rm = TRUE), 1.5)
  expect_gt(stats::median(.cwtRidge(4 * sin(2 * pi * 0.5 * t) + noise, fs, 0.09, 2.75)$prominence,
                          na.rm = TRUE), 4)
  off <- .cwtRidge(noise, fs, 0.09, 2.75, prominence = 0)
  expect_equal(off$meta$pct_masked_prominence, 0)
  expect_false(all(is.na(off$freq)))                           # unguarded: a number for every sample
})

test_that("freq_raw keeps the unmasked ridge, so band-placement QC survives the floor", {
  fs <- 20; n <- 20000L
  r <- .cwtRidge(.rednoise(n), fs, 0.09, 2.75)
  expect_length(r$freq_raw, n)
  expect_gt(sum(!is.na(r$freq_raw)), sum(!is.na(r$freq)))      # strictly more retained than reported
  # every reported sample must also be present in the raw ridge, and with the same value
  keep <- !is.na(r$freq)
  expect_equal(r$freq[keep], r$freq_raw[keep])
})

test_that(".cwtScaleBackground medians away a single-scale peak but follows a slope", {
  # a spike on a flat background is removed; a monotone ramp is reproduced (so a slope is NOT a peak)
  LP <- matrix(0, 41, 3); LP[21, ] <- 10
  expect_equal(max(.cwtScaleBackground(LP, 1:41, 21L)), 0)
  ramp <- matrix(rep(seq_len(41), 3), 41, 3)
  expect_equal(.cwtScaleBackground(ramp, 1:41, 21L)[21, 1], 21)
})
