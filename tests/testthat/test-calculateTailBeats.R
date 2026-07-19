# Tests for calculateTailBeats(). Both paths need the 'signal' package for the band-pass, so those tests skip when
# it is unavailable; argument-validation tests run regardless.

.sway <- function(id = "A01", freq = 1.0, fs = 10, dur = 180, seed = 1) {
  set.seed(seed)
  t <- seq(0, dur, by = 1 / fs); n <- length(t)
  d <- data.table::data.table(ID = id, datetime = as.POSIXct("2020-01-01", tz = "UTC") + t,
                              sway = sin(2 * pi * freq * t) + stats::rnorm(n, 0, 0.05))
  data.table::setattr(d, "nautilus.version", "test")
  d
}

run_tb <- function(d, ...) {
  res <- NULL
  invisible(capture.output(suppressWarnings(suppressMessages(
    res <- calculateTailBeats(d, motion.col = "sway", min.freq.Hz = 0.2, max.freq.Hz = 3,
                              return.data = TRUE, verbose = FALSE, ...)))))
  res
}

# A duty-cycled swim/glide record. The glide is BROADBAND NOISE, never an attenuated copy of the beat:
# an attenuated sinusoid keeps producing one "beat" per cycle, so it leaves the beat population roughly
# unchanged and hides any gate that is derived from that population. That is precisely how the previous
# version of this test passed while the shipped gate reported a pure-glide record as 93% swimming.
.duty <- function(duty, fs = 20, dur = 600, beat = 1, noise = 0.02, seed = 1) {
  set.seed(seed)
  t <- seq(0, dur, by = 1 / fs); n <- length(t)
  swim <- if (duty >= 1) rep(TRUE, n) else if (duty <= 0) rep(FALSE, n) else (t %% (30 / duty)) < 30
  x <- stats::rnorm(n, 0, noise)
  x[swim] <- x[swim] + beat * sin(2 * pi * 0.8 * t[swim])
  list(x = x, swim = swim, fs = fs)
}

test_that(".tailBeatsPeaks recovers frequency and amplitude, and no longer gates", {
  skip_if_not_installed("signal")
  set.seed(1)
  fs <- 20; t <- seq(0, 120, by = 1 / fs)
  x <- 2 * sin(2 * pi * 1.0 * t) + stats::rnorm(length(t), 0, 0.02)
  r <- nautilus:::.tailBeatsPeaks(x, fs, min.freq = 0.2, max.freq = 3, filter.low = 0.5, filter.high = 3)
  expect_equal(median(r$tbf_hz, na.rm = TRUE), 1.0, tolerance = 0.05)
  expect_equal(median(r$tbf_amplitude, na.rm = TRUE), 4.0, tolerance = 0.5)   # peak-to-trough
  expect_null(r$swimming)     # the behaviour call belongs to .classifyActivity, not the detector
})

.pinknoise <- function(n, sd) {
  f <- stats::fft(stats::rnorm(n)); k <- pmax(seq_len(n) - 1, 1)
  v <- Re(stats::fft(f / sqrt(k), inverse = TRUE))[seq_len(n)]
  sd * v / stats::sd(v)
}

test_that("swimming is NOT inferred from the signal by default -- tbf_swimming is all NA", {
  skip_if_not_installed("signal")
  # Video-validated real data showed no self-referential rule can separate propulsion from tag motion
  # on a towed tag (annotated stationary vs ram feeding, p = 0.94). So the default withholds the call.
  for (m in c("peaks", "wavelet")) for (duty in c(0, 0.5, 1)) {
    d <- .duty(duty)
    dt <- data.table::data.table(ID = "A01",
                                 datetime = as.POSIXct("2020-01-01", tz = "UTC") + seq(0, 600, by = 1 / 20),
                                 sway = d$x)
    data.table::setattr(dt, "nautilus.version", "test")
    out <- NULL
    invisible(capture.output(suppressWarnings(
      out <- calculateTailBeats(dt, motion.col = "sway", min.freq.Hz = 0.2, max.freq.Hz = 3,
                                method = m, verbose = FALSE)[[1]])))
    expect_true(all(is.na(out$tbf_swimming)), info = paste(m, duty))
    # and where a beat is present the oscillation IS still reported -- not nulled by a phantom gate
    if (duty > 0) expect_gt(mean(!is.na(out$tbf_hz)), 0.4)
  }
})

test_that("pct_swimming is recorded as NA, never as a fabricated number, when unclassified", {
  skip_if_not_installed("signal")
  out <- run_tb(list(A01 = .sway(freq = 1.0)))$A01
  step <- Filter(function(p) identical(p$step, "calculateTailBeats"), nautilus:::.getMeta(out)$processing)[[1]]
  expect_true(is.na(step$pct_swimming))
})

test_that("supplying min.amplitude turns classification on, and enforces the NA contract", {
  skip_if_not_installed("signal")
  # an absolute reference is the ONLY thing that enables the swimming call; then tbf_hz is nulled on
  # gliding rows so summarizeTagData()'s average is over swimming only
  d <- .duty(0.5)
  dt <- data.table::data.table(ID = "A01",
                               datetime = as.POSIXct("2020-01-01", tz = "UTC") + seq(0, 600, by = 1 / 20),
                               sway = d$x)
  data.table::setattr(dt, "nautilus.version", "test")
  out <- NULL
  invisible(capture.output(suppressWarnings(
    out <- calculateTailBeats(dt, motion.col = "sway", min.freq.Hz = 0.2, max.freq.Hz = 3,
                              method = "peaks", min.amplitude = 0.5, verbose = FALSE)[[1]])))
  expect_false(all(is.na(out$tbf_swimming)))                 # the call was made
  expect_equal(mean(out$tbf_swimming, na.rm = TRUE), 0.5, tolerance = 0.15)  # tracks the true duty
  glide <- !is.na(out$tbf_swimming) & !out$tbf_swimming
  expect_true(all(is.na(out$tbf_hz[glide])))                 # contract holds under classification
})

test_that("min.amplitude does not perturb the reported frequency (estimation decoupled from interpretation)", {
  skip_if_not_installed("signal")
  # an amplitude-modulated beat: half the beats are small. A classification threshold set above the
  # small beats must NOT make the peak detector skip them and report half the true frequency.
  set.seed(1); fs <- 20; t <- seq(0, 300, by = 1 / fs); n <- length(t)
  amp <- ifelse((t %% 8) < 4, 2, 0.4)
  x <- amp * sin(2 * pi * 1.5 * t) + stats::rnorm(n, 0, 0.02)
  d <- data.table::data.table(ID = "A01", datetime = as.POSIXct("2020-01-01", tz = "UTC") + t, sway = x)
  data.table::setattr(d, "nautilus.version", "test")
  freq_of <- function(ma) {
    o <- NULL
    invisible(capture.output(suppressWarnings(
      o <- calculateTailBeats(data.table::copy(d), motion.col = "sway", min.freq.Hz = 0.2, max.freq.Hz = 3,
                              method = "peaks", min.amplitude = ma, verbose = FALSE)[[1]])))
    median(o$tbf_hz, na.rm = TRUE)
  }
  f_none <- freq_of(NULL)
  expect_equal(f_none, 1.5, tolerance = 0.1)
  expect_equal(freq_of(0.9), f_none, tolerance = 0.05)   # setting a classification threshold must not halve it
})

test_that("a constant / zero-oscillation signal reports NA, not the band ceiling", {
  skip_if_not_installed("signal")
  r <- nautilus:::.cwtRidge(rep(5, 3000), fs = 20, min.freq = 0.1, max.freq = 3)
  expect_true(all(is.na(r$freq)))                        # DC used to pin every interior sample to 3 Hz
})

test_that("min.amplitude = 0 is rejected", {
  skip_if_not_installed("signal")
  expect_error(calculateTailBeats(list(A01 = .sway()), motion.col = "sway", min.amplitude = 0,
                                  verbose = FALSE), "positive amplitude")
})

test_that(".classifyActivity thresholds against min.amplitude, and withholds without it", {
  skip_if_not_installed("signal")
  glide <- nautilus:::.bandpassSegments(.duty(0)$x, 20, 0.18, 3.3, 4)
  swim  <- nautilus:::.bandpassSegments(.duty(1)$x, 20, 0.18, 3.3, 4)
  # with a reference: a pure glide is < threshold, a pure swim is > it
  expect_lt(mean(nautilus:::.classifyActivity(glide, 20, min.amplitude = 0.5)$swimming, na.rm = TRUE), 0.05)
  expect_gt(mean(nautilus:::.classifyActivity(swim, 20, min.amplitude = 0.5)$swimming, na.rm = TRUE), 0.95)
  # without one: all NA, and the source says why
  g <- nautilus:::.classifyActivity(swim, 20)
  expect_true(all(is.na(g$swimming)))
  expect_identical(g$source, "not classified")
  expect_true(is.finite(g$envelope.median))     # a scale the caller can size a min.amplitude from
})

test_that(".tailBeatsPeaks recovers a different frequency (0.5 Hz)", {
  skip_if_not_installed("signal")
  set.seed(1)                                                # deterministic noise draw
  fs <- 20; t <- seq(0, 120, by = 1 / fs)
  x <- 2 * sin(2 * pi * 0.5 * t) + stats::rnorm(length(t), 0, 0.02)
  r <- nautilus:::.tailBeatsPeaks(x, fs, min.freq = 0.2, max.freq = 3, filter.low = 0.2, filter.high = 3)
  expect_equal(median(r$tbf_hz, na.rm = TRUE), 0.5, tolerance = 0.05)
})

test_that("the peaks method recovers frequency + amplitude and adds the expected columns", {
  skip_if_not_installed("signal")
  out <- run_tb(list(A01 = .sway(freq = 1.0)), method = "peaks")$A01
  expect_true(all(c("tbf_hz", "tbf_amplitude", "tbf_swimming") %in% names(out)))
  expect_equal(median(out$tbf_hz, na.rm = TRUE), 1.0, tolerance = 0.1)
  expect_gt(median(out$tbf_amplitude, na.rm = TRUE), 0)
  expect_false("tbf_hz_alt" %in% names(out))            # one backend named -> no cross-check
  # QC stats recorded in the audit trail
  step <- Filter(function(p) identical(p$step, "calculateTailBeats"), nautilus:::.getMeta(out)$processing)[[1]]
  expect_equal(step$method, "peaks"); expect_equal(step$axis, "sway")
  expect_true(all(c("median_tbf_hz", "median_amplitude", "pct_swimming") %in% names(step)))
})

test_that("naming both backends runs both and cross-checks them", {
  skip_if_not_installed("signal")
  out <- run_tb(list(A01 = .sway(freq = 1.0)))$A01          # the default names both
  expect_true(all(c("tbf_hz", "tbf_hz_alt", "tbf_agree") %in% names(out)))
  expect_equal(median(out$tbf_hz, na.rm = TRUE), 1.0, tolerance = 0.1)
  expect_equal(median(out$tbf_hz_alt, na.rm = TRUE), 1.0, tolerance = 0.1)
  expect_gt(mean(out$tbf_agree, na.rm = TRUE), 0.9)        # a clean tone: the two should agree
  step <- Filter(function(p) identical(p$step, "calculateTailBeats"), nautilus:::.getMeta(out)$processing)[[1]]
  expect_equal(step$method, "peaks + wavelet")
  expect_true(all(c("pct_edge", "pct_agree") %in% names(step)))
})

.tone_tag <- function(f0, fs = 40, dur = 300, seed = 1) {
  set.seed(seed); t <- seq(0, dur, by = 1 / fs)
  d <- data.table::data.table(ID = "A01", datetime = as.POSIXct("2020-01-01", tz = "UTC") + t,
                              sway = sin(2 * pi * f0 * t) + stats::rnorm(length(t), 0, 0.05))
  data.table::setattr(d, "nautilus.version", "test"); d
}

test_that("a beat just outside the band piles up on the edge, and edge occupancy records it", {
  skip_if_not_installed("signal")
  # 3.5 Hz against a 0.2-3 Hz band leaks through the filter transition and the wavelet clamps it to the
  # ceiling, so estimates pile up at the edge -- which edge occupancy is there to detect. (Behavioural
  # classification is off by default, so this is about band placement, not swimming.)
  invisible(capture.output(out <- suppressWarnings(
    calculateTailBeats(.tone_tag(3.5), motion.col = "sway", min.freq.Hz = 0.2, max.freq.Hz = 3,
                       verbose = FALSE)[[1]])))
  step <- Filter(function(p) identical(p$step, "calculateTailBeats"), nautilus:::.getMeta(out)$processing)[[1]]
  expect_gt(step$pct_edge, 5)
  expect_true(is.na(step$pct_swimming))         # not classified without min.amplitude
})

test_that("a beat well inside the band does not trip edge occupancy", {
  skip_if_not_installed("signal")
  invisible(capture.output(out <- suppressWarnings(
    calculateTailBeats(.tone_tag(1.5), motion.col = "sway", min.freq.Hz = 0.2, max.freq.Hz = 3,
                       verbose = FALSE)[[1]])))
  step <- Filter(function(p) identical(p$step, "calculateTailBeats"), nautilus:::.getMeta(out)$processing)[[1]]
  expect_lt(step$pct_edge, 5)                # no false positive on a correctly-banded record
  expect_gt(step$pct_agree, 90)             # the two backends agree on a clean in-band tone
})

test_that("the wavelet method recovers frequency and now also amplitude", {
  skip_if_not_installed("signal")
  out <- run_tb(list(A01 = .sway(freq = 1.0)), method = "wavelet")$A01
  expect_true(all(c("tbf_hz", "tbf_amplitude") %in% names(out)))
  expect_equal(median(out$tbf_hz, na.rm = TRUE), 1.0, tolerance = 0.1)
  expect_equal(median(out$tbf_amplitude, na.rm = TRUE), 1.0, tolerance = 0.15)  # unit sine in
  expect_false("tbf_power_ratio" %in% names(out))   # amplitude-invariant and knob-dependent: removed
})

test_that("the wavelet amplitude is corrected for band-pass attenuation at the band edges", {
  skip_if_not_installed("signal")
  # A unit tone AT the band floor loses ~29% of its amplitude to the filter; uncorrected, that reads as
  # a real drop in swimming effort rather than an artefact.
  fs <- 20; t <- seq(0, 600, by = 1 / fs)
  d <- data.table::data.table(ID = "A01", datetime = as.POSIXct("2020-01-01", tz = "UTC") + t,
                              sway = sin(2 * pi * 0.2 * t))
  data.table::setattr(d, "nautilus.version", "test")
  out <- NULL
  invisible(capture.output(suppressWarnings(
    out <- calculateTailBeats(d, motion.col = "sway", min.freq.Hz = 0.2, max.freq.Hz = 3,
                              method = "wavelet", verbose = FALSE)[[1]])))
  expect_equal(median(out$tbf_amplitude, na.rm = TRUE), 1.0, tolerance = 0.05)
})

test_that(".bandpassPowerGain is bounded across the band, so it cannot amplify noise", {
  skip_if_not_installed("signal")
  g <- .bandpassPowerGain(seq(0.1, 3, length.out = 256), 20, 0.09, 3.3, 4)
  expect_true(all(g > 0 & g <= 1 + 1e-8))     # a Butterworth cannot amplify
  expect_lt(max(1 / g), 2)                    # so the correction stays bounded (measured ~1.41x)
})

test_that("both methods emit the same core columns", {
  skip_if_not_installed("signal")
  d <- list(A01 = .sway(freq = 1.0))
  cols <- function(m) names(run_tb(d, method = m)$A01)
  expect_true(all(c("tbf_hz", "tbf_amplitude") %in% cols("peaks")))
  expect_true(all(c("tbf_hz", "tbf_amplitude") %in% cols("wavelet")))
})

test_that("the wavelet band-pass fallback runs on a short series (no undefined-variable crash)", {
  skip_if_not_installed("signal")
  # a series shorter than filter.order*6 (=24) forces the 'insufficient data for filtering' fallback,
  # which previously assigned filtered_motion <- original_motion (an undefined variable) and errored.
  short <- .sway(freq = 1.0, dur = 2, fs = 10)                 # 21 samples
  expect_error(run_tb(list(A01 = short), method = "wavelet"), NA)
})

test_that("arguments removed with the WaveletComp engine are rejected, not silently ignored", {
  skip_if_not_installed("signal")
  d <- list(A01 = .sway())
  for (a in list(list(max.rows.per.batch = 1e4), list(ridge.only = FALSE),
                 list(power.ratio.threshold = 3), list(n.cores = 2))) {
    expect_error(do.call(calculateTailBeats,
                         c(list(d, motion.col = "sway", verbose = FALSE), a)), "unused argument")
  }
})

test_that("a different frequency is recovered (0.5 Hz)", {
  skip_if_not_installed("signal")
  out <- run_tb(list(A01 = .sway(freq = 0.5)))$A01
  expect_equal(median(out$tbf_hz, na.rm = TRUE), 0.5, tolerance = 0.1)
})

test_that("argument validation errors clearly", {
  skip_if_not_installed("signal")
  d <- list(A01 = .sway())
  expect_error(calculateTailBeats(d, motion.col = "sway", min.freq.Hz = -1, verbose = FALSE), "min.freq.Hz")
  expect_error(calculateTailBeats(d, motion.col = "sway", min.freq.Hz = 3, max.freq.Hz = 1, verbose = FALSE), "less than")
})

test_that("plot.file writes a multi-page PDF without touching the active device", {
  skip_if_not_installed("signal")
  pfile <- file.path(tempdir(), paste0("tb_", as.integer(runif(1, 1, 1e7)), ".pdf"))
  on.exit(unlink(pfile), add = TRUE)
  if (!is.null(grDevices::dev.list())) grDevices::graphics.off()
  run_tb(list(A01 = .sway()), plot.file = pfile)
  expect_true(file.exists(pfile))
  expect_gt(file.size(pfile), 0)
  expect_null(grDevices::dev.list())
})

test_that("the wavelet spectrogram renders on a record at a real epoch", {
  skip_if_not_installed("signal")
  # image() demands a regular x grid. Adding the POSIXct epoch (~1.6e9) to a grid spaced in fractions
  # of a second loses that regularity to floating point, so this errored on every real record while
  # passing on any synthetic one starting at t = 0.
  pfile <- file.path(tempdir(), paste0("tb_spec_", as.integer(runif(1, 1, 1e7)), ".pdf"))
  on.exit(unlink(pfile), add = TRUE)
  expect_error(
    invisible(capture.output(suppressWarnings(
      calculateTailBeats(.sway(fs = 10, dur = 300, freq = 1.0), motion.col = "sway",
                         min.freq.Hz = 0.2, max.freq.Hz = 3, method = "wavelet",
                         plot.file = pfile, verbose = FALSE)))),
    NA)
  expect_gt(file.size(pfile), 1000)
})

test_that("an unstable filter is caught even when it diverges to Inf", {
  skip_if_not_installed("signal")
  # +Inf is the most extreme instability there is, and the guard treated it as "not finite, therefore
  # nothing to judge" -- passing the very case the check exists for.
  x <- c(1, -1, 1, -1)
  expect_error(nautilus:::.assertFilterStable(x, c(Inf, 1, 1, 1), 20, 0.09, 3.3, 4), "numerically unstable")
  expect_true(nautilus:::.assertFilterStable(x, rep(NA_real_, 4), 20, 0.09, 3.3, 4))  # nothing filtered
  expect_true(nautilus:::.assertFilterStable(x, x, 20, 0.09, 3.3, 4))                 # pass-through
})

test_that("duplicated or out-of-order timestamps are rejected, not silently halved", {
  skip_if_not_installed("signal")
  # A duplicated record still reports the right sampling rate (the zero gap is discarded before the
  # median), but the signal is stretched, so every cycle spans twice the samples and every frequency
  # halves. Both backends read the same clock, so they agree and tbf_agree certifies the wrong answer.
  d <- .sway(freq = 1.0)
  dup <- d[rep(seq_len(nrow(d)), each = 2)]
  data.table::setattr(dup, "nautilus.version", "test")
  expect_error(calculateTailBeats(dup, motion.col = "sway", verbose = FALSE), "increase strictly")
  rev_d <- d[rev(seq_len(nrow(d)))]
  data.table::setattr(rev_d, "nautilus.version", "test")
  expect_error(calculateTailBeats(rev_d, motion.col = "sway", verbose = FALSE), "increase strictly")
})

test_that("an individual with no usable motion still gets the full schema, an audit entry, and a file", {
  skip_if_not_installed("signal")
  d <- .sway(freq = 1.0); d$sway <- NA_real_                # no usable motion
  dir <- file.path(tempdir(), paste0("tb_skip_", as.integer(runif(1, 1, 1e7)))); dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE), add = TRUE)
  out <- NULL
  invisible(capture.output(suppressWarnings(
    out <- calculateTailBeats(list(A01 = d), motion.col = "sway", verbose = FALSE, output.dir = dir)$A01)))
  # full schema, not just tbf_hz
  expect_true(all(c("tbf_hz", "tbf_amplitude", "tbf_swimming", "tbf_hz_alt", "tbf_agree") %in% names(out)))
  # an audit-trail entry exists and is marked
  step <- Filter(function(p) identical(p$step, "calculateTailBeats"), nautilus:::.getMeta(out)$processing)[[1]]
  expect_equal(step$note, "no valid motion data")
  # and the file was written
  expect_true(file.exists(file.path(dir, "A01.rds")))
})

test_that("a mistyped method name is rejected, not silently dropped", {
  skip_if_not_installed("signal")
  expect_error(calculateTailBeats(list(A01 = .sway()), motion.col = "sway",
                                  method = c("peaks", "wavlet"), verbose = FALSE), "Unknown")
})

test_that("calculateTailBeats errors fast when plot.file's directory is missing", {
  skip_if_not_installed("signal")
  expect_error(run_tb(list(A01 = .sway()), plot.file = file.path(tempdir(), "no_dir_qq", "x.pdf")), "does not exist")
})

test_that("verbose = FALSE is silent", {
  skip_if_not_installed("signal")
  out <- capture.output(suppressWarnings(suppressMessages(
    res <- calculateTailBeats(list(A01 = .sway()), motion.col = "sway", min.freq.Hz = 0.2,
                              max.freq.Hz = 3, verbose = FALSE))))
  expect_length(out, 0)
})

test_that("verbose output is a standardized cli diagnostic block (no legacy cat/crayon cruft)", {
  skip_if_not_installed("signal")
  grab <- function(v) paste(cli::cli_fmt(suppressWarnings(
    calculateTailBeats(list(A01 = .sway()), motion.col = c("sway", "heave"), min.freq.Hz = 0.2,
                       max.freq.Hz = 3, verbose = v))), collapse = "\n")
  d2 <- grab(2); d1 <- grab(1)
  expect_match(d2, "calculateTailBeats")                 # framed header
  expect_match(d2, "Smoothing:")                         # fixed config (smoothing) lives in the header
  expect_match(d2, "A01 \\(1/1\\)")                      # per-individual cli sub-header
  # the detailed block reports findings (key: value), not step narration
  expect_match(d2, "Methods: peaks \\+ wavelet")         # default runs both -> header names both ("Methods:")
  expect_match(d2, "Bandpass: [0-9.]+ . [0-9.]+ Hz")     # band is fixed config: its own header line, not per deployment
  expect_match(d2, "input:.*Hz")                         # rows | sampling rate | duration (fs shown ONCE, here)
  expect_match(d2, "axis: sway")                         # selected axis (single candidate present here)
  expect_match(d2, "swimming:")                          # merged activity + behaviour line
  expect_match(d2, "frequency: median")                  # dedicated frequency line
  expect_match(d2, "amplitude: median.*g")               # dedicated amplitude line (with unit)
  # noise removed from the per-deployment block: sampling headroom (folded into input), the per-deployment
  # bandpass line (now a header line), the peaks-only detection line, and the old split activity/behaviour
  expect_false(grepl("sampling:", d2, fixed = TRUE))
  expect_false(grepl("detection:", d2, fixed = TRUE))
  expect_false(grepl("wavelet:", d2, fixed = TRUE))
  expect_false(grepl("activity:", d2, fixed = TRUE))
  expect_false(grepl("behaviour:", d2, fixed = TRUE))
  expect_match(d2, "SUMMARY")
  expect_match(d2, "1 of 1 tag processed")
  expect_false(grepl("smoothing:", d2, fixed = TRUE))    # per-block smoothing line moved to the header
  expect_false(grepl("[1/1]", d2, fixed = TRUE))         # legacy crayon "[i/n]" header gone
  expect_false(grepl("input:", d1, fixed = TRUE))        # the diagnostic block is level-2 only
  expect_match(d1, "median")                             # normal level keeps the compact one-line outcome
  expect_match(d1, "swimming")
})

test_that(".reportTailBeatCohort rolls up the cohort, showing conditional lines only when they apply", {
  grab <- function(...) paste(cli::cli_fmt(nautilus:::.reportTailBeatCohort(2L, ...)), collapse = "\n")

  # rich case: mixed axes, two methods, swimming classified, and every QC flag tripped
  rich <- grab(n_total = 5L,
               freq   = c(0.80, 0.45, 1.20, 0.60, 0.90),
               axis   = c("surge", "surge", "heave", "sway", "surge"),
               reason = c("consensus", "power", "consensus", "power", "consensus"),
               harm   = c(NA, "heave", NA, NA, NA),
               agree  = c(0.95, 0.70, 0.99, 0.60, 0.90),
               swim   = c(0.5, 0.4, 0.6, 0.3, 0.5),
               edge   = c(0.0, 0.20, 0.0, 0.0, 0.0),
               methods = c("peaks", "wavelet"))
  expect_match(rich, "5 of 5 tags processed")
  expect_match(rich, "tail-beat frequency: median 0.80 Hz \\(IQR 0.60.0.90, range 0.45.1.20 Hz\\)")
  expect_match(rich, "axis used: surge 3 .")                             # tally, most-used first
  expect_match(rich, "sway 1"); expect_match(rich, "heave 1")            # the rest present (tie order not pinned)
  expect_match(rich, "method agreement: median 90% \\(peaks vs wavelet\\)")
  expect_match(rich, "swimming: median 50% across tags")
  expect_match(rich, "flags: 1 near band edge . 1 possible harmonic . 2 axis chosen without consensus")

  # clean batch: single axis, one method, nothing flagged -> only outcome + frequency
  clean <- grab(n_total = 3L, freq = c(0.5, 0.6, 0.7), axis = rep("sway", 3),
                reason = rep("consensus", 3), harm = rep(NA_character_, 3), agree = rep(NA_real_, 3),
                swim = rep(NA_real_, 3), edge = rep(0, 3), methods = "peaks")
  expect_match(clean, "3 of 3 tags processed")
  expect_match(clean, "tail-beat frequency: median")
  for (line in c("axis used:", "method agreement:", "swimming:", "flags:"))
    expect_false(grepl(line, clean, fixed = TRUE), info = line)          # each omitted when it does not apply

  # some tags produced no estimate -> the outcome tail spells it out
  noest <- grab(n_total = 4L, freq = c(0.5, NA, 0.7, NA), axis = c("sway", NA, "sway", NA),
                reason = c("consensus", NA, "consensus", NA), harm = rep(NA_character_, 4),
                agree = rep(NA_real_, 4), swim = rep(NA_real_, 4), edge = rep(NA_real_, 4), methods = "peaks")
  expect_match(noest, "4 tags processed . 2 with a tail-beat estimate \\(2 no signal\\)")

  # a single estimate -> a point value, no IQR/range
  one <- grab(n_total = 1L, freq = 0.62, axis = "sway", reason = "consensus", harm = NA_character_,
              agree = NA_real_, swim = NA_real_, edge = 0, methods = "peaks")
  expect_match(one, "tail-beat frequency: 0.62 Hz")
  expect_false(grepl("IQR", one, fixed = TRUE))
})

test_that("the run's SUMMARY block reports cohort frequency and the axis tally end to end", {
  skip_if_not_installed("signal")
  mk <- function(id, f) { set.seed(1); fs <- 20; t <- seq(0, 200, by = 1 / fs); n <- length(t)
    data.table::data.table(ID = id, datetime = as.POSIXct("2020-01-01", tz = "UTC") + t,
                           surge = sin(2 * pi * f * t) + stats::rnorm(n, 0, 0.05),
                           sway = stats::rnorm(n, 0, 0.05), heave = stats::rnorm(n, 0, 0.05)) }
  out <- paste(cli::cli_fmt(suppressWarnings(
    calculateTailBeats(list(A = mk("A", 0.8), B = mk("B", 1.1)), motion.col = c("surge", "sway", "heave"),
                       method = "peaks", min.freq.Hz = 0.1, max.freq.Hz = 2.5, verbose = 1))), collapse = "\n")
  expect_match(out, "SUMMARY")
  expect_match(out, "2 of 2 tags processed")
  expect_match(out, "tail-beat frequency: median")
})

test_that("smoothing no longer leaks the data.table 'hasNA' deprecation warning", {
  skip_if_not_installed("signal")
  w <- character(0)
  withCallingHandlers(
    suppressMessages(calculateTailBeats(list(A01 = .sway()), motion.col = "sway", min.freq.Hz = 0.2,
                                        max.freq.Hz = 3, smooth.window = 3, verbose = FALSE)),
    warning = function(cond) { w <<- c(w, conditionMessage(cond)); invokeRestart("muffleWarning") })
  expect_false(any(grepl("hasNA", w, fixed = TRUE)))     # the deprecated arg is gone, no leaked warning
})

test_that("a sampling rate exactly at Nyquist is rejected, not merely called marginal", {
  d <- .sway(fs = 6, dur = 60)                           # fs == 2 * max.freq.Hz exactly
  expect_error(suppressWarnings(calculateTailBeats(d, motion.col = "sway", min.freq.Hz = 0.2,
                                                   max.freq.Hz = 3, verbose = FALSE)),
               "Insufficient sampling frequency")
})

test_that("an unstable band-pass aborts instead of returning finite garbage, on both methods", {
  skip_if_not_installed("signal")
  # At 250 Hz a 0.09-3.3 Hz band is 0.07% of Nyquist, leaving the order-8 transfer function so
  # ill-conditioned that filtfilt returns ~1e15 from a unit-amplitude input -- finite, no warning, no NA.
  # Whether it fires is chaotic in fs (250.0000 explodes, 250.0032 does not), so the rate is pinned via
  # the attribute processTagData sets rather than inferred from timestamps.
  d <- .sway(fs = 250, dur = 60, freq = 0.5)
  data.table::setattr(d, "processed.sampling.frequency", 250)
  expect_error(suppressWarnings(calculateTailBeats(d, motion.col = "sway", min.freq.Hz = 0.1,
                                                   max.freq.Hz = 3, method = "peaks", verbose = FALSE)),
               "numerically unstable")
  skip_if_not_installed("signal")
  expect_error(suppressWarnings(calculateTailBeats(d, motion.col = "sway", min.freq.Hz = 0.1,
                                                   max.freq.Hz = 3, method = "wavelet", verbose = FALSE)),
               "numerically unstable")
})

test_that(".assertFilterStable passes a legitimate filter and an unfiltered pass-through", {
  set.seed(1); x <- sin(2 * pi * 0.5 * seq(0, 60, by = 1 / 20)) + rnorm(1201, 0, 0.05)
  expect_true(.assertFilterStable(x, nautilus:::.bandpassSegments(x, 20, 0.09, 3.3, 4), 20, 0.09, 3.3, 4))
  expect_true(.assertFilterStable(x, x, 20, 0.09, 3.3, 4))          # the unfiltered fallback path
  expect_true(.assertFilterStable(x, rep(NA_real_, length(x)), 20, 0.09, 3.3, 4))  # nothing filtered
})

test_that("max.freq.Hz above the whale-shark range is not discouraged for small-bodied species", {
  skip_if_not_installed("signal")
  w <- character(0)
  withCallingHandlers(
    invisible(calculateTailBeats(.sway(fs = 60, dur = 20), motion.col = "sway", min.freq.Hz = 1,
                                 max.freq.Hz = 10, method = "peaks", verbose = FALSE, plot = FALSE)),
    warning = function(cond) { w <<- c(w, conditionMessage(cond)); invokeRestart("muffleWarning") })
  expect_false(any(grepl("unusually high", w)))   # 10 Hz is correct for a small fish (~2-20 Hz)
})

test_that(".smoothSeries ignores scattered gaps instead of propagating them", {
  set.seed(1)
  fs <- 20; n <- 12000                                   # 600 s @ 20 Hz
  x <- rep(0.8, n); x[sample.int(n, round(0.02 * n))] <- NA_real_
  out <- .smoothSeries(x, smooth.window = 10, fs = fs)
  expect_lt(mean(is.na(out)), 0.05)                      # 2% gaps in; stats::filter used to give ~98% out
  expect_equal(stats::median(out, na.rm = TRUE), 0.8)    # and the surviving track is still the truth
})

test_that(".smoothSeries treats a sub-sample window as a no-op, never as a zero-width filter", {
  x <- rep(0.8, 100)
  expect_equal(.smoothSeries(x, smooth.window = 0.02, fs = 20), x)   # round(0.4) = 0 used to zero the series
  expect_identical(.smoothSeries(x, smooth.window = 0, fs = 20), x)  # disabled is untouched, not smoothed
})

test_that(".smoothSeries keeps an all-NA window as NA rather than NaN", {
  out <- .smoothSeries(c(rep(NA_real_, 20), rep(1, 5)), smooth.window = 1, fs = 5)
  expect_true(all(is.na(out)[1:15]))
  expect_false(any(is.nan(out)))                         # frollmean yields NaN for an empty window
})

test_that("the wavelet method survives scattered gaps in the motion channel", {
  skip_if_not_installed("signal")
  d <- .sway(fs = 10, dur = 300, freq = 1.0)
  set.seed(2)
  d$sway[sample.int(nrow(d), round(0.02 * nrow(d)))] <- NA_real_
  r <- run_tb(d, method = "wavelet", smooth.window = 10)
  expect_lt(mean(is.na(r[[1]]$tbf_hz)), 0.5)             # the whole track went NA before the shared smoother
})

test_that("output.dir writes a file that carries the calculateTailBeats audit-trail entry", {
  skip_if_not_installed("signal")
  dir <- file.path(tempdir(), paste0("tb_save_", as.integer(runif(1, 1, 1e7))))
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE), add = TRUE)
  run_tb(list(A01 = .sway()), output.dir = dir)
  f <- file.path(dir, "A01.rds")
  expect_true(file.exists(f))
  saved <- readRDS(f)
  step <- Filter(function(p) identical(p$step, "calculateTailBeats"), nautilus:::.getMeta(saved)$processing)
  expect_length(step, 1)                                 # the QC entry is present on disk, not just in memory
})

# ---- axis selection: cross-axis frequency consensus (#77) ----------------------------------

# Three-axis fixture: two axes share a locomotor tone; a third carries a LOUDER artefact at a NON-harmonic
# ratio (~0.75x, the real tow-pendulum pattern -- NOT 0.5x, which would be a genuine sub-harmonic). fs high
# enough and record long enough for a clean Welch peak.
.multiaxis <- function(loco = 0.8, artefact = 0.6, artefact.amp = 2, fs = 20, dur = 400, seed = 3) {
  set.seed(seed)
  t <- seq(0, dur, by = 1 / fs); n <- length(t)
  d <- data.table::data.table(
    ID = "M01", datetime = as.POSIXct("2020-01-01", tz = "UTC") + t,
    surge = 0.5 * sin(2 * pi * loco * t)      + stats::rnorm(n, 0, 0.03),   # locomotor
    heave = 0.5 * sin(2 * pi * loco * t + 1)  + stats::rnorm(n, 0, 0.03),   # locomotor (same freq)
    sway  = artefact.amp * sin(2 * pi * artefact * t) + stats::rnorm(n, 0, 0.03))  # loud artefact
  data.table::setattr(d, "nautilus.version", "test")
  d
}

test_that(".dominantInbandFreq recovers a tone via Welch averaging, ignoring low-frequency drift", {
  fs <- 20; t <- seq(0, 400, by = 1 / fs)
  x <- sin(2 * pi * 0.8 * t) + 3 * sin(2 * pi * 0.03 * t)   # strong sub-band drift + weaker 0.8 tone
  expect_equal(nautilus:::.dominantInbandFreq(x, fs, 0.1, 3), 0.8, tolerance = 0.02)
  expect_true(is.na(nautilus:::.dominantInbandFreq(rep(1, 5), fs, 0.1, 3)))   # too little data
})

test_that(".selectMotionAxis picks a cross-axis-corroborated axis, not the loudest artefact", {
  d <- .multiaxis()   # loco 0.8 on surge+heave, louder non-harmonic artefact 0.6 on sway (pendulum-like)
  sel <- nautilus:::.selectMotionAxis(d, c("surge", "sway", "heave"), 20, 0.1, 3)
  expect_true(sel$axis %in% c("surge", "heave"))   # the corroborated locomotor pair, NOT the loud sway
  expect_identical(sel$reason, "consensus")
  expect_true(sel$agree)
  expect_equal(unname(sel$freqs["sway"]), 0.6, tolerance = 0.03)   # artefact peak identified, just not chosen
})

test_that(".selectMotionAxis: single candidate returns it unlabelled; disagreement falls back to power", {
  d <- .multiaxis()
  s1 <- nautilus:::.selectMotionAxis(d, "sway", 20, 0.1, 3)
  expect_identical(s1$axis, "sway"); expect_identical(s1$reason, "single"); expect_true(is.na(s1$agree))
  # surge (0.8) vs sway (0.6) do not corroborate -> power fallback picks the louder sway, flagged unsupported
  s2 <- nautilus:::.selectMotionAxis(d, c("surge", "sway"), 20, 0.1, 3)
  expect_identical(s2$axis, "sway"); expect_identical(s2$reason, "power"); expect_false(s2$agree)
})

test_that(".selectMotionAxis DETECTS a possible 2f harmonic but never re-points (cannot be resolved from the signal)", {
  # A lateral swimmer: fundamental on ONE axis (sway, f), the 2f harmonic shared by surge+heave. The
  # corroborated 2f pair wins, but a stronger peak sits at ~half on sway: flag it, do NOT silently re-point.
  d <- .multiaxis(loco = 1.4, artefact = 0.7, artefact.amp = 3)   # surge/heave 1.4 Hz, sway 0.7 Hz (=1.4/2)
  sel <- nautilus:::.selectMotionAxis(d, c("surge", "sway", "heave"), 20, 0.1, 3)
  expect_true(sel$axis %in% c("surge", "heave"))     # keeps the corroborated pair (the reported frequency)
  expect_identical(sel$reason, "consensus")
  expect_identical(sel$harmonic_alt, "sway")         # surfaces the candidate fundamental
  expect_equal(unname(sel$freqs[sel$axis]), 1.4, tolerance = 0.05)   # reports 1.4, NOT a re-pointed 0.7
})

test_that(".selectMotionAxis never HALVES a correct consensus when an artefact sits near half the beat (finding A)", {
  # a genuine wingbeat consensus (surge/heave at 0.44) plus a LOUD artefact at ~half (0.22): re-pointing
  # would silently report 0.22. The detect-don't-resolve rule must keep 0.44 and merely flag the alternative.
  n <- 40000; t <- (0:(n - 1)) / 20
  d <- data.table::data.table(
    surge = sin(2 * pi * 0.44 * t) + stats::rnorm(n, 0, 0.03),
    heave = sin(2 * pi * 0.44 * t + 0.3) + stats::rnorm(n, 0, 0.03),
    sway  = 2.5 * sin(2 * pi * 0.22 * t) + stats::rnorm(n, 0, 0.03))   # loud, exactly half
  sel <- nautilus:::.selectMotionAxis(d, c("surge", "heave", "sway"), 20, 0.09, 3.3)
  expect_true(sel$axis %in% c("surge", "heave"))
  expect_equal(unname(sel$freqs[sel$axis]), 0.44, tolerance = 0.03)   # the TRUE beat, not the halved 0.22
  expect_identical(sel$harmonic_alt, "sway")                          # flagged, not acted on
})

test_that(".selectMotionAxis rejects dead AND near-dead channels via a relative power floor", {
  n <- 8000; t <- (0:(n - 1)) / 20
  # two exactly-flat axes + one real: the real axis must win
  d <- data.table::data.table(surge = rep(0, n), sway = rep(2.5, n),
                              heave = 0.5 * sin(2 * pi * 0.6 * t) + stats::rnorm(n, 0, 0.02))
  expect_identical(nautilus:::.selectMotionAxis(d, c("surge", "sway", "heave"), 20, 0.1, 3)$axis, "heave")
  # all-dead -> no usable axis -> NA (the caller then skips the individual)
  d0 <- data.table::data.table(surge = rep(0, n), sway = rep(1, n), heave = rep(-3, n))
  s0 <- nautilus:::.selectMotionAxis(d0, c("surge", "sway", "heave"), 20, 0.1, 3)
  expect_true(is.na(s0$axis)); expect_identical(s0$reason, "none")
  expect_true(is.na(nautilus:::.dominantInbandFreq(rep(3.7, n), 20, 0.1, 3)))   # flat -> NA, not band edge
  # near-dead (stuck + tiny dither) axes have power > 0 but must not corroborate and beat a real axis
  set.seed(2)
  dn <- data.table::data.table(sway  = 5 + stats::rnorm(n, 0, 1e-9),
                               surge = 2 + stats::rnorm(n, 0, 1e-9),
                               heave = sin(2 * pi * 0.7 * t) + stats::rnorm(n, 0, 0.05))
  expect_identical(nautilus:::.selectMotionAxis(dn, c("sway", "surge", "heave"), 20, 0.2, 1.5)$axis, "heave")
})

test_that("towed deployments emit ONE consolidated tow-pendulum warning, listing all of them", {
  skip_if_not_installed("signal")
  towed <- function(id) {
    d <- .sway(id = id, freq = 0.8, fs = 20, dur = 200)
    data.table::setattr(d, "nautilus", list(deployment = list(deployment_type = "towed")))
    d
  }
  w <- testthat::capture_warnings(
    invisible(capture.output(calculateTailBeats(list(T1 = towed("T1"), T2 = towed("T2")),
                                                motion.col = "sway", min.freq.Hz = 0.2, max.freq.Hz = 3,
                                                return.data = TRUE, verbose = FALSE))))
  towed_w <- w[grepl("tow-pendulum", w)]
  expect_length(towed_w, 1L)                                    # consolidated: one warning, not one per tag
  expect_match(towed_w, "2 towed deployments")                  # names the count
  expect_true(grepl("T1", towed_w) && grepl("T2", towed_w))     # and lists the affected deployments
})

test_that("the axis line splits into the selected axis and an indented peak-frequency sub-line", {
  skip_if_not_installed("signal")
  # multiple candidate axes present -> headline "axis: <axis> (<reason>)" plus a subordinate sub-line
  out <- paste(cli::cli_fmt(suppressWarnings(
    calculateTailBeats(.multiaxis(), motion.col = c("surge", "sway", "heave"), min.freq.Hz = 0.2,
                       max.freq.Hz = 3, return.data = TRUE, verbose = 2))), collapse = "\n")
  expect_match(out, "axis: \\w+ \\(")                    # headline: selected axis + reason in parentheses
  expect_match(out, "peak frequencies: .*Hz")            # the supporting evidence, unit once at the end
  expect_match(out, "\u21b3 peak frequencies:")          # carried on a subordinate sub-line (corner-arrow marker)
})

test_that("two methods: the header names both and a cross-check agreement line is reported", {
  skip_if_not_installed("signal")
  d <- .sway(freq = 0.8, fs = 20, dur = 200)
  fmt <- function(...) paste(cli::cli_fmt(suppressWarnings(
    calculateTailBeats(list(A01 = d), motion.col = "sway", min.freq.Hz = 0.2, max.freq.Hz = 3,
                       return.data = TRUE, verbose = 2, ...))), collapse = "\n")

  out2 <- fmt(method = c("peaks", "wavelet"))
  expect_match(out2, "Methods: peaks \\+ wavelet \\(cross-checked, peaks primary\\)")  # header names both + primary
  expect_match(out2, "agreement: [0-9]+% . wavelet median [0-9.]+ Hz")                 # the cross-check payoff

  out1 <- fmt(method = "peaks")
  expect_match(out1, "Method: peaks")                    # single method -> "Method:", no cross-check
  expect_false(grepl("agreement:", out1, fixed = TRUE))
})

test_that("disagreeing candidate axes trigger an axis-disagreement warning", {
  skip_if_not_installed("signal")
  d <- .multiaxis()   # loco 0.8 surge/heave, non-harmonic artefact 0.6 sway
  w <- testthat::capture_warnings(
    invisible(capture.output(calculateTailBeats(d, motion.col = c("surge", "sway"), min.freq.Hz = 0.2,
                                                max.freq.Hz = 3, return.data = TRUE, verbose = FALSE))))
  expect_true(any(grepl("disagree", w)))
})

test_that("a possible 2f-harmonic pick triggers a harmonic warning naming the alternative axis", {
  skip_if_not_installed("signal")
  d <- .multiaxis(loco = 1.4, artefact = 0.7, artefact.amp = 3)   # surge/heave 1.4, sway 0.7 (=1.4/2)
  w <- testthat::capture_warnings(
    invisible(capture.output(calculateTailBeats(d, motion.col = c("surge", "sway", "heave"),
                                                min.freq.Hz = 0.2, max.freq.Hz = 3,
                                                return.data = TRUE, verbose = FALSE))))
  expect_true(any(grepl("harmonic", w)))
  expect_true(any(grepl("sway", w)))
})
