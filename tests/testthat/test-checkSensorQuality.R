# Tests for checkSensorQuality() (signal-quality anomaly detection + repair, multi-channel).

# report-only by default (apply = FALSE): detects + reports, but does not modify the data
.run <- function(d, sensors, ...) {
  res <- NULL
  invisible(capture.output(suppressWarnings(suppressMessages(
    res <- checkSensorQuality(d, sensors = sensors, return.data = TRUE, verbose = FALSE, ...)))))
  res
}
.repair <- function(d, sensors, ...) .run(d, sensors, apply = TRUE, ...)   # apply = TRUE: write the repairs
.depth <- function(...) list(depth = anomalyControl(rate.threshold = 7, sensor.resolution = 0.5, sensor.accuracy.fixed = 0.1, ...))

# clean 1 Hz sensor series (gentle dive), with optional spike
.mk <- function(id = "A01", n = 60, spike_at = NULL, spike_val = 100, rate_hz = 1) {
  t <- seq(as.POSIXct("2020-01-01 00:00:00", tz = "UTC"), by = 1 / rate_hz, length.out = n)
  depth <- 10 + sin(seq_len(n) / 5)
  if (!is.null(spike_at)) depth[spike_at] <- spike_val
  d <- data.table::data.table(ID = id, datetime = t, depth = depth)
  data.table::setattr(d, "nautilus.version", "test")
  d
}

test_that("the result carries both curated_data and an issues table with the canonical schema", {
  out <- .run(as.data.frame(.mk()), .depth())
  expect_named(out, c("curated_data", "issues"))
  expect_named(out$curated_data, "A01")
  expect_s3_class(out$curated_data$A01, "data.table")
  expect_named(out$issues, c("id", "channel", "severity", "n_corrected", "spikes", "blocks", "pct_affected", "message"))
})

test_that("a list of multiple individuals returns curated_data of the same length", {
  out <- .run(list(A01 = .mk("A01"), B02 = .mk("B02")), .depth())
  expect_named(out$curated_data, c("A01", "B02"))
})

test_that("report-only (apply = FALSE) detects a spike but leaves the data untouched", {
  out <- .run(list(A01 = .mk(spike_at = 30, spike_val = 100)), .depth())
  expect_equal(out$curated_data$A01$depth[30], 100)                 # data unchanged
  expect_true(nrow(out$issues) >= 1L && out$issues$channel[1] == "depth")   # but reported
})

test_that("apply = TRUE removes a clear depth spike (interpolate = FALSE)", {
  out <- .repair(list(A01 = .mk(spike_at = 30, spike_val = 100)), .depth(), interpolate = FALSE)$curated_data$A01
  expect_true(is.na(out$depth[30])); expect_equal(nrow(out), 60)
})

test_that("apply = TRUE interpolates a clear depth spike (interpolate = TRUE)", {
  out <- .repair(list(A01 = .mk(spike_at = 30, spike_val = 100)), .depth(), interpolate = TRUE)$curated_data$A01
  expect_false(is.na(out$depth[30])); expect_lt(out$depth[30], 20)
})

test_that("a genuine spike is detected at a high sampling rate", {
  # 20 Hz: the rate gate is |value_diff| > sensor.resolution/dt (= 0.5/0.05 = 10); a 50 m spike clears it.
  d <- .mk(spike_at = 600, spike_val = 50, n = 1200, rate_hz = 20)
  out <- .repair(list(A01 = d), .depth(), interpolate = FALSE)$curated_data$A01
  expect_true(is.na(out$depth[600]))
})

test_that("the noise gate is a function of sensor.resolution, and that is a real limitation", {
  # This test used to assert simply that high-frequency noise is not mass-flagged, and it passed - but
  # only because `sensor.resolution` defaulted to 0.5, the World-Ocean archive DEPTH quantum, silently
  # applied to a TEMPERATURE channel. The gate is `abs(diff) > sensor.resolution / dt`, so at 16 Hz
  # that wrong constant produced a floor of 8 degrees and suppressed everything.
  #
  # Give the channel the resolution it actually has (0.05 degrees, the value this package's own
  # documentation uses for temperature) and the suppression collapses. That is not a regression: it is
  # the gate telling the truth about itself. A per-sample rate-of-change test cannot separate signal
  # from noise when the noise (sd 0.3) dwarfs the per-sample step the threshold implies
  # (2 units/s / 16 Hz = 0.125 units). Both behaviours are pinned so the limitation stays visible.
  set.seed(1)
  n <- 16 * 60 * 30
  t <- seq(as.POSIXct("2020-01-01 00:00:00", tz = "UTC"), by = 1 / 16, length.out = n)
  temp <- 18 + 0.4 * sin(seq(0, pi, length.out = n)) + stats::rnorm(n, 0, 0.3)
  d <- data.table::data.table(ID = "A01", datetime = t, temp = temp)
  data.table::setattr(d, "nautilus.version", "test")
  frac_na <- function(res) {
    out <- .repair(list(A01 = d), list(temp = anomalyControl(rate.threshold = 2, sensor.resolution = res,
                                                             sensor.accuracy.fixed = 0.1)))$curated_data$A01
    mean(is.na(out$temp))
  }
  # a resolution large enough that resolution/dt clears the noise: nothing is flagged
  expect_lt(frac_na(0.5), 0.001)
  # the channel's TRUE resolution: the gate no longer suppresses, and the series is mass-flagged.
  # Documented, not desired - fixing it needs a noise-floor estimator, not a larger constant.
  expect_gt(frac_na(0.05), 0.5)
})


test_that("clean data yields zero issues and is returned unchanged", {
  out <- .run(list(A01 = .mk()), .depth())
  expect_equal(nrow(out$issues), 0L)
  expect_false(any(is.na(out$curated_data$A01$depth)))
})

test_that("severity: an isolated spike is info; a stall (long constant run) is a warning block", {
  spike <- .run(list(A01 = .mk(spike_at = 30, spike_val = 100)), .depth())$issues
  expect_equal(spike$severity, "info"); expect_gt(spike$spikes, 0L); expect_equal(spike$blocks, 0L)

  stall <- .mk("S", n = 500); stall$depth[100:450] <- 42                # a 351-sample constant non-zero run
  iss <- .run(list(S = stall), list(depth = anomalyControl(rate.threshold = 7, sensor.resolution = 0.5, stall.threshold = 1)))$issues  # 1 min = 60 samples
  expect_equal(iss$severity, "warning"); expect_gt(iss$blocks, 0L)
})

test_that("several channels are screened in one call (the multi-channel unification)", {
  d <- .mk(spike_at = 30, spike_val = 100)                           # depth spike
  d[, temp := 15 + sin(seq_len(60) / 7)]; d$temp[45] <- 60           # + a temp spike
  out <- .repair(list(A01 = d),
                 list(depth = anomalyControl(rate.threshold = 7, sensor.resolution = 0.5, sensor.accuracy.fixed = 0.1),
                      temp  = anomalyControl(rate.threshold = 3, sensor.resolution = 0.05, sensor.accuracy.fixed = 0.1)),
                 interpolate = FALSE)$curated_data$A01
  expect_true(is.na(out$depth[30]))                                  # both channels corrected in one pass
  expect_true(is.na(out$temp[45]))
})

test_that("a channel absent on a given tag is skipped for that tag, not aborted", {
  has  <- .mk("HAS"); has[, temp := 15 + sin(seq_len(60) / 7)]
  none <- .mk("NONE")                                               # depth only, no temp
  out <- .run(list(HAS = has, NONE = none), list(temp = anomalyControl(rate.threshold = 7, sensor.resolution = 0.5, sensor.accuracy.fixed = 0.1)))
  expect_named(out$curated_data, c("HAS", "NONE"))
  expect_true("temp" %in% names(out$curated_data$HAS))
  expect_false("temp" %in% names(out$curated_data$NONE))            # unchanged; no NA column fabricated
})

test_that("plot.file writes a multi-page PDF without touching the active device", {
  pfile <- tempfile(fileext = ".pdf"); on.exit(unlink(pfile), add = TRUE)
  if (!is.null(grDevices::dev.list())) grDevices::graphics.off()
  out <- .run(list(A01 = .mk(spike_at = 30, spike_val = 100)), .depth(), plot.file = pfile)
  expect_named(out, c("curated_data", "issues"))
  expect_true(file.exists(pfile) && file.size(pfile) > 0)
  expect_null(grDevices::dev.list())
})

test_that("output.dir writes every checked dataset (clean included) with the audit entry", {
  dir <- tempfile(); dir.create(dir); on.exit(unlink(dir, recursive = TRUE), add = TRUE)
  dat <- list(A_clean = .mk("A_clean"), B_spike = .mk("B_spike", spike_at = 30, spike_val = 100))
  invisible(capture.output(suppressWarnings(suppressMessages(
    checkSensorQuality(dat, sensors = .depth(), apply = TRUE, output.dir = dir, return.data = FALSE, verbose = FALSE)))))
  expect_true(file.exists(file.path(dir, "A_clean.rds")) && file.exists(file.path(dir, "B_spike.rds")))
  step <- nautilus:::.getMeta(readRDS(file.path(dir, "A_clean.rds")))$processing
  expect_true(any(vapply(step, function(s) identical(s$step, "checkSensorQuality"), logical(1))))
})

test_that("a report-only run (apply = FALSE) needs neither return.data nor output.dir", {
  out <- NULL
  invisible(capture.output(suppressWarnings(suppressMessages(
    out <- checkSensorQuality(list(A01 = .mk(spike_at = 30, spike_val = 100)), sensors = .depth(),
                              apply = FALSE, return.data = FALSE, verbose = FALSE)))))
  expect_null(out$curated_data)
  expect_true(nrow(out$issues) >= 1L)                               # the report still comes back
})

test_that("argument validation aborts clearly", {
  expect_error(.run(list(A01 = .mk()), sensors = list()), "NAMED list", ignore.case = TRUE)
  # with apply = TRUE, discarding the repaired data (return.data = FALSE) requires an output.dir to write to
  expect_error(suppressMessages(checkSensorQuality(list(A01 = .mk()), sensors = .depth(),
                 apply = TRUE, return.data = FALSE, verbose = FALSE)), "output.dir", ignore.case = TRUE)
})

test_that("anomalyControl validates the accuracy specs (at most one; neither is allowed)", {
  expect_error(anomalyControl(rate.threshold = 7, sensor.resolution = 0.5, sensor.accuracy.fixed = 0.1, sensor.accuracy.percent = 1), "only one")
  expect_s3_class(anomalyControl(rate.threshold = 7, sensor.resolution = 0.5), "nautilus_anomaly")   # accuracy is optional
  # ...but the resolution is NOT optional: 0.5 was a depth quantum silently applied to every channel
  expect_error(anomalyControl(rate.threshold = 7), "sensor.resolution")
  expect_error(anomalyControl(rate.threshold = -1, sensor.resolution = 0.5), "rate.threshold")
})

test_that("saving (output.dir) requires apply = TRUE (a report-only save would only copy the input)", {
  d <- .mk(spike_at = 30, spike_val = 100)
  # report-only + save would write an uncurated copy that only LOOKS curated -> refuse
  expect_error(
    checkSensorQuality(d, sensors = .depth(), apply = FALSE,
                       output.dir = tempdir(), verbose = FALSE),
    "apply = TRUE", ignore.case = TRUE)
  # the valid combination (apply + save) still writes a curated file
  dir <- file.path(tempdir(), "csq_guard"); dir.create(dir, showWarnings = FALSE)
  on.exit(unlink(dir, recursive = TRUE), add = TRUE)
  invisible(capture.output(suppressWarnings(suppressMessages(
    checkSensorQuality(d, sensors = .depth(), apply = TRUE,
                       output.dir = dir, verbose = FALSE)))))
  expect_gte(length(list.files(dir, pattern = "\\.rds$")), 1L)
})


test_that("a report-only run that FOUND anomalies says the data is unchanged", {
  # `curated_data` comes back under a name that promises curation while holding the untouched input.
  # This was hit for real during validation: the function was run on a record with 84,419 impossible
  # depth samples, they were all still there afterwards, and the detector was briefly believed broken.
  # It was not - `apply` was FALSE, which is the default.
  d <- data.frame(ID = "A", datetime = as.POSIXct("2024-01-01", tz = "UTC") + seq_len(2000) - 1,
                  depth = c(rep(10, 1000), rep(3000, 1000)), stringsAsFactors = FALSE)
  ctl <- list(depth = anomalyControl(rate.threshold = 7, sensor.resolution = 0.5))

  txt <- paste(cli::cli_fmt(suppressWarnings(
    checkSensorQuality(list(A = d), sensors = ctl, plot = FALSE, verbose = 2))), collapse = " ")
  expect_match(txt, "REPORT ONLY")
  expect_match(txt, "apply = TRUE", fixed = TRUE)

  # and the claim it makes is true: anomalies found, data byte-identical to the input
  out <- suppressWarnings(suppressMessages(
    checkSensorQuality(list(A = d), sensors = ctl, plot = FALSE, verbose = FALSE)))
  expect_gt(nrow(out$issues), 0L)
  expect_equal(out$curated_data[[1]]$depth, d$depth)

  # ...whereas apply = TRUE really does repair, and then says nothing about report-only
  txt2 <- paste(cli::cli_fmt(suppressWarnings(
    checkSensorQuality(list(A = d), sensors = ctl, apply = TRUE, plot = FALSE, verbose = 2))),
    collapse = " ")
  expect_false(grepl("REPORT ONLY", txt2))
  rep <- suppressWarnings(suppressMessages(
    checkSensorQuality(list(A = d), sensors = ctl, apply = TRUE, plot = FALSE, verbose = FALSE)))
  expect_true(any(is.na(rep$curated_data[[1]]$depth)))
})

test_that("a clean report-only run does not emit the report-only warning", {
  # the warning must fire on FOUND-but-unrepaired, not on every non-apply call, or it becomes noise
  d <- data.frame(ID = "A", datetime = as.POSIXct("2024-01-01", tz = "UTC") + seq_len(600) - 1,
                  depth = 10 + sin(seq_len(600) / 50), stringsAsFactors = FALSE)
  txt <- paste(cli::cli_fmt(suppressWarnings(checkSensorQuality(
    list(A = d), sensors = list(depth = anomalyControl(rate.threshold = 7, sensor.resolution = 0.5)),
    plot = FALSE, verbose = 2))), collapse = " ")
  expect_false(grepl("REPORT ONLY", txt))
})
