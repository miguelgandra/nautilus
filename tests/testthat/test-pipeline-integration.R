# Integration test: the nautilus_tag class and its metadata audit trail must propagate
# through the processing pipeline (Phase 3 of the S3 refactor).

test_that(".collectOutput returns paths invisibly and data visibly (the console-noise contract)", {
  # The unified return contract shared by every write-through pipeline stage. return.data = FALSE writes
  # files for their side effect, so a top-level call must NOT auto-print the path vector (the noise this
  # was built to remove) - yet the value must stay available to capture or chain. return.data = TRUE is the
  # object the caller explicitly asked for, and prints/assigns as normal. No test covered this before, which
  # is how a visible path-dump shipped.
  paths <- withVisible(nautilus:::.collectOutput(list(A = 1), list("A.rds"), FALSE, "A"))
  expect_false(paths$visible)
  expect_identical(paths$value, "A.rds")           # value intact, only the auto-print is suppressed

  data <- withVisible(nautilus:::.collectOutput(list(A = 1L), list("A.rds"), TRUE, "A"))
  expect_true(data$visible)
  expect_identical(data$value, list(A = 1L))
})

test_that("class and processing audit trail accumulate across the pipeline", {
  t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
  n <- 300
  d <- data.table::data.table(
    ID = "A01", datetime = t0 + 0:(n - 1),
    ax = rnorm(n, 0, 0.02), ay = rnorm(n, 0, 0.02), az = 1 + rnorm(n, 0, 0.02),
    depth = 10 + 5 * sin(seq_len(n) / n * pi), temp = 20)
  # build an importTagData-style nautilus_tag
  m <- nautilus:::.newNautilusMeta(); m$id <- "A01"; m$sensors$timezone <- "UTC"
  m <- nautilus:::.appendProcessing(m, "importTagData")
  x <- nautilus:::new_nautilus_tag(d, m)

  out <- NULL
  invisible(capture.output(suppressWarnings(suppressMessages({
    r <- regularizeTimeSeries(list(A01 = x), gap.threshold = 2, return.data = TRUE, verbose = FALSE)
    # tilt_compass + no magnetometer -> heading NA, no declination lookup needed
    p <- processTagData(r, orientation.algorithm = "tilt_compass",
                        downsample.to = NULL, verbose = FALSE)
    out <- p$A01
  }))))

  expect_true(nautilus:::is_nautilus_tag(out))
  steps <- vapply(nautilus:::.getMeta(out)$processing, function(p) p$step, character(1))
  # processTagData also appends a lean depth_drift record; with no dry sensor / fixes it abstains
  expect_equal(steps, c("importTagData", "regularizeTimeSeries", "processTagData", "depth_drift"))
  expect_equal(nautilus:::.getMeta(out)$processing[[4]]$status, "abstained")
  # processTagData recorded the sampling rate into the consolidated metadata
  expect_equal(nautilus:::.getMeta(out)$sensors$sampling_hz_original, 1)
})


# ---- a deployment with channels removed by QC must not abort the batch ------------------------------

test_that("pipeline stages skip a channel-less deployment and process the rest", {
  # checkSensorIntegrity() legitimately drops channels (PIN_07 lost ax/ay/az to a railed Y axis), and
  # importTagData() has always been able to do the same via the exclude_sensors metadata column. Every
  # batch stage used to abort on the FIRST such deployment, discarding the work already done for the
  # others. A missing channel is a property of one tag, not of the cohort.
  set.seed(1); n <- 3000
  mk <- function(id, drop_accel = FALSE) {
    t0 <- as.POSIXct("2020-08-22 12:00:00", tz = "UTC")
    d <- data.table::data.table(
      ID = id, datetime = t0 + seq_len(n) / 10,
      ax = stats::rnorm(n, 0, .2), ay = stats::rnorm(n, 0, .2), az = 1 + stats::rnorm(n, 0, .2),
      gx = stats::rnorm(n, 0, .05), gy = stats::rnorm(n, 0, .05), gz = stats::rnorm(n, 0, .05),
      mx = 25 + stats::rnorm(n), my = stats::rnorm(n), mz = 40 + stats::rnorm(n),
      depth = pmax(0, 20 + 15 * sin(seq_len(n) / 200)), temp = 18 + stats::rnorm(n, 0, .05))
    m <- nautilus:::.newNautilusMeta(); m$id <- id
    m$deployment$lon <- -25.19; m$deployment$lat <- 37.05; m$deployment$datetime <- t0
    if (drop_accel) { d[, c("ax", "ay", "az") := NULL]; m$sensors$excluded <- c("ax", "ay", "az") }
    nautilus:::new_nautilus_tag(d, m)
  }
  dir <- withr::local_tempdir()
  for (nm in c("A_OK", "B_NOACC", "C_OK"))
    saveRDS(mk(nm, drop_accel = nm == "B_NOACC"), file.path(dir, paste0(nm, ".rds")))
  files <- sort(list.files(dir, full.names = TRUE))

  # each stage completes rather than aborting, and the healthy deployments survive
  expect_no_error(suppressWarnings(suppressMessages(
    regularizeTimeSeries(files, verbose = 0, return.data = TRUE))))
  expect_no_error(suppressWarnings(suppressMessages(
    filterDeploymentData(files, verbose = 0, return.data = TRUE))))

  # the accelerometer-dependent stages skip the curated tag and keep the other two
  map <- suppressWarnings(suppressMessages(checkTagMapping(files, verbose = 0)))
  expect_setequal(names(map), c("A_OK", "C_OK"))
  proc <- suppressWarnings(suppressMessages(processTagData(files, verbose = 0, return.data = TRUE)))
  # EXACT length, and no NULL holes: ">= 2" would pass on a 3-element list carrying a NULL for the
  # skipped tag, which is precisely the corrupt shape this work exists to prevent
  expect_length(proc, 2L)
  expect_false(any(vapply(proc, is.null, logical(1))))
  expect_setequal(names(proc), c("A_OK", "C_OK"))

  # calculateTailBeats keeps the deployment but writes the full NA schema, so a skipped tag is not
  # silently missing columns downstream - a deliberately different contract, locked here so it stays so
  tags <- lapply(files, readRDS); names(tags) <- tools::file_path_sans_ext(basename(files))
  tb <- suppressWarnings(suppressMessages(calculateTailBeats(tags, verbose = 0, return.data = TRUE)))
  expect_length(tb, 3L)
  expect_true("tbf_hz_peaks" %in% names(tb$B_NOACC))
  expect_true(all(is.na(tb$B_NOACC$tbf_hz_peaks)))
})
