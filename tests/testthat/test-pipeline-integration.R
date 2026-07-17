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
