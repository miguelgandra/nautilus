# Compare values and all observable attributes, including nested metadata. A data.table's internal
# self-reference is an implementation pointer rather than dataset state, so it is excluded.
.ownership_state <- function(x) {
  a <- attributes(x)
  a$.internal.selfref <- NULL
  data.table::copy(list(columns = lapply(x, identity), attributes = a[sort(names(a))]))
}

.ownership_tag <- function(n = 100L, hz = 2, motion = FALSE) {
  t <- as.POSIXct("2025-01-01", tz = "UTC") + (seq_len(n) - 1) / hz
  d <- data.frame(ID = "OWN", datetime = t, depth = seq(0.123456, 10.123456, length.out = n))
  if (motion) {
    d$ax <- 0.1 * sin(seq_len(n)); d$ay <- 0.2 * cos(seq_len(n)); d$az <- 1
  }
  x <- buildTagData(d, verbose = FALSE)
  m <- nautilus:::.getMeta(x)
  m$ancillary$positions <- list(data = data.table::data.table(
    datetime = t[1], type = "FastGPS", lon = -25, lat = 37, quality = "7"))
  nautilus:::.restoreMeta(x, m)
}

.ownership_quiet <- function(expr) suppressWarnings(suppressMessages(force(expr)))

.ownership_process <- function(x, ...) .ownership_quiet(processTagData(
  x, depth.drift = depthDriftControl(method = "none"), verbose = FALSE, ...))

test_that("writable resolution isolates repeated accesses, aliases and nested metadata", {
  x <- .ownership_tag()
  before <- .ownership_state(x)
  r <- nautilus:::.resolveInput(list(first = x, second = x))
  # Readers borrow canonical objects without duplicating the sensor payload.
  expect_identical(data.table::address(r$get(1)), data.table::address(x))
  a <- r$get(1, writable = TRUE)
  b <- r$get(2, writable = TRUE)
  c <- r$get(1, writable = TRUE)
  a[, depth := -1]
  attr(a, "nautilus")$ancillary$positions$data[, lon := 100]
  expect_identical(.ownership_state(x), before)
  expect_identical(.ownership_state(b), before)
  expect_identical(.ownership_state(c), before)

  # Freshly read/private tables can transfer ownership without a redundant full copy.
  private <- data.table::copy(x)
  expect_identical(data.table::address(nautilus:::.workingTable(private, owned = TRUE)),
                   data.table::address(private))
})

test_that("resolver migration and single-table splitting preserve caller state", {
  for (form in c("frame", "table", "tag")) {
    x <- .ownership_tag()
    if (form != "tag") {
      data.table::setattr(x, "nautilus", NULL)
      data.table::setattr(x, "class", c("data.table", "data.frame"))
    }
    if (form == "frame") x <- as.data.frame(x)
    before <- .ownership_state(x)
    r <- nautilus:::.resolveInput(list(x))
    expect_false(is.null(attr(r$get(1), "nautilus")), info = form)
    out <- r$get(1, writable = TRUE)
    out[, depth := -1]
    expect_identical(.ownership_state(x), before, info = form)
  }
  x <- .ownership_tag()
  before <- .ownership_state(x)
  out <- nautilus:::.resolveInput(x)$get(1, writable = TRUE)
  attr(out, "nautilus")$ancillary$positions$data[, lon := 100]
  expect_identical(.ownership_state(x), before)
})

test_that("processing preserves raw timestamps, precision, attributes and input aliases", {
  for (form in c("tag_list", "table_list", "frame_list", "single")) {
    x <- .ownership_tag()[100:1]
    if (form == "table_list") {
      data.table::setattr(x, "nautilus", NULL)
      data.table::setattr(x, "class", c("data.table", "data.frame"))
    }
    if (form == "frame_list") x <- as.data.frame(x)
    before <- .ownership_state(x)
    out <- .ownership_process(if (form == "single") x else list(x), downsample.to = 1)[[1]]
    expect_equal(nrow(out), 50L, info = form)
    expect_false(is.unsorted(out$datetime), info = form)
    expect_identical(.ownership_state(x), before, info = form)
  }
  x <- .ownership_tag()
  before <- .ownership_state(x)
  out <- .ownership_process(list(x), downsample.to = NULL)[[1]]
  expect_equal(out$depth[1], 0.12)
  out[, depth := -1]
  attr(out, "nautilus")$ancillary$positions$data[, lon := 100]
  expect_identical(.ownership_state(x), before)
})

test_that("relative axis mappings are applied independently to aliased slots", {
  x <- .ownership_tag(motion = TRUE)
  before <- .ownership_state(x)
  mapping <- data.frame(from = c("ay", "ax"), to = c("-ax", "ay"))
  out <- .ownership_quiet(applyAxisMapping(list(first = x, second = x), mapping,
                                         relative = TRUE, verbose = FALSE))
  expect_length(out, 2L)
  for (i in seq_along(out)) {
    expect_equal(out[[i]]$ax, -x$ay)
    expect_equal(out[[i]]$ay, x$ax)
    expect_equal(sum(processingHistory(out[[i]])$step == "applyAxisMapping"), 1L)
  }
  out[[1]][, ax := 999]
  attr(out[[1]], "nautilus")$ancillary$positions$data[, lon := 100]
  expect_equal(out[[2]]$ax, -x$ay)
  expect_equal(attr(out[[2]], "nautilus")$ancillary$positions$data$lon, -25)
  expect_identical(.ownership_state(x), before)
})

test_that("report-only QC leaves histories unchanged and returns independent datasets", {
  sensors <- list(depth = anomalyControl(rate.threshold = 7, sensor.resolution = 0.5))
  for (check in c("integrity", "quality")) for (return.data in c(FALSE, TRUE)) {
    x <- .ownership_tag()
    before <- .ownership_state(x)
    out <- if (check == "integrity") .ownership_quiet(checkSensorIntegrity(
      list(first = x, second = x), checks = "dead", return.data = return.data, verbose = FALSE))
    else .ownership_quiet(checkSensorQuality(list(first = x, second = x), sensors = sensors,
                                             return.data = return.data, verbose = FALSE))
    expect_identical(.ownership_state(x), before, info = check)
    expect_s3_class(out$issues, "data.frame")
    if (return.data) {
      expect_equal(processingHistory(out$curated_data[[1]]), processingHistory(x), info = check)
      expect_equal(processingHistory(out$curated_data[[2]]), processingHistory(x), info = check)
      out$curated_data[[1]][, depth := -1]
      attr(out$curated_data[[1]], "nautilus")$ancillary$positions$data[, lon := 100]
      expect_identical(.ownership_state(out$curated_data[[2]]), before, info = check)
      expect_identical(.ownership_state(x), before, info = check)
    } else expect_null(out$curated_data)
  }
})

test_that("QC apply mode isolates clean, flagged and unsorted caller datasets", {
  for (flagged in c(FALSE, TRUE)) {
    x <- .ownership_tag(motion = TRUE)
    if (flagged) x[, temp := 10]  # a dead channel eligible for exclusion
    before <- .ownership_state(x)
    out <- .ownership_quiet(checkSensorIntegrity(list(x), checks = "dead", apply = TRUE,
                                                verbose = FALSE))$curated_data[[1]]
    expect_identical(.ownership_state(x), before)
    if (flagged) expect_false("temp" %in% names(out))
    expect_equal(sum(processingHistory(out)$step == "checkSensorIntegrity"), 1L)
  }
  x <- .ownership_tag(n = 100L, hz = 1)
  x[50, depth := 1000]
  x <- x[100:1]
  before <- .ownership_state(x)
  sensors <- list(depth = anomalyControl(rate.threshold = 7, sensor.resolution = 0.5))
  out <- .ownership_quiet(checkSensorQuality(list(x), sensors = sensors, apply = TRUE,
                                            verbose = FALSE))$curated_data[[1]]
  expect_lt(max(out$depth, na.rm = TRUE), 1000)
  expect_false(is.unsorted(out$datetime))
  expect_equal(sum(processingHistory(out)$step == "checkSensorQuality"), 1L)
  expect_identical(.ownership_state(x), before)
})

test_that("regularization and deployment filtering own tables before sorting and clamping", {
  x <- .ownership_tag(hz = 1)
  x[, datetime := datetime + rep(c(0, 0.1, -0.1, 0.2), length.out = .N)]
  x <- x[100:1]
  before <- .ownership_state(x)
  out <- .ownership_quiet(regularizeTimeSeries(list(OWN = x), plot = FALSE, verbose = FALSE))
  expect_length(out, 1L)
  expect_identical(.ownership_state(x), before)
  expect_false("obs_datetime" %in% names(x))

  x <- as.data.frame(.ownership_tag(hz = 1))
  x$depth[1] <- -3
  before <- .ownership_state(x)
  window <- data.frame(ID = "OWN", start = min(x$datetime), end = max(x$datetime))
  out <- .ownership_quiet(filterDeploymentData(list(OWN = x), custom.deployment.times = window,
    min.deployment.hours = 0, plot = FALSE, verbose = FALSE))[[1]]
  expect_equal(min(out$depth), 0)
  expect_identical(.ownership_state(x), before)
})

test_that("mapping diagnostics and feature extraction preserve their input tables", {
  x <- .ownership_tag(n = 120L, hz = 1, motion = TRUE)[120:1]
  before <- .ownership_state(x)
  out <- .ownership_quiet(checkTagMapping(list(OWN = x), verbose = FALSE))
  expect_length(out, 1L)
  expect_identical(.ownership_state(x), before)

  x <- as.data.frame(.ownership_tag(n = 120L, hz = 1))
  before <- .ownership_state(x)
  out <- .ownership_quiet(extractFeatures(list(OWN = x), variables = "depth", metrics = "mean",
                                         window.size = 10, n.cores = 1, verbose = FALSE))
  expect_gt(nrow(out[[1]]), 0L)
  expect_identical(.ownership_state(x), before)
})

test_that("tail-beat no-motion output and location-filter no-op output are independent", {
  x <- .ownership_tag(hz = 20)
  before <- .ownership_state(x)
  out <- .ownership_quiet(calculateTailBeats(list(OWN = x), verbose = FALSE))[[1]]
  expect_true("tbf_swimming" %in% names(out))
  expect_identical(.ownership_state(x), before)

  # A no-fixes early return must not hand the caller's mutable metadata back as an alias.
  x <- .ownership_tag()
  attr(x, "nautilus")$ancillary <- list()
  before <- .ownership_state(x)
  out <- .ownership_quiet(filterLocations(list(OWN = x), verbose = FALSE))[[1]]
  out[, depth := -1]
  expect_identical(.ownership_state(x), before)
})

test_that("video subsets detach nested metadata as well as sensor rows", {
  x <- .ownership_tag()
  before <- .ownership_state(x)
  videos <- data.frame(ID = "OWN", start = x$datetime[2], end = x$datetime[10])
  out <- filterVideoPeriod(list(OWN = x), videos, verbose = FALSE)[[1]]
  expect_equal(nrow(out), 9L)
  out[, depth := -1]
  attr(out, "nautilus")$ancillary$positions$data[, lon := 100]
  expect_identical(.ownership_state(x), before)
})

test_that("calibration estimates modify only returned metadata", {
  x <- .ownership_tag(n = 600L, hz = 1, motion = TRUE)
  # A deterministic sphere with a hard-iron offset gives the estimator an identifiable calibration.
  z <- seq(-0.99, 0.99, length.out = nrow(x))
  angle <- seq_len(nrow(x)) * pi * (3 - sqrt(5))
  x[, `:=`(mx = 50 * sqrt(1 - z^2) * cos(angle) + 5,
            my = 50 * sqrt(1 - z^2) * sin(angle) - 3, mz = 50 * z + 2)]
  before <- .ownership_state(x)
  out <- .ownership_quiet(calibrateMagnetometer(list(OWN = x),
    control = magCalibrationControl(igrf.normalize = FALSE), verbose = FALSE))[[1]]
  expect_false(is.null(attr(out, "nautilus")$mag_calibration$proposed))
  expect_identical(.ownership_state(x), before)
  attr(out, "nautilus")$ancillary$positions$data[, lon := 100]
  expect_identical(.ownership_state(x), before)
})

test_that("an error after reference edits cannot partially modify the caller", {
  x <- .ownership_tag()[100:1]
  before <- .ownership_state(x)
  testthat::local_mocked_bindings(.appendProcessing = function(...) stop("injected ownership failure"),
                                 .package = "nautilus")
  expect_error(.ownership_process(list(x), downsample.to = 1), "injected ownership failure")
  expect_identical(.ownership_state(x), before)
})

test_that("file input and writable results never change the source file", {
  x <- .ownership_tag()
  path <- tempfile(fileext = ".rds")
  on.exit(unlink(path), add = TRUE)
  saveRDS(x, path)
  before <- tools::md5sum(path)
  out <- .ownership_process(path, downsample.to = 1)[[1]]
  out[, depth := -1]
  attr(out, "nautilus")$ancillary$positions$data[, lon := 100]
  expect_identical(tools::md5sum(path), before)
  expect_identical(.ownership_state(readRDS(path)), .ownership_state(x))
})
