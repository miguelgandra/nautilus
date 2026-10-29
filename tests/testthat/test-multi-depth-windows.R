.mwTag <- function(id = "MW", fs = 20) {
  t <- (seq_len(120 * fs) - 1) / fs
  v <- (40 * pi / 30) * cos(2 * pi * t / 30)
  pitch <- 10 * pi / 180 + 0.08 * v
  x <- buildTagData(data.frame(
    ID = id, datetime = as.POSIXct("2025-01-01", tz = "UTC") + t,
    depth = round(30 + 20 * sin(2 * pi * t / 30), 2),
    ax = -sin(pitch), ay = 0.01 * sin(t / 5), az = cos(pitch),
    gx = 0, gy = 0, gz = 0, mx = 20, my = 5, mz = 42), verbose = FALSE)
  m <- nautilus:::.getMeta(x)
  m$axis_mapping$applied <- TRUE
  nautilus:::.restoreMeta(x, m)
}

.mwRun <- function(x, windows, target = NULL, post = 1, ...) suppressWarnings(suppressMessages(
  processTagData(x, smoothing = smoothingControl(depth = windows, vertical = post),
                 depth.drift = depthDriftControl(method = "none"), downsample.to = target,
                 verbose = FALSE, ...)))

.mwRecord <- function(x) tail(Filter(function(p) identical(p$step, "processTagData"),
                                    getTagMetadata(x)$processing), 1)[[1]]

test_that("depth window vectors are numeric, finite, unique and positive", {
  expect_equal(smoothingControl(depth = c(5, 2.5, 10))$depth, c(5, 2.5, 10))
  expect_identical(smoothingControl(depth = c(5L, 10L))$depth, c(5L, 10L))
  expect_equal(smoothingControl(depth = c(primary = 5, other = 10))$depth, c(5, 10))
  expect_null(smoothingControl(depth = NULL)$depth)
  expect_equal(smoothingControl(depth = 0)$depth, 0)
  for (bad in list(numeric(), c(5, NA), c(5, NaN), c(5, Inf), -1, c(0, 5),
                   c(5, 0), c(5, 5), "5", TRUE, list(5, 10), factor(c(5, 10)),
                   matrix(c(5, 10)), c(5 + 0i, 10 + 0i))) {
    expect_error(smoothingControl(depth = bad), "smoothing\\$depth")
  }
  for (field in c("static", "orientation", "dba", "vertical")) {
    expect_error(do.call(smoothingControl, stats::setNames(list(c(1, 2)), field)),
                 paste0("smoothing\\$", field))
  }
})

test_that("window suffixes use locale-independent decimals and syntactic R names", {
  withr::local_options(OutDec = ",")
  cols <- nautilus:::.depthRateColumns(c(5, 2.5, 10, 0.0001))
  expect_identical(cols, c("vertical_velocity", "vertical_velocity_2.5s",
                           "vertical_velocity_10s", "vertical_velocity_0.0001s"))
  expect_identical(make.names(cols), cols)
  d <- data.frame(vertical_velocity_2.5s = 7)
  expect_equal(d$vertical_velocity_2.5s, 7)
  dt <- data.table::as.data.table(d)
  expect_equal(dt[, vertical_velocity_2.5s], 7)
  expect_error(smoothingControl(depth = c(5, 2.5, 2.500000000000001)), "column names")
})

test_that("multi-window output matches scalar runs at native and downsampled cadence", {
  input <- .mwTag()
  input$j <- 2L                  # user columns must not shadow the loop index inside data.table
  input$depth[400:402] <- NA_real_
  before <- data.table::copy(input)
  cols <- c("vertical_velocity", "vertical_velocity_2.5s", "vertical_velocity_10s")
  for (target in list(NULL, 2)) {
    primary <- .mwRun(input, 5, target)[[1]]
    multi <- .mwRun(input, c(5, 2.5, 10), target)[[1]]
    expect_identical(setdiff(names(multi), names(primary)), cols[-1])
    expect_equal(lapply(multi, identity)[names(primary)], lapply(primary, identity))
    expect_equal(multi$vertical_velocity_2.5s, .mwRun(input, 2.5, target)[[1]]$vertical_velocity)
    expect_equal(multi$vertical_velocity_10s, .mwRun(input, 10, target)[[1]]$vertical_velocity)
    record <- .mwRecord(multi)
    expect_equal(record$depth_smoothing, 5)
    expect_equal(record$vertical_smoothing, 1)
    expect_equal(record$vertical_rate_windows, stats::setNames(c(5, 2.5, 10), cols))
    expect_equal(record$vertical_rate_samples, stats::setNames(c(101, 51, 201), cols))
    expect_equal(record$vertical_rate_span_s, stats::setNames(c(5, 2.5, 10), cols))
    expect_equal(record$pitch_offset_deg, .mwRecord(primary)$pitch_offset_deg)
    expect_equal(record$roll_offset_deg, .mwRecord(primary)$roll_offset_deg)
  }
  expect_equal(as.data.frame(input), as.data.frame(before))
  expect_equal(getTagMetadata(input), getTagMetadata(before))
  primary10 <- .mwRun(input, 10)[[1]]
  multi10 <- .mwRun(input, c(10, 5, 2.5))[[1]]
  expect_equal(lapply(multi10, identity)[names(primary10)], lapply(primary10, identity))
  expect_equal(multi10$vertical_velocity_5s, .mwRun(input, 5)[[1]]$vertical_velocity)
})

test_that("scalar minimum stencils and a shared disabled post-mean retain their behaviour", {
  x <- .mwTag()
  for (post in list(NULL, 0)) {
    m <- .mwRun(x, c(5, 2.5), post = post)[[1]]
    expect_equal(m$vertical_velocity, .mwRun(x, 5, post = post)[[1]]$vertical_velocity)
    expect_equal(m$vertical_velocity_2.5s, .mwRun(x, 2.5, post = post)[[1]]$vertical_velocity)
  }
  minimal <- .mwRun(x, NULL)[[1]]
  expect_equal(minimal$vertical_velocity, .mwRun(x, 0)[[1]]$vertical_velocity)
  expect_length(grep("^vertical_velocity_", names(minimal)), 0)
})

test_that("cadence-limited windows warn once for the batch and retain requested columns", {
  warnings <- character()
  result <- withCallingHandlers(processTagData(list(.mwTag("A", 1), .mwTag("B", 1)),
    smoothing = smoothingControl(depth = c(1, 1.5, 5)),
    depth.drift = depthDriftControl(method = "none"), downsample.to = NULL, verbose = FALSE),
    warning = function(w) { warnings <<- c(warnings, conditionMessage(w)); invokeRestart("muffleWarning") })
  notes <- warnings[grepl("Vertical-velocity windows", warnings, fixed = TRUE)]
  expect_length(notes, 1)
  expect_match(notes, "A at 1 Hz")
  expect_match(notes, "B at 1 Hz")
  for (x in result) {
    expect_identical(x$vertical_velocity, x$vertical_velocity_1.5s)
    expect_equal(unname(.mwRecord(x)$vertical_rate_samples), c(3, 3, 5))
    expect_equal(unname(.mwRecord(x)$vertical_rate_span_s), c(2, 2, 4))
  }
})

test_that("multi-window columns and provenance survive file processing and saving", {
  root <- tempfile(); dir.create(root)
  infile <- file.path(root, "input.rds"); saveRDS(.mwTag(), infile)
  paths <- .mwRun(infile, c(5, 2.5, 10), target = 2, output.dir = root,
                   output.suffix = "_multi", return.data = FALSE)
  saved <- readRDS(paths[[1]])
  memory <- .mwRun(readRDS(infile), c(5, 2.5, 10), target = 2)[[1]]
  expect_equal(lapply(saved, identity), lapply(memory, identity))
  expect_equal(.mwRecord(saved)$vertical_rate_windows, .mwRecord(memory)$vertical_rate_windows)
  expect_s3_class(processingHistory(saved), "data.frame")
})

test_that("reprocessing replaces previously generated secondary velocity columns", {
  old <- .mwRun(.mwTag(), c(5, 10))[[1]]
  for (target in list(NULL, 2)) {
    new <- .mwRun(old, c(5, 2.5), target = target)[[1]]
    expect_true("vertical_velocity_2.5s" %in% names(new))
    expect_false("vertical_velocity_10s" %in% names(new))
  }
  expect_true("vertical_velocity_10s" %in% names(old))
  old_fractional <- .mwRun(.mwTag(), c(5, 2.5, 10))[[1]]
  scalar <- .mwRun(old_fractional, 5)[[1]]
  expect_length(grep("^vertical_velocity_", names(scalar)), 0)
  expect_true("vertical_velocity_2.5s" %in% names(old_fractional))
})
