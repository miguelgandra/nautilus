# Synthetic regressions for circular, missing-channel and multi-scale inference features.
.efe_data <- function(n = 40, hz = 1) {
  d <- data.table::data.table(ID = "EDGE", datetime = as.POSIXct("2024-01-01", tz = "UTC") +
                           (seq_len(n) - 1) / hz, depth = seq_len(n),
                         heading = (179 + seq_len(n) %% 3) %% 360)
  data.table::setattr(d, "processed.sampling.frequency", hz)
  d
}
.efe_run <- function(d, ...) suppressWarnings(suppressMessages(extractFeatures(
  list(EDGE = d), verbose = FALSE, missing.features = "keep", ...)$EDGE))

test_that("circular spread and turning metrics are invariant to rotations and representations", {
  x <- c(358, 359, 0, 1, 2, 1, 0, 359)
  for (metric in c("iqr", "sd", "range", "mrl", "rate")) {
    ref <- nautilus:::.circularMetric(x, metric, 2)
    for (rotation in c(1, 45, 90, 180, 359))
      expect_equal(nautilus:::.circularMetric((x + rotation) %% 360, metric, 2), ref,
                   tolerance = 1e-8)
    expect_equal(nautilus:::.circularMetric(x - 360, metric, 2), ref, tolerance = 1e-8)
  }
  expect_equal(nautilus:::.circularMetric(c(358, 359, 0, 1), "iqr", 1), 1.5)
  # Tied largest gaps must not reintroduce a dependence on absolute north.
  x <- c(0, 10, 120, 240)
  for (rotation in seq(0, 345, 15))
    expect_equal(nautilus:::.circularMetric((x + rotation) %% 360, "iqr", 1),
                 nautilus:::.circularMetric(x, "iqr", 1))
})

test_that("circling and roll-dependent features handle branch crossings", {
  d <- .efe_data(21); d$heading <- rep(c(359, 1), length.out = 21)
  a <- .efe_run(d, variables = "heading", metrics = "circling_behavior",
                enhanced.features = TRUE, window.size = 11)
  expect_equal(a$heading_circling[11], 20)
  d$heading <- (d$heading + 90) %% 360
  b <- .efe_run(d, variables = "heading", metrics = "circling_behavior",
                enhanced.features = TRUE, window.size = 11)
  expect_equal(a$heading_circling, b$heading_circling)
  d$pitch <- 0; d$roll <- rep(c(179, -179), length.out = 21)
  d$heading <- 0; d$activity <- 0; d$posture <- 0
  p <- data.frame(variable = c("activity", "posture", "roll"),
                  metric = c("activity_index", "posture_stability", "movement_jerk"))
  a <- .efe_run(d, parameter.grid = p, enhanced.features = TRUE, window.size = 5)
  expect_equal(a$activity_index[11], 2)
  expect_equal(a$roll_movement_jerk[11], 2)
  expect_gt(a$posture_stability[11], 0.3)
})

test_that("heading autocorrelation recovers after gaps without compressing time", {
  h <- (180 + cumsum(sin(seq_len(100) / 4))) %% 360
  h[30] <- NA_real_
  got <- nautilus:::.heading_autocorr_avg(h, window = 20)
  clean <- nautilus:::.heading_autocorr_avg(h[51:100], window = 20)
  expect_true(all(is.finite(got[60:85])))
  expect_equal(got[60:85], clean[10:35], tolerance = 1e-8)
  # Missing samples remain in their lag positions inside a window.
  local <- h[21:40]
  unwrapped <- rep(NA_real_, length(local))
  for (run in split(which(is.finite(local)), cumsum(c(TRUE, diff(which(is.finite(local))) > 1))))
    unwrapped[run] <- local[run[1]] + c(0, cumsum((diff(local[run]) + 180) %% 360 - 180))
  expected <- mean(stats::acf(unwrapped, plot = FALSE, lag.max = 5,
                              na.action = stats::na.pass)$acf[2:6])
  expect_equal(got[30], expected, tolerance = 1e-8)
})

test_that("all-missing local windows return NA for every summary", {
  d <- .efe_data(); d$depth[11:30] <- NA_real_
  for (aggregated in c(FALSE, TRUE)) {
    out <- .efe_run(d, variables = "depth", metrics = c("mean", "sum", "energy", "min", "max", "range"),
                    window.size = 5, aggregate = aggregated)
    rows <- which(out$datetime >= d$datetime[15] & out$datetime <= d$datetime[25])
    expect_true(all(is.na(as.matrix(out[rows, -(1:2)]))))
    expect_false(any(is.infinite(as.matrix(out[, -(1:2)]))))
  }
  # Non-finite sensor samples are unavailable too.
  d$depth[11:30] <- Inf
  out <- .efe_run(d, variables = "depth", metrics = "energy", window.size = 5)
  expect_true(is.na(out$depth_energy[20]))
})

test_that("unlabelled response windows remain NA, including after downsampling", {
  d <- .efe_data(); d$feeding <- factor(rep(NA_integer_, nrow(d)), levels = c(0, 1))
  for (agg in c(FALSE, TRUE)) for (ds in list(NULL, 0.2)) {
    out <- .efe_run(d, variables = "depth", metrics = "mean", window.size = 5,
                    aggregate = agg, response.col = "feeding", response.aggregation = "any",
                    downsample.to = ds)
    expect_true(all(is.na(out$feeding)))
  }
  d$feeding <- factor(c(NA, 0, 1, NA, NA, rep(NA, 35)), levels = c(0, 1))
  out <- .efe_run(d, variables = "depth", metrics = "mean", window.size = 5,
                  aggregate = TRUE, response.col = "feeding", response.aggregation = "any")
  expect_equal(as.character(out$feeding[1]), "1")
  expect_true(is.na(out$feeding[2]))
})

test_that("even windows and scales use a common timestamp in both modes", {
  d <- .efe_data(12); d$heading <- d$depth
  pg <- data.frame(variable = c("depth", "heading", "depth", "heading"),
                   metric = c("mean", "mean", "mean", "cumulative_heading_change"),
                   window_seconds = c(4, 4, 2, 4))
  out <- .efe_run(d, parameter.grid = pg, window.size = 4, enhanced.features = TRUE)
  expect_equal(out$depth_mean[2:10], seq(2.5, 10.5))
  expect_equal(out$heading_mean[2:10], out$depth_mean[2:10], tolerance = 1e-8)
  expect_equal(out$depth_mean_2s[2:10], seq(2.5, 10.5))
  expect_equal(out$heading_cumulative_change[2:10], rep(3, 9))
  agg <- .efe_run(d, parameter.grid = pg, window.size = 4, aggregate = TRUE,
                  enhanced.features = TRUE)
  expect_equal(agg$datetime, d$datetime[c(1, 5, 9)])
  expect_equal(agg$depth_mean, c(2.5, 6.5, 10.5))
  expect_equal(agg$depth_mean_2s, c(1.5, 5.5, 9.5))
  expect_equal(agg$heading_cumulative_change, rep(3, 3))
})

test_that("stride selects shared anchors independently of the window width", {
  d <- .efe_data(40, hz = 2)
  out <- .efe_run(d, variables = "depth", metrics = "mean", window.size = 3, step.size = 1)
  expect_equal(out$datetime, d$datetime[seq(1, 40, by = 2)])
  expect_equal(out$depth_mean[3:18], seq(5.5, 35.5, by = 2))
  pg <- data.frame(variable = "depth", metric = "mean", window_seconds = c(2, 4))
  out <- .efe_run(d, parameter.grid = pg, window.size = 2, aggregate = TRUE, step.size = 1)
  expect_equal(out$datetime, d$datetime[seq(1, 40, by = 2)])
  expect_equal(out$depth_mean[1:17], seq(2.5, 34.5, by = 2))
  expect_equal(out$depth_mean_4s[1:17], seq(4.5, 36.5, by = 2))
})

test_that("explicit segments and timestamp gaps cannot contribute to each other's windows or bins", {
  d <- .efe_data(20); d$segment <- rep(c("A", "B"), each = 10)
  d$depth <- rep(c(0, 100), each = 10)
  out <- .efe_run(d, variables = "depth", metrics = "mean", window.size = 5, segment.col = "segment")
  expect_true(all(is.na(out$depth_mean[9:12])))
  expect_equal(out$depth_mean[c(8, 13)], c(0, 100))
  expect_equal(out$segment, d$segment)
  out <- .efe_run(d, variables = "depth", metrics = "mean", window.size = 4,
                  aggregate = TRUE, segment.col = "segment")
  expect_equal(out$datetime, d$datetime[c(1, 5, 9, 11, 15, 19)])
  expect_equal(out$depth_mean, c(0, 0, 0, 100, 100, 100))
  out <- .efe_run(d, variables = "depth", metrics = "mean", window.size = 1,
                  segment.col = "segment", downsample.to = 1 / 30)
  expect_equal(out$depth_mean, c(0, 100))
  d$datetime[11:20] <- d$datetime[11:20] + 100
  out <- .efe_run(d, variables = "depth", metrics = "mean", window.size = 5)
  expect_true(all(is.na(out$depth_mean[9:12])))
})

test_that("coverage is checked per channel against the requested width", {
  d <- .efe_data(12); d$depth <- c(NA, NA, 3:12); d$heading[3] <- NA
  pg <- data.frame(variable = c("depth", "heading"), metric = "mean")
  out <- .efe_run(d, parameter.grid = pg, window.size = 4, aggregate = TRUE,
                  min.coverage = c(depth = 0.75, heading = 0.5))
  expect_true(is.na(out$depth_mean[1])); expect_true(is.finite(out$heading_mean[1]))
  expect_true(all(is.finite(out$depth_mean[2:3])))
  out <- .efe_run(d[1:10], variables = "depth", metrics = "mean", window.size = 4,
                  aggregate = TRUE, min.coverage = 0.75)
  expect_true(is.na(out$depth_mean[3])) # 2/4 samples in the final truncated window
  d$pitch <- 0; d$roll <- 0; d$activity <- 0; d$roll[1:3] <- NA
  out <- .efe_run(d, variables = "activity", metrics = "activity_index", enhanced.features = TRUE,
                  aggregate = TRUE, window.size = 4, min.coverage = c(roll = 0.75))
  expect_true(is.na(out$activity_index[1]))
})

test_that("physical derivatives scale by elapsed seconds and never bridge missing samples", {
  for (hz in c(1, 2, 5)) {
    d <- .efe_data(50 * hz, hz); t <- (seq_len(nrow(d)) - 1) / hz
    d$depth <- 3 * t; d$heading <- (359 + 4 * t) %% 360
    d$pitch <- t; d$roll <- (179 + 2 * t + 180) %% 360 - 180; d$activity <- 0
    pg <- data.frame(variable = c("depth", "heading", "depth", "depth", "activity"),
                     metric = c("rate", "rate", "depth_change_rate", "movement_jerk", "activity_index"))
    out <- .efe_run(d, parameter.grid = pg, window.size = 5, enhanced.features = TRUE)
    i <- 10 * hz
    expect_equal(as.numeric(out[i, -(1:2)]), c(3, 4, 3, 3, 7), tolerance = 1e-7)
    d$depth <- t^2
    out <- .efe_run(d, variables = "depth", metrics = "movement_smoothness",
                    window.size = 5, enhanced.features = TRUE)
    expect_equal(out$depth_movement_smoothness[i], 2, tolerance = 1e-7)
  }
  expect_true(is.na(nautilus:::.circularMetric(c(0, NA, 10), "rate", 1)))
})

test_that("new controls reject malformed arguments and record reproducible settings", {
  d <- .efe_data()
  for (extra in list(list(step.size = 0), list(step.size = NA_real_),
                     list(min.coverage = 1.1), list(min.coverage = c(0.2, 0.8)),
                     list(segment.col = "absent"),
                     list(parameter.grid = data.frame(variable = "depth", metric = "mean", window_seconds = -1)))) {
    args <- c(list(d = d, variables = "depth", metrics = "mean"), extra)
    expect_error(do.call(.efe_run, args))
  }
  out <- .efe_run(d, variables = "depth", metrics = "mean", window.size = 4,
                  step.size = 2, min.coverage = 0.75)
  pr <- tail(nautilus:::.getMeta(out)$processing, 1)[[1]]
  expect_equal(pr$step_size, 2)
  expect_equal(pr$min_coverage, 0.75)
})


test_that("downsampling averages circular locations as vectors on a fixed time grid", {
  d <- .efe_data(8); d$heading <- rep(c(179, 181), 4)
  pg <- data.frame(variable = "heading", metric = c("mean", "median", "sd"))
  out <- .efe_run(d, parameter.grid = pg, window.size = 1, downsample.to = 0.5)
  expect_true(all(abs(abs(out$heading_mean) - 180) < 1e-8))
  expect_true(all(abs(abs(out$heading_median) - 180) < 1e-8))
  # Bins stay anchored to input time after complete-case edge rows are omitted.
  out <- suppressWarnings(extractFeatures(list(EDGE = d), variables = "depth", metrics = "mean",
                                          window.size = 5, downsample.to = 0.25, verbose = FALSE)$EDGE)
  expect_equal(out$datetime, d$datetime[c(1, 5)])
  # Only absolute angular locations use vector averages; spread stays linear.
  d <- .efe_data(8); d$heading <- c(0, 0, 0, 90, 180, 180, 180, 270)
  raw <- .efe_run(d, variables = "heading", metrics = "sd", window.size = 2)
  out <- .efe_run(d, variables = "heading", metrics = "sd", window.size = 2, downsample.to = 0.25)
  expect_equal(out$heading_sd[1], mean(raw$heading_sd[1:4], na.rm = TRUE))
})

test_that("missing roll and surface windows cannot become false neutral activity", {
  d <- .efe_data(12); d$depth <- 0; d$vertical_speed <- NA_real_
  pg <- data.frame(variable = rep(c("depth", "vertical_speed"), each = 3),
                   metric = rep(c("skewness", "kurtosis", "entropy"), 2))
  skip_if_not_installed("moments"); skip_if_not_installed("entropy")
  out <- .efe_run(d, parameter.grid = pg, window.size = 4, aggregate = TRUE, min.coverage = 0.75)
  expect_equal(out$depth_kurtosis, c(3, 3, 3))
  expect_true(all(is.na(out$vertical_speed_kurtosis)))
  d$depth[1:4] <- NA_real_
  out <- .efe_run(d, parameter.grid = pg, window.size = 4, aggregate = TRUE)
  expect_true(is.na(out$depth_kurtosis[1]))
})

test_that("all enhanced metrics share missing-data and short-segment semantics", {
  d <- .efe_data(40)
  d$pitch <- sin(seq_len(40)); d$roll <- 0; d$activity <- 0; d$posture <- 0
  d$heading[11:30] <- NA; d$depth[11:30] <- NA
  d$pitch[11:30] <- NA; d$roll[11:30] <- NA
  hm <- c("net_heading_change", "cumulative_heading_change", "circular_variance_heading",
          "uturn_flag", "heading_autocorr_avg", "turning_rate_variability", "circling_behavior")
  mm <- c("oscillation_regularity", "movement_predictability", "movement_consistency", "movement_jerk",
          "movement_smoothness", "rolling_autocorrelation", "zero_crossing_rate",
          "depth_change_rate", "depth_change_consistency")
  pg <- data.frame(variable = c(rep("heading", length(hm)), rep("depth", length(mm)), "activity", "posture"),
                   metric = c(hm, mm, "activity_index", "posture_stability"))
  for (aggregate in c(FALSE, TRUE)) {
    out <- .efe_run(d, parameter.grid = pg, enhanced.features = TRUE, window.size = 5, aggregate = aggregate)
    mid <- out$datetime >= d$datetime[16] & out$datetime <= d$datetime[21]
    expect_true(all(is.na(as.matrix(out[mid, -(1:2)]))))
    expect_false(any(is.infinite(as.matrix(out[, -(1:2)]))))
    short <- .efe_run(d[1:3], parameter.grid = pg, enhanced.features = TRUE, window.size = 5, aggregate = aggregate)
    expect_equal(nrow(short), if (aggregate) 1 else 3)
    if (!aggregate) expect_true(all(is.na(as.matrix(short[, -(1:2)]))))
  }
})

test_that("reused and missing segment labels start distinct runs", {
  d <- .efe_data(20); d$segment <- c(rep("A", 6), NA, rep("B", 6), rep("A", 7))
  d$depth <- seq_len(20)
  out <- .efe_run(d, variables = "depth", metrics = "rate", window.size = 3, segment.col = "segment")
  expect_true(all(is.na(out$depth_rate[c(1, 6:8, 13:14, 20)])))
  expect_equal(out$depth_rate[c(3, 10, 17)], c(1, 1, 1))
  out <- .efe_run(d, variables = "depth", metrics = "mean", window.size = 1,
                  segment.col = "segment", downsample.to = 1 / 30)
  expect_equal(nrow(out), 4)
  expect_equal(out$depth_mean[c(1, 3, 4)], c(3.5, 10.5, 17))
  expect_true(is.na(out$depth_mean[2]))
})

test_that("bare tables, custom time columns and grid inputs preserve package conventions", {
  d <- .efe_data(12); data.table::setnames(d, "datetime", "time")
  pg <- data.table::data.table(variable = "depth", metric = "MEAN", window_seconds = NA_real_)
  before <- data.table::copy(pg)
  out <- suppressWarnings(extractFeatures(d, parameter.grid = pg, datetime.col = "time",
                                          window.size = 4, verbose = FALSE)$EDGE)
  expect_named(out, c("ID", "time", "depth_mean"))
  expect_equal(nrow(out), 9)
  expect_identical(pg, before)
  d$time[3] <- d$time[2]
  expect_error(.efe_run(d, variables = "depth", metrics = "mean", datetime.col = "time"), "increasing")
})

test_that("dense rolling kernels and sparse stride windows agree", {
  set.seed(22)
  d <- .efe_data(100)
  d$depth <- rnorm(100); d$heading <- (179 + rnorm(100)) %% 360
  d$depth[c(7:12, 20, 40)] <- NA; d$heading[c(10, 30:45)] <- NA
  d$segment <- rep(c("A", "B"), each = 50)
  for (w in c(1, 4, 9)) {
    pg <- rbind(data.frame(variable = "depth", metric = c("mean", "sum", "energy", "rate")),
                data.frame(variable = "heading", metric = c("mean", "sd", "mrl", "circular_variance_heading")))
    dense <- .efe_run(d, parameter.grid = pg, window.size = w, enhanced.features = TRUE,
                      segment.col = "segment", min.coverage = 0.6)
    sparse <- .efe_run(d, parameter.grid = pg, window.size = w, enhanced.features = TRUE,
                       segment.col = "segment", min.coverage = 0.6, step.size = 5)
    expect_equal(as.data.frame(sparse), as.data.frame(dense[seq(1, 100, by = 5)]), tolerance = 1e-7,
                 ignore_attr = TRUE)
  }
})

test_that("undefined vector means after binning obey the missing-feature policy", {
  d <- .efe_data(4); d$heading <- c(0, 180, 0, 180)
  kept <- .efe_run(d, variables = "heading", metrics = "mean", window.size = 1, downsample.to = 0.5)
  expect_equal(nrow(kept), 2)
  expect_true(all(is.na(kept$heading_mean)))
  omitted <- suppressWarnings(extractFeatures(list(EDGE = d), variables = "heading", metrics = "mean",
    window.size = 1, downsample.to = 0.5, verbose = FALSE)$EDGE)
  expect_equal(nrow(omitted), 0)
})

test_that("even response windows use the feature alignment and stride", {
  d <- .efe_data(8); d$feeding <- c(1, rep(0, 7))
  out <- .efe_run(d, variables = "depth", metrics = "mean", window.size = 4,
                  response.col = "feeding", response.aggregation = "any")
  expect_equal(as.numeric(as.character(out$feeding)), c(NA, 1, 0, 0, 0, 0, NA, NA))
  out <- .efe_run(d, variables = "depth", metrics = "mean", window.size = 4, step.size = 2,
                  response.col = "feeding", response.aggregation = "any")
  expect_equal(as.numeric(as.character(out$feeding)), c(NA, 0, 0, NA))
})

test_that("file input and saved output honour the same multi-scale controls", {
  d <- .efe_data(24); d$segment <- rep(c("A", "B"), each = 12)
  input <- tempfile(fileext = ".rds"); saveRDS(d, input)
  output <- tempfile(); dir.create(output)
  on.exit(unlink(c(input, output), recursive = TRUE), add = TRUE)
  pg <- data.frame(variable = "depth", metric = "mean", window_seconds = c(4, 8))
  args <- list(parameter.grid = pg, window.size = 4, step.size = 2, segment.col = "segment",
               min.coverage = 0.75, missing.features = "keep", verbose = FALSE)
  memory <- suppressWarnings(do.call(extractFeatures, c(list(data = list(EDGE = d)), args))$EDGE)
  paths <- suppressWarnings(do.call(extractFeatures, c(list(data = input, return.data = FALSE,
                                                           output.dir = output), args)))
  disk <- readRDS(paths[1])
  expect_equal(lapply(disk, identity), lapply(memory, identity))
  rec <- tail(nautilus:::.getMeta(disk)$processing, 1)[[1]]
  expect_equal(rec$segment_col, "segment")
  expect_equal(rec$parameter_grid, pg)
})

test_that("untyped NA channel placeholders remain unavailable measurements", {
  d <- .efe_data(12); d$depth <- NA
  for (aggregate in c(FALSE, TRUE)) {
    out <- .efe_run(d, variables = "depth", metrics = c("sum", "energy", "min", "max"),
                    window.size = 4, aggregate = aggregate)
    expect_true(all(is.na(as.matrix(out[, -(1:2)]))))
  }
})
