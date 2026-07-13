# Fixture-based tests for filterDeploymentData().
# Covers the Increment-5 fixes:
#   F1 - crash on the "no valid deployment" path (attachtime/poptime undefined)
#   F3 - .format_large_number used before defined on that same path
#   F4 - hardcoded column names broke non-default id/datetime/depth columns

# build a synthetic 1 Hz deployment: surface | deep | surface
.make_deployment <- function(id = "A01", deep = TRUE, cols = c("ID", "datetime", "depth")) {
  set.seed(42)
  surf <- 1800L; mid <- 3600L
  n <- 2L * surf + mid
  t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
  dt <- t0 + seq_len(n) - 1
  depth <- if (deep) {
    c(rep(0, surf), 20 + stats::rnorm(mid), rep(0, surf))
  } else {
    abs(stats::rnorm(n, 0, 0.5))  # shallow noise -> no deployment
  }
  d <- data.table::data.table(id, dt, pmax(0, depth), 15 + stats::rnorm(n), stats::rnorm(n))
  data.table::setnames(d, c(cols, "temp", "ax"))
  data.table::setattr(d, "nautilus.version", "test")  # avoid the "not processed" message
  list(data = d, t0 = t0, surf = surf, mid = mid)
}

run_quiet <- function(expr) {
  res <- NULL
  invisible(capture.output(suppressWarnings(suppressMessages(res <- eval.parent(substitute(expr))))))
  res
}

test_that("filterDeploymentData restores graphics par() when drawing (no leak)", {
  f <- .make_deployment()
  custom <- data.frame(ID = "A01", start = f$t0 + f$surf, end = f$t0 + f$surf + f$mid)
  pdf(file.path(tempdir(), paste0("par_", as.integer(runif(1, 1, 1e7)), ".pdf")))
  on.exit(dev.off(), add = TRUE)
  before_mar <- par("mar")
  invisible(capture.output(suppressWarnings(suppressMessages(
    filterDeploymentData(list(A01 = f$data), custom.deployment.times = custom,
                         plot.metrics = c("temp", "ax"),
                         plot = TRUE,
                         return.data = TRUE, verbose = FALSE)))))
  expect_equal(par("mar"), before_mar)   # caller's margins must be restored
})

test_that("filterDeploymentData writes a single multi-page PDF to plot.file without touching the active device", {
  f <- .make_deployment()
  custom <- data.frame(ID = "A01", start = f$t0 + f$surf, end = f$t0 + f$surf + f$mid)
  pfile <- file.path(tempdir(), paste0("fdd_", as.integer(runif(1, 1, 1e7)), ".pdf"))
  on.exit(unlink(pfile), add = TRUE)

  # no active device open: plot = FALSE means plot.file must not require/open one
  if (!is.null(grDevices::dev.list())) grDevices::graphics.off()
  run_quiet(filterDeploymentData(list(A01 = f$data), custom.deployment.times = custom,
                                 plot.metrics = c("temp", "ax"),
                                 plot = FALSE, plot.file = pfile,
                                 return.data = TRUE, verbose = FALSE))

  expect_true(file.exists(pfile))
  expect_gt(file.size(pfile), 0)
  # the active device must not have been opened by the file path
  expect_null(grDevices::dev.list())
})

test_that("filterDeploymentData errors fast when plot.file's directory does not exist", {
  f <- .make_deployment()
  bad <- file.path(tempdir(), "no_such_dir_xyz", "out.pdf")
  expect_error(suppressWarnings(filterDeploymentData(list(A01 = f$data), plot.file = bad,
                                                     return.data = TRUE, verbose = FALSE)),
               "does not exist")
})

test_that("filterDeploymentData errors fast on a non-existent output.dir", {
  f <- .make_deployment()
  expect_error(filterDeploymentData(list(A01 = f$data),
                                    output.dir = file.path(tempdir(), "missing_out_xyz"),
                                    return.data = FALSE, verbose = FALSE),
               "does not exist")
})

test_that("filterDeploymentData applies a full custom deployment window", {
  f <- .make_deployment()
  custom <- data.frame(ID = "A01", start = f$t0 + f$surf, end = f$t0 + f$surf + f$mid)

  res <- run_quiet(filterDeploymentData(
    list(A01 = f$data), custom.deployment.times = custom,
 plot = FALSE, return.data = TRUE
  ))

  expect_named(res, "filtered_data")
  kept <- res$filtered_data$A01
  expect_false(is.null(kept))
  expect_true(min(kept$datetime) >= custom$start)
  expect_true(max(kept$datetime) <= custom$end)
  # most kept records should be from the deep phase
  expect_gt(stats::median(kept$depth), 3.5)
})

test_that("filterDeploymentData works with non-default column names (F4 regression)", {
  f <- .make_deployment(cols = c("animal", "time", "dep"))

  res <- run_quiet(filterDeploymentData(
    list(A01 = f$data), id.col = "animal", datetime.col = "time", depth.col = "dep",
    custom.deployment.times = NULL,
 plot = FALSE, return.data = TRUE
  ))

  kept <- res$filtered_data$A01
  expect_false(is.null(kept))
  # the automated detection should trim the surface padding
  expect_lt(nrow(kept), nrow(f$data))
  expect_gt(stats::median(kept$dep), 3.5)
})

test_that("filterDeploymentData honours custom.deployment.times with a non-default id.col", {
  f <- .make_deployment(cols = c("animal", "time", "depth"))
  custom <- data.frame(animal = "A01", start = f$t0 + f$surf, end = f$t0 + f$surf + f$mid)

  res <- run_quiet(filterDeploymentData(
    list(A01 = f$data), id.col = "animal", datetime.col = "time",
    custom.deployment.times = custom,
 plot = FALSE, return.data = TRUE
  ))

  kept <- res$filtered_data$A01
  expect_false(is.null(kept))
  expect_true(min(kept$time) >= custom$start)
  expect_true(max(kept$time) <= custom$end)
})

test_that("filterDeploymentData is silent when verbose = FALSE", {
  f <- .make_deployment()
  custom <- data.frame(ID = "A01", start = f$t0 + f$surf, end = f$t0 + f$surf + f$mid)

  out <- capture.output(suppressWarnings(suppressMessages(
    res <- filterDeploymentData(list(A01 = f$data), custom.deployment.times = custom,
 plot = FALSE,
                                return.data = TRUE, verbose = FALSE)
  )))
  expect_length(out, 0)
})

test_that("filterDeploymentData discards datasets with no deployment without crashing (F1/F3)", {
  f <- .make_deployment(deep = FALSE)

  expect_no_error(
    res <- run_quiet(filterDeploymentData(
      list(A01 = f$data), custom.deployment.times = NULL,
 plot = FALSE, return.data = TRUE
    ))
  )
  # discarded -> filtered_data is present but empty
  expect_length(res$filtered_data, 0)
})

# --- temperature corroboration + min-duration guard (refinements) ---------------------------------

# shallow deployment depth alone misses, but a clear sustained temperature regime shift reveals it
.make_shallow_temp <- function() {
  set.seed(7)
  pre <- 600L; mid <- 3600L; post <- 600L; n <- pre + mid + post
  t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC"); dt <- t0 + seq_len(n) - 1
  depth <- pmax(0, abs(stats::rnorm(n, 0, 0.5)))                 # always shallow -> depth finds nothing
  temp  <- c(rep(25, pre), rep(15, mid), rep(25, post)) + stats::rnorm(n, 0, 0.1)  # boat 25C, water 15C
  d <- data.table::data.table(ID = "A01", dt, depth, temp)
  data.table::setnames(d, c("ID", "datetime", "depth", "temp"))
  data.table::setattr(d, "nautilus.version", "test")
  list(data = d, t0 = t0, pre = pre, mid = mid)
}

test_that("temperature rescues a shallow deployment that depth alone misses", {
  f <- .make_shallow_temp()

  # depth-only: no deployment -> discarded
  res0 <- run_quiet(filterDeploymentData(list(A01 = f$data), use.temperature = FALSE,
                                         plot = FALSE, return.data = TRUE))
  expect_length(res0$filtered_data, 0)

  # with temperature: the in-water (15 C) period is recovered
  res1 <- run_quiet(filterDeploymentData(list(A01 = f$data), use.temperature = TRUE,
                                         plot = FALSE, return.data = TRUE))
  kept <- res1$filtered_data$A01
  expect_false(is.null(kept))
  expect_gt(nrow(kept), 0.8 * f$mid)                # ~ the 3600 s in-water stretch
  expect_lt(nrow(kept), 1.1 * f$mid)                # but not the warm boat ends
  expect_true(all(abs(kept$temp - 15) < 1))         # only the cold (in-water) samples
})

test_that("a flat temperature trace leaves the depth result unchanged (temperature is a no-op)", {
  f <- .make_deployment()   # deep 1 h window, flat ~15 C temperature
  on_temp  <- run_quiet(filterDeploymentData(list(A01 = f$data), use.temperature = TRUE,
                                             plot = FALSE, return.data = TRUE))$filtered_data$A01
  off_temp <- run_quiet(filterDeploymentData(list(A01 = f$data), use.temperature = FALSE,
                                             plot = FALSE, return.data = TRUE))$filtered_data$A01
  expect_equal(nrow(on_temp), nrow(off_temp))       # identical window
})

test_that("min.deployment.hours discards windows that are too short", {
  set.seed(11)
  surf <- 1800L; spike <- 300L; n <- 2L * surf + spike     # 5-min deep spike
  t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC"); dt <- t0 + seq_len(n) - 1
  depth <- c(rep(0, surf), 20 + stats::rnorm(spike), rep(0, surf))
  d <- data.table::data.table(ID = "A01", dt, pmax(0, depth), 15 + stats::rnorm(n, 0, 0.1))
  data.table::setnames(d, c("ID", "datetime", "depth", "temp"))
  data.table::setattr(d, "nautilus.version", "test")

  # default guard (0.25 h = 15 min) discards the 5-min spike
  res_def <- run_quiet(filterDeploymentData(list(A01 = d), plot = FALSE, return.data = TRUE))
  expect_length(res_def$filtered_data, 0)

  # disabling the guard keeps it
  res_off <- run_quiet(filterDeploymentData(list(A01 = d), min.deployment.hours = 0,
                                            plot = FALSE, return.data = TRUE))
  expect_false(is.null(res_off$filtered_data$A01))
})

test_that("header is not a false global method, and the summary reports the realized custom/auto mix", {
  a <- .make_deployment(id = "A01"); b <- .make_deployment(id = "B02")
  dat <- list(A01 = a$data, B02 = b$data)
  custom <- data.frame(ID = "A01", start = a$t0 + a$surf, end = a$t0 + a$surf + a$mid)  # custom for A01 only

  # the verbose header/summary is emitted via cli; cli_fmt() captures it reliably (plain
  # capture.output() does not see cli output once testthat is managing the output streams).
  mixed <- paste(cli::cli_fmt(suppressWarnings(
    filterDeploymentData(dat, custom.deployment.times = custom, return.data = TRUE,
                         plot = FALSE, verbose = 1))),
    collapse = "\n")
  expect_match(mixed, "custom windows")                        # header shows custom-window capability
  expect_match(mixed, "\\(1 custom, 1 automatic\\)")           # realized mix in the summary

  # with no custom times supplied, the header is plainly automatic and the summary has no method split
  auto <- paste(cli::cli_fmt(suppressWarnings(
    filterDeploymentData(dat, return.data = TRUE, plot = FALSE, verbose = 1))), collapse = "\n")
  expect_match(auto, "depth changepoints \\(automatic\\)")
  expect_false(grepl("custom", auto))
})

# --- custom.deployment.times validation + partial-window correctness -------------------------------

test_that("custom.deployment.times validation catches bad windows", {
  f <- .make_deployment()
  base <- function(ct) suppressWarnings(filterDeploymentData(list(A01 = f$data), custom.deployment.times = ct,
                                                             plot = FALSE, return.data = TRUE, verbose = FALSE))
  # end <= start
  expect_error(base(data.frame(ID = "A01", start = f$t0 + f$surf + f$mid, end = f$t0 + f$surf)), "after")
  # duplicate IDs
  expect_error(base(data.frame(ID = c("A01", "A01"), start = f$t0 + c(1, 2), end = f$t0 + c(10, 20))), "duplicate")
  # both boundaries missing
  expect_error(base(data.frame(ID = "A01", start = as.POSIXct(NA), end = as.POSIXct(NA))), "both")
  # unknown ID -> warning (not error); call without suppressWarnings so it can be caught
  expect_warning(
    invisible(capture.output(suppressMessages(
      filterDeploymentData(list(A01 = f$data),
                           custom.deployment.times = data.frame(ID = "NOPE", start = f$t0 + f$surf, end = f$t0 + f$surf + f$mid),
                           plot = FALSE, return.data = TRUE, verbose = FALSE)))),
    "not in the data")
})

test_that("a partial custom window honours the supplied boundary (padding-shift regression)", {
  f <- .make_deployment()                                  # surface | deep | surface, 1 Hz
  cstart <- f$t0 + f$surf + 600L                           # 10 min into the deep phase
  res <- run_quiet(filterDeploymentData(
    list(A01 = f$data),
    custom.deployment.times = data.frame(ID = "A01", start = cstart, end = as.POSIXct(NA)),
    plot = FALSE, return.data = TRUE))
  kept <- res$filtered_data$A01
  expect_equal(as.numeric(min(kept$datetime)), as.numeric(cstart), tolerance = 1)  # custom start respected
  expect_gt(max(kept$datetime), cstart)                                            # end inferred after it
})

test_that("an out-of-range custom window is clamped, not discarded", {
  f <- .make_deployment()
  span_start <- min(f$data$datetime); span_end <- max(f$data$datetime)
  res <- run_quiet(filterDeploymentData(
    list(A01 = f$data),
    custom.deployment.times = data.frame(ID = "A01", start = span_start - 3600, end = span_end + 3600),
    plot = FALSE, return.data = TRUE))
  kept <- res$filtered_data$A01
  expect_false(is.null(kept))                              # not discarded
  expect_gte(as.numeric(min(kept$datetime)), as.numeric(span_start))  # clamped within data
  expect_lte(as.numeric(max(kept$datetime)), as.numeric(span_end))
})

test_that(".metricLabel auto-generates 'Name (unit)' and falls back to the raw name", {
  expect_equal(nautilus:::.metricLabel("temp"), "Temperature (\u00b0C)")
  expect_equal(nautilus:::.metricLabel("ax"), "Acc X (g)")
  expect_equal(nautilus:::.metricLabel("depth"), "Depth (m)")
  expect_equal(nautilus:::.metricLabel("made_up"), "made_up")
})

test_that("filterDeploymentData runs without a temp column and just omits its plot strip", {
  # electronics-temperature-only tags are imported without `temp`; temp must not be strictly required
  f <- .make_deployment()
  f$data[, temp := NULL]
  custom <- data.frame(ID = "A01", start = f$t0 + f$surf, end = f$t0 + f$surf + f$mid)

  # plotting ON with the DEFAULT plot.metrics (c("temp", "ax")): temp is absent, ax is present
  pfile <- file.path(tempdir(), paste0("notemp_", as.integer(runif(1, 1, 1e7)), ".pdf"))
  on.exit(unlink(pfile), add = TRUE)
  if (!is.null(grDevices::dev.list())) grDevices::graphics.off()
  res <- run_quiet(filterDeploymentData(list(A01 = f$data), custom.deployment.times = custom,
                                        plot = FALSE, plot.file = pfile,
                                        return.data = TRUE, verbose = FALSE))
  expect_true("A01" %in% names(res$filtered_data))          # processed, not aborted on missing temp
  expect_false("temp" %in% names(res$filtered_data$A01))    # temp stays absent
  expect_true("ax" %in% names(res$filtered_data$A01))       # the present plot metric is retained
  expect_true(file.exists(pfile) && file.size(pfile) > 0)   # panel rendered (temp strip simply omitted)

  # fully-automatic detection path also runs without temp (temperature corroboration skipped)
  res2 <- run_quiet(filterDeploymentData(list(A01 = f$data), plot = FALSE, return.data = TRUE, verbose = FALSE))
  expect_false(is.null(res2$filtered_data$A01))
})
