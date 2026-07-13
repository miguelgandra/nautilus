# Tests for regularizeTimeSeries() fixes:
#   R1 - partial sensor sets must not crash (was a hardcoded full-IMU column list)
#   R2 - time differences computed in seconds regardless of difftime auto-units
#   R3 - degenerate intervals (single row / identical timestamps) handled gracefully
#   R7 - interpolation limited to sensor channels (position columns left untouched)

.rts <- function(d, ...) {
  res <- NULL
  invisible(capture.output(suppressWarnings(suppressMessages(
    res <- regularizeTimeSeries(d, return.data = TRUE, verbose = FALSE, ...)))))
  res
}

# build one individual's data.table with a regular 1 Hz grid and a chosen set of channels
.mkdt <- function(channels, n = 30, gap_at = NULL) {
  t <- seq(as.POSIXct("2020-01-01 00:00:00", tz = "UTC"), by = 1, length.out = n)
  if (!is.null(gap_at)) t[(gap_at + 1):n] <- t[(gap_at + 1):n] + 10  # insert a 10 s gap
  dt <- data.table::data.table(ID = "A01", datetime = t)
  for (cc in channels) dt[[cc]] <- as.numeric(seq_len(n))
  data.table::setattr(dt, "nautilus.version", "test")
  dt
}

test_that("accelerometer-only data regularizes without crashing (R1)", {
  d <- .mkdt(c("ax", "ay", "az"), gap_at = 10)
  out <- .rts(list(A01 = d), gap.threshold = 5)
  expect_false(is.null(out$A01))
  expect_true(all(c("ax", "ay", "az") %in% names(out$A01)))
  expect_false(any(c("gx", "mx", "depth", "temp") %in% names(out$A01)))
})

test_that("TDR depth+temp-only data regularizes without crashing (R1)", {
  d <- .mkdt(c("depth", "temp"), gap_at = 10)
  out <- .rts(list(A01 = d), gap.threshold = 5)
  expect_false(is.null(out$A01))
  expect_true(all(c("depth", "temp") %in% names(out$A01)))
})

test_that("a single-row dataset is returned unchanged, not crashed (R3)", {
  t <- as.POSIXct("2020-01-01", tz = "UTC")
  d <- data.table::data.table(ID = "A01", datetime = t, ax = 1, ay = 2, az = 3)
  data.table::setattr(d, "nautilus.version", "test")
  out <- .rts(list(A01 = d))
  expect_equal(nrow(out$A01), 1L)
})

test_that("identical timestamps (zero interval) do not crash (R3)", {
  t <- rep(as.POSIXct("2020-01-01", tz = "UTC"), 5)
  d <- data.table::data.table(ID = "A01", datetime = t, ax = 1:5 * 1.0)
  data.table::setattr(d, "nautilus.version", "test")
  out <- .rts(list(A01 = d))
  expect_equal(nrow(out$A01), 5L)   # left unchanged, no error
})

test_that("a gap is regularized and short gaps interpolated", {
  d <- .mkdt(c("depth"), n = 30, gap_at = 10)   # 10 s gap after row 10
  out <- .rts(list(A01 = d), gap.threshold = 5, interpolation.method = "linear")$A01
  # grid is regular 1 Hz across the full span -> more rows than the original 30
  expect_gt(nrow(out), 30)
  d2 <- diff(as.numeric(out$datetime))
  expect_true(all(abs(d2 - 1) < 1e-6))          # exactly 1 s spacing
})

test_that("jitter (no gap) triggers regularization onto an exact grid (R6)", {
  # 1 Hz nominal, but one interval compressed to 0.4 s (deviation 0.6 s > 0.5 s
  # threshold). The old gaps-only rule (diff > 1.5 s) would NOT have caught this.
  t <- seq(as.POSIXct("2020-01-01 00:00:00", tz = "UTC"), by = 1, length.out = 20)
  t[10] <- t[10] - 0.6
  d <- data.table::data.table(ID = "A01", datetime = t, depth = as.numeric(1:20))
  data.table::setattr(d, "nautilus.version", "test")

  out <- .rts(list(A01 = d), gap.threshold = 0)$A01
  expect_true(attr(out, "regularization.performed"))
  expect_true(all(abs(diff(as.numeric(out$datetime)) - 1) < 1e-6))  # exact 1 s grid
})

test_that("a single data.frame input returns a named list (R5)", {
  t <- seq(as.POSIXct("2020-01-01 00:00:00", tz = "UTC"), by = 1, length.out = 10)
  df <- data.frame(ID = "A01", datetime = t, depth = as.numeric(1:10))
  out <- .rts(df)
  expect_type(out, "list")
  expect_named(out, "A01")
  expect_s3_class(out$A01, "data.table")
})

test_that("position columns (lat/lon) are NOT interpolated (R7)", {
  d <- .mkdt(c("depth"), n = 30, gap_at = 10)
  data.table::set(d, j = "lat", value = NA_real_)
  data.table::set(d, j = "lon", value = NA_real_)
  data.table::set(d, i = 1L, j = "lat", value = 38)   # a single sparse fix
  data.table::set(d, i = 1L, j = "lon", value = -25)
  out <- .rts(list(A01 = d), gap.threshold = 5)$A01
  expect_equal(sum(!is.na(out$lat)), 1L)        # still exactly one fix, not spread
})

test_that("coverage statistics are always recorded in the processing audit trail", {
  d <- .mkdt(c("depth", "temp"), n = 40, gap_at = 20)   # inserts a 10 s gap (interpolated at gap.threshold=15)
  out <- .rts(list(A01 = d), gap.threshold = 15)$A01
  h <- processingHistory(out)
  rec <- nautilus:::.getMeta(out)$processing
  step <- Filter(function(p) identical(p$step, "regularizeTimeSeries"), rec)[[1]]
  expect_true(all(c("nominal_hz", "jitter_mad_ms", "n_original", "n_regular",
                    "n_interpolated", "n_gap", "pct_interpolated", "pct_gap") %in% names(step)))
  expect_equal(step$nominal_hz, 1)
  expect_gt(step$n_interpolated, 0)              # the 10 s gap is below the 15 s threshold -> filled
})

test_that("plot.file writes a multi-page diagnostic PDF without touching the active device", {
  d <- .mkdt(c("depth", "temp"), n = 60, gap_at = 30)
  pfile <- file.path(tempdir(), paste0("rts_", as.integer(runif(1, 1, 1e7)), ".pdf"))
  on.exit(unlink(pfile), add = TRUE)
  if (!is.null(grDevices::dev.list())) grDevices::graphics.off()
  .rts(list(A01 = d, B02 = .mkdt(c("depth", "temp"), n = 60, gap_at = 20)),
       gap.threshold = 15, plot.file = pfile)
  expect_true(file.exists(pfile))
  expect_gt(file.size(pfile), 0)
  expect_null(grDevices::dev.list())
})

test_that("regularizeTimeSeries errors fast when plot.file's directory is missing", {
  d <- .mkdt(c("depth", "temp"))
  expect_error(.rts(list(A01 = d), plot.file = file.path(tempdir(), "no_dir_xyz", "x.pdf")), "does not exist")
})

test_that("file-path input regularizes without the required_cols bug (regression)", {
  d <- .mkdt(c("depth", "temp"), gap_at = 10)
  dir <- file.path(tempdir(), paste0("rts_in_", as.integer(runif(1, 1, 1e7))))
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE), add = TRUE)
  f <- file.path(dir, "A01.rds")
  saveRDS(d, f)

  out <- .rts(f, gap.threshold = 5)                       # data = a character path
  expect_false(is.null(out$A01))
  expect_true(all(c("depth", "temp") %in% names(out$A01)))
})

test_that("file-path input aborts when a required column is missing", {
  d <- .mkdt(c("depth"))
  d[["datetime"]] <- NULL                                 # drop the datetime column
  dir <- file.path(tempdir(), paste0("rts_bad_", as.integer(runif(1, 1, 1e7))))
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE), add = TRUE)
  f <- file.path(dir, "A01.rds")
  saveRDS(d, f)

  expect_error(.rts(f), "Missing required column")
})

# --- two-level triage report (redesign) ------------------------------------------------------------

# count pages in an R-generated PDF without a heavy dependency: "/Type /Page" minus "/Type /Pages"
.pdf_pages <- function(f) {
  b <- readBin(f, "raw", file.info(f)$size)
  length(grepRaw(charToRaw("/Type /Page"), b, all = TRUE)) -
    length(grepRaw(charToRaw("/Type /Pages"), b, all = TRUE))
}

# regular 1 Hz depth series, optionally with a big gap or timestamp jitter
.mk_reg <- function(id, n = 1000, gap_extra = 0, gap_at = 500, jitter = 0) {
  t <- seq(as.POSIXct("2020-01-01 00:00:00", tz = "UTC"), by = 1, length.out = n)
  if (jitter > 0) t <- sort(t + stats::rnorm(n, 0, jitter))
  if (gap_extra > 0) t[(gap_at + 1):n] <- t[(gap_at + 1):n] + gap_extra
  d <- data.table::data.table(ID = id, datetime = t, depth = 10 + sin(seq_len(n) / 9))
  data.table::setattr(d, "nautilus.version", "test")
  d
}

test_that("deployments are classified and reported worst-first in the triage table", {
  set.seed(1)
  dat <- list(A_clean = .mk_reg("A_clean"),
              B_gap   = .mk_reg("B_gap", gap_extra = 120),   # ~11% of grid is NA -> critical
              C_jit   = .mk_reg("C_jit", jitter = 0.4))      # jitter -> interpolation
  out <- paste(cli::cli_fmt(suppressWarnings(
    regularizeTimeSeries(dat, gap.threshold = 5, return.data = TRUE, plot = FALSE, verbose = 1))),
    collapse = "\n")
  expect_match(out, "REGULARIZATION SUMMARY")
  expect_match(out, "B_gap")
  expect_match(out, "critical")
  # within the triage table (after the header), the critical gappy deployment precedes the clean one
  tbl <- sub(".*REGULARIZATION SUMMARY", "", out)
  expect_lt(regexpr("B_gap", tbl), regexpr("A_clean", tbl))
})

test_that("only flagged deployments receive a detailed PDF page", {
  pf <- file.path(tempdir(), paste0("rep_", as.integer(runif(1, 1, 1e7)), ".pdf"))
  on.exit(unlink(pf), add = TRUE)
  dat <- list(A_clean = .mk_reg("A_clean"), B_clean = .mk_reg("B_clean"),
              C_gap = .mk_reg("C_gap", gap_extra = 120))
  invisible(capture.output(suppressWarnings(suppressMessages(
    regularizeTimeSeries(dat, gap.threshold = 5, plot.file = pf, return.data = TRUE, verbose = FALSE)))))
  expect_true(file.exists(pf))
  expect_equal(.pdf_pages(pf), 2L)        # 1 summary + 1 detail (only the gappy tag), not 3
})

test_that("all-healthy deployments yield only the summary page", {
  pf <- file.path(tempdir(), paste0("rep_", as.integer(runif(1, 1, 1e7)), ".pdf"))
  on.exit(unlink(pf), add = TRUE)
  dat <- list(A = .mk_reg("A"), B = .mk_reg("B"), C = .mk_reg("C"))
  invisible(capture.output(suppressWarnings(suppressMessages(
    regularizeTimeSeries(dat, plot.file = pf, return.data = TRUE, verbose = FALSE)))))
  expect_equal(.pdf_pages(pf), 1L)        # summary only; no per-deployment pages
})

test_that("force.plots draws a detailed page for every deployment", {
  pf <- file.path(tempdir(), paste0("rep_", as.integer(runif(1, 1, 1e7)), ".pdf"))
  on.exit(unlink(pf), add = TRUE)
  dat <- list(A = .mk_reg("A"), B = .mk_reg("B"))
  invisible(capture.output(suppressWarnings(suppressMessages(
    regularizeTimeSeries(dat, plot.file = pf, force.plots = TRUE, return.data = TRUE, verbose = FALSE)))))
  expect_equal(.pdf_pages(pf), 3L)        # summary + 2 detail
})

test_that("review.thresholds can relax classification (fewer flagged pages)", {
  pf <- file.path(tempdir(), paste0("rep_", as.integer(runif(1, 1, 1e7)), ".pdf"))
  on.exit(unlink(pf), add = TRUE)
  dat <- list(A = .mk_reg("A"), B = .mk_reg("B", gap_extra = 120))
  invisible(capture.output(suppressWarnings(suppressMessages(
    regularizeTimeSeries(dat, plot.file = pf, return.data = TRUE, verbose = FALSE,
                         review.thresholds = list(gap_pct_review = 90, gap_pct_critical = 95,
                                                  interp_pct_review = 90, interp_pct_critical = 95,
                                                  rows_added_pct_review = 900, rows_added_pct_critical = 999))))))
  expect_equal(.pdf_pages(pf), 1L)        # nothing crosses the loosened thresholds -> summary only
})

test_that("the audit trail records status and largest_gap_s", {
  d <- .mk_reg("A", gap_extra = 120)
  res <- .rts(list(A = d), gap.threshold = 5)$A
  step <- nautilus:::.getMeta(res)$processing
  rec  <- step[[length(step)]]
  expect_true(all(c("status", "largest_gap_s") %in% names(rec)))
  expect_true(rec$status %in% c("ok", "review", "critical"))
  expect_gt(rec$largest_gap_s, 0)
})

test_that("an unknown review.thresholds field errors clearly", {
  expect_error(.rts(list(A = .mk_reg("A")), review.thresholds = list(bogus = 1)), "Unknown")
})
