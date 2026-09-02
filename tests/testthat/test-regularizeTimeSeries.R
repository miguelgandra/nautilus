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

test_that("file-path input skips a deployment missing a required column, without aborting", {
  d <- .mkdt(c("depth"))
  d[["datetime"]] <- NULL                                 # drop the datetime column
  dir <- file.path(tempdir(), paste0("rts_bad_", as.integer(runif(1, 1, 1e7))))
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE), add = TRUE)
  f <- file.path(dir, "A01.rds")
  saveRDS(d, f)

  # A file missing a required column no longer aborts: it is ONE unusable deployment, and killing the
  # batch discarded every deployment already processed. It is skipped and absent from the result, and
  # a healthy deployment alongside it still comes through.
  good <- .mkdt(c("depth")); good[, ID := "A02"]
  saveRDS(good, file.path(dir, "A02.rds"))
  out <- .rts(sort(list.files(dir, full.names = TRUE)))
  expect_length(out, 1L)
  expect_false(is.null(out$A02))

  # the skip is announced at any verbosity - `.rts` swallows warnings, so assert on a direct call
  expect_warning(
    invisible(capture.output(regularizeTimeSeries(f, return.data = TRUE, verbose = FALSE))),
    "skipped")
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

test_that("flagged deployments are named worst-first, and healthy ones only counted", {
  set.seed(1)
  dat <- list(A_clean = .mk_reg("A_clean"),
              B_gap   = .mk_reg("B_gap", gap_extra = 120),   # ~11% of grid is NA -> critical
              C_jit   = .mk_reg("C_jit", jitter = 0.4))      # jitter -> interpolation
  out <- paste(cli::cli_fmt(suppressWarnings(
    regularizeTimeSeries(dat, gap.threshold = 5, return.data = TRUE, plot = FALSE, verbose = 1))),
    collapse = "\n")
  expect_match(out, "Needs review")
  expect_match(out, "B_gap")
  expect_match(out, "critical")
  # the clean deployment is counted in the status tally, never named IN THE SUMMARY (it still gets
  # its own per-deployment line above, which is where a caller looks for one deployment)
  tail <- sub(".*SUMMARY", "", gsub("\n", " ", out))
  expect_false(grepl("A_clean", tail, fixed = TRUE))
  # and where several are flagged, the worst leads
  listed <- sub(".*Needs review", "", out)
  if (grepl("C_jit", listed)) expect_lt(regexpr("B_gap", listed), regexpr("C_jit", listed))
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

# ---- channel cadence: a slower channel is not a gap ------------------------------------------------

test_that(".channelCadence measures a channel's own sampling spacing, conservatively", {
  expect_equal(nautilus:::.channelCadence(as.numeric(1:100)), 1)          # sampled every row
  x <- as.numeric(1:100); x[40:45] <- NA
  expect_equal(nautilus:::.channelCadence(x), 1)                          # real dropouts do not move the median
  s <- rep(NA_real_, 200); s[seq(1, 200, by = 20)] <- 1
  expect_equal(nautilus:::.channelCadence(s), 20)                         # 1 Hz against a 20 Hz grid
  expect_equal(nautilus:::.channelCadence(c(1, NA, NA)), 1)               # too few observations -> conservative
  expect_equal(nautilus:::.channelCadence(rep(NA_real_, 10)), 1)
})

test_that("a sub-grid-rate channel is left at its own cadence, not densified into the grid", {
  # A tag that logs paddle speed at 1 Hz against a 20 Hz inertial grid leaves 19 of every 20 rows empty for
  # that channel. Interpolating them fabricates ~20x the samples the tag ever recorded, and every pooled
  # statistic downstream then counts the copies as independent observations.
  n <- 4000; fs <- 20
  dt <- data.table::data.table(
    ID = "A01", datetime = as.POSIXct("2020-01-01", tz = "UTC") + (seq_len(n) - 1) / fs,
    ax = stats::rnorm(n), ay = stats::rnorm(n), az = 1 + stats::rnorm(n),
    depth = 20 + 10 * sin(seq_len(n) / 200), paddle_speed = NA_real_)
  dt$paddle_speed[seq(1, n, by = 20)] <- 0.5
  data.table::setattr(dt, "nautilus.version", "test")

  out <- NULL
  invisible(capture.output(suppressWarnings(
    out <- regularizeTimeSeries(list(A01 = dt), gap.threshold = 2, verbose = FALSE)[[1]])))

  expect_equal(sum(is.finite(out$paddle_speed)), n / 20)     # exactly what the tag recorded - nothing added
  expect_true(all(is.finite(out$depth)))                     # the grid-rate channels are untouched

  # and the coverage tally must not read the slow channel's empty rows as lost data
  step <- Filter(function(p) identical(p$step, "regularizeTimeSeries"),
                 nautilus:::.getMeta(out)$processing)[[1]]
  expect_equal(step$pct_interpolated, 0)
  expect_identical(step$status, "ok")
})

test_that("genuine dropouts in a grid-rate channel are still interpolated (no regression)", {
  n <- 4000; fs <- 20
  dt <- data.table::data.table(
    ID = "A01", datetime = as.POSIXct("2020-01-01", tz = "UTC") + (seq_len(n) - 1) / fs,
    ax = stats::rnorm(n), ay = stats::rnorm(n), az = 1 + stats::rnorm(n),
    depth = 20 + 10 * sin(seq_len(n) / 200))
  dt$depth[500:509] <- NA                                    # 10 rows = 0.5 s, inside gap.threshold
  data.table::setattr(dt, "nautilus.version", "test")

  out <- NULL
  invisible(capture.output(suppressWarnings(
    out <- regularizeTimeSeries(list(A01 = dt), gap.threshold = 2, verbose = FALSE)[[1]])))
  expect_false(anyNA(out$depth))                             # the true gap is filled, as before
})


# ---- the summary block -----------------------------------------------------------------------------
# A table of one row per deployment gets truncated and cannot be read at 46 deployments, so the summary
# is roll-ups plus only the deployments it is asking someone to look at.

.regCohort <- function(dir, n_clean = 4L) {
  t0 <- as.POSIXct("2020-01-01", tz = "UTC")
  mk <- function(id, tt) {
    x <- data.table::data.table(ID = id, datetime = tt, depth = seq_along(tt) %% 30)
    data.table::setattr(x, "nautilus.version", "test")
    saveRDS(x, file.path(dir, paste0(id, ".rds")))
  }
  for (i in seq_len(n_clean)) mk(sprintf("CLEAN_%02d", i), t0 + seq(0, 600, by = 1))
  mk("GAPPY", t0 + c(seq(0, 200, by = 1), seq(1400, 1600, by = 1)))    # a 20-minute hole
  list.files(dir, pattern = "\\.rds$", full.names = TRUE)
}
.regRun <- function(files, outd, ...) paste(cli::cli_fmt(suppressWarnings(
  regularizeTimeSeries(data = files, gap.threshold = 2, plot = FALSE, return.data = FALSE,
                       output.dir = outd, verbose = "detailed", ...))), collapse = "\n")


test_that("the summary rolls the cohort up instead of tabulating every deployment", {
  d <- withr::local_tempdir(); outd <- file.path(d, "out"); dir.create(outd)
  out <- .regRun(.regCohort(d), outd)
  tail <- sub(".*SUMMARY", "", gsub("\n", " ", out))

  expect_match(tail, "5 of 5 deployments processed")     # "processed", not "regularized"
  expect_match(tail, "rows added: median")
  expect_match(tail, "interpolated: median [0-9.]+% of grid points")
  expect_match(tail, "gaps: 4 deployments gap-free")
  expect_match(tail, "largest 20.0 min \\(GAPPY\\)")      # the worst gap, and who owns it

  # the healthy deployments are counted, never listed
  expect_match(tail, "ok:")
  for (i in 1:4) expect_false(grepl(sprintf("CLEAN_%02d", i), tail))
  expect_match(tail, "Needs review")
  expect_match(tail, "GAPPY")
})


test_that("a clean run has nothing to review, and says so by omission", {
  d <- withr::local_tempdir(); outd <- file.path(d, "out"); dir.create(outd)
  t0 <- as.POSIXct("2020-01-01", tz = "UTC")
  for (i in 1:3) {
    x <- data.table::data.table(ID = sprintf("C%d", i), datetime = t0 + seq(0, 600, by = 1),
                                depth = 1)
    data.table::setattr(x, "nautilus.version", "test"); saveRDS(x, file.path(d, sprintf("C%d.rds", i)))
  }
  out <- .regRun(list.files(d, pattern = "\\.rds$", full.names = TRUE), outd)
  expect_match(out, "3 of 3 deployments processed")
  expect_false(grepl("Needs review", out, fixed = TRUE))
})


test_that("the output pointers sit inside the summary and the runtime is last", {
  d <- withr::local_tempdir(); outd <- file.path(d, "out"); dir.create(outd)
  f <- file.path(d, "exclusions.csv")
  out <- .regRun(.regCohort(d), outd, exclusions.file = f, plot.file = file.path(d, "p.pdf"))
  lines <- strsplit(out, "\n", fixed = TRUE)[[1]]
  lines <- lines[nzchar(trimws(lines))]
  expect_match(lines[length(lines)], "runtime")          # nothing after the runtime
  expect_true(any(grepl("directory:", lines)))
  expect_true(any(grepl("exclusions:", lines)))
  expect_lt(max(grep("directory:", lines)), grep("runtime", lines)[1])
})


test_that("a long list of flagged deployments is capped", {
  metrics <- lapply(sprintf("BAD_%02d", 1:15), function(i)
    list(id = i, status = "critical", rows_added_pct = 50, pct_interp = 1, pct_gap = 30,
         largest_gap_s = 3600, n_gaps = 1L))
  out <- paste(cli::cli_fmt(nautilus:::.printRegularizationTriage(2L, metrics)), collapse = "\n")
  expect_match(out, "and 5 more")
  expect_match(out, "BAD_01")
})
