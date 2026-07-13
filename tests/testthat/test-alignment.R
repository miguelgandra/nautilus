# Tests for the cross-device clock alignment applied by importTagData(): alignmentControl() and the
# internal .readArchiveDepth()/.estimateClockOffset()/.alignWCClocks(). The primary archival tag
# (depth + IMU) and the paired Wildlife Computers tag (wet/dry + GPS) keep independent clocks; the
# offset is recovered by cross-correlating the shared depth channel. Fixtures are synthetic (a known
# multi-dive profile stamped on both devices with a known clock offset); the real-Wildlife-Computers
# validation (peak correlation ~1.0 across deployments; recovers the ~15-min offsets on PIN_01/PIN_03
# that drove the depth-drift over-correction) is recorded in AUDIT.md, not embedded here (research data).

# a smooth multi-dive depth profile at `fs` Hz over `dur` seconds -> a sharp, unique cross-correlation
# peak. Returned as absolute epoch seconds so it can be stamped on two independent clocks.
.mkDives <- function(dur = 7200, fs = 1, base = 1.5e9) {
  t <- seq(0, dur, by = 1 / fs)
  d <- 50 * exp(-((t - 1500) / 200)^2) +
       80 * exp(-((t - 3800) / 300)^2) +
       40 * exp(-((t - 6000) / 250)^2)
  list(t = base + t, depth = d)
}


test_that("alignmentControl(): defaults and validation", {
  a <- alignmentControl()
  expect_s3_class(a, "nautilus_alignment")
  expect_equal(a$method, "depth-xcorr")
  expect_equal(a$max.lag, 3600)
  expect_equal(a$min.overlap, 30)
  expect_equal(a$min.correlation, 0.9)
  expect_equal(alignmentControl(method = "none")$method, "none")
  expect_error(alignmentControl(method = "bogus"))
  expect_error(alignmentControl(min.correlation = 1.5), "min.correlation")
  expect_error(alignmentControl(max.lag = 0), "max.lag")
  # resolve-from-list path (as importTagData does) rejects unknown fields
  expect_error(nautilus:::.as_control(list(nope = 1), alignmentControl, "nautilus_alignment", "alignment"),
               "unknown field")
})


test_that(".estimateClockOffset(): recovers a known constant offset with peak correlation ~1", {
  p <- .mkDives()
  delta <- 300                                   # WC clock runs 300 s AHEAD of the primary tag
  est <- nautilus:::.estimateClockOffset(p$t, p$depth, p$t + delta, p$depth, alignmentControl())
  expect_equal(est$status, "aligned")
  expect_lt(abs(est$lag - (-delta)), 3)          # lag = value to ADD to WC time = -delta
  expect_gt(est$correlation, 0.98)
  expect_lt(est$correlation.unaligned, est$correlation)
})


test_that(".estimateClockOffset(): abstains when the depth records barely overlap", {
  p <- .mkDives(dur = 600)                        # 10 min record, well under the 30 min minimum overlap
  est <- nautilus:::.estimateClockOffset(p$t, p$depth, p$t, p$depth, alignmentControl())
  expect_equal(est$status, "abstained")
  expect_match(est$reason, "overlap")
})


test_that(".estimateClockOffset(): abstains when the reference depth is flat (no dives to lock onto)", {
  p <- .mkDives()
  flat <- rep(10, length(p$t))
  est <- nautilus:::.estimateClockOffset(p$t, flat, p$t + 120, p$depth, alignmentControl())
  expect_equal(est$status, "abstained")
  expect_match(est$reason, "flat")
})


test_that(".estimateClockOffset(): abstains when the two depth traces do not match", {
  p <- .mkDives()
  set.seed(1)
  noise <- runif(length(p$t), 0, 100)            # unrelated to the dive profile -> low peak correlation
  est <- nautilus:::.estimateClockOffset(p$t, p$depth, p$t, noise, alignmentControl())
  expect_equal(est$status, "abstained")
  expect_match(est$reason, "correlation")
})


test_that(".estimateClockOffset(): abstains when the true offset exceeds the search window", {
  p <- .mkDives()
  est <- nautilus:::.estimateClockOffset(p$t, p$depth, p$t + 300, p$depth,
                                         alignmentControl(max.lag = 100))   # true offset 300 > 100
  expect_equal(est$status, "abstained")
  expect_match(est$reason, "search edge")
})


# write a minimal Wildlife-Computers-style archive CSV (Time, Depth) so .readArchiveDepth() parses it
# with the same locale-dependent month name it was written with (round-trips regardless of locale)
.writeArchive <- function(t, depth, extra_cols = TRUE) {
  f  <- tempfile(fileext = ".csv")
  dt <- as.POSIXct(t, origin = "1970-01-01", tz = "UTC")
  df <- data.frame(Time = format(dt, "%H:%M:%OS %d-%b-%Y"), Depth = depth)
  if (extra_cols) { df$Dry <- 0L; df$Events <- "" }
  utils::write.csv(df, f, row.names = FALSE)
  f
}


test_that(".readArchiveDepth(): reads a WC archive, drops non-finite, sorts; NULL when unusable", {
  p <- .mkDives(dur = 100)
  f <- .writeArchive(p$t, p$depth)
  ad <- nautilus:::.readArchiveDepth(f)
  expect_s3_class(ad, "data.frame")
  expect_named(ad, c("t", "depth"))
  expect_false(is.unsorted(ad$t))
  expect_equal(nrow(ad), length(p$t))
  # missing file / NA path -> NULL
  expect_null(nautilus:::.readArchiveDepth(NA))
  expect_null(nautilus:::.readArchiveDepth(file.path(tempdir(), "does-not-exist.csv")))
  # a CSV without a Depth column -> NULL
  g <- tempfile(fileext = ".csv"); utils::write.csv(data.frame(Time = "x", Dry = 0), g, row.names = FALSE)
  expect_null(nautilus:::.readArchiveDepth(g))
})


test_that(".alignWCClocks(): shifts every WC stream onto the primary clock, leaves the primary untouched", {
  p     <- .mkDives()
  delta <- 240                                            # WC 240 s ahead
  sensor_data <- data.table::data.table(
    datetime = as.POSIXct(p$t, origin = "1970-01-01", tz = "UTC"), depth = p$depth)
  arch <- .writeArchive(p$t + delta, p$depth)             # WC archive depth stamped on the WC clock

  pos_t <- as.POSIXct(p$t[c(500, 1500, 3000)] + delta, origin = "1970-01-01", tz = "UTC")
  positions_anc <- list(source = "test", data = data.frame(
    datetime = pos_t, type = "FastGPS", lon = 1:3, lat = 1:3, quality = "7", stringsAsFactors = FALSE))
  dry_t <- as.POSIXct(p$t[c(100, 2000)] + delta, origin = "1970-01-01", tz = "UTC")
  dry_anc <- list(source = "test", encoding = "transitions",
                  data = data.frame(datetime = dry_t, dry = c(TRUE, FALSE)))

  orig_sensor <- sensor_data$datetime
  aln <- nautilus:::.alignWCClocks(sensor_data, positions_anc, dry_anc, arch, alignmentControl())

  expect_equal(aln$info$status, "aligned")
  expect_lt(abs(aln$info$offset.seconds - (-delta)), 3)   # recovered offset ~ -delta
  expect_equal(aln$info$n_fixes_shifted, 3L)
  # every WC timestamp moved by exactly the recorded offset...
  expect_equal(as.numeric(aln$positions_anc$data$datetime), as.numeric(pos_t) + aln$info$offset.seconds)
  expect_equal(as.numeric(aln$dry_anc$data$datetime),       as.numeric(dry_t) + aln$info$offset.seconds)
  # ...and the aligned fix times now land on the primary timeline (within a few seconds of the true event)
  expect_lt(max(abs(as.numeric(aln$positions_anc$data$datetime) - p$t[c(500, 1500, 3000)])), 3)
  # the primary sensor stream is the reference and is never moved
  expect_identical(sensor_data$datetime, orig_sensor)
})


test_that(".alignWCClocks(): method = 'none' disables alignment and shifts nothing", {
  p <- .mkDives(dur = 200)
  sensor_data <- data.table::data.table(
    datetime = as.POSIXct(p$t, origin = "1970-01-01", tz = "UTC"), depth = p$depth)
  arch <- .writeArchive(p$t + 60, p$depth)
  pos_t <- as.POSIXct(p$t[c(10, 50)] + 60, origin = "1970-01-01", tz = "UTC")
  positions_anc <- list(source = "test", data = data.frame(datetime = pos_t, lon = 1:2, lat = 1:2))
  aln <- nautilus:::.alignWCClocks(sensor_data, positions_anc, NULL, arch, alignmentControl(method = "none"))
  expect_equal(aln$info$status, "disabled")
  expect_equal(aln$info$offset.seconds, 0)
  expect_identical(aln$positions_anc$data$datetime, pos_t)
})


test_that(".alignWCClocks(): abstains (no shift) when the primary tag has no depth channel", {
  p <- .mkDives(dur = 3000)
  sensor_data <- data.table::data.table(
    datetime = as.POSIXct(p$t, origin = "1970-01-01", tz = "UTC"), ax = 0)   # no depth column
  arch <- .writeArchive(p$t, p$depth)
  pos_t <- as.POSIXct(p$t[c(10, 50)], origin = "1970-01-01", tz = "UTC")
  positions_anc <- list(source = "test", data = data.frame(datetime = pos_t, lon = 1:2, lat = 1:2))
  aln <- nautilus:::.alignWCClocks(sensor_data, positions_anc, NULL, arch, alignmentControl())
  expect_equal(aln$info$status, "abstained")
  expect_match(aln$info$reason, "no depth")
  expect_identical(aln$positions_anc$data$datetime, pos_t)
})
