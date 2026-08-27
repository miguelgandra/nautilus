# Tests for calculatePaddleSpeed(): the calibration is resolved once per tag and season, applied as a
# linear rescale of the rotation frequency, and checked against the animal's own diving.

# a swimming animal with a KNOWN speed and a paddle turning at a known rate, so both the applied slope
# and the in-situ estimate have a truth to be checked against
.pwTag <- function(id, pkg, year = 2019, slope = 0.07, speed = 1.2, n = 20000, hz = 20,
                   freq = TRUE, pitch = 40) {
  t0 <- as.POSIXct(paste0(year, "-06-01"), tz = "UTC")
  pit <- rep(c(rep(-pitch, 500), rep(pitch - 5, 500)), length.out = n)
  sp  <- speed + stats::rnorm(n, 0, 0.05)
  d <- data.table::data.table(ID = id, datetime = t0 + seq_len(n) / hz, depth = 20, pitch = pit,
                              vertical_velocity = sp * sin(-pit * pi / 180))
  if (freq) d[, paddle_freq := sp / slope]
  m <- nautilus:::.newNautilusMeta(); m$id <- id
  m$tag$package_id <- pkg; m$tag$paddle_wheel <- TRUE; m$deployment$datetime <- t0
  m$sensors$sampling_hz_original <- hz; m$sensors$sampling_hz_processed <- hz
  nautilus:::new_nautilus_tag(d, m)
}
.pwCal <- function(slope = 0.07, pkg = "71", year = 2019)
  data.frame(year = year, package_id = pkg, slope = slope, stringsAsFactors = FALSE)
.pwRun <- function(...) suppressWarnings(calculatePaddleSpeed(..., verbose = FALSE))


test_that("a measured calibration is applied as slope x frequency and recovers the true speed", {
  set.seed(1)
  out <- .pwRun(list(A = .pwTag("A", "71")), calibration = .pwCal())
  expect_equal(median(out$A$paddle_speed, na.rm = TRUE), 1.2, tolerance = 0.02)
  cal <- attr(out, "calibration")
  expect_identical(cal$slope_source, "measured")
  expect_equal(cal$slope, 0.07)
  # Exactly linear in the retained frequency, which is what makes a recalibration exact and cheap.
  # Checked with smoothing off: the default smooths the frequency first, so the pointwise ratio then
  # reflects the window rather than the slope.
  raw <- .pwRun(list(A = .pwTag("A", "71")), calibration = .pwCal(), smoothing = NULL)
  d <- data.table::as.data.table(raw$A)
  ok <- is.finite(d$paddle_speed) & is.finite(d$paddle_freq)
  expect_equal(unique(round(d$paddle_speed[ok] / d$paddle_freq[ok], 8)), 0.07)
})

test_that("the in-situ estimate recovers the slope from pitch and vertical velocity alone", {
  set.seed(2)
  out <- .pwRun(list(A = .pwTag("A", "71"), B = .pwTag("B", "71")), calibration = .pwCal(),
                validate = TRUE)
  cal <- attr(out, "calibration")
  expect_equal(cal$in_situ_slope, 0.07, tolerance = 0.02)   # the truth, from a channel the paddle never saw
  expect_gt(cal$in_situ_r, 0.95)
  expect_equal(cal$agreement, 1, tolerance = 0.03)
  expect_false(cal$flag)
  expect_equal(cal$n_deployments, 2L)                       # pooled across the tag's deployments
})

test_that("a calibration far from the in-situ estimate is flagged, a close one is not", {
  set.seed(3)
  tags <- list(A = .pwTag("A", "71"), B = .pwTag("B", "71"))
  fl <- function(slope, ...) attr(.pwRun(tags, calibration = .pwCal(slope), validate = TRUE, ...),
                                  "calibration")$flag
  expect_true(fl(0.07 * 1.6)); expect_true(fl(0.07 / 1.6))
  expect_false(fl(0.07 * 1.1))

  # the band is multiplicative, so the same proportional error flags either way round
  expect_identical(fl(0.07 * 1.5, agreement.threshold = 0.6), FALSE)
  expect_identical(fl(0.07 / 1.5, agreement.threshold = 0.6), FALSE)
  expect_identical(fl(0.07 * 1.5, agreement.threshold = 0.2), TRUE)
  expect_identical(fl(0.07 / 1.5, agreement.threshold = 0.2), TRUE)
  expect_error(.pwRun(tags, calibration = .pwCal(), agreement.threshold = 0), "greater than zero")
})

test_that("validation is off by default: the job is to calculate speed, not to audit it", {
  set.seed(31)
  tags <- list(A = .pwTag("A", "71"), B = .pwTag("B", "71"))
  plain <- attr(.pwRun(tags, calibration = .pwCal(0.07 * 1.6)), "calibration")
  expect_true(all(is.na(plain$in_situ_slope)))         # not computed
  expect_true(all(is.na(plain$agreement)))
  expect_false(any(plain$flag))                        # and nothing flagged on an absent comparison
  expect_false(all(is.na(.pwRun(tags, calibration = .pwCal())$A$paddle_speed)))   # speed still produced
  asked <- attr(.pwRun(tags, calibration = .pwCal(0.07 * 1.6), validate = TRUE), "calibration")
  expect_true(is.finite(asked$in_situ_slope))
  expect_true(asked$flag)
})

test_that("method = 'in-situ' fills a gap but never overrides a measured value", {
  set.seed(4)
  tags <- list(A = .pwTag("A", "71"), C = .pwTag("C", "99", slope = 0.11, speed = 1.4))
  out <- .pwRun(tags, calibration = .pwCal(), method = "in-situ")
  cal <- attr(out, "calibration")
  expect_identical(cal$slope_source[cal$package_id == "71"], "measured")
  expect_equal(cal$slope[cal$package_id == "71"], 0.07)      # untouched
  expect_identical(cal$slope_source[cal$package_id == "99"], "in-situ")
  expect_equal(cal$slope[cal$package_id == "99"], 0.11, tolerance = 0.03)
  expect_equal(median(out$C$paddle_speed, na.rm = TRUE), 1.4, tolerance = 0.05)
})

test_that("a logger that recorded speed itself is left alone, and a tag without a paddle gets NA", {
  set.seed(5)
  d <- .pwTag("D", "71", freq = FALSE); dd <- data.table::as.data.table(d)
  dd[, paddle_speed := 1.1]; d <- nautilus:::.restoreMeta(dd, nautilus:::.getMeta(d))
  e <- .pwTag("E", "71", freq = FALSE)
  me <- nautilus:::.getMeta(e); me$tag$paddle_wheel <- FALSE
  e <- nautilus:::.restoreMeta(e, me)

  out <- .pwRun(list(D = d, E = e), calibration = .pwCal())
  expect_true(all(out$D$paddle_speed == 1.1))                # no frequency to calibrate: kept verbatim
  expect_true(all(is.na(out$E$paddle_speed)))
  pr <- Filter(function(r) identical(r$step, "calculatePaddleSpeed"),
               nautilus:::.getMeta(out$D)$processing)
  expect_identical(pr[[length(pr)]]$status, "as-recorded")
})

test_that("the provenance travels with each deployment, so it survives a save and reload", {
  set.seed(6)
  out <- .pwRun(list(A = .pwTag("A", "71")), calibration = .pwCal(), validate = TRUE)
  f <- tempfile(fileext = ".rds"); on.exit(unlink(f), add = TRUE)
  saveRDS(out$A, f)
  pr <- Filter(function(r) identical(r$step, "calculatePaddleSpeed"),
               nautilus:::.getMeta(readRDS(f))$processing)
  p <- pr[[length(pr)]]
  expect_equal(p$slope, 0.07)
  expect_identical(p$slope_source, "measured")
  expect_equal(p$in_situ_slope, 0.07, tolerance = 0.02)
})

test_that("smoothing and the speed cap behave, and neither is applied upstream any more", {
  set.seed(7)
  tags <- list(A = .pwTag("A", "71"))
  # the cap is a speed-domain rule, so it belongs with the conversion
  out <- .pwRun(tags, calibration = .pwCal(), max.speed = 1)      # 1 km/h = 0.28 m/s
  expect_true(all(is.na(out$A$paddle_speed)))
  # without the cap nothing is discarded for being too fast (the centred smoother still leaves the
  # first and last half-window NA, which is not the cap's doing)
  free <- .pwRun(tags, calibration = .pwCal(), max.speed = NULL, smoothing = NULL)$A$paddle_speed
  expect_false(any(is.na(free)))
  # smoothing is a plain argument here; smoothingControl() no longer carries a paddle window
  expect_null(smoothingControl()$speed)
  expect_equal(smoothingControl()$vertical, 1)
  expect_s3_class(.pwRun(tags, calibration = .pwCal(), smoothing = NULL)$A, "nautilus_tag")
})

test_that("inputs are validated, and a missing calibration is refused rather than guessed", {
  set.seed(8)
  tags <- list(A = .pwTag("A", "71"))
  expect_error(calculatePaddleSpeed(tags, verbose = FALSE), "nothing to fill the gaps")
  expect_error(calculatePaddleSpeed(tags, calibration = data.frame(year = 2019), verbose = FALSE),
               "missing the column")
  expect_error(calculatePaddleSpeed(tags, calibration = .pwCal(), min.pitch = 95, verbose = FALSE),
               "below 90")
  expect_error(calculatePaddleSpeed(tags, calibration = .pwCal(), max.speed = -1, verbose = FALSE))
  # with no calibration at all, in-situ is a complete route
  out <- .pwRun(tags, method = "in-situ")
  expect_equal(attr(out, "calibration")$slope, 0.07, tolerance = 0.03)
})

test_that("processTagData no longer applies a calibration, and reconstructTrack says so", {
  expect_false("paddle.calibration" %in% names(formals(processTagData)))
  expect_true("calibration" %in% names(formals(calculatePaddleSpeed)))
  # asking for paddle speed when the step was never run is a missing step, not an empty result
  set.seed(9)
  d <- .pwTag("A", "71", freq = FALSE)
  expect_error(suppressWarnings(reconstructTrack(list(A = d), verbose = FALSE,
                 control = reconstructTrackControl(speed.method = "paddle"))),
               "paddle_speed", ignore.case = TRUE)
})
