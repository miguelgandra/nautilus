# Tests for calculatePaddleSpeed(): the calibration is resolved once per tag and season, applied as a
# linear rescale of the rotation frequency, and checked against the animal's own diving.

# a swimming animal with a KNOWN speed and a paddle turning at a known rate, so both the applied slope
# and the in-situ estimate have a truth to be checked against
.pwTag <- function(id, pkg, year = 2019, slope = 0.07, speed = 1.2, n = 20000, hz = 20,
                   freq = TRUE, pitch = 40, as.recorded = FALSE, paddle = TRUE) {
  t0 <- as.POSIXct(paste0(year, "-06-01"), tz = "UTC")
  pit <- rep(c(rep(-pitch, 500), rep(pitch - 5, 500)), length.out = n)
  sp  <- speed + stats::rnorm(n, 0, 0.05)
  d <- data.table::data.table(ID = id, datetime = t0 + seq_len(n) / hz, depth = 20, pitch = pit,
                              vertical_velocity = sp * sin(-pit * pi / 180))
  if (freq) d[, paddle_freq := sp / slope]
  if (as.recorded) d[, paddle_speed := sp]      # a logger that wrote a speed instead of a rotation rate
  m <- nautilus:::.newNautilusMeta(); m$id <- id
  m$tag$package_id <- pkg; m$tag$paddle_wheel <- paddle; m$deployment$datetime <- t0
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


# ---- a slope is only estimated where one can actually be used --------------------------------------
# Estimating a slope for a tag that never recorded a rotation rate manufactures a number that is
# reported and then never applied. Same defect as imputing for a tag with no paddle, different route in.

test_that("a tag that recorded speed directly gets no estimated slope, and is labelled as such", {
  set.seed(30)
  logger <- .pwTag("LOGGER", "71", 2020, n = 2000, freq = FALSE, as.recorded = TRUE)
  before <- as.numeric(logger$paddle_speed)        # captured up front: the fixture is random
  out <- .pwRun(list(A = .pwTag("HAS_FREQ", "51", 2019, n = 2000), B = logger),
                calibration = .pwCal(0.0736, "51", 2019))
  cal <- attr(out, "calibration")
  r <- cal[cal$package_id == "71", ]
  expect_true(is.na(r$slope))                      # nothing to convert, so nothing is invented
  expect_identical(r$slope_source, "as-recorded")
  expect_equal(as.numeric(out$LOGGER$paddle_speed), before)   # recorded speed passed through untouched
})


test_that("a tag with a paddle but no recorded data gets no estimated slope either", {
  set.seed(31)
  out <- .pwRun(list(A = .pwTag("HAS_FREQ", "51", 2019, n = 2000),
                     B = .pwTag("EMPTY", "91", 2019, n = 2000, freq = FALSE, paddle = TRUE)),
                calibration = .pwCal(0.0736, "51", 2019))
  cal <- attr(out, "calibration")
  r <- cal[cal$package_id == "91", ]
  expect_true(is.na(r$slope))
  expect_true(is.na(r$slope_source))               # not "as-recorded": nothing was recorded at all
})


test_that("a tag that DOES carry a rotation rate still has its gap filled", {
  set.seed(32)
  out <- .pwRun(list(A = .pwTag("HAS_FREQ", "51", 2019, n = 2000),
                     B = .pwTag("NEEDS_FILL", "52", 2020, n = 2000, slope = 0.104)),
                calibration = .pwCal(0.0736, "51", 2019))
  cal <- attr(out, "calibration")
  r <- cal[cal$package_id == "52", ]
  expect_true(is.finite(r$slope))                  # the gate must not suppress a real gap
  expect_false(is.na(r$slope_source))
  expect_true(all(is.finite(range(out$NEEDS_FILL$paddle_speed, na.rm = TRUE))))
})


test_that("a measured calibration is kept even where no deployment needs it", {
  set.seed(33)
  out <- .pwRun(list(A = .pwTag("HAS_FREQ", "51", 2019, n = 2000),
                     B = .pwTag("LOGGER", "71", 2020, n = 2000, freq = FALSE, as.recorded = TRUE)),
                calibration = rbind(.pwCal(0.0736, "51", 2019), .pwCal(0.0649, "71", 2020)))
  cal <- attr(out, "calibration")
  r <- cal[cal$package_id == "71", ]
  # a real drop-test result is an observation, not a guess, so it is reported rather than blanked
  expect_equal(r$slope, 0.0649)
  expect_identical(r$slope_source, "measured")
})


test_that("the internal bookkeeping columns stay out of the returned calibration", {
  set.seed(34)
  cal <- attr(.pwRun(list(A = .pwTag("A", "51", 2019, n = 2000)),
                     calibration = .pwCal(0.0736, "51", 2019)), "calibration")
  expect_false(any(c("key", "has_paddle", "needs_slope", "as_recorded") %in% names(cal)))
})



# ---- verbose output ------------------------------------------------------------------------------
# The layout follows calculateTailBeats(), which is the package's reference for a per-deployment
# workflow function: run settings in the header, one block per deployment, and every cohort-level
# result in the SUMMARY. These lock that split in place, because the natural drift is for a result
# that happens to be known early (the calibration) to creep back up into the header.

# one deployment of each outcome, small enough to log quickly
.pwCohort <- function() list(
  A = .pwTag("PIN_02", "51", 2019, n = 2000),
  B = .pwTag("PIN_08", "52", 2020, slope = 0.104, n = 2000),
  C = .pwTag("PIN_CAM_30", "71", 2020, n = 2000, freq = FALSE, as.recorded = TRUE),
  D = .pwTag("PIN_04", "91", 2019, n = 2000, freq = FALSE, paddle = FALSE))
.pwCal3 <- function() data.frame(year = c(2019, 2020), package_id = c("51", "51"),
                                 slope = c(0.0736, 0.0809), stringsAsFactors = FALSE)
.pwLog <- function(..., verbose = 2) paste(cli::cli_fmt(suppressWarnings(
  calculatePaddleSpeed(..., verbose = verbose))), collapse = "\n")

# One region of the log, sliced by the lines that open and close it. Done by line rather than with a
# regex because R's `.` does not cross newlines, so a `sub(".*SUMMARY", "", out)` silently trims
# nothing and every "this is not in the header" assertion would pass against the whole output.
.pwBetween <- function(out, from = NULL, to = NULL) {
  ln <- strsplit(out, "\n", fixed = TRUE)[[1]]
  i  <- if (is.null(from)) 1L else grep(from, ln)[1]
  j  <- if (is.null(to)) length(ln) else grep(to, ln)[1] - 1L
  if (is.na(i)) i <- 1L
  if (is.na(j) || j < i) j <- length(ln)
  paste(ln[seq.int(i, j)], collapse = "\n")
}


test_that("the header carries the run settings, and not the calibration it goes on to resolve", {
  set.seed(11)
  out <- .pwLog(.pwCohort(), calibration = .pwCal3(), validate = TRUE, smoothing = 2, max.speed = 8)
  expect_match(out, "Input: 4 deployments")
  expect_match(out, "Calibration: shared-rate")
  expect_match(out, "Smoothing: 2 s on the rotation frequency")
  expect_match(out, "Speed cap: 8 km/h")
  expect_match(out, "Validation: in situ")

  # the resolved slopes belong to the SUMMARY: nothing above the closing frame may quote one
  # The resolved slopes belong to the SUMMARY, so nothing above the first deployment block may quote
  # one. Tested on marks only a RESULT can produce: the settings line legitimately contains the word
  # "measured" while describing where gap-filling gets its slopes from.
  hdr <- .pwBetween(out, to = "PIN_02")
  expect_false(grepl("0.0736", hdr, fixed = TRUE))    # a resolved slope
  expect_false(grepl("pkg ", hdr, fixed = TRUE))      # the table's Tag column
  expect_false(grepl("Agreement", hdr, fixed = TRUE)) # ... and its in-situ columns
  expect_false(grepl("In situ", hdr, fixed = TRUE))
})


test_that("settings that are switched off say so, rather than going unmentioned", {
  set.seed(12)
  out <- .pwLog(.pwCohort(), calibration = .pwCal3(), smoothing = NULL, max.speed = NULL)
  expect_match(out, "Smoothing: none")
  expect_match(out, "Speed cap: none")
  expect_match(out, "Validation: off")
})


test_that("each deployment gets a block naming the slope applied, its source, and the speed it gave", {
  set.seed(13)
  out <- .pwLog(.pwCohort(), calibration = .pwCal3())
  expect_match(out, "PIN_02 \\(1/4\\)")
  expect_match(out, "PIN_04 \\(4/4\\)")               # the counter runs over the whole cohort
  expect_match(out, "input: 2 K rows .* 20 Hz .* package 51 .* 2019")
  expect_match(out, "calibration: 0.0736 m/s per Hz .* measured")
  expect_match(out, "speed: +median [0-9.]+ m/s \\([0-9.]+ .* [0-9.]+\\)")
  expect_match(out, "PIN_02 processed")

  # a tag that recorded speed itself has no slope to report, and must not borrow the cohort's
  block <- .pwBetween(out, from = "PIN_CAM_30", to = "PIN_04")
  expect_match(block, "calibration: not needed")
  expect_match(block, "speed recorded by the logger")
  expect_false(grepl("m/s per Hz", block, fixed = TRUE))
})


test_that("a deployment that gets no speed prints the skip line alone", {
  set.seed(14)
  out <- .pwLog(.pwCohort(), calibration = .pwCal3())
  block <- .pwBetween(out, from = "PIN_04", to = "SUMMARY")
  expect_match(block, "no paddle wheel")
  expect_match(block, "skipped")
  expect_false(grepl("input:", block, fixed = TRUE))     # nothing to describe, so nothing is described
  expect_false(grepl("speed:", block, fixed = TRUE))

  # a tag that HAS a paddle but recorded nothing is a different fault, and is named differently
  set.seed(15)
  out2 <- .pwLog(list(Z = .pwTag("PIN_11", "91", 2019, n = 2000, freq = FALSE, paddle = TRUE)),
                 calibration = .pwCal3())
  expect_match(out2, "no paddle data")
  expect_false(grepl("no paddle wheel", out2, fixed = TRUE))
})


test_that("the per-deployment tick survives at normal verbosity, without the block or its details", {
  set.seed(16)
  out <- .pwLog(.pwCohort(), calibration = .pwCal3(), verbose = 1)
  expect_match(out, "PIN_02 processed")
  expect_match(out, "no paddle wheel")
  expect_false(grepl("PIN_02 (1/4)", out, fixed = TRUE))  # no block rule
  expect_false(grepl("input:", out, fixed = TRUE))        # no detail lines
})


test_that("the SUMMARY tally is mutually exclusive and adds up to the cohort", {
  set.seed(17)
  out <- .pwLog(.pwCohort(), calibration = .pwCal3())
  expect_match(out, "SUMMARY")
  # two of the four get a speed: one calculated from a slope, one recorded by the logger
  expect_match(out, "3 of 4 deployments given a speed")
  expect_match(out, "Speed calculated: +2")
  expect_match(out, "Speed as recorded: +1")
  expect_match(out, "No paddle wheel: +1")
  # the headline counts the as-recorded deployment; the tally must not count it twice
  n <- as.integer(regmatches(out, regexpr("(?<=Speed calculated: {0,20})[0-9]+", out, perl = TRUE)))
  expect_identical(n, 2L)
})


test_that("the SUMMARY carries the calibration table, one row per tag and season", {
  set.seed(18)
  out <- .pwLog(.pwCohort(), calibration = .pwCal3(), validate = TRUE)
  tail <- .pwBetween(out, from = "SUMMARY")
  expect_match(tail, "Calibration")
  expect_match(tail, "Tag .*Slope .*Source .*In situ .*Agreement .*Deployments")
  expect_match(tail, "2019 / pkg 51 +0.0736 +measured")
  expect_match(tail, "2020 / pkg 52")
})


test_that("the in-situ columns appear only when there is an in-situ estimate to show", {
  set.seed(19)
  out <- .pwLog(.pwCohort(), calibration = .pwCal3())          # validate = FALSE
  tail <- .pwBetween(out, from = "SUMMARY")
  expect_match(tail, "Tag .*Slope .*Source .*Deployments")
  expect_false(grepl("In situ", tail, fixed = TRUE))           # no column of dashes
  expect_false(grepl("Agreement", tail, fixed = TRUE))
})


test_that("a flagged calibration is marked in its own row and named once in the attention line", {
  set.seed(20)
  tags <- list(A = .pwTag("PIN_02", "51", 2019, n = 4000))
  out <- .pwLog(tags, calibration = .pwCal(0.07 * 1.8, "51", 2019), validate = TRUE)
  tail <- .pwBetween(out, from = "SUMMARY")
  expect_match(tail, "2019 / pkg 51.*!")                       # the marker rides the agreement column
  expect_match(tail, "1 calibration differs by more than 35% from the in-situ estimate")
  expect_match(tail, "2019/pkg 51")

  # a calibration that agrees is neither marked nor mentioned
  set.seed(21)
  ok <- .pwBetween(.pwLog(tags, calibration = .pwCal(0.07, "51", 2019), validate = TRUE),
                   from = "SUMMARY")
  expect_false(grepl("!", ok, fixed = TRUE))
  expect_false(grepl("differs by more than", ok, fixed = TRUE))
})


test_that("the threshold quoted in the attention line tracks agreement.threshold", {
  set.seed(22)
  tags <- list(A = .pwTag("PIN_02", "51", 2019, n = 4000))
  out <- .pwLog(tags, calibration = .pwCal(0.07 * 1.4, "51", 2019), validate = TRUE,
                agreement.threshold = 0.2)
  expect_match(out, "more than 20% from the in-situ estimate")
})


test_that("the output pointers are listed only when something was written", {
  set.seed(23)
  dir <- withr::local_tempdir()
  pdf <- file.path(dir, "cal.pdf")
  out <- .pwLog(.pwCohort(), calibration = .pwCal3(), output.dir = dir, plot.file = pdf)
  expect_match(.pwBetween(out, from = "SUMMARY"), "Output")
  expect_match(out, "Directory:")
  expect_match(out, "Plots:")

  set.seed(24)
  bare <- .pwLog(.pwCohort(), calibration = .pwCal3())
  expect_false(grepl("Directory:", bare, fixed = TRUE))
  expect_false(grepl("Plots:", bare, fixed = TRUE))
})


test_that(".paddleSourceLabel gives a prose form for a line and a short form for a table column", {
  lbl <- nautilus:::.paddleSourceLabel
  expect_identical(lbl("measured"), "measured")
  expect_identical(lbl("tag-model"), "estimated (tag model)")
  expect_identical(lbl("tag-model", long = FALSE), "tag model")
  expect_identical(lbl("baseline"), "estimated (baseline)")
  expect_identical(lbl("baseline", long = FALSE), "baseline")
  expect_identical(lbl("in-situ", long = FALSE), "in situ")
  expect_identical(lbl("as-recorded", long = FALSE), "as recorded")
  expect_true(is.na(lbl(NA_character_)))
  expect_identical(lbl("something-new"), "estimated")      # an unknown source still reads sensibly
})


test_that("nothing is printed at all when verbose is off", {
  set.seed(25)
  expect_length(cli::cli_fmt(suppressWarnings(
    calculatePaddleSpeed(.pwCohort(), calibration = .pwCal3(), verbose = 0))), 0L)
})


# ---- the wear rate -----------------------------------------------------------------------------
# A missing slope is projected forward at an annual wear rate. "fixed-rate" takes that rate from the
# caller instead of estimating it, so it cannot run without one; the other methods fall back to it only
# when the calibrations are too sparse to estimate a rate of their own.

test_that("'fixed-rate' projects a missing slope at exactly the rate supplied", {
  set.seed(40)
  out <- .pwRun(list(A = .pwTag("A", "51", 2022, n = 2000)),
                calibration = .pwCal(0.074, "51", 2019), method = "fixed-rate",
                degradation.rate = 0.01)
  cal <- attr(out, "calibration")
  expect_equal(cal$slope, 0.074 + 3 * 0.01)        # three years of wear at the stated rate
  expect_identical(cal$slope_source, "tag-model")
})


test_that("'fixed-rate' aborts when no rate is supplied, naming the argument that is missing", {
  set.seed(41)
  expect_error(
    calculatePaddleSpeed(list(A = .pwTag("A", "51", 2022, n = 2000)),
                         calibration = .pwCal(0.074, "51", 2019), method = "fixed-rate", verbose = 0),
    "degradation.rate")
})


test_that("the rate is a fallback for the estimating methods when no trend can be fitted", {
  # a single calibration for the tag, so there is no repeat series to estimate a rate from
  set.seed(42)
  tags <- list(A = .pwTag("A", "51", 2022, n = 2000))
  slope <- function(rate) attr(.pwRun(tags, calibration = .pwCal(0.074, "51", 2019),
                                      degradation.rate = rate), "calibration")$slope
  expect_equal(slope(0.02), 0.074 + 3 * 0.02)
  expect_equal(slope(0.05), 0.074 + 3 * 0.05)      # the supplied rate is really what drives it
})


test_that("a degradation.rate that is not a number is rejected", {
  set.seed(43)
  expect_error(calculatePaddleSpeed(list(A = .pwTag("A", "51", 2022, n = 2000)),
                                    calibration = .pwCal(0.074, "51", 2019),
                                    degradation.rate = "fast", verbose = 0), "degradation.rate")
})


test_that("the wear rate is reported on its own header line, and only when supplied", {
  set.seed(44)
  tags <- list(A = .pwTag("A", "51", 2022, n = 2000))
  cal <- .pwCal(0.074, "51", 2019)
  expect_match(.pwLog(tags, calibration = cal, method = "fixed-rate", degradation.rate = 0.01),
               "Wear rate: 0.01 per year")
  expect_false(grepl("Wear rate", .pwLog(tags, calibration = cal), fixed = TRUE))
})


test_that("the wear rate travels in each deployment's processing record", {
  set.seed(45)
  out <- .pwRun(list(A = .pwTag("A", "51", 2022, n = 2000)),
                calibration = .pwCal(0.074, "51", 2019), method = "fixed-rate",
                degradation.rate = 0.01)
  pr <- nautilus:::.getMeta(out$A)$processing
  expect_equal(pr[[length(pr)]]$degradation_rate, 0.01)
  expect_identical(pr[[length(pr)]]$method, "fixed-rate")
})
