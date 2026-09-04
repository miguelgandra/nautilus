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
  expect_identical(cal$slope_source, "calibrated")
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

test_that("method = 'in-situ-pooled' fills a gap but never overrides a calibratedd value", {
  set.seed(4)
  tags <- list(A = .pwTag("A", "71"), C = .pwTag("C", "99", slope = 0.11, speed = 1.4))
  out <- .pwRun(tags, calibration = .pwCal(), method = "in-situ-pooled")
  cal <- attr(out, "calibration")
  expect_identical(cal$slope_source[cal$package_id == "71"], "calibrated")
  expect_equal(cal$slope[cal$package_id == "71"], 0.07)      # untouched
  expect_identical(cal$slope_source[cal$package_id == "99"], "in-situ-pooled")
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
  expect_identical(p$slope_source, "calibrated")
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
  default_free <- .pwRun(tags, calibration = .pwCal(), smoothing = NULL)$A$paddle_speed
  expect_equal(default_free, free)                              # no silent censoring by default
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
  out <- .pwRun(tags, method = "in-situ-pooled")
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
  expect_identical(r$slope_source, "calibrated")
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
  expect_match(out, "Slope estimation: projected-shared")
  expect_match(out, "Smoothing: 2 s on the rotation frequency")
  expect_match(out, "Speed cap: 8 km/h")
  expect_match(out, "Validation: from pitch and vertical velocity")

  # the resolved slopes belong to the SUMMARY: nothing above the closing frame may quote one
  # The resolved slopes belong to the SUMMARY, so nothing above the first deployment block may quote
  # one. Tested on marks only a RESULT can produce: the settings line legitimately contains the word
  # "calibrated" while describing where gap-filling gets its slopes from.
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
  expect_match(out, "slope: 0.0736 m/s per Hz \\(calibrated\\)")
  expect_match(out, "speed: median [0-9.]+ m/s \\([0-9.]+.[0-9.]+\\)")
  expect_match(out, "PIN_02 processed")

  # a tag that recorded speed itself has no slope to report, and must not borrow the cohort's
  block <- .pwBetween(out, from = "PIN_CAM_30", to = "PIN_04")
  expect_match(block, "slope: not needed")
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
  expect_match(out, "speed calculated: +2")
  expect_match(out, "speed as recorded: +1")
  expect_match(out, "no paddle wheel: +1")
  # The headline counts the as-recorded deployment; the tally must not count it twice. Read off the
  # line with a capture group rather than a lookbehind: a variable-length lookbehind needs a recent
  # PCRE2 and is rejected outright by older ones.
  ln <- grep("speed calculated", strsplit(out, "\n", fixed = TRUE)[[1]], value = TRUE)[1]
  expect_identical(as.integer(sub(".*speed calculated:[^0-9]*([0-9]+).*", "\\1", ln)), 2L)
})


test_that("the SUMMARY summarises the calibration instead of printing the whole table", {
  set.seed(18)
  out <- .pwLog(.pwCohort(), calibration = .pwCal3(), validate = TRUE)
  tail <- .pwBetween(out, from = "SUMMARY")
  expect_match(tail, "Slope estimation")                       # not "Calibration": the method, named
  expect_match(tail, "tag-seasons:")
  expect_match(tail, "calibrated")                             # provenance counted, not tabulated
  expect_match(tail, "slopes applied:")
  expect_match(tail, "attr\\(x, \"calibration\"\\)")             # where the full table lives

  # the per-tag-season table is gone from the console: no header row, no one-row-per-stratum listing
  expect_false(grepl("Tag  ", tail, fixed = TRUE))
  expect_false(grepl("2019 / pkg 51", tail, fixed = TRUE))
})


test_that("the cohort roll-ups follow the calculateTailBeats form", {
  set.seed(19)
  out <- .pwLog(.pwCohort(), calibration = .pwCal3(), validate = TRUE)
  tail <- .pwBetween(out, from = "SUMMARY")
  expect_match(tail, "speed: +median [0-9.]+ m/s \\(IQR [0-9.]+.[0-9.]+, range")
  expect_match(tail, "agreement: +median [0-9.]+ \\(IQR")
  expect_match(tail, "steep swimming: +median [0-9.]+%")

  # without validation there is no in-situ fit, so those two roll-ups have nothing to report
  set.seed(20)
  bare <- .pwBetween(.pwLog(.pwCohort(), calibration = .pwCal3()), from = "SUMMARY")
  expect_match(bare, "speed: +median")
  expect_false(grepl("agreement:", bare, fixed = TRUE))
  expect_false(grepl("steep swimming:", bare, fixed = TRUE))
})


test_that("a flagged calibration is named once in the attention line", {
  set.seed(21)
  tags <- list(A = .pwTag("PIN_02", "51", 2019, n = 4000))
  out <- .pwLog(tags, calibration = .pwCal(0.07 * 1.8, "51", 2019), validate = TRUE)
  tail <- .pwBetween(out, from = "SUMMARY")
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
  expect_match(out, "directory:")
  expect_match(out, "plots:")

  set.seed(24)
  bare <- .pwLog(.pwCohort(), calibration = .pwCal3())
  expect_false(grepl("directory:", bare, fixed = TRUE))
  expect_false(grepl("plots:", bare, fixed = TRUE))
})


test_that(".paddleSourceLabel gives a prose form for a line and a short form for a table column", {
  lbl <- nautilus:::.paddleSourceLabel
  expect_identical(lbl("calibrated"), "calibrated")
  expect_identical(lbl("projected-from-tag"), "projected from tag")
  expect_identical(lbl("projected-from-fleet"), "projected from fleet")
  expect_identical(lbl("in-situ-deployment"), "in situ, this deployment")
  expect_identical(lbl("in-situ-deployment", long = FALSE), "in situ, deployment")
  expect_identical(lbl("in-situ-pooled", long = FALSE), "in situ, pooled")
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
# A missing slope is projected forward at an annual wear rate. "projected-fixed" takes that rate from the
# caller instead of estimating it, so it cannot run without one; the other methods fall back to it only
# when the calibrations are too sparse to estimate a rate of their own.

test_that("'fixed-rate' projects a missing slope at exactly the rate supplied", {
  set.seed(40)
  out <- .pwRun(list(A = .pwTag("A", "51", 2022, n = 2000)),
                calibration = .pwCal(0.074, "51", 2019), method = "projected-fixed",
                degradation.rate = 0.01)
  cal <- attr(out, "calibration")
  expect_equal(cal$slope, 0.074 + 3 * 0.01)        # three years of wear at the stated rate
  expect_identical(cal$slope_source, "projected-from-tag")
})


test_that("'fixed-rate' aborts when no rate is supplied, naming the argument that is missing", {
  set.seed(41)
  expect_error(
    calculatePaddleSpeed(list(A = .pwTag("A", "51", 2022, n = 2000)),
                         calibration = .pwCal(0.074, "51", 2019), method = "projected-fixed", verbose = 0),
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
  expect_match(.pwLog(tags, calibration = cal, method = "projected-fixed", degradation.rate = 0.01),
               "Wear rate: 0.01 per year")
  expect_false(grepl("Wear rate", .pwLog(tags, calibration = cal), fixed = TRUE))
})


test_that("the wear rate travels in each deployment's processing record", {
  set.seed(45)
  out <- .pwRun(list(A = .pwTag("A", "51", 2022, n = 2000)),
                calibration = .pwCal(0.074, "51", 2019), method = "projected-fixed",
                degradation.rate = 0.01)
  pr <- nautilus:::.getMeta(out$A)$processing
  expect_equal(pr[[length(pr)]]$degradation_rate, 0.01)
  expect_identical(pr[[length(pr)]]$method, "projected-fixed")
})


# ---- estimation scope: in-situ-deployment vs in-situ-pooled ----------------------------------------
# The two methods run one estimator over different data. Pooling weights each deployment by its own
# Sxx, so these tests check WHICH data the slope came from, not the arithmetic of the fit.

# a deployment whose steep swimming is confined to one burst, so its usable duration is controllable
.pwBurst <- function(id, pkg, year = 2019, slope = 0.07, speed = 1.2, n = 40000, hz = 20,
                     steep_n = 20000L) {
  t0 <- as.POSIXct(paste0(year, "-06-01"), tz = "UTC")
  pit <- c(rep(-40, steep_n), rep(-2, n - steep_n))       # 2 deg is below any sane min.pitch
  sp  <- speed + stats::rnorm(n, 0, 0.02)
  d <- data.table::data.table(ID = id, datetime = t0 + seq_len(n) / hz, depth = 20, pitch = pit,
                              vertical_velocity = sp * sin(-pit * pi / 180),
                              paddle_freq = sp / slope)
  m <- nautilus:::.newNautilusMeta(); m$id <- id
  m$tag$package_id <- pkg; m$tag$paddle_wheel <- TRUE; m$deployment$datetime <- t0
  nautilus:::new_nautilus_tag(d, m)
}


test_that("in-situ-pooled gives one slope per tag-season, in-situ-deployment one per deployment", {
  set.seed(30)
  tags <- list(A = .pwTag("D1", "77", 2019, slope = 0.06, n = 20000),
               B = .pwTag("D2", "77", 2019, slope = 0.09, n = 20000))

  pooled <- .pwRun(tags, method = "in-situ-pooled", smoothing = NULL)
  s1 <- unique(round(pooled$D1$paddle_speed / data.table::as.data.table(pooled$D1)$paddle_freq, 8))
  s2 <- unique(round(pooled$D2$paddle_speed / data.table::as.data.table(pooled$D2)$paddle_freq, 8))
  expect_equal(s1[is.finite(s1)], s2[is.finite(s2)])       # same slope applied to both

  perdep <- .pwRun(tags, method = "in-situ-deployment", smoothing = NULL)
  p1 <- unique(round(perdep$D1$paddle_speed / data.table::as.data.table(perdep$D1)$paddle_freq, 8))
  p2 <- unique(round(perdep$D2$paddle_speed / data.table::as.data.table(perdep$D2)$paddle_freq, 8))
  expect_false(isTRUE(all.equal(p1[is.finite(p1)], p2[is.finite(p2)])))

  # each recovers its OWN truth, which pooling necessarily cannot
  expect_equal(p1[is.finite(p1)], 0.06, tolerance = 0.05)
  expect_equal(p2[is.finite(p2)], 0.09, tolerance = 0.05)
})


test_that("neither in-situ method displaces a calibration that exists", {
  set.seed(31)
  tags <- list(A = .pwTag("D1", "77", 2019, n = 20000))
  for (m in c("in-situ-deployment", "in-situ-pooled")) {
    cal <- attr(.pwRun(tags, calibration = .pwCal(0.0123, "77", 2019), method = m), "calibration")
    expect_identical(cal$slope_source, "calibrated")
    expect_equal(cal$slope, 0.0123)
  }
})


test_that("a deployment with too little steep swimming falls back to the pooled fit, and says so", {
  set.seed(32)
  # 400 samples at 20 Hz = 20 s, below the 60 s floor; its stratum-mate has plenty
  tags <- list(A = .pwBurst("SHORT", "78", 2019, steep_n = 400L),
               B = .pwBurst("LONG",  "78", 2019, steep_n = 20000L))
  out <- .pwRun(tags, method = "in-situ-deployment")
  rec <- function(x) { p <- nautilus:::.getMeta(x)$processing; p[[length(p)]] }
  expect_identical(rec(out$SHORT)$slope_source, "in-situ-pooled")     # fell back
  expect_identical(rec(out$LONG)$slope_source, "in-situ-deployment")  # used its own

  # the floor is what did it: the short deployment's own fit exists, it is just not viable
  expect_true(is.finite(rec(out$SHORT)$in_situ_slope))
  expect_lt(rec(out$SHORT)$in_situ_seconds, 60)
  expect_gt(rec(out$LONG)$in_situ_seconds, 60)
})


test_that("agreement is withheld wherever the comparison would be circular", {
  set.seed(33)
  tags <- list(A = .pwTag("D1", "79", 2019, n = 20000), B = .pwTag("D2", "79", 2019, n = 20000))
  rec <- function(x) { p <- nautilus:::.getMeta(x)$processing; p[[length(p)]] }

  # applied from this deployment's own fit -> circular
  d <- .pwRun(tags, method = "in-situ-deployment")
  expect_true(is.na(rec(d$D1)$agreement))

  # applied from the tag-season pool, with another deployment in it -> a real heterogeneity check
  p <- .pwRun(tags, method = "in-situ-pooled")
  expect_true(is.finite(rec(p$D1)$agreement))

  # a pooled fit with a single contributor is that contributor's own fit -> circular again
  solo <- .pwRun(list(A = .pwTag("D1", "80", 2019, n = 20000)), method = "in-situ-pooled")
  expect_true(is.na(rec(solo$D1)$agreement))

  # a calibrated slope is always independent of the animal's diving
  cal <- .pwRun(tags, calibration = .pwCal(0.07, "79", 2019), validate = TRUE)
  expect_true(is.finite(rec(cal$D1)$agreement))
})


test_that("the calibration table carries the between-deployment spread, unflagged", {
  set.seed(34)
  tags <- list(A = .pwTag("D1", "81", 2019, slope = 0.06, n = 20000),
               B = .pwTag("D2", "81", 2019, slope = 0.09, n = 20000))
  cal <- attr(.pwRun(tags, calibration = .pwCal(0.07, "81", 2019), validate = TRUE), "calibration")
  expect_identical(cal$slope_k, 2L)
  expect_gt(cal$slope_cv, 0.1)                      # 0.06 vs 0.09 is a wide spread
  expect_gt(cal$slope_ratio, 1.3)
  expect_true(is.finite(cal$slope_rel_se))
  # the spread carries no flag of its own: `flag` is the agreement check, nothing else
  expect_true(all(c("slope_cv", "slope_ratio", "slope_k", "slope_rel_se") %in% names(cal)))
  # the spread carries no flag of its own: `flag` is the agreement check and nothing else
  expect_identical(cal$flag, is.finite(cal$agreement) &
                     (cal$agreement > 1.35 | cal$agreement < 1 / 1.35))
})


test_that(".paddleViable admits a fit on its duration, not its precision", {
  fit <- function(n) nautilus:::.paddleFit(Sxy = n * 1, Sxx = n * 1, Syy = n * 1.0001, n = n)
  expect_false(nautilus:::.paddleViable(fit(600L), fs = 20))   # 30 s at 20 Hz
  expect_true(nautilus:::.paddleViable(fit(1200L), fs = 20))   # 60 s exactly
  expect_true(nautilus:::.paddleViable(fit(60L), fs = 1))      # 60 s at 1 Hz
  expect_false(nautilus:::.paddleViable(fit(2L), fs = 1))      # too few for a standard error
  expect_false(nautilus:::.paddleViable(nautilus:::.paddleFit(1, 0, 1, 100), fs = 20))  # undefined
})


test_that("the method names are the five documented ones", {
  expect_identical(eval(formals(calculatePaddleSpeed)$method),
                   c("projected-shared", "projected-fixed", "projected-per-tag",
                     "in-situ-deployment", "in-situ-pooled"))
  expect_error(suppressWarnings(calculatePaddleSpeed(.pwCohort(), method = "in-situ", verbose = 0)),
               "should be one of")
})

test_that("speed threshold filtering is opt-in", {
  expect_null(eval(formals(calculatePaddleSpeed)$max.speed))
})


# ---- verbose layout: the header names the METHOD, each block names the SOURCE ----------------------
# Keeping those two vocabularies apart is what makes a fallback visible, so these lock the split.

test_that("the header names the requested method and explains it on one indented line", {
  set.seed(40)
  tags <- list(A = .pwTag("D1", "82", 2019, n = 20000))
  hdr <- function(m, ...) .pwBetween(.pwLog(tags, method = m, ...), to = "D1")

  h <- hdr("in-situ-deployment")
  expect_match(h, "Slope estimation: in situ, per deployment")
  expect_match(h, "fallback: pooled within tag-season")

  h <- hdr("in-situ-pooled")
  expect_match(h, "Slope estimation: in situ, pooled within tag-season")
  expect_false(grepl("fallback", h, fixed = TRUE))

  h <- hdr("projected-shared", calibration = .pwCal(0.07, "82", 2019))
  expect_match(h, "Slope estimation: projected-shared")
  expect_match(h, "missing slopes projected from available calibrations")

  # the validation line reads in symbols, not words
  expect_match(hdr("in-situ-pooled", validate = TRUE, min.pitch = 25),
               "Validation: from pitch and vertical velocity \\(pitch \u2265 25\u00b0\\)")
})


test_that("a pooled fallback is named as a fallback, a pooled choice is not", {
  set.seed(41)
  tags <- list(A = .pwBurst("SHORT", "83", 2019, steep_n = 400L),
               B = .pwBurst("LONG",  "83", 2019, steep_n = 20000L))

  fb <- .pwBetween(.pwLog(tags, method = "in-situ-deployment"), from = "SHORT", to = "LONG")
  expect_match(fb, "slope: [0-9.]+ m/s per Hz \\(in situ, pooled\\)")
  expect_match(fb, "fallback: deployment-level estimate unavailable")
  expect_match(fb, "pooled across: 2 deployments")

  # the same slope, asked for rather than fallen back to, carries no fallback line
  ch <- .pwBetween(.pwLog(tags, method = "in-situ-pooled"), from = "SHORT", to = "LONG")
  expect_match(ch, "slope: [0-9.]+ m/s per Hz \\(in situ, pooled\\)")
  expect_false(grepl("fallback", ch, fixed = TRUE))
  expect_match(ch, "pooled across: 2 deployments")

  # a deployment using its own fit is neither pooled nor a fallback
  own <- .pwBetween(.pwLog(tags, method = "in-situ-deployment"), from = "LONG", to = "SUMMARY")
  expect_match(own, "slope: [0-9.]+ m/s per Hz \\(in situ, this deployment\\)")
  expect_false(grepl("pooled across", own, fixed = TRUE))
  expect_false(grepl("in-situ validation", own, fixed = TRUE))   # circular, so withheld
})


test_that("a skipped deployment prints one line and does not repeat its own name", {
  set.seed(42)
  out <- .pwLog(.pwCohort(), calibration = .pwCal3())
  block <- .pwBetween(out, from = "PIN_04 \\(4/4\\)", to = "SUMMARY")
  expect_match(block, "no paddle wheel")
  expect_match(block, "skipped")
  # the rule above already names it; the skip line must not repeat it
  expect_false(grepl("PIN_04  no paddle wheel", block, fixed = TRUE))
  expect_equal(length(grep("PIN_04", strsplit(block, "\n", fixed = TRUE)[[1]])), 1L)
})


test_that("the summary counts slope sources across deployments, not tag-seasons", {
  set.seed(43)
  # one tag-season, two deployments, both calibrated: 1 tag-season but 2 deployments
  tags <- list(A = .pwTag("D1", "84", 2019, n = 8000), B = .pwTag("D2", "84", 2019, n = 8000))
  tail <- .pwBetween(.pwLog(tags, calibration = .pwCal(0.07, "84", 2019)), from = "SUMMARY")
  expect_match(tail, "tag-seasons: +1")
  expect_match(tail, "slope sources across deployments")
  expect_match(tail, "calibrated: +2")
})


test_that("the slope-source tally sums to the deployments that actually got a speed", {
  set.seed(44)
  # PIN_04 has no paddle wheel but sits in a tag-season that IS calibrated: it inherits the source
  # without ever applying it, and must not be counted among the slopes used
  tags <- c(.pwCohort(), list(E = .pwTag("PIN_NOPAD", "51", 2019, n = 2000,
                                         freq = FALSE, paddle = FALSE)))
  out  <- .pwLog(tags, calibration = .pwCal3())
  tail <- .pwBetween(out, from = "SUMMARY")

  n_speed <- as.integer(sub(".*[^0-9]([0-9]+) of [0-9]+ deployments given a speed.*", "\\1",
                            grep("given a speed", strsplit(tail, "\n", fixed = TRUE)[[1]],
                                 value = TRUE)[1]))
  counts <- as.integer(sub("^ +[a-z, ]+: +([0-9]+)$", "\\1",
                           grep("^ {6}[a-z]", strsplit(tail, "\n", fixed = TRUE)[[1]],
                                value = TRUE)))
  expect_identical(sum(counts), n_speed)
})


test_that("the summary roll-ups are compact: unit on the median only, no padding", {
  set.seed(45)
  out  <- .pwLog(.pwCohort(), calibration = .pwCal3(), validate = TRUE)
  tail <- .pwBetween(out, from = "SUMMARY")
  ln <- function(k) grep(k, strsplit(tail, "\n", fixed = TRUE)[[1]], value = TRUE)[1]

  # the unit rides the median; repeating it on the range only costs width
  expect_match(ln("speed:"), "speed: median [0-9.]+ m/s \\(IQR [0-9.]+.[0-9.]+, range [0-9.]+.[0-9.]+\\)")
  expect_false(grepl("range [0-9.]+.[0-9.]+ m/s", ln("speed:")))

  # one space after the label, not a padded column
  expect_false(grepl("speed:  ", ln("speed:"), fixed = TRUE))
  expect_false(grepl("agreement:  ", ln("agreement:"), fixed = TRUE))
  expect_match(ln("steep swimming:"), "steep swimming: median [0-9.]+% of record \\(range")
})


test_that("the header keeps the package's own spacing, with no blank lines inside it", {
  set.seed(46)
  hdr <- .pwBetween(.pwLog(.pwCohort(), calibration = .pwCal3()), to = "PIN_02")
  lines <- strsplit(hdr, "\n", fixed = TRUE)[[1]]
  body <- lines[cumsum(grepl("Converting paddle rotation", lines)) > 0]
  body <- body[seq_len(max(which(grepl("Validation:", body))))]
  expect_false(any(!nzchar(trimws(body))))          # intro -> bullets -> arrows, uninterrupted
  expect_match(body[1], "Converting paddle rotation")
  expect_match(body[2], "Input: 4 deployments")
  expect_match(body[3], "Slope estimation:")
})


# ---- the calibration diagnostic --------------------------------------------------------------------
# The plot is the only place the between-deployment spread can be SEEN rather than asserted, so these
# check what it chooses to draw rather than how it looks.

test_that("the diagnostic gives a row to every deployment that has something to show", {
  set.seed(50)
  tags <- list(A = .pwTag("D1", "90", 2019, n = 20000),
               B = .pwTag("D2", "90", 2019, n = 20000, slope = 0.09),
               C = .pwTag("NOPAD", "91", 2019, n = 2000, freq = FALSE, paddle = FALSE))
  out <- .pwRun(tags, calibration = .pwCal(0.07, "90", 2019), validate = TRUE)
  cal <- attr(out, "calibration")

  # rebuild the per-deployment table the renderer reads
  sc <- lapply(names(tags), function(k)
    nautilus:::.paddleScanOne(data.table::as.data.table(tags[[k]]), k, 10, TRUE))
  r <- nautilus:::.paddleResolve(sc, .pwCal(0.07, "90", 2019), "projected-shared", NULL, 0.35, 0)
  r$dep$status <- ifelse(r$dep$own_n > 0L, "applied", "no paddle wheel")
  rows <- nautilus:::.paddleDiagRows(r$cal, r$dep)

  expect_setequal(rows$id, c("D1", "D2"))          # the no-paddle deployment earns no row
  expect_false("NOPAD" %in% rows$id)
  # deployments are ordered by the data behind their fit, best-supported first
  expect_identical(rows$key, rep("2019/90", 2L))
})


test_that("the diagnostic writes a file, and falls back to tag-season rows on a large fleet", {
  set.seed(51)
  dir <- withr::local_tempdir()
  tags <- list(A = .pwTag("D1", "92", 2019, n = 8000), B = .pwTag("D2", "92", 2019, n = 8000))
  sc <- lapply(names(tags), function(k)
    nautilus:::.paddleScanOne(data.table::as.data.table(tags[[k]]), k, 10, TRUE))
  r <- nautilus:::.paddleResolve(sc, .pwCal(0.07, "92", 2019), "projected-shared", NULL, 0.35, 0)
  r$dep$status <- "applied"

  f1 <- file.path(dir, "per-deployment.pdf")
  nautilus:::.renderPaddleDiagnostic(r$cal, r$dep, plot.file = f1)
  expect_true(file.exists(f1)); expect_gt(file.size(f1), 1000)

  # max.rows forces the tag-season layout; it must still produce a page
  f2 <- file.path(dir, "fallback.pdf")
  nautilus:::.renderPaddleDiagnostic(r$cal, r$dep, plot.file = f2, max.rows = 1L)
  expect_true(file.exists(f2)); expect_gt(file.size(f2), 1000)

  # nothing to draw is not an error
  empty <- r$dep; empty$status <- "no paddle wheel"; empty$own_n <- 0L
  f3 <- file.path(dir, "empty.pdf")
  expect_silent(nautilus:::.renderPaddleDiagnostic(r$cal, empty, plot.file = f3))
  expect_false(file.exists(f3))
})


test_that("every slope source the resolver can emit has a colour in the diagnostic", {
  # the previous plot tested for "measured", a value the rename retired, so every applied slope
  # silently drew in one colour; this ties the palette to the vocabulary
  srcs <- c("calibrated", "projected-from-tag", "projected-from-fleet",
            "in-situ-deployment", "in-situ-pooled")
  cols <- nautilus:::.paddleSourceColours()
  expect_true(all(srcs %in% names(cols)))
  expect_true(all(vapply(cols, nautilus:::.isColour, logical(1))))
  # and each has a short label for the legend
  expect_false(any(is.na(vapply(srcs, .paddleSourceLabel, character(1), long = FALSE))))
})
