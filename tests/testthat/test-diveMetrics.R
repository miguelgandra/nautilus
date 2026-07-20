# Tests for diveMetrics(): the fixed schema that lets a mixed cohort rbind, the censoring block that
# says what each row can and cannot support, and the circular handling that keeps a heading average
# from landing on the opposite side of the compass.

# `proc` is an optional named list appended as a processTagData provenance record, which is where
# depth_attenuation reads its smoothing window and the flag saying what that window was applied to.
# Left NULL the tag carries no processTagData step at all, which is its own case.
.dmTag <- function(id, depth, tnum = NULL, extra = NULL, proc = NULL) {
  n <- length(depth)
  if (is.null(tnum)) tnum <- seq_len(n) - 1                       # 1 Hz unless told otherwise
  d <- data.table::data.table(ID = id,
                              datetime = as.POSIXct("2020-01-01", tz = "UTC") + tnum,
                              depth = as.numeric(depth))
  if (!is.null(extra)) for (nm in names(extra)) d[[nm]] <- extra[[nm]]
  m <- nautilus:::.newNautilusMeta(); m$id <- id
  m <- nautilus:::.appendProcessing(m, "depth_drift", status = "applied",
                                    outcome = list(residual_m = 0.1))
  if (!is.null(proc))
    m <- do.call(nautilus:::.appendProcessing, c(list(m, "processTagData"), proc))
  nautilus:::new_nautilus_tag(d, m)
}

.dmCtl <- function(...) {
  a <- list(reference = "surface", depth.threshold = 5, surface.band = 2,
            min.prominence = 5, min.duration = 10, max.gap = 60)
  o <- list(...); a[names(o)] <- o
  do.call(diveControl, a)
}

.dmDetect <- function(tags, ctl = .dmCtl()) {
  if (inherits(tags, "nautilus_tag")) tags <- list(tags)
  detectDives(tags, control = ctl, verbose = FALSE)
}
.dmMetrics <- function(tags, ...) {
  if (inherits(tags, "nautilus_tag")) tags <- list(tags)
  diveMetrics(tags, ..., verbose = FALSE)
}

# --- fixtures ----------------------------------------------------------------------------------
# a square dive: 60 s up top, 600 s at 20 m, 60 s up top. Zero-slope flanks, so no phase structure.
.dmSquare <- c(rep(0, 60), rep(20, 600), rep(0, 60))
# a trapezoidal train: three dives with a real descent, bottom and ascent
.dmTrap <- c(rep(0, 40), rep(c(seq(0, 30, length.out = 40), rep(30, 120),
                               seq(30, 0, length.out = 40)), 3), rep(0, 40))
# the square dive with 99 s of DARK depth mid-plateau (regular timestamps throughout)
.dmDark <- local({ d <- .dmSquare; d[60 + 251:350] <- NA_real_; d })
# the square dive with 29 s of dark depth: too short to split, long enough to dent the coverage
.dmBrief <- local({ d <- .dmSquare; d[60 + 251:280] <- NA_real_; d })

# ONE deployment at 10 s sampling holding `n.short` clean 290 s dives plus a single 7,490 s excursion
# riddled with dropouts too brief to split it (5 samples on, 6 off -> 342/750 = 45.6% coverage). The
# long dive is what the verbose summary is supposed to flag; `n.short` moves the dive count across the
# nrow(out) >= 5 gate without changing anything else. `on`/`off` set the dropout duty cycle, and so the
# long dive's depth_coverage, without touching its duration: any `off` below 7 samples stays under the
# 60 s max.gap and therefore never splits the dive.
.dmLongCohort <- function(n.short, on = 5, off = 6) {
  short <- c(rep(20, 30), rep(0, 30))                              # a 290 s dive, then 300 s up top
  long  <- rep(c(rep(20, on), rep(NA_real_, off)), length.out = 750)  # 7,490 s, never dark for > 50 s
  dep <- c(rep(0, 30), rep(short, n.short), long, rep(0, 30))
  list(depth = dep, tnum = (seq_along(dep) - 1) * 10)
}

# ONE deployment at 10 s sampling holding dives of exactly the requested durations (seconds, multiples
# of the sampling step), each followed by 300 s at the surface and with full depth coverage throughout.
# Unlike .dmLongCohort this puts a real SPREAD on duration_s, which is what makes median + 5 x MAD
# something other than the median - the long-dive threshold cannot be exercised without it.
.dmDurations <- function(durations, dt = 10) {
  dep <- rep(0, 30)
  for (s in durations) dep <- c(dep, rep(20, s / dt + 1), rep(0, 30))
  list(depth = dep, tnum = (seq_along(dep) - 1) * dt)
}


# ---------------------------------------------------------------------------
# the fixed schema
# ---------------------------------------------------------------------------
test_that("the column set depends only on `variables` and `by.phase`, so a mixed cohort rbinds", {
  a <- .dmDetect(.dmTag("TRAP", .dmTrap))[[1]]                    # resolved phases, uncensored
  b <- .dmDetect(.dmTag("DARK", .dmDark))[[1]]                    # no phases, split by a dropout
  ma <- .dmMetrics(a); mb <- .dmMetrics(b)

  # the two deployments really are of different character...
  expect_equal(ma$phase_structure, rep("DBA", 3)); expect_true(all(ma$shape_supported))
  expect_equal(mb$phase_structure, rep("B", 2));   expect_false(any(mb$shape_supported))
  expect_equal(unique(ma$censoring), "none"); expect_equal(unique(mb$censoring), "depth_gap")
  # ...and yet the schema is identical, so rbind cannot fail
  expect_identical(names(ma), names(mb))
  expect_equal(nrow(rbind(ma, mb)), nrow(ma) + nrow(mb))
  both <- .dmMetrics(list(a, b))
  expect_identical(names(both), names(ma))
  expect_equal(nrow(both), 5L)
  expect_equal(both$ID, c(rep("TRAP", 3), rep("DARK", 2)))

  # `variables` and `by.phase` are the ONLY things that move the column set
  v  <- .dmMetrics(list(a, b), variables = c("temp", "heading"))
  vp <- .dmMetrics(list(a, b), variables = c("temp", "heading"), by.phase = TRUE)
  expect_equal(setdiff(names(v), names(both)),
               c("temp_mean", "temp_sd", "heading_mean_angle", "heading_mrl"))
  expect_equal(setdiff(names(vp), names(v)),
               c("temp_descent_mean", "temp_descent_sd", "temp_bottom_mean", "temp_bottom_sd",
                 "temp_ascent_mean", "temp_ascent_sd", "heading_descent_mean_angle",
                 "heading_bottom_mean_angle", "heading_ascent_mean_angle"))
  expect_identical(names(.dmMetrics(a, variables = c("temp", "heading"), by.phase = TRUE)), names(vp))
})

test_that("diveMetrics returns the documented class and a one-row-per-dive table", {
  m <- .dmMetrics(.dmDetect(.dmTag("TRAP", .dmTrap))[[1]])
  expect_s3_class(m, "nautilus_dive_metrics")
  expect_s3_class(m, "data.frame")
  expect_equal(nrow(m), 3L)
  expect_equal(m$dive_id, 1:3)
  # the settings that produced the dives travel with every row
  expect_equal(unique(m$depth_threshold_m), 5)
  expect_equal(unique(m$surface_band_m), 2)
  expect_equal(unique(m$reference), "surface")
  expect_equal(unique(m$direction), "down")
  # timing measured from the timestamps, not from n * dt
  expect_equal(m$duration_s, rep(189, 3))
  expect_equal(m$max_depth_m, rep(30, 3))
  expect_s3_class(m$start, "POSIXct")
})


# ---------------------------------------------------------------------------
# censoring
# ---------------------------------------------------------------------------
test_that("censoring takes the documented values and `complete` is exactly censoring == 'none'", {
  keep <- setdiff(seq_along(.dmSquare), 60 + 251:350)            # 100 s of record simply absent
  cohort <- .dmDetect(list(
    .dmTag("CLEAN",  .dmSquare),                                 # none
    .dmTag("BSTART", c(rep(20, 300), rep(0, 60))),               # boundary (starts mid-dive)
    .dmTag("TGAP",   .dmSquare[keep], tnum = keep - 1),          # time_gap
    .dmTag("DGAP",   .dmDark),                                   # depth_gap
    .dmTag("MIXED",  c(rep(20, 200), rep(NA_real_, 100), rep(20, 200), rep(0, 60)))))
  m <- .dmMetrics(cohort)

  expect_true(all(m$censoring %in% c("none", "boundary", "time_gap", "depth_gap", "mixed")))
  expect_identical(m$complete, m$censoring == "none")
  expect_type(m$complete, "logical")
  # every documented value is actually reachable, and lands where it should
  expect_equal(m$censoring[m$ID == "CLEAN"], "none")
  expect_equal(m$censoring[m$ID == "BSTART"], "boundary")
  expect_equal(m$censoring[m$ID == "TGAP"], c("time_gap", "time_gap"))
  expect_equal(m$censoring[m$ID == "DGAP"], c("depth_gap", "depth_gap"))
  expect_equal(m$censoring[m$ID == "MIXED"], c("mixed", "depth_gap"))   # boundary AND dropout
  # an uncensored dive is charged nothing
  expect_equal(m$n_gaps[m$ID == "CLEAN"], 0L)
  expect_equal(m$gap_s[m$ID == "CLEAN"], 0)
  expect_type(m$n_gaps, "integer")
})

test_that("a boundary dive is retained and flagged, with truncated_start/end set correctly", {
  starts <- .dmMetrics(.dmDetect(.dmTag("S", c(rep(20, 300), rep(0, 60))))[[1]])
  expect_equal(nrow(starts), 1L)                                  # retained, never dropped
  expect_true(starts$truncated_start); expect_false(starts$truncated_end)
  expect_equal(starts$censoring, "boundary"); expect_false(starts$complete)
  expect_equal(starts$n_gaps, 0L); expect_equal(starts$gap_s, 0)   # a boundary costs no record

  ends <- .dmMetrics(.dmDetect(.dmTag("E", c(rep(0, 60), rep(20, 300))))[[1]])
  expect_false(ends$truncated_start); expect_true(ends$truncated_end)
  expect_equal(ends$censoring, "boundary")

  # the complementary case: the same dive with surface either side is untruncated and complete
  mid <- .dmMetrics(.dmDetect(.dmTag("M", .dmSquare))[[1]])
  expect_false(mid$truncated_start); expect_false(mid$truncated_end)
  expect_equal(mid$censoring, "none"); expect_true(mid$complete)
})

test_that("gap_s charges the DARK RUN's span, not the sampling step", {
  m <- .dmMetrics(.dmDetect(.dmTag("DGAP", .dmDark))[[1]])
  expect_equal(nrow(m), 2L)
  expect_equal(m$censoring, c("depth_gap", "depth_gap"))
  expect_equal(m$n_gaps, c(1L, 1L))
  # 100 dark samples at 1 Hz span 99 s. The timestamp STEP either side of them is 1 s, which is what a
  # gap rule that only reads the clock would report; assert the span instead.
  expect_equal(m$gap_s, c(99, 99), tolerance = 1e-8)
  expect_false(isTRUE(all.equal(m$gap_s[1], 1)))
  # a time gap is charged the same way: 100 missing seconds is a 101 s step between kept samples
  keep <- setdiff(seq_along(.dmSquare), 60 + 251:350)
  mt <- .dmMetrics(.dmDetect(.dmTag("TGAP", .dmSquare[keep], tnum = keep - 1))[[1]])
  expect_equal(mt$gap_s, c(101, 101), tolerance = 1e-8)
})

test_that("an edge that BOTH jumped and went dark is charged ONCE, at the longer of the two spans", {
  # The record stops, and when it resumes the depth channel is still dark. The timestamp step and the
  # dark run then describe the SAME lost stretch from two directions, so charging their sum reports the
  # same seconds twice. Each fixture makes one of the two the longer, so neither "always the jump" nor
  # "always the dropout" passes either.

  # (i) 501 dark samples spanning 500 s, reached across a 200 s jump: the DROPOUT is the true extent
  dd <- c(rep(0, 60), rep(20, 300), rep(NA_real_, 501), rep(20, 300), rep(0, 60))
  td <- c(0:359, 559 + 0:500, 1060 + 0:299, 1360 + 0:59)
  md <- .dmMetrics(.dmDetect(.dmTag("DARKWINS", dd, tnum = td))[[1]])
  expect_equal(nrow(md), 2L)
  expect_equal(md$censoring[1], "mixed")                # a jump AND a dropout at the same edge
  expect_equal(md$n_gaps[1], 1L)                        # ONE interruption, not two
  expect_equal(md$gap_s[1], 500, tolerance = 1e-8)      # max(200, 500); their sum would be 700
  # the far side of the same dropout was reached without a jump, so it is a plain depth_gap of 500 s
  expect_equal(md$censoring[2], "depth_gap")
  expect_equal(md$gap_s[2], 500, tolerance = 1e-8)

  # (ii) the same edge with the magnitudes swapped: 101 dark samples spanning 100 s, across a 400 s jump
  dt <- c(rep(0, 60), rep(20, 300), rep(NA_real_, 101), rep(20, 300), rep(0, 60))
  tt <- c(0:359, 759 + 0:100, 860 + 0:299, 1160 + 0:59)
  mj <- .dmMetrics(.dmDetect(.dmTag("JUMPWINS", dt, tnum = tt))[[1]])
  expect_equal(nrow(mj), 2L)
  expect_equal(mj$censoring[1], "mixed")
  expect_equal(mj$n_gaps[1], 1L)
  expect_equal(mj$gap_s[1], 400, tolerance = 1e-8)      # max(400, 100); their sum would be 500
  expect_equal(mj$gap_s[2], 100, tolerance = 1e-8)      # the dropout alone bounds the second piece
})

test_that("depth_coverage separates a genuine foray from a sensor dropout", {
  full <- .dmMetrics(.dmDetect(.dmTag("FULL", .dmSquare))[[1]])
  expect_equal(full$depth_coverage, 1)
  expect_equal(full$n_samples, 600L)

  # a 29 s dropout is too short to split the dive, so it shows up in the coverage instead:
  # 600 samples in the dive, 30 of them without a depth -> 570 / 600
  brief <- .dmMetrics(.dmDetect(.dmTag("BRIEF", .dmBrief))[[1]])
  expect_equal(nrow(brief), 1L)
  expect_equal(brief$n_samples, 600L)
  expect_equal(brief$depth_coverage, 0.95, tolerance = 1e-12)
  expect_equal(brief$censoring, "none")            # not a gap by the max.gap rule, only thinner data

  # the pieces either side of a SPLITTING dropout carry full coverage - the dark samples are in neither
  split <- .dmMetrics(.dmDetect(.dmTag("DARK", .dmDark))[[1]])
  expect_equal(split$depth_coverage, c(1, 1))
  expect_equal(split$n_samples, c(250L, 250L))
})


# ---------------------------------------------------------------------------
# the inter-dive interval, and what censors it
# ---------------------------------------------------------------------------
test_that("a dropout BETWEEN two clean dives censors the interval, though neither dive is censored", {
  # The exact regression: derived from the bounding dives' own censoring, this row read as a clean
  # 221 s surface interval, when 99 s of it is a depth channel that had gone dark. A user filtering on
  # !inter_dive_censored to study surface behaviour would have kept precisely the worst rows.
  dep <- c(rep(0, 60), rep(20, 300), rep(0, 60), rep(NA_real_, 100), rep(0, 60), rep(20, 300), rep(0, 60))
  m <- .dmMetrics(.dmDetect(.dmTag("BETWEEN", dep))[[1]])
  expect_equal(nrow(m), 2L)
  # the dropout sits clear of both dives, so BOTH bounding dives are genuinely uncensored...
  expect_equal(m$censoring, c("none", "none"))
  expect_true(all(m$complete))
  expect_equal(m$gap_s, c(0, 0))
  expect_false(any(m$truncated_start | m$truncated_end))
  # ...and yet the interval between them is not something the record can describe
  expect_equal(m$inter_dive_s, c(221, NA_real_))
  expect_identical(m$inter_dive_censored, c(TRUE, NA))
})

test_that("a TIME gap between two clean dives censors the interval the same way", {
  dep <- c(rep(0, 60), rep(20, 300), rep(0, 60), rep(0, 60), rep(20, 300), rep(0, 60))
  tnum <- c(0:419, 620:1039)                              # 200 s of record missing, up top, between dives
  m <- .dmMetrics(.dmDetect(.dmTag("TBETWEEN", dep, tnum = tnum))[[1]])
  expect_equal(nrow(m), 2L)
  expect_equal(m$censoring, c("none", "none"))
  expect_true(all(m$complete))
  expect_equal(m$inter_dive_s, c(321, NA_real_))          # 359 s to 680 s, 200 s of it unrecorded
  expect_identical(m$inter_dive_censored, c(TRUE, NA))
})

test_that("an uninterrupted interval is NOT censored, and the last dive's interval is NA", {
  # the same two dives with nothing missing between them: the complementary case, so the two tests above
  # are about the gap and not merely about having two dives
  dep <- c(rep(0, 60), rep(20, 300), rep(0, 120), rep(20, 300), rep(0, 60))
  m <- .dmMetrics(.dmDetect(.dmTag("CLEANGAP", dep))[[1]])
  expect_equal(nrow(m), 2L)
  expect_equal(m$inter_dive_s, c(121, NA_real_))
  expect_identical(m$inter_dive_censored, c(FALSE, NA))
  expect_type(m$inter_dive_censored, "logical")

  # a lone dive has no interval at all - both columns are NA, never 0 and never FALSE
  one <- .dmMetrics(.dmDetect(.dmTag("ONE", .dmSquare))[[1]])
  expect_equal(nrow(one), 1L)
  expect_true(is.na(one$inter_dive_s))
  expect_true(is.na(one$inter_dive_censored))
  expect_type(one$inter_dive_censored, "logical")
})

test_that("a dive truncated at a record BOUNDARY leaves the interval beside it uncensored", {
  # These two questions are deliberately unlinked. An interval has a dive on each side, so it can never
  # be the thing a record edge cut short: `truncated_start` holds only for the first dive, which is
  # never a successor, and `truncated_end` only for the last, which has no successor. Censoring an
  # interval because a neighbour touches a record edge would throw away a surface interval that was
  # recorded in full, and an implementation that read the bounding dives' flags with the indices off by
  # one would do exactly that - hence both directions below.

  # the record STARTS mid-dive: dive 1 is boundary-censored, the interval after it is not
  st <- c(rep(20, 300), rep(0, 120), rep(20, 300), rep(0, 60))
  ms <- .dmMetrics(.dmDetect(.dmTag("TRUNCSTART", st))[[1]])
  expect_equal(nrow(ms), 2L)
  expect_true(ms$truncated_start[1]); expect_false(ms$truncated_end[1])
  expect_equal(ms$censoring, c("boundary", "none"))
  expect_identical(ms$complete, c(FALSE, TRUE))
  expect_equal(ms$inter_dive_s, c(121, NA_real_))
  expect_identical(ms$inter_dive_censored, c(FALSE, NA))

  # the mirror - the record STOPS mid-dive: dive 2 is boundary-censored, the interval before it is not
  en <- c(rep(0, 60), rep(20, 300), rep(0, 120), rep(20, 300))
  me <- .dmMetrics(.dmDetect(.dmTag("TRUNCEND", en))[[1]])
  expect_equal(nrow(me), 2L)
  expect_false(me$truncated_start[2]); expect_true(me$truncated_end[2])
  expect_equal(me$censoring, c("none", "boundary"))
  expect_identical(me$complete, c(TRUE, FALSE))
  expect_equal(me$inter_dive_s, c(121, NA_real_))
  expect_identical(me$inter_dive_censored, c(FALSE, NA))

  # and the FALSE above is not vacuous: the same truncated-start record, with 99 s of the surface
  # stretch between the dives gone dark, censors the interval while dive 1 stays boundary-censored
  dk <- c(rep(20, 300), rep(0, 10), rep(NA_real_, 100), rep(0, 10), rep(20, 300), rep(0, 60))
  md <- .dmMetrics(.dmDetect(.dmTag("TRUNCDARK", dk))[[1]])
  expect_equal(nrow(md), 2L)
  expect_equal(md$censoring, c("boundary", "none"))     # the dropout sits clear of both dives
  expect_equal(md$inter_dive_s, c(121, NA_real_))       # the same interval, to the second
  expect_identical(md$inter_dive_censored, c(TRUE, NA))
})


# ---------------------------------------------------------------------------
# the verbose summary: long dives are flagged, never split
# ---------------------------------------------------------------------------
test_that("the verbose summary flags an unusually long, poorly covered dive", {
  f <- .dmLongCohort(4)                                   # 4 short dives + 1 long one = 5 rows
  x <- .dmDetect(.dmTag("LONG", f$depth, tnum = f$tnum))[[1]]
  m <- .dmMetrics(x)
  # the fixture really is what the flag describes: one dive far beyond the 2 h floor, mostly absent record
  expect_equal(nrow(m), 5L)
  expect_equal(m$duration_s, c(290, 290, 290, 290, 7490))
  expect_equal(m$depth_coverage[5], 342 / 750, tolerance = 1e-12)
  expect_lt(m$depth_coverage[5], 0.5)
  # ...and it is FLAGGED, not split: the long dive is still a single row
  txt <- gsub("[[:space:]]+", " ",
              paste(cli::cli_fmt(diveMetrics(x, verbose = 2)), collapse = " "))
  expect_match(txt, "1 unusually long dive (max 2.1 h, median depth coverage 46%) - not split",
               fixed = TRUE)
  expect_match(txt, "low coverage: check these are forays and not sensor dropouts", fixed = TRUE)

  # the sub-detail is conditional on the coverage, not printed alongside every flag: the same long dive
  # with a complete depth channel is flagged WITHOUT it
  g <- .dmLongCohort(4)
  g$depth[is.na(g$depth)] <- 20
  xf <- .dmDetect(.dmTag("FULLCOV", g$depth, tnum = g$tnum))[[1]]
  tf <- gsub("[[:space:]]+", " ",
             paste(cli::cli_fmt(diveMetrics(xf, verbose = 2)), collapse = " "))
  expect_match(tf, "1 unusually long dive (max 2.1 h, median depth coverage 100%) - not split",
               fixed = TRUE)
  expect_false(grepl("low coverage", tf, fixed = TRUE))
})

test_that("the long-dive threshold is median + 5 x MAD wherever the spread clears the 2 h floor", {
  # .dmLongCohort cannot test this: its short dives are all 290 s, MAD is 0, and median + 5 x MAD
  # collapses onto the median, leaving the 2 h floor to decide everything. Here the durations really do
  # have a spread - median 3,500 s, MAD 1,482.6 s - so the threshold is 3,500 + 5 x 1,482.6 = 10,913 s
  # and the floor is the term that decides nothing.
  # The 20,000 s dive pins the coefficient from ABOVE: it sits between lim(5) = 14,869.5 and
  # lim(10) = 25,989, so inflating the 5 drops it out of the flag. Without such a row the coefficient
  # was pinned only from below and a 10 x MAD rule passed the suite unnoticed. Between them the two
  # outliers box the rule in on both sides: 1 x flags three dives, 5 x flags two, 10 x flags one.
  f <- .dmDurations(c(2000, 2500, 3000, 3500, 4000, 7500, 20000, 30000))
  x <- .dmDetect(.dmTag("SPREAD", f$depth, tnum = f$tnum))[[1]]
  m <- .dmMetrics(x)
  expect_equal(nrow(m), 8L)
  expect_equal(m$duration_s, c(2000, 2500, 3000, 3500, 4000, 7500, 20000, 30000))
  lim <- stats::median(m$duration_s) + 5 * stats::mad(m$duration_s)
  expect_equal(lim, 14869.5, tolerance = 1e-9)
  expect_gt(lim, 2 * 3600)
  # the 7,500 s dive is the discriminating row: past the 2 h floor, short of the dispersion threshold.
  # Shrink the coefficient to 1 and the threshold drops to 4,983 s, the floor takes over at 7,200 s, and
  # this dive joins the flag - so the count below is what pins the 5.
  expect_gt(m$duration_s[6], 2 * 3600); expect_lt(m$duration_s[6], lim)

  txt <- gsub("[[:space:]]+", " ",
              paste(cli::cli_fmt(diveMetrics(x, verbose = 2)), collapse = " "))
  expect_match(txt, "8 dives summarised", fixed = TRUE)
  expect_match(txt, "2 unusually long dives (max 8.3 h, median depth coverage 100%) - not split",
               fixed = TRUE)
  expect_false(grepl("1 unusually long dive ", txt, fixed = TRUE))   # a 10 x MAD rule would say this
  expect_false(grepl("3 unusually long dives", txt, fixed = TRUE))   # a 1 x MAD rule would say this
  expect_false(grepl("low coverage", txt, fixed = TRUE))     # full depth throughout
})

test_that("the 2 h floor decides when median + 5 x MAD lands below it", {
  # The complementary configuration, and the only one in which the floor rather than the dispersion
  # rule settles the outcome: median 500 s, MAD 296.5 s, threshold 1,982.6 s - far below 2 h. Two dives
  # sit above that threshold and only one of them above the floor, so dropping the floor changes the
  # answer where the test above could not see it.
  f <- .dmDurations(c(200, 300, 400, 500, 600, 5000, 9000))
  x <- .dmDetect(.dmTag("FLOOR", f$depth, tnum = f$tnum))[[1]]
  m <- .dmMetrics(x)
  expect_equal(nrow(m), 7L)
  expect_equal(m$duration_s, c(200, 300, 400, 500, 600, 5000, 9000))
  lim <- stats::median(m$duration_s) + 5 * stats::mad(m$duration_s)
  expect_equal(lim, 1982.6, tolerance = 1e-9)
  expect_lt(lim, 2 * 3600)
  # the 5,000 s dive is the discriminating row: over the dispersion threshold, under the floor
  expect_gt(m$duration_s[6], lim); expect_lt(m$duration_s[6], 2 * 3600)

  txt <- gsub("[[:space:]]+", " ",
              paste(cli::cli_fmt(diveMetrics(x, verbose = 2)), collapse = " "))
  expect_match(txt, "1 unusually long dive (max 2.5 h, median depth coverage 100%) - not split",
               fixed = TRUE)
  expect_false(grepl("2 unusually long dives", txt, fixed = TRUE))
})

test_that("the low-coverage sub-detail is gated at 0.5, not merely at less than full coverage", {
  # 7 samples of depth to every 3 dark ones: the long dive is measured 70% of the time, thin enough
  # that any "not complete" rule would fire and clear enough of the 0.5 the sub-detail is about. The
  # existing cases are 45.6% and 100%, which a threshold anywhere in (0.456, 1] would satisfy.
  g <- .dmLongCohort(4, on = 7, off = 3)
  x <- .dmDetect(.dmTag("MIDCOV", g$depth, tnum = g$tnum))[[1]]
  m <- .dmMetrics(x)
  expect_equal(nrow(m), 5L)
  expect_equal(m$duration_s, c(290, 290, 290, 290, 7490))    # the dropouts still never split the dive
  expect_equal(m$depth_coverage[5], 0.7, tolerance = 1e-12)
  expect_gt(m$depth_coverage[5], 0.5); expect_lt(m$depth_coverage[5], 1)

  txt <- gsub("[[:space:]]+", " ",
              paste(cli::cli_fmt(diveMetrics(x, verbose = 2)), collapse = " "))
  expect_match(txt, "1 unusually long dive (max 2.1 h, median depth coverage 70%) - not split",
               fixed = TRUE)
  expect_false(grepl("low coverage", txt, fixed = TRUE))
})

test_that("the long-dive flag is gated on 5 dives, so a handful of rows sets no threshold", {
  # median + 5 * MAD over 3 or 4 dives is not an outlier rule, it is noise; below 5 rows the block does
  # not run at all. The fixture is the flagged one minus a single short dive.
  f <- .dmLongCohort(3)
  x <- .dmDetect(.dmTag("FEW", f$depth, tnum = f$tnum))[[1]]
  m <- .dmMetrics(x)
  expect_equal(nrow(m), 4L)
  # the long dive still qualifies on every criterion the flag uses - only the row count differs
  expect_equal(m$duration_s, c(290, 290, 290, 7490))
  expect_gt(m$duration_s[4], 2 * 3600)
  expect_lt(m$depth_coverage[4], 0.5)
  txt <- gsub("[[:space:]]+", " ",
              paste(cli::cli_fmt(diveMetrics(x, verbose = 2)), collapse = " "))
  expect_match(txt, "4 dives summarised", fixed = TRUE)    # the summary block DID run
  expect_false(grepl("unusually long", txt, fixed = TRUE))
  expect_false(grepl("low coverage", txt, fixed = TRUE))
})


# ---------------------------------------------------------------------------
# depth_attenuation: what a smoothing window could have taken off this dive
# ---------------------------------------------------------------------------
test_that("depth_attenuation is charged only when the window reached the STORED depth channel", {
  # A 12 s dive against a recorded 10 s window is where the difference is largest: the retention
  # formula would charge 1 - 10 / (2 * 12) = 0.583, i.e. it would report that up to 42% of this dive's
  # amplitude may be missing. Whether that is true depends entirely on what the window was applied to,
  # so the two records below differ in the provenance flag and in nothing else.
  brief <- c(rep(0, 60), rep(20, 13), rep(0, 60))

  # (a) a CURRENT record states that the window never reached `depth` - it conditioned only the series
  # the vertical velocity was differentiated from - so nothing attenuated the depth measured here
  cur <- .dmMetrics(.dmDetect(.dmTag("CURRENT", brief,
                                     proc = list(depth_smoothing = 10,
                                                 depth_channel_smoothed = FALSE)))[[1]])
  expect_equal(nrow(cur), 1L)
  expect_equal(cur$duration_s, 12)                    # short enough that a charge would be visible
  expect_equal(cur$depth_attenuation, 1)

  # (b) a LEGACY record carries the window but no such flag, because it predates the change. There the
  # window did reach `depth`, and the row is charged the real retention.
  leg <- .dmMetrics(.dmDetect(.dmTag("LEGACY", brief, proc = list(depth_smoothing = 10)))[[1]])
  expect_equal(leg$duration_s, 12)
  expect_equal(leg$depth_attenuation, 1 - 10 / (2 * 12), tolerance = 1e-12)
  expect_lt(leg$depth_attenuation, cur$depth_attenuation)

  # the same legacy window on a 599 s dive costs almost nothing - the charge scales with duration, so
  # it is the formula that is pinned here and not a constant
  lng <- .dmMetrics(.dmDetect(.dmTag("LEGACYLONG", .dmSquare,
                                     proc = list(depth_smoothing = 10)))[[1]])
  expect_equal(lng$duration_s, 599)
  expect_equal(lng$depth_attenuation, 1 - 10 / (2 * 599), tolerance = 1e-12)

  # BELOW the window the retention is T / (2L), not the negative 1 - L/(2T) would give (1 - 40/24) and
  # not the 0 that clamping it produced. A 12 s dive under a 40 s window keeps 12/80 = 0.15 of its
  # amplitude - reduced, but nowhere near erased, which is what the clamp used to claim.
  wide <- .dmMetrics(.dmDetect(.dmTag("LEGACYWIDE", brief, proc = list(depth_smoothing = 40)))[[1]])
  expect_equal(wide$duration_s, 12)
  expect_equal(wide$depth_attenuation, 12 / (2 * 40), tolerance = 1e-12)
  expect_gt(wide$depth_attenuation, 0)

  # the two branches meet at T == L: both give exactly 0.5, so the column is continuous there
  meet <- .dmMetrics(.dmDetect(.dmTag("LEGACYMEET", brief, proc = list(depth_smoothing = 12)))[[1]])
  expect_equal(meet$depth_attenuation, 0.5, tolerance = 1e-12)

  # (c) no processTagData record at all: "nothing known to attenuate", which reads as 1
  non <- .dmMetrics(.dmDetect(.dmTag("NOPROV", brief))[[1]])
  expect_equal(non$depth_attenuation, 1)
  # ...and so does a record whose processTagData step logged no window
  nowin <- .dmMetrics(.dmDetect(.dmTag("NOWINDOW", brief,
                                       proc = list(static_window = 5)))[[1]])
  expect_equal(nowin$depth_attenuation, 1)
})


# ---------------------------------------------------------------------------
# per-sample channels
# ---------------------------------------------------------------------------
test_that("circular variables average around the wrap point, not through it", {
  n <- length(.dmSquare)
  wrap  <- rep(c(350, 355, 0, 5, 10), length.out = n)      # arithmetic mean of these is 144 degrees
  plain <- rep(c(80, 85, 90, 95, 100), length.out = n)     # a control that does not cross 0/360
  x <- .dmDetect(.dmTag("H", .dmSquare, extra = list(heading = wrap, hdg_plain = plain,
                                                     temp = rep(12, n))))[[1]]
  m <- .dmMetrics(x, variables = c("heading", "hdg_plain", "temp"),
                  circular.variables = c("heading", "hdg_plain"))

  # circular channels get _mean_angle / _mrl; linear ones get _mean / _sd. Never both.
  expect_true(all(c("heading_mean_angle", "heading_mrl", "temp_mean", "temp_sd") %in% names(m)))
  expect_false(any(c("heading_mean", "heading_sd", "temp_mean_angle", "temp_mrl") %in% names(m)))

  # the wrapped heading averages to 0/360, NOT to the 144 degrees a naive arithmetic mean gives
  wrapped <- ((m$heading_mean_angle + 180) %% 360) - 180
  expect_equal(wrapped, 0, tolerance = 1e-6)
  expect_gt(abs(m$heading_mean_angle - 144), 100)
  # mean resultant length of the five directions: (2 cos 10 + 2 cos 5 + 1) / 5
  expect_equal(m$heading_mrl, 0.99240098, tolerance = 1e-7)
  # the non-wrapping control lands on its arithmetic mean, so the circular path is not merely different
  expect_equal(m$hdg_plain_mean_angle, 90, tolerance = 1e-6)
  expect_equal(m$temp_mean, 12); expect_equal(m$temp_sd, 0)
})

test_that("an absent requested variable yields NA columns without changing the schema", {
  n <- length(.dmSquare)
  has <- .dmDetect(.dmTag("HAS", .dmSquare,
                          extra = list(temp = rep(12, n), aux = rep(3, n),
                                       heading = rep(90, n))))[[1]]
  lacks <- .dmDetect(.dmTag("LACKS", .dmSquare, extra = list(temp = rep(12, n))))[[1]]

  vars <- c("temp", "aux", "heading")
  mh <- expect_silent(.dmMetrics(has, variables = vars))
  ml <- expect_silent(.dmMetrics(lacks, variables = vars))       # no error, no warning
  expect_identical(names(mh), names(ml))
  expect_equal(nrow(ml), 1L)

  # the absent linear channel is NA, the absent circular channel is NA in both of its columns
  expect_true(is.na(ml$aux_mean)); expect_true(is.na(ml$aux_sd))
  expect_true(is.na(ml$heading_mean_angle)); expect_true(is.na(ml$heading_mrl))
  # the present ones are not, and the shared columns are unaffected
  expect_equal(mh$aux_mean, 3); expect_equal(mh$heading_mean_angle, 90)
  expect_equal(ml$temp_mean, 12)
  expect_equal(nrow(rbind(mh, ml)), 2L)
})


# ---------------------------------------------------------------------------
# nothing to summarise
# ---------------------------------------------------------------------------
test_that("no dives anywhere returns a 0-row table with the FULL schema and the right class", {
  flat <- .dmDetect(.dmTag("FLAT", rep(0.2, 400), extra = list(temp = rep(12, 400))))[[1]]
  expect_equal(max(flat$dive_id), 0L)                             # nothing to reduce
  e <- .dmMetrics(flat, variables = c("temp", "heading"))
  expect_s3_class(e, "nautilus_dive_metrics")
  expect_s3_class(e, "data.frame")
  expect_equal(nrow(e), 0L)

  # the schema is the full one, identical to a run that DID find dives - so an empty deployment rbinds
  n <- length(.dmSquare)
  full <- .dmMetrics(.dmDetect(.dmTag("D", .dmSquare, extra = list(temp = rep(12, n))))[[1]],
                     variables = c("temp", "heading"))
  expect_identical(names(e), names(full))
  expect_equal(nrow(rbind(e, full)), nrow(full))
  # the same guarantee with by.phase columns in play
  e2 <- .dmMetrics(flat, variables = c("temp", "heading"), by.phase = TRUE)
  f2 <- .dmMetrics(.dmDetect(.dmTag("D", .dmSquare, extra = list(temp = rep(12, n))))[[1]],
                   variables = c("temp", "heading"), by.phase = TRUE)
  # identical, not merely the same SET: the empty table and a populated one must hold the by.phase
  # columns in the same ORDER, or rbind() silently pairs a descent mean with an ascent sd
  expect_identical(names(e2), names(f2))
  expect_equal(nrow(rbind(e2, f2)), nrow(f2))
  # and the column types are the real ones, not a frame of logicals
  expect_type(e$ID, "character"); expect_type(e$dive_id, "integer")
  expect_type(e$duration_s, "double"); expect_type(e$complete, "logical")
  expect_type(e$censoring, "character"); expect_type(e$n_gaps, "integer")
  expect_s3_class(e$start, "POSIXct"); expect_s3_class(e$max_depth_time, "POSIXct")
})
