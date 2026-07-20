# Tests for detectDives(): the invariant three-column contract, the hysteresis / prominence / duration
# criteria, the reference and direction choices that make one definition serve every taxon, and - the
# part with a real deployment behind it - the gap and depth-dropout splitting that stops a blackout in
# the depth channel being reported as a multi-hour dive.

# a synthetic PROCESSED tag: a depth trace at 1 Hz (unless `tnum` says otherwise) plus the depth_drift
# provenance detectDives() looks for before it will trust a surface reference
.diveTag <- function(id, depth, tnum = NULL, zoc = TRUE, extra = NULL) {
  n <- length(depth)
  if (is.null(tnum)) tnum <- seq_len(n) - 1
  d <- data.table::data.table(ID = id,
                              datetime = as.POSIXct("2020-01-01", tz = "UTC") + tnum,
                              depth = as.numeric(depth))
  if (!is.null(extra)) for (nm in names(extra)) d[[nm]] <- extra[[nm]]
  m <- nautilus:::.newNautilusMeta(); m$id <- id
  if (zoc) m <- nautilus:::.appendProcessing(m, "depth_drift", status = "applied",
                                             outcome = list(residual_m = 0.1))
  nautilus:::new_nautilus_tag(d, m)
}

# a FULLY specified control: nothing is derived, so every assertion below is about detection rather
# than about the derivation of the floor
.diveCtl <- function(...) {
  a <- list(reference = "surface", depth.threshold = 5, surface.band = 2,
            min.prominence = 5, min.duration = 10, max.gap = 60)
  o <- list(...); a[names(o)] <- o
  do.call(diveControl, a)
}

.detect <- function(tags, ctl = .diveCtl()) {
  if (inherits(tags, "nautilus_tag")) tags <- list(tags)
  detectDives(tags, control = ctl, verbose = FALSE)
}
.nDives <- function(tag, ctl = .diveCtl()) max(.detect(tag, ctl)[[1]]$dive_id)
.diveProv <- function(x) {
  p <- Filter(function(r) identical(r$step, "detectDives"), nautilus:::.getMeta(x)$processing)
  p[[length(p)]]
}

# a square dive train: 30 s at the surface, then six 120 s excursions to 25 m separated by 60 s up top
.diveTrain <- c(rep(0, 30), rep(c(rep(25, 120), rep(0, 60)), 6))


# ---------------------------------------------------------------------------
# the three-column contract
# ---------------------------------------------------------------------------
test_that("dive_id / dive_phase / depth_baseline are added to EVERY deployment, dives or not", {
  res <- .detect(list(.diveTag("TRAIN", .diveTrain),          # dives
                      .diveTag("FLAT", rep(0.2, 500)),        # usable, but no excursion
                      .diveTag("DARK", rep(NA_real_, 200))))  # unusable: no depth at all
  expect_length(res, 3L)
  for (x in res) expect_true(all(c("dive_id", "dive_phase", "depth_baseline") %in% names(x)))
  # the schema does not vary with the outcome - a no-dive and an unusable deployment rbind with a good one
  expect_identical(names(res[[1]]), names(res[[2]]))
  expect_identical(names(res[[1]]), names(res[[3]]))

  # status is recorded per deployment and distinguishes the three outcomes
  expect_equal(vapply(res, function(x) .diveProv(x)$status, character(1)),
               c(TRAIN = "applied", FLAT = "applied_no_dives", DARK = "abstained_no_depth"))
  expect_equal(vapply(res, function(x) .diveProv(x)$n_dives, integer(1)),
               c(TRAIN = 6L, FLAT = 0L, DARK = 0L))
})

test_that("dive_id is integer 0 (never NA) outside dives and dive_phase carries an inter_dive level", {
  x <- .detect(.diveTag("TRAIN", .diveTrain))[[1]]
  expect_type(x$dive_id, "integer")
  expect_false(anyNA(x$dive_id))
  expect_equal(min(x$dive_id), 0L)
  # every sample outside a dive is 0L, and every 0L sample is labelled inter_dive
  expect_true(all(x$dive_id[x$depth < 1] == 0L))
  expect_true(all(as.character(x$dive_phase)[x$dive_id == 0L] == "inter_dive"))
  expect_false(any(as.character(x$dive_phase)[x$dive_id > 0L] == "inter_dive"))

  expect_s3_class(x$dive_phase, "factor")
  expect_false(anyNA(x$dive_phase))
  expect_equal(levels(x$dive_phase), c("descent", "bottom", "ascent", "inter_dive"))
  # the level exists even on a deployment where no sample sits outside... and on one where none sits in
  flat <- .detect(.diveTag("FLAT", rep(0.2, 300)))[[1]]
  expect_equal(levels(flat$dive_phase), c("descent", "bottom", "ascent", "inter_dive"))
  expect_true(all(flat$dive_id == 0L))
  expect_type(flat$depth_baseline, "double")
})


# ---------------------------------------------------------------------------
# what is and is not a dive
# ---------------------------------------------------------------------------
test_that("a clean dive train is recovered exactly, sample for sample", {
  x <- .detect(.diveTag("TRAIN", .diveTrain))[[1]]
  expect_equal(max(x$dive_id), 6L)
  # each excursion contributes exactly its 120 plateau samples, no more and no fewer
  expect_equal(as.integer(table(x$dive_id[x$dive_id > 0])), rep(120L, 6))
  expect_equal(range(which(x$dive_id == 1L)), c(31L, 150L))
  expect_equal(range(which(x$dive_id == 6L)), c(931L, 1050L))
  # ids are assigned in time order with no holes
  expect_equal(sort(unique(x$dive_id[x$dive_id > 0])), 1:6)
})

test_that("hysteresis stops one wiggly excursion fragmenting into many", {
  # crosses the 5 m threshold three times but only ever recovers to 3 m in between
  prof <- c(rep(0, 60), rep(10, 100), rep(3, 100), rep(10, 100), rep(3, 100), rep(10, 100), rep(0, 60))
  tg <- .diveTag("HYS", prof)

  # band 2 m: the trace never re-enters the band, so this is ONE dive spanning all three peaks
  x2 <- .detect(tg, .diveCtl(surface.band = 2))[[1]]
  expect_equal(max(x2$dive_id), 1L)
  expect_equal(range(which(x2$dive_id == 1L)), c(61L, 560L))
  expect_equal(sum(x2$dive_id == 1L), 500L)

  # the complementary case: widen the band past the 3 m recovery and the same trace IS three dives.
  # A naive single-threshold crossing counter would report three in both cases.
  x4 <- .detect(tg, .diveCtl(surface.band = 4))[[1]]
  expect_equal(max(x4$dive_id), 3L)
  expect_equal(as.integer(table(x4$dive_id[x4$dive_id > 0])), rep(100L, 3))
  expect_equal(range(which(x4$dive_id == 2L)), c(261L, 360L))
})

test_that("min.duration rejects an excursion that is too short to be measurable", {
  short <- c(rep(0, 60), rep(20, 20), rep(0, 60))       # a 19 s excursion to 20 m
  expect_equal(.nDives(.diveTag("D", short), .diveCtl(min.duration = 10)), 1L)
  expect_equal(.nDives(.diveTag("D", short), .diveCtl(min.duration = 60)), 0L)
  # the rejection is about duration, not depth: the same 19 s at 200 m is still rejected
  deep <- c(rep(0, 60), rep(200, 20), rep(0, 60))
  expect_equal(.nDives(.diveTag("D", deep), .diveCtl(min.duration = 60)), 0L)
})

test_that("min.prominence rejects a shallow fragment left behind by a gap", {
  # A run can only OPEN above depth.threshold, and diveControl() forbids min.prominence above the
  # threshold - so prominence bites on the pieces a split leaves behind. Here a 20 m dive is cut by a
  # 99 s depth dropout and resumes at 4 m: above the 2 m band (so still one run) but shallow.
  frag <- c(rep(0, 60), rep(20, 300), rep(NA_real_, 100), rep(4, 300), rep(0, 60))
  tg <- .diveTag("P", frag)
  expect_equal(.nDives(tg, .diveCtl(min.prominence = 2)), 2L)   # both pieces survive
  x <- .detect(tg, .diveCtl(min.prominence = 5))[[1]]           # the 4 m piece does not
  expect_equal(max(x$dive_id), 1L)
  expect_equal(range(which(x$dive_id == 1L)), c(61L, 360L))     # the deep piece is the one kept
})


# ---------------------------------------------------------------------------
# reference and direction: one definition, three taxa
# ---------------------------------------------------------------------------
test_that("reference = 'baseline' finds excursions in a record that never returns to zero", {
  # a fish holding ~100 m for 6 h with six 10 min excursions 30 m deeper, sampled every 10 s
  dt <- 10; n <- 6 * 3600 / dt
  tsec <- (seq_len(n) - 1) * dt
  dep <- rep(100, n)
  for (k in 0:5) dep[tsec >= k * 3600 + 1500 & tsec < k * 3600 + 2100] <- 130
  tg <- .diveTag("FISH", dep, tnum = tsec, zoc = FALSE)         # no anchored zero for such an animal

  ctl <- list(depth.threshold = 20, surface.band = 5, min.prominence = 20,
              min.duration = 60, max.gap = 120, baseline.window = 1)
  xb <- .detect(tg, do.call(.diveCtl, c(ctl, list(reference = "baseline"))))[[1]]
  expect_equal(max(xb$dive_id), 6L)
  expect_equal(as.integer(table(xb$dive_id[xb$dive_id > 0])), rep(60L, 6))   # 600 s at 10 s per sample
  # the baseline sits at the animal's holding depth, not at zero
  expect_equal(stats::median(xb$depth_baseline), 100, tolerance = 1)

  # the complementary case, and the reason "baseline" exists: a fixed surface threshold on the SAME
  # record calls the entire deployment one dive, because the fish never comes shallow
  xs <- .detect(tg, do.call(.diveCtl, c(ctl, list(reference = "surface", require.zoc = "ignore"))))[[1]]
  expect_equal(max(xs$dive_id), 1L)
  expect_equal(mean(xs$dive_id > 0), 1)
  expect_true(all(xs$depth_baseline == 0))
})

test_that("direction = 'up' finds excursions off the bottom", {
  # a benthic rester holding ~100 m with six 10 min excursions UP to 60 m
  dt <- 10; n <- 6 * 3600 / dt
  tsec <- (seq_len(n) - 1) * dt
  dep <- rep(100, n)
  for (k in 0:5) dep[tsec >= k * 3600 + 1500 & tsec < k * 3600 + 2100] <- 60
  tg <- .diveTag("BENTHIC", dep, tnum = tsec, zoc = FALSE)
  ctl <- list(reference = "baseline", depth.threshold = 20, surface.band = 5,
              min.prominence = 20, min.duration = 60, max.gap = 120, baseline.window = 1)

  xu <- .detect(tg, do.call(.diveCtl, c(ctl, list(direction = "up"))))[[1]]
  expect_equal(max(xu$dive_id), 6L)
  expect_equal(as.integer(table(xu$dive_id[xu$dive_id > 0])), rep(60L, 6))
  expect_true(all(xu$depth[xu$dive_id > 0] == 60))            # the shallow legs, not the bottom
  # the complementary case: downward excursions do not exist in this record
  xd <- .detect(tg, do.call(.diveCtl, c(ctl, list(direction = "down"))))[[1]]
  expect_equal(max(xd$dive_id), 0L)
})


# ---------------------------------------------------------------------------
# zero dives, and the settings that produced them
# ---------------------------------------------------------------------------
test_that("zero dives is a clean result and the settings are still recorded", {
  x <- expect_silent(.detect(.diveTag("FLAT", rep(0.2, 500)))[[1]])
  expect_equal(max(x$dive_id), 0L)
  p <- .diveProv(x)
  expect_equal(p$status, "applied_no_dives")
  expect_equal(p$n_dives, 0L)
  # the threshold that produced the zero travels with the deployment
  expect_equal(p$depth_threshold_m, 5)
  expect_equal(p$surface_band_m, 2)
  expect_equal(p$min_prominence_m, 5)
  expect_equal(p$min_duration_s, 10)
  expect_equal(p$reference, "surface")
  expect_equal(p$direction, "down")
  expect_equal(p$threshold_source, "user")
})

test_that("max_gap_s is recorded in the detectDives provenance, supplied or derived", {
  x <- .detect(.diveTag("TRAIN", .diveTrain), .diveCtl(max.gap = 45))[[1]]
  expect_equal(.diveProv(x)$max_gap_s, 45)
  # derived: max(60, 10 * median sampling interval). At 1 Hz that floor is 60 s...
  x1 <- .detect(.diveTag("TRAIN", .diveTrain), .diveCtl(max.gap = NULL))[[1]]
  expect_equal(.diveProv(x1)$max_gap_s, 60)
  # ...and at 30 s sampling the interval term takes over
  n <- length(.diveTrain)
  x30 <- .detect(.diveTag("SLOW", .diveTrain, tnum = (seq_len(n) - 1) * 30), .diveCtl(max.gap = NULL))[[1]]
  expect_equal(.diveProv(x30)$max_gap_s, 300)
})


# ---------------------------------------------------------------------------
# gap and dropout splitting
# ---------------------------------------------------------------------------
test_that(".diveSplitOnGaps cuts on a run of ABSENT DEPTH, not only on a jump in time", {
  # PIN_03: 20 Hz rows kept arriving while the depth channel was dark for 8.72 h, so median dt == max
  # dt and no time gap existed to find. Timestamps here are perfectly regular for the same reason.
  tnum <- 0:399
  runs <- data.frame(start_i = 1L, end_i = 400L, sign = 1)

  dark <- rep(20, 400); dark[201:300] <- NA_real_             # 99 s of no depth, regular 1 Hz clock
  s <- nautilus:::.diveSplitOnGaps(runs, tnum, dark, max.gap = 60)
  expect_equal(nrow(s), 2L)
  expect_equal(s$start_i, c(1L, 301L))                       # the dark samples belong to neither piece
  expect_equal(s$end_i, c(200L, 400L))
  expect_equal(s$n_gaps, c(1L, 1L))
  # the split came from the DEPTH channel and from nothing else: hand the SAME clock a complete depth
  # series and the run survives whole, so a time-only rule would have found nothing to cut here
  s0 <- nautilus:::.diveSplitOnGaps(runs, tnum, rep(20, 400), max.gap = 60)
  expect_equal(nrow(s0), 1L)
  expect_equal(c(s0$start_i, s0$end_i), c(1L, 400L))
  expect_equal(s0$n_gaps, 0L)

  # complementary: a 29 s dropout is shorter than max.gap and leaves the run whole
  brief <- rep(20, 400); brief[201:230] <- NA_real_
  s2 <- nautilus:::.diveSplitOnGaps(runs, tnum, brief, max.gap = 60)
  expect_equal(nrow(s2), 1L)
  expect_equal(c(s2$start_i, s2$end_i), c(1L, 400L))
  expect_equal(s2$n_gaps, 0L)

  # and a time jump with a perfectly complete depth channel splits just the same
  s3 <- nautilus:::.diveSplitOnGaps(runs, c(0:199, 300:499), rep(20, 400), max.gap = 60)
  expect_equal(nrow(s3), 2L)
  expect_equal(s3$start_i, c(1L, 201L))
  expect_equal(s3$end_i, c(200L, 400L))
})

test_that("a TIME gap longer than max.gap splits one excursion into two dives", {
  dep <- c(rep(0, 60), rep(20, 600), rep(0, 60))
  keep <- setdiff(seq_along(dep), 60 + 251:350)              # 100 s of the record simply missing
  tg <- .diveTag("TGAP", dep[keep], tnum = keep - 1)
  x <- .detect(tg)[[1]]
  expect_equal(max(x$dive_id), 2L)
  expect_equal(as.integer(table(x$dive_id[x$dive_id > 0])), c(250L, 250L))
  # complementary: the identical depth profile with no missing samples is ONE dive of 600
  whole <- .detect(.diveTag("WHOLE", dep))[[1]]
  expect_equal(max(whole$dive_id), 1L)
  expect_equal(sum(whole$dive_id == 1L), 600L)
})

test_that("a run of ABSENT DEPTH longer than max.gap splits the dive (PIN_03 regression)", {
  dep <- c(rep(0, 60), rep(20, 600), rep(0, 60))
  dep[60 + 251:350] <- NA_real_                              # 99 s dark, timestamps untouched
  tg <- .diveTag("DGAP", dep)
  x <- .detect(tg)[[1]]
  # the timestamps carry no evidence at all of the interruption
  tt <- as.numeric(x$datetime)
  expect_equal(stats::median(diff(tt)), max(diff(tt)))
  expect_equal(max(x$dive_id), 2L)
  expect_equal(as.integer(table(x$dive_id[x$dive_id > 0])), c(250L, 250L))
  # the dark samples are attributed to no dive, rather than interpolated across
  expect_true(all(x$dive_id[is.na(x$depth)] == 0L))
  expect_equal(range(which(x$dive_id == 1L)), c(61L, 310L))
  expect_equal(range(which(x$dive_id == 2L)), c(411L, 660L))
})

test_that("a dropout SHORTER than max.gap leaves the dive intact", {
  dep <- c(rep(0, 60), rep(20, 600), rep(0, 60))
  dep[60 + 251:280] <- NA_real_                              # 29 s dark, below the 60 s rule
  x <- .detect(.diveTag("SHORT", dep))[[1]]
  expect_equal(max(x$dive_id), 1L)
  expect_equal(sum(x$dive_id == 1L), 600L)                   # the dark samples stay inside the dive
  expect_true(all(x$dive_id[is.na(x$depth)] == 1L))
  # the threshold is max.gap and nothing else: lower it below 29 s and the same trace splits
  x2 <- .detect(.diveTag("SHORT", dep), .diveCtl(max.gap = 10))[[1]]
  expect_equal(max(x2$dive_id), 2L)
})
