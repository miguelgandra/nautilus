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
            min.amplitude = 5, min.duration = 10, max.gap = 60)
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

test_that("min.amplitude rejects a shallow fragment left behind by a gap", {
  # A run can only OPEN above depth.threshold, so the amplitude screen bites only on the pieces a
  # split leaves behind. Here a 20 m dive is cut by a 99 s depth dropout and resumes at 4 m: above the
  # 2 m band (so still one run) but not a 20 m dive.
  frag <- c(rep(0, 60), rep(20, 300), rep(NA_real_, 100), rep(4, 300), rep(0, 60))
  tg <- .diveTag("P", frag)
  expect_equal(.nDives(tg, .diveCtl(min.amplitude = 2)), 2L)    # both pieces survive
  x <- .detect(tg, .diveCtl(min.amplitude = 5))[[1]]            # the 4 m piece does not
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

  ctl <- list(depth.threshold = 20, surface.band = 5, min.amplitude = 20,
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
  expect_equal(p$min_amplitude_m, 5)
  expect_true(is.infinite(p$min_prominence_m))   # splitting is opt-in, so the default never splits
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


#######################################################################################################
# Topographic prominence ##############################################################################
#
# `min.prominence` used to be INERT by construction: a run only exists because the residual passed
# depth.threshold, and diveControl forbade min.prominence from exceeding depth.threshold, so the test
# `amp >= min.prominence` was true for every candidate. prominence_m was a second copy of amplitude_m.

# a W: down to 50, back to 15 (never reaching the 2 m band, so hysteresis cannot close), down to 48
.dpW <- function(peak2 = 48, saddle = 15) {
  c(0, 0, seq(0, 50, length.out = 20), seq(50, saddle, length.out = 15),
    seq(saddle, peak2, length.out = 15), seq(peak2, 0, length.out = 20), 0, 0)
}

test_that("hysteresis alone cannot separate a W; prominence can", {
  z <- .dpW()
  r <- .diveRuns(z, seq_along(z), threshold = 10, band = 2, sign = 1)
  expect_equal(nrow(r), 1L)                                    # one run: it never re-enters the band
  # the saddle is at 15 and the second peak reaches 48, so it stands 33 m proud
  expect_equal(nrow(.diveSplitOnProminence(r, z, min.prominence = 30)), 2L)
  expect_equal(nrow(.diveSplitOnProminence(r, z, min.prominence = 40)), 1L)  # too tall a bar: one dive
  expect_equal(nrow(.diveSplitOnProminence(r, z, min.prominence = Inf)), 1L) # never split
})

test_that("the split happens AT the saddle, not somewhere convenient", {
  z <- .dpW()
  r <- .diveRuns(z, seq_along(z), threshold = 10, band = 2, sign = 1)
  sp <- .diveSplitOnProminence(r, z, min.prominence = 10)
  expect_equal(nrow(sp), 2L)
  cut <- sp$end_i[1]
  expect_equal(cut + 1L, sp$start_i[2])                        # contiguous, nothing dropped
  # the cut sample is the minimum of the interior valley, within one sample of the true saddle
  interior <- (sp$start_i[1] + 5):(sp$end_i[2] - 5)
  expect_lte(abs(cut - interior[which.min(z[interior])]), 1L)
})

test_that("a shallower saddle splits and a deeper one does not, at the same threshold", {
  # the discriminating pair: only the saddle DEPTH differs, so nothing else can explain the outcome
  run <- function(saddle) {
    z <- .dpW(saddle = saddle)
    r <- .diveRuns(z, seq_along(z), threshold = 10, band = 2, sign = 1)
    nrow(.diveSplitOnProminence(r, z, min.prominence = 20))
  }
  expect_equal(run(15), 2L)      # second peak stands 48 - 15 = 33 m proud -> splits
  expect_equal(run(40), 1L)      # second peak stands only  48 - 40 =  8 m proud -> one dive
})

test_that("a dropout inside the excursion does not defeat the saddle search", {
  # cummax() propagates NA, so scoring the raw series would find no saddle at all for any run holding
  # a single missing sample - and short dropouts inside a dive are routine
  z <- .dpW(); zn <- z; zn[c(30, 55)] <- NA
  r <- .diveRuns(z, seq_along(z), threshold = 10, band = 2, sign = 1)
  expect_equal(nrow(.diveSplitOnProminence(r, zn, min.prominence = 10)), 2L)
})

test_that("splitting is OPT-IN: the derived default leaves excursions whole", {
  # deriving this as (threshold - band) turned 6,512 real dives into 11,658. Splitting a W is an
  # interpretive act and must be asked for, exactly as no maximum dive duration is imposed.
  n <- 900
  dep <- rep(0, n)
  dep[101:160] <- 40; dep[161:180] <- 12; dep[181:240] <- 38    # one W-shaped excursion
  tg <- data.frame(ID = "W", datetime = as.POSIXct("2024-01-01", tz = "UTC") + seq_len(n) - 1,
                   depth = dep, stringsAsFactors = FALSE)
  ctl <- function(...) diveControl(depth.threshold = 5, surface.band = 2, min.duration = 10, ...)
  n_of <- function(...) {
    a <- suppressWarnings(detectDives(tg, control = ctl(...), verbose = FALSE))[[1]]
    length(setdiff(unique(a$dive_id), 0L))
  }
  expect_equal(n_of(), 1L)                          # default: whole
  expect_equal(n_of(min.prominence = 10), 2L)       # opt in: separated
  expect_equal(n_of(min.prominence = 1e6), 1L)      # an unreachable bar is the explicit "never"
})

test_that("min.prominence may now exceed depth.threshold, which used to abort", {
  # that rule was what made the criterion inert; above the threshold is the meaningful way to say never
  expect_s3_class(diveControl(depth.threshold = 10, min.prominence = 50), "nautilus_dive")
  expect_error(diveControl(depth.threshold = 10, min.prominence = -1), "min.prominence")
})

test_that("prominence_m is no longer a second copy of amplitude_m", {
  n <- 600
  dep <- rep(0, n); dep[101:250] <- 30
  tg <- data.frame(ID = "A", datetime = as.POSIXct("2024-01-01", tz = "UTC") + seq_len(n) - 1,
                   depth = dep, stringsAsFactors = FALSE)
  a <- suppressWarnings(detectDives(tg, control = diveControl(depth.threshold = 5, surface.band = 2,
                                                              min.duration = 10), verbose = FALSE))
  m <- suppressWarnings(diveMetrics(a, verbose = FALSE))
  expect_equal(nrow(m), 1L)
  expect_false(isTRUE(all.equal(m$amplitude_m, m$prominence_m)))
  expect_lt(m$prominence_m, m$amplitude_m)          # amplitude is from the reference, prominence from
                                                    # the col, which sits at the band
})


# ---------------------------------------------------------------------------
# reference = "auto" needs BOTH conditions, not just an anchored zero
# ---------------------------------------------------------------------------

test_that("reference = 'auto' requires the record to visit the surface band, not only an anchored ZOC", {
  # an anchored zero on a record that never comes shallow: every sample sits at 40-60 m, so a
  # 0-referenced band is never re-entered and a surface threshold would report one endless dive
  set.seed(1)
  deep <- 50 + 10 * sin(seq(0, 12 * pi, length.out = 3000))
  tag  <- .diveTag("DEEP", deep, zoc = TRUE)

  # this fixture legitimately trips the median-baseline caution (its excursions fill the record), and
  # that warning now fires irrespective of `verbose` - see the test below
  res <- suppressWarnings(
    detectDives(list(tag), control = diveControl(reference = "auto", depth.threshold = 5,
                                                 surface.band = 2, min.duration = 10),
                verbose = FALSE))
  expect_identical(.diveProv(res[[1]])$reference, "baseline")

  # the same record with the occupancy requirement switched off resolves the old way, to "surface"
  res0 <- suppressWarnings(
    detectDives(list(tag), control = diveControl(reference = "auto", depth.threshold = 5,
                                                 surface.band = 2, min.duration = 10,
                                                 min.surface.occupancy = 0),
                verbose = FALSE))
  expect_identical(.diveProv(res0[[1]])$reference, "surface")
})

test_that("reference = 'auto' still picks surface for a record that does visit the band", {
  tag <- .diveTag("SHALLOW", .diveTrain, zoc = TRUE)
  res <- detectDives(list(tag), control = diveControl(reference = "auto", depth.threshold = 5,
                                                      surface.band = 2, min.duration = 10),
                     verbose = FALSE)
  expect_identical(.diveProv(res[[1]])$reference, "surface")
})

test_that("min.surface.occupancy is the threshold that decides, and it is honoured exactly", {
  # 10% of samples at the surface, the rest at 30 m
  depth <- c(rep(0, 300), rep(30, 2700))
  tag   <- .diveTag("TENPC", depth, zoc = TRUE)
  ref_at <- function(occ) {
    r <- detectDives(list(tag), control = diveControl(reference = "auto", depth.threshold = 5,
                                                      surface.band = 2, min.duration = 10,
                                                      min.surface.occupancy = occ),
                     verbose = FALSE)
    .diveProv(r[[1]])$reference
  }
  expect_identical(ref_at(0.05), "surface")   # 10% clears a 5% bar
  expect_identical(ref_at(0.20), "baseline")  # 10% fails a 20% bar
})

# ---------------------------------------------------------------------------
# wiggle.amplitude must reach diveMetrics(), not stop at the settings list
# ---------------------------------------------------------------------------

test_that("detectDives records the resolved wiggle amplitude in its provenance", {
  tag <- .diveTag("W", .diveTrain)
  expect_equal(.diveProv(.detect(tag, .diveCtl(wiggle.amplitude = 3))[[1]])$wiggle_amplitude_m, 3)
  # left NULL, the derived value is recorded rather than dropped
  expect_true(is.finite(.diveProv(.detect(tag, .diveCtl())[[1]])$wiggle_amplitude_m))
})

test_that("wiggle.amplitude changes the reversal count diveMetrics() reports", {
  # one dive with a resolvable shape - a graded descent, an oscillating bottom and a graded ascent -
  # carrying 2 m wiggles: counted at a 1 m threshold, ignored at 5 m
  depth <- c(rep(0, 40),
             seq(0, 30, length.out = 60),
             rep(c(rep(30, 20), rep(28, 20)), 7),
             seq(30, 0, length.out = 60),
             rep(0, 40))
  tag   <- .diveTag("WIG", depth)

  n_rev <- function(w) {
    d <- .detect(tag, .diveCtl(wiggle.amplitude = w))
    diveMetrics(d, verbose = FALSE)$n_reversals[1]
  }
  expect_gt(n_rev(1), n_rev(5))
  # at a 5 m bar the 2 m wiggles are gone and only the dive's own apex survives as a reversal
  expect_identical(n_rev(5), 1L)
})


# ---------------------------------------------------------------------------
# verbose reporting: grouped warnings, and a summary that reports outcomes
# ---------------------------------------------------------------------------

test_that("the baseline caution is raised once for the cohort, not once per deployment", {
  # excursions filling the record: every one of these trips the median-baseline caution
  set.seed(2)
  deep <- function(id) .diveTag(id, 50 + 10 * sin(seq(0, 12 * pi, length.out = 3000)), zoc = FALSE)
  tags <- list(A = deep("A"), B = deep("B"), C = deep("C"))
  ctl  <- diveControl(reference = "baseline", depth.threshold = 5, surface.band = 2, min.duration = 10)

  w <- testthat::capture_warnings(detectDives(tags, control = ctl, verbose = FALSE))
  expect_length(w, 1L)                                   # one warning, not three
  expect_match(w, "3 of 3 deployments")                  # and it says how many it covers
  expect_match(w, "A|B|C")                               # naming the deployments inline
})

test_that("a data-quality caution is not silenced by verbose = FALSE", {
  # `verbose` governs progress reporting, not correctness signals: the old code gated this warning on
  # the verbosity level, so a quiet run silently dropped it
  set.seed(3)
  tags <- list(A = .diveTag("A", 50 + 10 * sin(seq(0, 12 * pi, length.out = 3000)), zoc = FALSE))
  ctl  <- diveControl(reference = "baseline", depth.threshold = 5, surface.band = 2, min.duration = 10)
  expect_warning(detectDives(tags, control = ctl, verbose = FALSE), "running median baseline")
})

test_that("the summary reports deployments with and without dives in plain words", {
  grab <- function(...) paste(cli::cli_fmt(suppressWarnings(detectDives(...))), collapse = "\n")
  flat <- .diveTag("FLAT", rep(0, 600))                  # no excursions at all -> no dives
  out  <- grab(list(A = .diveTag("A", .diveTrain), FLAT = flat), control = .diveCtl(), verbose = 2)

  expect_match(out, "Detection settings")                # settings live in the header now
  expect_match(out, "Depth threshold:")
  expect_match(out, "Deployments with dives:")
  expect_match(out, "Deployments without dives:")
  expect_false(grepl("applied_no_dives", out))           # the raw status string is not user-facing
  expect_false(grepl("non-standard outcomes", out))
})


# ---------------------------------------------------------------------------
# the phase rule: what it measures, over what span, and against what
# ---------------------------------------------------------------------------

# A realistic dive profile with KNOWN limbs: linear descent, flat bottom, linear ascent, built at an
# arbitrary sampling rate so the same shape can be presented to the rule several ways.
.diveProfile <- function(hz = 1, dur = 600, peak = 100, f_desc = 0.25, f_bot = 0.5, quant = 0) {
  n <- round(dur * hz); f <- seq(0, 1, length.out = n)
  f_asc <- 1 - f_desc - f_bot
  z <- ifelse(f <= f_desc, peak * f / f_desc,
       ifelse(f <= f_desc + f_bot, peak, peak * (1 - (f - f_desc - f_bot) / f_asc)))
  if (quant > 0) z <- round(z / quant) * quant
  list(z = c(rep(0, round(60 * hz)), z, rep(0, round(60 * hz))), hz = hz)
}
.phaseFrac <- function(x) {
  p <- as.character(x$dive_phase)[x$dive_id > 0]
  c(descent = mean(p == "descent"), bottom = mean(p == "bottom"), ascent = mean(p == "ascent"))
}
.runProfile <- function(pr, ...) {
  tg <- .diveTag("P", pr$z, tnum = (seq_along(pr$z) - 1) / pr$hz)
  .detect(tg, .diveCtl(...))[[1]]
}

test_that("a V-shaped dive is reported with NO bottom phase", {
  # The property that separates a rate rule from a proportion-of-depth rule. `prop.depth` labels
  # 1 - bottom.prop of EVERY dive as bottom whatever its shape; the rate rule can say "there was no
  # bottom", and on a profile with no bottom that is the only correct answer.
  v <- .diveProfile(hz = 1, f_desc = 0.5, f_bot = 0)
  fr <- .phaseFrac(.runProfile(v))
  expect_identical(unname(fr[["bottom"]]), 0)
  expect_gt(fr[["descent"]], 0.4); expect_gt(fr[["ascent"]], 0.4)

  # and the alternative rule does exactly what it is documented to do on the same profile
  fp <- .phaseFrac(.runProfile(v, phase.method = "prop.depth", bottom.prop = 0.8))
  expect_gt(fp[["bottom"]], 0.15)
})

test_that("descent and ascent are treated symmetrically: reversing a dive swaps them", {
  # The ascent is the descent of the same dive played backwards, and one routine answers both, so this
  # is a structural guarantee rather than a coincidence of two hand-written branches. The old rule had
  # a fallback on the descent branch and none on the ascent, and ascent was empty in every deployment.
  #
  # Tested on the phase rule itself rather than through detectDives(), because hysteresis is directional
  # - a dive opens above the threshold and closes below the band - so the detected SPAN of a reversed
  # profile is not the mirror of the original, and that difference belongs to detection, not to phases.
  ctl <- diveControl()
  mirror <- function(z, hz = 1) {
    tv <- (seq_along(z) - 1) / hz
    f <- nautilus:::.divePhases(z, tv, ctl, list(), noise = 0.01)$phase
    b <- nautilus:::.divePhases(rev(z), tv, ctl, list(), noise = 0.01)$phase
    list(f = f, b = rev(b))
  }
  # A unique apex, so `which.max()` picks the same sample either way and the mirror is exact - to the
  # single apex sample, which the tie-break hands to the descent whichever way the dive is played.
  swap <- c(descent = "ascent", bottom = "bottom", ascent = "descent")
  v <- c(seq(0, 100, length.out = 150), seq(100, 0, length.out = 450))[-151]
  m <- mirror(v)
  expect_lte(sum(unname(swap[m$f]) != m$b), 1L)
  expect_identical(sum(m$f == "bottom"), 0L)             # a V, both ways round

  # a flat-topped dive mirrors to within the apex tie that `which.max()` breaks by position
  u <- c(seq(0, 100, length.out = 90), rep(100, 300), seq(100, 0, length.out = 210))
  m2 <- mirror(u)
  expect_equal(sum(m2$f == "descent"), sum(m2$b == "ascent"),  tolerance = 0.02)
  expect_equal(sum(m2$f == "ascent"),  sum(m2$b == "descent"), tolerance = 0.02)
})

test_that("the same profile splits the same way at 1, 20 and 100 Hz", {
  # Sampling-rate INDEPENDENCE, which the sample-count rule did not have: it differentiated one sample
  # at a time, so the noise floor was one depth quantum per sampling interval and rose with rate, and
  # it required a run of 5% of the dive's SAMPLES, which rose with rate too.
  fr <- lapply(c(1, 20, 100), function(h) .phaseFrac(.runProfile(.diveProfile(hz = h, quant = 0.01))))
  for (i in 2:3) {
    expect_equal(fr[[i]][["descent"]], fr[[1]][["descent"]], tolerance = 0.03)
    expect_equal(fr[[i]][["ascent"]],  fr[[1]][["ascent"]],  tolerance = 0.03)
  }
})

test_that("a fast descent does not set the bar for a slow ascent", {
  # The criterion is a fraction of EACH LIMB's own rate. Pooled over the dive - the previous behaviour -
  # the fast limb sets the threshold for the slow one, and on this profile (descent 6x faster than
  # ascent) the pooled criterion lands above the true ascent rate and no ascent is ever labelled.
  pr <- .diveProfile(hz = 1, dur = 1200, peak = 120, f_desc = 0.1, f_bot = 0.3)   # ascent 6x slower
  fr <- .phaseFrac(.runProfile(pr))
  expect_gt(fr[["ascent"]], 0.45)                        # the ascent is ~60% of the dive here
  expect_gt(fr[["descent"]], 0.05)
})

test_that("the boundary hold is a duration, not a share of the dive's sample count", {
  # 5% of a 20 Hz, 600 s dive is a 600-SAMPLE unbroken run; the same rule on the same shape at 1 Hz
  # asks for 30. Expressed in seconds the demand is the same animal behaviour either way, so a long
  # dive and a short one at the same rate get the same bottom.
  short <- .phaseFrac(.runProfile(.diveProfile(hz = 20, dur = 300)))
  long  <- .phaseFrac(.runProfile(.diveProfile(hz = 20, dur = 3000)))
  expect_equal(short[["bottom"]], long[["bottom"]], tolerance = 0.03)
})

test_that("phase.window and min.phase.duration are validated, reported and recorded", {
  expect_error(diveControl(phase.window = 0), "greater than zero")
  expect_error(diveControl(phase.window = -1), "between 0 and Inf")
  expect_error(diveControl(min.phase.duration = 0), "greater than zero")
  expect_error(diveControl(phase.window = "wide"))

  out <- paste(cli::cli_fmt(detectDives(.diveTag("A", .diveTrain),
                                        control = .diveCtl(phase.window = 12),
                                        verbose = 2)), collapse = "\n")
  expect_match(out, "Phase rule:")
  expect_match(out, "Rate window:\\s+12 s \\(user\\)")
  expect_match(out, "Min. phase:")
  expect_match(out, "Phase structure")

  x <- .detect(.diveTag("A", .diveTrain), .diveCtl(phase.window = 12))[[1]]
  expect_equal(.diveProv(x)$phase_window_s, 12)
  expect_equal(.diveProv(x)$min_phase_duration_s, 24)    # derived as twice the window

  # the rate scales are not recorded for a rule that does not use them
  y <- .detect(.diveTag("A", .diveTrain), .diveCtl(phase.method = "prop.depth"))[[1]]
  expect_true(is.na(.diveProv(y)$phase_window_s))
})

test_that("a whole missing limb is warned about, and a missing bottom is not", {
  # The check that would have caught the failure it exists because of: a phase rule that cannot see a
  # limb produces a perfectly well-formed table in which `ascent` never appears, and nothing else says
  # so. An empty BOTTOM is the opposite - it is the correct answer on a V-dive.
  v <- .diveProfile(hz = 1, f_desc = 0.5, f_bot = 0)
  expect_silent(suppressMessages(detectDives(
    .diveTag("V", rep(c(v$z, rep(0, 30)), 3)), control = .diveCtl(), verbose = 0)))

  # The tally is what the warning reads, so its rules are pinned here: fires above half the judged
  # dives, names the deployment and the count, stays quiet at or below half, and abstains rather than
  # accuses on dives the record cut short or on which the rule returned no answer at all.
  tally <- function(...) nautilus:::.divePhaseTally(list(...))
  dive <- function(d = TRUE, a = TRUE, trunc = FALSE)
    list(descent_established = d, ascent_established = a, truncated = trunc, structure = "DBA")
  ctl <- diveControl()

  bad <- c(tally(dive(a = FALSE), dive(a = FALSE), dive()), list(id = "X"))
  expect_warning(nautilus:::.warnDivePhases(list(bad), ctl), "No ascent phase was resolved")
  expect_warning(nautilus:::.warnDivePhases(list(bad), ctl), "X \\(2/3 dives\\)")

  half <- c(tally(dive(a = FALSE), dive()), list(id = "X"))       # exactly half is not "more than"
  expect_silent(nautilus:::.warnDivePhases(list(half), ctl))

  # truncated dives and NA answers leave nothing to judge, so nothing is claimed
  cut <- c(tally(dive(a = FALSE, trunc = TRUE), dive(a = FALSE, trunc = TRUE)), list(id = "X"))
  expect_silent(nautilus:::.warnDivePhases(list(cut), ctl))
  abst <- c(tally(dive(d = NA, a = NA), dive(d = NA, a = NA)), list(id = "X"))
  expect_silent(nautilus:::.warnDivePhases(list(abst), ctl))
})

test_that(".diveSlope matches a direct least-squares fit and tolerates gaps", {
  set.seed(11)
  n <- 400; tv <- seq_len(n) * 0.5; z <- 0.03 * tv + stats::rnorm(n, 0, 0.2)
  W <- 10; k <- floor((W / 2) / 0.5)
  brute <- vapply(seq_len(n), function(i) {
    j <- max(1L, i - k):min(n, i + k)
    unname(stats::coef(stats::lm(z[j] ~ tv[j]))[2])
  }, numeric(1))
  expect_equal(nautilus:::.diveSlope(z, tv, W), brute, tolerance = 1e-8)

  # NA depth carries no information and must not poison the whole window
  z2 <- z; z2[c(50:55, 200)] <- NA_real_
  s2 <- nautilus:::.diveSlope(z2, tv, W)
  expect_true(all(is.finite(s2[100:150])))
  expect_equal(mean(s2, na.rm = TRUE), 0.03, tolerance = 0.02)

  # THE property the one-sample difference had backwards. At a fixed depth quantum, dividing one
  # quantum by one sampling interval gives a noise floor that RISES with sampling rate; a slope over a
  # fixed span in seconds falls as rate^-0.5. Same true rate, same quantum, three sampling rates.
  err <- function(hz, how) {
    tt <- seq_len(round(600 * hz)) / hz
    zz <- round((0.37 * tt) / 0.05) * 0.05               # 5 cm quantum, 0.37 m/s, not a whole number of
    r <- if (how == "slope") nautilus:::.diveSlope(zz, tt, 10)  # quanta per sample at any of these rates
         else c(NA, diff(zz) / diff(tt))
    stats::sd(r, na.rm = TRUE)
  }
  expect_lt(err(100, "slope"), err(20, "slope"))
  expect_lt(err(20,  "slope"), err(1,  "slope"))
  expect_gt(err(100, "diff"),  err(20, "diff"))          # the old estimator, going the wrong way
  expect_lt(err(20,  "slope"), err(20, "diff") / 50)     # and two orders of magnitude apart at 20 Hz
})

test_that(".diveQuantum finds a lattice step and abstains when there is no lattice", {
  expect_equal(nautilus:::.diveQuantum(round(stats::runif(500, 0, 50) / 0.05) * 0.05), 0.05)
  expect_true(is.na(nautilus:::.diveQuantum(stats::runif(500, 0, 50))))    # not a lattice
  expect_true(is.na(nautilus:::.diveQuantum(rep(c(0, 25), 100))))          # too few levels to tell
})


# ---------------------------------------------------------------------------
# defects found by adversarial review of the phase rule
# ---------------------------------------------------------------------------

test_that("a deployment's phases do not depend on who else is in the call", {
  # The window is a span in SECONDS and has to be converted to samples on the record it is applied to.
  # Fed the COHORT median interval, a 20 Hz deployment sharing a call with two 1 Hz ones was measured
  # over a 20x wider span and reported a different phase structure than the same deployment alone -
  # cohort composition silently changing a per-deployment scientific result.
  mk <- function(id, hz) {
    leg <- function(a, b, s) seq(a, b, length.out = round(s * hz))
    one <- c(leg(0, 40, 40), rep(40, round(200 * hz)), leg(40, 0, 40), rep(0, round(60 * hz)))
    .diveTag(id, c(rep(0, round(60 * hz)), rep(one, 3)),
             tnum = (seq_len(round(60 * hz) + 3 * length(one)) - 1) / hz)
  }
  ctl <- .diveCtl(min.duration = 20)
  alone <- .phaseFrac(.detect(mk("H", 20), ctl)[[1]])
  mixed <- .phaseFrac(.detect(list(mk("H", 20), mk("A", 1), mk("B", 1)), ctl)[[1]])
  expect_identical(alone, mixed)
})

test_that("a single NA timestamp at the end of a dive does not swallow the bottom phase", {
  # Reversed time used to be measured from `tnum[m]`; one NA there made every reversed timestamp NA, so
  # no hold could be met, the ascent never resolved, and its label spread back over the whole bottom -
  # with ascent_established still TRUE, so nothing flagged it.
  z <- c(rep(0, 60), seq(0, 60, length.out = 60), rep(60, 300), seq(60, 0, length.out = 60), rep(0, 60))
  tv <- seq_along(z) - 1
  ph <- function(t) {
    p <- nautilus:::.divePhases(z, t, diveControl(), list(), noise = 0.02, dt = 1)$phase
    c(sum(p == "descent"), sum(p == "bottom"), sum(p == "ascent"))
  }
  bad <- tv; bad[length(bad)] <- NA_real_
  expect_identical(ph(tv), ph(bad))
  expect_gt(ph(bad)[2], 250)
})

test_that("a limb is not 'established' by instrument noise alone", {
  # `established` is what .warnDivePhases() reads to decide whether a limb was ever there, so a limb
  # that noise alone can establish silences the check. Two guards: a floor at THREE standard errors of
  # the slope (at one, a Gaussian clears it 16% of the time) and a start that must persist for one
  # window rather than for one sample. A flat noisy record used to report a descent in 99% of draws.
  set.seed(9)
  est <- replicate(60, {
    z <- 20 + stats::rnorm(600, 0, 0.05)
    r <- nautilus:::.divePhases(z, seq_len(600), diveControl(), list(), noise = 0.05, dt = 1)
    c(isTRUE(r$descent_established), isTRUE(r$ascent_established))
  })
  expect_lt(mean(est), 0.05)

  # and a real limb is still established on the same noise
  v <- c(seq(0, 60, length.out = 200), seq(60, 0, length.out = 400)) + stats::rnorm(600, 0, 0.05)
  r <- nautilus:::.divePhases(v, seq_len(600), diveControl(), list(), noise = 0.05, dt = 1)
  expect_true(r$descent_established); expect_true(r$ascent_established)
})

test_that("prop.depth abstains on whether a limb was seen, rather than answering structurally", {
  # It partitions geometry and detects nothing, so it has no opinion. Answering it structurally also
  # answered it wrongly: a dive whose absolute hysteresis threshold already exceeds `bottom.prop` of its
  # own amplitude starts with its first sample already "deep", so descent came out FALSE by
  # construction and every shallow-dive cohort drew a warning naming arguments this rule does not use.
  shallow <- rep(c(rep(0, 40), seq(0, 6, length.out = 20), rep(6, 60), seq(6, 0, length.out = 20)), 4)
  r <- nautilus:::.divePhases(c(seq(0, 6, length.out = 20), rep(6, 60), seq(6, 0, length.out = 20)),
                              seq_len(100), diveControl(phase.method = "prop.depth"), list())
  expect_true(is.na(r$descent_established)); expect_true(is.na(r$ascent_established))
  expect_silent(suppressMessages(detectDives(.diveTag("S", shallow),
    control = .diveCtl(phase.method = "prop.depth"), verbose = 0)))
})

test_that("a user-set min.phase.duration survives adaptive widening", {
  # Widening lifts a DERIVED hold with the window, because a hold shorter than the window it is
  # measured over is no evidence. A number the user chose is the shortest bottom they want reported, so
  # the window is capped at it instead - otherwise their bottoms vanish while the console still prints
  # the value they asked for.
  set.seed(5)
  z <- c(seq(0, 10, length.out = 100), rep(10, 40), seq(10, 0, length.out = 100)) +
       stats::rnorm(240, 0, 0.6)
  bot <- function(h) {
    s <- list(phase.window = 5, min.phase.duration = h, phase_duration_source = "user")
    sum(nautilus:::.divePhases(z, seq_len(240), diveControl(), s, noise = 0.6, dt = 1)$phase == "bottom")
  }
  expect_gt(bot(6), 0)                                   # a short user hold still finds the bottom
  expect_gte(bot(6), bot(30))                            # and a long one is not more permissive
})


test_that("a hesitation on the way down does not end the descent", {
  # A rate criterion alone cannot say WHICH pause ended the limb, because `crit` is a fraction of a
  # quantile over the whole limb, so the fast part of a limb sets the bar for the slow part of the same
  # limb. Taking the first sustained pause ended the descent wherever the animal hesitated: on a real
  # 1414 m dive it ended after 58 s at 9.7 m, leaving a continuous 0.76 m/s plunge labelled bottom, and
  # across 52 deployments the bottom spanned a median 81% of its dive's depth range. A pause now has to
  # be an ARRIVAL - the animal already within `1 - bottom.prop` of this limb's deepest point.
  hz <- 1
  step <- c(seq(0, 60, length.out = 60),     # descend to 60 m ...
            rep(60, 40),                     # ... hesitate for 40 s, far short of the apex ...
            seq(60, 400, length.out = 200),  # ... then continue to 400 m
            rep(400, 150),                   # the real bottom
            seq(400, 0, length.out = 200))
  tg <- .diveTag("STEP", c(rep(0, 40), step, rep(0, 40)), tnum = (seq_len(730) - 1) / hz)
  x <- .detect(tg, .diveCtl(min.duration = 30))[[1]]
  d <- as.numeric(x$depth)[x$dive_id > 0]; p <- as.character(x$dive_phase)[x$dive_id > 0]

  expect_true(all(c("descent", "bottom", "ascent") %in% p))
  # the descent reaches the real apex, not the hesitation depth
  expect_gt(max(d[p == "descent"]), 300)
  # and the bottom is a bottom: it spans a small part of the dive's depth range
  expect_lt(diff(range(d[p == "bottom"])) / max(d), 0.25)
  # the mid-descent hesitation is not labelled bottom
  expect_false(any(p[d > 55 & d < 70] == "bottom"))
})

test_that("the arrival tolerance is bottom.prop, and it bounds the bottom's depth span", {
  # bottom.prop governs both rules: outright for prop.depth, as the arrival test for vertical.rate.
  z <- c(rep(0, 30), seq(0, 100, length.out = 80), rep(100, 30), seq(100, 300, length.out = 120),
         rep(300, 100), seq(300, 0, length.out = 150), rep(0, 30))
  tg <- .diveTag("ARR", z, tnum = seq_along(z) - 1)
  span <- function(bp) {
    x <- .detect(tg, .diveCtl(min.duration = 30, bottom.prop = bp))[[1]]
    d <- as.numeric(x$depth)[x$dive_id > 0]; p <- as.character(x$dive_phase)[x$dive_id > 0]
    if (!any(p == "bottom")) return(0)
    diff(range(d[p == "bottom"])) / max(d)
  }
  expect_lt(span(0.80), 0.25)
  # a laxer tolerance lets the bottom begin further up, so it spans more depth; a stricter one less
  expect_gte(span(0.50), span(0.80))
  expect_lte(span(0.95), span(0.80))
})
