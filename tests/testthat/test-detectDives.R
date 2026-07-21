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
