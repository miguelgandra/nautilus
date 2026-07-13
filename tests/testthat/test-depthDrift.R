# Tests for the depth zero-offset drift correction: depthDriftControl() + the internal
# .correctDepthDrift()/.gatherSurfaceEvidence() (the surface-anchored ZOC).
# Fixtures are synthetic (a known linear drift + known surface bouts); the real-Wildlife-Computers
# validation (reproduces their `Corrected Depth` to within one LSB) is recorded in AUDIT.md, not
# embedded here (research data / CRAN portability). The WC-characteristics case below reproduces the
# key validated property synthetically: dry-time depth -> ~0 within one resolution step.

# a dive profile (surface bouts at 0 m, dives at 20 m) with a linear zero-offset drift added, plus a
# transition-encoded dry signal covering the surface bouts.
.mkDrift <- function(hours = 4, surf_every = 1800, surf_dur = 120, drift_max = 1.5, res = NULL, fs = 1) {
  n  <- hours * 3600 * fs
  t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
  dt <- t0 + (seq_len(n) - 1) / fs
  sec <- (seq_len(n) - 1) / fs
  at_surf <- (sec %% surf_every) < surf_dur
  true  <- ifelse(at_surf, 0, 20)
  drift <- seq(0, drift_max, length.out = n)
  depth <- true + drift
  if (!is.null(res)) depth <- round(depth / res) * res            # WC-like quantization
  ch  <- c(TRUE, at_surf[-1] != at_surf[-n])                       # transition points
  dry <- data.frame(datetime = dt[ch], dry = at_surf[ch])
  list(depth = depth, datetime = dt, dry = dry, true = true, drift = drift, at_surf = at_surf)
}
.cdd <- function(...) nautilus:::.correctDepthDrift(...)

test_that("depthDriftControl(): defaults and validation", {
  d <- depthDriftControl()
  expect_s3_class(d, "nautilus_depth_drift")
  expect_equal(d$method, "surface")
  expect_equal(d$surface.evidence, c("dry", "gps"))
  expect_equal(d$min.dry.duration, 3); expect_equal(d$max.gap, 6); expect_equal(d$min.anchors, 2)
  expect_error(depthDriftControl(method = "bogus"))
  expect_error(depthDriftControl(surface.evidence = "bogus"), "invalid")     # "depth" is now valid (shallow mode)
  expect_error(depthDriftControl(surface.evidence = character(0)), "at least one")
  expect_error(depthDriftControl(min.dry.duration = -1))
  expect_error(depthDriftControl(min.anchors = 0))
})

test_that("depthDriftControl coerces from a named list via .as_control", {
  x <- nautilus:::.as_control(list(max.gap = 12), depthDriftControl, "nautilus_depth_drift", "depth")
  expect_s3_class(x, "nautilus_depth_drift"); expect_equal(x$max.gap, 12)
  expect_error(nautilus:::.as_control(list(bogus = 1), depthDriftControl, "nautilus_depth_drift", "depth"), "unknown")
})

test_that("surface ZOC recovers a linear drift from dry anchors", {
  s <- .mkDrift(hours = 4, drift_max = 1.5)
  r <- .cdd(s$depth, s$datetime, dry = s$dry)
  expect_equal(r$status, "applied")
  expect_gte(r$n_anchors, 6L)
  expect_gt(cor(r$offset, s$drift), 0.99)                         # recovers the drift shape
  expect_lt(mean(abs(r$depth - s$true)), 0.1)                     # corrected ~ true depth
  expect_lt(r$outcome$residual_m, 0.05)                          # ~0 at the surface
  expect_gt(r$outcome$offset_range_m[2], 1.2)
  expect_equal(as.integer(r$anchors["dry"]), r$n_anchors)
})

test_that("surface ZOC works from GPS fixes alone", {
  s <- .mkDrift(hours = 4, drift_max = 1.5)
  starts <- which(diff(c(FALSE, s$at_surf)) == 1)                 # bout starts
  pos <- data.frame(datetime = s$datetime[starts] + 60, type = "FastGPS")   # a fix mid-bout
  r <- .cdd(s$depth, s$datetime, positions = pos, control = depthDriftControl(surface.evidence = "gps"))
  expect_equal(r$status, "applied")
  expect_lt(mean(abs(r$depth - s$true)), 0.1)
  expect_gt(as.integer(r$anchors["gps"]), 2L)
  expect_equal(as.integer(r$anchors["dry"]), 0L)                 # dry not consulted
})

test_that("abstains (depth untouched) when there is no surface evidence", {
  s <- .mkDrift()
  r <- .cdd(s$depth, s$datetime)                                  # no dry, no positions
  expect_equal(r$status, "abstained")
  expect_identical(r$depth, s$depth)
  expect_true(all(r$offset == 0))
  expect_true(is.na(r$outcome$residual_m))
})

test_that("a single anchor yields a constant-offset correction", {
  s <- .mkDrift(hours = 1, surf_every = 3600, surf_dur = 120, drift_max = 0.5)   # one bout only
  r <- .cdd(s$depth, s$datetime, dry = s$dry)
  expect_equal(r$status, "constant_offset")
  expect_equal(r$n_anchors, 1L)
  expect_equal(length(unique(r$offset)), 1L)
})

test_that("a gap wider than max.gap flags low-confidence spans (applied_with_gaps)", {
  n <- 8 * 3600; t0 <- as.POSIXct("2020-01-01", tz = "UTC"); dt <- t0 + 0:(n - 1); sec <- 0:(n - 1)
  at_surf <- (sec < 3600) & (sec %% 600 < 60)                    # surfacings only in the first hour
  depth <- ifelse(at_surf, 0, 20) + seq(0, 2, length.out = n)
  ch <- c(TRUE, at_surf[-1] != at_surf[-n]); dry <- data.frame(datetime = dt[ch], dry = at_surf[ch])
  r <- .cdd(depth, dt, dry = dry)
  expect_equal(r$status, "applied_with_gaps")
  expect_gt(r$max_gap_h, 6)
  expect_gt(nrow(r$low_confidence), 0L)
  expect_s3_class(r$low_confidence$start, "POSIXct")
})

test_that("WC-like quantized depth: dry-time residual within one resolution step", {
  s <- .mkDrift(hours = 6, drift_max = 1.0, res = 0.5)            # 0.5 m LSB, ~1 m drift
  r <- .cdd(s$depth, s$datetime, dry = s$dry)
  expect_equal(r$status, "applied")
  expect_lte(unname(quantile(abs(r$depth[s$at_surf]), 0.95)), 0.5)   # corrected surface depth <= 1 LSB
  expect_lte(r$outcome$residual_m, 0.5)
})

test_that("method='none' disables the correction (depth untouched)", {
  s <- .mkDrift()
  r <- .cdd(s$depth, s$datetime, dry = s$dry, control = depthDriftControl(method = "none"))
  expect_equal(r$status, "disabled")
  expect_identical(r$depth, s$depth)
})

test_that("the nautilus meta schema carries an empty ancillary slot", {
  m <- nautilus:::.newNautilusMeta()
  expect_true("ancillary" %in% names(m))
  expect_type(m$ancillary, "list")
  expect_length(m$ancillary, 0L)
})

test_that(".transitionEncode is lossless and round-trips through .dryIntervals", {
  t0 <- as.POSIXct("2020-01-01", tz = "UTC"); dt <- t0 + 0:19
  state <- c(rep(FALSE, 5), rep(TRUE, 5), rep(FALSE, 4), rep(TRUE, 6))   # two TRUE runs
  enc <- nautilus:::.transitionEncode(dt, state)
  expect_named(enc, c("datetime", "state"))
  expect_lt(nrow(enc), length(state))                                    # compressed
  dry <- data.frame(datetime = enc$datetime, dry = enc$state)            # ancillary$dry$data shape
  iv  <- nautilus:::.dryIntervals(dry, end_time = dt[20] + 1, min.dry.duration = 1)
  expect_equal(nrow(iv), 2L)                                             # both dry runs recovered
  expect_equal(as.numeric(iv$start[1]), as.numeric(dt[6]))
})

# --- shallow mode: opt-in surface inference from the depth trace ---------------------------------

test_that("depthDriftControl: shallow-mode 'depth' source + params validate", {
  d <- depthDriftControl(surface.evidence = "depth")
  expect_equal(d$surface.evidence, "depth")
  expect_equal(d$surface.quantile, 0.05); expect_equal(d$surface.band, 2)
  expect_error(depthDriftControl(surface.quantile = 1.5))
  expect_error(depthDriftControl(surface.band = -1))
})

test_that("shallow mode ('depth') recovers drift from the depth trace alone", {
  s <- .mkDrift(hours = 4, surf_dur = 180, drift_max = 1.5)              # 10% at surface (0 m) + dives (20 m)
  r <- .cdd(s$depth, s$datetime, control = depthDriftControl(surface.evidence = "depth"))
  expect_equal(r$status, "applied")
  expect_gt(as.integer(r$anchors["depth"]), 3L)                         # depth-derived anchors
  expect_gt(cor(r$offset, s$drift), 0.9)                                # recovers the drift shape
  expect_lt(mean(abs(r$depth - s$true)), 0.5)                           # corrected ~ true depth
})

test_that("shallow mode is opt-in: the default evidence ignores the depth trace", {
  s <- .mkDrift(hours = 4, surf_dur = 180)
  r <- .cdd(s$depth, s$datetime)                                        # default c('dry','gps'); no dry/fixes
  expect_equal(r$status, "abstained")                                   # depth trace not consulted
})

test_that("shallow mode fills gaps only where independent (dry) evidence is absent", {
  s <- .mkDrift(hours = 4, surf_dur = 180)
  # dry covering only the first two surface bouts (sec < 2400); depth should fill the rest
  early <- s$at_surf & ((seq_along(s$at_surf) - 1) < 2400)
  ch <- c(TRUE, early[-1] != early[-length(early)])
  dry_early <- data.frame(datetime = s$datetime[ch], dry = early[ch])
  r <- .cdd(s$depth, s$datetime, dry = dry_early,
            control = depthDriftControl(surface.evidence = c("dry", "depth")))
  expect_equal(r$status, "applied")
  expect_gt(as.integer(r$anchors["dry"]), 0L)
  expect_gt(as.integer(r$anchors["depth"]), 0L)                         # depth fills the later gap
})

test_that("a 'surface' fix that lands on a dive is rejected, not used as a deep zero-offset (over-correction bug)", {
  # regression: real GPS/dry 'surface' fixes frequently land on dives (mis-timed fix / coarse wet-dry sensor),
  # reading tens of metres. That is NOT the sensor zero drift; using it over-corrected depth well ABOVE the
  # (impossible) surface. An anchor is now valid only when it reads near the animal's shallowest level.
  s <- .mkDrift(hours = 4, drift_max = 1)                         # surface bouts at ~0 m, dives at ~20 m
  surf_time <- s$datetime[which(s$at_surf)[50]]                   # a genuine surfacing
  dive_time <- s$datetime[which(!s$at_surf)[500]]                 # a mis-timed fix -> lands on a ~20 m dive
  pos <- data.frame(datetime = c(surf_time, dive_time), type = "FastGPS", lon = 0, lat = 0,
                    stringsAsFactors = FALSE)
  ctrl <- depthDriftControl(surface.evidence = "gps")
  ev <- nautilus:::.gatherSurfaceEvidence(s$depth, s$datetime, dry = NULL, positions = pos, control = ctrl)
  expect_true(all(ev$anchors$offset < 5))                        # the ~20 m dive fix never becomes an anchor
  expect_false(any(ev$at_surface & s$depth > 10))                # nor does its dive window count as "at surface"
  r <- .cdd(s$depth, s$datetime, positions = pos, control = ctrl)
  expect_gt(min(r$depth, na.rm = TRUE), -(ctrl$surface.band + 1)) # correction never flies above the surface
  expect_lt(r$outcome$residual_m, 2)                             # residual reflects genuine surface samples only
})

test_that("when EVERY surface fix lands on a dive, the correction abstains rather than over-correcting", {
  s <- .mkDrift(hours = 4, drift_max = 1)
  pos <- data.frame(datetime = s$datetime[which(!s$at_surf)[c(200, 600, 1000)]],   # all fixes during dives
                    type = "FastGPS", lon = 0, lat = 0, stringsAsFactors = FALSE)
  r <- .cdd(s$depth, s$datetime, positions = pos, control = depthDriftControl(surface.evidence = "gps"))
  expect_equal(r$status, "abstained")                            # no valid evidence -> leave depth untouched
  expect_equal(r$depth, s$depth)
})

test_that(".depthDriftDiag formats the scannable verbose line across statuses", {
  f  <- nautilus:::.depthDriftDiag
  dd <- function(status, off, resid, n) list(status = status, n_anchors = n,
                                             outcome = list(offset_range_m = off, residual_m = resid))
  expect_null(f(dd("disabled", c(0, 0), NA, 0L)))                                    # off -> no line
  expect_equal(f(dd("abstained", c(0, 0), NA, 0L)), "depth drift: skipped (no surface evidence)")
  # magnitude leads; anchors + residual parenthesised ("." matches the en-dash to keep this source ASCII)
  expect_match(f(dd("applied", c(1, 47.8), 61.53, 7L)), "^depth drift: 1 . 47.8 m \\(7 anchors; residual 61.5 m\\)$")
  expect_equal(f(dd("constant_offset", c(6.16, 6.16), 0.1, 2L)), "depth drift: 6.2 m (2 anchors; residual 0.1 m)")
  gaps <- f(dd("applied_with_gaps", c(6.2, 6.2), NA, 2L))
  expect_match(gaps, "gaps"); expect_false(grepl("residual", gaps))                  # gaps flag; NA residual omitted
  expect_false(grepl("applied", f(dd("applied", c(1, 48), 0.1, 3L))))                # the word "applied" is gone
  expect_match(f(dd("applied", c(3, 3), 0.1, 1L)), "1 anchor;")                      # singular "anchor"
})
