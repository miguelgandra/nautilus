# Tests for processTagData() fixes (tilt_compass path; no Python required):
#   PT1 - pitch-offset attributes must survive downsampling (were set on the wrong object)
#   PT2 - downsampling bins by seconds
#   PT3 - verbose = FALSE is silent
#   plus a basic end-to-end smoke test of the produced metrics

# synthetic level-swimming full-IMU data at `rate` Hz for `secs` seconds
.mk <- function(id = "A01", secs = 60, rate = 10) {
  set.seed(1)
  n <- secs * rate
  t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
  dt <- t0 + (seq_len(n) - 1) / rate
  d <- data.table::data.table(
    ID = id, datetime = dt,
    ax = rnorm(n, 0, 0.02), ay = rnorm(n, 0, 0.02), az = 1 + rnorm(n, 0, 0.02),
    gx = rnorm(n, 0, 0.01), gy = rnorm(n, 0, 0.01), gz = rnorm(n, 0, 0.01),
    mx = 0.3 + rnorm(n, 0, 0.01), my = rnorm(n, 0, 0.01), mz = 0.4 + rnorm(n, 0, 0.01),
    depth = 10 + 5 * sin(seq_len(n) / n * pi), temp = 20,
    # position columns added by importTagData (all NA here; declination uses deployment.info attr)
    PTT = NA_character_, position_type = NA_character_,
    lat = NA_real_, lon = NA_real_, quality = NA_character_
  )
  data.table::setattr(d, "nautilus.version", "test")
  data.table::setattr(d, "deployment.info",
                      data.frame(datetime = t0, lon = -25, lat = 11))   # magnetic equator (IGRF dip ~0): synthetic fields have dip ~0, so the IGRF-aware calibration is consistent
  d
}

# synthetic DIVING data: body pitch tracks vertical velocity around a known mounting offset, so the
# Kawatsu pitch-offset regression has a strong fit (used to exercise the pitch-offset guard / PT1)
.mk_diving <- function(id = "A01", secs = 300, rate = 10, mount = 15) {
  set.seed(3)
  n <- secs * rate
  t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
  dt <- t0 + (seq_len(n) - 1) / rate
  depth <- 30 + 20 * sin(2 * pi * seq_len(n) / (rate * 60))      # 60-s dive cycles
  vv <- c(0, diff(depth)) * rate                                 # approx vertical velocity (m/s)
  theta <- (mount * pi / 180) + 0.15 * vv                        # body pitch = mounting offset + dive term (rad)
  d <- data.table::data.table(
    ID = id, datetime = dt,
    ax = -sin(theta) + rnorm(n, 0, 0.01), ay = rnorm(n, 0, 0.01), az = cos(theta) + rnorm(n, 0, 0.01),
    gx = rnorm(n, 0, 0.01), gy = rnorm(n, 0, 0.01), gz = rnorm(n, 0, 0.01),
    mx = 0.3 + rnorm(n, 0, 0.01), my = rnorm(n, 0, 0.01), mz = 0.4 + rnorm(n, 0, 0.01),
    depth = depth, temp = 20,
    PTT = NA_character_, position_type = NA_character_, lat = NA_real_, lon = NA_real_, quality = NA_character_
  )
  data.table::setattr(d, "nautilus.version", "test")
  data.table::setattr(d, "deployment.info", data.frame(datetime = t0, lon = -25, lat = 11))   # magnetic equator (IGRF dip ~0): synthetic fields have dip ~0, so the IGRF-aware calibration is consistent
  d
}

.run <- function(d, ...) {
  res <- NULL
  invisible(capture.output(suppressWarnings(suppressMessages(
    res <- processTagData(d, verbose = FALSE, ...)))))
  res
}

# the processTagData step record from the consolidated-metadata audit trail (provenance now lives
# in the `nautilus` meta object, not in parallel flat attributes)
.proc_rec <- function(x) {
  recs <- Filter(function(p) identical(p$step, "processTagData"), nautilus:::.getMeta(x)$processing)
  recs[[length(recs)]]
}

test_that("use.stored applies a high-confidence stored magnetometer calibration, ignores a low one", {
  inject <- function(confidence, axis_net = NULL) {
    tg <- .mk()                                                    # raw data: axis_mapping$net is NULL
    m  <- nautilus:::.getMeta(tg)
    m$deployment$lon <- -25; m$deployment$lat <- 38                # preserve coords for declination
    m$deployment$datetime <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
    mc <- nautilus:::.newMagCalibrationMeta()                        # nested contract; calibrateMagnetometer writes `proposed`
    mc$proposed <- list(
      params     = list(center = c(0.3, 0, 0.4), soft_iron = diag(3), axis_net = axis_net),
      qc         = list(confidence = confidence, coverage_ok = TRUE, radcv = 0.02,
                        igrf_residual = NA_real_, axis_span = rep(170, 3)),
      provenance = list(method = "ellipsoid", source = "per_package", group = "PKG", n_deployments = 3L))
    m$mag_calibration <- mc
    nautilus:::.restoreMeta(tg, m)
  }
  rec <- function(...) {
    o <- .run(...); if (is.list(o) && !is.data.frame(o)) o <- o[[1]]
    .proc_rec(o)
  }
  expect_match(rec(inject("high"))$calibration_source, "^stored per_package")   # same (raw) frame -> applied
  # the low/opt-out/frame-mismatch cases fall back to the INLINE engine (which now calibrates the band itself
  # -> "inline <status>"); the point is only that the STORED fit was NOT used.
  expect_match(rec(inject("low"))$calibration_source, "^inline")                # low confidence -> stored not used
  expect_match(rec(inject("high"), calibration = calibrationControl(use.stored = FALSE))$calibration_source, "^inline")   # opt-out
  # estimated in a DIFFERENT axis frame than the data now carries -> not applied (falls back to inline)
  expect_match(rec(inject("high", axis_net = list(accel = diag(3))))$calibration_source, "^inline")
})

test_that("processTagData records the calibration state machine (status / applied / trust) and is idempotent", {
  prop <- list(params     = list(center = c(0.3, 0, 0.4), soft_iron = matrix(c(1, .05, 0, .05, 1, 0, 0, 0, 1), 3), axis_net = NULL),
               qc         = list(confidence = "high", coverage_ok = TRUE, radcv = 0.02, igrf_residual = 1, axis_span = rep(170, 3)),
               provenance = list(method = "ellipsoid", source = "per_package"))
  tg <- .mk(); m <- nautilus:::.getMeta(tg); m$axis_mapping$applied <- TRUE; m$mag_calibration$proposed <- prop
  m$deployment$lon <- -25; m$deployment$lat <- 38; m$deployment$datetime <- as.POSIXct("2020-01-01", tz = "UTC")
  tg <- nautilus:::.restoreMeta(tg, m)
  o  <- suppressWarnings(.run(list(A01 = tg), downsample.to = NULL)$A01)
  mc <- nautilus:::.getMeta(o)$mag_calibration
  expect_equal(mc$status, "calibrated_3d")
  expect_true(isTRUE(mc$applied))                                       # applied flag flipped by processTagData
  expect_equal(mc$applied_params$center, c(0.3, 0, 0.4))               # the exact transform used is recorded
  expect_equal(nautilus:::.headingTrust(list(mag_calibration = mc)), "trusted")
  expect_equal(mc$proposed$params$center, c(0.3, 0, 0.4))              # proposed estimate untouched by the applier
  # idempotency: a second run must NOT re-apply the calibration (double-correction would re-subtract the
  # ~0.3 uT hard-iron centre; the tiny residual diff is only the paddle de-noise re-running)
  o2  <- suppressWarnings(suppressMessages(.run(list(A01 = o), downsample.to = NULL)$A01))
  mc2 <- nautilus:::.getMeta(o2)$mag_calibration
  expect_true(isTRUE(mc2$applied)); expect_equal(mc2$status, "calibrated_3d")   # state preserved
  expect_lt(max(abs(o2$mx - o$mx), na.rm = TRUE), 0.05)                          # NOT re-centred (<< |centre| 0.3)
})

test_that("a genuinely unobservable field (a single heading held) is left raw + flagged uncalibrated_raw (loud)", {
  # a near-1-D arc (~17 deg of yaw held): the in-plane centre is unconstrained in TWO directions, so the
  # engine's abort gate fires (recommend_apply = FALSE) regardless of IGRF - the field is left raw.
  set.seed(9); n <- 1200; t0 <- as.POSIXct("2020-01-01", tz = "UTC"); ph <- seq(0, 0.3, length.out = n)
  d <- data.table::data.table(ID = "A", datetime = t0 + (seq_len(n) - 1) / 10,
       ax = rnorm(n, 0, 0.02), ay = rnorm(n, 0, 0.02), az = 1 + rnorm(n, 0, 0.02),
       gx = rnorm(n, 0, 0.01), gy = rnorm(n, 0, 0.01), gz = rnorm(n, 0, 0.01),
       mx = cos(ph) * 50 + 5, my = sin(ph) * 50 - 3, mz = 2 + rnorm(n, 0, 0.05),
       depth = 10 + 5 * sin(seq_len(n) / n * pi), temp = 20)
  m <- nautilus:::.newNautilusMeta(); m$id <- "A"; m$axis_mapping$applied <- TRUE
  m$deployment$lon <- -25; m$deployment$lat <- 11; m$deployment$datetime <- t0
  tg <- nautilus:::new_nautilus_tag(d, m)
  ws <- character(0)
  o <- withCallingHandlers(processTagData(list(A = tg), verbose = FALSE),
                           warning = function(w) { ws <<- c(ws, conditionMessage(w)); invokeRestart("muffleWarning") })$A
  mc <- nautilus:::.getMeta(o)$mag_calibration
  expect_equal(mc$status, "uncalibrated_raw")
  expect_false(isTRUE(mc$applied))
  expect_equal(nautilus:::.headingTrust(list(mag_calibration = mc)), "untrusted")
  expect_true(any(grepl("NO magnetometer calibration", ws)))          # loud, default-level warning
})

test_that("tilt_compass run produces the expected motion/orientation metrics", {
  out <- .run(list(A01 = .mk()), orientation.algorithm = "tilt_compass", downsample.to = NULL)$A01
  expect_false(is.null(out))
  expect_true(all(c("accel", "odba", "vedba", "surge", "sway", "heave",
                    "roll", "pitch", "heading", "vertical_velocity", "turning_angle") %in% names(out)))
  # level swimming -> pitch and roll near 0
  expect_lt(abs(median(out$pitch, na.rm = TRUE)), 10)
  expect_lt(abs(median(out$roll, na.rm = TRUE)), 10)
})

test_that("downsampling to 1 Hz reduces rows and keeps pitch-offset provenance (PT1/PT2)", {
  out <- .run(list(A01 = .mk_diving(secs = 120, rate = 10)),
              orientation.algorithm = "tilt_compass", downsample.to = 1)$A01
  expect_lte(nrow(out), 130)                      # ~120 one-second bins (was 1200 rows)
  expect_equal(nautilus:::.getMeta(out)$sensors$sampling_hz_processed, 1)
  # PT1: the pitch-offset provenance is on the RETURNED (downsampled) object's metadata, not lost
  expect_false(is.na(.proc_rec(out)$pitch_offset_deg))
})

test_that("pitch-offset guard: applies a strong Kawatsu fit, skips a weak one (PT12)", {
  on <- .run(list(A01 = .mk_diving(mount = 15)), orientation.algorithm = "tilt_compass", downsample.to = NULL)$A01
  rec_on <- .proc_rec(on)
  expect_false(is.na(rec_on$pitch_offset_deg))                      # strong pitch-vs-vv fit -> applied
  expect_equal(rec_on$pitch_offset_deg, 15, tolerance = 4)          # recovers the mounting offset
  expect_gt(rec_on$pitch_offset_r2, 0.1)

  # the default .mk fixture dives, but pitch is ~flat (noise) -> weak fit -> correction skipped
  off <- .run(list(A01 = .mk(secs = 200, rate = 10)), orientation.algorithm = "tilt_compass", downsample.to = NULL)$A01
  expect_true(is.na(.proc_rec(off)$pitch_offset_deg))
})

test_that("correct.roll.offset removes a constant mounting roll bias", {
  d <- .mk()
  phi <- 20 * pi / 180                              # 20-degree mounting roll
  d[, ay := sin(phi) + rnorm(.N, 0, 0.02)]
  d[, az := cos(phi) + rnorm(.N, 0, 0.02)]

  on  <- .run(list(A01 = data.table::copy(d)), orientation.algorithm = "tilt_compass",
              downsample.to = NULL, orientation = orientationControl(correct.roll = TRUE))$A01
  off <- .run(list(A01 = data.table::copy(d)), orientation.algorithm = "tilt_compass",
              downsample.to = NULL, orientation = orientationControl(correct.roll = FALSE))$A01

  # corrected roll is centred near zero; uncorrected roll keeps the ~20-degree bias
  expect_lt(abs(median(on$roll, na.rm = TRUE)), 5)
  expect_gt(abs(median(off$roll, na.rm = TRUE)), 12)
  expect_equal(.proc_rec(on)$roll_offset_deg, 20, tolerance = 4)
  expect_true(is.na(.proc_rec(off)$roll_offset_deg))
})

test_that("a single data.frame input is accepted (split by ID)", {
  out <- .run(as.data.frame(.mk()), orientation.algorithm = "tilt_compass", downsample.to = NULL)
  expect_named(out, "A01")
})

test_that("tilt_compass works on data without a gyroscope (PT7 relaxed requirement)", {
  d <- .mk()
  d[, c("gx", "gy", "gz") := NULL]               # no gyroscope channels
  out <- .run(list(A01 = d), orientation.algorithm = "tilt_compass", downsample.to = NULL)$A01
  expect_false(is.null(out))
  expect_true(all(c("roll", "pitch", "heading") %in% names(out)))
  expect_false(any(c("gx", "gy", "gz") %in% names(out)))   # absent channels stay absent
})

test_that("processing works without a magnetometer (heading = NA) (PT7)", {
  d <- .mk()
  d[, c("mx", "my", "mz") := NULL]               # no magnetometer
  out <- .run(list(A01 = d), orientation.algorithm = "tilt_compass", downsample.to = NULL)$A01
  expect_false(is.null(out))
  expect_true(all(is.na(out$heading)))           # no heading without magnetometer
  expect_false(all(is.na(out$pitch)))            # but pitch/roll still computed
})

test_that("magnetometer output keeps uT-scale values, not unit vectors (PT9)", {
  out <- .run(list(A01 = .mk()), orientation.algorithm = "tilt_compass", downsample.to = NULL)$A01
  # raw mx ~ 0.3; calibrated values are not forced onto the unit sphere
  expect_gt(max(abs(out$mx), na.rm = TRUE), 0)
  rowsums <- out$mx^2 + out$my^2 + out$mz^2
  expect_false(isTRUE(all.equal(rowsums, rep(1, length(rowsums)))))  # not unit-normalized
})

test_that("mag calibration: robust hard-iron removes an injected bias and resists spikes (PT10)", {
  set.seed(7)
  d <- .mk(secs = 200, rate = 10)                          # 2000 rows, level accel
  n <- nrow(d)
  th <- seq(0, 6 * pi, length.out = n)                     # heading sweeps several turns
  ph <- (pi / 2) * sin(seq(0, 8 * pi, length.out = n))     # inclination oscillation -> full sphere coverage
  bias <- c(5, -3, 2)
  d[, mx := cos(ph) * cos(th) + bias[1] + rnorm(n, 0, 0.01)]
  d[, my := cos(ph) * sin(th) + bias[2] + rnorm(n, 0, 0.01)]
  d[, mz := sin(ph)           + bias[3] + rnorm(n, 0, 0.01)]
  d[c(10, 500, 1500), mx := 60]                            # spikes that would wreck a naive min/max midpoint

  out <- .run(list(A01 = data.table::copy(d)), orientation.algorithm = "tilt_compass", downsample.to = NULL,
              calibration = calibrationControl(hard.iron = TRUE, soft.iron = FALSE))$A01
  rec <- .proc_rec(out)
  expect_true(rec$hard_iron_applied)                                          # coverage sufficient -> applied
  expect_equal(rec$hard_iron_offset_uT, sqrt(sum(bias^2)), tolerance = 0.3)   # robust to the spikes
  # the stored field is centred: its robust midpoint is ~0 on every axis
  mid <- function(v) 0.5 * (stats::quantile(v, .98, na.rm = TRUE) + stats::quantile(v, .02, na.rm = TRUE))
  expect_lt(abs(mid(out$mx)), 0.3); expect_lt(abs(mid(out$my)), 0.3); expect_lt(abs(mid(out$mz)), 0.3)
})

test_that("mag calibration: an under-rotated band gets the regularized 2D fallback, not the old cliff (PT11)", {
  set.seed(8)
  d <- .mk(secs = 200, rate = 10)                          # equator coords: the synthetic band's dip ~0 matches IGRF
  n <- nrow(d)
  th <- seq(0, 6 * pi, length.out = n)
  d[, mx := cos(th) * 50 + 5 + rnorm(n, 0, 0.05)]          # full yaw, but z ~ constant: a thin equatorial band
  d[, my := sin(th) * 50 - 3 + rnorm(n, 0, 0.05)]          # (the level-swimming case that hit the zero-cal cliff)
  d[, mz := 2 + rnorm(n, 0, 0.05)]

  out <- .run(list(A01 = data.table::copy(d)), orientation.algorithm = "tilt_compass", downsample.to = NULL,
              calibration = calibrationControl(hard.iron = TRUE, soft.iron = TRUE))$A01
  mc  <- nautilus:::.getMeta(out)$mag_calibration
  rec <- .proc_rec(out)
  expect_equal(mc$status, "calibrated_2d_fallback")        # a real, flagged correction - NOT skipped to raw
  expect_equal(nautilus:::.headingTrust(list(mag_calibration = mc)), "partial")
  expect_true(rec$hard_iron_applied)                       # the in-plane hard-iron WAS removed
  mid <- function(v) 0.5 * (stats::quantile(v, .98, na.rm = TRUE) + stats::quantile(v, .02, na.rm = TRUE))
  expect_lt(abs(mid(out$mx)), 4); expect_lt(abs(mid(out$my)), 4)   # field centred in-plane
})

test_that("madgwick orientation runs natively (no Python) and produces level angles (PT8)", {
  out <- .run(list(A01 = .mk(secs = 80, rate = 25)),
              orientation.algorithm = "madgwick", downsample.to = NULL)$A01
  expect_false(is.null(out))
  expect_true(all(c("roll", "pitch", "heading") %in% names(out)))
  # level swimming -> filter converges to ~level after the transient
  tail_idx <- (nrow(out) - 200):nrow(out)
  expect_lt(abs(median(out$pitch[tail_idx], na.rm = TRUE)), 10)
  expect_lt(abs(median(out$roll[tail_idx], na.rm = TRUE)), 10)
  expect_false(all(is.na(out$heading)))   # MARG mode yields a heading
})

test_that("madgwick seeds the initial orientation (no start-of-record transient) (PT14)", {
  eul <- function(q) c(
    roll  = atan2(2 * (q[1]*q[2] + q[3]*q[4]), 1 - 2 * (q[2]^2 + q[3]^2)) * 180 / pi,
    pitch = asin(max(min(2 * (q[1]*q[3] - q[4]*q[2]), 1), -1)) * 180 / pi,
    yaw   = (atan2(2 * (q[1]*q[4] + q[2]*q[3]), 1 - 2 * (q[3]^2 + q[4]^2)) * 180 / pi) %% 360)

  # a static tilted MARG sample (true orientation: pitch 30 deg, heading 90 deg) held constant
  n <- 400; fs <- 25
  Q <- nautilus:::.madgwickAHRS(matrix(0, n, 3), matrix(rep(c(-0.5, 0, 0.866), each = n), n, 3),
                                matrix(rep(c(-0.433, -0.5, 0.75), each = n), n, 3), frequency = fs, beta = 0.02)
  first <- eul(Q[1, ])
  # the FIRST sample is already at the true orientation (seeded), not drifting up from identity (0,0,0)
  expect_equal(unname(first["pitch"]), 30, tolerance = 1)
  expect_equal(unname(first["yaw"]), 90, tolerance = 1)
  expect_lt(abs(unname(first["roll"])), 1)

  # level first sample seeds to ~level in the no-magnetometer (IMU) path
  q_lvl <- nautilus:::.madgwickAHRS(matrix(0, 50, 3), matrix(rep(c(0, 0, 1), each = 50), 50, 3),
                                    NULL, frequency = fs, beta = 0.02)[1, ]
  expect_lt(abs(unname(eul(q_lvl)["pitch"])), 1)
  expect_lt(abs(unname(eul(q_lvl)["roll"])), 1)

  # no usable accelerometer sample -> identity seed (graceful fallback)
  expect_equal(nautilus:::.madgwickSeed(matrix(NA_real_, 5, 3), NULL, FALSE), c(1, 0, 0, 0))
})

test_that("burst detection is VeDBA-keyed: catches an upward burst total-accel would miss (PT13)", {
  d <- .mk(secs = 120, rate = 10)                  # level: az ~ 1 g (gravity), ax/ay ~ 0
  # upward dynamic bursts: az dips well below the 1 g baseline (a_dyn opposes gravity). Their VeDBA is
  # large, but their TOTAL acceleration (~0.5 g) is BELOW the resting level - the old |accel| metric
  # would rank them near the minimum and never flag them.
  spikes <- c(400, 800)
  d[spikes, az := -0.5]
  out <- .run(list(A01 = d), orientation.algorithm = "tilt_compass", downsample.to = NULL,
              burst.quantiles = 0.99)$A01
  expect_true(all(out$burst99[spikes] == 1L))      # flagged on VeDBA despite low total acceleration
  # sanity: their total acceleration really is below the record median (so accel-keying would miss them)
  expect_lt(max(out$accel[spikes]), median(out$accel, na.rm = TRUE))
})

test_that("verbose = FALSE is silent (PT3)", {
  out <- capture.output(suppressWarnings(suppressMessages(
    res <- processTagData(list(A01 = .mk()), orientation.algorithm = "tilt_compass",
                          downsample.to = NULL, verbose = FALSE))))
  expect_length(out, 0)
})

test_that("verbose output is a standardized cli block (no legacy print/cat cruft)", {
  grab <- function(v) paste(cli::cli_fmt(suppressWarnings(
    processTagData(list(A01 = .mk()), orientation.algorithm = "tilt_compass",
                   downsample.to = NULL, verbose = v))), collapse = "\n")
  d2 <- grab(2); d1 <- grab(1)
  expect_match(d2, "processTagData")                 # framed header
  expect_match(d2, "A01 \\(1/1\\)")                  # per-individual cli sub-header
  # detailed level emits the diagnostic key:value block (findings, not step narration)
  expect_match(d2, "input:")                         # input shape line
  expect_match(d2, "channel")                        # channel count folded onto the input line
  expect_match(d2, "orientation:")                   # orientation method/posture line
  expect_match(d2, "median pitch")                   # posture medians reported
  expect_match(d2, "VeDBA")                           # motion line reports VeDBA (towed-tag-appropriate)
  expect_match(d2, "depth:")                          # depth gets its own line
  expect_false(grepl("ODBA", d2, fixed = TRUE))       # ODBA no longer shown in the console
  expect_false(grepl("sensors:", d2, fixed = TRUE))   # the verbose channel list is gone (count on input line)
  expect_match(d2, "SUMMARY")
  expect_match(d2, "1 of 1 tag processed")
  expect_false(grepl("Calculating", d2, fixed = TRUE))  # legacy step-narration gone
  expect_false(grepl("--->", d2, fixed = TRUE))      # legacy "--->" prefix gone
  expect_false(grepl("Done!", d2, fixed = TRUE))     # legacy "Done!" line gone
  expect_false(grepl("Saving file", d2, fixed = TRUE)) # spinner gone
  expect_false(grepl("A01 \\(1/1\\)", d1))            # per-step sub-header is level-2 only
  expect_false(grepl("input:", d1, fixed = TRUE))     # the diagnostic block is level-2 only
  expect_match(d1, "A01.*channel.*rows.*Hz")          # normal level: compact id . channels . rows . Hz summary
})

test_that("data.table progress messages are silenced during the run", {
  before <- getOption("datatable.showProgress")
  .run(list(A01 = .mk()), orientation.algorithm = "tilt_compass", downsample.to = NULL)
  expect_identical(getOption("datatable.showProgress"), before)   # option restored on exit
})

# a 3 h, 1 Hz tag with a linear depth-offset drift (0 -> 1.5 m) and a dry signal over surface bouts
.mk_drift_tag <- function(with_dry = TRUE) {
  set.seed(1); n <- 3 * 3600
  t0 <- as.POSIXct("2020-01-01", tz = "UTC"); dt <- t0 + 0:(n - 1); sec <- 0:(n - 1)
  at_surf <- (sec %% 1800) < 120                                  # surface bout every 30 min
  depth   <- ifelse(at_surf, 0, 20) + seq(0, 1.5, length.out = n) # dive profile + linear drift
  d <- data.table::data.table(ID = "A", datetime = dt,
    ax = rnorm(n, 0, .02), ay = rnorm(n, 0, .02), az = 1 + rnorm(n, 0, .02),
    gx = rnorm(n, 0, .01), gy = rnorm(n, 0, .01), gz = rnorm(n, 0, .01),
    mx = 0.3 + rnorm(n, 0, .01), my = rnorm(n, 0, .01), mz = 0.4 + rnorm(n, 0, .01),
    depth = depth, temp = 20,
    PTT = NA_character_, position_type = NA_character_, lat = NA_real_, lon = NA_real_, quality = NA_character_)
  m <- nautilus:::.newNautilusMeta(); m$id <- "A"
  m$deployment$datetime <- t0; m$deployment$lon <- -25; m$deployment$lat <- 38
  if (with_dry) {
    enc <- nautilus:::.transitionEncode(dt, at_surf)
    m$ancillary$dry <- list(source = "test", encoding = "transitions",
                            data = data.frame(datetime = enc$datetime, dry = enc$state))
  }
  nautilus:::new_nautilus_tag(d, m)
}
.dd_rec <- function(x) Filter(function(p) identical(p$step, "depth_drift"), nautilus:::.getMeta(x)$processing)

test_that("depth drift correction runs in-pipeline: applies the offset + logs a lean record", {
  on  <- .run(list(A = .mk_drift_tag(with_dry = TRUE)),  downsample.to = NULL)$A
  off <- .run(list(A = .mk_drift_tag(with_dry = FALSE)), downsample.to = NULL)$A   # no evidence -> abstains

  rec <- .dd_rec(on)
  expect_length(rec, 1L)
  expect_equal(rec[[1]]$status, "applied")
  expect_equal(rec[[1]]$params$method, "surface")
  expect_gt(rec[[1]]$outcome$offset_range_m[2], 1.0)              # offset ramps toward the ~1.5 m drift
  expect_equal(.dd_rec(off)[[1]]$status, "abstained")            # no dry / no fixes -> abstain

  # the correction shows up as the (abstained-raw minus corrected) depth difference: a 0 -> ~1.3 m ramp
  diff_off <- off$depth - on$depth
  expect_gt(max(diff_off, na.rm = TRUE), 0.9)
  expect_lt(min(diff_off, na.rm = TRUE), 0.3)
})

test_that("depth.drift = depthDriftControl(method='none') disables the correction (no record, depth untouched)", {
  on  <- .run(list(A = .mk_drift_tag(TRUE)), downsample.to = NULL)$A
  none <- .run(list(A = .mk_drift_tag(TRUE)), depth.drift = depthDriftControl(method = "none"), downsample.to = NULL)$A
  expect_length(.dd_rec(none), 0L)                                # disabled -> no depth_drift record
  expect_false(isTRUE(all.equal(on$depth, none$depth)))          # 'none' differs from the corrected run
})

test_that("processTagData persists median pitch/roll + row counts, and processingSummary reads them", {
  out <- .run(list(A01 = .mk_diving(secs = 120, rate = 10)),
              orientation.algorithm = "tilt_compass", downsample.to = 1)$A01
  rec <- .proc_rec(out)
  # B1: the per-deployment numbers the console prints are now persisted (single source of truth)
  expect_false(is.na(rec$median_pitch_deg)); expect_false(is.na(rec$median_roll_deg))
  expect_equal(rec$n_input, 1200L)                                # 120 s * 10 Hz
  expect_true(rec$n_output > 0L && rec$n_output <= 130L)          # ~120 one-second bins
  # B2: processingSummary projects one row per deployment from that provenance
  s <- processingSummary(list(A01 = out))
  expect_s3_class(s, "nautilus_processing_summary")
  expect_equal(nrow(s), 1L)
  expect_equal(s$algorithm, "tilt_compass")
  expect_equal(s$n_in, 1200L); expect_equal(s$median_pitch, rec$median_pitch_deg)
})

test_that("processTagData persists the depth-drift anchor count", {
  on  <- .run(list(A = .mk_drift_tag(with_dry = TRUE)), downsample.to = NULL)$A
  rec <- .dd_rec(on)[[1]]
  expect_true(!is.null(rec$n_anchors) && rec$n_anchors >= 1L)     # anchors now stored (was print-only)
})

test_that("ORDERING GUARD: warns on un-oriented data, silent once an axis mapping is applied", {
  catch_warns <- function(expr) {
    w <- character(0)
    withCallingHandlers(invisible(capture.output(suppressMessages(expr))),
      warning = function(cnd) { w <<- c(w, conditionMessage(cnd)); invokeRestart("muffleWarning") })
    w
  }
  # .mk() has no applied axis mapping (axis_mapping$applied defaults FALSE) -> the guard warns
  expect_true(any(grepl("axis mapping", catch_warns(processTagData(list(A01 = .mk()), downsample.to = NULL, verbose = FALSE)))))
  # applyAxisMapping() (identity) sets axis_mapping$applied = TRUE -> no ordering-guard warning
  oriented <- suppressWarnings(suppressMessages(applyAxisMapping(list(A01 = .mk()),
      mapping = data.frame(from = c("ax", "ay", "az"), to = c("ax", "ay", "az"), stringsAsFactors = FALSE),
      verbose = FALSE, return.data = TRUE)))
  expect_false(any(grepl("axis mapping", catch_warns(processTagData(oriented, downsample.to = NULL, verbose = FALSE)))))
})

# ---- paddle-wheel heading de-noise (shared .paddleState/.magDenoise; applied before calibration) -----
.mk_paddle <- function(rate = 50, secs = 40, amp = 4, f = 10, turn = 0.05) {
  d <- .mk(secs = secs, rate = rate)
  t <- seq_len(nrow(d)) / rate
  th <- 2 * pi * turn * t                                      # a slow, genuine heading change (the "animal turning")
  d[, mx := 0.4 * cos(th) + amp * cos(2 * pi * f * t)]         # slow field rotation + a fast paddle-magnet oscillation
  d[, my := 0.4 * sin(th) + amp * sin(2 * pi * f * t)]
  d
}
.hd_rough <- function(x) stats::median(abs(((diff(x$heading) + 180) %% 360) - 180), na.rm = TRUE)

test_that("processTagData de-noises a paddle-contaminated heading (auto) and leaves a clean magnetometer alone", {
  pad  <- .mk_paddle()
  # disable the post-orientation angle-smoother so the vector-domain paddle de-noise is what's being tested
  noflt <- smoothingControl(orientation = NULL)
  auto <- .run(pad, downsample.to = NULL, smoothing = noflt, orientation = orientationControl(heading.denoise = "auto"))
  off  <- .run(pad, downsample.to = NULL, smoothing = noflt, orientation = orientationControl(heading.denoise = "off"))
  a <- if (data.table::is.data.table(auto)) auto else auto[[1]]
  o <- if (data.table::is.data.table(off))  off  else off[[1]]
  expect_gt(nautilus:::.getMeta(a)$sensors$heading_denoise_window, 0)     # auto detected the paddle + applied a window
  expect_true(isTRUE(nautilus:::.getMeta(a)$sensors$paddle_contaminated))
  expect_lt(.hd_rough(a), .hd_rough(o))                                  # the de-noised heading is much smoother
  # a clean magnetometer is not de-noised
  clean <- .run(.mk(rate = 50, secs = 40), downsample.to = NULL, orientation = orientationControl(heading.denoise = "auto"))
  cc <- if (data.table::is.data.table(clean)) clean else clean[[1]]
  expect_equal(nautilus:::.getMeta(cc)$sensors$heading_denoise_window, 0)
  expect_false(isTRUE(nautilus:::.getMeta(cc)$sensors$paddle_contaminated))
})

test_that("manual mode applies a fixed de-noise window to a paddle deployment", {
  pad <- .mk_paddle()
  man <- .run(pad, downsample.to = NULL, orientation = orientationControl(heading.denoise = "manual", heading.denoise.window = 1.5))
  m <- if (data.table::is.data.table(man)) man else man[[1]]
  expect_equal(nautilus:::.getMeta(m)$sensors$heading_denoise_window, 1.5)
})

# ---- a constant imported paddle channel is dropped, not kept ---------------------------------------

test_that("a CONSTANT imported paddle channel is dropped to NA and warned about once", {
  # processTagData already judged such a column "not meaningful" but then ignored its own verdict whenever
  # the internal estimate could not run to replace it, leaving a dead sensor's fixed value in the output -
  # where a constant-zero speed reads downstream as that many genuine zero-speed samples.
  set.seed(1)
  n <- 4000; fs <- 20
  mk <- function(id, sp) {
    dt <- data.table::data.table(
      ID = id, datetime = as.POSIXct("2023-01-01", tz = "UTC") + (seq_len(n) - 1) / fs,
      ax = stats::rnorm(n, 0, .1), ay = stats::rnorm(n, 0, .1), az = 1 + stats::rnorm(n, 0, .1),
      depth = 20 + 10 * sin(seq_len(n) / 300), paddle_speed = sp)
    m <- nautilus:::.newNautilusMeta(); m$id <- id
    m$tag$paddle_wheel <- TRUE; m$tag$package_id <- 99
    m$deployment$datetime <- as.POSIXct("2023-01-01", tz = "UTC")
    m$axis_mapping$applied <- TRUE
    data.table::setattr(dt, "nautilus", m); class(dt) <- c("nautilus_tag", class(dt)); dt
  }
  tags <- list(DEAD_01 = mk("DEAD_01", rep(0, n)),                 # dead paddle: one fixed value
               GOOD_01 = mk("GOOD_01", stats::runif(n, 0.2, 1.5))) # a real, varying record

  out <- NULL
  # the calibration year does not match, so the internal estimate cannot run to replace the column -
  # exactly the path where the verdict used to be discarded
  w <- testthat::capture_warnings(
    invisible(capture.output(
      out <- processTagData(tags, paddle.calibration = data.frame(year = 2024, package_id = 99, slope = 0.35),
                            verbose = FALSE))))

  expect_equal(sum(is.finite(out[["DEAD_01"]]$paddle_speed)), 0)   # dropped
  expect_gt(sum(is.finite(out[["GOOD_01"]]$paddle_speed)), 0)      # a real channel is untouched

  paddle_w <- w[grepl("CONSTANT imported paddle", w)]
  expect_length(paddle_w, 1L)                                      # consolidated, not one per deployment
  expect_match(paddle_w, "DEAD_01")
  expect_false(grepl("GOOD_01", paddle_w))                         # only the offender is named
})

test_that("stored precision keeps each channel's quantum below its own noise floor", {
  # The storage-rounding table trades serialised size for precision. The quantum has to stay BELOW the
  # per-sample noise of the series actually stored, so the noise dithers the quantiser and later
  # averaging still recovers sub-quantum detail; a quantum at or above the noise makes the error
  # systematic and unrecoverable. Two entries used to violate that:
  #   - vertical_velocity at 2 dp (0.01 m/s) sat 4-5x above its measured 0.0018-0.0024 m/s noise floor,
  #     snapping sustained slow drift (gliding / buoyancy regulation) to exactly zero for minutes at a time;
  #   - odba/vedba at 3 dp were stored 38-66x coarser than the 4 dp surge/sway/heave they are summed from.
  set.seed(42)
  n <- 6000; fs <- 20
  dt <- data.table::data.table(
    ID = "PREC_01", datetime = as.POSIXct("2023-01-01", tz = "UTC") + (seq_len(n) - 1) / fs,
    ax = stats::rnorm(n, 0, .1), ay = stats::rnorm(n, 0, .1), az = 1 + stats::rnorm(n, 0, .1),
    # a slow, smooth vertical excursion: velocities live well inside the old 0.01 m/s quantum
    depth = 20 + 3 * sin(seq_len(n) / 900))
  m <- nautilus:::.newNautilusMeta(); m$id <- "PREC_01"
  m$deployment$datetime <- as.POSIXct("2023-01-01", tz = "UTC")
  m$axis_mapping$applied <- TRUE
  data.table::setattr(dt, "nautilus", m); class(dt) <- c("nautilus_tag", class(dt))
  data.table::setattr(dt, "nautilus.version", utils::packageVersion("nautilus"))

  out <- processTagData(list(PREC_01 = dt), downsample.to = NULL, verbose = FALSE)[["PREC_01"]]

  on_grid <- function(x, q) {
    x <- x[is.finite(x)]
    length(x) > 0L && max(abs(x / q - round(x / q))) < 1e-6
  }
  # Both halves matter. "On the q grid" alone is satisfied by ANY coarser rounding too (2 dp values sit
  # on a 1e-3 grid), so it cannot detect a regression on its own - the second clause is what pins the
  # quantum down, by requiring the series to actually USE levels the coarser grid does not have.
  expect_true(on_grid(out$vertical_velocity, 1e-3))    # 3 dp ...
  expect_false(on_grid(out$vertical_velocity, 1e-2))   # ... and genuinely finer than 2 dp
  for (nm in c("accel", "odba", "vedba")) {
    if (!nm %in% names(out)) next
    expect_true(on_grid(out[[nm]], 1e-4))              # 4 dp, matching surge/sway/heave ...
    expect_false(on_grid(out[[nm]], 1e-3))             # ... and genuinely finer than 3 dp
  }

  # the point of the change: a slow excursion is no longer flattened onto a handful of levels
  v <- out$vertical_velocity[is.finite(out$vertical_velocity)]
  expect_gt(length(unique(v)), 100L)
  expect_lt(mean(v == 0), 0.05)
})

test_that("the summary reports cohort volume, quoting stored rows against input rows", {
  # downsample.to (1 Hz by default) makes stored rows one to two orders of magnitude smaller than the
  # input, so the stored figure alone reads as data loss. The line carries both, plus summed tracked time.
  set.seed(7)
  mk <- function(id, secs, fs = 20) {
    n <- secs * fs
    t0 <- as.POSIXct("2023-01-01", tz = "UTC")
    d <- data.table::data.table(
      ID = id, datetime = t0 + (seq_len(n) - 1) / fs,
      ax = stats::rnorm(n, 0, .1), ay = stats::rnorm(n, 0, .1), az = 1 + stats::rnorm(n, 0, .1),
      depth = 20 + 10 * sin(seq_len(n) / 300), temp = 20)
    m <- nautilus:::.newNautilusMeta(); m$id <- id
    m$deployment$datetime <- t0; m$deployment$lon <- -25; m$deployment$lat <- 11
    m$axis_mapping$applied <- TRUE
    data.table::setattr(d, "nautilus", m)
    data.table::setattr(d, "nautilus.version", "test")
    class(d) <- c("nautilus_tag", class(d)); d
  }
  tags <- list(A01 = mk("A01", 3600), A02 = mk("A02", 1800))     # 1 h + 0.5 h at 20 Hz

  txt <- paste(cli::cli_fmt(suppressWarnings(
    processTagData(tags, downsample.to = 1, verbose = 1))), collapse = "\n")

  expect_match(txt, "total rows: .* \\(from .* input\\) \u00b7 duration: ")
  expect_match(txt, "duration: 1\\.5 h")                          # SUM of tracked time, not calendar span
  # stored (1 Hz) must be quoted against the larger 20 Hz input
  expect_match(txt, "from 108 K input")
})

test_that(".tagSpanSeconds sums tracked time and never poisons a running total", {
  t <- as.POSIXct("2023-01-01", tz = "UTC") + seq(0, 3600 * 10, by = 60)
  expect_equal(nautilus:::.tagSpanSeconds(t), 3600 * 10)
  # Date counts DAYS - via the shared coercion contract, not a bare as.numeric()
  expect_equal(nautilus:::.tagSpanSeconds(as.Date("2023-01-01") + 0:5), 5 * 86400)
  # an unusable or degenerate column contributes 0 rather than NA, so one bad tag cannot void the total
  expect_equal(nautilus:::.tagSpanSeconds(as.character(t)), 0)
  expect_equal(nautilus:::.tagSpanSeconds(t[1]), 0)
  expect_equal(nautilus:::.tagSpanSeconds(numeric(0)), 0)
})
