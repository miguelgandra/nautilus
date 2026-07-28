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
  expect_true(any(grepl("no magnetometer calibration", ws)))          # loud, default-level warning
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

test_that("jerk is computed as the native-rate norm of d(accel)/dt and aggregated to the stored series", {
  # a clean full-rate run must carry a finite, non-negative jerk channel (g/s)
  out <- .run(list(A01 = .mk()), orientation.algorithm = "tilt_compass", downsample.to = NULL)$A01
  expect_true("jerk" %in% names(out))
  expect_true(all(out$jerk >= 0, na.rm = TRUE))
  expect_true(any(is.finite(out$jerk)))
  # only the first sample (no predecessor for the difference) is NA
  expect_equal(sum(is.na(out$jerk)), 1L)

  # the magnitude tracks the native rate: it is a derivative, so at a genuinely higher sampling rate the
  # SAME per-sample noise process yields a proportionally larger jerk (diff * fs).
  slow <- .run(list(A01 = .mk(rate = 10)), orientation.algorithm = "tilt_compass", downsample.to = NULL)$A01
  fast <- .run(list(A01 = .mk(rate = 20)), orientation.algorithm = "tilt_compass", downsample.to = NULL)$A01
  ratio <- median(fast$jerk, na.rm = TRUE) / median(slow$jerk, na.rm = TRUE)
  expect_gt(ratio, 1.5); expect_lt(ratio, 2.5)
})

test_that("movement_jerk uses a single first difference (not the old triple difference)", {
  # a linear ramp has a CONSTANT first difference (= 1), so RMS-jerk is ~1 under the corrected single-diff.
  # The former triple-diff would difference that constant twice more to zero, giving ~0 - this locks the fix.
  mj <- nautilus:::.movement_jerk(as.numeric(1:100), window = 10)
  expect_equal(median(mj, na.rm = TRUE), 1, tolerance = 1e-6)
  expect_equal(sum(is.na(mj)), 1L)                # single diff loses exactly one leading sample
})

test_that("downsampling records BOTH sampling rates, which is what identifies the boxcar on depth", {
  # downsample.to mean-aggregates every numeric channel, depth included, so the bin IS a boxcar on the
  # stored depth channel. detectDives()/diveMetrics() derive the duration floor and depth_attenuation
  # from these two rates: a processed rate BELOW the original is the only evidence aggregation ran
  # (processTagData skips downsampling when the target meets or exceeds the native rate).
  out <- .run(list(A01 = .mk_diving(secs = 120, rate = 10)),
              orientation.algorithm = "tilt_compass", downsample.to = 1)$A01
  sen <- nautilus:::.getMeta(out)$sensors
  expect_equal(sen$sampling_hz_original, 10)
  expect_equal(sen$sampling_hz_processed, 1)
  expect_equal(nautilus:::.diveDepthBin(nautilus:::.getMeta(out)), 1)

  # and with no downsampling there is no bin to charge
  raw <- .run(list(A01 = .mk_diving(secs = 60, rate = 10)),
              orientation.algorithm = "tilt_compass", downsample.to = NULL)$A01
  expect_true(is.na(nautilus:::.diveDepthBin(nautilus:::.getMeta(raw))))
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


# ---- mounting-roll gate vs reporting threshold ------------------------------------------------
# These two used to share one 45-degree constant, so an offset just past it lost its correction AND
# was reported as an anomaly - with the anomaly's number being the uncorrected mount, not a residual.
# `.run` suppresses warnings, so a runner that COLLECTS them is required: the warning IS the assertion.

.mk_rolled <- function(deg, secs = 120, rate = 10) {
  d <- .mk(secs = secs, rate = rate)
  phi <- deg * pi / 180
  set.seed(7)
  d[, ay := sin(phi) + rnorm(.N, 0, 0.02)]
  d[, az := cos(phi) + rnorm(.N, 0, 0.02)]
  d
}

.run_warn <- function(d, ...) {
  w <- character(0); res <- NULL
  invisible(capture.output(suppressMessages(withCallingHandlers(
    res <- processTagData(d, verbose = FALSE, ...),
    warning = function(cond) { w <<- c(w, conditionMessage(cond)); invokeRestart("muffleWarning") }))))
  list(res = res, warnings = w)
}

test_that("a mount between warning.threshold and mount.roll.max is corrected AND reported", {
  # 50 deg: over the 45-deg reporting threshold, under the 60-deg apply gate
  out <- .run_warn(list(A01 = .mk_rolled(50)), orientation.algorithm = "tilt_compass",
                   downsample.to = NULL)
  rec <- .proc_rec(out$res$A01)

  expect_true(rec$roll_mount_unusual)                       # the mount is reported...
  expect_equal(rec$roll_offset_status, "applied")           # ...and still corrected
  expect_equal(rec$roll_offset_deg, 50, tolerance = 5)      # the applied offset is recorded
  expect_lt(abs(median(out$res$A01$roll, na.rm = TRUE)), 5) # so roll is centred near zero
  expect_false(rec$roll_anomaly_detected)                   # no residual anomaly

  expect_true(any(grepl("unusual mounting roll", out$warnings)))
  expect_true(any(grepl("corrected", out$warnings)))
  # the OLD behaviour would have refused the correction and warned about a -50 deg "roll anomaly"
  expect_false(any(grepl("roll residual after correction", out$warnings)))
})

test_that("a mount beyond mount.roll.max is refused, reported once, and keeps its estimate", {
  out <- .run_warn(list(A01 = .mk_rolled(70)), orientation.algorithm = "tilt_compass",
                   downsample.to = NULL)
  rec <- .proc_rec(out$res$A01)

  expect_equal(rec$roll_offset_status, "rejected_over_max")
  expect_true(is.na(rec$roll_offset_deg))                            # nothing was subtracted...
  expect_equal(rec$roll_offset_estimate_deg, 70, tolerance = 5)      # ...but the estimate survives
  expect_true(rec$roll_mount_unusual)
  expect_gt(abs(median(out$res$A01$roll, na.rm = TRUE)), 60)         # roll keeps the mount

  expect_true(any(grepl("unusual mounting roll", out$warnings)))
  expect_true(any(grepl("NOT corrected", out$warnings)))
  # exactly ONE roll warning: the residual here IS the mount, already named above
  expect_false(any(grepl("roll residual after correction", out$warnings)))
  expect_equal(sum(grepl("mounting roll|Roll residual", out$warnings)), 1L)
})

test_that("an ordinary mount is corrected silently, and mount.roll.max is honoured", {
  out <- .run_warn(list(A01 = .mk_rolled(20)), orientation.algorithm = "tilt_compass",
                   downsample.to = NULL)
  rec <- .proc_rec(out$res$A01)
  expect_equal(rec$roll_offset_status, "applied")
  expect_false(rec$roll_mount_unusual)
  expect_false(any(grepl("mounting roll|Roll residual", out$warnings)))

  # tightening the gate below the mount flips the same data to refused
  tight <- .run_warn(list(A01 = .mk_rolled(20)), orientation.algorithm = "tilt_compass",
                     downsample.to = NULL, orientation = orientationControl(mount.roll.max = 10))
  expect_equal(.proc_rec(tight$res$A01)$roll_offset_status, "rejected_over_max")
  expect_true(is.na(.proc_rec(tight$res$A01)$roll_offset_deg))
})

test_that("PIN_10 regression: a 45.04-degree mount is now corrected, not flagged as an anomaly", {
  # the real case this change exists for - PIN_08 (-43.40) was corrected and PIN_10 (-45.04) was not,
  # 1.64 degrees apart, because the apply gate and the alarm shared the same 45-degree constant
  out <- .run_warn(list(A01 = .mk_rolled(-45.04)), orientation.algorithm = "tilt_compass",
                   downsample.to = NULL)
  rec <- .proc_rec(out$res$A01)
  expect_equal(rec$roll_offset_status, "applied")
  expect_lt(abs(median(out$res$A01$roll, na.rm = TRUE)), 5)
  expect_false(rec$roll_anomaly_detected)
  expect_true(rec$roll_mount_unusual)                       # still surfaced, just not as an anomaly
  expect_equal(rec$mount_roll_max, 60)
})

# ---- verbose structure: every deployment gets its own block, skipped or not ---------------------
# cli writes through the condition system, so the console text has to be captured from the MESSAGE
# stream - and the output IS the assertion here, so nothing may be suppressed around it.

.console <- function(expr) {
  o <- character(0)
  invisible(capture.output(o <- capture.output(suppressWarnings(expr), type = "message")))
  paste(o, collapse = "\n")
}

.mk_tagged <- function(id, excluded = character(0), mutate = identity) {
  d <- mutate(.mk(id = id, secs = 120, rate = 10))
  m <- nautilus:::.newNautilusMeta(); m$id <- id; m$sensors$excluded <- excluded
  nautilus:::new_nautilus_tag(d, m)
}

test_that("a deployment skipped for a curated-away channel gets its own delimited block", {
  # the skip line used to print BEFORE the header, so it floated between the neighbouring blocks and
  # which tag it referred to had to be inferred from its position on screen
  ok  <- .mk_tagged("OK1")
  bad <- .mk_tagged("NOACC", excluded = c("ax", "ay", "az"),
                    mutate = function(d) { d[, c("ax", "ay", "az") := NULL]; d })

  txt <- .console(processTagData(list(OK1 = ok, NOACC = bad), downsample.to = NULL, verbose = 1))
  expect_match(txt, "NOACC \\(2/2\\)")                     # its own header, numbered like the rest
  expect_match(txt, "excluded by an earlier QC step")      # the reason, inside that block
  # the reason must come AFTER its header, not before it
  expect_lt(regexpr("NOACC \\(2/2\\)", txt), regexpr("excluded by an earlier QC step", txt))
})

test_that("an empty slot is reported instead of vanishing silently", {
  # this path used to `next` with no block, no reason and no entry in skipped_ids, so the deployment
  # disappeared from both the console and the end-of-run summary
  ok <- .mk_tagged("OK1")
  txt <- .console(processTagData(list(OK1 = ok, GONE = ok[0]), downsample.to = NULL, verbose = 1))
  expect_match(txt, "GONE \\(2/2\\)")
  expect_match(txt, "no data")

  # collect EVERY warning: this call also emits the axis-mapping ordering guard, and tryCatch(warning=)
  # would return on that one and never see the skip
  w <- character(0)
  invisible(capture.output(suppressMessages(withCallingHandlers(
    processTagData(list(OK1 = ok, GONE = ok[0]), downsample.to = NULL, verbose = FALSE),
    warning = function(cnd) { w <<- c(w, conditionMessage(cnd)); invokeRestart("muffleWarning") }))))
  expect_true(any(grepl("skipped for missing or unusable input", w)))
  expect_true(any(grepl("GONE", w)))
})

test_that("the sub-1 Hz guard cannot fire through the current rate estimator (documented)", {
  # The guard was an .abort() that would have killed the whole batch; it is now a per-deployment skip
  # for consistency. But it is unreachable as written, and that is worth locking down rather than
  # leaving as a test that cannot be built: the estimator is
  #     nrow / (number of distinct whole seconds present)
  # and every row falls in exactly one second, so n_distinct <= nrow and the ratio is ALWAYS >= 1.
  rate <- function(n, spacing) {
    dt <- as.POSIXct("2020-01-01", tz = "UTC") + (seq_len(n) - 1L) * spacing
    n / length(unique(lubridate::floor_date(dt, "sec")))
  }
  for (sp in c(0.05, 1, 4, 60, 3600)) expect_gte(rate(30L, sp), 1)

  # THE REAL CONSEQUENCE, and it is not a formatting issue: a genuinely slow record does not report a
  # fractional rate, it reports 1 Hz. A one-sample-per-minute series is treated as 1 Hz, so every
  # seconds -> samples window is 60x too short in real time. Locked here so the assumption is visible;
  # fixing it means changing the estimator (a span-based rate), which is a separate decision.
  expect_equal(round(rate(30L, 60)), 1)              # 1/60 Hz in reality
  expect_equal(round(rate(30L, 3600)), 1)            # 1/3600 Hz in reality
})

test_that(".deploymentLabel falls back from ID to source name to slot index", {
  f <- nautilus:::.deploymentLabel
  d <- data.table::data.table(ID = "PIN_99", x = 1)
  expect_equal(f(d, "/tmp/whatever.rds", 3L), "PIN_99")          # the ID wins
  expect_equal(f(d[0], "/tmp/PIN_07.rds", 3L), "PIN_07")         # empty -> the file name
  expect_equal(f(NULL, "/tmp/PIN_07.rds", 3L), "PIN_07")
  expect_equal(f(NULL, NA_character_, 3L), "slot 3")             # nothing at all -> the index
  expect_equal(f(data.table::data.table(x = 1), NULL, 5L), "slot 5")   # no ID column
})

# ---- end-of-run warnings: one per finding TYPE, ids inline -------------------------------------

test_that("orientation findings raise ONE warning per type, not one per deployment", {
  # the whole point: 3 rolled mounts used to be 3 warnings, and R replaces the entire warning surface
  # with "There were N warnings" once 11 accumulate - so the per-deployment form lost everything it
  # raised on a large batch
  tags <- list(A = .mk_rolled(50), B = .mk_rolled(52), C = .mk_rolled(54))
  names(tags) <- c("A", "B", "C")
  for (i in seq_along(tags)) tags[[i]][, ID := names(tags)[i]]

  out <- .run_warn(tags, orientation.algorithm = "tilt_compass", downsample.to = NULL)
  roll <- grep("unusual mounting roll", out$warnings, value = TRUE)
  expect_length(roll, 1L)                                  # ONE warning, not three
  expect_match(roll, "3 deployments")
  for (id in c("A", "B", "C")) expect_match(roll, id)      # every id named, none truncated
  expect_match(roll, "A \\(")                              # id carries its value inline
})

test_that("the affected ids are listed inline, not as a bulleted section", {
  tags <- list(A = .mk_rolled(50), B = .mk_rolled(52))
  for (i in seq_along(tags)) tags[[i]][, ID := names(tags)[i]]
  out <- .run_warn(tags, orientation.algorithm = "tilt_compass", downsample.to = NULL)
  roll <- grep("unusual mounting roll", out$warnings, value = TRUE)
  expect_false(grepl("Affected deployments:", roll))
  expect_match(roll, "A \\([-0-9.]+.*\\), B \\([-0-9.]+")   # comma-joined on one run
})

test_that("a corrected and an uncorrected mount are separate findings", {
  # they need different responses from the reader, so the state is in the headline and the item stays
  # a bare id + value
  tags <- list(OK = .mk_rolled(50), BAD = .mk_rolled(70))   # 70 exceeds the default mount.roll.max 60
  for (i in seq_along(tags)) tags[[i]][, ID := names(tags)[i]]
  out <- .run_warn(tags, orientation.algorithm = "tilt_compass", downsample.to = NULL)
  expect_length(grep("NOT corrected", out$warnings), 1L)
  expect_length(grep("roll, corrected", out$warnings), 1L)
  expect_match(grep("NOT corrected", out$warnings, value = TRUE), "BAD")
  expect_match(grep("roll, corrected", out$warnings, value = TRUE), "OK")
})

test_that("the cohort warnings no longer truncate the id list at 8", {
  # the four cli_warn sites used utils::head(ids, 8), which silently cut the list with no indication
  ids <- sprintf("D%02d", 1:12)
  tags <- stats::setNames(lapply(ids, function(i) { d <- .mk_tagged(i); d }), ids)
  out <- .run_warn(tags, orientation.algorithm = "tilt_compass", downsample.to = NULL)
  ax <- grep("without an applied axis mapping", out$warnings, value = TRUE)
  expect_length(ax, 1L)
  expect_match(ax, "12 deployments")
  for (i in ids) expect_match(ax, i)                        # all 12, including the 9th onward
})

test_that("no end-of-run warning repeats a multi-paragraph explanation", {
  ok  <- .mk_tagged("OK1")
  gone <- .mk_tagged("GONE")
  out <- .run_warn(list(OK1 = ok, GONE = gone[0]), downsample.to = NULL)
  for (w in out$warnings) {
    # a two-line shape: headline + one id run. Nothing carrying prose about mechanism or remedy.
    expect_false(grepl("dead-reckoned tracks will drift|pooled statistic|is idempotent", w))
  }
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

  paddle_w <- w[grepl("constant paddle channel", w)]
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

test_that("the stored depth channel is NOT smoothed, while vertical velocity still is", {
  # smoothing$depth conditions the series the DERIVATIVE is taken from. It used to also overwrite the
  # stored `depth` column, and a centred boxcar attenuates any excursion shorter than its window: at the
  # 10 s default a 3 m / 8 s dive was stored as 1.2 m. Harmless for the minutes-long dives this package
  # was first used on, fatal for short-dive taxa - and invisible, because the trace still looks like a dive.
  fs <- 10; secs <- 600
  n <- fs * secs
  t0 <- as.POSIXct("2023-01-01", tz = "UTC")
  # a flat record with one short, sharp 3 m excursion
  depth <- numeric(n)
  i0 <- n %/% 2; k <- (8 * fs) %/% 2                        # 8 s dive
  depth[(i0 - k + 1):i0] <- seq(0, 3, length.out = k)
  depth[(i0 + 1):(i0 + k)] <- seq(3, 0, length.out = k)
  d <- data.table::data.table(
    ID = "A01", datetime = t0 + (seq_len(n) - 1) / fs,
    ax = 0, ay = 0, az = 1, depth = depth, temp = 20)
  m <- nautilus:::.newNautilusMeta(); m$id <- "A01"
  m$deployment$datetime <- t0; m$deployment$lon <- -25; m$deployment$lat <- 11
  m$axis_mapping$applied <- TRUE
  data.table::setattr(d, "nautilus", m); data.table::setattr(d, "nautilus.version", "test")
  class(d) <- c("nautilus_tag", class(d))

  out <- suppressWarnings(processTagData(list(A01 = d), smoothing = smoothingControl(depth = 10),
                                         downsample.to = NULL, verbose = FALSE))[["A01"]]

  # the excursion keeps its amplitude: a 10 s boxcar would have left 3 * 8/(2*10) = 1.2 m
  expect_gt(max(out$depth, na.rm = TRUE), 2.5)
  # and vertical velocity is still computed from a SMOOTHED copy, so it stays bounded and finite
  expect_true(all(is.finite(out$vertical_velocity[!is.na(out$vertical_velocity)])))
  expect_lt(max(abs(out$vertical_velocity), na.rm = TRUE), 5)

  # widening the window must not change the stored depth at all (it only feeds the derivative)
  out2 <- suppressWarnings(processTagData(list(A01 = d), smoothing = smoothingControl(depth = 30),
                                          downsample.to = NULL, verbose = FALSE))[["A01"]]
  expect_equal(out$depth, out2$depth)
  expect_false(isTRUE(all.equal(out$vertical_velocity, out2$vertical_velocity)))
})

# ------------------------------------------------------------------------------------------------------
# Verbose-output polish: tag summary first, re-processing guard, jerk placement, no negative-zero depth
# ------------------------------------------------------------------------------------------------------

# a tag carrying tag-identity metadata (for the summary line) and a depth that dips a few cm below 0 at
# the surface (to exercise the negative-zero formatting fix), sampled at 20 Hz (so jerk is flagged).
.mk_tagged <- function(id = "A01", secs = 120, rate = 20) {
  set.seed(2); n <- secs * rate
  t0 <- as.POSIXct("2020-01-01", tz = "UTC")
  d <- data.table::data.table(
    ID = id, datetime = t0 + (seq_len(n) - 1) / rate,
    ax = rnorm(n, 0, 0.02), ay = rnorm(n, 0, 0.02), az = 1 + rnorm(n, 0, 0.02),
    gx = rnorm(n, 0, 0.01), gy = rnorm(n, 0, 0.01), gz = rnorm(n, 0, 0.01),
    mx = 20 + rnorm(n, 0, 0.05), my = 5 + rnorm(n, 0, 0.05), mz = -40 + rnorm(n, 0, 0.05),
    # full dive cycles whose surface troughs are clamped a few cm below 0 (residual sensor noise)
    depth = pmax(-0.3, 30 * sin(2 * pi * (seq_len(n) - 1) / (rate * 20))), temp = 20)
  m <- nautilus:::.newNautilusMeta(); m$id <- id
  m$deployment$lat <- 19.7; m$deployment$lon <- -156; m$deployment$datetime <- t0
  m$tag$model <- "CATS"; m$tag$type <- "MS"; m$tag$package_id <- 52
  m$axis_mapping$applied <- TRUE
  nautilus:::new_nautilus_tag(d, m)
}

.pt_lines <- function(tag, v = 2) {
  suppressWarnings(cli::cli_fmt(processTagData(list(A01 = tag), downsample.to = NULL, verbose = v)))
}

test_that("the tag-identity line is emitted first, before the findings block", {
  skip_if_not_installed("signal")
  ln <- .pt_lines(.mk_tagged())
  ln <- ln[nzchar(trimws(ln))]
  i_tag   <- grep("CATS", ln)[1]                       # bullet tag summary
  i_input <- grep("input:", ln)[1]                     # first finding
  i_sub   <- grep("A01 \\(1/1\\)", ln)[1]              # per-deployment sub-header
  expect_true(i_sub < i_tag && i_tag < i_input)        # header -> tag summary -> findings
})

test_that("the jerk caveat is shortened and sits immediately after the motion line", {
  skip_if_not_installed("signal")
  txt <- paste(.pt_lines(.mk_tagged()), collapse = "\n")
  expect_match(txt, "jerk computed at 20 Hz")
  expect_false(grepl("dominated by noise", txt))                       # long clause removed
  expect_match(txt, "coarse activity index only")
  ln <- .pt_lines(.mk_tagged()); ln <- ln[nzchar(trimws(ln))]
  expect_true(grep("jerk computed", ln)[1] == grep("motion:", ln)[1] + 1L)   # directly after motion
})

test_that("re-processing an already-processed dataset is flagged inline and once at the end", {
  skip_if_not_installed("signal")
  once <- suppressWarnings(processTagData(list(A01 = .mk_tagged()), downsample.to = NULL, verbose = FALSE))
  # inline (detailed): a concise per-deployment alert under the tag summary
  txt <- paste(.pt_lines(once[[1]]), collapse = "\n")
  expect_match(txt, "dataset already processed")
  # consolidated (any verbosity): one warning naming the affected deployments
  w <- testthat::capture_warnings(invisible(capture.output(
    processTagData(list(A01 = once[[1]]), downsample.to = NULL, verbose = "quiet"))))
  rw <- w[grepl("already been processed and", w)]
  expect_length(rw, 1L)
  expect_match(rw, "A01")
  # a fresh (never-processed) tag triggers neither
  expect_false(grepl("already processed", paste(.pt_lines(.mk_tagged()), collapse = "\n")))
})

test_that("depth reporting never shows negative zero when the surface dips a few cm below 0", {
  skip_if_not_installed("signal")
  tag <- .mk_tagged()
  ln <- .pt_lines(tag)
  depth_line <- grep("depth:", ln, value = TRUE)       # the "depth: <min> - <max> m" finding
  expect_match(depth_line, "depth: 0 ")                # min renders as 0, not -0
  expect_false(any(grepl("-0", depth_line, fixed = TRUE)))
  # the degree-formatted lines (orientation / offsets) must not print "-0.0"/"-0.00" either
  deg_lines <- grep("orientation:|offsets:", ln, value = TRUE)
  expect_false(any(grepl("-0.0", deg_lines, fixed = TRUE)))
  # and the stored depth is deliberately NOT clamped: the residual sub-surface noise is preserved
  p <- suppressWarnings(processTagData(list(A01 = tag), downsample.to = NULL, verbose = FALSE))[[1]]
  expect_lt(min(p$depth, na.rm = TRUE), 0)
})


# ---- heading reference frame: magnetic vs geographic ------------------------------------------------

test_that("a deployment with no position keeps a MAGNETIC heading instead of aborting", {
  # Calibration quality and reference frame are independent properties. Without a position the magnetic
  # declination is not computable, but the heading itself is still valid - just referenced to magnetic
  # north. Discarding it would lose every rotation-invariant analysis (turning rate, angular velocity,
  # circular variance) for no gain; aborting would lose the whole batch.
  set.seed(1); n <- 3000
  mk <- function(id, with_pos) {
    t0 <- as.POSIXct("2020-08-22 12:00:00", tz = "UTC")
    d <- data.table::data.table(
      ID = id, datetime = t0 + seq_len(n) / 10,
      ax = stats::rnorm(n, 0, .2), ay = stats::rnorm(n, 0, .2), az = 1 + stats::rnorm(n, 0, .2),
      gx = stats::rnorm(n, 0, .05), gy = stats::rnorm(n, 0, .05), gz = stats::rnorm(n, 0, .05),
      mx = 25 + stats::rnorm(n), my = stats::rnorm(n), mz = 40 + stats::rnorm(n),
      depth = pmax(0, 20 + 15 * sin(seq_len(n) / 200)), temp = 18 + stats::rnorm(n, 0, .05))
    m <- nautilus:::.newNautilusMeta(); m$id <- id
    if (with_pos) { m$deployment$lon <- -25.19; m$deployment$lat <- 37.05; m$deployment$datetime <- t0 }
    nautilus:::new_nautilus_tag(d, m)
  }
  tags <- list(WITH_POS = mk("WITH_POS", TRUE), NO_POS = mk("NO_POS", FALSE))

  res <- suppressWarnings(suppressMessages(
    processTagData(tags, verbose = 0, return.data = TRUE)))

  # both survive - the positionless one is not dropped
  expect_setequal(names(res), c("WITH_POS", "NO_POS"))

  # ...and each records WHICH north its heading refers to
  m_geo <- nautilus:::.getMeta(res$WITH_POS); m_mag <- nautilus:::.getMeta(res$NO_POS)
  expect_equal(nautilus:::.headingReference(m_geo), "geographic")
  expect_equal(nautilus:::.headingReference(m_mag), "magnetic")

  # the declination is recorded only where one was applied; the two fields do not contradict
  expect_false(is.na(m_geo$deployment$magnetic_declination))
  expect_true(is.na(m_mag$deployment$magnetic_declination))

  # the heading column is PRESENT and populated in both - the point of keeping it
  expect_true(any(!is.na(res$NO_POS$heading)))
  expect_true(any(!is.na(res$WITH_POS$heading)))
})

test_that("the magnetic-heading warning names the deployment at any verbosity", {
  # a magnetic heading is indistinguishable from a geographic one by inspection - same column, same
  # units, same range - so the warning is the only thing standing between it and a rotated track
  set.seed(2); n <- 2000
  t0 <- as.POSIXct("2020-08-22 12:00:00", tz = "UTC")
  d <- data.table::data.table(
    ID = "NOPOS", datetime = t0 + seq_len(n) / 10,
    ax = stats::rnorm(n, 0, .2), ay = stats::rnorm(n, 0, .2), az = 1 + stats::rnorm(n, 0, .2),
    mx = 25 + stats::rnorm(n), my = stats::rnorm(n), mz = 40 + stats::rnorm(n),
    depth = pmax(0, 20 + 10 * sin(seq_len(n) / 150)))
  m <- nautilus:::.newNautilusMeta(); m$id <- "NOPOS"
  tg <- nautilus:::new_nautilus_tag(d, m)
  # expect_warning() consumes only the MATCHING warning, so the fixture's other expected warnings (no
  # axis mapping, uncalibrated magnetometer) would leak into the suite's warning count. Collect them
  # all and assert on the one under test.
  w <- character(0)
  withCallingHandlers(
    invisible(capture.output(suppressMessages(processTagData(list(NOPOS = tg), verbose = 0)))),
    warning = function(e) { w <<- c(w, conditionMessage(e)); invokeRestart("muffleWarning") })
  expect_true(any(grepl("magnetic heading", w)))
  expect_true(any(grepl("NOPOS", w[grepl("magnetic heading", w)])))
})

test_that(".headingReference distinguishes not-recorded from recorded", {
  # a tag processed before the field existed must read "unknown", so a caller can decline to guess
  # rather than silently assuming geographic
  m <- nautilus:::.newNautilusMeta()
  expect_equal(nautilus:::.headingReference(m), "unknown")
  m$deployment$heading_reference <- "magnetic"
  expect_equal(nautilus:::.headingReference(m), "magnetic")
  m$deployment$heading_reference <- "geographic"
  expect_equal(nautilus:::.headingReference(m), "geographic")
  expect_equal(nautilus:::.headingReference(list()), "unknown")
})

test_that("the magnetic-heading guard fires only for DIRECTIONAL metrics", {
  # The whole point of keeping magnetic headings is that most heading analyses are rotation-invariant.
  # A guard that fired on every magnetic-heading deployment regardless of what was computed would be
  # noise, and noise is how a real warning gets ignored. It must track WHAT IS COMPUTED.
  g <- nautilus:::.warnMagneticHeading
  quiet <- function(...) { w <- character(0)
    withCallingHandlers(g(...), warning = function(e) { w <<- c(w, conditionMessage(e)); invokeRestart("muffleWarning") })
    w }

  # rotation-invariant metrics on a magnetic tag: a constant offset cancels, so nothing is wrong
  expect_length(quiet("PIN_07", c("sd", "rate", "mrl", "range", "iqr"), "x"), 0L)
  # an absolute statistic on the same tag: the offset rotates the answer
  expect_length(quiet("PIN_07", c("sd", "mean", "rate"), "x"), 1L)
  expect_length(quiet(c("A", "B"), "median", "x"), 1L)
  # nothing to warn about when no deployment is magnetic, whatever was requested
  expect_length(quiet(character(0), "mean", "x"), 0L)
  expect_length(quiet(NA_character_, "mean", "x"), 0L)

  # the directional set is exactly the two statistics that report a direction
  expect_setequal(nautilus:::.directionalHeadingMetrics(), c("mean", "median"))
})
