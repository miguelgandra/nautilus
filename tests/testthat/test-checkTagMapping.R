# Tests for checkTagMapping(): proper-rotation-only search, per-axis resolution, and the
# accelerometer degeneracy (vertical axis resolved; horizontal axes ambiguous).

# synthetic horizontal-swimming data: constant depth, gravity on a chosen raw axis.
.swim <- function(id = "A01", grav = c(0, 0, 1), n = 600, hz = 10) {
  set.seed(7)
  t <- as.POSIXct("2020-01-01", tz = "UTC") + (seq_len(n) - 1) / hz
  nz <- function() stats::rnorm(n, 0, 0.02)
  dt <- data.table::data.table(
    ID = id, datetime = t, depth = rep(5, n),               # constant depth -> horizontal
    ax = grav[1] + nz(), ay = grav[2] + nz(), az = grav[3] + nz())
  data.table::setattr(dt, "nautilus.version", "test")
  dt
}

run_ctm <- function(d, ...) {
  res <- NULL
  invisible(capture.output(suppressWarnings(suppressMessages(
    res <- checkTagMapping(d, verbose = FALSE, ...)))))
  res
}

# Synthetic-rotation harness: body-frame signals (pitch tracks dives, tail beat on sway, gravity on
# heave) mapped to RAW axes through the inverse of a KNOWN proper rotation M (body = M %*% raw).
# Defined here (before the tests) so every test_that block can use it.
.dive_tag <- function(M, id = "A01", fs = 10, dur = 200, period = 40, amp = 10,
                      U = 3, ftb = 1.0, A = 0.3, seed = 11) {
  set.seed(seed)
  n <- dur * fs
  t <- (seq_len(n) - 1) / fs
  depth <- 20 + amp * sin(2 * pi * t / period)
  vs <- c(0, diff(depth) * fs)
  theta <- -asin(pmax(-0.9, pmin(0.9, vs / U)))
  bx <- -sin(theta)              + stats::rnorm(n, 0, 0.01)
  by <- A * sin(2 * pi * ftb * t) + stats::rnorm(n, 0, 0.01)
  bz <- cos(theta)               + stats::rnorm(n, 0, 0.01)
  raw <- t(M) %*% rbind(bx, by, bz)
  dt <- data.table::data.table(ID = id, datetime = as.POSIXct("2020-01-01", tz = "UTC") + t,
                               depth = depth, ax = raw[1, ], ay = raw[2, ], az = raw[3, ])
  data.table::setattr(dt, "nautilus.version", "test")
  dt
}
# body = M %*% raw : body X(surge)=raw ay ; body Y(sway)=-raw ax ; body Z(heave)=raw az  (det +1)
.M <- matrix(c(0, 1, 0,  -1, 0, 0,  0, 0, 1), nrow = 3, byrow = TRUE)

test_that("only the 24 proper rotations are evaluated (reflections excluded)", {
  r <- run_ctm(list(A01 = .swim()))[["A01"]]
  expect_equal(nrow(r$all_results), 24L)                      # the 24 handedness-preserving candidates
  # a fully-resolved proposal is a proper rotation (det +1) - exercised on dive data below
  rr <- run_ctm(list(A01 = .dive_tag(.M)), stable.horizontal.window = 2)[["A01"]]
  M <- nautilus:::.mappingToSignedPerm(rr$proposal, c("ax", "ay", "az"))
  expect_false(is.null(M)); expect_equal(nautilus:::.signedPermDet(M), 1L)
})

test_that("the vertical axis is resolved but the horizontal axes are ambiguous (the known degeneracy)", {
  r <- run_ctm(list(A01 = .swim(grav = c(0, 0, 1))))[["A01"]]
  res <- r$resolution

  # Z (down) resolved to raw az, sign +1
  z <- res[res$body_axis == "Z", ]
  expect_equal(z$status, "resolved")
  expect_equal(z$source, "az")
  expect_equal(z$sign, 1)

  # X and Y cannot be separated by accelerometer alone
  expect_equal(res$status[res$body_axis == "X"], "ambiguous")
  expect_equal(res$status[res$body_axis == "Y"], "ambiguous")

  # horizontal axes ambiguous -> no (partial, unapplicable) accelerometer remap is proposed; the Z
  # resolution is reported in `resolution` above, not as a single-axis flip that breaks handedness
  expect_equal(nrow(r$proposal), 0L)

  # confidence reports the tie honestly
  expect_gt(r$confidence$n_static_tied, 1)
  expect_false(r$confidence$surge_resolved)
  expect_match(r$confidence$note, "ambiguous")
})

test_that("a non-identity vertical axis is recovered (gravity on raw ay)", {
  r <- run_ctm(list(A01 = .swim(grav = c(0, 1, 0))))[["A01"]]
  z <- r$resolution[r$resolution$body_axis == "Z", ]
  expect_equal(z$status, "resolved")
  expect_equal(z$source, "ay")             # body-down comes from raw ay
  expect_equal(z$sign, 1)
})

test_that("a fully-resolved proposal is directly consumable by applyAxisMapping()", {
  # use a fully-resolved (dive) deployment so the proposal is a complete accelerometer triplet
  r <- run_ctm(list(A01 = .dive_tag(.M)), stable.horizontal.window = 2)[["A01"]]
  expect_equal(nrow(r$proposal), 3L)
  raw <- data.table::data.table(ID = "A01", datetime = as.POSIXct("2020-01-01", tz = "UTC") + 0:2,
                                depth = 1:3, ax = 1:3, ay = 4:6, az = 7:9)
  m <- nautilus:::.newNautilusMeta(); m$id <- "A01"
  raw <- nautilus:::new_nautilus_tag(raw, m)
  out <- NULL
  expect_no_error(invisible(capture.output(suppressMessages(
    out <- applyAxisMapping(list(A01 = raw), r$proposal, verbose = FALSE)))))
  expect_true(tagMetadata(out[["A01"]])$axis_mapping$applied)
})

test_that("returns the structured per-individual shape", {
  r <- run_ctm(list(A01 = .swim()))[["A01"]]
  expect_named(r, c("id", "package_id", "logger_id", "tag", "type", "proposal", "resolution", "families", "confidence", "candidates", "all_results", "frame_state", "metric_explanation"))
  expect_s3_class(r$proposal, "data.frame")
  expect_named(r$proposal, c("from", "to"))
  expect_true(is.list(r$confidence) && !is.null(r$confidence$margin))
  # frame_state is the single source of truth: vertical + surge sub-states, the survivor set, conflicts,
  # and the documented-config validation result (`prior`, "absent" when no configs supplied)
  expect_named(r$frame_state, c("survivors", "vertical", "surge", "conflicts", "prior", "coreg"))
  expect_equal(r$frame_state$prior$status, "absent")
  expect_true(r$frame_state$vertical$status %in% c("resolved", "ambiguous", "conflict"))
  expect_true(r$frame_state$surge$status %in% c("resolved", "ambiguous", "conflict"))
})

# ---- P3 synthetic-rotation harness: known mapping + dives -> full recovery ---------------
# (.dive_tag and .M are defined near the top of the file so all tests can use them.)

test_that("with dives, the full mapping is recovered (surge from depth-rate, sway by handedness)", {
  r <- run_ctm(list(A01 = .dive_tag(.M)), stable.horizontal.window = 2)[["A01"]]
  res <- r$resolution
  expect_true(all(res$status == "resolved"))

  z <- res[res$body_axis == "Z", ]; expect_equal(z$source, "az"); expect_equal(z$sign, 1); expect_equal(z$evidence, "gravity")
  x <- res[res$body_axis == "X", ]; expect_equal(x$source, "ay"); expect_equal(x$sign, 1); expect_equal(x$evidence, "depth-rate")
  y <- res[res$body_axis == "Y", ]; expect_equal(y$source, "ax"); expect_equal(y$sign, -1); expect_equal(y$evidence, "handedness")

  expect_equal(r$proposal$from, c("ay", "ax", "az"))
  expect_equal(r$proposal$to,   c("ax", "-ay", "az"))
  expect_true(r$confidence$surge_resolved)
  expect_lt(r$confidence$surge_corr, -0.3)             # pitch tracks dive (strong negative corr)

  # the recovered proposal is a proper rotation and matches M
  Mr <- nautilus:::.mappingToSignedPerm(r$proposal, c("ax", "ay", "az"))
  expect_equal(nautilus:::.signedPermDet(Mr), 1L)
  expect_equal(Mr, .M)
})

test_that("without dives, surge/sway stay ambiguous (no over-confident guess)", {
  flat <- .dive_tag(.M, amp = 0)                         # constant depth -> no diving signal
  r <- run_ctm(list(A01 = flat), stable.horizontal.window = 2)[["A01"]]
  res <- r$resolution
  expect_equal(res$status[res$body_axis == "Z"], "resolved")     # vertical still resolved
  expect_equal(res$status[res$body_axis == "X"], "ambiguous")
  expect_equal(res$status[res$body_axis == "Y"], "ambiguous")
  expect_false(r$confidence$surge_resolved)
})

test_that("use.dynamics = FALSE leaves the horizontal axes ambiguous even with dives", {
  r <- run_ctm(list(A01 = .dive_tag(.M)), stable.horizontal.window = 2, use.dynamics = FALSE)[["A01"]]
  expect_equal(r$resolution$status[r$resolution$body_axis == "X"], "ambiguous")
  expect_false(r$confidence$surge_resolved)
})

test_that("the attachment-site mirror note is surfaced for pectoral mounts", {
  dt <- .dive_tag(.M)
  m <- nautilus:::.newNautilusMeta(); m$id <- "A01"; m$deployment$attachment_site <- "right_pectoral"
  dt <- nautilus:::new_nautilus_tag(dt, m)
  r <- run_ctm(list(A01 = dt), stable.horizontal.window = 2)[["A01"]]
  expect_equal(r$confidence$attachment_site, "right_pectoral")
  expect_match(r$confidence$note, "mirror-related")
})

test_that("accel-only data reports gyro/mag families as absent", {
  r <- run_ctm(list(A01 = .dive_tag(.M)), stable.horizontal.window = 2)[["A01"]]
  expect_equal(r$families$gyro$status, "absent/insufficient")
  expect_equal(r$families$mag$status, "absent/insufficient")
})

# ---- P3b: full IMU harness (accel + gyro + mag from one consistent orientation) ----------

# Build a full orientation R(t) (pitch tracks dives, roll & yaw oscillate) and derive gravity,
# magnetic field and body angular rates from it, then map each family to raw axes through a known
# (per-family) proper rotation.
.imu_tag <- function(M_acc, M_gyr = M_acc, M_mag = M_acc, id = "A01", fs = 10, dur = 300,
                     seed = 21, incl = 60, amp = 10, pitch.amp = 0) {
  set.seed(seed)
  n <- dur * fs; t <- (seq_len(n) - 1) / fs
  depth <- 20 + amp * sin(2 * pi * t / 40)
  vs <- c(0, diff(depth) * fs)
  # pitch tracks dives; with no dives (amp = 0) an optional independent pitch oscillation keeps the
  # gyro pitch-rate non-constant (so the gyro family can still be decisive without a depth signal).
  theta <- -asin(pmax(-0.9, pmin(0.9, vs / 3))) + (pitch.amp * pi / 180) * sin(2 * pi * t / 17)
  phi   <- (8 * pi / 180) * sin(2 * pi * t / 13)           # roll oscillation
  psi   <- 2 * pi * (t / 120) + 0.2 * sin(2 * pi * 1.0 * t) # yaw: slow heading sweep + tail beat
  Be <- c(cos(incl * pi / 180), 0, sin(incl * pi / 180))   # unit earth field (north + down)
  gE <- c(0, 0, 1)
  gB <- matrix(0, n, 3); mB <- matrix(0, n, 3)
  for (k in 1:n) {
    cph <- cos(phi[k]); sph <- sin(phi[k]); cth <- cos(theta[k]); sth <- sin(theta[k]); cps <- cos(psi[k]); sps <- sin(psi[k])
    Rx <- matrix(c(1, 0, 0, 0, cph, -sph, 0, sph, cph), 3, byrow = TRUE)
    Ry <- matrix(c(cth, 0, sth, 0, 1, 0, -sth, 0, cth), 3, byrow = TRUE)
    Rz <- matrix(c(cps, -sps, 0, sps, cps, 0, 0, 0, 1), 3, byrow = TRUE)
    R <- Rz %*% Ry %*% Rx
    gB[k, ] <- t(R) %*% gE; mB[k, ] <- t(R) %*% Be
  }
  accelB <- gB + matrix(stats::rnorm(n * 3, 0, 0.01), n, 3)
  magB   <- mB + matrix(stats::rnorm(n * 3, 0, 0.005), n, 3)
  gyroB  <- cbind(c(0, diff(phi)) * fs, c(0, diff(theta)) * fs, c(0, diff(psi)) * fs)  # body rates
  raw_a <- accelB %*% M_acc; raw_g <- gyroB %*% M_gyr; raw_m <- magB %*% M_mag         # raw = body %*% M
  dt <- data.table::data.table(ID = id, datetime = as.POSIXct("2020-01-01", tz = "UTC") + t, depth = depth,
                               ax = raw_a[, 1], ay = raw_a[, 2], az = raw_a[, 3],
                               gx = raw_g[, 1], gy = raw_g[, 2], gz = raw_g[, 3],
                               mx = raw_m[, 1], my = raw_m[, 2], mz = raw_m[, 3])
  data.table::setattr(dt, "nautilus.version", "test")
  dt
}

test_that("a shared-frame IMU recovers the same mapping for accel, gyro and mag", {
  r <- run_ctm(list(A01 = .imu_tag(.M)), stable.horizontal.window = 2)[["A01"]]
  expect_true(all(r$resolution$status == "resolved"))

  expect_equal(r$families$gyro$status, "resolved")
  expect_equal(nautilus:::.mappingToSignedPerm(r$families$gyro$mapping, c("gx", "gy", "gz")), .M)

  expect_equal(r$families$mag$status, "resolved")
  expect_equal(nautilus:::.mappingToSignedPerm(r$families$mag$mapping, c("mx", "my", "mz")), .M)
  expect_equal(r$families$mag$measured_inclination, 60, tolerance = 6)

  # combined proposal: 3 rows per resolved family, applies cleanly
  expect_equal(nrow(r$proposal), 9L)
  raw <- nautilus:::new_nautilus_tag(.imu_tag(.M, id = "A01"),
                                     `[[<-`(nautilus:::.newNautilusMeta(), "id", "A01"))
  out <- NULL
  expect_no_error(invisible(capture.output(suppressMessages(
    out <- applyAxisMapping(list(A01 = raw), r$proposal, verbose = FALSE)))))
})

test_that("Phase 3: a gyro in a DIFFERENT native frame than the accel resolves cleanly (no false conflict)", {
  # the gyro is a different proper rotation than the accelerometer (the real hardware case). The
  # strapdown validator must recognise it as a valid rotation and NOT flag a conflict; the composed
  # gyro->body mapping is recovered and proposed.
  Mg <- matrix(c(0, 0, 1, 1, 0, 0, 0, 1, 0), nrow = 3, byrow = TRUE)     # cyclic perm, det +1, != .M
  expect_equal(nautilus:::.signedPermDet(Mg), 1L)
  r <- run_ctm(list(A01 = .imu_tag(.M, M_gyr = Mg)), stable.horizontal.window = 2)[["A01"]]
  expect_equal(r$families$gyro$status, "resolved")
  expect_equal(nautilus:::.mappingToSignedPerm(r$families$gyro$mapping, c("gx", "gy", "gz")), Mg)
  expect_length(r$frame_state$conflicts, 0L)               # a valid different frame is NOT a conflict
  expect_true(all(c("gx", "gy", "gz") %in% r$proposal$from))
})

test_that("Phase 3: a gyro the strapdown cannot validate ABSTAINS (unresolved, no false conflict)", {
  # corrupt the gyro so it is no longer any rotation of the accelerometer (a faulty sensor / non-axis-
  # aligned preprocessing). The strapdown cannot confidently fit it - but "could not confirm" is NOT a
  # conflict (that produced near-universal false alarms on real swimming data). The gyro is left
  # unresolved (abstained) with NO conflict pushed and NO force-fit proposal; the accel is unaffected.
  d <- .imu_tag(.M)
  set.seed(99); n <- nrow(d)
  d$gx <- stats::rnorm(n); d$gy <- stats::rnorm(n); d$gz <- stats::rnorm(n)
  r <- run_ctm(list(A01 = d), stable.horizontal.window = 2)[["A01"]]
  expect_equal(r$families$gyro$status, "unresolved")            # abstain, not "inconsistent"
  expect_false(any(grepl("gyroscop", r$frame_state$conflicts, ignore.case = TRUE)))  # no false conflict
  expect_false(any(r$proposal$from %in% c("gx", "gy", "gz")))   # gyro not proposed
  # the accelerometer frame is unaffected - it still resolves and proposes its own rows
  expect_true(all(r$resolution$status == "resolved"))
  expect_true(all(c("ax", "ay", "az") %in% r$proposal$from))
})

test_that("Tier 2: the strapdown resolver is decisive with a clear fit on posture signal", {
  # the band-limited strapdown (.resolveGyroFrame) must reach a confident, well-separated fit on a tag
  # with posture signal (dive pitch + roll oscillation) - the real-data fix that lifted the correlation
  # from ~0.01 to ~0.7. A same-frame gyro fits IDENTITY (gyro shares the accel axes).
  gf <- nautilus:::.resolveGyroFrame(data.table::as.data.table(.imu_tag(.M)), 10, c("ax", "ay", "az"))
  expect_true(gf$decisive)
  expect_gte(gf$corr, 0.5)
  expect_gte(gf$separation, 0.1)
  expect_equal(gf$R, diag(3))
  # it also reports the co-die (R = identity) co-registration score, which for a shared frame IS the best
  expect_gte(gf$corr_identity, 0.5)
  # a gyro in a different native frame is still decisive, with a different (non-identity) rotation; the
  # co-die (identity) score is then worse than the best-fit rotation
  Mg <- matrix(c(0, 0, 1, 1, 0, 0, 0, 1, 0), 3, byrow = TRUE)
  gf2 <- nautilus:::.resolveGyroFrame(data.table::as.data.table(.imu_tag(.M, M_gyr = Mg)), 10, c("ax", "ay", "az"))
  expect_true(gf2$decisive)
  expect_false(isTRUE(all.equal(gf2$R, diag(3))))
  expect_lt(gf2$corr_identity, gf2$corr)
})

test_that("Phase 3: a co-registered gyro adopts the co-die default and reports coreg", {
  r <- run_ctm(list(A01 = .imu_tag(.M)), stable.horizontal.window = 2)[["A01"]]
  expect_equal(r$families$gyro$status, "resolved")
  expect_equal(r$families$gyro$source, "coreg-derived")            # adopted by co-registration, not resolved from data
  expect_equal(r$frame_state$coreg$status, "ok")
  expect_true(is.finite(r$families$gyro$coreg_corr) && r$families$gyro$coreg_corr > 0.2)
  # the adopted map is the co-die default det(M_acc) * M_acc (= M_acc here: an inferred proper rotation)
  expect_equal(nautilus:::.mappingToSignedPerm(r$families$gyro$mapping, c("gx", "gy", "gz")), .M)
})

test_that("Phase 3: a gyro that does NOT co-register (sign-reflected) fails the co-die check", {
  d <- .imu_tag(.M)
  d[, `:=`(gx = -gx, gy = -gy, gz = -gz)]                          # raw gyro reflected vs accel (det -1: not a rotation)
  r <- run_ctm(list(A01 = d), stable.horizontal.window = 2)[["A01"]]
  expect_equal(r$frame_state$coreg$status, "fail")                 # co-die default decisively rejected
  expect_true(is.finite(r$frame_state$coreg$corr) && r$frame_state$coreg$corr < 0.2)
  # the co-die default is NEVER silently adopted here: a resolved gyro must have come from a data-driven
  # fallback (source != coreg-derived), and coreg_fail is what surfaces it for review.
  if (identical(r$families$gyro$status, "resolved"))
    expect_false(identical(r$families$gyro$source, "coreg-derived"))
})

test_that("frame-consistency: decisive gyro/mag families are NOT proposed while the accel frame is unresolved", {
  # No dive signal -> the accelerometer surge/sway stays unresolved (only Z resolves, by gravity). The
  # gyro and mag still resolve, but only RELATIVE to the representative frame, which is arbitrary among
  # the surge/sway perms. Emitting their remaps would place gyro/mag in a frame the accelerometer is not
  # in. The frame-consistency rule must therefore drop them - nothing is proposed. (Pre-fix, this
  # produced a 6-row gyro+mag proposal with the accelerometer left in the raw frame.)
  r <- run_ctm(list(A01 = .imu_tag(.M, amp = 0, pitch.amp = 6)), stable.horizontal.window = 2)[["A01"]]
  expect_false(all(r$resolution$status == "resolved"))     # accel not fully resolved (no dives)
  expect_equal(r$frame_state$vertical$status, "resolved")  # Z by gravity ...
  expect_equal(r$frame_state$surge$status, "ambiguous")    # ... but surge/sway tied
  expect_equal(r$families$gyro$status, "resolved")         # the families ARE individually decisive ...
  expect_equal(r$families$mag$status, "resolved")
  expect_equal(nrow(r$proposal), 0L)                       # ... yet nothing is proposed (the fix)
})

test_that("plot.file writes a multi-page validation PDF without touching the active device", {
  pfile <- file.path(tempdir(), paste0("ctm_", as.integer(runif(1, 1, 1e7)), ".pdf"))
  on.exit(unlink(pfile), add = TRUE)
  if (!is.null(grDevices::dev.list())) grDevices::graphics.off()
  run_ctm(list(PIN_07 = .imu_tag(.M)), stable.horizontal.window = 2, plot.file = pfile)
  expect_true(file.exists(pfile))
  expect_gt(file.size(pfile), 0)
  expect_null(grDevices::dev.list())
})

test_that(".welchSpectrum bounds the bin count and finds a known peak", {
  fs <- 8; t <- (seq_len(8 * 600) - 1) / fs            # 10 min @ 8 Hz
  x <- sin(2 * pi * 1.2 * t)                            # 1.2 Hz tone
  ps <- nautilus:::.welchSpectrum(x, fs, seg = 1024)
  expect_length(ps$freq, 512L)                          # bounded by seg/2 regardless of series length
  expect_lt(abs(ps$freq[which.max(ps$power)] - 1.2), 0.05)   # peak at the tone
})

test_that(".decimateForPlot shrinks a long line but keeps the envelope (spikes)", {
  x <- seq_len(20000); y <- sin(x / 50); y[12345] <- 99   # an isolated spike
  ds <- nautilus:::.decimateForPlot(x, y, 2000L)
  expect_lt(length(ds$x), 2200L)                          # collapsed to ~target
  expect_equal(max(ds$y), 99)                             # spike survives the envelope
  expect_true(all(diff(ds$x) > 0))                        # x stays ordered
})

test_that("the diagnostic PDF stays bounded for a long, high-rate deployment", {
  # a 2 h x 50 Hz tag (360k rows) must not produce a bloated panel PDF
  set.seed(7); fs <- 50; dur <- 2 * 3600; n <- dur * fs; tm <- (seq_len(n) - 1) / fs
  depth <- 20 + 15 * sin(2 * pi * tm / 45)
  base <- .imu_tag(.M, fs = fs, dur = dur)               # reuse the harness, then overwrite depth length
  pfile <- file.path(tempdir(), paste0("ctm_big_", as.integer(runif(1, 1, 1e7)), ".pdf"))
  on.exit(unlink(pfile), add = TRUE)
  if (!is.null(grDevices::dev.list())) grDevices::graphics.off()
  run_ctm(list(BIG = base), stable.horizontal.window = 2, plot.file = pfile)
  expect_true(file.exists(pfile))
  expect_lt(file.size(pfile), 500 * 1024)                # bounded (pre-optimization this was ~1.8 MB)
})

test_that("the gyroscope is resolved independently of the accelerometer frame", {
  # gyro uses a DIFFERENT proper rotation than accel
  Mg <- matrix(c(1, 0, 0,  0, 0, -1,  0, 1, 0), nrow = 3, byrow = TRUE)   # +90 about x, det +1
  r <- run_ctm(list(A01 = .imu_tag(.M, M_gyr = Mg)), stable.horizontal.window = 2)[["A01"]]
  expect_equal(r$families$gyro$status, "resolved")
  expect_equal(nautilus:::.mappingToSignedPerm(r$families$gyro$mapping, c("gx", "gy", "gz")), Mg)
})

test_that("the magnetometer dip is cross-checked against IGRF when coordinates are present", {
  loc <- list(lon = -25, lat = 38, time = as.POSIXct("2020-09-01", tz = "UTC"))
  igrf <- oce::magneticField(loc$lon, loc$lat, loc$time)$inclination
  dt <- .imu_tag(.M, incl = igrf)                       # synthetic field matches the IGRF value
  m <- nautilus:::.newNautilusMeta(); m$id <- "A01"
  m$deployment$lon <- loc$lon; m$deployment$lat <- loc$lat; m$deployment$datetime <- loc$time
  dt <- nautilus:::new_nautilus_tag(dt, m)
  r <- run_ctm(list(A01 = dt), stable.horizontal.window = 2)[["A01"]]
  expect_equal(r$families$mag$expected_inclination, igrf, tolerance = 0.5)
  expect_lt(abs(r$families$mag$inclination_residual), 5)
  expect_match(r$confidence$note, "IGRF")
})

test_that("a magnetometer with a different VERTICAL axis is flagged inconsistent (not silently mapped)", {
  # mag vertical comes from a different raw axis than the accel/body vertical
  Mm <- matrix(c(1, 0, 0,  0, 0, 1,  0, -1, 0), nrow = 3, byrow = TRUE)   # mag z <- raw my (det +1)
  r <- run_ctm(list(A01 = .imu_tag(.M, M_mag = Mm)), stable.horizontal.window = 2)[["A01"]]
  expect_equal(r$families$mag$status, "inconsistent")
  expect_null(r$families$mag$mapping)
  expect_match(r$confidence$note, "DIFFERENT vertical")
})

# --- console reporting redesign (cli block + inline per-tag warnings) ------------------------------

test_that("verbose output is a cli block (resolved case), not printed data frames", {
  # .M swaps surge/sway (X=ay, Y=-ax) and leaves heave (Z=az) -> a confirmed-Z, remapped-X/Y block
  out <- paste(cli::cli_fmt(suppressWarnings(
    checkTagMapping(list(A01 = .dive_tag(.M)), stable.horizontal.window = 2, verbose = 1))),
    collapse = "\n")
  expect_match(out, "A01 \\(1/1\\)")                 # cli sub-header
  expect_match(out, "resolved: all three axes")
  expect_match(out, "proposed remapping: X = ")      # X/Y are real remaps
  expect_match(out, "evidence: .*gravity")           # the "resolution:" proof line, renamed to "evidence:"
  expect_match(out, "\\(X\\) depth-rate")            # axis-label-only notation (role words dropped)
  expect_false(grepl("surge (X)", out, fixed = TRUE)) # role words removed from the evidence line
  expect_match(out, "secondary sensors:")            # gyro/mag unpacked under this header (was "families:")
  expect_match(out, "gyro:"); expect_match(out, "mag:")
  expect_false(grepl("families:", out, fixed = TRUE)) # the old dense single-line label is gone
  expect_false(grepl("resolution:", out, fixed = TRUE)) # term collision with the sampling grid removed
  expect_false(grepl("interpretation", out))        # redundant interpretation line removed
  expect_false(grepl("body_axis", out))             # the resolution data frame is no longer printed
})

test_that("a no-change deployment is reported as confirmed, not a remap", {
  out <- paste(cli::cli_fmt(suppressWarnings(
    checkTagMapping(list(A01 = .dive_tag(diag(3))), stable.horizontal.window = 2, verbose = 1))),
    collapse = "\n")
  expect_match(out, "confirmed mapping: \\(all raw axes correct\\)")
  expect_false(grepl("proposed remapping", out))
})

test_that("an ambiguous deployment reports a partial / tied block and brackets the unverified axes", {
  out <- paste(cli::cli_fmt(suppressWarnings(
    checkTagMapping(list(B02 = .dive_tag(.M, amp = 0)), stable.horizontal.window = 2, verbose = 1))),
    collapse = "\n")
  expect_match(out, "partial: 1 of 3 axes resolved")
  expect_match(out, "tied across [0-9]+ candidates")
  expect_match(out, "X and Y unverified")
})

test_that("a fully-ambiguous (0 resolved) deployment never claims a confirmed mapping", {
  # tumbling tag: gravity sweeps all axes so even the vertical cannot be resolved -> 0 of 3 axes.
  set.seed(3); fs <- 10; n <- 300 * fs; t <- (seq_len(n) - 1) / fs
  ph <- 2 * pi * t / 37; th <- 2 * pi * t / 53
  d <- data.table::data.table(ID = "A01", datetime = as.POSIXct("2020-01-01", tz = "UTC") + t,
                              depth = 20 + 10 * sin(2 * pi * t / 50),
                              ax = sin(th) + stats::rnorm(n, 0, 0.01),
                              ay = cos(th) * sin(ph) + stats::rnorm(n, 0, 0.01),
                              az = cos(th) * cos(ph) + stats::rnorm(n, 0, 0.01))
  data.table::setattr(d, "nautilus.version", "test")
  r <- run_ctm(list(A01 = d), stable.horizontal.window = 2)[["A01"]]
  expect_true(all(r$resolution$status == "ambiguous"))   # nothing resolved
  out <- paste(cli::cli_fmt(suppressWarnings(
    checkTagMapping(list(A01 = d), stable.horizontal.window = 2, verbose = 1))), collapse = "\n")
  expect_match(out, "unresolved: 0 of 3 axes resolved")  # three-state status (not "partial: 0")
  expect_false(grepl("partial: 0", out))                 # the oxymoron is gone
  expect_match(out, "no remap proposed: .*unverified")
  expect_false(grepl("confirmed mapping", out))          # the paradox: must not claim confirmed
  expect_false(grepl("resolved axes correct", out))
})

test_that("per-tag notices appear inline and are not raised as stacked warnings", {
  # a flat-depth tag with too few rows for the default window -> previously a warning(); now an
  # inline '!' note inside the tag's block, with no R warning condition signalled.
  d <- .swim(n = 50)                                 # short, so stable.horizontal.window can't be met
  expect_no_warning(
    out <- paste(cli::cli_fmt(
      checkTagMapping(list(A01 = d), verbose = 1)), collapse = "\n"))
  expect_match(out, "dataset too short|no stable horizontal periods")
})

test_that("ORDERING GUARD: warns when a pipeline-processed record skipped filterDeploymentData", {
  catch_warns <- function(expr) {
    w <- character(0)
    withCallingHandlers(invisible(capture.output(suppressMessages(expr))),
      warning = function(cnd) { w <<- c(w, conditionMessage(cnd)); invokeRestart("muffleWarning") })
    w
  }
  base <- data.table::as.data.table(.imu_tag(.M))
  # a processing history WITHOUT filterDeploymentData -> the guard warns
  m1 <- nautilus:::.newNautilusMeta(); m1$id <- "A01"
  m1 <- nautilus:::.appendProcessing(m1, "importTagData")
  m1 <- nautilus:::.appendProcessing(m1, "regularizeTimeSeries")
  tag1 <- nautilus:::new_nautilus_tag(data.table::copy(base), m1)
  expect_true(any(grepl("filterDeploymentData", catch_warns(
    checkTagMapping(list(A01 = tag1), stable.horizontal.window = 2, verbose = FALSE)))))

  # adding a filterDeploymentData step silences the guard
  m2 <- nautilus:::.appendProcessing(m1, "filterDeploymentData")
  tag2 <- nautilus:::new_nautilus_tag(data.table::copy(base), m2)
  expect_false(any(grepl("filterDeploymentData", catch_warns(
    checkTagMapping(list(A01 = tag2), stable.horizontal.window = 2, verbose = FALSE)))))
})

# --- conservative proposal + deployment-type-aware scoring -----------------------------------------

test_that("the proposal excludes unresolved axes (no remap on weak evidence)", {
  r <- run_ctm(list(B = .dive_tag(.M, amp = 0)), stable.horizontal.window = 2)[["B"]]   # horizontal ambiguous
  # only the resolved vertical (Z = az) is proposed; surge/sway are NOT
  expect_true(all(r$proposal$to %in% c("az", "-az")))
  expect_false(any(r$resolution$status == "resolved" & r$resolution$body_axis %in% c("X", "Y")))
  expect_gt(nrow(r$candidates), 1)                          # tied set still available for inspection

  r_full <- run_ctm(list(A = .dive_tag(.M)), stable.horizontal.window = 2)[["A"]]        # all resolved
  expect_equal(nrow(r_full$proposal), 3L)
  expect_equal(nautilus:::.mappingToSignedPerm(r_full$proposal, c("ax","ay","az")), .M)
})

test_that("the vertical is resolved from gravity regardless of deployment.type / posture metric", {
  for (dt in list(NULL, "rigid", "towed")) {                # NULL -> unknown -> MAD default
    r <- run_ctm(list(A = .dive_tag(.M)), stable.horizontal.window = 2, deployment.type = dt)[["A"]]
    z <- r$resolution[r$resolution$body_axis == "Z", ]
    expect_equal(z$source, "az"); expect_equal(z$sign, 1); expect_equal(z$evidence, "gravity")
  }
})

test_that("a no-dive deployment gives an honest 'insufficient diving signal' note, never a fabricated reason", {
  # Phase 1: the note is derived strictly from the frame state. With no dive signal the surge anchor
  # cannot run, so the honest note is "insufficient diving signal" - never the old hard-coded
  # "towed tag (pitch decoupled from depth)" (which asserted a decoupling it never measured), and never
  # "weak pitch-depth correlation" (there is no correlation to call weak).
  out <- paste(cli::cli_fmt(suppressWarnings(
    checkTagMapping(list(B = .dive_tag(.M, amp = 0)), stable.horizontal.window = 2,
                    deployment.type = "towed", verbose = 1))), collapse = "\n")
  expect_false(grepl("weak pitch-depth correlation", out))
  expect_false(grepl("pitch decoupled from depth", out))
  expect_match(out, "insufficient diving signal")
})

test_that("the verbose=2 block is a clean dashboard: no clutter, diving line, nested secondary sensors", {
  out <- paste(cli::cli_fmt(suppressWarnings(suppressMessages(
    checkTagMapping(list(A01 = .imu_tag(.M)), stable.horizontal.window = 2, verbose = 2)))),
    collapse = "\n")
  # rule 1: the sampling-frequency and resolution-grid chatter is gone
  expect_false(grepl("sampling:", out, fixed = TRUE))
  expect_false(grepl("resolution grid:", out, fixed = TRUE))
  # rule 2: the dynamic (diving) line with a share-of-record figure, below the horizontal line
  expect_match(out, "horizontal: ")
  expect_match(out, "dynamic \\(diving\\): .*% of record")
  # rules 3+5: gyro/mag unpacked into a nested list with the resolved detail
  expect_match(out, "secondary sensors:")
  expect_match(out, "gyro: resolved \\(co-die default")
  expect_match(out, "mag: (consistent|inconsistent|unresolved|absent)")
})

test_that("deployment.type is validated", {
  expect_error(run_ctm(list(A = .dive_tag(.M)), deployment.type = "bogus"), "rigid")
})

# --- provisional magnetometer hard-iron correction (diagnostic only) -------------------------------

test_that(".hardIronOffset recovers an injected bias and gates on orientation coverage", {
  set.seed(1); n <- 3000; ph <- stats::runif(n, 0, 2 * pi); th <- stats::runif(n, 0, pi)
  sx <- sin(th) * cos(ph) * 50 + 5; sy <- sin(th) * sin(ph) * 50 - 3; sz <- cos(th) * 50 + 2   # sphere + bias
  hi <- nautilus:::.hardIronOffset(sx, sy, sz)
  expect_true(hi$coverage_ok)
  expect_equal(hi$offset, c(5, -3, 2), tolerance = 3)              # recovers the hard-iron offset
  expect_equal(hi$half_range, c(50, 50, 50), tolerance = 5)        # per-axis robust half-range (soft-iron scale)
  # a near-constant axis (animal barely rotates) -> coverage insufficient, not usable
  expect_false(nautilus:::.hardIronOffset(sx, sy, rep(2, n))$coverage_ok)
})

test_that("hard-iron is wired into checkTagMapping but conservatively gated on coverage", {
  # yaw-dominated swimming under-samples the vertical mag axis, so a hard-iron offset there is
  # under-determined; with the default mag.hard.iron = TRUE the coverage gate still refuses it.
  d <- .imu_tag(diag(3))
  d[, `:=`(mx = mx + 3, my = my - 2, mz = mz + 1.5)]              # inject a hard-iron offset
  r <- run_ctm(list(A = d), stable.horizontal.window = 2)[["A"]]
  expect_false(r$families$mag$hard_iron_applied)                  # gate refuses the under-determined offset
  expect_true("hard_iron_applied" %in% names(r$families$mag))     # but the diagnostic is reported
})

# --- paddle-wheel magnetometer de-noising (mirrors processTagData) ---------------------------------

# a shared-frame IMU tag carrying tag metadata (paddle flag) and an optional high-frequency tone on the
# magnetometer, emulating the spinning-magnet noise of a paddle wheel. The base tag is deterministic,
# so OFF/ON runs differ only by the paddle flag.
.paddle_tag <- function(paddle, noise_amp = 0, id = "A01") {
  dt <- .imu_tag(.M, id = id)
  if (noise_amp > 0) {
    t <- (seq_len(nrow(dt)) - 1) / 10
    tone <- noise_amp * sin(2 * pi * 3 * t)                       # 3 Hz paddle-like tone
    dt[, `:=`(mx = mx + tone, my = my + 0.9 * tone, mz = mz + 1.1 * tone)]
  }
  m <- nautilus:::.newNautilusMeta(); m$id <- id; m$tag$paddle_wheel <- paddle
  nautilus:::new_nautilus_tag(dt, m)
}

test_that("paddle-wheel magnetometer noise does not derail the mapping", {
  # a strong high-frequency tone on the magnetometer is suppressed (the 1 Hz resolution decimation is
  # itself an anti-aliasing low-pass, with the paddle pre-smooth as a further safeguard), so the
  # magnetometer still resolves correctly to the known mapping with a low dip variance.
  on <- run_ctm(list(A01 = .paddle_tag(TRUE, noise_amp = 0.3)), stable.horizontal.window = 2)[["A01"]]
  expect_equal(on$families$mag$status, "resolved")
  expect_equal(nautilus:::.mappingToSignedPerm(on$families$mag$mapping, c("mx", "my", "mz")), .M)
  expect_lt(on$families$mag$angle_sd, 3)
})

test_that("paddle-wheel de-noising does not alter a clean, standard (no-paddle) tag", {
  # a clean tag resolves the mag identically whether or not the paddle path runs
  std <- run_ctm(list(A01 = .paddle_tag(FALSE)), stable.horizontal.window = 2)[["A01"]]
  pad <- run_ctm(list(A01 = .paddle_tag(TRUE)),  stable.horizontal.window = 2)[["A01"]]
  expect_equal(std$families$mag$status, "resolved")
  expect_equal(pad$families$mag$status, "resolved")
  expect_equal(nautilus:::.mappingToSignedPerm(pad$families$mag$mapping, c("mx", "my", "mz")), .M)
})

test_that("the paddle-wheel de-noising note appears only for paddle tags", {
  on  <- paste(cli::cli_fmt(suppressWarnings(suppressMessages(
    checkTagMapping(list(A01 = .paddle_tag(TRUE)),  verbose = 2, stable.horizontal.window = 2)))), collapse = "\n")
  off <- paste(cli::cli_fmt(suppressWarnings(suppressMessages(
    checkTagMapping(list(A01 = .paddle_tag(FALSE)), verbose = 2, stable.horizontal.window = 2)))), collapse = "\n")
  expect_match(on, "paddle-wheel-induced magnetic noise")
  expect_no_match(off, "paddle-wheel-induced magnetic noise")
})

# --- robustness to degenerate / all-NA sensor channels --------------------------------------------

test_that(".safeCor returns NA instead of erroring on degenerate input", {
  expect_true(is.na(nautilus:::.safeCor(c(NA, NA, NA), c(1, 2, 3))))   # no complete pairs (would error in cor)
  expect_true(is.na(nautilus:::.safeCor(c(1, 1, 1, 1), c(1, 2, 3, 4)))) # constant series
  expect_true(is.na(nautilus:::.safeCor(c(1, 2), c(3, 4))))            # < 3 complete pairs
  expect_equal(nautilus:::.safeCor(c(1, 2, 3, 4), c(2, 4, 6, 8)), 1)   # ordinary correlation
})

test_that("all-NA gyroscope channels are handled gracefully (no 'no complete element pairs' crash)", {
  # camera-tag case: gyro columns present in the schema but empty -> must not abort checkTagMapping
  d <- .imu_tag(.M)
  d[, `:=`(gx = NA_real_, gy = NA_real_, gz = NA_real_)]
  res <- NULL
  expect_no_error(res <- run_ctm(list(A01 = d), stable.horizontal.window = 2))
  expect_equal(res$A01$families$gyro$status, "absent/insufficient")    # gyro unresolved, not a crash
  expect_true(all(res$A01$resolution$status %in% c("resolved", "ambiguous")))  # accel path still works
})

# --- downsampling for performance (resolution runs on a decimated grid) ----------------------------

test_that(".decimate block-means to the target rate and preserves the mean level", {
  t  <- as.POSIXct("2020-01-01", tz = "UTC") + (0:99) / 10        # 10 Hz, 10 s
  dt <- data.table::data.table(datetime = t, x = rep(1:10, each = 10))   # one level per second
  dec <- nautilus:::.decimate(dt, "x", "datetime", 1)             # -> 1 Hz
  expect_equal(nrow(dec), 10L)                                    # ten 1-second buckets
  expect_equal(dec$x, as.numeric(1:10))                          # each bucket's mean
  expect_s3_class(dec$datetime, "POSIXct")
})

test_that("a high-rate (50 Hz) tag resolves correctly via the decimated grid", {
  # the whole resolution runs on a 1 Hz copy; a 50 Hz shared-frame IMU must still recover the mapping
  r <- run_ctm(list(A01 = .imu_tag(.M, fs = 50)), stable.horizontal.window = 2)[["A01"]]
  expect_true(all(r$resolution$status == "resolved"))
  expect_equal(nautilus:::.mappingToSignedPerm(r$proposal[r$proposal$to %in% c("ax", "-ay", "az"), ],
                                               c("ax", "ay", "az")), .M)
  expect_equal(r$families$gyro$status, "resolved")               # gyro resolves at 1 Hz (slow roll/pitch rates)
  expect_equal(r$families$mag$status, "resolved")                # mag dip resolves at 1 Hz
})

test_that("the removed n.cores argument is rejected", {
  expect_error(run_ctm(list(A01 = .dive_tag(diag(3))), n.cores = 2), "unused argument")
})

# --- run summary, fault tolerance, and locomotor.axis ---------------------------------------------

test_that("the console summary breaks down resolved / partial / unresolved", {
  amb <- function(id) { set.seed(3); fs <- 10; n <- 300 * fs; t <- (seq_len(n) - 1) / fs
    ph <- 2 * pi * t / 37; th <- 2 * pi * t / 53
    d <- data.table::data.table(ID = id, datetime = as.POSIXct("2020-01-01", tz = "UTC") + t,
           depth = 20 + 10 * sin(2 * pi * t / 50), ax = sin(th) + stats::rnorm(n, 0, .01),
           ay = cos(th) * sin(ph) + stats::rnorm(n, 0, .01), az = cos(th) * cos(ph) + stats::rnorm(n, 0, .01))
    data.table::setattr(d, "nautilus.version", "test"); d }
  dat <- list(A = .imu_tag(.M, id = "A"), B = amb("B"))
  out <- paste(cli::cli_fmt(suppressWarnings(suppressMessages(
    checkTagMapping(dat, stable.horizontal.window = 2, verbose = 1)))), collapse = "\n")
  expect_match(out, "fully resolved:")                 # parenthetical axis counts dropped (redundant with prefix)
  expect_match(out, "partially resolved:")
  expect_match(out, "unresolved:")
  expect_false(grepl("(3/3)", out, fixed = TRUE))      # the (n/3) counts are gone
})

test_that("a failing tag is isolated (fault tolerance) and counted, not aborting the batch", {
  good <- .imu_tag(.M, id = "GOOD")
  n <- 600
  bad <- data.table::data.table(ID = "BAD", datetime = as.POSIXct("2020-01-01", tz = "UTC") + (0:(n - 1)) / 10,
                                depth = 10, ax = 0, ay = 0, az = 0)        # degenerate -> errors internally
  data.table::setattr(bad, "nautilus.version", "test")
  res <- NULL
  out <- paste(cli::cli_fmt(suppressWarnings(suppressMessages(
    res <- checkTagMapping(list(GOOD = good, BAD = bad), stable.horizontal.window = 2, verbose = 1)))), collapse = "\n")
  expect_true(all(res$GOOD$resolution$status == "resolved"))   # good tag fine; batch did not abort
  expect_match(out, "failed")                                  # failure surfaced in the summary
})

test_that(".tailBeatCorroboration honours locomotor.axis", {
  fs <- 8; t <- (seq_len(8 * 200) - 1) / fs; n <- length(t)
  tb <- data.table::data.table(datetime = as.POSIXct("2020-01-01", tz = "UTC") + t,
          ax = stats::rnorm(n, 0, 0.01), ay = stats::rnorm(n, 0, 0.01), az = sin(2 * pi * 1 * t))  # tone on heave
  best <- data.frame(newX_col = "ax", newX_sign = 1, newY_col = "ay", newY_sign = 1,
                     newZ_col = "az", newZ_sign = 1, stringsAsFactors = FALSE)
  r_h <- nautilus:::.tailBeatCorroboration(tb, best, "ax", "ay", "az", fs, c(0.2, 3), loco.axis = "heave")
  r_s <- nautilus:::.tailBeatCorroboration(tb, best, "ax", "ay", "az", fs, c(0.2, 3), loco.axis = "sway")
  expect_true(r_h$corroborated)    # heave carries the oscillation -> heave corroborates
  expect_false(r_s$corroborated)   # sway does not
})

test_that("locomotor.axis is validated", {
  expect_error(run_ctm(list(A01 = .dive_tag(diag(3))), locomotor.axis = "bogus"), "should be one of")
})

test_that("the diagnostic PDF starts with a run-summary page (multi-tag run)", {
  dat <- list(A = .imu_tag(.M, id = "A"), B = .imu_tag(.M, id = "B"))
  pfile <- file.path(tempdir(), paste0("ctm_sum_", as.integer(runif(1, 1, 1e7)), ".pdf"))
  on.exit(unlink(pfile), add = TRUE)
  if (!is.null(grDevices::dev.list())) grDevices::graphics.off()
  run_ctm(dat, stable.horizontal.window = 2, plot.file = pfile)
  expect_true(file.exists(pfile) && file.size(pfile) > 0)   # summary page + 2 panels rendered
  expect_null(grDevices::dev.list())                        # device cleaned up
})

test_that("the summary dashboard renders diverse records (resolved/partial/failed/NA/long IDs, both glyph modes)", {
  ax3 <- function(x, y, z) stats::setNames(c(x, y, z), c("X", "Y", "Z"))
  recs <- list(
    list(id = "RESOLVED_TAG", status = "resolved", n_resolved = 3L, gyro = "resolved", mag = "resolved",
         model = "CATS", package_id = "PKG_02", axes = ax3(TRUE, TRUE, TRUE), remap = "Proposed",
         tailbeat = TRUE, static_secs = 132, diving_pct = 45, surge_corr = -0.92, mag_resid = 3, site = "dorsal"),
    list(id = "A_VERY_LONG_TAG_IDENTIFIER_2023", status = "partial", n_resolved = 2L, gyro = "unresolved",
         mag = "inconsistent", model = "LITTLELEONARDO_W190", package_id = "PKG_07",
         axes = ax3(TRUE, FALSE, TRUE), remap = "None", tailbeat = NA,
         static_secs = 20, diving_pct = 2, surge_corr = NA, mag_resid = 22, site = "left pectoral"),  # all flagged
    list(id = "FAILED_TAG", status = "failed", n_resolved = NA, model = "CATS", package_id = "PKG_09",
         message = "magnetometer all-NA"),
    list(id = "SKIP_TAG", status = "skipped", n_resolved = 0L, model = "CATS", package_id = "PKG_01",
         message = "no horizontal data"))
  stats <- list(n_animals = 4, n_full = 1, n_part = 1, n_unres = 0, n_skip = 1, n_fail = 1, locomotor.axis = "sway")
  if (!is.null(grDevices::dev.list())) grDevices::graphics.off()

  # ASCII-fallback path on the base pdf device (production pairs !use_unicode with pdf())
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf), add = TRUE)
  grDevices::pdf(pf, width = 10, height = 7)
  expect_no_error(nautilus:::.drawTagMappingSummaryPage(recs, stats, use_unicode = FALSE))
  expect_no_error(nautilus:::.drawTagMappingSummaryPage(list(), stats, use_unicode = FALSE))  # empty batch
  grDevices::dev.off()
  expect_true(file.exists(pf) && file.size(pf) > 0)

  # unicode glyph path on cairo_pdf (production pairs use_unicode with cairo_pdf), only when cairo can
  # actually start - capabilities("cairo") can be TRUE on headless machines where cairo_pdf() still fails.
  if (nautilus:::.cairoOk()) {
    pf2 <- tempfile(fileext = ".pdf"); on.exit(unlink(pf2), add = TRUE)
    grDevices::cairo_pdf(pf2, width = 10, height = 7)
    expect_no_error(nautilus:::.drawTagMappingSummaryPage(recs, stats, use_unicode = TRUE))
    grDevices::dev.off()
    expect_true(file.exists(pf2) && file.size(pf2) > 0)
  }
})

# A mount pitched ~40 deg about the sway axis splits gravity between raw surge and raw heave during level
# swimming, so two verticals are near-equally gravity-aligned (a vertical gravity tie). The dive dynamics
# cannot break this tie (pitch<->depth-rate fixes surge but is blind to the heave choice), so the frame
# must be reported fully ambiguous - never surge/sway "resolved" on top of an unresolved heave.
.pitch_tilt_tag <- function(phi_deg = 40, id = "A01", fs = 10, dur = 600, U = 2.5, A = 0.3,
                            ftb = 1.0, seed = 11) {
  set.seed(seed); n <- dur * fs; t <- (seq_len(n) - 1) / fs
  seg <- 20; ramp <- 10; cyc <- 2 * (seg + ramp); ph <- t %% cyc; depth <- numeric(n); rate <- 2.2
  for (i in seq_len(n)) { p <- ph[i]
    depth[i] <- if (p < seg) 10 else if (p < seg + ramp) 10 + rate * (p - seg)
                else if (p < 2 * seg + ramp) 10 + rate * ramp
                else 10 + rate * ramp - rate * (p - (2 * seg + ramp)) }
  vs <- c(0, diff(depth) * fs); theta <- -asin(pmax(-0.9, pmin(0.9, vs / U)))
  bx <- -sin(theta) + stats::rnorm(n, 0, 0.01); by <- A * sin(2 * pi * ftb * t) + stats::rnorm(n, 0, 0.01)
  bz <- cos(theta) + stats::rnorm(n, 0, 0.01)
  phi <- phi_deg * pi / 180
  Ry <- matrix(c(cos(phi), 0, sin(phi), 0, 1, 0, -sin(phi), 0, cos(phi)), 3, byrow = TRUE)
  raw <- Ry %*% rbind(bx, by, bz)
  dt <- data.table::data.table(ID = id, datetime = as.POSIXct("2020-01-01", tz = "UTC") + t,
                               depth = depth, ax = raw[1, ], ay = raw[2, ], az = raw[3, ])
  data.table::setattr(dt, "nautilus.version", "test"); dt
}

test_that("a gravity-tied vertical never resolves the heave/surge or proposes a remap (heave undetermined)", {
  r <- run_ctm(list(A01 = .pitch_tilt_tag()), stable.horizontal.window = 2)[["A01"]]
  expect_false(is.null(r))
  expect_false(r$confidence$vertical_resolved)             # the vertical is genuinely gravity-tied here
  res <- r$resolution

  # The safety invariant: while the heave is gravity-tied it must stay ambiguous, no single fully-resolved
  # frame may be claimed, and no (partial, handedness-broken) remap proposed. Phase 2 note: a sway axis
  # that is INVARIANT across the plausible verticals (here the tilt axis) MAY resolve by survivor
  # agreement - that is sound; what must never happen is a resolved heave or a proposal over a tied frame.
  expect_equal(res$status[res$body_axis == "Z"], "ambiguous")   # heave undetermined
  expect_false(all(res$status == "resolved"))                   # never a single fully-resolved frame
  expect_false(r$confidence$surge_resolved)                     # global surge never resolved over a tied heave
  expect_equal(nrow(r$proposal), 0L)                            # no remap proposed
})

test_that("a gravity-ambiguous vertical reports candidates spanning ALL plausible verticals, narrowed by dive evidence", {
  # The tilt splits gravity between two axes, so the true/consensus vertical is plausible but not the
  # gravity-best one. The candidate set must SPAN both verticals, else consensusAxisMapping()'s
  # consistency gate would reject a group consensus on the competing vertical (the towed-tag non-rescue
  # bug). Phase 2 additionally NARROWS each vertical to its dive-resolved surge perm (one frame per
  # vertical), so consensus also respects this tag's own dive evidence.
  r <- run_ctm(list(A01 = .pitch_tilt_tag()), stable.horizontal.window = 2)[["A01"]]
  expect_false(r$confidence$vertical_resolved)
  zc <- unique(r$candidates[, c("newZ_col", "newZ_sign")])
  expect_gt(nrow(zc), 1L)                                  # candidates span more than one vertical (consensus reachable)
  expect_true(r$frame_state$surge$within_vertical)         # the surge was resolved inside each vertical
  expect_equal(nrow(r$candidates), nrow(zc))               # narrowed to one frame per plausible vertical
  expect_equal(r$confidence$n_static_tied, nrow(r$candidates))   # reported tie count matches the set
})

test_that("per-vertical surge narrowing is gated on use.dynamics (no dynamics -> full candidate set kept)", {
  r <- run_ctm(list(A01 = .pitch_tilt_tag()), stable.horizontal.window = 2, use.dynamics = FALSE)[["A01"]]
  expect_false(r$confidence$vertical_resolved)
  expect_false(r$frame_state$surge$within_vertical)        # nothing was narrowed
  # without the dive anchor every surge/sway perm of every plausible vertical is retained (>1 per vertical)
  expect_gt(nrow(r$candidates), nrow(unique(r$candidates[, c("newZ_col", "newZ_sign")])))
})

test_that("a gravity-RESOLVED vertical keeps the narrow 4-candidate set (no regression)", {
  r <- run_ctm(list(A01 = .swim(grav = c(0, 0, 1))))[["A01"]]
  expect_true(r$confidence$vertical_resolved)
  expect_equal(nrow(r$candidates), 4L)                                         # one vertical, 4 perms
  expect_equal(nrow(unique(r$candidates[, c("newZ_col", "newZ_sign")])), 1L)
})

test_that("surge_resolved never holds while the vertical is unresolved (handedness needs a known heave)", {
  # across a clean dive tag (resolvable) and the tilted tie (unresolvable), the implication must hold
  for (d in list(.dive_tag(.M), .pitch_tilt_tag())) {
    cf <- run_ctm(list(A01 = d), stable.horizontal.window = 2)[["A01"]]$confidence
    if (isTRUE(cf$surge_resolved)) expect_true(cf$vertical_resolved)
  }
})


# ---- configs: validate a documented axis_config against the inferred frame (Phase 2b) -----------

.with_cfg <- function(d, cfg) {
  d <- data.table::as.data.table(d); m <- nautilus:::.getMeta(d)
  m$tag$axis_config <- cfg; nautilus:::.setMeta(d, m); d
}
# a configs dictionary: the true mapping (== .M, what a clean dive tag resolves to), a wrong proper
# rotation (identity), and a reflection (det -1, outside the right-handed search).
.cfg_dict <- list(
  TRUE_CFG = data.frame(from = c("ay", "ax", "az"), to = c("ax", "-ay", "az"), stringsAsFactors = FALSE),
  WRONG    = data.frame(from = c("ax", "ay", "az"), to = c("ax", "ay", "az"),  stringsAsFactors = FALSE),
  REFL     = data.frame(from = c("ax", "ay"),       to = c("-ay", "-ax"),       stringsAsFactors = FALSE)
)

test_that("configs validation: a matching documented config is CONFIRMED, no conflict", {
  r <- run_ctm(list(A01 = .with_cfg(.imu_tag(.M), "TRUE_CFG")), stable.horizontal.window = 2,
               configs = .cfg_dict)[["A01"]]
  expect_equal(r$frame_state$prior$status, "confirmed")
  expect_length(r$frame_state$conflicts, 0L)
  # an accel-only config documents no gyro/mag rows, so those families stay 'absent'
  expect_equal(r$frame_state$prior$gyro_status, "absent")
  expect_equal(r$frame_state$prior$mag_status, "absent")
})

test_that("configs validation: a documented config outside the plausible set CONFLICTS and is flagged", {
  r <- run_ctm(list(A01 = .with_cfg(.imu_tag(.M), "WRONG")), stable.horizontal.window = 2,
               configs = .cfg_dict)[["A01"]]
  expect_equal(r$frame_state$prior$status, "conflict")
  expect_true(any(grepl("conflicts with the data", r$frame_state$conflicts)))
})

test_that("configs validation: a reflection config is reported as such, not a conflict", {
  r <- run_ctm(list(A01 = .with_cfg(.imu_tag(.M), "REFL")), stable.horizontal.window = 2,
               configs = .cfg_dict)[["A01"]]
  expect_equal(r$frame_state$prior$status, "reflection")     # det -1: the search can't judge it
  expect_length(r$frame_state$conflicts, 0L)
})

test_that("configs validation: a tag with no documented config is 'absent'", {
  r <- run_ctm(list(A01 = .with_cfg(.imu_tag(.M), NA_character_)), stable.horizontal.window = 2,
               configs = .cfg_dict)[["A01"]]
  expect_equal(r$frame_state$prior$status, "absent")
})

test_that("configs validation: an unknown config name fails that deployment with a clear message", {
  # caught by the per-deployment fault tolerance (a config typo is surfaced per tag, not a global abort)
  msgs <- character(0)
  res <- withCallingHandlers(
    run_ctm(list(A01 = .with_cfg(.imu_tag(.M), "Typo")), stable.horizontal.window = 2, configs = .cfg_dict),
    message = function(m) invokeRestart("muffleMessage"))
  expect_null(res[["A01"]])                                  # the deployment did not produce a result
})


# ---- configs: validate documented gyro/mag rows against the inferred family mappings (Phase 2c) ----
# gyro/mag rows mirror the accel TRUE_CFG structure (the shared-frame .M mapping that .imu_tag(.M)
# resolves all three families to). A family is only judged confirmed/conflict when the ACCEL config is
# itself confirmed (the inferred gyro/mag mappings are expressed relative to the accel body frame).
.cfg_fam <- list(
  FULL_OK  = data.frame(from = c("ay", "ax", "az", "gy", "gx", "gz", "my", "mx", "mz"),
                        to   = c("ax", "-ay", "az", "gx", "-gy", "gz", "mx", "-my", "mz"), stringsAsFactors = FALSE),
  GYRO_BAD = data.frame(from = c("ay", "ax", "az", "gx", "gy", "gz"),
                        to   = c("ax", "-ay", "az", "gx", "gy", "gz"), stringsAsFactors = FALSE),  # accel ok, gyro = identity
  ACC_BAD  = data.frame(from = c("ax", "ay", "az", "gy", "gx", "gz"),
                        to   = c("ax", "ay", "az", "gx", "-gy", "gz"), stringsAsFactors = FALSE))  # accel = identity, gyro ok

test_that("configs validation: matching gyro AND mag rows are CONFIRMED when the accel is confirmed", {
  r <- run_ctm(list(A01 = .with_cfg(.imu_tag(.M), "FULL_OK")), stable.horizontal.window = 2,
               configs = .cfg_fam)[["A01"]]
  expect_equal(r$frame_state$prior$status, "confirmed")          # accel reference is pinned
  expect_equal(r$frame_state$prior$gyro_status, "confirmed")
  expect_equal(r$frame_state$prior$mag_status, "confirmed")
  expect_length(r$frame_state$conflicts, 0L)
})

test_that("configs validation: a wrong documented gyro row CONFLICTS and is flagged", {
  r <- run_ctm(list(A01 = .with_cfg(.imu_tag(.M), "GYRO_BAD")), stable.horizontal.window = 2,
               configs = .cfg_fam)[["A01"]]
  expect_equal(r$frame_state$prior$status, "confirmed")          # accel still confirmed
  expect_equal(r$frame_state$prior$gyro_status, "conflict")
  expect_true(any(grepl("gyroscope config .* conflicts", r$frame_state$conflicts)))
})

test_that("configs validation: a gyro row is UNVERIFIABLE when the accel frame is not confirmed", {
  # the gyro doc is actually correct, but the accel config is wrong (identity) -> the inferred gyro
  # mapping is expressed in a frame not pinned to the doc, so the gyro must NOT be judged.
  r <- run_ctm(list(A01 = .with_cfg(.imu_tag(.M), "ACC_BAD")), stable.horizontal.window = 2,
               configs = .cfg_fam)[["A01"]]
  expect_equal(r$frame_state$prior$status, "conflict")           # accel disagrees with the data
  expect_equal(r$frame_state$prior$gyro_status, "unverifiable")  # gated: not judged off an unconfirmed accel
  # the only conflict recorded is the accel one - no spurious gyro conflict
  expect_false(any(grepl("gyroscope config", r$frame_state$conflicts)))
})

test_that(".resolveSurge excludes physically-impossible vertical speeds (max.vertical.speed guard)", {
  # A depth-sensor artifact (a stuck / saturated block) injects a handful of samples with |vertical
  # velocity| far beyond anything an animal can swim. Being extreme outliers, those few samples dominate
  # the Pearson pitch <-> depth-rate correlation and silently destroy the surge decision. The upper bound
  # excludes them. This reproduces the PIN_CAM_13 failure in miniature.
  n  <- 600L; tt <- seq_len(n)
  vs <- 1.5 * sin(2 * pi * tt / 120)                 # clean dives, +/- 1.5 m/s
  sx <- 0.05 * vs                                     # surge = x: static x tracks vs -> pitch anti-correlates
  sy <- 0.02 * sin(2 * pi * tt / 70)                 # sway: small, ~uncorrelated with vs
  sz <- rep(1, n)                                     # heave: ~1 g
  # inject the artifact block (as a stuck depth block produces at the step edge)
  k  <- 25L
  vs <- c(vs, rep(271, k)); sx <- c(sx, rep(0, k)); sy <- c(sy, rep(0.02, k)); sz <- c(sz, rep(1, k))
  fd   <- data.table::data.table(vertical_speed = vs)
  sd   <- list(static = list(x = sx, y = sy, z = sz))
  near <- data.frame(newX_col = c("ax", "ay"), newX_sign = c(1, 1),
                     newY_col = c("ay", "ax"), newY_sign = c(1, 1),
                     newZ_col = c("az", "az"), newZ_sign = c(1, 1), stringsAsFactors = FALSE)
  rs <- function(mv) nautilus:::.resolveSurge(fd, near, sd, "ax", "ay", "az",
                                              dive.speed.threshold = 0.2, min.dynamic.corr = 0.4,
                                              max.vertical.speed = mv)
  bounded   <- rs(5)                                  # the default guard
  unbounded <- rs(1e9)                                # the pre-fix behaviour
  expect_true(bounded$decisive)                       # spike excluded -> surge resolves
  expect_identical(bounded$best_index, 1L)            # ... to the correct (x) surge axis
  expect_lt(bounded$best_corr, -0.9)                  # strong pitch <-> depth-rate anti-correlation
  expect_false(isTRUE(unbounded$decisive))            # spike included -> correlation collapses, no decision
  expect_gt(unbounded$best_corr, bounded$best_corr)   # ... measurably degraded (less negative)
})

test_that("checkTagMapping validates max.vertical.speed (must exceed dive.speed.threshold)", {
  expect_error(run_ctm(list(A01 = .swim()), max.vertical.speed = -1), "max.vertical.speed")
  expect_error(run_ctm(list(A01 = .swim()), max.vertical.speed = 0.1, dive.speed.threshold = 0.2), "must exceed")
})

test_that("the validation panel's surge correlation matches the reported surge_corr under a depth artifact", {
  # Regression: a depth-sensor spike must not make the diagnostic PANEL contradict the resolved decision.
  # The panel recomputes its own surge-anchor r; it must exclude the same artifact speeds the resolver
  # does, so the plotted r == confidence$surge_corr (not a collapsed value under a green 'pass' box).
  d <- data.table::copy(.dive_tag(.M))                 # clean, fully-resolvable dives
  n <- nrow(d); d[(n - 400L):(n - 200L), depth := 3000]  # inject a stuck-depth artifact block
  data.table::setattr(d, "nautilus.version", "test")
  captured <- new.env()
  orig <- nautilus:::.plotTagMappingPanel
  assignInNamespace(".plotTagMappingPanel",
                    function(pd, use_unicode = TRUE) { assign(pd$id, pd$sa$r, envir = captured); invisible() },
                    ns = "nautilus")
  on.exit(assignInNamespace(".plotTagMappingPanel", orig, ns = "nautilus"), add = TRUE)
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf), add = TRUE)
  res <- run_ctm(list(A01 = d), plot.file = pf)[["A01"]]
  panel_r <- get("A01", envir = captured)
  expect_true(isTRUE(res$frame_state$surge$status == "resolved"))     # surge still resolves (artifact excluded)
  expect_equal(panel_r, res$confidence$surge_corr, tolerance = 0.05)  # panel r == reported surge_corr
  expect_lt(panel_r, -0.5)                                            # ... and both strong (not collapsed)
})
