# Tests for the shared signal-core primitives: signed-permutation group algebra,
# from/to <-> matrix translation, the faithful row-wise remap, and motion primitives.

# ---- signed-permutation group algebra -------------------------------------------------

test_that(".isSignedPerm / det / proper-rotation classify correctly", {
  I3 <- diag(3)
  expect_true(nautilus:::.isSignedPerm(I3))
  expect_equal(nautilus:::.signedPermDet(I3), 1L)
  expect_true(nautilus:::.isProperRotation(I3))

  # a 90-degree rotation about z: new_x = -old_y, new_y = old_x  (det +1)
  Rz <- matrix(c(0, -1, 0,
                 1,  0, 0,
                 0,  0, 1), nrow = 3, byrow = TRUE)
  expect_true(nautilus:::.isProperRotation(Rz))

  # a reflection (det -1): swap x/y with both negated, z identity
  Ref <- matrix(c(0, -1, 0,
                 -1,  0, 0,
                  0,  0, 1), nrow = 3, byrow = TRUE)
  expect_true(nautilus:::.isSignedPerm(Ref))
  expect_equal(nautilus:::.signedPermDet(Ref), -1L)
  expect_false(nautilus:::.isProperRotation(Ref))

  # not a signed permutation (two non-zeros in a column)
  bad <- matrix(c(1, 0, 0,
                  1, 0, 0,
                  0, 1, 0), nrow = 3, byrow = TRUE)
  expect_false(nautilus:::.isSignedPerm(bad))
})

test_that("compose and inverse obey the group laws", {
  Rz <- matrix(c(0, -1, 0,  1, 0, 0,  0, 0, 1), nrow = 3, byrow = TRUE)
  Rx <- matrix(c(1, 0, 0,  0, 0, -1,  0, 1, 0), nrow = 3, byrow = TRUE)  # +90 about x

  # apply-then-invert is identity
  expect_equal(nautilus:::.signedPermCompose(nautilus:::.signedPermInverse(Rz), Rz), diag(3))
  # composition of two proper rotations is a proper rotation
  C <- nautilus:::.signedPermCompose(Rx, Rz)
  expect_true(nautilus:::.isProperRotation(C))
  # inverse of a proper rotation is proper
  expect_true(nautilus:::.isProperRotation(nautilus:::.signedPermInverse(Rz)))
})

test_that(".signedPermApplyCols remaps the columns exactly", {
  dt <- data.table::data.table(ax = c(1, 2), ay = c(10, 20), az = c(100, 200))
  Rz <- matrix(c(0, -1, 0,  1, 0, 0,  0, 0, 1), nrow = 3, byrow = TRUE)  # new_x=-old_y, new_y=old_x
  out <- nautilus:::.signedPermApplyCols(dt, Rz, c("ax", "ay", "az"))
  expect_equal(out$ax, c(-10, -20))   # -ay
  expect_equal(out$ay, c(1, 2))       #  ax
  expect_equal(out$az, c(100, 200))   #  az
})

# ---- from/to <-> matrix ----------------------------------------------------------------

test_that(".properRotations returns the 24 det-+1 signed permutations", {
  R <- nautilus:::.properRotations()
  expect_length(R, 24L)
  expect_true(all(vapply(R, nautilus:::.isProperRotation, logical(1))))
  # all distinct
  keys <- vapply(R, function(M) paste(M, collapse = ","), character(1))
  expect_equal(length(unique(keys)), 24L)
})

test_that(".matrixToFromTo round-trips through .mappingToSignedPerm", {
  M <- matrix(c(0, 1, 0,  -1, 0, 0,  0, 0, 1), nrow = 3, byrow = TRUE)   # det +1
  ft <- nautilus:::.matrixToFromTo(M, c("mx", "my", "mz"), c("mx", "my", "mz"))
  M2 <- nautilus:::.mappingToSignedPerm(ft, c("mx", "my", "mz"))
  expect_equal(M2, M)
})

test_that(".mappingToSignedPerm recovers the matrix for a clean (reflection) mapping", {
  # the real CATS mini-diary mapping: ax->-ay, ay->-ax (az implicit identity) -> det -1
  ft <- data.frame(from = c("ax", "ay"), to = c("-ay", "-ax"), stringsAsFactors = FALSE)
  M <- nautilus:::.mappingToSignedPerm(ft, c("ax", "ay", "az"))
  expect_true(nautilus:::.isSignedPerm(M))
  expect_equal(nautilus:::.signedPermDet(M), -1L)   # genuinely a reflection
})

test_that(".mappingToSignedPerm returns NULL for dropped axes or non-permutations", {
  drop <- data.frame(from = c("mx", "my", "mz"), to = c("NA", "NA", "NA"), stringsAsFactors = FALSE)
  expect_null(nautilus:::.mappingToSignedPerm(drop, c("mx", "my", "mz")))

  # degenerate: only ax->az, leaves az duplicated and original az lost
  degen <- data.frame(from = "ax", to = "az", stringsAsFactors = FALSE)
  expect_null(nautilus:::.mappingToSignedPerm(degen, c("ax", "ay", "az")))
})

# ---- family completion: derived gyro map (gyro = det(M)*M) -----------------------------

test_that(".deriveGyroMap returns det(M)*M and is always a proper rotation", {
  # accel = -I (e.g. CATS Camera): det -1, so gyro = -(-I) = I (identity)
  cam <- data.frame(from = c("ax", "ay", "az"), to = c("-ax", "-ay", "-az"), stringsAsFactors = FALSE)
  g   <- nautilus:::.deriveGyroMap(cam)
  Mg  <- nautilus:::.mappingToSignedPerm(g, c("gx", "gy", "gz"))
  expect_equal(Mg, diag(3))
  expect_equal(nautilus:::.signedPermDet(Mg), 1L)

  # accel = swap+negate x/y (CATS mini-diary): det -1, gyro = det(M)*M = -M  (gx<->gy, -gz)
  ms  <- data.frame(from = c("ax", "ay"), to = c("-ay", "-ax"), stringsAsFactors = FALSE)
  Ma  <- nautilus:::.mappingToSignedPerm(ms, c("ax", "ay", "az"))
  Mg2 <- nautilus:::.mappingToSignedPerm(nautilus:::.deriveGyroMap(ms), c("gx", "gy", "gz"))
  expect_equal(Mg2, nautilus:::.signedPermDet(Ma) * Ma)
  expect_equal(nautilus:::.signedPermDet(Mg2), 1L)                 # derived gyro is always proper

  # det +1 accel permutation: gyro shares the SAME permutation (det factor = +1)
  rot <- data.frame(from = c("ax", "ay", "az"), to = c("ay", "-ax", "az"), stringsAsFactors = FALSE)
  Mr  <- nautilus:::.mappingToSignedPerm(rot, c("ax", "ay", "az"))
  Mgr <- nautilus:::.mappingToSignedPerm(nautilus:::.deriveGyroMap(rot), c("gx", "gy", "gz"))
  expect_equal(Mgr, Mr)
})

test_that(".deriveGyroMap returns NULL when the accel map is partial / dropped", {
  drop <- data.frame(from = c("ax", "ay", "az"), to = c("NA", "NA", "NA"), stringsAsFactors = FALSE)
  expect_null(nautilus:::.deriveGyroMap(drop))
})

test_that(".completeFamilies derives a gyro map for an accel-only spec, leaves mag alone", {
  acc <- data.frame(from = c("ax", "ay"), to = c("-ay", "-ax"), stringsAsFactors = FALSE)  # det -1
  out <- nautilus:::.completeFamilies(acc)
  expect_true(all(c("ax", "ay") %in% out$from))                    # accel rows preserved
  gy  <- out[grepl("^-?g", out$from), , drop = FALSE]
  expect_gt(nrow(gy), 0L)                                          # gyro rows added
  Mg  <- nautilus:::.mappingToSignedPerm(gy, c("gx", "gy", "gz"))
  Ma  <- nautilus:::.mappingToSignedPerm(acc, c("ax", "ay", "az"))
  expect_equal(Mg, nautilus:::.signedPermDet(Ma) * Ma)            # gyro = det(M)*M
  expect_equal(nrow(out[grepl("^-?m", out$from), , drop = FALSE]), 0L)  # no mag rows invented
})

test_that(".completeFamilies does not overwrite an explicit gyro map", {
  # accel is a reflection (would derive a non-identity gyro), but an explicit identity gyro is given
  spec <- data.frame(from = c("ax", "ay", "gx", "gy", "gz"),
                     to   = c("-ay", "-ax", "gx", "gy", "gz"),
                     stringsAsFactors = FALSE)
  expect_equal(nautilus:::.completeFamilies(spec), spec)           # unchanged: explicit gyro wins
})

test_that(".completeFamilies leaves a spec with no accel untouched (nothing to derive from)", {
  magonly <- data.frame(from = c("mx", "my"), to = c("-mx", "my"), stringsAsFactors = FALSE)
  expect_equal(nautilus:::.completeFamilies(magonly), magonly)
})

# ---- accel<->gyro co-registration correlation (.coregCorr) ------------------------------

# a co-registered accel(gravity direction) + gyro pair, built via the exact transport identity
# d(ghat)/dt = -omega x ghat: setting omega = d(ghat)/dt x ghat makes -omega x ghat == d(ghat)/dt
# exactly (since |ghat| = 1 => ghat . d(ghat)/dt = 0), so a correct .coregCorr must read ~ +1.
.coreg_pair <- function(fs = 10, n = 1200) {
  t  <- (seq_len(n) - 1) / fs
  th <- 0.6 * sin(2 * pi * t / 10) + 0.9
  ph <- 2 * pi * t / 13
  ghat <- cbind(sin(th) * cos(ph), sin(th) * sin(ph), cos(th))
  xp <- function(a, b) cbind(a[,2]*b[,3]-a[,3]*b[,2], a[,3]*b[,1]-a[,1]*b[,3], a[,1]*b[,2]-a[,2]*b[,1])
  m  <- rbind(ghat[2, ] - ghat[1, ], (ghat[3:n, ] - ghat[1:(n-2), ]) / 2, ghat[n, ] - ghat[n-1, ]) * fs
  list(accel = ghat, gyro = xp(m, ghat), fs = fs)
}

test_that(".coregCorr is ~ +1 for a co-registered pair and ~ -1 when the gyro is sign-reflected", {
  d <- .coreg_pair()
  r <- nautilus:::.coregCorr(d$accel, d$gyro, d$fs)
  expect_gt(r$corr, 0.95)
  expect_gt(r$frac, 0.2)
  expect_gt(r$n, 200L)
  # a uniform sign reflection of the gyro (det -1 relative to accel) inverts the correlation
  expect_lt(nautilus:::.coregCorr(d$accel, -d$gyro, d$fs)$corr, -0.95)
})

test_that(".coregCorr returns NA when there is too little rotation to judge", {
  n <- 1200
  accel <- matrix(rep(c(0, 0, 1), each = n), n, 3)        # gravity fixed -> no gravity-direction motion
  gyro  <- matrix(0, n, 3)                                # no rotation -> every sample fails the omega gate
  r <- nautilus:::.coregCorr(accel, gyro, 10)
  expect_true(is.na(r$corr))
  expect_equal(r$n, 0L)
})

test_that(".coregCorr guards degenerate input (bad fs, too short, mismatched rows)", {
  d <- .coreg_pair(n = 300)
  expect_true(is.na(nautilus:::.coregCorr(d$accel, d$gyro, NA_real_)$corr))
  expect_true(is.na(nautilus:::.coregCorr(d$accel, d$gyro, 0)$corr))
  expect_true(is.na(nautilus:::.coregCorr(d$accel[1:2, ], d$gyro[1:2, ], 10)$corr))
  expect_true(is.na(nautilus:::.coregCorr(d$accel, d$gyro[1:100, ], 10)$corr))
})

test_that(".applyAxisRemap reproduces destination-write semantics with simultaneous swaps", {
  dt <- data.table::data.table(ax = c(1, 2), ay = c(10, 20), az = c(100, 200))
  ft <- data.frame(from = c("ax", "ay"), to = c("-ay", "-ax"), stringsAsFactors = FALSE)
  nautilus:::.applyAxisRemap(dt, ft)
  expect_equal(dt$ax, c(-10, -20))   # new ax = -old ay
  expect_equal(dt$ay, c(-1, -2))     # new ay = -old ax
  expect_equal(dt$az, c(100, 200))   # untouched
  expect_equal(attr(dt, "dropped"), character(0))
})

test_that(".applyAxisRemap drops faulty axes via 'NA'", {
  dt <- data.table::data.table(mx = 1:3, my = 4:6, mz = 7:9)
  ft <- data.frame(from = c("mx", "my"), to = c("NA", "NA"), stringsAsFactors = FALSE)
  nautilus:::.applyAxisRemap(dt, ft)
  expect_true(all(is.na(dt$mx))); expect_true(all(is.na(dt$my)))
  expect_equal(dt$mz, 7:9)
  expect_setequal(attr(dt, "dropped"), c("mx", "my"))
})

# ---- motion primitives -----------------------------------------------------------------

test_that(".staticDynamicAccel splits into gravity + dynamic and sums back", {
  set.seed(1)
  n <- 200
  ax <- 0.1 * sin(seq_len(n) / 5); ay <- rnorm(n, 0, 0.05); az <- 1 + rnorm(n, 0, 0.05)
  sd <- nautilus:::.staticDynamicAccel(ax, ay, az, window = 21)
  # static + dynamic reconstructs the original
  expect_equal(sd$static$z + sd$dynamic$z, az)
  # static z hovers near gravity (1 g)
  expect_lt(abs(mean(sd$static$z) - 1), 0.05)
})

test_that(".verticalSpeed is depth-rate in m/s", {
  t <- as.POSIXct("2020-01-01", tz = "UTC") + (0:4) * 2  # 2-second steps
  depth <- c(0, 2, 6, 6, 0)
  vs <- nautilus:::.verticalSpeed(depth, t)
  expect_true(is.na(vs[1]))
  expect_equal(vs[2:5], c(1, 2, 0, -3))   # (diff depth) / 2 s
})

test_that(".tiltFromAccel returns level angles for a flat tag", {
  tilt <- nautilus:::.tiltFromAccel(0, 0, 1)
  expect_equal(tilt$pitch, 0)
  expect_equal(tilt$roll, 0)
  # pure nose-up: gravity entirely on -x -> pitch +90
  expect_equal(nautilus:::.tiltFromAccel(-1, 0, 0)$pitch, 90)
})

# ---- paddle-wheel magnetometer contamination (.paddleState / .magDenoise) ----------
# A paddle magnet adds a large, additive, ROTATING field. Model: a constant geomagnetic vector B plus an
# oscillating vector p(t) at frequency f; over a rotation p averages to ~0, so a vector running mean
# recovers B, and the field-magnitude inflation mean|m|/|mean m| flags the contamination.
.paddle_cloud <- function(fs = 50, secs = 40, f = 10, amp = 120, B = c(20, 0, 44)) {
  t <- seq_len(fs * secs) / fs
  M <- cbind(B[1] + amp * cos(2 * pi * f * t), B[2] + amp * sin(2 * pi * f * t), B[3] + 0 * t)
  list(M = M, fs = fs, f = f, B = B)
}

test_that(".magDenoise is identity at window<=0 and recovers the mean vector under a rotating oscillation", {
  cl <- .paddle_cloud()
  expect_identical(nautilus:::.magDenoise(cl$M, cl$fs, 0), cl$M)
  expect_identical(nautilus:::.magDenoise(cl$M, cl$fs, -1), cl$M)
  # a window spanning several rotations averages the oscillation away, leaving ~B
  dn <- nautilus:::.magDenoise(cl$M, cl$fs, 1)                    # 10 rotations at f=10 Hz
  ok <- stats::complete.cases(dn)
  expect_lt(max(abs(sweep(dn[ok, ], 2, cl$B))), 3)               # recovered field within a few uT
  expect_lt(stats::sd(sqrt(rowSums(dn[ok, ]^2))), 2)             # magnitude now ~constant
})

test_that(".paddleState flags a large rotating oscillation, ignores a clean field, finds the frequency", {
  cl <- .paddle_cloud(f = 10, amp = 120)
  ps <- nautilus:::.paddleState(cl$M, cl$fs)
  expect_true(ps$present)
  expect_equal(ps$freq, 10, tolerance = 0.6)                     # locates the paddle line
  expect_gt(ps$inflation, 2)                                     # magnitude strongly inflated
  expect_true(ps$recommend.window > 0 && ps$recommend.window <= 3)
  expect_true(isTRUE(ps$separation.ok))                          # 10 Hz >> turning band
  # a clean field with only a slow orientation change + mild noise is NOT flagged
  t <- seq_len(2000) / 50
  clean <- cbind(30 * cos(2 * pi * 0.05 * t), 30 * sin(2 * pi * 0.05 * t), rep(40, 2000)) + matrix(rnorm(6000, 0, 0.5), ncol = 3)
  cs <- nautilus:::.paddleState(clean, 50)
  expect_false(cs$present)
  expect_equal(cs$recommend.window, 0)                           # nothing to de-noise
})

test_that(".paddleState flags a slow paddle (near the turning band) as inseparable", {
  cl <- .paddle_cloud(f = 1.5, amp = 120, secs = 60)             # slow paddle close to fast turning
  ps <- nautilus:::.paddleState(cl$M, cl$fs, turn.band.hz = 0.5)
  expect_true(ps$present)
  expect_false(isTRUE(ps$separation.ok))                         # cannot low-pass it away -> gyro territory
})

test_that(".paddleState guards degenerate input", {
  expect_false(nautilus:::.paddleState(matrix(rnorm(30), 10, 3), 50)$present)   # too few rows
  expect_false(nautilus:::.paddleState(matrix(rnorm(300), 100, 3), 0)$present)  # bad fs
})

# ---- low-latency review orientation (.liteOrientation gyro fusion) ----------------
# Build a fast roll ramp (0 -> 40 deg over 0.4 s, then hold), synthesise the body-frame gravity it
# produces and the matching gyro rate, and check that fusing the gyro (a) keeps the correct sign and
# (b) tracks the bank sooner than the accel-only tilt (which must low-pass the dynamic accel out).
.fast_roll_clip <- function(fs = 25, n = 250, axis = c("roll", "pitch"), amp = 40, t_on = 2, dur = 0.4) {
  axis <- match.arg(axis)
  Rx <- function(a) matrix(c(1, 0, 0, 0, cos(a), -sin(a), 0, sin(a), cos(a)), 3, byrow = TRUE)
  Ry <- function(a) matrix(c(cos(a), 0, sin(a), 0, 1, 0, -sin(a), 0, cos(a)), 3, byrow = TRUE)
  dts <- 1 / fs; tt <- (0:(n - 1)) * dts
  ang <- pmin(amp, pmax(0, (tt - t_on) / dur * amp)); ang[tt < t_on] <- 0
  g <- c(0, 0, 1); acc <- matrix(0, n, 3)
  for (i in seq_len(n)) {
    R <- if (axis == "roll") Rx(ang[i] * pi / 180) else Ry(ang[i] * pi / 180)
    acc[i, ] <- t(R) %*% g
  }
  rate <- c(0, diff(ang)) / dts * pi / 180                 # body-axis angular velocity, rad/s
  gyro <- matrix(0, n, 3); gyro[, if (axis == "roll") 1L else 2L] <- rate
  dt <- data.table::data.table(datetime = as.POSIXct("2020-01-01", tz = "UTC") + tt,
                               ax = acc[, 1], ay = acc[, 2], az = acc[, 3],
                               gx = gyro[, 1], gy = gyro[, 2], gz = gyro[, 3])
  list(dt = dt, t = tt, ang = ang, fs = fs)
}

test_that(".liteOrientation fuses the gyro to track a fast bank sooner, with the right sign", {
  cl <- .fast_roll_clip(axis = "roll", amp = 40)
  fused <- nautilus:::.liteOrientation(cl$dt, cl$fs, low.latency = TRUE)
  accel <- nautilus:::.liteOrientation(cl$dt, cl$fs, low.latency = FALSE)
  reach <- function(v, target = 36) { w <- which(v >= target); if (length(w)) cl$t[w[1]] else Inf }
  # both converge to the true +40 deg (right-side-down => positive roll)
  expect_equal(utils::tail(fused$roll, 1), 40, tolerance = 0.5)
  expect_gt(utils::tail(fused$roll, 1), 0)
  # the fused dial reaches the bank strictly before the accel-only tilt (lower latency)
  expect_lt(reach(fused$roll), reach(accel$roll))
})

test_that(".liteOrientation gyro fusion keeps pitch sign (nose-up positive) and lowers latency", {
  cl <- .fast_roll_clip(axis = "pitch", amp = 30)
  fused <- nautilus:::.liteOrientation(cl$dt, cl$fs, low.latency = TRUE)
  accel <- nautilus:::.liteOrientation(cl$dt, cl$fs, low.latency = FALSE)
  reach <- function(v, target = 27) { w <- which(v >= target); if (length(w)) cl$t[w[1]] else Inf }
  expect_equal(utils::tail(fused$pitch, 1), 30, tolerance = 0.5)
  expect_gt(utils::tail(fused$pitch, 1), 0)
  expect_lt(reach(fused$pitch), reach(accel$pitch))
})

test_that(".liteOrientation falls back to the accel tilt when the gyro is absent or all-NA", {
  cl <- .fast_roll_clip(axis = "roll", amp = 40)
  accel <- nautilus:::.liteOrientation(cl$dt, cl$fs, low.latency = FALSE)
  # no gyro columns at all
  no_gyro <- nautilus:::.liteOrientation(cl$dt[, !c("gx", "gy", "gz")], cl$fs, low.latency = TRUE)
  expect_equal(no_gyro$roll, accel$roll)
  # gyro present but all NA -> identical to the accel-only path
  na_gyro <- data.table::copy(cl$dt); na_gyro[, `:=`(gx = NA_real_, gy = NA_real_, gz = NA_real_)]
  expect_equal(nautilus:::.liteOrientation(na_gyro, cl$fs, low.latency = TRUE)$roll, accel$roll)
})

test_that(".liteOrientation bounds drift across a dead-accel / live-gyro gap (no runaway integration)", {
  # a level animal whose accelerometer drops out mid-clip while the gyro keeps reporting a small
  # constant rate (bias-like). Pure gyro integration would run away (0.1 rad/s ~ 5.7 deg/s); carrying
  # the last finite tilt forward as the anchor must keep the displayed roll bounded near level.
  fs <- 25; gap_s <- 20; n <- fs * (gap_s + 4)
  t0 <- as.POSIXct("2020-01-01", tz = "UTC")
  d <- data.table::data.table(datetime = t0 + (0:(n - 1)) / fs,
                              ax = 0, ay = 0, az = 1,               # level
                              gx = 0.1, gy = 0, gz = 0)             # constant 0.1 rad/s (bias-like)
  gap <- seq.int(fs * 2 + 1L, n - fs * 2L)                          # blank the accelerometer mid-clip
  d[gap, `:=`(ax = NA_real_, ay = NA_real_, az = NA_real_)]
  roll <- nautilus:::.liteOrientation(d, fs, low.latency = TRUE)$roll
  expect_true(all(is.finite(roll[!is.na(roll)])))
  # unbounded integration over the gap would reach ~0.1*20*180/pi ~ 115 deg; the anchor bounds it
  expect_lt(max(abs(roll), na.rm = TRUE), 20)
})

test_that(".peakdet finds the right number of peaks/troughs and gates noise", {
  fs <- 20; t <- seq(0, 10, by = 1 / fs)
  x <- sin(2 * pi * 1.0 * t)                       # 1 Hz over 10 s -> ~10 peaks
  pk <- nautilus:::.peakdet(x, delta = 0.5)
  expect_equal(length(pk$peaks), 10, tolerance = 1)
  expect_equal(length(pk$troughs), 10, tolerance = 1)
  # peaks and troughs alternate (peak indices interleave trough indices)
  expect_true(abs(length(pk$peaks) - length(pk$troughs)) <= 1)
  # a flat (sub-delta) signal yields no beats -> the activity gate
  expect_length(nautilus:::.peakdet(0.05 * sin(2 * pi * t), delta = 0.5)$peaks, 0)
})

test_that(".bandpassSegments removes drift, passes the band, and respects gaps", {
  fs <- 20; t <- seq(0, 30, by = 1 / fs); n <- length(t)
  x <- sin(2 * pi * 1.0 * t) + 3 * t / 30 + 0.5 * sin(2 * pi * 0.02 * t)   # 1 Hz + linear drift + slow wobble
  bp <- nautilus:::.bandpassSegments(x, fs, low = 0.5, high = 3, order = 4)
  expect_equal(length(bp), n)
  # drift removed -> near-zero mean, amplitude ~1 (the 1 Hz component)
  expect_lt(abs(mean(bp, na.rm = TRUE)), 0.1)
  expect_equal(max(bp, na.rm = TRUE), 1, tolerance = 0.2)
  # a gap (NA run) stays NA and doesn't get filled
  xg <- x; xg[200:260] <- NA
  bpg <- nautilus:::.bandpassSegments(xg, fs, low = 0.5, high = 3)
  expect_true(all(is.na(bpg[200:260])))
  expect_false(all(is.na(bpg[1:150])))
})

test_that(".dominantFreq / .bandPower locate a sinusoid's frequency and its power", {
  fs <- 20; t <- seq(0, 30, by = 1 / fs)
  x <- sin(2 * pi * 1.5 * t) + 0.05 * sin(2 * pi * 0.1 * t)   # 1.5 Hz signal + 0.1 Hz drift
  expect_equal(nautilus:::.dominantFreq(x, fs, band = c(0.5, 5)), 1.5, tolerance = 0.05)
  # excluding the low (tether-like) band removes the 0.1 Hz drift power
  pwr_locomotor <- nautilus:::.bandPower(x, fs, band = c(0.5, 5))
  pwr_drift     <- nautilus:::.bandPower(x, fs, band = c(0.02, 0.4))
  expect_gt(pwr_locomotor, pwr_drift)
  expect_true(is.na(nautilus:::.bandPower(c(1, 2), fs, band = c(0.5, 5))))   # too short
})
