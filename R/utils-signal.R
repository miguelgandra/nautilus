#######################################################################################################
# Shared signal-processing primitives #################################################################
#######################################################################################################
#
# Low-level, dependency-free building blocks reused across the package: the signed-permutation group
# algebra that underpins axis remapping (importTagData / applyAxisMapping / checkTagMapping) and a
# handful of motion primitives (static/dynamic acceleration, vertical speed, tilt). Keeping a single
# implementation here avoids the per-function duplication that had crept in.
#
# Axis-mapping model
# ------------------
# A tag's IMU axes relate to the canonical tag (housing) frame by a *signed permutation* of the three
# axes -- one of 48 (3! orderings x 2^3 signs). Physically valid sensor remaps between right-handed
# frames are the 24 with determinant +1 (proper rotations); the other 24 are reflections that silently
# flip handedness (and therefore heading / any cross-product) downstream. We represent a signed
# permutation as a 3x3 integer matrix `M` mapping OLD axes to NEW axes: new = M %*% old, with exactly
# one non-zero (+/-1) per row and per column. Rows/cols are ordered (x, y, z).


#######################################################################################################
# Signed-permutation group algebra ####################################################################
#######################################################################################################

#' Is `M` a valid 3x3 signed-permutation matrix?
#' @keywords internal
#' @noRd
.isSignedPerm <- function(M) {
  if (!is.matrix(M) || !all(dim(M) == c(3, 3))) return(FALSE)
  if (!all(M %in% c(-1, 0, 1))) return(FALSE)
  all(rowSums(M != 0) == 1) && all(colSums(M != 0) == 1)
}

#' Determinant of a signed-permutation matrix (exactly +1 or -1).
#' @keywords internal
#' @noRd
.signedPermDet <- function(M) as.integer(round(det(M)))

#' Is `M` a proper rotation (handedness-preserving, det = +1)?
#' @keywords internal
#' @noRd
.isProperRotation <- function(M) .isSignedPerm(M) && .signedPermDet(M) == 1L

#' Compose two signed permutations: apply A first, then B (result maps old -> new).
#' @keywords internal
#' @noRd
.signedPermCompose <- function(B, A) B %*% A

#' Inverse of a signed permutation (orthogonal integer matrix -> transpose).
#' @keywords internal
#' @noRd
.signedPermInverse <- function(M) t(M)

#' Apply a signed permutation to three columns of a data.table (returns new x/y/z as a list).
#'
#' Reads the three source columns by name and returns the remapped triplet. Pure: does not mutate `dt`.
#' @keywords internal
#' @noRd
.signedPermApplyCols <- function(dt, M, cols) {
  old <- list(dt[[cols[1]]], dt[[cols[2]]], dt[[cols[3]]])
  out <- vector("list", 3L)
  for (i in 1:3) {
    j <- which(M[i, ] != 0)            # the single source axis feeding new axis i
    out[[i]] <- M[i, j] * old[[j]]
  }
  stats::setNames(out, cols)
}

#' The 24 proper-rotation signed permutations (3x3 matrices, determinant +1).
#' @keywords internal
#' @noRd
.properRotations <- function() {
  perms <- list(c(1, 2, 3), c(1, 3, 2), c(2, 1, 3), c(2, 3, 1), c(3, 1, 2), c(3, 2, 1))
  signs <- as.matrix(expand.grid(c(1, -1), c(1, -1), c(1, -1)))
  out <- list()
  for (p in perms) {
    for (s in seq_len(nrow(signs))) {
      sgn <- as.numeric(signs[s, ]); M <- matrix(0, 3, 3)
      for (i in 1:3) M[i, p[i]] <- sgn[i]
      if (.isProperRotation(M)) out[[length(out) + 1L]] <- M
    }
  }
  out
}

#' Express a signed-permutation matrix as a from/to mapping data.frame.
#'
#' @param M 3x3 signed permutation (body = M %*% raw).
#' @param raw_axes Character(3): raw axis names in (x,y,z) order, e.g. c("mx","my","mz").
#' @param body_axes Character(3): destination (body) axis names, e.g. c("mx","my","mz").
#' @return A data.frame with `from` (raw) and `to` (signed body axis).
#' @keywords internal
#' @noRd
.matrixToFromTo <- function(M, raw_axes, body_axes) {
  from <- character(3); to <- character(3)
  for (i in 1:3) {
    j <- which(M[i, ] != 0)
    from[i] <- raw_axes[j]
    to[i]   <- paste0(if (M[i, j] < 0) "-" else "", body_axes[i])
  }
  data.frame(from = from, to = to, stringsAsFactors = FALSE)
}


#######################################################################################################
# Mapping (from/to) <-> signed permutation ############################################################
#######################################################################################################

# The user-facing axis mapping is a data.frame of `from`/`to` rows, where `to` is a destination axis
# optionally sign-flipped ("-ay") or the literal "NA" to drop a faulty axis. These helpers translate
# a single sensor family's rows into the matrix form (for handedness / provenance bookkeeping) and
# back. A family whose rows do not form a clean signed permutation (incomplete, duplicated, or with a
# dropped axis) has no matrix representation -- `.mappingToSignedPerm` returns NULL in that case, and
# callers fall back to the faithful row-wise apply below.

#' Translate one family's from/to rows into an effective signed-permutation matrix, or NULL.
#'
#' Unspecified destinations default to identity (matching the apply semantics). Returns NULL if any
#' row drops an axis ("NA") or if the effective transform is not a valid signed permutation.
#' @param ft data.frame with `from`, `to` (already subset to this family).
#' @param axes Character(3): the family's axis names in (x, y, z) order, e.g. c("ax","ay","az").
#' @keywords internal
#' @noRd
.mappingToSignedPerm <- function(ft, axes) {
  if (any(ft$to == "NA")) return(NULL)                 # dropped axis -> not a permutation
  M <- diag(3)                                          # unspecified destinations stay identity
  for (r in seq_len(nrow(ft))) {
    to   <- ft$to[r]
    sign <- if (grepl("^\\-", to)) -1 else 1
    dest <- sub("^\\-", "", to)
    src  <- ft$from[r]
    di <- match(dest, axes); sj <- match(src, axes)
    if (is.na(di) || is.na(sj)) return(NULL)            # references an axis outside this family
    M[di, ] <- 0                                         # overwrite the identity row for this dest
    M[di, sj] <- sign
  }
  if (!.isSignedPerm(M)) return(NULL)
  M
}

#' Apply axis-mapping rows to sensor columns, faithfully reproducing the historical semantics.
#'
#' Destination-oriented copy-then-write: each row places (optionally negated) source data into the
#' destination column; "NA" drops the source axis. Reads from a snapshot so simultaneous swaps are
#' correct. Operates in place on `dt` (a data.table) and returns it invisibly, along with the set of
#' axes set to NA via attribute "dropped".
#' @param dt A data.table.
#' @param ft data.frame with `from`, `to` (any sensor families).
#' @keywords internal
#' @noRd
.applyAxisRemap <- function(dt, ft) {
  present <- unique(ft$from[ft$from %in% names(dt)])           # snapshot only existing source columns
  snapshot <- lapply(stats::setNames(present, present), function(cn) dt[[cn]])
  dropped <- character(0)
  for (r in seq_len(nrow(ft))) {
    from <- ft$from[r]; to <- ft$to[r]
    if (!from %in% names(snapshot)) next
    if (to == "NA") {
      data.table::set(dt, j = from, value = NA_real_)
      dropped <- c(dropped, from)
    } else if (grepl("^\\-", to)) {
      data.table::set(dt, j = sub("^\\-", "", to), value = -snapshot[[from]])
    } else {
      data.table::set(dt, j = to, value = snapshot[[from]])
    }
  }
  data.table::setattr(dt, "dropped", unique(dropped))
  invisible(dt)
}


#' Derive a gyroscope axis map from the accelerometer map (the co-die default strategy).
#'
#' The gyroscope shares the accelerometer's sensor die on these tags, so its axis PERMUTATION is the
#' same - but angular velocity is an AXIAL vector, so under a reflected accel map it carries the extra
#' determinant sign: gyro = det(M) * M (validated fleet-wide). Returns a gyro `from/to`, or NULL if the
#' accel map is partial/dropped (no derivation possible). This is the DEFAULT only; an explicit gyro map
#' always overrides it (see `.completeFamilies`), and det(M) is the universal axial-vector law - not a
#' tag-specific assumption. The co-die part (that the gyro shares the accel frame) is the default.
#' @param accel_ft data.frame `from`/`to` restricted to accelerometer rows (ax/ay/az).
#' @keywords internal
#' @noRd
.deriveGyroMap <- function(accel_ft) {
  M <- .mappingToSignedPerm(accel_ft, c("ax", "ay", "az"))
  if (is.null(M)) return(NULL)                                   # partial / dropped accel -> cannot derive
  .matrixToFromTo(.signedPermDet(M) * M, c("gx", "gy", "gz"), c("gx", "gy", "gz"))
}


#' Complete a per-family axis map: fill in families the spec omits, using default derivations.
#'
#' The user-facing configs specify the accelerometer only. Since accel/gyro share a die, the gyro map is
#' DERIVED (`.deriveGyroMap`) when absent, so the gyro is co-registered rather than left in the raw frame
#' (the bug this fixes). Explicitly-specified families are never overwritten (an explicit gyro map wins).
#' The magnetometer is left to its own strategy (documented map / per-package calibration) and is not
#' filled here. Deployment-agnostic: only the co-die gyro default lives here, and only as a default.
#' @param ft data.frame `from`/`to` (any families).
#' @keywords internal
#' @noRd
.completeFamilies <- function(ft) {
  fam_of  <- function(v) substr(sub("^-", "", v), 1L, 1L)
  present <- unique(fam_of(ft$from))
  if ("a" %in% present && !("g" %in% present)) {                 # accel mapped, gyro not -> derive gyro
    gyro_ft <- .deriveGyroMap(ft[fam_of(ft$from) == "a", , drop = FALSE])
    if (!is.null(gyro_ft)) ft <- rbind(ft, gyro_ft)
  }
  ft
}


#' Row-wise 3D cross product of two n x 3 matrices.
#' @keywords internal
#' @noRd
.rowCross <- function(a, b) {
  cbind(a[, 2] * b[, 3] - a[, 3] * b[, 2],
        a[, 3] * b[, 1] - a[, 1] * b[, 3],
        a[, 1] * b[, 2] - a[, 2] * b[, 1])
}

#' Accelerometer <-> gyroscope co-registration correlation (frame-level handedness check).
#'
#' Physics: when the accelerometer and gyroscope are expressed in ONE common orthonormal body frame, the
#' unit gravity direction ghat (from the low-passed accelerometer) obeys d(ghat)/dt = -omega x ghat,
#' where omega is the angular velocity (rad/s) from the gyroscope. The pooled Pearson correlation of the
#' measured d(ghat)/dt against the predicted -omega x ghat, over samples with enough rotation, is ~ +1
#' when the two sensors co-register, ~ -1 when the gyro is sign-reflected relative to the accel, and ~ 0
#' when the gyro is genuinely mis-registered or broken. Validated on 47 fleet deployments (correct maps
#' +0.43..+0.99; broken tags ~0). Both inputs must ALREADY be in the same frame (callers apply their axis
#' maps first); this scores the identity co-registration and does NOT search over rotations - that is the
#' job of \code{.resolveGyroFrame}, which shares the same identity but argmaxes over the 24 rotations.
#'
#' @param accel,gyro Numeric n x 3 matrices in a common body frame (accel in any consistent unit; gyro in
#'   rad/s). Rows must correspond in time.
#' @param fs Sampling frequency (Hz) of the supplied rows, used only for the ~1 s posture low-pass window.
#'   The correlation itself is scale-invariant, so an approximate fs is fine.
#' @param omega.thresh Minimum |omega| (rad/s) for a sample to count, gating out low-rotation
#'   posture-hold segments where the identity is uninformative. Default 0.15 (~8.6 deg/s).
#' @param min.samples Minimum usable (high-rotation, finite) samples; below this the correlation is NA
#'   (cannot judge). Default 200.
#' @param smooth.sec Posture low-pass window in seconds. Default 1.
#' @return A list \code{list(corr, frac, n)}: the co-registration correlation (NA when fewer than
#'   \code{min.samples} usable samples, i.e. too little rotation to judge), the fraction of rows that
#'   passed the rotation gate, and the usable-sample count.
#' @keywords internal
#' @noRd
.coregCorr <- function(accel, gyro, fs, omega.thresh = 0.15, min.samples = 200L, smooth.sec = 1) {
  na_out <- list(corr = NA_real_, frac = NA_real_, n = 0L)
  if (!is.finite(fs) || fs <= 0) return(na_out)
  accel <- as.matrix(accel); gyro <- as.matrix(gyro)
  n <- nrow(accel)
  if (n < 3L || nrow(gyro) != n || ncol(accel) != 3L || ncol(gyro) != 3L) return(na_out)
  sn <- max(2L, as.integer(round(smooth.sec * fs)))
  if (sn >= n) return(na_out)
  sm  <- function(v) data.table::frollmean(v, sn, fill = NA, align = "center")   # ~posture-band low-pass
  As  <- cbind(sm(accel[, 1]), sm(accel[, 2]), sm(accel[, 3]))
  Gs  <- cbind(sm(gyro[, 1]),  sm(gyro[, 2]),  sm(gyro[, 3]))
  gnorm <- sqrt(rowSums(As^2)); gnorm[!is.finite(gnorm) | gnorm < 1e-9] <- NA_real_
  ghat <- As / gnorm
  # central difference of ghat over rows 2:(n-1); the factor is irrelevant to a correlation, kept for clarity
  meas <- (ghat[3:n, , drop = FALSE] - ghat[1:(n - 2), , drop = FALSE]) * (fs / 2)
  gc   <- ghat[2:(n - 1), , drop = FALSE]
  wc   <- Gs[2:(n - 1), , drop = FALSE]
  pred <- -.rowCross(wc, gc)
  wmag <- sqrt(rowSums(wc^2))
  ok   <- is.finite(rowSums(meas)) & is.finite(rowSums(pred)) & is.finite(wmag) & wmag > omega.thresh
  nok  <- sum(ok)
  frac <- if (length(ok)) mean(ok) else NA_real_
  if (nok < min.samples) return(list(corr = NA_real_, frac = frac, n = nok))
  mv <- as.vector(meas[ok, ]); pv <- as.vector(pred[ok, ])
  corr <- if (stats::sd(mv) < 1e-12 || stats::sd(pv) < 1e-12) NA_real_ else stats::cor(mv, pv)
  list(corr = corr, frac = frac, n = nok)
}


#######################################################################################################
# Motion primitives ###################################################################################
#######################################################################################################

#' Centered rolling mean with edge padding (window forced odd; pads with the end values).
#' @keywords internal
#' @noRd
.rollmeanCentered <- function(x, window) {
  window <- as.integer(window)
  if (window < 1L) window <- 1L
  if (window %% 2L == 0L) window <- window + 1L
  if (length(x) < window) return(rep(NA_real_, length(x)))
  pad <- window %/% 2L
  padded <- c(rep(x[1], pad), x, rep(x[length(x)], pad))
  rolled <- data.table::frollmean(padded, n = window, align = "center", na.rm = TRUE)
  rolled[(pad + 1L):(pad + length(x))]
}

#' Split acceleration into static (gravity) and dynamic components via a centered rolling mean.
#'
#' @param ax,ay,az Numeric acceleration vectors (g or m/s^2).
#' @param window Integer window length in samples.
#' @return A list with `static` and `dynamic`, each a list of x/y/z vectors.
#' @keywords internal
#' @noRd
.staticDynamicAccel <- function(ax, ay, az, window) {
  sx <- .rollmeanCentered(ax, window); sy <- .rollmeanCentered(ay, window); sz <- .rollmeanCentered(az, window)
  list(static  = list(x = sx, y = sy, z = sz),
       dynamic = list(x = ax - sx, y = ay - sy, z = az - sz))
}

#' Vertical speed (m/s, positive downward following the depth convention) from a depth series.
#'
#' @param depth Numeric depth (m).
#' @param datetime POSIXct timestamps.
#' @return Numeric vector (same length; first element NA).
#' @keywords internal
#' @noRd
.verticalSpeed <- function(depth, datetime) {
  dt <- as.numeric(diff(datetime), units = "secs")
  c(NA_real_, diff(depth) / dt)
}

#' Vertical velocity (m/s) via a central difference on (optionally smoothed) depth.
#'
#' Differentiating a quantized depth series with a one-sample forward difference produces a coarse
#' staircase (steps of depth_resolution / dt) that swamps the true low-frequency vertical motion.
#' Low-passing depth before a centered difference (and optionally smoothing the velocity) removes
#' that artefact. Shared by processTagData() (orientation / VV) and checkTagMapping() (the surge
#' anchor) so both see the same vertical-velocity estimate.
#'
#' @param depth Numeric depth (m).
#' @param datetime POSIXct timestamps.
#' @param fs Sampling frequency (Hz), used to convert the smoothing windows (seconds) to samples.
#' @param depth.smoothing,speed.smoothing Smoothing windows in seconds, or `NULL` to disable.
#' @return A list with `depth` (the possibly-smoothed depth) and `velocity` (vertical velocity).
#' @keywords internal
#' @noRd
.verticalVelocity <- function(depth, datetime, fs, depth.smoothing = NULL, speed.smoothing = NULL) {
  if (!is.null(depth.smoothing)) {
    depth <- data.table::frollmean(depth, n = max(1L, round(fs * depth.smoothing)), fill = NA, align = "center")
  }
  n <- length(depth)
  if (n < 2L) return(list(depth = depth, velocity = rep(NA_real_, n)))
  dz <- data.table::shift(depth, type = "lead") - data.table::shift(depth, type = "lag")
  dt <- as.numeric(difftime(data.table::shift(datetime, type = "lead"),
                            data.table::shift(datetime, type = "lag"), units = "secs"))
  velocity <- dz / dt
  velocity[1] <- (depth[2] - depth[1]) / as.numeric(difftime(datetime[2], datetime[1], units = "secs"))
  velocity[n] <- (depth[n] - depth[n - 1]) / as.numeric(difftime(datetime[n], datetime[n - 1], units = "secs"))
  if (!is.null(speed.smoothing)) {
    velocity <- data.table::frollmean(velocity, n = max(1L, round(fs * speed.smoothing)), fill = NA, align = "center")
  }
  list(depth = depth, velocity = velocity)
}

#' Lightweight orientation pass from body-frame sensors (for axis-mapping validation).
#'
#' Derives just the orientation series a validation overlay needs - pitch, roll, heading and vertical
#' velocity - directly from already-body-framed accelerometer/magnetometer/depth data, WITHOUT the
#' calibration, mounting-offset correction, smoothing or downsampling that `processTagData()` applies.
#' This is deliberate: axis-mapping validation wants the RAW orientation a candidate mapping implies
#' (mounting-offset correction would shift the baseline off-physical and add a confound, with no benefit
#' for the handedness question). Mirrors `processTagData()`'s `tilt_compass` method but omits the
#' magnetic-declination correction - a constant offset that affects neither handedness nor the
#' heading-change used for turn detection.
#'
#' @param dt A data.table with body-frame `ax`/`ay`/`az` (and optionally `gx`/`gy`/`gz`, `mx`/`my`/`mz`,
#'   `depth`).
#' @param fs Sampling frequency (Hz).
#' @param posture.window Static-acceleration averaging window (seconds) used to extract gravity.
#' @param depth.smoothing Depth smoothing window (seconds) for the vertical-velocity estimate.
#' @param low.latency If `TRUE` (default) and a gyroscope is present, fuse it into the pitch/roll with a
#'   complementary filter so the displayed posture tracks quick banks tightly (see below).
#' @param fusion.tau Complementary-filter time constant (seconds): the gyro is followed below this
#'   timescale and the accel tilt re-anchors above it. Default 1.
#' @return A list with `pitch`, `roll`, `heading`, `vertical_velocity` (each length `nrow(dt)`).
#' @keywords internal
#' @noRd
.liteOrientation <- function(dt, fs, posture.window = 2, depth.smoothing = 3,
                             datetime.col = "datetime", low.latency = TRUE, fusion.tau = 1) {
  n <- nrow(dt)
  win <- max(1L, round(posture.window * fs))
  sd <- .staticDynamicAccel(dt[["ax"]], dt[["ay"]], dt[["az"]], win)
  tilt <- .tiltFromAccel(sd$static$x, sd$static$y, sd$static$z)
  pitch <- tilt$pitch; roll <- tilt$roll

  # Low-latency fusion for the overlay display. The accelerometer tilt is drift-free but SMEARS quick
  # rolls: it has to low-pass the dynamic acceleration out of gravity, which blurs the fast motion (the
  # centred window adds no net delay, but a fast bank still reaches full angle late). The gyroscope
  # measures body rotation directly, with no such blur. A complementary filter follows the gyro over
  # short timescales and re-anchors to the accel tilt over ~fusion.tau seconds, so the dial tracks the
  # animal's banks crisply without the gyro's slow drift. Body-frame rates: d(roll)/dt = gx,
  # d(pitch)/dt = gy (validated). An NA gyro sample contributes zero rate; across a gap in the accel
  # tilt the last finite tilt is carried forward as the anchor, so the estimate stays bounded near the
  # last known posture instead of integrating gyro drift without limit (a dead accel + live gyro run).
  if (isTRUE(low.latency) && all(c("gx", "gy", "gz") %in% names(dt)) && any(is.finite(dt[["gx"]])) &&
      is.finite(fs) && fs > 0 && n >= 2L) {
    dts <- 1 / fs
    a   <- fusion.tau / (fusion.tau + dts)                # gyro-trust weight in [0, 1)
    gxd <- dt[["gx"]] * (180 / pi); gyd <- dt[["gy"]] * (180 / pi)   # body roll/pitch rate, deg/s
    last_ar <- if (is.finite(tilt$roll[1]))  tilt$roll[1]  else NA_real_   # last finite accel anchor
    last_ap <- if (is.finite(tilt$pitch[1])) tilt$pitch[1] else NA_real_
    for (i in 2:n) {
      pr <- if (is.finite(roll[i - 1]))  roll[i - 1]  else if (is.finite(roll[i]))  roll[i]  else 0
      pp <- if (is.finite(pitch[i - 1])) pitch[i - 1] else if (is.finite(pitch[i])) pitch[i] else 0
      gr <- if (is.finite(gxd[i])) gxd[i] * dts else 0
      gp <- if (is.finite(gyd[i])) gyd[i] * dts else 0
      ar <- if (is.finite(tilt$roll[i]))  tilt$roll[i]  else last_ar        # carry forward across gaps
      ap <- if (is.finite(tilt$pitch[i])) tilt$pitch[i] else last_ap
      roll[i]  <- if (is.finite(ar)) a * (pr + gr) + (1 - a) * ar else pr + gr
      pitch[i] <- if (is.finite(ap)) a * (pp + gp) + (1 - a) * ap else pp + gp
      if (is.finite(tilt$roll[i]))  last_ar <- tilt$roll[i]
      if (is.finite(tilt$pitch[i])) last_ap <- tilt$pitch[i]
    }
  }

  # tilt-compensated magnetic heading (degrees, 0-360); NA near the gimbal-lock pole (|pitch| > 89.5)
  heading <- rep(NA_real_, n)
  if (all(c("mx", "my", "mz") %in% names(dt)) && any(is.finite(dt[["mx"]]))) {
    pr <- pitch * (pi / 180); rr <- roll * (pi / 180)
    mxc <- dt[["mx"]] * cos(pr) + dt[["my"]] * sin(pr) * sin(rr) + dt[["mz"]] * sin(pr) * cos(rr)
    myc <- dt[["my"]] * cos(rr) - dt[["mz"]] * sin(rr)
    heading <- ifelse(abs(pitch) > 89.5, NA_real_, (atan2(-myc, mxc) * (180 / pi)) %% 360)
  }

  vv <- if ("depth" %in% names(dt) && n >= 2L)
    .verticalVelocity(dt[["depth"]], dt[[datetime.col]], fs, depth.smoothing = depth.smoothing)$velocity
  else rep(NA_real_, n)

  list(pitch = pitch, roll = roll, heading = heading, vertical_velocity = vv)
}

#' Provisional hard-iron (spherical) magnetometer offset, with an orientation-coverage gate.
#'
#' Estimates the per-axis hard-iron bias as the midpoint of robust (2nd/98th percentile) extremes -
#' resistant to the spikes that a raw min/max midpoint is sensitive to - and the per-axis half-range
#' (also from the robust extremes) used for axis-aligned soft-iron scaling. Only reports the estimate
#' as usable (`coverage_ok = TRUE`) when the animal rotated enough that each axis spans a reasonable
#' fraction of the field magnitude; on poorly-sampled deployments the midpoint is biased (a partial
#' arc's centre is not the sphere centre) and the per-axis scales blow up, so it is flagged for the
#' caller to skip. Used both diagnostically (checkTagMapping's dip check) and for persisted
#' calibration (processTagData).
#' @param mx,my,mz Raw magnetometer channels.
#' @param min.coverage Minimum per-axis half-range as a fraction of the sphere radius (default 0.5).
#' @return A list with `offset` (length-3), `half_range` (length-3), `radius`, and `coverage_ok`, or
#'   NULL if too few samples.
#' @keywords internal
#' @noRd
.hardIronOffset <- function(mx, my, mz, min.coverage = 0.5) {
  M <- cbind(mx, my, mz)
  M <- M[stats::complete.cases(M), , drop = FALSE]
  if (nrow(M) < 100L) return(NULL)
  q <- apply(M, 2, stats::quantile, probs = c(0.02, 0.98), na.rm = TRUE)   # robust extremes
  off <- colMeans(q)
  half_range <- (q[2, ] - q[1, ]) / 2
  rad <- stats::median(sqrt(rowSums(sweep(M, 2, off)^2)))
  coverage_ok <- is.finite(rad) && rad > 0 && all(half_range >= min.coverage * rad)
  list(offset = unname(off), half_range = unname(half_range), radius = rad, coverage_ok = coverage_ok)
}

#' Pitch and roll (degrees) from a static-acceleration triplet.
#'
#' Uses the standard aerospace convention: pitch = atan2(-x, sqrt(y^2 + z^2)), roll = atan2(y, z).
#' @param sx,sy,sz Static (gravity) acceleration components.
#' @return A list with `pitch` and `roll` in degrees.
#' @keywords internal
#' @noRd
.tiltFromAccel <- function(sx, sy, sz) {
  list(pitch = atan2(-sx, sqrt(sy^2 + sz^2)) * 180 / pi,
       roll  = atan2(sy, sz) * 180 / pi)
}


#' One-sided periodogram of a (de-meaned) signal.
#'
#' @param x Numeric signal.
#' @param fs Sampling frequency (Hz).
#' @return A list with `freq` (Hz, > 0) and `power`, or NULL if too short / non-finite.
#' @keywords internal
#' @noRd
.periodogram <- function(x, fs) {
  x <- x[is.finite(x)]
  n <- length(x)
  if (n < 8L) return(NULL)
  x <- x - mean(x)
  P <- (Mod(stats::fft(x))^2) / n
  half <- 2:(n %/% 2L + 1L)                         # drop the DC term; positive frequencies
  list(freq = (half - 1L) * fs / n, power = P[half])
}

#' Welch power spectral density (averaged, windowed periodogram)
#'
#' A smooth, low-variance one-sided PSD estimate suited to long high-resolution records: the signal is
#' split into overlapping Hann-windowed segments, each segment's periodogram is computed, and the results
#' are averaged. Unlike a full-series \code{\link{.periodogram}} this is cheap even on multi-million-sample
#' series (many small FFTs of a fixed power-of-two length rather than one giant FFT), returns a compact
#' spectrum (a few hundred bins), and is far more legible when plotted. NA runs are tolerated: segments
#' more than half-missing are skipped and any remaining gaps within a used segment are mean-filled.
#'
#' @param x Numeric signal.
#' @param fs Sampling frequency (Hz).
#' @param seg.length Segment length (samples). `NULL` (default) picks a power of two giving a smooth
#'   estimate (clamped to `[256, 16384]`).
#' @param overlap Fractional overlap between segments (default 0.5).
#' @return A list with `freq` (Hz, > 0), `power` (PSD), `nseg` (segments averaged) and `seg` (segment
#'   length), or `NULL` if the series is too short / non-finite.
#' @keywords internal
#' @noRd
.welchPSD <- function(x, fs, seg.length = NULL, overlap = 0.5) {
  x <- as.numeric(x)
  n <- length(x)
  if (!is.finite(fs) || fs <= 0 || n < 64L) return(NULL)

  if (is.null(seg.length))
    seg.length <- 2^as.integer(floor(log2(min(n, max(1024, n / 8)))))
  seg <- as.integer(max(256L, min(seg.length, 16384L)))
  if (n < seg) seg <- 2L^as.integer(floor(log2(n)))
  if (seg < 16L) return(NULL)

  step   <- max(1L, as.integer(round(seg * (1 - overlap))))
  starts <- seq(1L, n - seg + 1L, by = step)
  if (!length(starts)) return(NULL)

  w    <- 0.5 - 0.5 * cos(2 * pi * (0:(seg - 1L)) / (seg - 1L))    # Hann window
  U    <- mean(w^2)                                                # window power (PSD normalisation)
  half <- 2:(seg %/% 2L + 1L)                                      # positive frequencies (drop DC)
  acc  <- numeric(length(half)); used <- 0L

  for (s in starts) {
    sv  <- x[s:(s + seg - 1L)]
    fin <- is.finite(sv)
    if (mean(fin) < 0.5) next                                      # skip a mostly-missing segment
    if (!all(fin)) sv[!fin] <- mean(sv[fin])                       # mean-fill remaining gaps
    sv  <- (sv - mean(sv)) * w
    acc <- acc + (Mod(stats::fft(sv))^2)[half] / (seg * fs * U)
    used <- used + 1L
  }
  if (used == 0L) return(NULL)
  list(freq = (half - 1L) * fs / seg, power = (acc / used) * 2, nseg = used, seg = seg)
}

#' Total spectral power of a signal within a frequency band.
#'
#' Used to find which axis carries the locomotor (tail-beat) oscillation; setting the band's lower
#' edge above the tag's tether/pendulum frequency excludes that contamination.
#' @param x Numeric signal. @param fs Sampling frequency (Hz). @param band Numeric c(low, high) in Hz.
#' @return The summed power in-band (numeric), or NA if the signal is too short.
#' @keywords internal
#' @noRd
.bandPower <- function(x, fs, band) {
  pg <- .periodogram(x, fs)
  if (is.null(pg)) return(NA_real_)
  inb <- pg$freq >= band[1] & pg$freq <= band[2]
  if (!any(inb)) return(0)
  sum(pg$power[inb])
}

#' Dominant frequency (Hz) of a signal within a band.
#' @param x Numeric signal. @param fs Sampling frequency (Hz). @param band Numeric c(low, high) in Hz.
#' @return The in-band frequency of peak power, or NA.
#' @keywords internal
#' @noRd
.dominantFreq <- function(x, fs, band) {
  pg <- .periodogram(x, fs)
  if (is.null(pg)) return(NA_real_)
  inb <- which(pg$freq >= band[1] & pg$freq <= band[2])
  if (!length(inb)) return(NA_real_)
  pg$freq[inb][which.max(pg$power[inb])]
}


#######################################################################################################
# Paddle-wheel magnetometer contamination (shared primitives) #########################################
#######################################################################################################

#' Zero-phase magnetometer de-noise (vector-domain running mean).
#'
#' A paddle-wheel speed sensor spins a magnet past the tag; it adds a LARGE, additive, oscillating field
#' (rotation frequency + harmonics) to the magnetometer. Because the interference is additive in the
#' FIELD-VECTOR domain and averages to ~0 over a rotation, a centred (zero-phase) running mean of the
#' mx/my/mz VECTOR suppresses it while preserving the slow, orientation-driven variation. This works far
#' better than smoothing the derived heading angle: heading is `atan2(field)`, a non-linear
#' normalisation, and when the paddle field is large each sample's heading points mostly along the magnet,
#' so circular-averaging the angles no longer lets the additive interference cancel (validated: vector
#' smoothing recovers a paddle-on track ~10x better than circular-mean heading smoothing). Filter the
#' VECTOR, then compute heading.
#' @param M n x 3 magnetometer matrix (columns mx, my, mz).
#' @param fs Sampling frequency (Hz).
#' @param window.sec Smoothing window (seconds). A non-positive or sub-2-sample window returns M unchanged.
#' @return The de-noised n x 3 matrix (NA at the edges from the centred window).
#' @keywords internal
#' @noRd
.magDenoise <- function(M, fs, window.sec) {
  M <- as.matrix(M)
  if (!is.finite(fs) || fs <= 0 || !is.finite(window.sec) || window.sec <= 0) return(M)
  w <- max(1L, as.integer(round(window.sec * fs)))
  if (w < 2L) return(M)
  out <- data.table::frollmean(list(M[, 1], M[, 2], M[, 3]), w, align = "center", fill = NA, na.rm = TRUE)
  matrix(c(out[[1]], out[[2]], out[[3]]), ncol = 3L, dimnames = dimnames(M))
}

#' Detect paddle-wheel magnetometer contamination and recommend a de-noise window.
#'
#' Reuses the Welch PSD to find the strongest narrow-band magnetometer peak above the tail-beat band and
#' below Nyquist (the paddle rotation line), quantifies contamination two ways (the peak prominence, and
#' the field-vector inflation `mean|m| / |mean m|` - ~1 when clean, large when a big oscillation dominates
#' the magnitude), and derives ONE reproducible de-noise window sitting between the paddle line and the
#' animal's turning band. `separation.ok = FALSE` flags a slow paddle whose frequency approaches the
#' turning rate, where NO fixed low-pass can separate them and a gyro/AHRS heading is needed instead.
#' Consolidates the paddle logic previously duplicated in checkSensorIntegrity and calibrateMagnetometer.
#' @param M n x 3 magnetometer matrix. @param fs Sampling frequency (Hz).
#' @param turn.band.hz Upper edge of the animal's heading-change bandwidth (Hz). Default 0.3.
#' @param min.freq Lower bound of the paddle search (Hz), above the tail-beat harmonics. Default 3.
#' @param inflation.min Field-magnitude inflation above which a contaminating oscillation is declared
#'   present (`mean|m|/|mean m|`; ~1 clean). Default 1.5. This - not the spectral prominence alone - is
#'   the presence criterion, because a small peak (e.g. a walking/tail-beat cadence) can be prominent yet
#'   NOT dominate the field or corrupt heading; only an oscillation that inflates the magnitude does.
#' @param min.freq Lower bound of the paddle-frequency search (Hz), above the tail-beat harmonics. Default 3.
#' @param cycles Rotations to average when recommending the window. @param window.bounds [min, max] window (s).
#' @return list(present, freq, prominence, inflation, recommend.window, separation.ok).
#' @keywords internal
#' @noRd
.paddleState <- function(M, fs, turn.band.hz = 0.3, min.freq = 3, inflation.min = 1.5,
                         cycles = 8, window.bounds = c(0.5, 3)) {
  na <- list(present = FALSE, freq = NA_real_, prominence = NA_real_, inflation = NA_real_,
             recommend.window = 0, separation.ok = NA)
  M <- as.matrix(M)
  if (!is.finite(fs) || fs <= 0 || nrow(M) < 64L) return(na)
  # contamination strength: does a fast oscillation inflate the field magnitude? (mean|m| vs |1s-mean m|)
  mm <- .magDenoise(M, fs, 1)
  inflation <- mean(sqrt(rowSums(M^2)), na.rm = TRUE) / mean(sqrt(rowSums(mm^2)), na.rm = TRUE)
  present <- is.finite(inflation) && inflation > inflation.min
  # locate the offending line (strongest narrow-band mag peak above the tail-beat band, below Nyquist)
  hi <- 0.98 * (fs / 2)
  best <- list(freq = NA_real_, prom = 0)
  for (j in 1:3) {
    pg <- tryCatch(.welchPSD(M[, j], fs), error = function(e) NULL); if (is.null(pg)) next
    band <- pg$freq >= min.freq & pg$freq <= hi
    if (sum(band) < 5L) next
    p <- pg$power[band]; med <- stats::median(p, na.rm = TRUE)
    if (!is.finite(med) || med <= 0) next
    prom <- max(p) / med
    if (prom > best$prom) best <- list(freq = pg$freq[band][which.max(p)], prom = prom)
  }
  rw <- if (present && is.finite(best$freq) && best$freq > 0)
          min(max(cycles / best$freq, window.bounds[1]), window.bounds[2]) else 0   # 0 = no paddle de-noise
  sep <- if (present && is.finite(best$freq)) isTRUE(best$freq > cycles * turn.band.hz) else NA
  list(present = present, freq = best$freq, prominence = best$prom, inflation = unname(inflation),
       recommend.window = rw, separation.ok = sep)
}


#######################################################################################################
# Oscillation primitives (tail-beat detection) ########################################################
#######################################################################################################

#' Abort when a Butterworth design has gone numerically unstable.
#'
#' A band that is narrow relative to Nyquist leaves the transfer-function coefficients ill-conditioned,
#' and `signal::filtfilt` then returns finite garbage: no error, no warning, no NA (measured from a
#' unit-amplitude input: ~6e15 at fs = 250 Hz, ~2e249 at 500 Hz, NaN at 1000 Hz). Neither `tryCatch` nor
#' a finiteness test can see it, and the normalised corners are individually valid so validating those
#' does not help either. A Butterworth has |H| <= 1 everywhere, so any gain at all proves the design
#' failed. Callers that legitimately skip filtering pass the input through unchanged and cannot trip this.
#' @param input,output Filter input and output. @param fs Sampling frequency (Hz).
#' @param low,high Band edges (Hz). @param order Butterworth order. @param id Optional ID for the message.
#' @keywords internal
#' @noRd
.assertFilterStable <- function(input, output, fs, low, high, order, id = NULL) {
  max_in <- suppressWarnings(max(abs(input), na.rm = TRUE))
  max_out <- suppressWarnings(max(abs(output), na.rm = TRUE))
  # -Inf means nothing finite was filtered, so there is no output to judge and nothing to assert. +Inf
  # is the opposite: the filter diverged outright, which is the loudest failure there is. Treating both
  # as "not finite, therefore fine" let the worst case through the check built to catch it.
  if (!is.finite(max_in) || identical(max_out, -Inf)) return(invisible(TRUE))
  if (is.finite(max_out) && max_out <= 10 * max_in) return(invisible(TRUE))
  .abort(c(
    if (is.null(id)) "The band-pass filter is numerically unstable at {.val {fs}} Hz."
    else "The band-pass filter is numerically unstable at {.val {fs}} Hz for ID {.val {id}}.",
    "i" = "A {low}-{high} Hz band is too narrow against Nyquist ({fs / 2} Hz) for an order-{order} design.",
    "i" = "Downsample the data, widen the band, or lower the filter order."
  ))
}

#' Amplitude envelope of a (band-passed) signal, via the analytic signal.
#'
#' The analytic signal z = x + i*H(x) has |z| equal to the instantaneous amplitude of x, so for a
#' band-passed record this tracks in-band effort directly, in the units of x. Computed by zeroing the
#' negative-frequency half of the spectrum (`signal` exports no `hilbert()`; this is the same thing in a
#' few lines of `stats::fft`). NAs are treated as zeros for the transform and restored afterwards, so a
#' gap neither leaks a step into the spectrum nor is silently reported as low effort.
#' @param x Numeric signal. @param fs Sampling frequency (Hz).
#' @param win.s Smoothing window (s) applied to the envelope; 0 to disable.
#' @return Numeric vector the same length as `x`.
#' @keywords internal
#' @noRd
.envelope <- function(x, fs, win.s = 1) {
  n <- length(x)
  isna <- is.na(x)
  X <- stats::fft(ifelse(isna, 0, x))
  h <- rep(0, n)                                   # one-sided spectrum: h doubles the positive half
  h[1] <- 1
  if (n %% 2 == 0) { h[n / 2 + 1] <- 1; if (n > 2) h[2:(n / 2)] <- 2 }
  else if (n > 1) h[2:((n + 1) / 2)] <- 2
  e <- Mod(stats::fft(X * h, inverse = TRUE) / n)
  e[isna] <- NA_real_
  if (isTRUE(win.s > 0)) e <- .smoothSeries(e, win.s, fs)
  e
}


#' Magnitude gain a zero-phase Butterworth band-pass applies at given frequencies.
#'
#' `filtfilt` runs the filter forwards and backwards, so the gain it applies is |H(f)|^2, not |H(f)|.
#' Any amplitude measured from the filtered signal is therefore attenuated by this factor -- ~29% at
#' each nominal band edge for the default 0.9*min / 1.1*max corners -- which would otherwise be read as
#' a real drop in effort rather than an artefact of the filter. Dividing by this restores the amplitude.
#' The response is evaluated exactly from the designed coefficients (H = B(z)/A(z) at z = exp(-i*w)),
#' so there is no empirical calibration constant.
#' @param at Frequencies (Hz) at which to evaluate. @param fs Sampling frequency (Hz).
#' @param low,high Band edges (Hz). @param order Butterworth order.
#' @return |H(at)|^2, the gain `filtfilt` applies.
#' @keywords internal
#' @noRd
.bandpassPowerGain <- function(at, fs, low, high, order) {
  # clamp exactly as .bandpassSegments does: this must describe the filter that was actually applied,
  # and signal::butter errors outright on a normalised corner at or above 1
  nyq <- fs / 2
  if (high >= nyq) high <- nyq * 0.99
  if (low <= 0) low <- 1e-6
  if (low >= high) return(rep(1, length(at)))          # no usable band: report no attenuation
  bf <- signal::butter(order, c(low, high) / nyq, type = "pass")
  z <- exp(-1i * 2 * pi * at / fs)
  num <- outer(z, seq_along(bf$b) - 1L, "^") %*% bf$b
  den <- outer(z, seq_along(bf$a) - 1L, "^") %*% bf$a
  as.vector(Mod(num / den)^2)
}


#' Segment-aware Butterworth band-pass filter.
#'
#' Applies a zero-phase Butterworth band-pass independently within each maximal run of finite values,
#' so the filter never rings across gaps. Runs shorter than `min.seg` (and NA values) are returned as
#' NA. Used to isolate the tail-beat band before peak / instantaneous-frequency analysis.
#' @param x Numeric signal. @param fs Sampling frequency (Hz). @param low,high Band edges (Hz).
#' @param order Butterworth order (default 4). @param min.seg Minimum segment length (samples); default
#'   `max(3*order+1, fs)`.
#' @return A numeric vector the same length as `x` (NA in gaps / too-short segments).
#' @keywords internal
#' @noRd
#' Design corner for a zero-phase (filtfilt) Butterworth so its -3 dB lands at `target` Hz.
#'
#' filtfilt runs the filter forwards and backwards, squaring the magnitude response, so the two-pass
#' -3 dB point is displaced from the single-pass design corner by k = (sqrt(2)-1)^(1/(2n)) (~0.803 for
#' order 2). For a LOW-pass the two-pass -3 dB sits at fc*k (below the corner), so the corner that lands
#' it at `target` is target/k. For a HIGH-pass the response is mirrored -- its two-pass -3 dB is at fc/k
#' (above the corner) -- so the corner is target*k. Getting this reciprocal wrong puts the cutoff a
#' half-octave off. Needed to match a replaced moving-average filter's measured cutoff.
#' @param target Desired zero-phase -3 dB frequency (Hz). @param order Single-pass Butterworth order.
#' @param type `"high"` or `"low"`.
#' @keywords internal
#' @noRd
.filtfiltCorner <- function(target, order, type = c("low", "high")) {
  type <- match.arg(type)
  k <- (sqrt(2) - 1)^(1 / (2 * order))
  if (type == "high") target * k else target / k
}

#' Segment-aware zero-phase Butterworth filter (high-, low- or band-pass).
#'
#' Applies `signal::filtfilt` independently within each maximal run of finite values, so the filter
#' never rings across gaps; runs shorter than `min.seg` (and NA values) are returned as NA. Each segment
#' is demeaned before filtering to kill the DC-driven edge transient (filtfilt seeds from the first
#' sample); for a low-pass, whose job is to preserve the DC level, the mean is added back afterwards
#' (exact, since a unity-DC-gain filtfilt returns the mean unchanged). The numerical-stability guard
#' catches an ill-conditioned narrow design that returns finite garbage.
#' @param x Numeric signal. @param fs Sampling frequency (Hz).
#' @param W Cutoff (Hz): a single value for `"high"`/`"low"`, or `c(low, high)` for `"pass"`. These are
#'   the DESIGN corners (single-pass); use `.filtfiltCorner()` first to match a target zero-phase -3 dB.
#' @param type `"high"`, `"low"` or `"pass"`. @param order Single-pass Butterworth order (filtfilt
#'   doubles the effective order). @param min.seg Minimum segment length (samples).
#' @return A numeric vector the same length as `x` (NA in gaps / too-short segments).
#' @keywords internal
#' @noRd
.filterSegments <- function(x, fs, W, type = c("pass", "high", "low"), order = 4, min.seg = NULL) {
  type <- match.arg(type)
  if (!requireNamespace("signal", quietly = TRUE)) {
    .abort("The {.pkg signal} package is required for filtering.")
  }
  nyq <- fs / 2
  Wn <- pmin(pmax(W / nyq, 1e-6), 0.999)
  bf <- signal::butter(order, Wn, type = type)
  out <- rep(NA_real_, length(x))
  finite <- is.finite(x)
  if (!any(finite)) return(out)
  r <- rle(finite); ends <- cumsum(r$lengths); starts <- ends - r$lengths + 1L
  min_len <- if (is.null(min.seg)) max(3L * order + 1L, round(fs)) else min.seg
  readd <- type == "low"                              # a low-pass must preserve the DC it passes
  for (k in which(r$values)) {
    if (r$lengths[k] >= min_len) {
      idx <- starts[k]:ends[k]
      seg <- x[idx]; m <- mean(seg)
      y <- as.numeric(signal::filtfilt(bf, seg - m))
      out[idx] <- if (readd) y + m else y
    }
  }
  .assertFilterStable(x, out, fs, min(W), max(W), order)
  out
}

#' Segment-aware Butterworth band-pass (thin wrapper over `.filterSegments`).
#' @keywords internal
#' @noRd
.bandpassSegments <- function(x, fs, low, high, order = 4, min.seg = NULL) {
  nyq <- fs / 2
  if (high >= nyq) high <- nyq * 0.99
  if (low <= 0)    low <- 1e-6
  .filterSegments(x, fs, c(low, high), type = "pass", order = order, min.seg = min.seg)
}

#' Peak / trough detection with an amplitude threshold (Billauer's algorithm).
#'
#' Walks the series once, registering a peak when the signal drops by at least `delta` from a running
#' maximum and a trough when it rises by `delta` from a running minimum. Peaks and troughs strictly
#' alternate, and `delta` gates out sub-threshold wiggles (noise / non-beating periods). O(n), base R,
#' NA-tolerant (NAs are skipped, so callers should detect within continuous segments).
#' @param v Numeric signal (typically band-passed). @param delta Minimum peak-to-trough amplitude.
#' @return A list with integer vectors `peaks` and `troughs` (indices into `v`).
#' @keywords internal
#' @noRd
.peakdet <- function(v, delta) {
  n <- length(v)
  peaks <- integer(0); troughs <- integer(0)
  mn <- Inf; mx <- -Inf; mnpos <- NA_integer_; mxpos <- NA_integer_
  lookformax <- TRUE
  for (i in seq_len(n)) {
    val <- v[i]
    if (is.na(val)) next
    if (val > mx) { mx <- val; mxpos <- i }
    if (val < mn) { mn <- val; mnpos <- i }
    if (lookformax) {
      if (val < mx - delta) { peaks <- c(peaks, mxpos); mn <- val; mnpos <- i; lookformax <- FALSE }
    } else {
      if (val > mn + delta) { troughs <- c(troughs, mnpos); mx <- val; mxpos <- i; lookformax <- TRUE }
    }
  }
  list(peaks = peaks, troughs = troughs)
}


# ------------------
# Depth zero-offset drift correction (depthDriftControl "surface" method)
# ------------------

#' An empty POSIXct span table (start/end), for the no-low-confidence case.
#' @keywords internal
#' @noRd
.emptySpans <- function() data.frame(start = .POSIXct(numeric(0), "UTC"), end = .POSIXct(numeric(0), "UTC"))

#' Sustained dry intervals from a transition-encoded wet/dry signal.
#'
#' @param dry data.frame with `datetime` (POSIXct transition times) and `dry` (logical state holding
#'   from that time until the next row). Run-length / transition encoded.
#' @param end_time POSIXct: the final row's state holds until here (the end of the deployment).
#' @param min.dry.duration Minimum interval duration (seconds) to keep.
#' @return data.frame with `start`, `end` (POSIXct) of the kept sustained-dry intervals (0+ rows).
#' @keywords internal
#' @noRd
.dryIntervals <- function(dry, end_time, min.dry.duration) {
  if (is.null(dry) || nrow(dry) == 0) return(.emptySpans())
  o <- order(dry$datetime); t <- dry$datetime[o]; s <- as.logical(dry$dry)[o]
  starts <- t
  ends   <- c(t[-1], end_time)
  keep   <- which(s & (as.numeric(ends) - as.numeric(starts) >= min.dry.duration))
  data.frame(start = starts[keep], end = ends[keep])
}

#' Gather surface anchors (time, offset) from evidence independent of the depth trace.
#'
#' Each anchor is a time at which the tag is known to be at the surface, paired with the depth the
#' sensor reads there - which, since the true depth is ~0, IS the zero offset to remove. Evidence:
#' sustained dry intervals, surface-implying position fixes (whose antenna must break the surface), and
#' an opt-in shallow mode that infers surface intervals from the depth trace (gap-filler only).
#'
#' @param depth Numeric depth (m).
#' @param datetime POSIXct timestamps (same length as `depth`).
#' @param dry Transition-encoded wet/dry table (see \code{.dryIntervals}), or NULL.
#' @param positions data.frame of position fixes with `datetime` and `type`, or NULL.
#' @param control A `nautilus_depth_drift` control object.
#' @return list(anchors = data.frame(time, offset, source), at_surface = logical(length(depth))).
#' @keywords internal
#' @noRd
.gatherSurfaceEvidence <- function(depth, datetime, dry, positions, control) {
  n <- length(depth); tn <- as.numeric(datetime)
  at_surface <- rep(FALSE, n)
  ax_t <- numeric(0); ax_o <- numeric(0); ax_s <- character(0)

  # A valid zero-offset anchor must read near the animal's SHALLOWEST sustained depth (the true surface
  # level). Independent "surface" evidence (a GPS fix, a dry interval) is only trustworthy for the offset
  # when the depth it reads there is actually near-surface: a fix that lands on a dive (its timestamp is
  # mis-aligned, or the wet/dry sensor is coarse) reads tens of metres, which is NOT the sensor zero drift
  # (that is cm to ~1 m). Using such a reading would over-correct and push the corrected depth impossibly
  # above the surface, so those anchors are rejected here. `surf_ref` + `surface.band` adapts per deployment:
  # a genuine large drift raises the shallowest level too, so real surfacings still pass.
  fin      <- depth[is.finite(depth)]
  surf_ref <- if (length(fin)) unname(stats::quantile(fin, control$surface.quantile)) else 0
  offset_ok <- function(o) is.finite(o) && o <= surf_ref + control$surface.band

  # dry sensor: sustained dry intervals -> one anchor each (midpoint time, median depth over interval)
  if ("dry" %in% control$surface.evidence && !is.null(dry) && nrow(dry) > 0 && n > 0) {
    iv <- .dryIntervals(dry, datetime[n], control$min.dry.duration)
    for (k in seq_len(nrow(iv))) {
      inw <- tn >= as.numeric(iv$start[k]) & tn < as.numeric(iv$end[k])
      if (!any(inw)) next
      o <- stats::median(depth[inw], na.rm = TRUE)
      if (!offset_ok(o)) next                          # not actually at the surface here -> not a zero offset
      at_surface[inw] <- TRUE
      ax_t <- c(ax_t, as.numeric(iv$start[k]) + (as.numeric(iv$end[k]) - as.numeric(iv$start[k])) / 2)
      ax_o <- c(ax_o, o); ax_s <- c(ax_s, "dry")
    }
  }

  # position fixes: surface-implying types (Fastloc-GPS / Argos / pop-up all require a dry antenna)
  if ("gps" %in% control$surface.evidence && !is.null(positions) && nrow(positions) > 0 &&
      all(c("datetime", "type") %in% names(positions)) && n > 0) {
    surf <- positions[positions$type %in% c("FastGPS", "Argos"), , drop = FALSE]
    half <- control$min.dry.duration / 2
    for (k in seq_len(nrow(surf))) {
      ft  <- as.numeric(surf$datetime[k])
      inw <- tn >= ft - half & tn <= ft + half
      o   <- if (any(inw)) stats::median(depth[inw], na.rm = TRUE) else depth[which.min(abs(tn - ft))]
      if (!offset_ok(o)) next                          # fix landed on a dive (mis-timed) -> reject
      if (any(inw)) at_surface[inw] <- TRUE
      ax_t <- c(ax_t, ft); ax_o <- c(ax_o, o); ax_s <- c(ax_s, "gps")
    }
  }

  # shallow mode (opt-in): infer surface intervals from the depth trace, as a GAP-FILLER only - kept
  # only where the independent (dry/gps) evidence is absent, since it assumes the shallowest sustained
  # depth is the surface (unsuitable for animals that rarely surface).
  if ("depth" %in% control$surface.evidence && n > 0) {
    indep_t <- ax_t                                    # dry/gps anchor times gathered above
    at_d <- .depthEnvelopeSurface(depth, control)
    rr <- rle(at_d); ends <- cumsum(rr$lengths); starts <- ends - rr$lengths + 1L
    for (k in which(rr$values)) {
      s <- starts[k]; e <- ends[k]
      ts <- as.numeric(datetime[s]); te <- as.numeric(datetime[e])
      if (te - ts < control$min.dry.duration) next
      if (length(indep_t) && any(indep_t >= ts & indep_t <= te)) next   # independent evidence already anchors here
      o <- stats::median(depth[s:e], na.rm = TRUE)
      if (!offset_ok(o)) next                          # (envelope samples are near-surface by construction)
      at_surface[s:e] <- TRUE
      ax_t <- c(ax_t, ts + (te - ts) / 2); ax_o <- c(ax_o, o); ax_s <- c(ax_s, "depth")
    }
  }

  ord <- order(ax_t)
  list(anchors = data.frame(time = ax_t[ord], offset = ax_o[ord], source = ax_s[ord],
                            stringsAsFactors = FALSE),
       at_surface = at_surface)
}

#' Per-sample "at surface" flag inferred from the depth trace (shallow mode, opt-in).
#'
#' Estimates the surface level as the `surface.quantile` quantile of depth over the deployment, and
#' flags samples whose depth is within `surface.band` metres of it. Assumes the shallowest sustained
#' depth is the surface, so it is unsuitable for animals that rarely surface (hence opt-in, and used
#' only as a gap-filler in \code{.gatherSurfaceEvidence}).
#'
#' @param depth Numeric depth (m).
#' @param control A `nautilus_depth_drift` control object.
#' @return Logical vector (length `depth`): TRUE where the sample is inferred to be at the surface.
#' @keywords internal
#' @noRd
.depthEnvelopeSurface <- function(depth, control) {
  n <- length(depth)
  if (n == 0L) return(logical(0))
  d <- depth[!is.na(depth)]
  if (!length(d)) return(rep(FALSE, n))
  surf <- unname(stats::quantile(d, control$surface.quantile))     # global shallowest-sustained level
  at <- !is.na(depth) & depth <= surf + control$surface.band
  at[is.na(at)] <- FALSE
  at
}

#' Correct depth zero-offset drift from surface evidence (the depthDriftControl "surface" method).
#'
#' Estimates the slowly-varying pressure-sensor zero offset from surface anchors (times the tag is
#' known to be at 0 m) and subtracts it. Independent of the depth trace itself; abstains when there is
#' too little surface evidence, rather than inventing a zero line. Offset-only (span/gain drift is not
#' corrected). See \code{\link{depthDriftControl}}.
#'
#' @param depth Numeric depth (m).
#' @param datetime POSIXct timestamps (same length as `depth`).
#' @param dry Transition-encoded wet/dry table, or NULL.
#' @param positions Position-fix data.frame (`datetime`, `type`), or NULL.
#' @param control A `nautilus_depth_drift` control object.
#' @return A list: `depth` (corrected), `offset` (applied, m), `status`
#'   (`disabled`/`abstained`/`constant_offset`/`applied`/`applied_with_gaps`), `n_anchors`,
#'   `anchors` (per-source counts), `max_gap_h`, `low_confidence` (spans), and `outcome`
#'   (list of `offset_range_m`, `residual_m`).
#' Format the one-line depth-drift diagnostic for the processTagData() detailed verbose block.
#'
#' Scannable across dozens of deployments: the correction MAGNITUDE leads (a range, or a single value
#' when constant), with coverage (anchors, gaps) and quality (residual) as parenthesised qualifiers, e.g.
#' `depth drift: 1 - 48 m (7 anchors; residual 61.5 m)`. Returns `NULL` when the correction is disabled
#' (no line), and `"depth drift: skipped (no surface evidence)"` when it abstained.
#' @param drift_res The list returned by \code{.correctDepthDrift}.
#' @keywords internal
#' @noRd
.depthDriftDiag <- function(drift_res) {
  status <- drift_res$status
  if (identical(status, "disabled"))  return(NULL)
  if (identical(status, "abstained")) return("depth drift: skipped (no surface evidence)")
  fmtm <- function(x) sub("\\.0$", "", formatC(x, format = "f", digits = 1))   # 1 dp, drop trailing .0
  off  <- drift_res$outcome$offset_range_m
  off_txt <- if (isTRUE(all.equal(off[1], off[2]))) paste0(fmtm(off[1]), " m")
             else paste0(fmtm(off[1]), " \u2013 ", fmtm(off[2]), " m")
  quals <- paste0(drift_res$n_anchors, " anchor", if (drift_res$n_anchors != 1L) "s" else "",
                  if (identical(status, "applied_with_gaps")) ", gaps" else "",
                  if (!is.na(drift_res$outcome$residual_m)) paste0("; residual ", fmtm(drift_res$outcome$residual_m), " m") else "")
  sprintf("depth drift: %s (%s)", off_txt, quals)
}


#' @keywords internal
#' @noRd
.correctDepthDrift <- function(depth, datetime, dry = NULL, positions = NULL, control = depthDriftControl()) {
  n <- length(depth)
  empty_anchors <- data.frame(time = numeric(0), offset = numeric(0), source = character(0),
                              stringsAsFactors = FALSE)
  none <- function(status) list(depth = depth, offset = rep(0, n), status = status, n_anchors = 0L,
                                anchors = integer(0), anchor_table = empty_anchors,
                                max_gap_h = NA_real_, low_confidence = .emptySpans(),
                                outcome = list(offset_range_m = c(0, 0), residual_m = NA_real_))
  if (!identical(control$method, "surface")) return(none("disabled"))
  if (n < 2L) return(none("abstained"))

  ev <- .gatherSurfaceEvidence(depth, datetime, dry, positions, control)
  a  <- ev$anchors
  a  <- a[is.finite(a$offset), , drop = FALSE]
  if (nrow(a) == 0) return(none("abstained"))
  tn <- as.numeric(datetime)
  counts <- table(factor(a$source, levels = c("dry", "gps", "depth")))

  if (nrow(a) < control$min.anchors) {
    off <- rep(stats::median(a$offset), n)                       # single constant offset
    status <- "constant_offset"; max_gap_h <- NA_real_; low <- .emptySpans()
  } else {
    if (anyDuplicated(a$time)) {                                 # collapse coincident anchor times
      ag <- stats::aggregate(offset ~ time, data = a, FUN = mean); xt <- ag$time; xo <- ag$offset
    } else { xt <- a$time; xo <- a$offset }
    off <- stats::approx(xt, xo, xout = tn, rule = 2)$y          # linear interpolation, hold the ends
    bp  <- c(tn[1], a$time, tn[n]); gaps <- diff(bp)
    max_gap_h <- max(gaps) / 3600
    wide <- which(gaps / 3600 > control$max.gap)
    low  <- if (length(wide)) data.frame(start = .POSIXct(bp[wide], "UTC"), end = .POSIXct(bp[wide + 1], "UTC"))
            else .emptySpans()
    status <- if (max_gap_h > control$max.gap) "applied_with_gaps" else "applied"
  }

  corrected <- depth - off
  resid <- if (any(ev$at_surface)) stats::quantile(abs(corrected[ev$at_surface]), 0.95, na.rm = TRUE) else NA_real_
  list(depth = corrected, offset = off, status = status, n_anchors = nrow(a),
       anchors = counts, anchor_table = a[, c("time", "offset", "source")],   # surface anchors used (diagnostics)
       max_gap_h = max_gap_h, low_confidence = low,
       outcome = list(offset_range_m = round(range(off), 3), residual_m = unname(round(resid, 3))))
}
