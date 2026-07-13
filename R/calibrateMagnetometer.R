#######################################################################################################
# Magnetometer hard/soft-iron calibration (optional, per-tag or per-package pooled) ###################
#######################################################################################################

# The magnetometer records the geomagnetic field distorted by the tag's own ferromagnetic parts
# (HARD-iron: a fixed additive offset) and by field-warping materials + per-axis sensitivity (SOFT-iron:
# a linear map). A clean calibration maps the raw ellipsoidal point cloud back onto a sphere:
#
#     m_calibrated = S %*% (m_raw - c)
#
# with c the hard-iron centre (length 3) and S the soft-iron matrix (3x3; diagonal = per-axis scale
# only, full = also cross-axis / misalignment). Estimating (c, S) needs the animal to rotate through
# enough orientations to populate the sphere. Free-swimming sharks mostly hold a near-horizontal
# posture, so the cloud is a thin band rather than a sphere and the fit is UNDER-DETERMINED - the full
# ellipsoid is typically ill-conditioned (validated: 50/51 fleet tags). Hence the honest design here:
# best-effort fit, graceful fallback (ellipsoid -> diagonal), pooling across a physical package to widen
# coverage, and a heading-CONFIDENCE flag from the radius CV + the IGRF dip residual. Nothing is applied
# to mx/my/mz by this function; the estimate is STORED for optional downstream use.

#' Robust rowwise norm of an n x 3 matrix.
#' @keywords internal
#' @noRd
.rowNorm3 <- function(M) sqrt(M[, 1]^2 + M[, 2]^2 + M[, 3]^2)

#' Diagonal (hard-iron + per-axis soft-iron) magnetometer fit.
#'
#' Wraps \code{.hardIronOffset} (robust 2/98-percentile sphere centre + per-axis half-range) and expresses
#' the per-axis sensitivity equalisation as a diagonal soft-iron matrix (scaling every axis to the mean
#' half-range, so the corrected field keeps its native magnitude). This reproduces exactly the inline
#' calibration processTagData applies, as a shared building block.
#' @param M n x 3 raw magnetometer matrix.
#' @param min.coverage Passed to \code{.hardIronOffset}.
#' @return list(center, soft_iron 3x3 diagonal, radius, coverage_ok, method_used = "diagonal"), or NULL.
#' @keywords internal
#' @noRd
.diagFit <- function(M, min.coverage = 0.5) {
  hi <- .hardIronOffset(M[, 1], M[, 2], M[, 3], min.coverage = min.coverage)
  if (is.null(hi)) return(NULL)
  scale <- mean(hi$half_range) / hi$half_range              # equalise per-axis, keep native magnitude
  list(center = hi$offset, soft_iron = diag(scale, 3), radius = hi$radius,
       coverage_ok = isTRUE(hi$coverage_ok), method_used = "diagonal")
}

#' Full-ellipsoid magnetometer fit (algebraic least squares + positive-definite / conditioning guard).
#'
#' Fits the general quadric \eqn{m^T Q m + u^T m = 1} to the point cloud, recovers the centre
#' \eqn{c = -\tfrac12 Q^{-1} u} and the ellipsoid form \eqn{A = Q / (1 + c^T Q c)}, and returns the
#' soft-iron matrix \eqn{S = A^{1/2}} (via a symmetric eigen-decomposition) that maps the ellipsoid onto
#' a UNIT sphere. Returns a weak flag (not a fit) when \eqn{A} is not positive-definite (the cloud does
#' not enclose an ellipsoid - the under-determined band case) or when the ellipsoid is too elongated
#' (condition number above `cond.max`), so the caller can fall back to the diagonal fit.
#' @param M n x 3 raw magnetometer matrix.
#' @param cond.max Maximum axis-ratio (condition number of A) accepted as a real ellipsoid. Default 25.
#' @return list(ok = TRUE, center, soft_iron 3x3 mapping to a UNIT sphere, condition), or
#'   list(ok = FALSE, reason, condition) when weak, or NULL when too few samples / singular.
#' @keywords internal
#' @noRd
.ellipsoidFit <- function(M, cond.max = 25) {
  M <- M[stats::complete.cases(M), , drop = FALSE]
  if (nrow(M) < 100L) return(NULL)
  # Pre-centre before forming the design matrix: fitting on raw coordinates makes the squared-term
  # columns dominated by the hard-iron offset^2, so the normal matrix crossprod(D) becomes severely
  # ill-conditioned whenever the offset is large relative to the field radius (or the data are raw ADC
  # counts) - qr.solve then fails and the ellipsoid is spuriously abandoned. The centre is restored below.
  mu <- colMeans(M)
  Mc <- sweep(M, 2, mu, "-")
  x <- Mc[, 1]; y <- Mc[, 2]; z <- Mc[, 3]
  D <- cbind(x * x, y * y, z * z, 2 * x * y, 2 * x * z, 2 * y * z, x, y, z)   # (m-mu)^T Q (m-mu) + u^T (m-mu) = 1
  beta <- tryCatch(qr.solve(crossprod(D), colSums(D)), error = function(e) NULL)   # colSums(D) = t(D) %*% 1
  if (is.null(beta) || any(!is.finite(beta))) return(NULL)
  Q <- matrix(c(beta[1], beta[4], beta[5],
                beta[4], beta[2], beta[6],
                beta[5], beta[6], beta[3]), 3, 3)
  Qinv <- tryCatch(solve(Q), error = function(e) NULL)
  if (is.null(Qinv)) return(NULL)
  cc <- as.numeric(-0.5 * (Qinv %*% beta[7:9]))             # centre in the pre-centred coordinates
  s <- 1 + as.numeric(t(cc) %*% Q %*% cc)                    # (m-mu-cc)^T (Q/s) (m-mu-cc) = 1
  if (!is.finite(s) || s == 0) return(NULL)
  A <- Q / s
  ev <- eigen(A, symmetric = TRUE)
  lam <- ev$values
  if (any(!is.finite(lam)) || min(lam) <= 0) return(list(ok = FALSE, reason = "non_pd", condition = NA_real_))
  cond <- max(lam) / min(lam)
  if (!is.finite(cond) || cond > cond.max) return(list(ok = FALSE, reason = "ill_conditioned", condition = cond))
  W <- ev$vectors %*% diag(sqrt(lam), 3) %*% t(ev$vectors)   # A^{1/2}: maps the ellipsoid onto a unit sphere
  list(ok = TRUE, center = mu + cc, soft_iron = W, condition = cond)   # centre restored to raw coordinates
}

#' Swept angular arc (degrees) covered by a set of angles - 360 minus the largest gap around the circle.
#' @keywords internal
#' @noRd
.sweptArc <- function(phi) {
  p <- sort((phi[is.finite(phi)] * 180 / pi) %% 360)
  if (length(p) < 2L) return(0)
  gaps <- c(diff(p), 360 - p[length(p)] + p[1])
  360 - max(gaps)
}

#' Band geometry of a centred magnetometer cloud (for the perpendicular pin + abort gates).
#'
#' PCA of the centred cloud: the thinnest principal direction is the band normal `nhat` (data-driven, so
#' mount-agnostic); `planarity` = smallest/largest eigenvalue (small = a clear band); `rho` = median
#' in-plane radius; `azimuth_span` = swept yaw arc (deg) in the band plane (detects a 1-D arc that the
#' per-axis span cannot); `extent` = overall orientation span (deg).
#' @keywords internal
#' @noRd
.bandGeometry <- function(M, center) {
  C <- sweep(as.matrix(M), 2, center, "-")
  C <- C[stats::complete.cases(C), , drop = FALSE]
  if (nrow(C) < 10L) return(NULL)
  pc <- eigen(stats::cov(C), symmetric = TRUE)              # eigenvalues in DECREASING order
  nhat <- pc$vectors[, 3]                                    # thinnest direction = band normal
  planarity <- if (pc$values[1] > 0) pc$values[3] / pc$values[1] else 1   # out-of-plane / in-plane: sphere ~1, band ~0
  linearity <- if (pc$values[1] > 0) pc$values[2] / pc$values[1] else 1   # 2nd in-plane / in-plane: band ~1, 1-D arc ~0
  Cip <- C - outer(as.numeric(C %*% nhat), nhat)            # in-plane residual
  rho <- stats::median(sqrt(rowSums(Cip^2)), na.rm = TRUE)
  phi <- atan2(as.numeric(C %*% pc$vectors[, 2]), as.numeric(C %*% pc$vectors[, 1]))
  list(nhat = nhat, rho = rho, planarity = planarity, linearity = linearity,
       azimuth_span = .sweptArc(phi), extent = max(.magAxisSpan(M, center), na.rm = TRUE))
}

#' Pin the UNOBSERVABLE perpendicular hard-iron centre component from the IGRF inclination.
#'
#' A thin band constrains the hard-iron centre in its plane but not perpendicular to it (a full-ellipsoid
#' fit's own perpendicular centre is unreliable there). Leaving that component at the band-plane midpoint
#' biases it by \eqn{F\sin(I)}, which leaks into heading as the animal pitches. The corrected field must sit
#' at the geomagnetic dip \eqn{I} to gravity, so the true centre is offset off the band plane by \eqn{d =
#' \rho\tan(I)} along the band normal. We therefore keep the WELL-constrained in-plane centre but RESET the
#' perpendicular coordinate to the robust band-plane midpoint `c_mid`, then offset by \eqn{\pm d} and pick
#' the sign whose corrected dip residual is smaller. Uses INCLINATION (robust) not intensity (which blows up
#' near zero dip). Gated: needs a finite IGRF inclination, co-framed gravity, a band normal, and a dip
#' residual that actually reconciles - otherwise it declines and leaves the centre at the midpoint.
#' @param center The in-plane centre estimate (the robust `.hardIronOffset` midpoint; its in-plane part is kept).
#' @param c_mid The robust `.hardIronOffset` midpoint (its perpendicular part is the band-plane base).
#' @return list(center, perp_source = "igrf_pin"|"prior_midpoint", dip_resid, pin_ok).
#' @keywords internal
#' @noRd
.pinPerpendicularCenter <- function(M, center, c_mid, W, igrf.incl, grav, bandgeo, control) {
  # keep the in-plane centre, reset the perpendicular coordinate to the robust band-plane midpoint;
  # this `base` (whose perpendicular is the reliable midpoint, not the unconstrained band-normal component)
  # is what a decline keeps
  base <- if (!is.null(bandgeo)) center - sum((center - c_mid) * bandgeo$nhat) * bandgeo$nhat else center
  decline <- function(dr = NA_real_) list(center = base, perp_source = "prior_midpoint", dip_resid = dr, pin_ok = FALSE)
  if (is.null(bandgeo) || is.null(grav) || !is.finite(igrf.incl)) return(decline())
  nhat <- bandgeo$nhat
  d <- bandgeo$rho * tan(igrf.incl * pi / 180)
  if (!is.finite(d)) return(decline())
  cands <- list(base + d * nhat, base - d * nhat)
  resid <- vapply(cands, function(cc) {
    r <- .magDipResidual(.applyMagCal(M, cc, W), grav, igrf.incl)$residual
    if (is.finite(r)) abs(r) else NA_real_ }, numeric(1))
  if (all(!is.finite(resid))) return(decline())
  k <- which.min(resid); dr <- resid[k]
  if (!is.finite(dr) || dr > control$igrf.residual.max) return(decline(dr))
  list(center = cands[[k]], perp_source = "igrf_pin", dip_resid = dr, pin_ok = TRUE)
}

#' Apply a stored magnetometer calibration to an n x 3 raw matrix: \eqn{S (m - c)} (rowwise).
#' @keywords internal
#' @noRd
.applyMagCal <- function(M, center, soft_iron) {
  sweep(as.matrix(M), 2, center, "-") %*% t(soft_iron)
}

#' The magnetometer family's 3x3 signed-permutation matrix from an axis-mapping `net`, or identity.
#' @keywords internal
#' @noRd
.magNetMatrix <- function(net) {
  if (is.null(net)) return(diag(3))
  m <- net[["mag"]]
  if (is.null(m) || !is.matrix(m) || !all(dim(m) == c(3L, 3L))) return(diag(3))
  m
}

#' Reframe an n x 3 magnetometer cloud from a source axis-mapping frame into a target frame.
#'
#' A hard/soft-iron fit is only valid in the frame it was made in (the `axis_net` gate in processTagData).
#' To fit a deployment's calibration from an EXTERNAL source recording, the source cloud must first be
#' expressed in the deployment's body frame. Both frames are signed permutations of the raw sensor axes;
#' the source cloud is `source-body = S . raw`, so `target-body = (T S^{-1}) . source-body`. Row vectors
#' map as `C %*% t(delta)` (the package convention `new_i = M[i,j] old_j`). Identity when the frames match
#' (the common same-package case), so this is a no-op unless the source was mapped differently.
#' @keywords internal
#' @noRd
.reframeMagCloud <- function(cloud, src_net, tgt_net) {
  S <- .magNetMatrix(src_net); T <- .magNetMatrix(tgt_net)
  if (identical(S, T)) return(cloud)
  delta <- T %*% t(S)                                  # source-body -> raw -> target-body
  as.matrix(cloud) %*% t(delta)
}

#' Fractional disagreement between two hard-iron centres, scaled by the field radius (dimensionless).
#'
#' The contamination detector the sphericity/dip QC cannot provide: a magnetic mass fixed relative to the
#' tag during an external calibration (boat steel, a dive tank) is absorbed into the centre yet leaves a
#' clean sphere at the right dip. Comparing the source centre to the deployment's own in-situ centre
#' surfaces it. Returns `Inf` when not computable (treated as a large disagreement by callers).
#' @keywords internal
#' @noRd
.magCenterAgreement <- function(center_src, center_insitu, radius) {
  if (is.null(center_insitu) || !all(is.finite(center_src)) || !all(is.finite(center_insitu)) ||
      !is.finite(radius) || radius <= 0) return(Inf)
  sqrt(sum((center_src - center_insitu)^2)) / radius
}

#' Per-axis angular coverage span (degrees) of a magnetometer cloud.
#'
#' How wide an arc the sample directions sweep along each sensor axis: on the centred, unit-normalised
#' cloud, the angular extent `asin(max) - asin(min)` of each component. A fully-tumbled cloud spans ~180
#' deg on every axis; a level-swimming band collapses toward 0 on the axis perpendicular to it. The
#' MINIMUM of the three is the limiting coverage - the intuitive "did we actually rotate enough" number,
#' and what to compare between an external calibration source and the deployment's own in-situ cloud
#' (a dedicated spin only helps if it beats in-situ here). Returns `c(NA, NA, NA)` when uncomputable.
#' @keywords internal
#' @noRd
.magAxisSpan <- function(M, center = NULL) {
  M <- as.matrix(M)
  if (!is.null(center)) M <- sweep(M, 2, center, "-")
  rn <- sqrt(rowSums(M^2)); ok <- is.finite(rn) & rn > 0
  if (sum(ok) < 10L) return(rep(NA_real_, 3))
  u <- M[ok, , drop = FALSE] / rn[ok]
  apply(u, 2, function(cc) { cc <- pmax(-1, pmin(1, cc[is.finite(cc)]))
                             if (length(cc) < 2L) NA_real_ else (asin(max(cc)) - asin(min(cc))) * 180 / pi })
}

#' Radius coefficient of variation of a corrected cloud (robust sphere-fit quality; 0 = perfect sphere).
#' @keywords internal
#' @noRd
.magRadcv <- function(Mc) {
  r <- .rowNorm3(Mc); r <- r[is.finite(r) & r > 0]
  if (length(r) < 10L) return(NA_real_)
  stats::sd(r) / stats::median(r)
}

#' Heading-confidence flag from the calibration QC.
#' high: well-covered sphere (low radius CV) AND, when checkable, a small IGRF dip residual.
#' @keywords internal
#' @noRd
.magConfidence <- function(radcv, igrf_residual, coverage_ok, radcv.max, igrf.residual.max,
                           status = NULL, azimuth_span = NA_real_, azimuth.min = 150) {
  dip_known <- is.finite(igrf_residual)
  # A 2D-fallback (thin band) fit corrects HARD-IRON ONLY (identity soft-iron). Heading (azimuth) trust
  # rests on IN-PLANE observability: a full swept yaw arc determines the in-plane centre (the textbook
  # horizontal-ring hard-iron estimate). The vertical DIP residual and the perpendicular-contaminated 3D
  # radcv are IRRELEVANT to heading and must NOT gate here - the GPS dead-reckoning CV showed that gating
  # on them discards satellite-verified heading gains (up to -45% track RMS, 78/88 held-out fixes) on
  # dive-heavy thin-band tags. coverage_ok is also FALSE by construction (no perpendicular spread), so it
  # is excluded too. Hard-cap at "medium": a band never verifies full 3D sphericity.
  if (identical(status, "calibrated_2d_fallback")) {
    base <- if (is.finite(azimuth_span) && azimuth_span >= azimuth.min) "medium" else "low"
    return(.capConfidence(base, "medium"))
  }
  if (!isTRUE(coverage_ok) || !is.finite(radcv)) return("low")
  # The IGRF dip residual is the ONLY check independent of the fit's own sphericity: a well-covered,
  # low-radcv sphere can still be mis-scaled or mis-oriented relative to the true geomagnetic field, and
  # only the dip catches it. So without a dip check (no co-framed accel or no deployment coordinates) the
  # fit is capped at "medium" - never "high" - however spherical it looks.
  if (dip_known && radcv <= radcv.max     && abs(igrf_residual) <= igrf.residual.max)     return("high")
  if (radcv <= 2 * radcv.max && (!dip_known || abs(igrf_residual) <= 2 * igrf.residual.max)) return("medium")
  "low"
}

#' Geomagnetic dip (inclination, degrees) of a corrected field against a gravity direction, and the
#' residual vs the IGRF inclination. Mirrors checkTagMapping's .resolveMag dip formula:
#' inclination = 90 - mean(angle between the unit field and the unit gravity vector).
#' @param Mc n x 3 corrected magnetometer.
#' @param grav n x 3 gravity (static-acceleration) direction, same rows as Mc.
#' @param igrf.incl Expected IGRF inclination (deg), or NA.
#' @return list(dip, residual) in degrees (both NA when not computable).
#' @keywords internal
#' @noRd
.magDipResidual <- function(Mc, grav, igrf.incl) {
  if (is.null(grav)) return(list(dip = NA_real_, residual = NA_real_))
  mn <- .rowNorm3(Mc); gn <- .rowNorm3(grav)
  ok <- is.finite(mn) & is.finite(gn) & mn > 0 & gn > 0
  if (sum(ok) < 100L) return(list(dip = NA_real_, residual = NA_real_))
  dotp <- rowSums((Mc[ok, , drop = FALSE] / mn[ok]) * (grav[ok, , drop = FALSE] / gn[ok]))
  ang  <- acos(pmax(-1, pmin(1, dotp))) * 180 / pi
  dip  <- 90 - mean(ang, na.rm = TRUE)
  list(dip = dip, residual = if (is.finite(igrf.incl)) dip - igrf.incl else NA_real_)
}

#' Fit a magnetometer calibration for one point cloud (the shared engine).
#'
#' Runs the requested `method` (ellipsoid, auto-falling-back to diagonal when the ellipsoid is
#' ill-conditioned / non-positive-definite), rescales the soft-iron so the corrected field keeps a target
#' magnitude (the IGRF intensity when supplied, else the cloud's own median radius, so the native uT scale
#' is preserved), and computes the QC (radius CV, IGRF dip residual, confidence). Does not touch the data.
#' @param M n x 3 raw magnetometer matrix (already pooled if per-package).
#' @param method "ellipsoid" or "diagonal".
#' @param min.coverage,cond.max Passed to the fits.
#' @param target.radius Target corrected magnitude (IGRF intensity, uT) or NA to keep the native radius.
#' @param grav,igrf.incl Optional gravity directions + IGRF inclination for the dip QC.
#' @param radcv.max,igrf.residual.max Confidence thresholds.
#' @return list(center, soft_iron 3x3, method_used, coverage_ok, radcv, dip, igrf_residual, condition,
#'   confidence, n), or NULL when the cloud is too small to fit at all.
#' @keywords internal
#' @noRd
.calibrateMag <- function(M, method = "ellipsoid", min.coverage = 0.5, cond.max = 25,
                          target.radius = NA_real_, grav = NULL, igrf.incl = NA_real_,
                          radcv.max = 0.1, igrf.residual.max = 15, planarity.max = 0.6,
                          azimuth.min = 150, linearity.abort = 0.1, extent.min = 40) {
  M <- as.matrix(M)
  n_finite <- sum(stats::complete.cases(M))
  # robust outlier trim: the LS ellipsoid fit (crossprod) and the PCA band geometry (cov) are NOT outlier-robust,
  # so a few spikes would skew the centre/planarity. Drop gross spikes beyond a robust radius (the quantile-
  # based .hardIronOffset IS robust). grav is kept row-aligned for the dip QC / pin.
  hi0 <- .hardIronOffset(M[, 1], M[, 2], M[, 3], min.coverage = 0)
  if (!is.null(hi0) && is.finite(hi0$radius) && hi0$radius > 0) {
    keep <- .rowNorm3(sweep(M, 2, hi0$offset, "-")) <= 4 * hi0$radius
    keep[!is.finite(keep)] <- FALSE
    if (sum(keep) >= 100L && sum(keep) < nrow(M)) {
      M <- M[keep, , drop = FALSE]
      if (!is.null(grav)) grav <- grav[keep, , drop = FALSE]
    }
  }
  status <- "calibrated_3d"; perp_source <- "data"
  azimuth_span <- NA_real_; extent <- NA_real_; recommend_apply <- TRUE
  # per-axis coverage of the (trimmed) cloud: TRUE only when EVERY axis is well sampled (a genuine 3D
  # cloud, e.g. a bench spin), FALSE for a thin swimming band. Gates whether the full soft-iron is trusted.
  hiC <- .hardIronOffset(M[, 1], M[, 2], M[, 3], min.coverage = min.coverage)
  coverage_ok <- !is.null(hiC) && isTRUE(hiC$coverage_ok)

  if (identical(method, "diagonal")) {                       # explicit escape hatch: per-axis fit only
    df <- .diagFit(M, min.coverage = min.coverage)
    if (is.null(df)) return(NULL)
    fit <- list(center = df$center, soft_iron = df$soft_iron, condition = NA_real_, method_used = "diagonal")
    status <- "calibrated_diagonal"
  } else {
    # 3D/2D gate: accept the FULL ellipsoid (hard + soft iron) only when the trimmed data GENUINELY
    # determine it - the exact `.ellipsoidFit` succeeds (a ridge-regularized variant was evaluated and
    # rejected: it shifted good-deployment headings 5-13 deg / 116 uT on strong soft-iron), the corrected
    # field is DIP-CONSISTENT
    # (a thin band's ellipsoid has an unconstrained perpendicular centre -> huge dip residual), AND the cloud
    # is well-covered on every axis. Otherwise (a swimming band) -> the constrained 2D fallback: robust
    # in-plane midpoint + IGRF perpendicular pin + IDENTITY soft-iron. Heading needs only the in-plane
    # hard-iron; the GPS dead-reckoning CV confirmed the thin-band soft-iron hurts and must not be applied.
    ef <- .ellipsoidFit(M, cond.max = cond.max)
    ef_dip_ok <- FALSE
    if (!is.null(ef) && isTRUE(ef$ok)) {
      # DIP CROSS-CHECK on the ellipsoid: a thin band can yield a numerically-successful ellipsoid whose
      # PERPENDICULAR centre is unconstrained -> a large geomagnetic dip residual (verified on the real
      # fleet: -42..-65 deg). The GPS dead-reckoning CV showed the full ellipsoid's thin-band soft-iron then
      # HURTS heading vs the hard-iron alone. So accept the 3D soft-iron only when the corrected field is
      # dip-consistent; otherwise fall to the hard-iron-only 2D path (which still fixes heading via the
      # in-plane centre). An uncheckable dip (no co-framed accel) is accepted - it cannot be judged, matching
      # the pre-check behaviour, and .magConfidence then caps such a fit at "medium".
      dip_ef  <- .magDipResidual(.applyMagCal(M, ef$center, ef$soft_iron), grav, igrf.incl)$residual
      ef_dip_ok <- !is.finite(dip_ef) || abs(dip_ef) <= igrf.residual.max
    }
    if (!is.null(ef) && isTRUE(ef$ok) && ef_dip_ok && coverage_ok) {
      fit <- list(center = ef$center, soft_iron = ef$soft_iron, condition = ef$condition, method_used = "ellipsoid")
      # calibrated_3d; the data constrains the perpendicular, so no band geometry / pin / abort needed
    } else {
      hi <- hiC                                              # reuse the up-front robust centre (offset/radius are
      if (is.null(hi)) {                                      # min.coverage-independent) -> too few samples: last-ditch diagonal
        df <- .diagFit(M, min.coverage = min.coverage)
        if (is.null(df)) return(NULL)
        fit <- list(center = df$center, soft_iron = df$soft_iron, condition = NA_real_, method_used = "diagonal (fallback)")
        status <- "calibrated_diagonal"
      } else {
        cmid <- hi$offset
        bg   <- .bandGeometry(M, cmid)                        # band geometry about the ROBUST midpoint
        azimuth_span <- if (!is.null(bg)) bg$azimuth_span else NA_real_
        extent <- if (!is.null(bg)) bg$extent else max(.magAxisSpan(M, cmid), na.rm = TRUE)
        pin  <- .pinPerpendicularCenter(M, cmid, cmid, diag(3), igrf.incl, grav, bg,
                                        list(igrf.residual.max = igrf.residual.max))
        fit <- list(center = pin$center, soft_iron = diag(3), condition = 1, method_used = "constrained 2d fallback")
        status <- "calibrated_2d_fallback"; perp_source <- pin$perp_source
        # abort: the in-plane centre is unobservable when the cloud is not a genuine planar ring -
        #  - a stationary blob: tiny angular extent, OR isotropic (planarity -> 1) so noise fills every
        #    azimuth and azimuth_span alone would falsely read "medium"; caught by planarity > planarity.max;
        #  - a single heading held (1-D arc): the 2nd in-plane variance collapses, so linearity -> 0.
        linearity <- if (!is.null(bg)) bg$linearity else 1
        planarity <- if (!is.null(bg)) bg$planarity else 1
        recommend_apply <- !((is.finite(extent) && extent < extent.min) ||
                             (is.finite(linearity) && linearity < linearity.abort) ||
                             (is.finite(planarity) && planarity > planarity.max))
      }
    }
  }

  # rescale the soft-iron so the corrected field keeps a meaningful magnitude (unchanged)
  Mc0 <- .applyMagCal(M, fit$center, fit$soft_iron)
  r0  <- stats::median(.rowNorm3(Mc0), na.rm = TRUE)
  tgt <- if (is.finite(target.radius) && target.radius > 0) target.radius else
           stats::median(.rowNorm3(sweep(M, 2, fit$center, "-")), na.rm = TRUE)
  if (is.finite(r0) && r0 > 0 && is.finite(tgt) && tgt > 0) fit$soft_iron <- fit$soft_iron * (tgt / r0)

  Mc  <- .applyMagCal(M, fit$center, fit$soft_iron)
  radcv <- .magRadcv(Mc)
  dipr  <- .magDipResidual(Mc, grav, igrf.incl)
  conf  <- .magConfidence(radcv, dipr$residual, coverage_ok, radcv.max, igrf.residual.max,
                          status = status, azimuth_span = azimuth_span, azimuth.min = azimuth.min)
  # a 2D fallback that tripped an abort gate is degenerate (a stationary blob scatters noise across all
  # azimuths, so azimuth_span alone would read "medium") - keep its confidence consistent with the fact
  # that it will not be applied.
  if (identical(status, "calibrated_2d_fallback") && !isTRUE(recommend_apply)) conf <- "low"
  list(center = fit$center, soft_iron = fit$soft_iron, method_used = fit$method_used,
       coverage_ok = coverage_ok, axis_span = .magAxisSpan(M, fit$center),
       radcv = radcv, dip = dipr$dip, igrf_residual = dipr$residual,
       condition = fit$condition, n = n_finite, status = status, perp_source = perp_source,
       azimuth_span = azimuth_span, recommend_apply = recommend_apply,
       confidence = conf)
}

#' Call .calibrateMag with all the thresholds sourced from a magCalibrationControl object. The `%||%`
#' guards let a bare named list (missing some band-fit fields) still work.
#' @keywords internal
#' @noRd
.calFromControl <- function(M, control, target.radius = NA_real_, grav = NULL, igrf.incl = NA_real_) {
  .calibrateMag(M, method = control$method, min.coverage = control$min.coverage, cond.max = control$cond.max,
                target.radius = target.radius, grav = grav, igrf.incl = igrf.incl,
                radcv.max = control$radcv.max, igrf.residual.max = control$igrf.residual.max,
                planarity.max = control$planarity.max %||% 0.6,
                azimuth.min = control$azimuth.min %||% 150, linearity.abort = control$linearity.abort %||% 0.1,
                extent.min = control$extent.min %||% 40)
}

#' Extract a decimated magnetometer point cloud + gravity direction from a tag.
#'
#' Strides the raw mx/my/mz to at most `target.n` points (preserving the raw values that populate the
#' calibration sphere) and, when accel + timestamps are present, a matching low-passed static-acceleration
#' (gravity) direction for the dip QC. When `paddle = TRUE`, first de-noises the magnetometer with a short
#' rolling mean - a spinning paddle-wheel magnet injects strong high-frequency noise that would otherwise
#' corrupt the fit (mirrors processTagData / checkTagMapping). Returns row-aligned `mag` and `grav` (both
#' may carry NAs; the fit and the dip QC filter internally), or NULL when there is no usable magnetometer.
#' @keywords internal
#' @noRd
.magGather <- function(x, datetime.col, paddle = FALSE, target.n = 8000L, grav.sec = 2, paddle.sec = 3) {
  if (!all(c("mx", "my", "mz") %in% names(x))) return(NULL)
  M <- cbind(x[["mx"]], x[["my"]], x[["mz"]])
  if (sum(stats::complete.cases(M)) < 200L) return(NULL)
  n <- nrow(x)
  fs <- if (datetime.col %in% names(x)) tryCatch(.tagFs(x, datetime.col), error = function(e) NA_real_) else NA_real_
  lp <- function(v, sec) data.table::frollmean(v, max(2L, as.integer(round(sec * fs))), fill = NA, align = "center")
  # paddle-wheel de-noise: remove the high-frequency spinning-magnet oscillation (the shared vector-domain
  # primitive), keeping the slow orientation-driven variation the calibration sphere is built from.
  if (isTRUE(paddle) && is.finite(fs) && fs > 0)
    M <- .magDenoise(M, fs, paddle.sec)
  stride <- max(1L, n %/% target.n)
  idx <- seq(1L, n, by = stride)
  grav <- NULL
  if (all(c("ax", "ay", "az") %in% names(x)) && is.finite(fs) && fs > 0)
    grav <- cbind(lp(x[["ax"]], grav.sec), lp(x[["ay"]], grav.sec), lp(x[["az"]], grav.sec))[idx, , drop = FALSE]
  list(mag = M[idx, , drop = FALSE], grav = grav)
}

#' IGRF total-field intensity (uT) + inclination (deg) at a deployment, or NAs.
#' @keywords internal
#' @noRd
.magIGRF <- function(dmeta) {
  out <- list(intensity = NA_real_, inclination = NA_real_)
  lon <- dmeta$lon %||% NA_real_; lat <- dmeta$lat %||% NA_real_; when <- dmeta$datetime %||% as.POSIXct(NA)
  if (is.finite(lon) && is.finite(lat) && !is.na(when)) {
    f <- tryCatch(oce::magneticField(longitude = lon, latitude = lat, time = when), error = function(e) NULL)
    if (!is.null(f)) out <- list(intensity = f$intensity / 1000, inclination = f$inclination)   # nT -> uT
  }
  out
}

#' Cap a confidence flag at a maximum level (low < medium < high).
#' @keywords internal
#' @noRd
.capConfidence <- function(conf, cap) {
  ord <- c(low = 1L, medium = 2L, high = 3L)
  if (is.null(conf) || is.na(conf) || is.na(match(conf, names(ord)))) return(conf)
  if (ord[[conf]] <= ord[[cap]]) conf else cap
}

#' Ingest external calibration recordings into per-source clouds + identity keys.
#'
#' Mirrors the per-deployment gather in the main function but for the `calibration.data` recordings: a
#' decimated (paddle-de-noised) magnetometer cloud, the package/logger identity keys used for matching, the
#' paddle flag, the axis-mapping net (so a source mapped in a different frame can be reframed), and the
#' recording's own coordinates (for provenance / QC re-anchoring). Sources with no usable magnetometer are
#' dropped.
#' @keywords internal
#' @noRd
.ingestCalSources <- function(calibration.data, match_keys, datetime.col, id.col, context) {
  r <- .resolveInput(calibration.data, id.col = id.col)
  ctx <- if (is.null(context)) rep(NA_character_, r$n) else rep(as.character(context), length.out = r$n)
  out <- vector("list", r$n)
  for (j in seq_len(r$n)) {
    x <- r$get(j)
    if (!data.table::is.data.table(x)) x <- data.table::as.data.table(x)
    meta <- .getMeta(x)
    true_id <- tryCatch(as.character(unique(x[[id.col]])[1]), error = function(e) NA_character_)
    who <- if (!is.na(true_id)) true_id else r$ids[j]
    paddle <- isTRUE(meta$tag$paddle_wheel)
    cloud <- .magGather(x, datetime.col, paddle = paddle)
    if (is.null(cloud)) next
    keyval <- function(k) { v <- meta$tag[[k]]; if (is.null(v) || length(v) != 1 || is.na(v) || !nzchar(as.character(v))) NA_character_ else as.character(v) }
    out[[j]] <- list(id = who, keys = stats::setNames(lapply(match_keys, keyval), match_keys),
                     paddle = paddle, cloud = cloud, net = meta$axis_mapping$net,
                     igrf = .magIGRF(meta$deployment), context = ctx[j])
  }
  Filter(Negate(is.null), out)
}

#' Match a deployment to its external calibration source(s) by identity key priority + paddle state.
#'
#' A bind is valid only under a strict conjunction: the FIRST match key (priority order) whose value is
#' non-blank and equal on both sides, AND an equal paddle-wheel flag (the spinning-magnet field cannot be
#' captured by a static ellipsoid, so paddle and non-paddle are never bound). Returns the matched source
#' records + the key that bound them, or NULL when nothing matches.
#' @keywords internal
#' @noRd
.matchCalSource <- function(target, sources, match_priority) {
  for (key in match_priority) {
    tv <- target$keys[[key]]
    if (is.null(tv) || is.na(tv) || !nzchar(tv)) next
    hit <- Filter(function(s) {
      sv <- s$keys[[key]]
      !is.null(sv) && !is.na(sv) && nzchar(sv) && identical(sv, tv) && identical(s$paddle, target$paddle)
    }, sources)
    if (length(hit)) return(list(sources = hit, key = key))
  }
  NULL
}

#' Fit a deployment's calibration from matched external source cloud(s).
#'
#' Reframes each matched source cloud into the deployment's body frame, pools them, and fits the shared
#' hard/soft-iron. The soft-iron SHAPE always comes from the (coverage-rich) source. The hard-iron CENTRE
#' is chosen conservatively: for a paddle deployment it is taken from the deployment's own in-situ estimate
#' (a static-paddle source centre is meaningless); otherwise it is the source centre, but only after a
#' cross-check against the in-situ centre (`center.reject`/`center.warn`) that catches a magnetic mass
#' co-rotating with the tag during the calibration. The field magnitude is re-anchored to the deployment's
#' IGRF, and all QC (radcv, dip) is evaluated on the deployment's own cloud - the data the fit will act on.
#' Returns list(cal, provenance) on success, list(reject = TRUE, reason) when the centre cross-check fails.
#' @keywords internal
#' @noRd
.fitFromSource <- function(target, matched, control) {
  # pool the reframed source clouds (each source may itself be a paddle-de-noised decimated cloud)
  Msrc <- do.call(rbind, lapply(matched$sources, function(s) .reframeMagCloud(s$cloud$mag, s$net, target$net)))
  ig   <- target$igrf
  tgt_intensity <- if (!is.null(control$target.field)) control$target.field
                   else if (control$igrf.normalize && is.finite(ig$intensity)) ig$intensity else NA_real_
  fit <- .calFromControl(Msrc, control, target.radius = tgt_intensity)
  if (is.null(fit)) return(list(reject = TRUE, reason = "source fit failed"))
  soft <- fit$soft_iron; source_center <- fit$center

  # the deployment's own in-situ hard-iron centre: the contamination reference + the paddle centre source
  own  <- target$cloud$mag
  hi   <- .hardIronOffset(own[, 1], own[, 2], own[, 3], min.coverage = 0)
  insitu_center <- if (!is.null(hi)) hi$offset else NULL
  radius <- if (!is.null(hi) && is.finite(hi$radius) && hi$radius > 0) hi$radius else
              stats::median(.rowNorm3(sweep(Msrc, 2, source_center, "-")), na.rm = TRUE)
  delta <- .magCenterAgreement(source_center, insitu_center, radius)

  conf_cap <- "high"
  if (isTRUE(target$paddle)) {
    # paddle deployment: soft-iron shape from the source, hard-iron centre from in-situ (never a frozen paddle)
    center_used <- insitu_center %||% source_center
    center_source <- if (!is.null(insitu_center)) "in_situ" else "source"
    if (is.null(insitu_center)) conf_cap <- "medium"     # no in-situ centre to anchor on -> less certain
  } else {
    if (is.finite(delta) && delta > control$center.reject)
      return(list(reject = TRUE, reason = sprintf("hard-iron centre disagrees with in-situ by %.0f%% of the field radius (> %.0f%%): probable magnetic contamination of the calibration",
                                                  100 * delta, 100 * control$center.reject)))
    center_used <- source_center; center_source <- "source"
    if (is.finite(delta) && delta > control$center.warn) conf_cap <- "medium"
  }

  # re-anchor the soft-iron scale to the target field magnitude for the CHOSEN centre, then QC on own cloud
  Mc0 <- .applyMagCal(own, center_used, soft)
  r0  <- stats::median(.rowNorm3(Mc0), na.rm = TRUE)
  tgt <- if (is.finite(tgt_intensity) && tgt_intensity > 0) tgt_intensity else
           stats::median(.rowNorm3(sweep(own, 2, center_used, "-")), na.rm = TRUE)
  if (is.finite(r0) && r0 > 0 && is.finite(tgt) && tgt > 0) soft <- soft * (tgt / r0)

  Mc    <- .applyMagCal(own, center_used, soft)
  radcv <- .magRadcv(Mc)
  dipr  <- .magDipResidual(Mc, target$cloud$grav, ig$inclination)
  conf  <- .magConfidence(radcv, dipr$residual, isTRUE(fit$coverage_ok), control$radcv.max, control$igrf.residual.max,
                          status = fit$status, azimuth_span = fit$azimuth_span, azimuth.min = control$azimuth.min %||% 150)
  conf  <- .capConfidence(conf, conf_cap)

  # coverage-gain check: a dedicated spin only helps if it out-covers the deployment's own cloud. Compare
  # the limiting (minimum) per-axis angular span; a source that is no better is applied but flagged.
  src_span    <- .magAxisSpan(Msrc, source_center)
  insitu_span <- .magAxisSpan(own, insitu_center %||% source_center)
  low_gain <- all(is.finite(src_span)) && all(is.finite(insitu_span)) && min(src_span) <= min(insitu_span)

  cal <- list(center = center_used, soft_iron = soft, method_used = fit$method_used,
              coverage_ok = isTRUE(fit$coverage_ok), axis_span = src_span, radcv = radcv, dip = dipr$dip,
              igrf_residual = dipr$residual, confidence = conf)
  prov <- list(source_ids = vapply(matched$sources, function(s) as.character(s$id), character(1)),
               context = matched$sources[[1]]$context %||% NA_character_,
               center_delta = if (is.finite(delta)) delta else NA_real_, center_source = center_source)
  list(reject = FALSE, cal = cal, provenance = prov, n = length(matched$sources), low_coverage_gain = low_gain)
}

#' Estimate and store an optional hard/soft-iron magnetometer calibration
#'
#' @description
#' Fits a magnetometer calibration - the hard-iron centre and the soft-iron matrix that map the raw
#' `mx/my/mz` point cloud back onto a sphere - and STORES it in each deployment's metadata
#' (`tagMetadata(x)$mag_calibration`) together with quality-control metrics and a heading-confidence flag.
#' It does **not** modify `mx/my/mz`; the stored estimate is applied later, optionally, by
#' \code{\link{processTagData}} (see `use.stored` in \code{\link{calibrationControl}}).
#'
#' @details
#' \strong{What the calibration does.} A magnetometer reports the local magnetic field on three axes, but the
#' tag itself distorts it. Permanently magnetised parts and nearby steel add a fixed offset - the
#' \emph{hard-iron} - while ferromagnetic material stretches and shears the field differently along each axis -
#' the \emph{soft-iron}. The calibrated field is
#' \deqn{M_{corrected} = S\,(M_{raw} - c),}
#' with \eqn{c} the 3-vector hard-iron centre and \eqn{S} the 3x3 soft-iron matrix. Geometrically, as the
#' animal turns the raw field traces an off-centre, tilted ellipsoid; \eqn{c} re-centres it and \eqn{S} maps
#' it back onto a sphere of the true field magnitude. Left uncorrected, the hard-iron alone swings the compass
#' heading by tens of degrees, and that error then accumulates into a drifting dead-reckoned track.
#'
#' \strong{Why free-swimming data is hard.} Recovering \eqn{c} and \eqn{S} needs the animal to rotate through
#' enough orientations to populate the whole sphere. A level-swimming shark mostly holds a near-horizontal
#' posture, so its field cloud is a thin, near-horizontal \emph{band} rather than a full sphere. In the plane
#' of the band (the horizontal circle swept as the animal changes heading) the centre is well determined; but
#' \emph{perpendicular} to the band the data barely move, so that component of \eqn{c} - and the out-of-plane
#' soft-iron shape - are effectively unobservable. Hence a best-effort estimate carrying an explicit, honest
#' \strong{confidence} flag rather than a guaranteed correction.
#'
#' \strong{The fit, step by step.}
#' \enumerate{
#'   \item \emph{Robust outlier trim.} Gross spikes are dropped first; the least-squares fit and the
#'     band-geometry PCA are not outlier-robust, so a few bad samples would skew the centre and shape.
#'   \item \emph{Full ellipsoid.} An algebraic least-squares ellipsoid (hard + soft iron together) is fitted.
#'   \item \emph{Accept the 3D fit only when the data earn it} (status \code{"calibrated_3d"}). The full
#'     soft-iron is trusted only when the cloud is genuinely three-dimensional - well covered on every axis
#'     AND \emph{dip-consistent}: after correction the field must sit at the true geomagnetic dip (inclination
#'     \eqn{I}) to gravity. A thin band can yield a numerically valid ellipsoid whose out-of-plane centre is
#'     arbitrary, betrayed by a large dip residual; such a fit is refused and routed to the fallback.
#'   \item \emph{Hard-iron-only 2D fallback for a band} (status \code{"calibrated_2d_fallback"}). Only the
#'     hard-iron is estimated: the robust in-plane centre (the midpoint of the swept horizontal circle) plus
#'     the single unobservable perpendicular component, which is \emph{pinned} from the geomagnetic
#'     inclination. Because the corrected field must dip by \eqn{I} to gravity, the true centre lies a distance
#'     \eqn{d = \rho\tan(I)} off the band plane (\eqn{\rho} = in-plane radius) along the band normal; the sign
#'     is chosen to best match the measured dip. The soft-iron is left at identity - an under-sampled scale
#'     cannot be trusted and must not distort the field.
#'   \item \emph{Confidence, matched to what heading actually needs.} Compass heading is read from the
#'     \emph{horizontal} field after tilt-compensation, so it depends on the in-plane hard-iron, not on the
#'     vertical/dip component. A full 3D fit earns \code{"high"} when it is spherical (low corrected-radius
#'     coefficient of variation) and dip-consistent. A 2D fallback is capped at \code{"medium"} and earns it
#'     from the \emph{swept yaw arc} alone: a near-complete rotation pins the in-plane centre, so the heading
#'     is trustworthy even though the perpendicular and soft-iron are not (this is validated against held-out
#'     GPS fixes - the dip and out-of-plane sphericity, being vertical, do not gate heading trust). Too little
#'     rotation, a single held heading, or a near-stationary blob give \code{"low"} - the heading is left raw.
#'     Treat \code{"low"} as "do not trust the calibrated heading".
#' }
#'
#' \strong{Per-package pooling.} Deployments that share a physical tag (`group.by`, default `package_id`;
#' pass several field names for a composite key, e.g. `c("package_id", "logger_id")`) carry the SAME
#' hard/soft-iron, but each samples a different band of
#' orientations. Pooling them (each field rescaled to a common magnitude first, optionally the IGRF
#' intensity) widens the effective sphere coverage and can lift an otherwise ill-posed fit into a usable
#' one. The pooled soft-iron shape is shared across the group; the hard-iron centre and field scale stay
#' per deployment. A package with a single deployment (or a blank key) is calibrated on its own
#' (`source = "per_tag"`). The fit method and thresholds are set via \code{\link{magCalibrationControl}}.
#'
#' \strong{Paddle wheel.} A magnetic paddle-wheel speed sensor injects strong high-frequency noise into
#' `mx/my/mz`; when `tag$paddle_wheel = TRUE` the cloud is de-noised with a short rolling mean before the
#' fit (as in \code{\link{processTagData}} / \code{\link{checkTagMapping}}). Because the paddle magnet is
#' also part of the tag's ferromagnetic environment, paddle and non-paddle deployments of one package are
#' calibrated separately - they are never pooled together.
#'
#' Run this AFTER \code{\link{applyAxisMapping}} (so the magnetometer and accelerometer share the body
#' frame the dip QC needs) and before \code{\link{processTagData}}.
#'
#' \strong{Diagnostic report.} When `plot` or `plot.file` is set, the function draws a worst-first
#' summary page (one row per deployment: pooling, method, sphere coverage, radius CV, dip residual,
#' confidence) followed by a detail page for each low/medium-confidence deployment (or every deployment
#' under `force.plots`). A detail page shows the raw vs corrected magnetometer cloud as three plane
#' projections with the target-field circle, the corrected field-magnitude histogram, and the geomagnetic
#' dip distribution against the IGRF inclination. The three panels answer the three questions behind the
#' confidence flag - is the field centred and spherical? tightly on a sphere? physically consistent with
#' the expected field? - so a low-confidence verdict is explained (a band that covers only a patch of the
#' sphere, or a dip far from - or split away from - the IGRF value), not merely asserted.
#'
#' @param data Input data: a `nautilus_tag` / data.frame, a (named) list of them, or a character vector
#'   of paths to `.rds` files. The output of \code{\link{applyAxisMapping}} is expected.
#' @param control A \code{\link{magCalibrationControl}} object (or a named list of its fields).
#' @param group.by Character vector naming the `tag` metadata key(s) that deployments sharing a physical
#'   tag are pooled by, one or more of `"package_id"` (default), `"logger_id"`, `"tag"` (model) and
#'   `"type"`. Pass several for a composite key (e.g. `c("package_id", "logger_id")`, pooling separately
#'   for each unique combination) - handled exactly as in \code{\link{consensusAxisMapping}}. A deployment
#'   missing any of the keys is calibrated on its own.
#' @param calibration.data Optional external calibration recording(s) to fit each deployment's calibration
#'   from, instead of its own (often under-covered) deployment cloud - e.g. a dedicated pre-deployment or
#'   post-recovery rotation through many orientations, or an untrimmed import. Accepts the SAME input types
#'   as `data` (a `nautilus_tag`, a named list, or file paths). Each recording is matched to a deployment by
#'   tag identity (`calibration.match`), its cloud reframed into the deployment's body frame, and the fit
#'   applied to the deployment - so the correction is estimated from good coverage but acts on the real
#'   data. Recordings sharing a deployment's key are pooled. `NULL` (default) reproduces the in-situ
#'   behaviour exactly. The soft-iron shape always comes from the source; the hard-iron centre is
#'   cross-checked against (and, for paddle tags, taken from) the deployment's own in-situ estimate to
#'   catch a magnetic mass that co-rotated with the tag during calibration (see `center.reject` in
#'   \code{\link{magCalibrationControl}}). A dedicated calibration should span as many orientations as
#'   possible in a magnetically clean setting (away from vessel/steel); it is judged by its QC, not trusted
#'   by label. The applied source, context, and centre agreement are recorded in `meta$mag_calibration`.
#' @param calibration.match Character vector of `tag` identity fields used to bind a `calibration.data`
#'   recording to a deployment, in priority order. Default `c("package_id", "logger_id")`: a non-blank,
#'   exactly-equal `package_id` (the physical housing, whose axis orientation and iron are fixed) is tried
#'   first, then `logger_id`. A bind additionally requires an equal `paddle_wheel` flag. Ignored when
#'   `calibration.data` is `NULL`.
#' @param calibration.on.missing What to do for a deployment with no usable `calibration.data` bind (no key
#'   match, or a source rejected by the centre cross-check): `"fallback"` (default) fits from the
#'   deployment's own cloud, as if no source were given, and reports it; `"error"` aborts; `"skip"` leaves
#'   the deployment uncalibrated. Ignored when `calibration.data` is `NULL`.
#' @param calibration.context Optional character label(s) annotating how the `calibration.data` was
#'   collected (e.g. `"in_water"`, `"bench"`, `"lab"`), recycled over the recordings and stored in
#'   `meta$mag_calibration$context` for provenance. Purely descriptive - it never changes the QC thresholds
#'   (calibration quality is judged from the diagnostics, not an assumed environment). Ignored when
#'   `calibration.data` is `NULL`.
#' @param return.data Logical. Return the processed data in memory (default `TRUE`). When `FALSE`, the
#'   function instead returns the paths of the `.rds` files it wrote, which feed directly into the next
#'   step's `data` argument -- so a large fleet can be processed without ever holding it all in memory.
#'   `return.data = FALSE` therefore requires an `output.dir`.
#' @param output.dir Character. Directory in which to write one `<id>.rds` file per deployment. Providing
#'   a directory is what triggers saving; `NULL` (default) writes nothing. The directory must already exist.
#' @param output.suffix Character. Optional suffix appended to each saved file name (before `.rds`), e.g.
#'   to tag a processing run or avoid clashes. Only used when `output.dir` is set. Default `NULL`.
#' @param compress Compression for the saved `.rds` files (only used when `output.dir` is set): `TRUE`
#'   (default, gzip), `FALSE`, or one of `"gzip"`/`"bzip2"`/`"xz"`. See \code{\link[base]{saveRDS}}.
#' @param plot Logical. If `TRUE`, draw the diagnostic report (see Details) to the active graphics
#'   device. Default `FALSE`.
#' @param plot.file Character. Path to a single multi-page PDF for the diagnostic report. The parent
#'   directory must exist. Default `NULL` (no file).
#' @param force.plots Logical. If `TRUE`, draw a detail page for every deployment, not only the
#'   low/medium-confidence ones. Default `FALSE`.
#' @param id.col,datetime.col Column names for the animal ID and timestamp. Defaults `"ID"`/`"datetime"`.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal", or `2`/"detailed" (default).
#'
#' @return If `return.data = TRUE`, a named list of the input objects, each with its `mag_calibration`
#'   metadata populated. The estimate is written to the `proposed` block
#'   (`tagMetadata(x)$mag_calibration$proposed`), a list of: `params` (`center`, `soft_iron`, `axis_net`),
#'   `qc` (`confidence`, `coverage_ok`, `radcv`, `igrf_residual`, `axis_span`), and `provenance` (`method`,
#'   `source`, `group`, `n_deployments`, plus any external-source fields). The raw `mx/my/mz` are left
#'   untouched and `applied` stays `FALSE` - \code{\link{processTagData}} applies the estimate later, only if
#'   it clears the confidence gate. If `return.data = FALSE`, a character vector of the written `.rds` file paths.
#' @seealso \code{\link{magCalibrationControl}}, \code{\link{applyAxisMapping}}, \code{\link{processTagData}}
#' @examples
#' \dontrun{
#' # Run after applyAxisMapping(), before processTagData()
#' oriented <- applyAxisMapping(data = files, mapping = qc)
#' cal <- calibrateMagnetometer(oriented, group.by = "package_id",
#'                              plot.file = "mag_cal.pdf")
#' tagMetadata(cal[[1]])$mag_calibration$proposed$qc$confidence
#'
#' # Pool separately for each unique package_id-logger_id combination (a composite key)
#' cal <- calibrateMagnetometer(oriented, group.by = c("package_id", "logger_id"))
#'
#' # Optionally fit from a dedicated rotation recording rather than the deployment cloud
#' cal <- calibrateMagnetometer(oriented, calibration.data = "spin.rds",
#'                              calibration.match = c("package_id", "logger_id"))
#' }
#' @export
calibrateMagnetometer <- function(data,
                                  control = magCalibrationControl(),
                                  group.by = "package_id",
                                  calibration.data = NULL,
                                  calibration.match = c("package_id", "logger_id"),
                                  calibration.on.missing = c("fallback", "error", "skip"),
                                  calibration.context = NULL,
                                  return.data = TRUE,
                                  output.dir = NULL,
                                  output.suffix = NULL,
                                  compress = TRUE,
                                  plot = FALSE,
                                  plot.file = NULL,
                                  force.plots = FALSE,
                                  id.col = "ID",
                                  datetime.col = "datetime",
                                  verbose = "detailed") {

  lvl <- .verbosity(verbose)
  control <- .as_control(control, magCalibrationControl, "nautilus_mag_calibration", "control")
  .assert_flag(return.data, "return.data")
  .assert_flag(plot, "plot"); .assert_flag(force.plots, "force.plots")
  .assert_writable_file(plot.file, "plot.file", ext = "pdf")
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  group.by <- .validateGroupBy(group.by); .assert_string(output.suffix, "output.suffix", null_ok = TRUE)
  .assert_dir(output.dir, "output.dir"); .assert_compress(compress)
  .assert_output(return.data, output.dir)
  # external calibration-source options (all inert when calibration.data is NULL)
  calibration.on.missing <- match.arg(calibration.on.missing)
  valid_keys <- c("package_id", "logger_id")
  bad_keys <- setdiff(calibration.match, valid_keys)
  if (length(bad_keys))
    .abort(c("{.arg calibration.match} has invalid key{?s} {.val {bad_keys}}.", "i" = "Valid keys: {.val {valid_keys}}."))
  if (!length(calibration.match)) .abort("{.arg calibration.match} must name at least one key.")
  if (!is.null(calibration.context) && (!is.character(calibration.context) || !length(calibration.context)))
    .abort("{.arg calibration.context} must be a non-empty character vector (or NULL).")

  start.time <- Sys.time()
  r <- .resolveInput(data, id.col = id.col)
  # ingest external calibration recordings, if any (each is matched to a deployment below)
  sources <- if (!is.null(calibration.data))
    .ingestCalSources(calibration.data, calibration.match, datetime.col, id.col, calibration.context) else NULL
  results <- if (return.data) vector("list", r$n) else NULL
  saved <- vector("list", r$n)
  make_plots <- plot || !is.null(plot.file)
  summary_records <- if (make_plots) vector("list", r$n) else NULL   # one report-table row per deployment
  payloads <- list()                                                 # a small detail-page payload per flagged tag

  hdr <- sprintf("Input: %d dataset%s", r$n, if (r$n != 1) "s" else "")
  if (!is.null(output.dir)) hdr <- c(hdr, paste0("Output: ", output.dir))
  .log_header(lvl, "calibrateMagnetometer", "Estimating the magnetometer hard/soft-iron calibration",
              bullets = hdr, arrow = sprintf("method: %s%s, pooled by: %s", control$method,
              if (control$igrf.normalize) " (IGRF-normalised)" else "", paste(group.by, collapse = " x ")))

  # ---- Pass 1: gather a decimated cloud + gravity + metadata per deployment (bounded memory) ----
  info <- vector("list", r$n)
  for (i in seq_len(r$n)) {
    x <- r$get(i)
    if (!data.table::is.data.table(x)) x <- data.table::as.data.table(x)
    meta <- .getMeta(x)
    true_id <- tryCatch(as.character(unique(x[[id.col]])[1]), error = function(e) NA_character_)
    who <- if (!is.na(true_id)) true_id else r$ids[i]
    paddle <- isTRUE(meta$tag$paddle_wheel)
    gathered <- .magGather(x, datetime.col, paddle = paddle)
    keyval <- function(k) { v <- meta$tag[[k]]; if (is.null(v) || length(v) != 1 || is.na(v) || !nzchar(as.character(v))) NA_character_ else as.character(v) }
    key  <- .compositeGroupKey(group.by, function(f) meta$tag[[f]])   # composite key over one or more group.by fields
    info[[i]] <- list(id = who, key = if (is.na(key) || !nzchar(key)) NA_character_ else key, paddle = paddle,
                      cloud = gathered, igrf = if (!is.null(gathered)) .magIGRF(meta$deployment) else NULL,
                      keys = stats::setNames(lapply(c("package_id", "logger_id"), keyval), c("package_id", "logger_id")),
                      net = meta$axis_mapping$net)
    rm(x)
  }

  # ---- group: deployments sharing a non-blank key are pooled; blanks are their own group. A magnetic
  # paddle wheel is part of the ferromagnetic environment (and its de-noised cloud differs), so paddle
  # and non-paddle deployments of one package are NOT pooled together. ----
  has_mag <- vapply(info, function(z) !is.null(z$cloud), logical(1))
  grp_key <- vapply(seq_len(r$n), function(i)
    if (!has_mag[i]) paste0("__none__", i)
    else if (is.na(info[[i]]$key)) paste0("__tag__", info[[i]]$id, "__", i)
    else paste0("pkg:", info[[i]]$key, "|paddle:", info[[i]]$paddle), character(1))
  groups <- split(seq_len(r$n), grp_key)

  # ---- Pass 2: fit per group (default in-situ) OR from external calibration sources; write back below ----
  cals <- vector("list", r$n)              # per-deployment filled $mag_calibration
  n_cal <- 0L; n_high <- 0L; n_medium <- 0L; n_low <- 0L; n_nomag <- 0L
  src_skipped <- character(0)              # deployments where an external-source bind was not usable (reason)
  src_nogain  <- character(0)              # bound deployments whose source did not out-cover the in-situ cloud
  if (is.null(sources)) for (g in groups) {
    members <- g[has_mag[g]]
    if (!length(members)) next                              # no usable magnetometer in the group
    pooled <- length(members) > 1L
    cd <- lapply(members, function(i) info[[i]]$cloud)

    if (!pooled) {
      i <- members[[1]]
      ig <- info[[i]]$igrf
      tgt <- if (!is.null(control$target.field)) control$target.field
             else if (control$igrf.normalize && is.finite(ig$intensity)) ig$intensity else NA_real_
      cal <- .calFromControl(cd[[1]]$mag, control, target.radius = tgt, grav = cd[[1]]$grav, igrf.incl = ig$inclination)
      if (is.null(cal) || !isTRUE(cal$recommend_apply)) next   # abort gate (blob / 1-D arc): propose nothing
      cals[[i]] <- .fillMagCal(cal, source = "per_tag", group = info[[i]]$id, n = 1L)
    } else {
      # pool the per-deployment clouds, each centred + normalised to unit radius (common scale), fit the
      # shared soft-iron SHAPE, then compose a per-deployment (centre, scale) calibration from it.
      pre <- lapply(cd, function(z) {
        hi <- .hardIronOffset(z$mag[, 1], z$mag[, 2], z$mag[, 3], min.coverage = 0)
        if (is.null(hi) || !is.finite(hi$radius) || hi$radius <= 0) return(NULL)
        list(c = hi$offset, rad = hi$radius, N = sweep(z$mag, 2, hi$offset, "-") / hi$radius)
      })
      keep <- !vapply(pre, is.null, logical(1))
      if (sum(keep) < 2L) {                                    # not enough usable members -> per-tag each
        for (i in members) {
          ig <- info[[i]]$igrf
          tgt <- if (!is.null(control$target.field)) control$target.field
                 else if (control$igrf.normalize && is.finite(ig$intensity)) ig$intensity else NA_real_
          cal <- .calFromControl(info[[i]]$cloud$mag, control, target.radius = tgt, grav = info[[i]]$cloud$grav, igrf.incl = ig$inclination)
          if (!is.null(cal) && isTRUE(cal$recommend_apply)) cals[[i]] <- .fillMagCal(cal, source = "per_tag", group = info[[i]]$id, n = 1L)
        }
        next
      }
      members_k <- members[keep]; pre <- pre[keep]
      P <- do.call(rbind, lapply(pre, function(p) p$N))
      shape <- .calFromControl(P, control, target.radius = 1)   # unit sphere; QC on the pool
      if (is.null(shape)) next                               # pooled fit failed; members left uncalibrated
      grp_name <- info[[members_k[[1]]]]$key %||% info[[members_k[[1]]]]$id
      for (j in seq_along(members_k)) {
        i <- members_k[[j]]; p <- pre[[j]]; ig <- info[[i]]$igrf
        R <- if (!is.null(control$target.field)) control$target.field
             else if (control$igrf.normalize && is.finite(ig$intensity)) ig$intensity else p$rad
        center_i <- p$c + as.numeric(shape$center) * p$rad
        soft_i   <- shape$soft_iron * (R / p$rad)
        Mc <- .applyMagCal(info[[i]]$cloud$mag, center_i, soft_i)
        dipr <- .magDipResidual(Mc, info[[i]]$cloud$grav, ig$inclination)
        conf <- .magConfidence(shape$radcv, dipr$residual, shape$coverage_ok, control$radcv.max, control$igrf.residual.max,
                               status = shape$status, azimuth_span = shape$azimuth_span, azimuth.min = control$azimuth.min %||% 150)
        cals[[i]] <- .fillMagCal(list(center = center_i, soft_iron = soft_i, method_used = shape$method_used,
                                      coverage_ok = shape$coverage_ok, axis_span = shape$axis_span,
                                      radcv = shape$radcv, dip = dipr$dip, igrf_residual = dipr$residual,
                                      status = shape$status, perp_source = "data", confidence = conf),
                                 source = "per_package", group = grp_name, n = length(members_k))
      }
    }
  } else for (i in seq_len(r$n)) {
    # ---- external calibration sources: fit each deployment from its matched source(s). Targets are NOT
    # pooled with each other here - the source already supplies the coverage that target-pooling
    # compensated for; sources sharing a package are pooled inside .fitFromSource. ----
    if (!has_mag[i]) next
    matched <- .matchCalSource(info[[i]], sources, calibration.match)
    bind <- NULL; reason <- NULL
    if (!is.null(matched)) {
      res <- .fitFromSource(info[[i]], matched, control)
      if (isTRUE(res$reject)) reason <- res$reason
      else {
        bind <- .fillMagCal(res$cal, source = "external",
                            group = info[[i]]$keys[[matched$key]] %||% info[[i]]$id,
                            n = res$n, provenance = res$provenance)
        if (isTRUE(res$low_coverage_gain)) src_nogain <- c(src_nogain, info[[i]]$id)
      }
    } else reason <- "no matching calibration recording"
    if (!is.null(bind)) { cals[[i]] <- bind; next }

    # no usable source bind -> honour calibration.on.missing
    src_skipped <- c(src_skipped, sprintf("%s: %s", info[[i]]$id, reason))
    if (identical(calibration.on.missing, "error"))
      .abort(c("No usable external calibration for deployment {.val {info[[i]]$id}}: {reason}.",
               "i" = "Adjust {.arg calibration.data}, or set {.arg calibration.on.missing} to {.val fallback}/{.val skip}."))
    if (identical(calibration.on.missing, "skip")) next
    # fallback: the current in-situ behaviour (fit from the deployment's own cloud)
    ig  <- info[[i]]$igrf
    tgt <- if (!is.null(control$target.field)) control$target.field
           else if (control$igrf.normalize && is.finite(ig$intensity)) ig$intensity else NA_real_
    cal <- .calFromControl(info[[i]]$cloud$mag, control, target.radius = tgt, grav = info[[i]]$cloud$grav, igrf.incl = ig$inclination)
    if (!is.null(cal) && isTRUE(cal$recommend_apply)) cals[[i]] <- .fillMagCal(cal, source = "per_tag", group = info[[i]]$id, n = 1L)
  }

  # ---- write back + report ----
  for (i in seq_len(r$n)) {
    x <- r$get(i)
    if (!data.table::is.data.table(x)) x <- data.table::as.data.table(x)
    meta <- .getMeta(x)
    who <- info[[i]]$id
    .log_h2(lvl, sprintf("%s (%d/%d)", who, i, r$n))
    cal <- cals[[i]]
    if (is.null(cal)) {
      n_nomag <- n_nomag + 1L
      .log_skip(lvl, if (!has_mag[i]) "no usable magnetometer" else "calibration not estimated")
    } else {
      cal$axis_net <- meta$axis_mapping$net                 # stamp the frame this estimate was fit in
      meta$mag_calibration <- .packProposed(cal)            # store as the nested `proposed` block (applier fills the rest)
      meta <- .appendProcessing(meta, "calibrateMagnetometer", source = cal$source, group = cal$group,
                                method = cal$method, radcv = cal$radcv, igrf_residual = cal$igrf_residual,
                                confidence = cal$confidence, n_deployments = cal$n_deployments)
      x <- .restoreMeta(x, meta)
      n_cal <- n_cal + 1L
      if (identical(cal$confidence, "high"))   n_high   <- n_high + 1L
      if (identical(cal$confidence, "medium")) n_medium <- n_medium + 1L
      if (identical(cal$confidence, "low"))    n_low    <- n_low + 1L
      resid_txt <- if (is.finite(cal$igrf_residual)) sprintf(", IGRF dip resid %+.0f\u00b0", cal$igrf_residual) else ""
      .log_detail(lvl, sprintf("%s %s", cal$source, cal$method))                    # method on its own line
      .log_detail(lvl, sprintf("radCV %.2f%s \u00b7 confidence %s",                  # quality metrics beneath it
                               cal$radcv, resid_txt, cal$confidence))
    }
    if (make_plots) {                                                # deferred-draw payload/record
      cov_i <- if (!is.null(cal) && !is.null(info[[i]]$cloud))    # computed ONCE on the full cloud, then
        .magCalCoverage(sweep(info[[i]]$cloud$mag, 2, cal$center, "-")) else NA_real_   # shared by both
      summary_records[[i]] <- .magCalSummaryRecord(who, cal, cov_i)
      detail <- !is.null(cal) && (force.plots || r$n == 1L || cal$confidence %in% c("low", "medium"))
      if (detail)
        payloads[[length(payloads) + 1L]] <- .magCalPayload(who, info[[i]]$cloud, cal, info[[i]]$igrf,
                                                            info[[i]]$paddle, coverage = cov_i,
                                                            radcv.max = control$radcv.max, dip.max = control$igrf.residual.max)
    }
    saved[i] <- list(.saveOutput(x, r$ids[i], output.dir = output.dir,
                                 output.suffix = output.suffix, compress = compress))
    if (!is.null(saved[[i]])) .log_ok(lvl, paste0("saved ", basename(saved[[i]])))
    if (return.data) results[[i]] <- x
    .log_gap(lvl); rm(x)
  }

  # ---- diagnostic report: worst-first summary page, then a detail page per flagged deployment ----
  if (make_plots) {
    stats <- list(n = r$n, n_cal = n_cal, n_high = n_high, n_medium = n_medium, n_low = n_low, n_nomag = n_nomag)
    draw <- function(to.file = FALSE, unicode = TRUE) {
      .drawMagCalSummaryPage(Filter(Negate(is.null), summary_records), stats, use_unicode = unicode)
      for (pl in payloads) .plotMagCalIndividual(pl, use_unicode = unicode)
    }
    # base pdf() for the file (ASCII glyphs, so it stays portable and parseable by the page-count tests);
    # `unicode` is driven by the device, so the caller's screen still gets Unicode when it can render it.
    .renderToDevices(draw, plot = plot, plot.file = plot.file, width = 10, height = 7.5)
  }

  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, n_cal, " of ", r$n, " dataset", if (r$n != 1) "s", " calibrated")
    if (n_high > 0L)  .log_arrow(lvl, "high heading confidence: ", n_high)
    if (n_low > 0L)   .log_arrow(lvl, "low confidence (untrusted heading): ", n_low)
    if (n_nomag > 0L) .log_arrow(lvl, "no usable magnetometer: ", n_nomag)
    if (length(src_skipped)) {
      verb <- if (identical(calibration.on.missing, "skip")) "left uncalibrated" else "fell back to in-situ"
      .log_arrow(lvl, sprintf("external calibration not bound (%s): %d", verb, length(src_skipped)))
      if (lvl >= 2L) for (s in src_skipped) .log_skip(lvl, s)
    }
    if (length(src_nogain))
      .log_arrow(lvl, sprintf("external source no better-covered than in-situ (applied, flagged): %s",
                              paste(src_nogain, collapse = ", ")))
    if (!is.null(output.dir)) .log_arrow(lvl, "output: ", output.dir)
    .log_runtime(lvl, start.time)
  }
  .collectOutput(results, saved, return.data, r$ids)
}

#' Build the FLAT per-deployment calibration estimate from a .calibrateMag result. This flat object is
#' what the in-function diagnostic report consumes (unchanged); it is reshaped into the nested
#' `meta$mag_calibration$proposed` contract by .packProposed() only at storage time.
#' @keywords internal
#' @noRd
.fillMagCal <- function(cal, source, group, n, provenance = NULL) {
  list(
    source        = source,
    group         = as.character(group),
    center        = as.numeric(cal$center),
    soft_iron     = cal$soft_iron,
    method        = cal$method_used,
    radcv         = cal$radcv,
    igrf_residual = cal$igrf_residual,
    coverage_ok   = isTRUE(cal$coverage_ok),
    axis_span     = if (!is.null(cal$axis_span)) as.numeric(cal$axis_span) else rep(NA_real_, 3),
    confidence    = cal$confidence,
    fit_status    = cal$status %||% "calibrated_3d",         # calibrated_3d | calibrated_2d_fallback | calibrated_diagonal
    perp_source   = cal$perp_source %||% "data",             # data | igrf_pin | prior_midpoint
    n_deployments = as.integer(n),
    axis_net      = NULL,                        # stamped by the write-back loop before storage
    source_ids    = provenance$source_ids,
    context       = provenance$context       %||% NA_character_,
    center_delta  = provenance$center_delta  %||% NA_real_,
    center_source = provenance$center_source %||% NA_character_
  )
}

#' Reshape a flat calibration estimate (from .fillMagCal) into the nested meta$mag_calibration contract,
#' populating ONLY the `proposed` block. calibrateMagnetometer proposes; it never sets applied/status
#' (that is processTagData's job).
#' @keywords internal
#' @noRd
.packProposed <- function(est) {
  m <- .newMagCalibrationMeta()
  m$proposed <- list(
    params = list(center = as.numeric(est$center), soft_iron = est$soft_iron, axis_net = est$axis_net),
    qc     = list(confidence = est$confidence, coverage_ok = isTRUE(est$coverage_ok),
                  radcv = est$radcv, igrf_residual = est$igrf_residual,
                  axis_span = est$axis_span %||% rep(NA_real_, 3)),
    provenance = list(method = est$method, source = est$source, group = est$group,
                      fit_status = est$fit_status %||% "calibrated_3d", perp_source = est$perp_source %||% "data",
                      n_deployments = est$n_deployments, source_ids = est$source_ids,
                      context = est$context %||% NA_character_,
                      center_delta = est$center_delta %||% NA_real_,
                      center_source = est$center_source %||% NA_character_))
  m
}

#######################################################################################################
# Diagnostic report ###################################################################################
#######################################################################################################

#' Equal-area sphere-coverage fraction of a centred magnetometer cloud.
#'
#' Bins the sample directions into 12 azimuth x 6 cos-polar equal-area cells and returns the fraction
#' occupied. A horizontal swimmer samples a band, so this is well below 1 even for a usable calibration;
#' it is the intuitive "how much of the sphere did we see" number behind the confidence gate.
#' @keywords internal
#' @noRd
.magCalCoverage <- function(N) {
  rn <- sqrt(rowSums(N^2)); ok <- is.finite(rn) & rn > 0
  if (sum(ok) < 10L) return(NA_real_)
  u <- N[ok, , drop = FALSE] / rn[ok]
  az <- atan2(u[, 2], u[, 1]); zc <- pmin(1, pmax(-1, u[, 3]))
  ai <- pmin(12L, 1L + floor((az + pi) / (2 * pi) * 12))
  zi <- pmin(6L,  1L + floor((zc + 1) / 2 * 6))
  length(unique((ai - 1L) * 6L + zi)) / 72
}

#' One summary-table row per deployment (drawn worst-first on the report's first page).
#' @keywords internal
#' @noRd
.magCalSummaryRecord <- function(id, cal, coverage) {
  if (is.null(cal))
    return(list(id = id, confidence = "no mag", rank = 4L, source = NA_character_, method = NA_character_,
                coverage = NA_real_, radcv = NA_real_, dip_resid = NA_real_, n_dep = NA_integer_))
  rk <- switch(cal$confidence, low = 0L, medium = 1L, high = 2L, 3L)
  list(id = id, confidence = cal$confidence, rank = rk, source = cal$source, method = cal$method,
       coverage = coverage, radcv = cal$radcv, dip_resid = cal$igrf_residual,
       n_dep = cal$n_deployments)
}

#' Assemble the small per-deployment plotting payload from the raw cloud + its filled calibration.
#'
#' Thins the cloud to `max.pts`, applies the calibration, and precomputes everything the detail page
#' needs: the corrected cloud + its median radius, the per-sample geomagnetic dip (whose mean equals the
#' stored `.magDipResidual` dip, so the panel and the header agree by construction), coverage, and the QC.
#' @keywords internal
#' @noRd
.magCalPayload <- function(id, cloud, cal, igrf, paddle, coverage, radcv.max = 0.1, dip.max = 15,
                           max.pts = 3000L) {
  M <- cloud$mag; grav <- cloud$grav
  keep <- stats::complete.cases(M)
  M <- M[keep, , drop = FALSE]; if (!is.null(grav)) grav <- grav[keep, , drop = FALSE]
  if (nrow(M) > max.pts) {                                        # thin only the SCATTER; coverage is
    idx <- round(seq(1L, nrow(M), length.out = max.pts))          # passed in (computed once on the full
    M <- M[idx, , drop = FALSE]; if (!is.null(grav)) grav <- grav[idx, , drop = FALSE]   # cloud) so the
  }                                                               # detail page and summary row always agree
  Bc  <- .applyMagCal(M, cal$center, cal$soft_iron)
  rn  <- sqrt(rowSums(Bc^2))
  dipr <- .magDipResidual(Bc, grav, if (is.null(igrf)) NA_real_ else igrf$inclination)
  dips <- NULL
  if (!is.null(grav)) {
    mn <- .rowNorm3(Bc); gn <- .rowNorm3(grav); ok <- is.finite(mn) & is.finite(gn) & mn > 0 & gn > 0
    if (any(ok)) dips <- 90 - acos(pmax(-1, pmin(1, rowSums((Bc[ok, , drop = FALSE] / mn[ok]) *
                                                            (grav[ok, , drop = FALSE] / gn[ok]))))) * 180 / pi
  }
  list(id = id, raw = M, cal = Bc, radius = stats::median(rn, na.rm = TRUE),
       target = if (is.null(igrf)) NA_real_ else igrf$intensity,
       radcv = cal$radcv, coverage = coverage,
       coverage_ok = isTRUE(cal$coverage_ok), dips = dips, dip = dipr$dip, dip_resid = dipr$residual,
       igrf_incl = if (is.null(igrf)) NA_real_ else igrf$inclination,
       confidence = cal$confidence, method = cal$method, source = cal$source,
       group = cal$group, n_dep = cal$n_deployments, paddle = isTRUE(paddle),
       radcv_max = radcv.max, dip_max = dip.max)
}

#' Confidence colour used across the report (green/amber/red for high/medium/low).
#' @keywords internal
#' @noRd
.magConfColour <- function(conf) switch(as.character(conf), high = "#2e7d32", medium = "#e08a00",
                                        low = "#c62828", "grey55")

#' Report page 1: a worst-first table of every deployment's calibration verdict.
#' @keywords internal
#' @noRd
.drawMagCalSummaryPage <- function(records, stats, use_unicode = TRUE) {
  oldpar <- graphics::par(no.readonly = TRUE); on.exit(graphics::par(oldpar), add = TRUE)
  graphics::layout(1)
  if (length(records))                                             # worst confidence first, then least-covered
    records <- records[order(vapply(records, function(r) r$rank, numeric(1)),
                             vapply(records, function(r) if (is.finite(r$coverage)) r$coverage else Inf, numeric(1)))]
  ell   <- if (use_unicode) "\u2026" else ".."
  truncr <- function(s, n) if (is.na(s)) "-" else if (nchar(s) > n) paste0(substr(s, 1, n - 1L), ell) else s
  fnum   <- function(v, fmt) if (is.null(v) || !is.finite(v)) "-" else sprintf(fmt, v)

  graphics::par(mar = c(1.5, 0.6, 1, 0.6)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
  graphics::text(0, 0.985, "calibrateMagnetometer  -  run summary", adj = c(0, 1), font = 2, cex = 1.4)
  graphics::text(1, 0.985, format(Sys.time(), "%Y-%m-%d %H:%M"), adj = c(1, 1), cex = 0.75, col = "grey40")
  graphics::text(0, 0.935, sprintf("%d dataset%s  |  %d calibrated   %d high   %d medium   %d low   %d no mag",
                 stats$n, if (stats$n != 1) "s" else "", stats$n_cal, stats$n_high, stats$n_medium,
                 stats$n_low, stats$n_nomag), adj = c(0, 1), cex = 0.95)
  graphics::text(0, 0.905, "sorted worst-first  -  heading confidence: green = high, amber = medium, red = low",
                 adj = c(0, 1), cex = 0.72, col = "grey45")

  cx <- c(id = 0.004, source = 0.30, method = 0.44, cover = 0.66, radcv = 0.76, dip = 0.86, conf = 0.955)
  hcy <- 0.855
  graphics::rect(0, hcy - 0.014, 1, hcy + 0.014, col = "grey90", border = NA)
  hdr <- c(id = "Deployment", source = "Pooling", method = "Method", cover = "Coverage",
           radcv = "radCV", dip = "Dip res.", conf = "Confidence")
  adj <- c(id = 0, source = 0, method = 0, cover = 1, radcv = 1, dip = 1, conf = 1)
  for (k in names(cx)) graphics::text(cx[[k]], hcy, hdr[[k]], adj = c(adj[[k]], 0.5), font = 2, cex = 0.6, col = "grey15")
  graphics::segments(0, hcy - 0.016, 1, hcy - 0.016, col = "grey55")

  rpp <- 40L; npg <- max(1L, ceiling(max(1L, length(records)) / rpp))
  drawn <- FALSE
  for (pg in seq_len(npg)) {
    if (pg > 1L) {
      graphics::par(mar = c(1.5, 0.6, 1, 0.6)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
      graphics::rect(0, hcy - 0.014, 1, hcy + 0.014, col = "grey90", border = NA)
      for (k in names(cx)) graphics::text(cx[[k]], hcy, hdr[[k]], adj = c(adj[[k]], 0.5), font = 2, cex = 0.6, col = "grey15")
      graphics::segments(0, hcy - 0.016, 1, hcy - 0.016, col = "grey55")
    }
    idx <- if (!length(records)) integer(0) else ((pg - 1L) * rpp + 1L):min(pg * rpp, length(records))
    row_top <- hcy - 0.030; dy <- 0.0195
    for (j in seq_along(idx)) {
      r <- records[[idx[j]]]; y <- row_top - dy * (j - 1L); cc <- .magConfColour(r$confidence)
      if (j %% 2L == 0L) graphics::rect(0, y - dy / 2, 1, y + dy / 2, col = "grey96", border = NA)
      graphics::text(cx[["id"]], y, truncr(r$id, 18), adj = c(0, 0.5), cex = 0.6, font = 2, col = "grey20")
      pool <- if (is.na(r$source)) "-" else if (identical(r$source, "per_package"))
        sprintf("package (%d)", r$n_dep) else "per tag"
      graphics::text(cx[["source"]], y, pool, adj = c(0, 0.5), cex = 0.58, col = "grey35")
      graphics::text(cx[["method"]], y, truncr(r$method, 16), adj = c(0, 0.5), cex = 0.55, col = "grey35")
      graphics::text(cx[["cover"]], y, if (is.finite(r$coverage)) sprintf("%.0f%%", 100 * r$coverage) else "-",
                     adj = c(1, 0.5), cex = 0.58, col = "grey20")
      graphics::text(cx[["radcv"]], y, fnum(r$radcv, "%.3f"), adj = c(1, 0.5), cex = 0.58, col = "grey20")
      graphics::text(cx[["dip"]], y, if (is.finite(r$dip_resid)) sprintf("%+.0f", r$dip_resid) else "-",
                     adj = c(1, 0.5), cex = 0.58, col = "grey20")
      graphics::text(cx[["conf"]], y, r$confidence, adj = c(1, 0.5), cex = 0.6,
                     font = if (identical(r$confidence, "low")) 2 else 1, col = cc)
      drawn <- TRUE
    }
    if (!drawn) graphics::text(0.5, 0.5, "no deployments to report", cex = 1, col = "grey50")
    if (npg > 1) graphics::mtext(sprintf("summary page %d / %d", pg, npg), side = 1, cex = 0.7, col = "grey50")
  }
  invisible(NULL)
}

#' Plain-language interpretation for a detail page, naming the actual reason behind the confidence flag
#' (patchy coverage, a dip far from IGRF, or a non-spherical corrected cloud). Kept as its own function
#' so the wording is unit-testable and so a `"low"` verdict never inherits the softer `"medium"` phrasing.
#' @keywords internal
#' @noRd
.magCalMessage <- function(p) {
  # thresholds match .magConfidence's low cutoffs (2x the high limits), so each branch corresponds to the
  # actual reason a deployment was NOT high: too little coverage, a grossly non-spherical corrected cloud,
  # a dip far from IGRF, or (reaching the else) a medium fit that is merely marginal.
  rmax <- (p$radcv_max %||% 0.1) * 2; dmax <- (p$dip_max %||% 15) * 2
  rcv  <- p$radcv %||% NA_real_; dres <- p$dip_resid %||% NA_real_
  dip_sd <- if (!is.null(p$dips) && length(p$dips) > 10L) stats::sd(p$dips, na.rm = TRUE) else NA_real_
  msg <- if (identical(p$confidence, "high"))
           "Centred, spherical, and the dip matches\nthe IGRF field: heading is trustworthy."
         else if (!isTRUE(p$coverage_ok))
           "The fit spans only a patch of the sphere,\nso the shape is under-constrained: heading\nis untrustworthy despite a tight local fit."
         else if (is.finite(rcv) && rcv > rmax)
           sprintf("The corrected cloud is not spherical enough\n(radius CV %.2f): the recovered field is\nunreliable - do not trust the heading.", rcv)
         else if (is.finite(dres) && abs(dres) > dmax)
           "The sphere fit is tight, but the dip is\nfar from the IGRF field: something is\nphysically off (check the mag axis mapping)."
         else "Fit is usable but not clean: treat heading\nwith caution."
  # a wide per-sample dip spread is a caveat, not proof of two modes; never contradict a high verdict
  if (is.finite(dip_sd) && dip_sd > 25 && !identical(p$confidence, "high"))
    msg <- paste0(msg, sprintf("\n\nThe per-sample dip is widely scattered\n(sd %.0f deg); if it is clearly two-peaked\nabove, suspect a magnetometer frame problem.", dip_sd))
  msg
}

#' Report detail page for one deployment: raw vs corrected sphere projections, corrected-field magnitude
#' and geomagnetic-dip histograms, and the confidence verdict. The three panels map onto the three
#' independent trust questions - centred + spherical? tightly on a sphere? physically consistent with
#' the IGRF field? - so the reason for a low-confidence flag is visible, not just asserted.
#' @keywords internal
#' @noRd
.plotMagCalIndividual <- function(p, use_unicode = TRUE) {
  oldpar <- graphics::par(no.readonly = TRUE)
  on.exit({ graphics::layout(1); graphics::par(oldpar) }, add = TRUE)
  BG <- "grey97"; GRID <- "grey88"; RAWC <- "grey58"; CALC <- "#1565c0"; REFC <- "#c62828"; DIPC <- "#6a1b9a"
  cc <- .magConfColour(p$confidence)

  graphics::layout(matrix(c(1, 1, 1, 1,  2, 3, 4, 5,  6, 7, 8, 9,  10, 11, 12, 12),
                          nrow = 4L, byrow = TRUE), heights = c(0.6, 1, 1, 1.05))

  pj <- function(P, i, j, lab, ttl, col, center0 = FALSE, R = NA) {
    rng <- range(c(P[, i], P[, j], if (center0) c(-R, R)), na.rm = TRUE)
    if (!all(is.finite(rng))) rng <- c(-1, 1)
    pad <- max(diff(rng) * 0.08, 1e-6)
    graphics::par(mar = c(3.4, 3.4, 2.0, 0.8), mgp = c(2.0, 0.6, 0), cex.axis = 0.8)
    graphics::plot(NA, xlim = rng + c(-pad, pad), ylim = rng + c(-pad, pad), asp = 1,
                   xlab = lab[1], ylab = lab[2], main = ttl, cex.main = 0.98, font.main = 1,
                   panel.first = {
                     u <- graphics::par("usr")
                     graphics::rect(u[1], u[3], u[2], u[4], col = BG, border = NA)
                     graphics::grid(col = GRID, lty = 1)
                   })
    if (center0) {
      graphics::abline(h = 0, v = 0, col = GRID)
      th <- seq(0, 2 * pi, length.out = 200)
      graphics::lines(R * cos(th), R * sin(th), col = REFC, lwd = 1.8)
    }
    graphics::points(P[, i], P[, j], pch = 16, cex = 0.35, col = grDevices::adjustcolor(col, 0.5))
  }

  # --- header + verdict chip ---
  graphics::par(mar = c(0.2, 1, 0.4, 1)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
  # title hierarchy: animal id (matches page B), then the section heading, then the method details
  graphics::text(0, 0.82, p$id, adj = c(0, 0.5), cex = 1.5, font = 2)
  graphics::text(0, 0.52, "Magnetometer calibration", adj = c(0, 0.5), cex = 1.02, font = 2, col = "grey20")
  pool <- if (identical(p$source, "per_package")) sprintf("pooled over package (%d deployments)", p$n_dep) else "per-tag fit"
  graphics::text(0, 0.22, sprintf("%s   |   method: %s%s", pool, p$method,
                 if (p$paddle) "   |   paddle wheel" else ""), adj = c(0, 0.5), cex = 0.85, col = "grey35")
  graphics::rect(0.63, 0.12, 1.0, 0.96, col = grDevices::adjustcolor(cc, 0.13), border = cc, lwd = 2)
  graphics::text(0.815, 0.68, sprintf("heading confidence:  %s", toupper(p$confidence)), cex = 1.15, font = 2, col = cc)
  cov_txt <- if (is.finite(p$coverage)) sprintf("coverage %.0f%%", 100 * p$coverage) else "coverage n/a"
  dip_txt <- if (is.finite(p$dip_resid)) sprintf("dip res %+.0f deg", p$dip_resid) else "dip n/a"
  graphics::text(0.815, 0.30, sprintf("%s   -   radCV %s   -   %s", cov_txt,
                 if (is.finite(p$radcv)) sprintf("%.3f", p$radcv) else "n/a", dip_txt),
                 cex = 0.82, col = "grey25")

  # --- row 1: raw projections + legend ---
  pj(p$raw, 1, 2, c("mx", "my"), "raw  X-Y", RAWC)
  pj(p$raw, 1, 3, c("mx", "mz"), "raw  X-Z", RAWC)
  pj(p$raw, 2, 3, c("my", "mz"), "raw  Y-Z", RAWC)
  graphics::par(mar = c(1, 0.5, 2, 0.5)); graphics::plot.new()
  graphics::legend("center", bty = "n", pch = 16, col = c(RAWC, CALC, REFC), pt.cex = 1.2, cex = 1.0,
                   legend = c("raw cloud", "calibrated", "median |B|"), title = "before / after")

  # --- row 2: calibrated projections + coverage note ---
  R <- if (is.finite(p$radius)) p$radius else 1
  pj(p$cal, 1, 2, c("Bx", "By"), "calibrated  X-Y", CALC, center0 = TRUE, R = R)
  pj(p$cal, 1, 3, c("Bx", "Bz"), "calibrated  X-Z", CALC, center0 = TRUE, R = R)
  pj(p$cal, 2, 3, c("By", "Bz"), "calibrated  Y-Z", CALC, center0 = TRUE, R = R)
  graphics::par(mar = c(1, 0.5, 2, 0.5)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
  covcol <- if (isTRUE(p$coverage_ok)) "#2e7d32" else "#c62828"
  graphics::text(0.5, 0.66, if (is.finite(p$coverage)) sprintf("sphere coverage\n%.0f%%", 100 * p$coverage) else "coverage\nn/a",
                 cex = 1.25, font = 2, col = covcol)
  graphics::text(0.5, 0.24, if (isTRUE(p$coverage_ok)) "adequately sampled" else "under-sampled\n(a band, not a sphere)",
                 cex = 0.9, col = "grey30")

  # --- row 3: magnitude histogram | dip histogram | interpretation ---
  graphics::par(mar = c(3.8, 3.6, 3.0, 1), mgp = c(2.2, 0.7, 0), cex.axis = 0.8)
  rn <- sqrt(rowSums(p$cal^2))
  h <- graphics::hist(rn, breaks = 40, plot = FALSE)
  graphics::plot(h, col = grDevices::adjustcolor(CALC, 0.5), border = NA, main = "corrected field magnitude",
                 cex.main = 1.0, font.main = 1, xlab = "|B| (uT)", ylab = "count")
  graphics::abline(v = R, col = REFC, lwd = 2)
  if (is.finite(p$target)) graphics::abline(v = p$target, col = "grey40", lwd = 1.4, lty = 2)
  graphics::legend("topright", bty = "n", cex = 0.8, lwd = c(2, 1.4), lty = c(1, 2), col = c(REFC, "grey40"),
                   legend = c(sprintf("median %.1f", R), if (is.finite(p$target)) sprintf("IGRF %.1f", p$target) else "IGRF n/a"))
  graphics::mtext(sprintf("radCV %s  -  tight = on a sphere",
                          if (is.finite(p$radcv)) sprintf("%.3f", p$radcv) else "n/a"), side = 3, line = 0.1, cex = 0.68, col = "grey35")

  if (!is.null(p$dips) && length(p$dips) > 10L) {
    graphics::hist(p$dips, breaks = 40, col = grDevices::adjustcolor(DIPC, 0.45), border = NA,
                   main = "geomagnetic dip (inclination)", cex.main = 1.0, font.main = 1,
                   xlab = "dip angle (deg)", ylab = "count")
    if (is.finite(p$igrf_incl)) graphics::abline(v = p$igrf_incl, col = REFC, lwd = 2)
    if (is.finite(p$dip)) graphics::abline(v = p$dip, col = DIPC, lwd = 2, lty = 2)
    graphics::legend("topright", bty = "n", cex = 0.8, lwd = 2, lty = c(1, 2), col = c(REFC, DIPC),
                     legend = c(if (is.finite(p$igrf_incl)) sprintf("IGRF %.0f", p$igrf_incl) else "IGRF n/a",
                                if (is.finite(p$dip)) sprintf("measured %.0f", p$dip) else "measured n/a"))
    graphics::mtext(sprintf("residual %s deg  -  independent physical check",
                            if (is.finite(p$dip_resid)) sprintf("%+.0f", p$dip_resid) else "n/a"),
                    side = 3, line = 0.1, cex = 0.68, col = "grey35")
  } else {
    graphics::par(mar = c(3.8, 1, 3.0, 1)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
    graphics::text(0.5, 0.5, "dip check unavailable\n(no gravity reference)", cex = 0.95, col = "grey45")
  }

  # interpretation
  graphics::par(mar = c(2, 1, 3.0, 1)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
  graphics::text(0.02, 0.9, "interpretation", adj = c(0, 1), font = 2, cex = 0.85, col = "grey30")
  graphics::text(0.02, 0.68, .magCalMessage(p), adj = c(0, 1), cex = 0.92, col = "grey20")
  invisible(NULL)
}
