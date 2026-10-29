#' Fit and confidence thresholds for calibrateMagnetometer()
#'
#' @description
#' Groups the tuning of [calibrateMagnetometer()] into one validated object, so the main call stays
#' uncluttered.
#'
#' Calibrating a magnetometer from free-swimming data is under-determined: a near-horizontal swimmer
#' sweeps a band of orientations rather than a sphere, so part of the correction is never observed.
#' These thresholds decide how much evidence a fit must show before it is accepted, and how much of the
#' result may be trusted. They are set conservatively, on the principle that an honest low-confidence
#' verdict is more useful than an optimistic correction.
#'
#' @details
#' The thresholds map onto the stages of the fit, which are described in the Details of
#' [calibrateMagnetometer()].
#'
#' Accepting the full three-dimensional ellipsoid rests on `cond.max`, which bounds how elongated an
#' ellipsoid may be and still be believed; `igrf.residual.max`, the dip tolerance; `radcv.max`, the
#' sphericity tolerance for high confidence; and `min.coverage`, which sets when a cloud counts as well
#' covered on every axis.
#'
#' For the hard-iron-only fallback used on a thin swimming band, `azimuth.min` is the swept yaw coverage
#' needed to trust the in-plane centre, while `planarity.max`, `linearity.abort` and `extent.min` reject
#' clouds that are not a genuine planar ring - a solid blob, or a single heading held throughout.
#'
#' \code{center.warn} and \code{center.reject} apply to non-paddle deployments when the fit comes
#' from an external \code{calibration.data} source. Paddle deployments use their own in-situ centre
#' where available.
#'
#' @param method How to fit the distortion. `"ellipsoid"` (default) fits the full hard-iron and
#'   soft-iron ellipsoid where the data genuinely determine it, and otherwise falls back to a
#'   constrained hard-iron fit. \code{"diagonal"} requests a per-axis offset-and-scale fit.
#'   Neither method establishes identifiability when orientation coverage is inadequate.
#' @param igrf.normalize Logical; whether to normalise the proposed corrected field to the expected
#'   geomagnetic intensity when a reference is available (default \code{TRUE}). Applies to individual,
#'   pooled and external-source fits. A supplied \code{target.field} takes precedence; otherwise,
#'   without a reference, the native centred field magnitude is retained. Pooled clouds are normalised
#'   to a common radius independently of this setting.
#' @param min.coverage Minimum per-axis coverage, as a fraction of the sphere radius, for the fit to be
#'   trusted (default `0.5`). Below this the animal did not turn through enough orientations. Lower it
#'   only if you are prepared to accept a fit resting on a narrow slice of the sphere.
#' @param cond.max Maximum ratio of the largest to smallest eigenvalues of the fitted ellipsoid's
#'   quadratic-form matrix (default \code{25}), equivalent to the squared ratio of its longest and
#'   shortest axes. An ill-conditioned fit is routed to the constrained hard-iron fallback, not the
#'   diagonal method.
#' @param radcv.max Maximum corrected-radius dispersion for high confidence (default \code{0.1}).
#'   Dispersion is calculated as the standard deviation divided by the median radius, not its mean.
#'   Coverage and available inclination diagnostics also enter the confidence assessment.
#' @param igrf.residual.max Largest absolute dip residual, in degrees, at or below which a full
#'   three-dimensional fit earns high confidence (default `15`). Dip residual is the measured
#'   geomagnetic inclination minus the value expected at that place. It also gates soft-iron acceptance:
#'   a thin-band ellipsoid whose corrected field misses the expected dip by more than this has an
#'   unconstrained perpendicular centre, so it is routed to the hard-iron-only fallback rather than
#'   applying a soft iron that cannot be trusted. It does not gate the fallback's own heading trust,
#'   which rests on in-plane yaw coverage, because heading needs the horizontal components and not the
#'   vertical dip.
#' @param center.warn,center.reject How closely the hard-iron centre estimated from an external source
#'   must agree with the deployment's own in-situ centre, as a fraction of the field radius. Used only
#'   for non-paddle deployments when a calibration is fitted from `calibration.data`. A disagreement
#'   above \code{center.reject} (default \code{0.35}) rejects the source; above \code{center.warn}
#'   (default \code{0.10}) confidence is capped at medium. This cross-check exists because a fixed
#'   magnetic mass that co-rotated with the tag during the calibration spin is absorbed into the centre and
#'   still passes every sphericity and dip test, so nothing internal to the recording can reveal it.
#'   `center.reject` must be at least `center.warn`.
#' @param azimuth.min Minimum swept yaw arc, in degrees, for a hard-iron-only fit to earn medium heading
#'   confidence (default `150`). Below this the animal did not turn through enough headings to place the
#'   in-plane centre, and the heading is left uncalibrated.
#' @param planarity.max Maximum planarity of the field cloud for a fallback fit to be applied (default
#'   `0.6`). A genuine swimming band is close to planar; a near-stationary cloud approaches an isotropic
#'   blob, whose apparently full azimuth coverage is only sensor noise, and is rejected.
#' @param linearity.abort Minimum linearity below which the cloud has collapsed to a one-dimensional arc,
#'   meaning a single heading was held, so the in-plane centre is unobservable and no fit is applied.
#'   Default `0.1`.
#' @param extent.min Minimum angular extent of the cloud about its centre, in degrees, below which it is
#'   a stationary blob and no fit is applied. Default `40`.
#' @param target.field Optional positive target field magnitude, in \eqn{\mu}T, overriding
#'   \code{igrf.normalize}. With \code{NULL} (default), the expected geomagnetic intensity is used
#'   when \code{igrf.normalize = TRUE} and a reference is available; otherwise the native centred
#'   field magnitude is retained.
#'
#' @return A validated `nautilus_mag_calibration` object for the `control` argument of
#'   [calibrateMagnetometer()].
#'
#' @seealso [calibrateMagnetometer()] for the function that consumes it; [calibrationControl()] for
#'   whether the resulting estimate is applied.
#'
#' @examples
#' magCalibrationControl(method = "diagonal")
#' @export
magCalibrationControl <- function(method = c("ellipsoid", "diagonal"),
                                  igrf.normalize = TRUE,
                                  min.coverage = 0.5,
                                  cond.max = 25,
                                  radcv.max = 0.1,
                                  igrf.residual.max = 15,
                                  center.warn = 0.10,
                                  center.reject = 0.35,
                                  planarity.max = 0.6,
                                  azimuth.min = 150,
                                  linearity.abort = 0.1,
                                  extent.min = 40,
                                  target.field = NULL) {
  method <- match.arg(method)
  .assert_flag(igrf.normalize, "control$igrf.normalize")
  .assert_number(min.coverage, "control$min.coverage", min = 0, max = 1)
  .assert_number(cond.max, "control$cond.max", min = 1)
  .assert_number(radcv.max, "control$radcv.max", min = 0)
  .assert_number(igrf.residual.max, "control$igrf.residual.max", min = 0)
  .assert_number(center.warn, "control$center.warn", min = 0)
  .assert_number(center.reject, "control$center.reject", min = 0)
  if (center.reject < center.warn)
    .abort("{.arg control$center.reject} ({center.reject}) must be >= {.arg control$center.warn} ({center.warn}).")
  .assert_number(planarity.max, "control$planarity.max", min = 0, max = 1)
  .assert_number(azimuth.min, "control$azimuth.min", min = 0, max = 360)
  .assert_number(linearity.abort, "control$linearity.abort", min = 0, max = 1)
  .assert_number(extent.min, "control$extent.min", min = 0, max = 180)
  if (!is.null(target.field)) .assert_number(target.field, "control$target.field", min = 0)
  structure(list(method = method, igrf.normalize = igrf.normalize, min.coverage = min.coverage,
                 cond.max = cond.max, radcv.max = radcv.max, igrf.residual.max = igrf.residual.max,
                 center.warn = center.warn, center.reject = center.reject,
                 planarity.max = planarity.max, azimuth.min = azimuth.min,
                 linearity.abort = linearity.abort, extent.min = extent.min,
                 target.field = target.field),
            class = "nautilus_mag_calibration")
}
