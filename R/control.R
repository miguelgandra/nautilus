#######################################################################################################
# Control objects for processTagData() ################################################################
#######################################################################################################

#' Smoothing windows for processTagData()
#'
#' @description
#' Groups the smoothing-window arguments of \code{\link{processTagData}} into one object, so the main
#' call stays uncluttered. Each value is a window length in seconds; set any to `NULL` to disable that
#' smoothing.
#'
#' @param static Window (s) setting the static/gravity separation: the acceleration is split into a
#'   static (gravity/posture) and a dynamic (motion) part with a zero-phase Butterworth high-pass whose
#'   -3 dB cutoff is the equivalent of this window (~`0.76 / static` Hz; the default 3 s gives ~0.25 Hz).
#'   The dynamic part underlies VeDBA/ODBA and surge/sway/heave, the static part the orientation
#'   pitch/roll -- so this is not a cosmetic post-smoother: it defines what counts as "dynamic", must be
#'   `> 0`, and cannot be disabled. Default 3.
#' @param orientation Window (s) for orientation metrics (roll, pitch, heading; circular mean). Default 1.
#' @param dba Window (s) for VeDBA/ODBA smoothing, applied as a zero-phase Butterworth low-pass whose
#'   -3 dB cutoff is the equivalent of this window (~`0.44 / dba` Hz; the default 2 s gives ~0.22 Hz).
#'   `NULL` disables the smoothing. Default 2.
#' @param depth Window (s) used to condition the depth series that vertical velocity is
#'   differentiated from. Default 10. This does NOT smooth the stored `depth` channel, which is
#'   kept drift-corrected but unsmoothed - a centred boxcar attenuates any excursion shorter than
#'   its window, which would shrink short dives (a 3 m / 8 s dive reads 1.2 m at the 10 s default).
#' @param vertical Window (s) applied to the vertical velocity derived from depth. Default 1.
#'   Paddle-wheel speed is not smoothed here: that window belongs to
#'   \code{\link{calculatePaddleSpeed}}, which turns the recorded paddle frequency into a speed.
#' @return A validated `nautilus_smoothing` object for the `smoothing` argument of \code{\link{processTagData}}.
#' @seealso \code{\link{processTagData}}, \code{\link{calibrationControl}}
#' @examples
#' smoothingControl(depth = 15, dba = NULL)   # 15 s depth window; disable DBA post-smoothing
#' @export
smoothingControl <- function(static = 3, orientation = 1, dba = 2, depth = 10, vertical = 1) {
  fields <- list(static = static, orientation = orientation, dba = dba, depth = depth, vertical = vertical)
  for (nm in names(fields)) if (!is.null(fields[[nm]])) .assert_number(fields[[nm]], paste0("smoothing$", nm), min = 0)
  if (is.null(fields$static) || fields$static <= 0)
    .abort("{.arg smoothing$static} must be a positive number (the gravity-separation window cannot be disabled).")
  structure(fields, class = "nautilus_smoothing")
}


#' Paddle-wheel frequency estimation settings for processTagData()
#'
#' @description
#' Groups the settings used by [processTagData()] to recover paddle-wheel rotation frequency from the
#' magnetometer. The estimator works in overlapping windows, reports a frequency only when the strongest
#' in-band component is an interior spectral peak, and requires that peak to stand above the spectral
#' background. This prevents broadband noise and sampling-boundary artefacts from becoming precise-looking
#' rotation rates.
#'
#' @param window.size Length of each spectral window in seconds (default `5`). Longer windows improve
#'   frequency resolution but follow changes in rotation rate more slowly.
#' @param step.size Time between successive estimates in seconds (default `1`). It must not exceed
#'   `window.size`.
#' @param min.freq.Hz Lower frequency searched, in hertz (default `0.1`). It must be positive.
#' @param max.freq.Hz Optional upper frequency searched, in hertz. `NULL` (default) uses the Nyquist-safe
#'   limit set by `nyquist.guard`; an explicit value can narrow the band but cannot bypass that guard.
#' @param nyquist.guard Fraction of the Nyquist frequency retained as a safe analysis band (default
#'   `0.9`). It must lie strictly between zero and one. Power whose dominant peak lies above this limit
#'   is treated as a sampling-boundary artefact rather than reassigned to a lower-frequency peak. This
#'   guard rejects boundary peaks but cannot recover frequencies that were already aliased during
#'   recording; the sensor sampling rate must still be high enough for the physical rotation range.
#' @param min.prominence Minimum ratio of peak power to median in-band background power (default `20`).
#'   A value of zero disables this quality gate, but the interior-peak and Nyquist guards still apply.
#' @param max.interp.gap Longest run of rejected estimates, in seconds, that may be filled between two
#'   accepted estimates (default `2`). `NULL` or zero leaves every rejected window missing. Leading and
#'   trailing gaps are never extrapolated.
#'
#' @return A validated `nautilus_paddle_frequency` object for the `paddle` argument of
#'   [processTagData()].
#'
#' @seealso [processTagData()] for frequency extraction and [calculatePaddleSpeed()] for the separate
#'   calibration step that converts frequency to speed.
#'
#' @examples
#' paddleFrequencyControl(max.freq.Hz = 35, min.prominence = 15)
#' paddleFrequencyControl(max.interp.gap = NULL)  # retain every rejected interval as missing
#' @export
paddleFrequencyControl <- function(window.size = 5,
                                   step.size = 1,
                                   min.freq.Hz = 0.1,
                                   max.freq.Hz = NULL,
                                   nyquist.guard = 0.9,
                                   min.prominence = 20,
                                   max.interp.gap = 2) {
  .assert_number(window.size, "paddle$window.size", min = 0)
  .assert_number(step.size, "paddle$step.size", min = 0)
  .assert_number(min.freq.Hz, "paddle$min.freq.Hz", min = 0)
  .assert_number(max.freq.Hz, "paddle$max.freq.Hz", min = 0, null_ok = TRUE)
  .assert_number(nyquist.guard, "paddle$nyquist.guard", min = 0)
  .assert_number(min.prominence, "paddle$min.prominence", min = 0)
  .assert_number(max.interp.gap, "paddle$max.interp.gap", min = 0, null_ok = TRUE)

  if (window.size <= 0) .abort("{.arg paddle$window.size} must be greater than zero.")
  if (step.size <= 0) .abort("{.arg paddle$step.size} must be greater than zero.")
  if (step.size > window.size)
    .abort("{.arg paddle$step.size} must not exceed {.arg paddle$window.size}.")
  if (min.freq.Hz <= 0) .abort("{.arg paddle$min.freq.Hz} must be greater than zero.")
  if (!is.null(max.freq.Hz) && max.freq.Hz <= min.freq.Hz)
    .abort("{.arg paddle$max.freq.Hz} must be greater than {.arg paddle$min.freq.Hz}.")
  if (nyquist.guard <= 0 || nyquist.guard >= 1)
    .abort("{.arg paddle$nyquist.guard} must lie strictly between zero and one.")

  structure(list(window.size = window.size, step.size = step.size,
                 min.freq.Hz = min.freq.Hz, max.freq.Hz = max.freq.Hz,
                 nyquist.guard = nyquist.guard, min.prominence = min.prominence,
                 max.interp.gap = max.interp.gap),
            class = "nautilus_paddle_frequency")
}

#' Magnetometer-calibration switches for processTagData()
#'
#' @description
#' Groups the magnetometer-calibration switches of [processTagData()] into one object. These decide
#' whether the magnetometer is corrected for the tag's own iron before heading is computed, and whether
#' a calibration already estimated by [calibrateMagnetometer()] is used in preference to one fitted on
#' the spot. The mounting offset corrections and the orientation-estimator tuning live in
#' [orientationControl()].
#'
#' @param hard.iron Logical; whether to apply a supported hard-iron centre correction
#'   (default \code{TRUE}). This subtracts the additive magnetic offset.
#' @param soft.iron Logical; whether to apply a supported soft-iron matrix
#'   (default \code{TRUE}). Full ellipsoid fits can include cross-axis terms; a constrained planar
#'   fallback does not estimate directional distortion. Confidence gates still apply.
#' @param use.stored Whether to prefer a calibration already stored in the metadata by
#'   [calibrateMagnetometer()], such as a fit pooled across every deployment of one tag (default
#'   `TRUE`). Reuse requires both correction switches, high or medium confidence, a finite centre,
#'   a stored matrix and matching axis metadata. Otherwise an inline per-deployment estimate is
#'   considered and is itself confidence-gated. Set \code{FALSE} to bypass stored proposals.
#'   A calibration already marked as applied is not applied or estimated again.
#'
#' @return A validated `nautilus_calibration` object for the `calibration` argument of
#'   [processTagData()].
#'
#' @seealso [processTagData()] for the function that consumes it; [calibrateMagnetometer()] for the
#'   stored calibration it can draw on; [orientationControl()] and [smoothingControl()] for the other
#'   processing settings.
#'
#' @examples
#' calibrationControl(soft.iron = FALSE)
#' @export
calibrationControl <- function(hard.iron = TRUE, soft.iron = TRUE, use.stored = TRUE) {
  flags <- list(hard.iron = hard.iron, soft.iron = soft.iron, use.stored = use.stored)
  for (nm in names(flags)) .assert_flag(flags[[nm]], paste0("calibration$", nm))
  structure(flags, class = "nautilus_calibration")
}


#' Tile-fetch settings for a satellite basemap
#'
#' @description
#' Tunes how satellite imagery is fetched and cached when [plotTracks()] or [filterLocations()] is asked
#' for `basemap = "satellite"`.
#'
#' It governs the automatic fetch only. A raster you pre-fetched yourself with [getBasemap()] and passed
#' in as `basemap` is drawn as given, and ignores everything here.
#'
#' @param provider Which tile provider to draw from. Default `"Esri.WorldImagery"`, which is satellite
#'   imagery; any provider \pkg{maptiles} knows will work, such as `"Esri.WorldTopoMap"` for a
#'   topographic canvas or `"OpenStreetMap"` for a street map. Providers differ in coverage and licence,
#'   so check the terms before publishing a figure drawn on one.
#' @param cache Whether to keep fetched tiles for reuse: `TRUE` (default) uses a persistent per-user
#'   cache, `FALSE` keeps them only for the session, and a directory path caches them there. Leave it on
#'   unless disk space is tight - it makes a redrawn figure instant and works offline.
#'
#' @details
#' Tile zoom and grid resolution are deliberately not exposed. Both are derived from the map extent, and
#' where you genuinely need exact control the better route is to pre-fetch a raster with [getBasemap()]
#' and pass it in, which also makes the figure reproducible.
#'
#' @return A validated `nautilus_basemap` object for the `basemap.control` argument of [plotTracks()] and
#'   [filterLocations()].
#'
#' @seealso [plotTracks()] and [filterLocations()] for the functions that consume it; [getBasemap()] for
#'   pre-fetching a raster instead.
#'
#' @examples
#' basemapControl(provider = "Esri.WorldTopoMap")
#' @export
#' @param zoom Tile zoom level, or `NULL` (default) to choose one from the extent and the size the
#'   basemap will be drawn at. The automatic choice targets roughly 1200 pixels across the panel -
#'   about 280 dpi in a [plotTracks()] map - bounded by a tile budget so a wide extent cannot silently
#'   issue thousands of requests; when the budget binds rather than the target, the detailed log says so.
#'
#'   This exists because the tile library's own default optimises for a cheap request, not a figure: it
#'   takes the largest zoom that still covers the area in four tiles. On a 54 x 68 km extent that is
#'   zoom 9 - 244 m per pixel, about 60 dpi on the page - which renders islands as blurred blobs.
#'
#'   Higher is not always better. A provider's imagery runs out at some zoom and it then serves empty
#'   tiles rather than an error: `Esri.WorldImagery` over the open Atlantic is complete to zoom 11 and
#'   about 5% black at zoom 12, where the coastline gains detail but the ocean loses it. An automatic
#'   choice is therefore checked for coverage and steps back down if the imagery is missing. A zoom you
#'   set explicitly is treated as a decision and is used as given.
basemapControl <- function(provider = "Esri.WorldImagery", cache = TRUE, zoom = NULL) {
  .assert_string(provider, "provider")
  .assert_count(zoom, "zoom", min = 0, null_ok = TRUE)
  if (!is.null(zoom) && zoom > 19)
    .abort("{.arg zoom} ({zoom}) is above the maximum tile zoom (19).")
  if (!(isTRUE(cache) || isFALSE(cache) || (is.character(cache) && length(cache) == 1L && !is.na(cache))))
    .abort("{.arg cache} must be {.code TRUE}, {.code FALSE}, or a single directory path.")
  structure(list(provider = provider, cache = cache, zoom = zoom), class = "nautilus_basemap")
}

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


#' Orientation-estimation tuning for processTagData()
#'
#' @description
#' Groups the specialised orientation settings of [processTagData()] into one object, leaving the
#' primary choice - the estimator itself - as that function's top-level `orientation.algorithm`
#' argument.
#'
#' Two distinct things are tuned here. The first is the estimator: how much the Madgwick filter trusts
#' the accelerometer against the gyroscope, and how paddle-wheel contamination is removed before heading
#' is computed. The second is the mounting geometry: a tag is never attached perfectly level, and the
#' resulting constant pitch and roll offsets are indistinguishable from the animal's own posture unless
#' they are estimated and removed.
#'
#' @param madgwick.beta The Madgwick filter's gain, which sets how much it trusts the accelerometer
#'   against the gyroscope (default `0.02`). Raise it if the estimated orientation drifts over long
#'   stretches; lower it if the orientation is jittery during vigorous swimming. Used only when
#'   `orientation.algorithm = "madgwick"`.
#' @param correct.pitch Whether to estimate and subtract the mounting pitch offset (default `TRUE`),
#'   taken from where the pitch-against-vertical-velocity relationship crosses zero. Disable it if you
#'   know the tag was mounted level and want the raw posture.
#' @param correct.roll Whether to estimate and subtract the mounting roll offset (default `TRUE`), taken
#'   as the median roll over level swimming.
#' @param pitch.offset.min.r2 Minimum R-squared of the pitch-against-vertical-velocity fit required
#'   before the pitch offset is subtracted (default `0.1`). Below this the fitted offset is really just
#'   the mean pitch, and subtracting it would strip out genuine posture, so the correction is skipped.
#' @param mount.roll.max Largest mounting roll offset, in degrees, that `correct.roll` will still
#'   subtract (default `60`). This is a plausibility gate rather than an alarm: beyond it, a large
#'   estimate more likely means the body frame is wrong than that the tag was clamped that far round, so
#'   the offset is recorded but left in place. It is deliberately wider than `warning.threshold`, because
#'   a steeply rolled clamp is a real mounting geometry - a left-side and a right-side attachment are
#'   mirror images - and such a deployment should be both corrected and flagged. Lower it to be stricter
#'   about what may be absorbed into the mount. Roll only: a large pitch offset would mean the tag points
#'   along the body rather than across it, which is not a normal mounting geometry, so the pitch
#'   correction stays capped by `warning.threshold`.
#' @param warning.threshold Threshold in degrees above which an orientation warning is raised (default
#'   `45`), for three independent checks: an unusual median absolute pitch, an unusual estimated mounting
#'   roll, raised whether or not the correction was applied, and an unusual median absolute roll left
#'   over after correction. It also caps the pitch offset correction. Lower it, to `35` say, to hear
#'   about moderately rolled mounts as well; it changes what is reported, not what is corrected.
#' @param heading.denoise How to suppress paddle-wheel contamination of the magnetometer before heading
#'   is computed. A spinning paddle magnet adds a large, fast oscillation to the field; because it adds
#'   to the field vector and averages to zero over a rotation, a centred running mean of the
#'   magnetometer vector removes it while preserving the slow, orientation-driven variation. `"auto"`
#'   (default) detects the paddle and derives one stable window per deployment from its rotation rate;
#'   `"manual"` always applies `heading.denoise.window`; `"off"` disables it. Where the paddle turns too
#'   slowly to be separated from the animal's own turning, no window can help and a warning is raised -
#'   use a gyroscope-based orientation estimator instead.
#' @param heading.denoise.window Smoothing window in seconds used when `heading.denoise = "manual"`
#'   (default `3`). It should span several paddle rotations but stay well short of the animal's turning
#'   timescale.
#'
#' @return A validated `nautilus_orientation` object for the `orientation` argument of
#'   [processTagData()].
#'
#' @seealso [processTagData()] for the function that consumes it; [calibrationControl()] and
#'   [smoothingControl()] for the other processing settings.
#'
#' @examples
#' orientationControl(correct.roll = FALSE)     # skip the roll-offset correction
#' orientationControl(madgwick.beta = 0.05)     # stronger Madgwick gain
#' orientationControl(heading.denoise = "manual", heading.denoise.window = 2)
#' @references
#' Madgwick SOH, Harrison AJL, Vaidyanathan R (2011) Estimation of IMU and MARG orientation using a
#' gradient descent algorithm. *IEEE International Conference on Rehabilitation Robotics*. 1-7.
#' \doi{10.1109/ICORR.2011.5975346}
#' @export
orientationControl <- function(madgwick.beta = 0.02, correct.pitch = TRUE, correct.roll = TRUE,
                               pitch.offset.min.r2 = 0.1, mount.roll.max = 60, warning.threshold = 45,
                               heading.denoise = c("auto", "manual", "off"),
                               heading.denoise.window = 3) {
  heading.denoise <- match.arg(heading.denoise)
  .assert_number(madgwick.beta, "orientation$madgwick.beta", min = 0)
  .assert_flag(correct.pitch, "orientation$correct.pitch")
  .assert_flag(correct.roll, "orientation$correct.roll")
  .assert_number(pitch.offset.min.r2, "orientation$pitch.offset.min.r2", min = 0, max = 1)
  .assert_number(mount.roll.max, "orientation$mount.roll.max", min = 0, max = 180)
  .assert_number(warning.threshold, "orientation$warning.threshold", min = 0)
  .assert_number(heading.denoise.window, "orientation$heading.denoise.window", min = 0)
  structure(list(madgwick.beta = madgwick.beta, correct.pitch = correct.pitch, correct.roll = correct.roll,
                 pitch.offset.min.r2 = pitch.offset.min.r2, mount.roll.max = mount.roll.max,
                 warning.threshold = warning.threshold,
                 heading.denoise = heading.denoise, heading.denoise.window = heading.denoise.window),
            class = "nautilus_orientation")
}


#' Anomaly-detection settings for one sensor channel
#'
#' @description
#' What counts as an impossible jump depends entirely on the channel. Depth can change by metres per
#' second during a dive; temperature cannot change by degrees per second anywhere in the ocean. A single
#' threshold across channels would either miss real faults or flag ordinary behaviour.
#'
#' `anomalyControl()` describes one channel, so [checkSensorQuality()] can screen several in a single
#' call, each judged on its own terms.
#'
#' @param rate.threshold How fast the channel can plausibly change, in units per second. A
#'   sample-to-sample change beyond this is treated as a spike rather than a measurement. Set it from
#'   what the animal and the environment allow, with some headroom: too low and normal behaviour is
#'   flagged, too high and real spikes survive into your analysis. Required.
#' @param sensor.resolution The smallest change the channel can express, in its measurement units.
#'   Used in the resolution gate preceding the rate test. The current gate is sampling-interval
#'   dependent; see [checkSensorQuality()] for its criterion and limitations.
#'
#'   Required, with no default, because resolution is a property of a particular instrument and channel
#'   and the package has no basis for guessing it: a value suited to depth in metres is an order of
#'   magnitude too coarse for temperature in degrees. Take it from the tag's specification, or from the
#'   smallest non-zero difference between consecutive raw readings.
#' @param sensor.accuracy.fixed,sensor.accuracy.percent The sensor's stated accuracy, as a fixed value in
#'   the channel's units or as a percentage of the reading. Supply at most one. Retained in this control
#'   object but not used by the detector or copied into the processing-history entry. Defaults `NULL`.
#' @param outlier.window How close together, in minutes, outliers must fall to be treated as one
#'   malfunction period rather than as separate spikes. Default 5. Widen it where a failing sensor
#'   glitches intermittently over a longer stretch.
#' @param stall.threshold Minimum duration of identical, strictly positive readings flagged as a stall,
#'   in minutes (default \code{5}), evaluated using the nominal sampling frequency. Zero and negative
#'   constant readings are not flagged. Raise it for channels that legitimately remain constant.
#' @return A validated `nautilus_anomaly` object, for one entry of [checkSensorQuality()]'s `sensors`
#'   argument.
#' @seealso [checkSensorQuality()]
#' @examples
#' anomalyControl(rate.threshold = 7, sensor.resolution = 0.5, sensor.accuracy.percent = 1)
#' @export
anomalyControl <- function(rate.threshold,
                           sensor.resolution,
                           sensor.accuracy.fixed = NULL,
                           sensor.accuracy.percent = NULL,
                           outlier.window = 5,
                           stall.threshold = 5) {
  .assert_number(rate.threshold, "rate.threshold", min = 0)
  .assert_number(sensor.resolution, "sensor.resolution", min = 0)
  .assert_number(outlier.window, "outlier.window", min = 0)
  .assert_number(stall.threshold, "stall.threshold", min = 0)
  if (!is.null(sensor.accuracy.fixed) && !is.null(sensor.accuracy.percent))
    .abort("Provide only one of {.arg sensor.accuracy.fixed} or {.arg sensor.accuracy.percent}, not both.")
  if (!is.null(sensor.accuracy.fixed))   .assert_number(sensor.accuracy.fixed, "sensor.accuracy.fixed", min = 0)
  if (!is.null(sensor.accuracy.percent)) .assert_number(sensor.accuracy.percent, "sensor.accuracy.percent", min = 0, max = 100)
  structure(list(rate.threshold = rate.threshold, sensor.resolution = sensor.resolution,
                 sensor.accuracy.fixed = sensor.accuracy.fixed, sensor.accuracy.percent = sensor.accuracy.percent,
                 outlier.window = outlier.window, stall.threshold = stall.threshold),
            class = "nautilus_anomaly")
}


#' Depth zero-offset drift-correction settings for processTagData()
#'
#' @description
#' Bundles the settings for the depth zero-offset drift correction applied by \code{\link{processTagData}}.
#' Pressure sensors accumulate a slowly-varying zero offset over a deployment (mainly thermal), so an
#' animal at the surface gradually stops reading 0 m. The correction estimates that offset from
#' independent surface evidence and subtracts it; by default it never infers the surface from the depth
#' trace, and it abstains rather than invent a zero line when evidence is too sparse. An opt-in "shallow
#' mode" (`surface.evidence = "depth"`) can additionally infer surface intervals from the depth trace.
#'
#' @param method Correction method: `"surface"` (surface-anchored zero-offset correction, the default)
#'   or `"none"` (disable; depth is left untouched).
#' @param surface.evidence Character vector of the evidence sources used to locate surface references,
#'   any of `"dry"` (a wet/dry sensor's sustained dry intervals), `"gps"` (surface-implying position
#'   fixes - Fastloc-GPS or Argos - whose antenna must break the surface), and `"depth"` (an opt-in
#'   "shallow mode" that infers surface intervals from the depth trace itself; see the `surface.*`
#'   arguments). `"dry"` and `"gps"` are independent of the depth trace and are the safe default;
#'   `"depth"` is used only as a gap-filler where the independent sources are absent, and it assumes the
#'   shallowest sustained depth is the surface, so it is unsuitable for animals that rarely surface.
#'   Default uses `"dry"` and `"gps"`.
#' @param min.dry.duration Minimum duration (seconds) of a sustained dry interval for it to count as a
#'   surface anchor; briefer dry flips (spray, wave wash-over) are ignored. Default 3.
#' @param max.gap Maximum interval (hours) between consecutive surface anchors for the correction to be
#'   considered fully reliable. Samples inside a longer gap are still corrected (the offset is
#'   interpolated across it) but flagged low-confidence, and the step status becomes `"applied_with_gaps"`.
#'   Default 6.
#' @param min.anchors Minimum number of surface anchors for a time-varying correction. With exactly one
#'   anchor a single constant offset is applied; with none the correction abstains and depth is left
#'   untouched. Default 2.
#' @param surface.quantile,surface.band The surface level is estimated as the `surface.quantile` (0.05)
#'   quantile of depth over the deployment (the animal's shallowest sustained depth). Two uses: (1) in
#'   "shallow mode" (`surface.evidence = "depth"`), a sample counts as at-surface when its depth is within
#'   `surface.band` (2 m) of that estimate; (2) for ALL evidence types, an anchor is a valid zero-offset
#'   only when its depth reads within `surface.band` of the surface level - a "surface" fix that lands on a
#'   dive (reading tens of metres) is a mis-timed/mislabelled fix, not the sensor zero drift, and is
#'   rejected (else it would over-correct the depth above the surface). `surface.band` should exceed both
#'   the surface wave/noise amplitude and the expected drift magnitude.
#' @return A validated `nautilus_depth_drift` object for the `depth.drift` argument of \code{\link{processTagData}}.
#' @seealso \code{\link{processTagData}}, \code{\link{smoothingControl}}, \code{\link{calibrationControl}}
#' @examples
#' depthDriftControl(surface.evidence = "dry", max.gap = 12)
#' depthDriftControl(method = "none")   # disable drift correction
#' @export
depthDriftControl <- function(method = c("surface", "none"),
                              surface.evidence = c("dry", "gps"),
                              min.dry.duration = 3,
                              max.gap = 6,
                              min.anchors = 2,
                              surface.quantile = 0.05,
                              surface.band = 2) {
  method <- match.arg(method)
  valid_ev <- c("dry", "gps", "depth")
  bad <- setdiff(surface.evidence, valid_ev)
  if (length(bad))
    .abort(c("{.arg depth.drift$surface.evidence} has invalid value{?s} {.val {bad}}.",
             "i" = "Valid sources: {.val {valid_ev}}."))
  if (!length(surface.evidence)) .abort("{.arg depth.drift$surface.evidence} must name at least one source.")
  .assert_number(min.dry.duration, "depth.drift$min.dry.duration", min = 0)
  .assert_number(max.gap, "depth.drift$max.gap", min = 0)
  .assert_count(min.anchors, "depth.drift$min.anchors", min = 1L)
  .assert_number(surface.quantile, "depth.drift$surface.quantile", min = 0, max = 1)
  .assert_number(surface.band, "depth.drift$surface.band", min = 0)
  structure(list(method = method, surface.evidence = unique(surface.evidence),
                 min.dry.duration = min.dry.duration, max.gap = max.gap, min.anchors = min.anchors,
                 surface.quantile = surface.quantile, surface.band = surface.band),
            class = "nautilus_depth_drift")
}


#' Cross-device clock-alignment settings for importTagData()
#'
#' @description
#' Bundles the settings for the temporal alignment [importTagData()] applies when a deployment pairs a
#' primary archival tag, recording depth and inertial data, with a separate Wildlife Computers tag
#' recording wet/dry state and Fastloc-GPS positions.
#'
#' The two devices keep independent clocks, which can disagree by anything from a few seconds to many
#' minutes. Nothing in either record reveals the offset on its own, yet it silently corrupts every step
#' that combines the streams: the depth zero-offset correction, and the position fixes that anchor a
#' dead-reckoned track.
#'
#' @details
#' The Wildlife Computers archive file records that tag's own depth, and often temperature, at a low
#' rate. Because depth is a physical quantity measured by *both* devices, cross-correlating the two
#' depth series recovers the offset directly: it is the lag at which they agree best. In real
#' deployments this peak is sharp, so the estimate is well determined. The streams carried on the
#' Wildlife Computers clock are then shifted onto the primary tag's timeline. The primary depth and
#' inertial stream is the reference and is never moved, and neither are the deployment and pop-up
#' positions, which come from the metadata table rather than from that clock.
#'
#' A single constant offset is estimated per deployment. Residual drift is negligible in practice - a
#' few seconds over a multi-day record - and dominated by the constant term.
#'
#' The correction abstains, shifting nothing and saying so, whenever the evidence is too weak to trust:
#' no shared depth channel, too little overlap between the records, a flat depth trace with no dives to
#' lock onto, or a peak correlation below `min.correlation`. The clock is never shifted silently, and
#' the estimated offset and its diagnostics are stored in the deployment's metadata.
#'
#' @param method How to align the clocks: `"depth-xcorr"` (default) cross-correlates the shared depth
#'   channel, and `"none"` disables alignment, keeping the Wildlife Computers streams on their own
#'   clock.
#' @param max.lag Largest absolute clock offset to search for, in seconds (default `3600`, one hour).
#'   This also acts as a sanity bound: a best lag landing on the edge of the search range is treated as
#'   unresolved and the correction abstains. Widen it only if you have reason to expect a larger
#'   disagreement.
#' @param min.overlap Minimum overlap between the two depth records, in minutes, required to attempt
#'   alignment (default `30`). Below this the correction abstains, because a short overlap can produce a
#'   convincing correlation peak at the wrong lag.
#' @param min.correlation Minimum peak correlation between the two depth traces, at the best lag, for
#'   the offset to be accepted (default `0.9`). Below this the profiles do not match well enough to trust
#'   the lag and the correction abstains. Lower it only for records whose depth traces are genuinely
#'   noisy, and check the stored diagnostics afterwards.
#'
#' @return A validated `nautilus_alignment` object for the `alignment` argument of [importTagData()].
#'
#' @seealso [importTagData()] for the function that consumes it; [depthDriftControl()] for the depth
#'   correction that depends on the alignment being right.
#'
#' @examples
#' alignmentControl(min.correlation = 0.95)   # stricter acceptance
#' alignmentControl(method = "none")          # disable clock alignment
#' @export
alignmentControl <- function(method = c("depth-xcorr", "none"),
                             max.lag = 3600,
                             min.overlap = 30,
                             min.correlation = 0.9) {
  method <- match.arg(method)
  .assert_number(max.lag, "alignment$max.lag", min = 1)
  .assert_number(min.overlap, "alignment$min.overlap", min = 0)
  .assert_number(min.correlation, "alignment$min.correlation", min = 0, max = 1)
  structure(list(method = method, max.lag = max.lag,
                 min.overlap = min.overlap, min.correlation = min.correlation),
            class = "nautilus_alignment")
}


#' Timestamp-recognition settings for getVideoMetadata()
#'
#' @description
#' Groups the settings [getVideoMetadata()] uses when it has to read a recording time off the picture,
#' from the clock a camera burns into its own footage.
#'
#' This is a fallback, not the normal path. The recording time is taken from the file name whenever a
#' camera writes one there, because that is exact, costs nothing and does not depend on the video at all.
#' Reading the screen is for cameras that write no such name, and for the optional cross-check. Nothing
#' here is consulted otherwise.
#'
#' @param model Which Tesseract model to use, trained on the overlay font. Default `"cam"`, the
#'   fine-tuned camera-tag model, downloaded on first use by [installCamOcrModel()]. Pass `"eng"`, or
#'   any other installed model, to skip that download at some cost in accuracy on this particular font.
#' @param box Where the timestamp sits in the frame, as `c(x, y, width, height)` in pixels, with `x` and
#'   `y` the top-left corner. The coordinates are read relative to `frame.height` and rescaled for
#'   videos of a different resolution, so one setting covers every resolution of the same camera. Default
#'   `c(3249, 2120, 325, 28)`, the bottom-right box of the 4K camera overlay. Change it for a camera that
#'   draws its clock somewhere else - grab a frame and read the pixel coordinates off it.
#' @param frame.height The frame height, in pixels, that `box` was measured against. Default `2160`.
#' @param search.radius How far, in pixels, to search around `box` for the bright timestamp panel
#'   (default `80`). This absorbs the small drift in overlay position between cameras and firmware
#'   versions, so a box measured on one unit still works on its siblings. Widen it if the clock moves
#'   more than that; too wide and the search can lock onto some other bright rectangle.
#' @param max.search.frames How many frames to try before giving up on a video (default `10`). The first
#'   frame of a clip is often black or half-exposed, which is what this exists for.
#' @param char.whitelist The characters the recogniser is allowed to return. `NULL` (default) uses the
#'   package's own alphabet - digits, the letters that spell the month abbreviations, and the few
#'   punctuation marks a timestamp needs - which is already restrictive enough for this job. Override
#'   it only for a camera whose clock uses a different format, and remember that the month
#'   abbreviation is read as letters, so a digits-only whitelist will break the parse.
#'
#' @return A validated `nautilus_ocr` object for the `ocr` argument of [getVideoMetadata()].
#'
#' @seealso [getVideoMetadata()] for the function that consumes it; [installCamOcrModel()] for
#'   pre-fetching the default model.
#'
#' @examples
#' ocrControl(box = c(120, 40, 300, 26), frame.height = 1080)   # 1080p camera, overlay top-left
#' @export
ocrControl <- function(model = "cam",
                       box = c(3249, 2120, 325, 28),
                       frame.height = 2160,
                       search.radius = 80,
                       max.search.frames = 10,
                       char.whitelist = NULL) {
  .assert_string(model, "ocr$model")
  if (!is.numeric(box) || length(box) != 4L || anyNA(box))
    .abort("{.arg ocr$box} must be a numeric vector {.code c(x, y, width, height)} of length 4.")
  if (any(box[1:2] < 0) || any(box[3:4] <= 0))
    .abort("{.arg ocr$box} must have non-negative {.code x}/{.code y} and positive {.code width}/{.code height}.")
  .assert_number(frame.height, "ocr$frame.height", min = 1)
  .assert_number(search.radius, "ocr$search.radius", min = 0)
  .assert_count(max.search.frames, "ocr$max.search.frames", min = 1L)
  .assert_string(char.whitelist, "ocr$char.whitelist", null_ok = TRUE)
  structure(list(model = model, box = as.numeric(box), frame.height = frame.height,
                 search.radius = search.radius, max.search.frames = max.search.frames,
                 char.whitelist = char.whitelist),
            class = "nautilus_ocr")
}


#' Detection thresholds for checkSensorIntegrity()
#'
#' @description
#' Sets the thresholds at which [checkSensorIntegrity()] grades a finding, so the main call stays
#' readable and every threshold is documented in one place. The defaults suit large marine vertebrates
#' carrying multi-sensor archival tags; a different species or tag system may warrant different ones.
#'
#' @details
#' Every field is a classification threshold: the value of a check's metric at which a finding is graded
#' `"info"`, `"warning"` or `"error"`. Fields are named `<check>.<severity>`, so the grade a number
#' produces can be read from its name.
#'
#' Severity is therefore a property of the measurement rather than of the check: 1% clipping and 99%
#' clipping come from the same check but are graded differently. Not every check offers every grade,
#' because an automatic error verdict is only defensible where a broken channel is clearly separated
#' from a healthy one; checks whose metric varies continuously expose a warning threshold only. See the
#' Details of [checkSensorIntegrity()].
#'
#' Settings that govern how a metric is computed - spectral search bands, robustness floors - are
#' deliberately not exposed. They are implementation choices rather than scientific ones, and keeping
#' them internal leaves the algorithms free to improve without changing this interface.
#'
#' @param duplication.error Duplication: a gyroscope or magnetometer triplet is a copy of the
#'   accelerometer when the per-axis \code{|r|} exceeds this on all three axes. Default 0.999. (A copied
#'   channel carries no independent information, so this is always an error.)
#' @param saturation.warning,saturation.error Saturation: the fraction of samples pinned at the channel's
#'   exact minimum or maximum (clipping). Above \code{saturation.warning} the channel is flagged for
#'   review; above \code{saturation.error} it has lost the dynamic range that quantitative use requires.
#'   Defaults 0.01 and 0.20.
#' @param accel.scale.warning,accel.scale.error Accelerometer scale: departure of the median
#'   static-acceleration magnitude from 1 g (in g). The two levels mean different things. A moderate
#'   departure is a calibration or scaling error (warning): it leaves roll and pitch untouched, because
#'   a common factor cancels in the arctangent, but it passes straight through every magnitude-derived
#'   channel - ODBA, VeDBA and tail-beat amplitude are all proportional to it, so two deployments of the
#'   same animal calibrated 10% apart are not comparable on effort. A large departure is a unit mistake
#'   - acceleration left in m/s^2 reads about 9.8 g - and the data cannot be used until it is fixed
#'   (error). Defaults 0.05 and 0.50.
#'
#'   The warning default was 0.20 and admitted a genuine 10% scale error in silence. Measured across a
#'   real 11-deployment cohort the static magnitude ranged from 0.89 to 1.03 g, i.e. errors up to 11%,
#'   and not one deployment reached the old threshold. The value is not an artefact of the low-pass the
#'   check uses: the within-window attitude spread was only 3.4-7.9 degrees, so vector averaging shrinks
#'   the magnitude by under 1%, and a per-sample estimator over the quietest samples agrees to 0.01 g.
#' @param mag.plausibility.warning Magnetometer plausibility: the robust coefficient of variation of the
#'   hard-iron-centred field magnitude (a stable field is near-constant). Default 0.4. Warning only: this
#'   metric varies continuously between deployments, with no break separating a degraded magnetometer
#'   from the tail of normal variation, so no automatic error grade would be defensible.
#' @param mag.break.warning Magnetometer break: how completely the field magnitude before and after the
#'   best candidate break separate - the Mann-Whitney probability of superiority between the two
#'   segments' window medians, from 0.5 (indistinguishable) to 1 (no overlap at all). Default 0.96,
#'   warning only. Deliberately a separation rather than a step size: a contaminated magnetometer's field
#'   magnitude varies with heading, so a turning animal swings it between levels throughout, and step
#'   size alone flags many sound records. Separation instead asks whether the level changed and did not
#'   come back, which is what contamination attaching or shedding actually does. Raise it towards 1 to
#'   flag only near-complete separations.
#' @param gyro.bias.info Gyroscope bias: the largest per-axis median offset, as a fraction of the
#'   rotational signal scale. Default 0.3. Info only.
#' @param paddle.warning Paddle-wheel contamination: the prominence (peak / median band power) of a
#'   narrow-band peak in the magnetometer spectrum. Default 30. Warning only.
#' @param dropout.info Dropout: the fraction of the deployment for which a channel is missing (NA).
#'   Default 0.5. Info only.
#' @return A validated `nautilus_integrity` object, for the `control` argument of
#'   [checkSensorIntegrity()].
#' @seealso [checkSensorIntegrity()], whose Details explain each check and what its metric measures.
#' @examples
#' integrityControl(saturation.error = 0.1)          # stricter: 10% clipping is already an error
#' integrityControl(mag.plausibility.warning = 0.5)  # more tolerant of an unstable field
#' integrityControl(mag.break.warning = 0.99)        # only near-perfect separation counts as a break
#' @export
integrityControl <- function(duplication.error        = 0.999,
                             saturation.warning       = 0.01,
                             saturation.error         = 0.20,
                             accel.scale.warning      = 0.05,
                             accel.scale.error        = 0.50,
                             mag.plausibility.warning = 0.40,
                             mag.break.warning        = 0.96,
                             gyro.bias.info           = 0.30,
                             paddle.warning           = 30,
                             dropout.info             = 0.50) {
  .assert_number(duplication.error, "duplication.error", min = 0, max = 1)
  .assert_number(saturation.warning, "saturation.warning", min = 0, max = 1)
  .assert_number(saturation.error, "saturation.error", min = 0, max = 1)
  .assert_number(accel.scale.warning, "accel.scale.warning", min = 0)
  .assert_number(accel.scale.error, "accel.scale.error", min = 0)
  .assert_number(mag.plausibility.warning, "mag.plausibility.warning", min = 0)
  .assert_number(mag.break.warning, "mag.break.warning", min = 0.5, max = 1)
  .assert_number(gyro.bias.info, "gyro.bias.info", min = 0)
  .assert_number(paddle.warning, "paddle.warning", min = 1)
  .assert_number(dropout.info, "dropout.info", min = 0, max = 1)
  # an error threshold that sits below its warning threshold would make the warning unreachable, and the
  # grade a value receives would stop being monotone in the metric - reject it rather than silently reorder
  if (saturation.error < saturation.warning)
    .abort("{.arg saturation.error} ({saturation.error}) must be >= {.arg saturation.warning} ({saturation.warning}).")
  if (accel.scale.error < accel.scale.warning)
    .abort("{.arg accel.scale.error} ({accel.scale.error}) must be >= {.arg accel.scale.warning} ({accel.scale.warning}).")
  structure(list(duplication.error = duplication.error,
                 saturation.warning = saturation.warning, saturation.error = saturation.error,
                 accel.scale.warning = accel.scale.warning, accel.scale.error = accel.scale.error,
                 mag.plausibility.warning = mag.plausibility.warning,
                 mag.break.warning = mag.break.warning,
                 gyro.bias.info = gyro.bias.info, paddle.warning = paddle.warning,
                 dropout.info = dropout.info),
            class = "nautilus_integrity")
}


#' Internal method parameters for the integrity checks (NOT user-facing).
#'
#' These govern HOW a metric is computed, not how it is interpreted: the paddle spectral search band and
#' the gyro-bias absolute floor. They are implementation details of the detectors - deliberately kept out
#' of `integrityControl()` so the algorithms can be improved without an API change - whereas everything a
#' user should reasonably tune (the metric -> severity thresholds) is public there.
#'   \itemize{
#'     \item `gyro.bias.min` - absolute floor (rad/s) a median offset must also clear, so a negligible
#'       offset is not flagged merely because the animal barely rotated (a tiny MAD inflates the ratio).
#'     \item `paddle.min.freq`, `paddle.harmonic.guard` - the search floor is
#'       `max(paddle.min.freq, paddle.harmonic.guard * f_tailbeat)` Hz, keeping it clear of the tail-beat
#'       fundamental and its harmonics (the main source of false positives).
#'     \item `paddle.max.freq.frac` - ceiling as a fraction of Nyquist, avoiding aliasing artefacts.
#'     \item `mag.break.window` - window DURATION (s) the field magnitude is summarised over. A duration
#'       rather than a count, so the statistic means the same thing on a 5 h and a 50 h record.
#'     \item `mag.break.min.frac` - each side of a candidate break must be at least this fraction of the
#'       record. This is what "persistent" means operationally, and it sets the blind spot: a break in
#'       the first or last `mag.break.min.frac` of a record cannot be seen.
#'     \item `mag.break.min.windows` - fewer windows than this and the check abstains rather than guess.
#'     \item `mag.break.min.rel` - the step must also be at least this fraction of the field magnitude,
#'       so perfect rank separation across a negligible shift (a stable sensor drifting) is not flagged.
#'   }
#' @keywords internal
#' @noRd
.integrityMethod <- function() {
  list(gyro.bias.min = 0.02, paddle.min.freq = 3.5, paddle.harmonic.guard = 6, paddle.max.freq.frac = 0.85,
       mag.break.window = 600, mag.break.min.frac = 0.15, mag.break.min.windows = 30L,
       mag.break.min.rel = 0.05)
}


#' Metric selection and window sizes for trackMetrics()
#'
#' @description
#' Selects which movement-path metrics [trackMetrics()] computes and the sizes of the successive windows
#' behind its temporal tortuosity columns, so the main call stays uncluttered.
#'
#' @param metrics Which metrics to compute: any of `"path_ratio"`, `"sinuosity"`, `"turning_angle"` and
#'   `"straightness"`, or `"all"` (the default). Narrow it when you only need one or two; the
#'   local-turning metrics are the more expensive to compute on a long track.
#' @param min.points The fewest valid positions a track needs before it is summarised at all; shorter
#'   tracks are skipped. Default `5`. Raise it if a handful of positions is not enough for the
#'   comparison you intend, since a two-point "path" is straight by construction.
#' @param hourly.window.h,daily.window.h The window lengths in hours behind the `Hourly_tortuosity` and
#'   `Daily_tortuosity` columns, each the mean path-to-displacement ratio over successive, non-overlapping
#'   windows of that length. Defaults `1` and `24`. Choose them to bracket the timescales your animal's behaviour
#'   actually switches on - a foraging bout and a diel cycle, say - rather than leaving them at values
#'   that fall between the two.
#'
#' @return A validated `nautilus_track_metrics` object for the `control` argument of [trackMetrics()].
#'
#' @seealso [trackMetrics()] for the function that consumes it.
#'
#' @examples
#' trackMetricsControl(metrics = c("path_ratio", "straightness"), min.points = 10)
#' @export
trackMetricsControl <- function(metrics = "all",
                                min.points = 5,
                                hourly.window.h = 1,
                                daily.window.h = 24) {
  available <- c("path_ratio", "sinuosity", "turning_angle", "straightness")
  if (!is.character(metrics) || !length(metrics))
    .abort("{.arg trackMetrics$metrics} must be a non-empty character vector.")
  bad <- setdiff(metrics, c(available, "all"))
  if (length(bad))
    .abort(c("{.arg trackMetrics$metrics} has invalid value{?s} {.val {bad}}.",
             "i" = "Valid values: {.val {c('all', available)}}."))
  .assert_count(min.points, "trackMetrics$min.points", min = 2L)
  .assert_number(hourly.window.h, "trackMetrics$hourly.window.h", min = 0)
  .assert_number(daily.window.h, "trackMetrics$daily.window.h", min = 0)
  if (hourly.window.h <= 0 || daily.window.h <= 0)
    .abort("{.arg trackMetrics$hourly.window.h} and {.arg trackMetrics$daily.window.h} must be > 0.")
  structure(list(metrics = metrics, min.points = as.integer(min.points),
                 hourly.window.h = hourly.window.h, daily.window.h = daily.window.h),
            class = "nautilus_track_metrics")
}


#' Tuning for the speed check in filterLocations()
#'
#' @description
#' Groups the tuning of the neighbour-consistency speed test used by [filterLocations()] into one
#' validated object. The threshold that matters most - the fastest speed you would believe - stays the
#' top-level `max.speed.kmh` argument of that function; this object governs only how the test is
#' applied.
#'
#' @param min.time.mins The shortest separation, in minutes, between two fixes for the speed implied
#'   between them to be trusted. Closer pairs are not judged, because a sub-threshold gap inflates the
#'   apparent speed unreliably: a metre of positional jitter over a few seconds looks like a huge
#'   speed. Default \code{0}; non-finite speeds, including those from zero-time intervals, are not
#'   judged. This filter does not remove duplicate timestamps. Raise it if your tag reports bursts
#'   of near-simultaneous fixes.
#' @param max.iterations The most removal passes to make. Each pass removes the single most egregious
#'   spike and recomputes speeds against the new neighbours, and the loop stops early once no fix is
#'   implausible. Default `50`. It is a runaway guard rather than a tuning knob; reaching it usually
#'   means the threshold is too tight for the data.
#' @param spike.angle An optional direction-reversal test, in degrees between 90 and 180, that
#'   supplements the speed test: an interior fix is also treated as a spike when the track's heading
#'   reverses by at least this much there *and* at least one adjoining segment exceeds
#'   `max.speed.kmh`. It can therefore flag a reversal with only one over-threshold segment, but does
#'   not flag reversals whose adjacent speeds are both below the threshold. \code{NULL} (default)
#'   disables it; choose the angle after inspecting representative tracks.
#'
#' @return A validated `nautilus_filter_locations` object for the `control` argument of
#'   [filterLocations()].
#'
#' @seealso [filterLocations()] for the function that consumes it.
#'
#' @examples
#' filterLocationsControl(min.time.mins = 2)     # ignore fix pairs less than 2 min apart
#' filterLocationsControl(spike.angle = 160)     # also flag sharp out-and-back spikes
#' @export
filterLocationsControl <- function(min.time.mins = 0,
                                   max.iterations = 50,
                                   spike.angle = NULL) {
  .assert_number(min.time.mins, "filterLocations$min.time.mins", min = 0)
  .assert_count(max.iterations, "filterLocations$max.iterations", min = 1L)
  .assert_number(spike.angle, "filterLocations$spike.angle", min = 90, max = 180, null_ok = TRUE)
  structure(list(min.time.mins = min.time.mins, max.iterations = as.integer(max.iterations),
                 spike.angle = spike.angle),
            class = "nautilus_filter_locations")
}


#' Control settings for reconstructTrack()
#'
#' @description Groups the dead-reckoning and track-correction knobs of \code{\link{reconstructTrack}} into a
#' single object: how the animal's swimming speed is set, the biological speed cap, and how the drifting
#' reckoned path is reconciled with verified positions.
#'
#' @details
#' Dead reckoning integrates a *speed* and a *heading* forward in time to reconstruct a movement path (see
#' the "How the reconstruction proceeds" section of \code{\link{reconstructTrack}}). Heading is produced upstream by
#' \code{\link{processTagData}}; this control object governs the two remaining ingredients - the **speed**
#' used at each step, and the **Verified Position Correction (VPC)** that ties the path back to known fixes.
#'
#' ## Choosing a speed method
#' Because the reckoning multiplies speed by heading, the *shape* of the track is set by heading while its
#' *scale* is set by speed. The options trade off honesty against realism:
#' \itemize{
#'   \item `"constant"` (default) - a single nominal speed (`constant.speed`). The safest choice: it makes
#'     no unsupported claim about moment-to-moment speed, so the track is shape-faithful but only nominally
#'     scaled. Between-fix VPC still rescales each segment to the true fix-to-fix distance.
#'   \item `"vedba"` - speed from a linear model `speed = intercept + slope x VeDBA`, where VeDBA
#'     (Vectorial Dynamic Body Acceleration) is the rotation-invariant activity metric computed by
#'     \code{\link{processTagData}}. Dynamic acceleration scales with locomotor effort, so VeDBA is a strong
#'     proxy for through-water speed (Bidder et al. 2012; Gunner et al. 2021). Supply the model via
#'     `vedba.model`, or leave it `NULL` to auto-calibrate from the deployment's own GPS fixes (see below).
#'   \item `"paddle"` - speed from a `paddle_speed` column (a mechanical paddle-wheel rotation count).
#'   \item `"depth_rate"` - horizontal speed inferred from vertical velocity and the dive geometry
#'     (`horizontal = vertical_velocity / tan(pitch)`). This is reliable ONLY on steep glides: near
#'     horizontal, `1 / tan(pitch)` explodes and a small pitch error yields a wildly wrong speed, so samples
#'     shallower than `depth.rate.min.pitch` are dropped and back-filled with `constant.speed`
#'     (Wensveen et al. 2015). Hence it is not the default.
#' }
#'
#' ## VeDBA auto-calibration (`vedba.model = NULL`)
#' When no model is supplied, `reconstructTrack` fits `speed = intercept + slope x VeDBA` from the
#' deployment itself: for every pair of consecutive position fixes it forms the straight-line
#' (great-circle) speed and the mean VeDBA over that interval, keeps only intervals during which the animal
#' travelled in a near-straight line (so straight-line distance approximates the true path length), and
#' regresses speed on VeDBA (Gunner et al. 2021). Sparse or tortuous fix sets rarely yield enough clean
#' intervals; when the calibration is under-determined or non-physical (slope <= 0) the method **falls back
#' to `constant.speed`** and records this in the processing log and metadata. For a definitive calibration,
#' fit the model externally against high-rate GPS and pass it via `vedba.model`.
#'
#' ## Rest gating (`rest.quantile`)
#' Reckoning drift accumulates whenever a non-zero speed is integrated, including while the animal is
#' resting. Setting `rest.quantile` holds the speed at zero whenever VeDBA falls in its lowest quantile
#' (e.g. `0.10` = the least-active 10% of samples), preventing spurious wandering during inactivity; Gunner
#' et al. (2021) found such activity-gating reduced net reconstruction error. Requires a `vedba` column;
#' `NULL` (default) disables it.
#'
#' @param speed.method How to set swimming speed for the reckoning: one of `"constant"` (default),
#'   `"vedba"`, `"paddle"`, or `"depth_rate"`. See *Choosing a speed method*.
#' @param constant.speed Numeric. Speed (m/s) for `speed.method = "constant"`, and the fallback used when
#'   direct speed estimates are unavailable. A substantial proportion of the reconstructed track may
#'   therefore rely on assumed rather than directly estimated speed. It is reached two different ways, and
#'   the distinction matters when you judge a track:
#'   \itemize{
#'     \item \emph{Gap back-fill.} Any sample still lacking a finite speed is set to `constant.speed`. In
#'       practice this bites hardest under `"depth_rate"`, where every sample pitched shallower than
#'       `depth.rate.min.pitch` is dropped - often the majority of a record. Paddle gaps mostly do NOT
#'       land here: interior gaps in `paddle_speed` are interpolated first, so only leading/trailing gaps
#'       (or an essentially absent channel) fall through.
#'     \item \emph{Whole-track fallback.} If the `"vedba"` calibration cannot be fitted, or a `"paddle"`
#'       record carries no usable speed channel, the \emph{entire} track is set to `constant.speed` and
#'       the reason is logged. The result is then a wholly assumed track, not a partially assumed one.
#'   }
#'   Check `speed_dr` for how much of the track is a single repeated value before interpreting fine-scale
#'   track structure. Default 0.5.
#' @param max.speed Numeric. Biological speed cap (m/s); any estimated speed above it is clipped. Default
#'   2.5.
#' @param vedba.model Speed-from-VeDBA calibration for `speed.method = "vedba"`. Either `NULL` (default;
#'   auto-calibrate from the deployment's GPS fixes, see *VeDBA auto-calibration*) or a length-2 numeric
#'   `c(intercept, slope)` giving `speed (m/s) = intercept + slope x VeDBA (g)`.
#' @param depth.rate.min.pitch Numeric. Minimum absolute pitch (degrees) at which `speed.method =
#'   "depth_rate"` is trusted; shallower samples are set NA and back-filled with `constant.speed`. Default
#'   45.
#' @param rest.quantile Numeric in \[0, 1\] or `NULL`. If set, the swimming speed is forced to zero wherever
#'   VeDBA is below this quantile of the deployment (activity/rest gating). `NULL` (default) disables it.
#'   Typical values are small (0.05-0.15).
#' @param vpc.method Verified Position Correction, i.e. how the reckoned path is reconciled with the fixes:
#'   \itemize{
#'     \item `"error_weighted"` (default) - additively distributes the reckoning drift between anchors,
#'       weighted by each fix's quality (via `anchor.error.radii`), so a noisy fix does not yank the track.
#'     \item `"linear"` - additively distributes the drift, forcing the track exactly through every fix.
#'     \item `"scale_rotate"` - the Gundog.Tracks correction (Gunner et al. 2021): per segment, rescales and
#'       rotates the whole reckoned sub-path (a similarity transform) so its shape is preserved while its end
#'       is pinned exactly onto the next fix. This is the more faithful correction for a \emph{systematic}
#'       drift - a mis-calibrated speed (a pure scale error) or a constant heading bias (a pure rotation) -
#'       whereas the additive methods are better suited to random/diffusive drift. It forces exactly through
#'       every fix (treating fixes as error-free); when *placing the corrected path* it ignores
#'       `anchor.error.radii`, `drift.rate` and `vpc.weighting` (as with `"linear"`, `anchor.error.radii` and
#'       `drift.rate` still drive the reported `pseudo_error`; only `vpc.weighting` is ignored end-to-end).
#'     \item `"none"` - leaves the raw reckoned path uncorrected.
#'   }
#' @param vpc.weighting How the drift between two fixes is spread across the intervening samples (applies to
#'   the additive `"error_weighted"`/`"linear"` methods only; `"scale_rotate"` ignores it):
#'   `"distance"` (default) in proportion to the reckoned distance travelled, `"time"` in proportion to
#'   elapsed time. Distance weighting is usually more faithful because reckoning error accrues with travel,
#'   not with clock time (an animal that rested then swam should absorb the drift while swimming); the two
#'   coincide at constant speed (Gunner et al. 2021).
#' @param drift.rate Numeric. Systematic (bias-like) dead-reckoning drift rate (m/s), growing linearly with
#'   time. Used by `vpc.method = "error_weighted"` to weigh reckoning confidence against a fix (the Kalman
#'   gain), and by every `vpc.method` to scale the reported `pseudo_error`. Default 0.5.
#' @param drift.diffusion Numeric. Random-walk (diffusive) drift-variance rate (m^2/s), adding a `sqrt(time)`
#'   term so the total reckoning error is `sqrt((drift.rate * t)^2 + drift.diffusion * t)`. This captures the
#'   regime where short segments are dominated by random heading noise (grows as `sqrt(t)`) and long ones by
#'   systematic bias (grows as `t`). Default 0 (reduces exactly to the linear `drift.rate * t` model).
#' @param anchor.error.radii Named numeric vector mapping `quality` values to expected position error radii
#'   (m). Defaults cover standard Argos/FastGPS classes plus deploy/pop-up.
#' @param include.depth Logical. Attach the measured depth as the vertical axis, so the output is a 3-D
#'   pseudo-track (`pseudo_lon`, `pseudo_lat`, `depth`). Default TRUE.
#' @param reconstructability.min Numeric >= 0. A soft reliability gate for tracks that have \strong{no
#'   interior fixes} (anchored only by the deployment and pop-up). It flags such a track as unreliable when
#'   its *directedness* - the net deploy-to-pop-up displacement divided by the reckoned path length - falls
#'   below this value, i.e. the animal's net progress was a small fraction of how far it swam, so the two
#'   endpoints cannot constrain the wandering interior (validated against held-out error on real deployments;
#'   see \code{\link{crossValidateTrack}}). On a flag, `reconstructTrack` issues a `warning()` and records the
#'   verdict in `meta$sensors$reconstructability` - it never aborts, so a directed track is still returned.
#'   Default 0.1; set to 0 to disable. This is a rough triage heuristic, not a hard rule: because the
#'   denominator is the *reckoned* path, directedness is effectively net speed divided by the mean reckoned
#'   speed, so a badly mis-set `constant.speed` (or a hot speed calibration) can mis-fire - the gate is only
#'   as sound as the speed estimate.
#' @references
#' Bidder OR, Soresina M, Shepard ELC, *et al.* (2012) The need for speed: testing acceleration for
#' estimating animal travel rates in terrestrial dead-reckoning systems. *Zoology*. 115:58-64.
#' \doi{10.1016/j.zool.2011.09.003}
#'
#' Gunner RM, Holton MD, Scantlebury MD, *et al.* (2021) Dead-reckoning animal movements in R: a reappraisal
#' using Gundog.Tracks. *Animal Biotelemetry*. 9:23. \doi{10.1186/s40317-021-00245-z}
#'
#' Wensveen PJ, Thomas L, Miller PJO (2015) A path reconstruction method integrating dead-reckoning and
#' position fixes applied to humpback whales. *Movement Ecology*. 3:31. \doi{10.1186/s40462-015-0061-6}
#' @return A validated `nautilus_reconstruct_track` control object, for the `control` argument of
#'   [reconstructTrack()].
#' @seealso \code{\link{reconstructTrack}}
#' @examples
#' reconstructTrackControl(speed.method = "paddle", vpc.method = "linear")
#' # VeDBA speed with an externally fitted calibration (speed = 0.15 + 3.1 * VeDBA):
#' reconstructTrackControl(speed.method = "vedba", vedba.model = c(0.15, 3.1))
#' @export
reconstructTrackControl <- function(speed.method = c("constant", "vedba", "paddle", "depth_rate"),
                                    constant.speed = 0.5,
                                    max.speed = 2.5,
                                    vedba.model = NULL,
                                    depth.rate.min.pitch = 45,
                                    rest.quantile = NULL,
                                    vpc.method = c("error_weighted", "linear", "scale_rotate", "none"),
                                    vpc.weighting = c("distance", "time"),
                                    drift.rate = 0.5,
                                    drift.diffusion = 0,
                                    anchor.error.radii = c("3" = 250, "2" = 500, "1" = 1500, "0" = 3000,
                                      "A" = 5000, "B" = 10000, "Z" = 50000,
                                      "FastGPS" = 50, "User" = 50, "Deploy" = 50, "Popup" = 50),
                                    include.depth = TRUE,
                                    reconstructability.min = 0.1) {
  speed.method  <- match.arg(speed.method)
  vpc.method    <- match.arg(vpc.method)
  vpc.weighting <- match.arg(vpc.weighting)
  .assert_number(constant.speed, "reconstructTrack$constant.speed", min = 0)
  .assert_number(max.speed, "reconstructTrack$max.speed", min = constant.speed)
  .assert_number(drift.rate, "reconstructTrack$drift.rate", min = 0)
  .assert_number(drift.diffusion, "reconstructTrack$drift.diffusion", min = 0)
  .assert_number(depth.rate.min.pitch, "reconstructTrack$depth.rate.min.pitch", min = 0, max = 90)
  .assert_number(rest.quantile, "reconstructTrack$rest.quantile", min = 0, max = 1, null_ok = TRUE)
  if (!is.null(vedba.model) && (!is.numeric(vedba.model) || length(vedba.model) != 2L || anyNA(vedba.model)))
    .abort("{.arg reconstructTrack$vedba.model} must be NULL (auto-calibrate) or a length-2 numeric c(intercept, slope).")
  .assert_flag(include.depth, "reconstructTrack$include.depth")
  .assert_number(reconstructability.min, "reconstructTrack$reconstructability.min", min = 0)
  if (!is.numeric(anchor.error.radii) || is.null(names(anchor.error.radii)) || anyNA(names(anchor.error.radii)))
    .abort("{.arg reconstructTrack$anchor.error.radii} must be a NAMED numeric vector (quality label -> error radius, m).")
  structure(list(speed.method = speed.method, constant.speed = constant.speed, max.speed = max.speed,
                 vedba.model = vedba.model, depth.rate.min.pitch = depth.rate.min.pitch,
                 rest.quantile = rest.quantile, vpc.method = vpc.method, vpc.weighting = vpc.weighting,
                 drift.rate = drift.rate, drift.diffusion = drift.diffusion,
                 anchor.error.radii = anchor.error.radii, include.depth = include.depth,
                 reconstructability.min = reconstructability.min),
            class = "nautilus_reconstruct_track")
}


#' Coerce a control argument (object, named list, or NULL) to its validated control object.
#' @keywords internal
#' @noRd
.as_control <- function(x, constructor, cls, arg) {
  if (is.null(x)) return(constructor())
  if (inherits(x, cls)) return(x)
  if (is.list(x)) {
    unknown <- setdiff(names(x), names(formals(constructor)))
    if (length(unknown)) .abort(c("{.arg {arg}} has unknown field{?s} {.val {unknown}}.",
                                          "i" = "Valid fields: {.val {names(formals(constructor))}}."))
    return(do.call(constructor, x))
  }
  .abort("{.arg {arg}} must be created with {.fn {deparse(substitute(constructor))}} (or a named list of its fields).")
}


#' Configure dive detection and phase classification
#'
#' @description
#' Creates a validated control object for [detectDives()]. Settings define the depth reference,
#' excursion direction, hysteresis thresholds, interruption handling and classification of
#' descent, bottom and ascent phases. The same object also supplies criteria used by
#' [diveMetrics()] through the detection settings recorded in deployment processing history.
#'
#' Numerical settings left unspecified are resolved by [detectDives()] from its input batch,
#' except \code{min.prominence}, for which \code{NULL} disables splitting. The constructor
#' validates supplied values but does not inspect data or derive thresholds.
#'
#' @param reference Character. Depth reference: \code{"auto"} (default), \code{"surface"} or
#'   \code{"baseline"}. Surface uses zero metres; baseline uses a centred running depth level.
#'   Automatic selection is deployment-specific; see Details.
#' @param direction Character. Excursion direction relative to the reference: \code{"down"}
#'   (default), \code{"up"} or \code{"both"}. Depth is assumed positive downwards.
#' @param depth.threshold Positive numeric entry threshold in metres from the reference.
#'   An excursion opens only when its signed departure exceeds this value.
#'   \code{NULL} (default) derives a shared threshold from depth-correction residuals.
#' @param surface.band Non-negative numeric return threshold in metres from the reference,
#'   despite its name also used with a running baseline. An excursion closes when its signed
#'   departure falls below this value. Must be smaller than \code{depth.threshold} when both
#'   are supplied. \code{NULL} (default) derives a shared band; see Details.
#' @param min.amplitude Non-negative numeric minimum peak departure from the reference, in
#'   metres, for a retained interval. Applied after interruption and prominence splitting,
#'   including to fragments that do not independently cross the entry threshold.
#'   \code{NULL} (default) uses \code{depth.threshold - surface.band}.
#' @param min.prominence Non-negative numeric threshold in metres for splitting sub-peaks
#'   within a candidate excursion. A split requires the smaller of the two adjacent peak
#'   heights to exceed their intervening saddle by at least this amount.
#'   \code{NULL} (default) or \code{0} disables splitting; a positive value opts in.
#'   This criterion differs from the endpoint-relative \code{prominence_m} returned by
#'   [diveMetrics()].
#' @param min.duration Non-negative numeric minimum retained duration in seconds, measured
#'   between the first and last samples of an interval. \code{NULL} (default) derives a
#'   shared duration floor from sampling intervals and known downsampling bin widths.
#' @param baseline.window Positive numeric full window width in hours for a running baseline.
#'   Default \code{3}. Used only when the resolved reference is \code{"baseline"}.
#'   Choose a span appropriate to the duration of excursions and changes in the background depth.
#' @param baseline.stat Character. Running baseline statistic: \code{"median"} (default) or
#'   \code{"quantile"}. Neither is appropriate for all excursion patterns; see Details.
#' @param baseline.quantile Numeric probability strictly between zero and one, used only with
#'   \code{baseline.stat = "quantile"}. \code{NULL} (default) selects \code{0.10} for downward
#'   excursions, \code{0.90} for upward excursions and \code{0.50} for both directions.
#' @param phase.method Character. Phase classification: \code{"vertical.rate"} (default)
#'   identifies transit limbs and sustained pauses using local depth slopes; \code{"prop.depth"}
#'   partitions the profile geometrically around its maximum reference-relative excursion.
#'   See Details for interpretation and limitations.
#' @param phase.window Positive numeric local least-squares slope window in seconds for
#'   \code{phase.method = "vertical.rate"}. \code{NULL} (default) derives the larger of
#'   five seconds and three batch-level sampling intervals. Applied windows depend on each
#'   dive's duration, deployment sampling interval and depth noise; see Details.
#' @param min.phase.duration Positive numeric duration in seconds for which a qualifying pause
#'   must persist before ending a transit limb. \code{NULL} (default) uses twice the resolved
#'   \code{phase.window}. Applied holds are adjusted for each dive's duration and sampling
#'   interval. Used only with \code{phase.method = "vertical.rate"}.
#' @param rate.crit Numeric proportion strictly between zero and one. Multiplies the
#'   limb-specific moving-rate quantile to define a transit criterion. Default \code{0.25}.
#'   A depth-noise-based lower bound is also applied.
#' @param rate.quantile Numeric probability in \code{(0, 1]} defining the moving-rate quantile
#'   separately for each transit limb. Default \code{0.90}. Used only with
#'   \code{phase.method = "vertical.rate"}; it does not change the fixed 90th-percentile
#'   rate summaries reported by [diveMetrics()].
#' @param bottom.prop Numeric proportion strictly between zero and one. Default \code{0.80}.
#'   For \code{"prop.depth"}, the bottom spans the first to last samples at or above this
#'   fraction of the maximum signed excursion. For \code{"vertical.rate"}, it defines the
#'   proximity to the extremum required for a pause to end a transit limb, relative to that
#'   limb's depth range. Increasing it requires closer approach to the extremum; decreasing
#'   it permits a broader bottom region.
#' @param bottom.max.directionality Optional numeric proportion in \code{[0, 1]}. Default
#'   \code{0.60}. For \code{"vertical.rate"}, the candidate bottom is checked for net depth
#'   change relative to its resolved vertical path. Larger ratios indicate predominantly
#'   directional transit. Such intervals are refined towards the extremum and reassessed;
#'   unresolved residence is removed rather than labelled bottom. \code{NULL} disables this
#'   additional check. It does not affect \code{"prop.depth"}; see Details for movement resolution.
#' @param max.gap Non-negative numeric maximum interruption in seconds that a dive may span.
#'   Timestamp jumps and runs of non-finite depth longer than this value split candidate
#'   intervals without interpolation. \code{NULL} (default) uses the larger of 60 seconds
#'   and ten batch-level sampling intervals.
#' @param wiggle.amplitude Non-negative numeric minimum depth reversal in metres counted by
#'   [diveMetrics()] as \code{n_reversals}. Does not split dives or change phase labels.
#'   A value of \code{0} disables reversal counting.
#'   \code{NULL} (default) uses the larger of 0.5 metres and three times the batch median
#'   depth-noise estimate.
#' @param min.surface.occupancy Numeric proportion in \code{[0, 1)}. For automatic reference
#'   selection, the minimum fraction of finite depth samples within \code{surface.band} of
#'   zero required to choose \code{"surface"}. Default \code{0.005}.
#'   Set to zero to use depth-correction provenance alone.
#' @param require.zoc Character. Action when an explicit surface reference is requested but
#'   no usable deployment has depth-correction provenance indicating an anchored zero:
#'   \code{"warn"} (default), \code{"error"} or \code{"ignore"}. This check is batch-level,
#'   not a per-deployment guarantee of an anchored zero; see Details.
#'
#' @details
#' ## Reference selection and baseline assumptions
#'
#' With \code{reference = "auto"}, [detectDives()] chooses a surface reference for a deployment
#' only when its latest depth-drift record has status \code{"applied"},
#' \code{"applied_with_gaps"} or \code{"constant_offset"}, and at least
#' \code{min.surface.occupancy} of its finite depth samples satisfy
#' \code{abs(depth) <= surface.band}. Otherwise it uses a running baseline.
#' Missing correction provenance therefore selects a baseline even if depths approach zero.
#'
#' Explicit \code{reference = "surface"} fixes the reference at zero; it does not perform a
#' zero-offset correction. The \code{require.zoc} check acts only when none of the usable
#' deployments has an anchored correction. In a mixed batch, it does not identify every
#' unanchored deployment. Inspect correction histories before imposing a surface reference
#' across such a batch, or use automatic deployment-specific selection.
#'
#' Running baselines use centred, finite-depth window statistics, evaluated on a grid and
#' interpolated between grid points for efficiency. Window widths are converted to sample
#' counts using the deployment's median sampling interval, so regularly sampled, chronologically
#' ordered records are recommended. A median can shift into excursions occupying much of its
#' window; a directional quantile can be displaced by a trending baseline. Diagnostic warnings
#' flag these risks without automatically changing the estimator or window.
#'
#' ## Derived batch-level settings
#'
#' Unspecified numerical settings are derived once from usable deployments in the call.
#' Let \eqn{r} be the largest available depth-correction residual in metres, \eqn{d} the
#' median of deployment median sampling intervals in seconds, \eqn{L} the largest known
#' downsampling bin width in seconds, and \eqn{n} the median deployment depth-noise estimate
#' in metres. The derivations are:
#'
#' \describe{
#'   \item{Entry threshold}{\eqn{\max(3r, 1)} metres; absent residual information uses
#'     \eqn{r = 0.34}, giving 1.02 metres.}
#'   \item{Return band}{The largest of \eqn{2r}, one tenth of the resolved entry threshold,
#'     and 0.5 metres; absent residual information uses \eqn{r = 0.25} for this calculation.}
#'   \item{Amplitude and duration}{Minimum amplitude is the entry threshold minus the return
#'     band. Minimum duration is \eqn{\max(4L, 4d, 10)} seconds. Unknown bin widths contribute
#'     zero, and an unavailable batch sampling interval uses one second.}
#'   \item{Interruption and reversal criteria}{Maximum gap is \eqn{\max(60, 10d)} seconds.
#'     Reversal amplitude is \eqn{\max(0.5, 3n)} metres, using \eqn{n = 0.1} when unavailable.}
#'   \item{Phase timing}{The nominal slope window is \eqn{\max(5, 3d)} seconds; the nominal
#'     pause duration is twice the resolved slope window.}
#' }
#'
#' A return band at or above a derived entry threshold is replaced by half that threshold,
#' including when only the band was explicitly supplied. Known depth-bin widths are inferred
#' from recorded original and processed sampling rates when processing reduced the rate.
#' The depth smoothing window in [smoothingControl()] conditions vertical-velocity estimation;
#' it is not treated as smoothing of the stored depth channel.
#'
#' These defaults are heuristic starting points, not guarantees of sensor resolution or
#' biological validity. Changing batch membership can change derived values. Specify study
#' criteria explicitly where reproducibility across separate batches is required, and inspect
#' the resolved settings recorded by [detectDives()].
#'
#' ## Vertical-rate phase classification
#'
#' The default method estimates centred local least-squares slopes of the signed
#' reference-relative depth excursion. Opening and return limbs are assessed independently
#' using their own moving-rate quantiles, with criteria bounded below by estimated slope noise.
#' Depth quantisation contributes to the noise estimate. A pause ends a limb only after the
#' profile reaches the region near its extremum defined by \code{bottom.prop}.
#'
#' For an excursion of duration \eqn{T} and deployment sampling interval \eqn{d_i}, the initial
#' slope window is \eqn{\max(\min(W, T/8), 3d_i)}, and the pause hold is
#' \eqn{\max(\min(H, T/4), 2d_i)}, where \eqn{W} and \eqn{H} are the resolved nominal settings.
#' Sampling floors can therefore exceed the duration-based caps for short, sparsely sampled dives.
#' Noisy or coarsely quantised profiles can trigger adaptive window widening. This can lengthen
#' a derived hold; when the hold was explicitly supplied, adaptive widening is limited by it.
#'
#' The interval between the independently detected limbs is a candidate bottom, not proof of
#' residence. It is smoothed over the applied slope window for validation only. The movement
#' deadband is the larger of three times the deployment depth-noise estimate and the smaller
#' limb rate criterion multiplied by that window (with a numerical floor). Depth quantisation
#' contributes to the noise estimate. A vertical path joins endpoints and extrema confirmed
#' by reversals clearing this deadband; forward and reverse paths are averaged for symmetry.
#' Raw sample-to-sample distances are not accumulated. Net changes within the deadband are
#' accepted as unresolved drift; otherwise net change divided by resolved path must not exceed
#' \code{bottom.max.directionality}. The default 0.60 requires at least 20% of resolved movement
#' to oppose the net direction, unless net progress is within the deadband. It is a heuristic
#' kinematic criterion, not a validated behavioural threshold.
#'
#' If the candidate is predominantly directional, its opening boundary (net descent) or closing
#' boundary (net ascent) is moved to the first or last sample within one deadband of the smoothed
#' candidate extremum. The shortened interval is reassessed, preserving a level bottom after a
#' slow approach. If directional progress still dominates, the limbs meet at the observed extremum
#' and bottom is empty. No check bridges non-finite depth or invalid timestamps; the existing
#' limb labels remain in those cases. Dives with unresolved limbs are not refined. Geometric
#' shape labels and their prominence settings are never used to determine phases.
#'
#' A V-shaped excursion can have no bottom phase. Limited sampling or insufficient vertical
#' variation can prevent one or both transit limbs from being resolved. Phase labels describe
#' depth-profile kinematics, not independently verified behavioural states. A rounded V can
#' legitimately retain a brief bottom; there is no rule forcing every V into descent-ascent.
#' The pause hold confirms boundaries, not a minimum duration of the final bottom interval.
#' Slow, directionally drifting working-depth periods can be shortened by this check; assess
#' sensitivity or relax/disable it when such periods belong to the study's bottom definition.
#'
#' ## Geometric phase classification and terminology
#'
#' With \code{phase.method = "prop.depth"}, bottom is the contiguous interval between the first
#' and last samples reaching \code{bottom.prop} times the maximum signed excursion. Samples
#' between these endpoints remain bottom even if the profile crosses back below the criterion.
#' This guarantees a non-empty labelled bottom for a retained profile, not a fixed proportion
#' of its duration, and can label the apex of a V-shaped excursion as bottom without a pause.
#' Transit phases can still be absent, for example when the entry threshold excludes much
#' of a shallow excursion.
#'
#' With either method, descent labels the opening limb away from the reference and ascent the
#' return limb. For \code{direction = "up"}, these are physical ascent and descent, respectively.
#' Bottom denotes the excursion extremum rather than the seabed. Select the reference, direction
#' and phase rule from the study's scientific question and depth-record limitations.
#'
#' @return A named list of class \code{nautilus_dive} containing the validated settings.
#'   Pass it to the \code{control} argument of [detectDives()]. Unspecified settings remain
#'   \code{NULL} until detection resolves them from the input batch.
#'
#' @seealso [detectDives()] for sample-level annotation; [diveMetrics()] for per-dive metrics
#'   and quality diagnostics; [diveShapeControl()] for geometric classification, separate from
#'   detection and phase assignment; [processTagData()] for depth correction and sampling provenance;
#'   [depthDriftControl()] for depth-zero correction settings.
#'
#' @examples
#' # Explicit surface-referenced study criteria (depth zero must already be established)
#' diveControl(
#'   reference = "surface", depth.threshold = 5,
#'   surface.band = 1, min.duration = 20
#' )
#'
#' # Downward excursions relative to a running depth level
#' diveControl(reference = "baseline", direction = "down", depth.threshold = 10)
#'
#' # Upward excursions; use a high quantile to estimate the deeper reference level
#' diveControl(
#'   reference = "baseline", direction = "up",
#'   baseline.stat = "quantile", depth.threshold = 10
#' )
#'
#' # Opt in to splitting distinct sub-peaks within multi-peaked excursions
#' diveControl(depth.threshold = 5, min.prominence = 10)
#' @export

diveControl <- function(reference             = c("auto", "surface", "baseline"),
                        direction             = c("down", "up", "both"),
                        depth.threshold       = NULL,
                        surface.band          = NULL,
                        min.amplitude         = NULL,
                        min.prominence        = NULL,
                        min.duration          = NULL,
                        baseline.window       = 3,
                        baseline.stat         = c("median", "quantile"),
                        baseline.quantile     = NULL,
                        phase.method          = c("vertical.rate", "prop.depth"),
                        phase.window          = NULL,
                        min.phase.duration    = NULL,
                        rate.crit             = 0.25,
                        rate.quantile         = 0.90,
                        bottom.prop           = 0.80,
                        max.gap               = NULL,
                        wiggle.amplitude      = NULL,
                        min.surface.occupancy = 0.005,
                        require.zoc           = c("warn", "error", "ignore"),
                        bottom.max.directionality = 0.60) {

  reference     <- match.arg(reference)
  direction     <- match.arg(direction)
  baseline.stat <- match.arg(baseline.stat)
  phase.method  <- match.arg(phase.method)
  require.zoc   <- match.arg(require.zoc)

  # every tunable is named, defaulted and validated; NULL means "derive and report", never "ignore"
  if (!is.null(depth.threshold))  .assert_number(depth.threshold,  "dive$depth.threshold",  min = 0)
  if (!is.null(surface.band))     .assert_number(surface.band,     "dive$surface.band",     min = 0)
  if (!is.null(min.amplitude))    .assert_number(min.amplitude,    "dive$min.amplitude",    min = 0)
  if (!is.null(min.prominence))   .assert_number(min.prominence,   "dive$min.prominence",   min = 0)
  if (!is.null(min.duration))     .assert_number(min.duration,     "dive$min.duration",     min = 0)
  if (!is.null(max.gap))          .assert_number(max.gap,          "dive$max.gap",          min = 0)
  if (!is.null(wiggle.amplitude)) .assert_number(wiggle.amplitude, "dive$wiggle.amplitude", min = 0)
  if (!is.null(phase.window)) {
    .assert_number(phase.window, "dive$phase.window", min = 0)
    if (phase.window <= 0) .abort("{.arg dive$phase.window} must be greater than zero.")
  }
  if (!is.null(min.phase.duration)) {
    .assert_number(min.phase.duration, "dive$min.phase.duration", min = 0)
    if (min.phase.duration <= 0) .abort("{.arg dive$min.phase.duration} must be greater than zero.")
  }
  .assert_number(baseline.window,       "dive$baseline.window",       min = 0)
  .assert_number(rate.crit,             "dive$rate.crit",             min = 0)
  .assert_number(rate.quantile,         "dive$rate.quantile",         min = 0)
  .assert_number(bottom.prop,           "dive$bottom.prop",           min = 0)
  .assert_number(min.surface.occupancy, "dive$min.surface.occupancy", min = 0)
  .assert_number(bottom.max.directionality, "dive$bottom.max.directionality",
                 min = 0, max = 1, null_ok = TRUE)

  if (baseline.window <= 0) .abort("{.arg dive$baseline.window} must be greater than zero.")
  if (rate.crit <= 0 || rate.crit >= 1)
    .abort("{.arg dive$rate.crit} must be in (0, 1); got {.val {rate.crit}}.")
  if (rate.quantile <= 0 || rate.quantile > 1)
    .abort("{.arg dive$rate.quantile} must be in (0, 1]; got {.val {rate.quantile}}.")
  if (bottom.prop <= 0 || bottom.prop >= 1)
    .abort("{.arg dive$bottom.prop} must be in (0, 1); got {.val {bottom.prop}}.")
  if (min.surface.occupancy < 0 || min.surface.occupancy >= 1)
    .abort("{.arg dive$min.surface.occupancy} must be in [0, 1); got {.val {min.surface.occupancy}}.")
  if (!is.null(baseline.quantile)) {
    .assert_number(baseline.quantile, "dive$baseline.quantile", min = 0)
    if (baseline.quantile <= 0 || baseline.quantile >= 1)
      .abort("{.arg dive$baseline.quantile} must be in (0, 1); got {.val {baseline.quantile}}.")
  }
  if (!is.null(depth.threshold) && depth.threshold <= 0)
    .abort("{.arg dive$depth.threshold} must be greater than zero.")

  # cross-field: hysteresis is the whole point, so a band at or above the threshold is meaningless
  if (!is.null(depth.threshold) && !is.null(surface.band) && surface.band >= depth.threshold)
    .abort(c("{.arg dive$surface.band} ({.val {surface.band}}) must be BELOW {.arg dive$depth.threshold} ({.val {depth.threshold}}).",
             "i" = "The band is where a dive ENDS; at or above the threshold a dive could never end."))
  # NOTE: min.prominence deliberately MAY exceed depth.threshold. It used to be forbidden, which -
  # combined with the fact that a run only exists because the residual passed depth.threshold - made
  # the prominence test true by construction and unable to reject anything. It is now the rule that
  # SPLITS a run at an interior saddle, and a value above the threshold is the meaningful way to say
  # "never split": no saddle can confer more prominence than the excursion's own depth.

  structure(list(reference = reference, direction = direction,
                 depth.threshold = depth.threshold, surface.band = surface.band,
                 min.amplitude = min.amplitude, min.prominence = min.prominence,
                 min.duration = min.duration,
                 baseline.window = baseline.window, baseline.stat = baseline.stat,
                 baseline.quantile = baseline.quantile,
                 phase.method = phase.method, phase.window = phase.window,
                 min.phase.duration = min.phase.duration,
                 rate.crit = rate.crit, rate.quantile = rate.quantile,
                 bottom.prop = bottom.prop, max.gap = max.gap, wiggle.amplitude = wiggle.amplitude,
                 min.surface.occupancy = min.surface.occupancy, require.zoc = require.zoc,
                 bottom.max.directionality = bottom.max.directionality),
            class = "nautilus_dive")
}


#' Configure geometric dive-shape classification
#'
#' @description
#' Creates a validated control object for optional V-, U- and W-shaped profile classification
#' in [diveMetrics()]. Rules use time-weighted profile broadness and significant internal
#' excursions. Whole-profile rules are independent of phase labels; optional bottom-scoped
#' W classification uses the annotations assigned by [detectDives()].
#'
#' Classes describe the geometry of the retained depth record, not feeding, resting or transit
#' behaviour. Default thresholds are heuristic starting points and require validation for the
#' study system, sampling resolution and dive definition.
#'
#' @param v.max.broadness Numeric in \code{[0, 1]}. Maximum normalised profile area classified
#'   as V when W is not supported. Default \code{0.60}. Must be smaller than
#'   \code{u.min.broadness}; the interval between the thresholds is deliberately unclassified.
#' @param u.min.broadness Numeric in \code{[0, 1]}. Minimum normalised profile area classified
#'   as U when W is not supported. Default \code{0.75}.
#' @param peak.prominence Positive numeric proportion no larger than one. Default \code{0.10}.
#'   Required rise and subsequent fall of a peak, as a fraction of the prepared profile's
#'   maximum departure from its endpoint chord. Absolute and resolution-based floors also
#'   apply. This hysteretic rise/fall criterion is not a general topographic-prominence estimator.
#' @param peak.prominence.cap Optional positive numeric cap in metres on the proportional
#'   component of the peak criterion. \code{NULL} (default) preserves uncapped proportions.
#'   Absolute and instrument-resolution floors are applied after the cap and may exceed it.
#'   Must not be smaller than \code{min.peak.amplitude}. It does not exclude larger reversals.
#' @param peak.scope Character. \code{"profile"} (default) counts significant peaks anywhere
#'   in the retained profile, independently of phase labels. \code{"bottom"} restricts W
#'   detection to peaks and their intervening valleys in one contiguous labelled bottom interval.
#'   Whole-dive preparation, amplitude and V/U broadness are unchanged. Invalid or unresolved
#'   descent-bottom-ascent annotations cause abstention, not a forced V; see Details.
#' @param min.peak.amplitude Non-negative numeric absolute floor for peak rise/fall in metres.
#'   Default \code{0.5}. Excursion relief and both observed limbs must also clear the effective
#'   resolution floor. Setting zero removes this explicit floor, not the estimated noise floor.
#' @param min.peak.separation Non-negative numeric minimum separation between peak times in
#'   seconds. Default \code{5}. The applied separation is at least two median within-dive sampling
#'   intervals. Conflicting adjacent peaks are resolved in favour of the taller peak.
#' @param smooth.window Non-negative numeric full width in seconds of a centred time-weighted
#'   box average applied only during classification. Default \code{3}; zero disables smoothing.
#'   A window exceeding one quarter of a dive's duration causes abstention rather than being
#'   silently shortened. Original depth and phase annotations are unchanged.
#' @param min.coverage Numeric proportion in \code{[0, 1]}. Minimum fraction of dive samples
#'   with finite depth. Default \code{0.95}. Coverage alone does not establish completeness:
#'   missing endpoints, long gaps and censored dives cause abstention independently.
#' @param max.gap Optional positive numeric maximum span in seconds between finite depth
#'   observations. Longer timestamp or missing-depth gaps cause abstention. \code{NULL}
#'   (default) uses the larger of five seconds and three median within-dive sampling intervals.
#'   Short, bracketed gaps are linearly interpolated for classification only, never extrapolated.
#' @param min.samples Integer of at least five. Minimum number of finite depth observations
#'   required per dive. Default \code{20}.
#' @param min.limb.prop Positive numeric proportion no larger than \code{0.5}. Minimum observed
#'   rise from the opening sample to the extremum and fall to the closing sample, each relative
#'   to the smoothed excursion range. Default \code{0.20}. Both limbs must also clear the
#'   resolution floor. This guards against artificial shapes created by endpoint detrending.
#' @param max.opposite.prop Numeric proportion in \code{[0, 0.5]}. Maximum tolerated opposing
#'   departure as a fraction of the dominant departure when inferring direction, and of the
#'   prepared height when checking movement below the endpoint chord. Default \code{0.20}.
#'   The effective tolerance is at least the resolution floor. Larger opposing departures
#'   cause direction abstention or an \code{"other"} complex-profile classification.
#' @param min.excursion.amplitude Optional non-negative numeric minimum prepared profile
#'   height in metres for shape classification. \code{NULL} (default) or zero disables this
#'   additional eligibility threshold; technical resolution requirements still apply.
#'   Height is the maximum positive departure of the smoothed, direction-oriented,
#'   reference-relative profile from the line joining its endpoints. Equality is eligible.
#'   Profiles below the threshold receive \code{NA} with status \code{"below_min_amplitude"};
#'   dive rows, other metrics, source depth and detection boundaries are retained.
#'
#' @details
#' ## Profile preparation and broadness
#'
#' Classification starts from depth minus \code{depth_baseline}. Upward excursions are inverted
#' so that movement away from the reference is positive. With detection direction \code{"both"},
#' the actual direction is inferred from the dominant signed departure; substantial departures
#' on both sides cause abstention. Without detection direction metadata, direction is inferred
#' from departure from the line joining the endpoints. Absolute values are not used to fold
#' upward and downward movements into one profile.
#'
#' Smoothing integrates a piecewise-linear profile over actual timestamps, with shorter windows
#' at the endpoints rather than zero padding. After checking that both limbs were observed,
#' a straight line between the smoothed endpoints is subtracted. Small negative departures
#' are clipped to zero; substantial departures below that line are reported as a complex profile.
#' The remaining profile is divided by its maximum height and integrated over elapsed time.
#' Broadness is that area divided by duration, on \code{[0, 1]}. An ideal unsmoothed triangle
#' has broadness 0.5; increasing time around the extremum generally increases this measure.
#' It is not the published time-allocation-at-depth index or a proportion of phase-labelled samples.
#'
#' ## Significant peaks and decision rules
#'
#' The proportional component is \code{peak.prominence} times the prepared profile height,
#' optionally limited by \code{peak.prominence.cap}. The peak criterion in metres is the largest
#' of this component, \code{min.peak.amplitude}, three times the deployment depth-noise estimate
#' and twice the estimated depth quantum. Noise is estimated by the median absolute deviation
#' of finite second differences divided by \eqn{\sqrt{6}}; the quantum estimate is used only
#' when a sufficiently populated depth lattice is detected.
#'
#' Peaks must have both a rise and a subsequent fall meeting this criterion. Sub-threshold
#' oscillations are suppressed by hysteresis; flat summits use their temporal midpoint.
#' After scope and separation screening, two or more significant peaks define W. Otherwise
#' V or U follows the broadness thresholds; intermediate profiles are
#' \code{"other"}. These operational definitions are not a universal taxonomic standard.
#'
#' ## Quality requirements and scope
#'
#' Censored, noncontiguous, insufficiently sampled, poorly covered or insufficiently resolved
#' profiles receive \code{NA} with a diagnostic status. Timestamps must be finite and strictly
#' increasing, and the reference must be finite. Even with complete coverage, an unresolved
#' opening or return limb causes abstention. There is no forced V/U/W assignment or estimated
#' probability of class membership.
#'
#' ## Bottom-scoped W classification
#'
#' With \code{peak.scope = "bottom"}, significant peaks are first detected over the full
#' prepared profile. Only peaks whose centres are labelled bottom are retained, before temporal
#' separation is applied. Phase runs must be either descent-ascent (a known empty bottom) or
#' descent-bottom-ascent with one contiguous bottom. This ensures the entire interval between
#' retained peak centres, including their valley, belongs to bottom. External limbs may still
#' establish the prominence of peaks at the bottom boundaries. Bottom is not cropped or renormalised.
#'
#' Two or more retained peaks define W. With zero or one bottom peak, V/U/other follows whole-dive
#' broadness; no bottom does not automatically imply V. Missing, invalid, noncontiguous or unresolved
#' phase structure gives \code{NA} with \code{"unresolved_phases"}. The main full-profile peak
#' must still be resolved. Upward excursions use the same logical opening-bottom-return labels.
#' Bottom-scoped results depend on the supplied phase method and settings. Rerun [detectDives()]
#' to use revised phase rules; [diveMetrics()] never repairs or overwrites old annotations.
#' A bottom W is geometric evidence of repeated vertical movement, not proof of searching or feeding.
#'
#' \code{min.excursion.amplitude} optionally limits shape classification to a study-specific
#' vertical scale, independently of the internal-peak criterion \code{min.peak.amplitude}.
#' It is checked after technical resolution and observed-limb requirements, but before
#' broadness and peaks are calculated. It is not an absolute-depth cutoff, the raw depth
#' range, or the \code{amplitude_m} returned by [diveMetrics()]. Unlike
#' \code{diveControl(min.amplitude = ...)}, it does not remove detected dives.
#' Small excursions may be genuine behaviour; withholding their shape is an analytical
#' eligibility decision, not evidence of sensor failure. There is no universal ecological
#' amplitude threshold. Select one using reviewed profiles, instrument resolution and the
#' study question, and report unclassified counts alongside shape proportions.
#'
#' Rules classify the intervals retained by [detectDives()], not an inferred full dive.
#' Detection thresholds crop the profile, and downsampling or smoothing can remove narrow
#' peaks. In particular, prominence splitting during detection can turn one W-shaped excursion
#' into separate dives; classification does not merge them. Review real profiles and threshold
#' sensitivity before using the labels in scientific analyses.
#'
#' @return A named list of class \code{nautilus_dive_shape} containing validated settings.
#'   Pass it to the \code{shape} argument of [diveMetrics()]. Results include the labels,
#'   statuses, broadness, significant peak count and effective prominence threshold.
#'
#' @seealso [diveMetrics()] for optional classification and output definitions;
#'   [diveControl()] for excursion detection and phase settings; [detectDives()] for
#'   sample-level annotations; [plotDepthProfiles()] for inspection of the source records.
#'
#' @examples
#' diveShapeControl()
#'
#' # Require broader U-shaped profiles and larger internal excursions
#' diveShapeControl(u.min.broadness = 0.80, peak.prominence = 0.20)
#'
#' # Explicit time and amplitude scales for a study
#' diveShapeControl(
#'   min.peak.amplitude = 1, min.peak.separation = 10,
#'   smooth.window = 2, max.gap = 5
#' )
#'
#' # An illustrative study-specific eligibility threshold, not a universal recommendation
#' diveShapeControl(min.excursion.amplitude = 10)
#'
#' # Illustrative deep-dive rule: validate the cap and bottom annotations for the study
#' diveShapeControl(peak.scope = "bottom", peak.prominence = 0.10, peak.prominence.cap = 50)
#' @export
diveShapeControl <- function(v.max.broadness = 0.60,
                             u.min.broadness = 0.75,
                             peak.prominence = 0.10,
                             min.peak.amplitude = 0.5,
                             min.peak.separation = 5,
                             smooth.window = 3,
                             min.coverage = 0.95,
                             max.gap = NULL,
                             min.samples = 20L,
                             min.limb.prop = 0.20,
                             max.opposite.prop = 0.20,
                             min.excursion.amplitude = NULL,
                             peak.prominence.cap = NULL,
                             peak.scope = c("profile", "bottom")) {
  peak.scope <- match.arg(peak.scope)
  .assert_number(v.max.broadness, "shape$v.max.broadness", min = 0, max = 1)
  .assert_number(u.min.broadness, "shape$u.min.broadness", min = 0, max = 1)
  if (v.max.broadness >= u.min.broadness)
    .abort("{.arg shape$v.max.broadness} must be smaller than {.arg shape$u.min.broadness}.")
  .assert_number(peak.prominence, "shape$peak.prominence", min = 0, max = 1)
  if (peak.prominence <= 0) .abort("{.arg shape$peak.prominence} must be greater than zero.")
  .assert_number(min.peak.amplitude, "shape$min.peak.amplitude", min = 0)
  .assert_number(min.peak.separation, "shape$min.peak.separation", min = 0)
  .assert_number(smooth.window, "shape$smooth.window", min = 0)
  .assert_number(min.coverage, "shape$min.coverage", min = 0, max = 1)
  .assert_number(max.gap, "shape$max.gap", min = 0, null_ok = TRUE)
  if (!is.null(max.gap) && max.gap <= 0) .abort("{.arg shape$max.gap} must be greater than zero.")
  .assert_count(min.samples, "shape$min.samples", min = 5L)
  .assert_number(min.limb.prop, "shape$min.limb.prop", min = 0, max = 0.5)
  if (min.limb.prop <= 0) .abort("{.arg shape$min.limb.prop} must be greater than zero.")
  .assert_number(max.opposite.prop, "shape$max.opposite.prop", min = 0, max = 0.5)
  .assert_number(min.excursion.amplitude, "shape$min.excursion.amplitude", min = 0, null_ok = TRUE)
  .assert_number(peak.prominence.cap, "shape$peak.prominence.cap", min = 0, null_ok = TRUE)
  if (!is.null(peak.prominence.cap) && peak.prominence.cap <= 0)
    .abort("{.arg shape$peak.prominence.cap} must be greater than zero.")
  if (!is.null(peak.prominence.cap) && peak.prominence.cap < min.peak.amplitude)
    .abort("{.arg shape$peak.prominence.cap} must not be below the {.arg min.peak.amplitude} floor.")
  structure(list(v.max.broadness = v.max.broadness, u.min.broadness = u.min.broadness,
                 peak.prominence = peak.prominence, min.peak.amplitude = min.peak.amplitude,
                 min.peak.separation = min.peak.separation, smooth.window = smooth.window,
                 min.coverage = min.coverage, max.gap = max.gap,
                 min.samples = as.integer(min.samples), min.limb.prop = min.limb.prop,
                 max.opposite.prop = max.opposite.prop,
                 min.excursion.amplitude = min.excursion.amplitude,
                 peak.prominence.cap = peak.prominence.cap, peak.scope = peak.scope),
            class = "nautilus_dive_shape")
}
