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
#' @param speed Window (s) for derived speed/velocity. Default 1.
#' @return A validated `nautilus_smoothing` object for the `smoothing` argument of \code{\link{processTagData}}.
#' @seealso \code{\link{processTagData}}, \code{\link{calibrationControl}}
#' @examples
#' smoothingControl(depth = 15, dba = NULL)   # 15 s depth window; disable DBA post-smoothing
#' @export
smoothingControl <- function(static = 3, orientation = 1, dba = 2, depth = 10, speed = 1) {
  fields <- list(static = static, orientation = orientation, dba = dba, depth = depth, speed = speed)
  for (nm in names(fields)) if (!is.null(fields[[nm]])) .assert_number(fields[[nm]], paste0("smoothing$", nm), min = 0)
  if (is.null(fields$static) || fields$static <= 0)
    .abort("{.arg smoothing$static} must be a positive number (the gravity-separation window cannot be disabled).")
  structure(fields, class = "nautilus_smoothing")
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
#' @param hard.iron Whether to correct the fixed offset that magnetised components inside the tag add to
#'   the field (default `TRUE`). This is the dominant distortion and the one heading depends on; there is
#'   rarely a reason to disable it.
#' @param soft.iron Whether to correct the per-axis scaling that ferromagnetic material imposes on the
#'   field (default `TRUE`). Only the axis-aligned scales are fitted here, not the cross-axis shear.
#'   Disable it if a deployment's orientation coverage is so poor that the scales are being fitted to
#'   noise.
#' @param use.stored Whether to prefer a calibration already stored in the metadata by
#'   [calibrateMagnetometer()], such as a fit pooled across every deployment of one tag (default
#'   `TRUE`). A stored fit is used only when it clears the confidence gate; a low-confidence one is
#'   ignored and the inline per-deployment estimate is used instead. Set `FALSE` to always estimate
#'   inline, for instance when comparing the two.
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


#' Control settings for a fetched raster basemap (\code{basemap = "satellite"})
#'
#' @description Tunes how a raster basemap canvas is fetched and cached by \code{\link{plotTracks}} /
#' \code{\link{filterLocations}} when \code{basemap = "satellite"}. It governs only the \emph{auto-fetch}
#' path; a pre-fetched raster passed directly as \code{basemap} (see \code{\link{getBasemap}}) ignores it.
#' Tile zoom and grid resolution are deliberately not exposed -- both are derived from the map extent, and
#' exact control is available by pre-fetching a raster and passing it in.
#'
#' @param provider Character. The \pkg{maptiles} tile provider id. Default `"Esri.WorldImagery"` (satellite
#'   imagery). Any provider \pkg{maptiles} knows works (e.g. `"Esri.WorldTopoMap"`, `"OpenStreetMap"`).
#' @param cache Cache fetched tiles for offline reuse: `TRUE` (default, a persistent per-user cache via
#'   \code{\link[tools]{R_user_dir}}), `FALSE` (session temp dir only), or a directory path.
#' @return A `nautilus_basemap` control object.
#' @seealso \code{\link{plotTracks}}, \code{\link{getBasemap}}
#' @examples
#' basemapControl(provider = "Esri.WorldTopoMap")
#' @export
basemapControl <- function(provider = "Esri.WorldImagery", cache = TRUE) {
  .assert_string(provider, "provider")
  if (!(isTRUE(cache) || isFALSE(cache) || (is.character(cache) && length(cache) == 1L && !is.na(cache))))
    .abort("{.arg cache} must be {.code TRUE}, {.code FALSE}, or a single directory path.")
  structure(list(provider = provider, cache = cache), class = "nautilus_basemap")
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
#' `center.warn` and `center.reject` apply only when the fit comes from an external `calibration.data`
#' source.
#'
#' @param method How to fit the distortion. `"ellipsoid"` (default) fits the full hard-iron and
#'   soft-iron ellipsoid where the data genuinely determine it, and otherwise falls back to a
#'   hard-iron-only fit that still corrects heading. `"diagonal"` forces a per-axis offset-and-scale fit,
#'   which is worth trying when the full ellipsoid is unstable.
#' @param igrf.normalize When pooling deployments of one tag, whether to rescale each deployment's field
#'   to the expected geomagnetic intensity at its location before pooling (default `TRUE`), so that
#'   deployments from different survey areas do not distort the shared soft iron. Ignored for a single
#'   deployment with no coordinates.
#' @param min.coverage Minimum per-axis coverage, as a fraction of the sphere radius, for the fit to be
#'   trusted (default `0.5`). Below this the animal did not turn through enough orientations. Lower it
#'   only if you are prepared to accept a fit resting on a narrow slice of the sphere.
#' @param cond.max Largest ellipsoid axis ratio still accepted as a real ellipsoid before falling back
#'   to the diagonal fit (default `25`). A very elongated ellipsoid usually means the cloud is a band
#'   being over-fitted rather than a genuinely distorted sphere.
#' @param radcv.max Coefficient of variation of the corrected radius at or below which a fit earns high
#'   confidence (default `0.1`; zero would be a perfect sphere). This is the sphericity test: how tightly
#'   the corrected field sits on one sphere.
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
#'   when a calibration is fitted from `calibration.data`. A disagreement above `center.reject` (default
#'   `0.35`) rejects the source; above `center.warn` (default `0.10`) the source is kept but its
#'   confidence is downgraded. This cross-check exists because a fixed magnetic mass that co-rotated with
#'   the tag during the calibration spin - vessel steel, most often - is absorbed into the centre and
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
#' @param target.field Optional override for the target field magnitude, in \eqn{\mu}T. By default the
#'   expected geomagnetic intensity is used where coordinates are available, and the cloud's own median
#'   radius otherwise. Set it if you know the local field and the deployment has no position.
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
#' @param sensor.resolution The smallest change the channel can express - its quantisation step. It stops
#'   ordinary rounding from registering as a rate of change, which otherwise makes a coarsely-quantised
#'   channel look full of spikes.
#'
#'   Required, with no default, because resolution is a property of a particular instrument and channel
#'   and the package has no basis for guessing it: a value suited to depth in metres is an order of
#'   magnitude too coarse for temperature in degrees. Take it from the tag's specification, or from the
#'   smallest non-zero difference between consecutive raw readings.
#' @param sensor.accuracy.fixed,sensor.accuracy.percent The sensor's stated accuracy, as a fixed value in
#'   the channel's units or as a percentage of the reading. Supply at most one. Recorded with the results
#'   for provenance; detection itself uses `sensor.resolution`. Defaults `NULL`.
#' @param outlier.window How close together, in minutes, outliers must fall to be treated as one
#'   malfunction period rather than as separate spikes. Default 5. Widen it where a failing sensor
#'   glitches intermittently over a longer stretch.
#' @param stall.threshold How long a run of identical, non-zero readings must last, in minutes, before
#'   the sensor is judged to have stalled. Default 5. Raise it for a channel that legitimately holds
#'   steady - a temperature record from an animal resting in a thermally uniform layer, for instance.
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
#'   static-acceleration magnitude from 1 g (in g). A small departure suggests an imperfect calibration
#'   (warning); a large one is a scaling or unit error - e.g. acceleration left in m/s^2 - rather than a
#'   calibration offset (error). Defaults 0.20 and 0.50.
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
                             accel.scale.warning      = 0.20,
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
#' Selects which movement-path metrics [trackMetrics()] computes and the sizes of the rolling windows
#' behind its temporal tortuosity columns, so the main call stays uncluttered.
#'
#' @param metrics Which metrics to compute: any of `"path_ratio"`, `"sinuosity"`, `"turning_angle"` and
#'   `"straightness"`, or `"all"` (the default). Narrow it when you only need one or two; the
#'   local-turning metrics are the more expensive to compute on a long track.
#' @param min.points The fewest valid positions a track needs before it is summarised at all; shorter
#'   tracks are skipped. Default `5`. Raise it if a handful of positions is not enough for the
#'   comparison you intend, since a two-point "path" is straight by construction.
#' @param hourly.window.h,daily.window.h The window lengths in hours behind the `Hourly_tortuosity` and
#'   `Daily_tortuosity` columns, each the mean path-to-displacement ratio over rolling windows of that
#'   length. Defaults `1` and `24`. Choose them to bracket the timescales your animal's behaviour
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
#'   speed. Default `0`, which judges every segment, the position record having already dropped
#'   duplicate timestamps. Raise it if your tag reports bursts of near-simultaneous fixes.
#' @param max.iterations The most removal passes to make. Each pass removes the single most egregious
#'   spike and recomputes speeds against the new neighbours, and the loop stops early once no fix is
#'   implausible. Default `50`. It is a runaway guard rather than a tuning knob; reaching it usually
#'   means the threshold is too tight for the data.
#' @param spike.angle An optional direction-reversal test, in degrees between 90 and 180, that
#'   supplements the speed test: an interior fix is also treated as a spike when the track's heading
#'   reverses by at least this much there *and* at least one adjoining segment exceeds
#'   `max.speed.kmh`. It catches the sharp out-and-back spikes that travel slowly enough to pass the
#'   speed test alone. `NULL` (default) disables it; around 160 degrees is a reasonable starting point.
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


#' What counts as a dive in detectDives()
#'
#' @description
#' Bundles the settings for [detectDives()] into one validated object, so the main call stays
#' uncluttered.
#'
#' A dive is a vertical excursion of the depth trace away from a reference level, detected by
#' two-threshold hysteresis with a prominence criterion and ended by a return to within a band of that
#' reference. Three axes make that one definition serve every taxon, and they are the only concessions
#' to taxonomy:
#'
#' - `reference` decides where "not diving" sits. `"surface"` suits air-breathers, whose zero is
#'   anchored by surfacing; `"baseline"` tracks a running level and suits fish that never surface, or
#'   benthic animals that rest at depth; `"auto"` chooses from the depth-drift provenance and reports
#'   which it picked.
#' - `direction` decides which way the animal departs from it: `"down"` for animals that excurse
#'   downward from a shallow level, `"up"` for benthic resters leaving the bottom, `"both"` for either.
#' - the hysteresis pair `depth.threshold` and `surface.band` set the scale of an excursion.
#'
#' Hysteresis is not optional. With a single threshold, sensor noise at the crossing splits one dive
#' into many, and the dive count becomes a property of the pressure transducer rather than of the
#' animal.
#'
#' @param reference Where "not diving" sits: `"auto"` (default), `"surface"` or `"baseline"`. See the
#'   Details for how `"auto"` decides.
#' @param direction Which direction an excursion runs: `"down"` (default), `"up"` or `"both"`.
#' @param depth.threshold How far past the reference, in metres, an excursion must go to count as a
#'   dive. `NULL` (default) derives a floor from the record and reports it - the smallest excursion the
#'   data can support, which is not an estimate of what the animal treats as a dive. Set it from your
#'   study system.
#' @param surface.band How close to the reference, in metres, depth must return before a dive is
#'   considered over. Must be less than `depth.threshold`, which is checked when you set both; if you
#'   set only the band and let the threshold derive, a band that lands at or above the derived
#'   threshold is replaced by half of it. `NULL` derives it as the largest of twice the
#'   zero-offset residual, a tenth of `depth.threshold`, and 0.5 m, so that it scales with the dive and
#'   not only with the uncertainty of the zero. The band answers "has the animal returned?" rather than
#'   "how well do we know the zero?", and one derived from the residual alone can be too tight ever to
#'   close - leaving a deep dive and the shallow oscillation that follows it merged into a single very
#'   long dive.
#' @param min.amplitude How far, in metres, a candidate must depart from the reference to count as a
#'   dive at all. A run opened by hysteresis has already cleared `depth.threshold`, so this bites on the
#'   fragments a split leaves behind: cut a 20 m dive with a depth dropout and the piece resuming at 4 m
#'   is still one run, but it is not a 20 m dive. `NULL` derives `depth.threshold - surface.band`.
#' @param min.prominence How far, in metres, a secondary peak must rise above the saddle separating it
#'   from its neighbour before it is treated as a dive in its own right. This is topographic prominence:
#'   an excursion to 50 m that returns only to 15 m and descends again to 48 m never re-enters the
#'   surface band, so hysteresis alone reports one dive, but the second peak stands 33 m above the
#'   saddle, and whether that is one dive or two is exactly what this argument decides.
#'
#'   `NULL` (the default) never splits: the excursion is reported whole, however many sub-peaks it
#'   contains. That is deliberate. Splitting is an interpretive act, and a deep excursion with a partial
#'   ascent in the middle may be precisely what the animal did - the same reasoning that stops this
#'   package imposing a maximum dive duration. A derived default is a poor substitute here, because a
#'   derived threshold is a record-resolution floor, and a re-ascent of half a metre inside a 50 m dive
#'   is not a second dive. Set a number from your study system to opt in. Either way the prominence
#'   itself is reported for every dive as `prominence_m` by [diveMetrics()], so you can see what
#'   splitting would do before choosing to do it.
#' @param min.duration The shortest measurable dive, in seconds. `NULL` derives a floor of four times
#'   the coarser of the downsampling bin width and the median sampling interval, with a lower bound of
#'   10 s.
#'   Bin-averaging attenuates any excursion short relative to a bin, so this floor tracks the resolution
#'   the record actually has - see the Details of [detectDives()].
#' @param baseline.window The window, in hours, over which the running baseline is computed. Default
#'   `3`. Shorten it if the baseline genuinely moves within a day; lengthen it if excursions are long
#'   enough to drag the baseline after them.
#' @param baseline.stat How the running baseline is estimated: `"median"` (default) or `"quantile"`.
#'   These have complementary failure modes, described in the Details.
#' @param baseline.quantile Which quantile to use when `baseline.stat = "quantile"`. `NULL` picks 0.10,
#'   0.90 or 0.50 according to `direction`.
#' @param phase.method How descent, bottom and ascent are separated: `"vertical.rate"` (default), which
#'   ends a phase when the animal stops descending or ascending briskly, or `"prop.depth"`, which
#'   defines the bottom geometrically as everything below a proportion of the dive's amplitude.
#' @param rate.crit,rate.quantile The vertical-rate phase rule: a phase ends when the rate falls below
#'   `rate.crit` times the dive's `rate.quantile` quantile of vertical rate. Defaults `0.25` and `0.90`.
#'   A quantile rather than the maximum, because the maximum of a smoothed series is an artefact of the
#'   smoothing window.
#' @param bottom.prop For `phase.method = "prop.depth"`: the bottom phase is the span deeper than this
#'   proportion of the dive's amplitude. Default `0.80`.
#' @param max.gap The longest interruption of the record, in seconds, that a single dive may span. An
#'   interruption is either a jump in time between consecutive samples or a run of samples carrying no
#'   finite depth - both mean the record stopped saying where the animal was. A longer one splits the
#'   dive and marks both parts censored; nothing is interpolated across it. `NULL` derives the larger of
#'   60 s and ten median sampling intervals, once per cohort, so that gap handling stays comparable
#'   between deployments.
#' @param wiggle.amplitude The smallest reversal within a dive, in metres, that counts as a wiggle and
#'   so contributes to the `n_reversals` [diveMetrics()] reports. `NULL` uses the larger of 0.5 m and
#'   three times the noise of the stored series, so that sensor noise is not read as behaviour. Raise it
#'   if you only want substantial within-dive excursions counted; lowering it below the noise floor
#'   counts the instrument rather than the animal.
#' @param min.surface.occupancy For `reference = "auto"`: the minimum fraction of samples that must fall
#'   within the surface band before `"surface"` is chosen, even where the zero is anchored. Default
#'   `0.005`. An anchored zero the animal never returns to cannot referee a surface threshold, and the
#'   result would be one dive spanning the whole record. Raise it to demand more convincing evidence of
#'   surfacing; set it to `0` to decide on the zero-offset provenance alone.
#' @param require.zoc What to do when `reference = "surface"` is requested but the zero-offset
#'   correction abstained, leaving the surface unanchored: `"warn"` (default), `"error"` or `"ignore"`.
#'
#' @details
#' ## How `reference = "auto"` decides
#'
#' It picks `"surface"` only when the depth-drift provenance record exists with a status of `applied`,
#' `applied_with_gaps` or `constant_offset`, and the record spends at least `min.surface.occupancy` of
#' its samples within the surface band. Otherwise it picks `"baseline"`. The decision and its reason are
#' reported, and it is made per deployment, so a cohort can resolve to a mixture.
#'
#' ## Choosing `baseline.stat`
#'
#' The two estimators fail in opposite regimes, and neither is universally correct. A running **median**
#' tracks a baseline that drifts during the deployment - an animal moving from shelf to slope - but
#' migrates into the excursions once they occupy more than about half the record. A low **quantile** is
#' immune to that duty cycle, but on a trending baseline it tracks the trailing edge of its window
#' rather than the local level. [detectDives()] measures both conditions and warns when the estimator
#' you chose is in its failing regime.
#'
#' @return A validated `nautilus_dive` object for the `control` argument of [detectDives()].
#'
#' @seealso [detectDives()] for the function that consumes it; [diveMetrics()] for the per-dive
#'   summary; [smoothingControl()] for the processing windows it refers to.
#'
#' @examples
#' diveControl(depth.threshold = 5)                             # a 5 m dive, surface-referenced
#' diveControl(reference = "baseline", depth.threshold = 20)    # a fish that never surfaces
#' diveControl(reference = "baseline", direction = "up")        # a benthic rester leaving the bottom
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
                        rate.crit             = 0.25,
                        rate.quantile         = 0.90,
                        bottom.prop           = 0.80,
                        max.gap               = NULL,
                        wiggle.amplitude      = NULL,
                        min.surface.occupancy = 0.005,
                        require.zoc           = c("warn", "error", "ignore")) {

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
  .assert_number(baseline.window,       "dive$baseline.window",       min = 0)
  .assert_number(rate.crit,             "dive$rate.crit",             min = 0)
  .assert_number(rate.quantile,         "dive$rate.quantile",         min = 0)
  .assert_number(bottom.prop,           "dive$bottom.prop",           min = 0)
  .assert_number(min.surface.occupancy, "dive$min.surface.occupancy", min = 0)

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
                 phase.method = phase.method, rate.crit = rate.crit, rate.quantile = rate.quantile,
                 bottom.prop = bottom.prop, max.gap = max.gap, wiggle.amplitude = wiggle.amplitude,
                 min.surface.occupancy = min.surface.occupancy, require.zoc = require.zoc),
            class = "nautilus_dive")
}
