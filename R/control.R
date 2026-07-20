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
#' Groups the magnetometer-calibration switches of \code{\link{processTagData}} into one object. The
#' mounting pitch/roll offset corrections and the orientation-estimator tuning live in
#' \code{\link{orientationControl}}.
#'
#' @param hard.iron Logical. Apply hard-iron (offset) magnetometer calibration. Default TRUE.
#' @param soft.iron Logical. Apply axis-aligned soft-iron (per-axis scale) magnetometer calibration;
#'   cross-axis misalignment terms are not fitted. Default TRUE.
#' @param use.stored Logical. If `TRUE` (default) and a stored magnetometer calibration is present in the
#'   metadata (from \code{\link{calibrateMagnetometer}}, e.g. a per-package pooled fit), apply THAT instead
#'   of estimating one inline - but only when it clears the confidence gate (a low-confidence stored fit is
#'   ignored, falling back to the inline per-tag estimate). Set `FALSE` to always estimate inline.
#' @return A validated `nautilus_calibration` object for the `calibration` argument of \code{\link{processTagData}}.
#' @seealso \code{\link{processTagData}}, \code{\link{calibrateMagnetometer}}, \code{\link{orientationControl}}, \code{\link{smoothingControl}}
#' @examples
#' calibrationControl(soft.iron = FALSE)
#' @export
calibrationControl <- function(hard.iron = TRUE, soft.iron = TRUE, use.stored = TRUE) {
  flags <- list(hard.iron = hard.iron, soft.iron = soft.iron, use.stored = use.stored)
  for (nm in names(flags)) .assert_flag(flags[[nm]], paste0("calibration$", nm))
  structure(flags, class = "nautilus_calibration")
}

#' Control the optional magnetometer hard/soft-iron calibration
#'
#' @description
#' Groups the tuning of \code{\link{calibrateMagnetometer}} into one validated object. Magnetometer
#' calibration from free-swimming field data is under-determined (a near-horizontal swimmer samples a
#' band, not a sphere), so this is a BEST-EFFORT estimate with an explicit heading-confidence flag; the
#' defaults are conservative and honest rather than optimistic.
#'
#' @details
#' The thresholds map onto the stages of the fit in \code{\link{calibrateMagnetometer}} (see its Details).
#' For accepting the full 3D ellipsoid: \code{cond.max} bounds how elongated an ellipsoid is still accepted,
#' \code{igrf.residual.max} is the dip tolerance that BOTH gates 3D soft-iron acceptance and defines "high"
#' confidence, \code{radcv.max} is the sphericity tolerance for "high", and \code{min.coverage} sets when a
#' cloud counts as well covered on every axis. For the hard-iron-only 2D fallback (a thin swimming band),
#' \code{azimuth.min} is the swept-yaw coverage needed to trust the in-plane centre, while
#' \code{planarity.max}, \code{linearity.abort} and \code{extent.min} reject clouds that are not a genuine
#' planar ring (a solid blob, or a single held heading). \code{center.warn} / \code{center.reject} apply only
#' when the fit is taken from an external \code{calibration.data} source.
#'
#' @param method Fit method. \code{"ellipsoid"} (default) fits the full hard-iron + soft-iron ellipsoid
#'   when the data genuinely determine it (well covered on every axis and dip-consistent) and otherwise
#'   falls back to a hard-iron-only fit that still corrects heading (see the Details of
#'   \code{\link{calibrateMagnetometer}}). \code{"diagonal"} forces a per-axis hard-iron + scale fit.
#' @param igrf.normalize Logical. When pooling deployments of one package, rescale each deployment's field
#'   to the IGRF total-field intensity at its location before pooling, so different survey locations do not
#'   distort the shared soft-iron. Default TRUE (ignored for a single deployment with no coordinates).
#' @param min.coverage Minimum per-axis coverage (half-range as a fraction of the sphere radius) for the
#'   fit to be trusted; below this the orientation coverage is too poor. Passed to the robust hard-iron
#'   estimate. Default 0.5.
#' @param cond.max Maximum ellipsoid axis-ratio (condition number) accepted as a real ellipsoid before
#'   falling back to the diagonal fit. Default 25.
#' @param radcv.max Corrected-radius coefficient of variation at/below which the fit is "high" confidence
#'   (0 = a perfect sphere). Default 0.1.
#' @param igrf.residual.max Absolute geomagnetic dip residual (measured minus IGRF inclination, degrees)
#'   at/below which a full 3D fit is "high" confidence. It ALSO gates 3D soft-iron acceptance: a
#'   thin-band ellipsoid whose corrected field exceeds this dip residual has an unconstrained perpendicular
#'   centre, so it is routed to the hard-iron-only 2D fallback instead of applying an untrustworthy
#'   soft-iron. It does NOT gate the 2D fallback's own heading trust, which rests on in-plane yaw coverage
#'   (heading needs the horizontal components, not the vertical dip). Default 15.
#' @param center.warn,center.reject Hard-iron-centre agreement thresholds used ONLY when a calibration is
#'   fit from an external source (see `calibration.data` in \code{\link{calibrateMagnetometer}}). The
#'   source's estimated hard-iron centre is cross-checked against the deployment's own in-situ centre, as a
#'   fraction of the field radius: a disagreement above `center.reject` (default 0.35) rejects the source
#'   bind (a fixed magnetic mass co-rotated with the tag during the calibration, e.g. boat steel - it is
#'   absorbed into the centre yet passes the sphericity/dip QC, so this is the only guard that catches it);
#'   above `center.warn` (default 0.10) the bind is kept but its confidence is downgraded. Ignored for the
#'   default in-situ calibration (no source). `center.reject` must be >= `center.warn`.
#' @param azimuth.min Minimum swept yaw arc (degrees, 0-360) for a hard-iron-only (2D-fallback) fit to earn
#'   "medium" heading confidence. Below this the animal did not turn through enough headings to determine
#'   the in-plane hard-iron centre, so the heading is left uncalibrated. Default 150.
#' @param planarity.max Maximum planarity (smallest / largest PCA eigenvalue of the field cloud) for a
#'   2D-fallback fit to be applied. A genuine swimming band is planar (planarity near 0); a near-stationary
#'   isotropic blob approaches 1 - its full azimuth coverage is only sensor noise - and is rejected. Default 0.6.
#' @param linearity.abort Minimum linearity (2nd / 1st PCA eigenvalue) below which the cloud has collapsed to
#'   a 1-D arc (a single heading held), so the in-plane centre is unobservable and the fit is not applied.
#'   Default 0.1.
#' @param extent.min Minimum angular extent (degrees) of the cloud about its centre, below which it is a
#'   stationary blob and the fit is not applied. Default 40.
#' @param target.field Optional numeric override for the target field magnitude (uT); by default the IGRF
#'   intensity (when coordinates are available) or the cloud's own median radius is used. Default NULL.
#' @return A validated `nautilus_mag_calibration` object for the `control` argument of
#'   \code{\link{calibrateMagnetometer}}.
#' @seealso \code{\link{calibrateMagnetometer}}, \code{\link{calibrationControl}}
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
#' Groups the specialised orientation-estimation knobs of \code{\link{processTagData}} into one object,
#' leaving the primary choice - the estimator itself - as the top-level `orientation.algorithm` argument
#' of \code{\link{processTagData}}. Covers the Madgwick filter gain, the mounting pitch/roll offset
#' corrections, and the orientation-anomaly warning threshold.
#'
#' @param madgwick.beta Numeric. The Madgwick filter's gain parameter (the accelerometer-vs-gyroscope
#'   trust trade-off); used only when `orientation.algorithm = "madgwick"`. Default 0.02.
#' @param correct.pitch Logical. Correct the mounting pitch offset (pitch vs vertical-velocity intercept). Default TRUE.
#' @param correct.roll Logical. Correct the mounting roll offset (median roll over level swimming). Default TRUE.
#' @param pitch.offset.min.r2 Minimum R^2 of the pitch-vs-vertical-velocity fit required to apply the
#'   `correct.pitch` mounting-offset correction. Below it the "offset" is really just the mean pitch, so
#'   subtracting it would strip genuine posture signal and the correction is skipped. Default 0.1.
#' @param warning.threshold Numeric. Threshold (degrees) above which a median |pitch| or |roll| raises an
#'   orientation-anomaly warning. It also caps the mounting-offset corrections: an implausibly large
#'   estimated offset (magnitude at or above this) is reported but not applied. Default 45.
#' @param heading.denoise How to suppress paddle-wheel magnetometer contamination before computing heading.
#'   A spinning paddle magnet adds a large, high-frequency oscillation to the field; because it is additive
#'   in the field-vector domain and averages to zero over a rotation, a centred (zero-phase) running mean
#'   of the magnetometer vector removes it while preserving the slow, orientation-driven variation.
#'   \code{"auto"} (default) detects the paddle and derives one stable window per deployment from its
#'   rotation frequency (via the shared paddle-state primitive); \code{"manual"} always applies
#'   `heading.denoise.window`; \code{"off"} disables it. When the paddle is too slow to separate from the
#'   animal's turning, no window can help and a warning is raised (use a gyro-based orientation instead).
#' @param heading.denoise.window Numeric. Smoothing window (seconds) used when `heading.denoise = "manual"`.
#'   Default 3.
#' @return A validated `nautilus_orientation` object for the `orientation` argument of \code{\link{processTagData}}.
#' @seealso \code{\link{processTagData}}, \code{\link{calibrationControl}}, \code{\link{smoothingControl}}
#' @examples
#' orientationControl(correct.roll = FALSE)     # skip the roll-offset correction
#' orientationControl(madgwick.beta = 0.05)     # stronger Madgwick gain
#' orientationControl(heading.denoise = "manual", heading.denoise.window = 2)
#' @export
orientationControl <- function(madgwick.beta = 0.02, correct.pitch = TRUE, correct.roll = TRUE,
                               pitch.offset.min.r2 = 0.1, warning.threshold = 45,
                               heading.denoise = c("auto", "manual", "off"),
                               heading.denoise.window = 3) {
  heading.denoise <- match.arg(heading.denoise)
  .assert_number(madgwick.beta, "orientation$madgwick.beta", min = 0)
  .assert_flag(correct.pitch, "orientation$correct.pitch")
  .assert_flag(correct.roll, "orientation$correct.roll")
  .assert_number(pitch.offset.min.r2, "orientation$pitch.offset.min.r2", min = 0, max = 1)
  .assert_number(warning.threshold, "orientation$warning.threshold", min = 0)
  .assert_number(heading.denoise.window, "orientation$heading.denoise.window", min = 0)
  structure(list(madgwick.beta = madgwick.beta, correct.pitch = correct.pitch, correct.roll = correct.roll,
                 pitch.offset.min.r2 = pitch.offset.min.r2, warning.threshold = warning.threshold,
                 heading.denoise = heading.denoise, heading.denoise.window = heading.denoise.window),
            class = "nautilus_orientation")
}


#' Per-channel anomaly-detection settings for checkSensorQuality()
#'
#' @description
#' Bundles the outlier-detection thresholds for one sensor channel, so \code{\link{checkSensorQuality}}
#' can screen several channels (each with its own settings) in a single call.
#'
#' @param rate.threshold Numeric. Rate-of-change threshold (units per second) beyond which a value is
#'   flagged as an outlier. Required.
#' @param sensor.resolution Numeric. The sensor's resolution (smallest detectable change); used to gate
#'   the rate of change so ordinary measurement noise is not flagged. Default 0.5.
#' @param sensor.accuracy.fixed,sensor.accuracy.percent Numeric. The sensor's accuracy, as a fixed value
#'   (same units as the channel) or a percentage of the reading. Provide at most one. Recorded for
#'   provenance; the rate gate uses `sensor.resolution` only. Defaults `NULL`.
#' @param outlier.window Numeric. Time window (minutes) within which consecutive outliers are grouped and
#'   treated as one anomaly period (a sensor-malfunction block). Default 5.
#' @param stall.threshold Numeric. Duration (minutes) beyond which a run of constant, non-zero readings
#'   is flagged as a stalled sensor. Default 5.
#' @return A validated `nautilus_anomaly` object for a `sensors` entry of \code{\link{checkSensorQuality}}.
#' @seealso \code{\link{checkSensorQuality}}
#' @examples
#' anomalyControl(rate.threshold = 7, sensor.resolution = 0.5, sensor.accuracy.percent = 1)
#' @export
anomalyControl <- function(rate.threshold,
                           sensor.resolution = 0.5,
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
#' Bundles the settings for the temporal alignment applied by \code{\link{importTagData}} when a
#' deployment pairs a primary archival tag (depth + IMU) with a separate Wildlife Computers tag
#' (wet/dry + Fastloc-GPS). The two devices keep independent clocks that can disagree by anything from a
#' few seconds to many minutes, and that offset - invisible without a shared signal - silently corrupts
#' every step that combines the streams (the depth zero-offset correction, dead-reckoning fix anchors).
#'
#' The Wildlife Computers `...-Archive.csv` records the tag's own depth (and temperature) at a low rate.
#' Because depth is a physical quantity measured by \emph{both} devices, cross-correlating the WC depth
#' against the primary tag's depth recovers the clock offset directly: the lag that maximises their
#' correlation is the offset. In real deployments this peak is razor-sharp (correlation ~1.0), so the
#' estimate is robust. The WC-clock streams (`meta$ancillary$positions`, `meta$ancillary$dry`) are then
#' shifted onto the primary tag's timeline. The primary depth/IMU stream is the reference and is never
#' moved; deploy/pop-up positions (which come from the metadata table, not the WC clock) are never moved.
#'
#' A single constant offset per deployment is estimated: in practice the residual clock drift is
#' negligible (a few seconds over a multi-day record) and dominated by the constant offset. The
#' correction is conservative - it \strong{abstains} (shifting nothing, and saying so) whenever the
#' evidence is too weak to trust: no shared depth channel, too little temporal overlap, a flat depth
#' trace with no dives to lock onto, or a peak correlation below `min.correlation`. The clock is never
#' silently shifted; the estimated offset and its diagnostics are stored in `meta$ancillary$alignment`.
#'
#' @param method Alignment method: `"depth-xcorr"` (cross-correlate the shared depth channel, the
#'   default) or `"none"` (disable; the WC streams are kept on their own clock).
#' @param max.lag Maximum absolute clock offset to search for, in seconds (the cross-correlation is
#'   evaluated over `[-max.lag, +max.lag]`). Also a sanity bound: a best lag landing on the search edge is
#'   treated as unresolved and the correction abstains. Default 3600 (one hour).
#' @param min.overlap Minimum temporal overlap between the two depth records, in minutes, required to
#'   attempt alignment. Below this the correction abstains. Default 30.
#' @param min.correlation Minimum peak Pearson correlation between the two depth traces (at the best lag)
#'   for the offset to be accepted. Below this the depth profiles do not match well enough to trust the
#'   lag, and the correction abstains. Default 0.9.
#' @return A validated `nautilus_alignment` object for the `alignment` argument of \code{\link{importTagData}}.
#' @seealso \code{\link{importTagData}}, \code{\link{depthDriftControl}}
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


#' On-screen-display OCR settings for getVideoMetadata()
#'
#' @description
#' Bundles the optical-character-recognition (OCR) settings used by \code{\link{getVideoMetadata}} to
#' read the timestamp burned into a camera's on-screen display. OCR is a *secondary* timestamp source:
#' \code{\link{getVideoMetadata}} takes the recording start time from the file name whenever possible
#' (exact and camera-agnostic), and falls back to OCR only for cameras whose file names carry no
#' timestamp (e.g. MOBIUS), or uses it as an optional cross-check. All OCR knobs therefore live here,
#' keeping the main call uncluttered.
#'
#' @param model Name of the Tesseract model (`traineddata`) trained on the overlay font. Default `"cam"`,
#'   the fine-tuned camera-tag model, which is downloaded on first use (see
#'   \code{\link{installCamOcrModel}}); use `"eng"` (or any installed model) to skip that.
#' @param box Integer vector `c(x, y, width, height)` giving the pixel location of the timestamp box
#'   within a frame: top-left corner `(x, y)` and its `width`/`height`. These coordinates are relative to
#'   a frame of height `frame.height`; for videos of a different resolution they are scaled by
#'   `actual_height / frame.height`, so the same control works across resolutions of the same camera.
#'   Default `c(3249, 2120, 325, 28)` (bottom-right box of the 4K camera overlay).
#' @param frame.height Reference frame height (pixels) the `box` coordinates assume. Default 2160 (4K).
#' @param search.radius Search radius (pixels) around the expected `box` used to lock onto the bright
#'   timestamp panel when the overlay drifts slightly between cameras/firmwares. Default 80.
#' @param max.search.frames Maximum number of frames to try when the first sampled frame yields no
#'   readable timestamp (e.g. a black frame at the very start of a clip). Default 10.
#' @param char.whitelist Optional string restricting the characters Tesseract may output (e.g.
#'   `"0123456789:- "`). `NULL` (default) uses the model's own configuration.
#' @return A validated `nautilus_ocr` object for the `ocr` argument of \code{\link{getVideoMetadata}}.
#' @seealso \code{\link{getVideoMetadata}}
#' @examples
#' ocrControl(box = c(120, 40, 300, 26), frame.height = 1080)   # 1080p camera, top-left overlay
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
#' Bundles the tunable thresholds of the sensor-integrity checks run by \code{\link{checkSensorIntegrity}}
#' into one object, so the main call stays uncluttered and every knob is documented and adjustable in one
#' place. The defaults are calibrated against real whale-shark deployments; adjust them for other species
#' or tag systems.
#'
#' @param dup.cor Duplication (error): a gyroscope or magnetometer triplet is flagged as a copy of the
#'   accelerometer when the per-axis \code{|r|} exceeds this on all three axes. Default 0.999.
#' @param saturation.frac Saturation (warning): a channel is flagged when the fraction of samples pinned
#'   at its exact minimum or maximum (clipping) exceeds this. Default 0.01.
#' @param mag.cv Magnetometer plausibility (warning): flagged when the robust coefficient of variation of
#'   the hard-iron-centred field magnitude \code{|B|} exceeds this (a stable field is near-constant).
#'   Default 0.4.
#' @param accel.scale.tol Accelerometer scale (warning): flagged when the median static-acceleration
#'   magnitude departs from 1 g by more than this (g). Default 0.2.
#' @param gyro.bias.frac,gyro.bias.min Gyroscope bias (info): flagged when the largest per-axis median
#'   offset exceeds BOTH `gyro.bias.frac` as a fraction of the rotational signal scale (robust MAD) AND
#'   `gyro.bias.min` in absolute terms (rad/s). The absolute floor stops a negligible offset from being
#'   flagged merely because the animal barely rotated (a tiny MAD inflates the relative measure).
#'   Defaults 0.3 and 0.02.
#' @param paddle.min.freq,paddle.harmonic.guard,paddle.prominence,paddle.max.freq.frac Paddle-wheel
#'   contamination (warning): the magnetometer spectrum is scanned for a narrow-band peak in the band from
#'   \code{max(paddle.min.freq, paddle.harmonic.guard * f_tailbeat)} Hz up to
#'   \code{paddle.max.freq.frac * Nyquist} Hz, where \code{f_tailbeat} is the record's dominant low
#'   frequency. The floor keeps the search clear of the tail-beat fundamental and its harmonics (the main
#'   source of false positives - a swimming animal's body oscillation modulates the magnetometer); the
#'   ceiling avoids aliasing artefacts near Nyquist. A peak is flagged when its prominence (peak / median
#'   band power) exceeds \code{paddle.prominence}. Defaults 3.5 Hz, 6, 30, 0.85.
#' @param dropout.frac Dropout (info): a channel is flagged when it is missing (NA) for more than this
#'   fraction of the deployment. Default 0.5.
#' @return A validated `nautilus_integrity` object for the `control` argument of
#'   \code{\link{checkSensorIntegrity}}.
#' @seealso \code{\link{checkSensorIntegrity}}
#' @examples
#' integrityControl(mag.cv = 0.5, paddle.prominence = 50)
#' @export
integrityControl <- function(dup.cor               = 0.999,
                             saturation.frac       = 0.01,
                             mag.cv                = 0.4,
                             accel.scale.tol       = 0.2,
                             gyro.bias.frac        = 0.3,
                             gyro.bias.min         = 0.02,
                             paddle.min.freq       = 3.5,
                             paddle.harmonic.guard = 6,
                             paddle.prominence     = 30,
                             paddle.max.freq.frac  = 0.85,
                             dropout.frac          = 0.5) {
  .assert_number(dup.cor, "dup.cor", min = 0, max = 1)
  .assert_number(saturation.frac, "saturation.frac", min = 0, max = 1)
  .assert_number(mag.cv, "mag.cv", min = 0)
  .assert_number(accel.scale.tol, "accel.scale.tol", min = 0)
  .assert_number(gyro.bias.frac, "gyro.bias.frac", min = 0)
  .assert_number(gyro.bias.min, "gyro.bias.min", min = 0)
  .assert_number(paddle.min.freq, "paddle.min.freq", min = 0)
  .assert_number(paddle.harmonic.guard, "paddle.harmonic.guard", min = 1)
  .assert_number(paddle.prominence, "paddle.prominence", min = 1)
  .assert_number(paddle.max.freq.frac, "paddle.max.freq.frac", min = 0, max = 1)
  .assert_number(dropout.frac, "dropout.frac", min = 0, max = 1)
  structure(list(dup.cor = dup.cor, saturation.frac = saturation.frac, mag.cv = mag.cv,
                 accel.scale.tol = accel.scale.tol, gyro.bias.frac = gyro.bias.frac, gyro.bias.min = gyro.bias.min,
                 paddle.min.freq = paddle.min.freq, paddle.harmonic.guard = paddle.harmonic.guard,
                 paddle.prominence = paddle.prominence, paddle.max.freq.frac = paddle.max.freq.frac,
                 dropout.frac = dropout.frac),
            class = "nautilus_integrity")
}


#' Control settings for trackMetrics()
#'
#' @description Selects which movement-path metrics \code{\link{trackMetrics}} computes and the sizes of
#' the rolling windows used for the temporal tortuosity columns.
#'
#' @param metrics Character vector of metrics to compute, any of `"path_ratio"`, `"fractal_dimension"`,
#'   `"sinuosity"`, `"turning_angle"`, `"straightness"`, or `"all"` (the default) for all of them.
#' @param min.points Minimum number of valid position fixes a track needs to be summarised. Tracks with
#'   fewer are skipped. Default 5.
#' @param hourly.window.h,daily.window.h Window sizes (hours) for the `Hourly_tortuosity` and
#'   `Daily_tortuosity` columns - the mean path/displacement ratio over rolling windows of that length.
#'   Defaults 1 and 24.
#' @return A validated `nautilus_track_metrics` object for the `control` argument of
#'   \code{\link{trackMetrics}}.
#' @seealso \code{\link{trackMetrics}}
#' @examples
#' trackMetricsControl(metrics = c("path_ratio", "straightness"), min.points = 10)
#' @export
trackMetricsControl <- function(metrics = "all",
                                min.points = 5,
                                hourly.window.h = 1,
                                daily.window.h = 24) {
  available <- c("path_ratio", "fractal_dimension", "sinuosity", "turning_angle", "straightness")
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
#' Groups the tuning knobs of the neighbour-consistency ("root") speed test used by
#' \code{\link{filterLocations}} into one validated object. The primary threshold - the maximum plausible
#' speed - stays the top-level \code{max.speed.kmh} argument of \code{\link{filterLocations}}; this object
#' controls only how that test is applied.
#'
#' @param min.time.mins Numeric. Minimum time separation (minutes) between two fixes for the implied speed
#'   between them to be trusted. Segments closer together in time are not judged, since a sub-threshold
#'   gap inflates the speed unreliably (a metre of GPS jitter over a few seconds looks like a huge speed).
#'   Default 0 (judge every segment; the position record already drops exact-duplicate timestamps).
#' @param max.iterations Integer. Maximum number of removal passes. Each pass removes the single most
#'   egregious spike and recomputes speeds against the new neighbours; the loop stops early once no fix is
#'   implausible. Default 50.
#' @param spike.angle Numeric in \[90, 180\], or `NULL`. Optional direction-reversal test that supplements
#'   the speed test: an interior fix is also treated as a spike when the track's heading reverses by at
#'   least this many degrees at that fix (a sharp out-and-back) \emph{and} at least one adjoining segment
#'   exceeds \code{max.speed.kmh}. Catches sharp spikes at moderate speed that the pure speed test misses.
#'   `NULL` (default) disables it.
#' @return A validated `nautilus_filter_locations` object for the `control` argument of
#'   \code{\link{filterLocations}}.
#' @seealso \code{\link{filterLocations}}
#' @examples
#' filterLocationsControl(min.time.mins = 2)              # ignore fix pairs less than 2 min apart
#' filterLocationsControl(spike.angle = 160)             # also flag sharp out-and-back spikes
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
#' the "Dead reckoning in brief" section of \code{\link{reconstructTrack}}). Heading is produced upstream by
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
#' @param constant.speed Numeric. Speed (m/s) for `speed.method = "constant"`, and the fallback whenever
#'   another method yields NA (shallow-pitch `depth_rate` samples, or a failed VeDBA calibration). Default
#'   0.5.
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
#' estimating animal travel rate in terrestrial dead-reckoning systems. *Zoology*. 115:58-64.
#' \doi{10.1016/j.zool.2011.09.003}
#'
#' Gunner RM, Holton MD, Scantlebury MD, *et al.* (2021) Dead-reckoning animal movements in R: a reappraisal
#' using Gundog.Tracks. *Animal Biotelemetry*. 9:23. \doi{10.1186/s40317-021-00245-z}
#'
#' Wensveen PJ, Thomas L, Miller PJO (2015) A path reconstruction method integrating dead-reckoning and
#' position fixes applied to humpback whales. *Movement Ecology*. 3:31. \doi{10.1186/s40462-015-0061-6}
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


#' Tuning for dive detection in detectDives()
#'
#' @description
#' Bundles the settings for \code{\link{detectDives}}. A dive is a vertical excursion of the depth
#' trace away from a reference level, detected by two-threshold hysteresis with a prominence
#' criterion and bounded by a return to within a band of that reference.
#'
#' Three axes make one definition serve every taxon, and they are the only concessions to taxonomy:
#' \itemize{
#'   \item \code{reference} - where "not diving" sits. \code{"surface"} (b(t) = 0) suits air-breathers
#'     whose zero is anchored by surfacing; \code{"baseline"} tracks a running level and suits fish
#'     that never surface, or benthic animals that rest at depth. \code{"auto"} chooses from the
#'     depth-drift provenance and reports which it picked.
#'   \item \code{direction} - \code{"down"} for animals that excurse downward from a shallow level,
#'     \code{"up"} for benthic resters that leave the bottom, \code{"both"} for either.
#'   \item the hysteresis pair \code{depth.threshold} / \code{surface.band} - the scale of an excursion.
#' }
#'
#' Hysteresis is not optional. With a single threshold, sensor noise at the crossing splits one dive
#' into many and the dive count becomes a property of the pressure transducer rather than the animal.
#'
#' @param reference Where "not diving" sits: \code{"auto"} (default), \code{"surface"} or
#'   \code{"baseline"}. See Details for how \code{"auto"} decides.
#' @param direction Excursion direction: \code{"down"} (default), \code{"up"} or \code{"both"}.
#' @param depth.threshold Numeric (m). Depth past the reference at which an excursion becomes a dive.
#'   \code{NULL} (default) derives a FLOOR from the record and reports it - the smallest excursion the
#'   data can support, which is NOT an estimate of what the animal treats as a dive. Set it from your
#'   study system.
#' @param surface.band Numeric (m). A dive ends only when depth returns to within this band of the
#'   reference. Must be less than \code{depth.threshold}. \code{NULL} derives
#'   \code{max(2 x ZOC residual, depth.threshold / 10, 0.5)} - it scales with the DIVE, because the band
#'   answers "has the animal returned?", not merely "how uncertain is the zero?". A band derived from the
#'   residual alone can be too tight to ever close: on a real record a 0.75 m band merged one deep dive
#'   and 1,700 s of shallow oscillation into a single 2,016 s "dive".
#' @param min.prominence Numeric (m). Minimum peak prominence relative to the reference.
#'   \code{NULL} uses \code{depth.threshold - surface.band}.
#' @param min.duration Numeric (s). Shortest measurable dive. \code{NULL} derives a floor from the
#'   depth smoothing window and the sampling interval, because a centred smoother attenuates any
#'   excursion shorter than its window.
#' @param baseline.window Numeric (h). Window for the running baseline. Default 3.
#' @param baseline.stat Baseline estimator: \code{"median"} (default) or \code{"quantile"}. These have
#'   complementary failure modes - see Details.
#' @param baseline.quantile Numeric in (0, 1) for \code{baseline.stat = "quantile"}. \code{NULL}
#'   picks 0.10 / 0.90 / 0.50 by \code{direction}.
#' @param phase.method How descent/bottom/ascent are split: \code{"vertical.rate"} (default) or
#'   \code{"prop.depth"}.
#' @param rate.crit,rate.quantile Numeric. The vertical-rate phase rule: a phase ends when the rate
#'   falls below \code{rate.crit} times the dive's \code{rate.quantile} quantile of vertical rate.
#'   Defaults 0.25 and 0.90. A quantile, not the maximum, because the maximum of a smoothed series is
#'   an artefact of the smoothing window.
#' @param bottom.prop Numeric in (0, 1). For \code{phase.method = "prop.depth"}: the bottom phase is
#'   the span deeper than this proportion of the dive's amplitude. Default 0.80.
#' @param max.gap Numeric (s). A sampling gap longer than this splits a dive and marks it censored.
#'   \code{NULL} uses \code{max(60, 10 x} median interval\code{)}.
#' @param wiggle.amplitude Numeric (m). Minimum amplitude for a within-dive reversal to count.
#'   \code{NULL} uses \code{max(0.5, 3 x} the stored series' noise\code{)}.
#' @param min.surface.occupancy Numeric in (0, 1). For \code{reference = "auto"}: the minimum fraction
#'   of samples within the surface band required before \code{"surface"} is chosen. Default 0.005.
#' @param require.zoc \code{"warn"} (default), \code{"error"} or \code{"ignore"} - what to do when
#'   \code{reference = "surface"} is requested but the zero-offset correction abstained.
#'
#' @details
#' \strong{How \code{reference = "auto"} decides.} It picks \code{"surface"} only when the
#' \code{depth_drift} provenance record exists with a status of \code{applied},
#' \code{applied_with_gaps} or \code{constant_offset}, AND the record spends at least
#' \code{min.surface.occupancy} of its samples within the surface band. Otherwise \code{"baseline"}.
#' The decision and its reason are reported.
#'
#' \strong{Choosing \code{baseline.stat}.} The two estimators fail in opposite regimes, and neither is
#' universally correct. A running \strong{median} tracks a baseline that drifts during the deployment
#' (an animal moving from shelf to slope) but migrates INTO the excursions once they occupy more than
#' about half the record. A low \strong{quantile} is immune to that duty cycle but, on a trending
#' baseline, tracks the window's trailing edge rather than the local level. \code{detectDives()}
#' measures both conditions and warns when the chosen estimator is in its failing regime.
#'
#' @return A validated \code{nautilus_dive} object for the \code{control} argument of
#'   \code{\link{detectDives}}.
#' @seealso \code{\link{detectDives}}, \code{\link{diveMetrics}}, \code{\link{smoothingControl}}
#' @examples
#' diveControl(depth.threshold = 5)                              # a 5 m dive, surface-referenced
#' diveControl(reference = "baseline", depth.threshold = 20)     # a fish that never surfaces
#' diveControl(reference = "baseline", direction = "up")         # a benthic rester leaving the bottom
#' @export

diveControl <- function(reference             = c("auto", "surface", "baseline"),
                        direction             = c("down", "up", "both"),
                        depth.threshold       = NULL,
                        surface.band          = NULL,
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
  if (!is.null(depth.threshold) && !is.null(min.prominence) && min.prominence > depth.threshold)
    .abort("{.arg dive$min.prominence} ({.val {min.prominence}}) cannot exceed {.arg dive$depth.threshold} ({.val {depth.threshold}}).")

  structure(list(reference = reference, direction = direction,
                 depth.threshold = depth.threshold, surface.band = surface.band,
                 min.prominence = min.prominence, min.duration = min.duration,
                 baseline.window = baseline.window, baseline.stat = baseline.stat,
                 baseline.quantile = baseline.quantile,
                 phase.method = phase.method, rate.crit = rate.crit, rate.quantile = rate.quantile,
                 bottom.prop = bottom.prop, max.gap = max.gap, wiggle.amplitude = wiggle.amplitude,
                 min.surface.occupancy = min.surface.occupancy, require.zoc = require.zoc),
            class = "nautilus_dive")
}
