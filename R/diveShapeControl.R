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
