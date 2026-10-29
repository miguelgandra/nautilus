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
#'   dive's duration, deployment sampling interval and depth noise; see Details. The slope
#'   estimator is shared with [processTagData()] and [checkTagMapping()]; phase classification
#'   uses signed reference-relative depth and does not apply velocity post-smoothing.
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
