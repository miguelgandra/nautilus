#######################################################################################################
# Per-dive metrics ####################################################################################
#######################################################################################################

#' Summarise detected dives and assess record completeness
#'
#' @description
#' Reduces deployment datasets annotated by [detectDives()] to one row per retained dive.
#' Summaries include timing, depth excursion, phase structure, vertical kinematics, detection
#' settings and record-completeness diagnostics. Additional sensor or derived channels can be
#' summarised over whole dives and, optionally, within phases.
#'
#' The function retains censored dives and unsupported phase structures with explicit indicators,
#' rather than imposing an analysis-specific quality filter. Use these indicators to select
#' observations appropriate to the scientific question, and [plotDives()] to inspect distributions
#' and relationships between per-dive metrics.
#'
#' @param data A \code{nautilus_tag} object, a list of deployment datasets, a data frame containing
#'   deployments identified by \code{id.col}, or a character vector of \code{.rds} file paths.
#'   The output of [detectDives()] is recommended. Required columns are \code{dive_id},
#'   \code{dive_phase}, \code{datetime.col} and \code{depth.col}; \code{depth_baseline} and
#'   detection processing history supply the reference and resolved settings.
#'   File inputs are read sequentially.
#' @param variables Optional character vector of numeric per-sample columns to summarise,
#'   for example \code{c("temp", "vedba", "tbf_hz_wavelet")}. \code{NULL} (default) adds
#'   no channel summaries. Absent channels receive the same summary columns filled with
#'   \code{NA_real_}; see Details for naming and missing-value handling.
#' @param circular.variables Character vector identifying which requested \code{variables}
#'   are angles in degrees. Default \code{c("heading", "roll")}. These use circular mean
#'   angles and mean resultant lengths rather than arithmetic summaries.
#'   \code{NULL} treats all requested variables as linear. Listing a channel here does not
#'   request its summary unless it is also in \code{variables}.
#' @param statistics Character vector selecting \code{"mean"}, \code{"sd"} or both
#'   (default) for linear \code{variables}. Does not change the fixed dive metrics or
#'   the summaries of circular variables.
#' @param by.phase Logical. Also summarise requested \code{variables} separately within
#'   descent, bottom and ascent. Default \code{FALSE}. Whole-dive summaries are always
#'   included when \code{variables} are supplied.
#' @param id.col Character. Name of the column identifying deployments, not animals.
#'   Default \code{"ID"}. Output deployment identifiers are stored in \code{ID}.
#' @param datetime.col Character. Name of the timestamp column. Default \code{"datetime"}.
#'   Standard pipeline inputs use \code{POSIXct} timestamps in chronological order.
#' @param depth.col Character. Name of the depth column in metres, positive downwards.
#'   Default \code{"depth"}.
#' @param verbose How much detail to print: \code{0}/\code{"quiet"},
#'   \code{1}/\code{"normal"}, or \code{2}/\code{"detailed"} (default). Detailed output
#'   adds phase-support, censoring and unusually long-dive diagnostics.
#' @param shape Optional control object from [diveShapeControl()] enabling geometric V-, U-
#'   and W-shaped profile classification. A named list of constructor arguments is also accepted.
#'   \code{NULL} (default) disables classification and preserves the existing metric schema.
#'   Shape rules do not alter dive boundaries, phase labels or the original depth channel.
#'
#' @details
#' ## Workflow and input assumptions
#'
#' Apply [processTagData()] to correct depth and derive any required channels, [detectDives()]
#' to annotate excursions, and this function to construct a per-dive analysis table.
#' Sampling resolution, depth reference and detection criteria affect the meaning of the
#' resulting metrics; they should be considered before combining deployments or study systems.
#'
#' Within each deployment, a positive \code{dive_id} should identify one contiguous interval
#' in an ordered timestamp record. The identifier is only unique together with the deployment
#' \code{ID}. Zero identifiers are not summarised. No missing intervals are interpolated, and
#' no dives are removed on the basis of duration, completeness or phase support.
#'
#' Deployments lacking required annotation, depth or timestamp columns are skipped with a warning.
#' If no dives can be summarised, the result is a typed zero-row table with the requested schema.
#' Manually annotated records are accepted, but missing \code{depth_baseline} uses zero as the
#' reference. Missing detection history leaves setting columns as \code{NA} and uses fallback
#' gap, slope-window and reversal criteria; it cannot reconstruct the original detection choices.
#'
#' ## Timing, depth and vertical rates
#'
#' Dive and phase durations are elapsed spans between their first and last timestamps, not sample
#' counts multiplied by a nominal interval. Phase spans need not sum to the dive duration because
#' intervals between adjacent phase endpoints are not assigned to either span. Threshold-crossing
#' times are not reconstructed.
#'
#' Depth maxima describe the physical depth record, whereas amplitude describes the largest
#' absolute departure from the reference. Despite its name, \code{max_depth_time} is the timestamp
#' of that largest absolute reference-relative departure, not necessarily the timestamp of
#' \code{max_depth_m}. These can differ for upward excursions or a changing baseline.
#' \code{prominence_m} is the maximum absolute departure minus the larger of its first and last
#' finite values; it is not the interior-saddle criterion used for optional dive splitting.
#'
#' Vertical rates are centred local least-squares slopes of physical depth against time, in
#' metres per second. Mean rates retain their sign: positive indicates increasing depth and
#' negative decreasing depth, irrespective of the excursion's phase labels. The
#' \code{descent_rate_q90} and \code{ascent_rate_q90} columns are fixed 90th percentiles of
#' absolute slope magnitude, not maximum rates.
#'
#' The slope window uses the nominal \code{phase_window_s} recorded by [detectDives()], limited
#' by dive duration and the deployment sampling interval. Without usable window provenance,
#' the default is the larger of five seconds and three sampling intervals. Per-dive adaptive
#' widening during detection is not recorded, so metric and detection windows can differ.
#' These rates are derived from depth, not from an existing \code{vertical_velocity} channel.
#'
#' \code{vertical_distance_m} sums absolute differences between consecutive depth samples.
#' It is sensitive to noise and quantisation, omits differences involving missing depth, and
#' cannot recover movement during unobserved intervals. It is not a reconstructed travel distance.
#'
#' ## Phase support and interpretation
#'
#' Phase terminology follows [detectDives()]: descent is the opening limb away from the reference,
#' ascent the return limb, and bottom the extremum region. For upward excursions the transit
#' labels are opposite to physical vertical direction. Bottom does not establish seabed contact,
#' feeding or resting.
#'
#' \code{shape_supported} requires at least two phases with positive timestamp spans.
#' A \code{"DA"} structure can therefore be supported with no bottom phase. In that case,
#' \code{bottom_duration_s} is zero and bottom-depth summaries are \code{NA}.
#' When shape is unsupported, the three phase durations, transit rates, bottom-depth summaries
#' and reversal count are \code{NA}. Whole-dive depth and timing metrics, vertical distance
#' and requested channel summaries are still computed. Requested phase-channel summaries
#' use available labelled samples independently of \code{shape_supported}.
#'
#' ## Censoring and coverage
#'
#' Excursions reaching either deployment boundary or bounding or containing interruptions longer
#' than the detection \code{max.gap} are retained and marked as censored. Causes distinguish
#' record boundaries, timestamp jumps and missing-depth runs. \code{complete} means no such
#' censoring was identified; it is not a general sensor-quality or behavioural-validity flag.
#' Short dropouts can remain in a complete dive.
#'
#' \code{n_gaps} and \code{gap_s} report interruptions associated with each dive. A timestamp
#' gap contributes its timestamp span; a depth dropout contributes the span from its first to
#' last missing sample. Coincident time and depth interruptions at a bounding edge are counted
#' once using the larger span. A gap can bound two retained fragments, so summing \code{gap_s}
#' across dives does not measure unique deployment-level missing time.
#'
#' \code{depth_coverage} is the fraction of retained rows with finite depth, not the fraction
#' of elapsed time observed. A timestamp gap can coexist with high depth coverage.
#' \code{inter_dive_censored} independently assesses long interruptions between consecutive
#' dives; completeness of the bounding dives does not guarantee an observed inter-dive interval.
#' The last dive has no subsequent interval and receives \code{NA} for both inter-dive columns.
#' An interval between baseline-relative excursions is not necessarily a surface recovery period.
#'
#' With at least five pooled dives, detailed console output identifies durations exceeding the
#' larger of two hours and the pooled median plus five median absolute deviations. This is an
#' advisory diagnostic, not a stored flag, duration cap or automatic exclusion.
#'
#' ## Model-based depth attenuation
#'
#' \code{depth_attenuation} describes worst-case peak retention for a symmetric triangular
#' excursion under bin averaging, inferred from recorded original and processed sampling rates.
#' For observed duration \eqn{T} and bin width \eqn{L}, it is \eqn{1 - L/T} when
#' \eqn{T \ge 2L}, otherwise \eqn{T/(4L)}. Values are bounded between zero and one.
#' This is a shape-specific diagnostic for reference-relative amplitude, not a general bound
#' on absolute maximum depth or an empirical correction for arbitrary dive shapes.
#'
#' A value of one is also the fallback when no applicable bin width or duration is available,
#' including missing sampling provenance; it does not prove that a record is unfiltered.
#' The depth smoothing window in [smoothingControl()] is not charged as stored-depth smoothing.
#' No attenuation correction is applied. Reprocess at finer resolution when binning compromises
#' the excursions required for the analysis.
#'
#' ## Optional geometric shape classification
#'
#' Supply \code{shape = diveShapeControl()} to classify the retained timestamped depth profiles,
#' independently of \code{phase_structure}, \code{shape_supported} and \code{n_reversals}.
#' Profiles are oriented relative to their depth reference, smoothed only for classification,
#' and detrended against the line joining their endpoints after checking that both transit
#' limbs were observed. Time-weighted normalised area measures broadness; amplitude-filtered
#' and temporally separated peaks identify significant internal excursions.
#'
#' Two or more qualifying peaks define \code{"W"}. A single peak defines \code{"V"} or
#' \code{"U"} according to the broadness thresholds. Intermediate or complex profiles are
#' \code{"other"}, not forced into one of the three classes. Unsuitable records receive
#' \code{NA} with a diagnostic status. Default thresholds are heuristic starting points,
#' not universally validated biological definitions. See [diveShapeControl()] for the full
#' preparation, resolution floors and decision rules.
#'
#' Shape classification does not redefine \code{shape_supported}, which remains an indicator
#' of resolved phase durations. Censored dives are not classified. Additional quality checks
#' concern chronological timestamps, contiguous identifiers, reference availability, finite-depth
#' coverage, gaps, sample count, excursion relief and observed limbs. Short bracketed gaps can
#' be interpolated for classification only; neither missing endpoints nor long gaps are bridged.
#'
#' The classes describe geometry, not independently established foraging or movement behaviour.
#' Detection thresholds and sampling resolution affect the retained shape. Optional prominence
#' splitting in [detectDives()] can turn a W-shaped excursion into separate dives; classification
#' respects those boundaries and does not join fragments. Validate thresholds against reviewed
#' profiles and assess sensitivity before using classes in scientific analyses.
#'
#' ## Additional channel summaries
#'
#' Linear variables produce \code{<variable>_mean} and/or \code{<variable>_sd}, according to
#' \code{statistics}. With \code{by.phase = TRUE}, the same statistics are added as
#' \code{<variable>_<phase>_<statistic>} for descent, bottom and ascent. At the default
#' statistics, this adds two columns per linear variable, or eight with phase summaries.
#' Means are sample-weighted, not weighted by elapsed time.
#'
#' Circular variables instead produce \code{<variable>_mean_angle} in degrees on
#' \code{[0, 360)} and \code{<variable>_mrl}, the mean resultant length on \code{[0, 1]}.
#' Phase summaries add only \code{<variable>_<phase>_mean_angle}, giving five columns per
#' circular variable when \code{by.phase = TRUE}. Non-finite angles are omitted.
#' Mean angles are \code{NA} when the mean resultant length is below 0.1; the whole-dive
#' resultant length is still returned. Magnetic heading requires appropriate declination
#' correction for geographic comparisons; resultant length is invariant to a constant rotation.
#'
#' Absent channels produce \code{NA} columns. For present linear channels, base-R missing-value
#' handling is used: an empty or entirely missing subset can yield \code{NaN} for its mean,
#' and fewer than two observations yield \code{NA} for its standard deviation. Use
#' \code{is.na()} to recognise both \code{NA} and \code{NaN}. Infinite linear values are not
#' automatically removed. Tables share the same columns when \code{variables},
#' \code{circular.variables}, \code{statistics}, \code{by.phase} and whether shape classification
#' is enabled are identical.
#'
#' @return A data frame of class \code{nautilus_dive_metrics}, with one row per positive
#'   dive identifier and a fixed core schema followed by requested channel summaries.
#'   Output deployment identifiers are always named \code{ID}. Core columns are:
#'   \describe{
#'     \item{Identification and detection settings}{\code{ID}, \code{dive_id},
#'       \code{reference}, \code{direction}, \code{depth_threshold_m},
#'       \code{surface_band_m} and \code{phase_method}. The reference is resolved per deployment;
#'       direction records the configured option, including \code{"both"} where applicable.}
#'     \item{Timing}{\code{start}, \code{end}, \code{duration_s} and integer
#'       \code{n_samples}. Standard pipeline timestamps are \code{POSIXct}.}
#'     \item{Depth}{\code{max_depth_m}, \code{max_depth_time}, \code{baseline_depth_m},
#'       \code{amplitude_m}, \code{prominence_m}, \code{mean_depth_m} and \code{sd_depth_m}.
#'       Baseline depth is the reference at the first dive sample; see Details for the
#'       distinction between maximum depth and the reported extremum time.}
#'     \item{Phase timing and depth}{\code{descent_duration_s}, \code{bottom_duration_s},
#'       \code{ascent_duration_s}, \code{bottom_depth_mean_m}, \code{bottom_depth_sd_m}
#'       and \code{phase_structure}. Structure concatenates \code{D}, \code{B} and \code{A}
#'       for phases with positive duration, or is \code{"X"} when none has positive duration.}
#'     \item{Vertical kinematics}{\code{descent_rate_mean}, \code{descent_rate_q90},
#'       \code{ascent_rate_mean}, \code{ascent_rate_q90}, \code{vertical_distance_m}
#'       and integer \code{n_reversals}. Rates are in metres per second; distance is in metres.
#'       Reversals count depth-direction changes meeting the recorded
#'       \code{wiggle.amplitude}, not exclusively bottom-phase movements.}
#'     \item{Inter-dive interval}{\code{inter_dive_s} measures the next dive's start minus
#'       the current dive's end; logical \code{inter_dive_censored} identifies long time or
#'       depth interruptions in that interval. Both are \code{NA} for the last dive.}
#'     \item{Record completeness}{Logical \code{complete}, \code{truncated_start} and
#'       \code{truncated_end}; integer \code{n_gaps}; \code{gap_s}; and \code{censoring},
#'       one of \code{"none"}, \code{"boundary"}, \code{"time_gap"}, \code{"depth_gap"}
#'       or \code{"mixed"}.}
#'     \item{Analytical support}{Numeric \code{depth_attenuation} and \code{depth_coverage},
#'       and logical \code{shape_supported}. See Details for their distinct interpretations.}
#'   }
#'   Dive durations and intervals are in seconds, depth quantities in metres, and phase/channel
#'   means and standard deviations retain their source units. An empty result preserves the schema.
#'   With shape classification enabled, five columns are added before channel summaries:
#'   \describe{
#'     \item{\code{dive_shape}}{Character: \code{"V"}, \code{"U"}, \code{"W"},
#'       \code{"other"}, or \code{NA} when classification abstains.}
#'     \item{\code{dive_shape_status}}{Character. \code{"classified"} for V/U/W,
#'       \code{"intermediate"} for a single peak between broadness thresholds, or
#'       \code{"complex_profile"} for substantial movement below the endpoint chord.
#'       Abstention reasons are \code{"censored"}, \code{"noncontiguous"},
#'       \code{"invalid_time"}, \code{"missing_reference"}, \code{"low_coverage"},
#'       \code{"gap"}, \code{"insufficient_samples"}, \code{"insufficient_resolution"},
#'       \code{"insufficient_limbs"} or \code{"ambiguous_direction"}.}
#'     \item{\code{shape_broadness}}{Numeric normalised profile area on \code{[0, 1]},
#'       not a proportion of bottom-phase samples.}
#'     \item{\code{shape_n_peaks}}{Integer number of qualifying peaks, including the main
#'       excursion peak, after prominence and separation screening.}
#'     \item{\code{shape_prominence_m}}{Numeric effective rise/fall criterion in metres,
#'       including the relative, absolute, noise and quantisation floors.}
#'   }
#'   Descriptors are \code{NA} when preparation abstains before they can be calculated.
#'   The \code{shape_classification} attribute records the method \code{"profile_rules"},
#'   algorithm version and validated control settings, including on zero-row results.
#'
#' @seealso [detectDives()] for sample-level annotation; [diveControl()] for detection and
#'   phase criteria; [diveShapeControl()] for optional geometric classification;
#'   [plotDives()] for per-dive plots; [plotDepthProfiles()] for source profiles;
#'   [summarizeTagData()] for deployment-level summaries.
#'
#' @examples
#' \dontrun{
#' processed_files <- list.files("data/processed", pattern = "\\.rds$", full.names = TRUE)
#' dives <- detectDives(
#'   processed_files,
#'   control = diveControl(depth.threshold = 5, surface.band = 1, min.duration = 20)
#' )
#' metrics <- diveMetrics(
#'   dives, variables = c("temp", "vedba", "heading"), by.phase = TRUE,
#'   shape = diveShapeControl()
#' )
#' table(metrics$dive_shape, useNA = "ifany")
#' table(metrics$dive_shape_status)
#'
#' # Select uncensored observations; choose a coverage criterion for the study
#' observed <- subset(metrics, complete & depth_coverage >= 0.95)
#' plotDives(observed, metrics = c("amplitude_m", "duration_s"))
#'
#' # Assess interval completeness separately from dive completeness
#' intervals <- subset(
#'   metrics, !is.na(inter_dive_censored) & !inter_dive_censored
#' )
#' }
#' @export

diveMetrics <- function(data,
                        variables          = NULL,
                        circular.variables = c("heading", "roll"),
                        statistics         = c("mean", "sd"),
                        by.phase           = FALSE,
                        id.col             = "ID",
                        datetime.col       = "datetime",
                        depth.col          = "depth",
                        verbose            = "detailed",
                        shape              = NULL) {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  statistics <- match.arg(statistics, c("mean", "sd"), several.ok = TRUE)
  .assert_flag(by.phase, "by.phase")
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  .assert_string(depth.col, "depth.col")
  if (!is.null(shape))
    shape <- .as_control(shape, diveShapeControl, "nautilus_dive_shape", "shape")
  if (!is.null(variables) && (!is.character(variables) || !length(variables)))
    .abort("{.arg variables} must be a non-empty character vector of column names, or {.code NULL}.")
  if (!is.null(circular.variables) && !is.character(circular.variables))
    .abort("{.arg circular.variables} must be a character vector, or {.code NULL}.")
  if (length(variables) > 10)
    cli::cli_warn(c("{length(variables)} variables requested; the table gains {length(variables) * (if (by.phase) 8 else 2)} columns.",
                    "i" = "Consider summarising a subset."))

  src <- .resolveInput(data, id.col)
  .log_header(lvl, "diveMetrics", "Summarising each detected dive",
              bullets = sprintf("Input: %d deployment%s\u00b7%s", src$n, if (src$n != 1) "s " else " ",
                                if (is.null(variables)) " depth and phase metrics only"
                                else sprintf(" plus %d channel%s", length(variables),
                                             if (length(variables) != 1) "s" else "")))

  rows <- list(); n_dep <- 0L; n_missing <- 0L
  magnetic_heading_ids <- character(0)   # heading referenced to magnetic north (see the guard below)
  pb <- .log_progress_start(lvl, src$n, "Reducing")
  for (i in seq_len(src$n)) {
    .log_progress_step(pb)
    x <- data.table::as.data.table(src$get(i))
    id <- as.character(.getMeta(x)$id %||% src$ids[i])
    # a per-dive MEAN ANGLE of heading reports an absolute direction, so it rotates with an uncorrected
    # declination; the mean resultant length reported beside it does not. Collected here, warned about
    # once below, and only when a mean angle of heading was actually requested.
    if (identical(.headingReference(.getMeta(x)), "magnetic"))
      magnetic_heading_ids <- c(magnetic_heading_ids, id)
    if (!all(c("dive_id", "dive_phase", datetime.col, depth.col) %in% names(x))) {
      n_missing <- n_missing + 1L; next
    }
    r <- .diveMetricsOne(x, id, datetime.col, depth.col, variables, circular.variables,
                         statistics, by.phase, shape)
    if (!is.null(r) && nrow(r)) { rows[[length(rows) + 1L]] <- r; n_dep <- n_dep + 1L }
  }
  .log_progress_done(pb)

  if ("heading" %in% circular.variables)
    .warnMagneticHeading(magnetic_heading_ids, intersect(statistics, "mean"), "Per-dive mean heading")

  if (n_missing > 0)
    cli::cli_warn(c("{n_missing} deployment{?s} lack{?s/} the {.field dive_id} column and {?was/were} skipped.",
                    "i" = "Run {.fn detectDives} first."))
  if (!length(rows)) {
    if (lvl >= 1L) {
      .log_summary(lvl); .log_done(lvl, 0L, " dives summarised")
      .log_runtime(lvl, start.time)
    }
    return(.diveShapeResult(
      .diveMetricsSchema(variables, circular.variables, statistics, by.phase, shape), shape
    ))
  }
  out <- do.call(rbind, rows); rownames(out) <- NULL

  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, nrow(out), " dive", if (nrow(out) != 1) "s", " summarised across ", n_dep,
              " deployment", if (n_dep != 1) "s")
    ok <- sum(out$shape_supported, na.rm = TRUE)
    .log_arrow(lvl, sprintf("phase structure resolved for %s of %s dive%s",
                            format(ok, big.mark = ","), format(nrow(out), big.mark = ","),
                            if (nrow(out) != 1) "s" else ""))
    if (!is.null(shape)) {
      n_classified <- sum(out$dive_shape_status == "classified")
      .log_arrow(lvl, sprintf("V/U/W shapes classified for %d of %d dives", n_classified, nrow(out)))
      counts <- table(out$dive_shape, useNA = "no")
      if (length(counts))
        .log_detail(lvl, paste(sprintf("%s: %d", names(counts), as.integer(counts)), collapse = " \u00b7 "))
      reasons <- table(out$dive_shape_status[out$dive_shape_status != "classified"])
      if (length(reasons))
        .log_detail(lvl, paste("shape diagnostics:",
                              paste(sprintf("%s: %d", names(reasons), as.integer(reasons)), collapse = " \u00b7 ")))
    }
    if (length(unique(out$reference)) > 1)
      .log_detail(lvl, sprintf("mixed reference across the cohort: %s",
                               paste(sprintf("%s x%d", names(table(out$reference)),
                                             as.integer(table(out$reference))), collapse = " \u00b7 ")))
    # Flag unusually long dives rather than splitting them: for a fish or shark a multi-hour excursion
    # may be entirely real, and truncating it would be worse than reporting an outlier. Coverage is
    # printed alongside so a genuine foray is distinguishable at a glance from a sensor dropout.
    if (nrow(out) >= 5L) {
      lim <- stats::median(out$duration_s, na.rm = TRUE) +
             5 * stats::mad(out$duration_s, na.rm = TRUE)
      long <- which(is.finite(out$duration_s) & out$duration_s > max(lim, 2 * 3600))
      if (length(long)) {
        cov_txt <- sprintf("%.0f%%", 100 * stats::median(out$depth_coverage[long], na.rm = TRUE))
        .log_detail(lvl, sprintf("%d unusually long dive%s (max %.1f h, median depth coverage %s) - not split",
                                 length(long), if (length(long) != 1) "s" else "",
                                 max(out$duration_s[long], na.rm = TRUE) / 3600, cov_txt))
        if (any(out$depth_coverage[long] < 0.5, na.rm = TRUE))
          .log_subdetail(lvl, "low coverage: check these are forays and not sensor dropouts")
      }
    }
    n_trunc <- sum(out$truncated_start | out$truncated_end, na.rm = TRUE)
    n_gapped <- sum(out$n_gaps > 0, na.rm = TRUE)
    if (n_trunc + n_gapped > 0)
      .log_detail(lvl, sprintf("censored: %d truncated at a record boundary \u00b7 %d gap-interrupted",
                               n_trunc, n_gapped))
    .log_runtime(lvl, start.time)
  }
  .diveShapeResult(out, shape)
}
