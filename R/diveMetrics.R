#######################################################################################################
# Per-dive metrics ####################################################################################
#######################################################################################################

#' Summarise each detected dive
#'
#' @description
#' Once dives have been labelled, most analyses work on dives rather than samples: a distribution of
#' maximum depths, a model of duration against temperature, a comparison of bottom time between
#' individuals. This function performs that reduction, turning the per-sample output of [detectDives()]
#' into one row per dive.
#'
#' Each row carries the dive's timing, depth, phase structure and kinematics, the detection settings
#' that produced it, and a quality block stating what that row can and cannot support - whether the
#' whole dive was recorded, how much of its depth channel was present, and how much of its amplitude
#' the processing could have removed.
#'
#' `variables` summarises any per-sample channel over each dive, with correct circular handling for
#' angles. That one argument is what makes this a general reducer rather than a fixed list of depth
#' statistics.
#'
#' @param data Data annotated by [detectDives()]: a tag object, a list of them, a single table with an
#'   `id.col`, or a character vector of `.rds` paths.
#' @param variables Per-sample columns to summarise for each dive, for example
#'   `c("temp", "odba", "tbf_hz_peaks")`. `NULL` (default) adds none. Each costs two columns, or eight
#'   with `by.phase = TRUE`, so a long list makes for a wide table.
#' @param circular.variables Which of `variables` are angles in degrees, and so must be summarised as a
#'   mean angle and a mean resultant length rather than averaged directly. Default `c("heading", "roll")`,
#'   matching [extractFeatures()].
#' @param statistics Which statistics to compute for `variables`: any of `"mean"` and `"sd"`.
#' @param by.phase Whether to also summarise `variables` separately within descent, bottom and ascent
#'   (default `FALSE`). Useful when a channel is expected to differ between phases, such as activity
#'   during descent against activity on the bottom.
#' @param id.col Which column identifies the animal (default `"ID"`).
#' @param datetime.col Which column holds the timestamps (default `"datetime"`).
#' @param depth.col Which column holds the depth record (default `"depth"`).
#' @param verbose How much detail to print: `0`/`"quiet"`, `1`/`"normal"`, or `2`/`"detailed"`
#'   (default).
#'
#' @details
#' ## What an NA means, one rule per block
#'
#' An NA in a phase or kinematics column means the quantity is not supported for that dive, and
#' `shape_supported` says so explicitly. An NA in `inter_dive_s` or `inter_dive_censored` means this is
#' the last dive in the deployment, so there is no following dive to measure to; a deployment holding a
#' single dive has both NA throughout. An NA in one of the `variables` columns means the source channel
#' was absent, or entirely NA over that dive - except for a circular variable, whose mean angle is also
#' NA when the directions over the dive cancel out, leaving a mean resultant length below 0.1 and so no
#' meaningful average direction to report. Column presence never varies with anything except
#' `variables` and `by.phase`, so binding tables from a mixed cohort always succeeds.
#'
#' ## Every row says how much of its own dive was recorded
#'
#' `truncated_start` and `truncated_end` mark a dive that touches the beginning or the end of the
#' record, meaning the tag started or stopped mid-dive. Such dives are kept and flagged, never dropped,
#' because dropping them shortens the tail of the observed duration distribution.
#'
#' `n_gaps` counts the interruptions bounding or interior to the dive, and `gap_s` is the seconds of
#' record lost at them - for a depth dropout, the span of the dark run rather than the timestamp step
#' across it. `censoring` names the cause and takes exactly one of five values: `"none"`, `"boundary"`
#' for a record edge, `"time_gap"` for a jump in the timestamps, `"depth_gap"` where the depth channel
#' went dark while sampling continued, or `"mixed"` for more than one of those. `complete` is `TRUE`
#' when `censoring` is `"none"` and in no other case. Filter on it before fitting anything to
#' `duration_s`, and report how many rows that removed.
#'
#' ## `inter_dive_censored` asks about the interval, not the dives around it
#'
#' `inter_dive_s` is the time from the end of one dive to the start of the next.
#' `inter_dive_censored` is `TRUE` when the record failed *during* that interval: a jump in the
#' timestamps longer than `max.gap`, or a run of non-finite depth whose span exceeds it, lying strictly
#' between the two dives. A shorter dropout leaves the interval uncensored.
#'
#' Nothing about the bounding dives enters it, and that is deliberate. An interval has a dive on each
#' side, so neither neighbour can be the dive a record boundary cut short. This is a different question
#' from whether either bounding dive was censored, and the difference matters: two dives can each be
#' `complete` and still be separated by a blackout lasting hours, which enters the table as a surface
#' interval describing the sensor rather than the animal. Filter on `!inter_dive_censored` before
#' reading `inter_dive_s` as a surface interval; filtering on the bounding dives' `complete` instead
#' would keep exactly that row.
#'
#' ## `depth_coverage` tells a foray from a dropout
#'
#' It is the fraction of the dive's samples carrying a finite depth. A long dive with high coverage was
#' measured throughout and may be real behaviour; a long dive with low coverage is mostly absent record,
#' and its `duration_s` and `max_depth_m` then describe the dropout rather than the animal. Nothing is
#' split on that basis, but the verbose summary flags unusually long dives with their median coverage,
#' so the call stays with you.
#'
#' ## The long-dive flag needs at least five dives to exist
#'
#' It marks every dive longer than the median duration plus five median absolute deviations of the
#' *pooled* table - every deployment in the call taken together, not each one on its own - with a floor
#' at two hours. The whole block is skipped when that pooled table holds fewer than five rows, because
#' "unusually long" is only definable against a distribution: over four values, a median and a deviation
#' are as likely to be inflated by the outlier as to expose it, so the flag would fire on nothing or on
#' everything.
#'
#' The consequence is about small cohorts rather than small deployments. Four dives summarised on their
#' own, one of them a long dropout, print no warning at all; the same four dives inside a
#' ten-deployment call are past the gate and their outlier is measured against the pooled distribution.
#' The flag is also a detailed-verbosity line: it is computed whenever anything is printed, but shown
#' only at `verbose = 2`, so at `verbose = 1` the check runs silently. On tables that small, or at that
#' verbosity, read `duration_s`, `depth_coverage` and `censoring` yourself.
#'
#' ## `depth_attenuation` bounds what binning could have taken off a dive
#'
#' The only filter reaching the stored `depth` channel is [processTagData()]'s `downsample.to`, which
#' mean-aggregates every numeric channel into bins - and bin-averaging is a boxcar.
#' (`smoothingControl(depth = )` does not reach it: that window conditions only the series vertical
#' velocity is differentiated from.) The bin width is read from the two recorded sampling rates, and
#' only a processed rate below the original counts as evidence that aggregation ran, because
#' downsampling is skipped when the requested rate meets or exceeds the native one. No downsampling, or
#' no sampling provenance, reads exactly 1.
#'
#' Bin-averaging is phase-dependent in a way a centred filter is not: a dive whose apex falls mid-bin
#' survives better than one whose apex lands on a bin boundary, and the difference between them is real.
#' This column reports the bound, not the lucky case. For a triangular excursion of duration \eqn{T}
#' under bins of width \eqn{L}, the worst-case peak retention is \eqn{1 - L/T} once \eqn{T \ge 2L}, and
#' \eqn{T/(4L)} below that, where the bin holds only half the triangle; the two meet at \eqn{T = 2L},
#' both giving 0.5. At a 1 Hz processed rate a 4 s dive keeps at least 0.75 of its amplitude, an 8 s
#' dive 0.875 and a 42 s dive 0.976. At 20 Hz the same dives keep 0.9875, 0.994 and 0.9988 - which is
#' the point of the column. It scales with the choice you made, so it says whether your downsampling
#' mattered rather than asserting that downsampling in general does.
#'
#' Reading it: 1 means nothing binned this dive; 0.6 means up to 40 per cent of `amplitude_m`,
#' `prominence_m` and `max_depth_m` may be missing. Act on a low value by excluding those rows from any
#' amplitude comparison, or by re-processing at a finer `downsample.to` and detecting again. Nothing is
#' corrected here, because the retention holds for a triangle and a real dive is not one - a
#' flat-bottomed dive loses less - so treat it as a bound on the loss rather than an estimate of it. The
#' same bin width sets the derived `min.duration` floor in [detectDives()], so the two agree by
#' construction.
#'
#' ## Rates are reported as `_q90`, not `_max`
#'
#' The maximum of a smoothed series is an artefact of the smoothing window, and its magnitude depends on
#' how long the dive lasted, so maxima are not comparable between a short dive and a long one even
#' within one animal.
#'
#' ## Over what span a rate is measured
#'
#' `descent_rate_*` and `ascent_rate_*` are least-squares slopes of depth against time over the window
#' the phase rule was configured with - `diveControl(phase.window = )`, recorded per deployment and read
#' back from the provenance, or re-derived from the sampling interval for a table annotated by hand.
#' Where the phase rule widened that window adaptively for a particular dive, on a coarse or slow depth
#' channel, the boundaries were cut over a wider span than the rates are measured over; that costs the
#' rate some precision and leaves it unbiased. Not a
#' one-sample difference: dividing one depth quantum by one sampling interval returns the pressure
#' transducer rather than the animal, and on a 20 Hz record it put `descent_rate_q90` at 1.60 m/s where
#' the animal's own rate was nearer 0.2. The signed means were never affected, because that noise is
#' zero-mean; the quantiles were entirely instrument.
#'
#' ## The thresholds travel with every row
#'
#' `reference`, `direction`, `depth_threshold_m` and `surface_band_m` are columns rather than metadata,
#' so a bound cohort table documents itself and a published dive count is reproducible from the table
#' alone. Where `detectDives(reference = "auto")` resolved differently across deployments, the
#' `reference` column makes that mixture visible.
#'
#' ## What is deliberately not computed
#'
#' Dive efficiency, aerobic dive limit and the dive-to-pause ratio are air-breather constructs that
#' assume the surface interval is a recovery period. `bottom_duration_s` and `inter_dive_s` are
#' provided; if those constructs are meaningful for your animal, form them yourself rather than have the
#' package assert that they apply.
#'
#' @return A data frame of class `nautilus_dive_metrics`, one row per dive, with a fixed schema:
#'   identification, timing, the detection settings that produced the dive, depth, phase structure,
#'   kinematics, a quality block, and last the requested `variables`. The quality block holds, in schema
#'   order, `inter_dive_s` - a timing measure, kept at the head of the block because its censoring flag
#'   belongs there - then `inter_dive_censored`, `complete`, `truncated_start`, `truncated_end`,
#'   `n_gaps`, `gap_s`, `censoring`, `depth_attenuation`, `depth_coverage` and `shape_supported`. The
#'   last is `TRUE` when at least two of descent, bottom and ascent were resolved, which is the
#'   precondition for every phase and kinematics column being anything other than NA. All are defined
#'   above.
#'
#' @seealso [detectDives()] for producing the input; [diveControl()] for what counts as a dive;
#'   [plotDives()] and [plotDistributions()] for looking at the result; [summarizeTagData()] for a
#'   deployment-level overview.
#'
#' @examples
#' \dontrun{
#' tag <- detectDives(processed, control = diveControl(depth.threshold = 5))
#' dt  <- diveMetrics(tag, variables = c("temp", "odba"), by.phase = TRUE)
#' plotDistributions(dt, metrics = c("max_depth_m", "duration_s"))
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
                        verbose            = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  statistics <- match.arg(statistics, c("mean", "sd"), several.ok = TRUE)
  .assert_flag(by.phase, "by.phase")
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  .assert_string(depth.col, "depth.col")
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
                         statistics, by.phase)
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
    return(structure(.diveMetricsSchema(variables, circular.variables, statistics, by.phase),
                     class = c("nautilus_dive_metrics", "data.frame")))
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
  structure(out, class = c("nautilus_dive_metrics", "data.frame"))
}
