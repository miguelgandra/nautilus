#######################################################################################################
# Per-dive metrics ####################################################################################
#######################################################################################################

#' Summarise each detected dive
#'
#' @description
#' Reduces the per-sample output of \code{\link{detectDives}} to one row per dive: timing, depth,
#' phase structure, kinematics, and a quality block saying what each row can and cannot support.
#'
#' `variables` summarises ANY per-sample channel over each dive - present or future - with correct
#' circular handling for headings and roll. That single argument is what makes this a general reducer
#' rather than a fixed list of depth statistics.
#'
#' @param data Data annotated by \code{\link{detectDives}}: `.rds` paths, a `nautilus_tag` /
#'   data.frame, or a list of them.
#' @param variables Character vector of per-sample columns to summarise per dive (e.g.
#'   `c("temp", "odba", "tbf_hz")`). `NULL` (default) adds none. Each costs 2 columns, or 8 with
#'   `by.phase = TRUE`.
#' @param circular.variables Character. Which of `variables` are angles in degrees, summarised as a
#'   mean angle and a mean resultant length. Default `c("heading", "roll")`, matching
#'   \code{\link{extractFeatures}}.
#' @param statistics Character. Which statistics to compute for `variables`: any of `"mean"`, `"sd"`.
#' @param by.phase Logical. Also summarise `variables` within descent / bottom / ascent. Default `FALSE`.
#' @param id.col,datetime.col,depth.col Column names for the deployment id, timestamp and depth.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal", or `2`/"detailed" (default).
#'
#' @details
#' \strong{NA semantics, one rule per block.} A NA in a phase or kinematics column means "not supported
#' for this dive", and `shape_supported` says so explicitly. A NA in `inter_dive_s` or
#' `inter_dive_censored` means "last dive in this deployment" - there is no following dive to measure to,
#' so a deployment holding a single dive has both NA throughout. A NA in an auxiliary column means the
#' source channel was absent or entirely NA over that dive. Column presence never varies with anything
#' except `variables` and `by.phase`, so `rbind` across a mixed cohort always succeeds.
#'
#' \strong{Every row says how much of its own dive was actually recorded.} `truncated_start` and
#' `truncated_end` mark a dive that touches the start or the end of the record - the tag began or stopped
#' mid-dive. Such dives are retained and flagged, never dropped, because dropping them shortens the
#' observed duration distribution at the tail. `n_gaps` counts the interruptions bounding (or interior
#' to) the dive and `gap_s` is the seconds of record lost at them; for a depth dropout that is the span
#' of the dark run, not the timestamp step across it. `censoring` names the cause and takes exactly one
#' of five values: `"none"`, `"boundary"` (a record edge), `"time_gap"` (a jump in the timestamps),
#' `"depth_gap"` (the depth channel went dark while sampling continued) or `"mixed"` (more than one of
#' those). `complete` is `TRUE` for `censoring == "none"` and nothing else - filter on it before fitting
#' anything to `duration_s`, and report how many rows that removed.
#'
#' \strong{`inter_dive_censored` asks about the interval, not about the dives that bound it.}
#' `inter_dive_s` is the time from the end of one dive to the start of the next, and
#' `inter_dive_censored` is `TRUE` when the record failed DURING that interval: a jump in the timestamps
#' longer than `max.gap`, or a run of non-finite depth whose span exceeds `max.gap`, lying strictly
#' between the two dives. A shorter dropout leaves the interval uncensored. Nothing about the bounding
#' dives enters it, deliberately: an interval has a dive on each side, so neither of its neighbours can be
#' the dive a record boundary cut short. That is a different question from whether either bounding dive
#' was censored, and the difference is the one that matters: two dives can each be `complete` and still be
#' separated by an 8.7 h blackout, which enters the table as an 8.7 h surface interval describing the
#' sensor rather than the animal. Filter on `!inter_dive_censored` before reading `inter_dive_s` as a
#' surface interval; filtering on the bounding dives' `complete` instead would keep exactly that row.
#'
#' \strong{`depth_coverage` is what tells a foray from a dropout.} It is the fraction of this dive's
#' samples carrying a finite depth. A long dive with high coverage was measured throughout and may be
#' real behaviour; a long dive with low coverage is mostly absent record, and its `duration_s` and
#' `max_depth_m` then describe the dropout rather than the animal. Nothing is split on that basis - see
#' \code{\link{detectDives}} - but the verbose summary flags unusually long dives with their median
#' coverage, so the call stays with you.
#'
#' \strong{The long-dive flag needs at least five dives to exist.} It marks every dive longer than the
#' median duration + 5 x MAD of the POOLED table - every deployment in the call taken together, not each
#' one on its own - floored at 2 h, and the whole block is skipped when that pooled table holds fewer than
#' five rows. A threshold is used at all because "unusually long" is only definable against a
#' distribution; on a handful of dives there is no distribution, and a median and a MAD taken over four
#' values are as likely to be inflated by the outlier as to expose it, so the flag would fire on nothing
#' or on everything. The consequence is about small COHORTS, not small deployments: a four-dive deployment
#' summarised on its own, one of those dives a nine-hour dropout, prints no warning at all, whereas the
#' same four dives inside a ten-deployment call are well past the gate and their outlier is measured
#' against the pooled distribution. The flag is also a verbosity-2 line: it is computed whenever anything
#' is printed at all, but printed only at `verbose = 2` / `"detailed"` (the default), so at
#' `verbose = TRUE` / `1` the check runs silently. On tables that small, or at that verbosity, read
#' `duration_s`, `depth_coverage` and `censoring` yourself.
#'
#' \strong{`depth_attenuation` bounds what a smoother could have taken off this dive.} A centred smoother
#' of length L records a triangular excursion of duration T at a fraction of its true amplitude that
#' depends on which is longer: `1 - L / (2T)` once the dive outlasts the window, and `T / (2L)` while it
#' does not. The two meet at `T = L`, where both give 0.5. Both branches matter, and only using the first
#' hides the regime that hurts: continued below `T = L` it goes negative, so a short dive was reported as
#' 0 - flattened out of the record entirely - when a moving average never does that (at `L = 2T` the true
#' retention is 0.25). Worked at the shipped 10 s window: a 3 m / 8 s dive is recorded as 1.2 m and a
#' 3 m / 20 s dive as 2.25 m. This column is that retention, evaluated with the dive's own `duration_s`
#' and the `depth_smoothing` window \code{\link{processTagData}} recorded for the record. Which rows it
#' actually charges depends on when the record was processed. Current
#' nautilus leaves the stored `depth` channel unsmoothed - the window there conditions only the series
#' vertical velocity is differentiated from - and records that it did so, so every dive measured off a
#' current record reads exactly 1: nothing attenuated the depth these metrics were taken from. LEGACY
#' records, written before nautilus stopped smoothing the stored depth channel, carry no such flag; there
#' the window did reach `depth`, and those rows are charged the real retention, which sits just below 1
#' for all but the shortest dives (at the shipped 10 s window a 46 s dive retains 0.89). It is likewise 1
#' whenever no smoothing window was recorded at all, which means "nothing known to attenuate" rather than
#' "measured as unattenuated". Reading it: 1 means the dive is long enough that the window costs it
#' nothing; 0.6 means up to 40% of `amplitude_m`, `prominence_m` and `max_depth_m` may be missing; 0.2
#' means the window is five times the dive and four fifths of its amplitude may be gone. Act on a low value by excluding those rows from any amplitude comparison, or by
#' re-processing with `smoothingControl(depth = )` lowered and detecting again; nothing is corrected here,
#' because the retention formula holds for a triangle and a real dive is not one.
#'
#' \strong{Rates are reported as `_q90`, not `_max`.} The maximum of a smoothed series is an artefact of
#' the smoothing window, and its magnitude depends on dive duration, so maxima are not comparable
#' between a short dive and a long one even within one animal.
#'
#' \strong{The threshold travels with every row.} `reference`, `direction`, `depth_threshold_m` and
#' `surface_band_m` are columns, not metadata, so a bound cohort table is self-documenting and a
#' published dive count is reproducible from the table alone. When `detectDives(reference = "auto")`
#' resolved differently across deployments, the `reference` column makes that mixture visible.
#'
#' \strong{What is deliberately not computed.} Dive efficiency, aerobic dive limit and dive:pause ratio
#' are air-breather constructs that assume the surface interval is a recovery period. `bottom_duration_s`
#' and `inter_dive_s` are provided; if those constructs are meaningful for your animal, form them
#' yourself rather than have the package assert they apply.
#'
#' @return A `data.frame` (class `nautilus_dive_metrics`), one row per dive, with a fixed schema:
#'   identification, timing, the detection settings that produced the dive, depth, phase structure,
#'   kinematics, a quality block,
#'   and last the requested `variables`. The quality block is, in schema order, `inter_dive_s` (a timing
#'   measure, stored at the head of the block because its censoring flag belongs there),
#'   `inter_dive_censored`, `complete`, `truncated_start`, `truncated_end`, `n_gaps`, `gap_s`,
#'   `censoring`, `depth_attenuation`, `depth_coverage` and `shape_supported` - the last being `TRUE`
#'   when at least two of descent / bottom / ascent were resolved, which is the precondition for every
#'   phase and kinematics column being anything but NA. All are defined in Details.
#' @seealso \link{detectDives}, \link{diveControl}, \link{summarizeTagData}, \link{plotDistributions}
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
  pb <- .log_progress_start(lvl, src$n, "Reducing")
  for (i in seq_len(src$n)) {
    .log_progress_step(pb)
    x <- data.table::as.data.table(src$get(i))
    id <- as.character(.getMeta(x)$id %||% src$ids[i])
    if (!all(c("dive_id", "dive_phase", datetime.col, depth.col) %in% names(x))) {
      n_missing <- n_missing + 1L; next
    }
    r <- .diveMetricsOne(x, id, datetime.col, depth.col, variables, circular.variables,
                         statistics, by.phase)
    if (!is.null(r) && nrow(r)) { rows[[length(rows) + 1L]] <- r; n_dep <- n_dep + 1L }
  }
  .log_progress_done(pb)

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
