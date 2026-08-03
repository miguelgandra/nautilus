#######################################################################################################
# Dive detection ######################################################################################
#######################################################################################################

#' Detect dives in a depth record
#'
#' @description
#' A depth trace is a continuous series, but most questions asked of it are about discrete events: how
#' deep, how long, how often, and what the animal was doing at the bottom. Turning one into the other
#' requires a definition of a dive, and the definitions in the literature are mostly taxon-specific -
#' built for animals that surface to breathe, and unusable for a fish that never comes shallow or a ray
#' that rests on the bottom.
#'
#' This function uses one definition throughout, and makes the taxonomy a choice rather than an
#' assumption. A dive is a vertical excursion away from a *reference level*, detected by two-threshold
#' hysteresis with a prominence criterion, and ended by a return to within a band of that reference. What
#' changes between taxa is where the reference sits and which way the animal departs from it:
#'
#' - **air-breathers** - seals, penguins, turtles, cetaceans: `reference = "surface"`, where the zero is
#'   anchored by the animal's own return to the surface.
#' - **fish that never surface**: `reference = "baseline"`, a running level that excursions depart from.
#'   A fixed surface threshold would report a single dive spanning the whole record for an animal that
#'   never comes shallow.
#' - **benthic resters** - nurse sharks, wobbegongs, rays, flatfish: `direction = "up"`, because their
#'   excursions leave the bottom rather than the surface.
#'
#' Three columns are added, and always all three: `dive_id`, which is `0L` outside any dive, `dive_phase`,
#' which has an explicit `inter_dive` level, and `depth_baseline`. Their presence never depends on the
#' settings, and `dive_id` and `dive_phase` are never NA. `depth_baseline` is NA only for a deployment
#' the detector had to abstain on, which is reported.
#'
#' @param data Processed data: a tag object, a list of them, a single table with an `id.col`, or a
#'   character vector of `.rds` paths.
#' @param control A control object from [diveControl()] governing what counts as a dive - the reference
#'   level, the excursion direction and the thresholds. Pass `diveControl(...)` to change it.
#' @param id.col Which column identifies the animal (default `"ID"`).
#' @param datetime.col Which column holds the timestamps (default `"datetime"`).
#' @param depth.col Which column holds the depth record (default `"depth"`).
#' @param plot Whether to draw the diagnostics to the active graphics device. Default `FALSE`.
#' @param plot.file Path to a PDF holding one diagnostic panel per deployment, showing the depth trace
#'   with the detected dives, the reference level and the thresholds marked. Default `NULL`.
#' @param return.data Whether to return the annotated data (default `TRUE`) or the written file paths.
#' @param output.dir Directory in which to write one annotated `.rds` file per deployment. Providing a
#'   directory is what triggers saving; `NULL` (default) writes nothing.
#' @param output.suffix Optional suffix appended to each saved file name, before `.rds`. Only used when
#'   `output.dir` is set.
#' @param compress Compression for the saved `.rds` files: `TRUE` (default, gzip), `FALSE`, or one of
#'   `"gzip"`, `"bzip2"` or `"xz"`. See [base::saveRDS()].
#' @param verbose How much detail to print: `0`/`"quiet"`, `1`/`"normal"`, or `2`/`"detailed"`
#'   (default).
#'
#' @details
#' ## Why `dive_id` is `0L` between dives, and not NA
#'
#' An NA would propagate. It survives arithmetic silently, it makes `dive_id > 0` return NA rather than
#' `FALSE`, and it is exactly what any `na.omit()` in your own pipeline deletes - so the inter-dive
#' samples, which are half the behaviour of interest, would vanish without a message. `0L` keeps the
#' column integer and makes "in a dive" a test that always answers, while the explicit `inter_dive`
#' factor level keeps `table(dive_phase)` complete and `split()` well defined.
#'
#' ## The labels do not travel into a feature table
#'
#' [extractFeatures()] returns the deployment identifier, the timestamp and the features it derived;
#' `dive_id` and `dive_phase` are not among them, with or without `downsample.to`. To model something
#' per dive or per phase, extract the features first and join the labels on the timestamp afterwards. Do
#' that join with the intervals in mind: once features are binned, a bin can straddle a dive boundary,
#' and which label it should then carry is a question about your analysis rather than one this package
#' should answer for you.
#'
#' ## The derived threshold is a floor, not an estimate
#'
#' When `depth.threshold` is left `NULL` the function derives the smallest excursion the *record* can
#' support, from the zero-offset residual and the noise of the stored series, and prints how it arrived
#' at the value. That is a property of the instrument and the processing, not of the animal. Set the
#' threshold from your study system, and choose it before looking at your response variable.
#'
#' ## Downsampling limits what is measurable
#'
#' Averaging samples into bins is a boxcar filter, and a boxcar attenuates any excursion short relative
#' to its width. The only setting that reaches the stored depth channel this way is
#' `processTagData(downsample.to = )`; `smoothingControl(depth = )` does not, because that window
#' conditions only the series vertical velocity is differentiated from. `min.duration` therefore
#' defaults to a floor derived from the downsampling bin rather than from any smoothing window, and the
#' function prints which one it used. [diveMetrics()] reports the surviving bound for each dive as
#' `depth_attenuation`. To reach shorter dives, re-process at a finer `downsample.to`.
#'
#' ## Zero dives is a result, not a failure
#'
#' It is reported together with the threshold that produced it, the observed depth range and the
#' reference used. The threshold is never relaxed until dives appear.
#'
#' @return The input with `dive_id`, `dive_phase` and `depth_baseline` added, or, when
#'   `return.data = FALSE`, the written file paths, invisibly.
#'
#' @references
#' Halsey LG, Bost C-A, Handrich Y (2007) A thorough and quantified method for classifying seabird
#' diving behaviour. *Polar Biology* 30:991-1004. \doi{10.1007/s00300-007-0257-3}
#'
#' Hagihara R, Jones RE, Sheppard JK, Hodgson AJ, Marsh H (2011) Minimizing errors in the analysis of
#' dive recordings from shallow-diving animals. *Journal of Experimental Marine Biology and Ecology*
#' 399:173-181. \doi{10.1016/j.jembe.2011.01.001}
#'
#' Luque SP, Fried R (2011) Recursive filtering for zero offset correction of diving depth time series
#' with GNU R package diveMove. *PLoS ONE* 6(1):e15850. \doi{10.1371/journal.pone.0015850}
#'
#' Wilson RP, Puetz K, Charrassin J-B, Lage J (1995) Artifacts arising from sampling interval in dive
#' depth studies of marine endotherms. *Polar Biology* 15:575-581. \doi{10.1007/BF00239649}
#'
#' @seealso [diveControl()] for what counts as a dive; [diveMetrics()] for reducing the result to one
#'   row per dive; [plotDives()] and [plotDepthProfiles()] for looking at it; [processTagData()] for the
#'   step that must come first.
#'
#' @examples
#' \dontrun{
#' tag <- detectDives(processed, control = diveControl(depth.threshold = 5))
#'
#' # A fish that never surfaces, or a benthic rester leaving the bottom
#' tag <- detectDives(processed, control = diveControl(reference = "baseline", direction = "up"))
#' }
#' @export

detectDives <- function(data,
                        control       = diveControl(),
                        id.col        = "ID",
                        datetime.col  = "datetime",
                        depth.col     = "depth",
                        plot          = FALSE,
                        plot.file     = NULL,
                        return.data   = TRUE,
                        output.dir    = NULL,
                        output.suffix = NULL,
                        compress      = TRUE,
                        verbose       = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  control <- .as_control(control, diveControl, "nautilus_dive", "control")
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  .assert_string(depth.col, "depth.col")
  .assert_flag(plot, "plot"); .assert_flag(return.data, "return.data")
  .assert_writable_file(plot.file, "plot.file", ext = "pdf")

  src <- .resolveInput(data, id.col)

  # The header frame is left OPEN: half the detection settings are derived from the cohort by the scan
  # below, so they cannot be reported until it has run. The progress bar drawn in between erases itself,
  # so the finished header still reads as one block.
  .log_header(lvl, "detectDives", "Detecting vertical excursions in the depth record",
              bullets = sprintf("Input: %d deployment%s", src$n, if (src$n != 1) "s" else ""),
              close = FALSE)

  ## ---- pass 1: gather what the DERIVED settings need, across the whole cohort -------------------
  # The floor is derived ONCE over all deployments (the maximum), never per deployment, so a cohort's
  # dive counts stay comparable by construction.
  scan <- vector("list", src$n)
  pb <- .log_progress_start(lvl, src$n, "Scanning")
  for (i in seq_len(src$n)) {
    .log_progress_step(pb)
    x <- data.table::as.data.table(src$get(i))
    scan[[i]] <- .diveScanOne(x, id.col, datetime.col, depth.col, src$ids[i])
  }
  .log_progress_done(pb)

  usable <- Filter(function(z) isTRUE(z$usable), scan)
  if (!length(usable))
    .abort(c("No deployment has usable {.field {depth.col}} + {.field {datetime.col}} data.",
             "i" = "Check the {.arg depth.col} / {.arg datetime.col} column names."))

  settings <- .diveDeriveSettings(usable, control, lvl)
  .reportDiveSettings(lvl, settings, control)
  .log_header_close(lvl)

  ## ---- pass 2: detect ---------------------------------------------------------------------------
  data_list <- vector("list", src$n); saved <- vector("list", src$n); ids <- rep(NA_character_, src$n)
  n_done <- 0L; tot_dives <- 0L; statuses <- character(0)
  refs <- rep(NA_character_, src$n)                     # resolved reference, for the cohort split
  risks <- vector("list", src$n)                        # baseline-estimator risks, grouped at the end
  collect_diag <- isTRUE(plot) || !is.null(plot.file)      # opt-in: nothing gathered unless asked
  diag_bundles <- vector("list", src$n)

  for (i in seq_len(src$n)) {
    x <- data.table::as.data.table(src$get(i))
    id <- as.character(.getMeta(x)$id %||% src$ids[i]); ids[i] <- id
    # a blank line BETWEEN blocks, but not before the first: the header already closes with one
    if (lvl >= 2L) { if (i > 1L) cli::cli_text(""); .log_h2(lvl, sprintf("%s (%d/%d)", id, i, src$n)) }

    res <- .detectDivesOne(x, scan[[i]], settings, control, datetime.col, depth.col, lvl, id)
    statuses <- c(statuses, res$status)
    refs[i] <- res$reference
    if (!is.null(res$risk)) risks[[i]] <- c(res$risk, list(id = id))
    if (lvl >= 2L) .reportDiveDeployment(lvl, res, settings, auto = identical(settings$reference, "per-deployment"))
    tot_dives <- tot_dives + res$n_dives

    # the three columns are added ALWAYS, even for an unusable deployment, so the schema never varies
    x[, dive_id := res$dive_id]
    x[, dive_phase := res$dive_phase]
    x[, depth_baseline := res$baseline]

    meta <- .getMeta(x)
    meta <- .appendProcessing(meta, "detectDives",
                              reference = res$reference, direction = control$direction,
                              depth_threshold_m = settings$depth.threshold,
                              surface_band_m = settings$surface.band,
                              min_amplitude_m = settings$min.amplitude,
                              min_prominence_m = settings$min.prominence,
                              min_duration_s = settings$min.duration,
                              max_gap_s = settings$max.gap,
                              wiggle_amplitude_m = settings$wiggle.amplitude,
                              threshold_source = settings$threshold_source,
                              phase_method = control$phase.method,
                              baseline_stat = control$baseline.stat,
                              n_dives = res$n_dives, status = res$status)
    x <- .restoreMeta(x, meta)

    if (collect_diag)
      diag_bundles[[i]] <- .captureDiveDiag(id, .asTimeSeconds(x[[datetime.col]]),
                                            .asNumericSafe(x[[depth.col]]), res$baseline,
                                            res$dive_id, res$dive_phase, settings,
                                            .asNumericSafe(x[[depth.col]]) - res$baseline, control)

    saved[i] <- list(.saveOutput(x, id, output.dir = output.dir,
                                 output.suffix = output.suffix, compress = compress))
    data_list[[i]] <- x
    n_done <- n_done + 1L
    if (lvl >= 2L) {
      .log_ok(lvl, format(res$n_dives, big.mark = ","), " dive", if (res$n_dives != 1) "s", " detected")
      if (!is.null(saved[[i]])) .log_ok(lvl, basename(saved[[i]]), " saved")
    }
  }

  ## ---- summary ----------------------------------------------------------------------------------
  # Grouped by kind, not by deployment: one warning per deployment buries a large cohort, and R keeps
  # only the first 50 warnings, so on a 51-deployment run the tail is dropped without trace.
  .warnDiveBaseline(Filter(Negate(is.null), risks), control, src$n)

  if (lvl >= 1L) {
    .log_summary(lvl)
    .reportDiveCohort(lvl, n_done, src$n, refs, tot_dives, statuses, output.dir)
    .log_runtime(lvl, start.time)
  }

  if (collect_diag) .renderDiveDiagnostic(diag_bundles, plot = plot, plot.file = plot.file)

  .collectOutput(data_list, saved, return.data, ids)
}


#' Render the "Detection settings" block: every setting that decides what becomes a dive, in one place.
#'
#' Each row says where its value came from. Which numbers the user chose and which the package inferred
#' from the record is the distinction a methods section needs, and it was previously scattered between
#' the header, a stray technical line and the summary.
#' @param lvl Resolved verbosity.
#' @param settings The resolved settings from `.diveDeriveSettings()`.
#' @param control The user's `diveControl()` object.
#' @keywords internal
#' @noRd
.reportDiveSettings <- function(lvl, settings, control) {
  if (lvl < 1L) return(invisible(NULL))
  src <- function(x) if (identical(x, "user")) "(user)" else "(derived)"

  # min.duration: when derived, say what derived it. A boxcar of the downsampling bin attenuates any
  # excursion short relative to its width, and the floor is what protects against reporting those.
  dur <- if (identical(settings$duration_source, "user")) {
    # a user value BELOW what the binning supports is worth flagging where it happens, not in a warning
    floor_s <- max(4 * (settings$depth_bin %||% 0), 4 * (settings$dt %||% 0), 10)
    if (is.finite(floor_s) && settings$min.duration < floor_s)
      sprintf("%.0f s (user; below the %.0f s the %.3g s binning supports)",
              settings$min.duration, floor_s, settings$depth_bin)
    else sprintf("%.0f s (user)", settings$min.duration)
  } else if (is.finite(settings$depth_bin) && settings$depth_bin > 0) {
    sprintf("%.0f s (derived: 4x the %.3g s downsampling bin)", settings$min.duration, settings$depth_bin)
  } else sprintf("%.0f s (derived)", settings$min.duration)

  rows <- c(
    Reference = as.character(control$reference),
    Direction = as.character(control$direction),
    `Depth threshold` = sprintf("%.2f m %s", settings$depth.threshold, src(settings$threshold_source)),
    `Surface band`    = sprintf("%.2f m %s", settings$surface.band, src(settings$band_source)),
    `Min. amplitude`  = if (identical(settings$amplitude_source, "derived"))
                          sprintf("%.2f m (derived: threshold - band)", settings$min.amplitude)
                        else sprintf("%.2f m (user)", settings$min.amplitude),
    # Inf is the internal sentinel for "never split a W-shaped excursion"; say that, not "Inf m"
    Prominence        = if (!is.finite(settings$min.prominence)) "not applied (excursions never split)"
                        else sprintf("%.2f m %s", settings$min.prominence, src(settings$prominence_source)),
    `Min. duration`   = dur,
    `Max. gap`        = sprintf("%.0f s %s", settings$max.gap, src(settings$gap_source)))

  # only meaningful where "auto" has a decision to make
  if (identical(control$reference, "auto"))
    rows <- c(rows, `Surface criterion` = sprintf("%.2f%% occupancy", 100 * control$min.surface.occupancy))
  # and the estimator only runs where some deployment resolves to a baseline reference
  if (!identical(control$reference, "surface"))
    rows <- c(rows, `Baseline estimator` = as.character(control$baseline.stat))

  .log_section(lvl, "Detection settings")
  .log_rows(lvl, rows)
  invisible(NULL)
}


#' One deployment's block: how the reference was decided, then the outcome.
#'
#' The two reason lines appear only where `reference = "auto"` had a decision to make; with an explicit
#' reference there is nothing to explain and they would be noise on every deployment.
#' @keywords internal
#' @noRd
.reportDiveDeployment <- function(lvl, res, settings, auto) {
  if (lvl < 2L) return(invisible(NULL))
  .log_arrow(lvl, "Reference: ", res$reference)
  if (auto && is.finite(res$occupancy))
    .log_rows(lvl, c(`ZOC status` = if (isTRUE(res$zoc_anchored)) "anchored" else "not anchored",
                     `Surface occupancy` = sprintf("%.2f%% (%.1f m band)",
                                                   100 * res$occupancy, settings$surface.band)),
              min_level = 2L)
  invisible(NULL)
}


#' The SUMMARY block: what happened, in sections. Settings are reported by the header, not repeated here.
#' @keywords internal
#' @noRd
.reportDiveCohort <- function(lvl, n_done, n_total, refs, tot_dives, statuses, output.dir) {
  if (lvl < 1L) return(invisible(NULL))
  tick <- cli::col_green(cli::symbol$tick)

  dep <- c(Processed = sprintf("%d/%d", n_done, n_total))
  n_surf <- sum(refs == "surface", na.rm = TRUE); n_base <- sum(refs == "baseline", na.rm = TRUE)
  if (n_surf + n_base > 0)
    dep <- c(dep, `Surface reference` = format(n_surf), `Baseline reference` = format(n_base))
  .log_section(lvl, "Deployments")
  .log_rows(lvl, dep, symbols = c(tick, rep(cli::symbol$bullet, length(dep) - 1L)))

  n_none <- sum(statuses == "applied_no_dives")
  res <- c(`Dives detected` = format(tot_dives, big.mark = ","),
           `Deployments with dives` = format(n_done - n_none))
  # "1 deployment yielded no dives" rather than "applied_no_dives x1": zero dives is a documented
  # result, not a non-standard outcome, and the raw status string means nothing to a reader
  if (n_none > 0) res <- c(res, `Deployments without dives` = format(n_none))
  other <- statuses[!statuses %in% c("applied", "applied_no_dives")]
  if (length(other)) {
    tb <- table(other)
    res <- c(res, Skipped = paste(sprintf("%d (%s)", as.integer(tb), names(tb)), collapse = ", "))
  }
  .log_section(lvl, "Results")
  .log_rows(lvl, res)

  if (!is.null(output.dir)) {
    .log_section(lvl, "Output")
    .log_rows(lvl, c(Directory = output.dir))
  }
  cli::cli_text("")
  invisible(NULL)
}


#' Raise the baseline-estimator cautions once per kind, naming the deployments they apply to.
#' @param risks Per-deployment risk records, each carrying `id`.
#' @param control The user's `diveControl()` object.
#' @param n_total Cohort size, for context in the message.
#' @keywords internal
#' @noRd
.warnDiveBaseline <- function(risks, control, n_total) {
  if (!length(risks)) return(invisible(NULL))
  cap <- function(txt) if (length(txt) > 10L)
    c(utils::head(txt, 10L), sprintf("(+%d more)", length(txt) - 10L)) else txt

  med <- Filter(function(r) isTRUE(r$median_at_risk), risks)
  if (length(med)) {
    who <- cap(vapply(med, function(r) sprintf("%s (%d%%)", r$id, round(100 * r$duty_cycle)), ""))
    cli::cli_warn(c(
      "The running median baseline sits inside the excursions for {length(med)} of {n_total} deployment{?s}, where they occupy more than half the record.",
      "!" = "{who}",
      "i" = "Use {.code diveControl(baseline.stat = \"quantile\")} for a duty cycle above ~50%."))
  }

  qua <- Filter(function(r) isTRUE(r$quantile_at_risk), risks)
  if (length(qua)) {
    who <- cap(vapply(qua, function(r) sprintf("%s (%.1f m/window)", r$id, r$drift_per_window_m), ""))
    cli::cli_warn(c(
      "The low-quantile baseline tracks the window edge rather than the local level for {length(qua)} of {n_total} deployment{?s}, whose baseline drifts within a window.",
      "!" = "{who}",
      "i" = "Use {.code diveControl(baseline.stat = \"median\")} on a drifting baseline."))
  }
  invisible(NULL)
}
