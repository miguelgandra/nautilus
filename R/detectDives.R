#######################################################################################################
# Dive detection ######################################################################################
#######################################################################################################

#' Detect vertical excursions in deployment depth records
#'
#' @description
#' Identifies discrete vertical excursions from a surface or running depth reference and annotates
#' each sample with a dive identifier, phase and reference depth. Detection uses two-threshold
#' hysteresis, duration and amplitude criteria, and optional splitting of multi-peaked excursions.
#' [diveControl()] defines the detection and phase-classification settings.
#'
#' Use after [processTagData()] to analyse the corrected depth record. Acceleration and orientation
#' channels are not required, so deployments retained with depth-only processing can be analysed.
#' The annotated datasets can be reduced to one row per dive with [diveMetrics()], inspected with
#' [plotDepthProfiles()], or saved for subsequent analyses.
#'
#' @param data A \code{nautilus_tag} object, a list of deployment datasets, a data frame containing
#'   deployments identified by \code{id.col}, or a character vector of \code{.rds} file paths.
#'   Corrected, quality-checked depth data from [processTagData()] are recommended. Each deployment
#'   must contain a timestamp column and numeric depth in metres, positive downwards. File inputs
#'   are read sequentially in each of the two processing passes.
#' @param control A control object from [diveControl()] specifying the reference, excursion
#'   direction, detection criteria and phase method. A named list of constructor arguments or
#'   \code{NULL} can also be supplied; unspecified settings use the constructor defaults.
#' @param id.col Character. Name of the column identifying deployments, not animals.
#'   Default \code{"ID"}.
#' @param datetime.col Character. Name of the timestamp column. Default \code{"datetime"}.
#'   Standard pipeline inputs use \code{POSIXct} timestamps in chronological order.
#' @param depth.col Character. Name of the depth column in metres, positive downwards.
#'   Default \code{"depth"}.
#' @param plot Logical. Draw detection diagnostics on the active graphics device.
#'   Default \code{FALSE}; see Details.
#' @param plot.file Optional path to a diagnostic PDF. Includes depth profiles, threshold
#'   sensitivity and representative phase classifications for each deployment.
#'   Default \code{NULL}, which writes no diagnostic file.
#' @param return.data Whether to return the annotated datasets in memory (default \code{TRUE})
#'   or return saved \code{.rds} paths invisibly. Use \code{FALSE} with \code{output.dir};
#'   without an output directory, no datasets are saved and there are no saved paths to return.
#' @param output.dir Character. Existing directory in which to write one \code{<id>.rds} file
#'   per deployment. Providing a directory triggers saving, including deployments on which
#'   detection abstained; \code{NULL} (default) writes nothing.
#' @param output.suffix Optional string appended to each saved filename before \code{.rds}.
#'   Used only when \code{output.dir} is supplied. Default \code{NULL}.
#' @param compress Compression passed to [base::saveRDS()]: \code{TRUE} (default, gzip),
#'   \code{FALSE}, \code{"gzip"}, \code{"bzip2"} or \code{"xz"}.
#' @param verbose How much detail to print: \code{0}/\code{"quiet"},
#'   \code{1}/\code{"normal"}, or \code{2}/\code{"detailed"} (default). Normal output reports
#'   resolved settings and batch outcomes; detailed output adds deployment-level diagnostics.
#'   Scientific diagnostic warnings are not suppressed by quiet verbosity.
#'
#' @details
#' ## Depth reference and scientific interpretation
#'
#' A dive is defined here as an excursion relative to a specified reference, not as a
#' taxon-independent behavioural state. A surface reference uses zero metres and requires a
#' defensible depth zero. A running baseline describes departures from the local depth level,
#' which can be more appropriate for animals that remain submerged. Downward, upward or both
#' excursion directions can be selected. See [diveControl()] for automatic reference selection,
#' baseline estimation and the assumptions of each phase method.
#'
#' The function uses the supplied depth channel without correcting its zero, resampling it or
#' overwriting it with a smoothed series. Input rows are not sorted: timestamps should already be
#' chronological, and a regularly sampled record is recommended for baseline and phase estimation.
#' Missing depth must remain missing rather than being treated as a surface observation.
#'
#' ## Detection and interruptions
#'
#' For each selected direction, the signed departure from the reference opens an excursion when
#' it is strictly greater than \code{depth.threshold}. The excursion closes when that departure
#' is strictly less than \code{surface.band}. The opening sample is included; the closing sample
#' is not. This hysteresis reduces repeated crossings caused by fluctuations near a single threshold.
#'
#' Candidate excursions are split at timestamp jumps or runs of non-finite depth longer than
#' \code{max.gap}; missing intervals are not interpolated. Short missing-depth runs carry the
#' current detection state, so an annotated dive can contain samples without valid depth.
#' Optional prominence splitting separates sufficiently distinct sub-peaks within an excursion.
#' It is disabled by default. The resulting intervals must satisfy \code{min.amplitude} and
#' \code{min.duration}, including fragments adjacent to interruptions or record boundaries.
#' No maximum dive duration is imposed.
#'
#' Timing is sample-based: duration is the elapsed time between the first and last retained
#' samples, not a reconstruction of the threshold-crossing times. Censoring, coverage and
#' inter-dive interval diagnostics are reported by [diveMetrics()]; truncated excursions are
#' retained if they meet the detection criteria.
#'
#' ## Shared settings and deployment-specific references
#'
#' Detection first scans all inputs, then derives unspecified numerical settings once from the
#' usable deployments. These settings are shared across the batch. With \code{reference = "auto"},
#' the reference is nevertheless selected separately for each deployment from depth-correction
#' provenance and occupancy of the surface band.
#'
#' Changing the deployments in a call can change derived settings and therefore dive counts.
#' Specify the relevant criteria explicitly when a fixed definition is required across separate
#' batches, and report the resolved settings in scientific analyses. Derived defaults are
#' processing heuristics, not estimates of a biologically meaningful dive threshold.
#'
#' ## Phase labels
#'
#' The factor levels are \code{"descent"}, \code{"bottom"}, \code{"ascent"} and
#' \code{"inter_dive"}. Descent denotes the opening limb away from the reference; ascent denotes
#' the return limb. For upward excursions these labels therefore correspond to physical ascent
#' and descent, respectively. Bottom denotes the region around the excursion extremum, not
#' necessarily proximity to the seabed or a period of feeding or resting.
#'
#' The default vertical-rate method can retain a V-shaped excursion with descent and ascent but
#' no bottom phase. Coarse sampling, depth quantisation or limited within-dive variation can
#' leave transit limbs unresolved. Diagnostics report baseline risks and poorly resolved limbs;
#' they do not automatically change the settings or manufacture a bottom phase.
#' Candidate bottoms are additionally checked for net directional change relative to a smoothed,
#' resolution-filtered vertical path. Predominantly directional intervals are refined towards the
#' extremum rather than treating slow continued transit as bottom. This check is controlled by
#' \code{bottom.max.directionality} and is independent of geometric shape classification.
#' Rounded V-shaped profiles can retain brief bottoms. See [diveControl()] for the exact rule.
#'
#' ## Deployment status and provenance
#'
#' The three annotation columns are added to every deployment, replacing existing columns with
#' the same names. Resolved detection settings, reference, direction, phase method, dive count
#' and status are appended to processing history:
#'
#' \describe{
#'   \item{\code{"applied"}}{Detection completed and retained at least one dive.}
#'   \item{\code{"applied_no_dives"}}{Usable depth and timestamps were available, but no
#'     excursions met the criteria.}
#'   \item{\code{"abstained_no_depth"}}{The required depth or timestamp information was
#'     unavailable or unusable. Zero labels in this case are not evidence of an absence of dives.}
#' }
#'
#' If no deployment has usable depth and timestamps, the call stops with an error.
#' Otherwise, unusable deployments remain in the output with neutral annotations and an
#' abstention status; they are not written to a deployment-exclusion log. Inspect outcomes with
#' [processingHistory()] or [getTagMetadata()] before interpreting deployment-level dive counts.
#'
#' ## Diagnostic plots
#'
#' Optional diagnostics show the depth trace and reference, a threshold-sensitivity sweep and
#' phase labels for representative short, median-duration and long excursions. Full-record traces
#' are decimated for display; detection itself uses the supplied samples. The sensitivity sweep
#' is illustrative and uses the downward direction when \code{direction = "both"}, rather than
#' reproducing the bidirectional dive count.
#'
#' @return With \code{return.data = TRUE}, a named list of annotated deployment datasets,
#'   including for a single input. Original channels and metadata are retained, with a new
#'   processing-history entry and three added or replaced columns:
#'   \describe{
#'     \item{\code{dive_id}}{Integer. Sequential positive identifiers within each deployment;
#'       \code{0L} outside retained dives. Use \code{ID} and \code{dive_id} together to identify
#'       dives across deployments. Never \code{NA}.}
#'     \item{\code{dive_phase}}{Factor with levels \code{"descent"}, \code{"bottom"},
#'       \code{"ascent"} and \code{"inter_dive"}. Never \code{NA}.}
#'     \item{\code{depth_baseline}}{Numeric reference depth in metres: zero for a surface
#'       reference, the estimated running level for a baseline reference, or \code{NA_real_}
#'       when detection abstains.}
#'   }
#'   With \code{return.data = FALSE}, returns the saved file paths invisibly.
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
#' @seealso [diveControl()] for detection and phase settings; [diveMetrics()] for per-dive
#'   summaries and optional shape classification; [diveShapeControl()] for shape criteria;
#'   [plotDepthProfiles()] for annotated depth traces; [plotDives()] for per-dive
#'   distributions; [processTagData()] for depth correction and preprocessing.
#'
#' @examples
#' \dontrun{
#' processed_files <- list.files("data/processed", pattern = "\\.rds$", full.names = TRUE)
#'
#' # Explicit study criteria; automatic reference selection remains deployment-specific
#' dives <- detectDives(
#'   processed_files,
#'   control = diveControl(depth.threshold = 5, surface.band = 1, min.duration = 20)
#' )
#' metrics <- diveMetrics(dives, variables = c("temp", "vedba"), by.phase = TRUE)
#' plotDives(metrics, metrics = c("amplitude_m", "duration_s"))
#'
#' # Upward excursions from a running depth baseline
#' upward_dives <- detectDives(
#'   processed_files,
#'   control = diveControl(reference = "baseline", direction = "up", depth.threshold = 10)
#' )
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
  # Saved controls predating this field use the current default, explicitly recorded below.
  if (!"bottom.max.directionality" %in% names(control))
    control$bottom.max.directionality <- formals(diveControl)$bottom.max.directionality
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
  phase_tally <- vector("list", src$n)                  # realised phase structure, grouped at the end
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
    if (!is.null(res$phases) && res$phases$n > 0L) phase_tally[[i]] <- c(res$phases, list(id = id))
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
                              phase_window_s = if (identical(control$phase.method, "vertical.rate"))
                                                 settings$phase.window else NA_real_,
                              min_phase_duration_s = if (identical(control$phase.method, "vertical.rate"))
                                                 settings$min.phase.duration else NA_real_,
                              rate_crit = control$rate.crit, rate_quantile = control$rate.quantile,
                              bottom_prop = control$bottom.prop,
                              bottom_max_directionality = if (identical(control$phase.method, "vertical.rate"))
                                                            control$bottom.max.directionality else NA_real_,
                              phase_version = 2L,
                              n_bottom_refined = res$phases$n_bottom_refined %||% 0L,
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
  .warnDivePhases(Filter(Negate(is.null), phase_tally), control)

  if (lvl >= 1L) {
    .log_summary(lvl)
    .reportDiveCohort(lvl, n_done, src$n, refs, tot_dives, statuses, output.dir,
                      Filter(Negate(is.null), phase_tally))
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

  # The phase rule's own scales, and only the ones the chosen rule actually uses. Both are seconds, so
  # what the rule demands of the animal reads the same at 1 Hz and at 200 Hz.
  rows <- c(rows, `Phase rule` = as.character(control$phase.method))
  if (identical(control$phase.method, "vertical.rate"))
    rows <- c(rows,
              `Rate window`  = sprintf("%.3g s %s", settings$phase.window,
                                       src(settings$phase_window_source)),
              `Min. phase`   = sprintf("%.3g s %s", settings$min.phase.duration,
                                       src(settings$phase_duration_source)),
              `Bottom directionality` = if (is.null(control$bottom.max.directionality)) "not checked"
                                       else sprintf("at most %.2f", control$bottom.max.directionality))
  else
    rows <- c(rows, `Bottom span` = sprintf("deeper than %.0f%% of amplitude", 100 * control$bottom.prop))

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
  if (!is.null(res$phases) && isTRUE(res$phases$n_bottom_refined > 0L))
    .log_arrow(lvl, res$phases$n_bottom_refined, " candidate bottom interval",
               if (res$phases$n_bottom_refined == 1L) "" else "s", " refined: directional transit")
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
.reportDiveCohort <- function(lvl, n_done, n_total, refs, tot_dives, statuses, output.dir,
                              phase_tally = list()) {
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
  n_refined <- sum(vapply(phase_tally, function(z) z$n_bottom_refined %||% 0L, numeric(1)))
  if (n_refined > 0) res <- c(res, `Candidate bottoms refined` = format(n_refined))
  .log_section(lvl, "Results")
  .log_rows(lvl, res)

  # What the phase rule actually produced, as a tally of D/B/A shorthand. A rule that is not working on
  # a record produces a well-formed table with a whole phase missing and says nothing; printing the
  # realised structure is what makes that visible without having to go looking for it.
  st <- unlist(lapply(phase_tally, function(z) z$structures), use.names = FALSE)
  if (length(st)) {
    tb <- sort(table(st), decreasing = TRUE)
    .log_section(lvl, "Phase structure")
    .log_rows(lvl, stats::setNames(sprintf("%s dive%s (%.0f%%)", format(as.integer(tb), big.mark = ","),
                                           ifelse(as.integer(tb) == 1, "", "s"),
                                           100 * as.integer(tb) / length(st)),
                                   .divePhaseLabel(names(tb))))
  }

  if (!is.null(output.dir)) {
    .log_section(lvl, "Output")
    .log_rows(lvl, c(Directory = output.dir))
  }
  cli::cli_text("")
  invisible(NULL)
}


#' Spell the D/B/A shorthand out, so the tally reads as dive shapes rather than as codes.
#' @keywords internal
#' @noRd
.divePhaseLabel <- function(code) {
  lab <- c(DBA = "descent + bottom + ascent", DA = "descent + ascent (no bottom)",
           DB = "descent + bottom (no ascent)", BA = "bottom + ascent (no descent)",
           D = "descent only", A = "ascent only", B = "bottom only", X = "unclassified")
  out <- unname(lab[code])
  ifelse(is.na(out), code, out)
}


#' Say so when a whole phase is missing from most dives.
#'
#' This is the check that would have caught the failure it exists because of. A phase rule that cannot
#' see a limb does not error and does not produce a malformed table: it produces a perfectly well-formed
#' one in which `ascent` never appears, and every downstream summary then silently describes half a
#' dive. Across five deployments the ascent fraction was exactly 0.0000 and nothing said a word.
#'
#' An empty BOTTOM is deliberately not warned about - a V-shaped dive has no bottom phase, and saying so
#' is the whole reason `"vertical.rate"` is the default. Dives the record cut short are excluded from
#' the count: a dive truncated by the start or end of the deployment legitimately lacks a limb.
#' @param tally Per-deployment phase tallies from `.divePhaseTally()`, each carrying `id`.
#' @param control The user's `diveControl()` object.
#' @keywords internal
#' @noRd
.warnDivePhases <- function(tally, control, frac = 0.5) {
  if (!length(tally)) return(invisible(NULL))
  cap <- function(txt) if (length(txt) > 10L)
    c(utils::head(txt, 10L), sprintf("(+%d more)", length(txt) - 10L)) else txt

  hit <- function(field) Filter(function(z) z$n_judged > 0L && z[[field]] / z$n_judged > frac, tally)
  say <- function(bad, limb, other) {
    if (!length(bad)) return(invisible(NULL))
    who <- cap(vapply(bad, function(z)
      sprintf("%s (%d/%d dives)", z$id, z[[paste0("no_", limb)]], z$n_judged), ""))
    tip <- if (identical(control$phase.method, "vertical.rate"))
      c("i" = "Widen {.code diveControl(phase.window = )} if the depth channel is noisy, or lower {.code rate.crit}. {.code diveControl(phase.method = \"prop.depth\")} splits on depth alone and always returns all three phases - at the cost of reporting a bottom phase on dives that have none.")
    else
      c("i" = "With {.code phase.method = \"prop.depth\"} this means the dives reach their deepest point at one end, which is what a record cut short looks like.")
    cli::cli_warn(c(
      "No {limb} phase was resolved in more than half the dives of {length(bad)} deployment{?s}, so {.field {other}} statistics there describe part of a dive.",
      "!" = "{who}", tip))
  }
  say(hit("no_descent"), "descent", "descent_duration_s")
  say(hit("no_ascent"),  "ascent",  "ascent_duration_s")
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
