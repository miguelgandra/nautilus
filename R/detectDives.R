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

  .log_header(lvl, "detectDives", "Detecting vertical excursions in the depth record",
              bullets = sprintf("Input: %d deployment%s \u00b7 reference %s \u00b7 direction %s",
                                src$n, if (src$n != 1) "s" else "",
                                control$reference, control$direction))

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

  ## ---- pass 2: detect ---------------------------------------------------------------------------
  data_list <- vector("list", src$n); saved <- vector("list", src$n); ids <- rep(NA_character_, src$n)
  n_done <- 0L; tot_dives <- 0L; statuses <- character(0)
  collect_diag <- isTRUE(plot) || !is.null(plot.file)      # opt-in: nothing gathered unless asked
  diag_bundles <- vector("list", src$n)

  for (i in seq_len(src$n)) {
    x <- data.table::as.data.table(src$get(i))
    id <- as.character(.getMeta(x)$id %||% src$ids[i]); ids[i] <- id
    if (lvl >= 2L) .log_h2(lvl, sprintf("%s (%d/%d)", id, i, src$n))

    res <- .detectDivesOne(x, scan[[i]], settings, control, datetime.col, depth.col, lvl, id)
    statuses <- c(statuses, res$status)
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
    if (lvl >= 2L) .log_ok(lvl, id, " \u00b7 ", res$n_dives, " dive", if (res$n_dives != 1) "s",
                           " \u00b7 ", res$status)
  }

  ## ---- summary ----------------------------------------------------------------------------------
  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, n_done, " of ", src$n, " deployment", if (src$n != 1) "s", " processed")
    .log_arrow(lvl, "reference: ", settings$reference_note)
    .log_arrow(lvl, sprintf("threshold: %.2f m (%s) \u00b7 band %.2f m \u00b7 prominence %.2f m \u00b7 min duration %.0f s (%s)",
                            settings$depth.threshold, settings$threshold_source, settings$surface.band,
                            settings$min.prominence, settings$min.duration, settings$duration_source))
    .log_arrow(lvl, sprintf("dives: %s across %d deployment%s", format(tot_dives, big.mark = ","),
                            n_done, if (n_done != 1) "s" else ""))
    if (any(statuses != "applied")) {
      tb <- table(statuses[statuses != "applied"])
      .log_detail(lvl, sprintf("non-standard outcomes: %s",
                               paste(sprintf("%s x%d", names(tb), as.integer(tb)), collapse = " \u00b7 ")))
    }
    if (!is.null(output.dir)) .log_arrow(lvl, "output: ", output.dir)
    .log_runtime(lvl, start.time)
  }

  if (collect_diag) .renderDiveDiagnostic(diag_bundles, plot = plot, plot.file = plot.file)

  .collectOutput(data_list, saved, return.data, ids)
}
