#######################################################################################################
# Dive detection ######################################################################################
#######################################################################################################

#' Detect dives in a depth record
#'
#' @description
#' Annotates each sample with the dive it belongs to. A dive is a vertical excursion of the depth trace
#' away from a **reference level**, detected by two-threshold hysteresis with a prominence criterion and
#' bounded by a return to within a band of that reference.
#'
#' One definition serves every taxon because the reference is a choice, not an assumption:
#' \itemize{
#'   \item \strong{air-breathers} (seals, penguins, turtles, cetaceans) - `reference = "surface"`, where
#'     the zero is anchored by the animal's own return to the surface.
#'   \item \strong{fish that never surface} - `reference = "baseline"`, a running level the excursions
#'     depart from. A fixed surface threshold finds ONE dive spanning the whole record for an animal
#'     that never comes shallow.
#'   \item \strong{benthic resters} (nurse sharks, wobbegongs, rays, flatfish) - `direction = "up"`,
#'     because their excursions leave the bottom rather than the surface.
#' }
#'
#' Three columns are added and always all three: `dive_id` (`0L` outside any dive), `dive_phase`
#' (with an explicit `inter_dive` level) and `depth_baseline`. Never NA, never conditional - see Details.
#'
#' @param data Processed data: `.rds` paths, a single `nautilus_tag` / data.frame, or a list of them.
#' @param control A \link{diveControl} object (or a list of overrides).
#' @param id.col,datetime.col,depth.col Column names for the deployment id, timestamp and depth.
#' @param plot Logical. Draw diagnostics to the active device. Default `FALSE`.
#' @param plot.file Character. Path to a PDF for the per-deployment diagnostic panels. Default `NULL`.
#' @param return.data Logical. Return the annotated data (`TRUE`, default) or the written paths.
#' @param output.dir Character. Directory to write annotated `.rds` files into. Default `NULL`.
#' @param output.suffix Character. Suffix for written files.
#' @param compress Logical or character passed to `saveRDS()`.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal", or `2`/"detailed" (default).
#'
#' @details
#' \strong{Why `dive_id` is `0L` and not `NA` between dives.} \code{\link{extractFeatures}} runs
#' `na.omit()` over its whole frame, so an NA-between-dives column would silently delete every
#' inter-dive sample from the feature table with no message. `0L` and an explicit `inter_dive` factor
#' level defuse that. Note the remaining half of that interaction is NOT yet defused: passing
#' `downsample.to` to `extractFeatures()` mean-aggregates every column and would produce fractional
#' `dive_id`s. Until that is fixed, extract features first and join dive labels by timestamp second.
#'
#' \strong{The threshold is a floor, not an estimate.} When `depth.threshold` is `NULL` the function
#' derives the smallest excursion the RECORD can support - from the zero-offset residual and the stored
#' series' noise - and prints its derivation. That is a property of the instrument and the processing,
#' not of the animal. Set it from your study system, and choose it before looking at your response
#' variable.
#'
#' \strong{Smoothing limits what is measurable.} A centred smoother attenuates any excursion shorter
#' than its window: at a 10 s window a 3 m / 8 s dive is recorded as 1.2 m. `min.duration` therefore
#' defaults to a floor derived from the `depth_smoothing` used by \code{\link{processTagData}}, and the
#' function says so. Records processed before nautilus stopped smoothing the stored depth channel carry
#' this bias; re-process with `smoothingControl(depth = ...)` lowered to reach shorter dives.
#'
#' \strong{An interruption in the record ends a dive; it is never interpolated across.} Two events count
#' as an interruption, and `max.gap` (seconds) is the longest of either a dive may span: a jump in time
#' between consecutive samples, and a run of samples whose depth is non-finite. They are the same event -
#' the record stopped saying where the animal was - but only the first is visible in the timestamps.
#' Deployment PIN_03's depth channel went dark for 8.72 h while rows kept arriving at 20 Hz, so the
#' sampling interval stayed perfectly regular (median dt == max dt == 0.050 s) and there was no time gap
#' to find; the dive stayed open across the whole dark stretch and was reported as one 8.9 h excursion to
#' 37 m, of which 97.6% of samples carried no depth at all. Counting the dark run as an interruption
#' brings that deployment's longest dive down to 1.14 h. The dive is split there and both parts marked
#' censored, because both alternatives are worse: interpolating invents an excursion shape that was never
#' measured, and dropping the interrupted dive biases the duration distribution against exactly the long
#' dives that interruptions tend to fall inside. \code{\link{diveMetrics}} reports what each dive lost.
#'
#' \strong{Long dives are flagged, never split.} No maximum duration is imposed. For an air-breather a
#' multi-hour dive would be impossible, but this package also serves fish, sharks and rays, for which a
#' multi-hour excursion may be entirely real - a duration cap would quietly turn one true observation
#' into two false ones, and it would do so most often in the taxa the cap was never designed for.
#' \code{\link{diveMetrics}} instead reports dives longer than the median + 5 x MAD (and at least 2 h)
#' with their `depth_coverage` alongside, which is what separates a genuine foray from a sensor dropout.
#'
#' \strong{Zero dives is a result, not a failure.} It is reported with the threshold that produced it,
#' the observed depth range and the reference used. The threshold is never relaxed until dives appear.
#'
#' @return The input with `dive_id`, `dive_phase` and `depth_baseline` added, or (when
#'   `return.data = FALSE`) the written file paths, invisibly.
#'
#' @seealso \link{diveControl}, \link{diveMetrics}, \link{processTagData}, \link{plotDepthProfiles}
#' @references
#' Halsey, L.G., Bost, C.-A. & Handrich, Y. (2007) A thorough and quantified method for classifying
#'   seabird diving behaviour. \emph{Polar Biology} 30:991-1004.
#' Hagihara, R., Jones, R.E., Sheppard, J.K., Hodgson, A.J. & Marsh, H. (2011) Minimising errors in the
#'   analysis of dive recordings from shallow-diving animals. \emph{J. Exp. Mar. Biol. Ecol.} 399:173-181.
#' Luque, S.P. & Fried, R. (2011) Recursive filtering for zero offset correction of diving depth time
#'   series. \emph{PLoS ONE} 6(1):e15850.
#' Wilson, R.P., Puetz, K., Charrassin, J.-B. & Lage, J. (1995) Artefacts arising from sampling interval
#'   in dive depth studies of marine endotherms. \emph{Polar Biology} 15:575-581.
#' @examples
#' \dontrun{
#' tag <- detectDives(processed, control = diveControl(depth.threshold = 5))
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
                              min_prominence_m = settings$min.prominence,
                              min_duration_s = settings$min.duration,
                              max_gap_s = settings$max.gap,
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
