#######################################################################################################
# Reconstruct a 3D pseudo-track by dead reckoning + Verified Position Correction #######################
#######################################################################################################

#' Reconstruct a 3D pseudo-track from orientation, speed and depth
#'
#' @description
#' Reconstructs the fine-scale movement path of a tagged animal by *dead reckoning*: it integrates the
#' per-sample heading and swimming speed produced upstream by \code{\link{processTagData}} into a
#' horizontal (east/north) displacement, attaches the measured depth as the vertical axis, and anchors the
#' accumulating reckoning drift to known positions (deployment, GPS/Argos fixes, pop-up) via Verified
#' Position Correction (VPC).
#'
#' The function is a pure integrator: it consumes the finished `heading` (already tilt-compensated,
#' paddle-de-noised and declination-corrected by \code{\link{processTagData}}) and is agnostic to how that
#' heading was obtained. The result is a \strong{pseudo-track} - a reconstruction, not a georeferenced
#' observation (hence the `pseudo_lon` / `pseudo_lat` columns) - most reliable between closely spaced fixes.
#'
#' @details
#' ## Dead reckoning in brief
#' Satellite fixes (Argos, FastGPS) locate a marine animal only when the tag breaks the surface, which for a
#' diving animal may be minutes to days apart - too coarse to see the actual path. *Dead reckoning*
#' (a term inherited from marine navigation) fills the gaps by stepping the position forward from the last
#' known point using the tag's own motion sensors: at each sample the animal is assumed to move a small
#' distance `speed x dt` in the direction it is `heading`, and these micro-steps are summed into a
#' continuous path (Wilson et al. 2007; Bidder et al. 2015; Gunner et al. 2021). The reconstruction is
#' therefore only as good as its speed and heading inputs, and - crucially - small, systematic heading or
#' speed errors *accumulate*, so the reckoned path slowly drifts away from the truth. That drift is why the
#' output is a *pseudo*-track and why the correction step below matters.
#'
#' The reconstruction proceeds in three steps:
#' \enumerate{
#'   \item \strong{Speed} (m/s) is assigned at every sample from `control$speed.method` - a constant, a
#'     VeDBA-based activity model, a paddle-wheel count, or the dive geometry. See
#'     \code{\link{reconstructTrackControl}} for the trade-offs (speed sets the *scale* of the track; heading
#'     sets its *shape*).
#'   \item \strong{Integration}: the horizontal speed (`speed x cos(pitch)`) is projected onto east/north by
#'     the heading and integrated forward with a spherical (small-angle destination-point) step - latitude
#'     advancing along the meridian and longitude scaled by the running latitude - so the track stays
#'     accurate across latitude excursions. The correction below then operates in a local equirectangular
#'     metre plane about the deployment.
#'   \item \strong{Verified Position Correction (VPC)}: the raw path is reconciled with the known positions
#'     (deployment, GPS/Argos fixes, pop-up). The drift accumulated between two successive fixes is
#'     distributed back across that segment so the corrected path stays continuous and is pulled onto (or
#'     exactly through) each fix, following the running-correction scheme of Gunner et al. (2021). With
#'     `vpc.method = "linear"` the track passes exactly through every fix; with `"error_weighted"` (default)
#'     each fix pulls only as hard as its quality warrants (via `anchor.error.radii`), so a noisy Argos point
#'     bends the track less than a precise FastGPS one; `"scale_rotate"` instead rescales and rotates each
#'     reckoned segment onto its bracketing fixes (the Gundog.Tracks correction, better for systematic drift);
#'     `"none"` returns the uncorrected reckoning. For the additive methods the drift is spread across each
#'     segment by reckoned distance travelled (`vpc.weighting = "distance"`, default) or by elapsed time.
#'     Each reconstructed position also carries a 1-sigma uncertainty (`pseudo_error`, metres): it is ~ the
#'     fix radius at each anchor and swells in between, following the reckoning-error model (`drift.rate`,
#'     `drift.diffusion`) combined forward-and-backward between the bracketing fixes.
#' }
#'
#' ## The vertical axis (3-D)
#' Because the tag also records depth, the reconstruction is genuinely three-dimensional: with
#' `control$include.depth = TRUE` the measured depth is attached as `pseudo_depth`, so `pseudo_lon` /
#' `pseudo_lat` / `pseudo_depth` describe where the animal was in the water column, not just at the surface.
#'
#' ## Interpreting the result, and where to go next
#' A pseudo-track is a plausible reconstruction, not an observation: absolute accuracy is highest just after
#' each fix and degrades with time/distance since the last one. For downstream analyses that need formal
#' per-position uncertainty (e.g. utilisation distributions, state-space behavioural models), hand the
#' corrected track to a continuous-time movement model such as \pkg{aniMotum} (Jonsen et al. 2023) or
#' \pkg{crawl} (Johnson et al. 2008); `reconstructTrack` is designed to feed such tools, not to replace them.
#' Track-level summaries (distance, tortuosity, home range) are available via \code{\link{trackMetrics}}.
#'
#' When a track has \strong{no interior fixes} (only the deployment and pop-up), the two endpoints cannot
#' constrain a wandering interior. A soft \emph{reconstructability} gate (see
#' `reconstructTrackControl$reconstructability.min`) flags such a track with a `warning()` and a verdict in
#' `meta$sensors$reconstructability` when its directedness is too low - the interior geometry should then be
#' treated with great caution. Use \code{\link{crossValidateTrack}} to quantify accuracy where fixes exist.
#'
#' @param data A `nautilus_tag` / data.frame, a (named) list of them, a single aggregated data.frame with
#'   an `id.col` column, or a character vector of paths to `.rds` files (the output of
#'   \code{\link{processTagData}}). Each must carry `datetime`, `heading` and `pitch` (and, per
#'   `speed.method`, `vertical_velocity`, `paddle_speed`, or `vedba`; and `depth` for the 3-D output).
#' @param control A \code{\link{reconstructTrackControl}} object (or a named list of its fields) grouping
#'   the speed and track-correction settings.
#' @param id.col,datetime.col Column names for the animal ID and timestamp. Defaults `"ID"`/`"datetime"`.
#' @param return.data Logical. Return the processed data in memory (default `TRUE`). When `FALSE`, the
#'   function instead returns the paths of the `.rds` files it wrote, which feed directly into the next
#'   step's `data` argument -- so a large fleet can be processed without ever holding it all in memory.
#'   `return.data = FALSE` therefore requires an `output.dir`.
#' @param output.dir Character. Directory in which to write one `<id>.rds` file per deployment. Providing
#'   a directory is what triggers saving; `NULL` (default) writes nothing. The directory must already exist.
#' @param output.suffix Character. Optional suffix appended to each saved file name (before `.rds`), e.g.
#'   to tag a processing run or avoid clashes. Only used when `output.dir` is set. Default `NULL`.
#' @param compress Compression for the saved `.rds` files (only used when `output.dir` is set): `TRUE`
#'   (default, gzip), `FALSE`, or one of `"gzip"`/`"bzip2"`/`"xz"`. See \code{\link[base]{saveRDS}}.
#' @param plot,plot.file Draw the diagnostic report to the active device (`plot = TRUE`) and/or a
#'   multi-page PDF (`plot.file`). The report is a worst-first summary page (per deployment: path length,
#'   net displacement, fixes, residual drift) followed by a detail page per flagged deployment (or all with
#'   `force.plots`): a map of the reconstructed track against the raw dead-reckoned path and the position
#'   fixes, the drift the VPC corrected at each fix, the speed source, and the depth profile. Defaults
#'   `FALSE` / `NULL`.
#' @param force.plots Logical. Draw a detail page for every deployment, not only the high-drift ones.
#'   Default `FALSE`.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet" (silent), `TRUE`/`1`/"normal" (header + a live progress
#'   bar across deployments + summary), or `2`/"detailed" (default; streams a per-deployment block - anchors,
#'   flags - instead of the bar). cli auto-hides the bar for fast runs.
#'
#' @return If `return.data = TRUE`, a named list of the input objects with `pseudo_lon`, `pseudo_lat`,
#'   `speed_dr` (the speed used), `pseudo_error` (the per-sample 1-sigma positional uncertainty in metres)
#'   and - when `control$include.depth` and a depth channel are present - `pseudo_depth` (the 3-D vertical
#'   axis) added; if `return.data = FALSE`, a character vector of the written `.rds` file paths.
#' @references
#' Wilson RP, Liebsch N, Davies IM, *et al.* (2007) All at sea with animal tracks; methodological and
#' analytical solutions for the resolution of movement. *Deep Sea Research Part II*. 54:193-210.
#' \doi{10.1016/j.dsr2.2006.11.017}
#'
#' Bidder OR, Walker JS, Jones MW, *et al.* (2015) Step by step: reconstruction of terrestrial animal
#' movement paths by dead-reckoning. *Movement Ecology*. 3:23. \doi{10.1186/s40462-015-0055-4}
#'
#' Gunner RM, Holton MD, Scantlebury MD, *et al.* (2021) Dead-reckoning animal movements in R: a reappraisal
#' using Gundog.Tracks. *Animal Biotelemetry*. 9:23. \doi{10.1186/s40317-021-00245-z}
#'
#' Johnson DS, London JM, Lea MA, Durban JW (2008) Continuous-time correlated random walk model for animal
#' telemetry data. *Ecology*. 89:1208-1215. \doi{10.1890/07-1032.1}
#'
#' Jonsen ID, Grecian WJ, Phillips L, *et al.* (2023) aniMotum, an R package for animal movement data:
#' rapid quality control, behavioural estimation and simulation. *Methods in Ecology and Evolution*.
#' 14:806-816. \doi{10.1111/2041-210X.14060}
#' @seealso \code{\link{reconstructTrackControl}}, \code{\link{processTagData}}, \code{\link{trackMetrics}},
#'   \code{\link{exportForSSM}}
#' @examples
#' \dontrun{
#' processed <- processTagData(imported)
#' # Dead-reckon the pseudo-track, anchoring accumulated drift to the known position fixes
#' tracks <- reconstructTrack(processed,
#'                            control = reconstructTrackControl(speed.method = "paddle",
#'                                                              vpc.method = "error_weighted"),
#'                            plot = TRUE)
#' trackMetrics(tracks)   # per-animal path summary
#' }
#' @export
reconstructTrack <- function(data,
                             control = reconstructTrackControl(),
                             id.col = "ID",
                             datetime.col = "datetime",
                             return.data = TRUE,
                             output.dir = NULL,
                             output.suffix = NULL,
                             compress = TRUE,
                             plot = FALSE,
                             plot.file = NULL,
                             force.plots = FALSE,
                             verbose = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  control <- .as_control(control, reconstructTrackControl, "nautilus_reconstruct_track", "control")
  .assert_flag(return.data, "return.data")
  .assert_flag(plot, "plot"); .assert_flag(force.plots, "force.plots")
  .assert_writable_file(plot.file, "plot.file", ext = "pdf")
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  .assert_string(output.suffix, "output.suffix", null_ok = TRUE)
  .assert_dir(output.dir, "output.dir"); .assert_compress(compress)
  .assert_output(return.data, output.dir)

  r <- .resolveInput(data, id.col = id.col)
  results <- if (return.data) vector("list", r$n) else NULL
  saved <- vector("list", r$n)
  make_plots <- plot || !is.null(plot.file)
  summary_records <- if (make_plots) vector("list", r$n) else NULL
  payloads <- list()

  hdr <- sprintf("Input: %d dataset%s", r$n, if (r$n != 1) "s" else "")
  if (!is.null(output.dir)) hdr <- c(hdr, paste0("Output: ", output.dir))
  .log_header(lvl, "reconstructTrack", "Reconstructing 3D pseudo-tracks", bullets = hdr,
              arrow = sprintf("%s DR + %s VPC", control$speed.method, control$vpc.method))

  n_ok <- 0L
  untrusted_ids <- character(0); partial_ids <- character(0)   # heading-calibration trust of the input (see below)
  pb <- .log_progress_start(lvl, r$n, "Reconstructing", min.level = 1L, max.level = 1L)   # NORMAL only (detailed streams)
  for (i in seq_len(r$n)) {
    .log_progress_step(pb)
    x <- r$get(i)
    if (!data.table::is.data.table(x)) x <- data.table::as.data.table(x)
    who <- tryCatch(as.character(unique(x[[id.col]])[1]), error = function(e) NA_character_)
    if (length(who) != 1L || is.na(who) || !nzchar(who)) who <- r$ids[i]
    .log_h2(lvl, sprintf("%s (%d/%d)", who, i, r$n))

    # heading-calibration trust: the track is only as good as the heading it integrates. A raw (uncalibrated)
    # field carries a hard-iron offset that biases every heading and compounds into drift, so surface it.
    trust <- .headingTrust(.getMeta(x))
    if (identical(trust, "untrusted"))    untrusted_ids <- c(untrusted_ids, who)
    else if (identical(trust, "partial")) partial_ids   <- c(partial_ids, who)

    res <- tryCatch(.reconstructTrackOne(x, control, datetime.col, lvl, who, make_plots),
                    error = function(e) { .log_skip(lvl, conditionMessage(e)); NULL })
    if (!is.null(res)) {
      n_ok <- n_ok + 1L
      d_txt <- if ("pseudo_depth" %in% names(res)) " (3D)" else ""
      .log_detail(lvl, sprintf("track derived%s \u00b7 %d fixes anchored", d_txt, attr(res, "n_anchors") %||% 0L))
      if (make_plots) {
        pl <- attr(res, "plot_payload")
        summary_records[[i]] <- .reconstructSummaryRecord(pl)
        if (!is.null(pl) && (force.plots || r$n == 1L || isTRUE(pl$stats$flag)))
          payloads[[length(payloads) + 1L]] <- pl
      }
      attr(res, "plot_payload") <- NULL                       # not persisted with the data
      saved_to <- .saveOutput(res, r$ids[i], output.dir = output.dir,
                              output.suffix = output.suffix, compress = compress)
      saved[i] <- list(saved_to)
      if (!is.null(saved_to)) .log_ok(lvl, paste0("saved ", basename(saved_to)))
      if (return.data) results[[i]] <- res
    }
    .log_gap(lvl); rm(x)
  }
  .log_progress_done(pb)

  # heading-trust warnings (aggregated, emitted once). Untrusted = a raw uncalibrated field -> loud; partial
  # = a diagonal/2D/low-confidence fit -> a lighter note. Both compound into dead-reckoning drift.
  if (length(untrusted_ids))
    cli::cli_warn(c(
      "{length(untrusted_ids)} pseudo-track{?s} dead-reckoned from an UNCALIBRATED magnetometer: {.val {utils::head(untrusted_ids, 8)}}.",
      "!" = "The heading carries the tag's hard-iron offset; absolute bearings and the reckoned path will drift badly.",
      "i" = "Calibrate first ({.fn calibrateMagnetometer}, ideally with {.arg calibration.data}); see {.code meta$mag_calibration$status}."))
  if (length(partial_ids) && lvl >= 1L)
    cli::cli_inform(c("i" = "{length(partial_ids)} track{?s} built from a partial (diagonal/low-confidence) magnetometer calibration: {.val {utils::head(partial_ids, 8)}} - headings are usable but not fully corrected."))

  # diagnostic report: worst-first summary page, then a track map + panels per flagged deployment
  if (make_plots) {
    stats <- list(n = r$n, n_ok = n_ok)
    draw <- function(to.file = FALSE, unicode = TRUE) {
      .drawReconstructSummaryPage(Filter(Negate(is.null), summary_records), stats)
      for (pl in payloads) .plotReconstructIndividual(pl)
    }
    .renderToDevices(draw, plot = plot, plot.file = plot.file, width = 10, height = 7.5)
  }

  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, n_ok, " of ", r$n, " pseudo-track", if (r$n != 1) "s", " reconstructed")
    if (!is.null(output.dir)) .log_arrow(lvl, "output: ", output.dir)
    .log_runtime(lvl, start.time)
  }
  keep <- if (return.data) !vapply(results, is.null, logical(1)) else !vapply(saved, is.null, logical(1))
  .collectOutput(results[keep], saved[keep], return.data, r$ids[keep])
}

#' Reconstruct one deployment's pseudo-track (dead reckoning + VPC). Returns the annotated data.table with
#' an `n_anchors` attribute, or NULL (via a thrown, caught message) when it cannot be built.
#' @keywords internal
#' @noRd
.reconstructTrackOne <- function(dt, control, datetime.col, lvl, id = NA_character_, make_plots = FALSE,
                                 holdout = integer(0)) {
  dt <- .withPositionColumns(dt)                             # reconstruct lon/lat fix columns from ancillary
  meta <- .getMeta(dt)

  req <- c(datetime.col, "heading", "pitch")
  if (control$speed.method == "paddle")     req <- c(req, "paddle_speed")
  if (control$speed.method == "depth_rate") req <- c(req, "vertical_velocity")
  if (control$speed.method == "vedba")      req <- c(req, "vedba")
  missing_cols <- setdiff(req, names(dt))
  if (length(missing_cols)) stop("missing column(s): ", paste(missing_cols, collapse = ", "))

  deploy_lat <- meta$deployment$lat; deploy_lon <- meta$deployment$lon
  if (is.null(deploy_lat) || is.null(deploy_lon) || is.na(deploy_lat) || is.na(deploy_lon))
    stop("no deployment coordinates")

  # per-step elapsed seconds: shared by the speed models (VeDBA straight-segment calibration) and the
  # displacement integration below. Non-positive/undefined gaps take the median positive step.
  dt_sec <- c(0, as.numeric(diff(dt[[datetime.col]]), units = "secs"))
  dt_sec[!is.finite(dt_sec) | dt_sec < 0] <- stats::median(dt_sec[dt_sec > 0], na.rm = TRUE)

  # known positions (deployment origin, pop-up, GPS/Argos fixes): one source of truth reused by the VeDBA
  # speed auto-calibration and the VPC correction further below
  pos_anchors <- .positionAnchors(dt, meta, datetime.col, deploy_lat, deploy_lon)
  # cross-validation hook: drop the held-out fix row(s) from the anchor set (never the deployment origin), so
  # neither the VPC nor the VeDBA speed fit sees them - the reconstructed position at that row is then a
  # genuine out-of-sample prediction to score against the withheld fix (see crossValidateTrack).
  if (length(holdout)) pos_anchors <- pos_anchors[!(idx %in% holdout & quality != "Deploy")]

  # 1. instantaneous swimming speed (m/s) - stored as the along-body speed; the horizontal component is
  # recovered as speed_dr * cos(pitch) at the integration step, uniformly across methods.
  speed_fit <- NULL
  if (control$speed.method == "paddle") {
    # A paddle channel often runs slower than the inertial grid (CEiiA logs it at 1 Hz against 20 Hz), and
    # regularizeTimeSeries deliberately leaves those rows empty rather than inventing samples that a pooled
    # statistic would then double-count. Dead reckoning DOES need a speed at every integration step, so the
    # continuous signal is reconstructed here, at the consumer that needs it: interpolate within the
    # channel's own gaps. Without this the generic fallback below would silently replace ~95% of a 1 Hz
    # paddle record with the constant speed, quietly gutting the reconstruction.
    sp <- dt$paddle_speed
    if (anyNA(sp) && sum(is.finite(sp)) >= 2L) sp <- zoo::na.approx(sp, na.rm = FALSE)
    dt[, speed_dr := sp]
  } else if (control$speed.method == "depth_rate") {
    # vertical -> along-body speed via the dive geometry (vertical_velocity = speed * sin(pitch)), so the
    # horizontal component recovered downstream is vertical_velocity / tan(pitch). Trustworthy only on steep
    # glides: near horizontal 1/sin(pitch) explodes and a small pitch error yields a wild speed, so samples
    # shallower than depth.rate.min.pitch are set NA and back-filled with the constant fallback rather than
    # fabricating a huge speed (Wensveen et al. 2015).
    steep <- is.finite(dt$pitch) & abs(dt$pitch) >= control$depth.rate.min.pitch
    sp <- abs(dt$vertical_velocity) / sin(abs(dt$pitch) * (pi / 180))
    sp[!steep] <- NA_real_
    dt[, speed_dr := sp]
  } else if (control$speed.method == "vedba") {
    speed_fit <- .vedbaSpeedModel(dt, pos_anchors, datetime.col, dt_sec, control)
    if (isTRUE(speed_fit$fallback)) {
      dt[, speed_dr := control$constant.speed]
      .log_skip(lvl, sprintf("VeDBA speed calibration unavailable (%s); using constant %.2f m/s",
                             speed_fit$reason, control$constant.speed))
    } else {
      dt[, speed_dr := speed_fit$a + speed_fit$b * vedba]
      .log_detail(lvl, sprintf("VeDBA speed = %.3f + %.3f x VeDBA  (%s%s)", speed_fit$a, speed_fit$b,
        speed_fit$meta$source,
        if (is.finite(speed_fit$meta$r2)) sprintf(", %d segments, R2 %.2f", speed_fit$meta$n_seg, speed_fit$meta$r2) else ""))
    }
  } else {
    dt[, speed_dr := control$constant.speed]
  }

  # optional activity/rest gating: hold speed at 0 while the animal is at rest (VeDBA in its low tail), so
  # reckoning drift does not accumulate during inactivity (Gunner et al. 2021)
  if (!is.null(control$rest.quantile)) {
    if ("vedba" %in% names(dt)) {
      thr <- stats::quantile(dt$vedba, control$rest.quantile, na.rm = TRUE)
      if (is.finite(thr)) dt[is.finite(vedba) & vedba < thr, speed_dr := 0]
    } else {
      .log_skip(lvl, "rest.quantile set but no VeDBA channel; speeds left ungated")
    }
  }

  dt[!is.finite(speed_dr), speed_dr := control$constant.speed]
  dt[speed_dr > control$max.speed, speed_dr := control$max.speed]
  dt[speed_dr < 0, speed_dr := 0]

  R_earth <- 6371000

  # 2. integrate horizontal velocity into the raw dead-reckoned track. The spherical (small-angle
  # destination-point) integration advances latitude along the meridian and longitude scaled by the RUNNING
  # latitude, so it stays accurate across latitude excursions (unlike a fixed-parallel flat earth). The raw
  # lat/lon track is then projected into a local per-point-latitude equirectangular metre plane about the
  # deployment, in which the VPC operates; the correction is projected back to lon/lat at the end.
  heading_rad <- dt$heading * (pi / 180); pitch_rad <- dt$pitch * (pi / 180)
  v_horiz <- dt$speed_dr * cos(pitch_rad)
  d_e <- v_horiz * sin(heading_rad) * dt_sec; d_e[!is.finite(d_e)] <- 0
  d_n <- v_horiz * cos(heading_rad) * dt_sec; d_n[!is.finite(d_n)] <- 0
  raw <- .deadReckonSpherical(d_e, d_n, deploy_lon, deploy_lat, R_earth)
  lp  <- .projLocal(raw$lon, raw$lat, deploy_lon, deploy_lat, R_earth)
  dt[, `:=`(disp_e = lp$e, disp_n = lp$n)]

  # reconstructability gate: for a track with NO interior fixes (anchored only by deploy [+ pop-up]), warn
  # when its directedness - net deploy->pop-up displacement over the reckoned path length - is too low, i.e.
  # the animal's net progress was a small fraction of how far it swam, so the endpoints cannot constrain the
  # wandering interior. A soft, tunable heuristic (validated vs held-out error); it never aborts.
  # skip the gate when fixes are being held out (crossValidateTrack) - the reduced anchor set is artificial,
  # so its endpoint-bounded assessment would be spurious
  recon <- if (!length(holdout)) .reconstructabilityGate(dt, pos_anchors, deploy_lon, deploy_lat, control, id, lvl) else NULL
  if (!is.null(recon)) { if (is.null(meta$sensors)) meta$sensors <- list(); meta$sensors$reconstructability <- recon }

  n_anchors <- 0L; anchors <- NULL

  if (control$vpc.method != "none" && !is.null(pos_anchors) && nrow(pos_anchors) >= 1L) {
    # attach the reckoned displacement at each known position and its true east/north (projected with the
    # same local map) - the residual between the two is the drift the VPC redistributes
    anchors <- data.table::copy(pos_anchors)
    anchors[, `:=`(disp_e = dt$disp_e[idx], disp_n = dt$disp_n[idx])]
    ap <- .projLocal(anchors$lon, anchors$lat, deploy_lon, deploy_lat, R_earth)
    anchors[, `:=`(true_e = ap$e, true_n = ap$n)]
    n_anchors <- nrow(anchors)

    dt[, `:=`(corr_e = disp_e, corr_n = disp_n)]
    if (nrow(anchors) > 1L && control$vpc.method == "scale_rotate") {
      # Gundog-style correction: per segment, rescale + rotate the whole reckoned sub-path so it is pinned at
      # the previous fix and lands exactly on the next (a similarity transform of the step vectors). Best for
      # SYSTEMATIC drift (a speed-scale or heading-bias error); ignores fix quality / drift.rate / weighting.
      sr <- .vpcScaleRotate(dt$disp_e, dt$disp_n, anchors, as.numeric(dt[[datetime.col]]))
      dt[, `:=`(corr_e = sr$e, corr_n = sr$n)]
    } else if (nrow(anchors) > 1L) {
      # additive redistribution with a running CARRY, so the correction is continuous across anchors and
      # lands on each fix (forced through exactly when linear/gain=1; elastically pulled when error_weighted).
      carry_e <- 0; carry_n <- 0
      for (a in seq_len(nrow(anchors) - 1L)) {
        si <- anchors$idx[a]; ei <- anchors$idx[a + 1L]
        tspan <- as.numeric(difftime(dt[[datetime.col]][ei], dt[[datetime.col]][si], units = "secs"))
        if (tspan <= 0) next
        gain <- 1.0
        if (control$vpc.method == "error_weighted") {
          r_gps <- .anchorRadius(anchors$quality[a + 1L], control$anchor.error.radii)
          r_dr  <- .driftError(tspan, control)                # reckoning error over the segment (m)
          gain  <- (r_dr^2) / (r_dr^2 + r_gps^2)              # 1D Kalman-style trust in the fix
        }
        err_e <- anchors$true_e[a + 1L] - (dt$disp_e[ei] + carry_e)   # residual to the fix from the carried DR
        err_n <- anchors$true_n[a + 1L] - (dt$disp_n[ei] + carry_n)
        # spread the drift across the segment: by reckoned distance travelled (default) or elapsed time. Both
        # run 0 -> 1 across the segment, so the correction stays continuous and lands on the fix at ei.
        if (control$vpc.weighting == "distance") {
          steplen <- c(0, sqrt(diff(dt$disp_e[si:ei])^2 + diff(dt$disp_n[si:ei])^2))
          cumlen  <- cumsum(steplen); total <- cumlen[length(cumlen)]
          w <- if (is.finite(total) && total > 0) cumlen / total
               else as.numeric(difftime(dt[[datetime.col]][si:ei], dt[[datetime.col]][si], units = "secs")) / tspan
        } else {
          w <- as.numeric(difftime(dt[[datetime.col]][si:ei], dt[[datetime.col]][si], units = "secs")) / tspan
        }
        dt[si:ei, corr_e := disp_e + carry_e + gain * err_e * w]
        dt[si:ei, corr_n := disp_n + carry_n + gain * err_n * w]
        carry_e <- carry_e + gain * err_e; carry_n <- carry_n + gain * err_n
      }
      # tail after the last fix: keep dead-reckoning FROM the last corrected position by carrying the final
      # accumulated offset forward, rather than snapping back to the origin-relative raw path at the next sample
      last_i <- anchors$idx[nrow(anchors)]
      if (last_i < nrow(dt)) dt[(last_i + 1L):.N, `:=`(corr_e = disp_e + carry_e, corr_n = disp_n + carry_n)]
    }
    bk <- .unprojLocal(dt$corr_e, dt$corr_n, deploy_lon, deploy_lat, R_earth)
    dt[, `:=`(pseudo_lon = bk$lon, pseudo_lat = bk$lat)]
    dt[, c("corr_e", "corr_n") := NULL]
  } else {
    bk <- .unprojLocal(dt$disp_e, dt$disp_n, deploy_lon, deploy_lat, R_earth)
    dt[, `:=`(pseudo_lon = bk$lon, pseudo_lat = bk$lat)]
  }

  # per-sample positional uncertainty (m, 1 sigma): reckoning error pinned to the known fixes. Between two
  # anchors it is the forward/backward reckoning-error combination (Brownian-bridge / RTS-smoother style),
  # ~ the fix radius at each anchor and bulging in between; with vpc = "none" it grows monotonically from
  # the deployment. Uses only timestamps + anchor radii, so it is computed after the correction above.
  unc_anchors <- if (control$vpc.method != "none") anchors else pos_anchors[1L]
  dt[, pseudo_error := .reckonUncertainty(dt, unc_anchors, control, datetime.col)]

  # build the (bounded) diagnostic payload while the raw displacement is still available
  pl <- if (make_plots) .reconstructPayload(dt, id, deploy_lon, deploy_lat, R_earth, control, datetime.col, anchors) else NULL
  dt[, c("disp_e", "disp_n") := NULL]

  # 3. vertical axis: attach the measured depth as z for a genuine 3-D pseudo-track
  if (isTRUE(control$include.depth) && "depth" %in% names(dt)) dt[, pseudo_depth := depth]

  proc_args <- list(meta, "reconstructTrack", speed_method = control$speed.method,
                    vpc = control$vpc.method, vpc_weighting = control$vpc.weighting, n_anchors = n_anchors,
                    drift_rate = control$drift.rate, drift_diffusion = control$drift.diffusion,
                    dim = if (isTRUE(control$include.depth) && "depth" %in% names(dt)) "3D" else "2D")
  if (!is.null(speed_fit)) {                                 # record how the VeDBA speed model was obtained
    fm <- speed_fit$meta
    proc_args <- c(proc_args, list(vedba_speed_source = fm$source, vedba_speed_intercept = fm$a,
                   vedba_speed_slope = fm$b, vedba_speed_r2 = fm$r2, vedba_speed_n_seg = fm$n_seg))
  }
  meta <- do.call(.appendProcessing, proc_args)
  dt <- .restoreMeta(dt, meta)
  attr(dt, "n_anchors") <- n_anchors
  if (!is.null(pl)) attr(dt, "plot_payload") <- pl
  dt
}

#' Spherical (small-angle destination-point) dead-reckoning integration. Given per-step east/north ground
#' displacements (metres), advances latitude along the meridian (`dlat = dn / R`) and longitude scaled by the
#' running latitude (`dlon = de / (R cos(lat))`), both by cumulative sum. This is the destination-point
#' formula to O((step/R)^2) - negligible for biologging step sizes - while staying vectorised. Correctly
#' tracks the changing E-W scale over a latitude excursion, unlike a fixed-parallel flat earth. Returns the
#' raw dead-reckoned `lon`/`lat` (decimal degrees).
#' @keywords internal
#' @noRd
.deadReckonSpherical <- function(d_e, d_n, lon0, lat0, R) {
  lat <- lat0 + cumsum(d_n) / R * (180 / pi)
  lon <- lon0 + cumsum(d_e / (R * cos(lat * pi / 180))) * (180 / pi)
  list(lon = lon, lat = lat)
}

#' Forward local map: lon/lat -> east/north metres in a per-point-latitude equirectangular plane about
#' (`lon0`, `lat0`). Self-consistent inverse of \code{.unprojLocal}: northing is the meridian arc, easting
#' uses the point's own latitude, so that reckoned positions and fixes share one metric for the VPC. The
#' longitude difference is wrapped to (-180, 180], so a fix just across the antimeridian projects to its true
#' short-way easting (no single biologging step or inter-fix segment spans > 180 deg).
#' @keywords internal
#' @noRd
.projLocal <- function(lon, lat, lon0, lat0, R) {
  dlon <- ((lon - lon0 + 180) %% 360) - 180
  list(e = dlon * (pi / 180) * R * cos(lat * pi / 180),
       n = (lat - lat0) * (pi / 180) * R)
}

#' Inverse local map: east/north metres -> lon/lat (decimal degrees). Recovers latitude from the northing
#' (meridian), then longitude from the easting scaled by that latitude - the exact inverse of
#' \code{.projLocal}; the returned longitude is wrapped to (-180, 180].
#' @keywords internal
#' @noRd
.unprojLocal <- function(e, n, lon0, lat0, R) {
  lat <- lat0 + n / R * (180 / pi)
  lon <- lon0 + e / (R * cos(lat * pi / 180)) * (180 / pi)
  list(lon = ((lon + 180) %% 360) - 180, lat = lat)
}

#' Collect a deployment's known-position anchors (deployment origin, pop-up, and genuine GPS/Argos fixes) in
#' time order - the single source of truth shared by the VeDBA speed auto-calibration and the VPC
#' correction. Returns a data.table (idx, time, lat, lon, quality); the deployment origin (idx 1) is always
#' present. The pop-up is bound before the sensor fixes so that, on an index collision, `unique(by = "idx")`
#' keeps Deploy > Popup > fix; rows that `.withPositionColumns` injects (position_type "Metadata ...") are
#' excluded, since deploy/pop-up are already added explicitly here.
#' @keywords internal
#' @noRd
.positionAnchors <- function(dt, meta, datetime.col, deploy_lat, deploy_lon) {
  anchors <- data.table::data.table(idx = 1L, time = dt[[datetime.col]][1], lat = deploy_lat,
                                    lon = deploy_lon, quality = "Deploy")
  if (!is.null(meta$deployment$popup_lat) && !is.null(meta$deployment$popup_lon) &&
      !is.na(meta$deployment$popup_lat) && !is.na(meta$deployment$popup_lon)) {
    pi_ <- which.min(abs(as.numeric(difftime(dt[[datetime.col]], meta$deployment$popup_datetime, units = "secs"))))
    if (length(pi_))
      anchors <- rbind(anchors, data.table::data.table(idx = pi_, time = dt[[datetime.col]][pi_],
        lat = meta$deployment$popup_lat, lon = meta$deployment$popup_lon, quality = "Popup"))
  }
  if (all(c("lat", "lon") %in% names(dt))) {
    pt <- if ("position_type" %in% names(dt)) as.character(dt$position_type) else rep(NA_character_, nrow(dt))
    fix <- which(!is.na(dt$lat) & !is.na(dt$lon) & !(!is.na(pt) & startsWith(pt, "Metadata")))
    if (length(fix))
      anchors <- rbind(anchors, data.table::data.table(idx = fix, time = dt[[datetime.col]][fix],
        lat = dt$lat[fix], lon = dt$lon[fix],
        quality = if ("quality" %in% names(dt)) as.character(dt$quality[fix]) else "Unknown"))
  }
  anchors <- unique(anchors, by = "idx"); data.table::setorder(anchors, idx)
  anchors
}

#' Resolve the VeDBA-to-speed model for `speed.method = "vedba"`. A user-supplied `control$vedba.model`
#' (length-2 numeric `c(intercept, slope)`) is accepted as-is; otherwise the model is auto-calibrated from
#' the deployment's own position fixes (Gunner et al. 2021): each pair of consecutive fixes gives a
#' straight-line (great-circle) speed, kept only when the intervening path was near-straight (so the
#' straight-line distance approximates the true swum distance), and speed is regressed on the interval-mean
#' VeDBA. Returns `list(fallback = FALSE, a, b, meta)` on success, or `list(fallback = TRUE, reason, meta)`
#' when the calibration is under-determined or non-physical - in which case the caller uses
#' `constant.speed`. `meta` records the source, coefficients, R-squared and segment count for the log/metadata.
#' @keywords internal
#' @noRd
.vedbaSpeedModel <- function(dt, pos_anchors, datetime.col, dt_sec, control,
                             straight.min = 0.9, min.seconds = 60, min.dist.m = 100, min.segments = 4L) {
  fb <- function(reason) list(fallback = TRUE, reason = reason,
    meta = list(source = "constant_fallback", a = NA_real_, b = NA_real_, r2 = NA_real_, n_seg = 0L))
  m <- control$vedba.model
  if (is.numeric(m) && length(m) == 2L)
    return(list(fallback = FALSE, a = m[[1]], b = m[[2]],
                meta = list(source = "user", a = m[[1]], b = m[[2]], r2 = NA_real_, n_seg = NA_integer_)))
  if (!"vedba" %in% names(dt)) return(fb("no VeDBA channel"))
  if (is.null(pos_anchors) || nrow(pos_anchors) < 3L) return(fb("too few position fixes"))

  hdg <- dt$heading * (pi / 180); pit <- dt$pitch * (pi / 180)
  vseg <- numeric(0); xseg <- numeric(0)
  for (a in seq_len(nrow(pos_anchors) - 1L)) {
    si <- pos_anchors$idx[a]; ei <- pos_anchors$idx[a + 1L]
    if (ei <= si) next
    tsec <- as.numeric(difftime(pos_anchors$time[a + 1L], pos_anchors$time[a], units = "secs"))
    if (!is.finite(tsec) || tsec < min.seconds) next
    dist_m <- .trackDistance(pos_anchors$lon[a], pos_anchors$lat[a],
                             pos_anchors$lon[a + 1L], pos_anchors$lat[a + 1L]) * 1000   # km -> m
    if (!is.finite(dist_m) || dist_m < min.dist.m) next
    seg <- (si + 1L):ei                                       # unit-speed heading path over the interval
    gross <- sum(cos(pit[seg]) * dt_sec[seg], na.rm = TRUE)   # horizontal path length at unit along-body speed
    if (!is.finite(gross) || gross <= 0) next
    dx <- sum(sin(hdg[seg]) * cos(pit[seg]) * dt_sec[seg], na.rm = TRUE)
    dy <- sum(cos(hdg[seg]) * cos(pit[seg]) * dt_sec[seg], na.rm = TRUE)
    straightness <- sqrt(dx^2 + dy^2) / gross                 # 1 = perfectly straight, 0 = closed loop
    if (!is.finite(straightness) || straightness < straight.min) next
    # ALONG-BODY speed, not horizontal: the GPS chord is a horizontal distance, so dividing it by the
    # unit-speed horizontal time-integral (gross = integral of cos(pitch) dt) removes the pitch projection
    # and yields the along-body speed that speed_dr * cos(pitch) re-projects downstream (net ~ gross here,
    # since segments are pre-filtered to straightness >= straight.min). At pitch 0, gross == tsec.
    v <- dist_m / gross
    if (!is.finite(v) || v > control$max.speed) next          # implausible -> a fix outlier, skip
    x <- mean(dt$vedba[si:ei], na.rm = TRUE)
    if (!is.finite(x)) next
    vseg <- c(vseg, v); xseg <- c(xseg, x)
  }
  if (length(vseg) < min.segments) return(fb(sprintf("only %d usable straight segment(s)", length(vseg))))
  if (!is.finite(stats::sd(xseg)) || stats::sd(xseg) <= 0) return(fb("no VeDBA spread across segments"))
  ft <- stats::lm(vseg ~ xseg)
  co <- stats::coef(ft); r2 <- summary(ft)$r.squared
  if (!all(is.finite(co)) || co[[2]] <= 0) return(fb("non-physical calibration (slope <= 0)"))
  list(fallback = FALSE, a = co[[1]], b = co[[2]],
       meta = list(source = "auto", a = co[[1]], b = co[[2]], r2 = r2, n_seg = length(vseg)))
}

#' Expected position error radius (m) for an anchor of the given `quality`, from control$anchor.error.radii;
#' unknown labels fall back to a coarse 1500 m (roughly an Argos class-1 fix).
#' @keywords internal
#' @noRd
.anchorRadius <- function(quality, radii, default = 1500) {
  q <- as.character(quality)
  if (length(q) == 1L && !is.na(q) && q %in% names(radii)) radii[[q]] else default
}

#' Accumulated dead-reckoning error (m) after `t` seconds: sqrt((drift.rate * t)^2 + drift.diffusion * t).
#' The linear term is a systematic (bias-like) drift; the sqrt(t) term is a random-walk (diffusive) drift.
#' With drift.diffusion = 0 this is exactly the linear drift.rate * t model.
#' @keywords internal
#' @noRd
.driftError <- function(t, control) {
  sqrt((control$drift.rate * t)^2 + control$drift.diffusion * t)
}

#' Reconstructability gate: for a track with no genuine interior fixes (anchored only by the deployment
#' [+ pop-up]), assess whether the two endpoints can constrain the interior. `directedness` = net
#' deploy->pop-up displacement / reckoned path length (a scale-free ratio, validated against held-out error);
#' below `control$reconstructability.min` the interior is unreliable, so warn (never abort) and return the
#' verdict for `meta$sensors$reconstructability`. Returns NULL when the gate does not apply (interior fixes
#' present, or gate disabled). `dt` must carry `disp_e`/`disp_n`; `pos_anchors` carries idx/lon/lat/quality.
#' @keywords internal
#' @noRd
.reconstructabilityGate <- function(dt, pos_anchors, deploy_lon, deploy_lat, control, id, lvl) {
  if (!isTRUE(control$reconstructability.min > 0)) return(NULL)
  if (nrow(pos_anchors[!(quality %in% c("Deploy", "Popup"))])) return(NULL)   # interior fixes constrain it
  path_m <- sum(sqrt(diff(dt$disp_e)^2 + diff(dt$disp_n)^2), na.rm = TRUE)     # reckoned path length (m)
  pu <- pos_anchors[quality == "Popup"]
  if (!nrow(pu)) {                                          # no pop-up and no interior fixes: fully unbounded
    warning(sprintf("reconstructTrack: '%s' has no pop-up and no interior fixes - the track is fully unbounded dead-reckoning; absolute positions are unconstrained.", id), call. = FALSE)
    .log_skip(lvl, "reconstructability: fully unbounded (deployment origin only)")
    return(list(endpoint_bounded = TRUE, directedness = NA_real_, net_km = NA_real_,
                path_km = round(path_m / 1000, 2), gate = "unbounded"))
  }
  net_m <- .trackDistance(deploy_lon, deploy_lat, pu$lon[1], pu$lat[1]) * 1000
  # directedness in [0, 1]; capped at 1. When the reckoned path is ~0 (all speeds 0, pitch +/-90) but the
  # pop-up is elsewhere (net > 0), the path->0+ limit is fully directed (1); only a truly stationary track
  # (net 0 and path 0) is 0.
  d <- if (is.finite(path_m) && path_m > 0) min(1, net_m / path_m) else if (is.finite(net_m) && net_m > 0) 1 else 0
  gate <- if (d < control$reconstructability.min) "warn" else "pass"
  if (gate == "warn") {
    warning(sprintf("reconstructTrack: '%s' is anchored only by its endpoints; the net displacement is %.0f%% of the reckoned path (directedness %.2f < %.2f) - the interior geometry is unreliable, treat the shape between the endpoints with great caution.",
                    id, 100 * d, d, control$reconstructability.min), call. = FALSE)
    .log_skip(lvl, sprintf("reconstructability: interior unreliable (directedness %.2f < %.2f, endpoints only)", d, control$reconstructability.min))
  }
  list(endpoint_bounded = TRUE, directedness = round(d, 3), net_km = round(net_m / 1000, 2),
       path_km = round(path_m / 1000, 2), gate = gate)
}

#' Gundog-style scale + rotate Verified Position Correction (Gunner et al. 2021). Between successive anchors
#' the reckoned sub-path is transformed by a single uniform scale (`|net_VP| / |net_DR|`) and rotation
#' (`angle(net_VP) - angle(net_DR)`) about the previous (corrected) anchor, so the sub-path SHAPE is
#' preserved while its far end is pinned exactly onto the next fix. In this flat displacement domain the
#' transform lands on each fix exactly in one pass (no iteration needed, unlike the spherical original).
#' Best for systematic drift (a mis-scaled speed = pure scale, a heading bias = pure rotation). The scale is
#' ill-conditioned when the net reckoned displacement is small relative to the reckoned path length (a
#' stationary or near-closed/tortuous segment): a large scale factor would then magnify the interior wiggles.
#' Such segments (and coincident fixes) instead fall back to additive redistribution, which preserves the
#' reckoned shape and still lands exactly on the fix. The tail past the last anchor continues the last
#' segment's transform. `anchors` must carry integer `idx` and the anchor coordinates `true_e`/`true_n`.
#' Returns list(e, n) of full-length corrected displacements (metres, relative to the deployment). `ts` =
#' epoch seconds; `straight.min` = the minimum net/arc ratio for a segment to be scaled+rotated.
#' @keywords internal
#' @noRd
.vpcScaleRotate <- function(disp_e, disp_n, anchors, ts, eps = 1e-6, straight.min = 0.1) {
  n <- length(disp_e)
  ce <- disp_e; cn <- disp_n
  idx <- anchors$idx; te <- anchors$true_e; tn <- anchors$true_n
  f_last <- 1; ca_last <- 1; sa_last <- 0; last_i <- idx[1L]
  for (a in seq_len(nrow(anchors) - 1L)) {
    si <- idx[a]; ei <- idx[a + 1L]; if (ei < si) next
    seg <- si:ei
    rel_e <- disp_e[seg] - disp_e[si]; rel_n <- disp_n[seg] - disp_n[si]
    dxDR <- disp_e[ei] - disp_e[si]; dyDR <- disp_n[ei] - disp_n[si]
    dxVP <- te[a + 1L] - te[a];      dyVP <- tn[a + 1L] - tn[a]
    lenDR <- sqrt(dxDR^2 + dyDR^2);  lenVP <- sqrt(dxVP^2 + dyVP^2)
    arc   <- sum(sqrt(diff(disp_e[seg])^2 + diff(disp_n[seg])^2))   # reckoned path length within the segment
    f <- lenVP / lenDR
    if (is.finite(f) && lenDR >= eps && lenVP >= eps && lenDR >= straight.min * arc) {
      # well-conditioned: scale + rotate the whole sub-path about the previous fix
      dAng <- atan2(dyVP, dxVP) - atan2(dyDR, dxDR); ca <- cos(dAng); sa <- sin(dAng)
      ce[seg] <- te[a] + f * (ca * rel_e - sa * rel_n)
      cn[seg] <- tn[a] + f * (sa * rel_e + ca * rel_n)
      f_last <- f; ca_last <- ca; sa_last <- sa
    } else {
      # ill-conditioned (stationary / near-closed / coincident fixes): additively redistribute the endpoint
      # error along the reckoned sub-path (distance-weighted, time as a fallback) - shape kept, fix still hit
      steplen <- c(0, sqrt(diff(disp_e[seg])^2 + diff(disp_n[seg])^2)); cuml <- cumsum(steplen)
      tot <- cuml[length(cuml)]
      w <- if (is.finite(tot) && tot > 0) cuml / tot
           else if (ts[ei] > ts[si]) (ts[seg] - ts[si]) / (ts[ei] - ts[si]) else seq(0, 1, length.out = length(seg))
      ce[seg] <- te[a] + rel_e + w * (dxVP - dxDR)
      cn[seg] <- tn[a] + rel_n + w * (dyVP - dyDR)
      f_last <- 1; ca_last <- 1; sa_last <- 0
    }
    last_i <- ei
  }
  if (last_i < n) {                                          # tail: extend the last segment's transform
    seg <- (last_i + 1L):n; k <- nrow(anchors)
    rel_e <- disp_e[seg] - disp_e[last_i]; rel_n <- disp_n[seg] - disp_n[last_i]
    ce[seg] <- te[k] + f_last * (ca_last * rel_e - sa_last * rel_n)
    cn[seg] <- tn[k] + f_last * (sa_last * rel_e + ca_last * rel_n)
  }
  list(e = ce, n = cn)
}

#' Per-sample 1-sigma positional uncertainty (m) for the pseudo-track. Between two successive anchors the
#' reckoning error is combined forward-from-the-previous and backward-from-the-next fix by inverse-variance
#' (a Brownian-bridge / RTS-smoother envelope): each leg is sqrt(anchor_radius^2 + driftError(dt)^2), so the
#' result is ~ the fix radius at each anchor and bulges mid-segment; after the last anchor (or, with a lone
#' deploy anchor for vpc = "none") it grows monotonically forward. See Wensveen et al. (2015) for the
#' forward/backward reconstruction idea. `anchors` must carry integer `idx` and `quality`.
#' @keywords internal
#' @noRd
.reckonUncertainty <- function(dt, anchors, control, datetime.col) {
  n <- nrow(dt); radii <- control$anchor.error.radii
  ts <- as.numeric(dt[[datetime.col]])                         # epoch seconds
  err <- rep(NA_real_, n)
  if (is.null(anchors) || !nrow(anchors)) return(sqrt(50^2 + .driftError(ts - ts[1L], control)^2))
  idx <- anchors$idx
  if (nrow(anchors) >= 2L) for (a in seq_len(nrow(anchors) - 1L)) {
    si <- idx[a]; ei <- idx[a + 1L]; if (ei < si) next
    seg <- si:ei
    r_a <- .anchorRadius(anchors$quality[a], radii); r_b <- .anchorRadius(anchors$quality[a + 1L], radii)
    e_fwd <- sqrt(r_a^2 + .driftError(ts[seg] - ts[si], control)^2)   # reckon from the previous fix
    e_bwd <- sqrt(r_b^2 + .driftError(ts[ei] - ts[seg], control)^2)   # reckon back from the next fix
    sig <- sqrt(1 / (1 / e_fwd^2 + 1 / e_bwd^2))                      # inverse-variance combination
    err[seg] <- pmin(err[seg], sig, na.rm = TRUE)                     # shared anchor idx: keep the tighter
  }
  # head before the first anchor (defensive; current callers always anchor idx 1): grow backward from it
  if (idx[1L] > 1L) {
    seg <- 1L:idx[1L]
    r_first <- .anchorRadius(anchors$quality[1L], radii)
    err[seg] <- pmin(err[seg], sqrt(r_first^2 + .driftError(ts[idx[1L]] - ts[seg], control)^2), na.rm = TRUE)
  }
  # tail after the last anchor (and the whole vpc = "none" case): grow FORWARD from the smoothed uncertainty
  # already at that anchor - NOT from its raw radius, which would step-jump when good reckoning had tightened
  # it (e.g. a precise fix followed by a coarse Argos anchor)
  last <- idx[length(idx)]
  if (last <= n) {
    seg <- last:n
    seed <- if (is.finite(err[last])) err[last] else .anchorRadius(anchors$quality[length(idx)], radii)
    err[seg] <- pmin(err[seg], sqrt(seed^2 + .driftError(ts[seg] - ts[last], control)^2), na.rm = TRUE)
  }
  err
}


#######################################################################################################
# Diagnostic report ###################################################################################
#######################################################################################################

#' Assemble the small per-deployment plotting payload (thinned raw + corrected track, anchors with the DR
#' drift at each fix, speed source, depth profile, summary stats). Built while `disp_e/disp_n` still exist.
#' @keywords internal
#' @noRd
.reconstructPayload <- function(dt, id, deploy_lon, deploy_lat, R_earth, control, datetime.col, anchors,
                                max.pts = 3000L) {
  n <- nrow(dt)
  idx <- if (n > max.pts) round(seq(1L, n, length.out = max.pts)) else seq_len(n)
  bk <- .unprojLocal(dt$disp_e, dt$disp_n, deploy_lon, deploy_lat, R_earth)                # raw DR lon/lat
  cl <- bk$lon; ct <- bk$lat
  anc <- NULL
  if (!is.null(anchors) && nrow(anchors)) {
    drift <- sqrt((anchors$true_e - anchors$disp_e)^2 + (anchors$true_n - anchors$disp_n)^2)   # DR error at fix (m)
    anc <- data.frame(lon = anchors$lon, lat = anchors$lat, quality = as.character(anchors$quality),
                      drift_m = drift, stringsAsFactors = FALSE)
  }
  step_km  <- .trackDistances(dt$pseudo_lon, dt$pseudo_lat)   # reuse the trackMetrics haversine helper
  total_km <- sum(step_km, na.rm = TRUE)
  net_km   <- .trackDistance(dt$pseudo_lon[1], dt$pseudo_lat[1], dt$pseudo_lon[n], dt$pseudo_lat[n])
  mdrift   <- if (!is.null(anc) && nrow(anc) > 1L) mean(anc$drift_m[-1], na.rm = TRUE) else NA_real_
  list(id = id, speed.method = control$speed.method, vpc = control$vpc.method,
       raw = list(lon = cl[idx], lat = ct[idx]),
       cor = list(lon = dt$pseudo_lon[idx], lat = dt$pseudo_lat[idx]),
       anchors = anc, speed = dt$speed_dr[idx],
       depth = if ("depth" %in% names(dt))
                 list(h = as.numeric(dt[[datetime.col]][idx] - dt[[datetime.col]][1], units = "hours"), z = dt$depth[idx])
               else NULL,
       stats = list(total_km = total_km, net_km = net_km, n_anchors = if (is.null(anchors)) 0L else nrow(anchors),
                    mean_drift_m = mdrift, max_drift_m = if (!is.null(anc)) max(anc$drift_m, na.rm = TRUE) else NA_real_,
                    flag = isTRUE(is.finite(mdrift) && mdrift > 5000)))
}

#' One summary-table row per deployment (drawn on the report's first page).
#' @keywords internal
#' @noRd
.reconstructSummaryRecord <- function(pl) {
  if (is.null(pl)) return(NULL)
  list(id = pl$id, distance_km = pl$stats$total_km, net_km = pl$stats$net_km, n_anchors = pl$stats$n_anchors,
       mean_drift_m = pl$stats$mean_drift_m, speed.method = pl$speed.method, vpc = pl$vpc, flag = pl$stats$flag)
}

#' Report page 1: a table of every deployment's reconstruction (worst-first by residual drift).
#' @keywords internal
#' @noRd
.drawReconstructSummaryPage <- function(records, stats) {
  oldpar <- graphics::par(no.readonly = TRUE); on.exit(graphics::par(oldpar), add = TRUE)
  graphics::layout(1)
  if (length(records))
    records <- records[order(-vapply(records, function(r) if (is.finite(r$mean_drift_m)) r$mean_drift_m else -1, numeric(1)))]
  fnum <- function(v, fmt) if (is.null(v) || !is.finite(v)) "-" else sprintf(fmt, v)
  graphics::par(mar = c(1.5, 0.6, 1, 0.6)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
  graphics::text(0, 0.985, "reconstructTrack  -  run summary", adj = c(0, 1), font = 2, cex = 1.4)
  graphics::text(1, 0.985, format(Sys.time(), "%Y-%m-%d %H:%M"), adj = c(1, 1), cex = 0.75, col = "grey40")
  graphics::text(0, 0.935, sprintf("%d dataset%s  |  %d reconstructed", stats$n, if (stats$n != 1) "s" else "", stats$n_ok),
                 adj = c(0, 1), cex = 0.95)
  graphics::text(0, 0.905, "sorted by residual drift (worst first)", adj = c(0, 1), cex = 0.72, col = "grey45")
  cx <- c(id = 0.004, dist = 0.42, net = 0.55, anc = 0.68, drift = 0.82, meth = 0.90)
  hcy <- 0.855; graphics::rect(0, hcy - 0.014, 1, hcy + 0.014, col = "grey90", border = NA)
  hdr <- c(id = "Deployment", dist = "Dist (km)", net = "Net (km)", anc = "Fixes", drift = "Drift (m)", meth = "Method")
  adj <- c(id = 0, dist = 1, net = 1, anc = 1, drift = 1, meth = 0)
  for (k in names(cx)) graphics::text(cx[[k]], hcy, hdr[[k]], adj = c(adj[[k]], 0.5), font = 2, cex = 0.6, col = "grey15")
  graphics::segments(0, hcy - 0.016, 1, hcy - 0.016, col = "grey55")
  row_top <- hcy - 0.030; dy <- 0.0195; drawn <- 0L
  for (j in seq_along(records)) {
    r <- records[[j]]; y <- row_top - dy * (j - 1L); if (y < 0.035) break
    if (j %% 2L == 0L) graphics::rect(0, y - dy / 2, 1, y + dy / 2, col = "grey96", border = NA)
    cc <- if (isTRUE(r$flag)) "#c62828" else "grey20"
    graphics::text(cx[["id"]], y, r$id, adj = c(0, 0.5), cex = 0.6, font = 2, col = cc)
    graphics::text(cx[["dist"]], y, fnum(r$distance_km, "%.2f"), adj = c(1, 0.5), cex = 0.58, col = "grey25")
    graphics::text(cx[["net"]], y, fnum(r$net_km, "%.2f"), adj = c(1, 0.5), cex = 0.58, col = "grey25")
    graphics::text(cx[["anc"]], y, r$n_anchors, adj = c(1, 0.5), cex = 0.58, col = "grey25")
    graphics::text(cx[["drift"]], y, fnum(r$mean_drift_m, "%.0f"), adj = c(1, 0.5), cex = 0.58, col = cc)
    graphics::text(cx[["meth"]], y, sprintf("%s/%s", r$speed.method, r$vpc), adj = c(0, 0.5), cex = 0.55, col = "grey35")
    drawn <- j
  }
  if (!length(records)) graphics::text(0.5, 0.5, "no tracks to report", cex = 1, col = "grey50")
  else if (drawn < length(records))
    graphics::text(0.5, 0.014, sprintf("+ %d more not shown (sorted worst-first)", length(records) - drawn),
                   adj = c(0.5, 0.5), cex = 0.72, col = "grey45", font = 3)
  invisible(NULL)
}

#' Report detail page for one deployment: the reconstructed track vs the raw DR path and the position
#' fixes (the map), the per-fix drift the VPC corrected, the speed source, and the depth profile.
#' @keywords internal
#' @noRd
.plotReconstructIndividual <- function(pl) {
  oldpar <- graphics::par(no.readonly = TRUE)
  on.exit({ graphics::layout(1); graphics::par(oldpar) }, add = TRUE)
  RAWC <- "grey62"; CORC <- "#1565c0"; DEP <- "#2e7d32"; POP <- "#c62828"; FIX <- "#e08a00"
  graphics::layout(matrix(c(1, 1, 1, 1,  2, 2, 2, 3,  2, 2, 2, 4,  5, 5, 5, 5), nrow = 4L, byrow = TRUE),
                  heights = c(0.5, 1, 1, 0.72))

  # --- header ---
  graphics::par(mar = c(0.2, 1, 0.4, 1)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
  graphics::text(0, 0.72, sprintf("%s  -  reconstructed pseudo-track", pl$id), adj = c(0, 0.5), cex = 1.5, font = 2)
  graphics::text(0, 0.28, sprintf("%s DR + %s VPC   |   %.2f km path, %.2f km net   |   %d fixes%s",
                 pl$speed.method, pl$vpc, pl$stats$total_km, pl$stats$net_km, pl$stats$n_anchors,
                 if (is.finite(pl$stats$mean_drift_m)) sprintf(", mean drift %.0f m", pl$stats$mean_drift_m) else ""),
                 adj = c(0, 0.5), cex = 0.92, col = "grey30")

  # --- map: raw DR (grey) vs corrected (blue) + fixes ---
  graphics::par(mar = c(3.4, 3.6, 2.0, 1), mgp = c(2.2, 0.7, 0), cex.axis = 0.8)
  xr <- range(c(pl$raw$lon, pl$cor$lon, pl$anchors$lon), na.rm = TRUE)
  yr <- range(c(pl$raw$lat, pl$cor$lat, pl$anchors$lat), na.rm = TRUE)
  graphics::plot(NA, xlim = xr, ylim = yr, asp = 1 / cos(mean(yr) * pi / 180), main = "reconstructed track",
                 cex.main = 1.0, font.main = 1, xlab = "longitude", ylab = "latitude")
  if (pl$vpc != "none") graphics::lines(pl$raw$lon, pl$raw$lat, col = RAWC, lwd = 1.0)
  graphics::lines(pl$cor$lon, pl$cor$lat, col = CORC, lwd = 1.6)
  graphics::points(pl$cor$lon[1], pl$cor$lat[1], pch = 16, col = DEP, cex = 1.1)
  if (!is.null(pl$anchors)) {
    ac <- ifelse(pl$anchors$quality == "Deploy", DEP, ifelse(pl$anchors$quality == "Popup", POP, FIX))
    graphics::points(pl$anchors$lon, pl$anchors$lat, pch = 21, bg = ac, col = "white", cex = 1.2, lwd = 0.6)
  }
  graphics::legend("topleft", bty = "n", cex = 0.72,
                   legend = c(if (pl$vpc != "none") "raw DR", "corrected", "fix"),
                   col = c(if (pl$vpc != "none") RAWC, CORC, FIX), lwd = c(if (pl$vpc != "none") 1, 2, NA),
                   pch = c(if (pl$vpc != "none") NA, NA, 16))

  # --- drift at each fix (m) ---
  graphics::par(mar = c(3.4, 3.6, 2.2, 0.8))
  if (!is.null(pl$anchors) && nrow(pl$anchors) > 1L) {
    d <- pl$anchors$drift_m[-1]
    graphics::barplot(d, col = FIX, border = NA, main = "DR drift at fix (m)", cex.main = 0.92, ylab = "m", las = 1)
  } else { graphics::plot.new(); graphics::text(0.5, 0.5, "no fixes\nto correct to", cex = 0.85, col = "grey50") }

  # --- speed source ---
  sp <- pl$speed[is.finite(pl$speed)]
  if (length(sp) > 2L) graphics::hist(sp, breaks = 30, col = grDevices::adjustcolor(CORC, 0.5), border = NA,
                                      main = "speed (m/s)", cex.main = 0.92, xlab = "", ylab = "")
  else { graphics::plot.new(); graphics::text(0.5, 0.5, "speed n/a", cex = 0.85, col = "grey50") }

  # --- depth profile (the vertical axis of the 3-D track) ---
  graphics::par(mar = c(3.6, 3.6, 2.0, 1))
  if (!is.null(pl$depth)) {
    graphics::plot(pl$depth$h, pl$depth$z, type = "l", col = "#00838f", lwd = 1, ylim = rev(range(pl$depth$z, na.rm = TRUE)),
                   main = "depth profile (3-D vertical axis)", cex.main = 0.95, font.main = 1,
                   xlab = "time (h)", ylab = "depth (m)")
  } else { graphics::plot.new(); graphics::text(0.5, 0.5, "no depth channel (2-D track)", cex = 0.95, col = "grey50") }
  invisible(NULL)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
