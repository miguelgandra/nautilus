#######################################################################################################
# Reconstruct a fine-scale movement path by dead reckoning + correction onto known fixes ###############
#######################################################################################################

#' Reconstruct movement paths from orientation and swimming speed
#'
#' @description
#' Integrates body heading, pitch and swimming speed to reconstruct a fine-scale movement path by
#' dead reckoning. The horizontal path can be corrected against deployment, pop-up and recorded
#' position fixes, and measured depth can be retained as its vertical component.
#'
#' The function is applied after [processTagData()], using orientation expressed in the animal's
#' body frame. Speed estimation and position correction are configured through
#' [reconstructTrackControl()]. The result is a pseudo-track: an estimated path conditional on
#' the sensor data, speed assumptions and available fixes, not a series of observed locations.
#'
#' @param data A tag dataset, a list of tag datasets, a data frame containing deployments identified
#'   by \code{id.col}, or a character vector of \code{.rds} file paths. The output of [processTagData()]
#'   is recommended. Each deployment requires timestamps, \code{heading} and \code{pitch}, the
#'   channels needed by the selected speed method, and deployment coordinates in its metadata.
#'   File inputs are read sequentially, one deployment at a time.
#' @param control A control object created by [reconstructTrackControl()], specifying the speed
#'   method, position-correction method, drift model and diagnostic thresholds. By default, speed
#'   is constant at 0.5 m/s and position correction is \code{"error_weighted"}.
#' @param id.col Character. Name of the column identifying deployments, not animals. Default
#'   \code{"ID"}.
#' @param datetime.col Character. Name of the \code{POSIXct} timestamp column. Default
#'   \code{"datetime"}. Observations must be in chronological order.
#' @param return.data Logical. Return reconstructed datasets in memory (default \code{TRUE}).
#'   When \code{FALSE}, return the paths of the saved \code{.rds} files invisibly; requires
#'   \code{output.dir}.
#' @param output.dir Character. Existing directory in which to save one \code{<id>.rds} file per
#'   successfully reconstructed deployment. Providing a directory triggers saving;
#'   \code{NULL} (default) writes no datasets.
#' @param output.suffix Character. Optional suffix appended to each saved filename before
#'   \code{.rds}. Used only when \code{output.dir} is supplied. Default \code{NULL}.
#' @param compress Compression passed to [base::saveRDS()]: \code{TRUE} (default, gzip),
#'   \code{FALSE}, \code{"gzip"}, \code{"bzip2"} or \code{"xz"}.
#' @param plot Logical. Draw the diagnostic report on the active graphics device. Default
#'   \code{FALSE}.
#' @param plot.file Character. Path to a diagnostic PDF, or \code{NULL} (default). Providing a
#'   path writes the report independently of \code{plot}; the parent directory must exist.
#' @param force.plots Logical. Include a detail page for every successfully reconstructed
#'   deployment rather than only flagged deployments. Default \code{FALSE}. A single-input
#'   run always includes its detail page when reconstruction succeeds and plotting is requested.
#' @param verbose Verbosity: \code{FALSE}/\code{0}/\code{"quiet"},
#'   \code{TRUE}/\code{1}/\code{"normal"}, or \code{2}/\code{"detailed"} (default).
#'   Normal output uses a deployment-level progress bar; detailed output reports each deployment.
#'
#' @details
#' ## Input preparation and reference frames
#'
#' Trim recordings to the deployment period with [filterDeploymentData()] and establish a valid,
#' chronologically ordered time series before reconstruction. The deployment coordinates are
#' assigned to the first input observation; the function does not trim the recording or estimate
#' an alternative starting position.
#'
#' \code{heading} and \code{pitch} are in degrees, with heading clockwise from north and pitch
#' relative to the horizontal. Absolute reconstruction requires a geographic heading referenced
#' to true north. A magnetic heading is not converted here: [processTagData()] applies magnetic
#' declination when deployment coordinates are available.
#'
#' The function warns when recorded metadata identifies a magnetic heading or an uncalibrated
#' magnetometer, but does not automatically reject those tracks. A partial calibration is reported
#' when verbosity permits. An unknown heading reference is not inferred. Inspect
#' \code{getTagMetadata(x)$deployment$heading_reference} and
#' \code{getTagMetadata(x)$mag_calibration} before interpreting absolute positions.
#'
#' ## Swimming speed
#'
#' Speed is resolved in m/s along the animal's longitudinal axis. The methods are:
#'
#' \describe{
#'   \item{\code{"constant"}}{Uses \code{control$constant.speed} throughout the deployment. This is
#'     the default and represents an assumed speed, not a sensor-derived estimate.}
#'   \item{\code{"paddle"}}{Uses \code{paddle_speed}, normally produced by [calculatePaddleSpeed()].
#'     A \code{paddle_freq} column alone is insufficient. Interior missing values are linearly
#'     interpolated by sample index when at least two finite speeds are available; this interpolation
#'     has no maximum-gap limit.}
#'   \item{\code{"vedba"}}{Uses a linear VeDBA-to-speed model, requiring \code{vedba} in g.
#'     Coefficients may be supplied in \code{control$vedba.model}; otherwise they are fitted from
#'     sufficiently straight intervals between the deployment's position anchors. An unavailable or
#'     unsuitable automatic fit falls back to \code{control$constant.speed}.}
#'   \item{\code{"depth_rate"}}{Uses the absolute vertical velocity divided by the sine of absolute
#'     pitch, requiring \code{vertical_velocity} in m/s. Only samples meeting
#'     \code{control$depth.rate.min.pitch} are used; shallower samples fall back to the constant speed.}
#' }
#'
#' Any remaining non-finite speed is replaced by \code{control$constant.speed}. Negative speeds are
#' set to zero and speeds above \code{control$max.speed} are clipped. The default reconstruction cap
#' is 2.5 m/s; it is independent of the optional threshold in [calculatePaddleSpeed()]. Optional
#' \code{control$rest.quantile} gating sets speed to zero below the specified VeDBA quantile when
#' that channel is available. These assumptions affect path length and must be considered alongside
#' the speed method.
#'
#' ## Dead-reckoning integration
#'
#' At each observation the horizontal speed is \eqn{v_h = v \cos(\theta)}, where \eqn{v} is the
#' along-body speed and \eqn{\theta} is pitch. Heading resolves this speed into eastward and
#' northward components, which are integrated over elapsed time on a spherical Earth. Position
#' correction is subsequently performed in a local east-north displacement representation.
#'
#' Non-finite displacement increments are set to zero. Thus, isolated missing orientation values do
#' not contribute movement to the uncorrected path; they are not evidence that the animal was
#' stationary. Reconstruction does not recover unobserved motion across sensor gaps or explicitly
#' model ocean currents.
#'
#' ## Position anchors and correction
#'
#' Anchors comprise the deployment origin, metadata pop-up coordinates where available, and recorded
#' position fixes. Sparse position columns are used when already present; otherwise they are resolved
#' from \code{meta$ancillary$positions}. Ancillary fixes within the sensor period are aligned to
#' the nearest observation, and the metadata pop-up is aligned to its nearest timestamp. Only one
#' anchor per sensor row is retained, with deployment and pop-up anchors taking precedence over
#' recorded fixes. Screen implausible recorded locations with [filterLocations()] beforehand.
#' Ancillary fix alignment uses the canonical \code{datetime} column; retain that column when
#' fixes are supplied only through metadata, even if \code{datetime.col} names another time column.
#'
#' Verified Position Correction (VPC) is controlled by \code{control$vpc.method}:
#'
#' \describe{
#'   \item{\code{"error_weighted"}}{The default. Distributes the displacement residual between
#'     anchors using a gain based on the configured dead-reckoning error and the next fix's error
#'     radius. The corrected path need not pass exactly through each fix.}
#'   \item{\code{"linear"}}{Applies the full additive correction between anchors, forcing the path
#'     through their coordinates.}
#'   \item{\code{"scale_rotate"}}{Rescales and rotates each segment to match its bracketing anchors.
#'     Poorly conditioned segments, including near-closed paths and coincident anchors, use an
#'     additive fallback. Fix-quality weighting does not affect the corrected coordinates.}
#'   \item{\code{"none"}}{Returns the uncorrected dead-reckoned path. Available position anchors may
#'     still be used to fit an automatic VeDBA speed model.}
#' }
#'
#' For additive correction, \code{control$vpc.weighting} distributes the residual by cumulative
#' reckoned distance (default) or elapsed time. Distance weighting falls back to time weighting
#' when a segment has no positive reckoned length. After the last anchor, the corrected path
#' continues without another positional constraint.
#'
#' ## Positional uncertainty and interpretation
#'
#' \code{pseudo_error} is a model-based horizontal error scale in metres, expressed as a nominal
#' one-standard-deviation uncertainty. Its magnitude depends on \code{control$anchor.error.radii},
#' \code{control$drift.rate} and \code{control$drift.diffusion}, not on independently measured
#' reconstruction error. Between correction anchors, forward and backward error estimates are combined
#' by inverse variance. Beyond the final anchor the error grows forwards; with no position correction,
#' it grows from the deployment origin.
#'
#' This uncertainty is an approximate diagnostic envelope, not a validated confidence region or a
#' state-space posterior. Anchor radii remain assumptions about fix quality. Use
#' [crossValidateTrack()] to assess held-out horizontal errors where suitable fixes are available.
#' Neither correction nor agreement at retained anchors demonstrates accuracy between them.
#'
#' With \code{control$include.depth = TRUE} (default), an available \code{depth} channel is copied
#' to \code{pseudo_depth}, in metres below the surface. Depth is measured independently of the
#' horizontal reconstruction and is not adjusted by VPC.
#'
#' ## Quality control and skipped deployments
#'
#' A deployment needs at least two observations with finite, paired heading and pitch values.
#' When both columns are present but this requirement fails, the deployment is omitted from the
#' outputs and named in an aggregated warning. Other per-deployment reconstruction failures,
#' including missing required columns or deployment coordinates, are skipped and reported when
#' verbosity permits. If a paddle-speed call produces no tracks and at least one deployment was not
#' skipped for unavailable orientation, the function stops with a \code{paddle_speed} diagnostic.
#' No shared exclusions file is written.
#'
#' For tracks without recorded interior fixes, \code{control$reconstructability.min} enables a
#' heuristic assessment of endpoint constraints. Directedness is the deployment-to-pop-up distance
#' divided by the uncorrected horizontal path length, capped at one. A low value raises a warning
#' without rejecting the track; an origin-only track is flagged as unbounded. The result is stored in
#' \code{meta$sensors$reconstructability}. This heuristic depends on the assumed speed and does not
#' validate the reconstructed interior. Setting the threshold to zero disables it.
#'
#' ## Metadata and diagnostics
#'
#' Returned datasets retain their metadata and gain a \code{reconstructTrack} processing record
#' describing the speed and correction methods, weighting, anchor count, drift parameters and output
#' dimension. VeDBA model details are recorded when that speed method is used. Inspect these records
#' with [getTagMetadata()] and [processingHistory()].
#'
#' The diagnostic report contains a deployment summary followed by detail pages for flagged tracks.
#' Flagging uses mean uncorrected displacement error at correction anchors above 5 km; this is not
#' held-out validation error. Detail pages compare raw and corrected horizontal paths, position
#' anchors, displacement residuals, swimming speed and depth where available. \code{force.plots}
#' requests all detail pages.
#'
#' @return When \code{return.data = TRUE}, a named list of successfully reconstructed
#'   \code{nautilus_tag} datasets, including for a single input. Sensor data and metadata are retained,
#'   with the following columns added or updated:
#'   \describe{
#'     \item{\code{pseudo_lon}, \code{pseudo_lat}}{Estimated longitude and latitude, in decimal degrees.}
#'     \item{\code{speed_dr}}{Along-body speed used for integration, in m/s, after fallback, gating and
#'       clipping. It is not re-estimated from the corrected path.}
#'     \item{\code{pseudo_error}}{Model-based horizontal positional uncertainty scale, in metres.}
#'     \item{\code{pseudo_depth}}{Copy of measured depth, in metres, when depth inclusion is enabled
#'       and the channel is present.}
#'   }
#'   When \code{return.data = FALSE}, a character vector of the \code{.rds} paths written to
#'   \code{output.dir}, returned invisibly. Skipped deployments are absent from both output forms.
#'
#' @references
#' Wilson RP, Liebsch N, Davies IM, et al. (2007) All at sea with animal tracks; methodological and
#' analytical solutions for the resolution of movement. \emph{Deep Sea Research Part II} 54:193-210.
#' \doi{10.1016/j.dsr2.2006.11.017}
#'
#' Bidder OR, Walker JS, Jones MW, et al. (2015) Step by step: reconstruction of terrestrial animal
#' movement paths by dead-reckoning. \emph{Movement Ecology} 3:23. \doi{10.1186/s40462-015-0055-4}
#'
#' Gunner RM, Holton MD, Scantlebury MD, et al. (2021) Dead-reckoning animal movements in R: a
#' reappraisal using Gundog.Tracks. \emph{Animal Biotelemetry} 9:23. \doi{10.1186/s40317-021-00245-z}
#'
#' @seealso [reconstructTrackControl()] for reconstruction settings; [processTagData()] for orientation
#'   and movement metrics; [calculatePaddleSpeed()] for paddle calibration; [filterLocations()] for
#'   location screening; [crossValidateTrack()] for held-out validation; [trackMetrics()] for path
#'   summaries; [exportForSSM()] for state-space model input.
#'
#' @examples
#' \dontrun{
#' # Reconstruct processed deployments using an explicitly selected constant speed.
#' control <- reconstructTrackControl(speed.method = "constant", constant.speed = 0.6)
#' tracks <- reconstructTrack(processed, control = control)
#' processingHistory(tracks[[1]])
#'
#' # Calibrate paddle rotation before using it to scale a track.
#' paddle_tags <- calculatePaddleSpeed(processed, calibration = paddle_calibration)
#' tracks <- reconstructTrack(paddle_tags,
#'                            control = reconstructTrackControl(speed.method = "paddle"))
#'
#' # Write a disk-backed result and diagnostic report; the output directory must exist.
#' track_files <- reconstructTrack(processed_files, control = control,
#'                                 output.dir = "./tracks", return.data = FALSE,
#'                                 plot.file = "./track_diagnostics.pdf")
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
  magnetic_ids  <- character(0)   # heading referenced to MAGNETIC north (an axis independent of calibration)
  no_orientation_ids <- character(0)
  pb <- .log_progress_start(lvl, r$n, "Reconstructing", min.level = 1L, max.level = 1L)   # NORMAL only (detailed streams)
  for (i in seq_len(r$n)) {
    .log_progress_step(pb)
    x <- r$get(i)
    if (!data.table::is.data.table(x)) x <- data.table::as.data.table(x)
    who <- tryCatch(as.character(unique(x[[id.col]])[1]), error = function(e) NA_character_)
    if (length(who) != 1L || is.na(who) || !nzchar(who)) who <- r$ids[i]
    .log_h2(lvl, sprintf("%s (%d/%d)", who, i, r$n))

    # A column of NA is a schema placeholder, not a heading estimate. Without this guard the
    # integrator turns every non-finite displacement into zero and writes a plausible-looking,
    # perfectly stationary track for a deployment whose accelerometer was excluded by QC.
    if (all(c("heading", "pitch") %in% names(x)) &&
        sum(is.finite(x$heading) & is.finite(x$pitch)) < 2L) {
      no_orientation_ids <- c(no_orientation_ids, who)
      .log_skip(lvl, "no usable heading and pitch; track skipped")
      .log_gap(lvl)
      rm(x)
      next
    }

    # heading-calibration trust: the track is only as good as the heading it integrates. A raw (uncalibrated)
    # field carries a hard-iron offset that biases every heading and compounds into drift, so surface it.
    trust <- .headingTrust(.getMeta(x))
    if (identical(trust, "untrusted"))    untrusted_ids <- c(untrusted_ids, who)
    else if (identical(trust, "partial")) partial_ids   <- c(partial_ids, who)

    # Reference frame is a SEPARATE axis from calibration quality: a perfectly calibrated magnetometer
    # still reads magnetic north. Dead reckoning projects the heading onto true north/east
    # (d_e = v*sin(heading), d_n = v*cos(heading)), so a magnetic heading rotates the entire track
    # rigidly about the deployment point - about 132 m of displacement per km travelled at the -7.6 deg
    # declination of the Azores. The path SHAPE is right; its bearing is not.
    #
    # Reachability: a heading is only left magnetic when NO position was available, and this function
    # already refuses a deployment without coordinates. The case this catches is the sequel - process
    # without a position, notice, add it to the metadata, then reconstruct. The track now builds and the
    # heading is still magnetic, which is exactly when the error would pass unnoticed.
    # "unknown" (a tag processed before the frame was recorded) is left alone rather than guessed at.
    if (identical(.headingReference(.getMeta(x)), "magnetic")) magnetic_ids <- c(magnetic_ids, who)

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

  .warn_grouped("{length(no_orientation_ids)} deployment{?s} {?was/were} skipped: no usable orientation for track reconstruction.",
                items = no_orientation_ids, style = "inline")

  # Asking for paddle speed and getting none from any deployment is a missing step, not a run with
  # nothing to show: a track scaled by an absent speed would be built entirely from the fallback and
  # would look like a result.
  if (identical(control$speed.method, "paddle") && n_ok == 0L && r$n > length(no_orientation_ids))
    .abort(c("No deployment carries {.field paddle_speed}, which {.code speed.method = \"paddle\"} needs.",
             "i" = "{.fn processTagData} records the paddle rotation as {.field paddle_freq};
                    {.fn calculatePaddleSpeed} turns it into a speed."))

  # heading-trust warnings (aggregated, emitted once). Untrusted = a raw uncalibrated field -> loud; partial
  # = a diagonal/2D/low-confidence fit -> a lighter note. Both compound into dead-reckoning drift.
  if (length(untrusted_ids))
    cli::cli_warn(c(
      "{length(untrusted_ids)} pseudo-track{?s} dead-reckoned from an UNCALIBRATED magnetometer: {.val {utils::head(untrusted_ids, 8)}}.",
      "!" = "The heading carries the tag's hard-iron offset; absolute bearings and the reckoned path will drift badly.",
      "i" = "Calibrate first ({.fn calibrateMagnetometer}, ideally with {.arg calibration.data}); see {.code meta$mag_calibration$status}."))
  if (length(magnetic_ids))
    cli::cli_warn(c(
      "{length(magnetic_ids)} pseudo-track{?s} dead-reckoned from a MAGNETIC heading: {.val {utils::head(magnetic_ids, 8)}}.",
      "!" = "No declination was applied, so the whole track is rotated relative to true north - the shape is right, the bearing is not.",
      "i" = "Set the deployment position and re-run {.fn processTagData}; the frame is recorded in {.code meta$deployment$heading_reference}."))
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
