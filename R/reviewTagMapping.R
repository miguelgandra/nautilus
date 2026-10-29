#######################################################################################################
# Confirm an inferred axis mapping against on-animal video ############################################
#######################################################################################################

#' Review sensor-axis mappings against synchronised deployment video
#'
#' @description
#' Selects deployments requiring axis-mapping review and renders short video clips with sensor-derived
#' attitude indicators. Documented and inferred frames, or representative ambiguous frames, can be
#' compared with visible animal manoeuvres. The function returns an editable deployment-level
#' decision sheet; it does not select a mapping or change the stored sensor data.
#'
#' Use after [checkTagMapping()] and optional [consensusAxisMapping()], before [applyAxisMapping()].
#' Supply the original diagnostic results as \code{mapping}; reconciled mappings can be supplied
#' separately as \code{base}. The completed review object is then passed to [applyAxisMapping()],
#' which applies decisions and records their provenance.
#'
#' @param data Raw-axis deployment data corresponding to \code{mapping}: a \code{nautilus_tag}
#'   object, a named list of deployment datasets, a data frame containing deployments identified
#'   by \code{id.col}, or a character vector of \code{.rds} file paths. Use the trimmed,
#'   sensor-checked records supplied to [checkTagMapping()]. Rendering requires usable canonical
#'   accelerometer channels; depth and gyroscope channels support additional diagnostics.
#' @param mapping Non-empty named list returned by [checkTagMapping()], containing deployment-level
#'   \code{frame_state} and \code{families} evidence for triage. A [consensusAxisMapping()] result
#'   is not a substitute for this evidence; pass it through \code{base} instead.
#' @param video.metadata Data frame returned by [getVideoMetadata()], with \code{ID}, \code{file},
#'   \code{start} and \code{end} columns. IDs must match the deployment identifiers, file paths must
#'   identify accessible videos, and \code{start}/\code{end} must be \code{POSIXct} times aligned
#'   with the sensor clock. Apply any video-clock corrections before review.
#' @param output.dir Character. Existing directory in which to write review clips named
#'   \code{<id>_<type>_<rank>.mp4}. Repeated runs can overwrite clips with the same names.
#' @param configs Optional named configuration dictionary used by [checkTagMapping()] and
#'   [applyAxisMapping()]. Required to offer the \code{"Documented"} alternative for a conflicting
#'   deployment carrying \code{tag$axis_config} metadata. Without it, that case uses a single
#'   proposed-frame indicator. Default \code{NULL}.
#' @param base Per-deployment mapping object used for deployments not overridden by a review
#'   decision: a [checkTagMapping()] or [consensusAxisMapping()] result, or a named list of
#'   \code{from}/\code{to} tables keyed by deployment ID. \code{NULL} (default) uses \code{mapping}.
#' @param ids Optional character vector of deployment IDs to review explicitly. \code{NULL}
#'   (default) selects deployments by the automatic triage described below. An explicit vector
#'   reviews only those IDs, including otherwise unflagged deployments.
#' @param include Character vector of automatic triage reasons: \code{"conflict"},
#'   \code{"coreg_fail"}, \code{"ambiguous"} and \code{"gyro_inconsistent"} by default.
#'   Add \code{"low_confidence"} to include a resolved vertical axis with unresolved surge.
#'   Unanalysed deployments with both sensor and video records are included automatically.
#' @param types Manoeuvre types passed to [findValidationSegments()]: \code{"roll"} and
#'   \code{"dive"} by default. \code{"turn"} is also accepted, but relies on an uncalibrated
#'   heading estimate and is not recommended as the primary frame-validation cue.
#' @param n Number of candidate segments requested per manoeuvre type and deployment
#'   (default \code{3}). The search requests at least \code{clips.per.deployment} candidates per
#'   type, then selects clips round-robin by type and rank.
#' @param window Segment duration and manoeuvre-smoothing window, in seconds, passed to
#'   [findValidationSegments()]. Default \code{20}.
#' @param clips.per.deployment Maximum number of clips rendered per deployment. Default \code{3}.
#' @param max.candidates Maximum number of representative vertical-axis hypotheses displayed for
#'   an ambiguous frame. Default \code{3}. Does not limit a documented-versus-proposed comparison;
#'   see Details for the scope of the candidate display.
#' @param side Dashboard position beside the source video: \code{"right"} (default) or
#'   \code{"left"}. Passed to [renderOverlayVideo()].
#' @param overlay.fps Positive dashboard update rate, in frames per second, passed to
#'   [renderOverlayVideo()]. Default \code{5}; independent of the source-video frame rate.
#' @param codec Output codec family: \code{"hevc"} (default) or \code{"h264"}, passed to
#'   [renderOverlayVideo()]. HEVC generally produces smaller files; H.264 has broader playback
#'   support. HEVC output is tagged for QuickTime compatibility.
#' @param id.col Character. Name of the column identifying deployments, not animals. Default
#'   \code{"ID"}. Video metadata always use the \code{ID} column.
#' @param datetime.col Character. Name of the \code{POSIXct} timestamp column. Default
#'   \code{"datetime"}. Retain the canonical column for the rendered sensor series; this function
#'   does not provide a general sensor-column renaming interface.
#' @param verbose How much detail to print: \code{0}/\code{"quiet"},
#'   \code{1}/\code{"normal"}, or \code{2}/\code{"detailed"} (default). Normal output includes
#'   deployment-level progress; detailed output also reports rendered clips.
#'
#' @details
#' ## Scientific role and prerequisites
#'
#' Video provides independent behavioural evidence for the sign and assignment of orientation axes,
#' particularly during visible banks and sustained dives. It is most useful where posture or diving
#' evidence alone cannot distinguish candidate frames. Interpret visible motion with respect to
#' the camera's mounting geometry: camera motion does not necessarily represent animal-body motion.
#'
#' Footage and sensors must share a correctly aligned time base. [getVideoClockCorrections()] and
#' [getVideoMetadata()] support explicit video-clock correction; review does not estimate clock
#' offsets. Deployment list names or file basenames must also match the diagnostic and video IDs.
#' Use canonical sensor columns \code{ax/ay/az}, \code{gx/gy/gz} and \code{depth}.
#'
#' Clip rendering requires the suggested \pkg{av} package and an accessible FFmpeg executable.
#' These dependencies are checked before triage, including calls that ultimately need no clips.
#' The validation dashboards are rendered beside the video through [renderOverlayVideo()].
#'
#' ## Deployment triage
#'
#' Each deployment receives its highest-priority reason selected by \code{include}:
#'
#' \describe{
#'   \item{\code{"conflict"}}{A conflict message is present in the diagnostic frame state.
#'     This can reflect a documented-configuration conflict or disagreement between estimators.}
#'   \item{\code{"coreg_fail"}}{The gyroscope's shared-frame relationship with the accelerometer
#'     was decisively rejected by the co-registration diagnostic.}
#'   \item{\code{"ambiguous"}}{Several accelerometer frames survive and the documented prior is
#'     absent, unverifiable or consistent with multiple candidates.}
#'   \item{\code{"gyro_inconsistent"}}{Independent gyroscope estimators disagree. Under the
#'     defaults this is normally already selected as a higher-priority conflict or co-registration
#'     failure; it can be selected when those reasons are omitted from \code{include}.}
#'   \item{\code{"low_confidence"}}{The vertical axis is resolved but surge remains ambiguous.
#'     This reason is opt-in and does not represent a numerical confidence threshold.}
#' }
#'
#' Deployments present in both \code{data} and \code{video.metadata} but absent from the diagnostic
#' list are additionally flagged as \code{"unanalysed"}, at the same priority as conflicts.
#' Explicitly requested IDs with no selected diagnostic reason are \code{"user_requested"}.
#' The queue is ordered by priority, then deployment ID; a returned row is a review task, not an
#' automatic rejection.
#'
#' ## Candidate frames and clip selection
#'
#' A conflict with available \code{configs} is displayed as \code{"Documented"} versus
#' \code{"Proposed"}. Ambiguous frames with more than one plausible vertical axis are represented
#' by one surviving frame per distinct vertical source/sign, up to \code{max.candidates}, labelled
#' \code{Z=...}. The current display does not enumerate every surge/sway permutation sharing a
#' vertical axis. An ambiguity confined to horizontal axes therefore uses a single indicator
#' and is not resolved automatically by the review sheet.
#'
#' Other cases show a single proposed-frame indicator; unanalysed records use their raw frame.
#' Candidate mappings are applied to temporary data copies. A lightweight orientation estimate uses
#' accelerometer tilt and, when available, complementary gyroscope fusion for responsive pitch/roll.
#' Heading is omitted from the validation display because the provisional magnetic estimate is not
#' a calibrated heading. These display estimates are not substitutes for [processTagData()].
#'
#' The manoeuvre search is restricted to video-covered intervals. Clips are chosen across requested
#' types by rank; missing channels, lack of coverage or lack of a suitable manoeuvre can produce
#' fewer clips than requested. Per-deployment and rendering errors are recorded in the returned
#' status rather than treated as an approved mapping.
#'
#' ## Decisions and application
#'
#' Inspect the rendered clips, then set \code{decision} to the appropriate label listed in
#' \code{options}. [applyAxisMapping()] matches labels without regard to case or surrounding
#' whitespace and uses the candidate mapping embedded in the review object. Keep the complete
#' object, including its attributes; [base::saveRDS()] preserves it for a later session.
#'
#' A deployment with candidate options and at least one rendered clip cannot be applied while
#' undecided: [applyAxisMapping()] stops with an error. Single-indicator cases and rows without
#' rendered footage do not require a decision and retain the base mapping. This fallback is not
#' evidence that the frame was validated; inspect unresolved cases before downstream analysis.
#' Decisions replace a deployment's base mapping rather than merging every base sensor-family row.
#' A vertical-axis candidate supplies an accelerometer mapping, with the gyroscope completion
#' rules of [applyAxisMapping()]; it does not independently establish magnetometer axes.
#'
#' Set \code{decision = "Exclude"} on any row when the deployment must not proceed to orientation
#' analysis. Application then omits the entire deployment, including otherwise usable depth or
#' temperature data; no output file is written for it. This is an explicit deployment-level decision,
#' not a stream-specific sensor exclusion. Unreviewed deployments retain their base mapping.
#'
#' @return A \code{nautilus_review} data frame, returned invisibly, with one row per selected
#'   deployment and columns:
#'   \describe{
#'     \item{\code{id}, \code{review_reason}, \code{priority}}{Deployment identifier, selected
#'       triage reason and integer priority (smaller values are reviewed first).}
#'     \item{\code{n_clips}}{Number of successfully rendered clips.}
#'     \item{\code{status}}{\code{"rendered"}, \code{"no_video"}, \code{"no_coverage"},
#'       \code{"no_segment"} or \code{"render_failed"}. A deployment is \code{"rendered"}
#'       when at least one clip succeeded; individual failures remain in the manifest.}
#'     \item{\code{options}}{Candidate labels separated by \code{" | "}, or an empty string
#'       where no comparison is offered. \code{"Exclude"} is valid independently of this field.}
#'     \item{\code{decision}}{Editable character column, initially \code{NA}, for a candidate
#'       label or \code{"Exclude"}.}
#'   }
#'   Attributes retain the candidate mappings (\code{review_candidates}), base mapping
#'   (\code{review_base}), output directory (\code{review_output}) and per-clip manifest
#'   (\code{review_manifest}). The manifest records deployment, reason, priority, mapping source,
#'   manoeuvre type/rank, \code{POSIXct} peak/start/end times, source-video basename, output path
#'   and status. When nothing requires review, an empty decision sheet still carries its base
#'   mapping. Pass the complete object as \code{mapping} to [applyAxisMapping()];
#'   [print.nautilus_review()] summarises the decisions.
#'
#' @seealso [checkTagMapping()] for diagnostic evidence; [consensusAxisMapping()] for an optional
#'   reconciled base; [applyAxisMapping()] for applying decisions and logging explicit exclusions;
#'   [getVideoClockCorrections()] and [getVideoMetadata()] for synchronised footage;
#'   [findValidationSegments()] and [renderOverlayVideo()] for segment selection and rendering;
#'   [processTagData()] for subsequent metric derivation.
#'
#' @examples
#' \dontrun{
#' # Raw-axis records already trimmed and checked for sensor integrity.
#' files <- list.files("./checked", pattern = "\\.rds$", full.names = TRUE)
#' evidence <- checkTagMapping(files, configs = configs)
#' reconciled <- consensusAxisMapping(evidence)
#'
#' # video_metadata has deployment IDs and clock-corrected POSIXct start/end times.
#' review <- reviewTagMapping(files, mapping = evidence,
#'                            video.metadata = video_metadata,
#'                            output.dir = "./review_clips",
#'                            configs = configs, base = reconciled)
#' print(review)
#' attr(review, "review_manifest")
#'
#' # After watching a Documented/Proposed comparison, record the supported choice.
#' # Use an ID and label present in this review's decision sheet.
#' review$decision[review$id == "DEPLOYMENT_01"] <- "Documented"
#' saveRDS(review, "./axis_mapping_review.rds")
#'
#' # Complete every rendered comparison before applying the full review object.
#' oriented <- applyAxisMapping(files, mapping = review,
#'                              exclusions.file = "./exclusions.csv")
#' }
#' @export

reviewTagMapping <- function(data,
                             mapping,
                             video.metadata,
                             output.dir,
                             configs = NULL,
                             base = NULL,
                             ids = NULL,
                             include = c("conflict", "coreg_fail", "ambiguous", "gyro_inconsistent"),
                             types = c("roll", "dive"),
                             n = 3,
                             window = 20,
                             clips.per.deployment = 3,
                             max.candidates = 3,
                             side = "right",
                             overlay.fps = 5,
                             codec = c("hevc", "h264"),
                             id.col = "ID",
                             datetime.col = "datetime",
                             verbose = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  .assert_number(n, "n", min = 1); .assert_number(window, "window", min = 0)
  .assert_number(clips.per.deployment, "clips.per.deployment", min = 1)
  .assert_number(max.candidates, "max.candidates", min = 1)
  .assert_dir(output.dir, "output.dir")
  if (!is.list(mapping) || !length(mapping)) .abort("{.arg mapping} must be the (non-empty) list returned by {.fn checkTagMapping}.")
  include <- match.arg(include, c("conflict", "coreg_fail", "ambiguous", "gyro_inconsistent", "low_confidence"), several.ok = TRUE)
  codec <- match.arg(codec)
  .assert_columns(video.metadata, c("ID", "file", "start", "end"), "video.metadata")

  # heavy dependencies, checked once up front (before any transform / render work)
  if (!requireNamespace("av", quietly = TRUE)) .abort("The {.pkg av} package is required: {.code install.packages('av')}.")
  invisible(.ffmpegBin())

  r <- .resolveInput(data, id.col = id.col)
  data_ids <- as.character(r$ids)
  video_ids <- unique(as.character(video.metadata$ID))

  # the mapping applied to un-reviewed deployments and overridden by the review decisions for flagged
  # ones; resolved to a compact routing set now (frame_state stays in `mapping`, used only for triage).
  base_set <- .asAxisMappingSet(base %||% mapping)

  # ---- triage: which deployments to review, why, and in what order ----
  queue <- .triageMappings(mapping, data_ids, video_ids, include, ids)
  if (!nrow(queue)) {
    if (lvl >= 1L) cli::cli_alert_info("No deployments require review (nothing matched the triage).")
    return(invisible(.emptyReview(base_set, output.dir)))
  }

  .log_header(lvl, "reviewTagMapping", "Rendering clips for manual orientation review",
              bullets = sprintf("Flagged: %d deployment%s \u00b7 %s", nrow(queue),
                                if (nrow(queue) != 1) "s" else "",
                                paste(sprintf("%d %s", table(queue$review_reason), names(table(queue$review_reason))), collapse = ", ")),
              arrow = sprintf("Output: %s", output.dir))

  # one progress bar over the queue (transient; auto-suppressed non-interactively); at the detailed level a
  # per-deployment result line prints above it ONLY for deployments that actually produced clips
  pb_id <- if (lvl >= 1L) cli::cli_progress_bar(
    format = "{cli::pb_spin} Rendering review clips {cli::pb_bar} {cli::pb_percent} \u00b7 {cli::pb_current}/{cli::pb_total} deployments",
    total = nrow(queue), clear = TRUE, .envir = environment()) else NULL

  manifest <- vector("list", nrow(queue))
  cand_store <- list()
  for (qi in seq_len(nrow(queue))) {
    id <- queue$id[qi]; reason <- queue$review_reason[qi]; prio <- queue$priority[qi]
    res <- tryCatch(
      .reviewOneDeployment(id = id, reason = reason, priority = prio, r = r, mapping = mapping,
                           video.metadata = video.metadata, configs = configs, output.dir = output.dir,
                           types = types, n = n, window = window, clips.per.deployment = clips.per.deployment,
                           max.candidates = max.candidates, side = side, overlay.fps = overlay.fps, codec = codec,
                           id.col = id.col, datetime.col = datetime.col),
      error = function(e) .reviewOut(.manifestRow(id, reason, prio, "error", status = "render_failed")))
    manifest[[qi]] <- res$rows
    if (length(res$candidates)) cand_store[[id]] <- res$candidates
    if (lvl >= 2L) {
      nr <- sum(res$rows$status == "rendered")
      if (nr > 0) {
        mode <- if (res$rows$mapping_source[1] %in% c("documented_vs_data", "compare")) "compare" else "single"
        .log_ok(lvl, sprintf("%s \u00b7 %s \u00b7 %d clip%s (%s)", id, reason, nr, if (nr != 1) "s" else "", mode))
      }
    }
    if (!is.null(pb_id)) cli::cli_progress_update(id = pb_id, set = qi)
  }
  if (!is.null(pb_id)) cli::cli_progress_done(id = pb_id)

  result <- do.call(rbind, manifest)          # per-clip manifest, kept on the review object as an attribute
  rownames(result) <- NULL
  sheet   <- .buildReviewSheet(queue, result, cand_store)
  review  <- .newReview(sheet, candidates = cand_store, base_mapping = base_set,
                        manifest = result, output.dir = output.dir)

  if (lvl >= 1L) {
    n_clips  <- sum(result$status == "rendered")
    n_deps   <- sum(sheet$status == "rendered")
    no_video <- sheet$id[sheet$status == "no_video"]
    no_seg   <- sheet$id[sheet$status %in% c("no_segment", "no_coverage")]
    failed   <- sheet$id[sheet$status == "render_failed"]
    needs    <- .reviewDecisionsNeeded(sheet, cand_store)
    .log_summary(lvl)
    .log_detail(lvl, sprintf("%d clip%s across %d deployment%s", n_clips, if (n_clips != 1) "s" else "",
                             n_deps, if (n_deps != 1) "s" else ""))
    if (length(needs))    .log_arrow(lvl, sprintf("decisions needed (fill `decision`): %d (%s)", length(needs), .truncList(needs)))
    if (length(no_seg))   .log_detail(lvl, sprintf("no clear manoeuvre on camera: %d (%s)", length(no_seg), .truncList(no_seg)))
    if (length(no_video)) .log_detail(lvl, sprintf("no footage, not reviewable: %d (%s)", length(no_video), .truncList(no_video)))
    if (length(failed))   .log_detail(lvl, sprintf("render failed: %d (%s)", length(failed), .truncList(failed)))
    .log_done(lvl, n_clips, " review clip", if (n_clips != 1) "s", " written to ", output.dir)
    .log_runtime(lvl, start.time)
  }
  invisible(review)
}
