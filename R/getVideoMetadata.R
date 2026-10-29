#' Extract timing metadata from camera-tag videos
#'
#' @description
#' Recursively discovers camera-tag video files and extracts the timing information required to align
#' them with archival sensor data. The function returns a standardised segment table with one row per
#' video, including its deployment identifier, recording start and end, duration, frame rate and file
#' path.
#'
#' Recording start times can be read from timestamps encoded in file names, obtained by optical
#' character recognition (OCR) from a clock burned into the image, or resolved by using the file name
#' first and OCR only as a fallback. Duration and frame rate are read directly from each video with
#' `ffprobe`. An optional OCR cross-check can identify disagreements between the encoded and on-screen
#' clocks.
#'
#' Deployment-level clock corrections can be supplied explicitly to place camera timestamps on the
#' same time base as the sensor stream. Every applied correction is retained in the returned table as
#' provenance. Corrections may be created manually or derived for supported sidecar metadata with
#' [getVideoClockCorrections()]; they are never inferred or applied silently by this function.
#'
#' The returned table is the entry point to the \pkg{nautilus} video workflow. It can be used to locate
#' the file covering a sensor-data interval, launch or annotate footage, inspect uncertain timestamps,
#' and render sensor overlays.
#'
#' @param video.folders Character vector of directories to search for videos. Subdirectories are
#'   searched recursively. For an unnamed vector, each directory basename becomes its deployment ID.
#'   For arbitrary layouts, supply a fully named vector whose names are the authoritative deployment
#'   IDs and whose values are the directories to search, for example
#'   `c(PIN_CAM_31 = "./PIN_CAM_31/MP4")`. Names may be repeated when one deployment spans several
#'   directories. A vector that mixes named and unnamed elements is rejected.
#' @param video.format Character vector of video-file extensions to include: `"mp4"`, `"mov"`, or both
#'   (default `"mp4"`).
#' @param timestamp.source Method used to obtain recording start times. `"auto"` (default) uses a
#'   recognised file-name timestamp where available and otherwise falls back to OCR. `"filename"` uses
#'   file names only and leaves an unrecognised start time as `NA`. `"ocr"` reads the on-screen clock
#'   for every video and ignores any timestamp in the file name.
#' @param cross.check Logical; whether to read the on-screen clock for videos whose primary timestamp
#'   came from the file name and compare the two estimates (default `FALSE`). Differences greater than
#'   two seconds are flagged in `ocr_flag`. This requires an additional OCR pass for every applicable
#'   video.
#' @param clock.corrections Optional data frame containing one row per deployment to correct, with
#'   columns `ID` and `clock_correction_s`. The latter gives the number of seconds to add to the
#'   extracted camera timestamp. An optional character column `clock_correction_source` records the
#'   provenance of each value and defaults to `"manual"` when absent or empty. Use
#'   [getVideoClockCorrections()] to derive a reviewable table from imported CATS sidecar metadata.
#'   Default `NULL`, which applies no correction.
#' @param ocr A control object from [ocrControl()] specifying the recognition model, position of the
#'   on-screen timestamp, image preprocessing and frame-search behaviour. It is consulted only when OCR
#'   is required.
#' @param use.parallel Logical; whether to process videos in parallel when more than one file is found
#'   (default `TRUE`).
#' @param n.cores Number of parallel workers, or `NULL` (default) to use all detected cores except one.
#'   Only used when `use.parallel = TRUE` and more than one video is processed.
#' @param verbose How much detail to print: `0`/`"quiet"`, `1`/`"normal"`, or `2`/`"detailed"`
#'   (default), which reports deployment-level extraction outcomes.
#'
#' @details
#' ## Deployment identifiers and file discovery
#'
#' Every supplied directory is searched recursively for the extensions selected by `video.format`.
#' Empty directories are omitted; the function stops if none of the supplied directories contains a
#' matching video. Each discovered file is assigned to the deployment associated with its search
#' directory.
#'
#' For the usual layout, in which each path is a deployment root, an unnamed vector is sufficient and
#' the basename of each path is used as the ID. When the search path itself is a generic media directory
#' such as `MP4` or `DCIM`, the path cannot identify the deployment reliably. In that case the vector
#' must be named explicitly. These resolved IDs are used throughout the returned table and are also the
#' keys against which `clock.corrections` is validated.
#'
#' ## Timestamp extraction
#'
#' The file-name parser recognises plausible timestamps in either `YYYYMMDD-HHMMSS` or
#' `YYMMDD-HHMMSS` form. File-name timestamps are preferred by `timestamp.source = "auto"` because they
#' are exact to the second, inexpensive to read and independent of image quality. File names without a
#' recognised timestamp fall back to OCR under `"auto"`, or remain missing under `"filename"`.
#'
#' OCR reads the clock burned into the video image using the settings in `ocr`. It is slower and may be
#' affected by darkness, motion blur, compression artefacts or an incorrectly specified crop. Use
#' `cross.check = TRUE` when validating a new camera system or file-naming convention. The difference is
#' calculated as the file-name time minus the OCR time; an absolute difference greater than two seconds
#' sets `ocr_flag` to `TRUE`.
#'
#' Duration and frame rate are read independently of the timestamp source. `end` is calculated as
#' `start + duration`; it is `NA` when either component is unavailable.
#'
#' ## Clock corrections and time zones
#'
#' File-name and OCR clock readings are parsed and stored as `POSIXct` values in UTC. This assigns a
#' common time-zone representation; it does not establish that the physical camera clock was configured
#' to UTC. Where a camera clock used a fixed local offset, the appropriate number of seconds must be
#' supplied through `clock.corrections` before the video and sensor timelines are compared.
#'
#' Correction IDs must match the deployment IDs resolved from `video.folders` exactly. Duplicate or
#' unmatched correction IDs are errors, and the complete correction table is validated before video
#' decoding begins. A non-zero correction is added to `start`, `end` and, where present, `ocr_start`.
#' The within-file comparison `ocr_offset_s` is unchanged because both clock estimates receive the same
#' shift. The original camera-clock reading remains recoverable as `start - clock_correction_s`.
#'
#' A new call always extracts timestamps afresh from the source files, so corrections cannot accumulate
#' across calls. Internal application also refuses to add a correction to a row already carrying a
#' non-zero `clock_correction_s`. A file without a start timestamp cannot be corrected: it remains in
#' the output with zero correction and missing correction provenance, and is reported to the user.
#'
#' ## External software and parallel processing
#'
#' `ffprobe` is required for every run to read video duration and frame rate. OCR additionally requires
#' `ffmpeg` to extract frames and the \pkg{tesseract} and \pkg{magick} packages to process them. The
#' camera-specific OCR model is prepared only when an OCR operation is actually required; a pure
#' file-name workflow does not load the image-recognition dependencies.
#'
#' Parallel processing uses a single worker pool across all discovered files, allowing deployments with
#' different numbers of segments to share the available workers. Set `use.parallel = FALSE` for
#' sequential execution or when diagnosing external-software problems.
#'
#' ## Quality control and warnings
#'
#' The function retains every discovered video, including files for which no reliable start timestamp
#' could be obtained. Such rows have missing `start`, `end` and `timestamp_source` values and are
#' reported in the final summary rather than silently discarded. OCR cross-check discrepancies are
#' returned as flags for review; they do not alter or remove the file-name timestamp.
#'
#' Missing input directories, unsupported arguments, a complete absence of matching video files, and
#' invalid clock-correction tables are treated as errors. Unreadable video properties are returned as
#' missing values where possible, preserving the file record for subsequent inspection.
#'
#' @return A data frame with one row per discovered video and the following columns:
#'
#' \describe{
#'   \item{`ID`}{Deployment identifier resolved from `video.folders`.}
#'   \item{`video`}{Video file name, without its directory.}
#'   \item{`start`, `end`}{Recording start and end as `POSIXct` values represented in UTC. These include
#'     any applied clock correction.}
#'   \item{`duration`}{Video duration in seconds.}
#'   \item{`frame_rate`}{Nominal video frame rate in frames per second.}
#'   \item{`file`}{Full path to the video file.}
#'   \item{`timestamp_source`}{Source selected for `start`: `"filename"`, `"ocr"`, or `NA` when neither
#'     produced a timestamp.}
#'   \item{`clock_correction_s`}{Number of seconds added to the extracted camera clock; `0` where no
#'     correction was applied.}
#'   \item{`clock_correction_source`}{Provenance supplied with the applied correction, or `NA` where the
#'     row was not corrected.}
#' }
#'
#' With `cross.check = TRUE`, the result also contains:
#'
#' \describe{
#'   \item{`ocr_start`}{Start time obtained independently from the on-screen clock, including any
#'     applied correction.}
#'   \item{`ocr_offset_s`}{File-name start minus OCR start, in seconds. Defined only where the primary
#'     timestamp came from the file name and OCR also succeeded.}
#'   \item{`ocr_flag`}{Logical flag indicating an absolute `ocr_offset_s` greater than two seconds.}
#' }
#'
#' @seealso [getVideoClockCorrections()] for deriving clock corrections from imported tag metadata;
#'   [ocrControl()] for configuring timestamp recognition; [saveUncertainTimestampFrames()] for
#'   reviewing unresolved or discrepant timestamps; [filterVideoPeriod()] and [launchVideo()] for
#'   locating and opening footage; [renderOverlayVideo()] for compositing video with sensor data.
#'
#' @examples
#' \dontrun{
#' # Extract one row per video from deployment-root directories.
#' video_metadata <- getVideoMetadata(
#'   c("./videos/PIN_CAM_01", "./videos/PIN_CAM_02")
#' )
#'
#' # Map nested media directories explicitly to deployment IDs.
#' camera_folders <- c(
#'   PIN_CAM_31 = "./videos/PIN_CAM_31/MP4",
#'   PIN_CAM_32 = "./videos/PIN_CAM_32/MP4"
#' )
#'
#' # Derive reviewable clock corrections from imported sensor datasets.
#' imported_files <- list.files("./01_imported", pattern = "[.]rds$", full.names = TRUE)
#' corrections <- getVideoClockCorrections(imported_files)
#' video_metadata <- getVideoMetadata(
#'   camera_folders,
#'   clock.corrections = corrections
#' )
#'
#' # Supply a manual correction when no supported sidecar record is available.
#' manual_correction <- data.frame(
#'   ID = "PIN_CAM_31",
#'   clock_correction_s = 3600,
#'   clock_correction_source = "deployment log"
#' )
#' video_metadata <- getVideoMetadata(
#'   camera_folders["PIN_CAM_31"],
#'   clock.corrections = manual_correction,
#'   cross.check = TRUE
#' )
#' }
#' @export

getVideoMetadata <- function(video.folders,
                             video.format = "mp4",
                             timestamp.source = c("auto", "filename", "ocr"),
                             cross.check = FALSE,
                             clock.corrections = NULL,
                             ocr = ocrControl(),
                             use.parallel = TRUE,
                             n.cores = NULL,
                             verbose = "detailed") {


  ##############################################################################
  # Initial checks and setup ###################################################
  ##############################################################################

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)

  # validate arguments and resolve the explicit deployment-ID -> search-folder contract before touching
  # the file system. Names are authoritative only when every path is named; otherwise basenames retain
  # the simple historical behaviour.
  folder_plan <- .resolveVideoFolders(video.folders)
  if (any(!video.format %in% c("mp4", "mov"))) .abort("{.arg video.format} must be {.val mp4} and/or {.val mov}.")
  timestamp.source <- timestamp.source[1]                    # take the default when the full vector is passed
  .assert_choice(timestamp.source, "timestamp.source", c("auto", "filename", "ocr"))
  .assert_flag(cross.check, "cross.check")
  .assert_flag(use.parallel, "use.parallel")
  ocr <- .as_control(ocr, ocrControl, "nautilus_ocr", "ocr")
  missing_folders <- folder_plan$folder[!dir.exists(folder_plan$folder)]
  if (length(missing_folders))
    .abort(c("These folders were not found:", stats::setNames(missing_folders, rep("*", length(missing_folders)))))


  ##############################################################################
  # Discover video files #######################################################
  ##############################################################################

  search_pattern <- paste0("[.](", paste0(video.format, collapse = "|"), ")$")
  video_files <- lapply(folder_plan$folder, function(folder)
    list.files(folder, full.names = TRUE, pattern = search_pattern, recursive = TRUE))

  # keep only folders with at least one video file
  keep <- lengths(video_files) > 0
  folder_plan <- folder_plan[keep, , drop = FALSE]; video_files <- video_files[keep]
  if (!length(video_files))
    .abort("No {.val {video.format}} video files found in {.arg video.folders}.")

  n_deployments <- length(unique(folder_plan$ID))
  n_videos  <- sum(lengths(video_files))

  # flat task list: one row per (deployment, video) across ALL deployments, so a single worker pool
  # load-balances across deployments (a 2-video folder and a 90-video one draw from the same pool)
  tasks <- data.frame(id = rep(folder_plan$ID, lengths(video_files)),
                      video = unlist(video_files, use.names = FALSE), stringsAsFactors = FALSE)

  # Validate the whole correction plan before invoking ffprobe/OCR, including exact matching to the
  # resolved deployment IDs. A typo must fail before expensive work starts.
  clock.corrections <- .validateVideoClockCorrections(clock.corrections, unique(tasks$id))


  ##############################################################################
  # File-name timestamps (primary source; cheap, dependency-free) ##############
  ##############################################################################

  fn_list  <- lapply(tasks$video, .videoStartFromFilename)
  has_fn   <- !vapply(fn_list, is.null, logical(1))
  fn_start <- as.POSIXct(vapply(fn_list, function(x) if (is.null(x)) NA_real_ else as.numeric(x), NA_real_),
                         origin = "1970-01-01", tz = "UTC")

  # is OCR actually needed?  Only then are tesseract / magick / ffmpeg required - a pure file-name run
  # (all names carry a timestamp, no cross-check) has none of those dependencies.
  ocr_needed <- switch(timestamp.source,
                       filename = FALSE,
                       ocr      = TRUE,
                       auto     = !all(has_fn)) || cross.check


  ##############################################################################
  # Resolve external tools + OCR model (only when OCR is needed) ###############
  ##############################################################################

  # ffprobe (duration / frame rate) is always required; ffmpeg (frame extraction) only for OCR. Resolved
  # once here and threaded to the workers (parallel-safe; no per-video Sys.which, no clusterExport of a resolver).
  ffprobe_bin <- .ffprobeBin()
  ffmpeg_bin  <- NULL
  ocr_model   <- ocr$model
  whitelist   <- NULL

  if (ocr_needed) {
    for (pkg in c("tesseract", "magick"))
      if (!requireNamespace(pkg, quietly = TRUE))
        .abort(c("The {.pkg {pkg}} package is required for OCR timestamp extraction: {.code install.packages('{pkg}')}.",
                 "i" = "Or set {.code timestamp.source = \"filename\"} to use file-name timestamps only."))
    ffmpeg_bin <- .ffmpegBin()
    prepared   <- .prepareOcrModel(ocr$model, lvl)
    ocr_model  <- prepared$model
    whitelist  <- if (!is.null(ocr$char.whitelist)) ocr$char.whitelist else prepared$whitelist
  }


  ##############################################################################
  # Setup parallel processing ##################################################
  ##############################################################################

  use_par <- use.parallel && n_videos > 1
  if (use_par) {

    if (is.null(n.cores)) n.cores <- max(1, parallel::detectCores() - 1)

    for (pkg in c("foreach", "doSNOW", "parallel"))
      if (!requireNamespace(pkg, quietly = TRUE))
        .abort(c("The {.pkg {pkg}} package is required for parallel processing.",
                 "i" = "Install it, or set {.code use.parallel = FALSE}."))

    cl <- parallel::makeCluster(n.cores)
    doSNOW::registerDoSNOW(cl)
    on.exit(tryCatch(parallel::stopCluster(cl), error = function(e) NULL), add = TRUE)

    `%dopar%` <- foreach::`%dopar%`

    # OCR / image packages in every worker (only when OCR is in play)
    if (ocr_needed) parallel::clusterEvalQ(cl, { library(tesseract); library(magick) })

    # make the WHOLE nautilus namespace available to each worker, so the internal pipeline functions
    # never have to be listed by hand (a forgotten name would be a silent worker error)
    parallel::clusterExport(cl, varlist = ls(getNamespace("nautilus"), all.names = TRUE),
                            envir = getNamespace("nautilus"))
  }


  ##############################################################################
  # Verbose header #############################################################
  ##############################################################################

  src_desc <- switch(timestamp.source,
                     auto     = "file name (OCR fallback)",
                     filename = "file name",
                     ocr      = "OCR")

  .log_header(lvl, "getVideoMetadata", "Reading timestamp metadata from camera videos",
              bullets = sprintf("Input: %d video%s across %d dataset%s",
                                n_videos, if (n_videos != 1) "s" else "",
                                n_deployments, if (n_deployments != 1) "s" else ""),
              arrow = sprintf("Timestamp source: %s%s%s%s", src_desc,
                              if (cross.check) " \u00b7 OCR cross-check" else "",
                              if (use_par) sprintf(" \u00b7 %d cores", n.cores) else "",
                              if (nrow(clock.corrections))
                                sprintf(" \u00b7 %d clock correction%s", nrow(clock.corrections),
                                        if (nrow(clock.corrections) != 1L) "s" else "") else ""))


  ##############################################################################
  # Analyse every video ########################################################
  ##############################################################################

  # one overall, self-clearing progress bar across all videos (transient; auto-suppressed non-interactively)
  pb_id <- if (lvl >= 1L) cli::cli_progress_bar(
    format = "{cli::pb_spin} Reading timestamps {cli::pb_bar} {cli::pb_percent} \u00b7 {cli::pb_current}/{cli::pb_total} videos",
    total = n_videos, clear = TRUE, .envir = environment()) else NULL

  if (use_par) {
    opts <- if (!is.null(pb_id)) list(progress = function(n) cli::cli_progress_update(id = pb_id, set = n)) else list()
    all_rows <- foreach::foreach(t = seq_len(nrow(tasks)), .options.snow = opts,
                                 .packages = if (ocr_needed) c("tesseract", "magick") else character(0),
                                 .combine = rbind) %dopar% {
                                   .analyseVideo(tasks$video[t], tasks$id[t], fn_start[t], timestamp.source,
                                                 cross.check, ocr, ocr_model, whitelist, ocr_engine = NULL,
                                                 ffmpeg_bin, ffprobe_bin)
                                 }
  } else {
    parts <- vector("list", nrow(tasks))
    for (t in seq_len(nrow(tasks))) {
      parts[[t]] <- .analyseVideo(tasks$video[t], tasks$id[t], fn_start[t], timestamp.source,
                                  cross.check, ocr, ocr_model, whitelist, ocr_engine = NULL,
                                  ffmpeg_bin, ffprobe_bin)
      if (!is.null(pb_id)) cli::cli_progress_update(id = pb_id, set = t)
    }
    all_rows <- do.call(rbind, parts)
  }
  if (!is.null(pb_id)) cli::cli_progress_done(id = pb_id)

  # Extraction above always starts from the source clock, so a fresh getVideoMetadata() call cannot
  # stack corrections. The shared helper still guards existing non-zero provenance so it is safe to
  # reuse on a previously produced table if that becomes part of the public API later.
  all_rows <- .applyVideoClockCorrections(all_rows, clock.corrections)


  ##############################################################################
  # Per-deployment reporting + summary #########################################
  ##############################################################################

  # split the flat result by ID (in input order) and - at the detailed level - emit one line per deployment
  result <- do.call(rbind, lapply(split(all_rows, factor(all_rows$ID, levels = unique(tasks$id))), function(dep) {
    if (lvl >= 2L) {
      nf <- sum(dep$timestamp_source == "filename", na.rm = TRUE)
      no <- sum(dep$timestamp_source == "ocr", na.rm = TRUE)
      nn <- sum(is.na(dep$timestamp_source))
      bits <- c(if (nf) sprintf("%d file name", nf), if (no) sprintf("%d OCR", no), if (nn) sprintf("%d none", nn))
      flag <- if (cross.check) { fl <- sum(dep$ocr_flag, na.rm = TRUE); if (fl) sprintf(" \u00b7 %d flagged", fl) else "" } else ""
      shifts <- unique(dep$clock_correction_s[is.finite(dep$clock_correction_s) & dep$clock_correction_s != 0])
      clock <- if (length(shifts) == 1L) sprintf(" \u00b7 clock %+.10g s", shifts) else ""
      .log_ok(lvl, sprintf("%s \u00b7 %d video%s \u00b7 %s%s%s", dep$ID[1], nrow(dep),
                           if (nrow(dep) != 1) "s" else "", paste(bits, collapse = ", "), flag, clock))
    }
    dep
  }))
  rownames(result) <- NULL

  if (lvl >= 1L) {
    .log_summary(lvl)
    nf <- sum(result$timestamp_source == "filename", na.rm = TRUE)
    no <- sum(result$timestamp_source == "ocr", na.rm = TRUE)
    nn <- sum(is.na(result$timestamp_source))
    ntot <- max(1, nrow(result))
    .log_detail(lvl, sprintf("from file name: %d/%d (%.0f%%)", nf, nrow(result), 100 * nf / ntot))
    if (no) .log_detail(lvl, sprintf("from OCR: %d/%d (%.0f%%)", no, nrow(result), 100 * no / ntot))
    if (cross.check) {
      fl <- sum(result$ocr_flag, na.rm = TRUE)
      .log_detail(lvl, sprintf("cross-check discrepancies (> 2 s): %d/%d", fl, nrow(result)))
    }
    nc <- sum(result$clock_correction_s != 0)
    if (nc) {
      nd <- length(unique(result$ID[result$clock_correction_s != 0]))
      .log_detail(lvl, sprintf("clock corrected: %d/%d video%s across %d deployment%s", nc, nrow(result),
                               if (nc != 1L) "s" else "", nd, if (nd != 1L) "s" else ""))
    }
    if (nn) cli::cli_alert_warning(sprintf("No timestamp for %d video%s (source unavailable) - {.field start} set to NA.",
                                           nn, if (nn != 1) "s" else ""))
    n_ok <- nf + no
    .log_done(lvl, n_ok, " timestamp", if (n_ok != 1) "s", " extracted from ", n_videos, " video", if (n_videos != 1) "s")
    .log_runtime(lvl, start.time)
  }

  result
}
