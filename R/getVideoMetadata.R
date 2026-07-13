#######################################################################################################
# Extract recording timestamps and metadata from biologging tag videos ################################
#######################################################################################################

#' Extract Metadata and Timestamps from Biologging Tag Video Files
#'
#' @description
#' Extracts the recording start time, end time, duration and frame rate of every video file found in
#' one or more directories, returning one row per video.
#'
#' The recording start time is taken, whenever possible, from the **file name**: on-camera systems
#' encode it as `YYYYMMDD-HHMMSS` or `YYMMDD-HHMMSS` (e.g. `CameraCMD71_Spot06-20190831-173658-...mp4`
#' or `230831-161949_CAM0bc99448_30.mp4`). File-name timestamps are exact, camera-agnostic and require
#' no image processing, so they are the primary and default source.
#'
#' For cameras whose file names carry no timestamp (e.g. MOBIUS units), the start time can instead be
#' read by Optical Character Recognition (OCR, via the Tesseract engine) from the timestamp burned into
#' the video's on-screen display. OCR is a *secondary* source: it is used only as a fallback where the
#' file name has no timestamp, or - with `cross.check = TRUE` - as an independent check on the file-name
#' time. All OCR settings (model, the on-screen box location, etc.) are bundled in \code{\link{ocrControl}}.
#'
#' Duration and frame rate are always read with `ffprobe`; `ffmpeg` and the OCR packages are required
#' only when OCR is actually used.
#'
#' @param video.folders Character vector. Paths to directories containing video files.
#' @param video.format Character vector. Allowed video formats, `"mp4"` and/or `"mov"`. Defaults to `"mp4"`.
#' @param timestamp.source How to obtain each video's start time:
#'   \itemize{
#'     \item `"auto"` (default): use the file-name timestamp; fall back to OCR only for videos whose file
#'       name has none.
#'     \item `"filename"`: use the file-name timestamp only (no OCR); videos without one get `NA`.
#'     \item `"ocr"`: read every timestamp by OCR from the on-screen display, ignoring the file name.
#'   }
#' @param cross.check Logical. When `TRUE`, videos whose start time came from the file name are also read
#'   by OCR and the two are compared; disagreements beyond 2 seconds are flagged (see the `ocr_flag`
#'   output column). Adds OCR cost but validates the file-name timestamps. Defaults to `FALSE`.
#' @param ocr An \code{\link{ocrControl}} object (or a named list of its fields) bundling the OCR
#'   settings - the Tesseract model, the on-screen timestamp box geometry, and the frame-search depth.
#'   Only consulted when OCR is used. Defaults to `ocrControl()`.
#' @param use.parallel Logical. Whether to process videos in parallel. Defaults to `TRUE`.
#' @param n.cores Integer. Number of cores for parallel processing. If `NULL`, uses `detectCores() - 1`.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal", or `2`/"detailed" (default).
#'
#' @return A data frame, one row per video, with columns: `ID`, `video` (file name), `start`/`end`
#'   (POSIXct, UTC), `duration` (seconds), `frame_rate` (fps), `file` (full path), and `timestamp_source`
#'   (`"filename"`, `"ocr"`, or `NA` when no timestamp could be obtained). When `cross.check = TRUE`,
#'   three further columns are added: `ocr_start` (the OCR-read start time), `ocr_offset_s`
#'   (`start - ocr_start`, seconds) and `ocr_flag` (logical; `TRUE` when the two disagree by > 2 s).
#'
#' @seealso \code{\link{ocrControl}}, \code{\link{saveUncertainTimestampFrames}}
#'
#' @examples
#' \dontrun{
#' # One row per video; start times taken from the file names where present
#' meta <- getVideoMetadata(c("./videos/PIN_CAM_01", "./videos/PIN_CAM_02"))
#'
#' # Cross-check the file-name timestamps against OCR of the on-screen clock
#' meta <- getVideoMetadata("./videos/PIN_CAM_01", cross.check = TRUE)
#' }
#'
#' @export

getVideoMetadata <- function(video.folders,
                             video.format = "mp4",
                             timestamp.source = c("auto", "filename", "ocr"),
                             cross.check = FALSE,
                             ocr = ocrControl(),
                             use.parallel = TRUE,
                             n.cores = NULL,
                             verbose = "detailed") {


  ##############################################################################
  # Initial checks and setup ###################################################
  ##############################################################################

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)

  # validate arguments
  if (!is.character(video.folders) || !length(video.folders)) .abort("{.arg video.folders} must be a character vector.")
  if (any(!video.format %in% c("mp4", "mov"))) .abort("{.arg video.format} must be {.val mp4} and/or {.val mov}.")
  timestamp.source <- timestamp.source[1]                    # take the default when the full vector is passed
  .assert_choice(timestamp.source, "timestamp.source", c("auto", "filename", "ocr"))
  .assert_flag(cross.check, "cross.check")
  .assert_flag(use.parallel, "use.parallel")
  ocr <- .as_control(ocr, ocrControl, "nautilus_ocr", "ocr")
  missing_folders <- video.folders[!dir.exists(video.folders)]
  if (length(missing_folders))
    .abort(c("These folders were not found:", stats::setNames(missing_folders, rep("*", length(missing_folders)))))


  ##############################################################################
  # Discover video files #######################################################
  ##############################################################################

  names(video.folders) <- basename(video.folders)
  search_pattern <- paste0("[.](", paste0(video.format, collapse = "|"), ")$")
  video_files <- lapply(video.folders, function(folder)
    list.files(folder, full.names = TRUE, pattern = search_pattern, recursive = TRUE))

  # keep only folders with at least one video file
  keep <- lengths(video_files) > 0
  video.folders <- video.folders[keep]; video_files <- video_files[keep]
  if (!length(video_files))
    .abort("No {.val {video.format}} video files found in {.arg video.folders}.")

  n_animals <- length(video.folders)
  n_videos  <- sum(lengths(video_files))

  # flat task list: one row per (deployment, video) across ALL deployments, so a single worker pool
  # load-balances across deployments (a 2-video folder and a 90-video one draw from the same pool)
  tasks <- data.frame(id = rep(names(video.folders), lengths(video_files)),
                      video = unlist(video_files, use.names = FALSE), stringsAsFactors = FALSE)


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
                                n_animals, if (n_animals != 1) "s" else ""),
              arrow = sprintf("Timestamp source: %s%s%s", src_desc,
                              if (cross.check) " \u00b7 OCR cross-check" else "",
                              if (use_par) sprintf(" \u00b7 %d cores", n.cores) else ""))


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
      .log_ok(lvl, sprintf("%s \u00b7 %d video%s \u00b7 %s%s", dep$ID[1], nrow(dep),
                           if (nrow(dep) != 1) "s" else "", paste(bits, collapse = ", "), flag))
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
    if (nn) cli::cli_alert_warning(sprintf("No timestamp for %d video%s (source unavailable) - {.field start} set to NA.",
                                           nn, if (nn != 1) "s" else ""))
    n_ok <- nf + no
    .log_done(lvl, n_ok, " timestamp", if (n_ok != 1) "s", " extracted from ", n_videos, " video", if (n_videos != 1) "s")
    .log_runtime(lvl, start.time)
  }

  result
}


################################################################################
# OCR model preparation ######################################################
################################################################################

# The fine-tuned cam-tag OCR model is ~11 MB, so it is NOT bundled with the package (CRAN keeps packages
# small); it is hosted as a GitHub release asset and fetched on demand into the per-user cache. Update all
# three constants together if the model is re-trained (md5 is the base-R integrity check; the sha256 for
# reference is 4dbab93bf5602352d037c7c0454278abe40a6a0fa47f242c2dca9c1e00e852d3).
.CAM_MODEL_URL   <- "https://github.com/miguelgandra/nautilus/releases/download/ocr-model-v1/cam.traineddata"
.CAM_MODEL_MD5   <- "139317158031f17b0ab26536cd99b1c9"
.CAM_MODEL_BYTES <- 11697722L

#' Resolve the cached cam-tag OCR model, downloading it on first use.
#'
#' Looks for a verified copy in the per-user cache (\code{tools::R_user_dir("nautilus", "cache")}); if it is
#' absent or corrupt and \code{download = TRUE}, fetches it from the GitHub release, verifies its md5, and
#' caches it for reuse.
#' @param download Fetch the model when the cache is empty/stale (FALSE = only report an existing cache hit).
#' @param quiet Suppress the progress/status messages.
#' @return Path to a verified `cam.traineddata` (the cached copy, or a session temp file if the cache is not
#'   writable), or NULL when it is absent and cannot be fetched.
#' @keywords internal
#' @noRd
.camModelPath <- function(download = TRUE, quiet = FALSE) {
  ok        <- function(p) is.character(p) && length(p) == 1L && file.exists(p) &&
                           identical(unname(tools::md5sum(p)), .CAM_MODEL_MD5)
  cache_dir <- tools::R_user_dir("nautilus", "cache")
  dest      <- file.path(cache_dir, "cam.traineddata")
  if (ok(dest)) return(dest)                                  # verified cache hit
  if (!download) return(NULL)
  if (!quiet) cli::cli_alert_info(
    "Downloading the cam-tag OCR model (~{round(.CAM_MODEL_BYTES / 1e6)} MB) from the {.pkg nautilus} GitHub release ...")
  tmp <- tempfile(fileext = ".traineddata")
  got <- tryCatch(suppressWarnings(utils::download.file(.CAM_MODEL_URL, tmp, mode = "wb", quiet = TRUE)) == 0L,
                  error = function(e) FALSE)
  if (!isTRUE(got) || !ok(tmp)) {
    unlink(tmp)
    if (!quiet) cli::cli_alert_warning(
      "Could not download or verify the cam-tag OCR model (offline, or the download was corrupt).")
    return(NULL)
  }
  cached <- tryCatch({                                        # cache for next time
    if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
    isTRUE(file.copy(tmp, dest, overwrite = TRUE)) && ok(dest)
  }, error = function(e) FALSE)
  if (isTRUE(cached)) { unlink(tmp); return(dest) }
  tmp                                                         # cache not writable -> valid for this session
}

#' Download the fine-tuned camera-tag OCR model
#'
#' @description
#' The `nautilus` optical-character-recognition (OCR) model that reads camera-tag on-screen timestamps is
#' ~11 MB and is NOT bundled with the package. \code{\link{getVideoMetadata}} fetches it automatically the
#' first time OCR is actually needed (that is, when a timestamp cannot be read from the file name). Call this
#' to pre-download it - for example before working offline. The model is cached in the per-user cache
#' directory (\code{tools::R_user_dir("nautilus", "cache")}) and reused thereafter, so it downloads only once.
#'
#' @param quiet Logical. Suppress progress messages. Default `FALSE`.
#' @return Invisibly, the path to the cached model, or `NULL` if it could not be downloaded (e.g. no
#'   internet). When it is unavailable, OCR falls back to Tesseract's generic `eng` model, which is less
#'   accurate for the cam-tag timestamp font.
#' @seealso \code{\link{getVideoMetadata}}
#' @examples
#' \dontrun{
#' installCamOcrModel()
#' }
#' @export
installCamOcrModel <- function(quiet = FALSE) {
  .assert_flag(quiet, "quiet")
  p <- .camModelPath(download = TRUE, quiet = quiet)
  if (!quiet) {
    if (is.null(p)) cli::cli_alert_danger("cam-tag OCR model not installed (see the warning above).")
    else            cli::cli_alert_success("cam-tag OCR model ready at {.file {p}}.")
  }
  invisible(p)
}

#' Ensure the OCR model is available and build the default character whitelist
#'
#' The fine-tuned cam-tag OCR model is fetched on demand (\code{.camModelPath}, exposed to users as
#' \code{\link{installCamOcrModel}}) and installed into the tesseract data directory automatically (no
#' interactive prompt - batch-safe). If that is not possible (offline, or the directory is not writable),
#' fall back to the generic `eng` model. Returns the (possibly adjusted) model name and the default
#' whitelist for the DDMmmYY on-screen timestamp format.
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.prepareOcrModel <- function(model, lvl) {

  # default whitelist for the "DDMmmYY HH:MM:SS" on-screen format: digits, month-abbreviation letters,
  # and the ":", "." and " " separators
  month_chars <- sort(unique(unlist(strsplit(paste(month.abb, collapse = ""), ""))))
  whitelist_default <- paste(c(0:9, month_chars, ":", ".", " "), collapse = "")

  if (identical(model, "cam")) {
    tesseract_path <- tesseract::tesseract_info()$datapath
    cam_data_path  <- file.path(tesseract_path, "cam.traineddata")

    if (!file.exists(cam_data_path)) {
      src <- .camModelPath(download = TRUE, quiet = lvl < 1L)     # cached or freshly downloaded model, or NULL
      installed <- !is.null(src) &&
        isTRUE(tryCatch(file.copy(src, cam_data_path, overwrite = TRUE), error = function(e) FALSE)) &&
        file.exists(cam_data_path)
      if (installed) {
        if (lvl >= 1L) cli::cli_alert_info("Installed the fine-tuned {.file cam.traineddata} OCR model.")
      } else {
        if (lvl >= 1L) cli::cli_alert_warning("Could not obtain {.file cam.traineddata} - using the {.val eng} model (less accurate for cam-tag timestamps).")
        model <- "eng"
      }
    }
  }

  list(model = model, whitelist = whitelist_default)
}


################################################################################
# Parallel-safe single-video analysis ########################################
################################################################################

#' Analyse a single video: duration, frame rate and start time (file name and/or OCR)
#'
#' Reads duration and frame rate with `ffprobe`, then resolves the recording start time according to
#' `timestamp.source`: from the pre-parsed file-name time `fn_start`, and/or by OCR of the on-screen
#' display. OCR is invoked only when required (no file-name time, or a cross-check), and the Tesseract
#' engine is built lazily - so a pure file-name deployment does no image work at all. Safe for parallel
#' workers (the engine is created inside the worker when needed).
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.analyseVideo <- function(video, id, fn_start, timestamp.source, cross.check,
                          ocr, ocr_model, whitelist, ocr_engine = NULL, ffmpeg_bin, ffprobe_bin) {

  # --- duration + frame rate (ffprobe); guard empty output (corrupt / non-video files) ---
  duration <- suppressWarnings(as.numeric(system2(ffprobe_bin,
    c("-i", shQuote(video), "-show_entries", "format=duration", "-v", "quiet", "-of", "csv=p=0"),
    stdout = TRUE, stderr = FALSE)))
  if (length(duration) != 1) duration <- NA_real_

  fr_raw <- system2(ffprobe_bin,
    c("-i", shQuote(video), "-select_streams", "v:0", "-show_entries", "stream=r_frame_rate", "-v", "quiet", "-of", "csv=p=0"),
    stdout = TRUE, stderr = FALSE)
  rate_parts <- suppressWarnings(as.numeric(strsplit(paste0(fr_raw, collapse = ""), "/")[[1]]))
  frame_rate <- if (length(rate_parts) == 2 && !is.na(rate_parts[2]) && rate_parts[2] != 0) rate_parts[1] / rate_parts[2] else NA_real_

  # --- lazy OCR: build the engine once (per worker) and read the on-screen start time only when asked ---
  engine_holder <- ocr_engine
  ocr_start_time <- function() {
    if (is.null(engine_holder))
      engine_holder <<- tesseract::tesseract(language = ocr_model,
        options = list(tessedit_pageseg_mode = 7, tessedit_char_whitelist = whitelist))
    .extractAndProcessFrame(video, id, 0, engine_holder, ocr, frame_rate = frame_rate,
                            ffmpeg_bin = ffmpeg_bin, ffprobe_bin = ffprobe_bin)$timestamp
  }

  # --- resolve start time + provenance ---
  have_fn   <- !is.na(fn_start)
  start     <- as.POSIXct(NA, tz = "UTC")
  source    <- NA_character_
  ocr_start <- as.POSIXct(NA, tz = "UTC")

  if (timestamp.source == "filename") {
    if (have_fn) { start <- fn_start; source <- "filename" }
  } else if (timestamp.source == "ocr") {
    ocr_start <- ocr_start_time()
    if (!is.na(ocr_start)) { start <- ocr_start; source <- "ocr" }
  } else {                                   # auto: file name first, OCR only if the name has no time
    if (have_fn) {
      start <- fn_start; source <- "filename"
    } else {
      ocr_start <- ocr_start_time()
      if (!is.na(ocr_start)) { start <- ocr_start; source <- "ocr" }
    }
  }

  # --- optional OCR cross-check (only meaningful when start came from the file name) ---
  ocr_offset_s <- NA_real_; ocr_flag <- NA
  if (cross.check && identical(source, "filename")) {
    if (is.na(ocr_start)) ocr_start <- ocr_start_time()
    if (!is.na(ocr_start)) {
      ocr_offset_s <- as.numeric(difftime(start, ocr_start, units = "secs"))
      ocr_flag <- abs(ocr_offset_s) > 2
    }
  }

  video_end <- if (!is.na(start) && !is.na(duration)) start + duration else as.POSIXct(NA, tz = "UTC")

  out <- data.frame(
    ID = id, video = basename(video), start = start, end = video_end,
    duration = duration, frame_rate = frame_rate, file = video,
    timestamp_source = source, stringsAsFactors = FALSE)

  if (cross.check) {
    out$ocr_start    <- ocr_start
    out$ocr_offset_s <- ocr_offset_s
    out$ocr_flag     <- ocr_flag
  }
  out
}


################################################################################
# Extract and OCR a single frame, with a blank-frame search fallback ##########
################################################################################

#' Extract and process a video frame to retrieve the embedded on-screen timestamp
#'
#' Attempts to extract a frame at a given time offset and read a timestamp by OCR. If the first frame has
#' no recognisable timestamp (partially blank, glitched, or missing overlay), it searches subsequent
#' frames (up to `ocr$max.search.frames`) and, on success, back-calculates the value to the requested
#' offset using the frame rate.
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.extractAndProcessFrame <- function(video, id, time_offset, ocr_engine, ocr, frame_rate = NULL,
                                    ffmpeg_bin, ffprobe_bin) {

  # process the requested frame first
  result <- .processSingleFrame(time_offset, video, id, ocr_engine, ocr, ffmpeg_bin = ffmpeg_bin)

  # if no timestamp was retrieved, re-try after locating the box within a search radius (overlay drift)
  if (is.na(result$timestamp)) {
    frame_id   <- paste0(id, "-", gsub("\\.", "_", as.character(time_offset)))
    frame_path <- file.path(tempdir(), sprintf("%s-frame.jpg", frame_id))
    system2(ffmpeg_bin, c("-y", "-ss", sprintf("%.3f", time_offset), "-i", shQuote(video),
                          "-vframes", "1", "-q:v", "1", "-vf", sprintf("scale=-1:%d", ocr$frame.height), shQuote(frame_path)),
            stdout = FALSE, stderr = FALSE)
    if (file.exists(frame_path)) {
      input_frame     <- magick::image_read(frame_path)
      detected_coords <- .detectTimestampBox(input_frame,
                                             expected_x = ocr$box[1], expected_y = ocr$box[2],
                                             expected_width = ocr$box[3], expected_height = ocr$box[4],
                                             search_radius = ocr$search.radius)
      result <- .processSingleFrame(time_offset, video, id, ocr_engine, ocr, box.coords = detected_coords, ffmpeg_bin = ffmpeg_bin)
      unlink(frame_path)
    }
  }

  # if we found a timestamp, or this is not the first frame, return the result
  if (!is.na(result$timestamp) || time_offset != 0)
    return(list(timestamp = result$timestamp, raw_ocr = result$raw_ocr))

  # first frame with no timestamp: search subsequent frames, correcting a hit back to the start time
  if (is.null(frame_rate) || is.na(frame_rate)) {
    fr_raw <- system2(ffprobe_bin,
      c("-i", shQuote(video), "-select_streams", "v:0", "-show_entries", "stream=r_frame_rate", "-v", "quiet", "-of", "csv=p=0"),
      stdout = TRUE, stderr = FALSE)
    rate_parts <- suppressWarnings(as.numeric(strsplit(paste0(fr_raw, collapse = ""), "/")[[1]]))
    frame_rate <- if (length(rate_parts) == 2 && !is.na(rate_parts[2]) && rate_parts[2] != 0) rate_parts[1] / rate_parts[2] else NA_real_
  }
  if (is.na(frame_rate) || frame_rate <= 0) return(list(timestamp = NA, raw_ocr = NA))

  time_per_frame <- 1 / frame_rate
  for (frame_num in seq_len(ocr$max.search.frames)) {
    search_time_offset <- frame_num * time_per_frame
    search_result <- .processSingleFrame(search_time_offset, video, id, ocr_engine, ocr, ffmpeg_bin = ffmpeg_bin)
    if (!is.na(search_result$timestamp))
      return(list(timestamp = search_result$timestamp - search_time_offset, raw_ocr = search_result$raw_ocr))
  }
  list(timestamp = NA, raw_ocr = NA)
}


################################################################################
# Extract + preprocess + OCR one frame at a given time ########################
################################################################################

#' Extract a single frame and OCR the on-screen timestamp box
#'
#' Extracts a frame at a given time offset with `ffmpeg`, crops the configured timestamp box, applies a
#' preprocessing pipeline to improve recognition, and OCRs it. If the cropped box has no dark pixels
#' (usually indicating no text) OCR is skipped to save time.
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.processSingleFrame <- function(current_time_offset, video, id, ocr_engine, ocr, box.coords = NULL, ffmpeg_bin) {

  # extract frame at the requested offset (fast seek), normalised to the reference frame height so the
  # box coordinates always apply
  frame_id         <- paste0(id, "-", gsub("\\.", "_", as.character(current_time_offset)))
  first_frame_path <- file.path(tempdir(), sprintf("%s-frame.jpg", frame_id))
  system2(ffmpeg_bin, c("-y", "-ss", sprintf("%.3f", current_time_offset), "-i", shQuote(video),
                        "-vframes", "1", "-q:v", "1", "-vf", sprintf("scale=-1:%d", ocr$frame.height), shQuote(first_frame_path)),
          stdout = FALSE, stderr = FALSE)

  if (!file.exists(first_frame_path))
    return(list(timestamp = NA, raw_ocr = NA, contains_text = FALSE))

  input_frame <- magick::image_read(first_frame_path)

  # timestamp box: the configured location, unless caller passed detected coordinates
  if (is.null(box.coords))
    box.coords <- list(x = ocr$box[1], y = ocr$box[2], width = ocr$box[3], height = ocr$box[4])
  crop_geometry <- sprintf("%dx%d+%d+%d", box.coords$width, box.coords$height, box.coords$x, box.coords$y)
  cropped_image <- magick::image_crop(input_frame, geometry = crop_geometry)

  # pad, then enhance/binarise to optimise recognition of the overlay digits
  processed_image <- magick::image_extent(cropped_image,
                                          geometry = sprintf("%dx%d", box.coords$width + 20, box.coords$height + 21),
                                          color = "black", gravity = "center")
  processed_image <- magick::image_convert(processed_image, colorspace = "gray")
  processed_image <- magick::image_negate(processed_image)
  processed_image <- magick::image_contrast(processed_image, sharpen = 1)
  processed_image <- magick::image_resize(processed_image, geometry = "300%")
  processed_image <- magick::image_morphology(processed_image, method = "Close", kernel = "Diamond", iterations = 1)
  processed_image <- magick::image_morphology(processed_image, method = "Open", kernel = "Disk", iterations = 1)
  processed_image <- magick::image_threshold(processed_image, type = "white", threshold = "70%")
  processed_image <- magick::image_threshold(processed_image, type = "black", threshold = "60%")

  # skip OCR when the box has no dark pixels (no text)
  img_data       <- magick::image_data(processed_image, channels = "gray")
  contains_black <- any(as.integer(img_data[1, , ]) == 0)
  unlink(first_frame_path)
  if (!contains_black)
    return(list(timestamp = NA, raw_ocr = NA, contains_text = FALSE))

  # OCR + parse
  ocr_text  <- tesseract::ocr(processed_image, engine = ocr_engine)
  timestamp <- .parseTimestamp(ocr_text, id, current_time_offset)
  list(timestamp = timestamp, raw_ocr = ocr_text, contains_text = TRUE)
}


################################################################################
# Auto-detect the timestamp box coordinates within a search radius ############
################################################################################

#' Detect the white timestamp box within a frame
#'
#' Searches around the expected box location for the bright timestamp panel and returns the bounding box
#' of its white pixels; if none are found, the expected coordinates are returned unchanged.
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.detectTimestampBox <- function(frame_image,
                                expected_x = 3249,
                                expected_y = 2120,
                                expected_width = 325,
                                expected_height = 28,
                                search_radius = 80) {

  # calculate search region, clamped to the frame
  search_width  <- expected_width  + (search_radius * 2)
  search_height <- expected_height + (search_radius * 2)
  search_x      <- expected_x - search_radius
  search_y      <- expected_y - search_radius

  frame_info    <- magick::image_info(frame_image)
  search_x      <- max(0, search_x)
  search_y      <- max(0, search_y)
  search_width  <- min(search_width,  frame_info$width  - search_x)
  search_height <- min(search_height, frame_info$height - search_y)

  search_geometry <- sprintf("%dx%d+%d+%d", search_width, search_height, search_x, search_y)
  search_region   <- magick::image_crop(frame_image, geometry = search_geometry)

  # binarise to isolate the white timestamp box
  gray_region <- magick::image_convert(search_region, colorspace = "gray")
  enhanced    <- magick::image_contrast(gray_region, sharpen = 2)
  binary      <- magick::image_threshold(enhanced, type = "white", threshold = "30%")
  binary      <- magick::image_negate(binary)
  cleaned     <- magick::image_morphology(binary, method = "Close", kernel = "Rectangle:10x3")
  cleaned     <- magick::image_morphology(cleaned, method = "Open", kernel = "Rectangle:5x2")

  # pixel matrix (0-255)
  pixel_data <- magick::image_data(cleaned, channels = "gray")
  if (is.character(pixel_data)) {
    pixel_matrix <- apply(pixel_data[1, , ], c(1, 2), function(x) strtoi(x, base = 16))
  } else {
    pixel_matrix <- as.numeric(pixel_data[1, , ])
    dim(pixel_matrix) <- dim(pixel_data)[2:3]
  }

  # near-white pixels (>= 252); if none, keep the expected coordinates
  white_coords <- which(pixel_matrix >= 252, arr.ind = TRUE)
  if (nrow(white_coords) == 0)
    return(list(x = expected_x, y = expected_y, width = expected_width, height = expected_height))

  # bounding box of white pixels (rows/cols map to y/x); convert back to frame coordinates
  min_x <- min(white_coords[, 1]); max_x <- max(white_coords[, 1])
  min_y <- min(white_coords[, 2]); max_y <- max(white_coords[, 2])
  list(x = search_x + min_x - 1, y = search_y + min_y - 1,
       width = max_x - min_x + 1, height = max_y - min_y + 1)
}


################################################################################
# On-screen timestamp parsing (DDMmmYY HH:MM:SS overlay) ######################
################################################################################

#' Parse an OCR-read on-screen timestamp string to POSIXct
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.parseTimestamp <- function(ocr_text, id, time_offset = 0) {

  # normalise whitespace
  ocr_text <- gsub("\\s+", " ", ocr_text)
  ocr_text <- gsub("\n", "", ocr_text)
  ocr_text <- trimws(ocr_text)

  # character-level OCR cleaning
  ocr_text <- .cleanCharacters(ocr_text)

  # fix stray dots in the time portion (e.g. "17:.00:20.152")
  if (nchar(ocr_text) >= 9) {
    time_part <- substr(ocr_text, 9, nchar(ocr_text))
    time_part <- gsub(":\\.", ":", time_part)
    time_part <- gsub("\\.(?=\\d{2}\\.)", ":", time_part, perl = TRUE)
    ocr_text  <- paste0(substr(ocr_text, 1, 8), time_part)
  }

  if (nchar(ocr_text) < 8) return(NA)

  day_str   <- .cleanNumericString(substr(ocr_text, 1, 2))
  month_str <- .cleanMonthString(substr(ocr_text, 3, 5))
  year_str  <- .cleanNumericString(substr(ocr_text, 6, 7))
  time_str  <- .cleanTimeString(substr(ocr_text, 9, nchar(ocr_text)))

  datetime_str <- paste0(day_str, month_str, year_str, " ", time_str)
  datetime_str <- .validateAndCorrectTime(datetime_str, time_offset)

  as.POSIXct(datetime_str, format = "%d%b%y %H:%M:%OS", tz = "UTC")
}


################################################################################
# Character-cleaning helpers for OCR output ###################################
################################################################################

#' @note These functions are intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.cleanCharacters <- function(text) {
  # common OCR misreads of digits (0<->O/Q/D, 1<->I/l/|, 2<->Z, 5<->S, 6<->G/b, 8<->B)
  text <- gsub("[OQD](?=\\d|$)", "0", text, perl = TRUE)
  text <- gsub("[Il|](?=\\d|:)", "1", text, perl = TRUE)
  text <- gsub("Z(?=\\d|:)",     "2", text, perl = TRUE)
  text <- gsub("S(?=\\d|:)",     "5", text, perl = TRUE)
  text <- gsub("[Gb](?=\\d|:)",  "6", text, perl = TRUE)
  text <- gsub("B(?=\\d|:)",     "8", text, perl = TRUE)
  # punctuation misreads
  text <- gsub(",", ".", text, fixed = TRUE)
  text <- gsub(";", ":", text, fixed = TRUE)
  text
}

.cleanNumericString <- function(str) {
  str <- gsub("[BQ]", "0", str)
  str <- gsub("[Il|]", "1", str)
  str <- gsub("Z", "2", str)
  str <- gsub("S", "5", str)
  str <- gsub("[Gb]", "6", str)
  str <- gsub(",", ".", str, fixed = TRUE)
  str <- gsub(" ", "", str, fixed = TRUE)
  str
}

.cleanTimeString <- function(str) {
  str <- .cleanNumericString(str)
  str <- gsub(";", ":", str, fixed = TRUE)
  str
}

.cleanMonthString <- function(str) {
  str <- gsub("0", "O", str, fixed = TRUE)   # 0 should be O in a month abbreviation
  str <- gsub("1", "I", str, fixed = TRUE)   # 1 should be I (rare)
  str <- gsub("S5ep", "Sep", str, fixed = TRUE)
  str
}

.validateAndCorrectTime <- function(datetime_str, expected_offset = 0) {

  parts <- strsplit(datetime_str, " ")[[1]]
  if (length(parts) < 2) return(datetime_str)

  time_components <- strsplit(parts[2], ":")[[1]]
  if (length(time_components) < 2) return(datetime_str)

  hour   <- time_components[1]
  minute <- time_components[2]

  # hours 00-23
  hour_num <- as.numeric(hour)
  if (!is.na(hour_num) && hour_num > 23) {
    if (hour_num >= 26 && hour_num <= 29) hour <- "20"
    else if (hour_num >= 30)              hour <- paste0("0", substr(hour, 2, 2))
  }

  # minutes 00-59
  minute_num <- as.numeric(minute)
  if (!is.na(minute_num) && minute_num >= 60) {
    tens_digit <- substr(minute, 1, 1)
    if (!is.na(as.numeric(tens_digit)) && as.numeric(tens_digit) > 5) minute <- paste0("0", substr(minute, 2, 2))
  }

  # seconds 00-59 (if present)
  if (length(time_components) >= 3) {
    second_part <- time_components[3]
    second_num  <- suppressWarnings(as.numeric(strsplit(second_part, "\\.")[[1]][1]))
    if (!is.na(second_num) && second_num >= 60) {
      tens_digit <- substr(second_part, 1, 1)
      if (!is.na(as.numeric(tens_digit)) && as.numeric(tens_digit) > 5)
        time_components[3] <- paste0("0", substr(second_part, 2, nchar(second_part)))
    }
  }

  time_components[1] <- hour
  time_components[2] <- minute
  parts[2] <- paste(time_components, collapse = ":")
  paste(parts, collapse = " ")
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
