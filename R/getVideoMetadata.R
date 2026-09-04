#######################################################################################################
# Extract recording timestamps and metadata from biologging tag videos ################################
#######################################################################################################

#' Derive video-clock corrections from imported tag metadata
#'
#' @description
#' Builds a small, reviewable correction table from the device and sensor-logging clock metadata retained
#' by [importTagData()]. It does not modify either the sensor data or any video timestamps. Pass the result
#' explicitly to the `clock.corrections` argument of [getVideoMetadata()] to apply it.
#'
#' @param data Imported sensor data in any standard pipeline form: a `nautilus_tag`, a named list of tag
#'   objects, or paths to saved `.rds` objects.
#'
#' @details
#' A correction is derived only for the unambiguous CATS sidecar case: `[logging]` explicitly states that
#' the sensor stream is UTC, the sensor object was imported in a fixed UTC-equivalent time zone, and
#' `[device] utc_offset` is finite and non-zero. The device clock is converted to UTC by adding
#' `-utc_offset * 3600` seconds. Thus a device offset of `-1` produces a video-clock correction of `+3600`
#' seconds.
#'
#' A non-zero device offset without an explicit UTC logging declaration is reported but omitted. The
#' function deliberately does not infer corrections from ambiguous or non-UTC logging metadata.
#'
#' @return A data frame with one row per suggested deployment correction and columns `ID`,
#'   `clock_correction_s`, `clock_correction_source`, `device_utc_offset_h`, and
#'   `logging_utc_offset_h`. A deployment needing no correction is omitted.
#'
#' @seealso [getVideoMetadata()] for applying the returned corrections while extracting video timing;
#'   [tagMetadata()] for inspecting the underlying sidecar provenance.
#'
#' @examples
#' \dontrun{
#' corrections <- getVideoClockCorrections(imported_tags)
#' corrections
#'
#' video_metadata <- getVideoMetadata("./videos", clock.corrections = corrections)
#' }
#' @export

getVideoClockCorrections <- function(data) {
  src <- .resolveInput(data)
  rows <- list()
  ambiguous <- character(0)

  scalar_offset <- function(x) {
    if (length(x) != 1L) return(NA_real_)
    value <- suppressWarnings(as.numeric(x))
    if (length(value) != 1L || !is.finite(value)) NA_real_ else value
  }
  utc_zones <- c("UTC", "GMT", "UCT", "UNIVERSAL", "ZULU", "UTC0", "GMT0",
                 "ETC/UTC", "ETC/GMT", "ETC/UCT", "ETC/UNIVERSAL", "ETC/ZULU")

  for (i in seq_len(src$n)) {
    tag <- src$get(i)
    meta <- .getMeta(tag)
    id <- meta$id %||% src$ids[i]
    if (length(id) != 1L || is.na(id) || !nzchar(as.character(id)))
      .abort("Every item in {.arg data} must have a non-missing deployment ID in its tag metadata.")
    id <- as.character(id)

    sidecar <- meta$sidecar
    if (is.null(sidecar) || !identical(sidecar$source_type, "cats_diary_txt") ||
        is.null(sidecar$device)) next
    device_offset <- scalar_offset(sidecar$device$utc_offset)
    if (!is.finite(device_offset) || abs(device_offset) < sqrt(.Machine$double.eps)) next

    logging_offset <- if (!is.null(sidecar$logging)) scalar_offset(sidecar$logging$utc_offset) else NA_real_
    if (!is.finite(logging_offset) || abs(logging_offset) >= sqrt(.Machine$double.eps)) {
      reason <- if (!is.finite(logging_offset)) "no explicit [logging] UTC declaration" else
        sprintf("[logging] offset is %gh, not UTC", logging_offset)
      ambiguous <- c(ambiguous, sprintf("%s (%s)", id, reason))
      next
    }

    sensor_timezone <- meta$sensors$timezone
    sensor_timezone <- if (length(sensor_timezone) == 1L && !is.na(sensor_timezone))
      toupper(as.character(sensor_timezone)) else NA_character_
    if (is.na(sensor_timezone) || !sensor_timezone %in% utc_zones) {
      label <- if (is.na(sensor_timezone)) "unknown" else sensor_timezone
      ambiguous <- c(ambiguous, sprintf("%s (sensor data use timezone %s, not fixed UTC)", id, label))
      next
    }

    rows[[length(rows) + 1L]] <- data.frame(
      ID = id,
      clock_correction_s = -device_offset * 3600,
      clock_correction_source = "cats_sidecar_device_to_utc",
      device_utc_offset_h = device_offset,
      logging_utc_offset_h = logging_offset,
      stringsAsFactors = FALSE
    )
  }

  if (length(ambiguous)) {
    ambiguous <- unique(ambiguous)
    .warn_grouped(
      "A video-clock correction could not be derived for {length(ambiguous)} deployment{?s}.",
      ambiguous,
      hints = "Automatic correction requires explicit UTC [logging] metadata and sensor data imported in UTC.",
      items.header = "Not corrected:"
    )
  }

  if (!length(rows)) {
    return(data.frame(
      ID = character(0), clock_correction_s = numeric(0),
      clock_correction_source = character(0), device_utc_offset_h = numeric(0),
      logging_utc_offset_h = numeric(0), stringsAsFactors = FALSE
    ))
  }

  out <- do.call(rbind, rows)
  duplicate_ids <- unique(out$ID[duplicated(out$ID)])
  if (length(duplicate_ids))
    .abort("{.arg data} contains more than one applicable clock record for deployment{?s} {.val {duplicate_ids}}.")
  rownames(out) <- NULL
  out
}


#' Read the timing of every camera-tag video file
#'
#' @description
#' Sensor data and video are only comparable once you know, to the second, when each video frame was
#' recorded. Camera tags do not make this easy: the camera keeps its own clock, files are split into
#' segments of arbitrary length, and different camera systems record the start time in different places
#' or not at all.
#'
#' This function builds the bridge. It reads the start time, end time, duration and frame rate of every
#' video in one or more directories and returns one row per file - the table every other video function
#' in the package takes as its map from a timestamp to a file and an offset within it.
#'
#' @param video.folders One or more directories holding video files.
#' @param video.format Which formats to read, `"mp4"` and/or `"mov"`. Default `"mp4"`.
#' @param timestamp.source Where to take each video's start time from:
#'
#'   - `"auto"` (default) uses the file-name timestamp and falls back to reading the screen only for
#'     videos whose name has none.
#'   - `"filename"` uses the file name alone, leaving videos without a timestamp as `NA`.
#'   - `"ocr"` reads every timestamp off the screen, ignoring the file name.
#' @param cross.check Whether to also read the on-screen timestamp for videos whose start time came from
#'   the file name, and compare the two (default `FALSE`). It costs an optical-character-recognition
#'   pass per video but validates the file-name times, which is worth doing once for a new camera system
#'   before trusting them for a whole study. Disagreements beyond two seconds are flagged in `ocr_flag`.
#' @param clock.corrections Optional data frame with one row per deployment and columns `ID` and
#'   `clock_correction_s`, the number of seconds to add to its extracted video timestamps. An optional
#'   `clock_correction_source` column records provenance; it defaults to `"manual"`. Use
#'   [getVideoClockCorrections()] to derive a reviewable table from imported CATS sidecar metadata.
#' @param ocr A control object from [ocrControl()] holding the recognition settings - the model, the
#'   position of the timestamp on screen, and how many frames to search. Only consulted when the screen
#'   is actually read. Pass `ocrControl(...)` to change it.
#' @param use.parallel Whether to process videos in parallel (default `TRUE`). Reading a directory of
#'   videos is limited by disk and decoding rather than by R, so this helps considerably.
#' @param n.cores How many cores to use. `NULL` (default) leaves one free.
#' @param verbose How much detail to print: `0`/`"quiet"`, `1`/`"normal"`, or `2`/`"detailed"`
#'   (default).
#'
#' @details
#' ## Where the start time comes from, and why the file name is preferred
#'
#' Most on-camera systems encode the recording time in the file name, as `YYYYMMDD-HHMMSS` or
#' `YYMMDD-HHMMSS`. That is the primary and default source, because it is exact, costs nothing to read,
#' and does not depend on the camera model, the video quality or where on the frame a clock happens to
#' be drawn.
#'
#' Some cameras write no timestamp into the file name, and for those the start time can be read by
#' optical character recognition from the clock burned into the picture. This is a secondary source: it
#' is slower, and it can misread a digit on a dark or motion-blurred frame. It is used only where the
#' file name has nothing to offer, or, with `cross.check = TRUE`, as an independent check on the file
#' name.
#'
#' Duration and frame rate are always read from the file itself with `ffprobe`, so an FFmpeg
#' installation is needed for every run. The `ffmpeg` binary itself, and the character-recognition
#' packages, are needed only when the screen is actually read.
#'
#' ## Clock corrections
#'
#' File-name and OCR timestamps are clock readings parsed in UTC. When a camera clock was configured at
#' a fixed offset, `clock.corrections` adds the specified number of seconds after extraction and before
#' the video table is returned. Corrections match the exact deployment IDs derived from the video-folder
#' basenames; unknown or duplicate IDs are errors. The original clock reading is recoverable as
#' `start - clock_correction_s`.
#'
#' A correction is applied to `start`, `end`, and, when present, `ocr_start`; the filename-versus-OCR
#' difference in `ocr_offset_s` therefore does not change. A video with no start timestamp remains
#' uncorrected and is reported. Re-running this function starts again from the source files, so
#' corrections cannot accumulate across runs.
#'
#' @return A data frame with one row per video and columns `ID`, `video` (the file name), `start` and
#'   `end`, `duration` in seconds, `frame_rate`, `file` (the full path), and `timestamp_source`, which is
#'   `"filename"`, `"ocr"`, or `NA` where no timestamp could be obtained at all. `clock_correction_s`
#'   records the number of seconds added to the extracted clock (`0` where none was applied), and
#'   `clock_correction_source` records its provenance (`NA` where uncorrected).
#'
#'   With `cross.check = TRUE`, three further columns are added: `ocr_start`, the time read from the
#'   screen; `ocr_offset_s`, the file-name time minus that; and `ocr_flag`, which is `TRUE` where the two
#'   disagree by more than two seconds.
#'
#' @seealso [getVideoClockCorrections()] for deriving corrections from imported tag metadata;
#'   [ocrControl()] for the recognition settings; [launchVideo()] and [filterVideoPeriod()] for what
#'   consumes this table; [renderOverlayVideo()] for compositing footage with sensor data.
#'
#' @examples
#' \dontrun{
#' # one row per video, start times taken from the file names where present
#' meta <- getVideoMetadata(c("./videos/PIN_CAM_01", "./videos/PIN_CAM_02"))
#'
#' # validate those file-name timestamps against the clock burned into the picture
#' meta <- getVideoMetadata("./videos/PIN_CAM_01", cross.check = TRUE)
#' subset(meta, ocr_flag)
#'
#' # Explicit manual correction: add one hour to this deployment's camera clock.
#' corrections <- data.frame(ID = "PIN_CAM_31", clock_correction_s = 3600)
#' meta <- getVideoMetadata("./videos/PIN_CAM_31", clock.corrections = corrections)
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

  # Validate the whole correction plan before invoking ffprobe/OCR, including exact matching to the
  # deployment IDs derived from the supplied folder names. A typo must fail before expensive work starts.
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
                                n_animals, if (n_animals != 1) "s" else ""),
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


################################################################################
# Video-clock correction contract #############################################
################################################################################

#' Validate and canonicalise a video-clock correction table
#' @keywords internal
#' @noRd

.validateVideoClockCorrections <- function(corrections, video.ids = NULL) {
  empty <- data.frame(ID = character(0), clock_correction_s = numeric(0),
                      clock_correction_source = character(0), stringsAsFactors = FALSE)
  if (is.null(corrections)) return(empty)
  if (!is.data.frame(corrections))
    .abort("{.arg clock.corrections} must be a data frame, or {.code NULL}.")

  .assert_columns(corrections, c("ID", "clock_correction_s"), "clock.corrections")
  if (!nrow(corrections)) return(empty)

  ids <- as.character(corrections$ID)
  bad_ids <- is.na(ids) | !nzchar(ids)
  if (any(bad_ids))
    .abort("{.field ID} in {.arg clock.corrections} must contain non-missing deployment identifiers.")
  duplicate_ids <- unique(ids[duplicated(ids)])
  if (length(duplicate_ids))
    .abort("{.arg clock.corrections} has more than one row for deployment{?s} {.val {duplicate_ids}}.")

  shifts <- corrections$clock_correction_s
  if (!is.numeric(shifts) || any(!is.finite(shifts)))
    .abort("{.field clock_correction_s} in {.arg clock.corrections} must contain finite numeric seconds.")

  if ("clock_correction_source" %in% names(corrections)) {
    source <- corrections$clock_correction_source
    if (is.factor(source)) source <- as.character(source)
    if (!is.character(source))
      .abort("{.field clock_correction_source} in {.arg clock.corrections} must be character.")
    source[is.na(source) | !nzchar(source)] <- "manual"
  } else {
    source <- rep("manual", length(ids))
  }

  if (!is.null(video.ids)) {
    unmatched <- setdiff(ids, as.character(video.ids))
    if (length(unmatched))
      .abort(c("Some {.arg clock.corrections} IDs do not match the supplied video folders: {.val {unmatched}}.",
               "i" = "Correction IDs must exactly match the video-folder basenames."))
  }

  data.frame(ID = ids, clock_correction_s = as.numeric(shifts),
             clock_correction_source = source, stringsAsFactors = FALSE)
}


#' Apply an already-validated video-clock correction plan
#'
#' Corrections are transactional with respect to existing provenance: every target is checked before any
#' timestamp is changed, and a non-zero prior correction aborts the whole operation. Rows with no start
#' timestamp cannot be changed and are reported; all other rows for that deployment are still corrected.
#' @keywords internal
#' @noRd

.applyVideoClockCorrections <- function(video.metadata, corrections) {
  .assert_columns(video.metadata, c("ID", "start", "end", "duration"), "video.metadata")
  if (!inherits(video.metadata$start, "POSIXct") || !inherits(video.metadata$end, "POSIXct"))
    .abort("Columns {.field start} and {.field end} in {.arg video.metadata} must be POSIXct.")
  if (!is.numeric(video.metadata$duration))
    .abort("Column {.field duration} in {.arg video.metadata} must contain numeric seconds.")
  if ("ocr_start" %in% names(video.metadata) && !inherits(video.metadata$ocr_start, "POSIXct"))
    .abort("Column {.field ocr_start} in {.arg video.metadata} must be POSIXct.")

  if (!"clock_correction_s" %in% names(video.metadata)) {
    video.metadata$clock_correction_s <- rep(0, nrow(video.metadata))
  } else if (!is.numeric(video.metadata$clock_correction_s) ||
             any(!is.finite(video.metadata$clock_correction_s))) {
    .abort("Existing {.field clock_correction_s} values in {.arg video.metadata} must be finite numeric seconds.")
  }
  if (!"clock_correction_source" %in% names(video.metadata)) {
    video.metadata$clock_correction_source <- rep(NA_character_, nrow(video.metadata))
  } else {
    if (is.factor(video.metadata$clock_correction_source))
      video.metadata$clock_correction_source <- as.character(video.metadata$clock_correction_source)
    if (!is.character(video.metadata$clock_correction_source))
      .abort("Existing {.field clock_correction_source} values in {.arg video.metadata} must be character.")
  }

  if (!nrow(corrections)) return(video.metadata)

  plan_row <- match(as.character(video.metadata$ID), corrections$ID)
  targeted <- !is.na(plan_row)
  shift <- rep(0, nrow(video.metadata))
  source <- rep(NA_character_, nrow(video.metadata))
  shift[targeted] <- corrections$clock_correction_s[plan_row[targeted]]
  source[targeted] <- corrections$clock_correction_source[plan_row[targeted]]
  targeted <- targeted & shift != 0

  already <- targeted & video.metadata$clock_correction_s != 0
  if (any(already)) {
    ids <- unique(as.character(video.metadata$ID[already]))
    .abort("Clock correction already applied to deployment{?s} {.val {ids}}. Refusing to apply another correction.")
  }

  missing_start <- targeted & is.na(video.metadata$start)
  if (any(missing_start)) {
    ids <- unique(as.character(video.metadata$ID[missing_start]))
    warning(sprintf("Clock correction was not applied to %d video%s without a start timestamp (%s).",
                    sum(missing_start), if (sum(missing_start) != 1L) "s" else "",
                    paste(ids, collapse = ", ")), call. = FALSE)
  }

  apply <- targeted & !missing_start
  if (!any(apply)) return(video.metadata)

  video.metadata$start[apply] <- video.metadata$start[apply] + shift[apply]
  video.metadata$end[apply] <- video.metadata$start[apply] + video.metadata$duration[apply]
  if ("ocr_start" %in% names(video.metadata)) {
    have_ocr <- apply & !is.na(video.metadata$ocr_start)
    video.metadata$ocr_start[have_ocr] <- video.metadata$ocr_start[have_ocr] + shift[have_ocr]
  }
  video.metadata$clock_correction_s[apply] <- shift[apply]
  video.metadata$clock_correction_source[apply] <- source[apply]
  video.metadata
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

#' Download the camera-tag timestamp-recognition model
#'
#' @description
#' The model that reads camera-tag on-screen timestamps is about 11 MB, which is too large to bundle
#' with the package, so it is downloaded on first use and cached thereafter.
#'
#' [getVideoMetadata()] fetches it automatically the first time it actually has to read a timestamp off
#' the picture. Call this to fetch it in advance - before working offline, or before a long batch run
#' where an unexpected download would be unwelcome.
#'
#' @param quiet Whether to suppress the progress messages. Default `FALSE`.
#'
#' @details
#' The model is cached in the per-user cache directory, `tools::R_user_dir("nautilus", "cache")`, and
#' reused from there, so the download happens once per machine rather than once per session. Calling
#' this when the model is already cached does nothing but confirm it.
#'
#' @return The path to the cached model, invisibly, or `NULL` where it could not be downloaded. In that
#'   case timestamp recognition falls back to Tesseract's generic English model, which still works but
#'   is noticeably less accurate on the camera-tag timestamp font.
#'
#' @seealso [getVideoMetadata()] for what uses the model; [ocrControl()] for selecting a different one.
#'
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
#' [installCamOcrModel()]) and installed into the tesseract data directory automatically (no
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
