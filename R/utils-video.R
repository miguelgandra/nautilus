

#' Convert an FFmpeg `HH:MM:SS.microseconds` progress value to seconds.
#' @keywords internal
#' @noRd
.ffmpegProgressSeconds <- function(x) {
  fields <- strsplit(x, ":", fixed = TRUE)[[1]]
  if (length(fields) != 3L) return(NA_real_)
  values <- suppressWarnings(as.numeric(fields))
  if (any(!is.finite(values))) return(NA_real_)
  values[1] * 3600 + values[2] * 60 + values[3]
}


#' Run FFmpeg while translating its machine-readable time output into a cli progress bar.
#'
#' A pipe keeps this synchronous and dependency-free while allowing FFmpeg to stream `-progress`
#' records. Diagnostics share the pipe and are ignored; the connection's close status remains the
#' authoritative process result used by the caller's existing software-encoder fallback.
#' @keywords internal
#' @noRd
.runFfmpegWithProgress <- function(ffmpeg, args, duration, lvl) {
  progress_args <- c("-progress", "pipe:1", "-nostats", args)
  command <- paste(shQuote(c(ffmpeg, progress_args)), collapse = " ")
  command <- paste(command, "2>&1")
  con <- base::pipe(command, open = "r")
  open <- TRUE
  pb <- if (lvl >= 1L) cli::cli_progress_bar(
    format = "{cli::pb_spin} Encoding video {cli::pb_bar} {cli::pb_percent}",
    total = 100L, .envir = parent.frame()) else NULL
  on.exit({
    if (open) suppressWarnings(close(con))
    .log_progress_done(pb)
  }, add = TRUE)

  repeat {
    line <- readLines(con, n = 1L, warn = FALSE)
    if (!length(line)) break
    if (startsWith(line, "out_time=")) {
      seconds <- .ffmpegProgressSeconds(sub("^out_time=", "", line))
      if (!is.null(pb) && is.finite(seconds) && is.finite(duration) && duration > 0)
        cli::cli_progress_update(id = pb, set = min(99L, as.integer(floor(100 * seconds / duration))))
    } else if (identical(line, "progress=end") && !is.null(pb)) {
      cli::cli_progress_update(id = pb, set = 100L)
    }
  }
  status <- suppressWarnings(close(con))
  open <- FALSE
  .log_progress_done(pb)
  pb <- NULL

  # `pipe()` returns the wait status shifted by eight bits on Unix, but a direct exit code on some
  # other platforms. Normalise only the unambiguous shifted form.
  status <- as.integer(status)
  if (length(status) != 1L || !is.finite(status)) return(1L)
  if (is.finite(status) && status > 255L && status %% 256L == 0L) status <- status %/% 256L
  status
}


#' Locate the ffmpeg binary (cross-platform), or abort with guidance.
#' @keywords internal
#' @noRd
.ffmpegBin <- function() {
  bin <- Sys.which("ffmpeg")
  if (!nzchar(bin)) .abort(c("FFmpeg was not found on the system path.", "i" = "Install FFmpeg to use {.fn renderOverlayVideo}."))
  unname(bin)
}


#' Locate the `ffprobe` binary (installed alongside FFmpeg), or abort with guidance.
#' @keywords internal
#' @noRd
.ffprobeBin <- function() {
  bin <- Sys.which("ffprobe")
  if (!nzchar(bin)) .abort(c("ffprobe was not found on the system path.", "i" = "Install FFmpeg (it provides ffprobe) to read video metadata."))
  unname(bin)
}


#' Does the local ffmpeg expose a given encoder?
#' @keywords internal
#' @noRd
.hasEncoder <- function(name) {
  enc <- tryCatch(system2(.ffmpegBin(), c("-hide_banner", "-encoders"), stdout = TRUE, stderr = FALSE), error = function(e) "")
  any(grepl(name, enc, fixed = TRUE))
}

#' Resolve video search folders to deployment identifiers
#'
#' An unnamed character vector follows the convenient deployment-root convention (`basename(path)` is
#' the ID). A fully named vector is the explicit escape hatch for arbitrary media layouts: its names are
#' the IDs, so `c(PIN_CAM_31 = ".../PIN_CAM_31/MP4")` never has to infer identity from path depth.
#' Mixing named and unnamed paths is rejected because a silent mixture of explicit and inferred IDs is
#' difficult to audit and especially dangerous when clock corrections are keyed by deployment.
#' @keywords internal
#' @noRd
.resolveVideoFolders <- function(video.folders) {
  if (!is.character(video.folders) || !length(video.folders))
    .abort("{.arg video.folders} must be a character vector.")
  if (anyNA(video.folders) || any(!nzchar(video.folders)))
    .abort("{.arg video.folders} must contain non-missing, non-empty folder paths.")

  supplied_names <- names(video.folders)
  named <- if (is.null(supplied_names)) rep(FALSE, length(video.folders)) else
    !is.na(supplied_names) & nzchar(supplied_names)
  if (any(named) && !all(named)) {
    .abort(c("{.arg video.folders} must be either entirely unnamed or have a deployment ID name for every folder.",
             "i" = "Use a fully named vector such as {.code c(PIN_CAM_31 = '/path/to/PIN_CAM_31/MP4')}."))
  }

  ids <- if (all(named)) supplied_names else basename(video.folders)
  if (anyNA(ids) || any(!nzchar(ids)))
    .abort("Could not derive a non-empty deployment ID for every {.arg video.folders} path.")

  data.frame(ID = as.character(ids), folder = unname(video.folders), stringsAsFactors = FALSE)
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
               "i" = "Correction IDs must exactly match the resolved deployment IDs ({.arg video.folders} names when supplied, otherwise folder basenames)."))
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

# Video interval selection, playback and review-frame helpers.

# filterVideoPeriod helpers ----

#' Logical mask: which of `t` fall within ANY [starts, ends] interval.
#' @keywords internal
#' @noRd
.inAnyInterval <- function(t, starts, ends) {
  # Union windows once, then binary-search samples instead of scanning a high-rate record per event.
  m <- logical(length(t)); a <- as.numeric(starts); b <- as.numeric(ends)
  good <- is.finite(a) & is.finite(b) & a <= b
  if (!any(good)) return(m)
  a <- a[good]; b <- b[good]; ord <- order(a, b); a <- a[ord]; b <- cummax(b[ord])
  begin <- c(TRUE, a[-1] > b[-length(b)])
  finish <- c(which(begin)[-1] - 1L, length(b))
  b <- b[finish]; a <- a[begin]
  time <- as.numeric(t); valid <- is.finite(time)
  k <- findInterval(time[valid], a)
  m[valid] <- k > 0L & time[valid] <= b[pmax(1L, k)]
  m[!valid] <- NA
  m
}

#######################################################################################################
#######################################################################################################
#######################################################################################################

# launchVideo helpers ----

#' Locate the VLC executable: the system path first, then the usual install locations.
#'
#' Hardcoding one absolute path per OS - as this did - fails on entirely ordinary installs: 32-bit VLC
#' under Windows' "Program Files (x86)", per-user Windows installs, and the Snap or Homebrew builds that
#' are the normal channel on several Linux distributions. Asking the system where the binary is mirrors
#' `.ffmpegBin()` / `.ffprobeBin()`, which already solve this same problem for FFmpeg.
#' @param path Optional user-supplied path; returned unchanged once confirmed to exist.
#' @keywords internal
#' @noRd
.vlcBin <- function(path = NULL) {
  if (!is.null(path)) {
    if (!file.exists(path))
      .abort(c("VLC was not found at {.file {path}}.", "i" = "Check {.arg vlc.path}, or leave it {.code NULL} to search automatically."))
    return(path)
  }
  bin <- Sys.which(if (.Platform$OS.type == "windows") "vlc.exe" else "vlc")
  if (nzchar(bin)) return(unname(bin))

  candidates <- switch(Sys.info()[["sysname"]],
    Darwin  = c("/Applications/VLC.app/Contents/MacOS/VLC",
                path.expand("~/Applications/VLC.app/Contents/MacOS/VLC")),
    Windows = c("C:/Program Files/VideoLAN/VLC/vlc.exe",
                "C:/Program Files (x86)/VideoLAN/VLC/vlc.exe",
                file.path(Sys.getenv("LOCALAPPDATA"), "Programs/VideoLAN/VLC/vlc.exe")),
              c("/usr/bin/vlc", "/usr/local/bin/vlc", "/snap/bin/vlc", "/var/lib/flatpak/exports/bin/org.videolan.VLC"))
  hit <- candidates[file.exists(candidates)]
  if (length(hit)) return(hit[1])
  .abort(c("VLC was not found on the system path or in the usual install locations.",
           "i" = "Install VLC, or pass its full path via {.arg vlc.path}."))
}

#' Close any running VLC instance (best-effort, cross-platform).
#'
#' Matched on the process NAME, not the full command line: `pkill -f vlc` matches any process whose
#' arguments merely contain "vlc" - an editor holding this file open, or a script with "vlc" in its
#' path - and would kill it. The Windows branch was already name-scoped via `/IM`; the Unix branches
#' now match it.
#' @keywords internal
#' @noRd
.closeVLC <- function() {
  switch(Sys.info()[["sysname"]],
         Darwin  = system2("pkill", c("-x", "VLC"), stdout = FALSE, stderr = FALSE),
         Windows = system2("taskkill", c("/F", "/IM", "vlc.exe"), stdout = FALSE, stderr = FALSE),
         system2("pkill", c("-x", "vlc"), stdout = FALSE, stderr = FALSE))
  invisible(NULL)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################

# saveUncertainTimestampFrames helpers ----

#' Extract, save and OCR one on-screen timestamp frame for manual review
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.saveReviewFrame <- function(row, reason, ocr, ocr_engine, output.dir, ffmpeg_bin, overwrite = FALSE) {

  video      <- row$file
  id         <- row$ID
  video_name <- tools::file_path_sans_ext(basename(video))
  tag        <- gsub("[^A-Za-z0-9]+", "-", reason)

  processed_path <- file.path(output.dir, sprintf("%s_%s_%s_processed.jpg", id, video_name, tag))
  original_path  <- file.path(output.dir, sprintf("%s_%s_%s_original.jpg",  id, video_name, tag))
  metadata_path  <- file.path(output.dir, sprintf("%s_%s_%s_report.txt",    id, video_name, tag))

  # honour overwrite = FALSE (skip work if the processed frame already exists)
  if (!overwrite && file.exists(processed_path))
    return(list(processed_path = processed_path, original_path = original_path, metadata_path = metadata_path))

  # extract the first frame (normalised to the reference height so the box coordinates apply)
  temp_frame_path <- file.path(tempdir(), sprintf("%s-review-frame.jpg", id))
  system2(ffmpeg_bin, c("-y", "-ss", "0", "-i", shQuote(video), "-vframes", "1", "-q:v", "1",
                        "-vf", sprintf("scale=-1:%d", ocr$frame.height), shQuote(temp_frame_path)),
          stdout = FALSE, stderr = FALSE)
  if (!file.exists(temp_frame_path))
    return(list(processed_path = NA_character_, original_path = NA_character_, metadata_path = NA_character_))

  # crop the configured timestamp box; save the raw crop
  input_frame   <- magick::image_read(temp_frame_path)
  crop_geometry <- sprintf("%dx%d+%d+%d", ocr$box[3], ocr$box[4], ocr$box[1], ocr$box[2])
  cropped_image <- magick::image_crop(input_frame, geometry = crop_geometry)
  magick::image_write(cropped_image, path = original_path, format = "jpg", quality = 95)

  # same preprocessing pipeline as getVideoMetadata(); save the processed image
  processed_image <- magick::image_convert(cropped_image, colorspace = "gray")
  processed_image <- magick::image_negate(processed_image)
  processed_image <- magick::image_contrast(processed_image, sharpen = 1)
  processed_image <- magick::image_resize(processed_image, geometry = "300%")
  processed_image <- magick::image_morphology(processed_image, method = "Close", kernel = "Diamond", iterations = 1)
  processed_image <- magick::image_morphology(processed_image, method = "Open", kernel = "Disk", iterations = 1)
  processed_image <- magick::image_threshold(processed_image, type = "white", threshold = "70%")
  processed_image <- magick::image_threshold(processed_image, type = "black", threshold = "60%")
  magick::image_write(processed_image, path = processed_path, format = "jpg", quality = 95)

  # what OCR reads from this frame (recorded in the report)
  ocr_text <- invisible(tesseract::ocr(processed_image, engine = ocr_engine))

  offset_line <- if (!is.null(row$ocr_offset_s) && !is.na(row$ocr_offset_s))
    sprintf("OCR offset (start - OCR): %.1f s\n", row$ocr_offset_s) else ""

  metadata_content <- paste0(
    "=== UNCERTAIN TIMESTAMP - MANUAL REVIEW ===\n",
    "Video: ", basename(video), "\n",
    "ID: ", id, "\n",
    "Reason flagged: ", reason, "\n",
    "Timestamp source: ", ifelse(is.na(row$timestamp_source), "none", row$timestamp_source), "\n",
    "Recorded start: ", ifelse(is.na(row$start), "FAILED", as.character(row$start)), "\n",
    offset_line,
    "Raw OCR text: '", trimws(ocr_text), "'\n",
    "Generated: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n",
    "=== HOW TO REVIEW ===\n",
    "1. Open the *_original.jpg crop to read the on-screen timestamp directly.\n",
    "2. Compare it with the *_processed.jpg to see what OCR 'saw'.\n",
    "3. If the recorded start is wrong, note the correct value.\n",
    "   Common OCR issues: 0/O, 1/I, 5/S confusion, poor contrast.\n")
  writeLines(metadata_content, metadata_path)

  unlink(temp_frame_path)
  list(processed_path = processed_path, original_path = original_path, metadata_path = metadata_path)
}



#######################################################################################################
# Video file-name timestamp parser ###################################################################
#######################################################################################################

#' Parse a recording start time from a video file name (UTC)
#'
#' Extracts the deployment/recording start time encoded in a camera video file name. Two on-camera
#' naming conventions are recognised, tried most-specific first:
#' \itemize{
#'   \item `YYYYMMDD-HHMMSS` (4-digit year), e.g. `CameraCMD71_Spot06-20190831-173658-819-00023.mp4`
#'   \item `YYMMDD-HHMMSS` (2-digit year), e.g. `230831-161949_CAM0bc99448_30.mp4`
#' }
#' The parsed instant is sanity-checked against a plausible recording window (years 2000-2100) so that
#' an unrelated run of digits in a camera name cannot be mistaken for a timestamp. File names without a
#' recognisable timestamp (e.g. `MOBIUS0000028_processed.mp4`) return `NULL`, signalling to the caller
#' that another source (OCR, or externally supplied metadata) is required.
#'
#' @param path A video file path or bare file name.
#' @return A length-one `POSIXct` (UTC), or `NULL` if no plausible timestamp is present.
#' @keywords internal
#' @noRd
.videoStartFromFilename <- function(path) {
  fn <- basename(path)
  patterns <- list(
    c(rx = "([0-9]{8})-([0-9]{6})", fmt = "%Y%m%d-%H%M%S"),   # full-year form, tried first
    c(rx = "([0-9]{6})-([0-9]{6})", fmt = "%y%m%d-%H%M%S")    # 2-digit-year form
  )
  lower <- as.POSIXct("2000-01-01", tz = "UTC")
  upper <- as.POSIXct("2100-01-01", tz = "UTC")
  for (p in patterns) {
    m <- regmatches(fn, regexpr(p[["rx"]], fn))
    if (!length(m)) next
    ts <- as.POSIXct(m, format = p[["fmt"]], tz = "UTC")
    if (!is.na(ts) && ts >= lower && ts <= upper) return(ts)
  }
  NULL
}
