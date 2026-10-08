#######################################################################################################
# Open camera-tag video at a given datetime ###########################################################
#######################################################################################################

#' Open camera-tag footage at a sensor timestamp
#'
#' @description
#' Locates the video segment covering a deployment timestamp and opens it in VLC at the corresponding
#' elapsed time. This supports visual examination of sensor-derived events, movement manoeuvres and
#' candidate behaviours without manually searching and navigating individual recording files.
#'
#' Segment timing and file paths are supplied through the table returned by [getVideoMetadata()]. The
#' function does not estimate clock corrections, annotate footage or render a sensor overlay; it opens
#' the original video using the timing information already present in that table.
#'
#' @param id Single non-empty character or factor value identifying the deployment. Matched exactly
#'   against the `ID` column of `video.metadata`; an identifier absent from that table raises an error.
#' @param datetime Single non-missing `POSIXct` timestamp of the moment to examine, on the same time
#'   base as the segment timestamps. Explicitly specifying the time zone when constructing this value
#'   is recommended; see Details.
#' @param video.metadata Data frame of video segments, as returned by [getVideoMetadata()], containing
#'   `ID` (deployment identifier), `start` and `end` (`POSIXct` recording timestamps), `video` (file
#'   label) and `file` (path to the video). Additional timing or clock-provenance columns are allowed
#'   but are not used by this function. Rows with missing file paths cannot be selected.
#' @param vlc.path Path to the VLC executable, or `NULL` (default) to search the system path followed
#'   by the usual macOS, Windows or Linux installation locations. Use an explicit path for an
#'   installation not found automatically. VLC must be installed separately from \pkg{nautilus}.
#' @param close.existing Logical; whether to attempt to close all running VLC instances before opening
#'   the selected segment (default `TRUE`). This can interrupt unrelated playback. Set `FALSE` to leave
#'   existing instances running; subsequent window behaviour depends on the user's VLC configuration.
#'
#' @details
#' ## Workflow and clock alignment
#'
#' First extract segment metadata with [getVideoMetadata()]. If the camera and sensor clocks differ,
#' review corrections from [getVideoClockCorrections()] or a manually constructed correction table and
#' pass them to `getVideoMetadata(clock.corrections = ...)` before using this function. `launchVideo()`
#' uses the resulting `start` and `end` values directly and does not apply their recorded corrections
#' again. An incorrectly aligned clock can open the wrong segment or seek to the wrong moment even when
#' a segment matches successfully.
#'
#' Comparisons use the absolute instants represented by the timestamps, not their displayed local clock
#' labels. Different display time zones are compatible when they represent the same instant; incorrectly
#' assigning a time zone to a recorded clock time is not corrected here. UTC is recommended for workflows
#' that align sensor data and footage.
#'
#' ## Segment selection and seeking
#'
#' Segment start, end and requested timestamps are floored to whole seconds for matching. Both
#' boundaries are inclusive. If several segments cover the same second, including a shared boundary
#' between adjacent files, the first matching row in `video.metadata` is selected. The function does
#' not sort overlapping records or choose between them using timing-quality flags.
#'
#' The VLC start offset is the whole-second floor of the elapsed time from the selected segment's
#' `start` to `datetime`. This is a segment-level navigation aid, not a frame-accurate synchronisation
#' procedure. VLC is launched asynchronously and playback is not monitored.
#'
#' If no usable segment covers the requested timestamp, an informative console message is printed and
#' `FALSE` is returned invisibly, without looking up VLC or closing a running instance. When a segment
#' matches, the function resolves VLC and checks that the selected file still exists. A stale path,
#' for example after moving footage or unmounting a drive, raises an error rather than falling back to
#' another overlapping segment.
#'
#' @return A logical value, invisibly: `TRUE` after issuing the asynchronous VLC launch command, or
#'   `FALSE` when the deployment is present but no usable segment covers `datetime`. `TRUE` does not
#'   confirm successful playback. Invalid arguments, an unknown deployment, an unavailable VLC
#'   executable or a missing selected video file raise an error. The segment table is not modified and
#'   no output file is written.
#'
#' @seealso [getVideoMetadata()] for segment timing and file discovery; [getVideoClockCorrections()] for
#'   reviewable clock corrections; [findValidationSegments()] for candidate review intervals;
#'   [renderOverlayVideo()] for compositing sensor visualisations; [annotateData()] for joining
#'   behavioural annotations to sensor data.
#'
#' @examples
#' \dontrun{
#' # VLC and the source videos must be available locally
#' video_metadata <- getVideoMetadata(
#'   c(deployment_01 = "./videos/deployment_01/MP4")
#' )
#' moment <- as.POSIXct("2023-08-31 17:40:00", tz = "UTC")
#' opened <- launchVideo(
#'   "deployment_01", moment, video_metadata, close.existing = FALSE
#' )
#'
#' # Align the video clock explicitly where imported metadata support a correction
#' corrections <- getVideoClockCorrections(imported_tags)
#' video_metadata <- getVideoMetadata(camera_folders, clock.corrections = corrections)
#' launchVideo("deployment_01", moment, video_metadata, close.existing = FALSE)
#' }
#' @export

launchVideo <- function(id,
                        datetime,
                        video.metadata,
                        vlc.path = NULL,
                        close.existing = TRUE) {

  # validate inputs
  if (missing(id) || !(is.character(id) || is.factor(id)) || length(id) != 1 || is.na(id) || !nzchar(as.character(id)))
    .abort("{.arg id} must be a single non-empty character/factor value.")
  id <- as.character(id)
  if (!inherits(datetime, "POSIXct")) .abort("{.arg datetime} must be POSIXct.")
  .assert_flag(close.existing, "close.existing")
  if (!is.null(vlc.path)) .assert_string(vlc.path, "vlc.path")
  .assert_columns(video.metadata, c("ID", "start", "end", "video", "file"), "video.metadata")
  if (!id %in% video.metadata$ID) .abort("{.arg id} {.val {id}} is not present in {.arg video.metadata}.")

  # find the video segment that contains `datetime` (compared at whole-second resolution)
  fsec <- function(x) as.POSIXct(floor(as.numeric(x)), origin = "1970-01-01", tz = "UTC")
  vm <- video.metadata[video.metadata$ID == id & !is.na(video.metadata$file), ]
  match_i <- which(fsec(vm$start) <= fsec(datetime) & fsec(vm$end) >= fsec(datetime))

  if (!length(match_i)) {
    starts <- vm$start
    msg <- if (all(is.na(starts))) "no usable video segments for this individual."
           else if (datetime > max(starts, na.rm = TRUE)) "the datetime is later than the last available video."
           else if (datetime < min(starts, na.rm = TRUE)) "the datetime is earlier than the first available video."
           else "no video segment covers the datetime (check the id, datetime and metadata)."
    cli::cli_alert_warning(msg)
    return(invisible(FALSE))
  }

  # resolve the VLC executable (only needed once a matching segment is found)
  vlc.path <- .vlcBin(vlc.path)

  # the metadata is built once by getVideoMetadata() and often reused later, so a path can go stale
  # (drive unmounted, footage archived). Without this the function reports success and opens nothing.
  if (!file.exists(hit_file <- as.character(vm$file[match_i[1]])))
    .abort(c("The video file for this segment was not found: {.file {hit_file}}.",
             "i" = "The paths in {.arg video.metadata} may be stale - re-run {.fn getVideoMetadata}."))

  hit <- vm[match_i[1], ]
  if (close.existing) {
    cli::cli_alert_info("Closing any running VLC instance")
    .closeVLC()
    Sys.sleep(0.5)   # VLC releases its single-instance lock asynchronously; without a pause the new
  }                  # process can be swallowed by the one still shutting down
  skip_secs <- floor(as.numeric(difftime(datetime, hit$start, units = "secs")))
  cli::cli_alert_info("Opening {.file {hit$video}} at +{skip_secs}s")
  # shQuote is required here: system2() quotes only `command`, never the `args` vector, so a path
  # containing a space would otherwise arrive at VLC split across several arguments.
  system2(vlc.path, c(sprintf("--start-time=%d", skip_secs), "--quiet", shQuote(hit_file)),
          wait = FALSE, stdout = FALSE, stderr = FALSE)
  invisible(TRUE)
}


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
