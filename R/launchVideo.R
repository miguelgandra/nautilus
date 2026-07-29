#######################################################################################################
# Open camera-tag video at a given datetime ###########################################################
#######################################################################################################

#' Open camera-tag video at a given datetime
#'
#' @description
#' Finds the video segment for an individual that contains a given datetime and opens it in VLC, seeked
#' to that instant - convenient for jumping straight to a moment of interest (e.g. a validation segment
#' from \link{findValidationSegments}). By default any running VLC instance is closed first.
#'
#' @param id Character/factor scalar: the individual whose video to open (matched in `video.metadata`).
#' @param datetime POSIXct: the instant to seek to.
#' @param video.metadata A data.frame of video segments with columns `ID`, `start`, `end`, `video` and
#'   `file`, as returned by \link{getVideoMetadata}.
#' @param vlc.path Optional path to the VLC executable. If `NULL` (default), VLC is looked up on the
#'   system path and then in the usual per-OS install locations.
#' @param close.existing Logical. Close any running VLC instance first. Default `TRUE`.
#'
#' @return `TRUE` invisibly once VLC has been launched; `FALSE` if no video segment covers
#'   `datetime`. Invalid arguments, a missing VLC executable and a missing video file all raise an
#'   error rather than returning `FALSE`.
#' @seealso \link{getVideoMetadata}, \link{findValidationSegments}.
#' @examples
#' \dontrun{
#' meta <- getVideoMetadata("./videos/PIN_CAM_01")
#' # jump straight to a moment of interest in VLC
#' launchVideo("PIN_CAM_01", as.POSIXct("2019-08-31 17:40:00", tz = "UTC"), meta)
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
