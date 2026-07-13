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
#' @param vlc.path Optional path to the VLC executable. If `NULL`, an OS-specific default is used.
#' @param close.existing Logical. Close any running VLC instance first. Default `TRUE`.
#'
#' @return `TRUE` invisibly if a video was opened, otherwise `FALSE`.
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
  if (is.null(vlc.path)) {
    vlc.path <- switch(Sys.info()[["sysname"]],
                       Darwin  = "/Applications/VLC.app/Contents/MacOS/VLC",
                       Windows = "C:/Program Files/VideoLAN/VLC/vlc.exe",
                       "/usr/bin/vlc")
  }
  if (!file.exists(vlc.path))
    .abort(c("VLC was not found at {.file {vlc.path}}.", "i" = "Install VLC or pass {.arg vlc.path}."))

  if (close.existing) { .closeVLC(); Sys.sleep(0.5) }
  hit <- vm[match_i[1], ]
  skip_secs <- floor(as.numeric(difftime(datetime, hit$start, units = "secs")))
  cli::cli_alert_info("Opening {.file {hit$video}} at +{skip_secs}s")
  system2(vlc.path, c(sprintf("--start-time=%d", skip_secs), "--quiet", shQuote(hit$file)),
          wait = FALSE, stdout = FALSE, stderr = FALSE)
  invisible(TRUE)
}


#' Close any running VLC instance (best-effort, cross-platform).
#' @keywords internal
#' @noRd
.closeVLC <- function() {
  switch(Sys.info()[["sysname"]],
         Darwin  = system2("pkill", c("-f", "VLC"), stdout = FALSE, stderr = FALSE),
         Windows = system2("taskkill", c("/F", "/IM", "vlc.exe"), stdout = FALSE, stderr = FALSE),
         system2("pkill", c("-f", "vlc"), stdout = FALSE, stderr = FALSE))
  invisible(NULL)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
