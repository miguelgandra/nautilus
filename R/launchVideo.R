#######################################################################################################
# Function to open a video at a specific datetime based on metadata ###################################
#######################################################################################################

#' This function searches for the video file that contains the given datetime
#' within its start and end times. If a matching video is found, it opens
#' the video using VLC at the time corresponding to the given datetime.
#'
#' @param id A \code{character} or \code{factor} representing the unique ID
#' for the animal associated with the video. This ID is used to search for the
#' corresponding video entry in the metadata.
#' @param datetime A \code{POSIXct} object representing the datetime to find
#' within the video metadata.
#' @param video.metadata A \code{data.frame} containing the metadata for each
#' video, as returned by \code{getVideoMetadata}.
#' @param vlc.path (Optional) The full path to the VLC application. If not
#' provided, a default path will be used based on the user's operating system.
#'
#' @return The function opens the corresponding video at the specified datetime
#' using VLC. If no video matches the datetime, an appropriate message is printed.
#' @export


launchVideo <- function(id, datetime, video.metadata, vlc.path=NULL){

  # automatically detect VLC path if not provided
  if(is.null(vlc.path)) {
    # determine the operating system and assign the default VLC path accordingly
    os_type <- Sys.info()["sysname"]
    if(os_type == "Darwin") vlc.path <- "/Applications/VLC.app/Contents/MacOS/VLC"
    else if(os_type == "Windows") vlc.path <- "C:/Program Files/VideoLAN/VLC/vlc.exe"
    else vlc.path <- "/usr/bin/vlc"
    # ensure VLC path exists
    if (!file.exists(vlc.path)) stop("VLC could not be found on your system. Please install it or specify the correct path using 'vlc.path'.", call. = FALSE)
  } else {
    # if the user provided a VLC path, ensure it exists
    if (!file.exists(vlc.path)) stop("Hmm, looks like the VLC path you provided doesn't exist. Please double-check it and try again.", call. = FALSE)
  }

  # check if 'id' is supplied, is of class character or factor, and is not empty
  if (missing(id) || !(is.character(id) || is.factor(id)) || length(id) == 0 || id == "") {
    stop("Error: 'id' must be supplied, must be of class 'character' or 'factor'.", call. = FALSE)
  }

  # ensure the datetime is of class POSIXct
  if (!inherits(datetime, "POSIXct")) {
    stop("Error: 'datetime' must be of class 'POSIXct'.", call.=FALSE)
  }

  # ensure video metadata is provided and has necessary columns
  if (missing(video.metadata) || !all(c("ID", "start", "end", "video", "file") %in% colnames(video.metadata))) {
    stop("Error: video.metadata must contain 'start', 'end', 'video', and 'file' columns.", call.=FALSE )
  }

  # Check if the provided id exists in video.metadata
  if (!id %in% video.metadata$ID) {
    stop("Error: The provided 'id' does not exist in the supplied video.metadata.", call. = FALSE)
  }

  # find the video entry that contains the datetime
  matching_video <- video.metadata[video.metadata$ID == id & video.metadata$start <= datetime & video.metadata$end >= datetime,]
  matching_video <- matching_video[!is.na(matching_video$file),]


  ###############################################################
  # if a matching video is found, open it at the specified time
  if(nrow(matching_video)>0){

    # print a message to console
    cat(paste0("Opening video: ", matching_video$video, "\n"))

    # calculate the time in seconds from the video's start to the target datetime
    skip_time <- floor(as.numeric(difftime(datetime, matching_video$start, units="secs")))

    # construct the VLC command to start the video at the calculated time
    #vlc_command <- sprintf('"%s" --start-time=%d "%s"', vlc.path, skip_time, matching_video$file)

    #  # construct the VLC command to start the video at the calculated time (and suppressed console output)
    vlc_command <- sprintf('"%s" --start-time=%d --quiet "%s" > /dev/null 2>&1', vlc.path, skip_time, matching_video$file)

    # Execute the command in the background
    system(vlc_command, wait=FALSE)

    ###############################################################
    # handle edge cases if no matching video is found for the datetime
  } else {

    # find the video entries matching the specified ID
    matching_videos <- video.metadata[video.metadata$ID == id,]

    # if the datetime is after the latest video start time
    if (datetime >= max(matching_videos$start, na.rm=TRUE)) {
      cat("The specified datetime is later than the latest available video.\n")

    # if the datetime is before the earliest video start time
    } else if (datetime <= min(matching_videos$start, na.rm=TRUE)) {
      cat("The specified datetime is earlier than the earliest available video.\n")

    # else
    } else
      cat("No matching video was found for the specified datetime. Double-check your 'id' and 'datetime' values, as well as the video metadata.\n")
  }
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
