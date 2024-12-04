#######################################################################################################
# Function to retrieve video metadata #################################################################
#######################################################################################################

#' Extract Video Metadata and Timestamps
#'
#' This function processes video files in a specified directory to extract metadata,
#' including start time, duration, end time, and frame rate. The start time is derived
#' from an OCR operation on the first frame of each video.
#'
#' @param directory Character string. Path to the directory containing video files.
#' Supported formats are `.mp4` and `.mov`.
#' 
#' @return A data frame containing metadata for each video, including:
#'   \describe{
#'     \item{video}{Filename of the video.}
#'     \item{start}{Start time of the video (POSIXct).}
#'     \item{end}{End time of the video (POSIXct).}
#'     \item{duration}{Duration of the video in seconds.}
#'     \item{frame_rate}{Frame rate of the video in frames per second.}
#'     \item{file}{Full path to the video file.}
#'   }
#'   
#' @export


getVideoMetadata <- function(directory){
  
  # ensure the input directory exists
  if(!dir.exists(directory)) stop("Error: The specified input directory does not exist.", call.=FALSE)
  
  # list all video files in the input directory
  video_files <- list.files(directory, pattern = "\\.(mp4|mov)$", full.names = TRUE)
  
  # check if there are any .mov files to process
  if(length(directory) == 0) stop("No video files found in the specified input directory.", call.=FALSE)
  
  # start the timer
  start_time <- Sys.time()
  
  # print a summary message
  cat(sprintf("Analysing %d video files...\n", length(video_files)))
  
  # initialize results list
  video_info <- vector("list", length(video_files))
  
  # loop through each video file
  for (i in seq_along(video_files)) {
    
    # retrieve current file
    video <- video_files[i]
    
    # print progress to the console
    cat(sprintf("[%d/%d] Analysing: %s\n", i, length(video_files), basename(video)))
    
    ############################################################################
    # extract initial datetime from the first frame  ###########################
    
    # define temporary paths
    first_frame_path <- file.path(tempdir(), sprintf("first_frame_video%02d.jpg", i))
    cropped_frame_path <- file.path(tempdir(), sprintf("cropped_frame_video%02d.jpg", i))
    
    # extract the first frame using FFmpeg
    system(sprintf('ffmpeg -y -i "%s" -vframes 1 -q:v 3 "%s"', video, first_frame_path), ignore.stdout=TRUE, ignore.stderr=TRUE)
    
    # perform OCR on the cropped area of the frame
    image <- magick::image_read(first_frame_path)
    cropped <- magick::image_crop(image, geometry = "180x26+1602+1044")
    magick::image_write(cropped, cropped_frame_path)
    ocr_text <- tesseract::ocr(cropped_frame_path)
    
    # parse OCR text to extract datetime
    day_str <- substr(ocr_text, 1, 2)  
    month_str <- substr(ocr_text, 3, 5) 
    year_str <- substr(ocr_text, 6, 7)  
    time_str <- substr(ocr_text, 9, nchar(ocr_text))
    numeric_str <- list(day_str, year_str, time_str)
    numeric_str <- lapply(numeric_str, function(x) gsub("[BQ]", "0", x))
    numeric_str <- lapply(numeric_str, function(x) gsub(",", ".", x, fixed=TRUE))
    numeric_str <- lapply(numeric_str, function(x) gsub(" ", "", x, fixed=TRUE))
    numeric_str <- lapply(numeric_str, function(x) gsub("\n", "", x, fixed=TRUE))
    month_str <- lapply(month_str, function(x) gsub("0", "O", x, fixed=TRUE))
    datetime <- paste0(numeric_str[[1]], month_str, numeric_str[[2]], " ", numeric_str[[3]])
    
    # convert to POSIXct
    video_start <- as.POSIXct(datetime, "%d%b%y %H:%M:%OS", tz="UTC")
    
    
    ############################################################################
    # extract video duration ###################################################
    
    duration_cmd <- sprintf('ffprobe -i "%s" -show_entries format=duration -v quiet -of csv="p=0"', video)
    duration <- as.numeric(system(duration_cmd, intern = TRUE))
    
    
    ############################################################################
    # calculate end time #######################################################
    
    if(!is.na(datetime) && !is.na(duration)){
      video_end <- video_start + duration
    }else {
      video_end <- NA
    }
    
    ############################################################################
    # extract frame rate #######################################################
    
    framerate_cmd <- sprintf('ffprobe -i "%s" -select_streams v:0 -show_entries stream=r_frame_rate -v quiet -of csv="p=0"', video)
    frame_rate_raw <- system(framerate_cmd, intern = TRUE)
    
    # split the fraction and calculate as numeric
    rate_parts <- strsplit(frame_rate_raw, "/")[[1]]
    frame_rate <- as.numeric(rate_parts[1]) / as.numeric(rate_parts[2])
    
    ############################################################################
    # format results ###########################################################
    
    video_info[[i]] <- data.frame("video" = basename(video),
                                  "start" = video_start,
                                  "end" = video_end, 
                                  "duration" = duration,
                                  "frame_rate" = frame_rate, 
                                  "file" = video,
                                  stringsAsFactors = FALSE)
    
  }
  
  ##############################################################################
  # combine results into a data frame and return ###############################
  
  result <- do.call(rbind, video_info)
  return(result)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
