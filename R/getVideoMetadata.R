#######################################################################################################
# Function to retrieve video metadata #################################################################
#######################################################################################################

#' Extract Metadata and Timestamps from Biologging Tag Video Files
#'
#' This function extracts key metadata from video files located within specified
#' directories. The extracted metadata includes the start time, end time, duration,
#' and frame rate of the videos. It is specifically designed to process videos
#' from biologging tags that include a black timestamp box positioned in the
#' bottom-right corner of the frames.
#' The function extracts individual frames using FFMPEG and employs Optical
#' Character Recognition (OCR) via the Tesseract engine to read the timestamps.
#' FFMPEG is also used to retrieve video information such as frame rate and duration.
#' As the position of the timestamp box is fixed (hardcoded), this function may
#' not be suitable for videos from tags with different configurations or formats.
#' For more details and usage guidelines, check out the `Details` section.
#'
#' @details
#' This function leverages the **tesseract** R package to perform Optical Character Recognition (OCR).
#' However, the **Tesseract OCR engine** itself must be installed separately on your system, as the R
#' package serves as a wrapper for it. The accuracy of the OCR process depends heavily on the version
#' and configuration of the Tesseract engine. For installation instructions, refer to the
#' [Tesseract GitHub page](https://github.com/tesseract-ocr/tesseract) or the relevant package documentation.
#'
#' The function extracts the start time of each video by applying OCR to the first frame. The `ocr.language`
#' parameter allows you to specify the OCR engine configuration. By default, this is set to `"cam"`, a custom-trained
#' OCR model optimized for reading timestamps from videos produced by specific biologging CAM tag devices. The custom
#' `"cam.traineddata"` file is included in the `inst/extdata` folder of the package repository.
#' For other video formats or devices, you may need to adjust the engine to a more appropriate OCR configuration.
#'
#' If `id.metadata` is supplied, the function uses the tagging date from the metadata as a reference,
#' extracting only the time component via OCR. This approach minimizes OCR errors by focusing on the
#' time portion of the timestamp, avoiding potential misinterpretations of the date.
#'
#' For optimal results, ensure both the `Tesseract OCR` engine and the custom-trained model are correctly
#' installed and configured. Additionally, verify the `FFMPEG` installation, as it is used to extract frames
#' and retrieve video details such as frame rate and duration.
#'
#' @param video.folders Character vector. Paths to directories containing video files.
#' @param video.format Character vector. Allowed video formats, such as "mp4" or "mov". Defaults to "mp4".
#' @param id.metadata Optional. Data frame containing metadata about the videos. Must include columns for
#'   identifying IDs (`id.col`) and tagging dates (`tagdate.col`). If provided, the tagging date is used to
#'   determine the start time of the video.
#' @param id.col Character string. Column name in `id.metadata` corresponding to folder IDs. Defaults to "ID".
#' @param tagdate.col Character string. Column name in `id.metadata` with tagging dates. Defaults to "tagging_date".
#' @param validate.timestamps In development.
#' @param ocr.language Character string. Language or configuration used by the OCR engine for recognizing
#' text in timestamp overlays. Defaults to "cam", indicating a predefined configuration for the target tag type.
#'
#' @return A data frame containing metadata for each video file, including:
#'   - `ID`: Folder identifier (if `id.metadata` is provided).
#'   - `video`: Video filename.
#'   - `start`: Start time of the video (POSIXct).
#'   - `end`: End time of the video (POSIXct).
#'   - `duration`: Video duration in seconds.
#'   - `frame_rate`: Frame rate in frames per second.
#'   - `file`: Full path to the video file.
#'
#' @export


getVideoMetadata <- function(video.folders,
                             video.format = "mp4",
                             id.metadata = NULL,
                             id.col = "ID",
                             tagdate.col = "tagging_date",
                             validate.timestamps = TRUE,
                             ocr.language = "cam"){

  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

  # start the timer
  start.time <- Sys.time()

  # check if the 'tesseract' package is installed.
  if(!requireNamespace("tesseract", quietly=TRUE)) stop("The 'tesseract' package is required for OCR but is not installed. Please install 'tesseract' using install.packages('tesseract') and try again.", call. = FALSE)

  # validate data.folders argument
  if(!is.character(video.folders)) stop("`video.folders` must be a character vector.", call. = FALSE)

  # check if the provided data.folders exist
  missing_folders <- video.folders[!dir.exists(video.folders)]
  if (length(missing_folders) > 0) {
    stop(paste0("The following folders were not found: ", paste(missing_folders, collapse = ", ")), call. = FALSE)
  }

  # ensure all video formats in the vector are valid; they must be either "mp4" or "mov".
  if (any(!video.format %in% c("mp4", "mov"))) {
    stop("Invalid video format. Please provide formats that are either 'mp4' or 'mov'.")
  }

  # validation checks for id.metadata arguments
  if(!is.null(id.metadata)){
    # check if specified columns exists in id.metadata
    if(!id.col %in% names(id.metadata)) stop(paste("The specified id.col ('", id.col, "') was not found in id.metadata.", sep = ""))
    if(!tagdate.col %in% names(id.metadata)) stop(paste("The specified tagdate.col ('", tagdate.col, "') was not found in id.metadata.", sep = ""))
    # check if tagdate.col is of class POSIXct
    if(!inherits(id.metadata[[tagdate.col]], "POSIXct")) {
      stop(paste("The column '", tagdate.col, "' in 'id.metadata' must be of class 'POSIXct'.", sep = ""), call. = FALSE)
    }
  }


  ##############################################################################
  # Verify and handle cam.traineddata ##########################################
  ##############################################################################

  if(ocr.language == "cam"){

    # get the Tesseract data directory path
    tesseract_path <- tesseract::tesseract_info()$datapath
    # define the full path for the custom Tesseract model 'cam.traineddata'
    cam_data_path <- file.path(tesseract_path, "cam.traineddata")
    # locate the 'cam.traineddata' file within the package's extdata directory
    package_data_path <- system.file("extdata", "cam.traineddata", package = "nautilus")

    # check if the 'cam.traineddata' file is missing from the Tesseract data directory
    if (!file.exists(cam_data_path)) {

      # print a warning message and prompt the user for action
      prompt_msg <- paste0(
        crayon::red$bold("Warning:\n"),
        "Looks like the 'cam.traineddata' file is missing from the tesseract folder. ",
        "This model was specifically fine-tuned for OCR of datetime stamps in cam tag videos. ",
        "Want me to move it there for you? (yes/no)\n"
      )
      cat(prompt_msg)

      # capture user input
      proceed <- readline()

      # if the user confirms (yes/y), attempt to move the file
      if(tolower(proceed) %in% c("yes", "y")) {
        if(file.copy(package_data_path, cam_data_path, overwrite = TRUE)){
          # inform the user if the file was successfully moved
          cat("All set! 'cam.traineddata' successfully moved to the tesseract data directory.\n")
        }else{
          # handle errors during the file copy process
          cat(crayon::red$bold("Yikes! I couldn't move the file...\n"))
        }
      }else{
        # inform the user that the default 'eng' model will be used
        cat("Ok, tesseract will use the 'eng' model instead. Just a heads-up, it might not be as accurate.\n")
      }
    }
  }


  ##############################################################################
  # Validate and configure OCR engine ##########################################
  ##############################################################################

  ocr_engine <- tryCatch(
    tesseract::tesseract(language = ocr.language, options = list(tessedit_pageseg_mode = 7)),
    error = function(e) {
      warning(sprintf("OCR engine '%s' not found. Falling back to default ('eng').", ocr.language), call. = FALSE)
      tesseract::tesseract(language = "eng", options = list(tessedit_pageseg_mode = 7))
    }
  )


  ##############################################################################
  # Retrieve video filenames from each directory ###############################
  ##############################################################################

  # validate folder animal IDs against id.metadata
  if(!is.null(id.metadata)){
    folder_ids <- basename(video.folders)
    missing_ids <- setdiff(folder_ids, id.metadata[[id.col]])
    if(length(missing_ids) > 0) {
      stop(paste0("\nThe following folder IDs were not found in 'id.metadata': ",
                  paste(missing_ids, collapse = ", "),
                  ".\nTo proceed, you can either:\n",
                  "1. Fix the issue by ensuring these folder IDs exist in 'id.metadata'.\n",
                  "2. Exclude these folders by modifying the 'video.folders' accordingly.\n"), call. = FALSE)
    }
  }


  # assign folder names to the list before using lapply
  names(video.folders) <- basename(video.folders)

  # define the search pattern for video files (extensions)
  search_pattern <- paste0("\\.(", paste0(video.format, collapse = "|"), ")$")

  # identify video files for each directory
  video_files <- lapply(video.folders, function(folder) {
    list.files(folder, full.names = TRUE, pattern = search_pattern, recursive = TRUE)
  })

  # keep only folders with at least one video file
  video.folders <- video.folders[sapply(video_files, length) > 0]


  ##############################################################################
  # Process videos for each folder #############################################
  ##############################################################################

  n_animals <- length(video.folders)
  n_videos <- sum(sapply(video_files, length))

  # feedback messages for the user
  cat(paste0(
    crayon::bold("\n=============== Extracting Video Metadata ===============\n"),
    "Processing ", n_videos, " video files across ", n_animals, ifelse(n_animals == 1, " dataset", " datasets"),
    " - OCR time!\n",
    crayon::bold("=========================================================\n\n")))


  # initialize results list
  video_info <- vector("list", n_animals)

  ###########################################################################
  # loop through each dataset (video folder) ################################
  for (i in 1:n_animals) {

    # retrieve the current animal/dataset ID + number of video files
    id <- names(video.folders)[i]
    n_videos <- length(video_files[[i]])

    # print animal ID to the console
    cat(crayon::blue$bold(id), "\n")

    # initialize a list to store results for all videos in this dataset
    individual_videos <- vector("list", n_videos)

    # initialize progress bar
    pb <- txtProgressBar(min=0, max=n_videos, initial=0, style=3)

    #########################################################################
    # iterate over each video file within the dataset #######################
    for(v in 1:n_videos){

      # retrieve the file path for the current video
      video <- video_files[[i]][v]

      # define temporary file paths for extracted and processed frames
      first_frame_path <- file.path(tempdir(),  sprintf("%s-first-frame-%03d.jpg", id, v))
      processed_frame_path <- file.path(tempdir(), sprintf("%s-processed-frame-%03d.png", id, v))

      # analyse the current video and store the results
      individual_videos[[v]] <- .analyseVideo(video, id, id.metadata, ocr_engine, first_frame_path, processed_frame_path, id.col, tagdate.col)

      # update progress bar
      setTxtProgressBar(pb, v)
    }

    # combine results for all videos in the current dataset into a single data frame
    video_info[[i]] <- do.call(rbind, individual_videos)

    # check if there is any mismatch in start dates
    # if a mismatch is found, video footage likely spans across different days (we need to add 24h)
    if(!is.null(id.metadata)){
     # check if each time is higher or lower than the preceding one
      time_differences <- diff(video_info[[i]]$start)
      # find indices where the start time is lower than the preceding one
      negative_indices <- which(time_differences < 0)
      # Update start and end times for all rows after the problematic rows
      if (length(negative_indices) > 0) {
        for (index in negative_indices) {
          # get the first index of the shifted date, and advance times from there
          adjustment_start_end <- (index + 1):nrow(video_info[[i]])
          # apply 24 hours adjustment (86400 seconds) to both 'start' and 'end' times after mismatch
          video_info[[i]]$start[adjustment_start_end] <- video_info[[i]]$start[adjustment_start_end] + 86400
          video_info[[i]]$end[adjustment_start_end] <- video_info[[i]]$end[adjustment_start_end] + 86400
        }
      }
    }

    # close progress bar
    close(pb)
  }


  ##############################################################################
  # Return results #############################################################
  ##############################################################################

  # combine results into a data frame
  result <- do.call(rbind, video_info)

  # print time taken
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  cat(sprintf("\nTotal execution time: %.02f %s\n\n", as.numeric(time.taken), base::units(time.taken)))

  # return results
  return(result)
}



################################################################################
# Define helper function to analyse video files ################################
################################################################################

#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.analyseVideo <- function(video,
                          id,
                          id.metadata,
                          ocr_engine,
                          first_frame_path,
                          processed_frame_path,
                          id.col,
                          tagdate.col){

  # extract the first frame using FFmpeg
  system(sprintf('ffmpeg -y -i "%s" -vframes 1 -q:v 1 -vf "scale=-1:2160" "%s"', video, first_frame_path), ignore.stdout=TRUE, ignore.stderr=TRUE)

  # read the extracted frame using the 'magick' package
  input_frame <- magick::image_read(first_frame_path)
  # crop the image to focus on the desired area
  cropped_image <- magick::image_crop(input_frame, geometry = "400x150+3210+2100")

  # image preprocessing
  processed_image <- magick::image_resize(cropped_image, geometry = "300%")
  processed_image <- magick::image_convert(processed_image, type = "Grayscale")
  processed_image <- magick::image_threshold(processed_image, type = "black", threshold = "15%")
  processed_image <- magick::image_morphology(processed_image, method = "Dilate", kernel = "Disk:0.3")
  processed_image <- magick::image_modulate(processed_image, brightness = 110, saturation = 100)
  processed_image <- magick::image_contrast(processed_image, sharpen = 5)

  # save the processed image for OCR
  magick::image_write(processed_image, path = processed_frame_path, format = "png", density = 300)

  # perform OCR on the processed image and extract the text
  ocr_text <- tesseract::ocr(processed_frame_path, engine = ocr_engine )

  # check if id.metadata is provided
  if(!is.null(id.metadata)){
    # extract the date portion from id.metadata (more reliable)
    tagging_date <- id.metadata[[tagdate.col]][id.metadata[[id.col]]==id]
    date_str <- format(tagging_date, "%d%b%y")
    # parse OCR text to extract the time portion
    time_str <- substr(ocr_text, 9, nchar(ocr_text))
    # clean and standardize the time string
    time_str <- gsub("[BQ]", "0", time_str)
    time_str <- gsub(",", ".", time_str, fixed = TRUE)
    time_str <- gsub(" ", "", time_str, fixed = TRUE)
    time_str <- gsub("\n", "", time_str, fixed = TRUE)
    # Combine the extracted date and time
    datetime <- paste0(date_str, " ", time_str)
  }else{
    # fall back to parsing the full date and time from OCR
    day_str <- substr(ocr_text, 1, 2)
    month_str <- substr(ocr_text, 3, 5)
    year_str <- substr(ocr_text, 6, 7)
    time_str <- substr(ocr_text, 9, nchar(ocr_text))
    # clean and standardize the strings
    numeric_str <- list(day_str, year_str, time_str)
    numeric_str <- lapply(numeric_str, function(x) gsub("[BQ]", "0", x))
    numeric_str <- lapply(numeric_str, function(x) gsub(",", ".", x, fixed = TRUE))
    numeric_str <- lapply(numeric_str, function(x) gsub(" ", "", x, fixed = TRUE))
    numeric_str <- lapply(numeric_str, function(x) gsub("\n", "", x, fixed = TRUE))
    month_str <- gsub("0", "O", month_str, fixed = TRUE)
    # combine the extracted parts
    datetime <- paste0(numeric_str[[1]], month_str, numeric_str[[2]], " ", numeric_str[[3]])
  }

  # check and replace 0 with O only if it's in the second position
  output_string <- sub("^(.{1})0", "\\1O", datetime)

  # check and replace invalid hour value "26" with "20"
  datetime <- sub("^([0-9]{2}[a-zA-Z]{3}[0-9]{2} )26", "\\120", datetime)

  # replace any  hour's tens digit higher than 2 with 0
  datetime <- sub("^([0-9]{2}[a-zA-Z]{3}[0-9]{2} )([3-9])", "\\10", datetime)

  # replace the tens digit of the minutes if it's greater than 5
  datetime <- sub("(\\d{2}:)([6-9])\\d", "\\10\\2", datetime)

  # replace any second tens digit higher than 5 with 0
  datetime <- sub("(\\d{2}:\\d{2}:)([6-9])", "\\10", datetime)

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

  video_info <- data.frame("ID" = id,
                           "video" = basename(video),
                           "start" = video_start,
                           "end" = video_end,
                           "duration" = duration,
                           "frame_rate" = frame_rate,
                           "file" = video,
                           stringsAsFactors = FALSE)

  # return result
  return(video_info)

}

#######################################################################################################
#######################################################################################################
#######################################################################################################
