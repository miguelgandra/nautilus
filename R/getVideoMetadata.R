#######################################################################################################
# Function to retrieve video metadata #################################################################
#######################################################################################################

#' Extract Video Metadata and Timestamps
#'
#' This function extracts metadata from video files within specified directories. The metadata includes
#' start time, end time, duration, and frame rate. The function is specifically fine-tuned for processing
#' videos outputted by a particular biologging tag format, characterized by a black timestamp box located
#' in the bottom right corner of the frames. The position of the timestamp box is hardcoded into the function,
#' which means it may not work correctly with videos from other tag formats or configurations. The start
#' time is determined using Optical Character Recognition (OCR) applied to the first frame of each video.
#' If `id.metadata` is provided, the function assumes that the tagging date of the animal corresponds to
#' the start date, and only the time portion is extracted via OCR. This approach improves the reliability
#' of time extraction by using the known tagging date as a reference. Note that OCR is not perfect,
#' and its accuracy may vary from video to video.
#'
#' @param video.folders Character vector. Paths to directories containing video files.
#' @param video.format Character vector. Allowed video formats, such as "mp4" or "mov". Defaults to "mp4".
#' @param id.metadata Optional. Data frame containing metadata about the videos. Must include columns for
#'   identifying IDs (`id.col`) and tagging dates (`tagdate.col`). If provided, the tagging date is used to
#'   determine the start time of the video.
#' @param id.col Character string. Column name in `id.metadata` corresponding to folder IDs. Defaults to "ID".
#' @param tagdate.col Character string. Column name in `id.metadata` with tagging dates. Defaults to "tagging_date".
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
                             tagdate.col = "tagging_date"){

  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

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
  # Define allowed characters for OCR processing ###############################
  ##############################################################################

  # extract unique characters from English month abbreviations
  month_letters <- paste0(month.abb,  collapse = "")
  month_letters <- unlist(strsplit(month_letters, split = ""))
  month_letters <- unique(month_letters)
  char_order <- order(ifelse(month_letters %in% LETTERS, 0, 1), month_letters)
  month_letters <- month_letters[char_order]
  month_letters <- paste0(month_letters,  collapse = "")

  # append numerical digits, punctuation, and spaces to allowed characters
  allowed_chars <- paste0(month_letters, "0123456789.: ")


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


  # identify video data files for each directory
  search_pattern <- paste0( "\\.(", paste0(video.format, collapse="|"), ")$")
  video_files <- sapply(video.folders, function(x) list.files(x, full.names = TRUE, pattern = search_pattern, recursive = TRUE))
  names(video_files) <- basename(video.folders)
  video_files <- stack(video_files)
  colnames(video_files) <- c("video", "ID")


  ##############################################################################
  # Process videos for each folder #############################################
  ##############################################################################

  # start the timer
  start_time <- Sys.time()

  # initialize results list
  video_info <- vector("list", length(video.folders))

  # feedback messages for the user
  cat(crayon::bold("Extracting Video Metadata\n"))
  cat(sprintf("Analysing %d video files from %d folders...\n", nrow(video_files), length(video.folders)))
  # print folder names
  #cat(crayon::blue(paste0("- ", basename(video.folders), collapse = "\n")), "\n")

  # initialize progress bar
  pb <- txtProgressBar(min=0, max=nrow(video_files), initial=0, style=3)


  #########################################################################
  # loop through each video file ##########################################
  for (i in 1:nrow(video_files)) {

    # retrieve current file
    id <- video_files$ID[i]
    video <- video_files$video[i]

    ############################################################################
    # extract initial datetime from the first frame  ###########################

    # define temporary paths
    first_frame_path <- file.path(tempdir(), sprintf("first_frame_video%04d.jpg", i))
    cropped_frame_path <- file.path(tempdir(), sprintf("cropped_frame_video%04d.png", i))

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
    magick::image_write(processed_image, path = cropped_frame_path, format = "png", density = 300)

    # perform OCR on the processed image and extract the text
    ocr_engine <- tesseract::tesseract(language = "eng", options=list(tessedit_pageseg_mode = 7, tessedit_char_whitelist = allowed_chars))
    ocr_text <- tesseract::ocr(cropped_frame_path, engine = ocr_engine )
    ocr_text

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

    # check and replace invalid hour value "26" with "20"
    datetime <- sub("^([0-9]{2}[a-zA-Z]{3}[0-9]{2} )26", "\\120", datetime)

    # replace any  hour's tens digit higher than 2 with 0
    datetime <- sub("^([0-9]{2}[a-zA-Z]{3}[0-9]{2} )([3-9])", "\\10", datetime)

    # Replace the tens digit of the minutes if it's greater than 5
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

    video_info[[i]] <- data.frame("ID" = id,
                                  "video" = basename(video),
                                  "start" = video_start,
                                  "end" = video_end,
                                  "duration" = duration,
                                  "frame_rate" = frame_rate,
                                  "file" = video,
                                  stringsAsFactors = FALSE)

    # update progress bar
    setTxtProgressBar(pb, i)

  }


  ##############################################################################
  # Return results #############################################################
  ##############################################################################

  # close progress bar
  close(pb)

  # combine results into a data frame and return
  result <- do.call(rbind, video_info)
  return(result)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
