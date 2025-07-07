#######################################################################################################
# Enhanced Function to retrieve video metadata with parallel processing ##########################
#######################################################################################################

#' Extract Metadata and Timestamps from Biologging Tag Video Files
#'
#' @description
#' This function extracts key metadata from video files located within specified
#' directories. The extracted metadata includes the start time, end time, duration,
#' and frame rate of the videos. It is specifically designed to process videos
#' from biologging tags that feature a black timestamp box positioned in the
#' bottom-right corner of the frames.
#'
#' The function extracts individual frames using FFMPEG and employs Optical
#' Character Recognition (OCR) via the Tesseract engine to read the timestamps
#' embedded in the video frames. FFMPEG is also used to retrieve video information
#' such as frame rate and duration.
#'
#' Because the timestamp box position is hardcoded and fixed, this function may
#' not be suitable for videos from biologging tags with different configurations,
#' timestamp placements, or video formats. Users should verify compatibility with
#' their specific video data prior to use.
#'
#' The function supports parallel processing to speed up metadata extraction from
#' large datasets. It also implements improved validation strategies based on
#' inter-video timing patterns, using gap-based checks to detect and correct
#' timestamp inconsistencies.
#'
#' @param video.folders Character vector. Paths to directories containing video files.
#' @param video.format Character vector. Allowed video formats, such as "mp4" or "mov". Defaults to "mp4".
#' @param tesseract.model Character string. Language or configuration used by the OCR engine for recognizing
#' text in timestamp overlays. Defaults to "cam", indicating a predefined configuration for the target tag type.
#' @param validate.timestamps Logical. Whether to perform temporal validation using multiple frames. Defaults to TRUE.
#' @param max.validation.attempts Integer. Maximum number of frame extractions to attempt for validation. Defaults to 6.
#' @param expected.gap.range Numeric vector of length 2. Expected range (in seconds) for gaps between video files. Defaults to c(10, 60).
#' @param gap.tolerance Numeric. Tolerance factor for gap validation (multiplier). Defaults to 2.0.
#' @param use.parallel Logical. Whether to use parallel processing. Defaults to TRUE.
#' @param n.cores Integer. Number of cores to use for parallel processing. If NULL, uses detectCores() - 1.

#' @return A data frame containing metadata for each video file with enhanced validation metrics.
#'
#' @export

getVideoMetadata <- function(video.folders,
                             video.format = "mp4",
                             tesseract.model = "cam",
                             validate.timestamps = TRUE,
                             max.validation.attempts = 6,
                             expected.gap.range = c(0, 60),
                             gap.tolerance = 2.0,
                             use.parallel = TRUE,
                             n.cores = NULL){


  ##############################################################################
  # Initial checks and setup ###################################################
  ##############################################################################

  start.time <- Sys.time()

  # check if the 'tesseract' package is installed.
  if(!requireNamespace("tesseract", quietly=TRUE)) stop("The 'tesseract' package is required for OCR but is not installed. Please install 'tesseract' using install.packages('tesseract') and try again.", call. = FALSE)

  # check if the 'tesseract' package is installed.
  if(!requireNamespace("magick", quietly=TRUE)) stop("The 'magick' package is required for image pre-processing but is not installed. Please install 'magick' using install.packages('magick') and try again.", call. = FALSE)

  # validate parallel computing packages
  if (use.parallel){
    if(!requireNamespace("foreach", quietly=TRUE)) stop("The 'foreach' package is required for parallel computing but is not installed. Please install 'foreach' using install.packages('foreach') and try again.", call. = FALSE)
    if(!requireNamespace("doSNOW", quietly=TRUE)) stop("The 'doSNOW' package is required for parallel computing but is not installed. Please install 'doSNOW' using install.packages('doSNOW') and try again.", call. = FALSE)
    if(!requireNamespace("parallel", quietly=TRUE)) stop("The 'parallel' package is required for parallel computing but is not installed. Please install 'parallel' using install.packages('parallel') and try again.", call. = FALSE)
  }

  # validate arguments
  if(!is.character(video.folders)) stop("`video.folders` must be a character vector.", call. = FALSE)

  missing_folders <- video.folders[!dir.exists(video.folders)]
  if (length(missing_folders) > 0) {
    stop(paste0("The following folders were not found: ", paste(missing_folders, collapse = ", ")), call. = FALSE)
  }

  if (any(!video.format %in% c("mp4", "mov"))) {
    stop("Invalid video format. Please provide formats that are either 'mp4' or 'mov'.")
  }



  ##############################################################################
  # Setup parallel processing ##################################################
  ##############################################################################

  if(use.parallel) {

    # define number of cores
    if(is.null(n.cores)) n.cores <- max(1, parallel::detectCores() - 1)

    # register parallel backend with the specified number of cores
    cl <- parallel::makeCluster(n.cores)
    doSNOW::registerDoSNOW(cl)

    # ensure the cluster is properly stopped when the function exits
    on.exit({
      tryCatch({
        if(exists("cl") && !is.null(cl)) {
          parallel::stopCluster(cl)
        }
      }, error = function(e) {
        # Ignore errors during cleanup
      })
    }, add = TRUE)


    # define the `%dopar%` operator locally for parallel execution
    `%dopar%` <- foreach::`%dopar%`

    # export necessary functions and variables to cluster
    parallel::clusterEvalQ(cl, {
      library(tesseract)
      library(magick)
    })

    # Export custom functions to cluster
    parallel::clusterExport(cl, c(".analyseVideo", ".extractAndProcessFrame",
                                  ".processSingleFrame", ".detectTimestampBox",
                                  ".parseTimestamp", ".cleanCharacters",
                                  ".cleanNumericString", ".cleanTimeString", ".cleanMonthString",
                                  ".validateAndCorrectTime", ".performTemporalValidation", ".isLikelyOCRError"),
                            envir = environment())

  }



  ##############################################################################
  # Verify and handle cam.traineddata ##########################################
  ##############################################################################

  if(tesseract.model == "cam"){

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
          stop("Yikes! I couldn't move the file...", call. = FALSE)
        }
      }else{
        # inform the user that the default 'eng' model will be used
        cat("Ok, tesseract will use the 'eng' model instead. Just a heads-up, it might not be as accurate.\n")
        tesseract.model <- "eng"
      }
    }
  }


  ##############################################################################
  # Configure OCR engine and retrieve video files #############################
  ##############################################################################

  # extract unique characters from month abbreviations and set whitelist for tesseract
  month_chars <- sort(unique(unlist(strsplit(paste(month.abb, collapse = ""), ""))))
  allowed_chars <- c(0:9, month_chars, ":", ".", " ")
  whitelist <- paste(allowed_chars, collapse = "")

  # OCR engine will be created within each worker process to avoid pointer issues
  ocr_engine <- NULL
  if(!use.parallel) {
    ocr_engine <- tesseract::tesseract(
      language = tesseract.model,
      options = list(
        tessedit_pageseg_mode = 7,
        tessedit_char_whitelist = whitelist
      )
    )
  }


  # fetch video files
  names(video.folders) <- basename(video.folders)
  search_pattern <- paste0("\\.(", paste0(video.format, collapse = "|"), ")$")

  # identify video files for each directory
  video_files <- lapply(video.folders, function(folder) {
    list.files(folder, full.names = TRUE, pattern = search_pattern, recursive = TRUE)
  })

  # Keep only folders with at least one video file
  video.folders <- video.folders[sapply(video_files, length) > 0]
  video_files <- video_files[sapply(video_files, length) > 0]


  ##############################################################################
  # Verbose messages ###########################################################
  ##############################################################################

  n_animals <- length(video.folders)
  n_videos <- sum(sapply(video_files, length))

  # feedback messages for the user
  cat(paste0(
    crayon::bold("\n============== Extracting Video Metadata ==============\n"),
    "Processing ", n_videos, " video files across ", n_animals, ifelse(n_animals == 1, " dataset", " datasets"),
    " - OCR time!\n",
    crayon::bold("=======================================================\n\n")))

  cat(paste0("Using Tesseract OCR model: ", tesseract.model, "\n"))
  if (use.parallel) {
    cat(paste0("Starting parallel computation: ", n.cores, " cores\n\n"))
  } else {
    cat("\n")
  }



  ##############################################################################
  # Process videos for each folder #############################################
  ##############################################################################

  # initialize results list
  video_info <- vector("list", n_animals)

  for (i in 1:n_animals) {

    # retrieve current ID and video file
    id <- names(video.folders)[i]
    current_videos <- video_files[[i]]
    n_videos_current <- length(current_videos)

    # print current ID being processed
    cat(sprintf("[%d/%d] %s - %d videos\n", i, n_animals, crayon::blue$bold(id), n_videos_current))


    ############################################################################
    # Parallel processing for multiple videos ##################################

    if(use.parallel && n_videos_current > 1) {

      # create progress function
      pb <- utils::txtProgressBar(max = n_videos_current, initial=0, style = 3)
      progress <- function(n) utils::setTxtProgressBar(pb, n)
      opts <- list(progress = progress)

      # run pipeline
      individual_videos <- foreach::foreach(v = 1:n_videos_current,
                                            .options.snow = opts,
                                            .packages = c("tesseract", "magick"),
                                            .combine = rbind) %dopar% {
                                              video <- current_videos[v]
                                              .analyseVideo(video, id, ocr_engine = NULL, tesseract.model,
                                                            whitelist, validate.timestamps, max.validation.attempts)
                                            }

      close(pb)


    ############################################################################
    # Sequential processing (for single videos or when parallel is disabled) ###

    } else {

      individual_videos <- vector("list", n_videos_current)
      pb <- txtProgressBar(min=0, max=n_videos_current, initial=0, style=3)

      for(v in 1:n_videos_current){
        video <- current_videos[v]
        individual_videos[[v]] <- .analyseVideo(video, id, ocr_engine, tesseract.model,
                                                whitelist, validate.timestamps, max.validation.attempts)
        setTxtProgressBar(pb, v)
      }

      close(pb)
      individual_videos <- do.call(rbind, individual_videos)
    }

    # apply gap-based validation and correction
    if(nrow(individual_videos) > 1) {
      individual_videos <- .applyGapBasedValidation(individual_videos, expected.gap.range, gap.tolerance)
    }

    # store results
    video_info[[i]] <- individual_videos
  }

  ##############################################################################
  # Cleanup and return results #################################################
  ##############################################################################

  # Ensure proper cluster cleanup
  if(use.parallel) {
    tryCatch({
      if(exists("cl") && !is.null(cl)) {
        parallel::stopCluster(cl)
        cl <- NULL
      }
    }, error = function(e) {
      warning("Error during cluster cleanup: ", e$message)
    })
  }

  # Combine results into a data frame
  result <- do.call(rbind, video_info)
  rownames(result) <- NULL

  # Print validation summary
  if(validate.timestamps && nrow(result) > 0) {
    avg_confidence <- mean(result$validation_confidence, na.rm = TRUE)
    high_confidence <- sum(result$validation_confidence >= 0.8, na.rm = TRUE)
    gap_corrected <- sum(result$gap_corrected, na.rm = TRUE)

    cat("\n")
    cat(sprintf("Validation summary:\n"))
    cat(sprintf("  - Average confidence: %.2f\n", avg_confidence))
    cat(sprintf("  - High confidence timestamps (\u22650.8): %d/%d (%.1f%%)\n",
                high_confidence, nrow(result), 100 * high_confidence / nrow(result)))
    cat(sprintf("  - Gap-corrected timestamps: %d/%d (%.1f%%)\n",
                gap_corrected, nrow(result), 100 * gap_corrected / nrow(result)))
  }

  # Print execution summary
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  cat(sprintf("\nTotal execution time: %.02f %s\n\n", as.numeric(time.taken), base::units(time.taken)))


  return(result)
}

################################################################################
# Gap-based validation function ##############################################
################################################################################

#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.applyGapBasedValidation <- function(video_data, expected.gap.range, gap.tolerance) {

  if (nrow(video_data) < 2) {
    message("Less than 2 rows in video_data, no gap analysis performed.")
    return(video_data)
  }

  # Initialize new columns
  video_data$gap_to_next <- NA_real_
  video_data$gap_flagged <- FALSE
  video_data$gap_corrected <- FALSE
  video_data$expected_gap <- NA_real_

  # Calculate gaps between consecutive videos
  for (i in 1:(nrow(video_data) - 1)) {
    if (!is.na(video_data$end[i]) && !is.na(video_data$start[i + 1])) {
      gap <- as.numeric(difftime(video_data$start[i + 1], video_data$end[i], units = "secs"))
      video_data$gap_to_next[i] <- gap
    }
  }

  # --- Determine the most likely (and reasonable) temporal gap ---
  # Filter for gaps within a broad, yet reasonable, initial range
  # This helps exclude extreme outliers before calculating the median
  initial_reasonable_gaps <- video_data$gap_to_next[
    !is.na(video_data$gap_to_next) &
      video_data$gap_to_next >= expected.gap.range[1] &
      video_data$gap_to_next <= expected.gap.range[2]
  ]

  if (length(initial_reasonable_gaps) == 0) {
    message("No reasonable gaps found to establish an expected gap. Returning original data.")
    return(video_data)
  }

  # Calculate the expected gap (median of filtered reasonable gaps)
  # Using median provides robustness against minor outliers
  expected_gap_value <- median(initial_reasonable_gaps, na.rm = TRUE)
  video_data$expected_gap <- expected_gap_value

  # Define the acceptable range around the expected gap
  lower_bound <- expected_gap_value * (1 - gap.tolerance)
  upper_bound <- expected_gap_value * (1 + gap.tolerance)

  # Adjust bounds to respect expected.gap.range if expected_gap_value is outside it
  lower_bound <- max(lower_bound, expected.gap.range[1])
  upper_bound <- min(upper_bound, expected.gap.range[2])

  # --- Identify and correct problematic gaps ---
  for (i in 1:(nrow(video_data) - 1)) {
    current_gap <- video_data$gap_to_next[i]

    if (!is.na(current_gap)) {
      # Flag gaps that are outside the calculated acceptable range
      is_spurious <- FALSE
      if (current_gap < lower_bound || current_gap > upper_bound) {
        is_spurious <- TRUE
        video_data$gap_flagged[i] <- TRUE
      }

      # --- Attempt correction only if flagged and a clear offset pattern exists ---
      if (is_spurious) {
        # Calculate the difference from the expected gap
        diff_from_expected <- current_gap - expected_gap_value

        correction_applied <- FALSE
        correction_amount <- 0

        # Pattern 1: Negative gap (start of next file is before end of current)
        # This often indicates a day rollover error (e.g., end is 23:59:00, start is 00:01:00 of next day, but year/month/day are wrong)
        if (current_gap < 0) {
          # Check for approximate 24-hour overlap (indicating a day miscalculation)
          if (abs(abs(current_gap) - 86400) < 3600) { # Within 1 hour of a 24-hour difference
            correction_amount <- 86400 # Add 24 hours to the next timestamp
            correction_applied <- TRUE
          }
        }
        # Pattern 2: Gap is significantly larger or smaller than expected
        else if (abs(diff_from_expected) > expected_gap_value * gap.tolerance) {

          # Common patterns for timestamp errors (in seconds):
          # 86400 seconds = 1 day
          # 3600 seconds = 1 hour
          # 60 seconds = 1 minute
          # Also consider multiples for multi-day errors, or year errors.

          # Check for multiples of a day (e.g., next day's timestamp but wrong year/month)
          if (abs(diff_from_expected %% 86400) < 3600) { # Is difference approximately a multiple of 24 hours?
            num_days_offset <- round(diff_from_expected / 86400)
            correction_amount <- -num_days_offset * 86400 # Subtract the full day multiples
            correction_applied <- TRUE
          }
          # Check for approximate year errors (365 or 366 days)
          else if (abs(diff_from_expected - (365 * 86400)) < 3600 * 24) { # Within 24 hours of 365 days
            correction_amount <- -(365 * 86400)
            correction_applied <- TRUE
          }
          else if (abs(diff_from_expected - (366 * 86400)) < 3600 * 24) { # Within 24 hours of 366 days (leap year)
            correction_amount <- -(366 * 86400)
            correction_applied <- TRUE
          }

        }

        # Apply correction if a clear pattern was identified
        if (correction_applied) {
          video_data$start[i + 1] <- video_data$start[i + 1] + correction_amount
          video_data$end[i + 1] <- video_data$end[i + 1] + correction_amount
          video_data$gap_corrected[i + 1] <- TRUE

          # Recalculate the gap after correction
          video_data$gap_to_next[i] <- as.numeric(difftime(video_data$start[i + 1], video_data$end[i], units = "secs"))
          # If corrected, the gap is no longer "flagged" for this iteration
          video_data$gap_flagged[i] <- FALSE
        }
      }
    }
  }

  return(video_data)
}


################################################################################
# Parallel-safe video analysis function ######################################
################################################################################

#' Analyse a single video to extract start and end times using embedded timestamps
#'
#' This function extracts the embedded timestamp from the first frame (or nearby frames, if needed)
#' and estimates the video's start and end time. Optionally, it validates and corrects the initial
#' timestamp based on inter-frame timing consistency. The function is designed to be safe for use
#' in parallel workflows (e.g., in `parallel::mclapply` or `future.apply`) by internally
#' creating the OCR engine if one is not supplied.
#'
#' It relies on `ffprobe` for extracting video duration and frame rate and uses a combination
#' of OCR and timestamp parsing to recover temporal information.

#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.analyseVideo <- function(video,
                          id,
                          ocr_engine = NULL,
                          tesseract.model,
                          whitelist,
                          validate.timestamps,
                          max.validation.attempts) {

  # create OCR engine if missing (e.g., in parallel workers)
  if (is.null(ocr_engine)) {
    ocr_engine <- tesseract::tesseract(
      language = tesseract.model,
      options = list(
        tessedit_pageseg_mode = 7,
        tessedit_char_whitelist = whitelist
      )
    )
  }

  # get video duration and frame rate first
  duration_cmd <- sprintf('ffprobe -i "%s" -show_entries format=duration -v quiet -of csv="p=0"', video)
  duration <- as.numeric(system(duration_cmd, intern = TRUE))

  framerate_cmd <- sprintf('ffprobe -i "%s" -select_streams v:0 -show_entries stream=r_frame_rate -v quiet -of csv="p=0"', video)
  frame_rate_raw <- system(framerate_cmd, intern = TRUE)
  rate_parts <- strsplit(frame_rate_raw, "/")[[1]]
  frame_rate <- as.numeric(rate_parts[1]) / as.numeric(rate_parts[2])

  # initialize validation variables
  validation_confidence <- 0
  frames_validated <- 1
  frames_searched <- 0
  timestamp_found_at_frame <- 0

  # extract and process first frame (with timestamp search capability)
  first_frame_result <- .extractAndProcessFrame(video, id, 0, ocr_engine, frame_rate = frame_rate,
                                                max_timestamp_search_frames = 10)
  video_start <- first_frame_result$timestamp

  # store information about timestamp search if it occurred
  if (!is.null(first_frame_result$frames_searched)) {
    frames_searched <- first_frame_result$frames_searched
    timestamp_found_at_frame <- ifelse(is.na(first_frame_result$timestamp_found_at_frame), 0, first_frame_result$timestamp_found_at_frame)
  }

  # perform temporal validation if requested and video is long enough
  if(validate.timestamps && !is.na(video_start) && duration > 10) {
    validation_result <- .performTemporalValidation(video, id, video_start, duration, frame_rate,
                                                    ocr_engine,  max.validation.attempts)
    video_start <- validation_result$corrected_timestamp
    validation_confidence <- validation_result$confidence
    frames_validated <- validation_result$frames_used
  } else {
    validation_confidence <- ifelse(is.na(video_start), 0, 0.6)
  }

  # calculate end time
  if(!is.na(video_start) && !is.na(duration)){
    video_end <- video_start + duration
  } else {
    video_end <- NA
  }

  # format results with additional columns for gap validation and timestamp search info
  video_info <- data.frame(
    "ID" = id,
    "video" = basename(video),
    "start" = video_start,
    "end" = video_end,
    "duration" = duration,
    "frame_rate" = frame_rate,
    "file" = video,
    "validation_confidence" = validation_confidence,
    "frames_validated" = frames_validated,
    "frames_searched_for_timestamp" = frames_searched,
    "timestamp_found_at_frame" = timestamp_found_at_frame,
    "gap_to_next" = NA,
    "gap_corrected" = FALSE,
    "expected_gap" = NA,
    stringsAsFactors = FALSE
  )

  # return result
  return(video_info)
}


################################################################################
# Function to extract and process a single frame with timestamp search ########
################################################################################

#' Extract and process a video frame to retrieve embedded timestamp information
#'
#' Attempts to extract a frame at a specified time offset from a video file and parse a timestamp using OCR.
#' If the initial frame does not contain a recognizable timestamp, the function iteratively searches subsequent
#' frames (up to a maximum number) until a valid timestamp is found. If so, it corrects the extracted timestamp
#' by accounting for the frame offset.
#'
#' This function is designed to increase robustness in scenarios where the first frame might be partially blank,
#' glitched, or missing the overlayed timestamp information.

#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.extractAndProcessFrame <- function(video, id, time_offset, ocr_engine, frame_rate = NULL,
                                    max_timestamp_search_frames = 10) {

  # process the requested frame first
  result <- .processSingleFrame(time_offset, video, id, ocr_engine)

  # if a timestamp could not be retrieved, try with dynamic coordinates
  if(is.na(result$timestamp)) {
    # first extract the frame
    frame_id <- paste0(id, "-", gsub("\\.", "_", as.character(time_offset)))
    frame_path <- file.path(tempdir(), sprintf("%s-frame.jpg", frame_id))
    # extract frame at specified time offset (fast seek)
    system(sprintf('ffmpeg -y -ss %.3f -i "%s" -vframes 1 -q:v 1 -vf "scale=-1:2160" "%s"',
                   time_offset, video, frame_path), ignore.stdout=TRUE, ignore.stderr=TRUE)
    input_frame <- magick::image_read(frame_path)
    # detect timestamp box coordinates
    detected_coords <- .detectTimestampBox(input_frame, expected_x = 3249, expected_y = 2120,
                                           expected_width = 325, expected_height = 28, search_radius = 100)
    # try to reprocess the frame with the box coordinates
    result <- .processSingleFrame(time_offset, video, id, ocr_engine, box.coords = detected_coords)
    unlink(frame_path)
  }

  # if we found a timestamp or this is not the first frame, return the result
  if (!is.na(result$timestamp) || time_offset != 0) {
    return(list(timestamp = result$timestamp, raw_ocr = result$raw_ocr))
  }

  # if this is the first frame and no timestamp was found, search subsequent frames
  if (time_offset == 0 && (is.na(result$timestamp) || !result$contains_text)) {

    # get frame rate if not provided
    if (is.null(frame_rate)) {
      framerate_cmd <- sprintf('ffprobe -i "%s" -select_streams v:0 -show_entries stream=r_frame_rate -v quiet -of csv="p=0"', video)
      frame_rate_raw <- system(framerate_cmd, intern = TRUE)
      rate_parts <- strsplit(frame_rate_raw, "/")[[1]]
      frame_rate <- as.numeric(rate_parts[1]) / as.numeric(rate_parts[2])
    }

    # calculate time increment per frame
    time_per_frame <- 1 / frame_rate

    # search through subsequent frames
    for (frame_num in 1:max_timestamp_search_frames) {
      search_time_offset <- frame_num * time_per_frame

      # process frame at this offset
      search_result <- .processSingleFrame(search_time_offset, video, id, ocr_engine)

      # if we found a valid timestamp, correct it back to the start time
      if (!is.na(search_result$timestamp)) {
        # Calculate the corrected start timestamp by subtracting the frame offset
        corrected_timestamp <- search_result$timestamp - search_time_offset

        return(list(
          timestamp = corrected_timestamp,
          raw_ocr = search_result$raw_ocr,
          frames_searched = frame_num,
          timestamp_found_at_frame = frame_num
        ))
      }
    }

    # if no timestamp found in any of the searched frames
    return(list(
      timestamp = NA,
      raw_ocr = NA,
      frames_searched = max_timestamp_search_frames,
      timestamp_found_at_frame = NA
    ))
  }

  # return original result if no search was needed
  return(list(timestamp = result$timestamp, raw_ocr = result$raw_ocr))
}


################################################################################
# # Helper function to process a single frame at given time ####################
################################################################################

#' Process a single frame from a video and extract timestamp using OCR
#'
#' Extracts a single frame at a given time offset using `ffmpeg`, applies preprocessing steps to improve OCR performance,
#' and attempts to extract a timestamp from the image. If the image lacks black pixels (usually indicative of absent text),
#' it skips OCR to save time. This function uses a combination of image enhancement, thresholding, and morphology to
#' optimise recognition of embedded timestamps.

#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd


.processSingleFrame <- function(current_time_offset,
                                video,
                                id,
                                ocr_engine,
                                box.coords = NULL) {

  # create unique temporary file paths
  frame_id <- paste0(id, "-", gsub("\\.", "_", as.character(current_time_offset)))
  first_frame_path <- file.path(tempdir(), sprintf("%s-frame.jpg", frame_id))

  # extract frame at specified time offset (fast seek)
  system(sprintf('ffmpeg -y -ss %.3f -i "%s" -vframes 1 -q:v 1 -vf "scale=-1:2160" "%s"',
                 current_time_offset, video, first_frame_path), ignore.stdout=TRUE, ignore.stderr=TRUE)

  # check if frame was extracted successfully
  if (!file.exists(first_frame_path)) {
    return(list(timestamp = NA, raw_ocr = NA, contains_text = FALSE))
  }

  # read frame
  input_frame <- magick::image_read(first_frame_path)

  # use timestamp box coordinates for cropping
  if(is.null(box.coords)){
    box.coords <- list(x = 3249, y = 2120, width = 325, height = 28)
  }
  crop_geometry <- sprintf("%dx%d+%d+%d", box.coords$width, box.coords$height,box.coords$x, box.coords$y)
  cropped_image <- magick::image_crop(input_frame, geometry = crop_geometry)

  # extend image by 10px in all directions
  processed_image <- magick::image_extent(cropped_image, geometry = "345x49", color = "black", gravity = "center")

  # enhanced preprocessing pipeline
  processed_image <- magick::image_convert(processed_image, colorspace = "gray")
  processed_image <- magick::image_negate(processed_image)
  processed_image <- magick::image_contrast(processed_image, sharpen = 1)
  processed_image <- magick::image_resize(processed_image, geometry = "300%")
  processed_image <- magick::image_morphology(processed_image, method = "Close", kernel = "Diamond", iterations = 1)
  processed_image <- magick::image_morphology(processed_image, method = "Open", kernel = "Disk", iterations = 1)
  processed_image <- magick::image_threshold(processed_image, type = "white", threshold = "70%")
  processed_image <- magick::image_threshold(processed_image, type = "black", threshold = "60%")

  # extract grayscale pixel values (returns 1 channel, character "0" to "255")
  img_data <- magick::image_data(processed_image, channels = "gray")

  # check if any pixel is black (value == "0")
  contains_black <- any(as.integer(img_data[1,,]) == 0)

  # clean up temporary files
  unlink(c(first_frame_path))

  # if image contains black pixels, text likely present
  if (!contains_black) {
    return(list(timestamp = NA, raw_ocr = NA, contains_text = FALSE))
  }

  # perform OCR
  ocr_text <- tesseract::ocr(processed_image, engine = ocr_engine)

  # parse timestamp with enhanced cleaning
  timestamp <- .parseTimestamp(ocr_text, id, current_time_offset)

  # return result
  return(list(timestamp = timestamp, raw_ocr = ocr_text, contains_text = TRUE))
}


################################################################################
# # Helper function to auto detect timestamp box coordinates ###################
################################################################################

#' Detect Timestamp Box in Frame Image
#'
#' Identifies the location of a white timestamp box within a video frame image
#' by searching around an expected position and computing the bounding box of
#' high-contrast white pixels.
#'
#' This internal helper function extracts a region of interest around the expected
#' timestamp location, applies a series of image processing steps to enhance and
#' binarize the image, and determines the bounding box of white pixels (assumed
#' to be the timestamp box). If no white pixels are found, the original expected
#' coordinates are returned.

#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.detectTimestampBox <- function(frame_image,
                                expected_x = 3249,
                                expected_y = 2120,
                                expected_width = 325,
                                expected_height = 28,
                                search_radius = 80) {

  # calculate search region
  search_width <- expected_width + (search_radius * 2)
  search_height <- expected_height + (search_radius * 2)
  search_x <- expected_x - search_radius
  search_y <- expected_y - search_radius

  # ensure boundaries
  frame_info <- magick::image_info(frame_image)
  search_x <- max(0, search_x)
  search_y <- max(0, search_y)
  search_width <- min(search_width, frame_info$width - search_x)
  search_height <- min(search_height, frame_info$height - search_y)

  search_geometry <- sprintf("%dx%d+%d+%d", search_width, search_height, search_x, search_y)
  search_region <- magick::image_crop(frame_image, geometry = search_geometry)

  # process to get binary image with white timestamp box
  gray_region <- magick::image_convert(search_region, colorspace = "gray")
  enhanced <- magick::image_contrast(gray_region, sharpen = 2)
  binary <- magick::image_threshold(enhanced, type = "white", threshold = "30%")
  binary <- magick::image_negate(binary)
  cleaned <- magick::image_morphology(binary, method = "Close", kernel = "Rectangle:10x3")
  cleaned <- magick::image_morphology(cleaned, method = "Open", kernel = "Rectangle:5x2")

  # extract pixel data (hex strings or numeric)
  pixel_data <- magick::image_data(cleaned, channels = "gray")
  # convert pixel data to numeric matrix (0–255)
  if (is.character(pixel_data)) {
    # assume hex if character values
    pixel_matrix <- apply(pixel_data[1,,], c(1, 2), function(x) strtoi(x, base = 16))
  } else {
    # already numeric
    pixel_matrix <- as.numeric(pixel_data[1,,])
    dim(pixel_matrix) <- dim(pixel_data)[2:3]
  }

  # find near white pixels (>250)
  white_coords <- which(pixel_matrix >= 252, arr.ind = TRUE)

  # no white pixels found, return original coordinates
  if (nrow(white_coords) == 0) {
    return(list(x = expected_x, y = expected_y, width = expected_width, height = expected_height))
  }

  # calculate bounding box of white pixels
  # white_coords gives us [row, col] which corresponds to [y, x] in image coordinates
  min_x <- min(white_coords[, 1])
  max_x <- max(white_coords[, 1])
  min_y <- min(white_coords[, 2])
  max_y <- max(white_coords[, 2])

  # calculate dimensions
  detected_width <- max_x - min_x + 1
  detected_height <- max_y - min_y + 1

  # convert back to original frame coordinates
  # note: R uses 1-based indexing, but image coordinates are typically 0-based
  detected_x <- search_x + min_x - 1
  detected_y <- search_y + min_y - 1

  return(list(x = detected_x, y = detected_y, width = detected_width, height = detected_height))
}


################################################################################
# Improved Temporal validation function ######################################
################################################################################

#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd
#'
.performTemporalValidation <- function(video, id, initial_timestamp, duration, frame_rate,
                                       ocr_engine, max_attempts) {
  if(is.na(initial_timestamp)) {
    return(list(corrected_timestamp = initial_timestamp, confidence = 0, frames_used = 1))
  }

  # Define validation points (in seconds)
  validation_points <- c(1, 5, 7, 11, 63, 70)
  validation_points <- validation_points[validation_points<=duration]
  validated_timestamps <- list()
  validated_timestamps[[1]] <- list(time = 0, timestamp = initial_timestamp)

  # Extract and validate additional frames
  for(i in seq_along(validation_points)) {
    time_offset <- validation_points[i]
    frame_result <- .extractAndProcessFrame(video, id, time_offset, ocr_engine)

    if(!is.na(frame_result$timestamp)) {
      validated_timestamps[[length(validated_timestamps) + 1]] <-
        list(time = time_offset, timestamp = frame_result$timestamp)
    }
  }

  # Calculate confidence and correct timestamp
  frames_used <- length(validated_timestamps)

  # If we have enough validation points, attempt correction
  corrected_timestamp <- initial_timestamp
  confidence <- 0.6  # Base confidence

  if(frames_used >= 3) {
    # Extract times and timestamps for analysis
    times <- sapply(validated_timestamps, function(x) x$time)
    timestamps <- sapply(validated_timestamps, function(x) x$timestamp)

    # Calculate time differences between consecutive validation points
    time_diffs <- diff(times)
    timestamp_diffs <- diff(as.numeric(timestamps))

    # Calculate expected vs actual time progression
    expected_rates <- time_diffs
    actual_rates <- timestamp_diffs

    # Check if the time progression is consistent (within ±1 seconds tolerance)
    rate_errors <- abs(actual_rates - expected_rates)
    consistent_progression <- mean(rate_errors <= 1, na.rm = TRUE)

    if(consistent_progression >= 0.65) {  # At least 65% of intervals are consistent (4 out of 6)
      # Use the most reliable validation points to estimate the true start time
      # Method 1: Back-calculate from each validation point
      estimated_starts <- numeric(length(times))
      for(i in seq_along(times)) {
        estimated_starts[i] <- as.numeric(timestamps[i]) - times[i]
      }

      # Remove outliers (more than 30 seconds difference from median)
      median_start <- median(estimated_starts, na.rm = TRUE)
      valid_estimates <- estimated_starts[abs(estimated_starts - median_start) <= 30]

      if(length(valid_estimates) >= 2) {
        # Use median of valid estimates as corrected timestamp
        corrected_start <- median(valid_estimates, na.rm = TRUE)
        corrected_timestamp <- as.POSIXct(corrected_start, origin = "1970-01-01", tz = "UTC")

        # Adjust confidence based on consistency and validation quality
        consistency_bonus <- 0.3 * consistent_progression
        confidence <- min(1.0, 0.6 + consistency_bonus)

        # Additional confidence boost if we have many validation points
        if(length(valid_estimates) >= 3) {
          confidence <- min(1.0, confidence + 0.1)
        }

        # Check if correction pattern is consistent with common OCR errors (slashed zeros)
        correction_seconds <- abs(as.numeric(corrected_timestamp) - as.numeric(initial_timestamp))
        if(.isLikelyOCRError(initial_timestamp, corrected_timestamp)) {
          confidence <- min(1.0, confidence + 0.1)
        }
      }
    } else {
      # Inconsistent progression - lower confidence but don't correct
      confidence <- 0.4
    }
  } else if(frames_used == 2) {
    # With only one validation point, do basic consistency check
    time_diff <- validated_timestamps[[2]]$time - validated_timestamps[[1]]$time
    timestamp_diff <- as.numeric(difftime(validated_timestamps[[2]]$timestamp,
                                          validated_timestamps[[1]]$timestamp, units = "secs"))

    if(abs(timestamp_diff - time_diff) <= 5) {
      confidence <- 0.7
    } else {
      confidence <- 0.3
    }
  }

  # Final confidence adjustment based on number of successful validations
  validation_success_rate <- (frames_used - 1) / length(validation_points)
  confidence <- confidence * (0.7 + 0.3 * validation_success_rate)

  return(list(
    corrected_timestamp = corrected_timestamp,
    confidence = min(1.0, confidence),
    frames_used = frames_used,
    correction_applied = !identical(corrected_timestamp, initial_timestamp)
  ))
}


################################################################################
################################################################################
################################################################################

#' Helper function to check if correction pattern matches common OCR errors
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd
#'
.isLikelyOCRError <- function(original_timestamp, corrected_timestamp) {
  # Convert timestamps to character strings for digit comparison
  original_str <- format(original_timestamp, "%Y-%m-%d %H:%M:%S")
  corrected_str <- format(corrected_timestamp, "%Y-%m-%d %H:%M:%S")

  # Remove non-digit characters for comparison
  original_digits <- gsub("[^0-9]", "", original_str)
  corrected_digits <- gsub("[^0-9]", "", corrected_str)

  # Check if strings are same length
  if(nchar(original_digits) != nchar(corrected_digits)) {
    return(FALSE)
  }

  # Count digit differences
  original_chars <- strsplit(original_digits, "")[[1]]
  corrected_chars <- strsplit(corrected_digits, "")[[1]]

  # Find positions where digits differ
  diff_positions <- which(original_chars != corrected_chars)

  # Check if differences are consistent with common OCR errors
  if(length(diff_positions) == 0) {
    return(FALSE)  # No differences
  }

  # Check if most differences involve the digit "0" (common OCR error)
  zero_errors <- 0
  for(pos in diff_positions) {
    if(original_chars[pos] == "0" || corrected_chars[pos] == "0") {
      zero_errors <- zero_errors + 1
    }
  }

  # If majority of errors involve "0", it's likely an OCR error
  return(zero_errors >= length(diff_positions) * 0.5)
}


################################################################################
# Enhanced timestamp parsing function ########################################
################################################################################

#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.parseTimestamp<- function(ocr_text, id,  time_offset = 0) {

  # Clean OCR text
  ocr_text <- gsub("\\s+", " ", ocr_text)  # normalize whitespace
  ocr_text <- gsub("\n", "", ocr_text)     # remove newlines
  ocr_text <- trimws(ocr_text)             # trim whitespace

  # Enhanced character cleaning with more robust replacements
  ocr_text <- .cleanCharacters(ocr_text)

  # Handle extra dots in time portion (e.g., "17:.00:20.152")
  # Replace patterns like ":." with ":" in the time portion
  if (nchar(ocr_text) >= 9) {
    time_part <- substr(ocr_text, 9, nchar(ocr_text))
    time_part <- gsub(":\\.", ":", time_part)  # Fix ":." to ":"
    time_part <- gsub("\\.(?=\\d{2}\\.)", ":", time_part, perl = TRUE)
    ocr_text <- paste0(substr(ocr_text, 1, 8), time_part)
  }

  # parse full datetime from OCR
  if(nchar(ocr_text) < 8) return(NA)

  day_str <- substr(ocr_text, 1, 2)
  month_str <- substr(ocr_text, 3, 5)
  year_str <- substr(ocr_text, 6, 7)
  time_str <- substr(ocr_text, 9, nchar(ocr_text))

  # Clean numeric components
  day_str <- .cleanNumericString(day_str)
  year_str <- .cleanNumericString(year_str)
  time_str <- .cleanTimeString(time_str)
  month_str <- .cleanMonthString(month_str)

  datetime_str <- paste0(day_str, month_str, year_str, " ", time_str)


  # additional time-specific validations and corrections
  datetime_str <- .validateAndCorrectTime(datetime_str, time_offset)

  # convert to POSIXct
  timestamp <- as.POSIXct(datetime_str, format = "%d%b%y %H:%M:%OS", tz = "UTC")

  # return
  return(timestamp)
}

################################################################################
# Enhanced character cleaning function ######################################
################################################################################

#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.cleanCharacters <- function(text) {

  #Common OCR misidentifications for digits

  # 0 can be misread as O, Q, D
  text <- gsub("[OQD](?=\\d|$)", "0", text, perl = TRUE)

  # 1 can be misread as I, l, |
  text <- gsub("[Il|](?=\\d|:)", "1", text, perl = TRUE)

  # 2 can be misread as Z
  text <- gsub("Z(?=\\d|:)", "2", text, perl = TRUE)

  # 5 can be misread as S
  text <- gsub("S(?=\\d|:)", "5", text, perl = TRUE)

  # 6 can be misread as G, b
  text <- gsub("[Gb](?=\\d|:)", "6", text, perl = TRUE)

  # 8 can be misread as B
  text <- gsub("B(?=\\d|:)", "8", text, perl = TRUE)

  # Handle common punctuation misidentifications
  text <- gsub(",", ".", text, fixed = TRUE)
  text <- gsub(";", ":", text, fixed = TRUE)

  return(text)
}

################################################################################
# Helper functions for cleaning specific components ##########################
################################################################################

#' @note These functions are intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.cleanNumericString <- function(str) {
  str <- gsub("[BQ]", "0", str)
  str <- gsub("[Il|]", "1", str)
  str <- gsub("Z", "2", str)
  str <- gsub("S", "5", str)
  str <- gsub("[Gb]", "6", str)
  str <- gsub(",", ".", str, fixed = TRUE)
  str <- gsub(" ", "", str, fixed = TRUE)
  return(str)
}

.cleanTimeString <- function(str) {
  str <- .cleanNumericString(str)
  str <- gsub(";", ":", str, fixed = TRUE)
  return(str)
}

.cleanMonthString <- function(str) {
  # Correct common month misidentifications
  str <- gsub("0", "O", str, fixed = TRUE)  # 0 should be O in month
  str <- gsub("1", "I", str, fixed = TRUE)  # 1 should be I in month (rare but possible)

  # Correct specific OCR misreadings of month abbreviations
  str <- gsub("S5ep", "Sep", str, fixed = TRUE)
  return(str)
}

.validateAndCorrectTime <- function(datetime_str, expected_offset = 0) {

  # Extract time components for validation
  parts <- strsplit(datetime_str, " ")[[1]]
  if(length(parts) < 2) return(datetime_str)

  time_part <- parts[2]
  time_components <- strsplit(time_part, ":")[[1]]

  if(length(time_components) >= 2) {
    hour <- time_components[1]
    minute <- time_components[2]

    # Validate and correct hours (00-23)
    hour_num <- as.numeric(hour)
    if(!is.na(hour_num)) {
      if(hour_num > 23) {
        # Common misidentification: 26 -> 20, 29 -> 20, etc.
        if(hour_num >= 26 && hour_num <= 29) {
          hour <- "20"
        } else if(hour_num >= 30) {
          hour <- paste0("0", substr(hour, 2, 2))
        }
      }
    }

    # Validate and correct minutes (00-59)
    minute_num <- as.numeric(minute)
    if(!is.na(minute_num)) {
      if(minute_num >= 60) {
        # If tens digit is wrong, replace with 0-5
        tens_digit <- substr(minute, 1, 1)
        if(as.numeric(tens_digit) > 5) {
          minute <- paste0("0", substr(minute, 2, 2))
        }
      }
    }

    # Validate seconds if present
    if(length(time_components) >= 3) {
      second_part <- time_components[3]
      second_num <- as.numeric(strsplit(second_part, "\\.")[[1]][1])
      if(!is.na(second_num) && second_num >= 60) {
        tens_digit <- substr(second_part, 1, 1)
        if(as.numeric(tens_digit) > 5) {
          time_components[3] <- paste0("0", substr(second_part, 2, nchar(second_part)))
        }
      }
    }

    # Reconstruct time part
    time_components[1] <- hour
    time_components[2] <- minute
    time_part <- paste(time_components, collapse = ":")
    parts[2] <- time_part
    datetime_str <- paste(parts, collapse = " ")
  }

  return(datetime_str)
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
