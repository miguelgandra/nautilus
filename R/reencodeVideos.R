#######################################################################################################
# Function to convert and reencode fragmented mov files to hvec format ################################
#######################################################################################################

#' Reencode MOV Videos to HEVC Format
#'
#' This function re-encodes all `.mov` video files in a specified input directory to HEVC (H.265) format.
#' By default, it uses hardware acceleration (`hevc_videotoolbox`) for faster encoding, but if hardware acceleration
#' is unavailable, the function will fall back to software encoding using `libx265`, which may take longer.
#' The re-encoded videos are saved as `.mp4` files in the specified output directory, or in the input directory by default.
#' Users can specify a Constant Rate Factor (`crf`) to control the balance between video quality and file size.
#' Additionally, the `overwrite` parameter determines whether to overwrite existing output files, skip them, or prompt the user.
#'
#' @param mov.directory A string specifying the path to the directory containing `.mov` files to re-encode.
#' @param output.directory A string specifying the path to the directory where re-encoded `.mp4` files will be saved.
#' Defaults to the same as `mov.directory`.
#' @param crf A numeric value for the Constant Rate Factor (CRF), which controls the quality and file size of the output video.
#' The CRF range is from 0 to 51, where:
#'   - A value of 0 represents lossless encoding, resulting in the highest quality but the largest file size.
#'   - A value of 28 is considered a good balance between quality and compression for most use cases. This is the default value.
#'   - A higher value (closer to 51) will result in lower quality and smaller file sizes.
#' @param overwrite A string that controls the behavior when output files already exist. Can be one of the following:
#'   - `ask`: Prompt the user for each file whether to overwrite it (default).
#'   - `TRUE`: Automatically overwrite existing files.
#'   - `FALSE`: Skip re-encoding for existing files.
#'
#' @details
#' The re-encoding is done using the `ffmpeg` tool with the following options:
#'   - Video codec: `hevc_videotoolbox` for hardware-accelerated encoding (if available) or `libx265` for software encoding.
#'   - Constant rate factor (`crf`) set by user or default (28) for a balance of quality and compression.
#'   - Preset: `fast` for faster encoding speed.
#'   - No audio (`-an`).
#'
#'
#' @return Prints progress and time statistics to the console. The re-encoded videos are saved in the specified output directory.
#' @export


reencodeVideos <- function(mov.directory,
                           output.directory = mov.directory,
                           crf = 28,
                           overwrite = "ask"){


  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

  # check if ffmpeg is installed
  ffmpeg_check <- system("which ffmpeg", intern = TRUE)
  if(length(ffmpeg_check) == 0) stop("ffmpeg is not installed. Please install ffmpeg to proceed.", call. = FALSE)

  # ensure the input and output directories exist
  if(!dir.exists(mov.directory)) stop("The specified input directory does not exist.", call. = FALSE)
  if(!dir.exists(output.directory)) stop("The specified output directory does not exist.", call. = FALSE)

  # validate the crf argument
  if(!is.numeric(crf) || length(crf) != 1 || crf < 0 || crf > 51) {
    stop("Error: 'crf' must be a numeric value between 0 and 51.", call. = FALSE)
  }

  # replace ~ with full path
  mov.directory <- path.expand(mov.directory)
  output.directory <- path.expand(output.directory)

  # list all .mov files in the input directory
  mov_files <- list.files(mov.directory, pattern=".mov", full.names = TRUE)

  # check if there are any .mov files to process
  if(length(mov_files) == 0) stop("No .mov files found in the specified input directory.", call. = FALSE)

  # check if hevc_videotoolbox codec is available
  hw_accel_check <- system("ffmpeg -codecs 2>&1 | grep hevc_videotoolbox", intern = TRUE)
  if(length(hw_accel_check) > 0) {
    # use hardware acceleration
    video_codec <- "hevc_videotoolbox"
  } else {
    # fall back to software encoding
    video_codec <- "libx265"
    # ask the user if they want to proceed with software encoding
    prompt_msg <- paste(crayon::red$bold("Warning:"),
                        "Hardware acceleration (hevc_videotoolbox) not available. Using libx265 instead.",
                        "This may take a long time to complete, depending on the size and number of videos.",
                        "Do you wish to continue anyway?",
                        crayon::black$bold("(yes/no)\n"))
    prompt_msg <- paste(strwrap(prompt_msg, width=getOption("width")), collapse="\n")
    cat(prompt_msg)
    # capture user input
    proceed <- readline()
    # convert input to lowercase and check if it is negative
    if(!(tolower(proceed) %in% c("yes", "y"))) {
      # exit the function if the user decides not to proceed
      cat("Process cancelled.\n")
      return(invisible(NULL))
    }
  }


  ##############################################################################
  # List files in console ######################################################
  ##############################################################################

  # start the timer
  start_time <- Sys.time()

  # print a message to the console
  cat(sprintf("Reencoding %d file%s to HEVC format:\n", length(mov_files),  ifelse(length(mov_files) == 1, "", "s")))

  # list all files to be converted
  if(length(mov_files) > 0) {
    cat(paste0(" - ", crayon::blue(basename(mov_files)), collapse = "\n"), "\n")
  }
  cat("\n")


  ##############################################################################
  # Process videos #############################################################
  ##############################################################################

  # loop through each .mov file for re-encoding
  for (i in seq_along(mov_files)) {

    # retrieve current file
    file <- mov_files[i]

    # define the output file path with the .mp4 extension in the output directory
    output_file <- file.path(output.directory, gsub("\\.mov$", ".mp4", basename(file)))

    # check if the output file exists
    if (file.exists(output_file)) {
      if (overwrite == "ask") {
        overwrite_msg <- paste0("File already exists:\n",
                               crayon::yellow(" -", basename(output_file)), "\n",
                               "Overwrite?",
                               crayon::black$bold(" (yes/no)\n"))
        cat(overwrite_msg)
        response <- readline()
        if (!tolower(response) %in% c("yes", "y")) {
          cat("File skipped.\n\n"); next
        }else{
          cat("\n")
        }
      } else if (!overwrite) {
        cat(sprintf("Skipping file %s (already exists).\n\n", basename(output_file)))
        next
      }
    }

    # get the duration of the video using ffprobe
    duration_command <- sprintf('ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "%s"', file)
    duration <- as.numeric(system(duration_command, intern = TRUE))
    if(is.na(duration)) stop("Error: Could not retrieve video duration for ", basename(file), call =FALSE)

    # print progress to the console
    cat(sprintf("[%d] Processing: %s\n", i, crayon::blue(basename(file))))

    # create a progress bar
    pb <- txtProgressBar(min=0, max=100, style=3)

    # construct the ffmpeg command and capture progress
    ffmpeg_command <- sprintf('ffmpeg -i "%s" -c:v %s -crf %d -preset fast -an -y "%s" 2>&1', file, video_codec, crf, output_file)
    # redirect stderr to stdout using '2>&1'
    process <- pipe(ffmpeg_command, "r")
    current_time <- 0
    repeat {
      line <- readLines(process, n=1, warn=FALSE)
      if(length(line) == 0) break
      # look for "time=" in ffmpeg's progress output
      if(grepl("time=", line)) {
        time_str <- sub(".*time=([0-9:.]+).*", "\\1", line)
        time_parts <- suppressWarnings(as.numeric(strsplit(time_str, "[:.]")[[1]]))
        if(all(!is.na(time_parts))) current_time <- sum(time_parts * c(3600, 60, 1, 0.001)) # Convert to seconds
        # calculate and update progress
        progress <-round(min(100, (current_time / duration) * 100))
        setTxtProgressBar(pb, progress)
      }
    }

    # close the progress bar and the process
    close(pb)
    close(process)
  }

  # print completion message
  cat("Reencoding process completed successfully.\n")

  # stop the timer
  end_time <- Sys.time()
  time.taken <- end_time - start_time
  cat(paste("Total execution time:", sprintf("%.02f", as.numeric(time.taken)), base::units(time.taken), "\n"))
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
