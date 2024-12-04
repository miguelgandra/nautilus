#######################################################################################################
# Function to convert and reencode fragmented mov files to hvec format ################################
#######################################################################################################

#' Reencode MOV Videos to HEVC Format
#'
#' This function re-encodes all `.mov` video files in a specified input directory to HEVC (H.265) format
#' using hardware acceleration (`hevc_videotoolbox`). The re-encoded videos are saved
#' as `.mp4` files in the specified output directory or the input directory by default.
#'
#' @param mov.directory A string specifying the path to the directory containing `.mov` files to re-encode.
#' @param output.directory A string specifying the path to the directory where re-encoded `.mp4` files will be saved. Defaults to the same as `mov_directory`.
#'
#' @details
#' - The re-encoding is done using the `ffmpeg` tool with the following options:
#'   - Video codec: `hevc_videotoolbox` for hardware-accelerated encoding
#'   - Constant rate factor (`crf`) of 28 for a balance of quality and compression
#'   - Preset: `fast` for faster encoding speed
#'   - No audio (`-an`).
#'
#' @return Prints progress and time statistics to the console. The re-encoded videos are saved in the specified output directory.
#' @export


reencodeVideos <- function(mov.directory, output.directory = mov.directory){

  # check if ffmpeg is installed
  ffmpeg_check <- system("which ffmpeg", intern = TRUE)
  if(length(ffmpeg_check) == 0) stop("Error: ffmpeg is not installed. Please install ffmpeg to proceed.", call. = FALSE)

  # ensure the input and output directories exist
  if(!dir.exists(mov.directory)) stop("Error: The specified input directory does not exist.", call. = FALSE)
  if(!dir.exists(output.directory)) stop("Error: The specified output directory does not exist.", call. = FALSE)

  # replace ~ with full path
  mov.directory <- path.expand(mov.directory)
  output.directory <- path.expand(output.directory)

  # list all .mov files in the input directory
  mov_files <- list.files(mov.directory, pattern=".mov", full.names = TRUE)

  # check if there are any .mov files to process
  if(length(mov_files) == 0) stop("No .mov files found in the specified input directory.", call. = FALSE)

  # start the timer
  start_time <- Sys.time()

  # print a summary message
  cat(sprintf("Reencoding %d files to HEVC format...\n", length(mov_files)))

  # loop through each .mov file for re-encoding
  for (i in seq_along(mov_files)) {

    # retrieve current file
    file <- mov_files[i]

    # define the output file path with the .mp4 extension in the output directory
    output_file <- file.path(output.directory, gsub("\\.mov$", ".mp4", basename(file)))

    # get the duration of the video using ffprobe
    duration_command <- sprintf('ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "%s"', file)
    duration <- as.numeric(system(duration_command, intern = TRUE))
    if(is.na(duration)) stop("Error: Could not retrieve video duration for ", basename(file), call =FALSE)

    # print progress to the console
    cat(sprintf("[%d] Processing: %s\n", i, basename(file)))

    # create a progress bar
    pb <- txtProgressBar(min=0, max=100, style=3)

    # construct the ffmpeg command and capture progress
    #ffmpeg_command <- sprintf('ffmpeg -i "%s" -c:v hevc_videotoolbox -crf 28 -preset fast -an "%s"', file, output_file)
    #process <- pipe(ffmpeg_command, "r")
    ffmpeg_command <- sprintf('ffmpeg -i "%s" -c:v hevc_videotoolbox -crf 28 -preset fast -an "%s" 2>&1', file, output_file)
    process <- pipe(ffmpeg_command, "r") # Redirect stderr to stdout using '2>&1'
    repeat {
      line <- readLines(process, n=1, warn=FALSE)
      if(length(line) == 0) break
      # look for "time=" in ffmpeg's progress output
      if(grepl("time=", line)) {
        time_str <- sub(".*time=([0-9:.]+).*", "\\1", line)
        time_parts <- as.numeric(strsplit(time_str, "[:.]")[[1]])
        current_time <- sum(time_parts * c(3600, 60, 1, 0.001)) # Convert to seconds
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
