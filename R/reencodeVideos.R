#######################################################################################################
# Function to convert and reencode fragmented mov files to hvec format ################################
#######################################################################################################

#' Reencode MOV Videos to HEVC Format
#'
#' This function re-encodes all `.mov` video files found in a specified input directory to
#' the more efficient HEVC (H.265) format. Users can choose the desired encoder
#' (e.g., `hevc_videotoolbox`, `libx265`, etc.) via the `encoder` parameter. By default,
#' the function uses `libx265` for software encoding, but users can opt for hardware-accelerated
#' encoding (`hevc_videotoolbox`) for faster processing times.
#' The re-encoded files are saved as `.mp4` videos in the designated output directory,
#' or in the same directory as the input files if no output path is provided.
#' The `crf` (Constant Rate Factor) or `video.quality` argument can be specified to fine-tune
#' the trade-off between video quality and file size, depending on the selected encoder.
#' Additionally, the `overwrite` parameter allows users to control how existing output
#' files are handled: overwrite them, skip them, or prompt the user before any action is taken.

#' @param mov.directory A string specifying the path to the directory containing `.mov` files to re-encode.
#' @param output.directory A string specifying the path to the directory where re-encoded `.mp4` files will be saved.
#' Defaults to the same as `mov.directory`.
#' @param file.suffix Character string to be appended to the base name of the
#' output files (before the `.mp4` extension). This can be useful for differentiating
#' reencoded files.
#' @param encoder A string specifying the video encoder to use for re-encoding videos.
#' Defaults to "hevc_videotoolbox", which utilizes hardware acceleration for faster processing when available.
#' Supported values, such as "libx265" for software encoding, depend on the encoders available in `ffmpeg`.
#' To view the available options, run `ffmpeg -codecs` in your terminal.
#' @param crf A numeric value for the Constant Rate Factor (CRF), which controls the quality and file size of the output video.
#' The CRF range is from 0 to 51, where:
#'   - A value of 0 represents lossless encoding, resulting in the highest quality but the largest file size.
#'   - A value of 18 is the default and provides high-quality output, free from most artifacts.
#'   - A higher value (closer to 51) will result in lower quality and smaller file sizes.
#' This parameter is **only used when software encoding (libx265)** is selected.
#' For hardware-accelerated encoders, such as `hevc_videotoolbox`, `h265_nvenc`, or `hevc_amf`, video quality is controlled by the `video.quality` parameter instead.
#' @param video.quality A numeric value between 1 and 100 that defines the quality of the output video. Higher values result in better quality but larger file sizes.
#' This parameter is used when hardware acceleration (e.g., `hevc_videotoolbox`, `h265_nvenc`, etc.) is selected as the encoder.
#' Defaults to 50.
#' @param preset A string specifying the encoding speed preset. This controls the trade-off between encoding speed and output file size/quality.
#' Valid options are: 'ultrafast', 'superfast', 'veryfast', 'faster', 'fast', 'medium', 'slow', 'slower', 'veryslow'.
#' Faster presets result in quicker encoding but lower compression, leading to larger file sizes, while slower presets provide better compression at the cost of longer encoding time.
#' The default is "medium".
#' @param overwrite A string that controls the behavior when output files already exist. Can be one of the following:
#'   - `ask`: Prompt the user for each file whether to overwrite it (default).
#'   - `TRUE`: Automatically overwrite existing files.
#'   - `FALSE`: Skip re-encoding for existing files.
#'
#' @details
#' The `encoder` parameter specifies the video encoder used for converting files to HEVC format.
#' Supported encoders depend on your FFmpeg installation and the hardware or software capabilities of your system.
#'
#' Below are some commonly used HEVC encoders:
#' - **libx265**: An open-source software-based HEVC encoder offering high-quality output and extensive customization options.#'
#' - **hevc_videotoolbox**: Apple's hardware-accelerated HEVC encoder available on macOS devices.
#' - **h265_nvenc**: NVIDIA's hardware-accelerated HEVC encoder. Requires an NVIDIA GPU with NVENC support.
#' - **hevc_amf**: AMD's hardware-accelerated HEVC encoder. Requires an AMD GPU with AMF support.
#' - **hevc_qsv**: Intel's hardware-accelerated HEVC encoder using Quick Sync Video technology. Requires a CPU with Quick Sync support.
#'
#' To check which encoders are available on your system, use the following command:
#'
#' ```bash
#' ffmpeg -encoders | grep hevc
#' ```
#'
#' The output will list all HEVC encoders supported by your FFmpeg installation. Choose the one that best fits your hardware and encoding requirements.
#'
#'
#' @return Prints progress and time statistics to the console. The re-encoded videos are saved in the specified output directory.
#' @export


reencodeVideos <- function(mov.directory,
                           output.directory = mov.directory,
                           file.suffix = "",
                           encoder = "libx265",
                           crf = 18,
                           video.quality = 50,
                           preset = "medium",
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

  # validate the video.quality argument
  if(!is.numeric(video.quality) || length(video.quality) != 1 || video.quality < 1 || video.quality > 100) {
    stop("Error: 'video.quality' must be a numeric value between 1 and 100.", call. = FALSE)
  }

  # validate the preset argument
  valid_presets <- c("ultrafast", "superfast", "veryfast", "faster", "fast", "medium", "slow", "slower", "veryslow")
  if (!preset %in% valid_presets) {
    stop("Error: Invalid preset value. Valid options are: 'ultrafast', 'superfast', 'veryfast', 'faster', 'fast', 'medium', 'slow', 'slower', 'veryslow'.", call. = FALSE)
  }

  # replace ~ with full path
  mov.directory <- path.expand(mov.directory)
  output.directory <- path.expand(output.directory)

  # list all .mov files in the input directory
  mov_files <- list.files(mov.directory, pattern=".mov", full.names = TRUE)
  mp4_files <- list.files(mov.directory, pattern=".mp4", full.names = TRUE)
  video_files <- c(mov_files, mp4_files)

  # check if there are any .mp4 files in the input directory
  if (length(mp4_files) > 0) {
    cat(paste0("The following files are already in .mp4 format:\n",
               crayon::blue(paste(basename(mp4_files), collapse = "\n"), "\n\n"),
               "Do you wish to continue processing? (yes/no)"))
    response <- readline()
    if (!tolower(response) %in% c("yes", "y")) {
      # exit the function if the user decides not to proceed
      cat("Processing cancelled.\n")
      return(invisible(NULL))
    }
  }

  # Check if there are any files to process
  if (length(video_files) == 0) {
    stop("No valid video files (.mov or .mp4) found in the specified input directory.", call. = FALSE)
  }

  # validate the encoder directly within the function
  encoders_output <- system("ffmpeg -codecs 2>&1", intern = TRUE)
  if (!any(grepl(paste0("\\b", encoder, "\\b"), encoders_output))) {
    stop(sprintf("Error: Specified encoder '%s' is not available in your ffmpeg installation. Please check your ffmpeg setup.", encoder), call. = FALSE)
  }


  ##############################################################################
  # List files in console ######################################################
  ##############################################################################

  # start the timer
  start_time <- Sys.time()

  # print a message to the console
  cat(sprintf("Reencoding %d file%s to HEVC format:\n", length(video_files),  ifelse(length(video_files) == 1, "", "s")))

  # list all files to be converted
  if(length(video_files) > 0) {
    cat(paste0(" - ", crayon::blue(basename(video_files)), collapse = "\n"), "\n")
  }
  cat("\n")


  ##############################################################################
  # Process videos #############################################################
  ##############################################################################

  # loop through each .mov file for re-encoding
  for (i in seq_along(video_files)) {

    # retrieve current file
    file <- video_files[i]

    # define the output file path and append the suffix to the output filename (with .mp4 extension)
    output_file <- file.path(output.directory, paste0(tools::file_path_sans_ext(basename(file)), file.suffix, ".mp4"))

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
    if (encoder %in% c("hevc_videotoolbox", "h265_nvenc", "hevc_amf", "hevc_qsv")) {
      # hardware accelerated encoders generally don't use CRF
      ffmpeg_command <- sprintf('ffmpeg -i "%s" -c:v %s -q:v %d -preset %s -tag:v hvc1 -an -y "%s" 2>&1',
                                file, encoder, video.quality, preset, output_file)
    } else if (encoder == "libx265") {
      # software encoder (libx265) uses CRF
      ffmpeg_command <- sprintf('ffmpeg -i "%s" -c:v %s -crf %d -preset %s -tag:v hvc1 -an -y "%s" 2>&1',
                                file, encoder, crf, preset, output_file)
    }

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
