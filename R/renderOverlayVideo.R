#######################################################################################################
# Function to render an overlay video with synchronized sensor data ##################################
#######################################################################################################

#' renderOverlayVideo
#'
#' This function generates a video with synchronized sensor data overlayed onto the video frames.
#' It processes the video and sensor data, extracts the frames, synchronizes them with the sensor data,
#' applies overlays, and outputs the final annotated video.
#'
#' @param video.file The path to the input video file.
#' @param video.metadata A data frame containing metadata about the video,
#'   including start and end times, frame rate, and video ID. This should be
#'   the output of the \code{\link{getVideoMetadata}} function.
#' @param sensor.data A data frame containing the sensor data, which includes timestamps and the associated
#'   metrics to be overlayed (e.g., depth, heading, pitch, roll).
#' @param output.file The name of the output video file (e.g., "output.mp4").
#' @param output.directory The directory where temporary and final files will be saved.
#' @param start.time (Optional) The start time in the video from which to begin processing,
#' in the format "HH:MM:SS". Defaults to "00:00:00".
#' @param end.time (Optional) The end time in the video up to which processing should occur,
#' in the format "HH:MM:SS".
#' @param duration (Optional) The duration of the video segment to process, in seconds. Overrides \code{end.time}
#' if specified.
#' @param overlay.side The side of the frame where overlays should be displayed ("left", "center" or "right"). Defaults to "left".
#' @param depth.window The time window (in seconds) for averaging or visualizing depth data. Defaults to 300 seconds (5 minutes).
#' @param vedba.window The time window (in seconds) for averaging or visualizing VE-DBA (Vectorial Dynamic Body Acceleration) data.
#'   Defaults to 30 seconds.
#' @param vertical.speed.window The time window (in seconds) for calculating vertical speed. Defaults to 30 seconds.
#' @param text.color The color of the overlay text. Defaults to "black".
#' @param sensor.val.color The color used to display sensor values. Defaults to "red3".
#' @param jpeg.quality An integer specifying the quality of the extracted JPEG frames.
#' This value controls the level of compression applied to the frames.
#' The `jpeg.quality` value is passed to `ffmpeg`'s `-qscale:v` option. A value of
#' `1` represents the best quality (larger file size), while a value of `31`
#' represents the worst quality (smaller file size). The default is `4`, which provides
#' a good balance between quality and file size.
#' @param video.compression Character. Compression format for the output video. Options are `"h264"` for standard compression
#'   and `"h265"` for HEVC compression. Default is `"h265"`.
#'   HEVC (H.265) offers higher compression efficiency compared to H.264, resulting in smaller file sizes
#'   with comparable or better video quality.
#' @param crf A numeric value for the Constant Rate Factor (CRF), which controls the quality and file size of the output video.
#' The CRF range is from 0 to 51, where:
#'   - A value of 0 represents lossless encoding, resulting in the highest quality but the largest file size.
#'   - A value of 28 is considered a good balance between quality and compression for most use cases. This is the default value.
#'   - A higher value (closer to 51) will result in lower quality and smaller file sizes.
#' @param cores The number of processor cores to use for parallel computation. Defaults to 1 (single-core).
#' @return A video file with synchronized sensor overlays saved to the specified \code{output.file}.
#'
#' @details
#' This function extracts frames from the specified video, aligns each frame with the corresponding timestamp in the
#' sensor data, and overlays the sensor data onto the frames. It supports single-core and multi-core processing
#' for frame overlay generation. The final annotated video is assembled using FFmpeg.
#'
#' @note
#' Ensure FFmpeg is installed and accessible via the system PATH, as it is required for video processing.
#'
#' @export


renderOverlayVideo <- function(video.file,
                               video.metadata,
                               sensor.data,
                               output.file,
                               output.directory = NULL,
                               start.time = NULL,
                               end.time = NULL,
                               duration = NULL,
                               overlay.side = "left",
                               depth.window = 5 * 60,
                               vedba.window = 30,
                               vertical.speed.window = 30,
                               text.color = "black",
                               sensor.val.color = "red3",
                               jpeg.quality = 3,
                               video.compression = "h265",
                               crf = 28,
                               cores = 1){



  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

  # check if ffmpeg is installed
  ffmpeg_check <- system("which ffmpeg", intern = TRUE)
  if(length(ffmpeg_check) == 0) stop("ffmpeg is not installed. Please install ffmpeg to proceed.", call. = FALSE)

  # check if the video file exists
  if (!file.exists(video.file)) stop("The specified video file does not exist.", call. = FALSE)

  # check if the output directory exists
  if (!is.null(output.directory) && !dir.exists(output.directory)) stop("The specified output directory does not exist: ", output.directory, call. = FALSE)

  # validate video.metadata structure
  required_metadata_cols <- c("ID", "video", "start", "end", "frame_rate")
  missing_cols <- setdiff(required_metadata_cols, colnames(video.metadata))
  if (length(missing_cols) > 0) {
    stop("The 'video.metadata' is missing the following required columns: ", paste(missing_cols, collapse = ", "), call. = FALSE)
  }

  # validate sensor.data structure
  required_data_cols <- c("datetime", "depth", "heading", "pitch", "roll", "vedba", "vertical_speed")
  missing_cols <- setdiff(required_data_cols, colnames(sensor.data))
  if (length(missing_cols) > 0) {
    stop("The 'sensor.data' is missing the following required columns: ", paste(missing_cols, collapse = ", "), call. = FALSE)
  }

  # validate overlay.side
  if (!overlay.side %in% c("left", "center", "right")) stop("Invalid value for 'overlay.side'. Must be one of: 'left', 'center', or 'right'.", call. = FALSE)

  # validate numerical arguments
  if (depth.window <= 0 || !is.numeric(depth.window)) stop("'depth.window' must be a positive number.", call. = FALSE)
  if (vedba.window <= 0 || !is.numeric(vedba.window)) stop("'vedba.window' must be a positive number.", call. = FALSE)
  if (vertical.speed.window <= 0 || !is.numeric(vertical.speed.window)) stop("'vertical.speed.window' must be a positive number.", call. = FALSE)
  if (cores <= 0 || !is.numeric(cores) || cores %% 1 != 0) stop("'cores' must be a positive integer.", call. = FALSE)

  # validate parallel computing packages
  if (cores>1 && !requireNamespace("foreach", quietly=TRUE)) stop("The 'foreach' package is required for parallel computing but is not installed. Please install 'foreach' using install.packages('foreach') and try again.", call. = FALSE)
  if (cores>1 && !requireNamespace("doSNOW", quietly=TRUE)) stop("The 'doSNOW' package is required for parallel computing but is not installed. Please install 'doSNOW' using install.packages('doSNOW') and try again.", call. = FALSE)
  if (cores>1 && !requireNamespace("parallel", quietly=TRUE)){
    stop("The 'parallel' package is required for parallel computing but is not installed. Please install 'parallel' using install.packages('parallel') and try again.", call. = FALSE)
  }else if(parallel::detectCores()<cores){
    stop(paste("Please choose a different number of cores for parallel computing (only", parallel::detectCores(), "available)."), call. = FALSE)
  }

  # validate that not both end.time and duration are supplied simultaneously
  if (!is.null(end.time) && !is.null(duration)) stop("Only one of 'end.time' or 'duration' can be provided, not both.", call. = FALSE)

  # validate start.time and end.time formats
  time_pattern <- "^([01]?[0-9]|2[0-3]):([0-5]?[0-9]):([0-5]?[0-9])$"
  if (!grepl(time_pattern, start.time)) stop("Invalid start.time format. Please use HH:MM:SS.", call. = FALSE)
  if (!grepl(time_pattern, end.time)) stop("Invalid end.time format. Please use HH:MM:SS.", call. = FALSE)

  # validate jpeg.quality argument
  if (jpeg.quality < 1 || jpeg.quality > 31 || !is.numeric(jpeg.quality)) stop("The 'jpeg.quality' argument must be an integer between 1 and 31.", call. = FALSE)

  # validate the crf argument
  if(!is.numeric(crf) || length(crf) != 1 || crf < 0 || crf > 51) stop("Error: 'crf' must be a numeric value between 0 and 51.", call. = FALSE)


  ##############################################################################
  # Retrieve video metadata ####################################################
  ##############################################################################

  selected_video <- video.metadata[video.metadata$video==basename(video.file),]
  id <- as.character(selected_video$ID)
  start <- selected_video$start
  end <-  selected_video$end
  duration <- selected_video$duration
  frame_rate <- selected_video$frame_rate


  ##############################################################################
  # Define paths and directory variables #######################################
  ##############################################################################

  # set a temporary output directory if not defined by the user
  if(is.null(output.directory)) output.directory <-  tempdir()

  # append the unique ID and the base name of the video file to the output path
  output.directory <- file.path(output.directory, id, tools::file_path_sans_ext(basename(video.file)))

  # check if the output directory already exists
  if (dir.exists(output.directory)) {

    # print a warning and prompt the user for confirmation
    prompt_msg <- paste0(
      crayon::red$bold("Warning: "),
      "The output directory already exists. ",
      "Existing files may be overwritten. ",
      "Proceed? (yes/no):\n"
    )
    # wrap the message to ensure it fits within the console width
    prompt_msg <- strwrap(prompt_msg, width = getOption("width"))
    # print the wrapped message
    cat(paste(prompt_msg, collapse = "\n"))

    # capture user input
    proceed <- readline()

    # convert input to lowercase and check if it is negative
    if(!(tolower(proceed) %in% c("yes", "y"))) {
      # exit the function if the user decides not to proceed
      cat("Processing cancelled.\n")
      return(invisible(NULL))
    }
  }

  # define subdirectories for storing frames, overlays, and processed frames
  frames_directory <- file.path(output.directory, "raw frames")
  overlays_directory <- file.path(output.directory, "overlays")
  processed_directory <- file.path(output.directory, "processed frames")

  # ensure the directories exist and are empty
  if (dir.exists(frames_directory)) unlink(frames_directory, recursive = TRUE)
  dir.create(frames_directory, recursive = TRUE)
  if (dir.exists(overlays_directory)) unlink(overlays_directory, recursive = TRUE)
  dir.create(overlays_directory, recursive = TRUE)
  if (dir.exists(processed_directory)) unlink(processed_directory, recursive = TRUE)
  dir.create(processed_directory, recursive = TRUE)


  ##############################################################################
  # 1 - Extract frames #########################################################
  ##############################################################################

  # print progress message
  cat("Extracting video frames...\n")

  # set default value for start.time if not supplied
  if(is.null(start.time)) {
    start.time <- "00:00:00"
  }

  # calculate the effective duration
  if (!is.null(end.time)) {
    # convert start.time and end.time to seconds
    start_seconds <- sum(as.numeric(strsplit(start.time, ":")[[1]]) * c(3600, 60, 1))
    end_seconds <- sum(as.numeric(strsplit(end.time, ":")[[1]]) * c(3600, 60, 1))
    effective_duration <- end_seconds - start_seconds
  } else if (!is.null(duration)) {
    # use duration directly
    effective_duration <- as.numeric(duration)
  } else {
    effective_duration  <- difftime(end, start, "secs")
  }

  # compute the expected frame count
  frame_count <- ceiling(effective_duration * frame_rate)
  cat(sprintf("Expected frame count: %d\n", frame_count))


  # construct the ffmpeg command based on which arguments are provided
  if (!is.null(end.time)) {
    # case 1: start.time and end.time are provided
    ffmpeg_command <- sprintf(
      "ffmpeg -i \"%s\" -ss %s -to %s -q:v %d \"%s/frame_%%06d.jpg\" 2>&1",
      video.file, start.time, end.time, jpeg.quality, frames_directory)
  } else if (!is.null(duration)) {
    # case 2: start.time and duration are provided
    ffmpeg_command <- sprintf(
      "ffmpeg -i \"%s\" -ss %s -t %s -q:v %d \"%s/frame_%%06d.jpg\" 2>&1",
      start.time, video.file, duration, jpeg.quality, frames_directory)
  }

  # initialize progress bar
  pb <- txtProgressBar(min=1, max=frame_count, initial=0, style=3)

  # redirect stderr to stdout using '2>&1'
  process <- pipe(ffmpeg_command, "r")
  extracted_frames <- 0
  repeat {
    line <- readLines(process, n = 1, warn = FALSE)
    if (length(line) == 0) break
    # look for "frame=" in ffmpeg's progress output
    if (grepl("frame=", line)) {
      frame_str <- sub(".*frame=\\s*([0-9]+).*", "\\1", line)
      current_frame <- as.numeric(frame_str)
      if (!is.na(current_frame)) {
        extracted_frames <- current_frame
        # update progress bar based on the extracted frame count
        setTxtProgressBar(pb, min(extracted_frames, frame_count))
      }
    }
  }

  # close the progress bar and the process
  close(pb)
  close(process)

  # get total number of frames
  nframes <- length(list.files(frames_directory))


  ##############################################################################
  # 2 - Assign a frame number to each dataset row ##############################
  ##############################################################################

  # convert the selected start time to elapsed seconds
  time_parts <- as.integer(strsplit(start.time, ":")[[1]])
  elapsed_secs <- time_parts[1] * 3600 + time_parts[2] * 60 + time_parts[3]
  start <- start + elapsed_secs

  # cut dataset to match
  sensor.data <- sensor.data[sensor.data$datetime>=start & sensor.data$datetime<=end,]

  # calculate the time difference between sensor data and video start time
  sensor.data$time_diff <- as.numeric(difftime(sensor.data$datetime, start, units = "secs"))

  # convert time difference to frame numbers (+1 because frames are 1-indexed)
  sensor.data$frame <- floor(sensor.data$time_diff * frame_rate) + 1


  ##############################################################################
  # 3a - Overlay sensor data (single core) #####################################
  ##############################################################################

  if(cores == 1){

    # print progress message
    cat("Creating data overlay graphs...\n")

    # initialize progress bar
    pb <- txtProgressBar(min=1, max=nframes, initial=0, style=3)

    # iterate over each frame
    for (i in 1:nframes) {

      # add overlays and save processed frame
      .processFrames(i, start, frame_rate, sensor.data,
                     frames_directory, overlays_directory, processed_directory,
                     overlay.side, depth.window, vedba.window, vertical.speed.window,
                     text.color, sensor.val.color)

      # update progress bar
      setTxtProgressBar(pb, i)

    }

    # close progress bar
    close(pb)

  }

  ##############################################################################
  # 3b - Overlay sensor data (parallel computing) ##############################
  ##############################################################################

  # use multiple cores to speed up computations
  if(cores > 1) {

    # print progress message
    cat("Generating data overlay graphs...\n")

    # print information to console
    cat(paste0("Starting parallel computation: ", cores, " cores\n"))

    # register parallel backend with the specified number of cores
    cl <- parallel::makeCluster(cores)
    doSNOW::registerDoSNOW(cl)

    # ensure the cluster is properly stopped when the function exits
    on.exit(parallel::stopCluster(cl))

    # define the `%dopar%` operator locally for parallel execution
    `%dopar%` <- foreach::`%dopar%`

    # initialize progress bar
    pb <- txtProgressBar(min=1, max=nframes, initial=0, style=3)

    # set progress bar options
    opts <- list(progress = function(n) setTxtProgressBar(pb, n))

    # process frames using parallel computation
    foreach::foreach(i=1:nframes, .options.snow=opts, .packages=c("magick"), .export=c(".processFrames")) %dopar% {
     .processFrames(i, start, frame_rate, sensor.data,
                    frames_directory, overlays_directory, processed_directory,
                    overlay.side, depth.window, vedba.window, vertical.speed.window,
                    text.color, sensor.val.color)
   }

   # close progress bar
   close(pb)
  }


  ##############################################################################
  # 4 - Create mp4 #############################################################
  ##############################################################################

  # print progress message
  cat("Encoding final video...\n")

  # specify the output MP4 file name
  output.file <- path.expand(output.file)

  # determine the codec and output format based on the video.compression argument
  if (video.compression == "h265") {
    # check if the hevc_videotoolbox codec (hardware acceleration) is available
    hw_accel_check <- system("ffmpeg -codecs 2>&1 | grep hevc_videotoolbox", intern = TRUE)
    if (length(hw_accel_check) > 0) {
      # hardware-accelerated HEVC encoding is available, use it
      video_codec <- "hevc_videotoolbox"
      cat("Using hardware-accelerated HEVC encoding with hevc_videotoolbox.\n")
    } else {
      # hardware acceleration is not available, fall back to software-based encoding
      video_codec <- "libx265"
      cat("Hardware acceleration (hevc_videotoolbox) not available. Falling back to software-based HEVC encoding with libx265.\n")
    }
  } else {
    # if video.compression is not "h265", use the standard H.264 codec
    video_codec <- "libx264"
    cat("Using standard H.264 encoding with libx264.\n")
  }

  # construct the ffmpeg command
  ffmpeg_command <- sprintf(
    'ffmpeg -framerate 30 -i "%s/final_frame_%%06d.jpg" -c:v %s -pix_fmt yuv420p -crf %d "%s" 2>&1',
    processed_directory,
    video_codec,
    crf,
    output.file)

  # initialize progress bar
  pb <- txtProgressBar(min=1, max=nframes, initial=0, style=3)

  # redirect stderr to stdout using '2>&1'
  process <- pipe(ffmpeg_command, "r")
  extracted_frames <- 0
  repeat {
    line <- readLines(process, n = 1, warn = FALSE)
    if (length(line) == 0) break
    # look for "frame=" in ffmpeg's progress output
    if (grepl("frame=", line)) {
      frame_str <- sub(".*frame=\\s*([0-9]+).*", "\\1", line)
      current_frame <- as.numeric(frame_str)
      if (!is.na(current_frame)) {
        extracted_frames <- current_frame
        # update progress bar based on the extracted frame count
        setTxtProgressBar(pb, min(extracted_frames, nframes))
      }
    }
  }

  # close the progress bar and the process
  close(pb)
  close(process)

  # print message
  cat("Video created successfully! ***\n")
}



################################################################################
# Define helper function to process frames #####################################
################################################################################

.processFrames <- function(i, start, frame_rate, sensor_data,
                           frames_directory, overlays_directory, processed_directory,
                           overlay.side, depth.window, vedba.window, vertical.speed.window,
                           text.color, sensor.val.color){

  # define paths
  frame_path <- sprintf("%s/frame_%06d.jpg", frames_directory, i)
  overlay_path <- sprintf("%s/overlay_%06d.png", overlays_directory, i)
  final_frame_path <- sprintf("%s/final_frame_%06d.jpg", processed_directory, i)

  # get current frame datetime
  current_time <- start + (i - 1) / frame_rate

  # find the closest row in sensor_data based on the frame number
  closest_row_index <- which.min(abs(sensor_data$frame - i))
  frame_data <- sensor_data[closest_row_index, ]

  # read the original frame
  frame <- magick::image_read(frame_path)

  # open a PNG device with transparency
  png(filename = overlay_path, width = 1200, height = 1800, bg = "transparent", res = 300)

  # set up layout
  mat <- matrix(c(1,2,3,4,4,4,5,5,5,6,6,6,7,7,7), nrow=5, byrow=TRUE)
  layout(mat, heights=c(2,1,1,1,1))
  par(mar = c(1, 1, 1, 1), oma = c(0, 0, 1.5, 0), xpd = NA)


  #################################################################
  # plot orientation dials ########################################

  orientation_metrics <- c("heading", "pitch", "roll")
  for(m in orientation_metrics){
    # create empty plot
    plot(0, 0, type = "n", xlim = c(-1.1, 1.1), ylim = c(-1.1, 1.1), axes = FALSE, ann = FALSE, asp = 1)
    # add scale labels
    heading_angles <- c(0, pi/2, pi, -pi/2) - pi/2
    heading_labels <- c("N", "W", "S", "E")
    pitch_angles <- seq(-90, 90, by = 30)
    roll_angles <- seq(-150, 180, by = 30)
    if(m=="heading"){
      text(-1.2 * cos(heading_angles), -1.2 * sin(heading_angles), labels = heading_labels, cex = 0.7,  col = text.color)
    }else if (m=="pitch") {
      sapply(pitch_angles, function(a) text(-1.2 * cos(pi * a / 180), 1.2 * sin(pi * a / 180), labels = a, cex = 0.7, col = text.color))
    } else if (m == "roll") {
      sapply(roll_angles, function(a) text(-1.2 * cos(pi * a / 180), 1.2 * sin(pi * a / 180), labels = a, cex = 0.7, col = text.color))
    }
    # add circular border
    symbols(0, 0, circles = 1, inches = FALSE, add = TRUE, bg = adjustcolor("black", alpha.f = 0.8), fg = "white")
    # add intermediate degree markings
    if(m=="heading") sapply(heading_angles, function(rad) lines(c(0, -1.05 * cos(rad)), c(0, 1.05 * sin(rad)), col = "gray40", lwd = 0.5))
    if(m=="pitch") sapply(pitch_angles * pi/180, function(rad) lines(c(0, -1.05 * cos(rad)), c(0, 1.05 * sin(rad)), col = "gray40", lwd = 0.5))
    if(m=="roll") sapply(seq(-150, 180, by = 30) * pi/180, function(rad) lines(c(0, -1.05 * cos(rad)), c(0, 1.05 * sin(rad)), col = "gray40", lwd = 0.5))
    # draw the arrow/line indicators
    if(m=="heading"){
      heading_rad <- (90 - frame_data$heading) * pi / 180
      arrows(x0 = cos(heading_rad) * -0.6, y0 = sin(heading_rad) * -0.6, cos(heading_rad) * 0.6, sin(heading_rad) * 0.6,
             col = "red", lwd = 2, length = 0.14)
    }else if(m=="pitch"){
      pitch_rad <- pi * frame_data$pitch / 180
      arrows(x0 = 0.6 * cos(pitch_rad), y0 = -0.6 * sin(pitch_rad), x1 = -0.6 * cos(pitch_rad), y1 = 0.6 * sin(pitch_rad),
             col = "cyan3", lwd = 2, length = 0.14)
    }else if(m=="roll"){
      roll_rad <- pi * frame_data$roll / 180
      segments(x0 = 0.6 * cos(roll_rad), y0 = -0.6 * sin(roll_rad), x1 = -0.6 * cos(roll_rad), y1 = 0.6 * sin(roll_rad),
               col="cyan3", lwd = 2)
      points(x=0, y=0, pch=16, col="cyan3", cex=1.4)
    }
    # add title
    text(0, 1.8, labels = tools::toTitleCase(m), cex = 1.2, font = 2, xpd = NA, col = text.color)
    value <- switch(m, heading=frame_data$heading, pitch=frame_data$pitch, roll=frame_data$roll)
    value <- sprintf("%.1f\u00BA", value)
    text(0, 1.5, labels = value, cex = 1, font = 1, xpd = NA, col = sensor.val.color)
  }


  #################################################################
  # plot depth graph ##############################################

  # filter sensor_data for the time window
  window_data <- sensor_data[abs(as.numeric(difftime(sensor_data$datetime, current_time, units = "secs"))) <= depth.window, ]

  # add left margin
  par(mar = c(2, 3, 2, 1))

  # retrieve depth range
  depth_range <- c(max(window_data$depth), 0)

  # plot depth time series
  plot(window_data$datetime, window_data$depth, type = "n", xlab = "", xaxs="i", axes = FALSE, ylim = depth_range)
  polygon(
    x = c(window_data$datetime, rev(window_data$datetime)),
    y = c(window_data$depth, rep(par("usr")[4], nrow(window_data))),
    col = adjustcolor("blue4", alpha.f=0.3), border = NA)
  lines(window_data$datetime, window_data$depth, col = "black", lwd = 1)
  points(frame_data$datetime, frame_data$depth, col = "red", pch = 16, cex = 1.8)
  title(main = "Depth", line = 1.6, xpd = NA, cex.main = 1.2, col.main = text.color)
  title(main=sprintf("%.1f m", frame_data$depth), font.main=1, line=0.55, cex.main=1.05, xpd=NA, col.main=sensor.val.color)
  axis(side=2, at=pretty(depth_range), labels=pretty(depth_range), las=1, cex.axis=0.7, xpd=FALSE)
  box()


  #################################################################
  # plot VeDBA graph ##############################################

  # filter sensor_data for the time window
  window_data <- sensor_data[abs(as.numeric(difftime(sensor_data$datetime, current_time, units = "secs"))) <= vedba.window, ]

  # plot VeDBA time series
  plot(window_data$datetime, window_data$vedba, type = "n", xlab = "", xaxs="i",
       axes = FALSE, ylim = range(window_data$vedba))
  polygon(
    x = c(window_data$datetime, rev(window_data$datetime)),
    y = c(window_data$vedba, rep(par("usr")[3], nrow(window_data))),
    col = adjustcolor("white", alpha.f=0.5), border = NA)
  lines(window_data$datetime, window_data$vedba, col = "black", lwd = 1)
  segments(x0 = frame_data$datetime, y0 = par("usr")[3], y1 = frame_data$vedba, col = "red", lwd = 1.8)
  points(frame_data$datetime, frame_data$vedba, col = "red", pch = 16, cex = 1)
  title(main = "VeDBA", line = 1.6, xpd = NA, cex.main = 1.2,  col.main = text.color)
  title(main=sprintf("%.1f g", frame_data$vedba), font.main=1, line=0.55, cex.main=1.05, xpd=NA, col.main=sensor.val.color)
  axis(side=2, at=pretty(range(window_data$vedba)), labels=pretty(range(window_data$vedba)),
       las=1, cex.axis=0.7, xpd=FALSE)
  box()

  #################################################################
  # plot vertical speed graph #####################################

  # filter sensor_data for the time window
  window_data <- sensor_data[abs(as.numeric(difftime(sensor_data$datetime, current_time, units = "secs"))) <= vertical.speed.window, ]

  # plot vertical speed time series
  plot(window_data$datetime, window_data$vertical_speed, type = "n", xlab = "", xaxs="i",
       axes = FALSE, ylim = range(window_data$vertical_speed, na.rm = TRUE))
  polygon(
    x = c(window_data$datetime, rev(window_data$datetime)),
    y = c(window_data$vertical_speed, rep(par("usr")[3], nrow(window_data))),
    col = adjustcolor("white", alpha.f=0.5), border = NA)
  lines(window_data$datetime, window_data$vertical_speed, col = "black", lwd = 1)
  segments(x0 = frame_data$datetime, y0 = par("usr")[3], y1 = frame_data$vertical_speed, col = "red", lwd = 1.8)
  points(frame_data$datetime, frame_data$vertical_speed, col = "red", pch = 16, cex = 1)
  title(main = "Vertical Speed", line = 1.6, xpd = NA, cex.main = 1.2, col.main = text.color)
  title(main=sprintf("%.1f m/s", frame_data$vertical_speed), font.main=1, line=0.55, cex.main=1.05, xpd=NA, col.main=sensor.val.color)
  axis(side=2, at=pretty(range(window_data$vertical_speed, na.rm=TRUE)), labels=pretty(range(window_data$vertical_speed, na.rm=TRUE)),
       las=1, cex.axis=0.7, xpd=FALSE)
  box()


  #################################################################
  # add current timestamp #########################################

  # set margins
  par(mar = c(1, 0, 1, 0))

  # create empty plot
  plot(0, 0, type = "n", ylim = c(-1, 1), axes = FALSE, ann = FALSE, asp = 1)

  # calculate positions 10% and 25% from the left edge
  width_range <- par("usr")[2] - par("usr")[1]
  left_pos1 <- par("usr")[1] + width_range * 0.10
  left_pos2 <- par("usr")[1] + width_range * 0.35
  # add time as text annotation
  text(x = left_pos1, y = 0.2, labels = "Sensor Time:", cex = 1.3, xpd = NA, font = 2, adj=c(0,0))
  text(x = left_pos2, y = 0.2, labels = format(frame_data$datetime, "%H:%M:%OS3", tz="UTC"),
       col = text.color, cex = 1.1, xpd = NA, adj=c(0,0))


  #################################################################
  # close overlay png #############################################

  # close the PNG device
  dev.off()


  #################################################################
  # combine frame and overlay graphs ##############################

  # read the overlay image
  overlay_img <- magick::image_read(overlay_path)

  # set the width and height of the overlay image
  overlay_width <- 500
  overlay_height <- 750

  # resize the overlay image before compositing
  overlay_img_resized <- magick::image_scale(overlay_img, paste0(overlay_width, "x", overlay_height))

  # get the width and height of the frame (assuming `frame` is an image object)
  frame_width <- as.integer(magick::image_info(frame)$width)
  frame_height <- as.integer(magick::image_info(frame)$height)

  # Define dynamic offset based on overlay.side
  if (overlay.side == "left") {
    # position it 10px from the left and top
    offset <- "+10+10"
  } else if (overlay.side == "right") {
    # position it near the right side (adjust as needed for your frame size)
    offset <- paste0("+", as.integer(frame_width - overlay_width - 10), "+10")
  } else {
    # default: center the overlay if the side isn't recognized
    offset <- paste0("+", as.integer((frame_width - overlay_width) / 2), "+10")
  }

  # combine the original frame with the resized overlay using the dynamic offset
  combined_img <- magick::image_composite(frame, overlay_img_resized, operator = "over", offset = offset)

  # save the combined image
  magick::image_write(combined_img, final_frame_path)
}



#######################################################################################################
#######################################################################################################
#######################################################################################################
