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
                               tailbeat.window = 30,
                               pseudo.track.window = 30,
                               epsg.code = NULL,
                               text.color = "black",
                               sensor.val.color = "red3",
                               jpeg.quality = 3,
                               video.compression = "h265",
                               crf = 28,
                               n.cores = 1) {

  # Check for required packages
  if (!requireNamespace("magick", quietly = TRUE)) {
    stop("The 'magick' package is required. Please install it using install.packages('magick')")
  }

  # Check FFmpeg installation
  if (system("which ffmpeg", intern = TRUE) == "") {
    stop("FFmpeg is not installed. Please install FFmpeg to proceed.")
  }

  ##############################################################################
  # Input Validation ###########################################################
  ##############################################################################

  # Validate input file exists
  if (!file.exists(video.file)) stop("The specified video file does not exist.", call. = FALSE)

  # Validate metadata structure
  required_metadata_cols <- c("ID", "video", "start", "end", "frame_rate")
  missing_cols <- setdiff(required_metadata_cols, colnames(video.metadata))
  if (length(missing_cols) > 0) {
    stop("Missing required columns in video.metadata: ", paste(missing_cols, collapse = ", "), call. = FALSE)
  }

  # Validate sensor.data structure
  required_data_cols <- c("datetime", "depth", "heading", "pitch", "roll", "vedba", "vertical_speed")
  missing_cols <- setdiff(required_data_cols, colnames(sensor.data))
  if (length(missing_cols) > 0) {
    stop("Missing required columns in sensor.data: ", paste(missing_cols, collapse = ", "), call. = FALSE)
  }

  # Validate time parameters
  if (!is.null(end.time) && !is.null(duration)) {
    stop("Only one of 'end.time' or 'duration' can be provided, not both.", call. = FALSE)
  }

  # Set default start.time if not provided
  if (is.null(start.time)) start.time <- "00:00:00"

  ##############################################################################
  # Setup Directories and Paths ################################################
  ##############################################################################

  # Set default output directory if not specified
  if (is.null(output.directory)) {
    output.directory <- tempdir()
    # Create a more specific temp directory
    output.directory <- file.path(output.directory, "video_overlay_temp")
  }

  # Create unique directory for this processing job
  video_base <- tools::file_path_sans_ext(basename(video.file))
  output.directory <- file.path(output.directory, video_base)

  # Create necessary subdirectories
  overlay_directory <- file.path(output.directory, "overlays")
  dir.create(overlay_directory, recursive = TRUE, showWarnings = FALSE)

  # Check for existing files and prompt user
  if (dir.exists(overlay_directory) && length(list.files(overlay_directory)) > 0) {
    cat("Warning: Overlay directory already contains files. Existing files may be overwritten.\n")
    proceed <- readline("Proceed? (yes/no): ")
    if (!tolower(proceed) %in% c("yes", "y")) {
      cat("Processing cancelled.\n")
      return(invisible(NULL))
    }
    unlink(list.files(overlay_directory, full.names = TRUE))
  }

  ##############################################################################
  # Video Metadata Processing ##################################################
  ##############################################################################

  # Get video metadata
  selected_video <- video.metadata[video.metadata$video == basename(video.file), ]
  if (nrow(selected_video) == 0) {
    stop("No matching video found in video.metadata for file: ", basename(video.file))
  }

  frame_rate <- selected_video$frame_rate
  video_start <- selected_video$start

  # Calculate effective duration
  if (!is.null(end.time)) {
    start_seconds <- sum(as.numeric(strsplit(start.time, ":")[[1]]) * c(3600, 60, 1))
    end_seconds <- sum(as.numeric(strsplit(end.time, ":")[[1]]) * c(3600, 60, 1))
    effective_duration <- end_seconds - start_seconds
  } else if (!is.null(duration)) {
    effective_duration <- as.numeric(duration)
  } else {
    effective_duration <- selected_video$duration
  }

  # Calculate expected frame count
  frame_count <- ceiling(effective_duration * frame_rate)

  ##############################################################################
  # Sensor Data Alignment ######################################################
  ##############################################################################

  # Convert start time to elapsed seconds
  time_parts <- as.integer(strsplit(start.time, ":")[[1]])
  elapsed_secs <- time_parts[1] * 3600 + time_parts[2] * 60 + time_parts[3]
  video_start <- video_start + elapsed_secs

  # Filter sensor data to video time range
  sensor.data <- sensor.data[sensor.data$datetime >= video_start &
                               sensor.data$datetime <= (video_start + effective_duration), ]

  # Calculate frame numbers for each sensor reading
  sensor.data$time_diff <- as.numeric(difftime(sensor.data$datetime, video_start, units = "secs"))
  sensor.data$frame <- floor(sensor.data$time_diff * frame_rate) + 1

  ##############################################################################
  # Generate Overlay Frames (Parallelized) #####################################
  ##############################################################################

  cat("Generating overlay frames...\n")

  # Determine if we have pseudo track data
  has_pseudo_track <- all(c("pseudo_lat", "pseudo_lon", "dead_reckon_vx", "dead_reckon_vy", "dead_reckon_vz") %in% colnames(sensor.data))

  # Prepare for parallel processing
  if (n.cores > 1) {
    if (!requireNamespace("foreach", quietly = TRUE)) {
      warning("Parallel processing requires 'foreach' package. Falling back to single core.")
      n.cores <- 1
    } else {
      # define the `%dopar%` operator locally for parallel execution
      `%dopar%` <- foreach::`%dopar%`
      cl <- parallel::makeCluster(n.cores)
      doSNOW::registerDoSNOW(cl)
      on.exit(parallel::stopCluster(cl), add = TRUE)
    }
  }

  # Process frames
  if (n.cores == 1) {
    # Single-core processing
    pb <- txtProgressBar(min = 1, max = frame_count, style = 3)
    for (i in 1:frame_count) {
      .generateOverlayFrame(i, video_start, frame_rate, sensor.data, overlay_directory,
                            overlay.side, depth.window, vedba.window, vertical.speed.window,
                            tailbeat.window, pseudo.track.window, has_pseudo_track,
                            text.color, sensor.val.color, epsg.code)
      setTxtProgressBar(pb, i)
    }
    close(pb)
  } else {
    # Parallel processing
    foreach::foreach(i = 1:frame_count, .export=".generateOverlayFrame", .packages = c("magick", "grDevices")) %dopar% {
      .generateOverlayFrame(i, video_start, frame_rate, sensor.data, overlay_directory,
                            overlay.side, depth.window, vedba.window, vertical.speed.window,
                            tailbeat.window, pseudo.track.window, has_pseudo_track,
                            text.color, sensor.val.color, epsg.code)
    }
  }

  ##############################################################################
  # Composite Overlays with FFmpeg #############################################
  ##############################################################################

  cat("Compositing overlays with FFmpeg...\n")

  # Determine overlay position
  position <- switch(overlay.side,
                     "left" = "10:10",
                     "right" = "main_w-overlay_w-10:10",
                     "(main_w-overlay_w)/2:10")

  # Determine video codec
  video_codec <- ifelse(video.compression == "h265",
                        ifelse(system("ffmpeg -codecs 2>&1 | grep hevc_videotoolbox", intern = TRUE) != "",
                               "hevc_videotoolbox", "libx265"),
                        "libx264")

  # Normalize paths for FFmpeg
  video_path <- normalizePath(video.file)
  overlay_path_pattern <- normalizePath(file.path(overlay_directory, "overlay_%06d.png"))
  output_path <- normalizePath(output.file, mustWork = FALSE)

  # Build FFmpeg command
  ffmpeg_command <- sprintf(
    'ffmpeg -y -i "%s" -i "%s" -filter_complex "[0:v][1:v] overlay=%s" -c:a copy -c:v %s -preset fast -crf %d -pix_fmt yuv420p "%s"',
    video_path, overlay_path_pattern, position, video_codec, crf, output_path
  )

  # Execute FFmpeg command
  system(ffmpeg_command)

  # Verify output file was created
  if (!file.exists(output.file)) {
    stop("Failed to create output video. Check FFmpeg output for errors.")
  }

  cat(sprintf("\nVideo successfully created: %s\n", output.file))
}


# Helper function to generate a single overlay frame
.generateOverlayFrame <- function(frame_num, video_start, frame_rate, sensor_data,
                                  overlay_directory, overlay.side, depth.window,
                                  vedba.window, vertical.speed.window, tailbeat.window,
                                  pseudo.track.window, has_pseudo_track, text.color,
                                  sensor.val.color, epsg.code) {

  # Calculate current time for this frame
  current_time <- video_start + (frame_num - 1) / frame_rate

  # Find closest sensor data row
  closest_row <- which.min(abs(sensor_data$frame - frame_num))
  frame_data <- sensor_data[closest_row, ]

  # Create overlay image path
  overlay_path <- file.path(overlay_directory, sprintf("overlay_%06d.png", frame_num))

  # Open PNG device
  if (has_pseudo_track) {
    png(overlay_path, width = 500, height = 1200, bg = "transparent", res = 100)
  } else {
    png(overlay_path, width = 500, height = 1000, bg = "transparent", res = 100)
  }

  # Set up plot layout
  if (has_pseudo_track) {
    layout(matrix(c(1,2,3,4,4,4,5,5,5,6,6,6,7,7,7,8,8,8,9,9,9), nrow=7, byrow=TRUE),
           heights=c(2.5, 1.4, 1.4, 1.4, 1.4, 3.2, 0.3))
  } else {
    layout(matrix(c(1,2,3,4,4,4,5,5,5,6,6,6,7,7,7,8,8,8), nrow=6, byrow=TRUE),
           heights=c(2,1,1,1,1,1))
  }

  # Set plot margins
  par(mar = c(1, 1, 1, 1), oma = c(0, 0, 1, 0), xpd = NA)


  #################################################################
  # Plot orientation dials ########################################

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
  # Plot depth graph ##############################################

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
  # Plot VeDBA graph ##############################################

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
  # Plot vertical speed graph #####################################

  # filter sensor_data for the time window
  window_data <- sensor_data[abs(as.numeric(difftime(sensor_data$datetime, current_time, units = "secs"))) <= vertical.speed.window, ]

  # create positive and negative subsets for different colors
  positive_speed <- window_data$vertical_speed >= 0
  negative_speed <- window_data$vertical_speed < 0

  # set plotting limits with inverted Y-axis
  y_range <- range(window_data$vertical_speed, na.rm = TRUE)
  ylim_vals <- rev(y_range)

  # create empty plot
  plot(window_data$datetime, window_data$vertical_speed, type = "n", ylim = ylim_vals,
       axes = FALSE, xlab = "", ylab = "", xaxs = "i", yaxs = "i")
  usr <- par("usr")

  # add polygon for positive values (below zero, since they represent descent)
  if (any(positive_speed, na.rm = TRUE)) {
    pos_data <- window_data[positive_speed, ]
    # ensure that the polygon goes from y = 0 to the plot's upper limit (usr[4])
    polygon(x = c(usr[1], as.numeric(pos_data$datetime), usr[2]),
            y = c(pmax(0, usr[4]), pos_data$vertical_speed, c(pmax(0, usr[4]))),
            col = adjustcolor("salmon", alpha.f = 0.4), border = NA)
  }

  # add polygon for negative values (above zero, since they represent ascent)
  if (any(negative_speed, na.rm = TRUE)) {
    neg_data <- window_data[negative_speed, ]
    # ensure that the polygon goes from y = 0 to the plot's lower limit (usr[3])
    polygon(x = c(usr[1], as.numeric(neg_data$datetime), usr[2]),
            y = c(min(0, usr[3]), neg_data$vertical_speed, min(0, usr[3])),
            col = adjustcolor("cyan2", alpha.f = 0.4), border = NA)
  }

  # add zero line if it falls within the plotting area
  if (0 >= usr[4] && 0 <= usr[3]) {
    segments(x0 = usr[1], y0 = 0, x1 = usr[2], y1 = 0, col = "black", lty = 2)
  }

  # plot vertical speed time series
  lines(window_data$datetime, window_data$vertical_speed, col = "black", lwd = 1)

  # add vertical line for current speed
  if(frame_data$vertical_speed>0){
    segments(x0 = frame_data$datetime, y0 = max(0, usr[4]), y1 = frame_data$vertical_speed, col = "red", lwd = 1.8)
  }else{
    segments(x0 = frame_data$datetime, y0 = min(0, usr[3]), y1 = frame_data$vertical_speed, col = "red", lwd = 1.8)
  }
  points(frame_data$datetime, frame_data$vertical_speed, col = "red", pch = 16, cex = 1)
  title(main = "Vertical Speed", line = 1.6, xpd = NA, cex.main = 1.2, col.main = text.color)
  title(main=sprintf("%.1f m/s", frame_data$vertical_speed), font.main=1, line=0.55, cex.main=1.05, xpd=NA, col.main=sensor.val.color)
  axis(side=2, at=pretty(range(window_data$vertical_speed, na.rm=TRUE)), labels=sprintf("%.1f", pretty(range(window_data$vertical_speed, na.rm=TRUE))),
       las=1, cex.axis=0.7, xpd=FALSE)
  box()


  #################################################################
  # Plot tail beat frequency graph ################################

  # filter sensor_data for the time window
  window_data <- sensor_data[abs(as.numeric(difftime(sensor_data$datetime, current_time, units = "secs"))) <= tailbeat.window, ]

  # set up plot area
  plot(window_data$datetime, window_data$tbf_hz, type = "n", ylim = range(0, window_data$tbf_hz, na.rm = TRUE),
       xlab = "", ylab = "", xaxs = "i", yaxs = "i", axes = FALSE)

  # get user coordinates for the plot
  usr <- par("usr")

  # run-length encoding to identify valid (non-NA) sections
  rle_data <- rle(!is.na(window_data$tbf_hz))

  if(any(rle_data$values)){
    index <- 1
    # loop through the runs to plot polygons for each valid segment
    for (i in 1:length(rle_data$lengths)) {
      # Only consider runs of valid data (length > 0 and TRUE for non-NA)
      if (rle_data$values[i]) {
        # get the indices for the valid data run
        valid_indices <- index:(index + rle_data$lengths[i] - 1)
        # extract valid data points
        valid_data <- window_data[valid_indices, ]
        # draw polygon for the valid section of data
        polygon(x = c(valid_data$datetime[1], valid_data$datetime, valid_data$datetime[length(valid_data$datetime)]),
                y = c(0, valid_data$tbf_hz, 0), col = adjustcolor("white", alpha.f = 0.5), border = "black")
        # update the index to the next section
        index <- index + rle_data$lengths[i]
      } else {
        # skip over the missing values in the data
        index <- index + rle_data$lengths[i]
      }
    }
  }

  # only add current point if not NA
  if (!is.na(frame_data$tbf_hz)) {
    segments(x0 = frame_data$datetime, y0 = 0, y1 = frame_data$tbf_hz, col = "red", lwd = 1.8)
    points(frame_data$datetime, frame_data$tbf_hz, col = "red", pch = 16, cex = 1)
    title(main = sprintf("%.1f Hz", frame_data$tbf_hz), font.main = 1, line = 0.55, cex.main = 1.05, xpd = NA, col.main = sensor.val.color)
  }

  # add titles and axes
  title(main = "Tail Beat Frequency", line = 1.6, xpd = NA, cex.main = 1.2, col.main = text.color)
  axis(side = 2, at = pretty(c(0, max(window_data$tbf_hz, na.rm = TRUE))), labels = sprintf("%.2f",pretty(c(0, max(window_data$tbf_hz, na.rm = TRUE)))),
       las = 1, cex.axis = 0.7, xpd = FALSE)
  box()



  #################################################################
  # Plot 3D pseudo track if available #############################

  if(has_pseudo_track) {

    # calculate depth range from complete track (not just window)
    depth_range <- range(sensor_data$depth, na.rm = TRUE)

    # filter data for the moving window
    window_data <- sensor_data[abs(as.numeric(difftime(sensor_data$datetime, current_time, units = "secs"))) <= pseudo.track.window, ]

    # only plot if we have data in the window
    if(nrow(window_data) > 0) {

      # set margins
      par(mar = c(0, 0, 0.5, 0), bg = "transparent")

      # find current index based on time
      current_idx <- which.min(abs(as.numeric(window_data$datetime - current_time)))

      # convert to sf object and project coordinates
      pseudo_coords <- data.frame(lon = window_data$pseudo_lon, lat = window_data$pseudo_lat)
      pseudo_sf <- sf::st_as_sf(pseudo_coords, coords = c("lon", "lat"), crs = 4326)
      pseudo_proj <- sf::st_transform(pseudo_sf, crs = epsg.code)
      xy_proj <- sf::st_coordinates(pseudo_proj)
      x_proj <- xy_proj[, 1]
      y_proj <- xy_proj[, 2]

      # relative coordinates for plotting
      rel_lon <- x_proj - x_proj[current_idx]
      rel_lat <- y_proj - y_proj[current_idx]
      rel_depth <- -window_data$pseudo_depth

      # calculate ranges for each axis
      x_range <- range(rel_lon, na.rm = TRUE)
      y_range <- range(rel_lat, na.rm = TRUE)
      z_range <- range(rel_depth, na.rm = TRUE)

      # find the largest range among x, y, z
      max_range <- max(diff(x_range), diff(y_range), diff(z_range))

      # adjust all axes to span the same range (centered on their midpoints)
      x_mid <- mean(x_range)
      y_mid <- mean(y_range)
      z_mid <- mean(z_range)

      # apply uniform scaling with padding (e.g., 1.2 for 20% padding)
      padding <- 1.2
      xlim <- x_mid + max_range * c(-0.5, 0.5) * padding
      ylim <- y_mid + max_range * c(-0.5, 0.5) * padding
      zlim <- z_mid + max_range * c(-0.5, 0.5) * padding

      # ensure depth increases downward (if needed)
      zlim <- sort(zlim, decreasing = TRUE)

      # calculate current heading (convert from compass bearing to math angle)
      #current_heading <- (90 - frame_data$heading) %% 360  # Convert to math angle (0=east, 90=north)
      #theta_angle <- current_heading  # This will control the pan (horizontal rotation)
      theta_angle <-  (90 - frame_data$heading) %% 360

      # fixed pitch angle (35 degrees is a good default for 3D perspective)
      phi_angle <- 35

      # generate color palette using complete track range
      depth_pal <- rev(.viridis_pal(100))
      #depth_pal <- colorRampPalette(c("blue", "cyan", "yellow"))(100)
      depth_colors <- depth_pal[cut(window_data$depth, breaks = seq(depth_range[1], depth_range[2], length.out = 101),
                                    include.lowest = TRUE)]


      ##############################################################
      # create empty plot with custom view #########################

      # draw empty persp box
      pmat <- plot3D::perspbox(x = rel_lon[current_idx], y = rel_lat[current_idx], z = rel_depth[current_idx],
                               xlab = "Longitude", ylab = "Latitude", zlab = "Depth",
                               xlim = xlim, ylim = ylim, zlim = zlim, box = FALSE,
                               theta = theta_angle, phi = phi_angle,
                               colkey = FALSE)

      # function to draw panels
      panelfunc <- function(x, y, z) {
        XY <- plot3D::trans3D(x, y, z, pmat = pmat)
        polygon(XY$x, XY$y, col = adjustcolor("black", alpha.f = 0.3), border = "black", lwd = 1.5)
      }

      # bottom panel (always static)
      panelfunc(
        x = c(xlim[1], xlim[1], xlim[2], xlim[2]),
        y = c(ylim[1], ylim[2], ylim[2], ylim[1]),
        z = rep(zlim[2], 4)
      )

      # left and back panels: switch based on theta_angle
      # determine "left" and "back" based on heading quadrant
      theta_normalized <- theta_angle %% 360
      theta_normalized <- theta_angle
      if (theta_normalized >= 45 && theta_normalized < 135) {
        # Heading ~ East: Left = South, Back = West
        panelfunc(x = rep(xlim[1], 4), y = c(ylim[1], ylim[1], ylim[2], ylim[2]), z = c(zlim[1], zlim[2], zlim[2], zlim[1])) # Left (South)
        panelfunc(x = c(xlim[1], xlim[1], xlim[2], xlim[2]), y = rep(ylim[1], 4), z = c(zlim[1], zlim[2], zlim[2], zlim[1])) # Back (West)
      } else if (theta_normalized >= 135 && theta_normalized < 225) {
        # Heading ~ South: Left = West, Back = North
        panelfunc(x = rep(xlim[1], 4), y = c(ylim[1], ylim[1], ylim[2], ylim[2]), z = c(zlim[1], zlim[2], zlim[2], zlim[1])) # Left (West)
        panelfunc(x = c(xlim[1], xlim[1], xlim[2], xlim[2]), y = rep(ylim[2], 4), z = c(zlim[1], zlim[2], zlim[2], zlim[1])) # Back (North)
      } else if (theta_normalized >= 225 && theta_normalized < 315) {
        # Heading ~ West: Left = North, Back = East
        panelfunc(x = rep(xlim[2], 4), y = c(ylim[1], ylim[1], ylim[2], ylim[2]), z = c(zlim[1], zlim[2], zlim[2], zlim[1])) # Left (North)
        panelfunc(x = c(xlim[1], xlim[1], xlim[2], xlim[2]), y = rep(ylim[2], 4), z = c(zlim[1], zlim[2], zlim[2], zlim[1])) # Back (East)
      } else {
        # Heading ~ North: Left = East, Back = South
        panelfunc(x = rep(xlim[2], 4), y = c(ylim[1], ylim[1], ylim[2], ylim[2]), z = c(zlim[1], zlim[2], zlim[2], zlim[1])) # Left (East)
        panelfunc(x = c(xlim[1], xlim[1], xlim[2], xlim[2]), y = rep(ylim[1], 4), z = c(zlim[1], zlim[2], zlim[2], zlim[1])) # Back (South)
      }


      ##############################################################
      # add bottom compass and cardinal marks #######################

      # compass settings
      compass_center_x <- mean(xlim)
      compass_center_y <- mean(ylim)
      compass_radius <- min(diff(xlim), diff(ylim)) * 0.5  # reach edge
      z_bottom <- zlim[2]

      # create full circle at bottom plane
      angle_seq <- seq(0, 2 * pi, length.out = 200)
      circle_x <- compass_center_x + compass_radius * cos(angle_seq)
      circle_y <- compass_center_y + compass_radius * sin(angle_seq)
      circle_z <- rep(z_bottom, length(angle_seq))

      # draw the circle (compass background)
      plot3D::polygon3D(x = circle_x, y = circle_y, z = circle_z,
                        col = rgb(0, 0, 0, 0.05), border = NA, add = TRUE)

      # Define angles for cardinal directions (in radians)
      north_angle <- pi / 2    # 90°
      east_angle <- 0          # 0°
      south_angle <- 3 * pi /2 # 270°
      west_angle <- pi         # 180°

      tick_length <- compass_radius * 0.1

      # function to add a tick mark and label
      add_compass_mark <- function(angle, label, color = "black") {
        # start and end points for tick mark
        tick_x1 <- compass_center_x + compass_radius * cos(angle)
        tick_y1 <- compass_center_y + compass_radius * sin(angle)
        tick_z1 <- z_bottom
        tick_x0 <- compass_center_x + (compass_radius - tick_length) * cos(angle)
        tick_y0 <- compass_center_y + (compass_radius - tick_length) * sin(angle)
        tick_z0 <- z_bottom

        # draw the tick mark
        plot3D::segments3D(x0 = tick_x0, y0 = tick_y0, z0 = tick_z0,
                           x1 = tick_x1, y1 = tick_y1, z1 = tick_z1,
                           col = color, lwd = 3, add = TRUE)

        # add label just outside the circle
        label_offset <- compass_radius * 0.25
        label_x <- compass_center_x + (compass_radius + label_offset) * cos(angle)
        label_y <- compass_center_y + (compass_radius + label_offset) * sin(angle)
        label_z <- z_bottom
        plot3D::text3D(x = label_x, y = label_y, z = label_z, labels = label,
                       add = TRUE, col = color, cex = 1)
      }

      # add all four cardinal directions
      add_compass_mark(north_angle, "N", "red")  # North (special color)
      add_compass_mark(east_angle, "E")          # East
      add_compass_mark(south_angle, "S")         # South
      add_compass_mark(west_angle, "W")          # West


      ##############################################################
      # add traveled path with solid colors ########################
      if(current_idx > 1) {
        plot3D::segments3D(
          x0 = rel_lon[1:(current_idx-1)],
          y0 = rel_lat[1:(current_idx-1)],
          z0 = rel_depth[1:(current_idx-1)],
          x1 = rel_lon[2:current_idx],
          y1 = rel_lat[2:current_idx],
          z1 = rel_depth[2:current_idx],
          col = depth_colors[1:(current_idx-1)],
          lwd = 3, add = TRUE
        )
      }

      ##############################################################
      # add upcoming path with transparency ########################
      if(current_idx < nrow(window_data)) {
        plot3D::segments3D(
          x0 = rel_lon[current_idx:(nrow(window_data)-1)],
          y0 = rel_lat[current_idx:(nrow(window_data)-1)],
          z0 = rel_depth[current_idx:(nrow(window_data)-1)],
          x1 = rel_lon[(current_idx+1):nrow(window_data)],
          y1 = rel_lat[(current_idx+1):nrow(window_data)],
          z1 = rel_depth[(current_idx+1):nrow(window_data)],
          col = adjustcolor(depth_colors[current_idx:(nrow(window_data)-1)], alpha.f = 0.3),
          lwd = 3, add = TRUE
        )
      }

      ##############################################################
      # add animal position as red point ###########################
      plot3D::points3D(x = rel_lon[current_idx], y = rel_lat[current_idx], z = rel_depth[current_idx],
                       col = "red", pch = 19, cex = 2.4, add = TRUE, alpha = 1)


      ##############################################################
      # add title ##################################################
      title(main = "Pseudo Track", line = 0.2, cex.main = 1.2, col.main = text.color)
    }

  }

  #################################################################
  # add current timestamp #########################################

  # set margins
  par(mar = c(0, 0, 1, 0))

  # create empty plot
  plot(0, 0, type = "n", ylim = c(-1, 1), axes = FALSE, ann = FALSE, asp = 1)

  # calculate positions 10% and 25% from the left edge
  width_range <- par("usr")[2] - par("usr")[1]
  left_pos1 <- par("usr")[1] + width_range * 0.10
  left_pos2 <- par("usr")[1] + width_range * 0.32
  # add time as text annotation
  text(x = left_pos1, y = 0.2, labels = "Sensor Time:", cex = 1.1, xpd = NA, font = 2, adj=c(0,0))
  text(x = left_pos2, y = 0.2, labels = format(frame_data$datetime, "%H:%M:%OS3", tz="UTC"),
       col = text.color, cex = 0.9, xpd = NA, adj=c(0,0))


  #################################################################
  # close overlay png #############################################


  # Close device
  dev.off()
}
