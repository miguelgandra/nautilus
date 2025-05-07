#######################################################################################################
# Function to import and process archival tag data ####################################################
#######################################################################################################

#' Import and Process Archival Tag Data
#'
#' This function imports and processes archival tag data from specified directories, supporting
#' sensor data for multiple individuals. It utilizes the fast `data.table::fread` function to
#' efficiently import CSV files. The function automatically computes several key metrics related to
#' acceleration, orientation, and linear motion based on accelerometer, magnetometer, and gyroscope time series
#' (see the Details section below for a list of metrics).
#' Additionally, the function can automatically integrate and merge positions obtained from Wildlife Computers tags
#' (e.g., MiniPATs, MK10s, and SPOT tags). The tag data should be included in a separate
#' folder within each individual's directory for correct integration. To handle large datasets, the function provides
#' the option to downsample the data to a specified frequency, reducing the data resolution and volume after the metrics
#' are calculated. This is particularly useful when working with high-frequency data from sensors, making it more
#' manageable for further analysis.
#'
#' @param data.folders Character vector. Paths to the folders containing data to be processed.
#' Each folder corresponds to an individual animal and should contain subdirectories with sensor data and possibly PSAT data.
#' @param sensor.subdirectory Character. Name of the subdirectory within each animal folder that contains sensor data (default: "CMD").
#' This subdirectory should include the sensor CSV files for the corresponding animal.
#' @param wc.subdirectory Character or NULL. Name of the subdirectory within each animal folder that contains Wildlife Computers tag data
#' (e.g., MiniPAT, MK10, or SPOT tag data), or NULL to auto-detect tag folders (default: NULL).
#' This subdirectory should contain the "Locations.csv" file with position data from the tag.
#' @param wc.subdirectory Character or NULL. Name of the subdirectory within each animal folder that contains PSAT data, or NULL to auto-detect PSAT folders (default: NULL).
#' This subdirectory should contain the "Location.csv" file with any fastloc position data from the PSAT.
#' @param save.files Logical. If `TRUE`, the processed data for each ID will be saved as RDS files
#' during the iteration process. This ensures that progress is saved incrementally, which can
#' help prevent data loss if the process is interrupted or stops midway. Default is `FALSE`.
#' @param output.folder Character. Path to the folder where the processed files will be saved.
#' This parameter is only used if `save.files = TRUE`. If `NULL`, the RDS file will be saved
#' in the data folder corresponding to each ID. Default is `NULL`.
#' @param output.suffix Character. A suffix to append to the file name when saving.
#' This parameter is only used if `save.files = TRUE`. If `NULL`, a suffix based on the sampling
#' rate (e.g., `-100Hz`) will be used. Default is `NULL`.
#' @param id.metadata Data frame. Metadata about the IDs to associate with the processed data.
#' Must contain at least columns for ID and tag type.
#' @param id.col Character. Column name for ID in `id.metadata` (default: "ID").
#' @param tag.col Character. Column name for the tag type in `id.metadata` (default: "tag").
#' @param lon.col Character. Column name for longitude in sensor data (default: "lon").
#' @param lat.col Character. Column name for latitude in sensor data (default: "lat").
#' @param tagdate.col Character. Column name for the tagging date in `id.metadata` (default: "tagging_date").
#' @param axis.mapping Optional. A data frame containing the axis transformations for the IMU (Inertial Measurement Unit).
#' This parameter is used to correctly configure the IMU axes to match the North-East-Down (NED) frame.
#' The data frame should have three columns:
#' \itemize{
#'   \item \strong{type}: A column specifying the tag type (`CAM` vs `CMD`).
#'   \item \strong{tag}: A column specifying the tag or sensor identifier. The tags indicated in this column should match the tag types in the \code{id.metadata} data frame.
#'   \item \strong{from}: A column indicating the original axis in the sensor's coordinate system.
#'   \item \strong{to}: A column specifying the target axis in the desired coordinate system.
#' }
#' Both signal and swap transformations are allowed. Transformations can be defined for different tags in case multiple tags were used.
#' @param sensor.smoothing.factor Optional. A factor that determines the size of the moving window (in timesteps)
#' used to smooth raw sensor signals, including acceleration, gyroscope, and magnetometer data.
#' The window size is calculated as `sampling.frequence / smoothing.factor`. A smaller value results
#' in greater smoothing. Set to NULL to disable smoothing. Defaults to 4.
#' @param dba.window Integer. Window size (in seconds) for calculating dynamic body acceleration. Defaults to 3.
#' @param dba.smoothing Optional. Smoothing window (in seconds) for VeDBA/ODBA metrics.
#' Uses arithmetic mean. Set to NULL to disable. Default: 2.
#' @param orientation.smoothing Optional. Smoothing window (in seconds) for orientation metrics (roll, pitch, heading).
#' Uses circular mean. Set to NULL to disable. Default: 1.
#' @param motion.smoothing Optional. Smoothing window (in seconds) for linear motion metrics (surge, sway, heave).
#' Uses arithmetic mean. Set to NULL to disable. Default: 1.
#' @param speed.smoothing Optional. Smoothing window (in seconds) for vertical speed.
#' Uses arithmetic mean. Set to NULL to disable. Default: 2.
#' @param burst.quantiles Numeric vector. Quantiles (0-1) to define burst swimming events based on acceleration thresholds.
#' Use NULL to disable burst detection. Defaults to c(0.95, 0.99) (95th and 99th percentiles).
#' @param downsample.to Numeric. Downsampling frequency in Hz (e.g., 1 for 1 Hz) to reduce data resolution.
#' Use NULL to retain the original resolution. Defaults to 1.
#' @param verbose Logical. If TRUE, the function will print detailed processing information. Defaults to TRUE.
#'
#' @details
#' The following key metrics are automatically calculated:
#'
#' \strong{Acceleration:}
#' \itemize{
#'   \item Total Acceleration: (g) - The total magnitude of the animal's acceleration, calculated from the three orthogonal accelerometer components.
#'   \item Vectorial Dynamic Body Acceleration (VeDBA): (g) Quantifies the physical acceleration of the animal, calculated as the vector magnitude of the dynamic body acceleration, which is the difference between raw accelerometer data and the moving average (static acceleration).
#'   \item Overall Dynamic Body Acceleration (ODBA): (g) A scalar measure of the animal's overall acceleration, calculated as the sum of the absolute values of the dynamic acceleration components along the X, Y, and Z axes.
#'   \item Burst Swimming Events: Identifies periods of high acceleration based on a given acceleration magnitude percentile, which can be used to detect burst swimming behavior. This metric is binary, indicating whether the acceleration exceeds the threshold.
#' }
#'
#' \strong{Orientation:}
#' \itemize{
#'   \item Roll: (degrees) The rotational movement of the animal around its longitudinal (x) axis, calculated using accelerometer and gyroscope data.
#'   \item Pitch: (degrees) The rotational movement of the animal around its lateral (y) axis, computed from accelerometer and gyroscope measurements.
#'   \item Heading: (degrees) The directional orientation of the animal, derived from the magnetometer data, representing the compass heading.
#'         The heading is corrected according to the magnetic declination value estimated based on the tagging location of each animal to account for local magnetic variation.
#' }
#'
#' \strong{Linear motion:}
#' \itemize{
#'   \item Surge: (g) The forward-backward linear movement of the animal along its body axis, derived from the accelerometer data.
#'   \item Sway: (g) The side-to-side linear movement along the lateral axis of the animal, also derived from the accelerometer data.
#'   \item Heave: (g) The vertical linear movement of the animal along the vertical axis, estimated from accelerometer data.
#'   \item Vertical Speed: (m/s) The vertical velocity of the animal, representing the speed at which the animal is moving vertically in the water column.
#'   Calculated as the difference in depth measurements divided by the time interval
#' }
#'
#'
#' @return A list where each element contains the processed sensor data for an individual folder.
#'         If PSAT data exists, additional processing for PSAT locations will be included.
#' @export


processTagData <- function(data.folders,
                           sensor.subdirectory = "CMD",
                           wc.subdirectory = NULL,
                           save.files = FALSE,
                           output.folder = NULL,
                           output.suffix = NULL,
                           id.metadata,
                           id.col = "ID",
                           tag.col = "tag",
                           lon.col = "lon",
                           lat.col = "lat",
                           tagdate.col = "tagging_date",
                           axis.mapping = NULL,
                           sensor.smoothing.factor = 5,
                           dba.window = 3,
                           dba.smoothing = 2,
                           orientation.smoothing = 1,
                           motion.smoothing = 1,
                           speed.smoothing = 2,
                           burst.quantiles = c(0.95, 0.99),
                           downsample.to = 1,
                           verbose = TRUE) {


  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

  # measure running time
  start.time <- Sys.time()

  # validate data.folders argument
  if(!is.character(data.folders)) stop("`data.folders` must be a character vector.", call. = FALSE)

  # check if the provided data.folders exist
  missing_folders <- data.folders[!dir.exists(data.folders)]
  if (length(missing_folders) > 0) {
    stop(paste0("The following folders were not found: ", paste(missing_folders, collapse = ", ")), call. = FALSE)
  }

  # ensure sensor.subdirectory is a single string
  if(!is.character(sensor.subdirectory) || length(sensor.subdirectory) != 1) {
    stop("`sensor.subdirectory` must be a single string.", call. = FALSE)
  }

  # ensure wc.subdirectory is either NULL or a single string
  if(!is.null(wc.subdirectory) && (!is.character(wc.subdirectory) || length(wc.subdirectory) != 1)) {
    stop("`wc.subdirectory` must be NULL or a single string.", call. = FALSE)
  }

  # ensure output.folder is a single string
  if(!is.null(output.folder) && (!is.character(output.folder) || length(output.folder) != 1)) {
    stop("`output.folder` must be NULL or a single string.", call. = FALSE)
  }

  # check if the output folder is valid (if specified)
  if(!is.null(output.folder) && !dir.exists(output.folder)){
    stop("The specified output folder does not exist. Please provide a valid folder path.", call. = FALSE)
  }

  # validate `id.metadata` argument
  if(!is.data.frame(id.metadata)) stop("`id.metadata` must be a data frame.", call. = FALSE)

  # check if specified columns exists in id.metadata
  if(!id.col %in% names(id.metadata)) stop(paste("The specified id.col ('", id.col, "') was not found in id.metadata.", sep = ""))
  if(!tag.col %in% names(id.metadata)) stop(paste("The specified tag.col ('", tag.col, "') was not found in id.metadata.", sep = ""))
  if(!lon.col %in% names(id.metadata)) stop(paste("The specified lon.col ('", lon.col, "') was not found in id.metadata.", sep = ""))
  if(!lat.col %in% names(id.metadata)) stop(paste("The specified lat.col ('", lat.col, "') was not found in id.metadata.", sep = ""))
  if(!tagdate.col %in% names(id.metadata)) stop(paste("The specified tagdate.col ('", tagdate.col, "') was not found in id.metadata.", sep = ""))


  # validate orientation.mapping
  if(!is.null(axis.mapping)){
    if (!all(c("type", "tag", "from", "to") %in% colnames(axis.mapping))) {
      stop("The 'axis.mapping' data frame must contain 'type', 'tag', 'from', and 'to' columns.", call. = FALSE)
    }else {
      if (any(!axis.mapping$tag %in% id.metadata[[tag.col]])) warning("Some tags in axis.mapping do not match tags in id.metadata.", call. = FALSE)
      if (any(!id.metadata[[tag.col]] %in% axis.mapping$tag)) {
        missing_tags <- setdiff(id.metadata[[tag.col]], axis.mapping$tag)
        warning_msg <- paste0("Warning: The following tag types in id.metadata are not present in axis.mapping: ",
                              paste(missing_tags, collapse = ", "), ". No axis transformations will be performed for these tag types.\n")
      }
    }
  }

  # validate smoothing and moving window parameters
  if(!is.numeric(sensor.smoothing.factor)) stop("`sensor.smoothing.factor` must be a numeric value.", call. = FALSE)
  if(!is.numeric(dba.window) || dba.window <= 0) stop("`dba.window` must be a positive numeric value.", call. = FALSE)
  if(!is.numeric(orientation.smoothing) || orientation.smoothing <= 0) stop("`orientation.smoothing` must be a positive numeric value.", call. = FALSE)
  if(!is.numeric(motion.smoothing) || motion.smoothing <= 0) stop("`motion.smoothing` must be a positive numeric value.", call. = FALSE)
  if(!is.numeric(speed.smoothing) || speed.smoothing <= 0) stop("`speed.smoothing` must be a positive numeric value.", call. = FALSE)
  if(!is.numeric(dba.smoothing) || dba.smoothing <= 0) stop("`dba.smoothing` must be a positive numeric value.", call. = FALSE)

  # validate `burst.quantiles`
  if (!is.numeric(burst.quantiles) || any(burst.quantiles <= 0) || any(burst.quantiles > 1)) {
    stop("`burst.quantiles` must be a numeric vector with values in the range (0, 1].", call. = FALSE)
  }

  # feedback for save files mode
  if (!is.logical(save.files)) stop("`save.files` must be a logical value (TRUE or FALSE).", call. = FALSE)

  # feedback for verbose mode
  if (!is.logical(verbose)) stop("`verbose` must be a logical value (TRUE or FALSE).", call. = FALSE)


  ##############################################################################
  # Retrieve directory files ###################################################
  ##############################################################################

  # validate folder animal IDs against id.metadata
  folder_ids <- basename(data.folders)
  missing_ids <- setdiff(folder_ids, id.metadata[[id.col]])
  if(length(missing_ids) > 0) {
    stop(paste0("\nThe following folder IDs were not found in 'id.metadata': ",
                       paste(missing_ids, collapse = ", "),
                       ".\nTo proceed, you can either:\n",
                       "1. Fix the issue by ensuring these folder IDs exist in 'id.metadata'.\n",
                       "2. Exclude these folders from the data processing by modifying the 'data.folders' accordingly.\n"), call. = FALSE)
  }

  # identify sensor data files (.csv) in the sensor folder for each directory
  data_files <- sapply(data.folders, function(x) {
    files <- list.files(file.path(x, sensor.subdirectory), full.names = TRUE, pattern = "\\.csv$")
    if (length(files) > 0) files[1] else NA
  })
  names(data_files) <- basename(data.folders)


  # check whether any sensor file is missing
  missing_folders <- names(data_files)[is.na(data_files)]
  if (length(missing_folders) > 0) {
    # print warning for missing sensor files and ask the user if they want to proceed
    prompt_msg <- paste0(crayon::red$bold("Warning:\n"),
                        "The following folders are missing sensor files and will be skipped:\n",
                        crayon::yellow$bold(paste0(missing_folders, collapse = ", ")), "\n",
                        "Do you want to proceed anyway? (yes/no)\n")
    cat(prompt_msg)
    # capture user input
    proceed <- readline()
    # convert input to lowercase and check if it is negative
    if(!(tolower(proceed) %in% c("yes", "y"))) {
      # exit the function if the user decides not to proceed
      cat("Processing cancelled.\n")
      return(invisible(NULL))
    }
  }

  # identify Wildlife Computers tag folders or use the specified wc.subdirectory parameter
  if (is.null(wc.subdirectory)) {
    # list and filter subdirectories for each main directory in data.folders
    wc_folders <- lapply(data.folders, function(main_dir) {
      # list immediate subdirectories
      subdirs <- list.dirs(main_dir, full.names = TRUE, recursive = FALSE)
      # filter subdirectories containing a 'Locations' file
      subdirs[sapply(subdirs, function(subdir) {any(grepl("Locations.csv", list.files(subdir)))})]
    })
  } else {
    # build full paths for the specified wc.subdirectory
    wc_folders <- lapply(data.folders, function(main_dir) file.path(main_dir, wc.subdirectory))
  }

  # locate locations files within the Wildlife Computers folders (e.g., "Locations.csv")
  wc_files <- sapply(wc_folders, function(x) {
    files <- list.files(x, full.names = TRUE, pattern = "Locations\\.csv$")
    if (length(files) > 0) files[1] else NA
  })
  names(wc_files) <- basename(data.folders)



  ##############################################################################
  # Initialize variables #######################################################
  ##############################################################################

  # create lists to store processed data, plots, and summaries for each animal
  n_animals <- length(data.folders)
  data_list <- vector("list", length = n_animals)
  data_plots <- vector("list", length = n_animals)
  summary_list <- vector("list", length = n_animals)

  # specify the column names to import
  import_cols <- c("Date (UTC)", "Time (UTC)", "Accelerometer X [m/s\xb2]", "Accelerometer Y [m/s\xb2]",
                   "Accelerometer Z [m/s\xb2]", "Gyroscope X [mrad/s]", "Gyroscope Y [mrad/s]", "Gyroscope Z [mrad/s]",
                   "Magnetometer X [\xb5T]", "Magnetometer Y [\xb5T]", "Magnetometer Z [\xb5T]", "Temperature (imu) [\xb0C]",
                   "Temperature (depth) [\xb0C]", "Temp. (magnet.) [\xb0C]", "Depth (200bar) 1 [m]", "Depth (200bar) [m]", "Camera time")


  ##############################################################################
  # Process data for each folder ###############################################
  ##############################################################################

  # feedback messages for the user
  cat(paste0(
    crayon::bold("\n============= Processing Tag Data =============\n"),
    "Fetching data from ", n_animals, " ", ifelse(n_animals == 1, "folder", "folders"), " - hang on tight!\n",
    crayon::bold("===============================================\n\n")
  ))

  # print folder names if verbose mode is enabled
  if(verbose){
    cat(crayon::blue(paste0("- ", basename(data.folders), collapse = "\n")), "\n\n")
  }

  # print warning
  if (exists("warning_msg")) message(warning_msg)

  # iterate over each animal
  for (i in seq_along(data_files)) {

    # get animal ID
    id <- basename(data.folders)[i]

    # print progress to the console
    cat(sprintf("[%d] %s\n", i, id))

    # retrieve sensor and WC locations file paths for the current animal
    sensor_file <- data_files[i]
    locations_file <- wc_files[i]

    # check if the sensor file exists
    if (is.na(sensor_file) || !file.exists(sensor_file)) {
      cat("Data file missing. Skipping.\n\n")
      data_list[[i]] <- NA
      next
    }

    # retrieve metadata for the current animal ID from the metadata table
    animal_info <- id.metadata[id.metadata[[id.col]]==id,]

    # load sensor data from CSV
    sensor_data <- suppressWarnings(data.table::fread(sensor_file, select=import_cols, showProgress=TRUE))

    # check if the sensor data file is empty or is missing columns
    if(nrow(sensor_data)==0 || ncol(sensor_data)<13){
      cat("Data format not recognized. Skipping.\n\n")
      data_list[[i]] <- NA
      warning(paste(id, "- Sensor data file is either empty or does not contain recognized column headers. No data was imported."), call. = FALSE)
      next
    }

    # output dataset size (rounded to the nearest thousand)
    if (verbose) {
      # get the number of rows in the dataset
      rows <- nrow(sensor_data)
      if (rows >= 1e6) {
        # check if the dataset has more than a million rows
        result <- paste(format(round(rows / 1e6, 1), nsmall = 1), "M")
      } else if (rows >= 1e3) {
        # check if the dataset has more than a thousand rows (but less than a million)
        result <- paste(format(round(rows / 1e3, 1), nsmall = 1), "K")
      } else {
        # for smaller datasets (less than a thousand rows), just display the raw number
        result <- as.character(rows)
      }
      # print the result
      cat("Total rows:", result, "\n")
    }

    # check if the "Camera time" column exists
    if ("Camera time" %in% colnames(sensor_data)) {
      # find the first index where "Camera time" is greater than zero
      camera_start <- which(sensor_data$`Camera time` > 0)[1]
      # remove the "Camera time" column from the dataset
      sensor_data[, "Camera time" := NULL]
    }else{
      camera_start <- NULL
    }

    # rename columns
    data.table::setnames(sensor_data, c("date", "time", "ax", "ay", "az", "gx", "gy", "gz", "mx", "my", "mz", "temp", "depth"))

    # convert datetime to POSIXct
    sensor_data[, datetime := as.POSIXct(paste(date, time), format = "%d.%m.%Y %H:%M:%OS", tz = "UTC") + 0.0001]

    # extract sample number
    sensor_data[, sample := as.numeric(substr(time, 10, 11))]

    # correct negative depths
    sensor_data[depth < 0, depth := 0]

    # add ID column
    sensor_data[, ID := id]

    # if camera_start is not NULL, extract the corresponding datetime
    if (!is.null(camera_start)) {camera_start <- sensor_data[camera_start, datetime]}

    # process WC location data if available
    if (!is.na(locations_file) && file.exists(locations_file)) {
      # provide feedback to the user if verbose mode is enabled
      if(verbose) cat("Importing location data from Wildlife Computers tag...\n")
      # import locations data
      locs_data <- data.table::fread(wc_files[i], select = c("Ptt", "Date", "Type", "Latitude", "Longitude", "Quality"))
      locs_data[, Date := as.POSIXct(Date, format = "%H:%M:%OS %d-%b-%Y", tz = "UTC")]
      data.table::setnames(locs_data, c("PTT", "timebin", "position_type", "lat", "lon", "quality"))
      # remove fractional seconds from PSAT locs datetimes
      locs_data[, timebin := as.POSIXct(floor(as.numeric(timebin)), origin = "1970-01-01", tz = "UTC")]
      # remove rows with duplicate 'timebin' values
      locs_data <- locs_data[!duplicated(timebin)]
      # add "timebin" column with datetime rounded to the nearest second (floor)
      sensor_data[, timebin := as.POSIXct(floor(as.numeric(datetime)), origin = "1970-01-01", tz = "UTC")]
      # merge PSAT positions with sensor data
      sensor_data <- merge(sensor_data, locs_data, by = "timebin", all.x = TRUE)
      # remove the "timebin" column
      sensor_data[, timebin := NULL]
    } else {
      # create empty columns for "PTT", "datetime", "type", "lat", "lon" with NA values
      sensor_data[, `:=`(PTT = NA, position_type = NA, lat = NA, lon = NA, quality = NA)]
    }


    ############################################################################
    # Apply axis.mapping transformations #######################################
    ############################################################################

    # retrieve tag model from metadata
    tag_model <- animal_info[[tag.col]]

    # proceed only if axis.mapping is provided and tag_model is valid
    if (!is.null(axis.mapping) && !is.na(tag_model)) {

      # check if tag_model exists in axis.mapping
      if(tag_model %in% axis.mapping$tag){

        # extract the unique types from the data frame
        types <- unique(axis.mapping$type)

        # check which type is present in the ID name (if no match, assign the default type)
        tag_type <- types[sapply(types, function(x) grepl(x, id))]
        if (length(tag_type) == 0) {tag_type <- "CMD"}

        # provide feedback to the user if verbose mode is enabled
        if(verbose) cat(paste("Applying axis mapping for tag:", tag_model, tag_type, "\n"))

        # filter the axis.mapping for the current tag type and model
        tag_mapping <- axis.mapping[axis.mapping$tag == tag_model & axis.mapping$type == tag_type,]

        # create a temporary copy of the data for simultaneous swaps
        temp_data <- data.table::copy(sensor_data)

        # change axis designation and direction to match the NED system
        for (row in 1:nrow(tag_mapping)) {
          from_axis <- tag_mapping$from[row]
          to_axis <- tag_mapping$to[row]
          if (grepl("^\\-", to_axis)) {
            # sign change
            axis_name <- sub("^\\-", "", to_axis)
            temp_data[[axis_name]] <- -sensor_data[[from_axis]]
          } else {
            # swap axes
            temp_data[[to_axis]] <- sensor_data[[from_axis]]
          }
        }

        # update the sensor data with the swapped values
        sensor_data <- data.table::copy(temp_data)
      }
    }


    ############################################################################
    # Smooth sensor signals (optional for noise reduction) #####################
    ############################################################################

    # calculate sampling frequency
    sampling_freq <- nrow(sensor_data)/length(unique(lubridate::floor_date(sensor_data$datetime, "sec")))
    sampling_freq <- plyr::round_any(sampling_freq, 5)

    # smooth signals using a moving average
    if(!is.null(sensor.smoothing.factor)) {

      # provide feedback to the user if verbose mode is enabled
      if(verbose) cat("Smoothing sensor signals...\n")

      # set window size
      smoothing_window <- round(sampling_freq / sensor.smoothing.factor)

      # smooth raw accelerometer values
      sensor_data[, ax := data.table::frollmean(ax, n = smoothing_window, fill = NA, align = "center")]
      sensor_data[, ay := data.table::frollmean(ay, n = smoothing_window, fill = NA, align = "center")]
      sensor_data[, az := data.table::frollmean(az, n = smoothing_window, fill = NA, align = "center")]

      # smooth raw gyroscope values
      sensor_data[, gx := data.table::frollmean(gx, n = smoothing_window, fill = NA, align = "center")]
      sensor_data[, gy := data.table::frollmean(gy, n = smoothing_window, fill = NA, align = "center")]
      sensor_data[, gz := data.table::frollmean(gz, n = smoothing_window, fill = NA, align = "center")]

      # smooth magnetometer values
      sensor_data[, mx := data.table::frollmean(mx, n = smoothing_window, fill = NA, align = "center")]
      sensor_data[, my := data.table::frollmean(my, n = smoothing_window, fill = NA, align = "center")]
      sensor_data[, mz := data.table::frollmean(mz, n = smoothing_window, fill = NA, align = "center")]
   }

    ############################################################################
    # Calculate acceleration metrics ###########################################
    ############################################################################

    # provide feedback to the user if verbose mode is enabled
    if(verbose) cat("Calculating acceleration metrics...\n")

    # convert acceleration axes from m/2 to Gs
    if(tag_model != "CEIIA"){
      sensor_data[, ax := ax / 9.80665]
      sensor_data[, ay := ay / 9.80665]
      sensor_data[, az := az / 9.80665]
      if(verbose) cat(paste0("Acceleration values converted from m/s", "\U00B2", " to g.\n"))
    }

    # calculate total acceleration
    sensor_data[, accel := sqrt(ax^2 + ay^2 + az^2)]

    # calculate dynamic and vectorial body acceleration using a moving window
    # doi: 10.3354/ab00104

    # calculate window parameters
    window_size <- dba.window * sampling_freq
    pad_length <- ceiling(window_size / 2)

    # define a reusable padding function - centered rolling mean with edge padding to avoid NAs
    .pad_rollmean <- function(x, window, pad_len) {
      # symmetric padding using first/last values
      padded <- c(rep(x[1], pad_len), x, rep(x[length(x)], pad_len))
      # compute centered rolling mean on padded data
      rolled <- data.table::frollmean(padded, n = window, align = "center", na.rm = TRUE)
      # trim to original length
      rolled[(pad_len + 1):(length(x) + pad_len)]
    }

    # calculate static (low-frequency) acceleration using padded rolling mean
    staticX = .pad_rollmean(sensor_data$ax, window_size, pad_length)
    staticY = .pad_rollmean(sensor_data$ay, window_size, pad_length)
    staticZ = .pad_rollmean(sensor_data$az, window_size, pad_length)

    # calculate dynamic (high-frequency) acceleration by removing static component
    dynamicX <- sensor_data$ax - staticX
    dynamicY <- sensor_data$ay - staticY
    dynamicZ <- sensor_data$az - staticZ

    # calculate ODBA (Overall Dynamic Body Acceleration) and VeDBA (Vectorial DBA)
    sensor_data[, `:=`(
      odba = abs(dynamicX) + abs(dynamicY) + abs(dynamicZ),
      vedba = sqrt(dynamicX^2 + dynamicY^2 + dynamicZ^2)
    )]

    # smooth the signals using a moving average (optional for noise reduction)
    if(!is.null(dba.smoothing)){
      window_size <- sampling_freq * dba.smoothing
      sensor_data[, odba := data.table::frollmean(odba, n = window_size, fill = NA, align = "center")]
      sensor_data[, vedba := data.table::frollmean(vedba, n = window_size, fill = NA, align = "center")]
    }

    # estimate burst swimming events (based on specified percentiles)
    if(!is.null(burst.quantiles)){
      for(q in burst.quantiles){
        accel_threshold <- quantile(sensor_data$accel, probs = q, na.rm = TRUE)
        burst_col <- paste0("burst", q*100)
        sensor_data[, (burst_col) := as.integer(accel >= accel_threshold)]
      }
    }

    ############################################################################
    # Calculate linear motion metrics ##########################################
    ############################################################################

    # provide feedback to the user if verbose mode is enabled
    if (verbose) cat("Calculating linear motion metrics...\n")

    # calculate surge
    # motion along the longitudinal (X) axis (forward/backward swimming)
    sensor_data[, surge := ax - staticX]

    # calculate sway
    # motion along the lateral (Y) axis (side-to-side swaying)
    sensor_data[, sway := ay - staticY]

    # calculate heave
    # motion along the vertical (Z) axis (up and down, often from diving or wave action)
    sensor_data[, heave := az - staticZ]

    # calculate vertical speed
    sensor_data[, vertical_speed := c(NA, diff(depth) / as.numeric(diff(datetime), units = "secs"))]

    # smooth the signals using a moving average (optional for noise reduction)
    if(!is.null(motion.smoothing)){
      window_size <- sampling_freq * motion.smoothing
      sensor_data[, surge := data.table::frollmean(surge, n = window_size, fill = NA, align = "center")]
      sensor_data[, sway := data.table::frollmean(sway, n = window_size, fill = NA, align = "center")]
      sensor_data[, heave := data.table::frollmean(heave, n = window_size, fill = NA, align = "center")]
    }
    if(!is.null(speed.smoothing)){
      window_size <- sampling_freq * speed.smoothing
      sensor_data[, vertical_speed := data.table::frollmean(vertical_speed, n = window_size, fill = NA, align = "center")]
    }


    ############################################################################
    # Calculate orientation metrics ############################################
    ############################################################################

    # provide feedback to the user if verbose mode is enabled
    if (verbose) cat("Calculating orientation metrics...\n")

    # calculate roll and pitch angles
    sensor_data[, roll := atan2(ay, az)]
    sensor_data[, pitch := atan2(-ax, sqrt(ay^2 + az^2))]

    # correct the magnetometer readings using the roll and pitch angles (tilt-compensated magnetic field vector)
    mx_comp <- sensor_data$mx*cos(sensor_data$pitch) + sensor_data$mz*sin(sensor_data$pitch)
    my_comp <- sensor_data$mx*sin(sensor_data$roll)*sin(sensor_data$pitch) + sensor_data$my*cos(sensor_data$roll) - sensor_data$mz*sin(sensor_data$roll)*cos(sensor_data$pitch)

    # convert roll and pitch from radians to degrees
    sensor_data[, roll := roll * (180 / pi)]
    sensor_data[, pitch := pitch * (180 / pi)]

    # calculate the heading and convert from radians to degrees
    sensor_data[, heading := atan2(my_comp, mx_comp) * (180 / pi)]

    # get magnetic declination value (in degrees)
    declination_deg <- oce::magneticField(longitude=animal_info[[lon.col]], latitude=animal_info[[lat.col]], time=animal_info[[tagdate.col]])$declination
    declination_deg <- round(declination_deg, 2)

    # correct for magnetic declination to get true heading and ensure it's in the 0-359 range
    sensor_data[, heading := heading + declination_deg]
    sensor_data[, heading := ifelse(heading < 0, heading + 360, heading)]
    sensor_data[, heading := ifelse(heading >= 360, heading - 360, heading)]

    # apply a moving circular mean to smooth the metrics time series
    if(!is.null(orientation.smoothing)) {
      window_size <- sampling_freq * orientation.smoothing
      sensor_data[, roll := .rollingCircularMean(roll, window = window_size, range = c(-180, 180) )]
      sensor_data[, pitch := .rollingCircularMean(pitch, window = window_size,  range = c(-90, 90))]
      sensor_data[, heading := .rollingCircularMean(heading, window = window_size, range = c(0, 360))]
    }


    ############################################################################
    # Downsample data ##########################################################
    ############################################################################

    # select columns to keep
    metrics <- c("temp","depth","ax", "ay", "az", "gx", "gy", "gz", "mx", "my", "mz", "accel","odba","vedba","roll", "pitch", "heading", "surge", "sway", "heave", "vertical_speed")
    position_cols <- c("PTT", "position_type", "lat", "lon", "quality")

    # store current sampling frequency
    sampling_rate <- sampling_freq

    # if a downsampling rate is specified, aggregate the data to the defined frequency (in Hz)
    if(!is.null(downsample.to)){

      # check if the specified downsampling frequency matches the dataset's sampling frequency
      if (downsample.to == sampling_freq) {
        if (verbose) cat("Dataset sampling rate is already", downsample.to, "Hz. Skipping downsampling.\n")
        processed_data <- sensor_data

      # check if the specified downsampling frequency exceeds the dataset's sampling frequency
      } else if(downsample.to > sampling_freq) {
        if (verbose) cat("Warning: Dataset sampling rate (", sampling_freq, "Hz) is lower than the specified downsampling rate (", downsample.to, "Hz). Skipping downsampling.\n", sep = "")
        processed_data <- sensor_data

      # start downsampling
      } else {

        # provide feedback to the user if verbose mode is enabled
        if (verbose)  cat("Downsampling data to", downsample.to, "Hz...\n")

        # store new sampling frequency
        sampling_rate <- downsample.to

        # convert the desired downsample rate to time interval in seconds
        downsample_interval <- 1 / downsample.to

        # round datetime to the nearest downsample interval
        sensor_data[, datetime := floor(as.numeric(datetime) / downsample_interval) * downsample_interval]
        sensor_data[, datetime := as.POSIXct(datetime, origin = "1970-01-01", tz = "UTC")]

        # temporarily suppress console output (redirect to a temporary file)
        sink(tempfile())

        #  aggregate data into the specified interval
        processed_data <- sensor_data[, lapply(.SD, function(x) if (is.numeric(x)) mean(x, na.rm = TRUE) else data.table::first(x)),
                                      by = datetime, .SDcols = c(metrics, position_cols)]

        # re-add ID column
        processed_data[, ID := id]

        # restore normal output
        sink()

        # sum burst swimming events (based on specified percentiles)
        if(!is.null(burst.quantiles)){
          burst_cols <- paste0("burst", burst.quantiles * 100)
          processed_bursts <- sensor_data[, lapply(.SD, sum, na.rm = TRUE), by = datetime, .SDcols = burst_cols]
          # combine the two aggregated datasets
          processed_data <- merge(processed_data, processed_bursts, by = "datetime", all.x = TRUE)
        }
      }

    } else{
      # if no downsampling rate is defined, return the original sensor data
      processed_data <- sensor_data
    }

    # reorder columns: ID, metrics, burst.quantiles (if exists), and position_cols
    data.table::setcolorder(processed_data, c("ID", "datetime", metrics, if(!is.null(burst.quantiles)) paste0("burst", burst.quantiles * 100), position_cols))


    ############################################################################
    # Check for temporal discontinuities #######################################
    ############################################################################

    # save original start and end datetimes
    first_datetime <- min(processed_data$datetime)
    last_datetime <- max(processed_data$datetime)

    # check for temporal discontinuities
    processed_data <- checkTimeGaps(data = processed_data,
                                    time.diff.threshold = 0.01,
                                    verbose = verbose)


    ############################################################################
    # Store processed data #####################################################
    ############################################################################

    # provide feedback to the user if verbose mode is enabled
    if (verbose) cat("Done! Data processed successfully.\n")


    # define sensor-specific rounding rules (units in brackets)
    rounding_specs <- list(
      # raw sensor data
      accelerometer = list(vars = c("ax", "ay", "az"), digits = 4),
      gyroscope = list(vars = c("gx", "gy", "gz"), digits = 2),
      magnetometer = list(vars = c("mx", "my", "mz"), digits = 2),
      # processed metrics
      temperature = list(vars = "temp", digits = 2),
      depth = list(vars = "depth", digits = 2),
      dynamics = list(vars = c("accel", "odba", "vedba"), digits = 3),
      orientation = list(vars = c("roll", "pitch", "heading"), digits = 2),
      movement = list(vars = c("surge", "sway", "heave"), digits = 4),
      velocity = list(vars = "vertical_speed", digits = 2)
    )

    # apply rounding to save memory
    for (group in rounding_specs) {
      processed_data[, (group$vars) := lapply(.SD, round, digits = group$digits),
                     .SDcols = group$vars]
    }

    # convert NaN to NA
    processed_data[, (names(processed_data)) := lapply(.SD, function(x) {x[is.nan(x)] <- NA; return(x)})]

    # create new attributes to save relevant variables
    attr(processed_data, 'directory') <- data.folders[i]
    attr(processed_data, 'id') <- id
    attr(processed_data, 'first.datetime') <- first_datetime
    attr(processed_data, 'last.datetime') <- last_datetime
    attr(processed_data, 'original.rows') <- rows
    attr(processed_data, 'original.sampling.frequency') <- sampling_freq
    attr(processed_data, 'processed.sampling.frequency') <- sampling_rate
    attr(processed_data, 'magnetic.declination') <- declination_deg
    attr(processed_data, 'sensor.smoothing.factor') <- sensor.smoothing.factor
    attr(processed_data, 'dba.window') <- dba.window
    attr(processed_data, 'dba.smoothing') <- dba.smoothing
    attr(processed_data, 'orientation.smoothing') <- orientation.smoothing
    attr(processed_data, 'motion.smoothing') <- motion.smoothing
    attr(processed_data, 'speed.smoothing') <- speed.smoothing
    attr(processed_data, 'time.diff.threshold') <- formals(checkTimeGaps)$time.diff.threshold
    attr(processed_data, 'camera.start') <- camera_start
    attr(processed_data, 'processing.date') <- Sys.time()

    # save the processed data as an RDS file
    if(save.files){

      # determine the output directory: use the specified output folder or the current data folder
      output_dir <- ifelse(!is.null(output.folder), output.folder, data.folders[i])

      # define the file suffix: use the specified suffix or default to a suffix based on the sampling rate
      sufix <- ifelse(!is.null(output.suffix), output.suffix, paste0("-", sampling_rate, "Hz"))

      # construct the output file name
      output_file <- file.path(output_dir, paste0(id, sufix, ".rds"))

      # save the processed data
      saveRDS(processed_data, output_file)
      if (verbose) cat(paste0("File saved: ", paste0(id, sufix, ".rds"), "\n"))
    }

    # print empty line
    cat("\n")

    # store processed sensor data in the list
    data_list[[i]] <- processed_data

    # clear unused objects from the environment to free up memory
    rm(sensor_data)
    rm(processed_data)
    gc()
  }


  ##############################################################################
  # Return processed data ######################################################
  ##############################################################################

  # print time taken
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  cat(crayon::bold("Total execution time:"), sprintf("%.02f", as.numeric(time.taken)), base::units(time.taken), "\n\n")

  # assign names to the data list
  names(data_list) <- basename(data.folders)

  # convert NA placeholders back to NULL
  na_indices <- which(sapply(data_list, function(x) identical(x, NA)))
  for (index in na_indices) {data_list[[index]] <- NULL}

  # return the list containing processed sensor data for all folders
  return(data_list)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################

