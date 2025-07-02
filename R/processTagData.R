#######################################################################################################
# Function to process archival tag data ###############################################################
#######################################################################################################

#' Process Archival Tag Data
#'
#' @description
#' This function processes high-resolution archival tag data, automatically computing a
#' wide range of kinematic and orientation metrics from accelerometer, magnetometer,
#' and gyroscope signals (see the *Details* section below for a complete list).
#'
#' Orientation is estimated by default using the tilt-compensated compass method, which fuses accelerometer
#' and magnetometer data to determine body orientation relative to gravity and magnetic north.
#' Optionally, a more advanced sensor fusion approach using the Madgwick filter can be applied.
#'
#' A full 3D magnetic calibration can be applied prior to orientation estimation, including both
#' hard iron (offset) and soft iron (scaling and misalignment) corrections.
#'
#' For tags equipped with a magnetic paddle wheel, the function can also estimate swimming speed.
#' This is achieved by extracting the dominant rotation frequency from the magnetometer's
#' z-axis signal using a Fast Fourier Transform (FFT), which is then converted
#' to speed using a tag-specific calibration slope
#'
#' After metric computation, the data can be downsampled to reduce its resolution and
#' size for downstream analysis.
#'
#' Note: Python and the associated libraries `AHRS` and `numpy` (accessible via `reticulate`) are required
#' if orientation is estimated using the Madgwick filter.
#'
#' @param data A list of data.tables/data.frames, one for each individual; a single aggregated data.table/data.frame
#' containing data from multiple animals (with an 'ID' column); or a character vector of file paths pointing to
#' `.rds` files, each containing data for a single individual. When a character vector is provided,
#' files are loaded sequentially to optimize memory use. The output of the \link{importTagData} function
#' is strongly recommended, as it formats the data appropriately for all downstream analysis.
#' @param downsample.to Numeric. Downsampling frequency in Hz (e.g., 1 for 1 Hz) to reduce data resolution.
#' Use NULL to retain the original resolution. Defaults to 1.
#' @param hard.iron.calibration Logical. Whether to apply hard-iron calibration (offset correction) to magnetometer data.
#' Default is TRUE.
#' @param soft.iron.calibration Logical. Whether to apply soft-iron calibration (scaling and misalignment correction) to magnetometer data.
#' Default is TRUE.
#' @param orientation.algorithm Orientation estimation algorithm:
#'  \itemize{
#'    \item \code{"tilt_compass"} (default): Lightweight 6-axis tilt-compensated compass.
#'    \item \code{"madgwick"}: High-accuracy 9-axis sensor fusion. Requires Python \code{AHRS} module.
#'  }
#' @param madgwick.beta Numeric. The Madgwick filter's gain parameter (default: 0.02).
#' This parameter controls the trade-off between gyroscope and accelerometer measurements.
#'   \itemize{
#'     \item Higher values (e.g., 0.2-0.3) trust the accelerometer more, leading to faster convergence but potentially more noise
#'.    \item Lower values (e.g., 0.01-0.05) trust the gyroscope more, resulting in smoother but potentially slower convergence
#'   }
#' Only used when \code{orientation.algorithm = "madgwick"}.
#' @param orientation.smoothing Optional. Smoothing window (in seconds) for orientation metrics (roll, pitch, heading).
#' Uses circular mean. Set to NULL to disable. Default: 1.
#' @param correct.pitch.offset A logical value (TRUE/FALSE) indicating whether to apply a pitch
#' offset correction based on the relationship between pitch angle and vertical speed.
#' Defaults to FALSE. If TRUE, a linear regression of pitch (in radians) against
#' `vertical_speed` is performed, and the intercept (pitch at 0 m/s vertical speed)
#' is subtracted from all pitch estimates. This method is adapted from Kawatsu et al. (2010)
#' to account for potential tag misalignment.
#' @param pitch.warning.threshold Numeric. Threshold (in degrees) for median pitch values that trigger orientation warnings.
#' Default: 45 (will warn if median |pitch| > 45 degrees).
#' @param roll.warning.threshold Numeric. Threshold (in degrees) for median roll values that trigger orientation warnings.
#' Default: 45 (will warn if median |roll| > 45 degrees).
#' @param dba.window Integer. Window size (in seconds) for calculating dynamic body acceleration. Defaults to 3.
#' @param dba.smoothing Optional. Smoothing window (in seconds) for VeDBA/ODBA metrics.
#' Uses arithmetic mean. Set to NULL to disable. Default: 2.
#' @param motion.smoothing Optional. Smoothing window (in seconds) for linear motion metrics (surge, sway, heave).
#' Uses arithmetic mean. Set to NULL to disable. Default: 1.
#' @param speed.smoothing Optional. Smoothing window (in seconds) for vertical speed.
#' Uses arithmetic mean. Set to NULL to disable. Default: 2.
#' @param calculate.paddle.speed Logical. If TRUE and the tag was equipped with a paddle wheel,
#' estimates animal speed based on paddle wheel rotation frequency. Default is FALSE.
#' @param speed.calibration.values A data.frame containing paddle wheel calibration values for speed estimation.
#' Must contain at least three columns:
#'  \itemize{
#'    \item \code{year}: The year the calibration was performed (integer)
#'    \item \code{package_id}: The package identifier matching the tag's attribute (character)
#'    \item \code{slope}: The calibration slope value (numeric)
#'  }
#' This parameter is only used when \code{calculate.paddle.speed = TRUE}. Default is NULL.
#' @param burst.quantiles Numeric vector. Quantiles (0-1) to define burst swimming events based on acceleration thresholds.
#' Use NULL to disable burst detection. Defaults to c(0.95, 0.99) (95th and 99th percentiles).
#' @param return.data Logical. Controls whether the function returns the processed data
#' as a list in memory. When processing large or numerous datasets, set to \code{FALSE} to reduce
#' memory usage. Note that either \code{return.data} or \code{save.files} must be \code{TRUE}
#' (or both). Default is \code{TRUE}.
#' @param save.files Logical. If `TRUE`, the processed data for each ID will be saved as RDS files
#' during the iteration process. This ensures that progress is saved incrementally, which can
#' help prevent data loss if the process is interrupted or stops midway. Default is `FALSE`.
#' @param output.folder Character. Path to the folder where the processed files will be saved.
#' This parameter is only used if `save.files = TRUE`. If `NULL`, the RDS file will be saved
#' in the data folder corresponding to each ID. Default is `NULL`.
#' @param output.suffix Character. A suffix to append to the file name when saving.
#' This parameter is only used if `save.files = TRUE`.
#' @param data.table.threads Integer or NULL. Specifies the number of threads
#' that data.table should use for parallelized operations. NULL (default): Uses data.table's current default threading.
#' Notes:
#' \itemize{
#'    \item Optimal thread count depends on your CPU cores and data size
#'    \item More threads use more RAM but can significantly speed up large operations
#'    \item Can be permanently set via \code{data.table::setDTthreads()}
#'    \item Current thread count: \code{data.table::getDTthreads()}
#'  }
#' @param verbose Logical. If TRUE, the function will print detailed processing information. Defaults to TRUE.

#' @details
#' This function computes a suite of movement and orientation metrics from high-frequency tri-axial sensor data,
#' including acceleration, orientation, and linear motion parameters. It also performs automatic magnetic calibration
#' to improve heading estimation.
#'
#' \strong{Acceleration:}
#' \itemize{
#'   \item Total Acceleration (g): The total magnitude of the animal's acceleration, calculated from the three orthogonal accelerometer components.
#'   \item Vectorial Dynamic Body Acceleration (VeDBA) (g): Quantifies the physical acceleration of the animal, calculated as the vector magnitude of the dynamic body acceleration, which is the difference between raw accelerometer data and the moving average (static acceleration).
#'   \item Overall Dynamic Body Acceleration (ODBA) (g): A scalar measure of the animal's overall acceleration, calculated as the sum of the absolute values of the dynamic acceleration components along the X, Y, and Z axes.
#'   \item Burst Swimming Events: Identifies periods of high acceleration based on a given acceleration magnitude percentile, which can be used to detect burst swimming behavior. This metric is binary, indicating whether the acceleration exceeds the threshold.
#' }
#'
#' \strong{Orientation:}
#'
#' Computed using sensor fusion algorithms:
#' \itemize{
#'   \item {Tilt-compensated compass} (default): A lightweight 6-axis fusion (accelerometer + magnetometer)
#'   to compute roll, pitch, and heading. The method first calculates tilt angles from accelerometer data,
#'   then compensates the magnetometer readings using these angles to compute a more accurate heading
#'   (as described in Gunner et al., 2021). This approach avoids gyroscope drift but may be affected by magnetic disturbances.
#'   \item {Madgwick filter}: A 9-axis fusion algorithm (accelerometer + gyroscope + magnetometer)
#'   implementing Sebastian Madgwick's quaternion-based gradient descent
#'   approach. This provides absolute orientation reference by incorporating
#'   Earth's magnetic field and is more robust to transient disturbances,
#'   at the cost of higher computational complexity.
#' }
#' Output includes:
#' \itemize{
#'   \item Roll (degrees): Rotational movement of the animal around its longitudinal (x) axis.
#'   \item Pitch (degrees): Rotational movement of the animal around its lateral (y) axis.
#'   \item Heading (degrees): Directional orientation of the animal, representing the compass heading.
#'         Heading is corrected based on the deployment coordinates using global geomagnetic declination models.
#' }
#'
#' \strong{Linear Motion:}
#' \itemize{
#'   \item Surge (g): The forward-backward linear movement of the animal along its body axis, derived from the accelerometer data.
#'   \item Sway (g): The side-to-side linear movement along the lateral axis of the animal, also derived from the accelerometer data.
#'   \item Heave (g): The vertical linear movement of the animal along the vertical axis, estimated from accelerometer data.
#'   \item Vertical Speed (m/s): The vertical velocity of the animal, representing the speed at which the animal is moving vertically in the water column.
#'   Calculated as the difference in depth measurements divided by the time interval
#' }
#'
#' \strong{Python Requirements}
#' \itemize{
#'   \item Requires the \code{reticulate} R package for interfacing with Python.
#'   \item The active Python environment must include the \code{AHRS} and \code{numpy} modules.
#'   \item If unavailable, the function will return an informative error with installation guidance.
#' }
#'
#' @return If \code{return.data = TRUE}, returns a list where each element contains the
#' processed sensor data for an individual folder. If \code{return.data = FALSE},
#' returns \code{NULL} invisibly. In all cases, data will be saved to disk if
#' \code{save.files = TRUE}.
#'
#' @references
#' Gunner RM, Holton MD, Scantlebury MD, *et al.* (2021) Dead-reckoning animal
#' movements in R: a reappraisal using Gundog. *Animal Biotelemetry*. 9:1–37.
#' \doi{10.1186/s40317-021-00245-z}
#'
#' Kawatsu S, Sato K, Watanabe Y, Hyodo S, Breves JP, Fox BK, *et al.* (2009).
#' A new method to calibrate attachment angles of data loggers in swimming sharks.
#' *EURASIP Journal on Advances in Signal Processing*. 2010, 732586.
#' \doi{10.1155/2010/732586}
#'
#' @seealso \link{importTagData}, \link{filterDeploymentData}.
#' @export


processTagData <- function(data,
                           downsample.to = 1,
                           hard.iron.calibration = TRUE,
                           soft.iron.calibration = TRUE,
                           orientation.algorithm = "tilt_compass",
                           madgwick.beta = 0.02,
                           orientation.smoothing = 1,
                           correct.pitch.offset = TRUE,
                           pitch.warning.threshold = 45,
                           roll.warning.threshold = 45,
                           dba.window = 3,
                           dba.smoothing = 2,
                           motion.smoothing = 1,
                           speed.smoothing = 2,
                           calculate.paddle.speed = FALSE,
                           speed.calibration.values = NULL,
                           burst.quantiles = c(0.95, 0.99),
                           return.data = TRUE,
                           save.files = FALSE,
                           output.folder = NULL,
                           output.suffix = NULL,
                           data.table.threads = NULL,
                           verbose = TRUE) {


  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

  # measure running time
  start.time <- Sys.time()

  # check if data is a character vector of RDS file paths
  is_filepaths <- is.character(data)
  if (is_filepaths) {
    # first, check all files exist
    missing_files <- data[!file.exists(data)]
    if (length(missing_files) > 0) {
      stop(paste("The following files were not found:\n",
                 paste("-", missing_files, collapse = "\n")), call. = FALSE)
    }
  } else if (!is.list(data) || inherits(data, "data.frame")) {
    # if it's a single data.frame, convert it to a list
    if (!"ID" %in% names(data)) {
      stop("Input data must contain an 'ID' column when not provided as a list.", call. = FALSE)
    }
    data <- split(data, data$ID)
  }


  # feedback for save files mode
  if (!is.logical(save.files)) stop("`save.files` must be a logical value (TRUE or FALSE).", call. = FALSE)

  # validate that at least one output method is selected
  if (!save.files && !return.data) {
    stop("Both 'save.files' and 'return.data' cannot be FALSE - this would result in data loss. ",
         "Please set at least one to TRUE.", call. = FALSE)
  }

  # define required columns
  required_cols <- c("ID", "datetime", "ax", "ay", "az", "gx", "gy", "gz", "mx", "my", "mz", "depth", "temp")

  # if data is already in memory (not file paths), validate upfront
  if (!is_filepaths) {

    # validate each dataset in the list
    lapply(data, function(dataset) {
      # check dataset structure
      if (!is.data.frame(dataset)) stop("Each element in the data list must be a data.frame or data.table", call. = FALSE)
      # check for required columns
      missing_cols <- setdiff(required_cols, names(dataset))
      if (length(missing_cols) > 0) stop(sprintf("Missing required columns: %s", paste(missing_cols, collapse = ", ")), call. = FALSE)
      # ensure datetime column is of POSIXct class
      if (!inherits(dataset$datetime, "POSIXct")) stop("The datetime column must be of class 'Date' or 'POSIXct'.", call. = FALSE)
    })

    # check for nautilus.version attribute in each dataset
    missing_attr <- sapply(data, function(x) {is.null(attr(x, "nautilus.version"))})
    if (any(missing_attr)) {
      message(paste0(
        "Warning: The following dataset(s) were likely not processed via importTagData():\n  - ",
        paste(names(data)[missing_attr], collapse = ", "),
        "\n\nIt is strongly recommended to run them through importTagData() to ensure proper formatting and avoid downstream errors.\n",
        "Proceed at your own risk."))
    }
  }

  # ensure output.folder is a single string
  if(!is.null(output.folder) && (!is.character(output.folder) || length(output.folder) != 1)) {
    stop("`output.folder` must be NULL or a single string.", call. = FALSE)
  }

  # check if the output folder is valid (if specified)
  if(!is.null(output.folder) && !dir.exists(output.folder)){
    stop("The specified output folder does not exist. Please provide a valid folder path.", call. = FALSE)
  }

  # validate smoothing and moving window parameters
  if(!orientation.algorithm %in% c("madgwick", "tilt_compass")) stop("'orientation.algorithm' must be either 'madgwick' or 'tilt_compass'", call. = FALSE)
  if(!is.numeric(dba.window) || dba.window <= 0) stop("`dba.window` must be a positive numeric value.", call. = FALSE)
  if(!is.null(orientation.smoothing)  && (!is.numeric(orientation.smoothing) || orientation.smoothing <= 0)) stop("`orientation.smoothing` must be a positive numeric value.", call. = FALSE)
  if(!is.null(motion.smoothing) && (!is.numeric(motion.smoothing) || motion.smoothing <= 0)) stop("`motion.smoothing` must be a positive numeric value.", call. = FALSE)
  if(!is.null(speed.smoothing) && (!is.numeric(speed.smoothing) || speed.smoothing <= 0)) stop("`speed.smoothing` must be a positive numeric value.", call. = FALSE)
  if(!is.null(dba.smoothing) && (!is.numeric(dba.smoothing) || dba.smoothing <= 0)) stop("`dba.smoothing` must be a positive numeric value.", call. = FALSE)

  # validate "burst.quantiles"
  if (!is.numeric(burst.quantiles) || any(burst.quantiles <= 0) || any(burst.quantiles > 1)) {
    stop("`burst.quantiles` must be a numeric vector with values in the range (0, 1].", call. = FALSE)
  }

  # validate "correct.pitch.offset"
  if (!is.logical(correct.pitch.offset)) stop("`correct.pitch.offset` must be a logical value (TRUE or FALSE).", call. = FALSE)

  # validate speed.calibration.values if paddle speed calculation is requested
  if (calculate.paddle.speed && !is.null(speed.calibration.values)) {
    if (!is.data.frame(speed.calibration.values)) stop("speed.calibration.values must be a data.frame", call. = FALSE)
    missing_cols <- setdiff(c("year", "package_id", "slope"), names(speed.calibration.values))
    if (length(missing_cols) > 0) {
      stop(paste("speed.calibration.values is missing required columns:",
                 paste(missing_cols, collapse = ", ")), call. = FALSE)
    }
    if (!is.numeric(speed.calibration.values$year)) stop("year column in speed.calibration.values must be numeric", call. = FALSE)
    if (!is.numeric(speed.calibration.values$slope)) stop("slope column in speed.calibration.values must be numeric", call. = FALSE)
  }

  # validate data.table threads if specified
  if (!is.null(data.table.threads)) {
    if (!is.numeric(data.table.threads) || data.table.threads < 1 || data.table.threads > parallel::detectCores()) {
      stop("data.table.threads must be between 1 and ", parallel::detectCores(), call. = FALSE)
    }
  }

  # feedback for verbose mode
  if (!is.logical(verbose)) stop("`verbose` must be a logical value (TRUE or FALSE).", call. = FALSE)


  ##############################################################################
  # Python checks #############################################################
  ##############################################################################

  if (orientation.algorithm == "madgwick") {

    # check that 'reticulate' is installed
    if (!requireNamespace("reticulate", quietly = TRUE)) {
      stop("The 'reticulate' package is required but not installed. Please install it using install.packages('reticulate') or switch to orientation.algorithm='tilt'.", call. = FALSE)
    }

    # load reticulate
    reticulate::use_python(Sys.which("python"), required = FALSE)

    # check that Python is available
    if (!reticulate::py_available(initialize = TRUE)) {
      stop("Python is not available in the current environment. Please configure Python using reticulate::use_python().")
    }

    # check that 'ahrs' and 'numpy' are available in the Python environment
    py_modules <- reticulate::py_list_packages()
    if (!"AHRS" %in% py_modules$package) {
      stop("The Python package 'AHRS' is not installed. Please run: reticulate::py_install('ahrs')")
    }
    if (!"numpy" %in% py_modules$package) {
      stop("The Python package 'numpy' is not installed. Please run: reticulate::py_install('numpy')")
    }
  }


  ##############################################################################
  # Initialize variables #######################################################
  ##############################################################################

  # create lists to store processed data, plots, and summaries for each animal
  n_animals <- length(data)
  data_list <- vector("list", length = n_animals)

  # feedback messages for the user
  cat(paste0(
    crayon::bold("\n================= Processing Tag Data =================\n"),
    "Crunching high-resolution sensor streams from ", n_animals, " ", ifelse(n_animals == 1, "tag", "tags"), "\n",
    crayon::bold("=======================================================\n\n")
  ))

  if (verbose & is_filepaths) cat("Loading data from RDS files...\n\n")


  # set data.table threads if specified
  if (!is.null(data.table.threads)) {
    original_threads <- data.table::getDTthreads()
    data.table::setDTthreads(threads = data.table.threads)
    on.exit(data.table::setDTthreads(threads = original_threads), add = TRUE)
  }


  ##############################################################################
  # Process data for each folder ###############################################
  ##############################################################################

  # iterate over each animal
  for (i in seq_along(data)) {

    ############################################################################
    # load data for the current individual if using file paths #################
    if (is_filepaths) {

      # get current file path
      file_path <- data[i]

      # load current file
      individual_data <- readRDS(file_path)

      # perform checks specific to loaded RDS files
      missing_cols <- setdiff(required_cols, names(individual_data))
      if (length(missing_cols) > 0) stop(sprintf("Missing required columns: %s in file '%s'", paste(missing_cols, collapse = ", "), basename(file_path)), call. = FALSE)
      if (!inherits(individual_data$datetime, "POSIXct")) stop("The datetime column in file '", basename(file_path), "' must be of class 'POSIXct'.", call. = FALSE)
      if (is.null(attr(individual_data, "nautilus.version"))) {
        message(paste0("Warning: File '", basename(file_path), "' was likely not processed via importTagData(). It is strongly recommended to run it through importTagData() to ensure proper formatting."))
      }

      # get ID
      id <- unique(individual_data[[id.col]])[1]

    ############################################################################
    # data is already in memory (list of data frames/tables) ###################
    } else {
      # retrieve ID from the dataset based on the specified 'id.col' column
      id <- names(data)[i]
      # access the individual dataset
      individual_data <- data[[i]]
    }

    # print current ID
    cat(crayon::bold(sprintf("[%d/%d] %s\n", i, n_animals, id)))

    # skip NULL or empty elements in the list
    if (is.null(individual_data) || length(individual_data) == 0) next

    # ensure data is ordered by datetime
    data.table::setorder(individual_data, datetime)

    # calculate sampling frequency
    sampling_freq <- nrow(individual_data) / length(unique(lubridate::floor_date(individual_data$datetime, "sec")))
    sampling_freq <- plyr::round_any(sampling_freq, 5)

    # store original attributes, excluding internal ones
    discard_attrs <- c("row.names", "class", ".internal.selfref", "names")
    original_attributes <- attributes(individual_data)
    original_attributes <- original_attributes[!names(original_attributes) %in% discard_attrs]


    ############################################################################
    # Calibrate magnetometer ###################################################
    ############################################################################

    # extract raw magnetometer data
    mag_data <- as.matrix(individual_data[, .(mx, my, mz)])

    # check if we have valid magnetometer data to calibrate
    valid_magnetometer_data <- all(
      !all(is.na(mag_data[, "mx"])),
      !all(is.na(mag_data[, "my"])),
      !all(is.na(mag_data[, "mz"]))
    )

    # proceed with calibration
    if (valid_magnetometer_data) {

      # check if the tag was equipped with a paddle wheel
      mz_raw <- NA
      has_paddle_info <- !is.null(attr(individual_data, "paddle.wheel"))
      if(has_paddle_info && isTRUE(attr(individual_data, "paddle.wheel"))) {
        # store original mz column to estimate paddle speed
        mz_raw <- mag_data[, "mz"]
        # apply 3-second rolling mean to all axes to remove noise
        if(verbose) cat("---> Removing paddle-wheel-induced magnetic noise\n")
        mag_data[, "mx"] <- zoo::rollmean(mag_data[, "mx"], k = sampling_freq * 3, align = "center", fill = "extend")
        mag_data[, "my"] <- zoo::rollmean(mag_data[, "my"], k = sampling_freq * 3, align = "center", fill = "extend")
        mag_data[, "mz"] <- zoo::rollmean(mag_data[, "mz"], k = sampling_freq * 3, align = "center", fill = "extend")
      }

      # initialize calibrated data with raw data
      mag_calibrated <- mag_data

      # provide feedback to the user if verbose mode is enabled
      if (verbose && (hard.iron.calibration || soft.iron.calibration)) {
        hi <- as.integer(hard.iron.calibration)
        si <- as.integer(soft.iron.calibration)
        if (hi == 1 && si == 1) {
          type <- "hard-iron and soft-iron"
        } else if (hi == 1 && si == 0) {
          type <- "hard-iron"
        } else if (hi == 0 && si == 1) {
          type <- "soft-iron"
        } else {
          type <- NULL
        }
        cat("---> Applying", type, "magnetic calibration\n")
      }

      # apply hard-iron calibration if enabled
      if (hard.iron.calibration) {
        # estimate hard-iron offset using midpoint method (average of max and min)
        hard_iron_offset <- 0.5 * (apply(mag_data, 2, max, na.rm = TRUE) + apply(mag_data, 2, min, na.rm = TRUE))
        # center the data (remove hard-iron bias)
        mag_calibrated <- sweep(mag_data, 2, hard_iron_offset, FUN = "-")
      }

      # apply soft-iron calibration if enabled
      if (soft.iron.calibration) {
        # estimate soft-iron scale factors (half of max chord length for each axis)
        soft_iron_scales <- 0.5 * (apply(mag_data, 2, max, na.rm = TRUE) - apply(mag_data, 2, min, na.rm = TRUE))
        # compute average scale (used to normalize all axes)
        avg_scale <- mean(soft_iron_scales)
        # apply axis-wise rescaling (soft-iron correction)
        mag_calibrated <- sweep(mag_calibrated, 2, avg_scale / soft_iron_scales, FUN = "*")
      }

      # normalize the calibrated data (unit sphere)
      mag_calibrated <- mag_calibrated / sqrt(rowSums(mag_calibrated^2))

      # update original columns
      individual_data[, `:=`(mx = mag_calibrated[,1], my = mag_calibrated[,2], mz = mag_calibrated[,3])]

      # clean up - remove all potential objects
      objs_to_remove <- c("mag_data", "mag_corrected", "cov_matrix", "eig", "V", "D_inv", "soft_iron_matrix", "mag_calibrated", "hard_iron_offset")
      rm(list = intersect(objs_to_remove, ls()))
      gc()

    # else print feedback message
    }else{
      if (verbose) message("No valid magnetometer data - skipping magnetic calibration.")
    }


    ############################################################################
    # Calculate acceleration metrics ###########################################
    ############################################################################

    # provide feedback to the user if verbose mode is enabled
    if(verbose) cat("---> Calculating acceleration metrics\n")

    # calculate total acceleration
    individual_data[, accel := sqrt(ax^2 + ay^2 + az^2)]

    # calculate dynamic and vectorial body acceleration using a moving window
    # doi: 10.3354/ab00104

    # calculate window parameters
    window_size <- dba.window * sampling_freq
    pad_length <- ceiling(window_size / 2)

    # calculate static (low-frequency) acceleration using padded rolling mean
    staticX = .pad_rollmean(individual_data$ax, window_size, pad_length)
    staticY = .pad_rollmean(individual_data$ay, window_size, pad_length)
    staticZ = .pad_rollmean(individual_data$az, window_size, pad_length)

    # calculate dynamic (high-frequency) acceleration by removing static component
    dynamicX <- individual_data$ax - staticX
    dynamicY <- individual_data$ay - staticY
    dynamicZ <- individual_data$az - staticZ

    # calculate ODBA (Overall Dynamic Body Acceleration) and VeDBA (Vectorial DBA)
    individual_data[, `:=`(
      odba = abs(dynamicX) + abs(dynamicY) + abs(dynamicZ),
      vedba = sqrt(dynamicX^2 + dynamicY^2 + dynamicZ^2)
    )]

    # smooth the signals using a moving average (optional for noise reduction)
    if(!is.null(dba.smoothing)){
      window_size <- sampling_freq * dba.smoothing
      individual_data[, odba := data.table::frollmean(odba, n = window_size, fill = NA, align = "center")]
      individual_data[, vedba := data.table::frollmean(vedba, n = window_size, fill = NA, align = "center")]
    }

    # estimate burst swimming events (based on specified percentiles)
    if(!is.null(burst.quantiles)){
      for(q in burst.quantiles){
        accel_threshold <- quantile(individual_data$accel, probs = q, na.rm = TRUE)
        burst_col <- paste0("burst", q*100)
        individual_data[, (burst_col) := as.integer(accel >= accel_threshold)]
      }
    }

    ############################################################################
    # Calculate linear motion metrics ##########################################
    ############################################################################

    # provide feedback to the user if verbose mode is enabled
    if (verbose) cat("---> Calculating linear motion metrics\n")

    # calculate surge
    # motion along the longitudinal (X) axis (forward/backward swimming)
    individual_data[, surge := ax - staticX]

    # calculate sway
    # motion along the lateral (Y) axis (side-to-side swaying)
    individual_data[, sway := ay - staticY]

    # calculate heave
    # motion along the vertical (Z) axis (up and down, often from diving or wave action)
    individual_data[, heave := az - staticZ]

    # calculate vertical speed
    individual_data[, vertical_speed := c(NA, diff(depth) / as.numeric(diff(datetime), units = "secs"))]

    # smooth the signals using a moving average (optional for noise reduction)
    if(!is.null(motion.smoothing)){
      window_size <- sampling_freq * motion.smoothing
      individual_data[, surge := data.table::frollmean(surge, n = window_size, fill = NA, align = "center")]
      individual_data[, sway := data.table::frollmean(sway, n = window_size, fill = NA, align = "center")]
      individual_data[, heave := data.table::frollmean(heave, n = window_size, fill = NA, align = "center")]
    }
    if(!is.null(speed.smoothing)){
      window_size <- sampling_freq * speed.smoothing
      individual_data[, vertical_speed := data.table::frollmean(vertical_speed, n = window_size, fill = NA, align = "center")]
    }


    ############################################################################
    # Calculate orientation metrics ############################################
    ############################################################################

    # first, check sensor data validity
    valid_accel_data <- !all(is.na(individual_data$ax)) &
      !all(is.na(individual_data$ay)) &
      !all(is.na(individual_data$az))

    valid_gyro_data <- !all(is.na(individual_data$gx)) &
      !all(is.na(individual_data$gy)) &
      !all(is.na(individual_data$gz))

    # determine feasible orientation methods
    use_madgwick <- orientation.algorithm == "madgwick" && valid_accel_data && valid_gyro_data
    use_tilt_compass <- orientation.algorithm == "tilt_compass" && valid_accel_data


    #############################################################
    # Python Madgwick filter ####################################
    if(use_madgwick){

      # load Python packages
      ahrs <- reticulate::import("ahrs", delay_load = TRUE)
      np <- reticulate::import("numpy", delay_load = TRUE)

      # prepare sensor data (always accel + gyro)
      acc_np <- np$array(as.matrix(individual_data[, .(ax, ay, az)]))
      gyr_np <- np$array(as.matrix(individual_data[, .(gx, gy, gz)]))

      # run Madgwick (with or without mag)
      if (valid_magnetometer_data) {
        # run the full Madgwick filter (MARG: acc + gyro + mag)
        if (verbose) cat("---> Calculating orientation using Madgwick filter (MARG mode)\n")
        mag_np <- np$array(as.matrix(individual_data[, .(mx, my, mz)]))
        madgwick <- ahrs$filters$Madgwick(gyr = gyr_np, acc = acc_np, mag = mag_np,
                                          frequency = sampling_freq, beta = madgwick.beta)
      } else {
        if (verbose) cat("---> Calculating orientation using Madgwick filter (IMU mode)\n")
        if (verbose) message("No valid magnetometer data (accel + gyro only, no heading)")
        madgwick <- ahrs$filters$Madgwick(gyr = gyr_np, acc = acc_np, mag = NULL,
                                          frequency = sampling_freq, beta = madgwick.beta)
      }

      # extract quaternions
      Q <- madgwick$Q
      w <- Q[, 1]; x <- Q[, 2]; y <- Q[, 3]; z <- Q[, 4]

      # compute pitch and roll
      pitch_deg <- asin(pmax(pmin(2 * (w * y - z * x), 1.0), -1.0)) * 180 / pi
      roll_deg <- atan2(2 * (w * x + y * z), 1 - 2 * (x^2 + y^2)) * 180 / pi

      # compute heading ONLY if mag was used
      heading_deg <- if (valid_magnetometer_data) {
        (atan2(2 * (w * z + x * y), 1 - 2 * (y^2 + z^2)) * 180 / pi) %% 360
      } else {
        rep(NA_real_, nrow(Q))
      }

      # store results
      individual_data[, `:=`(
        heading = heading_deg,
        pitch = pitch_deg,
        roll = roll_deg
      )]

      # cleanup - delete Python objects and force garbage collection
      to_remove <- c("madgwick", "acc_np", "gyr_np", "mag_np", "Q",
                     "w", "x", "y", "z", "heading_deg", "pitch_deg", "roll_deg")
      rm(list = intersect(to_remove, ls()))
      py_gc <- reticulate::import("gc")
      invisible(py_gc$collect())
      gc()


    #############################################################
    # else, default to the tilt-compensated compass method ######
    } else if (use_tilt_compass) {

      # provide feedback to the user if verbose mode is enabled
      if (verbose) cat("---> Calculating tilt-compensated orientation metrics\n")

      # add small constant to prevent division by zero
      epsilon <- 1e-7

      # compute normalized static accelerometer vectors
      acc_norm <- sqrt(staticX^2 + staticY^2 + staticZ^2 + epsilon)
      ax_norm <- staticX / acc_norm
      ay_norm <- staticY / acc_norm
      az_norm <- staticZ / acc_norm

      # calculate roll and pitch angles
      individual_data[, `:=`(
        roll = atan2(ay_norm, sign(az_norm) * sqrt(az_norm^2 + ax_norm^2 * epsilon)),
        pitch = atan2(-ax_norm, sqrt(ay_norm^2 + az_norm^2 + epsilon))
      )]

      # calculate heading only if magnetometer readings are valid
      if (valid_magnetometer_data) {

        # correct the magnetometer readings using the roll and pitch angles (tilt-compensated magnetic field vector)
        mx_comp <- individual_data$mx * cos(individual_data$pitch) + individual_data$my * sin(individual_data$pitch) * sin(individual_data$roll) + individual_data$mz * sin(individual_data$pitch) * cos(individual_data$roll)
        my_comp <- individual_data$my * cos(individual_data$roll) - individual_data$mz * sin(individual_data$roll)

        # convert roll and pitch from radians to degrees
        individual_data[, roll := roll * (180 / pi)]
        individual_data[, pitch := pitch * (180 / pi)]

        # calculate the heading and convert from radians to degrees (accounting for gimbal lock)
        individual_data[, heading := {ifelse(abs(pitch) > 89.5, NA_real_, atan2(-my_comp, mx_comp) * (180/pi))}]

      } else {
        # set heading to NA
        if (verbose) message("No valid magnetometer data - setting heading to NA.")
        individual_data[, heading := NA_real_]

        # convert roll and pitch from radians to degrees
        individual_data[, roll := roll * (180 / pi)]
        individual_data[, pitch := pitch * (180 / pi)]
      }

    # if all else fails
    } else {
      if (verbose) message("Insufficient sensor data - cannot compute orientation.")
      individual_data[, `:=`(roll = NA_real_, pitch = NA_real_, heading = NA_real_)]
    }


    ############################################################################
    ## convert magnetic heading to geographic heading ##########################

    # only proceed if heading exists and is not all NA
    if (!all(is.na(individual_data$heading))) {

      # determine location to use for magnetic declination calculation
      if (!is.null(attr(individual_data, "deployment.info"))) {
        # use deployment info if available
        deploy_info <- attr(individual_data, "deployment.info")
      } else {
        # fallback: use the first available row with valid longitude and latitude
        valid_idx <- which(!is.na(individual_data$lon) & !is.na(individual_data$lat))[1]
        if (!is.na(valid_idx)) {
          deploy_info <- data.frame(datetime=individual_data$datetime[valid_idx],
                                    lon = individual_data$lon[valid_idx],
                                    lat = individual_data$lat[valid_idx])
        } else {
          stop("No valid location found to estimate magnetic declination.", call. = FALSE)
        }
      }

      # get magnetic declination value (in degrees)
      declination_deg <- oce::magneticField(longitude=deploy_info$lon, latitude=deploy_info$lat, time=deploy_info$datetime)$declination
      declination_deg <- round(declination_deg, 2)

      # apply magnetic declination correction to convert from magnetic north to geographic north
      individual_data[, heading := (heading + declination_deg) %% 360]

    }else{

      # skip declination correction
      declination_deg <- NULL

    }


    ############################################################################
    # correct pitch offset if requested ########################################

    if (correct.pitch.offset) {

      # skip if all pitch values are NA
      if (all(is.na(individual_data$pitch))) {
        if (verbose) message("No valid pitch data - skipping pitch offset correction")
        pitch_offset_deg <- NULL
        pitch_offset_r2 <- NULL

      } else {

        if (verbose) cat("---> Correcting for potential pitch offset\n")

        # calculate 10-second rolling mean of vertical speed
        vv_window <- 10 * sampling_freq
        individual_data[, vv_smooth := data.table::frollmean(vertical_speed, n = vv_window, fill = NA, align = "center")]

        # convert pitch to radians for regression
        individual_data[, pitch_rad := pitch * (pi/180)]

        # perform linear regression between smoothed vertical velocity and pitch (in radians)
        pitch_model <- lm(pitch_rad ~ vv_smooth, data = individual_data[!is.na(vv_smooth) & !is.na(pitch_rad)])

        # get R squared
        pitch_offset_r2 <- summary(pitch_model)$r.squared

        # get intercept (pitch angle in radians when vertical velocity = 0)
        pitch_offset_rad <- coef(pitch_model)[1]

        # convert intercept back to degrees for correction
        pitch_offset_deg <- pitch_offset_rad * (180/pi)

        # only apply correction if offset is below threshold
        if(abs(pitch_offset_deg) < pitch.warning.threshold) {

          # correct pitch values by subtracting the offset (in degrees)
          individual_data[, pitch := pitch - pitch_offset_deg]

          # report the estimated offset in degrees
          if (verbose) cat(sprintf("     (pitch offset correction: %.2f\u00b0 | R\u00b2 = %.2f)\n", pitch_offset_deg, pitch_offset_r2))

        } else {

          if (verbose) cat("     (pitch offset exceeds threshold - no correction applied)\n")
          pitch_offset_deg <- NULL
          pitch_offset_r2 <- NULL
        }

        # clean up temporary columns
        individual_data[, c("pitch_rad", "vv_smooth") := NULL]
        rm(pitch_model, pitch_offset_rad, vv_window)
      }

    } else {
      pitch_offset_deg <- NULL
      pitch_offset_r2 <- NULL
    }


    ############################################################################
    # apply a moving circular mean to smooth the metrics time series ###########

    if(!is.null(orientation.smoothing)) {
      window_size <- sampling_freq * orientation.smoothing
      individual_data[, roll := .rollingCircularMean(roll, window = window_size, range = c(-180, 180) )]
      individual_data[, pitch := .rollingCircularMean(pitch, window = window_size,  range = c(-90, 90))]
      individual_data[, heading := .rollingCircularMean(heading, window = window_size, range = c(0, 360))]
    }

    ############################################################################
    # check for potential axis issues (misalignment, swaps, or sign flips) #####

    pitch_anomaly_detected <- FALSE
    roll_anomaly_detected <- FALSE

    # only check if pitch contain non-NA values
    if (!all(is.na(individual_data$pitch))) {
      median_pitch <- median(individual_data$pitch, na.rm = TRUE)
      if (abs(median_pitch) > pitch.warning.threshold) {
        message("Potential pitch anomaly: Median = ", round(median_pitch, 1), "\u00B0")
        pitch_anomaly_detected <- TRUE
        warning(sprintf("%s - Potential pitch anomaly detected (%.2f\u00b0)", id, median_pitch), call. = FALSE)
      }
    }

    # only check if roll contain non-NA values
    if (!all(is.na(individual_data$roll))) {
      median_roll <- median(individual_data$roll, na.rm = TRUE)
      if (abs(median_roll) > roll.warning.threshold) {
        message("Potential roll anomaly: Median = ", round(median_roll, 1), "\u00B0")
        roll_anomaly_detected <- TRUE
        warning(sprintf("%s - Potential roll anomaly detected (%.2f\u00b0)", id, median_roll), call. = FALSE)
      }
    }


    ############################################################################
    # Estimate paddle wheel rotation frequency #################################
    ############################################################################

    if (calculate.paddle.speed) {

      # determine if pre-calculated columns exist
      has_precalculated_freq <- "paddle_freq" %in% names(individual_data)
      has_precalculated_speed <- "paddle_speed" %in% names(individual_data)

      # initialize columns if they don't exist
      if (!has_precalculated_freq) individual_data[, paddle_freq := NA_real_]
      if (!has_precalculated_speed) individual_data[, paddle_speed := NA_real_]

      # initialize flag to determine if we should calculate speed internally
      perform_internal_calculation <- TRUE


      #############################################################
      # check for existing valid paddle data ######################

      # check if existing data is meaningful (not all NA and not constant)
      is_freq_meaningful <- has_precalculated_freq &&
        !all(is.na(individual_data$paddle_freq)) &&
        length(unique(na.omit(individual_data$paddle_freq))) > 1

      is_speed_meaningful <- has_precalculated_speed &&
        !all(is.na(individual_data$paddle_speed)) &&
        length(unique(na.omit(individual_data$paddle_speed))) > 1

      if (is_freq_meaningful && is_speed_meaningful) {
        if (verbose) cat("---> Paddle frequency and speed already present, skipping estimation\n")
        perform_internal_calculation <- FALSE

      } else if (has_precalculated_speed && !has_precalculated_freq && is_speed_meaningful) {
        if (verbose) cat("---> Paddle speed already present, setting paddle_freq to NA\n")
        perform_internal_calculation <- FALSE

      } else if ((has_precalculated_freq || has_precalculated_speed) && verbose) {
        if (verbose) cat("---> Paddle data found but invalid (constant or NA). Recalculating.\n")
      }

      #############################################################
      # remaining checks for paddle wheel setup ###################

      # check if the tag was equipped with a paddle whee
      if (perform_internal_calculation) {
        has_paddle_info <- !is.null(attr(individual_data, "paddle.wheel"))
        if (!has_paddle_info) {
          if (verbose) cat("---> No paddle wheel info found, skipping speed estimation\n")
          perform_internal_calculation <- FALSE
        } else if (attr(individual_data, "paddle.wheel") == FALSE) {
          if (verbose) cat("---> Tag has no paddle wheel, skipping speed estimation\n")
          perform_internal_calculation <- FALSE
        }
      }

      # check package ID and calibration
      if (perform_internal_calculation) {
        has_package <- !is.null(attr(individual_data, "package.id"))
        if (!has_package) {
          message("No packageID found for the current tag, skipping speed estimation")
          perform_internal_calculation <- FALSE
        } else {
          package_id <- attr(individual_data, "package.id")

          # Determine deployment year for calibration lookup
          if (!is.null(attr(individual_data, "deployment.info"))) {
            deploy_year <- as.integer(format(attr(individual_data, "deployment.info")$datetime, "%Y"))
          } else {
            deploy_year <- as.integer(format(individual_data$datetime[1], "%Y"))
          }

          tag_calibration <- speed.calibration.values[speed.calibration.values$year == deploy_year &
                                                        speed.calibration.values$package_id == package_id, ]
          has_calibration_info <- nrow(tag_calibration) > 0

          if (!has_calibration_info) {
            if (verbose) message("No calibration values found for this tag, skipping speed estimation")
            perform_internal_calculation <- FALSE
          } else if (sampling_freq < 50) {
            if (verbose) message("Sampling freq too low (\u2264 50Hz), skipping speed estimation")
            perform_internal_calculation <- FALSE
          }
        }
      }

      #############################################################
      # perform internal speed estimation if all checks pass ######

      if (perform_internal_calculation) {

        # print message
        if (verbose) {
          cat("---> Estimating paddle wheel rotation speed\n")
          cat(sprintf("     (calibration slope: %.4f)\n", tag_calibration$slope))
        }

        # calculate frequencies and speed
        paddle_data <- .getPaddleSpeed(
          mz = mz_raw,
          sampling.rate = sampling_freq,
          calibration.slope = tag_calibration$slope,
          smooth.window = speed.smoothing,
        )

        # add to sensor data
        individual_data[, paddle_freq := paddle_data$freq]
        individual_data[, paddle_speed := paddle_data$speed]
      }
    }


    ############################################################################
    # Downsample data ##########################################################
    ############################################################################

    # select columns to keep
    metrics <- c("temp","depth","ax", "ay", "az", "gx", "gy", "gz", "mx", "my", "mz",
                 "accel","odba","vedba","roll", "pitch", "heading",
                 "surge", "sway", "heave", "vertical_speed")
    if (calculate.paddle.speed) {
      if(has_paddle_info) metrics <- c(metrics, "paddle_freq", "paddle_speed")
    }
    position_cols <- c("PTT", "position_type", "lat", "lon", "quality")

    # store current sampling frequency
    sampling_rate <- sampling_freq

    # if a downsampling rate is specified, aggregate the data to the defined frequency (in Hz)
    if(!is.null(downsample.to)){

      # check if the specified downsampling frequency matches the dataset's sampling frequency
      if (downsample.to == sampling_freq) {
        if (verbose) cat("---> Dataset sampling rate is already", downsample.to, "Hz, skipping downsampling\n")
        processed_data <- individual_data

      # check if the specified downsampling frequency exceeds the dataset's sampling frequency
      } else if(downsample.to > sampling_freq) {
        if (verbose) cat("---> Dataset sampling rate (", sampling_freq, "Hz) is lower than the specified downsampling rate (", downsample.to, "Hz), skipping downsampling\n", sep = "")
        processed_data <- individual_data

      # start downsampling
      } else {

        # provide feedback to the user if verbose mode is enabled
        if (verbose)  cat("---> Downsampling data to", downsample.to, "Hz\n")

        # store new sampling frequency
        sampling_rate <- downsample.to

        # convert the desired downsample rate to time interval in seconds
        downsample_interval <- 1 / downsample.to

        # round datetime to the nearest downsample interval
        first_time <- individual_data$datetime[1]
        individual_data[, datetime := first_time + floor(as.numeric(datetime - first_time) / downsample_interval) * downsample_interval]

        # temporarily suppress console output (redirect to a temporary file)
        sink(tempfile())

        # define columns
        orientation_cols <- c("roll", "pitch", "heading")
        numeric_cols <- setdiff(metrics, orientation_cols)

        # aggregate numeric metrics using arithmetic mean
        processed_data <- individual_data[, lapply(.SD, mean, na.rm=TRUE), by = datetime, .SDcols = numeric_cols]

        # aggregate orientation metrics using circular mean
        processed_roll <- individual_data[, .(roll = .circularMean(roll, range = c(-180, 180))), by = datetime]
        processed_pitch <- individual_data[, .(pitch = .circularMean(pitch, range = c(-90, 90))), by = datetime]
        processed_heading <- individual_data[, .(heading = .circularMean(heading, range = c(0, 360))), by = datetime]

        # aggregate location column using first value
        processed_positions <- individual_data[, lapply(.SD, first), by = datetime, .SDcols = position_cols]

        # combine aggregated datasets
        processed_data <- Reduce(function(x, y) merge(x, y, by = "datetime", sort = FALSE),
                                 list(processed_data, processed_roll, processed_pitch, processed_heading, processed_positions))

        # sum burst swimming events (based on specified percentiles)
        if(!is.null(burst.quantiles)){
          burst_cols <- paste0("burst", burst.quantiles * 100)
          processed_bursts <- individual_data[, lapply(.SD, function(x) as.integer(sum(as.numeric(x), na.rm = TRUE) > 0)), by = datetime, .SDcols = burst_cols]
          # combine the two aggregated datasets
          processed_data <- merge(processed_data, processed_bursts, by = "datetime", all.x = TRUE)
        }

        # re-add ID column
        processed_data[, ID := id]

        # clean up
        objs_to_remove <- c("processed_roll", "processed_pitch", "processed_heading", "processed_positions", "processed_bursts")
        rm(list = intersect(objs_to_remove, ls()))
        gc()

        # restore normal output
        sink()
      }

    } else{
      # if no downsampling rate is defined, return the original sensor data
      processed_data <- individual_data
    }

    # reorder columns: ID, metrics, burst.quantiles (if exists), and position_cols
    data.table::setcolorder(processed_data, c("ID", "datetime", metrics, if(!is.null(burst.quantiles)) paste0("burst", burst.quantiles * 100), position_cols))



    ############################################################################
    # Store processed data #####################################################
    ############################################################################

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


    # restore the original attributes
    for (attr_name in names(original_attributes)) {
      attr(processed_data, attr_name) <- original_attributes[[attr_name]]
    }

    # create new attributes to save relevant variables
    attr(processed_data, 'original.sampling.frequency') <- sampling_freq
    attr(processed_data, 'processed.sampling.frequency') <- sampling_rate
    attr(processed_data, 'hard.iron.calibration') <- hard.iron.calibration
    attr(processed_data, 'soft.iron.calibration') <- soft.iron.calibration
    attr(processed_data, 'orientation.algorithm') <- orientation.algorithm
    attr(processed_data, 'madgwick.beta') <- madgwick.beta
    attr(processed_data, 'magnetic.declination') <- declination_deg
    attr(processed_data, 'dba.window') <- dba.window
    attr(processed_data, 'dba.smoothing') <- dba.smoothing
    attr(processed_data, 'orientation.smoothing') <- orientation.smoothing
    attr(processed_data, 'motion.smoothing') <- motion.smoothing
    attr(processed_data, 'speed.smoothing') <- speed.smoothing
    attr(individual_data, 'pitch.offset.value') <- pitch_offset_deg
    attr(individual_data, 'pitch.offset.model.r2') <- pitch_offset_r2
    attr(processed_data, 'pitch.warning.threshold') <- pitch.warning.threshold
    attr(processed_data, 'roll.warning.threshold') <- roll.warning.threshold
    attr(processed_data, 'pitch.anomaly.detected') <- pitch_anomaly_detected
    attr(processed_data, 'roll.anomaly.detected') <- roll_anomaly_detected
    attr(processed_data, 'processing.date') <- Sys.time()

    # sort final attributes
    internal_attrs <- c("names", "row.names", "class", ".internal.selfref", "index")
    id_attrs <- c("id", "directory", "tag.model", "tag.type", "package.id", "original.rows", "imported.columns",
                  "axis.mapping", "timezone", "deployment.info", "first.datetime", "last.datetime",
                  "original.sampling.frequency", "processed.sampling.frequency")
    sensor_attrs <- c("hard.iron.calibration", "soft.iron.calibration", "orientation.algorithm",
                      "madgwick.beta", "magnetic.declination", "dba.window", "dba.smoothing",
                       "orientation.smoothing", "motion.smoothing", "speed.smoothing", "paddle.wheel")
    checks_attrs <- c("regularization.performed", "pitch.offset.value",  "pitch.offset.model.r2",
                      "pitch.warning.threshold", "roll.warning.threshold","pitch.anomaly.detected", "roll.anomaly.detected")
    final_attrs <- c("nautilus.version", "processing.date")
    sorted_attrs <- c(internal_attrs, id_attrs, sensor_attrs, checks_attrs, final_attrs)

    # keep only those attributes that exist in the current object and are in sorted_attrs
    reordered_attrs <- attributes(processed_data)[intersect(sorted_attrs, names(attributes(processed_data)))]
    remaining_attrs <-  attributes(processed_data)[setdiff(names( attributes(processed_data)), names(reordered_attrs))]
    # combine both, with sorted ones first
    final_attrs <- c(reordered_attrs, remaining_attrs)
    # reassign the ordered attributes to the object
    attributes(processed_data) <- final_attrs


    # provide feedback to the user if verbose mode is enabled
    if (verbose) cat("Done! Data processed successfully.\n")

    # save the processed data as an RDS file
    if(save.files){

      # print without newline and immediately flush output
      if (verbose) {
        cat("Saving file... ")
        flush.console()
      }

      # determine the output directory: use the specified output folder, or if not provided,
      # use the folder of the current file (if data[i] is a file path), or default to "./"
      if (!is.null(output.folder)) {
        output_dir <- output.folder
      } else if (is_filepaths) {
        output_dir <- dirname(data[i])
      } else {
        output_dir <- "./"
      }

      # define the file suffix: use the specified suffix or default to a suffix based on the sampling rate
      sufix <- ifelse(!is.null(output.suffix), output.suffix, paste0("-", sampling_rate, "Hz"))

      # construct the output file name
      output_file <- file.path(output_dir, paste0(id, sufix, ".rds"))

      # save the processed data
      saveRDS(processed_data, output_file)

      # overwrite the line with completion message
      if (verbose) {
        cat("\r")
        cat(rep(" ", getOption("width")-1))
        cat("\r")
        cat(sprintf("\u2713 Saved: %s\n", basename(output_file)))
      }
    }

    # print empty line
    cat("\n")


    # store processed sensor data in the results list if needed
    if (return.data) {
      data_list[[i]] <- processed_data
    }

    # clear unused objects from the environment to free up memory
    rm(individual_data)
    rm(processed_data)
    gc()
  }


  ##############################################################################
  # Return processed data ######################################################
  ##############################################################################

  # print message
  if (verbose) cat(crayon::bold("That's a wrap!\n"))

  # print time taken
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  cat(crayon::bold("Total execution time:"), sprintf("%.02f", as.numeric(time.taken)), base::units(time.taken), "\n\n")

  # return imported data or NULL based on return.data parameter
  if (return.data) {
    # assign names to the data list
    names(data_list) <- sapply(data_list, function(x) unique(x$ID)[1])
    # convert NA placeholders back to NULL
    na_indices <- which(sapply(data_list, function(x) identical(x, NA)))
    for (index in na_indices) {data_list[[index]] <- NULL}
    # return the list containing processed sensor data for all folders
    return(data_list)
  } else {
    return(invisible(NULL))
  }

}

#######################################################################################################
#######################################################################################################
#######################################################################################################
