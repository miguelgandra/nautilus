#######################################################################################################
# Function to import and standardize archival tag data ################################################
#######################################################################################################

#' Import and Standardize Archival Biologging Tag Data
#'
#' @description
#' This function provides an end-to-end solution for importing and standardizing
#' high-resolution biologging data from archival tags. It handles raw sensor outputs from
#' various tag types (defaulting to G-Pilot and i-Pilot formats) and transforms them into
#' analysis-ready datasets with consistent formatting.
#'
#' It can process data from a single individual or automatically iterate through directories
#' to process data from multiple individuals sequentially, making it well-suited for
#' large-scale studies.
#'
#' Sensor time series are automatically processed and converted to standardized units
#' — `g` for acceleration, `rad/s` for angular velocity, and `µT` for magnetic fields —
#' while preserving the original temporal resolution and structure.
#'
#' Users can optionally specify a custom column mapping using the `import.mapping` argument,
#' which allows importing data from CSV files with non-standard column names. When provided,
#' this mapping defines how columns in the raw data correspond to standard sensor types,
#' including the associated units. This ensures compatibility with the function's
#' standardization routines, even when input data deviates from default naming conventions.
#'
#' Additionally, the function includes options for axis transformations to
#' convert IMU data into animal-centric coordinate frames, such as NED (North-East-Down).
#'
#' When available, location data from Wildlife Computers tags (e.g., MiniPAT, MK10, SPOT)
#' can be integrated automatically. These should be stored in dedicated subfolders
#' (e.g., `"SPOT"`) within each individual's directory. Deployment and pop-up locations,
#' if present in the metadata, are also extracted and included in the output.
#'
#' The function employs memory-efficient processing to handle large datasets (>10 million rows),
#' using optimized `data.table` operations with configurable multi-threading.

#' @note For optimal performance with very large datasets, consider:
#' \itemize{
#'   \item Setting \code{data.table.threads} to match available CPU cores;
#'   \item Using \code{save.files = TRUE} to reduce memory usage;
#'   \item Processing individuals sequentially rather than simultaneously.
#' }
#'
#'
#' @param data.folders Character vector. Paths to the folders containing data to be processed.
#' Each folder corresponds to an individual animal and should contain subdirectories with sensor data and possibly
#' additional (Wildlife Computers) tag data.
#' @param sensor.subdirectory Character. Name of the subdirectory within each animal folder that contains sensor data (default: "CMD").
#' This subdirectory should include the sensor CSV files for the corresponding animal.
#' @param wc.subdirectory Character or NULL. Name of the subdirectory within each animal folder that contains Wildlife Computers tag data
#' (e.g., MiniPAT, MK10, or SPOT tag data), or NULL to auto-detect tag folders (default: NULL).
#' This subdirectory should contain the "Locations.csv" file with position data from the tag.
#' @param timezone Character string specifying the timezone for all datetime conversions.
#' Must be one of the valid timezones from \code{OlsonNames()}. Defaults to "UTC" (Coordinated Universal Time).
#' @param import.mapping Data frame or NULL. Optional specification of column names to import
#' from the CSV files and their corresponding sensor type and units. If `NULL` (default), uses
#' standard column names for archival G-Pilot and i-Pilot data. When specified, it must be a data frame
#' with three columns:
#' \itemize{
#'    \item \strong{colname}: The exact column name as it appears in the input CSV file.
#'    \item \strong{sensor}: The standardized sensor name to be used in the processed data. Valid options include: \code{"date"}, \code{"time"}, \code{"datetime"}, \code{"ax"}, \code{"ay"}, \code{"az"}, \code{"gx"}, \code{"gy"}, \code{"gz"}, \code{"mx"}, \code{"my"}, \code{"mz"}, \code{"depth"}, \code{"temp"}, \code{"paddle_freq"}, \code{"paddle_speed"}.
#'    \item \strong{units}: The units of the sensor data. Valid options include: \code{"UTC"}, \code{"m/s2"}, \code{"g"}, \code{"mrad/s"}, \code{"rad/s"}, \code{"deg/s"}, \code{"uT"}, \code{"C"}, \code{"m"}, \code{"Hz"}, \code{"m/s"}, or \code{""} (an empty string) for unitless/dimensionless quantities.
#' }
#' For date and time columns, use `sensor = "datetime"` and `units = "UTC"`.
#' @param axis.mapping Optional. A data frame containing the axis transformations for the IMU (Inertial Measurement Unit).
#' This parameter is used to correctly configure the IMU axes to match the North-East-Down (NED) frame
#' or to mark faulty sensor data as NA.
#' The data frame should have four columns:
#' \itemize{
#'    \item \emph{type}: A column specifying the tag type (`CAM` vs `CMD`).
#'    \item \emph{tag}: A column specifying the tag or sensor identifier. The tags indicated in this column should match the tag types in the \code{id.metadata} data frame.
#'    \item \emph{from}: A column indicating the original axis in the sensor's coordinate system.
#'    \item \emph{to}: A column specifying the target axis in the desired coordinate system.
#' }
#' Both signal and swap transformations are allowed. Transformations can be defined for different tags in case multiple tags were used.
#' @param id.metadata Data frame. Metadata about the IDs to associate with the processed data.
#' Must contain at least columns for ID and tag type.
#' @param id.col Character. Column name for ID in `id.metadata` (default: "ID").
#' @param tag.model.col Character. Column name for the tag model in `id.metadata` (default: "tag").
#' @param tag.type.col Character string or `NULL`. The name of the column in `id.metadata` that specifies the tag's type (e.g., "Camera", "MS").
#'   If `NULL` (default), the tag type is inferred from the animal `id` (e.g., by checking for substrings like `"CAM"`).
#'   When specified, `axis.mapping` must include a 'type' column with values matching the unique tag types found in this `id.metadata` column.
#' @param deploy.date.col Character. Column name for the tagging date in `id.metadata` (default: "tagging_date").
#' @param deploy.lon.col Character. Column name for longitude in sensor data (default: "deploy_lon").
#' @param deploy.lat.col Character. Column name for latitude in sensor data (default: "deploy_lat").
#' @param pop.date.col Character or NULL. Column name for the popup date in `id.metadata`.
#'   If NULL (default), popup date information will not be imported. When specified,
#'   the column must contain POSIXct values.
#' @param pop.lon.col Character or NULL. Column name for popup longitude in `id.metadata`.
#'   If NULL (default), popup longitude will not be imported. Must be specified
#'   together with `pop.date.col` and `pop.lat.col` to enable popup location integration.
#' @param pop.lat.col Character or NULL. Column name for popup latitude in `id.metadata`.
#'   If NULL (default), popup latitude will not be imported. Must be specified
#'   together with `pop.date.col` and `pop.lon.col` to enable popup location integration.
#' @param package.id.col Character or NULL. Column name for the package ID in `id.metadata`
#'   If NULL (default), no package ID information will be imported in the processed data.
#' @param package.id.col Character. Column name for the package id in `id.metadata` (default: "package_id").
#' @param paddle.wheel.col Character string specifying the name of the column in `id.metadata`
#' that indicates whether each tag was equipped with a magnetic paddle wheel for speed estimation.
#' The column should contain logical values (TRUE/FALSE) or binary values (1/0) where 1/TRUE indicates
#' presence of a paddle wheel. If NULL (default), the function assumes no paddle wheel data needs to be processed.
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
#' }
#' @param verbose Logical. If TRUE, the function will print detailed processing information. Defaults to TRUE.
#'
#' @return If \code{return.data = TRUE}, returns a list where each element contains the
#' processed sensor data for an individual folder. If \code{return.data = FALSE},
#' returns \code{NULL} invisibly. In all cases, data will be saved to disk if
#' \code{save.files = TRUE}.
#'
#' @seealso \link{importTagData}
#' @export


importTagData <- function(data.folders,
                          sensor.subdirectory = "CMD",
                          wc.subdirectory = NULL,
                          timezone = "UTC",
                          import.mapping = NULL,
                          axis.mapping = NULL,
                          id.metadata,
                          id.col = "ID",
                          tag.model.col = "tag",
                          tag.type.col = NULL,
                          deploy.date.col = "tagging_date",
                          deploy.lon.col = "deploy_lon",
                          deploy.lat.col = "deploy_lat",
                          pop.date.col = NULL,
                          pop.lon.col = NULL,
                          pop.lat.col = NULL,
                          package.id.col = NULL,
                          paddle.wheel.col = NULL,
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

  # validate that at least one output method is selected
  if (!save.files && !return.data) {
    stop("Both 'save.files' and 'return.data' cannot be FALSE - this would result in data loss. ",
         "Please set at least one to TRUE.", call. = FALSE)
  }

  # validate `id.metadata` argument
  if(!is.data.frame(id.metadata)) stop("`id.metadata` must be a data frame or data table.", call. = FALSE)

  # convert to data.frame if it is a data.table
  if (inherits(id.metadata, "data.table")) id.metadata <- as.data.frame(id.metadata)

  # validate import.mapping
  if (!is.null(import.mapping)) {
    if (!is.data.frame(import.mapping)) {
      stop("`import.mapping` must be a data frame or NULL.", call. = FALSE)
    }
    required_cols_mapping <- c("colname", "sensor", "units")
    if (!all(required_cols_mapping %in% names(import.mapping))) {
      stop("`import.mapping` must be a data frame with columns: 'colname', 'sensor', and 'units'.", call. = FALSE)
    }
    # define valid sensor names
    valid_sensors <- c("date", "time", "datetime", "ax", "ay", "az", "gx", "gy", "gz",
                       "mx", "my", "mz", "depth", "temp", "paddle_speed", "paddle_freq")
    if (any(!import.mapping$sensor %in% valid_sensors)) {
      invalid_sensors <- unique(import.mapping$sensor[!import.mapping$sensor %in% valid_sensors])
      stop(paste0("Invalid sensor name(s) found in `import.mapping`: ",
                  paste(invalid_sensors, collapse = ", "),
                  ". Valid sensor names are: ", paste(valid_sensors, collapse = ", "), "."), call. = FALSE)
    }
    # validate units in import.mapping
    valid_units <- c("UTC", "m/s2", "g", "mrad/s", "rad/s", "deg/s", "uT", "C", "m", "m/s", "Hz", "")
    if (any(!import.mapping$units %in% valid_units)) {
      invalid_units <- unique(import.mapping$units[!import.mapping$units %in% valid_units])
      stop(paste0("Invalid unit(s) found in `import.mapping`: ",
                  paste(invalid_units, collapse = ", "),
                  ". Valid units are: ", paste(valid_units, collapse = ", "), "."), call. = FALSE)
    }
  }

  # validate orientation.mapping
  if(!is.null(axis.mapping)){
    # first ensure the required columns exist in axis.mapping
    if (!all(c("type", "tag", "from", "to") %in% colnames(axis.mapping))) stop("The 'axis.mapping' data frame must contain 'type', 'tag', 'from', and 'to' columns.", call. = FALSE)
    # issue warning in case any tag in axis.mapping is not present in id.metadata
    if (any(!axis.mapping$tag %in% id.metadata[[tag.model.col]])) warning_msg1 <- paste0("Some tags in axis.mapping are not present in the '", tag.model.col, "' column of 'id.metadata'\n")
    # scenario 1: tag.type.col is provided, use combined tag and type validation
    if (!is.null(tag.type.col)) {
      # issue warning in case any type in axis.mapping is not present in id.metadata
      if (any(!axis.mapping$type %in% id.metadata[[tag.type.col]])) warning_msg2 <- paste0("Some types in axis.mapping are not present in the '", tag.type.col, "' column of 'id.metadata'\n")
      # check if all tag + type combinations from id.metadata exist in axis.mapping
      metadata_tags <- paste(id.metadata[[tag.model.col]], id.metadata[[tag.type.col]], sep = "_")
      axis_mapping_tags <- paste(axis.mapping$tag, axis.mapping$type, sep = "_")
      missing_types <- setdiff(metadata_tags, axis_mapping_tags)
      if (length(missing_types) > 0) {
        # reconstruct original tag and type for the warning message
        missing_info <- strsplit(missing_types, "_")
        formatted_missing <- vapply(missing_info, function(x) paste0("- ", x[1], " ", x[2]), character(1))
        warning_msg3 <- paste0(
          "Some tag model x type combinations from id.metadata are missing in axis.mapping. ",
          "No axis transformations will be applied to these:\n",
          paste(formatted_missing, collapse = "\n"), "\n")
      }
    # scenario 2: tag.type.col is NULL, perform simpler tag model validation
    }else if (any(!id.metadata[[tag.model.col]] %in% axis.mapping$tag)) {
      missing_tags <- setdiff(id.metadata[[tag.model.col]], axis.mapping$tag)
      formatted_missing <- paste0("- ", missing_tags)
      warning_msg3 <- paste0(
        "Some tag models in id.metadata do not have corresponding entries in axis.mapping. ",
        "No axis transformations will be applied to these:\n",
        paste(formatted_missing, collapse = "\n"), "\n")
    }
  }

  # check if specified columns exists in id.metadata
  if(!id.col %in% names(id.metadata)) stop(paste("The specified id.col ('", id.col, "') was not found in id.metadata.", sep = ""), call. = FALSE)
  if(!tag.model.col %in% names(id.metadata)) stop(paste("The specified tag.model.col ('", tag.model.col, "') was not found in id.metadata.", sep = ""))
  if(!deploy.date.col %in% names(id.metadata)) stop(paste("The specified deploy.date.col ('", deploy.date.col, "') was not found in id.metadata.", sep = ""), call. = FALSE)
  if(!deploy.lon.col %in% names(id.metadata)) stop(paste("The specified deploy.lon.col ('", deploy.lon.col, "') was not found in id.metadata.", sep = ""), call. = FALSE)
  if(!deploy.lat.col %in% names(id.metadata)) stop(paste("The specified deploy.lat.col ('", deploy.lat.col, "') was not found in id.metadata.", sep = ""), call. = FALSE)
  if(!is.null(pop.date.col) && !pop.date.col %in% names(id.metadata)) stop(paste("The specified pop.date.col ('", pop.date.col, "') was not found in id.metadata.", sep = ""), call. = FALSE)
  if(!is.null(pop.lon.col) && !pop.lon.col %in% names(id.metadata)) stop(paste("The specified pop.lon.col ('", pop.lon.col, "') was not found in id.metadata.", sep = ""), call. = FALSE)
  if(!is.null(pop.lat.col) && !pop.lat.col %in% names(id.metadata)) stop(paste("The specified pop.lat.col ('", pop.lat.col, "') was not found in id.metadata.", sep = ""), call. = FALSE)
  if(!is.null(tag.type.col)){
    if(!tag.type.col %in% names(id.metadata)) stop(paste("The specified tag.type.col ('", tag.type.col, "') was not found in id.metadata.", sep = ""), call. = FALSE)
  }
  if(!is.null(package.id.col)){
    if(!package.id.col %in% names(id.metadata)) stop(paste("The specified package.id.col ('", package.id.col, "') was not found in id.metadata.", sep = ""), call. = FALSE)
  }
  if(!is.null(paddle.wheel.col)){
    if(!paddle.wheel.col %in% names(id.metadata)) stop(paste("The specified paddle.wheel.col ('", paddle.wheel.col, "') was not found in id.metadata.", sep = ""), call. = FALSE)
    paddle_values <- id.metadata[[paddle.wheel.col]]
    if(!is.logical(paddle_values)) {
      if(is.numeric(paddle_values)) {
        if(!all(paddle_values %in% c(0, 1, NA))) {
          stop("Numeric `paddle.wheel.col` must contain only 0, 1, or NA values.", call. = FALSE)
        }else{
          id.metadata[[paddle.wheel.col]] <- as.logical(paddle_values)
        }
      }else{
        stop("`paddle.wheel.col` must be either logical (TRUE/FALSE) or numeric binary (1/0).", call. = FALSE)
      }
    }
  }

  # check if deployment info exists for all individuals
  processing_ids <- basename(data.folders)
  processing_metadata <- id.metadata[id.metadata[[id.col]] %in% processing_ids, ]
  missing_deploy_idx <- is.na(processing_metadata[[deploy.date.col]]) | is.na(processing_metadata[[deploy.lon.col]]) | is.na(processing_metadata[[deploy.lat.col]])
  missing_deploy_ids <- processing_metadata[missing_deploy_idx, id.col]
  if (length(missing_deploy_ids) > 0) {
    stop(paste0(
      "The following IDs are missing required deployment information (datetime, lon, or lat):\n",
      paste(missing_deploy_info, collapse = ", "), "\n",
      "Please ensure all individuals have complete deployment metadata."
    ), call. = FALSE)
  }


  # check if deploy.date.col and pop.date.col are POSIXct
  if (!inherits(id.metadata[[deploy.date.col]], "POSIXct")) stop(paste0("Column '", deploy.date.col, "' must be of class POSIXct."), call. = FALSE)
  if (!is.null(pop.date.col) && !inherits(id.metadata[[pop.date.col]], "POSIXct")) stop(paste0("Column '", pop.date.col, "' must be of class POSIXct."), call. = FALSE)

  # validate timezone input
  if (!timezone %in% OlsonNames()) stop("Invalid timezone. Use OlsonNames() to see valid options.", call. = FALSE)

  # feedback for save files mode
  if (!is.logical(save.files)) stop("`save.files` must be a logical value (TRUE or FALSE).", call. = FALSE)

  # validate data.table threads if specified
  if (!is.null(data.table.threads)) {
    if (!is.numeric(data.table.threads) || data.table.threads < 1 || data.table.threads > parallel::detectCores()) {
      stop("data.table.threads must be between 1 and ", parallel::detectCores(), call. = FALSE)
    }
  }

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
  # Specify columns to import and respective mappings ##########################
  ##############################################################################

  # default column mappings 1
  CATS <- rbind(
    c("Date (UTC)", "date", "UTC"),
    c("Time (UTC)", "time", "UTC"),
    c("Accelerometer X [m/s\u00b2]", "ax", "m/s2"),
    c("Accelerometer Y [m/s\u00b2]", "ay", "m/s2"),
    c("Accelerometer Z [m/s\u00b2]", "az", "m/s2"),
    c("Gyroscope X [mrad/s]", "gx", "mrad/s"),
    c("Gyroscope Y [mrad/s]", "gy", "mrad/s"),
    c("Gyroscope Z [mrad/s]", "gz", "mrad/s"),
    c("Magnetometer X [\u00b5T]", "mx", "uT"),
    c("Magnetometer Y [\u00b5T]", "my", "uT"),
    c("Magnetometer Z [\u00b5T]", "mz", "uT"),
    c("Depth (200bar) [m]", "depth", "m"),
    c("Depth (200bar) 1 [m]", "depth", "m"),
    c("Depth (100bar) [m]", "depth", "m"),
    c("Temperature (depth) [\u00b0C]", "temp", "C"),
    c("Temperature (imu) [\u00b0C]", "temp", "C"),
    c("Temp. (magnet.) [\u00b0C]", "temp", "C")
  )

  # default column mappings 2
  CEIIA <- rbind(
    c("Date", "datetime", "UTC"),
    c("Ax (g)", "ax", "g"),
    c("Ay (g)", "ay", "g"),
    c("Az (g)", "az", "g"),
    c("Gx (\u00b0/s)", "gx", "deg/s"),
    c("Gy (\u00b0/s)", "gy", "deg/s"),
    c("Gz (\u00b0/s)", "gz", "deg/s"),
    c("Mx (\u00b5T)", "mx", "uT"),
    c("My (\u00b5T)", "my", "uT"),
    c("Mz (\u00b5T)", "mz", "uT"),
    c("Temperature (\u00b0C)", "temp", "C"),
    c("Temperature (\u00baC)", "temp", "C"),
    c("Depth (m)", "depth", "m"),
    c("Ticks/s", "paddle_freq", "Hz"),
    c("Velocity (m/s)", "paddle_speed", "m/s")
  )

  # combine all default mappings
  default_mappings <- as.data.frame(rbind(CATS, CEIIA), stringsAsFactors = FALSE)
  colnames(default_mappings) <- c("colname", "sensor", "units")

  # determine the actual mapping to use
  if (!is.null(import.mapping)) {
    active_mapping <- rbind(import.mapping, default_mappings)
  } else {
    active_mapping <- default_mappings
  }


  ##############################################################################
  # Initialize variables and print console messages ############################
  ##############################################################################

  # create lists to store processed data
  n_animals <- length(data.folders)
  data_list <- vector("list", length = n_animals)

  # set data.table threads if specified
  if (!is.null(data.table.threads)) {
    original_threads <- data.table::getDTthreads()
    data.table::setDTthreads(threads = data.table.threads)
    on.exit(data.table::setDTthreads(threads = original_threads), add = TRUE)
  }

  # feedback messages for the user
  cat(paste0(
    crayon::bold("\n=============== Importing Tag Data ===============\n"),
    "Fetching data from ", n_animals, " ", ifelse(n_animals == 1, "folder", "folders"), " - hang on tight!\n",
    crayon::bold("===================================================\n\n")
  ))

  # print folder names if verbose mode is enabled
  if(verbose) cat(crayon::blue(paste0("- ", basename(data.folders), collapse = "\n")), "\n\n")

  # print warning
  if (exists("warning_msg1")) message(warning_msg1)
  if (exists("warning_msg2")) message(warning_msg2)
  if (exists("warning_msg3")) message(warning_msg3)


  ##############################################################################
  # Import data for each folder ################################################
  ##############################################################################

  # iterate over each animal
  for (i in seq_along(data_files)) {

    # get animal ID
    id <- basename(data.folders)[i]

    # print progress to the console
    cat(crayon::bold(sprintf("[%d/%d] %s\n", i, n_animals, id)))

    # retrieve sensor and WC locations file paths for the current animal
    sensor_file <- data_files[i]
    locations_file <- wc_files[i]

    # check if the sensor file exists
    if (is.na(sensor_file) || !file.exists(sensor_file)) {
      message("Data file missing. Skipping.\n")
      data_list[[i]] <- NA
      next
    }

    # retrieve metadata for the current animal ID from the metadata table
    animal_info <- id.metadata[id.metadata[[id.col]]==id,]



    ############################################################################
    # Column mapping ###########################################################
    ############################################################################

    # read CSV header to get file column names
    csv_colnames <- names(data.table::fread(sensor_file, nrows = 0))

    # initialize a dataframe to store the file-specific mapping
    file_mapping <- data.frame(
      colname_in_csv = character(),
      sensor_name_out = character(),
      original_units_map = character(),
      stringsAsFactors = FALSE
    )

    # loop through target colnames and find their match in the CSV file
    for (j in 1:nrow(active_mapping)) {

      # get current targets
      target_col <- active_mapping$colname[j]
      target_sensor <- active_mapping$sensor[j]
      target_unit <- active_mapping$units[j]

      # find the corresponding column in imported colnames (try direct comparison)
      match_idx <- which(csv_colnames == target_col)

      # find the corresponding column by converting from Latin1 to UTF-8
      if(length(match_idx)==0){
        csv_colnames_utf8 <- iconv(csv_colnames, from = "Latin1", to = "UTF-8", sub = "byte")
        match_idx <- which(csv_colnames_utf8 == target_col)
      }

      # if any match
      if(length(match_idx)>0){
        # get the first match
        matched_name <- csv_colnames[match_idx[1]]
        # update file mapping
        file_mapping <- rbind(file_mapping, data.frame(colname_in_csv = matched_name, sensor_name_out = target_sensor, original_units_map = target_unit))
      }
    }

    # remove duplicated columns - keep only the first unique 'sensor_name_out' entry.
    file_mapping <- file_mapping[!duplicated(file_mapping$sensor_name_out), ]

    # check if all required sensor columns are present
    required_cols <- c("ax", "ay", "az", "gx", "gy", "gz", "mx", "my", "mz", "depth", "temp")
    optional_cols <- c("paddle_speed", "paddle_freq")

    missing_sensor_cols <- setdiff(required_cols, file_mapping$sensor_name_out)
    if (length(missing_sensor_cols) > 0) {
      warning(
        paste0(
          "Missing required columns: ", paste(missing_sensor_cols, collapse = ", "),
          ". Data not imported. Review file or mapping.\n"
        ),
        call. = FALSE,
        immediate. = TRUE
      )
      data_list[[i]] <- NA
      next
    }

    # ensure date/time combination logic has all parts if needed
    is_separate_datetime <- "date" %in% file_mapping$sensor_name_out && "time" %in% file_mapping$sensor_name_out
    is_single_datetime <- "datetime" %in% file_mapping$sensor_name_out
    if (!is_separate_datetime && !is_single_datetime) {
      stop(paste0("For ID ", id, ": Neither a 'datetime' column nor 'date' and 'time' columns were found in the CSV based on your mapping. Cannot proceed without time data."), call. = FALSE)
    }

    # run fread with correct select and colClasses
    selected_cols <- file_mapping$colname_in_csv
    col_classes <- setNames(rep("numeric", length(selected_cols)), selected_cols)

    # set colClasses for date/time columns to "character" for later parsing
    date_col_csv <- file_mapping$colname_in_csv[file_mapping$sensor_name_out == "date"][1]
    time_col_csv <- file_mapping$colname_in_csv[file_mapping$sensor_name_out == "time"][1]
    datetime_col_csv <- file_mapping$colname_in_csv[file_mapping$sensor_name_out == "datetime"][1]
    if (!is.na(date_col_csv)) col_classes[date_col_csv] <- "character"
    if (!is.na(time_col_csv)) col_classes[time_col_csv] <- "character"
    if (!is.na(datetime_col_csv)) col_classes[datetime_col_csv] <- "character"


    ############################################################################
    # Import data ##############################################################
    ############################################################################

    # import sensor data using fread
    sensor_data <- suppressWarnings(data.table::fread(sensor_file, select = selected_cols, colClasses = col_classes,
                                                      na.strings = c("NA", "", "NULL", "NaN", "-"), tz = timezone))


    # check if data was loaded correctly
    if (nrow(sensor_data) == 0 || ncol(sensor_data) == 0) {
      message(paste0("CSV file appears empty or has no recognized columns. No data imported. Please check file content or 'import.mapping'."))
      # mark as NA for consistency
      data_list[[i]] <- NA
      # skip to the next ID
      next
    }

    # rename columns based on file_mapping
    rename_map <- setNames(file_mapping$sensor_name_out, file_mapping$colname_in_csv)
    data.table::setnames(sensor_data, old = names(rename_map), new = unname(rename_map), skip_absent = TRUE)

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


    # retrieve tag model from metadata
    tag_model <- animal_info[[tag.model.col]]

    # retrieve tag type
    if(!is.null(tag.type.col)){
      # if tag.type.col specified in metadata, use the provided value
      tag_type <- animal_info[[tag.type.col]]
    }else{
      # otherwise: infer from animal ID string ("Camera" if contains "CAM")
      # default to "MS" (multisensor) when CAM not found in ID
      tag_type <- ifelse(grepl("CAM", id, fixed=T), "Camera", "MS")
    }

    # check package ID
    package_id <- NULL
    if(!is.null(package.id.col)) package_id <- animal_info[[package.id.col]]

    # print verbose
    if(verbose){
      output <- paste(tag_model, tag_type)
      if(!is.null(package_id)) output <- paste0(output, " | Package ID: ", package_id)
      cat(paste0(output, "\n"))
    }


    ############################################################################
    # Convert units ############################################################
    ############################################################################

    # handle datetime columns
    if (is_single_datetime) {
      # print message
      if (verbose) cat(paste0("--> Converting datetime column to POSIXct (TZ: '", timezone, "')\n"))
      # if `fread` parsed it directly (often does with `tz` arg), it's already POSIXct
      if (!inherits(sensor_data$datetime, "POSIXct")) {
        sensor_data[, datetime := as.POSIXct(datetime, tz = timezone) + 0.0001]
      } else {
        # ensure correct timezone even if fread parsed it
        attr(sensor_data$datetime, "tzone") <- timezone
      }
    } else if (is_separate_datetime) {
      # print message
      if (verbose) cat(paste0("--> Combining date and time to POSIXct (TZ: '", timezone, "')\n"))
      # combine date and time columns into POSIXct datetime
      sensor_data[, datetime := lubridate::fast_strptime(paste(date, time), "%d.%m.%Y %H:%M:%OS", tz = timezone) + 0.0001]
      # remove original date and time columns
      sensor_data[, c("date", "time") := NULL]
    }


    # define sensor groups, their standard target units, and corresponding messages.
    sensor_groups <- list(
      accel = list(cols = c("ax", "ay", "az"), standard_unit = "g", msg = paste0("--> Acceleration values converted from %s to g\n")),
      gyro = list(cols = c("gx", "gy", "gz"), standard_unit = "rad/s", msg = paste0("--> Gyroscope values converted from %s to rad/s\n")),
      mag = list(cols = c("mx", "my", "mz"), standard_unit = "uT", msg = paste0("--> Magnetometer values converted from %s to \U00B5T\n"))
    )

    # iterate through each predefined sensor group
    for (group_name in names(sensor_groups)) {
      group_info <- sensor_groups[[group_name]]
      group_cols <- group_info$cols
      group_message <- group_info$msg
      group_units <- group_info$standard_unit

      # filter `file_mapping` to only the relevant columns present
      rows_to_select <- file_mapping$sensor_name_out %in% group_cols & file_mapping$sensor_name_out %in% names(sensor_data)
      cols_to_process <- file_mapping[rows_to_select, c("sensor_name_out", "original_units_map")]

      # skip if no columns from this group are present or need processing
      if (nrow(cols_to_process) == 0) next

      # add the `target_unit` for each column using the pre-built lookup.
      cols_to_process$target_unit <- group_units

      # determine if any conversion is actually needed within this group
      conversion_needed <- any(
        cols_to_process$original_units_map != cols_to_process$target_unit &
          cols_to_process$target_unit != "" & !is.na(cols_to_process$target_unit))

      # if conversion is needed for any column in this group
      if (conversion_needed) {

        # apply conversions for each column in the group
        for (i_col in 1:nrow(cols_to_process)) {
          col_to_convert <- cols_to_process$sensor_name_out[i_col]
          original_unit <- cols_to_process$original_units_map[i_col]
          target_unit <- cols_to_process$target_unit[i_col]

          # only apply if this specific column actually needs conversion
          if (original_unit != target_unit && target_unit != "" && !is.na(target_unit)) {
            sensor_data[, (col_to_convert) := .convertUnits(get(col_to_convert), from.unit = original_unit, to.unit = target_unit)]
          }
        }

        # print the group message
        if (verbose) {
          cat(sprintf(group_message, original_unit))
        }
      }
    }

    # add ID column
    sensor_data[, ID := id]

    # order columns
    col_order <- c("ID", "datetime", required_cols)
    # ddd optional columns if they exist
    present_optional <- optional_cols[optional_cols %in% names(sensor_data)]
    if(length(present_optional) > 0) {
      col_order <- c(col_order, present_optional)
    }
    data.table::setcolorder(sensor_data, col_order)


    ############################################################################
    # Integrate known locations ################################################
    ############################################################################

    # create empty columns for "PTT", "datetime", "type", "lat", "lon" with NA values
    sensor_data[, `:=`(PTT = NA_character_,
                       position_type = NA_character_,
                       lat = NA_real_,
                       lon = NA_real_,
                       quality = NA_character_)]

    # process WC location data if available
    if (!is.na(locations_file) && file.exists(locations_file)) {
      # provide feedback to the user if verbose mode is enabled
      if(verbose) cat("--> Importing location data from Wildlife Computers tag\n")
      # import locations data
      locs_data <- data.table::fread(wc_files[i], select = c("Ptt", "Date", "Type", "Latitude", "Longitude", "Quality"))
      locs_data[, Date := as.POSIXct(Date, format = "%H:%M:%OS %d-%b-%Y", tz = timezone)]
      data.table::setnames(locs_data, c("PTT", "datetime", "position_type", "lat", "lon", "quality"))
      # remove rows with duplicate 'datetime' values
      locs_data <- locs_data[!duplicated(datetime)]
      # find the closest match for each location
      matched_indices <- sapply(locs_data$datetime, function(x) {
        which.min(abs(as.numeric(sensor_data$datetime) - as.numeric(x)))
      })
      # assign values only to the closest matching rows
      for (j in seq_along(matched_indices)) {
        idx <- matched_indices[j]
        sensor_data[idx, `:=`(PTT = locs_data$PTT[j],
                              position_type = locs_data$position_type[j],
                              lat = locs_data$lat[j],
                              lon = locs_data$lon[j],
                              quality = locs_data$quality[j])]
      }
    }


    # process location data from metadata if available
    metadata_locs <- list()
    # deployment location
    if (!is.na(animal_info[[deploy.date.col]]) && !is.na(animal_info[[deploy.lon.col]]) && !is.na(animal_info[[deploy.lat.col]])) {
      metadata_locs[[length(metadata_locs) + 1]] <- data.table::data.table(
        datetime = animal_info[[deploy.date.col]],
        position_type = "Metadata [deploy]",
        lat = animal_info[[deploy.lat.col]],
        lon = animal_info[[deploy.lon.col]])
    }
    # pop-up location
    if (!is.null(pop.date.col) && !is.null(pop.lon.col) && !is.null(pop.lat.col) &&
        !is.na(animal_info[[pop.date.col]]) && !is.na(animal_info[[pop.lon.col]]) && !is.na(animal_info[[pop.lat.col]])) {
      metadata_locs[[length(metadata_locs) + 1]] <- data.table::data.table(
        datetime = animal_info[[pop.date.col]],
        position_type = "Metadata [popup]",
        lat = animal_info[[pop.lat.col]],
        lon = animal_info[[pop.lon.col]])
    }
    # if there's at least one location to integrate
    if (length(metadata_locs) > 0) {
      # provide feedback to the user if verbose mode is enabled
      if (verbose) cat("--> Integrating deployment and/or pop-up locations from metadata\n")
      metadata_locs_dt <- data.table::rbindlist(metadata_locs)
      # find closest match in sensor_data
      matched_indices <- sapply(metadata_locs_dt$datetime, function(x) {
        which.min(abs(as.numeric(sensor_data$datetime) - as.numeric(x)))
      })
      # assign metadata locations to sensor_data
      for (j in seq_along(matched_indices)) {
        idx <- matched_indices[j]
        sensor_data[idx, `:=`(
          position_type = metadata_locs_dt$position_type[j],
          lat = metadata_locs_dt$lat[j],
          lon = metadata_locs_dt$lon[j]
        )]
      }
    }


    ############################################################################
    # Apply axis.mapping transformations #######################################
    ############################################################################

    # proceed only if axis.mapping is provided and tag_model is valid
    if (!is.null(axis.mapping) && !is.na(tag_model) && !is.na(tag_type)) {

      # filter the axis.mapping for the current tag model and tag type
      tag_mapping <- axis.mapping[axis.mapping$tag == tag_model & axis.mapping$type == tag_type,]

      # check if there is a specific mapping for this tag model and type
      if(nrow(tag_mapping) > 0){

        # provide feedback to the user if verbose mode is enabled
        if(verbose) cat(paste("--> Applying axis mapping\n"))

        # create a temporary copy of the data for simultaneous swaps
        temp_data <- data.table::copy(sensor_data)

        # change axis designation and direction to match the NED system
        for (row in 1:nrow(tag_mapping)) {
          from_axis <- tag_mapping$from[row]
          to_axis <- tag_mapping$to[row]
          if (to_axis == "NA") {
            # special case: set the original axis to NA
            temp_data[[from_axis]] <- NA_real_
          } else if (grepl("^\\-", to_axis)) {
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

      }else{
        if (verbose) cat(paste0("--> No specific axis mapping found for this tag, skipping transformations\n"))
        tag_mapping <- NA
      }
    }


    ############################################################################
    # Save relevant attributes #################################################
    ############################################################################

    # save original start and end datetimes
    first_datetime <- min(sensor_data$datetime)
    last_datetime <- max(sensor_data$datetime)

    # create deployment.info data frame
    deployment_info <- data.frame(
      datetime = animal_info[[deploy.date.col]],
      lon = animal_info[[deploy.lon.col]],
      lat = animal_info[[deploy.lat.col]]
    )

    # check if this tag has a paddle wheel
    has_paddle <- NULL
    if(!is.null(paddle.wheel.col)) has_paddle <- animal_info[[paddle.wheel.col]]

    # create new attributes to save relevant variables
    attr(sensor_data, 'nautilus.version') <- utils::packageVersion("nautilus")
    attr(sensor_data, 'id') <- id
    attr(sensor_data, 'directory') <- data.folders[i]
    attr(sensor_data, 'imported.columns') <- selected_cols
    attr(sensor_data, 'axis.mapping') <- tag_mapping
    attr(sensor_data, 'tag.model') <- tag_model
    attr(sensor_data, 'tag.type') <- tag_type
    attr(sensor_data, 'package.id') <- package_id
    attr(sensor_data, 'deployment.info') <- deployment_info
    attr(sensor_data, 'first.datetime') <- first_datetime
    attr(sensor_data, 'last.datetime') <- last_datetime
    attr(sensor_data, 'original.rows') <- rows
    attr(sensor_data, 'paddle.wheel') <- has_paddle
    attr(sensor_data, 'timezone') <- timezone
    attr(sensor_data, 'processing.date') <- Sys.time()


    ############################################################################
    # Save imported data #######################################################
    ############################################################################

    # provide feedback to the user if verbose mode is enabled
    if (verbose) cat("Done! Data imported successfully.\n")

    # save the processed data as an RDS file
    if(save.files){

      # print without newline and immediately flush output
      if (verbose) {
        cat("Saving file... ")
        flush.console()
      }

      # determine the output directory: use the specified output folder or the current data folder
      output_dir <- ifelse(!is.null(output.folder), output.folder, data.folders[i])

      # define the file suffix: use the specified suffix or default to an empty string
      suffix <- ifelse(!is.null(output.suffix), output.suffix, "")

      # construct the output file name
      output_file <- file.path(output_dir, paste0(id, suffix, ".rds"))

      # save the processed data
      saveRDS(sensor_data, output_file)

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
      data_list[[i]] <- sensor_data
    }

    # clear unused objects from the environment to free up memory
    rm(sensor_data)
    gc()

  }

  ##############################################################################
  # Return imported data #######################################################
  ##############################################################################

  # print message
  cat(crayon::bold("All done!\n"))

  # print time taken
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  cat(crayon::bold("Total execution time:"), sprintf("%.02f", as.numeric(time.taken)), base::units(time.taken), "\n\n")


  # return imported data or NULL based on return.data parameter
  if (return.data) {
    # assign names to the data list
    names(data_list) <- basename(data.folders)
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
