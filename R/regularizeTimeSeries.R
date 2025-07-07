#######################################################################################################
# Function to check and correct irregular time series in tag data #####################################
#######################################################################################################

#' Regularize irregular high-frequency biologging time series
#'
#' @description
#' `regularizeTimeSeries` processes high-frequency time series data from biologging sensors to
#' address irregularities such as time gaps and jitter. The function performs several key steps:
#' \enumerate{
#'   \item Detects and quantifies time gaps and temporal jitter within the series.
#'   \item Regularizes the time series to a consistent sampling interval.
#'   \item Assigns original records to the nearest regular timestamp within a defined threshold.
#'   \item Applies gap-filling strategies (e.g., linear, spline, LOCF interpolation) for
#'         short gaps, while leaving larger gaps as `NA`.
#' }
#'
#' @param data Input data, which can be either:
#'   \itemize{
#'     \item A list of data frames/tables (each containing sensor data for one individual)
#'     \item A single data frame/table
#'     \item Character vector of paths to RDS files containing sensor data
#'   }
#' @param id.col Character. Name of the column containing unique identifier for each tag/animal.
#' Used when input is a single data.frame that needs splitting. Default "ID".
#' @param datetime.col Character. Name of the datetime column. Must contain POSIXct values.
#'   Default "datetime".
#' @param time.threshold Numeric. Maximum allowed deviation (in seconds) from regular
#'   intervals. If `NULL` (default), automatically calculates as half of the nominal
#'   sampling interval. Records beyond this threshold won't be assigned to regular times.
#' @param gap.threshold Numeric. Maximum gap duration (in seconds) for interpolation.
#'   Gaps \eqn{\le} this value will be interpolated; larger gaps remain NA. Set to 0 to disable
#'   interpolation. Default is 5 seconds.
#' @param interpolation.method Character. Interpolation method for small gaps. One of:
#'   \itemize{
#'     \item "linear" (default) - Linear interpolation via `zoo::na.approx`
#'     \item "spline" - Spline interpolation via `zoo::na.spline`
#'     \item "locf" - Last observation carried forward via `zoo::na.locf`
#'   }
#' @param return.data Logical. Return processed data in memory? If FALSE and
#'   `save.files = FALSE`, issues warning. Default TRUE.
#' @param save.files Logical. Save processed data as RDS files? Default FALSE.
#' @param output.folder Character. Directory to save output files when `save.files = TRUE`.
#'   If NULL (default) and input is file paths, saves in same directory as input files.
#' @param output.suffix Character. A suffix to append to the file name when saving.
#' This parameter is only used if `save.files = TRUE`.
#' @param verbose Logical. Print progress messages? Default TRUE.
#'
#' @return Depending on input and parameters:
#' \itemize{
#'   \item If single dataset input: Returns regularized data.table
#'   \item If list/multiple files input: Returns list of regularized data.tables (when `return.data = TRUE`)
#'   \item If `return.data = FALSE`: Returns invisibly NULL (use with `save.files = TRUE`)
#' }
#'
#' @details
#' The regularization algorithm proceeds as follows for each individual's dataset:
#' \enumerate{
#'   \item The **median sampling interval** is calculated from the observed `datetime`
#'         differences. This median is used as the nominal interval for regularization.
#'   \item A **complete, regular time sequence** is generated, spanning from the
#'         first to the last timestamp of the original data, at the calculated
#'         nominal interval.
#'   \item Each original record is then **assigned to its nearest timestamp** in
#'         this regular sequence. An assignment is only considered valid if the
#'         time difference between the original record and the nearest regular
#'         timestamp is within the `time.threshold`. Records outside this threshold
#'         (or where no original record is near a regular timestamp) result in `NA`
#'         values in the data columns for those regular time points.
#'   \item For any `NA` values introduced during regularization (or existing initially),
#'         **gap-filling strategies** are applied. Gaps equal to or shorter than
#'         `gap.threshold` are interpolated using the specified `interpolation.method`.
#'         Longer gaps remain as `NA` to avoid spurious data generation.
#' }
#'
#' @note For large datasets, consider processing in batches with `save.files = TRUE` and
#' `return.data = FALSE` to avoid memory overload.
#'
#' @seealso
#' \link{importTagData}
#' \link{filterDeploymentData}
#' \code{\link[zoo]{na.approx}} for interpolation methods
#' \code{\link[data.table]{as.data.table}} for efficient data handling
#'
#' @importFrom data.table as.data.table setorder setnames setcolorder
#' @importFrom zoo na.approx na.spline na.locf
#' @importFrom stats median mad
#' @export

regularizeTimeSeries <- function(data,
                                 id.col = "ID",
                                 datetime.col = "datetime",
                                 time.threshold = NULL,
                                 gap.threshold = 5,
                                 interpolation.method = "linear",
                                 return.data = TRUE,
                                 save.files = FALSE,
                                 output.folder = NULL,
                                 output.suffix = NULL,
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
    if (!id.col %in% names(data)) {
      stop("Input data must contain a valid id.col when not provided as a list.", call. = FALSE)
    }
    data <- split(data, data[[id.col]])
  }

  # feedback for save files mode
  if (!is.logical(save.files)) stop("`save.files` must be a logical value (TRUE or FALSE).", call. = FALSE)

  # validate that at least one output method is selected
  if (!save.files && !return.data) {
    stop("Both 'save.files' and 'return.data' cannot be FALSE - this would result in data loss. ",
         "Please set at least one to TRUE.", call. = FALSE)
  }

  # define required columns
  required_cols <- c(datetime.col)

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
      if (!inherits(dataset[[datetime.col]], "POSIXct")) stop("The datetime column must be of class 'POSIXct'.", call. = FALSE)
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

  # validate interpolation method
  interpolation.method <- match.arg(interpolation.method, choices = c("linear", "spline", "locf"))

  # check threshold values
  if (!is.null(time.threshold)) {
    if (!is.numeric(time.threshold)) stop("time.threshold must be numeric", call. = FALSE)
    if (time.threshold <= 0) stop("time.threshold must be positive", call. = FALSE)
  }

  if (!is.numeric(gap.threshold) || gap.threshold < 0) stop("gap.threshold must be a non-negative number", call. = FALSE)

  ##############################################################################
  # Process data for each dataset ##############################################
  ##############################################################################

  # initialize results list if returning data
  n_animals <- length(data)
  if (return.data) results <- vector("list", length = n_animals)

  # feedback messages for the user
  cat(paste0(
    crayon::bold("\n============== Regularizing Time Series ==============\n"),
    "Performing time checks and regularization for ", n_animals, " ", ifelse(n_animals == 1, "tag", "tags"), "\n",
    crayon::bold("======================================================\n\n")
  ))


  if (verbose & is_filepaths) cat("Loading data from RDS files...\n\n")

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
      if (!inherits(individual_data[[datetime.col]], "POSIXct")) stop("The datetime column in file '", basename(file_path), "' must be of class 'POSIXct'.", call. = FALSE)
      if (is.null(attr(individual_data, "nautilus.version"))) {
        message(paste0("Warning: File '", basename(file_path), "' was likely not processed via importTagData(). It is strongly recommended to run it through importTagData() to ensure proper formatting."))
      }

      # add ID if not present
      id <- unique(individual_data[[id.col]])[1]


    } else {
      # data is already in memory (list of data frames/tables)
      id <- names(data)[i]
      individual_data <- data[[i]]
    }

    # print current ID
    if (verbose) cat(crayon::bold(sprintf("[%d/%d] %s\n", i, n_animals, id)))

    # skip NULL or empty elements in the list
    if (is.null(individual_data) || nrow(individual_data) == 0) {
      if (verbose) cat("Skipping empty dataset\n\n")
      next
    }

    # convert to data.table if not already
    if (!data.table::is.data.table(individual_data)) individual_data <- data.table::as.data.table(individual_data)

    # ensure data is ordered by datetime
    data.table::setorderv(individual_data, cols = datetime.col)

    # store original attributes, excluding internal ones
    discard_attrs <- c("row.names", "class", ".internal.selfref", "names")
    original_attributes <- attributes(individual_data)
    original_attributes <- original_attributes[!names(original_attributes) %in% discard_attrs]


    ############################################################################
    # Identify temporal irregularities #########################################
    ############################################################################

    # calculate time differences
    time_diffs <- round(as.numeric(diff(individual_data[[datetime.col]])), 6)

    # estimate nominal sampling rate
    nominal_interval <- stats::median(time_diffs, na.rm = TRUE)
    sampling_freq <- round(1 / nominal_interval, 1)

    # verbose update
    if(verbose){
      #cat("───────────────────────────────────────\n")
      cat("----------------------------------------\n")
      cat(sprintf("\u00B7 Sampling: %sHz | %ss nominal interval\n",
                  sampling_freq, round(nominal_interval, 2)))
    }

    # auto-detect sampling frequency and threshold if not provided
    if (is.null(time.threshold)) {
      time_threshold <- nominal_interval / 2
      if (verbose) cat(sprintf("\u00B7 Time threshold: %.3fs (auto-detected)\n", time_threshold))
    } else {
      time_threshold <- time.threshold
      if (verbose) cat(sprintf("\u00B7 Time threshold: %.3fs (user-specified)\n", time_threshold))
    }

    # check if regularization is needed (any time differences exceed threshold)
    needs_regularization <- any(time_diffs > (nominal_interval + time_threshold), na.rm = TRUE)

    # apply regularization if required
    if (needs_regularization) {

      # calculate the number and percentage of irregular intervals before the output section
      irregular_count <- sum(time_diffs > (nominal_interval + time_threshold), na.rm = TRUE)
      irregular_pct <- irregular_count / length(time_diffs) * 100

      # find the longest gap in original data
      longest_gap <- max(time_diffs, na.rm = TRUE)

      # verbose update
      if(verbose) cat(crayon::red(sprintf("\u00B7 Irregularities found: %d (%.1f%%) with max gap: %s\n",
                              irregular_count, irregular_pct, .formatDuration(longest_gap))))

      # print separator
      if(verbose) {
        cat("----------------------------------------\n")
        cat("Regularizing time series\n")
      }

      # create regular time sequence covering the full range of datetimes
      start_time <- min(individual_data[[datetime.col]])
      end_time <- max(individual_data[[datetime.col]])
      regular_times <- seq(from = start_time, to = end_time, by = nominal_interval)

      # create a data.table with the regular time sequence
      complete_data <- data.table::data.table(regular_time = regular_times)
      data.table::setnames(complete_data, "regular_time", datetime.col)

      # add ID to all rows in the complete dataset
      complete_data[, (id.col) := id]

      # duplicate the datetime column in the original data for later comparison
      individual_data[, obs_datetime := .SD[[1]], .SDcols = datetime.col]

      # define which columns to transfer from original data (excluding datetime and ID)
      cols_to_copy <- setdiff(names(individual_data), c(datetime.col, id.col))

      # set keys for rolling join
      data.table::setkeyv(individual_data, datetime.col)
      data.table::setkeyv(complete_data, datetime.col)

      # perform a rolling join: for each regular time, find the nearest observation
      complete_data[individual_data, (cols_to_copy) := mget(paste0("i.", cols_to_copy)), roll = "nearest"]

      # calculate time difference between the regular time and the matched observation time
      complete_data[, time_diff := abs(as.numeric(get(datetime.col) - obs_datetime))]

      # identify valid matches within the acceptable time difference threshold
      valid_matches <- complete_data[, (time_diff <= time_threshold) | is.na(time_diff)]

      # count how many new time steps were added
      added_steps <- nrow(complete_data) - nrow(individual_data)
      # convert to percentage
      pct_added <- added_steps / nrow(individual_data) * 100

      # print a summary if verbose is enabled
      if (verbose) {
        cat(sprintf("\u2713 Added %s time steps (%.2f%% increase)\n", .formatNumber(added_steps), pct_added))
        # print separator
        cat("----------------------------------------\n")
      }

      # set values outside threshold to NA (excluding datetime and ID columns)
      data_cols <- setdiff(names(complete_data), c(datetime.col, id.col, "obs_datetime", "time_diff"))
      for (col in data_cols) {
        complete_data[!valid_matches, (col) := NA]
      }

      # clean up helper columns
      complete_data[, obs_datetime := NULL]
      complete_data[, time_diff := NULL]

    # no regularization required
    } else {
      if(verbose) cat("\u00B7 Irregularities found: None\n")
      if(verbose) cat("----------------------------------------\n")
      complete_data <- individual_data
    }


    ############################################################################
    # Apply interpolation for small gaps #######################################
    ############################################################################

    if (gap.threshold > 0) {

      # columns to check for gaps (NA values)
      cols_to_check <- c("ax", "ay", "az", "mx", "my", "mz", "gx", "gy", "gz", "depth", "temp")
      # check if any of these columns have NA values
      has_gaps <- any(sapply(complete_data[, ..cols_to_check], function(col) any(is.na(col))))

      if (has_gaps) {

        # verbose update - state interpolation method and threshold
        if (verbose) cat(sprintf("Applying %s interpolation for gaps \u2264 %.2f secs\n", interpolation.method, gap.threshold))

        # create a logical matrix indicating NA values in the selected columns
        na_matrix <- is.na(complete_data[, ..cols_to_check])

        # Identify rows where *any* of the selected columns have NA
        na_pattern <- apply(na_matrix, 1, any)

        # use rle to find consecutive NA runs
        na_runs <- rle(na_pattern)

        # calculate gap durations in seconds
        gap_durations <- na_runs$lengths[na_runs$values] * nominal_interval

        # classify gaps by interpolation threshold
        small_gaps <- gap_durations <= gap.threshold
        large_gaps <- gap_durations > gap.threshold

        # count gaps
        n_small_gaps <- sum(small_gaps)
        n_large_gaps <- sum(large_gaps)

        # calculate total durations
        total_small_duration <- sum(gap_durations[small_gaps])
        total_large_duration <- sum(gap_durations[large_gaps])

        # interpolate small gaps
        numeric_cols <- names(complete_data)[sapply(complete_data, is.numeric)]
        numeric_cols <- setdiff(numeric_cols, c(datetime.col, id.col))

        for (col in numeric_cols) {
          if (interpolation.method == "linear") {
            complete_data[[col]] <- zoo::na.approx(complete_data[[col]],
                                                   maxgap = floor(gap.threshold / nominal_interval),
                                                   na.rm = FALSE)
          } else if (interpolation.method == "spline") {
            complete_data[[col]] <- zoo::na.spline(complete_data[[col]],
                                                   maxgap = floor(gap.threshold / nominal_interval),
                                                   na.rm = FALSE)
          } else if (interpolation.method == "locf") {
            maxgap_points <- floor(gap.threshold / nominal_interval)
            # first, forward fill
            complete_data[[col]] <- zoo::na.locf(complete_data[[col]], maxgap = maxgap_points,
                                                 na.rm = FALSE, fromLast = FALSE)
            # then, backward fill for any remaining NAs at the start or between filled values
            complete_data[[col]] <- zoo::na.locf(complete_data[[col]], maxgap = maxgap_points,
                                                 na.rm = FALSE, fromLast = TRUE)
          }
        }

        # verbose reporting of gap interpolation
        if (verbose) {
          if (n_small_gaps > 0) {
            cat(sprintf("\u2713 Filled %s gap%s (total duration: %s)\n",
                        .formatNumber(n_small_gaps),
                        ifelse(n_small_gaps == 1, "", "s"),
                        .formatDuration(total_small_duration)))
          }
          if (n_large_gaps > 0) {
            cat(sprintf("\u00B7 Skipped %s large gap%s (total duration: %s)\n",
                        .formatNumber(n_large_gaps),
                        ifelse(n_large_gaps == 1, "", "s"),
                        .formatDuration(total_large_duration)))
          }
        }

        # print separator
        if(verbose) cat("----------------------------------------\n")
      }
    }

    ############################################################################
    # Restore attributes #######################################################
    ############################################################################

    # restore the original attributes
    for (attr_name in names(original_attributes)) {
      attr(complete_data, attr_name) <- original_attributes[[attr_name]]
    }

    # add attribute indicating if regularization was performed
    attr(complete_data, "regularization.performed") <- needs_regularization

    # set column order to have ID and datetime first
    desired_order <- c(id.col, datetime.col, setdiff(names(complete_data), c(id.col, datetime.col)))
    data.table::setcolorder(complete_data, desired_order)


    ############################################################################
    # Save regularized data ####################################################
    ############################################################################

    # save the processed data as an RDS file
    if (save.files) {

      # print without newline and immediately flush output
      if (verbose) {
        cat("Saving file... ")
        flush.console()
      }

      # determine the output directory
      if (!is.null(output.folder)) {
        output_dir <- output.folder
      } else if (is_filepaths) {
        output_dir <- dirname(data[i])
      } else {
        output_dir <- "./"
      }

      # define the file suffix: use the specified suffix or default to an empty string
      suffix <- ifelse(!is.null(output.suffix), output.suffix, "")

      # construct the output file name
      output_file <- file.path(output_dir, paste0(id, suffix, ".rds"))

      # save the processed data
      saveRDS(complete_data, output_file)

      # overwrite the line with completion message
      if (verbose) {
        cat("\r")
        cat(rep(" ", getOption("width")-1))
        cat("\r")
        cat(sprintf("\u2713 Saved: %s\n", basename(output_file)))
      }
    }

    # print empty line
    if (verbose) cat("\n")

    # store processed sensor data in the results list if needed
    if (return.data) {
      results[[i]] <- complete_data
    }

    # clear unused objects from the environment to free up memory
    rm(individual_data, complete_data)
    gc(verbose = FALSE)
  }


  ##############################################################################
  # Return regularized data ####################################################
  ##############################################################################

  # print message
  if (verbose) cat(crayon::bold("Done and dusted!\n"))

  # print time taken
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  if (verbose) {
    cat(crayon::bold("Total execution time:"), sprintf("%.2f", as.numeric(time.taken)),
        attr(time.taken, "units"), "\n\n")
  }

  # return regularized data or NULL based on return.data parameter
  if (return.data) {
    # assign names to the data list
    if (is_filepaths) {
      names(results) <- sapply(data, function(x) tools::file_path_sans_ext(basename(x)))
    } else {
      names(results) <- names(data)
    }

    # if single dataset, return just the dataset (not a list)
    if (length(results) == 1 && !is.list(data)) {
      return(results[[1]])
    } else {
      return(results)
    }
  } else {
    return(invisible(NULL))
  }
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
