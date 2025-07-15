#######################################################################################################
# Function to extract features from a sliding window #################################################
#######################################################################################################

#' Extract Features from Sensor Data Using Sliding or Aggregated Windows
#'
#' This function calculates specified metrics (e.g., mean, standard deviation, circular statistics) for selected variables over a sliding window.
#' It can either retain the temporal resolution of the dataset or aggregate data into distinct, non-overlapping windows.
#' This is useful for preparing the dataset for machine learning or other analytical methods that require a structured set of features.
#'
#' @param data A list of tables/data frames (one per individual), a single aggregated data table/data frame,
#' or a character vector of file paths to RDS files containing sensor data.
#' If not a list or file paths, splits data by `id.col` (must be present in attributes).
#' Must include the variables specified in `variables`.
#' @param variables (Optional) A character vector of column names from `data` for which to calculate metrics.
#' If `parameter.grid` is supplied, this argument is ignored. If both `variables` and `parameter.grid` are `NULL`, an error will be thrown.
#' @param metrics (Optional) A character vector specifying the metrics to calculate. Supported metrics:
#' "mean", "median", sd", "min", "max", "sum", "range","iqr", "mad", "skewness", "kurtosis", "energy", "entropy", "mrl".
#' If `parameter.grid` is supplied, this argument is ignored. If both `metrics` and `parameter.grid` are `NULL`, an error will be thrown.
#' @param parameter.grid (Optional) A data frame with two columns: `variable` and `metric`.
#' If supplied, the function will use this grid to determine which variable-metric combinations to calculate.
#' If `NULL` (default), the `parameter.grid` will be generated from the `variables` and `metrics` arguments.
#' @param id.col A character string specifying the column containing individual IDs. Required if `data` is a single data frame.
#' @param datetime.col A character string specifying the column in `data` containing datetime information
#' (in POSIXct format). Default: `"datetime"`.
#' @param window.size An integer specifying the size of the sliding or aggregation window, in seconds.
#' The actual number of steps in the window is determined based on the sampling interval of the data.
#' @param aggregate Logical. If TRUE, uses non-overlapping windows for initial feature extraction.
#'  If FALSE (default), uses sliding windows.
#' @param downsample.to NULL or numeric (seconds). If specified, aggregates
#'  features into non-overlapping windows after initial extraction. Works with both
#'  aggregate=TRUE/FALSE. For example:
#'    - aggregate=FALSE, downsample.interval=60: 5s sliding → 1m bins
#'    - aggregate=TRUE, downsample.interval=60: 5s epochs → 1m bins
#' @param response.col (Optional) A character string specifying the column in `data` containing response or annotated labels (e.g., feeding events).
#' It must be binary (i.e., containing values 0 or 1).
#' @param circular.variables Character vector specifying variables that should be treated as circular (e.g., angles in degrees). Defaults to "heading" and "roll"
#' @param response.aggregation Optional. Method to aggregate `response.col` when `window.size > 1`:
#'   - `"majority"`: Assigns `1` if >50% of window values are `1` (default).
#'   - `"any"`: Assigns `1` if **any** value in the window is `1`.
#'  Ignored if `response.col = NULL`.
#' @param return.data Logical. If `TRUE`, the processed data will be returned as a list. Defaults to `TRUE`.
#' @param save.files Logical. If `TRUE`, the processed data for each individual will be saved as a separate RDS file. Defaults to `FALSE`.
#' @param output.folder (Optional) A character string specifying the directory where processed RDS files should be saved.
#' If `NULL` and `save.files = TRUE`: if `data` was provided as file paths, the output will be saved in the same directory as the input files;
#' otherwise, it defaults to the current working directory.
#' @param output.suffix (Optional) A character string to append to the filename of the output RDS files (e.g., "_features").
#' @param n.cores The number of processor cores to use for parallel computation. Defaults to 1 (single-core).

#' @details
#' This function supports the following metrics:
#' - "mean": The average value of the data. It provides the central tendency of the dataset within each window.
#' - "median": The middle value of the data when ordered. It divides the dataset into two equal halves, providing a robust measure of central tendency that is less affected by outliers.
#' - "sd": The standard deviation of the data. It quantifies the amount of variation or dispersion of the data values within each window.
#' - "range": The range (max - min) of the data. It represents the difference between the maximum and minimum values within each window.
#' - "min": The minimum value of the data. It indicates the smallest value in the dataset within each window.
#' - "max": The maximum value of the data. It indicates the largest value in the dataset within each window.
#' - "iqr": The interquartile range of the data. It is the difference between the 75th percentile (Q3) and the 25th percentile (Q1) of the dataset, measuring the spread of the middle 50% of the data.
#' - "sum": The sum of the data. It adds up all the values in the dataset within each window.
#' - "rate": Average rate of change per second of the data values. This metric quantifies how fast the variable changes over time within each window.
#' - "energy": The sum of the squares of the data. It reflects the magnitude of the values, commonly used in signal processing and analysis of periodicity.
#' - "skewness": The skewness of the data, a measure of the asymmetry of the distribution. A positive value indicates a right-skewed distribution, while a negative value indicates a left-skewed distribution.
#' - "kurtosis": The kurtosis of the data, a measure of the "tailedness" or sharpness of the distribution. High kurtosis indicates heavy tails, while low kurtosis indicates lighter tails.
#' - "entropy": The entropy of the data, which measures the unpredictability or randomness. High entropy values indicate more uncertainty or randomness in the data, while low values suggest more predictability.
#'
#'
#' For circular variables, the function supports:
#' - "mean": Directional average accounting for wrap-around.
#' - "median": Middle value in circular space.
#' - "sd": Angular dispersion (0° = no dispersion).
#' - "range": Shortest arc containing all angles.
#' - "iqr": Middle 50% of angles in circular space.
#' - "rate": Average rate of angular change per second, accounting for wrap-around. This represents how quickly the circular variable changes over time.
#' - "mrl": Mean resultant length, indicating concentration of circular values. (0 = uniform, 1 = identical directions).

#' Pitch (-90° to 90°) is treated as linear (by default) since it represents inclination rather than direction.


#' @return
#' A list of data frames, each corresponding to an individual animal, containing the calculated features for the specified
#' variables and metrics. The structure of the output depends on the `aggregate` parameter:
#' - If `aggregate = TRUE`: Each data frame will have fewer rows, with one row per aggregation window. The datetime column
#'   will be adjusted to represent the midpoint of each aggregation window, if present. The response column, if specified,
#'   will also be aggregated (e.g., summed or averaged) based on the corresponding window.
#' - If `aggregate = FALSE`: Each data frame will retain the original temporal resolution, with calculated metrics for each
#'   sliding window. The datetime column and response column (if provided) will not be aggregated but will instead match
#'   the temporal alignment of the sliding windows.
#'
#' @export


extractFeatures <- function(data,
                            variables = NULL,
                            metrics = NULL,
                            parameter.grid = NULL,
                            id.col = "ID",
                            datetime.col = "datetime",
                            window.size = 5,
                            aggregate = FALSE,
                            downsample.to = NULL,
                            response.col = NULL,
                            response.aggregation = "majority",
                            circular.variables = c("heading", "roll"),
                            return.data = TRUE,
                            save.files = FALSE,
                            output.folder = NULL,
                            output.suffix = NULL,
                            n.cores = 1){

  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

  # measure running time
  start.time <- Sys.time()

  # determine if data is a character vector of RDS file paths
  is_filepaths <- is.character(data)

  # if data is not file paths and not a list, split it by id.col
  if (!is_filepaths && !is.list(data)) {
    if (!id.col %in% names(data)) {
      stop("Input data must contain a valid 'id.col' when not provided as a list or file paths.", call. = FALSE)
    }
    data <- split(data, f = data[[id.col]])
  }

  # validate output parameters
  if (!is.logical(save.files)) stop("`save.files` must be a logical value (TRUE or FALSE).", call. = FALSE)
  if (!is.logical(return.data)) stop("`return.data` must be a logical value (TRUE or FALSE).", call. = FALSE)

  # ensure at least one output method is selected
  if (!save.files && !return.data) {
    stop("Both 'save.files' and 'return.data' cannot be FALSE - this would result in no output. ",
         "Please set at least one to TRUE.", call. = FALSE)
  }

  # validate downsample.to parameter
  if (!is.null(downsample.to)) {
    if (!is.numeric(downsample.to) || downsample.to <= 0) {
      stop("`downsample.to` must be a positive numeric value in seconds.", call. = FALSE)
    }
  }

  # warning for conflicting arguments
  if (!is.null(parameter.grid) && (!is.null(variables) || !missing(metrics))) {
    warning("Both `parameter.grid` and `variables`/`metrics` were supplied. `variables` and `metrics` will be ignored in favor of `parameter.grid`.", call. = FALSE, immediate. = TRUE)
  }

  # validate circular variables
  if (!is.null(circular.variables)) {
    if (!is.character(circular.variables)) {
      stop("`circular.variables` must be a character vector.", call. = FALSE)
    }
  }

  # define valid metrics
  valid_linear_metrics <- c("mean", "median", "sd", "range", "min", "max", "iqr", "mad", "sum", "rate", "energy", "skewness", "kurtosis", "entropy")
  valid_circular_metrics <- c("mean", "median", "sd", "range", "iqr", "mrl", "rate")

  # determine the parameter grid to use
  if (is.null(parameter.grid)) {
    # if not supplied, generate from variables and metrics
    if (is.null(variables) || length(variables) == 0) stop("If `parameter.grid` is not supplied, `variables` must be provided and not empty.", call. = FALSE)
    if (is.null(metrics) || length(metrics) == 0) stop("If `parameter.grid` is not supplied, `metrics` must be provided and not empty.", call. = FALSE)
    parameter_grid <- expand.grid(variable = variables, metric = metrics, stringsAsFactors = FALSE)
    parameter_grid <- parameter_grid[order(parameter_grid$variable),]
  } else {
    # if supplied, validate it
    if (!is.data.frame(parameter.grid)) stop("`parameter.grid` must be a data frame.", call. = FALSE)
    if (!all(c("variable", "metric") %in% names(parameter.grid))) stop("`parameter.grid` must contain 'variable' and 'metric' columns.", call. = FALSE)
    # convert metrics in the supplied grid to lowercase for consistency
    parameter.grid$metric <- tolower(parameter.grid$metric)
    parameter_grid <- parameter.grid
  }

  # validate metrics for circular and linear variables
  if (!is.null(circular.variables)) {
    circular_rows <- parameter_grid$variable %in% circular.variables
    invalid_circular <- parameter_grid[circular_rows & !(parameter_grid$metric %in% valid_circular_metrics), ]
    if (nrow(invalid_circular) > 0) {
      stop(paste(
        "Invalid metrics for circular variables:",
        paste(unique(invalid_circular$metric), collapse = ", "),
        "\nValid circular metrics:",
        paste(valid_circular_metrics, collapse = ", ")
      ), call. = FALSE)
    }
  }

  # validate linear metrics
  linear_rows <- if (!is.null(circular.variables)) {
    !(parameter_grid$variable %in% circular.variables)
  } else {
    rep(TRUE, nrow(parameter_grid))
  }
  invalid_linear <- parameter_grid[linear_rows & !(parameter_grid$metric %in% valid_linear_metrics), ]
  if (nrow(invalid_linear) > 0) {
    stop(paste(
      "Invalid metrics for linear variables:",
      paste(unique(invalid_linear$metric), collapse = ", "),
      "\nValid linear metrics:",
      paste(valid_linear_metrics, collapse = ", ")
    ), call. = FALSE)
  }

  # extract variables from the parameter grid for subsequent checks
  variables_to_check <- unique(parameter_grid$variable)

  # validate output parameters and check data existence/format
  if (is_filepaths) {
    missing_files <- data[!file.exists(data)]
    if (length(missing_files) > 0) {
      stop(paste("The following files were not found:\n",
                 paste("-", missing_files, collapse = "\n")), call. = FALSE)
    }
  } else {
    # if data is in memory, perform initial checks on the first element
    if (length(data) > 0) {
      first_dataset <- if (is.list(data)) data[[1]] else data
      if(any(!variables_to_check %in% colnames(first_dataset))) stop("Some specified variables are not present in the data.", call. = FALSE)
      if (!datetime.col %in% colnames(first_dataset)) stop("The specified datetime column does not exist in the data.", call. = FALSE)
      if (!inherits(first_dataset[[datetime.col]], "POSIXct")) stop(paste("The specified datetime column", datetime.col, "must be of class POSIXct."), call. = FALSE)
      if (!is.null(response.col)) {
        if (!response.col %in% colnames(first_dataset)) stop("The specified response column is not present in the data.", call. = FALSE)
        if (!all(first_dataset[[response.col]] %in% c(0, 1))) stop("The response column must be binary (0 or 1).", call. = FALSE)
      }
    }
  }

  # validate response.aggregation
  if (!is.null(response.col)) {
    if (!response.aggregation %in% c("majority", "any")) {
      stop('response.aggregation must be either "majority" or "any"', call. = FALSE)
    }
  }

  # validate parallel computing packages
  if (n.cores>1){
    if(!requireNamespace("foreach", quietly=TRUE)) stop("The 'foreach' package is required for parallel computing but is not installed. Please install 'foreach' using install.packages('foreach') and try again.", call. = FALSE)
    if(!requireNamespace("doSNOW", quietly=TRUE)) stop("The 'doSNOW' package is required for parallel computing but is not installed. Please install 'doSNOW' using install.packages('doSNOW') and try again.", call. = FALSE)
    if(!requireNamespace("parallel", quietly=TRUE)){
      stop("The 'parallel' package is required for parallel computing but is not installed. Please install 'parallel' using install.packages('parallel') and try again.", call. = FALSE)
    }else if(parallel::detectCores()<n.cores){
      stop(paste("Please choose a different number of cores for parallel computing (only", parallel::detectCores(), "available)."), call. = FALSE)
    }
  }

  # set export packages (required for parallel computing)
  export_packages <- "data.table"
  # check if 'skewness' or 'kurtosis' are in metrics and ensure 'moments' package is installed
  if(any(parameter_grid$metric %in% c("skewness", "kurtosis"))){
    if(!requireNamespace("moments", quietly=TRUE)) stop("The 'moments' package is required for skewness and/or kurtosis calculations but is not installed. Please install 'moments' using install.packages('moments') and try again.", call. = FALSE)
    export_packages <- c(export_packages, "moments")
  }

  # check if 'entropy' is in metrics and ensure 'entropy' package is installed
  if(any(parameter_grid$metric == "entropy")){
    if(!requireNamespace("entropy", quietly=TRUE)) stop("The 'entropy' package is required for entropy calculations but is not installed. Please install 'entropy' using install.packages('entropy') and try again.", call. = FALSE)
    export_packages <- c(export_packages, "entropy")
  }



  ##############################################################################
  # Process data ###############################################################
  ##############################################################################

  # calculate number of unique datasets
  n_animals <- length(data)

  # feedback messages for the user
  cat(paste0(
    crayon::bold("\n============= Extracting Data Features =============\n"),
    "Processing ", nrow(parameter_grid), " features across ", n_animals, ifelse(n_animals == 1, " dataset", " datasets"),
    " - buckle up!\n",
    crayon::bold("====================================================\n\n")))


  # initialize parallel backend
  if(n.cores > 1){
    # print message
    cat(paste0("Starting parallel computation: ", n.cores, " cores\n\n"))
    # register parallel backend with the specified number of cores
    cl <- parallel::makeCluster(n.cores)
    doSNOW::registerDoSNOW(cl)
    # ensure the cluster is properly stopped when the function exits
    on.exit(parallel::stopCluster(cl))
    # define the `%dopar%` operator locally for parallel execution
    `%dopar%` <- foreach::`%dopar%`
  }

  # initialize results list if returning data
  if (return.data) data_processed <- vector("list", length = n_animals)


  # iterate over each element in 'data'
  for (i in 1:length(data)) {

    ##################################################################
    # load data for the current individual if using file paths #######
    if (is_filepaths) {
      file_path <- data[i]
      individual_data <- readRDS(file_path)

      # extract ID from attributes or filename
      id <- unique(individual_data[[id.col]])[1]
      if (is.null(id)) id <- tools::file_path_sans_ext(basename(file_path))

      # perform checks for RDS files
      if(any(!variables_to_check %in% colnames(individual_data))) stop(paste0("Some specified variables are not present in the data for file: ", basename(file_path)), call. = FALSE)
      if (!datetime.col %in% colnames(individual_data)) stop(paste0("The specified datetime column does not exist in the data for file: ", basename(file_path)), call. = FALSE)
      if (!inherits(individual_data[[datetime.col]], "POSIXct")) stop(paste0("The specified datetime column ", datetime.col, " in file ", basename(file_path), " must be of class POSIXct."), call. = FALSE)
      if (!is.null(response.col)) {
        if (!response.col %in% colnames(individual_data)) stop(paste0("The specified response column is not present in the data for file: ", basename(file_path)), call. = FALSE)
        if (!all(individual_data[[response.col]] %in% c(0, 1))) stop(paste0("The response column in file ", basename(file_path), " must be binary (0 or 1)."), call. = FALSE)
      }

    ##################################################################
    # else, data is already in memory (list of data frames/tables) ###

    } else {
      individual_data <- data[[i]]
      id <- unique(data[[id.col]])[[i]]
      # fallback for id
      if (is.null(id) || id == "") id <- names(data)[i]
    }

    ##################################################################
    ##################################################################

    # name the list element for easy access later
    if (return.data) names(data_processed)[i] <- id

    # print animal ID to the console
    cat(crayon::blue$bold(id), "\n")

    # convert to data.table for faster processing
    if (!data.table::is.data.table(individual_data)) {
      individual_data <- data.table::setDT(individual_data)
    }

    # store original attributes before processing,  excluding internal ones
    discard_attrs <- c("row.names", "class", ".internal.selfref", "names")
    original_attributes <- attributes(individual_data)
    original_attributes <- original_attributes[!names(original_attributes) %in% discard_attrs]

    # ensure variables are numeric
    for (var in variables_to_check) {
      if (!is.numeric(individual_data[[var]])) {
        warning(paste("Variable", var, "is not numeric. Converting to numeric."), call. = FALSE)
        individual_data[[var]] <- as.numeric(as.character(individual_data[[var]]))
      }
    }

    # check if a response column is specified
    if (!is.null(response.col)) {
      # if the response column does not exist in the data frame, create it
      if (!(response.col %in% colnames(individual_data))) {
        individual_data[[response.col]] <- 0L
      }
      # if the response column is a factor, convert it to numeric
      if (is.factor(individual_data[[response.col]])) {
        individual_data[[response.col]] <- as.numeric(as.factor(individual_data[[response.col]])) - 1
      }
    }

    # calculate sampling frequency
    sampling_freq <- original_attributes$processed.sampling.frequency
    if (is.null(sampling_freq) || !is.numeric(sampling_freq) || sampling_freq <= 0) {
      # attempt to estimate sampling frequency if not found in attributes
      if (nrow(individual_data) > 1) {
        time_diffs <- diff(as.numeric(individual_data[[datetime.col]]))
        sampling_freq <- 1 / median(time_diffs)
        warning(paste0("Sampling frequency not found in attributes for ID '", id, "'. Estimated as ", round(sampling_freq, 2), " Hz."), call. = FALSE)
      } else {
        stop(paste0("Cannot determine sampling frequency for ID '", id, "'. Please ensure 'processed.sampling.frequency' is an attribute of your data or provide data with more than one row."), call. = FALSE)
      }
    }

    # convert the window size from seconds to steps
    window_steps <- round(window.size * sampling_freq)
    if (window_steps < 1) stop(paste0("Calculated window interval for ID '", id, "' is less than 1 step. Check your window.size and sampling frequency. Window size (seconds): ", window.size, ", Sampling Frequency (Hz): ", sampling_freq), call. = FALSE)

    # initialize an empty list to store features
    feature_list <- list()


    ###############################################################
    # sequential processing #######################################
    if(n.cores == 1){

      # initialize progress bar
      pb <- txtProgressBar(min=0, max=nrow(parameter_grid), initial=0, style=3)

      # loop through each
      for(p in 1:nrow(parameter_grid)){
        var <- parameter_grid$variable[p]
        metric <- parameter_grid$metric[p]
       feature_list[[p]] <- .calculateMetric(data = individual_data,
                                             var = var,
                                             metric = metric,
                                             sampling_freq = sampling_freq,
                                             window_steps = window_steps,
                                             aggregate = aggregate,
                                             circular_variables = circular.variables)

        # update progress bar
        setTxtProgressBar(pb, p)
      }
    }

    ###############################################################
    # parallel processing   #######################################
    if(n.cores > 1){

      # initialize progress bar
      pb <- txtProgressBar(min=0, max=nrow(parameter_grid), initial=0, style=3)

      # set progress bar options
      opts <- list(progress = function(n) setTxtProgressBar(pb, n))

      # perform parallel computation
      feature_list <- foreach::foreach(
        p = 1:nrow(parameter_grid),
        .options.snow = opts,
        .packages = export_packages,
        .export = c(".calculateMetric")
      ) %dopar% {
        var <- parameter_grid$variable[p]
        metric <- parameter_grid$metric[p]
        .calculateMetric(data = individual_data,
                         var = var,
                         metric = metric,
                         sampling_freq = sampling_freq,
                         window_steps = window_steps,
                         aggregate = aggregate,
                         circular_variables = circular.variables)
      }
    }

    ###############################################################
    ###############################################################

    # close progress bar
    close(pb)

    # assign names to the feature list
    names(feature_list) <- paste0(parameter_grid$variable, "_", parameter_grid$metric)


    ############################################################################
    # Aggregate response column (if specified) #################################
    ############################################################################

    # if a response column was provided, process it according to the specified aggregation method
    if (!is.null(response.col)) {

      # case 1: aggregate response values within each window (non-overlapping)
      if (aggregate) {

        # calculate window indices for aggregation
        idx <- seq(1, nrow(individual_data), by = window_steps)

        # assign 1 if the majority of values in the window are 1 (i.e., mean > 0.5), otherwise 0
        if (response.aggregation == "majority") {
          feature_list[[response.col]] <- sapply(idx, function(start_idx) {
            end_idx <- min(start_idx + window_steps - 1, nrow(individual_data))
            as.integer(mean(individual_data[[response.col]][start_idx:end_idx], na.rm = TRUE) > 0.5)
          })
        # assign 1 if any value in the window is 1, otherwise 0
        } else if (response.aggregation == "any") {
          feature_list[[response.col]] <- sapply(idx, function(start_idx) {
            end_idx <- min(start_idx + window_steps - 1, nrow(individual_data))
            as.integer(any(individual_data[[response.col]][start_idx:end_idx] == 1, na.rm = TRUE))
          })
        }
      # case 2: use a sliding window across the entire sequence (overlapping)
      } else {
        # apply majority rule in a sliding window: assign 1 if >50% of values are 1
        if (response.aggregation == "majority") {
          feature_list[[response.col]] <- zoo::rollapply(
            individual_data[[response.col]],
            width = window_steps,
            FUN = function(x) as.integer(mean(x, na.rm = TRUE) > 0.5),
            align = "center", fill = NA
          )
        # assign 1 if any value in the window is 1
        } else if (response.aggregation == "any") {
          feature_list[[response.col]] <- zoo::rollapply(
            individual_data[[response.col]],
            width = window_steps,
            FUN = function(x) as.integer(any(x == 1, na.rm = TRUE)),
            align = "center", fill = NA
          )
        }
      }
    }


    ############################################################################
    # Combine features #########################################################
    ############################################################################

    # combine features into a single data.table
    feature_data <- data.table::setDT(feature_list)

    # add the timestamp column
    if (!aggregate) {
      feature_data[, (datetime.col) := individual_data[[datetime.col]]]
    } else {
      # ensure we don't exceed data bounds
      idx <- seq(1, nrow(individual_data), by = window_steps)
      feature_data[, (datetime.col) := individual_data[[datetime.col]][idx]]
    }

    # add the 'ID' column to the processed data
    feature_data[, ID := id]

    # move ID and datetime.col to the first columns
    data.table::setcolorder(feature_data, c("ID", datetime.col))

    # convert response col back to factor
    if (!is.null(response.col)) {
      feature_data[, (response.col) := as.factor(get(response.col))]
    }


    # replace problematic metrics with defaults where appropriate
    # skewness to 0 (symmetric distribution), kurtosis to 3 (normal distribution baseline)
    # and entropy to 0 (no uncertainty, all values same)

    # this avoids removing biologically meaningful surface periods where depth is constant
    # (e.g., depth = 0), which would otherwise result in NA or NaN for some metrics
    # and be dropped if using na.omit() downstream.


    # define default replacement values for problematic metrics
    replacement_values <- list(skewness = 0, kurtosis = 3, entropy = 0)
    # define which variables (prefixes) you want to target for replacement
    target_prefixes <- c("depth", "vertical_speed")

    # create a logical vector indicating rows where depth is exactly zero (surface),
    # excluding missing depth values (NA), since we only want to replace NAs in metrics
    # caused by constant zero depth, not missing sensor data.
    depth_zero <- !is.na(individual_data$depth) & individual_data$depth == 0
    # create a temporary data.table linking datetime to depth_zero flag
    dt_flags <- data.table(datetime = individual_data[[datetime.col]], depth_zero = depth_zero)

    # merge the flag data into the features data by datetime (left join)
    feature_data <- merge(feature_data, dt_flags, by = datetime.col, all.x = TRUE)

    # loop over target variable prefixes (e.g., "depth", "vertical_speed") and metrics
    for (prefix in target_prefixes) {
      for (suffix in names(replacement_values)) {
        # find columns matching metric names, e.g. "depth_skewness"
        pattern <- paste0("^", prefix, "_", suffix, "$")
        matching_cols <- grep(pattern, names(feature_data), value = TRUE)
        for (col in matching_cols) {
          # find indices where metric is NA or NaN
          idx_replace <- which(is.na(feature_data[[col]]) | is.nan(feature_data[[col]]))
          # filter indices to only those where depth_zero flag is TRUE
          idx_replace <- idx_replace[which(feature_data$depth_zero[idx_replace] == TRUE)]
          # replace NA/NaN with the default replacement value for this metric
          feature_data[[col]][idx_replace] <- replacement_values[[suffix]]
        }
      }
    }

    # remove the temporary depth_zero flag column after replacements
    feature_data[, depth_zero := NULL]

    # remove rows with any (remaining) missing values (NA) in any column
    feature_data <- na.omit(feature_data)


    ############################################################################
    # Downsample data ##########################################################
    ############################################################################

    # if a downsampling rate is specified, aggregate the data to the defined frequency (in Hz)
    if(!is.null(downsample.to)){

      # check if the specified downsampling frequency matches the dataset's sampling frequency
      if (downsample.to == sampling_freq) {
        warning(paste(id, " - dataset sampling already", downsample.to, "Hz, downsampling skipped"), call. = FALSE)
        final_data <- feature_data

        # check if the specified downsampling frequency exceeds the dataset's sampling frequency
      } else if(downsample.to > sampling_freq) {
        warning(paste(id, " - dataset sampling (", downsample.to, "Hz) lower than the specified downsampling rate, downsampling skipped"), call. = FALSE)
        final_data <- feature_data

        # start downsampling
      } else {

        # select columns to keep
        feature_cols <- setdiff(colnames(feature_data), c(id.col, datetime.col))
        if (!is.null(response.col)) feature_cols <- setdiff(feature_cols, response.col)

        # convert the desired downsample rate to time interval in seconds
        downsample_interval <- 1 / downsample.to

        # round datetime to the nearest downsample interval
        first_time <- feature_data[[datetime.col]][1]
        feature_data[, (datetime.col) := first_time + floor(as.numeric(get(datetime.col) - first_time) / downsample_interval) * downsample_interval]

        # temporarily suppress console output (redirect to a temporary file)
        sink(tempfile())

        # aggregate metrics using arithmetic mean
        final_data <- feature_data[, lapply(.SD, mean, na.rm=TRUE), by = datetime.col, .SDcols = feature_cols]

        # handle response column aggregation if present
        if (!is.null(response.col)) {
          if (response.aggregation == "majority") {
            processed_response <- feature_data[, .(response = as.integer(mean(as.numeric(as.character(get(response.col))), na.rm = TRUE) > 0.5)),  by = c(datetime.col)]
          } else if (response.aggregation == "any") {
            processed_response <- feature_data[, .(response = as.integer(any(as.numeric(as.character(get(response.col))) == 1, na.rm = TRUE))),  by = c(datetime.col)]
          }
          # rename before merging
          data.table::setnames(processed_response, "response", response.col)
          # merge with downsampled features
          final_data <- merge(final_data, processed_response, by = datetime.col, sort = FALSE)
        }

        # re-add ID column
        final_data[, (id.col) := id]

        # clean up
        gc()

        # restore normal output
        sink()
      }

    } else{
      # if no downsampling rate is defined, return the original sensor data
      final_data <- feature_data
    }

    # reorder columns
    data.table::setcolorder(final_data, c(id.col, datetime.col, if(!is.null(response.col)) response.col, feature_cols))


    ############################################################################
    # Add additional attributes ################################################
    ############################################################################

    # reapply the original attributes to the processed data
    for (attr_name in names(original_attributes)) {
      attr(final_data, attr_name) <- original_attributes[[attr_name]]
    }

    # create new attributes to save relevant variables
    attr(final_data, "features.window.size") <- window.size
    attr(final_data, "features.aggregate") <- aggregate
    attr(final_data, "features.response_col") <- response.col
    attr(final_data, "features.response.aggregation") <- response.aggregation
    attr(final_data, 'processing.date') <- Sys.time()


    ############################################################################
    # Save processed data ######################################################
    ############################################################################

    # save the processed data as an RDS file
    if (save.files) {
      if (is_filepaths) {
        output_dir <- if (!is.null(output.folder)) output.folder else dirname(file_path)
      } else {
        output_dir <- if (!is.null(output.folder)) output.folder else "./"
      }
      if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

      suffix <- ifelse(!is.null(output.suffix), output.suffix, "")
      output_file <- file.path(output_dir, paste0(id, suffix, ".rds"))

      cat("Saving file... ")
      flush.console()
      saveRDS(final_data, output_file)
      cat("\r")
      cat(rep(" ", getOption("width")-1))
      cat("\r")
      cat(sprintf("\u2713 Saved: %s\n", basename(output_file)))
    }

    # store data to list if return.data is TRUE
    if (return.data) {
      data_processed[[i]] <- final_data
    }

    # clear individual data from memory
    rm(individual_data, feature_data, feature_list, final_data)
    # run garbage collection
    gc(verbose = FALSE)

    # newline after each individual's processing
    cat("\n")

  }

  ##############################################################################
  # Finalization ###############################################################
  ##############################################################################

  # print time taken
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  cat(sprintf("\nTotal execution time: %.02f %s\n\n", as.numeric(time.taken), base::units(time.taken)))

  # return results
  if (return.data) {
    return(data_processed)
  } else {
    return(invisible(NULL)) # Return nothing if not returning data
  }
}



################################################################################
# Define helper function to to calculate metrics ###############################
################################################################################

#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.calculateMetric <- function(data, var, metric, sampling_freq, window_steps, aggregate, circular_variables) {

  # access data
  x <- data[[var]]

  ##############################################################################
  # circular variables #########################################################
  if (var %in% circular_variables) {

    # sliding window for circular
    if (!aggregate) {
      return(zoo::rollapply(
        x,
        width = window_steps,
        FUN = function(w) .circularMetric(w, metric = metric, sampling_freq = sampling_freq),
        align = "center",
        fill = NA
      ))
    # aggregate for circular
    } else {
      n <- length(x)
      starts <- seq(1, n, by = window_steps)
      ends <- pmin(starts + window_steps - 1, n)
      result <- sapply(seq_along(starts), function(i) {
        window_data <- x[starts[i]:ends[i]]
        .circularMetric(window_data, metric = metric, sampling_freq = sampling_freq)
      })
      return(result)
    }


  ##############################################################################
  # linear variable processing #########################################################

  } else {

    if (!aggregate) {
      # use data.table's frollapply for efficient sliding window calculations
      switch(metric,
             mean = data.table::frollmean(x, window_steps, na.rm = TRUE, align = "center", fill = NA),
             median = data.table::frollapply(x, window_steps, median, na.rm = TRUE, align = "center", fill = NA),
             sd = data.table::frollapply(x, window_steps, sd, na.rm = TRUE, align = "center", fill = NA),
             min = data.table::frollapply(x, window_steps, min, na.rm = TRUE, align = "center", fill = NA),
             max = data.table::frollapply(x, window_steps, max, na.rm = TRUE, align = "center", fill = NA),
             sum = data.table::frollsum(x, window_steps, na.rm = TRUE, align = "center", fill = NA),
             # for more complex metrics, still use the original approach but with faster functions
             range = zoo::rollapply(x, window_steps, function(x) diff(range(x, na.rm = TRUE)), fill = NA, align = "center", partial = FALSE),
             iqr = zoo::rollapply(x, window_steps, IQR, na.rm = TRUE, fill = NA, align = "center", partial = FALSE),
             rate = zoo::rollapply(x, width = window_steps, function(x) {
               if (length(x) < 2 || all(is.na(x))) return(NA)
               mean(abs(diff(x)), na.rm = TRUE) * sampling_freq
             }, fill = NA, align = "center", partial = FALSE),
             energy = data.table::frollapply(x^2, window_steps, sum, na.rm = TRUE, align = "center", fill = NA),
             skewness = zoo::rollapply(x, window_steps, moments::skewness, na.rm = TRUE, fill = NA, align = "center", partial = FALSE),
             kurtosis = zoo::rollapply(x, window_steps, moments::kurtosis, na.rm = TRUE, fill = NA, align = "center", partial = FALSE),
             entropy = zoo::rollapply(x, window_steps, function(x) {
               x <- x[!is.na(x)]
               if(length(unique(x)) <= 1) return(NA)
               tryCatch({
                 hist_data <- hist(x, breaks = "Sturges", plot = FALSE)
                 p <- hist_data$density / sum(hist_data$density)
                 entropy::entropy(p)
               }, error = function(e) NA)
             }, fill = NA, align = "center", partial = FALSE)
      )

    # aggregate data into distinct windows - use vectorized operations where possible
    } else {
      n <- length(x)
      starts <- seq(1, n, by = window_steps)
      ends <- pmin(starts + window_steps - 1, n)

      # use vectorized operations for simple metrics
      switch(metric,
             mean = vapply(seq_along(starts), function(i) mean(x[starts[i]:ends[i]], na.rm = TRUE), numeric(1)),
             median = vapply(seq_along(starts), function(i) median(x[starts[i]:ends[i]], na.rm = TRUE), numeric(1)),
             sd = vapply(seq_along(starts), function(i) sd(x[starts[i]:ends[i]], na.rm = TRUE), numeric(1)),
             min = vapply(seq_along(starts), function(i) min(x[starts[i]:ends[i]], na.rm = TRUE), numeric(1)),
             max = vapply(seq_along(starts), function(i) max(x[starts[i]:ends[i]], na.rm = TRUE), numeric(1)),
             sum = vapply(seq_along(starts), function(i) sum(x[starts[i]:ends[i]], na.rm = TRUE), numeric(1)),
             # For complex metrics, keep original approach
             range = vapply(seq_along(starts), function(i) diff(range(x[starts[i]:ends[i]], na.rm = TRUE)), numeric(1)),
             iqr = vapply(seq_along(starts), function(i) IQR(x[starts[i]:ends[i]], na.rm = TRUE), numeric(1)),
             rate = vapply(seq_along(starts), function(i) {
               segment <- x[starts[i]:ends[i]]
               if (length(segment) < 2 || all(is.na(segment))) return(NA_real_)
               mean(abs(diff(segment)), na.rm = TRUE) * sampling_freq
             }, numeric(1)),
             energy = vapply(seq_along(starts), function(i) sum(x[starts[i]:ends[i]]^2, na.rm = TRUE), numeric(1)),
             skewness = vapply(seq_along(starts), function(i) moments::skewness(x[starts[i]:ends[i]], na.rm = TRUE), numeric(1)),
             kurtosis = vapply(seq_along(starts), function(i) moments::kurtosis(x[starts[i]:ends[i]], na.rm = TRUE), numeric(1)),
             entropy = vapply(seq_along(starts), function(i) {
               segment <- x[starts[i]:ends[i]]
               segment <- segment[!is.na(segment)]
               if(length(unique(segment)) <= 1) return(NA)
               tryCatch({
                 hist_data <- hist(segment, breaks = "Sturges", plot = FALSE)
                 p <- hist_data$density / sum(hist_data$density)
                 entropy::entropy(p)
               }, error = function(e) NA)
             }, numeric(1))
      )
    }

  }
}


################################################################################
# Define helper function for circular metrics ##################################
################################################################################

.circularMetric <- function(x_window, metric, sampling_freq) {

  # remove NAs and ensure sufficient data
  x_window <- x_window[!is.na(x_window)]
  if (length(x_window) == 0) return(NA)
  if (metric %in% c("sd", "range", "iqr") && length(x_window) < 2) return(NA)

  # convert to radians
  radians <- x_window * pi / 180

  # small helper function for angular difference
  .angular_diff <- function(a, b) {
    diff <- abs(a - b) %% 360
    pmin(diff, 360 - diff)
  }

  switch(metric,
         mean = {
           C <- mean(cos(radians))
           S <- mean(sin(radians))
           atan2(S, C) * 180 / pi
         },
         median = {
           sorted <- sort(x_window)
           diffs <- sapply(sorted, function(y) sum(abs(pi - abs(radians - y*pi/180))))
           sorted[which.min(diffs)]
         },
         sd = {
           C <- mean(cos(radians))
           S <- mean(sin(radians))
           R <- sqrt(C^2 + S^2)
           sqrt(-2 * log(R)) * 180 / pi
         },
         range = {
           if (length(x_window) < 2) return(NA)
           sorted <- sort(x_window)
           gaps <- c(diff(sorted), 360 - (sorted[length(sorted)] - sorted[1]))
           360 - max(gaps)
         },
         iqr = {
           if (length(x_window) < 2) return(NA)
           q <- quantile(x_window, probs = c(0.25, 0.75))
           diff <- (q[2] - q[1]) %% 360
           min(diff, 360 - diff)
         },
         mrl = {
           C <- mean(cos(radians))
           S <- mean(sin(radians))
           sqrt(C^2 + S^2)
         },
         rate = {
           diffs <- .angular_diff(x_window[-1], x_window[-length(x_window)])
           mean(diffs, na.rm = TRUE) * sampling_freq
         }
  )
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
