#######################################################################################################
# Function to extract features from a sliding or aggregated window ####################################
#######################################################################################################

#' Extract Features from Sensor Data Using Sliding or Aggregated Windows (Enhanced)
#'
#' This function calculates specified metrics (e.g., mean, standard deviation, circular statistics,
#' and complex ecological features) for selected variables over a sliding window.
#' It can either retain the temporal resolution of the dataset or aggregate data into distinct,
#' non-overlapping windows. Enhanced version includes complex movement and behavioral features.
#'
#' @param data A list of tables/data frames (one per individual), a single aggregated data table/data frame,
#' or a character vector of file paths to RDS files containing sensor data.
#' @param variables (Optional) A character vector of column names from `data` for which to calculate metrics.
#' If `parameter.grid` is supplied, this argument is ignored.
#' @param metrics (Optional) A character vector specifying the metrics to calculate.
#' @param parameter.grid (Optional) A data frame with columns: `variable`, `metric`, and optionally `window_seconds`.
#' If `window_seconds` is not provided, uses the global `window.size` parameter.
#' @param enhanced.features Logical. If TRUE, includes complex ecological features. Default FALSE.
#' @param id.col A character string specifying the column containing individual IDs.
#' @param datetime.col A character string specifying the column in `data` containing datetime information.
#' @param window.size An integer specifying the default size of the sliding or aggregation window, in seconds.
#' @param aggregate Logical. If TRUE, uses non-overlapping windows for initial feature extraction.
#' @param downsample.to NULL or numeric (seconds). If specified, aggregates features into non-overlapping windows.
#' @param response.col (Optional) A character string specifying the column containing response labels.
#' @param circular.variables Character vector specifying variables that should be treated as circular.
#' @param response.aggregation Method to aggregate `response.col`: "majority" or "any".
#' @param return.data Logical. If `TRUE`, returns processed data as a list.
#' @param save.files Logical. If `TRUE`, saves processed data as separate RDS files.
#' @param output.folder Directory where processed RDS files should be saved.
#' @param output.suffix Character string to append to output filenames.
#' @param n.cores Number of processor cores for parallel computation.
#'
#' @details
#' Enhanced version supports all original metrics plus complex ecological features:
#'
#' Complex Features (when enhanced.features = TRUE):
#' - "net_heading_change": Net change in heading over centered window
#' - "cumulative_heading_change": Cumulative sum of absolute heading changes
#' - "circular_variance_heading": Circular variance of heading values
#' - "oscillation_regularity": Regularity of oscillatory patterns
#' - "movement_predictability": Predictability using ratio of SD to mean
#' - "movement_consistency": Consistency using SD metrics
#' - "movement_smoothness": Smoothness using rate of change
#' - "movement_jerk": Enhanced jerk calculation
#' - "posture_stability": Stability from pitch/roll SD metrics
#' - "turning_rate_variability": Variability in turning rates
#' - "activity_index": Combined activity from multiple rates
#' - "rolling_autocorrelation": Rolling autocorrelation
#' - "zero_crossing_rate": Rate of zero crossings
#' - "circling_behavior": Detection of circling patterns
#' - "feeding_posture_index": Index for feeding posture detection
#' - "depth_change_rate": Rate of depth changes
#' - "depth_change_consistency": Consistency of depth changes
#'
#' @return A list of data frames with calculated features.
#' @export

extractFeatures <- function(data,
                            variables = NULL,
                            metrics = NULL,
                            parameter.grid = NULL,
                            enhanced.features = FALSE,
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
                            n.cores = 1) {

  ##############################################################################
  # Initial checks and setup ###################################################
  ##############################################################################

  start.time <- Sys.time()
  is_filepaths <- is.character(data)

  # Check optional packages for specific metrics
  optional_packages <- list(
    moments = c("skewness", "kurtosis"),
    entropy = "entropy",
    crayon = NULL  # For colored output
  )

  for (pkg in names(optional_packages)) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      if (pkg == "crayon") {
        # Crayon is optional for colored output
        warning(paste("Package", pkg, "not available. Output will not be colored."), call. = FALSE)
      } else {
        # Check if any requested metrics need this package
        needed_metrics <- optional_packages[[pkg]]
        stop(paste("Package", pkg, "is required for metrics:",
                   paste(needed_metrics, collapse = ", "), "but not installed."), call. = FALSE)
      }
    }
  }

  # Split data by id.col if not already a list or file paths
  if (!is_filepaths && !is.list(data)) {
    if (!id.col %in% names(data)) {
      stop("Input data must contain a valid 'id.col' when not provided as a list or file paths.", call. = FALSE)
    }
    data <- split(data, f = data[[id.col]])
  }

  # Validate output parameters
  if (!is.logical(save.files)) stop("`save.files` must be logical.", call. = FALSE)
  if (!is.logical(return.data)) stop("`return.data` must be logical.", call. = FALSE)
  if (!save.files && !return.data) {
    stop("Both 'save.files' and 'return.data' cannot be FALSE.", call. = FALSE)
  }

  # Validate enhanced.features parameter
  if (!is.logical(enhanced.features)) {
    stop("`enhanced.features` must be logical.", call. = FALSE)
  }

  # Define valid metrics
  valid_linear_metrics <- c("mean", "median", "sd", "range", "min", "max", "iqr",
                            "mad", "sum", "rate", "energy", "skewness", "kurtosis", "entropy")
  valid_circular_metrics <- c("mean", "median", "sd", "range", "iqr", "mrl", "rate")

  # Enhanced features list
  valid_enhanced_metrics <- c("net_heading_change", "cumulative_heading_change",
                              "circular_variance_heading", "oscillation_regularity",
                              "movement_predictability", "movement_consistency",
                              "movement_smoothness", "movement_jerk", "posture_stability",
                              "turning_rate_variability", "activity_index",
                              "rolling_autocorrelation", "zero_crossing_rate",
                              "circling_behavior", "feeding_posture_index",
                              "depth_change_rate", "depth_change_consistency",
                              "uturn_flag", "heading_autocorr_avg")

  # Determine parameter grid
  if (is.null(parameter.grid)) {
    if (is.null(variables) || length(variables) == 0) {
      stop("If `parameter.grid` is not supplied, `variables` must be provided.", call. = FALSE)
    }
    if (is.null(metrics) || length(metrics) == 0) {
      stop("If `parameter.grid` is not supplied, `metrics` must be provided.", call. = FALSE)
    }
    parameter_grid <- expand.grid(variable = variables, metric = metrics, stringsAsFactors = FALSE)
    parameter_grid$window_seconds <- window.size  # Add default window size
  } else {
    if (!is.data.frame(parameter.grid)) stop("`parameter.grid` must be a data frame.", call. = FALSE)
    if (!all(c("variable", "metric") %in% names(parameter.grid))) {
      stop("`parameter.grid` must contain 'variable' and 'metric' columns.", call. = FALSE)
    }
    parameter.grid$metric <- tolower(parameter.grid$metric)

    # Add window_seconds column if not present
    if (!"window_seconds" %in% names(parameter.grid)) {
      parameter.grid$window_seconds <- window.size
    }
    parameter_grid <- parameter.grid
  }

  # Validate metrics
  all_valid_metrics <- c(valid_linear_metrics, valid_circular_metrics)
  if (enhanced.features) {
    all_valid_metrics <- c(all_valid_metrics, valid_enhanced_metrics)
  }

  invalid_metrics <- parameter_grid$metric[!parameter_grid$metric %in% all_valid_metrics]
  if (length(invalid_metrics) > 0) {
    stop(paste("Invalid metrics found:", paste(unique(invalid_metrics), collapse = ", ")), call. = FALSE)
  }

  # Check for enhanced features when enhanced.features = FALSE
  if (!enhanced.features && any(parameter_grid$metric %in% valid_enhanced_metrics)) {
    stop("Enhanced metrics found in parameter.grid but enhanced.features = FALSE. Set enhanced.features = TRUE.", call. = FALSE)
  }

  # Load required packages for enhanced features
  if (enhanced.features) {
    required_packages <- c("circular", "zoo")
    for (pkg in required_packages) {
      if (!requireNamespace(pkg, quietly = TRUE)) {
        stop(paste("Package", pkg, "is required for enhanced features but not installed."), call. = FALSE)
      }
    }
  }

  # Validate parallel computing requirements
  if (n.cores > 1) {
    required_parallel_packages <- c("foreach", "doSNOW", "parallel")
    for (pkg in required_parallel_packages) {
      if (!requireNamespace(pkg, quietly = TRUE)) {
        stop(paste("Package", pkg, "is required for parallel computing but not installed."), call. = FALSE)
      }
    }
    if (parallel::detectCores() < n.cores) {
      stop(paste("Only", parallel::detectCores(), "cores available, but", n.cores, "requested."), call. = FALSE)
    }
  }

  ##############################################################################
  # Process data ###############################################################
  ##############################################################################

  # Calculate nº individuals
  n_animals <- length(data)

  # Print verbose message
  cat(paste0(
    crayon::bold("\n============= Extracting Data Features =============\n"),
    "Processing ", nrow(parameter_grid), " features across ", n_animals,
    ifelse(n_animals == 1, " dataset", " datasets"),
    ifelse(enhanced.features, " (Enhanced Mode)", ""), "\n",
    crayon::bold("====================================================\n\n")))

  # Initialize parallel backend if needed
  if (n.cores > 1) {
    cat(paste0("Starting parallel computation: ", n.cores, " cores\n\n"))
    cl <- parallel::makeCluster(n.cores)
    doSNOW::registerDoSNOW(cl)
    on.exit(parallel::stopCluster(cl))
    `%dopar%` <- foreach::`%dopar%`
  }

  # Initialize results
  if (return.data) data_processed <- vector("list", length = n_animals)

  # Process each dataset
  for (i in 1:length(data)) {

    # Load data
    if (is_filepaths) {
      file_path <- data[i]
      individual_data <- readRDS(file_path)
      id <- unique(individual_data[[id.col]])[1]
      if (is.null(id)) id <- tools::file_path_sans_ext(basename(file_path))
    } else {
      individual_data <- data[[i]]
      id <- unique(individual_data[[id.col]])[1]
      if (is.null(id) || id == "") id <- names(data)[i]
    }

    if (return.data) names(data_processed)[i] <- id

    # print current ID
    cat(crayon::blue$bold(sprintf("[%d/%d] %s\n", i, n_animals, id)))

    # Convert to data.table
    if (!data.table::is.data.table(individual_data)) {
      individual_data <- data.table::setDT(individual_data)
    }

    # Store original attributes
    discard_attrs <- c("row.names", "class", ".internal.selfref", "names")
    original_attributes <- attributes(individual_data)
    original_attributes <- original_attributes[!names(original_attributes) %in% discard_attrs]

    # Calculate sampling frequency for this individual
    sampling_freq <- original_attributes$processed.sampling.frequency
    if (is.null(sampling_freq) || !is.numeric(sampling_freq) || sampling_freq <= 0) {
      if (nrow(individual_data) > 1) {
        time_diffs <- diff(as.numeric(individual_data[[datetime.col]]))
        sampling_freq <- 1 / median(time_diffs)
        warning(paste0("Sampling frequency estimated as ", round(sampling_freq, 2), " Hz for ID '", id, "'."), call. = FALSE)
      } else {
        stop(paste0("Cannot determine sampling frequency for ID '", id, "'."), call. = FALSE)
      }
    }

    # Calculate window steps (CRITICAL FIX)
    window_steps <- round(window.size * sampling_freq)

    # Initialize feature list
    feature_list <- list()

    # Process features (sequential or parallel)
    if (n.cores == 1) {
      pb <- txtProgressBar(min = 0, max = nrow(parameter_grid), initial = 0, style = 3)
      for (p in 1:nrow(parameter_grid)) {
        var <- parameter_grid$variable[p]
        metric <- parameter_grid$metric[p]
        window_sec <- parameter_grid$window_seconds[p]

        feature_list[[p]] <- .calculateMetricEnhanced(
          data = individual_data,
          var = var,
          metric = metric,
          sampling_freq = sampling_freq,
          window_seconds = window_sec,
          aggregate = aggregate,
          circular_variables = circular.variables,
          enhanced = enhanced.features
        )
        setTxtProgressBar(pb, p)
      }
    } else {
      # Parallel processing
      pb <- txtProgressBar(min = 0, max = nrow(parameter_grid), initial = 0, style = 3)
      opts <- list(progress = function(n) setTxtProgressBar(pb, n))

      # Define functions to export for enhanced features
      enhanced_functions <- if(enhanced.features) {
        c(".calculateEnhancedFeature", "net_heading_change", "cumulative_heading_change",
          "circular_variance_heading", "oscillation_regularity", "movement_predictability",
          "movement_consistency", "movement_smoothness", "movement_jerk",
          "posture_stability_from_sd", "turning_rate_variability", "activity_index",
          "rolling_autocorrelation", "zero_crossing_rate", "circling_behavior",
          "feeding_posture_index", "depth_change_metrics", "uturn_flag",
          "heading_autocorr_avg", "ensure_length", ".circularMetric")
      } else {
        c(".circularMetric")
      }

      feature_list <- foreach::foreach(
        p = 1:nrow(parameter_grid),
        .options.snow = opts,
        .packages = c("data.table", if(enhanced.features) c("circular", "zoo")),
        .export = c(".calculateMetricEnhanced", enhanced_functions)
      ) %dopar% {
        var <- parameter_grid$variable[p]
        metric <- parameter_grid$metric[p]
        window_sec <- parameter_grid$window_seconds[p]

        .calculateMetricEnhanced(
          data = individual_data,
          var = var,
          metric = metric,
          sampling_freq = sampling_freq,
          window_seconds = window_sec,
          aggregate = aggregate,
          circular_variables = circular.variables,
          enhanced = enhanced.features
        )
      }
    }

    # Close progress bar
    close(pb)

    # Name features
    feature_names <- mapply(create_feature_name,
                            parameter_grid$variable,
                            parameter_grid$metric,
                            parameter_grid$window_seconds,
                            window.size,
                            SIMPLIFY = TRUE)

    names(feature_list) <- feature_names


    ############################################################################
    # Validate feature lengths before combining ################################
    ############################################################################

    expected_length <- if (aggregate) {
      length(seq(1, nrow(individual_data), by = window_steps))
    } else {
      nrow(individual_data)
    }

    # Check and fix feature lengths
    feature_lengths <- sapply(feature_list, length)
    if (length(unique(feature_lengths)) > 1) {
      warning(paste("Features have different lengths for ID:", id,
                    "- Expected:", expected_length,
                    "- Found:", paste(unique(feature_lengths), collapse = ", ")))

      # Standardize all features to expected length
      feature_list <- lapply(feature_list, function(feat) {
        if (length(feat) != expected_length) {
          if (length(feat) > expected_length) {
            return(feat[1:expected_length])
          } else {
            result <- rep(NA, expected_length)
            result[1:length(feat)] <- feat
            return(result)
          }
        }
        return(feat)
      })
    }

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

    # Proper datetime assignment
    if (!aggregate) {
      feature_data[, (datetime.col) := individual_data[[datetime.col]]]
    } else {
      # ensure we don't exceed data bounds
      idx <- seq(1, nrow(individual_data), by = window_steps)
      # Ensure idx doesn't exceed data length
      idx <- idx[idx <= nrow(individual_data)]

      # Match the length of datetime to features
      if (length(idx) != nrow(feature_data)) {
        idx <- idx[1:nrow(feature_data)]
      }

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

    # Replace problematic metrics with defaults where appropriate
    replacement_values <- list(skewness = 0, kurtosis = 3, entropy = 0)
    target_prefixes <- c("depth", "vertical_speed")

    # Only do replacement if depth column exists
    if ("depth" %in% names(individual_data)) {
      depth_zero <- !is.na(individual_data$depth) & individual_data$depth == 0
      dt_flags <- data.table::data.table(datetime = individual_data[[datetime.col]], depth_zero = depth_zero)

      feature_data <- merge(feature_data, dt_flags, by = datetime.col, all.x = TRUE)

      for (prefix in target_prefixes) {
        for (suffix in names(replacement_values)) {
          pattern <- paste0("^", prefix, "_", suffix, "$")
          matching_cols <- grep(pattern, names(feature_data), value = TRUE)
          for (col in matching_cols) {
            idx_replace <- which(is.na(feature_data[[col]]) | is.nan(feature_data[[col]]))
            idx_replace <- idx_replace[which(feature_data$depth_zero[idx_replace] == TRUE)]
            feature_data[[col]][idx_replace] <- replacement_values[[suffix]]
          }
        }
      }

      feature_data[, depth_zero := NULL]
    }

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
        warning(paste(id, " - dataset sampling (", sampling_freq, "Hz) lower than the specified downsampling rate, downsampling skipped"), call. = FALSE)
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
    feature_cols <- setdiff(colnames(final_data), c(id.col, datetime.col, response.col))
    data.table::setcolorder(final_data, c(id.col, datetime.col, if(!is.null(response.col)) response.col, feature_cols))


    ############################################################################
    # Add additional attributes ################################################
    ############################################################################

    # reapply the original attributes to the processed data
    for (attr_name in names(original_attributes)) {
      attr(final_data, attr_name) <- original_attributes[[attr_name]]
    }

    # create new attributes to save relevant variables
    if(!is.null(parameter.grid)){
      attr(final_data, "parameter.grid") <- parameter.grid
    } else {
      attr(final_data, "features.window.size") <- window.size
    }
    attr(final_data, "features.aggregate") <- aggregate
    attr(final_data, "features.response.col") <- response.col
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
# Enhanced metric calculation function #########################################
################################################################################

.calculateMetricEnhanced <- function(data, var, metric, sampling_freq, window_seconds,
                                     aggregate, circular_variables, enhanced = FALSE) {

  window_steps <- round(window_seconds * sampling_freq)
  x <- data[[var]]

  # Enhanced features
  if (enhanced && metric %in% c("net_heading_change", "cumulative_heading_change",
                                "circular_variance_heading", "oscillation_regularity",
                                "movement_predictability", "movement_consistency",
                                "movement_smoothness", "movement_jerk", "posture_stability",
                                "turning_rate_variability", "activity_index",
                                "rolling_autocorrelation", "zero_crossing_rate",
                                "circling_behavior", "feeding_posture_index",
                                "depth_change_rate", "depth_change_consistency",
                                "uturn_flag", "heading_autocorr_avg")) {

    return(.calculateEnhancedFeature(data, var, metric, window_steps, aggregate))
  }

  ##############################################################################
  # CIRCULAR VARIABLES #########################################################
  ##############################################################################

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
    # LINEAR VARIABLES ###########################################################
    ##############################################################################

  } else {

    if (!aggregate) {
      # Use data.table's frollapply for efficient sliding window calculations
      switch(metric,
             mean = data.table::frollmean(x, window_steps, na.rm = TRUE, align = "center", fill = NA),
             median = data.table::frollapply(x, window_steps, median, na.rm = TRUE, align = "center", fill = NA),
             sd = data.table::frollapply(x, window_steps, sd, na.rm = TRUE, align = "center", fill = NA),
             min = data.table::frollapply(x, window_steps, min, na.rm = TRUE, align = "center", fill = NA),
             max = data.table::frollapply(x, window_steps, max, na.rm = TRUE, align = "center", fill = NA),
             sum = data.table::frollsum(x, window_steps, na.rm = TRUE, align = "center", fill = NA),
             # For more complex metrics, still use the original approach but with faster functions
             range = zoo::rollapply(x, window_steps, function(x) diff(range(x, na.rm = TRUE)), fill = NA, align = "center", partial = FALSE),
             iqr = zoo::rollapply(x, window_steps, IQR, na.rm = TRUE, fill = NA, align = "center", partial = FALSE),
             mad = zoo::rollapply(x, window_steps, mad, na.rm = TRUE, fill = NA, align = "center", partial = FALSE),
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
             }, fill = NA, align = "center", partial = FALSE),
             # Default case for unknown metrics
             stop(paste("Unknown metric:", metric))
      )

      # Aggregate data into distinct windows - use vectorized operations where possible
    } else {
      n <- length(x)
      starts <- seq(1, n, by = window_steps)
      ends <- pmin(starts + window_steps - 1, n)

      # Use vectorized operations for simple metrics
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
             mad = vapply(seq_along(starts), function(i) mad(x[starts[i]:ends[i]], na.rm = TRUE), numeric(1)),
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
             }, numeric(1)),
             # Default case for unknown metrics
             stop(paste("Unknown metric:", metric))
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


################################################################################
# Enhanced feature calculation function (COMPLETE VERSION) ####################
################################################################################

.calculateEnhancedFeature <- function(data, var, metric, window_steps, aggregate) {

  n_rows <- nrow(data)

  result <- switch(metric,
                   # Heading-specific features
                   "net_heading_change" = {
                     if (var != "heading") stop("net_heading_change can only be applied to heading variable")
                     net_heading_change(data$heading, window = window_steps)
                   },
                   "cumulative_heading_change" = {
                     if (var != "heading") stop("cumulative_heading_change can only be applied to heading variable")
                     cumulative_heading_change(data$heading, window = window_steps)
                   },
                   "circular_variance_heading" = {
                     if (var != "heading") stop("circular_variance_heading can only be applied to heading variable")
                     circular_variance_heading(data$heading, window = window_steps)
                   },
                   "uturn_flag" = {
                     if (var != "heading") stop("uturn_flag can only be applied to heading variable")
                     uturn_flag(data$heading, window = window_steps)
                   },
                   "heading_autocorr_avg" = {
                     if (var != "heading") stop("heading_autocorr_avg can only be applied to heading variable")
                     heading_autocorr_avg(data$heading, window = window_steps)
                   },
                   "turning_rate_variability" = {
                     if (var != "heading") stop("turning_rate_variability can only be applied to heading variable")
                     turning_rate_variability(data$heading, window = window_steps)
                   },
                   "circling_behavior" = {
                     if (var != "heading") stop("circling_behavior can only be applied to heading variable")
                     circling_behavior(data$heading, window = window_steps)
                   },

                   # Composite features requiring specific variable names
                   "posture_stability" = {
                     if (var != "posture") stop("posture_stability should use variable = 'posture'")
                     posture_stability_from_sd(data, window = window_steps)
                   },
                   "activity_index" = {
                     if (var != "activity") stop("activity_index should use variable = 'activity'")
                     activity_index(data, window = window_steps)
                   },

                   # Movement features that can apply to any variable
                   "oscillation_regularity" = {
                     oscillation_regularity(data[[var]], window = window_steps)
                   },
                   "movement_jerk" = {
                     movement_jerk(data[[var]], window = window_steps)
                   },
                   "movement_smoothness" = {
                     movement_smoothness(data[[var]], window = window_steps)
                   },
                   "rolling_autocorrelation" = {
                     rolling_autocorrelation(data[[var]], window = window_steps)
                   },
                   "zero_crossing_rate" = {
                     zero_crossing_rate(data[[var]], window = window_steps)
                   },

                   # Movement predictability and consistency (calculate from raw data)
                   "movement_predictability" = {
                     # Calculate rolling mean and sd from raw variable data
                     signal <- data[[var]]
                     rolling_mean <- zoo::rollapply(signal, width = window_steps, FUN = mean,
                                                    na.rm = TRUE, fill = NA, align = "center")
                     rolling_sd <- zoo::rollapply(signal, width = window_steps, FUN = sd,
                                                  na.rm = TRUE, fill = NA, align = "center")
                     movement_predictability(rolling_mean, rolling_sd, window = window_steps)
                   },
                   "movement_consistency" = {
                     # Calculate rolling sd from raw variable data
                     signal <- data[[var]]
                     rolling_sd <- zoo::rollapply(signal, width = window_steps, FUN = sd,
                                                  na.rm = TRUE, fill = NA, align = "center")
                     movement_consistency(rolling_sd, window = window_steps)
                   },

                   # Depth-specific features
                   "depth_change_rate" = {
                     if (var != "depth") stop("depth_change_rate can only be applied to depth variable")
                     depth_metrics <- depth_change_metrics(data$depth, window = window_steps)
                     depth_metrics$rate
                   },
                   "depth_change_consistency" = {
                     if (var != "depth") stop("depth_change_consistency can only be applied to depth variable")
                     depth_metrics <- depth_change_metrics(data$depth, window = window_steps)
                     depth_metrics$consistency
                   },

                   # Feeding-specific features (commented out as they require multiple columns)
                   # "feeding_posture_index" = {
                   #   if (!all(c("pitch_sd", "roll_sd", "odba_mean", "vedba_mean") %in% names(data))) {
                   #     stop("feeding_posture_index requires pitch_sd, roll_sd, odba_mean, and vedba_mean columns")
                   #   }
                   #   feeding_posture_index(data$pitch_sd, data$roll_sd, data$odba_mean, data$vedba_mean, window = window_steps)
                   # },

                   # Default case
                   {
                     stop(paste("Unknown enhanced metric:", metric))
                   }
  )

  # Ensure result has correct length
  return(ensure_length(result, n_rows))
}

################################################################################
# 1. HEADING CHANGE FEATURES (Enhanced) ########################################
################################################################################

# Net change in heading over a centered window
net_heading_change <- function(heading, window = 10) {
  n <- length(heading)
  if (n < window * 2) return(rep(NA, n))  # Need enough data for lead/lag
  h <- circular::circular(heading, units = "degrees", template = "geographics")
  # Use smaller shifts - the original was using full window size
  half_window <- window %/% 2
  h_lead <- data.table::shift(h, n = half_window, type = "lead")
  h_lag  <- data.table::shift(h, n = half_window, type = "lag")
  diff_heading <- abs(as.numeric(h_lead) - as.numeric(h_lag))
  idx <- !is.na(diff_heading) & diff_heading > 180
  diff_heading[idx] <- 360 - diff_heading[idx]
  return(diff_heading)
}

# Cumulative sum of absolute heading changes
cumulative_heading_change <- function(heading, window = 10) {
  n <- length(heading)
  if (n < 2) return(rep(NA, n))
  h <- circular::circular(heading, units = "degrees", template = "geographics")
  diffs <- diff(h)
  diffs <- as.numeric(diffs)
  diffs[diffs > 180] <- diffs[diffs > 180] - 360
  diffs[diffs < -180] <- diffs[diffs < -180] + 360
  diffs <- abs(diffs)
  # Ensure we don't use a window larger than available data
  actual_window <- min(window, length(diffs))
  if (actual_window < 1) return(rep(NA, n))
  cum_change <- zoo::rollapply(diffs, width = actual_window, FUN = sum,
                               fill = NA, align = "center", partial = TRUE)
  return(c(NA, cum_change))
}

# Fixed circular variance of heading function
circular_variance_heading <- function(heading, window = 60) {
  n <- length(heading)
  if (n < window) return(rep(NA, n))

  # Convert to radians once
  h_rad <- heading * pi / 180

  # Pre-allocate result vector
  result <- rep(NA_real_, n)

  # Calculate for each position
  half_window <- window %/% 2
  for (i in (half_window + 1):(n - half_window)) {
    start_idx <- i - half_window
    end_idx <- start_idx + window - 1

    # Get window data
    window_data <- h_rad[start_idx:end_idx]
    valid_data <- window_data[!is.na(window_data)]

    if (length(valid_data) >= window/2) {
      # Calculate mean resultant length directly
      cos_sum <- sum(cos(valid_data))
      sin_sum <- sum(sin(valid_data))
      rho <- sqrt(cos_sum^2 + sin_sum^2) / length(valid_data)
      result[i] <- 1 - rho
    }
  }

  return(result)
}

# U-turn detection flag
uturn_flag <- function(heading, window = 60) {
  # Handle edge cases but still return 0s/1s where possible
  n <- length(heading)
  if (n < 10) return(rep(0, n))  # Too little data - assume no U-turns
  # Adjust window if needed but don't make it too small
  actual_window <- min(window, n)
  if (actual_window < 5) actual_window <- min(5, n)
  h <- circular::circular(heading, units = "degrees", template = "geographics")
  # Calculate net heading change over window
  net_changes <- zoo::rollapply(h, width = actual_window, FUN = function(x) {
    # Remove NAs first
    x_clean <- x[!is.na(x)]
    if (length(x_clean) < 2) return(0)
    start_heading <- as.numeric(x_clean[1])
    end_heading <- as.numeric(x_clean[length(x_clean)])
    # Handle potential NA values
    if (is.na(start_heading) || is.na(end_heading)) return(0)
    # Calculate angular difference
    diff_heading <- abs(end_heading - start_heading)
    if (diff_heading > 180) diff_heading <- 360 - diff_heading
    # Flag as U-turn if net change > 120 degrees - return 1 or 0, never NA
    return(as.numeric(diff_heading > 120))
  }, fill = 0, align = "center", partial = TRUE)
  # Ensure correct length and fill with 0s if needed
  if (length(net_changes) != n) {
    result <- rep(0, n)  # Default to 0, not NA
    if (length(net_changes) > 0) {
      copy_length <- min(length(net_changes), n)
      # Handle the centering offset
      if (actual_window > 1) {
        offset <- (actual_window - 1) %/% 2
        start_idx <- offset + 1
        end_idx <- min(start_idx + copy_length - 1, n)
        result[start_idx:end_idx] <- net_changes[1:(end_idx - start_idx + 1)]
      } else {
        result[1:copy_length] <- net_changes[1:copy_length]
      }
    }
    return(result)
  }

  return(net_changes)
}



# Heading autocorrelation average
heading_autocorr_avg <- function(heading, window = 60) {
  n <- length(heading)
  if (n < window) {
    if (n < 10) return(rep(NA, n))
    window <- max(10, n %/% 2)  # Use smaller window
  }
  h <- circular::circular(heading, units = "degrees", template = "geographics")
  autocorr_values <- zoo::rollapply(as.numeric(h), width = window, FUN = function(x) {
    # Remove NAs and check for sufficient valid data
    x_clean <- x[!is.na(x)]
    if (length(unique(x_clean)) <= 1 || length(x_clean) < 10) return(NA)
    tryCatch({
      # Calculate autocorrelation for lags 1-5 and take mean
      max_lag <- min(5, length(x_clean) %/% 4)  # Adjust max lag based on data
      if (max_lag < 1) return(NA)
      acf_result <- acf(x_clean, plot = FALSE, lag.max = max_lag, na.action = na.pass)
      if (length(acf_result$acf) < 2) return(NA)
      mean(acf_result$acf[2:min(6, length(acf_result$acf))], na.rm = TRUE)
    }, error = function(e) NA)
  }, fill = NA, align = "center", partial = TRUE)

  # Ensure correct length
  if (length(autocorr_values) != n) {
    result <- rep(NA, n)
    if (length(autocorr_values) > 0) {
      copy_length <- min(length(autocorr_values), n)
      result[1:copy_length] <- autocorr_values[1:copy_length]
    }
    return(result)
  }

  return(autocorr_values)
}


################################################################################
# 2. OSCILLATION REGULARITY FEATURES (Adapted for 1Hz) #########################
################################################################################

# Simplified oscillation regularity using existing means/SDs
oscillation_regularity <- function(signal, window = 60) {
  if (length(signal) < window || sum(!is.na(signal)) < 10) return(NA)

  # Use lower threshold for 1Hz data
  threshold <- quantile(signal, 0.55, na.rm = TRUE)
  signal_smooth <- zoo::rollmean(signal, k = 3, fill = "extend")

  # Simple peak detection
  peaks_idx <- which(diff(sign(diff(signal_smooth))) == -2) + 1
  if (length(peaks_idx) < 3) return(NA)

  intervals <- diff(peaks_idx)
  if (length(intervals) < 2) return(NA)

  cv_intervals <- sd(intervals, na.rm = TRUE) / mean(intervals, na.rm = TRUE)
  return(cv_intervals)
}

################################################################################
# 3. MOVEMENT PREDICTABILITY AND CONSISTENCY ###################################
################################################################################

# Movement predictability using ratio of rolling SD to mean
movement_predictability <- function(signal_mean, signal_sd, window = 60) {

  if (length(signal_mean) < window) return(NA)
  rolling_cv <- zoo::rollapply(signal_mean, width = window,
                               FUN = function(x) {
                                 if (sum(!is.na(x)) < window/2) return(NA)
                                 sd(x, na.rm = TRUE) / (abs(mean(x, na.rm = TRUE)) + 0.001)
                               }, fill = "extend", align = "center")
  return(rolling_cv)
}

# Movement consistency using existing SD metrics
movement_consistency <- function(signal_sd, window = 60) {

  rolling_cv_sd <- zoo::rollapply(signal_sd, width = window,
                                  FUN = function(x) {
                                    if (sum(!is.na(x)) < window/2) return(NA)
                                    sd(x, na.rm = TRUE) / (mean(x, na.rm = TRUE) + 0.001)
                                  }, fill = "extend", align = "center")
  return(rolling_cv_sd)
}

################################################################################
# 4. SMOOTHNESS FEATURES (Adapted for 1Hz) #####################################
################################################################################

# Movement smoothness using rate of change
movement_smoothness <- function(signal, window = 30) {
  n <- length(signal)
  if (n < 3) return(rep(NA, n))

  velocity <- diff(signal)  # First derivative
  acceleration <- diff(velocity)  # Second derivative

  # Rolling RMS of acceleration
  acc_squared <- acceleration^2

  if (window > length(acc_squared)) {
    # If window is larger than available data, return constant value
    rms_val <- sqrt(mean(acc_squared, na.rm = TRUE))
    return(rep(rms_val, n))
  }

  rms_acc <- sqrt(zoo::rollapply(acc_squared, width = window,
                                 FUN = mean, na.rm = TRUE, fill = NA, align = "center"))

  # Pad to match original length
  result <- rep(NA, n)
  result[3:(length(rms_acc) + 2)] <- rms_acc

  return(result)
}



# Enhanced jerk calculation
movement_jerk <- function(signal, window = 30) {
  if (length(signal) < 4) return(rep(NA, length(signal)))

  vel <- diff(signal)
  acc <- diff(vel)
  jerk <- diff(acc)
  jerk_squared <- jerk^2

  rms_jerk <- sqrt(zoo::rollapply(jerk_squared, width = min(window, length(jerk_squared)),
                                  FUN = mean, na.rm = TRUE, fill = "extend", align = "center"))

  # Pad to match original length
  result <- rep(NA, length(signal))
  result[4:length(result)] <- rms_jerk
  return(result)
}


################################################################################
# 5. POSTURE STABILITY (Using existing SD metrics) #############################
################################################################################

posture_stability_from_sd <- function(data, window = 60) {
  # Check for required columns - use raw pitch and roll data
  if (!all(c("pitch", "roll") %in% names(data))) {
    stop("posture_stability_from_sd requires pitch and roll columns in data")
  }

  n <- nrow(data)
  if (n < window) {
    return(rep(NA, n))
  }

  # Calculate rolling standard deviations
  pitch_instability <- zoo::rollapply(data$pitch, width = window, FUN = sd,
                                      na.rm = TRUE, fill = NA, align = "center")
  roll_instability <- zoo::rollapply(data$roll, width = window, FUN = sd,
                                     na.rm = TRUE, fill = NA, align = "center")

  # Combined stability (inverse of instability)
  stability <- 1 / (1 + pitch_instability + roll_instability)

  # Ensure result has same length as input
  if (length(stability) != n) {
    result <- rep(NA, n)
    valid_indices <- !is.na(stability)
    result[valid_indices] <- stability[valid_indices]
    return(result)
  }

  return(stability)
}



################################################################################
# 6. TURNING BEHAVIOR ##########################################################
################################################################################

turning_rate_variability <- function(heading, window = 60) {

  h <- circular::circular(heading, units = "degrees", template = "geographics")
  turning_rates <- abs(diff(as.numeric(h)))
  turning_rates[turning_rates > 180] <- 360 - turning_rates[turning_rates > 180]

  cv_turning <- zoo::rollapply(turning_rates, width = window,
                               FUN = function(x) {
                                 if (sum(!is.na(x)) < 2) return(NA)
                                 mean_val <- mean(x, na.rm = TRUE)
                                 if (mean_val == 0) return(0)
                                 sd(x, na.rm = TRUE) / mean_val
                               },
                               fill = "extend", align = "center")
  return(c(NA, cv_turning))
}

################################################################################
# 7. ACTIVITY INDICES (Enhanced) ###############################################
################################################################################

# Activity index using existing rate metrics
activity_index <- function(data, window = 60) {
  # Check for required columns - use raw sensor data
  required_cols <- c("pitch", "roll", "heading")
  if (!all(required_cols %in% names(data))) {
    stop(paste("activity_index requires columns:", paste(required_cols, collapse = ", ")))
  }

  n <- nrow(data)
  if (n < 2) {
    return(rep(NA, n))
  }

  # Calculate rates from raw data
  pitch_rate <- abs(c(NA, diff(data$pitch)))
  roll_rate <- abs(c(NA, diff(data$roll)))

  # For heading, handle circular differences
  heading_diffs <- diff(data$heading)
  heading_diffs[heading_diffs > 180] <- heading_diffs[heading_diffs > 180] - 360
  heading_diffs[heading_diffs < -180] <- heading_diffs[heading_diffs < -180] + 360
  heading_rate <- abs(c(NA, heading_diffs))

  # Combine rates (simple approach - avoid standardization issues)
  combined_rate <- pitch_rate + roll_rate + heading_rate

  # Calculate rolling mean activity level
  if (window > n) {
    return(rep(mean(combined_rate, na.rm = TRUE), n))
  }

  activity_level <- zoo::rollapply(combined_rate, width = window, FUN = mean,
                                   na.rm = TRUE, fill = NA, align = "center")

  # Ensure result has same length as input
  if (length(activity_level) != n) {
    result <- rep(NA, n)
    valid_length <- min(length(activity_level), n)
    result[1:valid_length] <- activity_level[1:valid_length]
    return(result)
  }

  return(activity_level)
}

################################################################################
# 8. ADDITIONAL UTILITY FEATURES ###############################################
################################################################################

# Rolling autocorrelation
rolling_autocorrelation <- function(signal, window = 60) {
  zoo::rollapply(signal, width = window, FUN = function(x) {
    if (length(unique(x)) > 1 && sum(!is.na(x)) > 3) {
      tryCatch({
        acf(x, plot = FALSE, lag.max = 1, na.action = na.pass)$acf[2]
      }, error = function(e) NA)
    } else NA
  }, fill = NA, align = "center")
}

# Zero-crossing rate
zero_crossing_rate <- function(signal, window = 60) {
  zoo::rollapply(signal, width = window, FUN = function(x) {
    if (sum(!is.na(x)) < window/2) return(NA)
    x_centered <- x - mean(x, na.rm = TRUE)
    signs <- sign(x_centered)
    sum(diff(signs) != 0, na.rm = TRUE) / length(x)
  }, fill = NA, align = "center")
}

# Depth change patterns
depth_change_metrics <- function(depth_mean, window = 60) {
  depth_change_rate <- abs(c(NA, diff(depth_mean)))
  depth_change_consistency <- zoo::rollapply(depth_change_rate, width = window,
                                             FUN = function(x) {
                                               if (sum(!is.na(x)) < window/2) return(NA)
                                               mean_val <- mean(x, na.rm = TRUE)
                                               if (mean_val == 0) return(0)
                                               sd(x, na.rm = TRUE) / mean_val
                                             },
                                             fill = "extend", align = "center")
  return(list(rate = depth_change_rate, consistency = depth_change_consistency))
}


################################################################################
# 9. FEEDING-SPECIFIC FEATURES #################################################
################################################################################

# Circling behavior detection
circling_behavior <- function(heading, window = 120) {
  h <- circular::circular(heading, units = "degrees", template = "geographics")

  # Calculate net rotation over window
  net_rotation <- zoo::rollapply(h, width = window, FUN = function(x) {
    if (length(x) < window/2) return(NA)
    total_change <- sum(abs(diff(as.numeric(x))), na.rm = TRUE)
    net_change <- abs(as.numeric(x[length(x)]) - as.numeric(x[1]))
    if (net_change > 180) net_change <- 360 - net_change

    # High total change with low net change indicates circling
    if (total_change == 0) return(0)
    circling_index <- total_change / (net_change + 1)
    return(circling_index)
  }, fill = "extend", align = "center")

  return(net_rotation)
}

# Feeding posture detection (based on pitch/roll stability and low activity)
feeding_posture_index <- function(pitch_sd, roll_sd, odba_mean, vedba_mean, window = 60) {
  # Normalize metrics
  pitch_stability <- 1 / (1 + pitch_sd)
  roll_stability <- 1 / (1 + roll_sd)
  low_activity <- 1 / (1 + odba_mean + vedba_mean)

  # Combined feeding posture index
  feeding_index <- zoo::rollapply(pitch_stability + roll_stability + low_activity,
                                  width = window, FUN = mean,
                                  na.rm = TRUE, fill = "extend", align = "center")
  return(feeding_index)
}

################################################################################
################################################################################
################################################################################

# Helper function to ensure consistent vector lengths
ensure_length <- function(result_vector, target_length) {
  if (length(result_vector) == target_length) {
    return(result_vector)
  } else if (length(result_vector) == 1) {
    # If single value, replicate it
    return(rep(result_vector, target_length))
  } else if (length(result_vector) > target_length) {
    # If too long, truncate
    return(result_vector[1:target_length])
  } else {
    # If too short, pad with NAs
    result <- rep(NA, target_length)
    result[1:length(result_vector)] <- result_vector
    return(result)
  }
}

################################################################################
################################################################################
################################################################################

# Add this helper function to avoid redundant naming
create_feature_name <- function(variable, metric, window_seconds, default_window_size) {

  # Define metrics that already contain the variable name and their clean versions
  metric_name_mapping <- list(
    "depth_change_rate" = "change_rate",
    "depth_change_consistency" = "change_consistency",
    "posture_stability" = "stability",
    "activity_index" = "index",
    "net_heading_change" = "net_change",
    "cumulative_heading_change" = "cumulative_change",
    "circular_variance_heading" = "circular_variance",
    "turning_rate_variability" = "turning_variability",
    "circling_behavior" = "circling",
    "feeding_posture_index" = "feeding_index",
    "uturn_flag" = "uturn",
    "heading_autocorr_avg" = "autocorr_avg"
  )

  # Clean the metric name if it's redundant
  if (metric %in% names(metric_name_mapping)) {
    clean_metric <- metric_name_mapping[[metric]]
    base_name <- paste0(variable, "_", clean_metric)
  } else {
    # For regular metrics, combine variable + metric normally
    base_name <- paste0(variable, "_", metric)
  }

  # Check if metric already contains the variable name
  if (metric %in% names(metric_name_mapping)) {
    clean_metric <- metric_name_mapping[[metric]]
    base_name <- paste0(variable, "_", clean_metric)
  } else {
    # For regular metrics, combine variable + metric
    base_name <- paste0(variable, "_", metric)
  }

  # Add window suffix if different from default
  if (window_seconds != default_window_size) {
    final_name <- paste0(base_name, "_", window_seconds, "s")
  } else {
    final_name <- base_name
  }

  return(final_name)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
