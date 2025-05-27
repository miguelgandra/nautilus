#######################################################################################################
# Function to estimate tail beat frequencies ##########################################################
#######################################################################################################

#' Calculate Tail Beat Frequencies
#'
#' This function uses continuous wavelet transform (CWT) to estimate tail beat frequencies
#' from motion sensor data. It can process data from multiple individuals
#' in parallel and provides visualization of the power spectrum with identified tail beats.
#' It features intelligent batch processing with automatic buffer zones to handle large
#' datasets efficiently while maintaining accuracy at batch boundaries.
#'
#' @param data A data frame or a list of data frames containing depth profile information.
#' @param id.col Character. The name of the column identifying individuals (default: "ID").
#' @param datetime.col Character. The name of the column containing datetime information (default: "datetime").
#' @param motion.col Character. The name of the column containing motion data (e.g., "sway") to analyze.
#' @param min.freq.Hz Numeric. The lowest frequency of interest (in Hz). Default is 0.05 Hz.
#' @param max.freq.Hz Numeric. The highest frequency of interest (in Hz). Default is 3 Hz.
#' @param max.rows.per.batch Numeric. Maximum number of rows to process in each batch when handling large datasets.
#' The function automatically determines optimal batch sizes based on frequency resolution requirements,
#' but this parameter sets an upper limit to prevent memory issues. Default is 200,000 rows.
#' Lower values reduce memory usage but may increase processing time for very large datasets.
#' @param smooth.window Numeric. Window size (in seconds) for moving average smoothing of frequency estimates.
#' Set to 0 to disable smoothing. Default is 10 seconds.
#' @param ridge.only Logical. If TRUE, only returns frequencies identified as ridges (connected local maxima)
#' in the wavelet power spectrum. If FALSE (default), uses all local maxima in the power spectrum.
#' @param power.ratio.threshold Numeric or NULL. Minimum ratio between peak power and average power
#' for a frequency to be considered valid. NULL disables this filter. Default is NULL.
#' @param max.interp.gap Numeric. Maximum gap (in seconds) for linear interpolation of missing frequency values.
#' Set to NULL to disable interpolation. Default is 10 seconds.
#' @param n.cores Number of CPU cores for parallel processing (default = 1)
#' @param plot Logical. Whether to generate plots. Default is TRUE.
#' @param output.dir Character. If provided, all plots will be saved as PNG files in this directory instead of being displayed interactively.
#'                   Default is NULL (plots shown in R graphics device).
#' @param png.width Numeric. Width of PNG output in inches (default = 10).
#' @param png.height Numeric. Height of PNG output in inches (default = 7).
#' @param png.res Numeric. Resolution of PNG output in ppi (default = 300).
#'
#' @return Returns the input data with added column \code{tbf_hz} containing estimated tail beat
#'         frequencies in Hz. For list inputs, returns a list of modified data frames.
#'
#' @details
#' The function implements a comprehensive workflow for tail beat frequency estimation:
#'
#' \strong{Frequency Detection Methods:}
#' \itemize{
#'   \item \code{ridge.only = TRUE}: Conservative approach using only connected maxima (ridges)
#'         in the wavelet power spectrum. Most robust against noise but may miss weaker signals.
#'
#'   \item \code{ridge.only = FALSE}: More inclusive approach using all local maxima, with optional
#'         quality filtering via \code{power.ratio.threshold}.
#' }
#'
#' \strong{Quality Control:}
#' \itemize{
#'   \item \code{power.ratio.threshold}: Filters frequencies where peak-to-average power ratio
#'         is below threshold (recommended: 1.5-3 for typical data)
#'
#'   \item \code{smooth.window}: Applies temporal smoothing to reduce high-frequency variability
#'         in estimates while preserving true behavioral patterns
#' }
#'
#' \strong{Gap Handling:}
#' \itemize{
#'   \item \code{max.interp.gap}: Intelligently fills short data gaps (< specified seconds) via
#'         linear interpolation while preserving longer gaps as NA values. This maintains signal
#'         continuity during brief measurement drops without over-interpolating prolonged gaps.
#' }
#'
#'\strong{Intelligent Batch Processing:}
#' \itemize{
#'   \item Automatically processes large datasets in memory-efficient batches when sampling frequency >1 Hz
#'   \item Implements frequency-adaptive buffer zones (based on min.freq.Hz) to prevent edge artifacts
#'   \item Seamlessly stitches results while maintaining temporal accuracy
#' }
#'
#' @note
#' The function requires the 'WaveletComp' package for the wavelet transform. For parallel processing,
#' it requires the 'foreach', 'doSNOW', and 'parallel' packages.
#'
#' \strong{Sampling Frequency Requirements:}
#' The input data must have a sampling frequency (1/timestep) that satisfies the Nyquist criterion:
#' \itemize{
#'   \item Minimum requirement: Sampling frequency > 2 x \code{max.freq.Hz}
#'   \item Recommended: Sampling frequency ≥ 4 x \code{max.freq.Hz} for reliable results
#'   \item Example: With \code{max.freq.Hz = 3}, data should be sampled at ≥ 12 Hz (minimum 6 Hz)
#' }#'
#'
#' @export


calculateTailBeats <- function(data,
                               id.col = "ID",
                               datetime.col = "datetime",
                               motion.col = "sway",
                               min.freq.Hz = 0.05,
                               max.freq.Hz = 3,
                               smooth.window = 10,
                               max.rows.per.batch = 2e5,
                               ridge.only = FALSE,
                               power.ratio.threshold = NULL,
                               max.interp.gap = 10,
                               n.cores = 1,
                               plot = TRUE,
                               output.dir = NULL,
                               png.width = 24,
                               png.height = 6,
                               png.res = 600) {

  ##############################################################################
  # Initial checks and setup ###################################################
  ##############################################################################

  # start the timer
  start.time <- Sys.time()

  # check if the 'WaveletComp' package is installed.
  if(!requireNamespace("WaveletComp", quietly=TRUE)) stop("The 'WaveletComp' package is required but is not installed. Please install 'WaveletComp' using install.packages('WaveletComp') and try again.", call. = FALSE)

  # if 'data' is not a list, split it into a list of individual data sets
  if (!is.list(data)) {
    data <- split(data, f = data[[id.col]])
  }

  # Check if list contains data frames
  if (!all(sapply(data, is.data.frame))) {
    stop("All elements in the input list must be data frames", call. = FALSE)
  }

  # validate column specifications
  if (!is.character(id.col) || length(id.col) != 1) stop("'id.col' must be a single character value", call. = FALSE)
  if (!is.character(datetime.col) || length(datetime.col) != 1) stop("'datetime.col' must be a single character value", call. = FALSE)
  if (!is.character(motion.col) || length(motion.col) != 1) stop("'motion.col' must be a single character value", call. = FALSE)

  # check if specified columns exist in the data
  if(!id.col %in% names(data[[1]])) stop(paste0("The specified id.col ('", id.col, "') was not found in the supplied data."), call. = FALSE)
  if(!datetime.col %in% names(data[[1]])) stop(paste0("The specified datetime.col ('", datetime.col, "') was not found in the supplied data."), call. = FALSE)
  if(!motion.col %in% names(data[[1]])) stop(paste0("The specified motion.col ('", motion.col, "') was not found in the supplied data."), call. = FALSE)

  # validate frequency parameters
  if (!is.numeric(min.freq.Hz) || length(min.freq.Hz) != 1 || min.freq.Hz <= 0) stop("'min.freq.Hz' must be a single positive numeric value", call. = FALSE)
  if (!is.numeric(max.freq.Hz) || length(max.freq.Hz) != 1 || max.freq.Hz <= 0) stop("'max.freq.Hz' must be a single positive numeric value", call. = FALSE)
  if (min.freq.Hz >= max.freq.Hz) stop("'min.freq.Hz' must be less than 'max.freq.Hz'", call. = FALSE)

  # warn if frequency range seems unusual for tail beats
  if (max.freq.Hz > 5) warning(paste("Specified max.freq.Hz of", max.freq.Hz, "Hz seems unusually high for tail beat frequencies. Typical fish tail beats range between 0.1-3 Hz."))
  if (min.freq.Hz < 0.01) warning(paste("Specified min.freq.Hz of", min.freq.Hz, "Hz seems unusually low for tail beat frequencies. This may detect non-tail-beat movements."))

  # validate smoothing window
  if (!is.numeric(smooth.window) || length(smooth.window) != 1 || smooth.window < 0) stop("'smooth.window' must be a single non-negative numeric value", call. = FALSE)
  if (smooth.window > 60) warning(paste("Large smoothing window of", smooth.window, "seconds may obscure true tail beat patterns"))

  # validate ridge.only
  if (!is.logical(ridge.only) || length(ridge.only) != 1) stop("'ridge.only' must be a single logical value (TRUE/FALSE)", call. = FALSE)

  # validate power ratio threshold
  if (!is.null(power.ratio.threshold)) {
    if (!is.numeric(power.ratio.threshold) || length(power.ratio.threshold) != 1 || power.ratio.threshold <= 0) {
      stop("'power.ratio.threshold' must be NULL or a single positive numeric value", call. = FALSE)
    }
    if (power.ratio.threshold < 1) {
      warning("power.ratio.threshold < 1 may not effectively filter low-quality signals")
    }
  }

  # validate max interpolation gap
  if (!is.null(max.interp.gap)) {
    if (!is.numeric(max.interp.gap) || length(max.interp.gap) != 1 || max.interp.gap <= 0) {
      stop("'max.interp.gap' must be NULL or a single positive numeric value", call. = FALSE)
    }
    if (max.interp.gap > 30) {
      warning(paste("Large max.interp.gap of", max.interp.gap, "seconds may lead to over-interpolation of missing data"))
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

  # create output directory if it doesn't exist
  if (!is.null(output.dir)) {
    if (!dir.exists(output.dir)) {
      stop(, call. = FALSE)
    }
    # force plotting when output directory is specified
    plot <- TRUE
  }


  ##############################################################################
  # Validate Sampling Frequency for Frequency Estimation #######################
  ##############################################################################

  # check if sampling frequency is sufficient for the requested max frequency
  # we want at least 4 samples per cycle for reliable frequency estimation
  # (Nyquist would be 2, but we're conservative)


  # retrieve data sampling frequencies
  data_hz <- unlist(lapply(data, function(dt) {
    if ("processed.sampling.frequency" %in% names(attributes(dt))) {
      attributes(dt)$processed.sampling.frequency
    } else {
       sf <- nrow(data_individual) / as.numeric(difftime(max(data_individual$date), min(data_individual$date), units = "secs"))
       round(sf / 5) * 5
    }
  }))

  # calculate Nyquist criteria
  nyquist_crit <- max.freq.Hz * 2
  recommended_crit <- max.freq.Hz * 4

  # identify problematic datasets
  insufficient_hz <- data_hz < nyquist_crit & !is.na(data_hz)
  marginal_hz <- data_hz >= nyquist_crit & data_hz < recommended_crit & !is.na(data_hz)

  # generate message only for the most severe issue
  if (any(insufficient_hz)) {
    offenders <- which(insufficient_hz)
    offender_names <- if (!is.null(names(data))) names(data)[offenders] else paste("Dataset", offenders)
    offender_freqs <- data_hz[offenders]
    stop(paste0(
      "Insufficient sampling frequency for ", length(offenders), " dataset(s):\n",
      paste0("- ", offender_names, ": ", offender_freqs, " Hz", collapse = "\n"), "\n\n",
      "Nyquist theorem requires at least ", nyquist_crit, " Hz to detect ", max.freq.Hz, " Hz frequencies.\n",
      "Recommended minimum is ", recommended_crit, " Hz (4x Nyquist) for reliable analysis.\n\n",
      "Solutions:\n",
      "1) Reduce max.freq.Hz to \u2264 ", round(min(data_hz, na.rm = TRUE)/4, 2), " Hz\n",
      "2) Use higher frequency data"
    ), call. = FALSE)
  } else if (any(marginal_hz)) {
    marginal_names <- if (!is.null(names(data))) names(data)[marginal_hz] else paste("Dataset", which(marginal_hz))
    warning(paste0(
      "Marginal sampling frequency for ", length(marginal_names), " dataset(s):\n",
      paste0("- ", marginal_names, ": ", data_hz[marginal_hz], " Hz", collapse = "\n"), "\n",
      "While above Nyquist rate (", nyquist_crit, " Hz), frequencies near ", max.freq.Hz,
      " Hz may be unreliable.\n",
      "Recommended minimum is ", recommended_crit, " Hz for robust analysis.\n",
      "Interpret high frequencies with caution or reduce max.freq.Hz parameter."
    ), call. = FALSE)
  }


  ##############################################################################
  # Perform Continuous Wavelet Transform (CWT) #################################
  ##############################################################################

  # calculate number of animals
  n_animals <- length(data)

  cat(paste0(
    crayon::bold("\n==================== Estimating Tail Beats ====================\n"),
    "Using ", motion.col, " data to estimate tail beat frequencies for ", n_animals,
    " ", ifelse(n_animals == 1, "tag", "tags"), "\n",
    crayon::bold("===============================================================\n\n")
  ))

  # Process each individual (sequentially)
  for (i in seq_along(data)) {
    current_id <- names(data)[i]
    cat(sprintf("[%d/%d] %s\n", i, n_animals, current_id))

    # Process the individual with parallel batch processing
    data[[i]] <- .runCWT(
      dt = data[[i]],
      animal_id = current_id,
      id.col = id.col,
      datetime.col = datetime.col,
      motion.col = motion.col,
      min.freq.Hz = min.freq.Hz,
      max.freq.Hz = max.freq.Hz,
      max.rows.per.batch = max.rows.per.batch,
      ridge.only = ridge.only,
      power.ratio.threshold = power.ratio.threshold,
      smooth.window = smooth.window,
      max.interp.gap = max.interp.gap,
      plot = plot,
      output.dir = output.dir,
      png.width = png.width,
      png.height = png.height,
      png.res = png.res,
      n.cores = n.cores
    )

    # Add space between individuals
    cat("\n")
  }


  ############################################################################
  # Return processed data ####################################################
  ############################################################################

  # print time taken
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  cat(crayon::bold("Total execution time:"), sprintf("%.02f", as.numeric(time.taken)), base::units(time.taken), "\n\n")

  # return the list containing tail beat metrics
  return(data)

}



################################################################################
# Helper function to process individual datasets ###############################
################################################################################

#' Internal wavelet function
#'
#' @inheritParams calculateTailBeats
#' @param dt Input data.table for one individual
#' @param animal_id ID of current individual
#'
#' @return data.table with dead-reckoned positions and VPC corrections
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal

.runCWT <- function(dt, animal_id, id.col, datetime.col, motion.col,
                    min.freq.Hz, max.freq.Hz, max.rows.per.batch, ridge.only,
                    power.ratio.threshold, smooth.window, max.interp.gap, plot,
                    output.dir, png.width, png.height, png.res, n.cores = 1) {

  # extract relevant columns and rename
  data_individual <- data.frame(
    ID = dt[[id.col]],
    date = dt[[datetime.col]],
    motion = dt[[motion.col]]
  )

  # get sampling frequency
  sampling_freq <- if ("processed.sampling.frequency" %in% names(attributes(dt))) {
    attributes(dt)$processed.sampling.frequency
  } else {
    sampling_rate <- nrow(data_individual) /
      as.numeric(difftime(max(data_individual$date),
                          min(data_individual$date),
                          units = "secs"))
    round(sampling_rate / 5) * 5
  }

  # sort by time and find valid data range
  data_individual <- data_individual[order(data_individual$date), ]
  valid_idx <- which(!is.na(data_individual$motion))
  if (length(valid_idx) == 0) {
    warning(paste("No valid motion data for ID:", animal_id), call. = FALSE)
    dt$tbf_hz <- NA_real_
    return(dt)
  }
  first_valid <- min(valid_idx)
  last_valid <- max(valid_idx)
  valid_data <- data_individual[first_valid:last_valid, ]

  # remove data_individual to free up memory
  n_rows <- nrow(data_individual)
  rm(data_individual)
  gc()

  # initialize variables for combined results
  n_rows_valid <- nrow(valid_data)
  combined_power <- NULL
  combined_periods <- NULL
  combined_axis1 <- NULL
  combined_coi1 <- NULL
  combined_coi2 <- NULL
  combined_dates <- NULL
  dominant_freqs <- numeric(n_rows_valid)
  coi_exclusions <- list(total_excluded = 0, batches_with_exclusions = 0)


  #####################################################################
  # Determine batch sizes #############################################
  #####################################################################

  # how many cycles of min.freq.Hz we want to resolve
  min_duration_cycles <- 6
  buffer_cycles <- 6

  # estimate the minimum batch length in samples to resolve low freq
  min_core_samples <- ceiling(min_duration_cycles * (1 / min.freq.Hz) * sampling_freq)
  buffer_samples <- ceiling(buffer_cycles * (1 / min.freq.Hz) * sampling_freq)
  min_total_batch_size <- min_core_samples + 2 * buffer_samples

  # limit batch size to something memory-safe
  batch_size <- max(min_total_batch_size, min(max.rows.per.batch, n_rows_valid))

  # calculate number of batches
  n_batches <- ceiling(n_rows_valid / batch_size)


  #####################################################################
  # Process in batches (sequential) ###################################
  #####################################################################

  if (n.cores == 1) {

    # initiate progress bar
    pb <- txtProgressBar(min = 0, max = n_batches, style = 3, width = 50, char = "=")

    # process each batch sequentially
    for (batch in seq_len(n_batches)) {

      batch_result <- .processBatch(
        batch = batch,
        valid_data = valid_data,
        batch_size = batch_size,
        buffer_samples = buffer_samples,
        n_rows_valid = n_rows_valid,
        sampling_freq = sampling_freq,
        min.freq.Hz = min.freq.Hz,
        max.freq.Hz = max.freq.Hz,
        ridge.only = ridge.only,
        power.ratio.threshold = power.ratio.threshold
      )

      # update frequency results
      dominant_freqs[batch_result$core_start:batch_result$core_end] <- batch_result$batch_dominant_freqs

      # update plotting components
      if (is.null(combined_power)) {
        combined_power <- batch_result$wavelet_power
        combined_periods <- batch_result$periods
        combined_axis1 <- batch_result$axis1
        combined_coi1 <- batch_result$coi1
        combined_coi2 <- batch_result$coi2
        combined_dates <- batch_result$dates
      } else {
        combined_power <- cbind(combined_power, batch_result$wavelet_power)
        combined_axis1 <- c(combined_axis1, batch_result$axis1)
        combined_coi1 <- c(combined_coi1, batch_result$coi1)
        combined_coi2 <- c(combined_coi2, batch_result$coi2)
        combined_dates <- c(combined_dates, batch_result$dates)
      }

      # update exclusions counter
      coi_exclusions$total_excluded <- coi_exclusions$total_excluded + batch_result$current_exclusions
      if (batch_result$current_exclusions > 0) {
        coi_exclusions$batches_with_exclusions <- coi_exclusions$batches_with_exclusions + 1
      }

      # update progress bar
      setTxtProgressBar(pb, batch)
    }

    #close progress bar
    close(pb)

    #####################################################################
    # Process in batches (parallel) #####################################
    #####################################################################

    } else {

      # register parallel backend with the specified number of cores
      cl <- parallel::makeCluster(n.cores)
      doSNOW::registerDoSNOW(cl)
      # ensure the cluster is properly stopped when the function exits
      on.exit(parallel::stopCluster(cl))
      # define the `%dopar%` operator locally for parallel execution
      `%dopar%` <- foreach::`%dopar%`

      # initialize progress bar
      pb <- txtProgressBar(min = 0, max = n_batches, style = 3, width = 50, char = "=")
      progress <- function(n) setTxtProgressBar(pb, n)
      opts <- list(progress = progress)

      # process batches in parallel
      batch_results <- foreach::foreach(
        batch = seq_len(n_batches),
        .options.snow = opts,
        .packages = "WaveletComp",
        .export = c(".processBatch")
      ) %dopar% {
        .processBatch(
          batch = batch,
          valid_data = valid_data,
          batch_size = batch_size,
          buffer_samples = buffer_samples,
          n_rows_valid = n_rows_valid,
          sampling_freq = sampling_freq,
          min.freq.Hz = min.freq.Hz,
          max.freq.Hz = max.freq.Hz,
          ridge.only = ridge.only,
          power.ratio.threshold = power.ratio.threshold
        )
      }

      # close progress bar
      close(pb)

      # combine results from parallel processing
      for (result in batch_results) {
        # update frequency results
        dominant_freqs[result$core_start:result$core_end] <- result$batch_dominant_freqs
        # update plotting components
        if (is.null(combined_power)) {
          combined_power <- result$wavelet_power
          combined_periods <- result$periods
          combined_axis1 <- result$axis1
          combined_coi1 <- result$coi1
          combined_coi2 <- result$coi2
          combined_dates <- result$dates
        } else {
          combined_power <- cbind(combined_power, result$wavelet_power)
          combined_axis1 <- c(combined_axis1, result$axis1)
          combined_coi1 <- c(combined_coi1, result$coi1)
          combined_coi2 <- c(combined_coi2, result$coi2)
          combined_dates <- c(combined_dates, result$dates)
        }
        # update exclusions counter
        coi_exclusions$total_excluded <- coi_exclusions$total_excluded + result$current_exclusions
        if (result$current_exclusions > 0) {
          coi_exclusions$batches_with_exclusions <- coi_exclusions$batches_with_exclusions + 1
        }
      }
    }


  #####################################################################
  # Process and save extracted tail beat frequencies ##################
  #####################################################################

  # apply moving average smoothing
  if (smooth.window > 0) {
    # convert window from seconds to samples
    window_samples <- round(smooth.window * sampling_freq)
    # pad the series to avoid NA at edges
    padded <- c(rep(dominant_freqs[1], window_samples),
                dominant_freqs,
                rep(dominant_freqs[length(dominant_freqs)], window_samples))
    # apply moving average
    smoothed <- stats::filter(padded, filter = rep(1/window_samples, window_samples), sides = 2)
    # remove padding and keep valid values
    dominant_freqs <- smoothed[(window_samples + 1):(length(smoothed) - window_samples)]
    dominant_freqs <- as.numeric(dominant_freqs)
  }

  # enforce frequency bounds
  dominant_freqs <- pmin(pmax(dominant_freqs, min.freq.Hz), max.freq.Hz)

  # performs linear interpolation to fill small gaps in the estimates
  if(!is.null(max.interp.gap)){
    # convert time-based gap threshold to samples
    gap_samples <- max.interp.gap * sampling_freq
    # only perform interpolation if gap threshold is positive
    if(gap_samples > 0) {
      # linear interpolation using zoo::na.approx:
      dominant_freqs <- zoo::na.approx(dominant_freqs, maxgap = gap_samples, na.rm = FALSE)
    }
  }

  # create full-length vector with NAs where no data
  tbf <- rep(NA_real_, n_rows)
  tbf[first_valid:last_valid] <- dominant_freqs

  # add to original data
  dt$tbf_hz <- tbf


  #####################################################################
  # Generate plot (if requested) ######################################
  #####################################################################

  if (plot) {

    # create a combined wavelet result object for plotting
    combined_result <- list(
      Power = combined_power,
      Period = combined_periods,
      axis.1 = combined_axis1,
      axis.2 = log2(combined_periods),
      coi.1 = combined_coi1,
      coi.2 = combined_coi2,
      series = list(date = combined_dates)
    )

    # grid regularization - handle NA gaps in time series
    time_gaps <- diff(combined_axis1) > (1.5/sampling_freq)

    # run the grid regularization if gaps exist
    if (any(time_gaps)) {
      # create full regular time grid
      full_time_grid <- seq(from = min(combined_dates), to = max(combined_dates), by = 1/sampling_freq)
      full_axis1 <- seq(0, (length(full_time_grid)-1)/sampling_freq, length.out = length(full_time_grid))
      # create empty power matrix with NA values
      full_power <- matrix(NA, nrow = length(combined_result$Period), ncol = length(full_axis1))
      # find matching time indices (fast approximation when times are nearly aligned)
      bin_width <- 1/sampling_freq
      matched_indices <- round(combined_result$axis.1/bin_width) + 1
      matched_indices <- pmin(pmax(matched_indices, 1), length(full_axis1))
      # only keep indices where the time match is close enough
      time_tolerance <- (1 / sampling_freq) / 2
      valid_matches <- which(abs(full_axis1[matched_indices] - combined_result$axis.1) < time_tolerance)
      matched_indices <- matched_indices[valid_matches]
      # map power values (with dimension check)
      if (length(matched_indices) > 0) {
        full_power[, matched_indices] <- combined_result$Power[, valid_matches]
      }
      # update the result object
      combined_result$axis.1 <- full_axis1
      combined_result$Power <- full_power
      combined_result$series$date <- full_time_grid
    }


    # downsample time axis if too large
    max_points <- 5e5
    if(length(combined_result$axis.1) > max_points) {
      # create a regular grid for axis.1
      regular_grid <- seq(min(combined_result$axis.1),  max(combined_result$axis.1), length.out = max_points)
      # find nearest existing points to the regular grid
      keep <- findInterval(regular_grid, combined_result$axis.1)
      keep <- pmin(keep, length(combined_result$axis.1))
      # apply downsampling
      combined_result$Power <- combined_result$Power[, keep, drop = FALSE]
      combined_result$axis.1 <- combined_result$axis.1[keep]
      combined_result$coi.1 <- combined_result$coi.1[keep]
      combined_result$coi.2 <- combined_result$coi.2[keep]
      combined_result$series$date <- combined_result$series$date[keep]
      # ensure the axis is perfectly regular (optional, forces exact regularity)
      combined_result$axis.1 <- regular_grid
    }


    # if output directory is specified, save to PNG
    if (!is.null(output.dir)) {
      # Create clean filename from animal ID
      png_file <- file.path(output.dir, paste0("tailbeats_", animal_id, ".png"))
      png(png_file, width = png.width, height = png.height, units = "in", res = png.res)
      on.exit(dev.off(), add = TRUE)
    }

    # set margins
    par(mar=c(5, 5, 4, 13))

    # set color breaks
    wavelet_levels <- quantile(combined_power, probs=seq(from=0, to=1, length.out=101))

    # set color palette
    color_pal <- rev(rainbow(100, start = 0, end = .7))

    # plot power spectrum
    image(x = combined_result$axis.1,
          y = combined_result$axis.2,
          z = t(combined_result$Power),
          col = color_pal,
          breaks = wavelet_levels,
          useRaster = TRUE,
          ylab = "",
          xlab = "",
          axes = FALSE,
          main = "",
          cex.main = 1.4)

    # add title
    title(main=animal_id, font.main=2, line=2.4, cex.main=1.6)
    title(main=paste0("[", motion.col, "]"), font.main=2, line=0.85, cex.main=1.4)

    # add date axis
    date_labels <- pretty(combined_result$series$date, n = 9)
    date_indices <- sapply(date_labels, function(x) which.min(abs(combined_result$series$date - x)))
    # keep only regularly spaced ticks (allow differences to vary by ±2 units)
    ok <- abs(diff(date_indices) - .mode(diff(date_indices))) <= 2
    date_labels <- date_labels[c(FALSE, ok) | c(ok, FALSE)]
    date_indices <- date_indices[c(FALSE, ok) | c(ok, FALSE)]
    date_positions <- combined_result$axis.1[date_indices]
    axis(1, at = date_positions,  labels = strftime(date_labels, "%d/%b %H:%M", tz = "UTC"), las = 1, cex.axis = 1.2)
    mtext("Date", side = 1, line = 3, cex = 1.4)

    # add custom frequency axis
    period_breaks <- unique(trunc(combined_result$axis.2))
    period_breaks[period_breaks<log2(combined_result$Period[1])] <- NA
    period_breaks <- na.omit(period_breaks)
    freq_labels <- sprintf("%.2f", 1 / 2^period_breaks)
    axis(2, at = period_breaks, labels = freq_labels, las = 1, cex.axis = 1.2)
    mtext("Frequency (Hz)", side = 2, line = 3.6, cex = 1.4)

    # add period axis
    axis(4, at = period_breaks, labels = 2^period_breaks, las = 1,  cex.axis = 1.2)
    mtext("Period (s)", side = 4, line = 2.5, cex = 1.4)

    # plot cone of influence
    polygon(c(combined_result$coi.1, rev(combined_result$coi.1)),
            c(combined_result$coi.2, rep(max(combined_result$coi.2, na.rm=TRUE), length(combined_result$coi.2))),
            border = NA, col = adjustcolor("black", alpha.f=0.6))

    # plot tail beat frequencies
    if (length(combined_result$axis.1) < length(combined_axis1)) {
      tbf_periods <- 1/dt$tbf_hz[keep]
    } else{
      tbf_periods <- 1/dt$tbf_hz
    }
    valid_indices <- which(!is.na(tbf_periods))
    points(x=combined_result$axis.1[valid_indices], y=log2(tbf_periods[valid_indices]), pch=21, bg="red4", lwd=0.05, cex=0.45)

    # plot color scale
    power_marks  = round(seq(from = 0, to = 1, length.out = 6) * 100)
    power_labels = formatC(as.numeric(wavelet_levels), digits = 2, format = "f")[power_marks + 1]
    .colorlegend(col=color_pal, zlim=c(0,100), zval=power_marks, zlab = power_labels, xpd=NA,
                 posx=c(0.945, 0.955), posy=c(0, 0.8), main = "Power\nSpectrum\n\n",
                 cex=1.2, main.cex=1.4)

    # draw box
    box()
  }


  #####################################################################
  # Return results ####################################################
  #####################################################################

  # last garbage collection
  gc()

  # return result
  return(dt)

}


################################################################################
# Helper function to process a single batch ####################################
################################################################################

#' Helper function to process a single batch
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal

.processBatch <- function(batch, valid_data, batch_size, buffer_samples, n_rows_valid,
                          sampling_freq, min.freq.Hz, max.freq.Hz, ridge.only,
                          power.ratio.threshold) {

  # initialize empty result list
  result <- list(
    batch_dominant_freqs = NULL,
    core_start = NULL,
    core_end = NULL,
    current_exclusions = 0,
    wavelet_result = NULL  # This will store the full wavelet result for plotting
  )

  # buffered batch indices (for wavelet processing)
  batch_start <- max(1, (batch - 1) * batch_size + 1 - buffer_samples)
  batch_end <- min(n_rows_valid, batch * batch_size + buffer_samples)

  # core batch indices (for stitching results)
  result$core_start <- (batch - 1) * batch_size + 1
  result$core_end <- min(batch * batch_size, n_rows_valid)

  # extract data for current batch (including buffer)
  batch_data <- valid_data[batch_start:batch_end, ]

  # determine central portion indices (core batch, without buffer)
  local_core_start <- result$core_start - batch_start + 1
  local_core_end <- result$core_end - batch_start + 1
  central_cols <- local_core_start:local_core_end

  # skip constant batches
  if (length(unique(na.omit(batch_data$motion))) <= 1) {
    warning(
      paste0("Constant motion data detected in batch ", batch, " (wavelet skipped)"),
      call. = FALSE)
    result$batch_dominant_freqs <- rep(NA_real_, result$core_end - result$core_start + 1)
    result$dates <- valid_data$date[batch_start:batch_end][central_cols]
    return(result)
  }

  # perform wavelet transform on batch
  result$wavelet_result <- WaveletComp::analyze.wavelet(
    my.data = batch_data,
    my.series = "motion",
    loess.span = 0,
    dt = 1/sampling_freq,
    dj = 0.05,
    lowerPeriod = 1/max.freq.Hz,
    upperPeriod = 1/min.freq.Hz,
    make.pval = FALSE,
    verbose = FALSE
  )

  # determine dominant tail-beat frequencies
  if (ridge.only) {
    # strict ridge-based approach (connected maxima in wavelet power spectrum)
    ridges <- WaveletComp::ridge(result$wavelet_result$Power[, central_cols, drop = FALSE])
    ridge_indices <- which(colSums(ridges) > 0)
    max_periods <- result$wavelet_result$Period[apply(result$wavelet_result$Power[, central_cols, drop = FALSE], 2, which.max)]
    dominant_periods <- rep(NA_real_, ncol(ridges))
    dominant_periods[ridge_indices] <- max_periods[ridge_indices]
  } else if (!is.null(power.ratio.threshold)) {
    # lenient dominant frequency approach with power ratio filtering
    power_ratio <- apply(result$wavelet_result$Power[, central_cols, drop = FALSE], 2,
                         function(x) max(x)/mean(x))
    dominant_periods <- ifelse(power_ratio > power.ratio.threshold,
                               result$wavelet_result$Period[apply(result$wavelet_result$Power[, central_cols, drop = FALSE], 2, which.max)],
                               NA)
  } else {
    # if no threshold specified, use all maximum power periods as-is
    dominant_periods <- result$wavelet_result$Period[apply(result$wavelet_result$Power[, central_cols, drop = FALSE], 2, which.max)]
  }

  # convert to frequencies
  result$batch_dominant_freqs <- 1 / dominant_periods

  # apply COI exclusion to central portion
  coi_periods <- result$wavelet_result$coi.2[central_cols]
  in_coi <- !is.na(result$batch_dominant_freqs) & (1/result$batch_dominant_freqs > coi_periods)
  result$batch_dominant_freqs[in_coi] <- NA_real_
  result$current_exclusions <- sum(in_coi, na.rm = TRUE)

  # add plotting components to result
  result$wavelet_power <- result$wavelet_result$Power[, central_cols, drop = FALSE]
  result$periods <- result$wavelet_result$Period
  result$axis1 <- result$wavelet_result$axis.1[central_cols] + (batch_start - 1)/sampling_freq
  result$coi1 <- result$wavelet_result$coi.1[central_cols]
  result$coi2 <- result$wavelet_result$coi.2[central_cols]
  result$dates <- valid_data$date[batch_start:batch_end][central_cols]

  return(result)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
