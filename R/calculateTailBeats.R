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
#' @param data A list of data.tables/data.frames, one for each individual; a single aggregated data.table/data.frame
#' containing data from multiple animals (with an 'ID' column); or a character vector of file paths pointing to
#' `.rds` files, each containing data for a single individual. When a character vector is provided,
#' files are loaded sequentially to optimize memory use. The output of the \link{processTagData} function
#' is strongly recommended, as it formats the data appropriately for all downstream analysis.
#' @param id.col Character. The name of the column identifying individuals (default: "ID").
#' @param datetime.col Character. The name of the column containing datetime information (default: "datetime").
#' @param motion.col Character. The name of the column containing motion data (e.g., "sway") to analyze.
#' @param min.freq.Hz Numeric. The lowest frequency of interest (in Hz). Default is 0.05 Hz.
#' @param max.freq.Hz Numeric. The highest frequency of interest (in Hz). Default is 3 Hz.
#' @param bandpass.filter Logical. Whether to apply bandpass filtering before wavelet analysis.
#' Default is TRUE. Highly recommended to reduce noise and improve signal quality.
#' @param filter.low.freq Numeric. Lower cutoff frequency for bandpass filter (in Hz).
#' If NULL (default), uses min.freq.Hz * 0.9 to allow some margin below the minimum frequency of interest.
#' @param filter.high.freq Numeric. Upper cutoff frequency for bandpass filter (in Hz).
#' If NULL (default), uses max.freq.Hz * 1.1 to allow some margin above the maximum frequency of interest.
#' @param filter.order Integer. Filter order for the Butterworth bandpass filter. Default is 4.
#' Higher orders provide steeper cutoffs but may introduce artifacts.
#' @param max.rows.per.batch Numeric. Maximum number of rows to process in each batch when handling large datasets.
#' The function automatically determines optimal batch sizes based on frequency resolution requirements,
#' but this parameter sets an upper limit to prevent memory issues. Default is 200,000 rows.
#' Lower values reduce memory usage but may increase processing time for very large datasets.
#' @param smooth.window Numeric. Window size (in seconds) for moving average smoothing of frequency estimates.
#' Set to 0 to disable smoothing. Default is 10 seconds.
#' @param ridge.only Logical. If TRUE (default), only returns frequencies identified as ridges (connected local maxima)
#' in the wavelet power spectrum. If FALSE, uses all local maxima in the power spectrum.
#' @param power.ratio.threshold Numeric or NULL. Power ratio threshold for frequency detection quality control.
#' This parameter controls the minimum peak-to-average power ratio required to accept a frequency estimate.
#' When \code{ridge.only = TRUE}: Applied during ridge detection as a minimum power threshold,
#' replacing the adaptive \code{scale.factor} from \code{WaveletComp::ridge()} with a fixed threshold approach.
#' When \code{ridge.only = FALSE}: Applied as a post-processing filter on the peak-to-average power ratios
#' of detected frequencies. Higher values increase selectivity (fewer, stronger signals), lower values
#' increase sensitivity (more, potentially weaker signals). Set to NULL to disable filtering.
#' Recommended range: 1.5-3 for typical data. Default is \code{2}.
#' @param max.interp.gap Numeric. Maximum gap (in seconds) for linear interpolation of missing frequency values.
#' Set to NULL to disable interpolation. Default is 10 seconds.
#' @param n.cores Number of CPU cores for parallel processing (default = 1)
#' @param plot.wavelet Logical. Whether to generate wavelet power spectrum plots. Default is TRUE.
#' @param plot.diagnostic Logical. Whether to generate diagnostic time series plots showing tail beat
#'        frequencies and power ratios. Default is FALSE.
#' @param plot.output.dir Character. If provided, all plots will be saved as PNG files in this directory instead of being displayed interactively.
#' Default is NULL (plots shown in R graphics device).
#' @param png.width Numeric. Width of PNG output in inches (default = 10).
#' @param png.height Numeric. Height of PNG output in inches (default = 7).
#' @param png.res Numeric. Resolution of PNG output in ppi (default = 300).
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
#'
#' @return Depending on parameters:
#' \itemize{
#'   \item If \code{return.data = TRUE} (default): Returns the input data with added columns \code{tbf_hz}
#'         containing estimated tail beat frequencies in Hz, and \code{power_ratio}.
#'         The \code{power_ratio} represents the ratio of the peak wavelet power at the estimated
#'         \code{tbf_hz} to the average wavelet power across the relevant frequency spectrum,
#'         providing a measure of the prominence of the tail beat frequency.
#'         For list/file inputs, returns a list of modified data frames with results for each individual.
#'   \item If \code{return.data = FALSE}: Returns \code{NULL} invisibly. Processed data will only be saved to
#'         files if \code{save.files = TRUE}.
#' }
#'
#' @details
#' The function implements a comprehensive workflow for tail beat frequency estimation:
#'
#' \strong{Bandpass Filtering (New):}
#' \itemize{
#'   \item \code{bandpass.filter = TRUE}: Applies a Butterworth bandpass filter to enhance signal quality
#'         by removing low-frequency drift and high-frequency noise outside the tail beat range.
#'   \item Filter cutoffs are automatically set based on frequency of interest with safety margins,
#'         or can be manually specified via \code{filter.low.freq} and \code{filter.high.freq}.
#'   \item Recommended for all analyses as it significantly improves signal-to-noise ratio.
#' }
#'
#' \strong{Frequency Detection Methods:}
#' \itemize{
#'   \item \code{ridge.only = TRUE}: Conservative approach using only connected maxima (ridges)
#'         in the wavelet power spectrum, identified via \code{WaveletComp::ridge()}. This method is
#'         the most robust against noise but may miss weaker signals.
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
#' The function requires the 'WaveletComp' package for the wavelet transform and the 'signal' package
#' for bandpass filtering. For parallel processing, it requires the 'foreach', 'doSNOW', and 'parallel' packages.
#'
#' \strong{Sampling Frequency Requirements:}
#' The input data must have a sampling frequency (1/timestep) that satisfies the Nyquist criterion:
#' \itemize{
#'   \item Minimum requirement: Sampling frequency > 2 x \code{max.freq.Hz}
#'   \item Recommended: Sampling frequency ≥ 4 x \code{max.freq.Hz} for reliable results
#'   \item Example: With \code{max.freq.Hz = 3}, data should be sampled at ≥ 12 Hz (minimum 6 Hz)
#' }
#'
#' @export

calculateTailBeats <- function(data,
                               id.col = "ID",
                               datetime.col = "datetime",
                               motion.col = "sway",
                               min.freq.Hz = 0.1,
                               max.freq.Hz = 3,
                               bandpass.filter = TRUE,
                               filter.low.freq = NULL,
                               filter.high.freq = NULL,
                               filter.order = 4,
                               smooth.window = 10,
                               max.rows.per.batch = 2e5,
                               ridge.only = TRUE,
                               power.ratio.threshold = 2,
                               max.interp.gap = 10,
                               plot.wavelet = TRUE,
                               plot.diagnostic = FALSE,
                               plot.output.dir = NULL,
                               png.width = 24,
                               png.height = 6,
                               png.res = 600,
                               return.data = TRUE,
                               save.files = FALSE,
                               output.folder = NULL,
                               output.suffix = NULL,
                               n.cores = 1) {

  ##############################################################################
  # Initial checks and setup ###################################################
  ##############################################################################

  # start the timer
  start.time <- Sys.time()

  # check if the 'WaveletComp' package is installed.
  if(!requireNamespace("WaveletComp", quietly=TRUE)) stop("The 'WaveletComp' package is required but is not installed. Please install 'WaveletComp' using install.packages('WaveletComp') and try again.", call. = FALSE)

  # check if the 'signal' package is installed (for bandpass filtering).
  if(bandpass.filter && !requireNamespace("signal", quietly=TRUE)) stop("The 'signal' package is required for bandpass filtering but is not installed. Please install 'signal' using install.packages('signal') and try again.", call. = FALSE)

  # validate column specifications
  if (!is.character(id.col) || length(id.col) != 1) stop("'id.col' must be a single character value", call. = FALSE)
  if (!is.character(datetime.col) || length(datetime.col) != 1) stop("'datetime.col' must be a single character value", call. = FALSE)
  if (!is.character(motion.col) || length(motion.col) != 1) stop("'motion.col' must be a single character value", call. = FALSE)

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
      stop(paste0("Input data must contain the specified id.col (", id.col, ") when not provided as a list."), call. = FALSE)
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
  required_cols <- c(id.col, datetime.col, motion.col)

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
      if (!inherits(dataset[[datetime.col]], "POSIXct")) stop("The datetime column must be of class 'Date' or 'POSIXct'.", call. = FALSE)
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

  # validate frequency parameters
  if (!is.numeric(min.freq.Hz) || length(min.freq.Hz) != 1 || min.freq.Hz <= 0) stop("'min.freq.Hz' must be a single positive numeric value", call. = FALSE)
  if (!is.numeric(max.freq.Hz) || length(max.freq.Hz) != 1 || max.freq.Hz <= 0) stop("'max.freq.Hz' must be a single positive numeric value", call. = FALSE)
  if (min.freq.Hz >= max.freq.Hz) stop("'min.freq.Hz' must be less than 'max.freq.Hz'", call. = FALSE)

  # warn if frequency range seems unusual for tail beats
  if (max.freq.Hz > 5) warning(paste("Specified max.freq.Hz of", max.freq.Hz, "Hz seems unusually high for tail beat frequencies. Typical fish tail beats range between 0.1-3 Hz."))
  if (min.freq.Hz < 0.01) warning(paste("Specified min.freq.Hz of", min.freq.Hz, "Hz seems unusually low for tail beat frequencies. This may detect non-tail-beat movements."))

  # validate bandpass filter parameters
  if (!is.logical(bandpass.filter) || length(bandpass.filter) != 1) stop("'bandpass.filter' must be a single logical value (TRUE/FALSE)", call. = FALSE)

  if (bandpass.filter) {
    # set default filter frequencies if not provided
    if (is.null(filter.low.freq)) filter.low.freq <- min.freq.Hz * 0.9
    if (is.null(filter.high.freq)) filter.high.freq <- max.freq.Hz * 1.1

    # validate filter frequencies
    if (!is.numeric(filter.low.freq) || length(filter.low.freq) != 1 || filter.low.freq <= 0) {
      stop("'filter.low.freq' must be a single positive numeric value", call. = FALSE)
    }
    if (!is.numeric(filter.high.freq) || length(filter.high.freq) != 1 || filter.high.freq <= 0) {
      stop("'filter.high.freq' must be a single positive numeric value", call. = FALSE)
    }
    if (filter.low.freq >= filter.high.freq) {
      stop("'filter.low.freq' must be less than 'filter.high.freq'", call. = FALSE)
    }

    # validate filter order
    if (!is.numeric(filter.order) || length(filter.order) != 1 || filter.order <= 0 || filter.order != round(filter.order)) {
      stop("'filter.order' must be a single positive integer", call. = FALSE)
    }

    # warn about filter settings
    if (filter.low.freq > min.freq.Hz) {
      warning(paste("Filter low cutoff (", filter.low.freq, "Hz) is higher than min.freq.Hz (", min.freq.Hz, "Hz). This may remove frequencies of interest."))
    }
    if (filter.high.freq < max.freq.Hz) {
      warning(paste("Filter high cutoff (", filter.high.freq, "Hz) is lower than max.freq.Hz (", max.freq.Hz, "Hz). This may remove frequencies of interest."))
    }
  }

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


  # validate plot parameters
  if (!is.logical(plot.wavelet) || length(plot.wavelet) != 1) {
    stop("'plot.wavelet' must be a single logical value", call. = FALSE)
  }
  if (!is.logical(plot.diagnostic) || length(plot.diagnostic) != 1) {
    stop("'plot.diagnostic' must be a single logical value", call. = FALSE)
  }

  # check if plot output directory exists
  if (!is.null(plot.output.dir)) {
    if (!dir.exists(plot.output.dir)) {
      stop("The specified plot output directory does not exist. Please provide a valid folder path.", call. = FALSE)
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

  ##############################################################################
  # Initialize variables #######################################################
  ##############################################################################

  # calculate number of animals
  n_animals <- length(data)

  cat(paste0(
    crayon::bold("\n==================== Estimating Tail Beats ====================\n"),
    "Using ", motion.col, " data to estimate tail beat frequencies for ", n_animals,
    " ", ifelse(n_animals == 1, "tag", "tags"), "\n",
    ifelse(bandpass.filter,
           paste0("Bandpass filtering: ", filter.low.freq, " - ", filter.high.freq, " Hz (order ", filter.order, ")\n"),
           "No bandpass filtering applied\n"),
    crayon::bold("===============================================================\n\n")
  ))

  # initialize list to hold results
  data_list <- vector("list", n_animals)


  ##############################################################################
  # Validate Sampling Frequency for Frequency Estimation #######################
  ##############################################################################

  # check if sampling frequency is sufficient for the requested max frequency
  # we want at least 4 samples per cycle for reliable frequency estimation
  # (Nyquist would be 2, but we're conservative)

  # print verbose
  cat("Validating sampling frequencies...\n")

  # retrieve data sampling frequencies
  data_hz <- unlist(lapply(data, function(dt) {

    # if data is file paths, we need to load the data first
    if (is_filepaths) dt <- readRDS(dt)

    if ("processed.sampling.frequency" %in% names(attributes(dt))) {
      attributes(dt)$processed.sampling.frequency
    } else {
      # calculate from timestamps if not available in attributes
      time_diff <- as.numeric(difftime(max(dt[[datetime.col]]), min(dt[[datetime.col]]),  units = "secs"))
      if (time_diff == 0) {
        warning("Cannot calculate sampling frequency - timestamps may be identical",  call. = FALSE)
        return(NA_real_)
      }
      sf <- nrow(dt) / time_diff
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
    offender_names <- if (!is.null(names(data))) names(data)[offenders] else {
      if (is_filepaths) basename(data[offenders]) else paste("Dataset", offenders)
    }
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
    marginal_names <- if (!is.null(names(data))) names(data)[marginal_hz] else {
      if (is_filepaths) basename(data[marginal_hz]) else paste("Dataset", which(marginal_hz))
    }
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

  # print message if n.cores > 1
  if(n.cores > 1) cat(paste0("Starting parallel computation: ", n.cores, " cores\n"))
  cat("\n")

  # process each individual (sequentially)
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

      ############################################################################
      # data is already in memory (list of data frames/tables) ###################
    } else {
      # access the individual dataset
      individual_data <- data[[i]]
    }

    # get ID
    id <- unique(individual_data[[id.col]])[1]

    # print current ID
    cat(crayon::bold(sprintf("[%d/%d] %s\n", i, n_animals, id)))

    # skip if no valid motion data
    if (all(is.na(individual_data[[motion.col]]))) {
      message("No valid motion data - skipping processing")
      if (inherits(individual_data, "data.table")) {
        individual_data[, tbf_hz := NA_real_]
      } else {
        individual_data$tbf_hz <- NA_real_
      }
      data_list[[i]] <- individual_data
      names(data_list)[i] <- id
      next
    }

    # process the individual with parallel batch processing
    data_list[[i]] <- .runCWT(
      dt = individual_data,
      animal_id = id,
      id.col = id.col,
      datetime.col = datetime.col,
      motion.col = motion.col,
      min.freq.Hz = min.freq.Hz,
      max.freq.Hz = max.freq.Hz,
      bandpass.filter = bandpass.filter,
      filter.low.freq = filter.low.freq,
      filter.high.freq = filter.high.freq,
      filter.order = filter.order,
      max.rows.per.batch = max.rows.per.batch,
      ridge.only = ridge.only,
      power.ratio.threshold = power.ratio.threshold,
      smooth.window = smooth.window,
      max.interp.gap = max.interp.gap,
      plot.wavelet = plot.wavelet,
      plot.diagnostic = plot.diagnostic,
      plot.filtering = plot.filtering,
      plot.output.dir = plot.output.dir,
      png.width = png.width,
      png.height = png.height,
      png.res = png.res,
      return.data = return.data,
      save.files = save.files,
      output.folder = output.folder,
      output.suffix = output.suffix,
      n.cores = n.cores
    )

    # add space between individuals
    cat("\n")
  }


  ############################################################################
  # Return processed data ####################################################
  ############################################################################

  # print time taken
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  cat(crayon::bold("Total execution time:"), sprintf("%.02f", as.numeric(time.taken)), base::units(time.taken), "\n\n")


  # return imported data or NULL based on return.data parameter
  if (return.data) {
    # return the list containing processed data with tail beat metrics
    names(data_list) <- sapply(data_list, function(x) unique(x[[id.col]])[1])
    return(data_list)
  } else {
    return(invisible(NULL))
  }

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
#' @noRd

.runCWT <- function(dt, animal_id, id.col, datetime.col, motion.col,
                    min.freq.Hz, max.freq.Hz, bandpass.filter, filter.low.freq,
                    filter.high.freq, filter.order, max.rows.per.batch, ridge.only,
                    power.ratio.threshold, smooth.window, max.interp.gap,
                    plot.wavelet, plot.diagnostic, plot.filtering, plot.output.dir,
                    png.width, png.height, png.res, return.data,
                    save.files, output.folder, output.suffix, n.cores = 1) {

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
  power_ratios <- numeric(n_rows_valid)
  na_full_mask <- rep(FALSE, n_rows_valid)

  ##############################################################################
  # Apply Bandpass Filter ######################################################
  ##############################################################################

  if (bandpass.filter) {
    cat("Applying bandpass filter...")

    # check if we have enough data points for filtering
    min_samples_needed <- filter.order * 6  # Rule of thumb: 6x filter order
    if (nrow(valid_data) < min_samples_needed) {
      warning(paste("Insufficient data points for filtering (", nrow(valid_data),
                    " < ", min_samples_needed, "). Skipping filter for ID:", animal_id), call. = FALSE)
      filtered_motion <- original_motion
    } else {
      # normalize frequencies to Nyquist frequency (sampling_freq/2)
      nyquist_freq <- sampling_freq / 2
      low_norm <- filter.low.freq / nyquist_freq
      high_norm <- filter.high.freq / nyquist_freq

      # check if normalized frequencies are valid
      if (low_norm <= 0 || high_norm >= 1 || low_norm >= high_norm) {
        warning(paste("Invalid filter frequencies for sampling rate. Skipping filter for ID:", animal_id), call. = FALSE)
        filtered_motion <- original_motion
      } else {
        # handle NAs by interpolating before filtering
        motion_for_filtering <- valid_data$motion
        if (any(is.na(motion_for_filtering))) {
          # simple linear interpolation for filtering
          motion_for_filtering <- zoo::na.approx(motion_for_filtering, na.rm = FALSE)
          # fill any remaining NAs at edges
          motion_for_filtering <- zoo::na.locf(motion_for_filtering, na.rm = FALSE)
          motion_for_filtering <- zoo::na.locf(motion_for_filtering, fromLast = TRUE, na.rm = FALSE)
        }

        # design and apply Butterworth bandpass filter
        tryCatch({
          # design filter
          filter_design <- signal::butter(n = filter.order,
                                          W = c(low_norm, high_norm),
                                          type = "pass")

          # apply filter (forward and backward to eliminate phase shift)
          filtered_motion <- signal::filtfilt(filter_design, motion_for_filtering)

          # restore original NAs where they existed
          filtered_motion[is.na(valid_data$motion)] <- NA_real_

          cat(" done\n")

        }, error = function(e) {
          warning(paste("Filter application failed for ID:", animal_id, ". Error:", e$message,
                        "Using unfiltered data."), call. = FALSE)
          filtered_motion <- original_motion
        })
      }
    }

    # update motion data with filtered version
    valid_data$motion <- filtered_motion
  }


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
        ridge.threshold = ridge.threshold,
        power.ratio.threshold = power.ratio.threshold,
        max.interp.gap = max.interp.gap,
        animal_id = animal_id
      )

      # update frequency results
      dominant_freqs[batch_result$core_start:batch_result$core_end] <- batch_result$batch_dominant_freqs

      # update power ratios
      power_ratios[batch_result$core_start:batch_result$core_end] <- batch_result$batch_power_ratios

      # update NA mask
      if (!is.null(batch_result$na_mask_plotting)) {
        na_full_mask[batch_result$core_start:batch_result$core_end] <- batch_result$na_mask_plotting
      }

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

      # update the na_full_mask for the aggregated plot
      na_full_mask[batch_result$core_start:batch_result$core_end] <- batch_result$na_mask_plotting

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
          ridge.threshold = ridge.threshold,
          power.ratio.threshold = power.ratio.threshold,
          max.interp.gap = max.interp.gap,
          animal_id = animal_id
        )
      }

      # close progress bar
      close(pb)

      # combine results from parallel processing
      for (result in batch_results) {
        # update frequency results
        dominant_freqs[result$core_start:result$core_end] <- result$batch_dominant_freqs
        # update power ratios
        power_ratios[result$core_start:result$core_end] <- result$batch_power_ratios # Use the new result from .processBatch
        # update NA mask
        if (!is.null(result$na_mask_plotting)) {
          na_full_mask[result$core_start:result$core_end] <- result$na_mask_plotting
        }
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

  # create full-length vector with NAs where no data for TBF
  tbf <- rep(NA_real_, n_rows)
  tbf[first_valid:last_valid] <- dominant_freqs
  # add tbf_hz to original data
  dt$tbf_hz <- tbf

  # create full-length vector with NAs where no data for power ratio
  power_ratio_full <- rep(NA_real_, n_rows)
  power_ratio_full[first_valid:last_valid] <- power_ratios
  # add power_ratio_hz to original data
  dt$tbf_power_ratio <- power_ratio_full


  #####################################################################
  # Generate wavelet plot (if requested) ##############################
  #####################################################################

  if (plot.wavelet) {

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
      # prepare COI vectors with NA values too
      full_coi1 <- rep(NA_real_, length(full_axis1))
      full_coi2 <- rep(NA_real_, length(full_axis1))
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
        full_coi1[matched_indices] <- combined_result$coi.1[valid_matches]
        full_coi2[matched_indices] <- combined_result$coi.2[valid_matches]
      }
      # update the result object
      combined_result$axis.1 <- full_axis1
      combined_result$Power <- full_power
      combined_result$coi.1 <- full_coi1
      combined_result$coi.2 <- full_coi2
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
      na_full_mask <- na_full_mask[keep]
      # ensure the axis is perfectly regular (optional, forces exact regularity)
      combined_result$axis.1 <- regular_grid
    }

    # apply the na_full_mask to the combined_power (set power to NA where there were too-long gaps)
    combined_result$Power[, na_full_mask] <- NA_real_


    # if output directory is specified, save to PNG
    if (!is.null(plot.output.dir)) {
      # Create clean filename from animal ID
      png_file <- file.path(plot.output.dir, paste0(animal_id, "-wavelet.png"))
      png(png_file, width = png.width, height = png.height, units = "in", res = png.res)
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
    valid_coi <- which(!is.na(combined_result$coi.1) & !is.na(combined_result$coi.2))
    if (length(valid_coi) > 0) {
      polygon(
        x = c(combined_result$coi.1[valid_coi], rev(combined_result$coi.1[valid_coi])),
        y = c(combined_result$coi.2[valid_coi], rep(max(combined_result$coi.2[valid_coi], na.rm = TRUE), length(valid_coi))),
        border = NA,
        col = adjustcolor("black", alpha.f = 0.6)
      )
    }

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

    # close png
    if (!is.null(plot.output.dir)) dev.off()

  }



  #####################################################################
  # Generate diagnostic plot (if requested) ###########################
  #####################################################################

  if (plot.diagnostic) {

    # prepare data
    plot_dates <- dt[[datetime.col]]
    plot_freqs <- dt$tbf_hz
    plot_ratios <- dt$tbf_power_ratio

    # define color palette for power ratios
    power_ratio_color_pal <- .jet_pal(100)

    # scale power_ratios to indices for the color palette (1 to 100)
    min_ratio <- min(plot_ratios, na.rm = TRUE)
    max_ratio <- max(plot_ratios, na.rm = TRUE)
    if (max_ratio == min_ratio) {
      color_indices <- rep(50, length(plot_ratios)) # Use a middle color
    } else {
      color_indices <- ceiling(((plot_ratios - min_ratio) / (max_ratio - min_ratio)) * 99) + 1
    }
    color_indices[color_indices < 1] <- 1
    color_indices[color_indices > 100] <- 100
    segment_colors <- power_ratio_color_pal[color_indices]

    # set up PNG output
    if (!is.null(plot.output.dir)) {
      png_file <- file.path(plot.output.dir, paste0(animal_id, "-diagnostic.png"))
      png(png_file, width = png.width, height = png.height, units = "in", res = png.res)
    }

    # save current par settings
    op <- par(no.readonly = TRUE)

    # set up plot layout
    layout(matrix(c(1, 2), nrow = 2, byrow = TRUE), heights = c(2.5, 1.8))

    ######################################################
    # plot top panel - estimated tail beat frequency
    par(mar = c(1, 5.5, 2.5, 11) + 0.1)

    # first, set up an empty plot with the correct axes ranges
    plot(plot_dates, plot_freqs, type = "n", xlab = "",  ylab = "",  xaxs = "i", axes = FALSE, main = "")

    # add background color
    rect(par("usr")[1], par("usr")[3], par("usr")[2], par("usr")[4], col="grey97", border=NA)

    # draw the color-coded segments for TBF
    for (i in 1:(length(plot_dates) - 1)) {
      segments(x0 = plot_dates[i], y0 = plot_freqs[i], x1 = plot_dates[i+1], y1 = plot_freqs[i+1],
               col = segment_colors[i], lwd = 2)
    }

    # add title
    title(main = paste("Tail Beat Frequency -", animal_id, " [", motion.col, "]"), font.main = 2, line = 1, cex.main = 1.4)

    # y-axis for tail beat frequency
    axis(2, cex.axis = 1.1, las = 1)
    mtext("Tail Beat Frequency (Hz)", side = 2, line = 3,  cex = 1.2)

    # draw box
    box()

    # add color legend
    ratio_labs <- pretty(plot_ratios)
    ratio_labs <- ratio_labs[ratio_labs>=min(plot_ratios, na.rm = TRUE) & ratio_labs<=max(plot_ratios, na.rm = TRUE)]
    .colorlegend(col=power_ratio_color_pal, zlim=range(plot_ratios, na.rm = TRUE), zval=ratio_labs,
                 posx=c(0.93, 0.94), posy = c(0, 0.85), main = "Power Ratio",
                 main.cex=1.1, digit=1,  cex=1)


    ######################################################
    # plot bottom panel - power ratio time series
    par(mar = c(4.5, 5.5, 0, 11) + 0.1)

    # first, set up an empty plot with the correct axes ranges
    plot(plot_dates, plot_ratios, type = "n", xlab = "",  ylab = "",  xaxs = "i", axes = FALSE, main = "")

    # add background color
    rect(par("usr")[1], par("usr")[3], par("usr")[2], par("usr")[4], col="grey97", border=NA)

    # draw the color-coded segments for power ratio
    for (i in 1:(length(plot_dates) - 1)) {
      segments(x0 = plot_dates[i], y0 = plot_ratios[i], x1 = plot_dates[i+1], y1 = plot_ratios[i+1], col = segment_colors[i], lwd = 1.5)
    }

    # y-axis for power ratio
    axis(2, cex.axis = 1.1, las = 1)
    abline(h=pretty(plot_ratios), lwd=0.5, lty=2, col="grey20")
    if(!is.null(power.ratio.threshold)) abline(h=power.ratio.threshold, lwd=1, lty=2, col="red")
    mtext("Power Ratio\n(max/mean)", side = 2, line = 3, cex = 1.2)

    # draw box
    box()

    # x-axis (shared for both plots, only drawn on bottom plot)
    date_labels <- pretty(plot_dates, n = 9)
    date_positions <- as.numeric(date_labels)
    axis(1, at = date_positions, labels = strftime(date_labels, "%d/%b %H:%M", tz = "UTC"),
         las = 1, cex.axis = 1.1)
    mtext("Date", side = 1, line = 2.8, cex = 1.2)

    # add call info as legend
    legend_text <- c(
      sprintf("Min Freq: %.2f Hz", min.freq.Hz),
      sprintf("Max Freq: %.2f Hz", max.freq.Hz),
      sprintf("Ridge Only: %s", ridge.only),
      sprintf("Power Ratio Thresh: %s", ifelse(is.null(power.ratio.threshold), "NULL", formatC(power.ratio.threshold, digits = 1, format = "f"))),
      sprintf("Smooth Window: %.1f s", smooth.window),
      sprintf("Max Interp Gap: %s", ifelse(is.null(max.interp.gap), "NULL", sprintf("%.1f s", max.interp.gap)))
    )

    # add the legend to the plot
    leg_info <- legend("bottomright", legend = legend_text, cex = 0.9,  bty = "n", xpd = TRUE, inset = c(-0.095, 0), y.intersp = 1.1)
    title_y_pos <- leg_info$rect$top + (par("cex") * 0.7 * strheight("M"))
    offset_x <- strwidth("M", cex = 0.9) * 1
    text(x = leg_info$rect$left + offset_x, y = title_y_pos, labels = "Parameters", adj = c(0, 0.5), cex = 1.1, xpd = NA)

    # close png
    if (!is.null(plot.output.dir)) dev.off()

    # reset layout and par settings
    layout(1)
    par(op)
  }


  #####################################################################
  # Return results ####################################################
  #####################################################################

  # save the processed data as an RDS file
  if(save.files){

    # print without newline and immediately flush output
    cat("Saving file... ")
    flush.console()

    # determine the output directory: use the specified output folder, or if not provided,
    # use the folder of the current file (if data[i] is a file path), or default to "./"
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
    output_file <- file.path(output_dir, paste0(animal_id, suffix, ".rds"))

    # save the processed data
    saveRDS(dt, output_file)

    # overwrite the line with completion message
    cat("\r")
    cat(rep(" ", getOption("width")-1))
    cat("\r")
    cat(sprintf("\u2713 Saved: %s\n", basename(output_file)))

  }

  # last garbage collection
  gc()

  # return result if needed
  if (return.data) {
    return(dt)
  }else{
    return(invisible(NULL))
  }


}


################################################################################
# Helper function to process a single batch ####################################
################################################################################

#' Helper function to process a single batch
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.processBatch <- function(batch, valid_data, batch_size, buffer_samples, n_rows_valid,
                          sampling_freq, min.freq.Hz, max.freq.Hz, ridge.only, ridge.threshold,
                          power.ratio.threshold, max.interp.gap, animal_id) {

  # initialize empty result list
  result <- list(
    batch_dominant_freqs = NULL,
    core_start = NULL,
    core_end = NULL,
    current_exclusions = 0,
    wavelet_result = NULL,  # This will store the full wavelet result for plotting
    batch_power_ratios = NULL,
    na_mask_plotting = NULL
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

  # mask of all original NAs in this batch_data
  original_motion_is_na <- is.na(batch_data$motion)
  too_long_na_mask <- rep(FALSE, length(original_motion_is_na))
  if (any(original_motion_is_na)) {
    # find contiguous NA blocks
    na_rle <- rle(original_motion_is_na)
    current_idx_in_batch <- 0
    for (i in seq_along(na_rle$lengths)) {
      segment_length <- na_rle$lengths[i]
      if (na_rle$values[i]) {
        gap_duration_seconds <- segment_length / sampling_freq
        if (gap_duration_seconds > max.interp.gap) {
          # mark this entire NA segment as "too long"
          too_long_na_mask[(current_idx_in_batch + 1):(current_idx_in_batch + segment_length)] <- TRUE
        }
      }
      current_idx_in_batch <- current_idx_in_batch + segment_length
    }

    # imputation for WaveletComp input - fill aLL NAs
    if (sum(!original_motion_is_na) >= 2) {
      batch_data$motion <- zoo::na.approx(batch_data$motion, na.rm = FALSE)
    }
    # fill any remaining NAs (especially at start/end of the batch_data) with LOCF/NOCB
    batch_data$motion <- zoo::na.locf(batch_data$motion, na.rm = FALSE)
    batch_data$motion <- zoo::na.locf(batch_data$motion, fromLast = TRUE, na.rm = FALSE)

    # if, after all imputation, the data is still all NAs (e.g., original batch was entirely NA),
    # then treat it as an unprocessable batch and return NAs.
    if (all(is.na(batch_data$motion))) {
      warning(sprintf("%s - Batch %d contains only NA motion data after imputation (wavelet skipped)", animal_id, batch), call. = FALSE)
      result$batch_dominant_freqs <- rep(NA_real_, result$core_end - result$core_start + 1)
      result$dates <- valid_data$date[batch_start:batch_end][central_cols]
      result$batch_power_ratios <- rep(NA_real_, result$core_end - result$core_start + 1)
      result$na_mask_plotting <- too_long_na_mask[central_cols]
      return(result)
    }
  }

  # skip constant batches
  if (length(unique(na.omit(batch_data$motion))) <= 1) {
    warning(sprintf("%s - Constant motion data detected in batch %d (wavelet skipped)", animal_id, batch), call. = FALSE)
    result$batch_dominant_freqs <- rep(NA_real_, result$core_end - result$core_start + 1)
    result$dates <- valid_data$date[batch_start:batch_end][central_cols]
    result$batch_power_ratios <- rep(NA_real_, result$core_end - result$core_start + 1)
    result$na_mask_plotting <- too_long_na_mask[central_cols]
    return(result)
  }

  # additional check for effectively constant data (handles floating-point precision issues)
  motion_range <- range(batch_data$motion, na.rm = TRUE)
  motion_span <- diff(motion_range)
  mean_abs_motion <- mean(abs(batch_data$motion), na.rm = TRUE)

  # if the range is extremely small relative to the mean absolute value, treat as constant
  # this catches cases where bandpass filtering produces tiny floating-point variations
  relative_variation <- if (mean_abs_motion > 0) motion_span / mean_abs_motion else motion_span

  if (relative_variation < 1e-10 || motion_span < 1e-15) {
    warning(sprintf("%s - Effectively constant motion data detected in batch %d after filtering (wavelet skipped)", animal_id, batch), call. = FALSE)
    result$batch_dominant_freqs <- rep(NA_real_, result$core_end - result$core_start + 1)
    result$dates <- valid_data$date[batch_start:batch_end][central_cols]
    result$batch_power_ratios <- rep(NA_real_, result$core_end - result$core_start + 1)
    result$na_mask_plotting <- too_long_na_mask[central_cols]
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

  # calculate power_ratio for the core part of the batch
  batch_power_ratio_values <- apply(result$wavelet_result$Power[, central_cols, drop = FALSE], 2, function(x) max(x) / mean(x))
  result$batch_power_ratios <- batch_power_ratio_values

  # determine dominant tail-beat frequencies
  if (ridge.only) {
    # strict ridge-based approach (connected maxima in wavelet power spectrum)
    ridges <- .ridge2(result$wavelet_result$Power[, central_cols, drop = FALSE], power.threshold = power.ratio.threshold)
    ridge_indices <- which(colSums(ridges) > 0)
    max_periods <- result$wavelet_result$Period[apply(result$wavelet_result$Power[, central_cols, drop = FALSE], 2, which.max)]
    dominant_periods <- rep(NA_real_, ncol(ridges))
    dominant_periods[ridge_indices] <- max_periods[ridge_indices]
  } else if (!is.null(power.ratio.threshold)) {
    # lenient dominant frequency approach with power ratio filtering
    dominant_periods <- ifelse(batch_power_ratio_values > power.ratio.threshold,
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

  # set results to NA where original gaps were identified as "too long"
  mask_for_results_central_cols <- too_long_na_mask[central_cols]
  result$batch_dominant_freqs[mask_for_results_central_cols] <- NA_real_
  result$batch_power_ratios[mask_for_results_central_cols] <- NA_real_
  result$na_mask_plotting <- mask_for_results_central_cols

  # add plotting components to result (reflecting potentially masked output)
  result$wavelet_power <- result$wavelet_result$Power[, central_cols, drop = FALSE]
  result$periods <- result$wavelet_result$Period
  result$axis1 <- result$wavelet_result$axis.1[central_cols] + (batch_start - 1)/sampling_freq
  result$coi1 <- result$wavelet_result$coi.1[central_cols]
  result$coi2 <- result$wavelet_result$coi.2[central_cols]
  result$dates <- valid_data$date[batch_start:batch_end][central_cols]

  # return
  return(result)
}


################################################################################
# Adapted Ridge Function #######################################################
################################################################################

#' Extract Ridge Lines from a Wavelet Power Spectrum
#'
#' This internal helper function identifies ridge lines in a wavelet power spectrum matrix.
#' It detects local maxima within a specified band around each scale level (row) in the spectrum,
#' and applies a power threshold to filter weak ridges.
#'
#' @param wavelet.spectrum Numeric matrix. The wavelet power spectrum with rows representing scale levels and columns time points.
#' @param power.threshold Numeric scalar, default 2. Minimum power threshold to consider a point part of a ridge.
#' @param band Integer scalar, default 5. Number of adjacent scale levels to check on each side to find local maxima.
#'
#' @return A numeric matrix of the same dimensions as \code{wavelet.spectrum}, with values 1 where ridge points are detected, and 0 elsewhere.
#'
#' @details
#' The function scans each column of the wavelet power spectrum and marks as ridge points those that are local maxima
#' within the \code{band} neighbourhood along the scale axis and exceed the \code{power.threshold}.
#' This is useful for identifying dominant periodicities or frequencies over time.
#'
#' @note
#' Adapted by Bruno Saraiva from the \code{ridge} function in the \pkg{waveletComp} package.
#' @noRd

.ridge2 <- function(wavelet.spectrum, power.threshold = 2, band = 5){

  min.level = power.threshold

  ridge.column = function(column.vec, band = band) {
    nrows = length(column.vec)
    ind = seq(1, nrows)
    band.max.vec = column.vec
    for (i in (1:band)) {
      lower.ind = ind - i
      lower.ind[lower.ind < 1] = 1
      upper.ind = ind + i
      upper.ind[upper.ind > nrows] = nrows
      band.max.vec = pmax(band.max.vec, column.vec[lower.ind],
                          column.vec[upper.ind])
    }

    my.ridge.column = rep(0, nrows)
    my.ridge.column[pmax(band.max.vec) == column.vec] = 1
    return(my.ridge.column)

  }

  Ridge = apply(wavelet.spectrum, 2, ridge.column, band = band)
  Ridge = Ridge * (wavelet.spectrum > min.level)
  return(invisible(Ridge))
}




#######################################################################################################
#######################################################################################################
#######################################################################################################
