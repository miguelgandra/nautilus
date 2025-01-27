#######################################################################################################
# Function to extract features from a sliding window #################################################
#######################################################################################################

#' Extract Features from a Sliding Window
#'
#' This function calculates specified metrics (e.g., mean, standard deviation) for selected variables over a sliding window.
#' It can either retain the temporal resolution of the dataset or aggregate data into distinct, non-overlapping windows.
#' This is useful for preparing the dataset for machine learning or other analytical methods that require a structured set of features.
#'
#' @param data A list of data tables/data frames, one for each individual, or a single aggregated data table/data frame
#' containing data from multiple animals. Must include the variables specified in `variables`.
#' @param variables A character vector of column names from `data` for which to calculate metrics.
#' @param metrics A character vector specifying the metrics to calculate. Supported metrics: "mean", "sd", "min", "max", "sum",
#' "range", "mad", "skewness", "kurtosis", "energy", "entropy".
#' @param window.size An integer specifying the size of the sliding or aggregation window, in seconds.
#' The actual number of steps in the window is determined based on the sampling interval of the data.
#' @param aggregate Logical. If `TRUE`, aggregates data into distinct, non-overlapping windows. If `FALSE`, retains temporal resolution.
#' @param datetime.col A character string specifying the column in `data` containing datetime information.
#' @param response.col (Optional) A character string specifying the column in `data` containing response or annotated labels (e.g., feeding events).
#' It must be binary (i.e., containing values 0 or 1).
#' @param cores The number of processor cores to use for parallel computation. Defaults to 1 (single-core).

#'
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
#' - "energy": The sum of the squares of the data. It reflects the magnitude of the values, commonly used in signal processing and analysis of periodicity.
#' - "skewness": The skewness of the data, a measure of the asymmetry of the distribution. A positive value indicates a right-skewed distribution, while a negative value indicates a left-skewed distribution.
#' - "kurtosis": The kurtosis of the data, a measure of the "tailedness" or sharpness of the distribution. High kurtosis indicates heavy tails, while low kurtosis indicates lighter tails.
#' - "entropy": The entropy of the data, which measures the unpredictability or randomness. High entropy values indicate more uncertainty or randomness in the data, while low values suggest more predictability.
#'
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
                            variables,
                            metrics = c("mean", "median", "sd", "range", "min", "max", "iqr", "sum",
                                        "energy", "skewness", "kurtosis", "entropy"),
                            window.size = 5,
                            aggregate = FALSE,
                            datetime.col = "datetime",
                            response.col = NULL,
                            cores = 1){

  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

  # measure running time
  start.time <- Sys.time()

  # if 'data' is not a list, split it into a list of individual data sets based on 'id.col'
  if (!is.list(data)) {
    data <- split(data, f = data[[id.col]])
  }

  # ensure specified variables exist in the dataset
  if(any(!variables %in% colnames(data[[1]]))) {
    stop("Some specified variables are not present in the data.", call. = FALSE)
  }

  # ensure valid metrics are specified
  valid_metrics = c("mean", "median", "sd", "range", "min", "max", "iqr", "sum", "energy", "skewness", "kurtosis", "entropy")
  metrics <- tolower(metrics)
  if(any(!metrics %in% valid_metrics)) {
    stop("Some specified metrics are not supported. Supported metrics: ", paste(valid_metrics, collapse = ", "), call. = FALSE)
  }

  # check that datetime.col exists in the original dataset
  if (!datetime.col %in% colnames(data[[1]])) {
    stop("The specified datetime column does not exist in the data.", call. = FALSE)
  }

  # check that the datetime column is of class POSIXct
  if (!inherits(data[[1]][[datetime.col]], "POSIXct")) {
    stop(paste("The specified datetime column", datetime.col, "must be of class POSIXct."), call. = FALSE)
  }

  # check if response column exists and is binary
  if (!is.null(response.col)) {
    if (!response.col %in% colnames(data[[1]])) {
      stop("The specified response column is not present in the data.", call. = FALSE)
    }
    if (!all(data[[1]][[response.col]] %in% c(0, 1))) {
      stop("The response column must be binary (0 or 1).", call. = FALSE)
    }
  }

  # validate parallel computing packages
  if (cores>1){
    if(!requireNamespace("foreach", quietly=TRUE)) stop("The 'foreach' package is required for parallel computing but is not installed. Please install 'foreach' using install.packages('foreach') and try again.", call. = FALSE)
    if(!requireNamespace("doSNOW", quietly=TRUE)) stop("The 'doSNOW' package is required for parallel computing but is not installed. Please install 'doSNOW' using install.packages('doSNOW') and try again.", call. = FALSE)
    if(!requireNamespace("parallel", quietly=TRUE)){
      stop("The 'parallel' package is required for parallel computing but is not installed. Please install 'parallel' using install.packages('parallel') and try again.", call. = FALSE)
    }else if(parallel::detectCores()<cores){
      stop(paste("Please choose a different number of cores for parallel computing (only", parallel::detectCores(), "available)."), call. = FALSE)
    }
  }

  # set export packages (required for parallel computing)
  export_packages <- "zoo"
  # check if 'skewness' or 'kurtosis' are in metrics and ensure 'moments' package is installed
  if(any(metrics %in% c("skewness", "kurtosis"))){
    if(!requireNamespace("moments", quietly=TRUE)) stop("The 'moments' package is required for skewness and/or kurtosis calculations but is not installed. Please install 'moments' using install.packages('moments') and try again.", call. = FALSE)
    export_packages <- c(export_packages, "moments")
  }

  # check if 'entropy' is in metrics and ensure 'entropy' package is installed
  if(any(metrics == "entropy")){
    if(!requireNamespace("entropy", quietly=TRUE)) stop("The 'entropy' package is required for entropy calculations but is not installed. Please install 'entropy' using install.packages('entropy') and try again.", call. = FALSE)
    export_packages <- c(export_packages, "entropy")
  }


  ##############################################################################
  # Process data ###############################################################
  ##############################################################################

  # calculate number of unique datasets
  n_animals <- length(data)

  # create a grid of variable and metric combinations
  parameter_grid <- expand.grid(variable = variables, metric = metrics, stringsAsFactors = FALSE)
  parameter_grid <- parameter_grid[order(parameter_grid$variable),]

  # feedback messages for the user
  cat(paste0(
    crayon::bold("\n============= Extracting Data Features =============\n"),
    "Processing ", nrow(parameter_grid), " features across ", n_animals, ifelse(n_animals == 1, " dataset", " datasets"),
    " - buckle up!\n",
    crayon::bold("====================================================\n\n")))


  # initialize parallel backend
  if(cores > 1){
    # print message
    cat(paste0("Starting parallel computation: ", cores, " cores\n\n"))
    # register parallel backend with the specified number of cores
    cl <- parallel::makeCluster(cores)
    doSNOW::registerDoSNOW(cl)
    # ensure the cluster is properly stopped when the function exits
    on.exit(parallel::stopCluster(cl))
    # define the `%dopar%` operator locally for parallel execution
    `%dopar%` <- foreach::`%dopar%`
  }

  # initialize list to hold features
  data_processed <- vector("list", length(data))

  # iterate over each element in 'data'
  for (i in 1:length(data)) {

    # retrieve data for current ID
    data_individual <- data[[i]]

    # store original attributes before processing,  excluding internal ones
    discard_attrs <- c("row.names", "class", ".internal.selfref", "names")
    original_attributes <- attributes(data_individual)
    original_attributes <- original_attributes[!names(original_attributes) %in% discard_attrs]

    # retrieve animal ID
    id <- original_attributes$id
    names(data_processed)[i] <- id

    # print animal ID to the console
    cat(crayon::blue$bold(id), "\n")

    # ensure variables are numeric
    for (var in variables) {
      if (!is.numeric(data_individual[[var]])) {
        warning(paste("Variable", var, "is not numeric. Converting to numeric."), call. = FALSE)
        data_individual[[var]] <- as.numeric(as.character(data_individual[[var]]))
      }
    }

    # check if a response column is specified
    if (!is.null(response.col)) {
      # if the response column does not exist in the data frame, create it
      if (!(response.col %in% colnames(data_individual))) {
        data_individual[[response.col]] <- 0
      }
      # if the response column is a factor, convert it to numeric
      if (is.factor(data_individual[[response.col]])) {
        data_individual[[response.col]] <- as.numeric(as.factor(data_individual[[response.col]])) - 1
      }
    }

    # calculate sampling frequency
    sampling_freq <- original_attributes$processed.sampling.frequency

    # convert the window size from seconds to steps
    window_interval <- window.size * sampling_freq

    # initialize an empty list to store features
    feature_list <- list()


    ###############################################################
    # sequential processing #######################################
    if(cores == 1){

      # initialize progress bar
      pb <- txtProgressBar(min=0, max=nrow(parameter_grid), initial=0, style=3)

      # loop through each
      for(i in 1:nrow(parameter_grid)){
        var <- parameter_grid$variable[i]
        metric <- parameter_grid$metric[i]
        feature_list[[i]] <- .calculateMetric(data_individual, var, metric, window_interval, aggregate)

        # update progress bar
        setTxtProgressBar(pb, i)
      }
    }

    ###############################################################
    # parallel processing   #######################################
    if(cores > 1){

      # initialize progress bar
      pb <- txtProgressBar(min=0, max=nrow(parameter_grid), initial=0, style=3)

      # set progress bar options
      opts <- list(progress = function(n) setTxtProgressBar(pb, n))

      # perform parallel computation
      feature_list <- foreach::foreach(
        i = 1:nrow(parameter_grid),
        .options.snow = opts,
        .packages = export_packages,
        .export = c(".calculateMetric")
      ) %dopar% {
        var <- parameter_grid$variable[i]
        metric <- parameter_grid$metric[i]
        .calculateMetric(data_individual, var, metric, window_interval, aggregate)
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

    # if response.col is specified, summarize labels within each window
    if (!is.null(response.col)) {
      if (!aggregate) {
        feature_list[[response.col]] <- zoo::rollapply(data_individual[[response.col]], width=window.size,
                                                       FUN=function(x) as.integer(mean(x, na.rm = TRUE) > 0.5),
                                                       align="center", fill=NA)
      } else {
        idx <- seq(1, nrow(data_individual), by = window.size)
        feature_list[[response.col]] <- sapply(idx, function(start_idx) {
          end_idx <- min(start_idx + window.size - 1, nrow(data_individual))
          as.integer(mean(data_individual[[response.col]][start_idx:end_idx], na.rm = TRUE) > 0.5)
        })
      }
    }

    ############################################################################
    # Combine features #########################################################
    ############################################################################

    # combine features into a single data frame
    feature_data <- as.data.frame(feature_list)

    # add the timestamp column
    if (!aggregate) {
      feature_data[[datetime.col]] <- data_individual[[datetime.col]]
    } else {
      feature_data[[datetime.col]] <- data_individual[[datetime.col]][seq(1, nrow(data_individual), by = window.size)]
    }

    # add the 'ID' column to the processed data
    feature_data[["ID"]] <- id

    # move datetime.col to the first column
    feature_data <- feature_data[, c("ID", datetime.col, setdiff(names(feature_data), c("ID", datetime.col)))]

    # convert response col back to factor
    if (!is.null(response.col)) {
      feature_data[[response.col]] <- as.factor(feature_data[[response.col]])
    }

    # remove rows with any missing values (NA) in any column
    feature_data <- na.omit(feature_data)

    # reapply the original attributes to the processed data
    for (attr_name in names(original_attributes)) {
      attr(feature_data, attr_name) <- original_attributes[[attr_name]]
    }

    # save data to list
    data_processed[[i]]  <- feature_data
  }


  ##############################################################################
  # Process data ###############################################################
  ##############################################################################

  # print time taken
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  cat(sprintf("\nTotal execution time: %.02f %s\n\n", as.numeric(time.taken), base::units(time.taken)))

  # return results
  return(data_processed)
}


################################################################################
# Define helper function to to calculate metrics ###############################
################################################################################

# Define a helper function to calculate metrics
.calculateMetric <- function(data, var, metric, window.size, aggregate) {

  # define a function for the metric
  metric_function <- switch(metric,
                            mean = function(x) mean(x, na.rm = TRUE),
                            median = function(x) median(x, na.rm = TRUE),
                            sd = function(x) sd(x, na.rm = TRUE),
                            range = function(x) diff(range(x, na.rm = TRUE)),
                            min = function(x) min(x, na.rm = TRUE),
                            max = function(x) max(x, na.rm = TRUE),
                            iqr = function(x) IQR(x, na.rm = TRUE),
                            sum = function(x) sum(x, na.rm = TRUE),
                            energy = function(x) sum(x^2, na.rm = TRUE),
                            skewness = function(x) moments::skewness(x, na.rm = TRUE),
                            kurtosis = function(x) moments::kurtosis(x, na.rm = TRUE),
                            entropy = function(x) {
                              hist_data <- hist(x, breaks = "Sturges", plot = FALSE)
                              p <- hist_data$density / sum(hist_data$density)
                              entropy::entropy(p)
                            })

  # apply the rolling function to the variable
  if (!aggregate) {
    zoo::rollapply(data[[var]], width = window.size, FUN = metric_function, align = "center", fill = NA)
  } else {
    # aggregate data into distinct windows
    idx <- seq(1, nrow(data), by = window.size)
    sapply(idx, function(start_idx) {
      end_idx <- min(start_idx + window.size - 1, nrow(data))
      metric_function(data[[var]][start_idx:end_idx])
    })
  }
}



#######################################################################################################
#######################################################################################################
#######################################################################################################
