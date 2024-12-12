#######################################################################################################
# Function to extract features from a sliding window #################################################
#######################################################################################################

#' Extract Features from a Sliding Window
#'
#' This function calculates specified metrics (e.g., mean, standard deviation) for selected variables over a sliding window.
#' It can either retain the temporal resolution of the dataset or aggregate data into distinct, non-overlapping windows.
#' This is useful for preparing the dataset for machine learning or other analytical methods that require a structured set of features.
#'
#' @param data A data frame containing the input data. Must include the variables specified in `variables`.
#' @param variables A character vector of column names from `data` for which to calculate metrics.
#' @param metrics A character vector specifying the metrics to calculate. Supported metrics: "mean", "sd", "min", "max", "sum".
#' @param window.size An integer specifying the size of the sliding or aggregation window.
#' @param aggregate Logical. If `TRUE`, aggregates data into distinct, non-overlapping windows. If `FALSE`, retains temporal resolution.
#' @param datetime.col A character string specifying the column in `data` containing datetime information.
#' @param response.col (Optional) A character string specifying the column in `data` containing response or annotated labels (e.g., feeding events).
#' It must be binary (i.e., containing values 0 or 1).

#' @return A data frame containing the calculated features. If `aggregate = TRUE`, the output will have fewer rows,
#' with one row per window. If `aggregate = FALSE`, the output retains the original number of rows, with
#' calculated metrics for each window. If a `datetime` column exists in the input data, it will be adjusted to match
#' the window aggregation, and the corresponding timestamp will be retained or adjusted based on the window size.
#' The response column, if specified, will also be aggregated or retained according to the same rules.


extractFeatures <- function(data,
                            variables = c("depth", "surge", "pitch"),
                            metrics = c("mean", "sd"),
                            window.size = 5,
                            aggregate = FALSE,
                            datetime.col = "datetime",
                            response.col = NULL){

  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

  # ensure specified variables exist in the dataset
  if(any(!variables %in% colnames(data))) {
    stop("Some specified variables are not present in the data.", call. = FALSE)
  }

  # ensure valid metrics are specified
  valid_metrics <- c("mean", "sd", "min", "max", "sum")
  if(any(!metrics %in% valid_metrics)) {
    stop("Some specified metrics are not supported. Supported metrics: ", paste(valid_metrics, collapse = ", "), call. = FALSE)
  }

  # check that datetime.col exists in the original dataset
  if (!datetime.col %in% colnames(data)) {
    stop("The specified datetime column does not exist in the data.", call. = FALSE)
  }

  # check that the datetime column is of class POSIXct
  if (!inherits(data[[datetime.col]], "POSIXct")) {
    stop(paste("The specified datetime column", datetime.col, "must be of class POSIXct."), call. = FALSE)
  }

  # check if response column exists and is binary
  if (!is.null(response.col)) {
    if (!response.col %in% colnames(data)) {
      stop("The specified response column is not present in the data.", call. = FALSE)
    }
    if (!all(data[[response.col]] %in% c(0, 1))) {
      stop("The response column must be binary (0 or 1).", call. = FALSE)
    }
  }


  ##############################################################################
  # Prepare input ##############################################################
  ##############################################################################

  # ensure variables are numeric
  for (var in variables) {
    if (!is.numeric(data[[var]])) {
      warning(paste("Variable", var, "is not numeric. Converting to numeric."), call. = FALSE)
      data[[var]] <- as.numeric(as.character(data[[var]]))
    }
  }

  # convert response col to numeric
  if (!is.null(response.col)) {
    if(is.factor(data[[response.col]])) data[[response.col]] <- as.numeric(as.factor(data[[response.col]])) - 1
  }

  ##############################################################################
  # Prepare output #############################################################
  ##############################################################################

  # initialize an empty list to store features
  feature_list <- list()

  # loop through each variable and calculate metrics using a sliding window
  for (var in variables) {

    for (metric in metrics) {

      # define a function for the metric
      metric_function <- switch(metric,
                                mean = function(x) mean(x, na.rm = TRUE),
                                sd = function(x) sd(x, na.rm = TRUE),
                                min = function(x) min(x, na.rm = TRUE),
                                max = function(x) max(x, na.rm = TRUE),
                                sum = function(x) sum(x, na.rm = TRUE))

      # apply the rolling function to the variable
      if (!aggregate) {
        feature_list[[paste0(var, "_", metric)]] <- zoo::rollapply(data[[var]], width=window.size,
                                                                   FUN=metric_function, align="center", fill=NA)
      } else {
      # aggregate data into distinct windows
        idx <- seq(1, nrow(data), by=window.size)
        feature_list[[paste0(var, "_", metric)]] <- sapply(idx, function(start_idx) {
          end_idx <- min(start_idx + window.size - 1, nrow(data))
          metric_function(data[[var]][start_idx:end_idx])
        })
      }
    }
  }

  # if response.col is specified, summarize labels within each window
  if (!is.null(response.col)) {
    if (!aggregate) {
      feature_list[[response.col]] <- zoo::rollapply(data[[response.col]], width=window.size,
                                                     FUN=function(x) as.integer(mean(x, na.rm = TRUE) > 0.5),
                                                     align="center", fill=NA)
    } else {
      idx <- seq(1, nrow(data), by = window.size)
      feature_list[[response.col]] <- sapply(idx, function(start_idx) {
        end_idx <- min(start_idx + window.size - 1, nrow(data))
        as.integer(mean(data[[response.col]][start_idx:end_idx], na.rm = TRUE) > 0.5)
      })
    }
  }

  ##############################################################################
  # Combine features ###########################################################
  ##############################################################################

  # combine features into a single data frame
  feature_data <- as.data.frame(feature_list)

  # add a timestamp column (if applicable)
  if (datetime.col %in% colnames(data)) {
    if (!aggregate) {
      feature_data[[datetime.col]] <- data[[datetime.col]]
    } else {
      feature_data[[datetime.col]] <- data[[datetime.col]][seq(1, nrow(data), by = window.size)]
    }
    # move datetime.col to the first column
    feature_data <- feature_data[, c(datetime.col, setdiff(names(feature_data), datetime.col))]
  }

  # convert response col back to factor
  if (!is.null(response.col)) {
    feature_data[[response.col]] <- as.factor(feature_data[[response.col]])
  }

  # remove rows with any missing values (NA) in any column
  feature_data <- na.omit(feature_data)

  # return data
  return(feature_data)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
