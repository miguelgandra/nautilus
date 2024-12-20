#######################################################################################################
# Check for Temporal Discontinuities in Time Series ###################################################
#######################################################################################################

#' Identify and Handle Temporal Discontinuities in Time Series Data
#'
#' This function identifies and handles temporal discontinuities in a specified datetime column of a dataset.
#' It determines the expected sampling interval by calculating the most frequent time difference
#' between consecutive timestamps. Any detected gaps larger than this interval are flagged as anomalies.
#'
#' Rows corresponding to these anomalies are removed based on their position relative to
#' two customizable thresholds:
#' - **Early data period:** If anomalies occur within the first early.threshold% of the dataset,
#'   all rows before the last anomaly in this range are discarded.
#' - **Late data period:** If anomalies occur within the last late.threshold% of the dataset,
#'   all rows after the first anomaly in this range are discarded.
#'
#' Anomalies detected within the central portion of the dataset, outside these thresholds, will not result in
#' any rows being removed. Instead, a warning is issued to alert the user to potential data irregularities
#' requiring manual inspection.
#'
#' @param data A data frame or data table containing a column with datetime information. The column should be of class POSIXt.
#' @param id.col A string representing the column name for the ID field (default is "ID").
#' @param datetime.col A character string specifying the name of the column containing the datetime values. Default is `"datetime"`.
#' @param early.threshold A numeric value between 0 and 1 that defines the portion of the dataset considered as the early / pre-deployment phase.
#' Rows falling within the first early.threshold * 100% of the dataset are classified as part of this phase.
#' Anomalies detected in this range will result in the removal of all earlier rows. The default value is 0.2 (20%).
#' @param late.threshold A numeric value between 0 and 1 that defines the portion of the dataset considered as the late / post-deployment phase.
#' Rows falling within the last (1 - late.threshold) * 100% of the dataset are classified as part of this phase.
#' Anomalies detected in this range will result in the removal of all subsequent rows. The default value is 0.8 (80%).
#' @param verbose Logical. If TRUE, the function will print detailed processing
#' information. Defaults to TRUE.
#'
#' @return A data frame (`data`) with rows before or after the detected temporal discontinuities removed.
#' @export


checkTimeGaps <- function(data,
                          id.col = "ID",
                          datetime.col = "datetime",
                          early.threshold = 0.2,
                          late.threshold = 0.8,
                          verbose = TRUE) {

  ##############################################################################
  # Input validation ###########################################################
  ##############################################################################

  # check if 'data' is either a data.frame or data.table
  if (!inherits(data, "data.frame") && !inherits(data, "data.table")) {
    stop("The input 'data' must be of class 'data.frame' or 'data.table'.", call. = FALSE)
  }

  # capture the original class of 'data' (either 'data.frame' or 'data.table')
  original_class <- class(data)

  # convert data.table to data.frame for processing
  if (inherits(data, "data.table")) {
    data <- as.data.frame(data)
  }

  # check if id.col exists in the data
  if (!(id.col %in% colnames(data))) {
    stop(paste0("The specified ID column (", id.col, ") does not exist in the data."), call. = FALSE)
  }

  # check if datetime.col exists in the data
  if (!(datetime.col %in% colnames(data))) {
    stop(paste0("The specified datetime column (", datetime.col, ") does not exist in the data."), call. = FALSE)
  }

  ##############################################################################
  # Check for temporal discontinuities between measurements ####################
  ##############################################################################

  # provide feedback to the user if verbose mode is enabled
  if (verbose) cat("Checking for temporal gaps in the data...\n")

  # calculate time difference between consecutive datetimes in seconds
  data$time_diff <- c(NA, diff(as.numeric(data[[datetime.col]])))

  # remove NA values (first row will have NA)
  time_diffs_no_na <- na.omit(data$time_diff)

  # find the most frequent time difference
  interval <- as.numeric(names(sort(table(time_diffs_no_na), decreasing = TRUE)[1]))

  # print the inferred sampling interval to the console
  #if (verbose) {
  #  interval_readable <- paste0(interval, " ", ifelse(interval == 1, "sec", "secs"))
  #  cat("Inferred sampling interval: ", interval_readable, "\n")
  #}

  ##############################################################################
  # Check for temporal discontinuities between measurements ####################
  ##############################################################################

  # find time_diff values greater than 1
  time_anomalies <- which(data$time_diff > interval)

  # if a gap is found
  if (length(time_anomalies) > 0) {

    # initialize discard ranges
    pre_deploy_cutoff <- 1
    post_deploy_cutoff <- nrow(data)

    # identify gaps occurring in the first 20% or last 80% of the total time
    pre_deploy_gaps <- time_anomalies[time_anomalies < round(early.threshold * nrow(data))]
    post_deploy_gaps <- time_anomalies[time_anomalies > round(late.threshold * nrow(data))]

    # if gaps are found in the beginning of the deployment
    if(length(pre_deploy_gaps) > 0) {
      # get the index of the last gap before the deployment period
      pre_deploy_cutoff <- max(pre_deploy_gaps)
      # calculate and print the number of rows discarded before the time gap
      discarded_interval <- round(difftime(data[[datetime.col]][1], data[[datetime.col]][pre_deploy_cutoff]), 1)
      # provide feedback about the number of discarded rows
      if (verbose) cat(sprintf("Time gaps found (%d): first %d rows discarded (%.1f %s).\n",
                               length(pre_deploy_gaps), pre_deploy_cutoff, discarded_interval, attr(discarded_interval, "units")))
    }

    # if gaps are found after the deployment period
    if(length(post_deploy_gaps) > 0) {
      # get the index of the first gap after the deployment period
      post_deploy_cutoff <- min(post_deploy_gaps)
      # calculate and print the number of rows discarded after the time gap
      discarded_interval <- round(difftime(data[[datetime.col]][post_deploy_cutoff], data[[datetime.col]][nrow(data)]), 1)
      if (verbose) cat(sprintf("Time gaps found (%d): last %d rows discarded (%.1f %s).\n",
                               length(post_deploy_gaps), nrow(data) - post_deploy_cutoff, discarded_interval, attr(discarded_interval, "units")))
    }

    # subset the data to remove outliers at both ends
    data <- data[pre_deploy_cutoff:post_deploy_cutoff, ]

    # check for remaining time gaps in the middle
    if(length(pre_deploy_gaps)==0 & length(post_deploy_gaps)==0) {
      if(verbose) cat(sprintf("Time gaps found, but not removed. Check warnings.\n", length(time_anomalies)))
      id <- unique(data[[id.col]])
      warning(paste(id, "-", length(time_anomalies), "time",
                    ifelse(length(time_anomalies) == 1, "gap", "gaps"),
                    "detected in the middle of the data range.",
                    "No rows were discarded. Please review the data to assess its validity."), call. = FALSE)
    }
  }

  ##############################################################################
  # Revert data back to its original class and return ##########################
  ##############################################################################

  # remove timediff column
  data <- data[,-which(colnames(data)=="time_diff")]

  # if the original data was a data.table, convert it back to data.table
  if ("data.table" %in% original_class) {
    data <- data.table(as.data.table(data))
  }

  # return results
  return(data)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
