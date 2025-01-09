#######################################################################################################
# Check for Vertical Displacement Speed Outliers ####################################################
#######################################################################################################

#' Identify and Handle Vertical Displacement Speed Outliers in Time Series Data
#'
#' This function checks for vertical displacement speed outliers in the data, where vertical speed
#' is calculated as the absolute difference in depth between consecutive measurements, adjusted for
#' the sampling rate. Spurious data points with vertical speeds exceeding a user-specified threshold
#' are flagged. Rows corresponding to these anomalies are removed based on their position relative to
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
#' @param data A data frame or data table containing depth data (from a single animal).
#' @param id.col A string representing the column name for the ID field (default is "ID").
#' @param depth.col A character string specifying the name of the column containing the depth values. Default is `"depth"`.
#' @param vertical.speed.threshold A numeric value specifying the threshold for vertical speed outliers (in m/s).
#' @param depth.sensor.resolution Numeric. The fixed uncertainty in the depth reading due to the sensor's resolution (e.g., ±0.5 meters).
#' @param depth.sensor.accuracy Numeric. The variable uncertainty in the depth reading, expressed as a percentage of the measured depth (e.g., ±1% of the depth reading).
#' @param early.threshold A numeric value between 0 and 1 that defines the portion of the dataset considered as the early / pre-deployment phase.
#' Rows falling within the first early.threshold * 100% of the dataset are classified as part of this phase.
#' Anomalies detected in this range will result in the removal of all earlier rows. The default value is 0.2 (20%).
#' @param late.threshold A numeric value between 0 and 1 that defines the portion of the dataset considered as the late / post-deployment phase.
#' Rows falling within the last (1 - late.threshold) * 100% of the dataset are classified as part of this phase.
#' Anomalies detected in this range will result in the removal of all subsequent rows. The default value is 0.8 (80%).
#' @param sampling.rate The sampling rate for the data (in Hz), used to adjust the vertical speed calculation. Default is `1`.
#' @param verbose Logical. If TRUE, the function will print detailed processing information. Defaults to TRUE.
#'
#' @return A data frame (`data`) with rows containing vertical speed outliers removed.
#' @export

checkVerticalSpeed <- function(data,
                               id.col = "ID",
                               depth.col = "depth",
                               vertical.speed.threshold,
                               sampling.rate,
                               depth.sensor.resolution = 0.5,
                               depth.sensor.accuracy = 1,
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

  # check if depth.col exists in the data
  if (!(depth.col %in% colnames(data))) {
    stop(paste0("The specified depth column (", depth.col, ") does not exist in the data."), call. = FALSE)
  }

  ##############################################################################
  # Calculate vertical displacement speed ####################################
  ##############################################################################

  # provide feedback to the user if verbose mode is enabled
  if (verbose) cat("Checking data for spurious depth values...\n")

  # calculate time difference (dt) between consecutive rows (in seconds)
  dt <- c(NA, diff(data$datetime))

  # calculate depth difference between consecutive rows (in meters)
  depth_diff <- c(NA, diff(data[[depth.col]]))

  # calculate vertical speed (m/s)
  vertical_speed <- depth_diff / dt

  # calculate the total uncertainty for each depth measurement (sensor accuracy + resolution)
  depth_uncertainty <- depth.sensor.resolution * 2 + abs(data[[depth.col]]) * depth.sensor.accuracy/100

  # only calculate vertical speed if the depth difference is large enough
  # compared to the sensor's accuracy/resolution and time difference
  vertical_speed <- ifelse(abs(depth_diff) > depth_uncertainty, depth_diff / dt,   NA)


  ##############################################################################
  # Check for vertical speed outliers ########################################
  ##############################################################################

  # identify indices where vertical speed exceeds the threshold
  outlier_indices <- which(abs(vertical_speed) > vertical.speed.threshold & !is.na(vertical_speed))

  # if outliers are found
  if (length(outlier_indices) > 0) {

    # initialize discard ranges
    pre_deploy_cutoff <- 1
    post_deploy_cutoff <- nrow(data)

    # identify gaps in the first 20% (pre-deployment) or last 80% (post-deployment) of the data
    pre_deploy_outliers <- outlier_indices[outlier_indices < round(early.threshold * nrow(data))]
    post_deploy_outliers <- outlier_indices[outlier_indices > round(late.threshold * nrow(data))]

    # handle outliers before the deployment period
    if (length(pre_deploy_outliers) > 0) {
      # get the index of the last outlier before the deployment period
      pre_deploy_cutoff <- max(pre_deploy_outliers) + 1
      discarded_percentage <- pre_deploy_cutoff / nrow(data) * 100
      # provide feedback about the number of discarded rows
      if (verbose) cat(sprintf("Vertical speed outliers detected: first %d rows discarded (%.1f%%).\n", pre_deploy_cutoff, discarded_percentage))
    }

    # handle outliers after the deployment period
    if (length(post_deploy_outliers) > 0) {
      # get the index of the first outlier after the deployment period
      post_deploy_cutoff <- min(post_deploy_outliers) - 1
      # calculate and print the number of rows discarded after the outlier
      discarded_percentage <- (nrow(data) - post_deploy_cutoff) / nrow(data) * 100
      # provide feedback about the number of discarded rows
      if (verbose) cat(sprintf("Vertical speed outliers detected: last %d rows discarded (%.1f%%).\n",
                               nrow(data) - post_deploy_cutoff, discarded_percentage))
    }

    # subset the data to remove outliers at both ends
    data <- data[pre_deploy_cutoff:post_deploy_cutoff, ]

    # check for remaining outliers in the middle
    if(length(pre_deploy_outliers)==0 & length(post_deploy_outliers)==0) {
      if(verbose) cat("Vertical speed outliers detected, but not removed. Check warnings.\n")
      id <- unique(data[[id.col]])
      warning(paste(id, "-", length(outlier_indices), "vertical speed",
                    ifelse(length(outlier_indices) == 1, "outlier", "outliers"),
                    "detected (>", vertical.speed.threshold, "m/s) in the middle of the data range.",
                    "No rows were discarded. Please review the data to assess its validity."), call. = FALSE)
    }
  }

  ##############################################################################
  # Revert data back to its original class and return ##########################
  ##############################################################################

  # revert back to original data class
  if ("data.table" %in% original_class) {
    data <- data.table::as.data.table(data)
  }

  # return cleaned data
  return(data)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
