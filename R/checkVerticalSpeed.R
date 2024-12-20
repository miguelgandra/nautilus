#######################################################################################################
# Check for Vertical Displacement Speed Outliers ####################################################
#######################################################################################################

#' Identify and Handle Vertical Displacement Speed Outliers in Time Series Data
#'
#' This function checks for vertical displacement speed outliers in the data, where vertical speed
#' is calculated as the absolute difference in depth between consecutive measurements, adjusted for
#' the sampling rate. Spurious data points with vertical speeds exceeding a user-specified threshold
#' are flagged, and rows with these outliers are removed based on their position relative to the
#' defined pre-deployment and post-deployment periods.
#'
#' @param data A data frame or data table containing depth data (from a single animal).
#' @param id.col A string representing the column name for the ID field (default is "ID").
#' @param depth.col A character string specifying the name of the column containing the depth values. Default is `"depth"`.
#' @param vertical.speed.threshold A numeric value specifying the threshold for vertical speed outliers (in m/s).
#' @param pre.deploy.threshold A numeric value between 0 and 1, indicating the threshold (as a percentage) for the start of the deployment period.
#' @param post.deploy.threshold A numeric value between 0 and 1, indicating the threshold (as a percentage) for the end of the deployment period.
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
                               pre.deploy.threshold = 0.2,
                               post.deploy.threshold = 0.8,
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

  # calculate vertical speed (m/s) between consecutive depth measurements
  vertical_speed <- c(NA, diff(data[[depth.col]]) * sampling.rate)


  ##############################################################################
  # Check for vertical speed outliers ########################################
  ##############################################################################

  # identify indices where vertical speed exceeds the threshold
  outlier_indices <- which(abs(vertical_speed) > vertical.speed.threshold & !is.na(vertical_speed))

  # if outliers are found
  if (length(outlier_indices) > 0) {

    # identify gaps in the first 20% (pre-deployment) or last 80% (post-deployment) of the data
    pre_deploy_outliers <- outlier_indices[outlier_indices < round(pre.deploy.threshold * nrow(data))]
    post_deploy_outliers <- outlier_indices[outlier_indices > round(post.deploy.threshold * nrow(data))]

    # handle outliers before the deployment period
    if (length(pre_deploy_outliers) > 0) {
      # get the index of the last outlier before the deployment period
      outlier_index <- max(pre_deploy_outliers) + 1
      discarded_percentage <- outlier_index/nrow(data)*100
      # remove rows before the outlier
      data <- data[outlier_index:nrow(data), ]
      # provide feedback about the number of discarded rows
      if (verbose) {
        cat(sprintf("Vertical speed outliers detected: first %d rows discarded (%.1f%%).\n", outlier_index, discarded_percentage))
      }
    }

    # handle outliers after the deployment period
    if (length(post_deploy_outliers) > 0) {
      # get the index of the first outlier after the deployment period
      outlier_index <- min(post_deploy_outliers) - 1
      # calculate and print the number of rows discarded after the outlier
      discarded_rows <- nrow(data) - outlier_index
      discarded_percentage <- discarded_rows/nrow(data)*100
      # remove rows after the outlier
      data <- data[1:outlier_index, ]
      # provide feedback about the number of discarded rows
      if (verbose) {
        cat(sprintf("Vertical speed outliers detected: last %d rows discarded (%.1f%%).\n", discarded_rows, discarded_percentage))
      }
    }

    # else, throw error
    if(length(pre_deploy_outliers)==0 & length(post_deploy_outliers)==0){
      cat("Vertical speed outliers detected, but not removed. Check warnings.\n")
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
