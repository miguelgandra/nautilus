#######################################################################################################
# Check for Temporal Discontinuities in Time Series ###################################################
#######################################################################################################

#' Identify and Handle Temporal Discontinuities in Time Series Data
#'
#' This function checks for temporal discontinuities in a specified datetime column of a dataset.
#' It automatically infers the sampling interval by identifying the most frequent time difference between
#' consecutive timestamps and reports it to the user. Detected gaps larger than this interval are flagged
#' as anomalies, and rows corresponding to these gaps are discarded based on their position relative to
#' the defined pre-deployment and post-deployment periods.
#'
#' @param data A data frame or data table containing a column with datetime information. The column should be of class POSIXt.
#' @param datetime.col A character string specifying the name of the column containing the datetime values. Default is `"datetime"`.
#' @param pre.deploy.threshold A numeric value between 0 and 1, indicating the threshold (as a percentage) for the start of the deployment period.
#' Data before this percentage will be considered as pre-deployment. Default is 0.2 (20%).
#' @param post.deploy.threshold A numeric value between 0 and 1, indicating the threshold (as a percentage) for the end of the deployment period.
#' Data after this percentage will be considered as post-deployment. Default is 0.8 (80%).
#' @param verbose Logical. If TRUE, the function will print detailed processing
#' information. Defaults to TRUE.
#'
#' @return A data frame (`data`) with rows before or after the detected temporal discontinuities removed.
#' @export


checkTimeGaps <- function(data,
                          datetime.col = "datetime",
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

  # check if datetime.col exists in the data
  if (!(datetime.col %in% colnames(data))) {
    stop(paste0("The specified datetime column (", datetime.col, ") does not exist in the data."), call. = FALSE)
  }

  ##############################################################################
  # Check for temporal discontinuities between measurements ####################
  ##############################################################################

  # calculate time difference between consecutive datetimes in seconds
  data$time_diff <- c(NA, diff(as.numeric(data[[datetime.col]])))

  # remove NA values (first row will have NA)
  time_diffs_no_na <- na.omit(data$time_diff)

  # find the most frequent time difference
  interval <- as.numeric(names(sort(table(time_diffs_no_na), decreasing = TRUE)[1]))

  # print the inferred sampling interval to the console
  if (verbose) {
    interval_readable <- paste0(interval, " ", ifelse(interval == 1, "sec", "secs"))
    cat("Inferred sampling interval: ", interval_readable, "\n")
  }

  ##############################################################################
  # Check for temporal discontinuities between measurements ####################
  ##############################################################################

  # find time_diff values greater than 1
  time_anomalies <- data$time_diff[which(data$time_diff > interval)]

  # if a gap is found
  if (length(time_anomalies) > 0) {
    # print the total number of time anomalies found to the console
    cat(paste0("Time gaps found (", length(time_anomalies),"): "))

    # identify the indices where the time anomalies occur
    time_anomalies_indices <- which(data$time_diff > interval)

    # identify gaps occurring in the first 20% or last 80% of the total time
    pre_deploy_gaps <- time_anomalies_indices[time_anomalies_indices < round(pre.deploy.threshold * nrow(data))]
    post_deploy_gaps <- time_anomalies_indices[time_anomalies_indices > round(post.deploy.threshold * nrow(data))]

    # if gaps are found in the beginning of the deployment
    if(length(pre_deploy_gaps) > 0) {
      # get the index of the last gap before the deployment period
      time_gap_index <- max(pre_deploy_gaps)
      # calculate and print the number of rows discarded before the time gap
      discarded_interval <- round(difftime(data[[datetime.col]][1], data[[datetime.col]][time_gap_index]), 1)
      cat(paste0("first ", time_gap_index, " rows discarded (", discarded_interval, " ", attr(discarded_interval, "units"), ")\n"))
      # discard data before the time gap
      data <- data[time_gap_index:nrow(data),]

    # if gaps are found after the deployment period
    } else if(length(post_deploy_gaps) > 0) {
      # get the index of the first gap after the deployment period
      time_gap_index <- min(post_deploy_gaps)
      # calculate and print the number of rows discarded after the time gap
      discarded_rows <- nrow(data) - time_gap_index
      discarded_interval <- round(difftime(data[[datetime.col]][time_gap_index], data[[datetime.col]][nrow(data)]), 1)
      cat(paste0("last ", discarded_rows, " rows discarded (", discarded_interval, " ", attr(discarded_interval, "units"), ")\n"))
      # discard data after the time gap
      data <- data[1:time_gap_index,]

    } else {
      stop("Time gap detected in the middle of the data range", call. = FALSE)
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
