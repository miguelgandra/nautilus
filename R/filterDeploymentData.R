#######################################################################################################
# Function to identify and extract deployment periods #################################################
#######################################################################################################

#' Filter deployment periods from a given dataset
#'
#' This function identifies deployment periods by analyzing changes in depth data using binary segmentation
#' to detect change points in both mean and variance. It then extracts the periods between the pre-deployment
#' and post-deployment phases based on the specified datetime range.
#'
#' The function uses a binary segmentation method (`cpt.meanvar`) to detect changes in depth and variance,
#' and it allows users to specify the maximum number of changepoints detected. The function also provides options
#' to visualize the data with custom plot options.
#'
#' @param data A data frame containing the processed dataset of each animal, as each one of the elements of the list
#' returned by the \link{processTagData} function. It should include a column for 'depth' and 'datetime'.
#' @param depth.threshold A numeric value specifying the minimum mean depth to classify a segment as part of
#' the deployment period (default is 3.5).
#' @param variance.threshold A numeric value specifying the minimum variance in depth measurements to classify
#' a segment as part of the deployment period. Segments with variance below this value are considered spurious (default is 6).
#' @param max.changepoints An integer specifying the maximum number of changepoints to detect (default is 6).
#' This parameter is passed to the \code{\link[changepoint]{cpt.meanvar}} function.
#' @param id.col A string representing the column name for the ID field (default is "ID").
#' @param datetime.col A string specifying the name of the column that contains timestamps for each observation.
#' This column must be in "POSIXct" format for proper processing (default is "datetime").
#' @param depth.col A string specifying the name of the column that contains depth measurements. Depth data is
#' used for detecting deployment periods (default is "depth").
#' @param plot.metrics A character vector of column names indicating additional metrics (e.g., acceleration or pitch)
#' to include in the visualization. These metrics are plotted alongside the depth data to aid in visually reviewing
#' the deployment period assignments (default is \code{c("pitch", "sway")}).
#' @param plot.metrics.labels A character vector specifying custom titles for the metrics plotted in the output.
#' If set to NULL, the column names provided in `plot.metrics` will be used as labels (default is NULL).
#' @param plot A boolean indicating whether to generate diagnostic plots showing depth and the specified metrics
#' during deployment periods. This visualization helps to validate the identified periods (default is TRUE).
#'
#' @return A filtered data frame containing the deployment period between the detected attachment and popup times.
#' @seealso \link{processTagData}, \code{\link[changepoint]{cpt.meanvar}}.
#' @export


filterDeploymentData <- function(data,
                                 depth.threshold = 3.5,
                                 variance.threshold = 6,
                                 max.changepoints = 6,
                                 id.col = "ID",
                                 datetime.col = "datetime",
                                 depth.col = "depth",
                                 plot.metrics = c("pitch", "sway"),
                                 plot.metrics.labels = NULL,
                                 plot = TRUE) {

  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

  # check if 'data' is either a data.frame or data.table
  if (!inherits(data, "data.frame") && !inherits(data, "data.table")) {
    stop("The input 'data' must be of class 'data.frame' or 'data.table'.", call. = FALSE)
  }

  # capture the original class of 'data' (either 'data.frame' or 'data.table')
  original_class <- class(data)

  # store original attributes before processing
  original_attributes <- attributes(data)

  # convert data.table to data.frame for processing
  if (inherits(data, "data.table")) {
    data <- as.data.frame(data)
  }

  # check if specified columns exists in the data
  if(!id.col %in% names(data)) stop(paste0("The specified id.col ('", id.col, "') was not found in the supplied data."), call. = FALSE)
  if(!datetime.col %in% names(data)) stop(paste0("The specified datetime.col ('", datetime.col, "') was not found in the supplied data."), call. = FALSE)
  if(!depth.col %in% names(data)) stop(paste0("The specified depth.col ('", depth.col, "') was not found in the supplied data."), call. = FALSE)

  # ensure datetime column is of POSIXct class
  if (!inherits(data[[datetime.col]], "POSIXct")) {
    stop(sprintf("The '%s' column must be of class 'Date' or 'POSIXct'.", datetime.col))
  }

  # ensure parameters are of correct type
  if (!is.numeric(depth.threshold) || length(depth.threshold) != 1 || depth.threshold <= 0) {
    stop("The 'depth.threshold' must be a positive numeric value.", call. = FALSE)
  }
  if (!is.numeric(variance.threshold) || length(variance.threshold) != 1 || variance.threshold <= 0) {
    stop("The 'variance.threshold' must be a positive numeric value.", call. = FALSE)
  }
  if (!is.numeric(max.changepoints) || max.changepoints != as.integer(max.changepoints) || max.changepoints <= 0) {
    stop("The 'max.changepoints' must be a positive integer.", call. = FALSE)
  }

  # validate plot parameters
  if(plot){
    # ensure that plot.metrics is a character vector of length 2
    if(!is.character(plot.metrics) || length(plot.metrics) != 2) {
      stop("The plot.metrics argument must be a character vector of length 2.", call.=FALSE)
    }
    # check if any of the specified plot.metrics are in the data column names
    if(!any(plot.metrics %in% names(data))) {
      stop(paste0("The specified plot.metrics (", paste(plot.metrics, collapse = ", "), ") were not found in the supplied data."), call. = FALSE)
    }
    # validate plot.metrics.labels
    if(is.null(plot.metrics.labels)){
      plot.metrics.labels <- plot.metrics
    }else if(length(plot.metrics.labels) != 2){
      stop("The plot.metrics.labels argument must be a character vector of length 2.", call. = FALSE)
    }
  }




  ##############################################################################
  # Prepare variables ##########################################################
  ##############################################################################

  # retrieve ID from the dataset based on the specified 'id.col' column
  id <- unique(data[[id.col]])

  # check if 'first.datetime' attribute exists in the dataset
  # if present, use it; otherwise, calculate the earliest datetime from the data
  if("first.datetime" %in% names(attributes(data))){
    first_datetime <- attributes(data)$first.datetime
  }else{
    first_datetime <- min(data[[datetime.col]], na.rm = TRUE)
  }

  # check if 'last.datetime' attribute exists in the dataset
  # if present, use it; otherwise, calculate the latest datetime from the data
  if("last.datetime" %in% names(attributes(data))){
    last_datetime <- attributes(data)$last.datetime
  }else{
    first_datetime <- max(data[[datetime.col]], na.rm = TRUE)
  }

  # check if PSAT columns are present
  psat_cols <- c("PTT", "position_type", "lon", "lat")
  positions_available <- FALSE
  if (all(psat_cols %in% colnames(data))) {
    # split PSAT positions for plotting (if available)
    if(any(!is.na(data$position_type))){
      positions_available <- TRUE
      fastloc_pos <- data[data$position_type == "FastGPS", c(datetime.col, "lon", "lat"), drop = FALSE]
      user_pos <- data[data$position_type == "User", c(datetime.col, "lon", "lat"), drop = FALSE]
    }
  }



  ##############################################################################
  # Add temporal buffer for changepoint analysis ###############################
  ##############################################################################

  # add 1 hour of 0-depth data before the first and after the last datetime
  before_df <- data.frame("ID"=id, "datetime"=seq(min(data[[datetime.col]])-3600, to=min(data[[datetime.col]])-1, by="1 sec"), depth=0)
  colnames(before_df)[2] <- datetime.col
  after_df <- data.frame("ID"=id, "datetime"=seq(max(data[[datetime.col]])+1, to=max(data[[datetime.col]])+3600, by="1 sec"), depth=0)
  colnames(after_df)[2] <- datetime.col

  # merge buffers with the original dataset
  data <- dplyr::bind_rows(before_df, data, after_df)
  data <- data[order(data[[datetime.col]]),]


  ##############################################################################
  # Filter out pre and post-deployment periods #################################
  ##############################################################################

  # feedback message for the user
  cat(paste0(
    crayon::bold("\n==================================================\n"),
    crayon::bold("========== Filtering Deployment Periods ==========\n"),
    "ID: ", crayon::blue$bold(id), "\n"))

  # run binary segmentation to detect change points in both mean and variance
  cp_depth <- suppressWarnings(changepoint::cpt.meanvar(data[[depth.col]], method="BinSeg", Q=max.changepoints, test.stat="Normal"))

  # extract changepoints
  changepoints <- changepoint::cpts(cp_depth)

  # add start and end indices
  changepoints <- c(1, changepoints, nrow(data))

  # calculate mean and variance for each segment
  segment_stats <- lapply(seq_along(changepoints[-1]), function(s) {
    start_idx <- changepoints[s]
    end_idx <- changepoints[s + 1] - 1
    segment <- data[[depth.col]][start_idx:end_idx]
    list(start = start_idx, end = end_idx, mean = mean(segment), variance = var(segment))
  })

  # convert to a data frame for easier manipulation
  segment_stats <- do.call(rbind, lapply(segment_stats, as.data.frame))

  # identify deployments
  deployment_segments <- which(segment_stats$mean >= depth.threshold | segment_stats$variance >= variance.threshold)
  spurious_segments <- which(segment_stats$mean < depth.threshold | segment_stats$variance < variance.threshold)

  # pre-deployment
  pre_deployment <- max(spurious_segments[spurious_segments < min(deployment_segments)])
  pre_segment_end <- segment_stats$end[pre_deployment]
  post_deployment <- min(spurious_segments[spurious_segments > max(deployment_segments)])
  post_segment_start <- segment_stats$start[post_deployment]

  # assign deploy_index and popup_index
  deploy_index <- pre_segment_end + 1
  popup_index <- post_segment_start - 1


  ##############################################################################
  # Print to console ###########################################################
  ##############################################################################

  attachtime <- data[[datetime.col]][deploy_index]
  poptime <- data[[datetime.col]][popup_index]

  # calculate the deploy duration
  total_duration <- difftime(last_datetime, first_datetime, units = "hours")
  total_duration <- paste(sprintf("%.2f", as.numeric(total_duration)), attributes(total_duration)$units)
  deploy_duration <- difftime(poptime, attachtime, units = "hours")
  deploy_duration <- paste(sprintf("%.2f", as.numeric(deploy_duration)), attributes(deploy_duration)$units)
  pre_deploy <- as.numeric(difftime(attachtime, first_datetime, units="hours"))
  pre_deploy <- sprintf("%dh:%02dm", floor(pre_deploy), round((pre_deploy - floor(pre_deploy)) * 60))
  post_deploy <- as.numeric(difftime(last_datetime, poptime, units="hours"))
  post_deploy <- sprintf("%dh:%02dm", floor(post_deploy), round((post_deploy - floor(post_deploy)) * 60))
  rows_discarded <- length(1:deploy_index) + length(popup_index:nrow(data))

  # print results to the console
  cat(sprintf("Total dataset duration: %s\n", total_duration))
  cat(sprintf("Estimated deployment duration: %s\n", deploy_duration))
  cat(sprintf("Attach time: %s (+%s)\n", strftime(attachtime, "%d/%b/%Y %H:%M:%S", tz="UTC"), pre_deploy))
  cat(sprintf("Popup time: %s (-%s)\n", strftime(poptime, "%d/%b/%Y %H:%M:%S", tz="UTC"), post_deploy))
  cat(sprintf("Rows removed: %d (~%.0f%%)", rows_discarded, (rows_discarded / nrow(data)) * 100))
  cat("\n")
  #cat(crayon::bold("\n==================================================\n"))

  ##############################################################################
  # Plot results ###############################################################
  ##############################################################################

  if(plot){

    ##################################################################
    # set up the layout ##############################################

    layout(matrix(1:3, ncol=1), heights=c(2,1,1))
    par(mar=c(0.6, 3.4, 2, 1), mgp=c(2.2,0.6,0))

    ##################################################################
    # generate the primary plot showing depth patterns ###############

    # set up empty plot with appropriate y-axis limits
    plot(y=data[[depth.col]], x=data[[datetime.col]], type="n", ylim=c(max(data[[depth.col]], na.rm=T), -5),
         main=id, xlab="", ylab="Depth (m)", las=1, xaxt="n", cex.axis=0.8, cex.lab=0.9, cex.main=1)
    # add a background shaded rectangle
    rect(par("usr")[1], par("usr")[3], par("usr")[2], par("usr")[4], col="grey97", border=NA)
    # plot the depth time series
    lines(y=data[[depth.col]], x=data[[datetime.col]], lwd=0.8)
    # add vertical red dashed lines to mark deployment and popup indices
    abline(v=data[[datetime.col]][c(deploy_index, popup_index)], col="red", lty=3)
    # highlight the discarded data in red
    lines(x=data[[datetime.col]][1:deploy_index], y=data[[depth.col]][1:deploy_index], col="red1", lwd=0.8)
    lines(x=data[[datetime.col]][popup_index:nrow(data)], y=data[[depth.col]][popup_index:nrow(data)], col="red1", lwd=0.8)
    # add a border around the plot area
    box()
    # if positions are available, highlight their timestamp
    if(positions_available){
      points(x=fastloc_pos[[datetime.col]], y=rep(par("usr")[4], nrow(fastloc_pos)), pch=16, col="blue", cex=0.8, xpd=T)
      points(x=user_pos[[datetime.col]], y=rep(par("usr")[4], nrow(user_pos)), pch=17, col="green2", cex=0.8, xpd=T)
      legend("bottomright", legend=c("Fastloc GPS", "User"), pch=c(16,17), col=c("blue","green2"),
             bty="n", horiz=T, cex=0.65, pt.cex=0.9, inset=c(-0.04, 0))
    }

    # reset the margin settings for subsequent plots
    par(mar=c(0.6, 3.4, 0.6, 1))

    ##################################################################
    # plot secondary and tertiary variables based on 'plot.metrics' ##
    for (v in 1:length(plot.metrics)) {

      # adjust margins for the last plot to provide space for labels
      if(v==length(plot.metrics))  par(mar=c(2, 3.4, 0.6, 1))

      # set up empty plot with appropriate y-axis limits
      plot(y=data[[plot.metrics[v]]], x=data[[datetime.col]], type="n", main="", xlab="",
           ylab=plot.metrics.labels[v], xaxt="n", las=1, cex.axis=0.8,  cex.lab=0.9)
      # add a background shaded rectangle
      rect(par("usr")[1], par("usr")[3], par("usr")[2], par("usr")[4], col="grey97", border=NA)
      # plot the metric time series
      lines(y=data[[plot.metrics[v]]], x=data[[datetime.col]], lwd=0.8)
      # add vertical red dashed lines to mark deployment and popup indices
      abline(v=data[[datetime.col]][c(deploy_index, popup_index)], col="red", lty=3)
      # highlight the discarded data in red
      lines(x=data[[datetime.col]][1:deploy_index], y=data[[plot.metrics[v]]][1:deploy_index], col="red1", lwd=0.8)
      lines(x=data[[datetime.col]][popup_index:nrow(data)], y=data[[plot.metrics[v]]][popup_index:nrow(data)], col="red1", lwd=0.8)
      # add a border around the plot area
      box()
      if(positions_available){
        points(x=fastloc_pos[[datetime.col]], y=rep(par("usr")[4], nrow(fastloc_pos)), pch=16, col="blue", cex=0.8, xpd=T)
        points(x=user_pos[[datetime.col]], y=rep(par("usr")[4], nrow(user_pos)), pch=17, col="green2", cex=0.8, xpd=T)
      }
    }

    ##################################################################
    # add a date x-axis ##############################################

    # determine the date range
    date_range <- range(data[[datetime.col]], na.rm = TRUE)
    # use 'pretty' to automatically calculate appropriate tick positions
    ticks <- pretty(date_range, n = 6)
    # set the date format
    date_format <- "%d-%b %H:%M"
    # add the custom x-axis with the selected date format
    axis(1, at=ticks, labels=format(ticks, date_format), cex.axis=0.8)

  }


  ##############################################################################
  # Save updated data ##########################################################
  ##############################################################################

  # subset the data from the specified indices (deploy_index to popup_index)
  data <- data[deploy_index:popup_index, ]

  # if the original data was a data.table, convert it back to data.table
  if ("data.table" %in% original_class) {
    data <- data.table::as.data.table(data)
  }

  # reapply the original attributes to the processed data
  attributes(data) <- original_attributes

  # return the filtered data
  return(data)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
