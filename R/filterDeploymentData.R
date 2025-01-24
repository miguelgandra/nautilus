#######################################################################################################
# Function to identify and extract deployment periods #################################################
#######################################################################################################

#' Filter deployment periods from a given dataset
#'
#' This function identifies deployment periods by analyzing changes in depth data using binary segmentation
#' to detect change points in both mean and variance. It then extracts the periods between the pre-deployment
#' and post-deployment phases based on the specified datetime range.
#'
#' @details
#' The function uses a binary segmentation method (`cpt.meanvar`) to detect changes in depth and variance,
#' and it allows users to specify the maximum number of changepoints detected.
#'
#' If the sampling frequency is greater than 1 Hz, the function downsamples the data to 1 Hz by rounding
#' datetime values to the nearest second and computing the mean for each second. This is done to improve speed
#' and avoid memory bottlenecks when handling large datasets.
#'
#' The function also provides options to visualize the data with custom plot options.
#'
#' @param data A list of data tables/data frames, one for each individual, or a single aggregated data table/data frame
#' containing data from multiple animals. The output of the \link{processTagData} function is recommended, as it formats
#' the data appropriately for further analysis.
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
#' @param display.plots A logical value indicating whether the diagnostic plots should be displayed.
#' These plots generate diagnostic visuals showing depth and additional metrics,
#' assisting users in reviewing the extracted deployment periods.
#' If set to `TRUE`, the plots will be shown in the active graphics device.
#' Note: If set to `TRUE`, the code can take longer to run due to the delay in plotting extensive data series to the screen.
#' If set to `FALSE`, no plots will be displayed, even if they are saved.
#' Default is `FALSE`.
#' @param save.plots A logical value indicating whether the diagnostic plots should be saved.
#' If set to `TRUE`, the plots will be recorded and stored in the `diagnostic_plots` list.
#' The saved plots can later be displayed using `replayPlot()`.
#' These plots generate diagnostic visuals showing depth and additional metrics, assisting users in reviewing the extracted deployment periods.
#' If set to `FALSE`, no plots will be saved.
#' Default is `FALSE`.
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
                                 display.plots = FALSE,
                                 save.plots = TRUE) {


  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

  # if 'data' is not a list, split it into a list of individual data sets based on 'id.col'
  if (!is.list(data)) {
    data <- split(data, f = data[[id.col]])
  }

  # check if specified columns exist in the data
  if(!id.col %in% names(data[[1]])) stop(paste0("The specified id.col ('", id.col, "') was not found in the supplied data."), call. = FALSE)
  if(!datetime.col %in% names(data[[1]])) stop(paste0("The specified datetime.col ('", datetime.col, "') was not found in the supplied data."), call. = FALSE)
  if(!depth.col %in% names(data[[1]])) stop(paste0("The specified depth.col ('", depth.col, "') was not found in the supplied data."), call. = FALSE)

  # ensure datetime column is of POSIXct class
  if (!inherits(data[[1]][[datetime.col]], "POSIXct")) {
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
  if (display.plots || save.plots) {
    # ensure that plot.metrics is a character vector of length 2
    if (!is.character(plot.metrics) || length(plot.metrics) != 2) {
      stop("The plot.metrics argument must be a character vector of length 2.", call. = FALSE)
    }
    # check if any of the specified plot.metrics are in the data column names
    if (!any(plot.metrics %in% names(data[[1]]))) {
      stop(paste0("The specified plot.metrics (", paste(plot.metrics, collapse = ", "), ") were not found in the supplied data."), call. = FALSE)
    }
    # validate plot.metrics.labels
    if (is.null(plot.metrics.labels)) {
      plot.metrics.labels <- plot.metrics
    } else if (length(plot.metrics.labels) != 2) {
      stop("The plot.metrics.labels argument must be a character vector of length 2.", call. = FALSE)
    }
  }


  ##############################################################################
  # Process each data element ##################################################
  ##############################################################################

  # initialize an empty list to store the processed data and the plots
  processed_data <- vector("list", length=length(data))
  names(processed_data) <- names(data)
  diagnostic_plots <- vector("list", length=length(data))
  names(diagnostic_plots) <- names(data)

  # iterate over each element in 'data'
  for (i in 1:length(data)) {

    # access the individual dataset
    individual_data <- data[[i]]

    # skip NULL or empty elements in the list
    if (is.null(individual_data) || length(individual_data) == 0) next

    # check if each element in the list is a valid data.frame or data.table
    if (!inherits(individual_data, "data.frame") && !inherits(individual_data, "data.table")) {
      stop("Each element of the 'data' list must be a data.frame or data.table.", call. = FALSE)
    }

    # capture the original class of 'data' (either 'data.frame' or 'data.table')
    original_class <- class(individual_data)[1]

    # store original attributes before processing,  excluding internal ones
    discard_attrs <- c("row.names", "class", ".internal.selfref", "names")
    original_attributes <- attributes(individual_data)
    original_attributes <- original_attributes[!names(original_attributes) %in% discard_attrs]

    # convert data.table to data.frame for processing
    if (inherits(individual_data, "data.frame")) {
      individual_data <- data.table::setDT(individual_data)
    }

    # retrieve ID from the dataset based on the specified 'id.col' column
    id <- unique(individual_data[[id.col]])



    ############################################################################
    # Prepare variables ########################################################
    ############################################################################

    # check if 'first.datetime' attribute exists in the dataset
    # if present, use it; otherwise, calculate the earliest datetime from the data
    first_datetime <- if ("first.datetime" %in% names(attributes(data[[i]]))) {
      attributes(data[[i]])$first.datetime
    } else {
      individual_data[, min(get(datetime.col), na.rm = TRUE)]
    }

    # check if 'last.datetime' attribute exists in the dataset
    # if present, use it; otherwise, calculate the latest datetime from the data
    last_datetime <- if ("last.datetime" %in% names(attributes(data[[i]]))) {
      attributes(data[[i]])$last.datetime
    } else {
      individual_data[, max(get(datetime.col), na.rm = TRUE)]
    }

    # check if PSAT columns are present
    psat_cols <- c("PTT", "position_type", "lon", "lat")
    positions_available <- FALSE
    if (all(psat_cols %in% colnames(individual_data))) {
      # split PSAT positions for plotting (if available)
      if (individual_data[, any(!is.na(position_type))]) {
        positions_available <- TRUE
        # filter for FastGPS positions
        fastloc_pos <- individual_data[position_type == "FastGPS", .SD, .SDcols = c(datetime.col, "lon", "lat")]
        # filter for User positions
        user_pos <- individual_data[position_type == "User", .SD, .SDcols = c(datetime.col, "lon", "lat")]
      }
    }


    ############################################################################
    # Downsample data to 1 Hz ##################################################
    ############################################################################

    # check if 'processed.sampling.frequency' attribute exists in the dataset
    # if present, use it; otherwise, calculate the sampling frequency from the data
    sampling_freq <- if ("processed.sampling.frequency" %in% names(attributes(individual_data))) {
      attributes(data[[i]])$processed.sampling.frequency
    } else {
      sampling_rate <- nrow(individual_data)/length(unique(lubridate::floor_date(individual_data[[datetime.col]], "sec")))
      plyr::round_any(sampling_rate, 5)
    }

    # check if the sampling frequency is greater than 1 Hz and downsample is required
    if(sampling_freq > 1){

      # temporarily suppress console output (redirect to a temporary file)
      sink(tempfile())

      # downsample to 1 Hz by rounding datetime to the nearest second and taking the mean value for each second
      reduced_data <- individual_data[, lapply(c(depth.col, plot.metrics), function(col) mean(get(col), na.rm = TRUE)),
                                      by = .(lubridate::floor_date(get(datetime.col)))]
      # correct the column names
      data.table::setnames(reduced_data, old = names(reduced_data), new =  c(datetime.col, c(depth.col, plot.metrics)))

      # add ID column
      reduced_data[, ID := id]

      # reorder columns
      data.table::setcolorder(reduced_data, c("ID", setdiff(names(reduced_data), "ID")))

      # restore normal output
      sink()

      # else, if downsampling is not required (sampling frequency <= 1 Hz)
    } else {

      #keep only relevant columns
      reduced_data <- individual_data[, c(id.col, datetime.col, depth.col, plot.metrics), with = FALSE]

    }


    ############################################################################
    # Add temporal buffer for changepoint analysis #############################
    ############################################################################

    # add 1 hour of 0-depth data before the first and after the last datetime
    before_dt <- data.table::data.table("ID" = id,
                                        datetime = seq(min(reduced_data[[datetime.col]]) - 3600,
                                                       to = min(reduced_data[[datetime.col]]) - 1,
                                                       by = "1 sec"),
                                        depth = 0)

    after_dt <- data.table::data.table("ID" = id,
                                       datetime = seq(max(reduced_data[[datetime.col]]) + 1,
                                                      to = max(reduced_data[[datetime.col]]) + 3600,
                                                      by = "1 sec"),
                                       depth = 0)

    # merge buffers with the original dataset
    reduced_data <- data.table::rbindlist(list(before_dt, reduced_data, after_dt), fill = TRUE)

    # order by datetime column
    data.table::setorder(reduced_data, datetime)


    ############################################################################
    # Filter out pre and post-deployment periods ###############################
    ############################################################################

    # feedback message for the user
    cat(paste0(
      crayon::bold("\n==================================================\n"),
      crayon::bold("========== Filtering Deployment Periods ==========\n"),
      "ID: ", crayon::blue$bold(id), "\n"))

    # run binary segmentation to detect change points in both mean and variance
    cp_depth <- suppressWarnings(changepoint::cpt.meanvar(reduced_data[[depth.col]], method="BinSeg", Q=max.changepoints, test.stat="Normal"))

    # extract changepoints
    changepoints <- changepoint::cpts(cp_depth)

    # add start and end indices
    changepoints <- c(1, changepoints, nrow(reduced_data))

    # calculate mean and variance for each segment
    segment_stats <- lapply(seq_along(changepoints[-1]), function(s) {
      start_idx <- changepoints[s]
      end_idx <- changepoints[s + 1] - 1
      segment <- reduced_data[[depth.col]][start_idx:end_idx]
      list(start = start_idx, end = end_idx, mean = mean(segment), variance = var(segment))
    })

    # convert to a data frame for easier manipulation
    segment_stats <- do.call(rbind, lapply(segment_stats, as.data.frame))

    # identify deployments
    deployment_segments <- which(segment_stats$mean >= depth.threshold | segment_stats$variance >= variance.threshold)
    spurious_segments <- which(segment_stats$mean < depth.threshold | segment_stats$variance < variance.threshold)

    # check if no valid deployment segments are identified
    if(length(deployment_segments)==0){
      # notify that the dataset is discarded
      cat("No valid deployment segments detected. Dataset discarded.\n")
      # assign deploy_index and popup_index when no valid segments are found
      deploy_index <- nrow(reduced_data)+1
      popup_index <-  nrow(reduced_data)+1

    }else{

      # identify the pre-deployment segment
      if(any(spurious_segments < min(deployment_segments))){
        pre_deployment <- max(spurious_segments[spurious_segments < min(deployment_segments)])
      }else{
        pre_deployment <- max(spurious_segments[spurious_segments <= min(deployment_segments)])
      }
      pre_segment_end <- segment_stats$end[pre_deployment]

      # identify the post-deployment segment
      if(any(spurious_segments > max(deployment_segments))){
        post_deployment <- min(spurious_segments[spurious_segments > max(deployment_segments)])
      }else{
        post_deployment <- min(spurious_segments[spurious_segments >= max(deployment_segments)])
      }
      post_segment_start <- segment_stats$start[post_deployment]

      # assign deploy_index and popup_index
      deploy_index <- pre_segment_end + 1
      popup_index <- post_segment_start - 1


      ##########################################################################
      # Print to console #######################################################

      attachtime <- reduced_data[[datetime.col]][deploy_index]
      poptime <- reduced_data[[datetime.col]][popup_index]

      # calculate the deploy duration
      total_duration <- difftime(last_datetime, first_datetime, units = "hours")
      total_duration <- paste(sprintf("%.2f", as.numeric(total_duration)), attributes(total_duration)$units)
      deploy_duration <- difftime(poptime, attachtime, units = "hours")
      deploy_duration <- paste(sprintf("%.2f", as.numeric(deploy_duration)), attributes(deploy_duration)$units)
      deploy_percentage <- as.numeric(difftime(poptime, attachtime, units = "hours"))/as.numeric(difftime(last_datetime, first_datetime, units = "hours"))*100
      pre_deploy <- as.numeric(difftime(attachtime, first_datetime, units="hours"))
      pre_deploy <- sprintf("%dh:%02dm", floor(pre_deploy), round((pre_deploy - floor(pre_deploy)) * 60))
      post_deploy <- as.numeric(difftime(last_datetime, poptime, units="hours"))
      post_deploy <- sprintf("%dh:%02dm", floor(post_deploy), round((post_deploy - floor(post_deploy)) * 60))
      rows_discarded <- length(1:deploy_index) + length(popup_index:nrow(reduced_data))

      # print results to the console
      cat(sprintf("Total dataset duration: %s\n", total_duration))
      cat(sprintf("Estimated deployment duration: %s (%.1f%%)\n", deploy_duration, deploy_percentage))
      cat(sprintf("Attach time: %s (+%s)\n", strftime(attachtime, "%d/%b/%Y %H:%M:%S", tz="UTC"), pre_deploy))
      cat(sprintf("Popup time: %s (-%s)\n", strftime(poptime, "%d/%b/%Y %H:%M:%S", tz="UTC"), post_deploy))
      cat(sprintf("Rows removed: %d (~%.0f%%)", rows_discarded, (rows_discarded / nrow(data)) * 100))
      cat("\n")
    }


    ############################################################################
    # Plot results #############################################################
    ############################################################################

    if(display.plots || save.plots){

      ##################################################################
      # set up the layout ##############################################

      # turn on an off-screen plotting device (null PDF device)
      if(!display.plots) {
        pdf(NULL)
        dev.control(displaylist = "enable")
      }

      layout(matrix(1:3, ncol=1), heights=c(2,1,1))
      par(mar=c(0.6, 3.4, 2, 2), mgp=c(2.2,0.6,0))
      total_rows <- nrow(reduced_data)

      ##################################################################
      # generate the primary plot showing depth patterns ###############

      # set up empty plot with appropriate y-axis limits
      plot(y=reduced_data[[depth.col]], x=reduced_data[[datetime.col]], type="n", ylim=c(max(reduced_data[[depth.col]], na.rm=T), -5),
           main=id, xlab="", ylab="Depth (m)", las=1, xaxt="n", cex.axis=0.8, cex.lab=0.9, cex.main=1)
      # add a background shaded rectangle
      rect(par("usr")[1], par("usr")[3], par("usr")[2], par("usr")[4], col="grey97", border=NA)
      # plot the depth time series
      lines(y=reduced_data[[depth.col]], x=reduced_data[[datetime.col]], lwd=0.8)
      # add vertical red dashed lines to mark deployment and popup indices
      abline(v=reduced_data[[datetime.col]][c(deploy_index, popup_index)], col="red", lty=3)
      # highlight the discarded data in red
      lines(x=reduced_data[[datetime.col]][1:deploy_index], y=reduced_data[[depth.col]][1:deploy_index], col="red1", lwd=0.8)
      lines(x=reduced_data[[datetime.col]][popup_index:total_rows], y=reduced_data[[depth.col]][popup_index:total_rows], col="red1", lwd=0.8)
      # add a border around the plot area
      box()
      # if positions are available, highlight their timestamp
      if(positions_available){
        points(x=fastloc_pos[[datetime.col]], y=rep(par("usr")[4], nrow(fastloc_pos)), pch=16, col="blue", cex=0.9, xpd=T)
        points(x=user_pos[[datetime.col]], y=rep(par("usr")[4], nrow(user_pos)), pch=17, col="green2", cex=0.9, xpd=T)
        legend("bottomright", legend=c("Fastloc GPS", "User"), pch=c(16,17), col=c("blue","green2"),
               bty="n", horiz=T, cex=0.65, pt.cex=0.95, inset=c(-0.04, 0))
      }

      # reset the margin settings for subsequent plots
      par(mar=c(0.6, 3.4, 0.6, 2))

      ##################################################################
      # plot secondary and tertiary variables based on 'plot.metrics' ##
      for (v in 1:length(plot.metrics)) {

        # adjust margins for the last plot to provide space for x-axis labels
        if(v==length(plot.metrics))  par(mar=c(2, 3.4, 0.6, 2))

        # set up empty plot with appropriate y-axis limits
        plot(y=reduced_data[[plot.metrics[v]]], x=reduced_data[[datetime.col]], type="n", main="", xlab="",
             ylab=plot.metrics.labels[v], xaxt="n", las=1, cex.axis=0.8,  cex.lab=0.9)
        # add a background shaded rectangle
        rect(par("usr")[1], par("usr")[3], par("usr")[2], par("usr")[4], col="grey97", border=NA)
        # plot the metric time series
        lines(y=reduced_data[[plot.metrics[v]]], x=reduced_data[[datetime.col]], lwd=0.8)
        # add vertical red dashed lines to mark deployment and popup indices
        abline(v=reduced_data[[datetime.col]][c(deploy_index, popup_index)], col="red", lty=3)
        # highlight the discarded data in red
        lines(x=reduced_data[[datetime.col]][1:deploy_index], y=reduced_data[[plot.metrics[v]]][1:deploy_index], col="red1", lwd=0.8)
        lines(x=reduced_data[[datetime.col]][popup_index:total_rows], y=reduced_data[[plot.metrics[v]]][popup_index:total_rows], col="red1", lwd=0.8)
        # add a border around the plot area
        box()
        if(positions_available){
          points(x=fastloc_pos[[datetime.col]], y=rep(par("usr")[4], nrow(fastloc_pos)), pch=16, col="blue", cex=0.9, xpd=T)
          points(x=user_pos[[datetime.col]], y=rep(par("usr")[4], nrow(user_pos)), pch=17, col="green2", cex=0.9, xpd=T)
        }
      }

      ##################################################################
      # add a date x-axis ##############################################

      # determine the date range
      date_range <- range(reduced_data[[datetime.col]], na.rm = TRUE)
      # use 'pretty' to automatically calculate appropriate tick positions
      ticks <- pretty(date_range, n = 6)
      # calculate minor tick positions between main ticks
      minor_ticks <- seq(from = min(ticks), to = max(ticks), by = diff(ticks)[1] / 2)
      minor_ticks <- setdiff(minor_ticks, ticks)
      # set the date format
      date_format <- "%d-%b %H:%M"
      # add the custom x-axis with the selected date format
      axis(1, at=ticks, labels=format(ticks, date_format), cex.axis=0.8)
      # add the minor ticks without labels
      axis(1, at = minor_ticks, labels = FALSE, tcl = -0.3)

      ##################################################################
      # record plot (if required) ######################################

      # capture the current plot
      if (save.plots) diagnostic_plots[[i]] <- recordPlot()

      # close the temporary device
      if(!display.plots) dev.off()

    }

    ############################################################################
    # Save updated data ########################################################
    ############################################################################

    # subset the data between the specified attachtime and poptime
    individual_data <- individual_data[individual_data[[datetime.col]] >= attachtime & individual_data[[datetime.col]] <= poptime, ]


    # if the original data was a data.table, convert it back to data.table
    if ("data.frame" %in% original_class) {
      individual_data <- as.data.frame(individual_data)
    }

    # reapply the original attributes to the processed data
    for (attr_name in names(original_attributes)) {
      attr(individual_data, attr_name) <- original_attributes[[attr_name]]
    }

    # save the filtered data
    processed_data[[i]] <- individual_data
    names(processed_data)[i] <- names(data)[i]

  }


  ##############################################################################
  # Return results #############################################################
  ##############################################################################

  # filter out NULL elements from the diagnostic_plots list
  diagnostic_plots <- diagnostic_plots[!sapply(diagnostic_plots, is.null)]

  # return both the processed data and diagnostic plots
  return(list(filtered_data = processed_data, plots = diagnostic_plots))

}

#######################################################################################################
#######################################################################################################
#######################################################################################################
