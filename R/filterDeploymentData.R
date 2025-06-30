#######################################################################################################
# Function to identify and extract deployment periods #################################################
#######################################################################################################

#' Filter deployment periods from a given dataset
#'
#' @description
#' This function identifies and extracts actual deployment periods from high-resolution biologging data.
#' It offers two primary methods for defining these periods: an **automated depth-based approach**
#' or the option to **supply custom start and end times** for each individual.
#'
#' When using the automated depth-based approach, the function analyzes changes in depth data
#' using binary segmentation (`cpt.meanvar`) to detect change points in both mean and variance.
#' It then extracts the periods between the pre-deployment and post-deployment phases
#' based on the specified depth and variance thresholds.
#'
#' For datasets with a sampling frequency greater than 1 Hz, the function automatically
#' downsamples the data to 1 Hz. This is achieved by rounding datetime values to the nearest second
#' and computing the mean for each second, significantly improving processing speed and
#' preventing memory bottlenecks when handling large datasets.
#'
#' The function also provides robust options to visualize the identified deployment periods
#' with custom plot options, including additional behavioral metrics, to aid in visual review and validation.
#'
#' @param data A list of data.tables/data.frames, one for each individual; a single aggregated data.table/data.frame
#' containing data from multiple animals (with an 'ID' column); or a character vector of file paths pointing to
#' `.rds` files, each containing data for a single individual. When a character vector is provided,
#' files are loaded sequentially to optimize memory use. The output of the \link{processTagData} function
#' is strongly recommended, as it formats the data appropriately for all downstream analysis.
#' @param id.col A string representing the column name for the ID field (default is "ID").
#' @param datetime.col A string specifying the name of the column that contains timestamps for each observation.
#' This column must be in "POSIXct" format for proper processing (default is "datetime").
#' @param depth.col A string specifying the name of the column that contains depth measurements. Depth data is
#' used for detecting deployment periods (default is "depth").
#' @param custom.deployment.times An optional `data.frame` or `data.table` with three columns: `ID`, `start`, and `end`.
#' This allows users to manually specify deployment periods for each individual, overriding the depth-based detection.
#' `start` and `end` must be in "POSIXct" format. If provided, `depth.threshold`, `variance.threshold`,
#' and `max.changepoints` will be ignored. Default is `NULL`.
#' @param depth.threshold A numeric value specifying the minimum mean depth to classify a segment as part of
#' the deployment period (default is 3.5). This parameter is ignored if `custom.deployment.times` is provided.
#' @param variance.threshold A numeric value specifying the minimum variance in depth measurements to classify
#' a segment as part of the deployment period. Segments with variance below this value are considered spurious (default is 6).
#' This parameter is ignored if `custom.deployment.times` is provided.
#' @param max.changepoints An integer specifying the maximum number of changepoints to detect (default is 6).
#' This parameter is passed to the \code{\link[changepoint]{cpt.meanvar}} function. This parameter is ignored if `custom.deployment.times` is provided.
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
#' @param plot.metrics An optional character vector of column names indicating additional metrics
#' (e.g., acceleration or temperature) to include in the visualization. These metrics are plotted
#' alongside the depth data to aid in visually reviewing the deployment period assignments.
#' Required only if `display.plots` or `save.plots` is `TRUE`. If NULL (default) and plots are requested,
#' defaults to `c("temp", "ax")`. Must be length 2 if provided.
#' @param plot.metrics.labels An optional character vector specifying custom titles for the metrics
#' plotted in the output. If NULL (default), the column names provided in `plot.metrics` will be
#' used as labels. Must be NULL if `plot.metrics` is NULL, and must be length 2 if provided.
#' Ignored unless plots are being generated (`display.plots` or `save.plots` is `TRUE`).
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
#' @return The function always returns a consistent named list structure when there are results:
#' \itemize{
#'   \item If both data and plots are available: Returns a list with \code{filtered_data} and \code{plots} elements
#'   \item If only data is available: Returns a list with just the \code{filtered_data} element
#'   \item If only plots are available: Returns a list with just the \code{plots} element
#'   \item If nothing is available (both \code{return.data} and \code{save.plots} are FALSE): Returns \code{NULL} invisibly
#' }
#' Note that data will be saved to disk if \code{save.files = TRUE}, regardless of the return value.
#'
#' @seealso \link{importTagData},, \link{processTagData}, \code{\link[changepoint]{cpt.meanvar}}.
#' @export


filterDeploymentData <- function(data,
                                 id.col = "ID",
                                 datetime.col = "datetime",
                                 depth.col = "depth",
                                 custom.deployment.times = NULL,
                                 depth.threshold = 3.5,
                                 variance.threshold = 6,
                                 max.changepoints = 6,
                                 display.plots = FALSE,
                                 save.plots = TRUE,
                                 plot.metrics = NULL,
                                 plot.metrics.labels = NULL,
                                 return.data = TRUE,
                                 save.files = FALSE,
                                 output.folder = NULL,
                                 output.suffix = NULL) {


  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

  # measure running time
  start.time <- Sys.time()

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
      stop(paste0("The specified id.col ('", id.col, "') was not found in the supplied data."), call. = FALSE)
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
  required_cols <- c(id.col, datetime.col, depth.col)
  if (display.plots || save.plots) required_cols <- c(required_cols, plot.metrics)

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
      if (!inherits(dataset[[datetime.col]], "POSIXct")) stop(paste0("The specified datetime.col ('", datetime.col, "') must be of class 'Date' or 'POSIXct'."), call. = FALSE)
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

  # validate custom.deployment.times if provided
  if (!is.null(custom.deployment.times)) {
    if (!is.data.frame(custom.deployment.times)) {
      stop("custom.deployment.times must be a data frame with columns: ID, start, end", call. = FALSE)
    }
    if (!all(c("ID", "start", "end") %in% names(custom.deployment.times))) {
      stop("custom.deployment.times must contain columns: ID, start, end", call. = FALSE)
    }
    if (!inherits(custom.deployment.times$start, "POSIXct") || !inherits(custom.deployment.times$end, "POSIXct")) {
      stop("The 'start' and 'end' columns in custom.deployment.times must be POSIXct", call. = FALSE)
    }
  }

  # validate plot parameters
  if (display.plots || save.plots) {
    # ensure that plot.metrics is a character vector of length 2
    if (!is.character(plot.metrics) || length(plot.metrics) != 2) {
      stop("The plot.metrics argument must be a character vector of length 2.", call. = FALSE)
    }
    # validate plot.metrics.labels
    if (is.null(plot.metrics.labels)) {
      plot.metrics.labels <- plot.metrics
    } else if (length(plot.metrics.labels) != 2) {
      stop("The plot.metrics.labels argument must be a character vector of length 2.", call. = FALSE)
    }
  }


  # validate plot parameters
  if (display.plots || save.plots) {
    # validate plot.metrics.labels - must be NULL if plot.metrics is NULL
    if (!is.null(plot.metrics.labels) && is.null(plot.metrics)) {
      stop("plot.metrics.labels was provided but plot.metrics was not.", call. = FALSE)
    }
    # if plot.metrics is NULL, set default values
    if (is.null(plot.metrics)) {
      plot.metrics <- c("temp", "ax")
      plot.metrics.labels <- c("Temperature (\u00B0C)", "Acc X (\u00B0)")
    }
    # ensure that plot.metrics is a character vector of length 2
    if (!is.character(plot.metrics) || length(plot.metrics) != 2) {
      stop("The plot.metrics argument must be a character vector of length 2.", call. = FALSE)
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
  n_animals <- length(data)
  processed_data <- vector("list", length=n_animals)
  names(processed_data) <- names(data)
  diagnostic_plots <- vector("list", length=n_animals)
  names(diagnostic_plots) <- names(data)

  # feedback message for the user
  cat(paste0(
    crayon::bold("\n=============== Filtering Deployment Periods ===============\n"),
    "Scanning data from ", n_animals, " ", ifelse(n_animals == 1, "tag", "tags"), " to extract deployment windows\n",
    crayon::bold("============================================================\n\n")
  ))


  # iterate over each element in 'data'
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
      if (!inherits(individual_data[[datetime.col]], "POSIXct")) stop(paste0("The specified datetime.col ('", datetime.col, "') must be of class 'Date' or 'POSIXct'."), call. = FALSE)
      if (is.null(attr(individual_data, "nautilus.version"))) {
        message(paste0("Warning: File '", basename(file_path), "' was likely not processed via importTagData(). It is strongly recommended to run it through importTagData() to ensure proper formatting."))
      }

      # get ID
      id <- unique(individual_data[[id.col]])[1]


    ############################################################################
    # data is already in memory (list of data frames/tables) ###################
    } else {
      # retrieve ID from the dataset based on the specified 'id.col' column
      id <- names(data)[i]
      # access the individual dataset
      individual_data <- data[[i]]
    }

    # print current ID
    cat(crayon::bold(sprintf("[%d/%d] %s\n", i, n_animals, id)))

    # skip NULL or empty elements in the list
    if (is.null(individual_data) || length(individual_data) == 0) next

    # convert data.table to data.frame for processing
    if (inherits(individual_data, "data.frame")) {
      individual_data <- data.table::setDT(individual_data)
    }

    # ensure data is ordered by datetime
    data.table::setorder(individual_data, datetime)

    # store original attributes, excluding internal ones
    discard_attrs <- c("row.names", "class", ".internal.selfref", "names")
    original_attributes <- attributes(individual_data)
    original_attributes <- original_attributes[!names(original_attributes) %in% discard_attrs]

    # correct negative depths
    individual_data[depth < 0, depth := 0]

    # set default flag
    valid_dataset <- TRUE


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
    location_cols <- c("PTT", "position_type", "lon", "lat")
    positions_available <- FALSE
    if (all(location_cols %in% colnames(individual_data))) {
      # split location for plotting (if available)
      if (individual_data[, any(!is.na(position_type))]) {
        positions_available <- TRUE
        # filter for metadata deployment positions
        deploy_pos <- individual_data[grepl("deploy", position_type, fixed=TRUE), .SDcols = c(datetime.col, "lon", "lat")]
        # filter for metadata popup positions
        popup_pos <- individual_data[grepl("popup", position_type, fixed=TRUE), .SDcols = c(datetime.col, "lon", "lat")]
        # filter for FastGPS positions
        fastloc_pos <- individual_data[position_type == "FastGPS", .SD, .SDcols = c(datetime.col, "lon", "lat")]
        # filter for User positions
        user_pos <- individual_data[position_type == "User", .SD, .SDcols = c(datetime.col, "lon", "lat")]
      }
    }


    ############################################################################
    # Downsample data to 1 Hz ##################################################
    ############################################################################

    # calculate the sampling frequency
    sampling_freq <- nrow(individual_data)/length(unique(lubridate::floor_date(individual_data[[datetime.col]], "sec")))
    sampling_freq <- plyr::round_any(sampling_freq, 5)

    # print to console
    cat(paste("Sampling frequency:", sampling_freq, "Hz\n"))


    # check if the sampling frequency is greater than 1 Hz and downsample is required
    if(sampling_freq > 1){

      # temporarily suppress console output (redirect to a temporary file)
      sink(tempfile())

      # determine which columns to downsample
      cols_to_downsample <- depth.col
      if (!is.null(plot.metrics)) cols_to_downsample <- c(cols_to_downsample, plot.metrics)

      # downsample to 1 Hz by rounding datetime to the nearest second and taking the mean value for each second
      reduced_data <- individual_data[, lapply(cols_to_downsample, function(col) mean(get(col), na.rm = TRUE)),
                                      by = .(lubridate::floor_date(get(datetime.col)))]

      # correct the column names
      new_names <- c(datetime.col, cols_to_downsample)
      data.table::setnames(reduced_data, old = names(reduced_data), new = new_names)

      # add ID column
      reduced_data[, (id.col) := id]

      # reorder columns
      data.table::setcolorder(reduced_data, c(id.col, setdiff(names(reduced_data), id.col)))

      # restore normal output
      sink()

      # else, if downsampling is not required (sampling frequency <= 1 Hz)
    } else {

      # keep only relevant columns
      cols_to_keep <- c(id.col, datetime.col, depth.col)
      if (display.plots || save.plots) cols_to_keep <- c(cols_to_keep, plot.metrics)
      reduced_data <- individual_data[, cols_to_keep, with = FALSE]

    }


    ############################################################################
    # Check for custom periods #################################################
    ############################################################################

    valid_deployment_times <- FALSE

    if (!is.null(custom.deployment.times)) {

      # find custom period for this ID
      custom_period <- custom.deployment.times[custom.deployment.times$ID == id, ]

      # use custom periods if available
      if (nrow(custom_period) > 0) {

        cat("Using custom deployment times\n")
        attachtime <- custom_period$start[1]
        poptime <- custom_period$end[1]

        # find corresponding indices in reduced_data
        attach_idx <- which.min(abs(reduced_data[[datetime.col]] - attachtime))
        pop_idx <- which.min(abs(reduced_data[[datetime.col]] - poptime))

        # verify custom periods are within data range
        if (attachtime < first_datetime || poptime > last_datetime) {
          message(paste("Custom period extends beyond data range, using depth-based approach instead"))
        } else {
          valid_deployment_times <- TRUE
        }
      }
    }


    ############################################################################
    # Add temporal buffer for changepoint analysis #############################
    ############################################################################

    # if not valid custom deployment periods were supplied
    if(!valid_deployment_times){

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
        # set flag
        valid_dataset <- FALSE
        # notify that the dataset is discarded
        message("No valid deployment segments detected. Dataset discarded.")
        # assign attach_idx and pop_idx when no valid segments are found
        attach_idx <- nrow(reduced_data)+1
        pop_idx <-  nrow(reduced_data)+1
        # calculate accurate stats for invalid deployment
        original_rows <- nrow(individual_data)
        kept_rows <- 0
        rows_discarded <- original_rows
        # print accurate stats
        total_hours <- sprintf("%.2f", as.numeric(difftime(last_datetime, first_datetime, units="hours")))
        cat(sprintf("Total dataset duration: %s hours\n", total_hours))
        cat("Estimated deployment duration: 0 hours (0%)\n")
        cat("Filtered dataset size: ~ 0\n")
        cat(sprintf("Rows removed: ~ %s\n", .format_large_number(original_rows)))

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

        # assign attach_idx and pop_idx
        attach_idx <- pre_segment_end + 1
        pop_idx <- post_segment_start - 1

        attachtime <- reduced_data[[datetime.col]][attach_idx]
        poptime <- reduced_data[[datetime.col]][pop_idx]
      }
    }


    ##########################################################################
    # Print to console #######################################################

    if(valid_dataset){

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

      # calculate row statistics
      original_rows <- nrow(individual_data)
      kept_rows <- sum(individual_data[[datetime.col]] >= attachtime & individual_data[[datetime.col]] <= poptime)
      rows_discarded <- original_rows - kept_rows

      # format large numbers with K/M suffixes
      .format_large_number <- function(n) {
        if (n >= 1e6) {
          paste0(round(n/1e6, 1), " M")
        } else if (n >= 1e3) {
          paste0(round(n/1e3, 1), " K")
        } else {
          as.character(n)
        }
      }

      # print results to the console
      cat(sprintf("Total dataset duration: %s\n", total_duration))
      cat(sprintf("Estimated deployment duration: %s (%.1f%%)\n", deploy_duration, deploy_percentage))
      cat(sprintf("Attach time: %s (+%s)\n", strftime(attachtime, "%d/%b/%Y %H:%M:%S", tz="UTC"), pre_deploy))
      cat(sprintf("Popup time: %s (-%s)\n", strftime(poptime, "%d/%b/%Y %H:%M:%S", tz="UTC"), post_deploy))
      cat(sprintf("Filtered dataset size: ~ %s\n", .format_large_number(kept_rows)))
      cat(sprintf("Rows removed: ~ %s\n", .format_large_number(rows_discarded)))

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
      abline(v=reduced_data[[datetime.col]][c(attach_idx, pop_idx)], col="red", lty=3)
      # highlight the discarded data in red
      lines(x=reduced_data[[datetime.col]][1:attach_idx], y=reduced_data[[depth.col]][1:attach_idx], col="red1", lwd=0.8)
      lines(x=reduced_data[[datetime.col]][pop_idx:total_rows], y=reduced_data[[depth.col]][pop_idx:total_rows], col="red1", lwd=0.8)
      # add a border around the plot area
      box()
      # if positions are available, highlight their timestamp
      if(positions_available){

        # initialize legend components
        legend_items <- list()
        legend_pch <- list()
        legend_bg <- list()
        # check and add each position type if it exists
        if(nrow(deploy_pos) > 0) {
          points(x=deploy_pos[[datetime.col]], y=rep(par("usr")[4], nrow(deploy_pos)), pch=24, bg="green2", lwd=0.2, cex=1, xpd=T)
          legend_items <- c(legend_items, "Deployment (metadata)")
          legend_pch <- c(legend_pch, 24)
          legend_bg <- c(legend_bg, "green2")
        }
        if(nrow(user_pos) > 0) {
          points(x=user_pos[[datetime.col]], y=rep(par("usr")[4], nrow(user_pos)), pch=22, bg="darkorchid1", lwd=0.2, cex=1, xpd=T)
          legend_items <- c(legend_items, "User")
          legend_pch <- c(legend_pch, 22)
          legend_bg <- c(legend_bg, "darkorchid1")
        }
        if(nrow(fastloc_pos) > 0) {
          points(x=fastloc_pos[[datetime.col]], y=rep(par("usr")[4], nrow(fastloc_pos)), pch=21, bg="blue", lwd=0.2, cex=1, xpd=T)
          legend_items <- c(legend_items, "Fastloc GPS")
          legend_pch <- c(legend_pch, 21)
          legend_bg <- c(legend_bg, "blue")
        }
        if(nrow(popup_pos) > 0) {
          points(x=popup_pos[[datetime.col]], y=rep(par("usr")[4], nrow(popup_pos)), pch=24, bg="green2", lwd=0.2, cex=1, xpd=T)
          legend_items <- c(legend_items, "Popup (metadata)")
          legend_pch <- c(legend_pch, 24)
          legend_bg <- c(legend_bg, "green2")
        }
        # only add legend if there are any items to show
        if(length(legend_items) > 0) {
          legend("bottomright", legend = unlist(legend_items), pch = unlist(legend_pch),
                 pt.bg = unlist(legend_bg), bty = "n", pt.lwd = 0.2,
                 cex = 0.65, pt.cex = 1, inset = c(0, 0.02), y.intersp = 1.4,
                 text.width = max(strwidth(unlist(legend_items), "user") * 0.8))
        }
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
        abline(v=reduced_data[[datetime.col]][c(attach_idx, pop_idx)], col="red", lty=3)
        # highlight the discarded data in red
        lines(x=reduced_data[[datetime.col]][1:attach_idx], y=reduced_data[[plot.metrics[v]]][1:attach_idx], col="red1", lwd=0.8)
        lines(x=reduced_data[[datetime.col]][pop_idx:total_rows], y=reduced_data[[plot.metrics[v]]][pop_idx:total_rows], col="red1", lwd=0.8)
        # add a border around the plot area
        box()
        if(positions_available){
          points(x=fastloc_pos[[datetime.col]], y=rep(par("usr")[4], nrow(fastloc_pos)), pch=21, bg="blue", lwd=0.2, cex=1, xpd=T)
          points(x=user_pos[[datetime.col]], y=rep(par("usr")[4], nrow(user_pos)), pch=22, bg="darkorchid1", lwd=0.2, cex=1, xpd=T)
          points(x=deploy_pos[[datetime.col]], y=rep(par("usr")[4], nrow(deploy_pos)), pch=24, bg="green2", lwd=0.2, cex=1, xpd=T)
          points(x=popup_pos[[datetime.col]], y=rep(par("usr")[4], nrow(popup_pos)), pch=24, bg="green2", lwd=0.2, cex=1, xpd=T)
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

    # reapply the original attributes to the processed data
    for (attr_name in names(original_attributes)) {
      attr(individual_data, attr_name) <- original_attributes[[attr_name]]
    }

    # save the filtered data as an RDS file
    if(save.files && valid_dataset){

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

      # define the file suffix: use the specified suffix or default to a suffix based on the sampling rate
      sufix <- ifelse(!is.null(output.suffix), output.suffix, "")

      # construct the output file name
      output_file <- file.path(output_dir, paste0(id, sufix, ".rds"))

      # save the processed data
      saveRDS(individual_data, output_file)

      # overwrite the line with completion message
      cat("\r")
      cat(rep(" ", getOption("width")-1))
      cat("\r")
      cat(sprintf("\u2713 Saved: %s\n", basename(output_file)))
    }

    # store filtered sensor data in the results list if needed
    if (return.data && valid_dataset) {
      processed_data[[i]] <- individual_data
      names(processed_data)[i] <- id
    }

    # print empty line
    cat("\n")

    # clear unused objects from the environment to free up memory
    rm(individual_data)
    gc()
  }


  ##############################################################################
  # Return results #############################################################
  ##############################################################################

  # print message
  cat("\n")
  cat(crayon::bold("All done!\n"))

  # print time taken
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  cat(crayon::bold("Total execution time:"), sprintf("%.02f", as.numeric(time.taken)), base::units(time.taken), "\n\n")


  # prepare the return object
  result <- list()

  # only include plots if they were requested and generated
  if (save.plots && length(diagnostic_plots) > 0) {
    # filter out NULL elements from the diagnostic_plots list
    diagnostic_plots <- diagnostic_plots[!sapply(diagnostic_plots, is.null)]
    if (length(diagnostic_plots) > 0) {
      result$plots <- diagnostic_plots
    }
  }

  # include data if requested
  if (return.data) {
    result$filtered_data <- processed_data
  }

  # determine what to return based on the results
  if (length(result) == 0) {
    # if nothing to return (both plots and data were not requested or empty)
    return(invisible(NULL))
  } else {
    # always return the full named list structure
    return(result)
  }

}

#######################################################################################################
#######################################################################################################
#######################################################################################################
