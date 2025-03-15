#######################################################################################################
# Identify and Handle Outliers in Time Series Data ####################################################
#######################################################################################################

#' Identify and Handle Outliers in Time Series Data
#'
#' This function identifies and processes outliers in a specified sensor variable by evaluating both
#' rapid changes between consecutive measurements and prolonged constant readings that may indicate
#' sensor failure (stall periods). Outliers are flagged when the rate of change exceeds a user-defined
#' threshold, accounting for sensor resolution and accuracy. Additionally, the function detects
#' prolonged periods of unchanged sensor values, which could suggest sensor malfunction.
#' The function classifies outliers into two categories:
#' 1. **Isolated Outliers**: Singular, transient anomalies (e.g., sensor glitches or logging errors).
#'    These values (spikes) are replaced with `NA` or, optionally, smoothed using linear interpolation.
#' 2. **Consecutive Anomaly Periods**: Clusters of outliers occurring within a specified time window,
#'    likely due to sensor malfunction or saturation. The function removes all affected values and replaces
#'    small isolated blocks of valid readings within these periods with `NA` to ensure data integrity.
#'    No data interpolation is applied in these cases.
#'
#' @param data A list of data tables/data frames, one for each individual, or a single aggregated data table/data frame
#' containing data from multiple animals. The output of the \link{processTagData} function is recommended, as it formats
#' the data appropriately for further analysis.
#' @param id.col A string representing the column name for the ID field (default is "ID").
#' @param datetime.col A string specifying the name of the column that contains timestamps for each observation.
#' This column must be in "POSIXct" format for proper processing (default is "datetime").
#' @param sensor.col A string representing the column name containing the sensor data to be analyzed.
#' @param sensor.name A string representing the name of the sensor being analyzed. This can be used for
#' descriptive purposes in print statements or output, such as "depth", "temperature", or other sensor types.
#' @param rate.threshold A numeric value representing the threshold rate of change (in units per second)
#' beyond which values will be flagged as outliers. Values exceeding this threshold will
#' be considered outliers and replaced with `NA` (or interpolated).
#' @param sensor.resolution Numeric. The sensor's resolution (the smallest detectable change in sensor readings).
#' This value is used for uncertainty estimation in rate of change calculations. Default is 0.5.
#' @param sensor.accuracy.fixed Numeric. A fixed sensor accuracy value (in the same units as `sensor.col`).
#' If provided, this value is used for uncertainty calculations. This argument should
#' not be used simultaneously with `sensor.accuracy.percent`.
#' @param sensor.accuracy.percent Numeric. A percentage value representing the sensor's accuracy, defined as
#' ± percentage of the sensor reading. This is used to calculate the uncertainty in the
#' sensor's measurements based on the readings. This argument should not be used
#' simultaneously with `sensor.accuracy.fixed`.
#' @param outlier.window Numeric. The time window (in minutes) within which consecutive outliers
#' are grouped and treated as a single anomaly period. If multiple outliers occur within this
#' window, all affected values—including small "islands" of valid data between them—are replaced
#' with `NA` to ensure data quality. Default is 10 minutes.
#' @param stall.threshold A numeric value specifying the threshold (in minutes)
#' for detecting prolonged constant sensor readings. The function will flag
#' sequences of identical values that persist beyond this threshold as potential
#' sensor failures. However, periods where the sensor value is zero (e.g., depth = 0,
#' indicating surface intervals) are excluded to prevent misidentification of
#' expected behavior.
#' @param interpolate Logical. If TRUE, the function will interpolate the missing values (flagged outliers) using
#' the `zoo` package's `na.approx` function. If FALSE, outliers will simply be replaced with `NA`.
#' Default is TRUE.
#'
#' @return A data frame (`data`) with rows containing vertical speed outliers removed.
#' @export

checkSensorAnomalies <- function(data,
                                 id.col = "ID",
                                 datetime.col = "datetime",
                                 sensor.col = "depth",
                                 sensor.name = sensor.col,
                                 rate.threshold,
                                 sensor.resolution = 0.5,
                                 sensor.accuracy.fixed = NULL,
                                 sensor.accuracy.percent = NULL,
                                 outlier.window = 5,
                                 stall.threshold = 5,
                                 interpolate = TRUE) {


  ##############################################################################
  # Input validation ###########################################################
  ##############################################################################

  # if 'data' is not a list, split it into a list of individual data sets based on 'id.col'
  if (!is.list(data)) {
    data <- split(data, f = data[[id.col]])
  }

  # check if specified columns exist in the data
  if(!id.col %in% names(data[[1]])) stop(paste0("The specified id.col ('", id.col, "') was not found in the supplied data."), call. = FALSE)
  if(!datetime.col %in% names(data[[1]])) stop(paste0("The specified datetime.col ('", datetime.col, "') was not found in the supplied data."), call. = FALSE)
  if(!sensor.col %in% names(data[[1]])) stop(paste0("The specified sensor.col ('", sensor.col, "') was not found in the supplied data."), call. = FALSE)

  # ensure datetime column is of POSIXct class
  if (!inherits(data[[1]][[datetime.col]], "POSIXct")) {
    stop(sprintf("The '%s' column must be of class 'Date' or 'POSIXct'.", datetime.col))
  }

  # check if all data eleemnts are either a data.frame or data.table
  any_invalid <- any(sapply(data, function(x) {!is.null(x) && !inherits(x, "data.frame") && !inherits(x, "data.table")}))
  if (any_invalid) {
    stop("All non-NULL elements of 'data_list' must be of class 'data.frame' or 'data.table'.", call. = FALSE)
  }

  # ensure either sensor.accuracy.fixed or sensor.accuracy.percent is provided, but not both
  if (!is.null(sensor.accuracy.fixed) && !is.null(sensor.accuracy.percent)) {
    stop("Error: Both 'sensor.accuracy.fixed' and 'sensor.accuracy.percent' cannot be provided at the same time.", call. = FALSE)
  } else if (is.null(sensor.accuracy.fixed) && is.null(sensor.accuracy.percent)) {
    stop("Error: Either 'sensor.accuracy.fixed' or 'sensor.accuracy.percent' must be provided.", call. = FALSE)
  }

  # check if sensor.accuracy.percent is greater than 0 and less than 100 (if provided)
  if (!is.null(sensor.accuracy.percent)) {
    if (sensor.accuracy.percent <= 0 || sensor.accuracy.percent >= 100) {
      stop("Error: 'sensor.accuracy.percent' must be greater than 0 and less than 100.", call. = FALSE)
    }
  }

  ##############################################################################
  # Process each data element ##################################################
  ##############################################################################

  # get the total number of animals in the dataset
  n_animals <- length(data)

  # initialize a flag to track whether any outliers are found
  no_errors_found <- TRUE

  # feedback message for the user
  cat(paste0(
    crayon::bold("\n================= Scanning for Sensor Anomalies =================\n"),
    "Analyzing ", tolower(sensor.name), " time series for ", n_animals, " ", ifelse(n_animals == 1, "tag", "tags"), " to ensure data integrity\n",
    crayon::bold("=================================================================\n\n")
  ))

  # iterate over each element in 'data'
  for (i in 1:length(data)) {

    # access the individual dataset
    individual_data <- data[[i]]

    # skip NULL or empty elements in the list
    if (is.null(individual_data) || length(individual_data) == 0) next

    # create a copy of the specified columns (ID, datetime, sensor) to preserve the original data for plotting
    plot_data <- data[[i]][, .SD, .SDcols = c(datetime.col, sensor.col)]

    # capture the original class of 'data' (either 'data.frame' or 'data.table')
    original_class <- class(individual_data)

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
    # Calculate rate of change #################################################
    ############################################################################

    # check if 'processed.sampling.frequency' attribute exists in the dataset
    # if present, use it; otherwise, calculate the sampling frequency from the data
    sampling_freq <- if ("processed.sampling.frequency" %in% names(attributes(individual_data))) {
      attributes(data[[i]])$processed.sampling.frequency
    } else {
      sampling_rate <- nrow(individual_data)/length(unique(lubridate::floor_date(individual_data[[datetime.col]], "sec")))
      plyr::round_any(sampling_rate, 5)
    }

    # calculate time difference (dt) between consecutive rows (in seconds)
    dt <- c(NA, diff(individual_data[[datetime.col]]))

    # calculate value difference between consecutive rows
    value_diff <- c(NA, diff(individual_data[[sensor.col]]))

    # calculate the uncertainty in sensor readings based on the accuracy percentage
    # the accuracy is calculated as the percentage of the absolute value of the sensor reading
    if(!is.null(sensor.accuracy.percent)){
      sensor_accuracy <- sensor.accuracy.percent/100 * abs(individual_data[[sensor.col]])
    }else{
      sensor_accuracy <- sensor.accuracy.fixed
    }

    # determine the sensor uncertainty as the maximum of sensor accuracy and sensor resolution
    # this ensures that the uncertainty is never less than the sensor's resolution
    sensor.uncertainty <- pmax(sensor_accuracy, sensor.resolution)

    # calculate the uncertainty in the rate of change using error propagation
    # the formula accounts for the uncertainty in sensor readings (sensor.uncertainty)
    # the term for time uncertainty is ignored, assuming no uncertainty in time measurements (set to 0)
    # sqrt((uncertainty due to value difference)^2 + (uncertainty due to time difference)^2)
    rate_uncertainty <- sqrt((1 / dt * sensor.uncertainty)^2 + ((value_diff) / dt^2 * 0)^2)

    # calculate rate of change
    # only compute the rate if the difference between consecutive values
    # exceeds the combined uncertainty (sensor resolution + accuracy),
    # ensuring minor fluctuations due to sensor noise are ignored.
    rate_of_change <- ifelse(abs(value_diff) > rate_uncertainty, value_diff / dt, NA)


    ############################################################################
    # Identify outliers and prolonged sensor anomalies #########################
    ############################################################################

    # identify indices where the rate of change exceeds the threshold
    jump_indices <- which(abs(rate_of_change) > rate.threshold & !is.na(rate_of_change))

    # compute the threshold for detecting sensor failures (in number of samples)
    stall_threshold_samples <- sampling_freq * 60 * stall.threshold

    # identify sequences of prolonged constant sensor values
    stall_indices <- c()
    rle_sensor <- rle(individual_data[[sensor.col]])
    stall_periods <- rle_sensor$lengths >= stall_threshold_samples & (rle_sensor$values > 0)

    # check if any prolonged constant sensor values were detected
    if(any(stall_periods)){

      # calculate the total duration of flagged sensor anomalies (in minutes)
      flagged_intervals <- rle_sensor$lengths[stall_periods]

      # identify start and end indices of prolonged constant sensor intervals
      rle_indices <- cumsum(rle_sensor$lengths)
      start_indices <- rle_indices[stall_periods] - rle_sensor$lengths[stall_periods] + 1
      end_indices <- rle_indices[stall_periods]

      # collect indices for flagged sensor anomalies
      for (a in seq_along(start_indices)) {
        stall_indices <- c(stall_indices, start_indices[a]:end_indices[a])
      }
    }

    # combine detected outlier indices (jumps and prolonged values)
    outlier_indices <- c(jump_indices, stall_indices)
    outlier_indices <- outlier_indices[order(outlier_indices)]
    outlier_indices <- unique(outlier_indices)

    # create empty lists to store outliers and interpolated values
    flagged_outliers <- c()
    interpolated_outliers <- c()


    ############################################################################
    # Group and handle detected anomalies ######################################
    ############################################################################

    # check if any potential outliers were identified.
    if (length(outlier_indices) > 0) {

      # set the flag to FALSE if any outliers are found
      no_errors_found <- FALSE

      # feedback message for the user
      cat(paste0("ID: ", crayon::blue$bold(id), "\n"))

      # convert the specified time window to steps based on the sampling rate
      anomaly_window <- outlier.window * 60 * sampling_freq

      # group outliers within the anomaly window (steps-based)
      outlier_groups <- split(outlier_indices, cumsum(c(1, diff(outlier_indices) > anomaly_window)))

      # initialize counters for messaging
      isolated_outliers_count <- 0
      interpolated_values_count <- 0
      malfunction_blocks_count <- 0
      malfunction_duration <- 0

      # process each group of outliers
      for (group in outlier_groups) {

        # sporadic sensor outlier
        if (length(group) == 2) {
          # identify the outlier with the larger absolute value
          # consecutive outlier indices occur back-to-back in the data (e.g., a spike and the subsequent value)
          idx1 <- group[1]
          idx2 <- group[2]
          remove_idx <- if (abs(individual_data[[sensor.col]][idx1]) > abs(individual_data[[sensor.col]][idx2])) idx1 else idx2
          # save the flagged outlier before removing
          flagged_outliers <- c(flagged_outliers, remove_idx)
          # convert the more extreme outlier to NA
          individual_data[remove_idx, (sensor.col) := NA]
          isolated_outliers_count <- isolated_outliers_count + 1
          # if interpolate is TRUE, interpolate the discarded values
          if (interpolate) {
            # store interpolated values before modifying the dataset
            interpolated_outliers <- c(interpolated_outliers, remove_idx)
            # apply interpolation only to remove_idx positions
            window <- max(1, remove_idx - 1):min(nrow(individual_data), remove_idx + 1)
            local_y <- individual_data[[sensor.col]][window]
            individual_data[remove_idx, (sensor.col) := zoo::na.approx(local_y, na.rm = FALSE)[which(window == remove_idx)]]
            interpolated_values_count <- interpolated_values_count + 1
          }

        # multiple outliers in close proximity: treat as sensor malfunction
        } else {
          # save the flagged outlier before removing
          flagged_outliers <- c(flagged_outliers, group)
          # convert outliers to NA
          individual_data[group, (sensor.col) := NA]
          malfunction_blocks_count <- malfunction_blocks_count + length(group)
          # calculate the duration of the malfunction block
          duration <- difftime(max(individual_data[[datetime.col]][group]), min(individual_data[[datetime.col]][group]), units = "secs")
          malfunction_duration <- malfunction_duration + as.numeric(duration)
          # find isolated values before converting them to NA
          isolated_flags <- which(.findIsolated(individual_data[[sensor.col]]))
          if (length(isolated_flags) > 0) {
            # store these flagged isolated values
            flagged_outliers <- c(flagged_outliers, isolated_flags)
            # convert isolated blocks of non-NA values to NA
            individual_data[isolated_flags, (sensor.col) :=  NA]
          }
        }
      }

        ########################################################################
        # Print console feedback ###############################################
        ########################################################################

        # print messages to the console based on the anomalies detected
        if (isolated_outliers_count > 0) {
          if (interpolate) {
            cat(sprintf("%s outliers found: %d isolated values replaced using interpolation.\n", tools::toTitleCase(sensor.name), isolated_outliers_count))
          } else {
            cat(sprintf("%s outliers found: %d isolated values converted to NA.\n", tools::toTitleCase(sensor.name), isolated_outliers_count))
          }
        }
        if (malfunction_blocks_count > 0) {
          if(malfunction_duration<60){
            cat(sprintf("%s anomalies found: %d values converted to NA (-%.1f secs).\n", tools::toTitleCase(sensor.name), malfunction_blocks_count, malfunction_duration))
          }else if(malfunction_duration<3600){
            cat(sprintf("%s anomalies found: %d values converted to NA (-%.1f mins).\n", tools::toTitleCase(sensor.name), malfunction_blocks_count, malfunction_duration/60))
          }else{
            cat(sprintf("%s anomalies found: %d values converted to NA (-%.1f hours).\n", tools::toTitleCase(sensor.name), malfunction_blocks_count, malfunction_duration/3600))
          }
        }


      ##########################################################################
      # Plot anomalies #########################################################
      ##########################################################################

      # downsample the data to 1 Hz to speed up plotting and reduce memory usage
      # high-frequency data can overwhelm plotting functions, especially for large datasets.
      # downsampling ensures that the plot remains responsive and visually clear.

      # check if the sampling frequency is greater than 1 Hz and downsample is required
      if(sampling_freq > 1){

        # temporarily suppress console output (redirect to a temporary file)
        sink(tempfile())

        # downsample to 1 Hz by rounding datetime to the nearest second and selecting the more extreme sensor value
        plot_data <- plot_data[, .(sensor = get(sensor.col)[which.max(abs(get(sensor.col)))]),
                                    by = .(datetime = lubridate::round_date(get(datetime.col), "second"))]

        # correct the column names to match the original dataset
        data.table::setnames(plot_data, old = names(plot_data), new =  c(datetime.col, sensor.col))

        # restore normal console output
        sink()

        # adjust the flagged_outliers and interpolated_outliers indices to match the downsampled data
        if (length(flagged_outliers) > 0) {
          flagged_outliers <- which(plot_data[[datetime.col]] %in% lubridate::round_date(individual_data[[datetime.col]][flagged_outliers], "second"))
        }
        if (length(interpolated_outliers) > 0) {
          interpolated_outliers <- which(plot_data[[datetime.col]] %in% lubridate::round_date(individual_data[[datetime.col]][interpolated_outliers], "second"))
        }
      }

      # set par
      par(mar=c(4,4,3,1), mgp=c(2.8, 0.7, 0))

      # define y-axis limits (flipping for depth)
      ylim <- if (grepl("depth", sensor.name, ignore.case = TRUE)) {
        rev(range(plot_data[[sensor.col]], na.rm = TRUE))
      } else {
        range(plot_data[[sensor.col]], na.rm = TRUE)
      }

      # set up empty plot with appropriate y-axis limits
      plot(x=plot_data[[datetime.col]], y=plot_data[[sensor.col]], type="n",
           ylim=ylim, main=id, xlab="", ylab="", cex.main=0.9, cex.axis=0.8, las=1)
      # add a background shaded rectangle
      rect(par("usr")[1], par("usr")[3], par("usr")[2], par("usr")[4], col="grey97", border=NA)
      # plot the sensor time series
      lines(x=plot_data[[datetime.col]], y=plot_data[[sensor.col]], lwd=0.6)
      points(x=plot_data[[datetime.col]], y=plot_data[[sensor.col]], pch=16, col="darkblue", cex=0.5)

      # add axes titles
      title(ylab=sensor.name, cex.lab=0.9)
      title(xlab="Time", line=1.8, cex.lab=0.9)

      # highlight removed outliers in red
      if(length(flagged_outliers)>0){
        points(x=plot_data[[datetime.col]][flagged_outliers], y=plot_data[[sensor.col]][flagged_outliers],
               pch=16, col="red3", cex=0.8)
      }

      # highlight interpolated values in green
      if(length(interpolated_outliers)>0){
        points(x=plot_data[[datetime.col]][interpolated_outliers], y=plot_data[[sensor.col]][interpolated_outliers],
               pch=16, col="cyan3", cex=0.8)
      }

      # add legend
      legend("top", inset=c(0, -0.1), legend=c("interpolated", "removed"),
             col=c("cyan3", "red3"), pch=16, bty="n", horiz=TRUE, cex=0.8, xpd=NA)


      ##########################################################################
      # Revert data back to its original class and save ########################
      ##########################################################################

      # revert back to original data class
      if ("data.table" %in% original_class) {
        individual_data <- data.table::as.data.table(individual_data)
      }

      # save the processed data
      data[[i]] <- individual_data

      # force garbage collection after processing each dataset
      gc()
    }
  }

  ##############################################################################
  # Return data ################################################################
  ##############################################################################

  # print a message if no outliers were found
  if(no_errors_found) cat("All good in the hood! No anomalies detected.\n")

  # return cleaned data
  return(data)
}



################################################################################
# Define helper function to identify sensor blocks surrounded by NAs ###########
################################################################################

# Helper function to identify islands (non-NA sequences surrounded by NAs, ignoring leading/trailing NAs)

#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.findIsolated <- function(vec) {

  # run-length encoding of NA values
  rle_na <- rle(is.na(vec))

  # identify positions where non-NA values are surrounded by NA values
  island_positions <- which(
    !rle_na$values &                     # current value is non-NA
      c(FALSE, rle_na$values[-length(rle_na$values)]) &  # previous value is NA (ignore start)
      c(rle_na$values[-1], FALSE)          # next value is NA (ignore end)
  )

  # create a logical vector indicating whether each value is part of an island
  is_island <- rep(FALSE, length(vec))
  for (pos in island_positions) {
    start <- sum(rle_na$lengths[1:(pos - 1)]) + 1
    end <- start + rle_na$lengths[pos] - 1
    is_island[start:end] <- TRUE
  }

  return(is_island)
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
