#######################################################################################################
# Summarize Tag Data ##################################################################################
#######################################################################################################

#' Generate Summary Statistics for Multiple Individuals
#'
#' @description This function takes processed data from multiple individuals (either a list with a data.table/data.frame for each animal or an aggregated data.table/data.frame containing data from multiple animals)
#' and outputs a summary table with key statistics for each individual.
#'
#' @param data A list of data tables/data frames, one for each individual, or a single aggregated data table/data frame containing data from multiple animals.
#' @param id.col Column name for the animal ID (default is "ID").
#' @param datetime.col Column name for the datetime (default is "datetime").
#'
#' @return A data frame with summary statistics for each animal, including the following columns:
#' \itemize{
#'   \item \strong{ID}: The unique identifier for each individual animal.
#'   \item \strong{Total duration (h)}: The total dataset duration, in hours (including pre- and post-deployment periods).
#'   \item \strong{Deploy start}: The estimated start (attachment) datetime of the deployment.
#'   \item \strong{Deploy end}: The estimated end (pop-up) datetime of the deployment.
#'   \item \strong{Deploy duration (h)}: The duration of the deployment, in hours, calculated from the deployment start to deployment end.
#'   \item \strong{Sampling freq (Hz)}: The sampling frequency of the original dataset in Hertz (Hz)..
#'   \item \strong{Magnetic declination}: The magnetic declination value used to correct heading estimates.
#'   \item \strong{Max Depth (m)}: The maximum depth recorded during the deployment, in meters.
#'   \item \strong{Temp range (ºC)}: The temperature range (minimum and maximum temperatures recorded) during the deployment, in degrees Celsius (°C).
#'   \item \strong{Fastloc GPS}: The number of "Fast GPS" positions recorded during the deployment (if any).
#'   \item \strong{User Locs}: The number of user-defined GPS positions recorded during the deployment (if any).
#' }
#'
#'
#'@seealso \link{processTagData}, \link{filterDeploymentData}.

#' @export

summarizeTagData <- function(data,
                             id.col = "ID",
                             datetime.col="datetime") {

  ##############################################################################
  # Input validation ###########################################################
  ##############################################################################

  # if 'data' is not a list, split it into a list of individual data sets
  if (!is.list(data)) {
    data <- split(data, f = data[[id.col]])
  }

  # return early if 'data' is empty
  if (length(data) == 0) {
    return(data.frame())
  }



  ##############################################################################
  # Generate summary ###########################################################
  ##############################################################################

  # initialize the summary list
  summary_list <- list()

  # iterate over each individual's data
  for (i in 1:length(data)) {

    # extract individual data
    data_individual <- data[[i]]

    # get temporal statistics
    id <- unique(data_individual[[id.col]])  # Assuming each individual has a unique ID
    deploy_start <- min(data_individual[[datetime.col]])
    deploy_end <- max(data_individual[[datetime.col]])
    data_start <- ifelse("first.datetime" %in% names(attributes(data_individual)), attributes(data_individual)$first.datetime, deploy_start)
    data_end <- ifelse("last.datetime" %in% names(attributes(data_individual)), attributes(data_individual)$last.datetime, deploy_end)
    start_date <- strftime(deploy_start, "%d/%b/%Y %H:%M", tz="UTC")
    end_date <- strftime(deploy_end, "%d/%b/%Y %H:%M", tz="UTC")
    total_duration <-  sprintf("%.1f", difftime(data_end, data_start, units="hours"))
    deploy_duration <- sprintf("%.1f", difftime(deploy_end, deploy_start, units="hours"))

    # get maximum depth and temperature range
    max_depth <- round(max(data_individual$depth, na.rm = TRUE))
    temp_range <- paste0(sprintf("%.1f", range(data_individual$temp, na.rm = TRUE)), collapse=" - ")
    # extract attributes with fallback to NA if not available
    sampling_freq <- ifelse("sampling.frequency" %in% names(attributes(data_individual)), attributes(data_individual)$sampling.frequency, NA)
    declination_deg <- ifelse("magnetic.declination" %in% names(attributes(data_individual)), attributes(data_individual)$magnetic.declination, NA)

    # get PSAT locations
    fastloc_positions <- length(which(!is.na(data_individual$position_type=="FastGPS")))
    user_positions <- length(which(!is.na(data_individual$position_type=="User")))

    # create a row for each individual
    summary_list[[i]] <- data.frame(
      "ID" = id,
      "Total duration (h)" = total_duration,
      "Deploy start" = start_date,
      "Deploy end" = end_date,
      "Deploy duration (h)" = deploy_duration,
      "Sampling freq (Hz)" = sampling_freq,
      "Magnetic declination" = declination_deg,
      "Max Depth (m)" = max_depth,
      "Temp range (\u00BAC)" = temp_range,
      "Fastloc GPS"=fastloc_positions,
      "User Locs"=user_positions,
      check.names = FALSE
    )
  }

  ##############################################################################
  # Return results #############################################################
  ##############################################################################

  # Combine the list of summaries into a single data frame
  summary_table <- do.call(rbind, summary_list)

  # Return summary table
  return(summary_table)

}

#######################################################################################################
#######################################################################################################
#######################################################################################################

