#######################################################################################################
# Summarize Tag Data ##################################################################################
#######################################################################################################

#' Generate Summary Statistics for Multiple Individuals
#'
#' @description This function takes processed data from multiple individuals (either a list with a data.table/data.frame for each animal or an aggregated data.table/data.frame containing data from multiple animals)
#' and outputs a summary table with key statistics for each individual.
#'
#' @param data A list of data tables/data frames, one for each individual, or a single aggregated data table/data frame containing data from multiple animals.
#' @param id.metadata A data frame containing metadata about the tagged animals, such as their length,
#' sex, or transmitter type. All columns in this data frame will be summarized and included
#' in the final table. If there are multiple rows per animal, variables will be collapsed
#' before merging with other statistics.
#' @param id.col Column name for the animal ID (default is "ID").
#' @param datetime.col Column name for the datetime (default is "datetime").
#' @param error.stat The statistic to use for variability/error calculation, either 'sd' (standard deviation)
#' or 'se' (standard error). Defaults to 'sd'.

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
                             id.metadata = NULL,
                             id.col = "ID",
                             datetime.col="datetime",
                             error.stat = "sd") {

  ##############################################################################
  # Input validation ###########################################################
  ##############################################################################

  # measure running time
  start.time <- Sys.time()

  # if 'data' is not a list, split it into a list of individual data sets
  if (!is.list(data)) {
    data <- split(data, f = data[[id.col]])
  }

  # return early if 'data' is empty
  if (length(data) == 0) {
    return(data.frame())
  }

  # check if id.metadata contains id.col
  if(!is.null(id.metadata) && !id.col %in% colnames(id.metadata)) {
    stop("The specified ID column ('id.col') does not exist in 'id.metadata'. Please ensure that the column name in 'id.metadata' matches the 'id.col' specified.", call. = FALSE)
  }

  # check error function
  error.stat <- tolower(error.stat)
  if(!error.stat %in% c("sd", "se")) {
    stop("Wrong error.stat argument, please choose between 'sd' and 'se'.", call. = FALSE)
  }


  ##############################################################################
  # Generate summary ###########################################################
  ##############################################################################

  # output message to console
  .printConsole("Starting summary generation...")

  # ensure all elements of 'data' are data tables
  data <- lapply(data, function(x) {
    if (!data.table::is.data.table(x)) {x <- data.table::setDT(x)}
    return(x)
  })

  # retrieve number of individuals
  nids <- length(data)

  # initialize a list to store summary results for each individual dataset
  summary_list <- vector("list", nids)


  ##############################################################################
  # single-core processing #####################################################

  # initialize progress bar
  pb <- txtProgressBar(min=0, max=nids, initial=0, style=3)

  # iterate over each individual's data
  for (i in 1:nids) {

    # generate a summary for the current dataset
    summary_list[[i]] <- .summarize(data[[i]], id.col, datetime.col)

    # update progress bar
    setTxtProgressBar(pb, i)
  }

  # close progress bar
  close(pb)


  ##############################################################################
  # Format and return results ##################################################
  ##############################################################################

  # combine the list of summaries into a single data frame
  summary_table <- do.call(rbind, summary_list)

  # format additional tag info (if available) and merge
  if(!is.null(id.metadata)) {
    # check column types
    column_types <- sapply(1:ncol(id.metadata),function(c) class(id.metadata[,c]))
    # identify numeric or integer columns
    numeric_cols <- which(column_types %in% c("numeric", "integer"))
    # aggregate data
    animal_info <- stats::aggregate(id.metadata[,-1], by=list(id.metadata[,id.col]), function(x) paste(unique(x), collapse="/"), drop=FALSE)
    # replace "NA" strings with actual NA values
    animal_info[animal_info=="NA"] <- NA
    # convert numeric columns back to numeric
    for (col in numeric_cols) {animal_info[, col] <- as.numeric(unlist(animal_info[, col]))}
    colnames(animal_info)[1] <- "ID"
    summary_table <- plyr::join(animal_info, summary_table, by="ID", type="left")
  }

  # define error function
  getErrorFun <- function(x) {
    if(error.stat=="sd"){return(sd(x, na.rm=TRUE))}
    if(error.stat=="se"){return(.standardError(x))}
  }

  # identify the column types and find numeric/integer columns
  summary_table$ID <- as.character(summary_table$ID)
  column_types <- sapply(summary_table, class)
  numeric_cols <- which(column_types %in% c("numeric", "integer"))
  # exclude columns whose names contain "ptt"
  numeric_cols <- numeric_cols[!grepl("ptt", names(summary_table)[numeric_cols], ignore.case = TRUE)]
   # determine the number of decimal places for numeric columns
  decimal_digits <- apply(summary_table[,numeric_cols], 2, .decimalPlaces)
  # coerce `decimal_digits` into a matrix if it's not already one
  if(!is.matrix(decimal_digits)) decimal_digits <- matrix(decimal_digits, nrow = 1, dimnames = list(NULL, names(decimal_digits)))
  decimal_digits <- apply(decimal_digits, 2, max, na.rm=TRUE)
  # add a new row to store mean ± standard error (SE) values
  summary_table[nrow(summary_table)+1,] <- NA
  summary_table$ID[nrow(summary_table)] <- "mean"
  # format numeric columns in the new row to include the mean and SE values
  summary_table[nrow(summary_table), numeric_cols] <- sprintf(paste0("%.", decimal_digits, "f"), colMeans(summary_table[,numeric_cols, drop=FALSE], na.rm=TRUE))
  errors <- sprintf(paste0("%.", decimal_digits, "f"), unlist(apply(summary_table[,numeric_cols, drop=FALSE], 2, getErrorFun)))
  summary_table[nrow(summary_table), numeric_cols] <- paste(summary_table[nrow(summary_table), numeric_cols], "\u00b1", errors)
  # format the remaining numeric rows in the table with appropriate decimal places
  for(c in numeric_cols) summary_table[-nrow(summary_table), c] <- sprintf(paste0("%.", decimal_digits[which(numeric_cols==c)], "f"),  as.numeric(summary_table[-nrow(summary_table), c]))
  # replace missing or invalid values with a placeholder ("-")
  summary_table[summary_table=="NA"] <- "-"
  summary_table[summary_table=="NaN \u00b1 NA"] <- "-"
  summary_table[is.na(summary_table)] <- "-"

  # print time taken
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  cat(crayon::bold("Total execution time:"), sprintf("%.02f", as.numeric(time.taken)), base::units(time.taken), "\n\n")

  # return summary table
  return(summary_table)

}



################################################################################
# Define helper function to summarize stats ####################################
################################################################################

#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.summarize <- function(data_individual, id.col, datetime.col){

  # skip iteration if data is missing or empty
  if (is.null(data_individual) || nrow(data_individual) == 0) return(NULL)

  # retrieve animal ID
  id <- unique(data_individual[[id.col]])

  # retrieve temporal statistics
  deploy_start <- min(data_individual[[datetime.col]], na.rm = TRUE)
  deploy_end <- max(data_individual[[datetime.col]], na.rm = TRUE)

  # se fast attribute access with data.table for temporal stats
  data_start <- if ("first.datetime" %chin% names(attributes(data_individual))) {
    attr(data_individual, "first.datetime")
  }else {
    deploy_start
  }
  data_end <- if ("last.datetime" %chin% names(attributes(data_individual))) {
    attr(data_individual, "last.datetime")
  } else {
    deploy_end
  }
  start_date <- strftime(deploy_start, "%d/%b/%Y %H:%M", tz="UTC")
  end_date <- strftime(deploy_end, "%d/%b/%Y %H:%M", tz="UTC")
  total_duration <-  round(as.numeric(difftime(data_end, data_start, units="hours")), 2)
  deploy_duration <- round(as.numeric(difftime(deploy_end, deploy_start, units="hours")), 2)

  # get maximum depth and temperature range
  mean_depth <- round(data_individual[, mean(.SD[[1]], na.rm = TRUE), .SDcols = "depth"], 0)
  max_depth <- round(data_individual[, max(.SD[[1]], na.rm = TRUE), .SDcols = "depth"], 0)
  mean_temp <- round(data_individual[, mean(.SD[[1]], na.rm = TRUE), .SDcols = "temp"], 1)
  min_temp <- round(data_individual[, min(.SD[[1]], na.rm = TRUE), .SDcols = "temp"], 1)
  max_temp <- round(data_individual[, max(.SD[[1]], na.rm = TRUE), .SDcols = "temp"], 1)

  # extract attributes with fallback to NA if not available
  sampling_freq <- if ("original.sampling.frequency" %chin% names(attributes(data_individual))) {
    sprintf("%.0f", attr(data_individual, "original.sampling.frequency"))
  } else {
    NA_character_
  }
  declination_deg <- if ("magnetic.declination" %chin% names(attributes(data_individual))) {
    sprintf("%.2f", attr(data_individual, "magnetic.declination"))
  } else {
    NA_character_
  }

  # count positions
  fastloc_positions <- data_individual[get("position_type") == "FastGPS", .N]
  user_positions <- data_individual[get("position_type") == "User", .N]


  # merge summary data into a data frame
  summary <- data.frame(
    "ID" = id,
    "Deploy start" = start_date,
    "Deploy end" = end_date,
    "Total duration (h)" = total_duration,
    "Deploy duration (h)" = deploy_duration,
    "Sampling freq (Hz)" = sampling_freq,
    "Magnetic declination" = declination_deg,
    "Mean Depth (m)" = mean_depth,
    "Max Depth (m)" = max_depth,
    "Mean Temp (\u00BAC)" = mean_temp,
    "Min Temp (\u00BAC)" = min_temp,
    "Max Temp (\u00BAC)" = max_temp,
    "Fastloc GPS"=fastloc_positions,
    "User Locs"=user_positions,
    check.names = FALSE
  )

  # return results
  return(summary)
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
