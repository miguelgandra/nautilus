#######################################################################################################
# Function to annotate dataset ########################################################################
#######################################################################################################

#' Annotate Dataset with Events or Behaviors of Interest
#'
#' This function takes an original dataset and a data frame with event start and end times, then creates a new binary column
#' in the dataset, which marks rows that fall within the specified event or behavior periods as `1`, and others as `0`.
#'
#' @param data A data frame containing the original dataset with a `datetime` column.
#' @param event.data A data frame containing the start and end times of events/behaviors (in `POSIXct` format).
#' @param column.name A character string specifying the name for the new binary column.
#' @param datetime.col A character string specifying the name of the column in `data` containing the datetime values.

#' @return A data frame with an additional binary column indicating whether each row falls within the specified event/behavior.


annotateData <- function(data, event.data, column.name = "event", datetime.col = "datetime") {

  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

  # ensure that event.data has the correct column names and types
  if (!all(c("start", "end") %in% colnames(event.data))) {
    stop("The event.data data frame must have 'start' and 'end' columns.", call. = FALSE)
  }

  # check that 'start' and 'end' are of class POSIXct
  if (!inherits(event.data$start, "POSIXct") || !inherits(event.data$end, "POSIXct")) {
    stop("The 'start' and 'end' columns in event.data must be of class POSIXct.", call. = FALSE)
  }

  # check that datetime.col exists in the original dataset
  if (!datetime.col %in% colnames(data)) {
    stop("The specified datetime column does not exist in the data.", call. = FALSE)
  }

  # check that the datetime column is of class POSIXct
  if (!inherits(data[[datetime.col]], "POSIXct")) {
    stop(paste("The specified datetime column", datetime.col, "must be of class POSIXct."), call. = FALSE)
  }


  ##############################################################################
  # Mark rows within event periods #############################################
  ##############################################################################

  # initialize the new column with 0 (no event)
  data[[column.name]] <- 0

  # loop through each event and mark the relevant rows in the dataset
  for (i in 1:nrow(event.data)) {
    event_start <- event.data$start[i]
    event_end <- event.data$end[i]

    # mark rows within the event period as 1
    data[[column.name]] <- ifelse(data[[datetime.col]] >= event_start & data[[datetime.col]] <= event_end, 1, data[[column.name]])
  }

  # convert column to factor
  data[[column.name]] <- as.factor(data[[column.name]])

  # return data
  return(data)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
