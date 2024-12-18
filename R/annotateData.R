#######################################################################################################
# Function to annotate dataset ########################################################################
#######################################################################################################

#' Annotate Dataset with Events or Behaviors of Interest
#'
#' This function takes an original dataset and a data frame with event start and end times, along with individual IDs,
#' then creates a new binary column in the dataset which marks rows that fall within the specified event or behavior
#' periods as `1` and others as `0`.
#'
#' @param data A data frame containing the original dataset with a `datetime` column.
#' @param annotations A data frame containing the start and end times of events/behaviors (columns specified by `start.col` and `end.col`)
#' and an individual ID column (specified by `id.col`).
#' @param event.name A character string specifying the name for the new binary column.
#' @param id.col A character string specifying the name of the column in both `data` and `annotations` containing animal IDs.
#' @param datetime.col A character string specifying the name of the column in `data` containing the datetime values.
#' @param start.col A character string specifying the name of the column in `annotations` containing event start times.
#' @param end.col A character string specifying the name of the column in `annotations` containing event end times.
#'
#' @return A data frame with an additional binary column indicating whether each row falls within the specified event/behavior.
#' @export

annotateData <- function(data,
                         annotations,
                         event.name = "event",
                         id.col = "ID",
                         datetime.col = "datetime",
                         start.col = "start",
                         end.col = "end") {

  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

  # ensure that annotations has the correct column names and types
  if (!all(c("start", "end") %in% colnames(annotations))) {
    stop("The annotations data frame must have 'start' and 'end' columns.", call. = FALSE)
  }

  # ensure that start.col and end.col exist in the annotations
  if (!start.col %in% colnames(annotations)) {
    stop(paste0("The specified start.col ('", start.col, "') was not found in the provided annotations"), call. = FALSE)
  }
  if (!end.col %in% colnames(annotations)) {
    stop(paste0("The specified end.col ('", end.col, "') was not found in the provided annotations"), call. = FALSE)
  }

  # check that id.col exists in the annotations
  if (!id.col %in% colnames(annotations)) {
    stop(paste0("The specified id.col ('", id.col, "') was not found in the provided annotations"), call. = FALSE)
  }

  # check that 'start' and 'end' are of class POSIXct
  if (!inherits(annotations[[start.col]], "POSIXct") || !inherits(annotations[[end.col]], "POSIXct")) {
    stop("The 'start' and 'end' columns in annotations must be of class POSIXct.", call. = FALSE)
  }

  # check that id.col exists in the original dataset
  if (!id.col %in% colnames(data)) {
    stop(paste0("The specified id.col ('", id.col, "') was not found in the provided data"), call. = FALSE)
  }

  # check that datetime.col exists in the original dataset
  if (!datetime.col %in% colnames(data)) {
    stop(paste0("The specified datetime.col ('", datetime.col, "') was not found in the provided data"), call. = FALSE)
  }

  # check that the datetime column is of class POSIXct
  if (!inherits(data[[datetime.col]], "POSIXct")) {
    stop(paste("The specified datetime column", datetime.col, "must be of class POSIXct."), call. = FALSE)
  }


  ##############################################################################
  # Mark rows within event periods #############################################
  ##############################################################################

  # initialize the new column with 0 (no event)
  data[[event.name]] <- 0

  # loop through each event and mark the relevant rows in the dataset
  for (i in 1:nrow(annotations)) {
    event_start <- annotations[[start.col]][i]
    event_end <- annotations[[end.col]][i]
    event_id <- annotations[[id.col]][i]

    # identify rows matching the current ID and within the event period
    condition <- (data[[id.col]] == event_id) & (data[[datetime.col]] >= event_start & data[[datetime.col]] <= event_end)

    # mark rows meeting the condition
    data[[event.name]] <- ifelse(condition, 1, data[[event.name]])
  }

  # convert column to factor
  data[[event.name]] <- as.factor(data[[event.name]])

  # return data
  return(data)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
