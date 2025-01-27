#######################################################################################################
# Function to annotate dataset ########################################################################
#######################################################################################################

#' Annotate Dataset with Events or Behaviors of Interest
#'
#' This function annotates a list of individual data frames (or a single aggregated data frame) with event or behavior information.
#' It links time-bound events (e.g., feeding, social interactions) to sensor data collected from individual animals,
#' with the event annotations typically derived from a detailed review of footage recorded by camera tags.
#' The function scans the dataset(s) for each animal, checking whether each row's timestamp falls within the defined time intervals of
#' specific events listed in the provided annotations data frame, marking rows that fall within the specified event or behavior
#' periods as `1` and those outside the periods as `0`.
#'
#' @param data A list of data tables/data frames, one for each individual, or a single aggregated data table/data frame containing data from multiple animals.
#' @param annotations A data frame containing the start and end times of events/behaviors (columns specified by `start.col` and `end.col`)
#' and an individual ID column (specified by `id.col`).
#' @param id.col A character string specifying the name of the column in both `data` and `annotations` containing animal IDs.
#' @param event.col A character string specifying
#' @param datetime.col A character string specifying the name of the column in `data` containing the datetime values.
#' @param start.col A character string specifying the name of the column in `annotations` containing event start times.
#' @param end.col A character string specifying the name of the column in `annotations` containing event end times.
#' @param selected.events A vector of event types to be annotated. If NULL (default), all events will be annotated.
#'
#' @return A list of data frames (one for each individual) with binary columns indicating the presence (1) or absence (0) of each specified event.
#' @export

annotateData <- function(data,
                         annotations,
                         id.col = "ID",
                         datetime.col = "datetime",
                         event.col = "event",
                         start.col = "start",
                         end.col = "end",
                         selected.events = NULL) {

  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

  # if 'data' is not a list, split it into a list of individual data sets
  if (!is.list(data)) {
    data <- split(data, f = data[[id.col]])
  }

  # check that col parameters exists in the annotations
  if (!id.col %in% colnames(annotations)) stop(paste0("The specified id.col ('", id.col, "') was not found in the provided annotations"), call. = FALSE)
  if (!event.col %in% colnames(annotations)) stop(paste0("The specified event.col ('", event.col, "') was not found in the provided annotations"), call. = FALSE)
  if (!start.col %in% colnames(annotations)) stop(paste0("The specified start.col ('", start.col, "') was not found in the provided annotations"), call. = FALSE)
  if (!end.col %in% colnames(annotations)) stop(paste0("The specified end.col ('", end.col, "') was not found in the provided annotations"), call. = FALSE)

  # check that 'start' and 'end' are of class POSIXct
  if (!inherits(annotations[[start.col]], "POSIXct") || !inherits(annotations[[end.col]], "POSIXct")) {
    stop("The 'start' and 'end' columns in annotations must be of class POSIXct.", call. = FALSE)
  }

  # check that id.col exists in all non-NULL data frames
  if (!all(sapply(data, function(df) is.null(df) || id.col %in% colnames(df)))) {
    stop(paste0("The specified id.col ('", id.col, "') was not found in all non-NULL data frames"), call. = FALSE)
  }

  # check that datetime.col exists in all non-NULL data frames
  if (!all(sapply(data, function(df) is.null(df) || datetime.col %in% colnames(df)))) {
    stop(paste0("The specified datetime.col ('", datetime.col, "') was not found in all non-NULL data frames"), call. = FALSE)
  }

  # check that the datetime column is of class POSIXct in all non-NULL data frames
  if (!all(sapply(data, function(df) is.null(df) || inherits(df[[datetime.col]], "POSIXct")))) {
    stop(paste("The specified datetime column", datetime.col, "must be of class POSIXct in all non-NULL data frames."), call. = FALSE)
  }

  # check if selected.events is NULL, a character vector, or empty
  if (!is.null(selected.events)) {
    if (!is.character(selected.events)) stop("The 'selected.events' argument must be a character vector.", call. = FALSE)
    # check that all selected events are valid event types present in the annotations
    valid_events <- unique(annotations$event)
    invalid_events <- setdiff(selected.events, valid_events)
    if (length(invalid_events) > 0) stop(paste("The following selected events are not found in the annotations: ", paste(invalid_events, collapse = ", ")), call. = FALSE)
  }


  ##############################################################################
  # Filter annotations by selected events #####################################
  ##############################################################################

  # if selected.events is not NULL, filter annotations to only include those events
  if (!is.null(selected.events)) {
    annotations <- annotations[annotations$event %in% selected.events, ]
  }


  ##############################################################################
  # Mark rows within event periods #############################################
  ##############################################################################

  # print a message to the console
  cat(crayon::bold("Processing Event Annotations\n"))

  # get the unique event types from the annotations data
  unique_events <- unique(annotations[[event.col]])

  # initialize a list to store the annotated data frames
  annotated_data <- vector("list", length(data))
  names(annotated_data) <- names(data)

  # loop through each data frame in the list
  for (i in seq_along(data)) {

    # extract the current dataset and its ID
    df <- data[[i]]
    id <- names(data)[i]

    # skip processing if the current data frame is NULL
    if(is.null(df)) next

    # filter annotations for the current individual
    id_annotations <- annotations[annotations[[id.col]] == id, ]

    # skip processing if there is no annotations for the current individual
    if(nrow(id_annotations)==0) next

    # loop through each unique event type
    for (event in unique_events) {
      # filter annotations for the current event type
      event_annotations <- id_annotations[id_annotations[[event.col]] == event, ]
      # skip processing if there is no annotations for the current event
      if(nrow(event_annotations)==0) next
      # initialize binary column with 0
      df[[event]] <- 0
      # initialize counter for current event
      event_count <- 0
      # initialize a cumulative counter for rows affected
      total_rows <- 0
      # check each event period
      for (j in 1:nrow(event_annotations)) {
        # extract the start and end for the current event
        start_time <- event_annotations[[start.col]][j]
        end_time <- event_annotations[[end.col]][j]
        # identify rows that fall within the event period
        condition <- df[[datetime.col]] >= start_time & df[[datetime.col]] <= end_time
        # check if any rows match the current event conditions
        rows_affected <- sum(condition)
        if (rows_affected > 0) {
          # increment the event count
          event_count <- event_count + 1
          # increment the total rows affected for this event type
          total_rows <- total_rows + rows_affected
          # mark rows within the event period
          df[[event]][condition] <- 1
        }
      }

      # print the feedback message for the current individual and events added
      if (event_count > 0) {
        cat(crayon::blue$bold(id), ": ", event_count,
            " event", if (event_count != 1) "s" else "",
            " added for event '", event, "' (", total_rows,
            " rows)\n", sep = "")
      }
    }

    # store the annotated data frame
    annotated_data[[i]] <- df
  }

  # remove NULL elements from the list
  annotated_data <- annotated_data[!sapply(annotated_data, is.null)]

  # return the annotated list of data frames
  return(annotated_data)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
