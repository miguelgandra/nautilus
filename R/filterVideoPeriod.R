#######################################################################################################
# Function to filter data based on video availability #################################################
#######################################################################################################


#' Filter data based on video availability periods
#'
#' This function subsets a list of data.tables to match the periods where video footage is available,
#' based on metadata containing video start and end times.
#'
#' @param data A \code{list} where each element is a \code{data.table} for a specific ID
#' (e.g., a specific camera or animal). Each dataset should include a column for datetimes.
#' @param video.metadata A \code{data.table} containing metadata for each video, with columns:
#' \code{ID} (unique identifier), \code{start} (start datetime), and \code{end} (end datetime).
#' @param id.col A \code{character} string indicating the column name in \code{video.metadata}
#' that contains the unique IDs matching the \code{data} list names. Default is \code{"ID"}.
#' @param datetime.col A \code{character} string indicating the name of the datetime column
#' in each dataset. Default is \code{"datetime"}.
#'
#' @return A list of filtered \code{data.table} objects, where each dataset contains only the rows
#' that fall within the video availability periods specified in \code{video.metadata}.
#' If no matching periods are found for a given ID, an empty \code{data.table} is returned.
#' @export


filterVideoPeriod <- function(data,
                              video.metadata,
                              id.col = "ID",
                              datetime.col = "datetime") {
  
  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################
  
  # if 'data' is not a list, split it into a list of individual data sets based on 'id.col'
  if (!is.list(data)) {
    data <- split(data, f = data[[id.col]])
  }
  
  # check that id.col exists in the original dataset
  if (!id.col %in% colnames(data[[1]])) {
    stop("The specified ID column does not exist in the data.", call. = FALSE)
  }
  
  # check that datetime.col exists in the original dataset
  if (!datetime.col %in% colnames(data[[1]])) {
    stop("The specified datetime column does not exist in the data.", call. = FALSE)
  }
  
  # check that the datetime column is of class POSIXct
  if (!inherits(data[[1]][[datetime.col]], "POSIXct")) {
    stop(paste("The specified datetime column", datetime.col, "must be of class POSIXct."), call. = FALSE)
  }
  
  # check required columns in video.metadata
  required_cols <- c(id.col, "start", "end")
  if (!all(required_cols %in% colnames(video.metadata))) {
    stop(sprintf("The 'video.metadata' must contain the following columns: %s", 
                 paste(required_cols, collapse = ", ")))
  }
  
  # ensure 'start' and 'end' columns are in datetime format
  if (!inherits(video.metadata$start, "POSIXct") || !inherits(video.metadata$end, "POSIXct")) {
    stop("'start' and 'end' columns in 'video.metadata' must be in POSIXct format.")
  }
  
  ##############################################################################
  # Filter data ################################################################
  ##############################################################################
  
  # calculate number of unique datasets
  n_animals <- length(data)
  
  # feedback messages for the user
  .printConsole("Filtering data based on video availability")
  
  # initialize an empty list to store filtered data
  data_subsetted <- vector("list", length(data))
  
  # iterate over each dataset in the input list
  for (i in 1:n_animals) {
  
    # retrieve data for current ID
    data_individual <- data[[i]]
    
    # store original attributes before processing,  excluding internal ones
    discard_attrs <- c("row.names", "class", ".internal.selfref", "names")
    original_attributes <- attributes(data_individual)
    original_attributes <- original_attributes[!names(original_attributes) %in% discard_attrs]
    
    # retrieve animal ID
    id <- original_attributes$id
    names(data_subsetted)[i] <- id
    
    # skip if no matching ID in video.metadata
    if (!id %in% video.metadata[[id.col]]) {next}
    
    # print message
    cat(crayon::blue$bold(id))
    
    # extract relevant video metadata for the current ID
    tag_videos <- video.metadata[video.metadata[[id.col]] == id, ]
    
    # subset the data.table using the video start and end times
    data_subsetted[[i]] <- data_individual[data_individual[[datetime.col]] >= min(tag_videos$start) & data_individual[[datetime.col]] <= max(tag_videos$end),]
    
    # calculate the duration of the subsetted period
    subsetted_period <- difftime(max(data_subsetted[[i]][[datetime.col]]), min(data_subsetted[[i]][[datetime.col]]))
    
    # print how much time was retained
    cat(sprintf(": %.1f %s retained\n", as.numeric(subsetted_period), attr(subsetted_period, "units")))
  }
    
  
  # return results
  return(data_subsetted)
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
