#######################################################################################################
# Function to filter data based on video availability #################################################
#######################################################################################################

#' Filter data based on video availability periods
#'
#' This function subsets a list of data.tables to match the periods where video footage is available,
#' based on metadata containing video start and end times. It can also adjust video end times
#' based on annotation end markers.
#'
#' @param data A \code{list} where each element is a \code{data.table} for a specific ID
#' (e.g., a specific camera or animal). Each dataset should include a column for datetimes.
#' @param video.metadata A \code{data.table} containing metadata for each video, with columns:
#' \code{ID} (unique identifier), \code{start} (start datetime), and \code{end} (end datetime).
#' @param annotation.ends A \code{data.table} containing adjusted end times for annotations,
#' with columns: \code{ID} (unique identifier) and \code{annotation_end} (datetime).
#' This table is used to override the 'end' time of the latest video segment for an ID.
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
                              annotation.ends = NULL, 
                              id.col = "ID",
                              datetime.col = "datetime") {
  
  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################
  
  # acceptable types: data.frame, data.table, or list of data.tables
  is_list <- is.list(data) && !is.data.frame(data)
  if (!(is_list || is.data.frame(data))) {
    stop("'data' must be a list, data.frame, or data.table.", call. = FALSE)
  }
  
  # if data is not a list, split it by ID and convert to list of data.tables
  if (!is_list) {
    if (!id.col %in% colnames(data)) {
      stop(sprintf("Column '%s' not found in input 'data'.", id.col), call. = FALSE)
    }
    data <- split(data, f = data[[id.col]])
  }
  
  # check that all data.tables contain the required columns
  missing_id_col <- any(!sapply(data, function(dt) id.col %in% colnames(dt)))
  missing_datetime_col <- any(!sapply(data, function(dt) datetime.col %in% colnames(dt)))
  if (missing_id_col) stop(sprintf("Column '%s' not found in at least one element of 'data'.", id.col), call. = FALSE)
  if (missing_datetime_col) stop(sprintf("Column '%s' not found in at least one element of 'data'.", datetime.col), call. = FALSE)
  
  
  # ensure datetime column is of class POSIXct in all elements
  invalid_datetime_class <- any(!vapply(data, function(dt) inherits(dt[[datetime.col]], "POSIXct"), logical(1)))
  if (invalid_datetime_class) {
    stop(sprintf("Column '%s' must be of class POSIXct in all elements of 'data'.", datetime.col), call. = FALSE)
  }
  
  # check that required columns exist in video.metadata
  required_cols <- c(id.col, "start", "end")
  missing_metadata_cols <- setdiff(required_cols, colnames(video.metadata))
  if (length(missing_metadata_cols) > 0) {
    stop(sprintf("'video.metadata' is missing the following required columns: %s",
                 paste(missing_metadata_cols, collapse = ", ")), call. = FALSE)
  }
  
  # ensure start and end columns are POSIXct
  if (!inherits(video.metadata$start, "POSIXct") || !inherits(video.metadata$end, "POSIXct")) {
    stop("Columns 'start' and 'end' in 'video.metadata' must be of class POSIXct.", call. = FALSE)
  }
  
  # check annotation.ends if provided
  if (!is.null(annotation.ends)) {
    if (!all(c(id.col, "annotation_end") %in% colnames(annotation.ends))) {
      stop("'annotation.ends' must contain columns 'ID' and 'annotation_end'.", call. = FALSE)
    }
    if (!inherits(annotation.ends$annotation_end, "POSIXct")) {
      stop("Column 'annotation_end' in 'annotation.ends' must be of class POSIXct.", call. = FALSE)
    }
  }
  
  ##############################################################################
  # Filter data ################################################################
  ##############################################################################
  
  # calculate number of unique datasets
  n_animals <- length(data)
  
  # feedback messages for the user
  cat(crayon::bold("Filtering data based on video availability\n"))
  
  # initialize an empty list to store filtered data
  data_subsetted <- vector("list", length(data))
  names(data_subsetted) <- names(data)
  
  
  # iterate over each dataset in the input list
  for (i in 1:n_animals) {
    
    # retrieve data for current ID
    data_individual <- data[[i]]
    
    # store original attributes before processing, excluding internal ones
    if(data.table::is.data.table(data_individual)){
      discard_attrs <- c("row.names", "class", ".internal.selfref", "names")
      original_attributes <- attributes(data_individual)
      original_attributes <- original_attributes[!names(original_attributes) %in% discard_attrs]
    }
    
    # retrieve animal ID
    id <- unique(data_individual[[id.col]])[1]
    
    # skip if no matching ID in video.metadata
    if (!id %in% video.metadata[[id.col]]) {
      cat(crayon::red(sprintf("Skipping ID %s: No video metadata found.\n", id)))
      next
    }
    
    # print message
    cat(crayon::blue$bold(id))
    
    # extract relevant video metadata for the current ID
    tag_videos <- video.metadata[video.metadata[[id.col]] == id, ]
    
    # Initialize an empty data.table to store segments for the current ID
    filtered_segments_for_id <- data.table::data.table()
    
    # Get annotation end for the current ID, if it exists
    current_annotation_end <- NULL
    if (!is.null(annotation.ends) && id %in% annotation.ends[[id.col]]) {
      current_annotation_end <- annotation.ends[annotation.ends[[id.col]] == id, "annotation_end", drop = TRUE]
    }
    
    # Iterate over each video segment for the current ID
    for (j in 1:nrow(tag_videos)) {
      video_start <- tag_videos$start[j]
      video_end <- tag_videos$end[j]
      
      # If this is the last video segment for the current ID
      # and an annotation end exists, adjust the video_end
      if (j == nrow(tag_videos) && !is.null(current_annotation_end)) {
        # Ensure the annotation_end does not extend beyond the original video_end
        video_end <- min(video_end, current_annotation_end)
      }
      
      # Subset the data.table using the current video segment's start and end times
      segment_data <- data_individual[data_individual[[datetime.col]] >= video_start &
                                        data_individual[[datetime.col]] <= video_end,]
      
      # If there's data in this segment, append it to the overall filtered data for the ID
      if (nrow(segment_data) > 0) {
        filtered_segments_for_id <- rbind(filtered_segments_for_id, segment_data)
      }
    }
    
    data_subsetted[[i]] <- filtered_segments_for_id
    
    # calculate the duration of the subsetted period
    if (nrow(data_subsetted[[i]]) > 0) {
      subsetted_period <- difftime(max(data_subsetted[[i]][[datetime.col]]), min(data_subsetted[[i]][[datetime.col]]))
      # print how much time was retained
      cat(sprintf(": %.1f %s retained\n", as.numeric(subsetted_period), attr(subsetted_period, "units")))
    } else {
      cat(": No data retained\n")
    }
  }
  
  
  # return results
  data_subsetted <- Filter(Negate(is.null), data_subsetted)
  return(data_subsetted)
}