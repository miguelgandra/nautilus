#######################################################################################################
# Function to filter data based on video availability and annotation intervals #########################
#######################################################################################################

#' Filter data based on video availability and annotation intervals
#'
#' @description
#' This function subsets a list of \code{data.table} objects in two steps:
#' 1. First retains only periods where video footage was available (from video.metadata)
#' 2. Then optionally further restricts to periods with manual annotations (from annotation.intervals)
#'
#' The function handles temporal jumps between videos by processing each video segment separately.
#' Annotation intervals are only applied after establishing video-available periods.
#'
#' For annotation intervals, missing \code{start} or \code{end} values are automatically filled with:
#' \itemize{
#'   \item the earliest video start time for missing \code{start} values
#'   \item the latest video end time for missing \code{end} values
#' }
#'
#' @param data A \code{list} where each element is a \code{data.table} for a specific ID
#' (e.g., a specific camera or animal). Each dataset should include a column for datetimes.
#' @param video.metadata A \code{data.table} containing metadata for each video segment, with columns:
#' \code{ID} (unique identifier), \code{start} (start datetime), and \code{end} (end datetime).
#' @param annotation.intervals An optional \code{data.frame} or \code{data.table} containing annotation intervals
#' with columns: \code{ID}, \code{start}, and \code{end} (all datetimes). If \code{NULL} (default), 
#' filtering is based solely on video availability.
#' @param id.col A \code{character} string indicating the column name in \code{video.metadata} and
#' \code{annotation.intervals} that contains the unique IDs matching the \code{data} list names. Default is \code{"ID"}.
#' @param datetime.col A \code{character} string indicating the name of the datetime column
#' in each dataset. Default is \code{"datetime"}.
#'
#' @return A \code{list} of filtered \code{data.table} objects, where each dataset contains only the rows
#' falling within both the video availability periods and the annotation intervals (if provided).
#' If no matching periods are found for a given ID, an empty \code{data.table} is returned.
#'
#' @export

filterVideoPeriod <- function(data,
                              video.metadata,
                              annotation.intervals = NULL,
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
  missing_datetime_col <- any(!sapply(data, function(dt) datetime.col %in% colnames(dt)))
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
  
  # check annotation.intervals if provided
  if (!is.null(annotation.intervals)) {
    # Allow data.frame or data.table
    req_annot_cols <- c(id.col, "start", "end")
    if (!all(req_annot_cols %in% colnames(annotation.intervals))) {
      stop(sprintf("'annotation.intervals' must contain columns: %s",
                   paste(req_annot_cols, collapse = ", ")), call. = FALSE)
    }
    if (!inherits(annotation.intervals$start, "POSIXct") ||
        !inherits(annotation.intervals$end, "POSIXct")) {
      stop("Columns 'start' and 'end' in 'annotation.intervals' must be of class POSIXct.", call. = FALSE)
    }
  }
  
  ##############################################################################
  # Filter data ################################################################
  ##############################################################################
  
  # verbose message
  cat(crayon::bold("Filtering data based on video availability\n"))
  
  # initialize an empty list to store filtered data
  data_subsetted <- vector("list", length(data))
  names(data_subsetted) <- names(data)
  
  # iterate over each dataset in the input list
  for (i in seq_along(data)) {
    
    # retrieve data for current ID
    data_individual <- data[[i]]
    id <- names(data)[i]
    
    # skip if no matching ID in video.metadata
    if (!id %in% video.metadata[[id.col]]) {
      cat(crayon::red(sprintf("Skipping ID %s: No video metadata found.\n", id)))
      next
    }
    
    cat(crayon::blue$bold(id))
    
    # extract relevant video metadata for the current ID
    tag_videos <- video.metadata[video.metadata[[id.col]] == id, ]
    
    # first filter: extract only video-covered periods
    video_filtered <- data.table::data.table()
    for (j in 1:nrow(tag_videos)) {
      video_start <- tag_videos$start[j]
      video_end <- tag_videos$end[j]
      
      segment_data <- data_individual[data_individual[[datetime.col]] >= video_start &
                                        data_individual[[datetime.col]] <= video_end, ]
      
      if (nrow(segment_data) > 0) {
        video_filtered <- rbind(video_filtered, segment_data)
      }
    }
    
    # if no video data found, skip to next ID
    if (nrow(video_filtered) == 0) {
      cat(": No video data retained\n")
      next
    }
    
    # second filter: apply annotation intervals if provided
    if (!is.null(annotation.intervals) && id %in% annotation.intervals[[id.col]]) {
      annot_periods <- annotation.intervals[annotation.intervals[[id.col]] == id, ]
      
      # fix missing starts/ends
      if (any(is.na(annot_periods$start))) {
        earliest_video <- min(video_filtered[[datetime.col]])
        annot_periods$start[is.na(annot_periods$start)] <- earliest_video
      }
      if (any(is.na(annot_periods$end))) {
        latest_video <- max(video_filtered[[datetime.col]])
        annot_periods$end[is.na(annot_periods$end)] <- latest_video
      }
      
      # filter data within annotation periods
      annotation_filtered <- data.table::data.table()
      for (k in 1:nrow(annot_periods)) {
        ann_start <- annot_periods$start[k]
        ann_end <- annot_periods$end[k]
        
        # find overlap between annotation period and video-filtered data
        annot_data <- video_filtered[video_filtered[[datetime.col]] >= ann_start &
                                       video_filtered[[datetime.col]] <= ann_end, ]
        
        if (nrow(annot_data) > 0) {
          annotation_filtered <- rbind(annotation_filtered, annot_data)
        }
      }
      
      data_subsetted[[i]] <- annotation_filtered
    } else {
      # no annotation intervals to apply
      data_subsetted[[i]] <- video_filtered
    }
    
    # print retention message
    if (nrow(data_subsetted[[i]]) > 0) {
      
      # calculate sampling interval (time between consecutive observations)
      time_diff <- as.numeric(difftime(data_subsetted[[i]][[datetime.col]][2], 
                                       data_subsetted[[i]][[datetime.col]][1], 
                                       units = "secs"))
      
      # calculate total coverage duration (number of observations * sampling interval)
      total_coverage <- nrow(data_subsetted[[i]]) * time_diff
      
      cat(sprintf(": %s retained\n", .formatDuration(total_coverage)))
    } else {
      cat(": No data retained\n")
    }
  }
  
  # remove NULL elements and return
  data_subsetted <- Filter(Negate(is.null), data_subsetted)
  return(data_subsetted)
}