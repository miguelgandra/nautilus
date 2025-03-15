


plotPredictions <- function(rf.model,
                            data,
                            features,
                            annotations = NULL,
                            video.metadata,
                            id.col = "ID",
                            datetime.col = "datetime",
                            depth.col = "depth") {
  
  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################
  
  # if 'data' is not a list, split it into a list of individual data sets based on 'id.col'
  if (!is.list(data)) {
    data <- split(data, f = data[[id.col]])
  }
  
  # check if specified columns exist in the data
  if(!id.col %in% names(data[[1]])) stop(paste0("The specified id.col ('", id.col, "') was not found in the supplied data."), call. = FALSE)
  if(!datetime.col %in% names(data[[1]])) stop(paste0("The specified datetime.col ('", datetime.col, "') was not found in the supplied data."), call. = FALSE)
  if(!depth.col %in% names(data[[1]])) stop(paste0("The specified depth.col ('", depth.col, "') was not found in the supplied data."), call. = FALSE)
  
  # ensure datetime column is of POSIXct class
  if (!inherits(data[[1]][[datetime.col]], "POSIXct")) {
    stop(sprintf("The '%s' column must be of class 'Date' or 'POSIXct'.", datetime.col))
  }
  
  ##############################################################################
  #  #############################################################
  ##############################################################################
  
  # retrieve animal ID
  id <- unique(data[[id.col]])

  # retrieve response column name
  response_col <- as.character(rf.model$call$formula[[2]])
  
  # make predictions using the trained Random Forest model
  predictions <- predict(rf.model, newdata = features, type = "response")
  
  # identify contiguous periods of predicted feeding events
  feeding_rle <- rle(as.numeric(as.character(predictions)))
  feeding_periods <- which(feeding_rle$values == 1) # Assuming 1 indicates feeding events
  
  # initialize an empty list for feeding ranges
  feeding_ranges <- list()
  
  # loop through each feeding period and extract start and end indices
  current_end <- 0
  for (i in seq_along(feeding_rle$lengths)) {
    length_segment <- feeding_rle$lengths[i]
    start <- current_end + 1
    end <- current_end + length_segment
    current_end <- end
    # check if this is a feeding period
    if (feeding_rle$values[i] == 1) {
      feeding_ranges[[length(feeding_ranges) + 1]] <- list(start = start, end = end)
    }
  }

  # extract relevant video metadata for the current ID
  tag_videos <- video.metadata[video.metadata[[id.col]] == id, ]
  
  # extract relevant annotations for the current ID
  if(!is.na(annotations)){
    tag_annotations <- annotations[annotations[[id.col]] == id, ]
    tag_annotations <- tag_annotations[tag_annotations$event == response_col,]
  }
  
  # aalculate the depth range and add a 10% buffer on top
  depth_range <- range(data[[depth.col]], na.rm = TRUE)
  ylim <- c(depth_range[2], depth_range[1] - 0.22 * diff(depth_range))
  
  # calculate coords
  vid_bottom <- depth_range[1] - 0.17 * diff(depth_range)
  vid_top <- depth_range[1] - 0.21 * diff(depth_range)
  vid_center <- depth_range[1] - 0.19 * diff(depth_range)
  predicted_coord <- depth_range[1] - 0.12 * diff(depth_range)
  annotation_coord <- depth_range[1] - 0.07 * diff(depth_range)
  
  ##############################################################################
  # Generate plots #############################################################
  ##############################################################################
  
  # set layout params
  par(mar=c(4,6,3,1), mgp = c(2.5, 0.8, 0))
  
  # create plot 
  plot(x=data[[datetime.col]], y=data[[depth.col]], type="n", xlab="Date", ylab="Depth (m)",
       main=id, ylim=ylim, axes=FALSE, cex.main=1, cex.lab=0.9)
  
  # add a background shaded rectangle
  rect(par("usr")[1], par("usr")[3], par("usr")[2], par("usr")[4], col="grey97", border=NA)
  # plot the depth time series
  lines(x=data[[datetime.col]], y=data[[depth.col]], lwd=0.8)
  # add date axis
  date_labs <- pretty(range(data$datetime), min.n = 5)
  axis(1, at=date_labs, labels=format(date_labs, "%H:%M", tz="UTC"), cex.axis = 0.9)
  # add depth axis
  depth_vals <- pretty(c(0, depth_range[2]))
  depth_vals <- depth_vals[depth_vals<=depth_range[2]]
  axis(2, at=depth_vals, labels=depth_vals, las=1, cex.axis = 0.9)
  # draw surface line
  abline(h=0, lty=3)
  
  # highlight predicted feeding events in red
  for (range in feeding_ranges) {
    lines(x = data[[datetime.col]][range$start:range$end], y = data[[depth.col]][range$start:range$end], 
          col = "orangered", lwd = 1.5)
    points(x = data[[datetime.col]][range$start:range$end],  y = rep(predicted_coord, length(range$start:range$end)),
           col = "orangered", pch = 16, cex = 0.9)
    axis(2, at=predicted_coord, labels="pred. feeding", las=1, cex.axis=0.8)
  }

  # add points for annotated feeding events
  if(nrow(tag_annotations)>0){
    for (i in seq_len(nrow(tag_annotations))) {
      annot_range <- tag_annotations$start[i] : tag_annotations$end[i]
      lines(x = annot_range, y = data[[depth.col]][  which(data[[datetime.col]] %in% annot_range)], col = "aquamarine4", lwd = 1.5)
      points(x = annot_range,  y = rep(annotation_coord, length(annot_range)), col = "aquamarine4", pch = 16, cex = 0.9)
      axis(2, at=annotation_coord, labels="obs. feeding", las=1, cex.axis=0.8)
    }
  }
  
  # draw rectangles for video footage availability
  if (nrow(tag_videos) > 0) {
    for (i in seq_len(nrow(tag_videos))) {
      rect(xleft = tag_videos[i, "start"], ybottom = vid_bottom, xright = tag_videos[i, "end"], ytop = vid_top,
           col = adjustcolor("orange", alpha.f = 0.3), border = "orange", density = 40, angle = 45)
    }
    axis(2, at=vid_center, labels="video", las=2, cex.axis=0.8)
  }

  
  # add a border around the plot area
  box()
  

  
}


#######################################################################################################
#######################################################################################################
#######################################################################################################