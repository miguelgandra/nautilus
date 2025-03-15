#######################################################################################################
# Function to plot depth time series  #################################################################
#######################################################################################################

#' Plot Depth Profiles with Colored Time Series
#'
#' This function generates depth profiles for individual subjects, with depth values plotted against time.
#' The points are color-coded based on a specified variable.
#'
#' @param data A data frame or a list of data frames containing depth profile information.
#' @param id.metadata Data frame. Metadata about the IDs to associate with the processed data.
#' Must contain at least columns for ID, release longitude and release latitude.
#' @param id.col Character. The name of the column identifying individuals (default: "ID").
#' @param datetime.col Character. The name of the column containing datetime information (default: "datetime").
#' @param depth.col Character. The name of the column containing depth values (default: "depth").
#' @param color.by Character. The name of the column used for color mapping (default: "temp").
#' @param color.by.label Character. Label for the color legend (default: "Temp (ºC)").
#' @param lon.col Character. The name of the column containing longitude information (default: "lon").
#' @param lat.col Character. The name of the column containing latitude information (default: "lat").
#' @param color.pal Character vector. A color palette for the depth profiles (default: `pals::jet(100)`).
#' @param cex.id Numeric. The expansion factor for the ID labels in plots. Defaults to 1.2.
#' @param cex.pt Numeric. The expansion factor for the points. Defaults to 0.4.
#' @param cex.axis Numeric. The expansion factor for axis labels. Defaults to 0.9.
#' @param cex.legend Numeric. The expansion factor for the legend text. Defaults to 0.8.
#' @param same.color.scale Logical. If `TRUE`, all plots use the same color scale for the mapped variable (default: `TRUE`).
#' @param same.depth.scale Logical. If `TRUE`, all plots use the same depth scale; if `FALSE`, each individual has its own scale (default: `FALSE`).
#' @param ncols Integer. Number of columns for the plot layout. If `NULL`, it is determined automatically.
#' @param nrows Integer. Number of rows for the plot layout. If `NULL`, it is determined automatically.
#'
#' @return A plot displaying depth profiles for each individual, with time on the x-axis and depth on the y-axis, colored by the specified variable.
#'
#' @export


plotDepthProfiles <- function(data,
                              id.metadata,
                              id.col = "ID",
                              datetime.col = "datetime",
                              depth.col = "depth",
                              color.by = "temp",
                              color.by.label = "Temp (\u00BAC)",
                              lon.col = "lon",
                              lat.col = "lat",
                              color.pal = NULL,
                              cex.id = 1.2,
                              cex.pt = 0.4,
                              cex.axis = 0.9,
                              cex.legend = 0.8,
                              same.color.scale = TRUE,
                              same.depth.scale = FALSE,
                              ncols = NULL,
                              nrows = NULL) {


  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

  # if 'data' is not a list, split it into a list of individual data sets
  if (!is.list(data)) {
    data <- split(data, f = data[[id.col]])
  }

  # check if specified columns exist in the data
  if(!id.col %in% names(data[[1]])) stop(paste0("The specified id.col ('", id.col, "') was not found in the supplied data."), call. = FALSE)
  if(!datetime.col %in% names(data[[1]])) stop(paste0("The specified datetime.col ('", datetime.col, "') was not found in the supplied data."), call. = FALSE)
  if(!depth.col %in% names(data[[1]])) stop(paste0("The specified depth.col ('", depth.col, "') was not found in the supplied data."), call. = FALSE)
  if(!color.by %in% names(data[[1]])) stop(paste0("The specified color.by variable ('", color.by, "') was not found in the supplied data."), call. = FALSE)

  # check if specified columns exist in the id metadata
  if(!lon.col %in% names(id.metadata)) stop(paste0("The specified lon.col ('", lon.col, "') was not found in the supplied metadata"), call. = FALSE)
  if(!lat.col %in% names(id.metadata)) stop(paste0("The specified lat.col ('", lat.col, "') was not found in the supplied metadata"), call. = FALSE)


  ##############################################################################
  # Set plot variables #########################################################
  ##############################################################################

  # remove empty datasets from the list
  data <- data[sapply(data, nrow) > 0]

  # if 'same.color.scale', calculate the global color range across all datasets
  if(same.color.scale){
    color_range <- lapply(data, function(x) range(x[[color.by]], na.rm = TRUE))
    color_range <- range(unlist(color_range))
  }
  # if 'same.depth.scale', calculate the global depth range across all datasets
  if(same.depth.scale){
    depth_range <- lapply(data, function(x) range(x[[depth.col]], na.rm = TRUE))
    depth_range <- range(unlist(depth_range))
  }

  # set color palette, if not specified
  if(is.null(color.pal)){
    color.pal <- .jet_pal(100)
    color.pal <- colorRampPalette(c(rep(color.pal[1:70], each=2), rep(color.pal[71:100], each=3)))(100)
  }


  ##############################################################################
  # Set layout variables #######################################################
  ##############################################################################
  # Dynamically adjust the number of columns and rows based on the number of plots

  # calculate the total number of plots
  total_plots <- length(data)

  # if nrows and ncols are not provided, calculate them dynamically
  if (is.null(nrows) && is.null(ncols)) {
    # maximum of 2 columns and 5 rows per page
    max_cols <- 2
    max_rows <- 5
    # adjust nrows and ncols for the last page if necessary
    ncols <- ifelse(total_plots==1, 1, max_cols)
    nrows <- min(max_rows, ceiling(total_plots / ncols))
  } else {
    # if nrows or ncols are provided, use them
    if (is.null(nrows)) {
      nrows <- ceiling(total_plots / ncols)
    }
    if (is.null(ncols)) {
      ncols <- ceiling(total_plots / nrows)
    }
  }

  # set up the plotting area with specified number of rows and columns.
  par(mfrow=c(nrows, ncols), mar = c(2, 5, 2, 5), oma = c(3, 0, 0, 0), mgp = c(3, 0.8, 0))


  ##############################################################################
  # Generate depth plots #######################################################
  ##############################################################################

  # loop through each dataset (individual)
  for (i in seq_along(data)) {

    # extract data for the current individual
    plot_data <- data[[i]]

    # get current ID
    id <- unique(plot_data[[id.col]])

    # extract longitude and latitude for the current ID
    lon <- id.metadata[[lon.col]][id.metadata[[id.col]] == id]
    lat <- id.metadata[[lat.col]][id.metadata[[id.col]] == id]

    # set individual depth range if scales are not shared across individuals
    if (!same.depth.scale) {
      depth_max <- max(plot_data[[depth.col]], na.rm = TRUE)
      depth_max <-  ceiling(depth_max / 10) * 10
      depth_range <- c(0, depth_max)
      depth_range[1] <- depth_range[1]-(depth_range[2]-depth_range[1])*0.18
      depth_range <- rev(depth_range)
    }

    # set the color scale range if shared across individuals
    if (same.color.scale) {
      color_range <- range(plot_data[[color.by]], na.rm = TRUE)
    }

    # scale color values for visualization
    plot_data$color_scaled <- round(.rescale(plot_data[[color.by]], from=color_range, to = c(0, 100)))

    ############################################################################
    # initialize an empty plot with correct axis limits     ####################
    plot(y = plot_data[[depth.col]], x = plot_data[[datetime.col]], type = "n",
         axes = FALSE, xaxs="i", xlab = "", ylab = "Depth (m)", ylim = depth_range)


    ############################################################################
    # add daylight background shading ##########################################
    if (!is.na(lon) && !is.na(lat)) {

      # create time sequence from plot x-axis limits
      time_seq <- seq(from = par("usr")[1], to = par("usr")[2], by = 60)
      time_seq <- as.POSIXct(time_seq, tz="UTC")

      # classify each timestamp into diel phases
      coords <- data.frame("lon"=lon, "lat"=lat)
      diel_phase <-  moby::getDielPhase(time_seq, coordinates = coords, phases = 3)

      # identify transition points between diel phases
      phase_change <- c(1, which(diff(as.numeric(factor(diel_phase))) != 0) + 1, length(time_seq))

      # Define colors for each diel phase
      diel_colors <- c("day" = "grey98", "crepuscule" = "grey92", "night" = "grey85")

      # convert time to numeric (required for rect() when plotting POSIXct on x-axis)
      time_numeric <- as.numeric(time_seq)
      xlims <- range(as.numeric(plot_data[[datetime.col]]))  # Match x-axis range of plot

      # draw background rectangles for each diel phase segment
      for (i in seq_len(length(phase_change) - 1)) {
        rect(
          xleft = max(time_numeric[phase_change[i]], xlims[1]),
          xright = min(time_numeric[phase_change[i + 1]], xlims[2]),
          ybottom = par("usr")[3], ytop = par("usr")[4],
          col = diel_colors[diel_phase[phase_change[i]]], border = NA
        )
      }
    }

    ############################################################################
    # add depth data points with color representing the chosen variable
    points(y = plot_data[[depth.col]], x = plot_data[[datetime.col]], pch = 16,
           col = color.pal[plot_data$color_scaled], cex = cex.pt)

    ############################################################################
    # add axes #################################################################

    # determine datetime range and generate suitable time breaks
    time_range <- range(plot_data[[datetime.col]])
    time_breaks <- pretty(time_range, n = 5)

    # add X-axis (datetime)
    axis.POSIXct(1, at = time_breaks, format = ifelse(diff(time_range) > 86400, "%d/%b", "%H:%M"), cex.axis = cex.axis)

    # add Y-axis (depth)
    axis(2, at = pretty(c(0, depth_range[1]), n=5), las = 1, cex.axis = cex.axis)

    ############################################################################
    # add depth guidelines #####################################################

    # add surface line
    abline(h=0, lty=2, lwd=1.2)

    # add max depth line
    abline(h=max(plot_data[[depth.col]], na.rm = TRUE), lty=2, lwd=1)


    ############################################################################
    # add legend and draw border ###############################################

    # add fish ID
    legend("topleft", inset=c(0,-0.036), legend = id, text.font = 2, bty = "n", cex=cex.id)

    # draw a box around the plot
    box()


    ############################################################################
    # add color legend #########################################################
    color_labs <- pretty(color_range)
    color_labs <- color_labs[color_labs>=min(color_range) & color_labs<=max(color_range)]
    .colorlegend(col=color.pal, zlim=color_range, zval=color_labs,
                 posx=c(0.915, 0.93), posy = c(0.05, 0.85), main = color.by.label,
                 main.cex=cex.legend+0.1, digit=1, main.adj=0, cex=cex.legend)

  }
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
