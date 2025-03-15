#######################################################################################################
#######################################################################################################
# HELPER FUNCTIONS ####################################################################################
#######################################################################################################
#######################################################################################################

# Set of functions for internal use within the 'nautilus' package


##################################################################################################
## Import namespaces   ###########################################################################

#' @import utils
#' @import stats
#' @import graphics
#' @import grDevices
#' @import data.table
NULL


##################################################################################################
## Print to console   ############################################################################

#' Print to console
#'
#' @description Prints a string to the console with a specific formatting.
#' @param string A character string to be printed to the console.
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.printConsole <- function(string){
  wrapped_text <- strwrap(string, width=getOption("width")*1.2)
  cat(paste0("\033[0;", 1, "m", wrapped_text, "\033[0m", "\n"))
}


##################################################################################################
## Decimal Places   ##############################################################################
## Sourced from https://stackoverflow.com/questions/5173692/how-to-return-number-of-decimal-places-in-r

#' Decimal Places
#'
#' @description Determines the number of decimal places in a numeric value.
#' @note This function is intended for internal use within the 'nautilus' package.
#' @keywords internal
#' @noRd

.decimalPlaces <- function(x) {
  if(is.na(x)){return(NA)}
  if (abs(x - round(x)) > .Machine$double.eps^0.5) {
    nchar(strsplit(sub('0+$', '', format(x, scientific=FALSE)), ".", fixed = TRUE)[[1]][[2]])
  } else {
    return(0)
  }
}
.decimalPlaces <- Vectorize(.decimalPlaces)


##################################################################################################
## Standard Error ################################################################################

#' Standard Error
#'
#' @description Computes the standard error (SE) of a numeric vector, defined as
#' the standard deviation divided by the square root of the sample size.
#' @note This function is intended for internal use within the 'nautilus' package.
#' @keywords internal
#' @noRd

.standardError <- function(x) {
  sd(x, na.rm = TRUE) / sqrt(sum(!is.na(x)))
}


##################################################################################################
## Mode ##########################################################################################

#' Compute the Mode of a Vector
#'
#' @note This function is intended for internal use within the 'nautilus' package.
#' @keywords internal
#' @noRd

.mode <- function(x) {
  uniq <- unique(x)
  uniq[which.max(tabulate(match(x, uniq)))]
}



##################################################################################################
## Calculate circular rolling mean   #############################################################

#' Calculate circular/angular rolling mean
#'
#' @description This function calculates the rolling circular mean of a set of angles (in degrees).
#' It accounts for the wrapping nature of angular data, ensuring correct averaging even when angles span
#' discontinuities, such as those found in heading, roll, or pitch angles.
#'
#' @param angles A numeric vector of angles (in degrees). Missing values (NA) are ignored.
#' @param window An integer specifying the size of the rolling window. It determines how many values
#' to consider for each smoothing operation.
#' @param range A numeric vector of length 2 specifying the desired range for the output
#' (e.g., c(0, 360) or c(-180, 180)). The first element represents the lower bound,
#' and the second represents the upper bound. The function adjusts the result to
#'  ensure it falls within this specified range.
#'
#' @details
#' The rolling circular mean is computed by first converting the angles to radians, calculating the mean sine
#' and cosine values over a moving window, and then deriving the resulting angle using the `atan2` function.
#' The result is returned in degrees and adjusted to fall within the specified range. This operation ensures that
#' angular data wrapping (e.g., crossing 360 degrees or -180 degrees) is handled correctly.
#'
#' @note This function is intended for internal use within the `nautilus` package.
#'
#' @keywords internal
#' @noRd

.rollingCircularMean <- function(angles, window, range = c(-180, 180)) {

  # validate input
  if (!is.numeric(angles)) stop("Input 'angles' must be a numeric vector.", call. = FALSE)
  if (!is.numeric(window) || window <= 0 || window != as.integer(window)) stop("'window' must be a positive integer.", call. = FALSE)
  if (length(range) != 2 || !is.numeric(range)) stop("Input 'range' must be a numeric vector of length 2.", call. = FALSE)

  # extract range bounds
  lower_bound <- range[1]
  upper_bound <- range[2]
  range_width <- upper_bound - lower_bound

  # convert angles to radians and compute sine and cosine
  sin_angles <- sin(angles * pi / 180)
  cos_angles <- cos(angles * pi / 180)

  # use data.table::frollsum to calculate rolling sums for sine and cosine
  roll_sin_sum <- data.table::frollsum(sin_angles, n = window, fill = NA, align = "center")
  roll_cos_sum <- data.table::frollsum(cos_angles, n = window, fill = NA, align = "center")

  # compute circular mean in radians
  circular_mean_deg <- atan2(roll_sin_sum, roll_cos_sum) * (180 / pi)

  # adjust to specified range
  circular_mean_deg <- (circular_mean_deg - lower_bound) %% range_width + lower_bound

  return(circular_mean_deg)
}


##################################################################################################
## Rescale function  #############################################################################
## Sourced from 'scales' package (https://CRAN.R-project.org/package=scales)

#' Rescale
#'
#' @description Rescales a numeric vector to a specified range.
#' @note This function is intended for internal use within the 'nautilus' package.
#' @keywords internal
#' @noRd

.rescale <- function (x, to=c(0, 1), from=range(x, na.rm=TRUE, finite=TRUE), ...) {
  if (.zero_range(from) || .zero_range(to)) return(ifelse(is.na(x), NA, mean(to)))
  (x - from[1])/diff(from) * diff(to) + to[1]
}

.zero_range <- function(x, tol=1000*.Machine$double.eps) {
  if (length(x) == 1) return(TRUE)
  if (length(x) != 2) stop("'x' must be length 1 or 2", call.=FALSE)
  if (any(is.na(x))) return(NA)
  if (x[1] == x[2]) return(TRUE)
  if (all(is.infinite(x))) return(FALSE)
  m <- min(abs(x))
  if (m == 0) return(FALSE)
  abs((x[1] - x[2]) / m) < tol
}



##################################################################################################
## Updated colorlegend function  #################################################################
## Adapted from 'shape' package (https://rdrr.io/cran/shape/src/R/colorlegend.R)
## - added main.adj + main.inset + support for scientific notation
## - added tick.length
## - added horizontal option
## - added zlab option

#' Color Legend
#'
#' @description Creates a color legend for a plot, adapted from https://rdrr.io/cran/shape/src/R/colorlegend.R with additional features such as main.adj, main.inset, and support for scientific notation.
#' @note This function is intended for internal use within the 'nautilus' package.
#' @keywords internal
#' @noRd

.colorlegend <- function(col,
                         zlim,
                         zlevels = 5,
                         dz = NULL,
                         zval = NULL,
                         zlab = NULL,
                         log = FALSE,
                         posx = c(0.9, 0.93),
                         posy = c(0.05, 0.9),
                         main = NULL,
                         main.cex = 1.0,
                         main.col = "black",
                         main.adj = 0.5,
                         lab.col = "black",
                         main.inset = 1,
                         digit = 0,
                         left = FALSE,
                         tick.length = 0.3,
                         lab.scientific = FALSE,
                         horizontal = FALSE,
                         ...) {

  ## Set the number of colors
  ncol <- length(col)

  # Set up a new plot layer without erasing the existing plot
  par(new=TRUE)

  ## Save original margin settings and initialize new margins
  omar <- nmar <- par("mar")

  ## Adjust margins based on orientation
  if (horizontal) {
    nmar[1] <- max(nmar[1], 4)  # Increase bottom margin for horizontal legend
  } else {
    nmar[c(2, 4)] <- 0  # Remove left and right margins for vertical legend
  }

  ## Apply updated margin settings
  par(mar=nmar)

  ## Create an empty plot for the legend without axes or frames
  plot(0, type="n", xlab="", ylab="", asp=1, axes=FALSE, frame.plot=FALSE,
       xlim=c(0, 1), ylim=c(0, 1), xaxs="i", yaxs="i")

  ## Get the plotting area coordinates
  pars <- par("usr")

  ## Calculate the size of the plot area
  dx <- pars[2] - pars[1]
  dy <- pars[4] - pars[3]


  ## Draw the color legend (horizontal or vertical)
  if (horizontal) {
    ## ---- Horizontal Legend ----

    ## Set the position of the legend
    ymin <- pars[3] + posy[1] * dy
    ymax <- pars[3] + posy[2] * dy
    xmin <- pars[1] + posx[1] * dx
    xmax <- pars[1] + posx[2] * dx

    ## Create colored rectangles for the horizontal legend
    X <- seq(xmin, xmax, length.out=ncol+1)
    rect(X[-(ncol+1)], ymin, X[-1], ymax, col=col, border=NA)
    rect(xmin, ymin, xmax, ymax, border=lab.col)

    ## Determine tick labels (either provided by zval or calculated)
    if (!is.null(zval)) {
      zz <- zval
    } else if (is.null(dz) & !is.null(zlevels)) {
      zz <- pretty(zlim, n=(zlevels + 1))  # Generate pretty tick labels
    }

    ## Apply logarithmic scaling if specified
    if (log) zz <- log10(zz)

    ## Draw tick marks and labels for the horizontal legend
    if (!is.null(zz)) {
      Xpos <- xmin + (zz - zlim[1]) / (zlim[2] - zlim[1]) * (xmax - xmin)

      ## Draw ticks
      tick.ystart <- ymin - (tick.length * (ymax - ymin))  # Tick length as fraction of height
      tick.yend <- ymin  # Y position of tick end
      segments(Xpos, tick.ystart, Xpos, tick.yend, col = lab.col)

      ## Format labels (scientific or fixed-point)
      if (lab.scientific) {
        labels <- format(zz, scientific = TRUE)
      } else {
        labels <- formatC(zz, digits = digit, format = "f")
      }

      # Replace with custom labels
      if (!is.null(zlab)) {
        labels[match(zz, zval)] <- zlab
      }


      ## Adjust y-position to ensure visibility
      text(Xpos, tick.ystart - 0.02 * dy, labels, col = lab.col, adj=c(0.5, 1), ...)
    }


  } else {
    ## ---- Vertical Legend ----

    ## Set the position of the legend
    ymin <- pars[3] + posy[1] * dy
    ymax <- pars[3] + posy[2] * dy
    xmin <- pars[1] + posx[1] * dx
    xmax <- pars[1] + posx[2] * dx

    ## Create colored rectangles for the vertical legend
    Y <- seq(ymin, ymax, length.out=ncol+1)
    rect(xmin, Y[-(ncol+1)], xmax, Y[-1], col=col, border=NA)
    rect(xmin, ymin, xmax, ymax, border=lab.col)

    ## Determine tick labels (either provided by zval or calculated)
    if (!is.null(zval)) {
      zz <- zval
    } else if (is.null(dz) & !is.null(zlevels)) {
      zz <- pretty(zlim, n=(zlevels + 1))  # Generate pretty tick labels
    }

    ## Apply logarithmic scaling if specified
    if (log) zz <- log10(zz)

    ## Draw tick marks and labels for the vertical legend
    if (!is.null(zz)) {
      Ypos <- ymin + (zz - zlim[1]) / (zlim[2] - zlim[1]) * (ymax - ymin)

      ## Draw ticks
      tick.xstart <- if (left) {xmin - (tick.length * (xmax - xmin))} else {xmax}
      tick.xend <- if (left) {xmin} else {xmax + (tick.length * (xmax - xmin))}
      segments(tick.xstart, Ypos, tick.xend, Ypos, col = lab.col)

      ## Format labels (scientific or fixed-point)
      if (lab.scientific) {
        labels <- format(zz, scientific = TRUE)
      } else {
        labels <- formatC(zz, digits = digit, format = "f")
      }

      # Replace with custom labels
      if (!is.null(zlab)) {
        labels[match(zz, zval)] <- zlab
      }

      # Adjust horizontal alignment based on legend position
      # 1 for right-alignment, 0 for left-alignment
      adj_value <- if (left) 1 else 0

      ## Add labels next to ticks
      #pos <- if (left) 2 else 4
      text(tick.xend + 0.01 * dx, Ypos, labels, col = lab.col, adj = adj_value, ...)
    }
  }

  ## ---- Main Title ----
  if (!is.null(main)) {
    if (horizontal) {
      # Place the title above the horizontal color scale and center it
      text(mean(c(xmin, xmax)), ymax + 0.05 * dy,  # y-position adjusted to ymax
           labels = main, adj = c(0.5, 0.5), cex = main.cex, col = main.col)
    } else {
      # Keep the title below the vertical color scale
      text(mean(c(xmin, xmax)), ymax + 0.05 * dy,
           labels=main, adj=c(main.adj, 0.5), cex=main.cex, col=main.col)
    }
  }

  ## Reset the margin settings
  par(new=FALSE)
  par(mar=omar)
}


##################################################################################################
## Jet color palette #############################################################################
## Sourced from pals::jet

#' Jet color palette
#'
#' @description This function generates a color palette using the Jet color scheme,
#' sourced from palr::jet function. The colors in this palette are slightly darker
#' (10%) than the original Jet palette.
#' @note This function is intended for internal use within the 'nautilus' package.
#' @keywords internal
#' @noRd

.jet_pal <- function(n=25){
  colors <- c("#020275", "#0808E5", "#4272CB", "#17E2E2", "#7FE07F", "#E2E207",
              "#DC7534", "#E50303", "#740101")
  colorRampPalette(colors)(n)
}


##################################################################################################
##################################################################################################
##################################################################################################
