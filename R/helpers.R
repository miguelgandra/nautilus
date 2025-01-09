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
#' @note This function is intended for internal use within the `moby` package.
#' @keywords internal
#' @noRd

.printConsole <- function(string){
  wrapped_text <- strwrap(string, width=getOption("width")*1.2)
  cat(paste0("\033[0;", 1, "m", wrapped_text, "\033[0m", "\n"))
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
#' @note This function is intended for internal use within the `moby` package.
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
##################################################################################################
##################################################################################################
