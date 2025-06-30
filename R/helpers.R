################################################################################################
################################################################################################
# HELPER FUNCTIONS #############################################################################
################################################################################################
################################################################################################

# Set of functions for internal use within the 'nautilus' package


#####################################################################################
## Import namespaces ################################################################
#####################################################################################
#' @import utils
#' @import stats
#' @import graphics
#' @import grDevices
#' @import data.table
NULL


#####################################################################################
## Print to console #################################################################
#####################################################################################

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


#####################################################################################
## Decimal Places ###################################################################
#####################################################################################
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


#####################################################################################
## Standard Error ###################################################################
#####################################################################################

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


#####################################################################################
## Mode #############################################################################
#####################################################################################

#' Compute the Mode of a Vector
#'
#' @note This function is intended for internal use within the 'nautilus' package.
#' @keywords internal
#' @noRd

.mode <- function(x) {
  uniq <- unique(x)
  uniq[which.max(tabulate(match(x, uniq)))]
}



#####################################################################################
## Convert sensor units  ############################################################
#####################################################################################

#' Convert Sensor Values Between Common Measurement Units
#'
#' Standardizes sensor measurements by converting between different units, ensuring consistent
#' units for downstream analysis. All gyroscope values are converted to rad/s and all
#' acceleration values to g (standard gravity) by default.
#'
#' @param value Numeric vector of values to convert
#' @param from.unit Character string specifying the current unit of measurement.
#'   Supported acceleration units: "m/s2", "g"
#'   Supported gyroscope units: "rad/s", "deg/s", "mrad/s"
#'   Supported temperature units: "C" (Celsius), "F" (Fahrenheit), "K" (Kelvin)
#'   Supported depth/pressure units: "m" (meters), "dbar" (decibar), "bar")
#'   Supported magnetic units: "uT" (microtesla), "nT" (nanotesla), "mG" (milligauss)
#'   Supported speed units: "m/s", "km/h", "knot", "mph"
#'   Other supported units: "" (unitless)
#' @param to.unit Character string specifying the target unit of measurement.
#'   Use "" or NA for unitless values. If NULL, converts to standard units:
#'   - Accelerometer data → "g"
#'   - Gyroscope data → "rad/s"
#'   - Temperature data → "C"
#'   - Depth/Pressure data → "m"
#'   - Magnetic data → "uT"
#'   - Speed data → "m/s"
#'   - Other data → original units
#' @param verbose Logical. If `TRUE`, prints messages about the conversions performed.
#'
#' @return Numeric vector of converted values with the following guarantees:
#'   - Acceleration always returned in g (standard gravity, 9.80665 m/s²)
#'   - Gyroscopic data always returned in rad/s
#'   - Magnetometer data always returned in µT
#'   - Temperature always returned in °C
#'   - Depth always returned in meters
#'
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal


.convertUnits <- function(value, from.unit, to.unit = NULL, verbose = FALSE) {

  # Ensure input units are character and not empty/NA if not handled by default
  from.unit <- as.character(from.unit)
  if (length(from.unit) == 0 || is.na(from.unit) || from.unit == "") {
    if (verbose) cat(paste0("Warning: `from.unit` is missing or empty. No conversion performed.\n"))
    return(value)
  }

  # Handle NULL to.unit by converting to standard units
  if (is.null(to.unit)) {
    if (verbose) cat(paste0("Converting from '", from.unit, "' to standard unit.\n"))
    to.unit <- dplyr::case_when(
      from.unit %in% c("m/s2", "g") ~ "g",
      from.unit %in% c("rad/s", "deg/s", "mrad/s") ~ "rad/s",
      from.unit %in% c("C", "F", "K") ~ "C",
      from.unit %in% c("m", "dbar", "bar") ~ "m",
      from.unit %in% c("uT", "nT", "mG") ~ "uT",
      from.unit %in% c("m/s", "km/h", "knot", "mph") ~ "m/s",
      TRUE ~ from.unit
    )
  } else {
    to.unit <- as.character(to.unit)
    if (is.na(to.unit) || to.unit == "") {
      if (verbose) cat(paste0("Target unit is empty or NA. Returning original values (unitless).\n"))
      if (!is.numeric(value)) {
        warning(paste0("Attempting to convert to unitless, but value is not numeric. Returning original values."), call. = FALSE, immediate = TRUE)
      }
      return(value)
    }
  }

  # Return unchanged if units match
  if (from.unit == to.unit) {
    if (verbose) cat(paste0("Units match ('", from.unit, "'). No conversion needed.\n"))
    return(value)
  }

  # --- Acceleration conversions ---
  if (from.unit == "m/s2" && to.unit == "g") {
    if (verbose) cat(paste0("Converting acceleration from m/s", "\U00B2", " to g.\n"))
    return(value / 9.80665)
  } else if (from.unit == "g" && to.unit == "m/s2") {
    if (verbose) cat(paste0("Converting acceleration from g to m/s", "\U00B2", ".\n"))
    return(value * 9.80665)

    # --- Gyroscope conversions ---
  } else if (from.unit == "deg/s" && to.unit == "rad/s") {
    if (verbose) cat(paste0("Converting gyroscope from deg/s to rad/s.\n"))
    return(value * pi / 180)
  } else if (from.unit == "rad/s" && to.unit == "deg/s") {
    if (verbose) cat(paste0("Converting gyroscope from rad/s to deg/s.\n"))
    return(value * 180 / pi)
  } else if (from.unit == "mrad/s" && to.unit == "rad/s") {
    if (verbose) cat(paste0("Converting gyroscope from mrad/s to rad/s.\n"))
    return(value / 1000)
  } else if (from.unit == "rad/s" && to.unit == "mrad/s") {
    if (verbose) cat(paste0("Converting gyroscope from rad/s to mrad/s.\n"))
    return(value * 1000)
  } else if (from.unit == "mrad/s" && to.unit == "deg/s") {
    if (verbose) cat(paste0("Converting gyroscope from mrad/s to deg/s.\n"))
    return(value * (180 / pi) / 1000)
  } else if (from.unit == "deg/s" && to.unit == "mrad/s") {
    if (verbose) cat(paste0("Converting gyroscope from deg/s to mrad/s.\n"))
    return(value * (pi / 180) * 1000)

    # --- Temperature conversions ---
  } else if (from.unit == "C" && to.unit == "F") {
    if (verbose) cat(paste0("Converting temperature from Celsius to Fahrenheit.\n"))
    return((value * 9/5) + 32)
  } else if (from.unit == "F" && to.unit == "C") {
    if (verbose) cat(paste0("Converting temperature from Fahrenheit to Celsius.\n"))
    return((value - 32) * 5/9)
  } else if (from.unit == "C" && to.unit == "K") {
    if (verbose) cat(paste0("Converting temperature from Celsius to Kelvin.\n"))
    return(value + 273.15)
  } else if (from.unit == "K" && to.unit == "C") {
    if (verbose) cat(paste0("Converting temperature from Kelvin to Celsius.\n"))
    return(value - 273.15)
  } else if (from.unit == "F" && to.unit == "K") {
    if (verbose) cat(paste0("Converting temperature from Fahrenheit to Kelvin.\n"))
    return((value - 32) * 5/9 + 273.15)
  } else if (from.unit == "K" && to.unit == "F") {
    if (verbose) cat(paste0("Converting temperature from Kelvin to Fahrenheit.\n"))
    return((value - 273.15) * 9/5 + 32)

    # --- Pressure/Depth conversions ---
  } else if (from.unit == "dbar" && to.unit == "m") {
    if (verbose) cat(paste0("Converting depth/pressure from decibar to meters (approx.).\n"))
    return(value)
  } else if (from.unit == "m" && to.unit == "dbar") {
    if (verbose) cat(paste0("Converting depth/pressure from meters to decibar (approx.).\n"))
    return(value)
  } else if (from.unit == "bar" && to.unit == "m") {
    if (verbose) cat(paste0("Converting depth/pressure from bar to meters (approx.).\n"))
    return(value * 10)
  } else if (from.unit == "m" && to.unit == "bar") {
    if (verbose) cat(paste0("Converting depth/pressure from meters to bar (approx.).\n"))
    return(value / 10)
  } else if (from.unit == "dbar" && to.unit == "bar") {
    if (verbose) cat(paste0("Converting pressure from decibar to bar.\n"))
    return(value / 10)
  } else if (from.unit == "bar" && to.unit == "dbar") {
    if (verbose) cat(paste0("Converting pressure from bar to decibar.\n"))
    return(value * 10)

    # --- Magnetic field conversions ---
  } else if (from.unit == "nT" && to.unit == "uT") {
    if (verbose) cat(paste0("Converting magnetic field from nanotesla to microtesla.\n"))
    return(value / 1000)
  } else if (from.unit == "uT" && to.unit == "nT") {
    if (verbose) cat(paste0("Converting magnetic field from microtesla to nanotesla.\n"))
    return(value * 1000)
  } else if (from.unit == "mG" && to.unit == "uT") {
    if (verbose) cat(paste0("Converting magnetic field from milligauss to microtesla.\n"))
    return(value / 10)
  } else if (from.unit == "uT" && to.unit == "mG") {
    if (verbose) cat(paste0("Converting magnetic field from microtesla to milligauss.\n"))
    return(value * 10)

    # --- Speed conversions ---
  } else if (from.unit == "km/h" && to.unit == "m/s") {
    if (verbose) cat(paste0("Converting speed from km/h to m/s.\n"))
    return(value / 3.6)
  } else if (from.unit == "m/s" && to.unit == "km/h") {
    if (verbose) cat(paste0("Converting speed from m/s to km/h.\n"))
    return(value * 3.6)
  } else if (from.unit == "knot" && to.unit == "m/s") {
    if (verbose) cat(paste0("Converting speed from knots to m/s.\n"))
    return(value * 0.514444)
  } else if (from.unit == "m/s" && to.unit == "knot") {
    if (verbose) cat(paste0("Converting speed from m/s to knots.\n"))
    return(value / 0.514444)
  } else if (from.unit == "mph" && to.unit == "m/s") {
    if (verbose) cat(paste0("Converting speed from mph to m/s.\n"))
    return(value * 0.44704)
  } else if (from.unit == "m/s" && to.unit == "mph") {
    if (verbose) cat(paste0("Converting speed from m/s to mph.\n"))
    return(value / 0.44704)

    # --- Fallback for unsupported conversions ---
  } else {
    warning(paste("No conversion defined from", from.unit, "to", to.unit,
                  "- returning original values for column."), call. = FALSE, immediate = TRUE)
    return(value)
  }
}




#####################################################################################
# Centered rolling mean with edge padding ###########################################
#####################################################################################

#' Centered Rolling Mean with Edge Padding
#'
#' Calculates a centered rolling mean while avoiding edge artifacts through symmetric padding.
#' Designed for time-series smoothing where maintaining data length and minimizing edge distortion
#' is critical. Uses efficient \code{data.table} backend for large datasets.
#'
#' @param x Numeric vector to be smoothed (may contain \code{NA}s).
#' @param window Integer width of the rolling window (in observations). For time-based windows,
#'               use \code{window = sampling_rate * seconds}.
#' @param pad_len Integer number of values to pad at each edge. Typically \code{ceiling(window/2)}.
#'                Determines how many values are mirrored at the boundaries.
#'
#' @return Numeric vector of the same length as \code{x} containing smoothed values.
#'         The first and last \code{pad_len} values are smoothed using mirrored padding.
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.pad_rollmean <- function(x, window, pad_len) {
  # symmetric padding using first/last values
  padded <- c(rep(x[1], pad_len), x, rep(x[length(x)], pad_len))
  # compute centered rolling mean on padded data
  rolled <- data.table::frollmean(padded, n = window, align = "center", na.rm = TRUE)
  # trim to original length
  rolled[(pad_len + 1):(length(x) + pad_len)]
}


#####################################################################################
# Format durations ##################################################################
#####################################################################################

#' Format a duration in seconds into a human-readable string
#'
#' Converts a numeric value in seconds into a formatted string using
#' appropriate units (seconds, minutes, or hours), rounded to two decimal places.
#'
#' @param seconds A numeric value representing a duration in seconds.
#' @return A character string representing the formatted duration.
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.formatDuration <- function(seconds) {
  if (seconds >= 3600) {
    sprintf("%.2f hours", seconds / 3600)
  } else if (seconds >= 60) {
    sprintf("%.2f minutes", seconds / 60)
  } else {
    sprintf("%.2f seconds", seconds)
  }
}

#####################################################################################
# Format numbers ####################################################################
#####################################################################################

#' Format a number with thousands separators
#'
#' Converts a numeric value into a character string with comma as the
#' thousands separator and no decimal places.
#'
#' @param x A numeric value to be formatted.
#' @return A character string with formatted number.
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.formatNumber <- function(x) {
  formatC(x, format = "f", big.mark = ",", digits = 0)
}


#####################################################################################
## Calculate circular rolling mean  #################################################
#####################################################################################

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

  # handle all-NA case early
  if (all(is.na(angles))) return(rep(NA_real_, length(angles)))

  # extract range info
  lower_bound <- range[1]
  upper_bound <- range[2]
  range_width <- upper_bound - lower_bound

  # convert angles to radians
  angles_rad <- angles * pi / 180

  # pad sin and cos signals separately
  pad_len <- floor(window / 2)

  sin_angles <- sin(angles_rad)
  cos_angles <- cos(angles_rad)

  pad_sin <- c(rep(sin_angles[!is.na(sin_angles)][1], pad_len), sin_angles, rep(sin_angles[!is.na(sin_angles)][length(sin_angles[!is.na(sin_angles)])], pad_len))
  pad_cos <- c(rep(cos_angles[!is.na(cos_angles)][1], pad_len), cos_angles, rep(cos_angles[!is.na(cos_angles)][length(cos_angles[!is.na(cos_angles)])], pad_len))

  # compute rolling sums on padded data
  roll_sin_sum <- data.table::frollsum(pad_sin, n = window, align = "center", na.rm = TRUE)
  roll_cos_sum <- data.table::frollsum(pad_cos, n = window, align = "center", na.rm = TRUE)

  # trim to original length
  roll_sin_sum <- roll_sin_sum[(pad_len + 1):(length(angles) + pad_len)]
  roll_cos_sum <- roll_cos_sum[(pad_len + 1):(length(angles) + pad_len)]

  # compute circular mean
  mean_rad <- atan2(roll_sin_sum, roll_cos_sum)
  mean_deg <- mean_rad * 180 / pi

  # wrap to desired range
  wrapped <- ((mean_deg - lower_bound) %% range_width) + lower_bound
  return(wrapped)
}


#####################################################################################
## Calculate  group-wise circular mean  #############################################
#####################################################################################

#' Calculate the circular (angular) mean of a set of angles
#'
#' @description
#' Computes the mean direction of a numeric vector of angular data, expressed in degrees.
#' This function accounts for the circular nature of angles (i.e., wrapping at 360 or 180 degrees),
#' ensuring that values near discontinuities (e.g., -179 and 179 degrees) are averaged correctly.
#'
#' @param angles A numeric vector of angles (in degrees). Missing values (`NA`) are ignored in the computation.
#' @param range A numeric vector of length 2 specifying the desired output range (e.g., `c(0, 360)` or `c(-180, 180)`).
#' The first value defines the lower bound and the second the upper bound of the output interval.
#'
#' @details
#' The circular mean is calculated by converting the input angles to radians, computing the mean sine and cosine values,
#' and deriving the resulting angle using the `atan2` function. The result is returned in degrees and wrapped to fall
#' within the user-specified range. If all values in `angles` are `NA`, the function returns `NA_real_`.
#'
#' @return A single numeric value representing the circular mean (in degrees), adjusted to the specified range.
#'
#' @note This function is intended for internal use within the `nautilus` package.
#'
#' @keywords internal
#' @noRd


.circularMean <- function(angles, range) {

  # fast NA check and return
  angles <- angles[!is.na(angles)]
  if (!length(angles)) return(NA_real_)

  # convert to radians, compute circular mean and wrap to specified range
  angles_rad <- angles * pi / 180
  mean_deg <- atan2(mean(sin(angles_rad)), mean(cos(angles_rad))) * 180 / pi
  ((mean_deg - range[1]) %% (range[2] - range[1])) + range[1]
}


#####################################################################################
## Rescale function  ################################################################
#####################################################################################
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



#####################################################################################
## Updated colorlegend function  ####################################################
#####################################################################################
## Adapted from 'shape' package (https://rdrr.io/cran/shape/src/R/colorlegend.R)
## - added main.adj + support for scientific notation
## - added tick.length
## - added horizontal option
## - added zlab option
## - added mgp parameter

#' Color Legend
#'
#' @description Creates a color legend for a plot, adapted from https://rdrr.io/cran/shape/src/R/colorlegend.R
#' with additional features such as main.adj, main.inset, and support for scientific notation.
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
                         digit = 0,
                         left = FALSE,
                         tick.length = 0.3,
                         lab.scientific = FALSE,
                         horizontal = FALSE,
                         mgp = c(1.5, 0.5, 0),  # New parameter similar to par()'s mgp
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

      ## Adjust y-position using mgp parameter
      label.offset <- mgp[1] * (tick.yend - tick.ystart)  # Use first mgp value for label offset
      text(Xpos, tick.ystart - label.offset, labels, col = lab.col, adj=c(0.5, 1), ...)
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

      ## Add labels next to ticks with mgp-controlled spacing
      label.offset <- mgp[1] * (tick.xend - tick.xstart)  # Use first mgp value for label offset
      text(tick.xend + label.offset, Ypos, labels, col = lab.col, adj = adj_value, ...)
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


#####################################################################################
## Jet color palette ################################################################
#####################################################################################
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


#####################################################################################
## Viridis color palette ############################################################
#####################################################################################
## Sourced from viridis::viridis(100)

#' Viridis color palette
#'
#' @description This function generates a color palette using the Viridis color scheme,
#' known for its perceptually uniform properties. The palette is adapted from the viridis package.
#' @note This function is intended for internal use within the 'moby' package.
#' @keywords internal
#' @noRd

.viridis_pal <- function(n) {
  viridis_colors <- c(
    "#440154FF", "#450558FF", "#46085CFF", "#470D60FF", "#471063FF",
    "#481467FF", "#481769FF", "#481B6DFF", "#481E70FF", "#482173FF",
    "#482576FF", "#482878FF", "#472C7AFF", "#472F7CFF", "#46327EFF",
    "#453581FF", "#453882FF", "#443B84FF", "#433E85FF", "#424186FF",
    "#404587FF", "#3F4788FF", "#3E4A89FF", "#3D4D8AFF", "#3C508BFF",
    "#3B528BFF", "#39558CFF", "#38598CFF", "#375B8DFF", "#355E8DFF",
    "#34608DFF", "#33638DFF", "#32658EFF", "#31688EFF", "#2F6B8EFF",
    "#2E6D8EFF", "#2D708EFF", "#2C718EFF", "#2B748EFF", "#2A768EFF",
    "#29798EFF", "#287C8EFF", "#277E8EFF", "#26818EFF", "#26828EFF",
    "#25858EFF", "#24878EFF", "#238A8DFF", "#228D8DFF", "#218F8DFF",
    "#20928CFF", "#20938CFF", "#1F968BFF", "#1F998AFF", "#1E9B8AFF",
    "#1F9E89FF", "#1FA088FF", "#1FA287FF", "#20A486FF", "#22A785FF",
    "#24AA83FF", "#25AC82FF", "#28AE80FF", "#2BB07FFF", "#2EB37CFF",
    "#31B67BFF", "#35B779FF", "#39BA76FF", "#3DBC74FF", "#41BE71FF",
    "#47C06FFF", "#4CC26CFF", "#51C56AFF", "#56C667FF", "#5BC863FF",
    "#61CA60FF", "#67CC5CFF", "#6DCD59FF", "#73D056FF", "#78D152FF",
    "#7FD34EFF", "#85D54AFF", "#8CD646FF", "#92D741FF", "#99D83DFF",
    "#A0DA39FF", "#A7DB35FF", "#ADDC30FF", "#B4DE2CFF", "#BBDE28FF",
    "#C2DF23FF", "#C9E020FF", "#D0E11CFF", "#D7E219FF", "#DDE318FF",
    "#E4E419FF", "#EBE51AFF", "#F1E51DFF", "#F7E620FF", "#FDE725FF"
  )
  return(colorRampPalette(viridis_colors)(n))
}


##################################################################################################
##################################################################################################
##################################################################################################
