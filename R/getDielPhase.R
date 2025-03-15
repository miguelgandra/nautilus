#######################################################################################################
# Estimate diel phase at a given datetimes and location ###############################################
#######################################################################################################

#' Estimate diel phase
#'
#' @description Function to retrieve diel phase (e.g. day/night) for given datetimes and coordinates.\cr
#'
#' The number of retrieved levels can be set by the 'phases' argument:\cr
#'  • phases=2:  day | night\cr
#'  • phases=3:  day | crepuscule | night\cr
#'  • phases=4:  dawn | day | dusk | night\cr
#'
#' Crepuscular periods can be defined based on different solar elevation angles:\cr
#'  • solar.depth=6: civil twilight\cr
#'  • solar.depth=12: nautical twilight\cr
#'  • solar.depth=18: astronomical twilight\cr
#'
#' @param datetimes A POSIXct object containing the respective datetimes or time-bins.
#' @param coordinates A SpatialPoints, matrix, or data frame object containing geographic (unprojected)
#' longitude and latitude coordinates (in that order) for which to estimate sunrise and sunset times.
#' If a single point or a matrix/data frame with one row is provided, the same coordinates
#' will be used for all calculations.
#' @param phases Integer indicating the number of diel phases to return (2, 3, or 4).
#' @param solar.depth Numeric value indicating the angle of the sun below the horizon (in degrees).
#' Passed to the \code{\link[suntools]{crepuscule}} function.
#' @return A factor indicating the diel phase.
#'
#' @examples
#' \dontrun{
#' datetimes <- as.POSIXct("2024-05-30 12:00:00")
#' coordinates <- c(-7.997, 37.008)
#' getDielPhase(datetimes, coordinates, phases=4, solar.depth=12)
#' }
#' @export


getDielPhase <- function(datetimes,
                         coordinates,
                         phases = 2,
                         solar.depth = 18) {

  ##############################################################################
  ## Initial checks ############################################################
  ##############################################################################

  # validate parameters
  errors <- c()
  if(!inherits(datetimes, "POSIXct")) errors <- c(errors, "Datetimes must be provided in POSIXct format.")
  if(!phases %in% c(2,3,4)) errors <- c(errors, "Number of phases should be between 2, 3 and 4")
  if(!inherits(coordinates, c("SpatialPoints", "matrix", "data.frame"))) errors <- c(errors, "Coordinates must be a SpatialPoints, matrix, or data frame object")
  if(is.data.frame(coordinates) && ncol(coordinates)!=2) errors <- c(errors, "Coordinates data.frame must contain 2 columns (longitude and latitude)")
  # print errors if any
  if(length(errors)>0){
    stop_message <- sapply(errors, function(x) paste(strwrap(x, width=getOption("width")), collapse="\n"))
    stop_message <- c("\n", paste0("- ", stop_message, collapse="\n"))
    stop(stop_message, call.=FALSE)
  }

  # convert SpatialPoints or data frame to matrix if necessary
  if(inherits(coordinates, "SpatialPoints")) {
    coordinates <- coordinates@coords
  }else if (is.data.frame(coordinates)) {
    coordinates <- as.matrix(coordinates)
  }

  # if only one row is supplied, repeat it for all datetimes values
  if (nrow(coordinates)==1) coordinates <- matrix(rep(coordinates, length(datetimes)), ncol=2, byrow=TRUE)

  # validate length of coordinates against length of datetimes
  if (nrow(coordinates) != length(datetimes)) {
    stop("Length of coordinates must be either 1 or equal to the length of datetimes", call.=FALSE)
  }

  # calculate sunrise and sunset times for the given coordinates
  coordinates <- matrix(coordinates, ncol=2)
  sunrise <- suntools::sunriset(coordinates, datetimes, POSIXct.out=TRUE, direction="sunrise")$time
  sunset <- suntools::sunriset(coordinates, datetimes, POSIXct.out=TRUE, direction="sunset")$time

  # directly return day/night without further calculations
  if(phases==2) {
    timeofday <- ifelse(datetimes >= sunrise & datetimes <= sunset, "day", "night")
    return(factor(timeofday, levels=c("day", "night")))
  }

  # otherwise calculate dawn/dusk times
  dusk <- suntools::crepuscule(coordinates, datetimes, POSIXct.out=TRUE, solarDep=solar.depth, direction="dusk")$time
  prev_dusk <- suntools::crepuscule(coordinates, datetimes-60*60*24, POSIXct.out=TRUE, solarDep=solar.depth, direction="dusk")$time
  dawn <- suntools::crepuscule(coordinates, datetimes, POSIXct.out=TRUE, solarDep=solar.depth, direction="dawn")$time

  # determine diel phase using a streamlined conditional structure
  timeofday <- ifelse(datetimes > prev_dusk & datetimes < dawn, "night",
                      ifelse(datetimes >= dawn & datetimes <= sunrise, "dawn",
                             ifelse(datetimes > sunrise & datetimes < sunset, "day",
                                    ifelse(datetimes >= sunset & datetimes <= dusk, "dusk", "night"))))

  # if 3 phases rename dusk and dawn to crepuscule
  if(phases == 3) {
    timeofday[timeofday %in% c("dawn", "dusk")] <- "crepuscule"
    return(factor(timeofday, levels=c("day", "crepuscule", "night")))
  }

  # else return the 4 phases
  if(phases == 4) {
    return(factor(timeofday, levels=c("dawn", "day", "dusk", "night")))
  }

}

#######################################################################################################
#######################################################################################################
#######################################################################################################
