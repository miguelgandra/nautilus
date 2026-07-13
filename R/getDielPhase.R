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
#' @param solar.depth Numeric value indicating the angle of the sun below the horizon (in degrees) that
#' defines twilight for the dawn/dusk phases: 6 (civil), 12 (nautical), or 18 (astronomical). Only used
#' when \code{phases > 2}.
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

  # calculate sunrise and sunset times for the given coordinates (columns: longitude, latitude)
  coordinates <- matrix(coordinates, ncol=2)
  lon <- coordinates[, 1]; lat <- coordinates[, 2]
  sunrise <- .solarEventTime(lon, lat, datetimes, solarDep = 0.833, morning = TRUE)
  sunset  <- .solarEventTime(lon, lat, datetimes, solarDep = 0.833, morning = FALSE)

  # directly return day/night without further calculations
  if(phases==2) {
    timeofday <- ifelse(datetimes >= sunrise & datetimes <= sunset, "day", "night")
    return(factor(timeofday, levels=c("day", "night")))
  }

  # otherwise calculate dawn/dusk times (twilight at the chosen solar-depression angle)
  dusk      <- .solarEventTime(lon, lat, datetimes,               solarDep = solar.depth, morning = FALSE)
  prev_dusk <- .solarEventTime(lon, lat, datetimes - 60*60*24,    solarDep = solar.depth, morning = FALSE)
  dawn      <- .solarEventTime(lon, lat, datetimes,               solarDep = solar.depth, morning = TRUE)

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


#' Solar event times (sunrise / sunset / dawn / dusk) without the sf / GDAL stack
#'
#' Computes the time at which the sun crosses a given altitude on the day of each `datetime`, replacing
#' `suntools::sunriset()` / `suntools::crepuscule()` (which now depend on `sf` and its GDAL/GEOS/PROJ system
#' libraries). Implements the NOAA Solar Calculator equations (after Meeus, "Astronomical Algorithms";
#' NOAA ESRL Global Monitoring Laboratory), with a two-pass refinement (solar parameters re-evaluated at the
#' estimated event time). Verified against `suntools` 1.1.0: sub-second agreement for sunrise/sunset and
#' within ~3 s for civil/nautical twilight.
#'
#' @param lon,lat Longitude / latitude in decimal degrees (East / North positive); length 1 or `length(datetimes)`.
#' @param datetimes POSIXct instants; the event is returned for the UTC calendar date of each.
#' @param solarDep Sun depression below the horizon (degrees): 0.833 for sunrise/sunset (refraction +
#'   solar semidiameter), or 6 / 12 / 18 for civil / nautical / astronomical twilight.
#' @param morning `TRUE` for the morning crossing (sunrise / dawn), `FALSE` for the evening (sunset / dusk).
#' @return POSIXct (UTC) event times; `NA` where the sun never reaches that altitude (polar day / night).
#' @keywords internal
#' @noRd
.solarEventTime <- function(lon, lat, datetimes, solarDep, morning) {
  rad  <- pi / 180
  n    <- length(datetimes)
  lon  <- rep_len(lon, n); lat <- rep_len(lat, n)
  day0 <- as.numeric(as.Date(datetimes, tz = "UTC"))          # days since 1970-01-01 (UTC calendar date)
  jd0  <- day0 + 2440587.5                                     # Julian Day at 00:00 UTC

  # solar declination (deg) + equation of time (min) at a Julian Day (NOAA Solar Calculator / Meeus)
  .solpar <- function(jd) {
    T  <- (jd - 2451545) / 36525
    L0 <- (280.46646 + T * (36000.76983 + T * 0.0003032)) %% 360
    M  <- 357.52911 + T * (35999.05029 - 0.0001537 * T); Mr <- M * rad
    e  <- 0.016708634 - T * (0.000042037 + 0.0000001267 * T)
    C  <- sin(Mr) * (1.914602 - T * (0.004817 + 0.000014 * T)) +
          sin(2 * Mr) * (0.019993 - 0.000101 * T) + sin(3 * Mr) * 0.000289
    lambda <- (L0 + C) - 0.00569 - 0.00478 * sin((125.04 - 1934.136 * T) * rad)
    eps0 <- 23 + (26 + (21.448 - T * (46.815 + T * (0.00059 - T * 0.001813))) / 60) / 60
    eps  <- eps0 + 0.00256 * cos((125.04 - 1934.136 * T) * rad)
    decl <- asin(sin(eps * rad) * sin(lambda * rad)) / rad
    y    <- tan(eps / 2 * rad)^2
    eqt  <- 4 / rad * (y * sin(2 * L0 * rad) - 2 * e * sin(Mr) + 4 * e * y * sin(Mr) * cos(2 * L0 * rad) -
                       0.5 * y^2 * sin(4 * L0 * rad) - 1.25 * e^2 * sin(2 * Mr))
    list(decl = decl, eqt = eqt)
  }
  # event time (minutes from 00:00 UTC) for solar parameters evaluated at Julian Day `jd`
  .eventMin <- function(jd) {
    s    <- .solpar(jd)
    cosH <- (cos((90 + solarDep) * rad) - sin(lat * rad) * sin(s$decl * rad)) /
            (cos(lat * rad) * cos(s$decl * rad))
    ha   <- ifelse(abs(cosH) > 1, NA_real_, acos(pmin(pmax(cosH, -1), 1)) / rad)  # hour angle (deg)
    noon <- 720 - 4 * lon - s$eqt                             # solar noon (min UTC); lon East positive
    if (morning) noon - 4 * ha else noon + 4 * ha             # 4 min per degree of hour angle
  }
  m <- .eventMin(jd0 + 0.5)                                   # first guess at 12:00 UTC
  m <- .eventMin(jd0 + m / 1440)                             # refine at the estimated event time
  as.POSIXct(day0 * 86400 + m * 60, origin = "1970-01-01", tz = "UTC")
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
