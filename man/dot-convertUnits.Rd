% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/helpers.R
\name{.convertUnits}
\alias{.convertUnits}
\title{Convert Sensor Values Between Common Measurement Units}
\usage{
.convertUnits(value, from.unit, to.unit = NULL, verbose = FALSE)
}
\arguments{
\item{value}{Numeric vector of values to convert}

\item{from.unit}{Character string specifying the current unit of measurement.
Supported acceleration units: "m/s2", "g"
Supported gyroscope units: "rad/s", "deg/s", "mrad/s"
Supported temperature units: "C" (Celsius), "F" (Fahrenheit), "K" (Kelvin)
Supported depth/pressure units: "m" (meters), "dbar" (decibar), "bar")
Supported magnetic units: "uT" (microtesla), "nT" (nanotesla), "mG" (milligauss)
Supported speed units: "m/s", "km/h", "knot", "mph"
Other supported units: "" (unitless)}

\item{to.unit}{Character string specifying the target unit of measurement.
Use "" or NA for unitless values. If NULL, converts to standard units:
\itemize{
\item Accelerometer data → "g"
\item Gyroscope data → "rad/s"
\item Temperature data → "C"
\item Depth/Pressure data → "m"
\item Magnetic data → "uT"
\item Speed data → "m/s"
\item Other data → original units
}}

\item{verbose}{Logical. If \code{TRUE}, prints messages about the conversions performed.}
}
\value{
Numeric vector of converted values with the following guarantees:
\itemize{
\item Acceleration always returned in g (standard gravity, 9.80665 m/s²)
\item Gyroscopic data always returned in rad/s
\item Magnetometer data always returned in µT
\item Temperature always returned in °C
\item Depth always returned in meters
}
}
\description{
Standardizes sensor measurements by converting between different units, ensuring consistent
units for downstream analysis. All gyroscope values are converted to rad/s and all
acceleration values to g (standard gravity) by default.
}
\note{
This function is intended for internal use within the \code{nautilus} package.
}
\keyword{internal}
