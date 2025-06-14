% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/checkSensorAnomalies.R
\name{checkSensorAnomalies}
\alias{checkSensorAnomalies}
\title{Identify and Handle Outliers in Time Series Data}
\usage{
checkSensorAnomalies(
  data,
  id.col = "ID",
  datetime.col = "datetime",
  sensor.col = "depth",
  sensor.name = sensor.col,
  rate.threshold,
  sensor.resolution = 0.5,
  sensor.accuracy.fixed = NULL,
  sensor.accuracy.percent = NULL,
  outlier.window = 5,
  stall.threshold = 5,
  interpolate = TRUE,
  return.data = TRUE,
  save.files = FALSE,
  save.mode = "all",
  output.folder = NULL,
  output.suffix = NULL,
  verbose = TRUE
)
}
\arguments{
\item{data}{A list of data.tables/data.frames, one for each individual; a single aggregated data.table/data.frame
containing data from multiple animals (with an 'ID' column); or a character vector of file paths pointing to
\code{.rds} files, each containing data for a single individual. When a character vector is provided,
files are loaded sequentially to optimize memory use. The output of the \link{importTagData} function
is strongly recommended, as it formats the data appropriately for all downstream analysis.}

\item{id.col}{A string representing the column name for the ID field (default is "ID").}

\item{datetime.col}{A string specifying the name of the column that contains timestamps for each observation.
This column must be in "POSIXct" format for proper processing (default is "datetime").}

\item{sensor.col}{A string representing the column name containing the sensor data to be analyzed.}

\item{sensor.name}{A string representing the name of the sensor being analyzed. This can be used for
descriptive purposes in print statements or output, such as "depth", "temperature", or other sensor types.}

\item{rate.threshold}{A numeric value representing the threshold rate of change (in units per second)
beyond which values will be flagged as outliers. Values exceeding this threshold will
be considered outliers and replaced with \code{NA} (or interpolated).}

\item{sensor.resolution}{Numeric. The sensor's resolution (the smallest detectable change in sensor readings).
This value is used for uncertainty estimation in rate of change calculations. Default is 0.5.}

\item{sensor.accuracy.fixed}{Numeric. A fixed sensor accuracy value (in the same units as \code{sensor.col}).
If provided, this value is used for uncertainty calculations. This argument should
not be used simultaneously with \code{sensor.accuracy.percent}.}

\item{sensor.accuracy.percent}{Numeric. A percentage value representing the sensor's accuracy, defined as
± percentage of the sensor reading. This is used to calculate the uncertainty in the
sensor's measurements based on the readings. This argument should not be used
simultaneously with \code{sensor.accuracy.fixed}.}

\item{outlier.window}{Numeric. The time window (in minutes) within which consecutive outliers
are grouped and treated as a single anomaly period. If multiple outliers occur within this
window, all affected values—including small "islands" of valid data between them—are replaced
with \code{NA} to ensure data quality. Default is 10 minutes.}

\item{stall.threshold}{A numeric value specifying the threshold (in minutes)
for detecting prolonged constant sensor readings. The function will flag
sequences of identical values that persist beyond this threshold as potential
sensor failures. However, periods where the sensor value is zero (e.g., depth = 0,
indicating surface intervals) are excluded to prevent misidentification of
expected behavior.}

\item{interpolate}{Logical. If TRUE, the function will interpolate the missing values (flagged outliers) using
the \code{zoo} package's \code{na.approx} function. If FALSE, outliers will simply be replaced with \code{NA}.
Default is TRUE.}

\item{return.data}{Logical. Controls whether the function returns the processed data
as a list in memory. When processing large or numerous datasets, set to \code{FALSE} to reduce
memory usage. Note that either \code{return.data} or \code{save.files} must be \code{TRUE}
(or both). Default is \code{TRUE}.}

\item{save.files}{Logical. If \code{TRUE}, the processed data for each ID will be saved as RDS files
during the iteration process. This ensures that progress is saved incrementally, which can
help prevent data loss if the process is interrupted or stops midway. Default is \code{FALSE}.}

\item{save.mode}{Character. Specifies which files to save when \code{save.files} is \code{TRUE}.
Can be "all" (saves all processed files, regardless of whether anomalies were corrected)
or "corrected" (only saves files where anomalies were detected and corrected).
Default is "all".}

\item{output.folder}{Character. Path to the folder where the processed files will be saved.
This parameter is only used if \code{save.files = TRUE}. If \code{NULL}, the RDS file will be saved
in the data folder corresponding to each ID. Default is \code{NULL}.}

\item{output.suffix}{Character. A suffix to append to the file name when saving.
This parameter is only used if \code{save.files = TRUE}.}

\item{verbose}{Logical. If TRUE, the function will print detailed processing information. Defaults to TRUE.}
}
\value{
Depending on input and parameters:
\itemize{
\item If single dataset input: Returns curated data.table
\item If list/multiple files input: Returns list of curated data.tables (when \code{return.data = TRUE})
\item If \code{return.data = FALSE}: Returns invisibly NULL (use with \code{save.files = TRUE})
}
}
\description{
This function identifies and processes outliers in a specified sensor variable by evaluating both
rapid changes between consecutive measurements and prolonged constant readings that may indicate
sensor failure (stall periods). Outliers are flagged when the rate of change exceeds a user-defined
threshold, accounting for sensor resolution and accuracy. Additionally, the function detects
prolonged periods of unchanged sensor values, which could suggest sensor malfunction.
The function classifies outliers into two categories:
\enumerate{
\item \strong{Isolated Outliers}: Singular, transient anomalies (e.g., sensor glitches or logging errors).
These values (spikes) are replaced with \code{NA} or, optionally, smoothed using linear interpolation.
\item \strong{Consecutive Anomaly Periods}: Clusters of outliers occurring within a specified time window,
likely due to sensor malfunction or saturation. The function removes all affected values and replaces
small isolated blocks of valid readings within these periods with \code{NA} to ensure data integrity.
No data interpolation is applied in these cases.
}
}
\note{
For large datasets, consider processing in batches with \code{save.files = TRUE} and
\code{return.data = FALSE} to avoid memory overload.
}
