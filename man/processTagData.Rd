% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/processTagData.R
\name{processTagData}
\alias{processTagData}
\title{Import and Process Archival Tag Data}
\usage{
processTagData(
  data.folders,
  sensor.subdirectory = "CMD",
  psat.subdirectory = NULL,
  id.metadata,
  id.col = "ID",
  tag.col = "tag",
  lon.col = "lon",
  lat.col = "lat",
  tagdate.col = "tagging_date",
  axis.mapping = NULL,
  dba.window = 3,
  smoothing.window = 5,
  burst.quantiles = c(0.95, 0.99),
  downsample.to = 1,
  vertical.speed.threshold = NULL,
  verbose = TRUE
)
}
\arguments{
\item{data.folders}{Character vector. Paths to the folders containing data to be processed.
Each folder corresponds to an individual animal and should contain subdirectories with sensor data and possibly PSAT data.}

\item{sensor.subdirectory}{Character. Name of the subdirectory within each animal folder that contains sensor data (default: "CMD").
This subdirectory should include the sensor CSV files for the corresponding animal.}

\item{psat.subdirectory}{Character or NULL. Name of the subdirectory within each animal folder that contains PSAT data, or NULL to auto-detect PSAT folders (default: NULL).
This subdirectory should contain the "Location.csv" file with any fastloc position data from the PSAT.}

\item{id.metadata}{Data frame. Metadata about the IDs to associate with the processed data.
Must contain at least columns for ID and tag type.}

\item{id.col}{Character. Column name for ID in \code{id.metadata} (default: "ID").}

\item{tag.col}{Character. Column name for the tag type in \code{id.metadata} (default: "tag").}

\item{lon.col}{Character. Column name for longitude in sensor data (default: "lon").}

\item{lat.col}{Character. Column name for latitude in sensor data (default: "lat").}

\item{tagdate.col}{Character. Column name for the tagging date in \code{id.metadata} (default: "tagging_date").}

\item{axis.mapping}{Optional. A data frame containing the axis transformations for the IMU (Inertial Measurement Unit).
This parameter is used to correctly configure the IMU axes to match the North-East-Down (NED) frame.
The data frame should have three columns:
\itemize{
\item \strong{tag}: A column specifying the tag or sensor identifier. The tags indicated in this column should match the tag types in the \code{id.metadata} data frame.
\item \strong{from}: A column indicating the original axis in the sensor's coordinate system.
\item \strong{to}: A column specifying the target axis in the desired coordinate system.
}
Both signal and swap transformations are allowed. Transformations can be defined for different tags in case multiple tags were used.}

\item{dba.window}{Integer. Window size (in seconds) for calculating dynamic body acceleration. Defaults to 3.}

\item{smoothing.window}{Optional. The size of the moving window (in timesteps) used for smoothing the signals.
A larger value results in greater smoothing. Set to NULL to disable smoothing. Defaults to 5.}

\item{burst.quantiles}{Numeric vector. Quantiles (0-1) to define burst swimming events based on acceleration thresholds.
Use NULL to disable burst detection. Defaults to c(0.95, 0.99) (95th and 99th percentiles).}

\item{downsample.to}{Numeric. Downsampling frequency in Hz (e.g., 1 for 1 Hz) to reduce data resolution.
Use NULL to retain the original resolution. Defaults to 1.}

\item{vertical.speed.threshold}{Numeric. A threshold value for vertical displacement speed (in meters per second).
If the calculated vertical speed exceeds this value, the corresponding rows will be removed from the dataset.
This threshold is useful for removing data points where vertical speed may be artificially high due to occasional sensor malfunctioning or noise.
Default is \code{NULL}, which means no filtering will be applied based on vertical speed.}

\item{verbose}{Logical. If TRUE, the function will print detailed processing information. Defaults to TRUE.}
}
\value{
A list where each element contains the processed sensor data for an individual folder.
If PSAT data exists, additional processing for PSAT locations will be included.
}
\description{
This function imports and processes archival tag data from specified directories, supporting
sensor data for multiple individuals. It utilizes the fast \code{data.table::fread} function to
efficiently import CSV files. The function automatically computes several key metrics related to
acceleration, orientation, and linear motion based on accelerometer, magnetometer, and gyroscope time series
(see the Details section below for a list of metrics).
Additionally, the function can automatically integrate and merge positions obtained from pop-up satellite archival tags (PSATs).
The PSAT data should be included in a separate folder within each individual’s directory for correct integration.
To handle large datasets, the function provides the option to downsample the data to a specified frequency, reducing
the data resolution and volume after the metrics are calculated. This is particularly useful when working with high-frequency
data from sensors, making it more manageable for further analysis
}
\details{
The following key metrics are automatically calculated:

\strong{Acceleration:}
\itemize{
\item Total Acceleration: (m/s²) - The total magnitude of the animal's acceleration, calculated from the three orthogonal accelerometer components.
\item Vectorial Dynamic Body Acceleration (VeDBA): (m/s²) Quantifies the physical acceleration of the animal, calculated as the vector magnitude of the dynamic body acceleration, which is the difference between raw accelerometer data and the moving average (static acceleration).
\item Overall Dynamic Body Acceleration (ODBA): (m/s²) A scalar measure of the animal's overall acceleration, calculated as the sum of the absolute values of the dynamic acceleration components along the X, Y, and Z axes.
\item Burst Swimming Events: Identifies periods of high acceleration based on a given acceleration magnitude percentile, which can be used to detect burst swimming behavior. This metric is binary, indicating whether the acceleration exceeds the threshold.
}

\strong{Orientation:}
\itemize{
\item Roll: (degrees) The rotational movement of the animal around its longitudinal (x) axis, calculated using accelerometer and gyroscope data.
\item Pitch: (degrees) The rotational movement of the animal around its lateral (y) axis, computed from accelerometer and gyroscope measurements.
\item Heading: (degrees) The directional orientation of the animal, derived from the magnetometer data, representing the compass heading.
The heading is corrected according to the magnetic declination value estimated based on the tagging location of each animal to account for local magnetic variation.
}

\strong{Linear motion:}
\itemize{
\item Surge: (m/s) The forward-backward linear movement of the animal along its body axis, derived from the accelerometer data.
\item Sway: (m/s) The side-to-side linear movement along the lateral axis of the animal, also derived from the accelerometer data.
\item Heave: (m/s) The vertical linear movement of the animal along the vertical axis, estimated from accelerometer data.
}
}
