###############################################################################################
## Miguel Gandra || CCMAR || m3gandra@gmail.com || December 2024 ##############################
## Tutorial: Processing Multi-Sensor Data with the 'nautilus' R Package #######################
###############################################################################################

# This script provides a step-by-step guide for using the "nautilus" R package to
# efficiently import and process archival tag data. It demonstrates the following features:
#
# ---> Import, standardize and process archival tag data from multiple animals.
# ---> Integrate and merge location data derived from PSATs (pop-up satellite archival tags)
# ---> Automatically filter pre- and post-deployment data to focus on animal attachment periods.
# ---> Automatically compute key metrics on acceleration, orientation, and linear motion.
# ---> Optionally downsample high-frequency sensor data to reduce volume and enhance manageability.
# ---> Clean up spurious data sections before or after large time gaps.
# ---> Detect and remove sensor outliers based on abrupt changes or prolonged flat-line readings.
# ---> Estimate tail beat frequencies based on wavelet analysis.
# ---> Generate summary statistics on key metrics for all animals.
# ---> Generate depth profiles for all animals (color-coded by temperature).


# NOTE: This package's functions were optimized for biologging data from G-Pilot and i-Pilot tags,
# with current parameters specifically fine-tuned for whale shark kinematics and behavioral patterns.
# When analyzing other species or tag systems, adjustments may be needed to account for differences
# in movement ecology, sensor specifications, or data structure.


################################################################################
# Directory Structure ##########################################################
################################################################################

# To ensure the functions work correctly, organize the data into a root directory containing
# one subdirectory for each tagged animal. Each animal's subdirectory must include:
#
# 1. Multi-Sensor Tag Data Folder (default: "CMD")
#    - Contains a CSV file with time-series data from multi-sensor tags
#      (e.g., depth, accelerometer, gyroscope, magnetometer).
#    - The folder name defaults to "CMD" but can be customized via the 'sensor.folders' argument.
#
# 2. Wildlife Computers Tag Data Folder (Optional)
#    - For integrating positions from Wildlife Computers tags (MiniPAT/MK10/SPOT).
#    - The name can be specified using the 'wc.subdirectory' argument or left as NULL for auto-detection.
#    - This folder must contain location data files in the standard Wildlife Computers format.
#
# Each animal folder name must match the "ID" in the metadata file ('id.metadata').
#
#
# Example directory layout:
# Root_Directory/
# ├── ID_01/
# │   ├── CMD/
# │   │   └── xxxxx-Multisensor22Splash52.csv
# │   └── SPOT/
# │       ├── xxxxx-Locations.csv
# │       └── other Wildlife Computers files
# └── ID_02/
#     ├── CMD/
#     │   └── xxxxx-CamaraCMD134Spot98.csv
#     └── MK10/
#         ├── xxxxx-Locations.csv
#         └── other Wildlife Computers files


################################################################################
# Install and load required packages ###########################################
################################################################################

# Check if the "readxl" package is installed; if not, install it
if(!require(readxl)){install.packages("readxl"); library(readxl)}

# Install and load the 'nautilus' package from GitHub
#devtools::install_github("miguelgandra/nautilus")
library(nautilus)


################################################################################
## Import animal metadata ######################################################
################################################################################

# Read metadata file containing details about tagged animals
animal_metadata <- readxl::read_excel("./PINTADO_metadata_multisensor.xlsx")

# Select relevant columns
selected_cols <- c("id", "dateTime", "site", "longitudeD", "latitudeD",
                   "sex", "size", "Nmax", "typeCMD", "PakageID", "satPtt", "padWheel",
                   "recoveryDate", "recoveryTime", "lonRecov", "latRecov",
                   "popupDatetime", "latPop", "lonPop")
animal_metadata <- as.data.frame(animal_metadata)[,selected_cols]

# Update column names to standardized format for further processing
colnames(animal_metadata) <- c("ID", "deploy_date", "deploy_site", "deploy_lon", "deploy_lat",
                               "sex", "size", "n_animals", "tag", "package_id", "satPtt", "paddle_wheel",
                               "recover_date", "recover_time", "recover_lon", "recover_lat",
                               "popup_date", "popup_lat", "popup_lon")

# Extract year from POSIXct deploy_date
animal_metadata$deploy_year <- as.integer(format(animal_metadata$deploy_date, "%Y"))

# Standardize tag labels
animal_metadata$tag[animal_metadata$tag=="4k"] <- "4K"
animal_metadata$tag[animal_metadata$tag=="Ceiia"] <- "CEIIA"

# Update CEIIA tags with year suffix for 2022 deployments
animal_metadata$tag[animal_metadata$tag == "CEIIA" & animal_metadata$deploy_year == 2022] <- "CEIIA_2022"



################################################################################
# Configure IMU Axes Mapping ###################################################
################################################################################

# Configure the axes mapping for IMU (Inertial Measurement Unit) data.
# It specifies how raw sensor axes (accelerometer, magnetometer, gyroscope) are
# mapped for different tags to match the NED (North-East-Down) coordinate system,
# or marked as NA for known faulty sensors.
# The "type" is automatically determined based on the ID names:
# - If the ID name includes the substring "CAM", it will be classified as type "CAM".
# - If no such match is found, the type will default to "CMD".
# The "tag" column in the configuration should match the tags included in the ID metadata.
# Accelerometer axes: ax, ay, and az.
# Magnetometer axes: mx, my, and mz.
# Gyroscope axes: gx, gy, and gz.

# Create the data frame for axis mapping
axes_config <- data.frame(
  type = character(),
  tag = character(),
  from = character(),
  to = character()
)

# Mapping for CATS CAM TAG
axes_config[1, ] <- c("CAM", "CATS", "ax", "-ax")
axes_config[2, ] <- c("CAM", "CATS", "ay", "-ay")
axes_config[3, ] <- c("CAM", "CATS", "az", "-az")

# Mapping for CEiiA CAM TAG (general)
axes_config[4, ] <- c("CAM", "CEIIA", "ax", "az")
axes_config[5, ] <- c("CAM", "CEIIA", "ay", "-ax")
axes_config[6, ] <- c("CAM", "CEIIA", "az", "ay")
axes_config[7, ] <- c("CAM", "CEIIA", "mx", "mz")
axes_config[8, ] <- c("CAM", "CEIIA", "mz", "-mx")

# Special mapping for CEiiA CAM TAG 2022 (faulty mag and gyr sensors)
axes_config[9, ] <- c("CAM", "CEIIA_2022", "mx", "NA")
axes_config[10, ] <- c("CAM", "CEIIA_2022", "my", "NA")
axes_config[11, ] <- c("CAM", "CEIIA_2022", "mz", "NA")
axes_config[12, ] <- c("CAM", "CEIIA_2022", "gx", "NA")
axes_config[13, ] <- c("CAM", "CEIIA_2022", "gy", "NA")
axes_config[14, ] <- c("CAM", "CEIIA_2022", "gz", "NA")

# Mapping for CATS mini-Diary TAG (CMD)
axes_config[15, ] <- c("CMD", "CATS", "ax", "-ay")
axes_config[16, ] <- c("CMD", "CATS", "ay", "-ax")


################################################################################
# Configure Custom Column Import Mapping #######################################
################################################################################

# In cases where your biologging data uses non-standard column names or units,
# you can specify a custom import column mapping, via the 'import.mapping' parameter
# of the importTagData() function. This specifies how raw data columns (with their
# original names and units) are mapped to standardized sensor names and units used
# in the analysis pipeline.
#
# The mapping must include three columns:
# - colname: Exact column name as it appears in the CSV file
# - sensor: Standardized sensor name (see valid options below)
# - units: Measurement units (must match supported unit types)
#
# Valid sensor names:
# - Time: "date", "time", "datetime"
# - Motion: "ax", "ay", "az" (accelerometer)
#           "gx", "gy", "gz" (gyroscope)
#           "mx", "my", "mz" (magnetometer)
# - Environmental: "depth", "temp"
#
# Valid units:
# - Time: "UTC"
# - Acceleration: "m/s2", "g"
# - Rotation: "mrad/s", "rad/s", "deg/s"
# - Magnetic: "uT"
# - Temperature: "C"
# - Depth: "m"
# - Unitless: "" (empty string)

# # Create the data frame for import mapping
# import_config <- data.frame(
#   colname = character(),
#   sensor = character(),
#   units = character(),
#   stringsAsFactors = FALSE
# )
#
# # Mapping for Custom CSV format (Example)
# import_config[26, ] <- c("UTC_Time", "datetime", "UTC")
# import_config[27, ] <- c("ACC_X", "ax", "g")
# import_config[28, ] <- c("ACC_Y", "ay", "g")
# import_config[29, ] <- c("ACC_Z", "az", "g")
# import_config[30, ] <- c("GYRO_X", "gx", "rad/s")
# import_config[31, ] <- c("GYRO_Y", "gy", "rad/s")
# import_config[32, ] <- c("GYRO_Z", "gz", "rad/s")
# import_config[33, ] <- c("MAG_X", "mx", "uT")
# import_config[34, ] <- c("MAG_Y", "my", "uT")
# import_config[35, ] <- c("MAG_Z", "mz", "uT")
# import_config[36, ] <- c("TEMP", "temp", "C")
# import_config[37, ] <- c("PRESSURE", "depth", "m")


################################################################################
# Import tag data #############################################################
################################################################################

# This section imports and standardizes raw tag data from multiple deployments
# using the `importTagData` function.
#
# To avoid loading all datasets into memory simultaneously, we set:
#   - `return.data = FALSE` to prevent storing data in RAM
#   - `save.files = TRUE` to save each processed dataset directly to disk
#
# This way, each individual's data is handled sequentially and saved as an `.rds` file
# during processing. The final `data_list` object will be empty, but all files will be
# safely stored in the specified output folder. This workflow is recommended when
# working with large datasets or limited memory.
#
# For details on all available arguments, run: ?importTagData


# Define the folder containing tag data
data_folders <- list.dirs("/Users/Mig/Desktop/Whale Sharks/data", recursive = FALSE)

# Select or exclude specific folders within the source data folder (if needed)
data_folders <- data_folders[1:58]

# Import and standardize tag data using the "importTagData" function
data_list <- importTagData(data.folders = data_folders,
                           sensor.subdirectory = "CMD",
                           wc.subdirectory = NULL,
                           timezone = "UTC",
                           import.mapping = NULL,
                           axis.mapping = axes_config,
                           id.metadata = animal_metadata,
                           id.col = "ID",
                           tag.col = "tag",
                           deploy.date.col = "deploy_date",
                           deploy.lon.col = "deploy_lon",
                           deploy.lat.col = "deploy_lat",
                           pop.date.col = "popup_date",
                           pop.lon.col = "popup_lon",
                           pop.lat.col = "popup_lat",
                           package.id.col = "package_id",
                           paddle.wheel.col = "paddle_wheel",
                           return.data = FALSE,
                           save.files = TRUE,
                           output.folder = "./data processed/imported/",
                           output.suffix = NULL,
                           data.table.threads = NULL,
                           verbose = TRUE)


################################################################################
# Filter out pre- and post-deployment data #####################################
################################################################################

# The `filterDeploymentData` function offers two ways to define the deployment period:

# 1. Automated Depth-Based Detection:
#    If no manual input is provided, the function applies a binary segmentation
#    algorithm to detect shifts in depth and its variance. These changes are
#    used to estimate the most likely attachment and detachment times. This method
#    is useful for quickly processing multiple individuals without prior knowledge
#    of exact deployment intervals.

# 2. Custom Deployment Times:
#    Alternatively, users can supply known deployment windows by passing a
#    `data.frame` to the `custom.deployment.times` argument. Each row must
#    contain an `ID`, `start`, and `end` time (as POSIXct objects). This input
#    overrides the automated detection and allows precise truncation of the
#    dataset to match known deployment intervals.

# Diagnostic plots are generated for each filtered dataset to visually assess
# the effectiveness of the filtering and review data quality and trends
# within the identified deployment window.


# Define known deployment intervals (manual input)
deploy_list <- list(
  list(ID = "PIN_02", start = as.POSIXct("2019-09-11 12:35:00", tz = "UTC"), end = as.POSIXct("2019-09-12 16:32:00", tz = "UTC")),
  list(ID = "PIN_09", start = as.POSIXct("2020-08-22 15:20:00", tz = "UTC"), end = as.POSIXct("2020-08-23 00:49:03", tz = "UTC")),
  list(ID = "PIN_10", start = as.POSIXct("2020-08-23 16:25:00", tz = "UTC"), end = as.POSIXct("2020-08-23 20:48:24", tz = "UTC")),
  list(ID = "PIN_16", start = as.POSIXct("2022-09-18 17:48:00", tz = "UTC"), end = as.POSIXct("2022-09-19 08:34:00", tz = "UTC")),
  list(ID = "PIN_23", start = as.POSIXct("2023-08-31 11:00:00", tz = "UTC"), end = as.POSIXct("2023-08-31 21:45:25", tz = "UTC")),
  list(ID = "PIN_CAM_03", start = as.POSIXct("2023-08-31 11:00:00", tz = "UTC"), end = as.POSIXct("2023-08-31 21:45:25", tz = "UTC")),
  list(ID = "PIN_CAM_13", start = as.POSIXct("2019-09-27 10:25:00", tz = "UTC"), end = as.POSIXct("2019-09-27 12:46:42", tz = "UTC"))
)
deploy_periods <- do.call(rbind, lapply(deploy_list, as.data.frame))


# Load the previously processed/downsampled files
data_files <- list.files("./data processed/imported", full.names = TRUE)

# Apply the 'filterDeploymentData' function to filter pre- and post-deployment periods
filter_results <- filterDeploymentData(data = data_files,
                                       id.col = "ID",
                                       datetime.col = "datetime",
                                       depth.col = "depth",
                                       custom.deployment.times = deploy_periods,
                                       depth.threshold = 3.5,
                                       variance.threshold = 6,
                                       max.changepoints = 6,
                                       display.plots = FALSE,
                                       save.plots = TRUE,
                                       plot.metrics = c("temp", "ax"),
                                       plot.metrics.labels = c("Temperature (\u00B0C)", "Acc X (\u00B0)"),
                                       return.data = FALSE,
                                       save.files = TRUE,
                                       output.folder = "./data processed/filtered/",
                                       output.suffix = NULL)


# Save the recorded plots to a PDF file for reviewing the filtered data.
pdf("./plots/filtered_deployments.pdf", width = 8.5, height = 5)
for(i in 1:length(filter_results$plots)){
  replayPlot(filter_results$plots[[i]], reloadPkgs = FALSE)
}
dev.off()




################################################################################
## Import paddle wheel calibration values ######################################
################################################################################

# Some tags have a magnetic paddle wheel that spins as the animal swims,
# enabling speed estimation from rotation frequency. processTagData()
# extracts these frequencies by applying a Fast Fourier Transform (FFT)
# to overlapping windows of magnetometer z-axis data to identify the dominant
# frequency, which is then converted to speed using a tag-specific calibration
# slope (assuming a zero y-intercept).

# This file holds calibration slopes and model fit metrics from linear regressions
# relating known speeds to paddle rotation frequencies. It must be provided to
# processTagData() to convert raw frequencies into actual speed estimates.
# Required columns: "year", "package_id", and "slope".

# Since not all deployments have associated calibration values, we apply the following
# logic to fill missing values:
# 1. For each tag (identified by package_id), available slopes from different years
#    are used to interpolate or extrapolate missing years using linear interpolation.
# 2. When no slope values are available for a given package_id, the global average
#    slope across all calibrated tags is used as a fallback. This baseline is then
#    adjusted for each deployment year using a user-defined annual slope increase,
#    accounting for the expected decline in paddle wheel performance over time.


# Import available speed regression values
calibration_regression <- read.csv("./paddle wheel calibration/Velocity_RotationHz_Regression.csv")
colnames(calibration_regression) <- c("year", "package_id", "slope", "r.squared", "adj.r.squared")
calibration_regression_sorted <- calibration_regression[order(calibration_regression$package_id, calibration_regression$year), ]

# Define the expected increase in slope per year due to paddle wheel performance decline.
# This value reflects how much the slope is expected to increase annually.
# A positive value means it takes more Hz to achieve the same m/s, implying reduced efficiency.
yearly_slope_increase <- 0.01

# Filter for tags with paddle wheels
paddle_metadata <- animal_metadata[animal_metadata$paddle_wheel == TRUE, ]
# Keep only unique combinations of deploy_year and package_id
expected_grid <- unique(paddle_metadata[, c("deploy_year", "package_id")])
colnames(expected_grid)[colnames(expected_grid) == "deploy_year"] <- "year"

# Merge with actual data and sort by package_id and year
paddle_calibration <- merge(expected_grid, calibration_regression, all.x = TRUE)
paddle_calibration <- paddle_calibration[order(paddle_calibration$package_id, paddle_calibration$year), ]

# Create a 'processed_slope' column to store our final, filled values
paddle_calibration$processed_slope <- NA

# Find the first calibration year and its corresponding slope for each unique package_id
first_calibration_per_tag <- calibration_regression_sorted[!duplicated(calibration_regression_sorted$package_id), ]
# Calculate the mean of these first-year slopes
global_avg_slope <- mean(first_calibration_per_tag$slope, na.rm = TRUE)

# Get a list of all unique package IDs
all_package_ids <- unique(paddle_calibration$package_id)

# Loop through each unique package ID for interpolation
for (pkg_id in all_package_ids) {
  idx_pkg <- which(paddle_calibration$package_id == pkg_id)
  current_years <- paddle_calibration$year[idx_pkg]
  current_slopes_original <- paddle_calibration$slope[idx_pkg]
  known_years <- current_years[!is.na(current_slopes_original)]
  known_slopes <- current_slopes_original[!is.na(current_slopes_original)]
  if (length(known_years) == 0) {
    # Case 1: No known slopes for this package ID at all.
    # Use the global average as a baseline and apply the general yearly trend.
    for (i in seq_along(current_years)) {
      year_offset <- current_years[i] - min(current_years)
      paddle_calibration$processed_slope[idx_pkg[i]] <- global_avg_slope + (year_offset * yearly_slope_increase)
    }
  } else if (length(known_years) == 1) {
    # Case 2: Exactly one known slope for this package ID.
    # Use this single known slope as a baseline for its specific year,
    # and adjust other years for this tag using the custom yearly trend.
    known_year_single <- known_years[1]
    known_slope_single <- known_slopes[1]
    for (i in seq_along(current_years)) {
      year_offset <- current_years[i] - known_year_single
      paddle_calibration$processed_slope[idx_pkg[i]] <- known_slope_single + (year_offset * yearly_slope_increase)
    }
  } else {
    # Case 3: Two or more known slopes for this package ID.
    # Perform linear interpolation/extrapolation using 'approx'.
    # This case relies on the observed trend for that specific tag if available,
    # as it's the most data-driven approach for that tag.
    interp_values <- approx(x = known_years, y = known_slopes, xout = current_years,
                            method = "linear", rule = 2)$y
    paddle_calibration$processed_slope[idx_pkg] <- interp_values
  }
}

# Replace the original column with the processed values
paddle_calibration$slope <- paddle_calibration$processed_slope
paddle_calibration$processed_slope <- NULL



################################################################################
# Process tag data #############################################################
################################################################################

# In this section, we process the previously filtered high-resolution datasets.
# The `processTagData()` function handles raw sensor signals (accelerometer,
# magnetometer, and gyroscope) and derives a wide array of orientation,
# kinematic, and motion metrics automatically.

# After computing metrics, data can be downsampled (e.g. to 20 Hz) to reduce
# resolution and file size for downstream analysis.

# To avoid loading all datasets into memory simultaneously, we set:
#   - `return.data = FALSE` to prevent storing data in RAM
#   - `save.files = TRUE` to save each processed dataset directly to disk

# For a complete description of all input arguments and the metrics computed,
# refer to the function help page:
#   ?processTagData


# Load the previously imported files
data_files <- list.files("./data processed/imported", full.names = TRUE)

# Process tag data using the "processTagData" function
data_list <- processTagData(data = data_files,
                            downsample.to = 20,
                            hard.iron.calibration = TRUE,
                            soft.iron.calibration = FALSE,
                            orientation.algorithm = "tilt_compass",
                            madgwick.beta = 0.02,
                            orientation.smoothing = 1,
                            pitch.warning.threshold = 45,
                            roll.warning.threshold = 45,
                            dba.window = 6,
                            dba.smoothing = 2,
                            motion.smoothing = 1,
                            speed.smoothing = 2,
                            calculate.paddle.speed = TRUE,
                            speed.calibration.values = paddle_calibration,
                            burst.quantiles = c(0.95, 0.99),
                            check.time.anomalies = TRUE,
                            return.data = FALSE,
                            save.files = TRUE,
                            output.folder = "./data processed/processed/20Hz",
                            output.suffix = NULL,
                            data.table.threads = NULL,
                            verbose = TRUE)



################################################################################
# Detect and remove sensor anomalies ###########################################
################################################################################

# The following code applies the "checkSensorAnomalies" function to detect and filter
# out potential sensor errors in depth and temperature time series data. The function
# identifies outliers based on two key criteria: rate of change (for detecting spikes or
# rapid fluctuations) and stall periods (for detecting sensor malfunctions or periods
# of constant readings). Optionally, the identified outliers can be replaced with
# interpolated values.

# Process depth time series to detect anomalies
filtered_list <- checkSensorAnomalies(data = filtered_list,
                                      id.col = "ID",
                                      sensor.col = "depth",
                                      sensor.name = "Depth",
                                      rate.threshold = 7,
                                      sensor.resolution = 0.5,
                                      sensor.accuracy.percent = 1,
                                      interpolate = TRUE)

# Process temperature time series to detect anomalies
filtered_list <- checkSensorAnomalies(data = filtered_list,
                                      id.col = "ID",
                                      sensor.col = "temp",
                                      sensor.name = "Temperature",
                                      rate.threshold = 1,
                                      sensor.resolution = 0.05,
                                      sensor.accuracy.fixed = 0.1,
                                      interpolate = TRUE)


# Recalculate Vertical Speed After Depth Corrections
# Recomputes vertical speed (m/s) for each dataset in the list following depth corrections.
# This must be called after `checkSensorAnomalies()` or any other function that modifies depth values,
# as vertical speed is derived from depth changes over time.
filtered_list <- lapply(filtered_list, function(x) {
  # Calculate vertical speed (m/s)
  x[, vertical_speed := c(NA, diff(depth) / as.numeric(diff(datetime), units = "secs"))]
  # Handle potential infinite values from division by zero
  x[is.infinite(vertical_speed), vertical_speed := NA_real_]
  # Optional smoothing (if speed.smoothing parameter exists)
  if("speed.smoothing" %in% names(attributes(x))) {
    sampling_freq <- attributes(x)$processed.sampling.frequency
    window_size <- sampling_freq * attributes(x)$speed.smoothing
    x[, vertical_speed := data.table::frollmean(vertical_speed, n = window_size, fill = NA, align = "center")]
  }
  return(x)
})


################################################################################
# Estimate Tail Beat Frequencies via Wavelet Analysis ##########################
################################################################################

# This function performs wavelet analysis to estimate tail beat frequencies from
# motion data (typically surge/sway acceleration).

# Important Note on Sampling Frequency:
# The input dataset must have a sampling frequency at least twice the chosen
# max.freq.Hz (Nyquist theorem). For reliable results, we recommend the sampling
# frequency be at least 4× higher than max.freq.Hz. For example, with max.freq.Hz = 2,
# the data should be sampled at ≥ 8 Hz (though ≥ 4 Hz would meet Nyquist minimum).

filtered_list <- calculateTailBeats(data = data_list,
                                    output.dir = "./plots/tail beat frequencies",
                                    motion.col = "surge",
                                    ridge.only = FALSE,
                                    min.freq.Hz = 0.02,
                                    max.freq.Hz = 2,
                                    smooth.window = 5,
                                    power.ratio.threshold = 2,
                                    max.interp.gap = 10,
                                    cores = 1)

################################################################################
# Save processed data ##########################################################
################################################################################

# Specify the directory where the CSV files will be saved
output_directory <- "./data processed/filtered/20Hz/"

# Loop through each animal dataset and save the processed data in RDS format (more storage-efficient)
# This also keeps the attributes associated with each dataset (required for the summarizeTagData() function)
for(i in 1:length(filtered_list)){
  # Construct the output filename
  suffix <- paste0(attributes(filtered_list[[i]])$processed.sampling.frequency, "Hz")
  output_file <- paste0(output_directory, names(filtered_list)[i], "-", suffix, ".rds")
  # Save the combined list as an RDS file
  saveRDS(filtered_list[[i]], file=output_file)
}

# Alternatively, save the filtered data as CSV for interoperability
#for(i in 1:length(data_list)){
#  # Construct the output filename
#  output_file <- paste0(output_directory, names(data_list)[i], "_processed.csv")
#  # Write the processed data to CSV
#  write.csv(data_list[[i]], file = output_file, row.names = FALSE)
# }


################################################################################
# Optional - Load processed files ##############################################
################################################################################

# Retrieve the list of processed files
data_files <- list.files("./data processed/imported", full.names = TRUE)

# Initialize a list to store the loaded datasets
filtered_list <- vector("list", length(data_files))

# Loop through each file, load the data, and assign names based on animal ID
for(i in 1:length(data_files)){
  filtered_list[[i]] <- readRDS(data_files[i])
  names(filtered_list)[i] <- attributes(filtered_list[[i]])$id
}


################################################################################
# Generate summary statistics  #################################################
################################################################################

# Generate summary statistics for each individual using the 'summarizeTagData' function.
# The function calculates key metrics like deployment duration, temperature range, max depth,
# sampling frequency, and GPS positions (Fastloc and user-defined) based on the provided data.

# Specify the metadata columns to include in the summary
metadata_cols <- c("ID", "sex", "size", "tag", "satPtt", "deploy_site")
# Extract the specified columns from the metadata table for summary calculations
summary_metadata <- animal_metadata[,metadata_cols]
# Rename the columns in the extracted metadata table for readability and consistency in the summary output
colnames(summary_metadata) <- c("ID", "Sex", "Size", "Tag", "SatPTT", "Tagging site")


# Call the summarizeTagData function to generate the summary statistics for each individual.
summary <- summarizeTagData(data = filtered_list,
                            id.metadata = summary_metadata)

# Print the summary table
print(summary)

# Save the summary table as CSV
write.csv(summary, file="./summary_table_all.csv", row.names = FALSE)


################################################################################
# Generate depth profiles  #####################################################
################################################################################

#' This section generates depth profiles for all animals in the dataset.
#' Depth values are plotted against time and color-coded by temperature.
#' The resulting plots are saved as a PDF file for further analysis.

# Generate depth profiles and save them to a PDF file
pdf("./plots/depth-profiles.pdf", width = 13, height = 9)
plotDepthProfiles(filtered_list,
                  animal_metadata,
                  color.by = "temp",
                  lon.col="deploy_lon",
                  lat.col = "deploy_lat")
# Close the PDF device
dev.off()


###############################################################################################
###############################################################################################
###############################################################################################
