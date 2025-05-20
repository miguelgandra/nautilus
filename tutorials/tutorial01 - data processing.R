###############################################################################################
## Miguel Gandra || CCMAR || m3gandra@gmail.com || December 2024 ##############################
## Tutorial: Processing Multi-Sensor Data with the 'nautilus' R Package #######################
###############################################################################################

# This script provides a step-by-step guide for using the "nautilus" R package to
# efficiently import and process archival tag data. It demonstrates the following features:
#
# ---> Import and process archival tag data from multiple animals.
# ---> Automatically compute key metrics on acceleration, orientation, and linear motion.
# ---> Integrate and merge location data derived from PSATs (pop-up satellite archival tags)
# ---> Optionally downsample high-frequency sensor data to reduce volume and enhance manageability.
# ---> Clean up spurious data sections before or after large time gaps.
# ---> Automatically filter pre- and post-deployment data to focus on animal attachment periods.
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
# 1. Multi-Sensor Tag Data Folder (Default: "CMD")
#    - This folder contains time-series data collected by the multi-sensor tag.
#    - The default name is "CMD," but it can be customized using the 'sensor.folders' argument.
#    - Files should include depth, accelerometer, gyroscope, magnetometer data in CSV format.
#
# 2. Wildlife Computers Tag Data Folder (Optional)
#    - For integrating positions from Wildlife Computers tags (MiniPAT/MK10/SPOT).
#    - The name can be specified using the 'wc.subdirectory' argument or left as NULL for auto-detection.
#    - This folder must contain location data files in the standard Wildlife Computers format.
#
# Each animal folder name must match the "ID" in the metadata file ('id.metadata').



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
                   "sex", "size", "Nmax", "typeCMD", "PakageID", "satPtt",
                   "recoveryDate", "recoveryTime", "lonRecov", "latRecov",
                   "popupDatetime", "latPop", "lonPop")
animal_metadata <- as.data.frame(animal_metadata)[,selected_cols]

# Update column names to standardized format for further processing
colnames(animal_metadata) <- c("ID", "deploy_date", "deploy_site", "deploy_lon", "deploy_lat",
                               "sex", "size", "n_animals", "tag", "packageID", "satPtt", "recover_date",
                               "recover_time", "recover_lon", "recover_lat", "popup_date",
                               "popup_lat", "popup_lon")

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
# Process tag data #############################################################
################################################################################

# NOTE: Specific folders can be selected to process the datasets in batches
# by defining a list of data folders to process. This can help avoid memory
# bottlenecks when processing many large datasets. By setting "save.files = TRUE",
# the processed files will be saved to the output folder.

# For detailed information on all available arguments and the computed metrics,
# please consult the function documentation by running:
# ?processTagData


# Define the folder containing tag data
data_folders <- list.dirs("/Users/Mig/Desktop/Whale Sharks/data", recursive = FALSE)

# Select or exclude specific folders within the source data folder (if needed)
data_folders <- data_folders[1:23]
#data_folders <- data_folders[24:58]


# Process tag data using the "processTagData" function from the "nautilus" package
data_list <- processTagData(data.folders = data_folders,
                            save.files = TRUE,
                            output.folder = "./data processed/complete/20Hz",
                            id.metadata = animal_metadata,
                            id.col = "ID",
                            tag.col = "tag",
                            deploy.date.col = "deploy_date",
                            deploy.lon.col = "deploy_lon",
                            deploy.lat.col = "deploy_lat",
                            pop.date.col = "popup_date",
                            pop.lon.col = "popup_lon",
                            pop.lat.col = "popup_lat",
                            axis.mapping = axes_config,
                            orientation.algorithm = "tilt_compass",
                            dba.window = 6,
                            dba.smoothing = 2,
                            orientation.smoothing = 1,
                            motion.smoothing = 1,
                            speed.smoothing = 2,
                            burst.quantiles = c(0.95, 0.99),
                            downsample.to = 20,
                            data.table.threads = 6,
                            verbose = TRUE)


################################################################################
# Optional - Load processed files ##############################################
################################################################################

# NOTE: If the datasets were processed in batches, the `data_files` list may not
# contain all the data. This step allows reloading all or specific datasets that
# were previously saved, so you can continue with the filtering and analysis steps.
# If all datasets were successfully processed and stored in the `data_list` object
# during the execution of the `processTagData` function, this step is not required.

# Retrieve the list of processed files
data_files <- list.files("./data processed/complete/20Hz", full.names = TRUE)

# Initialize a list to store the loaded datasets
data_list <- vector("list", length(data_files))

# Loop through each file, load the data, and assign names based on animal ID
for(i in 1:length(data_files)){
  data_list[[i]] <- readRDS(data_files[i])
  names(data_list)[i] <- attributes(data_list[[i]])$id
}



################################################################################
# Filter out pre- and post-deployment data #####################################
################################################################################

# Filter tag data to exclude pre- and post-deployment periods based on
# depth and variance thresholds. This step ensures that only relevant
# deployment data is kept for analysis. Additionally, diagnostic plots
# will be generated for each filtered dataset to visually assess the
# data quality and trends.

# Process data in subsets to manage memory usage and avoid memory overload errors
data_ms <- data_list[1:21]
data_cam <- data_list[22:53]
rm(data_list); gc()

# Apply the 'filterDeploymentData' function to filter pre- and post-deployment periods
filter_results <- filterDeploymentData(data = data_ms,
                                       depth.threshold = 3.5,
                                       variance.threshold = 6,
                                       max.changepoints = 6,
                                       plot.metrics = c("pitch", "sway"),
                                       plot.metrics.labels = c("Pitch (\u00BA)", "Sway (g)"),
                                       display.plots = FALSE,
                                       save.plots = TRUE)

# Extract the filtered data from the results
filtered_list <- filter_results$filtered_data

# Remove empty data tables from the list
filtered_list <- filtered_list[sapply(filtered_list, nrow) != 0]

# Save the recorded plots to a PDF file for reviewing the filtered data.
pdf("./plots/filtered_deployments_ms.pdf", width = 8, height = 5.5)
for(i in 1:length(filter_results$plots)){
  replayPlot(filter_results$plots[[i]], reloadPkgs = FALSE)
}
dev.off()


# Adjust deployment periods manually if needed
# Trim data for PIN_CAM_13 to exclude records after the tag pop-off time
popoff <- as.POSIXct("2019-09-27 12:46:42", tz="UTC")
filtered_list[["PIN_CAM_13"]] <- filtered_list[["PIN_CAM_13"]][datetime <= popoff]



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

filtered_list <- calculateTailBeats(data = filtered_list,
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
data_files <- list.files("./data processed/filtered/20Hz", full.names = TRUE)

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
