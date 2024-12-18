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
# ---> Generate summary statistics on key metrics for all animals.

# NOTE: The current functions were fine-tuned to analyze whale-shark data. Hence, further adjustments
# might be needed when analyzing different species, as behavioral and movement patterns may vary.


################################################################################
# Directory Structure ##########################################################
################################################################################

# To ensure the functions works correctly, organize the data into a root directory containing
# one subdirectory for each tagged animal. Each animal's subdirectory must include:
#
# 1. Multi-Sensor Tag Data Folder (Default: "CMD")
#    - This folder contains time-series data collected by the multi-sensor tag.
#    - The default name is "CMD," but it can be customized using the 'sensor.folders' argument.
#    - Files should include depth, accelerometer, gyroscope, magnetometer data in CSV format.
#
# 2. PSAT Data Folder (Optional)
#    - For integrating positions from pop-up satellite archival tags (PSATs).
#    - The name can be specified using the 'psat.folders' argument or left as NULL for auto-detection.
#    - This folder must contain a "Location.csv" file with Fastloc GPS or geolocation data.
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
animal_metadata <- read_excel("./PINTADO_metadata_multisensor.xlsx")

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

# Standardize tag labels
animal_metadata$tag[animal_metadata$tag=="4k"] <- "4K"
animal_metadata$tag[animal_metadata$tag=="Ceiia"] <- "CEIIA"


################################################################################
# Configure IMU Axes Mapping ###################################################
################################################################################

# Specify file containing IMU (Inertial Measurement Unit) axes configuration
axes_config_file <- "./axes_configuration.csv"

# If configuration file does not exist, create a default axes mapping
if(!file.exists(axes_config_file)){
  # Mapping for CATS mini-Diary TAG
  axes_config <- data.frame("tag"=NA, "from"=NA, "to"=NA)
  axes_config[1,] <- c("CATS", "ax", "-ay")
  axes_config[2,] <- c("CATS", "ay", "-ax")
  # Mapping for CEiiA CAM TAG
  axes_config[3,] <- c("CEIIA", "ax", "az")
  axes_config[4,] <- c("CEIIA", "ay", "-ax")
  axes_config[5,] <- c("CEIIA", "az", "ay")
  axes_config[6,] <- c("CEIIA", "mx", "mz")
  axes_config[7,] <- c("CEIIA", "mz", "-mx")
  # Save the default configuration to a CSV file for future use
  write.csv(axes_config, file=axes_config_file)
}else{
  # Load existing axes configuration
  axes_config <- read.csv(axes_config_file, header=TRUE)
}


################################################################################
# Process tag data #############################################################
################################################################################

# Define the folder containing tag data
data_folders <- list.dirs("/Users/Mig/Desktop/Whale Sharks/data", recursive = FALSE)

# Select specific folders within the source data folder (if needed)
data_folders <- data_folders[24:25]

# Process tag data using the "processTagData" function from the "nautilus" package
data_list <- processTagData(data.folders = data_folders,
                            id.metadata = animal_metadata,
                            lon.col = "deploy_lon",
                            lat.col = "deploy_lat",
                            tagdate.col = "deploy_date",
                            axis.mapping = axes_config,
                            verbose = TRUE)


################################################################################
# Filter out pre- and post-deployment data #####################################
################################################################################

# Create an empty list to store plots for each dataset
plot_list <- vector("list", length=length(data_list))

# Loop through each animal dataset in the 'data_list'
for(i in 1:length(data_list)){
  # Apply the 'filterDeploymentData' function to filter pre- and post-deployment periods
  data_list[[i]] <- filterDeploymentData(data = data_list[[i]],
                                         depth.threshold = 3.5,
                                         variance.threshold = 6,
                                         max.changepoints = 6,
                                         plot.metrics = c("pitch", "sway"),
                                         plot.metrics.labels = c("Pitch (degrees)", "Sway (m/s)"))
  # Capture the current plot for later export
  plot_list[[i]] <- recordPlot()
}

# Save the recorded plots to a PDF file for reviewing the filtered data.
plot_list <- plot_list[!sapply(plot_list,is.null)]
pdf("./filtered_deployments.pdf", width = 8, height = 5.5)
for(i in 1:length(plot_list)){
  replayPlot(plot_list[[i]], reloadPkgs = FALSE)
}
dev.off()


################################################################################
# Save processed data to CSV ###################################################
################################################################################

# Specify the directory where the CSV files will be saved
output_directory <- "./processed_data/"

# Create the output directory if it doesn't exist
if(!dir.exists(output_directory)){
  dir.create(output_directory)
}

# Loop through each animal dataset and save the filtered data as CSV
for(i in 1:length(data_list)){
  # Construct the output filename
  output_file <- paste0(output_directory, names(data_list)[i], "_processed.csv")
  # Write the processed data to CSV
  write.csv(data_list[[i]], file = output_file, row.names = FALSE)
 }


################################################################################
# Generate summary statistics  #################################################
################################################################################

# Generate summary statistics for each individual using the 'summarizeTagData' function.
# The function calculates key metrics like deployment duration, temperature range, max depth,
# sampling frequency, and GPS positions (Fastloc and user-defined) based on the provided data.

# Call the summarizeTagData function to generate the summary statistics for the data.
summary <- summarizeTagData(data_list)

# Print the summary table
print(summary)


###############################################################################################
###############################################################################################
###############################################################################################
