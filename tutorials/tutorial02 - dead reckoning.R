###############################################################################################
## Miguel Gandra || CCMAR || m3gandra@gmail.com || May 2025 ###################################
## Tutorial: Location Processing and Dead-Reckoning with the 'nautilus' R Package #############
###############################################################################################

# NOTE: This package's functions were optimized for biologging data from G-Pilot and i-Pilot tags,
# with current parameters specifically fine-tuned for whale shark kinematics and behavioral patterns.
# When analyzing other species or tag systems, adjustments may be needed to account for differences
# in movement ecology, sensor specifications, or data structure.


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

# Standardize tag labels
animal_metadata$tag[animal_metadata$tag=="4k"] <- "4K"
animal_metadata$tag[animal_metadata$tag=="Ceiia"] <- "CEIIA"


################################################################################
## Load processed files ########################################################
################################################################################

# Loop through each animal dataset and load the processed data in RDS format.
data_files <- list.files("./data processed/processed/20Hz", full.names = TRUE)
processed_list <- vector("list", length(data_files))
for(i in 1:length(data_files)){
  # Save the combined list as an RDS file
  processed_list[[i]] <- readRDS(data_files[i])
  names(processed_list)[i] <- attributes(processed_list[[i]])$id
}


################################################################################
## Analyze Relationship Between Tail-Beat Frequency and Paddle Speed ###########
################################################################################

qualifying_IDs <- c()
for (i in names(processed_list)) {
  # Check if 'tbf_hz' has at least one non-NA value
  has_non_NA_tbf <- any(!is.na( processed_list[[i]][["tbf_hz"]]))
  # Check if 'paddle_speed' has at least one non-NA value
  has_non_NA_paddle <- any(!is.na( processed_list[[i]][["paddle_speed"]]))
  # If both conditions are met, add the name to our list
  if (has_non_NA_tbf && has_non_NA_paddle) qualifying_IDs <- c(qualifying_IDs, i)
}

# Define window parameters
window_size_sec <- 5

# Define horizonal swimming thresholds
max_vertical_speed <- 0.5  # m/s
min_duration <- 60  # seconds (minimum sustained horizontal swimming)

# Process each dataset in the list
speed_results <- lapply(processed_list, function(dt) {

  # Check required columns exist
  if (!all(c("tbf_hz", "paddle_speed", "vertical_speed") %in% names(dt))) return(NULL)
  all_na_tbf <- all(is.na(dt$tbf_hz))
  all_na_paddle <- all(is.na(dt$paddle_speed))
  if(all_na_tbf && all_na_paddle) return(NULL)

  # Fetch sampling rate
  sampling_rate <- attributes(dt)$processed.sampling.frequency

  # Flag horizontal frames
  dt[, is_horizontal := abs(vertical_speed) < max_vertical_speed]

  # Identify sustained horizontal bouts (run length encoding)
  dt[, bout_id := data.table::rleid(is_horizontal)]

  # Calculate bout durations and filter
  bout_durations <- dt[, .(duration = max(datetime) - min(datetime)), by = bout_id][duration >= min_duration]

  # Calculate upper 25th percentile of tbf power ratios
  tbf_threshold <- quantile(dt$tbf_power_ratio, probs = 0.8, na.rm = TRUE)

  # Subset only qualifying horizontal bouts
  horizontal_dt <- dt[bout_id %in% bout_durations$bout_id & is_horizontal == TRUE]
  horizontal_dt <- horizontal_dt[tbf_power_ratio >= tbf_threshold]

  # Proceed only if sufficient data remains
  if (nrow(horizontal_dt) < min_duration * sampling_rate) return(NULL)

  # Create time windows (5s intervals)
  horizontal_dt[, window_id := floor(as.numeric(datetime) / window_size_sec)]

  # Calculate window averages (remove NA values)
  window_avgs <- horizontal_dt[, .(mean_tbf = mean(tbf_hz, na.rm = TRUE),
                                   mean_speed = mean(paddle_speed, na.rm = TRUE),
                                   n_obs = .N),
                               by = .(window_id)]

  # Filter windows with sufficient data (e.g., at least 50% coverage)
  window_avgs <- window_avgs[n_obs >= (window_size_sec * sampling_rate * 0.5)]

  # Only proceed if we have at least 3 windows with data
  if (nrow(window_avgs) < 3) return(NULL)

  # Additional check: make sure variables used in model are not NA
  if (all(is.na(window_avgs$mean_tbf)) || all(is.na(window_avgs$mean_speed))) return(NULL)


   # Fit linear model for this individual/trial
  model <- lm(mean_speed ~ mean_tbf, data = window_avgs)

  plot(mean_speed ~ mean_tbf, data = window_avgs, main = unique(dt$ID)[1], las=1)
  legend("topright", legend=paste0("R2 = ", round(summary(model)$r.squared, 2)), bty="n", cex=0.6)

  # Return results
  list(window_data = window_avgs,
       model_summary = summary(model),
       coefficients = coef(model),
       r_squared = summary(model)$r.squared)
})




################################################################################
# Filter Fastloc locations #####################################################
################################################################################

# 10 km/h as a conservative max cruising speed for whale sharks
processed_list <- filterLocations(data = processed_list,
                                  id.metadata = animal_metadata,
                                  deploy.lon.col = "deploy_lon",
                                  deploy.lat.col = "deploy_lat",
                                  max.speed.kmh = 10,
                                  max.distance.km = 200,
                                  min.satellites = NULL,
                                  plot = TRUE)


################################################################################
# Estimate Pseudo Tracks - Dead Reckoning ######################################
################################################################################


for (i in 1:length(processed_list)){

  individual_data <- processed_list[[i]]
  individual_data$lon[individual_data$position_type=="Metadata [deploy]"] <- NA
  individual_data$lat[individual_data$position_type=="Metadata [deploy]"] <- NA
  has_locs <- any(!is.na(individual_data$lon) & !is.na(individual_data$lat))
  if(has_locs){
    lon <- individual_data$lon
    lat <- individual_data$lat
  }else{
    lon <- NULL
    lat <- NULL
  }

  gundog_result <- Gundog.Tracks(TS = individual_data$datetime,
                                 h = individual_data$heading,
                                 v = 0.5,
                                 elv = individual_data$depth,
                                 ch = NULL,
                                 cs = NULL,
                                 m = 1,
                                 c = 1,
                                 ME = 1,
                                 la = attributes(individual_data)$deployment.info$lat,
                                 lo = attributes(individual_data)$deployment.info$lon,
                                 VP.lat = lon,
                                 VP.lon = lat,
                                 method="all",
                                 Outgoing = TRUE,
                                 bound = FALSE,
                                 plot = FALSE)

  if(!has_locs) {
    gundog_result <- gundog_result[,c("Timestamp", "DR.longitude", "DR.latitude",
                                      "DR.distance.2D", "DR.distance.3D",
                                      "DR.speed.2D", "DR.speed.3D")]
    colnames(gundog_result) <- c("datetime", "pseudo_lon", "pseudo_lat", "pseudo_distance_2D",
                                 "pseudo_distance_3D", "pseudo_speed_2D", "pseudo_speed_3D")
  }else{
    gundog_result <- gundog_result[,c("Timestamp", "DR.longitude.corr", "DR.latitude.corr",
                                      "DR.distance.2D", "DR.distance.3D",
                                      "DR.speed.2D", "DR.speed.3D")]
    colnames(gundog_result) <- c("datetime", "pseudo_lon", "pseudo_lat", "pseudo_distance_2D",
                                 "pseudo_distance_3D", "pseudo_speed_2D", "pseudo_speed_3D")
  }

  # merge
  pseudo_tracks <- merge(individual_data, gundog_result, by="datetime")




pseudo_tracks <- calculateTortuosity(data,
                                     id.col = "ID",
                                     lon.col = "lon",
                                     lat.col = "lat",
                                     datetime.col = "datetime",
                                     window.size = 10,
                                     metrics = "all",
                                     time.window = 24,
                                     min.points = 5)




pseudo_tracks <- estimate3DPseudoTrack(data = processed_list[1],
                                       id.metadata = animal_metadata,
                                       dba.col = "vedba",
                                       deploy.lon.col = "deploy_lon",
                                       deploy.lat.col = "deploy_lat",
                                       sampling.freq = 20,
                                       speed.model = "DBA",
                                       speed.coef = 1.0,
                                       speed.intercept = 0.0,
                                       fixed.speed = 1.0,
                                       smooth.window = 5,
                                       smooth.type = "median",
                                       vpc.method = "linear",
                                       vpc.freq = 60,
                                       cores = 4)

pseudo_tracks <- as.data.frame(pseudo_tracks$PIN_01)
p3 <- ggplot(pseudo_tracks, aes(x = pseudo_lon, y = pseudo_lat)) +
  geom_path(color = "blue") +
  # add points from data_plot, color-coded by position_type
  geom_point(data = data_plot, aes(x = lon, y = lat, color = position_type)) +
  theme_bw() +
  ggtitle(data_plot$ID[1]) +  # Using [1] to get a single value if ID is the same for all points
  scale_x_continuous(labels = function(x) sprintf("%.5f", x)) +
  scale_y_continuous(labels = function(x) sprintf("%.5f", x)) +
  # Optional: customize the color scale if needed
  scale_color_discrete(name = "Position Type")
print(p3)




################################################################################
## Retrieve video metadata #####################################################
################################################################################

# PIN_CAM_39
video_folders <- list.dirs("~/Desktop/Whale Sharks/CAMS", recursive = FALSE)
video_folders <- video_folders[grepl("PIN_CAM_39", video_folders, fixed=TRUE)]
video_info <- getVideoMetadata(video_folders,
                               id.metadata = animal_metadata,
                               tagdate.col = "deploy_date")
video_info$start[1] <- as.POSIXct("2022-10-10 15:42:27", tz="UTC")
video_info$start[2] <- as.POSIXct("2022-10-10 16:12:31", tz="UTC")
video_info$start[3] <- as.POSIXct("2022-10-10 16:42:35", tz="UTC")
video_info$start[4] <- as.POSIXct("2022-10-10 17:12:40", tz="UTC")
video_info$end[1] <- video_info$start[1] + video_info$duration[1]
video_info$end[2] <- video_info$start[2] + video_info$duration[2]
video_info$end[3] <- video_info$start[3] + video_info$duration[3]
video_info$end[4] <- video_info$start[4] + video_info$duration[4]


################################################################################
# Render overlays ##############################################################
################################################################################

# define paths
input_video <- "./CAMS/PIN_CAM_39/CamaraCMD134Spot98-20221010-154227-193-00003_feedingInteracoes.mp4"
renderOverlayVideo(video.file = input_video,
                   video.metadata = video_info,
                   sensor.data = pseudo_tracks2$PIN_CAM_39,
                   output.file = "~/Desktop/PIN_CAM_39-3D-v10.mp4",
                   output.directory = NULL,
                   start.time =  "00:01:10",
                   duration = 1 * 60 + 45,
                   epsg.code = 32626,
                   overlay.side = "left",
                   jpeg.quality = 4,
                   crf = 35,
                   cores = 5)

input_video <- "./CAMS/PIN_CAM_37/CamaraCMD134Spot98-20221004-161548-200-00007.mp4"
renderOverlayVideo(video.file = input_video,
                   video.metadata = video_info,
                   sensor.data = pseudo_tracks$PIN_CAM_37,
                   output.file = "~/Desktop/PIN_CAM_37-3D-v2.mp4",
                   output.directory = NULL,
                   start.time =  "00:23:00",
                   end.time =  "00:25:10",
                   epsg.code = 32626,
                   overlay.side = "left",
                   jpeg.quality = 4,
                   crf = 35,
                   cores = 5)



###############################################################################################
###############################################################################################
###############################################################################################
