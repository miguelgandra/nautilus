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
data_files <- list.files("./data processed/filtered/20Hz", full.names = TRUE)
filtered_list <- vector("list", length(data_files))
for(i in 1:length(data_files)){
  # Save the combined list as an RDS file
  filtered_list[[i]] <- readRDS(data_files[i])
  names(filtered_list)[i] <- attributes(filtered_list[[i]])$id
}

# Select or exclude specific folders within the source data folder (if needed)
filtered_list <- filtered_list[48:51]
gc()


################################################################################
## TEMPORARY - remove duplicated locations #####################################
################################################################################

filtered_list <- lapply(filtered_list, function(dt) {
  # round datetime to seconds (as integer)
  rounded_sec <- as.integer(dt$datetime)
  # identify valid location rows
  valid_pos <- !is.na(dt$lat) & !is.na(dt$lon)
  # among valid positions, mark duplicates within same second
  dup_pos <- duplicated(rounded_sec[valid_pos])
  # nullify all but the first position within each second
  dt[which(valid_pos)[dup_pos], c("lat", "lon", "position_type", "quality") := NA]
  dt
})

gc()

################################################################################
# Filter Fastloc locations #####################################################
################################################################################

# 10 km/h as a conservative max cruising speed for whale sharks
filtered_list <- filterLocations(data = filtered_list,
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

pseudo_tracks <- estimate3DPseudoTrack(data = filtered_list,
                                       id.metadata = animal_metadata,
                                       deploy.lon.col = "deploy_lon",
                                       deploy.lat.col = "deploy_lat",
                                       animal.mass = 15000,
                                       water.density = 1025,
                                       tag.drag.coef = 1.2,
                                       tag.area = 0.015,
                                       max.speed = 3,
                                       vpc.method = "distance",
                                       vpc.thresh = 1000,
                                       vpc.dist.step = 1,
                                       vpc.bound = TRUE,
                                       cores = 4)



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
