###############################################################################################
## Miguel Gandra || CCMAR || m3gandra@gmail.com ###############################################
## Tutorial 01: Processing Multi-Sensor Tag Data with the 'nautilus' R Package ################
###############################################################################################

# A step-by-step walkthrough of the core 'nautilus' pipeline, from raw archival-tag exports to
# standardised, quality-controlled and analysis-ready datasets. The workflow uses a
# whale-shark project as a real-world example and covers:
#
#   1.  Prepare and quality-check the per-deployment metadata.
#   2.  Import and standardize raw multi-sensor tag data (CATS / CEiiA tags).
#   3.  Trim each record to the on-animal deployment period.
#   4.  Regularize timestamps onto a uniform grid and interpolate short gaps.
#   5.  Screen the sensor channels for structural faults and transient anomalies.
#   6.  Resolve the IMU axis orientation (documented, inferred, and video-verified).
#   7.  Derive orientation, kinematic and motion metrics (DBA, pitch/roll/heading, speed, ...),
#       with depth-drift correction and optional downsampling.
#   8.  Estimate tail-beat frequencies.
#   9.  Produce per-deployment summaries and depth profiles.
#
# Throughout the pipeline, data are stored as 'nautilus_tag' objects, which combine the sensor
# time series with deployment metadata, sensor provenance and a processing history. Inspect any
# object with print() or summary(); access its metadata with tagMetadata(x) and its processing
# history with processingHistory(x).
#
# Notes:
#  - Default parameters are tuned to whale-shark kinematics and the CATS / CEiiA tags used in
#    this example. For other species, tag systems or study designs, adjust the arguments accordingly.
#  - File paths below are project-specific; edit them to match your own directory layout.
#  - The tutorial uses a disk-based workflow: each stage reads the previous stage's .rds files and
#    writes its own output files, keeping large datasets out of memory. Output directories are not
#    created automatically, so create the required './data interim/', './plots/' and './outputs/'
#    directories before running the pipeline.


################################################################################
# Expected directory structure                                                 #
################################################################################

# Organize the raw data as a root directory with one subdirectory per tagged animal. Each animal's
# subdirectory holds:
#
#   1. A multi-sensor tag folder (default name "CMD", set via `sensor.subdirectory`) containing one
#      CSV of time-series data (depth, accelerometer, gyroscope, magnetometer, ...).
#   2. (Optional) A Wildlife Computers folder (MiniPAT / MK10 / SPOT) with the location files, for
#      integrating positions. Auto-detected, or named via `wc.subdirectory`.
#
# Each animal-folder name must match the deployment "ID" in the metadata.
#
#   Root_Directory/
#   |-- PIN_01/
#   |   |-- CMD/   xxxxx-Multisensor22Splash52.csv
#   |   \-- SPOT/  xxxxx-Locations.csv, ...
#   \-- PIN_02/
#       |-- CMD/   xxxxx-CamaraCMD134Spot98.csv
#       \-- MK10/  xxxxx-Locations.csv, ...


################################################################################
# STEP 0. Install and load                                                     #
################################################################################

# 'readxl' is used only to read the metadata spreadsheet in STEP 1.
if (!require(readxl)) { install.packages("readxl"); library(readxl) }

# Install 'nautilus' from GitHub once, then load it.
# remotes::install_github("miguelgandra/nautilus", build_vignettes = TRUE)
library(nautilus)


################################################################################
# STEP 1. Prepare the deployment metadata                                      #
################################################################################

# Before processing the sensor data, we assemble a table containing the metadata
# for each deployment, including tagging location and time, tag characteristics
# and relevant biological information. This metadata is used throughout the
# workflow to identify deployments and animals, account for differences between
# tag configurations and support subsequent processing steps.

# The aim of this step is to create a tidy data frame with one row per
# deployment. The metadata will be validated in STEP 2.


# Read the project metadata table, with one row representing each deployment.
animal_metadata <- readxl::read_excel("./metadata/PINTADO_metadata_multisensor_formatted.xlsx")

# Keep the fields required for the analysis and assign consistent column names.
selected_cols <- c("deployment_id", "animal_id", "deploy_datetime", "deploy_lat", "deploy_lon",          
                   "site", "sex", "size_m", "package_id", "logger_id", "tag_type", "tag_model",
                    "paddle_wheel", "argos_ptt", "deployment_type", "attachment_site", "attachment_side",   
                    "leader_length", "gtr_nominal_h", "recovery_datetime", "recovery_lat", "recovery_lon",
                    "popup_datetime", "popup_lat",  "popup_lon")
animal_metadata <- as.data.frame(animal_metadata)[, selected_cols]

# Extract the deployment year to distinguish hardware configurations that changed
# between field seasons.
animal_metadata$deploy_year <- as.integer(format(animal_metadata$deploy_datetime, "%Y"))

# Assign the IMU orientation configuration used for each deployment. Orientation
# can differ among tag models and hardware configurations, so these labels link
# each deployment to the corresponding axis mapping applied later in STEP 8.
animal_metadata$axis_config <- paste(animal_metadata$tag_model, animal_metadata$tag_type)
animal_metadata$axis_config[animal_metadata$logger_id == "71" & animal_metadata$deploy_year == 2019] <- "CATS 2019 Camera"
animal_metadata$axis_config[animal_metadata$logger_id == 27] <- paste("CATS 27", animal_metadata$tag_type[animal_metadata$logger_id == 27])
animal_metadata$axis_config[animal_metadata$tag_model == "CEIIA" & animal_metadata$deploy_year == 2022 & animal_metadata$package_id == 71]  <- "CEIIA 2022 (71)"
animal_metadata$axis_config[animal_metadata$tag_model == "CEIIA" & animal_metadata$deploy_year == 2022 & animal_metadata$package_id == 134] <- "CEIIA 2022 (134)"
animal_metadata$axis_config[animal_metadata$tag_model == "CEIIA" & animal_metadata$deploy_year == 2023] <- "CEIIA 2023 Camera"
animal_metadata$axis_config[animal_metadata$deployment_id == "PIN_CAM_26"] <- "4K Camera"

# Leave uncertain configurations unspecified so they can be resolved later using
# the sensor data and information from comparable deployments.
animal_metadata$axis_config[animal_metadata$deployment_id %in% c("PIN_10", "PIN_12")] <- ""



################################################################################
# STEP 2. Quality-check the deployment metadata                                #
################################################################################

# Validate the deployment metadata before importing the sensor data. This step
# identifies potential problems such as duplicate deployment IDs, invalid
# coordinates, inconsistent deployment and recovery times, or overlapping use of
# the same tag.

deployments <- checkDeploymentMetadata(
  animal_metadata,
  columns = metadataColumns(
    # Required metadata
    id              = "deployment_id",
    tag_model       = "tag_model",
    deploy_datetime = "deploy_datetime",    
    deploy_lon      = "deploy_lon",
    deploy_lat      = "deploy_lat",
    # Optional deployment and tag metadata
    animal_id         = "animal_id",
    tag_type          = "tag_type",
    deploy_site       = "site",
    recovery_datetime = "recovery_datetime",  
    popup_datetime    = "popup_datetime",        
    popup_lon         = "popup_lon",
    popup_lat         = "popup_lat",
    package_id        = "package_id",        
    logger_id         = "logger_id",             
    axis_config       = "axis_config",       
    paddle_wheel      = "paddle_wheel",
    attachment_site   = "attachment_site",
    deployment_type   = "deployment_type",   
    # Biological traits retained with each deployment for subsequent analyses
    # A corrected value can be re-stamped later with updateBiometrics() - no re-import needed.
    traits            = c("sex", "size_m")),
  verbose          = "detailed")

# Inspect the reported issues. Correct any problems in the source metadata and
# repeat the validation before proceeding to the sensor-data workflow.
issues(deployments)                      # all reported issues.
issues(deployments, severity = "error")  # errors requiring correction.


################################################################################
# STEP 3. Import the tag data                                                  #
################################################################################

# importTagData() reads the raw multi-sensor files, standardises sensor names
# and units, imports available ancillary location data and attaches the validated
# deployment metadata.

# Standard CATS and CEiiA file layouts are recognised automatically, so no
# explicit import mapping is required here. For non-standard data formats, an
# import mapping can be supplied to identify the relevant sensor columns and
# their units.

# The validated deployments object provides both the deployment metadata and its
# associated quality-control results. The sensor data are imported in their
# original axis configuration; orientation into the animal's body frame is
# performed later in STEP 8.

# Root directory containing one subdirectory per deployment
data_root <- "/Users/Mig/Desktop/Whale Sharks/data"
data_folders <- list.dirs(data_root, recursive = FALSE)

# Optionally restrict the set of deployments processed in this run.
data_folders <- data_folders[1:58]

# Import and standardise the deployment data and attach paired Wildlife Computers
# position and wet/dry records where available. The archival and Wildlife Computers
# tag clocks are aligned using depth cross-correlation, and any deployments excluded
# during import are recorded in the shared exclusions log. Standardised datasets are
# saved as compressed .rds files.
importTagData(data.folders         = data_folders,
              sensor.subdirectory  = "CMD",
              wc.subdirectory      = NULL,      # NULL = auto-detect the Wildlife Computers folder
              metadata             = deployments,
              import.mapping       = NULL,      # NULL = standard CATS / CEiiA layout
              import.calibration   = TRUE,
              timezone             = "UTC",
              alignment            = alignmentControl(method = "depth-xcorr"),
              exclusions.file      = "./data interim/exclusions.csv", 
              return.data          = FALSE,
              output.dir           = "./data interim/01_imported",
              compress             = TRUE,
              verbose              = "detailed")


################################################################################
# STEP 4. Trim to the deployment period                                        #
################################################################################

# filterDeploymentData() identifies the on-animal deployment period and filters
# each dataset to that interval, removing data recorded before attachment and
# after detachment.
#
# Deployment boundaries can be determined automatically from changes in depth
# and depth variability, or supplied manually through `custom.deployment.times`.
# Manual start/end times override automatic detection for the corresponding
# boundary; an NA value leaves that boundary to automatic detection.
#
# Deployments shorter than `min.deployment.hours` are excluded. Diagnostic plots
# can be generated for visual inspection of the detected deployment periods, and
# exclusions are recorded in the shared exclusions log.

# Known deployment windows (manual overrides; NA = let the algorithm find that boundary).
deploy_list <- list(
  list(ID = "PIN_02",     start = as.POSIXct("2019-09-11 12:35:00", tz = "UTC"), end = as.POSIXct("2019-09-12 16:32:00", tz = "UTC")),
  list(ID = "PIN_06D",    start = as.POSIXct("2019-09-27 15:43:32", tz = "UTC"), end = as.POSIXct("2019-09-27 16:07:39", tz = "UTC")),
  list(ID = "PIN_09",     start = as.POSIXct("2020-08-22 15:20:00", tz = "UTC"), end = as.POSIXct("2020-08-23 00:49:03", tz = "UTC")),
  list(ID = "PIN_10",     start = as.POSIXct("2020-08-23 16:20:00", tz = "UTC"), end = NA),
  list(ID = "PIN_16",     start = as.POSIXct("2022-09-18 17:48:00", tz = "UTC"), end = as.POSIXct("2022-09-19 08:34:00", tz = "UTC")),
  list(ID = "PIN_23",     start = as.POSIXct("2023-08-31 11:00:00", tz = "UTC"), end = NA),
  list(ID = "PIN_CAM_03", start = as.POSIXct("2019-09-10 11:53:03", tz = "UTC"), end = as.POSIXct("2019-09-10 12:25:37", tz = "UTC")),
  list(ID = "PIN_CAM_06", start = as.POSIXct("2019-09-10 15:47:37", tz = "UTC"), end = as.POSIXct("2019-09-10 16:21:34", tz = "UTC")),
  list(ID = "PIN_CAM_07", start = NA, end = as.POSIXct("2019-09-12 12:06:30", tz = "UTC")),
  list(ID = "PIN_CAM_08", start = NA, end = as.POSIXct("2019-09-12 17:22:14", tz = "UTC")),
  list(ID = "PIN_CAM_10", start = NA, end = as.POSIXct("2019-09-14 13:47:11", tz = "UTC")),
  list(ID = "PIN_CAM_11", start = NA, end = as.POSIXct("2019-09-14 18:44:44", tz = "UTC")),
  list(ID = "PIN_CAM_13", start = as.POSIXct("2019-09-27 10:25:00", tz = "UTC"), end = as.POSIXct("2019-09-27 12:46:44", tz = "UTC")),
  list(ID = "PIN_CAM_14", start = as.POSIXct("2019-09-27 12:17:14", tz = "UTC"), end = as.POSIXct("2019-09-27 12:35:35", tz = "UTC")),
  list(ID = "PIN_CAM_15", start = as.POSIXct("2019-09-27 14:06:23", tz = "UTC"), end = as.POSIXct("2019-09-27 14:33:55", tz = "UTC")),
  list(ID = "PIN_CAM_25", start = as.POSIXct("2020-10-14 13:53:17", tz = "UTC"), end = NA),
  list(ID = "PIN_CAM_26", start = NA, end = as.POSIXct("2021-09-08 18:12:07", tz = "UTC")),
  list(ID = "PIN_CAM_32", start = as.POSIXct("2022-09-17 13:34:40", tz = "UTC"), end = as.POSIXct("2022-09-18 10:31:52", tz = "UTC"))
)
deploy_periods <- do.call(rbind, lapply(deploy_list, as.data.frame))

# Filter each deployment to its on-animal period. Here, known deployment boundaries
# are supplied where available, while missing boundaries are detected automatically.
# A minimum duration of 1 hour is required.
filterDeploymentData(data                    = list.files("./data interim/01_imported", full.names = TRUE),
                     custom.deployment.times = deploy_periods,   # known windows; NA boundaries auto-detected
                     depth.threshold         = 3.5,    # depth (m) that counts as "in the water" for detection
                     variance.threshold      = 6,      # depth-variance change that marks attachment/detachment
                     max.changepoints        = 6,
                     use.temperature         = FALSE,  
                     min.deployment.hours    = 1,      # discard anything shorter than this
                     plot                    = FALSE,   
                     plot.file               = "./plots/filtered_deployments.pdf",  
                     plot.metrics            = c("temp", "az"),  
                     exclusions.file         = "./data interim/exclusions.csv",  
                     return.data             = FALSE,
                     output.dir              = "./data interim/02_filtered",
                     verbose                 = "detailed")


################################################################################
# STEP 5. Put the samples on a regular time grid                               #
################################################################################

# regularizeTimeSeries() places each deployment on an evenly spaced time grid
# based on its median observed sampling interval. Original observations are
# assigned to the nearest grid point, short gaps are interpolated, and longer
# gaps are retained as missing values rather than being filled across periods
# without measurements.

# Records that are already sufficiently regular are passed through unchanged.


regularizeTimeSeries(data                 = list.files("./data interim/02_filtered", full.names = TRUE),
                     gap.threshold        = 2,        # fill gaps up to 2 s; leave longer ones as NA (0 = never fill)
                     interpolation.method = "linear", # or "spline" / "locf"
                     plot                 = FALSE,    
                     plot.file            = "./plots/regularization.pdf",
                     exclusions.file      = "./data interim/exclusions.csv",  
                     return.data          = FALSE,
                     output.dir           = "./data interim/03_checked",
                     verbose              = "detailed")


################################################################################
# STEP 6. Screen the sensor channels                                           #
################################################################################

# The quality-control workflow consists of three sequential steps. First, sensor integrity 
# is assessed to identify unreliable channels. Next, transient anomalies such as spikes or 
# sensor malfunctions are corrected. Finally, GPS/Argos position fixes are screened for 
# implausible locations. Performing these steps before downstream analyses helps ensure the
# data are as reliable as possible.


## 6.1 Structural integrity ------------------------------------------------------------------------
# checkSensorIntegrity() performs an initial assessment of sensor channels to identify hardware- or 
# firmware-related issues, such as duplicated, unresponsive, clipped, or otherwise implausible signals. 
# This integrity check should be run before sensor quality control to ensure corrupted channels are 
# identified before any corrections are applied. The recommended workflow is to first review the results 
# and then re-run the function with apply = TRUE to remove channels flagged as unreliable.
integrity <- checkSensorIntegrity(data   = list.files("./data interim/03_checked", full.names = TRUE),
                                  checks = c("duplication", "dead", "saturation", "mag.plausibility",
                                             "accel.scale", "gyro.bias", "paddle.contamination", "dropout"),
                                  apply  = TRUE,
                                  apply.severity = "error",
                                  plot   = FALSE,
                                  plot.file   = "./plots/sensor_integrity.pdf",
                                  return.data = FALSE,
                                  output.dir  = "./data interim/03_checked",
                                  verbose     = "detailed")


## 6.2 Transient signal quality --------------------------------------------------------------------
#checkSensorQuality() identifies and corrects common issues in sensor data, such as isolated spikes and 
# periods where sensors become stuck or stop recording properly. Users define the expected behaviour of 
# each sensor channel with anomalyControl(), allowing the function to detect implausible values.
quality <- checkSensorQuality(data    = list.files("./data interim/03_checked", full.names = TRUE),
                              sensors = list(
                                depth = anomalyControl(rate.threshold = 7,   # max plausible change per second (m/s)
                                                       sensor.resolution = 0.5),
                                temp  = anomalyControl(rate.threshold = 1,    # deg C per second
                                                       sensor.resolution = 0.05)),
                              apply         = TRUE,
                              interpolate   = TRUE,     # patch isolated spikes (vs. setting them to NA)
                              plot          = FALSE,
                              plot.file     = "./plots/sensor_quality.pdf",
                              return.data   = FALSE,
                              output.dir    = "./data interim/03_checked",
                              verbose       = "detailed")


## 6.3 Position fixes ------------------------------------------------------------------------------
# The filterLocations() function performs quality control on GPS/Argos positions before track analysis. 
# It identifies and removes unreliable fixes based on criteria such as poor satellite quality, impossible movement speeds, 
# or unrealistic spatial jumps. Importantly, only automatically generated locations are filtered, 
# while user-defined positions and deployment anchors are preserved.
filterLocations(data           = list.files("./data interim/03_checked", full.names = TRUE),
                max.speed.kmh   = 10,     # reject fixes implying > 10 km/h to both neighbours
                min.satellites  = 4,      # drop Fastloc fixes computed from < 4 satellites
                # max.distance.km = 300,  # optional gross-error bound (off by default; see ?filterLocations)
                basemap         = "land",
                coastline       = "auto",
                plot            = FALSE,
                plot.file       = "./plots/location_filter.pdf",
                return.data     = FALSE,
                output.dir      = "./data interim/03_checked",   # overwrite in place
                verbose         = "detailed")



################################################################################
# STEP 7. Read the camera video (optional; camera tags only)                   #
################################################################################

# For camera-equipped tags, getVideoMetadata() extracts video start times, 
# durations and frame rates for alignment with the sensor data. Timestamps are 
# taken from file names where available, with OCR used when needed; cross-checking 
# can flag disagreements between the two sources. 

# This step is only needed for the video-based orientation check in STEP 8 and 
# can be skipped for deployments without cameras.

# Root directory containing the camera-tag video folders
camera_folders <- list.dirs("/Users/Mig/Desktop/Whale Sharks/CAMS", recursive = FALSE)
#camera_folders <- list.dirs("/Volumes/T7 Shield/CAMS", recursive = TRUE)
#camera_folders <- camera_folders[grepl("MP4", basename(camera_folders), fixed = TRUE)]

# Extract video metadata and resolve clip start times for alignment with the sensor data
video_metadata <- getVideoMetadata(video.folders    = camera_folders,
                                   video.format     = c("mp4", "mov"),
                                   timestamp.source = "auto",   # file name first, OCR only where needed
                                   cross.check      = TRUE,     # also OCR the overlay and flag disagreements
                                   use.parallel     = TRUE,
                                   verbose          = "detailed")
#video_metadata$ID <- sub(".*/([^/]+)/MP4/.*", "\\1", video_metadata$file)

# Save timestamp crops for clips with uncertain or flagged start times, for 
# manual verification
video_metadata <- saveUncertainTimestampFrames(video.metadata = video_metadata,
                                               output.dir     = "./outputs/timestamps review")


# Save video metadata for use in later steps
write.csv(video_metadata, file = "./outputs/video_metadata.csv", row.names = FALSE)


################################################################################
# STEP 8. Resolve the IMU axis orientation                                     #
################################################################################

# Sensor axes depend on how the tag was attached, so they must be mapped to the 
# animal's body frame before calculating orientation and movement metrics. 
# nautilus supports a staged workflow that combines documented configurations,
# data-based checks, consensus across deployments and optional video review 
# before applying the final mapping.

## 8.1 Documented axis configurations -------------------------------------------------------------- 
# Map each documented tag configuration to the corresponding body axes. These 
# configurations are based on the manufacturer's tag build information.
configs <- list(
  "CATS MS"           = data.frame(from = c("ax", "ay"),       to = c("-ay", "-ax")),
  "CATS Camera"       = data.frame(from = c("ax", "ay", "az"), to = c("-ax", "-ay", "-az")),
  "CATS 2019 Camera"  = data.frame(from = c("ax", "ay"),       to = c("-ax", "-ay")),
  "CATS 27 MS"        = data.frame(from = c("ax", "ay", "az"), to = c("az", "-ax", "-ay")),
  "CATS 27 Camera"    = data.frame(from = c("ax", "az"),       to = c("-az", "ax")),
  "CEIIA 2022 (71)"   = data.frame(from = c("ax", "ay", "az"), to = c("ay", "az", "-ax")),
  "CEIIA 2022 (134)"  = data.frame(from = c("ax", "az"),       to = c("-ax", "-az")),
  "CEIIA 2023 Camera" = data.frame(from = c("ax", "ay", "az"), to = c("ay", "az", "ax")),
  "4K Camera"         = data.frame(from = c("ax", "az"),       to = c("az", "-ax"))
)

## 8.2 Check the mapping against the data ----------------------------------------------------------
# checkTagMapping() evaluates documented mappings against the sensor data and 
# infers an alternative mapping where the data provide sufficient evidence. 
# Deployments that remain ambiguous or inconsistent are flagged for review.
mapping_qc <- checkTagMapping(data                     = list.files("./data interim/03_checked", full.names = TRUE),
                              configs                  = configs,
                              static.threshold         = 0.1,      # how still counts as "static" for the gravity read
                              vertical.speed.threshold = 0.5,      # dive speed (m/s) that counts as real diving
                              dba.window               = 5,
                              use.dynamics             = TRUE,     # use dive dynamics to resolve the fore-aft axis
                              locomotor.band           = c(0.2, 3),
                              locomotor.axis           = "sway",   # tail beats show up on the lateral axis (see STEP 12's notes)
                              plot                     = FALSE,
                              plot.file                = "./plots/axis_mapping.pdf",
                              verbose                  = "detailed")

# Summarise the mapping status across deployments.
vapply(mapping_qc, function(x) x$frame_state$prior$status, character(1))

# Cache the results so later steps can be re-run without repeating the QC.
saveRDS(mapping_qc, "./outputs/mapping_qc.rds")
# mapping_qc <- readRDS("./outputs/mapping_qc.rds")


## 8.3 Resolve remaining uncertainty by consensus --------------------------------------------------
# Deployments from the same hardware share the same sensor geometry.
# consensusAxisMapping() uses this to resolve genuine ambiguities while flagging
# conflicts between otherwise confident deployments.
mapping_consensus <- consensusAxisMapping(results       = mapping_qc,
                                          group.by      = c("package_id", "logger_id"),  # what counts as "same hardware"
                                          min.agreement = 0.75,   # how strongly a group must agree to lend its mapping
                                          min.voters    = 2,      # and how many confident members it needs
                                          verbose       = "detailed")


## 8.4 Confirm uncertain deployments on video (optional) ------------------------------------------ 
# For camera deployments, reviewTagMapping() generates short comparison clips 
# for flagged cases, allowing the candidate mappings to be checked against the 
# shark's observed movements. The review returns a decision sheet for manual 
# confirmation.
review <- reviewTagMapping(data             = list.files("./data interim/03_checked", full.names = TRUE),
                           mapping          = mapping_qc,          # the per-deployment evidence to triage on
                           base             = mapping_consensus,   # the mapping actually applied, unless you override it
                           video.metadata   = video_metadata,
                           configs          = configs,
                           include          = c("conflict", "coreg_fail", "ambiguous", "gyro_inconsistent"),
                           output.dir = "./outputs/mapping review")

# decision sheet for flagged deployments
review   

# For comparison clips, choose the candidate that best matches the observed 
# movement. Deployments without a suitable mapping can be excluded.
review$decision[review$id == "PIN_CAM_26"] <- "Proposed" # ... one line per flagged deployment ... 
# review$decision[review$id == "PIN_CAM_XX"] <- "Exclude"



## 8.5 Apply the mapping ---------------------------------------------------------------------------
# applyAxisMapping() applies the final raw-to-body axis transformation. When 
# using the reviewed results, resolved decisions take precedence over the 
# consensus mapping; unresolved deployments retain the consensus mapping. 
# Accelerometer and gyroscope frames are checked for consistency when requested.
applyAxisMapping(data             = list.files("./data interim/03_checked", full.names = TRUE),
                 mapping          = review,                  # or mapping_consensus if you skipped 8.4
                 check.handedness = TRUE,                     # verify the accel/gyro frames agree
                 exclusions.file  = "./data interim/exclusions.csv",  
                 return.data      = FALSE,
                 output.dir       = "./data interim/04_oriented",
                 verbose          = "detailed")



################################################################################
# STEP 9. Calibrate the magnetometer (optional; for heading)                   #
################################################################################

# This step is only needed for magnetometer-derived heading. calibrateMagnetometer() 
# estimates the calibration from the oriented sensor data and stores it in the 
# metadata without modifying the raw magnetometer channels.

# Because reliable ellipsoid calibration requires broad 3D sensor coverage, 
# near-horizontal deployments may provide insufficient information. Deployments 
# from the same physical tag can therefore be pooled to improve coverage. 
# The resulting heading confidence is recorded as "high", "medium" or "low"; 
# low-confidence calibrations are not used automatically by processTagData().
calibrateMagnetometer(data          = list.files("./data interim/04_oriented", full.names = TRUE),
                      control       = magCalibrationControl(method = "ellipsoid"),  # hard-iron-only 2D fallback for a thin band
                      group.by      = c("package_id", "logger_id"),  # what counts as "same hardware"
                      plot          = FALSE,
                      plot.file     = "./plots/magnetometer_calibration.pdf",
                      return.data   = FALSE,
                      output.dir = "./data interim/05_processed",   
                      verbose       = "detailed")

# Heading confidence can be checked later via processingSummary()$heading_conf.


################################################################################
# STEP 10. Process the tag data                                                #
################################################################################

# processTagData() is the main processing step in the workflow. It takes the
# cleaned and correctly oriented data and derives the kinematic and movement
# metrics used in subsequent analyses, including body attitude (roll, pitch and
# heading), static and dynamic acceleration, dynamic body acceleration
# (VeDBA/ODBA), surge, sway, heave, vertical velocity and, where available,
# paddle-wheel speed.

# This step must be applied to the oriented data because posture and
# acceleration-derived metrics depend on a correctly defined body frame.
# Downsampling the processed data (here to 20 Hz) can substantially reduce file
# size while retaining the temporal resolution required for subsequent analyses.

# Processing options are organised into small control objects, each governing a
# specific aspect of the workflow. The most relevant options are illustrated
# below.

processTagData(
  data                  = list.files("./data interim/04_oriented", full.names = TRUE),
  downsample.to         = 20,               # output rate (Hz); the full-rate signal is used first, then decimated
  orientation.algorithm = "tilt_compass",   # the attitude estimator ("tilt_compass" or "madgwick")
  # Fine-tune the attitude estimator: correct for the tag's mounting pitch/roll offset, and (for "madgwick") its filter gain.
  orientation = orientationControl(correct.pitch = TRUE, correct.roll = TRUE,  heading.denoise = "auto"),
  # Magnetometer calibration. With use.stored = TRUE a trusted fit from STEP 9 is applied; otherwise the
  # same engine estimates one inline (full ellipsoid, or hard-iron-only for a thin band) and applies it
  # only when the animal rotated through enough headings to trust it.
  calibration = calibrationControl(hard.iron = TRUE, soft.iron = TRUE, use.stored = TRUE),
  # Smoothing windows, in seconds. 'static' sets the gravity/movement split and can't be switched off;
  # the rest are optional post-smoothers (set any to NULL to disable it).
  smoothing = smoothingControl(static = 5, orientation = 1, dba = 2, depth = 10, vertical = 1),
  # Correct the slow (mostly thermal) drift in the pressure sensor's zero, anchored to moments the tag
  # is known to be at the surface (the wet/dry sensor and GPS fixes). Set method = "none" to skip it.
  depth.drift = depthDriftControl(method = "surface", surface.evidence = c("dry", "gps")),
  burst.quantiles    = c(0.95, 0.99),       # acceleration thresholds that mark high-effort "burst" events
  plot               = FALSE,
  plot.file          = "./plots/processed_data.pdf",
  exclusions.file    = "./data interim/exclusions.csv",  
  return.data        = FALSE,
  output.dir         = "./data interim/05_processed",
  output.suffix      = "-20Hz",
  verbose            = "detailed")


# processingSummary() is the companion view: one row per deployment describing what the *pipeline* did
# (orientation estimator, mounting-offset corrections, magnetometer heading confidence, depth-drift
# outcome, sampling rates). Handy as a final provenance check across the whole cohort.
processing_summary <- processingSummary(list.files("./data interim/05_processed", full.names = TRUE))



################################################################################
# STEP 11. Paddle-wheel swimming speed (optional; paddle tags)                 #
################################################################################

# Some tags carry a magnetic paddle wheel that spins as the animal swims. processTagData() recovers
# its rotation rate from the magnetometer and stores it as `paddle_freq`; turning that into a speed
# needs one number per tag, measured by calibrating it before deployment. 

# calculatePaddleSpeed() turns the rotation rate recorded in STEP 10 into a swimming speed, using one
# calibration slope per tag and season. Tags that were never calibrated get a slope estimated from the
# ones that were ("projected-shared"); the "in-situ-*" methods estimate it from the animal itself
# instead, from how fast it changed depth while swimming at a steep angle - either pooled across each
# tag and season ("in-situ-pooled") or separately for each deployment ("in-situ-deployment").
#
# With validate = TRUE every tag is also checked against that same in-situ estimate, whether or not
# it needed one. The agreement is their ratio: 1 means the two agree, and anything far from it is
# flagged as worth a look.
# Because only one column depends on the calibration, a revised slope can be applied in seconds - there
# is no need to process the raw sensor data again.

# Measured calibration slopes (one row per calibration): year, package_id, slope (+ fit quality).
calibration_regression <- read.csv("./paddle wheel calibration/Velocity_RotationHz_Regression.csv")
colnames(calibration_regression) <- c("year", "package_id", "slope", "r.squared", "adj.r.squared")


paddle <- calculatePaddleSpeed(data        = list.files("./data interim/05_processed", full.names = TRUE),
                               calibration = calibration_regression,
                               method      = "projected-shared",  
                               validate    = TRUE,            # check every tag against the animal's own diving
                               min.pitch   = 20,
                               plot.file   = "./plots/paddle_calibration.pdf",
                               return.data = FALSE,
                               output.dir  = "./data interim/05_processed",
                               verbose     = "detailed")
 

# One row per tag and season: the slope applied, where it came from, and how it compares in situ.
paddle_calibration <- attr(paddle, "calibration")
write.csv(paddle_calibration, "./outputs/paddle_calibration.csv", row.names = FALSE)


################################################################################
# STEP 12. Estimate tail-beat frequencies                                      #
################################################################################

# calculateTailBeats() estimates tail-beat frequency from a motion channel, 
# returning per-beat frequency, amplitude and a swimming/gliding classification. 

# For lateral swimmers such as sharks and teleosts, tail beats are typically 
# clearest on the lateral "sway" axis. Other taxa may require the vertical 
# "heave" axis. The sampling rate should exceed twice the maximum frequency 
# being estimated (Nyquist), with at least 4x recommended.

calculateTailBeats(data            = list.files("./data interim/05_processed", full.names = TRUE),
                   method          = c("peaks", "wavelet"),
                   motion.col      = c("surge"),
                   min.freq.Hz     = 0.1,
                   max.freq.Hz     = 2.5,
                   bandpass.filter = TRUE,
                   smooth.window   = 5,
                   plot            = TRUE,
                   plot.file       = "./plots/tail_beats.pdf",
                   return.data     = FALSE,
                   output.dir      =  "./data interim/06_tailbeats",
                   verbose         = "detailed")
                               
                  

################################################################################
# STEP 13. Summarize each deployment                                           #
################################################################################

# summarizeTagData() produces a one-row-per-deployment table of key metrics, 
# including deployment duration, depth and temperature, sampling rate, position, 
# video and movement statistics. The QC'd deployments object completes the deployment list,
# so excluded deployments are retained in the summary.

# Load video metadata if needed
#video_metadata <- read.csv(file = "./outputs/video_metadata.csv")

# Build the per-deployment summary, including standard metadata, video information
# and processing exclusions
summary <- summarizeTagData(data           = list.files("./data interim/06_tailbeats", full.names = TRUE),
                            deployments    = deployments,
                            metadata       = "standard",
                            video.metadata = video_metadata,
                            exclusions     = "./data interim/exclusions.csv",
                            tbf.method     = "wavelet", 
                            error.stat     = "sd",
                            verbose        = "detailed")

# Format the summary for export, selecting variables and adding a summary row.
summary_table <- format(summary, style = "concise", 
                        decimals = c(size_m = 1, video_duration_h = 1), 
                        order.by = "id",
                        group.by = "status",
                        group.order = c("included", "excluded"),
                        include.summary.row = TRUE)
selected_cols <- c("ID", "Animal",	"Sex",	"Size m", "Site", "Lon (deg)",	"Lat (deg)",
                  "Tag type", "Status", "Reason", "Rec. start", "Rec. end", "Duration (h)",	
                  "Rate (Hz)", "Paddle wheel", "Video (h)", "Mean depth (m)",	"Max depth (m)",
                  "Mean temp. (deg C)", "Min temp. (deg C)", "Max temp. (deg C)")
summary_table <- summary_table[, selected_cols]


# Standardise selected fields for the final export
summary_table$`Paddle wheel`[summary_table$`Paddle wheel`==TRUE] <- 1
summary_table$`Paddle wheel`[summary_table$`Paddle wheel`==FALSE] <- 0
summary_table$`Tag type`[summary_table$`Tag type`=="Camera"] <- "camera"
summary_table$`Tag type`[summary_table$`Tag type`=="MS"] <- "diary"
summary_table$Site[summary_table$Site=="SE_PICO"] <- "PICO"

# Export the final deployment summary as a CSV.
write.csv2(summary_table, file = "./outputs/summary_table_v6.csv", row.names = FALSE, fileEncoding = "UTF-8")


################################################################################
# STEP 14. Plot depth profiles                                                 #
################################################################################

# plotDepthProfiles() plots depth over time for each deployment, with temperature 
# shown by colour and day/night periods shaded using the deployment coordinates. 
# The plots are written to a multi-page PDF for visual inspection.

plotDepthProfiles(data             = list.files("./data interim/06_tailbeats", full.names = TRUE),
                  color.by         = "temp",   # colour the trace by any per-sample metric
                  shade.diel       = TRUE,     # shade night vs day (uses each deployment's coordinates)
                  same.depth.scale = FALSE,    # let each panel use its own depth range
                  downsample       = 5,        # thin to ~5 s for a lighter PDF
                  plot             = FALSE,
                  plot.file        = "./plots/depth-profiles.pdf",
                  ncols            = 2,
                  nrows            = 7)

################################################################################
# STEP 15. Compare metric distributions across the cohort                      #
################################################################################

# plotDistributions() visualises the distribution of selected metrics across 
# deployments, helping reveal among-individual variation and differences that 
# summary statistics alone may obscure. The underlying distribution summaries 
# are also returned invisibly for further analysis.

dist_summary <- plotDistributions(data      = list.files("./data interim/06_tailbeats", full.names = TRUE),
                                  metrics   = c("tbf_hz_wavelet", "paddle_speed"),
                                  order.by  = "id",  
                                  min.n     = 30,         # ignore deployments with too few samples for a metric
                                  plot      = FALSE,
                                  plot.file = "./plots/metric-distributions.pdf")


################################################################################
# STEP 16. Map how the cohort uses the water column                            #
################################################################################

# plotTimeAtDepth() summarises time spent across depth or temperature bins, 
# optionally separating day and night or comparing groups. The underlying 
# per-bin summaries are also returned invisibly.

tad_summary <- plotTimeAtDepth(data      = list.files("./data interim/06_tailbeats", full.names = TRUE),
                               variable  = c("depth", "temp"),   # depth-use and thermal-use side by side
                               diel      = TRUE,                 # mirror night vs day (needs coordinates)
                               style     = "profile",            # or "heatmap" for a cohort-by-depth grid
                               plot      = FALSE,
                               plot.file = "./plots/time-at-depth.pdf")

# Compare groups using a theme preset:
# plotTimeAtDepth(profile_files, 
#                 group = "sex",
#                 theme = plotTheme("minimal"),
#                 plot.file = "./plots/tad-by-sex.pdf")


################################################################################
# End of pipeline: raw tag files to quality-controlled, oriented and processed 
# datasets, with deployment- and cohort-level summaries and visualisations. 
# Use processingHistory(x) to inspect how an individual dataset was processed. 
################################################################################