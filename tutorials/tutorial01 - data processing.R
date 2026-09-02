###############################################################################################
## Miguel Gandra || CCMAR || m3gandra@gmail.com ###############################################
## Tutorial 01: Processing Multi-Sensor Tag Data with the 'nautilus' R Package ################
###############################################################################################

# A step-by-step walkthrough of the core 'nautilus' pipeline, from raw archival-tag CSVs to
# analysis-ready, fully documented datasets. It follows the actual workflow of the PINTADO
# whale-shark project, so it doubles as a real-world example. The pipeline covers:
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
# Every stage returns 'nautilus_tag' objects: data.tables that also carry a consolidated metadata
# record - deployment info, sensors, calibration, and an append-only processing audit trail.
# Inspect any object with print() or summary(); read its metadata with tagMetadata(x) and its
# processing history with processingHistory(x).
#
# Notes:
#  - Default parameters are tuned to whale-shark kinematics and to the CATS / CEiiA tags used here.
#    For other species or tag systems, adjust the arguments accordingly.
#  - All file paths below are project-specific - edit them to match your own directory layout.
#  - The pipeline is disk-based: each stage reads the previous stage's files and writes its own
#    (return.data = FALSE, save.files = TRUE), so large datasets never all sit in memory at once.
#    Output folders are NOT created automatically - nautilus is fail-fast about paths, so create
#    "./data interim/...", "./plots" and "./outputs" before running.


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

# Before touching a single sensor sample, we assemble a clean table describing each deployment: where
# and when the animal was tagged, which tag it carried, and a few biological traits. This is the
# connective tissue of the whole analysis - it tells nautilus how to correct headings for local
# magnetic declination, which deployments share hardware, and which animal each record belongs to.
#
# Everything in this step is ordinary, project-specific data wrangling. The goal is simply a tidy,
# one-row-per-deployment data.frame; STEP 2 will quality-check it.

animal_metadata <- readxl::read_excel("./metadata/PINTADO_metadata_multisensor.xlsx")

# Keep the columns we need and give them consistent, readable names.
selected_cols <- c("id", "dateTime", "site", "longitudeD", "latitudeD",
                   "sex", "size", "Nmax", "type", "typeCMD", "PakageID", "ID_CMD",
                   "satPtt", "padWheel", "recoveryDate", "recoveryTime",
                   "lonRecov", "latRecov", "popupDatetime", "latPop", "lonPop",
                   "Observation")
animal_metadata <- as.data.frame(animal_metadata)[, selected_cols]
colnames(animal_metadata) <- c("ID", "deploy_date", "deploy_site", "deploy_lon", "deploy_lat",
                               "sex", "size", "n_animals", "type", "tag", "package_id", "cmd_id",
                               "satPtt", "paddle_wheel", "recover_date", "recover_time",
                               "recover_lon", "recover_lat", "popup_date", "popup_lat", "popup_lon", "obs")

# Deployment year - handy for telling apart hardware configurations that changed between field seasons.
animal_metadata$deploy_year <- as.integer(format(animal_metadata$deploy_date, "%Y"))

# The recovery date and time arrive in two separate columns; combine them into one POSIXct so the
# recovery-related checks in STEP 2 can run. One deployment's recovery time is unreliable, so we blank it.
has_recovery <- !is.na(animal_metadata$recover_date) & !is.na(animal_metadata$recover_time)
animal_metadata$recover_datetime <- as.POSIXct(NA, tz = "UTC")
animal_metadata$recover_datetime[has_recovery] <-
  as.POSIXct(paste(format(animal_metadata$recover_date[has_recovery], "%Y-%m-%d"),
                   format(animal_metadata$recover_time[has_recovery], "%H:%M:%S")),
             format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
animal_metadata$recover_datetime[animal_metadata$ID == "PIN_CAM_27"] <- NA

# Tidy a few tag / type labels so they group cleanly.
animal_metadata$tag[animal_metadata$tag == "4k"]       <- "4K"
animal_metadata$tag[animal_metadata$tag == "Ceiia"]    <- "CEIIA"
animal_metadata$type[animal_metadata$type == "Camara"] <- "Camera"

# Name the IMU orientation configuration for each deployment. Different tag models (and even the same
# model across seasons) mount their sensor chips at different angles, so each deployment gets the name
# of the configuration it was built with. STEP 8 turns these names into actual axis rotations. The base
# name is "<tag> <type>" (e.g. "CATS Camera"), with a handful of documented exceptions by package,
# logger, year or individual, taken from the tag manufacturer's configuration notes.
animal_metadata$axis_config <- paste(animal_metadata$tag, animal_metadata$type)
animal_metadata$axis_config[animal_metadata$cmd_id == "71" & animal_metadata$deploy_year == 2019] <- "CATS 2019 Camera"
animal_metadata$axis_config[animal_metadata$cmd_id == 27] <- paste("CATS 27", animal_metadata$type[animal_metadata$cmd_id == 27])
animal_metadata$axis_config[animal_metadata$tag == "CEIIA" & animal_metadata$deploy_year == 2022 & animal_metadata$package_id == 71]  <- "CEIIA 2022 (71)"
animal_metadata$axis_config[animal_metadata$tag == "CEIIA" & animal_metadata$deploy_year == 2022 & animal_metadata$package_id == 134] <- "CEIIA 2022 (134)"
animal_metadata$axis_config[animal_metadata$tag == "CEIIA" & animal_metadata$deploy_year == 2023] <- "CEIIA 2023 Camera"
animal_metadata$axis_config[animal_metadata$ID == "PIN_CAM_26"] <- "4K Camera"
# For two deployments the orientation was genuinely uncertain. Rather than hard-code a guess, we leave
# the configuration blank and let STEP 8 work it out from the data and from sibling deployments.
animal_metadata$axis_config[animal_metadata$ID %in% c("PIN_10", "PIN_12")] <- ""

# All of these tags were towed rather than rigidly bolted on. This matters later: a towed tag wobbles
# relative to the body, which changes how posture is scored.
animal_metadata$deployment_type <- "towed"

# Where on the animal was the tag attached? We parse it from the free-text field notes, then override
# with the video-verified assignments wherever we had footage to check against.
animal_metadata$attachment_site <- NA_character_
animal_metadata$attachment_site[grepl("dorsal",  animal_metadata$obs, ignore.case = TRUE)] <- "dorsal"
animal_metadata$attachment_site[grepl("esq",      animal_metadata$obs, ignore.case = TRUE)] <- "left_pectoral"
animal_metadata$attachment_site[grepl("direita",  animal_metadata$obs, ignore.case = TRUE)] <- "right_pectoral"
idx <- grepl("pectoral|peitoral", animal_metadata$obs, ignore.case = TRUE) & is.na(animal_metadata$attachment_site)
animal_metadata$attachment_site[idx] <- "pectoral"
animal_metadata$attachment_site[animal_metadata$ID %in% c("PIN_CAM_02", "PIN_CAM_05", "PIN_CAM_06",
                                                          "PIN_CAM_26", "PIN_CAM_41")] <- "dorsal"
animal_metadata$attachment_site[animal_metadata$ID %in% c("PIN_CAM_04", "PIN_CAM_22", "PIN_CAM_24",
                                                          "PIN_CAM_31", "PIN_CAM_32", "PIN_CAM_39")] <- "right_pectoral"

# done parsing the notes; drop them
animal_metadata$obs <- NULL   


################################################################################
# STEP 2. Quality-check the deployment metadata                                #
################################################################################

# qcDeploymentMetadata() validates and cleans the metadata before any sensor data is read - catching
# the small field-sheet slips that would otherwise quietly poison the analysis: a duplicate ID, an
# impossible tagging coordinate, a recovery date before the deployment, two deployments overlapping on
# the same physical tag. Fixing these here is far cheaper than discovering them after a long import.
#
# The trick is metadataColumns(): instead of a rename, it maps each of your columns to a nautilus
# "role". A role tells the package what a column represents, and that in turn switches on the checks
# and features that depend on it - deployment coordinates enable declination correction, a package_id
# enables per-package orientation consensus and paddle-wheel calibration, biological traits ride along
# into every object for later grouping. Roles you don't map are simply skipped.

deployments <- checkDeploymentMetadata(
  animal_metadata,
  columns = metadataColumns(
    # The five required roles (shown here even where they match the defaults, so the menu is visible):
    id              = "ID",
    tag_model       = "tag",
    deploy_datetime = "deploy_date",     # must already be POSIXct
    deploy_lon      = "deploy_lon",
    deploy_lat      = "deploy_lat",
    # Optional roles - each one you add turns on the checks/features that need it:
    tag_type          = "type",
    recovery_datetime = "recover_datetime",   # enables the recovery-before-deploy + duration checks
    popup_datetime    = "popup_date",         # pop-up location (needs all three popup_* together)
    popup_lon         = "popup_lon",
    popup_lat         = "popup_lat",
    package_id        = "package_id",         # groups deployments that share a physical tag
    logger_id         = "cmd_id",             # tracks a logger across board-swaps
    axis_config       = "axis_config",        # the orientation-config name used in STEP 8
    paddle_wheel      = "paddle_wheel",
    attachment_site   = "attachment_site",
    deployment_type   = "deployment_type",    # "towed" or "rigid"; selects the posture scorer
    deploy_site       = "deploy_site",        # the tagging locality by name: the coordinates say where
                                              # to a metre, the name is what you group and report by
    # animal_id belongs here when one animal can carry several tags - `id` identifies the deployment,
    # animal_id the animal, and summarizeTagData() reports both. This sheet has no separate animal
    # identifier (one deployment is one animal), so the role is left unmapped.
    # Passive biological traits: carried verbatim into each object's metadata (tagMetadata(x)$biometrics)
    # so they're available later for grouping, filtering and plotting (e.g. plotTimeAtDepth(group = "sex")).
    # A corrected value can be re-stamped later with updateBiometrics() - no re-import needed.
    traits            = c("sex", "size")),
  verbose          = "detailed")

# Read the verdict, fix anything flagged at the source, and re-run until it's clean.
issues(deployments)                      # all issues
issues(deployments, severity = "error")  # just the blocking ones


################################################################################
# STEP 3. Import the tag data                                                  #
################################################################################

# importTagData() reads each animal's multi-sensor CSV, standardizes the sensor names and units
# (acceleration to g, gyroscope to rad/s, magnetometer to uT, depth to metres, ...), folds in any
# Wildlife Computers location files, and attaches the metadata.
#
# It recognizes the standard CATS and CEiiA layouts out of the box, so import.mapping stays NULL.
# For a non-standard file, hand it a small data.frame mapping each raw column to a sensor and unit -
# valid sensors: datetime; ax/ay/az, gx/gy/gz, mx/my/mz; depth, temp; valid units: "UTC"; "g","m/s2";
# "rad/s","deg/s","mrad/s"; "uT"; "C"; "m". For example:
#   import.mapping = data.frame(
#     colname = c("UTC_Time","ACC_X","ACC_Y","ACC_Z","MAG_X","MAG_Y","MAG_Z","TEMP","PRESSURE"),
#     sensor  = c("datetime","ax","ay","az","mx","my","mz","temp","depth"),
#     units   = c("UTC","g","g","g","uT","uT","uT","C","m"))
#
# Passing the QC'd 'deployments' object as id.metadata does two things: it carries its own column
# schema (so no columns argument is needed here), and it carries the QC verdict - if the metadata
# failed STEP 2, the import refuses to start rather than wasting time on a long read. The data is
# imported in its raw axis frame; rotating it into the animal's frame is a deliberate, separate step
# (STEP 8).

# Root folder with one subdirectory per tagged animal (edit to your path).
data_root <- "/Users/Mig/Desktop/Whale Sharks/data"
data_folders <- list.dirs(data_root, recursive = FALSE)
data_folders <- data_folders[1:58]   # subset the deployments to process, if needed

importTagData(data.folders         = data_folders,
              sensor.subdirectory  = "CMD",
              wc.subdirectory      = NULL,      # NULL = auto-detect the Wildlife Computers folder
              metadata          = deployments,
              import.mapping       = NULL,      # NULL = standard CATS / CEiiA layout
              import.calibration   = TRUE,
              timezone             = "UTC",
              alignment            = alignmentControl(method = "depth-xcorr"),
              return.data          = FALSE,
              output.dir           = "./data interim/01_imported",
              compress             = TRUE,
              verbose              = "detailed")


################################################################################
# STEP 4. Trim to the deployment period                                        #
################################################################################

# filterDeploymentData() removes the pre-attachment and post-detachment data, keeping only the
# on-animal period. It works two ways:
#   - Automatic: binary segmentation of depth (and its variance) estimates the attachment and
#     detachment times - useful when the exact deployment window is unknown.
#   - Manual: pass a data.frame of known ID / start / end windows to `custom.deployment.times`;
#     these override the automatic detection (leave start or end as NA to auto-detect just that end).
# A diagnostic panel per deployment is written to a single multi-page PDF for review.

# Known deployment windows (manual overrides; NA = let the algorithm find that boundary).
deploy_list <- list(
  list(ID = "PIN_02",     start = as.POSIXct("2019-09-11 12:35:00", tz = "UTC"), end = as.POSIXct("2019-09-12 16:32:00", tz = "UTC")),
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


filterDeploymentData(data                    = list.files("./data interim/01_imported", full.names = TRUE),
                     custom.deployment.times = deploy_periods,   # known windows; NA boundaries auto-detected
                     depth.threshold         = 3.5,    # depth (m) that counts as "in the water" for detection
                     variance.threshold      = 6,      # depth-variance change that marks attachment/detachment
                     max.changepoints        = 6,
                     use.temperature         = FALSE,  # corroborate with temperature too, if it's reliable
                     min.deployment.hours    = 1,   # discard anything shorter than this
                     plot                    = FALSE,   # one diagnostic panel per deployment...
                     plot.file               = "./plots/filtered_deployments.pdf",  # ...into a single PDF to review
                     plot.metrics            = c("temp", "az"),  # extra traces to overlay on the panel
                     exclusions.file         = "./data interim/exclusions.csv",  # who left, and why
                     return.data             = FALSE,
                     output.dir              = "./data interim/02_filtered",
                     verbose                 = "detailed")


################################################################################
# STEP 5. Put the samples on a regular time grid                               #
################################################################################

# Tags rarely sample on a perfectly even clock: timestamps drift and jitter, and now and then a sample
# drops out entirely. Almost everything downstream - filtering, derivatives, frequency analysis -
# assumes evenly-spaced samples, so regularizeTimeSeries() snaps the record onto a uniform grid at its
# own median sampling interval. Short gaps are filled by interpolation; longer ones are left honest as
# NA rather than inventing behaviour across them. Coverage statistics (how much was interpolated, gap
# fraction, jitter) are stored in each object's metadata.

regularizeTimeSeries(data                 = list.files("./data interim/02_filtered", full.names = TRUE),
                     gap.threshold        = 2,        # fill gaps up to 2 s; leave longer ones as NA (0 = never fill)
                     interpolation.method = "linear", # or "spline" / "locf"
                     plot                 = FALSE,    
                     plot.file            = "./plots/regularization.pdf",
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

# the findings, one row per flagged channel
integrity$issues                                   


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

# the findings, one row per flagged channel
quality$issues  

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

# If your tags carry cameras, this reads each clip's start time, duration and frame rate so the footage
# can be lined up with the sensor stream. It is only needed for the video-based orientation check in
# STEP 8 (sub-step 8.4 below); skip it entirely for tags without cameras.
#
# getVideoMetadata() takes the start time from the video's file name wherever it can (exact, and
# independent of any on-screen clock), falling back to reading the burned-in timestamp with OCR only
# when the name carries no time. With cross.check = TRUE it OCRs the overlay as well and flags any clip
# whose file-name time and on-screen time disagree.


camera_folders <- list.dirs("/Users/Mig/Desktop/Whale Sharks/CAMS", recursive = FALSE)
#camera_folders <- list.dirs("/Volumes/T7 Shield/CAMS", recursive = TRUE)

video_metadata <- getVideoMetadata(video.folders    = camera_folders,
                                   video.format     = c("mp4", "mov"),
                                   timestamp.source = "auto",   # file name first, OCR only where needed
                                   cross.check      = TRUE,     # also OCR the overlay and flag disagreements
                                   use.parallel     = TRUE,
                                   verbose          = "detailed")
#video_metadata$ID <- sub(".*/[0-9]{4}/([^/]+)/.*", "\\1", video_metadata$file)


# For any clip whose start time is uncertain (OCR-sourced, missing, or flagged), save the timestamp
# crop to a folder so you can confirm it by eye.
video_metadata <- saveUncertainTimestampFrames(video.metadata = video_metadata,
                                               output.dir     = "./outputs/timestamps review")

# Fix by hand only the few clips that are genuinely wrong - typically a camera with a mis-set clock.
# Give the correct start; the end is recomputed from the duration.
overrides <- data.frame(
  video = c("CameraCMD71Spot17-20201006-172957-009-00005.mp4",
            "230831-161949_CAM0bc99448_30.mp4",
            "230831-171758_CAM0bc99448_30.mp4"),
  start = as.POSIXct(c("2020-09-06 17:29:57", "2023-08-31 16:19:49", "2023-08-31 17:17:59"), tz = "UTC"),
  stringsAsFactors = FALSE)
for (k in seq_len(nrow(overrides))) {
  i <- match(overrides$video[k], video_metadata$video)
  video_metadata$start[i] <- overrides$start[k]
  video_metadata$end[i]   <- overrides$start[k] + video_metadata$duration[i]
}

write.csv(video_metadata, file = "./outputs/video_metadata.csv", row.names = FALSE)
#video_metadata <- read.csv(file = "./outputs/video_metadata.csv")


################################################################################
# STEP 8. Resolve the IMU axis orientation                                     #
################################################################################

# A tag can be attached at any angle, but "pitch", "roll" and "heading" only mean something once we've
# rotated the sensor's raw axes into the animal's own frame - nose forward, belly down (a North-East-Down
# convention). Get this wrong and a left turn reads as a right one, and every posture metric is quietly
# corrupted. This is one of the most important steps in the pipeline, so nautilus gives it a small
# workflow: propose a mapping from the documented configuration, check it against the data, reconcile
# uncertain cases across sibling deployments, optionally confirm the tricky ones on video, and only then
# apply it.

## 8.1 Documented axis configurations --------------------------------------------------------------
# 'configs' maps each configuration name (the values placed in axis_config back in STEP 1) to its axis
# mapping: 'from' is a raw sensor axis, 'to' the destination body axis, optionally sign-flipped ("-ay").
# These come from the tag manufacturer's build notes. Deployments left blank in axis_config (like PIN_10)
# carry no documented mapping and are resolved from the data and consensus instead.
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
# checkTagMapping() lets the animal's own behaviour vote on the orientation. It reads the vertical axis
# from gravity during calm, low-motion moments; the fore-aft (surge) axis from how pitch tracks
# depth-rate during dives; the gyroscope from body rotation; and the magnetometer against the gravity
# frame and the expected geomagnetic field. Where a documented config agrees, it's confirmed; where one
# disagrees or is missing, the axes are inferred; where the data can't decide, the deployment is
# honestly flagged rather than forced.
mapping_qc <- checkTagMapping(data                     = list.files("./data interim/03_checked", full.names = TRUE),
                              configs                  = configs,
                              static.threshold         = 0.1,      # how still counts as "static" for the gravity read
                              vertical.speed.threshold = 0.5,      # dive speed (m/s) that counts as real diving
                              dba.window               = 5,
                              use.dynamics             = TRUE,     # use dive dynamics to resolve the fore-aft axis
                              locomotor.band           = c(0.2, 3),
                              locomotor.axis           = "sway",   # tail beats show up on the lateral axis (see STEP 13's notes)
                              plot                     = FALSE,
                              plot.file                = "./plots/axis_mapping.pdf",
                              verbose                  = "detailed")

# A quick look at where each deployment landed (confirmed / consistent / conflict / ...).
vapply(mapping_qc, function(x) x$frame_state$prior$status, character(1))

# This pass does real work, so cache it - the later steps can then be re-run without recomputing.
saveRDS(mapping_qc, "./outputs/mapping_qc.rds")
# mapping_qc <- readRDS("./outputs/mapping_qc.rds")


## 8.3 Rescue uncertain deployments by consensus ---------------------------------------------------
# Deployments that share the same physical tag share a fixed sensor geometry. consensusAxisMapping()
# uses that: within each hardware group it forms a confidence-weighted consensus and lends it to the
# weaker members, so a dive-rich deployment can rescue a short or flat-swimming sibling. It only ever
# fills genuine ambiguity - it never overrides a deployment that already resolved on its own, and if two
# confident deployments in a group disagree, it flags the conflict instead of papering over it.
mapping_consensus <- consensusAxisMapping(results       = mapping_qc,
                                          group.by      = c("package_id", "logger_id"),  # what counts as "same hardware"
                                          min.agreement = 0.75,   # how strongly a group must agree to lend its mapping
                                          min.voters    = 2,      # and how many confident members it needs
                                          verbose       = "detailed")


## 8.4 Confirm the tricky ones on video (optional) -------------------------------------------------
# For camera deployments, footage is the gold standard for handedness. reviewTagMapping() picks the
# deployments most worth a human look (a QC conflict, an ambiguous inference, or disagreeing sensors),
# finds their clearest rolls and dives, and renders short clips showing a sensor "attitude indicator"
# next to the real footage. Nothing is modified here - candidate mappings are applied only to temporary
# copies. It hands back a decision sheet: one row per flagged deployment, for you to fill in.
review <- reviewTagMapping(data             = list.files("./data interim/03_checked", full.names = TRUE),
                           mapping          = mapping_qc,          # the per-deployment evidence to triage on
                           base             = mapping_consensus,   # the mapping actually applied, unless you override it
                           video.metadata   = video_metadata,
                           configs          = configs,
                           include          = c("conflict", "coreg_fail", "ambiguous", "gyro_inconsistent"),
                           output.dir = "./outputs/mapping review")
review   # the decision sheet: flagged deployments, their candidate 'options', and a blank 'decision'


# How to read a clip: for a conflict the dashboard shows two labelled attitude indicators side by side -
# "Documented" (the recorded config) and "Proposed" (the frame the data prefers). Find a moment where
# the shark clearly banks to one side; the correct mapping is the indicator that leans the same way.
#
# Fill the 'decision' column with the winning option for each flagged deployment. Only comparison clips
# need a decision; single-candidate flags fall through to the base mapping.
review$decision[review$id == "PIN_CAM_26"] <- "Proposed"
# ... one line per flagged deployment (see 'review' for the full list) ...
# If a deployment's orientation is genuinely untrustworthy and no candidate is right, mark it "Exclude"
# and STEP 8.5 will drop it from the output:
# review$decision[review$id == "PIN_CAM_XX"] <- "Exclude"


## 8.5 Apply the mapping ---------------------------------------------------------------------------
# applyAxisMapping() rotates the raw axes into the body frame. The transform is absolute (raw -> body)
# and idempotent, so it's safe to re-run. Passing the reviewed sheet as 'mapping' does the sensible
# thing: un-reviewed deployments take the consensus base, decided ones take your chosen candidate - and
# the function refuses to proceed if a deployment that rendered a real comparison is still undecided, so
# handedness is never applied on a guess. Skipped the video review? Pass mapping = mapping_consensus.
#
# The gyroscope comes along automatically: for an accelerometer-only config its mapping is derived from
# the accelerometer's, and check.handedness verifies that the two agree. A left-handed convention is
# harmless; a genuine accel/gyro mismatch is warned about and recorded in tagMetadata(x)$axis_mapping$coreg_corr.
applyAxisMapping(data             = list.files("./data interim/03_checked", full.names = TRUE),
                 mapping          = review,                  # or mapping_consensus if you skipped 8.4
                 check.handedness = TRUE,                     # verify the accel/gyro frames agree
                 return.data      = FALSE,
                 output.dir       = "./data interim/04_oriented",
                 verbose          = "detailed")



################################################################################
# STEP 9. Calibrate the magnetometer (optional; for heading)                   #
################################################################################

# You only need this step if you care about magnetometer-derived heading. Skipping it is fine -
# processTagData() applies a solid per-axis magnetometer calibration on its own.
#
# The catch is physical: a clean magnetometer calibration wants the sensor swept through every
# orientation, tracing a full sphere. A whale shark cruising near-horizontal only ever traces a thin
# band of that sphere, so the fit is genuinely under-determined. calibrateMagnetometer() does the best
# it honestly can and, crucially, hands back a heading-confidence flag ("high"/"medium"/"low") so you
# know whether to trust the result. Pooling several deployments of one physical tag (group.by =
# "package_id") widens the coverage and can rescue a fit that would fail alone.
#
# It runs on the oriented files and stores its estimate in the metadata without altering mx/my/mz.
# processTagData() then picks up that stored calibration automatically - but only when its confidence
# is good enough; a low-confidence fit is kept for inspection and quietly ignored.

calibrateMagnetometer(data          = list.files("./data interim/04_oriented", full.names = TRUE),
                      control       = magCalibrationControl(method = "ellipsoid"),  # hard-iron-only 2D fallback for a thin band
                      group.by      = c("package_id", "logger_id"),  # what counts as "same hardware"
                      plot          = FALSE,
                      plot.file     = "./plots/magnetometer_calibration.pdf",
                      return.data   = FALSE,
                      output.dir = "./data interim/05_processed",   
                      verbose       = "detailed")
# You can check the per-deployment heading confidence later via processingSummary()$heading_conf.



################################################################################
# STEP 10. Read the paddle-wheel calibrations (optional; paddle tags)          #
################################################################################

# Some tags carry a magnetic paddle wheel that spins as the animal swims. processTagData() recovers
# its rotation rate from the magnetometer and stores it as `paddle_freq`; turning that into a speed
# needs one number per tag, measured by calibrating it before deployment. That step comes after
# processing (STEP 12), so a calibration can be revised or checked without reprocessing anything.

# Measured calibration slopes (one row per calibration): year, package_id, slope (+ fit quality).
calibration_regression <- read.csv("./paddle wheel calibration/Velocity_RotationHz_Regression.csv")
colnames(calibration_regression) <- c("year", "package_id", "slope", "r.squared", "adj.r.squared")


################################################################################
# STEP 11. Process the tag data                                                #
################################################################################

# processTagData() is the core derivation step: it takes the oriented, cleaned data and 
# computes the full set of kinematic and motion metrics. It estimates body attitude (roll, pitch, heading),
# splits acceleration into the static (gravity/posture) and dynamic (movement) parts, and computes the
# full metric suite - dynamic body acceleration (VeDBA/ODBA, a proxy for movement intensity widely used
# to estimate activity and, with species-specific calibration, energy expenditure), surge/sway/heave,
# vertical velocity, and the paddle-wheel rotation rate where available. It must run on the
# oriented files, since every posture metric depends on a correct body frame. Downsampling the output
# (here to 20 Hz) keeps the files manageable for downstream analysis without losing the behaviour.
 
# The processing knobs are grouped into small control objects, one per concern, so the call stays
# readable. Each is shown here with the options worth knowing about.

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
  return.data        = FALSE,
  output.dir         = "./data interim/05_processed",
  output.suffix      = "-20Hz",
  verbose            = "detailed")


# processingSummary() is the companion view: one row per deployment describing what the *pipeline* did
# (orientation estimator, mounting-offset corrections, magnetometer heading confidence, depth-drift
# outcome, sampling rates). Handy as a final provenance check across the whole cohort.
processing_summary <- processingSummary(list.files("./data interim/05_processed", full.names = TRUE))


################################################################################
# STEP 12. Paddle-wheel swimming speed (optional; paddle tags)                 #
################################################################################

# calculatePaddleSpeed() turns the rotation rate recorded in STEP 11 into a swimming speed, using one
# calibration slope per tag and season. Tags that were never calibrated get a slope estimated from the
# ones that were ("projected-shared"); the "in-situ-*" methods estimate it from the animal itself
# instead, from how fast it changed depth while swimming at a steep angle - either pooled across each
# tag-season ("in-situ-pooled") or separately for each deployment ("in-situ-deployment").
#
# validate = TRUE additionally checks every tag against that same in-situ estimate, whether or not it
# needed one. The agreement is their ratio: 1 means the two agree, and anything more than
# agreement.threshold away from it is flagged as worth a look. Because only one column depends on the
# calibration, a revised slope can be applied in seconds - there is no need to process the raw sensor
# data again.

paddle <- calculatePaddleSpeed(
  data        = list.files("./data interim/05_processed", full.names = TRUE),
  calibration = calibration_regression,
  method      = "projected-shared",  # fill missing slopes from the calibrations that do exist
  validate    = TRUE,            # off by default; check every tag against the animal's own diving
  plot.file   = "./plots/paddle_calibration.pdf",
  return.data = FALSE,
  output.dir  = "./data interim/05_processed",
  verbose     = "detailed")

# One row per tag and season: the slope applied, where it came from, and how it compares in situ.
paddle_calibration <- attr(paddle, "calibration")
write.csv(paddle_calibration, "./outputs/paddle_calibration.csv", row.names = FALSE)


################################################################################
# STEP 13. Estimate tail-beat frequencies                                      #
################################################################################

# calculateTailBeats() estimates the tail-beat frequency from a motion channel. Each backend names its
# own output, so provenance travels with the value: "peaks" returns tbf_hz_peaks and
# tbf_amplitude_peaks, "wavelet" returns tbf_hz_wavelet and tbf_amplitude_wavelet. Both amplitudes are
# the peak-to-trough excursion, so they are directly comparable. The swimming/gliding flag
# (tbf_swimming) is shared by the backends and so carries no suffix. Use tailBeatColumn() if you want
# code that works whichever backend was run. The call below runs both, which is what lets STEP 13 and
# the distribution plot below ask for the wavelet columns by name.
#
# Choosing the axis: for lateral swimmers (most sharks and teleosts) tail beats are cleanest on the
# lateral 'sway' axis; other taxa (e.g. cetaceans, rays) may need the vertical 'heave' axis. Using an
# axis perpendicular to the propulsive stroke can report double the true frequency. Sampling must
# exceed twice `max.freq.Hz` (Nyquist); >= 4x is recommended.
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
# STEP 14. Summarize each deployment                                           #
################################################################################

# summarizeTagData() builds a one-row-per-deployment table of headline metrics (duration, depth and
# temperature ranges, sampling rate, positions, tail-beat and speed statistics, ...). Passing the
# QC'd `deployments` object completes the roster (deployments with no processed data appear as
# excluded rows), and `extra.metadata` joins any extra per-ID covariates.
#
# `metadata = "standard"` (the default) also brings in the biometric traits recorded at import (sex,
# size, ...) and the tagging date, site and coordinates - the columns a deployment table is usually
# expected to carry. `animal_id` and `deploy_site` are roles (STEP 2), so they appear automatically
# wherever they were mapped. Use "all" for the pop-up position and the package/logger identifiers, "none" for the bare
# metric table, or name the fields and traits you want. These are filled from the roster for
# deployments whose data never arrived, so a tag that was never recovered still reports who was tagged,
# when and where instead of an empty row.
#
# `video.metadata` adds total footage per deployment (getVideoMetadata() returns one row per file; the
# totalling is done for you). `exclusions` is the shared exclusion log: every stage that can drop a
# deployment writes to it, so the summary can say why each one is missing whichever stage set it aside.
# It is a plain CSV you can open without R - see the `exclusions.file` written back in STEP 4.

summary <- summarizeTagData(data           = list.files("./data interim/06_tailbeats", full.names = TRUE),
                            deployments    = deployments,
                            metadata       = "standard",
                            video.metadata = video_metadata,
                            exclusions     = "./data interim/exclusions.csv",
                            tbf.method     = "wavelet", 
                            error.stat     = "sd",
                            verbose        = "detailed")


# For export, format() renders the publication-style version. It is ASCII by default - a spreadsheet
# opening a UTF-8 CSV with no byte-order mark guesses the encoding, and a degree sign then arrives as
# mojibake. Pass symbols = "unicode" where the consumer handles UTF-8 (knitr, flextable, a manuscript).
summary_table <- format(summary, style = "concise", include.summary.row = TRUE)
write.csv2(summary_table, file = "./outputs/summary_table.csv", row.names = FALSE, fileEncoding = "UTF-8")


################################################################################
# STEP 15. Plot depth profiles                                                 #
################################################################################

# A depth-versus-time profile is the most immediate portrait of a deployment: dive shape, vertical
# range, and the temperatures the animal moved through. plotDepthProfiles() draws one panel per
# deployment, coloured by temperature and shaded by day/night (read from each deployment's coordinates),
# and manages the multi-page PDF itself - so you just hand it the file paths. 

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
# STEP 16. Compare metric distributions across the cohort                      #
################################################################################

# Where STEP 13 gives one number per animal, plotDistributions() shows the whole shape of a metric: a
# stack of per-deployment violins over a pooled population strip, one panel per metric. It's the quick
# way to spot among-individual variation and multimodal behaviour that a mean would hide - and it
# returns the per-deployment distribution summary invisibly, for tables and stats.
dist_summary <- plotDistributions(data      = list.files("./data interim/06_tailbeats", full.names = TRUE),
                                  metrics   = c("tbf_hz_wavelet", "paddle_speed"),
                                  order.by  = "id",  
                                  min.n     = 30,         # ignore deployments with too few samples for a metric
                                  plot      = FALSE,
                                  plot.file = "./plots/metric-distributions.pdf")


################################################################################
# STEP 17. Map how the cohort uses the water column                            #
################################################################################

# Finally, a population view of habitat use: plotTimeAtDepth() shows how much time the animals spent at
# each depth (and temperature), as duration-weighted profiles with fine bins near the surface. Ask for
# both variables together to see time-at-depth beside time-at-temperature, mirror night against day, or
# facet by a biological trait to compare groups. It returns the underlying per-bin table invisibly.
tad_summary <- plotTimeAtDepth(data      = list.files("./data interim/06_tailbeats", full.names = TRUE),
                               variable  = c("depth", "temp"),   # depth-use and thermal-use side by side
                               diel      = TRUE,                 # mirror night vs day (needs coordinates)
                               style     = "profile",            # or "heatmap" for a cohort-by-depth grid
                               plot      = FALSE,
                               plot.file = "./plots/time-at-depth.pdf")
# Compare groups, restyled with a theme preset:
# plotTimeAtDepth(profile_files, group = "sex", theme = plotTheme("minimal"),
#                 plot.file = "./plots/tad-by-sex.pdf")


###############################################################################################
# And that's the run: from a folder of raw tag files to oriented, calibrated, analysis-ready
# datasets, plus cohort-level summaries and figures. Every object still carries its own story -
# processingHistory(x) will show you exactly how it was made.
###############################################################################################