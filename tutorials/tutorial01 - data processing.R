###############################################################################################
## Miguel Gandra || CCMAR || m3gandra@gmail.com ###############################################
## Tutorial 01: Processing Multi-Sensor Tag Data with the 'nautilus' R Package ################
###############################################################################################

# Biologging tags are wonderful and messy. A whale shark carries one for a day or two, then the
# tag pops off and is (hopefully) recovered, holding millions of rows of depth, acceleration,
# magnetometer, gyroscope and often video - all in the raw form the hardware happened to record it.
# This tutorial walks that raw pile through the full 'nautilus' pipeline and out the other side as
# tidy, analysis-ready datasets you can actually do biology with: reconstructed body posture, dynamic
# body acceleration, tail-beat frequency, dive profiles, and per-animal summaries.
#
# It follows the real workflow of the PINTADO whale-shark project, so besides being a walkthrough it
# doubles as a worked example, with genuine deployment quirks (mis-set camera clocks, ambiguous tag
# orientations, drifting pressure sensors) and how to handle them.
#
# The pipeline, in nine acts:
#
#   1. Prepare and quality-check the per-deployment metadata (who, where, which tag).
#   2. Import and standardize the raw multi-sensor data (CATS / CEiiA tags).
#   3. Trim each record down to the time the tag was actually on an animal.
#   4. Put the samples on a clean, evenly-spaced time grid.
#   5. Screen the sensor channels for faults and glitches.
#   6. Work out how each tag was oriented and rotate its axes into the animal's own frame.
#   7. Derive posture, kinematics and movement metrics (with speed and depth calibration).
#   8. Estimate tail-beat frequency.
#   9. Summarize and visualize the cohort.
#
# A few things to know before you start:
#  - Every stage returns 'nautilus_tag' objects: data.tables that also carry a consolidated metadata
#    record (deployment details, sensors, calibration, and an append-only processing log). Peek at any
#    object with print() or summary(); read its metadata with tagMetadata(x) and its full history with
#    processingHistory(x).
#  - The pipeline is disk-based: each stage reads the previous stage's files and writes its own
#    (pass return.data = FALSE and an output.dir - a directory is what triggers the save), so even large
#    datasets never all sit in memory at once. A stage run with return.data = FALSE returns the written
#    file paths, which feed straight into the next stage's data argument.
#  - nautilus is deliberately fail-fast about file paths: it aborts rather than silently creating
#    folders, so make the output directories once, up front (STEP 0).
#  - Defaults are tuned to whale-shark kinematics and to the CATS / CEiiA tags used here. For other
#    species or tag systems, adjust the arguments - most steps below show the ones worth knowing about.
#  - All file paths are placeholders; edit them to match your own layout.


################################################################################
# Expected directory structure                                                 #
################################################################################

# Organize the raw data as a root folder with one subfolder per tagged animal. Each animal's folder
# holds a multi-sensor tag folder (the time-series CSV) and, optionally, a Wildlife Computers folder
# with location files:
#
#   Root_Directory/
#   |-- PIN_01/
#   |   |-- CMD/   xxxxx-Multisensor22Splash52.csv     # depth, accel, gyro, mag, ...
#   |   \-- SPOT/  xxxxx-Locations.csv, ...            # (optional) Argos / GPS positions
#   \-- PIN_02/
#       |-- CMD/   xxxxx-CamaraCMD134Spot98.csv
#       \-- MK10/  xxxxx-Locations.csv, ...
#
# Each animal-folder name must match the deployment "ID" in the metadata.


################################################################################
# STEP 0. Install and load                                                     #
################################################################################

# 'readxl' is used only to read the metadata spreadsheet in STEP 1.
if (!require(readxl)) { install.packages("readxl"); library(readxl) }

# Install 'nautilus' from GitHub once, then load it.
# devtools::install_github("miguelgandra/nautilus")
library(nautilus)

# Create the folders the pipeline writes to. dir.create() is a safe no-op if they already exist.
output_dirs <- c(
  "./plots", "./outputs", "./outputs/timestamps review", "./outputs/axis review",
  file.path("./data interim", c("imported", "filtered", "regularized", "checked",
                                "oriented", "processed", "tailbeats")))
for (d in output_dirs) dir.create(d, recursive = TRUE, showWarnings = FALSE)


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
animal_metadata$obs <- NULL   # done parsing the notes; drop them


################################################################################
# STEP 2. Quality-check the deployment metadata                                #
################################################################################

# checkDeploymentMetadata() validates and cleans the metadata before any sensor data is read - catching
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
    # Passive biological traits: carried verbatim into each object's metadata (tagMetadata(x)$biometrics)
    # so they're available later for grouping, filtering and plotting (e.g. plotTimeAtDepth(group.by = "sex")).
    # A corrected value can be re-stamped later with updateBiometrics() - no re-import needed.
    traits            = c("sex", "size")),
  future.tolerance = 1,                  # allow a day of clock slop before flagging a "future" date
  verbose          = "detailed")

# Read the verdict, fix anything flagged at the source, and re-run until it's clean.
issues(deployments)                    # all issues
issues(deployments, severity = "error")# just the blocking ones


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
# Passing the QC'd 'deployments' object as metadata does two things: it carries its own column
# schema (so no columns argument is needed here), and it carries the QC verdict - if the metadata
# failed STEP 2, the import refuses to start rather than wasting time on a long read. The data is
# imported in its raw axis frame; rotating it into the animal's frame is a deliberate, separate step
# (STEP 8).

# Root folder with one subfolder per animal (edit to your path).
data_root    <- "path/to/tag data"
data_folders <- list.dirs(data_root, recursive = FALSE)
data_folders <- data_folders[1:58]       # subset the deployments to process, if you like

data_list <- importTagData(data.folders       = data_folders,
                           sensor.subdirectory = "CMD",        # the per-animal folder holding the tag CSV
                           wc.subdirectory     = NULL,         # NULL = auto-detect the Wildlife Computers folder
                           metadata         = deployments,
                           import.mapping      = NULL,         # NULL = standard CATS / CEiiA layout
                           import.calibration  = TRUE,         # also parse any sidecar calibration file, for provenance
                           timezone            = "UTC",        # labels the timestamps; never shifts them
                           return.data         = FALSE,
                           output.dir       = "./data interim/imported",
                           compress            = TRUE,
                           verbose             = "detailed")


################################################################################
# STEP 4. Trim to the on-animal period                                         #
################################################################################

# A tag records everything - the boat ride out, the handling, and often days of bobbing at the surface
# after the animal has shrugged it off. For the biology we only want the stretch when it was actually
# riding a shark. Off-animal data isn't just clutter: it would bias the gravity and dive-based
# reasoning that STEP 8 relies on, so it has to go first.
#
# filterDeploymentData() finds the on-animal window automatically, from changepoints in the depth mean
# and variance (a tag on a diving animal looks very different from one sitting on deck). Where you
# already know the exact times - say, from the field log or the video - you can supply them and skip
# the guesswork. Leave a start or end as NA to pin one boundary and auto-detect the other.

# Known deployment windows (NA = let the algorithm find that boundary).
deploy_list <- list(
  list(ID = "PIN_02",     start = as.POSIXct("2019-09-11 12:35:00", tz = "UTC"), end = as.POSIXct("2019-09-12 16:32:00", tz = "UTC")),
  list(ID = "PIN_09",     start = as.POSIXct("2020-08-22 15:20:00", tz = "UTC"), end = as.POSIXct("2020-08-23 00:49:03", tz = "UTC")),
  list(ID = "PIN_10",     start = as.POSIXct("2020-08-23 16:20:00", tz = "UTC"), end = NA),
  list(ID = "PIN_16",     start = as.POSIXct("2022-09-18 17:48:00", tz = "UTC"), end = as.POSIXct("2022-09-19 08:34:00", tz = "UTC")),
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
  list(ID = "PIN_CAM_32", start = as.POSIXct("2022-09-17 13:34:40", tz = "UTC"), end = as.POSIXct("2022-09-18 10:31:52", tz = "UTC"))
)
deploy_periods <- do.call(rbind, lapply(deploy_list, as.data.frame))

filter_results <- filterDeploymentData(data                    = list.files("./data interim/imported", full.names = TRUE),
                                       custom.deployment.times = deploy_periods,   # known windows; NA boundaries auto-detected
                                       depth.threshold         = 3.5,    # depth (m) that counts as "in the water" for detection
                                       variance.threshold      = 6,      # depth-variance change that marks attachment/detachment
                                       max.changepoints        = 6,
                                       use.temperature         = FALSE,  # corroborate with temperature too, if it's reliable
                                       min.deployment.hours    = 0.25,   # discard anything shorter than this
                                       plot                    = TRUE,   # one diagnostic panel per deployment...
                                       plot.file               = "./plots/filtered_deployments.pdf",  # ...into a single PDF to review
                                       plot.metrics            = c("temp", "az"),  # extra traces to overlay on the panel
                                       return.data             = FALSE,
                                       output.dir           = "./data interim/filtered",
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

data_list <- regularizeTimeSeries(data                 = list.files("./data interim/filtered", full.names = TRUE),
                                  gap.threshold        = 2,        # fill gaps up to 2 s; leave longer ones as NA (0 = never fill)
                                  interpolation.method = "linear", # or "spline" / "locf"
                                  plot                 = TRUE,     # flags deployments with notable gaps/jitter for review
                                  plot.file            = "./plots/regularization.pdf",
                                  return.data          = FALSE,
                                  output.dir        = "./data interim/regularized",
                                  verbose              = "detailed")


################################################################################
# STEP 6. Screen the channels                                                  #
################################################################################

# Quality-control, run in order. First we ask whether each sensor channel is a trustworthy instance of
# the sensor it claims to be (6.1); then we clean up transient glitches on the channels that survive
# (6.2); finally we screen the position fixes - the location channel - for implausible detections (6.3).
# All three are data-cleaning steps, done here before any downstream analysis consumes the data.

data_files <- list.files("./data interim/regularized", full.names = TRUE)

## 6.1 Structural integrity ------------------------------------------------------------------------
# checkSensorIntegrity() looks for hardware- and firmware-level faults - a channel that is a near-exact
# copy of the accelerometer (a known firmware bug), a dead channel that never changes, a clipped or
# implausible signal. Run it first: a corrupt channel should be flagged before STEP 6.2 cosmetically
# smooths over its symptoms. Two checks are error-level and mean "don't trust this channel" -
# "duplication" and "dead"; the rest are advisory. Here we run the full set for a thorough look, then
# re-run with apply = TRUE to actually drop the error-flagged channels.
integrity <- checkSensorIntegrity(data   = data_files,
                                  checks = c("duplication", "dead", "saturation", "mag.plausibility",
                                             "accel.scale", "gyro.bias", "paddle.contamination", "dropout"),
                                  apply  = FALSE,   # report first; set TRUE to drop the error-flagged channels
                                  plot   = TRUE,
                                  plot.file = "./plots/sensor_integrity.pdf")
integrity$issues                                    # the findings, one row per flagged channel

## 6.2 Transient signal quality --------------------------------------------------------------------
# checkSensorQuality() repairs the passing channels: isolated spikes and stuck/flat-line stretches. You
# describe each channel's expectations with anomalyControl() - how fast it can plausibly change, its
# resolution, how long a stall must last to count - and the function screens several channels in one
# call. Like STEP 6.1 it is report-first: run once with apply = FALSE to review the issues, then again
# with apply = TRUE to write the fixes (spikes interpolated, malfunction blocks removed).
data_list <- checkSensorQuality(data    = data_files,
                                sensors = list(
                                  depth = anomalyControl(rate.threshold = 7,   # max plausible change per second (m/s)
                                                         sensor.resolution = 0.5),
                                  temp  = anomalyControl(rate.threshold = 1,    # deg C per second
                                                         sensor.resolution = 0.05)),
                                apply         = TRUE,
                                interpolate   = TRUE,     # patch isolated spikes (vs. setting them to NA)
                                return.data   = FALSE,
                                output.dir = "./data interim/checked",
                                verbose       = "detailed")

## 6.3 Position fixes ------------------------------------------------------------------------------
# filterLocations() screens the tag's GPS/Argos fixes - the location channel - exactly as the two steps
# above screen the sensor channels. It is the location analogue of checkSensorQuality(), so it belongs
# here in the clean phase, before any track reconstruction or mapping consumes the fixes. It works off
# the canonical position record stored at import (meta$ancillary$positions), so the sensor time series
# is untouched. Three independent, opt-in checks: a satellite-count floor for weak Fastloc fixes; a
# neighbour-consistency speed test (a fix is a spike only when it implies an impossible speed to BOTH
# its neighbours - so a single genuinely fast segment is kept); and an optional gross-distance bound.
# Only the automatically-acquired FastGPS/Argos fixes are ever removed - User positions and the
# deploy/pop-up anchors are trusted and kept. Thresholds are species-specific: 10 km/h is a conservative
# cruising cap for a whale shark. Reads and re-saves the checked files in place.
data_files <- list.files("./data interim/checked", full.names = TRUE)
filterLocations(data           = data_files,
                max.speed.kmh   = 10,     # reject fixes implying > 10 km/h to both neighbours
                min.satellites  = 4,      # drop Fastloc fixes computed from < 4 satellites
                # max.distance.km = 300,  # optional gross-error bound (off by default; see ?filterLocations)
                plot            = TRUE,
                plot.file       = "./plots/location_filter.pdf",
                return.data     = FALSE,
                output.dir   = "./data interim/checked",   # overwrite in place
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

camera_folders <- list.dirs("path/to/camera videos", recursive = FALSE)

video_metadata <- getVideoMetadata(video.folders    = camera_folders,
                                   video.format     = c("mp4", "mov"),
                                   timestamp.source = "auto",   # file name first, OCR only where needed
                                   cross.check      = TRUE,     # also OCR the overlay and flag disagreements
                                   use.parallel     = TRUE,
                                   verbose          = "detailed")

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


################################################################################
# STEP 8. Work out the tag orientation                                         #
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
data_files <- list.files("./data interim/checked", full.names = TRUE)
mapping_qc <- checkTagMapping(data                     = data_files,
                              configs                  = configs,
                              deployment.type          = "towed",  # matches the posture scorer to the attachment
                              static.threshold         = 0.1,      # how still counts as "static" for the gravity read
                              vertical.speed.threshold = 0.5,      # dive speed (m/s) that counts as real diving
                              use.dynamics             = TRUE,     # use dive dynamics to resolve the fore-aft axis
                              locomotor.axis           = "sway",   # tail beats show up on the lateral axis (see STEP 12's notes)
                              plot                     = TRUE,
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
mapping_consensus <- consensusAxisMapping(mapping_qc,
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
review <- reviewTagMapping(data             = data_files,
                           mapping          = mapping_qc,          # the per-deployment evidence to triage on
                           base             = mapping_consensus,   # the mapping actually applied, unless you override it
                           video.metadata   = video_metadata,
                           configs          = configs,
                           include          = c("conflict", "coreg_fail", "ambiguous", "gyro_inconsistent"),
                           output.dir = "./outputs/axis review")
review   # the decision sheet: flagged deployments, their candidate 'options', and a blank 'decision'

# How to read a clip: for a conflict the dashboard shows two labelled attitude indicators side by side -
# "Documented" (the recorded config) and "Proposed" (the frame the data prefers). Find a moment where
# the shark clearly banks to one side; the correct mapping is the indicator that leans the same way.
#
# Fill the 'decision' column with the winning option for each flagged deployment. Only comparison clips
# need a decision; single-candidate flags fall through to the base mapping.
review$decision[review$id == "PIN_CAM_04"] <- "Documented"
review$decision[review$id == "PIN_CAM_08"] <- "Proposed"
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
data_list <- applyAxisMapping(data             = data_files,
                              mapping          = review,           # or mapping_consensus if you skipped 8.4
                              check.handedness = TRUE,             # verify the accel/gyro frames agree
                              return.data      = FALSE,
                              output.dir    = "./data interim/oriented",
                              verbose          = "detailed")


################################################################################
# STEP 9. Calibrate the magnetometer (optional; for heading)                   #
################################################################################

# You only need this step if you care about magnetometer-derived heading. Skipping it is fine -
# processTagData() estimates and applies the same best-effort calibration inline anyway (a full hard +
# soft-iron ellipsoid when the sensor is well swept, otherwise a hard-iron-only correction). Running this
# step first just lets you review the fit, and unlocks per-package pooling and external calibration sources.
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

calibrateMagnetometer(data          = list.files("./data interim/oriented", full.names = TRUE),
                      control       = magCalibrationControl(method = "ellipsoid"),  # full ellipsoid; hard-iron-only 2D fallback for a thin band
                      group.by      = "package_id",     # pool deployments of one tag (paddle/non-paddle stay separate)
                      return.data   = FALSE,
                      output.dir = "./data interim/oriented",   # writes back over the oriented files
                      verbose       = "detailed")
# You can check the per-deployment heading confidence later via processingSummary()$heading_conf.


################################################################################
# STEP 10. Prepare paddle-wheel speed calibration (optional; paddle tags)      #
################################################################################

# Some tags carry a small magnetic paddle wheel that spins as the animal swims; processTagData() turns
# its spin rate into swimming speed (speed = slope x frequency), where the slope is specific to each
# tag. Skip this step for tags without a paddle wheel.
#
# The snag is that a slope is measured for only some tag-years, yet processTagData() needs one for every
# deployed paddle tag. imputePaddleCalibration() fills the gaps: it learns how the slope drifts as a
# paddle wears (efficiency drops, so the slope creeps up with age) and projects a complete, gap-free
# table - measured values kept as-is, the rest imputed and labelled as such.

# Measured calibration slopes (one row per calibration): year, package_id, slope, and fit quality.
calibration_regression <- read.csv("./paddle wheel calibration/Velocity_RotationHz_Regression.csv")
colnames(calibration_regression) <- c("year", "package_id", "slope", "r.squared", "adj.r.squared")

# The deployments that need a slope (rename deploy_year -> year to match the calibration table).
paddle_deployments <- animal_metadata[, c("package_id", "deploy_year", "paddle_wheel")]
colnames(paddle_deployments)[colnames(paddle_deployments) == "deploy_year"] <- "year"

paddle_calibration <- imputePaddleCalibration(calibration       = calibration_regression,
                                              deployments       = paddle_deployments,
                                              method            = "shared-rate",   # one pooled wear rate, per-tag levels
                                              weights.col       = "r.squared",     # trust better-fit calibrations more
                                              slope.range       = c(0.02, 0.30),   # clamp imputed slopes to plausible values
                                              max.extrapolation = 3)               # flag slopes projected far beyond the data


################################################################################
# STEP 11. Derive the movement metrics                                         #
################################################################################

# This is the heart of the pipeline. processTagData() takes the oriented, cleaned data and reconstructs
# what the animal was doing: it calibrates the sensors, estimates body attitude (roll, pitch, heading),
# splits acceleration into the static (gravity/posture) and dynamic (movement) parts, and computes the
# full metric suite - dynamic body acceleration (VeDBA/ODBA, a proxy for movement intensity widely used
# to estimate activity and, with species-specific calibration, energy expenditure), surge/sway/heave,
# vertical velocity, and paddle-wheel speed where available. It must run on the
# oriented files, since every posture metric depends on a correct body frame. Downsampling the output
# (here to 20 Hz) keeps the files manageable for downstream analysis without losing the behaviour.
#
# The processing knobs are grouped into small control objects, one per concern, so the call stays
# readable. Each is shown here with the options worth knowing about.

data_list <- processTagData(
  data                  = list.files("./data interim/oriented", full.names = TRUE),
  downsample.to         = 20,               # output rate (Hz); the full-rate signal is used first, then decimated
  orientation.algorithm = "tilt_compass",   # the attitude estimator ("tilt_compass" or "madgwick")
  # Fine-tune the attitude estimator: correct for the tag's mounting pitch/roll offset, and (for
  # "madgwick") its filter gain.
  orientation = orientationControl(correct.pitch = TRUE, correct.roll = TRUE, madgwick.beta = 0.02),
  # Magnetometer calibration. With use.stored = TRUE a trusted fit from STEP 9 is applied; otherwise the
  # same engine estimates one inline (full ellipsoid, or hard-iron-only for a thin band) and applies it
  # only when the animal rotated through enough headings to trust it.
  calibration = calibrationControl(hard.iron = TRUE, soft.iron = TRUE, use.stored = TRUE),
  # Smoothing windows, in seconds. 'static' sets the gravity/movement split (a zero-phase Butterworth
  # high-pass) and can't be switched off; the rest are optional post-smoothers (set any to NULL to disable).
  smoothing = smoothingControl(static = 5, orientation = 1, dba = 2, depth = 10, speed = 1),
  # Correct the slow (mostly thermal) drift in the pressure sensor's zero, anchored to moments the tag
  # is known to be at the surface (the wet/dry sensor and GPS fixes). Set method = "none" to skip it.
  depth.drift = depthDriftControl(method = "surface", surface.evidence = c("dry", "gps")),
  paddle.calibration = paddle_calibration,  # from STEP 10; omit for non-paddle tags
  burst.quantiles    = c(0.95, 0.99),       # acceleration thresholds that mark high-effort "burst" events
  return.data        = FALSE,
  output.dir      = "./data interim/processed",
  output.suffix      = "-20Hz",
  verbose            = "detailed")


################################################################################
# STEP 12. Estimate tail-beat frequency                                        #
################################################################################

# How hard and how often an animal beats its tail is a direct window onto swimming effort and gait.
# calculateTailBeats() reads the rhythm straight from the acceleration signal, returning a per-beat
# frequency (tbf_hz) and a beat amplitude (tbf_amplitude, an effort proxy). It does NOT decide swimming
# vs gliding by default: on a towed tag that call cannot be made reliably from sway alone (flow and
# tether motion oscillate in the same band), so tbf_swimming is NA unless you pass a min.amplitude
# reference to threshold against.
#
# Two things matter for the axis. Which axis actually carries the beat depends on the species, the gait
# and where the tag sits, and it is not always the one the body plan suggests (in our own manta records
# the wingbeat sat in surge, not sway), so if you are unsure pass several candidates and let the built-in
# cross-axis consensus pick - it reports the axis it chose and flags a possible 2f harmonic. And watch
# the sampling rate: you need at least twice max.freq.Hz to resolve the beat (four times is comfortable),
# or the frequency can fold over and read double.

data_list <- calculateTailBeats(data            = list.files("./data interim/processed", full.names = TRUE),
                                method          = "peaks",   # per-beat peaks; "wavelet" gives a time-frequency ridge
                                motion.col      = "sway",    # lateral axis for a laterally-swimming shark
                                min.freq.Hz     = 0.1,       # ignore rhythms slower than this...
                                max.freq.Hz     = 2.5,       # ...and faster than this (needs >= ~10 Hz sampling)
                                bandpass.filter = TRUE,      # isolate the tail-beat band before detecting peaks
                                smooth.window   = 5,
                                plot            = TRUE,
                                plot.file       = "./plots/tail_beats.pdf",
                                return.data     = FALSE,
                                output.dir   = "./data interim/tailbeats",
                                verbose         = "detailed")


################################################################################
# STEP 13. Summarize each deployment                                           #
################################################################################

# summarizeTagData() distils each deployment into one row of headline numbers: how long the tag was on,
# the depth and temperature ranges the animal used, sampling rate, positions, and - now that STEP 12 has
# run - tail-beat and speed statistics. Passing the QC'd 'deployments' object completes the roster, so
# deployments that dropped out along the way still appear (as excluded rows) rather than silently
# vanishing; extra.metadata joins on any additional per-animal covariates you want alongside.

summary <- summarizeTagData(data           = list.files("./data interim/tailbeats", full.names = TRUE),
                            deployments    = deployments,                       # completes the roster
                            extra.metadata = animal_metadata[, c("ID", "sex", "size")],
                            error.stat     = "sd",       # spread statistic for the population summary row
                            verbose        = "detailed")

summary   # a typed table; its print method shows a formatted view with a population mean +/- sd row

# For export, format() renders the publication-style version (write.csv2 + UTF-8 keeps the degree and
# per-second unit symbols intact in the headers).
summary_table <- format(summary, style = "report", include.summary.row = TRUE)
write.csv2(summary_table, file = "./outputs/summary_table.csv", row.names = FALSE, fileEncoding = "UTF-8")

# processingSummary() is the companion view: one row per deployment describing what the *pipeline* did
# (orientation estimator, mounting-offset corrections, magnetometer heading confidence, depth-drift
# outcome, sampling rates). Handy as a final provenance check across the whole cohort.
processingSummary(list.files("./data interim/tailbeats", full.names = TRUE))


################################################################################
# STEP 14. Plot the dive profiles                                              #
################################################################################

# A depth-versus-time profile is the most immediate portrait of a deployment: dive shape, vertical
# range, and the temperatures the animal moved through. plotDepthProfiles() draws one panel per
# deployment, coloured by temperature and shaded by day/night (read from each deployment's coordinates),
# and manages the multi-page PDF itself - so you just hand it the file paths. We drop the handful of
# deployments too short to make a useful profile first.

exclude_ids   <- c("PIN_CAM_02", "PIN_CAM_03", "PIN_CAM_05", "PIN_CAM_06",
                   "PIN_CAM_07", "PIN_CAM_10", "PIN_CAM_14", "PIN_CAM_15")
profile_files <- list.files("./data interim/tailbeats", full.names = TRUE)
profile_files <- profile_files[!sub("-20Hz$", "", tools::file_path_sans_ext(basename(profile_files))) %in% exclude_ids]

plotDepthProfiles(profile_files,
                  color.by        = "temp",   # colour the trace by any per-sample metric
                  shade.diel       = TRUE,     # shade night vs day (uses each deployment's coordinates)
                  same.depth.scale = FALSE,    # let each panel use its own depth range
                  downsample       = 5,        # thin to ~5 s for a lighter PDF
                  plot             = FALSE,
                  plot.file        = "./plots/depth-profiles.pdf",
                  ncols            = 2,
                  nrows            = 9)


################################################################################
# STEP 15. Compare metric distributions across the cohort                      #
################################################################################

# Where STEP 13 gives one number per animal, plotDistributions() shows the whole shape of a metric: a
# stack of per-deployment violins over a pooled population strip, one panel per metric. It's the quick
# way to spot among-individual variation and multimodal behaviour that a mean would hide - and it
# returns the per-deployment distribution summary invisibly, for tables and stats.
dist_summary <- plotDistributions(profile_files,
                                  metrics   = c("tbf_hz", "paddle_speed"),
                                  order.by  = "median",   # rank the animals by their median value
                                  min.n     = 30,         # ignore deployments with too few samples for a metric
                                  plot      = FALSE,
                                  plot.file = "./plots/metric-distributions.pdf")


################################################################################
# STEP 16. Map how the cohort uses the water column                            #
################################################################################

# Finally, a population view of habitat use: plotTimeAtDepth() shows how much time the animals spent at
# each depth (and temperature), as duration-weighted profiles with fine bins near the surface. Ask for
# both variables together to see time-at-depth beside time-at-temperature, mirror night against day, or
# facet by a biological trait to compare groups. It returns the underlying per-bin table invisibly.
tad_summary <- plotTimeAtDepth(profile_files,
                               variable  = c("depth", "temp"),   # depth-use and thermal-use side by side
                               diel      = TRUE,                 # mirror night vs day (needs coordinates)
                               style     = "profile",            # or "heatmap" for a cohort-by-depth grid
                               plot      = FALSE,
                               plot.file = "./plots/time-at-depth.pdf")
# Compare groups, restyled with a theme preset:
# plotTimeAtDepth(profile_files, group.by = "sex", theme = plotTheme("minimal"),
#                 plot.file = "./plots/tad-by-sex.pdf")


###############################################################################################
# And that's the run: from a folder of raw tag files to oriented, calibrated, analysis-ready
# datasets, plus cohort-level summaries and figures. Every object still carries its own story -
# processingHistory(x) will show you exactly how it was made.
###############################################################################################
