###############################################################################################
## Miguel Gandra || CCMAR || m3gandra@gmail.com || July 2025 ##################################
## Script for Re-encoding Camera Videos to HEVC Format #########################################
###############################################################################################

# This script allows users to re-encode segmented .MOV video files captured by biologging
# camera tags into space-efficient HEVC (H.265) .mp4 format using FFmpeg.
#
# ---> Re-encodes high-resolution .MOV videos to HEVC format for archival and/or processing.
# ---> Supports both software (libx265) and hardware-accelerated (hevc_videotoolbox, etc.) encoding.
# ---> Users can control encoding quality, output file suffixes, overwrite policy, and destination folders.
# ---> FFmpeg must be installed and available on the system path.
# ---> Uses 'nautilus::reencodeVideos()' to automate batch processing of video folders.


################################################################################
# Install and load required packages ###########################################
################################################################################

# Uncomment and run the line below if 'devtools' is not already installed
# install.packages("devtools")

# Install and load the 'nautilus' package from GitHub if needed
# devtools::install_github("miguelgandra/nautilus")
library(nautilus)


################################################################################
# Re-encode video files to HEVC (H.265) format #################################
################################################################################

# Define the input directory containing .MOV video segments to be re-encoded
# Replace the path below with the folder containing your raw camera videos
video_directory <- "/Volumes/T7 Shield/CAMS/2022/PIN_CAM_31/MOV"

# Optional: Define an alternative output directory for saving the .mp4 files
# If omitted, the re-encoded files will be saved to the same folder as the originals
output_directory <- NULL  # e.g., "~/Desktop/reencoded_videos"

# Run the re-encoding process
# Key arguments:
# - encoder: Selects the encoding engine (e.g., 'libx265', 'hevc_videotoolbox')
# - crf: Used for software encoding (libx265); typical values are 18–28
# - video.quality: Used for hardware encoding; values 1–100
# - preset: Controls encoding speed (e.g., 'ultrafast', 'medium', 'veryslow')
# - overwrite: Defines behavior for existing output files ('ask', TRUE, FALSE)

reencodeVideos(mov.directory = video_directory,
               encoder = "hevc_videotoolbox",
               video.quality = 35)


###############################################################################################
###############################################################################################
###############################################################################################
