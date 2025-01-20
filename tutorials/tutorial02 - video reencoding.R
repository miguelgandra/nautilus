###############################################################################################
## Miguel Gandra || CCMAR || m3gandra@gmail.com || December 2024 ##############################
## Sample Script for Reencoding MOV Videos to HEVC Format #####################################
###############################################################################################

# This script allows users to:
# 1. Reencode segmented .MOV video files to HEVC (H.265) format.
# 2. Retrieve and display metadata for the video files.
#
# Requirements:
# - FFmpeg (installed outside of R) must be available on your system.
# - The 'nautilus' package, which can be installed from GitHub.
#
# Please ensure FFmpeg is installed correctly before running the script.


################################################################################
# Install and load required packages ###########################################
################################################################################

# Uncomment the following line if 'devtools' is not installed
# install.packages("devtools")

# Install and load the 'nautilus' package from GitHub
#devtools::install_github("miguelgandra/nautilus")
library(nautilus)


################################################################################
# Process videos  ##############################################################
################################################################################

# Specify the folder path containing the .MOV video files that you want to reencode
# Replace this path with the location of your own video files
video_directory <- "~/Desktop/Whale Sharks/CAMS/PIN_CAM_10/"

# Reencode all MOV videos in the specified directory to HEVC (H.265) format
# The reencoded videos will be saved as .mp4 files in the same directory by default
# You can specify an alternative output directory if desired
reencodeVideos(mov.directory=video_directory, encoder = "hevc_videotoolbox", video.quality = 35)


# Retrieve metadata for all processed videos in the specified directory
# This function returns details like duration, start time, end time, and frame rate.
video_info <- getVideoMetadata(video_directory)

# View the retrieved video metadata
# This will display information about each video
print(video_info)


###############################################################################################
###############################################################################################
###############################################################################################
