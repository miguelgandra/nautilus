
# nautilus <img src="resources/nautilus-logo.png" align="right" width="100" />

[![Project Status: Work in Progress](https://img.shields.io/badge/status-WIP-orange)](https://www.repostatus.org/#wip)  
[![](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
[![CRAN status](https://www.r-pkg.org/badges/version/nautilus)](https://CRAN.R-project.org/package=nautilus)
[![R-CMD-check](https://github.com/miguelgandra/nautilus/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/miguelgandra/nautilus/actions/workflows/R-CMD-check.yaml)
[![Github All Releases](https://img.shields.io/github/downloads/miguelgandra/nautilus/total.svg)]()


**nautilus** is an R package designed for the analysis and visualisation of high-resolution archival tag data from marine animals.
Although it was specifically designed to interoperate with G-Pilot and i-Pilot tags (Fontes et al., 2022), the package offers broader applicability and can potentially be adapted to other tagging systems.
The package also includes tools for integrating and analysing video data collected by onboard cameras.

**Note:** This package is still in its early stages of development and is being actively improved. Some features may change or expand as the package evolves.

**Reference:**
Fontes, J., Macena, B., Solleliet-Ferreira, S., Buyle, F., Magalhães, R., Bartolomeu, T., Liebsch, N., Meyer, C. & Afonso, P. (2022). The advantages and challenges of non-invasive towed PILOT tags for free-ranging deep-diving megafauna. *Animal Biotelemetry, 10*(1), 39. https://doi.org/10.1186/s40317-022-00310-1
<br/><br/>

## Features

- Process high-resolution time-series data from archival tags, including depth, temperature, 3-axis acceleration, 3-axis magnetometer, and 3-axis gyroscope data.
- Automatically identify and filter deployment periods (when the tag is attached to the animal) from pre- and post-deployment periods.
- Identify and handle outliers in sensor time-series data, improving data quality and reliability.
- Calculate a variety of behavioural and kinematic metrics (e.g., ODBA, surge, heading, pitch, etc.).
- Estimate tail beat frequencies using continuous wavelet transforms (CWT).
- Automatically generate publication-ready summary tables with key statistics from the processed datasets.
- Plot animals' depth profiles as colour-coded time-series, facilitating visual inspection of diving behaviour.
- Process video data associated with onboard cameras.
- Easily jump to specific datetimes in video files directly from R.
- Annotate datasets with events or behaviours of interest, linking time-bound events (e.g., feeding) from video review to the corresponding sensor data.
- Extract features from a sliding window, calculating metrics (e.g., mean, standard deviation) for selected variables, preparing datasets for machine learning or other analytical methods.
- Additional features are being developed to further enhance functionality.


## Installation

You can install the development version of **nautilus** from GitHub with:

```r
# Install devtools if you haven't already
# install.packages("devtools")
devtools::install_github("miguelgandra/nautilus")
```

## Tutorial Scripts

Two tutorial scripts are available to help you get started:
- Tutorial 01: Import, process, and analyze archival tag data.
- Tutorial 02: Reencode .MOV video files to HEVC format for improved compression efficiency and retrieve video metadata.

Both tutorials can be found in the tutorials/ directory of the package.


## Documentation

For detailed information on how to use nautilus, please refer to the package documentation.


## License
This project is licensed under the GPL v3 license.
