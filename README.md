
# nautilus <img src="resources/nautilus-logo.png" align="right" width="100" />

[![Project Status: Work in Progress](https://img.shields.io/badge/status-WIP-orange)](https://www.repostatus.org/#wip)  
[![](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
[![CRAN status](https://www.r-pkg.org/badges/version/nautilus)](https://CRAN.R-project.org/package=nautilus)
[![R-CMD-check](https://github.com/miguelgandra/nautilus/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/miguelgandra/nautilus/actions/workflows/R-CMD-check.yaml)
[![Github All Releases](https://img.shields.io/github/downloads/miguelgandra/nautilus/total.svg)]()


**nautilus** is an R package designed for the analysis and visualisation of high-resolution archival tag data from marine animals.
The package also includes tools for integrating and analysing video data collected by onboard cameras.

**Note:** This package is still in its early stages of development and is being actively improved. Some features may change or expand as the package evolves.


## Features

- Process high-resolution time-series data from archival tags, including depth, temperature, 3-axis acceleration, 3-axis magnetometer, and 3-axis gyroscope data.
- Automatically identify and filter deployment periods (when the tag is attached to the animal) from pre- and post-deployment periods.
- Calculate a variety of behavioural and movement-related metrics (e.g., ODBA, surge, heading, pitch, etc.).
- Process video data associated with onboard cameras.
- Easily jump to specific datetimes in video files directly from R.
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
- Tutorial 02: Reencode .MOV video files to HEVC format and retrieve video metadata.
Both tutorials can be found in the tutorials/ directory of the package.


## Documentation

For detailed information on how to use nautilus, please refer to the package documentation.


## License
This project is licensed under the GPL v3 license.
