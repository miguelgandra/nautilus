% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/plotDepthProfiles.R
\name{plotDepthProfiles}
\alias{plotDepthProfiles}
\title{Plot Depth Profiles with Colored Time Series}
\usage{
plotDepthProfiles(
  data,
  id.metadata,
  id.col = "ID",
  datetime.col = "datetime",
  depth.col = "depth",
  color.by = "temp",
  color.by.label = "Temp (ºC)",
  lon.col = "lon",
  lat.col = "lat",
  color.pal = NULL,
  cex.id = 1.2,
  cex.pt = 0.4,
  cex.axis = 0.9,
  cex.legend = 0.8,
  same.color.scale = TRUE,
  same.depth.scale = FALSE,
  ncols = NULL,
  nrows = NULL
)
}
\arguments{
\item{data}{A data frame or a list of data frames containing depth profile information.}

\item{id.metadata}{Data frame. Metadata about the IDs to associate with the processed data.
Must contain at least columns for ID, release longitude and release latitude.}

\item{id.col}{Character. The name of the column identifying individuals (default: "ID").}

\item{datetime.col}{Character. The name of the column containing datetime information (default: "datetime").}

\item{depth.col}{Character. The name of the column containing depth values (default: "depth").}

\item{color.by}{Character. The name of the column used for color mapping (default: "temp").}

\item{color.by.label}{Character. Label for the color legend (default: "Temp (ºC)").}

\item{lon.col}{Character. The name of the column containing longitude information (default: "lon").}

\item{lat.col}{Character. The name of the column containing latitude information (default: "lat").}

\item{color.pal}{Character vector. A color palette for the depth profiles (default: \code{pals::jet(100)}).}

\item{cex.id}{Numeric. The expansion factor for the ID labels in plots. Defaults to 1.2.}

\item{cex.pt}{Numeric. The expansion factor for the points. Defaults to 0.4.}

\item{cex.axis}{Numeric. The expansion factor for axis labels. Defaults to 0.9.}

\item{cex.legend}{Numeric. The expansion factor for the legend text. Defaults to 0.8.}

\item{same.color.scale}{Logical. If \code{TRUE}, all plots use the same color scale for the mapped variable (default: \code{TRUE}).}

\item{same.depth.scale}{Logical. If \code{TRUE}, all plots use the same depth scale; if \code{FALSE}, each individual has its own scale (default: \code{FALSE}).}

\item{ncols}{Integer. Number of columns for the plot layout. If \code{NULL}, it is determined automatically.}

\item{nrows}{Integer. Number of rows for the plot layout. If \code{NULL}, it is determined automatically.}
}
\value{
A plot displaying depth profiles for each individual, with time on the x-axis and depth on the y-axis, colored by the specified variable.
}
\description{
This function generates depth profiles for individual subjects, with depth values plotted against time.
The points are color-coded based on a specified variable.
}
