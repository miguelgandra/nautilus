% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/filterVideoPeriod.R
\name{filterVideoPeriod}
\alias{filterVideoPeriod}
\title{Filter data based on video availability periods}
\usage{
filterVideoPeriod(
  data,
  video.metadata,
  id.col = "ID",
  datetime.col = "datetime"
)
}
\arguments{
\item{data}{A \code{list} where each element is a \code{data.table} for a specific ID
(e.g., a specific camera or animal). Each dataset should include a column for datetimes.}

\item{video.metadata}{A \code{data.table} containing metadata for each video, with columns:
\code{ID} (unique identifier), \code{start} (start datetime), and \code{end} (end datetime).}

\item{id.col}{A \code{character} string indicating the column name in \code{video.metadata}
that contains the unique IDs matching the \code{data} list names. Default is \code{"ID"}.}

\item{datetime.col}{A \code{character} string indicating the name of the datetime column
in each dataset. Default is \code{"datetime"}.}
}
\value{
A list of filtered \code{data.table} objects, where each dataset contains only the rows
that fall within the video availability periods specified in \code{video.metadata}.
If no matching periods are found for a given ID, an empty \code{data.table} is returned.
}
\description{
This function subsets a list of data.tables to match the periods where video footage is available,
based on metadata containing video start and end times.
}
