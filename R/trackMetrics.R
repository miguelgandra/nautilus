#######################################################################################################
# Movement-path metrics (tortuosity + supporting track statistics) ####################################
#######################################################################################################

#' Summarise horizontal movement paths and trajectory geometry
#'
#' @description
#' Calculates deployment-level summaries of horizontal movement paths, including total path length,
#' net displacement, local turning, straightness and temporal tortuosity. Each eligible deployment is
#' reduced to one row, using reconstructed positions from [reconstructTrack()] or a supplied series of
#' geographic locations.
#'
#' The metrics describe complementary aspects of path geometry. Global path-to-displacement ratios
#' summarise the complete record, whereas local turning and windowed ratios describe finer-scale
#' variation. These are geometric descriptors rather than independent classifications of behaviour.
#'
#' @param data A tag dataset, a list of track datasets, a data frame containing multiple deployments
#'   identified by `id.col`, or a character vector of `.rds` file paths. Each track must contain
#'   longitude, latitude and timestamp columns. The output of [reconstructTrack()] can be supplied
#'   directly. File inputs are read sequentially; a named list can identify tables lacking `id.col`.
#' @param control A control object from [trackMetricsControl()], or a named list of its arguments,
#'   selecting the optional geometric metrics, minimum position count and temporal window lengths.
#'   Default `trackMetricsControl()`, which selects all optional metrics and requires at least five
#'   positions. The temporal tortuosity columns are computed regardless of the optional metric selection.
#' @param id.col Name of the deployment-identifier column (default `"ID"`). Used to split a single
#'   table and label output rows. For list or file inputs without a usable identifier, the list name
#'   or file basename is used. The output identifier column is always named `ID`.
#' @param lon.col,lat.col Names of the longitude and latitude columns, in decimal degrees. With both
#'   `NULL` (default), `pseudo_lon` and `pseudo_lat` are selected if both columns exist; otherwise
#'   `lon` and `lat` are used. Explicit names override the selection for each axis. Set both when
#'   summarising a different coordinate pair, such as observed rather than reconstructed positions.
#' @param datetime.col Name of the timestamp column (default `"datetime"`), expected to contain
#'   non-missing `POSIXct` values. Positions are ordered by this column before calculating metrics.
#' @param verbose How much detail to print: `0`/`"quiet"`, `1`/`"normal"` (per-deployment outcomes and
#'   summary), or `2`/`"detailed"` (default), which adds position counts, distance, duration and
#'   straightness diagnostics.
#'
#' @details
#' ## Workflow and input assumptions
#'
#' Use [reconstructTrack()] to estimate a dead-reckoned path and [plotTracks()] to inspect it before
#' summarising its geometry. Coordinate columns are selected by their presence, not by their coverage:
#' an entirely missing reconstructed pair does not trigger a fallback to observed coordinates when
#' both reconstructed columns exist. To summarise ancillary fixes, supply their position table
#' explicitly; the function does not read fixes from tag metadata or add deployment/pop-up anchors.
#'
#' Longitude and latitude should be finite geographic coordinates in decimal degrees, with consistent
#' timestamps. Rows with `NA` longitude or latitude are removed, and the remaining rows are sorted by
#' time. This is not a geographic or timestamp quality-control procedure: coordinates are not range
#' checked, duplicate timestamps are not resolved, and missing timestamps should be addressed beforehand.
#'
#' A deployment with missing required columns is warned about and omitted without stopping the other
#' tracks. A track with fewer than `control$min.points` positions after coordinate-NA removal is also
#' omitted. Empty input collections are rejected. Input datasets and metadata are not modified; the
#' function neither saves the summary nor writes to a deployment-exclusion log.
#'
#' ## Distances and local turning
#'
#' Step lengths and endpoint displacement are calculated using the haversine great-circle formula
#' with a spherical Earth radius of 6371 km. They describe horizontal movement, not three-dimensional
#' distance through the water column. Turning angles are signed differences between successive initial
#' step bearings, wrapped to -180 to 180 degrees; the reported turning metric uses their absolute values.
#'
#' `Path_ratio` is path length divided by endpoint displacement, whereas `Straightness` is the inverse
#' ratio where both are defined. Neither identifies where within the record a change in movement occurred.
#' `Sinuosity` uses the implemented Bovet and Benhamou (1988) expression
#' `1.18 * sd(turning angles in radians) / sqrt(mean positive step length in km)` and therefore has
#' units of inverse square-root kilometres. It requires at least one finite positive step and two
#' finite turning angles. Zero-length steps are omitted from the mean step length but their bearings
#' are not removed before calculating turning angles; repeated positions warrant particular care.
#'
#' ## Temporal tortuosity
#'
#' `Hourly_tortuosity` and `Daily_tortuosity` are unweighted means of path-to-displacement ratios in
#' successive, non-overlapping windows of `control$hourly.window.h` and `control$daily.window.h` hours
#' (defaults `1` and `24`). These are not sliding windows. Windows begin at the first retained timestamp
#' and advance by their full duration; a final incomplete window is omitted. Both window boundaries are
#' inclusive, so a position exactly on a shared boundary can contribute to both adjacent windows.
#'
#' Each window needs at least three retained positions and positive endpoint displacement. Unsupported
#' windows are omitted from the mean. A track shorter than a window returns `NA` for that column;
#' if no window provides a defined ratio, the mean may be `NaN`.
#'
#' ## Interpretation and limitations
#'
#' Missing coordinate rows are removed without splitting the trajectory. Consecutive retained positions
#' are connected even across long recording gaps, and temporal windows have no maximum-gap or minimum
#' temporal-coverage criterion. Longer intervals between fixes can conceal intervening movement and
#' reduce estimated path length; location noise can instead inflate distances and turning. Sampling
#' interval, reconstruction settings and location quality should therefore be comparable across tracks.
#'
#' The function does not propagate positional uncertainty into the metrics. A tortuous trajectory alone
#' is not evidence of foraging, and a reconstructed path is not an independently observed trajectory.
#' Assess reconstruction accuracy with [crossValidateTrack()] and interpret geometric metrics alongside
#' the sensor record and the study's biological context.
#'
#' @return A data frame with one row per eligible deployment. The following columns are always present
#'   in a non-empty result:
#'   \describe{
#'     \item{`ID`}{Deployment identifier, as character.}
#'     \item{`Total_points`}{Number of retained positions.}
#'     \item{`Track_duration_h`}{Time between the first and last retained positions, in hours.}
#'     \item{`Total_distance_km`}{Sum of consecutive horizontal step lengths, in kilometres.}
#'     \item{`Net_displacement_km`}{Great-circle distance between the first and last positions,
#'       in kilometres.}
#'     \item{`Hourly_tortuosity`, `Daily_tortuosity`}{Dimensionless mean path-to-displacement ratios
#'       over the respective temporal windows. Their names do not change when window lengths change.}
#'   }
#'   `control$metrics` selects additional columns:
#'   \describe{
#'     \item{`Path_ratio`}{Dimensionless total path length divided by net displacement; `NA` when
#'       endpoint displacement is zero.}
#'     \item{`Sinuosity`}{Local sinuosity index, in inverse square-root kilometres; `NA` when too few
#'       positive steps or turning angles are available.}
#'     \item{`Mean_turning_angle`}{Mean absolute change in bearing, in degrees.}
#'     \item{`Straightness`}{Dimensionless net displacement divided by total path length; `NA` when
#'       total path length is zero. Values near one indicate a relatively direct path.}
#'   }
#'   Duration and distance columns are rounded to two decimal places, and other numeric metrics to
#'   three. Infinite results are replaced by `NA`; undefined means may remain `NaN`. Use `is.na()`
#'   to recognise either missing-value form. If every track is omitted, the zero-row result contains
#'   only the five base columns from `ID` through `Net_displacement_km`.
#'
#' @references
#' Bovet P, Benhamou S (1988) Spatial analysis of animals' movements using a correlated random walk
#' model. *Journal of Theoretical Biology* 131:419-433. \doi{10.1016/S0022-5193(88)80038-9}
#'
#' @seealso [trackMetricsControl()] for metric selection and temporal windows; [reconstructTrack()]
#'   for track estimation; [crossValidateTrack()] for reconstruction validation; [plotTracks()] for
#'   maps; [filterLocations()] for location screening; [summarizeTagData()] for sensor-data summaries.
#'
#' @examples
#' # Illustrative geographic positions sampled once per hour
#' n <- 30
#' positions <- data.frame(
#'   ID = "deployment_01",
#'   datetime = as.POSIXct("2023-01-01", tz = "UTC") + (seq_len(n) - 1) * 3600,
#'   lon = seq(-25.2, -25.0, length.out = n),
#'   lat = 37 + 0.01 * sin(seq(0, 2 * pi, length.out = n))
#' )
#' metrics <- trackMetrics(positions, verbose = "quiet")
#' metrics[, c("ID", "Total_distance_km", "Straightness")]
#'
#' \dontrun{
#' # Reconstructed coordinate pairs are selected automatically
#' tracks <- reconstructTrack(processed)
#' metrics <- trackMetrics(tracks, control = trackMetricsControl(metrics = "all"))
#'
#' # Summarise observed fixes separately from the reconstructed track
#' fixes <- getTagMetadata(tracks[[1]])$ancillary$positions$data
#' observed_metrics <- trackMetrics(
#'   list(deployment_01 = fixes), lon.col = "lon", lat.col = "lat",
#'   control = trackMetricsControl(metrics = c("path_ratio", "straightness"))
#' )
#' }
#' @export
trackMetrics <- function(data,
                         control = trackMetricsControl(),
                         id.col = "ID",
                         lon.col = NULL,
                         lat.col = NULL,
                         datetime.col = "datetime",
                         verbose = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  control <- .as_control(control, trackMetricsControl, "nautilus_track_metrics", "control")
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  .assert_string(lon.col, "lon.col", null_ok = TRUE); .assert_string(lat.col, "lat.col", null_ok = TRUE)

  metrics <- control$metrics
  if ("all" %in% metrics)
    metrics <- c("path_ratio", "sinuosity", "turning_angle", "straightness")

  r <- .resolveInput(data, id.col = id.col)
  if (r$n == 0) return(.emptyTrackMetrics())

  .log_header(lvl, "trackMetrics", "Computing movement-path metrics",
              bullets = sprintf("Input: %d track%s", r$n, if (r$n != 1) "s" else ""),
              arrow = sprintf("metrics: %s", paste(metrics, collapse = ", ")))

  rows <- vector("list", r$n); n_ok <- 0L; n_skip <- 0L
  for (i in seq_len(r$n)) {
    x <- r$get(i)
    if (!data.table::is.data.table(x)) x <- data.table::as.data.table(x)
    who <- tryCatch(as.character(unique(x[[id.col]]))[1], error = function(e) NA_character_)
    if (length(who) != 1L || is.na(who) || !nzchar(who)) who <- r$ids[i]   # no ID col -> use the list name
    pos <- .trackPositionCols(x, lon.col, lat.col)
    .log_h2(lvl, sprintf("%s (%d/%d)", who, i, r$n))
    res <- .trackMetricsIndividual(x, who, pos$lon, pos$lat, datetime.col, metrics, control)
    if (is.null(res)) {
      n_skip <- n_skip + 1L
      .log_skip(lvl, sprintf("fewer than %d valid fixes", control$min.points))
    } else {
      rows[[i]] <- res; n_ok <- n_ok + 1L
      .log_detail(lvl, sprintf("%d fixes \u00b7 %.1f km over %.1f h \u00b7 straightness %.2f",
                               res$Total_points, res$Total_distance_km, res$Track_duration_h,
                               if (!is.null(res$Straightness)) res$Straightness else NA_real_))
    }
    .log_gap(lvl)
  }

  out <- do.call(rbind, rows)
  if (is.null(out)) out <- .emptyTrackMetrics()
  else {
    num <- vapply(out, is.numeric, logical(1))
    out[num] <- lapply(out[num], function(v) { v[is.infinite(v)] <- NA_real_; round(v, 3) })
    rownames(out) <- NULL
  }

  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, n_ok, " of ", r$n, " track", if (r$n != 1) "s", " summarised")
    if (n_skip > 0L) .log_arrow(lvl, "skipped (too few fixes): ", n_skip)
    .log_runtime(lvl, start.time)
  }
  out
}
