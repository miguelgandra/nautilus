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

#' Resolve the longitude/latitude columns for one track: honour explicit names, else prefer the
#' `reconstructTrack()` output (`pseudo_lon`/`pseudo_lat`) and fall back to raw `lon`/`lat`.
#' @keywords internal
#' @noRd
.trackPositionCols <- function(x, lon.col, lat.col) {
  # auto-detect the pair JOINTLY: prefer reconstructTrack's pseudo_* only when BOTH are present (never mix a
  # pseudo axis with a raw one), else fall back to lon/lat. Explicit names always override per axis.
  auto <- if (all(c("pseudo_lon", "pseudo_lat") %in% names(x))) c("pseudo_lon", "pseudo_lat") else c("lon", "lat")
  list(lon = if (!is.null(lon.col)) lon.col else auto[1],
       lat = if (!is.null(lat.col)) lat.col else auto[2])
}

#' An empty, correctly-typed track-metrics table (returned when there is nothing to summarise).
#' @keywords internal
#' @noRd
.emptyTrackMetrics <- function() {
  data.frame(ID = character(0), Total_points = integer(0), Track_duration_h = numeric(0),
             Total_distance_km = numeric(0), Net_displacement_km = numeric(0),
             stringsAsFactors = FALSE)
}

#' Movement-path metrics for a single animal's track. Returns a one-row data frame, or NULL when the
#' track has too few valid fixes.
#' @keywords internal
#' @noRd
.trackMetricsIndividual <- function(x, id, lon.col, lat.col, datetime.col, metrics, control) {
  if (is.null(x) || nrow(x) == 0) return(NULL)
  need <- c(lon.col, lat.col, datetime.col)
  # A track without positions yields no path metrics - the same outcome as one with too few fixes
  # (below), which this helper already signals by returning NULL. Returning NULL here too keeps ONE
  # unusable track from aborting the whole cohort; the caller counts and reports the omission.
  missing_cols <- need[!need %in% names(x)]
  if (length(missing_cols)) {
    cli::cli_warn(c("Track {.val {id}} is missing required column{?s} {.val {missing_cols}} - no metrics computed.",
                    "i" = "Set {.arg lon.col}/{.arg lat.col} (found: {.val {intersect(c('pseudo_lon','pseudo_lat','lon','lat'), names(x))}})."))
    return(NULL)
  }

  clean <- x[!is.na(get(lon.col)) & !is.na(get(lat.col))]
  if (nrow(clean) < control$min.points) return(NULL)
  clean <- clean[order(get(datetime.col))]

  lon <- clean[[lon.col]]; lat <- clean[[lat.col]]
  distances      <- .trackDistances(lon, lat)
  bearings       <- .trackBearings(lon, lat)
  turning_angles <- .trackTurningAngles(bearings)

  total_points   <- nrow(clean)
  track_duration <- as.numeric(difftime(max(clean[[datetime.col]]), min(clean[[datetime.col]]), units = "hours"))
  total_distance <- sum(distances, na.rm = TRUE)
  net_displacement <- .trackDistance(lon[1], lat[1], lon[total_points], lat[total_points])

  results <- data.frame(
    ID = id, Total_points = total_points,
    Track_duration_h = round(track_duration, 2),
    Total_distance_km = round(total_distance, 2),
    Net_displacement_km = round(net_displacement, 2),
    stringsAsFactors = FALSE)

  if ("path_ratio" %in% metrics)
    results$Path_ratio <- if (net_displacement > 0) total_distance / net_displacement else NA_real_

  if ("sinuosity" %in% metrics) {
    # Bovet & Benhamou (1988) sinuosity index: S = 1.18 * sigma / sqrt(q), where sigma is the SD of the
    # turning angles (radians) and q is the mean step length (km). Captures local path wiggliness rather
    # than the global start-to-end ratio (that is "path_ratio").
    step_len <- distances[is.finite(distances) & distances > 0]
    ta_rad   <- turning_angles[is.finite(turning_angles)] * pi / 180
    if (length(step_len) >= 1L && length(ta_rad) >= 2L) {
      q <- mean(step_len)
      results$Sinuosity <- if (q > 0) 1.18 * stats::sd(ta_rad) / sqrt(q) else NA_real_
    } else results$Sinuosity <- NA_real_
  }

  if ("turning_angle" %in% metrics)
    results$Mean_turning_angle <- mean(abs(turning_angles), na.rm = TRUE)

  if ("straightness" %in% metrics)
    results$Straightness <- if (total_distance > 0) net_displacement / total_distance else NA_real_

  results$Hourly_tortuosity <- if (track_duration >= control$hourly.window.h)
    .trackTemporalTortuosity(clean, lon.col, lat.col, datetime.col, control$hourly.window.h) else NA_real_
  results$Daily_tortuosity <- if (track_duration >= control$daily.window.h)
    .trackTemporalTortuosity(clean, lon.col, lat.col, datetime.col, control$daily.window.h) else NA_real_

  results
}

#######################################################################################################
# Geometry helpers (haversine distance, bearing, turning angle) #######################################
#######################################################################################################

#' Great-circle distance (km) between two points (haversine).
#' @keywords internal
#' @noRd
.trackDistance <- function(lon1, lat1, lon2, lat2) {
  R <- 6371                                                # Earth radius, km
  dLat <- (lat2 - lat1) * pi / 180
  dLon <- (lon2 - lon1) * pi / 180
  a <- sin(dLat / 2)^2 + cos(lat1 * pi / 180) * cos(lat2 * pi / 180) * sin(dLon / 2)^2
  R * 2 * atan2(sqrt(a), sqrt(1 - a))
}

#' Consecutive great-circle step distances (km) along a track.
#' @keywords internal
#' @noRd
.trackDistances <- function(lon, lat) {
  n <- length(lon)
  if (n < 2L) return(numeric(0))
  vapply(seq_len(n - 1L), function(i) .trackDistance(lon[i], lat[i], lon[i + 1L], lat[i + 1L]), numeric(1))
}

#' Initial bearing (degrees, 0-360) from point 1 to point 2.
#' @keywords internal
#' @noRd
.trackBearing <- function(lon1, lat1, lon2, lat2) {
  lon1 <- lon1 * pi / 180; lat1 <- lat1 * pi / 180
  lon2 <- lon2 * pi / 180; lat2 <- lat2 * pi / 180
  dLon <- lon2 - lon1
  y <- sin(dLon) * cos(lat2)
  x <- cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dLon)
  (atan2(y, x) * 180 / pi + 360) %% 360
}

#' Consecutive initial bearings (degrees) along a track.
#' @keywords internal
#' @noRd
.trackBearings <- function(lon, lat) {
  n <- length(lon)
  if (n < 2L) return(numeric(0))
  vapply(seq_len(n - 1L), function(i) .trackBearing(lon[i], lat[i], lon[i + 1L], lat[i + 1L]), numeric(1))
}

#' Turning angles (degrees, -180..180) between consecutive bearings.
#' @keywords internal
#' @noRd
.trackTurningAngles <- function(bearings) {
  n <- length(bearings)
  if (n < 2L) return(numeric(0))
  vapply(seq_len(n - 1L), function(i) {
    d <- bearings[i + 1L] - bearings[i]
    if (d > 180) d <- d - 360
    if (d < -180) d <- d + 360
    d
  }, numeric(1))
}

#' Mean path/displacement ratio over rolling windows of `window.hours` hours (NA if the track is shorter).
#' @keywords internal
#' @noRd
.trackTemporalTortuosity <- function(data, lon.col, lat.col, datetime.col, window.hours) {
  data <- data[order(get(datetime.col))]
  start_time <- min(data[[datetime.col]]); end_time <- max(data[[datetime.col]])
  if (as.numeric(difftime(end_time, start_time, units = "hours")) < window.hours) return(NA_real_)

  window_starts <- seq(start_time, end_time - window.hours * 3600, by = window.hours * 3600)
  vals <- vapply(seq_along(window_starts), function(i) {
    w <- data[get(datetime.col) >= window_starts[i] & get(datetime.col) <= window_starts[i] + window.hours * 3600]
    if (nrow(w) < 3L) return(NA_real_)
    path_length <- sum(.trackDistances(w[[lon.col]], w[[lat.col]]), na.rm = TRUE)
    net_disp <- .trackDistance(w[[lon.col]][1], w[[lat.col]][1], w[[lon.col]][nrow(w)], w[[lat.col]][nrow(w)])
    if (net_disp > 0) path_length / net_disp else NA_real_
  }, numeric(1))
  mean(vals, na.rm = TRUE)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
