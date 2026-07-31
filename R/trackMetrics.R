#######################################################################################################
# Movement-path metrics (tortuosity + supporting track statistics) ####################################
#######################################################################################################

#' Summarise each movement path into trajectory metrics
#'
#' @description
#' A reconstructed track holds thousands of positions, but the questions asked of it are usually
#' summary ones: how far did the animal travel, how far did it actually get, and how convoluted was the
#' route in between. The last of these - tortuosity - is a common proxy for behavioural mode, since a
#' straight transit and a tightly looping search look quite different even when they cover the same
#' distance.
#'
#' This function reduces each path to one row of such measures: path length, net displacement, and a
#' family of tortuosity and straightness indices. Run it on the pseudo-track from [reconstructTrack()],
#' or on any track carrying longitude, latitude and timestamps.
#'
#' @param data A tag object or data frame holding a track, a list of them, a single table with an
#'   `id.col`, or a character vector of `.rds` paths. Each must carry longitude, latitude and timestamp
#'   columns.
#' @param control A control object from [trackMetricsControl()] selecting which metrics to compute and
#'   the rolling-window sizes. Pass `trackMetricsControl(...)` to change it.
#' @param id.col Which column identifies the animal (default `"ID"`).
#' @param lon.col,lat.col Which columns hold longitude and latitude. Left `NULL` (default), the
#'   reconstructed columns `pseudo_lon` and `pseudo_lat` are used where present, and `lon`/`lat`
#'   otherwise. Name them explicitly to summarise a different track, such as the raw satellite fixes.
#' @param datetime.col Which column holds the timestamps (default `"datetime"`).
#' @param verbose How much detail to print: `0`/`"quiet"`, `1`/`"normal"`, or `2`/`"detailed"`
#'   (default).
#'
#' @details
#' The measures give complementary views of the same path. `Path_ratio` and `Straightness` are global,
#' comparing the route to the straight line from start to end, and so say nothing about where the
#' wandering happened. `Sinuosity` and `Mean_turning_angle` are driven by local turning instead, and
#' will separate two paths that share the same endpoints but not the same behaviour.
#' `Hourly_tortuosity` and `Daily_tortuosity` sit between the two, reporting how the
#' path-to-displacement ratio behaves over rolling windows.
#'
#' Distances are computed with the haversine great-circle formula. A metric is returned as `NA` where
#' the track is too short to support it, rather than guessed.
#'
#' Run this after [reconstructTrack()], which supplies `pseudo_lon` and `pseudo_lat`. It reduces a track
#' to a summary and never modifies the input.
#'
#' @return A data frame with one row per animal, holding whichever of these the selected metrics
#'   produce:
#'
#' - `ID` - the animal identifier.
#' - `Total_points` - how many positions were used.
#' - `Track_duration_h` - the span of the track in hours.
#' - `Total_distance_km` - path length, the sum of the great-circle steps.
#' - `Net_displacement_km` - straight-line distance from the first position to the last.
#' - `Path_ratio` - path length divided by net displacement; 1 for a straight path, larger for a
#'   convoluted one.
#' - `Sinuosity` - the index of Bovet and Benhamou (1988), `1.18 * sd(turning angle in radians) /
#'   sqrt(mean step length in km)`.
#' - `Mean_turning_angle` - the mean absolute change in bearing, in degrees.
#' - `Straightness` - net displacement divided by path length; 0 for a tortuous path, 1 for a straight
#'   one.
#' - `Hourly_tortuosity`, `Daily_tortuosity` - the mean path-to-displacement ratio over rolling windows
#'   of `control$hourly.window.h` and `control$daily.window.h` hours.
#'
#' @references
#' Bovet P, Benhamou S (1988) Spatial analysis of animals' movements using a correlated random walk
#' model. *Journal of Theoretical Biology* 131:419-433. \doi{10.1016/S0022-5193(88)80038-9}
#'
#' @seealso [trackMetricsControl()] for selecting the metrics; [reconstructTrack()] for producing the
#'   track; [summarizeTagData()] for a deployment-level overview.
#'
#' @examples
#' \dontrun{
#' tracks <- reconstructTrack(processed)
#'
#' # one summary row per animal, using pseudo_lon/pseudo_lat automatically
#' metrics <- trackMetrics(tracks, control = trackMetricsControl(metrics = "all"))
#' metrics[, c("ID", "Total_distance_km", "Straightness")]
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
