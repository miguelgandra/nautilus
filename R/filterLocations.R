#######################################################################################################
# Screen implausible position fixes (location-channel quality control) ################################
#######################################################################################################

#' Screen implausible satellite position fixes
#'
#' @description
#' Filters the ancillary position record of archival tag datasets using optional satellite-count,
#' distance and neighbour-consistency speed checks. Rejected fixes are removed from the stored position
#' record without changing sensor measurements or deleting rows from the sensor time series.
#'
#' The function is intended for location quality control after [importTagData()] and before
#' [reconstructTrack()] or [crossValidateTrack()] uses position fixes as spatial constraints. All
#' threshold checks are disabled by default; select limits appropriate to the tracking system, study
#' duration and movement ecology.
#'
#' @param data A tag dataset, a list of tag datasets, a data frame containing multiple deployments
#'   identified by \code{id.col}, or a character vector of \code{.rds} file paths. Position fixes must
#'   be stored in the ancillary metadata created by [importTagData()]. File inputs are read one
#'   deployment at a time.
#' @param metadata Optional deployment-metadata table containing \code{id.col}, \code{deploy.lon.col}
#'   and \code{deploy.lat.col}. Matching rows supply reference coordinates in preference to the stored
#'   deployment coordinates. If no matching row is available, stored coordinates are used. Where both
#'   sources are usable and \pkg{geosphere} is installed, discrepancies exceeding 1 km generate a
#'   warning. Default \code{NULL}.
#' @param id.col Name of the deployment-identifier column (default \code{"ID"}), also used to match
#'   rows in \code{metadata}.
#' @param max.speed.kmh Non-negative threshold for speed between retained fixes, in km/h.
#'   \code{NULL} (default) disables the speed check. Speeds represent displacement over the interval
#'   between fixes, not instantaneous swimming speed; select the threshold accordingly.
#' @param max.distance.km Non-negative maximum distance from the deployment reference location, in km.
#'   \code{NULL} (default) disables this check. This is a gross-error screen independent of elapsed
#'   time, and can remove genuine displacement in a long or wide-ranging deployment.
#' @param min.satellites Minimum satellite count required for a \code{"FastGPS"} fix. Must be a
#'   positive integer; \code{NULL} (default) disables this check. Missing or non-numeric counts are
#'   retained, and other position types are not assessed by satellite count.
#' @param control A [filterLocationsControl()] object or a named list of its arguments specifying
#'   minimum fix separation, the iteration limit and an optional direction-reversal test.
#'   \code{NULL} (default) uses the constructor defaults.
#' @param deploy.lon.col,deploy.lat.col Names of the longitude and latitude columns in
#'   \code{metadata} (defaults \code{"deploy_lon"} and \code{"deploy_lat"}). Coordinates must be
#'   geographic longitude and latitude in decimal degrees.
#' @param plot Logical; whether to display diagnostic maps on the active graphics device (default
#'   \code{FALSE}). Only deployments with removed fixes contribute a page.
#' @param plot.file Optional path to a multi-page diagnostic PDF, independent of \code{plot}. The name
#'   must end in \code{.pdf} and the parent directory must exist. No PDF is created when no fixes are
#'   removed. Default \code{NULL}.
#' @param basemap Map background: \code{"land"} (default), \code{"satellite"}, \code{"none"}, or a
#'   pre-fetched RGB \pkg{terra} \code{SpatRaster} returned by [getBasemap()]. Bathymetry rasters are
#'   not supported here. Satellite imagery requires the optional mapping packages and, for automatic
#'   retrieval, network access.
#' @param coastline Vector coastline: \code{"auto"} (default), \code{"high"}, \code{"low"},
#'   \code{"none"}, an \pkg{sf} geometry, a longitude/latitude table or matrix, or a spatial-file path.
#'   Custom coordinates must be geographic longitude and latitude. Coastlines are filled on a land
#'   background and outlined over imagery; \code{basemap = "none"} omits them. See [plotTracks()] for
#'   supported inputs and automatic resolution selection.
#' @param basemap.control A [basemapControl()] object controlling automatic satellite-tile retrieval.
#'   Ignored for other backgrounds and pre-fetched rasters.
#' @param return.data Logical; whether to return datasets in memory (default \code{TRUE}). With
#'   \code{FALSE}, written \code{.rds} paths are returned invisibly, requiring \code{output.dir}.
#' @param output.dir An existing directory in which datasets with a non-empty position record are
#'   saved as individual \code{<id>.rds} files. Supplying a directory triggers saving, including when
#'   no fixes were removed. Deployments without positions are not written. Default \code{NULL}.
#' @param output.suffix Optional string appended to saved deployment identifiers before \code{.rds},
#'   to label a processing run or avoid overwriting earlier files. Default \code{NULL}.
#' @param compress Compression used when saving \code{.rds} files: \code{TRUE} (default, gzip),
#'   \code{FALSE}, or one of \code{"gzip"}, \code{"bzip2"} or \code{"xz"}. Only used when
#'   \code{output.dir} is specified. See [base::saveRDS()].
#' @param verbose How much detail to print: \code{0}/\code{"quiet"}, \code{1}/\code{"normal"}, or
#'   \code{2}/\code{"detailed"} (default), which adds per-check diagnostics.
#'
#' @details
#' ## Position record and workflow
#'
#' Fixes are read from \code{getTagMetadata(x)$ancillary$positions$data}, whose canonical columns are
#' \code{datetime}, \code{type}, \code{lon}, \code{lat} and \code{quality}. The function does not
#' filter sample-level longitude or latitude columns. Supply valid geographic coordinates and
#' timestamps; this is not a general coordinate-validation or duplicate-removal utility.
#'
#' Only \code{"FastGPS"} and \code{"Argos"} fixes with non-missing coordinates can be removed.
#' Other types, including curated \code{"User"} fixes, are retained and still act as neighbours in the
#' speed check. Stored deployment and pop-up coordinates are separate reference points and are not
#' modified. Argos quality classes are not screened directly, and coastlines are for display rather
#' than land-crossing tests.
#'
#' ## Filtering sequence
#'
#' Enabled checks are applied in this order:
#'
#' \enumerate{
#'   \item Satellite count: remove \code{"FastGPS"} fixes with a known count below
#'     \code{min.satellites}.
#'   \item Distance: remove eligible fixes farther than \code{max.distance.km} from the reference
#'     deployment location. If that location is unavailable, a warning is issued and the distance
#'     check is skipped for that deployment; other checks continue.
#'   \item Speed: iteratively screen the remaining fixes in chronological order using geodesic
#'     distance divided by elapsed time.
#' }
#'
#' The speed check follows the neighbour-consistency principle of Freitas et al. (2008). An interior
#' fix is a candidate for removal when speeds to both adjacent retained fixes exceed
#' \code{max.speed.kmh}. An endpoint has only one neighbour and is a candidate when that segment
#' exceeds the threshold. A single fast segment within a record is insufficient to identify which
#' endpoint is erroneous.
#'
#' If \code{control$spike.angle} is set, an interior fix can also be removed when the change in
#' bearing reaches that angle and at least one adjacent speed exceeds the threshold. This supplements
#' rather than replaces the speed requirement. Non-finite speeds and intervals shorter than
#' \code{control$min.time.mins} are not judged.
#'
#' Each pass removes the candidate with the largest adjacent speed, then recomputes neighbours.
#' Filtering stops when no candidate remains or \code{control$max.iterations} is reached. Retained
#' segments are not guaranteed to fall below the speed threshold: one-sided exceedances, trusted fix
#' types and the iteration limit can leave faster segments in the record.
#'
#' ## Outputs and provenance
#'
#' There is no report-only mode: enabled checks immediately update returned or saved position records.
#' Preserve the original inputs if removed coordinates must remain available for subsequent review.
#' Supplied deployment metadata controls the reference location but does not replace stored deployment
#' coordinates.
#'
#' Each deployment with positions receives a processing-history entry recording the three main
#' thresholds and the number of removed fixes. Disabled thresholds are recorded as missing values.
#' The full control object and removed coordinates are not stored in that entry. Use
#' [processingHistory()] to inspect it; no deployment-exclusion log is written.
#'
#' Deployments without positions pass through unchanged in memory but are omitted from saved outputs
#' and the returned file-path vector. With all threshold checks disabled, a warning is issued and
#' position records are retained.
#'
#' ## Diagnostics and dependencies
#'
#' Diagnostic maps distinguish retained fixes from removals by the first failed check, and show the
#' path connecting retained fixes and available deployment anchors. These connecting segments are not
#' reconstructed underwater trajectories.
#'
#' The speed and distance checks require \pkg{geosphere}; satellite-count filtering alone does not.
#' The default land background uses locally available coastline data. Satellite backgrounds require
#' \pkg{maptiles}, \pkg{terra} and \pkg{sf} for automatic retrieval; tile settings are provided by
#' [basemapControl()]. See [plotTracks()] and [getBasemap()] for mapping dependencies and background
#' preparation.
#'
#' @return With \code{return.data = TRUE}, a named list of deployment datasets, with filtered
#'   ancillary position records where available and unchanged sensor time series. With
#'   \code{return.data = FALSE}, a character vector of written \code{.rds} paths, returned invisibly.
#'   Deployments without a position record are retained only in the in-memory result. Diagnostic
#'   graphics and saved files are optional side effects.
#'
#' @references
#' Freitas C, Lydersen C, Fedak MA, Kovacs KM (2008) A simple new algorithm to filter marine mammal
#' Argos locations. \emph{Marine Mammal Science} 24:315-325. \doi{10.1111/j.1748-7692.2007.00180.x}
#'
#' @seealso [importTagData()], [filterLocationsControl()], [checkSensorQuality()],
#'   [reconstructTrack()], [crossValidateTrack()], [plotTracks()], [processingHistory()].
#'
#' @examples
#' \dontrun{
#' # Thresholds are illustrative and must be validated for the study.
#' imported <- importTagData(folders, metadata = deployments)
#' cleaned <- filterLocations(
#'   imported,
#'   max.speed.kmh = 8,
#'   min.satellites = 4,
#'   control = filterLocationsControl(min.time.mins = 2),
#'   plot.file = "location_quality.pdf")
#'
#' getTagMetadata(cleaned[[1]])$ancillary$positions$data
#' processingHistory(cleaned[[1]])
#'
#' # For a disk-based workflow, the output directory must already exist.
#' cleaned.files <- filterLocations(
#'   list.files("./data interim/01_imported", pattern = "\\.rds$", full.names = TRUE),
#'   max.speed.kmh = 8,
#'   min.satellites = 4,
#'   output.dir = "./data interim/02_locations",
#'   return.data = FALSE)
#' }
#' @export


filterLocations <- function(data,
                            metadata = NULL,
                            id.col = "ID",
                            max.speed.kmh = NULL,
                            max.distance.km = NULL,
                            min.satellites = NULL,
                            control = NULL,
                            deploy.lon.col = "deploy_lon",
                            deploy.lat.col = "deploy_lat",
                            plot = FALSE,
                            plot.file = NULL,
                            basemap = c("land", "satellite", "none"),
                            coastline = "auto",
                            basemap.control = basemapControl(),
                            return.data = TRUE,
                            output.dir = NULL,
                            output.suffix = NULL,
                            compress = TRUE,
                            verbose = "detailed") {


  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

  # measure running time
  start.time <- Sys.time()

  # resolve the verbosity level (0 quiet / 1 normal / 2 detailed)
  lvl <- .verbosity(verbose)

  # show warnings inline (per-individual issues next to their dataset) rather than batched at the end;
  # only upgrade the default (never override a user's stricter setting). Restored on exit.
  if (identical(getOption("warn"), 0L) || identical(getOption("warn"), 0)) {
    .oldwarn <- options(warn = 1); on.exit(options(.oldwarn), add = TRUE)
  }

  # validate scalar arguments
  .assert_flag(return.data, "return.data"); .assert_flag(plot, "plot")
  .assert_string(id.col, "id.col")
  .assert_string(deploy.lon.col, "deploy.lon.col"); .assert_string(deploy.lat.col, "deploy.lat.col")
  .assert_number(max.speed.kmh, "max.speed.kmh", min = 0, null_ok = TRUE)
  .assert_number(max.distance.km, "max.distance.km", min = 0, null_ok = TRUE)
  .assert_count(min.satellites, "min.satellites", min = 1L, null_ok = TRUE)
  .assert_writable_file(plot.file, "plot.file", ext = "pdf")   # fail-fast: parent dir must exist
  # diagnostic-map background canvas: "land"/"none"/"satellite"(+ a pre-fetched raster) are live
  basemap.control <- .as_control(basemap.control, basemapControl, "nautilus_basemap", "basemap.control")
  bm <- .resolveBasemap(basemap, c("land", "satellite", "none"))
  # a depth canvas is a plotTracks concern (presentation), not location QC - reject it explicitly rather
  # than silently drawing a blank sea for a pre-fetched marmap grid
  if (identical(bm$kind, "bathymetry"))
    .abort(c("A bathymetry {.arg basemap} is not available in {.fn filterLocations}.",
             "i" = "Depth does not inform location QC; use {.val land} (default), {.val satellite} or {.val none}.",
             "i" = "For depth relief or isobaths on a presentation map, see {.fn plotTracks}."))
  coast_fill <- identical(bm$kind, "land")
  # resolve the coastline only when a map is actually drawn, so the low-res hint never fires on a
  # filter-only run (plot = FALSE); a silent no-op otherwise
  coast_spec <- if ((plot || !is.null(plot.file)) && bm$kind %in% c("land", "satellite", "raster"))
                  .resolveCoastline(coastline, lvl) else list(kind = "none")
  .assert_dir(output.dir, "output.dir")                        # fail-fast: must exist
  .assert_string(output.suffix, "output.suffix", null_ok = TRUE)
  .assert_compress(compress)
  ctrl <- .as_control(control, filterLocationsControl, "nautilus_filter_locations", "control")

  # at least one output method must be selected
  .assert_output(return.data, output.dir)

  # which checks were requested
  do_sat   <- !is.null(min.satellites)
  do_dist  <- !is.null(max.distance.km)
  do_speed <- !is.null(max.speed.kmh)

  # the speed and distance checks compute great-circle geometry
  if ((do_speed || do_dist) && !requireNamespace("geosphere", quietly = TRUE)) {
    .abort(c("The {.pkg geosphere} package is required for the speed / distance checks but is not installed.",
             "i" = "Install it with {.code install.packages(\"geosphere\")}, or leave {.arg max.speed.kmh} and {.arg max.distance.km} as {.code NULL}."))
  }

  # validate metadata if supplied (deployment coordinates for the distance check / map anchor)
  if (!is.null(metadata)) {
    .assert_columns(metadata, c(id.col, deploy.lon.col, deploy.lat.col), "metadata")
    metadata <- as.data.frame(metadata)
  }

  make_plots <- plot || !is.null(plot.file)

  # resolve the input into a uniform iterable (list / single df / .rds paths); guards empty input
  r <- .resolveInput(data, id.col = id.col)

  ##############################################################################
  # Header #####################################################################
  ##############################################################################

  # thresholds listed ONCE here, in the order the checks are applied, so the per-deployment blocks below
  # need only report counts (the numbers stay available without being repeated 52 times)
  criteria <- c(if (do_sat)   sprintf("Minimum satellites: %d", min.satellites),
                if (do_dist)  sprintf("Maximum distance: %g km from deployment", max.distance.km),
                if (do_speed) sprintf("Maximum speed: %g km/h", max.speed.kmh))
  hdr_bullets <- sprintf("Input: %d dataset%s", r$n, if (r$n != 1) "s" else "")
  if (!is.null(output.dir)) hdr_bullets <- c(hdr_bullets, paste0("Output: ", output.dir))
  hdr_bullets <- c(hdr_bullets, if (length(criteria)) "Filtering criteria:" else "No checks enabled")
  .log_header(lvl, "filterLocations", "Screening position fixes for implausible locations",
              bullets = hdr_bullets, sub = criteria)

  # nudge if the function would be a no-op (nothing enabled) - a QC step that removes nothing is
  # almost always an oversight (thresholds are species-specific, so there is no safe default)
  if (!length(criteria)) {
    cli::cli_warn(c("No location checks are enabled, so no fixes will be removed.",
                    "i" = "Set {.arg max.speed.kmh}, {.arg min.satellites} and/or {.arg max.distance.km} to screen the fixes."))
  }

  ##############################################################################
  # Process each data element ##################################################
  ##############################################################################

  results  <- if (return.data) vector("list", r$n) else NULL
  saved    <- vector("list", r$n)
  payloads <- if (make_plots) vector("list", r$n) else NULL
  n_touched <- 0L; total_removed <- 0L
  # The SUMMARY separates three different denominators that a single "across N datasets" used to blur:
  # how much was actually SCREENED, how much was SKIPPED for having no fixes, and how much was TOUCHED
  # by a removal. Most datasets that are screened lose nothing, so the removal count belongs to its own
  # (smaller) set, not to the input count.
  n_skipped <- 0L; n_screened <- 0L; total_fixes <- 0L
  # per-criterion tallies for the SUMMARY breakdown (why fixes were discarded, not just how many)
  total_sat <- 0L; total_dist <- 0L; total_speed <- 0L

  for (i in seq_len(r$n)) {

    # load / access the individual (metadata ensured / migrated by .resolveInput)
    x  <- r$get(i, writable = TRUE)
    id <- r$ids[i]
    .log_h2(lvl, sprintf("%s (%d/%d)", id, i, r$n))

    meta <- .getMeta(x)
    pos  <- .tagPositions(x)                          # canonical record: datetime,type,lon,lat,quality

    # nothing to screen: no position fixes for this deployment
    if (!nrow(pos)) {
      # plain hyphen, not an em dash: this line must survive a non-UTF-8 device, where a raw \u2014
      # would print as an escape (cli only auto-degrades its OWN symbols)
      if (lvl >= 1L) cli::cli_text("{cli::symbol$bullet} skipped - no position fixes")
      n_skipped <- n_skipped + 1L
      .log_gap(lvl)
      if (make_plots) payloads[[i]] <- NULL
      if (return.data) { results[[i]] <- x }
      next
    }

    # order fixes by time and pre-compute numeric time
    pos <- pos[order(pos$datetime), , drop = FALSE]
    pos$time_num <- as.numeric(pos$datetime)
    n_fix <- nrow(pos)
    n_screened <- n_screened + 1L; total_fixes <- total_fixes + n_fix

    # only the automatically-acquired fixes may be removed; User fixes are trusted anchors
    removable <- pos$type %in% c("FastGPS", "Argos") & !is.na(pos$lon) & !is.na(pos$lat)

    # resolve the reference deployment position (supplied metadata -> meta$deployment)
    deploy <- .resolveDeployPosition(meta, metadata, id, id.col, deploy.lon.col, deploy.lat.col, pos, lvl)

    # per-fix outcome, filled as the checks run (""=kept)
    reason <- rep(NA_character_, n_fix)               # NA while retained; set to the removing check
    removed <- rep(FALSE, n_fix)

    counts <- list(satellite = 0L, distance = 0L, speed = 0L)

    # ---- 1. satellite count (Fastloc-GPS only) ------------------------------------------------
    if (do_sat) {
      sat <- .asNumericSafe(pos$quality)                     # WC Fastloc Quality = satellite count
      hit <- which(!removed & removable & pos$type == "FastGPS" & !is.na(sat) & sat < min.satellites)
      if (length(hit)) { removed[hit] <- TRUE; reason[hit] <- "satellite"; counts$satellite <- length(hit) }
    }

    # ---- 2. distance from deployment (gross-error bound) --------------------------------------
    if (do_dist) {
      if (is.null(deploy)) {
        cli::cli_warn("{id}: no deployment position available; skipping the distance check.")
      } else {
        cand <- which(!removed & removable)
        if (length(cand)) {
          d_km <- geosphere::distGeo(cbind(pos$lon[cand], pos$lat[cand]),
                                     c(deploy$lon, deploy$lat)) / 1000
          hit <- cand[is.finite(d_km) & d_km > max.distance.km]
          if (length(hit)) { removed[hit] <- TRUE; reason[hit] <- "distance"; counts$distance <- length(hit) }
        }
      }
    }

    # ---- 3. speed (neighbour-consistency root test, iterative) --------------------------------
    if (do_speed) {
      keep_idx <- which(!removed)                                     # survivors, in time order
      sp_rm <- .locationSpeedFilter(lon = pos$lon[keep_idx], lat = pos$lat[keep_idx],
                                    time_num = pos$time_num[keep_idx],
                                    removable = removable[keep_idx],
                                    max.speed.kmh = max.speed.kmh, ctrl = ctrl)
      hit <- keep_idx[sp_rm]
      if (length(hit)) { removed[hit] <- TRUE; reason[hit] <- "speed"; counts$speed <- length(hit) }
    }

    n_rm <- sum(removed)

    # ---- per-individual reporting -------------------------------------------------------------
    bt <- cli::symbol$bullet
    .log_detail(lvl, "fixes: ", n_fix, " (FastGPS ", sum(pos$type == "FastGPS"), " ", bt,
                " Argos ", sum(pos$type == "Argos"), " ", bt, " User ", sum(pos$type == "User"), ")")
    if (do_sat)   .log_detail(lvl, "satellites: ", counts$satellite, " removed")
    if (do_dist)  .log_detail(lvl, "distance: ",  counts$distance,  " removed")
    if (do_speed) .log_detail(lvl, "speed: ",     counts$speed,     " removed")

    # ---- gather diagnostic payload BEFORE dropping the removed fixes ---------------------------
    if (make_plots) {
      dep <- meta$deployment
      popup <- if (!is.null(dep) && !is.null(dep$popup_lon) && !is.null(dep$popup_lat) &&
                   !is.na(dep$popup_lon) && !is.na(dep$popup_lat))
                 list(lon = dep$popup_lon, lat = dep$popup_lat) else NULL
      payloads[[i]] <- list(id = id, pos = pos, removed = removed, reason = reason,
                            deploy = deploy, popup = popup,
                            max.distance.km = if (do_dist) max.distance.km else NULL,
                            counts = counts, n_fix = n_fix)
    }

    # ---- write the survivors back to the canonical record -------------------------------------
    if (n_rm > 0) {
      surv <- pos[!removed, , drop = FALSE]
      surv$time_num <- NULL
      meta$ancillary$positions$data <- surv[, c("datetime", "type", "lon", "lat", "quality"), drop = FALSE]
    }
    meta <- .appendProcessing(meta, "filterLocations",
                              max_speed_kmh = if (do_speed) max.speed.kmh else NA_real_,
                              max_distance_km = if (do_dist) max.distance.km else NA_real_,
                              min_satellites = if (do_sat) min.satellites else NA_integer_,
                              removed = n_rm)
    x <- .restoreMeta(x, meta)

    # save to disk if requested
    saved_to <- .saveOutput(x, id, output.dir = output.dir, output.suffix = output.suffix,
                            compress = compress)
    saved[i] <- list(saved_to)

    # closing line
    if (n_rm > 0) {
      n_touched <- n_touched + 1L; total_removed <- total_removed + n_rm
      .log_skip(lvl, n_rm, " of ", n_fix, " fix", if (n_fix != 1) "es", " removed")
    }
    total_sat   <- total_sat   + counts$satellite
    total_dist  <- total_dist  + counts$distance
    total_speed <- total_speed + counts$speed
    if (!is.null(saved_to)) .log_ok(lvl, "saved ", basename(saved_to)) else .log_ok(lvl, id, " screened")
    .log_gap(lvl)

    if (return.data) results[[i]] <- x
  }

  ##############################################################################
  # Diagnostic maps ############################################################
  ##############################################################################

  if (make_plots) {
    to_draw <- Filter(function(p) !is.null(p) && any(p$removed), payloads)
    if (length(to_draw)) {
      draw <- function(to.file = FALSE, unicode = TRUE) {
        for (p in to_draw) .plotLocationPanel(p, coast_spec = coast_spec, coast_fill = coast_fill,
                                              bm = bm, basemap.control = basemap.control, unicode = unicode)
      }
      .renderToDevices(draw, plot = plot, plot.file = plot.file, width = 8, height = 8, cairo = TRUE)
    } else if (lvl >= 1L) {
      .log_info(lvl, "no fixes removed - no diagnostic maps to draw")
    }
  }

  ##############################################################################
  # Return #####################################################################
  ##############################################################################

  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, "Screened ", .formatNumber(total_fixes), " fix", if (total_fixes != 1) "es",
              " from ", n_screened, " dataset", if (n_screened != 1) "s")
    # a run where nothing was skipped gets no row, rather than a "0 datasets skipped" one
    if (n_skipped)
      .log_done(lvl, n_skipped, " dataset", if (n_skipped != 1) "s", " skipped (no position fixes)")
    if (total_removed) {
      # the colon introduces the per-criterion breakdown below, which only renders at the detailed
      # level - so it is only added when something actually follows it
      .log_done(lvl, .formatNumber(total_removed), " fix", if (total_removed != 1) "es",
                " removed from ", n_touched, " dataset", if (n_touched != 1) "s",
                if (lvl >= 2L) ":" else "")
      # why they were removed, one line per ENABLED check (a disabled check has no row, not a zero row)
      if (do_sat)   .log_subdetail(lvl, sprintf("Satellites (< %d): %d", min.satellites, total_sat))
      if (do_dist)  .log_subdetail(lvl, sprintf("Distance (> %g km): %d", max.distance.km, total_dist))
      if (do_speed) .log_subdetail(lvl, sprintf("Speed (> %g km/h): %d", max.speed.kmh, total_speed))
    } else if (n_screened) {
      # nothing removed: the per-criterion breakdown would be a column of zeros. Skipped entirely when
      # nothing was screened either - "0 fixes from 0 datasets" already says it.
      .log_done(lvl, "No fixes removed")
    }
    if (!is.null(output.dir)) .log_arrow(lvl, "output: ", output.dir)
    if (!is.null(plot.file)) .log_arrow(lvl, "plots: ", plot.file)
    .log_runtime(lvl, start.time)
  }

  .collectOutput(results, saved, return.data, r$ids)
}
