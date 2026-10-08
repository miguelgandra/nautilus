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
    x  <- r$get(i)
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


#######################################################################################################
# Internal: deployment-position resolver ##############################################################
#######################################################################################################

# The reference deployment coordinate used by the distance check and the diagnostic map. Resolution
# order: an explicit `metadata` row (deploy.lon.col / deploy.lat.col), then the tag's own metadata
# (meta$deployment, populated at import). When both metadata and
# meta$deployment are present they are cross-checked and a disagreement over 1 km is warned about.
# Returns list(lon, lat, source) or NULL when no reference is available.
#' @keywords internal
#' @noRd
.resolveDeployPosition <- function(meta, metadata, id, id.col, deploy.lon.col, deploy.lat.col, pos, lvl) {

  from_meta <- NULL
  dep <- meta$deployment
  if (!is.null(dep) && !is.null(dep$lon) && !is.null(dep$lat) && !is.na(dep$lon) && !is.na(dep$lat)) {
    from_meta <- list(lon = dep$lon, lat = dep$lat, source = "meta$deployment")
  }

  from_md <- NULL
  if (!is.null(metadata)) {
    row <- metadata[as.character(metadata[[id.col]]) == as.character(id), , drop = FALSE]
    if (nrow(row) > 0) {
      dl <- .asNumericSafe(row[[deploy.lon.col]][1]); da <- .asNumericSafe(row[[deploy.lat.col]][1])
      if (!is.na(dl) && !is.na(da)) from_md <- list(lon = dl, lat = da, source = "metadata")
    }
  }

  # cross-check the two independent sources
  if (!is.null(from_md) && !is.null(from_meta) && requireNamespace("geosphere", quietly = TRUE)) {
    dkm <- geosphere::distGeo(c(from_md$lon, from_md$lat), c(from_meta$lon, from_meta$lat)) / 1000
    if (is.finite(dkm) && dkm > 1) {
      cli::cli_warn("{id}: deployment position from metadata differs from the tag metadata by {sprintf('%.1f', dkm)} km.")
    }
  }

  # The deploy origin comes from authoritative metadata only. (A former last-resort "first User fix"
  # fallback was dropped: importTagData no longer imports User-type positions - they are deploy/pop-up
  # coordinates that belong in meta$deployment, not tracking fixes - so the fallback could not fire anyway.)
  from_md %||% from_meta
}


#######################################################################################################
# Internal: neighbour-consistency (root) speed filter #################################################
#######################################################################################################

# The speed spike filter of Freitas et al. (2008) (as in argosfilter::sda / aniMotum). `lon`/`lat`/
# `time_num` are the retained fixes in time order; `removable` marks which of them may be removed
# (FastGPS/Argos - never a User anchor). A fix is a spike when the implied speed to BOTH its previous
# and next retained neighbour exceeds `max.speed.kmh` (a one-sided fast segment is genuine travel and is
# kept). The single worst spike is removed, speeds are recomputed against the new neighbours, and the
# process repeats until none remain (or `ctrl$max.iterations`). With `ctrl$spike.angle` set, a sharp
# out-and-back reversal at moderate speed is also treated as a spike. Segments closer than
# `ctrl$min.time.mins` in time are not judged (a sub-threshold gap inflates speed unreliably).
# Returns the indices (into the supplied vectors) to remove.
#' @keywords internal
#' @noRd
.locationSpeedFilter <- function(lon, lat, time_num, removable, max.speed.kmh, ctrl) {

  n <- length(lon)
  removed <- rep(FALSE, n)
  if (n < 2L || !any(removable)) return(integer(0))

  min_dt_h <- (ctrl$min.time.mins %||% 0) / 60
  spike_ang <- ctrl$spike.angle                    # NULL -> angle test off
  max_it <- ctrl$max.iterations %||% 50L

  it <- 0L
  repeat {
    it <- it + 1L
    act <- which(!removed)                          # retained fixes, time order
    m <- length(act)
    if (m < 2L) break

    alon <- lon[act]; alat <- lat[act]; atime <- time_num[act]

    # segment speeds (km/h): element k = act[k] -> act[k+1]; NA when the time gap is too small to judge
    dt_h <- diff(atime) / 3600
    d_km <- geosphere::distGeo(cbind(alon[-m], alat[-m]), cbind(alon[-1], alat[-1])) / 1000
    v <- d_km / dt_h
    v[!is.finite(v) | dt_h < min_dt_h] <- NA_real_

    # optional turning angle at each interior fix (direction reversal), if requested
    turn <- rep(NA_real_, m)
    if (!is.null(spike_ang) && m >= 3L) {
      b_in  <- geosphere::bearing(cbind(alon[-m], alat[-m]), cbind(alon[-1], alat[-1]))   # length m-1
      for (k in 2:(m - 1L)) {
        delta <- ((b_in[k] - b_in[k - 1L] + 180) %% 360) - 180                            # signed turn [-180,180]
        turn[k] <- abs(delta)
      }
    }

    # score each removable fix; a spike gets a positive severity (worst removed first)
    sev <- rep(NA_real_, m)
    for (k in seq_len(m)) {
      if (!removable[act[k]]) next
      v_in  <- if (k > 1L) v[k - 1L] else NA_real_
      v_out <- if (k < m)  v[k]      else NA_real_
      over_in  <- isTRUE(v_in  > max.speed.kmh)
      over_out <- isTRUE(v_out > max.speed.kmh)
      interior <- k > 1L && k < m
      is_spike <- FALSE
      if (interior) {
        # root test: implausible to BOTH neighbours
        if (over_in && over_out) is_spike <- TRUE
        # angle test: a sharp reversal with at least one elevated segment
        if (!is.null(spike_ang) && !is.na(turn[k]) && turn[k] >= spike_ang && (over_in || over_out)) is_spike <- TRUE
      } else {
        # endpoint: a single implausible neighbour (a bad first/last fix)
        if (over_in || over_out) is_spike <- TRUE
      }
      if (is_spike) sev[k] <- max(v_in, v_out, na.rm = TRUE)
    }

    if (!any(is.finite(sev))) break
    worst <- act[which.max(sev)]                    # remove the single most egregious spike
    removed[worst] <- TRUE
    if (it >= max_it) break
  }

  which(removed)
}


#######################################################################################################
# Internal: per-individual diagnostic map #############################################################
#######################################################################################################

# One page per individual whose fixes were touched. Equal-aspect map of every fix coloured by outcome
# (kept, or removed by satellite / distance / speed), the chronological path through the retained fixes,
# the deployment anchor and (when present) the distance-cap ring and pop-up position, a legend
# attributing each removal to its check, an optional coastline (maps/mapdata, if installed) and scale
# bar (prettymapr, if installed). `p` is the payload assembled in filterLocations().
#' @keywords internal
#' @noRd
.plotLocationPanel <- function(p, coast_spec = NULL, coast_fill = TRUE, bm = NULL,
                               basemap.control = NULL, unicode = TRUE) {

  pos <- p$pos; removed <- p$removed; reason <- p$reason
  deploy <- p$deploy

  # palette (coherent with the deployment-filter panel tones)
  col_fast   <- "#2AA7A0"    # kept FastGPS
  col_argos  <- "#5B7FBD"    # kept Argos
  col_user   <- "#7E57C2"    # User anchors (trusted)
  col_path   <- "#B8C4CC"    # chronological path through kept fixes
  # The deployment anchor is a REFERENCE point, not a fix: charcoal keeps it out of every data hue
  # (teal FastGPS ramp, blue Argos, purple User, red removals, orange pop-up) so it reads instantly.
  # The previous green sat right next to the FastGPS teal and disappeared into it.
  col_deploy <- "#111111"    # deployment anchor
  col_popup  <- "#E8A33D"    # pop-up anchor
  # --- 4. FastGPS chronological ramp: same teal identity, lightness carrying time (first light -> last dark)
  fast_ramp  <- grDevices::colorRampPalette(c("#BFE8E4", "#0E5C57"))
  col_rm     <- c(satellite = "#C9A227", distance = "#C25B56", speed = "#B23A3A")

  kept <- !removed
  popup <- p$popup

  # plotting extent from every fix + the anchors (equal aspect; shared helper)
  xs <- c(pos$lon, if (!is.null(deploy)) deploy$lon, if (!is.null(popup)) popup$lon)
  ys <- c(pos$lat, if (!is.null(deploy)) deploy$lat, if (!is.null(popup)) popup$lat)
  ext <- .equalAspectExtent(xs, ys, f = 0.25)
  if (is.null(ext)) { graphics::plot.new(); return(invisible(NULL)) }
  lon_range <- ext$xlim; lat_range <- ext$ylim

  graphics::par(mar = c(4, 4.5, 3.4, 10.5), mgp = c(2.3, 0.7, 0))
  graphics::plot(NA, xlim = lon_range, ylim = lat_range, asp = ext$asp,
                 axes = FALSE, xlab = "", ylab = "", xaxs = "i", yaxs = "i")
  graphics::rect(graphics::par("usr")[1], graphics::par("usr")[3], graphics::par("usr")[2], graphics::par("usr")[4],
                 col = "#EAF1F6", border = NA)

  # raster basemap canvas (satellite / a pre-fetched raster), fetched at this panel's extent; then the
  # coastline (filled under "land", an outline over a raster canvas)
  tile_credit <- NULL
  if (!is.null(bm) && bm$kind %in% c("satellite", "raster")) {
    tile_rast <- if (identical(bm$kind, "satellite")) .fetchTiles(lon_range, lat_range, basemap.control)
                 else bm$raster
    if (!is.null(tile_rast)) {
      .drawTiles(tile_rast)
      tile_credit <- attr(tile_rast, "nautilus.credit", exact = TRUE) %||% bm$credit
    }
  }
  .drawCoastline(lon_range, lat_range, coast_spec, fill = coast_fill)

  graphics::axis(1, at = pretty(lon_range, 5), labels = sprintf("%.2f", pretty(lon_range, 5)), cex.axis = 0.85)
  graphics::axis(2, at = pretty(lat_range, 5), labels = sprintf("%.2f", pretty(lat_range, 5)), las = 1, cex.axis = 0.85)
  graphics::title(xlab = "Longitude", line = 2.2, cex.lab = 0.95); graphics::title(ylab = "Latitude", line = 3.1, cex.lab = 0.95)
  n_rm <- sum(removed)
  graphics::title(main = p$id, line = 1.9, cex.main = 1.1)
  graphics::title(main = sprintf("%d of %d fixes removed", n_rm, p$n_fix), line = 0.8, font.main = 1, cex.main = 0.85)

  # distance-cap ring around the deployment
  if (!is.null(p$max.distance.km) && !is.null(deploy) && requireNamespace("geosphere", quietly = TRUE)) {
    ring <- geosphere::destPoint(c(deploy$lon, deploy$lat), b = seq(0, 360, by = 5), d = p$max.distance.km * 1000)
    graphics::lines(ring[, 1], ring[, 2], col = col_rm[["distance"]], lty = 3, lwd = 1)
  }

  # chronological path through the RETAINED fixes (shows the cleaned trajectory)
  kp <- pos[kept, , drop = FALSE]
  if (nrow(kp) >= 2) graphics::lines(kp$lon, kp$lat, col = col_path, lwd = 1.1)

  # kept fixes, by type
  .pts <- function(sel, ...) if (any(sel)) graphics::points(pos$lon[sel], pos$lat[sel], ...)
  # FastGPS: one colour per fix, ramped over the deployment's own time span, so the eye reads the
  # chronology directly. `pos` is already time-sorted, so rank over the KEPT subset is the time order.
  sel_fast <- kept & pos$type == "FastGPS"
  if (any(sel_fast)) {
    nf <- sum(sel_fast)
    graphics::points(pos$lon[sel_fast], pos$lat[sel_fast], pch = 21,
                     bg = fast_ramp(max(nf, 2L))[seq_len(nf)], col = "white", lwd = 0.4, cex = 1.2)
  }
  .pts(kept & pos$type == "Argos",   pch = 22, bg = col_argos, col = "white", lwd = 0.4, cex = 1.2)
  .pts(kept & pos$type == "User",    pch = 24, bg = col_user,  col = "white", lwd = 0.4, cex = 1.3)

  # removed fixes, coloured by the check that removed them
  for (rr in names(col_rm)) .pts(removed & reason == rr, pch = 4, col = col_rm[[rr]], lwd = 2, cex = 1.3)

  # deployment + pop-up anchors
  if (!is.null(deploy)) graphics::points(deploy$lon, deploy$lat, pch = 23, bg = col_deploy, col = "white", lwd = 0.5, cex = 1.7)
  if (!is.null(popup))  graphics::points(popup$lon,  popup$lat,  pch = 23, bg = col_popup,  col = "white", lwd = 0.5, cex = 1.7)

  # legend (only entries actually present)
  lab <- character(0); pch <- integer(0); pcol <- character(0); pbg <- character(0)
  add <- function(l, pc, co, bg = NA) { lab[[length(lab) + 1L]] <<- l; pch[[length(pch) + 1L]] <<- pc; pcol[[length(pcol) + 1L]] <<- co; pbg[[length(pbg) + 1L]] <<- bg }
  # the FastGPS swatch takes the ramp's mid tone; the strip below the legend carries the time meaning
  if (any(sel_fast)) add(sprintf("FastGPS (%d)", sum(sel_fast)), 21, "white", fast_ramp(3)[2])
  if (any(kept & pos$type == "Argos"))   add(sprintf("Argos (%d)",   sum(kept & pos$type == "Argos")),   22, "white", col_argos)
  if (any(kept & pos$type == "User"))    add(sprintf("User (%d)",    sum(kept & pos$type == "User")),    24, "white", col_user)
  if (p$counts$speed > 0)     add(sprintf("removed: speed (%d)",     p$counts$speed),     4, col_rm[["speed"]])
  if (p$counts$distance > 0)  add(sprintf("removed: distance (%d)",  p$counts$distance),  4, col_rm[["distance"]])
  if (p$counts$satellite > 0) add(sprintf("removed: satellites (%d)", p$counts$satellite), 4, col_rm[["satellite"]])
  if (!is.null(deploy)) add("deployment", 23, "white", col_deploy)
  if (!is.null(popup))  add("pop-up", 23, "white", col_popup)
  # place the legend just outside the plot's right edge (device coords), so it never clips or overlaps data
  usr <- graphics::par("usr")
  lg <- graphics::legend(x = usr[2] + 0.03 * (usr[2] - usr[1]), y = usr[4], legend = lab, pch = pch,
                         col = pcol, pt.bg = pbg, bty = "n", xpd = NA, pt.lwd = 0.5, pt.cex = 1.2,
                         y.intersp = 1.3, cex = 0.72)

  # time key for the FastGPS ramp: without it the shading is decoration rather than information
  if (any(sel_fast)) {
    kx <- lg$rect$left; ky <- lg$rect$top - lg$rect$h - 0.05 * (usr[4] - usr[3])
    kw <- lg$rect$w * 0.72; kh <- 0.020 * (usr[4] - usr[3])
    cols <- fast_ramp(40); xs <- seq(kx, kx + kw, length.out = length(cols) + 1L)
    graphics::text(kx, ky, "FastGPS: time", adj = c(0, -0.45), cex = 0.62, xpd = NA)
    graphics::rect(xs[-length(xs)], ky - kh, xs[-1], ky, col = cols, border = NA, xpd = NA)
    graphics::rect(kx, ky - kh, kx + kw, ky, border = "#5A6672", lwd = 0.4, xpd = NA)
    graphics::text(c(kx, kx + kw), ky - kh, c("first", "last"), adj = c(0, 1.5), cex = 0.58, xpd = NA)
  }

  # scale bar, if prettymapr is available (shared helper)
  .mapScalebar()
  if (!is.null(tile_credit)) .drawAttribution(tile_credit)               # imagery provider credit
  # a complete panel border, drawn LAST so neither the basemap nor an edge fix overprints it (a light
  # grey box behind the darker axis lines read as "axes only")
  graphics::box(col = "#5A6672", lwd = 1)
  invisible(NULL)
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
