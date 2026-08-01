#######################################################################################################
# Screen implausible position fixes (location-channel quality control) ################################
#######################################################################################################

#' Screen implausible satellite position fixes
#'
#' @description
#' Satellite fixes are not all equally believable. A Fastloc-GPS position computed from too few
#' satellites is geometrically weak, an Argos position can decode to the wrong hemisphere, and either
#' can place an animal somewhere it could not possibly have swum to and back from in the time
#' available. Left in place, such a fix anchors a reconstructed track to a point the animal never
#' visited, and drags the surrounding path with it.
#'
#' This function screens the position record for those cases and removes the fixes that fail, leaving
#' the sensor time series untouched. It is the location-channel counterpart of [checkSensorQuality()],
#' so it belongs in the cleaning phase, before any track reconstruction or mapping consumes the fixes.
#'
#' All three checks are opt-in: with the defaults nothing is removed, so choose thresholds that suit
#' your species and tag.
#'
#' @param data A tag object, a list of them, a single table with an `id.col`, or a character vector of
#'   `.rds` paths - the output of [importTagData()] or any later step. Paths are read one deployment at
#'   a time, so a fleet too large for memory can be processed without ever holding it all.
#' @param metadata An optional deployment-metadata table, one row per deployment, supplying the
#'   reference coordinates for the distance check and the diagnostic map. `NULL` (default) uses the
#'   coordinates already stored in each tag's metadata at import. Where both are present they are
#'   cross-checked and a disagreement is warned about.
#' @param id.col Which column identifies the animal (default `"ID"`); also used to match rows in
#'   `metadata`.
#' @param max.speed.kmh The fastest sustained speed, in km/h, you would believe between two fixes.
#'   `NULL` (default) disables the speed check. Set it from your species' plausible sustained travel
#'   speed rather than its burst speed, since the test is applied between fixes that may be hours
#'   apart.
#' @param max.distance.km A gross-error bound, in kilometres, on how far a fix may lie from the
#'   deployment location. `NULL` (default) disables it. Read the Details before enabling: this catches
#'   decoding errors, and is not a movement constraint.
#' @param min.satellites The fewest satellites a Fastloc-GPS fix may be computed from and still be
#'   kept. `NULL` (default) disables the check. Four is the geometric minimum for a position; raising it
#'   trades fixes for confidence.
#' @param control A control object from [filterLocationsControl()] tuning how the speed test is applied
#'   - the minimum time separation, the iteration cap, and the optional direction-reversal test. Pass
#'   `filterLocationsControl(...)` to change it.
#' @param deploy.lon.col,deploy.lat.col Which columns in `metadata` hold the deployment longitude and
#'   latitude. Defaults `"deploy_lon"` and `"deploy_lat"`. Ignored when `metadata` is `NULL`.
#' @param plot Whether to draw the diagnostic map, one page per deployment with removed fixes, to the
#'   active graphics device. Default `FALSE`.
#' @param plot.file Path to a single multi-page PDF for the diagnostic maps. The parent directory must
#'   exist and the name must end in `.pdf`. `NULL` (default) writes no file. Independent of `plot`.
#' @param basemap The background canvas for the diagnostic map: `"land"` (default, a filled coastline
#'   over a flat sea), `"satellite"` for imagery tiles, which is useful for judging coastal fixes,
#'   `"none"` for blank sea, or a pre-fetched raster from [getBasemap()].
#' @param coastline Which vector coastline to draw: `"auto"` (default), `"high"`, `"low"`, `"none"`, or
#'   a custom coastline as an \pkg{sf} object, a lon/lat table, or a file path. See [plotTracks()] for
#'   the full resolution ladder. It is drawn filled under `basemap = "land"` and as an outline over a
#'   raster canvas.
#' @param basemap.control A control object from [basemapControl()] tuning the satellite fetch. Used only
#'   when `basemap = "satellite"`.
#' @param return.data Whether to return the processed data in memory (default `TRUE`). When `FALSE`, the
#'   function instead returns the paths of the `.rds` files it wrote, which feed directly into the next
#'   step's `data` argument - so a large fleet can be processed without ever holding it all in memory.
#'   `return.data = FALSE` therefore requires an `output.dir`.
#' @param output.dir Directory in which to write one `<id>.rds` file per deployment. Providing a
#'   directory is what triggers saving; `NULL` (default) writes nothing. The directory must already
#'   exist.
#' @param output.suffix Optional suffix appended to each saved file name, before `.rds`, to tag a
#'   processing run or avoid overwriting an earlier one. Only used when `output.dir` is set.
#' @param compress Compression for the saved `.rds` files: `TRUE` (default, gzip), `FALSE`, or one of
#'   `"gzip"`, `"bzip2"` or `"xz"`. Only used when `output.dir` is set. See [base::saveRDS()].
#' @param verbose How much detail to print: `0`/`"quiet"`, `1`/`"normal"`, or `2`/`"detailed"`
#'   (default), which adds per-check diagnostics.
#'
#' @details
#' The fixes live in the deployment's position record, created by [importTagData()], and this function
#' reads that record directly, so it can run at any point after import.
#'
#' ## The three checks
#'
#' They are applied in this order, and only to automatically acquired fixes.
#'
#' 1. **Satellite count** (`min.satellites`). A Fastloc-GPS fix computed from too few satellites is
#'    geometrically weak, so fixes below the threshold are removed. Argos fixes carry no satellite count
#'    and are exempt.
#' 2. **Distance from the deployment** (`max.distance.km`). A sanity bound for gross errors: a fix that
#'    decoded to an impossible location sits absurdly far from the release site. It is off by default,
#'    because it is anchored to the deployment and blind to elapsed time, so used as a movement
#'    constraint it will clip the genuine displacement of a wide-ranging animal. Set it, if at all, well
#'    beyond the animal's plausible range. The deployment and pop-up positions are never removed by it.
#' 3. **Speed** (`max.speed.kmh`). The principled check, described below.
#'
#' ## Why the speed check tests both neighbours
#'
#' Flagging every over-threshold step does not work, because a single fast segment implicates two fixes
#' and cannot say which of them is wrong. This function instead uses the neighbour-consistency, or
#' "root", test of Freitas et al. (2008), as implemented in `argosfilter::sda` and \pkg{aniMotum}: a fix
#' is implausible only when the implied speed to *both* its previous and its next retained fix exceeds
#' the threshold - that is, when it is an isolated spike the track jumps out to and back from. A
#' genuinely fast segment in the middle of a track, fast to one neighbour and normal to the other, is
#' kept, because it cannot be attributed to either fix.
#'
#' The first and last retained fix are the exception, and necessarily so: an endpoint has only one
#' neighbour, so there is nothing for the rule to compare against. There a single implausible segment
#' is enough to remove the fix, which is what catches a bad first or last position.
#'
#' The most egregious spike is removed, the speeds are recomputed against the new neighbours, and the
#' process repeats until every remaining fix is plausible or `control$max.iterations` is reached. The
#' optional direction-reversal test, `control$spike.angle`, additionally catches sharp out-and-back
#' spikes that travel at moderate speed.
#'
#' ## What is never removed
#'
#' Only `"FastGPS"` and `"Argos"` fixes - the automatically acquired positions that can be spurious -
#' are ever removed. `"User"` positions, curated by hand in the tag manufacturer's data portal, are
#' trusted and kept, though they still act as fixed anchors in the speed test. The deployment and pop-up
#' positions are reference points and are never touched.
#'
#' Removed fixes are dropped from the position record, and the counts and thresholds are written to the
#' processing history, readable with [processingHistory()]. The diagnostic maps show every fix coloured
#' by outcome - kept, or removed and by which check - together with the chronological path through the
#' retained fixes and the deployment anchors.
#'
#' @return If `return.data = TRUE`, a named list of tag objects with the implausible fixes removed from
#'   their position records. If `return.data = FALSE`, a character vector of the written `.rds` file
#'   paths. The diagnostic maps are a side effect of either.
#'
#' @references
#' Freitas C, Lydersen C, Fedak MA, Kovacs KM (2008) A simple new algorithm to filter marine mammal
#' Argos locations. *Marine Mammal Science* 24:315-325. \doi{10.1111/j.1748-7692.2007.00180.x}
#'
#' @seealso [importTagData()] for the step that creates the position record; [checkSensorQuality()] for
#'   the sensor-channel counterpart; [filterLocationsControl()] for tuning the speed test;
#'   [reconstructTrack()] and [crossValidateTrack()] for what consumes the cleaned fixes.
#'
#' @examples
#' \dontrun{
#' imported <- importTagData(folders, metadata = meta)
#'
#' # drop Fastloc fixes implying more than 8 km/h to both neighbours, or from fewer than 4 satellites
#' cleaned <- filterLocations(imported,
#'                            max.speed.kmh  = 8,
#'                            min.satellites = 4,
#'                            plot           = TRUE)
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

    # resolve the reference deployment position (metadata -> meta$deployment -> first User fix)
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
