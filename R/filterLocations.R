#######################################################################################################
# Screen implausible position fixes (location-channel quality control) ################################
#######################################################################################################

#' Screen implausible GPS/Argos position fixes
#'
#' @description
#' Quality-controls the location channel of a tag record: it screens the Wildlife Computers position
#' fixes (Fastloc-GPS and Argos) for implausible detections and removes them from the deployment's
#' canonical position record, leaving the sensor time series untouched. This is a data-cleaning step -
#' the location analogue of \code{\link{checkSensorQuality}} for the sensor channels - so it belongs in
#' the clean / quality-control phase, before any track reconstruction (\code{\link{reconstructTrack}},
#' \code{\link{crossValidateTrack}}) or mapping consumes the fixes.
#'
#' @details
#' The fixes live in the deployment's canonical position record (\code{meta$ancillary$positions}, created
#' by \code{\link{importTagData}}); this function reads that record directly, so it can run at any point
#' after import. Three independent, opt-in checks are applied, in order, to the automatically-acquired
#' fixes only:
#' \enumerate{
#'   \item \strong{Satellite count} (\code{min.satellites}). Fastloc-GPS fixes computed from too few
#'     satellites are geometrically weak. Fixes whose satellite count (the WC \code{Quality} field) is
#'     below the threshold are removed. Argos fixes carry no satellite count and are exempt.
#'   \item \strong{Distance from deployment} (\code{max.distance.km}). A gross-error sanity bound: a fix
#'     that decoded to an impossible location (e.g. the wrong hemisphere) sits absurdly far from the
#'     release site. \strong{Off by default}, because - anchored to the deployment and blind to elapsed
#'     time - it will clip the genuine displacement of a wide-ranging animal if set as a movement
#'     constraint. Use it only as a loose bound for decoding errors, well beyond the animal's plausible
#'     range. The deploy and pop-up positions are never removed by it.
#'   \item \strong{Speed} (\code{max.speed.kmh}). The principled check. Rather than flagging every
#'     over-threshold step (which cannot tell which of the two fixes is bad), it uses the
#'     \strong{neighbour-consistency ("root") test} of Freitas et al. (2008), as implemented in
#'     \code{argosfilter::sda} and \code{aniMotum}: a fix is implausible only when the implied speed to
#'     \emph{both} its previous and next retained fix exceeds \code{max.speed.kmh}, i.e. it is an isolated
#'     spike that the track jumps out to and back from. A single genuinely fast segment (fast to one
#'     neighbour, normal to the other) is kept, since it cannot be attributed to either fix. The most
#'     egregious spike is removed, the speeds are recomputed against the new neighbours, and the process
#'     repeats until every remaining fix is plausible (or \code{control$max.iterations} is reached). An
#'     optional direction-reversal test (\code{control$spike.angle}) additionally catches sharp
#'     out-and-back spikes at moderate speed.
#' }
#'
#' \strong{What is (and is not) filtered.} Only \code{"FastGPS"} and \code{"Argos"} fixes - the
#' automatically-acquired positions that can be spurious - are ever removed. \code{"User"} positions
#' (manually curated in the WC Data Portal) are trusted and kept; they still take part in the speed test
#' as fixed anchors. The deploy and pop-up positions (canonical in \code{meta$deployment}) are reference
#' points and are never touched. All three checks are opt-in: with the defaults nothing is removed, so
#' choose thresholds that suit your species and tag.
#'
#' Removed fixes are dropped from \code{meta$ancillary$positions}; the counts and thresholds are recorded
#' in the processing history (\code{\link{processingHistory}}). Diagnostic maps (\code{plot} /
#' \code{plot.file}) show every fix, coloured by outcome (kept, or removed and by which check), the
#' chronological path through the retained fixes, and the deployment anchors, for visual review.
#'
#' @param data A `nautilus_tag`/data.frame, a (named) list of them, or a character vector of `.rds`
#'   file paths - the output of \code{\link{importTagData}} (or any later step). A single aggregated
#'   data.frame is split by `id.col`. File paths are loaded one at a time to save memory.
#' @param id.metadata Optional deployment-metadata table (one row per deployment) supplying the reference
#'   deployment coordinates for the distance check and the diagnostic map. If `NULL` (default), the
#'   coordinates stored in each tag's metadata (`meta$deployment`, populated at import) are used. When
#'   both are present they are cross-checked and a disagreement is warned about.
#' @param id.col Character. Name of the ID column, used to split a single data.frame and to match rows in
#'   `id.metadata`. Default "ID".
#' @param datetime.col Character. Name of the POSIXct datetime column of the sensor data (used only for
#'   labelling; the position fixes carry their own timestamps). Default "datetime".
#' @param max.speed.kmh Numeric. Maximum plausible sustained speed (km/h) between consecutive fixes for
#'   the neighbour-consistency speed test. `NULL` (default) disables the speed check.
#' @param max.distance.km Numeric. Gross-error distance bound (km) from the deployment location; fixes
#'   farther than this are removed. `NULL` (default) disables the distance check. See Details before
#'   enabling - this is a decoding-error bound, not a movement constraint.
#' @param min.satellites Integer. Minimum satellite count for a Fastloc-GPS fix to be retained. `NULL`
#'   (default) disables the satellite check.
#' @param control A \code{\link{filterLocationsControl}} object (or a named list of its fields) tuning the
#'   speed test (minimum time separation, iteration cap, optional direction-reversal spike test). `NULL`
#'   (default) uses the defaults.
#' @param deploy.lon.col,deploy.lat.col Character. Names of the deployment longitude/latitude columns in
#'   `id.metadata`. Defaults "deploy_lon" / "deploy_lat". Ignored when `id.metadata` is `NULL`.
#' @param plot Logical. Draw the diagnostic map (one page per individual with removed fixes) to the active
#'   graphics device. Default `FALSE`.
#' @param plot.file Character. Path to a single multi-page PDF for the diagnostic maps. The parent
#'   directory must already exist; must end in `.pdf`. `NULL` (default) writes no file. Independent of
#'   `plot`.
#' @param return.data Logical. Return the processed data in memory (default `TRUE`). When `FALSE`, the
#'   function instead returns the paths of the `.rds` files it wrote, which feed directly into the next
#'   step's `data` argument -- so a large fleet can be processed without ever holding it all in memory.
#'   `return.data = FALSE` therefore requires an `output.dir`.
#' @param output.dir Character. Directory in which to write one `<id>.rds` file per deployment. Providing
#'   a directory is what triggers saving; `NULL` (default) writes nothing. The directory must already exist.
#' @param output.suffix Character. Optional suffix appended to each saved file name (before `.rds`), e.g.
#'   to tag a processing run or avoid clashes. Only used when `output.dir` is set. Default `NULL`.
#' @param compress Compression for the saved `.rds` files (only used when `output.dir` is set): `TRUE`
#'   (default, gzip), `FALSE`, or one of `"gzip"`/`"bzip2"`/`"xz"`. See \code{\link[base]{saveRDS}}.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet" (warnings and errors only), `TRUE`/`1`/"normal" (header,
#'   one line per individual, summary), or `2`/"detailed" (default; adds per-check diagnostics).
#'
#' @return If `return.data = TRUE`, a named list of filtered `nautilus_tag` objects (one per individual),
#'   with implausible fixes removed from `meta$ancillary$positions`; if `return.data = FALSE`, a character
#'   vector of the written `.rds` file paths. Diagnostic maps are a side effect.
#'
#' @references
#' Freitas C, Lydersen C, Fedak MA, Kovacs KM (2008) A simple new algorithm to filter marine mammal Argos
#' locations. \emph{Marine Mammal Science}. 24:315-325. \doi{10.1111/j.1748-7692.2007.00180.x}
#'
#' @seealso \code{\link{importTagData}}, \code{\link{checkSensorQuality}}, \code{\link{filterLocationsControl}},
#'   \code{\link{reconstructTrack}}, \code{\link{crossValidateTrack}}.
#' @examples
#' \dontrun{
#' imported <- importTagData(folders, id.metadata = meta)
#' # Location QC: drop Fastloc fixes implying > 8 km/h to both neighbours or from < 4 satellites
#' cleaned <- filterLocations(imported,
#'                            max.speed.kmh  = 8,
#'                            min.satellites = 4,
#'                            plot           = TRUE)
#' }
#' @export


filterLocations <- function(data,
                            id.metadata = NULL,
                            id.col = "ID",
                            datetime.col = "datetime",
                            max.speed.kmh = NULL,
                            max.distance.km = NULL,
                            min.satellites = NULL,
                            control = NULL,
                            deploy.lon.col = "deploy_lon",
                            deploy.lat.col = "deploy_lat",
                            plot = FALSE,
                            plot.file = NULL,
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
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  .assert_string(deploy.lon.col, "deploy.lon.col"); .assert_string(deploy.lat.col, "deploy.lat.col")
  .assert_number(max.speed.kmh, "max.speed.kmh", min = 0, null_ok = TRUE)
  .assert_number(max.distance.km, "max.distance.km", min = 0, null_ok = TRUE)
  .assert_count(min.satellites, "min.satellites", min = 1L, null_ok = TRUE)
  .assert_writable_file(plot.file, "plot.file", ext = "pdf")   # fail-fast: parent dir must exist
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

  # validate id.metadata if supplied (deployment coordinates for the distance check / map anchor)
  if (!is.null(id.metadata)) {
    .assert_columns(id.metadata, c(id.col, deploy.lon.col, deploy.lat.col), "id.metadata")
    id.metadata <- as.data.frame(id.metadata)
  }

  make_plots <- plot || !is.null(plot.file)

  # resolve the input into a uniform iterable (list / single df / .rds paths); guards empty input
  r <- .resolveInput(data, id.col = id.col)

  ##############################################################################
  # Header #####################################################################
  ##############################################################################

  active <- c(if (do_speed) sprintf("speed <= %g km/h", max.speed.kmh),
              if (do_dist)  sprintf("<= %g km from deployment", max.distance.km),
              if (do_sat)   sprintf(">= %d satellites", min.satellites))
  method_hdr <- if (length(active)) paste(active, collapse = paste0(" ", cli::symbol$bullet, " ")) else "no checks enabled"
  hdr_bullets <- sprintf("Input: %d dataset%s", r$n, if (r$n != 1) "s" else "")
  if (!is.null(output.dir)) hdr_bullets <- c(hdr_bullets, paste0("Output: ", output.dir))
  .log_header(lvl, "filterLocations", "Screening position fixes for implausible detections",
              bullets = hdr_bullets, arrow = paste0("Checks: ", method_hdr))

  # nudge if the function would be a no-op (nothing enabled) - a QC step that removes nothing is
  # almost always an oversight (thresholds are species-specific, so there is no safe default)
  if (!length(active)) {
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

  for (i in seq_len(r$n)) {

    # load / access the individual (metadata ensured / migrated by .resolveInput)
    x  <- r$get(i)
    id <- r$ids[i]
    .log_h2(lvl, sprintf("%s (%d/%d)", id, i, r$n))

    meta <- .getMeta(x)
    pos  <- .tagPositions(x)                          # canonical record: datetime,type,lon,lat,quality

    # nothing to screen: no position fixes for this deployment
    if (!nrow(pos)) {
      .log_skip(lvl, id, "  no position fixes ", cli::symbol$bullet, " skipped"); .log_gap(lvl)
      if (make_plots) payloads[[i]] <- NULL
      if (return.data) { results[[i]] <- x }
      next
    }

    # order fixes by time and pre-compute numeric time
    pos <- pos[order(pos$datetime), , drop = FALSE]
    pos$time_num <- as.numeric(pos$datetime)
    n_fix <- nrow(pos)

    # only the automatically-acquired fixes may be removed; User fixes are trusted anchors
    removable <- pos$type %in% c("FastGPS", "Argos") & !is.na(pos$lon) & !is.na(pos$lat)

    # resolve the reference deployment position (id.metadata -> meta$deployment -> first User fix)
    deploy <- .resolveDeployPosition(meta, id.metadata, id, id.col, deploy.lon.col, deploy.lat.col, pos, lvl)

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
    if (do_sat)   .log_detail(lvl, "satellites: ", counts$satellite, " removed (< ", min.satellites, ")")
    if (do_dist)  .log_detail(lvl, "distance: ",  counts$distance,  " removed (> ", max.distance.km, " km)")
    if (do_speed) .log_detail(lvl, "speed: ",     counts$speed,     " removed (> ", max.speed.kmh, " km/h to both neighbours)")

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
      .log_skip(lvl, id, "  ", n_rm, " of ", n_fix, " fix", if (n_fix != 1) "es", " removed",
                if (!is.null(saved_to)) paste0(" ", cli::symbol$bullet, " saved ", basename(saved_to)))
    } else {
      .log_ok(lvl, id, "  ", n_fix, " fix", if (n_fix != 1) "es", " retained",
              if (!is.null(saved_to)) paste0(" ", cli::symbol$bullet, " saved ", basename(saved_to)))
    }
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
        for (p in to_draw) .plotLocationPanel(p, unicode = unicode)
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
    .log_done(lvl, total_removed, " fix", if (total_removed != 1) "es", " removed across ",
              n_touched, " dataset", if (n_touched != 1) "s")
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
# order: an explicit `id.metadata` row (deploy.lon.col / deploy.lat.col), then the tag's own metadata
# (meta$deployment, populated at import), then the first "User" fix. When both id.metadata and
# meta$deployment are present they are cross-checked and a disagreement over 1 km is warned about.
# Returns list(lon, lat, source) or NULL when no reference is available.
#' @keywords internal
#' @noRd
.resolveDeployPosition <- function(meta, id.metadata, id, id.col, deploy.lon.col, deploy.lat.col, pos, lvl) {

  from_meta <- NULL
  dep <- meta$deployment
  if (!is.null(dep) && !is.null(dep$lon) && !is.null(dep$lat) && !is.na(dep$lon) && !is.na(dep$lat)) {
    from_meta <- list(lon = dep$lon, lat = dep$lat, source = "meta$deployment")
  }

  from_md <- NULL
  if (!is.null(id.metadata)) {
    row <- id.metadata[as.character(id.metadata[[id.col]]) == as.character(id), , drop = FALSE]
    if (nrow(row) > 0) {
      dl <- .asNumericSafe(row[[deploy.lon.col]][1]); da <- .asNumericSafe(row[[deploy.lat.col]][1])
      if (!is.na(dl) && !is.na(da)) from_md <- list(lon = dl, lat = da, source = "id.metadata")
    }
  }

  # cross-check the two independent sources
  if (!is.null(from_md) && !is.null(from_meta) && requireNamespace("geosphere", quietly = TRUE)) {
    dkm <- geosphere::distGeo(c(from_md$lon, from_md$lat), c(from_meta$lon, from_meta$lat)) / 1000
    if (is.finite(dkm) && dkm > 1) {
      cli::cli_warn("{id}: deployment position from id.metadata differs from the tag metadata by {sprintf('%.1f', dkm)} km.")
    }
  }

  # first User fix, as a last resort
  from_user <- NULL
  uidx <- which(pos$type == "User" & !is.na(pos$lon) & !is.na(pos$lat))
  if (length(uidx)) from_user <- list(lon = pos$lon[uidx[1]], lat = pos$lat[uidx[1]], source = "first User fix")

  from_md %||% from_meta %||% from_user
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
.plotLocationPanel <- function(p, unicode = TRUE) {

  pos <- p$pos; removed <- p$removed; reason <- p$reason
  deploy <- p$deploy

  # palette (coherent with the deployment-filter panel tones)
  col_fast   <- "#2AA7A0"    # kept FastGPS
  col_argos  <- "#5B7FBD"    # kept Argos
  col_user   <- "#7E57C2"    # User anchors (trusted)
  col_path   <- "#B8C4CC"    # chronological path through kept fixes
  col_deploy <- "#1D9E75"    # deployment anchor
  col_popup  <- "#E8A33D"    # pop-up anchor
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

  # optional coastline (skip silently if the map packages are absent)
  .drawCoastline(lon_range, lat_range)

  graphics::box(col = "#BBBBBB")
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
  .pts(kept & pos$type == "FastGPS", pch = 21, bg = col_fast,  col = "white", lwd = 0.4, cex = 1.2)
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
  if (any(kept & pos$type == "FastGPS")) add(sprintf("FastGPS (%d)", sum(kept & pos$type == "FastGPS")), 21, "white", col_fast)
  if (any(kept & pos$type == "Argos"))   add(sprintf("Argos (%d)",   sum(kept & pos$type == "Argos")),   22, "white", col_argos)
  if (any(kept & pos$type == "User"))    add(sprintf("User (%d)",    sum(kept & pos$type == "User")),    24, "white", col_user)
  if (p$counts$speed > 0)     add(sprintf("removed: speed (%d)",     p$counts$speed),     4, col_rm[["speed"]])
  if (p$counts$distance > 0)  add(sprintf("removed: distance (%d)",  p$counts$distance),  4, col_rm[["distance"]])
  if (p$counts$satellite > 0) add(sprintf("removed: satellites (%d)", p$counts$satellite), 4, col_rm[["satellite"]])
  if (!is.null(deploy)) add("deployment", 23, "white", col_deploy)
  if (!is.null(popup))  add("pop-up", 23, "white", col_popup)
  # place the legend just outside the plot's right edge (device coords), so it never clips or overlaps data
  usr <- graphics::par("usr")
  graphics::legend(x = usr[2] + 0.03 * (usr[2] - usr[1]), y = usr[4], legend = lab, pch = pch,
                   col = pcol, pt.bg = pbg, bty = "n", xpd = NA, pt.lwd = 0.5, pt.cex = 1.2,
                   y.intersp = 1.3, cex = 0.72)

  # scale bar, if prettymapr is available (shared helper)
  .mapScalebar()
  invisible(NULL)
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
