# Location filtering, track reconstruction, metrics and diagnostics.

# crossValidateTrack helpers ----

#' Leave-one-out cross-validation for a single deployment: withhold each genuine fix (and the pop-up) in
#' turn, reconstruct from the rest, and score the reconstructed position against the withheld fix.
#' @keywords internal
#' @noRd
.crossValidateOne <- function(dt, control, datetime.col, id) {
  dtp <- .withPositionColumns(data.table::as.data.table(dt))
  meta <- .getMeta(dtp)
  deploy_lat <- meta$deployment$lat; deploy_lon <- meta$deployment$lon
  if (is.null(deploy_lat) || is.null(deploy_lon) || is.na(deploy_lat) || is.na(deploy_lon)) return(NULL)
  if (!all(c(datetime.col, "heading", "pitch") %in% names(dtp))) return(NULL)

  pos_anchors <- .positionAnchors(dtp, meta, datetime.col, deploy_lat, deploy_lon)
  holdable <- pos_anchors[quality != "Deploy"]          # pop-up + genuine GPS/Argos fixes (never the origin)
  if (!nrow(holdable)) return(NULL)
  atime <- as.numeric(pos_anchors$time)

  rows <- vector("list", nrow(holdable))
  for (k in seq_len(nrow(holdable))) {
    h <- holdable$idx[k]
    res <- tryCatch(.reconstructTrackOne(data.table::copy(dtp), control, datetime.col, lvl = 0L, id = id,
                                         make_plots = FALSE, holdout = h),
                    error = function(e) NULL)
    if (is.null(res)) next
    pred_lon <- res$pseudo_lon[h]; pred_lat <- res$pseudo_lat[h]
    if (!is.finite(pred_lon) || !is.finite(pred_lat)) next
    err_m <- .trackDistance(pred_lon, pred_lat, holdable$lon[k], holdable$lat[k]) * 1000
    h_t <- as.numeric(holdable$time[k]); other_t <- atime[pos_anchors$idx != h]
    gap_h <- if (length(other_t)) min(abs(other_t - h_t)) / 3600 else NA_real_
    q <- as.character(holdable$quality[k])
    rows[[k]] <- data.frame(
      id = id, datetime = holdable$time[k], quality = q, error_m = err_m, gap_h = gap_h,
      interpolated = any(other_t < h_t) && any(other_t > h_t),
      fix_radius_m = .anchorRadius(q, control$anchor.error.radii),
      n_anchors_used = nrow(pos_anchors) - 1L,
      speed_method = control$speed.method, vpc_method = control$vpc.method, stringsAsFactors = FALSE)
  }
  data.table::rbindlist(Filter(Negate(is.null), rows))
}

#' Diagnostic page: held-out error vs the reckoning gap (with an empirical drift slope), plus the error
#' pooled empirical cumulative distribution of held-out errors.
#' @keywords internal
#' @noRd
.drawCrossValidation <- function(res, control) {
  oldpar <- graphics::par(no.readonly = TRUE); on.exit({ graphics::layout(1); graphics::par(oldpar) }, add = TRUE)
  IN <- "#1565c0"; EX <- "#e08a00"
  graphics::layout(matrix(c(1, 1, 2, 3), nrow = 2L, byrow = TRUE), heights = c(0.32, 1))

  gp <- res$gap_h > 0 & is.finite(res$gap_h)
  drift <- if (any(gp)) stats::median(res$error_m[gp] / res$gap_h[gp], na.rm = TRUE) else NA_real_
  graphics::par(mar = c(0.2, 1, 0.4, 1)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
  graphics::text(0, 0.72, "crossValidateTrack  -  held-out fix accuracy", adj = c(0, 0.5), font = 2, cex = 1.4)
  graphics::text(0, 0.24, sprintf("%d fixes / %d deployments   |   median %.0f m, 90th pct %.0f m   |   drift ~ %.0f m/h (%.2f m/s)   |   %s / %s",
                 nrow(res), length(unique(res$id)), stats::median(res$error_m),
                 stats::quantile(res$error_m, 0.9, names = FALSE), drift, drift / 3600,
                 control$speed.method, control$vpc.method),
                 adj = c(0, 0.5), cex = 0.9, col = "grey30")

  # --- error vs gap (the drift diagnostic) ---
  graphics::par(mar = c(3.7, 3.9, 1.8, 1), mgp = c(2.4, 0.7, 0), cex.axis = 0.85)
  cols <- ifelse(res$interpolated, IN, EX)
  graphics::plot(res$gap_h, res$error_m, pch = 19, cex = 0.9, col = grDevices::adjustcolor(cols, 0.7),
                 xlab = "gap to nearest retained fix (h)", ylab = "held-out error (m)",
                 main = "error vs reckoning gap", font.main = 1, cex.main = 1.0)
  if (sum(gp) >= 2L) {                                     # drift-through-origin slope (m per hour)
    sl <- stats::coef(stats::lm(error_m ~ 0 + gap_h, data = res[gp, , drop = FALSE]))[[1]]
    graphics::abline(0, sl, col = "grey45", lty = 2)
    graphics::mtext(sprintf("slope ~ %.0f m/h", sl), side = 3, line = -1.1, adj = 0.98, cex = 0.7, col = "grey45")
  }
  graphics::legend("topleft", bty = "n", cex = 0.75, pch = 19, col = c(IN, EX),
                   legend = c("interpolated", "extrapolated"))

  # --- error distribution (ECDF) ---
  graphics::par(mar = c(3.7, 3.9, 1.8, 1))
  e <- sort(res$error_m[is.finite(res$error_m)])
  if (length(e)) {
    graphics::plot(e, seq_along(e) / length(e), type = "s", col = IN, lwd = 1.8,
                   xlab = "held-out error (m)", ylab = "cumulative fraction", main = "error distribution",
                   font.main = 1, cex.main = 1.0, ylim = c(0, 1))
    graphics::abline(v = stats::median(e), col = "grey45", lty = 3)
    graphics::mtext(sprintf("median %.0f m", stats::median(e)), side = 3, line = -1.1, adj = 0.98, cex = 0.7, col = "grey45")
  } else { graphics::plot.new(); graphics::text(0.5, 0.5, "no finite errors", col = "grey50") }
  invisible(NULL)
}

# filterLocations helpers ----

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

# reconstructTrack helpers ----

#' Reconstruct one deployment's pseudo-track (dead reckoning + VPC). Returns the annotated data.table with
#' an `n_anchors` attribute, or NULL (via a thrown, caught message) when it cannot be built.
#' @keywords internal
#' @noRd
.reconstructTrackOne <- function(dt, control, datetime.col, lvl, id = NA_character_, make_plots = FALSE,
                                 holdout = integer(0)) {
  dt <- .withPositionColumns(dt)                             # reconstruct lon/lat fix columns from ancillary
  meta <- .getMeta(dt)

  req <- c(datetime.col, "heading", "pitch")
  if (control$speed.method == "paddle")     req <- c(req, "paddle_speed")
  if (control$speed.method == "depth_rate") req <- c(req, "vertical_velocity")
  if (control$speed.method == "vedba")      req <- c(req, "vedba")
  missing_cols <- setdiff(req, names(dt))
  if (length(missing_cols)) {
    # paddle_speed is produced by calculatePaddleSpeed(), not by processTagData(), so its absence is
    # usually a missing pipeline step rather than a tag without a paddle wheel
    hint <- if ("paddle_speed" %in% missing_cols) " - run calculatePaddleSpeed() first" else ""
    stop("missing column(s): ", paste(missing_cols, collapse = ", "), hint)
  }
  if (sum(is.finite(dt$heading) & is.finite(dt$pitch)) < 2L)
    stop("no usable heading and pitch; track reconstruction is unavailable")

  deploy_lat <- meta$deployment$lat; deploy_lon <- meta$deployment$lon
  if (is.null(deploy_lat) || is.null(deploy_lon) || is.na(deploy_lat) || is.na(deploy_lon))
    stop("no deployment coordinates")

  # per-step elapsed seconds: shared by the speed models (VeDBA straight-segment calibration) and the
  # displacement integration below. Non-positive/undefined gaps take the median positive step.
  dt_sec <- c(0, as.numeric(diff(dt[[datetime.col]]), units = "secs"))
  dt_sec[!is.finite(dt_sec) | dt_sec < 0] <- stats::median(dt_sec[dt_sec > 0], na.rm = TRUE)

  # known positions (deployment origin, pop-up, GPS/Argos fixes): one source of truth reused by the VeDBA
  # speed auto-calibration and the VPC correction further below
  pos_anchors <- .positionAnchors(dt, meta, datetime.col, deploy_lat, deploy_lon)
  # cross-validation hook: drop the held-out fix row(s) from the anchor set (never the deployment origin), so
  # neither the VPC nor the VeDBA speed fit sees them - the reconstructed position at that row is then a
  # genuine out-of-sample prediction to score against the withheld fix (see crossValidateTrack).
  if (length(holdout)) pos_anchors <- pos_anchors[!(idx %in% holdout & quality != "Deploy")]

  # 1. instantaneous swimming speed (m/s) - stored as the along-body speed; the horizontal component is
  # recovered as speed_dr * cos(pitch) at the integration step, uniformly across methods.
  speed_fit <- NULL
  if (control$speed.method == "paddle") {
    # A paddle channel often runs slower than the inertial grid (CEiiA logs it at 1 Hz against 20 Hz), and
    # regularizeTimeSeries deliberately leaves those rows empty rather than inventing samples that a pooled
    # statistic would then double-count. Dead reckoning DOES need a speed at every integration step, so the
    # continuous signal is reconstructed here, at the consumer that needs it: interpolate within the
    # channel's own gaps. Without this the generic fallback below would silently replace ~95% of a 1 Hz
    # paddle record with the constant speed, quietly gutting the reconstruction.
    sp <- dt$paddle_speed
    if (anyNA(sp) && sum(is.finite(sp)) >= 2L) sp <- zoo::na.approx(sp, na.rm = FALSE)
    dt[, speed_dr := sp]
  } else if (control$speed.method == "depth_rate") {
    # vertical -> along-body speed via the dive geometry (vertical_velocity = speed * sin(pitch)), so the
    # horizontal component recovered downstream is vertical_velocity / tan(pitch). Trustworthy only on steep
    # glides: near horizontal 1/sin(pitch) explodes and a small pitch error yields a wild speed, so samples
    # shallower than depth.rate.min.pitch are set NA and back-filled with the constant fallback rather than
    # fabricating a huge speed (Wensveen et al. 2015).
    steep <- is.finite(dt$pitch) & abs(dt$pitch) >= control$depth.rate.min.pitch
    sp <- abs(dt$vertical_velocity) / sin(abs(dt$pitch) * (pi / 180))
    sp[!steep] <- NA_real_
    dt[, speed_dr := sp]
  } else if (control$speed.method == "vedba") {
    speed_fit <- .vedbaSpeedModel(dt, pos_anchors, datetime.col, dt_sec, control)
    if (isTRUE(speed_fit$fallback)) {
      dt[, speed_dr := control$constant.speed]
      .log_skip(lvl, sprintf("VeDBA speed calibration unavailable (%s); using constant %.2f m/s",
                             speed_fit$reason, control$constant.speed))
    } else {
      dt[, speed_dr := speed_fit$a + speed_fit$b * vedba]
      .log_detail(lvl, sprintf("VeDBA speed = %.3f + %.3f x VeDBA  (%s%s)", speed_fit$a, speed_fit$b,
        speed_fit$meta$source,
        if (is.finite(speed_fit$meta$r2)) sprintf(", %d segments, R2 %.2f", speed_fit$meta$n_seg, speed_fit$meta$r2) else ""))
    }
  } else {
    dt[, speed_dr := control$constant.speed]
  }

  # optional activity/rest gating: hold speed at 0 while the animal is at rest (VeDBA in its low tail), so
  # reckoning drift does not accumulate during inactivity (Gunner et al. 2021)
  if (!is.null(control$rest.quantile)) {
    if ("vedba" %in% names(dt)) {
      thr <- stats::quantile(dt$vedba, control$rest.quantile, na.rm = TRUE)
      if (is.finite(thr)) dt[is.finite(vedba) & vedba < thr, speed_dr := 0]
    } else {
      .log_skip(lvl, "rest.quantile set but no VeDBA channel; speeds left ungated")
    }
  }

  dt[!is.finite(speed_dr), speed_dr := control$constant.speed]
  dt[speed_dr > control$max.speed, speed_dr := control$max.speed]
  dt[speed_dr < 0, speed_dr := 0]

  R_earth <- 6371000

  # 2. integrate horizontal velocity into the raw dead-reckoned track. The spherical (small-angle
  # destination-point) integration advances latitude along the meridian and longitude scaled by the RUNNING
  # latitude, so it stays accurate across latitude excursions (unlike a fixed-parallel flat earth). The raw
  # lat/lon track is then projected into a local per-point-latitude equirectangular metre plane about the
  # deployment, in which the VPC operates; the correction is projected back to lon/lat at the end.
  heading_rad <- dt$heading * (pi / 180); pitch_rad <- dt$pitch * (pi / 180)
  v_horiz <- dt$speed_dr * cos(pitch_rad)
  d_e <- v_horiz * sin(heading_rad) * dt_sec; d_e[!is.finite(d_e)] <- 0
  d_n <- v_horiz * cos(heading_rad) * dt_sec; d_n[!is.finite(d_n)] <- 0
  raw <- .deadReckonSpherical(d_e, d_n, deploy_lon, deploy_lat, R_earth)
  lp  <- .projLocal(raw$lon, raw$lat, deploy_lon, deploy_lat, R_earth)
  dt[, `:=`(disp_e = lp$e, disp_n = lp$n)]

  # reconstructability gate: for a track with NO interior fixes (anchored only by deploy [+ pop-up]), warn
  # when its directedness - net deploy->pop-up displacement over the reckoned path length - is too low, i.e.
  # the animal's net progress was a small fraction of how far it swam, so the endpoints cannot constrain the
  # wandering interior. A soft, tunable heuristic (validated vs held-out error); it never aborts.
  # skip the gate when fixes are being held out (crossValidateTrack) - the reduced anchor set is artificial,
  # so its endpoint-bounded assessment would be spurious
  recon <- if (!length(holdout)) .reconstructabilityGate(dt, pos_anchors, deploy_lon, deploy_lat, control, id, lvl) else NULL
  if (!is.null(recon)) { if (is.null(meta$sensors)) meta$sensors <- list(); meta$sensors$reconstructability <- recon }

  n_anchors <- 0L; anchors <- NULL

  if (control$vpc.method != "none" && !is.null(pos_anchors) && nrow(pos_anchors) >= 1L) {
    # attach the reckoned displacement at each known position and its true east/north (projected with the
    # same local map) - the residual between the two is the drift the VPC redistributes
    anchors <- data.table::copy(pos_anchors)
    anchors[, `:=`(disp_e = dt$disp_e[idx], disp_n = dt$disp_n[idx])]
    ap <- .projLocal(anchors$lon, anchors$lat, deploy_lon, deploy_lat, R_earth)
    anchors[, `:=`(true_e = ap$e, true_n = ap$n)]
    n_anchors <- nrow(anchors)

    dt[, `:=`(corr_e = disp_e, corr_n = disp_n)]
    if (nrow(anchors) > 1L && control$vpc.method == "scale_rotate") {
      # Gundog-style correction: per segment, rescale + rotate the whole reckoned sub-path so it is pinned at
      # the previous fix and lands exactly on the next (a similarity transform of the step vectors). Best for
      # SYSTEMATIC drift (a speed-scale or heading-bias error); ignores fix quality / drift.rate / weighting.
      sr <- .vpcScaleRotate(dt$disp_e, dt$disp_n, anchors, as.numeric(dt[[datetime.col]]))
      dt[, `:=`(corr_e = sr$e, corr_n = sr$n)]
    } else if (nrow(anchors) > 1L) {
      # additive redistribution with a running CARRY, so the correction is continuous across anchors and
      # lands on each fix (forced through exactly when linear/gain=1; elastically pulled when error_weighted).
      carry_e <- 0; carry_n <- 0
      for (a in seq_len(nrow(anchors) - 1L)) {
        si <- anchors$idx[a]; ei <- anchors$idx[a + 1L]
        tspan <- as.numeric(difftime(dt[[datetime.col]][ei], dt[[datetime.col]][si], units = "secs"))
        if (tspan <= 0) next
        gain <- 1.0
        if (control$vpc.method == "error_weighted") {
          r_gps <- .anchorRadius(anchors$quality[a + 1L], control$anchor.error.radii)
          r_dr  <- .driftError(tspan, control)                # reckoning error over the segment (m)
          gain  <- (r_dr^2) / (r_dr^2 + r_gps^2)              # 1D Kalman-style trust in the fix
        }
        err_e <- anchors$true_e[a + 1L] - (dt$disp_e[ei] + carry_e)   # residual to the fix from the carried DR
        err_n <- anchors$true_n[a + 1L] - (dt$disp_n[ei] + carry_n)
        # spread the drift across the segment: by reckoned distance travelled (default) or elapsed time. Both
        # run 0 -> 1 across the segment, so the correction stays continuous and lands on the fix at ei.
        if (control$vpc.weighting == "distance") {
          steplen <- c(0, sqrt(diff(dt$disp_e[si:ei])^2 + diff(dt$disp_n[si:ei])^2))
          cumlen  <- cumsum(steplen); total <- cumlen[length(cumlen)]
          w <- if (is.finite(total) && total > 0) cumlen / total
               else as.numeric(difftime(dt[[datetime.col]][si:ei], dt[[datetime.col]][si], units = "secs")) / tspan
        } else {
          w <- as.numeric(difftime(dt[[datetime.col]][si:ei], dt[[datetime.col]][si], units = "secs")) / tspan
        }
        dt[si:ei, corr_e := disp_e + carry_e + gain * err_e * w]
        dt[si:ei, corr_n := disp_n + carry_n + gain * err_n * w]
        carry_e <- carry_e + gain * err_e; carry_n <- carry_n + gain * err_n
      }
      # tail after the last fix: keep dead-reckoning FROM the last corrected position by carrying the final
      # accumulated offset forward, rather than snapping back to the origin-relative raw path at the next sample
      last_i <- anchors$idx[nrow(anchors)]
      if (last_i < nrow(dt)) dt[(last_i + 1L):.N, `:=`(corr_e = disp_e + carry_e, corr_n = disp_n + carry_n)]
    }
    bk <- .unprojLocal(dt$corr_e, dt$corr_n, deploy_lon, deploy_lat, R_earth)
    dt[, `:=`(pseudo_lon = bk$lon, pseudo_lat = bk$lat)]
    dt[, c("corr_e", "corr_n") := NULL]
  } else {
    bk <- .unprojLocal(dt$disp_e, dt$disp_n, deploy_lon, deploy_lat, R_earth)
    dt[, `:=`(pseudo_lon = bk$lon, pseudo_lat = bk$lat)]
  }

  # per-sample positional uncertainty (m, 1 sigma): reckoning error pinned to the known fixes. Between two
  # anchors it is the forward/backward reckoning-error combination (Brownian-bridge / RTS-smoother style),
  # ~ the fix radius at each anchor and bulging in between; with vpc = "none" it grows monotonically from
  # the deployment. Uses only timestamps + anchor radii, so it is computed after the correction above.
  unc_anchors <- if (control$vpc.method != "none") anchors else pos_anchors[1L]
  dt[, pseudo_error := .reckonUncertainty(dt, unc_anchors, control, datetime.col)]

  # build the (bounded) diagnostic payload while the raw displacement is still available
  pl <- if (make_plots) .reconstructPayload(dt, id, deploy_lon, deploy_lat, R_earth, control, datetime.col, anchors) else NULL
  dt[, c("disp_e", "disp_n") := NULL]

  # 3. vertical axis: attach the measured depth as z for a genuine 3-D pseudo-track
  if (isTRUE(control$include.depth) && "depth" %in% names(dt)) dt[, pseudo_depth := depth]

  proc_args <- list(meta, "reconstructTrack", speed_method = control$speed.method,
                    vpc = control$vpc.method, vpc_weighting = control$vpc.weighting, n_anchors = n_anchors,
                    drift_rate = control$drift.rate, drift_diffusion = control$drift.diffusion,
                    dim = if (isTRUE(control$include.depth) && "depth" %in% names(dt)) "3D" else "2D")
  if (!is.null(speed_fit)) {                                 # record how the VeDBA speed model was obtained
    fm <- speed_fit$meta
    proc_args <- c(proc_args, list(vedba_speed_source = fm$source, vedba_speed_intercept = fm$a,
                   vedba_speed_slope = fm$b, vedba_speed_r2 = fm$r2, vedba_speed_n_seg = fm$n_seg))
  }
  meta <- do.call(.appendProcessing, proc_args)
  dt <- .restoreMeta(dt, meta)
  attr(dt, "n_anchors") <- n_anchors
  if (!is.null(pl)) attr(dt, "plot_payload") <- pl
  dt
}

#' Spherical (small-angle destination-point) dead-reckoning integration. Given per-step east/north ground
#' displacements (metres), advances latitude along the meridian (`dlat = dn / R`) and longitude scaled by the
#' running latitude (`dlon = de / (R cos(lat))`), both by cumulative sum. This is the destination-point
#' formula to O((step/R)^2) - negligible for biologging step sizes - while staying vectorised. Correctly
#' tracks the changing E-W scale over a latitude excursion, unlike a fixed-parallel flat earth. Returns the
#' raw dead-reckoned `lon`/`lat` (decimal degrees).
#' @keywords internal
#' @noRd
.deadReckonSpherical <- function(d_e, d_n, lon0, lat0, R) {
  lat <- lat0 + cumsum(d_n) / R * (180 / pi)
  lon <- lon0 + cumsum(d_e / (R * cos(lat * pi / 180))) * (180 / pi)
  list(lon = lon, lat = lat)
}

#' Forward local map: lon/lat -> east/north metres in a per-point-latitude equirectangular plane about
#' (`lon0`, `lat0`). Self-consistent inverse of \code{.unprojLocal}: northing is the meridian arc, easting
#' uses the point's own latitude, so that reckoned positions and fixes share one metric for the VPC. The
#' longitude difference is wrapped to (-180, 180], so a fix just across the antimeridian projects to its true
#' short-way easting (no single biologging step or inter-fix segment spans > 180 deg).
#' @keywords internal
#' @noRd
.projLocal <- function(lon, lat, lon0, lat0, R) {
  dlon <- ((lon - lon0 + 180) %% 360) - 180
  list(e = dlon * (pi / 180) * R * cos(lat * pi / 180),
       n = (lat - lat0) * (pi / 180) * R)
}

#' Inverse local map: east/north metres -> lon/lat (decimal degrees). Recovers latitude from the northing
#' (meridian), then longitude from the easting scaled by that latitude - the exact inverse of
#' \code{.projLocal}; the returned longitude is wrapped to (-180, 180].
#' @keywords internal
#' @noRd
.unprojLocal <- function(e, n, lon0, lat0, R) {
  lat <- lat0 + n / R * (180 / pi)
  lon <- lon0 + e / (R * cos(lat * pi / 180)) * (180 / pi)
  list(lon = ((lon + 180) %% 360) - 180, lat = lat)
}

#' Collect a deployment's known-position anchors (deployment origin, pop-up, and genuine GPS/Argos fixes) in
#' time order - the single source of truth shared by the VeDBA speed auto-calibration and the VPC
#' correction. Returns a data.table (idx, time, lat, lon, quality); the deployment origin (idx 1) is always
#' present. The pop-up is bound before the sensor fixes so that, on an index collision, `unique(by = "idx")`
#' keeps Deploy > Popup > fix; rows that `.withPositionColumns` injects (position_type "Metadata ...") are
#' excluded, since deploy/pop-up are already added explicitly here.
#' @keywords internal
#' @noRd
.positionAnchors <- function(dt, meta, datetime.col, deploy_lat, deploy_lon) {
  anchors <- data.table::data.table(idx = 1L, time = dt[[datetime.col]][1], lat = deploy_lat,
                                    lon = deploy_lon, quality = "Deploy")
  if (!is.null(meta$deployment$popup_lat) && !is.null(meta$deployment$popup_lon) &&
      !is.na(meta$deployment$popup_lat) && !is.na(meta$deployment$popup_lon)) {
    pi_ <- which.min(abs(as.numeric(difftime(dt[[datetime.col]], meta$deployment$popup_datetime, units = "secs"))))
    if (length(pi_))
      anchors <- rbind(anchors, data.table::data.table(idx = pi_, time = dt[[datetime.col]][pi_],
        lat = meta$deployment$popup_lat, lon = meta$deployment$popup_lon, quality = "Popup"))
  }
  if (all(c("lat", "lon") %in% names(dt))) {
    pt <- if ("position_type" %in% names(dt)) as.character(dt$position_type) else rep(NA_character_, nrow(dt))
    fix <- which(!is.na(dt$lat) & !is.na(dt$lon) & !(!is.na(pt) & startsWith(pt, "Metadata")))
    if (length(fix))
      anchors <- rbind(anchors, data.table::data.table(idx = fix, time = dt[[datetime.col]][fix],
        lat = dt$lat[fix], lon = dt$lon[fix],
        quality = if ("quality" %in% names(dt)) as.character(dt$quality[fix]) else "Unknown"))
  }
  anchors <- unique(anchors, by = "idx"); data.table::setorder(anchors, idx)
  anchors
}

#' Resolve the VeDBA-to-speed model for `speed.method = "vedba"`. A user-supplied `control$vedba.model`
#' (length-2 numeric `c(intercept, slope)`) is accepted as-is; otherwise the model is auto-calibrated from
#' the deployment's own position fixes (Gunner et al. 2021): each pair of consecutive fixes gives a
#' straight-line (great-circle) speed, kept only when the intervening path was near-straight (so the
#' straight-line distance approximates the true swum distance), and speed is regressed on the interval-mean
#' VeDBA. Returns `list(fallback = FALSE, a, b, meta)` on success, or `list(fallback = TRUE, reason, meta)`
#' when the calibration is under-determined or non-physical - in which case the caller uses
#' `constant.speed`. `meta` records the source, coefficients, R-squared and segment count for the log/metadata.
#' @keywords internal
#' @noRd
.vedbaSpeedModel <- function(dt, pos_anchors, datetime.col, dt_sec, control,
                             straight.min = 0.9, min.seconds = 60, min.dist.m = 100, min.segments = 4L) {
  fb <- function(reason) list(fallback = TRUE, reason = reason,
    meta = list(source = "constant_fallback", a = NA_real_, b = NA_real_, r2 = NA_real_, n_seg = 0L))
  m <- control$vedba.model
  if (is.numeric(m) && length(m) == 2L)
    return(list(fallback = FALSE, a = m[[1]], b = m[[2]],
                meta = list(source = "user", a = m[[1]], b = m[[2]], r2 = NA_real_, n_seg = NA_integer_)))
  if (!"vedba" %in% names(dt)) return(fb("no VeDBA channel"))
  if (is.null(pos_anchors) || nrow(pos_anchors) < 3L) return(fb("too few position fixes"))

  hdg <- dt$heading * (pi / 180); pit <- dt$pitch * (pi / 180)
  vseg <- numeric(0); xseg <- numeric(0)
  for (a in seq_len(nrow(pos_anchors) - 1L)) {
    si <- pos_anchors$idx[a]; ei <- pos_anchors$idx[a + 1L]
    if (ei <= si) next
    tsec <- as.numeric(difftime(pos_anchors$time[a + 1L], pos_anchors$time[a], units = "secs"))
    if (!is.finite(tsec) || tsec < min.seconds) next
    dist_m <- .trackDistance(pos_anchors$lon[a], pos_anchors$lat[a],
                             pos_anchors$lon[a + 1L], pos_anchors$lat[a + 1L]) * 1000   # km -> m
    if (!is.finite(dist_m) || dist_m < min.dist.m) next
    seg <- (si + 1L):ei                                       # unit-speed heading path over the interval
    gross <- sum(cos(pit[seg]) * dt_sec[seg], na.rm = TRUE)   # horizontal path length at unit along-body speed
    if (!is.finite(gross) || gross <= 0) next
    dx <- sum(sin(hdg[seg]) * cos(pit[seg]) * dt_sec[seg], na.rm = TRUE)
    dy <- sum(cos(hdg[seg]) * cos(pit[seg]) * dt_sec[seg], na.rm = TRUE)
    straightness <- sqrt(dx^2 + dy^2) / gross                 # 1 = perfectly straight, 0 = closed loop
    if (!is.finite(straightness) || straightness < straight.min) next
    # ALONG-BODY speed, not horizontal: the GPS chord is a horizontal distance, so dividing it by the
    # unit-speed horizontal time-integral (gross = integral of cos(pitch) dt) removes the pitch projection
    # and yields the along-body speed that speed_dr * cos(pitch) re-projects downstream (net ~ gross here,
    # since segments are pre-filtered to straightness >= straight.min). At pitch 0, gross == tsec.
    v <- dist_m / gross
    if (!is.finite(v) || v > control$max.speed) next          # implausible -> a fix outlier, skip
    x <- mean(dt$vedba[si:ei], na.rm = TRUE)
    if (!is.finite(x)) next
    vseg <- c(vseg, v); xseg <- c(xseg, x)
  }
  if (length(vseg) < min.segments) return(fb(sprintf("only %d usable straight segment(s)", length(vseg))))
  if (!is.finite(stats::sd(xseg)) || stats::sd(xseg) <= 0) return(fb("no VeDBA spread across segments"))
  ft <- stats::lm(vseg ~ xseg)
  co <- stats::coef(ft); r2 <- summary(ft)$r.squared
  if (!all(is.finite(co)) || co[[2]] <= 0) return(fb("non-physical calibration (slope <= 0)"))
  list(fallback = FALSE, a = co[[1]], b = co[[2]],
       meta = list(source = "auto", a = co[[1]], b = co[[2]], r2 = r2, n_seg = length(vseg)))
}

#' Expected position error radius (m) for an anchor of the given `quality`, from control$anchor.error.radii;
#' unknown labels fall back to a coarse 1500 m (roughly an Argos class-1 fix).
#' @keywords internal
#' @noRd
.anchorRadius <- function(quality, radii, default = 1500) {
  q <- as.character(quality)
  if (length(q) == 1L && !is.na(q) && q %in% names(radii)) radii[[q]] else default
}

#' Accumulated dead-reckoning error (m) after `t` seconds: sqrt((drift.rate * t)^2 + drift.diffusion * t).
#' The linear term is a systematic (bias-like) drift; the sqrt(t) term is a random-walk (diffusive) drift.
#' With drift.diffusion = 0 this is exactly the linear drift.rate * t model.
#' @keywords internal
#' @noRd
.driftError <- function(t, control) {
  sqrt((control$drift.rate * t)^2 + control$drift.diffusion * t)
}

#' Reconstructability gate: for a track with no genuine interior fixes (anchored only by the deployment
#' and optional pop-up), assess whether the two endpoints can constrain the interior. `directedness` = net
#' deploy->pop-up displacement / reckoned path length (a scale-free ratio, validated against held-out error);
#' below `control$reconstructability.min` the interior is unreliable, so warn (never abort) and return the
#' verdict for `meta$sensors$reconstructability`. Returns NULL when the gate does not apply (interior fixes
#' present, or gate disabled). `dt` must carry `disp_e`/`disp_n`; `pos_anchors` carries idx/lon/lat/quality.
#' @keywords internal
#' @noRd
.reconstructabilityGate <- function(dt, pos_anchors, deploy_lon, deploy_lat, control, id, lvl) {
  if (!isTRUE(control$reconstructability.min > 0)) return(NULL)
  if (nrow(pos_anchors[!(quality %in% c("Deploy", "Popup"))])) return(NULL)   # interior fixes constrain it
  path_m <- sum(sqrt(diff(dt$disp_e)^2 + diff(dt$disp_n)^2), na.rm = TRUE)     # reckoned path length (m)
  pu <- pos_anchors[quality == "Popup"]
  if (!nrow(pu)) {                                          # no pop-up and no interior fixes: fully unbounded
    warning(sprintf("reconstructTrack: '%s' has no pop-up and no interior fixes - the track is fully unbounded dead-reckoning; absolute positions are unconstrained.", id), call. = FALSE)
    .log_skip(lvl, "reconstructability: fully unbounded (deployment origin only)")
    return(list(endpoint_bounded = TRUE, directedness = NA_real_, net_km = NA_real_,
                path_km = round(path_m / 1000, 2), gate = "unbounded"))
  }
  net_m <- .trackDistance(deploy_lon, deploy_lat, pu$lon[1], pu$lat[1]) * 1000
  # directedness in [0, 1]; capped at 1. When the reckoned path is ~0 (all speeds 0, pitch +/-90) but the
  # pop-up is elsewhere (net > 0), the path->0+ limit is fully directed (1); only a truly stationary track
  # (net 0 and path 0) is 0.
  d <- if (is.finite(path_m) && path_m > 0) min(1, net_m / path_m) else if (is.finite(net_m) && net_m > 0) 1 else 0
  gate <- if (d < control$reconstructability.min) "warn" else "pass"
  if (gate == "warn") {
    warning(sprintf("reconstructTrack: '%s' is anchored only by its endpoints; the net displacement is %.0f%% of the reckoned path (directedness %.2f < %.2f) - the interior geometry is unreliable, treat the shape between the endpoints with great caution.",
                    id, 100 * d, d, control$reconstructability.min), call. = FALSE)
    .log_skip(lvl, sprintf("reconstructability: interior unreliable (directedness %.2f < %.2f, endpoints only)", d, control$reconstructability.min))
  }
  list(endpoint_bounded = TRUE, directedness = round(d, 3), net_km = round(net_m / 1000, 2),
       path_km = round(path_m / 1000, 2), gate = gate)
}

#' Gundog-style scale + rotate Verified Position Correction (Gunner et al. 2021). Between successive anchors
#' the reckoned sub-path is transformed by a single uniform scale (`|net_VP| / |net_DR|`) and rotation
#' (`angle(net_VP) - angle(net_DR)`) about the previous (corrected) anchor, so the sub-path SHAPE is
#' preserved while its far end is pinned exactly onto the next fix. In this flat displacement domain the
#' transform lands on each fix exactly in one pass (no iteration needed, unlike the spherical original).
#' Best for systematic drift (a mis-scaled speed = pure scale, a heading bias = pure rotation). The scale is
#' ill-conditioned when the net reckoned displacement is small relative to the reckoned path length (a
#' stationary or near-closed/tortuous segment): a large scale factor would then magnify the interior wiggles.
#' Such segments (and coincident fixes) instead fall back to additive redistribution, which preserves the
#' reckoned shape and still lands exactly on the fix. The tail past the last anchor continues the last
#' segment's transform. `anchors` must carry integer `idx` and the anchor coordinates `true_e`/`true_n`.
#' Returns list(e, n) of full-length corrected displacements (metres, relative to the deployment). `ts` =
#' epoch seconds; `straight.min` = the minimum net/arc ratio for a segment to be scaled+rotated.
#' @keywords internal
#' @noRd
.vpcScaleRotate <- function(disp_e, disp_n, anchors, ts, eps = 1e-6, straight.min = 0.1) {
  n <- length(disp_e)
  ce <- disp_e; cn <- disp_n
  idx <- anchors$idx; te <- anchors$true_e; tn <- anchors$true_n
  f_last <- 1; ca_last <- 1; sa_last <- 0; last_i <- idx[1L]
  for (a in seq_len(nrow(anchors) - 1L)) {
    si <- idx[a]; ei <- idx[a + 1L]; if (ei < si) next
    seg <- si:ei
    rel_e <- disp_e[seg] - disp_e[si]; rel_n <- disp_n[seg] - disp_n[si]
    dxDR <- disp_e[ei] - disp_e[si]; dyDR <- disp_n[ei] - disp_n[si]
    dxVP <- te[a + 1L] - te[a];      dyVP <- tn[a + 1L] - tn[a]
    lenDR <- sqrt(dxDR^2 + dyDR^2);  lenVP <- sqrt(dxVP^2 + dyVP^2)
    arc   <- sum(sqrt(diff(disp_e[seg])^2 + diff(disp_n[seg])^2))   # reckoned path length within the segment
    f <- lenVP / lenDR
    if (is.finite(f) && lenDR >= eps && lenVP >= eps && lenDR >= straight.min * arc) {
      # well-conditioned: scale + rotate the whole sub-path about the previous fix
      dAng <- atan2(dyVP, dxVP) - atan2(dyDR, dxDR); ca <- cos(dAng); sa <- sin(dAng)
      ce[seg] <- te[a] + f * (ca * rel_e - sa * rel_n)
      cn[seg] <- tn[a] + f * (sa * rel_e + ca * rel_n)
      f_last <- f; ca_last <- ca; sa_last <- sa
    } else {
      # ill-conditioned (stationary / near-closed / coincident fixes): additively redistribute the endpoint
      # error along the reckoned sub-path (distance-weighted, time as a fallback) - shape kept, fix still hit
      steplen <- c(0, sqrt(diff(disp_e[seg])^2 + diff(disp_n[seg])^2)); cuml <- cumsum(steplen)
      tot <- cuml[length(cuml)]
      w <- if (is.finite(tot) && tot > 0) cuml / tot
           else if (ts[ei] > ts[si]) (ts[seg] - ts[si]) / (ts[ei] - ts[si]) else seq(0, 1, length.out = length(seg))
      ce[seg] <- te[a] + rel_e + w * (dxVP - dxDR)
      cn[seg] <- tn[a] + rel_n + w * (dyVP - dyDR)
      f_last <- 1; ca_last <- 1; sa_last <- 0
    }
    last_i <- ei
  }
  if (last_i < n) {                                          # tail: extend the last segment's transform
    seg <- (last_i + 1L):n; k <- nrow(anchors)
    rel_e <- disp_e[seg] - disp_e[last_i]; rel_n <- disp_n[seg] - disp_n[last_i]
    ce[seg] <- te[k] + f_last * (ca_last * rel_e - sa_last * rel_n)
    cn[seg] <- tn[k] + f_last * (sa_last * rel_e + ca_last * rel_n)
  }
  list(e = ce, n = cn)
}

#' Per-sample 1-sigma positional uncertainty (m) for the pseudo-track. Between two successive anchors the
#' reckoning error is combined forward-from-the-previous and backward-from-the-next fix by inverse-variance
#' (a Brownian-bridge / RTS-smoother envelope): each leg is sqrt(anchor_radius^2 + driftError(dt)^2), so the
#' result is ~ the fix radius at each anchor and bulges mid-segment; after the last anchor (or, with a lone
#' deploy anchor for vpc = "none") it grows monotonically forward. See Wensveen et al. (2015) for the
#' forward/backward reconstruction idea. `anchors` must carry integer `idx` and `quality`.
#' @keywords internal
#' @noRd
.reckonUncertainty <- function(dt, anchors, control, datetime.col) {
  n <- nrow(dt); radii <- control$anchor.error.radii
  ts <- as.numeric(dt[[datetime.col]])                         # epoch seconds
  err <- rep(NA_real_, n)
  if (is.null(anchors) || !nrow(anchors)) return(sqrt(50^2 + .driftError(ts - ts[1L], control)^2))
  idx <- anchors$idx
  if (nrow(anchors) >= 2L) for (a in seq_len(nrow(anchors) - 1L)) {
    si <- idx[a]; ei <- idx[a + 1L]; if (ei < si) next
    seg <- si:ei
    r_a <- .anchorRadius(anchors$quality[a], radii); r_b <- .anchorRadius(anchors$quality[a + 1L], radii)
    e_fwd <- sqrt(r_a^2 + .driftError(ts[seg] - ts[si], control)^2)   # reckon from the previous fix
    e_bwd <- sqrt(r_b^2 + .driftError(ts[ei] - ts[seg], control)^2)   # reckon back from the next fix
    sig <- sqrt(1 / (1 / e_fwd^2 + 1 / e_bwd^2))                      # inverse-variance combination
    err[seg] <- pmin(err[seg], sig, na.rm = TRUE)                     # shared anchor idx: keep the tighter
  }
  # head before the first anchor (defensive; current callers always anchor idx 1): grow backward from it
  if (idx[1L] > 1L) {
    seg <- 1L:idx[1L]
    r_first <- .anchorRadius(anchors$quality[1L], radii)
    err[seg] <- pmin(err[seg], sqrt(r_first^2 + .driftError(ts[idx[1L]] - ts[seg], control)^2), na.rm = TRUE)
  }
  # tail after the last anchor (and the whole vpc = "none" case): grow FORWARD from the smoothed uncertainty
  # already at that anchor - NOT from its raw radius, which would step-jump when good reckoning had tightened
  # it (e.g. a precise fix followed by a coarse Argos anchor)
  last <- idx[length(idx)]
  if (last <= n) {
    seg <- last:n
    seed <- if (is.finite(err[last])) err[last] else .anchorRadius(anchors$quality[length(idx)], radii)
    err[seg] <- pmin(err[seg], sqrt(seed^2 + .driftError(ts[seg] - ts[last], control)^2), na.rm = TRUE)
  }
  err
}

#######################################################################################################
# Diagnostic report ###################################################################################
#######################################################################################################

#' Assemble the small per-deployment plotting payload (thinned raw + corrected track, anchors with the DR
#' drift at each fix, speed source, depth profile, summary stats). Built while `disp_e/disp_n` still exist.
#' @keywords internal
#' @noRd
.reconstructPayload <- function(dt, id, deploy_lon, deploy_lat, R_earth, control, datetime.col, anchors,
                                max.pts = 3000L) {
  n <- nrow(dt)
  idx <- if (n > max.pts) round(seq(1L, n, length.out = max.pts)) else seq_len(n)
  bk <- .unprojLocal(dt$disp_e, dt$disp_n, deploy_lon, deploy_lat, R_earth)                # raw DR lon/lat
  cl <- bk$lon; ct <- bk$lat
  anc <- NULL
  if (!is.null(anchors) && nrow(anchors)) {
    drift <- sqrt((anchors$true_e - anchors$disp_e)^2 + (anchors$true_n - anchors$disp_n)^2)   # DR error at fix (m)
    anc <- data.frame(lon = anchors$lon, lat = anchors$lat, quality = as.character(anchors$quality),
                      drift_m = drift, stringsAsFactors = FALSE)
  }
  step_km  <- .trackDistances(dt$pseudo_lon, dt$pseudo_lat)   # reuse the trackMetrics haversine helper
  total_km <- sum(step_km, na.rm = TRUE)
  net_km   <- .trackDistance(dt$pseudo_lon[1], dt$pseudo_lat[1], dt$pseudo_lon[n], dt$pseudo_lat[n])
  mdrift   <- if (!is.null(anc) && nrow(anc) > 1L) mean(anc$drift_m[-1], na.rm = TRUE) else NA_real_
  list(id = id, speed.method = control$speed.method, vpc = control$vpc.method,
       raw = list(lon = cl[idx], lat = ct[idx]),
       cor = list(lon = dt$pseudo_lon[idx], lat = dt$pseudo_lat[idx]),
       anchors = anc, speed = dt$speed_dr[idx],
       depth = if ("depth" %in% names(dt))
                 list(h = as.numeric(dt[[datetime.col]][idx] - dt[[datetime.col]][1], units = "hours"), z = dt$depth[idx])
               else NULL,
       stats = list(total_km = total_km, net_km = net_km, n_anchors = if (is.null(anchors)) 0L else nrow(anchors),
                    mean_drift_m = mdrift, max_drift_m = if (!is.null(anc)) max(anc$drift_m, na.rm = TRUE) else NA_real_,
                    flag = isTRUE(is.finite(mdrift) && mdrift > 5000)))
}

#' One summary-table row per deployment (drawn on the report's first page).
#' @keywords internal
#' @noRd
.reconstructSummaryRecord <- function(pl) {
  if (is.null(pl)) return(NULL)
  list(id = pl$id, distance_km = pl$stats$total_km, net_km = pl$stats$net_km, n_anchors = pl$stats$n_anchors,
       mean_drift_m = pl$stats$mean_drift_m, speed.method = pl$speed.method, vpc = pl$vpc, flag = pl$stats$flag)
}

#' Report page 1: a table of every deployment's reconstruction (worst-first by residual drift).
#' @keywords internal
#' @noRd
.drawReconstructSummaryPage <- function(records, stats) {
  oldpar <- graphics::par(no.readonly = TRUE); on.exit(graphics::par(oldpar), add = TRUE)
  graphics::layout(1)
  if (length(records))
    records <- records[order(-vapply(records, function(r) if (is.finite(r$mean_drift_m)) r$mean_drift_m else -1, numeric(1)))]
  fnum <- function(v, fmt) if (is.null(v) || !is.finite(v)) "-" else sprintf(fmt, v)
  graphics::par(mar = c(1.5, 0.6, 1, 0.6)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
  graphics::text(0, 0.985, "reconstructTrack  -  run summary", adj = c(0, 1), font = 2, cex = 1.4)
  graphics::text(1, 0.985, format(Sys.time(), "%Y-%m-%d %H:%M"), adj = c(1, 1), cex = 0.75, col = "grey40")
  graphics::text(0, 0.935, sprintf("%d dataset%s  |  %d reconstructed", stats$n, if (stats$n != 1) "s" else "", stats$n_ok),
                 adj = c(0, 1), cex = 0.95)
  graphics::text(0, 0.905, "sorted by residual drift (worst first)", adj = c(0, 1), cex = 0.72, col = "grey45")
  cx <- c(id = 0.004, dist = 0.42, net = 0.55, anc = 0.68, drift = 0.82, meth = 0.90)
  hcy <- 0.855; graphics::rect(0, hcy - 0.014, 1, hcy + 0.014, col = "grey90", border = NA)
  hdr <- c(id = "Deployment", dist = "Dist (km)", net = "Net (km)", anc = "Fixes", drift = "Drift (m)", meth = "Method")
  adj <- c(id = 0, dist = 1, net = 1, anc = 1, drift = 1, meth = 0)
  for (k in names(cx)) graphics::text(cx[[k]], hcy, hdr[[k]], adj = c(adj[[k]], 0.5), font = 2, cex = 0.6, col = "grey15")
  graphics::segments(0, hcy - 0.016, 1, hcy - 0.016, col = "grey55")
  row_top <- hcy - 0.030; dy <- 0.0195; drawn <- 0L
  for (j in seq_along(records)) {
    r <- records[[j]]; y <- row_top - dy * (j - 1L); if (y < 0.035) break
    if (j %% 2L == 0L) graphics::rect(0, y - dy / 2, 1, y + dy / 2, col = "grey96", border = NA)
    cc <- if (isTRUE(r$flag)) "#c62828" else "grey20"
    graphics::text(cx[["id"]], y, r$id, adj = c(0, 0.5), cex = 0.6, font = 2, col = cc)
    graphics::text(cx[["dist"]], y, fnum(r$distance_km, "%.2f"), adj = c(1, 0.5), cex = 0.58, col = "grey25")
    graphics::text(cx[["net"]], y, fnum(r$net_km, "%.2f"), adj = c(1, 0.5), cex = 0.58, col = "grey25")
    graphics::text(cx[["anc"]], y, r$n_anchors, adj = c(1, 0.5), cex = 0.58, col = "grey25")
    graphics::text(cx[["drift"]], y, fnum(r$mean_drift_m, "%.0f"), adj = c(1, 0.5), cex = 0.58, col = cc)
    graphics::text(cx[["meth"]], y, sprintf("%s/%s", r$speed.method, r$vpc), adj = c(0, 0.5), cex = 0.55, col = "grey35")
    drawn <- j
  }
  if (!length(records)) graphics::text(0.5, 0.5, "no tracks to report", cex = 1, col = "grey50")
  else if (drawn < length(records))
    graphics::text(0.5, 0.014, sprintf("+ %d more not shown (sorted worst-first)", length(records) - drawn),
                   adj = c(0.5, 0.5), cex = 0.72, col = "grey45", font = 3)
  invisible(NULL)
}

#' Report detail page for one deployment: the reconstructed track vs the raw DR path and the position
#' fixes (the map), the per-fix drift the VPC corrected, the speed source, and the depth profile.
#' @keywords internal
#' @noRd
.plotReconstructIndividual <- function(pl) {
  oldpar <- graphics::par(no.readonly = TRUE)
  on.exit({ graphics::layout(1); graphics::par(oldpar) }, add = TRUE)
  RAWC <- "grey62"; CORC <- "#1565c0"; DEP <- "#2e7d32"; POP <- "#c62828"; FIX <- "#e08a00"
  graphics::layout(matrix(c(1, 1, 1, 1,  2, 2, 2, 3,  2, 2, 2, 4,  5, 5, 5, 5), nrow = 4L, byrow = TRUE),
                  heights = c(0.5, 1, 1, 0.72))

  # --- header ---
  graphics::par(mar = c(0.2, 1, 0.4, 1)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
  graphics::text(0, 0.72, sprintf("%s  -  reconstructed pseudo-track", pl$id), adj = c(0, 0.5), cex = 1.5, font = 2)
  graphics::text(0, 0.28, sprintf("%s DR + %s VPC   |   %.2f km path, %.2f km net   |   %d fixes%s",
                 pl$speed.method, pl$vpc, pl$stats$total_km, pl$stats$net_km, pl$stats$n_anchors,
                 if (is.finite(pl$stats$mean_drift_m)) sprintf(", mean drift %.0f m", pl$stats$mean_drift_m) else ""),
                 adj = c(0, 0.5), cex = 0.92, col = "grey30")

  # --- map: raw DR (grey) vs corrected (blue) + fixes ---
  graphics::par(mar = c(3.4, 3.6, 2.0, 1), mgp = c(2.2, 0.7, 0), cex.axis = 0.8)
  xr <- range(c(pl$raw$lon, pl$cor$lon, pl$anchors$lon), na.rm = TRUE)
  yr <- range(c(pl$raw$lat, pl$cor$lat, pl$anchors$lat), na.rm = TRUE)
  graphics::plot(NA, xlim = xr, ylim = yr, asp = 1 / cos(mean(yr) * pi / 180), main = "reconstructed track",
                 cex.main = 1.0, font.main = 1, xlab = "longitude", ylab = "latitude")
  if (pl$vpc != "none") graphics::lines(pl$raw$lon, pl$raw$lat, col = RAWC, lwd = 1.0)
  graphics::lines(pl$cor$lon, pl$cor$lat, col = CORC, lwd = 1.6)
  graphics::points(pl$cor$lon[1], pl$cor$lat[1], pch = 16, col = DEP, cex = 1.1)
  if (!is.null(pl$anchors)) {
    ac <- ifelse(pl$anchors$quality == "Deploy", DEP, ifelse(pl$anchors$quality == "Popup", POP, FIX))
    graphics::points(pl$anchors$lon, pl$anchors$lat, pch = 21, bg = ac, col = "white", cex = 1.2, lwd = 0.6)
  }
  graphics::legend("topleft", bty = "n", cex = 0.72,
                   legend = c(if (pl$vpc != "none") "raw DR", "corrected", "fix"),
                   col = c(if (pl$vpc != "none") RAWC, CORC, FIX), lwd = c(if (pl$vpc != "none") 1, 2, NA),
                   pch = c(if (pl$vpc != "none") NA, NA, 16))

  # --- drift at each fix (m) ---
  graphics::par(mar = c(3.4, 3.6, 2.2, 0.8))
  if (!is.null(pl$anchors) && nrow(pl$anchors) > 1L) {
    d <- pl$anchors$drift_m[-1]
    graphics::barplot(d, col = FIX, border = NA, main = "DR drift at fix (m)", cex.main = 0.92, ylab = "m", las = 1)
  } else { graphics::plot.new(); graphics::text(0.5, 0.5, "no fixes\nto correct to", cex = 0.85, col = "grey50") }

  # --- speed source ---
  sp <- pl$speed[is.finite(pl$speed)]
  if (length(sp) > 2L) graphics::hist(sp, breaks = 30, col = grDevices::adjustcolor(CORC, 0.5), border = NA,
                                      main = "speed (m/s)", cex.main = 0.92, xlab = "", ylab = "")
  else { graphics::plot.new(); graphics::text(0.5, 0.5, "speed n/a", cex = 0.85, col = "grey50") }

  # --- depth profile (the vertical axis of the 3-D track) ---
  graphics::par(mar = c(3.6, 3.6, 2.0, 1))
  if (!is.null(pl$depth)) {
    graphics::plot(pl$depth$h, pl$depth$z, type = "l", col = "#00838f", lwd = 1, ylim = rev(range(pl$depth$z, na.rm = TRUE)),
                   main = "depth profile (3-D vertical axis)", cex.main = 0.95, font.main = 1,
                   xlab = "time (h)", ylab = "depth (m)")
  } else { graphics::plot.new(); graphics::text(0.5, 0.5, "no depth channel (2-D track)", cex = 0.95, col = "grey50") }
  invisible(NULL)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################

# trackMetrics helpers ----

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
