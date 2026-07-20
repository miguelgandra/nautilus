#######################################################################################################
# Plot reconstructed movement tracks ##################################################################
#######################################################################################################

#' Plot reconstructed movement tracks
#'
#' @description
#' Draws a per-animal grid of maps showing each deployment's movement in space: the genuine surface
#' position fixes (Fastloc-GPS, Argos and user-entered), the deployment and pop-up anchors, and - when
#' present - the dead-reckoned pseudo-track from \code{\link{reconstructTrack}}. It is the horizontal
#' (spatial) counterpart to \code{\link{plotDepthProfiles}} (the vertical view), and the terminal
#' visualization step of the movement-tracks branch (\code{reconstructTrack()} ->
#' \code{crossValidateTrack()}/\code{trackMetrics()} -> \code{plotTracks()}).
#'
#' @details
#' Everything is drawn in a single WGS84 longitude/latitude coordinate system with a latitude-corrected
#' equal-aspect projection, so points, track and (optional) coastline co-register exactly. The basemap is
#' deliberately lightweight: an optional land outline from the \pkg{maps}/\pkg{mapdata} packages that
#' degrades to a silent no-op when they are absent (no tile server, no Java, no network). Optional
#' bathymetry contours (\code{show.bathymetry}, via \pkg{marmap}) are fetched once for the whole run and
#' reused across panels.
#'
#' \strong{Uncertainty.} A dead-reckoned track is a best estimate whose confidence shrinks at each
#' anchoring fix and grows in the gaps between them. \code{reconstructTrack} quantifies this as
#' \code{pseudo_error} (per-sample 1-sigma positional uncertainty, metres); with \code{show.uncertainty =
#' TRUE} it is drawn as a translucent corridor around the track, so a segment that passes close to a fix
#' is visibly tighter than one that has drifted mid-gap. The pseudo-track is never drawn with more visual
#' weight than the genuine fixes it interpolates between.
#'
#' \strong{Colouring.} With \code{color.by = "depth"} or \code{"speed"} the track is coloured by
#' \code{pseudo_depth} or \code{speed_dr} on a scale shared across all panels (with a compact colour bar),
#' revealing where the animal was in the water column or how fast it moved.
#'
#' Position fixes are read from each tag's canonical record (\code{meta$ancillary$positions}); the deploy
#' and pop-up coordinates from \code{meta$deployment}. A deployment with neither a pseudo-track nor any
#' fix is skipped (and reported).
#'
#' @param data A `nautilus_tag`/data.frame, a (named) list of them, or a character vector of `.rds`
#'   file paths - typically the output of \code{\link{reconstructTrack}} (its `pseudo_lon`/`pseudo_lat`
#'   columns drive the track). A single aggregated data.frame is split by `id.col`.
#' @param color.by Colour the pseudo-track by a variable: `NULL` (default, a single colour), `"depth"`
#'   (`pseudo_depth`) or `"speed"` (`speed_dr`). Ignored for deployments without that column.
#' @param show.uncertainty Logical. Draw the `pseudo_error` uncertainty corridor around the pseudo-track.
#'   Default `TRUE`.
#' @param show.bathymetry Logical. Overlay bathymetry contours fetched from NOAA via \pkg{marmap} (a
#'   network download, performed once for the whole run). Default `FALSE`.
#' @param bathy.resolution Numeric. Resolution (arc-minutes) of the NOAA bathymetry grid when
#'   `show.bathymetry = TRUE`. Larger is coarser and faster. Default 1.
#' @param theme A \code{\link{plotTheme}} object controlling text/axis colours and the sequential ramp
#'   used for `color.by`. Default `plotTheme()`.
#' @param colors Optional named character vector overriding the semantic map palette. Recognised names:
#'   `fastgps`, `argos`, `user`, `track`, `deploy`, `popup`, `sea`. Unspecified entries keep their
#'   defaults. Default `NULL`.
#' @param max.points Integer. Per-track cap on the number of pseudo-track points actually drawn (the track
#'   is strided down to this many, always keeping the true first and last point) to keep screen rendering
#'   fast and vector PDFs small. Default 5000.
#' @param ncols,nrows Integer or `NULL`. Panel grid dimensions. When both are `NULL` (default) a grid is
#'   chosen automatically (up to 2 columns x 5 rows per page) and the run paginates across pages.
#' @param plot Logical. Draw to the active graphics device. Default `TRUE`.
#' @param plot.file Character. Path to a single multi-page PDF for the maps. The parent directory must
#'   already exist; must end in `.pdf`. `NULL` (default) writes no file. Independent of `plot`.
#' @param cex Numeric master text-scaling factor for the panels. Default 1.
#' @param id.col,datetime.col Character. Names of the ID and POSIXct datetime columns. Defaults `"ID"` /
#'   `"datetime"`.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal", or `2`/"detailed" (default; adds a
#'   live progress bar and per-skip notes).
#'
#' @return Invisibly, a data.frame with one row per input deployment (`id`, `n_fix`, `n_track`, `drawn`).
#'   Called for its side effect: the maps drawn to the active device and/or the multi-page `plot.file`.
#' @seealso \code{\link{reconstructTrack}}, \code{\link{crossValidateTrack}}, \code{\link{trackMetrics}},
#'   \code{\link{filterLocations}}, \code{\link{plotDepthProfiles}}, \code{\link{exportForSSM}}.
#' @examples
#' \dontrun{
#' tracks <- reconstructTrack(list.files("./data interim/oriented", full.names = TRUE))
#' # Per-animal maps: fixes + the depth-coloured dead-reckoned track and its 1-sigma corridor
#' plotTracks(tracks, color.by = "depth", plot.file = "./plots/tracks.pdf")
#' }
#' @export


plotTracks <- function(data,
                       color.by         = NULL,
                       show.uncertainty = TRUE,
                       show.bathymetry  = FALSE,
                       bathy.resolution = 1,
                       theme            = plotTheme(),
                       colors           = NULL,
                       max.points       = 5000,
                       ncols            = NULL,
                       nrows            = NULL,
                       plot             = TRUE,
                       plot.file        = NULL,
                       cex              = 1,
                       id.col           = "ID",
                       datetime.col     = "datetime",
                       verbose          = "detailed") {

  ##############################################################################
  # Validate arguments #########################################################
  ##############################################################################

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)

  if (!is.null(color.by)) .assert_choice(color.by, "color.by", c("depth", "speed"))
  .assert_flag(show.uncertainty, "show.uncertainty")
  .assert_flag(show.bathymetry, "show.bathymetry")
  .assert_number(bathy.resolution, "bathy.resolution", min = 0)
  if (!inherits(theme, "nautilus_theme")) .abort("{.arg theme} must be a {.fn plotTheme} object.")
  if (!is.null(colors) && (is.null(names(colors)) || !is.character(colors)))
    .abort("{.arg colors} must be a NAMED character vector (e.g. {.code c(track = \"red\")}).")
  .assert_count(max.points, "max.points", min = 2L)
  if (!is.null(ncols)) .assert_count(ncols, "ncols", min = 1L)
  if (!is.null(nrows)) .assert_count(nrows, "nrows", min = 1L)
  .assert_number(cex, "cex", min = 0)
  .assert_flag(plot, "plot")
  .assert_writable_file(plot.file, "plot.file", ext = "pdf")   # fail-fast: parent dir must exist
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  if (!plot && is.null(plot.file))
    .abort(c("Nothing to plot.", "i" = "Set {.arg plot = TRUE} or provide a {.arg plot.file}."))
  if (show.bathymetry && !requireNamespace("marmap", quietly = TRUE))
    .abort(c("{.arg show.bathymetry = TRUE} needs the {.pkg marmap} package.",
             "i" = "Install it with {.code install.packages(\"marmap\")}, or set {.arg show.bathymetry = FALSE}."))

  # semantic map palette (override individual entries via `colors`)
  pal <- c(fastgps = "#2AA7A0", argos = "#5B7FBD", user = "#7E57C2", track = "#C0392B",
           deploy = "#1D9E75", popup = "#E8A33D", sea = "#EAF1F6")
  if (!is.null(colors)) pal[names(colors)] <- colors

  ##############################################################################
  # Gather each deployment's track + fixes #####################################
  ##############################################################################

  src <- .resolveInput(data, id.col)
  .log_header(lvl, "plotTracks", "Mapping surface fixes and dead-reckoned tracks",
              bullets = sprintf("Input: %d deployment%s%s", src$n, if (src$n != 1) "s" else "",
                                if (!is.null(color.by)) paste0(" ", cli::symbol$bullet, " coloured by ", color.by) else ""))

  payloads <- list(); summary_rows <- vector("list", src$n)
  n_empty <- 0L
  pb <- .log_progress_start(lvl, src$n, "Loading")
  for (i in seq_len(src$n)) {
    .log_progress_step(pb)
    x  <- src$get(i)
    id <- as.character(.getMeta(x)$id %||% src$ids[i])

    # genuine fixes (canonical record) + deploy/pop-up anchors
    fixes  <- .tagPositions(x)
    dep    <- .getMeta(x)$deployment
    deploy <- if (!is.null(dep) && all(is.finite(c(dep$lon, dep$lat)))) list(lon = dep$lon, lat = dep$lat) else NULL
    popup  <- if (!is.null(dep) && all(is.finite(c(dep$popup_lon, dep$popup_lat)))) list(lon = dep$popup_lon, lat = dep$popup_lat) else NULL

    # dead-reckoned pseudo-track (time-ordered, downsampled for drawing; true endpoints preserved)
    track <- .gatherPseudoTrack(x, datetime.col, color.by, max.points)

    n_fix <- nrow(fixes); n_track <- if (is.null(track)) 0L else nrow(track)
    summary_rows[[i]] <- data.frame(id = id, n_fix = n_fix, n_track = n_track, drawn = FALSE,
                                    stringsAsFactors = FALSE)
    if (n_fix == 0L && n_track == 0L) { n_empty <- n_empty + 1L; next }

    summary_rows[[i]]$drawn <- TRUE
    payloads[[length(payloads) + 1L]] <- list(id = id, fixes = fixes, track = track,
                                              deploy = deploy, popup = popup)
  }
  .log_progress_done(pb)
  summary_df <- do.call(rbind, summary_rows)

  if (!length(payloads)) {
    if (lvl >= 1L) { .log_summary(lvl); .log_done(lvl, "0 tracks plotted (no fixes or reconstructed tracks)"); .log_runtime(lvl, start.time) }
    return(invisible(summary_df))
  }

  ##############################################################################
  # Global colour scale + optional bathymetry (resolved once) ##################
  ##############################################################################

  color_range <- NULL; ramp <- NULL
  if (!is.null(color.by)) {
    vals <- unlist(lapply(payloads, function(p) if (!is.null(p$track) && "value" %in% names(p$track)) p$track$value else NULL))
    vals <- vals[is.finite(vals)]
    # a constant channel carries no colour information (and a zero-width scale is degenerate): fall back
    # to the single-colour track rather than dividing by a zero range
    if (length(vals) && diff(range(vals)) > 0) {
      color_range <- range(vals)
      ramp <- grDevices::colorRampPalette(theme$sequential)(100)
    }
  }

  # union extent (used for a single bathymetry fetch and returned nowhere else)
  bathy <- NULL
  if (show.bathymetry) {
    allx <- unlist(lapply(payloads, function(p) c(p$fixes$lon, p$track$lon, p$deploy$lon, p$popup$lon)))
    ally <- unlist(lapply(payloads, function(p) c(p$fixes$lat, p$track$lat, p$deploy$lat, p$popup$lat)))
    ext  <- .equalAspectExtent(allx, ally, f = 0.3)
    if (!is.null(ext)) bathy <- .fetchBathy(ext$xlim, ext$ylim, bathy.resolution, lvl)
  }

  ##############################################################################
  # Draw (paginated) to the caller's device and/or a multi-page PDF ############
  ##############################################################################

  lay <- .autoGrid(length(payloads), ncols, nrows)
  if (lvl >= 1L)
    .log_arrow(lvl, sprintf("layout: %d x %d per page%s", lay$nrows, lay$ncols,
                            if (length(lay$pages) > 1) sprintf(" %s %d pages", cli::symbol$bullet, length(lay$pages)) else ""))

  draw <- function(to.file = FALSE, unicode = TRUE) {
    graphics::par(oma = c(0.5, 0.5, 0.5, 0.5))
    for (pg in lay$pages) {
      graphics::par(mfrow = c(lay$nrows, lay$ncols))
      for (k in pg) .plotTrackPanel(payloads[[k]], pal = pal, theme = theme, color.by = color.by,
                                    color_range = color_range, ramp = ramp, bathy = bathy,
                                    show.uncertainty = show.uncertainty, cex = cex)
      for (b in seq_len(lay$per_page - length(pg))) graphics::plot.new()   # blank trailing cells
    }
  }
  .renderToDevices(draw, plot = plot, plot.file = plot.file,
                   width = 5.2 * lay$ncols, height = 5.0 * lay$nrows, cairo = TRUE)

  ##############################################################################
  # Summary ####################################################################
  ##############################################################################

  if (lvl >= 1L) {
    .log_summary(lvl)
    if (n_empty > 0) .log_detail(lvl, sprintf("no fixes or track (skipped): %d/%d", n_empty, src$n))
    .log_done(lvl, length(payloads), " track", if (length(payloads) != 1) "s", " plotted")
    if (!is.null(plot.file)) .log_arrow(lvl, "plots: ", plot.file)
    .log_runtime(lvl, start.time)
  }
  invisible(summary_df)
}


#######################################################################################################
# Internal: gather a time-ordered, downsampled pseudo-track ###########################################
#######################################################################################################

# Returns a data.frame(lon, lat, [value]) of the dead-reckoned track, ordered by time, with implausible
# rows dropped and strided down to at most `max.points` (the true first and last point are always kept so
# the endpoint markers are correct). `value` is the color.by channel (pseudo_depth / speed_dr) when
# requested and present. Returns NULL when the tag carries no pseudo-track.
#' @keywords internal
#' @noRd
.gatherPseudoTrack <- function(x, datetime.col, color.by, max.points) {
  nm <- names(x)
  if (!all(c("pseudo_lon", "pseudo_lat") %in% nm)) return(NULL)
  d <- data.table::as.data.table(x)
  # ordering a CHARACTER timestamp sorts it lexicographically ("01/02" before "28/01"), silently
  # reordering the track and bending the drawn path; coerce to real time first, or leave the order alone
  tnum <- if (datetime.col %in% nm) .asTimeSeconds(d[[datetime.col]]) else NULL
  ord <- if (!is.null(tnum)) order(tnum) else seq_len(nrow(d))
  lon <- .asNumericSafe(d[["pseudo_lon"]])[ord]; lat <- .asNumericSafe(d[["pseudo_lat"]])[ord]
  keep <- is.finite(lon) & is.finite(lat)
  lon <- lon[keep]; lat <- lat[keep]
  if (length(lon) < 1L) return(NULL)

  val <- NULL; err <- NULL
  vcol <- switch(color.by %||% "", depth = "pseudo_depth", speed = "speed_dr", NULL)
  if (!is.null(vcol) && vcol %in% nm) val <- .asNumericSafe(d[[vcol]])[ord][keep]   # factor colour channel
  if ("pseudo_error" %in% nm) err <- d[["pseudo_error"]][ord][keep]

  n <- length(lon)
  idx <- if (n > max.points) unique(c(seq(1L, n, by = ceiling(n / max.points)), n)) else seq_len(n)
  out <- data.frame(lon = lon[idx], lat = lat[idx], stringsAsFactors = FALSE)
  if (!is.null(val)) out$value <- val[idx]
  if (!is.null(err)) out$error <- err[idx]
  out
}


#######################################################################################################
# Internal: single-panel map drawer ###################################################################
#######################################################################################################

# Draws one deployment's map (base map, uncertainty corridor, pseudo-track, fixes, anchors, legend, scale
# bar) in a single lon/lat coordinate system. `payload` is assembled in plotTracks(); `color_range`/`ramp`
# are the shared colour scale (or NULL).
#' @keywords internal
#' @noRd
.plotTrackPanel <- function(payload, pal, theme, color.by, color_range, ramp, bathy,
                            show.uncertainty, cex) {

  fixes <- payload$fixes; track <- payload$track
  deploy <- payload$deploy; popup <- payload$popup
  ink <- theme$ink; axcol <- theme$axis

  # extent from every drawn element (equal aspect, shared helper)
  xs <- c(fixes$lon, track$lon, deploy$lon, popup$lon)
  ys <- c(fixes$lat, track$lat, deploy$lat, popup$lat)
  ext <- .equalAspectExtent(xs, ys, f = 0.2)
  if (is.null(ext)) { graphics::plot.new(); return(invisible(NULL)) }

  graphics::par(mar = c(3.6, 4.0, 3.0, 1.2), mgp = c(2.2, 0.6, 0))
  graphics::plot(NA, xlim = ext$xlim, ylim = ext$ylim, asp = ext$asp, axes = FALSE,
                 xlab = "", ylab = "", xaxs = "i", yaxs = "i")
  graphics::rect(graphics::par("usr")[1], graphics::par("usr")[3], graphics::par("usr")[2], graphics::par("usr")[4],
                 col = pal[["sea"]], border = NA)

  if (!is.null(bathy)) .drawBathy(bathy, ext$xlim, ext$ylim)
  .drawCoastline(ext$xlim, ext$ylim)
  graphics::box(col = "#BBBBBB")
  graphics::axis(1, at = pretty(ext$xlim, 5), labels = sprintf("%.2f", pretty(ext$xlim, 5)), col.axis = axcol, cex.axis = cex * 0.8)
  graphics::axis(2, at = pretty(ext$ylim, 5), labels = sprintf("%.2f", pretty(ext$ylim, 5)), las = 1, col.axis = axcol, cex.axis = cex * 0.8)
  graphics::title(xlab = "Longitude", line = 2.1, cex.lab = cex * 0.9, col.lab = ink)
  graphics::title(ylab = "Latitude", line = 2.6, cex.lab = cex * 0.9, col.lab = ink)
  graphics::title(main = payload$id, line = 1.5, cex.main = cex * 1.1, col.main = ink)
  graphics::title(main = .trackSubtitle(fixes, track), line = 0.5, font.main = 1, cex.main = cex * 0.8, col.main = theme$subtitle)

  # --- uncertainty corridor (translucent disks whose radius = pseudo_error), drawn UNDER the track ----
  if (show.uncertainty && !is.null(track) && "error" %in% names(track) && nrow(track) >= 1)
    .drawErrorCorridor(track$lon, track$lat, track$error, mean(ext$ylim))

  # --- pseudo-track ----------------------------------------------------------------------------------
  if (!is.null(track) && nrow(track) >= 2) {
    n <- nrow(track)
    if (!is.null(color.by) && !is.null(color_range) && "value" %in% names(track)) {
      segv <- (track$value[-n] + track$value[-1]) / 2                                   # per-segment value
      idx  <- pmax(1L, pmin(length(ramp), round(.rescale(segv, from = color_range, to = c(1, length(ramp))))))
      segcol <- ramp[idx]; segcol[is.na(segv)] <- pal[["track"]]
      graphics::segments(track$lon[-n], track$lat[-n], track$lon[-1], track$lat[-1], col = segcol, lwd = 1.6)
    } else {
      graphics::lines(track$lon, track$lat, col = pal[["track"]], lwd = 1.6)
    }
    # start / end markers (legended)
    graphics::points(track$lon[1], track$lat[1], pch = 21, bg = "white",  col = ink, lwd = 0.6, cex = cex * 1.2)
    graphics::points(track$lon[n], track$lat[n], pch = 21, bg = ink,      col = "white", lwd = 0.6, cex = cex * 1.2)
  }

  # --- genuine fixes, by type ------------------------------------------------------------------------
  .pts <- function(sel, ...) if (any(sel)) graphics::points(fixes$lon[sel], fixes$lat[sel], ...)
  .pts(fixes$type == "FastGPS", pch = 21, bg = pal[["fastgps"]], col = "white", lwd = 0.4, cex = cex)
  .pts(fixes$type == "Argos",   pch = 22, bg = pal[["argos"]],   col = "white", lwd = 0.4, cex = cex)
  .pts(fixes$type == "User",    pch = 24, bg = pal[["user"]],    col = "white", lwd = 0.4, cex = cex * 1.1)

  # --- deploy / pop-up anchors -----------------------------------------------------------------------
  if (!is.null(deploy)) graphics::points(deploy$lon, deploy$lat, pch = 23, bg = pal[["deploy"]], col = "white", lwd = 0.5, cex = cex * 1.5)
  if (!is.null(popup))  graphics::points(popup$lon,  popup$lat,  pch = 23, bg = pal[["popup"]],  col = "white", lwd = 0.5, cex = cex * 1.5)

  # --- legend + colour bar ---------------------------------------------------------------------------
  .trackLegend(fixes, track, deploy, popup, pal, ink, cex)
  if (!is.null(color.by) && !is.null(color_range) && !is.null(track) && "value" %in% names(track))
    .trackColorbar(ramp, color_range, .defaultColorLabel(color.by), ink, axcol, cex)

  .mapScalebar(label.cex = cex * 0.7)
  invisible(NULL)
}


#######################################################################################################
# Internal: panel sub-drawers #########################################################################
#######################################################################################################

# A one-line panel subtitle: fix counts and (if present) the track duration, correctly in hours/days.
#' @keywords internal
#' @noRd
.trackSubtitle <- function(fixes, track) {
  parts <- sprintf("%d fix%s", nrow(fixes), if (nrow(fixes) != 1) "es" else "")
  if (!is.null(track) && nrow(track) >= 2) parts <- c(parts, sprintf("%s track points", format(nrow(track), big.mark = ",")))
  paste(parts, collapse = "   |   ")
}

# Translucent uncertainty corridor: filled disks whose radius is the per-sample pseudo_error, drawn at a
# capped stride so the corridor bulges where the reckoning has drifted and pinches at the fixes. `err` is
# in metres; converted to degrees with a cos(lat) longitude correction.
#' @keywords internal
#' @noRd
.drawErrorCorridor <- function(lon, lat, err, mid_lat, max.disks = 60L) {
  ok <- is.finite(lon) & is.finite(lat) & is.finite(err) & err > 0
  if (!any(ok)) return(invisible(NULL))
  lon <- lon[ok]; lat <- lat[ok]; err <- err[ok]
  n <- length(lon)
  idx <- if (n > max.disks) unique(round(seq(1L, n, length.out = max.disks))) else seq_len(n)
  ang <- seq(0, 2 * pi, length.out = 24)
  coslat <- cos(mid_lat * pi / 180); if (!is.finite(coslat) || coslat <= 0) coslat <- 1
  fill <- grDevices::adjustcolor("#6C7A89", alpha.f = 0.14)
  for (k in idx) {
    dlat <- (err[k] / 111320)
    dlon <- dlat / coslat
    graphics::polygon(lon[k] + dlon * cos(ang), lat[k] + dlat * sin(ang), col = fill, border = NA)
  }
  invisible(NULL)
}

# Compact in-panel legend (top-left) listing only the elements actually drawn.
#' @keywords internal
#' @noRd
.trackLegend <- function(fixes, track, deploy, popup, pal, ink, cex) {
  lab <- character(0); pch <- integer(0); pcol <- character(0); pbg <- character(0); lty <- integer(0); lwd <- numeric(0)
  add <- function(l, pc, co, bg = NA, lt = NA, lw = NA) {
    lab[[length(lab) + 1L]] <<- l; pch[[length(pch) + 1L]] <<- pc; pcol[[length(pcol) + 1L]] <<- co
    pbg[[length(pbg) + 1L]] <<- bg; lty[[length(lty) + 1L]] <<- lt; lwd[[length(lwd) + 1L]] <<- lw }
  if (any(fixes$type == "FastGPS")) add(sprintf("FastGPS (%d)", sum(fixes$type == "FastGPS")), 21, "white", pal[["fastgps"]])
  if (any(fixes$type == "Argos"))   add(sprintf("Argos (%d)",   sum(fixes$type == "Argos")),   22, "white", pal[["argos"]])
  if (any(fixes$type == "User"))    add(sprintf("User (%d)",    sum(fixes$type == "User")),    24, "white", pal[["user"]])
  if (!is.null(track) && nrow(track) >= 2) {
    add("track", NA_integer_, pal[["track"]], NA, 1L, 1.6)
    add("start", 21, ink, "white"); add("end", 21, "white", ink)
  }
  if (!is.null(deploy)) add("deployment", 23, "white", pal[["deploy"]])
  if (!is.null(popup))  add("pop-up", 23, "white", pal[["popup"]])
  if (!length(lab)) return(invisible(NULL))
  graphics::legend("topleft", legend = lab, pch = pch, col = pcol, pt.bg = pbg, lty = lty, lwd = lwd,
                   bty = "o", bg = "#FFFFFFCC", box.col = "#CCCCCC", pt.lwd = 0.5, pt.cex = cex * 1.1,
                   cex = cex * 0.62, y.intersp = 1.1, inset = 0.015, seg.len = 1.4)
  invisible(NULL)
}

# Compact horizontal colour bar (bottom-right corner) for the color.by scale.
#' @keywords internal
#' @noRd
.trackColorbar <- function(ramp, color_range, label, ink, axcol, cex) {
  if (!all(is.finite(color_range)) || diff(color_range) <= 0) return(invisible(NULL))
  usr <- graphics::par("usr")
  w <- 0.30 * (usr[2] - usr[1]); h <- 0.022 * (usr[4] - usr[3])
  x0 <- usr[2] - w - 0.04 * (usr[2] - usr[1]); y0 <- usr[3] + 0.06 * (usr[4] - usr[3])
  xs <- seq(x0, x0 + w, length.out = length(ramp) + 1L)
  graphics::rect(xs[-length(xs)], y0, xs[-1], y0 + h, col = ramp, border = NA)
  graphics::rect(x0, y0, x0 + w, y0 + h, border = "#888888", lwd = 0.6)
  labs <- pretty(color_range, 3); labs <- labs[labs >= color_range[1] & labs <= color_range[2]]
  at <- x0 + w * (labs - color_range[1]) / diff(color_range)
  graphics::segments(at, y0, at, y0 - h * 0.4, col = axcol)
  graphics::text(at, y0 - h * 0.6, labels = labs, adj = c(0.5, 1), cex = cex * 0.6, col = axcol)
  graphics::text(x0 + w / 2, y0 + h * 1.6, labels = label, adj = c(0.5, 0), cex = cex * 0.62, col = ink)
  invisible(NULL)
}


#######################################################################################################
# Internal: bathymetry (opt-in, fetched once) #########################################################
#######################################################################################################

# Fetch a NOAA bathymetry grid covering [xlim] x [ylim] once for the whole run. Network call; returns a
# marmap `bathy` object or NULL on failure (a graceful, reported skip).
#' @keywords internal
#' @noRd
.fetchBathy <- function(xlim, ylim, resolution, lvl) {
  tryCatch({
    bathy <- NULL
    # capture.output() (not sink()) mutes getNOAA.bathy's progress prints exception-safely
    utils::capture.output(suppressMessages(
      bathy <- marmap::getNOAA.bathy(lon1 = xlim[1], lon2 = xlim[2], lat1 = ylim[1], lat2 = ylim[2],
                                     resolution = resolution, keep = FALSE)))
    bathy
  }, error = function(e) { .log_skip(lvl, "bathymetry unavailable: ", conditionMessage(e)); NULL })
}

# Draw bathymetry contours in lon/lat over the current panel (so they co-register with the data).
#' @keywords internal
#' @noRd
.drawBathy <- function(bathy, xlim, ylim) {
  lon <- as.numeric(rownames(bathy)); lat <- as.numeric(colnames(bathy))
  z <- unclass(bathy); z[z > 0] <- NA                                    # sea only
  lv <- pretty(range(z, na.rm = TRUE), 6); lv <- lv[lv < 0]
  if (length(lv))
    graphics::contour(lon, lat, z, levels = lv, add = TRUE, drawlabels = TRUE,
                      col = "#9DB4C0", lwd = 0.4, labcex = 0.5, method = "edge")
  invisible(NULL)
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
