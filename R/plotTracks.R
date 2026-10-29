#######################################################################################################
# Plot reconstructed movement tracks ##################################################################
#######################################################################################################

#' Plot position fixes and reconstructed movement tracks
#'
#' @description
#' Maps recorded position fixes, deployment and pop-up locations, and dead-reckoned movement tracks,
#' with one panel per deployment. Reconstructed tracks can be coloured by depth or speed and displayed
#' with their model-based horizontal uncertainty scale.
#'
#' The function supports visual assessment of reconstruction geometry and its relationship to observed
#' locations and geographic features. It can also display deployments carrying position fixes but no
#' reconstructed track. Mapping does not screen locations, alter the reconstruction or establish its
#' accuracy; use [filterLocations()] and [crossValidateTrack()] for those separate steps.
#'
#' @param data A tag dataset, a list of tag datasets, a data frame containing multiple deployments
#'   identified by `id.col`, or a character vector of `.rds` file paths. The output of
#'   [reconstructTrack()] is recommended. File inputs are read sequentially; the reduced plotting
#'   records are retained for rendering. See Details for the sources of track coordinates and fixes.
#' @param color.by Variable used to colour reconstructed track segments: `NULL` (default, a single
#'   colour), `"depth"` (`pseudo_depth`, in metres) or `"speed"` (`speed_dr`, in m/s). The colour scale
#'   is shared across panels. Missing channels and missing segment values use the single track colour.
#' @param show.uncertainty Logical; whether to draw the uncertainty overlay from `pseudo_error`, in
#'   metres (default `TRUE`). Has no effect when that column is absent or has no positive finite values.
#' @param basemap Background canvas: `"land"` (default, filled vector land over a uniform sea),
#'   `"bathymetry"` (shaded sea-floor relief), `"satellite"` (imagery tiles), or `"none"` (uniform sea
#'   without a coastline). Alternatively, supply a pre-fetched three-band RGB `terra::SpatRaster` or a
#'   `bathy` grid, as returned by [getBasemap()]. See Details for coordinate and dependency requirements.
#' @param coastline Vector coastline to draw: `"auto"` (default, the highest-resolution usable
#'   installed source), `"high"` (`mapdata::worldHires`), `"low"` (`maps::world`), or `"none"`.
#'   Alternatively, supply a spatial object of class `sf`, `sfc` or `sfg`, a longitude/latitude data
#'   frame or matrix with `NA`-separated rings, or a path to a spatial file. Coordinates must already
#'   be in the map's geographic reference frame. Ignored when `basemap = "none"`.
#' @param basemap.control A control object from [basemapControl()], or a named list of its arguments,
#'   specifying the tile provider, zoom and cache settings. Used only for an automatic satellite fetch;
#'   ignored for a pre-fetched canvas.
#' @param bathy.contours Bathymetric contour overlay: `FALSE` (default) or `NULL` disables it, `TRUE`
#'   selects isobaths automatically, and a finite numeric vector specifies contour depths in negative
#'   metres, for example `c(-50, -200, -1000)`. Can be used with any canvas. A bathymetry canvas and
#'   contours share the same depth grid; see Details for fetching and reuse.
#' @param theme A control object from [plotTheme()], or a named list of its arguments, specifying
#'   text and axis colours, panel styling, marker outlines, font family, text scale (`cex`) and the
#'   sequential colour ramp for `color.by`. Default `plotTheme()`.
#' @param colors Optional named character vector overriding map-element colours. Recognised names are
#'   `fastgps`, `argos`, `user`, `track`, `deploy`, `popup`, `sea`, `sea.deep`, `land`, `land.border`,
#'   `bathymetry`, `uncertainty`, `start` and `end`. `sea` and `sea.deep` set the shallow and deep ends
#'   of the bathymetric relief ramp; `start` and `end` set the endpoint marker fills. Unspecified entries
#'   retain their defaults. Unknown names and invalid colour values are rejected. Default `NULL`.
#' @param max.points Target maximum number of reconstructed positions retained for drawing per
#'   deployment (default `5000`, at least `2`). Stride-based thinning preserves the first and last
#'   finite positions; retaining the last position can add one point beyond this target. It reduces
#'   rendering time and PDF size without changing the input data or thinning the recorded fixes.
#' @param ncols,nrows Number of panel columns and rows, or `NULL` (default). With both `NULL`, the
#'   grid has at most two columns and five rows per page. With one supplied, the other is inferred to
#'   fit all panels; with both supplied, additional panels are paginated at that fixed capacity.
#' @param plot Logical; whether to draw maps on the active graphics device (default `TRUE`).
#' @param plot.file Path to a multi-page PDF, or `NULL` (default), which writes no file. The path must
#'   end in `.pdf` and its parent directory must exist. Independent of `plot`: setting `plot = FALSE`
#'   writes only the PDF. At least one output destination must be enabled.
#' @param id.col Name of the deployment-identifier column used to split a single input table (default
#'   `"ID"`). Panel labels use the deployment identifier in the tag metadata where available.
#' @param datetime.col Name of the timestamp column used to order reconstructed positions (default
#'   `"datetime"`). `POSIXct` is recommended. If absent or not a supported time representation, row
#'   order is retained; character timestamps are not parsed.
#' @param verbose How much detail to print: `0`/`"quiet"`, `1`/`"normal"` (header, layout and summary),
#'   or `2`/`"detailed"` (default), which adds loading progress and skipped-deployment counts.
#' @param events Optional data frame of event windows with `ID`, `event`, `start` and `end` (`POSIXct`)
#'   columns, for example from [detectCircling()]. Default `NULL` draws no event layer. Matching uses
#'   deployment IDs and timestamps, not spatial proximity. Event paths use `theme$palette` and are
#'   gathered independently before background thinning; each valid event subpath uses the `max.points`
#'   target. Missing coordinates, non-increasing timestamps and gaps exceeding four median positive
#'   sampling intervals break overlays. No positions are extrapolated or assigned across gaps.
#'   Extra deployment IDs are ignored for subset plots; a table whose IDs and detector roster do not
#'   match any input deployment raises a warning.
#'
#' @details
#' ## Workflow and position sources
#'
#' Apply the function after [reconstructTrack()] to inspect the horizontal projection of a
#' dead-reckoned track. Both `pseudo_lon` and `pseudo_lat` are required to draw it. Rows with non-finite
#' reconstructed coordinates are omitted, and the retained positions are ordered by the timestamp
#' column where usable. Lines connect the retained positions, including across omitted rows; they do
#' not identify or repair recording gaps. A single reconstructed position contributes to the map extent
#' but does not produce a track line or endpoint markers.
#' Event overlays, when requested, instead break at unavailable positions and recording gaps. They
#' highlight samples inside the supplied intervals; boundaries between samples are not interpolated.
#' A single matching position is drawn as a point. Windows without matching positions are reported.
#' Highlighted positions remain model-dependent pseudo-locations, not observed event locations or
#' independent validation of a detector using the same heading channel.
#'
#' Recorded fixes are read from the canonical ancillary position table, accessible through
#' `getTagMetadata(x)$ancillary$positions$data`. FastGPS, Argos and user-supplied positions have distinct
#' symbols. Deployment and pop-up coordinates are read from the deployment metadata. Fixes remain at
#' their own cadence and are not automatically trimmed to the sensor-recording interval or screened for
#' location quality. Sample-level `lon` and `lat` columns alone are not used as the recorded-fix layer.
#'
#' Deployments without a reconstructed position or a row in the ancillary position table are skipped,
#' even if deployment or pop-up coordinates are present. Skipped deployments remain in the returned
#' plotting summary, with `drawn = FALSE`; no deployment-exclusion log is written.
#'
#' ## Coordinate system and map layers
#'
#' Coordinates are interpreted as WGS84 longitude and latitude in decimal degrees. The display uses
#' longitude/latitude axes with an aspect correction at each panel's central latitude, not a general
#' map-projection transformation. Custom coastlines and pre-fetched canvases are drawn in their supplied
#' coordinates and must already use this reference frame. The function does not unwrap longitudes at
#' the antimeridian; broad or high-latitude extents require particular care in interpretation.
#'
#' Each panel has its own padded extent, derived from its fixes, reconstructed positions and metadata
#' anchors. A canvas, vector coastline and optional bathymetric contours are drawn as separate layers.
#' Land is filled over the default sea and bathymetric relief, and outlined over imagery.
#' `basemap = "none"` suppresses the coastline but does not disable requested contours or uncertainty.
#'
#' The default land canvas requires no network access. Automatic coastline selection prefers a usable
#' `worldHires` database from \pkg{mapdata}, falls back to `maps::world` with an informative message,
#' and draws no coastline if neither source is available. An explicit `coastline = "high"` request
#' errors if the high-resolution database cannot be used. Longitude/latitude tables and matrices need
#' no spatial package; spatial objects and non-RDS spatial files require \pkg{sf}.
#'
#' Fetching bathymetry requires \pkg{marmap}; fetching satellite tiles requires \pkg{maptiles},
#' \pkg{terra} and \pkg{sf}. Bathymetry and imagery are fetched for the combined deployment extent and
#' reused across panels. Bathymetric resolution is chosen from that extent, while tile settings are
#' controlled by `basemap.control`. A pre-fetched `bathy` grid avoids the depth download and can also
#' supply contours without \pkg{marmap}; a pre-fetched RGB raster requires \pkg{terra} but no tile
#' download. Failed downloads are reported at non-quiet verbosity and the remaining layers are drawn.
#' When publishing imagery, check the provider's terms and retain its attribution.
#'
#' ## Track colouring and positional uncertainty
#'
#' Depth and speed colouring use the retained plotting positions to establish one range for the whole
#' call. Segment colours represent the mean of their two endpoint values. If no finite range exists or
#' the channel is constant across the call, tracks use the single `track` colour instead. The sequential
#' ramp comes from `theme$sequential`; the named `colors` overrides affect map elements rather than the
#' qualitative series palette in the theme.
#'
#' `pseudo_error` from [reconstructTrack()] is a model-based horizontal error scale, expressed as a
#' nominal one-standard-deviation uncertainty in metres. It is displayed using translucent disks at a
#' subset of retained positions, with radius equal to the supplied error scale. This is an approximate
#' visual envelope, not a validated confidence region or an estimate of the precision of the recorded
#' fixes. Missing, zero or negative error values contribute no disk. See [reconstructTrack()] and
#' [crossValidateTrack()] for the assumptions and empirical assessment of reconstruction uncertainty.
#'
#' @return Invisibly, a data frame with one row per input deployment and columns:
#'   \describe{
#'     \item{`id`}{Deployment identifier used for the panel.}
#'     \item{`n_fix`}{Number of rows in the ancillary position table, excluding metadata anchors.}
#'     \item{`n_track`}{Number of finite reconstructed positions retained after plotting-only thinning.}
#'     \item{`drawn`}{Logical; whether the deployment was included in the panel set.}
#'   }
#'   Maps are drawn on the active device and/or saved to `plot.file`. Input datasets and their
#'   metadata are not modified. An empty input collection is rejected; a collection with no drawable
#'   records returns its summary with all `drawn` values `FALSE` and produces no map pages.
#'
#' @seealso [reconstructTrack()] for track estimation; [crossValidateTrack()] for reconstruction
#'   validation; [trackMetrics()] for path summaries; [filterLocations()] for location screening;
#'   [getBasemap()] and [basemapControl()] for background layers; [plotTheme()] for plot styling;
#'   [plotDepthProfiles()] for depth time series; [detectCircling()] for candidate event windows.
#'
#' @examples
#' # Illustrative reconstructed positions; no external files or map downloads are needed
#' n <- 20
#' track <- data.frame(
#'   ID = "deployment_01",
#'   datetime = as.POSIXct("2023-01-01", tz = "UTC") + seq_len(n) * 60,
#'   pseudo_lon = seq(-25.2, -25.1, length.out = n),
#'   pseudo_lat = 37 + 0.01 * sin(seq(0, pi, length.out = n)),
#'   pseudo_depth = seq(0, 50, length.out = n)
#' )
#' map_file <- tempfile(fileext = ".pdf")
#' map_summary <- plotTracks(
#'   track, color.by = "depth", basemap = "none",
#'   plot = FALSE, plot.file = map_file, verbose = "quiet"
#' )
#' map_summary
#' unlink(map_file)
#'
#' \dontrun{
#' # Map reconstructed deployments and save a report; ./plots must already exist
#' tracks <- reconstructTrack(processed)
#' plotTracks(tracks, color.by = "depth", plot.file = "./plots/tracks.pdf")
#' circles <- detectCircling(processed)
#' plotTracks(tracks, events = circles)
#'
#' # Reuse a fetched canvas for subsequent figures
#' canvas <- getBasemap(tracks, type = "satellite")
#' plotTracks(tracks, basemap = canvas, coastline = "high")
#' }
#' @export


plotTracks <- function(data,
                       color.by         = NULL,
                       show.uncertainty = TRUE,
                       basemap          = c("land", "bathymetry", "satellite", "none"),
                       coastline        = "auto",
                       basemap.control  = basemapControl(),
                       bathy.contours   = FALSE,
                       theme            = plotTheme(),
                       colors           = NULL,
                       max.points       = 5000,
                       ncols            = NULL,
                       nrows            = NULL,
                       plot             = TRUE,
                       plot.file        = NULL,
                       id.col           = "ID",
                       datetime.col     = "datetime",
                       verbose          = "detailed",
                       events           = NULL) {

  ##############################################################################
  # Validate arguments #########################################################
  ##############################################################################

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)

  if (!is.null(color.by)) .assert_choice(color.by, "color.by", c("depth", "speed"))
  .assert_flag(show.uncertainty, "show.uncertainty")
  # canvas: "land", "none", "bathymetry", "satellite", or a pre-fetched raster passed straight in
  basemap.control <- .as_control(basemap.control, basemapControl, "nautilus_basemap", "basemap.control")
  bm <- .resolveBasemap(basemap, c("land", "bathymetry", "satellite", "none"))
  # bathymetry contour overlay: FALSE | TRUE (auto isobaths) | numeric depths (explicit isobaths)
  bathy_levels <- .resolveBathyContours(bathy.contours)         # NULL when off; numeric(0) when auto
  bathy_on     <- !is.null(bathy_levels)
  theme <- .as_control(theme, plotTheme, "nautilus_theme", "theme")
  events <- .eventIntervals(events)
  event.palette <- if (!is.null(events)) .eventPalette(events, theme) else NULL
  .assert_count(max.points, "max.points", min = 2L)
  if (!is.null(ncols)) .assert_count(ncols, "ncols", min = 1L)
  if (!is.null(nrows)) .assert_count(nrows, "nrows", min = 1L)
  .assert_flag(plot, "plot")
  .assert_writable_file(plot.file, "plot.file", ext = "pdf")   # fail-fast: parent dir must exist
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  if (!plot && is.null(plot.file))
    .abort(c("Nothing to plot.", "i" = "Set {.arg plot = TRUE} or provide a {.arg plot.file}."))
  # marmap is only needed to FETCH a grid: a pre-fetched one (basemap = <bathy>) already carries the depths
  if (bathy_on && is.null(bm$bathy) && !requireNamespace("marmap", quietly = TRUE))
    .abort(c("{.arg bathy.contours} needs the {.pkg marmap} package.",
             "i" = "Install it with {.code install.packages(\"marmap\")}, pass a pre-fetched grid from {.fn getBasemap}, or set {.arg bathy.contours = FALSE}."))
  # resolve the coastline source ONCE (emits the low-res hint / explicit-request error a single time).
  # It is FILLED land under the vector "land" canvas and over the bathymetric relief (which paints only
  # the sea, so land must be drawn on top), and an OUTLINE over imagery (which already shows the land);
  # under "none" no coastline is drawn.
  coast_fill <- bm$kind %in% c("land", "bathymetry")
  coast_draw <- if (bm$kind %in% c("land", "bathymetry", "satellite", "raster"))
                  .resolveCoastline(coastline, lvl) else list(kind = "none")

  # Semantic MAP palette: one colour per map ELEMENT, deliberately not folded into `theme$palette` (which
  # is a qualitative SERIES palette for telling n categories apart - a track is not "category 3"). What a
  # theme slot genuinely describes is taken from the theme instead and never appears here: titles (ink),
  # axes (axis), panel/gridline chrome (panel, grid), marker outlines (bar.border), text scale and font.
  pal <- c(fastgps = "#2AA7A0", argos = "#5B7FBD", user = "#7E57C2", track = "#C0392B",
           deploy = "#1D9E75", popup = "#E8A33D", sea = "#EAF1F6", land = "#D9D2C5",
           land.border = "#B8AE9C", bathymetry = "#9DB4C0", uncertainty = "#6C7A89",
           # the deep end of the bathymetric-relief ramp; `sea` is its shallow end, so overriding either
           # end via `colors` re-tints the whole depth canvas (basemap = "bathymetry")
           sea.deep = "#2C4A63",
           # start/end are a CONTRAST PAIR, not chrome: the map reads them as light-disk versus
           # dark-disk. Kept here so no theme preset can collapse one into the other.
           start = "#FFFFFF", end = "#111111")
  if (!is.null(colors)) {
    if (!is.character(colors) || is.null(names(colors)) || !all(nzchar(names(colors))))
      .abort("{.arg colors} must be a NAMED character vector (e.g. {.code c(track = \"red\")}).")
    unknown <- setdiff(names(colors), names(pal))
    if (length(unknown))
      .abort(c("{.arg colors} has {length(unknown)} unrecognised name{?s}: {.val {unknown}}.",
               "i" = "Recognised names: {.val {names(pal)}}."))
    # Validate the VALUES, not just the names: an unchecked typo travels all the way into grDevices
    # mid-render and surfaces as a bare 'invalid color name', naming neither the argument nor the entry.
    bad <- colors[!vapply(colors, .isColour, logical(1))]
    if (length(bad))
      .abort(c("{.arg colors} has {length(bad)} entr{?y/ies} that {?is/are} not a valid colour: {.val {bad}}.",
               "i" = "Offending name{?s}: {.val {names(bad)}}.",
               "i" = "Use a colour name from {.fn grDevices::colors} or a hex string such as {.val #C0392B}."))
    pal[names(colors)] <- colors
  }

  ##############################################################################
  # Gather each deployment's track + fixes #####################################
  ##############################################################################

  src <- .resolveInput(data, id.col)
  .log_header(lvl, "plotTracks", "Mapping surface fixes and dead-reckoned tracks",
              bullets = sprintf("Input: %d deployment%s%s", src$n, if (src$n != 1) "s" else "",
                                if (!is.null(color.by)) paste0(" ", cli::symbol$bullet, " coloured by ", color.by) else ""))

  payloads <- list(); summary_rows <- vector("list", src$n)
  n_empty <- 0L; unmapped <- character(0)
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
    event.rows <- .eventsForDeployment(events, id)
    event.paths <- .eventTrackPaths(x, event.rows, datetime.col, max.points)
    if (length(event.paths$matched) && any(!event.paths$matched))
      unmapped <- c(unmapped, sprintf("%s: %d windows without reconstructed positions", id, sum(!event.paths$matched)))

    n_fix <- nrow(fixes); n_track <- if (is.null(track)) 0L else nrow(track)
    summary_rows[[i]] <- data.frame(id = id, n_fix = n_fix, n_track = n_track, drawn = FALSE,
                                    stringsAsFactors = FALSE)
    if (n_fix == 0L && n_track == 0L) { n_empty <- n_empty + 1L; next }

    summary_rows[[i]]$drawn <- TRUE
    payloads[[length(payloads) + 1L]] <- list(id = id, fixes = fixes, track = track,
                                              deploy = deploy, popup = popup,
                                              event.paths = event.paths$paths, event.palette = event.palette)
  }
  .log_progress_done(pb)
  summary_df <- do.call(rbind, summary_rows)
  .warnUnmatchedEvents(events, summary_df$id)
  .warn_grouped("Some event windows could not be mapped to reconstructed positions.", unmapped)

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

  # union extent: a SINGLE bathymetry and/or satellite fetch for the whole run, drawn clipped per panel.
  # ONE marmap grid serves both the relief canvas and the contour overlay, so asking for both costs one
  # download - which is exactly why the two are separate arguments over a shared layer.
  bathy <- NULL; tile_rast <- NULL; tile_credit <- NULL
  relief_on  <- identical(bm$kind, "bathymetry")
  need_tiles <- bm$kind %in% c("satellite", "raster")
  if (bathy_on || relief_on || need_tiles) {
    allx <- unlist(lapply(payloads, function(p) c(p$fixes$lon, p$track$lon, p$deploy$lon, p$popup$lon)))
    ally <- unlist(lapply(payloads, function(p) c(p$fixes$lat, p$track$lat, p$deploy$lat, p$popup$lat)))
    ext  <- .equalAspectExtent(allx, ally, f = 0.3)
    if (!is.null(ext)) {
      if (relief_on && !is.null(bm$bathy)) bathy <- bm$bathy            # pre-fetched grid, passed straight in
      else if (bathy_on || relief_on) bathy <- .fetchBathy(ext$xlim, ext$ylim, lvl)  # resolution auto from extent
      if (identical(bm$kind, "satellite")) tile_rast <- .fetchTiles(ext$xlim, ext$ylim, basemap.control, lvl)
      if (identical(bm$kind, "raster"))    tile_rast <- bm$raster
      tile_credit <- if (!is.null(tile_rast)) attr(tile_rast, "nautilus.credit", exact = TRUE) %||% bm$credit
    }
  }

  ##############################################################################
  # Draw (paginated) to the caller's device and/or a multi-page PDF ############
  ##############################################################################

  lay <- .autoGrid(length(payloads), ncols, nrows)
  if (lvl >= 1L)
    .log_arrow(lvl, sprintf("layout: %d x %d per page%s", lay$nrows, lay$ncols,
                            if (length(lay$pages) > 1) sprintf(" %s %d pages", cli::symbol$bullet, length(lay$pages)) else ""))

  draw <- function(to.file = FALSE, unicode = TRUE) {
    # the theme's typeface has to be set on the DEVICE (base graphics has no per-call family for titles
    # and legends alike); restored on exit so the caller's par() is never left mutated
    old <- graphics::par(family = theme$font.family, oma = c(0.5, 0.5, 0.5, 0.5))
    on.exit(graphics::par(old), add = TRUE)
    for (pg in lay$pages) {
      graphics::par(mfrow = c(lay$nrows, lay$ncols))
      for (k in pg) .plotTrackPanel(payloads[[k]], pal = pal, theme = theme, color.by = color.by,
                                    color_range = color_range, ramp = ramp, bathy = bathy,
                                    bathy.levels = bathy_levels, bathy.relief = relief_on,
                                    bathy.contours = bathy_on, coast_spec = coast_draw,
                                    coast_fill = coast_fill, tile_rast = tile_rast,
                                    tile_credit = tile_credit, show.uncertainty = show.uncertainty)
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
