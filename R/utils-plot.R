#######################################################################################################
# Shared internal plotting utilities ##################################################################
#######################################################################################################

# Device-handling primitives shared by the nautilus plotters (cairo-with-fallback, Unicode probing, the
# caller's-device/PDF contract), plus a default qualitative palette and the per-deployment group
# resolver. These previously lived in plot-utils.R alongside plotTheme(); the exported theme now has its
# own file (plotTheme.R) and this file holds only the internal machinery.

#' Does `cairo_pdf()` actually work at runtime?
#'
#' `capabilities("cairo")` only reports whether R was *built* with cairo; on headless machines the
#' cairo library can still fail to load when `cairo_pdf()` is actually called ("failed to load cairo
#' DLL"). This probes for real by opening (and closing) a throwaway cairo device.
#' @keywords internal
#' @noRd
.cairoOk <- function() {
  if (!isTRUE(capabilities("cairo"))) return(FALSE)
  tryCatch({
    f <- tempfile(fileext = ".pdf"); on.exit(unlink(f), add = TRUE)
    suppressWarnings(grDevices::cairo_pdf(f))
    ok <- identical(names(grDevices::dev.cur()), "cairo_pdf")
    if (ok) grDevices::dev.off()
    ok
  }, error = function(e) FALSE)
}

#' Open a PDF device for `path`, preferring `cairo_pdf` (reliable Unicode glyphs) but degrading to base
#' `pdf()` when cairo is unavailable OR fails to start at runtime. Leaves the opened device current and
#' returns `list(unicode = <the device can render multi-byte glyphs>)`.
#' @keywords internal
#' @noRd
.openPdfDevice <- function(path, width = 7, height = 7, cairo = TRUE) {
  if (cairo && isTRUE(capabilities("cairo"))) {
    ok <- tryCatch({
      suppressWarnings(grDevices::cairo_pdf(path, width = width, height = height, onefile = TRUE))
      identical(names(grDevices::dev.cur()), "cairo_pdf")   # confirm a cairo device is really current
    }, error = function(e) FALSE)
    if (isTRUE(ok)) return(list(unicode = TRUE))
  }
  grDevices::pdf(path, width = width, height = height)      # portable fallback: ASCII glyphs only
  list(unicode = FALSE)
}

#' Can the CURRENT graphics device render multi-byte (Unicode) glyphs? The base `pdf()`/`postscript()`
#' devices map text through single-byte AFM encodings and fail on non-Latin-1 characters; cairo, quartz,
#' X11(cairo), windows and RStudioGD handle Unicode. Used to decide ASCII vs Unicode for the caller's
#' (screen) device, which under a headless `R CMD check` is often a plain `pdf()` opened by `dev.new()`.
#' @keywords internal
#' @noRd
.deviceUnicode <- function() {
  nm <- names(grDevices::dev.cur())
  !is.null(nm) && !nm %in% c("pdf", "postscript", "xfig", "pictex")
}

#' Render a figure to the caller's device and/or a PDF, restoring graphics state on exit.
#'
#' `draw` renders the COMPLETE figure (all panels/pages) to the current device; it is called once per
#' target with `to.file` (TRUE for the PDF, FALSE for the caller's device) and `unicode` (TRUE when the
#' target device can render multi-byte glyphs, FALSE when only ASCII is safe). When `plot`, the figure is
#' drawn to the caller's active device (a fresh window is opened only if none exists), and the caller's
#' `par()` is saved and restored. When `plot.file` is set, the figure is also drawn to a (multi-page) PDF
#' that is closed on exit. The `on.exit` handlers are ordered so the PDF is closed BEFORE the caller's
#' device and `par()` are restored - the invariant every nautilus plotter needs.
#'
#' The PDF prefers `cairo_pdf` for reliable Unicode but falls back to base `pdf()` (with `unicode = FALSE`
#' passed to `draw`) when cairo cannot start, so the renderers never crash on a non-cairo device.
#'
#' @param draw Function `function(to.file = FALSE, unicode = TRUE)` rendering the whole figure.
#' @param plot Logical; draw to the caller's active graphics device.
#' @param plot.file Character path to a PDF, or `NULL`.
#' @param width,height Device size in inches (used for the PDF).
#' @param cairo Logical; prefer `cairo_pdf` (for reliable Unicode glyphs) when it actually works.
#' @return `invisible(NULL)`.
#' @keywords internal
#' @noRd
.renderToDevices <- function(draw, plot = TRUE, plot.file = NULL, width = 7, height = 7, cairo = FALSE) {
  # caller's screen device (open one only if the null device is current)
  caller_dev <- grDevices::dev.cur()
  if (plot && caller_dev == 1L) { grDevices::dev.new(); caller_dev <- grDevices::dev.cur() }
  if (plot) oldpar <- graphics::par(no.readonly = TRUE)

  # optional PDF: registered first so it closes FIRST on exit (before the device/par restore below)
  file_dev <- NULL; file_unicode <- FALSE
  if (!is.null(plot.file)) {
    file_unicode <- .openPdfDevice(plot.file, width = width, height = height, cairo = cairo)$unicode
    file_dev <- grDevices::dev.cur()
    on.exit(if (!is.null(file_dev) && file_dev %in% grDevices::dev.list()) grDevices::dev.off(file_dev), add = TRUE)
  }
  if (plot) on.exit({ if (caller_dev %in% grDevices::dev.list()) { grDevices::dev.set(caller_dev); graphics::par(oldpar) } }, add = TRUE)

  # render to each requested target; the caller's device may be a plain pdf() (e.g. dev.new() under a
  # headless check), so probe its real Unicode capability rather than assuming it
  if (plot)                 { grDevices::dev.set(caller_dev); draw(to.file = FALSE, unicode = .deviceUnicode()) }
  if (!is.null(file_dev))   { grDevices::dev.set(file_dev);   draw(to.file = TRUE,  unicode = file_unicode) }
  invisible(NULL)
}


#' A qualitative fill palette for distinguishing metrics/series (recycled for `n` beyond its length).
#' @keywords internal
#' @noRd
.qualitativePalette <- function(n) {
  base <- c("#4C9F9F", "#E8A33D", "#5B7FBD", "#C25B56", "#7FA65E", "#9B72AA", "#C77F9E", "#6FA8B5")
  rep(base, length.out = max(1L, n))[seq_len(max(1L, n))]
}


#######################################################################################################
# Group resolver (a shared way to attach a per-deployment grouping factor: species / sex / ...) #######
#######################################################################################################

#' The grouping label for one loaded deployment, from a flexible `group` spec.
#'
#' `group` may be: `NULL` (ungrouped); a length-1 column name resolved from the DATA (a column) or the
#' tag METADATA (`deployment`/`tag` fields, e.g. mapped at import via \link{metadataColumns}); a named
#' vector/list mapping deployment id -> group; or a two-column data.frame (id, group). This one resolver
#' is shared by the grouped plotters so grouping behaves identically everywhere.
#' @keywords internal
#' @noRd
.deploymentGroup <- function(x, id, group) {
  if (is.null(group)) return(NA_character_)
  if (is.data.frame(group)) {
    if (ncol(group) < 2) .abort("A data.frame {.arg group} needs two columns: deployment id and group.")
    hit <- as.character(group[[2]])[match(id, as.character(group[[1]]))]
    return(if (length(hit)) hit else NA_character_)
  }
  if (!is.null(names(group)) && is.atomic(group)) return(as.character(unname(group[id])))
  if (is.character(group) && length(group) == 1L) {
    if (group %in% names(x)) { v <- x[[group]]; v <- v[!is.na(v)]; return(if (length(v)) as.character(v[1]) else NA_character_) }
    m <- tryCatch(.getMeta(x), error = function(e) NULL)
    for (slot in list(m$biometrics, m$deployment, m$tag))     # biometrics first: the primary trait source
      if (!is.null(slot) && !is.null(slot[[group]])) return(as.character(slot[[group]][1]))
    return(NA_character_)
  }
  .abort("{.arg group} must be a column name, a named id->group vector, or a two-column data.frame.")
}


#######################################################################################################
# Shared map primitives (used by the geographic plotters: plotTracks, filterLocations) ################
#######################################################################################################

#' Equal-aspect (latitude-corrected) plotting extent for a set of lon/lat points.
#'
#' Pads the data range by `f`, squares it (so the map is not distorted), and returns the aspect ratio
#' that keeps one degree of latitude and longitude visually equal at the centre latitude. A degenerate
#' (single-point / zero-span) axis is nudged to a small window so the panel still renders. Returns NULL
#' when there is nothing finite to plot.
#' @keywords internal
#' @noRd
.equalAspectExtent <- function(xs, ys, f = 0.2) {
  xs <- xs[is.finite(xs)]; ys <- ys[is.finite(ys)]
  if (!length(xs) || !length(ys)) return(NULL)
  lon <- grDevices::extendrange(xs, f = f); lat <- grDevices::extendrange(ys, f = f)
  if (diff(lon) == 0) lon <- lon + c(-0.01, 0.01)
  if (diff(lat) == 0) lat <- lat + c(-0.01, 0.01)
  half <- max(diff(lon), diff(lat)) / 2
  lon <- mean(lon) + c(-half, half); lat <- mean(lat) + c(-half, half)
  list(xlim = lon, ylim = lat, asp = 1 / cospi(mean(lat) / 180))
}


#' Resolve the `bathy.contours` argument to contour levels (or NULL when off).
#'
#' `FALSE`/`NULL` -> off (returns NULL); `TRUE` -> on with auto-chosen isobaths (returns `numeric(0)`,
#' a sentinel the drawer reads as "pick pretty levels"); a numeric vector -> those explicit isobath
#' depths (negative metres). Keeping the toggle and the levels in one argument follows the value-or-data
#' rule shared with `basemap`/`coastline`.
#' @keywords internal
#' @noRd
.resolveBathyContours <- function(x) {
  if (is.null(x) || isFALSE(x)) return(NULL)
  if (isTRUE(x)) return(numeric(0))
  if (is.numeric(x) && length(x) >= 1L && all(is.finite(x))) return(as.numeric(x))
  .abort(c("{.arg bathy.contours} must be {.code FALSE}, {.code TRUE}, or a numeric vector of depths.",
           "i" = "e.g. {.code bathy.contours = c(-50, -200, -1000)}, or {.code seq(-200, -4000, by = -200)} for a regular interval."))
}


#' Resolve and validate the `basemap` canvas keyword.
#'
#' Resolves the vector canvases (`"land"`, `"none"`), the raster ones (`"bathymetry"`, `"satellite"`)
#' and a user-supplied raster, all of which are live. The only aborts are a missing suggested package
#' (marmap, maptiles) and a raster of the wrong class, each pointing at `getBasemap()`. `choices`
#' differs by function - `filterLocations` does not offer a bathymetry canvas.
#' @keywords internal
#' @noRd
.resolveBasemap <- function(basemap, choices) {
  # A non-character basemap is a user-supplied canvas, of either raster flavour: a terra SpatRaster
  # (imagery, from getBasemap(type = "satellite")) or a marmap bathy grid (depth, from
  # getBasemap(type = "bathymetry")). Both are "fetch once, pass back" reproducibility paths.
  if (!is.character(basemap)) {
    if (inherits(basemap, "bathy"))
      return(list(kind = "bathymetry", raster = NULL, bathy = basemap, credit = NULL))
    if (!requireNamespace("terra", quietly = TRUE) || !inherits(basemap, "SpatRaster"))
      .abort(c("A user-supplied {.arg basemap} must be a {.pkg terra} {.cls SpatRaster} (imagery) or a {.pkg marmap} {.cls bathy} grid (depth).",
               "i" = "Fetch one with {.fn getBasemap}, or pass a keyword ({.val land}, {.val bathymetry}, {.val satellite}, {.val none})."))
    return(list(kind = "raster", raster = basemap, bathy = NULL,
                credit = attr(basemap, "nautilus.credit", exact = TRUE)))
  }
  bm <- match.arg(basemap, choices)
  if (identical(bm, "bathymetry") && !requireNamespace("marmap", quietly = TRUE))
    .abort(c("{.arg basemap = \"bathymetry\"} (shaded relief) needs the {.pkg marmap} package.",
             "i" = "Install it with {.code install.packages(\"marmap\")}, pass a pre-fetched grid from {.fn getBasemap}, or use {.val land}."))
  if (identical(bm, "satellite")) {
    if (!requireNamespace("maptiles", quietly = TRUE))
      .abort(c("{.arg basemap = \"satellite\"} needs the {.pkg maptiles} package.",
               "i" = "Install it with {.code install.packages(\"maptiles\")}, pass a pre-fetched raster from {.fn getBasemap}, or use {.val land}."))
    return(list(kind = "satellite", raster = NULL, bathy = NULL, credit = NULL))
  }
  list(kind = bm, raster = NULL, bathy = NULL, credit = NULL)   # "land", "bathymetry" or "none"
}


#' Persistent cache directory for fetched basemap tiles (from a `basemapControl(cache=)` value).
#' @keywords internal
#' @noRd
.tileCacheDir <- function(cache) {
  if (isFALSE(cache)) return(NULL)                               # maptiles uses a session tempdir
  dir <- if (isTRUE(cache)) tools::R_user_dir("nautilus", "cache") else cache
  if (!dir.exists(dir)) dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  dir
}


#' Fetch a raster basemap (satellite/imagery tiles) covering [xlim] x [ylim], reprojected to lon/lat.
#'
#' Tiles arrive in Web Mercator (EPSG:3857); reprojecting to EPSG:4326 is what lets them co-register with
#' the WGS84 degree-based drawing - the exact step the original OpenStreetMap basemap omitted. A network
#' call, guarded and cached; returns a `terra` SpatRaster (with a `nautilus.credit` attribute) or NULL on
#' a reported failure. Needs \pkg{maptiles} + \pkg{terra} + \pkg{sf}.
#' @keywords internal
#' @noRd
#' Slippy-map tile indices for a lon/lat pair at zoom `z` (standard Web Mercator scheme).
#' @keywords internal
#' @noRd
.tileIndex <- function(lon, lat, z) {
  n   <- 2^z
  lat <- pmax(-85.05112878, pmin(85.05112878, lat))
  phi <- lat * pi / 180
  c(x = (lon + 180) / 360 * n,
    y = (1 - log(tan(phi) + 1 / cos(phi)) / pi) / 2 * n)
}


#' Choose a tile zoom from the extent and the size the basemap will actually be DRAWN at.
#'
#' maptiles' own default is the wrong question for a figure. Its `get_zoom()` picks the largest zoom
#' that still covers the area in four tiles and adds one, which is sized for a cheap request rather than
#' for print: on a 54 x 68 km extent it returns zoom 9, i.e. 244 m/px, 260 px across, about 60 dpi in a
#' `plotTracks()` panel - islands render as blurred blobs. Resolution is the whole point of a satellite
#' basemap, so the target here is pixels on the page (~`target.px` across the panel), bounded by a tile
#' budget so a wide extent cannot silently issue thousands of requests.
#' @param xlim,ylim Lon/lat extent.
#' @param target.px Pixels wanted across the drawn width. 1200 is ~280 dpi in a 4.3 in panel.
#' @param max.tiles Ceiling on total tiles fetched.
#' @return A list of `zoom`, the resulting `px` and `tiles`, and `capped` (TRUE when the tile budget,
#'   not the target, decided the answer - the caller reports that rather than hiding it).
#' @keywords internal
#' @noRd
.basemapZoom <- function(xlim, ylim, target.px = 1200, max.tiles = 160L) {
  lat  <- mean(ylim)
  m_w  <- abs(diff(xlim)) * 111320 * cos(lat * pi / 180)          # extent width in metres
  best <- NULL
  for (z in 1:18) {
    res    <- 156543.03392 * cos(lat * pi / 180) / 2^z            # m per pixel at this latitude
    px     <- m_w / res
    tl     <- .tileIndex(xlim[1], ylim[2], z); br <- .tileIndex(xlim[2], ylim[1], z)
    tiles  <- (floor(br[["x"]]) - floor(tl[["x"]]) + 1) * (floor(br[["y"]]) - floor(tl[["y"]]) + 1)
    if (tiles > max.tiles) break                                   # budget reached: keep the last one
    best <- list(zoom = z, px = px, tiles = tiles, capped = TRUE)
    if (px >= target.px) { best$capped <- FALSE; break }
  }
  best %||% list(zoom = 1L, px = NA_real_, tiles = 1L, capped = TRUE)
}


#' Fraction of an RGB tile raster that carries no imagery (all channels near zero).
#'
#' A provider's coverage runs out at some zoom and it then serves empty tiles rather than an error.
#' Measured on Esri.WorldImagery over the Azores: 0% empty at zoom <= 11, 5.5% at 12 and above, where
#' the open-ocean tiles go black while the coastline gains detail. Raising the zoom blindly therefore
#' trades blur for black patches, which is why `.fetchTiles()` checks this and steps back down.
#' @keywords internal
#' @noRd
.tileEmptyFraction <- function(rast) {
  a <- tryCatch(terra::as.array(rast), error = function(e) NULL)
  if (is.null(a) || length(dim(a)) < 3L || dim(a)[3] < 3L) return(0)
  mx <- apply(a[, , 1:3, drop = FALSE], c(1, 2), max)
  mean(mx < 12, na.rm = TRUE)
}


.fetchTiles <- function(xlim, ylim, control = basemapControl(), lvl = 0L) {
  for (pkg in c("maptiles", "terra", "sf"))
    if (!requireNamespace(pkg, quietly = TRUE))
      .abort(c("A satellite basemap needs the {.pkg {pkg}} package.",
               "i" = "Install {.pkg maptiles}, {.pkg terra} and {.pkg sf}, or use {.arg basemap = \"land\"}."))
  # get_tiles() wants an sf/sfc with a CRS, not a bare bbox: give it an sfc polygon of the extent
  aoi <- sf::st_as_sfc(sf::st_bbox(c(xmin = xlim[1], ymin = ylim[1], xmax = xlim[2], ymax = ylim[2]),
                                   crs = 4326))
  auto <- .basemapZoom(xlim, ylim)
  zoom <- control$zoom %||% auto$zoom
  grab <- function(z) tryCatch(
    suppressWarnings(maptiles::get_tiles(aoi, provider = control$provider, zoom = z, crop = TRUE,
                                         cachedir = .tileCacheDir(control$cache))),
    error = function(e) { .log_skip(lvl, "satellite basemap unavailable: ", conditionMessage(e)); NULL })

  rast <- grab(zoom)
  if (is.null(rast)) return(NULL)
  # Step back down where the provider has no imagery at this zoom. Only when the zoom was CHOSEN for the
  # user - an explicit control$zoom is a decision, not a suggestion, and is honoured as given.
  if (is.null(control$zoom)) {
    for (k in 1:2) {
      empty <- .tileEmptyFraction(rast)
      if (empty <= 0.02 || zoom <= 1L) break
      lower <- grab(zoom - 1L)
      if (is.null(lower) || .tileEmptyFraction(lower) >= empty) break
      .log_skip(lvl, sprintf("%s has no imagery for %.0f%% of the extent at zoom %d; using zoom %d",
                             control$provider, 100 * empty, zoom, zoom - 1L))
      rast <- lower; zoom <- zoom - 1L
    }
  }
  if (lvl >= 2L)
    .log_detail(lvl, sprintf("basemap: zoom %d (%s) %d x %d px%s", zoom,
                             if (is.null(control$zoom)) "auto" else "user", dim(rast)[2], dim(rast)[1],
                             if (is.null(control$zoom) && isTRUE(auto$capped))
                               sprintf(" - tile budget reached, capped below the %d px target", 1200L) else ""))
  # get_tiles(project = TRUE) already returns the AOI's CRS, so reprojecting is normally a no-op that
  # costs a full raster copy. Only do it when the CRS genuinely differs.
  if (!isTRUE(grepl("4326", terra::crs(rast, describe = TRUE)$code %||% "")))
    rast <- tryCatch(terra::project(rast, "EPSG:4326"), error = function(e) rast)
  attr(rast, "nautilus.credit") <- tryCatch(maptiles::get_credit(control$provider), error = function(e) NULL)
  rast
}


#' Draw an RGB raster basemap over the current lon/lat panel (co-registered via its geographic extent).
#' @keywords internal
#' @noRd
.drawTiles <- function(rast) {
  if (is.null(rast) || !requireNamespace("terra", quietly = TRUE)) return(invisible(NULL))
  tryCatch({
    e  <- as.vector(terra::ext(rast))                           # xmin, xmax, ymin, ymax
    a  <- terra::as.array(rast)
    if (length(dim(a)) < 3L || dim(a)[3] < 3L) return(invisible(NULL))
    rgb <- a[, , 1:3, drop = FALSE]
    if (max(rgb, na.rm = TRUE) > 1) rgb <- rgb / 255
    rgb[!is.finite(rgb)] <- 1
    graphics::rasterImage(grDevices::as.raster(rgb), e[1], e[3], e[2], e[4], interpolate = TRUE)
  }, error = function(e) invisible(NULL))
  invisible(NULL)
}


#' Small provider-attribution caption for a raster basemap (bottom-right, unobtrusive over imagery).
#' @keywords internal
#' @noRd
.drawAttribution <- function(credit, cex = 1) {
  if (is.null(credit) || !is.character(credit) || !nzchar(credit)) return(invisible(NULL))
  usr <- graphics::par("usr")
  graphics::text(usr[2] - 0.01 * diff(usr[1:2]), usr[3] + 0.02 * diff(usr[3:4]),
                 labels = credit, adj = c(1, 0), cex = cex * 0.5, col = "#FFFFFFCC")
  invisible(NULL)
}


#' Resolve the `coastline` argument to a drawable spec ONCE per run (and hint if it degrades).
#'
#' `coastline` is polymorphic (the value-or-data rule shared with `basemap`): a keyword selecting a
#' bundled vector source by resolution, or a user-supplied coastline of the appropriate type.
#'   \itemize{
#'     \item `"auto"` (default) - the highest-resolution installed source: `mapdata::worldHires` if
#'       present, else the coarse `maps::world` with a one-time hint to install \pkg{mapdata}, else none.
#'     \item `"high"` - force `worldHires`; errors (with an install hint) if \pkg{mapdata} is absent,
#'       because the caller asked for it explicitly.
#'     \item `"low"` - force the coarse `maps::world`.  `"none"` - draw no coastline.
#'     \item a custom coastline: an \pkg{sf} object, a two-column lon/lat `data.frame`/`matrix`
#'       (NA-separated rings), or a path to a spatial file (`.rds`, or anything \pkg{sf} can read).
#'   }
#' The one-time hint (and any explicit-request error) is emitted HERE, so it fires once per run rather
#' than once per panel. Returns a spec consumed by `.drawCoastline()`.
#' @keywords internal
#' @noRd
.resolveCoastline <- function(coastline = "auto", lvl = 0L) {
  # custom coastline (anything that is not one of the keywords)
  if (!(is.character(coastline) && length(coastline) == 1L &&
        coastline %in% c("auto", "high", "low", "none"))) {
    return(list(kind = "custom", polys = .coastlineToPolys(coastline)))
  }
  if (identical(coastline, "none")) return(list(kind = "none"))

  has_maps    <- requireNamespace("maps", quietly = TRUE)
  has_mapdata <- requireNamespace("mapdata", quietly = TRUE)

  if (identical(coastline, "high")) {
    if (!has_mapdata)
      .abort(c("{.arg coastline = \"high\"} needs the {.pkg mapdata} package.",
               "i" = "Install it with {.code install.packages(\"mapdata\")}, use {.arg coastline = \"low\"}, or pass a custom coastline."))
    if (!.mapdataUsable())
      .abort(c("The {.pkg mapdata} {.val worldHires} database is installed but could not be loaded.",
               "i" = "Re-install it with {.code install.packages(\"mapdata\")}, or use {.arg coastline = \"low\"}."))
    return(list(kind = "maps", db = "worldHires"))
  }
  if (identical(coastline, "low")) {
    if (!has_maps) return(list(kind = "none"))
    return(list(kind = "maps", db = "world"))
  }
  # "auto": prefer worldHires, fall back to the coarse world with a one-time hint, else nothing.
  # The worldHires database is VERIFIED here rather than assumed: a broken/unloadable one previously
  # failed inside the per-panel draw, where the error was swallowed and the map came out with NO LAND.
  if (has_mapdata) {
    if (.mapdataUsable()) return(list(kind = "maps", db = "worldHires"))
    .log_info(lvl, "the 'mapdata' worldHires database could not be loaded - falling back to the ",
              "coarse maps::world coastline.")
  }
  if (has_maps) {
    # plain text: .log_info wraps its message in a glue span, so cli class markup would print literally
    .log_info(lvl, "coastline is low-resolution (maps::world); install 'mapdata' for ",
              "high-resolution coastlines, or pass a custom coastline via `coastline=`.")
    return(list(kind = "maps", db = "world"))
  }
  list(kind = "none")
}


#' Is the \pkg{mapdata} `worldHires` database actually loadable? (Installed is not the same as usable.)
#' Probed once per run by `.resolveCoastline()` so a broken database degrades loudly to `world` instead
#' of silently drawing no land at all.
#' @keywords internal
#' @noRd
.mapdataUsable <- function() {
  isTRUE(.withMapdata("worldHires", tryCatch({
    maps::map("worldHires", plot = FALSE, namesonly = TRUE, xlim = c(-10, 10), ylim = c(35, 45))
    TRUE
  }, error = function(e) FALSE)))
}


#' Evaluate `expr` with the \pkg{mapdata} database ATTACHED, restoring the search path afterwards.
#'
#' `worldHires` lives in \pkg{mapdata}, but `maps::map()` resolves a database NAME through the search
#' path, not through a namespace - so `requireNamespace("mapdata")` (which loads without attaching) is
#' not enough and the call fails with "'worldHiresMapEnv' is not an exported object". This attaches the
#' package for the duration of the call and detaches it again only if we were the ones who attached it,
#' so a user who already had it attached is left untouched. A no-op for the bundled `maps` databases.
#' @keywords internal
#' @noRd
.withMapdata <- function(db, expr) {
  if (identical(db, "worldHires") && !"package:mapdata" %in% search()) {
    if (!requireNamespace("mapdata", quietly = TRUE)) return(invisible(NULL))
    suppressWarnings(attachNamespace("mapdata"))
    on.exit(try(detach("package:mapdata", character.only = TRUE), silent = TRUE), add = TRUE)
  }
  force(expr)
}


#' Normalise a user-supplied coastline to a list of lon/lat polygon rings (NA-separated within each).
#' @keywords internal
#' @noRd
.coastlineToPolys <- function(x) {
  # a path: read it first, then re-dispatch on the loaded object
  if (is.character(x) && length(x) == 1L) {
    if (!file.exists(x)) .abort("Custom {.arg coastline} file not found: {.path {x}}.")
    obj <- if (grepl("\\.rds$", x, ignore.case = TRUE)) readRDS(x)
           else if (requireNamespace("sf", quietly = TRUE)) sf::st_read(x, quiet = TRUE)
           else .abort(c("Reading a spatial coastline file needs the {.pkg sf} package.",
                         "i" = "Install {.pkg sf}, or pass an {.pkg sf} object / a lon-lat data.frame."))
    return(.coastlineToPolys(obj))
  }
  # an sf / sfc object -> coordinate matrix grouped by polygon
  if (inherits(x, c("sf", "sfc", "sfg"))) {
    if (!requireNamespace("sf", quietly = TRUE))
      .abort("A custom {.arg coastline} of class {.cls sf} needs the {.pkg sf} package installed.")
    co <- sf::st_coordinates(sf::st_geometry(x))
    grp <- interaction(as.data.frame(co)[, setdiff(colnames(co), c("X", "Y")), drop = FALSE], drop = TRUE)
    parts <- split(as.data.frame(co[, c("X", "Y")]), grp)
    m <- do.call(rbind, lapply(parts, function(p) rbind(as.matrix(p), c(NA, NA))))
    return(list(unname(m)))
  }
  # a data.frame / matrix of lon,lat (NA rows separate rings)
  d <- as.data.frame(x)
  lon <- .pickCol(d, c("lon", "longitude", "x")); lat <- .pickCol(d, c("lat", "latitude", "y"))
  if (is.null(lon) || is.null(lat)) {
    if (ncol(d) < 2L) .abort("A custom {.arg coastline} data.frame/matrix needs lon and lat columns.")
    lon <- 1L; lat <- 2L
  }
  list(unname(as.matrix(d[, c(lon, lat)])))
}

#' First data.frame column whose (lower-cased) name matches one of `cands`; NULL if none.
#' @keywords internal
#' @noRd
.pickCol <- function(d, cands) {
  hit <- which(tolower(names(d)) %in% cands)
  if (length(hit)) hit[1] else NULL
}


#' Draw a resolved coastline spec over the current lon/lat panel. A silent no-op for `kind = "none"`.
#'
#' Everything is drawn directly in WGS84 lon/lat - no reprojection - so it co-registers with the data
#' primitives, and the panel clips it to the visible extent.
#' @keywords internal
#' @noRd
.drawCoastline <- function(lon_range, lat_range, spec = NULL, land = "#D9D2C5", border = "#B8AE9C",
                           fill = TRUE) {
  if (is.null(spec)) spec <- .resolveCoastline("auto")     # back-compat default when no spec supplied
  if (identical(spec$kind, "none")) return(invisible(NULL))
  col <- if (fill) land else NA                            # outline only over a raster canvas (imagery shows through)
  if (identical(spec$kind, "maps")) {
    .withMapdata(spec$db, tryCatch(
      suppressWarnings(maps::map(spec$db, add = TRUE, fill = fill, col = if (fill) land else border,
                                 border = border, lwd = 0.4, xlim = lon_range, ylim = lat_range)),
      error = function(e) invisible(NULL)))
    return(invisible(NULL))
  }
  # custom rings
  for (m in spec$polys)
    tryCatch(graphics::polygon(m[, 1], m[, 2], col = col, border = border, lwd = 0.4),
             error = function(e) invisible(NULL))
  invisible(NULL)
}


#' Add a metric scale bar to the current lon/lat map, if `prettymapr` is installed (else a silent no-op).
#' @keywords internal
#' @noRd
.mapScalebar <- function(label.cex = 0.75) {
  if (!requireNamespace("prettymapr", quietly = TRUE)) return(invisible(NULL))
  suppressMessages(suppressWarnings(prettymapr::addscalebar(
    plotunit = "latlon", plotepsg = 4326, widthhint = 0.22, unitcategory = "metric",
    htin = 0.05, padin = c(0.12, 0.12), style = "bar", lwd = 1,
    linecol = "black", label.col = "black", label.cex = label.cex)))
  invisible(NULL)
}


#' Resolve a per-page panel grid (+ pagination) for `n` panels.
#'
#' Auto-sizes when `ncols`/`nrows` are NULL (max 2 columns, up to 5 rows per page), honouring either
#' dimension when supplied. Returns the grid dimensions, the panels-per-page capacity, and the list of
#' per-page panel-index vectors, so a draw loop can paginate correctly instead of over-plotting one page.
#' @keywords internal
#' @noRd
.autoGrid <- function(n, ncols = NULL, nrows = NULL, max.cols = 2L, max.rows = 5L) {
  n <- max(1L, as.integer(n))
  if (is.null(ncols) && is.null(nrows)) {
    ncols <- if (n == 1L) 1L else min(max.cols, n)
    nrows <- min(max.rows, ceiling(n / ncols))
  } else if (is.null(nrows)) {
    nrows <- ceiling(n / ncols)
  } else if (is.null(ncols)) {
    ncols <- ceiling(n / nrows)
  }
  ncols <- as.integer(ncols); nrows <- as.integer(nrows)
  per_page <- ncols * nrows
  list(ncols = ncols, nrows = nrows, per_page = per_page,
       pages = split(seq_len(n), ceiling(seq_len(n) / per_page)))
}


#' Draw a time axis whose labels are in the DATA's timezone, never the session's.
#'
#' `graphics::axis.POSIXct(side, at = )` called without `x` looks like it will honour the timezone
#' carried by `at`, and does not: internally it sets `tz <- ""` (because `x` is missing) and then
#' overwrites the tick vector's own attribute with `attr(z, "tzone") <- tz`. The labels are therefore
#' formatted in whatever zone the analyst's machine happens to be in. This is silent, plausible, and
#' wrong: on a Europe/Lisbon machine every depth profile of a UTC record was labelled one hour late, so
#' a real dive at 12:26 UTC read as 13:26 and could not be reconciled with the video that recorded it.
#' It was found only because a camera deployment's footage disagreed with its own depth trace.
#'
#' So the zone is resolved explicitly and the labels are formatted here, rather than delegated. A
#' vector with no zone falls back to UTC, which is the package's storage contract ([importTagData()]
#' stamps it) - never to the session zone, which is a property of the analyst, not of the data.
#'
#' The date-versus-clock rule lives here too, so the call sites cannot drift apart.
#'
#' Two shapes are supported. With `at = NULL` the ticks are chosen from `time` and drawn at the instants
#' they represent - an ordinary time axis. With `at` supplied the panel's x axis is NOT a time axis (a
#' bin index, a sample number, a shifted coordinate system) and the caller states where each tick goes
#' and which instant it stands for; `time` is then the label timestamps, one per position. The second
#' form is why this cannot simply wrap [graphics::axis.POSIXct()].
#' @param time A `POSIXct` vector (or epoch seconds): the instants to be LABELLED, not tick positions.
#' @param n Approximate number of ticks, passed to [pretty()]. Ignored when `at` is supplied.
#' @param tz Timezone for the labels. `NULL` (default) reads it from `time`, falling back to `"UTC"`.
#' @param side Axis side, as in [graphics::axis()].
#' @param at Tick positions in the panel's own coordinates. `NULL` (default) derives them from `time`.
#' @param fmt Explicit [strftime()] format. `NULL` (default) picks dates over a day, clock time under.
#' @param ... Passed to [graphics::axis()] (`cex.axis`, `col`, `col.axis`, ...).
#' @return Invisibly, a list of the tick positions (`at`) and the strings drawn (`labels`), so a test
#'   can assert on the rendering without inspecting a device.
#' @keywords internal
#' @noRd
.axisTime <- function(time, n = 5, tz = NULL, side = 1L, at = NULL, fmt = NULL, ...) {
  if (is.null(tz)) tz <- attr(time, "tzone")
  if (is.null(tz) || !nzchar(tz[1])) tz <- "UTC"
  tz   <- tz[1]
  tnum <- as.numeric(time)
  rng  <- range(tnum, na.rm = TRUE)
  if (is.null(at)) {
    # ticks chosen from the data, drawn at the times they represent
    lab_t <- as.numeric(pretty(.asPosix(rng, tz), n = n))
    pos   <- lab_t
  } else {
    # the panel's x axis is NOT a time axis (bin index, sample number, an offset coordinate system);
    # the caller says where each tick goes and which instant it stands for
    lab_t <- tnum
    pos   <- as.numeric(at)
    if (length(pos) != length(lab_t))
      .abort(c("{.arg at} and {.arg time} must be the same length when tick positions are supplied.",
               "i" = "Got {length(pos)} position{?s} for {length(lab_t)} timestamp{?s}."))
  }
  if (is.null(fmt)) fmt <- if (diff(rng) > 86400) "%d/%b" else "%H:%M"
  z   <- .asPosix(lab_t, tz)   # re-tag: the instant is unchanged, only how it is written down
  lab <- format(z, format = fmt, tz = tz)
  graphics::axis(side, at = pos, labels = lab, ...)
  invisible(list(at = z, labels = lab, tz = tz))
}


#' Tag epoch seconds with a timezone, without going through as.POSIXct()'s defaulting rules.
#' @keywords internal
#' @noRd
.asPosix <- function(x, tz) structure(as.numeric(x), class = c("POSIXct", "POSIXt"), tzone = tz)

# Additional shared plot helpers


#####################################################################################
## Rescale function  ################################################################
#####################################################################################
## Sourced from 'scales' package (https://CRAN.R-project.org/package=scales)

#' Rescale
#'
#' @description Rescales a numeric vector to a specified range.
#' @note This function is intended for internal use within the 'nautilus' package.
#' @keywords internal
#' @noRd

.rescale <- function (x, to=c(0, 1), from=range(x, na.rm=TRUE, finite=TRUE), ...) {
  if (.zero_range(from) || .zero_range(to)) return(ifelse(is.na(x), NA, mean(to)))
  (x - from[1])/diff(from) * diff(to) + to[1]
}

.zero_range <- function(x, tol=1000*.Machine$double.eps) {
  if (length(x) == 1) return(TRUE)
  if (length(x) != 2) stop("'x' must be length 1 or 2", call.=FALSE)
  if (any(is.na(x))) return(NA)
  if (x[1] == x[2]) return(TRUE)
  if (all(is.infinite(x))) return(FALSE)
  m <- min(abs(x))
  if (m == 0) return(FALSE)
  abs((x[1] - x[2]) / m) < tol
}



#####################################################################################
## Updated colorlegend function  ####################################################
#####################################################################################
## Adapted from 'shape' package (https://rdrr.io/cran/shape/src/R/colorlegend.R)
## - added main.adj + support for scientific notation
## - added tick.length
## - added horizontal option
## - added zlab option
## - added mgp parameter

#' Color Legend
#'
#' @description Creates a color legend for a plot, adapted from https://rdrr.io/cran/shape/src/R/colorlegend.R
#' with additional features such as main.adj, main.inset, and support for scientific notation.
#' @note This function is intended for internal use within the 'nautilus' package.
#' @keywords internal
#' @noRd

.colorlegend <- function(col,
                         zlim,
                         zlevels = 5,
                         dz = NULL,
                         zval = NULL,
                         zlab = NULL,
                         log = FALSE,
                         posx = c(0.9, 0.93),
                         posy = c(0.05, 0.9),
                         main = NULL,
                         main.cex = 1.0,
                         main.col = "black",
                         main.adj = 0.5,
                         lab.col = "black",
                         digit = 0,
                         left = FALSE,
                         tick.length = 0.3,
                         lab.scientific = FALSE,
                         horizontal = FALSE,
                         mgp = c(1.5, 0.5, 0),  # New parameter similar to par()'s mgp
                         ...) {

  ## Set the number of colors
  ncol <- length(col)

  ## Save original margin settings (and ensure they are restored even on error)
  omar <- par("mar")
  on.exit(par(new = FALSE, mar = omar), add = TRUE)

  # Set up a new plot layer without erasing the existing plot
  par(new=TRUE)

  ## initialize new margins
  nmar <- omar

  ## Adjust margins based on orientation
  if (horizontal) {
    nmar[1] <- max(nmar[1], 4)  # Increase bottom margin for horizontal legend
  } else {
    nmar[c(2, 4)] <- 0  # Remove left and right margins for vertical legend
  }

  ## Apply updated margin settings
  par(mar=nmar)

  ## Create an empty plot for the legend without axes or frames
  plot(0, type="n", xlab="", ylab="", asp=1, axes=FALSE, frame.plot=FALSE,
       xlim=c(0, 1), ylim=c(0, 1), xaxs="i", yaxs="i")

  ## Get the plotting area coordinates
  pars <- par("usr")

  ## Calculate the size of the plot area
  dx <- pars[2] - pars[1]
  dy <- pars[4] - pars[3]


  ## Draw the color legend (horizontal or vertical)
  if (horizontal) {
    ## ---- Horizontal Legend ----

    ## Set the position of the legend
    ymin <- pars[3] + posy[1] * dy
    ymax <- pars[3] + posy[2] * dy
    xmin <- pars[1] + posx[1] * dx
    xmax <- pars[1] + posx[2] * dx

    ## Create colored rectangles for the horizontal legend
    X <- seq(xmin, xmax, length.out=ncol+1)
    rect(X[-(ncol+1)], ymin, X[-1], ymax, col=col, border=NA)
    rect(xmin, ymin, xmax, ymax, border=lab.col)

    ## Determine tick labels (either provided by zval or calculated)
    if (!is.null(zval)) {
      zz <- zval
    } else if (is.null(dz) & !is.null(zlevels)) {
      zz <- pretty(zlim, n=(zlevels + 1))  # Generate pretty tick labels
    }

    ## Apply logarithmic scaling if specified
    if (log) zz <- log10(zz)

    ## Draw tick marks and labels for the horizontal legend
    if (!is.null(zz)) {
      Xpos <- xmin + (zz - zlim[1]) / (zlim[2] - zlim[1]) * (xmax - xmin)

      ## Draw ticks
      tick.ystart <- ymin - (tick.length * (ymax - ymin))  # Tick length as fraction of height
      tick.yend <- ymin  # Y position of tick end
      segments(Xpos, tick.ystart, Xpos, tick.yend, col = lab.col)

      ## Format labels (scientific or fixed-point)
      if (lab.scientific) {
        labels <- format(zz, scientific = TRUE)
      } else {
        labels <- formatC(zz, digits = digit, format = "f")
      }

      # Replace with custom labels
      if (!is.null(zlab)) {
        labels[match(zz, zval)] <- zlab
      }

      ## Adjust y-position using mgp parameter
      label.offset <- mgp[1] * (tick.yend - tick.ystart)  # Use first mgp value for label offset
      text(Xpos, tick.ystart - label.offset, labels, col = lab.col, adj=c(0.5, 1), ...)
    }


  } else {
    ## ---- Vertical Legend ----

    ## Set the position of the legend
    ymin <- pars[3] + posy[1] * dy
    ymax <- pars[3] + posy[2] * dy
    xmin <- pars[1] + posx[1] * dx
    xmax <- pars[1] + posx[2] * dx

    ## Create colored rectangles for the vertical legend
    Y <- seq(ymin, ymax, length.out=ncol+1)
    rect(xmin, Y[-(ncol+1)], xmax, Y[-1], col=col, border=NA)
    rect(xmin, ymin, xmax, ymax, border=lab.col)

    ## Determine tick labels (either provided by zval or calculated)
    if (!is.null(zval)) {
      zz <- zval
    } else if (is.null(dz) & !is.null(zlevels)) {
      zz <- pretty(zlim, n=(zlevels + 1))  # Generate pretty tick labels
    }

    ## Apply logarithmic scaling if specified
    if (log) zz <- log10(zz)

    ## Draw tick marks and labels for the vertical legend
    if (!is.null(zz)) {
      Ypos <- ymin + (zz - zlim[1]) / (zlim[2] - zlim[1]) * (ymax - ymin)

      ## Draw ticks
      tick.xstart <- if (left) {xmin - (tick.length * (xmax - xmin))} else {xmax}
      tick.xend <- if (left) {xmin} else {xmax + (tick.length * (xmax - xmin))}
      segments(tick.xstart, Ypos, tick.xend, Ypos, col = lab.col)

      ## Format labels (scientific or fixed-point)
      if (lab.scientific) {
        labels <- format(zz, scientific = TRUE)
      } else {
        labels <- formatC(zz, digits = digit, format = "f")
      }

      # Replace with custom labels
      if (!is.null(zlab)) {
        labels[match(zz, zval)] <- zlab
      }

      # Adjust horizontal alignment based on legend position
      # 1 for right-alignment, 0 for left-alignment
      adj_value <- if (left) 1 else 0

      ## Add labels next to ticks with mgp-controlled spacing
      label.offset <- mgp[1] * (tick.xend - tick.xstart)  # Use first mgp value for label offset
      text(tick.xend + label.offset, Ypos, labels, col = lab.col, adj = adj_value, ...)
    }
  }

  ## ---- Main Title ----
  if (!is.null(main)) {
    if (horizontal) {
      # Place the title above the horizontal color scale and center it
      text(mean(c(xmin, xmax)), ymax + 0.05 * dy,  # y-position adjusted to ymax
           labels = main, adj = c(0.5, 0.5), cex = main.cex, col = main.col)
    } else {
      # Keep the title below the vertical color scale
      text(mean(c(xmin, xmax)), ymax + 0.05 * dy,
           labels=main, adj=c(main.adj, 0.5), cex=main.cex, col=main.col)
    }
  }

  ## Reset the margin settings
  par(new=FALSE)
  par(mar=omar)
}


#####################################################################################
## Jet color palette ################################################################
#####################################################################################
## Sourced from pals::jet

#' Jet color palette
#'
#' @description This function generates a color palette using the Jet color scheme,
#' sourced from palr::jet function. The colors in this palette are slightly darker
#' (10%) than the original Jet palette.
#' @note This function is intended for internal use within the 'nautilus' package.
#' @keywords internal
#' @noRd



#####################################################################################
## Viridis color palette ############################################################
#####################################################################################
## Sourced from viridis::viridis(100)

#' Viridis color palette
#'
#' @description This function generates a color palette using the Viridis color scheme,
#' known for its perceptually uniform properties. The palette is adapted from the viridis package.
#' @note This function is intended for internal use within the 'moby' package.
#' @keywords internal
#' @noRd

.viridis_pal <- function(n) {
  viridis_colors <- c(
    "#440154FF", "#450558FF", "#46085CFF", "#470D60FF", "#471063FF",
    "#481467FF", "#481769FF", "#481B6DFF", "#481E70FF", "#482173FF",
    "#482576FF", "#482878FF", "#472C7AFF", "#472F7CFF", "#46327EFF",
    "#453581FF", "#453882FF", "#443B84FF", "#433E85FF", "#424186FF",
    "#404587FF", "#3F4788FF", "#3E4A89FF", "#3D4D8AFF", "#3C508BFF",
    "#3B528BFF", "#39558CFF", "#38598CFF", "#375B8DFF", "#355E8DFF",
    "#34608DFF", "#33638DFF", "#32658EFF", "#31688EFF", "#2F6B8EFF",
    "#2E6D8EFF", "#2D708EFF", "#2C718EFF", "#2B748EFF", "#2A768EFF",
    "#29798EFF", "#287C8EFF", "#277E8EFF", "#26818EFF", "#26828EFF",
    "#25858EFF", "#24878EFF", "#238A8DFF", "#228D8DFF", "#218F8DFF",
    "#20928CFF", "#20938CFF", "#1F968BFF", "#1F998AFF", "#1E9B8AFF",
    "#1F9E89FF", "#1FA088FF", "#1FA287FF", "#20A486FF", "#22A785FF",
    "#24AA83FF", "#25AC82FF", "#28AE80FF", "#2BB07FFF", "#2EB37CFF",
    "#31B67BFF", "#35B779FF", "#39BA76FF", "#3DBC74FF", "#41BE71FF",
    "#47C06FFF", "#4CC26CFF", "#51C56AFF", "#56C667FF", "#5BC863FF",
    "#61CA60FF", "#67CC5CFF", "#6DCD59FF", "#73D056FF", "#78D152FF",
    "#7FD34EFF", "#85D54AFF", "#8CD646FF", "#92D741FF", "#99D83DFF",
    "#A0DA39FF", "#A7DB35FF", "#ADDC30FF", "#B4DE2CFF", "#BBDE28FF",
    "#C2DF23FF", "#C9E020FF", "#D0E11CFF", "#D7E219FF", "#DDE318FF",
    "#E4E419FF", "#EBE51AFF", "#F1E51DFF", "#F7E620FF", "#FDE725FF"
  )
  return(colorRampPalette(viridis_colors)(n))
}

# Plot themes, layouts, panels, legends and visual summaries.

# plotDepthProfiles helpers ----

################################################################################
# Internal helpers ###########################################################
################################################################################

#' Default colour-legend label for a mapped variable
#' @keywords internal
#' @noRd
.defaultColorLabel <- function(color.by) {
  known <- c(temp = "Temperature (\u00b0C)", depth = "Depth (m)", speed = "Speed (m/s)")
  if (color.by %in% names(known)) unname(known[color.by]) else color.by
}

#' Bin-average a deployment's data for plotting (fixes the datetime/ID columns from the arguments)
#' @keywords internal
#' @noRd
.downsampleForPlot <- function(tag, id.col, datetime.col, seconds) {
  if (is.null(seconds) || seconds <= 0) {
    # no binning, but the same coercion contract must still hold, or a factor depth reaches the renderer
    dt0 <- data.table::as.data.table(tag)
    for (col in setdiff(names(dt0), c(id.col, datetime.col))) {
      if (is.numeric(dt0[[col]])) next
      z <- .asNumericSafe(dt0[[col]])
      if (any(is.finite(z))) data.table::set(dt0, j = col, value = z)
    }
    return(dt0)
  }
  dt <- data.table::as.data.table(tag)
  tvec <- dt[[datetime.col]]
  tz <- attr(tvec, "tzone"); if (is.null(tz) || !nzchar(tz)) tz <- "UTC"
  tnum <- .asTimeSeconds(tvec)                       # Date counts DAYS: as.numeric() collapsed the record
  if (is.null(tnum))
    .abort(c("{.arg datetime.col} ({.val {datetime.col}}) must hold date-times, not {.cls {class(tvec)[1]}}.",
             "i" = "Convert it with {.fn as.POSIXct} before plotting."))
  binvec <- as.POSIXct(floor(tnum / seconds) * seconds, origin = "1970-01-01", tz = tz)
  # a factor/character numeric column would otherwise be dropped by the is.numeric filter below and the
  # panel drawn empty, so recover anything that is numeric once the shared contract is applied
  cand <- setdiff(names(dt), c(id.col, datetime.col))
  for (col in cand) {
    if (is.numeric(dt[[col]])) next
    z <- .asNumericSafe(dt[[col]])
    if (any(is.finite(z))) data.table::set(dt, j = col, value = z)
  }
  num_cols <- setdiff(names(dt)[vapply(dt, is.numeric, logical(1))], c(id.col, datetime.col))
  # group by externally-computed vectors, so the source table is never modified (no defensive copy needed)
  by_list <- if (id.col %in% names(dt)) list(.id = dt[[id.col]], .bin = binvec) else list(.bin = binvec)
  agg <- dt[, lapply(.SD, mean, na.rm = TRUE), by = by_list, .SDcols = num_cols]
  for (col in num_cols) data.table::set(agg, i = which(is.nan(agg[[col]])), j = col, value = NA_real_)
  if (".id" %in% names(agg)) data.table::setnames(agg, ".id", id.col)
  data.table::setnames(agg, ".bin", datetime.col)
  agg[]
}

#' Inverted, padded depth axis limits (surface at top, deepest at bottom, headroom for the ID label)
#' @keywords internal
#' @noRd
.depthYlim <- function(max_depth) {
  top <- ceiling(max_depth / 10) * 10
  if (!is.finite(top) || top <= 0) top <- 10
  c(top, -top * 0.18)
}

#' Resolve the panel grid and pagination for `n` deployments
#' @keywords internal
#' @noRd
.depthProfileLayout <- function(n, ncols, nrows, legend) {
  extra <- if (isTRUE(legend) && n >= 2L) 1L else 0L      # a shared legend consumes one grid cell; size for it
  if (is.null(ncols) && is.null(nrows)) {
    ncols <- if (n == 1) 1L else 2L
    nrows <- min(5L, ceiling((n + extra) / ncols))
  } else if (is.null(nrows)) {
    nrows <- min(5L, ceiling((n + extra) / ncols))
  } else if (is.null(ncols)) {
    ncols <- ceiling((n + extra) / nrows)
  }
  ncols <- as.integer(ncols); nrows <- as.integer(nrows)
  per_page <- ncols * nrows
  # A shared colour legend takes the LAST grid cell (a single horizontal bar, proportional to one panel)
  # rather than a full-height side column - far less overwhelming. Only possible when the grid has a spare
  # cell (>= 2 cells); with a single panel we fall back to that panel's own (compact) legend.
  reserve  <- isTRUE(legend) && per_page >= 2L
  capacity <- if (reserve) per_page - 1L else per_page                  # panels per page (last cell = legend)
  pages    <- split(seq_len(n), ceiling(seq_len(n) / capacity))

  list(matrix = matrix(seq_len(per_page), nrow = nrows, ncol = ncols, byrow = TRUE),
       widths = rep(1, ncols), heights = rep(1, nrows),
       per_page = per_page, capacity = capacity, legend_cell = if (reserve) per_page else NA_integer_,
       pages = pages, nrows = nrows, ncols = ncols,
       width = 6.5 * ncols, height = 1.9 * nrows + 1)
}

#' Draw a single depth-profile panel
#' @keywords internal
#' @noRd
.drawDepthPanel <- function(dep, color.by, depth.col, datetime.col, color.pal, color_range, depth_ylim,
                            shade.diel, geom, point.size, lwd, cex, panel.legend, color.label, theme) {

  d     <- dep$data
  depth <- d[[depth.col]]
  time  <- d[[datetime.col]]

  # map colours (guard the palette index to 1..length; NAs stay transparent)
  has_color <- !is.null(color_range) && all(is.finite(color_range)) && diff(color_range) > 0 &&
    color.by %in% names(d) && any(!is.na(d[[color.by]]))
  point_col <- rep(theme$ink, length(depth))                        # no colour variable -> plain ink trace
  if (has_color) {
    idx <- round(.rescale(d[[color.by]], from = color_range, to = c(1, length(color.pal))))
    point_col <- color.pal[idx]
  }

  graphics::par(mar = c(2.2, 5, 2.2, if (panel.legend) 5 else 1.2))
  plot(x = time, y = depth, type = "n", axes = FALSE, xaxs = "i", xlab = "", ylab = "Depth (m)",
       ylim = depth_ylim, xlim = dep$time.range, cex.lab = cex, col.lab = theme$ink)

  # Panel fill from the theme, but only when diel shading is switched off for the whole figure: with
  # shading on the background greys MEAN a phase, so a themed fill on a panel that merely lacks
  # coordinates would read as one of them.
  if (!shade.diel) {
    usr <- graphics::par("usr")
    graphics::rect(usr[1], usr[3], usr[2], usr[4], col = theme$panel, border = NA)
  }
  if (shade.diel && all(is.finite(dep$coords))) .shadeDiel(dep$coords)
  .drawEventBands(dep$events, dep$event.palette, dep$time.range %||% time, theme, cex)

  # geometry: a coloured LINE traces the dive (default), coloured POINTS show the raw samples, or BOTH.
  # The line is drawn as gap-broken segments so it never connects across a recording gap (see .drawColorLine).
  if (geom %in% c("line", "both")) .drawColorLine(time, depth, point_col, lwd = lwd)
  if (geom %in% c("points", "both")) graphics::points(x = time, y = depth, pch = 16, col = point_col, cex = point.size)

  # axes: date or clock time depending on the record length, labelled in the DATA's zone (see .axisTime)
  .axisTime(dep$time.range %||% time, n = 5, cex.axis = cex * 0.95, col = theme$axis, col.axis = theme$axis)
  graphics::axis(2, at = pretty(c(0, depth_ylim[1]), n = 5), las = 1, cex.axis = cex * 0.95,
                 col = theme$axis, col.axis = theme$axis)

  graphics::abline(h = 0, lty = 2, lwd = 1.2, col = theme$ink)                          # surface
  graphics::abline(h = max(depth, na.rm = TRUE), lty = 2, lwd = 1, col = theme$ink)     # maximum depth
  graphics::legend("topleft", inset = c(0, -0.036), legend = dep$id, text.font = 2, bty = "n",
                   cex = cex * 1.2, text.col = theme$ink)
  graphics::box(col = theme$axis)

  # per-panel legend only when panels are independently scaled - a compact bar (~1/3 panel height, centred)
  # rather than a full-height strip, so it does not dominate the data
  if (panel.legend && has_color) {
    labs <- pretty(color_range); labs <- labs[labs >= color_range[1] & labs <= color_range[2]]
    .colorlegend(col = color.pal, zlim = color_range, zval = labs, posx = c(0.925, 0.945),
                 posy = c(0.34, 0.66), main = color.label, main.cex = cex * 0.85,
                 digit = 1, main.adj = 0, cex = cex * 0.75, main.col = theme$ink, lab.col = theme$axis)
  }
}

#' Draw a depth trace as a colour-mapped line, broken across gaps
#'
#' Each consecutive pair of samples becomes one segment, coloured by the earlier sample's mapped colour.
#' Because the colour variable (temperature by default) varies smoothly with depth, adjacent segments blend,
#' so the trace reads as a continuously-coloured trajectory. Segments are DROPPED where either endpoint is
#' missing or where the time step is far larger than the record's typical cadence, so the line never invents
#' a dive across a surface interval or a recording gap. NA colours (samples with no colour value) are skipped
#' by `segments()` too.
#' @keywords internal
#' @noRd
.drawColorLine <- function(time, depth, col, lwd = 1.1) {
  n <- length(depth)
  if (n < 2L) return(invisible(NULL))
  tnum <- as.numeric(time)
  dt   <- diff(tnum)
  step <- stats::median(dt[dt > 0], na.rm = TRUE)                 # the typical sampling interval
  gap  <- !is.finite(dt) | (is.finite(step) & dt > 4 * step)      # a step far beyond it = a real gap
  x0 <- tnum[-n]; y0 <- depth[-n]; x1 <- tnum[-1]; y1 <- depth[-1]
  drop <- gap | is.na(y0) | is.na(y1)
  x0[drop] <- NA                                                  # segments() skips any NA endpoint
  graphics::segments(x0, y0, x1, y1, col = col[-n], lwd = lwd, lend = 1)
  invisible(NULL)
}

#' The three diel-shading fills (day / crepuscule / night), shared by the panels and the legend key
#'
#' Deliberately NOT taken from the theme's `day` / `night` slots, and the one piece of chrome in this
#' figure that stays fixed. Two reasons, both checked by rendering it the other way:
#'   * this panel is a colour-mapped trace drawn across the WHOLE plotting area, so the background is
#'     behind the data everywhere. `night = "#294763"` is a dark navy - over a night-heavy record it
#'     swallows the cool end of the ramp entirely, and `day = "#DCEAF6"` tints the panel the same blue
#'     the ramp uses, so the shading reads as data.
#'   * there are THREE phases here (day / twilight / night) against two-and-a-bit theme slots, so the
#'     middle class would have to be invented by interpolation and would carry no theme meaning anyway.
#' The greys stay near-white to near-light so they separate from each other without competing with any
#' colour scale. Only the key's swatch OUTLINE follows the theme, since it is ordinary axis chrome.
#' @keywords internal
#' @noRd
.dielFills <- function() c(day = "grey98", crepuscule = "grey92", night = "grey85")

#' Shade the current panel background by diel phase (day / twilight / night)
#' @keywords internal
#' @noRd
.shadeDiel <- function(coords) {
  usr    <- graphics::par("usr")
  step   <- max(60, (usr[2] - usr[1]) / 1500)                    # cap resolution so long records stay cheap
  tseq   <- as.POSIXct(seq(usr[1], usr[2], by = step), origin = "1970-01-01", tz = "UTC")
  phase  <- getDielPhase(tseq, coords = data.frame(lon = coords[["lon"]], lat = coords[["lat"]]), phases = 3)
  breaks <- c(1, which(diff(as.integer(factor(phase))) != 0) + 1, length(tseq))
  cols   <- .dielFills()
  tnum   <- as.numeric(tseq)
  for (j in seq_len(length(breaks) - 1))
    graphics::rect(xleft = tnum[breaks[j]], xright = tnum[breaks[j + 1]],
                   ybottom = usr[3], ytop = usr[4], col = cols[phase[breaks[j]]], border = NA)
}

#' Draw the shared legend cell: a horizontal colour bar and, beneath it, a compact diel-shading key
#'
#' Occupies the reserved last grid cell (see .depthProfileLayout(legend = TRUE)). The bar spans the central
#' ~75% of the cell width and a slim band, so it reads as a legend rather than a fourth panel; the freed
#' space below carries a small Day / Twilight / Night key when diel shading was drawn - the grey bands are
#' meaningless without it. The swatch fills come from `.dielFills()` (fixed, see there); only their
#' outline and the surrounding text follow the theme.
#' @keywords internal
#' @noRd
.drawLegendCell <- function(color.pal, zlim, label, cex, show.diel = FALSE, theme) {
  graphics::par(mar = c(0.6, 3.0, 0.6, 3.0))
  graphics::plot.new()
  pad <- diff(zlim) * (1 / 0.75 - 1) / 2                           # widen the x-window so the bar fills 75% of it
  graphics::plot.window(xlim = c(zlim[1] - pad, zlim[2] + pad), ylim = c(0, 1), xaxs = "i", yaxs = "i")

  # The whole legend block (title + bar + tick labels + diel key) is CENTRED in the cell rather than pinned
  # to its top, so the gap to the panel above and the gap below are balanced. `mid` is the bar's centre,
  # offset upwards when a diel key has to fit underneath.
  strip_h <- 0.13                                                  # ~30% shorter than before
  mid     <- if (isTRUE(show.diel)) 0.55 else 0.47
  yb <- mid - strip_h / 2; yt <- mid + strip_h / 2

  n  <- length(color.pal); xs <- seq(zlim[1], zlim[2], length.out = n + 1)
  graphics::rect(xs[-(n + 1)], yb, xs[-1], yt, col = color.pal, border = NA)
  graphics::rect(zlim[1], yb, zlim[2], yt, border = theme$axis)
  labs <- pretty(zlim); labs <- labs[labs >= zlim[1] & labs <= zlim[2]]
  graphics::axis(1, at = labs, pos = yb, cex.axis = cex * 0.95, tcl = -0.3, mgp = c(3, 0.35, 0),
                 col = theme$axis, col.axis = theme$axis)
  graphics::text(mean(zlim), yt + 0.10, label, font = 2, cex = cex * 1.0, col = theme$ink)  # title above the strip

  # Diel key, drawn by hand rather than via legend(): base legend() ties its swatch size to the text
  # height with no separate control, and these need to be big enough to tell three greys apart. Widths are
  # converted through the device aspect so the swatches come out SQUARE despite the cell's x/y unit scales.
  if (isTRUE(show.diel)) {
    keys  <- c("Day", "Twilight", "Night"); fills <- unname(.dielFills())
    cexd  <- cex * 0.9
    key_y <- mid - 0.255                                           # below the bar's tick labels
    usr <- graphics::par("usr"); pin <- graphics::par("pin")
    sq_h <- 0.085                                                  # swatch height, in cell units
    sq_w <- sq_h * ((usr[2] - usr[1]) / pin[1]) / ((usr[4] - usr[3]) / pin[2])
    gap  <- sq_w * 0.5                                             # swatch -> its label
    sep  <- sq_w * 1.2                                             # between entries
    lw     <- graphics::strwidth(keys, cex = cexd)
    item_w <- sq_w + gap + lw
    x <- mean(zlim) - (sum(item_w) + sep * (length(keys) - 1)) / 2
    for (i in seq_along(keys)) {
      graphics::rect(x, key_y - sq_h / 2, x + sq_w, key_y + sq_h / 2, col = fills[i], border = theme$axis)
      graphics::text(x + sq_w + gap, key_y, keys[i], adj = c(0, 0.5), cex = cexd, col = theme$axis)
      x <- x + item_w[i] + sep
    }
  }
}


#######################################################################################################
#######################################################################################################
#######################################################################################################

# plotDistributions helpers ----

################################################################################
# Internal helpers ###########################################################
################################################################################

#' The metrics auto-selected when `metrics = NULL` (kinematic / effort, not environmental).
#' @keywords internal
#' @noRd
.distAutoMetrics <- function(cols = NULL) {
  base <- c("paddle_speed", "speed", "vedba", "odba", "vertical_velocity")
  # Tail-beat estimates are named after the backend that produced them, so the auto list cannot be a
  # fixed string. Take whichever backends actually ran: with both, both panels appear, which is the
  # honest default for a function whose job is to show distributions.
  tb <- if (is.null(cols)) "tbf_hz" else grep("^tbf_hz_", cols, value = TRUE)
  c(tb, base)
}

#' Axis label for a metric: an explicit `labels` entry, else a built-in default, else the column name.
#' @keywords internal
#' @noRd
.distLabel <- function(metric, labels = NULL) {
  # an explicit label for the column as it is actually named wins outright: that is the key the
  # examples use and the one .distAutoMetrics() produces, so stripping first would silently ignore it.
  if (!is.null(labels) && metric %in% names(labels)) return(unname(labels[[metric]]))
  # otherwise a backend suffix is stripped for the lookup and appended to the label, so tbf_hz_wavelet
  # reads as "Tail-beat frequency (Hz), wavelet" rather than falling through to the raw column name
  backend <- sub("^tbf_(hz|amplitude)_", "", metric)
  if (!identical(backend, metric)) metric <- sub(paste0("_", backend, "$"), "", metric)
  else backend <- NA_character_
  known <- c(tbf_hz = "Tail-beat frequency (Hz)", tbf_amplitude = "Tail-beat amplitude",
             paddle_speed = "Paddle speed (m/s)", speed = "Speed (m/s)", vedba = "VeDBA (g)",
             odba = "ODBA (g)", vertical_velocity = "Vertical velocity (m/s)",
             depth = "Depth (m)", temp = "Temperature (\u00b0C)")
  lab <- if (!is.null(labels) && metric %in% names(labels)) unname(labels[[metric]])
         else if (metric %in% names(known)) unname(known[metric])
         else metric
  if (!is.na(backend)) lab <- paste0(lab, ", ", backend)
  lab
}

#' One tidy distribution-summary row for a deployment x metric (NA statistics when empty).
#' @keywords internal
#' @noRd
.distSummaryRow <- function(id, metric, x) {
  if (!length(x)) {
    q <- rep(NA_real_, 6)
    return(data.frame(id = id, metric = metric, n = 0L, mean = NA_real_, median = NA_real_, sd = NA_real_,
                      q05 = q[1], q25 = q[2], q75 = q[3], q95 = q[4], min = q[5], max = q[6],
                      stringsAsFactors = FALSE))
  }
  qs <- stats::quantile(x, c(.05, .25, .75, .95), na.rm = TRUE, names = FALSE)
  data.frame(id = id, metric = metric, n = length(x), mean = mean(x), median = stats::median(x),
             sd = stats::sd(x), q05 = qs[1], q25 = qs[2], q75 = qs[3], q95 = qs[4],
             min = min(x), max = max(x), stringsAsFactors = FALSE)
}

#' Resolve the shared deployment order (returns ids top-to-bottom, i.e. first = top row).
#' @keywords internal
#' @noRd
.distOrder <- function(ids, order.by, order_values) {
  ids <- unique(ids)
  if (order.by == "input") return(ids)
  if (order.by == "id")    return(sort(ids))
  meds <- vapply(ids, function(id) { x <- order_values[[id]]; if (length(x)) stats::median(x, na.rm = TRUE) else NA_real_ }, numeric(1))
  ids[order(meds, decreasing = TRUE, na.last = TRUE)]          # highest median at the top
}

#' Darken a fill colour for use as its own polygon outline.
#'
#' The densities are drawn as a semi-transparent fill outlined in the SAME hue, which leaves the edge
#' barely distinguishable from the interior - so overlapping or adjacent densities blur together. Darkening
#' the outline defines each shape without introducing a second colour into the palette.
#' @param col A colour; `factor` < 1 darkens (0.62 = ~40% darker).
#' @keywords internal
#' @noRd
.distDarken <- function(col, factor = 0.62) {
  v <- grDevices::col2rgb(col)[, 1] * factor
  grDevices::rgb(v[1], v[2], v[3], maxColorValue = 255)
}

#' Upper-trimmed x-axis limits for a metric's pooled samples (robust to long tails).
#' @keywords internal
#' @noRd
.distXlim <- function(pooled, trim) {
  if (!length(pooled)) return(c(0, 1))
  lo <- min(pooled, na.rm = TRUE)
  hi <- if (trim >= 1) max(pooled, na.rm = TRUE) else stats::quantile(pooled, trim, na.rm = TRUE, names = FALSE)
  if (!is.finite(lo) || !is.finite(hi) || hi <= lo) hi <- lo + 1
  c(lo, hi)
}

#' Named vector of population reference values ("median"/"mean") for a metric's pooled samples.
#' @keywords internal
#' @noRd
.distReferences <- function(pooled, which) {
  out <- numeric(0)
  if (!length(pooled)) return(out)
  if ("median" %in% which) out["median"] <- stats::median(pooled, na.rm = TRUE)
  if ("mean"   %in% which) out["mean"]   <- mean(pooled, na.rm = TRUE)
  out
}

#' Colours for the reference lines.
#' @keywords internal
#' @noRd
.distRefStyle <- function() list(median = list(col = "#2C5F8A", lty = 2), mean = list(col = "#B03A2E", lty = 3))

#' Kernel density for the pooled marginal, spanning a slightly padded x-window.
#' @keywords internal
#' @noRd
.distDensity <- function(x, xlim) {
  pad <- 0.04 * diff(xlim)
  stats::density(x, n = 512, from = xlim[1] - pad, to = xlim[2] + pad, na.rm = TRUE)
}

#' Restrict a density estimate to an x-window (so a violin ends cleanly at the axis, not beyond it).
#' @keywords internal
#' @noRd
.clampDensity <- function(d, xlim) {
  keep <- d$x >= xlim[1] & d$x <= xlim[2]
  if (!any(keep)) return(d)
  list(x = d$x[keep], y = d$y[keep])
}

#' PDF device size (inches) for M metric columns x ny deployment rows. The height budgets a fixed
#' marginal strip (~0.95 in, matching the layout's lcm(2.4)), the violin-row chrome (x-axis), and a
#' per-deployment row height.
#' @keywords internal
#' @noRd
.distDeviceSize <- function(M, ny, show.marginal, cex) {
  marg_in <- if (show.marginal) 0.95 else 0
  list(width  = max(5, 3.8 * M + 1.5) * max(1, cex * 0.9),
       height = (marg_in + 0.95 + 0.19 * ny) * max(1, cex * 0.9))
}

#' Draw one reference line into the current panel (vertical, styled by kind).
#' @keywords internal
#' @noRd
.distRefLines <- function(refs, cex) {
  sty <- .distRefStyle()
  for (k in names(refs))
    graphics::abline(v = refs[[k]], col = sty[[k]]$col, lty = sty[[k]]$lty, lwd = 1.6)
}

#' Subtle panel chrome shared by both rows: the theme's panel fill with thin, unobtrusive vertical
#' grid lines at the metric ticks in the theme's grid colour, drawn BEFORE the data so the
#' distributions stay the focus. Vertical only - horizontal lines would clutter the stacked-violin rows.
#' @keywords internal
#' @noRd
.distPanelBackground <- function(xlim, theme) {
  usr <- graphics::par("usr")
  graphics::rect(usr[1], usr[3], usr[2], usr[4], col = theme$panel, border = NA)
  gx <- pretty(xlim, n = 5); gx <- gx[gx > xlim[1] & gx < xlim[2]]      # interior ticks only (not on the border)
  if (length(gx)) graphics::abline(v = gx, col = theme$grid, lwd = 0.5)
}

#' Top-row panel: the pooled population density strip for one metric.
#' @keywords internal
#' @noRd
.drawMarginalPanel <- function(g, label, fill, mar, cex, first, theme) {
  graphics::par(mar = mar)
  d <- g$marginal
  ymax <- if (is.null(d)) 1 else max(d$y) * 1.08
  plot(NA, xlim = g$xlim, ylim = c(0, ymax), axes = FALSE, xaxs = "i", yaxs = "i", xlab = "", ylab = "")
  .distPanelBackground(g$xlim, theme)                                    # subtle fill + grid, behind the data
  if (!is.null(d)) {
    xx <- c(d$x[1], d$x, d$x[length(d$x)]); yy <- c(0, d$y, 0)
    graphics::polygon(xx, yy, col = grDevices::adjustcolor(fill, 0.55), border = .distDarken(fill), lwd = 1.4)
  }
  .distRefLines(g$refs, cex)
  if (first) {
    graphics::axis(2, at = pretty(c(0, ymax), n = 2), las = 1, cex.axis = cex * 0.7, tcl = -0.25)
    graphics::mtext("Density", side = 2, line = 2.3, cex = cex * 0.7)
  }
  graphics::mtext(label, side = 3, line = 0.5, font = 2, cex = cex * 0.92)   # column title
  graphics::box()                                                # full border around the panel
}

#' Bottom-row panel: stacked per-deployment horizontal violins for one metric.
#' @keywords internal
#' @noRd
.drawViolinPanel <- function(vals, ord_ids, g, fill, min.n, mar, cex, first, title, theme) {
  ny <- length(ord_ids)
  graphics::par(mar = mar)
  # first id at the TOP: row r (from top) sits at y = ny - r + 1
  plot(NA, xlim = g$xlim, ylim = c(0.4, ny + 0.6), axes = FALSE, xaxs = "i", yaxs = "i", xlab = "", ylab = "")
  .distPanelBackground(g$xlim, theme)                                    # subtle fill + grid, behind the violins
  fill_t <- grDevices::adjustcolor(fill, 0.7)
  for (r in seq_len(ny)) {
    y0 <- ny - r + 1
    x <- vals[[ord_ids[r]]]
    if (length(x) < min.n) next
    d <- stats::density(x, n = 512, na.rm = TRUE)                 # natural support: the violin ends at
    d <- .clampDensity(d, g$xlim)                                 # this deployment's own range (clipped to xlim)
    hw <- 0.44 * d$y / max(d$y)                                   # per-row normalised half-width
    graphics::polygon(c(d$x, rev(d$x)), c(y0 + hw, rev(y0 - hw)), col = fill_t, border = .distDarken(fill), lwd = 0.7)
    graphics::segments(stats::median(x), y0 - 0.34, stats::median(x), y0 + 0.34, col = theme$ink, lwd = 0.9)  # median tick
  }
  .distRefLines(g$refs, cex)
  graphics::axis(1, at = pretty(g$xlim, n = 5), cex.axis = cex * 0.85, tcl = -0.3)
  if (first) {
    graphics::axis(2, at = ny:1, labels = ord_ids, las = 1, cex.axis = cex * 0.72, tick = FALSE, line = -0.4)
    graphics::mtext("Deployment", side = 2, line = mar[2] - 1.4, cex = cex * 0.95, font = 2)
  }
  if (!is.null(title)) graphics::mtext(title, side = 3, line = 0.4, font = 2, cex = cex * 0.92)
  graphics::box()                                                # full border around the panel
}


#######################################################################################################
#######################################################################################################
#######################################################################################################

# plotDives helpers ----

#######################################################################################################
# Per-dive metric comparison across deployments #######################################################
#######################################################################################################
#
# UNIT DRAWN = the dive. UNIT COMPARED = the individual. That pair is the whole design.
#
# WHY NOT A BAR OF PER-INDIVIDUAL MAXIMA (the shape originally parked for this function). A bar encodes
# magnitude as a length from a true zero, and under `reference = "baseline"` no such zero exists at the
# surface: the level the excursion departs from is the seabed, or whatever depth the animal was holding.
# Measured over 52 real deployments / 6,512 dives, `reference = "auto"` resolved MIXED - 4,572 baseline,
# 1,940 surface - and median `max_depth_m` was 23.02 m under baseline against 15.49 m under surface,
# while median `amplitude_m` went the other way (13.67 vs 15.49). The median |max_depth_m - amplitude_m|
# was 6.01 m under baseline and exactly 0.00 m under surface. So a bar of max depth would have drawn a
# seabed offset as dive depth, on two thirds of the dives, silently.
#
# A per-individual MAXIMUM is additionally an extreme whose expectation grows with n, and n ranged from
# 1 to 424 dives per deployment in that same cohort. Hence: every dive is a point, the marker is a
# median with an IQR, and `amplitude_m` (measured from the baseline, so direction-agnostic) is the
# default metric for every taxon.




#' Built-in axis label for a dive metric.
#'
#' Falls back to the column name, so an unknown or user-added metric is still drawable.
#' @keywords internal
#' @noRd
.diveLabel <- function(metric, reference = NULL) {
  base <- .diveLabelBase(metric)
  # An ABSOLUTE depth compared across deployments that were detected against different zeros is not one
  # quantity. Under `reference = "baseline"` the zero is the level the animal was holding, so 30 m on a
  # shelf animal and 30 m on a slope animal are different measurements wearing the same number. Say so
  # in the label rather than in a footnote nobody reads.
  mixed <- !is.null(reference) && length(stats::na.omit(unique(reference))) > 1L
  ifelse(.diveIsDepth(metric) & mixed, paste0(base, " [mixed reference]"), base)
}

#' @keywords internal
#' @noRd
.diveLabelBase <- function(metric) {
  lab <- c(
    amplitude_m         = "Dive amplitude (m)",
    max_depth_m         = "Max. depth (m)",
    mean_depth_m        = "Mean depth (m)",
    sd_depth_m          = "Depth SD (m)",
    baseline_depth_m    = "Baseline depth (m)",
    bottom_depth_mean_m = "Bottom depth (m)",
    duration_s          = "Dive duration (s)",
    descent_duration_s  = "Descent duration (s)",
    bottom_duration_s   = "Bottom duration (s)",
    ascent_duration_s   = "Ascent duration (s)",
    inter_dive_s        = "Inter-dive interval (s)",
    descent_rate_mean   = "Descent rate (m/s)",
    descent_rate_q90    = "Descent rate, q90 (m/s)",
    ascent_rate_mean    = "Ascent rate (m/s)",
    ascent_rate_q90     = "Ascent rate, q90 (m/s)",
    vertical_distance_m = "Vertical distance (m)",
    n_reversals         = "Reversals per dive",
    depth_coverage      = "Depth coverage",
    depth_attenuation   = "Depth attenuation")[metric]
  unname(ifelse(is.na(lab), metric, lab))
}

#' Is this metric an ABSOLUTE depth (measured from a real zero) or a magnitude (no origin to mark)?
#'
#' A property of the metric, never of `direction` - which is why no direction-dependent axis code
#' exists anywhere in this function.
#' @keywords internal
#' @noRd
.diveIsDepth <- function(metric) {
  metric %in% c("max_depth_m", "mean_depth_m", "baseline_depth_m", "bottom_depth_mean_m")
}

#' Which dives may enter the statistics for THIS metric.
#'
#' Two flags, not one. `inter_dive_s` describes the interval BETWEEN two dives and has its own
#' censoring column; every other metric describes the dive itself and follows `complete`. Using
#' `complete` for the interval keeps precisely the row diveMetrics() exists to flag: two uncensored
#' dives with a blackout between them, whose "surface interval" is a property of the sensor.
#' @keywords internal
#' @noRd
.diveUsable <- function(dm, metric) {
  # .asNumericSafe, not as.numeric: a factor coerces to its LEVEL CODES without warning, which is the
  # package's shared coercion contract precisely because plotDistributions was bitten by it - a factored
  # amplitude column came back with median 3 instead of 30 and a perfectly plausible figure.
  v <- .asNumericSafe(dm[[metric]])
  ok <- is.finite(v)
  cens_ok <- if (identical(metric, "inter_dive_s")) {
    if ("inter_dive_censored" %in% names(dm)) !.isTrueVec(dm$inter_dive_censored) else rep(TRUE, nrow(dm))
  } else {
    if ("complete" %in% names(dm)) .isTrueVec(dm$complete) else rep(TRUE, nrow(dm))
  }
  # CENSORING IS DECIDED FIRST. The two exclusions are not independent - a boundary-truncated dive is
  # exactly the dive whose ascent phase fails to resolve - so requiring a finite value before counting a
  # dive as censored moves every censored-AND-valueless dive into "unsupported". Measured on 8 real
  # deployments that reported n_censored = 0 for ascent_rate_q90 while all four censored dives sat in
  # the other bucket, inflating it by 18%: the small number read as zero and contaminated the big one.
  list(value = v,
       used        = ok & cens_ok,
       censored    = !cens_ok,
       unsupported = cens_ok & !ok)
}

#' NA-safe elementwise isTRUE. `NA` is not evidence that a dive is complete, so it must not pass a
#' filter that asks whether the dive is complete.
#' @keywords internal
#' @noRd
.isTrueVec <- function(z) {
  z <- suppressWarnings(as.logical(z))   # as.logical("yes") is a silent NA, not an error
  !is.na(z) & z
}

#' Reduce a dive-metrics table to one row per deployment and metric.
#'
#' The whole honesty contract of plotDives() lives here, with no graphics to argue about: which dives
#' count for which metric, how many were dropped and for which of the two distinct reasons, and whether
#' there is enough left to justify drawing a marker at all.
#' @keywords internal
#' @noRd
.diveSummaryTable <- function(dm, metrics, id.col = "ID", min.n = 5L, groups = NULL) {
  ids <- unique(as.character(dm[[id.col]]))
  rows <- list()
  for (id in ids) {
    sub <- dm[as.character(dm[[id.col]]) == id, , drop = FALSE]
    for (m in metrics) {
      if (!m %in% names(sub)) {
        rows[[length(rows) + 1L]] <- .diveSummaryRow(id, m, groups, sub, NULL, min.n)
        next
      }
      rows[[length(rows) + 1L]] <- .diveSummaryRow(id, m, groups, sub, .diveUsable(sub, m), min.n)
    }
  }
  out <- do.call(rbind, rows)
  rownames(out) <- NULL
  out
}

#' @keywords internal
#' @noRd
.diveSummaryRow <- function(id, metric, groups, sub, u, min.n) {
  n_dives <- nrow(sub)
  vals <- if (is.null(u)) numeric(0) else u$value[u$used]
  q <- if (length(vals)) stats::quantile(vals, c(0.25, 0.5, 0.75), names = FALSE, na.rm = TRUE)
       else rep(NA_real_, 3)
  first_chr <- function(col) {
    if (!col %in% names(sub)) return(NA_character_)
    z <- unique(as.character(sub[[col]])); z <- z[!is.na(z)]
    if (!length(z)) NA_character_ else if (length(z) == 1L) z else paste(sort(z), collapse = "/")
  }
  data.frame(
    id = id,
    # %||% only catches NULL; an id absent from `groups` subscripts to NA, which must stay NA rather
    # than silently become the string "NA" downstream
    group = if (is.null(groups)) NA_character_
            else { g <- unname(groups[id]); if (length(g) != 1L || is.na(g)) NA_character_ else as.character(g) },
    metric = metric,
    n_dives = as.integer(n_dives),
    n_used = if (is.null(u)) 0L else as.integer(sum(u$used)),
    n_censored = if (is.null(u)) 0L else as.integer(sum(u$censored)),
    n_unsupported = if (is.null(u)) as.integer(n_dives) else as.integer(sum(u$unsupported)),
    median = q[2], q25 = q[1], q75 = q[3],
    min = if (length(vals)) min(vals) else NA_real_,
    max = if (length(vals)) max(vals) else NA_real_,
    reference = first_chr("reference"),
    direction = first_chr("direction"),
    # a marker asserts a central tendency; too few dives and the points are the honest answer on their own
    drawn = !is.null(u) && sum(u$used) >= min.n,
    stringsAsFactors = FALSE)
}

#######################################################################################################
# Rendering ###########################################################################################
#######################################################################################################

#' Report the decisions that shaped the figure, before any of them is applied.
#'
#' Every one of these changes what the reader sees - which dives counted, where the axis was cut,
#' whether a median was drawn at all - so they belong in the header, not inferred afterwards from the
#' figure.
#' @keywords internal
#' @noRd
.reportDivePlotSettings <- function(lvl, metrics, trim, min.n, order.by, group.by, n_ids,
                                    max.per.page, n_pages) {
  .log_section(lvl, "Plot settings")
  rows <- c("Metrics"            = paste(metrics, collapse = ", "),
            "Axis trim"          = sprintf("%.3g (values above the %s percentile sit at the edge)",
                                           trim, .ordinalSuffix(round(trim * 100))),
            "Median drawn from"  = sprintf("%d usable dive%s", min.n, if (min.n != 1) "s" else ""),
            "Order"              = switch(order.by,
                                          id = "deployment id", input = "order of appearance",
                                          median = "median of the ordering metric"),
            "Grouping"           = group.by %||% "none",
            "Layout"             = if (n_pages > 1L)
                                     sprintf("%d pages, up to %d deployments each", n_pages, max.per.page)
                                   else sprintf("1 page, %d deployment%s", n_ids,
                                                if (n_ids != 1) "s" else ""))
  .log_rows(lvl, rows)
}

#' Ordinal suffix for a whole percentile ("95th", "90th", "1st").
#' @keywords internal
#' @noRd
.ordinalSuffix <- function(n) {
  s <- if (n %% 100 %in% 11:13) "th" else switch(as.character(n %% 10), "1" = "st", "2" = "nd",
                                                 "3" = "rd", "th")
  paste0(n, s)
}

#' The closing summary: what was drawn, what was left out, and where it went.
#' @keywords internal
#' @noRd
.reportDivePlotSummary <- function(lvl, st, metrics, ids, pages, trim, min.n, plot.file, start.time) {
  .log_summary(lvl)
  agg <- function(f) vapply(metrics, function(m) sum(f(st[st$metric == m, ]), na.rm = TRUE), numeric(1))
  used <- agg(function(z) z$n_used)
  excl <- agg(function(z) z$n_censored + z$n_unsupported)
  offs <- agg(function(z) z$n_trimmed)
  # a dash rather than a zero: a column of zeros reads as a finding, when it means nothing happened
  num <- function(x) ifelse(x > 0, format(x, big.mark = ",", trim = TRUE), "-")

  .log_section(lvl, "Dives per metric")
  .log_table(lvl, data.frame(Metric = metrics, Used = num(used), Excluded = num(excl),
                             `Off-scale` = num(offs), check.names = FALSE,
                             stringsAsFactors = FALSE))
  if (any(excl > 0))
    .log_note(lvl, paste("Excluded dives were censored by the record's start or end, or had no",
                         "descent/bottom/ascent resolved. They are drawn in outline."))
  if (any(offs > 0))
    .log_note(lvl, sprintf(paste("Off-scale dives lie beyond the trimmed axis (trim = %.3g). They are",
                                 "drawn at the axis edge, not removed."), trim))

  # said ONCE for the cohort, not repeated under every metric: it is a property of the deployment
  no_med <- unique(st$id[!st$drawn])
  .log_section(lvl, "Deployments")
  rows <- c("Drawn" = format(length(ids), big.mark = ","))
  if (length(no_med)) {
    shown <- utils::head(no_med, 8L)
    rows <- c(rows, stats::setNames(
      sprintf("%d (%s%s)", length(no_med), paste(shown, collapse = ", "),
              if (length(no_med) > length(shown)) sprintf(", +%d more", length(no_med) - length(shown)) else ""),
      sprintf("Without a median (< %d dives)", min.n)))
  }
  if (length(pages) > 1L) rows <- c(rows, "Pages" = format(length(pages)))
  .log_rows(lvl, rows)

  if (!is.null(plot.file)) {
    .log_section(lvl, "Output")
    .log_rows(lvl, c("File" = plot.file))
  }
  .log_runtime(lvl, start.time)
}

#' Split one metric's dives per deployment into the ones that count and the ones only drawn.
#' @keywords internal
#' @noRd
.pdPerId <- function(dm, metric, ids, id.col) {
  lapply(ids, function(id) {
    sub <- dm[as.character(dm[[id.col]]) == id, , drop = FALSE]
    if (!metric %in% names(sub)) return(list(used = numeric(0), excl = numeric(0)))
    u <- .diveUsable(sub, metric)
    # Non-finite values are dropped HERE, not later: `censored` is decided before finiteness (see
    # .diveUsable), so a boundary-truncated dive whose ascent never resolved is censored AND valueless.
    # Such a dive has no position on the axis, and carrying its NA into the placement arithmetic turns
    # the off-scale test into NA and aborts the panel. It is still counted in the summary, from `st`.
    list(used = u$value[u$used], excl = u$value[u$censored & is.finite(u$value)])
  })
}

#' Value-axis range for one metric, shared by every page.
#'
#' The axis is governed by the dives that COUNT - an excluded outlier must not set the scale for a
#' median it took no part in - but it must still CONTAIN every marker the figure draws, or a deployment
#' whose median sits above the pooled quantile has its marker clipped away silently while the returned
#' table reports `drawn = TRUE`. The floor likewise has to reach the excluded points, or they are
#' clipped and never counted, contradicting the promise that excluded dives are drawn, not removed.
#' @keywords internal
#' @noRd
.pdMetricRange <- function(per_id, st, metric, ids, trim) {
  used_all <- unlist(lapply(per_id, `[[`, "used"))
  excl_all <- unlist(lapply(per_id, `[[`, "excl"))
  if (!length(used_all)) used_all <- 0
  hi <- stats::quantile(used_all, trim, names = FALSE, na.rm = TRUE)
  mk <- st[st$metric == metric & st$id %in% ids & st$drawn, , drop = FALSE]
  if (nrow(mk)) hi <- max(hi, max(mk$q75, mk$median, na.rm = TRUE), na.rm = TRUE)
  lo <- min(c(used_all, excl_all, if (nrow(mk)) mk$q25 else NULL, 0), na.rm = TRUE)
  if (!is.finite(hi) || hi <= lo) hi <- lo + 1
  c(lo, hi)
}

#' Clip the group blocks of a grouped layout to the deployments on ONE page, in panel coordinates.
#' @keywords internal
#' @noRd
.pdPageBlocks <- function(blocks, xp) {
  if (is.null(blocks)) return(NULL)
  top <- max(xp); rng <- range(xp)
  out <- lapply(blocks, function(b) {
    s <- max(b[1], rng[1]); e <- min(b[2], rng[2])
    if (s > e) NULL else c(top - e + 1, top - s + 1)
  })
  out <- out[!vapply(out, is.null, logical(1))]
  if (length(out)) out else NULL
}

#' Fold one page's draw record into the cohort-wide record.
#'
#' The record is the figure's audit trail and describes the whole cohort, so pagination must not make it
#' describe only the last page drawn.
#' @keywords internal
#' @noRd
.pdMergeRecords <- function(acc, rec) {
  if (is.null(acc)) return(rec)
  acc$ids     <- c(acc$ids, rec$ids)
  acc$ypos    <- c(acc$ypos, rec$ypos)
  acc$trimmed <- c(acc$trimmed, rec$trimmed)
  acc$clipped <- acc$clipped + rec$clipped
  acc$markers <- c(acc$markers, rec$markers)
  acc
}

#' Draw ONE metric panel into the current plot region.
#'
#' Every dive is drawn. Dives excluded from the statistics are drawn in outline rather than removed,
#' because a reader cannot judge a median they cannot see the basis of - and the two reasons a dive is
#' excluded (censored, phase-unsupported) are not interchangeable, so the panel note names both.
#' @keywords internal
#' @noRd
.pdDrawPanel <- function(dm, st, metric, ids, theme, trim, id.col, show.labels = TRUE,
                         label = NULL, ypos = NULL, cols = NULL, blocks = NULL, gutter = TRUE,
                         xlim = NULL) {
  # ONE ROW PER DEPLOYMENT, metric value on x. The transpose is what makes a large cohort readable: the
  # deployment count now drives HEIGHT, which can grow and paginate, instead of WIDTH, which cannot.
  # At 49 deployments the old geometry asked for a 56-inch canvas, so every glyph and label was scaled
  # into illegibility by whatever rendered it.
  ny   <- length(ids)
  ypos <- ypos %||% rev(seq_len(ny))                 # first id at the TOP, as in plotDistributions
  cols <- cols %||% rep(theme$axis, ny)

  per_id <- .pdPerId(dm, metric, ids, id.col)

  # A paginated figure whose pages carry different axes is not a comparison, so the caller fixes the
  # range across the whole cohort once and passes it in; computing it here is the single-page fallback.
  rng <- xlim %||% .pdMetricRange(per_id, st, metric, ids, trim)
  lo <- rng[1]; hi <- rng[2]

  graphics::plot.new()
  # the y span follows `ypos`, not the deployment count, so the gaps a grouped layout inserts between
  # blocks are honoured instead of pushing the last rows off the panel
  graphics::plot.window(xlim = c(lo, hi), ylim = c(min(ypos) - 0.6, max(ypos) + 0.6),
                        xaxs = "i", yaxs = "i")
  usr <- graphics::par("usr")
  graphics::rect(usr[1], usr[3], usr[2], usr[4], col = theme$panel, border = NA)
  ticks <- graphics::axTicks(1)
  graphics::abline(v = ticks, col = theme$grid, lwd = 0.8)      # vertical only: y is categorical now
  # An ABSOLUTE depth is measured from a zero that means something (the surface, or the baseline the
  # animal was holding), so that zero is marked. The axis is NOT inverted: inverting depth is a
  # convention for depth on the VERTICAL axis, and here the value runs left to right like every other
  # metric - flipping it would put the shallowest deployment on the right of the deepest.
  zero_line <- .diveIsDepth(metric)
  if (zero_line) graphics::abline(v = 0, col = theme$ink, lty = 2, lwd = 0.9)

  clipped <- 0L
  trimmed <- integer(ny)
  for (i in seq_len(ny)) {
    d <- per_id[[i]]
    n <- length(d$used)
    # a dense cloud and a sparse one must both stay readable, so alpha tracks n rather than being fixed
    al <- max(0.25, min(0.75, 0.75 * sqrt(30 / max(n, 1))))
    yi <- ypos[i]
    # Deterministic spread, not runif(): a published figure must redraw identically, and a golden-ratio
    # sequence scatters more evenly across the band than random jitter does at small n.
    jit <- function(k) if (!k) numeric(0) else yi + (((seq_len(k) * 0.6180339887498949) %% 1) - 0.5) * 0.54
    # na.rm even though .pdPerId already filters: a single NA reaching here would make the off-scale
    # test NA and abort the whole panel, which is a poor failure mode for a plotting function
    pin <- function(v) list(inside = v[!is.na(v) & v >= lo & v <= hi],
                            edge = sum(v > hi | v < lo, na.rm = TRUE))
    pu <- pin(d$used); pe <- pin(d$excl)
    clipped <- clipped + pu$edge + pe$edge
    if (length(pe$inside))
      graphics::points(pe$inside, jit(length(pe$inside)), pch = 1, cex = 0.62,
                       col = grDevices::adjustcolor(theme$ink, alpha.f = 0.5))
    if (length(pu$inside))
      graphics::points(pu$inside, jit(length(pu$inside)), pch = 16, cex = 0.7,
                       col = grDevices::adjustcolor(cols[i], alpha.f = al))
    if (pu$edge + pe$edge > 0) {
      # chevrons rather than the old up/down triangles: the axis runs left-to-right now, so the marker
      # has to point the way the values went
      above <- sum(d$used > hi, na.rm = TRUE) + sum(d$excl > hi, na.rm = TRUE)
      below <- (pu$edge + pe$edge) - above
      # inset by a hair so the glyph sits INSIDE the panel: centred exactly on the limit, half of it
      # spills into the gutter and collides with that row's n=
      pad <- 0.014 * (hi - lo)
      if (above) graphics::points(hi - pad, yi, pch = 62, cex = 0.95, col = theme$ink)
      if (below) graphics::points(lo + pad, yi, pch = 60, cex = 0.95, col = theme$ink)
    }
    trimmed[i] <- pu$edge + pe$edge

    s <- st[st$id == ids[i] & st$metric == metric, ]
    if (nrow(s) && isTRUE(s$drawn)) {
      graphics::segments(s$q25, yi, s$q75, yi, lwd = 1.9, col = theme$ink)
      graphics::segments(c(s$q25, s$q75), c(yi, yi) - 0.13, c(s$q25, s$q75), c(yi, yi) + 0.13,
                         lwd = 1.9, col = theme$ink)          # caps, so the IQR reads as a range
      graphics::segments(s$median, yi - 0.26, s$median, yi + 0.26, lwd = 2.8, col = theme$ink)
    }
    # the usable count, in a fixed right-hand gutter. Always the same place on every row, so it can be
    # scanned or ignored; under each column (as before) it was unreadable at any realistic figure size.
    if (gutter && nrow(s))
      graphics::mtext(paste0("n=", s$n_used), side = 4, at = yi, line = 0.35, las = 1,
                      cex = 0.62 * theme$cex, col = grDevices::adjustcolor(theme$ink, alpha.f = 0.85))
  }

  # The metric is a COLUMN TITLE and the value axis carries bare ticks: with one panel per metric the
  # units belong at the head of the column, not repeated beneath every one of them.
  graphics::axis(1, at = ticks, cex.axis = 0.78 * theme$cex, col = NA, col.ticks = theme$axis,
                 col.axis = theme$ink)
  graphics::mtext(label %||% .diveLabel(metric), side = 3, line = 0.5, font = 2,
                  cex = 0.82 * theme$cex, col = theme$ink)
  # deployment labels once per page, on the leftmost panel: repeating them per column would spend the
  # width the metrics need
  if (show.labels) {
    graphics::axis(2, at = ypos, labels = ids, las = 1, tick = FALSE, line = -0.4,
                   cex.axis = 0.76 * theme$cex, col.axis = theme$ink)
    if (!is.null(blocks))
      for (l in names(blocks))
        graphics::mtext(l, side = 2, line = 5.1, at = mean(blocks[[l]]), font = 2,
                        cex = 0.8 * theme$cex, col = theme$ink)
  }
  graphics::box(col = theme$grid)
  invisible(list(metric = metric, xlim = c(lo, hi), lo = lo, hi = hi, zero_line = zero_line,
                 ylim = c(min(ypos) - 0.6, max(ypos) + 0.6),
                 ids = ids, ypos = ypos, trimmed = stats::setNames(trimmed, ids),
                 clipped = clipped,
                 markers = stats::setNames(lapply(seq_along(ids), function(i) {
                   z <- st[st$id == ids[i] & st$metric == metric, ]
                   if (!nrow(z) || !isTRUE(z$drawn)) NULL
                   else c(median = z$median, q25 = z$q25, q75 = z$q75)
                 }), ids)))
}

#' Draw the figure key into the bottom outer margin.
#'
#' The glyphs carry meaning that no axis states (which dives counted, which were only drawn, where the
#' axis was cut), so the key is drawn with the ACTUAL symbols rather than described in a subtitle - a
#' reader should not have to translate "open = excluded" into what is on the panel.
#' @keywords internal
#' @noRd
.pdDrawKey <- function(theme) {
  op <- graphics::par(fig = c(0, 1, 0, 1), oma = c(0, 0, 0, 0), mar = c(0, 0, 0, 0), new = TRUE)
  on.exit(graphics::par(op), add = TRUE)
  graphics::plot.new()
  faded <- grDevices::adjustcolor(theme$ink, alpha.f = 0.6)
  graphics::legend("bottom", horiz = TRUE, bty = "n", xpd = NA, inset = c(0, 0),
                   cex = 0.72 * theme$cex, text.col = theme$ink,
                   legend = c("dive (used)", "dive (excluded)", "beyond axis", "median with IQR"),
                   pch = c(16, 1, 62, NA), lty = c(NA, NA, NA, 1), lwd = c(NA, NA, NA, 2.2),
                   pt.cex = c(0.9, 0.8, 0.9, NA), col = c(faded, faded, theme$ink, theme$ink))
  invisible(NULL)
}

#' Accept a metrics table, or a list of them, and return one bound table.
#'
#' Deliberately does NOT accept processed tags and run the detector for you: the dive threshold is a
#' scientific decision that must be visible in the call that made it, not buried in a plotting call.
#' @keywords internal
#' @noRd
.pdBindMetrics <- function(data, id.col) {
  # BEFORE the generic emptiness guard, whose advice is about mistyped directories and would be wrong
  # here. A zero-row dive table is not a path error; it is a cohort in which nothing was detected, and
  # that deserves its own message. Checked first, or this branch is unreachable.
  if (is.data.frame(data) && !nrow(data))
    .abort(c("{.arg data} has no rows: there are no dives to plot.",
             "i" = "A cohort with no detected dives is a result, not a failure - check {.fn detectDives}.",
             "i" = "If the detection floor is too high, lower {.code diveControl(depth.threshold = )}."))
  .assert_nonempty(data, "data")
  if (is.data.frame(data)) dm <- data
  else if (is.list(data)) {
    parts <- Filter(function(z) is.data.frame(z) && nrow(z), data)
    if (!length(parts))
      .abort(c("{.arg data} holds no dive-metrics rows.",
               "i" = "Pass the result of {.fn diveMetrics}."))
    dm <- do.call(rbind, parts)
  } else {
    .abort(c("{.arg data} must be a {.fn diveMetrics} table, or a list of them.",
             "i" = "Got {.cls {class(data)[1]}}."))
  }
  # `x == NA` is NA and `df[NA, ]` returns a row of NAs rather than none, so an NA id would both invent
  # a phantom deployment and inflate every real one's counts. Drop them loudly.
  if (id.col %in% names(dm) && anyNA(dm[[id.col]])) {
    n_na <- sum(is.na(dm[[id.col]]))
    cli::cli_warn(c("{n_na} row{?s} ha{?ve/s} no {.field {id.col}} and {?was/were} dropped.",
                    "i" = "A dive with no deployment id cannot be placed on the figure."))
    dm <- dm[!is.na(dm[[id.col]]), , drop = FALSE]
    if (!nrow(dm)) .abort("Every row lacks a {.field {id.col}}; there is nothing to plot.")
  }
  need <- c(id.col, "dive_id", "duration_s")
  miss <- setdiff(need, names(dm))
  if (length(miss))
    .abort(c("{.arg data} is missing {.field {miss}}, so it is not a {.fn diveMetrics} table.",
             "i" = "Run {.fn diveMetrics} on the output of {.fn detectDives}."))
  dm
}

#' Order the deployment slots. One order, shared by every panel.
#'
#' Panels that ordered themselves would let a reader compare the wrong columns between them, so the
#' order is decided once from `order.metric` and applied everywhere - the same rule plotDistributions
#' uses, and the same direction (largest median first).
#' @keywords internal
#' @noRd
.pdOrder <- function(id_vec, order.by, st, order.metric) {
  ids <- unique(id_vec)                              # order of first appearance = "input"
  if (identical(order.by, "input")) return(ids)
  if (identical(order.by, "id")) return(sort(ids))
  meds <- vapply(ids, function(id) {
    v <- st$median[st$id == id & st$metric == order.metric]
    if (!length(v)) NA_real_ else v[1]
  }, numeric(1))
  ids[order(meds, decreasing = TRUE, na.last = TRUE)]
}

#' Resolve a panel's axis label: an explicit `labels` entry wins, otherwise the built-in one.
#'
#' Extracted rather than left inline so it can be tested directly. A label only ever reaches the figure,
#' never the returned table, so a test that calls plotDives() and checks for no error proves nothing
#' about whether the override was honoured - which is how a broken override survived a first pass here.
#' @keywords internal
#' @noRd
.pdResolveLabel <- function(metric, labels = NULL, reference = NULL) {
  if (!is.null(labels) && metric %in% names(labels)) unname(labels[[metric]])
  else .diveLabel(metric, reference)
}

#' Resolve a per-deployment grouping label from the flexible `group` spec.
#'
#' Blank strings are not group labels - they arrive from spreadsheets constantly and would otherwise
#' become a group called "".
#' @keywords internal
#' @noRd
.pdGroups <- function(dm, id.col, group) {
  if (is.null(group)) return(NULL)
  ids <- unique(as.character(dm[[id.col]]))
  g <- vapply(ids, function(id) {
    sub <- dm[as.character(dm[[id.col]]) == id, , drop = FALSE]
    as.character(.deploymentGroup(sub, id, group))[1]
  }, character(1))
  g[!is.na(g) & !nzchar(trimws(g))] <- NA_character_
  if (all(is.na(g)))
    cli::cli_warn(c("No deployment has a usable {.arg group.by} value; drawing one ungrouped block.",
                    "i" = "Check that the grouping column or lookup covers the deployment ids."))
  stats::setNames(g, ids)
}

#' Slot positions, colours and block spans for the categorical axis.
#'
#' Deployments a grouping does not cover go into a trailing `(ungrouped)` block rather than being
#' dropped. Every deployment owns a slot here - unlike a facetted plotter, where an ungrouped
#' deployment has no panel to live in - so silently omitting one would mean a figure that claims to
#' show a cohort while showing part of it.
#' @keywords internal
#' @noRd
.pdLayout <- function(ids, grp, palette = NULL, gap = 0.75) {
  if (is.null(grp)) {
    return(list(ids = ids, xpos = seq_along(ids), cols = NULL, blocks = NULL))
  }
  g <- grp[ids]
  lev <- sort(unique(stats::na.omit(g)))
  ung <- any(is.na(g))
  ord <- unlist(lapply(lev, function(l) ids[!is.na(g) & g == l]), use.names = FALSE)
  if (ung) ord <- c(ord, ids[is.na(g)])
  gl <- c(lev, if (ung) "(ungrouped)")
  key <- ifelse(is.na(grp[ord]), "(ungrouped)", grp[ord])

  x <- numeric(length(ord)); at <- 0
  blocks <- list()
  for (l in gl) {
    w <- which(key == l)
    x[w] <- at + seq_along(w)
    blocks[[l]] <- c(at + 1, at + length(w))
    at <- at + length(w) + gap
  }
  pal <- .themePalette(palette, length(gl))
  list(ids = ord, xpos = x, cols = stats::setNames(pal, gl)[key],
       blocks = blocks)
}

# plotTheme helpers ----

#######################################################################################################
# Plot theme (a shared, publication-ready visual style; one object customises every nautilus plot) ###
#######################################################################################################

#' Darken a colour toward black by a multiplicative factor.
#'
#' Used to give a filled bar an outline in its own hue rather than a contrasting one: a border drawn
#' from the fill reads as definition, while a white or grey border reads as a separate element and
#' fragments a stack of bars. 0.72 is dark enough to hold an edge against the fill at print size
#' without turning into a black keyline.
#' @param col Any colour R understands.
#' @param factor Multiplier applied to each RGB channel; below 1 darkens.
#' @keywords internal
#' @noRd
.darkenColor <- function(col, factor = 0.72) {
  rgb <- grDevices::col2rgb(col, alpha = FALSE) / 255
  grDevices::rgb(rgb[1, ] * factor, rgb[2, ] * factor, rgb[3, ] * factor)
}

#' Base field set for each theme preset.
#' @keywords internal
#' @noRd
.themePreset <- function(preset) {
  common <- list(palette = c("#2FA4A0", "#E7913B", "#3E86C0", "#8B6BB1", "#C25B56", "#7FA65E", "#5B7FBD", "#C77F9E"),
                 # The ramp a mapped variable is drawn through. These stops are the package's original
                 # depth-profile palette: the theme migration replaced them with a three-stop blue
                 # sequential ramp, believing it was moving only the SOURCE of the colours, but for a
                 # colour-mapped trace the base colours ARE the palette, and a page of profiles went
                 # monochrome. Restored, and locked by a test.
                 sequential = c("#020275", "#0808E5", "#4272CB", "#17E2E2", "#7FE07F", "#E2E207",
                                "#DC7534", "#E50303", "#740101"),
                 bar.alpha = 1, font.family = "", cex = 1)
  switch(preset,
    light   = c(list(panel = "grey97", grid = "grey88", ink = "#1B1F27", axis = "#586074",
                     subtitle = "#9AA1B0", day = "#F6D2AE", night = "#294763",
                     day.border = .darkenColor("#F6D2AE"), bar.border = "#FFFFFF"), common),
    minimal = c(list(panel = "#FFFFFF", grid = "#E7E9EE", ink = "#1B1F27", axis = "#586074",
                     subtitle = "#9AA1B0", day = "#F6D2AE", night = "#294763",
                     day.border = .darkenColor("#F6D2AE"), bar.border = "#FFFFFF"), common),
    classic = c(list(panel = "#EDEDED", grid = "#FFFFFF", ink = "#000000", axis = "#333333",
                     subtitle = "#666666", day = "#FFFFFF", night = "#4D4D4D", day.border = "#333333",
                     bar.border = "#4D4D4D"),
                utils::modifyList(common, list(palette = grey.colors(6, 0.25, 0.75), bar.border = "#4D4D4D"))))
}

#' Is `x` a single, valid colour?
#' @keywords internal
#' @noRd
.isColour <- function(x) {
  # col2rgb(NA) SUCCEEDS - it returns white - so a bare tryCatch would accept NA as a valid colour and
  # the element would silently draw transparent or white instead of aborting. A missing colour is a
  # mistake to report, not a colour to honour.
  if (length(x) != 1L || is.na(x)) return(FALSE)
  isTRUE(tryCatch({ grDevices::col2rgb(x); TRUE }, error = function(e) FALSE))
}

#' Resolve a theme's `palette` field to exactly `n` colours (a colour vector, or an hcl.colors name).
#' @keywords internal
#' @noRd
.themePalette <- function(spec, n) {
  n <- max(1L, n)
  if (length(spec) == 1L && is.character(spec) && !.isColour(spec)) {
    ok <- tryCatch(grDevices::hcl.colors(max(n, 2L), spec), error = function(e) NULL)
    if (!is.null(ok)) return(ok[seq_len(n)])
    .abort("{.arg palette} {.val {spec}} is not a colour or a known {.fn hcl.colors} palette.")
  }
  rep(spec, length.out = n)[seq_len(n)]
}

# plotTimeAtDepth helpers ----

################################################################################
# Binning (duration-weighted) #################################################
################################################################################

#' Duration-weighted time and percent per bin for one deployment (gaps capped to the median interval).
#' @keywords internal
#' @noRd
.timeAtDepthBins <- function(v, tnum, breaks, gap.factor) {
  ok <- is.finite(v) & is.finite(tnum); v <- v[ok]; tnum <- tnum[ok]
  nb <- length(breaks) - 1L
  if (length(v) < 2L) return(list(time = rep(0, nb), pct = rep(0, nb)))
  o <- order(tnum); v <- v[o]; tnum <- tnum[o]
  gaps <- diff(tnum); modal <- stats::median(gaps[gaps > 0]); if (!is.finite(modal) || modal <= 0) modal <- 1
  # Repeated timestamps (e.g. a 20 Hz record whose stamps were rounded to whole seconds) represent NO
  # elapsed time. Crediting each the modal interval, as the blanket replacement below used to, inflated
  # the total by the duplication factor - a 1-hour record reported as 20 hours. Only OVERLONG gaps are
  # capped at the modal interval; non-positive ones are worth nothing.
  w <- c(gaps, modal)
  w[!is.finite(w) | w > gap.factor * modal] <- modal
  w[w <= 0] <- 0
  idx <- findInterval(v, breaks, rightmost.closed = TRUE, all.inside = TRUE)
  tsum <- as.numeric(tapply(w, factor(idx, levels = seq_len(nb)), sum)); tsum[is.na(tsum)] <- 0
  list(time = tsum, pct = if (sum(w) > 0) 100 * tsum / sum(w) else rep(0, nb))
}

#' Bin breaks: explicit `breaks`, else `bin.width`/`n.bins`, else a smart per-variable default.
#' @keywords internal
#' @noRd
.tadResolveBreaks <- function(variable, rng, bin.width, breaks, n.bins) {
  if (!is.null(breaks)) return(sort(unique(as.numeric(breaks))))
  if (!is.null(bin.width) || !is.null(n.bins)) return(.tadBreaks(rng, bin.width, n.bins %||% 20L))
  .tadDefaultBreaks(variable, rng)
}

#' ~`n.bins` pretty bins, or an exact `bin.width`.
#' @keywords internal
#' @noRd
.tadBreaks <- function(rng, bin.width, n.bins) {
  if (!is.null(bin.width) && bin.width > 0) {
    lo <- floor(rng[1] / bin.width) * bin.width; hi <- ceiling(rng[2] / bin.width) * bin.width
    return(seq(lo, if (hi <= lo) lo + bin.width else hi, by = bin.width))
  }
  b <- pretty(rng, n = n.bins); if (length(b) < 2) b <- range(rng) + c(0, if (diff(rng) == 0) 1 else 0); b
}

#' Smart default bins: surface-fine / deep-coarse for depth; uniform pretty bins otherwise.
#' @keywords internal
#' @noRd
.tadDefaultBreaks <- function(variable, rng) {
  if (!identical(variable, "depth")) return(pretty(rng, 15))
  tmpl <- c(0, 2, 4, 6, 8, 10, 15, 20, 30, 50, 75, 100, 150, 200, 300, 400, 600, 800, 1000, 1500, 2000, 3000, 5000)
  below <- tmpl[tmpl < rng[2]]; cover <- tmpl[tmpl >= rng[2]][1]
  b <- c(below, if (!is.na(cover)) cover else max(tmpl))
  b <- unique(b[b >= floor(min(rng[1], 0))])
  if (length(b) < 2) pretty(rng, 12) else b
}

################################################################################
# Grouping + diel helpers ######################################################
################################################################################

#' Ordered group levels for faceting.
#' @keywords internal
#' @noRd
.tadGroupLevels <- function(loaded, order.by, variable) {
  g <- vapply(loaded, function(d) d$group %||% NA_character_, character(1))
  g <- g[!is.na(g) & nzchar(trimws(g))]        # a blank cell is not a group; gcols[[""]] was a hard error
  lv <- unique(g)
  if (order.by == "median") {
    # a deployment that lacks the ordering variable contributes nothing here; unlist() on all-NULL used
    # to yield numeric(0), which vapply rejects with an opaque length error. Such a group sorts last.
    med <- vapply(lv, function(gg) {
      ids <- names(g)[g == gg]
      z <- unlist(lapply(loaded[ids], function(d) d$series[[variable[1]]]))
      z <- z[is.finite(z)]
      if (!length(z)) NA_real_ else stats::median(z)
    }, numeric(1))
    lv <- lv[order(med, na.last = TRUE)]
  } else if (order.by == "id") lv <- sort(lv)
  lv
}

#' A single representative day/night phase vector for one deployment (or NULL if no coords).
#' @keywords internal
#' @noRd
.tadDielPhase <- function(x, coords, id, datetime.col, lvl) {
  co <- .tadCoords(x, coords, id)
  if (is.null(co)) { if (lvl >= 1L) cli::cli_alert_warning("{.val {id}}: no coords for diel split; skipped."); return(NULL) }
  as.character(getDielPhase(x[[datetime.col]], matrix(co, ncol = 2), phases = 2))
}

#' Validate the shape of `coords` up front.
#'
#' Every malformed shape used to fail silently: `.tadCoords()` simply fell through to NULL, so the diel
#' split was skipped for every deployment and the run ended in a blank figure (or, for a two-column
#' data.frame, a bare "undefined columns selected" from deep inside the resolver).
#' @keywords internal
#' @noRd
.tadCheckCoordinates <- function(coords) {
  bad <- function(...) .abort(c(..., "i" = "Give {.arg coords} as {.code c(lon, lat)}, a named {.code id -> c(lon, lat)} list, or {.code data.frame(id, lon, lat)}."))
  if (is.data.frame(coords)) {
    if (ncol(coords) < 3L) bad("{.arg coords} data.frame needs three columns (id, lon, lat), not {ncol(coords)}.")
    .tadCheckLonLat(coords[[2]], coords[[3]])
    return(invisible(TRUE))
  }
  if (is.list(coords)) {
    if (is.null(names(coords))) bad("{.arg coords} list must be named by deployment id.")
    for (nm in names(coords)) {
      z <- suppressWarnings(as.numeric(coords[[nm]]))
      if (length(z) != 2L || anyNA(z)) bad("{.arg coords[[{nm}]]} must be two finite numbers {.code c(lon, lat)}.")
      .tadCheckLonLat(z[1], z[2])
    }
    return(invisible(TRUE))
  }
  if (!is.numeric(coords) || length(coords) != 2L || anyNA(coords))
    bad("{.arg coords} must be two finite numbers {.code c(lon, lat)}.")
  .tadCheckLonLat(coords[1], coords[2])
  invisible(TRUE)
}

#' Longitude/latitude range check (a swapped pair is the usual cause of an out-of-range latitude).
#' @keywords internal
#' @noRd
.tadCheckLonLat <- function(lon, lat) {
  lon <- suppressWarnings(as.numeric(lon)); lat <- suppressWarnings(as.numeric(lat))
  ok <- is.finite(lon) & is.finite(lat)
  if (any(!ok)) .abort("{.arg coords} contains non-finite longitude/latitude values.")
  if (any(abs(lat) > 90)) .abort(c("{.arg coords} latitude out of range: {.val {lat[abs(lat) > 90][1]}} (must be -90 to 90).",
                                   "i" = "Longitude and latitude may be the wrong way round."))
  if (any(abs(lon) > 180)) .abort("{.arg coords} longitude out of range: {.val {lon[abs(lon) > 180][1]}} (must be -180 to 180).")
  invisible(TRUE)
}

#' Resolve a single representative c(lon, lat) for a deployment (data cols -> arg -> metadata).
#' @keywords internal
#' @noRd
.tadCoords <- function(x, coords, id) {
  ln <- intersect(c("lon", "longitude"), names(x)); la <- intersect(c("lat", "latitude"), names(x))
  if (length(ln) && length(la)) {
    lo <- stats::median(x[[ln[1]]], na.rm = TRUE); aa <- stats::median(x[[la[1]]], na.rm = TRUE)
    if (is.finite(lo) && is.finite(aa)) return(c(lo, aa))
  }
  if (!is.null(coords)) {
    if (is.numeric(coords) && length(coords) == 2) return(coords)
    if (is.list(coords) && !is.data.frame(coords) && !is.null(coords[[id]])) return(as.numeric(coords[[id]]))
    if (is.data.frame(coords)) { hit <- coords[as.character(coords[[1]]) == id, , drop = FALSE]
      if (nrow(hit)) return(as.numeric(hit[1, 2:3])) }
  }
  m <- tryCatch(.getMeta(x)$deployment, error = function(e) NULL)
  lo <- m$lon %||% m$longitude; aa <- m$lat %||% m$latitude          # canonical fields are lon/lat
  if (!is.null(lo) && !is.null(aa) && is.finite(lo[1]) && is.finite(aa[1])) return(c(lo[1], aa[1]))
  NULL
}

################################################################################
# Labels ######################################################################
################################################################################

#' @keywords internal
#' @noRd
.tadLabel <- function(v) switch(v, depth = "Depth bin (m)", temp = "Temperature bin (\u00b0C)", sprintf("%s bin", v))

#' @keywords internal
#' @noRd
.tadTitle <- function(v) switch(v, depth = "time-at-depth", temp = "time-at-temperature", sprintf("time-at-%s", v))

#' Seconds-since-epoch for a deployment's time column, or abort naming the column.
#'
#' The bin weights are elapsed SECONDS. `as.numeric()` was applied blind, so a Date column (days) came
#' out 86400x too small and a character column came out all-NA - both of which produced a complete,
#' plausible-looking, entirely zero figure that the console still reported as plotted.
#' @keywords internal
#' @noRd
.tadTimeSeconds <- function(z, datetime.col, id) {
  tnum <- .asTimeSeconds(z)                                          # shared contract (see utils-plot.R)
  if (!is.null(tnum)) return(tnum)
  .abort(c("{.arg datetime.col} ({.val {datetime.col}}) must hold date-times, not {.cls {class(z)[1]}}.",
           "i" = "Deployment {.val {id}} carries a {.cls {class(z)[1]}} column; convert it with {.fn as.POSIXct} first."))
}

#' First free `id_2`, `id_3`, ... for a deployment id that collides with one already loaded.
#' @keywords internal
#' @noRd
.tadUniqueId <- function(id, taken) {
  k <- 2L
  while (paste0(id, "_", k) %in% taken) k <- k + 1L
  paste0(id, "_", k)
}

#' Capitalise the first character (panel/banner titles are sentence case package-wide).
#' @keywords internal
#' @noRd
.tadCap1 <- function(s) { substr(s, 1, 1) <- toupper(substr(s, 1, 1)); s }

#' Spelled-out variable name for prose (console summary): "temp" is a column name, not a word.
#' @keywords internal
#' @noRd
.tadVarName <- function(v) switch(v, depth = "depth", temp = "temperature", v)

#' Measurement unit suffix for a variable, "" when unknown.
#' @keywords internal
#' @noRd
.tadUnit <- function(v) switch(v, depth = " m", temp = " \u00b0C", "")

#' Whether a variable's axis runs downwards.
#'
#' Depth alone: a depth axis reads down the page because that is the direction the animal travels, and
#' a profile drawn the other way up is hard to read against a dive trace. Every other variable keeps the
#' conventional orientation - on a temperature panel, inverting the axis would put the coldest water at
#' the top for no reason. An unrecognised variable is treated conventionally.
#' @keywords internal
#' @noRd
.tadFlipY <- function(v) identical(v, "depth")

#' One bin's range as prose, e.g. "0-2 m" / "22-24 degrees C" (en dash), for the console summary.
#' The top bin is open-ended in the plot's own labels, so match that here rather than implying a
#' ceiling the data may exceed.
#' @keywords internal
#' @noRd
.tadBinRange <- function(breaks, i, v) {
  u <- .tadUnit(v)
  if (i >= length(breaks) - 1L) return(sprintf("> %g%s", breaks[i], u))
  sprintf("%g\u2013%g%s", breaks[i], breaks[i + 1L], u)
}

#' @keywords internal
#' @noRd
.tadXlab <- function(v) sprintf("Time at %s (%% per deployment)", .tadVarName(v))

#' Bin-range labels ("0-10"); the last bin becomes ">X" (open-ended top).
#' @keywords internal
#' @noRd
.binLabels <- function(breaks) {
  lo <- breaks[-length(breaks)]; hi <- breaks[-1]
  lab <- sprintf("%g-%g", lo, hi); lab[length(lab)] <- sprintf(">%g", lo[length(lo)]); lab
}

################################################################################
# Rendering: profiles ##########################################################
################################################################################

#' Figure size (inches).
#'
#' Height follows the BIN COUNT rather than a flat per-row constant: a panel showing 20 depth strata needs
#' more height than one showing 6 temperature strata, and the old fixed 2.7 in/row squeezed the default
#' depth ladder into ~7 pt bands - which is why the bin labels used to be thinned to every second one.
#' Growth is capped so a grouped x 2-variable figure cannot run to a page metres long.
#'
#' Width is floored by the BANNER, not just the panel grid: the title strip spans the whole figure, so a
#' narrow single-variable canvas used to truncate its own subtitle mid-word.
#' @keywords internal
#' @noRd
.tadFigSize <- function(n_var, n_grp, style, cex, nb_max = 12L, n_dep = 1L, banner_in = 0) {
  s <- max(1, cex * 0.92)
  if (style == "heatmap") {
    return(list(width  = max(1.5 + 3.6 * n_var, 1.5 + 0.30 * n_dep) * s,
                height = max(5.4, 1.5 + 0.20 * nb_max) * s))
  }
  lay <- .tadPanelGrid(n_var, n_grp)
  # data region grows with bins; chrome (axes + panel titles) is roughly constant per row
  # The data region is what the reader looks at, so it gets the growth: ~30% taller bars and a wider
  # panel, with the per-row chrome allowance trimmed so the figure itself grows by less than the panel
  # does and the layout does not turn spindly.
  data_in <- min(max(2.99, 0.176 * nb_max), 5.85)
  row_in  <- data_in + 1.18
  list(width  = max(1.1 + 3.6 * lay$nc, banner_in) * s,
       height = (0.9 + row_in * lay$nr) * s)
}

#' Banner strings, needed both to draw the strip and to size the canvas that must hold it.
#' @keywords internal
#' @noRd
.tadBannerText <- function(variable, grouped, diel) {
  cap1 <- function(s) { substr(s, 1, 1) <- toupper(substr(s, 1, 1)); s }
  ttl <- paste0(cap1(paste(vapply(variable, .tadTitle, ""), collapse = " and ")), if (grouped) " by group" else "")
  sub <- if (grouped) "Group means \u00b1 SE across individuals" else "Pooled across individuals (mean per deployment \u00b1 SE)"
  if (diel) sub <- paste0(sub, " \u00b7 night vs day")
  list(title = ttl, subtitle = sub)
}

#' Inches of canvas the banner needs (title at 1.5 cex, subtitle at 1.0, plus the diel key).
#' strwidth needs a device, so measure on a throwaway one rather than guessing character widths.
#' @keywords internal
#' @noRd
.tadBannerWidth <- function(banner, diel, cex) {
  w <- tryCatch({
    grDevices::pdf(NULL, width = 7, height = 7); on.exit(grDevices::dev.off(), add = TRUE)
    graphics::par(mar = c(0, 0, 0, 0)); graphics::plot.new()
    max(graphics::strwidth(banner$title, units = "inches", cex = 1.5 * cex),
        graphics::strwidth(banner$subtitle, units = "inches", cex = 1.0 * cex))
  }, error = function(e) NA_real_)
  if (!is.finite(w)) return(0)
  w + 0.30 + if (diel) 1.45 else 0                     # left inset + room for the night/day key
}

#' Panel grid dimensions (nr x nc) for the profile layout.
#' @keywords internal
#' @noRd
.tadPanelGrid <- function(n_var, n_grp) {
  if (n_grp == 1) return(list(nr = 1L, nc = n_var))            # pooled: variables in a row
  if (n_var == 1) { nc <- min(n_grp, 4L); return(list(nr = ceiling(n_grp / nc), nc = nc)) }
  list(nr = n_grp, nc = n_var)                                 # grouped x 2 vars: group rows, var cols
}

#' Render the profile grid: one panel per (group x variable), title strip + optional diel legend.
#' @keywords internal
#' @noRd
.tadRenderProfiles <- function(binned, breaks_by_var, variable, glevels, gcols, grp_of, phases, diel, grouped, theme,
                               same.scale = TRUE) {
  n_var <- length(variable); n_grp <- length(glevels); lay <- .tadPanelGrid(n_var, n_grp)
  panels <- if (n_grp == 1) lapply(variable, function(v) list(g = glevels[1], v = v))
            else if (n_var == 1) lapply(glevels, function(g) list(g = g, v = variable[1]))
            else unlist(lapply(glevels, function(g) lapply(variable, function(v) list(g = g, v = v))), recursive = FALSE)

  # Aggregate EVERY panel up front: the group facets only support comparison if they share an x scale, and
  # that maximum is not knowable until all panels are aggregated. Per variable, not globally, so depth and
  # temperature keep their own (unrelated) ranges.
  aggs <- lapply(panels, function(p) {
    ids <- names(grp_of)[if (grouped) grp_of == p$g else TRUE]; ids <- ids[!is.na(ids)]
    list(ids = ids, agg = .tadAggregate(binned[[p$v]], ids, phases, length(breaks_by_var[[p$v]]) - 1L))
  })
  xmax_by_var <- NULL
  if (isTRUE(same.scale)) {
    xmax_by_var <- stats::setNames(vapply(variable, function(v) {
      w <- which(vapply(panels, function(p) identical(p$v, v), logical(1)))
      max(vapply(w, function(i) {
        a <- aggs[[i]]$agg
        max(unlist(lapply(a, function(z) z$mean + z$se)), 0, na.rm = TRUE)
      }, numeric(1)), 0.001, na.rm = TRUE) * 1.04
    }, numeric(1)), variable)
  }

  # region 1 = a full-width title strip; regions 2.. = the panel grid (row-major, matching the loop)
  m <- matrix(1L + seq_len(lay$nr * lay$nc), lay$nr, lay$nc, byrow = TRUE)
  graphics::layout(rbind(matrix(1L, 1, lay$nc), m),
                   heights = c(graphics::lcm(1.9 * max(1, theme$cex)), rep(1, lay$nr)))
  graphics::par(mar = c(0, 0, 0, 0)); graphics::plot.new()
  banner <- .tadBannerText(variable, grouped, diel)
  graphics::text(0.012, 0.66, banner$title, adj = 0, font = 2, cex = 1.5 * theme$cex, col = theme$ink)
  graphics::text(0.012, 0.24, banner$subtitle, adj = 0, cex = 1.0 * theme$cex, col = theme$subtitle)
  if (diel) {
    # right-anchored off its own measured width: a hardcoded x clipped the key on narrow canvases. Each
    # swatch carries the border the PANEL draws it with, so the key matches the encoding it explains.
    key <- graphics::legend(0, 0, c("Night", "Day"), fill = c(theme$night, theme$day), horiz = TRUE,
                            bty = "n", cex = 1.02 * theme$cex, plot = FALSE)
    graphics::legend(max(0.5, 0.985 - key$rect$w), 0.94, c("Night", "Day"), fill = c(theme$night, theme$day),
                     border = c(.darkenColor(theme$night), theme$day.border), horiz = TRUE, bty = "n",
                     cex = 1.02 * theme$cex, text.col = theme$axis)
  }

  for (k in seq_along(panels)) {
    p <- panels[[k]]; v <- p$v; br <- breaks_by_var[[v]]; row <- ((k - 1L) %/% lay$nc) + 1L; col <- ((k - 1L) %% lay$nc) + 1L
    ids <- aggs[[k]]$ids; agg <- aggs[[k]]$agg
    y.axis <- col == 1L || !identical(p$v, panels[[k - 1L]]$v)   # draw the bin axis when the variable changes
    x.axis <- row == lay$nr || (k + lay$nc) > length(panels)     # bottom of its column
    # pooled single-variable figures would repeat the banner verbatim, so the panel title is dropped there;
    # with two variables it identifies the column and is kept (sentence case, matching the banner).
    ttl_p <- if (grouped) p$g else if (n_var == 1L) NULL else .tadCap1(.tadVarName(v))
    ttl_col <- if (grouped) gcols[[p$g]] else theme$ink
    sub_p <- if (grouped) sprintf("n = %d", length(ids)) else NULL
    # theme$night MEANS night once diel is on; a pooled bar is not night, so it takes the palette instead
    fill <- if (grouped) gcols[[p$g]] else .themePalette(theme$palette, 1L)[1]
    .tadProfilePanel(br, agg, diel, theme, fill, .tadLabel(v), .tadXlab(v), ttl_p, ttl_col, sub_p, y.axis, x.axis,
                     flip.y = .tadFlipY(v),
                     xmax = if (is.null(xmax_by_var)) NULL else xmax_by_var[[v]])
  }
}

#' Mean +/- SE per bin (length `nb`) across a set of deployments, per phase.
#' @keywords internal
#' @noRd
.tadAggregate <- function(binned_v, ids, phases, nb) {
  out <- list()
  for (ph in phases) {
    cols <- binned_v[[ph]][ids]; cols <- cols[!vapply(cols, is.null, logical(1))]
    if (!length(cols)) { out[[ph]] <- list(mean = rep(0, nb), se = rep(0, nb)); next }
    M <- do.call(cbind, cols)                                   # nb x n_deployment
    out[[ph]] <- list(mean = rowMeans(M, na.rm = TRUE),
                      se = if (ncol(M) > 1) apply(M, 1, stats::sd, na.rm = TRUE) / sqrt(ncol(M)) else rep(0, nb))
  }
  out
}

#' One profile panel: equal-height bin bands, single or diel-mirrored, mean +/- SE.
#' @keywords internal
#' @noRd
.tadProfilePanel <- function(breaks, agg, diel, theme, fill, ylab, xlab, title, title.col, subtitle, y.axis, x.axis,
                             flip.y, xmax = NULL) {
  nb <- length(breaks) - 1L; cex <- theme$cex
  if (diel) { L <- agg[["night"]]; R <- agg[["day"]] } else { L <- NULL; R <- agg[["all"]] }
  if (is.null(xmax)) xmax <- max(c(R$mean + R$se, if (!is.null(L)) L$mean + L$se), 0.001, na.rm = TRUE) * 1.04
  xlim <- if (diel) c(-xmax, xmax) else c(0, xmax)
  # mar is CONSTANT across every panel of a figure - varying it by which axes get drawn made the plot
  # regions different sizes, so the same bin was a different physical height in adjacent panels and the
  # frames did not line up. Only the drawing calls below are gated on y.axis / x.axis.
  # Margins trimmed to hand the width to the panel. The figure's own width is floored by the banner
  # strip (see .tadFigSize), so on a single-variable figure the only way to widen the data region is
  # to take it back from the chrome. The left margin still clears the widest bin label.
  graphics::par(mar = c(3.5, 4.6, 1.9, 0.9), mgp = c(3, 0.55, 0), tcl = -0.22)      # mgp[1] unused: titles are mtext-placed
  # bin 1 holds the lowest values, so a flipped axis puts it at the top (shallowest depth first) and a
  # conventional one at the bottom (coldest temperature first); the labels ride the bins either way
  ylim <- if (isTRUE(flip.y)) c(nb + 0.5, 0.5) else c(0.5, nb + 0.5)
  graphics::plot(NA, xlim = xlim, ylim = ylim, axes = FALSE, xlab = "", ylab = "", xaxs = "i", yaxs = "i")
  gx <- pretty(c(0, xmax), 4); gx <- gx[gx <= xmax]                                  # pretty() overshoots xlim
  at <- if (diel) sort(unique(c(-gx, gx))) else gx                                   # sorted, no duplicated 0
  graphics::rect(xlim[1], 0.5, xlim[2], nb + 0.5, col = theme$panel, border = NA)
  graphics::abline(v = at[at != 0], col = theme$grid, lwd = 0.6)                     # subtle grid
  # Each bar is outlined in a darker shade of its OWN fill. A contrasting border (white, grey) reads as
  # a separate element and fragments the stack; a border in the same hue reads as definition and keeps
  # the profile cohesive. `day.border` stays the documented override for the day bar.
  bar <- function(x0, x1, b, col, bord) graphics::rect(x0, b - 0.5, x1, b + 0.5, col = grDevices::adjustcolor(col, theme$bar.alpha), border = bord, lwd = 0.6)
  for (b in seq_len(nb)) {
    if (diel) { bar(-L$mean[b], 0, b, theme$night, .darkenColor(theme$night)); bar(0, R$mean[b], b, theme$day, theme$day.border) }
    else bar(0, R$mean[b], b, fill, .darkenColor(fill))
  }
  # SE whiskers, drawn as conventional T-bars (stem + a cap at each end). Bins with no spread are SKIPPED
  # rather than drawn as a bare cap on the bar's edge, which would read as a measured zero-width interval
  # when it actually means a single deployment contributed (.tadAggregate sets se = 0 when ncol == 1).
  err <- function(m, se, sgn) {
    ok <- is.finite(m) & is.finite(se) & se > 0
    if (!any(ok)) return(invisible(NULL))
    y  <- seq_len(nb)[ok]
    lo <- sgn * pmax(0, (m - se)[ok]); hi <- sgn * (m + se)[ok]
    # Lighter and thinner than the bar edges: the whiskers qualify the estimate, they are not the
    # measurement, and at full ink weight they out-shouted the bars they belong to.
    cap <- 0.14                                              # half-height of the caps, in bin units
    ec  <- grDevices::adjustcolor(theme$ink, 0.50)
    graphics::segments(lo, y, hi, y, col = ec, lwd = 0.9)
    graphics::segments(lo, y - cap, lo, y + cap, col = ec, lwd = 0.9)
    graphics::segments(hi, y - cap, hi, y + cap, col = ec, lwd = 0.9)
  }
  if (diel) { err(L$mean, L$se, -1); err(R$mean, R$se, 1) } else err(R$mean, R$se, 1)
  if (diel) graphics::abline(v = 0, col = theme$axis, lwd = 1.2)
  # axes (type sizes lifted onto the package band - these were the smallest in the plot* family)
  graphics::axis(1, at = at, labels = if (x.axis) paste0(abs(at), "%") else FALSE,
                 col = NA, col.ticks = theme$axis, col.axis = theme$axis, cex.axis = 0.85 * cex)
  if (y.axis) {
    # every bin keeps its label: the figure height now scales with nb (see .tadFigSize), so the old
    # every-other-bin thinning is unnecessary - and it used to delete the open-top ">X" label whenever
    # nb was even, leaving the deepest stratum anonymous.
    graphics::axis(2, at = seq_len(nb), labels = .binLabels(breaks), las = 1, col = NA,
                   col.ticks = theme$axis, col.axis = theme$axis, cex.axis = 0.72 * cex)
    # line 3.6, not 4.2: the left margin was trimmed to widen the panel, and a title placed at the old
    # offset is centred beyond the canvas edge and loses its first character.
    graphics::mtext(ylab, 2, line = 3.6, col = theme$ink, cex = 0.95 * cex, font = 2)
  }
  if (x.axis) graphics::mtext(xlab, 1, line = 2.4, col = theme$axis, cex = 0.95 * cex)
  if (!is.null(title))
    graphics::mtext(title, 3, line = if (is.null(subtitle)) 0.5 else 0.95, adj = 0, font = 2, col = title.col, cex = 1.0 * cex)
  if (!is.null(subtitle)) graphics::mtext(subtitle, 3, line = 0.1, adj = 0, col = theme$subtitle, cex = 0.72 * cex)
  graphics::box(col = theme$axis)                                # full border, matching the themed axes
}

################################################################################
# Rendering: heatmap (compact all-individuals option) ##########################
################################################################################

#' Render a themed deployment x bin heatmap per variable, sharing a colour bar.
#' @keywords internal
#' @noRd
.tadRenderHeatmaps <- function(binned, breaks_by_var, variable, loaded, order.by, theme, grouped) {
  pal <- grDevices::colorRampPalette(theme$sequential)(100)
  ids0 <- names(loaded)
  ord <- switch(order.by,
                median = ids0[order(vapply(loaded, function(d) {
                  z <- d$series[[variable[1]]]; z <- z[is.finite(z)]
                  if (!length(z)) NA_real_ else stats::median(z)
                }, numeric(1)), na.last = TRUE)],
                id = sort(ids0), ids0)
  # cbind DROPS deployments with no column for this variable, so the label vector has to be subset the
  # same way - otherwise column k is drawn under the k-th id of the full list and every column after a
  # gap is captioned with the wrong animal.
  ord_by_var <- lapply(variable, function(v) ord[!vapply(binned[[v]][["all"]][ord], is.null, logical(1))])
  names(ord_by_var) <- variable
  mats <- lapply(variable, function(v) do.call(cbind, binned[[v]][["all"]][ord_by_var[[v]]]))
  zmax <- max(vapply(mats, function(M) max(M, na.rm = TRUE), numeric(1)), 1)
  n_var <- length(variable)
  graphics::layout(matrix(seq_len(n_var + 1L), 1), widths = c(rep(1, n_var), graphics::lcm(3.0)))
  for (vi in seq_len(n_var)) {
    v <- variable[vi]; br <- breaks_by_var[[v]]; nb <- length(br) - 1L; M <- mats[[vi]]
    graphics::par(mar = c(6.2, 5.0, 2.6, 0.7), mgp = c(3, 0.6, 0))   # left margin on every panel: each has its own axis
    ylim <- if (.tadFlipY(v)) c(nb + 0.5, 0.5) else c(0.5, nb + 0.5)   # depth reads down, others up
    graphics::plot(NA, xlim = c(0.5, ncol(M) + 0.5), ylim = ylim, axes = FALSE, xaxs = "i", yaxs = "i", xlab = "", ylab = "")
    for (j in seq_len(ncol(M))) for (b in seq_len(nb)) {
      idx <- max(1L, min(100L, round(.rescale(M[b, j], from = c(0, zmax), to = c(1, 100)))))
      graphics::rect(j - 0.5, b - 0.5, j + 0.5, b + 0.5, col = pal[idx], border = NA)
    }
    # EVERY variable gets its own bin axis. Sharing one axis was wrong, not merely terse: the panels hold
    # different bin counts (e.g. 19 depth strata vs 12 temperature strata) drawn as equal-height rows, so a
    # temperature row was read against a depth label - a band at 22 degrees C sat against ">300 m".
    graphics::axis(2, at = seq_len(nb), labels = .binLabels(br), las = 1, col = NA, col.ticks = theme$axis,
                   col.axis = theme$axis, cex.axis = 0.68 * theme$cex)
    graphics::mtext(.tadLabel(v), 2, line = 3.9, font = 2, col = theme$ink, cex = 0.92 * theme$cex)
    graphics::text(seq_len(ncol(M)), graphics::grconvertY(0, "npc", "user"), labels = ord_by_var[[v]], srt = 45, adj = c(1, 1.3),
                   xpd = NA, cex = theme$cex * min(0.8, 22 / ncol(M)), col = theme$axis)
    graphics::mtext(.tadCap1(.tadVarName(v)), 3, line = 0.6, adj = 0, font = 2, col = theme$ink, cex = 0.95 * theme$cex)
    graphics::box(col = theme$axis)                              # frame the heatmap grid
  }
  graphics::par(mar = c(6.2, 0.6, 2.6, 3.0))
  graphics::plot(NA, xlim = c(0, 1), ylim = c(0, zmax), axes = FALSE, xaxs = "i", yaxs = "i", xlab = "", ylab = "")
  ys <- seq(0, zmax, length.out = 101); graphics::rect(0, ys[-101], 1, ys[-1], col = pal, border = NA); graphics::rect(0, 0, 1, zmax, border = theme$axis)
  graphics::axis(4, at = pretty(c(0, zmax), 5), las = 1, col = NA, col.ticks = theme$axis, col.axis = theme$axis, cex.axis = 0.7 * theme$cex)
  graphics::mtext("% time", 3, line = 0.3, cex = 0.78 * theme$cex, font = 2, col = theme$ink)
}


#######################################################################################################
#######################################################################################################
#######################################################################################################

# plotTracks helpers ----

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
                            bathy.levels = NULL, bathy.relief = FALSE, bathy.contours = TRUE,
                            coast_spec = NULL, coast_fill = TRUE,
                            tile_rast = NULL, tile_credit = NULL, show.uncertainty) {

  fixes <- payload$fixes; track <- payload$track
  deploy <- payload$deploy; popup <- payload$popup
  ink <- theme$ink; axcol <- theme$axis; cex <- theme$cex
  outline <- theme$bar.border          # the theme's "border against a fill" colour: here, marker outlines

  # extent from every drawn element (equal aspect, shared helper)
  xs <- c(fixes$lon, track$lon, deploy$lon, popup$lon)
  ys <- c(fixes$lat, track$lat, deploy$lat, popup$lat)
  ext <- .equalAspectExtent(xs, ys, f = 0.2)
  if (is.null(ext)) { graphics::plot.new(); return(invisible(NULL)) }

  # Margins and label offsets are measured in TEXT LINES, but the labels are drawn with a per-call cex
  # (which does not change the line height): at theme$cex = 1.6 the axis titles would be overprinted by
  # the tick labels. Scaling both by cex keeps cex = 1 pixel-identical and keeps the panel legible above
  # it. The right margin carries no text, so it stays put.
  # ...but mar is measured in LINES against a fixed figure region, so on a dense grid (2 x 5 panels)
  # scaling it by a large cex overruns the region entirely and base R aborts with "figure margins too
  # large". Cap the scale by what this panel can actually give up, keeping at least 65% of each
  # dimension for the map itself. At cex = 1 the cap never binds, so the default figure is unchanged.
  mar_base <- c(3.6, 4.0, 3.0, 1.2)
  fin <- graphics::par("fin"); csi <- graphics::par("csi")
  room <- function(lines, inches) if (lines <= 0 || !is.finite(inches)) cex else (0.65 * inches) / csi / lines
  sc <- max(1, min(cex, room(sum(mar_base[c(1, 3)]), fin[2]), room(sum(mar_base[c(2, 4)]), fin[1])))
  graphics::par(mar = mar_base * c(sc, sc, sc, 1), mgp = c(2.2, 0.6, 0) * sc)
  graphics::plot(NA, xlim = ext$xlim, ylim = ext$ylim, asp = ext$asp, axes = FALSE,
                 xlab = "", ylab = "", xaxs = "i", yaxs = "i")
  graphics::rect(graphics::par("usr")[1], graphics::par("usr")[3], graphics::par("usr")[2], graphics::par("usr")[4],
                 col = pal[["sea"]], border = NA)

  # canvas first (imagery OR depth relief - never both), then the contour overlay on top of it
  if (!is.null(tile_rast)) .drawTiles(tile_rast)                        # raster canvas over the sea colour
  if (isTRUE(bathy.relief) && !is.null(bathy))
    .drawBathyRelief(bathy, ext$xlim, ext$ylim, shallow = pal[["sea"]], deep = pal[["sea.deep"]])
  if (isTRUE(bathy.contours) && !is.null(bathy))
    .drawBathy(bathy, ext$xlim, ext$ylim, col = pal[["bathymetry"]], cex = cex,
               levels = if (length(bathy.levels)) bathy.levels else NULL)
  .drawCoastline(ext$xlim, ext$ylim, coast_spec, land = pal[["land"]], border = pal[["land.border"]],
                 fill = coast_fill)
  graphics::box(col = axcol)
  graphics::axis(1, at = pretty(ext$xlim, 5), labels = sprintf("%.2f", pretty(ext$xlim, 5)), col = axcol, col.axis = axcol, cex.axis = cex * 0.8)
  graphics::axis(2, at = pretty(ext$ylim, 5), labels = sprintf("%.2f", pretty(ext$ylim, 5)), las = 1, col = axcol, col.axis = axcol, cex.axis = cex * 0.8)
  graphics::title(xlab = "Longitude", line = 2.1 * sc, cex.lab = cex * 0.9, col.lab = ink)
  graphics::title(ylab = "Latitude", line = 2.6 * sc, cex.lab = cex * 0.9, col.lab = ink)
  graphics::title(main = payload$id, line = 1.5 * cex, cex.main = cex * 1.1, col.main = ink)
  graphics::title(main = .trackSubtitle(fixes, track), line = 0.5 * cex, font.main = 1, cex.main = cex * 0.8, col.main = theme$subtitle)

  # --- uncertainty corridor (translucent disks whose radius = pseudo_error), drawn UNDER the track ----
  if (show.uncertainty && !is.null(track) && "error" %in% names(track) && nrow(track) >= 1)
    .drawErrorCorridor(track$lon, track$lat, track$error, mean(ext$ylim), fill = pal[["uncertainty"]])

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
    # Start and end are told apart by their FILL - light disk versus dark disk - not by their border.
    # Routing that fill through a chrome slot collapsed the distinction: under the `classic` preset
    # bar.border is #4D4D4D and ink is #000000, so both markers became dark disks with dark rings.
    # These are map semantics, so they live in `pal` and are overridable through `colors`.
    graphics::points(track$lon[1], track$lat[1], pch = 21, bg = pal[["start"]], col = ink, lwd = 0.6, cex = cex * 1.2)
    graphics::points(track$lon[n], track$lat[n], pch = 21, bg = pal[["end"]], col = pal[["start"]], lwd = 0.6, cex = cex * 1.2)
  }

  # Event paths retain their own temporal subsets, independent of background stride thinning.
  .drawEventTracks(payload$event.paths, payload$event.palette)

  # --- genuine fixes, by type ------------------------------------------------------------------------
  .pts <- function(sel, ...) if (any(sel)) graphics::points(fixes$lon[sel], fixes$lat[sel], ...)
  .pts(fixes$type == "FastGPS", pch = 21, bg = pal[["fastgps"]], col = outline, lwd = 0.4, cex = cex)
  .pts(fixes$type == "Argos",   pch = 22, bg = pal[["argos"]],   col = outline, lwd = 0.4, cex = cex)
  .pts(fixes$type == "User",    pch = 24, bg = pal[["user"]],    col = outline, lwd = 0.4, cex = cex * 1.1)

  # --- deploy / pop-up anchors -----------------------------------------------------------------------
  if (!is.null(deploy)) graphics::points(deploy$lon, deploy$lat, pch = 23, bg = pal[["deploy"]], col = outline, lwd = 0.5, cex = cex * 1.5)
  if (!is.null(popup))  graphics::points(popup$lon,  popup$lat,  pch = 23, bg = pal[["popup"]],  col = outline, lwd = 0.5, cex = cex * 1.5)

  # --- legend + colour bar ---------------------------------------------------------------------------
  .trackLegend(fixes, track, deploy, popup, pal, theme, payload$event.paths, payload$event.palette)
  if (!is.null(color.by) && !is.null(color_range) && !is.null(track) && "value" %in% names(track))
    .trackColorbar(ramp, color_range, .defaultColorLabel(color.by), theme)

  .mapScalebar(label.cex = cex * 0.7)
  if (!is.null(tile_rast)) .drawAttribution(tile_credit, cex)           # provider credit, over the imagery
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
.drawErrorCorridor <- function(lon, lat, err, mid_lat, fill = "#6C7A89", max.disks = 60L) {
  ok <- is.finite(lon) & is.finite(lat) & is.finite(err) & err > 0
  if (!any(ok)) return(invisible(NULL))
  lon <- lon[ok]; lat <- lat[ok]; err <- err[ok]
  n <- length(lon)
  idx <- if (n > max.disks) unique(round(seq(1L, n, length.out = max.disks))) else seq_len(n)
  ang <- seq(0, 2 * pi, length.out = 24)
  coslat <- cos(mid_lat * pi / 180); if (!is.finite(coslat) || coslat <= 0) coslat <- 1
  fill <- grDevices::adjustcolor(fill, alpha.f = 0.14)
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
.trackLegend <- function(fixes, track, deploy, popup, pal, theme, event.paths = NULL, event.palette = NULL) {
  ink <- theme$ink; cex <- theme$cex; outline <- theme$bar.border
  lab <- character(0); pch <- integer(0); pcol <- character(0); pbg <- character(0); lty <- integer(0); lwd <- numeric(0)
  add <- function(l, pc, co, bg = NA, lt = NA, lw = NA) {
    lab[[length(lab) + 1L]] <<- l; pch[[length(pch) + 1L]] <<- pc; pcol[[length(pcol) + 1L]] <<- co
    pbg[[length(pbg) + 1L]] <<- bg; lty[[length(lty) + 1L]] <<- lt; lwd[[length(lwd) + 1L]] <<- lw }
  if (any(fixes$type == "FastGPS")) add(sprintf("FastGPS (%d)", sum(fixes$type == "FastGPS")), 21, outline, pal[["fastgps"]])
  if (any(fixes$type == "Argos"))   add(sprintf("Argos (%d)",   sum(fixes$type == "Argos")),   22, outline, pal[["argos"]])
  if (any(fixes$type == "User"))    add(sprintf("User (%d)",    sum(fixes$type == "User")),    24, outline, pal[["user"]])
  if (!is.null(track) && nrow(track) >= 2) {
    add("track", NA_integer_, pal[["track"]], NA, 1L, 1.6)
    add("start", 21, ink, pal[["start"]]); add("end", 21, pal[["start"]], pal[["end"]])
  }
  if (!is.null(deploy)) add("deployment", 23, outline, pal[["deploy"]])
  if (!is.null(popup))  add("pop-up", 23, outline, pal[["popup"]])
  types <- unique(vapply(event.paths, function(p) p$event, character(1)))
  for (type in types) add(type, NA_integer_, unname(event.palette[type]), NA, 1L, 3)
  if (!length(lab)) return(invisible(NULL))
  graphics::legend("topleft", legend = lab, pch = pch, col = pcol, pt.bg = pbg, lty = lty, lwd = lwd,
                   bty = "o", bg = grDevices::adjustcolor(theme$panel, alpha.f = 0.8), box.col = theme$grid,
                   text.col = ink, pt.lwd = 0.5, pt.cex = cex * 1.1,
                   cex = cex * 0.62, y.intersp = 1.1, inset = 0.015, seg.len = 1.4)
  invisible(NULL)
}

# Compact horizontal colour bar (bottom-right corner) for the color.by scale.
#' @keywords internal
#' @noRd
.trackColorbar <- function(ramp, color_range, label, theme) {
  if (!all(is.finite(color_range)) || diff(color_range) <= 0) return(invisible(NULL))
  ink <- theme$ink; axcol <- theme$axis; cex <- theme$cex
  usr <- graphics::par("usr")
  w <- 0.30 * (usr[2] - usr[1]); h <- 0.022 * (usr[4] - usr[3])
  x0 <- usr[2] - w - 0.04 * (usr[2] - usr[1]); y0 <- usr[3] + 0.06 * (usr[4] - usr[3])
  xs <- seq(x0, x0 + w, length.out = length(ramp) + 1L)
  graphics::rect(xs[-length(xs)], y0, xs[-1], y0 + h, col = ramp, border = NA)
  graphics::rect(x0, y0, x0 + w, y0 + h, border = axcol, lwd = 0.6)
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
.fetchBathy <- function(xlim, ylim, lvl) {
  # Grid resolution (arc-minutes) is derived from the extent, not a user knob: aim for ~300 grid points
  # across the wider span (fine for an island window, coarse for an ocean basin), floored at 1' (ETOPO1).
  span <- max(diff(range(xlim)), diff(range(ylim)))
  resolution <- max(1, min(60, round(span * 60 / 300)))
  tryCatch({
    bathy <- NULL
    # capture.output() (not sink()) mutes getNOAA.bathy's progress prints exception-safely
    utils::capture.output(suppressMessages(
      bathy <- marmap::getNOAA.bathy(lon1 = xlim[1], lon2 = xlim[2], lat1 = ylim[1], lat2 = ylim[2],
                                     resolution = resolution, keep = FALSE)))
    bathy
  }, error = function(e) { .log_skip(lvl, "bathymetry unavailable: ", conditionMessage(e)); NULL })
}

#' Draw a shaded/coloured bathymetric relief as the panel canvas, from a marmap grid.
#'
#' The depth CANVAS, as opposed to `.drawBathy()`'s contour OVERLAY - the same grid serves both, so a
#' single fetch covers `basemap = "bathymetry"` and `bathy.contours` together. Land (z >= 0) is masked so
#' only the sea is painted and the coastline draws filled land on top. `graphics::image()` takes lon/lat
#' vectors with z as a `[lon, lat]` matrix - exactly marmap's own layout (and `.drawBathy`'s) - so the
#' relief co-registers with the data without any transposition or flip.
#' @param bathy A marmap `bathy` object.
#' @param xlim,ylim Panel extent (lon/lat).
#' @param shallow,deep The two ends of the depth ramp (shallow water -> deep water).
#' @keywords internal
#' @noRd
.drawBathyRelief <- function(bathy, xlim, ylim, shallow = "#EAF1F6", deep = "#2C4A63") {
  lon <- as.numeric(rownames(bathy)); lat <- as.numeric(colnames(bathy))
  z <- unclass(bathy); z[z >= 0] <- NA                                   # sea only; land is drawn over it
  if (!any(is.finite(z))) return(invisible(NULL))                        # an all-land window: nothing to paint
  ramp <- grDevices::colorRampPalette(c(deep, shallow))(120)             # deep -> shallow (image() maps low->high)
  tryCatch(
    graphics::image(lon, lat, z, col = ramp, add = TRUE, useRaster = FALSE),
    error = function(e) invisible(NULL))
  invisible(NULL)
}

.drawBathy <- function(bathy, xlim, ylim, col = "#9DB4C0", cex = 1, levels = NULL) {
  lon <- as.numeric(rownames(bathy)); lat <- as.numeric(colnames(bathy))
  z <- unclass(bathy); z[z > 0] <- NA                                    # sea only
  # explicit isobaths when supplied (kept only where the range actually has depth), else pretty auto levels
  lv <- if (!is.null(levels)) levels[levels < 0] else { l <- pretty(range(z, na.rm = TRUE), 6); l[l < 0] }
  lv <- lv[is.finite(lv)]
  if (length(lv))
    graphics::contour(lon, lat, z, levels = lv, add = TRUE, drawlabels = TRUE,
                      col = col, lwd = 0.4, labcex = cex * 0.5, method = "edge")
  invisible(NULL)
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
