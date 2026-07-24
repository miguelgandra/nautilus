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
#' Phase 1 implements the vector canvases only: `"land"` (filled coastline + sea) and `"none"` (blank
#' sea). The raster canvases (`"bathymetry"`, `"satellite"`) and a user-supplied raster are reserved and
#' error cleanly, pointing at the vector features that ARE available. `choices` differs by function
#' (`filterLocations` does not offer a bathymetry canvas).
#' @keywords internal
#' @noRd
.resolveBasemap <- function(basemap, choices) {
  # a non-character basemap is a user-supplied raster canvas (a terra SpatRaster, e.g. from getBasemap())
  if (!is.character(basemap)) {
    if (!requireNamespace("terra", quietly = TRUE) || !inherits(basemap, "SpatRaster"))
      .abort(c("A user-supplied {.arg basemap} must be a {.pkg terra} {.cls SpatRaster}.",
               "i" = "Fetch one with {.fn getBasemap}, or pass a keyword ({.val land}, {.val satellite}, {.val none})."))
    return(list(kind = "raster", raster = basemap, credit = attr(basemap, "nautilus.credit", exact = TRUE)))
  }
  bm <- match.arg(basemap, choices)
  if (identical(bm, "bathymetry"))
    .abort(c("{.arg basemap = \"bathymetry\"} (shaded relief) is not available yet (reserved for a later release).",
             "i" = "For depth contours on the current map, set {.arg bathy.contours = TRUE}.",
             "i" = "Or use {.val land} (default), {.val satellite}, or {.val none}."))
  if (identical(bm, "satellite")) {
    if (!requireNamespace("maptiles", quietly = TRUE))
      .abort(c("{.arg basemap = \"satellite\"} needs the {.pkg maptiles} package.",
               "i" = "Install it with {.code install.packages(\"maptiles\")}, pass a pre-fetched raster from {.fn getBasemap}, or use {.val land}."))
    return(list(kind = "satellite", raster = NULL, credit = NULL))
  }
  list(kind = bm, raster = NULL, credit = NULL)                 # "land" or "none"
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
.fetchTiles <- function(xlim, ylim, control = basemapControl(), lvl = 0L) {
  for (pkg in c("maptiles", "terra", "sf"))
    if (!requireNamespace(pkg, quietly = TRUE))
      .abort(c("A satellite basemap needs the {.pkg {pkg}} package.",
               "i" = "Install {.pkg maptiles}, {.pkg terra} and {.pkg sf}, or use {.arg basemap = \"land\"}."))
  # get_tiles() wants an sf/sfc with a CRS, not a bare bbox: give it an sfc polygon of the extent
  aoi <- sf::st_as_sfc(sf::st_bbox(c(xmin = xlim[1], ymin = ylim[1], xmax = xlim[2], ymax = ylim[2]),
                                   crs = 4326))
  rast <- tryCatch(
    suppressWarnings(maptiles::get_tiles(aoi, provider = control$provider, crop = TRUE,
                                         cachedir = .tileCacheDir(control$cache))),
    error = function(e) { .log_skip(lvl, "satellite basemap unavailable: ", conditionMessage(e)); NULL })
  if (is.null(rast)) return(NULL)
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
    return(list(kind = "maps", db = "worldHires"))
  }
  if (identical(coastline, "low")) {
    if (!has_maps) return(list(kind = "none"))
    return(list(kind = "maps", db = "world"))
  }
  # "auto": prefer worldHires, fall back to the coarse world with a one-time hint, else nothing
  if (has_mapdata) return(list(kind = "maps", db = "worldHires"))
  if (has_maps) {
    # plain text: .log_info wraps its message in a glue span, so cli class markup would print literally
    .log_info(lvl, "coastline is low-resolution (maps::world); install 'mapdata' for ",
              "high-resolution coastlines, or pass a custom coastline via `coastline=`.")
    return(list(kind = "maps", db = "world"))
  }
  list(kind = "none")
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
    tryCatch(
      suppressWarnings(maps::map(spec$db, add = TRUE, fill = fill, col = if (fill) land else border,
                                 border = border, lwd = 0.4, xlim = lon_range, ylim = lat_range)),
      error = function(e) invisible(NULL))
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
