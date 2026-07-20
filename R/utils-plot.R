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


#' Draw a light land/coastline over the current lon/lat panel from `maps`/`mapdata`, if installed.
#'
#' A graceful, silent no-op when `maps` is absent (so the geographic plotters keep working without the
#' Suggests). Everything is drawn directly in WGS84 lon/lat - no reprojection - so it co-registers with
#' the data primitives. Uses the high-resolution `worldHires` database when `mapdata` is available,
#' otherwise the coarser `world`.
#' @keywords internal
#' @noRd
.drawCoastline <- function(lon_range, lat_range, land = "#D9D2C5", border = "#B8AE9C") {
  if (!requireNamespace("maps", quietly = TRUE)) return(invisible(NULL))
  db <- if (requireNamespace("mapdata", quietly = TRUE)) "worldHires" else "world"
  tryCatch(
    suppressWarnings(maps::map(db, add = TRUE, fill = TRUE, col = land, border = border,
                               lwd = 0.4, xlim = lon_range, ylim = lat_range)),
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



#######################################################################################################
# Shared coercion contract for user-supplied columns ##################################################
#######################################################################################################

#' Numeric coercion for a user-named data column.
#'
#' `as.numeric()` on a FACTOR returns its integer LEVEL CODES, not its values. Applied blind to a column
#' the user named, that silently substitutes 1..nlevels for the real measurements: a factor depth column
#' of 100/200/300 m becomes 1/2/3, and every figure and summary built on it is wrong while looking
#' entirely plausible. Factors therefore go via their labels; everything else is coerced directly.
#'
#' Text that does not parse becomes `NA` rather than an error, so callers can decide whether an unusable
#' column is fatal or merely skipped - but they must decide, rather than plotting the NAs.
#' @keywords internal
#' @noRd
.asPlotNumeric <- function(z) {
  if (is.factor(z)) z <- as.character(z)
  suppressWarnings(as.numeric(z))
}

#' Seconds-since-epoch for a user-named time column, or `NULL` if it is not a time at all.
#'
#' Elapsed-time arithmetic across the package assumes SECONDS. `as.numeric()` was applied blind, so a
#' `Date` column - which counts DAYS - came out 86400x too small (a 300-day record drawn on a five-minute
#' axis), and a character column came out all-`NA`, producing a complete but empty figure. Returning
#' `NULL` for a non-time column lets each caller raise an error that names its own argument.
#' @keywords internal
#' @noRd
.asPlotTime <- function(z) {
  if (inherits(z, "POSIXct")) return(as.numeric(z))
  if (inherits(z, "POSIXlt")) return(as.numeric(as.POSIXct(z)))
  if (inherits(z, "Date")) return(as.numeric(z) * 86400)          # Date counts DAYS, not seconds
  if (is.numeric(z)) return(as.numeric(z))                        # already epoch seconds
  NULL
}

#' Is `z` usable as a time column at all? (cheap guard for callers that only need to reject.)
#' @keywords internal
#' @noRd
.isPlotTime <- function(z) !is.null(.asPlotTime(z))
