#######################################################################################################
# Plot per-deployment depth profiles ##################################################################
#######################################################################################################

#' Plot depth profiles, coloured by an environmental variable
#'
#' @description
#' Draws one depth-vs-time profile per deployment, with points coloured by a chosen variable
#' (temperature by default) and a diel (day / twilight / night) background shading. Deployments are
#' laid out in a grid, paginated automatically when they do not fit on one page.
#'
#' Like the other workflow functions, `data` accepts the package's canonical input forms - a character
#' vector of `.rds` file paths, a single `nautilus_tag` / data.frame, or a list of them - and the
#' deployment coordinates used for the diel shading are read from each object's metadata, so no
#' separate metadata table is required.
#'
#' @param data Depth data: a character vector of `.rds` file paths, a single `nautilus_tag` /
#'   data.frame, or a list of them (see \code{\link{processTagData}}).
#' @param color.by Character. Name of the column mapped to colour. Default `"temp"`.
#' @param color.label Character. Legend title for `color.by`. `NULL` (default) derives a sensible label
#'   (e.g. a degree-Celsius temperature label for `"temp"`).
#' @param color.pal Character vector of colours for the mapped variable, overriding the theme. `NULL`
#'   (default) builds the ramp from the theme's `sequential` colours, weighted to give extra resolution
#'   to the upper range. This is a DATA mapping rather than chrome, so it stays user-settable
#'   independently of `theme`: a figure may well need a domain-specific temperature scale while keeping
#'   the rest of the family's look.
#' @param same.color.scale Logical. If `TRUE` (default), all panels share one colour scale and a single
#'   legend; if `FALSE`, each panel is scaled and labelled independently.
#' @param same.depth.scale Logical. If `TRUE`, all panels share one depth axis; if `FALSE` (default),
#'   each panel is scaled to its own maximum depth.
#' @param shade.diel Logical. If `TRUE` (default), shade the background by diel phase (day / twilight /
#'   night). Requires deployment coordinates in the metadata; panels without them are left unshaded. The
#'   shading greys are fixed rather than taken from the theme's `day`/`night` colours: this panel carries
#'   a colour-mapped trace across its whole area, so the background must stay neutral and pale or it
#'   competes with (and, at night, swallows) the data. A Day / Twilight / Night key is drawn beside the
#'   colour bar when a shared legend is shown.
#' @param geom Character. How each dive trace is drawn: `"line"` (default) maps the colour onto a continuous
#'   line that traces the dive shape (broken across recording gaps); `"points"` draws coloured samples only;
#'   `"both"` overlays points on the line. A line reads most clearly for a smoothly-varying colour variable
#'   (e.g. temperature); `"points"` can suit a noisy or sparse one.
#' @param downsample Numeric. Bin width (seconds) for averaging before plotting, which keeps
#'   high-resolution records legible and fast to draw. Default 5; set `NULL` to plot every sample.
#' @param plot Logical. If `TRUE` (default), draw to the active graphics device.
#' @param plot.file Character. Path to a multi-page PDF to draw into (one page per grid of deployments).
#'   Independent of `plot`: set either or both. The function manages the device itself. Default `NULL`.
#' @param ncols,nrows Integer. Grid columns / rows per page. If `NULL` (default) both are chosen
#'   automatically (up to 2 columns and 5 rows per page).
#' @param theme A \link{plotTheme} object, or a list of overrides, controlling the visual style: text
#'   scaling (`cex`), font family, the ink / axis colours of the panel chrome, and the `sequential` ramp
#'   used for `color.by` when `color.pal` is `NULL`.
#' @param point.size Numeric. Plotting-character size (`cex`) for the depth points, used when `geom` draws
#'   points (`"points"` or `"both"`). Default 0.4.
#' @param lwd Numeric. Line width for the dive trace, used when `geom` draws a line (`"line"` or `"both"`).
#'   Default 1.6.
#' @param id.col,datetime.col,depth.col Character. Column names for the ID, datetime and depth.
#'   Defaults `"ID"`, `"datetime"`, `"depth"`.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet" (silent), `TRUE`/`1`/"normal" (header + summary), or
#'   `2`/"detailed" (default): additionally reports per-deployment data-quality notes (deployments missing
#'   the colour variable or coordinates) and shows a live progress bar while the tags are read.
#'
#' @return Invisibly `NULL`; called for its side effect (the plot).
#' @seealso \code{\link{processTagData}}, \code{\link{summarizeTagData}}
#' @examples
#' \dontrun{
#' # Draw to a multi-page PDF, coloured by temperature
#' plotDepthProfiles(list.files("./processed", full.names = TRUE),
#'                   plot = FALSE, plot.file = "./plots/depth-profiles.pdf")
#' }
#' @export

plotDepthProfiles <- function(data,
                              color.by         = "temp",
                              color.label      = NULL,
                              color.pal        = NULL,
                              same.color.scale = TRUE,
                              same.depth.scale = FALSE,
                              shade.diel       = TRUE,
                              geom             = c("line", "points", "both"),
                              downsample       = 5,
                              plot             = TRUE,
                              plot.file        = NULL,
                              ncols            = NULL,
                              nrows            = NULL,
                              theme            = plotTheme(),
                              point.size       = 0.4,
                              lwd              = 1.6,
                              id.col           = "ID",
                              datetime.col     = "datetime",
                              depth.col        = "depth",
                              verbose          = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)

  ##############################################################################
  # Validate arguments #########################################################
  ##############################################################################

  .assert_string(color.by, "color.by")
  .assert_string(color.label, "color.label", null_ok = TRUE)
  .assert_flag(same.color.scale, "same.color.scale")
  .assert_flag(same.depth.scale, "same.depth.scale")
  .assert_flag(shade.diel, "shade.diel")
  geom <- match.arg(geom)
  .assert_flag(plot, "plot")
  if (!is.null(downsample)) .assert_number(downsample, "downsample", min = 0)
  .assert_writable_file(plot.file, "plot.file", ext = "pdf")     # fail-fast: parent dir must exist
  if (!is.null(ncols)) .assert_count(ncols, "ncols", min = 1)
  if (!is.null(nrows)) .assert_count(nrows, "nrows", min = 1)
  theme <- .as_control(theme, plotTheme, "nautilus_theme", "theme")
  .assert_number(point.size, "point.size", min = 0)
  .assert_number(lwd, "lwd", min = 0)
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col"); .assert_string(depth.col, "depth.col")
  if (!is.null(color.pal) && (!is.character(color.pal) || !length(color.pal)))
    .abort("{.arg color.pal} must be a non-empty character vector of colours.")

  if (!plot && is.null(plot.file))
    .abort(c("Nothing to plot.", "i" = "Set {.arg plot = TRUE} or provide a {.arg plot.file}."))

  if (is.null(color.label)) color.label <- .defaultColorLabel(color.by)

  # This figure was tuned at cex = 1.15 before the theme existed. Folding that base in here means
  # theme$cex = 1 reproduces the tuned layout exactly and scales from there, rather than silently
  # shrinking every label by 13% the moment the function joined the family.
  cex <- theme$cex * 1.15


  ##############################################################################
  # Load, validate and downsample every deployment #############################
  ##############################################################################

  src <- .resolveInput(data, id.col)

  .log_header(lvl, "plotDepthProfiles", "Plotting depth profiles",
              bullets = sprintf("Input: %d deployment%s \u00b7 coloured by %s",
                                src$n, if (src$n != 1) "s" else "", color.by))

  deployments <- list()
  n_no_color <- 0L; n_no_coord <- 0L
  pb <- .log_progress_start(lvl, src$n, "Loading")                  # live bar at detailed verbosity (lvl >= 2)
  for (i in seq_len(src$n)) {
    .log_progress_step(pb)
    tag <- src$get(i)
    .validateColumns(tag, c(datetime.col, depth.col), where = if (src$is_filepaths) basename(src$paths[i]) else NULL)
    meta <- .getMeta(tag)
    d <- .downsampleForPlot(tag, id.col, datetime.col, downsample)
    if (nrow(d) == 0) next
    lon <- meta$deployment$lon %||% NA_real_
    lat <- meta$deployment$lat %||% NA_real_
    has_color <- color.by %in% names(d) && any(!is.na(d[[color.by]]))
    if (!has_color) n_no_color <- n_no_color + 1L
    if (!all(is.finite(c(lon, lat)))) n_no_coord <- n_no_coord + 1L
    deployments[[length(deployments) + 1L]] <-
      list(id = meta$id %||% src$ids[i], coords = c(lon = lon, lat = lat), data = d, has_color = has_color)
  }
  .log_progress_done(pb)
  if (!length(deployments)) .abort("No non-empty datasets to plot.")


  ##############################################################################
  # Global scales, palette and layout ##########################################
  ##############################################################################

  # shared colour range across all panels (when requested and any panel has colour data)
  color_range <- NULL
  if (same.color.scale) {
    rng <- unlist(lapply(deployments, function(d) if (d$has_color) range(d$data[[color.by]], na.rm = TRUE) else NULL))
    if (length(rng)) color_range <- range(rng, na.rm = TRUE)
  }
  use_shared_legend <- same.color.scale && !is.null(color_range)

  # shared depth axis (deepest deployment), when requested
  depth_ylim_global <- NULL
  if (same.depth.scale)
    depth_ylim_global <- .depthYlim(max(vapply(deployments, function(d) max(d$data[[depth.col]], na.rm = TRUE), numeric(1)), na.rm = TRUE))

  # Default palette: the theme's sequential ramp, re-weighted so that more colour resolution is devoted
  # to the upper range (the last 30% of the ramp is stretched 3:2 against the rest). That weighting is
  # what keeps the warm/high end of a temperature scale readable instead of saturating into one tone,
  # so it is preserved verbatim from the pre-theme jet default - only the BASE colours now come from
  # the theme.
  if (is.null(color.pal)) {
    base_pal  <- grDevices::colorRampPalette(theme$sequential)(100)
    color.pal <- grDevices::colorRampPalette(c(rep(base_pal[1:70], each = 2), rep(base_pal[71:100], each = 3)))(100)
  }

  lay <- .depthProfileLayout(length(deployments), ncols, nrows, legend = use_shared_legend)

  if (lvl >= 1L)
    .log_arrow(lvl, sprintf("layout: %d x %d per page%s", lay$nrows, lay$ncols,
                            if (length(lay$pages) > 1) sprintf(" \u00b7 %d pages", length(lay$pages)) else ""))


  ##############################################################################
  # Draw (to the caller's device and/or a multi-page PDF, via the shared helper)#
  ##############################################################################

  shared_slot <- use_shared_legend && !is.na(lay$legend_cell)             # one horizontal legend in the last cell
  # the diel key rides in the shared legend cell, but only when shading was actually drawn somewhere
  any_shaded  <- shade.diel && any(vapply(deployments, function(d) all(is.finite(d$coords)), logical(1)))
  draw <- function(to.file = FALSE, unicode = TRUE) {
    graphics::par(oma = c(2.5, 0, 1.2, 0), mgp = c(3, 0.8, 0), family = theme$font.family)
    for (pg in lay$pages) {
      graphics::layout(lay$matrix, widths = lay$widths, heights = lay$heights)
      for (k in pg) {
        dep <- deployments[[k]]
        depth_ylim <- if (same.depth.scale) depth_ylim_global else .depthYlim(max(dep$data[[depth.col]], na.rm = TRUE))
        panel_range <- if (same.color.scale) color_range else if (dep$has_color) range(dep$data[[color.by]], na.rm = TRUE) else NULL
        .drawDepthPanel(dep, color.by, depth.col, datetime.col, color.pal, panel_range, depth_ylim,
                        shade.diel = shade.diel, geom = geom, point.size = point.size, lwd = lwd, cex = cex,
                        panel.legend = !shared_slot, color.label = color.label, theme = theme)
      }
      # fill empty PANEL cells; the reserved last cell then receives the shared colour legend + diel key
      for (b in seq_len(lay$capacity - length(pg))) graphics::plot.new()
      if (shared_slot) .drawLegendCell(color.pal, color_range, color.label, cex, show.diel = any_shaded, theme = theme)
      else for (b in seq_len(lay$per_page - lay$capacity)) graphics::plot.new()   # (0 when no cell was reserved)
    }
  }

  .renderToDevices(draw, plot = plot, plot.file = plot.file, width = lay$width, height = lay$height)


  ##############################################################################
  # Summary ####################################################################
  ##############################################################################

  if (lvl >= 1L) {
    .log_summary(lvl)
    if (n_no_color > 0) .log_detail(lvl, sprintf("no '%s' data (plotted unmapped): %d/%d", color.by, n_no_color, length(deployments)))
    if (shade.diel && n_no_coord > 0) .log_detail(lvl, sprintf("no coordinates (diel shading skipped): %d/%d", n_no_coord, length(deployments)))
    .log_done(lvl, length(deployments), " depth profile", if (length(deployments) != 1) "s", " plotted")
    if (!is.null(plot.file)) .log_arrow(lvl, "plots: ", plot.file)
    .log_runtime(lvl, start.time)
  }

  invisible(NULL)
}


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
  has_color <- !is.null(color_range) && color.by %in% names(d) && any(!is.na(d[[color.by]]))
  point_col <- rep(theme$ink, length(depth))                        # no colour variable -> plain ink trace
  if (has_color) {
    idx <- round(.rescale(d[[color.by]], from = color_range, to = c(1, length(color.pal))))
    point_col <- color.pal[idx]
  }

  graphics::par(mar = c(2.2, 5, 2.2, if (panel.legend) 5 else 1.2))
  plot(x = time, y = depth, type = "n", axes = FALSE, xaxs = "i", xlab = "", ylab = "Depth (m)",
       ylim = depth_ylim, cex.lab = cex, col.lab = theme$ink)

  # Panel fill from the theme, but only when diel shading is switched off for the whole figure: with
  # shading on the background greys MEAN a phase, so a themed fill on a panel that merely lacks
  # coordinates would read as one of them.
  if (!shade.diel) {
    usr <- graphics::par("usr")
    graphics::rect(usr[1], usr[3], usr[2], usr[4], col = theme$panel, border = NA)
  }
  if (shade.diel && all(is.finite(dep$coords))) .shadeDiel(dep$coords)

  # geometry: a coloured LINE traces the dive (default), coloured POINTS show the raw samples, or BOTH.
  # The line is drawn as gap-broken segments so it never connects across a recording gap (see .drawColorLine).
  if (geom %in% c("line", "both")) .drawColorLine(time, depth, point_col, lwd = lwd)
  if (geom %in% c("points", "both")) graphics::points(x = time, y = depth, pch = 16, col = point_col, cex = point.size)

  # axes: date or clock time depending on the record length
  tr   <- range(time, na.rm = TRUE)
  span <- as.numeric(difftime(tr[2], tr[1], units = "secs"))
  graphics::axis.POSIXct(1, at = pretty(tr, n = 5), format = if (span > 86400) "%d/%b" else "%H:%M",
                         cex.axis = cex * 0.95, col = theme$axis, col.axis = theme$axis)
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
#' meaningless without it. The swatch fills come from \code{.dielFills()} (fixed, see there); only their
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
