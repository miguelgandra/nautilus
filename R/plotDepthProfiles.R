#######################################################################################################
# Plot per-deployment depth profiles ##################################################################
#######################################################################################################

#' Plot depth profiles, coloured by an environmental variable
#'
#' @description
#' The depth record is where a deployment's behaviour is most legible: dive shape, diel structure and
#' the transition between them are all visible at a glance, and all but invisible in a table of
#' summary statistics.
#'
#' This function draws one depth-against-time profile per deployment, coloured by a variable of your
#' choosing - temperature by default, so the animal's position in the water column can be read against
#' the thermal structure it was moving through - over a day, twilight and night background. Deployments
#' are laid out in a grid and paginated automatically.
#'
#' Like the other workflow functions, `data` accepts the package's canonical input forms - a character
#' vector of `.rds` file paths, a single `nautilus_tag` / data.frame, or a list of them - and the
#' deployment coordinates used for the diel shading are read from each object's metadata, so no
#' separate metadata table is required.
#'
#' @param data Depth data: a character vector of `.rds` file paths, a single `nautilus_tag` /
#'   data.frame, or a list of them (see [processTagData()]).
#' @param color.by Character. Name of the column mapped to colour. Default `"temp"`.
#'   A constant channel uses a uniform trace without a colour bar.
#' @param color.label Character. Legend title for `color.by`. `NULL` (default) derives a sensible label
#'   (e.g. a degree-Celsius temperature label for `"temp"`).
#' @param color.pal Character vector of colours for the mapped variable, overriding the theme. `NULL`
#'   (default) builds the ramp from the theme's `sequential` colours, weighted to give extra resolution
#'   to the upper range. This maps data rather than styling the panel, so it stays settable
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
#'   competes with (and, at night, swallows) the data. A Day / Twilight / Night key is drawn beneath
#'   the colour bar when a shared legend is shown.
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
#' @param theme A [plotTheme()] object, or a list of overrides, controlling the visual style: text
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
#' @param events Optional data frame of event windows with `ID`, `event`, `start` and `end` (`POSIXct`)
#'   columns, for example from [detectCircling()]. Default `NULL` draws no event layer. Matching is by
#'   deployment and time; windows are clipped to the visible profile and drawn as translucent bands
#'   using `theme$palette`. Their endpoints are independent of trace downsampling. For a specific dive,
#'   subset `data` to that dive before plotting. An absent band does not establish behavioural absence
#'   where the detector was unassessable. Extra deployment IDs are ignored for subset plots; a table
#'   whose IDs and detector roster do not match any plotted deployment raises a warning.
#'   No sample columns or input metadata are changed.
#'
#' @return Invisibly `NULL`; called for its side effect (the plot).
#' @seealso [processTagData()], [summarizeTagData()], [detectCircling()], [annotateData()]
#' @examples
#' \dontrun{
#' # Draw to a multi-page PDF, coloured by temperature
#' plotDepthProfiles(list.files("./processed", full.names = TRUE),
#'                   plot = FALSE, plot.file = "./plots/depth-profiles.pdf")
#'
#' # Highlight candidate events; subset a deployment first to inspect a specific dive
#' circles <- detectCircling(processed)
#' plotDepthProfiles(processed, events = circles)
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
                              verbose          = "detailed",
                              events           = NULL) {

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
  events <- .eventIntervals(events)
  event.palette <- if (!is.null(events)) .eventPalette(events, theme) else NULL
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
  n_no_color <- 0L; n_no_coord <- 0L; n_no_depth <- 0L
  pb <- .log_progress_start(lvl, src$n, "Loading")                  # live bar at detailed verbosity (lvl >= 2)
  for (i in seq_len(src$n)) {
    .log_progress_step(pb)
    tag <- src$get(i)
    # a deployment with no depth channel simply gets no panel: one unplottable tag must not cost the
    # whole figure (the omission is counted and reported below)
    if (length(setdiff(c(datetime.col, depth.col), names(tag)))) { n_no_depth <- n_no_depth + 1L; next }
    meta <- .getMeta(tag)
    d <- .downsampleForPlot(tag, id.col, datetime.col, downsample)
    if (nrow(d) == 0) next
    lon <- meta$deployment$lon %||% NA_real_
    lat <- meta$deployment$lat %||% NA_real_
    has_color <- color.by %in% names(d) && any(!is.na(d[[color.by]]))
    if (!has_color) n_no_color <- n_no_color + 1L
    if (!all(is.finite(c(lon, lat)))) n_no_coord <- n_no_coord + 1L
    deployments[[length(deployments) + 1L]] <-
      list(id = meta$id %||% src$ids[i], coords = c(lon = lon, lat = lat), data = d, has_color = has_color,
           events = .eventsForDeployment(events, meta$id %||% src$ids[i]), event.palette = event.palette,
           time.range = if (!is.null(events)) range(tag[[datetime.col]], na.rm = TRUE) else NULL)
  }
  .log_progress_done(pb)
  if (!length(deployments)) .abort("No non-empty datasets to plot.")
  .warnUnmatchedEvents(events, vapply(deployments, function(d) as.character(d$id), character(1)))


  ##############################################################################
  # Global scales, palette and layout ##########################################
  ##############################################################################

  # shared colour range across all panels (when requested and any panel has colour data)
  color_range <- NULL
  if (same.color.scale) {
    rng <- unlist(lapply(deployments, function(d) if (d$has_color) range(d$data[[color.by]], na.rm = TRUE) else NULL))
    if (length(rng)) color_range <- range(rng, na.rm = TRUE)
  }
  use_shared_legend <- same.color.scale && !is.null(color_range) && all(is.finite(color_range)) && diff(color_range) > 0

  # shared depth axis (deepest deployment), when requested
  depth_ylim_global <- NULL
  if (same.depth.scale)
    depth_ylim_global <- .depthYlim(max(vapply(deployments, function(d) max(d$data[[depth.col]], na.rm = TRUE), numeric(1)), na.rm = TRUE))

  # Default palette: the theme's continuous ramp, re-weighted so that more colour resolution is devoted
  # to the upper range (the last 30% of the ramp is stretched 3:2 against the rest). That weighting is
  # what keeps the warm/high end of a temperature scale readable instead of saturating into one tone.
  # Both the weighting and the ramp itself come from the pre-theme default; override either with
  # `color.pal` here, or `sequential` in plotTheme() to restyle every mapped trace at once.
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
    if (n_no_depth > 0) .log_skip(lvl, sprintf("no depth channel (no panel drawn): %d deployment%s",
                                                n_no_depth, if (n_no_depth != 1) "s" else ""))
    if (n_no_color > 0) .log_detail(lvl, sprintf("no '%s' data (plotted unmapped): %d/%d", color.by, n_no_color, length(deployments)))
    if (shade.diel && n_no_coord > 0) .log_detail(lvl, sprintf("no coordinates (diel shading skipped): %d/%d", n_no_coord, length(deployments)))
    .log_done(lvl, length(deployments), " depth profile", if (length(deployments) != 1) "s", " plotted")
    if (!is.null(plot.file)) .log_arrow(lvl, "plots: ", plot.file)
    .log_runtime(lvl, start.time)
  }

  invisible(NULL)
}
