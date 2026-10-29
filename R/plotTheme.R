#' Visual theme for nautilus plots
#'
#' @description
#' A small style object controlling the shared look of the nautilus plotting functions
#' ([plotTimeAtDepth()], [plotDistributions()], ...). Pass it as their `theme` argument. Start
#' from a `preset` and override individual fields as needed - so figures are publication-ready by default
#' but trivially adapted to your own palette, transparency, or typography without touching plotting code.
#'
#' \preformatted{
#' plotTimeAtDepth(data, theme = plotTheme())                       # polished default
#' plotTimeAtDepth(data, theme = plotTheme("minimal", cex = 1.2))   # a preset, larger text
#' plotTimeAtDepth(data, theme = plotTheme(day = "#EAF3FB", palette = "viridis"))
#' }
#'
#' @param preset Base style: `"light"` (default; soft panel, subtle gridlines), `"minimal"` (white
#'   panel), or `"classic"` (greyscale, print-journal look).
#' @param panel,grid Panel background and gridline colours.
#' @param ink,axis,subtitle Colours for titles, axis text/ticks, and subtitles.
#' @param palette Qualitative palette for groups: a vector of colours, or the name of an
#'   [grDevices::hcl.colors()] palette (e.g. `"viridis"`).
#' @param sequential Continuous ramp (>= 2 colours) for a variable mapped to colour -- currently the
#'   `color.by` trace in [plotDepthProfiles()], the `color.by` track in [plotTracks()], and the
#'   heatmaps in [plotTimeAtDepth()]. The default spans blue to dark red through
#'   cyan, green, yellow and orange, which is what makes a temperature gradient legible at a glance
#'   across a page of small multiples; a monochrome ramp collapses that variation into one hue. Note it
#'   is chosen for discriminability rather than for monotonic lightness, so for a figure that must
#'   survive greyscale printing or that is read by value rather than by pattern, pass a perceptually
#'   uniform ramp instead, e.g. `sequential = hcl.colors(9, "Viridis")`.
#' @param day,night Fill colours for the diel-split day and night bars. The defaults pair a warm day
#'   with a cool night, so the two sides of a mirrored profile read as two categories rather than one
#'   variable at two intensities.
#' @param day.border Outline for the day bar. Left `NULL` while `day` is supplied, it is derived as a
#'   darker shade of `day`, so a restyled fill keeps a matching outline; name it to override. The
#'   night and pooled bars always derive their outline from their own fill.
#' @param bar.alpha Fill opacity for bars (0-1).
#' @param bar.border Bar border colour.
#' @param font.family Font family (e.g. `""`/`"sans"`, `"serif"`).
#' @param cex Master text-scaling factor.
#' @return A `nautilus_theme` object.
#' @seealso [plotTimeAtDepth()]
#' @examples
#' # Build a theme object to pass as the `theme` argument of a nautilus plotter
#' th <- plotTheme("light")
#' th <- plotTheme("minimal", cex = 1.2)
#' th <- plotTheme(day = "#EAF3FB", palette = "viridis")
#' @export
plotTheme <- function(preset = c("light", "minimal", "classic"),
                      panel = NULL, grid = NULL, ink = NULL, axis = NULL, subtitle = NULL,
                      palette = NULL, sequential = NULL, day = NULL, night = NULL, day.border = NULL,
                      bar.alpha = NULL, bar.border = NULL, font.family = NULL, cex = NULL) {
  preset <- match.arg(preset)
  th <- .themePreset(preset)
  ov <- list(panel = panel, grid = grid, ink = ink, axis = axis, subtitle = subtitle, palette = palette,
             sequential = sequential, day = day, night = night, day.border = day.border,
             bar.alpha = bar.alpha, bar.border = bar.border, font.family = font.family, cex = cex)
  for (k in names(ov)) if (!is.null(ov[[k]])) th[[k]] <- ov[[k]]

  .assert_number(th$bar.alpha, "bar.alpha", min = 0, max = 1)
  .assert_number(th$cex, "cex", min = 0)
  # min = 0 is inclusive, and cex = 0 reaches base graphics as an opaque
  # 'invalid value specified for graphical parameter "cex.axis"'
  if (th$cex <= 0) .abort("{.arg cex} in {.fn plotTheme} must be greater than zero.")
  .assert_string(th$font.family, "font.family")
  # a caller who restyles `day` but not `day.border` gets a border derived from their own colour,
  # so the two never drift apart; naming both keeps the explicit value.
  if (!is.null(day) && is.null(day.border)) th$day.border <- .darkenColor(th$day)
  for (nm in c("panel", "grid", "ink", "axis", "subtitle", "day", "night", "day.border", "bar.border"))
    if (!.isColour(th[[nm]])) .abort("{.arg {nm}} in {.fn plotTheme} must be a single valid colour.")
  if (length(th$sequential) < 2 || !all(vapply(th$sequential, .isColour, logical(1))))
    .abort("{.arg sequential} in {.fn plotTheme} must be >= 2 valid colours.")
  # `palette` was the one colour slot never value-checked. A single string is allowed because
  # .themePalette() accepts an hcl.colors() palette NAME there; anything longer must be real colours,
  # or a typo reaches grDevices mid-render as a bare "invalid color name" that names neither the
  # argument nor the offending entry - the exact failure the plotter-level check used to prevent.
  if (!length(th$palette)) .abort("{.arg palette} in {.fn plotTheme} must not be empty.")
  if (length(th$palette) > 1L) {
    bad <- th$palette[!vapply(th$palette, .isColour, logical(1))]
    if (length(bad))
      .abort(c("{.arg palette} in {.fn plotTheme} contains {length(bad)} value{?s} that {?is/are} not a colour: {.val {bad}}.",
               "i" = "Use a colour name from {.fn grDevices::colors}, a hex string such as {.val #4C72B0},",
               "i" = "or a single {.fn grDevices::hcl.colors} palette name."))
  }
  th$preset <- preset
  structure(th, class = c("nautilus_theme", "list"))
}
