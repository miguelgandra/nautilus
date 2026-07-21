#######################################################################################################
# Plot theme (a shared, publication-ready visual style; one object customises every nautilus plot) ###
#######################################################################################################

#' Visual theme for nautilus plots
#'
#' @description
#' A small style object controlling the shared look of the nautilus plotting functions
#' (\link{plotTimeAtDepth}, \link{plotDistributions}, ...). Pass it as their `theme` argument. Start
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
#'   \code{\link[grDevices]{hcl.colors}} palette (e.g. `"viridis"`).
#' @param sequential Sequential ramp (>= 2 colours) for the heatmap style.
#' @param day,night,day.border Colours for the diel-split (day / night) bars and the day-bar border.
#' @param bar.alpha Fill opacity for bars (0-1).
#' @param bar.border Bar border colour.
#' @param font.family Font family (e.g. `""`/`"sans"`, `"serif"`).
#' @param cex Master text-scaling factor.
#' @return A `nautilus_theme` object.
#' @seealso \link{plotTimeAtDepth}
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

#' Base field set for each theme preset.
#' @keywords internal
#' @noRd
.themePreset <- function(preset) {
  common <- list(palette = c("#2FA4A0", "#E7913B", "#3E86C0", "#8B6BB1", "#C25B56", "#7FA65E", "#5B7FBD", "#C77F9E"),
                 sequential = c("#F7FBFF", "#3B7BB4", "#08306B"), bar.alpha = 1, font.family = "", cex = 1)
  switch(preset,
    light   = c(list(panel = "grey97", grid = "grey88", ink = "#1B1F27", axis = "#586074",
                     subtitle = "#9AA1B0", day = "#DCEAF6", night = "#294763", day.border = "#AFC9E0",
                     bar.border = "#FFFFFF"), common),
    minimal = c(list(panel = "#FFFFFF", grid = "#E7E9EE", ink = "#1B1F27", axis = "#586074",
                     subtitle = "#9AA1B0", day = "#DCEAF6", night = "#294763", day.border = "#AFC9E0",
                     bar.border = "#FFFFFF"), common),
    classic = c(list(panel = "#EDEDED", grid = "#FFFFFF", ink = "#000000", axis = "#333333",
                     subtitle = "#666666", day = "#FFFFFF", night = "#4D4D4D", day.border = "#333333",
                     bar.border = "#4D4D4D"),
                utils::modifyList(common, list(palette = grey.colors(6, 0.25, 0.75), bar.border = "#4D4D4D"))))
}

#' Is `x` a single, valid colour?
#' @keywords internal
#' @noRd
.isColour <- function(x) {
  length(x) == 1L && (is.character(x) || is.numeric(x)) &&
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
