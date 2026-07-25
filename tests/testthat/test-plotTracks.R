# Tests for plotTracks() - the movement-track map plotter. Because it draws only lightweight, optional
# base-graphics layers (no tile server / rJava / network), the full render path IS exercised here against
# a throwaway PDF device, unlike the old plotMaps().

Sys.setlocale("LC_TIME", "C")

.mk_track_tag <- function(id = "A01", n = 300, with_track = TRUE, with_fixes = TRUE) {
  t0 <- as.POSIXct("2021-01-01 00:00:00", tz = "UTC")
  lon <- -25.3 + cumsum(rep(0.0004, n)); lat <- 37 + cumsum(rep(0.0003, n))
  d <- data.table::data.table(ID = id, datetime = t0 + seq_len(n), depth = seq(0, 30, length.out = n))
  if (with_track)
    d[, `:=`(pseudo_lon = lon, pseudo_lat = lat, pseudo_depth = depth,
             pseudo_error = seq(50, 800, length.out = n), speed_dr = seq(0.3, 1.2, length.out = n))]
  m <- nautilus:::.newNautilusMeta(); m$id <- id
  m$deployment$lon <- lon[1]; m$deployment$lat <- lat[1]; m$deployment$datetime <- t0
  m$deployment$popup_lon <- lon[n]; m$deployment$popup_lat <- lat[n]; m$deployment$popup_datetime <- t0 + n
  if (with_fixes) {
    fi <- c(1, n %/% 2, n)
    m$ancillary$positions <- list(source = "test", data = data.frame(
      datetime = t0 + fi, type = c("User", "FastGPS", "Argos"),
      lon = lon[fi], lat = lat[fi], quality = c(NA, "7", NA), stringsAsFactors = FALSE))
  }
  nautilus:::new_nautilus_tag(d, m)
}

# render to a throwaway PDF so the full draw path executes without a screen device
draw_to_pdf <- function(expr) {
  pf <- tempfile(fileext = ".pdf"); grDevices::pdf(pf)
  on.exit({ if (grDevices::dev.cur() != 1L) grDevices::dev.off(); unlink(pf) }, add = TRUE)
  force(expr)
}

# Render to an UNCOMPRESSED pdf() and read back what was actually drawn, so theme effects can be asserted
# on the rendered output rather than on a call that merely did not error. Base pdf() writes each string as
# a text matrix ("<a> <b> <c> <d> <x> <y> Tm"), whose scale is the point size the glyphs were drawn at,
# and declares the typefaces it used as /BaseFont entries.
render_pdf_lines <- function(expr) {
  pf <- tempfile(fileext = ".pdf")
  grDevices::pdf(pf, compress = FALSE)
  on.exit({ if (grDevices::dev.cur() != 1L) grDevices::dev.off(); unlink(pf) }, add = TRUE)
  force(expr)
  grDevices::dev.off()
  readLines(pf, warn = FALSE)
}
pdf_text_sizes <- function(lines) {
  hit <- regmatches(lines, regexpr("Tf +-?[0-9.]+ +-?[0-9.]+", lines))
  if (!length(hit)) return(numeric(0))
  nums <- lapply(strsplit(sub("^Tf +", "", hit), " +"), as.numeric)
  vapply(nums, function(v) max(abs(v)), numeric(1))              # upright OR rotated text
}
pdf_fonts <- function(lines) unique(regmatches(lines, regexpr("/BaseFont /[A-Za-z-]+", lines)))

test_that("plotTracks returns a per-deployment summary and renders without error", {
  res <- draw_to_pdf(plotTracks(list(A01 = .mk_track_tag()), verbose = FALSE))
  expect_s3_class(res, "data.frame")
  expect_named(res, c("id", "n_fix", "n_track", "drawn"))
  expect_equal(res$n_fix, 3L)
  expect_equal(res$n_track, 300L)
  expect_true(res$drawn)
})

test_that("color.by = depth/speed and show.uncertainty run the full draw path", {
  expect_silent(draw_to_pdf(plotTracks(list(A01 = .mk_track_tag()), color.by = "depth",
                                       show.uncertainty = TRUE, verbose = FALSE)))
  expect_silent(draw_to_pdf(plotTracks(list(A01 = .mk_track_tag()), color.by = "speed", verbose = FALSE)))
})

test_that("a fixes-only deployment (no pseudo-track) still plots", {
  res <- draw_to_pdf(plotTracks(list(A01 = .mk_track_tag(with_track = FALSE)), verbose = FALSE))
  expect_equal(res$n_track, 0L)
  expect_true(res$drawn)
})

test_that("a deployment with neither fixes nor a track is skipped, not drawn", {
  res <- draw_to_pdf(plotTracks(list(A01 = .mk_track_tag(with_track = FALSE, with_fixes = FALSE)),
                                verbose = FALSE))
  expect_false(res$drawn)
})

test_that("writes a multi-page PDF and paginates across pages", {
  tags <- stats::setNames(lapply(sprintf("A%02d", 1:5), function(id) .mk_track_tag(id, n = 50)),
                          sprintf("A%02d", 1:5))
  pf <- withr::local_tempfile(fileext = ".pdf")
  res <- plotTracks(tags, ncols = 2, nrows = 2, plot = FALSE, plot.file = pf, verbose = FALSE)
  expect_true(file.exists(pf) && file.info(pf)$size > 0)
  expect_equal(nrow(res), 5L)
  expect_true(all(res$drawn))
})

test_that("argument validation is strict and cli-formatted", {
  expect_error(plotTracks(list(A01 = .mk_track_tag()), color.by = "bogus"), "color.by")
  expect_error(plotTracks(list(A01 = .mk_track_tag()), plot = FALSE), "Nothing to plot")
  expect_error(plotTracks(list(A01 = .mk_track_tag()), theme = "light"), "theme")
  expect_error(plotTracks(list(A01 = .mk_track_tag()), colors = "red"), "NAMED")
  expect_error(plotTracks(character(0)), "empty|no ")   # .resolveInput fail-loud on empty
})

test_that("the pseudo-track is drawn in time order regardless of input row order", {
  # a shuffled track: .gatherPseudoTrack must re-sort by datetime so endpoints are correct
  tag <- .mk_track_tag(n = 100)
  d <- data.table::as.data.table(tag)
  shuffled <- d[sample(nrow(d))]
  tr <- nautilus:::.gatherPseudoTrack(shuffled, "datetime", NULL, 5000)
  expect_equal(tr$lon, sort(tr$lon))              # ascending lon == chronological (track goes east)
})


#######################################################################################################
# Theme migration #####################################################################################
#
# plotTracks used to ACCEPT a `theme` and then ignore most of it, while carrying its own `cex = 1`:
# theme$cex and theme$font.family reached nothing at all. These tests assert on what was actually
# RENDERED, because a text size or a colour never reaches a return value - a call that runs quietly
# proves only that the argument was accepted, not that it was used.

test_that("theme replaces the old cex argument entirely", {
  expect_false("cex" %in% names(formals(plotTracks)))
  expect_true(all(c("theme", "colors") %in% names(formals(plotTracks))))
  # The old default was cex = 1 and plotTheme()$cex is 1, so - unlike plotDistributions - there is no
  # legacy base to fold in here: the default figure is pixel-for-pixel the pre-migration one.
  expect_equal(plotTheme()$cex, 1)
})

test_that("theme$cex actually scales the text on the page (the bug: it was ignored)", {
  tag   <- list(A01 = .mk_track_tag(n = 60))
  small <- pdf_text_sizes(render_pdf_lines(plotTracks(tag, theme = plotTheme(), verbose = FALSE)))
  big   <- pdf_text_sizes(render_pdf_lines(plotTracks(tag, theme = plotTheme(cex = 1.6), verbose = FALSE)))
  expect_gt(length(small), 5L)                       # titles, axes, legend, colour-bar labels ...
  # Compare the SIZE CLASSES, not the individual strings: pdf() quantises text to whole points, and
  # base axis() silently drops tick labels that would overlap once the text grows, so the two pages do
  # not carry the same number of strings.
  cls_small <- sort(unique(round(small))); cls_big <- sort(unique(round(big)))
  expect_equal(length(cls_big), length(cls_small))
  expect_true(all(cls_big > cls_small))              # EVERY size class on the page grew
  expect_equal(max(cls_big) / max(cls_small), 1.6, tolerance = 0.1)
})

test_that("theme$font.family reaches the device (the typeface really changes)", {
  tag   <- list(A01 = .mk_track_tag(n = 60))
  sans  <- pdf_fonts(render_pdf_lines(plotTracks(tag, theme = plotTheme(), verbose = FALSE)))
  serif <- pdf_fonts(render_pdf_lines(plotTracks(tag, theme = plotTheme(font.family = "serif"), verbose = FALSE)))
  expect_true(any(grepl("Helvetica", sans)))
  expect_false(any(grepl("Times", sans)))
  expect_true(any(grepl("Times", serif)))            # par(family=) is set on the device, then restored
})

test_that("a bad theme is rejected by name rather than failing deep inside the drawing code", {
  expect_error(plotTracks(list(A01 = .mk_track_tag()), theme = list(panel = "not-a-colour"),
                          verbose = FALSE), "panel")
  expect_error(plotTracks(list(A01 = .mk_track_tag()), theme = list(cex = 0), verbose = FALSE), "cex")
  expect_error(plotTracks(list(A01 = .mk_track_tag()), theme = list(bogus = 1), verbose = FALSE), "bogus")
})

test_that("`colors` stays a SEMANTIC map palette, and its values are validated by name", {
  tag <- list(A01 = .mk_track_tag(n = 60))
  # a typo in a VALUE used to travel all the way into grDevices as a bare "invalid color name"
  err <- expect_error(plotTracks(tag, colors = c(track = "definitely-not-a-colour"), verbose = FALSE),
                      "colors")
  expect_match(conditionMessage(err), "track")
  expect_match(conditionMessage(err), "definitely-not-a-colour")
  # a typo in a NAME used to be silently added to the palette and ignored
  expect_error(plotTracks(tag, colors = c(tracks = "red"), verbose = FALSE), "unrecognised")
})

test_that("a `colors` override reaches the drawn page", {
  tag <- list(A01 = .mk_track_tag(n = 60))
  plain <- render_pdf_lines(plotTracks(tag, verbose = FALSE))
  red   <- render_pdf_lines(plotTracks(tag, colors = c(sea = "#FF0000"), verbose = FALSE))
  expect_false(any(grepl("^1.000 0.000 0.000 scn", plain)))
  expect_true(any(grepl("^1.000 0.000 0.000 scn", red)))          # the sea rectangle, now pure red
})

test_that("the theme reaches the canvas, and start/end stay a contrast pair", {
  # NOT a source grep: a grep proves a literal is absent from a file, not that a colour reached the
  # figure, and it ERRORs under R CMD check where ../../R does not exist. Watch the graphics calls.
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf))
  seen <- list(box = character(0), pts_bg = character(0), pts_col = character(0))
  testthat::with_mocked_bindings(
    box = function(which = "plot", col = NA, ...) { seen$box <<- c(seen$box, as.character(col)); invisible(NULL) },
    points = function(x, y, pch = NA, bg = NA, col = NA, ...) {
      seen$pts_bg  <<- c(seen$pts_bg,  as.character(bg))
      seen$pts_col <<- c(seen$pts_col, as.character(col)); invisible(NULL)
    },
    .package = "graphics",
    suppressMessages(suppressWarnings(
      plotTracks(list(A01 = .mk_track_tag()), theme = plotTheme(axis = "#123456"),
                 plot = FALSE, plot.file = pf, verbose = FALSE))))
  expect_true("#123456" %in% seen$box)          # the frame is the theme's axis colour, not a literal
  expect_false("#BBBBBB" %in% seen$box)

  # start and end are told apart by FILL. Routing that through a chrome slot collapsed them under the
  # classic preset (bar.border #4D4D4D against ink #000000: two dark disks). They live in `colors`.
  expect_true("#FFFFFF" %in% seen$pts_bg)
  expect_true("#111111" %in% seen$pts_bg)
  for (pre in c("light", "classic", "minimal")) {
    seen$pts_bg <- character(0)
    testthat::with_mocked_bindings(
      points = function(x, y, pch = NA, bg = NA, col = NA, ...) {
        seen$pts_bg <<- c(seen$pts_bg, as.character(bg)); invisible(NULL) },
      .package = "graphics",
      suppressMessages(suppressWarnings(
        plotTracks(list(A01 = .mk_track_tag()), theme = plotTheme(pre), plot = FALSE, plot.file = pf, verbose = FALSE))))
    expect_true(all(c("#FFFFFF", "#111111") %in% seen$pts_bg),
                info = paste("start/end collapsed under preset", pre))
  }
})



test_that("a dense page does not abort at a large theme$cex", {
  # Scaling `mar` by cex fixed an overprint at cex 1.6 on a 1-panel figure and introduced an ABORT on
  # a 2x5 one - mar is measured in text lines against a fixed figure region, so it overran it. Every
  # test and demo image at the time rendered 1 or 3 panels, so nothing caught it. Now capped.
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf))
  tags <- stats::setNames(lapply(sprintf("T%02d", 1:10), function(i) .mk_track_tag(id = i)),
                          sprintf("T%02d", 1:10))
  for (cx in c(1, 1.6, 2.5))
    expect_no_error(suppressMessages(suppressWarnings(
      plotTracks(tags, theme = plotTheme(cex = cx), plot = FALSE, plot.file = pf, verbose = FALSE))),
      message = paste("aborted at theme$cex =", cx))
})

test_that("a colour that is not a colour is rejected by name, NA included", {
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf))
  p <- function(...) suppressMessages(plotTracks(list(A01 = .mk_track_tag()), ...,
                                                 plot = FALSE, plot.file = pf, verbose = FALSE))
  expect_error(p(colors = c(track = "notacolour")), "not a valid colour")
  # NA used to pass: col2rgb(NA) SUCCEEDS and returns white, so the element drew transparent instead
  # of the call aborting - the same failure class the check was added to close.
  expect_error(p(colors = c(track = NA_character_)), "not a valid colour")
  expect_error(p(colors = c(nosuchelement = "red")), "unrecognised")
})

# ---- Phase 1: basemap canvas + coastline ladder + bathy.contours -------------------------------------

test_that("the raster canvases are dependency-gated, and a bad canvas object is rejected", {
  tag <- list(A01 = .mk_track_tag(n = 40))
  # a canvas object of the wrong class is rejected with a pointer to getBasemap (SpatRaster or bathy)
  expect_error(draw_to_pdf(plotTracks(tag, basemap = matrix(1, 2, 2), verbose = FALSE)), "SpatRaster")
  # an unknown keyword is rejected by match.arg
  expect_error(draw_to_pdf(plotTracks(tag, basemap = "bogus", verbose = FALSE)))
  # satellite needs maptiles: on a machine without it, a clean install-hint error
  skip_if(requireNamespace("maptiles", quietly = TRUE), "maptiles installed - the guard is not exercised")
  expect_error(draw_to_pdf(plotTracks(tag, basemap = "satellite", verbose = FALSE)), "maptiles")
})

test_that("a pre-fetched SpatRaster basemap renders (canvas + coastline outline), needing no network", {
  skip_if_not_installed("terra")
  tag <- list(A01 = .mk_track_tag(n = 60))
  r <- terra::rast(nrows = 30, ncols = 30, xmin = -25.35, xmax = -24.95, ymin = 36.9, ymax = 37.1,
                   nlyrs = 3, crs = "EPSG:4326")
  terra::values(r) <- cbind(120, 150, 180)
  attr(r, "nautilus.credit") <- "Tiles (c) Test"
  expect_silent(draw_to_pdf(plotTracks(tag, basemap = r, verbose = FALSE)))            # + coastline outline
  expect_silent(draw_to_pdf(plotTracks(tag, basemap = r, coastline = "none", verbose = FALSE)))  # imagery only
})

test_that("basemapControl validates; getBasemap's bathymetry type is marmap-gated", {
  expect_s3_class(basemapControl(), "nautilus_basemap")
  expect_error(basemapControl(cache = 1L), "cache")
  skip_if(requireNamespace("marmap", quietly = TRUE), "marmap installed - the fetch guard is not exercised")
  expect_error(getBasemap(list(A01 = .mk_track_tag(n = 20)), type = "bathymetry"), "marmap")
})

test_that("basemap = 'none' and the coastline keywords all render the full draw path", {
  tag <- list(A01 = .mk_track_tag(n = 60))
  expect_silent(draw_to_pdf(plotTracks(tag, basemap = "none", verbose = FALSE)))
  expect_silent(draw_to_pdf(plotTracks(tag, coastline = "none", verbose = FALSE)))
  expect_silent(draw_to_pdf(plotTracks(tag, coastline = "low", verbose = FALSE)))
})

test_that("a custom coastline draws with no map packages required (data.frame of lon/lat)", {
  # a small square 'island' near the synthetic track; graphics::polygon path, zero dependencies
  ring <- data.frame(lon = c(-25.29, -25.27, -25.27, -25.29, -25.29),
                     lat = c(37.02, 37.02, 37.04, 37.04, 37.02))
  expect_silent(draw_to_pdf(plotTracks(list(A01 = .mk_track_tag(n = 60)),
                                       coastline = ring, verbose = FALSE)))
  # a two-column matrix is accepted too
  expect_silent(draw_to_pdf(plotTracks(list(A01 = .mk_track_tag(n = 60)),
                                       coastline = as.matrix(ring), verbose = FALSE)))
})

test_that("coastline = 'high' errors with an install hint when mapdata is absent", {
  skip_if(requireNamespace("mapdata", quietly = TRUE), "mapdata is installed - fallback path not exercised")
  expect_error(nautilus:::.resolveCoastline("high"), "mapdata")
  expect_error(draw_to_pdf(plotTracks(list(A01 = .mk_track_tag(n = 40)),
                                      coastline = "high", verbose = FALSE)), "mapdata")
})

test_that("bathy.contours validates its value and requires marmap", {
  tag <- list(A01 = .mk_track_tag(n = 40))
  # bad value
  expect_error(draw_to_pdf(plotTracks(tag, bathy.contours = "deep", verbose = FALSE)),
               "bathy.contours")
  # on (TRUE or numeric levels) needs marmap
  skip_if(requireNamespace("marmap", quietly = TRUE), "marmap installed - the guard is not exercised")
  expect_error(draw_to_pdf(plotTracks(tag, bathy.contours = TRUE, verbose = FALSE)), "marmap")
  expect_error(draw_to_pdf(plotTracks(tag, bathy.contours = c(-50, -200), verbose = FALSE)), "marmap")
})

test_that("the auto coastline falls back with a single hint when mapdata is absent", {
  skip_if(requireNamespace("mapdata", quietly = TRUE), "mapdata installed - no fallback")
  skip_if_not(requireNamespace("maps", quietly = TRUE), "maps needed for the world fallback")
  txt <- paste(cli::cli_fmt(nautilus:::.resolveCoastline("auto", 2L)), collapse = "\n")
  expect_match(txt, "low-resolution")
  expect_match(txt, "mapdata")
})

# ---- Phase 3: bathymetric relief canvas -------------------------------------------------------------

# a marmap-shaped depth grid (rownames = lon, colnames = lat, z = depth; +ve is land): a central island
# ringed by deepening sea. Lets the relief canvas be exercised without marmap or a network call.
.mk_bathy <- function(xlim = c(-25.36, -24.94), ylim = c(36.88, 37.12), nx = 60) {
  glon <- seq(xlim[1], xlim[2], length.out = nx); glat <- seq(ylim[1], ylim[2], length.out = nx)
  z <- outer(glon, glat, function(a, b) {
    r <- sqrt(((a + 25.15) / 0.10)^2 + ((b - 37.0) / 0.08)^2)
    ifelse(r < 0.5, 30, -2000 * (r - 0.5))
  })
  dimnames(z) <- list(glon, glat); class(z) <- "bathy"; z
}

test_that("a pre-fetched bathy grid is accepted as the canvas and renders (no marmap needed)", {
  tag <- list(A01 = .mk_track_tag(n = 60))
  z <- .mk_bathy()
  spec <- nautilus:::.resolveBasemap(z, c("land", "bathymetry", "satellite", "none"))
  expect_identical(spec$kind, "bathymetry")
  expect_false(is.null(spec$bathy))
  expect_silent(draw_to_pdf(plotTracks(tag, basemap = z, verbose = FALSE)))
})

test_that("the depth CANVAS and the depth CONTOURS compose over one grid", {
  tag <- list(A01 = .mk_track_tag(n = 60)); z <- .mk_bathy()
  # relief alone, isobaths alone (over the default land canvas is marmap-gated, so use the grid), and both
  expect_silent(draw_to_pdf(plotTracks(tag, basemap = z, verbose = FALSE)))
  expect_silent(draw_to_pdf(plotTracks(tag, basemap = z, bathy.contours = TRUE, verbose = FALSE)))
  expect_silent(draw_to_pdf(plotTracks(tag, basemap = z, bathy.contours = c(-200, -800), verbose = FALSE)))
})

test_that("sea.deep is a recognised map-palette entry (the deep end of the relief ramp)", {
  tag <- list(A01 = .mk_track_tag(n = 40)); z <- .mk_bathy()
  expect_silent(draw_to_pdf(plotTracks(tag, basemap = z, colors = c(sea.deep = "#12303F"), verbose = FALSE)))
  expect_error(draw_to_pdf(plotTracks(tag, basemap = z, colors = c(sea.deep = "not-a-colour"),
                                      verbose = FALSE)))
})

test_that("filterLocations rejects a bathymetry canvas (depth does not serve location QC)", {
  tag <- list(A01 = .mk_track_tag(n = 40))
  expect_error(filterLocations(tag, basemap = .mk_bathy(), plot = TRUE, verbose = FALSE),
               "not available in")
})

test_that("basemap = 'bathymetry' needs marmap to fetch, but a pre-fetched grid does not", {
  tag <- list(A01 = .mk_track_tag(n = 40))
  skip_if(requireNamespace("marmap", quietly = TRUE), "marmap installed - the fetch guard is not exercised")
  expect_error(draw_to_pdf(plotTracks(tag, basemap = "bathymetry", verbose = FALSE)), "marmap")
  # contours alongside a pre-fetched grid must NOT demand marmap (nothing left to fetch)
  expect_silent(draw_to_pdf(plotTracks(tag, basemap = .mk_bathy(), bathy.contours = TRUE, verbose = FALSE)))
})

# ---- regression: the worldHires database must actually be USABLE, not merely installed --------------
# maps::map() resolves a database NAME through the search path, so requireNamespace("mapdata") alone
# left "worldHires" unresolvable; the error was swallowed per panel and maps came out with NO LAND.

test_that(".withMapdata makes worldHires resolvable and restores the search path", {
  skip_if_not_installed("mapdata")
  skip_if_not_installed("maps")
  was_attached <- "package:mapdata" %in% search()
  nm <- nautilus:::.withMapdata("worldHires", maps::map("worldHires", plot = FALSE, namesonly = TRUE,
                                                       xlim = c(-25.9, -24.6), ylim = c(36.6, 37.5)))
  expect_true(length(nm) >= 1L)                       # the Azores islands are in this window
  expect_true(any(grepl("Azores", nm)))
  # the search path is left exactly as we found it
  expect_equal("package:mapdata" %in% search(), was_attached)
})

test_that("a bare maps::map('worldHires') call is NOT resolvable without the attach (the bug)", {
  skip_if_not_installed("mapdata")
  skip_if("package:mapdata" %in% search(), "mapdata is attached in this session")
  expect_error(maps::map("worldHires", plot = FALSE, namesonly = TRUE,
                         xlim = c(-25.9, -24.6), ylim = c(36.6, 37.5)))
})

test_that("coastline = 'high' actually draws land (not a silent no-op)", {
  skip_if_not_installed("mapdata")
  # the window must CONTAIN land for this to mean anything: a track around Santa Maria (Azores).
  # The default fixture sits in open water just NW of it, where every coastline setting draws the same.
  t0 <- as.POSIXct("2021-01-01", tz = "UTC"); n <- 60
  lon <- seq(-25.20, -25.00, length.out = n); lat <- seq(36.92, 37.02, length.out = n)
  d <- data.table::data.table(ID = "SM", datetime = t0 + seq_len(n), depth = 0,
                              pseudo_lon = lon, pseudo_lat = lat, pseudo_depth = 0,
                              pseudo_error = 100, speed_dr = 1)
  m <- nautilus:::.newNautilusMeta(); m$id <- "SM"
  m$deployment$lon <- lon[1]; m$deployment$lat <- lat[1]; m$deployment$datetime <- t0
  tag <- list(SM = nautilus:::new_nautilus_tag(d, m))
  with_land <- render_pdf_lines(plotTracks(tag, coastline = "high", verbose = FALSE))
  no_land   <- render_pdf_lines(plotTracks(tag, coastline = "none", verbose = FALSE))
  expect_gt(length(with_land), length(no_land))       # land adds real drawing operations
  # and the high-resolution database is materially richer than the coarse one at island scale
  low_land <- render_pdf_lines(plotTracks(tag, coastline = "low", verbose = FALSE))
  expect_gt(length(with_land), length(low_land))
})

test_that(".mapdataUsable() reports the database as usable when mapdata is installed", {
  skip_if_not_installed("mapdata")
  expect_true(nautilus:::.mapdataUsable())
})
