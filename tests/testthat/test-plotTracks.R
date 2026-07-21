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
