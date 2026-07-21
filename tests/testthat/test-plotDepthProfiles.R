# Tests for plotDepthProfiles(): the pure internals (layout/pagination, depth-axis, downsampling,
# label defaults) and argument validation are unit-tested here; a synthetic end-to-end draw to a
# temporary PDF exercises the full path without external data or an interactive device.

test_that(".depthProfileLayout picks sensible grids and paginates", {
  L1 <- nautilus:::.depthProfileLayout(1, NULL, NULL, legend = FALSE)
  expect_equal(c(L1$nrows, L1$ncols), c(1, 1))
  expect_length(L1$pages, 1)

  L6 <- nautilus:::.depthProfileLayout(6, NULL, NULL, legend = FALSE)
  expect_equal(c(L6$nrows, L6$ncols), c(3, 2))          # auto: 2 cols
  expect_length(L6$pages, 1)

  L18 <- nautilus:::.depthProfileLayout(18, NULL, NULL, legend = FALSE)
  expect_equal(c(L18$nrows, L18$ncols), c(5, 2))        # capped at 5 rows -> 10 per page
  expect_equal(L18$per_page, 10)
  expect_length(L18$pages, 2)

  # explicit grid is respected and paginated
  L <- nautilus:::.depthProfileLayout(40, 2, 9, legend = FALSE)
  expect_equal(L$per_page, 18)
  expect_length(L$pages, 3)

  # a shared legend takes the LAST grid CELL (a horizontal bar), not an extra side column
  Lg <- nautilus:::.depthProfileLayout(6, NULL, NULL, legend = TRUE)
  expect_equal(ncol(Lg$matrix), Lg$ncols)              # no extra column added
  expect_length(Lg$widths, Lg$ncols)
  expect_equal(Lg$legend_cell, Lg$per_page)            # legend occupies the last (bottom-right) cell
  expect_equal(Lg$capacity, Lg$per_page - 1L)          # one fewer panel slot per page
  expect_length(Lg$pages, 1)                           # auto-grid grew (4x2) so all 6 fit on one page + legend
  expect_equal(c(Lg$nrows, Lg$ncols), c(4, 2))
  # a full grid with an explicit legend reserves the last cell (explicit dims respected, capacity drops)
  Le <- nautilus:::.depthProfileLayout(14, 2, 7, legend = TRUE)
  expect_equal(Le$per_page, 14); expect_equal(Le$capacity, 13); expect_equal(Le$legend_cell, 14)
  # single panel: no spare cell -> falls back to that panel's own legend (no reservation)
  L1g <- nautilus:::.depthProfileLayout(1, NULL, NULL, legend = TRUE)
  expect_true(is.na(L1g$legend_cell))
})

test_that(".depthYlim inverts the axis with headroom for the ID label", {
  y <- nautilus:::.depthYlim(47)
  expect_equal(y, c(50, -9))                            # top rounded to 10s; 18% headroom above 0
  expect_gt(y[1], y[2])                                 # inverted (deep at the bottom)
  expect_equal(nautilus:::.depthYlim(0), c(10, -1.8))   # degenerate depth -> a usable default
})

test_that(".downsampleForPlot honours id.col/datetime.col and never mutates the source", {
  dt <- data.table::data.table(
    animal = "X",
    time   = as.POSIXct("2020-01-01 00:00:00", tz = "UTC") + (0:29),   # 30 s, 1 Hz
    depth  = as.numeric(1:30),
    temp   = as.numeric(30:1))
  r <- nautilus:::.downsampleForPlot(dt, id.col = "animal", datetime.col = "time", seconds = 5)
  expect_equal(nrow(r), 6)                              # 30 s -> six 5 s bins (the NSE-hardcoding bug fix)
  expect_true(all(c("animal", "time", "depth", "temp") %in% names(r)))
  expect_equal(r$depth[1], mean(1:5))                   # first bin averaged
  # the source table is untouched (no defensive copy needed, no stray bin column added)
  expect_equal(nrow(dt), 30)
  expect_false(any(c(".bin", ".__bin__") %in% names(dt)))
  # seconds = NULL returns the data unchanged
  expect_identical(nautilus:::.downsampleForPlot(dt, "animal", "time", NULL), dt)
})

test_that(".defaultColorLabel maps known variables and falls back to the column name", {
  expect_equal(nautilus:::.defaultColorLabel("temp"), "Temperature (\u00b0C)")
  expect_equal(nautilus:::.defaultColorLabel("speed"), "Speed (m/s)")
  expect_equal(nautilus:::.defaultColorLabel("vedba"), "vedba")
})

test_that("plotDepthProfiles validates arguments before doing any work", {
  d <- data.frame(ID = "a", datetime = Sys.time(), depth = 1)
  expect_error(plotDepthProfiles(d, color.by = 1, verbose = FALSE), "color.by", ignore.case = TRUE)
  expect_error(plotDepthProfiles(d, plot.file = "x.png", verbose = FALSE), "plot.file", ignore.case = TRUE)
  expect_error(plotDepthProfiles(d, theme = list(cex = -1), verbose = FALSE), "cex", ignore.case = TRUE)
  expect_error(plotDepthProfiles(d, downsample = -5, verbose = FALSE), "downsample", ignore.case = TRUE)
  expect_error(plotDepthProfiles(d, ncols = 0, verbose = FALSE), "ncols", ignore.case = TRUE)
  # neither an active plot nor a file -> nothing to do
  expect_error(plotDepthProfiles(d, plot = FALSE, verbose = FALSE), "nothing to plot", ignore.case = TRUE)
})

test_that("plotDepthProfiles draws a multi-deployment PDF end to end (synthetic data)", {
  mk <- function(id, n = 200) data.frame(
    ID       = id,
    datetime = as.POSIXct("2020-01-01", tz = "UTC") + seq_len(n),
    depth    = abs(sin(seq_len(n) / 20)) * 40,
    temp     = 15 + cos(seq_len(n) / 30) * 3)
  tags <- list(A = mk("A"), B = mk("B"), C = mk("C"))
  tmp <- tempfile(fileext = ".pdf"); on.exit(unlink(tmp), add = TRUE)

  # default (shared scale + one legend); coords are absent -> diel shading is skipped, not an error
  expect_no_error(plotDepthProfiles(tags, plot = FALSE, plot.file = tmp, verbose = FALSE))
  expect_gt(file.size(tmp), 0)

  # per-individual scale (per-panel legends) + shared depth axis + shading off
  tmp2 <- tempfile(fileext = ".pdf"); on.exit(unlink(tmp2), add = TRUE)
  expect_no_error(plotDepthProfiles(tags, same.color.scale = FALSE, same.depth.scale = TRUE,
                                    shade.diel = FALSE, plot = FALSE, plot.file = tmp2, verbose = FALSE))
  expect_gt(file.size(tmp2), 0)

  # a single deployment
  tmp3 <- tempfile(fileext = ".pdf"); on.exit(unlink(tmp3), add = TRUE)
  expect_no_error(plotDepthProfiles(tags[1], plot = FALSE, plot.file = tmp3, verbose = FALSE))
  expect_gt(file.size(tmp3), 0)
})

test_that("plotDepthProfiles accepts each geom and rejects an unknown one", {
  mk <- function(id, n = 200) data.frame(
    ID = id, datetime = as.POSIXct("2020-01-01", tz = "UTC") + seq_len(n),
    depth = abs(sin(seq_len(n) / 20)) * 40, temp = 15 + cos(seq_len(n) / 30) * 3)
  tags <- list(A = mk("A"), B = mk("B"))
  for (g in c("line", "points", "both")) {
    tmp <- tempfile(fileext = ".pdf")
    expect_no_error(plotDepthProfiles(tags, geom = g, plot = FALSE, plot.file = tmp, verbose = FALSE))
    expect_gt(file.size(tmp), 0)
    unlink(tmp)
  }
  expect_error(plotDepthProfiles(tags, geom = "squiggle", plot = FALSE,
                                 plot.file = tempfile(fileext = ".pdf"), verbose = FALSE),
               "should be one of")                              # match.arg rejects it before any drawing
})

test_that(".drawColorLine breaks the trace across recording gaps and missing samples", {
  captured <- NULL
  testthat::local_mocked_bindings(
    segments = function(x0, y0, x1, y1, ...) { captured <<- list(x0 = x0, y0 = y0, y1 = y1); invisible() },
    .package = "graphics")
  # 6 samples ~30 s apart, EXCEPT a ~1 h jump between #3 and #4 (a gap), and #5's depth is NA
  t     <- as.POSIXct("2020-01-01", tz = "UTC") + c(0, 30, 60, 3600, 3630, 3660)
  depth <- c(5, 10, 15, 20, NA, 30)
  nautilus:::.drawColorLine(t, depth, rep("red", 6))
  # segment i joins sample i -> i+1; a dropped segment has x0 = NA (segments() then skips it)
  expect_false(is.na(captured$x0[1]))                          # 0 -> 30 s: kept
  expect_false(is.na(captured$x0[2]))                          # 30 -> 60 s: kept
  expect_true(is.na(captured$x0[3]))                           # 60 s -> 1 h jump: broken
  expect_true(is.na(captured$x0[4]))                           # into the NA-depth sample: broken
  expect_true(is.na(captured$x0[5]))                           # out of the NA-depth sample: broken
})


#######################################################################################################
# Theme migration #####################################################################################
#
# plotDepthProfiles used to carry its own `cex = 1.15` and a private jet-style ramp, while the rest of
# the family takes a shared `theme`. This is the migration.

# Two deployments, drawn to a throwaway PDF - enough to exercise the real drawing path.
.dpTags <- function() {
  mk <- function(id, n = 200) data.frame(
    ID = id, datetime = as.POSIXct("2020-01-01", tz = "UTC") + seq_len(n),
    depth = abs(sin(seq_len(n) / 20)) * 40, temp = 15 + cos(seq_len(n) / 30) * 3)
  list(A = mk("A"), B = mk("B"))
}

# The ramp the function is expected to build for a given theme: the theme's sequential colours,
# re-weighted 2:3 so the upper 30% of the ramp gets extra resolution (the shape inherited from the
# pre-theme jet default).
.dpExpectedRamp <- function(theme = plotTheme()) {
  base <- grDevices::colorRampPalette(theme$sequential)(100)
  grDevices::colorRampPalette(c(rep(base[1:70], each = 2), rep(base[71:100], each = 3)))(100)
}

# Capture the palette that actually reaches the panel drawer. A colour never reaches a return value,
# so a quiet call is no evidence at all that the argument was honoured - this asserts the resolved one.
.dpCapturePalette <- function(...) {
  captured <- NULL
  testthat::local_mocked_bindings(
    .drawDepthPanel = function(dep, color.by, depth.col, datetime.col, color.pal, ...) {
      captured <<- color.pal
      graphics::plot.new()                                     # keep the layout's cell sequence intact
    })
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf), add = TRUE)
  plotDepthProfiles(.dpTags(), ..., plot = FALSE, plot.file = pf, verbose = FALSE)
  captured
}

test_that("theme replaces the old cex argument entirely", {
  expect_false("cex" %in% names(formals(plotDepthProfiles)))
  expect_true("theme" %in% names(formals(plotDepthProfiles)))
  # point.size / lwd are geometry, not text scaling, so they stay
  expect_true(all(c("point.size", "lwd") %in% names(formals(plotDepthProfiles))))
})

test_that("a bad theme is rejected by name rather than failing deep inside the drawing code", {
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf))
  expect_error(plotDepthProfiles(.dpTags(), theme = list(panel = "not-a-colour"),
                                 plot = FALSE, plot.file = pf, verbose = FALSE), "panel")
  expect_error(plotDepthProfiles(.dpTags(), theme = list(sequential = "#FF0000"),
                                 plot = FALSE, plot.file = pf, verbose = FALSE), "sequential")
  expect_error(plotDepthProfiles(.dpTags(), theme = list(nonsense = 1),
                                 plot = FALSE, plot.file = pf, verbose = FALSE), "nonsense")
  expect_error(plotDepthProfiles(.dpTags(), theme = "light",
                                 plot = FALSE, plot.file = pf, verbose = FALSE), "plotTheme")
})

test_that("the theme reaches the drawing layer with its values intact", {
  # NOT a source grep. A grep survives `cex <- theme$cex * 1.15` followed one line later by a
  # `cex <- 1.15` that throws the theme away, and it survives every slot being swapped for a literal
  # so long as the text still appears somewhere. It also breaks under R CMD check, where tests run
  # from <pkg>.Rcheck/tests/testthat and ../../R does not exist. Capture the real arguments instead.
  seen <- NULL
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf))
  cap <- function(dep, color.by, depth.col, datetime.col, color.pal, panel_range, depth_ylim,
                  cex, theme, ...) { seen <<- list(cex = cex, theme = theme); invisible(NULL) }
  testthat::local_mocked_bindings(.drawDepthPanel = cap, .package = "nautilus")
  suppressMessages(plotDepthProfiles(.dpTags(), theme = plotTheme(ink = "#123456"),
                                     plot = FALSE, plot.file = pf, verbose = FALSE))
  expect_false(is.null(seen))
  expect_equal(seen$cex, 1.15)                    # the tuned base is folded in at theme$cex = 1
  expect_equal(seen$theme$ink, "#123456")
  suppressMessages(plotDepthProfiles(.dpTags(), theme = plotTheme(cex = 2),
                                     plot = FALSE, plot.file = pf, verbose = FALSE))
  expect_equal(seen$cex, 2.3)                     # ...and theme$cex still scales it
})


test_that("the default colour ramp is built from the theme's sequential colours", {
  expect_identical(.dpCapturePalette(), .dpExpectedRamp())
  # a different sequential ramp changes the mapped colours (not merely accepted in silence)
  th <- plotTheme(sequential = c("#000000", "#FF0000"))
  got <- .dpCapturePalette(theme = th)
  expect_identical(got, .dpExpectedRamp(th))
  expect_false(identical(got, .dpExpectedRamp()))
  # The weighting is load-bearing, not decoration: with a plain black -> white base the ramp is NOT
  # linear. Its midpoint sits PAST mid-grey, because the upper 30% of the base is stretched 3:2, which
  # is what keeps the high end of a temperature scale from saturating into one tone.
  g   <- .dpCapturePalette(theme = plotTheme(sequential = c("#000000", "#FFFFFF")))
  lum <- function(x) mean(grDevices::col2rgb(x))
  expect_gt(lum(g[50]), lum("#808080") + 5)                     # a linear ramp would land on mid-grey
  expect_identical(c(g[1], g[100]), c("#000000", "#FFFFFF"))    # endpoints of the theme ramp are kept
})

test_that("color.pal still overrides the theme for this data mapping", {
  pal <- c("#000000", "#444444", "#888888", "#FFFFFF")
  expect_identical(.dpCapturePalette(color.pal = pal), pal)
  # an explicit ramp wins over an explicit theme, too
  expect_identical(.dpCapturePalette(color.pal = pal, theme = plotTheme(sequential = c("#000000", "#FF0000"))), pal)
  expect_error(plotDepthProfiles(.dpTags(), color.pal = 42, plot = FALSE,
                                 plot.file = tempfile(fileext = ".pdf"), verbose = FALSE), "color.pal")
})

test_that("the diel bands stay neutral grey and the key border comes from the theme", {
  # The diel greys are a DELIBERATE partial migration: theme$day/#DCEAF6 and theme$night/#294763 are
  # the same blues the depth ramp uses, so a themed background reads as data and the trace and the
  # black max-depth rule nearly vanish over the night band. Watch what is painted, not the source.
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf))
  fills <- character(0)
  testthat::with_mocked_bindings(
    rect = function(xleft, ybottom, xright, ytop, col = NA, border = NA, ...) {
      fills <<- c(fills, as.character(col)); invisible(NULL)
    },
    .package = "graphics",
    suppressMessages(plotDepthProfiles(.dpTags(), shade.diel = TRUE,
                                       theme = plotTheme(day = "#AA0000", night = "#00AA00"),
                                       plot = FALSE, plot.file = pf, verbose = FALSE)))
  # the theme's diel colours must not reach the canvas at all
  expect_false("#AA0000" %in% fills)
  expect_false("#00AA00" %in% fills)
  # ...and the function that OWNS the decision still returns the neutral triple. Asserting the
  # resolved values, not the source text: swap the greys for theme slots and this fails.
  fl <- .dielFills()
  expect_named(fl, c("day", "crepuscule", "night"))
  expect_equal(unname(fl), c("grey98", "grey92", "grey85"))
  expect_false(any(fl %in% c(plotTheme()$day, plotTheme()$night, plotTheme()$day.border)))
})

