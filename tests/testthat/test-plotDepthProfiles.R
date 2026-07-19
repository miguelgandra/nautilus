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
  expect_equal(nautilus:::.defaultColorLabel("temp"), "Temperature (°C)")
  expect_equal(nautilus:::.defaultColorLabel("speed"), "Speed (m/s)")
  expect_equal(nautilus:::.defaultColorLabel("vedba"), "vedba")
})

test_that("plotDepthProfiles validates arguments before doing any work", {
  d <- data.frame(ID = "a", datetime = Sys.time(), depth = 1)
  expect_error(plotDepthProfiles(d, color.by = 1, verbose = FALSE), "color.by", ignore.case = TRUE)
  expect_error(plotDepthProfiles(d, plot.file = "x.png", verbose = FALSE), "plot.file", ignore.case = TRUE)
  expect_error(plotDepthProfiles(d, cex = -1, verbose = FALSE), "cex", ignore.case = TRUE)
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
