# Shared coercion contract for user-supplied columns (.asPlotNumeric / .asPlotTime, R/utils-plot.R).
#
# Every plotting function reads columns the USER named, so every one of them met the same two traps:
# as.numeric() on a factor returns level CODES rather than values, and as.numeric() on a Date returns
# DAYS where the caller wanted seconds. Both produced complete, plausible, wrong figures.

test_that(".asPlotNumeric reads factor labels, not level codes", {
  f <- factor(c("100", "200", "300"))
  expect_equal(nautilus:::.asPlotNumeric(f), c(100, 200, 300))
  expect_equal(as.numeric(f), c(1, 2, 3))                       # the trap being avoided
  expect_equal(nautilus:::.asPlotNumeric(c(1.5, 2.5)), c(1.5, 2.5))
  expect_equal(nautilus:::.asPlotNumeric(c("1.5", "x")), c(1.5, NA))   # unparseable -> NA, not an error
  expect_equal(nautilus:::.asPlotNumeric(1:3), c(1, 2, 3))
})

test_that(".asPlotTime returns seconds for every accepted class, NULL otherwise", {
  t0 <- as.POSIXct("2023-01-01", tz = "UTC")
  expect_equal(nautilus:::.asPlotTime(t0), as.numeric(t0))
  expect_equal(nautilus:::.asPlotTime(as.POSIXlt(t0)), as.numeric(t0))
  # a Date counts DAYS: the whole point of the helper
  expect_equal(nautilus:::.asPlotTime(as.Date("2023-01-01")), as.numeric(as.Date("2023-01-01")) * 86400)
  expect_equal(nautilus:::.asPlotTime(100), 100)
  expect_null(nautilus:::.asPlotTime("2023-01-01"))             # character is NOT a time
  expect_null(nautilus:::.asPlotTime(factor("2023-01-01")))
  expect_false(nautilus:::.isPlotTime("2023-01-01"))
})

test_that("a factor metric no longer becomes its level codes in plotDistributions", {
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf), add = TRUE)
  set.seed(2)
  n <- 300
  a <- data.frame(ID = "A", datetime = as.POSIXct("2023-01-01", tz = "UTC") + seq_len(n) * 60,
                  sp = factor(sample(c("100", "200", "300"), n, TRUE)), stringsAsFactors = FALSE)
  s <- suppressWarnings(suppressMessages(
    plotDistributions(list(A = a), metrics = "sp", plot = FALSE, plot.file = pf, verbose = FALSE)))
  expect_gt(s$median[1], 50)                                    # was 2 (a level code), should be ~200
  expect_lte(s$max[1], 300)
})

test_that(".downsampleForPlot keeps a numeric-valued factor column and bins a Date correctly", {
  n <- 300
  # a factor depth used to be dropped by the is.numeric filter, leaving an empty panel reported as plotted
  d <- data.table::data.table(ID = "A", datetime = as.POSIXct("2023-01-01", tz = "UTC") + seq_len(n) * 300,
                              depth = factor(round(seq(100, 300, length.out = n))), temp = 20)
  z <- nautilus:::.downsampleForPlot(d, "ID", "datetime", 5)
  expect_true("depth" %in% names(z))
  expect_gt(max(z$depth, na.rm = TRUE), 250)

  # the no-binning path must honour the same contract
  z0 <- nautilus:::.downsampleForPlot(d, "ID", "datetime", NULL)
  expect_true(is.numeric(z0$depth))

  # a Date column counts DAYS; binning it through as.numeric() collapsed the record into 1970
  d2 <- data.table::data.table(ID = "A", datetime = as.Date("2023-01-01") + seq_len(n),
                               depth = seq(10, 200, length.out = n))
  z2 <- nautilus:::.downsampleForPlot(d2, "ID", "datetime", 86400)
  expect_gt(as.numeric(format(min(z2$datetime), "%Y")), 2000)   # not 1970
  expect_gt(nrow(z2), 100)                                      # not collapsed to a single bin

  # a column that is not a time at all names itself
  d3 <- data.table::copy(d2); d3$datetime <- as.character(d3$datetime)
  expect_error(nautilus:::.downsampleForPlot(d3, "ID", "datetime", 5), "must hold date-times")
})

test_that("a track is ordered chronologically and its coordinates survive as numbers", {
  tt <- seq(as.POSIXct("2024-01-28 00:00", tz = "UTC"), by = "1 hour", length.out = 168)
  # text timestamps sort lexicographically: "01/02" lands before "28/01", silently reordering the path
  d <- data.table::data.table(ID = "A", datetime = format(tt, "%d/%m/%Y %H:%M"),
                              pseudo_lon = -25 + seq_along(tt) * 0.003,
                              pseudo_lat = 17 + seq_along(tt) * 0.001)
  g <- nautilus:::.gatherPseudoTrack(d, "datetime", NULL, 1e5)
  expect_false(is.unsorted(g$lon))                              # monotone input stays monotone

  # factor coordinates used to leave the track unusable
  d2 <- data.table::data.table(ID = "A", datetime = tt,
                               pseudo_lon = factor(-25 + seq_along(tt) * 0.003),
                               pseudo_lat = factor(17 + seq_along(tt) * 0.001))
  g2 <- nautilus:::.gatherPseudoTrack(d2, "datetime", NULL, 1e5)
  expect_true(is.numeric(g2$lon))
  expect_lt(max(g2$lon), -20)                                   # real degrees, not level codes
  expect_gt(min(g2$lat), 15)
})
