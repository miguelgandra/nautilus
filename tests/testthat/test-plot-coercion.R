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

test_that("splitting an aggregated table on a factor id does not invent empty deployments", {
  # split() keeps a factor's UNUSED levels, so an id column subset down from a larger cohort - the usual
  # way an animal gets dropped - yielded a zero-row group per missing level. That phantom was counted in
  # the deployment total, announced in the header, and carried into every downstream summary.
  agg <- rbind(data.frame(ID = "A1", depth = seq_len(200), stringsAsFactors = FALSE),
               data.frame(ID = "B1", depth = seq_len(200), stringsAsFactors = FALSE))
  agg$ID <- factor(agg$ID, levels = c("A1", "B1", "C1"))        # C1 was dropped upstream

  r <- nautilus:::.resolveInput(agg, "ID")
  expect_equal(r$n, 2L)
  expect_setequal(r$ids, c("A1", "B1"))
  expect_true(all(vapply(seq_len(r$n), function(i) nrow(r$get(i)) > 0, logical(1))))

  # a character id column has always behaved this way; the factor path now matches it
  agg_chr <- agg; agg_chr$ID <- as.character(agg_chr$ID)
  expect_equal(nautilus:::.resolveInput(agg_chr, "ID")$n, r$n)

  # a factor with no unused levels is unaffected
  agg2 <- agg; agg2$ID <- factor(as.character(agg2$ID))
  expect_equal(nautilus:::.resolveInput(agg2, "ID")$n, 2L)
})

test_that("plotDistributions rejects a metric nobody carries and a colour that is not a colour", {
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf), add = TRUE)
  n <- 200
  a <- data.frame(ID = "A", datetime = as.POSIXct("2023-01-01", tz = "UTC") + seq_len(n) * 3600,
                  vedba = abs(stats::rnorm(n, .5, .1)), odba = abs(stats::rnorm(n, .3, .1)),
                  stringsAsFactors = FALSE)

  # a mistyped metric used to be drawn as a fully-formed EMPTY panel - title, axes, grid, deployment
  # labels, a 0-1 fallback x range - and reported as successfully plotted
  expect_error(suppressMessages(plotDistributions(list(A = a), metrics = "vedbaa", plot = FALSE,
                                                  plot.file = pf, verbose = FALSE)),
               "No requested metric is present")
  expect_warning(
    s <- suppressMessages(plotDistributions(list(A = a), metrics = c("vedba", "odbaa"), plot = FALSE,
                                            plot.file = pf, verbose = FALSE)),
    "No data for")
  expect_equal(unique(s$metric), "vedba")            # the usable metric is still plotted, alone

  # dropping a metric must not leave order.metric pointing at it
  expect_warning(
    s2 <- suppressMessages(plotDistributions(list(A = a), metrics = c("vedba", "odbaa"),
                                             order.metric = "odbaa", order.by = "median",
                                             plot = FALSE, plot.file = pf, verbose = FALSE)),
    "No data for")
  expect_equal(unique(s2$metric), "vedba")

  # `colors` was type-checked but never value-checked, so a typo reached grDevices mid-render as a bare
  # "invalid color name" naming neither the argument nor the offending entry
  expect_error(suppressMessages(plotDistributions(list(A = a), metrics = c("vedba", "odba"),
                                                  colors = c("steelblue", "notacolour"),
                                                  plot = FALSE, plot.file = pf, verbose = FALSE)),
               "not a colour")
  expect_no_error(suppressMessages(plotDistributions(list(A = a), metrics = c("vedba", "odba"),
                                                     colors = c("steelblue", "#4C72B0"),
                                                     plot = FALSE, plot.file = pf, verbose = FALSE)))
})

test_that("plotTracks colours by a factor channel without erroring", {
  # .asPlotNumeric now covers the colour channel too; this was the one case left uncertain after the
  # coercion sweep, so it is pinned rather than assumed
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf), add = TRUE)
  n <- 200
  d <- data.frame(ID = "A", datetime = as.POSIXct("2023-01-01", tz = "UTC") + seq_len(n) * 3600,
                  pseudo_lon = -25 + seq_len(n) * 0.003, pseudo_lat = 17 + seq_len(n) * 0.001,
                  pseudo_depth = factor(round(abs(stats::rnorm(n, 50, 20)))),
                  speed_dr = abs(stats::rnorm(n, 1, .3)), stringsAsFactors = FALSE)
  expect_no_error(suppressWarnings(suppressMessages(
    plotTracks(list(A = d), color.by = "depth", plot = FALSE, plot.file = pf, verbose = FALSE))))
})
