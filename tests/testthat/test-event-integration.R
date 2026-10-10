.ei_tag <- function(id = "A", n = 200) {
  data.frame(ID = id, datetime = as.POSIXct("2023-01-01", tz = "UTC") + (0:(n - 1)),
             depth = 20 + 10 * sin((0:(n - 1)) / 30), temp = 24,
             pseudo_lon = seq(-25.2, -25.1, length.out = n), pseudo_lat = 37)
}
.ei_events <- function(id = "A", start = 50, end = 70, event = "circling") {
  data.frame(ID = id, event = event,
             start = as.POSIXct("2023-01-01", tz = "UTC") + start,
             end = as.POSIXct("2023-01-01", tz = "UTC") + end)
}

test_that("interval union preserves inclusive boundaries, nested windows, ordering and NAs", {
  t <- as.POSIXct("2023-01-01", tz = "UTC") + c(0:10, NA)
  a <- t[c(8, 3, 4)]; b <- t[c(10, 6, 5)]
  reference <- Reduce(`|`, lapply(seq_along(a), function(i) t >= a[i] & t <= b[i]))
  expect_identical(nautilus:::.inAnyInterval(t, a, b), reference)
  expect_identical(nautilus:::.inAnyInterval(t, t[0], t[0]), rep(FALSE, length(t)))
})

test_that("binary-search interval unions agree with direct matching of random windows", {
  set.seed(12)
  origin <- as.POSIXct("2023-01-01", tz = "UTC")
  for (i in 1:20) {
    t <- origin + c(sample(0:100, 100, replace = TRUE), NA)
    a <- origin + sample(0:100, 30, replace = TRUE)
    b <- a + sample(0:50, 30, replace = TRUE)
    reference <- Reduce(`|`, lapply(seq_along(a), function(j) t >= a[j] & t <= b[j]))
    expect_identical(nautilus:::.inAnyInterval(t, a, b), reference)
  }
})

test_that("annotation honours assessment coverage without changing manual defaults", {
  x <- .ei_tag(); events <- .ei_events()
  coverage <- .ei_events(start = 40, end = 100)[, c("ID", "start", "end")]
  y <- annotateData(x, events, assessed.intervals = coverage, verbose = "quiet")[[1]]
  expect_true(all(is.na(y$circling[1:40])))
  expect_equal(y$circling[41:50], rep(0, 10))
  expect_equal(y$circling[51:71], rep(1, 21))
  expect_true(all(is.na(y$circling[102:200])))
  manual <- annotateData(x, events, verbose = "quiet")[[1]]
  expect_false(anyNA(manual$circling))
})

test_that("zero-event detector tables and all-unassessed coverage have explicit labels", {
  events <- .ei_events()[0, ]
  attr(events, "event_types") <- "circling"
  attr(events, "assessed_intervals") <- .ei_events(start = 10, end = 20)[, c("ID", "start", "end")]
  x <- .ei_tag()
  y <- annotateData(x, events, verbose = "quiet")[[1]]
  expect_equal(y$circling[11:21], rep(0, 11))
  expect_true(all(is.na(y$circling[-(11:21)])))
  attr(events, "assessed_intervals") <- attr(events, "assessed_intervals")[0, ]
  expect_true(all(is.na(annotateData(x, events, verbose = "quiet")[[1]]$circling)))
  expect_error(annotateData(x, .ei_events()[0, ], verbose = "quiet"), "empty")
})

test_that("annotation copies tag data and reads file IDs instead of filename suffixes", {
  x <- data.table::as.data.table(.ei_tag())
  before <- serialize(x, NULL)
  annotateData(list(A = x), .ei_events(), verbose = "quiet")
  expect_identical(serialize(x, NULL), before)
  f <- withr::local_tempfile(fileext = "_processed.rds"); saveRDS(x, f)
  out <- annotateData(f, .ei_events(), verbose = "quiet")
  expect_named(out, "A")
  expect_equal(sum(out$A$circling), 21)
  expect_error(annotateData(list(wrong = x), .ei_events(), verbose = "quiet"), "matching its list name")
  x$ID[2] <- "B"
  expect_error(annotateData(list(A = x), .ei_events(), verbose = "quiet"), "one deployment ID")
})

test_that("generic event validation rejects bad clocks and reversed windows", {
  e <- .ei_events(); e$start <- as.character(e$start)
  expect_error(nautilus:::.eventIntervals(e), "POSIXct")
  e <- .ei_events(start = 70, end = 50)
  expect_error(nautilus:::.eventIntervals(e), "end before")
  e <- .ei_events(); e$ID <- NA_character_
  expect_error(nautilus:::.eventIntervals(e), "deployment ID")
})

test_that("depth bands use exact boundaries despite binning and clip at the visible range", {
  seen <- NULL
  testthat::local_mocked_bindings(
    rect = function(xleft, ybottom, xright, ytop, ...) { seen <<- list(a = xleft, b = xright) },
    legend = function(...) invisible(NULL),
    .package = "graphics")
  pf <- withr::local_tempfile(fileext = ".pdf"); grDevices::pdf(pf)
  on.exit(grDevices::dev.off(), add = TRUE)
  graphics::plot(1:2)
  time <- .ei_tag()$datetime[c(1, 31)]
  nautilus:::.drawEventBands(.ei_events(start = 13, end = 70), c(circling = "red"), time, plotTheme(), 1)
  expect_equal(seen$a, as.numeric(time[1]) + 13)
  expect_equal(seen$b, as.numeric(time[2]))
})

test_that("depth panels retain raw time bounds for events in the final downsampling bin", {
  seen <- NULL
  testthat::local_mocked_bindings(.drawDepthPanel = function(dep, ...) { seen <<- dep })
  x <- .ei_tag()
  pf <- withr::local_tempfile(fileext = ".pdf")
  plotDepthProfiles(x, events = .ei_events(start = 191, end = 199), downsample = 30,
                    plot = FALSE, plot.file = pf, verbose = "quiet")
  expect_equal(seen$time.range, range(x$datetime))
  expect_lt(max(seen$data$datetime), max(seen$time.range))
})

test_that("short event paths survive whole-track thinning and remain time matched", {
  x <- .ei_tag(n = 1000)
  ev <- .ei_events(start = 501, end = 505)
  paths <- nautilus:::.eventTrackPaths(x, ev, "datetime", 2)
  expect_true(paths$matched)
  expect_length(paths$paths, 1L)
  d <- paths$paths[[1]]$data
  expect_equal(d$datetime[c(1, nrow(d))], ev$start + c(0, 4))
  expect_equal(d$lon[c(1, nrow(d))], x$pseudo_lon[c(502, 506)])
})

test_that("event paths break across missing coordinates and actual recording gaps", {
  x <- .ei_tag(n = 100)
  x$pseudo_lon[55:60] <- NA
  result <- nautilus:::.eventTrackPaths(x, .ei_events(start = 50, end = 70), "datetime", 5000)
  expect_length(result$paths, 2L)
  expect_lt(max(result$paths[[1]]$data$datetime), min(result$paths[[2]]$data$datetime))
  x <- .ei_tag(n = 100); x$datetime[61:100] <- x$datetime[61:100] + 3600
  result <- nautilus:::.eventTrackPaths(x, .ei_events(start = 50, end = 3700), "datetime", 5000)
  expect_length(result$paths, 2L)
  expect_equal(result$paths[[1]]$data$datetime[nrow(result$paths[[1]]$data)], x$datetime[60])
})

test_that("event mapping does not substitute a nearest position outside the window", {
  result <- nautilus:::.eventTrackPaths(.ei_tag(), .ei_events(start = 500, end = 600), "datetime", 5000)
  expect_false(result$matched)
  expect_length(result$paths, 0L)
})

test_that("both plotting layers render generic events and leave inputs unchanged", {
  x <- data.table::as.data.table(.ei_tag()); before <- serialize(x, NULL)
  events <- rbind(.ei_events(), .ei_events(start = 100, end = 120, event = "feeding"))
  depth.pdf <- withr::local_tempfile(fileext = ".pdf")
  map.pdf <- withr::local_tempfile(fileext = ".pdf")
  expect_no_error(plotDepthProfiles(x, events = events, plot = FALSE, plot.file = depth.pdf, verbose = "quiet"))
  expect_no_error(plotTracks(x, events = events, basemap = "none", max.points = 2,
                            plot = FALSE, plot.file = map.pdf, verbose = "quiet"))
  expect_gt(file.info(depth.pdf)$size, 0)
  expect_gt(file.info(map.pdf)$size, 0)
  expect_identical(serialize(x, NULL), before)
})

test_that("unmatched IDs and unavailable spatial windows warn even in quiet mode", {
  x <- .ei_tag(); pf <- withr::local_tempfile(fileext = ".pdf")
  expect_warning(plotDepthProfiles(x, events = .ei_events(id = "wrong"), plot = FALSE,
                                  plot.file = pf, verbose = "quiet"), "wrong")
  expect_warning(plotTracks(x, events = .ei_events(start = 500, end = 600), basemap = "none",
                           plot = FALSE, plot.file = pf, verbose = "quiet"), "windows without")
})

test_that("cohort event tables and zero-event detector rosters support subset plots quietly", {
  x <- .ei_tag(); pf <- withr::local_tempfile(fileext = ".pdf")
  cohort <- rbind(.ei_events(), .ei_events(id = "B"))
  expect_silent(plotDepthProfiles(x, events = cohort, plot = FALSE, plot.file = pf, verbose = "quiet"))
  expect_silent(plotTracks(x, events = cohort, basemap = "none", plot = FALSE,
                          plot.file = pf, verbose = "quiet"))
  events <- .ei_events(id = "B")
  attr(events, "circling_detection") <- list(deployments = data.frame(ID = c("A", "B")))
  expect_silent(plotDepthProfiles(x, events = events, plot = FALSE, plot.file = pf, verbose = "quiet"))
  expect_silent(plotTracks(x, events = events[0, ], basemap = "none", plot = FALSE,
                          plot.file = pf, verbose = "quiet"))
})

test_that("event annotations are compatible with dive summaries and many-to-many interval joins", {
  x <- .ei_tag()
  x$dive_id <- rep(1:2, each = 100)
  x$dive_phase <- rep(c(rep("descent", 30), rep("bottom", 40), rep("ascent", 30)), 2)
  events <- .ei_events(start = 50, end = 120)
  coverage <- .ei_events(start = 40, end = 160)[, c("ID", "start", "end")]
  labelled <- annotateData(x, events, assessed.intervals = coverage, verbose = "quiet")
  dm <- diveMetrics(labelled, variables = "circling", by.phase = TRUE, verbose = "quiet")
  expect_equal(nrow(dm), 2L)
  expect_equal(dm$circling_mean, c(50 / 60, 21 / 61))
  ev <- data.table::as.data.table(events)[, .(ID, event_start = start, event_end = end)]
  di <- data.table::as.data.table(dm)[, .(ID, dive_id, dive_start = start, dive_end = end)]
  data.table::setkeyv(di, c("ID", "dive_start", "dive_end"))
  links <- data.table::foverlaps(ev, di, by.x = c("ID", "event_start", "event_end"),
                               by.y = c("ID", "dive_start", "dive_end"), type = "any", nomatch = NULL)
  expect_equal(links$dive_id, 1:2)
  expect_equal(as.numeric(difftime(pmin(links$event_end, links$dive_end),
                                  pmax(links$event_start, links$dive_start), units = "secs")), c(49, 20))
})
