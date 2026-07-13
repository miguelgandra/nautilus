# Tests for trackMetrics() (renamed from calculateTortuosity) + trackMetricsControl().
# Focus: the metric math is preserved (esp. the Bovet & Benhamou 1988 sinuosity), the reconstructTrack ->
# trackMetrics column handoff (pseudo_lon/pseudo_lat auto-detection), and the control/skip/empty plumbing.

.track <- function(lon, lat, id = "A01", lon.name = "lon", lat.name = "lat") {
  n <- length(lon)
  d <- data.table::data.table(ID = id, datetime = as.POSIXct("2020-01-01", tz = "UTC") + seq_len(n) * 3600)
  data.table::set(d, j = lon.name, value = lon)          # set() keeps the selfref valid for later `:=`
  data.table::set(d, j = lat.name, value = lat)
  d[]
}
run_tm <- function(d, metrics = "all", ...) {
  trackMetrics(list(d), control = trackMetricsControl(metrics = metrics), verbose = FALSE, ...)
}

# ---- metric math (preserved from calculateTortuosity) ---------------------------------

test_that("sinuosity is small for a straight track and larger for a zig-zag", {
  straight <- .track(lon = seq(0, 0.2, length.out = 30), lat = rep(0, 30))
  zig      <- .track(lon = seq(0, 0.2, length.out = 30), lat = rep(c(0.01, -0.01), length.out = 30))
  rs <- run_tm(straight, "sinuosity"); rz <- run_tm(zig, "sinuosity")
  expect_true("Sinuosity" %in% names(rs))
  expect_true(is.finite(rs$Sinuosity))
  expect_lt(rs$Sinuosity, rz$Sinuosity)
  expect_gt(rs$Sinuosity, -1e-9)
})

test_that("sinuosity is distinct from path_ratio", {
  r <- run_tm(.track(seq(0, 0.2, length.out = 30), rep(c(0.01, -0.01), length.out = 30)),
              c("path_ratio", "sinuosity"))
  expect_true(all(c("Path_ratio", "Sinuosity") %in% names(r)))
  expect_false(isTRUE(all.equal(r$Path_ratio, r$Sinuosity)))
})

test_that("sinuosity matches the Bovet & Benhamou (1988) formula", {
  d <- .track(seq(0, 0.2, length.out = 30), rep(c(0.01, -0.01), length.out = 30))
  r <- run_tm(d, "sinuosity")
  dist <- nautilus:::.trackDistances(d$lon, d$lat)
  brg  <- nautilus:::.trackBearings(d$lon, d$lat)
  ta   <- nautilus:::.trackTurningAngles(brg) * pi / 180
  step <- dist[is.finite(dist) & dist > 0]
  expected <- round(1.18 * stats::sd(ta[is.finite(ta)]) / sqrt(mean(step)), 3)
  expect_equal(r$Sinuosity, expected, tolerance = 1e-8)
})

test_that("a straight track is maximally straight (straightness ~ 1, path_ratio ~ 1)", {
  r <- run_tm(.track(seq(0, 1, length.out = 40), rep(0, 40)), c("straightness", "path_ratio"))
  expect_equal(r$Straightness, 1, tolerance = 1e-3)
  expect_equal(r$Path_ratio, 1, tolerance = 1e-3)
})

test_that("great-circle distance helper matches a known value", {
  # ~1 degree of latitude is ~111 km
  expect_equal(nautilus:::.trackDistance(0, 0, 0, 1), 111.19, tolerance = 0.1)
})

# ---- reconstructTrack -> trackMetrics column handoff ---------------------------------------

test_that("pseudo_lon/pseudo_lat (reconstructTrack output) are auto-detected by default", {
  d <- .track(seq(0, 0.2, length.out = 30), rep(c(0.01, -0.01), length.out = 30),
              lon.name = "pseudo_lon", lat.name = "pseudo_lat")             # no lon/lat columns
  expect_no_error(res <- run_tm(d, "straightness"))
  expect_equal(nrow(res), 1L)
  expect_true(is.finite(res$Total_distance_km) && res$Total_distance_km > 0)
})

test_that("raw lon/lat are used when there is no pseudo-track, and explicit names override", {
  raw <- .track(seq(0, 0.2, length.out = 20), rep(0, 20))                    # only lon/lat
  expect_no_error(run_tm(raw, "straightness"))
  # a custom column name is honoured
  d <- .track(seq(0, 0.2, length.out = 20), rep(0, 20), lon.name = "x", lat.name = "y")
  expect_no_error(trackMetrics(list(d), lon.col = "x", lat.col = "y", verbose = FALSE))
  # a track with none of the known columns errors clearly
  bad <- data.table::data.table(ID = "A", datetime = as.POSIXct("2020-01-01", tz = "UTC") + 1:10)
  expect_error(trackMetrics(list(bad), verbose = FALSE), "missing required column", ignore.case = TRUE)
})

# ---- plumbing: control, skipping, input shapes ----------------------------------------

test_that("tracks with fewer than control$min.points fixes are skipped", {
  short <- .track(c(0, 0.01, 0.02), c(0, 0, 0))                              # 3 fixes
  # default min.points = 5 -> skipped -> empty result
  expect_equal(nrow(trackMetrics(list(short), verbose = FALSE)), 0L)
  # lower the threshold -> summarised
  res <- trackMetrics(list(short), control = trackMetricsControl(min.points = 3), verbose = FALSE)
  expect_equal(nrow(res), 1L)
})

test_that("a named list whose tables lack an ID column falls back to the list name (no crash)", {
  d <- .track(seq(0, 0.2, length.out = 20), rep(0, 20))
  d[, ID := NULL]                                                            # drop the ID column
  res <- trackMetrics(list(SHARK1 = d), verbose = FALSE)                     # id must come from the name
  expect_equal(nrow(res), 1L)
  expect_equal(res$ID, "SHARK1")
})

test_that(".trackPositionCols resolves the lon/lat pair jointly, never mixing pseudo with raw", {
  both <- .track(0:5 / 10, rep(0, 6)); both[, `:=`(pseudo_lon = lon + 1, pseudo_lat = lat + 1)]
  expect_equal(nautilus:::.trackPositionCols(both, NULL, NULL), list(lon = "pseudo_lon", lat = "pseudo_lat"))
  # only one pseudo axis present -> fall back to the raw PAIR, never pseudo_lon + lat
  partial <- .track(0:5 / 10, rep(0, 6)); partial[, pseudo_lon := lon + 1]
  expect_equal(nautilus:::.trackPositionCols(partial, NULL, NULL), list(lon = "lon", lat = "lat"))
  # explicit names override
  expect_equal(nautilus:::.trackPositionCols(both, "x", "y"), list(lon = "x", lat = "y"))
})

test_that("a single aggregated data.frame is split by id.col into one row per animal", {
  a <- .track(seq(0, 0.2, length.out = 20), rep(0, 20), id = "A")
  b <- .track(seq(0, 0.2, length.out = 20), rep(c(0.01, -0.01), length.out = 20), id = "B")
  res <- trackMetrics(rbind(a, b), verbose = FALSE)
  expect_setequal(res$ID, c("A", "B"))
  expect_equal(nrow(res), 2L)
})

test_that("empty input fails loudly (a mistyped list.files() must not silently yield an empty table)", {
  expect_error(trackMetrics(list(), verbose = FALSE), "empty", ignore.case = TRUE)
  expect_error(trackMetrics(character(0), verbose = FALSE), "empty", ignore.case = TRUE)
  # the typed empty table is still built internally for the no-rows-produced path
  et <- nautilus:::.emptyTrackMetrics()
  expect_s3_class(et, "data.frame"); expect_equal(nrow(et), 0L)
  expect_true(all(c("ID", "Total_distance_km") %in% names(et)))
})

test_that("trackMetricsControl validates its fields", {
  expect_error(trackMetricsControl(metrics = "bogus"), "invalid", ignore.case = TRUE)
  expect_error(trackMetricsControl(min.points = 1), "whole number")
  expect_error(trackMetricsControl(hourly.window.h = 0), "> 0")
  expect_error(trackMetrics(list(.track(0:9 / 50, rep(0, 10))), control = list(metrics = "nope"), verbose = FALSE))
  expect_s3_class(trackMetricsControl(), "nautilus_track_metrics")
})
