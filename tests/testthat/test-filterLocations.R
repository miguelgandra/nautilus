# Tests for filterLocations() - the location-channel quality-control step.
# Focus: the neighbour-consistency (root) speed test, the satellite/distance checks, the type-exemption
# rules (User/Argos), the canonical-record write-back, and the public API contract.

# ---------------------------------------------------------------------------
# helper: build a nautilus_tag carrying a canonical position record
# ---------------------------------------------------------------------------
make_tag <- function(fixes, id = "shark01", deploy = c(lon = 0, lat = 0), popup = NULL) {
  t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
  span <- range(fixes$datetime)
  sens <- data.table::data.table(ID = id,
                                 datetime = seq(span[1], span[2], by = "1 min"),
                                 depth = 0)
  m <- nautilus:::.newNautilusMeta()
  m$id <- id
  m$deployment$lon <- deploy[["lon"]]; m$deployment$lat <- deploy[["lat"]]
  m$deployment$datetime <- t0
  if (!is.null(popup)) {
    m$deployment$popup_lon <- popup[["lon"]]; m$deployment$popup_lat <- popup[["lat"]]
    m$deployment$popup_datetime <- span[2]
  }
  m$ancillary$positions <- list(source = "test", data = fixes)
  nautilus:::new_nautilus_tag(sens, m)
}

fx <- function(offsets_h, type, lon, lat, quality = NA_character_) {
  t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
  data.frame(datetime = t0 + 3600 * offsets_h, type = type, lon = lon, lat = lat,
             quality = as.character(quality), stringsAsFactors = FALSE)
}

surv <- function(res, id = "shark01") nautilus:::.tagPositions(res[[id]])

# ---------------------------------------------------------------------------
# root speed test
# ---------------------------------------------------------------------------
test_that("the root test removes an isolated spike and keeps the plausible track", {
  f <- fx(0:4, c("User", "FastGPS", "FastGPS", "FastGPS", "FastGPS"),
          lon = c(0, 0.01, 0.02, 5, 0.03), lat = c(0, 0, 0, 5, 0))
  out <- filterLocations(list(shark01 = make_tag(f)), max.speed.kmh = 10, verbose = FALSE)
  s <- surv(out)
  expect_equal(nrow(s), 4L)                       # spike gone
  expect_false(any(s$lon == 5))
})

test_that("a single genuine fast segment is KEPT (cannot be attributed to one fix)", {
  # jump east fast once (B->C ~ 90 km/h), then slow again: C is a plausible new location, not a spike
  f <- fx(0:4, rep("FastGPS", 5), lon = c(0, 0.01, 1, 1.01, 1.02), lat = 0)
  out <- filterLocations(list(shark01 = make_tag(f)), max.speed.kmh = 10, verbose = FALSE)
  s <- surv(out)
  expect_equal(nrow(s), 5L)                       # nothing removed
  expect_true(any(abs(s$lon - 1) < 1e-9))
})

test_that("a bad first fix (endpoint spike) is removed", {
  f <- fx(0:3, rep("FastGPS", 4), lon = c(5, 0, 0.01, 0.02), lat = c(5, 0, 0, 0))
  out <- filterLocations(list(shark01 = make_tag(f)), max.speed.kmh = 10, verbose = FALSE)
  s <- surv(out)
  expect_equal(nrow(s), 3L)
  expect_false(any(s$lon == 5))
})

test_that("User fixes are never removed, even when they imply an overspeed", {
  # two User fixes far apart in space but close in time: an overspeed with no removable culprit
  f <- fx(c(0, 1), c("User", "User"), lon = c(0, 5), lat = c(0, 5))
  expect_warning(
    out <- filterLocations(list(shark01 = make_tag(f)), max.speed.kmh = 10, verbose = FALSE),
    NA)                                            # no error/warning about removal; just keeps them
  expect_equal(nrow(surv(out)), 2L)
})

# ---------------------------------------------------------------------------
# satellite and distance checks
# ---------------------------------------------------------------------------
test_that("the satellite check removes low-count FastGPS but exempts Argos and User", {
  f <- fx(0:3, c("User", "FastGPS", "Argos", "FastGPS"),
          lon = c(0, 0.01, 0.02, 0.03), lat = 0, quality = c(NA, "2", NA, "8"))
  out <- filterLocations(list(shark01 = make_tag(f)), min.satellites = 4, verbose = FALSE)
  s <- surv(out)
  expect_equal(nrow(s), 3L)                       # only the 2-satellite FastGPS removed
  expect_true("Argos" %in% s$type)                # Argos has no sat count -> exempt
  expect_false(any(s$type == "FastGPS" & s$quality == "2"))
})

test_that("the distance check is off by default and exempts User", {
  f <- fx(0:1, c("User", "FastGPS"), lon = c(0, 3), lat = c(0, 3))   # FastGPS ~ 470 km away
  # default: distance check off -> nothing removed by distance (no checks -> expected nudge)
  out0 <- suppressWarnings(filterLocations(list(shark01 = make_tag(f)), verbose = FALSE))
  expect_equal(nrow(surv(out0)), 2L)
  # enabled: the far FastGPS is removed, the User anchor is kept
  out1 <- filterLocations(list(shark01 = make_tag(f)), max.distance.km = 100, verbose = FALSE)
  s <- surv(out1)
  expect_equal(nrow(s), 1L)
  expect_equal(s$type, "User")
})

test_that("spike.angle catches a sharp out-and-back reversal at moderate speed", {
  # an out-and-back at a speed just over the cap that a pure root test also catches; here we assert the
  # control validates and runs without error and removes the reversal
  f <- fx(0:4, rep("FastGPS", 5), lon = c(0, 0.1, 0.5, 0.1, 0.2), lat = c(0, 0, 0.4, 0, 0))
  out <- filterLocations(list(shark01 = make_tag(f)), max.speed.kmh = 20,
                         control = filterLocationsControl(spike.angle = 150), verbose = FALSE)
  expect_lt(nrow(surv(out)), 5L)
})

# ---------------------------------------------------------------------------
# API contract
# ---------------------------------------------------------------------------
test_that("no checks enabled warns and leaves the record unchanged", {
  f <- fx(0:2, rep("FastGPS", 3), lon = c(0, 0.01, 0.02), lat = 0)
  expect_warning(out <- filterLocations(list(shark01 = make_tag(f)), verbose = FALSE),
                 "No location checks")
  expect_equal(nrow(surv(out)), 3L)
})

test_that("a tag with no position fixes is skipped and returned unchanged", {
  t0 <- as.POSIXct("2020-01-01", tz = "UTC")
  sens <- data.table::data.table(ID = "s", datetime = seq(t0, by = "1 min", length.out = 10), depth = 0)
  m <- nautilus:::.newNautilusMeta(); m$id <- "s"
  x <- nautilus:::new_nautilus_tag(sens, m)
  out <- filterLocations(list(s = x), max.speed.kmh = 10, verbose = FALSE)
  expect_equal(nrow(nautilus:::.tagPositions(out[["s"]])), 0L)
})

test_that("the processing history records the run and the thresholds", {
  f <- fx(0:3, rep("FastGPS", 4), lon = c(0, 0.01, 5, 0.02), lat = c(0, 0, 5, 0))
  out <- filterLocations(list(shark01 = make_tag(f)), max.speed.kmh = 10, verbose = FALSE)
  h <- processingHistory(out[["shark01"]])
  expect_true("filterLocations" %in% h$step)
  expect_match(h$details[h$step == "filterLocations"], "removed = 1")
})

test_that("return.data = FALSE returns the written paths; output.dir writes an rds", {
  f <- fx(0:3, rep("FastGPS", 4), lon = c(0, 0.01, 5, 0.02), lat = c(0, 0, 5, 0))
  dir <- withr::local_tempdir()
  res <- filterLocations(list(shark01 = make_tag(f)), max.speed.kmh = 10,
                         return.data = FALSE, output.dir = dir, verbose = FALSE)
  expect_type(res, "character")
  expect_true(all(file.exists(res)))
  expect_true(file.exists(file.path(dir, "shark01.rds")))
  saved <- readRDS(file.path(dir, "shark01.rds"))
  expect_equal(nrow(nautilus:::.tagPositions(saved)), 3L)
})

# ---------------------------------------------------------------------------
# filterLocationsControl validation
# ---------------------------------------------------------------------------
test_that("filterLocationsControl validates its fields", {
  expect_s3_class(filterLocationsControl(), "nautilus_filter_locations")
  expect_error(filterLocationsControl(min.time.mins = -1), "min.time.mins")
  expect_error(filterLocationsControl(max.iterations = 0), "max.iterations")
  expect_error(filterLocationsControl(spike.angle = 45), "spike.angle")
  expect_error(filterLocations(1, control = list(bogus = 1)), "unknown field")
})
