# Tests for the single-canonical positions migration: the complete WC position record lives in
# meta$ancillary$positions (never snapped to sensor rows or trimmed to the deployment window), and
# consumers read it via .tagPositions() / the .withPositionColumns() reconstruction shim.

Sys.setlocale("LC_TIME", "C")

.mk_pos_tag <- function(n = 40, with_out_of_window = TRUE) {
  t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
  d  <- data.table::data.table(ID = "A", datetime = t0 + 0:(n - 1), depth = seq(0, 20, length.out = n), temp = 20)
  fixes <- data.frame(datetime = c(t0 + 10, t0 + 20, t0 + 30), type = c("FastGPS", "User", "FastGPS"),
                      lon = c(-25.1, -25.2, -25.3), lat = c(38.1, 38.2, 38.3),
                      quality = c("7", NA, "9"), stringsAsFactors = FALSE)
  if (with_out_of_window)   # a post-deployment surface-drift fix, 2 h after the record ends
    fixes <- rbind(fixes, data.frame(datetime = t0 + 7200, type = "FastGPS", lon = -25.9, lat = 38.9,
                                     quality = "8", stringsAsFactors = FALSE))
  m <- nautilus:::.newNautilusMeta(); m$id <- "A"
  m$deployment$datetime <- t0; m$deployment$lon <- -25.0; m$deployment$lat <- 38.0
  m$ancillary$positions <- list(source = "test", data = fixes)
  nautilus:::new_nautilus_tag(d, m)
}

test_that(".readPositionsAncillary reads tracking fixes and EXCLUDES User (deploy/pop-up) rows", {
  f <- tempfile(fileext = ".csv"); on.exit(unlink(f), add = TRUE)
  loc <- data.frame(Date = c("00:00:10 01-Jan-2020", "00:00:30 01-Jan-2020", "02:00:00 01-Jan-2020"),
                    Type = c("FastGPS", "User", "FastGPS"), Latitude = c(38.1, 38.2, 38.9),
                    Longitude = c(-25.1, -25.2, -25.9), Quality = c("7", "", "9"), stringsAsFactors = FALSE)
  data.table::fwrite(loc, f)
  anc <- nautilus:::.readPositionsAncillary(f)
  expect_equal(nrow(anc$data), 2L)                                  # the 2 FastGPS fixes; the User row dropped
  expect_named(anc$data, c("datetime", "type", "lon", "lat", "quality"))
  expect_true(all(anc$data$type == "FastGPS"))
  expect_false("User" %in% anc$data$type)
  expect_s3_class(anc$data$datetime, "POSIXct")
  expect_null(nautilus:::.readPositionsAncillary(NA))
})

test_that(".readPositionsAncillary returns NULL for a deploy-only (all-User) record", {
  f <- tempfile(fileext = ".csv"); on.exit(unlink(f), add = TRUE)
  loc <- data.frame(Date = c("14:00:00 30-Aug-2023", "09:20:00 31-Aug-2023"),
                    Type = c("User", "User"), Latitude = c(38.0, 38.0),
                    Longitude = c(-29.9, -29.9), Quality = c("", ""), stringsAsFactors = FALSE)
  data.table::fwrite(loc, f)
  expect_null(nautilus:::.readPositionsAncillary(f))                # no tracking fixes -> no positions stream
})

test_that(".tagPositions returns the canonical record (empty when absent)", {
  expect_equal(nrow(nautilus:::.tagPositions(.mk_pos_tag())), 4L)
  bare <- nautilus:::new_nautilus_tag(data.table::data.table(ID = "A", datetime = Sys.time()), nautilus:::.newNautilusMeta())
  expect_equal(nrow(nautilus:::.tagPositions(bare)), 0L)
})

test_that(".withPositionColumns reconstructs in-window fix columns (+ deploy row); safe when columns exist", {
  sh <- nautilus:::.withPositionColumns(.mk_pos_tag())
  n_fix <- sum(!is.na(sh$position_type) & !grepl("Metadata", sh$position_type))
  expect_equal(n_fix, 3L)                                           # the 3 in-window fixes (out-of-window excluded)
  expect_true(any(grepl("deploy", sh$position_type)))              # deploy row from meta$deployment
  # a tag that already carries position columns is returned untouched (no wipe)
  legacy <- data.table::data.table(datetime = Sys.time(), position_type = "FastGPS", lon = 1, lat = 2)
  expect_equal(nautilus:::.withPositionColumns(legacy)$lon, 1)
})

test_that("summarizeTagData counts total in-window fixes from the canonical record", {
  s <- summarizeTagData(list(A = .mk_pos_tag()), verbose = FALSE)
  expect_equal(s$n_positions, 3L)             # 2 FastGPS + 1 User in-window (the 2 h post-record fix excluded)
})

test_that("filterLocations filters bad fixes, writes back ancillary, preserves out-of-window fixes", {
  skip_if_not_installed("geosphere")
  t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
  d  <- data.table::data.table(ID = "A", datetime = t0 + 0:39, depth = seq(0, 20, length.out = 40), temp = 20)
  fixes <- data.frame(datetime = c(t0 + 10, t0 + 20, t0 + 30, t0 + 7200), type = "FastGPS",
                      lon = c(-25.1, -25.11, -20.0, -25.2), lat = c(38.1, 38.11, 38.1, 38.5),  # -20 is ~450 km away
                      quality = c("7", "8", "6", "9"), stringsAsFactors = FALSE)
  m <- nautilus:::.newNautilusMeta(); m$id <- "A"; m$deployment$datetime <- t0; m$deployment$lon <- -25.1; m$deployment$lat <- 38.1
  m$ancillary$positions <- list(source = "test", data = fixes)
  tag <- nautilus:::new_nautilus_tag(d, m)
  fl <- NULL
  invisible(capture.output(suppressWarnings(suppressMessages(
    fl <- filterLocations(list(A = tag), metadata = data.frame(ID = "A", deploy_lon = -25.1, deploy_lat = 38.1),
                          max.distance.km = 100, plot = FALSE)))))
  after <- nautilus:::.tagPositions(fl[["A"]])
  expect_equal(nrow(after), 3L)                                    # far fix dropped, 3 kept
  expect_true(any(after$datetime == t0 + 7200))                    # out-of-window fix preserved
  expect_false(any(round(after$lon, 1) == -20.0))                  # the far fix is gone
  expect_false(any(c("lon", "position_type") %in% names(fl[["A"]])))   # single-canonical: no position columns
})
