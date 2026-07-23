# Tests for the configurable required-sensor set (item 2): the importer should
# accept partial sensor sets (accelerometer-only, TDR depth+temp, etc.) by
# default, and enforce a specific set when `required.sensors` is given.

.units <- c(ax = "g", ay = "g", az = "g", gx = "rad/s", gy = "rad/s", gz = "rad/s",
            mx = "uT", my = "uT", mz = "uT", depth = "m", temp = "C")

# build an ID_01/CMD/rec.csv holding `dt` + the requested channels, plus a matching mapping
.mk <- function(channels) {
  root <- file.path(tempdir(), paste0("rs_", as.integer(runif(1, 1, 1e7))))
  dir.create(file.path(root, "ID_01", "CMD"), recursive = TRUE, showWarnings = FALSE)
  n <- 20; t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
  df <- data.frame(dt = format(t0 + 0:(n - 1), "%Y-%m-%d %H:%M:%S"))
  for (cc in channels) df[[cc]] <- runif(n)
  data.table::fwrite(df, file.path(root, "ID_01", "CMD", "rec.csv"))
  mapping <- data.frame(
    colname = c("dt", channels), sensor = c("datetime", channels),
    units = c("UTC", unname(.units[channels])), stringsAsFactors = FALSE)
  list(root = root, mapping = mapping)
}

.meta <- function() data.frame(
  ID = "ID_01", tag = "T", type = "MS",
  deploy_date = as.POSIXct("2020-01-01 00:00:00", tz = "UTC"),
  deploy_lon = -25, deploy_lat = 38, stringsAsFactors = FALSE)

.run <- function(fx, required.sensors = NULL) {
  res <- NULL
  invisible(capture.output(suppressWarnings(suppressMessages(
    res <- importTagData(file.path(fx$root, "ID_01"), import.mapping = fx$mapping,
 metadata = .meta(),
                         columns = metadataColumns(deploy_datetime = "deploy_date"), required.sensors = required.sensors,
                         return.data = TRUE, verbose = FALSE)))))
  res
}

test_that("accelerometer-only data imports by default (NULL required.sensors)", {
  fx <- .mk(c("ax", "ay", "az"))
  on.exit(unlink(fx$root, recursive = TRUE), add = TRUE)
  dt <- .run(fx)[["ID_01"]]
  expect_false(is.null(dt))
  expect_identical(names(dt)[1:5], c("ID", "datetime", "ax", "ay", "az"))
  expect_false(any(c("gx", "mx", "depth", "temp") %in% names(dt)))  # no phantom columns
})

test_that("TDR-style depth+temp data imports by default", {
  fx <- .mk(c("depth", "temp"))
  on.exit(unlink(fx$root, recursive = TRUE), add = TRUE)
  dt <- .run(fx)[["ID_01"]]
  expect_false(is.null(dt))
  expect_true(all(c("depth", "temp") %in% names(dt)))
  expect_false(any(c("ax", "gx", "mx") %in% names(dt)))
})

test_that("required.sensors enforces presence and rejects when missing", {
  fx <- .mk(c("ax", "ay", "az"))               # no depth
  on.exit(unlink(fx$root, recursive = TRUE), add = TRUE)
  expect_null(.run(fx, required.sensors = "depth")[["ID_01"]])      # dropped
  expect_false(is.null(.run(fx, required.sensors = c("ax", "ay", "az"))[["ID_01"]]))  # satisfied
})

test_that("a fully-specified required IMU set is honoured", {
  fx <- .mk(c("ax", "ay", "az", "gx", "gy", "gz", "mx", "my", "mz", "depth", "temp"))
  on.exit(unlink(fx$root, recursive = TRUE), add = TRUE)
  full <- c("ax", "ay", "az", "gx", "gy", "gz", "mx", "my", "mz")
  expect_false(is.null(.run(fx, required.sensors = full)[["ID_01"]]))
})

test_that("invalid required.sensors raises a clear error", {
  fx <- .mk(c("ax", "ay", "az"))
  on.exit(unlink(fx$root, recursive = TRUE), add = TRUE)
  expect_error(
    suppressWarnings(importTagData(file.path(fx$root, "ID_01"), import.mapping = fx$mapping,
                                   metadata = .meta(), columns = metadataColumns(deploy_datetime = "deploy_date"),
                                   required.sensors = c("ax", "not_a_sensor"),
                                   return.data = TRUE, verbose = FALSE)),
    "required.sensors"
  )
})
