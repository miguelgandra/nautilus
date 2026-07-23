# Tests for robust multi-CSV ingestion (item 1).
# Real folders may hold overlapping fragments (e.g. PIN_CAM_30), genuine
# sequential parts, files from a different device/schema, and clutter
# (Locations.csv, zero-byte files). The importer must assemble them correctly.

.mp <- data.frame(
  colname = c("dt", "ax", "ay", "az", "gx", "gy", "gz", "mx", "my", "mz", "depth", "temp"),
  sensor  = c("datetime", "ax", "ay", "az", "gx", "gy", "gz", "mx", "my", "mz", "depth", "temp"),
  units   = c("UTC", "g", "g", "g", "rad/s", "rad/s", "rad/s", "uT", "uT", "uT", "m", "C"),
  stringsAsFactors = FALSE
)

.md <- function() data.frame(
  ID = "ID_01", tag = "T", type = "MS",
  deploy_date = as.POSIXct("2020-01-01 00:00:00", tz = "UTC"),
  deploy_lon = -25, deploy_lat = 38, stringsAsFactors = FALSE
)

# write a sensor CSV covering second offsets `secs` from a fixed origin
.sensor_csv <- function(path, secs, cols = c("dt","ax","ay","az","gx","gy","gz","mx","my","mz","depth","temp")) {
  t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
  n <- length(secs)
  df <- data.frame(dt = format(t0 + secs, "%Y-%m-%d %H:%M:%S"))
  for (cc in setdiff(cols, "dt")) df[[cc]] <- if (cc == "depth") runif(n, 0, 40) else rnorm(n)
  data.table::fwrite(df[, cols, drop = FALSE], path)
}

.run_import <- function(root) {
  res <- NULL
  invisible(capture.output(suppressWarnings(suppressMessages(
    res <- importTagData(
      data.folders = file.path(root, "ID_01"),
      import.mapping = .mp, metadata = .md(),
      columns = metadataColumns(deploy_datetime = "deploy_date"), return.data = TRUE,
      verbose = FALSE)
  ))))
  res
}

test_that("overlapping fragments are dropped (PIN_CAM_30 scenario)", {
  root <- file.path(tempdir(), paste0("ov_", as.integer(runif(1, 1, 1e7))))
  dir.create(file.path(root, "ID_01", "CMD"), recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)

  .sensor_csv(file.path(root, "ID_01", "CMD", "main.csv"), secs = 0:99)   # primary, 100 rows
  .sensor_csv(file.path(root, "ID_01", "CMD", "frag1.csv"), secs = 5:14)  # inside primary
  .sensor_csv(file.path(root, "ID_01", "CMD", "frag2.csv"), secs = 50:59) # inside primary

  dt <- .run_import(root)[["ID_01"]]
  expect_equal(nrow(dt), 100)                       # fragments contributed nothing
  expect_false(any(duplicated(dt$datetime)))        # no duplicate timestamps
})

test_that("genuine sequential parts are concatenated", {
  root <- file.path(tempdir(), paste0("seq_", as.integer(runif(1, 1, 1e7))))
  dir.create(file.path(root, "ID_01", "CMD"), recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)

  .sensor_csv(file.path(root, "ID_01", "CMD", "part1.csv"), secs = 0:49)
  .sensor_csv(file.path(root, "ID_01", "CMD", "part2.csv"), secs = 50:99)

  dt <- .run_import(root)[["ID_01"]]
  expect_equal(nrow(dt), 100)
  expect_false(any(duplicated(dt$datetime)))
  expect_true(!is.unsorted(dt$datetime))            # assembled output is time-ordered
})

test_that("partial overlap keeps only unique coverage", {
  root <- file.path(tempdir(), paste0("po_", as.integer(runif(1, 1, 1e7))))
  dir.create(file.path(root, "ID_01", "CMD"), recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)

  .sensor_csv(file.path(root, "ID_01", "CMD", "big.csv"), secs = 0:99)    # primary
  .sensor_csv(file.path(root, "ID_01", "CMD", "tail.csv"), secs = 90:119) # overlaps 90:99, extends to 119

  dt <- .run_import(root)[["ID_01"]]
  expect_equal(nrow(dt), 120)                        # 0:99 from big + 100:119 unique tail
  expect_false(any(duplicated(dt$datetime)))
})

test_that("files of a different schema/device are excluded", {
  root <- file.path(tempdir(), paste0("sc_", as.integer(runif(1, 1, 1e7))))
  dir.create(file.path(root, "ID_01", "CMD"), recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)

  .sensor_csv(file.path(root, "ID_01", "CMD", "full.csv"), secs = 0:49)   # complete schema
  # a CSV lacking the gyroscope channels (incomplete schema) must not be merged
  .sensor_csv(file.path(root, "ID_01", "CMD", "partial.csv"), secs = 0:49,
              cols = c("dt", "ax", "ay", "az", "mx", "my", "mz", "depth", "temp"))

  dt <- .run_import(root)[["ID_01"]]
  expect_equal(nrow(dt), 50)                         # only the complete-schema file used
  expect_true(all(c("gx", "gy", "gz") %in% names(dt)))
})

test_that("Locations.csv and zero-byte files are ignored", {
  root <- file.path(tempdir(), paste0("cl_", as.integer(runif(1, 1, 1e7))))
  dir.create(file.path(root, "ID_01", "CMD"), recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)

  .sensor_csv(file.path(root, "ID_01", "CMD", "rec.csv"), secs = 0:49)
  writeLines(c("Ptt,Date,Type,Latitude,Longitude,Quality", "1,x,Argos,38,-25,3"),
             file.path(root, "ID_01", "CMD", "Locations.csv"))
  file.create(file.path(root, "ID_01", "CMD", "empty.csv"))

  dt <- .run_import(root)[["ID_01"]]
  expect_equal(nrow(dt), 50)
})

test_that("corrupt trailing rows are tolerated", {
  root <- file.path(tempdir(), paste0("cr_", as.integer(runif(1, 1, 1e7))))
  dir.create(file.path(root, "ID_01", "CMD"), recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)

  f <- file.path(root, "ID_01", "CMD", "rec.csv")
  .sensor_csv(f, secs = 0:49)
  cat("\n   \n", file = f, append = TRUE)            # blank/garbage trailing lines

  dt <- .run_import(root)[["ID_01"]]
  expect_gte(nrow(dt), 50)
  expect_false(any(is.na(dt$datetime)))
})
