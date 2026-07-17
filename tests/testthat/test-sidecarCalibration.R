# Tests for Phase-A sidecar calibration ingestion (parse & store; no application).
# All fixtures are written to the session tempdir.

.diary_txt <- function(depth_offset = "-37.8, -1", asa = "174, 175, 164", crlf = FALSE, utc_offset = "0") {
  ln <- c(
    "[device]", "sn=CC-07-99", "id=TestDevice", paste0("utc_offset=", utc_offset), "",
    "[logging]", "first_entry=01.01.2020 00:00:00 (UTC)", "",
    "[activated sensors]",
    "01_name=Accelerometer", "01_offset=0, 0, 0", "01_factor=1, 1, 1",
    "02_name=Gyroscope", "02_offset=0, 0, 0", "02_factor=1, 1, 1",
    "03_name=Magnetometer", "03_offset=0, 0, 0", "03_factor=1, 1, 1",
    paste0("03_coefficient=ASAX = ", sub(",.*", "", asa),
           ", ASAY = ", sub("^[^,]*,\\s*([^,]*),.*", "\\1", asa),
           ", ASAZ = ", sub(".*,\\s*", "", asa)),
    "08_name=Depth (200bar)", paste0("08_offset=", depth_offset), "08_factor=1",
    "09_name=Temperature (depth)", "09_offset=0", "09_factor=1"
  )
  if (crlf) paste(ln, collapse = "\r\n") else ln
}

test_that(".parseCATSDiaryTxt extracts offsets, factors and ASA", {
  p <- file.path(tempdir(), "cal.txt")
  on.exit(unlink(p), add = TRUE)
  writeLines(.diary_txt(), p)

  cal <- nautilus:::.parseCATSDiaryTxt(p)
  expect_equal(cal$source_type, "cats_diary_txt")
  expect_equal(cal$device$sn, "CC-07-99")
  expect_equal(cal$calibration$depth$offset, c(-37.8, -1))   # variable-length vector
  expect_equal(cal$calibration$accel$factor, c(1, 1, 1))
  expect_equal(cal$calibration$mag$asa, c(174, 175, 164))
  expect_equal(cal$device$utc_offset, 0)                       # recording-zone provenance
})

test_that("a non-zero sidecar utc_offset is parsed", {
  p <- file.path(tempdir(), "cal_tz.txt")
  on.exit(unlink(p), add = TRUE)
  writeLines(.diary_txt(utc_offset = "2"), p)
  expect_equal(nautilus:::.parseCATSDiaryTxt(p)$device$utc_offset, 2)
})

test_that("ASA of 0/0/0 is treated as unset (NA)", {
  p <- file.path(tempdir(), "cal0.txt")
  on.exit(unlink(p), add = TRUE)
  writeLines(.diary_txt(asa = "0, 0, 0"), p)
  cal <- nautilus:::.parseCATSDiaryTxt(p)
  expect_true(all(is.na(cal$calibration$mag$asa)))
})

test_that("CRLF line endings are tolerated", {
  p <- file.path(tempdir(), "calcrlf.txt")
  on.exit(unlink(p), add = TRUE)
  writeBin(charToRaw(.diary_txt(crlf = TRUE)), p)
  cal <- nautilus:::.parseCATSDiaryTxt(p)
  expect_equal(cal$calibration$depth$offset, c(-37.8, -1))
})

test_that("a non-calibration .txt (e.g. WC version.txt) returns NULL", {
  p <- file.path(tempdir(), "version.txt")
  on.exit(unlink(p), add = TRUE)
  writeLines(c("Wildlife Computers", "DAP Processor 3.0", "exported 2020"), p)
  expect_null(nautilus:::.parseCATSDiaryTxt(p))
})

test_that(".parseCATSResumeJson returns metadata only (no calibration)", {
  p <- file.path(tempdir(), "x_resume.json")
  on.exit(unlink(p), add = TRUE)
  writeLines(c('{', '  "06.sampleRate": 20,', '  "07.sensors": { "imu": true }', '}'), p)
  j <- nautilus:::.parseCATSResumeJson(p)
  expect_equal(j$sample_rate, 20)
  expect_null(j$calibration)
})

test_that(".readSidecarCalibration pairs by basename and prefers .txt", {
  d <- file.path(tempdir(), paste0("sc_", as.integer(runif(1, 1, 1e7))))
  dir.create(d, showWarnings = FALSE)
  on.exit(unlink(d, recursive = TRUE), add = TRUE)
  writeLines("dummy", file.path(d, "rec.csv"))
  writeLines(.diary_txt(depth_offset = "4.97"), file.path(d, "rec.txt"))
  cal <- nautilus:::.readSidecarCalibration(file.path(d, "rec.csv"))
  expect_equal(cal$calibration$depth$offset, 4.97)

  # no sidecar -> NULL
  writeLines("dummy", file.path(d, "lonely.csv"))
  expect_null(nautilus:::.readSidecarCalibration(file.path(d, "lonely.csv")))
})

test_that("importTagData attaches the calibration attribute from a sibling .txt", {
  root <- file.path(tempdir(), paste0("imp_", as.integer(runif(1, 1, 1e7))))
  dir.create(file.path(root, "ID_01", "CMD"), recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)

  mp <- data.frame(
    colname = c("dt","ax","ay","az","gx","gy","gz","mx","my","mz","depth","temp"),
    sensor  = c("datetime","ax","ay","az","gx","gy","gz","mx","my","mz","depth","temp"),
    units   = c("UTC","g","g","g","rad/s","rad/s","rad/s","uT","uT","uT","m","C"),
    stringsAsFactors = FALSE)
  meta <- data.frame(ID = "ID_01", tag = "T", type = "MS",
                     deploy_date = as.POSIXct("2020-01-01", tz = "UTC"),
                     deploy_lon = -25, deploy_lat = 38, stringsAsFactors = FALSE)
  n <- 20; t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
  df <- data.frame(dt = format(t0 + 0:(n-1), "%Y-%m-%d %H:%M:%S"))
  for (cc in c("ax","ay","az","gx","gy","gz","mx","my","mz","depth","temp")) df[[cc]] <- runif(n)
  data.table::fwrite(df, file.path(root, "ID_01", "CMD", "rec.csv"))
  writeLines(.diary_txt(depth_offset = "-5"), file.path(root, "ID_01", "CMD", "rec.txt"))

  run <- function(import.calibration) {
    res <- NULL
    invisible(capture.output(suppressWarnings(suppressMessages(
      res <- importTagData(file.path(root, "ID_01"), import.mapping = mp,
                           id.metadata = meta, columns = metadataColumns(deploy_datetime = "deploy_date"),
                           return.data = TRUE, verbose = FALSE,
                           import.calibration = import.calibration)))))
    res[["ID_01"]]
  }

  cal <- nautilus:::.getMeta(run(TRUE))$calibration
  expect_false(is.null(cal))
  expect_equal(cal$calibration$depth$offset, -5)
  # values are stored, not applied: depth column is unchanged (still 0..1 from runif)
  expect_lte(max(run(TRUE)$depth, na.rm = TRUE), 1)

  # opt-out leaves no calibration in metadata
  expect_null(nautilus:::.getMeta(run(FALSE))$calibration)
})

test_that("sidecar sample rate is not persisted in calibration metadata (inferred from data only)", {
  root <- file.path(tempdir(), paste0("sr_", as.integer(runif(1, 1, 1e7))))
  dir.create(file.path(root, "ID_01", "CMD"), recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)

  mp <- data.frame(colname = c("dt","ax","ay","az","depth","temp"),
                   sensor  = c("datetime","ax","ay","az","depth","temp"),
                   units   = c("UTC","g","g","g","m","C"), stringsAsFactors = FALSE)
  meta <- data.frame(ID = "ID_01", tag = "T", type = "MS",
                     deploy_date = as.POSIXct("2020-01-01", tz = "UTC"),
                     deploy_lon = -25, deploy_lat = 38, stringsAsFactors = FALSE)
  n <- 20; t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
  df <- data.frame(dt = format(t0 + 0:(n-1), "%Y-%m-%d %H:%M:%S"))
  for (cc in c("ax","ay","az","depth","temp")) df[[cc]] <- runif(n)
  data.table::fwrite(df, file.path(root, "ID_01", "CMD", "rec.csv"))
  # a camera _resume.json sidecar carries a sampleRate field
  writeLines(c('{', '  "06.sampleRate": 20,', '  "07.sensors": { "imu": true }', '}'),
             file.path(root, "ID_01", "CMD", "rec_resume.json"))

  res <- NULL
  invisible(capture.output(suppressWarnings(suppressMessages(
    res <- importTagData(file.path(root, "ID_01"), import.mapping = mp,
                         id.metadata = meta, columns = metadataColumns(deploy_datetime = "deploy_date"),
                         return.data = TRUE, verbose = FALSE)))))
  cal <- nautilus:::.getMeta(res[["ID_01"]])$calibration
  expect_false(is.null(cal))               # the sidecar was paired and stored
  expect_null(cal$sample_rate)             # but sample rate is NOT persisted via calibration
})

test_that(".wcModel treats a literal 'Unknown' instrument as no model (falls back to WC)", {
  d <- file.path(tempdir(), paste0("wc_", as.integer(runif(1, 1, 1e7))))
  dir.create(d, showWarnings = FALSE)
  on.exit(unlink(d, recursive = TRUE), add = TRUE)

  data.table::fwrite(data.frame(Ptt = 1, Instrument = "Unknown"), file.path(d, "Summary.csv"))
  expect_null(nautilus:::.wcModel(d))                 # -> caller uses the "WC" fallback

  data.table::fwrite(data.frame(Ptt = 1, Instrument = "MK10"), file.path(d, "Summary.csv"))
  expect_equal(nautilus:::.wcModel(d), "MK10")        # a real model is still returned
})

test_that("a sidecar utc_offset disagreeing with `timezone` warns and is recorded in metadata", {
  root <- file.path(tempdir(), paste0("imptz_", as.integer(runif(1, 1, 1e7))))
  dir.create(file.path(root, "ID_01", "CMD"), recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)

  mp <- data.frame(
    colname = c("dt","ax","ay","az","gx","gy","gz","mx","my","mz","depth","temp"),
    sensor  = c("datetime","ax","ay","az","gx","gy","gz","mx","my","mz","depth","temp"),
    units   = c("UTC","g","g","g","rad/s","rad/s","rad/s","uT","uT","uT","m","C"),
    stringsAsFactors = FALSE)
  meta <- data.frame(ID = "ID_01", tag = "T", type = "MS",
                     deploy_date = as.POSIXct("2020-01-01", tz = "UTC"),
                     deploy_lon = -25, deploy_lat = 38, stringsAsFactors = FALSE)
  n <- 20; t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
  df <- data.frame(dt = format(t0 + 0:(n-1), "%Y-%m-%d %H:%M:%S"))
  for (cc in c("ax","ay","az","gx","gy","gz","mx","my","mz","depth","temp")) df[[cc]] <- runif(n)
  data.table::fwrite(df, file.path(root, "ID_01", "CMD", "rec.csv"))
  writeLines(.diary_txt(utc_offset = "2"), file.path(root, "ID_01", "CMD", "rec.txt"))

  run <- function(timezone) {
    res <- NULL
    invisible(capture.output(
      res <- importTagData(file.path(root, "ID_01"), import.mapping = mp,
                           id.metadata = meta, columns = metadataColumns(deploy_datetime = "deploy_date"),
                           return.data = TRUE, verbose = FALSE, timezone = timezone)))
    res[["ID_01"]]
  }

  # default UTC contradicts the sidecar's +2 -> deferred aggregated warning (run() is verbose == 0);
  # offset recorded regardless. At verbose >= 1 this would surface via the cli UI, not a base-R warning.
  expect_warning(out <- run("UTC"), "UTC-offset mismatch")
  expect_equal(nautilus:::.getMeta(out)$sensors$recording_utc_offset, 2)

  # a matching zone (+2) -> no timezone-contradiction warning (other unrelated warnings may occur)
  msgs <- character(0)
  withCallingHandlers(
    suppressMessages(run("Etc/GMT-2")),
    warning = function(w) { msgs <<- c(msgs, conditionMessage(w)); invokeRestart("muffleWarning") })
  expect_false(any(grepl("UTC-offset mismatch", msgs)))

  # the sidecar constants are still CAPTURED as provenance (only their console display was dropped)
  suppressWarnings(out <- run("UTC"))
  cal <- nautilus:::.getMeta(out)$calibration
  expect_false(is.null(cal))
  expect_equal(cal$device$utc_offset, 2)
})

test_that(".reportCalibration confirms the sidecar but does not recite its constant values", {
  # the values (depth zero-offset, ASA) are firmware corrections already baked into the export; nautilus
  # keeps them in meta$calibration for auditing but must not print them each import - that was pure noise
  # no analysis step acts on. Only a one-line provenance confirmation should reach the console.
  cal <- list(source = "20221017-CamCMD134.txt",
              calibration = list(depth = list(offset = 14.7), mag = list(asa = c(180, 181, 169))))
  out <- cli::cli_fmt(nautilus:::.reportCalibration(cal, lvl = 2L))
  expect_true(any(grepl("calibration sidecar", out)))
  expect_true(any(grepl("CamCMD134", out)))                       # names the file (provenance)
  expect_false(any(grepl("depth offset|14\\.7", out)))            # no value dump
  expect_false(any(grepl("ASA|180|181|169", out)))
  expect_length(cli::cli_fmt(nautilus:::.reportCalibration(cal, lvl = 1L)), 0L)  # silent below detailed
})
