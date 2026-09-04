# Tests for sidecar ingestion (device/logging/calibration provenance; no calibration application).
# All fixtures are written to the session tempdir.

.diary_txt <- function(depth_offset = "-37.8, -1", asa = "174, 175, 164", crlf = FALSE,
                       utc_offset = "0", logging_zone = "UTC") {
  ln <- c(
    "[device]", "sn=CC-07-99", "id=TestDevice", paste0("utc_offset=", utc_offset), "",
    "[logging]", paste0("first_entry=01.01.2020 00:00:00 (", logging_zone, ")"),
    paste0("last_entry=01.01.2020 00:00:19 (", logging_zone, ")"), "",
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
  expect_equal(cal$device$utc_offset, 0)
  expect_equal(cal$logging$timezone_label, "UTC")
  expect_equal(cal$logging$utc_offset, 0)
  expect_equal(cal$logging$first_entry, as.POSIXct("2020-01-01 00:00:00", tz = "UTC"))
  expect_equal(cal$logging$last_entry, as.POSIXct("2020-01-01 00:00:19", tz = "UTC"))
})

test_that("a non-zero sidecar utc_offset is parsed", {
  p <- file.path(tempdir(), "cal_tz.txt")
  on.exit(unlink(p), add = TRUE)
  writeLines(.diary_txt(utc_offset = "2"), p)
  expect_equal(nautilus:::.parseCATSDiaryTxt(p)$device$utc_offset, 2)
})

test_that("unknown logging zones are preserved but not interpreted", {
  p <- file.path(tempdir(), "cal_unknown_tz.txt")
  on.exit(unlink(p), add = TRUE)
  writeLines(.diary_txt(logging_zone = "CEST"), p)
  log <- nautilus:::.parseCATSDiaryTxt(p)$logging
  expect_equal(log$timezone_label, "CEST")
  expect_true(is.na(log$utc_offset))
  expect_true(is.na(log$first_entry))
})

test_that("a device/logging sidecar without calibration is still recognised", {
  p <- file.path(tempdir(), "clock_only.txt")
  on.exit(unlink(p), add = TRUE)
  writeLines(c("[device]", "utc_offset=-1", "", "[logging]",
               "first_entry=01.01.2020 00:00:00 (UTC)"), p)
  sidecar <- nautilus:::.parseCATSDiaryTxt(p)
  expect_equal(sidecar$device$utc_offset, -1)
  expect_equal(sidecar$logging$utc_offset, 0)
  expect_null(sidecar$calibration)
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

test_that("a non-CATS .txt (e.g. WC version.txt) returns NULL", {
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

test_that(".readSidecar pairs by basename and prefers .txt", {
  d <- file.path(tempdir(), paste0("sc_", as.integer(runif(1, 1, 1e7))))
  dir.create(d, showWarnings = FALSE)
  on.exit(unlink(d, recursive = TRUE), add = TRUE)
  writeLines("dummy", file.path(d, "rec.csv"))
  writeLines(.diary_txt(depth_offset = "4.97"), file.path(d, "rec.txt"))
  sidecar <- nautilus:::.readSidecar(file.path(d, "rec.csv"))
  expect_equal(sidecar$calibration$depth$offset, 4.97)

  # no sidecar -> NULL
  writeLines("dummy", file.path(d, "lonely.csv"))
  expect_null(nautilus:::.readSidecar(file.path(d, "lonely.csv")))
})

test_that("importTagData retains the complete sibling sidecar", {
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

  run <- function(import.sidecar) {
    res <- NULL
    invisible(capture.output(suppressWarnings(suppressMessages(
      res <- importTagData(file.path(root, "ID_01"), import.mapping = mp,
                           metadata = meta, columns = metadataColumns(deploy_datetime = "deploy_date"),
                           return.data = TRUE, verbose = FALSE,
                           import.sidecar = import.sidecar)))))
    res[["ID_01"]]
  }

  sidecar <- nautilus:::.getMeta(run(TRUE))$sidecar
  expect_false(is.null(sidecar))
  expect_equal(sidecar$calibration$depth$offset, -5)
  expect_equal(sidecar$logging$utc_offset, 0)
  # values are stored, not applied: depth column is unchanged (still 0..1 from runif)
  expect_lte(max(run(TRUE)$depth, na.rm = TRUE), 1)

  # opt-out leaves no sidecar in metadata
  expect_null(nautilus:::.getMeta(run(FALSE))$sidecar)
})

test_that("sidecar sample rate is retained as provenance but not used as sensor metadata", {
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
                         metadata = meta, columns = metadataColumns(deploy_datetime = "deploy_date"),
                         return.data = TRUE, verbose = FALSE)))))
  sidecar <- nautilus:::.getMeta(res[["ID_01"]])$sidecar
  expect_false(is.null(sidecar))            # the sidecar was paired and stored
  expect_equal(sidecar$sample_rate, 20)      # source declaration is retained as provenance
  expect_true(is.na(nautilus:::.getMeta(res[["ID_01"]])$sensors$sampling_hz_original))
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

test_that("device and logging clocks are kept separate and reported through both warning channels", {
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
  writeLines(.diary_txt(utc_offset = "-1"), file.path(root, "ID_01", "CMD", "rec.txt"))

  run <- function(verbose = FALSE, timezone = "UTC") {
    res <- NULL
    output <- cli::cli_fmt(
      res <- importTagData(file.path(root, "ID_01"), import.mapping = mp,
                           metadata = meta, columns = metadataColumns(deploy_datetime = "deploy_date"),
                           return.data = TRUE, verbose = verbose, timezone = timezone))
    list(tag = res[["ID_01"]], output = paste(output, collapse = "\n"))
  }

  # Quiet mode uses the deferred base-R warning channel. It identifies a VIDEO/device issue, not a
  # sensor timezone mismatch, and retains the two offsets under their correct meanings.
  msgs <- character(0)
  quiet <- withCallingHandlers(
    suppressMessages(run(FALSE)),
    warning = function(w) { msgs <<- c(msgs, conditionMessage(w)); invokeRestart("muffleWarning") })
  expect_true(any(grepl("Device clock", msgs, fixed = TRUE)))
  expect_true(any(grepl("require \\+1h correction", msgs)))
  expect_false(any(grepl("Time zone", msgs, fixed = TRUE)))
  stored <- nautilus:::.getMeta(quiet$tag)
  expect_equal(format(quiet$tag$datetime[1], "%Y-%m-%d %H:%M:%S", tz = "UTC"),
               "2020-01-01 00:00:00")
  expect_equal(stored$sensors$recording_utc_offset, 0)
  expect_equal(stored$sidecar$logging$utc_offset, 0)
  expect_equal(stored$sidecar$device$utc_offset, -1)

  # Detailed mode emits the concise deployment-level line and repeats the category in the final Issues
  # block. `cli_fmt()` captures both paths reliably under testthat.
  detailed <- suppressWarnings(run("detailed"))$output
  expect_match(detailed, "\\[logging\\] is UTC but \\[device\\] offset is -1h")
  expect_match(detailed, "Video timestamps may require \\+1h correction")
  expect_match(detailed, "Device clock: 1 deployment")
})

test_that("an explicit logging zone is checked against the requested sensor timezone", {
  mapping <- data.frame(colname_in_csv = "dt", sensor_name_out = "datetime",
                        original_units_map = "UTC", stringsAsFactors = FALSE)
  sidecar <- list(device = list(utc_offset = 0), logging = list(utc_offset = 0))
  status <- nautilus:::.catsClockStatus(
    mapping, sidecar, "Etc/GMT+1", as.POSIXct("2020-01-01", tz = "Etc/GMT+1"))
  expect_true(status$timezone_mismatch)
  expect_match(status$timezone_note, "\\[logging\\] is UTC")
  expect_match(status$timezone_note, 'timezone = "UTC"', fixed = TRUE)
  expect_false(status$device_clock_mismatch)
})

test_that("explicit UTC sensor headers remain authoritative without a sidecar", {
  mapping <- data.frame(colname_in_csv = c("Date (UTC)", "Time (UTC)"),
                        sensor_name_out = c("date", "time"),
                        original_units_map = c("UTC", "UTC"), stringsAsFactors = FALSE)
  status <- nautilus:::.catsClockStatus(
    mapping, NULL, "UTC", as.POSIXct("2020-01-01", tz = "UTC"))
  expect_equal(status$recording_utc_offset, 0)
  expect_false(status$timezone_mismatch)
  expect_false(status$device_clock_mismatch)
})

test_that("clock status without a sidecar has no device/logging diagnostic", {
  mapping <- data.frame(colname_in_csv = "dt", sensor_name_out = "datetime",
                        original_units_map = "UTC", stringsAsFactors = FALSE)
  status <- nautilus:::.catsClockStatus(
    mapping, NULL, "UTC", as.POSIXct("2020-01-01", tz = "UTC"))
  expect_false(status$timezone_mismatch)
  expect_false(status$device_clock_mismatch)
  expect_true(is.na(status$recording_utc_offset))
})

test_that("half-hour device offsets are not treated as matching UTC", {
  mapping <- data.frame(colname_in_csv = "dt", sensor_name_out = "datetime",
                        original_units_map = "UTC", stringsAsFactors = FALSE)
  sidecar <- list(device = list(utc_offset = 0.5), logging = list(utc_offset = 0))
  status <- nautilus:::.catsClockStatus(
    mapping, sidecar, "UTC", as.POSIXct("2020-01-01", tz = "UTC"))

  expect_true(status$device_clock_mismatch)
  expect_match(status$device_clock_note, "device.*0.5h", ignore.case = TRUE)
  expect_match(status$device_clock_note, "-0.5h correction", fixed = TRUE)
})

test_that(".reportSidecar confirms the sidecar but does not recite its constant values", {
  # the values (depth zero-offset, ASA) are firmware corrections already baked into the export; nautilus
  # keeps them in meta$sidecar for auditing but must not print them each import - that was pure noise
  # no analysis step acts on. Only a one-line provenance confirmation should reach the console.
  sidecar <- list(source = "20221017-CamCMD134.txt",
                  calibration = list(depth = list(offset = 14.7), mag = list(asa = c(180, 181, 169))))
  out <- cli::cli_fmt(nautilus:::.reportSidecar(sidecar, lvl = 2L))
  expect_true(any(grepl("sidecar", out)))
  expect_true(any(grepl("CamCMD134", out)))                       # names the file (provenance)
  expect_false(any(grepl("depth offset|14\\.7", out)))            # no value dump
  expect_false(any(grepl("ASA|180|181|169", out)))
  expect_length(cli::cli_fmt(nautilus:::.reportSidecar(sidecar, lvl = 1L)), 0L)  # silent below detailed
})
