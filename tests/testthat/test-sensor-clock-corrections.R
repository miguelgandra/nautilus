# Synthetic exports: exercise the public import boundary without changing research data.
.sensor_clock_fixture <- function(ids = c("PIN_CAM_30", "OTHER"),
                                  time = as.POSIXct("2022-09-04 23:59:58", tz = "UTC") +
                                    c(0, 0.125, 0.5, 1.375, 3, 4.25),
                                  depth = seq_along(time)) {
  root <- tempfile(); dir.create(root)
  folders <- file.path(root, ids)
  for (folder in folders) {
    dir.create(file.path(folder, "CMD"), recursive = TRUE)
    data.table::fwrite(data.frame(
      dt = paste0(format(time, "%Y-%m-%dT%H:%M:%OS3", tz = "UTC"), "Z"),
      ax = seq_along(time) / 10, depth = depth), file.path(folder, "CMD", "sensor.csv"))
    writeLines(c("[device]", "utc_offset=0", "", "[logging]",
                 paste0("first_entry=", format(min(time), "%d.%m.%Y %H:%M:%OS3", tz = "UTC"), " (UTC)"),
                 paste0("last_entry=", format(max(time), "%d.%m.%Y %H:%M:%OS3", tz = "UTC"), " (UTC)")),
               file.path(folder, "CMD", "sensor.txt"))
  }
  list(root = root, folders = folders, time = time,
       metadata = data.frame(ID = ids, tag = "TestTag", deploy_lon = -25, deploy_lat = 37,
         tagging_date = min(time) + 43200 - 3600, recovery_date = max(time) + 43200 + 3600,
         popup_date = max(time) + 43200 + 1800))
}

.sensor_clock_import <- function(fixture, ...) {
  importTagData(fixture$folders, metadata = fixture$metadata,
    columns = metadataColumns(recovery_datetime = "recovery_date", popup_datetime = "popup_date"),
    import.mapping = data.frame(colname = c("dt", "ax", "depth"),
      sensor = c("datetime", "ax", "depth"), units = c("UTC", "g", "m")),
    verbose = FALSE, ...)
}

.sensor_clock_plan <- function(seconds = 43200) {
  data.frame(ID = "PIN_CAM_30", clock_correction_s = seconds,
             clock_correction_source = "field, light and video audit")
}

test_that("sensor corrections preserve samples, source provenance and independent field dates", {
  f <- .sensor_clock_fixture()
  on.exit(unlink(f$root, recursive = TRUE), add = TRUE)
  raw <- .sensor_clock_import(f)
  out_dir <- file.path(f$root, "out"); dir.create(out_dir)
  corrected <- .sensor_clock_import(f, clock.corrections = .sensor_clock_plan(), output.dir = out_dir)
  tag <- corrected$PIN_CAM_30
  before <- getTagMetadata(raw$PIN_CAM_30)
  after <- getTagMetadata(tag)

  expect_identical(as.numeric(tag$datetime), as.numeric(raw$PIN_CAM_30$datetime) + 43200)
  expect_identical(diff(as.numeric(tag$datetime)), diff(as.numeric(raw$PIN_CAM_30$datetime)))
  expect_identical(tag$ax, raw$PIN_CAM_30$ax)
  expect_identical(tag$depth, raw$PIN_CAM_30$depth)
  expect_identical(tag$ID, raw$PIN_CAM_30$ID)
  expect_identical(nrow(tag), nrow(raw$PIN_CAM_30))
  expect_identical(attr(tag$datetime, "tzone"), "UTC")
  expect_identical(after$sensors$timezone, "UTC")
  expect_identical(after$sensors$clock, before$sensors$clock)
  expect_identical(after$sensors$recording_utc_offset, before$sensors$recording_utc_offset)
  expect_identical(after$sidecar, before$sidecar)
  expect_identical(after$deployment, before$deployment)
  expect_identical(after$deployment$datetime, f$metadata$tagging_date[1])
  expect_identical(after$deployment$recovery_datetime, f$metadata$recovery_date[1])
  expect_identical(after$deployment$popup_datetime, f$metadata$popup_date[1])
  expect_identical(after$span$first_datetime, min(tag$datetime))
  expect_identical(after$span$last_datetime, max(tag$datetime))
  expect_identical(after$span$original_rows, before$span$original_rows)

  record <- after$sensors$clock_correction
  expect_equal(record$clock_correction_s, 43200)
  expect_equal(record$clock_correction_source, "field, light and video audit")
  expect_identical(record$original_first, min(raw$PIN_CAM_30$datetime))
  expect_identical(record$original_last, max(raw$PIN_CAM_30$datetime))
  expect_identical(record$corrected_first, min(tag$datetime))
  expect_identical(record$corrected_last, max(tag$datetime))
  expect_length(after$processing, 1L)
  expect_identical(after$processing[[1]]$clock_correction, record)
  expect_equal(processingHistory(tag)$step, "importTagData")
  saved <- readRDS(file.path(out_dir, "PIN_CAM_30.rds"))
  expect_identical(getTagMetadata(saved), after)
  expect_identical(as.numeric(saved$datetime), as.numeric(tag$datetime))
  expect_identical(corrected$OTHER$datetime, raw$OTHER$datetime)
  expect_null(getTagMetadata(corrected$OTHER)$sensors$clock_correction)
})

test_that("omitted, empty and zero plans retain uncorrected data and metadata", {
  f <- .sensor_clock_fixture()
  on.exit(unlink(f$root, recursive = TRUE), add = TRUE)
  stable <- function(tag) {
    meta <- unclass(getTagMetadata(tag))
    meta$processing <- lapply(meta$processing, function(p) { p$time <- NULL; p })
    list(values = lapply(tag, identity), meta = meta)
  }
  raw <- lapply(.sensor_clock_import(f), stable)
  for (plan in list(NULL, .sensor_clock_plan()[FALSE, ], .sensor_clock_plan(0))) {
    got <- .sensor_clock_import(f, clock.corrections = plan)
    expect_identical(lapply(got, stable), raw)
    expect_null(getTagMetadata(got$PIN_CAM_30)$processing[[1]]$clock_correction)
  }
})

test_that("correction plans fail before sensor reading for invalid values or deployment IDs", {
  f <- .sensor_clock_fixture()
  on.exit(unlink(f$root, recursive = TRUE), add = TRUE)
  testthat::local_mocked_bindings(read_cats = function(...) stop("reader reached"), .package = "nautilus")
  bad <- list(
    list(ID = "PIN_CAM_30", clock_correction_s = 1),
    data.frame(ID = "PIN_CAM_30"),
    data.frame(ID = c("PIN_CAM_30", "PIN_CAM_30"), clock_correction_s = 1),
    data.frame(ID = NA_character_, clock_correction_s = 1),
    data.frame(ID = " ", clock_correction_s = 1),
    data.frame(ID = "PIN_CAM_30", clock_correction_s = "43200"),
    data.frame(ID = "PIN_CAM_30", clock_correction_s = Inf),
    data.frame(ID = "PIN_CAM_30", clock_correction_s = NA_real_),
    data.frame(ID = "PIN_CAM_30", clock_correction_s = 1, clock_correction_source = 1),
    data.frame(ID = "TYPO", clock_correction_s = 1))
  messages <- c("data frame", "clock_correction_s", "more than one row", "non-missing", "non-missing",
                "finite numeric", "finite numeric", "finite numeric", "must be character", "do not match")
  for (i in seq_along(bad))
    expect_error(.sensor_clock_import(f, clock.corrections = bad[[i]]), messages[i])
})

test_that("negative and fractional repairs add seconds without reinterpreting display zones", {
  f <- .sensor_clock_fixture(ids = "PIN_CAM_30",
    time = as.POSIXct("2025-03-30 00:30:00", tz = "UTC") + c(0, 3600, 7200))
  on.exit(unlink(f$root, recursive = TRUE), add = TRUE)
  for (seconds in c(-43200, 0.25, 43200.5)) {
    plan <- data.frame(ID = "PIN_CAM_30", clock_correction_s = seconds)
    tag <- .sensor_clock_import(f, timezone = "Europe/Lisbon", import.sidecar = FALSE,
                                clock.corrections = plan)[[1]]
    expect_identical(as.numeric(tag$datetime), as.numeric(f$time) + seconds)
    expect_identical(attr(tag$datetime, "tzone"), "Europe/Lisbon")
    expect_identical(getTagMetadata(tag)$sensors$clock$explicit_offsets, "Z")
    expect_identical(getTagMetadata(tag)$sensors$clock_correction$clock_correction_source, "manual")
  }
})

test_that("re-importing applies a repair once and internal reapplication refuses to mutate data", {
  f <- .sensor_clock_fixture(ids = "PIN_CAM_30")
  on.exit(unlink(f$root, recursive = TRUE), add = TRUE)
  plan <- .sensor_clock_plan()
  paths <- .sensor_clock_import(f, clock.corrections = plan, output.dir = f$root, return.data = FALSE)
  first <- readRDS(paths[1])
  second <- .sensor_clock_import(f, clock.corrections = plan)[[1]]
  expect_identical(as.numeric(second$datetime), as.numeric(first$datetime))
  expect_length(getTagMetadata(second)$processing, 1L)

  res <- list(data = data.table::data.table(datetime = f$time, depth = seq_along(f$time)))
  original <- data.table::copy(res$data)
  corrected <- nautilus:::.applySensorClockCorrection(res, plan)
  expect_identical(res$data$datetime, original$datetime)
  before <- data.table::copy(corrected$data)
  expect_error(nautilus:::.applySensorClockCorrection(corrected, plan), "already applied")
  expect_identical(corrected$data$datetime, before$datetime)
  expect_identical(corrected$data$depth, before$depth)
})

test_that("correction restores overlap before WC alignment and downstream deployment trimming", {
  elapsed <- 0:7200
  time <- as.POSIXct("2022-09-04 00:00:00", tz = "UTC") + elapsed
  depth <- 50 * exp(-((elapsed - 1500) / 200)^2) +
    80 * exp(-((elapsed - 3800) / 300)^2) + 40 * exp(-((elapsed - 6000) / 250)^2)
  f <- .sensor_clock_fixture(ids = "PIN_CAM_30", time = time, depth = depth)
  on.exit(unlink(f$root, recursive = TRUE), add = TRUE)
  wc <- file.path(f$folders[1], "WC"); dir.create(wc)
  wc_time <- time + 43200 + 240  # Primary clock is 12 h slow; WC clock is 4 min fast.
  stamp <- function(x) format(x, "%H:%M:%OS3 %d-%b-%Y", tz = "UTC")
  data.table::fwrite(data.frame(Time = stamp(wc_time), Depth = depth, Dry = as.integer(depth < 1)),
                     file.path(wc, "PIN_CAM_30-Archive.csv"))
  fix <- c(500, 1500, 3000)
  data.table::fwrite(data.frame(Ptt = 1, Date = stamp(wc_time[fix]), Type = "FastGPS",
    Quality = "7", Latitude = 37, Longitude = -25), file.path(wc, "PIN_CAM_30-Locations.csv"))

  raw <- suppressWarnings(.sensor_clock_import(f))[[1]]
  expect_identical(getTagMetadata(raw)$ancillary$alignment$reason_code, "insufficient_overlap")
  corrected <- .sensor_clock_import(f, clock.corrections = .sensor_clock_plan())[[1]]
  meta <- getTagMetadata(corrected)
  expect_identical(meta$ancillary$alignment$status, "aligned")
  expect_lt(abs(meta$ancillary$alignment$offset.seconds + 240), 3)
  expect_identical(as.numeric(corrected$datetime), as.numeric(time) + 43200)
  expect_identical(corrected$depth, raw$depth)
  expect_equal(as.numeric(meta$ancillary$positions$data$datetime), as.numeric(time[fix]) + 43200,
               tolerance = 1e-8)

  disabled <- .sensor_clock_import(f, clock.corrections = .sensor_clock_plan(),
                                   alignment = alignmentControl(method = "none"))[[1]]
  expect_identical(as.numeric(getTagMetadata(disabled)$ancillary$positions$data$datetime),
                   as.numeric(wc_time[fix]))
  expect_identical(getTagMetadata(disabled)$ancillary$alignment$status, "disabled")

  window <- data.frame(ID = "PIN_CAM_30", start = time[1] + 43200 + 1000,
                        end = time[1] + 43200 + 1800)
  trimmed <- filterDeploymentData(list(PIN_CAM_30 = corrected), custom.deployment.times = window,
    min.deployment.hours = 0, plot = FALSE, verbose = FALSE)[[1]]
  keep <- elapsed >= 1000 & elapsed <= 1800
  expect_identical(as.numeric(trimmed$datetime), as.numeric(time[keep]) + 43200)
  expect_identical(trimmed$depth, corrected$depth[keep])
  expect_identical(getTagMetadata(trimmed)$sensors$clock_correction, meta$sensors$clock_correction)
})

test_that("Little Leonardo camera-on flags follow the sensor clock while field dates stay fixed", {
  root <- tempfile(); folder <- file.path(root, "PIN_CAM_30"); dir.create(folder, recursive = TRUE)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  .write_crlf(c("ACCELERATION DATA", "100 msec/point", "START DATE 0000/00/00", "X,Y,Z",
                rep("0,0,1", 40)), file.path(folder, "record_A.txt"))
  .write_crlf(c("Depth, Temp, Video", "1,20,0", "2,21,1", "3,22,1", "4,23,0"),
              file.path(folder, "record_DT.txt"))
  start <- as.POSIXct("2022-09-04 00:00:00", tz = "UTC")
  metadata <- data.frame(ID = "PIN_CAM_30", tag = "Little Leonardo", tagging_date = start + 43200,
                         deploy_lon = -25, deploy_lat = 37, data_start = start)
  run <- function(correction = NULL) importTagData(folder, format = "little_leonardo", metadata = metadata,
    columns = metadataColumns(data_start = "data_start"), clock.corrections = correction, verbose = FALSE)[[1]]
  raw <- run()
  corrected <- run(.sensor_clock_plan())
  before <- getTagMetadata(raw); after <- getTagMetadata(corrected)
  expect_identical(as.numeric(corrected$datetime), as.numeric(raw$datetime) + 43200)
  expect_identical(as.numeric(after$ancillary$video$data$datetime),
                   as.numeric(before$ancillary$video$data$datetime) + 43200)
  expect_identical(after$ancillary$video$data$video, before$ancillary$video$data$video)
  expect_identical(after$deployment, before$deployment)
  expect_identical(after$sensors$clock, before$sensors$clock)
  expect_identical(metadata$data_start, start)
  expect_identical(corrected$depth, raw$depth)
})

test_that("sensor and external camera corrections require separate explicit calls", {
  f <- .sensor_clock_fixture(ids = "PIN_CAM_30")
  on.exit(unlink(f$root, recursive = TRUE), add = TRUE)
  file.create(file.path(f$folders[1], "20220904-235958_CAM.mp4"))
  testthat::local_mocked_bindings(
    .ffprobeBin = function() "unused",
    .analyseVideo = function(video, id, fn_start, ...) {
      data.frame(ID = id, video = basename(video), start = fn_start, end = fn_start + 5,
        duration = 5, frame_rate = 25, file = video, timestamp_source = "filename")
    }, .package = "nautilus")
  video <- function(plan = NULL) getVideoMetadata(f$folders, timestamp.source = "filename",
    clock.corrections = plan, use.parallel = FALSE, verbose = FALSE)
  raw <- .sensor_clock_import(f)[[1]]
  raw_video <- video()
  sensor_only <- .sensor_clock_import(f, clock.corrections = .sensor_clock_plan())[[1]]
  expect_identical(video()$start, raw_video$start)
  expect_identical(getVideoClockCorrections(sensor_only), getVideoClockCorrections(raw))
  corrected_video <- video(.sensor_clock_plan())
  expect_identical(as.numeric(corrected_video$start), as.numeric(raw_video$start) + 43200)
  expect_identical(as.numeric(.sensor_clock_import(f)[[1]]$datetime), as.numeric(raw$datetime))
  expect_equal(as.numeric(sensor_only$datetime[1]) - as.numeric(corrected_video$start),
               as.numeric(raw$datetime[1]) - as.numeric(raw_video$start))
})
