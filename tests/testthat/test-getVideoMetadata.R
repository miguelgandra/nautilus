# Tests for getVideoMetadata() and its helpers under the file-name-first design.
#
# The pure, dependency-free surface is unit-tested here: the file-name timestamp parser
# (.videoStartFromFilename), the ocrControl() validator, and getVideoMetadata()'s argument checks (which
# all fire before any ffprobe / ffmpeg / OCR work). The full extraction path (ffprobe duration + frame
# rate, and the OCR fallback) needs real tag videos plus external tools, and is verified separately.

test_that(".videoStartFromFilename parses both camera generations and rejects non-timestamps", {
  f <- nautilus:::.videoStartFromFilename
  # 2019 embedded YYYYMMDD-HHMMSS (full-year form wins over the trailing 6-digit form)
  expect_equal(f("CameraCMD71_Spot06-20190831-173658-819-00023.mp4"),
               as.POSIXct("2019-08-31 17:36:58", tz = "UTC"))
  # 2023 prefix YYMMDD-HHMMSS
  expect_equal(f("230831-161949_CAM0bc99448_30.mp4"),
               as.POSIXct("2023-08-31 16:19:49", tz = "UTC"))
  # a full path is fine (parser uses basename)
  expect_equal(f("/a/b/Camara71-20230715-091500-001.mov"),
               as.POSIXct("2023-07-15 09:15:00", tz = "UTC"))
  # no timestamp -> NULL (e.g. MOBIUS units, or plain names)
  expect_null(f("MOBIUS0000028_processed.mp4"))
  expect_null(f("clip_no_timestamp.mp4"))
  # implausible digit runs are rejected by the 2000-2100 guard, not mistaken for a timestamp
  expect_null(f("weird-999999-999999-run.mp4"))
  expect_null(f("PIN_12345678-000000.mp4"))
  # result is a length-one UTC POSIXct
  ts <- f("230831-161949_x.mp4")
  expect_s3_class(ts, "POSIXct")
  expect_length(ts, 1)
  expect_identical(attr(ts, "tzone"), "UTC")
})

test_that("ocrControl() returns validated defaults and rejects bad fields", {
  d <- ocrControl()
  expect_s3_class(d, "nautilus_ocr")
  expect_equal(d$box, c(3249, 2120, 325, 28))
  expect_equal(d$model, "cam")
  expect_equal(d$frame.height, 2160)
  expect_equal(d$max.search.frames, 10)
  # box must be length-4 with non-negative x/y and positive width/height
  expect_error(ocrControl(box = c(1, 2, 3)), "box", ignore.case = TRUE)
  expect_error(ocrControl(box = c(1, 2, 0, 10)), "box", ignore.case = TRUE)
  expect_error(ocrControl(box = c(-1, 2, 10, 10)), "box", ignore.case = TRUE)
  # scalar fields
  expect_error(ocrControl(model = 123), "model", ignore.case = TRUE)
  expect_error(ocrControl(max.search.frames = 0), "max.search.frames", ignore.case = TRUE)
  expect_error(ocrControl(frame.height = 0), "frame.height", ignore.case = TRUE)
  # a character whitelist is accepted
  expect_s3_class(ocrControl(char.whitelist = "0123456789:- "), "nautilus_ocr")
})

test_that("ocrControl coercion via .as_control accepts a named list and rejects unknown fields", {
  x <- nautilus:::.as_control(list(model = "eng", box = c(1, 1, 10, 10)), ocrControl, "nautilus_ocr", "ocr")
  expect_s3_class(x, "nautilus_ocr")
  expect_equal(x$model, "eng")
  expect_error(nautilus:::.as_control(list(bogus = 1), ocrControl, "nautilus_ocr", "ocr"),
               "unknown", ignore.case = TRUE)
})

test_that("getVideoMetadata() argument validation fires before any OCR / ffprobe work", {
  d <- tempfile(); dir.create(d); on.exit(unlink(d, recursive = TRUE), add = TRUE)
  expect_error(getVideoMetadata(123, verbose = FALSE), "video.folders", ignore.case = TRUE)
  expect_error(getVideoMetadata(character(0), verbose = FALSE), "video.folders", ignore.case = TRUE)
  expect_error(getVideoMetadata(d, video.format = "avi", verbose = FALSE), "video.format", ignore.case = TRUE)
  expect_error(getVideoMetadata(d, timestamp.source = "bogus", verbose = FALSE), "timestamp.source", ignore.case = TRUE)
  expect_error(getVideoMetadata(d, cross.check = "yes", verbose = FALSE), "cross.check", ignore.case = TRUE)
  expect_error(getVideoMetadata(d, ocr = list(bogus = 1), verbose = FALSE), "ocr", ignore.case = TRUE)
})

test_that("getVideoMetadata() reports missing and empty folders clearly", {
  expect_error(getVideoMetadata("/no/such/folder/xyz", verbose = FALSE), "not found", ignore.case = TRUE)
  d <- tempfile(); dir.create(d); on.exit(unlink(d, recursive = TRUE), add = TRUE)
  expect_error(getVideoMetadata(d, verbose = FALSE), "No .*video", ignore.case = TRUE)   # empty folder
})


# Video-clock correction contract ####################################################################

.video_clock_tag <- function(id, device.offset = NA_real_, logging.offset = NA_real_, timezone = "UTC") {
  d <- data.table::data.table(ID = id, datetime = as.POSIXct("2022-09-04 12:30:25", tz = "UTC"),
                              depth = 1)
  meta <- nautilus:::.newNautilusMeta()
  meta$id <- id
  meta$sensors$timezone <- timezone
  meta$sensors$recording_utc_offset <- logging.offset
  meta$sidecar <- list(
    source = paste0(id, ".txt"), source_type = "cats_diary_txt",
    device = list(utc_offset = device.offset),
    logging = list(utc_offset = logging.offset)
  )
  nautilus:::new_nautilus_tag(d, meta)
}

test_that("getVideoClockCorrections() derives only explicit UTC device corrections", {
  tags <- list(
    PIN_CAM_31 = .video_clock_tag("PIN_CAM_31", device.offset = -1, logging.offset = 0),
    PIN_CAM_32 = .video_clock_tag("PIN_CAM_32", device.offset = 0, logging.offset = 0),
    PIN_CAM_33 = .video_clock_tag("PIN_CAM_33", device.offset = 0.5, logging.offset = 0)
  )

  got <- getVideoClockCorrections(tags)
  expect_s3_class(got, "data.frame")
  expect_named(got, c("ID", "clock_correction_s", "clock_correction_source",
                      "device_utc_offset_h", "logging_utc_offset_h"))
  expect_equal(got$ID, c("PIN_CAM_31", "PIN_CAM_33"))
  expect_equal(got$clock_correction_s, c(3600, -1800))
  expect_equal(got$clock_correction_source, rep("cats_sidecar_device_to_utc", 2))
  expect_equal(got$device_utc_offset_h, c(-1, 0.5))
  expect_equal(got$logging_utc_offset_h, c(0, 0))
})

test_that("getVideoClockCorrections() declines ambiguous clock metadata", {
  no_logging <- .video_clock_tag("NO_LOG", device.offset = -1, logging.offset = NA_real_)
  local_logging <- .video_clock_tag("LOCAL_LOG", device.offset = -1, logging.offset = 1)
  wrong_import_zone <- .video_clock_tag("WRONG_ZONE", device.offset = -1, logging.offset = 0,
                                        timezone = "Europe/Lisbon")

  expect_warning(
    got <- getVideoClockCorrections(list(no_logging, local_logging, wrong_import_zone)),
    "could not be derived"
  )
  expect_equal(nrow(got), 0)
  expect_named(got, c("ID", "clock_correction_s", "clock_correction_source",
                      "device_utc_offset_h", "logging_utc_offset_h"))
})

test_that("video-clock correction tables are strict and canonical", {
  validate <- nautilus:::.validateVideoClockCorrections
  manual <- validate(data.frame(ID = "PIN_CAM_31", clock_correction_s = 3600), "PIN_CAM_31")
  expect_equal(manual$clock_correction_source, "manual")

  expect_error(validate(list(ID = "A", clock_correction_s = 1)), "data frame")
  expect_error(validate(data.frame(ID = "A")), "clock_correction_s")
  expect_error(validate(data.frame(ID = c("A", "A"), clock_correction_s = c(1, 2))), "more than one row")
  expect_error(validate(data.frame(ID = "A", clock_correction_s = NA_real_)), "finite numeric")
  expect_error(validate(data.frame(ID = "TYPO", clock_correction_s = 1), "A"), "do not match")
})

test_that("video-clock corrections shift every device-clock timestamp and retain provenance", {
  start <- as.POSIXct("2022-09-04 11:30:25", tz = "UTC") + c(0, 100, 200)
  video <- data.frame(
    ID = c("PIN_CAM_31", "PIN_CAM_31", "PIN_CAM_32"),
    video = c("a.mp4", "b.mp4", "c.mp4"),
    start = start,
    end = start + 10,
    duration = c(10, 10, 10),
    ocr_start = start + 1,
    ocr_offset_s = rep(-1, 3),
    stringsAsFactors = FALSE
  )
  corrections <- nautilus:::.validateVideoClockCorrections(
    data.frame(ID = "PIN_CAM_31", clock_correction_s = 3600,
               clock_correction_source = "manual"),
    unique(video$ID)
  )

  got <- nautilus:::.applyVideoClockCorrections(video, corrections)
  expect_equal(got$start[1:2], video$start[1:2] + 3600)
  expect_equal(got$start[3], video$start[3])
  expect_equal(got$end, got$start + got$duration)
  expect_equal(got$ocr_start[1:2], video$ocr_start[1:2] + 3600)
  expect_equal(got$ocr_offset_s, video$ocr_offset_s)
  expect_equal(got$clock_correction_s, c(3600, 3600, 0))
  expect_equal(got$clock_correction_source, c("manual", "manual", NA_character_))

  expect_error(nautilus:::.applyVideoClockCorrections(got, corrections), "already applied")

  plain <- nautilus:::.applyVideoClockCorrections(
    video, nautilus:::.validateVideoClockCorrections(NULL)
  )
  expect_equal(plain$clock_correction_s, rep(0, 3))
  expect_true(all(is.na(plain$clock_correction_source)))
})

test_that("videos without a timestamp remain uncorrected and are reported", {
  missing <- as.POSIXct(NA_real_, origin = "1970-01-01", tz = "UTC")
  video <- data.frame(ID = "PIN_CAM_31", start = missing, end = missing, duration = 10)
  corrections <- nautilus:::.validateVideoClockCorrections(
    data.frame(ID = "PIN_CAM_31", clock_correction_s = 3600), "PIN_CAM_31")

  expect_warning(
    got <- nautilus:::.applyVideoClockCorrections(video, corrections),
    "without a start timestamp"
  )
  expect_true(is.na(got$start))
  expect_equal(got$clock_correction_s, 0)
  expect_true(is.na(got$clock_correction_source))
})

test_that("getVideoMetadata() applies a correction table after extraction", {
  root <- tempfile(); dir.create(root)
  folder <- file.path(root, "PIN_CAM_31"); dir.create(folder)
  video_path <- file.path(folder, "20220904-113025_CAM.mp4")
  file.create(video_path)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)

  testthat::local_mocked_bindings(
    .ffprobeBin = function() "unused",
    .analyseVideo = function(video, id, fn_start, timestamp.source, cross.check,
                             ocr, ocr_model, whitelist, ocr_engine, ffmpeg_bin, ffprobe_bin) {
      data.frame(ID = id, video = basename(video), start = fn_start, end = fn_start + 10,
                 duration = 10, frame_rate = 25, file = video,
                 timestamp_source = "filename", stringsAsFactors = FALSE)
    },
    .package = "nautilus"
  )

  corrections <- data.frame(ID = "PIN_CAM_31", clock_correction_s = 3600)
  got <- NULL
  output <- cli::cli_fmt(
    got <- getVideoMetadata(folder, timestamp.source = "filename", clock.corrections = corrections,
                            use.parallel = FALSE, verbose = "detailed")
  )
  expect_equal(got$start, as.POSIXct("2022-09-04 12:30:25", tz = "UTC"))
  expect_equal(got$clock_correction_s, 3600)
  expect_equal(got$clock_correction_source, "manual")
  expect_match(paste(output, collapse = "\n"), "clock \\+3600 s")
  expect_match(paste(output, collapse = "\n"), "clock corrected: 1/1 video")
})
