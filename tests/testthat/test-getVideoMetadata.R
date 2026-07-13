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
