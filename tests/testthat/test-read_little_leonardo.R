# Tests for read_little_leonardo(): the Little Leonardo format reader.
# Fixtures are synthesised here (the real archive is not shipped), reproducing the format's quirks:
# a header block, CRLF line endings, padded columns, a trailing comma and no-leading-zero decimals.
# They are written through `.write_crlf()` (helper-crlf.R) - see the test below for why that matters.

.ll_fixture <- function(dir = tempfile(), n_sec = 5L, hz = 10L, with_dt = TRUE, id = "LLTEST",
                        video = NULL, with_video = TRUE) {
  dir.create(dir, showWarnings = FALSE, recursive = TRUE)
  n <- n_sec * hz
  set.seed(7)
  ax <- round(stats::rnorm(n, 0, 0.2), 2); ay <- round(stats::rnorm(n, 0, 0.2), 2)
  az <- round(stats::rnorm(n, 1, 0.2), 2)
  acc <- c("ACCELERATION DATA ", "", sprintf(" %d msec/point", 1000L / hz),
           "RECORD TIME   0h 1m", "START DATE   0000/00/00", "START TIME    00:00:00", "",
           " X  ,Y   ,Z   ",
           sprintf("%s,%s,%s,", ax, ay, az))              # note the trailing comma
  .write_crlf(acc, file.path(dir, paste0(id, "_A.txt")))
  vid <- if (is.null(video)) rep(0L, n_sec) else as.integer(video)   # default: all-off (no video ancillary)
  if (with_dt) {
    if (with_video) {
      dep <- c("Depth, Temp, and Video Data ", "",        # <- title line: must NOT be read as the header
               "RECORD TIME   0 hours 1 minutes", "START DATE   0000/00/00", "START TIME    00:00:00", "",
               "Depth , Temp,  Video ",
               sprintf("%s  ,%s  ,%d", seq_len(n_sec), 20 + seq_len(n_sec), vid))
    } else {                                               # a 2-column DT: depth/temp only, no video flag
      dep <- c("Depth and Temp Data ", "",
               "RECORD TIME   0 hours 1 minutes", "START DATE   0000/00/00", "START TIME    00:00:00", "",
               "Depth , Temp ",
               sprintf("%s  ,%s", seq_len(n_sec), 20 + seq_len(n_sec)))
    }
    .write_crlf(dep, file.path(dir, paste0(id, "_DT.txt")))
  }
  list(dir = dir, n = n, n_sec = n_sec, hz = hz, ax = ax, ay = ay, az = az, video = vid)
}

.t0 <- function() as.POSIXct("2025-07-21 22:33:00", tz = "UTC")

test_that(".write_crlf writes exactly CRLF, with no platform translation", {
  # A byte-level lock, because this fixture family is only faithful if its line endings are. Writing CRLF
  # via `writeLines(x, <path>, sep = "\r\n")` uses a TEXT-mode connection, and on Windows that translates
  # the "\n" in the separator into "\r\n" - emitting "\r\r\n". Nothing errors: `readLines()` is also
  # text-mode and still reports the right line numbers, but `fread()` memory-maps the file in BINARY, reads
  # "\r\r\n" as two breaks, and so starts parsing four lines early - mid-header. The frame comes back with
  # the header block as data and four extra rows. This test fails on Windows the moment that regresses.
  p <- tempfile()
  .write_crlf(c("a", "b"), p)
  expect_identical(readBin(p, "raw", file.info(p)$size),
                   as.raw(c(0x61, 0x0d, 0x0a, 0x62, 0x0d, 0x0a)))
  expect_identical(readLines(p, warn = FALSE), c("a", "b"))
})

test_that(".llHeaderRate parses the declared 'msec/point' rate", {
  expect_equal(nautilus:::.llHeaderRate(c("ACCELERATION DATA", " 10 msec/point")), 100)
  expect_equal(nautilus:::.llHeaderRate(c("x", " 40 msec/point")), 25)
  expect_true(is.na(nautilus:::.llHeaderRate(c("ACCELERATION DATA", "no rate here"))))
})

test_that(".llSkip anchors on the column header, not a title line that starts the same way", {
  # regression: a depth file's TITLE is "Depth, Temp, and Video Data" -- a loose "^Depth," matched it and
  # silently skipped to the wrong line, shifting every depth/temperature value by the header block.
  lines <- c("Depth, Temp, and Video Data ", "", "RECORD TIME   0 hours 1 minutes",
             "START DATE   0000/00/00", "START TIME    00:00:00", "", "Depth , Temp,  Video ", "0.2,23.2,0")
  expect_equal(nautilus:::.llSkip(lines, "^\\s*Depth\\s*,"), 7L)   # the column header, not line 1
  expect_true(is.na(nautilus:::.llSkip(lines, "^\\s*X\\s*,")))
})

test_that(".llDetect recognises a Little Leonardo folder and rejects others", {
  f <- .ll_fixture()
  expect_true(nautilus:::.llDetect(f$dir))
  empty <- tempfile(); dir.create(empty)
  expect_false(nautilus:::.llDetect(empty))                        # nothing there
  writeLines("a,b,c", file.path(empty, "something.csv"))
  expect_false(nautilus:::.llDetect(empty))                        # a CSV is not a LL export
})

test_that("read_little_leonardo reads accel + depth, synthesising timestamps from start + header rate", {
  f <- .ll_fixture(n_sec = 5L, hz = 10L)
  res <- nautilus:::read_little_leonardo(f$dir, start = .t0(), timezone = "UTC")
  expect_null(res$reason)
  d <- res$data
  expect_equal(nrow(d), f$n)
  expect_true(all(c("datetime", "ax", "ay", "az", "depth", "temp") %in% names(d)))
  expect_equal(d$ax, f$ax); expect_equal(d$az, f$az)               # values, incl. no-leading-zero decimals
  # timestamps: start + (0:(n-1))/hz, interpreted in `timezone`
  expect_equal(as.numeric(d$datetime[1]), as.numeric(.t0()))
  expect_equal(as.numeric(diff(d$datetime[1:2])), 1 / f$hz, tolerance = 1e-4)
  expect_identical(attr(d$datetime, "tzone"), "UTC")
  # the 1 Hz depth stream is expanded onto the accel grid by whole-second index
  expect_equal(d$depth[1], 1); expect_equal(d$depth[f$hz + 1L], 2)
  expect_equal(res$unit_notes, "timestamps synthesised from data_start + 10 Hz (header)")
})

test_that("the per-second video flag is preserved as a transition-encoded ancillary, not a sensor channel", {
  # video ON for seconds 3-4 of a 6-second, 10 Hz record
  f <- .ll_fixture(n_sec = 6L, hz = 10L, video = c(0, 0, 1, 1, 0, 0))
  res <- nautilus:::read_little_leonardo(f$dir, start = .t0(), timezone = "UTC")
  expect_null(res$reason)
  va <- res$ancillary$video
  expect_false(is.null(va))
  expect_identical(va$encoding, "transitions")
  expect_named(va$data, c("datetime", "video"))
  # transitions: off at t0, on at t0+2 s (accel row 21), off at t0+4 s - three rows, states F/T/F
  expect_equal(nrow(va$data), 3L)
  expect_equal(va$data$video, c(FALSE, TRUE, FALSE))
  expect_equal(as.numeric(va$data$datetime[2]) - as.numeric(.t0()), 2)
  # it is coverage metadata, NOT a measurand: it must not leak into the sensor table
  expect_false("video" %in% names(res$data))
})

test_that("no video ancillary is produced when the camera never recorded or the column is absent", {
  # all-off video column -> nothing to preserve
  off <- nautilus:::read_little_leonardo(.ll_fixture(video = c(0, 0, 0, 0, 0))$dir, start = .t0())
  expect_null(off$ancillary$video)
  # a 2-column DT (depth/temp only, no video flag) -> no ancillary, and depth/temp still read
  none <- nautilus:::read_little_leonardo(.ll_fixture(with_video = FALSE)$dir, start = .t0())
  expect_null(none$ancillary$video)
  expect_true(all(c("depth", "temp") %in% names(none$data)))
})

# ---------------------------------------------------------------------------------------------------
# Channels the export declares but this reader does not read.
#
# Little Leonardo sells more sensor suites than the one deployment this reader was written against: the
# model name encodes them ("3M" = tri-axial compass, "P" = propeller swim-speed sensor). The MATLAB
# importer in Cade et al. (2021) reads a compass from columns 4-6 of the SAME acceleration file, and an
# optional Speed column from the depth file, so both layouts demonstrably exist. Reading them would mean
# guessing their units, which the magnetometer calibration and the paddle-speed calibration cannot do
# safely - so the reader names them as unread instead of dropping them silently.
# ---------------------------------------------------------------------------------------------------

# a fixture with arbitrary extra columns in either file
.ll_wide_fixture <- function(dir = tempfile(), accel_header = " X  ,Y   ,Z   ", n_accel = 3L,
                             dt_header = "Depth , Temp,  Video ", n_dt = 3L, n_sec = 5L, hz = 10L) {
  dir.create(dir, showWarnings = FALSE, recursive = TRUE)
  n <- n_sec * hz
  set.seed(11)
  acc_rows <- vapply(seq_len(n), function(i)
    paste0(paste(round(stats::rnorm(n_accel), 2), collapse = ","), ","), character(1))
  .write_crlf(c("ACCELERATION DATA ", "", sprintf(" %d msec/point", 1000L / hz),
                "RECORD TIME   0h 1m", "START DATE   0000/00/00", "START TIME    00:00:00", "",
                accel_header, acc_rows), file.path(dir, "LLW_A.txt"))
  dt_rows <- vapply(seq_len(n_sec), function(i)
    paste(round(c(i, 20 + i, stats::runif(max(0L, n_dt - 2L), 0.5, 2)), 2), collapse = ","), character(1))
  .write_crlf(c("Depth, Temp, and Other Data ", "", "RECORD TIME   0 hours 1 minutes",
                "START DATE   0000/00/00", "START TIME    00:00:00", "", dt_header, dt_rows),
              file.path(dir, "LLW_DT.txt"))
  dir
}

test_that(".llColumnNames counts channels from the header, not from fread's column count", {
  # the acceleration file's trailing comma gives fread a 4th, all-NA column: counting columns would
  # report a phantom extra channel on every healthy file, so the guard must read the header instead
  expect_identical(nautilus:::.llColumnNames(" X  ,Y   ,Z   "), c("X", "Y", "Z"))
  expect_identical(nautilus:::.llColumnNames("Depth , Temp,  Video "), c("Depth", "Temp", "Video"))
  expect_identical(nautilus:::.llColumnNames(character(0)), character(0))
  expect_identical(nautilus:::.llColumnNames(NA_character_), character(0))
})

test_that("a healthy deployment reports nothing unread (the trailing comma must not trip the guard)", {
  r <- nautilus:::read_little_leonardo(.ll_fixture()$dir, start = .t0())
  expect_length(r$unread_columns, 0L)
  r2 <- nautilus:::read_little_leonardo(.ll_fixture(with_video = FALSE)$dir, start = .t0())
  expect_length(r2$unread_columns, 0L)
})

test_that("compass columns in the acceleration file are named as unread, not silently dropped", {
  d <- .ll_wide_fixture(accel_header = " X  ,Y   ,Z   ,MX  ,MY  ,MZ  ", n_accel = 6L)
  r <- nautilus:::read_little_leonardo(d, start = .t0())
  # the channels that ARE read stay correct
  expect_true(all(c("ax", "ay", "az") %in% names(r$data)))
  expect_false(any(c("mx", "my", "mz") %in% names(r$data)))   # not mapped on a positional guess
  # and the ones that are not read are named, per file
  expect_length(r$unread_columns, 1L)
  expect_match(r$unread_columns, "LLW_A\\.txt")
  expect_match(r$unread_columns, "MX, MY, MZ")
})

test_that("a non-video third column in the depth file is not encoded as camera coverage", {
  # the dangerous case a column COUNT cannot catch: Depth,Temp,Speed is exactly as wide as
  # Depth,Temp,Video, so only the header name distinguishes a propeller trace from a camera flag
  d <- .ll_wide_fixture(dt_header = "Depth , Temp,  Speed ", n_dt = 3L)
  r <- nautilus:::read_little_leonardo(d, start = .t0())
  expect_null(r$ancillary$video)                              # no fabricated camera coverage
  expect_true(all(c("depth", "temp") %in% names(r$data)))     # depth/temp still read
  expect_length(r$unread_columns, 1L)
  expect_match(r$unread_columns, "LLW_DT\\.txt: Speed")
})

test_that("extra depth-file columns beyond the third are named as unread", {
  d <- .ll_wide_fixture(dt_header = "Depth , Temp,  Video , Speed ", n_dt = 4L)
  r <- nautilus:::read_little_leonardo(d, start = .t0())
  expect_match(r$unread_columns, "LLW_DT\\.txt: Speed")
  expect_false(grepl("Video", r$unread_columns))              # the video flag IS read, so not listed
})

test_that("both files' unread channels are reported together, one entry each", {
  d <- .ll_wide_fixture(accel_header = " X  ,Y   ,Z   ,MX  ,MY  ,MZ  ", n_accel = 6L,
                        dt_header = "Depth , Temp,  Speed ", n_dt = 3L)
  r <- nautilus:::read_little_leonardo(d, start = .t0())
  expect_length(r$unread_columns, 2L)
  expect_true(any(grepl("_A\\.txt", r$unread_columns)))
  expect_true(any(grepl("_DT\\.txt", r$unread_columns)))
})

test_that("read_little_leonardo refuses to guess a clock, and reports why", {
  f <- .ll_fixture()
  expect_match(nautilus:::read_little_leonardo(f$dir, start = NULL)$reason, "no recording start time")
  expect_match(nautilus:::read_little_leonardo(f$dir, start = NA)$reason, "no recording start time")
  # the contract's failure shape: data NULL, reason set
  r <- nautilus:::read_little_leonardo(f$dir, start = NULL)
  expect_null(r$data); expect_false(r$tz_mismatch); expect_identical(r$temp_status, "none")
})

test_that("read_little_leonardo reports a missing or unreadable acceleration file", {
  empty <- tempfile(); dir.create(empty)
  expect_match(nautilus:::read_little_leonardo(empty, start = .t0())$reason, "no sensor file")
  bad <- tempfile(); dir.create(bad)
  .write_crlf(c("ACCELERATION DATA", " 10 msec/point", "", "1,2,3"), file.path(bad, "X_A.txt"))
  expect_match(nautilus:::read_little_leonardo(bad, start = .t0())$reason, "unrecognised header")
})

test_that("an accel-only Little Leonardo deployment (no _DT.txt) still reads", {
  f <- .ll_fixture(with_dt = FALSE)
  res <- nautilus:::read_little_leonardo(f$dir, start = .t0(), timezone = "UTC")
  expect_null(res$reason)
  expect_setequal(intersect(names(res$data), nautilus:::.sensorChannels()), c("ax", "ay", "az"))
  expect_match(res$assembly$decisions, "no _DT.txt")
})

test_that("the reader's output feeds buildTagData() and the rest of the pipeline", {
  f <- .ll_fixture(n_sec = 40L, hz = 10L)
  res <- nautilus:::read_little_leonardo(f$dir, start = .t0(), timezone = "UTC")
  tag <- buildTagData(res$data, id = "LLTEST", timezone = "UTC", verbose = FALSE)
  expect_s3_class(tag, "nautilus_tag")
  m <- nautilus:::.getMeta(tag)
  expect_identical(m$id, "LLTEST")
  expect_setequal(m$sensors$present, c("ax", "ay", "az", "depth", "temp"))
  expect_equal(m$sensors$sampling_hz_original, 10)
})

# ---- wiring: importTagData(format=) / the tag_format + data_start roles ----------------------------

.ll_deploy <- function(id = "LLTEST", n_sec = 20L, hz = 10L, video = NULL) {
  root <- tempfile(); dir.create(root)
  f <- .ll_fixture(dir = file.path(root, id), n_sec = n_sec, hz = hz, id = id, video = video)
  c(f, list(root = root, folder = file.path(root, id)))
}
.ll_md <- function(id = "LLTEST", data_start = .t0()) {
  data.frame(id = id, tag = "Little Leonardo", type = "LL",
             deploy_date = .t0(), lon = -28.6, lat = 38.6, data_start = data_start,
             stringsAsFactors = FALSE)
}
.ll_cols <- function(...) metadataColumns(id = "id", tag_model = "tag", tag_type = "type",
                                          deploy_datetime = "deploy_date", deploy_lon = "lon",
                                          deploy_lat = "lat", data_start = "data_start", ...)

test_that("importTagData(format = 'little_leonardo') imports a LL deployment end to end", {
  f <- .ll_deploy()
  tags <- importTagData(data.folders = f$folder, format = "little_leonardo",
                        metadata = .ll_md(), columns = .ll_cols(),
                        return.data = TRUE, verbose = "quiet")
  expect_length(tags, 1)
  tag <- tags[["LLTEST"]]
  expect_s3_class(tag, "nautilus_tag")
  expect_equal(nrow(tag), f$n)
  expect_true(all(c("ID", "datetime", "ax", "ay", "az", "depth", "temp") %in% names(tag)))
  m <- nautilus:::.getMeta(tag)
  expect_identical(m$id, "LLTEST")
  expect_identical(m$tag$model, "Little Leonardo")
  expect_setequal(m$sensors$present, c("ax", "ay", "az", "depth", "temp"))
  expect_equal(as.numeric(m$span$first_datetime), as.numeric(.t0()))
  # the audit trail names the operation the user invoked, not the internal assembler
  expect_identical(vapply(m$processing, function(p) p$step, character(1)), "importTagData")
})

test_that("importTagData surfaces unread channels: warning when quiet, tally when verbose", {
  # a deployment whose logger declares a compass and a propeller channel this reader does not read.
  # The import must SUCCEED (the channels that were read are correct) but must not pass silently.
  root <- tempfile(); dir.create(root)
  .ll_wide_fixture(dir = file.path(root, "LLTEST"),
                   accel_header = " X  ,Y   ,Z   ,MX  ,MY  ,MZ  ", n_accel = 6L,
                   dt_header = "Depth , Temp,  Speed ", n_dt = 3L)
  args <- list(data.folders = file.path(root, "LLTEST"), format = "little_leonardo",
               metadata = .ll_md(), columns = .ll_cols(), return.data = TRUE)

  # verbose = 0 ("quiet"): the deferred, catchable base-R warning channel
  w <- testthat::capture_warnings(tags <- do.call(importTagData, c(args, list(verbose = "quiet"))))
  expect_true(any(grepl("Unread channels", w)))
  expect_true(any(grepl("MX, MY, MZ", w)))
  expect_true(any(grepl("Speed", w)))
  # the import still succeeded, with the channels it could read
  expect_s3_class(tags[["LLTEST"]], "nautilus_tag")
  expect_setequal(nautilus:::.getMeta(tags[["LLTEST"]])$sensors$present,
                  c("ax", "ay", "az", "depth", "temp"))

  # verbose >= 1: cli-only, echoed in the SUMMARY tally, and no base-R warning (the documented split)
  out <- testthat::capture_messages(
    expect_warning(do.call(importTagData, c(args, list(verbose = 2))), NA))
  expect_true(any(grepl("unread channels", out, ignore.case = TRUE)))
})

test_that("importTagData carries the LL video flag through to meta$ancillary$video", {
  f <- .ll_deploy(n_sec = 20L, hz = 10L, video = c(rep(0, 5), rep(1, 10), rep(0, 5)))  # on for 10 s
  tag <- importTagData(data.folders = f$folder, format = "little_leonardo",
                       metadata = .ll_md(), columns = .ll_cols(),
                       return.data = TRUE, verbose = "quiet")[["LLTEST"]]
  vid <- nautilus:::.getMeta(tag)$ancillary$video
  expect_false(is.null(vid))
  expect_identical(vid$encoding, "transitions")
  expect_equal(vid$data$video, c(FALSE, TRUE, FALSE))          # off -> on (t0+5s) -> off (t0+15s)
  expect_false("video" %in% names(tag))                        # never a sensor column
})

test_that("the tag_format metadata role dispatches per deployment, overriding the format argument", {
  f <- .ll_deploy()
  md <- .ll_md(); md$fmt <- "little_leonardo"
  # format = "cats" would find nothing here; the tag_format column must win
  tags <- importTagData(data.folders = f$folder, format = "cats",
                        metadata = md, columns = .ll_cols(tag_format = "fmt"),
                        return.data = TRUE, verbose = "quiet")
  expect_s3_class(tags[["LLTEST"]], "nautilus_tag")
  expect_equal(nrow(tags[["LLTEST"]]), f$n)
})

test_that("an unsupported format is rejected, from the argument or the tag_format column", {
  f <- .ll_deploy()
  expect_error(importTagData(data.folders = f$folder, format = "nope",
                             metadata = .ll_md(), columns = .ll_cols(), verbose = "quiet"),
               "Unsupported tag format")
  md <- .ll_md(); md$fmt <- "not_a_reader"
  expect_error(importTagData(data.folders = f$folder, format = "little_leonardo",
                             metadata = md, columns = .ll_cols(tag_format = "fmt"), verbose = "quiet"),
               "Unsupported tag format")
})

test_that("a clock-less logger without data_start fails loudly rather than inventing a time", {
  f <- .ll_deploy()
  # data_start not mapped -> the reader refuses. The files EXIST, so the pre-flight passes and this is a
  # per-deployment failure (reported + collected), not a whole-run abort: the deployment yields no tag.
  w <- testthat::capture_warnings(
    tags <- importTagData(data.folders = f$folder, format = "little_leonardo", metadata = .ll_md(),
                          columns = metadataColumns(id = "id", tag_model = "tag",
                                                    deploy_datetime = "deploy_date",
                                                    deploy_lon = "lon", deploy_lat = "lat"),
                          return.data = TRUE, verbose = "quiet"))
  expect_true(any(grepl("could not be imported|LLTEST", w)))       # the failure is surfaced, not swallowed
  expect_true(length(tags) == 0L || !inherits(tags[[1]], "nautilus_tag"))   # and no tag is invented
})

test_that("the run guard names the format when no folder holds readable data", {
  empty <- tempfile(); dir.create(file.path(empty, "NOPE"), recursive = TRUE)
  expect_error(importTagData(data.folders = file.path(empty, "NOPE"), format = "little_leonardo",
                             metadata = .ll_md("NOPE"), columns = .ll_cols(), verbose = "quiet"),
               "Little Leonardo")
})
