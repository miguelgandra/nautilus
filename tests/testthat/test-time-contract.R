.time_build <- function(time, ...) buildTagData(data.frame(datetime = time, depth = seq_along(time)),
                                              id = "TIME", verbose = FALSE, ...)

.time_import <- function(text, timezone = "UTC", ambiguous = "error") {
  folder <- tempfile(); dir.create(file.path(folder, "TIME", "CMD"), recursive = TRUE)
  on.exit(unlink(folder, recursive = TRUE), add = TRUE)
  data.table::fwrite(data.frame(stamp = text, depth = seq_along(text)),
                     file.path(folder, "TIME", "CMD", "sensor.csv"))
  metadata <- data.frame(ID = "TIME", tag = "TestTag",
    tagging_date = as.POSIXct("2025-01-01", tz = "UTC"), deploy_lon = -25, deploy_lat = 37)
  importTagData(file.path(folder, "TIME"), metadata = metadata, timezone = timezone,
    ambiguous = ambiguous, verbose = FALSE,
    import.mapping = data.frame(colname = c("stamp", "depth"), sensor = c("datetime", "depth"),
                                units = c("UTC", "m")))
}

test_that("POSIXct inputs preserve their epochs independently of source and display zones", {
  for (source in c("Europe/Lisbon", "America/New_York", "Asia/Kathmandu")) {
    time <- as.POSIXct("2025-07-01 12:00:00", tz = source) + (0:3) / 20
    for (display in c("UTC", "Europe/Paris")) {
      tag <- .time_build(time, timezone = display)
      expect_identical(as.numeric(tag$datetime), as.numeric(time))
      expect_identical(attr(tag$datetime, "tzone"), display)
      m <- getTagMetadata(tag)$sensors
      expect_identical(m$timezone, display)
      expect_identical(m$clock$source_timezone, source)
      expect_null(m$clock$reinterpretation)
    }
  }
  # Empty/missing display labels do not make epoch seconds into an uninterpreted local clock.
  time <- as.POSIXct("2025-07-01", tz = "UTC") + 0:2
  for (zone in list(NULL, "")) {
    attr(time, "tzone") <- zone
    expect_identical(as.numeric(.time_build(time)$datetime), as.numeric(time))
  }
})

test_that("supplied timestamps take precedence over all generation arguments", {
  time <- as.POSIXct("2025-07-01", tz = "UTC") + c(0, 0.2, 1.7)
  inputs <- list(list(start = time[1] + 3600), list(sampling.rate = 100),
                 list(start = "unused invalid start", sampling.rate = -1))
  for (args in inputs) {
    expect_warning(tag <- do.call(.time_build, c(list(time = time), args)), "ignored")
    expect_identical(as.numeric(tag$datetime), as.numeric(time))
    expect_false(getTagMetadata(tag)$sensors$clock$generated)
  }
  x <- data.table::data.table(when = time, depth = 1:3)
  before <- data.table::copy(x)
  expect_warning(tag <- buildTagData(x, id = "TIME", datetime.col = "when", start = time[1],
                                     sampling.rate = 100, verbose = FALSE), "ignored")
  expect_identical(as.numeric(tag$datetime), as.numeric(time))
  expect_false("when" %in% names(tag))
  expect_identical(x, before)
  expect_error(buildTagData(data.frame(datetime = "invalid", depth = 1), id = "TIME",
                            start = time[1], sampling.rate = 1, verbose = FALSE), "POSIXct")
})

test_that("fixed-rate synthesis resolves start once and crosses DST using elapsed seconds", {
  starts <- c("2025-03-09 06:59:59", "2025-11-02 05:59:59")
  for (text in starts) {
    start <- lubridate::with_tz(as.POSIXct(text, tz = "UTC"), "America/New_York")
    for (display in c("UTC", "America/New_York")) {
      tag <- buildTagData(data.frame(depth = 1:8), id = "TIME", start = start,
                          sampling.rate = 2, timezone = display, verbose = FALSE)
      expect_equal(as.numeric(tag$datetime), as.numeric(start) + (0:7) / 2)
      expect_identical(tag$depth, 1:8)
      expect_equal(diff(as.numeric(tag$datetime)), rep(0.5, 7))
      expect_true(getTagMetadata(tag)$sensors$clock$generated)
    }
    # Explicit legacy reinterpretation affects the start, never each generated sample separately.
    tag <- buildTagData(data.frame(depth = 1:8), id = "TIME", start = start,
      sampling.rate = 2, timezone = "UTC", time.mode = "clock", verbose = FALSE)
    expect_equal(diff(as.numeric(tag$datetime)), rep(0.5, 7))
    expect_identical(tag$depth, 1:8)
  }
})

test_that("explicit clock reinterpretation records its source and changes only the output", {
  time <- as.POSIXct("2025-07-01 12:00:00", tz = "Europe/Lisbon") + 0:2
  tag <- .time_build(time, time.mode = "clock")
  expect_equal(as.numeric(tag$datetime) - as.numeric(time), rep(3600, 3))
  clock <- getTagMetadata(tag)$sensors$clock
  expect_identical(clock$source_timezone, "UTC")
  expect_identical(clock$input_timezone, "Europe/Lisbon")
  expect_equal(clock$reinterpretation$min_shift_seconds, 3600)
  expect_equal(clock$reinterpretation$max_shift_seconds, 3600)
  expect_equal(tail(getTagMetadata(tag)$processing, 1)[[1]]$clock, clock)
  expect_identical(attr(time, "tzone"), "Europe/Lisbon")
  attr(time, "tzone") <- NULL
  expect_error(.time_build(time, time.mode = "clock"), "declared input tzone")
})

test_that("civil DST gaps reject and repeated times require an explicit policy", {
  parse <- nautilus:::.parseTagClock
  for (policy in c("error", "earliest", "latest"))
    expect_error(parse("2025-03-09 02:30:00", "America/New_York", policy), "nonexistent")
  expect_error(parse("2025-11-02 01:30:00", "America/New_York"), "ambiguous")
  first <- parse("2025-11-02 01:30:00", "America/New_York", "earliest")$datetime
  last <- parse("2025-11-02 01:30:00", "America/New_York", "latest")$datetime
  expect_equal(as.numeric(last) - as.numeric(first), 3600)
  expect_equal(as.numeric(first), as.numeric(as.POSIXct("2025-11-02 05:30:00", tz = "UTC")))
  # POSIXct itself is unambiguous even if displayed in the repeated hour.
  expect_equal(as.numeric(.time_build(lubridate::with_tz(c(first, last), "America/New_York"))$datetime),
               as.numeric(c(first, last)))
  wall <- as.POSIXct("2025-11-02 01:30:00", tz = "UTC")
  expect_error(.time_build(wall, timezone = "America/New_York", time.mode = "clock"), "ambiguous")
  tag <- .time_build(wall, timezone = "America/New_York", time.mode = "clock", ambiguous = "latest")
  expect_equal(as.numeric(tag$datetime), as.numeric(last))
})

test_that("raw timestamp offsets identify instants and fixed logger offsets never observe DST", {
  parse <- nautilus:::.parseTagClock
  text <- c("2025-11-02T01:30:00-04:00", "2025-11-02T01:30:00-0500")
  out <- parse(text, "America/New_York")
  expect_equal(diff(as.numeric(out$datetime)), 3600)
  expect_equal(out$clock$explicit_offsets, c("-04:00", "-0500"))
  expect_true(is.na(out$clock$source_timezone))
  expect_equal(as.numeric(parse("2025-07-01T12:00+01:00")$datetime),
               as.numeric(as.POSIXct("2025-07-01 11:00:00", tz = "UTC")))
  out <- parse(c("2025-07-01T12:00:00.125Z", "2025-07-01T13:00:00.125+01:00"))
  expect_identical(as.numeric(out$datetime[1]), as.numeric(out$datetime[2]))
  expect_equal(as.numeric(out$datetime[1]), as.numeric(as.POSIXct("2025-07-01 12:00:00", tz = "UTC")) + 0.125)
  for (month in c("01", "07")) {
    out <- parse(paste0("2025-", month, "-01 12:00:00"), "+05:30")
    expect_identical(format(out$datetime, "%H:%M:%S", tz = "UTC"), "06:30:00")
    expect_identical(out$clock$source_timezone, "+05:30")
  }
  time <- as.POSIXct("2025-07-01 12:00:00", tz = "UTC")
  expect_identical(as.numeric(.time_build(time, timezone = "+05:30")$datetime), as.numeric(time))
  expect_equal(as.numeric(.time_build(time, timezone = "+05:30", time.mode = "clock")$datetime),
               as.numeric(time) - 5.5 * 3600)
  expect_identical(getTagMetadata(.time_build(time, timezone = "+05:30"))$sensors$clock$requested_timezone,
                   "+05:30")
  attr(time, "tzone") <- "+01:00"
  expect_equal(as.numeric(.time_build(time, time.mode = "clock")$datetime), as.numeric(time) + 3600)
})

test_that("invalid timestamps and zones cannot create apparently valid tag objects", {
  for (bad in c(NA_real_, NaN, Inf, -Inf)) {
    expect_error(.time_build(.POSIXct(c(0, bad), "UTC")), "finite POSIXct")
    expect_error(buildTagData(data.frame(depth = 1), id = "TIME", start = .POSIXct(bad, "UTC"),
                              sampling.rate = 1, verbose = FALSE), "finite POSIXct")
  }
  for (zone in c("", "not_a_zone", "+25:00", "+05:99"))
    expect_error(.time_build(as.POSIXct("2025-01-01", tz = "UTC"), timezone = zone))
  for (text in c("2025-02-30 12:00:00", "2025-01-01 24:01:00", "2025-07-01T12:00:00garbage",
                 "2025-07-01 12:00:00 UTC junk", "2025-01-01 12:00:00+25:00"))
    expect_error(nautilus:::.parseTagClock(text))
})

test_that("date/time parsing preserves sample instants without an epsilon", {
  x <- data.table::data.table(date = c("01.07.2025", "01.07.2025"), time = c("12:00:00", "12:00:00.25"))
  out <- nautilus:::.addDatetime(x, data.frame(sensor_name_out = c("date", "time")), "Europe/Lisbon")
  base <- as.POSIXct("2025-07-01 11:00:00", tz = "UTC")
  expect_identical(as.numeric(out$datetime), as.numeric(base) + c(0, 0.25))
  time <- lubridate::with_tz(base + c(0, 0.25), "Europe/Lisbon")
  x <- data.table::data.table(datetime = time)
  out <- nautilus:::.addDatetime(x, data.frame(sensor_name_out = "datetime"), "UTC")
  expect_identical(as.numeric(out$datetime), as.numeric(time))
})

test_that("public imports preserve ISO offsets, source-clock metadata and fixed-offset clocks", {
  text <- c("2025-07-01T12:00:00+01:00", "2025-07-01T12:00:01+01:00")
  tag <- .time_import(text)[[1]]
  expected <- as.POSIXct("2025-07-01 11:00:00", tz = "UTC") + 0:1
  expect_equal(nrow(tag), 2L)
  expect_identical(as.numeric(tag$datetime), as.numeric(expected))
  clock <- getTagMetadata(tag)$sensors$clock
  expect_equal(clock$explicit_offsets, "+01:00")
  expect_length(clock$sources, 1L)
  expect_true(is.na(clock$source_timezone))
  tag <- .time_import(c("2025-07-01 12:00:00", "2025-07-01 12:00:01"), "+01:00")[[1]]
  expect_identical(as.numeric(tag$datetime), as.numeric(expected))
  expect_identical(getTagMetadata(tag)$sensors$timezone, "UTC")
  expect_identical(getTagMetadata(tag)$sensors$clock$source_timezone, "+01:00")
  # Legacy objects remain readable without inventing a clock interpretation or moving their times.
  data.table::setattr(tag, "nautilus", NULL)
  before <- as.numeric(tag$datetime)
  expect_null(getTagMetadata(tag)$sensors$clock)
  expect_identical(as.numeric(tag$datetime), before)
})

test_that("public import rejects unresolved or partial clock readings and records the failure", {
  for (text in c("2025-03-09 02:30:00", "2025-11-02 01:30:00", "2025-07-01T12:00:00junk")) {
    out <- suppressWarnings(.time_import(text, "America/New_York"))
    expect_length(out, 0L)
    reasons <- attr(out, "nautilus.exclusions")$reason
    expect_match(reasons, "invalid timestamps")
  }
  first <- .time_import("2025-11-02 01:30:00", "America/New_York", "earliest")[[1]]
  last <- .time_import("2025-11-02 01:30:00", "America/New_York", "latest")[[1]]
  expect_equal(as.numeric(last$datetime) - as.numeric(first$datetime), 3600)
})

test_that("Little Leonardo public import preserves absolute recording starts and source provenance", {
  folder <- tempfile(); dir.create(file.path(folder, "TIME"), recursive = TRUE)
  on.exit(unlink(folder, recursive = TRUE), add = TRUE)
  writeLines(c("ACCELERATION DATA", "", "100 msec/point", "START DATE 0000/00/00", "X,Y,Z",
               rep("0,0,1", 4)), file.path(folder, "TIME", "TIME_A.txt"))
  start <- as.POSIXct("2025-07-01 12:00:00", tz = "Europe/Lisbon")
  metadata <- data.frame(ID = "TIME", tag = "Little Leonardo", tagging_date = start,
                         deploy_lon = -25, deploy_lat = 37, data_start = start)
  tag <- importTagData(file.path(folder, "TIME"), format = "little_leonardo", metadata = metadata,
    columns = metadataColumns(data_start = "data_start"), verbose = FALSE)[[1]]
  expect_equal(as.numeric(tag$datetime), as.numeric(start) + (0:3) / 10)
  expect_identical(getTagMetadata(tag)$sensors$clock$source_timezone, "Europe/Lisbon")
  expect_true(getTagMetadata(tag)$sensors$clock$generated)
  expect_equal(as.numeric(getTagMetadata(tag)$deployment$datetime), as.numeric(start))
})

test_that("UTC display alone cannot qualify a non-UTC source for automatic video correction", {
  tag <- .time_import(c("2025-07-01 12:00:00", "2025-07-01 12:00:01"), "+01:00")[[1]]
  meta <- attr(tag, "nautilus")
  meta$sidecar <- list(source_type = "cats_diary_txt", device = list(utc_offset = -1),
                       logging = list(utc_offset = 0))
  nautilus:::.setMeta(tag, meta)
  expect_warning(out <- getVideoClockCorrections(tag), "could not be derived")
  expect_equal(nrow(out), 0L)
  # Conversely, a different display zone cannot disqualify explicitly UTC raw observations.
  tag <- .time_import("2025-07-01T12:00:00Z", "America/New_York")[[1]]
  new_meta <- attr(tag, "nautilus"); new_meta$sidecar <- meta$sidecar
  nautilus:::.setMeta(tag, new_meta)
  expect_equal(getVideoClockCorrections(tag)$clock_correction_s, 3600)
})

test_that("source-zone offsets do not contain artificial fractional-second drift", {
  ref <- as.POSIXct("2025-07-01 12:00:00", tz = "UTC") + 0.125
  expect_identical(nautilus:::.tzOffsetHours("UTC", ref), 0)
  expect_identical(nautilus:::.tzOffsetHours("Europe/Lisbon", ref), 1)
  expect_identical(nautilus:::.tzOffsetHours("+05:30", ref), 5.5)
  expect_true(is.na(nautilus:::.tzOffsetHours("unknown", ref)))
  expect_true(is.na(nautilus:::.tzOffsetHours("Europe/Lisbon", as.POSIXct(NA))))
})
