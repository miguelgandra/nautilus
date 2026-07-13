# Tests for launchVideo(): input validation + match logic (VLC is never actually launched here).

.vm <- function() data.frame(ID = "A01",
                             start = as.POSIXct("2020-01-01 00:00:00", tz = "UTC"),
                             end   = as.POSIXct("2020-01-01 00:10:00", tz = "UTC"),
                             video = "v1.mp4", file = "/tmp/v1.mp4", stringsAsFactors = FALSE)

test_that("input validation aborts clearly", {
  dt <- as.POSIXct("2020-01-01 00:05:00", tz = "UTC")
  expect_error(launchVideo("", dt, .vm()), "id", ignore.case = TRUE)                        # empty id
  expect_error(launchVideo("A01", "2020-01-01", .vm()), "POSIXct")                          # datetime not POSIXct
  expect_error(launchVideo("A01", dt, data.frame(ID = "A01")), "column", ignore.case = TRUE) # missing columns
  expect_error(launchVideo("ZZZ", dt, .vm()), "not present", ignore.case = TRUE)            # id absent
})

test_that("a datetime outside any video returns FALSE (no launch)", {
  out_after  <- suppressMessages(launchVideo("A01", as.POSIXct("2020-01-01 05:00:00", tz = "UTC"), .vm()))
  out_before <- suppressMessages(launchVideo("A01", as.POSIXct("2019-12-31 23:00:00", tz = "UTC"), .vm()))
  expect_false(out_after)
  expect_false(out_before)
})

test_that("a matching datetime aborts when VLC is not found (rather than launching)", {
  dt <- as.POSIXct("2020-01-01 00:05:00", tz = "UTC")               # within [00:00, 00:10]
  expect_error(launchVideo("A01", dt, .vm(), vlc.path = "/no/such/vlc", close.existing = FALSE),
               "VLC", ignore.case = TRUE)
})
