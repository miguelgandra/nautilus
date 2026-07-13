# Tests for filterVideoPeriod(): subset to video-covered periods (+ optional annotation intervals).

.t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
.mk <- function(id = "A01", n = 600) data.table::data.table(ID = id, datetime = .t0 + 0:(n - 1), depth = runif(n, 0, 50))
.vm <- function(id = "A01") data.table::data.table(ID = id, start = .t0 + c(100, 400), end = .t0 + c(200, 500))  # two segments
.run <- function(...) { res <- NULL; invisible(capture.output(suppressWarnings(suppressMessages(res <- filterVideoPeriod(..., verbose = FALSE))))); res }

test_that("retains only rows within the video segments", {
  out <- .run(list(A01 = .mk()), .vm())
  expect_named(out, "A01")
  expect_equal(nrow(out$A01), 202L)                                # [100,200] + [400,500], inclusive (101 each)
  expect_true(all(out$A01$datetime >= .t0 + 100 & out$A01$datetime <= .t0 + 200 |
                  out$A01$datetime >= .t0 + 400 & out$A01$datetime <= .t0 + 500))
})

test_that("annotation intervals further restrict within the video periods", {
  annot <- data.table::data.table(ID = "A01", start = .t0 + 150, end = .t0 + 450)
  out <- .run(list(A01 = .mk()), .vm(), annotation.intervals = annot)
  expect_equal(nrow(out$A01), 102L)                                # ([150,200] + [400,450]) inside video coverage
})

test_that("a missing annotation start/end is filled from the video coverage", {
  annot <- data.table::data.table(ID = "A01", start = as.POSIXct(NA, tz = "UTC"), end = .t0 + 450)   # start filled with earliest covered time
  out <- .run(list(A01 = .mk()), .vm(), annotation.intervals = annot)
  expect_equal(min(out$A01$datetime), .t0 + 100)
  expect_lte(max(out$A01$datetime), .t0 + 450)
})

test_that("an individual with no video metadata is dropped, not errored", {
  out <- .run(list(B99 = .mk("B99")), .vm("A01"))
  expect_length(out, 0)
})

test_that("validation aborts on a non-POSIXct metadata interval / missing column", {
  bad <- data.table::data.table(ID = "A01", start = 1:2, end = 3:4)           # not POSIXct
  expect_error(filterVideoPeriod(list(A01 = .mk()), bad, verbose = FALSE), "POSIXct")
  expect_error(filterVideoPeriod(list(A01 = .mk()), data.table::data.table(ID = "A01"), verbose = FALSE),
               "column", ignore.case = TRUE)
})

test_that("verbose = FALSE is silent", {
  out <- capture.output(suppressWarnings(suppressMessages(
    res <- filterVideoPeriod(list(A01 = .mk()), .vm(), verbose = FALSE))))
  expect_length(out, 0)
})
