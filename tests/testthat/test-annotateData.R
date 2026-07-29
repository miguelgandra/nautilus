# Tests for annotateData(): marking scored event windows onto sensor data.
#
# Several of these are regression locks for defects found in the pre-refactor version, where the
# function silently produced wrong data rather than failing. Those are called out individually, because
# each one is the kind of fault a green test suite would otherwise never surface.

.ad_tag <- function(id, n = 10, from = "2023-05-01 09:00:00", step = 60) {
  data.frame(ID       = id,
             datetime = as.POSIXct(from, tz = "UTC") + seq_len(n) * step,
             depth    = seq_len(n) / 10,
             stringsAsFactors = FALSE)
}

.ad_ann <- function(id = "s1", event = "feeding",
                    start = "2023-05-01 09:02:00", end = "2023-05-01 09:05:00") {
  data.frame(ID    = id,
             event = event,
             start = as.POSIXct(start, tz = "UTC"),
             end   = as.POSIXct(end,   tz = "UTC"),
             stringsAsFactors = FALSE)
}

.ad <- function(...) suppressMessages(annotateData(..., verbose = "quiet"))


# ---------------------------------------------------------------------------------------------------
# Core behaviour
# ---------------------------------------------------------------------------------------------------

test_that("samples inside a window are marked 1 and those outside 0", {
  res <- .ad(list(s1 = .ad_tag("s1")), .ad_ann())
  d <- res$s1
  expect_true("feeding" %in% names(d))
  inside <- d$datetime >= as.POSIXct("2023-05-01 09:02:00", tz = "UTC") &
            d$datetime <= as.POSIXct("2023-05-01 09:05:00", tz = "UTC")
  expect_identical(d$feeding == 1, inside)          # exactly the in-window rows, no more, no fewer
  expect_setequal(unique(d$feeding), c(0, 1))
})

test_that("window boundaries are inclusive at both ends", {
  # a sample exactly on start and exactly on end must both be marked
  tag <- data.frame(ID = "s1",
                    datetime = as.POSIXct(c("2023-05-01 09:02:00", "2023-05-01 09:03:00",
                                            "2023-05-01 09:05:00", "2023-05-01 09:06:00"), tz = "UTC"),
                    stringsAsFactors = FALSE)
  d <- .ad(list(s1 = tag), .ad_ann())$s1
  expect_identical(d$feeding, c(1, 1, 1, 0))
})

test_that("overlapping windows for one event are unioned, not double-marked", {
  ann <- rbind(.ad_ann(start = "2023-05-01 09:02:00", end = "2023-05-01 09:05:00"),
               .ad_ann(start = "2023-05-01 09:04:00", end = "2023-05-01 09:07:00"))
  d <- .ad(list(s1 = .ad_tag("s1")), ann)$s1
  expect_setequal(unique(d$feeding), c(0, 1))       # never 2
  expect_equal(sum(d$feeding), 6)                   # 09:02..09:07 inclusive
})


# ---------------------------------------------------------------------------------------------------
# Regression locks for silent-wrong-data defects
# ---------------------------------------------------------------------------------------------------

test_that("a single aggregated data.frame is split by id, not treated as a list of columns", {
  # REGRESSION: `if (!is.list(data))` never fired, because a data.frame IS a list. The documented
  # aggregated-table input therefore died with a misleading "id.col not found" error.
  agg <- rbind(.ad_tag("s1"), .ad_tag("s2"))
  res <- .ad(agg, .ad_ann(id = "s1"))
  expect_named(res, c("s1", "s2"), ignore.order = TRUE)
  expect_equal(nrow(res$s1), 10)
  expect_equal(sum(res$s2$feeding), 0)              # s2 was never scored
})

test_that("every individual receives every event column, whoever was scored for what", {
  # REGRESSION: the second loop re-read data[[i]] instead of the zero-filled table, so an individual
  # only got columns for ITS OWN events - the returned tables could not be row-bound.
  ann <- rbind(.ad_ann(id = "s1", event = "feeding"),
               .ad_ann(id = "s2", event = "social"))
  res <- .ad(list(s1 = .ad_tag("s1"), s2 = .ad_tag("s2")), ann)
  expect_setequal(names(res$s1), names(res$s2))
  expect_true(all(c("feeding", "social") %in% names(res$s1)))
  expect_equal(sum(res$s1$social), 0)               # present, and correctly all-zero
  expect_gt(sum(res$s1$feeding), 0)
  expect_silent(do.call(rbind, res))                # the property that actually matters downstream
})

test_that("an individual with no annotations gets the full column set, all zero", {
  # REGRESSION: the pre-refactor inversion gave the UNSCORED animal the complete set and the scored
  # one only its own - exactly backwards.
  ann <- rbind(.ad_ann(id = "s1", event = "feeding"),
               .ad_ann(id = "s1", event = "social", start = "2023-05-01 09:07:00",
                       end = "2023-05-01 09:08:00"))
  res <- .ad(list(s1 = .ad_tag("s1"), s9 = .ad_tag("s9")), ann)
  expect_setequal(names(res$s9), names(res$s1))
  expect_equal(sum(res$s9$feeding), 0)
  expect_equal(sum(res$s9$social), 0)
})

test_that("an event named after an existing column is refused, not silently written over it", {
  # REGRESSION: scoring an event called "depth" replaced the depth trace with 0/1 and said nothing.
  expect_error(.ad(list(s1 = .ad_tag("s1")), .ad_ann(event = "depth")),
               "overwrite", ignore.case = TRUE)
  expect_error(.ad(list(s1 = .ad_tag("s1")), .ad_ann(event = "datetime")),
               "overwrite", ignore.case = TRUE)
})

test_that("annotation rows with a missing start or end are dropped, not propagated as NA", {
  # REGRESSION: an NA bound made the comparison NA and errored out of the subassignment.
  ann <- rbind(.ad_ann(), .ad_ann(start = NA_character_, end = "2023-05-01 09:09:00"))
  res <- expect_no_error(.ad(list(s1 = .ad_tag("s1")), ann))
  expect_equal(sum(res$s1$feeding), 4)              # only the usable window was applied
})


# ---------------------------------------------------------------------------------------------------
# Provenance and object integrity
# ---------------------------------------------------------------------------------------------------

test_that("the call is recorded in the processing history and the tag class survives", {
  tg <- suppressMessages(buildTagData(.ad_tag("s1"), id = "s1", verbose = "quiet"))
  res <- .ad(list(s1 = tg), .ad_ann())
  expect_s3_class(res$s1, "nautilus_tag")
  hist <- processingHistory(res$s1)
  expect_true("annotateData" %in% hist$step)
  det <- hist$details[hist$step == "annotateData"]
  expect_match(det, "events = feeding")
  expect_match(det, "n_windows = 1")
})

test_that("a plain data.frame input is not given a spurious processing history", {
  res <- .ad(list(s1 = .ad_tag("s1")), .ad_ann())
  expect_false(inherits(res$s1, "nautilus_tag"))
})


# ---------------------------------------------------------------------------------------------------
# Validation and reporting
# ---------------------------------------------------------------------------------------------------

test_that("missing or wrongly-typed inputs abort with a message naming the culprit", {
  tag <- list(s1 = .ad_tag("s1"))
  expect_error(.ad(tag, .ad_ann()[, c("ID", "event", "start")]), "end",    ignore.case = TRUE)
  expect_error(.ad(tag, transform(.ad_ann(), start = as.character(start))), "POSIXct")
  bad <- .ad_tag("s1"); bad$datetime <- as.character(bad$datetime)
  expect_error(.ad(list(s1 = bad), .ad_ann()), "POSIXct")
  expect_error(.ad(list(), .ad_ann()), "empty|non-empty", ignore.case = TRUE)
})

test_that("the datetime error names the column the caller actually passed", {
  tag <- .ad_tag("s1"); names(tag)[2] <- "time"; tag$time <- as.character(tag$time)
  expect_error(.ad(list(s1 = tag), .ad_ann(), datetime.col = "time"), "time")
})

test_that("selected.events filters, and an unknown event is rejected", {
  ann <- rbind(.ad_ann(event = "feeding"),
               .ad_ann(event = "social", start = "2023-05-01 09:07:00", end = "2023-05-01 09:08:00"))
  res <- .ad(list(s1 = .ad_tag("s1")), ann, selected.events = "feeding")
  expect_true("feeding" %in% names(res$s1))
  expect_false("social" %in% names(res$s1))         # deselected events get no column at all
  expect_error(.ad(list(s1 = .ad_tag("s1")), ann, selected.events = "nonesuch"), "nonesuch")
})

test_that("annotations for an individual absent from data raise a grouped warning", {
  # scoring effort that cannot be applied is worth surfacing - it usually means a typo or a
  # deployment dropped upstream
  expect_warning(suppressMessages(annotateData(list(s1 = .ad_tag("s1")),
                                               rbind(.ad_ann(id = "s1"), .ad_ann(id = "ghost")),
                                               verbose = "quiet")),
                 "ghost")
})

test_that("NULL list elements are skipped and omitted from the result", {
  res <- .ad(list(s1 = .ad_tag("s1"), s2 = NULL), .ad_ann())
  expect_named(res, "s1")
})

test_that("verbose = 'quiet' silences the console", {
  expect_silent(annotateData(list(s1 = .ad_tag("s1")), .ad_ann(), verbose = "quiet"))
})
