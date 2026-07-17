# Tests for buildTagData(): the public in-memory nautilus_tag constructor.

.mkdf <- function(n = 3000, fs = 20, with_dt = TRUE, id = TRUE) {
  d <- data.frame(ax = stats::rnorm(n), ay = stats::rnorm(n), az = 1 + stats::rnorm(n),
                  depth = abs(stats::rnorm(n, 5)), temp = stats::rnorm(n, 18))
  if (with_dt) d$datetime <- as.POSIXct("2025-01-01", tz = "UTC") + (seq_len(n) - 1) / fs
  if (id) d$ID <- "A01"
  d
}

test_that("buildTagData builds a valid nautilus_tag with the expected meta", {
  tag <- buildTagData(.mkdf(), verbose = FALSE)
  expect_s3_class(tag, "nautilus_tag")
  expect_true(data.table::is.data.table(tag))
  m <- nautilus:::.getMeta(tag)
  expect_identical(m$id, "A01")
  expect_setequal(m$sensors$present, c("ax", "ay", "az", "depth", "temp"))
  expect_equal(m$sensors$sampling_hz_original, 20)
  expect_equal(m$span$original_rows, 3000L)
  expect_false(m$axis_mapping$applied)                                   # raw: no transform applied
  expect_true(!is.null(attr(tag, "nautilus.version")))
  expect_true(any(vapply(m$processing, function(p) identical(p$step, "buildTagData"), logical(1))))
})

test_that("buildTagData flows through processTagData accelerometer-only (heading NA, no abort)", {
  skip_if_not_installed("signal")
  tag <- buildTagData(.mkdf(n = 4000), id = "A01", verbose = FALSE)
  out <- suppressWarnings(processTagData(tag, downsample.to = NULL, verbose = "quiet"))
  if (is.list(out) && !data.table::is.data.table(out)) out <- out[[1]]
  expect_true(all(c("odba", "vedba", "pitch", "roll", "surge", "sway", "heave") %in% names(out)))
  expect_true(all(is.na(out$heading)))
})

test_that("start + sampling.rate synthesises timestamps when there is no datetime column", {
  d <- .mkdf(n = 2000, with_dt = FALSE, id = FALSE)
  tag <- buildTagData(d, id = "S01", start = as.POSIXct("2025-06-01 09:00:00", tz = "UTC"),
                      sampling.rate = 50, verbose = FALSE)
  m <- nautilus:::.getMeta(tag)
  expect_equal(m$sensors$sampling_hz_original, 50)
  expect_equal(as.numeric(m$span$first_datetime), as.numeric(as.POSIXct("2025-06-01 09:00:00", tz = "UTC")))
  expect_equal(as.numeric(diff(tag$datetime[1:2])), 1 / 50, tolerance = 1e-4)  # POSIXct epoch float precision
  expect_error(buildTagData(d, id = "S01", start = as.POSIXct("2025-06-01", tz = "UTC")), "sampling.rate")
})

test_that("sensor.mapping renames non-canonical columns", {
  d <- data.frame(X = stats::rnorm(500), Y = stats::rnorm(500), Z = 1 + stats::rnorm(500),
                  datetime = as.POSIXct("2025-01-01", tz = "UTC") + (0:499) / 20)
  tag <- buildTagData(d, id = "M01", sensor.mapping = c(ax = "X", ay = "Y", az = "Z"), verbose = FALSE)
  expect_true(all(c("ax", "ay", "az") %in% names(tag)))
  expect_setequal(nautilus:::.getMeta(tag)$sensors$present, c("ax", "ay", "az"))
  expect_error(buildTagData(d, id = "M01", sensor.mapping = c(ax = "NOPE")), "not in")
})

test_that("id resolves from argument, ID column, or metadata, and errors when absent or ambiguous", {
  expect_identical(nautilus:::.getMeta(buildTagData(.mkdf(id = TRUE), verbose = FALSE))$id, "A01")     # ID column
  expect_identical(nautilus:::.getMeta(buildTagData(.mkdf(id = FALSE), id = "ARG", verbose = FALSE))$id, "ARG")
  expect_identical(nautilus:::.getMeta(
    buildTagData(.mkdf(id = FALSE), metadata = list(id = "META1"), verbose = FALSE))$id, "META1")
  expect_error(buildTagData(.mkdf(id = FALSE), verbose = FALSE), "id")                                  # none
  d2 <- .mkdf(id = FALSE); d2$ID <- rep(c("A", "B"), length.out = nrow(d2))
  expect_error(buildTagData(d2, verbose = FALSE), "several IDs")                                        # ambiguous
})

test_that("metadata maps role-named fields onto the meta schema, incl. traits", {
  md <- list(deploy_lon = -28.6, deploy_lat = 38.6, tag_model = "Little Leonardo", tag_type = "LL",
             attachment_site = "left pectoral fin", deployment_type = "rigid", logger_id = "LL07",
             TL = 125.5, Species = "S. zygaena")
  tag <- buildTagData(.mkdf(), id = "LL01", metadata = md, traits = c("TL", "Species"), verbose = FALSE)
  m <- nautilus:::.getMeta(tag)
  expect_equal(m$deployment$lon, -28.6);  expect_equal(m$deployment$lat, 38.6)
  expect_identical(m$deployment$attachment_site, "left pectoral fin")
  expect_identical(m$deployment$deployment_type, "rigid")
  expect_identical(m$tag$model, "Little Leonardo"); expect_identical(m$tag$logger_id, "LL07")
  expect_equal(m$biometrics$TL, 125.5); expect_identical(m$biometrics$Species, "S. zygaena")
})

test_that("required.sensors is enforced", {
  expect_s3_class(buildTagData(.mkdf(), required.sensors = c("ax", "depth"), verbose = FALSE), "nautilus_tag")
  d <- .mkdf(); d$mx <- NULL
  expect_error(buildTagData(d, required.sensors = c("ax", "mx"), verbose = FALSE), "missing")
  expect_error(buildTagData(.mkdf(), required.sensors = "not_a_sensor", verbose = FALSE), "valid sensor")
})

test_that("input validation: empty data, no timestamp, non-POSIXct, NA times, no sensors", {
  expect_error(buildTagData(.mkdf()[0, ], id = "X", verbose = FALSE))                                   # empty
  expect_error(buildTagData(.mkdf(with_dt = FALSE), id = "X", verbose = FALSE), "no")                   # no time
  bad <- .mkdf(); bad$datetime <- as.character(bad$datetime)
  expect_error(buildTagData(bad, verbose = FALSE), "POSIXct")
  nat <- .mkdf(); nat$datetime[5] <- NA
  expect_error(buildTagData(nat, verbose = FALSE), "NA")
  ns <- data.frame(foo = 1:100, datetime = as.POSIXct("2025-01-01", tz = "UTC") + (0:99))
  expect_error(buildTagData(ns, id = "X", verbose = FALSE), "sensor channels")
})

test_that("buildTagData does not mutate the caller's data frame", {
  d <- .mkdf(n = 500)
  before_cols <- names(d); before_class <- class(d)
  invisible(buildTagData(d, verbose = FALSE))
  expect_identical(names(d), before_cols)      # no ID rename, no datetime rename, no new columns
  expect_identical(class(d), before_class)     # still a plain data.frame, not classed/keyed
})

test_that("a non-'datetime' timestamp column is renamed to datetime", {
  d <- .mkdf(); names(d)[names(d) == "datetime"] <- "time_utc"
  tag <- buildTagData(d, id = "R01", datetime.col = "time_utc", verbose = FALSE)
  expect_true("datetime" %in% names(tag))
  expect_false("time_utc" %in% names(tag))
})

test_that("sensor.mapping rejects a target that collides with an existing column or is duplicated", {
  d <- data.frame(ax = rep(1, 50), X = rep(9, 50),
                  datetime = as.POSIXct("2025-01-01", tz = "UTC") + (0:49) / 20)
  expect_error(buildTagData(d, id = "C", sensor.mapping = c(ax = "X"), verbose = FALSE), "already exist")
  d2 <- data.frame(P = stats::rnorm(50), Q = stats::rnorm(50), az = 1 + stats::rnorm(50),
                   datetime = as.POSIXct("2025-01-01", tz = "UTC") + (0:49) / 20)
  expect_error(buildTagData(d2, id = "C", sensor.mapping = c(ax = "P", ax = "Q"), verbose = FALSE), "same target")
})

test_that("the datetime column's tzone matches meta$sensors$timezone (clock interpreted, not shifted)", {
  d <- .mkdf(n = 200)
  d$datetime <- as.POSIXct("2025-06-01 08:00:00", tz = "America/New_York") + (seq_len(200) - 1) / 20
  tag <- buildTagData(d, id = "TZ1", timezone = "UTC", verbose = FALSE)
  expect_identical(attr(tag$datetime, "tzone"), "UTC")                    # invariant: column tz == meta tz
  expect_identical(nautilus:::.getMeta(tag)$sensors$timezone, "UTC")
  expect_identical(format(tag$datetime[1], "%H:%M:%S", tz = "UTC"), "08:00:00")  # wall-clock preserved
})

test_that("a mapped trait is recorded even when NA, matching the importTagData path", {
  tag <- buildTagData(.mkdf(), id = "T1", metadata = list(TL = 125.5, Sex = NA_character_),
                      traits = c("TL", "Sex"), verbose = FALSE)
  b <- nautilus:::.getMeta(tag)$biometrics
  expect_equal(b$TL, 125.5)
  expect_true("Sex" %in% names(b))   # the key exists (the trait WAS mapped)...
  expect_true(is.na(b$Sex))          # ...carrying NA, rather than being silently omitted
})

test_that("buildTagData and importTagData share one meta assembler (.assembleTagMeta)", {
  # the shared core populates the schema; sampling.hz is the one parameterised difference
  d <- .mkdf(n = 400)
  m1 <- nautilus:::.assembleTagMeta(data.table::as.data.table(d), id = "X1", timezone = "UTC")
  expect_identical(m1$id, "X1")
  expect_setequal(m1$sensors$present, c("ax", "ay", "az", "depth", "temp"))
  expect_true(is.na(m1$sensors$sampling_hz_original))          # NULL sampling.hz -> left NA (import path)
  expect_equal(m1$span$original_rows, 400L)
  expect_false(m1$axis_mapping$applied)
  expect_length(m1$processing, 0)                              # no provenance: each caller appends its own
  m2 <- nautilus:::.assembleTagMeta(data.table::as.data.table(d), id = "X1", timezone = "UTC",
                                    sampling.hz = 20)
  expect_equal(m2$sensors$sampling_hz_original, 20)            # buildTagData path supplies a rate
})
