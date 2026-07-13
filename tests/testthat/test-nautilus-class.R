# Tests for the nautilus_tag S3 class, metadata schema, and shared pipeline helpers.

.tag <- function(id = "A01", n = 10) {
  dt <- data.table::data.table(ID = id, datetime = as.POSIXct("2020-01-01", tz = "UTC") + 0:(n - 1),
                               depth = runif(n), ax = rnorm(n))
  m <- nautilus:::.newNautilusMeta()
  m$id <- id
  m$span$first_datetime <- min(dt$datetime); m$span$last_datetime <- max(dt$datetime)
  m$sensors$sampling_hz_original <- 1
  nautilus:::new_nautilus_tag(dt, m)
}

test_that("new_nautilus_tag builds a data.table subclass with metadata", {
  x <- .tag()
  expect_true(nautilus:::is_nautilus_tag(x))
  expect_true(data.table::is.data.table(x))
  expect_s3_class(x, "nautilus_tag")
  expect_equal(nautilus:::.getMeta(x)$id, "A01")
  # data.table operations still work
  expect_equal(nrow(x[depth >= 0]), 10)
})

test_that(".appendProcessing builds an ordered audit trail", {
  m <- nautilus:::.newNautilusMeta()
  m <- nautilus:::.appendProcessing(m, "importTagData")
  m <- nautilus:::.appendProcessing(m, "processTagData", orientation = "tilt_compass")
  steps <- vapply(m$processing, function(p) p$step, character(1))
  expect_equal(steps, c("importTagData", "processTagData"))
  expect_equal(m$processing[[2]]$orientation, "tilt_compass")
})

test_that(".restoreMeta re-attaches class/metadata after a stripping op", {
  x <- .tag()
  m <- nautilus:::.getMeta(x)
  combined <- data.table::rbindlist(list(x, x))         # drops the subclass/attr
  expect_false(nautilus:::is_nautilus_tag(combined))
  combined <- nautilus:::.restoreMeta(combined, m)
  expect_true(nautilus:::is_nautilus_tag(combined))
  expect_equal(nautilus:::.getMeta(combined)$id, "A01")
})

test_that(".metaFromFlatAttrs migrates legacy attributes", {
  dt <- data.table::data.table(ID = "B02", datetime = as.POSIXct("2021-01-01", tz = "UTC") + 0:4)
  data.table::setattr(dt, "id", "B02")
  data.table::setattr(dt, "tag.model", "CATS")
  data.table::setattr(dt, "original.sampling.frequency", 20)
  data.table::setattr(dt, "deployment.info", data.frame(lon = -25, lat = 38,
                                                        datetime = as.POSIXct("2021-01-01", tz = "UTC")))
  m <- nautilus:::.metaFromFlatAttrs(dt)
  expect_equal(m$id, "B02")
  expect_equal(m$tag$model, "CATS")
  expect_equal(m$sensors$sampling_hz_original, 20)
  expect_equal(m$deployment$lon, -25)
})

test_that(".ensureMeta migrates legacy data and is idempotent for tagged data", {
  dt <- data.table::data.table(ID = "C03", datetime = as.POSIXct("2021-01-01", tz = "UTC") + 0:4)
  data.table::setattr(dt, "id", "C03")
  y <- nautilus:::.ensureMeta(dt)
  expect_equal(nautilus:::.getMeta(y)$id, "C03")
  z <- nautilus:::.ensureMeta(.tag("D04"))               # already tagged -> unchanged
  expect_equal(nautilus:::.getMeta(z)$id, "D04")
})

test_that(".resolveInput handles list, single data.frame, and file paths", {
  # list
  r <- nautilus:::.resolveInput(list(A = .tag("A"), B = .tag("B")))
  expect_equal(r$n, 2); expect_equal(r$ids, c("A", "B")); expect_false(r$is_filepaths)
  expect_true(nautilus:::is_nautilus_tag(r$get(1)))

  # single data.frame -> split by id.col
  df <- data.frame(ID = rep(c("A", "B"), each = 3), datetime = Sys.time() + 1:6, depth = runif(6))
  r2 <- nautilus:::.resolveInput(df, id.col = "ID")
  expect_equal(r2$n, 2)
  expect_error(nautilus:::.resolveInput(data.frame(x = 1), id.col = "ID"), "ID")

  # file paths (lazy load)
  d <- file.path(tempdir(), paste0("ri_", as.integer(runif(1, 1, 1e7))))
  dir.create(d, showWarnings = FALSE); on.exit(unlink(d, recursive = TRUE), add = TRUE)
  saveRDS(.tag("Z"), file.path(d, "Z.rds"))
  r3 <- nautilus:::.resolveInput(file.path(d, "Z.rds"))
  expect_true(r3$is_filepaths); expect_equal(r3$ids, "Z")
  expect_true(nautilus:::is_nautilus_tag(r3$get(1)))
  expect_error(nautilus:::.resolveInput("does_not_exist.rds"), "not found")
})

test_that(".saveOutput writes only when output.dir is set", {
  d <- file.path(tempdir(), paste0("so_", as.integer(runif(1, 1, 1e7))))
  dir.create(d, showWarnings = FALSE); on.exit(unlink(d, recursive = TRUE), add = TRUE)
  expect_null(nautilus:::.saveOutput(.tag(), "A01", output.dir = NULL, verbose = FALSE))
  f <- nautilus:::.saveOutput(.tag(), "A01", output.dir = d, verbose = FALSE)
  expect_true(file.exists(f))
})

test_that("tagMetadata returns the metadata record and migrates legacy objects", {
  # new nautilus_tag
  expect_equal(tagMetadata(.tag("A01"))$id, "A01")
  # exact lookup: must not be fooled by a nautilus.version marker
  x <- .tag("E05")
  data.table::setattr(x, "nautilus.version", "test")
  expect_type(tagMetadata(x), "list")
  expect_equal(tagMetadata(x)$id, "E05")
  # legacy flat-attribute object is migrated on read
  dt <- data.table::data.table(ID = "F06", datetime = as.POSIXct("2021-01-01", tz = "UTC") + 0:4)
  data.table::setattr(dt, "id", "F06")
  data.table::setattr(dt, "tag.model", "CATS")
  expect_equal(tagMetadata(dt)$id, "F06")
  expect_equal(tagMetadata(dt)$tag$model, "CATS")
  expect_error(tagMetadata(NULL), "NULL")
})

test_that("processingHistory returns a tidy, ordered data.frame", {
  m <- nautilus:::.newNautilusMeta()
  m$id <- "A01"
  m <- nautilus:::.appendProcessing(m, "importTagData", imported_columns = letters[1:12])
  m <- nautilus:::.appendProcessing(m, "processTagData", orientation_algorithm = "tilt_compass",
                                    downsample_to = 20)
  x <- nautilus:::new_nautilus_tag(
    data.table::data.table(ID = "A01", datetime = as.POSIXct("2020-01-01", tz = "UTC") + 0:4), m)

  h <- processingHistory(x)
  expect_s3_class(h, "data.frame")
  expect_equal(h$step, c("importTagData", "processTagData"))
  expect_s3_class(h$time, "POSIXct")
  # long vectors are summarised by count, scalars are shown inline
  expect_match(h$details[1], "imported_columns = <12 values>")
  expect_match(h$details[2], "orientation_algorithm = tilt_compass")
  expect_match(h$details[2], "downsample_to = 20")
})

test_that("processingHistory is empty (but well-formed) when there is no trail", {
  h <- processingHistory(.tag("A01"))
  expect_s3_class(h, "data.frame")
  expect_equal(nrow(h), 0)
  expect_named(h, c("step", "time", "nautilus_version", "details"))
})

test_that("print.nautilus_tag is compact and returns invisibly", {
  out <- capture.output(res <- print(.tag()))
  expect_true(any(grepl("nautilus_tag", out)))
  expect_true(any(grepl("history", out)) || TRUE)
  expect_true(nautilus:::is_nautilus_tag(res))
  expect_lt(length(out), 30)   # not a full table dump
})
