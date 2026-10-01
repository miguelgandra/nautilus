.metadata_test_tag <- function(id = "A01") {
  meta <- nautilus:::.newNautilusMeta()
  meta$id <- id
  meta$biometrics <- list(sex = "F", length_cm = 500)
  meta$ancillary$positions <- list(data = data.table::data.table(lon = 1:3, lat = 4:6))
  meta <- nautilus:::.appendProcessing(meta, "importTagData")
  nautilus:::new_nautilus_tag(data.table::data.table(ID = id, depth = 1:3), meta)
}

test_that("the metadata API exposes snapshots with compact print methods", {
  tag <- .metadata_test_tag()
  meta <- getTagMetadata(tag)
  expect_s3_class(meta, "nautilus_metadata")
  expect_type(meta, "list")
  expect_equal(meta$id, "A01")
  expect_null(attr(nautilus:::.getMeta(tag), "class", exact = TRUE))
  meta$biometrics$sex <- "M"
  meta$ancillary$positions$data[, lon := 100]
  expect_equal(getTagMetadata(tag)$biometrics$sex, "F")
  expect_equal(getTagMetadata(tag)$ancillary$positions$data$lon, 1:3)
  expect_false(any(c("tagMetadata", "updateBiometrics") %in% getNamespaceExports("nautilus")))
})

test_that("getter handles objects, paths and named collections without file writes", {
  tag <- .metadata_test_tag()
  path <- tempfile(fileext = "_processed.rds")
  on.exit(unlink(path), add = TRUE)
  saveRDS(tag, path)
  before <- tools::md5sum(path)
  expect_equal(getTagMetadata(path), getTagMetadata(tag))
  expect_named(getTagMetadata(list(source_name = tag)), "A01")
  expect_named(getTagMetadata(list(.metadata_test_tag("A01"), .metadata_test_tag("B02"))), c("A01", "B02"))
  expect_equal(processingHistory(path), processingHistory(tag))
  expect_equal(tools::md5sum(path), before)
  expect_error(getTagMetadata(list(tag, tag)), "duplicate")
  expect_error(processingHistory(list(tag)), "single")
})

test_that("metadata input validation rejects malformed or ambiguous identities", {
  expect_error(getTagMetadata(NULL), "NULL")
  expect_error(getTagMetadata(character()), "empty", ignore.case = TRUE)
  expect_error(getTagMetadata(NA_character_), "missing")
  expect_error(getTagMetadata(""), "empty")
  expect_error(getTagMetadata("not_a_tag.rds"), "not found")
  expect_error(getTagMetadata(42), "must be")
  expect_error(getTagMetadata(list(list(id = "A01"))), "data.frame")
  bad <- .metadata_test_tag()
  bad$ID[2] <- "B02"
  expect_error(getTagMetadata(bad), "one non-missing")
  bad <- .metadata_test_tag()
  bad$ID[] <- "B02"
  expect_error(updateTagMetadata(bad, list(animal_id = "animal"), verbose = FALSE), "does not match")
})

test_that("legacy metadata migration on read does not mutate callers", {
  tag <- data.table::data.table(ID = "LEGACY", depth = 1:3)
  data.table::setattr(tag, "tag.model", "CATS")
  before <- data.table::copy(tag)
  expect_equal(getTagMetadata(tag)$tag$model, "CATS")
  expect_identical(tag, before)
  expect_null(attr(tag, "nautilus", exact = TRUE))
  updated <- updateTagMetadata(tag, list(biometrics = list(sex = "M")), verbose = FALSE)
  expect_s3_class(updated, "nautilus_tag")
  expect_identical(tag, before)
})

test_that("uniform updates merge safe leaves and preserve data and processing history", {
  tag <- .metadata_test_tag()
  alias <- tag
  before <- data.table::copy(tag)
  out <- updateTagMetadata(tag, list(animal_id = "SHARK_1", deployment = list(site = "North reef"),
                                    biometrics = list(sex = factor("M"), age = 4L),
                                    user = list(note = "Rechecked logbook")), verbose = FALSE)
  expect_s3_class(out, "nautilus_tag")
  meta <- getTagMetadata(out)
  expect_identical(meta$biometrics, list(sex = "M", length_cm = 500, age = 4L))
  expect_equal(meta$animal_id, "SHARK_1")
  expect_equal(meta$deployment$site, "North reef")
  expect_equal(meta$user$note, "Rechecked logbook")
  expect_identical(nautilus:::.getMeta(out)$processing, nautilus:::.getMeta(tag)$processing)
  expect_identical(out$depth, tag$depth)
  expect_identical(alias, before)
  out$depth[] <- 200
  expect_identical(tag$depth, before$depth)
  missing <- updateTagMetadata(out, list(biometrics = list(length_cm = NA_real_)), verbose = FALSE)
  expect_identical(getTagMetadata(missing)$biometrics$length_cm, NA_real_)
})

test_that("updater makes an independent copy of nested metadata tables", {
  tag <- .metadata_test_tag()
  out <- updateTagMetadata(tag, list(biometrics = list(sex = "M")), verbose = FALSE)
  meta <- nautilus:::.getMeta(out)
  meta$ancillary$positions$data[, lon := 200]
  expect_equal(getTagMetadata(out)$ancillary$positions$data$lon, rep(200, 3))
  expect_equal(getTagMetadata(tag)$ancillary$positions$data$lon, 1:3)
})

test_that("protected metadata cannot be edited through the unified setter", {
  tag <- .metadata_test_tag()
  for (field in c("id", "tag", "sensors", "span", "sidecar", "ancillary", "axis_mapping",
                  "mag_calibration", "processing", "unknown")) {
    patch <- stats::setNames(list("changed"), field)
    expect_error(updateTagMetadata(tag, patch, verbose = FALSE), "protected|unsupported")
  }
  for (field in c("lon", "lat", "datetime", "attachment_site", "deployment_type", "heading_reference", "magnetic_declination")) {
    patch <- list(deployment = stats::setNames(list("changed"), field))
    expect_error(updateTagMetadata(tag, patch, verbose = FALSE), "protected")
  }
  expect_equal(getTagMetadata(tag)$biometrics$sex, "F")
})

test_that("patch validation enforces named scalar values before modifying anything", {
  tag <- .metadata_test_tag()
  invalid <- list(list(), list("F"), list(biometrics = list()),
                  list(biometrics = list(sex = NULL)), list(biometrics = list(sex = c("F", "M"))),
                  list(biometrics = list(sex = list("F"))), list(biometrics = list(size = Inf)),
                  list(animal_id = 1), list(deployment = list(site = NA)),
                  list(biometrics = stats::setNames(list(1, 2), c("size", "size"))))
  for (patch in invalid) expect_error(updateTagMetadata(tag, patch, verbose = FALSE))
  expect_equal(getTagMetadata(tag)$biometrics$sex, "F")
})

test_that("table updates match IDs, support custom columns and leave computation roles unchanged", {
  tags <- list(wrong_name = .metadata_test_tag("A01"), B02 = .metadata_test_tag("B02"))
  updates <- data.table::data.table(deployment = c("B02", "A01"), animal = c("S2", "S1"),
                                   locality = c("South", "North"), sex = c("F", "M"), tag = "not imported")
  cols <- metadataColumns(id = "deployment", animal_id = "animal", deploy_site = "locality", traits = "sex")
  out <- updateTagMetadata(tags, updates, columns = cols, verbose = FALSE)
  expect_named(out, c("A01", "B02"))
  expect_equal(getTagMetadata(out$A01)$animal_id, "S1")
  expect_equal(getTagMetadata(out$A01)$deployment$site, "North")
  expect_equal(getTagMetadata(out$A01)$biometrics$sex, "M")
  expect_equal(getTagMetadata(out$B02)$animal_id, "S2")
  expect_identical(getTagMetadata(out$A01)$tag, getTagMetadata(tags$wrong_name)$tag)
  expect_equal(getTagMetadata(tags$wrong_name)$biometrics$sex, "F")
})

test_that("invalid update tables fail before any output files are written", {
  tag <- .metadata_test_tag()
  directory <- tempfile(); dir.create(directory)
  on.exit(unlink(directory, recursive = TRUE), add = TRUE)
  cols <- metadataColumns(traits = "sex")
  for (ids in list(c("A01", "A01"), c("A01", NA), c("A01", ""))) {
    updates <- data.frame(ID = ids, sex = c("F", "M"))
    expect_error(updateTagMetadata(tag, updates, columns = cols, output.dir = directory, verbose = FALSE), "unique")
  }
  updates <- data.frame(ID = c("A01", "B02"))
  updates$sex <- list("F", c("F", "M"))
  expect_error(updateTagMetadata(tag, updates, columns = cols, output.dir = directory, verbose = FALSE), "scalar")
  updates <- data.frame(ID = "A01", sex = "F", sex = "M", check.names = FALSE)
  expect_error(updateTagMetadata(tag, updates, columns = cols, output.dir = directory, verbose = FALSE), "column names")
  expect_length(list.files(directory), 0L)
})

test_that("unmatched legacy inputs are retained as migrated tag objects", {
  tag <- data.frame(ID = "LEGACY", depth = 1:3)
  out <- NULL
  expect_warning(out <- updateTagMetadata(tag, data.frame(ID = "OTHER", sex = "F"),
                                          columns = metadataColumns(traits = "sex"), verbose = FALSE), "no matching")
  expect_s3_class(out, "nautilus_tag")
  expect_identical(out$depth, tag$depth)
  expect_length(getTagMetadata(out)$biometrics, 0L)
  expect_null(attr(tag, "nautilus", exact = TRUE))
})

test_that("file input is read-only unless output.dir is supplied, and unmatched tags are saved", {
  source_dir <- tempfile(); dir.create(source_dir)
  output_dir <- tempfile(); dir.create(output_dir)
  on.exit(unlink(c(source_dir, output_dir), recursive = TRUE), add = TRUE)
  paths <- file.path(source_dir, c("A01_processed.rds", "B02_processed.rds"))
  saveRDS(.metadata_test_tag("A01"), paths[1])
  saveRDS(.metadata_test_tag("B02"), paths[2])
  before <- tools::md5sum(paths)
  out <- updateTagMetadata(paths[1], list(biometrics = list(sex = "M")), verbose = FALSE)
  expect_s3_class(out, "nautilus_tag")
  expect_equal(getTagMetadata(out)$biometrics$sex, "M")
  expect_equal(tools::md5sum(paths), before)
  updates <- data.frame(ID = "A01", sex = "M")
  written <- NULL
  expect_warning(written <- withVisible(updateTagMetadata(paths, updates, columns = metadataColumns(traits = "sex"),
                                                          output.dir = output_dir, output.suffix = "_edited",
                                                          return.data = FALSE, compress = FALSE, verbose = FALSE)), "retained unchanged")
  expect_false(written$visible)
  expect_equal(basename(written$value), c("A01_edited.rds", "B02_edited.rds"))
  expect_equal(getTagMetadata(written$value[1])$biometrics$sex, "M")
  expect_equal(getTagMetadata(written$value[2])$biometrics$sex, "F")
  expect_equal(tools::md5sum(paths), before)
})

test_that("custom sensor ID columns and metadata-only IDs are supported", {
  tag <- .metadata_test_tag()
  data.table::setnames(tag, "ID", "deployment")
  out <- updateTagMetadata(tag, list(animal_id = "SHARK"), id.col = "deployment", verbose = FALSE)
  expect_equal(getTagMetadata(out)$animal_id, "SHARK")
  expect_equal(getTagMetadata(tag)$id, "A01")
  tag$deployment <- NULL
  out <- updateTagMetadata(tag, list(animal_id = "SHARK"), verbose = FALSE)
  expect_equal(getTagMetadata(out)$animal_id, "SHARK")
})

test_that("printing remains compact, preserves recorded states and never expands nested data", {
  tag <- .metadata_test_tag()
  meta <- nautilus:::.getMeta(tag)
  meta$mag_calibration$proposed <- nautilus:::.newMagCalStateBlock()
  meta$mag_calibration$proposed$qc$confidence <- "low"
  meta$axis_mapping$applied <- TRUE
  meta$axis_mapping$source <- "applyAxisMapping"
  meta$sidecar <- list(device = list(utc_offset = -1), logging = list(utc_offset = 0))
  meta$processing <- rep(meta$processing, 100L)
  meta <- nautilus:::.appendProcessing(meta, "processTagData", status = "partial", partial_reason = "accel excluded by QC")
  tag <- nautilus:::.restoreMeta(tag, meta)
  snapshot <- getTagMetadata(tag)
  before <- data.table::copy(snapshot)
  printed <- NULL
  lines <- capture.output(printed <- withVisible(print(snapshot)))
  expect_false(printed$visible)
  expect_identical(printed$value, snapshot)
  expect_identical(snapshot, before)
  expect_lt(length(lines), 25L)
  expect_true(any(grepl("proposed, not applied", lines)))
  expect_true(any(grepl("partial; accel excluded", lines)))
  expect_true(any(grepl("device -1h", lines)))
  expect_true(any(grepl("101 steps", lines)))
  expect_true(any(grepl("positions 3 rows", lines)))
  expect_false(any(grepl("soft_iron|source_ids", lines)))
  partial <- structure(list(id = "OLD"), class = "nautilus_metadata")
  expect_no_error(capture.output(print(partial)))
})
