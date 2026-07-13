# Tests for the metadataColumns() metadata-column schema and its coercion helper.

test_that("metadataColumns returns a validated schema with canonical defaults", {
  s <- metadataColumns()
  expect_s3_class(s, "nautilus_metadata_columns")
  expect_equal(s$id, "ID")
  expect_equal(s$deploy_datetime, "tagging_date")
  expect_null(s$package_id)
})

test_that("metadataColumns overrides only the named fields", {
  s <- metadataColumns(deploy_datetime = "deploy_date", package_id = "PackageID")
  expect_equal(s$deploy_datetime, "deploy_date")
  expect_equal(s$package_id, "PackageID")
  expect_equal(s$id, "ID")                       # untouched
})

test_that("metadataColumns validates field types and required fields", {
  expect_error(metadataColumns(id = NULL), "columns\\$id")          # required
  expect_error(metadataColumns(id = c("a", "b")), "single")
  expect_silent(metadataColumns(package_id = NULL))                 # optional may be NULL
})

test_that(".as_metadata_columns accepts a schema, a named list, or NULL", {
  expect_identical(nautilus:::.as_metadata_columns(NULL)$id, "ID")
  s <- metadataColumns(tag_type = "type")
  expect_identical(nautilus:::.as_metadata_columns(s), s)
  fromlist <- nautilus:::.as_metadata_columns(list(tag_type = "type", package_id = "pid"))
  expect_equal(fromlist$tag_type, "type")
  expect_equal(fromlist$package_id, "pid")
  expect_error(nautilus:::.as_metadata_columns(list(bogus = "x")), "unknown field")
  expect_error(nautilus:::.as_metadata_columns(42), "metadataColumns")
})


test_that("metadataColumns accepts the exclude_sensors role (optional)", {
  expect_null(metadataColumns()$exclude_sensors)
  expect_equal(metadataColumns(exclude_sensors = "bad_sensors")$exclude_sensors, "bad_sensors")
})


test_that("metadataColumns accepts the axis_config role (optional)", {
  expect_null(metadataColumns()$axis_config)
  expect_equal(metadataColumns(axis_config = "cfg")$axis_config, "cfg")
})
