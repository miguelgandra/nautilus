.identity_data <- function(ids = NULL) {
  d <- data.frame(datetime = as.POSIXct("2025-01-01", tz = "UTC") + 0:3, depth = 1:4)
  if (!is.null(ids)) d$ID <- rep(ids, length.out = 4L)
  d
}
.identity_build <- function(metadata = NULL, ...) {
  buildTagData(.identity_data(), id = "D1", metadata = metadata, verbose = FALSE, ...)
}
.identity_raw_metadata <- function() {
  data.frame(ID = "D1", tag = "Test", tagging_date = as.POSIXct("2025-01-01", tz = "UTC"),
             deploy_lon = -25.2, deploy_lat = 37.1)
}
.identity_import <- function(metadata, columns = metadataColumns(), output.dir = NULL) {
  root <- tempfile(); dir.create(file.path(root, "D1", "CMD"), recursive = TRUE)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  data.table::fwrite(data.frame(dt = c("2025-01-01 00:00:00", "2025-01-01 00:00:01"), depth = 1:2),
                     file.path(root, "D1", "CMD", "sensor.csv"))
  importTagData(file.path(root, "D1"), metadata = metadata, columns = columns,
    import.mapping = data.frame(colname = c("dt", "depth"), sensor = c("datetime", "depth"), units = c("UTC", "m")),
    output.dir = output.dir, verbose = FALSE)
}

test_that("constructor identity sources must agree even with an explicit ID", {
  mixed <- .identity_data(c("D1", "D2"))
  for (id in list(NULL, "D3"))
    expect_error(buildTagData(mixed, id = id, verbose = FALSE), "several IDs")
  expect_error(buildTagData(mixed, metadata = list(id = "D3"), verbose = FALSE), "several IDs")
  expect_error(buildTagData(.identity_data("D1"), id = "D3", verbose = FALSE), "not match")
  expect_error(buildTagData(.identity_data("D1"), metadata = list(id = "D3"), verbose = FALSE), "not match")
  expect_error(.identity_build(list(id = "D3")), "not match")
  expect_error(.identity_build(list(id = "D1", ID = "D3")), "not match")
  # Renaming a column cannot hide identities that were present on the raw input.
  expect_error(buildTagData(mixed[, c("ID", "datetime")], id = "D3",
                            sensor.mapping = c(depth = "ID"), verbose = FALSE), "several IDs")
  expect_error(buildTagData(.identity_data(), metadata = list(identifier = "D3"), verbose = FALSE), "No deployment")
  tag <- .identity_build()
  data.table::set(tag, j = "ID", value = rep("D3", nrow(tag)))
  expect_error(buildTagData(tag, id = "D3", verbose = FALSE), "not match")
})

test_that("deployment identity cannot be recycled, truncated, or filled over missing row IDs", {
  for (id in list(c("D1", "D2"), character(), NA_character_, "", "  ", TRUE))
    expect_error(buildTagData(.identity_data(), id = id, verbose = FALSE))
  for (ids in list(c("D1", NA), c("D1", ""), rep(NA_character_, 4)))
    expect_error(buildTagData(.identity_data(ids), id = "D1", verbose = FALSE), "one non-missing")
  d <- .identity_data(factor(" 001 "))
  before <- serialize(d, NULL)
  tag <- buildTagData(d, metadata = list(ID = factor("001"), animal_id = factor(" A01 ")),
                       verbose = FALSE)
  expect_identical(tag$ID, rep("001", 4))
  expect_identical(getTagMetadata(tag)$animal_id, "A01")
  expect_identical(serialize(d, NULL), before)
  expect_identical(.identity_build(list(id = NA_character_))$ID, rep("D1", 4))
})

test_that("metadata rows require unique names and scalar fields, and traits must exist", {
  for (md in list(list("value"), list(deploy_lon = c(-25, -26)),
                  list(deploy_lon = list(-25)), list(a = 1, a = 2), list(a = matrix(1))))
    expect_error(.identity_build(md))
  expect_error(.identity_build(traits = "absent"), "Trait fields")
  expect_error(.identity_build(list(sex = "F"), traits = NA_character_), "traits")
  expect_error(.identity_build(list(deploy_datetime = "2025-01-01")), "POSIXct")
})

test_that("factor, text and numeric coordinates share one geographic meaning", {
  for (cast in list(identity, as.character, factor)) {
    md <- list(deploy_lon = cast(-25.2), deploy_lat = cast(37.1),
               popup_lon = cast(-24.9), popup_lat = cast(37.3),
               package_id = factor("007"), logger_id = factor(" L01 "),
               paddle_wheel = 1, sex = factor("F"))
    before <- serialize(md, NULL)
    m <- getTagMetadata(.identity_build(md, traits = "sex"))
    expect_equal(c(m$deployment$lon, m$deployment$lat), c(-25.2, 37.1))
    expect_equal(c(m$deployment$popup_lon, m$deployment$popup_lat), c(-24.9, 37.3))
    expect_identical(m$tag$package_id, "007")
    expect_identical(m$tag$logger_id, "L01")
    expect_identical(m$tag$paddle_wheel, TRUE)
    expect_identical(m$biometrics$sex, "F")
    expect_identical(serialize(md, NULL), before)
  }
  expect_s3_class(.identity_build(), "nautilus_tag")
  m <- getTagMetadata(.identity_build(list(deploy_lon = NA_real_, deploy_lat = NA_real_)))
  expect_true(is.na(m$deployment$lon))
})

test_that("malformed and incomplete locations fail construction and are reported by QC", {
  for (patch in list(list(deploy_lon = "west", deploy_lat = 37), list(deploy_lon = 181, deploy_lat = 37),
                     list(deploy_lon = -25), list(deploy_lon = -25, deploy_lat = NA_real_),
                     list(popup_lon = -25, popup_lat = 91), list(popup_lon = Inf, popup_lat = 37),
                     list(deploy_lon = NaN, deploy_lat = 37))) {
    expect_error(.identity_build(patch), "Invalid deployment metadata")
    md <- .identity_raw_metadata()
    for (nm in names(patch)) md[[nm]] <- patch[[nm]]
    if (identical(names(patch), "deploy_lon")) md$deploy_lat <- NA_real_
    cols <- if (any(grepl("popup", names(patch)))) metadataColumns(popup_lon = "popup_lon", popup_lat = "popup_lat") else metadataColumns()
    qc <- suppressWarnings(checkDeploymentMetadata(md, columns = cols, verbose = FALSE))
    expect_false(attr(qc, "nautilus.qc")$passed)
    expect_gt(nrow(issues(qc, severity = "error")), 0)
  }
})

test_that("event metadata keeps absolute instants and enforces chronology", {
  deploy <- as.POSIXct("2025-07-01 12:00:00", tz = "Europe/Lisbon")
  md <- list(deploy_datetime = deploy, popup_datetime = deploy + 10, recovery_datetime = deploy + 20)
  tag <- .identity_build(md)
  m <- getTagMetadata(tag)
  expect_identical(m$deployment$datetime, deploy)
  expect_identical(m$deployment$popup_datetime, deploy + 10)
  expect_identical(m$deployment$recovery_datetime, deploy + 20)
  for (field in c("popup_datetime", "recovery_datetime")) {
    bad <- md; bad[[field]] <- deploy - 1
    expect_error(.identity_build(bad), "precedes deployment")
    bad[[field]] <- .POSIXct(Inf, "UTC")
    expect_error(.identity_build(bad), "finite POSIXct")
  }
  file <- tempfile(fileext = ".rds"); on.exit(unlink(file), add = TRUE)
  saveRDS(tag, file)
  expect_identical(getTagMetadata(file)$deployment, m$deployment)
  s <- summarizeTagData(tag, metadata = "recovery_datetime", verbose = FALSE)
  expect_equal(as.numeric(s$recovery_datetime), as.numeric(deploy + 20))
})

test_that("plain and QC-approved imports use the same normalized values and retain recovery", {
  md <- .identity_raw_metadata()
  md$ID <- factor(" D1 "); md$deploy_lon <- factor("-25.2"); md$deploy_lat <- factor("37.1")
  md$recovery <- md$tagging_date + 100
  cols <- metadataColumns(recovery_datetime = "recovery")
  before <- serialize(md, NULL)
  qc <- checkDeploymentMetadata(md, columns = cols, verbose = FALSE)
  a <- .identity_import(md, cols)[[1]]
  b <- .identity_import(qc)[[1]]
  for (tag in list(a, b)) {
    m <- getTagMetadata(tag)
    expect_identical(m$id, "D1")
    expect_equal(c(m$deployment$lon, m$deployment$lat), c(-25.2, 37.1))
    expect_equal(m$deployment$recovery_datetime, md$recovery)
  }
  expect_identical(getTagMetadata(a)$deployment, getTagMetadata(b)$deployment)
  expect_identical(serialize(md, NULL), before)
})

test_that("import checks current values and normalization failures before writing data", {
  good <- .identity_raw_metadata()
  bad <- good; bad$deploy_lon <- "west"
  expect_error(.identity_import(bad), "missing deployment info|Metadata QC")
  bad <- good; bad$popup <- good$tagging_date - 1
  expect_error(.identity_import(bad, metadataColumns(popup_datetime = "popup")), "Metadata QC")
  qc <- checkDeploymentMetadata(good, verbose = FALSE)
  qc$deploy_lon <- 220
  expect_true(attr(qc, "nautilus.qc")$passed) # a stale stamp is not authorization to skip validation
  output <- tempfile(); dir.create(output); on.exit(unlink(output, recursive = TRUE), add = TRUE)
  expect_error(.identity_import(qc, output.dir = output), "Metadata QC")
  expect_length(list.files(output), 0L)
  failed <- suppressWarnings(checkDeploymentMetadata(transform(good, deploy_lon = 220), verbose = FALSE))
  expect_error(buildTagData(.identity_data(), metadata = failed, verbose = FALSE), "failed metadata QC")
})

test_that("metadata table updates normalize matching keys without altering caller tables", {
  tag <- .identity_build()
  update <- data.frame(ID = factor(" D1 "), sex = factor("F"))
  before <- serialize(update, NULL)
  out <- updateTagMetadata(tag, update, columns = metadataColumns(traits = "sex"), verbose = FALSE)
  expect_identical(getTagMetadata(out)$biometrics$sex, "F")
  expect_identical(serialize(update, NULL), before)
  expect_identical(getTagMetadata(updateTagMetadata(tag, list(animal_id = " A01 "), verbose = FALSE))$animal_id, "A01")
  duplicate <- data.frame(ID = c("D1", " D1 "), sex = "F")
  expect_error(updateTagMetadata(tag, duplicate, columns = metadataColumns(traits = "sex"), verbose = FALSE), "unique")
})


test_that("QC reports impossible optional events and missing required positions", {
  md <- .identity_raw_metadata()
  md$popup <- md$tagging_date - 1
  qc <- suppressWarnings(checkDeploymentMetadata(md, columns = metadataColumns(popup_datetime = "popup"), verbose = FALSE))
  expect_true("popup_before_deploy" %in% issues(qc)$check)
  md$popup <- .POSIXct(Inf, "UTC")
  qc <- suppressWarnings(checkDeploymentMetadata(md, columns = metadataColumns(popup_datetime = "popup"), verbose = FALSE))
  expect_true("invalid_datetime" %in% issues(qc)$check)
  md$deploy_lon <- md$deploy_lat <- NA_real_
  qc <- suppressWarnings(checkDeploymentMetadata(md, verbose = FALSE))
  expect_true("missing_location" %in% issues(qc)$check)
})
