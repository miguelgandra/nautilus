# Fixture-based tests for importTagData().
# Covers the Increment-4 fixes:
#   I1 - crash on a default import (an undefined mapping variable; axis.mapping has since been removed)
#   I3 - result list corrupted when >= 2 folders are skipped
#   I4 - readline() must not block in non-interactive sessions

# ASCII column mapping so fixtures avoid non-ASCII header matching
.mapping <- data.frame(
  colname = c("dt", "ax", "ay", "az", "gx", "gy", "gz", "mx", "my", "mz", "depth", "temp"),
  sensor  = c("datetime", "ax", "ay", "az", "gx", "gy", "gz", "mx", "my", "mz", "depth", "temp"),
  units   = c("UTC", "g", "g", "g", "rad/s", "rad/s", "rad/s", "uT", "uT", "uT", "m", "C"),
  stringsAsFactors = FALSE
)

.meta <- function(ids) {
  data.frame(
    ID = ids, tag = "TestTag", type = "MS",
    deploy_date = as.POSIXct("2020-01-01 00:00:00", tz = "UTC"),
    deploy_lon = -25.0, deploy_lat = 38.0,
    stringsAsFactors = FALSE
  )
}

# create a folder tree; `valid` get a sensor CSV, `empty` get an empty CMD dir
.make_fixture <- function(valid, empty = character()) {
  root <- file.path(tempdir(), paste0("nautilus_test_", as.integer(stats::runif(1, 1, 1e7))))
  n <- 40
  dt <- seq(as.POSIXct("2020-01-01 00:00:00", tz = "UTC"), by = 1, length.out = n)
  for (id in valid) {
    dir.create(file.path(root, id, "CMD"), recursive = TRUE, showWarnings = FALSE)
    df <- data.frame(
      dt = format(dt, "%Y-%m-%d %H:%M:%S"),
      ax = stats::rnorm(n), ay = stats::rnorm(n), az = stats::rnorm(n),
      gx = stats::rnorm(n), gy = stats::rnorm(n), gz = stats::rnorm(n),
      mx = stats::rnorm(n), my = stats::rnorm(n), mz = stats::rnorm(n),
      depth = stats::runif(n, 0, 50), temp = stats::runif(n, 18, 22),
      stringsAsFactors = FALSE
    )
    data.table::fwrite(df, file.path(root, id, "CMD", "sensor.csv"))
  }
  for (id in empty) dir.create(file.path(root, id, "CMD"), recursive = TRUE, showWarnings = FALSE)
  root
}

test_that("importTagData imports a folder with default arguments (I1 regression)", {
  root <- .make_fixture("ID_01")
  on.exit(unlink(root, recursive = TRUE), add = TRUE)

  res <- NULL
  invisible(capture.output(
    res <- importTagData(
      data.folders = file.path(root, "ID_01"),
      import.mapping = .mapping,
      id.metadata = .meta("ID_01"),
      columns = metadataColumns(deploy_datetime = "deploy_date"),
      return.data = TRUE, verbose = FALSE
    )
  ))

  expect_type(res, "list")
  expect_named(res, "ID_01")
  dt <- res[["ID_01"]]
  expect_s3_class(dt$datetime, "POSIXct")
  expect_true(all(c("ID", "datetime", "ax", "az", "mz", "depth", "temp") %in% names(dt)))
  expect_equal(nrow(dt), 40)
  expect_identical(nautilus:::.getMeta(dt)$tag$model, "TestTag")
  # import is raw: an axis_mapping slot exists in metadata but nothing is applied
  expect_true("axis_mapping" %in% names(nautilus:::.getMeta(dt)))
  expect_false(nautilus:::.getMeta(dt)$axis_mapping$applied)
})

test_that("importTagData carries declared traits into meta$biometrics (import-and-store)", {
  root <- .make_fixture("ID_01"); on.exit(unlink(root, recursive = TRUE), add = TRUE)
  md <- cbind(.meta("ID_01"), sex = "F", length = 8.2, species = "R. typus", stringsAsFactors = FALSE)
  res <- NULL
  invisible(capture.output(
    res <- importTagData(file.path(root, "ID_01"), import.mapping = .mapping, id.metadata = md,
                         columns = metadataColumns(deploy_datetime = "deploy_date", traits = c("sex", "length", "species")),
                         return.data = TRUE, verbose = FALSE)))
  bio <- nautilus:::.getMeta(res[["ID_01"]])$biometrics
  expect_equal(bio$sex, "F")
  expect_equal(bio$length, 8.2)                          # numeric type preserved
  expect_equal(bio$species, "R. typus")
})

test_that("importTagData errors when a declared trait column is missing from id.metadata", {
  root <- .make_fixture("ID_01"); on.exit(unlink(root, recursive = TRUE), add = TRUE)
  expect_error(suppressMessages(importTagData(file.path(root, "ID_01"), import.mapping = .mapping,
    id.metadata = .meta("ID_01"), columns = metadataColumns(deploy_datetime = "deploy_date", traits = "sex"),
    return.data = TRUE, verbose = FALSE)), "sex", ignore.case = TRUE)
})

test_that("traits survive the qc -> nautilus_deployments -> importTagData path (regression)", {
  # the qc normalisation must NOT fold `traits` into the single-column role machinery (it crashed on a
  # multi-trait vector and renamed a single trait column to "traits", losing the name)
  root <- .make_fixture("ID_01"); on.exit(unlink(root, recursive = TRUE), add = TRUE)
  md  <- cbind(.meta("ID_01"), sex = factor("F", levels = c("F", "M")), length = 8.2)
  dep <- suppressMessages(qcDeploymentMetadata(md, columns = metadataColumns(deploy_datetime = "deploy_date",
                          traits = c("sex", "length")), verbose = FALSE))
  expect_true(all(c("sex", "length") %in% names(dep)))          # columns kept under their own names
  res <- NULL
  invisible(capture.output(res <- importTagData(file.path(root, "ID_01"), import.mapping = .mapping,
    id.metadata = dep, return.data = TRUE, verbose = FALSE)))
  bio <- nautilus:::.getMeta(res[["ID_01"]])$biometrics
  expect_equal(bio$sex, "F"); expect_true(is.character(bio$sex)); expect_equal(bio$length, 8.2)
})

test_that("importTagData returns nautilus_tag objects with populated metadata", {
  root <- .make_fixture("ID_01")
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  res <- NULL
  invisible(capture.output(suppressWarnings(suppressMessages(
    res <- importTagData(file.path(root, "ID_01"), import.mapping = .mapping,
                         id.metadata = .meta("ID_01"), columns = metadataColumns(deploy_datetime = "deploy_date"),
                         return.data = TRUE, verbose = FALSE)))))
  x <- res[["ID_01"]]
  expect_true(nautilus:::is_nautilus_tag(x))
  m <- nautilus:::.getMeta(x)
  expect_equal(m$id, "ID_01")
  expect_equal(m$tag$model, "TestTag")
  expect_equal(m$deployment$lon, -25)
  expect_equal(m$processing[[1]]$step, "importTagData")
})

test_that("importTagData is silent when verbose = FALSE", {
  root <- .make_fixture("ID_01")
  on.exit(unlink(root, recursive = TRUE), add = TRUE)

  out <- capture.output(suppressWarnings(suppressMessages(
    res <- importTagData(
      data.folders = file.path(root, "ID_01"),
      import.mapping = .mapping,
      id.metadata = .meta("ID_01"), columns = metadataColumns(deploy_datetime = "deploy_date"),
      return.data = TRUE, verbose = FALSE
    )
  )))
  expect_length(out, 0)
})

test_that("importTagData imports raw (no axis transform) and reads attachment.site.col", {
  root <- .make_fixture("ID_01")
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  meta <- .meta("ID_01"); meta$site <- "left_pectoral"

  res <- NULL
  invisible(capture.output(suppressWarnings(suppressMessages(
    res <- importTagData(file.path(root, "ID_01"), import.mapping = .mapping, id.metadata = meta,
                         columns = metadataColumns(deploy_datetime = "deploy_date", tag_type = "type",
                                               attachment_site = "site"),
                         return.data = TRUE, verbose = FALSE)))))
  x <- res[["ID_01"]]
  expect_false(nautilus:::.getMeta(x)$axis_mapping$applied)     # import is raw - no orientation applied
  expect_equal(nautilus:::.getMeta(x)$deployment$attachment_site, "left_pectoral")
})

test_that("importTagData drops the correct elements when >= 2 folders are skipped (I3) and does not hang non-interactively (I4)", {
  root <- .make_fixture(valid = c("ID_01", "ID_04"), empty = c("ID_02", "ID_03"))
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  folders <- file.path(root, c("ID_01", "ID_02", "ID_03", "ID_04"))

  res <- NULL
  invisible(capture.output(
    suppressWarnings(
      res <- importTagData(
        data.folders = folders,
        import.mapping = .mapping,
        id.metadata = .meta(c("ID_01", "ID_02", "ID_03", "ID_04")),
        columns = metadataColumns(deploy_datetime = "deploy_date"),
        return.data = TRUE, verbose = FALSE
      )
    )
  ))

  # only the two valid folders survive, and they keep their correct names
  expect_named(res, c("ID_01", "ID_04"))
  expect_identical(nautilus:::.getMeta(res[["ID_01"]])$id, "ID_01")
  expect_identical(nautilus:::.getMeta(res[["ID_04"]])$id, "ID_04")
})

test_that("compress controls saved-file size and validates input; default writes a readable rds", {
  root <- .make_fixture("ID_01")
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  out_dir <- file.path(root, "out"); dir.create(out_dir)

  imp <- function(...) invisible(capture.output(suppressWarnings(suppressMessages(
    importTagData(file.path(root, "ID_01"), import.mapping = .mapping,
                  id.metadata = .meta("ID_01"), columns = metadataColumns(deploy_datetime = "deploy_date"),
                  return.data = FALSE, output.dir = out_dir, verbose = FALSE, ...)))))

  imp(compress = FALSE, output.suffix = "_u")
  imp(compress = TRUE,  output.suffix = "_z")
  fu <- file.path(out_dir, "ID_01_u.rds"); fz <- file.path(out_dir, "ID_01_z.rds")
  expect_true(file.exists(fu) && file.exists(fz))
  expect_gt(file.size(fu), file.size(fz))               # uncompressed is larger
  expect_s3_class(readRDS(fu), "nautilus_tag")          # default (uncompressed) is readable
  expect_equal(nrow(readRDS(fu)), nrow(readRDS(fz)))    # identical content either way

  expect_error(
    importTagData(file.path(root, "ID_01"), import.mapping = .mapping, id.metadata = .meta("ID_01"),
                  columns = metadataColumns(deploy_datetime = "deploy_date"),
                  output.dir = out_dir, compress = "lz4", verbose = FALSE),
    "compress")
})

test_that("verbose = 2 adds per-individual sub-headers + grouped detail; tally summarises skips", {
  root <- .make_fixture(valid = c("ID_01", "ID_02"), empty = "ID_03")
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  folders <- file.path(root, c("ID_01", "ID_02", "ID_03"))
  meta <- .meta(c("ID_01", "ID_02", "ID_03"))

  # cli_fmt() captures cli output reliably; plain capture.output() does not once testthat
  # is managing the output streams.
  grab <- function(v) paste(cli::cli_fmt(suppressWarnings(
    importTagData(folders, import.mapping = .mapping, id.metadata = meta,
                  columns = metadataColumns(deploy_datetime = "deploy_date"),
                  return.data = TRUE, verbose = v))), collapse = "\n")

  d2 <- grab(2); d1 <- grab(1)
  expect_match(d2, "ID_01 \\(1/3\\)")             # per-individual sub-header with (i/n) counter
  expect_match(d2, "sensors:")                     # new grouped detail
  expect_match(d2, "span:")
  expect_match(d2, "SUMMARY")                       # final summary block has its own marker
  expect_match(d2, "Issues")                        # consolidated end-of-run issues block
  expect_match(d2, "skipping 1 folder \\(no sensor file found\\)")   # concise, present-tense pre-flight
  expect_match(d2, "ID_03")                         # affected ID listed
  expect_false(grepl("sensors:", d1))               # detail is level-2 only
  expect_match(d1, "skipping 1 folder \\(no sensor file found\\)")   # issues block also shown at the normal level
})

# ---- temperature blacklisting (electronics temp vs water temp) -----------------------------------

# minimal active mapping mirroring the production defaults' reliable temp sources
.temp_am <- data.frame(
  colname = c("Accelerometer X [m/s2]", "Temperature (depth) [\u00b0C]", "Depth (200bar) 2 [\u00b0C]"),
  sensor  = c("ax", "temp", "temp"),
  units   = c("m/s2", "C", "C"), stringsAsFactors = FALSE)
.temp_amn <- nautilus:::.normalizeHeader(.temp_am$colname)
.temp_bln <- nautilus:::.normalizeHeader(c("Temp. (magnet.) [\u00b0C]", "Temperature (imu) [\u00b0C]"))
.bfm <- function(header, am = .temp_am, amn = .temp_amn, bln = .temp_bln)
  nautilus:::.buildFileMapping(header, am, amn, bln)

test_that(".buildFileMapping maps a reliable water-temperature column (temp_status 'ok')", {
  fm <- .bfm(c("Accelerometer X [m/s2]", "Temperature (depth) [\u00b0C]"))
  expect_true("temp" %in% fm$sensor_name_out)
  expect_equal(attr(fm, "temp_status"), "ok")
})

test_that(".buildFileMapping blacklists an electronics-only temperature (temp_status 'blacklisted_only')", {
  fm <- .bfm(c("Accelerometer X [m/s2]", "Temp. (magnet.) [\u00b0C]"))
  expect_false("temp" %in% fm$sensor_name_out)              # never auto-mapped
  expect_equal(attr(fm, "temp_status"), "blacklisted_only")
  expect_match(attr(fm, "temp_blacklisted_present"), "magnet")
})

test_that(".buildFileMapping prefers the reliable source when both are present", {
  fm <- .bfm(c("Accelerometer X [m/s2]", "Temperature (imu) [\u00b0C]", "Depth (200bar) 2 [\u00b0C]"))
  expect_equal(attr(fm, "temp_status"), "ok")
  expect_equal(fm$colname_in_csv[fm$sensor_name_out == "temp"], "Depth (200bar) 2 [\u00b0C]")
})

test_that(".buildFileMapping honours an explicit user override of a blacklisted header (temp_status 'override')", {
  am  <- rbind(data.frame(colname = "Temp. (magnet.) [\u00b0C]", sensor = "temp", units = "C",
                          stringsAsFactors = FALSE), .temp_am)
  amn <- nautilus:::.normalizeHeader(am$colname)
  fm  <- .bfm(c("Accelerometer X [m/s2]", "Temp. (magnet.) [\u00b0C]"), am = am, amn = amn)
  expect_true("temp" %in% fm$sensor_name_out)               # explicit intent wins
  expect_equal(attr(fm, "temp_status"), "override")
})

test_that(".buildFileMapping reports no temperature when none is present (temp_status 'none')", {
  fm <- .bfm("Accelerometer X [m/s2]")
  expect_false("temp" %in% fm$sensor_name_out)
  expect_equal(attr(fm, "temp_status"), "none")
  expect_length(attr(fm, "temp_blacklisted_present"), 0)
})

# integration: a fixture whose only temperature column is the magnetometer (electronics) sensor
.make_temp_fixture <- function(id, temp_header) {
  root <- file.path(tempdir(), paste0("nautilus_temp_", as.integer(stats::runif(1, 1, 1e7))))
  n <- 40
  dt <- seq(as.POSIXct("2020-01-01 00:00:00", tz = "UTC"), by = 1, length.out = n)
  dir.create(file.path(root, id, "CMD"), recursive = TRUE, showWarnings = FALSE)
  df <- data.frame(dt = format(dt, "%Y-%m-%d %H:%M:%S"),
                   ax = stats::rnorm(n), ay = stats::rnorm(n), az = stats::rnorm(n),
                   depth = stats::runif(n, 0, 50), check.names = FALSE)
  if (!is.null(temp_header)) df[[temp_header]] <- stats::runif(n, 18, 22)
  data.table::fwrite(df, file.path(root, id, "CMD", "sensor.csv"))
  root
}

# a minimal user mapping for datetime + the IMU/depth channels (defaults still appended after it,
# so the temperature blacklist from the package defaults remains active)
.temp_user_map <- data.frame(
  colname = c("dt", "ax", "ay", "az", "depth"),
  sensor  = c("datetime", "ax", "ay", "az", "depth"),
  units   = c("UTC", "g", "g", "g", "m"), stringsAsFactors = FALSE)

.import_temp <- function(root, id, mapping = .temp_user_map) {
  out <- NULL; w <- character(0)
  withCallingHandlers(
    invisible(capture.output(suppressMessages(
      out <- importTagData(file.path(root, id), import.mapping = mapping,
                           id.metadata = .meta(id), columns = metadataColumns(deploy_datetime = "deploy_date"),
                           return.data = TRUE, verbose = FALSE)))),
    warning = function(wn) { w <<- c(w, conditionMessage(wn)); invokeRestart("muffleWarning") })
  list(data = out[[id]], warnings = w)
}

test_that("importTagData leaves temp unset and warns when only an electronics temp is present", {
  root <- .make_temp_fixture("ID_T1", "Temp. (magnet.) [\u00b0C]")
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  out <- .import_temp(root, "ID_T1")
  expect_false("temp" %in% names(out$data))                 # missing-sensor convention: column absent
  # verbose == 0 -> aggregated deferred warning carries the temperature tally
  expect_true(any(grepl("discarded, electronics sensor only", out$warnings)))
})

test_that("importTagData keeps a reliable water temp and does not warn", {
  root <- .make_temp_fixture("ID_T2", "Temperature (depth) [\u00b0C]")
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  out <- .import_temp(root, "ID_T2")
  expect_true("temp" %in% names(out$data))
  expect_false(any(grepl("electronics sensor", out$warnings)))
})

test_that("importTagData honours an explicit override of a blacklisted temp but warns", {
  root <- .make_temp_fixture("ID_T3", "Temp. (magnet.) [\u00b0C]")
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  override <- rbind(.temp_user_map,
                    data.frame(colname = "Temp. (magnet.) [\u00b0C]", sensor = "temp", units = "C",
                               stringsAsFactors = FALSE))
  out <- .import_temp(root, "ID_T3", mapping = override)
  expect_true("temp" %in% names(out$data))                  # explicit intent wins
  expect_true(any(grepl("using an overridden electronics sensor", out$warnings)))
})

test_that("at verbose >= 1 issues are cli-only (inline + tally, no base-R warning)", {
  root <- .make_temp_fixture("ID_T1", "Temp. (magnet.) [\u00b0C]")
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  msgs <- character(0)
  txt <- paste(cli::cli_fmt(withCallingHandlers(
    suppressMessages(importTagData(file.path(root, "ID_T1"), import.mapping = .temp_user_map,
 id.metadata = .meta("ID_T1"),
                                   columns = metadataColumns(deploy_datetime = "deploy_date"),
                                   return.data = TRUE, verbose = 2)),
    warning = function(w) { msgs <<- c(msgs, conditionMessage(w)); invokeRestart("muffleWarning") })),
    collapse = "\n")
  expect_match(txt, "temp: electronics sensor only \\(Temp\\. \\(magnet\\.\\)\\)")  # inline bullet (lvl 2)
  expect_match(txt, "Temperature: 1 discarded, electronics sensor only")           # echoed in the tally
  expect_length(msgs, 0)                                                            # no base-R warnings
})

test_that("a folder absent from id.metadata is a hard abort", {
  root <- .make_fixture("ID_01")                              # ID_01 valid (tag TestTag)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  dir.create(file.path(root, "ID_X", "CMD"), recursive = TRUE, showWarnings = FALSE)  # exists, not in metadata
  expect_error(
    suppressWarnings(suppressMessages(importTagData(file.path(root, c("ID_01", "ID_X")), import.mapping = .mapping,
                                     id.metadata = .meta("ID_01"),
                                     columns = metadataColumns(deploy_datetime = "deploy_date"),
                                     return.data = TRUE, verbose = 0))),
    "not found in")                                           # hard abort: ID_X absent from id.metadata
})


# ---- metadata QC integration (Phase 1) ---------------------------------------------------------

test_that("importTagData consumes a QC'd nautilus_deployments directly (no columns arg)", {
  root <- .make_fixture("ID_01")
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  meta <- qcDeploymentMetadata(.meta("ID_01"),
                               columns = metadataColumns(deploy_datetime = "deploy_date"),
                               verbose = 0)
  expect_s3_class(meta, "nautilus_deployments")
  expect_true(attr(meta, "nautilus.qc")$passed)

  res <- NULL
  invisible(capture.output(
    res <- importTagData(data.folders = file.path(root, "ID_01"),
                         import.mapping = .mapping, id.metadata = meta,
                         return.data = TRUE, verbose = FALSE)))
  expect_named(res, "ID_01")
  expect_equal(nrow(res[["ID_01"]]), 40)
})

test_that("importTagData rejects a FAILED nautilus_deployments before importing", {
  root <- .make_fixture("ID_01")
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  bad <- .meta("ID_01"); bad$deploy_lon <- 200            # out-of-range -> QC error
  meta <- suppressWarnings(qcDeploymentMetadata(bad,
            columns = metadataColumns(deploy_datetime = "deploy_date"), verbose = 0))
  expect_false(attr(meta, "nautilus.qc")$passed)
  expect_error(
    suppressWarnings(importTagData(data.folders = file.path(root, "ID_01"),
                     import.mapping = .mapping, id.metadata = meta,
                     return.data = TRUE, verbose = FALSE)),
    "failed metadata QC")
})

test_that("importTagData runs an inline QC guard on un-QC'd metadata and aborts on errors", {
  root <- .make_fixture("ID_01")
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  bad <- .meta("ID_01"); bad$deploy_lon <- 200            # out-of-range -> guard error
  expect_error(
    suppressWarnings(importTagData(data.folders = file.path(root, "ID_01"),
                     import.mapping = .mapping, id.metadata = bad,
                     columns = metadataColumns(deploy_datetime = "deploy_date"),
                     return.data = TRUE, verbose = FALSE)),
    "Metadata QC")
})


test_that("importTagData drops channels listed in the exclude_sensors metadata column", {
  root <- .make_fixture("ID_01")
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  meta <- .meta("ID_01"); meta$bad <- "mag"
  res <- NULL
  invisible(capture.output(
    res <- importTagData(data.folders = file.path(root, "ID_01"), import.mapping = .mapping, id.metadata = meta,
             columns = metadataColumns(deploy_datetime = "deploy_date", exclude_sensors = "bad"),
             return.data = TRUE, verbose = FALSE)))
  dt <- res[["ID_01"]]
  expect_false(any(c("mx", "my", "mz") %in% names(dt)))     # mag dropped -> absent
  expect_true(all(c("ax", "az", "depth") %in% names(dt)))   # other channels kept
  m <- nautilus:::.getMeta(dt)
  expect_setequal(m$sensors$excluded, c("mx", "my", "mz"))  # recorded in provenance
  expect_false(any(c("mx", "my", "mz") %in% m$sensors$present))
})


test_that("importTagData carries axis_config onto the tag metadata", {
  root <- .make_fixture("ID_01")
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  meta <- .meta("ID_01"); meta$cfg <- "CATS Camera"
  res <- NULL
  invisible(capture.output(
    res <- importTagData(data.folders = file.path(root, "ID_01"), import.mapping = .mapping, id.metadata = meta,
             columns = metadataColumns(deploy_datetime = "deploy_date", axis_config = "cfg"),
             return.data = TRUE, verbose = FALSE)))
  expect_equal(nautilus:::.getMeta(res[["ID_01"]])$tag$axis_config, "CATS Camera")
})

test_that(".readDryAncillary parses a WC archive 'Dry' column into a transition-encoded entry", {
  f <- tempfile(fileext = ".csv"); on.exit(unlink(f), add = TRUE)
  t  <- seq(as.POSIXct("2022-09-17 13:00:00", tz = "UTC"), by = 1, length.out = 30)
  dry <- ifelse(seq_len(30) >= 6 & seq_len(30) <= 15, 1L, 0L)               # a 10-sample dry stretch
  arch <- data.frame(Time = c("13:00:00 17-Sep-2022", format(t, "%H:%M:%S %d-%b-%Y")),  # +1 config row
                     Depth = c(NA, seq(0, 20, length.out = 30)),
                     Dry   = c("", as.character(dry)),                       # config row has a blank Dry
                     stringsAsFactors = FALSE)
  data.table::fwrite(arch, f)
  anc <- nautilus:::.readDryAncillary(f)
  expect_false(is.null(anc))
  expect_equal(anc$encoding, "transitions")
  expect_named(anc$data, c("datetime", "dry"))
  expect_s3_class(anc$data$datetime, "POSIXct")
  expect_lt(nrow(anc$data), 31L)                                            # transition-compressed
  iv <- nautilus:::.dryIntervals(anc$data, end_time = max(t) + 1, min.dry.duration = 1)
  expect_equal(nrow(iv), 1L)                                                # the dry stretch recovered
  expect_null(nautilus:::.readDryAncillary(NA))                            # absent -> NULL
  f2 <- tempfile(fileext = ".csv"); on.exit(unlink(f2), add = TRUE)
  data.table::fwrite(data.frame(Time = "x", Depth = 1), f2)
  expect_null(nautilus:::.readDryAncillary(f2))                            # no Dry column -> NULL
})

test_that("importTagData reads the WC 'Dry' archive into ancillary$dry", {
  root <- .make_fixture("ID_01")
  wc <- file.path(root, "ID_01", "WC"); dir.create(wc, recursive = TRUE, showWarnings = FALSE)
  t <- seq(as.POSIXct("2020-01-01 00:00:00", tz = "UTC"), by = 1, length.out = 40)  # aligns to sensor window
  dry <- ifelse(seq_len(40) >= 10 & seq_len(40) <= 20, 1L, 0L)
  arch <- data.frame(Time = c("00:00:00 01-Jan-2020", format(t, "%H:%M:%S %d-%b-%Y")),
                     Depth = c(NA, seq(0, 50, length.out = 40)),
                     Dry   = c("", as.character(dry)),
                     Events = c("Conductivity Threshold: 80", rep("", 40)),
                     stringsAsFactors = FALSE)
  data.table::fwrite(arch, file.path(wc, "out-Archive.csv"))
  res <- NULL
  # this fixture's WC archive cannot support clock alignment (a 39 s overlap, or no depth channel), so
  # the aligner abstains and - since that abstention now reaches the consolidated issues - a deferred
  # warning is raised at verbose = 0. Expected here; the abstention itself is tested on its own below.
  invisible(capture.output(suppressWarnings(
    res <- importTagData(file.path(root, "ID_01"), import.mapping = .mapping, id.metadata = .meta("ID_01"),
                         columns = metadataColumns(deploy_datetime = "deploy_date"),
                         wc.subdirectory = "WC", return.data = TRUE, verbose = FALSE))))
  anc <- nautilus:::.getMeta(res[["ID_01"]])$ancillary$dry
  expect_false(is.null(anc))
  expect_equal(anc$encoding, "transitions")
  expect_true(any(anc$data$dry))                                            # the dry stretch is present
  expect_false("Dry" %in% names(res[["ID_01"]]))                           # NOT carried as a measurand column
})

test_that("importTagData reads the complete WC Locations record into ancillary$positions (no columns)", {
  root <- .make_fixture("ID_01")                                           # sensor window: 2020-01-01 00:00:00 + 0..39 s
  wc <- file.path(root, "ID_01", "WC"); dir.create(wc, recursive = TRUE, showWarnings = FALSE)
  loc <- data.frame(Ptt = 1,
                    Date = c("00:00:10 01-Jan-2020", "00:00:30 01-Jan-2020", "02:00:00 01-Jan-2020"),
                    Type = c("FastGPS", "User", "FastGPS"), Quality = c("7", "", "9"),
                    Latitude = c(38.1, 38.2, 38.9), Longitude = c(-25.1, -25.2, -25.9), stringsAsFactors = FALSE)
  data.table::fwrite(loc, file.path(wc, "ID_01-Locations.csv"))
  res <- NULL
  # this fixture's WC archive cannot support clock alignment (a 39 s overlap, or no depth channel), so
  # the aligner abstains and - since that abstention now reaches the consolidated issues - a deferred
  # warning is raised at verbose = 0. Expected here; the abstention itself is tested on its own below.
  invisible(capture.output(suppressWarnings(
    res <- importTagData(file.path(root, "ID_01"), import.mapping = .mapping, id.metadata = .meta("ID_01"),
                         columns = metadataColumns(deploy_datetime = "deploy_date"),
                         wc.subdirectory = "WC", return.data = TRUE, verbose = FALSE))))
  pos <- nautilus:::.getMeta(res[["ID_01"]])$ancillary$positions
  expect_false(is.null(pos))
  expect_equal(nrow(pos$data), 3L)                                         # complete record, incl. the out-of-window 02:00 fix
  expect_true(all(c("FastGPS", "User") %in% pos$data$type))
  expect_false(any(c("position_type", "lon", "lat") %in% names(res[["ID_01"]])))   # NOT carried as columns
})

test_that("importTagData fails loudly on empty input, not a silent empty import", {
  # (a) data.folders resolved to character(0) - the mistyped-list.files() pattern from the bug report
  expect_error(importTagData(data.folders = character(0), id.metadata = .meta("ID_01"),
                             columns = metadataColumns(deploy_datetime = "deploy_date"), verbose = FALSE),
               "empty", ignore.case = TRUE)

  # (b) folders exist but hold NO sensor CSV in any of them (e.g. a mistyped sensor.subdirectory) ->
  #     previously imported 0 of N and returned an empty list / invisible NULL; now a clear abort
  root <- .make_fixture(valid = character(), empty = c("ID_01", "ID_02"))   # empty CMD dirs, no CSVs
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  expect_error(
    suppressWarnings(importTagData(data.folders = file.path(root, c("ID_01", "ID_02")),
                                   import.mapping = .mapping, id.metadata = .meta(c("ID_01", "ID_02")),
                                   columns = metadataColumns(deploy_datetime = "deploy_date"),
                                   return.data = TRUE, verbose = FALSE)),
    "No readable", ignore.case = TRUE)

  # (c) a mistyped sensor.subdirectory on otherwise-valid folders aborts the same way
  root2 <- .make_fixture(valid = c("ID_01", "ID_02"))                       # real CSVs, but under "CMD"
  on.exit(unlink(root2, recursive = TRUE), add = TRUE)
  expect_error(
    suppressWarnings(importTagData(data.folders = file.path(root2, c("ID_01", "ID_02")),
                                   sensor.subdirectory = "CMDX",             # typo -> no CSV found anywhere
                                   import.mapping = .mapping, id.metadata = .meta(c("ID_01", "ID_02")),
                                   columns = metadataColumns(deploy_datetime = "deploy_date"),
                                   return.data = TRUE, verbose = FALSE)),
    "No readable", ignore.case = TRUE)
})


test_that("clock-alignment abstentions reach the final SUMMARY, not only the inline block", {
  # The inline amber line is emitted at verbose = 2 only, and scrolls away in a long run. "Which tags
  # failed to align?" is exactly the question asked at the end, so the abstention is collected at every
  # verbosity and surfaced in the consolidated issues (as deferred warnings at verbose = 0).
  root <- .make_fixture("ID_01"); on.exit(unlink(root, recursive = TRUE), add = TRUE)
  wc <- file.path(root, "ID_01", "WC"); dir.create(wc, recursive = TRUE, showWarnings = FALSE)
  t <- seq(as.POSIXct("2020-01-01 00:00:00", tz = "UTC"), by = 1, length.out = 40)
  # a FLAT reference depth: nothing for the cross-correlation to lock onto, so the aligner abstains
  arch <- data.frame(Time  = c("00:00:00 01-Jan-2020", format(t, "%H:%M:%S %d-%b-%Y")),
                     Depth = c(NA, rep(10, 40)),
                     Dry   = c("", rep("0", 40)),
                     Events = c("Conductivity Threshold: 80", rep("", 40)),
                     stringsAsFactors = FALSE)
  data.table::fwrite(arch, file.path(wc, "out-Archive.csv"))

  w <- character(0)
  invisible(capture.output(withCallingHandlers(
    importTagData(file.path(root, "ID_01"), import.mapping = .mapping, id.metadata = .meta("ID_01"),
                  columns = metadataColumns(deploy_datetime = "deploy_date"),
                  wc.subdirectory = "WC", return.data = TRUE, verbose = 0),
    warning = function(cnd) { w <<- c(w, conditionMessage(cnd)); invokeRestart("muffleWarning") })))

  hit <- grep("Clock alignment", w, value = TRUE)
  expect_length(hit, 1L)
  expect_match(hit, "not applied")
  expect_match(hit, "ID_01", fixed = TRUE)          # names the affected deployment, not just a count
})

test_that("the calibration sidecar line carries no redundant parenthetical", {
  # cosmetic: "(stored in metadata)" was implicit and only lengthened the line
  src <- deparse(nautilus:::.reportCalibration)
  expect_false(any(grepl("stored in metadata", src, fixed = TRUE)))
  expect_true(any(grepl("calibration sidecar", src, fixed = TRUE)))
})
