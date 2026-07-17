# Tests for importTagData(format = "auto"): per-reader detection and its fail-loud resolution.
#
# Fixtures are written in the REAL CEiiA dialect, not the ASCII shorthand the other import tests use. That
# is not incidental: detection resolves headers through the BUILT-IN mappings only, so a fixture whose
# header resolves only via `import.mapping` (like `.make_fixture()`'s `dt,ax,ay,...`) is invisible to a
# detector by design. The CEiiA mapping rows happen to be pure ASCII ("Date", "Ax (g)", "Depth (m)"), which
# keeps this file ASCII while still exercising a real dialect; the Latin-1 CATS dialect is covered too, via
# the intToUtf8 pattern from test-headerMatching.R.

sup2 <- intToUtf8(0x00B2)   # superscript two: real CATS headers write "Accelerometer X [m/s2]" with it

# a stock CEiiA-dialect deployment: header resolves through the built-in mappings
.ceiia_fixture <- function(root = tempfile(), id = "CE_01", sub = "CMD", n = 40L) {
  dir.create(file.path(root, id, sub), recursive = TRUE, showWarnings = FALSE)
  dt <- seq(as.POSIXct("2020-01-01", tz = "UTC"), by = 1, length.out = n)
  df <- data.frame(`Date` = format(dt, "%Y-%m-%d %H:%M:%S"),
                   `Ax (g)` = stats::rnorm(n), `Ay (g)` = stats::rnorm(n), `Az (g)` = stats::rnorm(n),
                   `Depth (m)` = stats::runif(n, 0, 50),
                   check.names = FALSE, stringsAsFactors = FALSE)
  data.table::fwrite(df, file.path(root, id, sub, "sensor.csv"))
  list(root = root, folder = file.path(root, id), id = id)
}

# write one CSV with an arbitrary header under <root>/<id>/CMD.
# fwrite, not writeLines: tests run under a C locale, where writeLines renders a non-ASCII header as the
# literal text "<U+00B2>" and the fixture silently stops being the dialect it claims to be.
.csv_fixture <- function(header, id = "X_01", root = tempfile(), file = "f.csv") {
  dir.create(file.path(root, id, "CMD"), recursive = TRUE, showWarnings = FALSE)
  df <- as.data.frame(matrix(1, nrow = 2L, ncol = length(header)))
  names(df) <- header
  data.table::fwrite(df, file.path(root, id, "CMD", file))
  list(root = root, folder = file.path(root, id), id = id)
}

# a Little Leonardo deployment. Defined here rather than reused from test-read_little_leonardo.R: testthat
# runs each test file in its own environment, so helpers do not cross files.
.auto_ll <- function(root = tempfile(), id = "LLTEST", n_sec = 20L, hz = 10L) {
  dir.create(file.path(root, id), recursive = TRUE, showWarnings = FALSE)
  n <- n_sec * hz
  set.seed(7)
  .write_crlf(c("ACCELERATION DATA ", "", sprintf(" %d msec/point", as.integer(1000L / hz)),
               "RECORD TIME   0h 1m", "START DATE   0000/00/00", "START TIME    00:00:00", "",
               " X  ,Y   ,Z   ",
               sprintf("%s,%s,%s,", round(stats::rnorm(n), 2), round(stats::rnorm(n), 2),
                       round(stats::rnorm(n, 1), 2))),
             file.path(root, id, paste0(id, "_A.txt")))
  list(root = root, folder = file.path(root, id), id = id, n = n)
}
.auto_ll_t0 <- function() as.POSIXct("2025-07-21 22:33:00", tz = "UTC")
.auto_ll_md <- function(id = "LLTEST", fmt = NULL) {
  md <- data.frame(id = id, tag = "Little Leonardo", type = "LL", deploy_date = .auto_ll_t0(),
                   lon = -28.6, lat = 38.6, data_start = .auto_ll_t0(), stringsAsFactors = FALSE)
  if (!is.null(fmt)) md$fmt <- fmt
  md
}
.auto_ll_cols <- function(...) metadataColumns(id = "id", tag_model = "tag", tag_type = "type",
                                               deploy_datetime = "deploy_date", deploy_lon = "lon",
                                               deploy_lat = "lat", data_start = "data_start", ...)

.ce_md <- function(id = "CE_01", fmt = NULL) {
  md <- data.frame(id = id, tag = "CEiiA", type = "archival",
                   deploy_date = as.POSIXct("2020-01-01", tz = "UTC"), lon = -28.6, lat = 38.6,
                   stringsAsFactors = FALSE)
  if (!is.null(fmt)) md$fmt <- fmt
  md
}
.ce_cols <- function(...) metadataColumns(id = "id", tag_model = "tag", tag_type = "type",
                                          deploy_datetime = "deploy_date", deploy_lon = "lon",
                                          deploy_lat = "lat", ...)


# ---- the detect contract: composed, never authored ------------------------------------------------

test_that("every reader entry exposes has_data, confirm and a composed detect", {
  r <- nautilus:::.readerFormats()
  expect_setequal(names(r), c("cats", "little_leonardo"))
  for (k in names(r)) {
    expect_true(all(c("label", "has_data", "confirm", "detect") %in% names(r[[k]])), info = k)
    expect_true(is.function(r[[k]]$detect), info = k)
  }
})

test_that(".withDetect makes `detect => has_data` true by construction", {
  # the invariant matters because violating it fails SILENTLY: a folder detect claims but has_data denies is
  # dropped by the pre-flight as "missing" and skipped with a "no sensor file found" notice - confident,
  # wrong and quiet. A reader author cannot break it, because they never write detect.
  broken <- nautilus:::.withDetect(list(has_data = function(f, s) FALSE, confirm = function(f, s) TRUE))
  expect_false(broken$detect("anywhere", "CMD"))

  both <- nautilus:::.withDetect(list(has_data = function(f, s) TRUE, confirm = function(f, s) TRUE))
  expect_true(both$detect("anywhere", "CMD"))

  # and on real entries against real folders of BOTH shapes. Asserted unconditionally (`detect implies
  # has_data`, written out): gating the assertion on `detect` firing would run zero assertions - and pass -
  # if detect were hard-wired FALSE.
  folders <- c(.ceiia_fixture()$folder, .auto_ll()$folder)
  r <- nautilus:::.readerFormats()
  for (k in names(r)) for (folder in folders) {
    implies <- !isTRUE(r[[k]]$detect(folder, "CMD")) || isTRUE(r[[k]]$has_data(folder, "CMD"))
    expect_true(implies, info = paste(k, folder))
  }
  # and each reader really does fire on its own folder, so the loop above is not vacuous
  expect_true(r$cats$detect(folders[1], "CMD"))
  expect_true(r$little_leonardo$detect(folders[2], "CMD"))
})

test_that(".withDetect swallows a throwing predicate rather than taking the run down", {
  boom <- nautilus:::.withDetect(list(has_data = function(f, s) stop("kaboom"),
                                      confirm  = function(f, s) TRUE))
  expect_false(boom$detect("anywhere", "CMD"))
  boom2 <- nautilus:::.withDetect(list(has_data = function(f, s) TRUE,
                                       confirm  = function(f, s) stop("kaboom")))
  expect_false(boom2$detect("anywhere", "CMD"))
})


# ---- the CATS detector ----------------------------------------------------------------------------

test_that(".catsConfirm identifies BOTH built-in dialects", {
  # the two dialects share no header literal, which is why detection resolves roles instead of matching
  # strings: any regex catching one would miss the other.
  ce <- .ceiia_fixture()
  expect_true(nautilus:::.catsConfirm(ce$folder, "CMD"))

  cats <- .csv_fixture(c("Date (UTC)", "Time (UTC)",
                         paste0("Accelerometer X [m/s", sup2, "]"),
                         paste0("Accelerometer Y [m/s", sup2, "]"),
                         paste0("Accelerometer Z [m/s", sup2, "]")))
  expect_true(nautilus:::.catsConfirm(cats$folder, "CMD"))
})

test_that(".catsConfirm resolves the expected roles, not merely TRUE", {
  # a boolean would sleep through an encoding regression that silently drops channels; assert the roles
  ce <- .ceiia_fixture()
  map <- nautilus:::.defaultMappings()
  p <- nautilus:::.probeSensorCSV(file.path(ce$folder, "CMD", "sensor.csv"),
                                  map, nautilus:::.normalizeHeader(map$colname))
  expect_true(p$has_dt)
  expect_setequal(p$channels, c("ax", "ay", "az", "depth"))
})

test_that(".catsConfirm is FALSE for anything that is not a stock CATS/CEiiA export", {
  expect_false(nautilus:::.catsConfirm(.csv_fixture(c("foo", "bar", "baz"))$folder, "CMD"))

  # A co-deployed Wildlife Computers sidecar. Use an `out-SST.csv` shape, NOT `...Locations.csv`:
  # `.listSensorCSVs` excludes Locations by FILENAME, so that fixture never reaches the rule under test and
  # would pass even with the rule deleted. The other WC sidecars (SST, PDTs, Histos) are not name-excluded,
  # do reach it, and each resolves the CEiiA `Date` row and ZERO channels - so requiring >= 1 real CHANNEL
  # rather than just a role is exactly what stops them looking like sensor data.
  # (the real out-SST.csv header, verbatim: it carries Date, Depth and Temperature, but not in any spelling
  # the mappings know - so it resolves the `Date` role and zero channels)
  expect_false(nautilus:::.catsConfirm(
    .csv_fixture(c("DeployID", "Ptt", "DepthSensor", "Instr", "Date", "LocationQuality",
                   "Latitude", "Longitude", "Depth", "Temperature", "Source"),
                 file = "out-SST.csv")$folder, "CMD"))

  # an R-mangled derived export (make.names() turns "Date (UTC)" into "Date..UTC.") - a real archive shape
  expect_false(nautilus:::.catsConfirm(
    .csv_fixture(c("", "Date..UTC.", "Time..UTC.", "Accelerometer.X..m.s2."))$folder, "CMD"))

  # a nautilus-derived export sitting beside raw data
  expect_false(nautilus:::.catsConfirm(
    .csv_fixture(c("X", "Y", "Z", "ID", "RunNo", "Datetime"))$folder, "CMD"))

  empty <- tempfile(); dir.create(file.path(empty, "E_01", "CMD"), recursive = TRUE)
  expect_false(nautilus:::.catsConfirm(file.path(empty, "E_01"), "CMD"))
})

test_that(".catsConfirm accepts a depth-only export, matching what the reader accepts", {
  # `required.sensors = NULL` accepts >= 1 channel, so this imports fine under format = "cats". Auto must
  # not be stricter than explicit - that is the worst failure mode for an opt-in convenience.
  f <- .csv_fixture(c("Date", "Depth (m)"))
  expect_true(nautilus:::.catsConfirm(f$folder, "CMD"))
})

test_that(".catsConfirm probes EVERY candidate, not just the first or largest", {
  # real deployments hold a raw export beside a derived/mangled one; which is "largest" is a coin flip
  f <- .csv_fixture(c("", "Date..UTC.", "Accelerometer.X..m.s2."), file = "aaa_mangled.csv")
  writeLines(c("Date,Ax (g),Depth (m)", "2020-01-01 00:00:00,0.1,1.0"),
             file.path(f$folder, "CMD", "zzz_raw.csv"))
  expect_true(nautilus:::.catsConfirm(f$folder, "CMD"))
})

test_that(".defaultMappings carries both dialects and is a pure move of the former inline table", {
  m <- nautilus:::.defaultMappings()
  expect_identical(colnames(m), c("colname", "sensor", "units"))
  expect_equal(nrow(m), 30L)
  expect_true("Date (UTC)" %in% m$colname)          # CATS
  expect_true("Ax (g)" %in% m$colname)              # CEiiA
  expect_setequal(m$sensor[m$colname %in% c("Ax (g)", "Ay (g)", "Az (g)")], c("ax", "ay", "az"))
})


# ---- the Little Leonardo detector -----------------------------------------------------------------

test_that(".llConfirm requires a rate the reader could actually use", {
  f <- .auto_ll()$folder
  expect_true(nautilus:::.llConfirm(f))

  # the proven false positive: another vendor whose header merely says "ACCELERATION DATA". .llDetect (the
  # discovery probe) fires on the title cue; confirm does not, because there is no parsable rate - and a
  # rate is exactly what read_little_leonardo() refuses to proceed without.
  other <- tempfile(); dir.create(other)
  .write_crlf(c("ACCELERATION DATA EXPORT v2", "Rate: 25 Hz", "", "X,Y,Z", "1,2,3"),
             file.path(other, "TAG_A.txt"))
  expect_true(nautilus:::.llDetect(other))
  expect_false(nautilus:::.llConfirm(other))
  expect_false(nautilus:::.readerFormats()$little_leonardo$detect(other, "CMD"))

  nofile <- tempfile(); dir.create(nofile)
  expect_false(nautilus:::.llConfirm(nofile))
})

test_that(".llDetect reads as far as the reader does (12 lines, not 6)", {
  # a folder whose rate line sits at line 7 is read fine by format = "little_leonardo"; a 6-line detection
  # window would skip it under "auto", making auto weaker than explicit.
  d <- tempfile(); dir.create(d)
  .write_crlf(c("TITLE", "", "", "", "", "", " 10 msec/point", "", "X,Y,Z", "1,2,3"),
             file.path(d, "T_A.txt"))
  expect_true(nautilus:::.llDetect(d))
  expect_true(nautilus:::.llConfirm(d))
})


# ---- resolution: the four states -------------------------------------------------------------------

test_that("format = 'auto' routes a stock CEiiA deployment to read_cats()", {
  f <- .ceiia_fixture()
  tags <- importTagData(data.folders = f$folder, format = "auto", id.metadata = .ce_md(),
                        columns = .ce_cols(), return.data = TRUE, verbose = "quiet")
  expect_s3_class(tags[["CE_01"]], "nautilus_tag")
  expect_setequal(nautilus:::.getMeta(tags[["CE_01"]])$sensors$present, c("ax", "ay", "az", "depth"))
})

test_that("format = 'auto' routes a Little Leonardo deployment to its own reader", {
  f <- .auto_ll()
  tags <- importTagData(data.folders = f$folder, format = "auto", id.metadata = .auto_ll_md(),
                        columns = .auto_ll_cols(), return.data = TRUE, verbose = "quiet")
  expect_s3_class(tags[["LLTEST"]], "nautilus_tag")
  expect_equal(nrow(tags[["LLTEST"]]), f$n)
})

test_that("format = 'auto' imports a MIXED batch in one call", {
  ce <- .ceiia_fixture()
  ll <- .auto_ll()
  md <- rbind(
    data.frame(id = "CE_01", tag = "CEiiA", type = "archival",
               deploy_date = as.POSIXct("2020-01-01", tz = "UTC"), lon = -28.6, lat = 38.6,
               data_start = as.POSIXct(NA, tz = "UTC"), stringsAsFactors = FALSE),
    data.frame(id = "LLTEST", tag = "Little Leonardo", type = "LL",
               deploy_date = as.POSIXct("2025-07-21 22:33:00", tz = "UTC"), lon = -28.6, lat = 38.6,
               data_start = as.POSIXct("2025-07-21 22:33:00", tz = "UTC"), stringsAsFactors = FALSE))
  tags <- importTagData(data.folders = c(ce$folder, ll$folder), format = "auto", id.metadata = md,
                        columns = metadataColumns(id = "id", tag_model = "tag", tag_type = "type",
                                                  deploy_datetime = "deploy_date", deploy_lon = "lon",
                                                  deploy_lat = "lat", data_start = "data_start"),
                        return.data = TRUE, verbose = "quiet")
  expect_setequal(names(tags), c("CE_01", "LLTEST"))
  expect_s3_class(tags[["CE_01"]], "nautilus_tag")
  expect_s3_class(tags[["LLTEST"]], "nautilus_tag")
})

test_that("files present but unidentifiable is a LOUD per-folder failure, and the run continues", {
  # the state the design record originally collapsed into "error listing supported formats". Aborting here
  # would take a batch of 60 good folders + 1 odd one from 60 imports to zero; calling it "no sensor file
  # found" would lie about a folder full of data. It is neither: it is one named failure.
  ce <- .ceiia_fixture()
  junk <- .csv_fixture(c("foo", "bar"), id = "JUNK_01", root = ce$root)
  md <- rbind(.ce_md("CE_01"), .ce_md("JUNK_01"))
  w <- testthat::capture_warnings(
    tags <- importTagData(data.folders = c(ce$folder, junk$folder), format = "auto", id.metadata = md,
                          columns = .ce_cols(), return.data = TRUE, verbose = "quiet"))
  # assert the LOUD text AND the absence of the quiet text. Matching "JUNK_01" alone would not do: the
  # quiet-skip notice names the folder too, so such a test stays green even if the feature regresses into
  # a skip - which is the whole distinction under test.
  expect_true(any(grepl("could not be imported", w)))
  expect_false(any(grepl("no sensor file found", w)))
  expect_s3_class(tags[["CE_01"]], "nautilus_tag")              # and the good folder still imports
})

test_that("a folder's failure is never silenced by another folder sharing its basename", {
  # the loud/quiet split is per-FOLDER, but the pre-flight's `missing_folders` holds BASENAMES. Two roots
  # can legitimately hold the same deployment id (this project has data/PIN_01 and _archive/PIN_01), and an
  # empty one must not turn a data-bearing one's failure into "no sensor file found".
  # ONE metadata row, TWO folders carrying that id (the metadata QC blocks duplicate rows, so this is the
  # shape that actually reaches the loop).
  a <- .csv_fixture(c("foo", "bar"), id = "DUP_01")             # has data, unidentifiable -> must be LOUD
  b <- tempfile(); dir.create(file.path(b, "DUP_01", "CMD"), recursive = TRUE)   # empty -> quiet skip
  w <- testthat::capture_warnings(
    importTagData(data.folders = c(a$folder, file.path(b, "DUP_01")), format = "auto",
                  id.metadata = .ce_md("DUP_01"), columns = .ce_cols(), return.data = TRUE,
                  verbose = "quiet"))
  expect_true(any(grepl("could not be imported", w)))
})

test_that("a folder with no data at all stays a quiet pre-flight skip under auto", {
  ce <- .ceiia_fixture()
  dir.create(file.path(ce$root, "EMPTY_01", "CMD"), recursive = TRUE)
  md <- rbind(.ce_md("CE_01"), .ce_md("EMPTY_01"))
  w <- testthat::capture_warnings(
    tags <- importTagData(data.folders = c(ce$folder, file.path(ce$root, "EMPTY_01")), format = "auto",
                          id.metadata = md, columns = .ce_cols(), return.data = TRUE, verbose = "quiet"))
  # the EXISTING pre-flight notice, not an auto-specific one: here "no sensor file found" is simply true
  expect_true(any(grepl("no sensor file found", w)))
  expect_true(any(grepl("EMPTY_01", w)))
  expect_s3_class(tags[["CE_01"]], "nautilus_tag")
})

test_that("auto aborts when it matches more than one reader, naming the folder and both readers", {
  # synthetic: no real folder is ambiguous. An LL accel file with a valid rate AND a stock CEiiA CSV, both
  # at the folder root, so both readers' discovery fires.
  root <- tempfile(); id <- "BOTH_01"
  dir.create(file.path(root, id), recursive = TRUE)
  .write_crlf(c("ACCELERATION DATA", "", " 10 msec/point", "", "X,Y,Z", "1,2,3"),
             file.path(root, id, "T_A.txt"))
  writeLines(c("Date,Ax (g),Depth (m)", "2020-01-01 00:00:00,0.1,1.0"), file.path(root, id, "s.csv"))
  expect_error(
    importTagData(data.folders = file.path(root, id), format = "auto", sensor.subdirectory = ".",
                  id.metadata = .ce_md(id), columns = .ce_cols(), verbose = "quiet"),
    "matched more than one"
  )
})

test_that("the ambiguous abort renders cleanly for several folders, and never evaluates a folder name", {
  # two regressions in one message: cli's {?a/b} inflection does not interpolate a nested {}, so the plural
  # branch leaked raw glue; and the bullets carry folder ids into cli's glue layer, so a deployment named
  # "PIN_{01}" was reported as "PIN_1" - the abort mangling the one thing it exists to name.
  mk_both <- function(root, id) {
    dir.create(file.path(root, id), recursive = TRUE, showWarnings = FALSE)
    .write_crlf(c("ACCELERATION DATA", "", " 10 msec/point", "", "X,Y,Z", "1,2,3"),
               file.path(root, id, "T_A.txt"))
    writeLines(c("Date,Ax (g),Depth (m)", "2020-01-01 00:00:00,0.1,1.0"), file.path(root, id, "s.csv"))
    file.path(root, id)
  }
  root <- tempfile()
  ids <- c("AMB_01", "PIN_{01}")                    # the second one is the glue-injection case
  folders <- vapply(ids, function(i) mk_both(root, i), character(1))
  md <- do.call(rbind, lapply(ids, .ce_md))
  err <- tryCatch(importTagData(data.folders = folders, format = "auto", sensor.subdirectory = ".",
                                id.metadata = md, columns = .ce_cols(), verbose = "quiet"),
                  error = function(e) conditionMessage(e))
  expect_match(err, "matched more than one")
  expect_match(err, "2 folders")                    # pluralised, not raw "{length(ambiguous)}"
  expect_false(grepl("length(ambiguous)", err, fixed = TRUE))
  expect_true(grepl("PIN_{01}", err, fixed = TRUE)) # the id survives verbatim, not mangled to "PIN_1"
})

test_that("auto aborts, naming itself, when NO reader can even find files anywhere in the batch", {
  # note the distinction this pins: a folder holding an UNRECOGNISED csv is not this case - it has data, so
  # it is the per-folder failure above and the run continues. The whole-run abort is reserved for a batch in
  # which no reader found any data at all (here: an empty CMD).
  root <- tempfile(); dir.create(file.path(root, "N_01", "CMD"), recursive = TRUE)
  err <- tryCatch(importTagData(data.folders = file.path(root, "N_01"), format = "auto",
                                id.metadata = .ce_md("N_01"), columns = .ce_cols(), verbose = "quiet"),
                  error = function(e) conditionMessage(e))
  expect_match(err, "No readable")
  expect_match(err, "auto")            # names the mode that failed
  expect_false(grepl("NA", err))       # never echoes back an unresolved format as if the user chose it
})


# ---- precedence: column > argument > detection ------------------------------------------------------

test_that("a tag_format value wins over auto, and only the unnamed rows are detected", {
  ce <- .ceiia_fixture()
  ll <- .auto_ll()
  md <- rbind(
    data.frame(id = "CE_01", tag = "CEiiA", type = "archival",
               deploy_date = as.POSIXct("2020-01-01", tz = "UTC"), lon = -28.6, lat = 38.6,
               data_start = as.POSIXct(NA, tz = "UTC"), fmt = NA_character_, stringsAsFactors = FALSE),
    data.frame(id = "LLTEST", tag = "Little Leonardo", type = "LL",
               deploy_date = as.POSIXct("2025-07-21 22:33:00", tz = "UTC"), lon = -28.6, lat = 38.6,
               data_start = as.POSIXct("2025-07-21 22:33:00", tz = "UTC"),
               fmt = "little_leonardo", stringsAsFactors = FALSE))
  cols <- metadataColumns(id = "id", tag_model = "tag", tag_type = "type", deploy_datetime = "deploy_date",
                          deploy_lon = "lon", deploy_lat = "lat", data_start = "data_start",
                          tag_format = "fmt")
  # LLTEST is NAMED (honoured); CE_01's cell is NA, so it falls through to detection
  tags <- importTagData(data.folders = c(ce$folder, ll$folder), format = "auto", id.metadata = md,
                        columns = cols, return.data = TRUE, verbose = "quiet")
  expect_s3_class(tags[["CE_01"]], "nautilus_tag")
  expect_s3_class(tags[["LLTEST"]], "nautilus_tag")
})

test_that("'auto' is legal as the argument but never as a tag_format column value", {
  # the column names a READER; "auto" is not one. Keeping it out of .readerFormats() is what buys this.
  f <- .ceiia_fixture()
  md <- .ce_md("CE_01", fmt = "auto")
  expect_error(importTagData(data.folders = f$folder, format = "cats", id.metadata = md,
                             columns = .ce_cols(tag_format = "fmt"), verbose = "quiet"),
               "Unsupported tag format")
})

test_that("an unknown format is still rejected, and auto does not widen what the column accepts", {
  f <- .ceiia_fixture()
  expect_error(importTagData(data.folders = f$folder, format = "nope", id.metadata = .ce_md(),
                             columns = .ce_cols(), verbose = "quiet"),
               "Unsupported tag format")
  md <- .ce_md("CE_01", fmt = "not_a_reader")
  expect_error(importTagData(data.folders = f$folder, format = "auto", id.metadata = md,
                             columns = .ce_cols(tag_format = "fmt"), verbose = "quiet"),
               "Unsupported tag format")
})

test_that(".checkFormat's allow= admits a value without advertising it as supported", {
  expect_true(nautilus:::.checkFormat("auto", "format", allow = "auto"))
  expect_error(nautilus:::.checkFormat("auto", "columns$tag_format"), "Unsupported tag format")
  # "auto" must not leak into the "Supported:" list a user is told to choose from
  err <- tryCatch(nautilus:::.checkFormat("bogus", "format", allow = "auto"),
                  error = function(e) conditionMessage(e))
  expect_false(grepl("auto", err))
})
