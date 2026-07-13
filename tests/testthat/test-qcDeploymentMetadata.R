# Tests for qcDeploymentMetadata(): role-gated metadata QC + normalisation.
# Assertions check the returned object / issue table (locale-independent), not console glyphs.

# a clean baseline metadata table (all roles mappable); helpers tweak single columns
.qc_raw <- function(n = 4) {
  data.frame(
    id  = paste0("PIN_", sprintf("%02d", seq_len(n))),
    tag = "CATS",
    dt  = as.POSIXct("2019-09-10", tz = "UTC") + (seq_len(n) - 1) * 86400,
    lon = -30, lat = 10,
    rec = as.POSIXct("2019-09-12", tz = "UTC") + (seq_len(n) - 1) * 86400,
    pkg = paste0("P", seq_len(n)),
    cmd = paste0("CATS-", seq_len(n)),
    stringsAsFactors = FALSE
  )
}

.qc_cols <- function(...) metadataColumns(id = "id", tag_model = "tag", deploy_datetime = "dt",
                                          deploy_lon = "lon", deploy_lat = "lat", ...)

# suppress the verbose=0 aggregated warning here (it is covered by its own dedicated test)
run_qc <- function(raw, columns, ...) {
  suppressWarnings(qcDeploymentMetadata(raw, columns = columns, verbose = 0, ...))
}


test_that("a clean table passes and returns a nautilus_deployments object", {
  out <- run_qc(.qc_raw(), .qc_cols(recovery_datetime = "rec", package_id = "pkg", logger_id = "cmd"))
  expect_s3_class(out, "nautilus_deployments")
  st <- attr(out, "nautilus.qc")
  expect_true(st$passed)
  expect_equal(st$status, "passed")
  expect_equal(st$n_errors, 0L)
  expect_equal(nrow(qcIssues(out, severity = "error")), 0L)
  # canonical role columns are present after normalisation
  expect_true(all(c("id", "deploy_datetime", "recovery_datetime", "package_id") %in% names(out)))
})

test_that("required-only mapping disables package/logger/recovery checks (role-gating)", {
  raw <- .qc_raw()
  raw$rec[2] <- raw$dt[2] - 86400        # recovery-before-deploy, but recovery is NOT mapped
  out <- run_qc(raw, .qc_cols())         # no recovery/package/logger roles
  expect_setequal(unique(qcIssues(out)$check), character(0))   # nothing flagged
  expect_true(attr(out, "nautilus.qc")$passed)
})

test_that("duplicate IDs are flagged as errors", {
  raw <- .qc_raw(); raw$id[3] <- raw$id[1]
  out <- run_qc(raw, .qc_cols())
  iss <- qcIssues(out, severity = "error")
  expect_true("duplicate_id" %in% iss$check)
  expect_false(attr(out, "nautilus.qc")$passed)
})

test_that("recovery before deployment is an error (needs recovery role)", {
  raw <- .qc_raw(); raw$rec[2] <- raw$dt[2] - 86400
  out <- run_qc(raw, .qc_cols(recovery_datetime = "rec"))
  expect_true("recovery_before_deploy" %in% qcIssues(out)$check)
})

test_that("future deployment dates are a warning, not an error", {
  raw <- .qc_raw(); raw$dt[1] <- Sys.time() + 30 * 86400
  out <- run_qc(raw, .qc_cols())
  iss <- qcIssues(out)
  expect_true("future_deploy" %in% iss$check)
  expect_equal(iss$severity[iss$check == "future_deploy"], "warning")
  expect_equal(attr(out, "nautilus.qc")$status, "passed_with_warnings")
})

test_that("out-of-range coordinates are flagged", {
  raw <- .qc_raw(); raw$lon[2] <- 200
  out <- run_qc(raw, .qc_cols())
  expect_true("implausible_location" %in% qcIssues(out, severity = "error")$check)
})

test_that("overlapping deployments on one package are flagged (needs package + recovery)", {
  raw <- .qc_raw(4)
  raw$pkg <- "SHARED"                    # all on one package
  raw$rec <- raw$dt + 5 * 86400          # 5-day windows starting 1 day apart -> overlap
  out <- run_qc(raw, .qc_cols(recovery_datetime = "rec", package_id = "pkg"))
  expect_true("package_overlap" %in% qcIssues(out, severity = "error")$check)
})

test_that("logger reuse is reported as info, not an error", {
  raw <- .qc_raw(); raw$cmd <- "CATS-134"     # one logger for all deployments
  out <- run_qc(raw, .qc_cols(logger_id = "cmd"))
  iss <- qcIssues(out)
  expect_true("logger_reuse" %in% iss$check)
  expect_equal(iss$severity[iss$check == "logger_reuse"], "info")
  expect_true(attr(out, "nautilus.qc")$passed)
})

test_that("identifier whitespace is normalised and reported as info", {
  raw <- .qc_raw(); raw$id[1] <- "  PIN_01  "
  out <- run_qc(raw, .qc_cols())
  expect_true("identifier_normalized" %in% qcIssues(out)$check)
  expect_equal(out$id[out$deploy_datetime == min(out$deploy_datetime)], "PIN_01")  # trimmed
})

test_that("rows are returned in chronological order", {
  raw <- .qc_raw(4)[c(3, 1, 4, 2), ]          # shuffled
  out <- run_qc(raw, .qc_cols())
  expect_false(is.unsorted(out$deploy_datetime))
})

test_that("non-POSIXct datetime columns abort with a clear message", {
  raw <- .qc_raw(); raw$dt <- as.character(raw$dt)
  expect_error(run_qc(raw, .qc_cols()), "POSIXct")
})

test_that("a mapped column missing from the table aborts", {
  raw <- .qc_raw()
  expect_error(run_qc(raw, .qc_cols(package_id = "does_not_exist")), "not found")
})

test_that("qcIssues errors on a non-deployments object", {
  expect_error(qcIssues(data.frame(a = 1)), "nautilus_deployments")
})

test_that("verbose = 0 emits an aggregated warning only when errors exist", {
  raw_bad <- .qc_raw(); raw_bad$id[2] <- raw_bad$id[1]
  expect_warning(qcDeploymentMetadata(raw_bad, columns = .qc_cols(), verbose = 0), "error")
  raw_ok <- .qc_raw()
  expect_silent(qcDeploymentMetadata(raw_ok, columns = .qc_cols(), verbose = 0))
})


test_that("exclude_sensors column is parsed, reported, and rides on the deployments table", {
  raw <- .qc_raw(4); raw$bad <- c("mag, gyro", "", "mx", "")
  out <- run_qc(raw, .qc_cols(exclude_sensors = "bad"))
  iss <- qcIssues(out)
  expect_true("known_bad_sensor" %in% iss$check)
  expect_match(iss$message[iss$check == "known_bad_sensor" & iss$id == "PIN_01"], "gyro, mag unusable")
  expect_true("exclude_sensors" %in% names(out))            # the column travels on the deployments table
  expect_true(attr(out, "nautilus.qc")$passed)              # info findings never fail QC
})

test_that("an invalid exclude_sensors token is an error issue, not an abort", {
  raw <- .qc_raw(4); raw$bad <- c("notasensor", "", "", "")
  out <- run_qc(raw, .qc_cols(exclude_sensors = "bad"))
  expect_true("invalid_exclude_sensors" %in% qcIssues(out, severity = "error")$check)
})
