# Tests for processingSummary(): the one-row-per-deployment projection of processTagData() provenance.

# a synthetic PROCESSED tag: fabricated processTagData + depth_drift audit records (no real run needed)
.mk_processed <- function(id = "A01", algo = "tilt_compass", median_pitch = -1, median_roll = 0.2,
                          pitch_off = -2.9, r2 = 0.8, roll_off = -1.5, drift = "applied",
                          off = c(1, 48), residual = 0.1, anchors = 7L, pitch_anom = FALSE, roll_anom = FALSE) {
  m <- nautilus:::.newNautilusMeta(); m$id <- id; m$tag$model <- "CATS"
  m$sensors$sampling_hz_original <- 100; m$sensors$sampling_hz_processed <- 20
  m <- nautilus:::.appendProcessing(m, "processTagData",
         orientation_algorithm = algo, median_pitch_deg = median_pitch, median_roll_deg = median_roll,
         pitch_offset_deg = pitch_off, pitch_offset_r2 = r2, roll_offset_deg = roll_off,
         hard_iron_offset_uT = 15.9, magnetic_declination = -3.2,
         pitch_anomaly_detected = pitch_anom, roll_anomaly_detected = roll_anom,
         n_input = 100000L, n_output = 20000L)
  if (!is.null(drift))
    m <- nautilus:::.appendProcessing(m, "depth_drift", status = drift, n_anchors = anchors,
           outcome = list(offset_range_m = off, residual_m = residual))
  nautilus:::new_nautilus_tag(data.table::data.table(ID = id, depth = 1:5), m)
}

test_that("processingSummary projects provenance into one typed row per deployment", {
  s <- processingSummary(list(A01 = .mk_processed("A01"),
                              B02 = .mk_processed("B02", drift = "applied_with_gaps", off = c(6.2, 6.2), anchors = 2L)))
  expect_s3_class(s, "nautilus_processing_summary")
  expect_equal(nrow(s), 2L)
  expect_setequal(s$id, c("A01", "B02"))
  a <- s[s$id == "A01", ]
  expect_equal(a$algorithm, "tilt_compass")
  expect_equal(a$pitch_offset, -2.9); expect_equal(a$pitch_r2, 0.8); expect_equal(a$roll_offset, -1.5)
  expect_equal(a$median_pitch, -1); expect_equal(a$hard_iron_uT, 15.9); expect_equal(a$declination, -3.2)
  expect_equal(a$drift_status, "applied"); expect_equal(a$drift_offset_m, 48)   # max(|c(1, 48)|)
  expect_equal(a$drift_residual_m, 0.1); expect_equal(a$drift_anchors, 7L)
  expect_equal(a$hz_in, 100); expect_equal(a$hz_out, 20)
  expect_equal(a$n_in, 100000L); expect_equal(a$n_out, 20000L); expect_equal(a$flag, "")
  # types are preserved (a projection, not a formatted table)
  expect_type(s$median_pitch, "double"); expect_type(s$n_in, "integer"); expect_type(s$id, "character")
  # a constant offset collapses to the single value
  expect_equal(s[s$id == "B02", ]$drift_offset_m, 6.2)
})

test_that("processingSummary surfaces the co-registration + magnetometer heading QC when present", {
  m <- nautilus:::.getMeta(.mk_processed("Q1"))
  m$axis_mapping$coreg_corr <- 0.93                              # from applyAxisMapping (Phase 2)
  mc <- nautilus:::.newMagCalibrationMeta()                      # nested contract: APPLIED qc set by processTagData
  mc$status <- "calibrated_3d"; mc$applied <- TRUE
  mc$qc <- list(confidence = "high", coverage_ok = TRUE, radcv = 0.04, igrf_residual = 6.2, axis_span = rep(170, 3))
  mc$applied_params <- list(center = c(0, 0, 0), soft_iron = diag(3), axis_net = NULL)
  m$mag_calibration <- mc                                        # from calibrateMagnetometer + processTagData
  tg <- nautilus:::new_nautilus_tag(data.table::data.table(ID = "Q1", depth = 1:5), m)
  q <- processingSummary(list(Q1 = tg))
  expect_equal(q$coreg_corr, 0.93)
  expect_equal(q$heading_conf, "high")
  expect_equal(q$mag_radcv, 0.04)
  expect_equal(q$mag_dip_resid, 6.2)
  # a tag with no gyro / no stored calibration reports NA for these (not an error)
  s0 <- processingSummary(list(A01 = .mk_processed("A01")))
  expect_true(is.na(s0$coreg_corr)); expect_true(is.na(s0$heading_conf)); expect_true(is.na(s0$mag_radcv))
})

test_that("processingSummary handles a missing depth_drift record and surfaces anomaly flags", {
  s <- processingSummary(list(Z9 = .mk_processed("Z9", drift = NULL, pitch_anom = TRUE, roll_anom = TRUE)))
  expect_equal(s$drift_status, "none")
  expect_true(is.na(s$drift_offset_m)); expect_true(is.na(s$drift_anchors))
  expect_equal(s$flag, "pitch,roll")
})

test_that("processingSummary reads a single object, a file vector, and a directory", {
  one <- processingSummary(.mk_processed("S1"))            # single tag object (metadata not lost to a split)
  expect_equal(nrow(one), 1L); expect_equal(one$id, "S1")
  dir <- file.path(tempdir(), "psum_test"); dir.create(dir, showWarnings = FALSE)
  on.exit(unlink(dir, recursive = TRUE), add = TRUE)
  saveRDS(.mk_processed("F1"), file.path(dir, "F1.rds")); saveRDS(.mk_processed("F2"), file.path(dir, "F2.rds"))
  expect_equal(nrow(processingSummary(dir)), 2L)
  expect_setequal(processingSummary(list.files(dir, full.names = TRUE))$id, c("F1", "F2"))
})

test_that("processingSummary fails loudly on empty input; the typed 0-row object still prints a count", {
  expect_error(processingSummary(list()), "empty", ignore.case = TRUE)
  expect_error(processingSummary(character(0)), "empty", ignore.case = TRUE)
  # the typed 0-row object (reachable internally) still classes + prints correctly
  e <- structure(nautilus:::.processingSummaryTemplate(), class = c("nautilus_processing_summary", "data.frame"))
  expect_s3_class(e, "nautilus_processing_summary"); expect_equal(nrow(e), 0L)
  expect_true(all(c("id", "pitch_offset", "drift_status", "n_in", "flag") %in% names(e)))
  expect_output(print(e), "0 deployments")
  expect_output(print(processingSummary(list(A = .mk_processed("A")))), "1 deployment")
})
