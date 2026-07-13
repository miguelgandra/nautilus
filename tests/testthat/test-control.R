# Tests for the processTagData() control objects (smoothingControl / calibrationControl /
# orientationControl / depthDriftControl).

test_that("smoothingControl validates windows and allows NULL to disable", {
  s <- smoothingControl()
  expect_s3_class(s, "nautilus_smoothing")
  expect_equal(s$depth, 10)
  expect_null(smoothingControl(dba = NULL)$dba)
  expect_error(smoothingControl(depth = -1), "smoothing\\$depth")
  expect_error(smoothingControl(depth = "x"), "smoothing\\$depth")
})

test_that("smoothingControl carries the static gravity-separation window, which cannot be disabled", {
  expect_equal(smoothingControl()$static, 3)                 # default
  expect_equal(smoothingControl(static = 5)$static, 5)
  expect_error(smoothingControl(static = NULL), "static")     # mandatory (gravity separation)
  expect_error(smoothingControl(static = 0), "static")        # must be > 0
})

test_that("calibrationControl validates the magnetometer-calibration flags only", {
  cal <- calibrationControl()
  expect_s3_class(cal, "nautilus_calibration")
  expect_true(cal$hard.iron && cal$soft.iron)
  expect_null(cal$correct.pitch)                              # offset corrections moved to orientationControl
  expect_false(calibrationControl(soft.iron = FALSE)$soft.iron)
  expect_error(calibrationControl(hard.iron = 1), "calibration\\$hard.iron")
  expect_true(calibrationControl()$use.stored)                 # apply a stored calibration by default
  expect_false(calibrationControl(use.stored = FALSE)$use.stored)
})

test_that("magCalibrationControl validates the fit method + thresholds", {
  mc <- magCalibrationControl()
  expect_s3_class(mc, "nautilus_mag_calibration")
  expect_equal(mc$method, "ellipsoid")                         # default: exact ellipsoid + dip/coverage-gated 2D fallback
  expect_true(mc$igrf.normalize)
  expect_equal(mc$radcv.max, 0.1)
  expect_equal(mc$planarity.max, 0.6)                          # band-fit thresholds present
  expect_null(mc$target.field)
  expect_equal(magCalibrationControl(method = "diagonal")$method, "diagonal")
  expect_error(magCalibrationControl(method = "bogus"))        # match.arg
  expect_error(magCalibrationControl(igrf.normalize = 1), "igrf.normalize")
  expect_error(magCalibrationControl(min.coverage = 2), "min.coverage")   # must be in [0, 1]
  expect_error(magCalibrationControl(radcv.max = -1), "radcv.max")
})

test_that("orientationControl carries the offset corrections, fit gate + warning threshold", {
  o <- orientationControl()
  expect_s3_class(o, "nautilus_orientation")
  expect_true(o$correct.pitch && o$correct.roll)
  expect_equal(o$pitch.offset.min.r2, 0.1)                    # relocated from calibrationControl()
  expect_equal(o$madgwick.beta, 0.02)                         # relocated from processTagData()
  expect_equal(o$warning.threshold, 45)                       # relocated from processTagData()
  expect_false(orientationControl(correct.roll = FALSE)$correct.roll)
  expect_error(orientationControl(correct.pitch = NA), "orientation\\$correct.pitch")
  expect_error(orientationControl(pitch.offset.min.r2 = 2), "pitch.offset.min.r2")   # must be in [0, 1]
  expect_error(orientationControl(madgwick.beta = -1), "madgwick.beta")
})

test_that(".as_control accepts an object, a named list, or NULL", {
  expect_equal(nautilus:::.as_control(NULL, smoothingControl, "nautilus_smoothing", "smoothing")$depth, 10)
  s <- smoothingControl(depth = 15)
  expect_identical(nautilus:::.as_control(s, smoothingControl, "nautilus_smoothing", "smoothing"), s)
  fromlist <- nautilus:::.as_control(list(depth = 15, dba = NULL), smoothingControl, "nautilus_smoothing", "smoothing")
  expect_equal(fromlist$depth, 15); expect_null(fromlist$dba)
  expect_error(nautilus:::.as_control(list(bogus = 1), smoothingControl, "nautilus_smoothing", "smoothing"), "unknown field")
})
