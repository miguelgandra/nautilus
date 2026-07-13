# Tests for imputePaddleCalibration(): the paddle-wheel speed-calibration gap-filler.

# Two tags with a clean, identical +0.02/year trend -> the shared-rate model must recover 0.02 exactly,
# interpolate the gap year, keep measured values, and project a baseline for an uncalibrated tag.
.cal2 <- function() data.frame(
  package_id = c("A", "A", "B", "B"),
  year       = c(2018, 2020, 2018, 2020),
  slope      = c(0.10, 0.14, 0.08, 0.12),
  r.squared  = c(0.99, 0.99, 0.99, 0.99), stringsAsFactors = FALSE)

.dep <- function() data.frame(
  package_id = c("A", "A", "A", "B", "C"),       # C is deployed but never calibrated
  year       = c(2018, 2019, 2020, 2019, 2019),
  paddle_wheel = TRUE, stringsAsFactors = FALSE)

test_that("shared-rate recovers a clean degradation rate, keeps measured, and fills the gap", {
  r <- imputePaddleCalibration(.cal2(), .dep(), verbose = FALSE)
  expect_equal(attr(r, "degradation.rate"), 0.02, tolerance = 1e-8)
  # measured years returned exactly and flagged
  a2018 <- r[r$package_id == "A" & r$year == 2018, ]
  expect_equal(a2018$slope, 0.10); expect_equal(a2018$slope_source, "measured")
  # interpolated gap year on the shared trend
  a2019 <- r[r$package_id == "A" & r$year == 2019, ]
  expect_equal(a2019$slope, 0.12, tolerance = 1e-6); expect_equal(a2019$slope_source, "tag-model")
})

test_that("a never-calibrated tag gets a baseline slope from the global level + shared rate", {
  r <- imputePaddleCalibration(.cal2(), .dep(), verbose = FALSE)
  c2019 <- r[r$package_id == "C" & r$year == 2019, ]
  expect_equal(c2019$slope_source, "baseline")
  expect_equal(c2019$n_calibrations, 0L)
  expect_equal(c2019$slope, 0.11, tolerance = 1e-6)        # mean tag-level (-40.27) + 0.02*2019
})

test_that("the output is directly consumable by processTagData (required columns + types)", {
  r <- imputePaddleCalibration(.cal2(), .dep(), verbose = FALSE)
  expect_true(all(c("year", "package_id", "slope") %in% names(r)))   # processTagData's required set
  expect_true(is.numeric(r$year)); expect_true(is.character(r$package_id)); expect_true(is.numeric(r$slope))
  expect_false(anyNA(r$slope))                                       # gap-free
})

test_that("keep.measured = FALSE replaces measured values with the model fit", {
  on  <- imputePaddleCalibration(.cal2(), .dep(), keep.measured = TRUE,  verbose = FALSE)
  off <- imputePaddleCalibration(.cal2(), .dep(), keep.measured = FALSE, verbose = FALSE)
  expect_true(any(on$slope_source == "measured"))
  expect_false(any(off$slope_source == "measured"))                  # nothing flagged measured
})

test_that("fixed-rate uses the supplied degradation rate", {
  r <- imputePaddleCalibration(.cal2(), .dep(), method = "fixed-rate", degradation.rate = 0.05, verbose = FALSE)
  expect_equal(attr(r, "degradation.rate"), 0.05)
  # A measured 2018 = 0.10; level = 0.10 - 0.05*2018; predict 2019 = level + 0.05*2019 = 0.15
  expect_equal(r[r$package_id == "A" & r$year == 2019, ]$slope, 0.12, tolerance = 1e-6)  # mean of the 2 detrended levels
})

test_that("method = 'fixed-rate' requires degradation.rate", {
  expect_error(imputePaddleCalibration(.cal2(), .dep(), method = "fixed-rate", verbose = FALSE), "degradation.rate")
})

test_that("a negative estimated rate is floored at 0 with a warning (non.negative.rate)", {
  cal <- data.frame(package_id = c("A", "A", "B", "B"), year = c(2018, 2020, 2018, 2020),
                    slope = c(0.14, 0.10, 0.12, 0.08), stringsAsFactors = FALSE)  # decreasing -> rate < 0
  expect_warning(imputePaddleCalibration(cal, .dep(), verbose = FALSE), "negative")
  r <- suppressWarnings(imputePaddleCalibration(cal, .dep(), verbose = FALSE))
  expect_equal(attr(r, "degradation.rate"), 0)
})

test_that("slope.range clamps imputed slopes (not measured) and warns", {
  expect_warning(
    r <- imputePaddleCalibration(.cal2(), .dep(), slope.range = c(0.05, 0.115), verbose = FALSE),
    "Clamped")
  expect_true(all(r$slope[r$slope_source != "measured"] <= 0.115))
  expect_equal(r[r$package_id == "A" & r$year == 2018, ]$slope, 0.10)   # measured 0.10 kept (in range anyway)
})

test_that("max.extrapolation warns on slopes projected far beyond the calibration range", {
  dep <- data.frame(package_id = "A", year = 2030, paddle_wheel = TRUE, stringsAsFactors = FALSE)
  expect_warning(imputePaddleCalibration(.cal2(), dep, max.extrapolation = 3, verbose = FALSE), "beyond")
})

test_that("paddle.col filters deployments; an absent flag column warns and uses all", {
  dep <- data.frame(package_id = c("A", "B"), year = c(2019, 2019),
                    paddle_wheel = c(TRUE, FALSE), stringsAsFactors = FALSE)
  r <- imputePaddleCalibration(.cal2(), dep, verbose = FALSE)
  expect_true("A" %in% r$package_id); expect_false("B" %in% r$package_id)   # non-paddle B excluded
  expect_warning(imputePaddleCalibration(.cal2(), .dep()[, c("package_id", "year")], verbose = FALSE),
                 "No .* column")
})

test_that("duplicate tag-year calibrations are averaged with a warning", {
  cal <- rbind(.cal2(), data.frame(package_id = "A", year = 2018, slope = 0.12,
                                   r.squared = 0.9, stringsAsFactors = FALSE))   # dup A-2018
  expect_warning(imputePaddleCalibration(cal, .dep(), verbose = FALSE), "[Dd]uplicate")
})

test_that("argument and column validation errors are clear", {
  expect_error(imputePaddleCalibration(.cal2()[, c("package_id", "year")], .dep(), verbose = FALSE), "slope")
  expect_error(imputePaddleCalibration(.cal2(), .dep(), slope.range = c(0.3, 0.1), verbose = FALSE), "slope.range")
})

test_that("a single calibrated tag works (1-level factor edge case)", {
  cal <- data.frame(package_id = c("51", "51"), year = c(2019, 2021), slope = c(0.074, 0.088),
                    stringsAsFactors = FALSE)
  dep <- data.frame(package_id = c("51", "999"), year = c(2022, 2022), paddle_wheel = TRUE,
                    stringsAsFactors = FALSE)
  r <- imputePaddleCalibration(cal, dep, verbose = FALSE)
  expect_equal(attr(r, "degradation.rate"), 0.007, tolerance = 1e-6)   # (0.088-0.074)/2
  expect_equal(r[r$package_id == "51", ]$slope_source, "tag-model")    # 2022 projected from the one tag
  expect_equal(r[r$package_id == "999", ]$slope_source, "baseline")    # uncalibrated -> baseline
  expect_false(anyNA(r$slope))
})

test_that("the verbose block is a standardized cli block", {
  out <- paste(cli::cli_fmt(suppressWarnings(
    imputePaddleCalibration(.cal2(), .dep(), verbose = 2))), collapse = "\n")
  expect_match(out, "imputePaddleCalibration")          # framed header
  expect_match(out, "degradation rate:")              # detailed diagnostic
  expect_match(out, "filled: measured")               # provenance counts
  expect_match(out, "SUMMARY")
})
