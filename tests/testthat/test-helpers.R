# Unit tests for stable internal helper functions.
# These functions are believed correct; the tests lock in expected behaviour
# and provide a regression net for the upcoming correctness fixes elsewhere.

# ---------------------------------------------------------------------------
# .convertUnits
# ---------------------------------------------------------------------------
test_that(".convertUnits handles acceleration conversions", {
  expect_equal(.convertUnits(9.80665, "m/s2", "g"), 1)
  expect_equal(.convertUnits(1, "g", "m/s2"), 9.80665)
})

test_that(".convertUnits handles temperature conversions", {
  expect_equal(.convertUnits(0, "C", "F"), 32)
  expect_equal(.convertUnits(100, "C", "F"), 212)
  expect_equal(.convertUnits(32, "F", "C"), 0)
  expect_equal(.convertUnits(0, "C", "K"), 273.15)
  expect_equal(.convertUnits(273.15, "K", "C"), 0)
  # round-trip
  expect_equal(.convertUnits(.convertUnits(17, "C", "F"), "F", "C"), 17)
})

test_that(".convertUnits handles gyroscope, magnetic and speed conversions", {
  expect_equal(.convertUnits(180, "deg/s", "rad/s"), pi)
  expect_equal(.convertUnits(pi, "rad/s", "deg/s"), 180)
  expect_equal(.convertUnits(1000, "nT", "uT"), 1)
  expect_equal(.convertUnits(3.6, "km/h", "m/s"), 1)
  expect_equal(.convertUnits(1, "knot", "m/s"), 0.514444)
})

test_that(".convertUnits returns standard units when to.unit is NULL", {
  expect_equal(.convertUnits(9.80665, "m/s2", NULL), 1)        # -> g
  expect_equal(.convertUnits(180, "deg/s", NULL), pi)          # -> rad/s
  expect_equal(.convertUnits(1000, "nT", NULL), 1)             # -> uT
})

test_that(".convertUnits is a no-op when units match or are empty", {
  expect_equal(.convertUnits(42, "g", "g"), 42)
  expect_equal(suppressWarnings(.convertUnits(42, "", "g")), 42)
})

test_that(".convertUnits warns and returns input for unsupported conversion", {
  expect_warning(out <- .convertUnits(5, "g", "C"))
  expect_equal(out, 5)
})

test_that(".convertUnits is vectorised over the value argument", {
  expect_equal(.convertUnits(c(0, 100), "C", "F"), c(32, 212))
})

test_that(".convertUnits handles depth/pressure conversions (hydrostatic approx.)", {
  # 1 dbar ~ 0.9945 m of seawater; not the old 1:1 identity
  expect_equal(.convertUnits(100, "dbar", "m"), 99.45)
  expect_equal(.convertUnits(100, "m", "dbar"), 100 / 0.9945)
  # round-trip dbar -> m -> dbar is exact
  expect_equal(.convertUnits(.convertUnits(250, "dbar", "m"), "m", "dbar"), 250)
  # bar is consistent: 1 bar = 10 dbar ~ 9.945 m
  expect_equal(.convertUnits(1, "bar", "m"), 9.945)
  expect_equal(.convertUnits(1, "dbar", "bar"), 0.1)
  # dbar -> bar -> m equals dbar -> m directly (graph consistency)
  expect_equal(.convertUnits(.convertUnits(50, "dbar", "bar"), "bar", "m"),
               .convertUnits(50, "dbar", "m"))
})

# ---------------------------------------------------------------------------
# .circularMean
# ---------------------------------------------------------------------------
test_that(".circularMean averages correctly across the wrap point", {
  expect_equal(.circularMean(c(350, 10), c(0, 360)), 0, tolerance = 1e-8)
  expect_equal(.circularMean(c(10, 20, 30), c(0, 360)), 20, tolerance = 1e-8)
})

test_that(".circularMean ignores NA and returns NA when all missing", {
  expect_equal(.circularMean(c(10, NA, 30), c(0, 360)), 20, tolerance = 1e-8)
  expect_true(is.na(.circularMean(c(NA_real_, NA_real_), c(0, 360))))
})

# ---------------------------------------------------------------------------
# group.by helpers (.validateGroupBy / .compositeGroupKey) - one consistent API
# ---------------------------------------------------------------------------
test_that(".validateGroupBy accepts one or more canonical keys and rejects the rest", {
  expect_identical(.validateGroupBy("package_id"), "package_id")
  expect_identical(.validateGroupBy(c("package_id", "logger_id")), c("package_id", "logger_id"))
  expect_error(.validateGroupBy("bogus"), "must be one or more")
  expect_error(.validateGroupBy(character(0)), "must be one or more")
})

test_that(".compositeGroupKey joins present fields and NAs any item missing a field", {
  m <- list(package_id = "PKG1", logger_id = "LOG2")
  expect_identical(.compositeGroupKey("package_id", function(f) m[[f]]), "PKG1")
  expect_identical(.compositeGroupKey(c("package_id", "logger_id"), function(f) m[[f]]), "PKG1 | LOG2")
  expect_true(is.na(.compositeGroupKey(c("package_id", "logger_id"), function(f) list(package_id = "PKG1")[[f]])))
  expect_true(is.na(.compositeGroupKey("package_id", function(f) list(package_id = "")[[f]])))   # blank -> NA (own group)
})

# ---------------------------------------------------------------------------
# small numeric helpers
# ---------------------------------------------------------------------------
test_that(".standardError computes SD / sqrt(n) and ignores NA", {
  x <- c(1, 2, 3, 4)
  expect_equal(.standardError(x), sd(x) / 2)
  expect_equal(.standardError(c(x, NA)), sd(x) / 2)
})

test_that(".mode returns the most frequent value", {
  expect_equal(.mode(c(1, 2, 2, 3, 3, 3)), 3)
  expect_equal(.mode(c("a", "b", "b")), "b")
})

test_that(".decimalPlaces counts decimals", {
  expect_equal(.decimalPlaces(1), 0)
  expect_equal(.decimalPlaces(1.5), 1)
  expect_equal(.decimalPlaces(1.25), 2)
})

test_that(".rescale maps to the requested range", {
  expect_equal(.rescale(c(0, 5, 10), to = c(0, 1)), c(0, 0.5, 1))
})
