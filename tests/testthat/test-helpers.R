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


# --- provenance encoding: stored text must be locale-independent -------------------------------------

test_that(".appendProcessing normalises externally-sourced text to UTF-8", {
  # the real CATS header bytes: latin1 superscript-two, micro sign and degree sign, which fread hands
  # back with no declared encoding. Built from raw so this file stays ASCII.
  lat1 <- c(rawToChar(as.raw(c(0x41, 0x63, 0x63, 0x20, 0x5b, 0x6d, 0x2f, 0x73, 0xb2, 0x5d))),
            rawToChar(as.raw(c(0x4d, 0x61, 0x67, 0x20, 0x5b, 0xb5, 0x54, 0x5d))),
            rawToChar(as.raw(c(0x54, 0x65, 0x6d, 0x70, 0x20, 0x5b, 0xb0, 0x43, 0x5d))))
  expect_false(any(validUTF8(lat1)))                       # the input really is invalid UTF-8

  m <- nautilus:::.appendProcessing(nautilus:::.newNautilusMeta(), "importTagData",
                                    directory = "/tmp/x", imported_columns = lat1)
  stored <- m$processing[[1]]$imported_columns
  expect_true(all(validUTF8(stored)))                      # ...and the stored copy is not
  expect_equal(stored, iconv(lat1, from = "latin1", to = "UTF-8"))   # text preserved, not mangled

  # non-character provenance is untouched, and the record keeps its shape
  m2 <- nautilus:::.appendProcessing(nautilus:::.newNautilusMeta(), "s", n = 5L, flag = TRUE, x = NULL)
  expect_identical(m2$processing[[1]]$n, 5L)
  expect_identical(m2$processing[[1]]$flag, TRUE)
})

test_that("a tag written in one locale carries no encoding warning when read in another", {
  # THE bug this guards: text stored without a declared encoding means something different in a
  # different locale. R keeps only the first 50 warnings of a call, so a batch of such reads also
  # discards whatever the package itself was trying to warn about.
  skip_on_cran()
  lat1 <- rawToChar(as.raw(c(0x41, 0x63, 0x63, 0x20, 0x5b, 0x6d, 0x2f, 0x73, 0xb2, 0x5d)))
  f <- tempfile(fileext = ".rds"); on.exit(unlink(f), add = TRUE)

  m <- nautilus:::.appendProcessing(nautilus:::.newNautilusMeta(), "importTagData", imported_columns = lat1)
  saveRDS(nautilus:::new_nautilus_tag(data.table::data.table(ID = "A", ax = 1), m), f)

  w <- character(0)
  withCallingHandlers(readRDS(f), warning = function(e) { w <<- c(w, conditionMessage(e)); invokeRestart("muffleWarning") })
  expect_length(w, 0L)
})

test_that("the raw header bytes still select columns out of a latin1 CSV", {
  # The counterpart guarantee: `colname_in_csv` must NOT be normalised, because it is what
  # fread(select=) matches against. Re-encoding it makes every non-ASCII sensor column silently
  # vanish from the import - 4 columns become 1.
  f <- tempfile(fileext = ".csv"); on.exit(unlink(f), add = TRUE)
  writeBin(charToRaw(paste0("Date,Acc [m/s", rawToChar(as.raw(0xb2)), "],Mag [",
                            rawToChar(as.raw(0xb5)), "T]\n1,0.1,25\n")), f)
  hdr <- names(data.table::fread(f, nrows = 0))
  expect_equal(ncol(data.table::fread(f, select = hdr)), 3L)             # raw bytes: all columns
  # fread warns once PER skipped column, and expect_warning consumes only the first - suppress the
  # whole call and assert on the result, so the suite's warning count stays clean.
  n <- suppressWarnings(ncol(data.table::fread(f, select = nautilus:::.toUTF8(hdr))))
  expect_lt(n, 3L)                                                       # normalised: columns lost
})
