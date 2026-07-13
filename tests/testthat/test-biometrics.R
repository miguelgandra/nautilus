# Tests for the biological-traits feature: the metadataColumns `traits` passthrough, the meta$biometrics
# slot, updateBiometrics() (the refresh utility), and grouping a plot by an imported trait.

.mk_tag <- function(id, bio = list()) {
  m <- nautilus:::.newNautilusMeta(); m$id <- id; m$biometrics <- bio
  nautilus:::new_nautilus_tag(data.table::data.table(ID = id, datetime = as.POSIXct("2021-01-01", tz = "UTC") + 1:40,
                                                     depth = stats::runif(40, 0, 40)), m)
}

test_that("metadataColumns accepts a traits vector, dedups, and rejects bad values", {
  mc <- metadataColumns(traits = c("sex", "length", "sex"))
  expect_equal(mc$traits, c("sex", "length"))
  expect_null(metadataColumns()$traits)                          # NULL by default
  expect_error(metadataColumns(traits = 1), "traits", ignore.case = TRUE)
  expect_error(metadataColumns(traits = c("sex", "")), "traits", ignore.case = TRUE)
  expect_error(metadataColumns(traits = c("sex", NA)), "traits", ignore.case = TRUE)
})

test_that("the metadata schema carries an (empty) biometrics slot", {
  expect_true("biometrics" %in% names(nautilus:::.newNautilusMeta()))
  expect_length(nautilus:::.newNautilusMeta()$biometrics, 0L)
})

test_that("a plot groups by an imported trait read from meta$biometrics", {
  tags <- list(A = .mk_tag("A", list(sex = "F")), B = .mk_tag("B", list(sex = "M")), C = .mk_tag("C", list(sex = "F")))
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf), add = TRUE)
  s <- suppressMessages(plotTimeAtDepth(tags, group = "sex", plot = FALSE, plot.file = pf, verbose = FALSE))
  expect_setequal(unique(s$group), c("F", "M"))                  # grouping resolved from stored biometrics
})

test_that("updateBiometrics refreshes traits from a corrected table, preserving type", {
  tags <- list(A01 = .mk_tag("A01", list(sex = "F")), B02 = .mk_tag("B02"))
  idm <- data.frame(ID = c("A01", "B02"), sex = c("M", "F"), length = c(9.9, 5.1), stringsAsFactors = FALSE)
  out <- suppressMessages(updateBiometrics(tags, idm, columns = metadataColumns(traits = c("sex", "length")), verbose = FALSE))
  expect_equal(tagMetadata(out$A01)$biometrics$sex, "M")         # corrected F -> M
  expect_equal(tagMetadata(out$A01)$biometrics$length, 9.9)      # numeric preserved
  expect_equal(tagMetadata(out$B02)$biometrics$sex, "F")         # newly set
  expect_true("updateBiometrics" %in% processingHistory(out$A01)$step)
})

test_that("updateBiometrics resolves the id from meta$id when the data has no ID column", {
  m <- nautilus:::.newNautilusMeta(); m$id <- "A01"
  x <- nautilus:::new_nautilus_tag(data.table::data.table(depth = 1:3), m)   # id only in meta, no ID column
  idm <- data.frame(ID = "A01", sex = "M", stringsAsFactors = FALSE)
  out <- suppressMessages(updateBiometrics(list(A01 = x), idm, columns = metadataColumns(traits = "sex"), verbose = FALSE))
  expect_equal(tagMetadata(out$A01)$biometrics$sex, "M")       # regression: previously errored "length zero"
})

test_that("factor-valued traits are stored as character (no foreign levels leak in)", {
  idm <- data.frame(ID = "A01", species = factor("R. typus", levels = c("R. typus", "M. birostris")), stringsAsFactors = FALSE)
  out <- suppressMessages(updateBiometrics(list(A01 = .mk_tag("A01")), idm,
                          columns = metadataColumns(traits = "species"), verbose = FALSE))
  expect_type(tagMetadata(out$A01)$biometrics$species, "character")
  expect_equal(tagMetadata(out$A01)$biometrics$species, "R. typus")
})

test_that("updateBiometrics warns on an unmatched id and leaves it untouched", {
  tags <- list(A01 = .mk_tag("A01", list(sex = "F")), Z99 = .mk_tag("Z99"))
  idm <- data.frame(ID = "A01", sex = "M", stringsAsFactors = FALSE)
  out <- NULL   # capture inside the expectation: expect_warning() returns the condition, not the value
  expect_warning(out <- suppressMessages(updateBiometrics(tags, idm, columns = metadataColumns(traits = "sex"), verbose = FALSE)),
                 "no matching", ignore.case = TRUE)
  expect_length(tagMetadata(out$Z99)$biometrics, 0L)
})

test_that("updateBiometrics validates arguments", {
  tags <- list(A01 = .mk_tag("A01"))
  idm <- data.frame(ID = "A01", sex = "M", stringsAsFactors = FALSE)
  expect_error(updateBiometrics(tags, idm, verbose = FALSE), "traits", ignore.case = TRUE)          # no traits declared
  expect_error(updateBiometrics(tags, idm, columns = metadataColumns(traits = "nope"), verbose = FALSE),
               "not found", ignore.case = TRUE)                                                     # trait col absent
  expect_error(updateBiometrics(tags, idm, columns = metadataColumns(traits = "sex"),
               return.data = FALSE, verbose = FALSE), "output.dir", ignore.case = TRUE)
})
