# Tests for exportForSSM(): formats a reconstructTrack pseudo-track into the aniMotum/crawl-ready frame
# (id/date/lc/lon/lat + degree errors from pseudo_error), thinned to one position per time bin.

.mk_ssm_tag <- function(id = "A", n = 600L, err = TRUE) {
  t0 <- as.POSIXct("2021-01-01", tz = "UTC")
  d <- data.table::data.table(ID = id, datetime = t0 + seq_len(n),
    pseudo_lon = seq(0, 0.05, length.out = n), pseudo_lat = rep(-17, n),
    pseudo_error = seq(50, 500, length.out = n))
  if (!err) d[, pseudo_error := NULL]
  m <- nautilus:::.newNautilusMeta(); m$id <- id
  nautilus:::new_nautilus_tag(d, m)
}

test_that("exportForSSM emits aniMotum-ready columns (x.sd/y.sd) with GL class and metre errors", {
  out <- exportForSSM(list(A = .mk_ssm_tag()), thin.minutes = 0, verbose = FALSE)
  expect_setequal(names(out), c("id", "date", "lc", "lon", "lat", "x.sd", "y.sd"))
  expect_true(all(out$lc == "GL"))
  expect_equal(nrow(out), 600L)                                              # thin.minutes = 0 -> all samples
  err_m <- seq(50, 500, length.out = 600)
  expect_equal(out$y.sd[100], err_m[100], tolerance = 1e-9)                  # x.sd/y.sd = pseudo_error, in metres
  expect_equal(out$x.sd, out$y.sd)                                           # isotropic reckoning error
})

test_that("thin.minutes keeps one position per time bin", {
  full <- exportForSSM(list(A = .mk_ssm_tag(n = 600L)), thin.minutes = 0, verbose = FALSE)
  thin <- exportForSSM(list(A = .mk_ssm_tag(n = 600L)), thin.minutes = 1, verbose = FALSE)   # 600 s -> ~10 bins
  expect_lt(nrow(thin), nrow(full))
  expect_lte(nrow(thin), 11L)
  expect_false(is.unsorted(as.numeric(thin$date)))                           # still time-ordered
})

test_that("error.col = NULL (or absent) exports the 5-column GPS format (no x.sd/y.sd)", {
  a <- exportForSSM(list(A = .mk_ssm_tag()), error.col = NULL, thin.minutes = 0, verbose = FALSE)
  b <- exportForSSM(list(A = .mk_ssm_tag(err = FALSE)), thin.minutes = 0, verbose = FALSE)  # no pseudo_error col
  expect_true(all(a$lc == "G")); expect_setequal(names(a), c("id", "date", "lc", "lon", "lat"))
  expect_true(all(b$lc == "G")); expect_false(any(c("x.sd", "y.sd") %in% names(b)))
})

test_that("a mixed error / no-error export keeps x.sd/y.sd, NA on the GPS-only deployment", {
  out <- exportForSSM(list(A = .mk_ssm_tag(id = "A", err = TRUE), B = .mk_ssm_tag(id = "B", err = FALSE)),
                      thin.minutes = 0, verbose = FALSE)
  expect_true(all(c("x.sd", "y.sd") %in% names(out)))
  expect_true(all(out$lc[out$id == "A"] == "GL") && all(is.finite(out$x.sd[out$id == "A"])))
  expect_true(all(out$lc[out$id == "B"] == "G")  && all(is.na(out$x.sd[out$id == "B"])))   # per-obs NA is fine
})

test_that("deployments lacking position columns are skipped; multiple ids are stacked", {
  bad <- nautilus:::new_nautilus_tag(
    data.table::data.table(ID = "B", datetime = as.POSIXct("2021-01-01", tz = "UTC") + 1:10),
    nautilus:::.newNautilusMeta())
  out <- exportForSSM(list(A = .mk_ssm_tag(id = "A"), B = bad), thin.minutes = 0, verbose = FALSE)
  expect_setequal(unique(out$id), "A")                                       # B has no pseudo_lon -> skipped
  two <- exportForSSM(list(A = .mk_ssm_tag(id = "A"), C = .mk_ssm_tag(id = "C")),
                      thin.minutes = 0, verbose = FALSE)
  expect_setequal(unique(two$id), c("A", "C"))                               # both stacked, keyed by id
})

test_that("exportForSSM validates its arguments and returns an empty frame when nothing exports", {
  expect_error(exportForSSM(list(A = .mk_ssm_tag()), thin.minutes = -1), "between")
  bad <- nautilus:::new_nautilus_tag(
    data.table::data.table(ID = "B", datetime = as.POSIXct("2021-01-01", tz = "UTC") + 1:10),
    nautilus:::.newNautilusMeta())
  empty <- exportForSSM(list(B = bad), verbose = FALSE)
  expect_equal(nrow(empty), 0L)
  expect_setequal(names(empty), c("id", "date", "lc", "lon", "lat"))
})
