# Independent local lm() fits and physical invariants for the shared depth-rate estimator.
.rateReference <- function(z, t, at, k) vapply(at, function(i) {
  j <- max(1L, i - k):min(length(z), i + k)
  tt <- t[j] - t[i]
  unname(stats::coef(stats::lm(z[j] ~ tt))[2])
}, numeric(1))

test_that("local depth rates preserve signed linear motion and truncate at edges", {
  for (hz in c(1, 20, 100)) {
    t <- as.numeric(as.POSIXct("2020-01-01", tz = "UTC")) + (0:(20 * hz)) / hz
    for (speed in c(-7.32, 0, 5.94)) {
      z <- 200 + speed * (t - t[1])
      got <- nautilus:::.depthRate(z, t, 5, dt = 1 / hz)
      expect_equal(got, rep(speed, length(t)), tolerance = 1e-7)
    }
  }
  expect_equal(smoothingControl()$depth, 5)
  expect_equal(formals(checkTagMapping)$depth.smoothing, 5)
  expect_equal(formals(nautilus:::.liteOrientation)$depth.smoothing, 5)
})

test_that("local depth rates fit actual timestamps and honour the requested window", {
  set.seed(41)
  t <- cumsum(runif(500, 0.09, 0.11))
  z <- 20 + 5 * sin(t / 3) + rnorm(length(t), sd = 0.03)
  for (W in c(0, 5, 10)) {
    k <- max(1L, floor(W / (2 * 0.1)))
    at <- if (k == 1L) 2:(length(t) - 1L) else seq_along(t)
    got <- nautilus:::.depthRate(z, t, W, dt = 0.1)
    expect_equal(got[at], .rateReference(z, t, at, k), tolerance = 1e-8)
  }
  expect_equal(nautilus:::.depthRate(z, t, NULL, dt = 0.1),
               nautilus:::.depthRate(z, t, 0, dt = 0.1))
  rate <- nautilus:::.depthRate(z, t, 5, dt = 0.1)
  expect_equal(nautilus:::.depthRate(z, t, 5, dt = 0.1, post_s = 1),
               data.table::frollmean(rate, 10, align = "center", fill = NA))
})

test_that("depth fits and rate post-means never bridge missing data or timestamp breaks", {
  # Flat sections at very different depths must have zero velocity even next to a break.
  t <- c(0:19, 100:119, NA_real_, 120:139, 130:149, 149:168)
  z <- rep(c(0, 100, NA, 300, 400, 500), c(20, 20, 1, 20, 20, 20))
  for (post in list(NULL, 3)) {
    rate <- nautilus:::.depthRate(z, t, 5, dt = 1, post_s = post)
    expect_true(all(abs(rate[is.finite(rate)]) < 1e-10))
    expect_true(is.na(rate[41]))
    expect_false(any(is.infinite(rate)))
    if (!is.null(post)) expect_true(all(is.na(rate[c(20, 21, 61, 62, 81, 82)])))
  }
  z <- c(rep(0, 20), NA, Inf, rep(100, 20))
  rate <- nautilus:::.depthRate(z, seq_along(z), 5)
  expect_true(all(is.na(rate[21:22])))
  expect_equal(rate[-(21:22)], rep(0, 40))
  expect_true(all(is.na(nautilus:::.depthRate(c(1, 2, 3), c(1, 1, 1), 5))))
})

test_that("short and unsupported records return missing rates without extrapolation", {
  for (n in 0:2) expect_identical(nautilus:::.depthRate(seq_len(n), seq_len(n)), rep(NA_real_, n))
  expect_equal(nautilus:::.depthRate(c(0, 1, 2), 0:2, 5), c(1, 1, 1))
  expect_true(all(is.na(nautilus:::.depthRate(c(0, 1, 2), 0:2, 5, post_s = 10))))
  expect_identical(nautilus:::.depthRate(rep(NA_real_, 10), 1:10), rep(NA_real_, 10))
})

test_that("long-record depth slopes retain local-fit precision across block boundaries", {
  n <- 2000000L
  t <- (seq_len(n) - 1) / 20
  z <- 100 + 80 * sin(2 * pi * t / 900) + 30 * sin(2 * pi * t / 53)
  at <- unique(as.integer(c(1, 99999:100002, 199999:200002,
                            seq(1000, n - 1000, length.out = 101), n)))
  got <- nautilus:::.depthRate(z, t, 5, dt = 0.05)
  expect_lt(max(abs(got[at] - .rateReference(z, t, at, 50))), 2e-6)
})

test_that("minimum slope windows retain precision on long records", {
  t <- (0:199999) / 20
  z <- sin(t)
  at <- c(2999L, 3000L, 3001L, 50000L, 99999L, 100000L, 150000L, 199999L)
  got <- nautilus:::.depthRate(z, t, NULL, dt = 0.05)
  expect_lt(max(abs(got[at] - .rateReference(z, t, at, 1))), 2e-6)
})
