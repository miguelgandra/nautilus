# Tests for findValidationSegments(): synthetic processed-style data with a KNOWN sharp turn (~t=200),
# a KNOWN roll event (~t=400), and a KNOWN dive (~t=500); the detector must locate each.

.t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")

.mk_proc <- function(id = "A01", n = 600) {
  set.seed(1)
  dt <- .t0 + 0:(n - 1)                                   # 1 Hz
  # heading: constant 90, then a +90 deg turn over 20 s centred at t=200
  heading <- rep(90, n)
  heading[191:210] <- 90 + cumsum(rep(90 / 20, 20))
  heading[211:n]   <- 180
  heading <- heading %% 360
  turning_angle <- c(0, ((diff(heading) + 180) %% 360) - 180)
  # roll: ~0, a +40 deg roll excursion for ~16 s centred at t=400
  roll <- rnorm(n, 0, 0.5); roll[393:408] <- 40
  # depth: shallow, a 30 m dive over 20 s centred at t=500
  depth <- rep(2, n); depth[491:510] <- 2 + cumsum(rep(30 / 20, 20)); depth[511:n] <- 32
  vertical_velocity <- c(0, diff(depth))                  # 1 Hz -> m/s
  d <- data.table::data.table(ID = id, datetime = dt, depth = depth, heading = heading,
                              pitch = rnorm(n, 0, 1), roll = roll, vedba = runif(n, 0, 0.3),
                              vertical_velocity = vertical_velocity, turning_angle = turning_angle)
  m <- nautilus:::.newNautilusMeta(); m$id <- id; m$sensors$sampling_hz_processed <- 1
  nautilus:::new_nautilus_tag(d, m)
}

.run <- function(...) {
  res <- NULL
  invisible(capture.output(suppressWarnings(suppressMessages(res <- findValidationSegments(..., verbose = FALSE)))))
  res
}
.secs <- function(a, b) abs(as.numeric(difftime(a, b, units = "secs")))

test_that("locates the sharp turn, the roll, and the dive at their true times", {
  out <- .run(list(A01 = .mk_proc()), n = 1)
  expect_s3_class(out, "data.frame")
  expect_setequal(out$type, c("turn", "roll", "dive"))
  expect_lt(.secs(out$peak_time[out$type == "turn"], .t0 + 200), 12)
  expect_lt(.secs(out$peak_time[out$type == "roll"], .t0 + 400), 12)
  expect_lt(.secs(out$peak_time[out$type == "dive"], .t0 + 500), 12)
})

test_that("reports interpretable magnitudes (net turn deg, peak roll deg, net depth m)", {
  out <- .run(list(A01 = .mk_proc()), n = 1)
  expect_equal(out$value[out$type == "turn"], 90, tolerance = 12)   # net heading change
  expect_equal(out$value[out$type == "roll"], 40, tolerance = 4)    # peak |roll|
  expect_equal(out$value[out$type == "dive"], 30, tolerance = 4)    # net depth change
  expect_equal(out$unit[out$type == "turn"], "deg")
  expect_equal(out$unit[out$type == "dive"], "m")
})

test_that("returns typed columns and POSIXct windows of the requested width", {
  out <- .run(list(A01 = .mk_proc()), types = "dive", n = 1, window = 20)
  expect_equal(names(out), c("id", "type", "rank", "start", "end", "peak_time", "value", "unit"))
  expect_s3_class(out$start, "POSIXct")
  expect_type(out$value, "double")
  expect_equal(as.numeric(difftime(out$end, out$start, units = "secs")), 20)
})

test_that("n controls the number of (non-overlapping) segments per type", {
  out <- .run(list(A01 = .mk_proc()), types = "dive", n = 3)
  expect_equal(nrow(out), 3L)
  expect_equal(out$rank, 1:3)
  # the clearest (rank 1) is the real dive; segments do not overlap
  expect_lt(.secs(out$peak_time[1], .t0 + 500), 12)
  expect_true(all(diff(sort(as.numeric(out$peak_time))) >= 20))
})

test_that("a type whose column is absent is skipped, not errored", {
  d <- .mk_proc()
  d[, c("turning_angle", "heading") := NULL]               # remove the 'turn' inputs
  out <- .run(list(A01 = d))
  expect_false("turn" %in% out$type)
  expect_true(all(c("roll", "dive") %in% out$type))
})

test_that("only the requested types are returned", {
  out <- .run(list(A01 = .mk_proc()), types = c("turn", "roll"))
  expect_setequal(unique(out$type), c("turn", "roll"))
})

test_that("verbose = FALSE is silent", {
  out <- capture.output(suppressWarnings(suppressMessages(
    res <- findValidationSegments(list(A01 = .mk_proc()), verbose = FALSE))))
  expect_length(out, 0)
})
