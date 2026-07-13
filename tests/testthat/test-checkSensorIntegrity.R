# Tests for checkSensorIntegrity(): structural sensor-channel validity (Phase 1: duplication + dead).

.mkint <- function(id, dup = FALSE, dead = FALSE, n = 500) {
  set.seed(1); t0 <- as.POSIXct("2022-09-17 13:00:00", tz = "UTC")
  ax <- rnorm(n, 0, .3); ay <- rnorm(n, 0, .3); az <- 1 + rnorm(n, 0, .3)
  d <- data.table::data.table(ID = id, datetime = t0 + (0:(n - 1)) / 20, ax = ax, ay = ay, az = az,
        gx = rnorm(n, 0, .1), gy = rnorm(n, 0, .1), gz = rnorm(n, 0, .1),
        mx = 25 + rnorm(n, 0, 1), my = rnorm(n, 0, 1), mz = 40 + rnorm(n, 0, 1),
        depth = pmax(0, 20 + 15 * sin((0:(n - 1)) / 50)), temp = 18 + rnorm(n, 0, .05))
  if (dup)  d[, `:=`(gx = ax, gy = ay, gz = az)]     # gyroscope duplicated from the accelerometer (firmware bug)
  if (dead) d[, temp := 20]                          # dead temperature sensor (constant)
  m <- nautilus:::.newNautilusMeta(); m$id <- id
  nautilus:::new_nautilus_tag(d, m)
}
.run <- function(...) {
  o <- NULL
  invisible(capture.output(suppressWarnings(suppressMessages(o <- checkSensorIntegrity(..., verbose = FALSE)))))
  o
}

test_that("duplication: a gyro copy of the accelerometer is flagged at error severity", {
  res <- .run(list(A = .mkint("A", dup = TRUE)))
  dup <- res$issues[res$issues$check == "duplication", ]
  expect_true("gyro" %in% dup$channel)
  expect_equal(unique(dup$severity), "error")
  expect_gt(dup$metric[dup$channel == "gyro"], 0.999)
})

test_that("dead: a constant channel is flagged", {
  res <- .run(list(A = .mkint("A", dead = TRUE)))
  expect_true(any(res$issues$check == "dead" & res$issues$channel == "temp"))
  expect_equal(res$issues$severity[res$issues$check == "dead"], "error")
})

test_that("a clean deployment yields zero findings", {
  res <- .run(list(A = .mkint("A")))
  expect_equal(nrow(res$issues), 0L)
})

test_that("apply = FALSE reports only; apply = TRUE drops flagged channels + records them as excluded", {
  x <- .mkint("A", dup = TRUE)
  r0 <- .run(list(A = x), apply = FALSE)
  expect_true(all(c("gx", "gy", "gz") %in% names(r0$curated_data$A)))       # untouched
  r1 <- .run(list(A = x), apply = TRUE)
  expect_false(any(c("gx", "gy", "gz") %in% names(r1$curated_data$A)))      # dropped
  expect_setequal(nautilus:::.getMeta(r1$curated_data$A)$sensors$excluded, c("gx", "gy", "gz"))
})

test_that("the issues table has the canonical schema", {
  res <- .run(list(A = .mkint("A", dup = TRUE)))
  expect_named(res$issues, c("id", "channel", "check", "severity", "metric", "message"))
  expect_type(res$issues$metric, "double")
  expect_true(all(res$issues$id == "A"))
})

test_that("checks = 'dead' skips the duplication check", {
  res <- .run(list(A = .mkint("A", dup = TRUE)), checks = "dead")
  expect_false(any(res$issues$check == "duplication"))
})

test_that("accepts a character vector of .rds file paths", {
  d <- tempfile(); dir.create(d); on.exit(unlink(d, recursive = TRUE), add = TRUE)
  saveRDS(.mkint("A", dup = TRUE), file.path(d, "A.rds"))
  res <- .run(file.path(d, "A.rds"))
  expect_true("A" %in% res$issues$id)
})

test_that("argument validation aborts clearly", {
  expect_error(checkSensorIntegrity(list(A = .mkint("A")), checks = "bogus", verbose = FALSE), "arg", ignore.case = TRUE)
  expect_error(checkSensorIntegrity(list(A = .mkint("A")), control = list(bogus = 1), verbose = FALSE), "control", ignore.case = TRUE)
  # capturing the (possibly-dropped) data is required only with apply = TRUE
  expect_error(checkSensorIntegrity(list(A = .mkint("A")), apply = TRUE, return.data = FALSE, verbose = FALSE),
               "output.dir", ignore.case = TRUE)
})

test_that("a report-only run (apply = FALSE) needs neither return.data nor output.dir", {
  res <- .run(list(A = .mkint("A", dup = TRUE)), apply = FALSE, return.data = FALSE)
  expect_null(res$curated_data)
  expect_true(any(res$issues$check == "duplication"))         # the report still comes back
})

test_that("plot.file writes a diagnostic PDF for flagged deployments", {
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f), add = TRUE)
  .run(list(A = .mkint("A", dup = TRUE)), plot.file = f)
  expect_true(file.exists(f) && file.size(f) > 0)
})

# --- Phase 2: opt-in plausibility checks (advisory: warning/info, never dropped by apply) --------------

# regular 3-D magnetometer rotation (full sphere coverage) + an accelerometer/gyro baseline
.mkp <- function(id, mut = identity, n = 20 * 180, fs = 20) {
  set.seed(1); t0 <- as.POSIXct("2022-01-01", tz = "UTC")
  th <- seq(0, 60 * pi, length.out = n); phi <- seq(0.2, pi - 0.2, length.out = n)
  d <- data.table::data.table(ID = id, datetime = t0 + (0:(n - 1)) / fs,
    ax = rnorm(n, 0, .2), ay = rnorm(n, 0, .2), az = 1 + rnorm(n, 0, .2),
    gx = rnorm(n, 0, .1), gy = rnorm(n, 0, .1), gz = rnorm(n, 0, .1),
    mx = 40 * sin(phi) * cos(th) + rnorm(n, 0, .3), my = 40 * sin(phi) * sin(th) + rnorm(n, 0, .3),
    mz = 40 * cos(phi) + rnorm(n, 0, .3), depth = abs(20 * sin((0:(n - 1)) / 300)), temp = 18 + rnorm(n, 0, .05))
  d <- mut(d); m <- nautilus:::.newNautilusMeta(); m$id <- id
  nautilus:::new_nautilus_tag(d, m)
}
.iss <- function(tag, checks) .run(list(x = tag), checks = checks)$issues

test_that("the opt-in checks are advisory and OFF by default", {
  sat <- .mkp("A", function(d) { d$ax <- pmin(d$ax, 0.25); d })      # a clipped accelerometer axis
  expect_equal(nrow(.iss(sat, c("duplication", "dead"))), 0L)        # default checks don't see it
  hit <- .iss(sat, "saturation")
  expect_equal(hit$check, "saturation"); expect_equal(hit$severity, "warning")   # opt-in flags it
})

test_that("saturation flags a clipped channel, and passes a clean one", {
  expect_equal(.iss(.mkp("clean"), "saturation")$channel, character(0))
  hit <- .iss(.mkp("clip", function(d) { d$az <- pmin(d$az, 1.1); d }), "saturation")
  expect_true("az" %in% hit$channel); expect_gt(hit$metric[hit$channel == "az"], 0.01)
})

test_that("mag.plausibility flags an unstable |B| (well-covered), passes a stable field, abstains on poor coverage", {
  expect_equal(nrow(.iss(.mkp("clean"), "mag.plausibility")), 0L)                     # spherical, stable |B|
  hit <- .iss(.mkp("softiron", function(d) { d$mx <- d$mx * 3; d }), "mag.plausibility") # severe soft-iron stretch
  expect_equal(hit$check, "mag.plausibility"); expect_gt(hit$metric, 0.4)
  # a barely-rotated mag (a short arc) can't be centred, so the check abstains rather than false-positive
  poor <- .mkp("poor", function(d) { a <- seq(0, 0.4, length.out = nrow(d))
    d[, `:=`(mx = 40 * cos(a), my = 40 * sin(a), mz = 25)]; d })
  expect_equal(nrow(.iss(poor, "mag.plausibility")), 0L)
})

test_that("accel.scale flags a wrong-unit accelerometer (~1 g expected)", {
  expect_equal(nrow(.iss(.mkp("clean"), "accel.scale")), 0L)
  hit <- .iss(.mkp("ms2", function(d) { d[, `:=`(ax = ax * 9.81, ay = ay * 9.81, az = az * 9.81)]; d }), "accel.scale")
  expect_equal(hit$channel, "accel"); expect_gt(hit$metric, 5)
})

test_that("gyro.bias flags a persistent offset (info severity)", {
  expect_equal(nrow(.iss(.mkp("clean"), "gyro.bias")), 0L)
  hit <- .iss(.mkp("bias", function(d) { d$gx <- d$gx + 0.2; d }), "gyro.bias")
  expect_equal(hit$check, "gyro.bias"); expect_equal(hit$severity, "info")
})

test_that("paddle.contamination is axis-agnostic and suppressed on a documented paddle tag", {
  # inject a HIGH-frequency narrow-band peak on my (5 Hz, in the paddle band; NOT mz - the check must
  # not assume a fixed axis)
  peaked <- .mkp("pad", function(d) { d$my <- d$my + 6 * sin(2 * pi * 5 * (0:(nrow(d) - 1)) / 20); d })
  hit <- .iss(peaked, "paddle.contamination")
  expect_equal(hit$channel, "my"); expect_equal(hit$severity, "warning")
  # the same signature on a paddle_wheel = TRUE deployment is expected, not flagged
  m <- nautilus:::.getMeta(peaked); m$tag$paddle_wheel <- TRUE
  peaked_pw <- nautilus:::.restoreMeta(peaked, m)
  expect_equal(nrow(.iss(peaked_pw, "paddle.contamination")), 0L)
})

test_that("paddle.contamination IGNORES a tail-beat-band peak but flags a high-frequency one (recalibrated)", {
  # a strong peak at 0.5 Hz - the swimming / tail-beat band - is the animal's body oscillation, NOT a
  # paddle, and must not be flagged (this was the dominant false-positive source before recalibration)
  tb <- .mkp("tb", function(d) { d$mz <- d$mz + 8 * sin(2 * pi * 0.5 * (0:(nrow(d) - 1)) / 20); d })
  expect_equal(nrow(.iss(tb, "paddle.contamination")), 0L)
  # the same-strength peak at 5 Hz (above the tail-beat band, below Nyquist) IS flagged
  hi <- .mkp("hi", function(d) { d$mz <- d$mz + 8 * sin(2 * pi * 5 * (0:(nrow(d) - 1)) / 20); d })
  expect_equal(.iss(hi, "paddle.contamination")$channel, "mz")
})

test_that("dropout flags a mostly-missing channel (info)", {
  expect_equal(nrow(.iss(.mkp("clean"), "dropout")), 0L)
  hit <- .iss(.mkp("drop", function(d) { d$temp[seq_len(round(0.7 * nrow(d)))] <- NA_real_; d }), "dropout")
  expect_equal(hit$channel, "temp"); expect_equal(hit$severity, "info"); expect_gt(hit$metric, 0.5)
})

test_that("gyro.bias needs an absolutely meaningful offset, not just a relatively large one", {
  # a barely-rotating gyro (tiny MAD) with a negligible 0.01 rad/s offset: large RELATIVE, but below the
  # absolute floor -> not flagged
  small <- .mkp("small", function(d) { d[, `:=`(gx = rnorm(nrow(d), 0.012, 0.01), gy = rnorm(nrow(d), 0, 0.01), gz = rnorm(nrow(d), 0, 0.01))]; d })
  expect_equal(nrow(.iss(small, "gyro.bias")), 0L)
  # a genuine 0.05 rad/s offset clears both the relative and the absolute threshold
  big <- .mkp("big", function(d) { d[, `:=`(gx = rnorm(nrow(d), 0.05, 0.01), gy = rnorm(nrow(d), 0, 0.01), gz = rnorm(nrow(d), 0, 0.01))]; d })
  expect_equal(.iss(big, "gyro.bias")$check, "gyro.bias")
})

# --- new internal helpers -----------------------------------------------------------------------------

test_that("integrityControl returns validated defaults and rejects bad fields", {
  d <- integrityControl()
  expect_s3_class(d, "nautilus_integrity")
  expect_equal(d$paddle.min.freq, 3.5); expect_equal(d$paddle.prominence, 30)
  expect_equal(d$gyro.bias.min, 0.02); expect_equal(d$mag.cv, 0.4)
  expect_error(integrityControl(dup.cor = 1.5), "dup.cor", ignore.case = TRUE)
  expect_error(integrityControl(paddle.max.freq.frac = 2), "max.freq.frac", ignore.case = TRUE)
  expect_error(integrityControl(paddle.prominence = 0.5), "prominence", ignore.case = TRUE)
  # a named list is coerced; an unknown field is rejected
  expect_s3_class(nautilus:::.as_control(list(mag.cv = 0.5), integrityControl, "nautilus_integrity", "control"), "nautilus_integrity")
  expect_error(nautilus:::.as_control(list(bogus = 1), integrityControl, "nautilus_integrity", "control"), "unknown", ignore.case = TRUE)
})

test_that(".welchPSD returns a compact one-sided PSD that locates a known peak", {
  fs <- 50; n <- 40000L; t <- (0:(n - 1)) / fs
  x <- sin(2 * pi * 6 * t) + rnorm(n, 0, 0.1)                 # a 6 Hz tone in noise
  pg <- nautilus:::.welchPSD(x, fs)
  expect_true(all(c("freq", "power", "nseg") %in% names(pg)))
  expect_lt(length(pg$freq), n / 2)                          # compact (not the full-series periodogram)
  expect_lt(max(pg$freq), fs / 2 + 1e-6)                     # one-sided, below Nyquist
  expect_equal(pg$freq[which.max(pg$power)], 6, tolerance = 0.2)
  expect_null(nautilus:::.welchPSD(1:10, fs))                # too short -> NULL
})

test_that(".dynamicPanelLayout adapts to the panel count without empty cells", {
  expect_equal(dim(nautilus:::.dynamicPanelLayout(1)$mat), c(1L, 1L))
  expect_equal(nautilus:::.dynamicPanelLayout(2)$mat, matrix(c(1L, 2L), 2L, 1L))     # stacked
  expect_equal(nautilus:::.dynamicPanelLayout(3)$mat, rbind(c(1L, 2L), c(3L, 3L)))   # odd -> last panel spans
  expect_equal(dim(nautilus:::.dynamicPanelLayout(4)$mat), c(2L, 2L))
  expect_equal(nautilus:::.dynamicPanelLayout(5)$mat, rbind(c(1L, 2L), c(3L, 4L), c(5L, 5L)))
  expect_false(any(nautilus:::.dynamicPanelLayout(6)$mat == 0L))                     # no empty cells
})

test_that("output.dir requires apply = TRUE (a report-only save would only copy the input)", {
  x <- .mkint("A", dup = TRUE)
  # report-only + save would write an uncurated copy that only LOOKS curated -> refuse
  expect_error(
    checkSensorIntegrity(list(A = x), apply = FALSE,
                         output.dir = tempdir(), verbose = FALSE),
    "apply = TRUE", ignore.case = TRUE)
  # the valid combination (apply + save) still writes a curated file
  dir <- file.path(tempdir(), "csi_guard"); dir.create(dir, showWarnings = FALSE)
  on.exit(unlink(dir, recursive = TRUE), add = TRUE)
  invisible(capture.output(suppressWarnings(suppressMessages(
    checkSensorIntegrity(list(A = x), apply = TRUE,
                         output.dir = dir, verbose = FALSE)))))
  expect_gte(length(list.files(dir, pattern = "\\.rds$")), 1L)
})
