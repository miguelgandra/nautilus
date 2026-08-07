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

  # the exclusion belongs to the RETURNED data only. data.table `:=` deletes by reference, so without an
  # explicit copy this would delete the columns from the caller's own object - destroying sensor data the
  # function only promised to withhold. `x` must survive an apply = TRUE run untouched.
  expect_true(all(c("gx", "gy", "gz") %in% names(x)))
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

test_that("accel.scale runs by default and grades a unit error as an error", {
  # unit/scale mistakes are the single most likely user error when building a tag by hand, so
  # accel.scale must fire without being asked for. (The full default set is asserted below.)
  # acceleration left in m/s^2 and never converted to g: a ~9.8 g static magnitude is not a blemish,
  # it is a channel that cannot support orientation or dynamic acceleration at all.
  x <- .mkint("A")
  x[, `:=`(ax = ax * 9.80665, ay = ay * 9.80665, az = az * 9.80665)]
  res <- suppressWarnings(.run(list(A = x), apply = TRUE))   # DEFAULT checks - no `checks =` argument
  hit <- res$issues[res$issues$check == "accel.scale", ]
  expect_equal(nrow(hit), 1L)
  expect_equal(hit$severity, "error")                       # deviation 8.8 g >> accel.scale.error
  expect_gt(hit$metric, 9)                                  # the REPORTED metric is the magnitude, ~9.8 g

  # error severity + apply = TRUE means the accelerometer is excluded, and recorded as such
  expect_false(any(c("ax", "ay", "az") %in% names(res$curated_data$A)))
  expect_true(all(c("ax", "ay", "az") %in% nautilus:::.getMeta(res$curated_data$A)$sensors$excluded))

  # ...and an error-severity finding warns loudly whether or not apply intervened. `.run` swallows
  # warnings, so this one calls the function directly.
  expect_warning(invisible(capture.output(checkSensorIntegrity(list(A = x), apply = FALSE, verbose = FALSE))),
                 "integrity", ignore.case = TRUE)
})

test_that("the SUMMARY names the error-severity deployments, independently of the warning", {
  # R keeps only the FIRST 50 warnings of a top-level call, so a batch that emits many warnings before
  # this one would DISCARD it. The console block is the guarantee that survives that; a run must never
  # depend on the warning alone to reveal a compromised channel.
  x <- .mkint("A")
  x[, `:=`(ax = ax * 9.80665, ay = ay * 9.80665, az = az * 9.80665)]
  # cli writes its alerts through the condition system, so BOTH streams have to be captured here
  out <- character(0)
  invisible(capture.output(out <- capture.output(
    suppressWarnings(checkSensorIntegrity(list(A = x), apply = FALSE, verbose = 1)),
    type = "message")))
  expect_true(any(grepl("error", out, fixed = TRUE)))
  expect_true(any(grepl("A: accel", out, fixed = TRUE)))   # the deployment is NAMED, not just counted
})

test_that("accel.scale grades a modest scale error as a warning that apply leaves alone", {
  # a 30% gain error sits between accel.scale.warning (0.20) and accel.scale.error (0.50): worth
  # reporting, not worth discarding the accelerometer over. This is the case the graded model exists
  # to separate from the one above.
  x <- .mkint("A")
  x[, `:=`(ax = ax * 1.30, ay = ay * 1.30, az = az * 1.30)]
  res <- .run(list(A = x), apply = TRUE)
  hit <- res$issues[res$issues$check == "accel.scale", ]
  expect_equal(hit$severity, "warning")
  expect_true(all(c("ax", "ay", "az") %in% names(res$curated_data$A)))   # default apply.severity = "error"
  expect_equal(nrow(res$curated_data$A), nrow(x))

  # a cautious run CAN act on warnings, without changing how the finding was classified
  res2 <- .run(list(A = x), apply = TRUE, apply.severity = "warning")
  expect_equal(res2$issues$severity[res2$issues$check == "accel.scale"], "warning")
  expect_false(any(c("ax", "ay", "az") %in% names(res2$curated_data$A)))
})

test_that("saturation runs by default, covers the IMU only, and never drops a channel", {
  expect_setequal(eval(formals(checkSensorIntegrity)$checks),
                  c("duplication", "dead", "accel.scale", "saturation"))

  # a gyro axis railed at its range limit
  x <- .mkint("A")
  x[, gz := pmin(gz, stats::quantile(gz, 0.90))]
  res <- .run(list(A = x), apply = TRUE)
  hit <- res$issues[res$issues$check == "saturation", ]
  expect_true("gz" %in% hit$channel)
  expect_equal(unique(hit$severity), "warning")
  expect_true("gz" %in% names(res$curated_data$A))       # advisory: nothing is dropped
})

test_that("saturation ignores depth, whose surface floor is a resting state and not clipping", {
  # a depth channel sitting at exactly 0 for most of the record is a surfacing animal. It must not be
  # flagged: the check is scoped to .imuFamilies(), and depth/temp range faults belong to
  # checkSensorQuality(). Without that scoping this fixture would flag at 70% - as would most of a
  # real fleet.
  x <- .mkint("A")
  x[1:350, depth := 0]
  expect_gt(mean(x$depth == 0), 0.5)
  res <- .run(list(A = x))
  expect_equal(nrow(res$issues[res$issues$check == "saturation", ]), 0L)
})

test_that("a per-axis gain error slips past accel.scale (documented blind spot)", {
  # locks the limitation stated in ?checkSensorIntegrity and vignette("orientation-methods"):
  # a scalar magnitude test cannot see an error confined to one axis of a near-level animal.
  x <- .mkint("A")
  x[, ay := ay * 1.20]
  res <- .run(list(A = x))
  expect_equal(nrow(res$issues[res$issues$check == "accel.scale", ]), 0L)
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

test_that("dead covers the paddle channels, and names the cause from the documented flag", {
  # a constant paddle column is a verdict on the INPUT - it reads the same whatever processTagData was
  # asked to compute - so it belongs to the QC stage. `dead` already had the exact predicate; it simply
  # was not looking at these columns.
  mk_paddle <- function(id, const, paddle_wheel) {
    d <- .mkint(id)
    d[, paddle_speed := const]
    m <- nautilus:::.newNautilusMeta(); m$id <- id; m$tag$paddle_wheel <- paddle_wheel
    nautilus:::new_nautilus_tag(d, m)
  }
  dead_doc <- .iss(mk_paddle("A", 0, TRUE), "dead")
  expect_true("paddle_speed" %in% dead_doc$channel)
  expect_equal(dead_doc$severity[dead_doc$channel == "paddle_speed"], "error")
  expect_match(dead_doc$message[dead_doc$channel == "paddle_speed"], "dead or jammed paddle wheel")

  # same grade when no paddle is documented - the consequence for the data is identical; only the
  # cause named in the message differs
  dead_undoc <- .iss(mk_paddle("B", 0, FALSE), "dead")
  expect_equal(dead_undoc$severity[dead_undoc$channel == "paddle_speed"], "error")
  expect_match(dead_undoc$message[dead_undoc$channel == "paddle_speed"], "no paddle wheel is recorded")

  # a varying paddle passes
  live <- .mkint("C"); live[, paddle_speed := seq_len(.N) / .N]
  m <- nautilus:::.newNautilusMeta(); m$id <- "C"; m$tag$paddle_wheel <- TRUE
  expect_equal(nrow(.iss(nautilus:::new_nautilus_tag(live, m), "dead")), 0L)
})

test_that("a legitimately sparse paddle channel is not flagged by dropout or saturation", {
  # CATS logs Velocity at 1 Hz onto a 20 Hz grid and regularizeTimeSeries leaves sub-rate channels
  # uninterpolated, so a HEALTHY paddle is ~95% NA. This is why `dead` got its own candidate set rather
  # than the paddle being added to ctx$fams, which dropout and saturation also build from.
  d <- .mkint("A", n = 2000)
  d[, paddle_speed := NA_real_]
  idx <- seq(1L, nrow(d), by = 20L)
  data.table::set(d, i = idx, j = "paddle_speed", value = seq_along(idx) / length(idx))
  m <- nautilus:::.newNautilusMeta(); m$id <- "A"; m$tag$paddle_wheel <- TRUE
  tg <- nautilus:::new_nautilus_tag(d, m)

  expect_equal(nrow(.iss(tg, "dead")), 0L)          # sparse but varying -> not dead
  drop <- .iss(tg, "dropout")
  expect_false("paddle_speed" %in% drop$channel)     # 95% NA, and correctly ignored
  sat <- .iss(tg, "saturation")
  expect_false("paddle_speed" %in% sat$channel)
})

test_that("apply drops a dead paddle channel, so processTagData never sees it", {
  d <- .mkint("A"); d[, paddle_speed := 0]
  m <- nautilus:::.newNautilusMeta(); m$id <- "A"; m$tag$paddle_wheel <- TRUE
  tg <- nautilus:::new_nautilus_tag(d, m)
  out <- .run(list(x = tg), checks = "dead", apply = TRUE, return.data = TRUE)
  cd <- out$curated_data[[1]]
  expect_false("paddle_speed" %in% names(cd))
  expect_true("paddle_speed" %in% tagMetadata(cd)$sensors$excluded)
})

# ---- mag.break -------------------------------------------------------------------------------
# A long, coarse fixture: the check summarises 10-minute windows and needs >= 30 of them, so 8 h at
# 1 Hz. `field(i)` sets the magnitude of the field vector at sample i, which is what the check reads.
.mkbreak <- function(id, field, n = 8 * 3600, fs = 1) {
  set.seed(4); t0 <- as.POSIXct("2022-01-01", tz = "UTC")
  th <- seq(0, 80 * pi, length.out = n); phi <- seq(0.2, pi - 0.2, length.out = n)
  f <- field(seq_len(n))
  data.table::data.table(ID = id, datetime = t0 + (0:(n - 1)) / fs,
    ax = rnorm(n, 0, .2), ay = rnorm(n, 0, .2), az = 1 + rnorm(n, 0, .2),
    gx = rnorm(n, 0, .1), gy = rnorm(n, 0, .1), gz = rnorm(n, 0, .1),
    mx = f * sin(phi) * cos(th) + rnorm(n, 0, .3), my = f * sin(phi) * sin(th) + rnorm(n, 0, .3),
    mz = f * cos(phi) + rnorm(n, 0, .3),
    depth = abs(20 * sin((0:(n - 1)) / 300)), temp = 18 + rnorm(n, 0, .05)) -> d
  m <- nautilus:::.newNautilusMeta(); m$id <- id
  nautilus:::new_nautilus_tag(d, m)
}

test_that("mag.break flags a persistent step and passes a stationary field", {
  flat <- .mkbreak("flat", function(i) rep(45, length(i)))
  expect_equal(nrow(.iss(flat, "mag.break")), 0L)

  step <- .mkbreak("step", function(i) ifelse(i < length(i) * 0.55, 55, 44))
  hit <- .iss(step, "mag.break")
  expect_equal(hit$check, "mag.break")
  expect_equal(hit$severity, "warning")          # never error: the fleet metric is not bimodal
  expect_equal(hit$channel, "mag")
  expect_gt(hit$metric, 0.96)
  expect_match(hit$message, "does not return")
})

test_that("mag.break is NOT fooled by a field that swings between the same two levels", {
  # THE false-positive case, and the reason the metric is rank separation rather than step size: a
  # contaminated magnetometer's |m| varies with heading, so an animal that keeps turning oscillates
  # between levels all deployment. The median gap is as large as the genuine step above; the
  # separation is not, because the level keeps coming back.
  osc <- .mkbreak("osc", function(i) ifelse((i %/% 2400) %% 2 == 0, 55, 44))
  expect_equal(nrow(.iss(osc, "mag.break")), 0L)

  # and the naive statistic really would have fired here - the two records span the same two levels
  sc_osc  <- nautilus:::.magBreakScan(osc, nautilus:::.imuFamilies()$mag)
  sc_step <- nautilus:::.magBreakScan(.mkbreak("s", function(i) ifelse(i < length(i) * 0.55, 55, 44)),
                                      nautilus:::.imuFamilies()$mag)
  expect_gt(sc_osc$step, 5)                      # a large median gap...
  expect_lt(sc_osc$auc, 0.9)                     # ...but poor separation
  expect_gt(sc_step$auc, 0.96)                   # whereas the true step separates
})

test_that("mag.break abstains on a record too short to show persistence, rather than guessing", {
  short <- .mkbreak("short", function(i) ifelse(i < length(i) * 0.55, 55, 44), n = 2 * 3600)
  expect_null(nautilus:::.magBreakScan(short, nautilus:::.imuFamilies()$mag))
  expect_equal(nrow(.iss(short, "mag.break")), 0L)
})

test_that("mag.break ignores a perfectly-separated but negligible step", {
  # a stable sensor drifting by 1% separates cleanly but is not a change of magnetic environment
  tiny <- .mkbreak("tiny", function(i) ifelse(i < length(i) * 0.55, 45.0, 44.6))
  sc <- nautilus:::.magBreakScan(tiny, nautilus:::.imuFamilies()$mag)
  expect_gt(sc$auc, 0.96)                        # the separation gate alone would pass it
  expect_lt(sc$rel, 0.05)                        # the effect-size gate is what stops it
  expect_equal(nrow(.iss(tiny, "mag.break")), 0L)
})

test_that("mag.break is opt-in, and apply never drops the magnetometer for it", {
  step <- .mkbreak("step", function(i) ifelse(i < length(i) * 0.55, 55, 44))
  expect_equal(nrow(.iss(step, c("duplication", "dead"))), 0L)   # not in the default set

  # the finding describes the RECORD (one calibration cannot span it), not a bad channel: the
  # magnetometer is fine and the pre-break data is fully usable, so excluding it would destroy far
  # more than it protects. Even at apply.severity = "warning" the channel must survive.
  out <- .run(list(x = step), checks = "mag.break", apply = TRUE, apply.severity = "warning",
              return.data = TRUE)
  cd <- out$curated_data[[1]]
  expect_true(all(c("mx", "my", "mz") %in% names(cd)))
  expect_false(any(c("mx", "my", "mz") %in% tagMetadata(cd)$sensors$excluded))
  expect_equal(nrow(out$issues[out$issues$check == "mag.break", ]), 1L)   # still reported

  # a genuine channel fault in the same run is still acted on - the exemption is per-check
  bad <- .mkbreak("bad", function(i) rep(45, length(i)))
  data.table::setDT(bad)[, temp := 20]                                   # dead channel (error)
  o2 <- .run(list(x = bad), checks = c("dead", "mag.break"), apply = TRUE, return.data = TRUE)
  expect_false("temp" %in% names(o2$curated_data[[1]]))
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

test_that("integrityControl exposes classification thresholds only, and validates them", {
  d <- integrityControl()
  expect_s3_class(d, "nautilus_integrity")
  expect_equal(d$saturation.warning, 0.01); expect_equal(d$saturation.error, 0.20)
  expect_equal(d$accel.scale.warning, 0.20); expect_equal(d$accel.scale.error, 0.50)
  expect_equal(d$mag.plausibility.warning, 0.40); expect_equal(d$paddle.warning, 30)
  # low-level algorithm constants are private, NOT part of the public control object
  expect_null(d$paddle.min.freq); expect_null(d$gyro.bias.min); expect_null(d$paddle.harmonic.guard)
  expect_equal(nautilus:::.integrityMethod()$paddle.min.freq, 3.5)
  expect_equal(nautilus:::.integrityMethod()$gyro.bias.min, 0.02)
  expect_error(integrityControl(duplication.error = 1.5), "duplication.error", ignore.case = TRUE)
  expect_error(integrityControl(paddle.warning = 0.5), "paddle.warning", ignore.case = TRUE)
  # an error threshold below its warning threshold would make the warning unreachable
  expect_error(integrityControl(saturation.error = 0.005), "saturation.error", ignore.case = TRUE)
  expect_error(integrityControl(accel.scale.error = 0.1), "accel.scale.error", ignore.case = TRUE)
  # a named list is coerced; an unknown field (including a removed one) is rejected
  expect_s3_class(nautilus:::.as_control(list(mag.plausibility.warning = 0.5), integrityControl,
                                        "nautilus_integrity", "control"), "nautilus_integrity")
  expect_error(nautilus:::.as_control(list(mag.cv = 0.5), integrityControl, "nautilus_integrity", "control"),
               "unknown", ignore.case = TRUE)
})

test_that(".integrityGrade escalates with the metric and honours absent thresholds", {
  g <- nautilus:::.integrityGrade
  # graded check: below warning -> pass, between -> warning, above error -> error
  expect_true(is.na(g(0.005, warning = 0.01, error = 0.20)))
  expect_equal(g(0.0105, warning = 0.01, error = 0.20), "warning")
  expect_equal(g(0.987, warning = 0.01, error = 0.20), "error")
  # a warning-only check can never reach error, however extreme the metric
  expect_equal(g(10, warning = 0.4), "warning")
  # non-finite metrics abstain rather than grading
  expect_true(is.na(g(NA_real_, warning = 0.01, error = 0.2)))
  expect_true(is.na(g(NaN, warning = 0.01)))
  # ranks order the severities so apply.severity can be compared as a floor
  expect_true(nautilus:::.severityRank("error") > nautilus:::.severityRank("warning"))
  expect_true(nautilus:::.severityRank("warning") > nautilus:::.severityRank("info"))
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


#######################################################################################################
# accel.calibration: the per-axis companion to accel.scale #############################################
#
# accel.scale grades |median(||A||) - 1|, a scalar on the MAGNITUDE, and is by construction insensitive
# to the per-axis errors that move roll and pitch: a 0.02 g lateral offset shifts it by 0.0002 (its
# warning threshold is 0.2) while producing ~10 deg of roll error. These tests pin that separation.

# a static-dominated record at a realistic posture spread, with a known error injected
.aclSim <- function(n = 60000, offset = c(0, 0, 0), gain = c(1, 1, 1), sd_deg = 20, noise = 0.004,
                    fs = 10) {
  set.seed(11)
  # attitude must vary SMOOTHLY: the check derives its static component from a ~2 s rolling mean, and
  # white-noise attitude would be averaged to zero by it (a fixture bug, not a detector limitation)
  smooth <- function(k) {
    w <- max(3L, round(fs * 20))                                   # ~20 s correlation time
    z <- as.numeric(stats::filter(stats::rnorm(n + 2 * w), rep(1 / w, w), sides = 2))
    z <- z[!is.na(z)][seq_len(n)]
    z / stats::sd(z) * k * pi / 180
  }
  p <- smooth(sd_deg); r <- smooth(sd_deg)
  A <- cbind(-sin(p), cos(p) * sin(r), cos(p) * cos(r))
  A <- sweep(sweep(A, 2, gain, "*"), 2, offset, "+") + matrix(stats::rnorm(3 * n, 0, noise), ncol = 3)
  d <- data.table::data.table(ID = "A01",
        datetime = as.POSIXct("2020-01-01", tz = "UTC") + seq_len(n) / fs,
        ax = A[, 1], ay = A[, 2], az = A[, 3], depth = 0)
  data.table::setattr(d, "nautilus.version", "test")
  d
}
.aclRun <- function(d, ...) {
  r <- suppressWarnings(suppressMessages(
    checkSensorIntegrity(list(A01 = d), checks = "accel.calibration", apply = FALSE,
                         return.data = FALSE, verbose = FALSE, ...)))
  if (is.data.frame(r)) r else r$issues
}

test_that("a clean accelerometer produces no finding", {
  expect_equal(nrow(.aclRun(.aclSim())), 0L)
})

test_that("a lateral offset is caught, and reported as degrees of attitude error", {
  iss <- .aclRun(.aclSim(offset = c(0, 0.03, 0)))
  expect_gte(nrow(iss), 1L)
  off <- iss[grepl("offset", iss$message), , drop = FALSE]
  expect_equal(nrow(off), 1L)
  expect_true(off$severity %in% c("warning", "error"))
  expect_gt(off$metric, 1)                       # degrees, not g
  expect_match(off$message, "deg of attitude error")
})

test_that("that same offset is invisible to accel.scale - the two checks are not redundant", {
  d <- .aclSim(offset = c(0, 0.03, 0))
  sc <- suppressWarnings(suppressMessages(
    checkSensorIntegrity(list(A01 = d), checks = "accel.scale", apply = FALSE,
                         return.data = FALSE, verbose = FALSE)))
  sc <- if (is.data.frame(sc)) sc else sc$issues
  expect_equal(nrow(sc), 0L)                     # accel.scale sees nothing...
  expect_gte(nrow(.aclRun(d)), 1L)               # ...while accel.calibration does
})

test_that("a per-axis gain error is caught and named separately from an offset", {
  iss <- .aclRun(.aclSim(gain = c(1, 1.12, 1)))
  g <- iss[grepl("gain", iss$message), , drop = FALSE]
  expect_equal(nrow(g), 1L)
  expect_match(g$message, "spread")
})

test_that("a UNIFORM gain error is not reported: it cancels in the angle", {
  # this is accel.scale's job, and correcting it here would mask a unit bug
  iss <- .aclRun(.aclSim(gain = c(1.1, 1.1, 1.1)))
  expect_equal(nrow(iss[grepl("gain", iss$message), , drop = FALSE]), 0L)
})

test_that("a record that is not gravity-dominated is declined, not guessed at", {
  # a tag held in ONE attitude with sustained specific acceleration: the model does not hold, and the
  # fit would otherwise return physically impossible parameters (measured: offsets to 0.96 g on real data)
  set.seed(12); n <- 60000
  A <- cbind(rep(0, n), rep(0, n), rep(1, n)) + matrix(stats::rnorm(3 * n, 0, 0.25), ncol = 3)
  d <- data.table::data.table(ID = "A01",
        datetime = as.POSIXct("2020-01-01", tz = "UTC") + seq_len(n) / 10,
        ax = A[, 1], ay = A[, 2], az = A[, 3], depth = 0)
  data.table::setattr(d, "nautilus.version", "test")
  iss <- .aclRun(d)
  expect_equal(nrow(iss), 1L)
  expect_equal(iss$severity, "info")
  expect_match(iss$message, "could not be assessed")
  expect_false(grepl("offset \\(", iss$message))   # no parameter is reported when declining
})

test_that("the check is opt-in and silent on a short record", {
  expect_equal(nrow(.aclRun(.aclSim(n = 2000))), 0L)          # below accel.calibration.min.n
  d <- .aclSim(offset = c(0, 0.03, 0))
  dflt <- suppressWarnings(suppressMessages(
    checkSensorIntegrity(list(A01 = d), apply = FALSE, return.data = FALSE, verbose = FALSE)))
  dflt <- if (is.data.frame(dflt)) dflt else dflt$issues
  expect_equal(nrow(dflt[dflt$check == "accel.calibration", , drop = FALSE]), 0L)
})

test_that("thresholds are honoured and validated", {
  d <- .aclSim(offset = c(0, 0.03, 0))
  expect_equal(nrow(.aclRun(d, control = integrityControl(accel.calibration.warning = 90,
                                                          accel.calibration.error = 91))), 0L)
  expect_error(integrityControl(accel.calibration.warning = 5, accel.calibration.error = 1), "must be >=")
})


# ---- the two failure modes, and why one gate is not enough ------------------------------------------
# An adversarial review found the first version reported a 1.06 deg warning on a PERFECT sensor once the
# posture spread narrowed to 5 degrees. The residual gate cannot see that: it FELL (0.0054 -> 0.0018) as
# the fit degenerated, because an ill-conditioned design fits its own noise confidently.

test_that("a narrow posture range is declined on conditioning, not reported as an error", {
  iss <- .aclRun(.aclSim(sd_deg = 5))            # perfect sensor, too little posture spread
  expect_equal(nrow(iss), 1L)
  expect_equal(iss$severity, "info")
  expect_match(iss$message, "posture range is too narrow")
  expect_false(any(grepl("offset \\(|gain \\(", iss$message)))
})

test_that("the residual gate and the conditioning gate catch OPPOSITE failures", {
  # narrow posture: low residual, huge condition -> only the conditioning gate fires
  narrow <- .aclRun(.aclSim(sd_deg = 5))
  expect_match(narrow$message, "condition")
  # one fixed attitude plus heavy specific acceleration: low condition is irrelevant, residual fires
  set.seed(12); n <- 60000
  A <- cbind(rep(0, n), rep(0, n), rep(1, n)) + matrix(stats::rnorm(3 * n, 0, 0.25), ncol = 3)
  d <- data.table::data.table(ID = "A01",
        datetime = as.POSIXct("2020-01-01", tz = "UTC") + seq_len(n) / 10,
        ax = A[, 1], ay = A[, 2], az = A[, 3], depth = 0)
  data.table::setattr(d, "nautilus.version", "test")
  iss <- .aclRun(d)
  expect_equal(nrow(iss), 1L)
  expect_equal(iss$severity, "info")
})

test_that("a heave-axis offset is caught - the lateral-pair-only bound missed it entirely", {
  # verified before the fix: z-offsets of 0.02-0.15 g were invisible to BOTH accel.scale and this check,
  # while producing ~2 deg of real pitch error
  iss <- .aclRun(.aclSim(offset = c(0, 0, 0.10)))
  off <- iss[grepl("offset", iss$message), , drop = FALSE]
  expect_equal(nrow(off), 1L)
  expect_gt(off$metric, 1)
  sc <- suppressWarnings(suppressMessages(
    checkSensorIntegrity(list(A01 = .aclSim(offset = c(0, 0, 0.10))), checks = "accel.scale",
                         apply = FALSE, return.data = FALSE, verbose = FALSE)))
  sc <- if (is.data.frame(sc)) sc else sc$issues
  expect_equal(nrow(sc), 0L)                     # still invisible to the magnitude check
})

test_that("the reported angle is evaluated over observed postures, not an unreachable worst case", {
  # the all-orientation bound atan(|c|) is true but loose - an animal that never inverts cannot attain
  # it. Same offset, wider posture range -> a LARGER realised error, which a fixed bound could not show.
  narrow <- .aclRun(.aclSim(offset = c(0, 0, 0.08), sd_deg = 15))
  wide   <- .aclRun(.aclSim(offset = c(0, 0, 0.08), sd_deg = 35))
  mn <- narrow[grepl("offset", narrow$message), "metric"]
  mw <- wide[grepl("offset", wide$message), "metric"]
  expect_true(length(mn) == 1L && length(mw) == 1L)
  expect_gt(mw, mn)
})

test_that("the conditioning threshold is exposed and validated", {
  expect_true("accel.calibration.condition" %in% names(integrityControl()))
  expect_error(integrityControl(accel.calibration.condition = 0), "must be")
  # relaxing it lets the degenerate fit through, which is what makes it the operative gate
  iss <- .aclRun(.aclSim(sd_deg = 5), control = integrityControl(accel.calibration.condition = 1e9))
  expect_false(identical(iss$severity, "info") && grepl("condition", iss$message))
})
