# Bottom validation is a phase-detection rule, not a consequence of geometric shape labels.
.dbPhase <- function(z, t = seq_along(z) - 1, control = diveControl(), noise = 0.01) {
  nautilus:::.divePhases(z, t, control,
                        list(phase.window = 5, min.phase.duration = 10), noise = noise,
                        dt = stats::median(diff(t)))
}

.dbShape <- function(z, phase, control = diveShapeControl(peak.scope = "bottom"), t = seq_along(z) - 1) {
  nautilus:::.classifyDiveShapeOne(z, rep(0, length(z)), t, "down", TRUE, control,
                                  phase = phase)
}

test_that("bottom and capped-prominence controls are explicit and validated", {
  expect_equal(diveControl()$bottom.max.directionality, 0.6)
  expect_null(diveControl(bottom.max.directionality = NULL)$bottom.max.directionality)
  for (v in list(-0.1, 1.1, NA_real_, Inf, TRUE, c(0.5, 0.8)))
    expect_error(diveControl(bottom.max.directionality = v))
  expect_identical(diveShapeControl()$peak.scope, "profile")
  expect_null(diveShapeControl()$peak.prominence.cap)
  expect_error(diveShapeControl(peak.scope = "descent"))
  for (v in list(0, -1, NA_real_, Inf, TRUE, c(10, 50)))
    expect_error(diveShapeControl(peak.prominence.cap = v))
  expect_error(diveShapeControl(peak.prominence.cap = 1, min.peak.amplitude = 2), "floor")
})

test_that("prominence caps preserve absolute and instrument floors", {
  t <- 0:600
  for (a in c(50, 500, 1400)) {
    z <- stats::approx(c(0, 300, 600), c(0, a, 0), xout = t)$y
    ctl <- diveShapeControl(smooth.window = 0, peak.prominence.cap = 50)
    x <- nautilus:::.classifyDiveShapeOne(z, rep(0, length(z)), t, "down", TRUE, ctl)
    expect_equal(x$shape_prominence_m, min(0.1 * a, 50))
  }
  z <- stats::approx(c(0, 300, 600), c(0, 1400, 0), xout = t)$y
  x <- nautilus:::.classifyDiveShapeOne(z, rep(0, length(z)), t, "down", TRUE,
                                      diveShapeControl(peak.prominence.cap = 50), resolution = 70)
  expect_equal(x$shape_prominence_m, 70)
})

test_that("bottom scope ignores transit peaks without renormalising the profile", {
  t <- 0:1000
  z <- stats::approx(c(0, 120, 180, 500, 1000), c(0, 50, 30, 100, 0), xout = t)$y
  phase <- ifelse(t < 490, "descent", ifelse(t > 510, "ascent", "bottom"))
  profile <- .dbShape(z, phase, diveShapeControl(peak.scope = "profile"))
  bottom <- .dbShape(z, phase)
  expect_identical(profile$dive_shape, "W")
  expect_identical(bottom$dive_shape, "V")
  expect_identical(bottom$shape_n_peaks, 1L)
  expect_equal(bottom$shape_broadness, profile$shape_broadness)
  expect_equal(bottom$shape_prominence_m, profile$shape_prominence_m)
  da <- ifelse(t <= 500, "descent", "ascent")
  no_bottom <- .dbShape(z, da)
  expect_identical(no_bottom$dive_shape, "V")
  expect_identical(no_bottom$shape_n_peaks, 0L)
})

test_that("bottom W retains full-profile boundary peaks and internal valleys", {
  t <- 0:600
  z <- stats::approx(c(0, 200, 300, 400, 600), c(0, 100, 70, 100, 0), xout = t)$y
  phase <- ifelse(t < 200, "descent", ifelse(t > 400, "ascent", "bottom"))
  ctl <- diveShapeControl(peak.scope = "bottom", smooth.window = 0)
  x <- .dbShape(z, phase, ctl)
  expect_identical(x$dive_shape, "W")
  expect_identical(x$shape_n_peaks, 2L)
  expect_equal(x$shape_prominence_m, 10)
  expect_equal(.dbShape(rev(z), rev(c(descent = "ascent", bottom = "bottom", ascent = "descent")[phase]), ctl)$shape_n_peaks, 2L)
  broken <- phase; broken[300] <- "descent"
  expect_identical(.dbShape(z, broken, ctl)$dive_shape_status, "unresolved_phases")
  missing <- phase; missing[300] <- NA_character_
  expect_identical(.dbShape(z, missing, ctl)$dive_shape_status, "unresolved_phases")
  expect_identical(.dbShape(z, rep("bottom", length(z)), ctl)$dive_shape_status, "unresolved_phases")
})

test_that("resolved movement is direction-symmetric and does not accumulate sub-resolution jitter", {
  f <- nautilus:::.diveResolvedMovement
  expect_equal(f(seq(0, 100, length.out = 1001), 1), c(net = 100, path = 100))
  expect_equal(f(rep(100, 1000), 1), c(net = 0, path = 0))
  jitter <- 100 + 0.1 * sin(seq_len(2000))
  expect_equal(f(jitter, 1)[["path"]], 0)
  z <- c(seq(0, 100, length.out = 301), seq(100, 40, length.out = 200), seq(40, 120, length.out = 500))
  expect_equal(f(z, 1), f(rev(z), 1))
  expect_equal(f(z, 1), f(-z, 1))
  expect_equal(f(z + 1000, 1), f(z, 1))
  expect_gte(f(z, 1)[["path"]], f(z, 1)[["net"]])
  expect_gt(f(c(0, 0.6, -0.6, 0.6, 0), 1)[["path"]], 0)
  expect_true(all(is.na(f(c(1, NA, 2), 1))))
})

test_that("public detection preserves boundaries and source data and records refinement", {
  z <- c(seq(0, 820, length.out = 200), seq(820, 1000, length.out = 600),
         rep(1000, 300), seq(1000, 0, length.out = 400))
  depth <- c(rep(0, 30), z, rep(0, 30))
  tab <- data.table::data.table(ID = "slow", datetime = as.POSIXct("2020-01-01", tz = "UTC") +
                                seq_along(depth), depth = depth, temp = 20)
  meta <- nautilus:::.newNautilusMeta(); meta$id <- "slow"
  tag <- nautilus:::new_nautilus_tag(tab, meta)
  before <- data.table::copy(tag)
  ctl <- diveControl(reference = "surface", require.zoc = "ignore", depth.threshold = 5,
                     surface.band = 2, min.duration = 10)
  unchecked <- ctl; unchecked["bottom.max.directionality"] <- list(NULL)
  old <- detectDives(tag, control = unchecked, verbose = FALSE)[[1]]
  new <- detectDives(tag, control = ctl, verbose = FALSE)[[1]]
  console <- testthat::capture_messages(detectDives(tag, control = ctl, verbose = TRUE))
  expect_match(paste(console, collapse = "\n"), "Candidate bottoms refined")
  expect_identical(tag, before)
  expect_identical(new$depth, old$depth)
  expect_identical(new$temp, old$temp)
  expect_identical(new$dive_id, old$dive_id)
  expect_identical(new$depth_baseline, old$depth_baseline)
  record <- tail(nautilus:::.getMeta(new)$processing, 1)[[1]]
  expect_identical(record$n_bottom_refined, 1L)
  expect_equal(record$bottom_max_directionality, 0.60)
  expect_equal(record$bottom_prop, 0.80)
  expect_identical(record$phase_version, 2L)
  legacy <- ctl; legacy$bottom.max.directionality <- NULL
  expect_equal(detectDives(tag, control = legacy, verbose = FALSE),
               detectDives(tag, control = ctl, verbose = FALSE), ignore_attr = TRUE)
  path <- tempfile(fileext = ".rds"); on.exit(unlink(path), add = TRUE)
  saveRDS(tag, path)
  disk <- detectDives(path, control = ctl, verbose = FALSE)[[1]]
  expect_identical(disk$dive_phase, new$dive_phase)
  shape <- diveShapeControl(peak.scope = "bottom", peak.prominence.cap = 50)
  metrics <- diveMetrics(new, shape = shape, verbose = FALSE)
  expect_identical(metrics$dive_shape, "U")
  expect_identical(attr(metrics, "shape_classification")$control, shape)
  saveRDS(new, path)
  expect_equal(diveMetrics(path, shape = shape, verbose = FALSE), metrics)
  pctl <- ctl; pctl$phase.method <- "prop.depth"
  pn <- detectDives(tag, control = pctl, verbose = FALSE)[[1]]
  pctl["bottom.max.directionality"] <- list(NULL)
  po <- detectDives(tag, control = pctl, verbose = FALSE)[[1]]
  expect_identical(pn$dive_phase, po$dive_phase)
})

test_that("short drift, noise and absent phase evidence do not create false W support", {
  t <- 0:600
  z <- stats::approx(c(0, 150, 450, 600), c(0, 100, 100, 0), xout = t)$y
  set.seed(214)
  noisy <- z + rnorm(length(z), sd = 0.03)
  p <- .dbPhase(noisy, noise = 0.03)$phase
  expect_gt(mean(p[t >= 160 & t <= 440] == "bottom"), 0.95)
  drift <- z; drift[t >= 150 & t <= 450] <- drift[t >= 150 & t <= 450] +
    seq(0, 0.05, length.out = 301)
  expect_gt(mean(.dbPhase(drift)$phase[t >= 160 & t <= 440] == "bottom"), 0.95)
  profile <- diveShapeControl(peak.scope = "profile")
  expect_false(is.na(.dbShape(z, NULL, profile)$dive_shape))
  expect_identical(.dbShape(z, NULL)$dive_shape_status, "unresolved_phases")
})

test_that("slow directional arrival is not bottom and a real plateau survives refinement", {
  # Fast descent followed by a slow but uninterrupted final descent, then the return limb.
  z <- c(seq(0, 820, length.out = 200), seq(820, 1000, length.out = 600),
         seq(1000, 0, length.out = 400))
  old <- .dbPhase(z, control = diveControl(bottom.max.directionality = NULL))
  new <- .dbPhase(z)
  expect_gt(sum(old$phase == "bottom"), 500)
  expect_lt(sum(new$phase == "bottom"), 30)
  # A long, genuinely level bottom after the slow approach must not disappear with that approach.
  u <- c(z[1:800], rep(1000, 300), z[801:1200])
  p <- .dbPhase(u)$phase
  expect_gt(sum(p[801:1100] == "bottom"), 280)
  expect_lt(sum(p[201:780] == "bottom"), 10)
})

test_that("bottom validation preserves oscillatory, flat and rounded turnarounds symmetrically", {
  for (hz in c(1, 20, 100)) {
    t <- seq(0, 1000, by = 1 / hz)
    z <- stats::approx(c(0, 250, 350, 450, 550, 650, 1000),
                       c(0, 1000, 920, 1030, 930, 1010, 0), xout = t)$y
    f <- .dbPhase(z, t = t)$phase
    b <- rev(.dbPhase(rev(z), t = t)$phase)
    swap <- c(descent = "ascent", bottom = "bottom", ascent = "descent")
    expect_lte(mean(unname(swap[f]) != b), 0.02)
    expect_gt(mean(f[t >= 270 & t <= 620] == "bottom"), 0.95)
    flat <- .dbPhase(rep(100, length(t)), t = t)$phase
    expect_true(all(flat == "bottom"))
  }
  t <- 0:1000
  rounded <- 100 * sin(pi * t / max(t))^2  # Rounded apex, but whole-profile broadness is 0.5.
  phase <- .dbPhase(rounded, t = t)$phase
  expect_gt(sum(phase == "bottom"), 0L)
  expect_identical(.dbShape(rounded, phase, t = t)$dive_shape, "V")
})

test_that("bottom validation never bridges non-finite depth or invalid timestamps", {
  t <- 0:200
  z <- stats::approx(c(0, 80, 150, 200), c(0, 80, 100, 0), xout = t)$y
  validate <- nautilus:::.diveValidateBottom
  unchanged <- list(first = 80L, last = 160L, refined = FALSE)
  missing <- z; missing[100] <- NA_real_
  expect_identical(validate(missing, t, 80L, 160L, 5, 1, 0.6), unchanged)
  duplicate <- t; duplicate[100] <- duplicate[99]
  expect_identical(validate(z, duplicate, 80L, 160L, 5, 1, 0.6), unchanged)
  invalid <- t; invalid[100] <- NA_real_
  expect_identical(validate(z, invalid, 80L, 160L, 5, 1, 0.6), unchanged)
  expect_identical(validate(z, t, 80L, 160L, 5, 1, NULL), unchanged)
})
