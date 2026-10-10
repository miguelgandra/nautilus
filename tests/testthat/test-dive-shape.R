# Geometric shape rules are opt-in and independent of phase labels and dive segmentation.

.dsProfile <- function(type = "V", t = 0:240) {
  vertices <- switch(type,
                     V = list(t = c(0, 120, 240), z = c(0, 40, 0)),
                     U = list(t = c(0, 50, 190, 240), z = c(0, 40, 40, 0)),
                     W = list(t = c(0, 60, 120, 180, 240), z = c(0, 40, 10, 38, 0)),
                     other = list(t = c(0, 80, 160, 240), z = c(0, 40, 40, 0)))
  stats::approx(vertices$t, vertices$z, xout = t)$y
}

.dsClassify <- function(z, t = seq_along(z) - 1, control = diveShapeControl(),
                         baseline = rep(0, length(z)), direction = "down", complete = TRUE,
                         resolution = 0, contiguous = TRUE) {
  nautilus:::.classifyDiveShapeOne(z, baseline, t, direction, complete, control,
                                  resolution, contiguous)
}

.dsTag <- function(type, id = type, direction = "down", phase.method = "vertical.rate") {
  z <- c(rep(0, 30), .dsProfile(type), rep(0, 30))
  baseline <- if (direction == "up") 100 else 0
  depth <- baseline + if (direction == "up") -z else z
  dt <- data.table::data.table(ID = id, datetime = as.POSIXct("2020-01-01", tz = "UTC") +
                               seq_along(z) - 1, depth = depth)
  meta <- nautilus:::.newNautilusMeta(); meta$id <- id
  tag <- nautilus:::new_nautilus_tag(dt, meta)
  if (direction == "up") {
    # Fix the known reference after detection rather than estimating it from this short fixture.
    tag$depth <- z
  }
  out <- detectDives(
    tag,
    control = diveControl(reference = "surface", require.zoc = "ignore", depth.threshold = 1,
                          surface.band = 0.5, min.duration = 10, min.prominence = NULL,
                          phase.method = phase.method),
    verbose = FALSE
  )[[1]]
  if (direction == "up") {
    out$depth <- depth
    out$depth_baseline <- rep(100, length(z))
    m <- nautilus:::.getMeta(out)
    m$processing[[length(m$processing)]]$direction <- "up"
    out <- nautilus:::.restoreMeta(out, m)
  }
  out
}

test_that("shape controls validate thresholds, units and scalar values", {
  ctl <- diveShapeControl()
  expect_s3_class(ctl, "nautilus_dive_shape")
  expect_identical(ctl$min.samples, 20L)
  expect_null(ctl$min.excursion.amplitude)
  expect_identical(diveShapeControl(min.excursion.amplitude = 0)$min.excursion.amplitude, 0)
  expect_error(diveShapeControl(v.max.broadness = 0.8, u.min.broadness = 0.7), "smaller")
  expect_error(diveShapeControl(v.max.broadness = -1), "between")
  expect_error(diveShapeControl(u.min.broadness = 2), "between")
  expect_error(diveShapeControl(peak.prominence = 0), "greater than zero")
  expect_error(diveShapeControl(peak.prominence = Inf), "finite")
  expect_error(diveShapeControl(min.peak.amplitude = -1), "between")
  expect_error(diveShapeControl(min.peak.separation = NA_real_), "finite")
  expect_error(diveShapeControl(smooth.window = c(1, 2)), "single")
  expect_error(diveShapeControl(min.coverage = 1.1), "between")
  expect_error(diveShapeControl(max.gap = 0), "greater than zero")
  expect_error(diveShapeControl(min.samples = 4), "whole")
  expect_error(diveShapeControl(min.samples = 20.5), "whole")
  expect_error(diveShapeControl(min.limb.prop = 0), "greater than zero")
  expect_error(diveShapeControl(min.limb.prop = 0.6), "between")
  expect_error(diveShapeControl(max.opposite.prop = -0.1), "between")
  expect_error(diveShapeControl(max.opposite.prop = 0.6), "between")
  expect_error(diveShapeControl(min.excursion.amplitude = -1), "between")
  for (value in list(NA_real_, Inf, c(1, 2), numeric(0), "10", TRUE))
    expect_error(diveShapeControl(min.excursion.amplitude = value), "single, finite number")
})

test_that("canonical V, U and W profiles receive transparent descriptors", {
  for (type in c("V", "U", "W")) {
    x <- .dsClassify(.dsProfile(type))
    expect_identical(x$dive_shape, type)
    expect_identical(x$dive_shape_status, "classified")
    expect_identical(x$shape_n_peaks, if (type == "W") 2L else 1L)
    expect_true(x$shape_broadness >= 0 && x$shape_broadness <= 1)
    expect_gte(x$shape_prominence_m, 0.5)
  }
  triangle <- .dsClassify(.dsProfile(), control = diveShapeControl(smooth.window = 0))
  expect_equal(triangle$shape_broadness, 0.5)
  middle <- .dsClassify(.dsProfile("other"))
  expect_identical(middle$dive_shape, "other")
  expect_identical(middle$dive_shape_status, "intermediate")
  expect_gt(middle$shape_broadness, 0.60)
  expect_lt(middle$shape_broadness, 0.75)
})

test_that("classification is invariant to excursion direction, baseline and time reversal", {
  for (type in c("V", "U", "W")) {
    z <- .dsProfile(type)
    baseline <- seq(100, 150, length.out = length(z))
    down <- .dsClassify(baseline + z, baseline = baseline)
    up <- .dsClassify(baseline - z, baseline = baseline, direction = "up")
    both_down <- .dsClassify(z, direction = "both")
    both_up <- .dsClassify(-z, direction = "both")
    inferred <- .dsClassify(-z, direction = NULL)
    reverse <- .dsClassify(rev(z))
    expect_identical(down$dive_shape, type)
    expect_identical(up$dive_shape, type)
    expect_identical(both_down$dive_shape, type)
    expect_identical(both_up$dive_shape, type)
    expect_identical(inferred$dive_shape, type)
    expect_identical(reverse$dive_shape, type)
    expect_equal(down$shape_broadness, up$shape_broadness)
    expect_equal(reverse$shape_broadness, down$shape_broadness)
  }
  crossing <- stats::approx(c(0, 60, 120, 180, 240), c(0, 40, 0, -30, 0), xout = 0:240)$y
  mixed <- .dsClassify(crossing, direction = "both")
  expect_true(is.na(mixed$dive_shape))
  expect_identical(mixed$dive_shape_status, "ambiguous_direction")
  complex <- stats::approx(c(0, 30, 100, 170, 240), c(0, -15, 40, -15, 0), xout = 0:240)$y
  other <- .dsClassify(complex)
  expect_identical(other$dive_shape, "other")
  expect_identical(other$dive_shape_status, "complex_profile")
  expect_true(is.na(other$shape_broadness))
})

test_that("relative thresholds transfer across scales while absolute floors still apply", {
  ctl <- diveShapeControl(min.peak.amplitude = 0)
  for (type in c("V", "U", "W")) {
    a <- .dsClassify(.dsProfile(type), control = ctl)
    b <- .dsClassify(10 * .dsProfile(type), control = ctl)
    expect_identical(a$dive_shape, b$dive_shape)
    expect_equal(a$shape_broadness, b$shape_broadness)
    expect_equal(b$shape_prominence_m, 10 * a$shape_prominence_m)
  }
  expect_identical(.dsClassify(.dsProfile() / 100)$dive_shape_status, "insufficient_resolution")
  expect_identical(.dsClassify(.dsProfile(), resolution = 50)$dive_shape_status,
                   "insufficient_resolution")
})

test_that("minimum excursion amplitude withholds labels without changing peak rules", {
  for (type in c("V", "U", "W", "other")) {
    z <- .dsProfile(type) / 5
    plain <- .dsClassify(z)
    zero <- .dsClassify(z, control = diveShapeControl(min.excursion.amplitude = 0))
    permissive <- .dsClassify(z, control = diveShapeControl(min.excursion.amplitude = 5))
    expect_identical(zero, plain)
    expect_identical(permissive, plain)
    blocked <- .dsClassify(z, control = diveShapeControl(min.excursion.amplitude = 10))
    expect_identical(blocked$dive_shape, NA_character_)
    expect_identical(blocked$dive_shape_status, "below_min_amplitude")
    expect_identical(blocked$shape_broadness, NA_real_)
    expect_identical(blocked$shape_n_peaks, NA_integer_)
    expect_identical(blocked$shape_prominence_m, NA_real_)
    expect_identical(.dsClassify(.dsProfile(type),
                                 control = diveShapeControl(min.excursion.amplitude = 10)),
                     .dsClassify(.dsProfile(type)))
  }
})

test_that("excursion eligibility uses prepared height and includes the exact threshold", {
  t <- 0:240
  z <- .dsProfile() / 5
  ctl <- diveShapeControl(smooth.window = 0, min.excursion.amplitude = 8)
  expect_identical(.dsClassify(z, control = ctl)$dive_shape, "V")
  strict <- diveShapeControl(smooth.window = 0, min.excursion.amplitude = 8.01)
  chord <- seq(100, 105, length.out = length(z))
  for (profile in list(z, chord + z, 100 - z, rev(chord + z))) {
    direction <- if (identical(profile, 100 - z)) "up" else "down"
    allowed <- .dsClassify(profile, direction = direction, control = ctl)
    blocked <- .dsClassify(profile, direction = direction, control = strict)
    expect_identical(allowed$dive_shape, "V")
    expect_equal(allowed$shape_broadness, 0.5)
    expect_identical(blocked$dive_shape_status, "below_min_amplitude")
  }
  baseline <- seq(100, 150, length.out = length(z))
  expect_identical(.dsClassify(baseline + z, baseline = baseline, control = strict)$dive_shape_status,
                   "below_min_amplitude")
  # Eligibility is measured after smoothing, which slightly lowers this sharp apex.
  smoothed <- .dsClassify(z, control = diveShapeControl(min.excursion.amplitude = 8))
  expect_identical(smoothed$dive_shape_status, "below_min_amplitude")
  expect_identical(.dsClassify(z, complete = FALSE, control = strict)$dive_shape_status, "censored")
  expect_identical(.dsClassify(z, resolution = 10, control = strict)$dive_shape_status,
                   "insufficient_resolution")
})

test_that("prominence and temporal separation control W assignment", {
  shallow <- stats::approx(c(0, 60, 120, 180, 240), c(0, 40, 35, 39, 0), xout = 0:240)$y
  permissive <- .dsClassify(shallow, control = diveShapeControl(smooth.window = 0, peak.prominence = 0.05))
  strict <- .dsClassify(shallow, control = diveShapeControl(smooth.window = 0, peak.prominence = 0.25))
  expect_identical(permissive$dive_shape, "W")
  expect_identical(strict$shape_n_peaks, 1L)
  expect_false(identical(strict$dive_shape, "W"))
  separated <- .dsClassify(.dsProfile("W"), control = diveShapeControl(min.peak.separation = 200))
  expect_identical(separated$shape_n_peaks, 1L)
  expect_false(identical(separated$dive_shape, "W"))
  plateau <- nautilus:::.diveShapePeaks(c(0:10, rep(10, 20), 9:0), 0:40, 1, 0)
  expect_identical(plateau, 21L)
  repeated <- c(0, 5, 10, 9, 8, 9, 10, 5, 0)
  peak <- nautilus:::.diveShapePeaks(repeated, 0:8, 3, 0)
  expect_identical(peak, 3L)
  expect_equal(repeated[peak], 10)
})

test_that("time-weighted preparation is stable across sampling rates and unequal sample density", {
  for (type in c("V", "U", "W")) {
    coarse <- .dsClassify(.dsProfile(type), t = 0:240)
    t <- seq(0, 240, by = 0.2)
    fine <- .dsClassify(.dsProfile(type, t), t = t)
    expect_identical(fine$dive_shape, coarse$dive_shape)
    expect_equal(fine$shape_broadness, coarse$shape_broadness, tolerance = 0.002)
    expect_identical(fine$shape_n_peaks, coarse$shape_n_peaks)
  }
  t <- sort(unique(c(seq(0, 240, by = 8), seq(50, 190, by = 0.1), 50, 190)))
  ctl <- diveShapeControl(smooth.window = 0, max.gap = 10)
  dense_bottom <- .dsClassify(.dsProfile("U", t), t = t, control = ctl)
  regular <- .dsClassify(.dsProfile("U"), control = ctl)
  expect_equal(dense_bottom$shape_broadness, regular$shape_broadness)
  expect_identical(dense_bottom$dive_shape, "U")
})

test_that("smoothing is time based, endpoint safe and explicitly limited", {
  t <- c(0, 1, 4, 10, 13)
  expect_equal(nautilus:::.diveShapeSmooth(rep(7, 5), t, 3), rep(7, 5))
  expect_equal(nautilus:::.diveShapeSmooth(t, t, 2)[2:4], t[2:4])
  expect_equal(nautilus:::.diveShapeSmooth(t, t, 0), t)
  wide <- .dsClassify(.dsProfile(), control = diveShapeControl(smooth.window = 61))
  expect_true(is.na(wide$dive_shape))
  expect_identical(wide$dive_shape_status, "insufficient_resolution")
})

test_that("noise and quantisation floors suppress insignificant oscillations", {
  z <- .dsProfile("U") + 0.15 * sin((0:240) * 2)
  floor <- nautilus:::.diveShapeResolution(z)
  result <- .dsClassify(z, resolution = floor)
  expect_identical(result$dive_shape, "U")
  expect_identical(result$shape_n_peaks, 1L)
  lattice <- round(.dsProfile() / 2) * 2
  expect_gte(nautilus:::.diveShapeResolution(lattice), 4)
  quantised <- .dsClassify(lattice, resolution = nautilus:::.diveShapeResolution(lattice))
  expect_identical(quantised$dive_shape, "V")
  expect_gte(quantised$shape_prominence_m, 4)
})

test_that("quality failures abstain explicitly instead of manufacturing a shape", {
  z <- .dsProfile()
  expect_identical(.dsClassify(z, complete = FALSE)$dive_shape_status, "censored")
  expect_identical(.dsClassify(z, contiguous = FALSE)$dive_shape_status, "noncontiguous")
  expect_identical(.dsClassify(z[1:10])$dive_shape_status, "insufficient_samples")
  expect_identical(.dsClassify(rep(20, 241))$dive_shape_status, "insufficient_resolution")
  expect_identical(.dsClassify(seq(0, 40, length.out = 241))$dive_shape_status, "insufficient_limbs")
  t <- 0:240; t[100] <- t[99]
  expect_identical(.dsClassify(z, t = t)$dive_shape_status, "invalid_time")
  t[100] <- NA_real_
  expect_identical(.dsClassify(z, t = t)$dive_shape_status, "invalid_time")
  baseline <- rep(0, 241); baseline[100] <- NA_real_
  expect_identical(.dsClassify(z, baseline = baseline)$dive_shape_status, "missing_reference")
  missing <- z; missing[70:100] <- NA_real_
  expect_identical(.dsClassify(missing)$dive_shape_status, "low_coverage")
  missing <- z; missing[70:76] <- NA_real_
  expect_identical(.dsClassify(missing)$dive_shape_status, "gap")
  missing <- z; missing[1] <- NA_real_
  expect_identical(.dsClassify(missing)$dive_shape_status, "insufficient_limbs")
  missing <- z; missing[70:71] <- NA_real_
  interpolated <- .dsClassify(missing)
  expect_identical(interpolated$dive_shape, "V")
  expect_equal(interpolated$shape_broadness, .dsClassify(z)$shape_broadness)
  # A jump in the timestamps is also a gap, even with 100% finite depth.
  t <- 0:240; t[121:241] <- t[121:241] + 30
  expect_identical(.dsClassify(z, t = t)$dive_shape_status, "gap")
})

test_that("opting in preserves all original metrics, source data and phase annotations", {
  tags <- lapply(c("V", "U", "W"), .dsTag)
  original <- lapply(tags, data.table::copy)
  plain <- diveMetrics(tags, verbose = FALSE)
  enabled <- diveMetrics(tags, shape = diveShapeControl(), verbose = FALSE)
  expect_identical(enabled$dive_shape, c("V", "U", "W"))
  added <- c("dive_shape", "dive_shape_status", "shape_broadness", "shape_n_peaks", "shape_prominence_m")
  expect_identical(setdiff(names(enabled), names(plain)), added)
  restored <- enabled[, names(plain), drop = FALSE]
  attr(restored, "shape_classification") <- NULL
  expect_identical(restored, plain)
  for (i in seq_along(tags)) expect_identical(tags[[i]], original[[i]])
  contract <- attr(enabled, "shape_classification")
  expect_identical(contract$method, "profile_rules")
  expect_identical(contract$version, 2L)
  expect_identical(contract$control, diveShapeControl())
  expect_null(attr(plain, "shape_classification"))
  expect_identical(diveMetrics(tags, shape = NULL, verbose = FALSE), plain)
  expect_equal(diveMetrics(tags, shape = list(), verbose = FALSE), enabled)
  expect_error(diveMetrics(tags, shape = list(bogus = 1), verbose = FALSE), "unknown field")
  expect_error(diveMetrics(tags, shape = TRUE, verbose = FALSE), "must be created")
})

test_that("amplitude eligibility preserves dive rows, summaries, inputs and provenance", {
  small <- .dsTag("V", id = "small")
  small$depth <- small$depth / 5
  small$depth_baseline <- small$depth_baseline / 5
  small$temp <- rep(22, nrow(small))
  large <- .dsTag("W", id = "large")
  large$temp <- rep(24, nrow(large))
  tags <- list(small, large)
  original <- lapply(tags, data.table::copy)
  ctl <- diveShapeControl(min.excursion.amplitude = 10)
  unrestricted <- diveMetrics(tags, variables = "temp", shape = diveShapeControl(), verbose = FALSE)
  restricted <- diveMetrics(tags, variables = "temp", shape = ctl, verbose = FALSE)
  expect_identical(names(restricted), names(unrestricted))
  expect_identical(restricted$ID, c("small", "large"))
  expect_identical(restricted$dive_shape, c(NA_character_, "W"))
  expect_identical(restricted$dive_shape_status, c("below_min_amplitude", "classified"))
  shape_columns <- c("dive_shape", "dive_shape_status", "shape_broadness", "shape_n_peaks",
                     "shape_prominence_m")
  other_columns <- setdiff(names(restricted), shape_columns)
  expect_identical(restricted[other_columns], unrestricted[other_columns], ignore_attr = TRUE)
  expect_equal(restricted$temp_mean, c(22, 24))
  for (i in seq_along(tags)) expect_identical(tags[[i]], original[[i]])
  expect_identical(attr(restricted, "shape_classification")$control, ctl)
  expect_equal(diveMetrics(tags, variables = "temp", shape = list(min.excursion.amplitude = 10),
                          verbose = FALSE), restricted)
  path <- tempfile(fileext = ".rds"); on.exit(unlink(path), add = TRUE)
  saveRDS(small, path)
  expect_equal(diveMetrics(path, variables = "temp", shape = ctl, verbose = FALSE),
               diveMetrics(small, variables = "temp", shape = ctl, verbose = FALSE))
  empty <- data.table::copy(small); empty$dive_id <- 0L
  result <- diveMetrics(empty, variables = "temp", shape = ctl, verbose = FALSE)
  expect_equal(nrow(result), 0L)
  expect_identical(names(result), names(restricted))
  expect_identical(attr(result, "shape_classification")$control, ctl)
  withr::local_options(list(cli.width = 200))
  normal <- cli::cli_fmt(invisible(diveMetrics(tags, shape = ctl, verbose = "normal")))
  expect_match(paste(normal, collapse = "\n"),
               "shape withheld for 1 dive below 10 m excursion amplitude")
  detailed <- cli::cli_fmt(invisible(diveMetrics(tags, shape = ctl, verbose = "detailed")))
  expect_match(paste(detailed, collapse = "\n"), "below_min_amplitude: 1")
  quiet <- cli::cli_fmt(invisible(diveMetrics(tags, shape = ctl, verbose = FALSE)))
  expect_length(quiet, 0L)
})

test_that("phase methods and realised directions do not redefine geometric classes", {
  a <- diveMetrics(.dsTag("V"), shape = diveShapeControl(), verbose = FALSE)
  b <- diveMetrics(.dsTag("V", phase.method = "prop.depth"), shape = diveShapeControl(), verbose = FALSE)
  expect_identical(a$phase_structure, "DA")
  expect_identical(b$phase_structure, "DBA")
  expect_identical(a$dive_shape, b$dive_shape)
  expect_equal(a$shape_broadness, b$shape_broadness)
  no_phases <- .dsTag("V")
  no_phases$dive_phase <- factor(ifelse(no_phases$dive_id > 0, "bottom", "inter_dive"),
                                levels = c("descent", "bottom", "ascent", "inter_dive"))
  independent <- diveMetrics(no_phases, shape = diveShapeControl(), verbose = FALSE)
  expect_false(independent$shape_supported)
  expect_identical(independent$dive_shape, "V")
  up <- diveMetrics(.dsTag("W", direction = "up"), shape = diveShapeControl(), verbose = FALSE)
  expect_identical(up$dive_shape, "W")
  expect_equal(up$shape_broadness, diveMetrics(.dsTag("W"), shape = diveShapeControl(),
                                            verbose = FALSE)$shape_broadness)
})

test_that("empty, mixed-quality, custom-column and file inputs preserve the shape schema", {
  good <- .dsTag("V")
  none <- data.table::copy(good); none$dive_id <- 0L; none$dive_phase <- factor("inter_dive")
  empty <- diveMetrics(none, shape = diveShapeControl(), variables = "vedba", verbose = FALSE)
  full <- diveMetrics(good, shape = diveShapeControl(), variables = "vedba", verbose = FALSE)
  expect_equal(nrow(empty), 0L)
  expect_identical(names(empty), names(full))
  expect_type(empty$shape_n_peaks, "integer")
  expect_type(empty$shape_broadness, "double")
  expect_identical(attr(empty, "shape_classification"), attr(full, "shape_classification"))
  censored <- data.table::copy(good)
  censored$dive_id[seq_len(which(censored$dive_id > 0)[1])] <- 1L
  bad <- diveMetrics(censored, shape = diveShapeControl(), verbose = FALSE)
  expect_identical(bad$dive_shape_status, "censored")
  expect_true(is.na(bad$dive_shape))
  expect_identical(names(bad), names(diveMetrics(good, shape = diveShapeControl(), verbose = FALSE)))
  noncontiguous <- data.table::copy(good); noncontiguous$dive_id[100] <- 0L
  expect_identical(diveMetrics(noncontiguous, shape = diveShapeControl(), verbose = FALSE)$dive_shape_status,
                   "noncontiguous")
  renamed <- data.table::copy(good)
  data.table::setnames(renamed, c("datetime", "depth"), c("time", "pressure_depth"))
  expect_equal(diveMetrics(renamed, datetime.col = "time", depth.col = "pressure_depth",
                          shape = diveShapeControl(), verbose = FALSE),
               diveMetrics(good, shape = diveShapeControl(), verbose = FALSE))
  path <- tempfile(fileext = ".rds"); on.exit(unlink(path), add = TRUE)
  saveRDS(good, path)
  expect_equal(diveMetrics(path, shape = diveShapeControl(), verbose = FALSE),
               diveMetrics(good, shape = diveShapeControl(), verbose = FALSE))
})

test_that("prominence splitting changes the classification unit, not the classifier's boundaries", {
  retained <- .dsTag("W")
  unsplit <- diveMetrics(retained, shape = diveShapeControl(), verbose = FALSE)
  expect_identical(unsplit$dive_shape, "W")
  split <- detectDives(
    retained,
    control = diveControl(reference = "surface", require.zoc = "ignore", depth.threshold = 1,
                          surface.band = 0.5, min.duration = 10, min.prominence = 15),
    verbose = FALSE
  )
  result <- diveMetrics(split, shape = diveShapeControl(), verbose = FALSE)
  expect_equal(nrow(result), 2L)
  expect_false(any(result$dive_shape == "W", na.rm = TRUE))
  expect_identical(unique(split[[1]]$dive_id), c(0L, 1L, 2L))
})
