.circle_tag <- function(id = "A", t = 0:180, rate = 6, pitch = 0) {
  data.frame(ID = id, datetime = as.POSIXct("2023-01-01", tz = "UTC") + t,
             heading = (rate * t) %% 360, pitch = pitch, depth = 20, temp = 24)
}
.circle_detect <- function(x, ...) detectCircling(x, verbose = "quiet", ...)

test_that("steady clockwise and counterclockwise rotations have correct geometry and units", {
  for (rate in c(6, -6)) {
    e <- .circle_detect(.circle_tag(rate = rate), smooth.window = 0)
    expect_equal(nrow(e), 1L)
    expect_s3_class(e, "nautilus_circling_events")
    expect_equal(e$n_rotations, 3)
    expect_equal(e$mean_turn_rate_deg_s, rate)
    expect_equal(e$rotation_period_s, 60)
    expect_equal(e$directionality, 1)
    expect_equal(e$turn_rate_cv, 0, tolerance = 1e-10)
    expect_equal(e$direction, if (rate > 0) "clockwise" else "counterclockwise")
    expect_true(e$censored)
  }
})

test_that("minimum net rotation equality is accepted", {
  e <- .circle_detect(.circle_tag(t = 0:120), smooth.window = 0)
  expect_equal(e$n_rotations, 2)
  expect_equal(nrow(.circle_detect(.circle_tag(t = 0:119), smooth.window = 0)), 0L)
})

test_that("wrap crossings and a constant north-reference offset do not change detection", {
  x <- .circle_tag()
  a <- .circle_detect(x)
  x$heading <- (x$heading + 179) %% 360
  b <- .circle_detect(x)
  expect_equal(a, b)
  x$heading <- x$heading + 720
  expect_equal(.circle_detect(x), b)
})

test_that("heading oscillations and biased back-and-forth turning are not fictitious circles", {
  x <- .circle_tag(t = 0:600)
  x$heading <- (20 * sin(2 * pi * (0:600) / 30)) %% 360
  expect_equal(nrow(.circle_detect(x)), 0L)
  delta <- rep(c(rep(4, 5), rep(-3, 4)), 100)
  x <- .circle_tag(t = 0:length(delta))
  x$heading <- c(0, cumsum(delta)) %% 360
  expect_equal(nrow(.circle_detect(x, smooth.window = 0)), 0L)
  loose <- .circle_detect(x, smooth.window = 0, min.directionality = 0.2)
  expect_gte(loose$n_rotations, 2)
  expect_lt(loose$directionality, 0.3)
})

test_that("rates use elapsed seconds and are independent of native sampling rate", {
  a <- .circle_detect(.circle_tag(), smooth.window = 0)
  b <- .circle_detect(.circle_tag(t = seq(0, 180, by = 0.02)), smooth.window = 0)
  expect_equal(b$n_rotations, a$n_rotations, tolerance = 1e-7)
  expect_equal(b$mean_turn_rate_deg_s, a$mean_turn_rate_deg_s, tolerance = 1e-6)
  irregular <- c(0, cumsum(rep(c(0.5, 1.5), 100)))
  e <- .circle_detect(.circle_tag(t = irregular))
  expect_equal(e$mean_turn_rate_deg_s, 6, tolerance = 1e-7)
  expect_equal(e$directionality, 1)
  expect_true(all(diff(as.numeric(attr(e, "assessed_intervals")$start)) >= 0))
})

test_that("smoothing has explicit support edges and does not average wrapped angles", {
  e <- .circle_detect(.circle_tag())
  expect_equal(e$mean_turn_rate_deg_s, 6)
  expect_equal(e$duration_s, 174)
  a <- attr(e, "assessed_intervals")
  expect_equal(as.numeric(a$end - a$start, units = "secs"), 174)
  expect_equal(as.numeric(e$start - .circle_tag()$datetime[1], units = "secs"), 3)
})

test_that("direction changes split and backdate the second event without losing its first steps", {
  delta <- c(rep(6, 180), rep(-6, 180))
  x <- .circle_tag(t = 0:length(delta)); x$heading <- c(0, cumsum(delta)) %% 360
  e <- .circle_detect(x, smooth.window = 0)
  expect_equal(e$direction, c("clockwise", "counterclockwise"))
  expect_equal(e$n_rotations, c(3, 3))
  expect_equal(e$end[1], e$start[2])
  expect_equal(e$circling_id, 1:2)
})

test_that("short pauses are included but longer pauses split events", {
  make <- function(n) {
    delta <- c(rep(6, 180), rep(0, n), rep(6, 180))
    x <- .circle_tag(t = 0:length(delta)); x$heading <- c(0, cumsum(delta)) %% 360
    x
  }
  e <- .circle_detect(make(5), smooth.window = 0)
  expect_equal(nrow(e), 1L)
  expect_equal(e$n_rotations, 6)
  expect_equal(e$duration_s, 365)
  expect_gt(e$turn_rate_cv, 0)
  e <- .circle_detect(make(6), smooth.window = 0)
  expect_equal(nrow(e), 2L)
  expect_equal(e$duration_s, c(180, 180))
})

test_that("gaps cannot accumulate rotations or prolong an observed event", {
  t <- c(0:180, 3600 + (181:360))
  e <- .circle_detect(.circle_tag(t = t), smooth.window = 0)
  expect_equal(nrow(e), 2L)
  expect_lt(max(e$duration_s), 181)
  expect_equal(nrow(attr(e, "assessed_intervals")), 2L)
  t <- c(0:90, 3600 + (91:180))
  e <- .circle_detect(.circle_tag(t = t), smooth.window = 0)
  expect_equal(nrow(e), 0L)  # two incomplete rotations must not be combined across a blackout
  expect_equal(attr(e, "circling_detection")$deployments$n_events, 0L)
})

test_that("invalid heading, vertical posture and optional raw-rate limits break assessment", {
  x <- .circle_tag(t = 0:600)
  x$pitch[251:350] <- 85
  e <- .circle_detect(x, smooth.window = 0)
  expect_equal(nrow(e), 2L)
  expect_equal(nrow(attr(e, "assessed_intervals")), 2L)
  labelled <- annotateData(x, e, verbose = "quiet")[[1]]
  expect_true(all(is.na(labelled$circling[251:350])))
  x$pitch <- 0; x$heading[251:350] <- NA_real_
  e <- .circle_detect(x, smooth.window = 0)
  expect_equal(nrow(e), 2L)
  x <- .circle_tag(t = 0:270); x$heading[101:271] <- (x$heading[101:271] + 90) %% 360
  e <- .circle_detect(x, smooth.window = 0, max.turn.rate = 10)
  expect_equal(nrow(e), 1L)
  expect_equal(e$mean_turn_rate_deg_s, 6)
  expect_gte(e$start, x$datetime[101])
})

test_that("exact half-turn steps abstain instead of choosing an arbitrary direction", {
  x <- .circle_tag()
  x$heading[91:181] <- (x$heading[91:181] + 174) %% 360 # step 6 + 174 = 180
  expect_equal(nrow(.circle_detect(x, smooth.window = 0)), 0L)
})

test_that("recorded mounting pitch offsets are undone only for the posture screen", {
  x <- data.table::as.data.table(.circle_tag(pitch = 65))
  meta <- nautilus:::.newNautilusMeta(); meta$id <- "A"
  meta <- nautilus:::.appendProcessing(meta, "processTagData", pitch_offset_deg = 20)
  x <- nautilus:::new_nautilus_tag(x, meta)
  expect_warning(e <- .circle_detect(x), "could not be assessed")
  expect_equal(nrow(e), 0L)
  expect_equal(attr(e, "circling_detection")$deployments$pitch_offset_deg, 20)
  expect_equal(x$pitch, rep(65, nrow(x)))
  expect_equal(nrow(.circle_detect(x, max.abs.pitch = NULL)), 1L)
})

test_that("unassessable and valid no-event deployments remain distinguishable in a batch", {
  a <- .circle_tag(); a$heading <- 0
  b <- .circle_tag(id = "B"); b$heading <- NA_real_
  expect_warning(e <- .circle_detect(list(A = a, B = b)), "B")
  expect_equal(nrow(e), 0L)
  report <- attr(e, "circling_detection")$deployments
  expect_equal(report$n_events, c(0L, NA_integer_))
  expect_equal(report$status[2], "not_assessed")
  labelled <- annotateData(list(A = a, B = b), e, verbose = "quiet")
  expect_true(any(labelled$A$circling == 0, na.rm = TRUE))
  expect_true(all(is.na(labelled$B$circling)))
})

test_that("missing pitch requires an explicit opt-out and bad timestamps do not contaminate batches", {
  x <- .circle_tag(); x$pitch <- NULL
  expect_warning(.circle_detect(x), "missing pitch")
  expect_equal(nrow(.circle_detect(x, max.abs.pitch = NULL)), 1L)
  x <- .circle_tag(); x$datetime[2] <- x$datetime[1]
  expect_warning(.circle_detect(x), "duplicate timestamps")
  x$datetime[2] <- NA
  expect_warning(.circle_detect(x), "timestamps")
})

test_that("covariates follow dive conventions and unavailable summaries are typed NA", {
  x <- .circle_tag(); x$vedba <- NA_real_
  e <- .circle_detect(x, variables = c("depth", "heading", "vedba", "absent"))
  expect_equal(e$depth_mean, 20)
  expect_equal(e$depth_sd, 0)
  expect_true(is.na(e$vedba_mean) && !is.nan(e$vedba_mean))
  expect_true(is.na(e$absent_sd))
  expect_true(all(c("heading_mean_angle", "heading_mrl") %in% names(e)))
})

test_that("file and memory inputs agree and caller objects are unchanged", {
  x <- data.table::as.data.table(.circle_tag())
  before <- serialize(x, NULL)
  a <- .circle_detect(list(A = x))
  expect_identical(serialize(x, NULL), before)
  f <- withr::local_tempfile(fileext = ".rds"); saveRDS(x, f)
  expect_equal(.circle_detect(f), a)
  shuffled <- x[rev(seq_len(nrow(x)))]
  expect_equal(.circle_detect(shuffled), a)
})

test_that("magnetic north is accepted but known raw magnetometer use warns", {
  x <- data.table::as.data.table(.circle_tag())
  meta <- nautilus:::.newNautilusMeta(); meta$id <- "A"
  meta$deployment$heading_reference <- "magnetic"
  x <- nautilus:::new_nautilus_tag(x, meta)
  expect_silent(.circle_detect(x))
  expect_warning(.circle_detect(x, variables = "heading"), "MAGNETIC heading")
  expect_silent(.circle_detect(x, variables = "depth"))
  meta$mag_calibration$status <- "uncalibrated_raw"
  x <- nautilus:::new_nautilus_tag(x, meta)
  expect_warning(.circle_detect(x), "uncalibrated magnetometer")
})

test_that("control arguments are validated without silently changing cutoffs", {
  x <- .circle_tag()
  expect_error(.circle_detect(x, min.rotations = 0), "greater than zero")
  expect_error(.circle_detect(x, min.directionality = 1.1), "min.directionality")
  expect_error(.circle_detect(x, max.gap = 0), "max.gap")
  expect_error(.circle_detect(x, max.abs.pitch = 90), "less than 90")
  expect_error(.circle_detect(x, max.turn.rate = 0.1), "at least")
  expect_error(.circle_detect(x, smooth.window = -1), "smooth.window")
  expect_error(.circle_detect(list(A = x, another = x)), "Duplicate deployment")
  expect_error(.circle_detect(character(0)), "empty")
})

test_that("mixed or inconsistent deployment IDs are not combined into fictitious events", {
  x <- .circle_tag(); x$ID[91:181] <- "B"
  expect_warning(e <- .circle_detect(list(A = x)), "dataset ID")
  expect_equal(nrow(e), 0L)
  expect_true(is.na(attr(e, "circling_detection")$deployments$n_events))
  x <- data.table::as.data.table(.circle_tag())
  meta <- nautilus:::.newNautilusMeta(); meta$id <- "wrong"
  x <- nautilus:::new_nautilus_tag(x, meta)
  expect_warning(.circle_detect(list(A = x)), "inconsistent with metadata")
})
