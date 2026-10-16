.overlay_fixture <- function(t = seq(0, 4, by = .125), freq = 1, amplitude = .2) {
  t0 <- as.POSIXct("2020-01-01", tz = "UTC")
  data.table::data.table(datetime = t0 + t, pitch = t, roll = 10*t,
                         heading = (359 + t) %% 360, depth = 5 + t,
                         vedba = .1 + .01*t, vertical_velocity = rep(.1, length(t)),
                         tbf_hz_peaks = rep_len(freq, length(t)),
                         tbf_amplitude_peaks = rep_len(amplitude, length(t)))
}

.overlay_tetrahedron <- function(...) {
  overlayModel(rbind(c(1, 0, 0), c(-1, .3, 0), c(-1, -.3, 0), c(-.5, 0, -.3)),
               rbind(c(1, 2, 3), c(1, 4, 2), c(1, 3, 4), c(2, 4, 3)), ...)
}

.overlay_motion <- function(d, ...) {
  nautilus:::.resolveOverlayMotion(overlayMotionControl(...), d,
                                   nautilus:::.prepareOverlayModel("shark"))
}

test_that("prepared built-in models preserve production geometry and share vertices", {
  for (name in c("shark", "cetacean", "turtle", "fish", "manta")) {
    old <- nautilus:::.tagModel3D(name)
    m <- nautilus:::.prepareOverlayModel(name, 20)
    expect_equal(m$part, old$part)
    expect_lt(nrow(m$vertices), sum(vapply(old$faces, ncol, integer(1))))
    for (i in seq_along(m$faces))
      expect_equal(t(m$vertices[m$faces[[i]], , drop = FALSE]), old$faces[[i]])
    expect_true(any(m$rig$weights > 0))
    expect_true(is.finite(m$display.scale) && m$display.scale > 0)
  }
})

test_that("custom models validate geometry, topology, weights and coordinate scale", {
  m <- .overlay_tetrahedron()
  expect_s3_class(m, "nautilus_overlay_model")
  expect_equal(max(apply(m$vertices, 2, function(x) diff(range(x)))), 2)
  bad <- m$vertices; bad[1, 1] <- Inf
  expect_error(overlayModel(bad, m$faces), "finite numeric matrix")
  expect_error(overlayModel(matrix(0, 3, 2), m$faces), "three columns")
  expect_error(overlayModel(m$vertices, rbind(c(1, 2, 99))), "one-based")
  expect_error(overlayModel(m$vertices, rbind(c(1, 2, 2))), "distinct")
  expect_error(overlayModel(m$vertices, rbind(c(1, 2, 3.5))), "one-based")
  expect_error(overlayModel(m$vertices, list(1:4)), "triangles")
  expect_error(overlayModel(rbind(c(0, 0, 0), c(1, 0, 0), c(2, 0, 0)), rbind(1:3)), "degenerate")
  expect_error(.overlay_tetrahedron(part = "body"), "per face")
  expect_error(.overlay_tetrahedron(motion = "lateral"), "weights")
  expect_error(.overlay_tetrahedron(motion = "lateral", weights = rep(0, 4)), "positive")
  expect_error(.overlay_tetrahedron(motion = "lateral", weights = c(0, 0, 1, 2)), "between zero and one")
  expect_error(.overlay_tetrahedron(pivot = c(0, NA, 0)), "pivot")
  expect_error(.overlay_tetrahedron(name = ""), "empty")
  expect_error(overlayModel(matrix(1, 5001, 3), rbind(1:3)), "5000")
  expect_error(overlayModel(m$vertices, rep(m$faces, 501)), "2000")
  m$faces[[1]][1] <- 100
  expect_error(nautilus:::.prepareOverlayModel(m), "one-based")
})

test_that("deformation fixes zero-weight vertices and respects fixed display bounds", {
  for (name in c("shark", "cetacean", "turtle", "fish", "manta")) {
    m <- nautilus:::.prepareOverlayModel(name, 30)
    rest <- m$vertices
    expect_equal(nautilus:::.deformOverlayModel(m, 1, 0, 30, pi/3), rest)
    expect_equal(nautilus:::.deformOverlayModel(m, 1, 1, 0, pi/3), rest)
    for (phase in seq(0, 2*pi, length.out = 25)) {
      v <- nautilus:::.deformOverlayModel(m, phase, 1, 30, pi/3)
      expect_equal(v[m$rig$weights == 0, , drop = FALSE], rest[m$rig$weights == 0, , drop = FALSE])
      expect_lte(max(sqrt(rowSums(v^2))) * m$display.scale, 1.08 + 1e-12)
    }
    expect_false(isTRUE(all.equal(nautilus:::.deformOverlayModel(m, pi/2, 1, 30, 0), rest)))
  }
  custom <- .overlay_tetrahedron(motion = "lateral", weights = c(0, 1, 1, .5), pivot = c(0, 0, 0))
  custom <- nautilus:::.prepareOverlayModel(custom, 20)
  expect_false(isTRUE(all.equal(nautilus:::.deformOverlayModel(custom, pi/2, 1, 20, 0), custom$vertices)))
})

test_that("local deformation precedes the established attitude rotation", {
  rotation <- nautilus:::.overlayAttitudeRotation(30, 20)
  expect_equal(rotation %*% t(rotation), diag(3), tolerance = 1e-12)
  up <- c(0, 0, -1)
  expect_lt(sum(up * (nautilus:::.overlayAttitudeRotation(0, 30) %*% c(0, 1, 0))), 0)
  expect_gt(sum(up * (nautilus:::.overlayAttitudeRotation(30, 0) %*% c(1, 0, 0))),
            sum(up * c(1, 0, 0)))
  m <- nautilus:::.prepareOverlayModel("shark", 20)
  v <- nautilus:::.deformOverlayModel(m, pi/2, 1, 20, 0)
  projected <- nautilus:::.projectPreparedOverlayModel(m, v, rotation)
  world <- v %*% t(rotation)
  expect_equal(projected$points,
               nautilus:::.projectPreparedOverlayModel(m, world, diag(3))$points)
  expect_equal(projected$points[, 2], as.numeric(world %*% up) * m$display.scale)
})

test_that("the original rear camera has a zeroed pitch reference and no heading-like azimuth", {
  # Probe the nose, tail and right side independently of the animal mesh or animation.
  vertices <- rbind(c(1, 0, 0), c(-1, 0, 0), c(0, 1, 0))
  model <- list(faces = list(1:3), display.scale = 1)
  project <- function(pitch, roll = 0) {
    nautilus:::.projectPreparedOverlayModel(model, vertices,
      nautilus:::.overlayAttitudeRotation(pitch, roll))$points
  }
  level <- project(0)
  expect_equal(level[1, 2] - level[2, 2], 0)
  expect_equal(level[1, 1], level[2, 1]) # Rear alignment has no sideways longitudinal component.
  for (angle in c(6, 18, 24, 45, 75)) {
    down <- project(-angle); up <- project(angle)
    expect_lt(down[1, 2] - down[2, 2], 0)
    expect_gt(up[1, 2] - up[2, 2], 0)
    expect_equal(down[, 1], up[, 1])
    expect_equal(down[, 2], -up[, 2])
  }
  expect_gt(project(0, 30)[3, 1], 0)
  expect_lt(project(0, 30)[3, 2], level[3, 2])
  expect_gt(project(0, -30)[3, 2], level[3, 2])
  camera <- nautilus:::.overlayAttitudeCamera()
  elevation <- 24*pi/180
  expect_equal(camera$right, c(0, 1, 0))
  expect_equal(camera$up, c(sin(elevation), 0, -cos(elevation)))
  expect_equal(camera$depth, c(cos(elevation), 0, sin(elevation)))
  expect_equal(crossprod(camera$view.rotation), diag(3), tolerance = 1e-12)
  expect_equal(det(camera$view.rotation), 1, tolerance = 1e-12)
  basis <- cbind(camera$right, camera$up, camera$depth)
  expect_equal(crossprod(basis), diag(3), tolerance = 1e-12)
  # Test actual sensor-frame pitch independently of the display calibration.
  for (angle in c(-75, -24, -18, 0, 18, 24, 75)) {
    physical <- nautilus:::.overlayAttitudeRotation(angle, 0) %*% c(1, 0, 0)
    expect_equal(physical[3], -sin(angle*pi/180))
    expect_equal(project(angle)[1, 1], 0)
    expect_equal(project(angle)[1, 2], sin(angle*pi/180))
  }
})

test_that("animated TBF readout follows the current driving frequency and withholds unsupported values", {
  d <- .overlay_fixture()
  motion <- .overlay_motion(d)
  state <- nautilus:::.prepareOverlayFrameState(d, d$datetime[1] + .5, motion)
  fd <- nautilus:::.overlayFrameData(state, 1, d)
  # The readout must use the animation's interpolated state, not a raw sensor-column lookup.
  fd$.motion.frequency <- .437
  fd$tbf_hz_peaks <- 99
  model <- nautilus:::.prepareOverlayModel("shark", 20)
  captured <- list()
  testthat::local_mocked_bindings(text = function(x, y, labels, ...) {
    captured[[length(captured) + 1L]] <<- c(list(y = y, label = labels), list(...))
  }, .package = "graphics")
  grDevices::pdf(file = NULL, width = 6, height = 6)
  on.exit(grDevices::dev.off(), add = TRUE)
  draw <- function(control) {
    captured <<- list()
    nautilus:::.drawPreparedAttitudeModel(fd, nautilus:::.overlayTheme(), model, control)
    vapply(captured, function(x) x$label, character(1))
  }
  labels <- draw(motion)
  expect_true("TBF: 0.44 Hz" %in% labels)
  tbf <- which(startsWith(labels, "TBF:"))
  hpr <- which(startsWith(labels, "H "))
  expect_lt(captured[[tbf]]$y, captured[[hpr]]$y)
  expect_gte(captured[[tbf]]$y, graphics::par("usr")[3])
  expect_identical(captured[[tbf]]$cex, captured[[hpr]]$cex)
  expect_identical(captured[[tbf]]$font, captured[[hpr]]$font)
  expect_false(any(grepl("Schematic motion|peaks", labels)))
  expect_false(any(startsWith(draw(NULL), "TBF:")))
  for (status in c("unavailable", "gliding")) {
    fd$.motion.status <- status
    fd$.motion.frequency <- 0
    labels <- draw(motion)
    expect_true("TBF: NA" %in% labels)
    expect_false(any(grepl("Schematic motion|peaks", labels)))
  }
  fd$.motion.status <- "estimated"
  fd$.motion.frequency <- NA_real_
  expect_true("TBF: NA" %in% draw(motion))
})

test_that("phase is the time integral of frequency and is independent of the requested clip", {
  d <- .overlay_fixture()
  motion <- .overlay_motion(d, amplitude.reference = .4, phase.offset = .3)
  at <- d$datetime[1] + seq(0, 4, by = 1/32)
  state <- nautilus:::.prepareOverlayFrameState(d, at, motion)
  expect_equal(state$frames$.motion.phase, .3 + 2*pi*seq(0, 4, by = 1/32), tolerance = 1e-8)
  expect_equal(state$frames$.motion.strength, rep(.5, length(at)))
  short <- nautilus:::.prepareOverlayFrameState(d, at[65:97], motion)
  expect_equal(short$frames, state$frames[65:97])
  # With f(t)=1+t the exact integrated phase is 2*pi*(t+t^2/2), including between samples.
  t <- as.numeric(difftime(d$datetime, d$datetime[1], units = "secs"))
  d$tbf_hz_peaks <- 1 + t
  state <- nautilus:::.prepareOverlayFrameState(d, at, .overlay_motion(d, phase.lag = 0))
  x <- seq(0, 4, by = 1/32)
  expect_equal(state$frames$.motion.phase, 2*pi*(x + x^2/2), tolerance = 1e-8)
})

test_that("motion distinguishes missing data, zero amplitude and explicit gliding", {
  d <- .overlay_fixture(t = 0:6)
  d$tbf_amplitude_peaks[2] <- NA
  d$tbf_amplitude_peaks[4] <- 0
  d$tbf_swimming <- c(NA, NA, NA, NA, FALSE, NA, NA)
  at <- d$datetime[1] + seq(0, 6, by = .5)
  motion <- .overlay_motion(d, phase.offset = .25)
  state <- nautilus:::.prepareOverlayFrameState(d, at, motion)
  expect_equal(state$frames$.motion.status[1:5], c("estimated", "unavailable", "unavailable", "unavailable", "estimated"))
  expect_equal(state$frames$.motion.status[9], "gliding")
  expect_equal(state$frames$.motion.strength[7], 0)
  expect_equal(state$frames$.motion.status[7], "estimated")
  expect_equal(state$frames$.motion.phase[c(1, 5, 11)], rep(.25, 3))
  missing <- state$frames$.motion.status != "estimated"
  expect_true(all(state$frames$.motion.strength[missing] == 0))
  expect_true(all(is.finite(state$frames$.motion.phase)))
})

test_that("sensor gaps stop interpolation and phase, including at the gap edges", {
  d <- .overlay_fixture(t = c(0, 1, 5, 6))
  at <- d$datetime[1] + c(0, .5, 1, 2, 4, 5, 5.5, 6, 7)
  state <- nautilus:::.prepareOverlayFrameState(d, at, .overlay_motion(d), max.gap = 1)
  expect_true(all(is.na(state$frames$pitch[c(4, 5, 9)])))
  expect_equal(state$frames$.motion.status[c(4, 5, 9)], rep("unavailable", 3))
  expect_equal(state$frames$.motion.phase[c(1, 6)], c(0, 0))
  expect_equal(state$frames$.motion.phase[c(3, 8)], c(2*pi, 2*pi))
})

test_that("orientation interpolation follows short roll and heading arcs", {
  d <- .overlay_fixture(t = 0:1)
  d$roll <- c(179, -179); d$heading <- c(359, 1); d$pitch <- c(10, 20)
  state <- nautilus:::.prepareOverlayFrameState(d, d$datetime[1] + .5)
  expect_equal(abs(state$frames$roll), 180)
  expect_equal(state$frames$heading, 0)
  expect_equal(state$frames$pitch, 15)
  fd <- nautilus:::.overlayFrameData(state, 1, d)
  expect_equal(fd$datetime, d$datetime[1] + .5)
  expect_equal(fd$depth, d$depth[1])
  expect_equal(fd$pitch, 15)
  d$pitch[2] <- NA
  expect_true(is.na(nautilus:::.prepareOverlayFrameState(d, d$datetime[1] + .5)$frames$pitch))
  d$datetime[2] <- d$datetime[1]
  expect_error(nautilus:::.prepareOverlayFrameState(d, d$datetime), "strictly increasing")
  d$datetime[2] <- NA
  expect_error(nautilus:::.prepareOverlayFrameState(d, d$datetime[1]), "finite POSIXct")
})

test_that("backend selection is explicit, paired and stable across clip boundaries", {
  d <- .overlay_fixture()
  d$tbf_hz_wavelet <- rep(2, nrow(d)); d$tbf_amplitude_wavelet <- rep(.8, nrow(d))
  control <- .overlay_motion(d, method = "wavelet", amplitude.reference = 1)
  expect_equal(control$columns, c("tbf_hz_wavelet", "tbf_amplitude_wavelet"))
  state <- nautilus:::.prepareOverlayFrameState(d, d$datetime, control)
  expect_equal(state$frames$.motion.strength, rep(.8, nrow(d)))
  expect_equal(.overlay_motion(d)$method, "peaks")
  d$tbf_hz_peaks[] <- NA_real_
  expect_equal(.overlay_motion(d)$method, "wavelet")
  d$tbf_hz_wavelet[] <- NA_real_
  control <- .overlay_motion(d)
  expect_equal(control$method, "peaks")
  expect_true(all(nautilus:::.prepareOverlayFrameState(d, d$datetime, control)$frames$.motion.status == "unavailable"))
  d$tbf_amplitude_peaks <- NULL
  expect_error(.overlay_motion(d, method = "peaks"), "tbf_amplitude_peaks")
  expect_error(nautilus:::.resolveOverlayMotion(overlayMotionControl(), d, .overlay_tetrahedron()), "no motion profile")
  expect_error(overlayMotionControl(amplitude.reference = 0), "amplitude.reference")
  expect_error(overlayMotionControl(max.angle = 90), "max.angle")
  expect_error(overlayMotionControl(phase.lag = NA_real_), "phase.lag")
})

test_that("motion rate checks consider both dashboard and output video rate", {
  d <- .overlay_fixture(freq = 3)
  state <- nautilus:::.prepareOverlayFrameState(d, d$datetime, .overlay_motion(d))
  expect_warning(nautilus:::.checkOverlayMotionRate(state, 5, 30), "undersampled")
  expect_warning(nautilus:::.checkOverlayMotionRate(state, 30, 5), "undersampled")
  expect_warning(nautilus:::.checkOverlayMotionRate(state, 15, 30), "fewer than eight")
  expect_no_warning(nautilus:::.checkOverlayMotionRate(state, 30, 30))
})

test_that("prepared dashboards draw without media encoding for every existing layout", {
  d <- .overlay_fixture()
  model <- nautilus:::.prepareOverlayModel("shark", 20)
  motion <- .overlay_motion(d)
  state <- nautilus:::.prepareOverlayFrameState(d, d$datetime[1] + .5, motion)
  fd <- nautilus:::.overlayFrameData(state, 1, d)
  grDevices::pdf(file = NULL, width = 12, height = 12)
  on.exit(grDevices::dev.off(), add = TRUE)
  for (composition in c("beside", "overlay")) for (dashboard in c("general", "compact", "expanded", "focus", "ribbon")) {
    metrics <- if (dashboard == "ribbon") c("orientation", "depth") else c("orientation", "depth", "vedba")
    plan <- nautilus:::.resolveDashboardPlan(dashboard, metrics)
    context <- nautilus:::.prepareOverlayContext(d, plan)
    expect_no_error(nautilus:::.drawPreparedOverlayDashboard(fd, context$clip, fd$datetime, 300, 30,
      nautilus:::.overlayTheme(composition), NULL, NULL, plan, context, model, motion))
  }
  candidates <- data.frame(label = c("A", "B"), pitch = c("pitch", "pitch"), roll = c("roll", "roll"))
  for (dashboard in c("validation", "validation-compare")) {
    plan <- nautilus:::.resolveDashboardPlan(dashboard, candidates = candidates)
    context <- nautilus:::.prepareOverlayContext(d, plan)
    expect_no_error(nautilus:::.drawPreparedOverlayDashboard(fd, d, fd$datetime, 300, 30,
      nautilus:::.overlayTheme(), NULL, candidates, plan, context, model, NULL))
  }
  for (name in c("cetacean", "turtle", "fish", "manta"))
    expect_no_error(nautilus:::.drawPreparedAttitudeModel(fd, nautilus:::.overlayTheme(),
      nautilus:::.prepareOverlayModel(name, 20), motion))
  expect_no_error(nautilus:::.drawPreparedAttitudeModel(fd, nautilus:::.overlayTheme(),
    nautilus:::.prepareOverlayModel(.overlay_tetrahedron()), NULL))
})

# Exercise both media orchestration paths without producing a video or a PNG sequence.
test_that("production orchestration prepares once and controls motion in both composition modes", {
  d <- .overlay_fixture()
  source <- tempfile(fileext = ".mp4"); file.create(source)
  outputs <- c(tempfile(fileext = ".mp4"), tempfile(fileext = ".mp4"))
  on.exit(unlink(c(source, outputs)), add = TRUE)
  draws <- list(); rates <- numeric(); encodes <- list(); builds <- 0L
  prepare <- nautilus:::.prepareOverlayModel
  testthat::local_mocked_bindings(
    .ffmpegBin = function() "ffmpeg",
    .hasEncoder = function(name) FALSE,
    .prepareOverlayModel = function(...) { builds <<- builds + 1L; prepare(...) },
    .drawPreparedOverlayDashboard = function(fd, ...) { draws[[length(draws) + 1L]] <<- fd },
    .runFfmpegWithProgress = function(ffmpeg, args, duration, lvl) {
      encodes[[length(encodes) + 1L]] <<- args
      file.create(utils::tail(args, 1L)); 0L
    }, .package = "nautilus")
  testthat::local_mocked_bindings(
    av_video_info = function(...) list(video = list(framerate = 30, width = 1280, height = 720), duration = 4),
    av_capture_graphics = function(expr, framerate, ...) { rates <<- c(rates, framerate); force(expr) },
    .package = "av")
  testthat::local_mocked_bindings(png = function(...) grDevices::pdf(file = NULL), .package = "grDevices")
  for (i in seq_along(outputs)) {
    draws <- list()
    expect_no_warning(renderOverlayVideo(source, d, outputs[i], video.start = d$datetime[1],
      start = d$datetime[1] + 1, end = d$datetime[1] + 2,
      composition = c("beside", "overlay")[i], motion = overlayMotionControl(), verbose = FALSE))
    expect_length(draws, 30)
    expect_equal(draws[[1]]$.motion.phase, 2*pi)
    expect_equal(draws[[30]]$datetime, d$datetime[1] + 1 + 29/30)
  }
  expect_equal(rates, 30)
  expect_equal(builds, 2L)
  expect_length(encodes, 2)
  run.alias <- function(...) {
    output <- tempfile(fileext = ".mp4")
    on.exit(unlink(output), add = TRUE)
    draws <<- list()
    renderOverlayVideo(source, d, output, video.start = d$datetime[1],
      start = d$datetime[1] + 1, end = d$datetime[1] + 2, verbose = FALSE, ...)
    draws
  }
  animated <- run.alias(animate = TRUE)
  expect_length(animated, 30)
  expect_true(all(vapply(animated, function(fd) fd$.motion.status == "estimated", logical(1))))
  configured <- run.alias(animate = TRUE, motion = overlayMotionControl(phase.offset = .7))
  expect_equal(configured[[1]]$.motion.phase, 2*pi + .7)
  rigid <- run.alias(animate = FALSE, motion = overlayMotionControl())
  expect_length(rigid, 5)
  expect_true(all(vapply(rigid, function(fd) fd$.motion.status == "disabled", logical(1))))
  expect_error(run.alias(animate = "yes"), "animate")
  expect_error(renderOverlayVideo(source, d, outputs[1]), "already exists")
  expect_error(renderOverlayVideo(source, d, source, overwrite = TRUE), "differ from the source")
  expect_error(renderOverlayVideo(source, d, tempfile(fileext = ".mp4"),
    dashboard = "validation", motion = overlayMotionControl()), "axis-validation")
  expect_error(renderOverlayVideo(source, d, tempfile(fileext = ".mp4"),
    orientation = "dials", motion = overlayMotionControl()), "full orientation")
})
