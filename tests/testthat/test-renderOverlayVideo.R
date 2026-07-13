# Tests for renderOverlayVideo(): input validation, the filename sync parser, the frame drawer, and a
# full end-to-end render (skipped unless av + ffmpeg are available) using a tiny synthetic source video.

.mk_sensor <- function(id = "A01", t0 = as.POSIXct("2020-01-01 00:00:00", tz = "UTC"), n = 60) {
  d <- data.table::data.table(
    ID = id, datetime = t0 + 0:(n - 1),
    depth = 5 + 4 * sin(seq_len(n) / n * pi), heading = (seq_len(n) * 6) %% 360,
    pitch = rnorm(n, 0, 5), roll = rnorm(n, 0, 5),
    vedba = runif(n, 0, 0.4), vertical_velocity = rnorm(n, 0, 0.3),
    ax = rnorm(n, 0, 0.1), ay = rnorm(n, 0, 0.3), az = rnorm(n, 1, 0.1),
    gx = rnorm(n, 0, 0.2), gy = rnorm(n, 0, 0.2), gz = rnorm(n, 0, 0.2))
  d
}
.theme <- list(bg = "grey12", text = "grey90", value = "#ff453a")

# the container fourcc tag of a file's video stream (NA if ffprobe is unavailable)
.vtag <- function(f) {
  ff <- Sys.which("ffprobe"); if (!nzchar(ff)) return(NA_character_)
  trimws(suppressWarnings(system2(ff, c("-v", "error", "-select_streams", "v", "-show_entries",
    "stream=codec_tag_string", "-of", "default=nk=1:nw=1", shQuote(f)), stdout = TRUE))[1])
}

test_that(".videoStartFromFilename parses a YYMMDD-HHMMSS prefix (UTC), else NULL", {
  ts <- nautilus:::.videoStartFromFilename("230831-161949_CAM0bc99448_30.mp4")
  expect_s3_class(ts, "POSIXct")
  expect_equal(ts, as.POSIXct("2023-08-31 16:19:49", tz = "UTC"))
  expect_null(nautilus:::.videoStartFromFilename("clip_without_timestamp.mp4"))
})

test_that(".fmt_duration handles sub-minute durations in seconds", {
  expect_equal(nautilus:::.fmt_duration(20), "20 s")
  expect_equal(nautilus:::.fmt_duration(125), "2 m")
})

test_that("input validation aborts clearly (before touching av/ffmpeg)", {
  vid <- tempfile(fileext = ".mp4"); file.create(vid); on.exit(unlink(vid), add = TRUE)
  out <- file.path(tempdir(), "out.mp4")
  # missing required sensor column
  expect_error(renderOverlayVideo(vid, .mk_sensor()[, !"roll"], out, verbose = FALSE),
               "missing column", ignore.case = TRUE)
  # missing video file
  expect_error(renderOverlayVideo(file.path(tempdir(), "nope.mp4"), .mk_sensor(), out, verbose = FALSE),
               "not found", ignore.case = TRUE)
  # non-existent output directory
  expect_error(renderOverlayVideo(vid, .mk_sensor(), "/no/such/dir/out.mp4", verbose = FALSE),
               "directory does not exist", ignore.case = TRUE)
  # codec is a family enum, not a raw ffmpeg encoder name
  expect_error(renderOverlayVideo(vid, .mk_sensor(), file.path(tempdir(), "o.mp4"), codec = "libx264", verbose = FALSE),
               "should be one of", ignore.case = TRUE)
})

test_that(".dashboardColumns lists the right required columns per dashboard", {
  expect_true(all(c("vedba", "heading") %in% nautilus:::.dashboardColumns("general")))
  # validation needs only depth + attitude (heading + accel traces were dropped in the redesign;
  # gyro gx/gy/gz are optional, used only when present)
  expect_setequal(nautilus:::.dashboardColumns("validation"), c("datetime", "depth", "pitch", "roll"))
  cand <- data.frame(label = "c1", pitch = "pitch.c1", roll = "roll.c1", stringsAsFactors = FALSE)
  expect_true(all(c("pitch.c1", "roll.c1") %in% nautilus:::.dashboardColumns("validation-compare", cand)))
})

test_that("the compare dashboard requires a candidates spec", {
  expect_error(renderOverlayVideo(tempfile(fileext = ".mp4"), .mk_sensor(), tempfile(fileext = ".mp4"),
                                  dashboard = "validation-compare", verbose = FALSE),
               "candidates", ignore.case = TRUE)
})

test_that("all three dashboards render a frame without error (incl. NA-tolerant)", {
  clip <- .mk_sensor()
  clip$heading[1] <- NA_real_; clip$roll[2] <- NA_real_         # NAs must not break any panel
  pdf(NULL); on.exit(grDevices::dev.off())
  expect_no_error(nautilus:::.drawDashboard("general", clip[30], clip, clip$datetime[30], 300, 30, .theme, NULL, NULL))
  expect_no_error(nautilus:::.drawDashboard("validation", clip[30], clip, clip$datetime[30], 300, 30, .theme, "cap", NULL))
  # gyro panel must degrade gracefully when the channels are absent
  expect_no_error(nautilus:::.drawDashboard("validation", clip[, !c("gx", "gy", "gz")][30],
                                            clip[, !c("gx", "gy", "gz")], clip$datetime[30], 300, 30, .theme, "cap", NULL))
  clip$pitch.c1 <- clip$pitch; clip$roll.c1 <- clip$roll; clip$pitch.c2 <- -clip$pitch; clip$roll.c2 <- -clip$roll
  cand <- data.frame(label = c("c1", "c2"), pitch = c("pitch.c1", "pitch.c2"), roll = c("roll.c1", "roll.c2"), stringsAsFactors = FALSE)
  expect_no_error(nautilus:::.drawDashboard("validation-compare", clip[30], clip, clip$datetime[30], 300, 30, .theme, "cap", cand))
})

test_that(".drawAttitudeModel3D is NA-tolerant and draws", {
  pdf(NULL); on.exit(grDevices::dev.off())
  expect_no_error(nautilus:::.drawAttitudeModel3D(NA_real_, NA_real_, .theme, label = "x"))
  expect_no_error(nautilus:::.drawAttitudeModel3D(15, -30, .theme))
})

test_that(".tagModel3D is a valid low-poly mesh", {
  m <- nautilus:::.tagModel3D()
  expect_true(length(m$faces) > 20L)                       # a body tube + fins
  expect_equal(length(m$faces), length(m$part))
  expect_true(all(vapply(m$faces, function(f) is.matrix(f) && nrow(f) == 3L && ncol(f) >= 3L, logical(1))))
  expect_setequal(unique(m$part), c("body", "fin"))
})

test_that("the 3-D attitude projection has the correct handedness (roll right-down, pitch nose-up)", {
  # reproduce the drawer's rotation + camera projection (kept in lockstep with .drawAttitudeModel3D)
  proj <- function(pitch, roll, v) {
    th <- pitch * pi / 180; ro <- roll * pi / 180
    Rx <- matrix(c(1, 0, 0, 0, cos(ro), -sin(ro), 0, sin(ro), cos(ro)), 3, byrow = TRUE)
    Ry <- matrix(c(cos(th), 0, sin(th), 0, 1, 0, -sin(th), 0, cos(th)), 3, byrow = TRUE)
    w  <- (Ry %*% Rx) %*% v; a <- 24 * pi / 180
    c(xs = sum(c(0, 1, 0) * w), ys = sum(c(sin(a), 0, -cos(a)) * w))     # screen x (right), y (up)
  }
  right <- c(0, 1, 0)                                       # a point on the animal's right (+y)
  nose  <- c(1, 0, 0)                                       # the nose (+x forward)
  # a positive roll (right side down): the right-side point stays on the right and moves DOWN
  expect_gt(proj(0, 30, right)["xs"], 0)                    # animal's right -> viewer's right
  expect_lt(proj(0, 30, right)["ys"], proj(0, 0, right)["ys"])   # right side drops
  # the LEFT side rises (mirror) - so a mirrored mapping would visibly bank the wrong way
  expect_gt(proj(0, 30, c(0, -1, 0))["ys"], proj(0, 0, c(0, -1, 0))["ys"])
  # a positive pitch (nose up): the nose rises on screen
  expect_gt(proj(30, 0, nose)["ys"], proj(0, 0, nose)["ys"])
})

test_that("end-to-end: composites a dashboard panel beside a synthetic source video", {
  skip_if_not_installed("av")
  skip_if(!nzchar(Sys.which("ffmpeg")), "ffmpeg not available")
  t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
  src <- tempfile(fileext = ".mp4"); on.exit(unlink(src), add = TRUE)
  # a small 2 s solid-colour source video (480x720 @ 30 fps - tall enough for the dashboard)
  suppressWarnings(av::av_capture_graphics(
    { for (i in 1:60) { par(mar = c(0, 0, 0, 0)); plot.new(); rect(0, 0, 1, 1, col = "steelblue", border = NA) } },
    output = src, width = 480, height = 720, framerate = 30, verbose = FALSE))
  out <- tempfile(fileext = ".mp4"); on.exit(unlink(out), add = TRUE)
  expect_no_error(suppressWarnings(suppressMessages(
    renderOverlayVideo(src, .mk_sensor(t0 = t0, n = 10), out,
                       video.start = t0, overlay.fps = 5, codec = "h264", crf = 30, verbose = FALSE))))
  expect_true(file.exists(out))
  info <- av::av_video_info(out)
  expect_equal(info$video$height, 720)
  expect_gt(info$video$width, 480)                              # source + dashboard panel
  tag <- .vtag(out)                                             # h264 muxes as avc1 (QuickTime-universal)
  if (!is.na(tag)) expect_equal(tag, "avc1")
})

test_that("codec = 'hevc' tags the stream hvc1 for QuickTime compatibility", {
  skip_if_not_installed("av")
  skip_if(!nzchar(Sys.which("ffmpeg")), "ffmpeg not available")
  skip_if(!nautilus:::.hasEncoder("hevc_videotoolbox") && !nautilus:::.hasEncoder("libx265"), "no HEVC encoder")
  t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
  src <- tempfile(fileext = ".mp4"); on.exit(unlink(src), add = TRUE)
  suppressWarnings(av::av_capture_graphics(
    { for (i in 1:30) { par(mar = c(0, 0, 0, 0)); plot.new(); rect(0, 0, 1, 1, col = "tomato", border = NA) } },
    output = src, width = 480, height = 720, framerate = 30, verbose = FALSE))
  out <- tempfile(fileext = ".mp4"); on.exit(unlink(out), add = TRUE)
  suppressWarnings(suppressMessages(
    renderOverlayVideo(src, .mk_sensor(t0 = t0, n = 10), out, video.start = t0, overlay.fps = 5,
                       codec = "hevc", verbose = FALSE)))
  expect_true(file.exists(out))
  expect_equal(av::av_video_info(out)$video$codec, "hevc")
  tag <- .vtag(out)
  if (!is.na(tag)) expect_equal(tag, "hvc1")                    # NOT the default hev1 (which QuickTime refuses)
})
