#######################################################################################################
# Render a sensor-overlay video #######################################################################
#######################################################################################################

#' Render a synchronised sensor dashboard alongside camera-tag video
#'
#' @description
#' Composites a live sensor dashboard as a side panel next to camera-tag footage, time-synchronised to
#' the sensor data. It is the workhorse for **visual validation** - e.g. confirming an inferred axis
#' mapping against an observed turn or roll - and for presentation. Two dashboards are available:
#' \describe{
#'   \item{`"general"`}{Orientation dials (heading/pitch/roll) plus scrolling depth, activity (VeDBA)
#'     and vertical-speed traces - the presentation default.}
#'   \item{`"validation"`}{A large **attitude indicator** - a low-poly 3-D body model, viewed from
#'     behind and above, that banks and pitches with the animal - over a scrolling gyroscope trace and
#'     depth, tuned for judging an axis mapping by eye. Heading is intentionally omitted: how the body
#'     banks is the cue, and an uncalibrated heading is unreliable.}
#'   \item{`"validation-compare"`}{Two or more **labelled** attitude indicators side by side, one per
#'     candidate mapping (e.g. "Documented" vs "Proposed"), beneath a one-line guidance header - so the
#'     correct mapping is read off a single clip by keeping the model that banks like the animal.
#'     Driven by \link{reviewTagMapping}.}
#' }
#'
#' The dashboard is rendered straight to a video stream with the \pkg{av} package (no thousands of
#' intermediate image files), and at a **decoupled frame rate** (`overlay.fps`, default 5): the sensor
#' state changes slowly, so there is no need to redraw it at the video's frame rate. A single FFmpeg
#' pass then stacks the panel beside the (trimmed) source video. Together this is one to two orders of
#' magnitude faster than rendering one image per video frame.
#'
#' @param video Path to the source video file.
#' @param data A `nautilus_tag` (or a single data.frame) holding the time-synchronised sensor series.
#'   The required columns depend on `dashboard` (see Details).
#' @param output Path to the output video file to create (its directory must exist).
#' @param dashboard Which dashboard to render: `"general"` (default), `"validation"` or
#'   `"validation-compare"`.
#' @param video.start POSIXct giving the sensor datetime of the video's first frame (the sync anchor).
#'   If `NULL`, it is parsed from a `YYMMDD-HHMMSS` prefix in the file name when present (otherwise an
#'   error is raised). For frame-accurate sync, derive it from \link{getVideoMetadata}.
#' @param start,end Optional POSIXct bounds (in sensor time) to clip the output to a segment of interest
#'   (e.g. a validation window from \link{findValidationSegments}). Default: the full overlap of video
#'   and sensor coverage.
#' @param side Which side to place the dashboard panel, `"right"` (default) or `"left"`.
#' @param overlay.fps Dashboard render frame rate (Hz). Default 5. Lower is faster; the source video
#'   keeps its own frame rate.
#' @param panel.width Panel width in pixels. Default `NULL` scales it to the video height.
#' @param depth.window,activity.window Width (seconds) of the scrolling depth panel and of the
#'   activity/vertical-speed/trace panels. Defaults 300 and 30.
#' @param caption Optional one-line caption drawn on the validation dashboards (e.g. the mapping under
#'   test and the review reason).
#' @param candidates For `"validation-compare"` only: a data.frame describing the candidate attitude
#'   indicators, with columns `label`, `pitch` and `roll` (the last two naming the per-candidate pitch
#'   and roll columns in `data`).
#' @param crf Constant rate factor for the software encoders (`libx265`/`libx264`; lower = higher
#'   quality / larger). Default 23. Ignored by the hardware VideoToolbox encoders (which use a fixed
#'   quality setting).
#' @param codec Output codec family: `"hevc"` (default) or `"h264"`. `"hevc"` (H.265) gives much smaller
#'   files and is tagged `hvc1` for QuickTime compatibility; `"h264"` (tagged `avc1`) is larger but plays
#'   essentially everywhere (all browsers, older devices). Each prefers the macOS hardware VideoToolbox
#'   encoder when present, else the software `libx26x` encoder. Output is always `yuv420p` with a
#'   fast-start (leading `moov`) MP4 container.
#' @param keep.temp Keep the intermediate dashboard video. Default `FALSE`.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal", or `2`/"detailed" (default).
#'
#' @details
#' Required `data` columns by dashboard (all alongside `datetime`):
#' \itemize{
#'   \item `"general"`: `depth`, `heading`, `pitch`, `roll`, `vedba`, `vertical_velocity`.
#'   \item `"validation"`: `depth`, `pitch`, `roll`; the gyroscope panel uses `gx`/`gy`/`gz` when
#'     present (omitted if absent).
#'   \item `"validation-compare"`: `depth` plus the per-candidate pitch/roll columns named in
#'     `candidates`.
#' }
#'
#' @return The output file path, invisibly.
#' @seealso \link{reviewTagMapping}, \link{processTagData}, \link{findValidationSegments}, \link{getVideoMetadata}.
#' @examples
#' \dontrun{
#' tag  <- processTagData(imported)[["PIN_CAM_01"]]
#' meta <- getVideoMetadata("./videos/PIN_CAM_01")
#' # composite the sensor dashboard beside the footage, synced to the first frame
#' renderOverlayVideo(meta$file[1], tag, "./overlay/PIN_CAM_01.mp4",
#'                    dashboard = "general", video.start = meta$start[1])
#' }
#' @export

renderOverlayVideo <- function(video,
                               data,
                               output,
                               dashboard = c("general", "validation", "validation-compare"),
                               video.start = NULL,
                               start = NULL,
                               end = NULL,
                               side = c("right", "left"),
                               overlay.fps = 5,
                               panel.width = NULL,
                               depth.window = 300,
                               activity.window = 30,
                               caption = NULL,
                               candidates = NULL,
                               crf = 23,
                               codec = c("hevc", "h264"),
                               keep.temp = FALSE,
                               verbose = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  dashboard <- match.arg(dashboard)
  side <- match.arg(side)
  codec <- match.arg(codec)
  .assert_number(overlay.fps, "overlay.fps", min = 0.1)
  .assert_number(crf, "crf", min = 0)
  if (dashboard == "validation-compare" &&
      (is.null(candidates) || !all(c("label", "pitch", "roll") %in% names(candidates)) || !nrow(candidates)))
    .abort("{.arg candidates} (a data.frame with {.field label}/{.field pitch}/{.field roll}) is required for the compare dashboard.")
  if (!file.exists(video)) .abort("Video file not found: {.file {video}}.")
  if (!dir.exists(dirname(normalizePath(output, mustWork = FALSE)))) .abort("Output directory does not exist: {.file {dirname(output)}}.")

  # resolve the sensor data to a single data.table with the columns this dashboard needs
  if (is.list(data) && !inherits(data, "data.frame")) data <- data[[1]]
  data <- data.table::as.data.table(data)
  need <- .dashboardColumns(dashboard, candidates)
  miss <- setdiff(need, names(data))
  if (length(miss)) .abort(c("{.arg data} is missing {cli::qty(miss)}column{?s} required by the {.val {dashboard}} dashboard: {.val {miss}}.",
                             "i" = "Pass the output of {.fn processTagData} (general) or use {.fn reviewTagMapping} (validation)."))
  data.table::setorder(data, datetime)

  # heavy dependencies (checked only once the inputs are known good)
  if (!requireNamespace("av", quietly = TRUE)) .abort("The {.pkg av} package is required: {.code install.packages('av')}.")
  ffmpeg <- .ffmpegBin()

  # probe the source video (dimensions, frame rate, duration)
  info <- av::av_video_info(video)
  v_fps <- info$video$framerate; v_w <- info$video$width; v_h <- info$video$height; v_dur <- info$duration
  if (!is.finite(v_h) || v_h < 360)
    .abort(c("The source video is too short (height {v_h}px) for the dashboard panel.",
             "i" = "{.fn renderOverlayVideo} needs a video at least 360px tall."))

  # sync anchor: sensor datetime of the video's first frame
  if (is.null(video.start)) {
    video.start <- .videoStartFromFilename(video)
    if (is.null(video.start)) .abort(c("Could not determine {.arg video.start} from the file name.",
                                       "i" = "Pass {.arg video.start} (POSIXct of the first video frame) explicitly."))
  }
  if (!inherits(video.start, "POSIXct")) .abort("{.arg video.start} must be POSIXct.")

  # the renderable window = overlap of (video coverage) and (sensor coverage), clipped to start/end
  v_lo <- video.start; v_hi <- video.start + v_dur
  s_lo <- min(data$datetime); s_hi <- max(data$datetime)
  clip_lo <- max(v_lo, s_lo, if (!is.null(start)) start else v_lo)
  clip_hi <- min(v_hi, s_hi, if (!is.null(end))   end   else v_hi)
  clip_dur <- as.numeric(difftime(clip_hi, clip_lo, units = "secs"))
  if (!is.finite(clip_dur) || clip_dur <= 0)
    .abort("The requested window does not overlap the video/sensor coverage.")
  seek_secs <- as.numeric(difftime(clip_lo, video.start, units = "secs"))   # offset into the source video

  .log_header(lvl, "renderOverlayVideo", "Compositing the sensor dashboard onto the video",
              bullets = c(sprintf("Source: %s (%dx%d, %g fps)", basename(video), v_w, v_h, v_fps),
                          sprintf("Clip: %s for %s", format(clip_lo, "%H:%M:%S"), .fmt_duration(clip_dur))),
              arrow = sprintf("Dashboard: %s \u00b7 %g fps panel on the %s \u00b7 av render + 1 ffmpeg pass",
                              dashboard, overlay.fps, side))

  # subset sensor data to the clip plus window padding (so per-frame lookups stay cheap)
  pad <- max(depth.window, activity.window)
  clip <- data[datetime >= clip_lo - pad & datetime <= clip_hi + pad]

  # one dashboard frame per overlay step; map each to its nearest sensor row (precomputed, vectorised)
  n_frames <- max(1L, as.integer(ceiling(clip_dur * overlay.fps)))
  frame_times <- clip_lo + (seq_len(n_frames) - 1) / overlay.fps
  idx <- findInterval(as.numeric(frame_times), as.numeric(clip$datetime))
  idx <- pmin(pmax(idx, 1L), nrow(clip))

  panel_w <- if (is.null(panel.width)) as.integer(round(v_h * if (dashboard == "validation-compare") 0.85 else 0.55)) else as.integer(panel.width)
  if (panel_w %% 2L != 0L) panel_w <- panel_w + 1L                          # H.26x requires even dimensions
  theme <- list(bg = "grey12", text = "grey92", value = "#ff453a")
  # font size scaled to the panel so the dashboard stays legible when the whole composite is viewed at a
  # normal (down-scaled) resolution - the main readability lever
  ps <- max(16L, as.integer(round(panel_w / 30)))

  # ---- render the dashboard straight to a video via av (no intermediate PNGs) ----
  dash_video <- tempfile(fileext = ".mp4")
  if (!keep.temp) on.exit(unlink(dash_video), add = TRUE)
  if (lvl >= 1L) .log_detail(lvl, sprintf("rendering %s dashboard frame%s at %g fps", .formatLargeNumber(n_frames),
                                          if (n_frames != 1) "s" else "", overlay.fps))
  av::av_capture_graphics(
    expr = {
      for (i in seq_len(n_frames))
        .drawDashboard(dashboard, clip[idx[i]], clip, frame_times[i], depth.window, activity.window,
                       theme, caption, candidates)
    },
    output = dash_video, width = panel_w, height = v_h, framerate = overlay.fps, verbose = FALSE, pointsize = ps)

  # ---- single FFmpeg pass: trim the source, stack the panel beside it ----
  # Resolve the codec FAMILY to a concrete encoder (prefer the macOS hardware VideoToolbox encoder),
  # its rate-control flag, and the container tag. HEVC must be tagged `hvc1` (ffmpeg muxes it as `hev1`
  # by default, which QuickTime refuses to open); H.264's default `avc1` is already universal.
  enc <- switch(codec,
                hevc = if (.hasEncoder("hevc_videotoolbox")) "hevc_videotoolbox" else "libx265",
                h264 = if (.hasEncoder("h264_videotoolbox")) "h264_videotoolbox" else "libx264")
  tag_arg <- if (codec == "hevc") c("-tag:v", "hvc1")
  q_arg   <- if (grepl("videotoolbox", enc)) c("-q:v", "60") else c("-crf", as.character(crf))
  stack <- if (side == "right") "[0:v][ov]hstack=inputs=2[vout]" else "[ov][0:v]hstack=inputs=2[vout]"
  filt  <- sprintf("[1:v]fps=%g,scale=-2:%d,setsar=1[ov];%s", v_fps, v_h, stack)
  args <- c("-y", "-ss", sprintf("%.3f", seek_secs), "-t", sprintf("%.3f", clip_dur), "-i", video,
            "-i", dash_video, "-filter_complex", filt, "-map", "[vout]", "-map", "0:a?",
            "-c:v", enc, q_arg, tag_arg, "-pix_fmt", "yuv420p", "-movflags", "+faststart",
            normalizePath(output, mustWork = FALSE))
  if (lvl >= 1L) .log_detail(lvl, "compositing with ffmpeg")
  status <- suppressWarnings(system2(ffmpeg, shQuote(args), stdout = FALSE, stderr = FALSE))
  if (status != 0 || !file.exists(output)) .abort("FFmpeg failed to create the output video (exit status {status}).")

  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, "overlay video written")
    .log_arrow(lvl, "output: ", output)
    .log_runtime(lvl, start.time)
  }
  invisible(output)
}


################################################################################
# Dashboards (internal) ########################################################
################################################################################

#' Columns a given dashboard requires (alongside `datetime`).
#' @keywords internal
#' @noRd
.dashboardColumns <- function(dashboard, candidates = NULL) {
  switch(dashboard,
         "general"            = c("datetime", "depth", "heading", "pitch", "roll", "vedba", "vertical_velocity"),
         "validation"         = c("datetime", "depth", "pitch", "roll"),
         "validation-compare" = c("datetime", "depth", unique(c(candidates$pitch, candidates$roll))))
}

#' Dispatch one dashboard frame to its drawer.
#' @keywords internal
#' @noRd
.drawDashboard <- function(dashboard, fd, clip, current_time, depth.window, activity.window, theme, caption, candidates) {
  fd <- as.list(fd)
  switch(dashboard,
         "general"            = .drawDashboardGeneral(fd, clip, current_time, depth.window, activity.window, theme),
         "validation"         = .drawDashboardValidation(fd, clip, current_time, depth.window, activity.window, theme, caption),
         "validation-compare" = .drawDashboardCompare(fd, clip, current_time, depth.window, theme, candidates, caption))
}

#' General presentation dashboard: 3 dials + depth/VeDBA/vertical-speed traces + timestamp.
#' @keywords internal
#' @noRd
.drawDashboardGeneral <- function(fd, clip, current_time, depth.window, activity.window, theme) {
  graphics::layout(matrix(c(1,2,3, 4,4,4, 5,5,5, 6,6,6, 7,7,7), nrow = 5, byrow = TRUE),
                   heights = c(2.2, 1.5, 1.2, 1.2, 0.35))
  op <- graphics::par(bg = theme$bg, mar = c(1, 1, 1.6, 1), oma = c(0, 0, 0.4, 0))
  on.exit(graphics::par(op), add = TRUE)
  .drawDial("heading", fd$heading, theme)
  .drawDial("pitch",   fd$pitch,   theme)
  .drawDial("roll",    fd$roll,    theme)
  graphics::par(mar = c(1.6, 3, 2.2, 1))
  .drawSeriesPanel(clip, current_time, depth.window,    "depth",             "Depth",          "m",   fd$depth,             theme, invert = TRUE,  fill = "#1f4e8c")
  .drawSeriesPanel(clip, current_time, activity.window, "vedba",             "VeDBA",          "g",   fd$vedba,             theme, invert = FALSE, fill = "#9a9a9a")
  .drawSeriesPanel(clip, current_time, activity.window, "vertical_velocity", "Vertical Speed", "m/s", fd$vertical_velocity, theme, invert = FALSE, fill = "#c97b7b")
  graphics::par(mar = c(0, 0, 0, 0))
  .drawTimestamp(fd$datetime, theme)
}

#' Axis-validation dashboard: a large attitude indicator (the handedness cue) + the roll-rate gyro trace
#' + depth + guidance caption. The heading compass was dropped: the lite-orientation heading (uncalibrated
#' magnetometer, and a wrong mapping is exactly what is under review) is unreliable AND heading is not the
#' handedness cue - the ROLL is.
#' @keywords internal
#' @noRd
.drawDashboardValidation <- function(fd, clip, current_time, depth.window, activity.window, theme, caption) {
  graphics::layout(matrix(c(1, 2, 3, 4), ncol = 1), heights = c(2.8, 1.3, 1.2, 0.5))
  op <- graphics::par(bg = theme$bg, mar = c(1, 1, 2.2, 1), oma = c(0, 0, 0.3, 0))
  on.exit(graphics::par(op), add = TRUE)
  .drawAttitudeModel3D(fd$pitch, fd$roll, theme)
  graphics::par(mar = c(1.6, 3.2, 2.0, 1))
  .drawTriTrace(clip, current_time, activity.window, c("gx", "gy", "gz"),
                c("roll-rate", "pitch-rate", "yaw-rate"), "Gyroscope (body)", theme, fd)
  .drawSeriesPanel(clip, current_time, depth.window, "depth", "Depth", "m", fd$depth, theme, invert = TRUE, fill = "#1f4e8c")
  graphics::par(mar = c(0, 0, 0, 0))
  .drawCaption(fd$datetime, caption, theme)
}

#' Multi-candidate comparison dashboard: N attitude indicators side by side + depth + legend.
#' @keywords internal
#' @noRd
.drawDashboardCompare <- function(fd, clip, current_time, depth.window, theme, candidates, caption) {
  N <- nrow(candidates)
  pal <- .candidatePalette(N)
  # a prominent guidance HEADER spanning the top, then the N attitude indicators, depth, and timestamp
  m <- rbind(rep(1L, N), 1L + seq_len(N), rep(N + 2L, N), rep(N + 3L, N))
  graphics::layout(m, heights = c(0.55, 2.7, 1.0, 0.32))
  op <- graphics::par(bg = theme$bg, oma = c(0, 0, 0.3, 0)); on.exit(graphics::par(op), add = TRUE)
  graphics::par(mar = c(0, 1, 0, 1)); .drawHeader(caption, theme)
  graphics::par(mar = c(1, 1, 2.4, 1))
  for (k in seq_len(N))
    .drawAttitudeModel3D(fd[[candidates$pitch[k]]], fd[[candidates$roll[k]]], theme,
                         label = candidates$label[k], body.col = pal[k])
  graphics::par(mar = c(1.6, 3.2, 2.0, 1))
  .drawSeriesPanel(clip, current_time, depth.window, "depth", "Depth", "m", fd$depth, theme, invert = TRUE, fill = "#1f4e8c")
  graphics::par(mar = c(0, 0, 0, 0)); .drawTimestamp(fd$datetime, theme)
}

#' A prominent, auto-sized guidance header (the "what to look for" instruction) spanning the panel top.
#' @keywords internal
#' @noRd
.drawHeader <- function(caption, theme) {
  plot(0, 0, type = "n", ann = FALSE, axes = FALSE, xlim = c(0, 1), ylim = c(0, 1))
  if (is.null(caption) || !nzchar(caption)) return(invisible())
  w <- graphics::strwidth(caption, units = "inches", cex = 1)
  cex <- if (w > 0) min(1.3, 0.97 * graphics::par("pin")[1] / w) else 1.1
  graphics::text(0.5, 0.5, caption, col = theme$text, font = 2, cex = cex, xpd = NA)
}


################################################################################
# Panel drawers (internal) #####################################################
################################################################################

#' Low-poly 3-D body model used by the attitude indicator, in the animal body frame (x forward / nose,
#' y right, z down). A fusiform body (rings of elliptical cross-sections) plus a dorsal, two pectoral
#' and a forked caudal fin. Returned as a list of faces (each a 3 x k matrix of vertex coordinates) and
#' a matching part label ("body" / "fin") for styling.
#' @keywords internal
#' @noRd
.tagModel3D <- function() {
  stn <- c(-1.00, -0.55, -0.10,  0.35,  0.70,  0.95,  1.05)   # x stations, tail -> nose
  ry  <- c( 0.015, 0.16,  0.24,  0.26,  0.20,  0.10,  0.012)  # cross-section half-width  (y)
  rz  <- c( 0.015, 0.13,  0.19,  0.21,  0.16,  0.09,  0.012)  # cross-section half-height (z)
  nt  <- 8L
  ang <- utils::head(seq(0, 2 * pi, length.out = nt + 1L), nt)
  rings <- lapply(seq_along(stn), function(i) rbind(rep(stn[i], nt), ry[i] * cos(ang), rz[i] * sin(ang)))
  faces <- list(); part <- character(0)
  add <- function(m, p) { faces[[length(faces) + 1L]] <<- m; part[length(part) + 1L] <<- p }
  for (i in seq_len(length(stn) - 1L)) {                      # tube: quad faces between adjacent rings
    A <- rings[[i]]; B <- rings[[i + 1L]]
    for (j in seq_len(nt)) { k <- if (j == nt) 1L else j + 1L; add(cbind(A[, j], A[, k], B[, k], B[, j]), "body") }
  }
  add(cbind(c(0.42, 0, -0.20), c(0.02, 0, -0.20), c(0.22, 0, -0.62)), "fin")     # dorsal fin (up, -z)
  add(cbind(c(0.42,  0.22, 0.05), c(0.16,  0.22, 0.09), c(0.34,  0.56, 0.20)), "fin")  # right pectoral (+y)
  add(cbind(c(0.42, -0.22, 0.05), c(0.16, -0.22, 0.09), c(0.34, -0.56, 0.20)), "fin")  # left  pectoral (-y)
  add(cbind(c(-0.90, 0, -0.03), c(-1.34, 0, -0.34), c(-1.10, 0, 0.01)), "fin")   # caudal upper lobe
  add(cbind(c(-0.90, 0,  0.03), c(-1.30, 0,  0.26), c(-1.10, 0, 0.01)), "fin")   # caudal lower lobe
  list(faces = faces, part = part)
}

#' Outward-ish unit normal of a face (first three vertices), for flat shading.
#' @keywords internal
#' @noRd
.faceNormal <- function(w) {
  e1 <- w[, 2] - w[, 1]; e2 <- w[, 3] - w[, 1]
  n  <- c(e1[2] * e2[3] - e1[3] * e2[2], e1[3] * e2[1] - e1[1] * e2[3], e1[1] * e2[2] - e1[2] * e2[1])
  nn <- sqrt(sum(n^2)); if (is.finite(nn) && nn > 0) n / nn else c(0, 0, 1)
}

#' Multiply a base colour by a shading intensity (clamped to the 0-1 range).
#' @keywords internal
#' @noRd
.shadeColor <- function(col, s) {
  s <- max(0, min(1, s)); rgb <- grDevices::col2rgb(col)[, 1] / 255
  grDevices::rgb(rgb[1] * s, rgb[2] * s, rgb[3] * s)
}

#' Attitude indicator: a low-poly 3-D body model, viewed from behind and above (a chase-cam), banking
#' and pitching with the animal. Roll rolls the body (right side down for a positive roll); pitch lifts
#' or drops the nose. The way the body banks is the at-a-glance handedness cue - confirm its direction
#' against the video, since a mirrored mapping banks it the wrong way. Heading is not shown (it is not
#' needed for the handedness call and the lightweight review orientation cannot estimate it reliably).
#'
#' Conventions (validated): body frame x forward, y right, z down; body -> world rotation is
#' \eqn{R = R_y(\text{pitch}) R_x(\text{roll})} (yaw omitted); the camera looks forward and ~24 deg down
#' with screen-right = world +y, so the animal's right maps to the viewer's right and the handedness
#' matches the underlying tilt convention (roll > 0 = right side down, pitch > 0 = nose up).
#' @keywords internal
#' @noRd
.drawAttitudeModel3D <- function(pitch, roll, theme, label = NULL, body.col = "#ff453a") {
  plot(0, 0, type = "n", xlim = c(-1.25, 1.25), ylim = c(-1.5, 1.5), axes = FALSE, ann = FALSE, asp = 1)
  graphics::symbols(0, 0, circles = 1.12, inches = FALSE, add = TRUE,
                    bg = grDevices::adjustcolor("black", 0.82), fg = "grey60")
  graphics::segments(-0.98, 0, 0.98, 0, col = grDevices::adjustcolor("#5fa8d3", 0.3), lwd = 1)  # level reference

  th <- (if (is.finite(pitch)) pitch else 0) * pi / 180
  ro <- (if (is.finite(roll))  roll  else 0) * pi / 180
  Rx <- matrix(c(1, 0, 0,  0, cos(ro), -sin(ro),  0, sin(ro), cos(ro)), 3, byrow = TRUE)
  Ry <- matrix(c(cos(th), 0, sin(th),  0, 1, 0,  -sin(th), 0, cos(th)), 3, byrow = TRUE)
  R  <- Ry %*% Rx                                            # body -> world (yaw omitted)

  a   <- 24 * pi / 180                                       # camera look-down angle
  rr  <- c(0, 1, 0)                                          # screen-right = world +y (animal's right)
  uu  <- c(sin(a), 0, -cos(a))                               # screen-up
  dd  <- c(cos(a), 0, sin(a))                                # into-screen (depth, for painter's order)
  Lto <- c(0.25, -0.30, -0.92); Lto <- Lto / sqrt(sum(Lto^2))  # light from up / slightly left+front

  m <- .tagModel3D()
  polys <- lapply(m$faces, function(f) {
    w <- R %*% f
    list(xs = as.numeric(rr %*% w), ys = as.numeric(uu %*% w),
         depth = mean(as.numeric(dd %*% w)), light = 0.40 + 0.60 * abs(sum(.faceNormal(w) * Lto)))
  })
  depth <- vapply(polys, function(p) p$depth, numeric(1))
  dn    <- if (diff(range(depth)) > 0) (depth - min(depth)) / diff(range(depth)) else depth * 0    # 0 near, 1 far
  scl   <- 0.9
  for (k in order(depth, decreasing = TRUE)) {                # painter's: far -> near
    p <- polys[[k]]
    col  <- .shadeColor(body.col, p$light * (0.72 + 0.28 * (1 - dn[k])))   # + gentle far-dimming for depth
    bord <- if (identical(m$part[k], "fin")) grDevices::adjustcolor("grey10", 0.85) else grDevices::adjustcolor(col, 0.5)
    graphics::polygon(p$xs * scl, p$ys * scl, col = col, border = bord, lwd = 0.6)
  }

  if (!is.null(label)) graphics::text(0, 1.37, label, cex = 1.45, font = 2, col = body.col, xpd = NA)
  graphics::text(0, -1.37, sprintf("Roll %s\u00b0  Pitch %s\u00b0",
                                   if (is.finite(roll)) sprintf("%+.0f", roll) else "NA",
                                   if (is.finite(pitch)) sprintf("%+.0f", pitch) else "NA"),
                 cex = 1.15, col = theme$text, xpd = NA)
}

#' Circular orientation gauge for heading / pitch / roll.
#' @keywords internal
#' @noRd
.drawDial <- function(metric, value, theme) {
  plot(0, 0, type = "n", xlim = c(-1.15, 1.15), ylim = c(-1.15, 1.15), axes = FALSE, ann = FALSE, asp = 1)
  graphics::symbols(0, 0, circles = 1, inches = FALSE, add = TRUE, bg = grDevices::adjustcolor("black", 0.82), fg = "grey80")
  if (identical(metric, "heading")) {
    ang <- c(0, pi/2, pi, -pi/2) - pi/2
    graphics::text(-1.34 * cos(ang), -1.34 * sin(ang), c("N","W","S","E"), cex = 0.72, col = theme$text)
    for (a in ang) graphics::lines(c(0, -1.05 * cos(a)), c(0, 1.05 * sin(a)), col = "grey45", lwd = 0.5)
    if (is.finite(value)) {
      rad <- (90 - value) * pi/180
      graphics::arrows(cos(rad) * -0.62, sin(rad) * -0.62, cos(rad) * 0.62, sin(rad) * 0.62, col = "#ff3b30", lwd = 2.4, length = 0.13)
    }
  } else {
    grid_a <- if (identical(metric, "pitch")) seq(-90, 90, 30) else seq(-150, 180, 30)
    for (a in grid_a * pi/180) graphics::lines(c(0, -1.05 * cos(a)), c(0, 1.05 * sin(a)), col = "grey45", lwd = 0.5)
    for (a in grid_a) graphics::text(-1.30 * cos(a * pi/180), 1.30 * sin(a * pi/180), a, cex = 0.58, col = theme$text)
    if (is.finite(value)) {
      rad <- value * pi/180
      graphics::segments(0.62 * cos(rad), -0.62 * sin(rad), -0.62 * cos(rad), 0.62 * sin(rad), col = "#34c8c8", lwd = 2.6)
      graphics::points(0, 0, pch = 16, col = "#34c8c8", cex = 1.2)
    }
  }
  graphics::text(0, 1.74, tools::toTitleCase(metric), cex = 1.1, font = 2, xpd = NA, col = theme$text)
  graphics::text(0, 1.46, if (is.finite(value)) sprintf("%.0f\u00b0", value) else "NA", cex = 0.95, xpd = NA, col = theme$value)
}

#' Scrolling time-series panel with the current value marked.
#' @keywords internal
#' @noRd
.drawSeriesPanel <- function(clip, current_time, win, col, label, unit, cur_val, theme, invert, fill) {
  wd <- clip[abs(as.numeric(difftime(clip$datetime, current_time, units = "secs"))) <= win, ]
  y <- wd[[col]]; rng <- range(y, na.rm = TRUE)
  if (!all(is.finite(rng))) rng <- c(0, 1)
  ylim <- if (invert) c(rng[2], rng[1]) else rng
  plot(wd$datetime, y, type = "n", xlab = "", ylab = "", xaxs = "i", axes = FALSE, ylim = ylim)
  usr <- graphics::par("usr"); base <- if (invert) usr[4] else usr[3]
  graphics::polygon(c(wd$datetime, rev(wd$datetime)), c(y, rep(base, nrow(wd))), col = grDevices::adjustcolor(fill, 0.35), border = NA)
  graphics::lines(wd$datetime, y, col = "grey15", lwd = 1)
  if (is.finite(cur_val)) graphics::points(current_time, cur_val, col = "#ff3b30", pch = 16, cex = 1.5)
  graphics::title(main = label, line = 1.5, cex.main = 1.15, col.main = theme$text, xpd = NA)
  graphics::title(main = if (is.finite(cur_val)) sprintf("%.1f %s", cur_val, unit) else "NA",
                  font.main = 1, line = 0.5, cex.main = 1.0, col.main = theme$value, xpd = NA)
  graphics::axis(2, at = pretty(rng), las = 1, cex.axis = 0.65, col = "grey60", col.axis = theme$text)
}

#' Scrolling three-axis trace (e.g. body gyroscope or acceleration) with a now-marker and legend.
#' Axes whose column is absent are skipped; the panel degrades to a placeholder if none are present.
#' @keywords internal
#' @noRd
.drawTriTrace <- function(clip, current_time, win, cols, labels, title, theme, fd) {
  present <- cols %in% names(clip)
  line.cols <- c("#ff453a", "#34c8c8", "#ffd60a")
  wd <- clip[abs(as.numeric(difftime(clip$datetime, current_time, units = "secs"))) <= win, ]
  if (!any(present) || !nrow(wd)) {
    plot(0, 0, type = "n", axes = FALSE, ann = FALSE, xlim = c(0, 1), ylim = c(0, 1))
    graphics::title(main = title, line = 1.5, cex.main = 1.15, col.main = theme$text, xpd = NA)
    graphics::text(0.5, 0.5, "unavailable", col = "grey55", cex = 0.95)
    return(invisible())
  }
  ys <- lapply(cols[present], function(c) wd[[c]])
  rng <- range(unlist(ys), na.rm = TRUE); if (!all(is.finite(rng))) rng <- c(-1, 1)
  plot(wd$datetime, ys[[1]], type = "n", xlab = "", ylab = "", xaxs = "i", axes = FALSE, ylim = rng)
  graphics::abline(h = 0, col = "grey35", lwd = 0.6)
  cl <- line.cols[present]
  for (j in seq_along(ys)) graphics::lines(wd$datetime, ys[[j]], col = cl[j], lwd = 1.1)
  for (j in seq_along(ys)) {
    cv <- fd[[cols[present][j]]]
    if (is.finite(cv)) graphics::points(current_time, cv, col = cl[j], pch = 16, cex = 1.1)
  }
  graphics::title(main = title, line = 1.5, cex.main = 1.15, col.main = theme$text, xpd = NA)
  graphics::axis(2, at = pretty(rng), las = 1, cex.axis = 0.62, col = "grey60", col.axis = theme$text)
  graphics::legend("topright", legend = labels[present], col = cl, lwd = 1.6, bty = "n",
                   text.col = theme$text, cex = 0.62, horiz = TRUE, xpd = NA, inset = c(0, -0.06))
}

#' Bottom strip: timestamp only (general dashboard).
#' @keywords internal
#' @noRd
.drawTimestamp <- function(datetime, theme) {
  plot(0, 0, type = "n", ann = FALSE, axes = FALSE, xlim = c(0, 1), ylim = c(0, 1))
  graphics::text(0.5, 0.6, format(datetime, "%Y-%m-%d %H:%M:%OS1", tz = "UTC"), col = theme$text, cex = 1.05, font = 2)
}

#' Bottom strip: timestamp plus an optional caption line (validation dashboards).
#' @keywords internal
#' @noRd
.drawCaption <- function(datetime, caption, theme) {
  plot(0, 0, type = "n", ann = FALSE, axes = FALSE, xlim = c(0, 1), ylim = c(0, 1))
  graphics::text(0.5, 0.72, format(datetime, "%Y-%m-%d %H:%M:%OS1", tz = "UTC"), col = theme$text, cex = 1.0, font = 2)
  if (!is.null(caption) && nzchar(caption))
    graphics::text(0.5, 0.26, caption, col = "grey70", cex = 0.72, xpd = NA)
}


################################################################################
# Small helpers (internal) #####################################################
################################################################################

#' Distinct body-glyph colours for up to several candidate mappings.
#' @keywords internal
#' @noRd
.candidatePalette <- function(n) {
  pal <- c("#ff453a", "#34c8c8", "#ffd60a", "#bf5af2", "#30d158")
  pal[((seq_len(max(n, 1)) - 1L) %% length(pal)) + 1L]
}

#' Locate the ffmpeg binary (cross-platform), or abort with guidance.
#' @keywords internal
#' @noRd
.ffmpegBin <- function() {
  bin <- Sys.which("ffmpeg")
  if (!nzchar(bin)) .abort(c("FFmpeg was not found on the system path.", "i" = "Install FFmpeg to use {.fn renderOverlayVideo}."))
  unname(bin)
}

#' Locate the `ffprobe` binary (installed alongside FFmpeg), or abort with guidance.
#' @keywords internal
#' @noRd
.ffprobeBin <- function() {
  bin <- Sys.which("ffprobe")
  if (!nzchar(bin)) .abort(c("ffprobe was not found on the system path.", "i" = "Install FFmpeg (it provides ffprobe) to read video metadata."))
  unname(bin)
}

#' Does the local ffmpeg expose a given encoder?
#' @keywords internal
#' @noRd
.hasEncoder <- function(name) {
  enc <- tryCatch(system2(.ffmpegBin(), c("-hide_banner", "-encoders"), stdout = TRUE, stderr = FALSE), error = function(e) "")
  any(grepl(name, enc, fixed = TRUE))
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
