#######################################################################################################
# Render a sensor-overlay video #######################################################################
#######################################################################################################

#' Render a time-synchronised sensor dashboard with camera-tag video
#'
#' @description
#' Renders sensor measurements from one archival-tag deployment together with its corresponding video.
#' The result provides a common visual timeline for checking inferred movement against observed
#' behaviour, reviewing sensor-axis mappings, and preparing annotated material for presentation.
#'
#' The dashboard can be placed beside the footage on an opaque panel or composited directly over the
#' footage with a transparent background. Presentation dashboards are assembled from selectable metric
#' modules, while the validation dashboards retain fixed layouts designed for assessing sensor-axis
#' orientation.
#'
#' Synchronisation is determined by `video.start`, which identifies the sensor-clock time represented by
#' the first frame of `video`. The function does not estimate or correct clock offsets. Video timestamps
#' should therefore be checked with [getVideoMetadata()] and, where necessary, corrected before
#' rendering. Sensor values and metadata are not modified.
#'
#' @param video Path to one source video file.
#' @param data A `nautilus_tag`, data frame or data table containing the sensor data for one deployment;
#'   alternatively, a list containing exactly one such dataset. A finite `POSIXct` column named
#'   `datetime` is required. Additional required columns depend on `dashboard` and `metrics`; see
#'   Details. Processed output from [processTagData()] is recommended.
#' @param output Path of the video file to create. The parent directory must already exist. An existing
#'   file at this path is overwritten. An `.mp4` output is recommended for the supported codecs.
#' @param dashboard Dashboard layout. `"general"` (default), `"compact"`, `"expanded"`, `"ribbon"`
#'   and `"focus"` are configurable presentation layouts. `"validation"` and
#'   `"validation-compare"` are fixed sensor-axis review layouts used by [reviewTagMapping()]. See
#'   Details.
#' @param metrics Character vector naming presentation modules in display order, or `NULL` (default) to
#'   use the preset associated with `dashboard`. Supported values are `"orientation"`, `"heading"`,
#'   `"pitch"`, `"roll"`, `"depth"`, `"vedba"`, `"vertical_velocity"`, `"paddle_speed"`,
#'   `"paddle_freq"`, `"tbf_hz_peaks"`, `"tbf_hz_wavelet"` and `"pseudo_trajectory"`. Module names
#'   must be unique after resolving `orientation`. This argument cannot be supplied for the fixed
#'   validation dashboards.
#' @param composition Method used to combine the dashboard and footage. `"beside"` (default) adds an
#'   opaque panel to the left or right of the video, increasing the output width. `"overlay"` places a
#'   transparent dashboard over the video and preserves the source width and height. The `"ribbon"`
#'   dashboard requires `"overlay"`.
#' @param orientation Representation used when `metrics` contains `"orientation"`. `"model"` (default)
#'   draws a single three-dimensional attitude model with a heading compass and numeric heading, pitch
#'   and roll. `"dials"` replaces it with three separate gauges. This argument does not change the fixed
#'   validation dashboards.
#' @param orientation.model Schematic animal model used by three-dimensional orientation modules:
#'   `"shark"` (default), `"cetacean"`, `"turtle"`, `"fish"` or `"manta"`. The choice changes only the
#'   displayed silhouette; it does not alter the underlying orientation values or coordinate
#'   conventions. Presentation dashboards ignore it when `orientation = "dials"`; fixed validation
#'   dashboards always use the selected model. `"cetacean"` depicts a dolphin-like odontocete,
#'   `"turtle"` a hard-shelled sea turtle, and `"fish"` a generic ray-finned fish; these are not
#'   species-specific anatomical reconstructions.
#' @param video.start One finite `POSIXct` value giving the sensor-clock time represented by the first
#'   frame of `video`. `NULL` (default) attempts to read a UTC timestamp in `YYYYMMDD-HHMMSS` or
#'   `YYMMDD-HHMMSS` form from the file name. A value returned by [getVideoMetadata()] is recommended.
#' @param start,end Optional finite `POSIXct` bounds, expressed on the same clock as `video.start` and
#'   `data$datetime`. `NULL` (default) uses the full temporal overlap between the sensor record and video.
#'   These arguments can restrict the render to a validation or behavioural interval.
#' @param side Position of the dashboard: `"right"` (default) or `"left"`. In beside mode it determines
#'   panel order; in overlay mode it anchors the dashboard to that edge. It is ignored by the full-width
#'   `"ribbon"` layout.
#' @param overlay.fps Positive dashboard update rate in frames per second (default `5`). It is independent
#'   of the source video frame rate, which is retained in the output. Lower values reduce rendering time
#'   but update sensor indicators less frequently.
#' @param panel.width Dashboard width in pixels, or `NULL` (default) to calculate it from the source video
#'   and selected layout. In overlay mode the automatically calculated width is limited to part of the
#'   source frame. Ignored by the full-width `"ribbon"` layout.
#' @param depth.window,activity.window Positive context windows in seconds on each side of the current
#'   frame. `depth.window` controls `"depth"` (default `300`); `activity.window` controls the other
#'   scrolling scalar modules (default `30`). Module-specific values in `metric.windows` take precedence.
#' @param metric.windows Optional named numeric vector giving context windows, in seconds on each side of
#'   the current frame, for selected scrolling modules. For example,
#'   `c(depth = 600, vedba = 20, pseudo_trajectory = 120)`. Names must identify modules selected for the
#'   current dashboard; non-scrolling modules cannot be assigned a window. Default `NULL`.
#' @param background.alpha Opacity of the local module backplates in overlay mode, between `0` and `1`
#'   (default `0.72`). Text, traces and indicators remain opaque, and areas outside the modules remain
#'   transparent. Ignored in beside mode.
#' @param caption Optional one-line caption for `"validation"` or `"validation-compare"`, such as the
#'   candidate mapping and reason for review. Default `NULL`.
#' @param candidates For `"validation-compare"`, a data frame with columns `label`, `pitch` and `roll`.
#'   Each row defines one candidate: `label` supplies its displayed name, while `pitch` and `roll` contain
#'   the names of the corresponding columns in `data`. Required for `"validation-compare"` and otherwise
#'   ignored.
#' @param crf Non-negative constant-rate-factor value used by software encoders (default `23`). Lower
#'   values generally increase output quality and file size. Ignored when a macOS VideoToolbox hardware
#'   encoder is used.
#' @param codec Output codec family: `"hevc"` (default) or `"h264"`. HEVC generally produces smaller
#'   files and is written with the `hvc1` tag for QuickTime compatibility. H.264 has broader playback
#'   support and is preferable for distribution. The function uses a macOS hardware encoder when
#'   available and retries with the corresponding software encoder if the hardware session fails.
#' @param keep.temp Logical; whether to retain the intermediate dashboard video or transparent PNG frame
#'   directory for diagnosis (default `FALSE`).
#' @param verbose How much detail to print: `0`/`"quiet"`, `1`/`"normal"`, or `2`/`"detailed"`
#'   (default). Normal and detailed output include separate progress indicators for dashboard-frame
#'   rendering and FFmpeg composition when the console supports live progress output.
#'
#' @details
#' ## Time alignment and rendered interval
#'
#' `video.start` is the only synchronisation anchor: it must give the time, on the sensor clock, of the
#' first source-video frame. If it is omitted, the file-name timestamp is interpreted as UTC. No
#' time-zone conversion, device-clock correction or cross-correlation is performed during rendering.
#' Where video and sensor clocks differ, resolve that discrepancy through [getVideoMetadata()] and
#' [getVideoClockCorrections()] before calling this function.
#'
#' The rendered interval is the intersection of the video coverage, the sensor coverage, and any
#' `start`/`end` bounds. The function stops if that intersection is empty. At each dashboard update, the
#' displayed state is taken from the most recent sensor observation at or before the frame time. The
#' timestamp printed on the dashboard is displayed in UTC.
#'
#' ## Dashboard layouts
#'
#' \describe{
#'   \item{`"general"`}{Full presentation dashboard. Its preset contains `"orientation"`, `"depth"`,
#'     `"vedba"` and `"vertical_velocity"`. It supports both composition modes.}
#'   \item{`"compact"`}{Narrow stacked dashboard. Its preset contains `"orientation"`, `"depth"` and
#'     `"vedba"`.}
#'   \item{`"expanded"`}{Wide dashboard led by `"pseudo_trajectory"`, followed by orientation and
#'     scrolling depth and activity context. The input must normally be the output of
#'     [reconstructTrack()].}
#'   \item{`"ribbon"`}{Full-width lower-third overlay. Its preset contains `"depth"`, `"vedba"` and
#'     `"vertical_velocity"`; at most four modules can be selected.}
#'   \item{`"focus"`}{Emphasises the first selected module and presents the remaining modules as compact
#'     current-value tiles.}
#'   \item{`"validation"`}{Fixed sensor-axis review dashboard containing one large pitch/roll attitude
#'     model, a three-axis gyroscope trace where those channels are available, and depth. Heading is
#'     intentionally omitted because roll handedness, rather than compass direction, is the relevant
#'     validation cue.}
#'   \item{`"validation-compare"`}{Fixed comparison dashboard containing one pitch/roll attitude model
#'     for each row of `candidates`, together with depth and the optional review caption.}
#' }
#'
#' ## Metric modules and input columns
#'
#' Every input requires `datetime`. Presentation modules have the following additional contracts:
#'
#' \describe{
#'   \item{`"orientation"`}{`heading`, `pitch` and `roll`, in degrees. The model represents pitch and
#'     roll; heading is shown on the accompanying compass. `orientation = "dials"` uses the same columns.}
#'   \item{`"heading"`, `"pitch"`, `"roll"`}{The identically named angular column, in degrees.}
#'   \item{`"depth"`}{`depth`, in metres.}
#'   \item{`"vedba"`}{`vedba`, in g.}
#'   \item{`"vertical_velocity"`}{`vertical_velocity`, in m/s, positive during descent and negative
#'     during ascent.}
#'   \item{`"paddle_speed"`}{`paddle_speed`, in m/s.}
#'   \item{`"paddle_freq"`}{`paddle_freq`, in Hz.}
#'   \item{`"tbf_hz_peaks"`, `"tbf_hz_wavelet"`}{The identically named tail-beat frequency estimate,
#'     in Hz.}
#'   \item{`"pseudo_trajectory"`}{`pseudo_lon` and `pseudo_lat`, in decimal degrees, and `pseudo_depth`,
#'     in metres. `heading` is optional and, where present, supplies the current-direction arrow.}
#' }
#'
#' The `"validation"` dashboard requires `depth`, `pitch` and `roll`; `gx`, `gy` and `gz` are optional.
#' `"validation-compare"` requires `depth` plus every pitch and roll column named by `candidates`.
#' A requested module with missing required columns causes an error rather than being silently omitted.
#'
#' ## Scientific and visual conventions
#'
#' Orientation modules display values already present in `data`; they do not estimate orientation or
#' verify that the inertial axes are expressed in the animal's body frame. Apply [applyAxisMapping()] and
#' [processTagData()] before interpreting the model as animal posture. The three-dimensional model is a
#' visual representation of pitch and roll, not an independent orientation estimate. Model choices are
#' deliberately schematic and are intended to provide a recognisable attitude cue rather than an
#' anatomically precise reconstruction.
#'
#' Plot limits are calculated once from the sensor subset used for the render, including context-window
#' padding, so they do not expand and contract between frames. For series with more than 20 observations,
#' scalar limits use the 0.5th and 99.5th percentiles with padding; VeDBA uses zero and its 99.5th
#' percentile; vertical velocity uses a symmetric 99.5th-percentile absolute limit. Extreme traces
#' outside those robust limits can therefore be clipped visually, while the displayed current numeric
#' value remains unchanged.
#'
#' VeDBA is shown as the raw trace plus a centred one-second moving mean used only for display. The
#' smoother does not modify `data` or any saved sensor values. Vertical velocity follows the package sign
#' convention: negative ascent is shown above zero in blue, and positive descent below zero in orange.
#' A zero line separates the two directions.
#'
#' The pseudo-trajectory is projected once into a local east/north plane in metres. Plot bounds, camera
#' angle and vertical exaggeration remain fixed for the sensor subset used for the render. Past movement
#' is solid, future context is dashed, and the current point is linked to its surface projection. Sensor
#' gaps are not bridged. The stated depth exaggeration is chosen for legibility; the display is a local
#' representation of an existing pseudo-track, not a geographic map or a new position estimate.
#'
#' ## Composition, encoding and temporary files
#'
#' Beside mode retains the source height and adds `panel.width` to its width. Overlay mode preserves both
#' source dimensions. The source video frame rate is retained, while dashboard graphics are updated at
#' `overlay.fps`. Source audio is included in the rendered interval where present.
#'
#' Rendering requires the \pkg{av} package and an FFmpeg executable on the system path. Beside mode stores
#' the dashboard temporarily as a compact video. Overlay mode stores transparent PNG frames so their
#' alpha channel is preserved until the final FFmpeg composition. Temporary material is removed after a
#' successful or failed render unless `keep.temp = TRUE`.
#'
#' @return The `output` path, returned invisibly after the video has been written.
#'
#' @seealso [getVideoMetadata()] and [getVideoClockCorrections()] for video timing;
#'   [processTagData()] for deriving the displayed sensor metrics; [reconstructTrack()] for producing the
#'   pseudo-trajectory columns; [reviewTagMapping()] and [findValidationSegments()] for sensor-axis
#'   validation.
#'
#' @examples
#' \dontrun{
#' tag <- processed[["PIN_CAM_01"]]
#' video_metadata <- getVideoMetadata("./videos/PIN_CAM_01")
#'
#' # Render the default presentation dashboard beside the footage.
#' renderOverlayVideo(video = video_metadata$file[1],
#'                    data = tag,
#'                    output = "./overlay/PIN_CAM_01.mp4",
#'                    video.start = video_metadata$start[1])
#'
#' # Place a selected set of modules directly over the video.
#' renderOverlayVideo(video = video_metadata$file[1],
#'                    data = tag,
#'                    output = "./overlay/PIN_CAM_01_compact.mp4",
#'                    dashboard = "compact",
#'                    metrics = c("orientation", "depth", "vedba"),
#'                    composition = "overlay",
#'                    side = "right",
#'                    video.start = video_metadata$start[1])
#'
#' # Replace the combined orientation model with separate gauges.
#' renderOverlayVideo(video = video_metadata$file[1],
#'                    data = tag,
#'                    output = "./overlay/PIN_CAM_01_dials.mp4",
#'                    orientation = "dials",
#'                    video.start = video_metadata$start[1])
#'
#' # Use a different schematic body model without changing the sensor mapping.
#' renderOverlayVideo(video = video_metadata$file[1],
#'                    data = tag,
#'                    output = "./overlay/PIN_CAM_01_cetacean.mp4",
#'                    orientation.model = "cetacean",
#'                    video.start = video_metadata$start[1])
#'
#' # Give the reconstructed pseudo-trajectory priority in an expanded dashboard.
#' tracked <- reconstructTrack(list(PIN_CAM_01 = tag))
#' renderOverlayVideo(video = video_metadata$file[1],
#'                    data = tracked[["PIN_CAM_01"]],
#'                    output = "./overlay/PIN_CAM_01_track.mp4",
#'                    dashboard = "expanded",
#'                    video.start = video_metadata$start[1])
#' }
#' @export

renderOverlayVideo <- function(video,
                               data,
                               output,
                               dashboard = c("general", "compact", "expanded", "ribbon", "focus",
                                             "validation", "validation-compare"),
                               metrics = NULL,
                               composition = c("beside", "overlay"),
                               orientation = c("model", "dials"),
                               orientation.model = c("shark", "cetacean", "turtle", "fish", "manta"),
                               video.start = NULL,
                               start = NULL,
                               end = NULL,
                               side = c("right", "left"),
                               overlay.fps = 5,
                               panel.width = NULL,
                               depth.window = 300,
                               activity.window = 30,
                               metric.windows = NULL,
                               background.alpha = 0.72,
                               caption = NULL,
                               candidates = NULL,
                               crf = 23,
                               codec = c("hevc", "h264"),
                               keep.temp = FALSE,
                               verbose = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  dashboard <- match.arg(dashboard)
  composition <- match.arg(composition)
  orientation <- match.arg(orientation)
  orientation.model <- match.arg(orientation.model)
  side <- match.arg(side)
  codec <- match.arg(codec)
  .assert_number(overlay.fps, "overlay.fps", min = 0.1)
  .assert_number(crf, "crf", min = 0)
  .assert_number(depth.window, "depth.window", min = 0.1)
  .assert_number(activity.window, "activity.window", min = 0.1)
  .assert_number(background.alpha, "background.alpha", min = 0)
  if (background.alpha > 1) .abort("{.arg background.alpha} must lie between zero and one.")
  if (!is.null(panel.width)) .assert_number(panel.width, "panel.width", min = 1)
  if (identical(dashboard, "ribbon") && !identical(composition, "overlay"))
    .abort("The {.val ribbon} dashboard requires {.code composition = \"overlay\"}.")
  if (dashboard == "validation-compare" &&
      (is.null(candidates) || !all(c("label", "pitch", "roll") %in% names(candidates)) || !nrow(candidates)))
    .abort("{.arg candidates} (a data.frame with {.field label}/{.field pitch}/{.field roll}) is required for the compare dashboard.")
  if (dashboard %in% c("validation", "validation-compare") && !is.null(metrics))
    .abort("{.arg metrics} cannot be supplied for the fixed {.val {dashboard}} dashboard.")
  if (!file.exists(video)) .abort("Video file not found: {.file {video}}.")
  if (!dir.exists(dirname(normalizePath(output, mustWork = FALSE)))) .abort("Output directory does not exist: {.file {dirname(output)}}.")

  # resolve the sensor data to a single data.table with the columns this dashboard needs
  if (is.list(data) && !inherits(data, "data.frame")) {
    if (length(data) != 1L)
      .abort("{.arg data} must contain exactly one deployment for video rendering.")
    data <- data[[1]]
  }
  data <- data.table::copy(data.table::as.data.table(data))
  plan <- .resolveDashboardPlan(dashboard, metrics, orientation, orientation.model, candidates,
                                depth.window, activity.window, metric.windows)
  need <- .dashboardColumns(dashboard, candidates, plan$metrics, orientation)
  miss <- setdiff(need, names(data))
  if (length(miss)) .abort(c("{.arg data} is missing {cli::qty(miss)}column{?s} required by the {.val {dashboard}} dashboard: {.val {miss}}.",
                             "i" = "Pass the output of {.fn processTagData} (general) or use {.fn reviewTagMapping} (validation)."))
  data.table::setorder(data, datetime)
  if (!inherits(data$datetime, "POSIXct")) .abort("{.field datetime} in {.arg data} must be POSIXct.")
  if (!nrow(data) || !any(is.finite(as.numeric(data$datetime))))
    .abort("{.arg data} contains no finite timestamps to align with the video.")

  # heavy dependencies (checked only once the inputs are known good)
  if (!requireNamespace("av", quietly = TRUE)) .abort("The {.pkg av} package is required: {.code install.packages('av')}.")
  ffmpeg <- .ffmpegBin()

  # probe the source video (dimensions, frame rate, duration)
  info <- av::av_video_info(video)
  v_fps <- info$video$framerate; v_w <- info$video$width; v_h <- info$video$height; v_dur <- info$duration
  if (!is.finite(v_h) || !is.finite(v_w) || !is.finite(v_fps) || !is.finite(v_dur) ||
      v_w <= 0 || v_h <= 0 || v_fps <= 0 || v_dur <= 0)
    .abort("Could not read valid dimensions, frame rate, and duration from the source video.")
  if (v_h < 360)
    .abort(c("The source video is too short (height {v_h}px) for the dashboard panel.",
             "i" = "{.fn renderOverlayVideo} needs a video at least 360px tall."))

  # sync anchor: sensor datetime of the video's first frame
  if (is.null(video.start)) {
    video.start <- .videoStartFromFilename(video)
    if (is.null(video.start)) .abort(c("Could not determine {.arg video.start} from the file name.",
                                       "i" = "Pass {.arg video.start} (POSIXct of the first video frame) explicitly."))
  }
  if (!inherits(video.start, "POSIXct") || length(video.start) != 1L || !is.finite(as.numeric(video.start)))
    .abort("{.arg video.start} must be one finite POSIXct value.")
  if (!is.null(start) && (!inherits(start, "POSIXct") || length(start) != 1L || !is.finite(as.numeric(start))))
    .abort("{.arg start} must be one finite POSIXct value or NULL.")
  if (!is.null(end) && (!inherits(end, "POSIXct") || length(end) != 1L || !is.finite(as.numeric(end))))
    .abort("{.arg end} must be one finite POSIXct value or NULL.")

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
              arrow = sprintf("Dashboard: %s \u00b7 %s \u00b7 %g fps \u00b7 %s",
                              dashboard, composition, overlay.fps,
                              if (dashboard == "ribbon") "lower edge" else side))

  # subset sensor data to the clip plus window padding (so per-frame lookups stay cheap)
  pad <- max(c(depth.window, activity.window, unname(plan$windows)), na.rm = TRUE)
  clip <- data[datetime >= clip_lo - pad & datetime <= clip_hi + pad]
  context <- .prepareOverlayContext(clip, plan)
  clip <- context$clip

  # one dashboard frame per overlay step; map each to its nearest sensor row (precomputed, vectorised)
  n_frames <- max(1L, as.integer(ceiling(clip_dur * overlay.fps)))
  frame_times <- clip_lo + (seq_len(n_frames) - 1) / overlay.fps
  idx <- findInterval(as.numeric(frame_times), as.numeric(clip$datetime))
  idx <- pmin(pmax(idx, 1L), nrow(clip))

  if (identical(dashboard, "ribbon")) {
    panel_w <- as.integer(v_w)
    panel_h <- .evenDimension(round(v_h * 0.30))
  } else {
    factor <- switch(dashboard, expanded = 0.72, `validation-compare` = 0.85, compact = 0.45,
                     focus = 0.50, 0.55)
    panel_w <- if (is.null(panel.width)) as.integer(round(v_h * factor)) else as.integer(panel.width)
    if (composition == "overlay") panel_w <- min(panel_w, as.integer(round(v_w * 0.48)))
    panel_w <- .evenDimension(panel_w)
    panel_h <- as.integer(v_h)
  }
  if (panel_w < 180L || panel_h < 120L)
    .abort("The resolved dashboard dimensions are too small to render legibly.")
  if (composition == "overlay" && panel_w > v_w)
    .abort("{.arg panel.width} cannot exceed the source width in overlay mode.")
  theme <- .overlayTheme(composition, background.alpha)
  # font size scaled to the panel so the dashboard stays legible when the whole composite is viewed at a
  # normal (down-scaled) resolution - the main readability lever
  ps <- max(14L, as.integer(round(min(panel_w, panel_h) / 30)))

  if (lvl >= 1L) .log_detail(lvl, sprintf("rendering %s dashboard frame%s at %g fps", .formatLargeNumber(n_frames),
                                          if (n_frames != 1) "s" else "", overlay.fps))
  frame_pb <- .log_progress_start(lvl, n_frames, "Rendering dashboard", min.level = 1L)
  if (composition == "beside") {
    # Preserve the established disk-efficient route for the default: one compact dashboard video rather
    # than thousands of images. Alpha is unnecessary because this panel is opaque.
    dashboard_artifact <- tempfile("nautilus-dashboard-", fileext = ".mp4")
    if (!keep.temp) on.exit(unlink(dashboard_artifact), add = TRUE)
    tryCatch(av::av_capture_graphics(
      expr = {
        for (i in seq_len(n_frames)) {
          .drawDashboard(dashboard, clip[idx[i]], clip, frame_times[i], depth.window, activity.window,
                         theme, caption, candidates, plan = plan, context = context)
          .log_progress_step(frame_pb)
        }
      },
      output = dashboard_artifact, width = panel_w, height = panel_h,
      framerate = overlay.fps, verbose = FALSE, pointsize = ps),
      finally = .log_progress_done(frame_pb))
    dashboard_input <- c("-i", dashboard_artifact)
  } else {
    # PNG frames are used only where their alpha channel is needed. FFmpeg consumes the sequence directly
    # in the final composition pass, so the transparent dashboard is never flattened by an intermediate
    # video codec.
    dashboard_artifact <- tempfile("nautilus-overlay-")
    dir.create(dashboard_artifact)
    if (!keep.temp) on.exit(unlink(dashboard_artifact, recursive = TRUE), add = TRUE)
    frame_pattern <- file.path(dashboard_artifact, "dashboard_%06d.png")
    png_args <- list(filename = frame_pattern, width = panel_w, height = panel_h,
                     bg = theme$canvas, pointsize = ps)
    if (capabilities("cairo")) png_args$type <- "cairo"
    do.call(grDevices::png, png_args)
    device_open <- TRUE
    tryCatch({
      for (i in seq_len(n_frames)) {
        .drawDashboard(dashboard, clip[idx[i]], clip, frame_times[i], depth.window, activity.window,
                       theme, caption, candidates, plan = plan, context = context)
        .log_progress_step(frame_pb)
      }
    }, finally = {
      if (device_open) grDevices::dev.off()
      .log_progress_done(frame_pb)
    })
    dashboard_input <- c("-framerate", sprintf("%g", overlay.fps), "-start_number", "1", "-i", frame_pattern)
  }

  # ---- one FFmpeg pass: trim, composite, map audio and encode the final output ----
  # Resolve the codec FAMILY to a concrete encoder (prefer the macOS hardware VideoToolbox encoder),
  # its rate-control flag, and the container tag. HEVC must be tagged `hvc1` (ffmpeg muxes it as `hev1`
  # by default, which QuickTime refuses to open); H.264's default `avc1` is already universal.
  enc <- switch(codec,
                hevc = if (.hasEncoder("hevc_videotoolbox")) "hevc_videotoolbox" else "libx265",
                h264 = if (.hasEncoder("h264_videotoolbox")) "h264_videotoolbox" else "libx264")
  tag_arg <- if (codec == "hevc") c("-tag:v", "hvc1")
  q_arg   <- if (grepl("videotoolbox", enc)) c("-q:v", "60") else c("-crf", as.character(crf))
  if (composition == "beside") {
    stack <- if (side == "right") "[base][ov]hstack=inputs=2[vout]" else "[ov][base]hstack=inputs=2[vout]"
    filt <- sprintf("[0:v]setsar=1[base];[1:v]fps=%g,scale=%d:%d:flags=lanczos,format=rgba,setsar=1[ov];%s",
                    v_fps, panel_w, v_h, stack)
  } else {
    xy <- if (dashboard == "ribbon") c("0", "main_h-overlay_h") else
      if (side == "right") c("main_w-overlay_w", "0") else c("0", "0")
    filt <- sprintf("[0:v]setsar=1[base];[1:v]fps=%g,format=rgba,setsar=1[ov];[base][ov]overlay=x=%s:y=%s:shortest=1:format=auto[vout]",
                    v_fps, xy[1], xy[2])
  }
  args <- c("-y", "-ss", sprintf("%.3f", seek_secs), "-t", sprintf("%.3f", clip_dur), "-i", video,
            dashboard_input,
            "-filter_complex", filt, "-map", "[vout]", "-map", "0:a?", "-t", sprintf("%.3f", clip_dur),
            "-c:v", enc, q_arg, tag_arg, "-pix_fmt", "yuv420p", "-movflags", "+faststart",
            normalizePath(output, mustWork = FALSE))
  if (lvl >= 1L) .log_detail(lvl, "compositing with ffmpeg")
  status <- .runFfmpegWithProgress(ffmpeg, args, clip_dur, lvl)
  # Listing a VideoToolbox encoder does not guarantee that macOS can open a hardware compression
  # session at this moment. Fall back once to the corresponding software encoder instead of leaving
  # behind an empty output file when the hardware is busy or rejects the resolved dimensions.
  if (status != 0 && grepl("videotoolbox", enc)) {
    fallback <- if (codec == "hevc") "libx265" else "libx264"
    if (.hasEncoder(fallback)) {
      if (lvl >= 1L) .log_detail(lvl, sprintf("hardware encoder unavailable; retrying with %s", fallback))
      encoder_at <- match("-c:v", args) + 1L
      quality_at <- match("-q:v", args)
      args[encoder_at] <- fallback
      args[quality_at] <- "-crf"
      args[quality_at + 1L] <- as.character(crf)
      status <- .runFfmpegWithProgress(ffmpeg, args, clip_dur, lvl)
    }
  }
  if (status != 0 || !file.exists(output)) .abort("FFmpeg failed to create the output video (exit status {status}).")

  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, "overlay video written")
    .log_arrow(lvl, "output: ", output)
    if (keep.temp) .log_arrow(lvl, "dashboard temporary files: ", dashboard_artifact)
    .log_runtime(lvl, start.time)
  }
  invisible(output)
}


################################################################################
# Dashboards (internal) ########################################################
################################################################################

#' Registry of presentation modules and their data contracts.
#' @keywords internal
#' @noRd
.overlayMetricRegistry <- function() {
  list(
    orientation       = list(columns = c("heading", "pitch", "roll"), type = "orientation",
                             label = "Orientation", unit = "", colour = "#ff453a", window = NA_real_),
    heading           = list(columns = "heading", type = "dial", label = "Heading", unit = "\u00b0",
                             colour = "#ff453a", window = NA_real_),
    pitch             = list(columns = "pitch", type = "dial", label = "Pitch", unit = "\u00b0",
                             colour = "#34c8c8", window = NA_real_),
    roll              = list(columns = "roll", type = "dial", label = "Roll", unit = "\u00b0",
                             colour = "#34c8c8", window = NA_real_),
    depth             = list(columns = "depth", type = "depth", label = "Depth", unit = "m",
                             colour = "#0a84ff", window = "depth"),
    vedba             = list(columns = "vedba", type = "vedba", label = "VeDBA", unit = "g",
                             colour = "#ffd60a", window = "activity"),
    vertical_velocity = list(columns = "vertical_velocity", type = "vertical_velocity",
                             label = "Vertical velocity", unit = "m/s", colour = "#ff9f0a",
                             window = "activity"),
    paddle_speed      = list(columns = "paddle_speed", type = "series", label = "Paddle speed",
                             unit = "m/s", colour = "#30d158", window = "activity"),
    paddle_freq       = list(columns = "paddle_freq", type = "series", label = "Paddle frequency",
                             unit = "Hz", colour = "#64d2ff", window = "activity"),
    tbf_hz_peaks      = list(columns = "tbf_hz_peaks", type = "series", label = "Tailbeat frequency",
                             unit = "Hz", colour = "#bf5af2", window = "activity"),
    tbf_hz_wavelet    = list(columns = "tbf_hz_wavelet", type = "series", label = "Wavelet tailbeat",
                             unit = "Hz", colour = "#af52de", window = "activity"),
    pseudo_trajectory = list(columns = c("pseudo_lon", "pseudo_lat", "pseudo_depth"),
                             type = "trajectory", label = "Pseudo-trajectory", unit = "m",
                             colour = "#30d158", window = 120)
  )
}

#' Resolve a dashboard preset, orientation representation and per-module windows.
#' @keywords internal
#' @noRd
.resolveDashboardPlan <- function(dashboard, metrics = NULL, orientation = "model",
                                  orientation.model = "shark", candidates = NULL,
                                  depth.window = 300, activity.window = 30, metric.windows = NULL) {
  if (dashboard %in% c("validation", "validation-compare"))
    return(list(dashboard = dashboard, metrics = character(), windows = numeric(),
                orientation = orientation, orientation.model = orientation.model))

  presets <- list(
    general  = c("orientation", "depth", "vedba", "vertical_velocity"),
    compact  = c("orientation", "depth", "vedba"),
    expanded = c("pseudo_trajectory", "orientation", "depth", "vedba", "vertical_velocity"),
    ribbon   = c("depth", "vedba", "vertical_velocity"),
    focus    = c("depth", "orientation", "vedba", "vertical_velocity")
  )
  if (is.null(metrics)) metrics <- presets[[dashboard]]
  if (!is.character(metrics) || !length(metrics) || anyNA(metrics) || any(!nzchar(metrics)))
    .abort("{.arg metrics} must be a non-empty character vector of module names.")

  registry <- .overlayMetricRegistry()
  unknown <- setdiff(metrics, names(registry))
  if (length(unknown))
    .abort(c("Unknown dashboard {cli::qty(unknown)}module{?s}: {.val {unknown}}.",
             "i" = "Choose from {.val {names(registry)}}."))
  if (orientation == "dials")
    metrics <- unlist(lapply(metrics, function(x) if (x == "orientation") c("heading", "pitch", "roll") else x),
                      use.names = FALSE)
  if (anyDuplicated(metrics))
    .abort("{.arg metrics} resolves to duplicated modules: {.val {unique(metrics[duplicated(metrics)])}}.")
  if (dashboard == "ribbon" && length(metrics) > 4L)
    .abort("The {.val ribbon} dashboard supports at most four modules.")

  windows <- vapply(metrics, function(metric) {
    value <- registry[[metric]]$window
    if (is.character(value) && value == "depth") depth.window else
      if (is.character(value) && value == "activity") activity.window else
        if (is.numeric(value)) value else NA_real_
  }, numeric(1))
  names(windows) <- metrics

  if (!is.null(metric.windows)) {
    if (!is.numeric(metric.windows) || is.null(names(metric.windows)) ||
        anyNA(names(metric.windows)) || any(!nzchar(names(metric.windows))) || anyDuplicated(names(metric.windows)) ||
        any(!is.finite(metric.windows)) || any(metric.windows <= 0))
      .abort("{.arg metric.windows} must be a named numeric vector of positive finite seconds.")
    bad <- setdiff(names(metric.windows), names(windows)[is.finite(windows)])
    if (length(bad))
      .abort("{.arg metric.windows} names must identify selected scrolling modules: {.val {bad}} does not.")
    windows[names(metric.windows)] <- metric.windows
  }
  list(dashboard = dashboard, metrics = metrics, windows = windows, orientation = orientation,
       orientation.model = orientation.model)
}

#' Columns a given dashboard requires (alongside `datetime`).
#' @keywords internal
#' @noRd
.dashboardColumns <- function(dashboard, candidates = NULL, metrics = NULL, orientation = "model") {
  if (dashboard == "validation") return(c("datetime", "depth", "pitch", "roll"))
  if (dashboard == "validation-compare")
    return(c("datetime", "depth", unique(c(candidates$pitch, candidates$roll))))
  if (is.null(metrics))
    metrics <- .resolveDashboardPlan(dashboard, orientation = orientation)$metrics
  registry <- .overlayMetricRegistry()
  unique(c("datetime", unlist(lapply(metrics, function(x) registry[[x]]$columns), use.names = FALSE)))
}

#' Prepare display-only smoothers, stable scales and projected trajectory geometry once per clip.
#' @keywords internal
#' @noRd
.prepareOverlayContext <- function(clip, plan) {
  clip <- data.table::copy(clip)
  if ("vedba" %in% plan$metrics) {
    hz <- tryCatch(.estimateHz(clip$datetime), error = function(e) NA_real_)
    n <- if (is.finite(hz)) max(1L, as.integer(round(hz))) else 1L
    n <- min(n, nrow(clip))
    smooth <- data.table::frollmean(clip$vedba, n = n, align = "center", fill = NA_real_, na.rm = TRUE)
    smooth[!is.finite(smooth)] <- clip$vedba[!is.finite(smooth)]
    clip[, (".overlay_vedba") := smooth]
  }

  scalar <- intersect(plan$metrics, c("depth", "vedba", "vertical_velocity", "paddle_speed",
                                      "paddle_freq", "tbf_hz_peaks", "tbf_hz_wavelet"))
  ranges <- setNames(vector("list", length(scalar)), scalar)
  for (metric in scalar) {
    values <- if (metric == "vedba" && ".overlay_vedba" %in% names(clip)) clip$.overlay_vedba else clip[[metric]]
    ranges[[metric]] <- .overlayRange(values, metric)
  }
  trajectory <- if ("pseudo_trajectory" %in% plan$metrics) .preparePseudoTrajectory(clip) else NULL
  list(clip = clip, ranges = ranges, trajectory = trajectory)
}

#' Dispatch one dashboard frame to its drawer.
#' @keywords internal
#' @noRd
.drawDashboard <- function(dashboard, fd, clip, current_time, depth.window, activity.window, theme,
                           caption, candidates, plan = NULL, context = NULL) {
  theme <- .completeOverlayTheme(theme)
  fd <- as.list(fd)
  if (dashboard == "validation")
    return(.drawDashboardValidation(fd, clip, current_time, depth.window, activity.window, theme, caption,
                                    model = if (is.null(plan)) "shark" else plan$orientation.model))
  if (dashboard == "validation-compare")
    return(.drawDashboardCompare(fd, clip, current_time, depth.window, theme, candidates, caption,
                                 model = if (is.null(plan)) "shark" else plan$orientation.model))
  if (is.null(plan))
    plan <- .resolveDashboardPlan(dashboard, depth.window = depth.window, activity.window = activity.window)
  if (is.null(context)) context <- .prepareOverlayContext(clip, plan)
  .drawPresentationDashboard(fd, context$clip, current_time, theme, plan, context)
}

#' Draw a presentation dashboard from its resolved module plan.
#' @keywords internal
#' @noRd
.drawPresentationDashboard <- function(fd, clip, current_time, theme, plan, context) {
  metrics <- plan$metrics
  if (plan$dashboard == "ribbon") {
    n <- length(metrics)
    graphics::layout(rbind(seq_len(n), rep(n + 1L, n)), heights = c(1, 0.22))
    op <- graphics::par(bg = theme$canvas, oma = c(0, 0, 0, 0))
    on.exit(graphics::par(op), add = TRUE)
    for (metric in metrics) .drawMetricModule(metric, fd, clip, current_time, theme, plan, context)
    graphics::par(mar = c(0, 0, 0, 0)); .drawTimestamp(fd$datetime, theme)
    return(invisible())
  }

  groups <- .presentationGroups(metrics, plan$dashboard)
  mats <- list(); heights <- numeric(); next_id <- 1L
  for (i in seq_along(groups$metrics)) {
    group <- groups$metrics[[i]]
    ids <- next_id + seq_along(group) - 1L
    next_id <- next_id + length(group)
    mats[[i]] <- rep(ids, each = 6L / length(ids))
    heights[i] <- if (groups$tiles[i]) 0.72 else max(vapply(group, .moduleHeight, numeric(1)))
  }
  mats[[length(mats) + 1L]] <- rep(next_id, 6L)
  heights <- c(heights, 0.22)
  lower_gap <- isTRUE(theme$overlay) && identical(plan$dashboard, "general")
  if (lower_gap) {
    mats[[length(mats) + 1L]] <- rep(next_id + 1L, 6L)
    heights <- c(heights, 0.34)
  }
  graphics::layout(do.call(rbind, mats), heights = heights)
  op <- graphics::par(bg = theme$canvas, oma = c(0, 0, 0, 0))
  on.exit(graphics::par(op), add = TRUE)
  for (i in seq_along(groups$metrics)) {
    for (metric in groups$metrics[[i]]) {
      if (groups$tiles[i]) .drawMetricTile(metric, fd, theme) else
        .drawMetricModule(metric, fd, clip, current_time, theme, plan, context)
    }
  }
  graphics::par(mar = c(0, 3.1, 0, 0.8)); .drawTimestamp(fd$datetime, theme)
  if (lower_gap) {
    graphics::par(mar = c(0, 0, 0, 0))
    .drawTransparentSpacer()
  }
  invisible()
}

#' Arrange modules into rows; dial trios share a row and focus support modules become tiles.
#' @keywords internal
#' @noRd
.presentationGroups <- function(metrics, dashboard) {
  if (dashboard == "focus") {
    rest <- if (length(metrics) > 1L) split(metrics[-1L], ceiling(seq_along(metrics[-1L]) / 3)) else list()
    return(list(metrics = c(list(metrics[1L]), unname(rest)),
                tiles = c(FALSE, rep(TRUE, length(rest)))))
  }
  groups <- list(); i <- 1L
  while (i <= length(metrics)) {
    if (metrics[i] %in% c("heading", "pitch", "roll")) {
      j <- i
      while (j < length(metrics) && metrics[j + 1L] %in% c("heading", "pitch", "roll")) j <- j + 1L
      groups[[length(groups) + 1L]] <- metrics[i:j]
      i <- j + 1L
    } else {
      groups[[length(groups) + 1L]] <- metrics[i]
      i <- i + 1L
    }
  }
  list(metrics = groups, tiles = rep(FALSE, length(groups)))
}

#' Relative row height for a full module.
#' @keywords internal
#' @noRd
.moduleHeight <- function(metric) {
  type <- .overlayMetricRegistry()[[metric]]$type
  switch(type, trajectory = 3.1, orientation = 2.1, dial = 2.1, 1.15)
}

#' Draw one registered module.
#' @keywords internal
#' @noRd
.drawMetricModule <- function(metric, fd, clip, current_time, theme, plan, context) {
  spec <- .overlayMetricRegistry()[[metric]]
  if (spec$type %in% c("orientation", "dial")) graphics::par(mar = c(0.5, 3.1, 1.3, 0.8)) else
    if (spec$type == "trajectory") graphics::par(mar = c(0.5, 0.5, 1.3, 0.5)) else
    graphics::par(mar = c(1.2, 3.1, 1.9, 0.8))
  if (spec$type == "orientation")
    return(.drawAttitudeModel3D(.scalarValue(fd, "pitch"), .scalarValue(fd, "roll"), theme,
                                heading = .scalarValue(fd, "heading"), show.heading = TRUE,
                                label = spec$label, model = plan$orientation.model))
  if (spec$type == "dial") return(.drawDial(metric, .scalarValue(fd, metric), theme))
  if (spec$type == "trajectory")
    return(.drawPseudoTrajectory(context$trajectory, current_time, plan$windows[[metric]],
                                 .scalarValue(fd, "heading"), theme))
  if (spec$type == "vedba")
    return(.drawVedbaPanel(clip, current_time, plan$windows[[metric]], .scalarValue(fd, metric),
                           theme, context$ranges[[metric]]))
  if (spec$type == "vertical_velocity")
    return(.drawVerticalVelocityPanel(clip, current_time, plan$windows[[metric]], .scalarValue(fd, metric),
                                      theme, context$ranges[[metric]]))
  .drawSeriesPanel(clip, current_time, plan$windows[[metric]], metric, spec$label, spec$unit,
                   .scalarValue(fd, metric), theme, invert = spec$type == "depth", fill = spec$colour,
                   fixed.range = context$ranges[[metric]])
}

#' Draw a compact current-value tile for a focus-dashboard support metric.
#' @keywords internal
#' @noRd
.drawMetricTile <- function(metric, fd, theme) {
  spec <- .overlayMetricRegistry()[[metric]]
  graphics::par(mar = c(0.4, 0.4, 0.4, 0.4))
  plot(0, 0, type = "n", ann = FALSE, axes = FALSE, xlim = c(0, 1), ylim = c(0, 1))
  .drawPanelBackground(theme)
  if (metric == "orientation") {
    value <- sprintf("H %s\u00b0  P %s\u00b0  R %s\u00b0", .formatOverlayValue(.scalarValue(fd, "heading"), 0),
                     .formatOverlayValue(.scalarValue(fd, "pitch"), 0),
                     .formatOverlayValue(.scalarValue(fd, "roll"), 0))
  } else if (metric == "pseudo_trajectory") {
    value <- "3-D local track"
  } else {
    value <- paste(.formatOverlayValue(.scalarValue(fd, metric), 2), spec$unit)
  }
  graphics::text(0.06, 0.70, spec$label, adj = 0, col = theme$muted, cex = 0.75, font = 2)
  graphics::text(0.06, 0.35, value, adj = 0, col = theme$value, cex = 1.05, font = 2)
  invisible()
}

#' Axis-validation dashboard: a large attitude indicator (the handedness cue) + the roll-rate gyro trace
#' + depth + guidance caption. The heading compass was dropped: the lite-orientation heading (uncalibrated
#' magnetometer, and a wrong mapping is exactly what is under review) is unreliable AND heading is not the
#' handedness cue - the ROLL is.
#' @keywords internal
#' @noRd
.drawDashboardValidation <- function(fd, clip, current_time, depth.window, activity.window, theme, caption,
                                     model = "shark") {
  graphics::layout(matrix(c(1, 2, 3, 4), ncol = 1), heights = c(2.8, 1.3, 1.2, 0.5))
  op <- graphics::par(bg = theme$canvas, mar = c(1, 1, 2.2, 1), oma = c(0, 0, 0.3, 0))
  on.exit(graphics::par(op), add = TRUE)
  .drawAttitudeModel3D(fd$pitch, fd$roll, theme, model = model)
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
.drawDashboardCompare <- function(fd, clip, current_time, depth.window, theme, candidates, caption,
                                  model = "shark") {
  N <- nrow(candidates)
  pal <- .candidatePalette(N)
  # a prominent guidance HEADER spanning the top, then the N attitude indicators, depth, and timestamp
  m <- rbind(rep(1L, N), 1L + seq_len(N), rep(N + 2L, N), rep(N + 3L, N))
  graphics::layout(m, heights = c(0.55, 2.7, 1.0, 0.32))
  op <- graphics::par(bg = theme$canvas, oma = c(0, 0, 0.3, 0)); on.exit(graphics::par(op), add = TRUE)
  graphics::par(mar = c(0, 1, 0, 1)); .drawHeader(caption, theme)
  graphics::par(mar = c(1, 1, 2.4, 1))
  for (k in seq_len(N))
    .drawAttitudeModel3D(fd[[candidates$pitch[k]]], fd[[candidates$roll[k]]], theme,
                         label = candidates$label[k], body.col = pal[k], model = model)
  graphics::par(mar = c(1.6, 3.2, 2.0, 1))
  .drawSeriesPanel(clip, current_time, depth.window, "depth", "Depth", "m", fd$depth, theme, invert = TRUE, fill = "#1f4e8c")
  graphics::par(mar = c(0, 0, 0, 0)); .drawTimestamp(fd$datetime, theme)
}

#' A prominent, auto-sized guidance header (the "what to look for" instruction) spanning the panel top.
#' @keywords internal
#' @noRd
.drawHeader <- function(caption, theme) {
  plot(0, 0, type = "n", ann = FALSE, axes = FALSE, xlim = c(0, 1), ylim = c(0, 1))
  .drawPanelBackground(theme)
  if (is.null(caption) || !nzchar(caption)) return(invisible())
  w <- graphics::strwidth(caption, units = "inches", cex = 1)
  cex <- if (w > 0) min(1.3, 0.97 * graphics::par("pin")[1] / w) else 1.1
  graphics::text(0.5, 0.5, caption, col = theme$text, font = 2, cex = cex, xpd = NA)
}


################################################################################
# Panel drawers (internal) #####################################################
################################################################################

#' Configuration registry for the schematic orientation models.
#'
#' Each entry defines the longitudinal body envelope, appendage polygons and optional surface-detail
#' lines in the common animal frame (x forward, y right, z down). The shark entry is deliberately kept
#' unchanged; the other entries are recognizable, generic representatives rather than species models.
#' @keywords internal
#' @noRd
.orientationModelRegistry <- local({
  appendage <- function(...) list(...)
  models <- list(
    shark = list(
      stations = c(-1.00, -0.55, -0.10, 0.35, 0.70, 0.95, 1.05),
      width = c(0.015, 0.16, 0.24, 0.26, 0.20, 0.10, 0.012),
      height = c(0.015, 0.13, 0.19, 0.21, 0.16, 0.09, 0.012),
      appendages = appendage(
        cbind(c(0.42, 0, -0.20), c(0.02, 0, -0.20), c(0.22, 0, -0.62)),
        cbind(c(0.42, 0.22, 0.05), c(0.16, 0.22, 0.09), c(0.34, 0.56, 0.20)),
        cbind(c(0.42, -0.22, 0.05), c(0.16, -0.22, 0.09), c(0.34, -0.56, 0.20)),
        cbind(c(-0.90, 0, -0.03), c(-1.34, 0, -0.34), c(-1.10, 0, 0.01)),
        cbind(c(-0.90, 0, 0.03), c(-1.30, 0, 0.26), c(-1.10, 0, 0.01))
      )),
    cetacean = list(
      # Dolphin-like odontocete: distinct melon and beak, swept dorsal/pectoral fins, horizontal flukes.
      sections = 12L,
      stations = c(-1.09, -0.88, -0.55, -0.14, 0.30, 0.67, 0.91, 1.06, 1.29),
      width = c(0.025, 0.07, 0.15, 0.27, 0.29, 0.23, 0.15, 0.065, 0.012),
      height = c(0.025, 0.06, 0.12, 0.21, 0.22, 0.18, 0.12, 0.055, 0.010),
      appendages = appendage(
        cbind(c(0.08, 0, -0.21), c(-0.26, 0, -0.18), c(-0.12, 0, -0.48), c(-0.02, 0, -0.43)),
        cbind(c(0.22, 0.26, 0.03), c(-0.16, 0.25, 0.04), c(-0.36, 0.64, 0.07),
              c(-0.08, 0.57, 0.04), c(0.15, 0.36, 0.02)),
        cbind(c(0.22, -0.26, 0.03), c(-0.16, -0.25, 0.04), c(-0.36, -0.64, 0.07),
              c(-0.08, -0.57, 0.04), c(0.15, -0.36, 0.02)),
        cbind(c(-1.07, 0.00, 0.00), c(-1.20, 0.30, 0.00), c(-1.38, 0.72, 0.01),
              c(-1.51, 0.40, 0.02), c(-1.38, 0.08, 0.02)),
        cbind(c(-1.07, 0.00, 0.00), c(-1.20, -0.30, 0.00), c(-1.38, -0.72, 0.01),
              c(-1.51, -0.40, 0.02), c(-1.38, -0.08, 0.02))
      ),
      details = appendage(
        rbind(c(0.89, 0.96, 1.05), c(-0.10, 0, 0.10), c(-0.105, -0.115, -0.07))
      ),
      eyes = rbind(c(0.87, 0.87), c(0.14, -0.14), c(-0.08, -0.08))),
    turtle = list(
      # Hard-shelled sea turtle: domed oval carapace, short neck/head, long foreflippers, small hind pair.
      sections = 12L,
      stations = c(-0.79, -0.66, -0.43, -0.08, 0.28, 0.52, 0.67, 0.80, 0.98, 1.10, 1.25),
      width = c(0.018, 0.23, 0.40, 0.48, 0.44, 0.31, 0.11, 0.13, 0.18, 0.16, 0.025),
      height = c(0.012, 0.10, 0.19, 0.26, 0.25, 0.16, 0.07, 0.07, 0.10, 0.09, 0.015),
      appendages = appendage(
        cbind(c(0.52, 0.30, 0.02), c(0.20, 0.46, 0.04), c(-0.06, 0.77, 0.05),
              c(-0.33, 1.04, 0.06), c(-0.08, 0.99, 0.07), c(0.27, 0.66, 0.03)),
        cbind(c(0.52, -0.30, 0.02), c(0.20, -0.46, 0.04), c(-0.06, -0.77, 0.05),
              c(-0.33, -1.04, 0.06), c(-0.08, -0.99, 0.07), c(0.27, -0.66, 0.03)),
        cbind(c(-0.49, 0.35, 0.04), c(-0.72, 0.27, 0.05), c(-0.90, 0.62, 0.05),
              c(-0.62, 0.55, 0.05)),
        cbind(c(-0.49, -0.35, 0.04), c(-0.72, -0.27, 0.05), c(-0.90, -0.62, 0.05),
              c(-0.62, -0.55, 0.05)),
        cbind(c(-0.76, 0, 0.02), c(-0.98, 0, 0.03), c(-0.79, 0, 0.08))
      ),
      details = appendage(
        # Midline vertebral scutes and paired costal sutures on the dorsal carapace.
        rbind(c(-0.65, -0.42, -0.10, 0.22, 0.49, 0.62),
              c(0, 0, 0, 0, 0, 0), c(-0.12, -0.205, -0.265, -0.255, -0.18, -0.10)),
        rbind(c(-0.44, -0.35, -0.22, -0.10), c(0, 0.14, 0.29, 0.39),
              c(-0.205, -0.19, -0.14, -0.07)),
        rbind(c(-0.44, -0.35, -0.22, -0.10), c(0, -0.14, -0.29, -0.39),
              c(-0.205, -0.19, -0.14, -0.07)),
        rbind(c(-0.08, -0.03, 0.01, 0.04), c(0, 0.17, 0.34, 0.46),
              c(-0.265, -0.24, -0.14, -0.05)),
        rbind(c(-0.08, -0.03, 0.01, 0.04), c(0, -0.17, -0.34, -0.46),
              c(-0.265, -0.24, -0.14, -0.05)),
        rbind(c(0.27, 0.29, 0.29, 0.29), c(0, 0.16, 0.31, 0.42),
              c(-0.255, -0.23, -0.14, -0.06)),
        rbind(c(0.27, 0.29, 0.29, 0.29), c(0, -0.16, -0.31, -0.42),
              c(-0.255, -0.23, -0.14, -0.06))
      ),
      eyes = rbind(c(1.07, 1.07), c(0.14, -0.14), c(-0.09, -0.09))),
    fish = list(
      # Generic ray-finned fish, not one species: deeper trunk, rounded head/operculum, symmetric tail.
      sections = 12L,
      stations = c(-1.02, -0.86, -0.60, -0.24, 0.18, 0.53, 0.80, 1.03, 1.15),
      width = c(0.020, 0.075, 0.14, 0.23, 0.28, 0.25, 0.18, 0.10, 0.025),
      height = c(0.020, 0.07, 0.19, 0.33, 0.36, 0.33, 0.25, 0.13, 0.025),
      appendages = appendage(
        cbind(c(0.33, 0, -0.34), c(-0.19, 0, -0.32), c(-0.52, 0, -0.22),
              c(-0.31, 0, -0.51), c(0.10, 0, -0.57)),
        cbind(c(-0.24, 0, 0.32), c(-0.69, 0, 0.16), c(-0.54, 0, 0.38), c(-0.38, 0, 0.45)),
        cbind(c(0.55, 0.22, 0.07), c(0.23, 0.26, 0.08), c(0.12, 0.54, 0.11),
              c(0.43, 0.46, 0.06)),
        cbind(c(0.55, -0.22, 0.07), c(0.23, -0.26, 0.08), c(0.12, -0.54, 0.11),
              c(0.43, -0.46, 0.06)),
        cbind(c(-0.22, 0.20, 0.24), c(-0.42, 0.18, 0.26), c(-0.35, 0.39, 0.31)),
        cbind(c(-0.22, -0.20, 0.24), c(-0.42, -0.18, 0.26), c(-0.35, -0.39, 0.31)),
        cbind(c(-1.01, 0, -0.01), c(-1.24, 0, -0.42), c(-1.52, 0, -0.53),
              c(-1.39, 0, 0), c(-1.52, 0, 0.53), c(-1.24, 0, 0.42), c(-1.01, 0, 0.01))
      ),
      details = appendage(
        rbind(c(0.58, 0.64, 0.62, 0.54), c(0.21, 0.22, 0.20, 0.17),
              c(-0.18, -0.11, -0.04, 0.02)),
        rbind(c(0.58, 0.64, 0.62, 0.54), c(-0.21, -0.22, -0.20, -0.17),
              c(-0.18, -0.11, -0.04, 0.02))
      ),
      eyes = rbind(c(0.92, 0.92), c(0.14, -0.14), c(-0.10, -0.10))),
    manta = list(
      # Mobulid disc: wing-like pectorals form the wide diamond; two cephalic lobes and a whip tail.
      sections = 12L,
      stations = c(-1.45, -0.77, -0.49, -0.17, 0.23, 0.57, 0.79, 0.88),
      width = c(0.005, 0.018, 0.15, 0.29, 0.38, 0.33, 0.19, 0.075),
      height = c(0.004, 0.014, 0.035, 0.06, 0.08, 0.07, 0.045, 0.012),
      appendages = appendage(
        cbind(c(0.75, 0.19, 0.015), c(0.49, 0.40, 0.02), c(0.14, 0.86, 0.03),
              c(-0.12, 1.29, 0.04), c(-0.22, 1.34, 0.04), c(-0.41, 0.79, 0.02),
              c(-0.53, 0.29, 0.01)),
        cbind(c(0.75, -0.19, 0.015), c(0.49, -0.40, 0.02), c(0.14, -0.86, 0.03),
              c(-0.12, -1.29, 0.04), c(-0.22, -1.34, 0.04), c(-0.41, -0.79, 0.02),
              c(-0.53, -0.29, 0.01)),
        cbind(c(0.78, 0.12, -0.015), c(1.15, 0.11, -0.005), c(1.13, 0.24, 0.005),
              c(0.85, 0.25, 0.01)),
        cbind(c(0.78, -0.12, -0.015), c(1.15, -0.11, -0.005), c(1.13, -0.24, 0.005),
              c(0.85, -0.25, 0.01)),
        cbind(c(-0.52, 0, -0.025), c(-0.73, 0, -0.025), c(-0.61, 0, -0.10))
      ),
      details = appendage(
        rbind(c(0.78, 0.86), c(-0.11, 0.11), c(-0.048, -0.048))
      ),
      eyes = rbind(c(0.75, 0.75), c(0.27, -0.27), c(-0.03, -0.03)))
  )
  function() models
})

#' Build a low-poly 3-D body model from the orientation-model registry.
#'
#' Returned as a list of faces (each a 3 x k matrix of vertex coordinates), matching part labels
#' (`"body"` or `"fin"`) for styling, optional surface-detail polylines and eye positions.
#' @keywords internal
#' @noRd
.tagModel3D <- function(model = "shark") {
  spec <- .orientationModelRegistry()[[model]]
  if (is.null(spec)) .abort("Unknown orientation model: {.val {model}}.")
  stn <- spec$stations
  ry <- spec$width
  rz <- spec$height
  nt  <- if (is.null(spec$sections)) 8L else spec$sections
  ang <- utils::head(seq(0, 2 * pi, length.out = nt + 1L), nt)
  rings <- lapply(seq_along(stn), function(i) rbind(rep(stn[i], nt), ry[i] * cos(ang), rz[i] * sin(ang)))
  faces <- list(); part <- character(0)
  add <- function(m, p) { faces[[length(faces) + 1L]] <<- m; part[length(part) + 1L] <<- p }
  for (i in seq_len(length(stn) - 1L)) {                      # tube: quad faces between adjacent rings
    A <- rings[[i]]; B <- rings[[i + 1L]]
    for (j in seq_len(nt)) { k <- if (j == nt) 1L else j + 1L; add(cbind(A[, j], A[, k], B[, k], B[, j]), "body") }
  }
  for (fin in spec$appendages) add(fin, "fin")
  list(faces = faces, part = part, details = spec$details, eyes = spec$eyes)
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
#' and pitching with the animal. In presentation mode a small heading compass completes the orientation
#' complication. Validation mode deliberately omits it because its lightweight orientation is not
#' magnetometer-calibrated and roll handedness is the cue under review.
#'
#' Conventions (validated): body frame x forward, y right, z down; body -> world rotation is
#' \eqn{R = R_y(\text{pitch}) R_x(\text{roll})} (yaw omitted); the camera looks forward and ~24 deg down
#' with screen-right = world +y, so the animal's right maps to the viewer's right and the handedness
#' matches the underlying tilt convention (roll > 0 = right side down, pitch > 0 = nose up).
#' @keywords internal
#' @noRd
.drawAttitudeModel3D <- function(pitch, roll, theme, label = NULL, body.col = "#ff453a",
                                 heading = NA_real_, show.heading = FALSE, model = "shark") {
  plot(0, 0, type = "n", xlim = c(-1.35, 1.35), ylim = c(-1.55, 1.55), axes = FALSE, ann = FALSE, asp = 1)
  .drawPanelBackground(theme)
  graphics::symbols(0, 0, circles = 1.05, inches = FALSE, add = TRUE,
                    bg = grDevices::adjustcolor("black", 0.58), fg = theme$border)
  graphics::segments(-0.94, 0, 0.94, 0, col = grDevices::adjustcolor("#64d2ff", 0.35), lwd = 1)  # level reference

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

  m <- .tagModel3D(model)
  polys <- lapply(m$faces, function(f) {
    w <- R %*% f
    list(xs = as.numeric(rr %*% w), ys = as.numeric(uu %*% w),
         depth = mean(as.numeric(dd %*% w)), light = 0.40 + 0.60 * abs(sum(.faceNormal(w) * Lto)))
  })
  depth <- vapply(polys, function(p) p$depth, numeric(1))
  dn    <- if (diff(range(depth)) > 0) (depth - min(depth)) / diff(range(depth)) else depth * 0    # 0 near, 1 far
  scl   <- 0.84
  for (k in order(depth, decreasing = TRUE)) {                # painter's: far -> near
    p <- polys[[k]]
    col  <- .shadeColor(body.col, p$light * (0.72 + 0.28 * (1 - dn[k])))   # + gentle far-dimming for depth
    bord <- if (identical(m$part[k], "fin")) grDevices::adjustcolor("grey10", 0.85) else grDevices::adjustcolor(col, 0.5)
    graphics::polygon(p$xs * scl, p$ys * scl, col = col, border = bord, lwd = 0.6)
  }
  # The simple painter has no z-buffer. Do not paint dorsal scutes/eyes across the underside when an
  # animal rolls over; the underlying body and fin faces remain visible from either side.
  dorsal_visible <- as.numeric(dd %*% R %*% c(0, 0, -1)) < 0
  if (dorsal_visible) {
    if (length(m$details)) for (line in m$details) {
      w <- R %*% line
      graphics::lines(as.numeric(rr %*% w) * scl, as.numeric(uu %*% w) * scl,
                      col = .shadeColor(body.col, 0.42), lwd = 1.1)
    }
    if (!is.null(m$eyes)) {
      w <- R %*% m$eyes
      graphics::points(as.numeric(rr %*% w) * scl, as.numeric(uu %*% w) * scl,
                       pch = 21, cex = 0.62, bg = "#11161a", col = .shadeColor(body.col, 0.48), lwd = 0.7)
    }
  }

  if (isTRUE(show.heading)) {
    cx <- 0.91; cy <- 0.93; radius <- 0.25
    graphics::symbols(cx, cy, circles = radius, inches = FALSE, add = TRUE,
                      bg = grDevices::adjustcolor("black", 0.72), fg = theme$muted)
    graphics::text(cx, cy + radius + 0.08, "N", cex = 0.55, font = 2, col = theme$text)
    if (is.finite(heading)) {
      rad <- (90 - heading) * pi / 180
      graphics::arrows(cx, cy, cx + cos(rad) * radius * 0.72, cy + sin(rad) * radius * 0.72,
                       length = 0.08, lwd = 2, col = "#ff453a")
    }
  }

  if (!is.null(label)) graphics::text(0, 1.38, label, cex = 1.35, font = 2, col = theme$text, xpd = NA)
  values <- if (isTRUE(show.heading))
    sprintf("H %s\u00b0   P %s\u00b0   R %s\u00b0",
            .formatOverlayValue(heading, 0), .formatOverlayValue(pitch, 0), .formatOverlayValue(roll, 0)) else
    sprintf("Roll %s\u00b0  Pitch %s\u00b0",
            .formatOverlayValue(roll, 0, signed = TRUE), .formatOverlayValue(pitch, 0, signed = TRUE))
  graphics::text(0, -1.38, values, cex = 1.05, col = theme$value, xpd = NA, font = 2)
  invisible()
}

#' Circular orientation gauge for heading / pitch / roll.
#' @keywords internal
#' @noRd
.drawDial <- function(metric, value, theme) {
  plot(0, 0, type = "n", xlim = c(-1.15, 1.15), ylim = c(-1.15, 1.15), axes = FALSE, ann = FALSE, asp = 1)
  .drawPanelBackground(theme)
  graphics::symbols(0, 0, circles = 1, inches = FALSE, add = TRUE,
                    bg = grDevices::adjustcolor("black", 0.62), fg = theme$muted)
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

#' Draw a consistently styled metric title and current value.
#'
#' The complete title is rendered in one explicit font and at one shared target size across scalar
#' modules. Long combinations are reduced only enough to fit the available plot width.
#' @keywords internal
#' @noRd
.drawMetricTitle <- function(label, value, theme, line = 0.65, cex = 1.02) {
  text <- paste0(label, "  \u00b7  ", value)
  available <- graphics::par("pin")[1] * 0.96
  width_at_target <- graphics::strwidth(text, units = "inches", cex = cex,
                                        family = "sans", font = 2)
  if (is.finite(width_at_target) && width_at_target > available)
    cex <- cex * available / width_at_target
  graphics::mtext(text, side = 3, line = line, at = mean(graphics::par("usr")[1:2]),
                  adj = 0.5, cex = cex, col = theme$text, family = "sans", font = 2)
  invisible()
}

#' Scrolling time-series panel with a clip-stable scale and the current value marked.
#' @keywords internal
#' @noRd
.drawSeriesPanel <- function(clip, current_time, win, col, label, unit, cur_val, theme, invert, fill,
                             fixed.range = NULL) {
  wd <- clip[abs(as.numeric(difftime(clip$datetime, current_time, units = "secs"))) <= win, ]
  y <- wd[[col]]
  rng <- if (is.null(fixed.range)) .overlayRange(y, if (col == "depth") "depth" else "series") else fixed.range
  ylim <- if (invert) c(rng[2], rng[1]) else rng
  xlim <- current_time + c(-win, win)
  plot(wd$datetime, y, type = "n", xlab = "", ylab = "", xaxs = "i", yaxs = "i",
       axes = FALSE, ylim = ylim, xlim = xlim)
  .drawPanelBackground(theme)
  usr <- graphics::par("usr"); base <- if (invert) usr[4] else usr[3]
  graphics::polygon(c(wd$datetime, rev(wd$datetime)), c(y, rep(base, nrow(wd))), col = grDevices::adjustcolor(fill, 0.35), border = NA)
  graphics::lines(wd$datetime, y, col = fill, lwd = 1.25)
  graphics::abline(v = current_time, col = grDevices::adjustcolor(theme$text, 0.35), lwd = 0.7)
  if (is.finite(cur_val)) graphics::points(current_time, cur_val, col = "#ff3b30", pch = 16, cex = 1.5)
  current_label <- if (is.finite(cur_val)) sprintf("%s %s", .formatOverlayValue(cur_val, 2), unit) else "NA"
  .drawMetricTitle(label, current_label, theme)
  graphics::axis(2, at = pretty(rng), las = 1, cex.axis = 0.65, col = "grey60", col.axis = theme$text)
  invisible()
}

#' VeDBA activity envelope: raw trace plus a one-second display-only smoother.
#' @keywords internal
#' @noRd
.drawVedbaPanel <- function(clip, current_time, win, cur_val, theme, fixed.range) {
  wd <- clip[abs(as.numeric(difftime(clip$datetime, current_time, units = "secs"))) <= win, ]
  smooth <- if (".overlay_vedba" %in% names(wd)) wd$.overlay_vedba else wd$vedba
  xlim <- current_time + c(-win, win)
  plot(wd$datetime, smooth, type = "n", xlab = "", ylab = "", xaxs = "i", yaxs = "i",
       axes = FALSE, ylim = fixed.range, xlim = xlim)
  .drawPanelBackground(theme)
  graphics::polygon(c(wd$datetime, rev(wd$datetime)), c(smooth, rep(0, nrow(wd))),
                    col = grDevices::adjustcolor("#ffd60a", 0.24), border = NA)
  graphics::lines(wd$datetime, wd$vedba, col = grDevices::adjustcolor(theme$muted, 0.48), lwd = 0.65)
  graphics::lines(wd$datetime, smooth, col = "#ffd60a", lwd = 1.8)
  graphics::abline(v = current_time, col = grDevices::adjustcolor(theme$text, 0.35), lwd = 0.7)
  if (is.finite(cur_val)) graphics::points(current_time, cur_val, col = "#ffffff", bg = "#ffd60a",
                                           pch = 21, cex = 1.3, lwd = 1.2)
  current_label <- if (is.finite(cur_val)) sprintf("%s g", .formatOverlayValue(cur_val, 3)) else "NA"
  .drawMetricTitle("VeDBA activity", current_label, theme)
  graphics::axis(2, at = pretty(fixed.range), las = 1, cex.axis = 0.65,
                 col = theme$muted, col.axis = theme$text)
  invisible()
}

#' Signed vertical velocity: ascent above, descent below, with a stable symmetric scale and zero line.
#' @keywords internal
#' @noRd
.drawVerticalVelocityPanel <- function(clip, current_time, win, cur_val, theme, fixed.range) {
  wd <- clip[abs(as.numeric(difftime(clip$datetime, current_time, units = "secs"))) <= win, ]
  displayed <- -wd$vertical_velocity
  ylim <- c(-fixed.range[2], -fixed.range[1])
  xlim <- current_time + c(-win, win)
  plot(wd$datetime, displayed, type = "n", xlab = "", ylab = "", xaxs = "i", yaxs = "i",
       axes = FALSE, ylim = ylim, xlim = xlim)
  .drawPanelBackground(theme)
  ascent <- pmax(displayed, 0)
  descent <- pmin(displayed, 0)
  graphics::polygon(c(wd$datetime, rev(wd$datetime)), c(ascent, rep(0, nrow(wd))),
                    col = grDevices::adjustcolor("#64d2ff", 0.38), border = NA)
  graphics::polygon(c(wd$datetime, rev(wd$datetime)), c(descent, rep(0, nrow(wd))),
                    col = grDevices::adjustcolor("#ff9f0a", 0.42), border = NA)
  ascent_line <- displayed; ascent_line[ascent_line < 0] <- NA_real_
  descent_line <- displayed; descent_line[descent_line > 0] <- NA_real_
  graphics::lines(wd$datetime, ascent_line, col = "#64d2ff", lwd = 1.5)
  graphics::lines(wd$datetime, descent_line, col = "#ff9f0a", lwd = 1.5)
  graphics::abline(h = 0, col = theme$text, lwd = 1.0)
  graphics::abline(v = current_time, col = grDevices::adjustcolor(theme$text, 0.35), lwd = 0.7)
  if (is.finite(cur_val)) graphics::points(current_time, -cur_val, col = "#ffffff",
                                           bg = if (cur_val >= 0) "#ff9f0a" else "#64d2ff",
                                           pch = 21, cex = 1.3, lwd = 1.2)
  direction <- if (!is.finite(cur_val) || abs(cur_val) < 1e-9) "" else if (cur_val > 0) " \u00b7 descent" else " \u00b7 ascent"
  current_label <- if (is.finite(cur_val))
    sprintf("%s m/s%s", .formatOverlayValue(cur_val, 2, signed = TRUE), direction) else "NA"
  .drawMetricTitle("Vertical velocity", current_label, theme)
  ticks <- pretty(ylim)
  graphics::axis(2, at = ticks, labels = .formatOverlayValue(-ticks, 1, signed = TRUE), las = 1,
                 cex.axis = 0.62, col = theme$muted, col.axis = theme$text)
  graphics::text(graphics::par("usr")[1], ylim[2] * 0.82, "ASCENT", adj = 0,
                 col = "#64d2ff", font = 2, cex = 0.62)
  graphics::text(graphics::par("usr")[1], ylim[1] * 0.82, "DESCENT", adj = 0,
                 col = "#ff9f0a", font = 2, cex = 0.62)
  invisible()
}

#' Project the whole pseudo-track once into a fixed local 3-D display coordinate system.
#' @keywords internal
#' @noRd
.preparePseudoTrajectory <- function(clip) {
  ok <- is.finite(clip$pseudo_lon) & is.finite(clip$pseudo_lat) & is.finite(clip$pseudo_depth)
  if (sum(ok) < 2L) return(list(available = FALSE))
  lon0 <- stats::median(clip$pseudo_lon[ok]); lat0 <- stats::median(clip$pseudo_lat[ok])
  xy <- .projLocal(clip$pseudo_lon, clip$pseudo_lat, lon0, lat0, 6371000)
  east <- xy$e; north <- xy$n; depth <- clip$pseudo_depth
  horizontal_span <- max(diff(range(east[ok])), diff(range(north[ok])), 1)
  depth_span <- diff(range(depth[ok]))
  vertical_exaggeration <- if (is.finite(depth_span) && depth_span > 0)
    min(8, max(1, 0.38 * horizontal_span / depth_span)) else 1

  # Fixed oblique camera: east runs mostly right, north recedes up-right, depth points down.
  screen_x <- 0.866 * east - 0.5 * north
  surface_y <- 0.25 * east + 0.433 * north
  screen_y <- surface_y - vertical_exaggeration * depth
  ok_screen <- ok & is.finite(screen_x) & is.finite(screen_y)
  time_step <- diff(as.numeric(clip$datetime))
  typical_step <- stats::median(time_step[is.finite(time_step) & time_step > 0], na.rm = TRUE)
  if (!is.finite(typical_step)) typical_step <- 1
  discontinuity <- c(TRUE, !head(ok_screen, -1L) | !tail(ok_screen, -1L) |
                       !is.finite(time_step) | time_step > max(2, 5 * typical_step))
  segment <- cumsum(discontinuity)
  xlim <- .paddedRange(c(screen_x[ok_screen], 0), 0.10)
  ylim <- .paddedRange(c(screen_y[ok_screen], surface_y[ok_screen], 0), 0.10)
  scale_m <- .niceScale(horizontal_span / 4)
  list(available = TRUE, datetime = clip$datetime, east = east, north = north, depth = depth,
       x = screen_x, y = screen_y, surface_y = surface_y, ok = ok_screen, xlim = xlim, ylim = ylim,
       segment = segment, vertical_exaggeration = vertical_exaggeration, scale_m = scale_m)
}

#' Draw a stable pseudo-trajectory with past/future context, current point and a surface tie-line.
#' @keywords internal
#' @noRd
.drawPseudoTrajectory <- function(track, current_time, win, heading, theme) {
  if (is.null(track) || !isTRUE(track$available)) {
    plot(0, 0, type = "n", axes = FALSE, ann = FALSE, xlim = c(0, 1), ylim = c(0, 1))
    .drawPanelBackground(theme)
    graphics::text(0.5, 0.5, "Pseudo-trajectory unavailable", col = theme$muted, cex = 0.9)
    return(invisible())
  }
  plot(0, 0, type = "n", axes = FALSE, ann = FALSE, xlim = track$xlim, ylim = track$ylim,
       xaxs = "i", yaxs = "i", asp = 1)
  .drawPanelBackground(theme)
  delta <- as.numeric(difftime(track$datetime, current_time, units = "secs"))
  past <- which(track$ok & delta >= -win & delta <= 0)
  future <- which(track$ok & delta > 0 & delta <= win)
  visible <- c(past, future)
  .drawSegmentedTrack(track$x, track$surface_y, visible, track$segment,
                      col = grDevices::adjustcolor("#64d2ff", 0.28), lwd = 0.8, lty = 2)
  .drawSegmentedTrack(track$x, track$y, future, track$segment, col = theme$muted, lwd = 1.2, lty = 3)
  .drawSegmentedTrack(track$x, track$y, past, track$segment, col = "#30d158", lwd = 2.6, lty = 1)
  current <- which.min(abs(delta))
  if (length(current) && track$ok[current]) {
    graphics::segments(track$x[current], track$surface_y[current], track$x[current], track$y[current],
                       col = grDevices::adjustcolor("#64d2ff", 0.55), lty = 2, lwd = 1)
    graphics::points(track$x[current], track$surface_y[current], pch = 1, cex = 0.7, col = "#64d2ff")
    graphics::points(track$x[current], track$y[current], pch = 21, cex = 1.45,
                     col = "white", bg = "#ff453a", lwd = 1.2)
    if (is.finite(heading)) {
      arrow_m <- max(track$scale_m * 0.6, 1)
      de <- sin(heading * pi / 180) * arrow_m
      dn <- cos(heading * pi / 180) * arrow_m
      graphics::arrows(track$x[current], track$y[current],
                       track$x[current] + 0.866 * de - 0.5 * dn,
                       track$y[current] + 0.25 * de + 0.433 * dn,
                       col = "#ffd60a", lwd = 1.8, length = 0.09)
    }
  }
  usr <- graphics::par("usr")
  sx <- usr[1] + diff(usr[1:2]) * 0.07; sy <- usr[3] + diff(usr[3:4]) * 0.09
  dx <- 0.866 * track$scale_m; dy <- 0.25 * track$scale_m
  graphics::segments(sx, sy, sx + dx, sy + dy, col = theme$text, lwd = 2)
  graphics::text(sx + dx / 2, sy + dy + diff(usr[3:4]) * 0.035,
                 sprintf("%s m", format(track$scale_m, trim = TRUE, scientific = FALSE)),
                 col = theme$text, cex = 0.62)
  graphics::text(usr[2] - diff(usr[1:2]) * 0.03, usr[4] - diff(usr[3:4]) * 0.06,
                 sprintf("Depth \u00d7%.1f", track$vertical_exaggeration), adj = 1,
                 col = theme$muted, cex = 0.65)
  graphics::legend("bottomright", legend = c("past", "future", "surface"),
                   col = c("#30d158", theme$muted, grDevices::adjustcolor("#64d2ff", 0.55)),
                   lty = c(1, 3, 2), lwd = c(2.2, 1.2, 0.8), bty = "n",
                   text.col = theme$text, cex = 0.54, horiz = TRUE, inset = 0.015)
  graphics::title(main = "3-D pseudo-trajectory", line = 0.5, cex.main = 1.15,
                  col.main = theme$text, xpd = NA)
  invisible()
}

#' Draw only within contiguous valid pseudo-track segments, never across a sensor gap.
#' @keywords internal
#' @noRd
.drawSegmentedTrack <- function(x, y, index, segment, col, lwd, lty) {
  if (length(index) < 2L) return(invisible())
  groups <- split(index, segment[index])
  for (group in groups)
    if (length(group) > 1L) graphics::lines(x[group], y[group], col = col, lwd = lwd, lty = lty)
  invisible()
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
    .drawPanelBackground(theme)
    graphics::title(main = title, line = 1.5, cex.main = 1.15, col.main = theme$text, xpd = NA)
    graphics::text(0.5, 0.5, "unavailable", col = "grey55", cex = 0.95)
    return(invisible())
  }
  ys <- lapply(cols[present], function(c) wd[[c]])
  rng <- range(unlist(ys), na.rm = TRUE); if (!all(is.finite(rng))) rng <- c(-1, 1)
  plot(wd$datetime, ys[[1]], type = "n", xlab = "", ylab = "", xaxs = "i", axes = FALSE, ylim = rng)
  .drawPanelBackground(theme)
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
  .drawPanelBackground(theme)
  graphics::text(0.5, 0.52, format(datetime, "%Y-%m-%d %H:%M:%OS1", tz = "UTC"),
                 col = theme$text, cex = 1.0, font = 2, family = "mono")
}

#' Consume a deliberately empty layout row without painting over the source video.
#' @keywords internal
#' @noRd
.drawTransparentSpacer <- function() {
  plot(0, 0, type = "n", ann = FALSE, axes = FALSE, xlim = c(0, 1), ylim = c(0, 1))
  invisible()
}

#' Bottom strip: timestamp plus an optional caption line (validation dashboards).
#' @keywords internal
#' @noRd
.drawCaption <- function(datetime, caption, theme) {
  plot(0, 0, type = "n", ann = FALSE, axes = FALSE, xlim = c(0, 1), ylim = c(0, 1))
  .drawPanelBackground(theme)
  graphics::text(0.5, 0.72, format(datetime, "%Y-%m-%d %H:%M:%OS1", tz = "UTC"),
                 col = theme$text, cex = 1.0, font = 2, family = "mono")
  if (!is.null(caption) && nzchar(caption))
    graphics::text(0.5, 0.26, caption, col = "grey70", cex = 0.72, xpd = NA)
}


################################################################################
# Small helpers (internal) #####################################################
################################################################################

#' Complete a caller-supplied theme, preserving compatibility with older internal tests.
#' @keywords internal
#' @noRd
.completeOverlayTheme <- function(theme) {
  if (is.null(theme$bg)) theme$bg <- "#080b0e"
  if (is.null(theme$canvas)) theme$canvas <- theme$bg
  if (is.null(theme$module)) theme$module <- grDevices::adjustcolor(theme$bg, 0.96)
  if (is.null(theme$border)) theme$border <- grDevices::adjustcolor("white", 0.25)
  if (is.null(theme$text)) theme$text <- "grey92"
  if (is.null(theme$muted)) theme$muted <- "grey65"
  if (is.null(theme$value)) theme$value <- "#ff453a"
  if (is.null(theme$overlay)) theme$overlay <- FALSE
  theme
}

#' Theme for opaque beside panels or transparent on-video modules.
#' @keywords internal
#' @noRd
.overlayTheme <- function(composition = "beside", background.alpha = 0.72) {
  if (composition == "overlay") {
    .completeOverlayTheme(list(canvas = "transparent", bg = "transparent",
                               module = grDevices::adjustcolor("#071017", background.alpha),
                               border = grDevices::adjustcolor("white", min(0.42, background.alpha)),
                               text = "#f5f7fa", muted = "#aab4bd", value = "#ffffff",
                               overlay = TRUE))
  } else {
    .completeOverlayTheme(list(canvas = "#070b0f", bg = "#070b0f", module = "#101820",
                               border = "#293744", text = "#f5f7fa", muted = "#9aa6b2",
                               value = "#ffffff", overlay = FALSE))
  }
}

#' Paint the current panel's local backplate.
#' @keywords internal
#' @noRd
.drawPanelBackground <- function(theme) {
  usr <- graphics::par("usr")
  graphics::rect(usr[1], usr[3], usr[2], usr[4], col = theme$module,
                 border = theme$border, lwd = 0.7, xpd = FALSE)
  invisible()
}

#' A robust clip-stable y range for a dashboard metric.
#' @keywords internal
#' @noRd
.overlayRange <- function(x, metric = "series") {
  x <- x[is.finite(x)]
  if (!length(x)) return(if (metric == "vertical_velocity") c(-1, 1) else c(0, 1))
  if (metric == "vedba") return(c(0, max(0.05, unname(stats::quantile(x, 0.995, na.rm = TRUE)))))
  if (metric == "vertical_velocity") {
    lim <- max(0.05, unname(stats::quantile(abs(x), 0.995, na.rm = TRUE)))
    return(c(-lim, lim))
  }
  probs <- if (length(x) > 20L) c(0.005, 0.995) else c(0, 1)
  rng <- as.numeric(stats::quantile(x, probs, na.rm = TRUE, names = FALSE))
  if (metric != "depth" && rng[1] >= 0) rng[1] <- 0
  .paddedRange(rng, 0.06)
}

#' Pad a numeric range and safely expand constant inputs.
#' @keywords internal
#' @noRd
.paddedRange <- function(x, fraction = 0.08) {
  rng <- range(x[is.finite(x)], na.rm = TRUE)
  if (!all(is.finite(rng))) return(c(0, 1))
  span <- diff(rng)
  if (!is.finite(span) || span <= 0) span <- max(abs(rng), 1) * 0.1
  rng + c(-1, 1) * span * fraction
}

#' Select a readable metric scale length no larger than the requested target.
#' @keywords internal
#' @noRd
.niceScale <- function(target) {
  if (!is.finite(target) || target <= 0) return(1)
  power <- 10^floor(log10(target))
  choices <- c(1, 2, 5, 10) * power
  max(choices[choices <= target])
}

#' Return one finite scalar from a dashboard row, otherwise NA.
#' @keywords internal
#' @noRd
.scalarValue <- function(fd, name) {
  value <- fd[[name]]
  if (is.null(value) || !length(value) || !is.finite(value[1])) NA_real_ else as.numeric(value[1])
}

#' Format a numeric dashboard value without leaking NaN/Inf.
#' @keywords internal
#' @noRd
.formatOverlayValue <- function(x, digits = 1, signed = FALSE) {
  ans <- rep("NA", length(x))
  ok <- is.finite(x)
  fmt <- if (signed) paste0("%+.", digits, "f") else paste0("%.", digits, "f")
  ans[ok] <- sprintf(fmt, x[ok])
  ans
}

#' Return an even positive dimension for codecs that require chroma-aligned frames.
#' @keywords internal
#' @noRd
.evenDimension <- function(x) {
  x <- max(2L, as.integer(round(x)))
  x - (x %% 2L)
}

#' Distinct body-glyph colours for up to several candidate mappings.
#' @keywords internal
#' @noRd
.candidatePalette <- function(n) {
  pal <- c("#ff453a", "#34c8c8", "#ffd60a", "#bf5af2", "#30d158")
  pal[((seq_len(max(n, 1)) - 1L) %% length(pal)) + 1L]
}

#' Convert an FFmpeg `HH:MM:SS.microseconds` progress value to seconds.
#' @keywords internal
#' @noRd
.ffmpegProgressSeconds <- function(x) {
  fields <- strsplit(x, ":", fixed = TRUE)[[1]]
  if (length(fields) != 3L) return(NA_real_)
  values <- suppressWarnings(as.numeric(fields))
  if (any(!is.finite(values))) return(NA_real_)
  values[1] * 3600 + values[2] * 60 + values[3]
}

#' Run FFmpeg while translating its machine-readable time output into a cli progress bar.
#'
#' A pipe keeps this synchronous and dependency-free while allowing FFmpeg to stream `-progress`
#' records. Diagnostics share the pipe and are ignored; the connection's close status remains the
#' authoritative process result used by the caller's existing software-encoder fallback.
#' @keywords internal
#' @noRd
.runFfmpegWithProgress <- function(ffmpeg, args, duration, lvl) {
  progress_args <- c("-progress", "pipe:1", "-nostats", args)
  command <- paste(shQuote(c(ffmpeg, progress_args)), collapse = " ")
  command <- paste(command, "2>&1")
  con <- base::pipe(command, open = "r")
  open <- TRUE
  pb <- if (lvl >= 1L) cli::cli_progress_bar(
    format = "{cli::pb_spin} Encoding video {cli::pb_bar} {cli::pb_percent}",
    total = 100L, .envir = parent.frame()) else NULL
  on.exit({
    if (open) suppressWarnings(close(con))
    .log_progress_done(pb)
  }, add = TRUE)

  repeat {
    line <- readLines(con, n = 1L, warn = FALSE)
    if (!length(line)) break
    if (startsWith(line, "out_time=")) {
      seconds <- .ffmpegProgressSeconds(sub("^out_time=", "", line))
      if (!is.null(pb) && is.finite(seconds) && is.finite(duration) && duration > 0)
        cli::cli_progress_update(id = pb, set = min(99L, as.integer(floor(100 * seconds / duration))))
    } else if (identical(line, "progress=end") && !is.null(pb)) {
      cli::cli_progress_update(id = pb, set = 100L)
    }
  }
  status <- suppressWarnings(close(con))
  open <- FALSE
  .log_progress_done(pb)
  pb <- NULL

  # `pipe()` returns the wait status shifted by eight bits on Unix, but a direct exit code on some
  # other platforms. Normalise only the unambiguous shifted form.
  status <- as.integer(status)
  if (length(status) != 1L || !is.finite(status)) return(1L)
  if (is.finite(status) && status > 255L && status %% 256L == 0L) status <- status %/% 256L
  status
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
