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
#'   `datetime` is required, with unique timestamps. Input is sorted on a copy. Additional required
#'   columns depend on `dashboard` and `metrics`; see
#'   Details. Processed output from [processTagData()] is recommended.
#' @param output Path of the video file to create. The parent directory must already exist. An existing
#'   file is preserved unless `overwrite = TRUE`. The source video cannot be overwritten. An `.mp4`
#'   output is recommended for the supported codecs.
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
#' @param orientation.model Species used by three-dimensional orientation modules:
#'   `"blue_shark"` (default), `"whale_shark"`, `"mako"`, `"hammerhead"`, `"manta"`,
#'   `"devil_ray"`, `"turtle"`, `"dolphin"`, `"whale"`, `"marlin"` or `"tuna"`.
#'   Each name selects its approved geometry, appearance and motion profile.
#'   The choice does not alter sensor orientation values or coordinate conventions.
#'   Presentation dashboards ignore it when `orientation = "dials"`; fixed validation
#'   dashboards use the selected model. A validated [overlayModel()] or
#'   [overlaySpeciesModel()] object can also be supplied. Use [overlaySpeciesCatalog()]
#'   to list species and motion profiles. Retired generic names `"shark"`, `"cetacean"`
#'   and `"fish"` are not aliases; select an explicit species instead.
#' @param video.start One finite `POSIXct` value giving the sensor-clock time represented by the first
#'   frame of `video`. `NULL` (default) attempts to read a UTC timestamp in `YYYYMMDD-HHMMSS` or
#'   `YYMMDD-HHMMSS` form from the file name. A value returned by [getVideoMetadata()] is recommended.
#' @param start,end Optional finite `POSIXct` bounds, expressed on the same clock as `video.start` and
#'   `data$datetime`. `NULL` (default) uses the full temporal overlap between the sensor record and video.
#'   These arguments can restrict the render to a validation or behavioural interval.
#' @param side Position of the dashboard: `"right"` (default) or `"left"`. In beside mode it determines
#'   panel order; in overlay mode it anchors the dashboard to that edge. It is ignored by the full-width
#'   `"ribbon"` layout.
#' @param overlay.fps Positive dashboard update rate in frames per second. `NULL` (default) uses 5 fps
#'   for rigid models and the source video rate when motion is enabled. The source frame rate is retained
#'   in the output. Lower values reduce rendering time; explicit rates are checked for motion aliasing.
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
#' @param motion `NULL` (default) for a rigid model, or [overlayMotionControl()] to enable schematic
#'   animation. Requires a full orientation model module in a presentation dashboard; animation is
#'   unavailable in validation dashboards or when `orientation = "dials"`.
#' @param max.sensor.gap Largest sensor interval, in seconds, across which orientation and motion may
#'   be interpolated (default 1). Missing estimates also break motion segments.
#' @param overwrite Whether to overwrite an existing output (default `FALSE`). The output path must
#'   always differ from the source video.
#' @param animate Optional logical shortcut. `TRUE` enables default schematic motion when `motion` is
#'   `NULL`, or uses the supplied control. `FALSE` disables motion. `NULL` (default) leaves `motion` in
#'   control.
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
#' `start`/`end` bounds. The function stops if that intersection is empty. Scalar values use the most
#' recent sensor observation at or before the frame time. Pitch is linearly interpolated, while roll
#' and heading follow their shortest angular arc. Missing values and intervals longer than
#' `max.sensor.gap` are not bridged. The timestamp shows the actual dashboard frame time in UTC.
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
#' ## Schematic animation and custom geometry
#'
#' `animate = TRUE` enables default motion; `motion = overlayMotionControl(...)` allows explicit
#' control of the backend, amplitude reference, maximum bend and phase. Shark/fish models bend
#' laterally, cetaceans vertically, and turtle/manta models use paired flipper/wing bending.
#' `method = "auto"` selects one backend for the full deployment, preferring positive peaks estimates
#' and otherwise wavelet. Motion requires paired numeric `tbf_hz_peaks`/`tbf_amplitude_peaks` or
#' `tbf_hz_wavelet`/`tbf_amplitude_wavelet` columns from [calculateTailBeats()].
#'
#' Frequency is integrated over the full supplied sensor record, so restricting `start`/`end` does
#' not change phase in overlapping clips. Missing estimates and long sensor gaps break a motion
#' segment; the next valid segment restarts at `phase.offset`. An explicit logical
#' `tbf_swimming = FALSE` shows a neutral model labelled as gliding. Unavailable estimates show
#' a neutral model labelled as unavailable. The default amplitude reference is the 95th percentile
#' of valid positive amplitudes over the full supplied deployment. Amplitude scales the configured
#' maximum visual bend with saturation; it does not measure tail displacement or joint angle.
#' Frequency estimates do not recover actual beat phase. Keep full deployment data and use
#' `start`/`end` for reproducible comparisons between overlapping clips.
#'
#' The original rear camera alignment is retained, without an oblique azimuth. A display-only
#' pitch-zero rotation compensates the camera elevation before projection so descents and ascents
#' show symmetric signed pitch cues. Physical body rotations use x forward, y right, z down, with
#' positive pitch nose up and positive roll right side down. Heading remains a separate compass.
#' Animated insets display the interpolated driving frequency as `TBF: X.XX Hz` below H/P/R with
#' matching font size and weight, or `TBF: NA` during unavailable/gliding intervals.
#'
#' [overlayModel()] validates custom geometry and optional per-vertex deformation weights before
#' rendering. Geometry is prepared once and display scale stays fixed across frames. Custom meshes
#' are limited to 5000 vertices and 2000 untextured triangles. Consistent winding and simple surfaces
#' are expected: the painter's algorithm has no z-buffer and cannot resolve arbitrary intersections.
#' Model-file import, textures and skeletal rigs are outside this interface.
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
#'   validation; [overlayModel()] for custom geometry and [overlayMotionControl()] for schematic motion.
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
#' # Animate the model using estimated tail-beat frequency and amplitude.
#' # tag must include the paired tbf_hz_* and tbf_amplitude_* columns.
#' renderOverlayVideo(video = video_metadata$file[1],
#'                    data = tag,
#'                    output = "./overlay/PIN_CAM_01_animated.mp4",
#'                    composition = "overlay",
#'                    animate = TRUE,
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
#'                    orientation.model = "dolphin",
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
                               orientation.model = "blue_shark",
                               video.start = NULL,
                               start = NULL,
                               end = NULL,
                               side = c("right", "left"),
                               overlay.fps = NULL,
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
                               verbose = "detailed",
                               motion = NULL,
                               max.sensor.gap = 1,
                               overwrite = FALSE,
                               animate = NULL) {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  dashboard <- match.arg(dashboard)
  composition <- match.arg(composition)
  orientation <- match.arg(orientation)
  side <- match.arg(side)
  codec <- match.arg(codec)
  if (!is.null(animate)) {
    .assert_flag(animate, "animate")
    if (!animate) motion <- NULL
    else if (is.null(motion)) motion <- overlayMotionControl()
  }
  .assert_number(overlay.fps, "overlay.fps", min = 0.1, null_ok = TRUE)
  .assert_number(max.sensor.gap, "max.sensor.gap", min = .Machine$double.eps)
  .assert_flag(overwrite, "overwrite")
  .assert_flag(keep.temp, "keep.temp")
  .assert_string(video, "video")
  .assert_writable_file(output, "output", null_ok = FALSE)
  if (file.exists(output) && !overwrite)
    .abort("Output already exists. Choose a separate {.arg output} path or explicitly set {.code overwrite = TRUE}.")
  if (identical(normalizePath(video, mustWork = FALSE), normalizePath(output, mustWork = FALSE)))
    .abort("The output path must differ from the source video.")
  if (!is.null(motion) && dashboard %in% c("validation", "validation-compare"))
    .abort("Schematic motion is disabled for axis-validation dashboards; use a presentation dashboard.")
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
  if (!nrow(data) || any(!is.finite(as.numeric(data$datetime))) ||
      any(diff(as.numeric(data$datetime)) <= 0))
    .abort("Sensor timestamps must be finite and unique; resolve duplicate timestamps before rendering.")
  has.model <- dashboard %in% c("validation", "validation-compare") || "orientation" %in% plan$metrics
  if (!is.null(motion) && (!has.model || (dashboard == "focus" && plan$metrics[1L] != "orientation")))
    .abort("Schematic motion requires a full orientation model module in the dashboard.")
  max.angle <- if (inherits(motion, "nautilus_overlay_motion") && is.list(motion)) motion$max.angle else 0
  prepared.model <- .prepareOverlayModel(orientation.model, max.angle)
  motion <- .resolveOverlayMotion(motion, data, prepared.model)

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

  if (is.null(overlay.fps)) overlay.fps <- if (is.null(motion)) 5 else v_fps

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

  .log_header(lvl, "renderOverlayVideo", "Rendering the sensor dashboard",
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

  # Prepare the dashboard clock and interpolated orientation/motion before drawing.
  n_frames <- max(1L, as.integer(ceiling(clip_dur * overlay.fps)))
  frame_times <- clip_lo + (seq_len(n_frames) - 1) / overlay.fps
  # Full deployment integration preserves phase when start/end or plot windows change.
  pitch.cols <- unique(c("pitch", candidates$pitch))
  roll.cols <- unique(c("roll", candidates$roll))
  frame.state <- .prepareOverlayFrameState(data, frame_times, motion, max.sensor.gap,
                                           pitch.cols, roll.cols)
  .checkOverlayMotionRate(frame.state, overlay.fps, v_fps)

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
          fd <- .overlayFrameData(frame.state, i, data)
          .drawPreparedOverlayDashboard(fd, clip, frame_times[i], depth.window, activity.window,
                                        theme, caption, candidates, plan, context, prepared.model, motion)
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
        fd <- .overlayFrameData(frame.state, i, data)
        .drawPreparedOverlayDashboard(fd, clip, frame_times[i], depth.window, activity.window,
                                      theme, caption, candidates, plan, context, prepared.model, motion)
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
