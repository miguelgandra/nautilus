#######################################################################################################
# Derive movement and orientation metrics from raw sensor data #########################################
#######################################################################################################

#' Derive movement and orientation metrics from raw sensor data
#'
#' @description
#' An archival tag records acceleration along three axes, a magnetic field vector, pressure and, on some
#' tags, angular velocity. None of these is a quantity behaviour is described in. Between them and a
#' sentence such as "the animal descended at 0.8 m/s, rolled 30 degrees to its left and swam hardest in
#' the hour after dusk" sits a chain of corrections and derivations, each of which can be got wrong
#' quietly.
#'
#' This function performs that chain in one pass. It separates the animal's own acceleration from
#' gravity, estimates body orientation, corrects the depth record for sensor drift and the pitch and roll
#' for how the tag was mounted, calibrates the magnetometer for the iron the tag carries, and derives
#' the activity, posture and vertical-motion channels the rest of the package works from. It can then
#' downsample the result, since most analyses do not need hundreds of samples per second.
#'
#' Run it after [applyAxisMapping()], so the sensor axes are already in the animal's frame. Every
#' correction it applies is reported and recorded in the deployment's metadata, and any that had to
#' abstain says so rather than proceeding on an assumption.
#'
#' @param data A tag object, a list of them, a single table holding several deployments in a column
#'   named `ID`, or a character vector of `.rds` paths. Paths are read one deployment at a time, so a fleet too large for memory can be
#'   processed without ever holding it all. The output of [importTagData()] is strongly recommended, as
#'   it puts the data in the form every later step expects. The inertial axes must already be in the
#'   animal's body frame: run [applyAxisMapping()] first, or the orientation metrics will be wrong
#'   without appearing so. A warning is raised for any deployment whose axis mapping has not been
#'   applied.
#' @param downsample.to The rate in Hz to reduce the data to after the metrics are computed, or `NULL`
#'   to keep the original resolution (default `1`). The metrics are always derived at the full recorded
#'   rate, so downsampling costs nothing in accuracy for the channels it averages; but it does bound
#'   what remains measurable afterwards, since averaging into bins attenuates any event shorter than a
#'   bin. Keep the native rate if you intend to study individual jerk spikes or short dives.
#' @param orientation.algorithm How to estimate body orientation. `"tilt_compass"` (default) is a
#'   lightweight six-axis method using the accelerometer and magnetometer only. `"madgwick"` adds the
#'   gyroscope, giving a nine-axis fusion that rides through brief disturbances better, at a higher
#'   computational cost and requiring gyroscope data. Which is better is deployment-dependent rather
#'   than universal.
#' @param orientation A control object from [orientationControl()] governing the estimator's tuning and
#'   the mounting-offset corrections. Pass `orientationControl(...)` to change it.
#' @param calibration A control object from [calibrationControl()] governing whether the magnetometer is
#'   corrected for the tag's own iron, and whether a calibration already stored by
#'   [calibrateMagnetometer()] is preferred over one fitted here. Pass `calibrationControl(...)` to
#'   change it.
#' @param smoothing A control object from [smoothingControl()] governing the window lengths used to
#'   separate gravity from motion and to condition the derived channels. Pass `smoothingControl(...)` to
#'   change it.
#' @param depth.drift A control object from [depthDriftControl()] governing the depth zero-offset
#'   correction. Pass `depthDriftControl(method = "none")` to disable it.
#' @param paddle.calibration A data frame of paddle-wheel calibration values. Supplying it is what
#'   enables paddle-wheel speed estimation - there is no separate switch - so leave it `NULL` to skip
#'   speed entirely. It needs at least three columns: `year`, the year the calibration was performed;
#'   `package_id`, matching the tag's own identifier; and `slope`, the calibration slope.
#'   [imputePaddleCalibration()] builds a complete, gap-free table of this form from a set of measured
#'   calibrations, projecting slopes for tag-years that were never calibrated.
#' @param burst.quantiles Quantiles of instantaneous VeDBA used to flag burst swimming, or `NULL` to
#'   skip it (default `c(0.95, 0.99)`). Each is a threshold relative to the deployment itself: `0.95`
#'   always flags the most active 5 per cent of that record's samples, whatever the animal was doing.
#'   That makes the flags comparable in fraction of samples across deployments, but not in absolute
#'   activity level - and only before downsampling, which flags a bin whenever any sample in it was
#'   flagged.
#' @param plot Whether to render the correction diagnostics to the active graphics device (default
#'   `FALSE`). Intended for a single deployment; for a batch use `plot.file`, since `TRUE` on more than
#'   one deployment warns and floods the device.
#' @param plot.file Path to a single multi-page PDF holding the diagnostic report, or `NULL` (default)
#'   to render nothing. Each deployment gets a page showing the magnetometer calibration as the raw
#'   cloud collapses onto a sphere, the depth zero-offset drift, and the pitch and roll mounting-offset
#'   fits, so each correction can be checked by eye. The bundle is gathered while the raw data is in
#'   memory; the processing itself stays headless. Must end in `.pdf`.
#' @param return.data Whether to return the processed data in memory (default `TRUE`). When `FALSE`, the
#'   function instead returns the paths of the `.rds` files it wrote, which feed directly into the next
#'   step's `data` argument - so a large fleet can be processed without ever holding it all in memory.
#'   `return.data = FALSE` therefore requires an `output.dir`.
#' @param output.dir Directory in which to write one `<id>.rds` file per deployment. Providing a
#'   directory is what triggers saving; `NULL` (default) writes nothing. The directory must already
#'   exist.
#' @param output.suffix Optional suffix appended to each saved file name, before `.rds`, to tag a
#'   processing run or avoid overwriting an earlier one. Only used when `output.dir` is set.
#' @param compress Compression for the saved `.rds` files: `TRUE` (default, gzip), `FALSE`, or one of
#'   `"gzip"`, `"bzip2"` or `"xz"`. Only used when `output.dir` is set. See [base::saveRDS()].
#' @param data.table.threads How many threads data.table may use, or `NULL` (default) to leave its
#'   current setting alone. More threads speed up large operations at the cost of memory. Set it
#'   permanently with `data.table::setDTthreads()` and read the current value with
#'   `data.table::getDTthreads()`.
#' @param verbose How much detail to print: `0`/`"quiet"`, `1`/`"normal"` (header, per-deployment
#'   outcome and summary), or `2`/`"detailed"` (default), which adds the full per-step log.
#'
#' @details
#' ## What is computed
#'
#' Acceleration, once gravity has been separated from the animal's own motion:
#'
#' - **Total acceleration (g)** - the overall strength of the signal at each instant, combining the
#'   animal's movement and the constant pull of gravity.
#' - **Vectorial dynamic body acceleration, VeDBA (g)** - how vigorously the animal is moving once the
#'   steady pull of gravity is set aside. One of the most widely used indices of activity and energy
#'   use. Because it does not depend on which way the tag is facing, it stays reliable even on tags that
#'   can shift or rotate on the animal.
#' - **Overall dynamic body acceleration, ODBA (g)** - the sum of the absolute dynamic acceleration
#'   along the three axes. Retained for comparability with the wider literature, but it is
#'   orientation-dependent, so **VeDBA is the better choice for towed or loosely attached tags**.
#' - **Jerk (g/s)** - how suddenly the movement changes from one moment to the next. Steady swimming
#'   gives low jerk; a strike at prey, a startle or an abrupt turn give brief spikes, which is why jerk
#'   is often used as a clue to possible prey capture, particularly on fast tags mounted near the head
#'   (Ydesen et al. 2014). **It flags sudden movement, not confirmed feeding**: any quick motion raises
#'   it, including the animal breaking the surface or a knock to the tag, so check events against video
#'   where you can. Jerk only carries this detail on tags recording at roughly 30 Hz or more; on slower
#'   tags it is mostly noise, and a caution is shown. On downsampled output it becomes an average level
#'   of jerkiness rather than a record of individual spikes, so use the full-resolution data to see
#'   single events and read it alongside the original sampling rate (`sampling_hz_original`).
#' - **Burst swimming events** - a flag marking each animal's most energetic moments, where its overall
#'   activity is among the highest for that deployment. The cut-off is set relative to that deployment's
#'   own record, so at full resolution it always marks the same fraction of samples. Downsampling then
#'   flags a whole bin whenever any sample in it was flagged, so the fraction of returned rows carrying
#'   a flag grows with the tag's native rate: at the default `downsample.to = 1`, a 1 Hz and a 100 Hz
#'   tag do not return comparable fractions. Compare bursts at full resolution, or between tags
#'   recording at the same rate. Use it to ask when the animal was working hardest or sustaining high
#'   effort, as opposed to the brief, sudden movements jerk captures. Set the fraction flagged with
#'   `burst.quantiles`.
#'
#' Orientation, in degrees:
#'
#' - **Roll** - rotation about the animal's longitudinal axis.
#' - **Pitch** - rotation about its lateral axis.
#' - **Heading** - the compass direction the tag faces. See the two sections below, which cover what
#'   heading can and cannot be used for.
#' - **Turning angle** - the change in heading between consecutive samples, taken as the smallest
#'   angular difference and constrained to -180 to 180 degrees.
#'
#' Linear motion:
#'
#' - **Surge, sway and heave (g)** - the forward-and-back, side-to-side and up-and-down components of
#'   the animal's own acceleration, along its body axes.
#' - **Vertical velocity (m/s)** - the rate of change of depth, positive on descent. It is computed by
#'   central differences on a smoothed copy of the depth series, with the window set by
#'   `smoothing$depth`, because differentiating a raw pressure trace amplifies its quantisation noise.
#'   The stored `depth` column itself is drift-corrected but not smoothed, so short vertical excursions
#'   keep their true amplitude.
#'
#' ## Estimating orientation
#'
#' The **tilt-compensated compass** (default) fuses the accelerometer and magnetometer. It reads the
#' tilt angles from gravity, then uses them to level the magnetometer before computing heading (Gunner
#' et al. 2021). It cannot drift, having no integration step, but it is affected by magnetic
#' disturbance and by the animal's own acceleration masquerading as gravity.
#'
#' The **Madgwick filter** adds the gyroscope, using a quaternion gradient-descent fusion. It rides
#' through transient disturbances better, at a higher computational cost, and its accelerometer-against-
#' gyroscope trade-off is set by `madgwick.beta` in [orientationControl()].
#'
#' ## Geographic and magnetic heading
#'
#' Where a deployment position is available, from the metadata or else the first usable coordinates in
#' the data, the magnetic declination for that place and date is obtained from a global geomagnetic
#' model and added, giving a *geographic* heading relative to true north. Where no position is
#' available the declination cannot be computed and the heading is left *magnetic*, relative to
#' magnetic north, rather than discarded. Which one you have is recorded in
#' `meta$deployment$heading_reference`, and a magnetic heading is reported in a warning.
#'
#' **A magnetic heading is still valid for relative orientation.** It differs from a geographic one by a
#' constant offset, the declination, which is a few degrees in some regions and more than 25 in others
#' (about -8 degrees in the Azores, -26 near Cape Town, +22 off New Zealand). Any
#' measure built from angle *differences*, or from the length of a resultant vector, is therefore
#' unaffected, because the offset cancels exactly: turning angle and turning rate, angular velocity,
#' circular variance, standard deviation and mean resultant length, heading autocorrelation, net and
#' cumulative heading change, and circling or turn detection all give identical answers either way.
#'
#' What a magnetic heading must not be used for is any absolute claim about direction, where the offset
#' rotates the result instead of cancelling: a circular mean or median heading, a dead-reckoned track
#' from [reconstructTrack()], comparison against GPS or Argos fixes, or any figure whose north
#' orientation is read off the page. Functions computing those check `heading_reference` and warn, while
#' the rotation-invariant ones stay silent, so a warning means the distinction genuinely affects what
#' you asked for.
#'
#' Heading describes the orientation of the tag rather than of the animal, so its accuracy depends on
#' how well the two are aligned - and **no mounting correction reaches heading**. Heading is computed
#' from the raw pitch and roll, and the mounting offsets described next are subtracted afterwards
#' without heading being recomputed. [applyAxisMapping()] resolves the sensor-axis orientation it can
#' determine, but where the mapping was derived from the accelerometer alone the magnetometer is left in
#' its raw chip frame, and residual mounting rotation about the vertical axis is never estimated. Treat
#' heading as the least well constrained of the orientation outputs.
#'
#' ## Mounting offsets
#'
#' A tag is never attached perfectly level, and the resulting constant pitch and roll offsets are
#' indistinguishable from the animal's posture unless they are estimated and removed. Both corrections
#' assume the tag is approximately aligned with the animal, and they have opposite data requirements.
#' The roll offset is the median roll over the most level part of the record, so it needs level
#' swimming. The pitch offset is read from the relationship between pitch and vertical speed, so it
#' needs *diving*, and it is declined outright on a record with too little vertical movement, reported
#' as an insufficient diving signal. The pitch correction follows Kawatsu et al. (2010).
#'
#' Large mounting deviations may leave errors in every derived orientation and movement estimate. See
#' `vignette("orientation-methods", package = "nautilus")` for how each correction is estimated and what
#' it can and cannot recover.
#'
#' ## Magnetometer calibration
#'
#' The magnetometer is corrected for the tag's own iron before heading is computed, using the same
#' engine as [calibrateMagnetometer()], whose Details give the full method. Where the field cloud is
#' genuinely three-dimensional it fits the full ellipsoid; for the thin band a level swimmer usually
#' produces it estimates the offset only, pinning the unobservable perpendicular component from the
#' geomagnetic inclination.
#'
#' Either way the correction is applied only when it clears the confidence gate: if the animal did not
#' rotate through enough headings to constrain the estimate, the heading is left raw rather than
#' distorted. A calibration already computed and stored by [calibrateMagnetometer()], such as a pooled
#' or externally sourced fit, is used when present and trusted; otherwise one is fitted here. The
#' switches are in [calibrationControl()].
#'
#' @section End-of-run warnings:
#' Findings are aggregated by type rather than by deployment. Each raises at most one warning, naming
#' the count and then listing the affected deployments inline with the value that differs:
#'
#' ```
#'   3 deployments have an unusual mounting roll, corrected (offset).
#'   PIN_10 (-45.0 deg), PIN_11 (-43.2 deg), PIN_12 (-39.8 deg)
#' ```
#'
#' The warning says what happened and to which deployments; what it means and what to do about it is
#' below. The per-deployment log at `verbose = "detailed"` reports each finding where it occurred, with
#' the value that triggered it.
#'
#' \describe{
#'   \item{potential pitch anomaly}{The median pitch exceeds `orientation$warning.threshold`. Either the
#'     tag is mounted far from the body axis or the axis mapping is wrong; check [checkTagMapping()]
#'     before using any posture metric.}
#'   \item{unusual mounting roll, corrected or not corrected}{The estimated mounting roll exceeds
#'     `orientation$warning.threshold`. Corrected means it was below `orientation$mount.roll.max` and has
#'     been subtracted, so the orientation is usable and the value is reported only so that an unusual
#'     mount is not silent. Not corrected means it exceeded that gate and was left in place, so roll and
#'     heading are rotated by roughly that amount.}
#'   \item{roll residual after correction}{The median roll is large and is not explained by a mounting
#'     offset that was measured but deliberately left uncorrected. Where the roll correction ran, this
#'     means it did not take; where it was disabled or could not be estimated, it means a large roll was
#'     left in place. Usually an axis-mapping problem rather than a mounting one.}
#'   \item{no magnetometer calibration}{Neither a trusted stored fit nor a coverage-passing inline
#'     estimate was available, so the heading carries the tag's uncorrected offset - the dominant source
#'     of dead-reckoning drift. Supply a fit through `calibrateMagnetometer(calibration.data = )`, or
#'     collect more rotation coverage. Recorded as `"uncalibrated_raw"` in `meta$mag_calibration$status`.}
#'   \item{magnetic heading}{No deployment position was available, so the declination could not be
#'     applied and the heading refers to magnetic north. See the heading section above for what this
#'     does and does not affect.}
#'   \item{processed without an applied axis mapping}{Orientation assumes body-frame inertial axes. Run
#'     [applyAxisMapping()] first unless the data is already in that frame.}
#'   \item{constant paddle channel, now set to NA}{The imported paddle column held one fixed value for
#'     the whole deployment - a dead or absent paddle wheel, not a measurement. Left in place it would
#'     count as that many genuine speed samples in any pooled statistic. Supply a `paddle.calibration`
#'     row so speed can be estimated from the magnetometer, or exclude these deployments from speed
#'     analyses.}
#'   \item{already processed and re-run}{The input already carried a `processTagData` step. Calibration
#'     and downsampling are skipped, but **the metrics are recomputed from the already-downsampled
#'     columns, so they will not reproduce the first run** - jerk and the separation of gravity from
#'     motion both depend on the sampling rate. Re-process from the imported data rather than from
#'     processed output.}
#'   \item{skipped for missing or unusable input}{The deployment has no entry in the returned data and
#'     was not written to `output.dir`. A channel removed by [checkSensorIntegrity()] is recorded in
#'     `meta$sensors$excluded`.}
#' }
#'
#' @return If `return.data = TRUE`, a named list holding one processed deployment per element. If
#'   `return.data = FALSE`, a character vector of the written `.rds` file paths. Files are written
#'   whenever `output.dir` is set, regardless of what is returned.
#'
#' @references
#' Gunner RM, Holton MD, Scantlebury MD, *et al.* (2021) Dead-reckoning animal movements in R: a
#' reappraisal using Gundog.Tracks. *Animal Biotelemetry* 9:23. \doi{10.1186/s40317-021-00245-z}
#'
#' Kawatsu S, Sato K, Watanabe Y, Hyodo S, Breves JP, Fox BK, *et al.* (2010) A new method to calibrate
#' attachment angles of data loggers in swimming sharks. *EURASIP Journal on Advances in Signal
#' Processing* 2010:732586. \doi{10.1155/2010/732586}
#'
#' Ydesen KS, Wisniewska DM, Hansen JD, Beedholm K, Johnson M, Madsen PT (2014) What a jerk: prey
#' engulfment revealed by high-rate, super-cranial accelerometry on a harbour seal. *Journal of
#' Experimental Biology* 217:2239-2243. \doi{10.1242/jeb.100016}
#'
#' @seealso [importTagData()] for reading the raw files; [applyAxisMapping()] for the step that must
#'   come first; [detectDives()] and [calculateTailBeats()] for what typically follows.
#'
#' @examples
#' \dontrun{
#' # Axis-map the imported data into the body frame first, then derive
#' # kinematics and orientation and downsample to 1 Hz.
#' oriented <- applyAxisMapping(imported)
#' tag <- processTagData(oriented,
#'                       downsample.to = 1,
#'                       orientation.algorithm = "tilt_compass",
#'                       paddle.calibration = paddle_cal)
#'
#' # A batch of saved deployments: write a diagnostic PDF and save incrementally.
#' processTagData(list.files("./oriented", full.names = TRUE),
#'                plot.file = "./qc/corrections.pdf",
#'                return.data = FALSE, output.dir = "./processed")
#' }
#' @export
processTagData <- function(data,
                           downsample.to = 1,
                           orientation.algorithm = "tilt_compass",
                           orientation = orientationControl(),
                           calibration = calibrationControl(),
                           smoothing = smoothingControl(),
                           depth.drift = depthDriftControl(),
                           paddle.calibration = NULL,
                           burst.quantiles = c(0.95, 0.99),
                           plot = FALSE,
                           plot.file = NULL,
                           return.data = TRUE,
                           output.dir = NULL,
                           output.suffix = NULL,
                           compress = TRUE,
                           data.table.threads = NULL,
                           verbose = "detailed") {

  # resolve the control objects, then unpack into the local names used throughout the body
  calibration <- .as_control(calibration, calibrationControl, "nautilus_calibration", "calibration")
  orientation <- .as_control(orientation, orientationControl, "nautilus_orientation", "orientation")
  smoothing   <- .as_control(smoothing,   smoothingControl,   "nautilus_smoothing",   "smoothing")
  depth.control <- .as_control(depth.drift, depthDriftControl, "nautilus_depth_drift", "depth.drift")
  hard.iron.calibration <- calibration$hard.iron
  soft.iron.calibration <- calibration$soft.iron
  use.stored.calibration <- calibration$use.stored %||% TRUE
  madgwick.beta                 <- orientation$madgwick.beta
  correct.pitch.offset          <- orientation$correct.pitch
  correct.roll.offset           <- orientation$correct.roll
  pitch.offset.min.r2           <- orientation$pitch.offset.min.r2
  mount.roll.max                <- orientation$mount.roll.max %||% 60
  orientation.warning.threshold <- orientation$warning.threshold
  heading.denoise               <- orientation$heading.denoise %||% "auto"
  heading.denoise.window        <- orientation$heading.denoise.window %||% 3
  static.window         <- smoothing$static
  orientation.smoothing <- smoothing$orientation
  dba.smoothing         <- smoothing$dba
  depth.smoothing       <- smoothing$depth
  speed.smoothing       <- smoothing$speed


  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

  # measure running time
  start.time <- Sys.time()

  # verbosity level (0 quiet / 1 normal / 2 detailed)
  lvl <- .verbosity(verbose)
  # per-step detail line (level >= 2), routed through the shared cli logger (a `->` bullet) so the
  # style matches the other workflow functions.
  say <- function(...) .log_detail(lvl, ...)

  # silence data.table's internal progress messages for the duration of the run (restored on exit)
  old_dt_progress <- options(datatable.showProgress = FALSE)
  on.exit(options(old_dt_progress), add = TRUE)

  # scalar argument validation
  .assert_flag(return.data, "return.data")
  orientation.algorithm <- match.arg(orientation.algorithm, c("tilt_compass", "madgwick"))
  if (!is.null(downsample.to)) .assert_number(downsample.to, "downsample.to", min = 0)
  if (!is.numeric(burst.quantiles) || any(burst.quantiles <= 0) || any(burst.quantiles > 1)) {
    .abort("{.arg burst.quantiles} must be a numeric vector with values in (0, 1].")
  }
  .assert_dir(output.dir, "output.dir")                         # fail-fast: must exist
  .assert_string(output.suffix, "output.suffix", null_ok = TRUE)
  .assert_compress(compress)
  .assert_output(return.data, output.dir)
  # opt-in per-deployment diagnostic PDF (correction QC): gather while raw data is in memory, render after
  .assert_flag(plot, "plot")
  .assert_writable_file(plot.file, "plot.file", ext = "pdf", null_ok = TRUE)
  collect_diag <- isTRUE(plot) || !is.null(plot.file)
  diag_bundles <- list()

  # resolve input: a character vector of RDS paths, or an in-memory list / single data.frame
  is_filepaths <- is.character(data)
  .assert_nonempty(data, "data")             # loud failure on empty input (e.g. a typo'd list.files() -> character(0))
  if (is_filepaths) {
    missing_files <- data[!file.exists(data)]
    if (length(missing_files) > 0) .abort(c("These input files were not found:", stats::setNames(missing_files, rep("*", length(missing_files)))))
  } else if (!is.list(data) || inherits(data, "data.frame")) {
    .assert_columns(data, "ID", "data")
    data <- split(data, data$ID)
  }

  # define required columns based on the chosen orientation algorithm:
  #  - always: ID, datetime, tri-axial accelerometer, depth
  #  - madgwick additionally requires the gyroscope
  # The magnetometer (needed only for heading) and temperature are optional; any
  # absent recognized channels are simply skipped downstream.
  required_cols <- c("ID", "datetime", "ax", "ay", "az", "depth")
  if (orientation.algorithm == "madgwick") required_cols <- c(required_cols, "gx", "gy", "gz")

  # if data is already in memory (not file paths), validate each dataset up front
  if (!is_filepaths) {
    for (nm in names(data)) {
      # channel presence is checked per deployment inside the loop (a curated deployment is skipped,
      # not fatal); only the datetime CLASS is a structural contract worth failing up front
      if (!inherits(data[[nm]]$datetime, "POSIXct")) {
        .abort("The {.field datetime} column must be POSIXct in {.val {nm}}.")
      }
    }
    missing_attr <- vapply(data, function(x) is.null(attr(x, "nautilus.version")), logical(1))
    if (any(missing_attr)) {
      cli::cli_warn(c("Some datasets were likely not processed via {.fn importTagData}: {.val {names(data)[missing_attr]}}.",
                      "i" = "Run them through {.fn importTagData} first to ensure correct formatting."))
    }
  }

  # validate paddle.calibration if supplied (its presence is what enables paddle-speed estimation)
  if (!is.null(paddle.calibration)) {
    # coerce to data.frame if it's a data.table
    if (data.table::is.data.table(paddle.calibration)) paddle.calibration <- as.data.frame(paddle.calibration)
    if (!is.data.frame(paddle.calibration)) .abort("{.arg paddle.calibration} must be a data.frame.")
    missing_cols <- setdiff(c("year", "package_id", "slope"), names(paddle.calibration))
    if (length(missing_cols) > 0) {
      .abort("{.arg paddle.calibration} is missing required column(s): {.val {missing_cols}}.")
    }
    if (!is.numeric(paddle.calibration$year)) .abort("Column {.field year} in {.arg paddle.calibration} must be numeric.")
    if (!is.numeric(paddle.calibration$slope)) .abort("Column {.field slope} in {.arg paddle.calibration} must be numeric.")
  }

  # validate data.table threads if specified
  if (!is.null(data.table.threads)) {
    n_cores <- parallel::detectCores()
    if (!is.numeric(data.table.threads) || data.table.threads < 1 || data.table.threads > n_cores) {
      .abort("{.arg data.table.threads} must be a single number between 1 and {n_cores}.")
    }
  }


  ##############################################################################
  # Initialize variables #######################################################
  ##############################################################################

  # create lists to store processed data, plots, and summaries for each animal
  n_animals <- length(data)
  data_list <- vector("list", length = n_animals)
  saved     <- vector("list", length = n_animals)    # per-deployment written .rds path (NULL where nothing saved)
  ids       <- rep(NA_character_, n_animals)          # per-slot deployment id (NA marks a skipped slot)
  if (isTRUE(plot) && n_animals > 1L)                            # active-device diagnostics flood for a batch
    warning("processTagData: plot = TRUE renders a page-set per deployment to the active device; for a batch, prefer plot.file = <one PDF>.", call. = FALSE)

  # header
  hdr_bullets <- sprintf("Input: %d tag%s", n_animals, if (n_animals != 1) "s" else "")
  if (!is.null(output.dir)) hdr_bullets <- c(hdr_bullets, paste0("Output: ", output.dir))
  .log_header(lvl, "processTagData", "Plotting the course: deriving orientation and motion metrics",
              bullets = hdr_bullets,
              arrow = paste0("Method: ", orientation.algorithm, " orientation",
                             if (!is.null(downsample.to)) paste0(", downsample to ", downsample.to, " Hz")))
  n_done <- 0L
  # cohort volume totals for the summary line: input rows, stored rows, and summed tracked time.
  # Kept as running sums rather than derived at the end, because `data_list` holds only what the
  # caller asked for (return.data = FALSE stores paths, not tables).
  tot_in <- 0; tot_out <- 0; tot_secs <- 0


  # set data.table threads if specified
  if (!is.null(data.table.threads)) {
    original_threads <- data.table::getDTthreads()
    data.table::setDTthreads(threads = data.table.threads)
    on.exit(data.table::setDTthreads(threads = original_threads), add = TRUE)
  }


  ##############################################################################
  # Process data for each folder ###############################################
  ##############################################################################

  # iterate over each animal
  unoriented_ids <- character(0)                 # ordering guard: tags not run through applyAxisMapping()
  uncalibrated_ids <- character(0)               # requested magnetometer that received ZERO correction (raw heading)
  dead_paddle_ids <- character(0)                # imported paddle channel was constant (dead sensor) and was dropped
  reprocessed_ids <- character(0)                # input already carried a processTagData step (accidental re-run)
  skipped_ids <- character(0)   # deployments set aside for missing/unusable input
  nodecl_ids  <- character(0)   # heading kept as MAGNETIC (no position -> no declination available)
  # Orientation findings are ACCUMULATED and warned once per finding type at the end, never once per
  # deployment. Warning inside the loop scaled with the cohort: 10 rolled mounts meant 10 warnings, and
  # R replaces the whole warning surface with "There were N warnings (use warnings() to see them)" as
  # soon as 11 accumulate - so on a large batch the per-deployment form lost every message it raised.
  # Each item is "<id> (<value>)", so the group reads as one line of ids at any cohort size.
  pitch_anom_items   <- character(0)   # median pitch beyond the warning threshold
  roll_mount_items   <- character(0)   # unusual mounting roll, offset APPLIED
  roll_uncorr_items  <- character(0)   # unusual mounting roll, offset NOT applied (exceeds mount.roll.max)
  roll_resid_items   <- character(0)   # roll left over after a correction that did not take

  for (i in seq_along(data)) {

    ############################################################################
    # load data for the current individual if using file paths #################
    if (is_filepaths) {

      # get current file path
      file_path <- data[i]

      # load current file
      individual_data <- readRDS(file_path)

      # A channel curated away by checkSensorIntegrity() (or by the exclude_sensors metadata column)
      # is a property of ONE deployment. Set it aside and carry on: aborting here discarded every
      # deployment already processed in the batch.
      missing_cols <- setdiff(required_cols, names(individual_data))
      skip_reason <- if (length(missing_cols) > 0)
        .explainMissingColumns(missing_cols, tryCatch(attr(individual_data, "nautilus", exact = TRUE),
                                                      error = function(e) NULL))
      else if (!inherits(individual_data$datetime, "POSIXct")) "the datetime column is not POSIXct"
      if (!is.null(skip_reason)) {
        lab <- .deploymentLabel(individual_data, file_path, i)
        # a skipped deployment gets its own delimited block, like every other one: the reason must be
        # attributable to a tag at a glance, not inferred from its position between neighbouring blocks
        .log_h2(lvl, sprintf("%s (%d/%d)", lab, i, n_animals), min_level = 1L)
        .log_skip(lvl, skip_reason, " ", cli::symbol$bullet, " skipped")
        skipped_ids <- c(skipped_ids, lab)
        .log_gap(lvl)
        next
      }
      if (is.null(attr(individual_data, "nautilus.version"))) {
        cli::cli_warn(c("File {.file {basename(file_path)}} was likely not processed via {.fn importTagData}.",
                        "i" = "Run it through {.fn importTagData} first to ensure correct formatting."))
      }

    ############################################################################
    # data is already in memory (list of data frames/tables) ###################
    } else {

      # access the individual dataset
      individual_data <- data[[i]]
      file_path <- NA_character_

      # the in-memory branch is gated exactly like the file branch above: a deployment whose channels
      # were curated away is skipped, not fatal, and the two entry points must not disagree about what
      # counts as usable input
      if (!is.null(individual_data) && NROW(individual_data)) {
        missing_cols <- setdiff(required_cols, names(individual_data))
        skip_reason <- if (length(missing_cols) > 0)
          .explainMissingColumns(missing_cols, tryCatch(attr(individual_data, "nautilus", exact = TRUE),
                                                        error = function(e) NULL))
        else if (!inherits(individual_data$datetime, "POSIXct")) "the datetime column is not POSIXct"
        if (!is.null(skip_reason)) {
          lab <- .deploymentLabel(individual_data, names(data)[i], i)
          .log_h2(lvl, sprintf("%s (%d/%d)", lab, i, n_animals), min_level = 1L)
          .log_skip(lvl, skip_reason, " ", cli::symbol$bullet, " skipped")
          skipped_ids <- c(skipped_ids, lab)
          .log_gap(lvl)
          next
        }
      }
    }

    # An empty or NULL slot used to `next` SILENTLY - no block, no reason, and no entry in
    # `skipped_ids`, so the deployment vanished from both the console and the end-of-run summary.
    # It gets the same block and the same accounting as any other skip.
    if (is.null(individual_data) || length(individual_data) == 0 || NROW(individual_data) == 0) {
      lab <- .deploymentLabel(individual_data, if (is_filepaths) file_path else names(data)[i], i)
      .log_h2(lvl, sprintf("%s (%d/%d)", lab, i, n_animals), min_level = 1L)
      .log_skip(lvl, "no data ", cli::symbol$bullet, " skipped")
      skipped_ids <- c(skipped_ids, lab)
      .log_gap(lvl)
      next
    }

    # get ID
    id <- unique(individual_data$ID)[1]
    ids[i] <- as.character(id)                  # index-aligned with data_list / saved (skipped slots stay NA)

    # per-individual sub-header (level-2 only; groups this individual's detail lines)
    .log_h2(lvl, sprintf("%s (%d/%d)", id, i, n_animals))

    # ensure data.table (split() of a single data.frame yields data.frames)
    if (!data.table::is.data.table(individual_data)) individual_data <- data.table::as.data.table(individual_data)

    # ensure the consolidated nautilus metadata is present (migrating legacy attrs)
    individual_data <- .ensureMeta(individual_data)
    imeta <- .getMeta(individual_data)   # input metadata (deployment, tag, etc.)

    # RE-PROCESSING GUARD: the input already carries a processTagData step in its audit trail, so this is
    # an accidental re-run. Re-running is idempotent (calibration/downsample skip, metrics recomputed),
    # but almost always unintended, so flag it per deployment and once at the end for batch runs.
    already_processed <- any(vapply(imeta$processing %||% list(),
                                    function(p) identical(p$step, "processTagData"), logical(1)))
    if (already_processed) reprocessed_ids <- c(reprocessed_ids, as.character(id))

    # ORDERING GUARD: orientation (pitch/roll/heading) assumes the IMU axes are already in the animal
    # body frame. If applyAxisMapping() was not run, that assumption is silently violated - flag it.
    if (!isTRUE(imeta$axis_mapping$applied)) unoriented_ids <- c(unoriented_ids, as.character(id))

    # ensure data is ordered by datetime
    data.table::setorder(individual_data, datetime)

    # calculate sampling frequency (rounded to whole Hz; the windowing below assumes >= 1 Hz)
    sampling_freq <- nrow(individual_data) / length(unique(lubridate::floor_date(individual_data$datetime, "sec")))
    sampling_freq <- round(sampling_freq)
    # A sampling rate below 1 Hz is a property of ONE deployment, so it sets that deployment aside
    # instead of aborting: this used to kill the whole batch, discarding every tag already processed -
    # the same defect class fixed across the pipeline in 6295c62 / f20c71c. `ids[i]` is reset to NA so
    # the slot is dropped by the `keep <- !is.na(ids)` filter and no NULL hole reaches the caller.
    if (!is.finite(sampling_freq) || sampling_freq < 1) {
      .log_skip(lvl, sprintf("sampling rate below 1 Hz (%s Hz) %s skipped",
                             format(sampling_freq), cli::symbol$bullet))
      skipped_ids <- c(skipped_ids, as.character(id))
      ids[i] <- NA_character_
      .log_gap(lvl)
      next
    }
    # seconds -> whole-sample window, floored at 1 (guards fractional static / smoothing windows)
    win <- function(seconds) max(1L, as.integer(round(seconds * sampling_freq)))

    # per-deployment diagnostics are collected here and emitted as one ordered block at the end of the
    # tag (replacing per-step narration with the actual findings). Built only at the detailed level.
    diag <- character(0)
    attrs_line <- NULL
    n_input <- nrow(individual_data)
    if (lvl >= 2L) {
      # tag attributes line; each part is dropped when its metadata is absent (no "NA . NA" noise)
      .has <- function(x) !is.null(x) && length(x) && !is.na(x) && nzchar(as.character(x))
      attrs_parts <- c(if (.has(imeta$tag$model)) as.character(imeta$tag$model),
                       if (.has(imeta$tag$type))  as.character(imeta$tag$type),
                       if (.has(imeta$tag$package_id)) paste0("package ", imeta$tag$package_id))
      attrs_line <- if (length(attrs_parts)) paste(attrs_parts, collapse = " \u00b7 ") else NULL
      secs <- as.numeric(difftime(individual_data$datetime[n_input], individual_data$datetime[1], units = "secs"))
      n_chan_in <- length(intersect(.sensorChannels(), names(individual_data)))
      diag["input"] <- sprintf("input: %s rows | %d channel%s | ~%g Hz | %s", .formatLargeNumber(n_input),
                               n_chan_in, if (n_chan_in != 1) "s" else "", sampling_freq, .fmt_duration(secs))

      # tag identity FIRST (it names the deployment and contexts everything below), then the re-processing
      # alert immediately under it. The findings block (input, calibration, ...) is emitted after the work.
      if (!is.null(attrs_line)) cli::cli_text("{cli::symbol$bullet} {attrs_line}")
      if (already_processed)
        say("! dataset already processed (re-running is usually unintended)")
    }

    # store original attributes, excluding internal ones
    discard_attrs <- c("row.names", "class", ".internal.selfref", "names")
    original_attributes <- attributes(individual_data)
    original_attributes <- original_attributes[!names(original_attributes) %in% discard_attrs]


    ############################################################################
    # Calibrate magnetometer ###################################################
    ############################################################################

    # extract raw magnetometer data (the magnetometer is optional; without it,
    # heading is left NA and only roll/pitch are derived)
    has_mag_cols <- all(c("mx", "my", "mz") %in% names(individual_data))
    if (has_mag_cols) {
      mag_data <- as.matrix(individual_data[, .(mx, my, mz)])
      valid_magnetometer_data <- !all(is.na(mag_data[, "mx"])) &&
        !all(is.na(mag_data[, "my"])) && !all(is.na(mag_data[, "mz"]))
    } else {
      valid_magnetometer_data <- FALSE
    }

    # check if paddle wheel info is present in the metadata
    has_paddle_info <- !is.null(imeta) && !is.na(imeta$tag$paddle_wheel)

    # actual calibration outcome (recorded in the processing trail); set inside the block below
    hard_iron_applied <- FALSE; soft_iron_applied <- FALSE; hard_iron_offset_mag <- NA_real_
    coverage_ok <- NA                                        # set inside the block; stays NA in the not_requested path
    calibration_source <- "none"                             # "none" | "inline (...)" | "stored <source> (<confidence>)"
    paddle_state <- NULL; heading_denoise_used <- 0          # paddle-wheel de-noise state (metadata; set below)
    mag_diag <- NULL                                          # per-deployment mag diagnostic bundle (when collect_diag)
    mag_state <- imeta$mag_calibration                       # the nested calibration state to persist (set in the block below)

    # proceed with calibration
    if (valid_magnetometer_data) {

      # paddle-wheel de-noise (shared .paddleState / .magDenoise primitives), applied BEFORE calibration so
      # the hard/soft-iron fit sees the CLEAN field rather than the oscillation-dominated one (the paddle's
      # huge per-axis range would otherwise corrupt the soft-iron). A spinning paddle magnet adds a large
      # high-frequency oscillation that is additive in the field-vector domain and averages to ~0 over a
      # rotation, so a centred (zero-phase) running mean of the mag vector removes it while keeping the slow
      # orientation signal. Window: data-derived per deployment ("auto") or fixed ("manual"); see orientationControl().
      # capture the raw mz for paddle-speed estimation, tied to the PADDLE-WHEEL flag (identical whether or
      # not heading de-noising runs, since it is taken before .magDenoise) - independent of heading.denoise.
      mz_raw <- if (isTRUE(imeta$tag$paddle_wheel)) mag_data[, "mz"] else NA
      mag_denoised <- FALSE
      if (heading.denoise != "off") {
        paddle_state <- .paddleState(mag_data, sampling_freq)
        if (isTRUE(imeta$tag$paddle_wheel) || isTRUE(paddle_state$present)) {
          if (heading.denoise == "manual") {
            dn_win <- heading.denoise.window                 # fixed window applied consistently
          } else {
            dn_win <- paddle_state$recommend.window          # auto: window from the detected paddle frequency
            # fall back to the manual window only when the paddle IS present but no window could be derived;
            # when auto reports the field clean (present = FALSE), trust it and do not over-smooth.
            if ((!is.finite(dn_win) || dn_win <= 0) && isTRUE(paddle_state$present)) dn_win <- heading.denoise.window
          }
          if (is.finite(dn_win) && dn_win > 0) {
            dn <- .magDenoise(mag_data, sampling_freq, dn_win)
            na_edges <- !stats::complete.cases(dn)
            dn[na_edges, ] <- mag_data[na_edges, ]            # keep the raw field at the centred-window edges
            mag_data <- dn; mag_denoised <- TRUE; heading_denoise_used <- dn_win
          }
          if (isFALSE(paddle_state$separation.ok))
            say("! paddle rotation too slow to separate from turning - heading may be unreliable; consider orientation.algorithm = 'madgwick'")
        }
      }

      # initialize calibrated data with raw data
      mag_calibrated <- mag_data

      # A STORED calibration (from calibrateMagnetometer, e.g. a per-package pooled or externally-sourced fit)
      # is applied when use.stored is on, both switches are requested, and it clears the confidence gate (a
      # low-confidence stored fit is ignored); a pooled/source fit can determine the full soft-iron that a
      # single under-covered deployment cannot. Otherwise the SAME engine runs inline (.calibrateMag): the
      # full ellipsoid when the cloud is well covered and dip-consistent, else the hard-iron-only 2D fallback
      # (in-plane centre + IGRF-pinned perpendicular + identity soft-iron). Either way the correction is
      # applied ONLY past the abort gates AND at high/medium confidence; on poorly-sampled deployments a
      # partial arc's midpoint is BIASED (not the sphere centre), so we leave the heading raw rather than
      # apply a harmful correction.
      prop <- imeta$mag_calibration$proposed                 # the estimate from calibrateMagnetometer (or NULL)
      already_applied <- isTRUE(imeta$mag_calibration$applied)  # idempotency: a prior run already corrected mx/my/mz
      # a stored (proposed) calibration is only valid in the exact axis-mapping frame it was estimated in; if
      # the data has since been re-mapped (different net), fall through to the inline estimate instead.
      use_stored <- !already_applied && isTRUE(use.stored.calibration) && hard.iron.calibration && soft.iron.calibration &&
                    !is.null(prop) && !is.null(prop$params$soft_iron) &&
                    isTRUE(prop$qc$confidence %in% c("high", "medium")) && all(is.finite(prop$params$center)) &&
                    identical(prop$params$axis_net, imeta$axis_mapping$net)
      applied_center <- c(0, 0, 0); applied_soft_iron <- diag(3)   # the exact transform applied (identity = none)
      inline_eng <- NULL; applied_status <- NULL                   # shared-engine result + applied status
      ig <- .magIGRF(imeta$deployment)                             # geomagnetic field once per deployment (reused below)

      if (already_applied) {
        # do NOT re-apply (that would double-correct); keep the field + the recorded applied state
        ap <- imeta$mag_calibration$applied_params
        applied_center <- ap$center %||% c(0, 0, 0); applied_soft_iron <- ap$soft_iron %||% diag(3)
        hard_iron_applied <- FALSE; soft_iron_applied <- FALSE
        coverage_ok <- isTRUE(imeta$mag_calibration$qc$coverage_ok)
        calibration_source <- "already applied"
        # (no inline message here: the "calibration: already applied (idempotent skip)" diagnostic line
        #  below already reports this, and the re-processing alert covers the "already processed" case)
      } else if (use_stored) {
        mag_calibrated <- .applyMagCal(mag_data, prop$params$center, prop$params$soft_iron)
        hard_iron_applied <- TRUE; soft_iron_applied <- TRUE; coverage_ok <- TRUE
        hard_iron_offset_mag <- sqrt(sum(prop$params$center^2))
        calibration_source <- sprintf("stored %s (%s)", prop$provenance$source %||% "calibration", prop$qc$confidence)
        applied_center <- prop$params$center; applied_soft_iron <- prop$params$soft_iron
        applied_status <- prop$provenance$fit_status %||% "calibrated_3d"
      } else if (hard.iron.calibration || soft.iron.calibration) {
        # INLINE via the SHARED engine (.calibrateMag, identical to calibrateMagnetometer), so a thin band
        # gets the hard-iron 2D fallback + IGRF perpendicular pin rather than being left raw. Fit on a
        # decimated cloud (bounded cost); apply to the full field. Gravity = a ~2 s low-pass of accel.
        grav_lp <- NULL
        if (all(c("ax", "ay", "az") %in% names(individual_data))) {
          gw <- max(2L, as.integer(round(2 * sampling_freq)))
          grav_lp <- cbind(data.table::frollmean(individual_data$ax, gw, fill = NA, align = "center"),
                           data.table::frollmean(individual_data$ay, gw, fill = NA, align = "center"),
                           data.table::frollmean(individual_data$az, gw, fill = NA, align = "center"))
        }
        stride <- max(1L, nrow(mag_data) %/% 8000L); idx <- seq(1L, nrow(mag_data), by = stride)
        eng <- .calibrateMag(mag_data[idx, , drop = FALSE],
                             grav = if (!is.null(grav_lp)) grav_lp[idx, , drop = FALSE] else NULL,
                             igrf.incl = ig$inclination,
                             target.radius = if (is.finite(ig$intensity)) ig$intensity else NA_real_)
        # apply only a TRUSTED inline fit (past the abort gates AND high/medium confidence) - an under-
        # determined band with no IGRF pin (low confidence) is left raw rather than applying a possibly-
        # worse-than-raw correction, mirroring the use_stored gate.
        if (!is.null(eng) && isTRUE(eng$recommend_apply) && isTRUE(eng$confidence %in% c("high", "medium"))) {
          # honour the hard/soft-iron toggles: hard-iron only -> centre with an identity soft-iron
          applied_center    <- if (hard.iron.calibration) eng$center else c(0, 0, 0)
          applied_soft_iron <- if (soft.iron.calibration) eng$soft_iron else diag(3)
          mag_calibrated <- .applyMagCal(mag_data, applied_center, applied_soft_iron)
          hard_iron_applied <- hard.iron.calibration; soft_iron_applied <- soft.iron.calibration
          coverage_ok <- isTRUE(eng$coverage_ok); hard_iron_offset_mag <- sqrt(sum(applied_center^2))
          calibration_source <- sprintf("inline %s", eng$status); inline_eng <- eng; applied_status <- eng$status
        } else {
          hard_iron_applied <- FALSE; soft_iron_applied <- FALSE; coverage_ok <- FALSE
          applied_status <- "uncalibrated_raw"; inline_eng <- eng; calibration_source <- "inline (not applied)"
        }
      }

      # ---- assemble the nested calibration state (single source of truth). `proposed` preserved. ----
      requested_cal <- hard.iron.calibration || soft.iron.calibration
      applied_any   <- already_applied || use_stored || isTRUE(hard_iron_applied) || isTRUE(soft_iron_applied)
      mag_status <-
        if (already_applied)               (imeta$mag_calibration$status %||% "calibrated_3d")
        else if (!requested_cal)           "not_requested"
        else if (!is.null(applied_status)) applied_status         # stored fit's / inline engine's outcome
        else                               "uncalibrated_raw"
      if (already_applied) {
        mag_state <- imeta$mag_calibration                   # preserve entirely; nothing re-applied this run
      } else {
        applied_qc <-
          if (use_stored)              prop$qc
          else if (!is.null(inline_eng) && identical(applied_status, inline_eng$status))
                                       list(confidence = inline_eng$confidence, coverage_ok = isTRUE(inline_eng$coverage_ok),
                                            radcv = inline_eng$radcv, igrf_residual = inline_eng$igrf_residual,
                                            axis_span = inline_eng$axis_span)
          else                         list(confidence = NA_character_, coverage_ok = NA, radcv = NA_real_,
                                            igrf_residual = NA_real_, axis_span = rep(NA_real_, 3))
        mag_method <- if (use_stored) (prop$provenance$method %||% "stored")
                      else if (!is.null(inline_eng)) (inline_eng$method_used %||% "inline") else NA_character_
        perp_src   <- if (use_stored) (prop$provenance$perp_source %||% "data")
                      else if (!is.null(inline_eng)) (inline_eng$perp_source %||% "data") else NA_character_
        mag_state <- imeta$mag_calibration                   # keep `proposed`
        mag_state$status         <- mag_status
        mag_state$applied        <- isTRUE(applied_any)
        mag_state$applied_params <- list(center = applied_center, soft_iron = applied_soft_iron, axis_net = imeta$axis_mapping$net)
        mag_state$qc             <- applied_qc
        mag_state$provenance     <- list(method = mag_method, source = calibration_source, perp_source = perp_src)
        if (identical(mag_status, "uncalibrated_raw")) uncalibrated_ids <- c(uncalibrated_ids, as.character(id))
      }

      # diagnostic: the calibration actually applied (+ offset magnitude / coverage skip reason / paddle)
      if (lvl >= 2L) {
        requested <- hard.iron.calibration || soft.iron.calibration
        if (already_applied) {
          cal <- "already applied (idempotent skip)"
        } else if (use_stored) {
          cal <- sprintf("%s (|offset| %.1f \u00b5T)", calibration_source, hard_iron_offset_mag)
        } else if (!requested) {
          cal <- "none"
        } else if (identical(mag_status, "uncalibrated_raw")) {
          cal <- "skipped - left raw (insufficient rotation coverage / no IGRF pin)"
        } else {                                              # inline fit applied
          cal <- sprintf("inline %s", inline_eng$status %||% "")
          if (isTRUE(hard_iron_applied)) cal <- sprintf("%s (|offset| %.1f \u00b5T)", cal, hard_iron_offset_mag)
        }
        if (mag_denoised) diag["denoise"] <- "paddle-wheel mag de-noised"
        diag["calibration"] <- paste0("calibration: ", cal)
      }

      # diagnostic capture (opt-in): the cloud the calibration SAW + the transform actually applied, while
      # both are in memory. Reconstructed corrected cloud (raw -> sphere) is what the mag panel renders.
      if (collect_diag) {
        req_any    <- hard.iron.calibration || soft.iron.calibration
        diag_source <-
          if (use_stored)                        calibration_source
          else if (hard_iron_applied || soft_iron_applied)
            paste0("inline ", paste(c(if (hard_iron_applied) "hard-iron", if (soft_iron_applied) "soft-iron"), collapse = " + "))
          else if (req_any)                      "no calibration (insufficient rotation coverage)"
          else                                   "no calibration (not requested)"
        accel_xyz <- if (all(c("ax", "ay", "az") %in% names(individual_data)))
                       cbind(individual_data$ax, individual_data$ay, individual_data$az) else NULL
        # the engine's own verdict (not recomputed from the possibly-identity applied transform)
        diag_conf <- if (!is.null(inline_eng)) inline_eng$confidence
                     else if (use_stored)      prop$qc$confidence
                     else if (already_applied) imeta$mag_calibration$qc$confidence
                     else                      NA_character_
        mag_diag <- .captureMagDiag(mag_data, accel_xyz, applied_center, applied_soft_iron,
                                    coverage_ok, diag_source, ig, fs = sampling_freq,
                                    confidence = diag_conf, status = mag_status)
      }

      # store the calibrated magnetometer field (keeps the documented uT-scale values;
      # heading is computed from atan2 ratios downstream, which are scale-invariant, so
      # no unit-sphere normalization is needed here)
      individual_data[, `:=`(mx = mag_calibrated[,1], my = mag_calibrated[,2], mz = mag_calibrated[,3])]

      # clean up calibration working objects
      objs_to_remove <- c("mag_data", "mag_calibrated", "hi")
      rm(list = intersect(objs_to_remove, ls()))

    # else: no usable magnetometer
    }else{
      if (lvl >= 2L) diag["calibration"] <- "calibration: skipped (no magnetometer)"
      mag_state <- imeta$mag_calibration
      mag_state$status <- "no_magnetometer"; mag_state$applied <- FALSE
    }


    ############################################################################
    # Calculate acceleration metrics ###########################################
    ############################################################################

    # calculate total acceleration
    individual_data[, accel := sqrt(ax^2 + ay^2 + az^2)]

    # Split acceleration into a dynamic (motion) and a static (gravity/posture) part.
    # doi: 10.3354/ab00104
    #
    # The dynamic part is a zero-phase Butterworth high-pass, NOT the former moving-average subtraction.
    # A running mean is a boxcar filter, and the high-pass it induces (raw - boxcar) has deep periodic
    # sinc nulls: the old 1 s motion post-filter zeroed 1/2/3 Hz outright, silently erasing fast tail-beats
    # and ray wingbeats before any analysis saw them (validated on reef-manta video ground truth). The
    # Butterworth passes the whole in-band spectrum flat and rings only near its cutoff. The `static`
    # window (seconds) is mapped to the equivalent -3 dB cutoff so the default (3 s) reproduces the
    # previous ~0.25 Hz split; 0.7554 is that boxcar-subtract -3 dB * window constant (from the exact
    # Dirichlet response at fs). `.filtfiltCorner` pre-compensates for filtfilt squaring the response, and
    # `.filterSegments` filters each finite run independently so the filter never rings across a data gap.
    hp_cut <- .filtfiltCorner(0.7554 / static.window, order = 2, type = "high")
    dynamicX <- .filterSegments(individual_data$ax, sampling_freq, hp_cut, type = "high", order = 2)
    dynamicY <- .filterSegments(individual_data$ay, sampling_freq, hp_cut, type = "high", order = 2)
    dynamicZ <- .filterSegments(individual_data$az, sampling_freq, hp_cut, type = "high", order = 2)

    # the static (gravity/posture) component is the complement, so static + dynamic == raw exactly; the
    # high-pass rejects DC, leaving gravity and slow posture in static (the pitch/roll reference below)
    staticX <- individual_data$ax - dynamicX
    staticY <- individual_data$ay - dynamicY
    staticZ <- individual_data$az - dynamicZ

    # calculate ODBA (Overall Dynamic Body Acceleration) and VeDBA (Vectorial DBA)
    individual_data[, `:=`(
      odba = abs(dynamicX) + abs(dynamicY) + abs(dynamicZ),
      vedba = sqrt(dynamicX^2 + dynamicY^2 + dynamicZ^2)
    )]

    # smooth VeDBA/ODBA with a zero-phase Butterworth low-pass (optional). A moving average would again
    # impose sinc nulls; the Butterworth rolls off smoothly. The `dba` window maps to the equivalent
    # -3 dB cutoff (0.4430 = the textbook moving-average -3 dB * window constant, matching the former 2 s
    # boxcar's ~0.22 Hz). The low-pass preserves the DC level, so the smoothed activity keeps its
    # magnitude; a low-pass can dip a hair below zero at sharp onsets, so the non-negative energy proxy is
    # clamped at 0.
    if(!is.null(dba.smoothing)){
      lp_cut <- .filtfiltCorner(0.4430 / dba.smoothing, order = 2, type = "low")
      individual_data[, odba := pmax(.filterSegments(odba, sampling_freq, lp_cut, type = "low", order = 2), 0)]
      individual_data[, vedba := pmax(.filterSegments(vedba, sampling_freq, lp_cut, type = "low", order = 2), 0)]
    }

    # estimate burst swimming events: the top quantile(s) of INSTANTANEOUS VeDBA (vectorial dynamic body
    # acceleration). Keyed on VeDBA, not total |accel|: |accel| = |g + a_dyn| is dominated by the ~1 g
    # gravity baseline and inflated by the dynamic component's projection onto gravity, so it over-flags
    # gravity-aligned (e.g. descending) bursts and can MISS an upward burst whose |accel| dips below 1 g;
    # VeDBA = |a_dyn| is isotropic in the dynamic acceleration. The unsmoothed dynamics are used so brief
    # bursts are not attenuated by the dba smoothing. NOTE: the threshold is RELATIVE (per-deployment) -
    # it always flags the top (1 - q) fraction of this record, not an absolute activity level.
    if(!is.null(burst.quantiles)){
      vedba_inst <- sqrt(dynamicX^2 + dynamicY^2 + dynamicZ^2)
      for(q in burst.quantiles){
        vedba_threshold <- stats::quantile(vedba_inst, probs = q, na.rm = TRUE)
        burst_col <- paste0("burst", q*100)
        individual_data[, (burst_col) := as.integer(vedba_inst >= vedba_threshold)]
      }
    }

    # jerk: the magnitude of the rate of change of acceleration, ||d a / dt|| (g/s), formed HERE at the
    # native sampling rate from the raw (total) acceleration. The norm is rotation-invariant and a constant
    # gravity vector differentiates to zero, so - unlike ODBA/VeDBA - no gravity/static removal is needed;
    # this is the "norm-jerk" of the biologging literature (Ydesen et al. 2014, doi:10.1242/jeb.100016). It
    # emphasises brief high-frequency transients (strikes, startles, rapid manoeuvres). Being a DERIVATIVE
    # it MUST be formed before any downsampling - at 1 Hz the transient band is already gone - after which
    # the per-second mean aggregation below yields a mean-jerk activity index. A first difference across an
    # NA gap yields NA (not a spurious spike), so gaps propagate cleanly.
    jerkX <- c(NA_real_, diff(individual_data$ax)) * sampling_freq
    jerkY <- c(NA_real_, diff(individual_data$ay)) * sampling_freq
    jerkZ <- c(NA_real_, diff(individual_data$az)) * sampling_freq
    individual_data[, jerk := sqrt(jerkX^2 + jerkY^2 + jerkZ^2)]
    rm(jerkX, jerkY, jerkZ)

    # jerk amplifies high-frequency and quantisation noise: below ~30 Hz the prey-capture strike band
    # aliases away (Broell et al. 2013, doi:10.1242/jeb.077396), so on low-rate tags jerk is only a coarse
    # activity index, never an event detector. Recorded as a diagnostic (not an inline message) so it sits
    # in the findings block right after motion; the rate rationale lives in this comment and the docs.
    if (lvl >= 2L && sampling_freq < 25)
      diag["jerk"] <- sprintf("! jerk computed at %g Hz \u2013 treat as a coarse activity index only", sampling_freq)

    ############################################################################
    # Calculate linear motion metrics ##########################################
    ############################################################################

    # Linear-motion axes are the dynamic acceleration components (surge = ax - staticX = dynamicX, etc.),
    # i.e. the high-pass output computed above. The former optional 1 s moving-average post-smoother is
    # gone: it was the worst offender for the sinc nulls (it zeroed 1/2/3 Hz), so smoothing these axes
    # would re-introduce exactly the tail-beat/wingbeat erasure this change removes. Downstream analyses
    # (e.g. calculateTailBeats) band-pass these axes themselves, so they need the full spectrum here.
    individual_data[, surge := dynamicX]   # longitudinal (X): forward/backward
    individual_data[, sway  := dynamicY]   # lateral (Y): side-to-side
    individual_data[, heave := dynamicZ]   # vertical (Z): up/down (diving, wave action)

    ############################################################################
    # Depth zero-offset drift correction (before vertical velocity) ############
    ############################################################################

    # Correct the slowly-varying pressure-sensor zero offset from independent surface evidence (the WC
    # wet/dry signal in meta$ancillary$dry + surface-implying position fixes), BEFORE depth feeds the
    # vertical-velocity and every absolute-depth metric. Self-gating: abstains when evidence is too sparse.
    drift_res <- NULL; depth_diag <- NULL
    if ("depth" %in% names(individual_data)) {
      dry_tab <- if (!is.null(imeta$ancillary$dry)) imeta$ancillary$dry$data else NULL
      pos_tab <- .tagPositions(individual_data)        # canonical positions (meta$ancillary$positions)
      drift_res <- .correctDepthDrift(individual_data$depth, individual_data$datetime,
                                      dry = dry_tab, positions = pos_tab, control = depth.control)
      # capture BEFORE the in-place overwrite: individual_data$depth is still the raw (pre-correction) trace
      if (collect_diag) depth_diag <- .captureDepthDiag(individual_data$depth, individual_data$datetime, drift_res)
      individual_data[, depth := drift_res$depth]
      if (lvl >= 2L) {
        dl <- .depthDriftDiag(drift_res)          # scannable one-liner (NULL when the correction is off)
        if (!is.null(dl)) diag["depthdrift"] <- dl
      }
    }

    # diagnostic: dynamic-acceleration and depth ranges (the headline motion outputs)
    if (lvl >= 2L) {
      vedba_r <- range(individual_data$vedba, na.rm = TRUE)             # VeDBA: rotation-invariant, robust for towed tags
      diag["motion"] <- sprintf("motion: VeDBA %.2f \u2013 %.2f g", vedba_r[1], vedba_r[2])
      if ("depth" %in% names(individual_data)) {
        # .noNegZero: a residual few-cm-below-surface min otherwise prints as "-0" at 0 dp
        dep_r <- .noNegZero(range(individual_data$depth, na.rm = TRUE), 0)
        diag["depth"] <- sprintf("depth: %.0f \u2013 %.0f m", dep_r[1], dep_r[2])
      }
    }

    ############################################################################
    # Apply depth smoothing if requested ######################################
    ############################################################################

    ############################################################################
    # Smooth depth + calculate vertical velocity ###############################
    ############################################################################

    # The centered-difference vertical velocity is computed by the shared .verticalVelocity() helper, so
    # processTagData and checkTagMapping use an identical estimate.
    #
    # `depth.smoothing` conditions the series the DERIVATIVE is taken from - differentiating a raw
    # pressure trace amplifies its quantisation noise - and nothing more. The stored `depth` channel is
    # deliberately left UNSMOOTHED (drift-corrected only): a centred boxcar attenuates any excursion
    # shorter than its window, so overwriting depth with the smoothed series silently shrank short dives.
    # For the shipped 10 s default a 3 m / 8 s dive was stored as 1.2 m and a 3 m / 20 s dive as 2.25 m
    # (retention 1 - L/2T for a triangular profile), which is invisible in the deep, minutes-long dives
    # this package was first used on and severe for short-dive taxa. Consumers that want a smoothed depth
    # should smooth it themselves, at a window chosen for their own question.
    .vv <- .verticalVelocity(individual_data$depth, individual_data$datetime, sampling_freq,
                             depth.smoothing = depth.smoothing, speed.smoothing = speed.smoothing)
    individual_data[, vertical_velocity := .vv$velocity]
    rm(.vv)


    ############################################################################
    # Calculate orientation metrics ############################################
    ############################################################################

    # first, check sensor data validity
    valid_accel_data <- !all(is.na(individual_data$ax)) &
      !all(is.na(individual_data$ay)) &
      !all(is.na(individual_data$az))

    valid_gyro_data <- !all(is.na(individual_data$gx)) &
      !all(is.na(individual_data$gy)) &
      !all(is.na(individual_data$gz))

    # determine feasible orientation methods
    use_madgwick <- orientation.algorithm == "madgwick" && valid_accel_data && valid_gyro_data
    use_tilt_compass <- orientation.algorithm == "tilt_compass" && valid_accel_data
    orient_method <- NA_character_; heading_ok <- FALSE   # captured below for the diagnostic block


    #############################################################
    # Madgwick filter (native R) ################################
    if(use_madgwick){

      # prepare sensor matrices (accel + gyro always; magnetometer when valid)
      acc_mat <- as.matrix(individual_data[, .(ax, ay, az)])
      gyr_mat <- as.matrix(individual_data[, .(gx, gy, gz)])
      mag_mat <- if (valid_magnetometer_data) as.matrix(individual_data[, .(mx, my, mz)]) else NULL

      heading_ok <- valid_magnetometer_data
      orient_method <- sprintf("madgwick (%s, \u03b2 %g)", if (heading_ok) "MARG" else "IMU", madgwick.beta)

      # run the native-R Madgwick filter -> quaternions (w, x, y, z)
      Q <- .madgwickAHRS(gyr = gyr_mat, acc = acc_mat, mag = mag_mat,
                         frequency = sampling_freq, beta = madgwick.beta)
      w <- Q[, 1]; x <- Q[, 2]; y <- Q[, 3]; z <- Q[, 4]

      # compute pitch and roll
      pitch_deg <- asin(pmax(pmin(2 * (w * y - z * x), 1.0), -1.0)) * 180 / pi
      roll_deg <- atan2(2 * (w * x + y * z), 1 - 2 * (x^2 + y^2)) * 180 / pi

      # compute heading ONLY if mag was used
      heading_deg <- if (valid_magnetometer_data) {
        (atan2(2 * (w * z + x * y), 1 - 2 * (y^2 + z^2)) * 180 / pi) %% 360
      } else {
        rep(NA_real_, nrow(Q))
      }

      # store results
      individual_data[, `:=`(
        heading = heading_deg,
        pitch = pitch_deg,
        roll = roll_deg
      )]

      # clean up working objects
      rm(acc_mat, gyr_mat, mag_mat, Q, w, x, y, z, heading_deg, pitch_deg, roll_deg)


    #############################################################
    # else, default to the tilt-compensated compass method ######
    } else if (use_tilt_compass) {

      orient_method <- "tilt_compass"; heading_ok <- valid_magnetometer_data

      # roll and pitch (degrees) from the static (gravity) acceleration via the shared tilt helper
      # (same aerospace convention as checkTagMapping; atan2 is scale-invariant so no normalization needed)
      tilt <- .tiltFromAccel(staticX, staticY, staticZ)
      individual_data[, `:=`(roll = tilt$roll, pitch = tilt$pitch)]

      # heading only if the magnetometer is valid: tilt-compensate the field (roll/pitch in radians),
      # then take the magnetic heading; NA near the gimbal-lock pole (|pitch| > 89.5 deg)
      if (valid_magnetometer_data) {
        pr <- tilt$pitch * (pi / 180); rr <- tilt$roll * (pi / 180)
        mx_comp <- individual_data$mx * cos(pr) + individual_data$my * sin(pr) * sin(rr) + individual_data$mz * sin(pr) * cos(rr)
        my_comp <- individual_data$my * cos(rr) - individual_data$mz * sin(rr)
        individual_data[, heading := ifelse(abs(pitch) > 89.5, NA_real_, atan2(-my_comp, mx_comp) * (180 / pi))]
      } else {
        individual_data[, heading := NA_real_]
      }

    # if all else fails (captured by the orientation diagnostic line below as "insufficient sensor data")
    } else {
      individual_data[, `:=`(roll = NA_real_, pitch = NA_real_, heading = NA_real_)]
    }


    ############################################################################
    ## convert magnetic heading to geographic heading ##########################

    # only proceed if heading exists and is not all NA
    if (!all(is.na(individual_data$heading))) {

      # determine location to use for magnetic declination calculation
      if (!is.null(imeta) && !is.na(imeta$deployment$lon) && !is.na(imeta$deployment$lat)) {
        # use deployment info from metadata
        deploy_info <- data.frame(datetime = imeta$deployment$datetime,
                                  lon = imeta$deployment$lon,
                                  lat = imeta$deployment$lat)
      } else {
        # fallback: use the first available row with valid longitude and latitude
        valid_idx <- which(!is.na(individual_data$lon) & !is.na(individual_data$lat))[1]
        deploy_info <- if (!is.na(valid_idx))
          data.frame(datetime = individual_data$datetime[valid_idx],
                     lon = individual_data$lon[valid_idx],
                     lat = individual_data$lat[valid_idx]) else NULL
      }

      if (is.null(deploy_info)) {
        # No position, so declination is not computable. Keep the MAGNETIC heading rather than aborting
        # or discarding it: relative measures (turning rate, angular velocity, circular variance) are
        # unaffected by the constant offset, so the column is still useful. What must not happen is a
        # magnetic heading being read as geographic, so the frame is recorded explicitly and the
        # deployment is named in a warning that fires at any verbosity.
        heading_ref <- "magnetic"
        declination_deg <- NULL
        nodecl_ids <- c(nodecl_ids, as.character(id))
      } else {
        declination_deg <- round(oce::magneticField(longitude = deploy_info$lon, latitude = deploy_info$lat,
                                                    time = deploy_info$datetime)$declination, 2)
        # magnetic north -> geographic north
        individual_data[, heading := (heading + declination_deg) %% 360]
        heading_ref <- "geographic"
      }

    }else{

      # no heading at all: neither a declination nor a reference frame applies
      declination_deg <- NULL
      heading_ref <- NA_character_

    }


    ############################################################################
    # correct pitch offset if requested ########################################

    off_pitch <- NULL; off_roll <- NULL    # applied-offset diagnostics (NULL = none applied)
    pitch_diag <- NULL; roll_diag <- NULL  # per-deployment pitch/roll offset diagnostic bundles
    if (correct.pitch.offset) {

      # skip if all pitch values are NA
      if (all(is.na(individual_data$pitch))) {
        pitch_offset_deg <- NULL
        pitch_offset_r2 <- NULL

      } else {

        # Kawatsu mounting-pitch estimate: the intercept of pitch (rad) vs smoothed vertical velocity
        # (the pitch at zero vertical speed). It is only trustworthy when the animal dived enough to
        # define the line AND the linear pitch-vs-vertical-velocity relationship actually holds; a weak
        # fit means the "offset" is really just the mean pitch, so subtracting it would strip genuine
        # posture signal. We therefore gate the correction on the model R-squared (pitch.offset.min.r2).
        individual_data[, vv_smooth := data.table::frollmean(vertical_velocity, n = win(10), fill = NA, align = "center")]
        individual_data[, pitch_rad := pitch * (pi/180)]
        fit_data <- individual_data[!is.na(vv_smooth) & !is.na(pitch_rad)]

        # the line is undefined without enough points spanning a range of vertical velocities
        vv_sd <- if (nrow(fit_data)) stats::sd(fit_data$vv_smooth) else NA_real_
        degenerate <- nrow(fit_data) < 100L || !is.finite(vv_sd) || vv_sd < 1e-6
        pitch_model     <- if (!degenerate) stats::lm(pitch_rad ~ vv_smooth, data = fit_data) else NULL
        pitch_offset_r2  <- if (!is.null(pitch_model)) summary(pitch_model)$r.squared else NA_real_
        pitch_offset_deg <- if (!is.null(pitch_model)) unname(coef(pitch_model)[1]) * (180/pi) else NA_real_

        # apply only with a sufficiently strong fit AND a physically plausible (sub-threshold) offset
        apply_offset <- is.finite(pitch_offset_r2) && pitch_offset_r2 >= pitch.offset.min.r2 &&
                        is.finite(pitch_offset_deg) && abs(pitch_offset_deg) < orientation.warning.threshold
        if (apply_offset) {
          individual_data[, pitch := pitch - pitch_offset_deg]
          off_pitch <- sprintf("pitch %+.2f\u00b0 (R\u00b2 %.2f)", .noNegZero(pitch_offset_deg, 2), pitch_offset_r2)
        } else {
          # record WHY it was skipped (shown in the detailed diagnostic block)
          # `apply_offset` above is NA-safe, but this explanation ladder was not: a deployment with no
          # posture variation (a flat-mounted tag on a level animal, or a synthetic fixture) yields a
          # non-finite R2 that is not `degenerate`, and `if (NA < x)` aborts the whole run.
          off_pitch <- paste0("pitch offset skipped (",
            if (degenerate) "insufficient diving signal"
            else if (!is.finite(pitch_offset_r2)) "regression did not converge"
            else if (pitch_offset_r2 < pitch.offset.min.r2) sprintf("weak fit R\u00b2 %.2f < %.2f", pitch_offset_r2, pitch.offset.min.r2)
            else if (!is.finite(pitch_offset_deg)) "offset not estimable"
            else sprintf("offset %+.1f\u00b0 over threshold", pitch_offset_deg), ")")
          pitch_offset_deg <- NULL            # not applied; keep pitch_offset_r2 as computed for provenance
        }

        # capture the fit + scatter BEFORE cleanup (intercept kept even when gated, so the panel shows it)
        if (collect_diag)
          pitch_diag <- .capturePitchDiag(fit_data, pitch_model, pitch_offset_r2, apply_offset,
                                          pitch.offset.min.r2, orientation.warning.threshold, off_pitch)

        # clean up temporary columns
        individual_data[, c("pitch_rad", "vv_smooth") := NULL]
        if (!is.null(pitch_model)) rm(pitch_model)
      }

    } else {
      pitch_offset_deg <- NULL
      pitch_offset_r2 <- NULL
    }


    ############################################################################
    # correct roll offset if requested #########################################

    # The tag's mounting roll (housing->body) shows up as a persistent roll bias during steady,
    # level swimming, when a symmetric animal cruises upright on average. We estimate it as the
    # median roll over the most level half of the record and subtract it. For towed fin-clamped
    # tags this bias depends on the attachment site (a left vs right pectoral mount is mirror-imaged),
    # which the empirical median captures automatically.

    # Two SEPARATE questions, deliberately not sharing a threshold (they used to, and the shared
    # constant made a mount just past it lose its correction AND get reported as an anomaly - the
    # reported number then being the uncorrected mount rather than a residual):
    #   1. is this offset plausible enough to SUBTRACT?  -> mount.roll.max (wide; a steeply rolled
    #      clamp is a real geometry, so correct it)
    #   2. is this mount UNUSUAL enough to mention?      -> orientation.warning.threshold (narrow;
    #      raised on the ESTIMATE, so it fires whether or not the correction was applied)
    roll_offset_estimate <- NA_real_     # what was measured (NA = not estimable / not requested)
    roll_applied         <- FALSE        # was it actually subtracted
    roll_offset_status   <- if (correct.roll.offset) "not_estimable" else "not_requested"

    if (correct.roll.offset) {

      if (all(is.na(individual_data$roll))) {
        roll_offset_deg <- NULL

      } else {

        # smoothed vertical velocity, to isolate steady (near-level) swimming
        vv_window <- win(10)
        individual_data[, vv_smooth := data.table::frollmean(vertical_velocity, n = vv_window, fill = NA, align = "center")]

        # the more level half of the record (smallest |vertical velocity|)
        horiz_cut <- stats::median(abs(individual_data$vv_smooth), na.rm = TRUE)
        roll_offset_estimate <- stats::median(individual_data[abs(vv_smooth) <= horiz_cut & !is.na(roll), roll], na.rm = TRUE)
        # diagnostic: the PRE-correction level-swimming roll + the computed median (kept even if the gate rejects)
        roll_level_samp <- if (collect_diag) individual_data[abs(vv_smooth) <= horiz_cut & !is.na(roll), roll] else NULL

        roll_applied <- is.finite(roll_offset_estimate) && abs(roll_offset_estimate) < mount.roll.max
        if (roll_applied) {
          # subtract the offset and re-wrap roll into [-180, 180]
          individual_data[, roll := ((roll - roll_offset_estimate + 180) %% 360) - 180]
          roll_offset_deg    <- roll_offset_estimate
          roll_offset_status <- "applied"
          off_roll <- sprintf("roll %+.2f\u00b0", .noNegZero(roll_offset_estimate, 2))
        } else {
          roll_offset_deg    <- NULL       # nothing was subtracted; the estimate is kept separately
          roll_offset_status <- if (is.finite(roll_offset_estimate)) "rejected_over_max" else "not_estimable"
          off_roll <- if (is.finite(roll_offset_estimate))
            sprintf("roll offset skipped (%+.1f\u00b0 exceeds mount.roll.max %.0f\u00b0)",
                    roll_offset_estimate, mount.roll.max)
          else "roll offset skipped (not estimable)"
        }
        if (collect_diag)
          roll_diag <- .captureRollDiag(roll_level_samp, roll_offset_estimate, roll_applied, mount.roll.max)

        # clean up temporary column
        individual_data[, vv_smooth := NULL]
        rm(vv_window, horiz_cut)
      }

    } else {
      roll_offset_deg <- NULL
    }


    ############################################################################
    # apply a moving circular mean to smooth the metrics time series ###########

    if(!is.null(orientation.smoothing)) {
      window_size <- win(orientation.smoothing)
      # roll and heading wrap (circular); pitch is bounded [-90, 90] and does NOT wrap,
      # so it is smoothed with an ordinary moving mean to avoid pole distortion
      individual_data[, roll := .rollingCircularMean(roll, window = window_size, range = c(-180, 180) )]
      individual_data[, pitch := data.table::frollmean(pitch, n = window_size, fill = NA, align = "center")]
      individual_data[, heading := .rollingCircularMean(heading, window = window_size, range = c(0, 360))]
    }

    ############################################################################
    # check for potential axis issues (misalignment, swaps, or sign flips) #####

    pitch_anomaly_detected <- FALSE
    roll_anomaly_detected <- FALSE
    roll_mount_unusual <- FALSE
    median_pitch <- NA_real_; median_roll <- NA_real_

    # only check if pitch contain non-NA values
    if (!all(is.na(individual_data$pitch))) {
      median_pitch <- median(individual_data$pitch, na.rm = TRUE)
      if (abs(median_pitch) > orientation.warning.threshold) {
        .log_skip(lvl, "potential pitch anomaly: median = ", round(median_pitch, 1), "\u00b0")
        pitch_anomaly_detected <- TRUE
        pitch_anom_items <- c(pitch_anom_items, sprintf("%s (%.1f\u00b0)", id, median_pitch))
      }
    }

    # UNUSUAL MOUNT: judged on the ESTIMATED offset, so it reports the same quantity whether or not
    # the correction was applied. (The old check ran on the median roll AFTER correction, which is
    # ~0 by construction whenever the correction succeeds - so it could only ever fire on a
    # deployment whose correction had been refused, and then reported the mount as if it were a
    # residual.)
    if (is.finite(roll_offset_estimate) && abs(roll_offset_estimate) > orientation.warning.threshold) {
      roll_mount_unusual <- TRUE
      .log_skip(lvl, "unusual mounting roll: ", round(roll_offset_estimate, 1), "\u00b0 (",
                if (roll_applied) "corrected" else "NOT corrected", ")")
      # split by what was DONE, not by degree: a corrected mount leaves a usable orientation and an
      # uncorrected one does not, so they need different responses from the reader. Putting that in the
      # headline keeps each item a bare id and value.
      item <- sprintf("%s (%.1f\u00b0)", id, roll_offset_estimate)
      if (roll_applied) roll_mount_items  <- c(roll_mount_items, item)
      else              roll_uncorr_items <- c(roll_uncorr_items, item)
    }

    # RESIDUAL ANOMALY: roll left over after the correction, i.e. the correction did not take.
    # Skipped when the offset was estimated but refused, because there the residual IS the mount and
    # the notice above has already reported that number - warning twice about one thing is noise.
    if (!all(is.na(individual_data$roll))) {
      median_roll <- median(individual_data$roll, na.rm = TRUE)
      residual_is_the_mount <- is.finite(roll_offset_estimate) && !roll_applied
      if (abs(median_roll) > orientation.warning.threshold && !residual_is_the_mount) {
        .log_skip(lvl, "roll residual after correction: median = ", round(median_roll, 1), "\u00b0")
        roll_anomaly_detected <- TRUE
        roll_resid_items <- c(roll_resid_items, sprintf("%s (%.1f\u00b0)", id, median_roll))
      }
    }

    # diagnostics: orientation (method, posture medians, heading availability) and applied offsets
    if (lvl >= 2L) {
      hd <- paste0("heading ", if (heading_ok) "ok" else "NA")
      diag["orientation"] <- if (is.na(orient_method)) "orientation: insufficient sensor data"
        else if (is.finite(median_pitch))
          sprintf("orientation: median pitch %.1f\u00b0 \u00b7 roll %.1f\u00b0 \u00b7 %s",
                  .noNegZero(median_pitch, 1), .noNegZero(median_roll, 1), hd)
        else paste0("orientation: ", hd)
      off <- c(off_pitch, off_roll)
      if (length(off)) diag["offsets"] <- paste0("offsets: ", paste(off, collapse = " \u00b7 "))
    }


    ############################################################################
    # Calculate turning angles #################################################
    ############################################################################

    # step 1: create the column with NA_real_
    individual_data[, turning_angle := NA_real_]

    # step 2: fill it only if valid heading values exist
    if (any(!is.na(individual_data$heading))) {
      individual_data[, turning_angle := {
        circular_diff <- function(a, b) ((a - b + 180) %% 360) - 180
        if (.N < 2 || all(is.na(heading))) {
          rep(NA_real_, .N)
        } else {
          h_back  <- shift(heading, 1)
          h_front <- shift(heading, type = "lead")
          turn <- circular_diff(h_front, h_back) / 2
          # Handle edges if values are not NA
          turn[1]   <- if (!anyNA(heading[1:2])) circular_diff(heading[2], heading[1]) else NA_real_
          turn[.N]  <- if (!anyNA(heading[(.N - 1):.N])) circular_diff(heading[.N], heading[.N - 1]) else NA_real_
          turn
        }
      }]
    }


    ############################################################################
    # Estimate paddle wheel rotation frequency #################################
    ############################################################################

    if (!is.null(paddle.calibration)) {

      # determine if pre-calculated columns exist
      has_precalculated_freq <- "paddle_freq" %in% names(individual_data)
      has_precalculated_speed <- "paddle_speed" %in% names(individual_data)

      # initialize columns if they don't exist
      if (!has_precalculated_freq) individual_data[, paddle_freq := NA_real_]
      if (!has_precalculated_speed) individual_data[, paddle_speed := NA_real_]

      # initialize flag to determine if we should calculate speed internally
      perform_internal_calculation <- TRUE


      #############################################################
      # check for existing valid paddle data ######################

      # check if existing data is meaningful (not all NA and not constant)
      is_freq_meaningful <- has_precalculated_freq &&
        !all(is.na(individual_data$paddle_freq)) &&
        length(unique(na.omit(individual_data$paddle_freq))) > 1

      is_speed_meaningful <- has_precalculated_speed &&
        !all(is.na(individual_data$paddle_speed)) &&
        length(unique(na.omit(individual_data$paddle_speed))) > 1

      if (is_freq_meaningful && is_speed_meaningful) {
        diag["speed"] <- "speed: paddle freq + speed already present (kept)"
        perform_internal_calculation <- FALSE

      } else if (has_precalculated_speed && !has_precalculated_freq && is_speed_meaningful) {
        diag["speed"] <- "speed: paddle speed already present (freq set NA)"
        perform_internal_calculation <- FALSE
      }

      #############################################################
      # remaining checks for paddle wheel setup ###################

      # check if the tag was equipped with a paddle wheel
      if (perform_internal_calculation) {
        has_paddle_info <- !is.null(imeta) && !is.na(imeta$tag$paddle_wheel)
        if (!has_paddle_info) {
          diag["speed"] <- "speed: skipped (no paddle-wheel info)"
          perform_internal_calculation <- FALSE
        } else if (isFALSE(imeta$tag$paddle_wheel)) {
          diag["speed"] <- "speed: skipped (no paddle wheel)"
          perform_internal_calculation <- FALSE
        }
      }

      # check package ID and calibration
      if (perform_internal_calculation) {
        package_id <- if (!is.null(imeta)) imeta$tag$package_id else NA
        has_package <- !is.null(package_id) && !all(is.na(package_id))
        if (!has_package) {
          diag["speed"] <- "speed: skipped (no package_id)"
          perform_internal_calculation <- FALSE
        } else {

          # Determine deployment year for calibration lookup
          if (!is.null(imeta) && !is.na(imeta$deployment$datetime)) {
            deploy_year <- as.integer(format(imeta$deployment$datetime, "%Y"))
          } else {
            deploy_year <- as.integer(format(individual_data$datetime[1], "%Y"))
          }

          tag_calibration <- paddle.calibration[paddle.calibration$year == deploy_year &
                                                        paddle.calibration$package_id == package_id, ]
          has_calibration_info <- nrow(tag_calibration) > 0

          if (!has_calibration_info) {
            diag["speed"] <- "speed: skipped (no calibration values)"
            perform_internal_calculation <- FALSE
          } else if (sampling_freq < 50) {
            diag["speed"] <- "speed: skipped (sampling < 50 Hz)"
            perform_internal_calculation <- FALSE
          }
        }
      }

      #############################################################
      # perform internal speed estimation if all checks pass ######

      if (perform_internal_calculation) {

        # calculate frequencies and speed
        paddle_data <- .getPaddleSpeed(
          mz = mz_raw,
          sampling.rate = sampling_freq,
          calibration.slope = tag_calibration$slope,
          smooth.window = speed.smoothing
        )

        # add to sensor data
        individual_data[, paddle_freq := paddle_data$freq]
        individual_data[, paddle_speed := paddle_data$speed]

        # diagnostic: estimated speed range + calibration slope used
        if (lvl >= 2L) {
          sp_r <- range(paddle_data$speed, na.rm = TRUE)
          diag["speed"] <- sprintf("speed: %.2f \u2013 %.2f m/s (paddle wheel \u00b7 slope %.4f)", sp_r[1], sp_r[2], tag_calibration$slope)
        }
      }

      #############################################################
      # act on the meaningfulness verdict #########################

      # A pre-calculated paddle column that failed the test above is not data: it is CONSTANT - a dead or
      # absent paddle wheel writing one fixed value for the whole deployment. Until now that verdict was
      # computed and then ignored whenever the internal estimate could not run to replace it, so the
      # degenerate column survived into the output, where a constant-zero speed reads downstream as millions
      # of genuine zero-speed samples and quietly becomes the mode of any pooled distribution. Drop it to NA
      # instead: "this deployment has no paddle speed" is the honest record, and the value it held is
      # reported below rather than silently discarded.
      if (!perform_internal_calculation) {
        dropped <- character(0); held <- numeric(0)
        if (has_precalculated_speed && !is_speed_meaningful && !all(is.na(individual_data$paddle_speed))) {
          held <- stats::na.omit(individual_data$paddle_speed)[1]
          individual_data[, paddle_speed := NA_real_]; dropped <- c(dropped, "speed")
        }
        if (has_precalculated_freq && !is_freq_meaningful && !all(is.na(individual_data$paddle_freq))) {
          individual_data[, paddle_freq := NA_real_]; dropped <- c(dropped, "freq")
        }
        if (length(dropped)) {
          dead_paddle_ids <- c(dead_paddle_ids, id)
          prev <- if ("speed" %in% names(diag)) diag[["speed"]] else "speed: not estimated"
          diag["speed"] <- sprintf("%s \u00b7 dropped constant paddle %s%s", prev,
                                   paste(dropped, collapse = " + "),
                                   if (length(held)) sprintf(" (held %g throughout)", held) else "")
        }
      }
    }


    ############################################################################
    # Downsample data ##########################################################
    ############################################################################

    # select columns to keep (raw channels that are absent for partial sensor sets,
    # e.g. no gyroscope/magnetometer/temperature, are dropped via the intersect below)
    metrics <- c("temp","depth","ax", "ay", "az", "gx", "gy", "gz", "mx", "my", "mz",
                 "accel","odba","vedba","jerk","roll", "pitch", "heading",
                 "surge", "sway", "heave", "vertical_velocity", "turning_angle",
                 "paddle_freq", "paddle_speed")          # paddle cols kept only when present (dropped by intersect below)
    metrics <- intersect(metrics, names(individual_data))

    # store current sampling frequency
    sampling_rate <- sampling_freq

    # if a downsampling rate is specified, aggregate the data to the defined frequency (in Hz)
    if(!is.null(downsample.to)){

      # check if the specified downsampling frequency matches the dataset's sampling frequency
      if (downsample.to == sampling_freq) {
        if (lvl >= 2L) diag["downsample"] <- sprintf("downsample: skipped (already %g Hz)", sampling_freq)
        processed_data <- individual_data

      # check if the specified downsampling frequency exceeds the dataset's sampling frequency
      } else if(downsample.to > sampling_freq) {
        if (lvl >= 2L) diag["downsample"] <- sprintf("downsample: skipped (data %g Hz < target %g Hz)", sampling_freq, downsample.to)
        processed_data <- individual_data

      # start downsampling
      } else {

        # store new sampling frequency
        sampling_rate <- downsample.to

        # convert the desired downsample rate to time interval in seconds
        downsample_interval <- 1 / downsample.to

        # round datetime to the nearest downsample interval (seconds explicit, so the
        # binning never depends on difftime's auto-chosen units)
        first_time <- individual_data$datetime[1]
        individual_data[, datetime := first_time + floor(as.numeric(datetime - first_time, units = "secs") / downsample_interval) * downsample_interval]

        # define columns
        orientation_cols <- c("roll", "pitch", "heading")
        numeric_cols <- setdiff(metrics, orientation_cols)

        # aggregate numeric metrics using arithmetic mean
        processed_data <- individual_data[, lapply(.SD, mean, na.rm=TRUE), by = datetime, .SDcols = numeric_cols]

        # aggregate orientation metrics: roll and heading wrap (circular mean), but
        # pitch is bounded [-90, 90] and does not wrap (ordinary mean)
        processed_roll <- individual_data[, .(roll = .circularMean(roll, range = c(-180, 180))), by = datetime]
        processed_pitch <- individual_data[, .(pitch = mean(pitch, na.rm = TRUE)), by = datetime]
        processed_heading <- individual_data[, .(heading = .circularMean(heading, range = c(0, 360))), by = datetime]

        # combine aggregated datasets
        processed_data <- Reduce(function(x, y) merge(x, y, by = "datetime", sort = FALSE),
                                 list(processed_data, processed_roll, processed_pitch, processed_heading))

        # sum burst swimming events (based on specified percentiles)
        if(!is.null(burst.quantiles)){
          burst_cols <- paste0("burst", burst.quantiles * 100)
          processed_bursts <- individual_data[, lapply(.SD, function(x) as.integer(sum(as.numeric(x), na.rm = TRUE) > 0)), by = datetime, .SDcols = burst_cols]
          # combine the two aggregated datasets
          processed_data <- merge(processed_data, processed_bursts, by = "datetime", all.x = TRUE)
        }

        # re-add ID column
        processed_data[, ID := id]

        # clean up
        objs_to_remove <- c("processed_roll", "processed_pitch", "processed_heading", "processed_bursts")
        rm(list = intersect(objs_to_remove, ls()))
      }

    } else{
      # if no downsampling rate is defined, return the original sensor data
      processed_data <- individual_data
    }

    # diagnostic: the downsampling outcome (rows before -> after), when a resample actually happened
    if (lvl >= 2L && sampling_rate < sampling_freq) {
      diag["downsample"] <- sprintf("downsample: %s \u2192 %s rows",
                                    .formatLargeNumber(n_input), .formatLargeNumber(nrow(processed_data)))
    }

    # reorder columns: ID, metrics, burst.quantiles (if present)
    # (intersect keeps only columns that exist, supporting partial sensor sets)
    final_order <- c("ID", "datetime", metrics,
                     if(!is.null(burst.quantiles)) paste0("burst", burst.quantiles * 100))
    data.table::setcolorder(processed_data, intersect(final_order, names(processed_data)))



    ############################################################################
    # Store processed data #####################################################
    ############################################################################

    # Define sensor-specific rounding rules (units in brackets).
    #
    # Rounding does NOT reduce in-memory size - a rounded double is still 8 bytes. It reduces the
    # SERIALISED size (repeated values compress well: ~3x smaller .rds), which is why it is applied
    # here, at the storage step, after every metric has been computed at full precision.
    #
    # Choosing the digits: the quantum must sit BELOW the channel's own per-sample noise, so that the
    # noise dithers the quantiser and later averaging still recovers sub-quantum detail. A quantum at
    # or above the noise makes the error systematic (samples snap the same way) and no downstream
    # smoothing can undo it. Measure it against the noise of the series actually STORED, not the raw
    # sensor LSB.
    #
    # `depth` is no longer smoothed before storage (it was, and the 10 s mean of ~200 dithered counts
    # was what justified 2 dp). It is now drift-corrected raw, so the relevant scale is the sensor
    # quantum itself - 6.2 cm on CATS, 0.5 m on a Wildlife Computers archive. 2 dp (1 cm) still sits
    # below the finest of those, so the channel is stored losslessly against its own instrument and
    # the compression argument is unchanged; the DOWNSAMPLE mean (default 1 Hz) supplies the dithering
    # that the smoother used to.
    rounding_specs <- list(
      # raw sensor data
      accelerometer = list(vars = c("ax", "ay", "az"), digits = 4),   # [g]
      gyroscope = list(vars = c("gx", "gy", "gz"), digits = 2),       # [rad/s] - NOT deg/s
      magnetometer = list(vars = c("mx", "my", "mz"), digits = 2),    # [uT]
      # processed metrics
      temperature = list(vars = "temp", digits = 2),                  # [degrees C]
      depth = list(vars = "depth", digits = 2),                       # [m]
      # odba/vedba are sums of the 4 dp surge/sway/heave below; storing them coarser than their own
      # inputs was the dominant error in the dynamics chain and produced quantile-threshold ties.
      dynamics = list(vars = c("accel", "odba", "vedba"), digits = 4),# [g]
      # jerk is a derivative (g/s), so its per-sample noise floor scales with the native rate; 3 dp keeps
      # the stored (mean-aggregated) series above that floor while resolving the mean-jerk activity level.
      jerk = list(vars = "jerk", digits = 3),                         # [g/s]
      orientation = list(vars = c("roll", "pitch", "heading"), digits = 2), # [degrees]
      movement = list(vars = c("surge", "sway", "heave"), digits = 4),# [g]
      # 3 dp: measured noise floor of the stored series is 0.0018-0.0024 m/s, so 0.01 m/s was above it
      # and silenced sustained slow drift (<0.005 m/s, i.e. gliding / buoyancy regulation). 4 dp would
      # sit below the noise floor and store nothing but noise.
      velocity = list(vars = "vertical_velocity", digits = 3)         # [m/s]
    )

    # apply rounding (only to columns that are present)
    for (group in rounding_specs) {
      vars <- intersect(group$vars, names(processed_data))
      if (length(vars) > 0) {
        processed_data[, (vars) := lapply(.SD, round, digits = group$digits), .SDcols = vars]
      }
    }

    # convert NaN to NA
    processed_data[, (names(processed_data)) := lapply(.SD, function(x) {x[is.nan(x)] <- NA; return(x)})]


    # restore the original attributes
    for (attr_name in names(original_attributes)) {
      attr(processed_data, attr_name) <- original_attributes[[attr_name]]
    }

    # update the consolidated metadata (the SINGLE source of provenance - no parallel flat attributes):
    # record the realised sampling rates / sensors present / declination as structured fields, append a
    # full processing-step record (parameters + results) to the audit trail, and re-class as nautilus_tag.
    meta <- .getMeta(processed_data)
    if (!is.null(meta)) {
      meta$sensors$sampling_hz_original  <- sampling_freq
      meta$sensors$sampling_hz_processed <- sampling_rate
      meta$sensors$present <- intersect(.sensorChannels(), names(processed_data))
      meta$sensors$heading_denoise_window  <- heading_denoise_used           # paddle-wheel de-noise applied
      meta$sensors$paddle_contaminated     <- if (!is.null(paddle_state)) isTRUE(paddle_state$present) else NA
      meta$deployment$magnetic_declination <- declination_deg %||% NA_real_
      meta$deployment$heading_reference     <- heading_ref
      meta$mag_calibration <- mag_state                                      # the single source of truth for calibration state
      meta <- .appendProcessing(meta, "processTagData",
                                orientation_algorithm   = orientation.algorithm,
                                madgwick_beta           = if (orientation.algorithm == "madgwick") madgwick.beta else NA_real_,
                                hard_iron               = hard.iron.calibration,
                                soft_iron               = soft.iron.calibration,
                                hard_iron_applied       = hard_iron_applied,
                                soft_iron_applied       = soft_iron_applied,
                                hard_iron_offset_uT     = hard_iron_offset_mag,
                                calibration_source      = calibration_source,
                                magnetic_declination    = declination_deg %||% NA_real_,
                                heading_reference       = heading_ref,
                                heading_denoise_window  = heading_denoise_used,
                                paddle_freq_hz          = if (!is.null(paddle_state)) paddle_state$freq else NA_real_,
                                static_window           = static.window,
                                dba_smoothing           = dba.smoothing %||% NA_real_,
                                orientation_smoothing   = orientation.smoothing %||% NA_real_,
                                speed_smoothing         = speed.smoothing %||% NA_real_,
                                depth_smoothing         = depth.smoothing %||% NA_real_,
                                pitch_offset_deg        = pitch_offset_deg %||% NA_real_,
                                pitch_offset_r2         = pitch_offset_r2 %||% NA_real_,
                                roll_offset_deg         = roll_offset_deg %||% NA_real_,
                                roll_offset_estimate_deg = roll_offset_estimate,
                                roll_offset_status      = roll_offset_status,
                                median_pitch_deg        = median_pitch,
                                median_roll_deg         = median_roll,
                                orientation_warning_threshold = orientation.warning.threshold,
                                mount_roll_max          = mount.roll.max,
                                pitch_offset_min_r2     = pitch.offset.min.r2,
                                pitch_anomaly_detected  = pitch_anomaly_detected,
                                roll_mount_unusual      = roll_mount_unusual,
                                roll_anomaly_detected   = roll_anomaly_detected,
                                attachment_site         = meta$deployment$attachment_site %||% NA_character_,
                                downsample_to           = downsample.to %||% NA_real_,
                                n_input                 = n_input,
                                n_output                = nrow(processed_data))
      # depth zero-offset drift correction: its own lean record (skipped when the method is disabled)
      if (!is.null(drift_res) && !identical(drift_res$status, "disabled")) {
        dd_args <- list(params  = list(method = depth.control$method,
                                       surface_evidence = depth.control$surface.evidence,
                                       min_dry_duration_s = depth.control$min.dry.duration,
                                       max_gap_h = depth.control$max.gap),
                        status    = drift_res$status,
                        n_anchors = drift_res$n_anchors,
                        outcome   = drift_res$outcome)
        if (nrow(drift_res$low_confidence)) dd_args$details <- list(low_confidence = drift_res$low_confidence)
        meta <- do.call(.appendProcessing, c(list(meta, "depth_drift"), dd_args))
      }
      processed_data <- .restoreMeta(processed_data, meta)
    }


    # save the processed data as an RDS file (only when an output directory is provided)
    saved_to <- .saveOutput(processed_data, id, output.dir = output.dir, output.suffix = output.suffix, compress = compress)
    saved[i] <- list(saved_to)                  # single-bracket keeps the slot (a NULL path must not shrink the list)

    # emit the collected diagnostics as one ordered block (detailed level only): the tag attributes
    # line, then each finding in pipeline order. Slots left unset (e.g. speed when not requested)
    # are simply skipped, so the block always reflects exactly what happened for this deployment.
    if (lvl >= 2L) {
      # the tag identity and re-processing alert were emitted up front (right after the header); this
      # block is the findings, in pipeline order. Jerk sits immediately after motion (related kinematics).
      for (k in c("input", "calibration", "denoise", "orientation", "offsets",
                  "motion", "jerk", "depthdrift", "depth", "speed", "downsample")) {
        if (!is.na(diag[k])) say(diag[[k]])
      }
    }

    # curated per-ID outcome. Detailed: a minimal tick (the breakdown is in the block above). Normal: a
    # compact one-line summary per tag (id . channels . rows . Hz), since the detail block is suppressed.
    if (lvl >= 2L) {
      if (!is.null(saved_to)) .log_ok(lvl, "saved ", basename(saved_to))
      else                    .log_ok(lvl, id, " processed")
    } else {
      n_chan <- length(intersect(.sensorChannels(), names(processed_data)))
      b <- cli::symbol$bullet
      .log_ok(lvl, id, " ", b, " ", n_chan, " channel", if (n_chan != 1) "s", " ", b, " ",
              .formatLargeNumber(nrow(processed_data)), " rows ", b, " ", sampling_rate, " Hz")
    }
    tot_in <- tot_in + n_input
    tot_out <- tot_out + nrow(processed_data)
    tot_secs <- tot_secs + .tagSpanSeconds(processed_data$datetime)
    n_done <- n_done + 1L
    .log_gap(lvl)                          # blank line separates this individual's block from the next


    # store processed sensor data in the results list if needed
    if (return.data) {
      data_list[[i]] <- processed_data
    }

    # accumulate this deployment's diagnostic bundle (mag + depth + pitch/roll)
    if (collect_diag) {
      pr_diag <- if (!is.null(pitch_diag) || !is.null(roll_diag)) list(pitch = pitch_diag, roll = roll_diag) else NULL
      diag_bundles[[length(diag_bundles) + 1L]] <- list(id = id, paddle = isTRUE(imeta$tag$paddle_wheel),
                                                        mag = mag_diag, depth = depth_diag, pitchroll = pr_diag)
    }

    # drop references before the next iteration (R reclaims memory automatically;
    # an explicit gc() every iteration would only slow the loop down)
    rm(individual_data)
    rm(processed_data)
  }

  # ORDERING-GUARD warning (fires at any verbosity): computed orientation is only valid on axis-mapped
  # data. Emitted once with the affected ids rather than once per deployment.
  .warn_grouped("{length(unoriented_ids)} deployment{?s} {?was/were} processed without an applied axis mapping.",
                items = unoriented_ids, style = "inline")

  # zero-correction warning: a requested magnetometer received NO hard/soft-iron correction (neither a
  # trusted stored fit nor a coverage-passing inline estimate). Heading is then computed from a raw field
  # still carrying the tag's hard-iron offset - the dominant source of dead-reckoning drift. Loud by default.
  .warn_grouped("{length(uncalibrated_ids)} deployment{?s} {?has/have} no magnetometer calibration.",
                items = uncalibrated_ids, style = "inline")

  # one consolidated notice for every deployment whose imported paddle channel turned out to be constant.
  # Warned rather than logged because dropping an imported channel changes the data, and consolidated
  # rather than per-deployment so a large batch does not drown in identical messages.
  .warn_grouped("{length(dead_paddle_ids)} deployment{?s} had a constant paddle channel, now set to NA.",
                items = dead_paddle_ids, style = "inline")

  # one consolidated notice for every deployment that had already been through processTagData - the common
  # cause is pointing the function at an already-processed output folder. Re-running is idempotent, so this
  # is a warning, not an error, but it is almost always a mistake worth catching in a batch run.
  .warn_grouped("{length(reprocessed_ids)} deployment{?s} had already been processed and {?was/were} re-run.",
                items = reprocessed_ids, style = "inline")

  # render the opt-in per-deployment diagnostic PDF (correction QC) from the gathered bundles. A rendering
  # failure must never discard the (expensive) processed data - warn and return it, don't abort the run.
  if (collect_diag && length(diag_bundles))
    tryCatch(.renderProcessingDiagnostic(diag_bundles, plot = plot, plot.file = plot.file),
             error = function(e) warning("processTagData: diagnostic rendering failed (", conditionMessage(e),
                                         "); returning the processed data anyway.", call. = FALSE))


  ##############################################################################
  # Return processed data ######################################################
  ##############################################################################

  # final summary
  # A magnetic heading looks exactly like a geographic one - same column, same units, same range - so the
  # only thing standing between it and a silently rotated track is this warning plus the recorded
  # `heading_reference`. Name the deployments at any verbosity.
  # ---- end-of-run warnings: ONE per finding type, never one per deployment -------------------------
  # Every group has the same two-line shape - a headline that says what happened and how many, then the
  # affected ids inline with the value that differs. The explanations that used to be repeated here for
  # every group live in the per-deployment verbose block (which states them where they happened) and in
  # the Diagnostics section of this function's documentation.
  .warn_grouped("{length(pitch_anom_items)} deployment{?s} show{?s/} a potential pitch anomaly (median pitch).",
                items = pitch_anom_items, style = "inline")

  .warn_grouped("{length(roll_uncorr_items)} deployment{?s} {?has/have} an unusual mounting roll, NOT corrected (offset).",
                items = roll_uncorr_items, style = "inline")

  .warn_grouped("{length(roll_mount_items)} deployment{?s} {?has/have} an unusual mounting roll, corrected (offset).",
                items = roll_mount_items, style = "inline")

  .warn_grouped("{length(roll_resid_items)} deployment{?s} {?has/have} a roll residual after correction (median roll).",
                items = roll_resid_items, style = "inline")

  .warn_grouped("{length(nodecl_ids)} deployment{?s} {?has/have} a magnetic heading (no position for the declination).",
                items = nodecl_ids, style = "inline")

  # Deployments set aside for missing/unusable input are announced at ANY verbosity: a silent skip in a
  # large batch is how a cohort quietly shrinks between pipeline steps.
  .warn_grouped("{length(skipped_ids)} deployment{?s} {?was/were} skipped for missing or unusable input.",
                items = skipped_ids, style = "inline")

  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, n_done, " of ", n_animals, " tag", if (n_animals != 1) "s", " processed")
    # scale of the batch. Stored rows are quoted against the INPUT rows because downsample.to (1 Hz by
    # default) makes the two differ by one to two orders of magnitude - without the comparison the drop
    # reads as data loss rather than the intended reduction.
    if (n_done > 0)
      .log_arrow(lvl, "total rows: ", .formatLargeNumber(tot_out),
                 " (from ", .formatLargeNumber(tot_in), " input) \u00b7 duration: ", .fmt_duration(tot_secs))
    if (!is.null(output.dir)) .log_arrow(lvl, "output: ", output.dir)
    .log_runtime(lvl, start.time)
  }

  # return the processed data (named by ID) or, when return.data = FALSE, the written .rds paths.
  # `keep` drops skipped slots (their id stays NA), keeping data_list / saved / ids index-aligned.
  keep <- !is.na(ids)
  .collectOutput(data_list[keep], saved[keep], return.data, ids[keep])

}

#######################################################################################################
#######################################################################################################
#######################################################################################################
