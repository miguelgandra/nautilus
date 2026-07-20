#######################################################################################################
# Function to process archival tag data ###############################################################
#######################################################################################################

#' Process Archival Tag Data
#'
#' @description
#' This function processes high-resolution archival tag data, automatically computing a
#' wide range of kinematic and orientation metrics from accelerometer, magnetometer,
#' and gyroscope signals (see the *Details* section below for a complete list).
#'
#' Orientation is estimated by default using the tilt-compensated compass method, which fuses accelerometer
#' and magnetometer data to determine body orientation relative to gravity and magnetic north.
#' Optionally, a more advanced sensor fusion approach using the Madgwick filter can be applied.
#'
#' Magnetometer calibration can be applied prior to orientation estimation, using the SAME engine as
#' \code{\link{calibrateMagnetometer}} (see its Details for the full pipeline and the maths). When the field
#' cloud is genuinely three-dimensional it fits the full hard + soft iron ellipsoid (correcting cross-axis
#' misalignment); for the usual thin swimming band it estimates the hard-iron centre only, pinning the
#' unobservable perpendicular component from the geomagnetic inclination and leaving the soft-iron at
#' identity. Either way the correction is applied ONLY when it clears an honest confidence gate - if the
#' animal did not rotate through enough headings to constrain the estimate, the heading is left raw rather
#' than distorted. A calibration already computed and stored by \code{\link{calibrateMagnetometer}} (e.g. a
#' per-package pooled or externally-sourced fit) is reused when present and trusted; otherwise the fit is
#' estimated inline. The switches are grouped in \code{\link{calibrationControl}}.
#'
#' For tags equipped with a magnetic paddle wheel, the function can also estimate swimming speed.
#' This is achieved by extracting the dominant rotation frequency from the magnetometer's
#' z-axis signal using a Fast Fourier Transform (FFT), which is then converted
#' to speed using a tag-specific calibration slope
#'
#' After metric computation, the data can be downsampled to reduce its resolution and
#' size for downstream analysis.
#'
#' @param data A list of data.tables/data.frames, one for each individual; a single aggregated data.table/data.frame
#' containing data from multiple animals (with an 'ID' column); or a character vector of file paths pointing to
#' `.rds` files, each containing data for a single individual. When a character vector is provided,
#' files are loaded sequentially to optimize memory use. The output of the \link{importTagData} function
#' is strongly recommended, as it formats the data appropriately for all downstream analysis. The IMU
#' axes are expected to be in the animal body frame: run \link{applyAxisMapping} first, otherwise the
#' orientation metrics (pitch/roll/heading) are silently wrong - a warning is raised for any deployment
#' whose axis mapping has not been applied.
#' @param downsample.to Numeric. Downsampling frequency in Hz (e.g., 1 for 1 Hz) to reduce data resolution.
#' Use NULL to retain the original resolution. Defaults to 1.
#' @param orientation.algorithm Orientation estimation algorithm:
#'  \itemize{
#'    \item \code{"tilt_compass"} (default): Lightweight 6-axis tilt-compensated compass.
#'    \item \code{"madgwick"}: High-accuracy 9-axis sensor fusion via a native-R implementation
#'      of Madgwick's gradient-descent AHRS filter (requires gyroscope data).
#'  }
#' @param orientation A control object from \code{\link{orientationControl}} grouping the specialised
#'   orientation-estimation knobs: the Madgwick filter gain `madgwick.beta` (used only when
#'   `orientation.algorithm = "madgwick"`), the mounting-offset corrections `correct.pitch` /
#'   `correct.roll`, the pitch-offset fit gate `pitch.offset.min.r2`, and the anomaly `warning.threshold`
#'   (degrees, above which a median |pitch|/|roll| is flagged and an implausibly large offset is not
#'   applied). Defaults to all on. Pass a named list to override some, e.g.
#'   `orientation = orientationControl(correct.roll = FALSE)`. The pitch-offset correction is adapted from
#'   Kawatsu et al. (2010); the roll offset is the median roll over the most level half of the record
#'   (mirror-imaged for left/right attachment sites, captured empirically).
#' @param calibration A control object from \code{\link{calibrationControl}} grouping the magnetometer
#'   calibration switches `hard.iron` and `soft.iron`. Defaults to both on. Pass a named list to override,
#'   e.g. `calibration = calibrationControl(soft.iron = FALSE)`.
#' @param smoothing A control object from \code{\link{smoothingControl}} grouping the windows
#'   (seconds): `static` (3, the gravity-separation window underlying VeDBA/ODBA, surge/sway/heave and
#'   orientation - it cannot be disabled), and the post-smoothers `orientation` (1), `dba` (2),
#'   `depth` (10), `speed` (1). Set any post-smoother to `NULL` to disable it, e.g.
#'   `smoothing = smoothingControl(depth = 15)`.
#' @param depth.drift A control object from \code{\link{depthDriftControl}} governing the depth zero-offset
#'   drift correction. The slowly-varying pressure-sensor zero offset is estimated from independent surface
#'   evidence (the Wildlife Computers wet/dry signal in `meta$ancillary$dry` and surface-implying
#'   position fixes) and subtracted before depth feeds the vertical velocity and every absolute-depth
#'   metric. On by default and self-gating: it abstains (leaving depth untouched) when surface evidence
#'   is too sparse. Set `depth.drift = depthDriftControl(method = "none")` to disable it.
#' @param paddle.calibration A data.frame of paddle-wheel calibration values. Supplying it is what
#' enables paddle-wheel speed estimation (there is no separate on/off flag); leave it `NULL` to skip
#' speed. Must contain at least three columns:
#'  \itemize{
#'    \item \code{year}: The year the calibration was performed (integer)
#'    \item \code{package_id}: The package identifier matching the tag's attribute (character)
#'    \item \code{slope}: The calibration slope value (numeric)
#'  }
#' \code{\link{imputePaddleCalibration}} builds a complete, gap-free table of this form from a set of
#' measured calibrations, projecting slopes for tag-years that were never calibrated.
#' @param burst.quantiles Numeric vector. Quantiles (0-1) of instantaneous VeDBA used to flag burst
#' swimming events. Each quantile is a **relative, per-deployment** threshold: it always flags the top
#' `1 - q` fraction of that record's samples (e.g. 0.95 flags the most active 5%), not an absolute
#' activity level, so flags are not comparable in magnitude across deployments. Use NULL to disable
#' burst detection. Defaults to c(0.95, 0.99) (95th and 99th percentiles).
#' @param plot Logical. If `TRUE`, render the per-deployment correction-diagnostic pages to the active
#'   graphics device. Intended for a single deployment; for a batch, prefer `plot.file` (a `TRUE` value
#'   with more than one deployment warns and floods the device). Defaults to `FALSE` (fully headless).
#' @param plot.file Optional path to a single multi-page PDF. When supplied, `processTagData()` gathers a
#'   compact, decimated diagnostic bundle for each deployment **while the raw data is in memory** and, after
#'   processing, writes a QC report so each correction can be visually verified: the magnetometer
#'   calibration (the raw point cloud collapsing onto a sphere), the depth zero-offset drift, and the
#'   pitch/roll mounting-offset fit. The heavy processing itself stays headless; nothing is rendered when
#'   this is `NULL` (default). Must end in `.pdf`.
#' @param return.data Logical. Return the processed data in memory (default `TRUE`). When `FALSE`, the
#'   function instead returns the paths of the `.rds` files it wrote, which feed directly into the next
#'   step's `data` argument -- so a large fleet can be processed without ever holding it all in memory.
#'   `return.data = FALSE` therefore requires an `output.dir`.
#' @param output.dir Character. Directory in which to write one `<id>.rds` file per deployment. Providing
#'   a directory is what triggers saving; `NULL` (default) writes nothing. The directory must already exist.
#' @param output.suffix Character. Optional suffix appended to each saved file name (before `.rds`), e.g.
#'   to tag a processing run or avoid clashes. Only used when `output.dir` is set. Default `NULL`.
#' @param compress Compression for the saved `.rds` files (only used when `output.dir` is set): `TRUE`
#'   (default, gzip), `FALSE`, or one of `"gzip"`/`"bzip2"`/`"xz"`. See \code{\link[base]{saveRDS}}.
#' @param data.table.threads Integer or NULL. Specifies the number of threads
#' that data.table should use for parallelized operations. NULL (default): Uses data.table's current default threading.
#' Notes:
#' \itemize{
#'    \item Optimal thread count depends on your CPU cores and data size
#'    \item More threads use more RAM but can significantly speed up large operations
#'    \item Can be permanently set via \code{data.table::setDTthreads()}
#'    \item Current thread count: \code{data.table::getDTthreads()}
#'  }
#' @param verbose Verbosity level: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal" (header, per-ID
#' outcome, summary), or `2`/"detailed" (default; adds the full per-step processing log).
#' Defaults to `"detailed"`.

#' @details
#' This function computes a suite of movement and orientation metrics from high-frequency tri-axial sensor data,
#' including acceleration, orientation, and linear motion parameters. It also performs automatic magnetic calibration
#' to improve heading estimation.
#'
#' \strong{Acceleration:}
#' \itemize{
#'   \item Total Acceleration (g): The total magnitude of the animal's acceleration, calculated from the three orthogonal accelerometer components.
#'   \item Vectorial Dynamic Body Acceleration (VeDBA) (g): Quantifies the physical acceleration of the animal, calculated as the vector magnitude of the dynamic body acceleration, which is the difference between raw accelerometer data and the moving average (static acceleration).
#'   \item Overall Dynamic Body Acceleration (ODBA) (g): A scalar measure of the animal's overall acceleration, calculated as the sum of the absolute values of the dynamic acceleration components along the X, Y, and Z axes. Retained for comparability, but note ODBA is orientation-dependent; \strong{VeDBA is preferred for towed or loosely-attached tags} because it is rotation-invariant.
#'   \item Burst Swimming Events: A binary flag marking the most energetic samples, defined as the top quantile(s) of instantaneous VeDBA (the magnitude of the dynamic, gravity-removed acceleration). VeDBA is used rather than total acceleration because the latter is dominated by the gravity baseline and biased toward gravity-aligned bursts. The threshold is relative (per-deployment): each quantile always flags the same fraction of a record (see \code{burst.quantiles}).
#' }
#'
#' \strong{Orientation:}
#'
#' Computed using sensor fusion algorithms:
#' \itemize{
#'   \item {Tilt-compensated compass} (default): A lightweight 6-axis fusion (accelerometer + magnetometer)
#'   to compute roll, pitch, and heading. The method first calculates tilt angles from accelerometer data,
#'   then compensates the magnetometer readings using these angles to compute a more accurate heading
#'   (as described in Gunner et al., 2021). This approach avoids gyroscope drift but may be affected by magnetic disturbances.
#'   \item {Madgwick filter}: A 9-axis fusion algorithm (accelerometer + gyroscope + magnetometer)
#'   implementing Sebastian Madgwick's quaternion-based gradient descent
#'   approach. This provides absolute orientation reference by incorporating
#'   Earth's magnetic field and is more robust to transient disturbances,
#'   at the cost of higher computational complexity.
#' }
#' Output includes:
#' \itemize{
#'   \item Roll (degrees): Rotational movement of the animal around its longitudinal (x) axis.
#'   \item Pitch (degrees): Rotational movement of the animal around its lateral (y) axis.
#'   \item Heading (degrees): Directional orientation of the animal, representing the compass heading.
#'         Heading is corrected based on the deployment coordinates using global geomagnetic declination models.
#'   \item Turning Angle (degrees): The change in heading between consecutive time points, representing the animal's turning behaviour. Calculated as the minimum angular difference between consecutive headings, constrained between -180 and 180 degrees.
#' }
#'
#' \strong{Linear Motion:}
#' \itemize{
#'   \item Surge (g): The forward-backward linear movement of the animal along its body axis, derived from the accelerometer data.
#'   \item Sway (g): The side-to-side linear movement along the lateral axis of the animal, also derived from the accelerometer data.
#'   \item Heave (g): The vertical linear movement of the animal along the vertical axis, estimated from accelerometer data.
#'   \item Vertical Velocity (m/s): The rate of change in the animal's depth over time, including direction (positive for descent, negative for ascent).
#'   Vertical velocity is calculated using a central difference method on a smoothed copy of the depth
#'   series (window set by \code{smoothing$depth}); differentiating a raw pressure trace would amplify
#'   its quantisation noise. The stored \code{depth} column itself is drift-corrected but NOT smoothed,
#'   so short vertical excursions keep their true amplitude.
#'   An optional secondary smoothing step can be applied to the resulting velocity time series (see \code{speed.smoothing}).
#' }
#'
#' @return If \code{return.data = TRUE}, a list where each element contains the processed sensor data for
#' an individual (named by ID); if \code{return.data = FALSE}, a character vector of the written \code{.rds}
#' file paths. Files are written to disk whenever \code{output.dir} is set.
#'
#' @references
#' Gunner RM, Holton MD, Scantlebury MD, *et al.* (2021) Dead-reckoning animal
#' movements in R: a reappraisal using Gundog. *Animal Biotelemetry*. 9:1-37.
#' \doi{10.1186/s40317-021-00245-z}
#'
#' Kawatsu S, Sato K, Watanabe Y, Hyodo S, Breves JP, Fox BK, *et al.* (2009).
#' A new method to calibrate attachment angles of data loggers in swimming sharks.
#' *EURASIP Journal on Advances in Signal Processing*. 2010, 732586.
#' \doi{10.1155/2010/732586}
#'
#' @seealso \link{importTagData}, \link{filterDeploymentData}.
#' @examples
#' \dontrun{
#' # Axis-map the imported data into the body frame first, then reconstruct
#' # kinematics/orientation and downsample to 1 Hz.
#' oriented <- applyAxisMapping(imported)
#' tag <- processTagData(oriented,
#'                       downsample.to = 1,
#'                       orientation.algorithm = "tilt_compass",
#'                       paddle.calibration = paddle_cal)
#'
#' # Batch of saved deployments: write a QC diagnostic PDF and save incrementally.
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
      .assert_columns(data[[nm]], required_cols, sprintf("data[['%s']]", nm))
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
  for (i in seq_along(data)) {

    ############################################################################
    # load data for the current individual if using file paths #################
    if (is_filepaths) {

      # get current file path
      file_path <- data[i]

      # load current file
      individual_data <- readRDS(file_path)

      # perform checks specific to loaded RDS files
      missing_cols <- setdiff(required_cols, names(individual_data))
      if (length(missing_cols) > 0) .abort("Missing required column(s) in {.file {basename(file_path)}}: {.val {missing_cols}}.")
      if (!inherits(individual_data$datetime, "POSIXct")) .abort("The datetime column in {.file {basename(file_path)}} must be of class {.cls POSIXct}.")
      if (is.null(attr(individual_data, "nautilus.version"))) {
        cli::cli_warn(c("File {.file {basename(file_path)}} was likely not processed via {.fn importTagData}.",
                        "i" = "Run it through {.fn importTagData} first to ensure correct formatting."))
      }

    ############################################################################
    # data is already in memory (list of data frames/tables) ###################
    } else {

      # access the individual dataset
      individual_data <- data[[i]]
    }

    # skip NULL or empty elements before any logging or ID access
    if (is.null(individual_data) || length(individual_data) == 0) next

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

    # ORDERING GUARD: orientation (pitch/roll/heading) assumes the IMU axes are already in the animal
    # body frame. If applyAxisMapping() was not run, that assumption is silently violated - flag it.
    if (!isTRUE(imeta$axis_mapping$applied)) unoriented_ids <- c(unoriented_ids, as.character(id))

    # ensure data is ordered by datetime
    data.table::setorder(individual_data, datetime)

    # calculate sampling frequency (rounded to whole Hz; the windowing below assumes >= 1 Hz)
    sampling_freq <- nrow(individual_data) / length(unique(lubridate::floor_date(individual_data$datetime, "sec")))
    sampling_freq <- round(sampling_freq)
    if (!is.finite(sampling_freq) || sampling_freq < 1) {
      .abort(c("Estimated sampling frequency for {.val {id}} is below 1 Hz ({sampling_freq} Hz).",
               "i" = "{.fn processTagData} requires at least 1 Hz; check the timestamps or regularize the series first."))
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
        if (lvl >= 1L) say("i magnetometer already calibrated - skipping re-application (idempotent)")
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
        dep_r <- range(individual_data$depth, na.rm = TRUE)
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
    individual_data[, depth := .vv$depth]
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
        if (!is.na(valid_idx)) {
          deploy_info <- data.frame(datetime=individual_data$datetime[valid_idx],
                                    lon = individual_data$lon[valid_idx],
                                    lat = individual_data$lat[valid_idx])
        } else {
          .abort("No valid location found to estimate magnetic declination.")
        }
      }

      # get magnetic declination value (in degrees)
      declination_deg <- oce::magneticField(longitude=deploy_info$lon, latitude=deploy_info$lat, time=deploy_info$datetime)$declination
      declination_deg <- round(declination_deg, 2)

      # apply magnetic declination correction to convert from magnetic north to geographic north
      individual_data[, heading := (heading + declination_deg) %% 360]

    }else{

      # skip declination correction
      declination_deg <- NULL

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
          off_pitch <- sprintf("pitch %+.2f\u00b0 (R\u00b2 %.2f)", pitch_offset_deg, pitch_offset_r2)
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

    if (correct.roll.offset) {

      if (all(is.na(individual_data$roll))) {
        roll_offset_deg <- NULL

      } else {

        # smoothed vertical velocity, to isolate steady (near-level) swimming
        vv_window <- win(10)
        individual_data[, vv_smooth := data.table::frollmean(vertical_velocity, n = vv_window, fill = NA, align = "center")]

        # the more level half of the record (smallest |vertical velocity|)
        horiz_cut <- stats::median(abs(individual_data$vv_smooth), na.rm = TRUE)
        roll_offset_deg <- stats::median(individual_data[abs(vv_smooth) <= horiz_cut & !is.na(roll), roll], na.rm = TRUE)
        # diagnostic: the PRE-correction level-swimming roll + the computed median (kept even if the gate rejects)
        roll_level_samp <- if (collect_diag) individual_data[abs(vv_smooth) <= horiz_cut & !is.na(roll), roll] else NULL
        roll_median_all <- roll_offset_deg

        # only apply if finite and below the warning threshold
        roll_applied <- is.finite(roll_offset_deg) && abs(roll_offset_deg) < orientation.warning.threshold
        if (roll_applied) {
          # subtract the offset and re-wrap roll into [-180, 180]
          individual_data[, roll := ((roll - roll_offset_deg + 180) %% 360) - 180]
          off_roll <- sprintf("roll %+.2f\u00b0", roll_offset_deg)
        } else {
          roll_offset_deg <- NULL
        }
        if (collect_diag)
          roll_diag <- .captureRollDiag(roll_level_samp, roll_median_all, roll_applied, orientation.warning.threshold)

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
    median_pitch <- NA_real_; median_roll <- NA_real_

    # only check if pitch contain non-NA values
    if (!all(is.na(individual_data$pitch))) {
      median_pitch <- median(individual_data$pitch, na.rm = TRUE)
      if (abs(median_pitch) > orientation.warning.threshold) {
        .log_skip(lvl, "potential pitch anomaly: median = ", round(median_pitch, 1), "\u00B0")
        pitch_anomaly_detected <- TRUE
        warning(sprintf("%s - Potential pitch anomaly detected (%.2f\u00b0)", id, median_pitch), call. = FALSE)
      }
    }

    # only check if roll contain non-NA values
    if (!all(is.na(individual_data$roll))) {
      median_roll <- median(individual_data$roll, na.rm = TRUE)
      if (abs(median_roll) > orientation.warning.threshold) {
        .log_skip(lvl, "potential roll anomaly: median = ", round(median_roll, 1), "\u00b0")
        roll_anomaly_detected <- TRUE
        warning(sprintf("%s - Potential roll anomaly detected (%.2f\u00b0)", id, median_roll), call. = FALSE)
      }
    }

    # diagnostics: orientation (method, posture medians, heading availability) and applied offsets
    if (lvl >= 2L) {
      hd <- paste0("heading ", if (heading_ok) "ok" else "NA")
      diag["orientation"] <- if (is.na(orient_method)) "orientation: insufficient sensor data"
        else if (is.finite(median_pitch))
          sprintf("orientation: median pitch %.1f\u00b0 \u00b7 roll %.1f\u00b0 \u00b7 %s", median_pitch, median_roll, hd)
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
                 "accel","odba","vedba","roll", "pitch", "heading",
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
                                median_pitch_deg        = median_pitch,
                                median_roll_deg         = median_roll,
                                orientation_warning_threshold = orientation.warning.threshold,
                                pitch_offset_min_r2     = pitch.offset.min.r2,
                                pitch_anomaly_detected  = pitch_anomaly_detected,
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
      if (!is.null(attrs_line)) cli::cli_text("{cli::symbol$bullet} {attrs_line}")
      for (k in c("input", "calibration", "denoise", "orientation", "offsets",
                  "motion", "depthdrift", "depth", "speed", "downsample")) {
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
  if (length(unoriented_ids)) {
    cli::cli_warn(c(
      "{length(unoriented_ids)} tag{?s} processed without an applied axis mapping: {.val {utils::head(unoriented_ids, 8)}}.",
      "i" = "Orientation (pitch/roll/heading) assumes body-frame IMU axes - run {.fn applyAxisMapping} first, unless the data is already in the body frame."))
  }

  # zero-correction warning: a requested magnetometer received NO hard/soft-iron correction (neither a
  # trusted stored fit nor a coverage-passing inline estimate). Heading is then computed from a raw field
  # still carrying the tag's hard-iron offset - the dominant source of dead-reckoning drift. Loud by default.
  if (length(uncalibrated_ids)) {
    cli::cli_warn(c(
      "{length(uncalibrated_ids)} deployment{?s} got NO magnetometer calibration (raw field): {.val {utils::head(uncalibrated_ids, 8)}}.",
      "!" = "Heading carries the uncorrected hard-iron offset; dead-reckoned tracks will drift.",
      "i" = "Provide a dedicated calibration via {.code calibrateMagnetometer(calibration.data=)}, or collect more rotation coverage. Status recorded as {.val uncalibrated_raw} in {.code meta$mag_calibration$status}."))
  }

  # one consolidated notice for every deployment whose imported paddle channel turned out to be constant.
  # Warned rather than logged because dropping an imported channel changes the data, and consolidated
  # rather than per-deployment so a large batch does not drown in identical messages.
  if (length(dead_paddle_ids)) {
    cli::cli_warn(c(
      "{length(dead_paddle_ids)} deployment{?s} had a CONSTANT imported paddle channel, now set to {.val NA}: {.val {utils::head(dead_paddle_ids, 8)}}.",
      "!" = "A channel holding one fixed value is a dead or absent paddle wheel, not a measurement; left in place it would count as that many genuine speed samples in any pooled statistic.",
      "i" = "These deployments simply have no paddle speed. Supply a {.arg paddle.calibration} row so it can be estimated from the magnetometer, or exclude them from speed analyses."))
  }

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
