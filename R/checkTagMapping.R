#######################################################################################################
# Guess IMU Axis Mapping for Multiple Animals/Datasets ################################################
#######################################################################################################

#' Assess sensor-axis mappings from deployment data
#'
#' @description
#' Evaluates candidate signed permutations of the accelerometer axes and identifies mappings
#' consistent with gravity loading, posture and, where informative, diving dynamics. Optional
#' gyroscope and magnetometer diagnostics assess their relationship to the accelerometer frame.
#' Documented configurations can be compared with the inferred mappings without applying them.
#'
#' The function is used on raw-axis data after [filterDeploymentData()] and sensor quality control,
#' before [applyAxisMapping()] and [processTagData()]. It returns deployment-level evidence and
#' proposals, not remapped sensor data. Proposals can be reconciled with [consensusAxisMapping()]
#' and inspected against synchronised footage with [reviewTagMapping()] before application.
#'
#' @param data A \code{nautilus_tag} object, a named list of deployment datasets, a data frame
#'   containing deployments identified by \code{id.col}, or a character vector of \code{.rds}
#'   file paths. Each deployment requires \code{POSIXct} timestamps, depth in metres and all three
#'   accelerometer channels in g. File inputs are read sequentially. Use raw-axis records trimmed
#'   to the on-animal period; see Details.
#' @param id.col Character. Name of the column identifying deployments, not animals. Default
#'   \code{"ID"}.
#' @param datetime.col Character. Name of the \code{POSIXct} timestamp column. Default
#'   \code{"datetime"}.
#' @param depth.col Character. Name of the depth column, in metres and positive downwards.
#'   Default \code{"depth"}.
#' @param ax.col,ay.col,az.col Character. Names of the raw X-, Y- and Z-axis accelerometer
#'   columns, in g. Defaults \code{"ax"}, \code{"ay"} and \code{"az"}.
#' @param configs Optional named list of documented configurations, each a data frame with
#'   \code{from} and \code{to} columns as described in [applyAxisMapping()]. Each deployment's
#'   \code{tag$axis_config} metadata selects the configuration to assess. Configurations are
#'   compared with the data-derived frame, not used to force a solution. Default \code{NULL}.
#' @param deployment.type Mount type used for posture scoring: \code{"rigid"} uses median
#'   absolute roll and pitch, assuming a near-level posture during horizontal swimming;
#'   \code{"towed"} uses their median absolute deviations, allowing persistent mounting offsets.
#'   Supply one value for all deployments or a vector in input order. \code{NULL} (default)
#'   uses each deployment's \code{deployment_type} metadata, with the stability-based scorer
#'   when unknown. This changes posture ranking and associated diagnostics, not the gravity anchor.
#' @param static.threshold Non-negative tolerance, in g, around \code{g.value} for classifying
#'   the magnitude of the smoothed acceleration vector as approximately static. Default \code{0.1}.
#' @param vertical.speed.threshold Non-negative absolute vertical-velocity threshold, in m/s,
#'   for identifying approximately horizontal swimming periods. Default \code{0.5}.
#' @param stable.horizontal.window Non-negative duration, in seconds, over which all observations
#'   must meet the horizontal-swimming criterion. Default \code{10}. If the record is too short
#'   or no stable interval is found, individual horizontal observations are used instead.
#' @param g.value Expected acceleration magnitude under static conditions, in g. Default \code{1}.
#' @param dba.window Non-negative centred rolling-mean window, in seconds, used to estimate
#'   static acceleration. Default \code{2.5}.
#' @param depth.smoothing,vertical.smoothing Non-negative centred windows, in seconds, for the
#'   local linear depth slope (default 5) and its rolling-mean post-smoother (default 1).
#'   `NULL` or zero selects the minimum three-point slope stencil for `depth.smoothing`, or disables
#'   the rate post-smoother for `vertical.smoothing`. The estimator and edge/gap handling are shared
#'   with [processTagData()]; see [smoothingControl()].
#' @param tie.tolerance Non-negative numeric value retained in the API (default \code{2}).
#'   The current gravity-anchored implementation does not use this argument to select candidates
#'   or resolve axes; changing it does not alter the inferred mapping.
#' @param use.dynamics Logical. Use diving dynamics to resolve the horizontal accelerometer
#'   axes and run gyroscope, magnetometer and locomotor-power diagnostics. Default \code{TRUE}.
#'   When \code{FALSE}, inference is restricted to gravity and posture evidence, which generally
#'   leaves surge and sway ambiguous.
#' @param dive.speed.threshold Non-negative minimum absolute vertical velocity, in m/s, for
#'   samples used by the pitch--depth-rate correlation. Default \code{0.2}.
#' @param max.vertical.speed Maximum absolute vertical velocity, in m/s, admitted to that
#'   correlation (default \code{5}). Must exceed \code{dive.speed.threshold}. Faster samples
#'   are omitted from the diagnostic as potentially implausible depth changes; sensor data are
#'   not filtered by this argument.
#' @param min.dynamic.corr Minimum correlation magnitude for accepting the pitch--depth-rate
#'   anchor or the gyroscope tilt-rate estimator. Default \code{0.4}. Additional separation
#'   and signal-availability criteria also apply; this is not a confidence probability.
#' @param locomotor.band Numeric vector \code{c(low, high)} defining the frequency band, in Hz,
#'   for locomotor-power corroboration. Default \code{c(0.2, 3)}. Choose a biologically relevant
#'   band that the sampling rate can resolve, excluding known tether or housing oscillations.
#' @param locomotor.axis Body axis expected to carry the largest locomotor-band power:
#'   \code{"sway"} (default, lateral oscillation), \code{"heave"} (dorsoventral oscillation),
#'   or \code{"surge"} (longitudinal oscillation). This diagnostic corroborates a candidate
#'   but does not determine whether its axes are resolved.
#' @param mag.hard.iron Logical. Estimate and apply a provisional spherical hard-iron offset
#'   for the magnetometer dip diagnostic when orientation coverage is sufficient. Default
#'   \code{TRUE}. This correction is diagnostic only; it is not saved as a calibration or applied
#'   to the returned sensor data.
#' @param plot Logical. Draw the diagnostic report on the active graphics device. Default
#'   \code{FALSE}.
#' @param plot.file Character. Path to a diagnostic PDF, or \code{NULL} (default). The report
#'   contains a run-summary page followed by one page per evaluated deployment. Providing a path
#'   writes the report independently of \code{plot}; the parent directory must exist.
#' @param verbose How much detail to print: \code{0}/\code{"quiet"},
#'   \code{1}/\code{"normal"}, or \code{2}/\code{"detailed"} (default). Normal output reports
#'   deployment-level conclusions; detailed output adds diagnostic steps.
#'
#' @details
#' ## Reference frames and input preparation
#'
#' Axis mapping establishes the relationship between recorded sensor axes and the longitudinal
#' (X, surge), lateral (Y, sway) and dorsoventral (Z, heave) body axes used by [processTagData()].
#' The model is a constant signed permutation: axis exchange and sign inversion, not an arbitrary
#' three-dimensional rotation. Continuous mounting offsets are treated separately during processing.
#'
#' Trim off-animal periods and inspect sensor integrity and quality before inference. A stationary
#' tag on deck or a detached tag can provide gravity and posture evidence unrelated to the animal.
#' Records with a processing history but no [filterDeploymentData()] step produce a warning.
#' Inference on already remapped axes does not recover an absolute raw-to-body mapping.
#'
#' List names and file basenames supply the result identifiers. Keep them consistent with deployment
#' IDs in the sensor data and video metadata. Use the canonical IMU column names throughout the
#' four-function workflow: the application step recognises \code{ax/ay/az}, \code{gx/gy/gz} and
#' \code{mx/my/mz}, and does not rename custom sensor columns automatically.
#'
#' ## Accelerometer evidence
#'
#' The search evaluates 24 proper-rotation signed permutations (determinant +1). For each candidate,
#' smoothed acceleration provides a gravity-loading term and roll/pitch posture terms. The latter
#' are median absolute angles for rigid mounts or median absolute deviations for towed or unknown
#' mounts. Scores rank candidates for presentation; they are not statistical confidence measures.
#'
#' The vertical axis is anchored independently by its signed static gravity loading. Alternative
#' vertical axes within \code{0.3 * g.value} of the strongest loading remain plausible. Horizontal
#' axes are then assessed from the negative relationship between pitch and depth rate during
#' diving: under the package convention, descending motion has positive vertical velocity and
#' nose-down pitch is negative. Acceptance requires sufficient diving samples, correlation strength
#' and separation from competing assignments. Sway is completed by the proper-rotation constraint.
#'
#' An axis is resolved only when every surviving candidate agrees on its source and sign. When
#' vertical loading is ambiguous, diving evidence can narrow candidates within each plausible
#' vertical frame without claiming to resolve heave. The accelerometer proposal remains empty
#' until all three axes are resolved. Locomotor-band power is corroborative only.
#'
#' These are conditional biomechanical assumptions, not universal guarantees. Persistent tag tilt,
#' towing motion, little depth variation, sensor gaps or movement unrelated to longitudinal swimming
#' can leave the frame ambiguous or weaken the pitch--depth-rate relationship. Retain and inspect
#' that uncertainty rather than treating the highest-ranked candidate as an established body frame.
#'
#' ## Gyroscope and magnetometer diagnostics
#'
#' Gyroscope inference starts from the shared-frame assumption: for accelerometer matrix \code{M},
#' angular velocity transforms as \code{det(M) * M}. The extra determinant factor is required
#' because angular velocity is an axial vector. The shared native sensor frame is a hardware
#' assumption, assessed through the gravity-direction identity
#' \code{d(ghat)/dt = -omega x ghat}.
#'
#' When this assumption is consistent with the data, or rotation is insufficient to reject it,
#' the derived gyroscope mapping is retained. A decisive failure triggers independent tilt-rate and
#' gravity-rotation estimators; disagreement leaves the gyroscope unresolved and is flagged for review.
#' A resolved status with insufficient rotation is therefore not independent empirical confirmation.
#'
#' Magnetometer diagnostics test whether a candidate frame yields a sufficiently stable angle
#' between gravity and the magnetic field. Magnetic dip cannot independently resolve heading axes.
#' The common-frame mapping is tested rather than a full independent magnetometer frame being inferred.
#' Deployment location and date, when available, provide an International Geomagnetic Reference Field
#' (IGRF) inclination comparison through \pkg{oce}; an unavailable prediction remains \code{NA}.
#' For tags marked \code{tag$paddle_wheel = TRUE}, magnetometer data are smoothed over three seconds
#' before these diagnostics to reduce rotating-magnet interference.
#'
#' Family diagnostics can be reported for a representative ambiguous accelerometer frame, but gyro
#' and magnetometer rows enter \code{proposal} only when the accelerometer frame is fully resolved.
#' These checks do not replace [calibrateMagnetometer()] or establish a geographic heading reference.
#'
#' ## Documented configurations
#'
#' \code{frame_state$prior$status} is \code{"confirmed"} when the sole surviving accelerometer frame
#' matches the documented configuration; \code{"consistent"} when it is one of several survivors;
#' or \code{"conflict"} when it lies outside that set. A documented reflection (determinant -1)
#' is reported as \code{"reflection"}, not a conflict: it is outside the proper-rotation search.
#' Missing configurations are \code{"absent"} and non-permutation configurations are
#' \code{"unverifiable"}.
#'
#' Explicit gyroscope and magnetometer configuration rows are assessed only when the documented
#' accelerometer frame is confirmed and the corresponding inferred family mapping is available.
#' Their statuses are \code{"confirmed"}, \code{"conflict"}, \code{"unverifiable"} or \code{"absent"}.
#' Configuration conflicts are diagnostic; they never replace the data-derived proposal.
#'
#' ## Computation and unavailable records
#'
#' Low-frequency diagnostics use block-mean data at 1 Hz, or the native rate when lower. A separate,
#' higher-rate copy is retained for locomotor-band power when dynamics are enabled. Sensor units are
#' not converted here; optional gyroscope channels must be in rad/s and magnetometer channels must
#' share consistent magnetic-field units, normally microtesla.
#'
#' Missing required channels, unreadable records, insufficient horizontal data or failed inference
#' leave no entry in the returned mapping list. Skips and failures are reported, while other
#' deployments continue; an entirely unsuccessful run returns an empty list. Structural argument
#' errors and missing input paths can stop the call before deployment-level inference. A missing
#' result is not an identity mapping and should not be interpreted as a verified frame.
#'
#' @return A named list with one element per successfully evaluated deployment, carrying the
#'   \code{nautilus.mapping.producer} attribute. Each element contains:
#'   \describe{
#'     \item{\code{id}, \code{package_id}, \code{logger_id}, \code{tag}, \code{type}}{The result
#'       identifier and metadata grouping keys; unavailable grouping values are \code{NA}.
#'       \code{tag} is the tag model, not a sensor dataset.}
#'     \item{\code{proposal}}{A data frame with \code{from} and \code{to} columns containing
#'       complete, resolved sensor-family mappings for [applyAxisMapping()]. It is empty when
#'       the accelerometer frame is unresolved, even if family diagnostics suggest a mapping.}
#'     \item{\code{resolution}}{A data frame with \code{body_axis}, \code{status}, \code{source},
#'       \code{sign} and \code{evidence}. Each body axis is \code{"resolved"} or \code{"ambiguous"};
#'       unresolved source, sign and evidence values are \code{NA}.}
#'     \item{\code{frame_state}}{The surviving candidate frames, vertical and surge evidence,
#'       gyroscope co-registration result, documented-configuration statuses and conflict messages.
#'       This is the deployment-level evidence used by [reviewTagMapping()].}
#'     \item{\code{families}}{Gyroscope and magnetometer diagnostic lists, with \code{status}
#'       (\code{"resolved"}, \code{"unresolved"}, \code{"inconsistent"} or
#'       \code{"absent/insufficient"}) and a mapping where resolved. Additional fields record
#'       correlation, dip variability, magnetic inclination and provisional hard-iron diagnostics
#'       when available. Angular diagnostics are in degrees.}
#'     \item{\code{confidence}}{Diagnostic summaries: score margin, candidate count, vertical/surge
#'       resolution, surge correlation, number of dynamic samples, locomotor corroboration,
#'       attachment site and an explanatory note. These are not probabilities of correctness.}
#'     \item{\code{candidates}}{A data frame of surviving accelerometer frames, encoded by
#'       \code{newX_col/newY_col/newZ_col}, their signs and a presentation score.}
#'     \item{\code{all_results}}{The scored search table, ordered by increasing score. Posture
#'       terms are in degrees and gravity loading is in g; the combined score is heuristic.}
#'     \item{\code{metric_explanation}}{A character summary of the scoring methodology.}
#'   }
#'
#' @seealso [importTagData()] for raw-axis data; [filterDeploymentData()] for deployment trimming;
#'   [checkSensorIntegrity()] and [checkSensorQuality()] for sensor quality control;
#'   [consensusAxisMapping()] for cross-deployment reconciliation; [reviewTagMapping()] for video
#'   review; [applyAxisMapping()] for applying the selected mappings; [processTagData()] for
#'   deriving body-frame metrics.
#'
#' @examples
#' \dontrun{
#' # Raw-axis records already trimmed and checked for sensor integrity.
#' files <- list.files("./checked", pattern = "\\.rds$", full.names = TRUE)
#' evidence <- checkTagMapping(files, plot.file = "./plots/axis_mapping.pdf")
#' evidence[[1]]$resolution
#' evidence[[1]]$proposal
#'
#' # Optional reconciliation across deployments of the same physical tag unit.
#' reconciled <- consensusAxisMapping(evidence, group.by = "package_id")
#' reconciled$provenance
#'
#' # Apply only after inspecting the evidence and any required video review.
#' oriented <- applyAxisMapping(files, mapping = reconciled)
#'
#' # Dorsoventral propulsion: change the corroborating axis, not the frame convention.
#' evidence <- checkTagMapping(files, locomotor.axis = "heave")
#' }
#' @export


checkTagMapping <- function(data,
                            id.col = "ID",
                            datetime.col = "datetime",
                            depth.col = "depth",
                            ax.col = "ax",
                            ay.col = "ay",
                            az.col = "az",
                            configs = NULL,
                            deployment.type = NULL,
                            static.threshold = 0.1,
                            vertical.speed.threshold = 0.5,
                            stable.horizontal.window = 10,
                            g.value = 1,
                            dba.window = 2.5,
                            depth.smoothing = 5,
                            vertical.smoothing = 1,
                            tie.tolerance = 2,
                            use.dynamics = TRUE,
                            dive.speed.threshold = 0.2,
                            max.vertical.speed = 5,
                            min.dynamic.corr = 0.4,
                            locomotor.band = c(0.2, 3),
                            locomotor.axis = "sway",
                            mag.hard.iron = TRUE,
                            plot = FALSE,
                            plot.file = NULL,
                            verbose = "detailed") {


  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

  # measure running time
  start.time <- Sys.time()

  # verbosity level (0 quiet / 1 normal / 2 detailed); per-step detail prints only at >= 2
  lvl <- .verbosity(verbose)

  # scalar argument validation
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col"); .assert_string(depth.col, "depth.col")
  .assert_number(static.threshold, "static.threshold", min = 0)
  .assert_number(vertical.speed.threshold, "vertical.speed.threshold", min = 0)
  .assert_number(stable.horizontal.window, "stable.horizontal.window", min = 0)
  .assert_number(g.value, "g.value", min = 0)
  .assert_number(dba.window, "dba.window", min = 0)
  if (!is.null(depth.smoothing)) .assert_number(depth.smoothing, "depth.smoothing", min = 0)
  if (!is.null(vertical.smoothing)) .assert_number(vertical.smoothing, "vertical.smoothing", min = 0)
  .assert_number(tie.tolerance, "tie.tolerance", min = 0)
  .assert_number(dive.speed.threshold, "dive.speed.threshold", min = 0)
  .assert_number(max.vertical.speed, "max.vertical.speed", min = 0)
  if (max.vertical.speed <= dive.speed.threshold)
    .abort("{.arg max.vertical.speed} ({max.vertical.speed}) must exceed {.arg dive.speed.threshold} ({dive.speed.threshold}).")
  .assert_number(min.dynamic.corr, "min.dynamic.corr", min = 0)
  .assert_flag(use.dynamics, "use.dynamics"); .assert_flag(plot, "plot"); .assert_flag(mag.hard.iron, "mag.hard.iron")
  if (!is.null(configs)) .validateConfigs(configs)              # documented-config dictionary to validate vs data
  # deployment.type: NULL (auto), or a scalar / per-dataset vector of "rigid" / "towed"
  if (!is.null(deployment.type)) {
    if (!is.character(deployment.type) || !all(deployment.type %in% c("rigid", "towed"))) {
      .abort('{.arg deployment.type} must be NULL or a character vector of "rigid" / "towed".')
    }
  }
  if (!is.numeric(locomotor.band) || length(locomotor.band) != 2 || locomotor.band[1] >= locomotor.band[2]) {
    .abort("{.arg locomotor.band} must be a numeric vector c(low, high) with low < high (Hz).")
  }
  # locomotor.axis: the body axis expected to carry the tail-beat (lateral "sway" for fish/sharks,
  # dorso-ventral "heave" for mobulids/cetaceans, "surge" for unusual styles)
  locomotor.axis <- match.arg(locomotor.axis, c("sway", "heave", "surge"))
  loco_role  <- c(sway = "Y", heave = "Z", surge = "X")[[locomotor.axis]]   # body-axis letter
  loco_label <- sprintf("%s (%s)", tools::toTitleCase(locomotor.axis), loco_role)
  .assert_writable_file(plot.file, "plot.file", ext = "pdf")     # fail-fast: parent dir must exist
  make_panel <- plot || !is.null(plot.file)

  # resolve input: a character vector of RDS paths, or an in-memory list / single data.frame
  is_filepaths <- is.character(data)
  .assert_nonempty(data, "data")             # loud failure on empty input (e.g. a typo'd list.files() -> character(0))
  if (is_filepaths) {
    missing_files <- data[!file.exists(data)]
    if (length(missing_files) > 0) .abort(c("These input files were not found:", stats::setNames(missing_files, rep("*", length(missing_files)))))
  } else if (!is.list(data) || inherits(data, "data.frame")) {
    .assert_columns(data, id.col, "data")
    data <- split(data, data[[id.col]])
  }

  # define required columns and validate each in-memory dataset up front
  required_cols <- c(id.col, datetime.col, depth.col, ax.col, ay.col, az.col)
  if (!is_filepaths) {
    # Channel presence is NOT gated here: a deployment whose channels were curated away by
    # checkSensorIntegrity() is set aside inside the loop, so the rest of the cohort still runs. Only
    # the datetime CLASS is checked up front, and only for deployments that carry the column - it is a
    # structural contract on the input rather than a property of any one sensor.
    for (nm in names(data)) {
      if (datetime.col %in% names(data[[nm]]) && !inherits(data[[nm]][[datetime.col]], "POSIXct")) {
        .abort("{.arg datetime.col} ({.val {datetime.col}}) must be a POSIXct column in {.val {nm}}.")
      }
    }
    missing_attr <- vapply(data, function(x) is.null(attr(x, "nautilus.version")), logical(1))
    if (any(missing_attr)) {
      cli::cli_warn(c("Some datasets were likely not processed via {.fn importTagData}: {.val {names(data)[missing_attr]}}.",
                      "i" = "Run them through {.fn importTagData} first to ensure correct formatting."))
    }
  }

  # diagnostic panels are rendered AFTER the loop so a run-summary page can be drawn first; here we
  # only remember the caller's active device (for the `plot` case). The device is opened post-loop.
  caller_dev <- grDevices::dev.cur()

  ##############################################################################
  # Data Preparation ###########################################################
  ##############################################################################

  # initialize results list if returning data
  n_animals <- length(data)

  # resolve an explicit deployment.type argument (scalar -> recycled; otherwise length(data)).
  # When NULL, each dataset's type is taken from metadata, falling back to "unknown".
  dep_arg <- deployment.type
  if (!is.null(dep_arg)) {
    if (length(dep_arg) == 1L) dep_arg <- rep(dep_arg, n_animals)
    else if (length(dep_arg) != n_animals) {
      .abort("{.arg deployment.type} must be length 1 or length(data) ({n_animals}).")
    }
  }

  # build the candidate axis mappings. A valid sensor remap between two right-handed frames is a
  # signed permutation with determinant +1 (a proper rotation); the 24 reflections (det -1) flip
  # handedness and would silently invert heading / cross-products downstream, so we exclude them.
  raw_cols <- c(ax.col, ay.col, az.col)
  perms <- list(c(1, 2, 3), c(1, 3, 2), c(2, 1, 3), c(2, 3, 1), c(3, 1, 2), c(3, 2, 1))
  sign_grid <- as.matrix(expand.grid(c(1, -1), c(1, -1), c(1, -1)))
  grid_rows <- list()
  for (p in perms) {
    for (s in seq_len(nrow(sign_grid))) {
      sgn <- as.numeric(sign_grid[s, ])
      M <- matrix(0, 3, 3)                       # body axis i = sgn[i] * raw[p[i]]
      for (i in 1:3) M[i, p[i]] <- sgn[i]
      if (!.isProperRotation(M)) next             # keep proper rotations only (24 of 48)
      grid_rows[[length(grid_rows) + 1L]] <- data.frame(
        newX_col = raw_cols[p[1]], newY_col = raw_cols[p[2]], newZ_col = raw_cols[p[3]],
        newX_sign = sgn[1], newY_sign = sgn[2], newZ_sign = sgn[3], stringsAsFactors = FALSE)
    }
  }
  permutations_grid <- do.call(rbind, grid_rows)

  # initialize list to store results, plus per-tag summary records (one per input, for the run summary)
  # and the failed-tag tracker (per-tag fault tolerance keeps one bad tag from aborting the batch)
  # Pre-sized like the other per-tag accumulators. It used to be `list()`, which auto-extended on the
  # first successful write: a deployment skipped or failed BEFORE a later success left a NULL element
  # with an NA name, and .asAxisMappingSet() then rejected the whole object ("not a recognised mapping
  # object") - so one skipped tag aborted applyAxisMapping()/reviewTagMapping() for the entire cohort.
  # Pre-sizing plus the compaction at the return keeps the result dense and correctly named.
  results_list <- vector("list", n_animals)
  summary_records <- vector("list", n_animals)
  panel_payloads  <- vector("list", n_animals)
  failed_ids <- character(0)
  skipped_ids <- character(0)          # deployments set aside for missing/unusable input (not failures)

  # header
  .log_header(lvl, "checkTagMapping", "Inferring IMU axis orientation",
              bullets = sprintf("Input: %d dataset%s", n_animals, if (n_animals != 1) "s" else ""),
              arrow = paste0("Method: rotation search, ", nrow(permutations_grid),
                             " proper-rotation candidates per tag"))




  ##############################################################################
  # Core Functionality #########################################################
  ##############################################################################

  # iterate over each animal
  untrimmed_ids <- character(0)                  # ordering guard: records not trimmed via filterDeploymentData()
  for (i in seq_along(data)) {

    ############################################################################
    # load data for the current individual if using file paths #################
    if (is_filepaths) {

      # get current file path
      file_path <- data[i]
      id <- tools::file_path_sans_ext(basename(file_path))

      # Load + validate as ONE fallible unit. This block sits ABOVE the per-tag tryCatch below, so an
      # abort here would kill the whole batch - which is exactly what the documented contract ("a single
      # bad dataset is recorded as failed and skipped, never aborting the whole batch") promises it will
      # not do. A corrupt file, a missing channel or a bad datetime class is a property of ONE
      # deployment; the other 51 must still be processed.
      individual_data <- tryCatch(readRDS(file_path), error = function(e) e)
      skip_reason <- NULL
      if (inherits(individual_data, "error")) {
        skip_reason <- sprintf("unreadable file (%s)", conditionMessage(individual_data))
      } else {
        missing_cols <- setdiff(required_cols, names(individual_data))
        if (length(missing_cols) > 0) {
          nm <- tryCatch(attr(individual_data, "nautilus", exact = TRUE), error = function(e) NULL)
          skip_reason <- .explainMissingColumns(missing_cols, nm)
        } else if (!inherits(individual_data[[datetime.col]], "POSIXct")) {
          skip_reason <- sprintf("the %s column is not POSIXct", datetime.col)
        }
      }
      if (!is.null(skip_reason)) {
        # a skipped deployment still gets its own delimited block, like every other one: the reason must
        # be attributable to a tag at a glance, not inferred from position between neighbouring blocks
        .log_h2(lvl, sprintf("%s (%d/%d)", id, i, n_animals), min_level = 1L)
        .log_skip(lvl, skip_reason, " ", cli::symbol$bullet, " skipped")
        summary_records[[i]] <- list(id = id, status = "skipped", n_resolved = 0L,
                                     gyro = NA_character_, mag = NA_character_,
                                     model = NA_character_, package_id = NA_character_,
                                     message = skip_reason)
        skipped_ids <- c(skipped_ids, id)
        .log_gap(lvl)
        next
      }
      if (is.null(attr(individual_data, "nautilus.version"))) {
        message(paste0("Warning: File '", basename(file_path), "' was likely not processed via importTagData(). It is strongly recommended to run it through importTagData() to ensure proper formatting."))
      }

    } else {
      # data is already in memory (list of data frames/tables)
      id <- names(data)[i]
      individual_data <- data[[i]]
      # the in-memory branch is gated the same way as the file branch (see above): a deployment whose
      # channels were curated away is skipped, not fatal
      if (!is.null(individual_data) && NROW(individual_data)) {
        missing_cols <- setdiff(required_cols, names(individual_data))
        if (length(missing_cols) > 0) {
          nm <- tryCatch(attr(individual_data, "nautilus", exact = TRUE), error = function(e) NULL)
          skip_reason <- .explainMissingColumns(missing_cols, nm)
          .log_h2(lvl, sprintf("%s (%d/%d)", id, i, n_animals), min_level = 1L)   # own block, as above
          .log_skip(lvl, skip_reason, " ", cli::symbol$bullet, " skipped")
          summary_records[[i]] <- list(id = id, status = "skipped", n_resolved = 0L,
                                       gyro = NA_character_, mag = NA_character_,
                                       model = NA_character_, package_id = NA_character_,
                                       message = skip_reason)
          skipped_ids <- c(skipped_ids, id)
          .log_gap(lvl)
          next
        }
      }
    }

    # capture deployment metadata (if present): attachment site for the sway-sign cross-check, and
    # location/date for the IGRF magnetic-inclination cross-check below
    nmeta <- tryCatch(attr(individual_data, "nautilus", exact = TRUE), error = function(e) NULL)
    dmeta <- nmeta$deployment
    attach_site <- dmeta$attachment_site %||% NA_character_
    if (length(attach_site) == 0) attach_site <- NA_character_

    # ORDERING GUARD: axis-mapping inference (gravity from static periods, dive dynamics) assumes the
    # record is trimmed to the on-animal period; pre/post-deployment data (e.g. a floating tag after
    # detachment) corrupts it. Flag records with no filterDeploymentData step in their history.
    proc_steps <- if (!is.null(nmeta$processing))
      vapply(nmeta$processing, function(s) as.character(s$step %||% ""), character(1)) else character(0)
    # only nag when the record went through the nautilus pipeline (has a history) but skipped trimming -
    # data with no processing trail may have been prepared/trimmed externally, so we do not assume.
    if (length(proc_steps) && !"filterDeploymentData" %in% proc_steps)
      untrimmed_ids <- c(untrimmed_ids, as.character(id))
    # grouping identifiers carried through for consensusAxisMapping(): the chip-to-housing axis
    # orientation is a per-unit manufacturing constant, so deployments are reconciled across a shared
    # `package_id` (default) or `tag` model / `type` (set via consensusAxisMapping(group.by=)).
    .scalar_chr <- function(x) { x <- x %||% NA_character_; if (length(x) != 1) NA_character_ else as.character(x) }
    package_id <- .scalar_chr(nmeta$tag$package_id)
    logger_id  <- .scalar_chr(nmeta$tag$logger_id)
    tag_model  <- .scalar_chr(nmeta$tag$model)
    tag_type   <- .scalar_chr(nmeta$tag$type)
    # paddle-wheel flag: a magnetic paddle wheel injects strong high-frequency noise into mx/my/mz,
    # which would inflate the dip variance the magnetometer mapping relies on. When present we pre-smooth
    # the magnetometer below, mirroring the safeguard in processTagData().
    has_paddle <- isTRUE(nmeta$tag$paddle_wheel)

    # resolve deployment type: explicit arg > metadata > "unknown". It selects the posture scorer -
    # "rigid" rewards near-zero roll/pitch (level resting posture); "towed"/"unknown" reward stability
    # (MAD), which tolerates the resting pitch/roll offsets typical of fin-clamp / tethered mounts.
    dep_type <- if (!is.null(dep_arg)) dep_arg[i] else {
      mt <- dmeta$deployment_type %||% NA_character_
      if (length(mt) == 1L && !is.na(mt) && mt %in% c("rigid", "towed")) mt else "unknown"
    }
    posture_metric <- if (identical(dep_type, "rigid")) "median" else "mad"

    deploy_lon  <- .asNumericSafe(dmeta$lon %||% NA_real_)
    deploy_lat  <- .asNumericSafe(dmeta$lat %||% NA_real_)
    deploy_time <- dmeta$datetime %||% as.POSIXct(NA)

    # expected geomagnetic inclination at the deployment (IGRF via oce), for cross-checking the
    # magnetometer-derived dip
    expected_incl <- NA_real_
    if (is.finite(deploy_lon) && is.finite(deploy_lat) && !is.na(deploy_time)) {
      expected_incl <- tryCatch(
        oce::magneticField(longitude = deploy_lon, latitude = deploy_lat, time = deploy_time)$inclination,
        error = function(e) NA_real_)
    }

    # per-individual block header (level >= 1); per-tag notices are collected and shown in-block
    ind_warns <- character(0)
    .log_h2(lvl, sprintf("%s (%d/%d)", id, i, n_animals), min_level = 1L)

    # per-tag fault tolerance: a single bad dataset is recorded as "failed" and skipped, never aborting
    # the whole batch. (`next` propagates normally through tryCatch.)
    tryCatch({

    # skip NULL or empty elements in the list
    if (is.null(individual_data) || nrow(individual_data) == 0) {
      .log_skip(lvl, "empty dataset ", cli::symbol$bullet, " skipped")
      summary_records[[i]] <- list(id = id, status = "skipped", n_resolved = 0L,
                                   gyro = NA_character_, mag = NA_character_,
                                   model = tag_model, package_id = package_id, message = "empty dataset")
      .log_gap(lvl)
      next
    }

    # Diagnostic helper columns and sorting must not reach the caller's sensor table.
    individual_data <- .workingTable(individual_data, owned = is_filepaths)

    # ensure data is ordered by datetime
    data.table::setorderv(individual_data, cols = datetime.col)


    ############################################################################
    # Calculate parameters #####################################################
    ############################################################################

    # estimate nominal sampling rate
    time_diffs <- c(NA, round(as.numeric(diff(individual_data[[datetime.col]])), 6))

    nominal_interval <- stats::median(time_diffs, na.rm = TRUE)
    sampling_freq <- round(1 / nominal_interval, 1)
    orig_sampling_freq <- sampling_freq

    ############################################################################
    # Downsample for the (low-frequency) axis resolution #######################
    ############################################################################

    # The entire resolution is low-frequency: gravity loading (static accel), dive pitch vs depth-rate,
    # tilt-rate correlations and the magnetic dip are all well below 1 Hz, so the search runs on a 1 Hz
    # block-mean copy (an anti-aliasing low-pass) - cutting work by the full sampling factor (20-100x).
    # Only the tail-beat corroboration needs higher frequencies; it keeps a separate copy decimated
    # just enough to hold the locomotor band below Nyquist. The returned mapping is rate-independent
    # (axis labels + correlations), so nothing downstream is affected by the working rate.
    res_fs <- min(orig_sampling_freq, 1)                       # resolution tier (1 Hz, or native if lower)
    tb_fs  <- min(orig_sampling_freq, ceiling(2.5 * locomotor.band[2]))   # tail-beat tier (Nyquist > band)
    tb_data <- NULL
    if (use.dynamics) {
      tb_cols <- c(ax.col, ay.col, az.col)
      tb_data <- if (orig_sampling_freq > tb_fs)
        .decimate(individual_data, tb_cols, datetime.col, tb_fs)
      else data.table::copy(individual_data[, .SD, .SDcols = c(datetime.col, tb_cols)])
    }
    if (orig_sampling_freq > res_fs) {
      res_cols <- c(depth.col, ax.col, ay.col, az.col,
                    intersect(c("gx", "gy", "gz", "mx", "my", "mz"), names(individual_data)))
      individual_data <- .decimate(individual_data, res_cols, datetime.col, res_fs)
      individual_data[, (id.col) := id]
      sampling_freq <- res_fs
    }

    # calculate window parameters for rolling mean
    window_size <- round(dba.window * sampling_freq)
    if (window_size %% 2 == 0) window_size <- window_size + 1
    pad_length <- floor(window_size / 2)


    ############################################################################
    # Extract horizontal swimming periods ######################################
    ############################################################################

    # The same local depth slopes and optional rate post-mean used by processTagData().
    individual_data[, vertical_speed := .depthRate(individual_data[[depth.col]],
                                                  as.numeric(individual_data[[datetime.col]]),
                                                  depth.smoothing, dt = 1 / sampling_freq,
                                                  post_s = vertical.smoothing)]

    # snapshot the full (unfiltered) series for the dynamic anchors below (surge, gyro, mag): they
    # need the DIVING / manoeuvring periods that the horizontal filter is about to discard. Carry
    # the gyroscope/magnetometer channels too when present.
    snap_cols <- c(datetime.col, depth.col, ax.col, ay.col, az.col, "vertical_speed",
                   intersect(c("gx", "gy", "gz", "mx", "my", "mz"), names(individual_data)))
    full_data <- data.table::copy(individual_data[, .SD, .SDcols = snap_cols])

    # paddle-wheel magnetometer de-noising: a spinning magnet adds strong high-frequency noise to
    # mx/my/mz. Mirror processTagData() with a 3-second centered rolling mean on the magnetometer (and
    # the magnetometer only) so the dip diagnostic, hard-iron estimate and mag panel all see the
    # de-noised signal. Gated on the paddle-wheel flag, so standard tags are untouched.
    if (has_paddle && all(c("mx", "my", "mz") %in% names(full_data))) {
      .log_detail(lvl, "removing paddle-wheel-induced magnetic noise")
      paddle_window <- round(3 * sampling_freq)
      full_data[, `:=`(mx = .rollmeanCentered(mx, paddle_window),
                       my = .rollmeanCentered(my, paddle_window),
                       mz = .rollmeanCentered(mz, paddle_window))]
    }

    # identify horizontal periods
    individual_data[, is_horizontal := (abs(vertical_speed) <= vertical.speed.threshold)]

    # filter the data to only include these horizontal periods
    # to ensure "stable" horizontal periods, apply a rolling window on is.horizontal
    filtered_data <- individual_data[is_horizontal == TRUE]

    # convert stable.horizontal.window to number of samples
    stable_window_samples <- round(stable.horizontal.window * sampling_freq)
    # ensure it's an odd number for centered window
    if (stable_window_samples %% 2 == 0) stable_window_samples <- stable_window_samples + 1

    if (nrow(individual_data) < stable_window_samples) {
      ind_warns <- c(ind_warns, sprintf("dataset too short (%s samples) for stable.horizontal.window (%s samples) - no stability filtering",
                                         .formatNumber(nrow(individual_data)), .formatNumber(stable_window_samples)))
      # if data is too short for the window, use the point-wise filtered data
      filtered_data <- individual_data[is_horizontal == TRUE]
    } else {
      # calculate rolling sum of horizontal points (0 or 1).
      individual_data[, horizontal_sum_in_window := .pad_rollsum(as.numeric(is_horizontal), stable_window_samples, floor(stable_window_samples/2))]
      # a point is 'stable horizontal' if it is itself horizontal AND the entire window around it is also horizontal.
      individual_data[, is_stable_horizontal := (is_horizontal == TRUE & horizontal_sum_in_window == stable_window_samples)]
      # filter the data to only include these stable horizontal periods
      filtered_data <- individual_data[is_stable_horizontal == TRUE]
    }

    # if no stable horizontal periods found, fall back to non-stable horizontal
    if (nrow(filtered_data) == 0) {
      ind_warns <- c(ind_warns, "no stable horizontal periods - used all horizontal points")

      # fallback to all horizontal points
      filtered_data <- individual_data[is_horizontal == TRUE]

      # if still no horizontal points at all, then skip
      if (nrow(filtered_data) == 0) {
        for (w in ind_warns) .log_skip(lvl, w)
        .log_skip(lvl, "no horizontal periods found ", cli::symbol$bullet, " dataset skipped")
        summary_records[[i]] <- list(id = id, status = "skipped", n_resolved = 0L,
                                     gyro = NA_character_, mag = NA_character_,
                                     model = tag_model, package_id = package_id, message = "no horizontal data")
        .log_gap(lvl)
        next
      } else {
        # duration uses the working-grid interval (1/sampling_freq); the data is decimated to the
        # resolution grid above, so multiplying the (decimated) row count by the ORIGINAL interval
        # would under-report the real horizontal time by the decimation factor.
        .log_detail(lvl, "horizontal: ", .formatDuration(nrow(filtered_data) / sampling_freq), " (non-stable)")
        individual_data <- filtered_data
      }
    } else {
      # use the stable horizontal data
      .log_detail(lvl, "horizontal: ", .formatDuration(nrow(filtered_data) / sampling_freq), " (stable)")
      individual_data <- filtered_data
    }

    # dynamic (diving) data available to the surge anchor (.resolveSurge): the diving / manoeuvring
    # points the horizontal filter discards. Same working-grid interval as the horizontal line, plus
    # the share of the record, so the reader can judge whether there was enough signal to trust surge.
    n_horizontal <- nrow(filtered_data)                        # static periods used for gravity / posture
    n_dive <- sum(abs(full_data$vertical_speed) > dive.speed.threshold, na.rm = TRUE)
    static_secs <- n_horizontal / sampling_freq                # carried into the PDF summary table
    diving_pct  <- 100 * n_dive / nrow(full_data)
    .log_detail(lvl, "dynamic (diving): ", .formatDuration(n_dive / sampling_freq),
                " (", round(diving_pct), "% of record)")


    # keep only relevant columns
    individual_data <- individual_data[, .SD, .SDcols = c(required_cols)]


    ############################################################################
    # Score all candidate mappings (single vectorized pass) ####################
    ############################################################################

    # all 24 candidates are scored together; the rotation-invariant work (rolling means, magnitude,
    # static mask) is done once inside .scoreAxisMappings rather than once per candidate. The posture
    # term is deployment-aware: "median" (rigid, rewards near-zero roll/pitch) or "mad" (towed/unknown,
    # rewards stability so resting pitch/roll offsets are not penalized).
    results_df <- .scoreAxisMappings(individual_data, permutations_grid, ax.col, ay.col, az.col,
                                     window_size, pad_length, static.threshold, g.value,
                                     posture.metric = posture_metric)
    results_full <- cbind(permutations_grid, results_df)

    # full ranking (for all_results / margin)
    results_sorted <- results_full[order(results_full$score, na.last = TRUE), , drop = FALSE]
    rownames(results_sorted) <- NULL
    if (all(!is.finite(results_full$score))) {
      .abort(c("No valid mapping could be found with a finite score.",
               "i" = "Most often this is a gap: a run of missing values in any single accelerometer channel makes every candidate non-finite. A gap only has to span one {.arg dba.window} to do it, so widening {.arg dba.window} is the usual fix.",
               "i" = "Other causes are insufficient static periods, sensor calibration problems, or acceleration in the wrong units (see {.arg g.value}).",
               "i" = "Check sensor integrity with {.fn checkSensorIntegrity} before relaxing {.arg static.threshold}, as lowering thresholds may hide the underlying sensor problem rather than fix it."))
    }

    # Resolve the VERTICAL axis from gravity loading, independently of the posture score: body +Z is
    # the candidate whose signed static-Z is most gravity-aligned (~ +g). This is magnitude-based and
    # offset-tolerant, so it is unaffected by resting pitch/roll offsets and by the posture metric
    # (median vs MAD) - a "stably wrong" vertical has low posture variance but the wrong gravity loading.
    z_i    <- which.max(results_full$median_static_az)
    z_col  <- results_full$newZ_col[z_i]; z_sign <- results_full$newZ_sign[z_i]
    z_grav <- results_full$median_static_az[z_i]
    # the candidates sharing that vertical are the 4 surge/sway permutations -- the horizontal tie set
    same_z <- results_full$newZ_col == z_col & results_full$newZ_sign == z_sign & is.finite(results_full$score)
    near <- results_full[same_z, , drop = FALSE]
    near <- near[order(near$score), , drop = FALSE]
    best <- near[1, ]
    # the vertical is ambiguous only if a DIFFERENT Z axis/sign is nearly as gravity-aligned (an
    # extreme mount tilt where no single axis clearly carries gravity). `tie.tolerance` is retained
    # for API compatibility but no longer gates the static tie (the vertical is now gravity-anchored).
    # NOTE: this surge/heave gravity tie is NOT dynamically breakable. The depth-rate <-> pitch
    # correlation pins which axis is SURGE but is (near-)invariant to the heave choice - during a dive
    # the depth-rate signal sits in the surge component, so every candidate sharing the true surge axis
    # scores the same correlation regardless of which axis it calls heave. Gravity is the only
    # discriminator of the vertical, so when gravity ties it the whole frame is honestly left ambiguous.
    vert_grav_sep <- 0.3 * g.value
    vertical_resolved <- !any(results_full$median_static_az >= z_grav - vert_grav_sep &
                              paste(results_full$newZ_col, results_full$newZ_sign) != paste(z_col, z_sign),
                              na.rm = TRUE)

    # static/dynamic accelerometer decomposition over the full (unfiltered) snapshot, computed ONCE and
    # reused by the surge anchor, the body tilt below, and the diagnostic panel.
    sd_full <- .staticDynamicAccel(full_data[[ax.col]], full_data[[ay.col]], full_data[[az.col]], window_size)

    # ---- SURGE estimator: break the surge/sway tie using depth-rate <-> pitch over dives ----
    # The correct surge axis is the one whose body pitch tracks vertical velocity during dives; once
    # surge and the (gravity-fixed) heave are known, the proper-rotation constraint closes sway. Gated on
    # `vertical_resolved`: surge and sway are defined relative to a known heave (sway is closed by
    # handedness from surge x heave), so they cannot be resolved while the vertical is still tied - doing
    # so would assert a handedness-derived sway on top of an unknown heave (a physical impossibility).
    # (The depth-rate <-> pitch correlation pins SURGE but is near-invariant to the heave choice, so it
    # cannot itself break a vertical gravity tie.)
    surge_resolved <- FALSE; dyn <- NULL
    if (use.dynamics && vertical_resolved && nrow(near) > 1) {
      dyn <- .resolveSurge(full_data, near, sd_full, ax.col, ay.col, az.col,
                           dive.speed.threshold, min.dynamic.corr, max.vertical.speed)
      if (!is.null(dyn) && isTRUE(dyn$decisive)) {
        best <- near[dyn$best_index, ]
        near <- near[dyn$best_index, , drop = FALSE]      # collapse to the resolved candidate
        surge_resolved <- TRUE
      }
    }

    # ---- SURVIVORS: the single source of truth ----
    # The candidate frames consistent with the (gravity) vertical and the (depth-rate) surge decisions.
    # Vertical resolved -> the gravity-best vertical's perms (`near`: 4 surge/sway perms, or 1 once surge
    # resolves). Vertical gravity-AMBIGUOUS (mount tilt where no single axis clearly carries gravity) ->
    # every gravity-plausible vertical's perms. There the depth-rate anchor still cannot pick the heave
    # (it is near-invariant to the heave choice), but it CAN pick the surge WITHIN each plausible vertical
    # (Phase 2): we run the anchor over each vertical's 4 surge/sway perms and, when it is DECISIVE,
    # collapse that vertical to its single surge-resolved perm. This narrows the reported set (e.g. 8 -> 2:
    # one frame per vertical) and makes consensusAxisMapping() respect this tag's own dive evidence,
    # WITHOUT claiming to resolve the still-undetermined heave. Every downstream output is a PROJECTION of
    # `survivors`; `score` orders it for PRESENTATION only (zero weight in any resolution decision).
    surge_per_vertical <- list()
    if (vertical_resolved) {
      survivors <- near
    } else {
      plaus <- results_full[is.finite(results_full$score) &
                            results_full$median_static_az >= z_grav - vert_grav_sep, , drop = FALSE]
      surv_parts <- list()
      for (vk in unique(paste(plaus$newZ_col, plaus$newZ_sign))) {
        perms_v <- plaus[paste(plaus$newZ_col, plaus$newZ_sign) == vk, , drop = FALSE]
        perms_v <- perms_v[order(perms_v$score), , drop = FALSE]
        dyn_v <- if (use.dynamics && nrow(perms_v) > 1)
          .resolveSurge(full_data, perms_v, sd_full, ax.col, ay.col, az.col, dive.speed.threshold, min.dynamic.corr, max.vertical.speed) else NULL
        dec_v <- !is.null(dyn_v) && isTRUE(dyn_v$decisive)
        surv_parts[[vk]] <- if (dec_v) perms_v[dyn_v$best_index, , drop = FALSE] else perms_v
        surge_per_vertical[[vk]] <- list(decisive = dec_v, corr = if (!is.null(dyn_v)) dyn_v$best_corr else NA_real_)
      }
      survivors <- do.call(rbind, surv_parts)
      survivors <- survivors[order(survivors$score), , drop = FALSE]
      best <- survivors[1, ]      # representative = gravity-best vertical's (surge-resolved) perm
    }
    n_static_tied <- nrow(survivors)
    n_plausible_verticals <- length(unique(paste(survivors$newZ_col, survivors$newZ_sign)))
    # TRUE when the per-vertical anchor resolved the surge inside EVERY plausible vertical (so only the
    # heave is left undetermined) - drives the honest "surge resolved within each" wording in the notes.
    surge_narrowed <- length(surge_per_vertical) > 0 &&
      all(vapply(surge_per_vertical, function(s) isTRUE(s$decisive), logical(1)))

    # the structured frame state Phase 3 estimators will extend (gyro-vertical, gyro-surge, conflicts)
    frame_state <- list(
      survivors = survivors,
      vertical  = list(status = if (vertical_resolved) "resolved" else "ambiguous",
                       axis = if (vertical_resolved) z_col else NA_character_,
                       sign = if (vertical_resolved) z_sign else NA_real_,
                       by   = if (vertical_resolved) "gravity" else NA_character_,
                       n_plausible = n_plausible_verticals),
      surge     = list(status = if (surge_resolved) "resolved" else "ambiguous",
                       by   = if (surge_resolved) "depth-rate" else NA_character_,
                       corr = if (!is.null(dyn)) dyn$best_corr else NA_real_,
                       dive_signal = !is.null(dyn),
                       within_vertical = isTRUE(surge_narrowed),
                       per_vertical = surge_per_vertical),
      conflicts = character(0))

    # ---- validate the documented axis config (if `configs` supplied) against the inferred frame ----
    # Compared over the ACCELEROMETER body frame (what `survivors` represents): confirmed = the data
    # uniquely resolved to exactly the documented frame; consistent = the documented frame is among the
    # still-plausible survivors (data ambiguous); conflict = it is outside the plausible set (the doc
    # disagrees with the data) -> flagged. A reflection config (det -1) lies outside the proper-rotation
    # search space, so the data can neither confirm nor refute it (reported, never a conflict).
    prior <- list(status = "absent", config = NA_character_, gyro_status = "absent", mag_status = "absent")
    exp_ft <- NULL
    if (!is.null(configs)) {
      exp_ft <- .expandAxisConfigForTag(nmeta$tag$axis_config, configs, id)
      if (!is.null(exp_ft)) {
        prior$config <- trimws(as.character(nmeta$tag$axis_config))
        exp_acc <- exp_ft[exp_ft$from %in% c("ax", "ay", "az"), c("from", "to"), drop = FALSE]
        M_exp <- if (nrow(exp_acc)) .mappingToSignedPerm(exp_acc, c("ax", "ay", "az")) else NULL
        if (is.null(M_exp)) {
          prior$status <- "unverifiable"            # no clean accel permutation to compare (e.g. a dropped axis)
        } else if (.signedPermDet(M_exp) == -1L) {
          prior$status <- "reflection"              # left-handed raw frame; the right-handed search can't judge it
        } else {
          surv_keys <- paste(survivors$newX_col, survivors$newX_sign, survivors$newY_col, survivors$newY_sign,
                             survivors$newZ_col, survivors$newZ_sign, sep = "|")
          if (.permMatrixKey(M_exp, c("ax", "ay", "az")) %in% surv_keys) {
            prior$status <- if (nrow(survivors) == 1L) "confirmed" else "consistent"
          } else {
            prior$status <- "conflict"
            frame_state$conflicts <- c(frame_state$conflicts, sprintf(
              "documented axis config '%s' conflicts with the data (it implies an accelerometer frame outside the gravity/dynamics-plausible set)",
              prior$config))
          }
        }
      }
    }
    frame_state$prior <- prior

    # tail-beat corroboration of the chosen surge/sway assignment, on the higher-rate copy (the 1 Hz
    # resolution grid cannot hold the locomotor band). Annotation only - it never gates resolution.
    tb <- if (!is.null(tb_data)) .tailBeatCorroboration(tb_data, best, ax.col, ay.col, az.col, tb_fs, locomotor.band, loco.axis = locomotor.axis) else NULL
    tb_corroborated <- if (!is.null(tb)) tb$corroborated else NA

    # ---- gyroscope & magnetometer family resolution (P3b), relative to the accel body frame ----
    # body-frame series from the chosen accelerometer candidate
    sbr <- stats::setNames(list(sd_full$static$x, sd_full$static$y, sd_full$static$z), c(ax.col, ay.col, az.col))
    surge_s <- best$newX_sign * sbr[[best$newX_col]]
    sway_s  <- best$newY_sign * sbr[[best$newY_col]]
    heave_s <- best$newZ_sign * sbr[[best$newZ_col]]
    body_tilt <- .tiltFromAccel(surge_s, sway_s, heave_s)
    gbody <- cbind(surge_s, sway_s, heave_s)

    # accelerometer mapping as a matrix (body = M_acc %*% raw), for the magnetometer common-frame check
    axesA <- c(ax.col, ay.col, az.col)
    M_acc <- matrix(0, 3, 3)
    M_acc[1, match(best$newX_col, axesA)] <- best$newX_sign
    M_acc[2, match(best$newY_col, axesA)] <- best$newY_sign
    M_acc[3, match(best$newZ_col, axesA)] <- best$newZ_sign

    # a gyro present-but-empty (all-NA, e.g. some camera tags) or too sparse to co-register is NOT a usable
    # gyro: require enough finite triples before either RESOLVING or co-registering it, so a broken/sparse
    # gyro is never proposed unvalidated (the correlation resolver here and the co-die block below share
    # this gate). The magnetometer is independent and keeps its own presence check.
    has_gyro <- all(c("gx", "gy", "gz") %in% names(full_data)) &&
                sum(is.finite(full_data$gx) & is.finite(full_data$gy) & is.finite(full_data$gz)) >= 60L
    gyro_res <- if (use.dynamics && has_gyro) .resolveGyro(full_data, body_tilt$pitch, body_tilt$roll, sampling_freq, min.dynamic.corr) else NULL
    mag_res  <- if (use.dynamics) .resolveMag(full_data, gbody, M_acc, sampling_freq, window_size, hard.iron = mag.hard.iron) else NULL

    # ---- Phase 3: gyroscope mapping via the co-die default, validated by co-registration ----
    # The gyroscope shares the accelerometer die (the co-die default), so its body-frame map is the
    # AXIAL-vector transform of the accel map: gyro = det(M_acc) * M_acc (angular velocity is a
    # pseudovector, so it carries the reflection sign; the result is always a proper rotation). We VALIDATE
    # that default against the data with the strapdown gravity-direction identity dghat/dt = -omega x ghat,
    # scored for the co-die (R = identity) hypothesis by .resolveGyroFrame ($corr_identity). If it
    # co-registers - or there is too little rotation to judge - the derived map is adopted. If it is
    # DECISIVELY rejected (r < 0.2 with rotation), the co-die assumption is wrong for this tag (e.g. an
    # independent gyro die) and we fall back to the data-driven resolvers: the correlation resolver
    # (.resolveGyro, above) and the strapdown frame estimator, composed with the correct axial law
    # det(M_acc) * M_acc * R. The two are cross-checked (a clean disagreement flags + abstains).
    gyro_inconsistent <- FALSE
    coreg_corr <- NA_real_; coreg_status <- "absent"
    if (use.dynamics && has_gyro) {
      gyro_frame  <- .resolveGyroFrame(full_data, sampling_freq, c(ax.col, ay.col, az.col))
      derived_map <- .matrixToFromTo(.signedPermDet(M_acc) * M_acc, c("gx", "gy", "gz"), c("gx", "gy", "gz"))
      coreg_corr  <- if (!is.null(gyro_frame)) gyro_frame$corr_identity %||% NA_real_ else NA_real_
      motion_ok   <- !is.null(gyro_frame) && isTRUE(gyro_frame$motion_ok)
      # co-die verdict; "fail" only when decisively contradicted (r < 0.2 with rotation), matching
      # applyAxisMapping's threshold (fleet-calibrated: correct maps stay >= 0.2, broken tags ~0).
      coreg_status <- if (!motion_ok || !is.finite(coreg_corr)) "unknown"
                      else if (coreg_corr >= 0.2) "ok" else "fail"

      if (coreg_status %in% c("ok", "unknown")) {
        # co-die default holds (or cannot be refuted) -> adopt gyro = det(M_acc) * M_acc
        gyro_res <- list(decisive = TRUE, source = "coreg-derived", coreg_corr = coreg_corr, mapping = derived_map)
      } else {
        # co-die REJECTED by the data -> data-driven fallback (correlation vs strapdown reconciliation),
        # composing the strapdown gyro-chip -> accel-chip rotation with the AXIAL law det(M_acc) * M_acc * R.
        strap_res <- NULL
        if (!is.null(gyro_frame) && isTRUE(gyro_frame$decisive)) {
          C <- .signedPermDet(M_acc) * (M_acc %*% gyro_frame$R)
          strap_res <- list(decisive = TRUE, source = "strapdown", strap_corr = gyro_frame$corr,
                            mapping = .matrixToFromTo(C, c("gx", "gy", "gz"), c("gx", "gy", "gz")))
        }
        if (!is.null(gyro_res) && isTRUE(gyro_res$decisive) && !is.null(strap_res)) {
          # both decisive: do they agree?
          Mc <- .mappingToSignedPerm(gyro_res$mapping,  c("gx", "gy", "gz"))
          Ms <- .mappingToSignedPerm(strap_res$mapping, c("gx", "gy", "gz"))
          if (!is.null(Mc) && !is.null(Ms) && !isTRUE(all.equal(Mc, Ms))) {
            gyro_inconsistent <- TRUE
            gyro_res$decisive <- FALSE                       # disputed: abstain, do not propose
            frame_state$conflicts <- c(frame_state$conflicts,
              "gyroscope mapping disagreement between the correlation and strapdown estimators - inspect before use")
          } else {
            gyro_res$source <- "correlation+strapdown"; gyro_res$strap_corr <- gyro_frame$corr
          }
        } else if (!is.null(strap_res)) {
          gyro_res <- strap_res                              # only the strapdown is decisive -> resolve via it
        }
        # (neither the correlation resolver nor the strapdown decisive -> gyro_res stays unresolved)
        if (!is.null(gyro_res)) gyro_res$coreg_corr <- coreg_corr
      }
    }
    frame_state$coreg <- list(corr = coreg_corr, status = coreg_status)

    # per body-axis resolution, PROJECTED from `survivors`: an axis is RESOLVED iff every surviving frame
    # agrees on its raw source AND sign. A vertical gravity tie then falls out naturally - the survivors
    # span more than one heave, so Z does not agree and is reported ambiguous (no special-case needed).
    # `evidence` names the estimator that pinned each axis.
    resolve_axis <- function(col_f, sign_f, evidence) {
      if (length(unique(paste(survivors[[col_f]], survivors[[sign_f]]))) == 1L) {
        data.frame(status = "resolved", source = survivors[[col_f]][1], sign = survivors[[sign_f]][1],
                   evidence = evidence, stringsAsFactors = FALSE)
      } else {
        data.frame(status = "ambiguous", source = NA_character_, sign = NA_real_,
                   evidence = NA_character_, stringsAsFactors = FALSE)
      }
    }
    ev_xy <- if (surge_resolved) c("depth-rate", "handedness") else c("accelerometer", "accelerometer")
    resolution <- cbind(body_axis = c("X", "Y", "Z"),
                        rbind(resolve_axis("newX_col", "newX_sign", ev_xy[1]),
                              resolve_axis("newY_col", "newY_sign", ev_xy[2]),
                              resolve_axis("newZ_col", "newZ_sign", "gravity")))

    # proposal (raw -> body axis, signed), ready for applyAxisMapping(). Conservative by design: the
    # accelerometer rows are included ONLY when all three axes are resolved, because a partial
    # accelerometer remap is not a valid proper rotation (you cannot flip one axis without breaking
    # handedness). When the horizontal axes are ambiguous no accelerometer remap is proposed - the
    # per-axis detail is in `resolution`, the tied set in `candidates`. Decisive gyro / mag families
    # (each a complete triplet) are appended independently.
    sgn_str <- function(s, axis) paste0(if (s < 0) "-" else "", axis)
    body_raw <- c(X = ax.col, Y = ay.col, Z = az.col)
    acc_best <- list(X = list(best$newX_col, best$newX_sign), Y = list(best$newY_col, best$newY_sign),
                     Z = list(best$newZ_col, best$newZ_sign))
    all_resolved <- all(resolution$status == "resolved")          # a single surviving frame
    proposal <- if (all_resolved) {
      do.call(rbind, lapply(c("X", "Y", "Z"), function(k)
        data.frame(from = acc_best[[k]][[1]], to = sgn_str(acc_best[[k]][[2]], body_raw[[k]]), stringsAsFactors = FALSE)))
    } else data.frame(from = character(0), to = character(0), stringsAsFactors = FALSE)
    rownames(proposal) <- NULL
    # FRAME-CONSISTENCY RULE: the gyro and mag mappings are expressed relative to the accelerometer body
    # frame, so they are appended ONLY when that frame is fully resolved. Emitting a decisive gyro/mag
    # remap while the accel frame is still ambiguous would let a caller apply a family remap in a frame
    # the accelerometer is not in - leaving the sensor families in different coordinate systems.
    if (all_resolved && !is.null(gyro_res) && isTRUE(gyro_res$decisive)) proposal <- rbind(proposal, gyro_res$mapping)
    if (all_resolved && !is.null(mag_res)  && isTRUE(mag_res$decisive))  proposal <- rbind(proposal, mag_res$mapping)

    # confidence margin: gravity-loading separation of the chosen vertical over the best alternative
    # vertical (presentation / diagnostic only - it carries no weight in the resolution decisions).
    other_v <- results_full[is.finite(results_full$score) &
                            paste(results_full$newZ_col, results_full$newZ_sign) != paste(z_col, z_sign), , drop = FALSE]
    margin <- if (nrow(other_v) > 0) min(other_v$score) - best$score else NA_real_
    # confidence note, derived strictly from the frame state - it never asserts a state the math did not
    # reach (e.g. it does not claim "vertical resolved" while the heave is still gravity-ambiguous).
    conf_note <- if (vertical_resolved && surge_resolved) {
      sprintf("All axes resolved (vertical: gravity; surge: depth-rate r=%.2f; sway: handedness%s).",
              dyn$best_corr, if (isTRUE(tb_corroborated)) ", tail-beat corroborated" else "")
    } else if (!vertical_resolved) {
      extra <- if (isTRUE(surge_narrowed)) " surge/sway resolved within each candidate (depth-rate);" else ""
      sprintf("Vertical axis gravity-ambiguous (mount tilt):%s %d candidate frame%s across %d plausible vertical%s; heave undetermined - resolve via consensus or visual validation.",
              extra, n_static_tied, if (n_static_tied != 1) "s" else "", n_plausible_verticals, if (n_plausible_verticals != 1) "s" else "")
    } else {
      reason <- if (is.null(dyn)) "insufficient diving signal" else sprintf("weak/ambiguous pitch-depth correlation (best r=%.2f)", dyn$best_corr)
      sprintf("Vertical axis resolved (gravity); surge/sway ambiguous - %s. %d candidates tied; confirm with visual validation.",
              reason, n_static_tied)
    }

    # gyro / mag family notes
    gyro_status <- if (is.null(gyro_res)) "absent/insufficient" else if (isTRUE(gyro_res$decisive)) "resolved" else if (isTRUE(gyro_inconsistent)) "inconsistent" else "unresolved"
    mag_status <- if (is.null(mag_res)) {
      "absent/insufficient"
    } else if (isTRUE(mag_res$decisive)) {
      "resolved"
    } else if (!is.null(mag_res$best_possible_sd) && is.finite(mag_res$best_possible_sd) && mag_res$best_possible_sd <= 8) {
      "inconsistent"                                  # a different vertical fits far better than the accel frame
    } else {
      "unresolved"
    }
    if (!is.null(gyro_res) && isTRUE(gyro_res$decisive)) {
      conf_note <- paste0(conf_note, if (identical(gyro_res$source, "coreg-derived")) {
          if (is.finite(coreg_corr)) sprintf(" Gyroscope co-registered with the accelerometer (co-die default, r=%.2f).", coreg_corr)
          else " Gyroscope mapped from the accelerometer (co-die default; too little rotation to verify)."
        } else if (identical(gyro_res$source, "strapdown"))
          sprintf(" Gyroscope co-die default rejected (r=%.2f); resolved from data by strapdown (gravity-rotation r=%.2f).", coreg_corr, gyro_res$strap_corr %||% NA_real_)
        else
          sprintf(" Gyroscope co-die default rejected (r=%.2f); resolved from data (roll-rate r=%.2f, pitch-rate r=%.2f).", coreg_corr, gyro_res$cor_roll %||% NA_real_, gyro_res$cor_pitch %||% NA_real_))
    } else if (isTRUE(gyro_inconsistent)) {
      conf_note <- paste0(conf_note, sprintf(" Gyroscope co-die default rejected (r=%.2f) and the fallback estimators (correlation vs strapdown) disagree (see frame_state$conflicts); not proposed.", coreg_corr))
    } else if (identical(coreg_status, "fail")) {
      conf_note <- paste0(conf_note, sprintf(" Gyroscope does NOT co-register with the accelerometer (r=%.2f) and no data-driven alternative resolved; not proposed - inspect.", coreg_corr))
    }
    # IGRF cross-check of the measured vs expected magnetic inclination
    incl_residual <- if (!is.null(mag_res) && !is.null(mag_res$measured_inclination) && is.finite(expected_incl)) {
      mag_res$measured_inclination - expected_incl
    } else NA_real_
    igrf_note <- if (is.finite(incl_residual)) {
      flag <- if (abs(incl_residual) > 10) " (LARGE residual - check magnetometer calibration / mapping)" else " (consistent with IGRF)"
      sprintf(" IGRF inclination %.0f deg vs measured %.0f deg, residual %.0f deg%s.", expected_incl, mag_res$measured_inclination, incl_residual, flag)
    } else ""

    if (mag_status == "resolved") {
      conf_note <- paste0(conf_note, sprintf(" Magnetometer consistent with the accel frame (dip SD=%.1f deg; inclination ~%.0f deg). Note: the mag heading axes share the accel surge/sway resolution (dip cannot fix heading).", mag_res$angle_sd, mag_res$measured_inclination), igrf_note)
    } else if (mag_status == "inconsistent") {
      conf_note <- paste0(conf_note, " Magnetometer appears to use a DIFFERENT vertical axis than the accelerometer (independent mag frame) - inspect before use.", igrf_note)
    }

    families <- list(
      gyro = if (is.null(gyro_res)) list(status = "absent/insufficient", mapping = NULL, coreg_corr = coreg_corr)
             else list(status = gyro_status, mapping = if (isTRUE(gyro_res$decisive)) gyro_res$mapping else NULL,
                       source = gyro_res$source %||% "correlation", coreg_corr = gyro_res$coreg_corr %||% coreg_corr,
                       cor_roll = gyro_res$cor_roll %||% NA_real_, cor_pitch = gyro_res$cor_pitch %||% NA_real_,
                       strap_corr = gyro_res$strap_corr %||% NA_real_),
      mag  = if (is.null(mag_res)) list(status = "absent/insufficient", mapping = NULL)
             else list(status = mag_status, mapping = if (isTRUE(mag_res$decisive)) mag_res$mapping else NULL,
                       consistent_with_accel = mag_res$consistent_with_accel %||% FALSE,
                       angle_sd = mag_res$angle_sd %||% NA_real_, measured_inclination = mag_res$measured_inclination %||% NA_real_,
                       expected_inclination = expected_incl, inclination_residual = incl_residual,
                       hard_iron_applied = mag_res$hard_iron_applied %||% FALSE,
                       hard_iron_offset = mag_res$hard_iron_offset))

    # ---- validate the documented gyro/mag config rows against the inferred family mappings ----
    # The inferred gyro/mag mappings are expressed RELATIVE to the accelerometer body frame, so the doc
    # can only be judged when that reference frame is itself pinned to the doc - i.e. the accelerometer
    # config is `confirmed` (a single survivor equal to the documented frame). Under any other accel
    # state the inferred and documented body frames may differ by a rotation, so the comparison is
    # meaningless and we report `unverifiable` (resolve the accel first). A family stays unverifiable
    # when the data did not decisively resolve it (or the gyro was flagged inconsistent). A conflict
    # means the DOC is wrong - it is reported, but never alters the data-inferred proposal.
    if (!is.null(configs) && !is.null(exp_ft)) {
      validate_family_config <- function(fam_axes, inferred_map) {
        exp_fam <- exp_ft[exp_ft$from %in% fam_axes, c("from", "to"), drop = FALSE]
        if (nrow(exp_fam) == 0L) return("absent")                 # the config documents no rows for this family
        M_exp_f <- .mappingToSignedPerm(exp_fam, fam_axes)
        if (is.null(M_exp_f)) return("unverifiable")              # doc rows drop an axis / reference another family
        if (!identical(prior$status, "confirmed")) return("unverifiable")  # accel reference not pinned to the doc
        if (is.null(inferred_map)) return("unverifiable")         # data did not decisively resolve this family
        M_inf_f <- .mappingToSignedPerm(inferred_map, fam_axes)
        if (is.null(M_inf_f)) return("unverifiable")
        if (.permMatrixKey(M_exp_f, fam_axes) == .permMatrixKey(M_inf_f, fam_axes)) "confirmed" else "conflict"
      }
      gst <- validate_family_config(c("gx", "gy", "gz"), families$gyro$mapping)
      mst <- validate_family_config(c("mx", "my", "mz"), families$mag$mapping)
      frame_state$prior$gyro_status <- gst
      frame_state$prior$mag_status  <- mst
      if (identical(gst, "conflict")) frame_state$conflicts <- c(frame_state$conflicts, sprintf(
        "documented gyroscope config '%s' conflicts with the data-inferred gyro mapping", prior$config))
      if (identical(mst, "conflict")) frame_state$conflicts <- c(frame_state$conflicts, sprintf(
        "documented magnetometer config '%s' conflicts with the data-inferred mag mapping", prior$config))
    }

    # attachment-site cross-check (left/right pectoral mounts are mirror images -> sway sign flips)
    if (!is.na(attach_site) && grepl("pectoral", attach_site, ignore.case = TRUE)) {
      conf_note <- paste0(conf_note,
        sprintf(" Attachment site '%s': the sway sign is mirror-related between left/right pectoral mounts - cross-check sign consistency across sides.", attach_site))
    }

    confidence <- list(margin = margin, n_static_tied = n_static_tied,
                       surge_resolved = surge_resolved, vertical_resolved = vertical_resolved,
                       surge_corr = if (!is.null(dyn)) dyn$best_corr else NA_real_,
                       tailbeat_corroborated = tb_corroborated,
                       n_dynamic_samples = if (!is.null(dyn)) dyn$n_dynamic else 0L,
                       attachment_site = attach_site, note = conf_note)
    candidates <- survivors[, c("newX_col", "newX_sign", "newY_col", "newY_sign", "newZ_col", "newZ_sign", "score")]

    metric_explanation <- paste0(
      "Static score = median(abs(roll)) + median(abs(pitch)) + abs(median(remapped static az) - ", g.value,
      "); lower is better. Search restricted to the 24 handedness-preserving (det +1) signed permutations. ",
      "Surge/sway disambiguated (when `use.dynamics`) via the depth-rate <-> pitch correlation over diving periods.")

    # per-individual result (the core diagnostic output; shown at the normal level and above), as a
    # clean cli block. The full proposal / resolution / confidence data structures stay in the
    # returned object for programmatic use; only the console rendering changes.
    if (lvl >= 1L) {
      all_resolved <- all(resolution$status == "resolved")
      amb <- resolution$body_axis[resolution$status == "ambiguous"]
      res <- resolution$body_axis[resolution$status == "resolved"]

      # per-axis change vs the raw frame: "confirmed" (resolved & identity), "remap" (resolved &
      # a swap/sign change), or "unresolved". Drives whether a remap is proposed at all.
      chg <- vapply(seq_len(3L), function(k) {
        if (resolution$status[k] != "resolved") return("unresolved")
        ident <- identical(resolution$source[k], body_raw[[resolution$body_axis[k]]]) && resolution$sign[k] == 1
        if (ident) "confirmed" else "remap"
      }, character(1))
      remap_ax <- resolution$body_axis[chg == "remap"]
      unver_ax <- resolution$body_axis[chg == "unresolved"]

      # status line: three distinct states - fully resolved, partial (1-2), or unresolved (0)
      n_res <- length(res)
      if (n_res == 3L) {
        status_key <- "v"; status_txt <- "resolved: all three axes"
      } else if (n_res >= 1L) {
        status_key <- "!"; status_txt <- sprintf("partial: %d of 3 axes resolved", n_res)
      } else {
        status_key <- "x"; status_txt <- "unresolved: 0 of 3 axes resolved"
      }

      # mapping line: deterministic prefix. Only resolved-and-changed axes are proposed for remapping;
      # confirmed axes need no change; unresolved axes are flagged [.. unverified], never applied.
      unver_phrase <- paste(unver_ax, collapse = " and ")
      if (length(remap_ax) > 0) {
        remap_str <- vapply(remap_ax, function(k) sprintf("%s = %s%s", k,
                            if (resolution$sign[match(k, resolution$body_axis)] < 0) "-" else "",
                            resolution$source[match(k, resolution$body_axis)]), character(1))
        map_line <- paste0("proposed remapping: ", paste(remap_str, collapse = " | "))
        if (length(unver_ax) > 0) map_line <- paste0(map_line, " [", unver_phrase, " unverified]")
      } else if (length(unver_ax) == 0) {
        map_line <- "confirmed mapping: (all raw axes correct)"
      } else if (length(res) > 0) {
        # some axes confirmed correct, the rest unverified
        map_line <- paste0("confirmed mapping: (resolved axes correct) [", unver_phrase, " unverified]")
      } else {
        # nothing resolved - never claim a confirmed mapping
        map_line <- paste0("no remap proposed: ", unver_phrase, " unverified")
      }

      # evidence: which physical signal resolved each axis (resolved axes first, then any tied group).
      # Notation reads as `(X) depth-rate` (axis label only - the role words are dropped to save space);
      # renamed from "resolution:" to avoid the term collision with the sampling "resolution grid". The
      # underlying resolution$* fields are unchanged.
      parts <- character(0)
      for (k in which(resolution$status == "resolved")) {
        a <- resolution$body_axis[k]
        parts <- c(parts, sprintf("(%s) %s", a, resolution$evidence[k]))
      }
      if (length(amb))
        parts <- c(parts, sprintf("(%s) tied across %d candidates", paste(amb, collapse = "/"), n_static_tied))
      evidence_line <- paste0("evidence: ", paste(parts, collapse = " \u00b7 "))

      # secondary sensors (gyro / mag): unpacked into a nested bulleted list, each bullet coloured by its
      # own state (green = ok, yellow = weak/unresolved, red = inconsistent, grey = absent) so the block
      # reads like a dashboard. State text stays monochrome - the symbol/colour is the signal.
      state_col <- function(state) switch(state, ok = cli::col_green, weak = cli::col_yellow,
                                          bad = cli::col_red, absent = cli::col_grey)
      g <- families$gyro
      gyro_txt   <- if (identical(g$status, "resolved")) {
                      if (identical(g$source, "coreg-derived"))
                        if (is.finite(g$coreg_corr)) sprintf("resolved (co-die default, coreg r %.2f)", g$coreg_corr)
                        else "resolved (co-die default, unverified)"
                      else if (identical(g$source, "strapdown")) sprintf("resolved (coreg fail; strapdown r %.2f)", g$strap_corr %||% NA_real_)
                      else sprintf("resolved (roll r %.2f, pitch r %.2f)", g$cor_roll, g$cor_pitch)
                    } else if (identical(g$status, "inconsistent")) "inconsistent (gyro estimators disagree)"
                    else if (identical(g$status, "absent/insufficient")) "absent"
                    else if (is.finite(g$coreg_corr) && g$coreg_corr < 0.2) sprintf("unresolved (co-die rejected, coreg r %.2f)", g$coreg_corr)
                    else "unresolved"
      gyro_state <- if (identical(g$status, "resolved")) "ok"
                    else if (identical(g$status, "inconsistent")) "bad"
                    else if (identical(g$status, "absent/insufficient")) "absent" else "weak"
      mg <- families$mag
      hi_tag <- if (isTRUE(mg$hard_iron_applied)) ", hard-iron corrected" else ""
      mag_txt <- if (identical(mg$status, "resolved")) {
        ig <- if (is.finite(mg$inclination_residual)) sprintf(", IGRF %+.0f\u00b0", mg$inclination_residual) else ""
        sprintf("consistent (dip SD %.1f\u00b0%s%s)", mg$angle_sd, ig, hi_tag)
      } else if (identical(mg$status, "inconsistent")) paste0("inconsistent (independent vertical frame", hi_tag, ")")
        else if (identical(mg$status, "absent/insufficient")) "absent" else "unresolved"
      mag_state <- if (identical(mg$status, "resolved")) "ok"
                   else if (identical(mg$status, "inconsistent")) "bad"
                   else if (identical(mg$status, "absent/insufficient")) "absent" else "weak"

      # concise note, derived strictly from the frame state (never a fabricated reason). Suppressed when
      # it would only restate the resolved header. Full rationale always stays in confidence$note.
      note <- if (all_resolved) {
        if (isTRUE(tb_corroborated)) paste0("tail-beat corroborated on ", locomotor.axis) else NULL
      } else if (!vertical_resolved) {
        extra <- if (isTRUE(surge_narrowed)) "surge/sway resolved within each; " else ""
        sprintf("vertical gravity-ambiguous (mount tilt); %s%d candidates across %d verticals - resolve via consensus",
                extra, n_static_tied, n_plausible_verticals)
      } else if (!is.null(dyn) && is.finite(dyn$best_corr)) {
        sprintf("weak pitch-depth correlation (r=%.2f); confirm with visual validation", dyn$best_corr)
      } else "insufficient diving signal; confirm with visual validation"

      # the mapping line mirrors the status symbol (tick / ! / cross) via its cli_bullets key, so cli
      # colours the SYMBOL to match the status line above it; the line text itself stays default colour.
      bl <- stats::setNames(c(status_txt, map_line), c(status_key, status_key))
      bl <- c(bl, stats::setNames(evidence_line, ">"))
      if (!is.null(note)) bl <- c(bl, stats::setNames(note, "i"))
      bl <- c(bl, stats::setNames("secondary sensors:", ">"))
      bl <- c(bl, stats::setNames(paste0(state_col(gyro_state)(cli::symbol$bullet), " gyro: ", gyro_txt), " "))
      bl <- c(bl, stats::setNames(paste0(state_col(mag_state)(cli::symbol$bullet), " mag: ", mag_txt), " "))
      # documented-config validation (only when `configs` supplied and this tag has one)
      if (!is.null(configs) && !identical(frame_state$prior$status, "absent")) {
        ps <- frame_state$prior$status; pc <- frame_state$prior$config
        ptxt <- switch(ps,
          confirmed  = sprintf("documented config '%s': confirmed by data", pc),
          consistent = sprintf("documented config '%s': consistent (data ambiguous)", pc),
          conflict   = sprintf("documented config '%s': CONFLICTS with data", pc),
          reflection = sprintf("documented config '%s': reflection (left-handed raw frame; not validated by the search)", pc),
          sprintf("documented config '%s': cannot validate (dropped/partial axes)", pc))
        bl <- c(bl, stats::setNames(ptxt, switch(ps, confirmed = "v", consistent = "i", conflict = "x", "!")))
        # per-family lines for any documented gyro/mag rows (shown only when the config includes them)
        fam_cfg_line <- function(fn, st) switch(st,
          confirmed = sprintf("documented %s config: confirmed by data", fn),
          conflict  = sprintf("documented %s config: CONFLICTS with data", fn),
          sprintf("documented %s config: cannot validate (accel frame not confirmed / family unresolved)", fn))
        for (fam in list(c("gyro", frame_state$prior$gyro_status), c("mag", frame_state$prior$mag_status))) {
          if (!identical(fam[2], "absent"))
            bl <- c(bl, stats::setNames(fam_cfg_line(fam[1], fam[2]),
                                        switch(fam[2], confirmed = "v", conflict = "x", "!")))
        }
      }
      if (length(ind_warns)) bl <- c(bl, stats::setNames(ind_warns, rep("!", length(ind_warns))))
      cli::cli_bullets(bl)
      cli::cli_text("")
    }

    # ---- visual validation panel (P4): build a BOUNDED, pre-reduced payload (drawn after the loop) ----
    if (make_panel) {
      # dynamic axes for the spectra come from the tail-beat-rate copy so the locomotor peak is not
      # aliased away by the 1 Hz resolution grid (fall back to the resolution grid if no dynamics).
      dbr <- if (!is.null(tb)) tb$dynamic
             else stats::setNames(list(sd_full$dynamic$x, sd_full$dynamic$y, sd_full$dynamic$z), c(ax.col, ay.col, az.col))
      spectra_fs <- if (!is.null(tb)) tb$fs else sampling_freq

      # panel 1 (surge anchor): correlation on the diving points, with physically-impossible speeds
      # excluded by the SAME bound .resolveSurge uses - so the panel's r matches the reported
      # surge_corr and a depth artifact cannot make the plot contradict the decision. Thinned scatter.
      vsf <- full_data$vertical_speed
      dive <- is.finite(vsf) & abs(vsf) > dive.speed.threshold & abs(vsf) <= max.vertical.speed
      di <- which(dive)
      sa_r <- if (length(di) > 5) .safeCor(body_tilt$pitch[di], vsf[di]) else NA_real_
      sa_di <- if (length(di) > 4000L) di[round(seq(1, length(di), length.out = 4000L))] else di
      sa <- list(pitch = body_tilt$pitch[sa_di], vs = vsf[sa_di], r = sa_r, n = length(di))

      # panel 2 (gravity partition): windowed to +/- 2 min around the strongest PLAUSIBLE dive (never an
      # artifact spike, so the window lands on a real dive), with depth
      k <- if (length(di)) di[which.max(abs(vsf[di]))] else max(1L, length(vsf) %/% 2L)
      half <- min(length(surge_s) %/% 2L, round(120 * sampling_freq))
      gidx <- max(1, k - half):min(length(surge_s), k + half)
      gp <- list(t = as.numeric(full_data[[datetime.col]][gidx] - full_data[[datetime.col]][gidx][1]),
                 surge = surge_s[gidx], sway = sway_s[gidx], heave = heave_s[gidx],
                 depth = full_data[[depth.col]][gidx])

      # panel 3 (spectra): Welch-averaged (bounded bins) per body axis, in-band kept
      sp <- NULL
      pgs <- lapply(stats::setNames(list(best$newX_sign * dbr[[best$newX_col]],
                                         best$newY_sign * dbr[[best$newY_col]],
                                         best$newZ_sign * dbr[[best$newZ_col]]), c("surge", "sway", "heave")),
                    .welchSpectrum, fs = spectra_fs)
      if (all(!vapply(pgs, is.null, logical(1)))) {
        inb <- pgs$surge$freq <= locomotor.band[2] * 1.6
        sp <- list(freq = pgs$surge$freq[inb], surge = pgs$surge$power[inb],
                   sway = pgs$sway$power[inb], heave = pgs$heave$power[inb])
      }

      # panel 4 (mag dip): envelope-decimated dip-angle line
      md <- NULL
      if (all(c("mx", "my", "mz") %in% names(full_data))) {
        hi_off <- if (!is.null(mag_res)) mag_res$hard_iron_offset else NULL
        mxv <- full_data$mx; myv <- full_data$my; mzv <- full_data$mz
        if (!is.null(hi_off)) { mxv <- mxv - hi_off[1]; myv <- myv - hi_off[2]; mzv <- mzv - hi_off[3] }
        rmm <- cbind(.rollmeanCentered(mxv, window_size), .rollmeanCentered(myv, window_size),
                     .rollmeanCentered(mzv, window_size)) %*% t(M_acc)
        ghat <- gbody / sqrt(rowSums(gbody^2)); rn <- sqrt(rowSums(rmm^2))
        ang <- acos(pmax(-1, pmin(1, rowSums((rmm / rn) * ghat)))) * 180 / pi
        if (any(is.finite(ang))) {
          tmin <- as.numeric(full_data[[datetime.col]] - full_data[[datetime.col]][1]) / 60
          dec <- .decimateForPlot(tmin, ang, 2000L)
          md <- list(t = dec$x, angle = dec$y, mean = mean(ang, na.rm = TRUE),
                     expected = if (is.finite(expected_incl)) 90 - expected_incl else NA_real_)
        }
      }

      panel_payloads[[i]] <- list(id = id, sa = sa, gp = gp, sp = sp, md = md,
                                  locomotor.band = locomotor.band, loco.axis = locomotor.axis,
                                  loco_role = loco_role, dive.speed.threshold = dive.speed.threshold,
                                  flags = list(surge_resolved = surge_resolved, vertical_resolved = vertical_resolved,
                                               tb_corroborated = tb_corroborated, mag_status = mag_status),
                                  resolution = resolution)
    }

    # save results to list (id / package_id / tag / type carried for consensusAxisMapping grouping)
    results_list[[i]] <- list(id = id, package_id = package_id, logger_id = logger_id,
                              tag = tag_model, type = tag_type,
                              proposal = proposal, resolution = resolution, families = families,
                              confidence = confidence, candidates = candidates, all_results = results_sorted,
                              frame_state = frame_state, metric_explanation = metric_explanation)
    names(results_list)[i] <- id

    # per-tag summary record (drives the console run-summary and the PDF summary page). Carries the
    # per-axis verdict, the remap state, the dynamic-data quality metrics and context for the dashboard.
    n_res_i <- sum(resolution$status == "resolved")
    axes_resolved <- stats::setNames(resolution$status[match(c("X", "Y", "Z"), resolution$body_axis)] == "resolved",
                                     c("X", "Y", "Z"))
    n_confirmed <- sum(vapply(seq_len(3L), function(k) {
      resolution$status[k] == "resolved" &&
        identical(resolution$source[k], body_raw[[resolution$body_axis[k]]]) && resolution$sign[k] == 1
    }, logical(1)))
    remap_state <- if (n_res_i < 3L) "None" else if (n_confirmed == 3L) "Confirmed" else "Proposed"
    summary_records[[i]] <- list(id = id,
      status = if (n_res_i == 3L) "resolved" else if (n_res_i >= 1L) "partial" else "unresolved",
      n_resolved = n_res_i, gyro = families$gyro$status, mag = families$mag$status,
      model = tag_model, package_id = package_id, axes = axes_resolved, remap = remap_state,
      tailbeat = tb_corroborated, static_secs = static_secs, diving_pct = diving_pct,
      surge_corr = confidence$surge_corr, mag_resid = families$mag$inclination_residual,
      site = attach_site,
      # the documented-config verdict was already computed for this tag's console block; carrying it
      # here is what lets the SUMMARY aggregate it without a second pass over anything
      prior = frame_state$prior$status %||% NA_character_)

    }, error = function(e) {
      failed_ids <<- c(failed_ids, id)
      summary_records[[i]] <<- list(id = id, status = "failed", n_resolved = NA_integer_,
                                    gyro = NA_character_, mag = NA_character_, message = conditionMessage(e))
      if (lvl >= 1L) cli::cli_alert_danger("{id}: {conditionMessage(e)} - skipped")
      .log_gap(lvl)
    })
  }

  # Deployments set aside for missing/unusable input are announced at ANY verbosity: a silent skip in a
  # 52-tag batch is how a cohort quietly shrinks between pipeline steps.
  .warn_grouped(
    "{length(skipped_ids)} deployment{?s} {?was/were} skipped: axis mapping needs the accelerometer.",
    items = skipped_ids,
    hints = c("Their axis mapping is UNRESOLVED - they carry no entry in the returned mapping set.",
              "This is expected when {.fn checkSensorIntegrity} excluded a channel; see {.code meta$sensors$excluded}."))

  # ORDERING-GUARD warning (fires at any verbosity): un-trimmed records let post-detachment / off-animal
  # data bias the axis-mapping inference. Emitted once with the affected ids.
  if (length(untrimmed_ids)) {
    cli::cli_warn(c(
      "{length(untrimmed_ids)} record{?s} {?has/have} no {.fn filterDeploymentData} step in {?its/their} history: {.val {utils::head(untrimmed_ids, 8)}}.",
      "i" = "Axis-mapping inference assumes on-animal data - trim to the deployment window first, or off-animal periods (e.g. a detached, floating tag) may bias the result."))
  }


  ##############################################################################
  # Return results #############################################################
  ##############################################################################

  # aggregate per-tag outcomes for the run summary (single source of truth for console + PDF page)
  recs  <- Filter(Negate(is.null), summary_records)
  stat  <- vapply(recs, function(r) r$status, character(1))
  n_full <- sum(stat == "resolved"); n_part <- sum(stat == "partial")
  n_unres <- sum(stat == "unresolved"); n_skip <- sum(stat == "skipped"); n_fail <- sum(stat == "failed")
  n_eval <- n_full + n_part + n_unres                       # tags that produced a result

  # final summary: headline tally + a hierarchical, scannable breakdown of how the batch resolved
  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, n_eval, " of ", n_animals, " tag", if (n_animals != 1) "s", " evaluated",
              if (n_fail > 0) paste0(" (", n_fail, " failed)") else "")
    tick <- cli::col_green(cli::symbol$tick); warn <- cli::col_yellow("!"); crss <- cli::col_red(cli::symbol$cross)
    info <- cli::col_blue(cli::symbol$info)
    # Counts right-aligned under a common label width, so the block scans as a column of numbers rather
    # than a ragged list. cli_verbatim, NOT cli_text: cli_text normalises runs of whitespace for prose
    # wrapping, which silently collapses the padding and returns the column to a ragged list. Verbatim
    # also means no glue, so a label containing braces would be safe; the symbols carry their own colour.
    row <- function(sym, label, n, w)
      cli::cli_verbatim(paste0("  ", sym, " ", sprintf("%-*s %3d", w, paste0(label, ":"), n)))
    w1 <- 19L
    row(tick, "fully resolved", n_full, w1)
    row(warn, "partially resolved", n_part, w1)
    row(crss, "unresolved", n_unres, w1)
    if (n_skip > 0) row(crss, "skipped", n_skip, w1)
    if (n_fail > 0) row(crss, "failed", n_fail, w1)

    # ---- documented-configuration validation -------------------------------------------------------
    # Only when `configs` was supplied: without one every tag is "absent" and the block would be a
    # column of zeros under a single meaningless total. The tally covers the EVALUATED tags (skipped and
    # failed ones never reached the comparison), so it sums to the headline count above it.
    if (!is.null(configs)) {
      pri <- vapply(recs, function(r) as.character(r$prior %||% NA_character_), character(1))
      pri <- pri[!is.na(pri)]
      if (length(pri)) {
        n_conf <- sum(pri == "confirmed"); n_cons <- sum(pri == "consistent")
        n_cfl  <- sum(pri == "conflict")
        # "reflection" (left-handed raw frame) and "unverifiable" (dropped/partial axes) both mean the
        # search could not judge the documented config; they differ only in cause, which the
        # per-deployment block already gives. One row here keeps the summary scannable.
        n_unv  <- sum(pri %in% c("reflection", "unverifiable"))
        n_none <- sum(pri == "absent")
        cli::cli_text("")
        cli::cli_verbatim("  configuration validation")
        w2 <- 21L
        row(tick, "confirmed", n_conf, w2)
        row(warn, "consistent", n_cons, w2)
        row(crss, "conflict", n_cfl, w2)
        row(warn, "unverifiable", n_unv, w2)
        row(info, "no documented config", n_none, w2)
      }
    }
    if (!is.null(plot.file)) .log_arrow(lvl, "plots: ", plot.file)
    .log_runtime(lvl, start.time)
  }

  # ---- render diagnostics: a run-summary page first, then one page per evaluated tag ----
  if (make_panel) {
    run_stats <- list(n_animals = n_animals, n_full = n_full, n_part = n_part, n_unres = n_unres,
                      n_skip = n_skip, n_fail = n_fail, locomotor.axis = locomotor.axis)
    draw_all <- function(unicode) {
      .drawTagMappingSummaryPage(summary_records, run_stats, use_unicode = unicode)
      for (p in panel_payloads) if (!is.null(p)) .plotTagMappingPanel(p, use_unicode = unicode)
    }
    if (!is.null(plot.file)) {
      # cairo_pdf renders Unicode (the pass/fail marks) reliably; degrade to pdf() with ASCII marks when
      # cairo is unavailable or fails to start at runtime (e.g. headless machines).
      file_unicode <- .openPdfDevice(plot.file, width = 10, height = 7, cairo = TRUE)$unicode
      file_dev <- grDevices::dev.cur()
      tryCatch(draw_all(file_unicode),
               finally = if (file_dev %in% grDevices::dev.list()) grDevices::dev.off(file_dev))
    }
    if (plot) {
      if (caller_dev <= 1L) { grDevices::dev.new(); caller_dev <- grDevices::dev.cur() }
      grDevices::dev.set(caller_dev); draw_all(TRUE)
    }
  }

  # drop the slots of skipped/failed tags, so the returned object is a dense, fully-named set of
  # mappings rather than one carrying NULL holes for the deployments that produced nothing
  keep <- !vapply(results_list, is.null, logical(1))
  results_list <- results_list[keep]

  # return results, tagged with the producer so applyAxisMapping() can route + attribute it
  attr(results_list, "nautilus.mapping.producer") <- "checkTagMapping"
  return(results_list)
}
