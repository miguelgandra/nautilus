#######################################################################################################
# Guess IMU Axis Mapping for Multiple Animals/Datasets ################################################
#######################################################################################################

#' Guess IMU Axis Mapping for Tag Data
#'
#' @description
#' Systematically evaluates possible accelerometer axis mappings to identify the most likely
#' correct orientation of IMU (Inertial Measurement Unit) sensors in archival tags deployed
#' on marine animals. The function tests all possible permutations and sign combinations of
#' the raw axes, scoring each based on how well the remapped data matches expected physical behavior.
#' The "goodness" of each mapping is determined by how well calculated roll and pitch
#' angles align with expected values (median absolute roll/pitch near zero) and
#' how closely the remapped Z-axis acceleration matches gravity during static periods.
#'
#' @param data Input data, which can be either:
#'   \itemize{
#'     \item A list of data frames/tables (each containing sensor data for one individual)
#'     \item A single data frame/table
#'     \item Character vector of paths to RDS files containing sensor data
#'   }
#'   The inference assumes the record is trimmed to the on-animal period: off-animal data (e.g. a tag
#'   drifting after detachment) can bias the axis estimate, so a warning is raised for any pipeline-
#'   processed deployment whose history lacks a \link{filterDeploymentData} step.
#' @param id.col Character. Name of the column containing unique identifier for each tag/animal.
#' Used when input is a single data.frame that needs splitting. Default "ID".
#' @param datetime.col Character. Name of the datetime column. Must contain POSIXct values.
#'   Default "datetime".
#' @param depth.col Character. Name of the column containing raw depth data. Defaults to "depth".
#' @param ax.col Character. Name of the column containing raw X-axis acceleration data. Defaults to "ax".
#' @param ay.col Character. Name of the column containing raw Y-axis acceleration data. Defaults to "ay".
#' @param az.col Character. Name of the column containing raw Z-axis acceleration data. Defaults to "az".
#' @param configs Optional named dictionary of documented configurations (config name -> `from`/`to`
#'   data.frame), the same object accepted by \code{\link{applyAxisMapping}}. When supplied, each tag's
#'   `axis_config` metadata (set at import) is looked up and its documented accelerometer frame is
#'   **validated against the inferred frame**: per deployment, `frame_state$prior$status` reports
#'   `"confirmed"` (data uniquely resolved to exactly that frame), `"consistent"` (the documented frame
#'   is among the still-plausible candidates), `"conflict"` (it lies outside the plausible set - the doc
#'   disagrees with the data, also added to `frame_state$conflicts`), or `"reflection"` (a left-handed
#'   raw frame the right-handed search cannot judge). If the config also documents gyroscope or
#'   magnetometer rows, those are validated too (`frame_state$prior$gyro_status` /
#'   `mag_status`): `"confirmed"` / `"conflict"` against the inferred family mapping, or `"unverifiable"`.
#'   Because the inferred gyro/mag mappings are expressed relative to the accelerometer body frame, a
#'   gyro/mag row can only be judged when the accelerometer config is itself `"confirmed"` (otherwise the
#'   reference frame is not pinned to the doc and the family is `"unverifiable"`). Default `NULL`.
#' @param deployment.type Mount type controlling the posture scorer: `"rigid"` rewards near-zero
#'   resting roll/pitch (assumes a level resting posture); `"towed"` rewards stability (MAD), which
#'   tolerates the resting offsets typical of fin-clamp / tethered mounts, and suppresses the weak
#'   pitch-depth warning (decoupling is expected). `NULL` (default) takes the type per dataset from
#'   metadata (\code{columns = metadataColumns(deployment_type = ...)} in \code{\link{importTagData}}),
#'   falling back to the stability scorer when unknown. Either a single value or a per-dataset vector.
#'   The vertical axis is resolved from gravity regardless, so this only affects horizontal ranking
#'   and warnings.
#' @param static.threshold A numeric value (in g's) used to identify "static" periods.
#'   Data points where the magnitude of the **static** accelerometer vector is within this
#'   threshold of 1g are considered static. Defaults to 0.1g.
#' @param vertical.speed.threshold Numeric. Speed threshold (m/s) for horizontal periods. Default 0.5.
#' @param stable.horizontal.window Numeric. Window size (s) for stable horizontal identification. Default 10.
#' @param g.value The gravitational acceleration value (in g's) expected when static. Defaults to 1.
#' @param dba.window The window size (in seconds) for calculating static acceleration using a rolling mean. Defaults to 2.5 seconds.
#' @param depth.smoothing,speed.smoothing Smoothing windows (seconds) for the vertical-velocity estimate
#'   used by the surge anchor (centered difference on smoothed depth, then optional velocity smoothing -
#'   the same estimate as \code{\link{processTagData}}). Low-passing removes the quantization staircase
#'   that otherwise degrades the pitch / depth-rate relationship. Defaults 10 and 1; `NULL` disables.
#' @param tie.tolerance Numeric. Score gap (in the score's units, roughly degrees) within which
#'   competing mappings are treated as tied when assessing per-axis resolution. A body axis is
#'   reported as "resolved" only if all mappings within this tolerance of the best agree on it.
#'   Defaults to 2.
#' @param use.dynamics Logical. If `TRUE` (default), attempt to break the horizontal (surge/sway)
#'   tie left by the accelerometer scorer using diving dynamics: the correct surge axis is the one
#'   whose body pitch correlates with vertical velocity during dives, after which the proper-rotation
#'   constraint fixes the sway axis and sign. If `FALSE`, only the gravity-based vertical axis is
#'   resolved.
#' @param dive.speed.threshold Numeric. Minimum absolute vertical velocity (m/s) for a sample to
#'   count as "diving" in the surge anchor. Defaults to 0.2.
#' @param max.vertical.speed Numeric. Maximum plausible absolute vertical velocity (m/s) for a
#'   swimming animal. Samples faster than this are treated as depth-sensor artifacts (e.g. a stuck or
#'   saturated depth block, which can inject speeds of tens to hundreds of m/s) and excluded from the
#'   surge-resolution correlation, so a corrupt depth segment cannot hijack the pitch /
#'   vertical-velocity anchor. Must exceed `dive.speed.threshold`. Defaults to 5.
#' @param min.dynamic.corr Numeric. Minimum magnitude of the pitch / vertical-velocity correlation
#'   required to accept the surge axis as resolved. Defaults to 0.4.
#' @param locomotor.band Numeric `c(low, high)`. Frequency band (Hz) used for the tail-beat power
#'   corroboration of the locomotor axis; set the lower edge above the tag's tether/pendulum frequency
#'   to exclude that contamination. Defaults to `c(0.2, 3)`.
#' @param locomotor.axis Character. Body axis expected to carry the propulsive (tail-beat) oscillation,
#'   used to corroborate the mapping: `"sway"` (default; lateral, fish/sharks), `"heave"` (dorso-ventral,
#'   mobulid rays / cetaceans), or `"surge"`. The corroboration passes when this axis carries the most
#'   locomotor-band power of the three, and the spectra panel labels/highlights it accordingly.
#' @param mag.hard.iron Logical. If `TRUE` (default), apply a provisional spherical (hard-iron) offset
#'   correction to the magnetometer before the dip-consistency diagnostic, but only when orientation
#'   coverage is sufficient (else it is skipped). This is diagnostic only and never persisted; it
#'   improves the dip plot / consistency check on uncalibrated deployments.
#' @param plot Logical. If `TRUE`, draw a per-individual visual-validation panel (depth-rate vs
#'   pitch, gravity partition over a dive, per-axis spectra, magnetometer dip angle) to the active
#'   graphics device. Default `FALSE`.
#' @param plot.file Character. Path to a single multi-page PDF in which to save the validation panels
#'   (one page per individual). The parent directory must already exist; must end in `.pdf`. If
#'   `NULL` (default), no file is written. Independent of `plot`.
#' @param verbose Verbosity level: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal" (a per-individual cli
#'   block: status, proposed mapping, per-axis resolution, gyro/mag families, and a one-line note),
#'   or `2`/"detailed" (default; adds low-level per-step diagnostics). Defaults to `"detailed"`.
#'
#' @return A named list, one element per individual, each containing:
#'   \itemize{
#'     \item \code{id}, \code{package_id}, \code{tag}, \code{type}: the deployment identifier plus the
#'       grouping keys carried from metadata (\code{NA} if absent) - the physical-unit \code{package_id},
#'       the \code{tag} model and \code{type}. Any of these can be used by \code{\link{consensusAxisMapping}}
#'       (via its \code{group.by}) to reconcile mapping solutions across related deployments.
#'     \item \code{proposal}: a \code{from}/\code{to} data.frame for the best-scoring mapping, ready to
#'       pass to \code{\link{applyAxisMapping}}.
#'     \item \code{resolution}: a data.frame stating, for each body axis (X/Y/Z), whether it is
#'       \code{"resolved"} or \code{"ambiguous"}, the raw source axis/sign when resolved, and the
#'       \code{evidence} that resolved it ("gravity", "depth-rate", or "handedness").
#'     \item \code{families}: gyroscope and magnetometer results. The gyroscope defaults to the
#'       co-die transform of the accelerometer map (\code{gyro = det(M)*M}, an axial vector) and that
#'       default is VALIDATED by co-registration (the strapdown identity \code{d(ghat)/dt = -omega x ghat}
#'       scored for the identity hypothesis; see \code{families$gyro$coreg_corr} and
#'       \code{frame_state$coreg}). When it co-registers (or there is too little rotation to judge) it is
#'       adopted (\code{source = "coreg-derived"}); when it is decisively rejected the data-driven
#'       resolvers (roll-rate / pitch-rate correlation and the strapdown frame estimator) take over and
#'       the deployment is flagged \code{coreg_fail} for \code{\link{reviewTagMapping}}. The magnetometer
#'       is validated against the accelerometer frame via the gravity-field dip (its heading axes cannot
#'       be fixed from dip alone, so they share the accelerometer resolution). Each has a \code{status}
#'       ("resolved" / "unresolved" / "inconsistent" / "absent/insufficient") and a \code{mapping}.
#'       The magnetometer also reports \code{measured_inclination} and, when deployment coordinates
#'       are available, the IGRF \code{expected_inclination} and their \code{inclination_residual}.
#'     \item \code{confidence}: a list with the score \code{margin} to the runner-up, the number of
#'       static-tied candidates, whether surge was resolved, the surge correlation, tail-beat
#'       corroboration, the attachment site, and a human-readable \code{note}.
#'     \item \code{candidates}: the set of near-best (tied) mappings.
#'     \item \code{all_results}: scores for all evaluated mappings.
#'     \item \code{metric_explanation}: description of the scoring methodology.
#'   }
#'
#' @details
#' The function evaluates the 24 handedness-preserving signed permutations of the raw axes (proper
#' rotations, determinant +1; the 24 reflections are excluded because they flip handedness and would
#' invert heading and cross-product quantities downstream). Each is scored from the accelerometer by:
#' \enumerate{
#'   \item Median absolute roll and pitch angles (should be near zero for horizontal posture)
#'   \item Deviation of static Z-axis acceleration from expected gravity during static periods
#' }
#' Lower scores indicate better mappings; the function assumes animals spend significant time in
#' horizontal postures (e.g. resting or steady swimming).
#'
#' \strong{Interpretation.} The gravity-based score reliably resolves the \emph{vertical}
#' (dorsoventral) axis but cannot, on its own, distinguish the two horizontal axes (surge vs sway):
#' several mappings tie. When \code{use.dynamics = TRUE}, the function then disambiguates the
#' horizontal axes from diving dynamics - the correct surge axis is the one whose body pitch tracks
#' vertical velocity during dives - and the proper-rotation constraint fixes the sway axis and sign;
#' a tail-beat power check corroborates the sway axis. If there is insufficient diving signal (e.g.
#' an animal that swims flat), the surge/sway assignment is reported as \emph{ambiguous} rather than
#' guessed. The \code{resolution} and \code{confidence} elements make this explicit.
#'
#' The gyroscope and magnetometer families are then resolved against the accelerometer body frame
#' (see \code{families}). Across several deployments of one tag unit, pool the proposals with
#' \code{\link{consensusAxisMapping}}. Set \code{plot} / \code{plot.file} for a per-individual visual
#' validation panel (depth-rate vs pitch, gravity partition over a dive, per-axis spectra, mag dip).
#'
#' For tags whose metadata flags a magnetic paddle wheel (\code{tag$paddle_wheel = TRUE}), the
#' magnetometer is pre-smoothed with a 3-second rolling mean before the dip diagnostic - mirroring the
#' safeguard in \code{\link{processTagData}} - so the spinning-magnet noise does not inflate the dip
#' variance. Standard tags are unaffected.
#'
#' \strong{Performance.} The axis resolution is entirely low-frequency (gravity loading, dive pitch,
#' tilt-rate correlations, magnetic dip), so for high-rate tags it runs on a 1 Hz block-mean copy of
#' the data (an anti-aliasing low-pass) rather than the full series - typically a 20-100x reduction in
#' work with no effect on the result, which is a set of axis labels. Only the tail-beat corroboration
#' keeps a higher-rate copy (decimated just enough to hold \code{locomotor.band} below Nyquist).
#'
#' @seealso \code{\link{applyAxisMapping}}, \code{\link{consensusAxisMapping}}, \code{\link{importTagData}}
#' @examples
#' \dontrun{
#' # Imported (raw-axis) deployments; infer the IMU axis orientation per tag
#' files <- list.files("imported", pattern = "\\.rds$", full.names = TRUE)
#' qc <- checkTagMapping(data = files, plot.file = "axis_qc.pdf")
#' qc[[1]]$proposal      # best-scoring from/to mapping for the first deployment
#'
#' # Reconcile across deployments of one unit, then apply the mapping
#' qc <- consensusAxisMapping(qc)
#' oriented <- applyAxisMapping(data = files, mapping = qc)
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
                            depth.smoothing = 10,
                            speed.smoothing = 1,
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
  if (!is.null(speed.smoothing)) .assert_number(speed.smoothing, "speed.smoothing", min = 0)
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
    for (nm in names(data)) {
      .assert_columns(data[[nm]], required_cols, sprintf("data[['%s']]", nm))
      if (!inherits(data[[nm]][[datetime.col]], "POSIXct")) {
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
  results_list <- list()
  summary_records <- vector("list", n_animals)
  panel_payloads  <- vector("list", n_animals)
  failed_ids <- character(0)

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

      # load current file
      individual_data <- readRDS(file_path)

      # perform checks specific to loaded RDS files
      missing_cols <- setdiff(required_cols, names(individual_data))
      if (length(missing_cols) > 0) .abort("Missing required column(s) in {.file {basename(file_path)}}: {.val {missing_cols}}.")
      if (!inherits(individual_data[[datetime.col]], "POSIXct")) .abort("The datetime column in {.file {basename(file_path)}} must be of class {.cls POSIXct}.")
      if (is.null(attr(individual_data, "nautilus.version"))) {
        message(paste0("Warning: File '", basename(file_path), "' was likely not processed via importTagData(). It is strongly recommended to run it through importTagData() to ensure proper formatting."))
      }

      # add ID if not present
      if (!id.col %in% names(individual_data)) {
        id <- unique(individual_data[[id.col]])[1]
      }

    } else {
      # data is already in memory (list of data frames/tables)
      id <- names(data)[i]
      individual_data <- data[[i]]
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

    # convert to data.table if not already
    if (!data.table::is.data.table(individual_data)) individual_data <- data.table::as.data.table(individual_data)

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

    # vertical speed via a centered difference on (optionally smoothed) depth - the same estimate
    # processTagData uses. A raw one-sample difference of quantized depth is a coarse staircase that
    # degrades the pitch / depth-rate relationship the surge anchor relies on; smoothing removes it.
    individual_data[, vertical_speed := .verticalVelocity(individual_data[[depth.col]],
                                                          individual_data[[datetime.col]], sampling_freq,
                                                          depth.smoothing = depth.smoothing,
                                                          speed.smoothing = speed.smoothing)$velocity]

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
      site = attach_site)

    }, error = function(e) {
      failed_ids <<- c(failed_ids, id)
      summary_records[[i]] <<- list(id = id, status = "failed", n_resolved = NA_integer_,
                                    gyro = NA_character_, mag = NA_character_, message = conditionMessage(e))
      if (lvl >= 1L) cli::cli_alert_danger("{id}: {conditionMessage(e)} - skipped")
      .log_gap(lvl)
    })
  }

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
    cli::cli_text("  {tick} fully resolved:     {n_full}")
    cli::cli_text("  {warn} partially resolved: {n_part}")
    cli::cli_text("  {crss} unresolved:         {n_unres}")
    if (n_skip > 0) cli::cli_text("  {crss} skipped (no usable data): {n_skip}")
    if (n_fail > 0) cli::cli_text("  {crss} failed:                  {n_fail}")
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

  # return results, tagged with the producer so applyAxisMapping() can route + attribute it
  attr(results_list, "nautilus.mapping.producer") <- "checkTagMapping"
  return(results_list)
}



# A canonical string key for a signed-permutation matrix (body = M %*% raw), in the same
# X-col|X-sign|Y-col|Y-sign|Z-col|Z-sign order used for the `survivors` rows, so a documented config's
# matrix can be tested for membership in the candidate set by a simple string match.
#' @keywords internal
#' @noRd
.permMatrixKey <- function(M, axes) {
  parts <- character(0)
  for (i in 1:3) { j <- which(M[i, ] != 0)[1]; parts <- c(parts, axes[j], as.character(M[i, j])) }
  paste(parts, collapse = "|")
}


################################################################################
# Internal function for evaluate a given mapping ###############################
################################################################################

# Define a helper function to calculate mapping score

#
# Score ALL candidate mappings for one individual in a single pass. A signed permutation only
# reorders / sign-flips the three raw axes, so the expensive parts are rotation-invariant and are
# computed ONCE here rather than 24x: the three raw-axis static (gravity) components, the static
# vector magnitude, the static-period mask, and the per-axis static medians. Each candidate is then
# scored by indexing + sign + two atan2 calls. The score is algebraically identical to the previous
# per-candidate implementation (staticX_mapped = sign * static[perm]; magnitude is sign/perm
# invariant), so results are unchanged - only far cheaper. Returns a data.frame (one row per grid
# row, in grid order) with median_abs_roll / median_abs_pitch / median_static_az / score.
.scoreAxisMappings <- function(individual_data, permutations_grid, ax.col, ay.col, az.col,
                               window_size, pad_length, static.threshold, g.value,
                               posture.metric = "median") {

  epsilon <- 1e-7
  raw <- c(ax.col, ay.col, az.col)
  # posture term: "median" rewards near-zero roll/pitch (level resting posture; rigid mounts);
  # "mad" rewards stability regardless of the resting offset (towed / fin-clamp mounts, or unknown)
  posture <- if (identical(posture.metric, "mad"))
    function(x) stats::mad(x, na.rm = TRUE) else function(x) median(abs(x), na.rm = TRUE)

  # the three raw-axis static (gravity) components, computed once and reused by every candidate
  s <- stats::setNames(lapply(raw, function(cc) .pad_rollmean(individual_data[[cc]], window_size, pad_length)), raw)
  if (all(is.na(s[[ax.col]])) || all(is.na(s[[ay.col]])) || all(is.na(s[[az.col]]))) {
    .abort(c("All static acceleration values are NA after the rolling mean.",
             "i" = "The data is likely too short for {.arg dba.window} at this sampling rate, or contains extensive NAs (expected minimum {window_size} samples)."))
  }

  # rotation-invariant static magnitude + static-period mask (identical for all 24 candidates)
  accel_mag <- sqrt(s[[ax.col]]^2 + s[[ay.col]]^2 + s[[az.col]]^2 + epsilon)
  static_mask <- abs(accel_mag - g.value) < static.threshold
  static_mask[is.na(static_mask)] <- FALSE

  # normalized static components per raw axis; finiteness is the same for every candidate (a signed
  # permutation of finite values stays finite), so check it once
  norm <- stats::setNames(lapply(raw, function(cc) s[[cc]] / accel_mag), raw)
  finite_ok <- all(vapply(norm, function(v) all(is.finite(v)), logical(1)))

  # per-raw-axis median of the static component over the static periods (drives the static-az term)
  med_static <- if (any(static_mask))
    stats::setNames(vapply(raw, function(cc) stats::median(s[[cc]][static_mask], na.rm = TRUE), numeric(1)), raw) else NULL

  np <- nrow(permutations_grid)
  out <- data.frame(median_abs_roll = rep(NA_real_, np), median_abs_pitch = rep(NA_real_, np),
                    median_static_az = rep(NA_real_, np), score = rep(NA_real_, np))

  for (p in seq_len(np)) {
    px <- permutations_grid$newX_col[p]; py <- permutations_grid$newY_col[p]; pz <- permutations_grid$newZ_col[p]
    sx <- permutations_grid$newX_sign[p]; sy <- permutations_grid$newY_sign[p]; sz <- permutations_grid$newZ_sign[p]

    # reject (score Inf) when the static vector is degenerate or there are no static periods, exactly
    # as the previous implementation did (it then left the medians NA)
    if (!finite_ok || is.null(med_static)) { out$score[p] <- Inf; next }

    ax_norm <- sx * norm[[px]]; ay_norm <- sy * norm[[py]]; az_norm <- sz * norm[[pz]]
    roll_deg  <- atan2(ay_norm, az_norm) * (180 / pi)
    pitch_deg <- atan2(-ax_norm, sqrt(ay_norm^2 + az_norm^2 + epsilon)) * (180 / pi)

    mar <- posture(roll_deg)
    map <- posture(pitch_deg)
    msz <- sz * med_static[[pz]]                          # median(staticZ_mapped) over static periods
    sc  <- mar + map + abs(msz - g.value)

    out$median_abs_roll[p]  <- mar
    out$median_abs_pitch[p] <- map
    out$median_static_az[p] <- msz
    out$score[p] <- if (is.na(sc)) Inf else sc
  }

  out
}


################################################################################
# Internal function: break the surge/sway tie using diving dynamics ############
################################################################################

# Pearson correlation that degrades gracefully instead of erroring. stats::cor(use = "complete.obs")
# throws "no complete element pairs" when two series never overlap on finite values (e.g. a camera tag
# whose gyro columns are present but all-NA), which suppressWarnings cannot catch. Returns NA_real_
# when there are fewer than 3 complete pairs or either series is constant; every caller already treats
# a non-finite correlation as "unresolved".
#' @keywords internal
#' @noRd
.safeCor <- function(a, b) {
  ok <- is.finite(a) & is.finite(b)
  if (sum(ok) < 3L) return(NA_real_)
  a <- a[ok]; b <- b[ok]
  if (stats::sd(a) < 1e-9 || stats::sd(b) < 1e-9) return(NA_real_)
  stats::cor(a, b)
}

# Block-mean decimation to `target_fs` Hz: groups rows into fixed time buckets and averages each named
# numeric column. This is an anti-aliasing low-pass that preserves all structure below target_fs / 2,
# letting the (low-frequency) axis resolution run on far fewer rows. Returns a data.table with the
# datetime column (at bucket starts) + the averaged value columns.
#' @keywords internal
#' @noRd
.decimate <- function(dt, value.cols, datetime.col, target_fs) {
  tnum <- as.numeric(dt[[datetime.col]])
  bucket <- floor((tnum - tnum[1]) * target_fs)
  out <- dt[, lapply(.SD, mean, na.rm = TRUE), by = list(.bucket = bucket), .SDcols = value.cols]
  out[, (datetime.col) := dt[[datetime.col]][1] + .bucket / target_fs]
  out[, .bucket := NULL]
  data.table::setcolorder(out, c(datetime.col, value.cols))
  out[]
}

# Tail-beat corroboration of the chosen surge/sway/heave assignment: the body `loco.axis` (the
# expected propulsive axis) should carry the most locomotor-band power of the three. Runs on the
# tail-beat-rate copy (`tb_data`, decimated only far enough to keep the locomotor band below Nyquist),
# not the 1 Hz resolution copy. Returns the per-axis band powers (named surge/sway/heave), the
# corroboration flag, and the signed dynamic axes (reused by the diagnostic panel), or NULL if too short.
#' @keywords internal
#' @noRd
.tailBeatCorroboration <- function(tb_data, best, ax.col, ay.col, az.col, fs, locomotor.band, loco.axis = "sway") {
  win <- max(round(2.5 * fs), 3L); if (win %% 2L == 0L) win <- win + 1L
  sd <- .staticDynamicAccel(tb_data[[ax.col]], tb_data[[ay.col]], tb_data[[az.col]], win)
  dyn_by_raw <- stats::setNames(list(sd$dynamic$x, sd$dynamic$y, sd$dynamic$z), c(ax.col, ay.col, az.col))
  pw <- c(surge = .bandPower(dyn_by_raw[[best$newX_col]], fs, locomotor.band),
          sway  = .bandPower(dyn_by_raw[[best$newY_col]], fs, locomotor.band),
          heave = .bandPower(dyn_by_raw[[best$newZ_col]], fs, locomotor.band))
  if (all(!is.finite(pw))) return(NULL)
  corroborated <- is.finite(pw[[loco.axis]]) && pw[[loco.axis]] >= max(pw, na.rm = TRUE)
  list(power = pw, corroborated = corroborated, dynamic = dyn_by_raw, fs = fs)
}

# Among the static-tied candidates (which share the vertical axis but differ in the horizontal
# assignment), the correct surge axis is the one whose body pitch tracks vertical velocity during
# dives. Returns the chosen candidate (index into `near`), the pitch<->depth-rate correlation, and a
# decisiveness flag -- or NULL if there is not enough diving signal to decide. The static/dynamic
# decomposition is passed in (`sd`, computed once by the caller) to avoid recomputing it. Tail-beat
# corroboration is handled separately by .tailBeatCorroboration on the higher-rate copy.

#' @keywords internal
#' @noRd
.resolveSurge <- function(full_data, near, sd, ax.col, ay.col, az.col,
                          dive.speed.threshold, min.dynamic.corr, max.vertical.speed) {

  vs <- full_data[["vertical_speed"]]
  # Diving samples only, AND physically plausible. A depth-sensor artifact (e.g. a stuck / saturated
  # block) injects |vertical velocity| of tens to hundreds of m/s; being extreme outliers, those few
  # samples dominate the Pearson pitch <-> depth-rate correlation below and silently destroy the surge
  # decision (the correlation collapses toward zero). A swimming animal cannot move vertically faster
  # than `max.vertical.speed`, so anything beyond it is a depth artifact, not signal, and is excluded.
  mask <- is.finite(vs) & abs(vs) > dive.speed.threshold & abs(vs) <= max.vertical.speed
  if (sum(mask, na.rm = TRUE) < 30L) return(NULL)            # not enough diving signal to decide

  static_by_raw <- stats::setNames(list(sd$static$x, sd$static$y, sd$static$z), c(ax.col, ay.col, az.col))

  # pitch<->vertical-speed correlation for each tied candidate (most negative = correct surge,
  # since a descending animal (vs > 0) pitches nose-down (pitch < 0))
  corrs <- vapply(seq_len(nrow(near)), function(k) {
    surge_s <- near$newX_sign[k] * static_by_raw[[near$newX_col[k]]]
    sway_s  <- near$newY_sign[k] * static_by_raw[[near$newY_col[k]]]
    heave_s <- near$newZ_sign[k] * static_by_raw[[near$newZ_col[k]]]
    pitch <- .tiltFromAccel(surge_s, sway_s, heave_s)$pitch
    .safeCor(pitch[mask], vs[mask])
  }, numeric(1))

  ord <- order(corrs)                                        # most negative first
  best <- ord[1]; best_corr <- corrs[best]
  separation <- if (length(ord) > 1 && is.finite(corrs[ord[2]])) corrs[ord[2]] - best_corr else Inf
  decisive <- is.finite(best_corr) && best_corr <= -min.dynamic.corr && separation >= 0.15

  list(best_index = best, corr = corrs, best_corr = best_corr, separation = separation,
       decisive = decisive, n_dynamic = sum(mask, na.rm = TRUE))
}


################################################################################
# Internal function: resolve the GYROSCOPE mapping vs the body frame ###########
################################################################################

# Given the resolved accelerometer body frame (its pitch / roll time series), the gyroscope axis
# whose signal tracks roll-rate is body X (surge), the one tracking pitch-rate is body Y (sway), and
# the remainder is body Z (yaw); signs come from the correlation signs, with Z closed by handedness.
# Returns a from/to mapping (raw gyro -> body) + diagnostics, or NULL if gyro is absent / motion is
# insufficient / the assignment is ambiguous.

#' @keywords internal
#' @noRd
.resolveGyro <- function(full_data, pitch_t, roll_t, fs, min.dynamic.corr) {
  if (!all(c("gx", "gy", "gz") %in% names(full_data))) return(NULL)
  dt <- 1 / fs
  pitch_rate <- c(NA, diff(pitch_t)) / dt
  roll_rate  <- c(NA, diff(roll_t))  / dt
  # NA-safe variance gate: sd() is NA with < 2 finite values, so guard against that too (an all-NA
  # tilt would otherwise make the comparison NA and error in `if`).
  sd_p <- stats::sd(pitch_rate, na.rm = TRUE); sd_r <- stats::sd(roll_rate, na.rm = TRUE)
  if (!is.finite(sd_p) || !is.finite(sd_r) || sd_p < 1e-6 || sd_r < 1e-6) return(NULL)

  G <- list(gx = full_data$gx, gy = full_data$gy, gz = full_data$gz)
  cor_roll  <- vapply(G, function(g) .safeCor(g, roll_rate),  numeric(1))
  cor_pitch <- vapply(G, function(g) .safeCor(g, pitch_rate), numeric(1))
  # We only need the axis that best tracks roll-rate (surge) and the one tracking pitch-rate (sway).
  # The remaining (yaw) axis legitimately need NOT correlate with either - and a near-constant yaw
  # channel (e.g. after downsampling removes its high-frequency content) yields an NA correlation - so
  # require only that at least one axis correlates with each, never that all nine are finite.
  if (all(!is.finite(cor_roll)) || all(!is.finite(cor_pitch))) return(NULL)

  x_axis <- names(G)[which.max(abs(cor_roll))]      # surge: tracks roll-rate (which.max ignores NA)
  y_axis <- names(G)[which.max(abs(cor_pitch))]     # sway:  tracks pitch-rate
  if (x_axis == y_axis) return(list(decisive = FALSE, reason = "gyro surge/sway map to the same raw axis"))
  if (!is.finite(cor_roll[[x_axis]]) || !is.finite(cor_pitch[[y_axis]])) return(NULL)
  z_axis <- setdiff(names(G), c(x_axis, y_axis))
  x_sign <- sign(cor_roll[[x_axis]]); y_sign <- sign(cor_pitch[[y_axis]])

  # build body = M %*% raw with z sign chosen to make a proper rotation (det +1)
  axes <- c("gx", "gy", "gz")
  M <- matrix(0, 3, 3)
  M[1, match(x_axis, axes)] <- x_sign
  M[2, match(y_axis, axes)] <- y_sign
  M[3, match(z_axis, axes)] <- 1
  if (.signedPermDet(M) == -1L) M[3, match(z_axis, axes)] <- -1
  mapping <- .matrixToFromTo(M, axes, axes)

  strength <- min(abs(cor_roll[[x_axis]]), abs(cor_pitch[[y_axis]]))
  list(decisive = strength >= min.dynamic.corr, mapping = mapping,
       cor_roll = cor_roll[[x_axis]], cor_pitch = cor_pitch[[y_axis]], strength = strength)
}


################################################################################
# Internal function: frame-agnostic GYRO validator (strapdown consistency) #####
################################################################################

# Frame-agnostic gyroscope RESOLVER + cross-check, via the strapdown identity for the gravity DIRECTION,
# dghat/dt = -omega x ghat. For each of the 24 proper rotations R (gyro-chip -> accel-chip) it maps the
# gyro into the accel-chip frame, predicts dghat/dt from (R . omega) and the measured ghat, and
# correlates the prediction with the MEASURED dghat/dt; the true R satisfies the identity and wins.
#
# CRITICAL (learned from real data): both sides must be band-limited to the POSTURE timescale. The
# gravity direction is the static (heavily smoothed) accel, so dghat/dt lives in a sub-Hz band; the raw
# gyro is full-bandwidth and dominated by tail-beat / yaw - rotation ABOUT gravity, which omega x ghat
# cannot see. Correlating the two directly buries the signal (best r ~ 0.01 on real tags). Low-passing
# the gyro to the SAME posture band before the cross product, using a clean +/- stencil derivative, and
# restricting the correlation to the most-rotating samples lifts the correct R to r ~ 0.66-0.83 with
# clear separation on tags with posture signal, while flat swimmers stay low and ABSTAIN. The estimator
# is SELF-CONTAINED (re-derives the posture-band gravity from the raw accel at a fixed window,
# independent of the caller's dba.window) and TILT-INVARIANT (a whole-tag tilt cancels in the relative
# mapping). It resolves the gyro-vs-accel RELATIVE frame only - NOTHING about the accel vertical.
# Returns list(decisive, motion_ok, R, corr, separation, n), or NULL if the gyro/accel is absent or the
# series is too short.
#' @keywords internal
#' @noRd
.resolveGyroFrame <- function(full_data, fs, accel.cols, posture.window = 5, deriv.window = 1,
                              min.corr = 0.5, min.separation = 0.1, motion.quantile = 0.6,
                              min.motion = 1e-5) {
  if (!all(c("gx", "gy", "gz") %in% names(full_data))) return(NULL)
  if (!all(accel.cols %in% names(full_data))) return(NULL)
  n <- nrow(full_data); if (is.null(n) || n < 60L) return(NULL)
  w <- max(3L, round(posture.window * fs))                     # posture-band smoothing window (samples)
  lp <- function(x) data.table::frollmean(x, w, fill = NA, align = "center")

  # posture-band gravity DIRECTION from the accel (self-contained), and the gyro low-passed to the SAME
  # band - this is the key step that removes the tail-beat / yaw energy swamping the strapdown signal.
  sx <- lp(full_data[[accel.cols[1]]]); sy <- lp(full_data[[accel.cols[2]]]); sz <- lp(full_data[[accel.cols[3]]])
  gmag <- sqrt(sx^2 + sy^2 + sz^2)
  ghat <- cbind(sx, sy, sz) / gmag
  Wf <- cbind(lp(full_data$gx), lp(full_data$gy), lp(full_data$gz))

  # clean central difference of ghat over a +/- (deriv.window/2) stencil (a 1-sample diff of a smoothed
  # signal is noise-dominated at high rate)
  k <- max(1L, round(0.5 * deriv.window * fs))
  dgh <- (rbind(ghat[-(1:k), , drop = FALSE], matrix(NA_real_, k, 3)) -
          rbind(matrix(NA_real_, k, 3), ghat[seq_len(n - k), , drop = FALSE])) / (2 * k / fs)

  # restrict to the most-rotating samples (high SNR for the gravity-direction rate)
  mag <- sqrt(rowSums(dgh^2))
  ok0 <- is.finite(rowSums(Wf)) & is.finite(rowSums(ghat)) & is.finite(mag)
  if (sum(ok0) < 30L) return(NULL)
  thr <- stats::quantile(mag[ok0], motion.quantile, na.rm = TRUE)
  ok <- ok0 & mag >= thr
  if (sum(ok) < 30L) return(NULL)
  motion <- mean(mag[ok]^2, na.rm = TRUE)
  if (!is.finite(motion) || motion < min.motion)
    return(list(decisive = FALSE, motion_ok = FALSE, corr = NA_real_, corr_identity = NA_real_,
                separation = NA_real_, n = sum(ok)))

  Wk <- Wf[ok, , drop = FALSE]; gk <- ghat[ok, , drop = FALSE]; meas <- as.vector(dgh[ok, , drop = FALSE])
  rots <- .properRotations()
  rowcross <- function(A, B) cbind(A[, 2] * B[, 3] - A[, 3] * B[, 2],
                                   A[, 3] * B[, 1] - A[, 1] * B[, 3],
                                   A[, 1] * B[, 2] - A[, 2] * B[, 1])
  # co-die (R = identity) co-registration score: whether the RAW gyro already tracks the RAW accel's
  # gravity-direction rate. By signed-permutation invariance this equals the body-frame co-registration
  # of the derived map (accel via M, gyro via det(M)*M), so it directly scores the co-die default.
  corr_identity <- .safeCor(as.vector(-rowcross(Wk, gk)), meas)
  corrs <- vapply(rots, function(R) .safeCor(as.vector(-rowcross(Wk %*% t(R), gk)), meas), numeric(1))
  o <- order(corrs, decreasing = TRUE)
  best_corr <- corrs[o[1]]
  sep <- if (length(o) > 1 && is.finite(corrs[o[2]])) best_corr - corrs[o[2]] else Inf
  decisive <- is.finite(best_corr) && best_corr >= min.corr && sep >= min.separation
  list(decisive = decisive, motion_ok = TRUE, R = rots[[o[1]]], corr = best_corr,
       corr_identity = corr_identity, separation = sep, n = sum(ok))
}


################################################################################
# Internal function: resolve the MAGNETOMETER mapping vs the body frame ########
################################################################################

# The angle between the (earth-referenced) magnetic field and gravity is the geomagnetic dip
# complement -- constant in time only when the magnetometer is correctly aligned to the body frame.
# A mis-aligned VERTICAL makes that angle vary with posture. BUT the angle is invariant to rotations
# about gravity, so the dip alone cannot fix the magnetometer's HEADING axes (a 4-fold ambiguity,
# analogous to the accelerometer surge/sway tie -- there is no absolute heading reference).
#
# We therefore validate the common-frame hypothesis: apply the resolved ACCELEROMETER mapping to the
# magnetometer and test whether it yields a constant dip. If so, the magnetometer is consistent with
# (and adopts) the accelerometer/body frame. If a different vertical fits far better, we flag the
# magnetometer as inconsistent (a genuinely different mag frame) rather than guess its heading.
# Returns a from/to mapping + measured dip + diagnostics, or NULL if mag is absent / posture too static.

#' @keywords internal
#' @noRd
.resolveMag <- function(full_data, gbody, M_acc, fs, window_size, min.angle.sd = 8, hard.iron = TRUE) {
  if (!all(c("mx", "my", "mz") %in% names(full_data))) return(NULL)

  # provisional hard-iron (spherical) correction for the dip diagnostic only - never persisted. Many
  # deployments lack a magnetometer calibration; an uncorrected hard-iron bias rotates relative to the
  # field as the animal turns, so the dip varies even for a correctly-aligned sensor and the
  # common-frame test yields false "inconsistent" flags. Centring the sphere removes that, but only
  # when orientation coverage is sufficient (else the estimate is unreliable and is skipped).
  mxv <- full_data$mx; myv <- full_data$my; mzv <- full_data$mz
  hi <- if (hard.iron) .hardIronOffset(mxv, myv, mzv) else NULL
  hi_applied <- !is.null(hi) && isTRUE(hi$coverage_ok)
  if (hi_applied) { mxv <- mxv - hi$offset[1]; myv <- myv - hi$offset[2]; mzv <- mzv - hi$offset[3] }

  # smooth the magnetometer to suppress high-frequency noise, then unit-normalise gravity
  smx <- .rollmeanCentered(mxv, window_size)
  smy <- .rollmeanCentered(myv, window_size)
  smz <- .rollmeanCentered(mzv, window_size)
  gmag <- sqrt(rowSums(gbody^2)); ghat <- gbody / gmag
  ok <- is.finite(smx) & is.finite(smy) & is.finite(smz) & is.finite(gmag) & gmag > 0
  if (sum(ok) < 50L) return(NULL)
  mm <- cbind(smx, smy, smz)[ok, , drop = FALSE]
  gh <- ghat[ok, , drop = FALSE]

  angle_stats <- function(M) {
    rm <- mm %*% t(M)
    rn <- sqrt(rowSums(rm^2)); keep <- rn > 0
    dotp <- rowSums((rm[keep, , drop = FALSE] / rn[keep]) * gh[keep, , drop = FALSE])
    ang <- acos(pmax(-1, pmin(1, dotp))) * 180 / pi
    list(sd = stats::sd(ang, na.rm = TRUE), mean = mean(ang, na.rm = TRUE))
  }

  # the data must actually discriminate verticals (posture varied enough)
  sds <- vapply(.properRotations(), function(M) angle_stats(M)$sd, numeric(1))
  if (!is.finite(min(sds)) || (max(sds) - min(sds)) < min.angle.sd) {
    return(list(decisive = FALSE, reason = "insufficient posture variation to validate magnetometer",
                hard_iron_applied = hi_applied, hard_iron_offset = if (hi_applied) hi$offset else NULL))
  }

  # validate the accelerometer frame on the magnetometer
  acc <- angle_stats(M_acc)
  consistent <- is.finite(acc$sd) && acc$sd <= min.angle.sd && acc$sd <= min(sds) + min.angle.sd
  mapping <- .matrixToFromTo(M_acc, c("mx", "my", "mz"), c("mx", "my", "mz"))
  list(decisive = consistent, mapping = if (consistent) mapping else NULL,
       angle_sd = acc$sd, best_possible_sd = min(sds),
       measured_inclination = 90 - acc$mean, consistent_with_accel = consistent,
       hard_iron_applied = hi_applied, hard_iron_offset = if (hi_applied) hi$offset else NULL)
}


################################################################################
# Internal helpers: keep the diagnostic panels small + fast to render ##########
################################################################################

# Welch-averaged power spectrum: split the (finite) series into fixed-length, 50%-overlapping,
# Hann-windowed segments and average their periodograms. The number of frequency bins is bounded by
# the segment length (not the series length), so the spectra panel has a constant, small vector
# footprint regardless of deployment duration - and the averaged estimate is smoother than a single
# full-length FFT. Returns list(freq, power) or NULL if the series is too short.
#' @keywords internal
#' @noRd
.welchSpectrum <- function(x, fs, seg = 1024L, overlap = 0.5) {
  x <- x[is.finite(x)]
  n <- length(x)
  if (n < 16L) return(NULL)
  seg <- as.integer(min(seg, n))
  step <- max(1L, floor(seg * (1 - overlap)))
  starts <- seq.int(1L, n - seg + 1L, by = step); if (!length(starts)) starts <- 1L
  w <- 0.5 - 0.5 * cos(2 * pi * (seq_len(seg) - 1L) / (seg - 1L))   # Hann window
  wnorm <- sum(w^2)
  half <- 2:(seg %/% 2L + 1L)                                       # drop DC; positive frequencies
  acc <- numeric(length(half))
  for (s in starts) {
    seg_x <- x[s:(s + seg - 1L)]
    seg_x <- (seg_x - mean(seg_x)) * w
    acc <- acc + (Mod(stats::fft(seg_x))^2)[half] / wnorm
  }
  list(freq = (half - 1L) * fs / seg, power = acc / length(starts))
}

# Min/max-envelope decimation of a line series for plotting: bins the points and keeps the per-bin
# minimum and maximum (so spikes / the visible envelope survive), reducing a multi-thousand-point path
# to ~`n_target` points. No-op when already short enough.
#' @keywords internal
#' @noRd
.decimateForPlot <- function(x, y, n_target = 2000L) {
  n <- length(x)
  if (n <= n_target) return(list(x = x, y = y))
  bin <- as.integer(cut(seq_len(n), breaks = n_target %/% 2L, labels = FALSE))
  keep <- unlist(lapply(split(seq_len(n), bin), function(ii) {
    yy <- y[ii]; if (all(is.na(yy))) return(ii[1])
    unique(c(ii[which.min(yy)], ii[which.max(yy)]))
  }), use.names = FALSE)
  keep <- sort(keep)
  list(x = x[keep], y = y[keep])
}

# Draw an "Expected:" guide-rail caption along the bottom of the current panel, auto-sizing the text
# so it always fits the panel width (long strings shrink rather than overrun into neighbours).
#' @keywords internal
#' @noRd
.panelHint <- function(hint) {
  w <- graphics::strwidth(hint, units = "inches", cex = 1)
  cex <- if (w > 0) min(0.62, (graphics::par("pin")[1] * 0.98) / w) else 0.62
  graphics::mtext(hint, side = 1, line = 3.15, cex = cex, col = "grey45", font = 3)
}


################################################################################
# Internal function: visual validation panel for an axis-mapping result ########
################################################################################

# A 2x2 diagnostic panel: (1) the surge anchor (depth-rate vs body pitch), (2) the gravity partition
# over the strongest dive with a depth-profile backdrop, (3) the per-axis dynamic spectra (the sway
# axis should carry the tail-beat peak), and (4) the magnetometer dip angle through time. Each panel
# carries an "Expected:" guide-rail caption, and body axes are labelled surge (X) / sway (Y) /
# heave (Z). The series are subsampled / envelope-decimated / Welch-averaged so the PDF stays small.

# Map an algorithm verdict to a panel colour + title prefix (pass/fail/warn; "skip" = no border).
#' @keywords internal
#' @noRd
.panelStatus <- function(state, use_unicode = TRUE) {
  switch(state,
    pass = list(col = "green4",     pre = if (use_unicode) "[\u2714] " else "[v] "),
    fail = list(col = "red3",       pre = if (use_unicode) "[\u2716] " else "[x] "),
    warn = list(col = "darkorange", pre = "[!] "),
    list(col = NA, pre = ""))   # skip / absent -> no border, no prefix
}

#' @keywords internal
#' @noRd
.plotTagMappingPanel <- function(pd, use_unicode = TRUE) {
  oldpar <- graphics::par(no.readonly = TRUE); on.exit(graphics::par(oldpar), add = TRUE)
  # extra bottom margin for the guide-rail caption; extra right margin for the depth axis in panel 2
  graphics::par(mfrow = c(2, 2), mar = c(4.4, 4, 2.6, 3), mgp = c(2.2, 0.7, 0), oma = c(0, 0, 2.4, 0))
  fl <- pd$flags
  drawbox <- function(st) if (!is.na(st$col)) graphics::box(col = st$col, lwd = 2)

  # 1. surge anchor: pitch vs depth-rate (pass when surge resolved). Pre-thinned scatter; r on all.
  # When the surge frame is provisional (vertical not yet pinned), the correlation can look strong but
  # was NOT used to resolve - say so, so the plotted r never contradicts the red border.
  st <- .panelStatus(if (isTRUE(fl$surge_resolved)) "pass" else "fail", use_unicode)
  sa_sub <- if (!isTRUE(fl$surge_resolved) && !isTRUE(fl$vertical_resolved)) " (provisional frame: vertical unresolved)" else ""
  if (!is.null(pd$sa) && pd$sa$n > 5) {
    graphics::plot(pd$sa$pitch, pd$sa$vs, pch = 16, cex = 0.3, col = "#3366aa55",
                   xlab = "Body pitch (deg)", ylab = "Vertical speed (m/s)",
                   main = paste0(st$pre, "Surge anchor: pitch vs depth-rate", sa_sub),
                   cex.main = 0.95, cex.axis = 0.8, cex.lab = 0.9, las = 1)
    graphics::abline(h = 0, col = "grey70", lty = 3)
    graphics::legend("topright", legend = sprintf("r = %.2f", pd$sa$r), bty = "n", cex = 0.9)
    .panelHint("Expected: Tight negative correlation (animal pitches nose-down to descend)")
    drawbox(st)
  } else { graphics::plot.new(); graphics::title("Surge anchor: insufficient diving signal", cex.main = 0.95) }

  # 2. gravity partition over the strongest dive, with the depth profile as a soft filled backdrop on
  # a secondary (inverted) axis (pass when vertical resolved). Upper ylim padded so the legend clears.
  st <- .panelStatus(if (isTRUE(fl$vertical_resolved)) "pass" else "fail", use_unicode)
  gp <- pd$gp; tt <- gp$t; dep <- gp$depth
  if (length(tt) >= 2) {
    yl <- range(c(gp$surge, gp$sway, gp$heave), na.rm = TRUE); yl[2] <- yl[2] + 0.18 * diff(yl)
    if (!is.null(dep) && any(is.finite(dep))) {
      dr <- range(dep, na.rm = TRUE); if (diff(dr) < 1e-6) dr <- dr + c(-0.5, 0.5)
      graphics::plot(tt, dep, type = "n", axes = FALSE, ann = FALSE, ylim = c(dr[2], dr[1]))  # surface at top
      fin <- is.finite(dep)
      graphics::polygon(c(tt[fin][1], tt[fin], tt[fin][sum(fin)]), c(dr[1], dep[fin], dr[1]),
                        col = "#9ecae133", border = NA)                  # water column above the animal
      graphics::axis(4, col = "#4a78a8", col.axis = "#4a78a8", cex.axis = 0.75)
      graphics::mtext("Depth (m)", side = 4, line = 1.8, cex = 0.7, col = "#4a78a8")
      graphics::par(new = TRUE)
    }
    graphics::plot(tt, gp$heave, type = "l", col = "black", lwd = 1.2, ylim = yl,
                   xlab = "Time (s)", ylab = "Static accel (g)",
                   main = paste0(st$pre, "Gravity partition over a dive"),
                   cex.main = 0.95, cex.axis = 0.8, cex.lab = 0.9, las = 1)
    graphics::lines(tt, gp$surge, col = "#cc3333", lwd = 1.2)
    graphics::lines(tt, gp$sway,  col = "#3399cc", lwd = 1.2)
    graphics::legend("topright", legend = c("heave (Z)", "surge (X)", "sway (Y)"),
                     col = c("black", "#cc3333", "#3399cc"), lwd = 1.2, bty = "n", cex = 0.8, horiz = TRUE)
    .panelHint("Expected: Gravity shifts from Heave (Z) to Surge (X) during a dive; Sway (Y) stays near 0g.")
    drawbox(st)
  } else { graphics::plot.new(); graphics::title("Gravity partition: insufficient data", cex.main = 0.95) }

  # 3. per-axis dynamic spectra (the locomotor axis should carry the tail beat). Welch-averaged.
  st <- .panelStatus(if (isTRUE(fl$tb_corroborated)) "pass" else "warn", use_unicode)
  loco_title <- sprintf("%s (%s)", tools::toTitleCase(pd$loco.axis), pd$loco_role)
  loco_note  <- c(sway = "tail beats are lateral", heave = "dorso-ventral oscillation",
                  surge = "fore-aft oscillation")[[pd$loco.axis]]
  if (!is.null(pd$sp)) {
    sp <- pd$sp; cols <- c(surge = "#cc3333", sway = "#3399cc", heave = "grey40")
    lwds <- c(surge = 1, sway = 1, heave = 1); lwds[pd$loco.axis] <- 2.4   # emphasise the locomotor axis
    band_mask <- sp$freq >= pd$locomotor.band[1] & sp$freq <= pd$locomotor.band[2] * 1.6
    ymax <- max(c(sp$surge[band_mask], sp$sway[band_mask], sp$heave[band_mask]), na.rm = TRUE) * 1.1
    if (!is.finite(ymax) || ymax <= 0) ymax <- max(c(sp$surge, sp$sway, sp$heave), na.rm = TRUE)
    graphics::plot(sp$freq, sp$surge, type = "l", col = cols[["surge"]], lwd = lwds[["surge"]], ylim = c(0, ymax),
                   xlab = "Frequency (Hz)", ylab = "Power",
                   main = paste0(st$pre, "Per-axis spectra (", loco_title, " = tail beat)"),
                   cex.main = 0.95, cex.axis = 0.8, cex.lab = 0.9, las = 1)
    graphics::rect(pd$locomotor.band[1], 0, pd$locomotor.band[2], ymax, col = "#00000010", border = NA)
    graphics::lines(sp$freq, sp$sway,  col = cols[["sway"]],  lwd = lwds[["sway"]])
    graphics::lines(sp$freq, sp$heave, col = cols[["heave"]], lwd = lwds[["heave"]])
    graphics::legend("topright", legend = c("surge (X)", "sway (Y)", "heave (Z)"), col = cols,
                     lwd = lwds[c("surge", "sway", "heave")], bty = "n", cex = 0.8)
    .panelHint(sprintf("Expected: Tallest peak on %s in the shaded locomotor band (%s).", loco_title, loco_note))
    drawbox(st)
  } else { graphics::plot.new(); graphics::title("Per-axis spectra: too short", cex.main = 0.95) }

  # 4. magnetometer dip angle over time (pass = resolved, fail = unresolved/inconsistent, no border = absent)
  ms <- fl$mag_status %||% "absent/insufficient"
  st <- .panelStatus(if (identical(ms, "resolved")) "pass"
                     else if (ms %in% c("unresolved", "inconsistent")) "fail" else "skip", use_unicode)
  if (!is.null(pd$md)) {
    md <- pd$md
    graphics::plot(md$t, md$angle, type = "l", col = "#996600", xlab = "Time (min)",
                   ylab = "angle(g, mag) (deg)", main = paste0(st$pre, "Magnetometer dip"),
                   cex.main = 0.95, cex.axis = 0.8, cex.lab = 0.9, las = 1)
    graphics::abline(h = md$mean, col = "grey60", lty = 3)
    if (is.finite(md$expected)) {
      graphics::abline(h = md$expected, col = "red2", lty = 2)
      graphics::legend("topright", legend = "IGRF expected", col = "red2", lty = 2, bty = "n", cex = 0.75)
    }
    .panelHint("Expected: Flat line near the dashed IGRF reference. Wild swings indicate an unresolved frame.")
    drawbox(st)
  } else { graphics::plot.new(); graphics::title("Magnetometer: absent", cex.main = 0.95) }

  # overall title with the per-axis resolution summary
  res <- pd$resolution
  summ <- paste(sprintf("%s:%s", res$body_axis, substr(res$status, 1, 3)), collapse = "  ")
  graphics::mtext(sprintf("%s   -   axis mapping   [%s]", pd$id, summ), outer = TRUE, font = 2, cex = 1)
  invisible(NULL)
}


################################################################################
# Internal function: run-summary page (page 1 of the diagnostic PDF) ###########
################################################################################

# A text/table page: aggregate run stats + a per-tag table sorted worst-first (failed, then unresolved,
# partial, resolved) so the tags needing manual review are at the top. Auto-paginated.
#' @keywords internal
#' @noRd
.drawTagMappingSummaryPage <- function(records, stats, use_unicode = TRUE) {
  oldpar <- graphics::par(no.readonly = TRUE); on.exit(graphics::par(oldpar), add = TRUE)
  records <- Filter(Negate(is.null), records)
  rankv <- c(failed = 0, skipped = 1, unresolved = 2, partial = 3, resolved = 4)
  if (length(records)) {
    ord <- order(vapply(records, function(r) rankv[[r$status]], numeric(1)),
                 vapply(records, function(r) { v <- r$n_resolved; if (is.null(v) || is.na(v)) -1 else v }, numeric(1)))
    records <- records[ord]
  }

  # ---- glyphs (with the established ASCII fallback for fonts lacking the marks) -------------------
  g_tick <- if (use_unicode) "\u2713" else "v"
  g_cross <- if (use_unicode) "\u2717" else "x"
  g_dash <- if (use_unicode) "\u2013" else "-"
  ell <- if (use_unicode) "\u2026" else ".."
  deg <- if (use_unicode) "\u00b0" else " deg"
  dot <- if (use_unicode) "\u00b7" else "|"

  # ---- small formatting helpers ------------------------------------------------------------------
  nz  <- function(s) !is.null(s) && length(s) == 1 && !is.na(s) && nzchar(as.character(s))
  truncr <- function(s, n) { if (!nz(s)) return("-"); s <- as.character(s); if (nchar(s) > n) paste0(substr(s, 1, n - 1), ell) else s }
  truncl <- function(s, n) { if (!nz(s)) return("-"); s <- as.character(s); if (nchar(s) > n) paste0(ell, substr(s, nchar(s) - n + 2, nchar(s))) else s }
  model_pkg <- function(r) { if (!nz(r$model) && !nz(r$package_id)) return("-")
    paste0(truncr(r$model, 7), "/", truncr(r$package_id, 8)) }
  site_fmt <- function(s) { if (!nz(s)) return("-")
    if (grepl("left",  s, ignore.case = TRUE) && grepl("pect", s, ignore.case = TRUE)) return("L pect")
    if (grepl("right", s, ignore.case = TRUE) && grepl("pect", s, ignore.case = TRUE)) return("R pect")
    truncr(s, 7) }
  gyro_word <- function(s) if (!nz(s)) "-" else switch(s, resolved = "resolved", unresolved = "unresolved",
                                                       "absent/insufficient" = "absent", s)
  mag_word  <- function(s) if (!nz(s)) "-" else switch(s, resolved = "consistent", inconsistent = "inconsistent",
                                                       unresolved = "unresolved", "absent/insufficient" = "absent", s)
  word_col  <- function(s) if (!nz(s)) "grey60" else switch(s, resolved = "green4", inconsistent = "red3",
                                                            unresolved = "darkorange", "absent/insufficient" = "grey55", "grey30")
  remap_col <- function(s) switch(s %||% "None", Confirmed = "green4", Proposed = "darkorange", "grey55")
  id_col    <- function(s) switch(s, resolved = "grey10", partial = "darkorange", unresolved = "red3",
                                  failed = "red3", skipped = "grey55", "grey10")
  # numeric cells -> list(text, colour, font); bold-red when outside healthy parameters
  f_static <- function(s) { if (!nz(s)) return(list("-", "grey60", 1L)); s <- as.numeric(s)
    txt <- if (s < 600) sprintf("%.0fs", s) else sprintf("%.0fm", s / 60)
    if (s < 30) list(txt, "red3", 2L) else if (s < 60) list(txt, "darkorange", 1L) else list(txt, "grey20", 1L) }
  f_dive <- function(p) { if (!nz(p)) return(list("-", "grey60", 1L)); p <- as.numeric(p)
    txt <- sprintf("%.0f%%", p)
    if (p < 5) list(txt, "red3", 2L) else if (p < 10) list(txt, "darkorange", 1L) else list(txt, "grey20", 1L) }
  f_surge <- function(r) { if (!nz(r)) return(list("-", "grey60", 1L)); r <- as.numeric(r)
    if (abs(r) < 0.4) list(sprintf("%+.2f", r), "red3", 2L) else list(sprintf("%+.2f", r), "grey20", 1L) }
  f_magd <- function(d) { if (!nz(d)) return(list("-", "grey60", 1L)); d <- as.numeric(d)
    txt <- sprintf("%+.0f%s", d, deg)
    if (abs(d) > 10) list(txt, "red3", 2L) else list(txt, "grey20", 1L) }
  cell <- function(x, y, v, cx_) graphics::text(x, y, v[[1]], adj = c(0, 0.5), cex = cx_, col = v[[2]], font = v[[3]])

  # ---- column anchors (normalized x; tuned for the 10 in landscape canvas) ------------------------
  cx <- c(id = 0.004, model = 0.118, ax = 0.248, ay = 0.270, az = 0.292, remap = 0.318,
          gyro = 0.420, mag = 0.520, tail = 0.638, site = 0.716, static = 0.798,
          dive = 0.850, surge = 0.898, magd = 0.958)
  hdr_lab <- c(id = "Tag ID", model = "Model / Pkg", ax = "X", ay = "Y", az = "Z", remap = "Remapping",
               gyro = "Gyro", mag = "Mag", tail = "Tail-Beat", site = "Site", static = "Static",
               dive = "Dive", surge = "Surge r", magd = if (use_unicode) "Mag \u0394" else "Mag d")
  cex_h <- 0.62; cex_d <- 0.58

  rpp <- 29L
  npg <- max(1L, ceiling(max(1L, length(records)) / rpp))
  for (pg in seq_len(npg)) {
    idx <- if (length(records) == 0) integer(0) else ((pg - 1L) * rpp + 1L):min(pg * rpp, length(records))
    graphics::par(mar = c(1.5, 0.6, 1, 0.6)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))

    graphics::text(0, 0.985, "checkTagMapping  -  run summary", adj = c(0, 1), font = 2, cex = 1.4)
    graphics::text(1, 0.985, format(Sys.time(), "%Y-%m-%d %H:%M"), adj = c(1, 1), cex = 0.75, col = "grey40")
    agg <- sprintf("%d tags  |  resolved %d   partial %d   unresolved %d%s%s",
                   stats$n_animals, stats$n_full, stats$n_part, stats$n_unres,
                   if (stats$n_skip > 0) sprintf("   skipped %d", stats$n_skip) else "",
                   if (stats$n_fail > 0) sprintf("   failed %d", stats$n_fail) else "")
    graphics::text(0, 0.93, agg, adj = c(0, 1), cex = 0.95)
    graphics::text(0, 0.90, sprintf("locomotor axis: %s   -   sorted worst-first for review", stats$locomotor.axis),
                   adj = c(0, 1), cex = 0.72, col = "grey45")

    # header band + bold header + rule
    hcy <- 0.855; dy <- 0.0265
    graphics::rect(0, hcy - 0.016, 1, hcy + 0.016, col = "grey90", border = NA)
    for (k in names(cx)) {
      ax_axis <- k %in% c("ax", "ay", "az")
      graphics::text(cx[[k]], hcy, hdr_lab[[k]], adj = if (ax_axis) c(0.5, 0.5) else c(0, 0.5),
                     font = 2, cex = cex_h, col = "grey15")
    }
    graphics::segments(0, hcy - 0.018, 1, hcy - 0.018, col = "grey55")

    row_top <- hcy - 0.040
    for (j in seq_along(idx)) {
      r <- records[[idx[j]]]; y <- row_top - dy * (j - 1L)
      if (j %% 2L == 0L) graphics::rect(0, y - dy / 2, 1, y + dy / 2, col = "grey96", border = NA)  # zebra
      graphics::text(cx[["id"]], y, truncl(r$id, 13), adj = c(0, 0.5), cex = cex_d, font = 2, col = id_col(r$status))
      graphics::text(cx[["model"]], y, model_pkg(r), adj = c(0, 0.5), cex = cex_d, col = "grey30")

      # failed / skipped: no per-axis data -> a single spanning status note
      if (r$status %in% c("failed", "skipped")) {
        msg <- r$message %||% r$status
        graphics::text(cx[["ax"]], y, paste0(r$status, " ", g_dash, " ", truncr(msg, 70)),
                       adj = c(0, 0.5), cex = cex_d, font = 3, col = id_col(r$status))
        next
      }

      # per-axis X / Y / Z verdict
      for (a in c("ax", "ay", "az")) {
        ax_name <- toupper(substr(a, 2, 2))
        ok <- isTRUE(r$axes[[ax_name]])
        graphics::text(cx[[a]], y, if (ok) g_tick else g_cross, adj = c(0.5, 0.5), cex = cex_d,
                       font = 2, col = if (ok) "green4" else "red3")
      }
      graphics::text(cx[["remap"]], y, r$remap %||% "None", adj = c(0, 0.5), cex = cex_d, col = remap_col(r$remap))
      graphics::text(cx[["gyro"]], y, gyro_word(r$gyro), adj = c(0, 0.5), cex = cex_d, col = word_col(r$gyro))
      graphics::text(cx[["mag"]],  y, mag_word(r$mag),   adj = c(0, 0.5), cex = cex_d, col = word_col(r$mag))
      tb <- r$tailbeat
      graphics::text(cx[["tail"]], y, if (!nz(tb)) g_dash else if (isTRUE(tb)) g_tick else g_cross,
                     adj = c(0.5, 0.5), cex = cex_d, font = 2,
                     col = if (isTRUE(tb)) "green4" else if (identical(tb, FALSE)) "grey55" else "grey60")
      graphics::text(cx[["site"]], y, site_fmt(r$site), adj = c(0, 0.5), cex = cex_d, col = "grey30")
      cell(cx[["static"]], y, f_static(r$static_secs), cex_d)
      cell(cx[["dive"]],   y, f_dive(r$diving_pct),    cex_d)
      cell(cx[["surge"]],  y, f_surge(r$surge_corr),   cex_d)
      cell(cx[["magd"]],   y, f_magd(r$mag_resid),     cex_d)
    }

    # footer legend + pagination
    graphics::text(0, 0.018, sprintf("bold red = outside healthy range:  static <60s  %s  dive <10%%  %s  |surge r| <0.4  %s  mag residual >10%s", dot, dot, dot, deg),
                   adj = c(0, 0), cex = 0.62, col = "grey45")
    if (npg > 1) graphics::mtext(sprintf("summary page %d / %d", pg, npg), side = 1, cex = 0.7, col = "grey50")
  }
  invisible(NULL)
}


################################################################################
# Helper function for padded rolling sum #######################################
################################################################################

.pad_rollsum <- function(x, window, pad_len) {
  if (length(x) == 0) return(numeric(0))
  if (length(x) < window) {
    # If data is shorter than window, return NAs for the entire sequence
    return(rep(NA_real_, length(x)))
  }

  # Symmetric padding using first/last values
  padded <- c(rep(x[1], pad_len), x, rep(x[length(x)], pad_len))
  # Compute centered rolling sum on padded data
  rolled <- data.table::frollsum(padded, n = window, align = "center", na.rm = FALSE) # na.rm=FALSE if sum should be NA if any in window is NA
  # Trim to original length
  return(rolled[(pad_len + 1):(pad_len + length(x))])
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
