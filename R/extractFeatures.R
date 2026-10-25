#######################################################################################################
# Function to extract features from a sliding or aggregated window ####################################
#######################################################################################################

#' Extract windowed features from sensor data
#'
#' @description
#' Summarise sensor channels over time windows to build a feature matrix for behavioural inference.
#' Features at different scales share output timestamps. Windows may slide at an independent stride
#' or start at regularly spaced anchors, with optional segment boundaries and channel coverage checks.
#'
#' @param data A list of tables/data frames (one per deployment), a single table/data frame with an
#'   identifier column, or a character vector of paths to sensor `.rds` files.
#' @param variables Character vector of channels. Together with `metrics`, defines a full grid.
#'   Ignored when `parameter.grid` is supplied.
#' @param metrics Character vector of summaries; see Details. Ignored with `parameter.grid`.
#' @param parameter.grid A data frame of `variable` and `metric` pairs, optionally with a positive
#'   `window_seconds` per row. Missing durations use `window.size`. Output names must be unique.
#' @param enhanced.features Logical. Enable the composite descriptors listed in Details.
#' @param id.col Identifier column, default `"ID"`.
#' @param datetime.col POSIXct timestamp column, default `"datetime"`.
#' @param window.size Default window duration in seconds, also used for response aggregation.
#' @param aggregate Logical. With `FALSE` (default), use centred sliding windows. With `TRUE`, each
#'   output timestamp is the start of its window. The default stride then equals `window.size`.
#' @param downsample.to Optional output frequency in Hz for averaging already computed features into
#'   bins. Circular means and medians are averaged as unit vectors; other features use arithmetic
#'   means. Bins start at the first input timestamp of each segment, including when edge rows are
#'   omitted. Use `step.size` to compute features directly at a coarser cadence without averaging them.
#' @param response.col Optional column of binary response labels (numeric, logical, or factor 0/1).
#' @param circular.variables Channels interpreted as angles in degrees, default `c("heading", "roll")`.
#'   Applies to basic summaries and to differences in movement and orientation descriptors.
#' @param response.aggregation `"majority"` (mean of observed labels greater than 0.5) or `"any"`
#'   (at least one observed 1). Entirely unlabelled windows and output bins remain `NA`.
#' @param missing.features `"omit"` (default) drops rows containing missing features or responses;
#'   `"keep"` retains them. Neither option imputes unavailable measurements.
#' @param return.data Logical. Return a named list of feature tables (default `TRUE`); `FALSE` returns
#'   paths to saved `.rds` files and requires `output.dir`.
#' @param output.dir Existing directory for one `<id>.rds` file per deployment, or `NULL` to skip saving.
#' @param output.suffix Optional file name suffix before `.rds`.
#' @param compress Compression passed to [base::saveRDS()], default `TRUE`.
#' @param n.cores Number of processor cores, default 1.
#' @param verbose `FALSE`/0 (quiet), `TRUE`/1/`"normal"` (default), or 2/`"detailed"` (progress and diagnostics).
#' @param step.size Optional positive stride in seconds, independent of window duration. Defaults to
#'   one sample for sliding windows and `window.size` for start-anchored windows. Both widths and
#'   stride are rounded to the nearest whole number of samples, with a minimum of one sample.
#' @param segment.col Optional column identifying contiguous segments. A change of label starts a new
#'   segment, even if a previous label reappears later. Missing segment labels produce `NA` features.
#'   Windows, derivatives and downsampling bins never combine segments. The column is kept in output.
#' @param min.coverage Minimum fraction of finite samples per input channel in a window, either a
#'   scalar for all channels or a named numeric vector (e.g. `c(heading = 0.8, depth = 0.9)`). Unnamed
#'   channels in a named vector default to 0. The default 0 accepts any observed data; entirely missing
#'   channels always return `NA`. Composite features must meet the threshold for every required channel.
#'   Coverage uses the requested width, including for truncated start-anchored windows.
#'
#' @details
#' ## Window alignment and segments
#'
#' Input must have finite, strictly increasing POSIXct timestamps on a regular sampling grid within
#' each segment. Sampling rate comes from metadata, falling back to the median timestamp interval.
#' A timestamp gap greater than 1.5 sample intervals automatically starts a new segment. Use
#' [regularizeTimeSeries()] first for irregular records.
#'
#' Anchors begin at the first sample of each segment and advance by `step.size`. Every grid row is
#' evaluated at these same anchors, regardless of its width; no positional padding or truncation of
#' feature vectors is applied. With sliding windows, a width of `w` samples at row `i` spans
#' `i - floor((w - 1)/2)` through `i + ceiling((w - 1)/2)`. Thus a four-sample window spans
#' `(i-1):(i+2)`. All metrics and responses use this convention. Incomplete sliding spans at segment
#' edges return `NA`.
#'
#' With `aggregate = TRUE`, widths extend forward from each anchor. The last window is clipped to
#' the segment end; increase `min.coverage` to reject short final spans. At the default stride, the
#' default-width windows tile the record. Different widths or an explicit stride can overlap or leave
#' gaps. Response labels always use `window.size`, with the same alignment and stride as features.
#'
#' ## Basic metrics
#'
#' Linear channels support `mean`, `median`, `sd`, `range`, `min`, `max`, `iqr`, `mad`, `sum`, `rate`,
#' `energy`, `skewness`, `kurtosis` and `entropy`. Circular channels support `mean`, `median`, `sd`,
#' `range`, `iqr`, `mrl` and `rate`. Incompatible basic pairs are rejected before processing.
#'
#' Circular spread uses angles modulo 360. Circular IQR is the type-7 linear IQR after cutting the
#' circle in its largest empty arc; tied largest gaps use the smallest resulting IQR. This definition
#' is rotationally invariant, including across north. Circular means use `atan2(sum(sin), sum(cos))`;
#' a vanishing resultant has no defined mean or circular SD and returns `NA`. Means report degrees
#' in `[-180, 180]`, while medians report an observed direction modulo 360. Only absolute heading
#' means and medians depend on magnetic versus geographic north; see [processTagData()].
#'
#' Non-finite sensor values are treated as unavailable. Empty windows return `NA`, including sums,
#' energy and extrema. Rate features use only consecutive observed samples: missing observations are
#' never deleted before differencing. Rates are expressed per second; angles use shortest signed
#' steps, including roll by default. `energy` is the sum of squared samples, not a time integral.
#'
#' For observed, constant depth or vertical-speed windows entirely at the surface (`depth == 0`),
#' undefined shape summaries use `skewness = 0`, `kurtosis = 3`, `entropy = 0`. This applies at every
#' scale and never replaces missing channels, insufficient coverage or incomplete sliding spans.
#'
#' ## Enhanced features
#'
#' Enable with `enhanced.features = TRUE`. Heading-specific metrics require variable `heading`;
#' depth-specific metrics require `depth`. The composite descriptors use sentinel variables called
#' `posture` and `activity`: those columns must exist, but their values are ignored.
#' \describe{
#'   \item{`net_heading_change` -> `heading_net_change`}{Absolute shortest angular difference between
#'     the window's first and last headings, in degrees.}
#'   \item{`cumulative_heading_change` -> `heading_cumulative_change`}{Sum of absolute shortest steps,
#'     in degrees, between adjacent observed headings within the window.}
#'   \item{`circular_variance_heading` -> `heading_circular_variance`}{`1 - R`, with mean resultant length `R`.}
#'   \item{`turning_rate_variability` -> `heading_turning_variability`}{Coefficient of variation (CV)
#'     of absolute heading rates. Zero for at least two identical rates, `NA` for fewer than two.}
#'   \item{`circling_behavior` -> `heading_circling`}{Sum of absolute shortest steps divided by
#'     `(absolute shortest endpoint change + 1 degree)`. The dimensionless index is high for repeated
#'     turning with small endpoint change, including alternating turns. It is not an event detector;
#'     use [detectCircling()] for sustained directional rotations.}
#'   \item{`uturn_flag` -> `heading_uturn`}{1 if shortest endpoint change exceeds 120 degrees, otherwise 0.}
#'   \item{`heading_autocorr_avg` -> `heading_autocorr_avg`}{Mean autocorrelation over lags 1 to 5 of
#'     locally unwrapped headings, requiring at least ten observed samples and nonzero variation.
#'     Missing samples retain their lag positions; unwrapping resumes after a gap on the nearest branch.}
#'   \item{`oscillation_regularity`}{CV of inter-peak intervals; requires at least three peaks and ten
#'     observed samples in the window. Lower values indicate more regular oscillation.}
#'   \item{`movement_jerk`}{RMS first derivative, in input units per second. Applied to acceleration,
#'     this is per-channel jerk; the rotation-invariant native-rate `jerk` channel from [processTagData()]
#'     is a distinct quantity.}
#'   \item{`movement_smoothness`}{RMS second derivative, in input units per second squared. Higher
#'     values indicate less smooth movement.}
#'   \item{`movement_predictability`}{CV of centred rolling means over the window, using
#'     `sd / (abs(mean) + 0.001)`. Higher values indicate less predictable movement.}
#'   \item{`movement_consistency`}{CV of centred rolling SDs, using `sd / (mean + 0.001)`.
#'     Both second-order descriptors need at least half a window of finite rolling inputs and can
#'     therefore lose additional rows near segment edges. Circular inputs are unwrapped per segment.}
#'   \item{`posture_stability` -> `posture_stability`}{`1 / (1 + sd(pitch) + circular_sd(roll))` by
#'     default; approaches 1 for steady posture. Requires pitch and roll. Pair with variable `posture`.}
#'   \item{`activity_index` -> `activity_index`}{Mean of the summed absolute pitch, roll and heading
#'     rates, in degrees per second. Requires all three channels at each contributing step.
#'     Pair with variable `activity`.}
#'   \item{`rolling_autocorrelation`}{Lag-1 autocorrelation. Circular inputs are locally unwrapped;
#'     missing samples keep their positions. Requires four observed samples and nonzero variation.}
#'   \item{`zero_crossing_rate`}{Number of consecutive observed samples crossing the window mean,
#'     divided by the number of samples in the window. This is a dimensionless proportion.}
#'   \item{`depth_change_rate` -> `depth_change_rate`}{Mean absolute first derivative of depth within
#'     the window, in depth units per second. Formerly an unwindowed per-sample difference.}
#'   \item{`depth_change_consistency` -> `depth_change_consistency`}{CV of those absolute depth rates.}
#' }
#'
#' ## Output schema and compatibility
#'
#' One column per grid row, named `<variable>_<metric>` (or the abbreviated enhanced name above).
#' A `_<seconds>s` suffix is appended only when that row's width differs from `window.size`.
#' Identifier and timestamp come first, followed by the optional segment and response columns.
#' Processing history records the resolved grid, sampling rate, stride and coverage controls.
#'
#' Default cadence, argument positions, feature names and complete-case filtering are preserved.
#' Corrected circular, missing-data, alignment and rate calculations intentionally change affected
#' feature values; models fitted to the former values should be retrained.
#'
#' @return A named list of `data.table`s, one per deployment, or paths to saved files when
#'   `return.data = FALSE`. A deployment with no complete requested features has zero rows under
#'   `missing.features = "omit"`; use `"keep"` to inspect its timeline.
#' @seealso [processTagData()], [annotateData()], [detectCircling()], [regularizeTimeSeries()].
#' @examples
#' df <- data.frame(
#'   ID = "shark01", datetime = as.POSIXct("2024-05-30 12:00:00", tz = "UTC") + 0:19,
#'   vedba = abs(sin(seq(0, 4, length.out = 20))))
#' extractFeatures(list(shark01 = df), variables = "vedba", metrics = c("mean", "sd"),
#'                 window.size = 5, step.size = 1)
#' grid <- data.frame(variable = "vedba", metric = "mean", window_seconds = c(3, 7))
#' extractFeatures(list(shark01 = df), parameter.grid = grid, window.size = 3,
#'                 step.size = 1, min.coverage = 0.8, missing.features = "keep")
#' @export

extractFeatures <- function(data,
                            variables = NULL,
                            metrics = NULL,
                            parameter.grid = NULL,
                            enhanced.features = FALSE,
                            id.col = "ID",
                            datetime.col = "datetime",
                            window.size = 5,
                            aggregate = FALSE,
                            downsample.to = NULL,
                            response.col = NULL,
                            response.aggregation = c("majority", "any"),
                            missing.features = c("omit", "keep"),
                            circular.variables = c("heading", "roll"),
                            return.data = TRUE,
                            output.dir = NULL,
                            output.suffix = NULL,
                            compress = TRUE,
                            n.cores = 1,
                            verbose = "normal",
                            step.size = NULL,
                            segment.col = NULL,
                            min.coverage = 0) {

  ##############################################################################
  # Initial checks and setup ###################################################
  ##############################################################################

  start.time <- Sys.time()
  is_filepaths <- is.character(data)
  .assert_nonempty(data, "data")             # loud failure on empty input (e.g. a typo'd list.files() -> character(0))
  .assert_compress(compress)

  # data.frames are lists too: distinguish tables from per-deployment collections.
  if (!is_filepaths && is.data.frame(data)) {
    if (!id.col %in% names(data))
      .abort("{.arg data} must contain the {.val {id.col}} column.")
    data <- split(data, f = data[[id.col]], drop = TRUE)
  }

  # Validate output parameters
  .assert_flag(return.data, "return.data")
  .assert_output(return.data, output.dir)

  # Validate enhanced.features parameter
  if (!is.logical(enhanced.features)) {
    .abort("{.arg enhanced.features} must be a single logical value.")
  }

  lvl <- .verbosity(verbose)
  start.time <- Sys.time()

  # Structural argument validation, up front. These used to fail deep inside the loop with errors that
  # named an internal symbol rather than the argument at fault: a mistyped `response.aggregation` died
  # with "object 'lab' not found", `window.size = 0` was accepted and silently produced zero rows, and
  # `n.cores = 0` took the parallel branch with no backend registered ("could not find function
  # %dopar%"). An argument error should name the argument.
  response.aggregation <- match.arg(response.aggregation, c("majority", "any"))
  missing.features <- match.arg(missing.features)
  .assert_number(window.size, "window.size", min = 0)
  # strictly positive, checked separately: `min = 0` would accept 0, which used to be ACCEPTED and
  # silently returned zero rows. Expressing it as an epsilon bound would print
  # "must be between 2.22e-16 and Inf", which tells the reader nothing.
  if (window.size <= 0)
    .abort(c("{.arg window.size} must be greater than zero (it is a duration in seconds).",
             "x" = "Got {.val {window.size}}."))
  .assert_flag(aggregate, "aggregate")
  .assert_number(step.size, "step.size", min = 0, null_ok = TRUE)
  if (!is.null(step.size) && step.size <= 0)
    .abort("{.arg step.size} must be greater than zero (seconds).")
  if (!is.null(segment.col)) .assert_string(segment.col, "segment.col")
  if (!is.null(segment.col) && segment.col %in% c(id.col, datetime.col, response.col))
    .abort("{.arg segment.col} must differ from the identifier, datetime and response columns.")
  if (!is.numeric(min.coverage) || !length(min.coverage) || any(!is.finite(min.coverage)) ||
      any(min.coverage < 0 | min.coverage > 1) ||
      (length(min.coverage) > 1L && is.null(names(min.coverage))) ||
      (!is.null(names(min.coverage)) &&
       (anyNA(names(min.coverage)) || any(!nzchar(names(min.coverage))) || anyDuplicated(names(min.coverage)))))
    .abort("{.arg min.coverage} must be a fraction in [0, 1], or a uniquely named numeric vector of per-channel fractions.")
  .assert_count(n.cores, "n.cores", min = 1)
  .assert_flag(enhanced.features, "enhanced.features")
  if (!is.null(circular.variables) && !is.character(circular.variables))
    .abort("{.arg circular.variables} must be a character vector of variable names, or {.code NULL}.")
  n_rows_out <- 0L                 # feature rows actually delivered, across the cohort
  dropped_items <- character(0)    # per-deployment NA-row losses (reported once, at the end)
  retained_missing_items <- character(0)

  # Define valid metrics
  valid_linear_metrics <- c("mean", "median", "sd", "range", "min", "max", "iqr",
                            "mad", "sum", "rate", "energy", "skewness", "kurtosis", "entropy")
  valid_circular_metrics <- c("mean", "median", "sd", "range", "iqr", "mrl", "rate")

  # Enhanced features list
  valid_enhanced_metrics <- c("net_heading_change", "cumulative_heading_change",
                              "circular_variance_heading", "oscillation_regularity",
                              "movement_predictability", "movement_consistency",
                              "movement_smoothness", "movement_jerk", "posture_stability",
                              "turning_rate_variability", "activity_index",
                              "rolling_autocorrelation", "zero_crossing_rate",
                              "circling_behavior",
                              "depth_change_rate", "depth_change_consistency",
                              "uturn_flag", "heading_autocorr_avg")

  # Determine parameter grid
  if (is.null(parameter.grid)) {
    if (is.null(variables) || length(variables) == 0) {
      .abort("{.arg variables} is required when {.arg parameter.grid} is not supplied.")
    }
    if (is.null(metrics) || length(metrics) == 0) {
      .abort("{.arg metrics} is required when {.arg parameter.grid} is not supplied.")
    }
    parameter_grid <- expand.grid(variable = variables, metric = metrics, stringsAsFactors = FALSE)
    parameter_grid$window_seconds <- window.size  # Add default window size
  } else {
    # `parameter.grid` wins, but say so: silently discarding the caller's `variables`/`metrics` is how a
    # run quietly computes something other than what was asked for.
    if (!is.null(variables) || !is.null(metrics))
      cli::cli_warn(c("{.arg parameter.grid} was supplied, so {.arg variables} and {.arg metrics} are ignored.",
                      "i" = "Supply either {.arg parameter.grid} OR {.arg variables} + {.arg metrics}, not both."))
    if (!is.data.frame(parameter.grid)) .abort("{.arg parameter.grid} must be a data.frame.")
    if (!all(c("variable", "metric") %in% names(parameter.grid))) {
      .abort("{.arg parameter.grid} must contain {.field variable} and {.field metric} columns.")
    }
    parameter.grid <- as.data.frame(data.table::copy(parameter.grid))
    parameter.grid$metric <- tolower(parameter.grid$metric)

    # Add window_seconds column if not present
    if (!"window_seconds" %in% names(parameter.grid)) {
      parameter.grid$window_seconds <- window.size
    }
    parameter_grid <- parameter.grid
  }

  if (!nrow(parameter_grid) || anyNA(parameter_grid$variable) ||
      any(!nzchar(as.character(parameter_grid$variable))))
    .abort("{.arg parameter.grid} must contain nonempty variable and metric pairs.")
  parameter_grid$variable <- as.character(parameter_grid$variable)
  parameter_grid$metric <- tolower(as.character(parameter_grid$metric))
  if (!is.numeric(parameter_grid$window_seconds))
    .abort("{.arg parameter.grid} window_seconds must be numeric durations in seconds.")
  parameter_grid$window_seconds[is.na(parameter_grid$window_seconds)] <- window.size
  if (any(!is.finite(parameter_grid$window_seconds) | parameter_grid$window_seconds <= 0))
    .abort("{.arg parameter.grid} window_seconds must contain positive finite durations.")
  feature_names <- mapply(.create_feature_name, parameter_grid$variable, parameter_grid$metric,
                          parameter_grid$window_seconds, window.size, USE.NAMES = FALSE)
  if (anyDuplicated(feature_names) || any(feature_names %in% c(id.col, datetime.col, response.col, segment.col)))
    .abort("{.arg parameter.grid} produces duplicate or reserved output column names.")

  # Validate metrics
  all_valid_metrics <- c(valid_linear_metrics, valid_circular_metrics)
  if (enhanced.features) {
    all_valid_metrics <- c(all_valid_metrics, valid_enhanced_metrics)
  }

  invalid_metrics <- parameter_grid$metric[!parameter_grid$metric %in% all_valid_metrics]
  if (length(invalid_metrics) > 0) {
    # distinguish "not a metric" from "a metric you have not enabled" - the second used to be reported
    # as the first, which sends the reader looking for a typo that is not there
    gated <- intersect(unique(invalid_metrics), valid_enhanced_metrics)
    unknown <- setdiff(unique(invalid_metrics), valid_enhanced_metrics)
    if (length(gated))
      .abort(c("Metric{?s} {.val {gated}} require{?s/} {.code enhanced.features = TRUE}.",
               "i" = "They are enhanced features and are not computed unless enabled."))
    .abort(c("Invalid metric{?s}: {.val {unknown}}.",
             "i" = "Available: {.val {sort(unique(all_valid_metrics))}}."))
  }

  # Validation is VARIABLE-AWARE, not just metric-aware. Circular variables are summarised by a
  # 7-metric subset (an angle has no meaningful sum, skewness or entropy on a linear scale), and the
  # pairing used to pass this gate and fail deep inside the per-deployment loop instead - after the
  # cohort had already been read.
  circ_rows <- parameter_grid$variable %in% circular.variables
  if (any(circ_rows)) {
    bad <- unique(parameter_grid$metric[circ_rows & !parameter_grid$metric %in%
                                        c(valid_circular_metrics, valid_enhanced_metrics)])
    if (length(bad))
      .abort(c("Metric{?s} {.val {bad}} cannot be computed on a circular variable.",
               "i" = "Circular variables ({.val {intersect(circular.variables, parameter_grid$variable)}}) support: {.val {valid_circular_metrics}}.",
               "i" = "Adjust {.arg metrics}, or drop the variable from {.arg circular.variables} to treat it linearly."))
  }

  # `downsample.to` is a FREQUENCY in Hz, matching processTagData - the roxygen used to say seconds,
  # so a user following it was wrong by the reciprocal, silently. Validate it like its sibling does.
  .assert_number(downsample.to, "downsample.to", min = 0, null_ok = TRUE)
  if (!is.null(downsample.to) && downsample.to <= 0)
    .abort("{.arg downsample.to} must be greater than zero (Hz).")
  optional_packages <- list(moments = c("skewness", "kurtosis"), entropy = "entropy")
  for (pkg in names(optional_packages)) {
    if (any(parameter_grid$metric %in% optional_packages[[pkg]]) && !requireNamespace(pkg, quietly = TRUE))
      .abort("Package {.pkg {pkg}} is required for the requested metrics.")
  }

  # Check for enhanced features when enhanced.features = FALSE
  if (!enhanced.features && any(parameter_grid$metric %in% valid_enhanced_metrics)) {
    .abort(c("{.arg parameter.grid} requests enhanced metric{?s} but {.code enhanced.features = FALSE}.",
             "i" = "Set {.code enhanced.features = TRUE}, or remove those metrics."))
  }

  # Load required packages for enhanced features
  if (enhanced.features) {
    # `circular` was required here but never used: the five call sites wrapped a heading in
    # circular::circular() and then did the wrap-correction by hand, and the wrapper is provably inert
    # for every operation applied to it (diff, data.table::shift, zoo::rollapply and the values
    # themselves are bit-identical with and without it - checked directly and by comparing all five
    # affected helpers). It was a hard gate blocking the whole enhanced-feature set for nothing.
    required_packages <- c("zoo")
    for (pkg in required_packages) {
      if (!requireNamespace(pkg, quietly = TRUE)) {
        .abort(c("Package {.pkg {pkg}} is required for {.code enhanced.features = TRUE} but is not installed.",
                 "i" = "Install it, or use {.code enhanced.features = FALSE}."))
      }
    }
  }

  # Validate parallel computing requirements
  if (n.cores > 1) {
    required_parallel_packages <- c("foreach", "doSNOW", "parallel")
    for (pkg in required_parallel_packages) {
      if (!requireNamespace(pkg, quietly = TRUE)) {
        .abort(c("Package {.pkg {pkg}} is required for {.code n.cores > 1} but is not installed.",
                 "i" = "Install it, or run serially with {.code n.cores = 1}."))
      }
    }
    available_cores <- parallel::detectCores()
    if (!is.na(available_cores) && available_cores < n.cores) {
      .abort("{.arg n.cores} ({n.cores}) exceeds the number of available cores.")
    }
  }

  ##############################################################################
  # Process data ###############################################################
  ##############################################################################

  # Calculate no. individuals
  n_animals <- length(data)

  .log_header(lvl, "extractFeatures", "Building a rolling-window feature matrix",
              bullets = c(sprintf("Input: %d dataset%s", n_animals, if (n_animals != 1) "s" else ""),
                          sprintf("Features: %d (%d variable%s x %d metric%s)",
                                  nrow(parameter_grid), length(unique(parameter_grid$variable)),
                                  if (length(unique(parameter_grid$variable)) != 1) "s" else "",
                                  length(unique(parameter_grid$metric)),
                                  if (length(unique(parameter_grid$metric)) != 1) "s" else ""),
                          if (!is.null(output.dir)) sprintf("Output: %s", output.dir)),
              arrow = c(sprintf("Window: %g s%s", window.size,
                                if (aggregate) " (start-anchored)" else " (sliding)"),
                        if (enhanced.features) "Mode: enhanced features enabled",
                        if (n.cores > 1) sprintf("Cores: %d", n.cores)))

  # Initialize parallel backend if needed
  if (n.cores > 1) {
    cl <- parallel::makeCluster(n.cores)
    on.exit(parallel::stopCluster(cl), add = TRUE)
    # Evaluate in the workers' base namespace; serializing .libPaths itself
    # copies its private library-location binding rather than updating theirs.
    parallel::clusterCall(cl, eval, substitute(.libPaths(paths), list(paths = .libPaths())),
                          envir = baseenv())
    doSNOW::registerDoSNOW(cl)
    `%dopar%` <- foreach::`%dopar%`
  }

  # Initialize results
  if (return.data) data_processed <- vector("list", length = n_animals)
  saved <- vector("list", length = n_animals)
  ids <- character(n_animals)

  # Was an ABSOLUTE heading statistic actually requested? A magnetic heading is perfectly valid for the
  # rotation-invariant majority (turning rate, circular variance/sd/mrl, heading change, circling), so
  # the guard below tracks WHAT IS COMPUTED rather than what data happens to be held - a warning that
  # fired on every magnetic-heading deployment regardless of the metric would be one users learn to
  # ignore. Only a circular mean/median of heading reports a direction that the declination rotates.
  # A requested variable that exists in NO deployment is a typo, not a partial-sensor case, and used to
  # surface as a cryptic failure deep in the metric dispatch (`x <- data[[var]]` -> NULL). Catch it up
  # front where the input is already in memory; for file-path input the check happens per deployment
  # below, since peeking would mean reading every file twice.
  if (is.list(data) && !is.data.frame(data) && length(data)) {
    have <- unique(unlist(lapply(data, function(d) if (is.null(d)) character(0) else names(d))))
    nowhere <- setdiff(unique(parameter_grid$variable), have)
    if (length(nowhere))
      .abort(c("Requested variable{?s} not present in any deployment: {.val {nowhere}}.",
               "i" = "Available: {.val {sort(setdiff(have, c(id.col, datetime.col)))}}."))
  }

  .grid_for_guard <- if (exists("parameter_grid", inherits = FALSE)) parameter_grid else NULL
  heading_directional <- if (!is.null(.grid_for_guard))
    unique(.grid_for_guard$metric[.grid_for_guard$variable %in% "heading" &
                                  .grid_for_guard$metric %in% .directionalHeadingMetrics()])
  else character(0)
  magnetic_heading_ids <- character(0)

  # Process each dataset
  for (i in 1:length(data)) {

    # Load data
    if (is_filepaths) {
      file_path <- data[i]
      individual_data <- readRDS(file_path)
      id <- unique(individual_data[[id.col]])[1]
      if (is.null(id)) id <- tools::file_path_sans_ext(basename(file_path))
      if (identical(.headingReference(.getMeta(individual_data)), "magnetic"))
        magnetic_heading_ids <- c(magnetic_heading_ids, as.character(id))
    } else {
      individual_data <- data[[i]]
      id <- unique(individual_data[[id.col]])[1]
      if (is.null(id) || id == "") id <- names(data)[i]
      if (identical(.headingReference(.getMeta(individual_data)), "magnetic"))
        magnetic_heading_ids <- c(magnetic_heading_ids, as.character(id))
    }

    if (return.data) names(data_processed)[i] <- id
    ids[i] <- id

    .log_h2(lvl, sprintf("%s (%d/%d)", id, i, n_animals), min_level = 1L)

    # Feature workers may use reference operations; acquire ownership before dispatching them.
    individual_data <- .workingTable(individual_data, owned = is_filepaths)

    # Store original attributes
    discard_attrs <- c("row.names", "class", ".internal.selfref", "names")
    original_attributes <- attributes(individual_data)
    original_attributes <- original_attributes[!names(original_attributes) %in% discard_attrs]

    if (!nrow(individual_data)) .abort("No sensor rows for ID {.val {id}}.")
    if (!datetime.col %in% names(individual_data))
      .abort("Missing timestamp column {.val {datetime.col}} for ID {.val {id}}.")
    .validateInstants(individual_data[[datetime.col]], "Feature timestamps")
    times <- as.numeric(individual_data[[datetime.col]])
    if (any(diff(times) <= 0))
      .abort("Feature timestamps must be strictly increasing for ID {.val {id}}.")
    if (!is.null(segment.col) && !segment.col %in% names(individual_data))
      .abort("Missing {.arg segment.col} column {.val {segment.col}} for ID {.val {id}}.")
    if (!is.null(response.col) && !response.col %in% names(individual_data))
      .abort("Missing {.arg response.col} column {.val {response.col}} for ID {.val {id}}.")

    # Calculate sampling frequency for this individual: prefer the rate recorded in the consolidated
    # metadata (set by processTagData), falling back to the legacy flat attribute, then to timestamps.
    m <- tryCatch(.getMeta(individual_data), error = function(e) NULL)
    cand <- c(m$sensors$sampling_hz_processed, m$sensors$sampling_hz_original,
              original_attributes$processed.sampling.frequency)
    cand <- cand[is.finite(cand) & cand > 0]
    sampling_freq <- if (length(cand)) cand[1] else NA_real_
    if (is.null(sampling_freq) || !is.numeric(sampling_freq) || !is.finite(sampling_freq) || sampling_freq <= 0) {
      if (nrow(individual_data) > 1) {
        time_diffs <- diff(as.numeric(individual_data[[datetime.col]]))
        sampling_freq <- 1 / median(time_diffs)
        warning(paste0("Sampling frequency estimated as ", round(sampling_freq, 2), " Hz for ID '", id, "'."), call. = FALSE)
      } else {
        stop(paste0("Cannot determine sampling frequency for ID '", id, "'."), call. = FALSE)
      }
    }

    # All scales share one set of anchors; no truncation, padding or positional joins.
    window_steps <- max(1L, round(window.size * sampling_freq))
    stride <- if (is.null(step.size)) { if (aggregate) window_steps else 1L } else
      max(1L, round(step.size * sampling_freq))
    layout <- .featureLayout(individual_data, times, sampling_freq, stride, segment.col)
    anchors <- layout$anchors
    calculate <- function(p) .calculateMetricEnhanced(
      data = individual_data, var = parameter_grid$variable[p], metric = parameter_grid$metric[p],
      sampling_freq = sampling_freq, window_seconds = parameter_grid$window_seconds[p],
      aggregate = aggregate, circular_variables = circular.variables, enhanced = enhanced.features,
      layout = layout, min_coverage = min.coverage)
    pb <- .log_progress_start(lvl, nrow(parameter_grid), "Features", min.level = 2L)
    if (n.cores == 1) {
      feature_list <- lapply(seq_len(nrow(parameter_grid)), function(p) {
        value <- calculate(p)
        .log_progress_step(pb)
        value
      })
    } else {
      p <- NULL # foreach iteration binding (NSE)
      opts <- list(progress = function(n) .log_progress_step(pb))
      feature_list <- foreach::foreach(
        p = seq_len(nrow(parameter_grid)), .options.snow = opts,
        .packages = "nautilus"
      ) %dopar% { calculate(p) }
    }
    .log_progress_done(pb)
    names(feature_list) <- feature_names

    if (!is.null(response.col)) {
      bounds <- .featureBounds(layout, window_steps, aggregate)
      feature_list[[response.col]] <- vapply(seq_along(anchors), function(j) {
        if (!bounds$valid[j]) return(NA_integer_)
        .featureResponse(individual_data[[response.col]][seq.int(bounds$start[j], bounds$end[j])],
                         response.aggregation)
      }, integer(1))
    }
    feature_data <- data.table::as.data.table(feature_list)
    feature_data[, (datetime.col) := individual_data[[datetime.col]][anchors]]
    feature_data[, (id.col) := id]
    if (!is.null(segment.col)) feature_data[, (segment.col) := individual_data[[segment.col]][anchors]]
    # Kept internally through downsampling, so bins can never blend separate segments.
    feature_data[, .feature_segment := layout$segment[anchors]]
    data.table::setcolorder(feature_data, c(id.col, datetime.col, segment.col))
    if (!is.null(response.col)) feature_data[, (response.col) := as.factor(get(response.col))]

    # Complete-case output remains the default, but a caller may retain NA rows for diagnostics or a
    # missing-aware learner. Do not conflate a missing stream with zero activity, and do not make a
    # deployment-level exclusion out of a few incomplete windows.
    n_before <- nrow(feature_data)
    if (identical(missing.features, "omit")) {
      feature_data <- stats::na.omit(feature_data)
      n_missing <- n_before - nrow(feature_data)
    } else n_missing <- sum(!stats::complete.cases(feature_data))
    if (n_missing > 0L) {
      action <- if (identical(missing.features, "omit")) "dropped" else "retained"
      .log_subdetail(lvl, sprintf("rows with missing features %s: %s of %s",
                                  action, .formatNumber(n_missing), .formatNumber(n_before)))
      item <- sprintf("%s: %s of %s rows (%.1f%%)", id, .formatNumber(n_missing),
                      .formatNumber(n_before), 100 * n_missing / max(n_before, 1L))
      if (identical(missing.features, "omit")) dropped_items <- c(dropped_items, item)
      else retained_missing_items <- c(retained_missing_items, item)
    }
    if (nrow(feature_data) == 0L)
      .log_skip(lvl, id, "  no complete feature rows (short window or unavailable requested stream)")


    ############################################################################
    # Downsample data ##########################################################
    ############################################################################

    final_data <- feature_data
    binned <- FALSE
    n_bins_missing <- 0L
    if (!is.null(downsample.to) && nrow(feature_data)) {
      output_rate <- sampling_freq / stride
      if (downsample.to >= output_rate) {
        .log_note(lvl, "Requested downsampling rate is at or above the feature rate; binning skipped.")
      } else {
        binned <- TRUE
        origins <- times[layout$starts]
        # Work in explicit seconds (difftime otherwise auto-selects minutes/hours/days).
        elapsed <- as.numeric(feature_data[[datetime.col]]) - origins[feature_data$.feature_segment]
        bins <- floor(elapsed * downsample.to + 1e-7)
        feature_data[, (datetime.col) := .POSIXct(origins[.feature_segment] + bins / downsample.to,
                                                 attr(individual_data[[datetime.col]], "tzone"))]
        circular_cols <- feature_names[parameter_grid$variable %in% circular.variables &
                                         parameter_grid$metric %in% c("mean", "median")]
        final_data <- feature_data[, lapply(names(.SD), function(cc) {
          if (cc %in% circular_cols) .circularMetric(.SD[[cc]], "mean", sampling_freq)
          else .featureFinite(mean(.SD[[cc]], na.rm = TRUE))
        }), by = c(".feature_segment", datetime.col), .SDcols = feature_names]
        data.table::setnames(final_data, tail(names(final_data), length(feature_names)), feature_names)
        if (!is.null(response.col)) {
          processed_response <- feature_data[, list(value = .featureResponse(get(response.col), response.aggregation)),
                                              by = c(".feature_segment", datetime.col)]
          data.table::setnames(processed_response, "value", response.col)
          final_data <- merge(final_data, processed_response, by = c(".feature_segment", datetime.col), sort = FALSE)
          final_data[, (response.col) := as.factor(get(response.col))]
        }
        if (!is.null(segment.col))
          final_data[, (segment.col) := individual_data[[segment.col]][layout$starts[.feature_segment]]]
        final_data[, (id.col) := id]
      }
    }
    final_data[, .feature_segment := NULL]
    # A bin of opposing angular locations has no defined vector mean, even when
    # all individual feature rows were observed. Honour the missing-row policy
    # for values made undefined by binning as well.
    if (binned) {
      n_bins_missing <- sum(!stats::complete.cases(final_data))
      if (identical(missing.features, "omit")) final_data <- stats::na.omit(final_data)
      if (n_bins_missing && !n_missing) {
        item <- sprintf("%s: %s output bins with undefined summaries", id, .formatNumber(n_bins_missing))
        if (identical(missing.features, "omit")) dropped_items <- c(dropped_items, item)
        else retained_missing_items <- c(retained_missing_items, item)
      }
    }

    # reorder columns
    feature_cols <- setdiff(colnames(final_data), c(id.col, datetime.col, response.col))
    data.table::setcolorder(final_data, c(id.col, datetime.col, segment.col, if(!is.null(response.col)) response.col, setdiff(feature_cols, segment.col)))


    ############################################################################
    # Add additional attributes ################################################
    ############################################################################

    # reapply the original attributes to the processed data
    for (attr_name in names(original_attributes)) {
      attr(final_data, attr_name) <- original_attributes[[attr_name]]
    }

    # create new attributes to save relevant variables
    if(!is.null(parameter.grid)){
      attr(final_data, "parameter.grid") <- parameter.grid
    } else {
      attr(final_data, "features.window.size") <- window.size
    }
    attr(final_data, "features.aggregate") <- aggregate
    attr(final_data, "features.response.col") <- response.col
    attr(final_data, "features.response.aggregation") <- response.aggregation

    # Append a provenance record, like every other pipeline step. The source tag's metadata was being
    # copied onto the feature table verbatim, which is misleading on its own: the rows are no longer
    # sensor samples, and nothing recorded that a feature step had happened at all. `processing.date`
    # is dropped - the audit trail carries the timestamp, and two competing conventions for the same
    # fact is how they drift apart.
    fmeta <- .getMeta(.ensureMeta(final_data))
    if (!is.null(fmeta)) {
      fmeta <- .appendProcessing(fmeta, "extractFeatures",
                                 n_features   = nrow(parameter_grid),
                                 variables    = paste(unique(parameter_grid$variable), collapse = ","),
                                 metrics      = paste(unique(parameter_grid$metric), collapse = ","),
                                 window_size  = window.size,
                                 aggregate    = aggregate,
                                 step_size    = stride / sampling_freq,
                                 step_size_requested = step.size %||% NA_real_,
                                 segment_col  = segment.col,
                                 min_coverage = min.coverage,
                                 parameter_grid = parameter_grid,
                                 sampling_hz = sampling_freq,
                                 enhanced     = enhanced.features,
                                 missing_features = missing.features,
                                 rows_missing = n_missing,
                                 bins_missing = n_bins_missing,
                                 downsample_to = downsample.to %||% NA_real_,
                                 rows_out     = nrow(final_data))
      final_data <- .restoreMeta(final_data, fmeta)
    }


    ############################################################################
    # Save processed data ######################################################
    ############################################################################

    # save the processed data as an RDS file (writing is triggered by a non-NULL output.dir)
    saved[i] <- list(.saveOutput(final_data, id, output.dir = output.dir, output.suffix = output.suffix, compress = compress))

    n_rows_out <- n_rows_out + nrow(final_data)
    .log_ok(lvl, id, "  ", .formatNumber(nrow(final_data)), " feature row",
            if (nrow(final_data) != 1) "s", " x ", nrow(parameter_grid), " feature",
            if (nrow(parameter_grid) != 1) "s")

    # store data to list if return.data is TRUE
    if (return.data) {
      data_processed[[i]] <- final_data
    }

    # clear individual data from memory
    rm(individual_data, feature_data, feature_list, final_data)
    # run garbage collection
    gc(verbose = FALSE)

    # newline after each individual's processing
    .log_gap(lvl)

  }

  ##############################################################################
  # Finalization ###############################################################
  ##############################################################################

  # One grouped warning instead of one per deployment: the diagnosis and the remedy do not vary by tag,
  # and R keeps only the first 50 warnings of a call, so a per-deployment warning in a large cohort is
  # both noisy and unreliable.
  .warn_grouped(
    "{length(dropped_items)} deployment{?s} lost feature rows to missing values.",
    items = dropped_items,
    hints = c("Rows are dropped when any requested feature or response is NA; a missing stream can remove every row.",
              "Use {.code missing.features = \"keep\"} to retain NA rows, or request only available features."))

  .warn_grouped(
    "{length(retained_missing_items)} deployment{?s} retained feature rows with missing values.",
    items = retained_missing_items,
    hints = "Check your model's missing-value policy before fitting; no imputation was applied.")

  # Silent unless BOTH conditions hold: a deployment whose heading is magnetic, and a directional
  # statistic of that heading actually among the requested metrics.
  .warnMagneticHeading(magnetic_heading_ids, heading_directional, "Circular heading statistics")


  if (lvl >= 1L) {
    .log_summary(lvl)
    n_ok <- sum(!is.na(ids))
    .log_done(lvl, .formatNumber(n_rows_out), " feature row", if (n_rows_out != 1) "s",
              " from ", n_ok, " of ", n_animals, " dataset", if (n_animals != 1) "s")
    .log_arrow(lvl, nrow(parameter_grid), " feature", if (nrow(parameter_grid) != 1) "s",
               " per row (", length(unique(parameter_grid$variable)), " variable",
               if (length(unique(parameter_grid$variable)) != 1) "s", " x ",
               length(unique(parameter_grid$metric)), " metric",
               if (length(unique(parameter_grid$metric)) != 1) "s", ")")
    if (!is.null(output.dir)) .log_arrow(lvl, "output: ", output.dir)
    .log_runtime(lvl, start.time)
  }

  # return results
  .collectOutput(data_processed, saved, return.data, ids)
}
