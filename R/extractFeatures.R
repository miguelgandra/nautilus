#######################################################################################################
# Function to extract features from a sliding or aggregated window ####################################
#######################################################################################################

#' Extract windowed features from sensor data
#'
#' @description
#' A classifier cannot learn a behaviour from one sample. What distinguishes a burst of feeding from
#' steady swimming is not the acceleration at any instant but its character over a few seconds - how
#' variable it is, how fast it changes, how directed the animal's heading stays.
#'
#' This function turns the sample-by-sample record into that description: it summarises the variables
#' you choose over rolling time windows, producing one column per variable-and-metric pair. The result
#' is the feature matrix a behavioural classifier is trained on, with the labels from [annotateData()]
#' supplying the response.
#'
#' Windows either slide sample by sample, keeping the input's temporal resolution, or tile the record
#' without overlap.
#'
#' @param data A list of tables/data frames (one per individual), a single aggregated data table/data frame,
#' or a character vector of file paths to RDS files containing sensor data.
#' @param variables Which columns to summarise. Every metric in `metrics` is computed for every
#'   variable named here, so the two together define a full grid; use `parameter.grid` instead when you
#'   want particular pairings. Ignored when `parameter.grid` is supplied.
#' @param metrics Which summaries to compute over each window. The available metrics differ between
#'   linear and circular variables, listed below.
#' @param parameter.grid An explicit table of `variable` and `metric` pairs, and optionally a
#'   `window_seconds` per row. Use it in place of `variables` and `metrics` when the full grid would
#'   be wasteful, or when different variables want different window lengths - a tail beat and a dive
#'   are not described on the same timescale. Rows without `window_seconds` use `window.size`.
#' @param enhanced.features Whether to add the composite ecological descriptors listed below (default
#'   `FALSE`; requires the \pkg{zoo} package). They are more interpretable than raw moments but most
#'   apply to one variable only, and one of them can cost every row of a short deployment.
#' @param id.col Which column identifies the animal (default `"ID"`).
#' @param datetime.col Which column holds the timestamps (default `"datetime"`).
#' @param window.size The default window length, in seconds. Choose it from the timescale of the
#'   behaviour you are trying to separate: too short and the summary is dominated by noise, too long
#'   and two different behaviours are averaged into one row.
#' @param aggregate Whether to tile the record into non-overlapping windows (`TRUE`) rather than
#'   sliding one sample at a time. Tiling gives far fewer rows, and rows that are statistically
#'   independent of each other - which matters if you intend to cross-validate.
#' @param downsample.to Target output rate in Hz (e.g. `1` for one feature row per second), or `NULL`
#'   (default) to leave the feature rows at their native rate. It is a frequency, matching
#'   [processTagData()], so the row interval is `1/downsample.to` seconds. Feature columns are
#'   averaged within each output bin.
#' @param response.col (Optional) A character string specifying the column containing response labels.
#' @param circular.variables Character vector specifying variables that should be treated as circular.
#' @param response.aggregation Method to aggregate `response.col`: "majority" or "any".
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
#' @param n.cores Number of processor cores for parallel computation.
#' @param verbose Controls console output: `FALSE`/`0` (quiet - warnings and errors only), `TRUE`/`1`
#'   (`"normal"`: header, one line per deployment, and a summary), or `2`/`"detailed"` (adds a progress
#'   bar over the feature grid and per-deployment diagnostics). Defaults to `"normal"`.
#'
#' @details
#' ## Windows
#'
#' `window.size` is a duration in seconds, converted per deployment to a whole number of samples from
#' that deployment's own sampling rate. The same call therefore gives comparable features across
#' records sampled at different rates - but the number of samples per window, and hence the variance
#' of every metric, differs between them.
#'
#' Sliding windows are centred, so roughly half a window at each end of a record has no complete span
#' and yields `NA`. Those rows are then removed, described below, which means the leading and trailing
#' half-window of every deployment is systematically absent from the output. With `aggregate = TRUE`
#' the record is tiled instead, and each output row is timestamped at the start of its window.
#'
#' ## Which metrics apply to which variables
#'
#' Variables named in `circular.variables` are angles in degrees and are summarised with circular
#' statistics; everything else is treated linearly. The two sets are not interchangeable - the mean of
#' 350 and 10 degrees is 0, not 180 - so requesting a linear-only metric for a circular variable is an
#' error rather than a silent miscalculation:
#' \itemize{
#'   \item **linear**: `mean`, `median`, `sd`, `range`, `min`, `max`, `iqr`, `mad`, `sum`, `rate`,
#'         `energy`, `skewness`, `kurtosis`, `entropy`
#'   \item **circular**: `mean`, `median`, `sd`, `range`, `iqr`, `mrl`, `rate`
#' }
#' A circular `mean` or `median` reports an absolute direction and is therefore affected by whether the
#' heading is referenced to magnetic or geographic north; the remaining circular metrics are built from
#' angle differences and are unaffected. See [processTagData()] for that distinction.
#'
#' ## Output schema
#'
#' One row per retained window and one column per grid row, with the identifier and datetime columns
#' first. A column is named `<variable>_<metric>` - `depth_mean`, `heading_mrl` - except where the
#' metric already carries its variable's name, in which case the repetition is stripped: the enhanced
#' metrics listed below give their exact column names for that reason.
#'
#' A window is appended to the name only where that grid row's `window_seconds` **differs from**
#' `window.size`, so that the same metric at several scales stays distinguishable. A row whose
#' `window_seconds` happens to equal `window.size` is not suffixed, even though the grid named it
#' explicitly - so build expected column names from that rule rather than from the presence of a
#' `window_seconds` column.
#'
#' ## Which rows survive
#'
#' Any row still carrying an `NA` in any feature column is dropped before the table is returned. That
#' keeps the matrix usable by learners which cannot accept missing values, but it is not a neutral
#' operation: the loss is concentrated at the record edges, as above, and at any gap in the input, so
#' it is systematic rather than random. The number of rows lost is reported per deployment and
#' summarised in a warning. A deployment shorter than the widest requested window loses every row.
#'
#' ## Surface substitution
#'
#' Where `depth == 0`, the distribution-shape metrics are replaced with their neutral values -
#' `skewness = 0`, `kurtosis = 3`, `entropy = 0` - rather than left as the undefined results a
#' constant or near-constant signal produces. This is a deliberate assumption, that a stationary tag
#' at the surface is not a distribution worth describing, and not a computation.
#'
#' It reaches only the columns named exactly `depth_<metric>` and `vertical_speed_<metric>`. A
#' distribution-shape metric on any other variable is left undefined, and so is one on depth itself
#' if the grid gave that row a non-default window, since the column is then named with a window
#' suffix. Because undefined values are dropped with their rows, that can cost a surfaced deployment
#' every row.
#'
#' ## Enhanced features
#'
#' With `enhanced.features = TRUE`, which requires the \pkg{zoo} package. These are composite
#' descriptors rather than standard summary statistics, and most apply to one variable only.
#'
#' Two of them, `posture_stability` and `activity_index`, are unusual: the variable you pair them with
#' in the grid is a **sentinel name, not an input**. Name them against a variable called exactly
#' `posture` and `activity` respectively - a column of that name must exist, or the run aborts - but the
#' values in that column are never read. Both descriptors are computed from the orientation channels
#' instead, as noted below.
#'
#' The output column names are given here because several differ from the metric name: where a metric
#' already carries its variable, the redundancy is stripped, so `net_heading_change` on `heading`
#' becomes `heading_net_change` rather than `heading_net_heading_change`.
#'
#' \describe{
#'   \item{`net_heading_change` -> `heading_net_change`}{Absolute angular difference between the
#'     window's leading and lagging halves, in degrees. Heading only.}
#'   \item{`cumulative_heading_change` -> `heading_cumulative_change`}{Sum of absolute wrap-corrected
#'     step changes, in degrees. Heading only.}
#'   \item{`circular_variance_heading` -> `heading_circular_variance`}{`1 - R`, where `R` is the mean
#'     resultant length: 0 for a perfectly held course, 1 for uniformly scattered headings. Heading
#'     only.}
#'   \item{`turning_rate_variability` -> `heading_turning_variability`}{Coefficient of variation of the
#'     absolute turning rate. Heading only.}
#'   \item{`circling_behavior` -> `heading_circling`}{Total absolute heading change divided by the net
#'     change over the window, as `total / (net + 1)`. It is a dimensionless ratio, not an angle, and it
#'     is **large when the net rotation is small** - which is the point: an animal that turns a great
#'     deal while ending up where it started is circling. A straight transit scores near 1. Heading
#'     only, and note that the numerator is not wrap-corrected, so each 360-degree crossing inside a
#'     window adds to it.}
#'   \item{`uturn_flag` -> `heading_uturn`}{1 where the heading reverses by more than 120 degrees within
#'     the window, else 0. Heading only. The cut-off is fixed, not a parameter.}
#'   \item{`heading_autocorr_avg` -> `heading_autocorr_avg`}{Mean autocorrelation over lags 1 to 5 of
#'     the *unwrapped* heading, so the result does not depend on the animal's absolute bearing. Heading
#'     only.}
#'   \item{`oscillation_regularity`}{Coefficient of variation of the interval between peaks within the
#'     window; low values indicate metronomic oscillation. **Requires at least three peaks inside a
#'     single window**: where the window is shorter than about three cycles of the signal the result is
#'     `NA` throughout, and the deployment can lose every row.}
#'   \item{`movement_jerk`}{Windowed RMS of the input's first difference - its jerk, when the input is
#'     an acceleration channel. Distinct from the rotation-invariant `jerk` channel [processTagData()]
#'     produces, which is computed at native rate before any downsampling.}
#'   \item{`movement_smoothness`}{Windowed RMS of the input's *second* difference. Note the sense: a
#'     higher value means a *less* smooth signal.}
#'   \item{`movement_predictability`}{Coefficient of variation of the *rolling mean* series - that is,
#'     of the smoothed signal, not of the raw samples in the window. Note the sense: a higher value
#'     means *more* variable, and so *less* predictable.}
#'   \item{`movement_consistency`}{Coefficient of variation of the *rolling standard deviation* series.
#'     It asks whether the signal's variability is itself steady, which is a different question from
#'     `movement_predictability` above; the two take different inputs.}
#'   \item{`posture_stability` -> `posture_stability`}{`1 / (1 + rolling sd(pitch) + rolling sd(roll))`,
#'     approaching 1 for a steadily held posture. Requires `pitch` and `roll` columns. Pair it with the
#'     sentinel variable `posture`, whose own values are ignored.}
#'   \item{`activity_index` -> `activity_index`}{Rolling mean of the summed absolute rates of change of
#'     `pitch`, `roll` and heading, the last wrap-corrected. Requires all three columns. Pair it with
#'     the sentinel variable `activity`, whose own values are ignored.}
#'   \item{`rolling_autocorrelation`}{Lag-1 autocorrelation within the window.}
#'   \item{`zero_crossing_rate`}{Proportion of consecutive samples that cross the window's own **mean**,
#'     not zero. That distinction matters, because the natural inputs here - VeDBA, ODBA, depth - are
#'     non-negative and would score exactly 0 under a true zero-crossing count.}
#'   \item{`depth_change_rate` -> `depth_change_rate`}{Absolute first difference of depth,
#'     **per sample**. Unlike every other feature here it is not windowed and not divided by the sample
#'     interval, so its magnitude depends on the sampling rate and is not comparable between records
#'     sampled differently. Depth only.}
#'   \item{`depth_change_consistency` -> `depth_change_consistency`}{Coefficient of variation of that
#'     rate over the window. Depth only.}
#' }
#'
#' The `movement_smoothness` and `movement_predictability` names describe the inverse of what they
#' measure; they are kept for compatibility and the sense is stated above rather than silently assumed.
#'
#' @return If `return.data = TRUE`, a named list with one `data.table` per deployment: the identifier
#'   and datetime columns followed by one `<variable>_<metric>` column per grid row, with rows carrying
#'   any `NA` removed (see *Rows* in Details). Each table carries an `extractFeatures` entry in its
#'   processing history recording the grid, window and row count. If `return.data = FALSE`, a character
#'   vector of the written `.rds` file paths instead.
#' @seealso [processTagData()] for the derived channels most of these features summarise,
#'   [detectDives()] and [diveMetrics()] for dive-resolved descriptors.
#' @examples
#' # Minimal single deployment: one numeric sensor column sampled at 1 Hz. Pass a
#' # named list of per-individual tables (a bare data.frame is read as columns).
#' df <- data.frame(
#'   ID = "shark01",
#'   datetime = as.POSIXct("2024-05-30 12:00:00", tz = "UTC") + 0:19,
#'   vedba = abs(sin(seq(0, 4, length.out = 20)))
#' )
#' # 5 s sliding-window mean and SD of VeDBA
#' extractFeatures(list(shark01 = df), variables = "vedba",
#'                 metrics = c("mean", "sd"), window.size = 5)
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
                            circular.variables = c("heading", "roll"),
                            return.data = TRUE,
                            output.dir = NULL,
                            output.suffix = NULL,
                            compress = TRUE,
                            n.cores = 1,
                            verbose = "normal") {

  ##############################################################################
  # Initial checks and setup ###################################################
  ##############################################################################

  start.time <- Sys.time()
  is_filepaths <- is.character(data)
  .assert_nonempty(data, "data")             # loud failure on empty input (e.g. a typo'd list.files() -> character(0))
  .assert_compress(compress)

  # Check optional (Suggests) packages needed only by specific metrics
  optional_packages <- list(
    moments = c("skewness", "kurtosis"),
    entropy = "entropy"
  )

  for (pkg in names(optional_packages)) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      needed_metrics <- optional_packages[[pkg]]
      .abort(c("Package {.pkg {pkg}} is required for metric{?s} {.val {needed_metrics}} but is not installed.",
               "i" = "Install it, or drop those metrics from {.arg metrics}."))
    }
  }

  # Split data by id.col if not already a list or file paths
  if (!is_filepaths && !is.list(data)) {
    if (!id.col %in% names(data)) {
      .abort("{.arg data} must contain the {.val {id.col}} column when not supplied as a list or file paths.")
    }
    data <- split(data, f = data[[id.col]])
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
  .assert_number(window.size, "window.size", min = 0)
  # strictly positive, checked separately: `min = 0` would accept 0, which used to be ACCEPTED and
  # silently returned zero rows. Expressing it as an epsilon bound would print
  # "must be between 2.22e-16 and Inf", which tells the reader nothing.
  if (window.size <= 0)
    .abort(c("{.arg window.size} must be greater than zero (it is a duration in seconds).",
             "x" = "Got {.val {window.size}}."))
  .assert_number(n.cores, "n.cores", min = 1)
  .assert_flag(enhanced.features, "enhanced.features")
  if (!is.null(circular.variables) && !is.character(circular.variables))
    .abort("{.arg circular.variables} must be a character vector of variable names, or {.code NULL}.")
  n_rows_out <- 0L                 # feature rows actually delivered, across the cohort
  dropped_items <- character(0)    # per-deployment NA-row losses (reported once, at the end)
  lowfs_items   <- character(0)    # deployments whose sampling rate could only be estimated

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
    parameter.grid$metric <- tolower(parameter.grid$metric)

    # Add window_seconds column if not present
    if (!"window_seconds" %in% names(parameter.grid)) {
      parameter.grid$window_seconds <- window.size
    }
    parameter_grid <- parameter.grid
  }

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
    if (parallel::detectCores() < n.cores) {
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
                                if (aggregate) " (non-overlapping)" else " (sliding)"),
                        if (enhanced.features) "Mode: enhanced features enabled",
                        if (n.cores > 1) sprintf("Cores: %d", n.cores)))

  # Initialize parallel backend if needed
  if (n.cores > 1) {
    cl <- parallel::makeCluster(n.cores)
    doSNOW::registerDoSNOW(cl)
    on.exit(parallel::stopCluster(cl))
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

    # Convert to data.table
    if (!data.table::is.data.table(individual_data)) {
      individual_data <- data.table::setDT(individual_data)
    }

    # Store original attributes
    discard_attrs <- c("row.names", "class", ".internal.selfref", "names")
    original_attributes <- attributes(individual_data)
    original_attributes <- original_attributes[!names(original_attributes) %in% discard_attrs]

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

    # Calculate window steps (CRITICAL FIX)
    window_steps <- round(window.size * sampling_freq)

    # Initialize feature list
    feature_list <- list()

    # Process features (sequential or parallel)
    if (n.cores == 1) {
      pb <- .log_progress_start(lvl, nrow(parameter_grid), "Features", min.level = 2L)
      for (p in 1:nrow(parameter_grid)) {
        var <- parameter_grid$variable[p]
        metric <- parameter_grid$metric[p]
        window_sec <- parameter_grid$window_seconds[p]

        feature_list[[p]] <- .calculateMetricEnhanced(
          data = individual_data,
          var = var,
          metric = metric,
          sampling_freq = sampling_freq,
          window_seconds = window_sec,
          aggregate = aggregate,
          circular_variables = circular.variables,
          enhanced = enhanced.features
        )
        .log_progress_step(pb)
      }
    } else {
      # Parallel processing
      pb <- .log_progress_start(lvl, nrow(parameter_grid), "Features", min.level = 2L)
      opts <- list(progress = function(n) .log_progress_step(pb))

      feature_list <- foreach::foreach(
        p = 1:nrow(parameter_grid),
        .options.snow = opts,
        # The helpers now live in the package namespace, so loading nautilus in each worker makes them
        # available. The previous `.export` list hand-enumerated 21 names and had to be edited in
        # lockstep with the helper roster - exactly the kind of parallel list that silently goes stale.
        .packages = c("nautilus", "data.table", if (enhanced.features) "zoo")
      ) %dopar% {
        var <- parameter_grid$variable[p]
        metric <- parameter_grid$metric[p]
        window_sec <- parameter_grid$window_seconds[p]

        .calculateMetricEnhanced(
          data = individual_data,
          var = var,
          metric = metric,
          sampling_freq = sampling_freq,
          window_seconds = window_sec,
          aggregate = aggregate,
          circular_variables = circular.variables,
          enhanced = enhanced.features
        )
      }
    }

    # Close progress bar
    .log_progress_done(pb)

    # Name features
    feature_names <- mapply(.create_feature_name,
                            parameter_grid$variable,
                            parameter_grid$metric,
                            parameter_grid$window_seconds,
                            window.size,
                            SIMPLIFY = TRUE)

    names(feature_list) <- feature_names


    ############################################################################
    # Validate feature lengths before combining ################################
    ############################################################################

    expected_length <- if (aggregate) {
      length(seq(1, nrow(individual_data), by = window_steps))
    } else {
      nrow(individual_data)
    }

    # Check and fix feature lengths
    feature_lengths <- sapply(feature_list, length)
    if (length(unique(feature_lengths)) > 1) {
      warning(paste("Features have different lengths for ID:", id,
                    "- Expected:", expected_length,
                    "- Found:", paste(unique(feature_lengths), collapse = ", ")))

      # Standardize all features to expected length
      feature_list <- lapply(feature_list, function(feat) {
        if (length(feat) != expected_length) {
          if (length(feat) > expected_length) {
            return(feat[1:expected_length])
          } else {
            result <- rep(NA, expected_length)
            result[1:length(feat)] <- feat
            return(result)
          }
        }
        return(feat)
      })
    }

    ############################################################################
    # Aggregate response column (if specified) #################################
    ############################################################################

    # if a response column was provided, process it according to the specified aggregation method
    if (!is.null(response.col)) {

      # case 1: aggregate response values within each window (non-overlapping)
      if (aggregate) {

        # calculate window indices for aggregation
        idx <- seq(1, nrow(individual_data), by = window_steps)

        # assign 1 if the majority of values in the window are 1 (i.e., mean > 0.5), otherwise 0
        if (response.aggregation == "majority") {
          feature_list[[response.col]] <- sapply(idx, function(start_idx) {
            end_idx <- min(start_idx + window_steps - 1, nrow(individual_data))
            as.integer(mean(individual_data[[response.col]][start_idx:end_idx], na.rm = TRUE) > 0.5)
          })
          # assign 1 if any value in the window is 1, otherwise 0
        } else if (response.aggregation == "any") {
          feature_list[[response.col]] <- sapply(idx, function(start_idx) {
            end_idx <- min(start_idx + window_steps - 1, nrow(individual_data))
            as.integer(any(individual_data[[response.col]][start_idx:end_idx] == 1, na.rm = TRUE))
          })
        }
        # case 2: use a sliding window across the entire sequence (overlapping)
      } else {
        # apply majority rule in a sliding window: assign 1 if >50% of values are 1
        if (response.aggregation == "majority") {
          feature_list[[response.col]] <- zoo::rollapply(
            individual_data[[response.col]],
            width = window_steps,
            FUN = function(x) as.integer(mean(x, na.rm = TRUE) > 0.5),
            align = "center", fill = NA
          )
          # assign 1 if any value in the window is 1
        } else if (response.aggregation == "any") {
          feature_list[[response.col]] <- zoo::rollapply(
            individual_data[[response.col]],
            width = window_steps,
            FUN = function(x) as.integer(any(x == 1, na.rm = TRUE)),
            align = "center", fill = NA
          )
        }
      }
    }


    ############################################################################
    # Combine features #########################################################
    ############################################################################

    # combine features into a single data.table
    feature_data <- data.table::setDT(feature_list)

    # Proper datetime assignment
    if (!aggregate) {
      feature_data[, (datetime.col) := individual_data[[datetime.col]]]
    } else {
      # ensure we don't exceed data bounds
      idx <- seq(1, nrow(individual_data), by = window_steps)
      # Ensure idx doesn't exceed data length
      idx <- idx[idx <= nrow(individual_data)]

      # Match the length of datetime to features
      if (length(idx) != nrow(feature_data)) {
        idx <- idx[1:nrow(feature_data)]
      }

      feature_data[, (datetime.col) := individual_data[[datetime.col]][idx]]
    }

    # write the identifier under the caller's own `id.col`. It used to be hard-coded to "ID" while every
    # later step looked it up by `id.col`, so any non-default value aborted the run outright - a
    # documented, exported argument that only worked at its default.
    feature_data[, (id.col) := id]

    # move the identifier and datetime.col to the first columns
    data.table::setcolorder(feature_data, c(id.col, datetime.col))

    # convert response col back to factor
    if (!is.null(response.col)) {
      feature_data[, (response.col) := as.factor(get(response.col))]
    }

    # Replace problematic metrics with defaults where appropriate
    replacement_values <- list(skewness = 0, kurtosis = 3, entropy = 0)
    target_prefixes <- c("depth", "vertical_speed")

    # Only do replacement if depth column exists
    if ("depth" %in% names(individual_data)) {
      depth_zero <- !is.na(individual_data$depth) & individual_data$depth == 0
      dt_flags <- data.table::data.table(datetime = individual_data[[datetime.col]], depth_zero = depth_zero)

      feature_data <- merge(feature_data, dt_flags, by = datetime.col, all.x = TRUE)

      for (prefix in target_prefixes) {
        for (suffix in names(replacement_values)) {
          pattern <- paste0("^", prefix, "_", suffix, "$")
          matching_cols <- grep(pattern, names(feature_data), value = TRUE)
          for (col in matching_cols) {
            idx_replace <- which(is.na(feature_data[[col]]) | is.nan(feature_data[[col]]))
            idx_replace <- idx_replace[which(feature_data$depth_zero[idx_replace] == TRUE)]
            feature_data[[col]][idx_replace] <- replacement_values[[suffix]]
          }
        }
      }

      feature_data[, depth_zero := NULL]
    }

    # remove rows with any (remaining) missing values (NA) in any column
    # Rows carrying any NA are dropped. That is deliberate - a feature matrix with holes is unusable for
    # most learners - but it is NOT free: the leading and trailing half-window of every deployment is
    # always NA, so the loss is systematic rather than random, and it used to happen with no count and
    # no explanation. Record it and report it.
    n_before <- nrow(feature_data)
    feature_data <- stats::na.omit(feature_data)
    n_lost <- n_before - nrow(feature_data)
    if (n_lost > 0) {
      .log_subdetail(lvl, sprintf("%s rows dropped (incomplete windows): %s of %s",
                                  "", .formatNumber(n_lost), .formatNumber(n_before)))
      dropped_items <- c(dropped_items,
                         sprintf("%s: %s of %s rows (%.1f%%)", id, .formatNumber(n_lost),
                                 .formatNumber(n_before), 100 * n_lost / max(n_before, 1L)))
    }
    if (nrow(feature_data) == 0L)
      .log_skip(lvl, id, "  no complete windows - the record is shorter than the widest window")


    ############################################################################
    # Downsample data ##########################################################
    ############################################################################

    # if a downsampling rate is specified, aggregate the data to the defined frequency (in Hz)
    if(!is.null(downsample.to)){

      # check if the specified downsampling frequency matches the dataset's sampling frequency
      if (downsample.to == sampling_freq) {
        warning(paste(id, " - dataset sampling already", downsample.to, "Hz, downsampling skipped"), call. = FALSE)
        final_data <- feature_data

        # check if the specified downsampling frequency exceeds the dataset's sampling frequency
      } else if(downsample.to > sampling_freq) {
        warning(paste(id, " - dataset sampling (", sampling_freq, "Hz) lower than the specified downsampling rate, downsampling skipped"), call. = FALSE)
        final_data <- feature_data

        # start downsampling
      } else {

        # select columns to keep
        feature_cols <- setdiff(colnames(feature_data), c(id.col, datetime.col))
        if (!is.null(response.col)) feature_cols <- setdiff(feature_cols, response.col)

        # convert the desired downsample rate to time interval in seconds
        downsample_interval <- 1 / downsample.to

        # round datetime to the nearest downsample interval
        first_time <- feature_data[[datetime.col]][1]
        feature_data[, (datetime.col) := first_time + floor(as.numeric(get(datetime.col) - first_time) / downsample_interval) * downsample_interval]

        # (no output sink here: the one that used to wrap this block had no on.exit, so any error left
        # the CALLER's console redirected to a temp file - and it suppressed nothing, because data.table
        # `:=`/`[` and gc() do not auto-print inside a function and warnings go to stderr.)

        # aggregate metrics using arithmetic mean
        final_data <- feature_data[, lapply(.SD, mean, na.rm=TRUE), by = datetime.col, .SDcols = feature_cols]

        # handle response column aggregation if present
        if (!is.null(response.col)) {
          if (response.aggregation == "majority") {
            processed_response <- feature_data[, .(response = as.integer(mean(as.numeric(as.character(get(response.col))), na.rm = TRUE) > 0.5)),  by = c(datetime.col)]
          } else if (response.aggregation == "any") {
            processed_response <- feature_data[, .(response = as.integer(any(as.numeric(as.character(get(response.col))) == 1, na.rm = TRUE))),  by = c(datetime.col)]
          }
          # rename before merging
          data.table::setnames(processed_response, "response", response.col)
          # merge with downsampled features
          final_data <- merge(final_data, processed_response, by = datetime.col, sort = FALSE)
        }

        # re-add ID column
        final_data[, (id.col) := id]

        # clean up
        gc()
      }

    } else{
      # if no downsampling rate is defined, return the original sensor data
      final_data <- feature_data
    }

    # reorder columns
    feature_cols <- setdiff(colnames(final_data), c(id.col, datetime.col, response.col))
    data.table::setcolorder(final_data, c(id.col, datetime.col, if(!is.null(response.col)) response.col, feature_cols))


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
                                 enhanced     = enhanced.features,
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
    "{length(dropped_items)} deployment{?s} lost feature rows to incomplete windows.",
    items = dropped_items,
    hints = c("Rows are dropped when any requested feature is NA - always the leading and trailing half-window, so the loss is systematic, not random.",
              "Shorten {.arg window.size} to retain more of each record."))

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


################################################################################
# Enhanced metric calculation function #########################################
################################################################################

.calculateMetricEnhanced <- function(data, var, metric, sampling_freq, window_seconds,
                                     aggregate, circular_variables, enhanced = FALSE) {

  window_steps <- round(window_seconds * sampling_freq)
  x <- data[[var]]

  # Enhanced features
  if (enhanced && metric %in% c("net_heading_change", "cumulative_heading_change",
                                "circular_variance_heading", "oscillation_regularity",
                                "movement_predictability", "movement_consistency",
                                "movement_smoothness", "movement_jerk", "posture_stability",
                                "turning_rate_variability", "activity_index",
                                "rolling_autocorrelation", "zero_crossing_rate",
                                "circling_behavior",
                                "depth_change_rate", "depth_change_consistency",
                                "uturn_flag", "heading_autocorr_avg")) {

    return(.calculateEnhancedFeature(data, var, metric, window_steps, aggregate))
  }

  ##############################################################################
  # CIRCULAR VARIABLES #########################################################
  ##############################################################################

  if (var %in% circular_variables) {
    # sliding window for circular
    if (!aggregate) {
      return(zoo::rollapply(
        x,
        width = window_steps,
        FUN = function(w) .circularMetric(w, metric = metric, sampling_freq = sampling_freq),
        align = "center",
        fill = NA
      ))
      # aggregate for circular
    } else {
      n <- length(x)
      starts <- seq(1, n, by = window_steps)
      ends <- pmin(starts + window_steps - 1, n)
      result <- sapply(seq_along(starts), function(i) {
        window_data <- x[starts[i]:ends[i]]
        .circularMetric(window_data, metric = metric, sampling_freq = sampling_freq)
      })
      return(result)
    }

    ##############################################################################
    # LINEAR VARIABLES ###########################################################
    ##############################################################################

  } else {

    if (!aggregate) {
      # Use data.table's frollapply for efficient sliding window calculations
      switch(metric,
             mean = data.table::frollmean(x, window_steps, na.rm = TRUE, align = "center", fill = NA),
             median = data.table::frollapply(x, window_steps, median, na.rm = TRUE, align = "center", fill = NA),
             sd = data.table::frollapply(x, window_steps, sd, na.rm = TRUE, align = "center", fill = NA),
             min = data.table::frollapply(x, window_steps, min, na.rm = TRUE, align = "center", fill = NA),
             max = data.table::frollapply(x, window_steps, max, na.rm = TRUE, align = "center", fill = NA),
             sum = data.table::frollsum(x, window_steps, na.rm = TRUE, align = "center", fill = NA),
             # For more complex metrics, still use the original approach but with faster functions
             range = zoo::rollapply(x, window_steps, function(x) diff(range(x, na.rm = TRUE)), fill = NA, align = "center", partial = FALSE),
             iqr = zoo::rollapply(x, window_steps, IQR, na.rm = TRUE, fill = NA, align = "center", partial = FALSE),
             mad = zoo::rollapply(x, window_steps, mad, na.rm = TRUE, fill = NA, align = "center", partial = FALSE),
             rate = zoo::rollapply(x, width = window_steps, function(x) {
               if (length(x) < 2 || all(is.na(x))) return(NA)
               mean(abs(diff(x)), na.rm = TRUE) * sampling_freq
             }, fill = NA, align = "center", partial = FALSE),
             energy = data.table::frollapply(x^2, window_steps, sum, na.rm = TRUE, align = "center", fill = NA),
             skewness = zoo::rollapply(x, window_steps, moments::skewness, na.rm = TRUE, fill = NA, align = "center", partial = FALSE),
             kurtosis = zoo::rollapply(x, window_steps, moments::kurtosis, na.rm = TRUE, fill = NA, align = "center", partial = FALSE),
             entropy = zoo::rollapply(x, window_steps, function(x) {
               x <- x[!is.na(x)]
               if(length(unique(x)) <= 1) return(NA)
               tryCatch({
                 hist_data <- hist(x, breaks = "Sturges", plot = FALSE)
                 p <- hist_data$density / sum(hist_data$density)
                 entropy::entropy(p)
               }, error = function(e) NA)
             }, fill = NA, align = "center", partial = FALSE),
             # Default case for unknown metrics
             stop(paste("Unknown metric:", metric))
      )

      # Aggregate data into distinct windows - use vectorized operations where possible
    } else {
      n <- length(x)
      starts <- seq(1, n, by = window_steps)
      ends <- pmin(starts + window_steps - 1, n)

      # Use vectorized operations for simple metrics
      switch(metric,
             mean = vapply(seq_along(starts), function(i) mean(x[starts[i]:ends[i]], na.rm = TRUE), numeric(1)),
             median = vapply(seq_along(starts), function(i) median(x[starts[i]:ends[i]], na.rm = TRUE), numeric(1)),
             sd = vapply(seq_along(starts), function(i) sd(x[starts[i]:ends[i]], na.rm = TRUE), numeric(1)),
             min = vapply(seq_along(starts), function(i) min(x[starts[i]:ends[i]], na.rm = TRUE), numeric(1)),
             max = vapply(seq_along(starts), function(i) max(x[starts[i]:ends[i]], na.rm = TRUE), numeric(1)),
             sum = vapply(seq_along(starts), function(i) sum(x[starts[i]:ends[i]], na.rm = TRUE), numeric(1)),
             # For complex metrics, keep original approach
             range = vapply(seq_along(starts), function(i) diff(range(x[starts[i]:ends[i]], na.rm = TRUE)), numeric(1)),
             iqr = vapply(seq_along(starts), function(i) IQR(x[starts[i]:ends[i]], na.rm = TRUE), numeric(1)),
             mad = vapply(seq_along(starts), function(i) mad(x[starts[i]:ends[i]], na.rm = TRUE), numeric(1)),
             rate = vapply(seq_along(starts), function(i) {
               segment <- x[starts[i]:ends[i]]
               if (length(segment) < 2 || all(is.na(segment))) return(NA_real_)
               mean(abs(diff(segment)), na.rm = TRUE) * sampling_freq
             }, numeric(1)),
             energy = vapply(seq_along(starts), function(i) sum(x[starts[i]:ends[i]]^2, na.rm = TRUE), numeric(1)),
             skewness = vapply(seq_along(starts), function(i) moments::skewness(x[starts[i]:ends[i]], na.rm = TRUE), numeric(1)),
             kurtosis = vapply(seq_along(starts), function(i) moments::kurtosis(x[starts[i]:ends[i]], na.rm = TRUE), numeric(1)),
             entropy = vapply(seq_along(starts), function(i) {
               segment <- x[starts[i]:ends[i]]
               segment <- segment[!is.na(segment)]
               if(length(unique(segment)) <= 1) return(NA)
               tryCatch({
                 hist_data <- hist(segment, breaks = "Sturges", plot = FALSE)
                 p <- hist_data$density / sum(hist_data$density)
                 entropy::entropy(p)
               }, error = function(e) NA)
             }, numeric(1)),
             # Default case for unknown metrics
             stop(paste("Unknown metric:", metric))
      )
    }
  }
}


################################################################################
# Define helper function for circular metrics ##################################
################################################################################

.circularMetric <- function(x_window, metric, sampling_freq) {

  # remove NAs and ensure sufficient data
  x_window <- x_window[!is.na(x_window)]
  if (length(x_window) == 0) return(NA)
  if (metric %in% c("sd", "range", "iqr") && length(x_window) < 2) return(NA)

  # convert to radians
  radians <- x_window * pi / 180

  # small helper function for angular difference
  .angular_diff <- function(a, b) {
    diff <- abs(a - b) %% 360
    pmin(diff, 360 - diff)
  }

  switch(metric,
         mean = {
           C <- mean(cos(radians))
           S <- mean(sin(radians))
           atan2(S, C) * 180 / pi
         },
         median = {
           # The circular median is the observed angle MINIMISING the sum of angular distances to all
           # others. The previous expression summed (pi - distance) and minimised that, i.e. it
           # MAXIMISED total distance and returned a point roughly antipodal to the data: for headings
           # clustered at 90 degrees it answered 270. Use the angular distance directly.
           sorted <- sort(x_window)
           tot <- vapply(sorted, function(y) sum(.angular_diff(x_window, y), na.rm = TRUE), numeric(1))
           sorted[which.min(tot)]
         },
         sd = {
           C <- mean(cos(radians))
           S <- mean(sin(radians))
           R <- sqrt(C^2 + S^2)
           sqrt(-2 * log(R)) * 180 / pi
         },
         range = {
           if (length(x_window) < 2) return(NA)
           sorted <- sort(x_window)
           gaps <- c(diff(sorted), 360 - (sorted[length(sorted)] - sorted[1]))
           360 - max(gaps)
         },
         iqr = {
           if (length(x_window) < 2) return(NA)
           q <- quantile(x_window, probs = c(0.25, 0.75))
           diff <- (q[2] - q[1]) %% 360
           min(diff, 360 - diff)
         },
         mrl = {
           C <- mean(cos(radians))
           S <- mean(sin(radians))
           sqrt(C^2 + S^2)
         },
         rate = {
           diffs <- .angular_diff(x_window[-1], x_window[-length(x_window)])
           mean(diffs, na.rm = TRUE) * sampling_freq
         }
  )
}


################################################################################
# Enhanced feature calculation function (COMPLETE VERSION) ####################
################################################################################

.calculateEnhancedFeature <- function(data, var, metric, window_steps, aggregate) {

  n_rows <- nrow(data)

  result <- switch(metric,
                   # Heading-specific features
                   "net_heading_change" = {
                     if (var != "heading") stop("net_heading_change can only be applied to heading variable")
                     .net_heading_change(data$heading, window = window_steps)
                   },
                   "cumulative_heading_change" = {
                     if (var != "heading") stop("cumulative_heading_change can only be applied to heading variable")
                     .cumulative_heading_change(data$heading, window = window_steps)
                   },
                   "circular_variance_heading" = {
                     if (var != "heading") stop("circular_variance_heading can only be applied to heading variable")
                     .circular_variance_heading(data$heading, window = window_steps)
                   },
                   "uturn_flag" = {
                     if (var != "heading") stop("uturn_flag can only be applied to heading variable")
                     .uturn_flag(data$heading, window = window_steps)
                   },
                   "heading_autocorr_avg" = {
                     if (var != "heading") stop("heading_autocorr_avg can only be applied to heading variable")
                     .heading_autocorr_avg(data$heading, window = window_steps)
                   },
                   "turning_rate_variability" = {
                     if (var != "heading") stop("turning_rate_variability can only be applied to heading variable")
                     .turning_rate_variability(data$heading, window = window_steps)
                   },
                   "circling_behavior" = {
                     if (var != "heading") stop("circling_behavior can only be applied to heading variable")
                     .circling_behavior(data$heading, window = window_steps)
                   },

                   # Composite features requiring specific variable names
                   "posture_stability" = {
                     if (var != "posture") stop("posture_stability should use variable = 'posture'")
                     .posture_stability_from_sd(data, window = window_steps)
                   },
                   "activity_index" = {
                     if (var != "activity") stop("activity_index should use variable = 'activity'")
                     .activity_index(data, window = window_steps)
                   },

                   # Movement features that can apply to any variable
                   "oscillation_regularity" = {
                     .oscillation_regularity(data[[var]], window = window_steps)
                   },
                   "movement_jerk" = {
                     .movement_jerk(data[[var]], window = window_steps)
                   },
                   "movement_smoothness" = {
                     .movement_smoothness(data[[var]], window = window_steps)
                   },
                   "rolling_autocorrelation" = {
                     .rolling_autocorrelation(data[[var]], window = window_steps)
                   },
                   "zero_crossing_rate" = {
                     .zero_crossing_rate(data[[var]], window = window_steps)
                   },

                   # Movement predictability and consistency (calculate from raw data)
                   "movement_predictability" = {
                     # Calculate rolling mean and sd from raw variable data
                     signal <- data[[var]]
                     rolling_mean <- zoo::rollapply(signal, width = window_steps, FUN = mean,
                                                    na.rm = TRUE, fill = NA, align = "center")
                     rolling_sd <- zoo::rollapply(signal, width = window_steps, FUN = sd,
                                                  na.rm = TRUE, fill = NA, align = "center")
                     .movement_predictability(rolling_mean, rolling_sd, window = window_steps)
                   },
                   "movement_consistency" = {
                     # Calculate rolling sd from raw variable data
                     signal <- data[[var]]
                     rolling_sd <- zoo::rollapply(signal, width = window_steps, FUN = sd,
                                                  na.rm = TRUE, fill = NA, align = "center")
                     .movement_consistency(rolling_sd, window = window_steps)
                   },

                   # Depth-specific features
                   "depth_change_rate" = {
                     if (var != "depth") stop("depth_change_rate can only be applied to depth variable")
                     depth_metrics <- .depth_change_metrics(data$depth, window = window_steps)
                     depth_metrics$rate
                   },
                   "depth_change_consistency" = {
                     if (var != "depth") stop("depth_change_consistency can only be applied to depth variable")
                     depth_metrics <- .depth_change_metrics(data$depth, window = window_steps)
                     depth_metrics$consistency
                   },

                   # Default case
                   {
                     stop(paste("Unknown enhanced metric:", metric))
                   }
  )

  # Ensure result has correct length
  return(.ensure_length(result, n_rows))
}
