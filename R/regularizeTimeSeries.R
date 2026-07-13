#######################################################################################################
# Function to check and correct irregular time series in tag data #####################################
#######################################################################################################

#' Regularize irregular high-frequency biologging time series
#'
#' @description
#' `regularizeTimeSeries` processes high-frequency time series data from biologging sensors to
#' address irregularities such as time gaps and jitter. The function performs several key steps:
#' \enumerate{
#'   \item Detects and quantifies time gaps and temporal jitter within the series.
#'   \item Regularizes the time series to a consistent sampling interval.
#'   \item Assigns original records to the nearest regular timestamp within a defined threshold.
#'   \item Applies gap-filling strategies (e.g., linear, spline, LOCF interpolation) for
#'         short gaps, while leaving larger gaps as `NA`.
#' }
#'
#' @param data Input data, which can be either:
#'   \itemize{
#'     \item A list of data frames/tables (each containing sensor data for one individual)
#'     \item A single data frame/table
#'     \item Character vector of paths to RDS files containing sensor data
#'   }
#' @param id.col Character. Name of the column containing unique identifier for each tag/animal.
#' Used when input is a single data.frame that needs splitting. Default "ID".
#' @param datetime.col Character. Name of the datetime column. Must contain POSIXct values.
#'   Default "datetime".
#' @param time.threshold Numeric. Maximum allowed deviation (in seconds) from regular
#'   intervals. If `NULL` (default), automatically calculates as half of the nominal
#'   sampling interval. Records beyond this threshold won't be assigned to regular times.
#' @param gap.threshold Numeric. Maximum gap duration (in seconds) for interpolation.
#'   Gaps \eqn{\le} this value will be interpolated; larger gaps remain NA. Set to 0 to disable
#'   interpolation. Default is 5 seconds.
#' @param interpolation.method Character. Interpolation method for small gaps. One of:
#'   \itemize{
#'     \item "linear" (default) - Linear interpolation via `zoo::na.approx`
#'     \item "spline" - Spline interpolation via `zoo::na.spline`
#'     \item "locf" - Last observation carried forward via `zoo::na.locf`
#'   }
#' @param plot Logical. If `TRUE`, draw the diagnostic report (see `plot.file`) to the active
#'   graphics device. Default `FALSE`.
#' @param plot.file Character. Path to a single multi-page PDF for the diagnostic report. The report
#'   is a two-level triage: a **global summary page** (a worst-first table of all deployments with
#'   their status and key regularization metrics) followed by **detailed pages only for deployments
#'   that exceed the review thresholds** (regularization impact, gap diagnostics, an annotated
#'   coverage strip, and a zoom on the most severe event). Healthy deployments get no detailed page.
#'   The parent directory must already exist; must end in `.pdf`. If `NULL` (default), no file is
#'   written. Independent of `plot`. Regardless of plotting, the per-deployment status and key
#'   coverage statistics are always recorded in the processing audit trail.
#' @param review.thresholds Named list overriding the thresholds that classify a deployment as
#'   `"review"` or `"critical"` (and thus earns a detailed page). Recognised fields:
#'   `gap_pct_review`/`gap_pct_critical` (default 1 / 5), `interp_pct_review`/`interp_pct_critical`
#'   (5 / 20), `rows_added_pct_review`/`rows_added_pct_critical` (10 / 100), and the optional absolute
#'   `large_gap_seconds` (default `NULL`, off). `NULL` (default) uses all defaults.
#' @param force.plots Logical. If `TRUE`, generate a detailed page for every deployment, not only
#'   the flagged ones (restores the old one-page-per-deployment behaviour). Default `FALSE`.
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
#' @param verbose Verbosity level: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal", or
#'   `2`/"detailed" (default; adds low-level per-step diagnostics). Defaults to `"detailed"`.
#'
#' @return If `return.data = TRUE`, a named list of regularized `data.table`s (one element
#'   per individual), regardless of whether the input was a single dataset, a list, or a
#'   vector of file paths; if `return.data = FALSE`, a character vector of the written `.rds`
#'   file paths.
#'
#' @details
#' The regularization algorithm proceeds as follows for each individual's dataset:
#' \enumerate{
#'   \item The **median sampling interval** is calculated from the observed `datetime`
#'         differences. This median is used as the nominal interval for regularization.
#'   \item A **complete, regular time sequence** is generated, spanning from the
#'         first to the last timestamp of the original data, at the calculated
#'         nominal interval.
#'   \item Each original record is then **assigned to its nearest timestamp** in
#'         this regular sequence. An assignment is only considered valid if the
#'         time difference between the original record and the nearest regular
#'         timestamp is within the `time.threshold`. Records outside this threshold
#'         (or where no original record is near a regular timestamp) result in `NA`
#'         values in the data columns for those regular time points.
#'   \item For any `NA` values introduced during regularization (or existing initially),
#'         **gap-filling strategies** are applied. Gaps equal to or shorter than
#'         `gap.threshold` are interpolated using the specified `interpolation.method`.
#'         Longer gaps remain as `NA` to avoid spurious data generation.
#' }
#'
#' @note For large datasets, consider processing in batches with an `output.dir` and
#' `return.data = FALSE` to avoid memory overload.
#'
#' @seealso
#' \link{importTagData}
#' \link{filterDeploymentData}
#' \code{\link[zoo]{na.approx}} for interpolation methods
#' \code{\link[data.table]{as.data.table}} for efficient data handling
#'
#' @importFrom data.table as.data.table setorder setnames setcolorder
#' @importFrom zoo na.approx na.spline na.locf
#' @importFrom stats median mad
#' @examples
#' # Snap an irregular, jittered series onto a uniform grid, filling short gaps:
#' d <- data.frame(ID = "shark01",
#'                 datetime = as.POSIXct("2020-01-01 00:00:00", tz = "UTC") +
#'                            c(0, 0.9, 2.1, 3.0, 4.2, 7.0, 8.1, 9.0),
#'                 depth = c(1.0, 1.2, 1.5, 1.8, 2.0, 3.1, 3.4, 3.6))
#' reg <- regularizeTimeSeries(d, gap.threshold = 2, verbose = FALSE)
#' reg[["shark01"]]
#' @export

regularizeTimeSeries <- function(data,
                                 id.col = "ID",
                                 datetime.col = "datetime",
                                 time.threshold = NULL,
                                 gap.threshold = 5,
                                 interpolation.method = "linear",
                                 plot = FALSE,
                                 plot.file = NULL,
                                 review.thresholds = NULL,
                                 force.plots = FALSE,
                                 return.data = TRUE,
                                 output.dir = NULL,
                                 output.suffix = NULL,
                                 compress = TRUE,
                                 verbose = "detailed") {


  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

  # measure running time
  start.time <- Sys.time()

  # verbosity level (0 quiet / 1 normal / 2 detailed)
  lvl <- .verbosity(verbose)

  # scalar argument validation
  .assert_flag(return.data, "return.data")
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  interpolation.method <- match.arg(interpolation.method, choices = c("linear", "spline", "locf"))
  .assert_number(gap.threshold, "gap.threshold", min = 0)
  if (!is.null(time.threshold)) .assert_number(time.threshold, "time.threshold", min = 0)
  .assert_flag(plot, "plot")
  .assert_flag(force.plots, "force.plots")
  .assert_writable_file(plot.file, "plot.file", ext = "pdf")     # fail-fast: parent dir must exist
  .assert_dir(output.dir, "output.dir")                         # fail-fast: must exist
  .assert_string(output.suffix, "output.suffix", null_ok = TRUE)
  .assert_compress(compress)
  make_plots <- plot || !is.null(plot.file)
  review_thr  <- .regularizationThresholds(review.thresholds)    # merged classification thresholds
  run_metrics <- list()                                          # one triage row per deployment
  run_payloads <- list()                                         # small plotting payloads (flagged only)
  .assert_output(return.data, output.dir)

  # resolve input: a character vector of RDS paths, or an in-memory list / single data.frame
  is_filepaths <- is.character(data)
  if (is_filepaths) {
    missing_files <- data[!file.exists(data)]
    if (length(missing_files) > 0) .abort(c("These input files were not found:", stats::setNames(missing_files, rep("*", length(missing_files)))))
  } else if (!is.list(data) || inherits(data, "data.frame")) {
    .assert_columns(data, id.col, "data")
    data <- split(data, data[[id.col]])
  }

  # validate each in-memory dataset up front
  if (!is_filepaths) {
    for (nm in names(data)) {
      .assert_columns(data[[nm]], datetime.col, sprintf("data[['%s']]", nm))
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

  ##############################################################################
  # Process data for each dataset ##############################################
  ##############################################################################

  # initialize results list if returning data
  n_animals <- length(data)
  if (return.data) results <- vector("list", length = n_animals)
  saved <- vector("list", length = n_animals)   # per-item written paths (NULL when nothing written)

  # header
  hdr_bullets <- sprintf("Input: %d tag%s", n_animals, if (n_animals != 1) "s" else "")
  if (!is.null(output.dir)) hdr_bullets <- c(hdr_bullets, paste0("Output: ", output.dir))
  .log_header(lvl, "regularizeTimeSeries", "Regularizing to a uniform time grid",
              bullets = hdr_bullets, arrow = paste0("Method: ", interpolation.method, " interpolation"))
  n_done <- 0L

  # graphics setup (active device for `plot`, single multi-page PDF for `plot.file`)
  caller_dev <- grDevices::dev.cur()
  if (plot && caller_dev == 1L) { grDevices::dev.new(); caller_dev <- grDevices::dev.cur() }
  if (plot) oldpar <- graphics::par(no.readonly = TRUE)
  file_dev <- NULL
  if (!is.null(plot.file)) {
    grDevices::pdf(plot.file, width = 10, height = 7)
    file_dev <- grDevices::dev.cur()
    on.exit(grDevices::dev.off(file_dev), add = TRUE)
  }
  if (plot) on.exit({ if (caller_dev %in% grDevices::dev.list()) { grDevices::dev.set(caller_dev); graphics::par(oldpar) } }, add = TRUE)

  # columns each per-individual dataset must contain (checked for file-path input)
  required_cols <- c(id.col, datetime.col)

  # iterate over each animal
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
      if (!inherits(individual_data[[datetime.col]], "POSIXct")) .abort("The datetime column in {.file {basename(file_path)}} must be of class {.cls POSIXct}.")
      if (is.null(attr(individual_data, "nautilus.version"))) {
        message(paste0("Warning: File '", basename(file_path), "' was likely not processed via importTagData(). It is strongly recommended to run it through importTagData() to ensure proper formatting."))
      }

      # add ID if not present
      id <- unique(individual_data[[id.col]])[1]


    } else {
      # data is already in memory (list of data frames/tables)
      id <- names(data)[i]
      individual_data <- data[[i]]
    }

    # per-individual sub-header (level-2 only; groups this individual's detail lines)
    .log_h2(lvl, sprintf("%s (%d/%d)", id, i, n_animals))

    # skip NULL or empty elements in the list
    if (is.null(individual_data) || nrow(individual_data) == 0) {
      .log_skip(lvl, id, "  empty dataset ", cli::symbol$bullet, " skipped")
      run_metrics[[length(run_metrics) + 1L]] <- .regularizationStub(id, "empty")
      .log_gap(lvl)
      next
    }

    # convert to data.table if not already
    if (!data.table::is.data.table(individual_data)) individual_data <- data.table::as.data.table(individual_data)

    # ensure the consolidated nautilus metadata is present (migrating legacy attrs)
    individual_data <- .ensureMeta(individual_data)

    # ensure data is ordered by datetime
    data.table::setorderv(individual_data, cols = datetime.col)

    # store original attributes, excluding internal ones
    discard_attrs <- c("row.names", "class", ".internal.selfref", "names")
    original_attributes <- attributes(individual_data)
    original_attributes <- original_attributes[!names(original_attributes) %in% discard_attrs]


    ############################################################################
    # Identify temporal irregularities #########################################
    ############################################################################

    # calculate time differences (force seconds: as.numeric(difftime) otherwise
    # returns the auto-chosen difftime units, which are not guaranteed to be seconds)
    time_diffs <- round(as.numeric(diff(individual_data[[datetime.col]]), units = "secs"), 6)

    # estimate nominal sampling rate
    nominal_interval <- stats::median(time_diffs, na.rm = TRUE)

    # a regular grid cannot be built without a valid positive interval (e.g. a single
    # row, or all-identical timestamps); leave such datasets unchanged
    if (is.na(nominal_interval) || nominal_interval <= 0) {
      .log_skip(lvl, id, "  too few distinct timestamps ", cli::symbol$bullet, " left unchanged")
      run_metrics[[length(run_metrics) + 1L]] <- .regularizationStub(id, "unchanged", nrow(individual_data))
      if (return.data) results[[i]] <- individual_data
      saved[i] <- list(.saveOutput(individual_data, id, output.dir = output.dir,
                                   output.suffix = output.suffix, compress = compress))
      .log_gap(lvl)
      next
    }

    sampling_freq <- round(1 / nominal_interval, 1)

    # sampling line (level-2 detail)
    .log_detail(lvl, "sampling: ", sampling_freq, " Hz (nominal ", round(nominal_interval, 2), "s)")

    # auto-detect the jitter threshold if not supplied (computation only; not printed)
    time_threshold <- if (is.null(time.threshold)) nominal_interval / 2 else time.threshold

    # check if regularization is needed (any time differences exceed threshold)
    # regularization is needed if any interval deviates from the nominal interval by
    # more than the threshold (gaps as well as jitter / compressed intervals), since
    # downstream spectral analyses assume an exactly-regular sampling grid
    needs_regularization <- any(abs(time_diffs - nominal_interval) > time_threshold, na.rm = TRUE)

    # apply regularization if required
    if (needs_regularization) {

      # number of irregular intervals + the longest gap (for the irregularities line)
      irregular_count <- sum(abs(time_diffs - nominal_interval) > time_threshold, na.rm = TRUE)
      longest_gap <- max(time_diffs, na.rm = TRUE)

      # irregularities line (level-2 detail)
      .log_detail(lvl, "irregularities: ", .formatNumber(irregular_count), " events (max gap: ",
                  .formatDuration(longest_gap), ")")

      # create regular time sequence covering the full range of datetimes
      start_time <- min(individual_data[[datetime.col]])
      end_time <- max(individual_data[[datetime.col]])
      regular_times <- seq(from = start_time, to = end_time, by = nominal_interval)

      # create a data.table with the regular time sequence
      complete_data <- data.table::data.table(regular_time = regular_times)
      data.table::setnames(complete_data, "regular_time", datetime.col)

      # add ID to all rows in the complete dataset
      complete_data[, (id.col) := id]

      # duplicate the datetime column in the original data for later comparison
      individual_data[, obs_datetime := .SD[[1]], .SDcols = datetime.col]

      # define which columns to transfer from original data (excluding datetime and ID)
      cols_to_copy <- setdiff(names(individual_data), c(datetime.col, id.col))

      # set keys for rolling join
      data.table::setkeyv(individual_data, datetime.col)
      data.table::setkeyv(complete_data, datetime.col)

      # perform a rolling join: for each regular time, find the nearest observation
      complete_data[individual_data, (cols_to_copy) := mget(paste0("i.", cols_to_copy)), roll = "nearest"]

      # calculate time difference between the regular time and the matched observation time
      complete_data[, time_diff := abs(as.numeric(get(datetime.col) - obs_datetime))]

      # identify valid matches within the acceptable time difference threshold
      valid_matches <- complete_data[, (time_diff <= time_threshold) | is.na(time_diff)]

      # count how many new time steps were added
      added_steps <- nrow(complete_data) - nrow(individual_data)
      # convert to percentage
      pct_added <- added_steps / nrow(individual_data) * 100

      # regularization line (level-2 detail)
      .log_detail(lvl, "regularized: +", .formatNumber(added_steps), " rows (",
                  sprintf("%.1f%%", pct_added), " increase)")

      # set values outside threshold to NA (excluding datetime and ID columns)
      data_cols <- setdiff(names(complete_data), c(datetime.col, id.col, "obs_datetime", "time_diff"))
      for (col in data_cols) {
        complete_data[!valid_matches, (col) := NA]
      }

      # temporal displacement of the kept (observed) grid points = how far each was pulled from its
      # matched observation (<= time_threshold by construction; a measure of absorbed jitter). Captured
      # before the helper columns are dropped; summarised for the diagnostics, not persisted.
      kept_disp <- complete_data$time_diff[valid_matches]
      kept_disp <- kept_disp[is.finite(kept_disp)]
      disp_median_ms <- if (length(kept_disp)) round(stats::median(kept_disp) * 1000, 1) else 0
      disp_p95_ms    <- if (length(kept_disp)) round(stats::quantile(kept_disp, 0.95, names = FALSE) * 1000, 1) else 0

      # clean up helper columns
      complete_data[, obs_datetime := NULL]
      complete_data[, time_diff := NULL]

    # no regularization required
    } else {
      .log_detail(lvl, "irregularities: none")
      complete_data <- individual_data
      disp_median_ms <- 0; disp_p95_ms <- 0      # grid == original; no displacement
    }


    ############################################################################
    # Apply interpolation for small gaps #######################################
    ############################################################################

    # interpolate only recognized sensor channels that are actually present (so partial sensor sets work)
    sensor_cols <- intersect(.sensorChannels(), names(complete_data))

    # snapshot the coverage state of the regular grid BEFORE interpolation: a grid row is
    # "missing" if any present sensor channel is NA (an out-of-threshold slot or a true gap)
    na_after_join <- if (length(sensor_cols) > 0) {
      Reduce(`|`, lapply(sensor_cols, function(col) is.na(complete_data[[col]])))
    } else rep(FALSE, nrow(complete_data))

    if (gap.threshold > 0 && length(sensor_cols) > 0) {

      # check if any sensor column has NA values
      has_gaps <- any(vapply(sensor_cols, function(col) anyNA(complete_data[[col]]), logical(1)))

      if (has_gaps) {

        # identify rows where any of the present sensor columns have NA
        na_pattern <- Reduce(`|`, lapply(sensor_cols, function(col) is.na(complete_data[[col]])))

        # use rle to find consecutive NA runs
        na_runs <- rle(na_pattern)

        # calculate gap durations in seconds
        gap_durations <- na_runs$lengths[na_runs$values] * nominal_interval

        # classify gaps by interpolation threshold
        small_gaps <- gap_durations <= gap.threshold
        large_gaps <- gap_durations > gap.threshold

        # count gaps
        n_small_gaps <- sum(small_gaps)
        n_large_gaps <- sum(large_gaps)

        # calculate total durations
        total_small_duration <- sum(gap_durations[small_gaps])
        total_large_duration <- sum(gap_durations[large_gaps])

        # interpolate small gaps in the present sensor channels (numeric only)
        numeric_cols <- sensor_cols[vapply(sensor_cols, function(col) is.numeric(complete_data[[col]]), logical(1))]

        for (col in numeric_cols) {
          if (interpolation.method == "linear") {
            complete_data[[col]] <- zoo::na.approx(complete_data[[col]],
                                                   maxgap = floor(gap.threshold / nominal_interval),
                                                   na.rm = FALSE)
          } else if (interpolation.method == "spline") {
            complete_data[[col]] <- zoo::na.spline(complete_data[[col]],
                                                   maxgap = floor(gap.threshold / nominal_interval),
                                                   na.rm = FALSE)
          } else if (interpolation.method == "locf") {
            maxgap_points <- floor(gap.threshold / nominal_interval)
            # first, forward fill
            complete_data[[col]] <- zoo::na.locf(complete_data[[col]], maxgap = maxgap_points,
                                                 na.rm = FALSE, fromLast = FALSE)
            # then, backward fill for any remaining NAs at the start or between filled values
            complete_data[[col]] <- zoo::na.locf(complete_data[[col]], maxgap = maxgap_points,
                                                 na.rm = FALSE, fromLast = TRUE)
          }
        }

        # interpolation line (level-2 detail): gaps filled, and any too-large gaps left as NA
        if (n_small_gaps > 0 || n_large_gaps > 0) {
          parts <- character(0)
          if (n_small_gaps > 0)
            parts <- c(parts, sprintf("filled %s gap%s (%s)", .formatNumber(n_small_gaps),
                                      if (n_small_gaps == 1) "" else "s", .formatDuration(total_small_duration)))
          if (n_large_gaps > 0)
            parts <- c(parts, sprintf("skipped %s large gap%s (%s)", .formatNumber(n_large_gaps),
                                      if (n_large_gaps == 1) "" else "s", .formatDuration(total_large_duration)))
          .log_detail(lvl, "interpolation: ", paste(parts, collapse = ", "))
        }
      }
    }

    ############################################################################
    # Coverage accounting (always computed; powers the log, metadata and panel)
    ############################################################################

    # final NA state, then classify each regular-grid row as observed / interpolated / gap
    na_final <- if (length(sensor_cols) > 0) {
      Reduce(`|`, lapply(sensor_cols, function(col) is.na(complete_data[[col]])))
    } else rep(FALSE, nrow(complete_data))
    n_regular     <- nrow(complete_data)
    n_interp      <- sum(na_after_join & !na_final)
    n_gap         <- sum(na_final)
    n_observed    <- n_regular - n_interp - n_gap
    pct_interp    <- if (n_regular > 0) 100 * n_interp / n_regular else 0
    pct_gap       <- if (n_regular > 0) 100 * n_gap / n_regular else 0
    nominal_hz    <- round(1 / nominal_interval, 3)
    jitter_mad_ms <- round(stats::mad(time_diffs, na.rm = TRUE) * 1000, 1)

    # per-deployment metrics + status (cheap; reuses the vectors above). gap_stats is attached for
    # payload building and stripped before the row is stored in the run-level triage list.
    m <- .regularizationMetrics(id, nrow(individual_data), na_after_join, na_final, nominal_interval,
                                nominal_hz, jitter_mad_ms, disp_median_ms, disp_p95_ms, review_thr)

    # gaps line (level-2 detail): the fraction of the regular grid left as NA (too large to fill)
    .log_detail(lvl, "gaps: ", sprintf("%.1f%%", pct_gap), " (", .formatNumber(n_gap), " rows)")

    ############################################################################
    # Restore attributes #######################################################
    ############################################################################

    # restore the original attributes
    for (attr_name in names(original_attributes)) {
      attr(complete_data, attr_name) <- original_attributes[[attr_name]]
    }

    # add attribute indicating if regularization was performed
    attr(complete_data, "regularization.performed") <- needs_regularization

    # record this step in the metadata audit trail (incl. coverage accounting) and re-class
    meta <- .getMeta(complete_data)
    if (!is.null(meta)) {
      meta <- .appendProcessing(meta, "regularizeTimeSeries",
                                regularization_performed = needs_regularization,
                                gap_threshold = gap.threshold,
                                interpolation_method = interpolation.method,
                                nominal_hz = nominal_hz, jitter_mad_ms = jitter_mad_ms,
                                n_original = nrow(individual_data), n_regular = n_regular,
                                n_interpolated = n_interp, n_gap = n_gap,
                                pct_interpolated = round(pct_interp, 2), pct_gap = round(pct_gap, 2),
                                status = m$status, largest_gap_s = round(m$largest_gap_s, 1))
      complete_data <- .restoreMeta(complete_data, meta)
    }

    # set column order to have ID and datetime first
    desired_order <- c(id.col, datetime.col, setdiff(names(complete_data), c(id.col, datetime.col)))
    data.table::setcolorder(complete_data, desired_order)


    ############################################################################
    # Save regularized data ####################################################
    ############################################################################

    # save the processed data as an RDS file (writes only when output.dir is set)
    saved_to <- .saveOutput(complete_data, id, output.dir = output.dir,
                            output.suffix = output.suffix, compress = compress)
    saved[i] <- list(saved_to)

    # closing line: one status per tag. At the detailed level the details are shown above, so the
    # closing line just reports the save outcome; at the normal level it carries the one-line summary.
    if (lvl >= 2L) {
      .log_ok(lvl, if (!is.null(saved_to)) paste0("saved ", basename(saved_to)) else "regularized")
    } else {
      .log_ok(lvl, id, "  ", nominal_hz, " Hz ", cli::symbol$bullet,
              " interpolated ", round(pct_interp, 1), "% ", cli::symbol$bullet,
              " gaps ", round(pct_gap, 1), "%",
              if (!is.null(saved_to)) paste0(" ", cli::symbol$bullet, " saved ", basename(saved_to)))
    }
    n_done <- n_done + 1L
    .log_gap(lvl)                          # blank line separates this individual's block from the next

    # build the small plotting payload only for deployments that will get a detail page (flagged, all
    # under force.plots, or the sole deployment of a single-tag run), while the full series is still in
    # memory; healthy tags in a multi-tag run keep metrics only.
    if (make_plots && (force.plots || n_animals == 1L || m$status %in% c("review", "critical"))) {
      run_payloads[[length(run_payloads) + 1L]] <-
        .regularizationPayload(m, individual_data, complete_data, datetime.col, sensor_cols,
                               na_after_join, na_final, nominal_interval)
    }
    m$gap_stats <- NULL                              # drop the (potentially large) gap vectors
    run_metrics[[length(run_metrics) + 1L]] <- m

    # store processed sensor data in the results list if needed
    if (return.data) {
      results[[i]] <- complete_data
    }

    # drop references before the next iteration (R reclaims memory automatically;
    # an explicit gc() every iteration would only slow the loop down)
    rm(individual_data, complete_data)
  }


  ##############################################################################
  # Reporting (deferred: built after all deployments are processed) ############
  ##############################################################################

  # two-level triage report. Drawing is deferred so the global summary can be page 1, and so detail
  # pages exist only for flagged deployments (or all, under force.plots). Payloads are small.
  if (make_plots && length(run_metrics) > 0) {
    draw_report <- function() {
      drew <- FALSE
      if (length(run_metrics) > 1L) { .drawRegularizationSummaryPage(run_metrics, interpolation.method); drew <- TRUE }
      if (length(run_payloads) > 0) {
        sev <- .regularizationSeverity(lapply(run_payloads, function(p) p$m))
        for (k in order(-sev)) .drawRegularizationDetail(run_payloads[[k]])
        drew <- TRUE
      }
      if (!drew) .drawRegularizationSummaryPage(run_metrics, interpolation.method)  # always emit a page
    }
    if (!is.null(file_dev)) { grDevices::dev.set(file_dev); draw_report() }
    if (plot) { if (caller_dev > 1L) grDevices::dev.set(caller_dev); draw_report() }
  }

  ##############################################################################
  # Return regularized data ####################################################
  ##############################################################################

  # final summary + console triage table (level >= 1)
  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, n_done, " of ", n_animals, " tag", if (n_animals != 1) "s", " regularized")
    if (!is.null(output.dir)) .log_arrow(lvl, "output: ", output.dir)
    if (!is.null(plot.file)) .log_arrow(lvl, "plots: ", plot.file)
    .log_runtime(lvl, start.time)
    .printRegularizationTriage(lvl, run_metrics)
  }

  # unified output contract: a named list (one element per individual, consistent with
  # importTagData() and filterDeploymentData()) when return.data, else the written paths
  ids <- if (is_filepaths) tools::file_path_sans_ext(basename(data)) else names(data)
  .collectOutput(results, saved, return.data, ids)
}


#######################################################################################################
# Internal: regularization diagnostic panel ###########################################################
#######################################################################################################

# Default review thresholds (dimensionless metrics generalise across sampling rates; the absolute
# `large_gap_seconds` trigger is off by default). User overrides are merged on top.
#' @keywords internal
#' @noRd
.regularizationThresholds <- function(user = NULL) {
  d <- list(gap_pct_review = 1, gap_pct_critical = 5,
            interp_pct_review = 5, interp_pct_critical = 20,
            rows_added_pct_review = 10, rows_added_pct_critical = 100,
            large_gap_seconds = NULL)
  if (!is.null(user)) {
    if (!is.list(user) || is.null(names(user))) .abort("{.arg review.thresholds} must be a named list.")
    unknown <- setdiff(names(user), names(d))
    if (length(unknown)) .abort("Unknown {.arg review.thresholds} field{?s}: {.val {unknown}}.")
    for (nm in names(user)) d[[nm]] <- user[[nm]]
  }
  d
}

# Gap statistics from the final NA mask (single rle pass). Durations in seconds; run start/end
# indices retained for the coverage markers and the targeted zoom.
#' @keywords internal
#' @noRd
.regularizationGapStats <- function(na_final, nominal_interval) {
  r <- rle(na_final)
  ends <- cumsum(r$lengths); starts <- ends - r$lengths + 1L
  g <- r$values
  durs <- r$lengths[g] * nominal_interval
  list(n_gaps = sum(g), durations_s = durs, starts = starts[g], ends = ends[g],
       largest_s = if (length(durs)) max(durs) else 0,
       median_s  = if (length(durs)) stats::median(durs) else 0,
       total_s   = sum(durs))
}

# Classify a deployment as ok / review / critical from its metrics and thresholds.
#' @keywords internal
#' @noRd
.classifyRegularization <- function(m, thr) {
  crit <- m$pct_gap >= thr$gap_pct_critical ||
          m$pct_interp >= thr$interp_pct_critical ||
          m$rows_added_pct >= thr$rows_added_pct_critical ||
          (!is.null(thr$large_gap_seconds) && m$largest_gap_s >= thr$large_gap_seconds)
  if (crit) return("critical")
  rev <- m$pct_gap >= thr$gap_pct_review ||
         m$pct_interp >= thr$interp_pct_review ||
         m$rows_added_pct >= thr$rows_added_pct_review
  if (rev) "review" else "ok"
}

# Per-deployment metrics + status, computed once from vectors already in hand (cheap; the only extra
# pass is the gap rle). `gap_stats` is attached for payload building and stripped before storing.
#' @keywords internal
#' @noRd
.regularizationMetrics <- function(id, n_original, na_after_join, na_final, nominal_interval,
                                   nominal_hz, jitter_mad_ms, disp_median_ms, disp_p95_ms, thr) {
  n_regular <- length(na_final)
  n_interp  <- sum(na_after_join & !na_final)
  n_gap     <- sum(na_final)
  n_obs     <- n_regular - n_interp - n_gap
  gs <- .regularizationGapStats(na_final, nominal_interval)
  m <- list(id = id, n_original = n_original, n_regular = n_regular,
            rows_added = n_regular - n_original,
            rows_added_pct = if (n_original > 0) 100 * (n_regular - n_original) / n_original else 0,
            n_observed = n_obs, n_interp = n_interp, n_gap = n_gap,
            pct_obs    = if (n_regular > 0) 100 * n_obs / n_regular else 0,
            pct_interp = if (n_regular > 0) 100 * n_interp / n_regular else 0,
            pct_gap    = if (n_regular > 0) 100 * n_gap / n_regular else 0,
            n_gaps = gs$n_gaps, largest_gap_s = gs$largest_s,
            median_gap_s = gs$median_s, total_missing_s = gs$total_s,
            disp_median_ms = disp_median_ms, disp_p95_ms = disp_p95_ms,
            nominal_hz = nominal_hz, jitter_mad_ms = jitter_mad_ms, gap_stats = gs)
  m$status <- .classifyRegularization(m, thr)
  m
}

# A skipped/unchanged deployment still appears in the triage table (no detail page).
#' @keywords internal
#' @noRd
.regularizationStub <- function(id, status, n_original = NA_integer_) {
  list(id = id, status = status, n_original = n_original, n_regular = n_original,
       rows_added = 0L, rows_added_pct = if (isTRUE(status == "unchanged")) 0 else NA_real_,
       n_observed = NA_integer_, n_interp = NA_integer_, n_gap = NA_integer_,
       pct_obs = NA_real_, pct_interp = NA_real_, pct_gap = NA_real_,
       n_gaps = NA_integer_, largest_gap_s = NA_real_, median_gap_s = NA_real_,
       total_missing_s = NA_real_, disp_median_ms = NA_real_, disp_p95_ms = NA_real_,
       nominal_hz = NA_real_, jitter_mad_ms = NA_real_)
}

# Numeric severity score for worst-first ordering (status tier, then gap%, interp%, rows-added%).
#' @keywords internal
#' @noRd
.regularizationSeverity <- function(metrics) {
  tier <- c(empty = 0, skipped = 0, unchanged = 1, ok = 2, review = 3, critical = 4)
  vapply(metrics, function(m) {
    t <- tier[[m$status]]; if (length(t) == 0) t <- 0
    g <- if (is.na(m$pct_gap)) 0 else m$pct_gap
    i <- if (is.na(m$pct_interp)) 0 else m$pct_interp
    r <- if (is.na(m$rows_added_pct)) 0 else min(m$rows_added_pct, 9999)
    t * 1e6 + g * 1000 + i * 10 + r / 10000
  }, numeric(1))
}

# Downsampled coverage status (<= nbin bins; worst status per bin so rare gaps stay visible).
#' @keywords internal
#' @noRd
.regularizationCoverageBins <- function(na_after_join, na_final, nbin = 1500L) {
  n <- length(na_final)
  status <- ifelse(na_final, 2L, ifelse(na_after_join, 1L, 0L))
  nbin <- as.integer(min(n, nbin))
  bin  <- findInterval(seq_len(n), seq(1, n, length.out = nbin + 1)[-(nbin + 1)])
  as.integer(tapply(status, bin, max))
}

# Targeted zoom window for a FLAGGED deployment: the largest unresolved gap (shown as a split window
# with the empty span collapsed and annotated), else the largest interpolation run, else NULL.
#' @keywords internal
#' @noRd
.regularizationZoomData <- function(individual_data, complete_data, datetime.col, sensor_cols,
                                    na_after_join, na_final, gap_stats, nominal_interval) {
  num <- sensor_cols[vapply(sensor_cols, function(c) is.numeric(complete_data[[c]]), logical(1))]
  col <- if ("depth" %in% num) "depth" else if (length(num)) num[1] else NA_character_
  if (is.na(col)) return(NULL)
  tt <- complete_data[[datetime.col]]; n <- length(tt); gv <- complete_data[[col]]
  interp <- na_after_join & !na_final
  raw_in <- function(rng) {
    if (is.null(rng)) return(list(t = numeric(0), v = numeric(0)))
    s <- individual_data[[datetime.col]] >= rng[1] & individual_data[[datetime.col]] <= rng[2]
    list(t = individual_data[[datetime.col]][s], v = individual_data[[col]][s])
  }
  # choose the temporally largest event so a trivial gap never wins over a substantial interpolation
  interp_rle <- if (any(interp)) rle(interp) else NULL
  interp_run_s <- if (!is.null(interp_rle)) max(interp_rle$lengths[interp_rle$values]) * nominal_interval else 0
  gap_s <- if (gap_stats$n_gaps > 0) gap_stats$largest_s else 0

  if (gap_s > 0 && gap_s >= interp_run_s) {
    k <- which.max(gap_stats$durations_s); gs <- gap_stats$starts[k]; ge <- gap_stats$ends[k]
    m <- 200L
    li <- seq.int(max(1L, gs - m), max(1L, gs - 1L)); ri <- seq.int(min(n, ge + 1L), min(n, ge + m))
    if (gs <= 1L) li <- integer(0)
    if (ge >= n)  ri <- integer(0)
    return(list(type = "gap", col = col,
                pre_t = tt[li], pre_v = gv[li], post_t = tt[ri], post_v = gv[ri],
                raw_pre  = raw_in(if (length(li)) range(tt[li]) else NULL),
                raw_post = raw_in(if (length(ri)) range(tt[ri]) else NULL),
                gap_s = gap_stats$durations_s[k],
                gap_t0 = tt[max(1L, gs - 1L)], gap_t1 = tt[min(n, ge + 1L)]))
  }
  if (interp_run_s > 0) {
    r <- interp_rle; ends <- cumsum(r$lengths); starts <- ends - r$lengths + 1L
    k <- which(r$values)[which.max(r$lengths[r$values])]
    center <- round((starts[k] + ends[k]) / 2)
    w <- as.integer(min(n %/% 2L, max(50, round(60 / nominal_interval))))
    win <- max(1L, center - w):min(n, center + w)
    rw <- raw_in(range(tt[win]))
    return(list(type = "interp", col = col, gt = tt[win], gv = gv[win], gi = interp[win],
                ot = rw$t, ov = rw$v))
  }
  NULL
}

# Assemble the small plotting payload for a flagged deployment (drawn after the loop).
#' @keywords internal
#' @noRd
.regularizationPayload <- function(m, individual_data, complete_data, datetime.col, sensor_cols,
                                   na_after_join, na_final, nominal_interval) {
  tt <- complete_data[[datetime.col]]; n <- length(tt)
  gs <- m$gap_stats
  topk <- if (gs$n_gaps) order(-gs$durations_s)[seq_len(min(5L, gs$n_gaps))] else integer(0)
  gap_marks <- if (length(topk))
    data.frame(frac = ((gs$starts[topk] + gs$ends[topk]) / 2) / n, dur = gs$durations_s[topk]) else NULL
  list(m = m,
       cov = .regularizationCoverageBins(na_after_join, na_final),
       zoom = .regularizationZoomData(individual_data, complete_data, datetime.col, sensor_cols,
                                      na_after_join, na_final, gs, nominal_interval),
       gap_marks = gap_marks,
       t_start = tt[1], t_mid = tt[round(n / 2)], t_end = tt[n])
}

# Compact duration label for tables / strips ("12.4m", "3.2h", "45s").
#' @keywords internal
#' @noRd
.formatDurationShort <- function(s) {
  if (length(s) == 0 || is.na(s) || s <= 0) return("-")
  if (s >= 86400) sprintf("%.1fd", s / 86400)
  else if (s >= 3600) sprintf("%.1fh", s / 3600)
  else if (s >= 60)   sprintf("%.1fm", s / 60)
  else sprintf("%.0fs", s)
}

# Console triage table (level >= 1): one row per deployment, worst-first, via cli_verbatim so the
# fixed-width columns survive. The status token is colourised; alignment is computed on plain text.
#' @keywords internal
#' @noRd
.printRegularizationTriage <- function(lvl, metrics) {
  if (lvl < 1L || length(metrics) < 2L) return(invisible(NULL))
  metrics <- metrics[order(-.regularizationSeverity(metrics))]
  cli::cli_text("")
  .log_h2(lvl, "REGULARIZATION SUMMARY", min_level = 1L)
  pc <- function(x) if (is.na(x)) "-" else sprintf("%.1f%%", x)
  scol <- list(ok = cli::col_green, review = cli::col_yellow, critical = cli::col_red,
               unchanged = cli::col_grey, empty = cli::col_grey, skipped = cli::col_grey)
  cli::cli_verbatim(sprintf("%-14s %-9s %8s %8s %8s %9s %6s",
                            "tag", "status", "+rows", "interp", "gap", "max gap", "gaps"))
  for (m in metrics) {
    paint <- scol[[m$status]]; if (is.null(paint)) paint <- function(x) x
    cli::cli_verbatim(paste0(
      sprintf("%-14s ", substr(m$id, 1, 14)),
      paint(sprintf("%-9s", substr(m$status, 1, 9))),
      sprintf(" %8s %8s %8s %9s %6s",
              pc(m$rows_added_pct), pc(m$pct_interp), pc(m$pct_gap),
              .formatDurationShort(m$largest_gap_s),
              if (is.na(m$n_gaps)) "-" else as.character(m$n_gaps))))
  }
  invisible(NULL)
}

# PDF page 1: deployment-level overview (run totals + a worst-first table whose rows carry an inline
# observed/interpolated/gap composition bar). Auto-paginates for large runs.
#' @keywords internal
#' @noRd
.drawRegularizationSummaryPage <- function(metrics, interp_method) {
  col_obs <- "#639922"; col_int <- "#EF9F27"; col_gap <- "#E24B4A"
  scol <- c(ok = col_obs, review = col_int, critical = col_gap,
            unchanged = "#888780", empty = "#888780", skipped = "#888780")
  metrics <- metrics[order(-.regularizationSeverity(metrics))]
  n <- length(metrics)
  is_flag <- vapply(metrics, function(m) m$status %in% c("review", "critical"), logical(1))
  n_flag <- sum(is_flag); n_crit <- sum(vapply(metrics, function(m) identical(m$status, "critical"), logical(1)))
  tot_in  <- sum(vapply(metrics, function(m) if (is.na(m$n_original)) 0 else m$n_original, numeric(1)))
  tot_out <- sum(vapply(metrics, function(m) if (is.na(m$n_regular)) 0 else m$n_regular, numeric(1)))
  per_page <- 30L
  chunks <- split(seq_len(n), ceiling(seq_len(n) / per_page))
  cx <- c(tag = 0.0, status = 0.18, rows = 0.42, interp = 0.52, gap = 0.62, maxgap = 0.74, gaps = 0.82)
  for (ci in seq_along(chunks)) {
    idx <- chunks[[ci]]
    graphics::par(mar = c(1, 1.5, 3, 1.5)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
    graphics::text(0, 1.10, "regularizeTimeSeries  -  run summary", adj = c(0, 1), font = 2, cex = 1.3, xpd = NA)
    graphics::text(1, 1.10, sprintf("%s interpolation  -  %d deployment%s%s", interp_method, n,
                   if (n != 1) "s" else "", if (length(chunks) > 1) sprintf("  (page %d/%d)", ci, length(chunks)) else ""),
                   adj = c(1, 1), cex = 0.8, col = "#5F5E5A", xpd = NA)
    graphics::text(0, 1.04, sprintf("%d flagged (%d critical)    rows %s -> %s", n_flag, n_crit,
                   .formatLargeNumber(tot_in), .formatLargeNumber(tot_out)), adj = c(0, 1), cex = 0.85, col = "#5F5E5A", xpd = NA)
    yh <- 0.95
    hd <- function(key, lab, a) graphics::text(cx[key], yh, lab, adj = c(a, 0.5), cex = 0.78, col = "#888780")
    hd("tag", "tag", 0); hd("status", "status", 0); hd("rows", "+rows", 1); hd("interp", "interp", 1)
    hd("gap", "gap", 1); hd("maxgap", "max gap", 1); hd("gaps", "gaps", 1)
    graphics::text(0.92, yh, "composition", adj = c(0.5, 0.5), cex = 0.78, col = "#888780")
    graphics::segments(0, yh - 0.02, 1, yh - 0.02, col = "#CCCCCC", lwd = 0.5)
    # fixed row height so a few deployments cluster at the top instead of stretching down the page
    rh <- min(0.03, (yh - 0.06) / per_page)
    ys <- (yh - 0.05) - (seq_along(idx) - 1) * rh
    for (j in seq_along(idx)) {
      m <- metrics[[idx[j]]]; y <- ys[j]
      graphics::text(cx["tag"], y, substr(m$id, 1, 18), adj = c(0, 0.5), cex = 0.82)
      graphics::text(cx["status"], y, m$status, adj = c(0, 0.5), cex = 0.82, font = 2,
                     col = if (is.na(scol[m$status])) "#888780" else scol[m$status])
      pc <- function(x) if (is.na(x)) "-" else sprintf("%.1f%%", x)
      graphics::text(cx["rows"], y, pc(m$rows_added_pct), adj = c(1, 0.5), cex = 0.82)
      graphics::text(cx["interp"], y, pc(m$pct_interp), adj = c(1, 0.5), cex = 0.82)
      graphics::text(cx["gap"], y, pc(m$pct_gap), adj = c(1, 0.5), cex = 0.82,
                     col = if (!is.na(m$pct_gap) && m$pct_gap >= 5) col_gap else "black")
      graphics::text(cx["maxgap"], y, .formatDurationShort(m$largest_gap_s), adj = c(1, 0.5), cex = 0.82)
      graphics::text(cx["gaps"], y, if (is.na(m$n_gaps)) "-" else as.character(m$n_gaps), adj = c(1, 0.5), cex = 0.82)
      if (!is.na(m$pct_obs)) {
        bx0 <- 0.85; bw <- 0.14; comp <- c(m$pct_obs, m$pct_interp, m$pct_gap) / 100
        xs <- bx0 + bw * cumsum(c(0, comp)); bb <- min(rh * 0.34, 0.012)
        for (k in 1:3) graphics::rect(xs[k], y - bb, xs[k + 1], y + bb, col = c(col_obs, col_int, col_gap)[k], border = NA)
      }
    }
    if (ci == length(chunks))
      graphics::text(0, 0.0, sprintf("%d flagged -> detail pages follow    -    %d healthy -> no page",
                     n_flag, n - n_flag), adj = c(0, 1), cex = 0.72, col = "#888780", xpd = NA)
  }
  invisible(NULL)
}

# PDF detail page for one flagged deployment: regularization impact, gap diagnostics (sorted gap
# durations, replacing the old interval histogram), an annotated coverage strip, and a zoom on the
# most severe event. Drawn only for review/critical deployments (or all, under force.plots).
#' @keywords internal
#' @noRd
.drawRegularizationDetail <- function(p) {
  col_obs <- "#639922"; col_int <- "#EF9F27"; col_gap <- "#E24B4A"; col_grid <- "#185FA5"; col_raw <- "#444441"
  m <- p$m
  scol <- c(ok = col_obs, review = col_int, critical = col_gap, unchanged = "#888780", empty = "#888780")[m$status]
  if (is.na(scol)) scol <- "#888780"
  graphics::layout(matrix(c(1, 1, 2, 3, 4, 4, 5, 5), ncol = 2, byrow = TRUE), heights = c(0.4, 1.35, 0.72, 1.4))

  # ---- header ----
  graphics::par(mar = c(0.2, 1, 0.6, 1)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
  graphics::text(0, 0.55, m$id, adj = c(0, 0.5), font = 2, cex = 1.4)
  graphics::text(graphics::strwidth(m$id, cex = 1.4) + 0.015, 0.55, toupper(m$status), adj = c(0, 0.5),
                 font = 2, cex = 0.9, col = scol)
  graphics::text(1, 0.55, sprintf("%g Hz   %s -> %s rows   jitter %s ms", m$nominal_hz,
                 .formatLargeNumber(m$n_original), .formatLargeNumber(m$n_regular), m$jitter_mad_ms),
                 adj = c(1, 0.5), cex = 0.85, col = "#5F5E5A")

  # ---- regularization impact (left) ----
  graphics::par(mar = c(0.5, 1, 1.6, 1)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
  graphics::title(main = "regularization impact", cex.main = 0.95, adj = 0)
  rows <- c(sprintf("original rows      %s", .formatNumber(m$n_original)),
            sprintf("regularized rows   %s", .formatNumber(m$n_regular)),
            sprintf("rows added         +%s (%+.1f%%)", .formatNumber(m$rows_added), m$rows_added_pct))
  for (k in seq_along(rows)) graphics::text(0, 0.95 - (k - 1) * 0.15, rows[k], adj = c(0, 1), cex = 0.9, family = "mono")
  yb <- 0.32; comp <- c(m$pct_obs, m$pct_interp, m$pct_gap) / 100; xs <- cumsum(c(0, comp))
  for (k in 1:3) graphics::rect(xs[k], yb - 0.06, xs[k + 1], yb + 0.06, col = c(col_obs, col_int, col_gap)[k], border = NA)
  graphics::text(0, yb - 0.15, sprintf("observed %.1f%%    interp %.1f%%    gap %.1f%%",
                 m$pct_obs, m$pct_interp, m$pct_gap), adj = c(0, 1), cex = 0.8, col = "#5F5E5A")

  # ---- gap diagnostics (right) ----
  graphics::par(mar = c(0.5, 1, 1.6, 1)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
  graphics::title(main = "gap diagnostics", cex.main = 0.95, adj = 0)
  graphics::text(0, 0.95, sprintf("gaps %d     largest %s     median %s     missing %s",
                 m$n_gaps, .formatDurationShort(m$largest_gap_s), .formatDurationShort(m$median_gap_s),
                 .formatDurationShort(m$total_missing_s)), adj = c(0, 1), cex = 0.82, col = "#5F5E5A")
  gs <- m$gap_stats
  if (!is.null(gs) && gs$n_gaps > 0) {
    top <- order(-gs$durations_s)[seq_len(min(6L, gs$n_gaps))]; dd <- gs$durations_s[top]; mx <- max(dd)
    yk <- seq(0.78, 0.10, length.out = length(dd)); bh <- min(0.06, 0.55 / length(dd))
    for (k in seq_along(dd)) {
      graphics::rect(0, yk[k] - bh, 0.78 * dd[k] / mx, yk[k] + bh, col = col_gap, border = NA)
      graphics::text(0.80, yk[k], .formatDurationShort(dd[k]), adj = c(0, 0.5), cex = 0.78)
    }
  } else {
    graphics::text(0.5, 0.4, "no unresolved gaps", adj = c(0.5, 0.5), cex = 0.9, col = "#888780")
  }

  # ---- coverage strip ----
  graphics::par(mar = c(2.4, 1, 2.0, 1)); nb <- length(p$cov)
  graphics::plot(c(0, nb), c(0, 1), type = "n", axes = FALSE, xlab = "", ylab = "",
                 main = "coverage over deployment", cex.main = 0.92)
  pal <- c(col_obs, col_int, col_gap)
  graphics::rect(seq_len(nb) - 1, 0, seq_len(nb), 1, col = pal[p$cov + 1L], border = NA)
  graphics::rect(0, 0, nb, 1, border = "#888780", lwd = 0.5)
  if (!is.null(p$gap_marks)) for (k in seq_len(nrow(p$gap_marks))) {
    xx <- p$gap_marks$frac[k] * nb
    graphics::segments(xx, 1, xx, 1.12, col = col_gap, lwd = 0.9, xpd = NA)
    graphics::text(xx, 1.16, .formatDurationShort(p$gap_marks$dur[k]), cex = 0.68, col = col_gap, adj = c(0.5, 0), xpd = NA)
  }
  graphics::axis(1, at = c(0, nb / 2, nb), labels = format(c(p$t_start, p$t_mid, p$t_end), "%d-%b %H:%M"),
                 cex.axis = 0.72, tcl = -0.3)

  # ---- targeted zoom ----
  graphics::par(mar = c(3.2, 3.6, 2.0, 1), mgp = c(2.1, 0.6, 0))
  z <- p$zoom
  if (is.null(z)) {
    graphics::plot.new(); graphics::title("targeted zoom: nothing noteworthy", cex.main = 0.9)
  } else if (z$type == "gap") {
    nL <- length(z$pre_v); nR <- length(z$post_v); gw <- max(2L, round((nL + nR) * 0.2))
    xL <- seq_len(nL); xR <- if (nR) nL + gw + seq_len(nR) else integer(0); xmax <- max(2L, nL + gw + nR)
    yl <- range(c(z$pre_v, z$post_v, z$raw_pre$v, z$raw_post$v), na.rm = TRUE)
    if (!all(is.finite(yl))) yl <- c(0, 1)
    graphics::plot(NA, xlim = c(1, xmax), ylim = yl, axes = FALSE, xlab = "", ylab = z$col,
                   main = "largest gap (zoom)", cex.main = 0.9, cex.lab = 0.85)
    graphics::rect(nL + 0.5, yl[1], nL + gw + 0.5, yl[2], col = "#E24B4A22", border = NA)
    graphics::text(nL + gw / 2 + 0.5, yl[2], .formatDurationShort(z$gap_s), col = col_gap, cex = 0.82, adj = c(0.5, 1))
    if (nL) graphics::lines(xL, z$pre_v, col = col_grid, lwd = 1.1)
    if (nR) graphics::lines(xR, z$post_v, col = col_grid, lwd = 1.1)
    if (nL && length(z$raw_pre$t))
      graphics::points(stats::approx(as.numeric(z$pre_t), xL, xout = as.numeric(z$raw_pre$t), rule = 2)$y,
                       z$raw_pre$v, pch = 16, col = col_raw, cex = 0.5)
    if (nR && length(z$raw_post$t))
      graphics::points(stats::approx(as.numeric(z$post_t), xR, xout = as.numeric(z$raw_post$t), rule = 2)$y,
                       z$raw_post$v, pch = 16, col = col_raw, cex = 0.5)
    graphics::axis(2, cex.axis = 0.8, las = 1); graphics::box(col = "#CCCCCC")
    graphics::axis(1, at = c(1, xmax), labels = format(c(z$gap_t0, z$gap_t1), "%H:%M"), cex.axis = 0.78)
  } else {
    yl <- range(c(z$gv, z$ov), na.rm = TRUE); if (!all(is.finite(yl))) yl <- c(0, 1)
    graphics::plot(z$gt, z$gv, type = "l", col = col_grid, lwd = 1.1, ylim = yl, xlab = "time", ylab = z$col,
                   main = "largest interpolation (zoom)", cex.main = 0.9, cex.axis = 0.8, cex.lab = 0.85, las = 1)
    if (any(z$gi)) graphics::points(z$gt[z$gi], z$gv[z$gi], pch = 1, col = col_int, cex = 0.9, lwd = 1.2)
    graphics::points(z$ot, z$ov, pch = 16, col = col_raw, cex = 0.5)
    graphics::legend("topright", legend = c("raw", "interpolated"), pch = c(16, 1),
                     col = c(col_raw, col_int), bty = "n", cex = 0.72)
  }
  invisible(NULL)
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
