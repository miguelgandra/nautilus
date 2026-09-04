#######################################################################################################
# Place archival tag records on a regular time grid ###################################################
#######################################################################################################

#' Place archival tag records on a regular time grid
#'
#' @description
#' Resamples each deployment onto an evenly spaced time grid at its nominal sampling rate. Original
#' observations are assigned to the nearest grid point, short gaps are interpolated, and longer gaps are
#' retained as missing values so that periods without measurement remain visible to later analyses.
#'
#' Archival tags rarely produce the perfectly even series their nominal rate implies: clocks drift,
#' buffers flush late, and power saving or memory pressure drops samples outright. Most subsequent
#' analyses assume even spacing. Frequency-domain methods such as tail-beat estimation read an uneven
#' series as though it were even, dead-reckoning integrates over assumed time steps, and dive metrics
#' measure durations in samples. None of these signal the problem, which is why it is worth resolving
#' beforehand.
#'
#' The function is intended to be applied after the record has been trimmed to the deployment period
#' with [filterDeploymentData()], and before [processTagData()]. Each deployment is classified by how
#' much its coverage was affected, and that classification, with the supporting statistics, is recorded
#' in the deployment metadata.
#'
#' @param data A tag dataset, a list of tag datasets, a data frame containing multiple deployments
#'   identified by `id.col`, or a character vector of `.rds` file paths. When file paths are supplied,
#'   deployments are processed sequentially, allowing large collections to be regularised without
#'   loading them all into memory.
#' @param id.col Column identifying individuals (default `"ID"`).
#' @param datetime.col Column containing timestamps (default `"datetime"`).
#' @param time.threshold How far, in seconds, an observation may sit from a grid point and still be
#'   assigned to it. `NULL` (default) uses half the nominal sampling interval, so each observation
#'   belongs to exactly one grid point. Widen it only where a tag's timestamps are known to be coarse:
#'   too wide and distinct observations compete for the same grid point.
#' @param gap.threshold Longest gap, in seconds, that will be interpolated (default `5`). Longer gaps
#'   are retained as missing. Set to `0` to interpolate nothing.
#' @param interpolation.method Method used to fill gaps up to `gap.threshold`: `"linear"` (default,
#'   [zoo::na.approx()]), `"spline"` ([zoo::na.spline()]), or `"locf"`, last observation carried forward
#'   ([zoo::na.locf()]).
#' @param plot Whether to draw the diagnostic report to the active graphics device (default `FALSE`).
#' @param plot.file Path to a multi-page PDF holding the diagnostic report, or `NULL` (default). The
#'   parent directory must already exist, and the path must end in `.pdf`. Independent of `plot`: set
#'   either, or both. See Details for what the report contains.
#' @param review.thresholds Named list overriding the thresholds that classify a deployment as
#'   `"review"` or `"critical"`, or `NULL` (default) to use the defaults throughout. See Details for the
#'   recognised fields.
#' @param force.plots Whether to produce a detailed page for every deployment rather than only the
#'   flagged ones (default `FALSE`).
#' @param return.data Whether to return the regularised datasets in memory (default `TRUE`). When
#'   `FALSE`, the function returns the paths of the `.rds` files written to `output.dir`, which feed
#'   directly into the next step's `data` argument; this requires `output.dir` to be specified.
#' @param output.dir An existing directory in which to save one regularised `<id>.rds` file per
#'   deployment. Supplying a directory is what triggers saving; `NULL` (default) writes nothing.
#' @param exclusions.file Optional path to the shared deployment-exclusion log, a CSV recording every
#'   deployment this stage set aside and why. The log holds current state, not history: each stage
#'   refreshes its own rows for the deployments in the current call, so a deployment that stops being
#'   excluded loses its row without disturbing deployments outside a partial run. Pass the same path to
#'   every stage, and to [summarizeTagData()], which uses it to report why each deployment is missing.
#'   Default `NULL`, which writes nothing.
#' @param output.suffix Optional string appended to each saved file name, before `.rds`, to label a
#'   processing run or avoid overwriting an earlier one. Only used when `output.dir` is specified.
#' @param compress Compression used when saving `.rds` files: `TRUE` (default, gzip), `FALSE`, or one of
#'   `"gzip"`, `"bzip2"` or `"xz"`. Only used when `output.dir` is specified. See [base::saveRDS()].
#' @param verbose How much detail to print: `0`/`"quiet"`, `1`/`"normal"`, or `2`/`"detailed"`
#'   (default), which adds per-deployment diagnostics.
#'
#' @details
#' ## Regularisation procedure
#'
#' For each deployment:
#'
#' \enumerate{
#'   \item The nominal sampling interval is taken as the median of the observed timestamp differences.
#'   \item A regular time sequence is generated at that interval, spanning the first to the last
#'     timestamp of the original record.
#'   \item Each original observation is assigned to its nearest grid point, provided it falls within
#'     `time.threshold` of it. A grid point with no observation within that distance is left missing.
#'   \item Gaps no longer than `gap.threshold` are filled using `interpolation.method`; longer gaps are
#'     retained as missing.
#' }
#'
#' ## Assigning observations to the grid
#'
#' An observation further from every grid point than `time.threshold` is not forced onto one it does not
#' belong to: the grid point is left empty and treated as a gap. The default of half the nominal interval
#' partitions the timeline exactly, so no observation is discarded and none is assigned twice.
#'
#' ## Which gaps are filled
#'
#' The distinction drawn by `gap.threshold` is between two different events. A dropped observation or two
#' is a recording artefact, and interpolating across it restores a series the sensor would otherwise have
#' produced. A gap of minutes is a period with no measurement, and filling it fabricates behaviour: a
#' dive that was never recorded, or a stretch of level swimming that never happened. Choose the threshold
#' from what the sensor and the animal can change over; a value spanning a whole dive will invent one.
#'
#' ## Channels sampled below the grid rate
#'
#' Some tags log a channel more slowly than the inertial sensors, typically depth or temperature. Such
#' channels are recognised and left at their own cadence rather than being densified onto every grid
#' point, which would turn a once-per-second measurement into a spuriously high-resolution one and make
#' its empty rows look like missing data in the coverage statistics.
#'
#' ## Records already on a regular grid
#'
#' A record whose intervals all fall within `time.threshold` of the nominal interval is already regular.
#' It is passed through unchanged: no grid is constructed and nothing is interpolated. The processing
#' history records this as `regularization_performed = FALSE`, so a record that needed no work remains
#' distinguishable from one that was never processed.
#'
#' ## Quality classification
#'
#' Each deployment is classified `"ok"`, `"review"` or `"critical"` from three coverage metrics: the
#' percentage of grid points left as gaps, the percentage filled by interpolation, and the percentage by
#' which the row count grew relative to the original record. Reaching any critical threshold gives
#' `"critical"`, reaching any review threshold gives `"review"`, and otherwise the deployment is `"ok"`.
#'
#' `review.thresholds` overrides the defaults, field by field:
#'
#' \describe{
#'   \item{`gap_pct_review`, `gap_pct_critical`}{Percentage of grid points left as gaps. Defaults `1`
#'     and `5`.}
#'   \item{`interp_pct_review`, `interp_pct_critical`}{Percentage of grid points filled by
#'     interpolation. Defaults `5` and `20`.}
#'   \item{`rows_added_pct_review`, `rows_added_pct_critical`}{Percentage increase in row count over the
#'     original record. Defaults `10` and `100`.}
#'   \item{`large_gap_seconds`}{Optional absolute limit: a record whose longest single gap reaches this
#'     many seconds is classified `"critical"`. Default `NULL`, disabled. This field has no review tier.}
#' }
#'
#' The classification governs which deployments receive a detailed diagnostic page. It does not affect
#' the returned data: a `"critical"` deployment is regularised and returned like any other, flagged
#' rather than withheld.
#'
#' ## Diagnostic report
#'
#' The report is a two-level triage. A global summary page tabulates every deployment, worst first, with
#' its status and key metrics. Detailed pages follow only for deployments classified `"review"` or
#' `"critical"`, each showing the regularisation impact, gap diagnostics, an annotated coverage strip and
#' a zoom on the most severe event. Healthy deployments in a batch get no detailed page, so the report
#' stays short on a large, clean collection.
#'
#' A call processing a single deployment always produces its detailed page, and `force.plots = TRUE`
#' produces one for every deployment regardless of status.
#'
#' ## Processing history
#'
#' Whether or not plots are drawn, each deployment records a `regularizeTimeSeries` step giving the
#' nominal rate and timestamp jitter, the row counts before and after, the numbers and percentages of
#' interpolated and missing grid points, the longest gap, the settings applied, and the resulting status.
#' The coverage statistics are therefore available without rerunning the function or reading the report.
#'
#' @return When `return.data = TRUE`, a named list of regularised `data.table` objects keyed by
#'   identifier, one per deployment, whether the input was a single dataset, a list, or a vector of file
#'   paths. When `return.data = FALSE`, a character vector of the paths to the written `.rds` files.
#'   Files are written whenever `output.dir` is specified, regardless of the value of `return.data`.
#'
#' @seealso [filterDeploymentData()] for trimming the record to the deployment period, which should come
#'   first; [processTagData()] for the movement and orientation processing that follows;
#'   [importTagData()] for reading the raw exports; [zoo::na.approx()] for the interpolation methods.
#'
#' @importFrom data.table as.data.table setorder setnames setcolorder
#' @importFrom zoo na.approx na.spline na.locf
#' @importFrom stats median mad
#'
#' @examples
#' # An irregular, jittered series placed on a uniform grid, with short gaps filled.
#' d <- data.frame(ID = "shark01",
#'                 datetime = as.POSIXct("2020-01-01 00:00:00", tz = "UTC") +
#'                            c(0, 0.9, 2.1, 3.0, 4.2, 7.0, 8.1, 9.0),
#'                 depth = c(1.0, 1.2, 1.5, 1.8, 2.0, 3.1, 3.4, 3.6))
#' reg <- regularizeTimeSeries(d, gap.threshold = 2, verbose = FALSE)
#' reg[["shark01"]]
#'
#' \dontrun{
#' # A batch, with a diagnostic report and stricter gap tolerance.
#' regularised <- regularizeTimeSeries(deployed,
#'                                     gap.threshold = 2,
#'                                     plot.file = "./qc/regularization.pdf")
#'
#' # A collection too large to hold in memory: write each deployment and pass the paths on.
#' regularizeTimeSeries(list.files("./deployed", full.names = TRUE),
#'                      return.data = FALSE, output.dir = "./regularized")
#' }
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
                                 exclusions.file = NULL,
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
  .assert_writable_file(exclusions.file, "exclusions.file", ext = "csv", null_ok = TRUE)
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
  skipped_ids <- character(0)   # deployments set aside for missing/unusable input
  scope_ids <- character(0)     # every deployment evaluated, including successful partial runs
  # the reason travels with the id, for the shared exclusions log
  skipped_rows <- list()
  note_skip <- function(id, reason) {
    scope_ids <<- c(scope_ids, as.character(id))
    skipped_ids <<- c(skipped_ids, id)
    skipped_rows[[length(skipped_rows) + 1L]] <<- .exclusionsRow(id, "regularizeTimeSeries", reason)
  }

  for (i in seq_along(data)) {

    ############################################################################
    # load data for the current individual if using file paths #################
    if (is_filepaths) {

      # get current file path
      file_path <- data[i]

      # load current file
      individual_data <- readRDS(file_path)

      # This function only ever required an ID and a datetime (it interpolates whatever sensor channels
      # it finds - see the intersect() at the resampling step), so a missing column here is a structural
      # problem with ONE file, not a reason to abandon the batch.
      missing_cols <- setdiff(required_cols, names(individual_data))
      skip_reason <- if (length(missing_cols) > 0)
        .explainMissingColumns(missing_cols, tryCatch(attr(individual_data, "nautilus", exact = TRUE),
                                                      error = function(e) NULL))
      else if (!inherits(individual_data[[datetime.col]], "POSIXct")) "the datetime column is not POSIXct"
      if (!is.null(skip_reason)) {
        .log_skip(lvl, tools::file_path_sans_ext(basename(file_path)), "  ", skip_reason,
                  " ", cli::symbol$bullet, " skipped")
        note_skip(tools::file_path_sans_ext(basename(file_path)), skip_reason)
        .log_gap(lvl)
        next
      }
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

    scope_ids <- c(scope_ids, as.character(id))

    # per-individual sub-header (level-2 only; groups this individual's detail lines)
    .log_h2(lvl, sprintf("%s (%d/%d)", id, i, n_animals))

    # skip NULL or empty elements in the list
    if (is.null(individual_data) || nrow(individual_data) == 0) {
      .log_skip(lvl, id, "  empty dataset ", cli::symbol$bullet, " skipped")
      note_skip(id, "empty dataset")
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

    # Split the channels by their OWN sampling cadence. Some tags log a channel slower than the IMU (CEiiA
    # writes Velocity / Ticks-per-second at 1 Hz against a 20 Hz inertial grid), so 19 of every 20 rows are
    # structurally empty for that channel. Those are NOT gaps, and treating them as such corrupts both steps
    # below: it densifies the channel ~20x with interpolated values indistinguishable from observations
    # (which downstream pooled statistics then count as independent samples), and it makes almost every grid
    # row look "missing" in the coverage tally, raising a false "critical" status.
    sub_rate  <- vapply(sensor_cols, function(col) .channelCadence(complete_data[[col]]) > 1.5, logical(1))
    grid_cols <- sensor_cols[!sub_rate]                       # sampled at (or near) the grid rate
    slow_cols <- sensor_cols[sub_rate]                        # genuinely slower: left at their own cadence

    # snapshot the coverage state of the regular grid BEFORE interpolation: a grid row is
    # "missing" if any GRID-RATE sensor channel is NA (an out-of-threshold slot or a true gap). Slower
    # channels are excluded - their empty rows are by design, not missing data.
    na_after_join <- if (length(grid_cols) > 0) {
      Reduce(`|`, lapply(grid_cols, function(col) is.na(complete_data[[col]])))
    } else rep(FALSE, nrow(complete_data))

    if (gap.threshold > 0 && length(grid_cols) > 0) {

      # check if any grid-rate sensor column has NA values
      has_gaps <- any(vapply(grid_cols, function(col) anyNA(complete_data[[col]]), logical(1)))

      if (has_gaps) {

        # identify rows where any of the present GRID-RATE sensor columns have NA
        na_pattern <- Reduce(`|`, lapply(grid_cols, function(col) is.na(complete_data[[col]])))

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

        # interpolate small gaps in the GRID-RATE sensor channels (numeric only). Slower channels are
        # deliberately excluded: nothing is invented between their own samples, so they reach downstream at
        # their true cadence and a pooled statistic counts each observation once.
        numeric_cols <- grid_cols[vapply(grid_cols, function(col) is.numeric(complete_data[[col]]), logical(1))]

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

    # final NA state, then classify each regular-grid row as observed / interpolated / gap. Judged on the
    # GRID-RATE channels only, matching `na_after_join`: a slower channel's empty rows are its cadence, not
    # a gap, and counting them here would report a near-total loss of coverage on a perfectly good record.
    na_final <- if (length(grid_cols) > 0) {
      Reduce(`|`, lapply(grid_cols, function(col) is.na(complete_data[[col]])))
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
  # Deployments set aside for missing/unusable input are announced at ANY verbosity: a silent skip in a
  # large batch is how a cohort quietly shrinks between pipeline steps.
  .warn_grouped(
    "{length(skipped_ids)} deployment{?s} {?was/were} skipped for missing or unusable input.",
    items = skipped_ids,
    hints = c("They carry no entry in the returned data and were not written to {.arg output.dir}.",
              "A channel removed by {.fn checkSensorIntegrity} is recorded in {.code meta$sensors$excluded}."))

  # Refresh only this stage's rows for deployments evaluated by the current call.
  excl <- .exclusionsBind(skipped_rows)
  .exclusionsWrite(excl, exclusions.file, "regularizeTimeSeries", scope.ids = scope_ids)

  if (lvl >= 1L) {
    .log_summary(lvl)
    # "processed", not "regularized": a record left unchanged, or set aside, was processed too, and
    # the stronger word would imply every deployment came through cleanly
    .log_done(lvl, n_done, " of ", n_animals, " deployment", if (n_animals != 1) "s", " processed")
    # roll-ups, the outcome tally, the deployments worth looking at, and where it all went. The output
    # pointers moved inside so the runtime stays the last line, as it is in every other function.
    .printRegularizationTriage(lvl, run_metrics, output.dir, plot.file, exclusions.file)
    cli::cli_text("")
    .log_runtime(lvl, start.time)
  }

  # unified output contract: a named list (one element per individual, consistent with
  # importTagData() and filterDeploymentData()) when return.data, else the written paths
  ids <- if (is_filepaths) tools::file_path_sans_ext(basename(data)) else names(data)
  out <- .collectOutput(results, saved, return.data, ids)
  if (!is.null(out) && nrow(excl)) attr(out, "nautilus.exclusions") <- excl
  # Returning `out` bare would strip the invisibility .collectOutput() set on the paths branch, so a
  # top-level call printed the whole wall of file paths. Re-apply it; the data branch stays visible.
  if (isTRUE(return.data)) out else invisible(out)
}


#######################################################################################################
# Internal: regularization diagnostic panel ###########################################################
#######################################################################################################

# Default review thresholds (dimensionless metrics generalise across sampling rates; the absolute
# `large_gap_seconds` trigger is off by default). User overrides are merged on top.
#' @keywords internal
#' @noRd
#' Estimate a channel's own sampling cadence, in grid rows per observation
#'
#' A tag may log a channel more slowly than the inertial grid it is being placed on - CEiiA writes
#' `Velocity (m/s)` and `Ticks/s` once a second against a 20 Hz grid, leaving 19 of every 20 rows empty for
#' those columns. Those empty rows are the channel's DESIGN, not missing data, so they must not be counted
#' as gaps or filled by interpolation.
#'
#' The cadence is the median spacing between consecutive observations, which is robust to real dropouts
#' (a handful of long gaps cannot move the median) and returns 1 for any channel sampled on every row.
#' @param x A numeric channel, possibly containing NAs.
#' @return Median rows per observation; 1 when the channel is sampled at (or near) the grid rate, or when
#'   there are too few observations to judge - the conservative answer, since it preserves existing
#'   behaviour rather than silently disabling interpolation.
#' @keywords internal
#' @noRd
.channelCadence <- function(x) {
  idx <- which(!is.na(x))
  if (length(idx) < 3L) return(1)
  s <- stats::median(diff(idx))
  if (!is.finite(s) || s < 1) 1 else s
}


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
.printRegularizationTriage <- function(lvl, metrics, output.dir = NULL, plot.file = NULL,
                                       exclusions.file = NULL, max.listed = 10L) {
  if (lvl < 1L || !length(metrics)) return(invisible(NULL))
  num <- function(f) vapply(metrics, function(m) {
    v <- m[[f]]; if (is.null(v) || !length(v)) NA_real_ else as.numeric(v) }, numeric(1))
  status <- vapply(metrics, function(m) m$status %||% "ok", character(1))

  ## ---- cohort roll-ups, in the calculateTailBeats form ------------------------------------------
  # the qualifier rides the median, not the parenthesis: "median 0.6% of grid points (IQR ...)"
  spread <- function(v, unit = "", d = 1, of = "") {
    v <- v[is.finite(v)]
    if (!length(v)) return(NULL)
    q <- stats::quantile(v, c(0.25, 0.5, 0.75), names = FALSE)
    sprintf("median %.*f%s%s (IQR %.*f\u2013%.*f, range %.*f\u2013%.*f)",
            d, q[2], unit, of, d, q[1], d, q[3], d, min(v), d, max(v))
  }
  ra <- spread(num("rows_added_pct"), "%")
  if (!is.null(ra)) .log_arrow(lvl, "rows added: ", ra)
  it <- spread(num("pct_interp"), "%", of = " of grid points")
  if (!is.null(it)) .log_arrow(lvl, "interpolated: ", it)

  # Gaps get their own line because they are what most warrants catching: how many records came
  # through gap-free, and the single worst gap with the deployment that owns it.
  gp <- num("pct_gap"); lg <- num("largest_gap_s")
  if (any(is.finite(gp)) || any(is.finite(lg))) {
    free <- sum(is.finite(gp) & gp == 0)
    parts <- sprintf("%d deployment%s gap-free", free, if (free != 1L) "s" else "")
    if (any(is.finite(lg)) && max(lg, na.rm = TRUE) > 0) {
      w <- which.max(replace(lg, !is.finite(lg), -Inf))
      parts <- paste0(parts, " \u00b7 largest ", .fmtSecondsSpelled(lg[w]),
                      " (", metrics[[w]]$id, ")")
    }
    .log_arrow(lvl, "gaps: ", parts)
  }

  ## ---- the outcome tally, in a fixed order so the rows visibly sum to the cohort ----------------
  ord <- c("ok", "unchanged", "review", "critical", "empty", "skipped")
  tally <- table(factor(status, levels = ord))
  keep <- as.integer(tally) > 0L
  if (any(keep)) {
    sym <- c(ok = cli::col_green(cli::symbol$tick), unchanged = cli::symbol$bullet,
             review = cli::symbol$bullet, critical = cli::col_red("!"),
             empty = cli::symbol$bullet, skipped = cli::symbol$bullet)
    .log_section(lvl, "Status")
    .log_rows(lvl, stats::setNames(as.integer(tally)[keep], ord[keep]),
              symbols = unname(sym[ord[keep]]))
  }

  ## ---- only the deployments the run is asking someone to look at -------------------------------
  flag <- which(status %in% c("review", "critical"))
  if (length(flag)) {
    flag <- flag[order(-.regularizationSeverity(metrics[flag]))]
    shown <- utils::head(flag, max.listed)
    .log_section(lvl, "Needs review")
    for (i in shown) {
      m <- metrics[[i]]
      mark <- if (identical(m$status, "critical")) cli::col_red("!") else cli::symbol$bullet
      pc <- function(x) if (is.na(x)) "-" else sprintf("%.1f%%", x)
      cli::cli_verbatim(sprintf("  %s %-12s %-9s \u00b7 +%s rows \u00b7 %s gaps \u00b7 largest %s",
                                mark, substr(m$id, 1, 12), m$status,
                                pc(m$rows_added_pct), pc(m$pct_gap),
                                .fmtSecondsSpelled(m$largest_gap_s)))
    }
    if (length(flag) > length(shown))
      cli::cli_verbatim(sprintf("    and %d more", length(flag) - length(shown)))
  }

  out_rows <- c(if (!is.null(output.dir)) c(directory = output.dir),
                if (!is.null(plot.file)) c(plots = plot.file),
                if (!is.null(exclusions.file)) c(exclusions = exclusions.file))
  if (length(out_rows)) { .log_section(lvl, "Output"); .log_rows(lvl, out_rows) }
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
  # x is the coverage BIN INDEX, not time: three fixed positions carrying three named instants
  .axisTime(c(p$t_start, p$t_mid, p$t_end), at = c(0, nb / 2, nb), fmt = "%d-%b %H:%M",
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
    .axisTime(c(z$gap_t0, z$gap_t1), at = c(1, xmax), fmt = "%H:%M", cex.axis = 0.78)
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
