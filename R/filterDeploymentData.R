#######################################################################################################
# Function to identify and extract deployment periods #################################################
#######################################################################################################

#' Filter deployment periods from a given dataset
#'
#' @description
#' This function identifies and extracts actual deployment periods from high-resolution biologging data.
#' It offers two primary methods for defining these periods: an **automated depth-based approach**
#' or the option to **supply custom start and end times** for each individual.
#'
#' When using the automated depth-based approach, the function analyzes changes in depth data
#' using binary segmentation (`cpt.meanvar`) to detect change points in both mean and variance.
#' It then extracts the periods between the pre-deployment and post-deployment phases
#' based on the specified depth and variance thresholds.
#'
#' For datasets with a sampling frequency greater than 1 Hz, the function automatically
#' downsamples the data to 1 Hz. This is achieved by rounding datetime values to the nearest second
#' and computing the mean for each second, significantly improving processing speed and
#' preventing memory bottlenecks when handling large datasets.
#'
#' The function also provides robust options to visualize the identified deployment periods
#' with custom plot options, including additional behavioral metrics, to aid in visual review and validation.
#'
#' @param data A list of data.tables/data.frames, one for each individual; a single aggregated data.table/data.frame
#' containing data from multiple animals (with an 'ID' column); or a character vector of file paths pointing to
#' `.rds` files, each containing data for a single individual. When a character vector is provided,
#' files are loaded sequentially to optimize memory use. The output of the \link{processTagData} function
#' is strongly recommended, as it formats the data appropriately for all downstream analysis.
#' @param id.col A string representing the column name for the ID field (default is "ID").
#' @param datetime.col A string specifying the name of the column that contains timestamps for each observation.
#' This column must be in "POSIXct" format for proper processing (default is "datetime").
#' @param depth.col A string specifying the name of the column that contains depth measurements. Depth data is
#' used for detecting deployment periods (default is "depth").
#' @param custom.deployment.times An optional `data.frame` or `data.table` with three columns: the ID column
#' (named as given by `id.col`, default "ID"), `start`, and `end`.
#' This allows users to manually specify deployment periods for each individual, overriding or supplementing the depth-based detection.
#' `start` and `end` must be in "POSIXct" format. Users can provide:
#' \itemize{
#'   \item Both `start` and `end` times to fully override automatic detection
#'   \item Only `start` time (with `end` as NA) - the end time will be estimated using depth analysis
#'   \item Only `end` time (with `start` as NA) - the start time will be estimated using depth analysis
#' }
#' When partial times are provided, the missing boundary is inferred using the same depth-based changepoint analysis
#' as the automated approach. If provided, `depth.threshold`, `variance.threshold`, and `max.changepoints`
#' are ignored for fully specified periods but used for estimating missing boundaries in partial specifications.
#' Default is `NULL`.
#' @param depth.threshold A numeric value specifying the minimum mean depth to classify a segment as part of
#' the deployment period (default is 3.5). This parameter is ignored for fully specified custom deployment times
#' but used when estimating missing boundaries in partial custom specifications.
#' @param variance.threshold A numeric value specifying the minimum variance in depth measurements to classify
#' a segment as part of the deployment period. Segments with variance below this value are considered spurious (default is 6).
#' This parameter is ignored for fully specified custom deployment times but used when estimating missing boundaries
#' in partial custom specifications.
#' @param max.changepoints An integer specifying the maximum number of changepoints to detect (default is 6).
#' This parameter is passed to the \code{\link[changepoint]{cpt.meanvar}} function. This parameter is ignored
#' for fully specified custom deployment times but used when estimating missing boundaries in partial custom specifications.
#' @param temp.col A string giving the temperature column used to corroborate the depth-based detection
#' (default "temp"). If the column is absent, temperature corroboration is silently skipped.
#' @param use.temperature Logical. If `TRUE` (default), temperature is used as a **secondary, strictly
#' additive** signal in the fully-automated path: it can rescue a deployment that stayed too shallow for
#' the depth criterion to catch (e.g. prolonged surface feeding) and extend the window across shallow
#' in-water edges, but it can never shrink the depth-based result. It is gated on a clear, sustained
#' difference between the out-of-water (boat/air) and in-water temperature regimes, so a flat or
#' uninformative temperature trace leaves the depth result unchanged. Depth remains the primary signal;
#' the accelerometer/gyroscope/magnetometer are intentionally **not** used, as the pre-attachment vessel
#' and diver phases generate high motion that would confound a motion-based detector. Ignored for custom
#' deployment times.
#' @param min.deployment.hours Numeric. Minimum duration (hours) for an automatically detected
#' deployment window; shorter windows (e.g. a transient depth spike or a brief diver test-dive) are
#' treated as "no deployment" and discarded (default 0.25). Ignored for custom deployment times.
#' @param plot A logical value indicating whether the diagnostic plots should be drawn to the
#' active graphics device. These plots generate diagnostic visuals showing depth and additional
#' metrics, assisting users in reviewing the extracted deployment periods.
#' Note: If set to `TRUE`, the code can take longer to run due to the delay in plotting extensive data series to the screen.
#' Default is `FALSE`.
#' @param plot.file Character. Path to a single multi-page PDF in which to write the diagnostic
#' panels (one page per individual). The parent directory must already exist (a missing directory is
#' an error, not silently created). Must end in `.pdf`. If `NULL` (default), no file is written.
#' Independent of `plot`: set `plot.file` to save without displaying, or set both to do both.
#' @param plot.metrics An optional character vector of column names indicating additional metrics
#' (e.g., acceleration or temperature) to include in the visualization. These metrics are plotted
#' alongside the depth data to aid in visually reviewing the deployment period assignments.
#' Required only if `plot` is `TRUE` or `plot.file` is set. If NULL (default) and plots are requested,
#' defaults to `c("temp", "ax")`. Must be length 2 if provided. These are cosmetic, not required for
#' detection: a metric absent from a dataset (e.g. `temp` when only an electronics temperature sensor
#' was available) is simply omitted from that individual's panel rather than causing an error.
#' @param plot.metrics.labels An optional character vector of axis labels for the two `plot.metrics`.
#' If NULL (default), labels are generated automatically as "Name (unit)" for recognised nautilus
#' channels (e.g. "Temperature (°C)", "Acc X (g)", "Depth (m)"), falling back to the raw column
#' name for any unrecognised or user-derived column. Provide this only to override the automatic
#' labels (e.g. for bespoke columns); must be length 2 if given. Ignored unless plots are generated.
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
#' @param verbose Verbosity level: `FALSE`/`0`/"quiet" (warnings and errors only), `TRUE`/`1`/"normal"
#' (header, one line per individual, final summary), or `2`/"detailed" (the default; adds low-level
#' per-step diagnostics). Defaults to `"detailed"`.
#'
#' @return When `return.data = TRUE`, a named list of filtered `data.table`s (one per successfully filtered
#' individual, keyed by ID). When `return.data = FALSE`, the written `.rds` file paths as a character vector,
#' returned **invisibly** (so a top-level call does not print them) and ready to chain into the next step's
#' `data` argument. Diagnostic plots are emitted as a side effect
#' (drawn to the active device when `plot = TRUE` and/or written to the multi-page PDF `plot.file`), not
#' returned. Data is written to disk whenever `output.dir` is set, regardless of the return value.
#'
#' @seealso \link{importTagData}, \link{processTagData}, \code{\link[changepoint]{cpt.meanvar}}.
#' @examples
#' \dontrun{
#' imported <- importTagData(folders, id.metadata = meta)
#' # Trim each record to the on-animal window detected from the depth trace:
#' deployed <- filterDeploymentData(imported,
#'                                  depth.threshold      = 3.5,
#'                                  min.deployment.hours = 0.25,
#'                                  plot                 = TRUE,
#'                                  plot.metrics         = c("temp", "az"))
#' }
#' @export


filterDeploymentData <- function(data,
                                 id.col = "ID",
                                 datetime.col = "datetime",
                                 depth.col = "depth",
                                 custom.deployment.times = NULL,
                                 depth.threshold = 3.5,
                                 variance.threshold = 6,
                                 max.changepoints = 6,
                                 temp.col = "temp",
                                 use.temperature = TRUE,
                                 min.deployment.hours = 0.25,
                                 plot = FALSE,
                                 plot.file = NULL,
                                 plot.metrics = NULL,
                                 plot.metrics.labels = NULL,
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

  # validate scalar arguments and resolve the verbosity level (0 quiet / 1 normal / 2 detailed)
  lvl <- .verbosity(verbose)

  # show warnings as they occur (per-individual issues appear inline with their dataset) rather than
  # R's default deferred batch at the end. Only upgrade the default (warn = 0); never override a
  # user's stricter setting. Restored on exit.
  if (identical(getOption("warn"), 0L) || identical(getOption("warn"), 0)) {
    .oldwarn <- options(warn = 1); on.exit(options(.oldwarn), add = TRUE)
  }
  # silence data.table's "Processed N groups..." progress bar (from the 1 Hz down-sampling grouping),
  # which would otherwise interrupt the per-individual blocks. Restored on exit.
  .olddt <- options(datatable.showProgress = FALSE); on.exit(options(.olddt), add = TRUE)

  .assert_flag(return.data, "return.data")
  .assert_flag(plot, "plot")
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col"); .assert_string(depth.col, "depth.col")
  .assert_number(depth.threshold, "depth.threshold", min = 0)
  .assert_number(variance.threshold, "variance.threshold", min = 0)
  .assert_count(max.changepoints, "max.changepoints", min = 1)
  .assert_string(temp.col, "temp.col")
  .assert_flag(use.temperature, "use.temperature")
  .assert_number(min.deployment.hours, "min.deployment.hours", min = 0)
  .assert_writable_file(plot.file, "plot.file", ext = "pdf")   # fail-fast: parent dir must exist
  .assert_dir(output.dir, "output.dir")                        # fail-fast: must exist
  .assert_string(output.suffix, "output.suffix", null_ok = TRUE)
  .assert_compress(compress)

  # check if data is a character vector of RDS file paths
  is_filepaths <- is.character(data)
  .assert_nonempty(data, "data")             # loud failure on empty input (e.g. a typo'd list.files() -> character(0))
  if (is_filepaths) {
    missing_files <- data[!file.exists(data)]
    if (length(missing_files) > 0) {
      .abort(c("These input files were not found:", stats::setNames(missing_files, rep("*", length(missing_files)))))
    }
  } else if (!is.list(data) || inherits(data, "data.frame")) {
    # a single data.frame -> split by id.col
    .assert_columns(data, id.col, "data")
    data <- split(data, data[[id.col]])
  }

  # the sole illegal output request: keep nothing and write nowhere
  .assert_output(return.data, output.dir)

  # whether any diagnostic plot is produced at all (active device and/or PDF file)
  make_plots <- plot || !is.null(plot.file)

  # resolve the diagnostic-plot metrics (the two extra series shown alongside depth). Labels default
  # to auto-generated "Name (unit)" from the sensor lookup; plot.metrics.labels overrides per metric.
  if (make_plots) {
    if (is.null(plot.metrics)) plot.metrics <- c("temp", "ax")
    if (!is.character(plot.metrics) || length(plot.metrics) != 2) {
      .abort("{.arg plot.metrics} must be a character vector of length 2.")
    }
    if (is.null(plot.metrics.labels)) {
      plot.metrics.labels <- .metricLabel(plot.metrics)
    } else if (length(plot.metrics.labels) != 2) {
      .abort("{.arg plot.metrics.labels} must be a character vector of length 2.")
    }
  } else if (!is.null(plot.metrics.labels) && is.null(plot.metrics)) {
    .abort("{.arg plot.metrics.labels} was provided but {.arg plot.metrics} was not.")
  }

  # define required columns. Only the detection essentials are strictly required; `plot.metrics` are
  # best-effort cosmetics for the diagnostic panel - a dataset missing one (e.g. `temp`, which is now
  # legitimately absent when only an electronics temperature sensor was available) is still processed,
  # and the absent metric strip is simply omitted from the plot.
  required_cols <- c(id.col, datetime.col, depth.col)

  # if data is already in memory (not file paths), validate each dataset up front
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

  # validate custom.deployment.times if provided (ID column named by `id.col`; start/end fixed)
  if (!is.null(custom.deployment.times)) {
    .assert_columns(custom.deployment.times, c(id.col, "start", "end"), "custom.deployment.times")
    cdt <- custom.deployment.times
    if (!inherits(cdt$start, "POSIXct") || !inherits(cdt$end, "POSIXct")) {
      .abort("{.field start} and {.field end} in {.arg custom.deployment.times} must be POSIXct.")
    }
    cdt_ids <- as.character(cdt[[id.col]])
    # duplicate IDs (ambiguous which window applies)
    dups <- unique(cdt_ids[duplicated(cdt_ids)])
    if (length(dups)) {
      .abort(c("{.arg custom.deployment.times} has duplicate ID{?s}: {.val {dups}}.",
               "i" = "Provide at most one window per ID."))
    }
    # rows with neither boundary (no information)
    both_na <- is.na(cdt$start) & is.na(cdt$end)
    if (any(both_na)) {
      .abort(c("{.arg custom.deployment.times} has row{?s} with both {.field start} and {.field end} missing: {.val {cdt_ids[both_na]}}.",
               "i" = "Supply at least one boundary, or remove the row."))
    }
    # end must be strictly after start when both are given
    bad_order <- !is.na(cdt$start) & !is.na(cdt$end) & cdt$end <= cdt$start
    if (any(bad_order)) {
      .abort("{.arg custom.deployment.times}: {.field end} must be after {.field start} for ID{?s} {.val {cdt_ids[bad_order]}}.")
    }
    # IDs not present in the data are ignored (only checkable up front for in-memory input)
    if (!is_filepaths) {
      unknown <- setdiff(cdt_ids, names(data))
      if (length(unknown)) {
        cli::cli_warn(c("{.arg custom.deployment.times} has ID{?s} not in the data: {.val {unknown}}.",
                        "i" = "Those windows will be ignored - check for typos."))
      }
    }
  }

  ##############################################################################
  # Process each data element ##################################################
  ##############################################################################

  # initialize an empty list to store the processed data
  n_animals <- length(data)
  processed_data <- vector("list", length=n_animals)
  names(processed_data) <- names(data)
  saved <- vector("list", length=n_animals)      # per-item written paths (for return.data = FALSE)

  # header: a framed block, visually isolated from the per-individual sections that follow.
  # It states the run's capability/configuration, not a single global method (the realized per-
  # individual method is shown in the level-2 detail, and the realized mix is summarised at the end).
  hdr_bullets <- sprintf("Input: %d folder%s", n_animals, if (n_animals != 1) "s" else "")
  if (!is.null(output.dir)) hdr_bullets <- c(hdr_bullets, paste0("Output: ", output.dir))
  method_hdr <- if (is.null(custom.deployment.times)) "depth changepoints (automatic)"
                else "depth changepoints (automatic) + custom windows"
  .log_header(lvl, "filterDeploymentData", "Isolating deployment periods from raw dive data",
              bullets = hdr_bullets, arrow = paste0("Method: ", method_hdr))

  # graphics setup. When `plot`, draw to the caller's active device (saving/restoring its state).
  # When `plot.file`, open ONE multi-page PDF (one page per individual) and close it on exit.
  caller_dev <- grDevices::dev.cur()
  if (plot && caller_dev == 1L) { grDevices::dev.new(); caller_dev <- grDevices::dev.cur() }
  if (plot) oldpar <- graphics::par(no.readonly = TRUE)            # captured on the caller device
  file_dev <- NULL
  if (!is.null(plot.file)) {
    grDevices::pdf(plot.file, width = 11, height = 7.5)
    file_dev <- grDevices::dev.cur()
    on.exit(grDevices::dev.off(file_dev), add = TRUE)              # runs first: close the PDF
  }
  if (plot) on.exit({ if (caller_dev %in% grDevices::dev.list()) { grDevices::dev.set(caller_dev); graphics::par(oldpar) } }, add = TRUE)
  n_filtered <- 0L; n_discarded <- 0L; n_custom <- 0L; n_auto <- 0L   # n_custom/n_auto: realized method mix

  # iterate over each element in 'data'
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
      if (!inherits(individual_data[[datetime.col]], "POSIXct")) .abort("Column {.val {datetime.col}} in {.file {basename(file_path)}} must be of class {.cls POSIXct}.")
      if (is.null(attr(individual_data, "nautilus.version"))) {
        cli::cli_warn(c("{.file {basename(file_path)}} was likely not processed via {.fn importTagData}.",
                        "i" = "Run it through {.fn importTagData} first to ensure correct formatting."))
      }

      # get ID
      id <- unique(individual_data[[id.col]])[1]


    ############################################################################
    # data is already in memory (list of data frames/tables) ###################
    } else {
      # retrieve ID from the dataset based on the specified 'id.col' column
      id <- names(data)[i]
      # access the individual dataset
      individual_data <- data[[i]]
    }

    # per-individual sub-header (level-2 only; groups this individual's detail lines)
    .log_h2(lvl, sprintf("%s (%d/%d)", id, i, n_animals))

    # skip NULL or empty elements in the list
    if (is.null(individual_data) || length(individual_data) == 0) next

    # convert data.table to data.frame for processing
    if (inherits(individual_data, "data.frame")) {
      individual_data <- data.table::setDT(individual_data)
    }

    # ensure the consolidated nautilus metadata is present (migrating legacy attrs)
    individual_data <- .ensureMeta(individual_data)

    # ensure data is ordered by datetime
    data.table::setorderv(individual_data, datetime.col)

    # store original attributes, excluding internal ones
    discard_attrs <- c("row.names", "class", ".internal.selfref", "names")
    original_attributes <- attributes(individual_data)
    original_attributes <- original_attributes[!names(original_attributes) %in% discard_attrs]

    # correct negative depths
    individual_data[get(depth.col) < 0, (depth.col) := 0]

    # set default flags
    valid_dataset <- TRUE
    temp_rescued  <- FALSE   # set TRUE if temperature recovered a deployment depth alone missed


    ############################################################################
    # Prepare variables ########################################################
    ############################################################################

    # check if 'first.datetime' attribute exists in the dataset
    # if present, use it; otherwise, calculate the earliest datetime from the data
    first_datetime <- if ("first.datetime" %in% names(attributes(data[[i]]))) {
      attributes(data[[i]])$first.datetime
    } else {
      individual_data[, min(get(datetime.col), na.rm = TRUE)]
    }

    # check if 'last.datetime' attribute exists in the dataset
    # if present, use it; otherwise, calculate the latest datetime from the data
    last_datetime <- if ("last.datetime" %in% names(attributes(data[[i]]))) {
      attributes(data[[i]])$last.datetime
    } else {
      individual_data[, max(get(datetime.col), na.rm = TRUE)]
    }

    # positions for the diagnostic plot: FastGPS/User fixes from the canonical record
    # (meta$ancillary$positions); the deploy/pop-up reference times come from meta$deployment below
    pos_all <- .tagPositions(individual_data)
    positions_available <- nrow(pos_all) > 0
    if (positions_available) {
      mkpos <- function(sub) {
        out <- data.table::data.table(lon = sub$lon, lat = sub$lat)
        data.table::set(out, j = datetime.col, value = sub$datetime)
        out
      }
      fastloc_pos <- mkpos(pos_all[pos_all$type == "FastGPS", , drop = FALSE])
      user_pos    <- mkpos(pos_all[pos_all$type == "User", , drop = FALSE])
    }


    ############################################################################
    # Downsample data to 1 Hz ##################################################
    ############################################################################

    # calculate the (approximate) sampling frequency, in Hz (reported in the ordered block below)
    sampling_freq <- nrow(individual_data)/length(unique(lubridate::floor_date(individual_data[[datetime.col]], "sec")))
    sampling_freq <- round(sampling_freq)


    # check if the sampling frequency is greater than 1 Hz and downsample is required
    if(sampling_freq > 1){

      # determine which columns to downsample (depth always; temperature when used for detection;
      # plus any plot metrics). temp.col is optional - skipped if absent.
      cols_to_downsample <- depth.col
      if (use.temperature && temp.col %in% names(individual_data)) cols_to_downsample <- c(cols_to_downsample, temp.col)
      if (!is.null(plot.metrics)) cols_to_downsample <- c(cols_to_downsample, intersect(plot.metrics, names(individual_data)))
      cols_to_downsample <- unique(cols_to_downsample)

      # downsample to 1 Hz by rounding datetime to the nearest second and taking the mean value for each second
      reduced_data <- individual_data[, lapply(cols_to_downsample, function(col) mean(get(col), na.rm = TRUE)),
                                      by = .(lubridate::floor_date(get(datetime.col)))]

      # correct the column names
      new_names <- c(datetime.col, cols_to_downsample)
      data.table::setnames(reduced_data, old = names(reduced_data), new = new_names)

      # add ID column
      reduced_data[, (id.col) := id]

      # reorder columns
      data.table::setcolorder(reduced_data, c(id.col, setdiff(names(reduced_data), id.col)))

      # else, if downsampling is not required (sampling frequency <= 1 Hz)
    } else {

      # keep only relevant columns
      cols_to_keep <- c(id.col, datetime.col, depth.col)
      if (use.temperature && temp.col %in% names(individual_data)) cols_to_keep <- c(cols_to_keep, temp.col)
      if (make_plots) cols_to_keep <- c(cols_to_keep, intersect(plot.metrics, names(individual_data)))
      reduced_data <- individual_data[, unique(cols_to_keep), with = FALSE]

    }


    ############################################################################
    # Check for custom periods #################################################
    ############################################################################

    valid_deployment_times <- FALSE
    partial_deployment_times <- FALSE
    custom_start_only <- FALSE
    custom_end_only <- FALSE

    if (!is.null(custom.deployment.times)) {

      # find custom period for this ID
      custom_period <- custom.deployment.times[custom.deployment.times[[id.col]] == id, ]

      # use custom periods if available
      if (nrow(custom_period) > 0) {

        has_start <- !is.na(custom_period$start[1])
        has_end <- !is.na(custom_period$end[1])

        # case 1: both start and end are provided. A window partly outside the data is clamped to the
        # available range; only a window entirely outside falls back to depth-based detection.
        if (has_start && has_end) {
          attachtime <- custom_period$start[1]
          poptime <- custom_period$end[1]
          if (poptime <= first_datetime || attachtime >= last_datetime) {
            cli::cli_warn("{id}: custom window is entirely outside the data range; using depth-based detection instead.")
          } else {
            if (attachtime < first_datetime || poptime > last_datetime) {
              cli::cli_warn("{id}: custom window extends beyond the data range; clamped to the available data.")
              attachtime <- max(attachtime, first_datetime)
              poptime    <- min(poptime, last_datetime)
            }
            attach_idx <- which.min(abs(reduced_data[[datetime.col]] - attachtime))
            pop_idx    <- which.min(abs(reduced_data[[datetime.col]] - poptime))
            valid_deployment_times <- TRUE
          }

        # case 2: only start is provided (end estimated). Clamp an early start to the data start;
        # a start at/after the data end leaves no room, so fall back to depth-based estimation.
        } else if (has_start && !has_end) {
          partial_deployment_times <- TRUE
          custom_start_only <- TRUE
          attachtime <- custom_period$start[1]
          if (attachtime >= last_datetime) {
            cli::cli_warn("{id}: custom start time is at/after the end of the data; using depth-based detection instead.")
            partial_deployment_times <- FALSE
            custom_start_only <- FALSE
          } else {
            if (attachtime < first_datetime) {
              cli::cli_warn("{id}: custom start time precedes the data; clamped to the data start.")
              attachtime <- first_datetime
            }
            attach_idx <- which.min(abs(reduced_data[[datetime.col]] - attachtime))
          }

        # case 3: only end is provided (start estimated). Clamp a late end to the data end;
        # an end at/before the data start leaves no room, so fall back to depth-based estimation.
        } else if (!has_start && has_end) {
          partial_deployment_times <- TRUE
          custom_end_only <- TRUE
          poptime <- custom_period$end[1]
          if (poptime <= first_datetime) {
            cli::cli_warn("{id}: custom end time is at/before the start of the data; using depth-based detection instead.")
            partial_deployment_times <- FALSE
            custom_end_only <- FALSE
          } else {
            if (poptime > last_datetime) {
              cli::cli_warn("{id}: custom end time is after the data; clamped to the data end.")
              poptime <- last_datetime
            }
            pop_idx <- which.min(abs(reduced_data[[datetime.col]] - poptime))
          }
        }
      }
    }

    ############################################################################
    # Add temporal buffer for changepoint analysis #############################
    ############################################################################

    # if not valid custom deployment periods were supplied OR partial times need estimation
    if(!valid_deployment_times){

      # add 1 hour of 0-depth data before the first and after the last datetime
      # (named via the user-supplied id/datetime/depth columns for consistency)
      before_dt <- data.table::data.table(
        id, seq(min(reduced_data[[datetime.col]]) - 3600,
                to = min(reduced_data[[datetime.col]]) - 1, by = "1 sec"), 0)
      data.table::setnames(before_dt, c(id.col, datetime.col, depth.col))

      after_dt <- data.table::data.table(
        id, seq(max(reduced_data[[datetime.col]]) + 1,
                to = max(reduced_data[[datetime.col]]) + 3600, by = "1 sec"), 0)
      data.table::setnames(after_dt, c(id.col, datetime.col, depth.col))

      # merge buffers with the original dataset
      reduced_data <- data.table::rbindlist(list(before_dt, reduced_data, after_dt), fill = TRUE)

      # order by datetime column
      data.table::setorderv(reduced_data, datetime.col)


      ############################################################################
      # Filter out pre and post-deployment periods ###############################
      ############################################################################

      # run binary segmentation to detect change points in both mean and variance
      cp_depth <- suppressWarnings(changepoint::cpt.meanvar(reduced_data[[depth.col]], method="BinSeg", Q=max.changepoints, test.stat="Normal"))

      # extract changepoints
      changepoints <- changepoint::cpts(cp_depth)

      # add start and end indices
      changepoints <- c(1, changepoints, nrow(reduced_data))

      # calculate mean and variance for each segment
      segment_stats <- lapply(seq_along(changepoints[-1]), function(s) {
        start_idx <- changepoints[s]
        end_idx <- changepoints[s + 1] - 1
        segment <- reduced_data[[depth.col]][start_idx:end_idx]
        list(start = start_idx, end = end_idx, mean = mean(segment), variance = var(segment))
      })

      # convert to a data frame for easier manipulation
      segment_stats <- do.call(rbind, lapply(segment_stats, as.data.frame))

      # ---- classify segments (DISJOINT): a segment is "spurious" only if it is BOTH shallow AND
      # steady; everything else is "deployment". The previous OR/OR rule double-counted ambiguous
      # segments (e.g. a steady deep cruise is both "deep" and "low-variance"), which could clip a
      # genuine deployment or admit surface noise.
      spurious_segments   <- which(segment_stats$mean < depth.threshold & segment_stats$variance < variance.threshold)
      deployment_segments <- setdiff(seq_len(nrow(segment_stats)), spurious_segments)
      deployment_found    <- length(deployment_segments) > 0

      # depth-based candidate window (indices into reduced_data). The 1 h zero-depth padding guarantees
      # a spurious segment at each end, so a pre/post segment normally exists; fall back defensively to
      # the data extremes.
      if (deployment_found) {
        pre  <- spurious_segments[spurious_segments < min(deployment_segments)]
        post <- spurious_segments[spurious_segments > max(deployment_segments)]
        if (!(partial_deployment_times && custom_start_only)) {
          attach_idx <- if (length(pre))  segment_stats$end[max(pre)] + 1   else 1L
        }
        if (!(partial_deployment_times && custom_end_only)) {
          pop_idx    <- if (length(post)) segment_stats$start[min(post)] - 1 else nrow(reduced_data)
        }
      }

      # ---- temperature corroboration (fully-automated path only; strictly additive). It can rescue a
      # deployment that stayed too shallow for the depth criterion, or extend the window across shallow
      # in-water edges, but never shrinks the depth result. Gated on a clear, sustained out-of-water vs
      # in-water temperature difference, so a flat/uninformative trace is a no-op.
      depth_found <- deployment_found       # remember whether depth alone found a window
      if (use.temperature && !partial_deployment_times && temp.col %in% names(reduced_data)) {
        min_run <- max(1L, as.integer(round(min.deployment.hours * 3600)))
        tref <- .refineWithTemperature(reduced_data[[temp.col]],
                                       if (deployment_found) attach_idx else NA_integer_,
                                       if (deployment_found) pop_idx    else NA_integer_,
                                       deployment_found, min_run)
        if (!is.null(tref)) { attach_idx <- tref$attach; pop_idx <- tref$pop; deployment_found <- TRUE }
        temp_rescued <- !depth_found && deployment_found
      }

      # ---- minimum-duration guard (fully-automated windows only): reject transient depth spikes or
      # brief diver test-dives that are too short to be a real deployment.
      if (deployment_found && !partial_deployment_times) {
        dur_h <- as.numeric(difftime(reduced_data[[datetime.col]][pop_idx],
                                     reduced_data[[datetime.col]][attach_idx], units = "hours"))
        if (is.finite(dur_h) && dur_h < min.deployment.hours) deployment_found <- FALSE
      }

      if (!deployment_found) {
        valid_dataset <- FALSE
        # indices/stats for the (skipped) plotting + discard summary; the .log_skip line below reports it
        attach_idx <- nrow(reduced_data) + 1
        pop_idx <- nrow(reduced_data) + 1
        original_rows <- nrow(individual_data)
        kept_rows <- 0
        rows_discarded <- original_rows

      } else {
        # partial custom times are now complete (the estimated boundary was filled above)
        if (partial_deployment_times) valid_deployment_times <- TRUE
        # authoritative boundary times: keep the user's custom value on the side they supplied, use the
        # depth-derived index otherwise. A custom-side index was computed BEFORE the changepoint padding
        # was prepended, so re-derive BOTH indices from the final times (into the padded reduced_data).
        if (!isTRUE(custom_start_only)) attachtime <- reduced_data[[datetime.col]][attach_idx]
        if (!isTRUE(custom_end_only))   poptime    <- reduced_data[[datetime.col]][pop_idx]
        attach_idx <- which.min(abs(reduced_data[[datetime.col]] - attachtime))
        pop_idx    <- which.min(abs(reduced_data[[datetime.col]] - poptime))
      }
    }

    # if no valid deployment was detected, the dataset is discarded: report it, skip the plotting
    # and subsetting (attachtime/poptime are undefined here), and move on to the next individual
    if (!valid_dataset) {
      .log_skip(lvl, id, "  no deployment detected ", cli::symbol$bullet, " discarded")
      n_discarded <- n_discarded + 1L
      .log_gap(lvl)
      next
    }

    ##########################################################################
    # Deployment statistics (valid_dataset is guaranteed TRUE here) ###########

    deploy_duration <- difftime(poptime, attachtime, units = "hours")
    deploy_duration <- paste(sprintf("%.2f", as.numeric(deploy_duration)), attributes(deploy_duration)$units)
    deploy_percentage <- as.numeric(difftime(poptime, attachtime, units = "hours"))/as.numeric(difftime(last_datetime, first_datetime, units = "hours"))*100
    pre_deploy <- as.numeric(difftime(attachtime, first_datetime, units="hours"))
    pre_deploy <- sprintf("%dh:%02dm", floor(pre_deploy), round((pre_deploy - floor(pre_deploy)) * 60))
    post_deploy <- as.numeric(difftime(last_datetime, poptime, units="hours"))
    post_deploy <- sprintf("%dh:%02dm", floor(post_deploy), round((post_deploy - floor(post_deploy)) * 60))
    original_rows <- nrow(individual_data)
    kept_rows <- sum(individual_data[[datetime.col]] >= attachtime & individual_data[[datetime.col]] <= poptime)
    rows_discarded <- original_rows - kept_rows

    # realized detection method for this individual (drives the per-individual method line and the
    # custom/automatic split in the run summary)
    method_kind <- if (isTRUE(valid_deployment_times)) "custom" else "automatic"
    method_label <- if (method_kind == "custom") {
      if (isTRUE(custom_start_only)) "custom start + inferred end"
      else if (isTRUE(custom_end_only)) "inferred start + custom end"
      else "custom window"
    } else if (isTRUE(temp_rescued)) "automatic (temperature rescue)"
      else "automatic (depth changepoints)"

    # level-2 details, grouped under the sub-header, in reading order:
    # method -> window -> coverage -> trimming -> retained/removed (the save message follows below).
    .log_detail(lvl, "method: ", method_label)
    .log_detail(lvl, "window: ", strftime(attachtime, "%Y-%m-%d %H:%M", tz = "UTC"), " ",
                cli::symbol$arrow_right, " ", strftime(poptime, "%Y-%m-%d %H:%M", tz = "UTC"))
    .log_detail(lvl, "coverage: ", deploy_duration, " (", sprintf("%.0f%%", deploy_percentage), ")")
    .log_detail(lvl, "trimming: pre ", pre_deploy, " ", cli::symbol$bullet, " post ", post_deploy)
    .log_detail(lvl, "retained: ~", .formatLargeNumber(kept_rows), " rows | removed: ~",
                .formatLargeNumber(rows_discarded), " rows")

    ############################################################################
    # Plot results #############################################################
    ############################################################################

    if (make_plots) {

      # describe how this individual's window was determined (drives the status badge)
      method_kind <- if (isTRUE(valid_deployment_times)) "custom" else "automatic"
      method_label <- if (!valid_dataset) "no deployment detected"
        else if (method_kind == "automatic") { if (isTRUE(temp_rescued)) "temperature rescue" else "depth + temperature" }
        else if (isTRUE(custom_start_only)) "custom start, inferred end"
        else if (isTRUE(custom_end_only))   "inferred start, custom end"
        else "custom window"

      # per-boundary method: in a partial-custom window the two ends differ (e.g. custom start +
      # inferred end), so each boundary is coloured by its own origin in the panel.
      full_custom <- isTRUE(valid_deployment_times) && !isTRUE(partial_deployment_times)
      attach_method  <- if (full_custom || isTRUE(custom_start_only)) "custom" else "automatic"
      release_method <- if (full_custom || isTRUE(custom_end_only))   "custom" else "automatic"

      # metadata deploy/release times (scalar, for reference lines); read canonically from meta$deployment
      meta_deploy  <- (.getMeta(individual_data)$deployment$datetime)       %||% as.POSIXct(NA)
      meta_release <- (.getMeta(individual_data)$deployment$popup_datetime) %||% as.POSIXct(NA)

      # only show metric strips for metrics actually present in this dataset (keeps labels aligned);
      # an absent metric (e.g. `temp` on an electronics-temperature-only tag) simply has no strip.
      metric_keep <- plot.metrics %in% names(reduced_data)

      # assemble everything the panel needs, then render it to the PDF page and/or active device
      pd <- list(
        id = id, valid = valid_dataset, method_kind = method_kind, method_label = method_label,
        attach_method = attach_method, release_method = release_method,
        data = reduced_data, datetime.col = datetime.col, depth.col = depth.col,
        first_datetime = first_datetime, last_datetime = last_datetime,
        attach_idx = attach_idx, pop_idx = pop_idx,
        metrics = plot.metrics[metric_keep], metric.labels = plot.metrics.labels[metric_keep],
        positions_available = positions_available,
        meta_deploy = meta_deploy, meta_release = meta_release,
        fastloc_pos = if (positions_available) fastloc_pos else NULL,
        user_pos    = if (positions_available) user_pos    else NULL,
        attachtime  = if (valid_dataset) attachtime else as.POSIXct(NA),
        poptime     = if (valid_dataset) poptime    else as.POSIXct(NA),
        deploy_duration   = if (valid_dataset) deploy_duration   else NA_character_,
        deploy_percentage = if (valid_dataset) deploy_percentage else NA_real_,
        kept_rows = kept_rows, rows_discarded = rows_discarded)

      # suppress device font/encoding warnings (text.default / mbcsToSbcs substituting glyphs like
      # the arrow on non-UTF devices); they are not meaningful and would break the per-individual blocks
      if (!is.null(file_dev)) { grDevices::dev.set(file_dev); suppressWarnings(.drawDeploymentPanel(pd)) }
      if (plot) { if (caller_dev > 1L) grDevices::dev.set(caller_dev); suppressWarnings(.drawDeploymentPanel(pd)) }
    }

    ############################################################################
    # Save updated data ########################################################
    ############################################################################

    # subset the data between the specified attachtime and poptime
    individual_data <- individual_data[individual_data[[datetime.col]] >= attachtime & individual_data[[datetime.col]] <= poptime, ]

    # reapply the original attributes to the processed data
    for (attr_name in names(original_attributes)) {
      attr(individual_data, attr_name) <- original_attributes[[attr_name]]
    }

    # record this step in the metadata audit trail and re-class as nautilus_tag
    meta <- .getMeta(individual_data)
    if (!is.null(meta)) {
      meta$span$first_datetime <- attachtime
      meta$span$last_datetime  <- poptime
      meta <- .appendProcessing(meta, "filterDeploymentData",
                                attach_time = attachtime, popup_time = poptime)
      individual_data <- .restoreMeta(individual_data, meta)
    }

    # persist the filtered data (path-as-switch: writes only when output.dir is set)
    saved_to <- if (valid_dataset) .saveOutput(individual_data, id, output.dir = output.dir,
                                               output.suffix = output.suffix, compress = compress) else NULL
    saved[i] <- list(saved_to)

    # closing line. At the detailed level the window/retained details are already shown above, so the
    # closing line just reports the save outcome; at the normal level it carries the one-line summary.
    if (lvl >= 2L) {
      .log_ok(lvl, if (!is.null(saved_to)) paste0("saved ", basename(saved_to)) else "filtered")
    } else {
      .log_ok(lvl, id, "  kept ", deploy_duration, " (", sprintf("%.0f", deploy_percentage), "%) ",
              cli::symbol$bullet, " ", strftime(attachtime, "%H:%M %d-%b", tz = "UTC"),
              " ", cli::symbol$arrow_right, " ", strftime(poptime, "%H:%M %d-%b", tz = "UTC"),
              if (!is.null(saved_to)) paste0(" ", cli::symbol$bullet, " saved ", basename(saved_to)))
    }
    n_filtered <- n_filtered + 1L
    # tally the realized method (valid_deployment_times is TRUE only when a custom window was actually
    # used; an out-of-range custom time that fell back to detection counts as automatic)
    if (isTRUE(valid_deployment_times)) n_custom <- n_custom + 1L else n_auto <- n_auto + 1L
    .log_gap(lvl)                          # blank line separates this individual's block from the next

    # store filtered sensor data in the results list if needed
    if (return.data && valid_dataset) {
      processed_data[[i]] <- individual_data
      names(processed_data)[i] <- id
    }

    # drop the reference to the processed data before the next iteration
    # (R's garbage collector reclaims it automatically; an explicit gc() every
    # iteration would only slow the loop down)
    rm(individual_data)
  }


  ##############################################################################
  # Return results #############################################################
  ##############################################################################

  # final run summary: its own block (its own rule), conceptually distinct from per-individual output,
  # with the information split across lines for scannability.
  if (lvl >= 1L) {
    .log_summary(lvl)
    # realized custom/automatic split shown only when custom windows were supplied
    split_note <- if (!is.null(custom.deployment.times) && n_filtered > 0)
      paste0(" (", n_custom, " custom, ", n_auto, " automatic)") else ""
    .log_done(lvl, n_filtered, " dataset", if (n_filtered != 1) "s", " filtered", split_note)
    if (n_discarded > 0) cli::cli_text("{cli::symbol$bullet} {n_discarded} dataset{?s} discarded")
    if (!is.null(output.dir)) .log_arrow(lvl, "output: ", output.dir)
    if (!is.null(plot.file)) .log_arrow(lvl, "plots: ", plot.file)
    .log_runtime(lvl, start.time)
  }


  # unified return contract, matching the rest of the pipeline (no bespoke `filtered_data` wrapper):
  # return.data = TRUE -> the retained tag objects as a named list keyed by ID (discarded/empty individuals
  # dropped); return.data = FALSE -> the written `.rds` paths, invisibly, which chain into the next step's
  # `data` argument. Both feed downstream without unwrapping.
  if (return.data) {
    return(processed_data[!vapply(processed_data, is.null, logical(1))])
  }
  invisible(unlist(saved, use.names = FALSE))

}


#######################################################################################################
# Internal: temperature corroboration #################################################################
#######################################################################################################

# Strictly-additive temperature refinement of a depth-based deployment window. `temp` is the 1 Hz
# temperature vector (NA in the padded ends); `attach`/`pop` are the depth-based window indices (NA when
# depth found nothing); `found` says whether depth found a deployment; `min_run` is the minimum sustained
# in-water length (samples) required to trust temperature. Returns a list(attach, pop, valid) or NULL
# (leave the depth result unchanged). It can only widen or rescue a window, never shrink one.
#' @keywords internal
#' @noRd
.refineWithTemperature <- function(temp, attach, pop, found, min_run,
                                   base_win = 300L, tol_floor = 1.0, k = 4) {
  n <- length(temp)
  if (n < 3L) return(NULL)

  # out-of-water reference: when depth gave a window, use the regions outside it; otherwise the first
  # and last `base_win` FINITE samples (assumed to bracket the boat / float-and-recovery phases). Using
  # finite samples skips the NA-filled padding at the series ends.
  if (found && is.finite(attach) && is.finite(pop) && attach > 1L && pop < n) {
    out_idx <- c(seq_len(attach - 1L), (pop + 1L):n)
    base <- temp[out_idx]; base <- base[is.finite(base)]
  } else {
    fin <- which(is.finite(temp))
    if (length(fin) < 10L) return(NULL)
    w <- min(base_win, length(fin) %/% 3L)
    base <- temp[c(utils::head(fin, w), utils::tail(fin, w))]
  }
  if (length(base) < 5L) return(NULL)

  # a sample is "in water" if its temperature departs from the out-of-water baseline by more than a
  # robust tolerance (at least `tol_floor` degrees, or k robust SDs of the baseline)
  T0 <- stats::median(base); scale <- stats::mad(base)
  tol <- max(tol_floor, k * scale)
  in_water <- is.finite(temp) & abs(temp - T0) >= tol
  if (!any(in_water)) return(NULL)

  # require a sustained in-water run before trusting temperature at all
  run <- .longestTrueRun(in_water)
  if (is.null(run) || (run[2] - run[1] + 1L) < min_run) return(NULL)

  if (found) {
    # extend the depth window outward across contiguous in-water samples (shallow in-water edges)
    a <- attach; p <- pop
    while (a > 1L && isTRUE(in_water[a - 1L])) a <- a - 1L
    while (p < n  && isTRUE(in_water[p + 1L])) p <- p + 1L
    list(attach = a, pop = p, valid = TRUE)
  } else {
    # rescue: depth found nothing, but temperature shows a sustained in-water period
    list(attach = run[1], pop = run[2], valid = TRUE)
  }
}

# Start/end indices of the longest contiguous run of TRUE in a logical vector (NA treated as FALSE);
# NULL if there are none.
#' @keywords internal
#' @noRd
.longestTrueRun <- function(x) {
  x[is.na(x)] <- FALSE
  if (!any(x)) return(NULL)
  r <- rle(x)
  ends <- cumsum(r$lengths); starts <- ends - r$lengths + 1L
  tr <- which(r$values)
  best <- tr[which.max(r$lengths[tr])]
  c(starts[best], ends[best])
}


#######################################################################################################
# Internal: metric axis labels ########################################################################
#######################################################################################################

# Human-readable "Name (unit)" labels for the diagnostic-plot metrics, generated from a lookup of the
# standard nautilus channels (units as produced by processTagData). Unknown / user-derived columns
# fall back to the raw column name. Vectorised over `col`.
#' @keywords internal
#' @noRd
.metricLabel <- function(col) {
  map <- c(
    ax = "Acc X (g)", ay = "Acc Y (g)", az = "Acc Z (g)",
    gx = "Gyro X (rad/s)", gy = "Gyro Y (rad/s)", gz = "Gyro Z (rad/s)",
    mx = "Mag X (\u00b5T)", my = "Mag Y (\u00b5T)", mz = "Mag Z (\u00b5T)",
    depth = "Depth (m)", temp = "Temperature (\u00b0C)",
    pitch = "Pitch (\u00b0)", roll = "Roll (\u00b0)", yaw = "Yaw (\u00b0)", heading = "Heading (\u00b0)",
    vedba = "VeDBA (g)", odba = "ODBA (g)", odba_smooth = "ODBA (g)", vedba_smooth = "VeDBA (g)",
    speed = "Speed (m/s)", paddle_speed = "Paddle speed (m/s)", paddle_freq = "Paddle freq (Hz)")
  lab <- unname(map[tolower(col)])
  ifelse(is.na(lab), col, lab)
}


#######################################################################################################
# Internal: deployment-filtering diagnostic panel #####################################################
#######################################################################################################

# One page per individual, designed for fast visual validation of the inferred deployment window:
#   * a title row with the ID and a colour-coded method/outcome BADGE (automatic / custom / discarded)
#     plus a two-line inline stat block (attached / released / duration / retained) with full datetimes;
#   * an inverted depth panel where the retained window is the figure (coloured) and the discarded ends
#     recede (grey); the retained boundaries (solid red) and the metadata deploy/release times (dashed
#     green) are marked and explained by a legend; optional Wildlife Computers fixes sit along the top;
#   * the detection-relevant sensor strips (temperature first by default), sharing a dated x-axis that
#     includes the YEAR; and
#   * a footer caption stating the data span and the metadata deploy/release times.
# The x-axis is clipped to the real data span (the 1 h zero-depth changepoint padding is excluded).
# `pd` is the list assembled in filterDeploymentData().

#' @keywords internal
#' @noRd
.drawDeploymentPanel <- function(pd) {

  d  <- pd$data
  tt <- d[[pd$datetime.col]]
  n  <- nrow(d)
  ai <- pd$attach_idx; pp <- pd$pop_idx
  has_window <- isTRUE(pd$valid) && is.finite(ai) && is.finite(pp) && ai <= n && pp <= n && ai < pp
  kept <- if (has_window) c(tt[ai], tt[pp]) else NULL
  xlim <- c(pd$first_datetime, pd$last_datetime)   # real data span (excludes internal padding)

  # palette
  col_kept <- "#185FA5"; col_disc <- "#9AA0A6"; band <- "#378ADD22"; col_meta <- "#1D9E75"
  col_auto <- "#A32D2D"; col_custom <- "#E8A33D"          # retained boundary, coloured by method
  col_fast <- "#2AA7A0"; col_user <- "#7E57C2"            # location-fix tones (distinct from depth blue)
  badge_col <- if (!has_window) "#B23A3A" else if (identical(pd$method_kind, "custom")) "#E8A33D" else "#1D9E75"
  attach_col  <- if (identical(pd$attach_method,  "custom")) col_custom else col_auto
  release_col <- if (identical(pd$release_method, "custom")) col_custom else col_auto

  fmt_dt <- function(x) if (length(x) && is.finite(x)) strftime(x, "%Y-%m-%d %H:%M", tz = "UTC") else "NA"

  nmet <- length(pd$metrics)
  has_fast <- isTRUE(pd$positions_available) && !is.null(pd$fastloc_pos) && nrow(pd$fastloc_pos) > 0
  has_user <- isTRUE(pd$positions_available) && !is.null(pd$user_pos)    && nrow(pd$user_pos)    > 0
  have_pos <- has_fast || has_user

  # layout: header, [location strip], depth, metric strips, footer
  heights <- c(1.15, if (have_pos) 0.26, 2.2, rep(1.0, nmet), 0.34)
  graphics::layout(matrix(seq_along(heights), ncol = 1), heights = heights)

  # ---- title + status badge + inline stats (labels same size as values; rows kept close with clear
  #      space below, and the attach/release labels live inside the depth panel, never up here) -------
  graphics::par(mar = c(0.2, 4.4, 0.4, 1.2))
  graphics::plot(0:1, 0:1, type = "n", axes = FALSE, xlab = "", ylab = "", xaxs = "i", yaxs = "i")
  graphics::text(0, 0.86, pd$id, adj = c(0, 0.5), font = 2, cex = 1.7)
  btxt <- if (!has_window) paste0("DISCARDED  \u00b7  ", pd$method_label)
          else paste0(toupper(pd$method_kind), "  \u00b7  ", pd$method_label)
  bx0 <- graphics::strwidth(pd$id, cex = 1.7) + graphics::strwidth("  ", cex = 1.7)
  bw  <- graphics::strwidth(btxt, cex = 0.92) + graphics::strwidth("MM", cex = 0.92)
  graphics::rect(bx0, 0.72, bx0 + bw, 1.0, col = badge_col, border = NA)
  graphics::text(bx0 + bw / 2, 0.86, btxt, col = "white", font = 2, cex = 0.92, adj = c(0.5, 0.5))

  draw_pairs <- function(prs, ly) {
    x <- 0; cx <- 1.0                               # label and value share the same size
    for (k in seq_along(prs)) {
      lab <- prs[[k]][1]; val <- prs[[k]][2]
      graphics::text(x, ly, lab, adj = c(0, 0.5), cex = cx, col = col_disc); x <- x + graphics::strwidth(lab, cex = cx) + graphics::strwidth(" ", cex = cx)
      graphics::text(x, ly, val, adj = c(0, 0.5), cex = cx, font = 2);        x <- x + graphics::strwidth(val, cex = cx)
      if (k < length(prs)) { sep <- "      \u00b7     "; graphics::text(x, ly, sep, adj = c(0, 0.5), cex = cx, col = col_disc); x <- x + graphics::strwidth(sep, cex = cx) }
    }
  }
  if (has_window) {
    draw_pairs(list(c("Attached", fmt_dt(pd$attachtime)), c("Released", fmt_dt(pd$poptime))), 0.52)
    draw_pairs(list(c("Duration", pd$deploy_duration),
                    c("Retained", sprintf("%.0f%% (%s)", pd$deploy_percentage, .formatLargeNumber(pd$kept_rows)))), 0.30)
  } else {
    draw_pairs(list(c("Status", "no deployment detected"),
                    c("Discarded", paste0(.formatLargeNumber(pd$rows_discarded), " rows"))), 0.40)
  }

  # helper: draw a series with the retained window shaded, kept (colour) over discarded (grey), plus
  # the metadata (dashed green) reference lines and the retained boundaries coloured by their method.
  panel <- function(y, ylab, ylim = NULL, lwd = 1.0, draw_axis = FALSE) {
    if (is.null(ylim)) ylim <- range(y, na.rm = TRUE)
    graphics::plot(tt, y, type = "n", xlim = xlim, ylim = ylim, xaxt = "n", xlab = "", ylab = ylab,
                   las = 1, cex.axis = 0.95, cex.lab = 1.05)
    if (has_window) graphics::rect(kept[1], graphics::par("usr")[3], kept[2], graphics::par("usr")[4], col = band, border = NA)
    graphics::lines(tt, y, col = col_disc, lwd = lwd * 0.8)
    if (has_window) {
      keep <- tt >= kept[1] & tt <= kept[2]
      graphics::lines(tt[keep], y[keep], col = col_kept, lwd = lwd)
    }
    if (is.finite(pd$meta_deploy))  graphics::abline(v = pd$meta_deploy,  col = col_meta, lty = 2, lwd = 1.0)
    if (is.finite(pd$meta_release)) graphics::abline(v = pd$meta_release, col = col_meta, lty = 2, lwd = 1.0)
    if (has_window) {
      graphics::abline(v = kept[1], col = attach_col,  lty = 1, lwd = 1.4)
      graphics::abline(v = kept[2], col = release_col, lty = 1, lwd = 1.4)
    }
    graphics::box(col = "#CCCCCC")
    if (draw_axis) {
      ticks <- pretty(xlim, n = 6)
      graphics::axis(1, at = ticks, labels = format(ticks, "%d %b %Y\n%H:%M"), cex.axis = 0.82, padj = 0.5)
    }
  }

  # ---- location strip (narrow band above depth; FastGPS / User distinguished by colour) -------
  if (have_pos) {
    graphics::par(mar = c(0.15, 4.4, 0.15, 1.2))
    graphics::plot(xlim, c(0, 1), type = "n", xlim = xlim, ylim = c(0, 1), axes = FALSE, xlab = "", ylab = "")
    if (has_window) graphics::rect(kept[1], 0, kept[2], 1, col = band, border = NA)
    if (has_fast) graphics::points(pd$fastloc_pos[[pd$datetime.col]], rep(0.5, nrow(pd$fastloc_pos)), pch = 16, col = paste0(col_fast, "CC"), cex = 1.04)
    if (has_user) graphics::points(pd$user_pos[[pd$datetime.col]],    rep(0.5, nrow(pd$user_pos)),    pch = 16, col = paste0(col_user, "CC"), cex = 1.04)
    graphics::box(col = "#CCCCCC")
    graphics::mtext("fixes", side = 2, las = 1, line = 0.4, cex = 0.7, col = col_disc)
  }

  # ---- depth panel (inverted) -----------------------------------------------------------------
  graphics::par(mar = c(0.5, 4.4, 0.6, 1.2), mgp = c(2.7, 0.7, 0))
  dmax <- max(d[[pd$depth.col]], na.rm = TRUE)
  dmin <- min(-2, min(d[[pd$depth.col]], na.rm = TRUE))
  headroom <- 0.10 * (dmax - dmin)              # dedicated space above the trace for the boundary labels
  ytop <- dmin - headroom
  panel(d[[pd$depth.col]], "Depth (m)", ylim = c(dmax, ytop), lwd = 1.1)
  if (has_window) {
    y_lab <- ytop + 0.5 * headroom              # centred in the headroom band, always above the depth trace
    graphics::text(kept[1], y_lab, "attach",  col = attach_col,  cex = 0.92, font = 2, pos = 4, offset = 0.2)
    graphics::text(kept[2], y_lab, "release", col = release_col, cex = 0.92, font = 2, pos = 2, offset = 0.2)
  }

  # legend: boundaries by method, metadata reference, location tones (filled circles)
  lab <- character(0); lty <- integer(0); lcol <- character(0); lpch <- integer(0)
  add <- function(l, ty, co, pc = NA_integer_) { lab <<- c(lab, l); lty <<- c(lty, ty); lcol <<- c(lcol, co); lpch <<- c(lpch, pc) }
  if (has_window) {
    if (attach_col == col_auto   || release_col == col_auto)   add("automatic boundary", 1L, col_auto)
    if (attach_col == col_custom || release_col == col_custom) add("custom boundary",    1L, col_custom)
  }
  if (is.finite(pd$meta_deploy) || is.finite(pd$meta_release)) add("metadata deploy/release", 2L, col_meta)
  if (has_fast) add("FastGPS fix",   NA_integer_, col_fast, 16L)
  if (has_user) add("User position", NA_integer_, col_user, 16L)
  if (length(lab)) graphics::legend("bottomright", legend = lab, lty = lty, col = lcol, pch = lpch,
                                    bty = "o", bg = "#FFFFFFCC", box.col = "#CCCCCC", cex = 0.8, seg.len = 1.6, inset = 0.01)

  # ---- metric strips --------------------------------------------------------------------------
  for (v in seq_along(pd$metrics)) {
    last <- v == nmet
    graphics::par(mar = c(if (last) 2.9 else 0.5, 4.4, 0.6, 1.2))
    if (pd$metrics[v] %in% names(d)) panel(d[[pd$metrics[v]]], pd$metric.labels[v], lwd = 0.9, draw_axis = last)
    else { graphics::plot.new(); graphics::box(col = "#CCCCCC") }
  }

  # ---- footer caption (rest of the deployment timeline; data span and metadata clearly separated) ---
  graphics::par(mar = c(0.2, 4.4, 0.2, 1.2))
  graphics::plot(0:1, 0:1, type = "n", axes = FALSE, xlab = "", ylab = "")
  graphics::text(0, 0.5, adj = c(0, 0.5), cex = 0.82, col = "#555555",
                 labels = paste0("Data span  ", fmt_dt(pd$first_datetime), " -> ", fmt_dt(pd$last_datetime),
                                 "          |          Metadata deploy  ", fmt_dt(pd$meta_deploy),
                                 "   |   Metadata release  ", fmt_dt(pd$meta_release)))
  invisible(NULL)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
