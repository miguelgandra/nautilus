#######################################################################################################
# Function to import and standardize archival tag data ################################################
#######################################################################################################

#' Import and Standardize Archival Biologging Tag Data
#'
#' @description
#' This function provides an end-to-end solution for importing and standardizing
#' high-resolution biologging data from archival tags. It handles raw sensor outputs from
#' various tag types (defaulting to G-Pilot and i-Pilot formats) and transforms them into
#' analysis-ready datasets with consistent formatting.
#'
#' It can process data from a single individual or automatically iterate through directories
#' to process data from multiple individuals sequentially, making it well-suited for
#' large-scale studies.
#'
#' Sensor time series are automatically processed and converted to standardized units
#' — `g` for acceleration, `rad/s` for angular velocity, and `µT` for magnetic fields —
#' while preserving the original temporal resolution and structure.
#'
#' Users can optionally specify a custom column mapping using the `import.mapping` argument,
#' which allows importing data from CSV files with non-standard column names. When provided,
#' this mapping defines how columns in the raw data correspond to standard sensor types,
#' including the associated units. This ensures compatibility with the function's
#' standardization routines, even when input data deviates from default naming conventions.
#'
#' Sensor data is imported in its raw axis frame; converting the IMU to an animal-centric frame such as
#' NED (North-East-Down) is a separate, explicit step afterwards via \code{\link{applyAxisMapping}}
#' (optionally validated against the data with \code{\link{checkTagMapping}}).
#'
#' When available, location data from Wildlife Computers tags (e.g., MiniPAT, MK10, SPOT)
#' can be integrated automatically. These should be stored in dedicated subfolders
#' (e.g., `"SPOT"`) within each individual's directory. Deployment and pop-up locations,
#' if present in the metadata, are also extracted and included in the output.
#'
#' The function employs memory-efficient processing to handle large datasets (>10 million rows),
#' using optimized `data.table` operations with configurable multi-threading.

#' @note For optimal performance with very large datasets, consider:
#' \itemize{
#'   \item Setting \code{data.table.threads} to match available CPU cores;
#'   \item Setting \code{return.data = FALSE} with an \code{output.dir} to stream results to disk;
#'   \item Processing individuals sequentially rather than simultaneously.
#' }
#'
#'
#' @param data.folders Character vector. Paths to the folders containing data to be processed.
#' Each folder corresponds to an individual animal and should contain subdirectories with sensor data and possibly
#' additional (Wildlife Computers) tag data.
#' @param sensor.subdirectory Character. Name of the subdirectory within each animal folder that contains sensor data (default: "CMD").
#' @param format Character. The raw data format to read: `"cats"` (default; the CATS/CEiiA multi-sensor
#'   CSV export) or `"little_leonardo"` (Little Leonardo archival loggers - a `_A.txt` acceleration file
#'   plus an optional `_DT.txt` depth/temperature file, which carry no clock and so additionally need the
#'   `data_start` role, see \code{\link{metadataColumns}}). To import several tag makes in ONE call, map
#'   the `tag_format` role instead: a per-deployment value there overrides this argument. Data you have
#'   already read into R does not need a reader at all - see \code{\link{buildTagData}}.
#' This subdirectory should include the sensor CSV files for the corresponding animal.
#' @param wc.subdirectory Character or NULL. Name of the subdirectory within each animal folder that contains Wildlife Computers tag data
#' (e.g., MiniPAT, MK10, or SPOT tag data), or NULL to auto-detect tag folders (default: NULL).
#' This subdirectory should contain the "Locations.csv" file with position data from the tag.
#' @param timezone Character. The time zone in which the tag **recorded** its timestamps. It is used
#'   to *interpret* the (time-zone-naive) clock readings in the sensor files: the recorded clock
#'   values are preserved and the resulting `datetime` column is simply labelled with this zone -
#'   **no shifting or conversion is performed**. Must be a valid \code{OlsonNames()} zone. Defaults
#'   to "UTC", which matches standard CATS exports (whose columns are labelled "Date (UTC)" /
#'   "Time (UTC)"); set it only if your tag was configured to log in local time. If a CATS calibration
#'   sidecar reporting a non-zero `utc_offset` is found and it disagrees with this argument, a warning
#'   is issued (the clock is never silently shifted). Wildlife Computers location files are always read
#'   as UTC (their native zone), independent of this argument.
#' @param import.mapping Data frame or NULL. Optional specification of column names to import
#' from the CSV files and their corresponding sensor type and units. If `NULL` (default), uses
#' standard column names for archival G-Pilot and i-Pilot data. When specified, it must be a data frame
#' with three columns:
#' \itemize{
#'    \item \strong{colname}: The exact column name as it appears in the input CSV file.
#'    \item \strong{sensor}: The standardized sensor name to be used in the processed data. Valid options include: \code{"date"}, \code{"time"}, \code{"datetime"}, \code{"ax"}, \code{"ay"}, \code{"az"}, \code{"gx"}, \code{"gy"}, \code{"gz"}, \code{"mx"}, \code{"my"}, \code{"mz"}, \code{"depth"}, \code{"temp"}, \code{"paddle_freq"}, \code{"paddle_speed"}.
#'    \item \strong{units}: The units of the sensor data. Valid options include: \code{"UTC"}, \code{"m/s2"}, \code{"g"}, \code{"mrad/s"}, \code{"rad/s"}, \code{"deg/s"}, \code{"uT"}, \code{"C"}, \code{"m"}, \code{"Hz"}, \code{"m/s"}, or \code{""} (an empty string) for unitless/dimensionless quantities.
#' }
#' For date and time columns, use `sensor = "datetime"` and `units = "UTC"`.
#'
#' \strong{Temperature handling.} Only water-temperature channels are mapped to \code{temp}: the
#' pressure-sensor thermistor (\code{"Temperature (depth)"} / \code{"Depth (200bar) 2"}) and the CEIIA
#' \code{"Temperature (deg C)"}. Tag-electronics channels (\code{"Temp. (magnet.)"}, the magnetometer
#' chip, and \code{"Temperature (imu)"}, the IMU board) are blacklisted and never auto-mapped, because
#' they report internal temperature, not the environment. If a deployment's only temperature column is
#' a blacklisted one, \code{temp} is left unset (consistent with how any other missing sensor is
#' handled) and a per-deployment warning is issued. Explicitly mapping a blacklisted header via
#' \code{import.mapping} overrides the blacklist (it is then used as \code{temp}) but still warns.
#' @param required.sensors Character vector or `NULL`. The sensor channels that must be
#'   present for a file to be imported. If `NULL` (default), a file is imported as long as
#'   it has a usable datetime and at least one recognized sensor channel — so
#'   accelerometer-only, gyroscope-free, magnetometer-free, or depth/temperature-only
#'   (TDR-style) datasets are all supported. Whichever recognized channels are present are
#'   kept; the rest are simply absent from the output. To enforce a specific set (e.g.
#'   require depth for dive analysis, or a full IMU), pass their names, e.g.
#'   `c("depth", "temp")` or `c("ax","ay","az","gx","gy","gz","mx","my","mz")`. Valid names:
#'   `ax`, `ay`, `az`, `gx`, `gy`, `gz`, `mx`, `my`, `mz`, `depth`, `temp`, `paddle_speed`,
#'   `paddle_freq`. Note that downstream analyses may still require particular sensors.
#' @param id.metadata Data frame. Metadata about the IDs to associate with the processed data.
#' Must contain at least columns for ID, tag model, and deployment date/longitude/latitude (see `columns`).
#' Preferably a \code{nautilus_deployments} object returned by \code{\link{qcDeploymentMetadata}}: it
#' carries its own column schema (so `columns` is not needed) and a QC verdict that is reported in the
#' header and rejected if it failed. If a plain data.frame is supplied instead, the same metadata checks
#' are run inline as a guard, and blocking errors abort the import before the long read.
#' @param columns A column-mapping schema built with \code{\link{metadataColumns}}, describing which
#'   columns of `id.metadata` hold each piece of deployment information (ID, tag model/type,
#'   deployment and pop-up datetime/longitude/latitude, package ID, paddle wheel, attachment site).
#'   Fields default to the canonical nautilus names, so metadata that already uses those names needs
#'   no configuration; override only the columns that differ, e.g.
#'   `columns = metadataColumns(deploy_datetime = "tagging_date", package_id = "PackageID")`.
#' @param return.data Logical. Return the imported data in memory (default `TRUE`). When `FALSE`, the
#'   function instead returns the paths of the `.rds` files it wrote, which feed directly into the next
#'   step's `data` argument -- so a large fleet can be imported without ever holding it all in memory.
#'   `return.data = FALSE` therefore requires an `output.dir`.
#' @param output.dir Character. Directory in which to write one `<id>.rds` file per deployment. Providing
#'   a directory is what triggers saving; `NULL` (default) writes nothing. The directory must already exist.
#' @param output.suffix Character. Optional suffix appended to each saved file name (before `.rds`), e.g.
#'   to tag a processing run or avoid clashes. Only used when `output.dir` is set. Default `NULL`.
#' @param compress Compression for the saved `.rds` files (only used when `output.dir` is set).
#' `TRUE` (default, gzip) produces smaller files. Set `FALSE` to write uncompressed, which is
#' markedly faster for very large tags - gzip compression can otherwise dominate the import time -
#' at the cost of larger files (typically ~1.5-2x). May also be one of `"gzip"`, `"bzip2"`, `"xz"`
#' to choose the codec explicitly. The file content is identical either way; `readRDS()` reads both
#' transparently. Passed to \code{\link[base]{saveRDS}}.
#' @param data.table.threads Integer or NULL. Specifies the number of threads
#' that data.table should use for parallelized operations. NULL (default): Uses data.table's current default threading.
#' Notes:
#' \itemize{
#'    \item Optimal thread count depends on your CPU cores and data size
#'    \item More threads use more RAM but can significantly speed up large operations
#'    \item Can be permanently set via \code{data.table::setDTthreads()}
#'    \item Current thread count: \code{data.table::getDTthreads()}
#' }
#' @param import.calibration Logical. If `TRUE` (default), the function looks for a
#'   calibration sidecar paired with each sensor CSV (a CATS mini-diary `.txt`, or a
#'   camera `_resume.json`) and, when found, attaches the parsed calibration constants
#'   (per-sensor offsets/factors, magnetometer ASA coefficients, depth zero-offset) to
#'   the output as a `"calibration"` attribute. The values are stored for provenance
#'   and downstream use but are **not** applied to the sensor data at import time.
#'   If no sidecar is found, the attribute is `NULL`.
#' @param alignment A control object from \code{\link{alignmentControl}} governing the temporal alignment
#'   between a primary archival tag (depth + IMU) and a paired Wildlife Computers tag (wet/dry + GPS),
#'   which keep independent clocks. When both record depth, the offset between their clocks is recovered
#'   by cross-correlating the two depth traces and the WC-clock streams (`positions`, `dry`) are shifted
#'   onto the primary tag's timeline, so downstream steps that combine them (the depth zero-offset
#'   correction, dead-reckoning fix anchors) see one consistent clock. The correction abstains when the
#'   evidence is too weak (no shared depth, too little overlap, a poor correlation) and is recorded in
#'   `meta$ancillary$alignment`. Pass `alignmentControl(method = "none")` to disable it.
#' @param verbose Verbosity level: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal" (framed header, one line
#'   per folder, and a final `SUMMARY` block with the run count, runtime and consolidated issues), or
#'   `2`/"detailed" (default; adds per-individual diagnostics: tag attributes, sensor channels, row
#'   count and sampling rate, span, calibration sidecar, unit conversions, integrated locations and any
#'   excluded sensors). Defaults to `"detailed"`.
#'
#'   \strong{Issue reporting is verbosity-gated.} Data-quality issues (folders with no sensor file,
#'   locations outside the recording period, timezone mismatches, discarded
#'   electronics temperature sensors, ...) are surfaced through the console UI when `verbose >= 1`
#'   (pre-flight ones as amber alerts in the header, per-deployment ones inline, all of them echoed in
#'   the final `SUMMARY` tally) and \strong{no base-R \code{warning()} is emitted}. Only when
#'   `verbose = 0`/quiet are these issues emitted as aggregated R \code{warning()} conditions (once
#'   each, at the very end). Automated / headless callers that catch issues with
#'   \code{tryCatch()} / \code{withCallingHandlers()} must therefore run with `verbose = 0`; at the
#'   default `"detailed"` the cli console is the channel and no warning conditions are raised.
#'
#' @return If \code{return.data = TRUE}, a named list where each element contains the imported sensor
#' data for one folder. If \code{return.data = FALSE}, a character vector of the written `.rds` file
#' paths. Data is written to disk whenever \code{output.dir} is set.
#'
#' @seealso \link{processTagData}, \link{filterDeploymentData}
#' @examples
#' \dontrun{
#' # QC the deployment metadata first, then feed it straight to the import:
#' meta <- qcDeploymentMetadata(raw_metadata, columns = metadataColumns())
#' imported <- importTagData(
#'   data.folders        = list.dirs("./tags", recursive = FALSE),
#'   sensor.subdirectory = "CMD",
#'   id.metadata         = meta,
#'   timezone            = "UTC")
#' }
#' @export


importTagData <- function(data.folders,
                          sensor.subdirectory = "CMD",
                          format = "cats",
                          wc.subdirectory = NULL,
                          timezone = "UTC",
                          import.mapping = NULL,
                          required.sensors = NULL,
                          id.metadata,
                          columns = metadataColumns(),
                          return.data = TRUE,
                          output.dir = NULL,
                          output.suffix = NULL,
                          compress = TRUE,
                          data.table.threads = NULL,
                          import.calibration = TRUE,
                          alignment = alignmentControl(),
                          verbose = "detailed") {

  # A QC'd metadata table (a nautilus_deployments from qcDeploymentMetadata) is authoritative about its
  # own column names and carries a QC verdict: adopt its embedded schema and keep the stamp (reported in
  # the header; a FAILED verdict is rejected before the long read). A plain data.frame is screened
  # inline further below by the same check engine, as a guard.
  qc_stamp <- NULL
  if (is_nautilus_deployments(id.metadata)) {
    qc_stamp <- attr(id.metadata, "nautilus.qc")
    columns  <- attr(id.metadata, "nautilus.columns") %||% columns
    class(id.metadata) <- "data.frame"
  }

  # resolve the metadata column schema, then unpack into the local names used below
  columns <- .as_metadata_columns(columns)
  id.col              <- columns$id
  tag.model.col       <- columns$tag_model
  tag.type.col        <- columns$tag_type
  deploy.date.col     <- columns$deploy_datetime
  deploy.lon.col      <- columns$deploy_lon
  deploy.lat.col      <- columns$deploy_lat
  pop.date.col        <- columns$popup_datetime
  pop.lon.col         <- columns$popup_lon
  pop.lat.col         <- columns$popup_lat
  package.id.col      <- columns$package_id
  logger.id.col       <- columns$logger_id
  exclude.sensors.col <- columns$exclude_sensors
  axis.config.col     <- columns$axis_config
  paddle.wheel.col    <- columns$paddle_wheel
  attachment.site.col <- columns$attachment_site
  deployment.type.col <- columns$deployment_type
  tag.format.col      <- columns$tag_format                    # per-deployment reader (overrides `format`)
  data.start.col      <- columns$data_start                    # recording start for clock-less loggers
  traits.cols         <- columns$traits                        # passive attribute columns carried to meta$biometrics


  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

  # measure running time
  start.time <- Sys.time()

  # verbosity level (0 quiet / 1 normal / 2 detailed); also validates `verbose`
  lvl <- .verbosity(verbose)

  # resolve the cross-device clock-alignment control (accepts an alignmentControl() object or a named
  # list of its fields); validates its fields up front, before any deployment is read
  alignment <- .as_control(alignment, alignmentControl, "nautilus_alignment", "alignment")

  # Issue-reporting architecture (verbosity-gated, no double-printing):
  #   * verbose >= 1 : issues are shown purely through the cli UI - pre-flight ones as amber alerts in
  #                    the header, per-deployment ones inline, and ALL of them echoed in the final
  #                    SUMMARY tally. No base-R warning() is fired.
  #   * verbose == 0 : the cli UI is silent; instead every collected issue is emitted once, aggregated,
  #                    as a base-R warning() at the very end (via on.exit, so they survive an error
  #                    mid-run) - this is the channel headless / automated callers can catch.
  # Collectors are initialised up front so the on.exit builder is robust even if the run errors early.
  preflight_issues <- character(0)                                  # pre-flight (folder / metadata QC)
  failed_ids <- character(0); tz_issue_ids <- character(0)
  temp_discard_ids <- character(0); temp_override_ids <- character(0)
  ids_str <- function(ids) { ids <- unique(ids); if (length(ids)) paste0(" (", paste(ids, collapse = ", "), ")") else "" }
  # consolidate every collector into one ordered set of issue lines (read at call time, so the closure
  # always sees the final counts). Used for both the cli tally and the deferred warnings.
  .collectIssues <- function() {
    out <- preflight_issues
    if (length(failed_ids))        out <- c(out, sprintf("Failed: %d folder%s could not be imported%s",
                                                         length(failed_ids), if (length(failed_ids) != 1) "s" else "", ids_str(failed_ids)))
    if (length(tz_issue_ids))      out <- c(out, sprintf("Time zone: %d UTC-offset mismatch%s", length(tz_issue_ids), ids_str(tz_issue_ids)))
    if (length(temp_discard_ids))  out <- c(out, sprintf("Temperature: %d discarded, electronics sensor only%s", length(temp_discard_ids), ids_str(temp_discard_ids)))
    if (length(temp_override_ids)) out <- c(out, sprintf("Temperature: %d using an overridden electronics sensor%s", length(temp_override_ids), ids_str(temp_override_ids)))
    out
  }
  if (lvl == 0L) on.exit(for (it in .collectIssues()) warning(it, call. = FALSE), add = TRUE)

  # validate data.folders
  if (!is.character(data.folders)) .abort("{.arg data.folders} must be a character vector of folder paths.")
  .assert_nonempty(data.folders, "data.folders", "input folders")   # e.g. a typo'd list.files() -> character(0)
  missing_folders <- data.folders[!dir.exists(data.folders)]
  if (length(missing_folders) > 0) {
    .abort(c("Some {.arg data.folders} paths do not exist:", "x" = "{.path {missing_folders}}"))
  }

  # subdirectories and output folder (fail-fast: output.dir must already exist)
  .assert_string(sensor.subdirectory, "sensor.subdirectory")
  .assert_string(format, "format")
  .checkFormat(format, "format")            # fail on an unknown reader before anything is read
  .assert_string(wc.subdirectory, "wc.subdirectory", null_ok = TRUE)
  .assert_dir(output.dir, "output.dir", null_ok = TRUE)

  # the sole illegal output request: keep nothing and write nowhere
  .assert_flag(return.data, "return.data")
  .assert_compress(compress)
  .assert_output(return.data, output.dir)

  # validate `id.metadata`
  if (!is.data.frame(id.metadata)) .abort("{.arg id.metadata} must be a data.frame or data.table.")
  if (inherits(id.metadata, "data.table")) id.metadata <- as.data.frame(id.metadata)

  # validate import.mapping
  if (!is.null(import.mapping)) {
    if (!is.data.frame(import.mapping)) .abort("{.arg import.mapping} must be a data.frame or {.code NULL}.")
    required_cols_mapping <- c("colname", "sensor", "units")
    if (!all(required_cols_mapping %in% names(import.mapping))) {
      .abort("{.arg import.mapping} must have columns {.val {required_cols_mapping}}.")
    }
    valid_sensors <- c("date", "time", "datetime", "ax", "ay", "az", "gx", "gy", "gz",
                       "mx", "my", "mz", "depth", "temp", "paddle_speed", "paddle_freq")
    if (any(!import.mapping$sensor %in% valid_sensors)) {
      invalid_sensors <- unique(import.mapping$sensor[!import.mapping$sensor %in% valid_sensors])
      .abort(c("Invalid sensor name(s) in {.arg import.mapping}: {.val {invalid_sensors}}.",
               "i" = "Valid names: {.val {valid_sensors}}."))
    }
    valid_units <- c("UTC", "m/s2", "g", "mrad/s", "rad/s", "deg/s", "uT", "C", "m", "m/s", "Hz", "")
    if (any(!import.mapping$units %in% valid_units)) {
      invalid_units <- unique(import.mapping$units[!import.mapping$units %in% valid_units])
      .abort(c("Invalid unit(s) in {.arg import.mapping}: {.val {invalid_units}}.",
               "i" = "Valid units: {.val {valid_units}}."))
    }
  }

  # canonical ordering of all recognized sensor channels (datetime is handled separately).
  # Only channels actually present are kept, so partial sensor sets are supported.
  sensor_channels <- .sensorChannels()

  # validate required.sensors: NULL (import any file with datetime + >=1 sensor) or a
  # subset of the recognized sensor channels
  if (!is.null(required.sensors)) {
    if (!is.character(required.sensors) || !all(required.sensors %in% sensor_channels)) {
      .abort(c("{.arg required.sensors} must be {.code NULL} or valid sensor names.",
               "i" = "Valid names: {.val {sensor_channels}}."))
    }
  }

  # check that the configured metadata columns exist in id.metadata (NULL = optional column, skipped)
  need_col <- function(col, field) {
    if (!is.null(col) && !col %in% names(id.metadata)) {
      .abort(c("Column {.val {col}} (set via {.code columns${field}}) was not found in {.arg id.metadata}.",
               "i" = "Columns present: {.val {names(id.metadata)}}."))
    }
  }
  need_col(id.col,              "id")
  need_col(tag.model.col,       "tag_model")
  need_col(deploy.date.col,     "deploy_datetime")
  need_col(deploy.lon.col,      "deploy_lon")
  need_col(deploy.lat.col,      "deploy_lat")
  need_col(pop.date.col,        "popup_datetime")
  need_col(pop.lon.col,         "popup_lon")
  need_col(pop.lat.col,         "popup_lat")
  need_col(tag.type.col,        "tag_type")
  need_col(package.id.col,      "package_id")
  need_col(logger.id.col,       "logger_id")
  need_col(exclude.sensors.col, "exclude_sensors")
  need_col(axis.config.col,     "axis_config")
  need_col(attachment.site.col, "attachment_site")
  need_col(deployment.type.col, "deployment_type")
  for (tr in traits.cols) need_col(tr, "traits")             # every declared trait column must exist

  # paddle-wheel column: if set, must be logical or numeric 0/1 (coerced to logical)
  if (!is.null(paddle.wheel.col)) {
    need_col(paddle.wheel.col, "paddle_wheel")
    paddle_values <- id.metadata[[paddle.wheel.col]]
    if (!is.logical(paddle_values)) {
      if (is.numeric(paddle_values) && all(paddle_values %in% c(0, 1, NA))) {
        id.metadata[[paddle.wheel.col]] <- as.logical(paddle_values)
      } else {
        .abort("Column {.val {paddle.wheel.col}} ({.code columns$paddle_wheel}) must be logical or numeric 0/1.")
      }
    }
  }

  # check if deployment info exists for all individuals
  processing_ids <- basename(data.folders)
  processing_metadata <- id.metadata[id.metadata[[id.col]] %in% processing_ids, ]
  missing_deploy_idx <- is.na(processing_metadata[[deploy.date.col]]) | is.na(processing_metadata[[deploy.lon.col]]) | is.na(processing_metadata[[deploy.lat.col]])
  missing_deploy_ids <- processing_metadata[missing_deploy_idx, id.col]
  if (length(missing_deploy_ids) > 0) {
    .abort(c("Some IDs are missing deployment info (datetime, lon, or lat) in {.arg id.metadata}:",
             "x" = "{.val {missing_deploy_ids}}"))
  }

  # deployment / pop-up datetimes must be POSIXct
  if (!inherits(id.metadata[[deploy.date.col]], "POSIXct")) {
    .abort("Column {.val {deploy.date.col}} in {.arg id.metadata} must be of class {.cls POSIXct}.")
  }
  if (!is.null(pop.date.col) && !inherits(id.metadata[[pop.date.col]], "POSIXct")) {
    .abort("Column {.val {pop.date.col}} in {.arg id.metadata} must be of class {.cls POSIXct}.")
  }

  # timezone
  if (!timezone %in% OlsonNames()) {
    .abort(c("{.arg timezone} is not a valid time zone: {.val {timezone}}.",
             "i" = "See {.code OlsonNames()} for valid options."))
  }

  # data.table threads, if specified
  if (!is.null(data.table.threads)) {
    n_cores <- parallel::detectCores()
    if (!is.numeric(data.table.threads) || data.table.threads < 1 || data.table.threads > n_cores) {
      .abort("{.arg data.table.threads} must be a single number between 1 and {n_cores}.")
    }
  }



  ##############################################################################
  # Retrieve directory files ###################################################
  ##############################################################################

  # validate folder animal IDs against id.metadata
  folder_ids <- basename(data.folders)
  missing_ids <- setdiff(folder_ids, id.metadata[[id.col]])
  if (length(missing_ids) > 0) {
    .abort(c("Some folder IDs were not found in {.arg id.metadata}: {.val {missing_ids}}.",
             "i" = "Add these IDs to {.arg id.metadata}, or drop the folders from {.arg data.folders}."))
  }

  # resolve each deployment's raw format: the `tag_format` metadata column wins (so one call can mix tag
  # makes), otherwise the `format` argument applies to all. Explicit and QC'd - nothing is sniffed.
  ids_in <- basename(data.folders)
  fmt_by_folder <- stats::setNames(rep(format, length(data.folders)), ids_in)
  if (!is.null(tag.format.col)) {
    for (k in seq_along(ids_in)) {
      v <- id.metadata[[tag.format.col]][id.metadata[[id.col]] == ids_in[k]]
      if (length(v) && !is.na(v[1]) && nzchar(as.character(v[1]))) fmt_by_folder[k] <- as.character(v[1])
    }
    .checkFormat(fmt_by_folder, "columns$tag_format")
  }

  # does each folder hold data its reader can read? Discovery is format-specific (a ".csv under
  # sensor.subdirectory" is only true of CATS), so ask the reader rather than assuming a layout.
  readers <- .readerFormats()
  data_files <- vapply(seq_along(data.folders), function(k) {
    isTRUE(readers[[fmt_by_folder[k]]]$has_data(data.folders[k], sensor.subdirectory))
  }, logical(1))
  data_files <- ifelse(data_files, data.folders, NA_character_)
  names(data_files) <- ids_in

  # folders without a sensor file will be skipped (no interactive prompt: keeps the behaviour
  # identical in scripts, notebooks and the console). The warning is emitted after the header below,
  # so it reads as a pre-flight notice under the section title.
  missing_folders <- names(data_files)[is.na(data_files)]

  # whole-run guard: if NOT ONE folder held readable data (e.g. a mistyped `sensor.subdirectory`, the
  # wrong `format`, or folders lacking the sensor subdirectory entirely), fail fast rather than silently
  # importing 0 of N. This is distinct from folders whose data is later filtered out (e.g. by
  # `required.sensors`) - there data WAS found, so those runs still return their (possibly empty) result
  # with per-folder warnings.
  if (all(is.na(data_files))) {
    fmt_lab <- paste(unique(vapply(unique(fmt_by_folder), function(f) readers[[f]]$label, character(1))),
                     collapse = " / ")
    .abort(c("No readable {fmt_lab} data was found in any of the {length(data.folders)} {.arg data.folders}.",
             "i" = "Check {.arg format} ({.val {unique(unname(fmt_by_folder))}}) matches your tags.",
             "i" = "For CATS/CEiiA, each folder needs a {.field {sensor.subdirectory}} subdirectory holding sensor {.file .csv} files."))
  }

  # identify Wildlife Computers tag folders or use the specified wc.subdirectory parameter
  if (is.null(wc.subdirectory)) {
    # list and filter subdirectories for each main directory in data.folders
    wc_folders <- lapply(data.folders, function(main_dir) {
      # list immediate subdirectories
      subdirs <- list.dirs(main_dir, full.names = TRUE, recursive = FALSE)
      # a deployment folder need not have ANY subdirectory (e.g. a Little Leonardo export sits directly
      # in it), and sapply() over an empty vector returns list() - which would abort the subscript below.
      if (!length(subdirs)) return(character(0))
      # filter subdirectories containing a 'Locations' file
      subdirs[vapply(subdirs, function(subdir) any(grepl("Locations.csv", list.files(subdir))), logical(1))]
    })
  } else {
    # build full paths for the specified wc.subdirectory
    wc_folders <- lapply(data.folders, function(main_dir) file.path(main_dir, wc.subdirectory))
  }

  # locate locations files within the Wildlife Computers folders (e.g., "Locations.csv")
  wc_files <- sapply(wc_folders, function(x) {
    files <- list.files(x, full.names = TRUE, pattern = "Locations\\.csv$")
    if (length(files) > 0) files[1] else NA
  })
  names(wc_files) <- basename(data.folders)

  # locate the archival files too (e.g. "out-Archive.csv"): they carry the wet/dry ('Dry') signal used
  # as independent surface evidence for the depth drift correction (read into meta$ancillary$dry below)
  archive_files <- sapply(wc_folders, function(x) {
    files <- list.files(x, full.names = TRUE, pattern = "Archive\\.csv$")
    if (length(files) > 0) files[1] else NA
  })
  names(archive_files) <- basename(data.folders)


  ##############################################################################
  # Specify columns to import and respective mappings ##########################
  ##############################################################################

  # default column mappings 1
  CATS <- rbind(
    c("Date (UTC)", "date", "UTC"),
    c("Time (UTC)", "time", "UTC"),
    c("Accelerometer X [m/s\u00b2]", "ax", "m/s2"),
    c("Accelerometer Y [m/s\u00b2]", "ay", "m/s2"),
    c("Accelerometer Z [m/s\u00b2]", "az", "m/s2"),
    c("Gyroscope X [mrad/s]", "gx", "mrad/s"),
    c("Gyroscope Y [mrad/s]", "gy", "mrad/s"),
    c("Gyroscope Z [mrad/s]", "gz", "mrad/s"),
    c("Magnetometer X [\u00b5T]", "mx", "uT"),
    c("Magnetometer Y [\u00b5T]", "my", "uT"),
    c("Magnetometer Z [\u00b5T]", "mz", "uT"),
    c("Depth (200bar) [m]", "depth", "m"),
    c("Depth (200bar) 1 [m]", "depth", "m"),
    c("Depth (100bar) [m]", "depth", "m"),
    c("Temperature (depth) [\u00b0C]", "temp", "C"),
    c("Depth (200bar) 2 [\u00b0C]", "temp", "C")
    # NOTE: "Temp. (magnet.) [\u00b0C]" and "Temperature (imu) [\u00b0C]" are deliberately NOT mapped
    # here - they report tag electronics (magnetometer chip / IMU board) temperature, not water
    # temperature, and are blacklisted below (see `temp_blacklist`).
  )

  # default column mappings 2
  CEIIA <- rbind(
    c("Date", "datetime", "UTC"),
    c("Ax (g)", "ax", "g"),
    c("Ay (g)", "ay", "g"),
    c("Az (g)", "az", "g"),
    c("Gx (\u00b0/s)", "gx", "deg/s"),
    c("Gy (\u00b0/s)", "gy", "deg/s"),
    c("Gz (\u00b0/s)", "gz", "deg/s"),
    c("Mx (\u00b5T)", "mx", "uT"),
    c("My (\u00b5T)", "my", "uT"),
    c("Mz (\u00b5T)", "mz", "uT"),
    c("Temperature (\u00B0C)", "temp", "C"),
    c("Depth (m)", "depth", "m"),
    c("Ticks/s", "paddle_freq", "Hz"),
    c("Velocity (m/s)", "paddle_speed", "m/s")
  )

  # combine all default mappings
  default_mappings <- as.data.frame(rbind(CATS, CEIIA), stringsAsFactors = FALSE)
  colnames(default_mappings) <- c("colname", "sensor", "units")

  # determine the actual mapping to use
  if (!is.null(import.mapping)) {
    active_mapping <- rbind(import.mapping, default_mappings)
  } else {
    active_mapping <- default_mappings
  }

  # pre-compute normalized target names once. Normalization makes header matching
  # tolerant to encoding (UTF-8/Latin-1), confusable symbols (degree sign vs the
  # masculine ordinal commonly exported by CATS firmware, micro sign vs Greek mu,
  # superscript two), and to whitespace/case differences across tag exporters.
  active_mapping_norm <- .normalizeHeader(active_mapping$colname)

  # internal temperature blacklist: channels that report tag ELECTRONICS temperature (magnetometer
  # chip / IMU board), not water temperature. They are never auto-mapped to `temp`; the reliable
  # water sources are the pressure-sensor thermistor ("Temperature (depth)" / "Depth (200bar) 2") and
  # the CEIIA "Temperature (deg C)". A deployment whose ONLY temperature column is blacklisted gets
  # `temp` left unset (with a per-deployment notice). If the user explicitly maps a blacklisted header
  # via `import.mapping`, that intent wins but a warning is raised.
  temp_blacklist      <- c("Temp. (magnet.) [\u00b0C]", "Temperature (imu) [\u00b0C]")
  temp_blacklist_norm <- .normalizeHeader(temp_blacklist)



  ##############################################################################
  # Initialize variables and print console messages ############################
  ##############################################################################

  # create lists to store processed data
  n_animals <- length(data.folders)
  data_list <- vector("list", length = n_animals)
  saved     <- vector("list", length = n_animals)      # per-item written paths (for return.data = FALSE)

  # set data.table threads if specified
  if (!is.null(data.table.threads)) {
    original_threads <- data.table::getDTthreads()
    data.table::setDTthreads(threads = data.table.threads)
    on.exit(data.table::setDTthreads(threads = original_threads), add = TRUE)
  }

  # metadata QC: a pre-QC'd nautilus_deployments carries a verdict (shown in the header, rejected here
  # if FAILED); a plain data.frame is screened inline by the same engine as qcDeploymentMetadata(),
  # restricted to the deployments being imported. Errors abort before the long read.
  qc_guard_err <- .empty_issues(); qc_guard_inline <- is.null(qc_stamp)
  if (qc_guard_inline) {
    qc_norm <- tryCatch(.qcNormalizeMetadata(processing_metadata, columns), error = function(e) NULL)
    if (!is.null(qc_norm)) {
      g <- .runMetadataQC(qc_norm$data, qc_norm$roles)
      qc_guard_err <- g[g$severity == "error", , drop = FALSE]
    }
  }

  # header: a framed block, visually isolated from the per-individual sections that follow
  hdr_bullets <- sprintf("Input: %d folder%s", n_animals, if (n_animals != 1) "s" else "")
  if (!is.null(output.dir)) hdr_bullets <- c(hdr_bullets, paste0("Output: ", output.dir))
  hdr_arrow <- paste0("Mode: ", if (n_animals > 1) "batch import" else "single run")
  if (!is.null(qc_stamp))
    hdr_arrow <- c(hdr_arrow, sprintf("metadata QC: %s (%s)",
                                      if (isTRUE(qc_stamp$passed)) "passed" else "FAILED",
                                      format(qc_stamp$timestamp, "%Y-%m-%d %H:%M")))
  .log_header(lvl, "importTagData", "Welcome aboard: importing biologging data",
              bullets = hdr_bullets, arrow = hdr_arrow)

  # finalise the pre-flight collector with the no-sensor-file folders. Shown as amber cli alerts just
  # under the framed header so the user can spot
  # structural problems and abort before the long read; echoed again in the final tally. At verbose 0
  # the cli is silent and these are emitted as deferred warnings (on.exit) like every other issue.
  if (length(missing_folders) > 0)
    preflight_issues <- c(preflight_issues, sprintf("skipping %d folder%s (no sensor file found): %s",
                                                    length(missing_folders), if (length(missing_folders) != 1) "s" else "",
                                                    paste(unique(missing_folders), collapse = ", ")))
  # the "QC not run" notice is a status line, not a data-quality issue: shown at lvl >= 1 only, and
  # deliberately kept out of the deferred-warning collector (headless callers get the abort instead).
  qc_notice <- if (qc_guard_inline) "metadata QC not run \u2014 validating inline" else NULL
  if (lvl >= 1L && (length(preflight_issues) || !is.null(qc_notice))) {
    for (it in preflight_issues) cli::cli_alert_warning("Pre-flight: {it}")
    if (!is.null(qc_notice)) cli::cli_alert_warning("Pre-flight: {qc_notice}")
    cli::cli_text("")                                   # blank line before the first individual block
  }

  # reject metadata with blocking errors before committing to the (potentially long) import
  if (!is.null(qc_stamp) && isFALSE(qc_stamp$passed)) {
    .abort(c("{.arg id.metadata} failed metadata QC ({qc_stamp$n_errors} error{?s}).",
             "i" = "Inspect them with {.code qcIssues()} and fix the metadata before importing."))
  }
  if (nrow(qc_guard_err) > 0L) {
    .abort(c("Metadata QC found {nrow(qc_guard_err)} blocking error{?s} in the deployments being imported:",
             stats::setNames(qc_guard_err$message, rep("x", nrow(qc_guard_err))),
             "i" = "Run {.code qcDeploymentMetadata()} to diagnose, or fix the metadata."))
  }


  ##############################################################################
  # Import data for each folder ################################################
  ##############################################################################

  # iterate over each animal. Per-run issue trackers (counts + affected IDs) feed the final summary.
  n_done <- 0L                                        # issue collectors are initialised at the top
  for (i in seq_along(data_files)) {

    # get animal ID
    id <- basename(data.folders)[i]

    # per-individual sub-header (level-2 only; groups this individual's detail lines)
    .log_h2(lvl, sprintf("%s (%d/%d)", id, i, n_animals))

    # retrieve the WC locations file path for the current animal
    locations_file <- wc_files[i]

    # retrieve metadata for the current animal ID from the metadata table
    animal_info <- id.metadata[id.metadata[[id.col]]==id,]


    ############################################################################
    # Read this individual's sensor data (CATS / CEiiA format) #################
    ############################################################################

    # channels this deployment's metadata flags unusable (a known data-quality fact, e.g. a firmware
    # bug). Resolved here (metadata is format-agnostic) and applied by the reader BEFORE unit
    # conversion, so a dropped channel never reaches the conversion notes.
    exclude_want <- character(0)
    if (!is.null(exclude.sensors.col)) {
      exclude_want <- tryCatch(.expandSensorTokens(animal_info[[exclude.sensors.col]]),
                               error = function(e) character(0))
    }

    # files -> canonical frame: probe/merge every candidate CSV (maximal-unique-coverage, resilient to
    # overlapping fragments and corrupt trailing rows), map the header onto sensor roles, parse the
    # timestamps, drop excluded channels, read the calibration sidecar and convert to canonical units.
    # read_cats() RETURNS its per-deployment status and report data rather than printing or collecting:
    # the collectors below are locals read by an on.exit closure, so a write from inside another frame
    # would silently vanish, and the report has to interleave with metadata lines only known out here.
    # dispatch on this deployment's resolved format. A simple switch, not a registry: readers are
    # internal, so adding one is a reader file + a line here + an entry in .readerFormats().
    fmt_i <- fmt_by_folder[[i]]
    res <- switch(
      fmt_i,
      cats = read_cats(
        folder = data.folders[i], sensor.subdirectory = sensor.subdirectory,
        active_mapping = active_mapping, active_mapping_norm = active_mapping_norm,
        temp_blacklist_norm = temp_blacklist_norm,
        required.sensors = required.sensors, timezone = timezone,
        import.calibration = import.calibration, exclude.channels = exclude_want,
        verbose = lvl >= 2L),
      little_leonardo = read_little_leonardo(
        folder = data.folders[i], sensor.subdirectory = sensor.subdirectory,
        # the raw carries no clock: the recording start comes from the `data_start` metadata role
        start = if (!is.null(data.start.col)) animal_info[[data.start.col]][1] else NULL,
        timezone = timezone, exclude.channels = exclude_want, verbose = lvl >= 2L),
      .abort("Unsupported tag format: {.val {fmt_i}}."))

    # skip this individual if no usable sensor data could be assembled. A "no sensor CSV" folder that
    # was already flagged pre-flight (missing_folders) is covered there - just a quiet inline skip, no
    # double-counting. Anything else (corrupt file, missing required sensors, or a no-CSV not caught
    # pre-flight) is a genuine per-deployment failure -> collected into `failed_ids`.
    if (is.null(res$data)) {
      # "no data here" reads differently per format ("no sensor CSV ..." / "no sensor file: ...");
      # either way, a folder the pre-flight already flagged is a quiet skip, not a second failure.
      preflight_covered <- (startsWith(res$reason, "no sensor CSV") ||
                            startsWith(res$reason, "no sensor file")) && id %in% missing_folders
      if (preflight_covered) {
        .log_skip(lvl, id, " skipped ", cli::symbol$bullet, " ", res$reason)
      } else {
        if (lvl >= 1L) cli::cli_alert_danger("{id}: {res$reason}. Data not imported.")
        failed_ids <- c(failed_ids, id)
      }
      data_list[[i]] <- NA
      .log_gap(lvl)
      next
    }

    sensor_data       <- res$data
    file_mapping      <- res$mapping
    selected_cols     <- res$selected_cols
    calibration_info  <- res$calibration_info
    excluded_channels <- res$excluded
    temp_status       <- res$temp_status
    rows <- nrow(sensor_data)

    # collect the reader's per-deployment issues. Temperature: the reliable `temp` is the pressure-sensor
    # thermistor; electronics channels (IMU board / magnetometer chip) are blacklisted, so `temp` is left
    # unset (missing-sensor convention) when the only temperature column is blacklisted, while an explicit
    # user override is honoured but flagged. Timezone: the sidecar's UTC offset disagreed with `timezone`.
    # Surfaced inline (lvl >= 2) below, echoed in the final tally (lvl >= 1), deferred warning (lvl == 0).
    if (identical(temp_status, "blacklisted_only")) temp_discard_ids  <- c(temp_discard_ids, id)
    else if (identical(temp_status, "override"))    temp_override_ids <- c(temp_override_ids, id)
    if (isTRUE(res$tz_mismatch)) tz_issue_ids <- c(tz_issue_ids, id)

    if (lvl >= 2L && length(excluded_channels)) {
      .log_skip(lvl, "excluded sensors (metadata): ",
                paste(.channelsToFamilies(excluded_channels), collapse = ", "))
    }

    # tag attributes (model / type / package): the most fundamental dataset attributes, shown first in
    # the individual's block and reused in the normal-level one-line summary.
    tag_model <- animal_info[[tag.model.col]]
    if (!is.null(tag.type.col)) {
      tag_type <- animal_info[[tag.type.col]]
    } else {
      # infer from the ID string ("Camera" if it contains "CAM", else "MS" multisensor)
      tag_type <- ifelse(grepl("CAM", id, fixed = TRUE), "Camera", "MS")
    }
    package_id <- if (!is.null(package.id.col)) animal_info[[package.id.col]] else NULL
    tag_attrs <- paste(tag_model, tag_type)
    if (!is.null(package_id)) tag_attrs <- paste0(tag_attrs, " ", cli::symbol$bullet, " package ", package_id)

    # what was read (level-2 detail): tag attributes, sensor channels, row count + approximate rate,
    # and the recorded span (data is already time-sorted, so first/last are O(1))
    if (lvl >= 2L) {
      attrs_line <- paste0(cli::symbol$bullet, " ", tag_attrs)
      cli::cli_text("{attrs_line}")            # pass as data, never re-parsed as a cli template
      # per-file assembly decisions, printed after the tag attributes and before the sensor details
      .reportAssembly(res$assembly, lvl)
      chans <- intersect(.sensorChannels(), names(sensor_data))
      .log_detail(lvl, "sensors: ", paste(chans, collapse = ", "))
      # temperature reliability, injected directly under the sensors row (punchy, not a wrapped warning)
      if (identical(temp_status, "blacklisted_only")) {
        .log_skip(lvl, "temp: electronics sensor only (Temp. (magnet.)) \u2014 discarded")
      } else if (identical(temp_status, "override")) {
        ov <- file_mapping$colname_in_csv[file_mapping$sensor_name_out == "temp"][1]
        .log_skip(lvl, "temp: using overridden electronics sensor (", ov, ") per import.mapping")
      }
      t1 <- sensor_data$datetime[1]; t2 <- sensor_data$datetime[rows]
      secs <- as.numeric(difftime(t2, t1, units = "secs"))
      fs <- if (is.finite(secs) && secs > 0) (rows - 1) / secs else NA_real_
      .log_detail(lvl, formatC(rows, format = "d", big.mark = ","), " rows",
                  if (is.finite(fs)) paste0(" | ~", signif(fs, 3), " Hz"))
      .log_detail(lvl, "span: ", format(t1, "%Y-%m-%d %H:%M"), " to ", format(t2, "%Y-%m-%d %H:%M"),
                  " (", .fmt_duration(secs), ")")
    }

    # calibration sidecar (read by read_cats(); parsed and stored for provenance, values are NOT applied
    # to the sensor data at this stage) and the recording-zone cross-check: if the sidecar reports a UTC
    # offset that disagrees with `timezone`, the recorded clock is probably local (never silently
    # shifted). Rendered in the original order: report, then the timezone note.
    if (isTRUE(import.calibration)) {
      .reportCalibration(calibration_info, lvl)
      if (lvl >= 2L && !is.null(res$tz_note)) .log_skip(lvl, res$tz_note)
    }


    # unit conversion happened in read_cats() (every channel to canonical g / rad/s / uT, driven by the
    # CSV's declared units); render its merged per-group note here, where the original printed it.
    if (lvl >= 2L && length(res$unit_notes)) .log_detail(lvl, "units: ", paste(res$unit_notes, collapse = ", "))

    # add ID column
    sensor_data[, ID := id]

    # order columns: ID, datetime, then whichever recognized sensor channels are
    # present (supports partial sensor sets, e.g. accelerometer-only or TDR data)
    present_channels <- sensor_channels[sensor_channels %in% names(sensor_data)]
    data.table::setcolorder(sensor_data, c("ID", "datetime", present_channels))


    ############################################################################
    # Wildlife Computers ancillary streams + clock alignment ###################
    ############################################################################

    # A co-deployed WC tag is a separate DEVICE, not a format of this one: it gets its own reader and
    # attaches to the assembled tag, so no primary reader ever learns about it (see R/read_wc.R). The
    # COMPLETE position record is kept as an independent ancillary stream at its own cadence - NOT snapped
    # to sensor rows and NOT trimmed to the recording window - so the full fix history survives downstream
    # (tracks, maps, post-deployment surface drift). Deploy/pop-up positions live in meta$deployment.
    wc_anc <- read_wc(locations_file, archive_files[i])
    if (lvl >= 2L && !is.null(wc_anc$positions)) {
      wc_model <- .wcModel(dirname(wc_files[i])); instr <- if (!is.null(wc_model)) wc_model else "WC"
      .log_detail(lvl, "locations: ", sprintf("%d %s fix%s", nrow(wc_anc$positions$data), instr,
                                              if (nrow(wc_anc$positions$data) != 1) "es" else ""))
    }

    # Align the co-deployed device's clock onto this tag's timeline and attach. attachAncillary() is the
    # single home for that alignment; it abstains and shifts nothing when the evidence is weak.
    att <- attachAncillary(sensor_data, wc_anc, align = alignment)
    # report only when there was actually a stream to shift: the aligner also records an abstention for
    # tags with no co-deployed device, and that is provenance, not something to narrate.
    if (lvl >= 2L && length(wc_anc)) .reportAlignment(att$info, lvl)


    ############################################################################
    # Save relevant attributes #################################################
    ############################################################################

    # (the recorded span is derived from the data by the shared assembler below)

    # check if this tag has a paddle wheel
    has_paddle <- NULL
    if(!is.null(paddle.wheel.col)) has_paddle <- animal_info[[paddle.wheel.col]]

    # lightweight "processed by nautilus" marker (all other provenance now lives in
    # the consolidated nautilus metadata object built below)
    attr(sensor_data, 'nautilus.version') <- utils::packageVersion("nautilus")

    # build the consolidated nautilus metadata object and class the output as a nautilus_tag. The schema
    # is populated by the SHARED assembler (.assembleTagMeta), which buildTagData() also fronts, so the
    # reader path and the in-memory path cannot drift as new per-format readers are added. This function
    # supplies the role-named row (it holds the resolved column names) and adds its own extras below.
    role_meta <- list(
      deploy_lon      = animal_info[[deploy.lon.col]],
      deploy_lat      = animal_info[[deploy.lat.col]],
      deploy_datetime = animal_info[[deploy.date.col]],
      tag_model       = tag_model,
      tag_type        = tag_type,
      package_id      = package_id %||% NA,
      paddle_wheel    = has_paddle %||% NA
    )
    if (!is.null(pop.date.col))        role_meta$popup_datetime  <- animal_info[[pop.date.col]]
    if (!is.null(pop.lon.col))         role_meta$popup_lon       <- animal_info[[pop.lon.col]]
    if (!is.null(pop.lat.col))         role_meta$popup_lat       <- animal_info[[pop.lat.col]]
    if (!is.null(attachment.site.col)) role_meta$attachment_site <- animal_info[[attachment.site.col]]
    if (!is.null(deployment.type.col)) role_meta$deployment_type <- animal_info[[deployment.type.col]]
    if (!is.null(logger.id.col))       role_meta$logger_id       <- animal_info[[logger.id.col]]
    if (!is.null(axis.config.col))     role_meta$axis_config     <- animal_info[[axis.config.col]]
    # passive biological traits ride along under their own names (carried through verbatim)
    for (tr in traits.cols) role_meta[[tr]] <- animal_info[[tr]]

    # sampling.hz = NULL: import never persists a rate - it is inferred from the timestamps downstream
    # (see meta$sensors / processTagData)
    meta <- .assembleTagMeta(sensor_data, id = id, metadata = role_meta, traits = traits.cols,
                             timezone = timezone, sampling.hz = NULL)

    # ---- extras only the file-import path has -------------------------------------------------------
    meta$sensors$excluded <- excluded_channels       # channels dropped per the exclude_sensors metadata
    meta$sensors$recording_utc_offset <- if (!is.null(calibration_info)) calibration_info$device$utc_offset else NA_real_
    # store only calibration-specific provenance; sampling frequency is inferred from the data,
    # never persisted as a calibration-derived attribute
    cal_to_store <- calibration_info
    if (!is.null(cal_to_store)) cal_to_store$sample_rate <- NULL
    meta$calibration  <- cal_to_store
    # ancillary streams from any co-deployed device (read + clock-aligned above): wet/dry
    # (transition-encoded, consumed by the depth drift correction) and the full position record. The
    # clock-alignment provenance rides alongside them, so the applied offset - or the reason the aligner
    # abstained - stays inspectable downstream.
    for (nm in names(att$ancillary)) meta$ancillary[[nm]] <- att$ancillary[[nm]]
    meta <- .appendProcessing(meta, "importTagData",
                              directory = data.folders[i],
                              imported_columns = selected_cols,
                              timezone = timezone)
    sensor_data <- new_nautilus_tag(sensor_data, meta)


    ############################################################################
    # Save imported data #######################################################
    ############################################################################

    # persist the imported data (path-as-switch: writes only when output.dir is set)
    saved_to <- .saveOutput(sensor_data, id, output.dir = output.dir, output.suffix = output.suffix, compress = compress)
    saved[i] <- list(saved_to)

    # per-individual summary line. At the detailed level the block above already shows rows / model /
    # span, so the closing line is terse (just the save outcome); at the normal level it carries the
    # full one-line summary (id, rows, model, save).
    n_done <- n_done + 1L
    if (lvl >= 2L) {
      .log_ok(lvl, if (!is.null(saved_to)) paste0("saved ", basename(saved_to)) else "imported")
    } else {
      .log_ok(lvl, id, "  imported ", cli::symbol$bullet, " ",
              formatC(rows, format = "d", big.mark = ","), " rows ", cli::symbol$bullet, " ", tag_attrs,
              if (!is.null(saved_to)) paste0(" ", cli::symbol$bullet, " saved ", basename(saved_to)))
    }

    .log_gap(lvl)                          # blank line separates this individual's block from the next

    # store processed sensor data in the results list if needed
    if (return.data) {
      data_list[[i]] <- sensor_data
    }

    # drop the reference to the processed data before the next iteration
    # (R's garbage collector reclaims it automatically; an explicit gc() every
    # iteration would only slow the loop down)
    rm(sensor_data)

  }

  ##############################################################################
  # Return imported data #######################################################
  ##############################################################################

  # ---- final run summary -----------------------------------------------------------------------
  # Its own block (its own rule + blank-line separation), because the overall run statistics are
  # conceptually distinct from the continuous per-individual processing above.
  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, n_done, " of ", n_animals, " folder", if (n_animals != 1) "s", " imported")
    if (!is.null(output.dir)) .log_arrow(lvl, "output: ", output.dir)
    .log_runtime(lvl, start.time)

    # master Issues tally - echoes EVERY category (pre-flight + per-deployment) so the final verdict
    # is complete without scrolling. cli only; the verbose == 0 channel is the deferred warnings.
    issues <- .collectIssues()
    if (length(issues)) {
      cli::cli_text("")
      cli::cli_alert_warning("{.strong Issues}")
      for (it in issues) cli::cli_text("  {cli::symbol$bullet} {it}")
    }
  }


  # return the imported objects (return.data = TRUE) or the written .rds file paths (return.data = FALSE,
  # which chain into the next step's `data` argument)
  if (return.data) {
    # assign names to the data list
    names(data_list) <- basename(data.folders)
    # convert NA placeholders back to NULL (single subset-assignment to avoid the
    # index-shifting bug that arises when removing list elements one at a time)
    na_indices <- which(sapply(data_list, function(x) identical(x, NA)))
    if (length(na_indices) > 0) data_list[na_indices] <- NULL
    # return the list containing processed sensor data for all folders
    return(data_list)
  }
  unlist(saved, use.names = FALSE)
}


################################################################################
# Internal helpers for multi-CSV ingestion #####################################
################################################################################

#' List candidate sensor CSV files in an individual's sensor subdirectory
#'
#' Returns full paths to plausible sensor CSVs, excluding Wildlife Computers
#' location files and zero-byte files.
#' @keywords internal
#' @noRd

.listSensorCSVs <- function(folder, sensor.subdirectory) {
  dir <- file.path(folder, sensor.subdirectory)
  if (!dir.exists(dir)) return(character(0))
  files <- list.files(dir, full.names = TRUE, pattern = "(?i)\\.csv$")
  if (!length(files)) return(files)
  # drop Wildlife Computers location files and empty files
  files <- files[!grepl("Locations\\.csv$", basename(files), ignore.case = TRUE)]
  info <- file.info(files)
  files[!is.na(info$size) & info$size > 0]
}


#' Build the sensor mapping for a single CSV header (normalized matching)
#'
#' Resolves each recognized header column to a standardized sensor name (priority = row order in
#' `active_mapping`, user `import.mapping` rows first). Temperature is special-cased: headers in
#' `blacklist_norm` (tag electronics temperature, not water) are never auto-mapped to `temp`, but
#' their presence is recorded so the caller can warn. The returned data.frame carries two attributes:
#' \code{temp_status} (one of "ok", "override", "blacklisted_only", "none") and
#' \code{temp_blacklisted_present} (the original blacklisted header labels found in this file).
#' @return A data.frame with columns colname_in_csv / sensor_name_out /
#'   original_units_map (plus the two attributes above), or NULL if no mappable columns are found.
#' @keywords internal
#' @noRd

.buildFileMapping <- function(header, active_mapping, active_mapping_norm, blacklist_norm = character(0)) {
  hnorm <- .normalizeHeader(header)
  rows <- list()
  for (j in seq_len(nrow(active_mapping))) {
    idx <- which(hnorm == active_mapping_norm[j])
    if (length(idx) > 0) {
      rows[[length(rows) + 1]] <- data.frame(
        colname_in_csv = header[idx[1]],
        sensor_name_out = active_mapping$sensor[j],
        original_units_map = active_mapping$units[j],
        stringsAsFactors = FALSE)
    }
  }
  if (!length(rows)) return(NULL)
  fm <- do.call(rbind, rows)
  fm <- fm[!duplicated(fm$sensor_name_out), ]

  # temperature reliability: which blacklisted (electronics) temp headers are physically present, and
  # whether the resolved `temp` (if any) came from one of them (only possible via explicit user mapping,
  # since the defaults never list blacklisted headers).
  blacklisted_present <- header[hnorm %in% blacklist_norm]
  temp_src <- fm$colname_in_csv[fm$sensor_name_out == "temp"]
  temp_status <- if (length(temp_src)) {
    if (.normalizeHeader(temp_src[1]) %in% blacklist_norm) "override" else "ok"
  } else if (length(blacklisted_present)) {
    "blacklisted_only"
  } else {
    "none"
  }
  attr(fm, "temp_status") <- temp_status
  attr(fm, "temp_blacklisted_present") <- blacklisted_present
  fm
}


#' Offset (in hours) of an Olson time zone at a given instant (DST-aware). UTC -> 0.
#' @keywords internal
#' @noRd
.tzOffsetHours <- function(tz, ref = Sys.time()) {
  ref <- as.POSIXct(ref[1], tz = "UTC")
  if (is.na(ref)) ref <- as.POSIXct(Sys.time(), tz = "UTC")
  local_clock <- as.POSIXct(format(ref, tz = tz, usetz = FALSE), tz = "UTC")
  as.numeric(difftime(local_clock, ref, units = "hours"))
}


#' Parse and attach a POSIXct `datetime` column to a freshly-read sensor table
#'
#' Handles either a single `datetime` column or separate `date` + `time`
#' columns. The +0.0001 s offset matches the historical behaviour (a
#' floating-point safeguard against off-by-one-second rounding downstream).
#' @keywords internal
#' @noRd

.addDatetime <- function(d, file_mapping, timezone) {
  sensors <- file_mapping$sensor_name_out
  if ("datetime" %in% sensors) {
    if (!inherits(d$datetime, "POSIXct")) {
      d[, datetime := as.POSIXct(datetime, tz = timezone) + 0.0001]
    } else {
      # already parsed (e.g. by fread, which assumes UTC): force the clock reading into `timezone`
      # WITHOUT shifting it, so this matches the character/date+time paths (interpret, never convert).
      d[, datetime := lubridate::force_tz(datetime, timezone) + 0.0001]
    }
  } else if (all(c("date", "time") %in% sensors)) {
    # lt = FALSE returns POSIXct directly; the default POSIXlt would force a costly
    # as.POSIXct.POSIXlt conversion (and a large allocation) on the next operation.
    d[, datetime := lubridate::fast_strptime(paste(date, time), "%d.%m.%Y %H:%M:%OS",
                                             tz = timezone, lt = FALSE) + 0.0001]
    d[, c("date", "time") := NULL]
  }
  d
}


#' Locate, probe and assemble all sensor CSVs for one individual
#'
#' Implements robust multi-CSV ingestion: it probes every candidate CSV
#' (header schema, datetime capability, required sensors, size), keeps the
#' schema group of the largest usable file (other schemas are treated as
#' separate devices and skipped), then assembles the group using a
#' maximal-unique-coverage policy — reading files largest-first and keeping
#' only rows that fall outside the time span already covered. This is resilient
#' to overlapping fragments, duplicate exports, and corrupt trailing rows.
#'
#' @return A list with elements: `data` (assembled data.table with a parsed
#'   `datetime` column and renamed sensor columns, or NULL), `mapping` (the
#'   primary file's column mapping), `reason` (why nothing was returned, when
#'   `data` is NULL), `decisions` (per-file log), `n_files`, `n_excluded`,
#'   and `duplicates_removed`.
#' @keywords internal
#' @noRd

.assembleSensorData <- function(folder, sensor.subdirectory, active_mapping,
                                active_mapping_norm, required.sensors, timezone,
                                temp_blacklist_norm = character(0),
                                verbose = TRUE) {

  files <- .listSensorCSVs(folder, sensor.subdirectory)
  if (!length(files)) return(list(data = NULL, reason = "no sensor CSV files found"))

  # a file is "usable" if it has a datetime and the required sensors. When
  # required.sensors is NULL, any file with >=1 recognized sensor channel qualifies.
  .meetsRequirement <- function(sensors) {
    channels <- setdiff(sensors, c("date", "time", "datetime"))
    if (is.null(required.sensors)) length(channels) >= 1L
    else all(required.sensors %in% sensors)
  }

  # ---- probe each candidate (cheap: header only) ----
  probes <- lapply(files, function(f) {
    header <- tryCatch(names(data.table::fread(f, nrows = 0)), error = function(e) NULL)
    if (is.null(header) || !length(header)) return(NULL)
    fm <- .buildFileMapping(header, active_mapping, active_mapping_norm, temp_blacklist_norm)
    if (is.null(fm)) return(NULL)
    sensors <- sort(unique(fm$sensor_name_out))
    list(file = f, mapping = fm,
         schema = paste(sensors, collapse = "+"),
         has_dt = ("datetime" %in% sensors) || all(c("date", "time") %in% sensors),
         has_required = .meetsRequirement(sensors),
         size = file.info(f)$size)
  })
  probes <- Filter(Negate(is.null), probes)
  if (!length(probes)) return(list(data = NULL, reason = "no CSV with recognizable sensor columns"))

  # ---- select the primary schema group (schema of the largest usable file) ----
  usable <- Filter(function(p) p$has_dt && p$has_required, probes)
  if (!length(usable)) {
    biggest <- probes[[which.max(vapply(probes, function(p) p$size, numeric(1)))]]
    miss <- if (!is.null(required.sensors)) setdiff(required.sensors, biggest$mapping$sensor_name_out) else character(0)
    reason <- if (length(miss)) paste0("missing required columns: ", paste(miss, collapse = ", "))
              else "no file with both a usable datetime and recognized sensor data"
    return(list(data = NULL, reason = reason))
  }
  primary <- usable[[which.max(vapply(usable, function(p) p$size, numeric(1)))]]
  group  <- Filter(function(p) identical(p$schema, primary$schema), usable)
  others <- Filter(function(p) !identical(p$schema, primary$schema), probes)

  # order the group largest-first (primary record dominates overlaps)
  group <- group[order(vapply(group, function(p) p$size, numeric(1)), decreasing = TRUE)]

  # ---- read & coverage-merge ----
  covered   <- list()   # list of c(start, end) epoch-second intervals already covered
  assembled <- list()
  decisions <- list()

  for (p in group) {
    fm <- p$mapping
    sel <- fm$colname_in_csv
    cc <- stats::setNames(rep("numeric", length(sel)), sel)
    for (s in c("date", "time", "datetime")) {
      cn <- fm$colname_in_csv[fm$sensor_name_out == s]
      if (length(cn)) cc[cn] <- "character"
    }
    d <- tryCatch(
      suppressWarnings(data.table::fread(p$file, select = sel, colClasses = cc,
                                         na.strings = c("NA", "", "NULL", "NaN", "-"), showProgress = FALSE)),
      error = function(e) NULL)
    if (is.null(d) || nrow(d) == 0) {
      decisions[[length(decisions) + 1]] <- list(file = p$file, total = 0L, kept = 0L, note = "empty/unreadable")
      next
    }
    data.table::setnames(d, old = fm$colname_in_csv, new = fm$sensor_name_out, skip_absent = TRUE)
    d <- .addDatetime(d, fm, timezone)
    d <- d[!is.na(datetime)]
    if (nrow(d) == 0) {
      decisions[[length(decisions) + 1]] <- list(file = p$file, total = 0L, kept = 0L, note = "no valid timestamps")
      next
    }
    data.table::setorder(d, datetime)
    ts <- as.numeric(d$datetime)
    keep <- rep(TRUE, length(ts))
    for (iv in covered) keep[ts >= iv[1] & ts <= iv[2]] <- FALSE
    total <- nrow(d)
    covered[[length(covered) + 1]] <- c(min(ts), max(ts))
    d_keep <- d[keep]
    if (nrow(d_keep) > 0) assembled[[length(assembled) + 1]] <- d_keep
    note <- if (nrow(d_keep) == 0) "dropped: fully overlapping"
            else if (nrow(d_keep) < total) "merged (partial overlap removed)"
            else "merged"
    decisions[[length(decisions) + 1]] <- list(file = p$file, total = total, kept = nrow(d_keep),
                                               start = min(d$datetime), end = max(d$datetime), note = note)
  }

  for (o in others) {
    decisions[[length(decisions) + 1]] <- list(file = o$file, total = NA_integer_, kept = 0L,
                                               note = "skipped: different schema/device")
  }

  if (!length(assembled)) return(list(data = NULL, reason = "no usable rows after assembly",
                                      decisions = decisions))

  out <- data.table::rbindlist(assembled, use.names = TRUE, fill = TRUE)
  data.table::setorder(out, datetime)
  dup <- duplicated(out$datetime)
  n_dup <- sum(dup)
  if (n_dup > 0) out <- out[!dup]

  list(data = out, mapping = primary$mapping, primary_file = primary$file,
       decisions = decisions, n_files = length(group), n_excluded = length(others),
       duplicates_removed = n_dup)
}


#' Report per-file assembly decisions (level-2 detail).
#' @keywords internal
#' @noRd

.reportAssembly <- function(assembly, lvl = 1L) {
  if (lvl < 2L) return(invisible(NULL))
  fmt <- function(n) if (is.na(n)) "-" else formatC(n, format = "d", big.mark = ",")
  if (length(assembly$decisions) > 1 || assembly$n_excluded > 0) {
    head <- sprintf("assembled %d CSV file(s)", assembly$n_files)
    if (assembly$n_excluded > 0) head <- paste0(head, sprintf(" (%d skipped: other schema)", assembly$n_excluded))
    .log_detail(lvl, head)
    # individual files listed as indented, no-symbol sub-lines under the "assembled ..." parent.
    # When a whole file is discarded (kept == 0) the row count is redundant, so show only the reason.
    sub <- vapply(assembly$decisions, function(d) {
      if (!is.na(d$kept) && d$kept == 0 && !is.null(d$note)) {
        paste0(basename(d$file), ": ", d$note)
      } else {
        paste0(basename(d$file), ": ", fmt(d$kept), " rows", if (!is.null(d$note)) paste0(", ", d$note) else "")
      }
    }, character(1))
    if (assembly$duplicates_removed > 0)
      sub <- c(sub, paste0(fmt(assembly$duplicates_removed), " duplicate timestamps removed"))
    if (length(sub)) cli::cli_bullets(stats::setNames(sub, rep(" ", length(sub))))
  }
  invisible(NULL)
}


################################################################################
# Internal helpers for sidecar calibration ingestion ###########################
################################################################################

#' Parse a comma-separated numeric list (e.g. "-37.8, -1") into a numeric vector
#' @keywords internal
#' @noRd

.parseNumList <- function(s) {
  if (length(s) != 1 || is.na(s) || !nzchar(trimws(s))) return(numeric(0))
  suppressWarnings(as.numeric(trimws(strsplit(s, ",")[[1]])))
}


#' Parse a CATS mini-diary `.txt` sidecar into a calibration structure
#'
#' Reads the INI-style device/calibration file written alongside CATS multisensor
#' CSVs. Returns NULL if the file is not a calibration sidecar (e.g. a Wildlife
#' Computers `version.txt`/`sources.txt`), i.e. if it lacks the `[device]` and
#' `[activated sensors]` sections. Channels are identified by their `NN_name`
#' (not the numeric index, which is not stable across firmwares). Magnetometer
#' ASA coefficients of `0/0/0` are treated as unset (NA).
#' @keywords internal
#' @noRd

.parseCATSDiaryTxt <- function(path) {
  lines <- tryCatch(readLines(path, warn = FALSE), error = function(e) NULL)
  if (is.null(lines) || !length(lines)) return(NULL)
  lines <- .toUTF8(lines)
  lines <- trimws(gsub("\r", "", lines))
  low <- tolower(lines)

  # must look like a CATS device/calibration file
  if (!any(low == "[device]") || !any(low == "[activated sensors]")) return(NULL)

  is_header <- grepl("^\\[", lines)
  section_range <- function(name) {
    start <- which(low == name)[1]
    if (is.na(start)) return(integer(0))
    after <- which(is_header & seq_along(lines) > start)
    end <- if (length(after)) after[1] - 1L else length(lines)
    start:end
  }
  kv <- function(rng, key) {
    pat <- paste0("^", key, "\\s*=\\s*")
    hit <- grep(pat, lines[rng], ignore.case = TRUE, value = TRUE)
    if (!length(hit)) return(NA_character_)
    trimws(sub(pat, "", hit[1], ignore.case = TRUE))
  }

  # --- device block ---
  dev <- section_range("[device]")
  device <- list(sn = kv(dev, "sn"), id = kv(dev, "id"),
                 utc_offset = suppressWarnings(as.numeric(kv(dev, "utc_offset"))))

  # --- activated sensors block ---
  as_rng <- section_range("[activated sensors]")
  name_lines <- grep("^[0-9]{2}_name\\s*=", lines[as_rng], value = TRUE)
  cal <- list()
  for (nl in name_lines) {
    nn <- sub("_name.*", "", nl)
    nm <- tolower(trimws(sub("^[0-9]{2}_name\\s*=\\s*", "", nl)))
    chan <- if (grepl("acceleromet", nm)) "accel"
            else if (grepl("gyroscop", nm)) "gyro"
            else if (grepl("magnetomet", nm)) "mag"
            else if (grepl("^depth", nm)) "depth"
            else if (grepl("temperature \\(depth\\)", nm)) "temp"
            else NA_character_
    if (is.na(chan)) next
    entry <- list(offset = .parseNumList(kv(as_rng, paste0(nn, "_offset"))),
                  factor = .parseNumList(kv(as_rng, paste0(nn, "_factor"))))
    if (chan == "mag") {
      coef <- kv(as_rng, paste0(nn, "_coefficient"))
      if (!is.na(coef)) {
        asa <- suppressWarnings(as.numeric(regmatches(coef, gregexpr("[-0-9.]+", coef))[[1]]))
        if (length(asa) && all(asa == 0, na.rm = TRUE)) asa <- NA_real_  # 0/0/0 = unset
        entry$asa <- asa
      }
    }
    cal[[chan]] <- entry
  }
  if (!length(cal)) return(NULL)

  list(source = path, source_type = "cats_diary_txt",
       device = device, sample_rate = NA_real_, calibration = cal)
}


#' Parse a CATS camera `_resume.json` sidecar (metadata only; no calibration)
#'
#' Extracts the sample rate via a lightweight regex (avoids a JSON dependency).
#' Camera resume files do not carry accelerometer/magnetometer/depth calibration
#' constants, so `calibration` is NULL.
#' @keywords internal
#' @noRd

.parseCATSResumeJson <- function(path) {
  lines <- tryCatch(readLines(path, warn = FALSE), error = function(e) NULL)
  if (is.null(lines)) return(NULL)
  txt <- paste(.toUTF8(lines), collapse = " ")
  sr <- NA_real_
  m <- regmatches(txt, regexpr('"[0-9.]*sampleRate"\\s*:\\s*[0-9.]+', txt, ignore.case = TRUE))
  if (length(m)) sr <- suppressWarnings(as.numeric(sub('.*:\\s*', '', m)))
  list(source = path, source_type = "cats_resume_json",
       device = list(), sample_rate = sr, calibration = NULL)
}


#' Read the calibration sidecar paired with a sensor CSV
#'
#' Looks for a sibling `<basename>.txt` (CATS mini-diary calibration) and, failing
#' that, a `<basename>_resume.json` (CATS camera metadata). Returns NULL when no
#' valid sidecar is found.
#' @keywords internal
#' @noRd

.readSidecarCalibration <- function(csv_path) {
  base <- sub("\\.csv$", "", csv_path, ignore.case = TRUE)
  txt  <- paste0(base, ".txt")
  json <- paste0(base, "_resume.json")
  if (file.exists(txt)) {
    cal <- .parseCATSDiaryTxt(txt)
    if (!is.null(cal)) return(cal)
  }
  if (file.exists(json)) return(.parseCATSResumeJson(json))
  NULL
}


#' Summarise an ingested calibration sidecar (level-2 detail): the sidecar file on a `->` line, then
#' the parsed calibration constants on their own indented sub-lines (no `->` prefix) to show they are
#' metadata belonging to that file. Values are stored for provenance only; they are NOT applied at
#' import. `depth offset` / `mag ASA` may be vectors (one value per depth channel, or the three
#' magnetometer axes) - all non-missing values are shown, joined by " / ".
#' @keywords internal
#' @noRd

.reportCalibration <- function(cal, lvl = 1L) {
  if (lvl < 2L) return(invisible(NULL))
  if (is.null(cal)) { .log_detail(lvl, "no calibration sidecar found"); return(invisible(NULL)) }
  .log_detail(lvl, "calibration sidecar: ", basename(cal$source))

  sub <- character(0)
  d <- cal$calibration$depth$offset
  if (length(d) && !all(is.na(d)) && any(d != 0, na.rm = TRUE)) {
    sub <- c(sub, paste0("depth offset: ", paste(d[!is.na(d)], collapse = " / "), " m"))
  }
  a <- cal$calibration$mag$asa
  if (length(a) && !all(is.na(a))) {
    sub <- c(sub, paste0("mag sensitivity (ASA): ", paste(a[!is.na(a)], collapse = " / ")))
  }
  # sampling frequency is intentionally NOT shown here: it is inferred from the data and reported
  # earlier alongside the row count, so the calibration sidecar lists only calibration-specific fields.
  # indented, no-symbol bullets sit visually subordinate to the sidecar line above
  if (length(sub)) cli::cli_bullets(stats::setNames(sub, rep(" ", length(sub))))
  invisible(NULL)
}

#' Best-effort Wildlife Computers tag model from a `Summary.csv` in the WC directory (e.g. "MK10",
#' "SPLASH", "MiniPAT", "SPOT"). Returns NULL if no Summary.csv is present or no model column is
#' recognised, in which case the caller reports only the fix count.
#' @keywords internal
#' @noRd
.wcModel <- function(wc_dir) {
  if (is.null(wc_dir) || is.na(wc_dir) || !dir.exists(wc_dir)) return(NULL)
  f <- list.files(wc_dir, pattern = "Summary\\.csv$", full.names = TRUE, ignore.case = TRUE)
  if (!length(f)) return(NULL)
  s <- tryCatch(data.table::fread(f[1], nrows = 1, showProgress = FALSE), error = function(e) NULL)
  if (is.null(s) || !nrow(s)) return(NULL)
  cand <- grep("^(instr|instrument|model|tag.?model|device|platform)$", names(s), ignore.case = TRUE, value = TRUE)
  for (cc in cand) {
    v <- as.character(s[[cc]][1])
    # a literal "Unknown" in the Summary.csv counts as no instrument name (caller falls back to "WC")
    if (length(v) && !is.na(v) && nzchar(trimws(v)) && !identical(tolower(trimws(v)), "unknown")) return(trimws(v))
  }
  NULL
}

#' Read the Wildlife Computers archival wet/dry ('Dry') signal into an ancillary metadata entry.
#'
#' The WC `...Archive.csv` carries a boolean `Dry` column (a sustained-dry interval means the tag was
#' at the surface). Kept at native rate and transition-encoded in the tag metadata `ancillary` tier -
#' independent surface evidence for the depth zero-offset drift correction, NOT a measurand channel.
#'
#' @param archive_file Path to a WC `...Archive.csv` (or NA / a nonexistent path).
#' @return list(source, encoding, data = data.frame(datetime, dry)) for `meta$ancillary$dry`, or NULL
#'   when the file is absent or has no usable `Dry` column.
#' @keywords internal
#' @noRd
.readDryAncillary <- function(archive_file) {
  if (is.null(archive_file) || is.na(archive_file) || !file.exists(archive_file)) return(NULL)
  hdr <- tryCatch(names(data.table::fread(archive_file, nrows = 0, showProgress = FALSE)), error = function(e) NULL)
  if (is.null(hdr) || !all(c("Time", "Dry") %in% hdr)) return(NULL)
  d <- tryCatch(data.table::fread(archive_file, select = c("Time", "Dry"), showProgress = FALSE),
                error = function(e) NULL)
  if (is.null(d) || nrow(d) == 0) return(NULL)
  # WC archive timestamps are UTC (native); config/event rows carry a blank Dry and are dropped here
  t   <- as.POSIXct(d$Time, format = "%H:%M:%OS %d-%b-%Y", tz = "UTC")
  dv  <- suppressWarnings(as.integer(d$Dry))
  ok  <- !is.na(t) & !is.na(dv)
  if (!any(ok)) return(NULL)
  o   <- order(t[ok]); t <- t[ok][o]; dry <- (dv[ok] == 1L)[o]
  enc <- .transitionEncode(t, dry)
  list(source = "Wildlife Computers conductivity sensor", encoding = "transitions",
       data = data.frame(datetime = enc$datetime, dry = enc$state))
}

#' Read the complete Wildlife Computers position record (`...Locations.csv`) into an ancillary entry.
#'
#' The full fix history at its own cadence - NOT snapped to sensor rows and NOT trimmed to the recording
#' window - so downstream analyses (tracks, maps, post-deployment drift) keep every fix. Carries the
#' useful WC columns (type, quality) rather than being constrained by a fixed sensor-table layout.
#'
#' @param locations_file Path to a WC `...Locations.csv` (or NA / a nonexistent path).
#' @return list(source, data = data.frame(datetime, type, lon, lat, quality)) for
#'   `meta$ancillary$positions`, or NULL when the file is absent or lacks the required columns.
#' @keywords internal
#' @noRd
.readPositionsAncillary <- function(locations_file) {
  if (is.null(locations_file) || is.na(locations_file) || !file.exists(locations_file)) return(NULL)
  hdr <- tryCatch(names(data.table::fread(locations_file, nrows = 0, showProgress = FALSE)), error = function(e) NULL)
  if (is.null(hdr) || !all(c("Date", "Latitude", "Longitude") %in% hdr)) return(NULL)
  sel <- intersect(c("Date", "Type", "Latitude", "Longitude", "Quality"), hdr)
  d <- tryCatch(data.table::fread(locations_file, select = sel, showProgress = FALSE), error = function(e) NULL)
  if (is.null(d) || nrow(d) == 0) return(NULL)
  dt <- as.POSIXct(d$Date, format = "%H:%M:%OS %d-%b-%Y", tz = "UTC")   # WC Locations timestamps are UTC
  out <- data.frame(datetime = dt,
                    type    = if ("Type" %in% names(d)) as.character(d$Type) else NA_character_,
                    lon     = as.numeric(d$Longitude), lat = as.numeric(d$Latitude),
                    quality = if ("Quality" %in% names(d)) as.character(d$Quality) else NA_character_,
                    stringsAsFactors = FALSE)
  out <- out[!is.na(out$datetime), , drop = FALSE]
  out <- out[!duplicated(out$datetime), , drop = FALSE]
  out <- out[order(out$datetime), , drop = FALSE]
  if (!nrow(out)) return(NULL)
  list(source = "Wildlife Computers Locations", data = out)
}


#' Read the low-rate depth trace from a Wildlife Computers archive (`...-Archive.csv`).
#'
#' The WC tag logs its own pressure (depth) at a low rate alongside the wet/dry signal. This shared
#' physical channel - the same quantity the primary archival tag measures at high rate - is what lets
#' \code{.estimateClockOffset()} recover the offset between the two devices' clocks. Config/event rows
#' carry a blank Depth and are dropped here.
#'
#' @param archive_file Path to a WC `...-Archive.csv` (or NA / a nonexistent path).
#' @return data.frame(t = numeric seconds since epoch, depth) sorted by time, or NULL when the file is
#'   absent or lacks a usable Depth column.
#' @keywords internal
#' @noRd
.readArchiveDepth <- function(archive_file) {
  if (is.null(archive_file) || is.na(archive_file) || !file.exists(archive_file)) return(NULL)
  hdr <- tryCatch(names(data.table::fread(archive_file, nrows = 0, showProgress = FALSE)), error = function(e) NULL)
  if (is.null(hdr) || !all(c("Time", "Depth") %in% hdr)) return(NULL)
  d <- tryCatch(data.table::fread(archive_file, select = c("Time", "Depth"), showProgress = FALSE),
                error = function(e) NULL)
  if (is.null(d) || nrow(d) == 0) return(NULL)
  t  <- as.POSIXct(d$Time, format = "%H:%M:%OS %d-%b-%Y", tz = "UTC")   # WC archive timestamps are UTC
  dp <- suppressWarnings(as.numeric(d$Depth))
  ok <- is.finite(as.numeric(t)) & is.finite(dp)
  if (!any(ok)) return(NULL)
  o <- order(t[ok])
  data.frame(t = as.numeric(t[ok])[o], depth = dp[ok][o])
}


#' Estimate the clock offset between two depth records by depth-vs-depth cross-correlation.
#'
#' Both series measure the same physical quantity (depth), so the lag that maximises their correlation
#' is the offset between the devices' clocks. A single constant offset is estimated (drift is negligible
#' in practice). The reference is `ref_*` (the primary tag); the returned `lag` is the value in seconds to
#' \strong{add} to the `wc_*` (moving) timestamps to place them on the reference timeline. The search is
#' coarse (10 s) then refined (1 s) over `[-max.lag, +max.lag]`.
#'
#' @param ref_t,ref_d Reference time (numeric seconds) and depth (the primary archival tag).
#' @param wc_t,wc_d Moving time (numeric seconds) and depth (the Wildlife Computers archive).
#' @param control A `nautilus_alignment` control (uses `max.lag`, `min.overlap`, `min.correlation`).
#' @return list(status = "aligned"|"abstained", lag, correlation, correlation.unaligned, overlap.sec,
#'   reason). `lag` is present (best estimate) even on abstain for diagnostics.
#' @keywords internal
#' @noRd
.estimateClockOffset <- function(ref_t, ref_d, wc_t, wc_d, control) {
  abstain <- function(reason, ...) c(list(status = "abstained", reason = reason), list(...))
  okr <- is.finite(ref_t) & is.finite(ref_d); ref_t <- ref_t[okr]; ref_d <- ref_d[okr]
  okw <- is.finite(wc_t)  & is.finite(wc_d);  wc_t  <- wc_t[okw];  wc_d  <- wc_d[okw]
  if (length(ref_t) < 10L || length(wc_t) < 10L) return(abstain("no shared depth samples"))

  # overlap window shared by both records
  lo <- max(min(ref_t), min(wc_t)); hi <- min(max(ref_t), max(wc_t))
  overlap <- hi - lo
  if (!is.finite(overlap) || overlap < control$min.overlap * 60)
    return(abstain(sprintf("depth overlap %s < %g min", .fmt_duration(max(overlap, 0)), control$min.overlap),
                   overlap.sec = max(overlap, 0)))

  # decimate a very dense reference (e.g. 100 Hz over days) so the one-off interpolation stays cheap;
  # the target is only a ~1 Hz comparison grid, so a few hundred thousand reference points is ample
  if (length(ref_t) > 5e5L) { idx <- unique(round(seq(1, length(ref_t), length.out = 5e5L)))
                              ref_t <- ref_t[idx]; ref_d <- ref_d[idx] }
  g   <- seq(lo, hi, by = 2)                                   # 1 Hz is overkill; 0.5 Hz grid
  ref <- stats::approx(ref_t, ref_d, xout = g, rule = 2)$y
  if (!any(is.finite(ref)) || stats::sd(ref, na.rm = TRUE) < 0.5)
    return(abstain("reference depth too flat to align (no dives to lock onto)"))

  corAt <- function(L) {
    w <- stats::approx(wc_t + L, wc_d, xout = g, rule = 2)$y
    suppressWarnings(stats::cor(ref, w, use = "complete.obs"))
  }
  span   <- control$max.lag
  coarse <- seq(-span, span, by = 10)
  cc     <- vapply(coarse, corAt, numeric(1))
  if (!any(is.finite(cc))) return(abstain("degenerate cross-correlation"))
  bi <- which.max(cc)
  if (bi == 1L || bi == length(coarse))
    return(abstain("best lag at search edge (increase alignment max.lag)",
                   lag = coarse[bi], correlation = cc[bi], overlap.sec = overlap))
  c0   <- coarse[bi]
  fine <- seq(c0 - 9, c0 + 9, by = 1)
  ccf  <- vapply(fine, corAt, numeric(1))
  bestL <- fine[which.max(ccf)]; bestcor <- max(ccf, na.rm = TRUE)
  cor0  <- corAt(0)
  if (!is.finite(bestcor) || bestcor < control$min.correlation)
    return(abstain(sprintf("peak correlation %.2f < %.2f", bestcor, control$min.correlation),
                   lag = bestL, correlation = bestcor, correlation.unaligned = cor0, overlap.sec = overlap))
  list(status = "aligned", lag = bestL, correlation = bestcor,
       correlation.unaligned = cor0, overlap.sec = overlap, reason = NA_character_)
}


#' Align the Wildlife Computers ancillary streams onto the primary tag's clock.
#'
#' Reads the WC archive depth, estimates the constant clock offset against the primary tag's depth
#' (\code{.estimateClockOffset}), and - when the estimate is trustworthy - shifts every WC-clock stream
#' (`positions`, `dry`) by that offset so they share the primary tag's timeline. The primary sensor data
#' is the reference and is never moved. Abstains (shifting nothing) whenever there is no shared depth
#' channel or the evidence is too weak; the outcome is always recorded for provenance.
#'
#' @param sensor_data The assembled primary-tag data.table (needs `datetime` and `depth`).
#' @param positions_anc,dry_anc The WC ancillary lists (or NULL) to be shifted in place.
#' @param archive_file Path to the WC `...-Archive.csv` supplying the shared depth channel.
#' @param control A `nautilus_alignment` control.
#' @return list(positions_anc, dry_anc, info) where `info` is the provenance record stored in
#'   `meta$ancillary$alignment`.
#' @keywords internal
#' @noRd
.alignWCClocks <- function(sensor_data, positions_anc, dry_anc, archive_file, control) {
  none <- function(status, reason) list(positions_anc = positions_anc, dry_anc = dry_anc,
    info = list(method = control$method, status = status, reason = reason,
                offset.seconds = 0, correlation = NA_real_))
  if (identical(control$method, "none")) return(none("disabled", "alignment method = none"))
  if (is.null(positions_anc) && is.null(dry_anc))
    return(none("abstained", "no Wildlife Computers streams to align"))
  if (is.null(sensor_data[["depth"]]) || !any(is.finite(sensor_data$depth)))
    return(none("abstained", "primary tag has no depth channel"))
  arch <- .readArchiveDepth(archive_file)
  if (is.null(arch)) return(none("abstained", "no shared depth channel in WC archive"))

  est <- .estimateClockOffset(as.numeric(sensor_data$datetime), sensor_data$depth,
                              arch$t, arch$depth, control)
  info <- list(method = control$method, status = est$status, reason = est$reason %||% NA_character_,
               offset.seconds = if (identical(est$status, "aligned")) est$lag else 0,
               correlation = est$correlation %||% NA_real_,
               correlation.unaligned = est$correlation.unaligned %||% NA_real_,
               overlap.seconds = est$overlap.sec %||% NA_real_)
  if (!identical(est$status, "aligned")) return(c(list(positions_anc = positions_anc, dry_anc = dry_anc), list(info = info)))

  # apply the shift to every WC-clock stream (constant offset preserves the dry transition structure)
  shift <- est$lag
  n_fix <- 0L
  if (!is.null(positions_anc)) { positions_anc$data$datetime <- positions_anc$data$datetime + shift
                                 n_fix <- nrow(positions_anc$data) }
  if (!is.null(dry_anc))       dry_anc$data$datetime <- dry_anc$data$datetime + shift
  info$n_fixes_shifted <- n_fix
  list(positions_anc = positions_anc, dry_anc = dry_anc, info = info)
}


#' Report the clock-alignment outcome as one detailed-level line.
#'
#' @param info The `aln$info` provenance record from \code{.alignWCClocks}.
#' @param lvl The resolved verbosity level.
#' @keywords internal
#' @noRd
.reportAlignment <- function(info, lvl) {
  if (is.null(info) || identical(info$status, "disabled")) return(invisible())
  if (identical(info$status, "aligned")) {
    .log_detail(lvl, "clock alignment: shifted ancillary streams by ",
                sprintf("%+d s", as.integer(round(info$offset.seconds))),
                " (cor = ", sprintf("%.2f", info$correlation), ")")
  } else {
    .log_skip(lvl, "clock alignment: not applied \u2014 ", info$reason)
  }
  invisible()
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
