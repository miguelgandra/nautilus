#' Import and standardise archival tag data
#'
#' @description
#' Imports archival tag data from deployment-specific folders and converts supported raw exports into
#' the standardised data structure used by \pkg{nautilus}. Sensor channels are mapped to standardised
#' names and units, timestamps are converted to `POSIXct`, and deployment metadata are attached to each
#' imported dataset.
#'
#' The function supports multiple archival tag formats and can process a single deployment or a whole
#' collection in one call. Format-specific readers are selected through `format`, while `import.mapping`
#' defines the mapping for exports the readers do not recognise.
#'
#' Imported datasets are returned as `nautilus_tag` objects carrying sensor data, deployment metadata,
#' sensor provenance and a processing history. Alternatively they can be written to individual `.rds`
#' files and returned as paths, allowing large collections to be processed without holding the complete
#' dataset in memory.
#'
#' This is the entry point to a \pkg{nautilus} analysis, used once at the start of a study. Deployment
#' metadata should preferably be validated with [checkDeploymentMetadata()] beforehand, and the metadata
#' schema can be specified with [metadataColumns()]. Normalized identifiers and factor/numeric-text
#' coordinates are used for both validation and import. Current values are rechecked even when a QC
#' stamp is present. Mapped recovery times are retained separately from pop-up times in tag metadata.
#' For data already in R, use [buildTagData()].
#'
#' @param data.folders Character vector of paths to deployment folders, one per deployment to import.
#' @param sensor.subdirectory Subdirectory within each deployment folder holding the primary sensor
#'   files (default `"CMD"`). Change it to match your own folder layout.
#' @param format Format of the raw tag data: `"cats"` (default), `"little_leonardo"`, or `"auto"` to
#'   identify each deployment from its own files. See Details.
#' @param wc.subdirectory Subdirectory holding paired Wildlife Computers data, or `NULL` (default) to
#'   locate it automatically. Set it only where automatic detection selects the wrong folder.
#' @param timezone Source zone for unqualified logger clock strings (default `"UTC"`), as an Olson name
#'   or a fixed UTC offset such as `"+01:00"`. Explicit offsets in timestamp text take precedence.
#'   For POSIXct recording starts, this selects display only and preserves their instants. Fixed-offset
#'   clocks are stored with a UTC display; source-clock interpretation is retained separately.
#' @param ambiguous Policy for repeated civil times at a DST transition: `"error"` (default),
#'   `"earliest"`, or `"latest"`. Nonexistent times always fail the deployment's import. Prefer explicit
#'   per-row offsets for a recording spanning both occurrences of a repeated hour.
#' @param import.mapping Optional data frame defining a custom mapping between source-column names and
#'   standardised \pkg{nautilus} sensor channels, or `NULL` (default) to use the built-in
#'   format-specific mappings. See Details for the required columns and accepted values.
#' @param required.sensors Optional character vector of sensor channels a deployment must carry to be
#'   imported. `NULL` (default) accepts any record with usable timestamps and at least one recognised
#'   channel.
#' @param metadata Data frame of deployment-level metadata identifying the deployment and its animal
#'   and tag, and optionally carrying deployment times, locations and other fields recognised by
#'   [metadataColumns()]. An object returned by [checkDeploymentMetadata()] is recommended.
#' @param missing.deployments How to handle deployments present in `metadata` but absent from
#'   `data.folders`. The default, `"exclude"`, treats `metadata` as the expected study roster: missing
#'   folders are reported and recorded as import-stage exclusions. `"ignore"` treats them as outside
#'   the scope of this call, which is useful for an intentional partial import.
#' @param columns Metadata-column mapping built with [metadataColumns()] (default `metadataColumns()`).
#'   Needed only where your metadata column names differ from the package defaults.
#' @param return.data Whether to return the imported datasets in memory (default `TRUE`). When `FALSE`,
#'   the function returns the paths of the `.rds` files written to `output.dir`, which feed directly
#'   into the next step's `data` argument; this requires `output.dir` to be specified.
#' @param output.dir An existing directory in which to write one `.rds` file per successfully imported
#'   deployment. Supplying a directory is what triggers saving; `NULL` (default) writes nothing.
#' @param exclusions.file Optional path to the shared deployment-exclusion log, a CSV recording every
#'   deployment this stage set aside and why. The log holds current state, not history: each stage
#'   refreshes its own rows for the deployments in the current call, so a deployment that stops being
#'   excluded loses its row without disturbing deployments outside a partial run. Pass the same path to
#'   every stage, and to [summarizeTagData()], which uses it to report why each deployment is missing.
#'   Default `NULL`, which writes nothing.
#' @param output.suffix Optional string appended to each output file name, before `.rds`, to
#'   distinguish one import run from another. Only used when `output.dir` is specified.
#' @param compress Compression used when saving `.rds` files: `TRUE` (default, gzip), `FALSE`, or one of
#'   `"gzip"`, `"bzip2"` or `"xz"`. See [base::saveRDS()].
#' @param data.table.threads Number of threads available to \pkg{data.table}, or `NULL` (default) to
#'   leave the current setting unchanged. More threads may speed up large imports at the cost of memory.
#' @param import.sidecar Whether the sidecar paired with the primary sensor export is read and retained
#'   as provenance (default `TRUE`). Sidecars may describe the device clock, the sensor logging period,
#'   and calibration constants already applied by the exporter. See Details.
#' @param alignment Control object from [alignmentControl()] specifying how the archival tag clock is
#'   aligned with paired Wildlife Computers data. Use `alignmentControl(method = "none")` to retain the
#'   original clocks.
#' @param verbose How much detail to print: `0`/`"quiet"`, `1`/`"normal"`, or `2`/`"detailed"`
#'   (default), which reports deployment-level diagnostics.
#'
#' @details
#' ## Data import and standardisation
#'
#' Each deployment folder is read with the reader corresponding to `format`. Two formats are supported:
#' the CATS/CEiiA multi-sensor CSV export (`"cats"`), and Little Leonardo archival loggers
#' (`"little_leonardo"`, an `_A.txt` acceleration file with an optional `_DT.txt` depth and temperature
#' file). Little Leonardo loggers record no clock, so they additionally require a `data_start` entry in
#' the metadata giving the recording start; see [metadataColumns()].
#'
#' With `format = "auto"` the reader is selected separately for each deployment from its own files.
#' Detection does not guess: a folder matching two readers, or none, is reported rather than assumed, so
#' a misidentified deployment fails visibly instead of being read as the wrong make. Only stock exports
#' are recognised; an export needing `import.mapping` is reported as unidentifiable and should be named
#' explicitly.
#'
#' To import several tag makes in one call, either use `"auto"` or add a `tag_format` column to the
#' metadata. A per-deployment value takes precedence over `format`, and the two combine: `"auto"` with a
#' `tag_format` column naming only some deployments honours those and detects the rest.
#'
#' ## Sensor channels and units
#'
#' Recognised sensor channels are mapped to a common \pkg{nautilus} vocabulary:
#'
#' \describe{
#'   \item{`datetime`}{Timestamp of each observation, as `POSIXct`.}
#'   \item{`ax`, `ay`, `az`}{Tri-axial acceleration, in g.}
#'   \item{`gx`, `gy`, `gz`}{Tri-axial angular velocity, in rad/s, where a gyroscope is present.}
#'   \item{`mx`, `my`, `mz`}{Tri-axial magnetic field, in microtesla, where a magnetometer is present.}
#'   \item{`depth`}{Depth, in metres.}
#'   \item{`temp`}{Water temperature, in degrees Celsius, where a suitable channel is available.}
#'   \item{`paddle_freq`}{Paddle-wheel rotation frequency, in Hz, where the tag recorded one.}
#'   \item{`paddle_speed`}{Paddle-wheel swimming speed, in m/s, where the logger recorded a speed
#'     directly rather than a rotation rate.}
#' }
#'
#' Format-specific column names and units are converted to this representation, and the original
#' sampling rate is preserved. Unrecognised or explicitly excluded channels are not imported.
#'
#' Data are imported in the tag's own axis frame. Rotating them into an animal-centred frame is a
#' separate, explicit step, performed by [applyAxisMapping()] and optionally checked against the data
#' with [checkTagMapping()]. Orientation metrics computed before that step will not describe the animal.
#'
#' ## Metadata
#'
#' The `metadata` argument supplies deployment-level information associated with each imported dataset:
#' the deployment identifier, tag information, deployment, recovery and pop-up events and locations, and any other
#' fields defined through [metadataColumns()].
#'
#' Metadata are preferably supplied as an object returned by [checkDeploymentMetadata()], which carries
#' the metadata-column schema and a quality-control verdict. A plain data frame is also accepted and is
#' checked during import.
#'
#' Metadata fields are stored separately from the sensor time series and are carried through subsequent
#' processing stages.
#'
#' By default `metadata` is also the expected import roster. A metadata deployment with no matching
#' folder in `data.folders` is reported and recorded as excluded with the reason that no raw-data folder
#' was supplied. This records absence without guessing why the tag was unavailable. For an intentional
#' partial import, set `missing.deployments = "ignore"` or subset `metadata` to the selected folders.
#'
#' ## Time handling
#'
#' POSIXct timestamps identify absolute instants. Raw clock strings without an offset are interpreted
#' once in `timezone`; offset-bearing ISO timestamps (Z, +HH:MM or +HHMM) already identify instants and
#' keep them. `data_start` for clock-less loggers is also an absolute POSIXct instant: samples are
#' generated by adding elapsed seconds, including through daylight-saving transitions.
#'
#' For unqualified clock strings, choose the clock the logger actually used. A fixed offset such as
#' `"+01:00"` does not observe DST; `"Europe/Lisbon"` does. Repeated times require an explicit policy;
#' missing, invalid, nonexistent or unsupported clock readings fail the deployment rather than being
#' partially parsed or silently removed. Raw layouts are year-month-day (hyphens or slashes), with an
#' optional `HH:MM[:SS.fraction]` time, and CATS `day.month.year + HH:MM:SS.fraction`. ISO timestamps may use T.
#' Leading/trailing whitespace is accepted; unrelated trailing text is not.
#'
#' The stored column and `meta$sensors$timezone` share a display zone. `meta$sensors$clock` separately
#' retains source zones, explicit offsets, ambiguity policy and per-file interpretation records. No
#' artificial epsilon is added to sample times. Existing saved objects are read without rewriting epochs
#' or inventing missing clock provenance. To reinterpret incorrectly labelled in-memory POSIXct clocks
#' deliberately, use [buildTagData()] with `time.mode = "clock"`.
#'
#' Explicit sensor-clock information in the source header or sidecar `[logging]` block is checked against
#' `timezone` and retained as provenance. A `[device]` UTC offset is kept separate: it may describe the
#' camera clock used by video file names or overlays rather than the exported sensor timestamps. Neither
#' source causes observations to be shifted silently.
#'
#' ## Custom column mappings
#'
#' Non-standard exports can be imported through `import.mapping`, a data frame with one row per channel
#' to import and three columns:
#'
#' \describe{
#'   \item{`colname`}{The column name in the source file.}
#'   \item{`sensor`}{The \pkg{nautilus} channel it supplies: one of `"datetime"`, `"date"`, `"time"`,
#'     `"ax"`, `"ay"`, `"az"`, `"gx"`, `"gy"`, `"gz"`, `"mx"`, `"my"`, `"mz"`, `"depth"`, `"temp"`,
#'     `"paddle_freq"` or `"paddle_speed"`.}
#'   \item{`units`}{The units the source column is in: one of `"UTC"`, `"m/s2"`, `"g"`, `"mrad/s"`,
#'     `"rad/s"`, `"deg/s"`, `"uT"`, `"C"`, `"m"`, `"Hz"`, `"m/s"`, or `""` for a dimensionless
#'     channel.}
#' }
#'
#' A user-supplied mapping takes precedence over the built-in ones, so an export the readers do not
#' recognise can be imported without modifying the package's defaults.
#'
#' ## Required sensors
#'
#' By default a deployment is imported when it carries usable timestamps and at least one recognised
#' sensor channel, so accelerometer-only and depth-and-temperature-only tags enter the common workflow
#' normally.
#'
#' `required.sensors` imposes stricter requirements at import. Naming the channels an analysis cannot
#' proceed without, for example `c("depth", "temp")`, causes deployments lacking either to be rejected
#' at import rather than partway through the pipeline.
#'
#' ## Temperature channels
#'
#' Only water-temperature channels are mapped to `temp`. Tag-electronics channels, such as those on the
#' magnetometer chip or the IMU board, report the temperature inside the housing rather than the
#' animal's environment, and are never mapped automatically. A deployment whose only temperature channel
#' is one of these is imported without `temp`, and the discard is reported.
#'
#' Mapping such a channel explicitly through `import.mapping` overrides this, and is still flagged in
#' the import diagnostics.
#'
#' ## Sidecar provenance
#'
#' With `import.sidecar = TRUE`, information accompanying the primary export is recorded under
#' `meta$sidecar` where available. This includes separate `device` and `logging` clock metadata plus any
#' calibration constants. Calibration constants document processing already performed by the tag
#' firmware and are not re-applied during import. Clock metadata are used only for validation and
#' provenance; they never trigger a silent time shift.
#'
#' ## Paired Wildlife Computers tags
#'
#' Where a deployment also carries a Wildlife Computers tag (MiniPAT, MK10, SPOT), its position records
#' and wet/dry events are read and attached to the deployment metadata as ancillary streams. Video
#' coverage is not read here; see [getVideoMetadata()].
#'
#' The two tags keep independent clocks, so `alignment` controls whether they are aligned before the
#' records are combined. The alignment metadata are retained, so the offset applied, or the decision not
#' to align, remains traceable. Pass `alignmentControl(method = "none")` to retain the original clocks.
#'
#' ## Output and large datasets
#'
#' Deployments are read one at a time, so memory use does not grow with the size of the collection.
#' Supplying `output.dir` writes one `.rds` file per successfully imported deployment; adding
#' `return.data = FALSE` returns those paths instead of the data, keeping the whole collection out of
#' memory. The returned paths can be passed directly to subsequent \pkg{nautilus} functions.
#'
#' For records of tens of millions of rows, `compress = FALSE` removes the compression step, which can
#' otherwise dominate the import. The files are roughly 1.5 to 2 times larger; their contents are
#' identical, and `readRDS()` reads either.
#'
#' ## Processing history
#'
#' Each imported dataset records an `importTagData` processing step giving the source directory, the
#' columns imported and the time zone applied. This provenance travels with the deployment metadata and
#' can be used to trace the origin and processing history of the dataset.
#'
#' ## Quality control and skipped deployments
#'
#' Structural and metadata checks run before and during import. A deployment may be skipped or rejected
#' where sensor files are missing, the source data cannot be read, required sensors are unavailable,
#' positions fall outside the recording period, or other import requirements are not met.
#'
#' Diagnostics are reported as the import runs and collected in the closing summary. With `verbose = 0`
#' the same issues are raised as R warnings instead of console messages, so that scripted callers can
#' capture them.
#'
#' @return When `return.data = TRUE`, a named list of `nautilus_tag` objects, one per successfully
#'   imported deployment, each carrying its standardised sensor data, deployment metadata, sensor
#'   provenance and processing history. When `return.data = FALSE`, a character vector of the paths to
#'   the `.rds` files written to `output.dir`.
#'
#' @seealso [checkDeploymentMetadata()] for validating deployment metadata; [metadataColumns()] for the
#'   metadata-column schema; [buildTagData()] for constructing `nautilus_tag` objects from data already
#'   in R; [filterDeploymentData()] for trimming each record to the deployment period;
#'   [applyAxisMapping()] for mapping sensor axes to the animal's body frame; [processTagData()] for
#'   deriving movement and orientation metrics.
#'
#' @examples
#' \dontrun{
#' # Validate the deployment metadata before importing against it.
#' meta <- checkDeploymentMetadata(raw_metadata)
#'
#' # Import a collection of CATS deployments.
#' tags <- importTagData(data.folders = list.dirs("./tags", recursive = FALSE),
#'                       metadata = meta,
#'                       timezone = "UTC")
#'
#' # Import an intentional subset without treating the remainder of the roster as missing.
#' subset_tags <- importTagData(data.folders = selected_folders,
#'                              metadata = meta,
#'                              missing.deployments = "ignore")
#'
#' # An archive holding more than one tag make: identify each deployment from its own files.
#' tags <- importTagData(data.folders = list.dirs("./tags", recursive = FALSE),
#'                       metadata = meta,
#'                       format = "auto")
#'
#' # A collection too large to hold in memory: write each deployment to disk and pass the paths on.
#' files <- importTagData(data.folders = list.dirs("./tags", recursive = FALSE),
#'                        metadata = meta,
#'                        output.dir = "./imported",
#'                        return.data = FALSE)
#' }
#' @export
importTagData <- function(data.folders,
                          sensor.subdirectory = "CMD",
                          format = "cats",
                          wc.subdirectory = NULL,
                          timezone = "UTC",
                          import.mapping = NULL,
                          required.sensors = NULL,
                          metadata,
                          missing.deployments = "exclude",
                          columns = metadataColumns(),
                          return.data = TRUE,
                          output.dir = NULL,
                          exclusions.file = NULL,
                          output.suffix = NULL,
                          compress = TRUE,
                          data.table.threads = NULL,
                          import.sidecar = TRUE,
                          alignment = alignmentControl(),
                          verbose = "detailed",
                          ambiguous = c("error", "earliest", "latest")) {

  ambiguous <- match.arg(ambiguous)

  # A QC'd metadata table (a nautilus_deployments from checkDeploymentMetadata) is authoritative about its
  # own column names and carries a QC verdict: adopt its embedded schema and keep the stamp (reported in
  # the header; a FAILED verdict is rejected before the long read). Current values are also checked below.
  qc_stamp <- NULL
  if (is_nautilus_deployments(metadata)) {
    qc_stamp <- attr(metadata, "nautilus.qc")
    columns  <- attr(metadata, "nautilus.columns") %||% columns
    class(metadata) <- "data.frame"
  }

  # resolve the metadata column schema, then unpack into the local names used below
  columns <- .as_metadata_columns(columns)
  if (!is.data.frame(metadata)) .abort("{.arg metadata} must be a data.frame or data.table.")
  # Use the normalized values for matching and assembly, not just for a discarded QC copy.
  normalized_metadata <- .qcNormalizeMetadata(metadata, columns)
  metadata <- normalized_metadata$data
  columns <- normalized_metadata$schema
  id.col              <- columns$id
  tag.model.col       <- columns$tag_model
  tag.type.col        <- columns$tag_type
  deploy.date.col     <- columns$deploy_datetime
  recovery.date.col   <- columns$recovery_datetime
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
  animal.id.col       <- columns$animal_id                     # the ANIMAL; `id` identifies the DEPLOYMENT
  deploy.site.col     <- columns$deploy_site                   # tagging site NAME (coordinates are separate)


  ##############################################################################
  # Initial checks #############################################################
  ##############################################################################

  # measure running time
  start.time <- Sys.time()

  # verbosity level (0 quiet / 1 normal / 2 detailed); also validates `verbose`
  lvl <- .verbosity(verbose)

  # Strict by default: metadata describes the expected cohort. An intentional partial run can opt out
  # without having to alter the authoritative metadata table.
  missing.deployments <- match.arg(missing.deployments, c("exclude", "ignore"))

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
  # The typed collector is initialised up front so the on.exit renderer is robust even if the run errors
  # early. A record keeps its machine-readable category/reason and raw evidence; presentation is deferred
  # to the inline, final-cli, or quiet-warning renderer appropriate to that output channel.
  import_issues <- list()
  add_issue <- function(category, id, reason_code = NA_character_, details = list(), key = id) {
    import_issues[[length(import_issues) + 1L]] <<-
      .newImportIssue(category, id, reason_code, details, key)
    invisible(NULL)
  }
  missing_deployment_ids <- character(0)                            # metadata rows with no supplied folder
  # the reason travels with the id, for the shared exclusions log
  failed_rows <- list()
  note_fail <- function(id, reason) {
    failed_rows[[length(failed_rows) + 1L]] <<- .exclusionsRow(id, "importTagData", reason)
  }
  if (lvl == 0L) on.exit(.warnImportIssues(import_issues), add = TRUE)

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
  # fail on an unknown reader before anything is read. "auto" is legal HERE only - never as a `tag_format`
  # column value, which names a reader.
  .checkFormat(format, "format", allow = "auto")
  .assert_string(wc.subdirectory, "wc.subdirectory", null_ok = TRUE)
  .assert_dir(output.dir, "output.dir", null_ok = TRUE)
  .assert_writable_file(exclusions.file, "exclusions.file", ext = "csv", null_ok = TRUE)

  # the sole illegal output request: keep nothing and write nowhere
  .assert_flag(return.data, "return.data")
  .assert_flag(import.sidecar, "import.sidecar")
  .assert_compress(compress)
  .assert_output(return.data, output.dir)

  # validate `metadata`
  if (!is.data.frame(metadata)) .abort("{.arg metadata} must be a data.frame or data.table.")
  if (inherits(metadata, "data.table")) metadata <- as.data.frame(metadata)

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

  # Folder basenames are deployment identifiers throughout import. Keep them positional: two paths may
  # legitimately share a basename, and the reader/discovery state for one must never mask the other.
  folder_ids <- basename(data.folders)

  # check if deployment info exists for all individuals. In strict mode the whole metadata roster is in
  # scope (including rows whose raw folder is absent); ignore mode deliberately limits QC to the selected
  # folders, preserving the established partial-import behaviour.
  metadata_ids <- as.character(metadata[[id.col]])
  processing_ids <- if (identical(missing.deployments, "exclude")) metadata_ids else folder_ids
  processing_metadata <- metadata[metadata[[id.col]] %in% processing_ids, ]
  missing_deploy_idx <- is.na(processing_metadata[[deploy.date.col]]) | is.na(processing_metadata[[deploy.lon.col]]) | is.na(processing_metadata[[deploy.lat.col]])
  missing_deploy_ids <- processing_metadata[missing_deploy_idx, id.col]
  if (length(missing_deploy_ids) > 0) {
    .abort(c("Some IDs are missing deployment info (datetime, lon, or lat) in {.arg metadata}:",
             "x" = "{.val {missing_deploy_ids}}"))
  }

  # timezone
  .timeZoneSpec(timezone)

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

  # validate folder animal IDs against metadata
  missing_ids <- setdiff(folder_ids, metadata_ids)
  if (length(missing_ids) > 0) {
    .abort(c("Some folder IDs were not found in {.arg metadata}: {.val {missing_ids}}.",
             "i" = "Add these IDs to {.arg metadata}, or drop the folders from {.arg data.folders}."))
  }

  # Reverse match: in the default strict mode every valid metadata ID is an expected deployment. This is
  # observational only - absence of a folder cannot establish whether a tag was lost, unrecovered, or
  # archived elsewhere - so the exclusion reason states exactly what the importer could verify.
  valid_metadata_ids <- unique(metadata_ids[!is.na(metadata_ids) & nzchar(metadata_ids)])
  if (identical(missing.deployments, "exclude")) {
    missing_deployment_ids <- setdiff(valid_metadata_ids, folder_ids)
    if (length(missing_deployment_ids)) {
      for (id in missing_deployment_ids) {
        add_issue("missing_data", id)
        note_fail(id, "no matching raw data folder in data.folders")
      }
    }
  }
  exclusion_scope_ids <- if (identical(missing.deployments, "exclude")) valid_metadata_ids else folder_ids

  # resolve each deployment's raw format: the `tag_format` metadata column wins (so one call can mix tag
  # makes), otherwise the `format` argument applies to all. Both are explicit and QC'd - nothing is sniffed
  # unless the user asks for it with `format = "auto"` (below).
  ids_in <- basename(data.folders)
  fmt_by_folder <- stats::setNames(rep(format, length(data.folders)), ids_in)
  if (!is.null(tag.format.col)) {
    col_vals <- character(0)
    for (k in seq_along(ids_in)) {
      v <- metadata[[tag.format.col]][metadata[[id.col]] == ids_in[k]]
      if (length(v) && !is.na(v[1]) && nzchar(as.character(v[1]))) {
        fmt_by_folder[k] <- as.character(v[1])
        col_vals <- c(col_vals, as.character(v[1]))
      }
    }
    # validate the values that came FROM THE COLUMN, not the merged vector: `fmt_by_folder` was seeded from
    # `format`, so blaming the column for an argument-sourced value would misattribute the error (and
    # `format = "auto"` is legal here while a `tag_format` of "auto" is not).
    .checkFormat(col_vals, "columns$tag_format")
  }

  readers <- .readerFormats()

  # `format = "auto"`: sniff ONLY the folders nobody named. Restricting detection to the still-"auto"
  # entries is what implements the documented precedence (tag_format column > format= > detection) - and it
  # gives per-deployment auto for free: a column naming only some rows leaves exactly the rest to detection.
  auto_present <- stats::setNames(rep(FALSE, length(data.folders)), ids_in)
  ambiguous_folders <- character(0)
  for (k in which(fmt_by_folder == "auto")) {
    # every probe is wrapped, not just the ones `detect` composes: a reader whose `has_data` throws on a
    # malformed folder must not take the whole run down here either.
    fires <- function(which_fn) {
      names(readers)[vapply(readers, function(r) {
        isTRUE(tryCatch(which_fn(r)(data.folders[k], sensor.subdirectory), error = function(e) FALSE))
      }, logical(1))]
    }
    matched <- fires(function(r) r$detect)
    if (length(matched) == 1L) {
      fmt_by_folder[k] <- matched
    } else if (length(matched) > 1L) {
      ambiguous_folders <- c(ambiguous_folders, sprintf("%s: matched %s", ids_in[k], paste(
        sprintf("%s (%s)", matched, vapply(readers[matched], function(r) r$label, character(1))),
        collapse = " and ")))
    } else {
      # nobody claimed it. `has_data` separates the two very different reasons, which must not be
      # conflated: files ARE here but no reader recognises them (a real per-folder failure, reported in
      # the loop below) vs there is nothing here at all (the existing quiet pre-flight skip).
      auto_present[k] <- length(fires(function(r) r$has_data)) > 0L
      fmt_by_folder[k] <- NA_character_
    }
  }
  if (length(ambiguous_folders)) {
    # the bullets are DATA (they carry folder ids), but cli hands every string to glue - so a deployment
    # named "PIN_{01}" would be evaluated and reported as "PIN_1", mangling the one thing this abort exists
    # to name. Escape the braces rather than trusting ids to be glue-safe.
    n_amb <- length(ambiguous_folders)
    esc <- gsub("}", "}}", gsub("{", "{{", ambiguous_folders, fixed = TRUE), fixed = TRUE)
    .abort(c("{.code format = \"auto\"} matched more than one reader for {n_amb} folder{?s}.",
             stats::setNames(esc, rep("x", n_amb)),
             "i" = "Auto-detection never guesses between readers.",
             "i" = "Set {.arg format} explicitly, or map the {.field tag_format} role in {.fn metadataColumns} to name the reader per deployment."))
  }
  # POSTCONDITION: no element of fmt_by_folder is "auto" - each is a reader name or NA. Everything
  # downstream (the pre-flight probe, the label lookup, the dispatch switch) indexes `readers` by these
  # values and would fail obscurely on a leaked "auto".

  # does each folder hold data its reader can read? Discovery is format-specific (a ".csv under
  # sensor.subdirectory" is only true of CATS), so ask the reader rather than assuming a layout.
  data_files <- vapply(seq_along(data.folders), function(k) {
    fk <- fmt_by_folder[[k]]
    # NA = auto recognised no reader. Files-but-unidentifiable is NOT a "missing" folder: calling it one
    # would report "no sensor file found" for a folder full of data the user can see.
    if (is.na(fk)) return(isTRUE(auto_present[[k]]))
    isTRUE(readers[[fk]]$has_data(data.folders[k], sensor.subdirectory))
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
    # drop the NAs an all-auto run leaves behind: they have no reader, so they have no label to name and
    # nothing useful to echo back as "the format you asked for".
    fmts <- as.character(stats::na.omit(unique(unname(fmt_by_folder))))
    if (!length(fmts)) {
      .abort(c("No readable tag data was found in any of the {length(data.folders)} {.arg data.folders}.",
               "i" = "{.code format = \"auto\"} found no primary sensor data in any folder: no reader could find its files.",
               "i" = "Supported formats: {.val {names(readers)}}. Set {.arg format} explicitly if these are a supported make with custom column names (see {.arg import.mapping})."))
    }
    fmt_lab <- paste(unique(vapply(fmts, function(f) readers[[f]]$label, character(1))), collapse = " / ")
    .abort(c("No readable {fmt_lab} data was found in any of the {length(data.folders)} {.arg data.folders}.",
             "i" = "Check {.arg format} ({.val {fmts}}) matches your tags.",
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

  # the built-in CATS + CEiiA dialects (R/utils-import.R - the reader owns its own header knowledge, and
  # `format = "auto"` resolves headers through this same table)
  default_mappings <- .defaultMappings()

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
  # if FAILED); a plain data.frame is screened inline by the same engine as checkDeploymentMetadata(),
  # restricted to the deployments being imported. Errors abort before the long read.
  qc_guard_inline <- is.null(qc_stamp)
  # Recheck current values even with a saved QC stamp: metadata tables may have been edited.
  g <- rbind(normalized_metadata$notes,
             .runMetadataQC(processing_metadata, normalized_metadata$roles))
  in_scope <- is.na(g$id) | g$id %in% processing_ids
  qc_guard_err <- g[g$severity == "error" & in_scope, , drop = FALSE]

  # header: a framed block, visually isolated from the per-individual sections that follow
  hdr_bullets <- sprintf("Input: %d folder%s", n_animals, if (n_animals != 1) "s" else "")
  n_expected <- length(exclusion_scope_ids)
  if (identical(missing.deployments, "exclude") && n_expected != n_animals)
    hdr_bullets <- c(hdr_bullets, sprintf("Expected: %d metadata deployment%s",
                                          n_expected, if (n_expected != 1L) "s" else ""))
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
  missing_folder_idx <- which(is.na(data_files))
  if (length(missing_folder_idx) > 0L) {
    for (k in missing_folder_idx) {
      add_issue("skipped_folder", names(data_files)[k], key = data.folders[k])
    }
  }
  # the "QC not run" notice is a status line, not a data-quality issue: shown at lvl >= 1 only, and
  # deliberately kept out of the deferred-warning collector (headless callers get the abort instead).
  qc_notice <- if (qc_guard_inline) "metadata QC not run \u2014 validating inline" else NULL
  preflight_messages <- .formatImportPreflight(import_issues)
  if (lvl >= 1L && (length(preflight_messages) || !is.null(qc_notice))) {
    for (it in preflight_messages) cli::cli_alert_warning("Pre-flight: {it}")
    if (!is.null(qc_notice)) cli::cli_alert_warning("Pre-flight: {qc_notice}")
    cli::cli_text("")                                   # blank line before the first individual block
  }

  # reject metadata with blocking errors before committing to the (potentially long) import
  if (!is.null(qc_stamp) && isFALSE(qc_stamp$passed)) {
    .abort(c("{.arg metadata} failed metadata QC ({qc_stamp$n_errors} error{?s}).",
             "i" = "Inspect them with {.code issues()} and fix the metadata before importing."))
  }
  if (nrow(qc_guard_err) > 0L) {
    .abort(c("Metadata QC found {nrow(qc_guard_err)} blocking error{?s} in the deployments being imported:",
             stats::setNames(qc_guard_err$message, rep("x", nrow(qc_guard_err))),
             "i" = "Run {.code checkDeploymentMetadata()} to diagnose, or fix the metadata."))
  }


  ##############################################################################
  # Import data for each folder ################################################
  ##############################################################################

  # iterate over each animal. Per-run issue trackers (counts + affected IDs) feed the final summary.
  n_done <- 0L                                        # issue collectors are initialised at the top
  imported_ids <- character(0)                        # deployment count stays unique across duplicate basenames
  tot_rows <- 0; tot_secs <- 0                        # cohort volume, for the summary line
  for (i in seq_along(data_files)) {

    # get animal ID
    id <- basename(data.folders)[i]

    # per-individual sub-header (level-2 only; groups this individual's detail lines)
    .log_h2(lvl, sprintf("%s (%d/%d)", id, i, n_animals))

    # retrieve the WC locations file path for the current animal
    locations_file <- wc_files[i]

    # retrieve metadata for the current animal ID from the metadata table
    animal_info <- metadata[metadata[[id.col]]==id,]


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
    # timestamps, drop excluded channels, read the paired sidecar and convert to canonical units.
    # read_cats() RETURNS its per-deployment status and report data rather than printing or collecting:
    # the collectors below are locals read by an on.exit closure, so a write from inside another frame
    # would silently vanish, and the report has to interleave with metadata lines only known out here.
    # dispatch on this deployment's resolved format. A simple switch, not a registry: readers are
    # internal, so adding one is a reader file + a line here + an entry in .readerFormats().
    fmt_i <- fmt_by_folder[[i]]

    # `format = "auto"` left this folder unresolved (NA). Every folder reaches this loop - there is no
    # earlier gate - and `switch(NA_character_, ...)` falls through to the default, so without this guard an
    # unidentified folder would abort the WHOLE run with "Unsupported tag format: NA". Mirrors the reader's
    # own skip/fail block below, so the tally and the deferred verbose=0 warning pick it up unchanged.
    if (is.na(fmt_i)) {
      # `is.na(data_files[[i]])`, NOT `id %in% missing_folders`: the two states are per-FOLDER, but
      # `missing_folders` holds basenames. Two folders sharing a basename (this project has both
      # data/PIN_01 and _archive/PIN_01) would let an empty one's id silence the other's failure into
      # "no sensor file found" - the exact lie this split exists to prevent. `data_files` is positional.
      if (is.na(data_files[[i]])) {                  # nothing here at all: the pre-flight notice covers it
        .log_skip(lvl, id, " skipped ", cli::symbol$bullet, " no sensor file found")
      } else {                                       # files ARE here, none identifiable: a real failure
        if (lvl >= 1L) {
          cli::cli_alert_danger(paste0("{id}: could not identify the tag format ({.code format = \"auto\"}); ",
                                       "set {.arg format} or the {.field tag_format} metadata column. Data not imported."))
        }
        add_issue("failed_import", id, "unidentified_format",
                  details = list(reason = "Could not identify the tag format"))
        note_fail(id, "could not identify the tag format")
      }
      data_list[[i]] <- NA
      .log_gap(lvl)
      next
    }

    res <- switch(
      fmt_i,
      cats = read_cats(
        folder = data.folders[i], sensor.subdirectory = sensor.subdirectory,
        active_mapping = active_mapping, active_mapping_norm = active_mapping_norm,
        temp_blacklist_norm = temp_blacklist_norm,
        required.sensors = required.sensors, timezone = timezone,
        import.sidecar = import.sidecar, exclude.channels = exclude_want,
        verbose = lvl >= 2L, ambiguous = ambiguous),
      little_leonardo = read_little_leonardo(
        folder = data.folders[i], sensor.subdirectory = sensor.subdirectory,
        # the raw carries no clock: the recording start comes from the `data_start` metadata role
        start = if (!is.null(data.start.col)) animal_info[[data.start.col]][1] else NULL,
        timezone = timezone, exclude.channels = exclude_want, verbose = lvl >= 2L),
      .abort("Unsupported tag format: {.val {fmt_i}}."))

    # skip this individual if no usable sensor data could be assembled. A "no sensor CSV" folder that
    # was already flagged pre-flight (missing_folders) is covered there - just a quiet inline skip, no
    # double-counting. Anything else (corrupt file, missing required sensors, or a no-CSV not caught
    # pre-flight) is a genuine per-deployment failure -> collected as a typed import issue.
    if (is.null(res$data)) {
      # "no data here" reads differently per format ("no sensor CSV ..." / "no sensor file: ...");
      # either way, a folder the pre-flight already flagged is a quiet skip, not a second failure.
      preflight_covered <- (startsWith(res$reason, "no sensor CSV") ||
                            startsWith(res$reason, "no sensor file")) && is.na(data_files[[i]])
      if (preflight_covered) {
        .log_skip(lvl, id, " skipped ", cli::symbol$bullet, " ", res$reason)
      } else {
        if (lvl >= 1L) cli::cli_alert_danger("{id}: {res$reason}. Data not imported.")
        add_issue("failed_import", id, "reader_failure", details = list(reason = res$reason))
      }
      note_fail(id, res$reason)
      data_list[[i]] <- NA
      .log_gap(lvl)
      next
    }

    sensor_data       <- res$data
    file_mapping      <- res$mapping
    selected_cols     <- res$selected_cols
    sidecar_info      <- res$sidecar_info
    excluded_channels <- res$excluded
    temp_status       <- res$temp_status
    reader_ancillary  <- res$ancillary          # primary-tag ancillary streams the reader parsed in-band
    rows <- nrow(sensor_data)

    # collect the reader's per-deployment issues. Temperature: the reliable `temp` is the pressure-sensor
    # thermistor; electronics channels (IMU board / magnetometer chip) are blacklisted, so `temp` is left
    # unset (missing-sensor convention) when the only temperature column is blacklisted, while an explicit
    # user override is honoured but flagged. Clock findings distinguish the sensor recording zone from
    # a different device/camera clock; neither condition causes timestamps to be shifted.
    # Surfaced inline (lvl >= 2) below, echoed in the final tally (lvl >= 1), deferred warning (lvl == 0).
    if (identical(temp_status, "blacklisted_only")) add_issue("temperature_discarded", id)
    else if (identical(temp_status, "override"))    add_issue("temperature_override", id)
    if (isTRUE(res$timezone_mismatch)) {
      add_issue("sensor_timezone", id, details = list(note = res$timezone_note))
    }
    if (isTRUE(res$device_clock_mismatch)) {
      add_issue("device_clock", id, details = list(
        logging_utc_offset = res$logging_utc_offset,
        device_utc_offset = res$device_utc_offset,
        suggested_video_shift_h = res$suggested_video_shift_h
      ))
    }
    # Columns the raw files declare but the reader does not read (e.g. a Little Leonardo compass or
    # propeller channel). Not a fault in the data and not a reason to refuse the import - the channels
    # that WERE read are correct - but it must not pass silently, or a user loses a sensor without
    # being told. Same three-tier route as the issues above: inline, tally, deferred warning.
    if (length(res$unread_columns)) {
      add_issue("unread_channels", id,
                details = list(columns = paste(res$unread_columns, collapse = "; ")))
    }

    if (lvl >= 2L && length(excluded_channels)) {
      .log_skip(lvl, "excluded sensors (metadata): ",
                paste(.channelsToFamilies(excluded_channels), collapse = ", "))
    }
    if (lvl >= 2L && length(res$unread_columns)) {
      .log_skip(lvl, "unread channels: ", paste(res$unread_columns, collapse = "; "))
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

    # Sidecar provenance and clock checks. Calibration constants are retained but never re-applied.
    # Sensor/source and device/logging discrepancies are separate diagnostics and neither shifts data.
    if (isTRUE(import.sidecar)) .reportSidecar(sidecar_info, lvl)
    if (lvl >= 2L && !is.null(res$timezone_note)) .log_skip(lvl, res$timezone_note)
    if (lvl >= 2L && isTRUE(res$device_clock_mismatch)) {
      logging <- .formatClockOffset(res$logging_utc_offset)
      device <- .formatClockOffset(res$device_utc_offset)
      if (identical(logging, "0h")) {
        .log_skip(lvl, "[logging] is UTC but [device] offset is ", device, ".")
      } else {
        .log_skip(lvl, "[logging] offset is ", logging, " but [device] offset is ", device, ".")
      }
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
    # attaches to the assembled tag, so no primary reader ever learns about it (see R/utils-import.R). The
    # complete TRACKING-fix record (FastGPS/Argos; the deploy/pop-up "User" rows are dropped - they live
    # in meta$deployment) is kept as an independent ancillary stream at its own cadence - NOT snapped to
    # sensor rows and NOT trimmed to the recording window - so the full fix history survives downstream
    # (tracks, maps, post-deployment surface drift). Deploy/pop-up positions live in meta$deployment.
    wc_anc <- read_wc(locations_file, archive_files[i])

    # Identify the co-deployed device. The serial from the .wch filename is the only identifier present on
    # every deployment (the Summary.csv model/PTT are often "Unknown"), so it anchors the label; the model
    # enriches it when known. Computed unconditionally so it is stored in metadata regardless of verbosity.
    wc_model <- wc_serial <- NULL
    if (length(wc_anc)) {
      wc_dir <- if (!is.na(locations_file)) dirname(locations_file) else
                if (!is.na(archive_files[i])) dirname(archive_files[i]) else NA_character_
      wc_model  <- .wcModel(wc_dir)
      wc_serial <- .wcSerial(wc_dir)
    }
    # verbose (level 2): one "ancillary tag" line, with each stream that device contributed nested beneath.
    if (lvl >= 2L && length(wc_anc)) {
      .log_detail(lvl, "ancillary tag: Wildlife Computers ",
                  if (!is.null(wc_model)) wc_model else "tag",
                  if (!is.null(wc_serial)) paste0(" (", wc_serial, ")") else "")
      if (!is.null(wc_anc$positions))
        .log_subdetail(lvl, "locations: ", .fmtFixTypes(wc_anc$positions$data$type))
      if (!is.null(wc_anc$dry)) {
        n_dry <- sum(wc_anc$dry$data$dry, na.rm = TRUE)
        .log_subdetail(lvl, "wet/dry: ", formatC(n_dry, format = "d", big.mark = ","),
                       " surface interval", if (n_dry != 1L) "s" else "")
      }
    }

    # Align the co-deployed device's clock onto this tag's timeline and attach. attachAncillary() is the
    # single home for that alignment; it abstains and shifts nothing when the evidence is weak.
    att <- attachAncillary(sensor_data, wc_anc, align = alignment)
    # report only when there was actually a stream to shift: the aligner also records an abstention for
    # tags with no co-deployed device, and that is provenance, not something to narrate.
    # Collect the abstentions for the SUMMARY, at EVERY verbosity - the inline amber line is only
    # emitted at verbose = 2 and scrolls away in a long run, yet "which tags failed to align?" is
    # exactly the question asked at the end. Collection is independent of whether it was narrated.
    if (length(wc_anc) && !is.null(att$info) &&
        !identical(att$info$status, "disabled") && !identical(att$info$status, "aligned")) {
      add_issue("clock_alignment", id, att$info$reason_code %||% "unspecified",
                details = list(
                  reason = att$info$reason %||% "unspecified",
                  correlation = att$info$correlation %||% NA_real_,
                  threshold = alignment$min.correlation,
                  overlap_seconds = att$info$overlap.seconds %||% NA_real_,
                  minimum_minutes = alignment$min.overlap
                ))
    }
    if (lvl >= 2L && length(wc_anc)) .reportAlignment(att$info, lvl)

    # primary-tag video coverage (e.g. the Little Leonardo in-band flag): a top-level line, since it
    # belongs to the primary logger, not a co-deployed device. Reconstruct on-duration from the encoded
    # transitions (closed at the record end) for a coverage percentage.
    if (lvl >= 2L && !is.null(reader_ancillary$video)) {
      vt <- as.numeric(reader_ancillary$video$data$datetime); vs <- as.logical(reader_ancillary$video$data$video)
      t0 <- as.numeric(min(sensor_data$datetime, na.rm = TRUE)); tend <- as.numeric(max(sensor_data$datetime, na.rm = TRUE))
      on_dur <- sum((c(vt[-1], tend) - vt)[vs]); total <- tend - t0; n_int <- sum(vs)
      .log_detail(lvl, "video: ", n_int, " recording interval", if (n_int != 1L) "s" else "",
                  if (is.finite(total) && total > 0) sprintf(" (%.0f%% of record)", 100 * on_dur / total) else "")
    }


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
    if (!is.null(recovery.date.col))   role_meta$recovery_datetime <- animal_info[[recovery.date.col]]
    if (!is.null(animal.id.col))       role_meta$animal_id       <- animal_info[[animal.id.col]]
    if (!is.null(deploy.site.col))     role_meta$deploy_site     <- animal_info[[deploy.site.col]]
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
                             timezone = timezone, sampling.hz = NULL, clock = res$clock)

    # ---- extras only the file-import path has -------------------------------------------------------
    meta$sensors$excluded <- excluded_channels       # channels dropped per the exclude_sensors metadata
    meta$sensors$recording_utc_offset <- res$recording_utc_offset
    # The sidecar declaration is provenance only; processing still infers its working sampling rate from
    # timestamps rather than trusting or copying `sidecar$sample_rate` into the sensor metadata.
    meta$sidecar <- sidecar_info
    # ancillary streams from any co-deployed device (read + clock-aligned above): wet/dry
    # (transition-encoded, consumed by the depth drift correction) and the full position record. The
    # clock-alignment provenance rides alongside them, so the applied offset - or the reason the aligner
    # abstained - stays inspectable downstream.
    for (nm in names(att$ancillary)) meta$ancillary[[nm]] <- att$ancillary[[nm]]
    # co-deployed device identity (make / model / serial) for provenance and summaries: the serial anchors
    # it (present on every deployment), the model enriches it when the Summary.csv names one.
    if (length(wc_anc))
      meta$ancillary$source_device <- list(make = "Wildlife Computers",
                                           model  = wc_model  %||% NA_character_,
                                           serial = wc_serial %||% NA_character_)
    # primary-tag ancillary streams the reader parsed in-band (e.g. the Little Leonardo video-coverage
    # flag -> meta$ancillary$video). These belong to the primary logger, not a co-deployed device, and
    # are carried through verbatim; file-based video (CATS/CEiiA) stays with the video subsystem.
    for (nm in names(reader_ancillary)) meta$ancillary[[nm]] <- reader_ancillary[[nm]]
    meta <- .appendProcessing(meta, "importTagData",
                              directory = data.folders[i],
                              imported_columns = selected_cols,
                              timezone = meta$sensors$timezone, clock = res$clock)
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
    tot_rows <- tot_rows + nrow(sensor_data)
    tot_secs <- tot_secs + .tagSpanSeconds(sensor_data[["datetime"]])
    n_done <- n_done + 1L
    imported_ids <- c(imported_ids, id)
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

  # Refresh only the import-stage rows for deployments this call evaluated. In strict mode that is the
  # full metadata roster; in ignore mode it is just the supplied folders, so a partial run cannot erase
  # exclusions belonging to deployments outside its scope.
  excl <- .exclusionsBind(failed_rows)
  .exclusionsWrite(excl, exclusions.file, "importTagData", scope.ids = exclusion_scope_ids)

  # ---- final run summary -----------------------------------------------------------------------
  # Its own block (its own rule + blank-line separation), because the overall run statistics are
  # conceptually distinct from the continuous per-individual processing above.
  if (lvl >= 1L) {
    .log_summary(lvl)
    if (identical(missing.deployments, "exclude")) {
      n_imported <- length(unique(imported_ids))
      .log_done(lvl, n_imported, " of ", n_expected, " expected deployment",
                if (n_expected != 1L) "s", " imported")
    } else {
      .log_done(lvl, n_done, " of ", n_animals, " folder", if (n_animals != 1) "s", " imported")
    }
    # scale of what was ingested: raw samples at native rate, and summed tracked time across tags
    if (n_done > 0)
      .log_arrow(lvl, "total rows: ", .formatLargeNumber(tot_rows),
                 " \u00b7 duration: ", .fmt_duration(tot_secs))
    if (!is.null(output.dir)) .log_arrow(lvl, "output: ", output.dir)
    if (!is.null(exclusions.file)) .log_arrow(lvl, "exclusions: ", exclusions.file)
    .log_runtime(lvl, start.time)

    # Master issues report - echoes every category (pre-flight + per-deployment) so the final verdict is
    # complete without scrolling. The quiet channel renders these same records as grouped warnings.
    .renderImportIssues(lvl, import_issues)
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
    if (nrow(excl)) attr(data_list, "nautilus.exclusions") <- excl
    return(data_list)
  }
  # return.data = FALSE: the written .rds paths, invisibly - a top-level import must not auto-print a wall
  # of paths, but the value stays available to chain into the next step or capture (see .collectOutput).
  paths <- unlist(saved, use.names = FALSE)
  if (nrow(excl)) paths <- structure(paths, nautilus.exclusions = excl)
  invisible(paths)
}
