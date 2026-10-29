#######################################################################################################
# buildTagData(): construct a nautilus_tag from in-memory data ########################################
#######################################################################################################

#' Build a tag object from data already in R
#'
#' @description
#' Not every deployment arrives as a file nautilus can read. The logger may use an export format that
#' is not yet supported, the data may have been through another analysis tool, a collaborator may send
#' a processed table, or the record may be simulated for testing a method.
#'
#' `buildTagData()` assembles sensor data you already hold in R into a tag object identical in
#' structure to one [importTagData()] reads from disk, so it flows through the rest of the pipeline
#' unchanged.
#'
#' This is a constructor, not a processing step: it applies no axis rotation, no calibration and no
#' unit conversion. Supply the channels in the tag's own recorded frame, with acceleration in g.
#' Reorienting the axes is a later, deliberate step - see [applyAxisMapping()].
#'
#' @details
#' ## Sensor columns
#' One row per sample. Channels should use the canonical names `ax`, `ay`, `az`, `gx`, `gy`, `gz`,
#' `mx`, `my`, `mz`, `depth`, `temp`, `paddle_speed` and `paddle_freq`; whichever are present are
#' recorded in the tag's metadata. Columns under other names can be renamed on the way in with
#' `sensor.mapping`. At least one sensor channel is required.
#'
#' ## Timestamps
#' Either supply a POSIXct column naming the time of each sample, or - for loggers that record only a
#' start time and a fixed rate - supply `start` and `sampling.rate`, and the timestamps are generated
#' as `start + (0:(n - 1)) / sampling.rate`. Existing POSIXct values and `start` are absolute instants:
#' `timezone` changes their display, preserving seconds since the epoch. Existing timestamps take
#' precedence over `start` and `sampling.rate`; redundant generation arguments are ignored with a warning.
#'
#' To repair POSIXct values that were incorrectly labelled while parsing a logger clock, explicitly use
#' `time.mode = "clock"`. This preserves their calendar readings and interprets them in `timezone`, changing
#' the instants. For generated series, only `start` is reinterpreted, then elapsed seconds are added.
#' The input zone, interpretation and shift range are recorded in `getTagMetadata(tag)$sensors$clock`.
#' Missing DST times are rejected; repeated times require `ambiguous` or a correctly parsed instant.
#'
#' ## Deployment metadata
#' Optional. Metadata is read using the role names from [metadataColumns()] - where and when the tag
#' was deployed and recovered, the tag model and type, the attachment site, and so on. A single-row
#' object from [checkDeploymentMetadata()] can be passed directly, since its columns already use those
#' names. Columns named in `traits` are carried through unchanged as biometric traits, for example
#' length or sex. Anything not supplied stays missing; none of it is needed to run an accelerometer
#' analysis. Supplied fields are scalar values. Numeric text and factor coordinate labels are converted
#' to geographic values; coordinates must be finite and within longitude/latitude bounds, with both
#' members of a position supplied together. Event times must be POSIXct, and recovery and pop-up times
#' cannot precede deployment. Recovery and pop-up remain separate events in the stored metadata.
#'
#' All known deployment IDs must agree: the argument, every row in `ID`, metadata `id`/`ID`, and any
#' stored tag identity. IDs are character keys with normalized whitespace; case and leading zeros are
#' preserved. Missing or mixed row IDs are rejected even with an explicit `id`. Correct identities in
#' the source before construction; this constructor does not relabel an existing deployment.
#'
#' ## Tags without a magnetometer or gyroscope
#' A tag carrying only accelerometer channels and depth is fully usable. [processTagData()] derives
#' dynamic body acceleration, the surge, sway and heave components and pitch and roll from
#' acceleration alone, leaving heading missing, and [checkTagMapping()] can still resolve the
#' tag-to-body axes from gravity and diving behaviour.
#'
#' Note that [processTagData()] does require a `depth` channel, which it uses for vertical velocity and
#' for the mounting-pitch correction. `buildTagData()` itself does not, so a depth-less tag can be
#' built for use with the functions that do not need it.
#'
#' @param data A data frame or data.table with one row per sample: a timestamp column - or `start` and
#'   `sampling.rate` instead - and one or more sensor columns.
#' @param id The deployment identifier. `NULL` (default) takes it from an `ID` column in `data`, or
#'   from `metadata`. All supplied identities must agree. An error is raised if none is available,
#'   if row IDs are missing or mixed, or if an existing identity conflicts with `id`.
#' @param datetime.col Which column holds the timestamps (default `"datetime"`). Set it when your
#'   table names that column something else; it is renamed on output so the rest of the pipeline sees
#'   a consistent name.
#' @param start,sampling.rate For loggers that record no clock: the time of the first sample and the
#'   recording rate in Hz, from which the timestamps are generated. Little Leonardo tags are the
#'   common case. Ignored when a timestamp column is present.
#' @param metadata Optional deployment metadata: a single-row data frame, a `nautilus_deployments`
#'   object, or a named list, keyed by the role names from [metadataColumns()].
#' @param traits Which columns of `metadata` to carry through as biometric traits, for example length,
#'   mass or sex. These are stored with the tag and travel with it, but are not used by any analysis
#'   step.
#' @param sensor.mapping How your column names map onto the canonical sensor names, for example
#'   `c(ax = "X", ay = "Y", az = "Z")`. Needed only when the columns are not already canonically named.
#' @param timezone Output display zone (default `"UTC"`), or the source logger zone when explicitly
#'   reinterpreting with `time.mode = "clock"`. Use an Olson name or a fixed UTC offset such as
#'   `"+01:00"`. Fixed-offset results are displayed in UTC and retain their source basis in metadata.
#' @param time.mode `"instant"` (default) preserves existing POSIXct instants. `"clock"` explicitly
#'   reinterprets their calendar readings in `timezone`. This is an opt-in compatibility path for
#'   incorrectly labelled logger clocks, not a clock-drift correction.
#' @param ambiguous How to resolve repeated civil times during explicit clock interpretation:
#'   `"error"` (default), `"earliest"`, or `"latest"`. Nonexistent DST times always error.
#' @param required.sensors Which sensor channels a tag must carry to be built. `NULL` (default)
#'   requires only a timestamp and at least one recognised channel. Name the channels your analysis
#'   depends on to have an incomplete tag rejected here rather than later.
#' @param verbose How much detail to print: `0`/`"quiet"`, `1`/`"normal"`, or `2`/`"detailed"`.
#'
#' @return A validated `nautilus_tag`: a data.table carrying the sensor data together with the
#'   deployment metadata and a processing record, ready for [processTagData()].
#'
#' @seealso [importTagData()] to read tags from disk; [metadataColumns()] and
#'   [checkDeploymentMetadata()] for the metadata; [applyAxisMapping()] for the axis frame;
#'   [processTagData()] for the next step.
#' @examples
#' # a minimal accelerometer-only tag from an in-memory data frame
#' n  <- 6000
#' df <- data.frame(
#'   datetime = as.POSIXct("2025-01-01", tz = "UTC") + (seq_len(n) - 1) / 20,
#'   ax = rnorm(n), ay = rnorm(n), az = 1 + rnorm(n), depth = abs(rnorm(n, 5)))
#' tag <- buildTagData(df, id = "DEMO01")
#'
#' # loggers with no timestamp column: synthesise from a start time + rate
#' raw <- data.frame(ax = rnorm(n), ay = rnorm(n), az = 1 + rnorm(n))
#' tag <- buildTagData(raw, id = "DEMO02",
#'                     start = as.POSIXct("2025-01-01 12:00:00", tz = "UTC"),
#'                     sampling.rate = 20)
#' @export
buildTagData <- function(data,
                         id = NULL,
                         datetime.col = "datetime",
                         start = NULL,
                         sampling.rate = NULL,
                         metadata = NULL,
                         traits = NULL,
                         sensor.mapping = NULL,
                         timezone = "UTC",
                         required.sensors = NULL,
                         verbose = TRUE,
                         time.mode = c("instant", "clock"),
                         ambiguous = c("error", "earliest", "latest")) {

  lvl <- .verbosity(verbose)
  time.mode <- match.arg(time.mode)
  ambiguous <- match.arg(ambiguous)
  .timeZoneSpec(timezone)

  # ---- input + a private working copy (never mutate the caller's object) ----
  if (!is.data.frame(data)) .abort("{.arg data} must be a {.cls data.frame} or {.cls data.table}.")
  .assert_nonempty(data, "data", "rows")
  dt <- data.table::copy(data.table::as.data.table(data))

  # ---- rename sensor columns to canonical names (names = canonical, values = current) ----
  if (!is.null(sensor.mapping)) {
    if (!is.character(sensor.mapping) || is.null(names(sensor.mapping)) || any(names(sensor.mapping) == "")) {
      .abort("{.arg sensor.mapping} must be a named character vector, e.g. {.code c(ax = \"X\", ay = \"Y\")}.")
    }
    targets <- names(sensor.mapping); sources <- unname(sensor.mapping)
    missing_src <- setdiff(sources, names(dt))
    if (length(missing_src)) {
      .abort(c("{.arg sensor.mapping} refers to columns not in {.arg data}: {.val {missing_src}}.",
               "i" = "Columns present: {.val {names(dt)}}."))
    }
    # a rename that lands on an already-taken name would create duplicate columns and silently drop data
    if (anyDuplicated(targets)) {
      .abort("{.arg sensor.mapping} maps several columns to the same target: {.val {unique(targets[duplicated(targets)])}}.")
    }
    clash <- targets[targets %in% names(dt) & targets != sources]   # target exists and is not its own source
    if (length(clash)) {
      .abort(c("{.arg sensor.mapping} would rename onto column(s) that already exist: {.val {clash}}.",
               "i" = "Rename or drop the existing column(s) first, or map them explicitly."))
    }
    data.table::setnames(dt, old = sources, new = targets)
  }

  # ---- timestamps: an existing POSIXct column, or synthesised from start + sampling.rate ----
  synth <- !datetime.col %in% names(dt)
  if (synth) {
    if (is.null(start) && is.null(sampling.rate)) {
      .abort(c("{.arg data} has no {.val {datetime.col}} column.",
               "i" = "Supply a POSIXct column, or generate timestamps with start + sampling.rate."))
    }
    if (!identical(datetime.col, "datetime") && "datetime" %in% names(dt))
      .abort("{.arg datetime.col} names an absent column while {.val datetime} exists; select the existing timestamp column explicitly.")
    if (is.null(start) || is.null(sampling.rate)) {
      .abort("Supply BOTH {.arg start} (a POSIXct) and {.arg sampling.rate} (Hz) to synthesise timestamps.")
    }
    if (!inherits(start, "POSIXct") || length(start) != 1L) .abort("{.arg start} must be a single POSIXct value.")
    .assert_number(sampling.rate, "sampling.rate", min = .Machine$double.eps)
    resolved <- .resolveTagTime(start, timezone, time.mode, ambiguous)
    resolved$clock$generated <- TRUE
    dt[, datetime := resolved$datetime + (seq_len(.N) - 1L) / sampling.rate]
  } else {
    if (!inherits(dt[[datetime.col]], "POSIXct")) {
      .abort(c("Column {.val {datetime.col}} must be {.cls POSIXct}.",
               "i" = "Convert it first, e.g. {.code as.POSIXct(x, tz = \"{timezone}\")}."))
    }
    if (!identical(datetime.col, "datetime")) {
      if ("datetime" %in% names(dt)) dt[, datetime := NULL]      # avoid a name clash on rename
      data.table::setnames(dt, old = datetime.col, new = "datetime")
    }
    if (!is.null(start) || !is.null(sampling.rate))
      cli::cli_warn("Existing timestamps take precedence: {.arg start} and {.arg sampling.rate} are ignored.")
    resolved <- .resolveTagTime(dt$datetime, timezone, time.mode, ambiguous)
    dt[, datetime := resolved$datetime]
  }
  .validateInstants(dt$datetime)
  data.table::setorderv(dt, "datetime")                          # imported tags are time-sorted

  # Validate every identity source before assigning the canonical deployment key.
  if (is_nautilus_deployments(metadata) && isFALSE(attr(metadata, "nautilus.qc")$passed))
    .abort("{.arg metadata} failed metadata QC; inspect {.code issues()} and rerun QC after correcting it.")
  md <- .asMetadataRow(metadata)
  id <- .resolveDeploymentId(if ("ID" %in% names(data)) data$ID else NULL,
                             id = id, metadata = md,
                             stored = .getMeta(data)$id %||% attr(data, "id", exact = TRUE))
  data.table::set(dt, j = "ID", value = rep(id, nrow(dt)))

  # ---- sensors present + required.sensors gate ----
  # (bind to a local: cli mis-parses `{.val {.sensorChannels()}}` -- the inner `{.` reads as a style tag)
  sensor_names <- .sensorChannels()
  present <- intersect(sensor_names, names(dt))
  if (!length(present)) {
    .abort(c("{.arg data} has no recognised sensor channels.",
             "i" = "Expected one or more of {.val {sensor_names}} (rename via {.arg sensor.mapping})."))
  }
  if (!is.null(required.sensors)) {
    if (!is.character(required.sensors) || !all(required.sensors %in% sensor_names)) {
      .abort("{.arg required.sensors} must be {.code NULL} or valid sensor names ({.val {sensor_names}}).")
    }
    missing_req <- setdiff(required.sensors, present)
    if (length(missing_req)) {
      .abort(c("Required sensor channel(s) missing: {.val {missing_req}}.",
               "i" = "Present: {.val {present}}."))
    }
  }

  # ---- consolidated metadata (shared assembler; this function is its public face) ----
  n <- nrow(dt)
  samp_hz <- if (synth) sampling.rate else .estimateHz(dt$datetime)
  meta <- .assembleTagMeta(dt, id = id, metadata = md, traits = traits, timezone = timezone,
                           sampling.hz = samp_hz, clock = resolved$clock)
  meta <- .appendProcessing(meta, "buildTagData",
                            source = "in-memory data.frame", rows = n,
                            channels = paste(present, collapse = ", "),
                            timestamps = if (synth) "synthesised (start + rate)" else "supplied",
                            timezone = meta$sensors$timezone, clock = resolved$clock)

  # lightweight "assembled by nautilus" marker (parallels importTagData)
  data.table::setattr(dt, "nautilus.version", utils::packageVersion("nautilus"))
  out <- new_nautilus_tag(dt, meta)

  # ---- report ----
  if (lvl >= 1L) {
    .log_header(lvl, "buildTagData",
                sprintf("Assembling a nautilus_tag for %s", id),
                bullets = c(sprintf("%s rows | %g Hz", .formatLargeNumber(n), samp_hz %||% NA_real_),
                            sprintf("sensors: %s", paste(present, collapse = ", "))))
    .log_ok(lvl, sprintf("built %s (%s)", id, if (synth) "timestamps synthesised" else "timestamps supplied"))
  }
  out
}
