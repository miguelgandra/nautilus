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
#' as `start + (0:(n - 1)) / sampling.rate`. Timestamps are taken as given in `timezone` and are never
#' silently reinterpreted.
#'
#' ## Deployment metadata
#' Optional. Metadata is read using the role names from [metadataColumns()] - where and when the tag
#' was deployed and recovered, the tag model and type, the attachment site, and so on. A single-row
#' object from [checkDeploymentMetadata()] can be passed directly, since its columns already use those
#' names. Columns named in `traits` are carried through unchanged as biometric traits, for example
#' length or sex. Anything not supplied stays missing; none of it is needed to run an accelerometer
#' analysis.
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
#'   from `metadata`. An error is raised if none is available, or if `data` mixes several animals.
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
#' @param timezone The time zone the timestamps are recorded in (default `"UTC"`). This records what
#'   the timestamps mean; it does not shift them.
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
                         verbose = TRUE) {

  lvl <- .verbosity(verbose)

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
  synth <- !is.null(start) || !is.null(sampling.rate)
  if (synth) {
    if (is.null(start) || is.null(sampling.rate)) {
      .abort("Supply BOTH {.arg start} (a POSIXct) and {.arg sampling.rate} (Hz) to synthesise timestamps.")
    }
    if (!inherits(start, "POSIXct") || length(start) != 1L) .abort("{.arg start} must be a single POSIXct value.")
    .assert_number(sampling.rate, "sampling.rate", min = .Machine$double.eps)
    dt[, datetime := start + (seq_len(.N) - 1L) / sampling.rate]
  } else {
    if (!datetime.col %in% names(dt)) {
      .abort(c("{.arg data} has no {.val {datetime.col}} column.",
               "i" = "Rename it via {.arg datetime.col}, or synthesise timestamps with {.arg start} + {.arg sampling.rate}."))
    }
    if (!inherits(dt[[datetime.col]], "POSIXct")) {
      .abort(c("Column {.val {datetime.col}} must be {.cls POSIXct}.",
               "i" = "Convert it first, e.g. {.code as.POSIXct(x, tz = \"{timezone}\")}."))
    }
    if (!identical(datetime.col, "datetime")) {
      if ("datetime" %in% names(dt)) dt[, datetime := NULL]      # avoid a name clash on rename
      data.table::setnames(dt, old = datetime.col, new = "datetime")
    }
  }
  if (any(is.na(dt$datetime))) .abort("The timestamp column contains {.val NA} values; every sample needs a time.")
  # interpret the recorded clock IN `timezone` (never shift it), so the column's tzone always equals
  # meta$sensors$timezone - the same invariant importTagData guarantees via .addDatetime()
  dt[, datetime := lubridate::force_tz(datetime, timezone)]
  data.table::setorderv(dt, "datetime")                          # imported tags are time-sorted

  # ---- deployment ID: argument > metadata > an existing ID column ----
  md <- .asMetadataRow(metadata)
  id <- id %||% .metaField(md, "id") %||% .metaField(md, "ID")
  if (is.null(id) && "ID" %in% names(dt)) {
    ids <- unique(stats::na.omit(dt$ID))
    if (length(ids) > 1L) {
      .abort(c("{.arg data} contains several IDs ({.val {as.character(ids)}}).",
               "i" = "buildTagData() builds ONE deployment; split the data and call it per ID."))
    }
    if (length(ids) == 1L) id <- as.character(ids[1])
  }
  if (is.null(id) || is.na(id) || !nzchar(id)) {
    .abort("No deployment {.arg id}: pass {.arg id}, an {.val ID} column, or an {.code id} in {.arg metadata}.")
  }
  id <- as.character(id)
  dt[, ID := id]

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
                           sampling.hz = samp_hz)
  meta <- .appendProcessing(meta, "buildTagData",
                            source = "in-memory data.frame", rows = n,
                            channels = paste(present, collapse = ", "),
                            timestamps = if (synth) "synthesised (start + rate)" else "supplied",
                            timezone = timezone)

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


#' Assemble the consolidated nautilus metadata shared by every ingestion path.
#'
#' The single place the `nautilus_tag` meta SCHEMA is populated, so the reader path
#' (\code{importTagData()} -> \code{read_cats()}) and the in-memory path (\code{buildTagData()}) cannot
#' drift apart as new per-format readers are added. \code{buildTagData()} is its public face.
#'
#' It deliberately does NOT append a processing record: each caller appends its own, so an imported
#' tag's audit trail names \code{importTagData} - the operation the user actually invoked - rather than
#' leaking this internal step. Callers add their own extras afterwards (calibration sidecar, excluded
#' channels, WC ancillary streams).
#' @param data The canonical frame (read for the channels present and the recorded span).
#' @param id Deployment ID.
#' @param metadata A flattened role-named metadata row (\code{metadataColumns()} roles), or NULL.
#' @param traits Names in `metadata` to carry through as passive biometric traits.
#' @param timezone Time zone the tag recorded its clock in.
#' @param sampling.hz Original sampling rate, or NULL to leave it NA (importTagData does not persist a
#'   rate - it is inferred from the timestamps downstream).
#' @return A nautilus metadata list, without a processing record.
#' @keywords internal
#' @noRd
.assembleTagMeta <- function(data, id, metadata = NULL, traits = NULL, timezone = "UTC",
                             sampling.hz = NULL) {
  meta <- .newNautilusMeta()
  meta$id <- as.character(id)
  meta <- .applyDeploymentMetadata(meta, metadata, traits)
  meta$sensors$present  <- intersect(.sensorChannels(), names(data))
  meta$sensors$timezone <- timezone
  if (!is.null(sampling.hz)) meta$sensors$sampling_hz_original <- sampling.hz
  meta$span$first_datetime <- min(data$datetime)
  meta$span$last_datetime  <- max(data$datetime)
  meta$span$original_rows  <- nrow(data)
  meta$axis_mapping <- .newAxisMappingMeta()      # raw: no axis transform is applied at assembly
  meta
}


#' Coerce `metadata` (a 1-row data.frame / nautilus_deployments / named list) to a flat named list of
#' scalars, or NULL. Multi-row input is rejected (buildTagData builds one deployment).
#' @keywords internal
#' @noRd
.asMetadataRow <- function(metadata) {
  if (is.null(metadata)) return(NULL)
  if (is.data.frame(metadata)) {
    if (nrow(metadata) != 1L) {
      .abort(c("{.arg metadata} must be a single deployment row (got {nrow(metadata)}).",
               "i" = "Subset it to one row before calling buildTagData()."))
    }
    return(as.list(metadata))
  }
  if (is.list(metadata)) return(metadata)
  .abort("{.arg metadata} must be a 1-row data.frame, a nautilus_deployments row, or a named list.")
}

#' Pull a single scalar field from a flattened metadata row (NULL if absent/NA).
#' @keywords internal
#' @noRd
.metaField <- function(md, name) {
  if (is.null(md) || !name %in% names(md)) return(NULL)
  v <- md[[name]]
  if (length(v) == 0L) return(NULL)
  v <- v[[1]]
  if (length(v) == 1L && is.na(v)) return(NULL)
  v
}

#' Map metadataColumns()-role fields from a metadata row onto the nautilus meta schema.
#' @keywords internal
#' @noRd
.applyDeploymentMetadata <- function(meta, md, traits) {
  if (is.null(md)) return(meta)
  chr <- function(x) if (is.null(x)) NULL else as.character(x)
  num <- function(x) if (is.null(x)) NULL else as.numeric(x)
  # the ANIMAL, which is not the deployment: one shark can carry several tags across a season, and
  # `id` identifies the deployment. Kept at the top level rather than under `deployment`, because it is
  # a property of the animal that outlives any one attachment.
  if (!is.null(v <- chr(.metaField(md, "animal_id"))))        meta$animal_id <- v
  # deployment block
  if (!is.null(v <- num(.metaField(md, "deploy_lon"))))       meta$deployment$lon <- v
  if (!is.null(v <- num(.metaField(md, "deploy_lat"))))       meta$deployment$lat <- v
  if (!is.null(v <- .metaField(md, "deploy_datetime")))       meta$deployment$datetime <- v
  if (!is.null(v <- chr(.metaField(md, "deploy_site"))))      meta$deployment$site <- v
  if (!is.null(v <- num(.metaField(md, "popup_lon"))))        meta$deployment$popup_lon <- v
  if (!is.null(v <- num(.metaField(md, "popup_lat"))))        meta$deployment$popup_lat <- v
  if (!is.null(v <- .metaField(md, "popup_datetime")))        meta$deployment$popup_datetime <- v
  if (!is.null(v <- chr(.metaField(md, "attachment_site"))))  meta$deployment$attachment_site <- v
  if (!is.null(v <- chr(.metaField(md, "deployment_type"))))  meta$deployment$deployment_type <- v
  # tag block
  if (!is.null(v <- chr(.metaField(md, "tag_model"))))        meta$tag$model <- v
  if (!is.null(v <- chr(.metaField(md, "tag_type"))))         meta$tag$type <- v
  if (!is.null(v <- .metaField(md, "package_id")))            meta$tag$package_id <- v
  if (!is.null(v <- chr(.metaField(md, "logger_id"))))        meta$tag$logger_id <- v
  if (!is.null(v <- .metaField(md, "paddle_wheel")))          meta$tag$paddle_wheel <- v
  if (!is.null(v <- chr(.metaField(md, "axis_config"))))      meta$tag$axis_config <- v
  # passive traits -> biometrics (kept verbatim, factors flattened to character so a stored trait never
  # carries the table's other levels). Unlike the role fields above, a trait is recorded even when NA:
  # the key's presence is the record that the trait was mapped, which is what importTagData() has always
  # done - so both ingestion paths agree.
  for (tr in traits) {
    if (is.null(md) || !tr %in% names(md)) next
    v <- md[[tr]]
    if (length(v) == 0L) next
    v <- v[[1]]
    meta$biometrics[[tr]] <- if (is.factor(v)) as.character(v) else v
  }
  meta
}

#' Estimate the sampling rate (Hz) from a sorted POSIXct vector; NA if it cannot be determined.
#' @keywords internal
#' @noRd
.estimateHz <- function(datetime) {
  if (length(datetime) < 2L) return(NA_real_)
  d <- as.numeric(diff(as.numeric(datetime)))
  d <- d[is.finite(d) & d > 0]
  if (!length(d)) return(NA_real_)
  hz <- 1 / stats::median(d)
  if (abs(hz - round(hz)) < 0.01 * max(1, round(hz))) round(hz) else round(hz, 2)
}
