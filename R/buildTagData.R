#######################################################################################################
# buildTagData(): construct a nautilus_tag from in-memory data ########################################
#######################################################################################################

#' Build a nautilus_tag from an in-memory data frame
#'
#' @description
#' Assembles a validated \code{nautilus_tag} from sensor data you already hold in memory - the same
#' internal object \code{\link{importTagData}} produces from files, but starting from a
#' \code{data.frame}/\code{data.table} you supply. Use it when the tag format is not one
#' \code{importTagData()} can read yet, when the data was exported from another tool, or for simulated
#' data. The result flows through the rest of the pipeline (\code{\link{processTagData}},
#' \code{\link{checkTagMapping}}, \code{\link{applyAxisMapping}}, ...) exactly like an imported tag.
#'
#' @details
#' \strong{Sensor columns.} One row per sample. Sensor channels should use the canonical names
#' \code{ax, ay, az, gx, gy, gz, mx, my, mz, depth, temp, paddle_speed, paddle_freq}; any that are present
#' are recorded in \code{meta$sensors$present}. Columns under other names can be renamed on the way in with
#' \code{sensor.mapping} (e.g. \code{c(ax = "X", ay = "Y", az = "Z")}). At least one sensor channel must be
#' present. \strong{This is a raw constructor: no axis transform, calibration or unit conversion is
#' applied} - orientation is a downstream \code{\link{applyAxisMapping}} / \code{\link{processTagData}}
#' step, so supply the axes in the tag's own recorded frame and units (acceleration in g).
#'
#' \strong{Timestamps.} Provide either a POSIXct \code{datetime} column (named by \code{datetime.col}) or,
#' for loggers that store only a start time and a fixed rate, a POSIXct \code{start} plus a
#' \code{sampling.rate} (Hz) - the timestamps are then synthesised as
#' \code{start + (0:(n-1)) / sampling.rate}. Timestamps should already be POSIXct in \code{timezone}; they
#' are not silently reinterpreted.
#'
#' \strong{Metadata.} Optional deployment metadata is mapped from \code{metadata} using the
#' \code{\link{metadataColumns}} role names (\code{deploy_lon}, \code{deploy_lat}, \code{deploy_datetime},
#' \code{popup_lon}/\code{lat}/\code{datetime}, \code{tag_model}, \code{tag_type}, \code{package_id},
#' \code{logger_id}, \code{axis_config}, \code{paddle_wheel}, \code{attachment_site},
#' \code{deployment_type}). A single-row \code{nautilus_deployments} object from
#' \code{\link{qcDeploymentMetadata}} is accepted directly (its columns already use these names). Columns
#' named in \code{traits} are carried through verbatim as passive biometric traits (into
#' \code{meta$biometrics}). Anything not supplied stays \code{NA}; none of it is required to run the
#' accelerometer pipeline.
#'
#' \strong{Magnetometer/gyroscope-free tags.} A tag carrying accelerometer channels (\code{ax/ay/az}) plus
#' \code{depth} needs no magnetometer or gyroscope: \code{processTagData()} derives ODBA/VeDBA,
#' surge/sway/heave and pitch/roll from acceleration alone (heading is left \code{NA}), and
#' \code{checkTagMapping()} resolves the tag-to-body axes from gravity and diving dynamics. Note that
#' \code{processTagData()} currently requires a \code{depth} channel (used for vertical velocity and the
#' mounting-pitch correction); \code{buildTagData()} itself does not - it will build a depth-less tag for
#' use with functions that do not need depth.
#'
#' @param data A \code{data.frame}/\code{data.table}, one row per sample: a timestamp column (or use
#'   \code{start} + \code{sampling.rate}) and one or more sensor columns.
#' @param id Character deployment ID. If \code{NULL}, taken from an \code{"ID"} column in \code{data} or
#'   from \code{metadata}; an error is raised if none is available or if \code{data} mixes several IDs.
#' @param datetime.col Name of the POSIXct timestamp column in \code{data} (default \code{"datetime"});
#'   renamed to \code{"datetime"} on output. Ignored when \code{start} + \code{sampling.rate} are given.
#' @param start,sampling.rate Optional. When \code{data} has no timestamp column: a length-1 POSIXct
#'   \code{start} and a positive \code{sampling.rate} (Hz) synthesise the \code{datetime} column.
#' @param metadata Optional single-row \code{data.frame}/\code{nautilus_deployments} or named list of
#'   deployment metadata, keyed by \code{\link{metadataColumns}} role names.
#' @param traits Character vector of \code{metadata} column names to carry through as passive biometric
#'   traits (into \code{meta$biometrics}).
#' @param sensor.mapping Optional named character vector renaming \code{data} columns to canonical sensor
#'   names, e.g. \code{c(ax = "X", ay = "Y", az = "Z")} (names = canonical, values = current column names).
#' @param timezone Time zone the timestamps are recorded in (default \code{"UTC"}); stored in
#'   \code{meta$sensors$timezone}.
#' @param required.sensors Character vector of sensor channels that must be present, or \code{NULL}
#'   (require only a timestamp and at least one sensor channel).
#' @param verbose Verbosity: \code{FALSE}/\code{0}/"quiet", \code{TRUE}/\code{1}/"normal",
#'   \code{2}/"detailed".
#' @return A validated \code{nautilus_tag} (a \code{data.table} carrying the consolidated \code{nautilus}
#'   metadata), ready for \code{\link{processTagData}}.
#' @seealso \code{\link{importTagData}}, \code{\link{processTagData}}, \code{\link{applyAxisMapping}},
#'   \code{\link{qcDeploymentMetadata}}, \code{\link{metadataColumns}}
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
  # deployment block
  if (!is.null(v <- num(.metaField(md, "deploy_lon"))))       meta$deployment$lon <- v
  if (!is.null(v <- num(.metaField(md, "deploy_lat"))))       meta$deployment$lat <- v
  if (!is.null(v <- .metaField(md, "deploy_datetime")))       meta$deployment$datetime <- v
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
