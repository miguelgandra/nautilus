#######################################################################################################
# read_cats(): the CATS / CEiiA format reader #########################################################
#######################################################################################################
#
# The per-manufacturer PARSING stage of ingestion, extracted from importTagData() so that the rest of the
# import (deployment discovery, metadata, co-deployed Wildlife Computers streams, tag assembly, saving)
# is format-agnostic and a new logger only needs a new reader.
#
# Contract: files on disk -> a canonical sensor frame. It reads and merges the deployment's sensor CSVs,
# maps the header onto nautilus sensor roles, parses the timestamps, drops caller-specified channels,
# reads the calibration sidecar and converts every channel to canonical units (g / rad/s / uT). What it
# returns is exactly what `buildTagData()`/the tag assembly needs as input.
#
# Two rules keep it honest, both learned from the code it replaces:
#  1. It NEVER writes to the caller's deferred-issue collectors. Those are locals in importTagData()
#     mutated via `<-` and read by an on.exit closure; assigning to them from inside another function
#     frame would silently vanish and the verbose=0 warning channel would go quiet. Status is RETURNED
#     (`temp_status`, `tz_mismatch`) and the caller collects.
#  2. It NEVER prints the per-deployment report. Logging in the original is interleaved with parsing
#     (exclusion -> attrs -> assembly -> sensors -> temp -> span -> calibration -> timezone -> units), so
#     the reader returns report DATA (`assembly`, `unit_notes`, `tz_note`, ...) and the caller renders it
#     in that order - the pattern `.reportAssembly()` / `.reportCalibration()` already established.


#' The built-in CATS / CEiiA column mappings
#'
#' The two dialects `read_cats()` understands, as `colname -> sensor role + original unit`. They live here,
#' beside the reader that owns this knowledge, because they are CATS knowledge - not import machinery.
#'
#' The two dialects share no header literal (CATS writes `Date (UTC)` / `Accelerometer X [m/s2]`, CEiiA
#' writes `Date` / `Ax (g)`), which is why identifying the format is a matter of resolving a header through
#' this table rather than matching any string: see \code{.catsConfirm}.
#' @return A data.frame with columns `colname`, `sensor`, `units`.
#' @keywords internal
#' @noRd
.defaultMappings <- function() {
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
    # temperature, and are blacklisted by the caller (see `temp_blacklist`).
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
    c("Temperature (\u00b0C)", "temp", "C"),
    c("Depth (m)", "depth", "m"),
    c("Ticks/s", "paddle_freq", "Hz"),
    c("Velocity (m/s)", "paddle_speed", "m/s")
  )

  # combine all default mappings
  out <- as.data.frame(rbind(CATS, CEIIA), stringsAsFactors = FALSE)
  colnames(out) <- c("colname", "sensor", "units")
  out
}


#' Probe one candidate sensor CSV: header only, never data
#'
#' Reads just the header and resolves it against a mapping table. Shared by `.assembleSensorData()` (which
#' picks the primary file) and `.catsConfirm()` (which decides whether this is a CATS folder at all), so
#' "what makes a CSV usable" has ONE definition rather than drifting between the reader and the detector.
#'
#' Cheap by contract: candidates reach several GB, so this must stay `nrows = 0`.
#'
#' @param f Path to a candidate CSV.
#' @param active_mapping,active_mapping_norm The mapping table and its normalised header.
#' @param blacklist_norm Normalised names of blacklisted (electronics) temperature columns.
#' @return list(file, mapping, schema, sensors, channels, has_dt, size), or NULL when the header is
#'   unreadable or resolves to nothing. `channels` excludes the date/time roles - a file carrying only a
#'   bare `Date` column resolves a role but no CHANNEL, which is what keeps Wildlife Computers sidecars
#'   (Locations, Histos, ...) from looking like sensor data.
#' @keywords internal
#' @noRd
.probeSensorCSV <- function(f, active_mapping, active_mapping_norm, blacklist_norm = character(0)) {
  header <- tryCatch(names(data.table::fread(f, nrows = 0)), error = function(e) NULL)
  if (is.null(header) || !length(header)) return(NULL)
  fm <- .buildFileMapping(header, active_mapping, active_mapping_norm, blacklist_norm)
  if (is.null(fm)) return(NULL)
  sensors <- sort(unique(fm$sensor_name_out))
  list(file = f, mapping = fm,
       schema = paste(sensors, collapse = "+"),
       sensors = sensors,
       channels = setdiff(sensors, c("date", "time", "datetime")),
       has_dt = ("datetime" %in% sensors) || all(c("date", "time") %in% sensors),
       size = file.info(f)$size)
}


#' Is this positively a CATS/CEiiA folder?
#'
#' The `confirm` half of the `cats` entry's detection (see `.readerFormats`). Content-based, and derived
#' rather than invented: it asks exactly what `.assembleSensorData()` asks when picking a primary file with
#' `required.sensors = NULL` - does SOME candidate CSV's header resolve, through the built-in mappings, to a
#' datetime plus at least one real sensor channel? So `detect("cats")` means precisely "read_cats(), using
#' built-in mappings, would find a usable file here".
#'
#' Semantic, not lexical, because the two dialects share no header literal (see `.defaultMappings`): any
#' regex catching CATS misses CEiiA. Routing through `.normalizeHeader()` also inherits its encoding folding,
#' which matters - real CATS headers are Latin-1 and write the degree sign in ways a naive conversion drops.
#'
#' Built-in mappings ONLY, never the user's `import.mapping`: detection must be an intrinsic property of the
#' folder, not a function of an unrelated argument (a user mapping three columns of any CSV would otherwise
#' make that folder "cats"). The documented consequence is that a custom exporter needs `format = "cats"`.
#'
#' EVERY candidate is probed, never just the first or largest: real deployments hold a raw multi-GB export
#' beside a derived/mangled one, and which is "largest" is a coin flip.
#' @param folder,sensor.subdirectory As passed to `importTagData()`.
#' @return TRUE / FALSE. Never errors.
#' @keywords internal
#' @noRd
.catsConfirm <- function(folder, sensor.subdirectory) {
  files <- .listSensorCSVs(folder, sensor.subdirectory)
  if (!length(files)) return(FALSE)
  map      <- .defaultMappings()
  map_norm <- .normalizeHeader(map$colname)
  for (f in files) {
    p <- .probeSensorCSV(f, map, map_norm)
    # >= 1 channel, not the accel triad: `.meetsRequirement(required.sensors = NULL)` accepts one channel,
    # so a depth/temp-only CATS export imports fine under format = "cats" and must stay identifiable under
    # "auto" - auto weaker than explicit is the worst failure mode for an opt-in convenience. Excluding the
    # date/time roles is what does the real work: it is why a WC sidecar with a bare `Date` column (which
    # resolves the CEiiA datetime row and nothing else) is not mistaken for sensor data.
    if (!is.null(p) && p$has_dt && length(p$channels) >= 1L) return(TRUE)
  }
  FALSE
}


#' The supported raw formats, and what each reader needs to find its data
#'
#' The dispatch table for `importTagData(format=)` / the `tag_format` metadata role. Adding a format means
#' adding an entry here plus its reader file - nothing else.
#'
#' Each entry supplies TWO predicates, because the two questions are genuinely different:
#' \itemize{
#'   \item `has_data(folder, sensor.subdirectory)` - "can I find my data, GIVEN the user told me this is my
#'     format?" Used by the pre-flight, which must know whether a folder holds readable data before the loop
#'     ("a .csv under sensor.subdirectory" is only true of CATS). It may be permissive.
#'   \item `confirm(folder, sensor.subdirectory)` - "is this POSITIVELY my format?" Used only by
#'     `format = "auto"`. It must be content-based; "a file exists" is never a confirmation.
#' }
#' `detect` is COMPOSED from them, never authored (see `.withDetect`), which is what makes `has_data` safe to
#' leave permissive: the cats probe matches any folder with a CSV, and would be a catch-all as a detector.
#' @keywords internal
#' @noRd
.readerFormats <- function() {
  lapply(list(
    cats = list(
      label = "CATS / CEiiA",
      has_data = function(folder, sensor.subdirectory) {
        length(list.files(file.path(folder, sensor.subdirectory), pattern = "\\.csv$")) > 0L
      },
      confirm = .catsConfirm),
    little_leonardo = list(
      label = "Little Leonardo",
      has_data = function(folder, sensor.subdirectory) .llDetect(folder, sensor.subdirectory),
      confirm = .llConfirm)
  ), .withDetect)
}


#' Compose a reader entry's `detect` from its `has_data` and `confirm`.
#'
#' `detect` is derived, not written by the reader author, so the invariant `detect => has_data` holds BY
#' CONSTRUCTION. That invariant matters because violating it fails SILENTLY: a folder that detect claims but
#' has_data denies is dropped by the pre-flight as "missing" and skipped with a "no sensor file found"
#' notice - a confident, wrong, quiet answer. Deriving it makes that unfalsifiable rather than a prose rule
#' the next reader author must remember.
#'
#' Both calls are wrapped: a detector that throws on a malformed folder must not take the run down.
#' @param e A reader entry with `has_data` and `confirm`.
#' @keywords internal
#' @noRd
.withDetect <- function(e) {
  force(e)
  e$detect <- function(folder, sensor.subdirectory) {
    ok <- function(fn) isTRUE(tryCatch(fn(folder, sensor.subdirectory), error = function(err) FALSE))
    ok(e$has_data) && ok(e$confirm)
  }
  e
}


#' Validate a format name against the dispatch table.
#'
#' @param x Format name(s) to check.
#' @param arg The argument name to blame in the error.
#' @param allow Extra values legal in THIS position but which are not readers (i.e. `"auto"`). Deliberately
#'   not listed under "Supported", so `"auto"` never advertises itself as a legal `tag_format` column value -
#'   and deliberately not an entry in `.readerFormats()`, which would legalise it everywhere at once and
#'   need a fake label and a switch branch.
#' @keywords internal
#' @noRd
.checkFormat <- function(x, arg = "format", allow = character(0)) {
  known <- names(.readerFormats())
  bad <- setdiff(unique(stats::na.omit(x)), c(known, allow))
  if (length(bad)) {
    .abort(c("Unsupported tag {cli::qty(length(bad))}format{?s}: {.val {bad}}.",
             "i" = "Supported: {.val {known}}."))
  }
  invisible(TRUE)
}


#' Read a CATS/CEiiA deployment folder into a canonical sensor frame
#'
#' @param folder Path to the deployment folder.
#' @param sensor.subdirectory Subdirectory holding the sensor CSVs.
#' @param active_mapping,active_mapping_norm The resolved import mapping and its normalised header.
#' @param temp_blacklist_norm Normalised names of blacklisted (electronics) temperature columns.
#' @param required.sensors Channels that must be present, or NULL.
#' @param timezone Time zone the tag recorded its clock in.
#' @param import.calibration Logical; read the paired calibration sidecar.
#' @param exclude.channels Character vector of channels to drop (resolved from deployment metadata by
#'   the caller). Applied BEFORE unit conversion, so the unit notes describe only retained channels -
#'   exactly as the original ordering did.
#' @param verbose Logical; passed to `.assembleSensorData()` for its own inline reporting.
#' @return A list: `data` (canonical frame, or NULL when unusable), `reason` (why, when `data` is NULL),
#'   `assembly` (pass-through for `.reportAssembly()`), `mapping`, `selected_cols`, `calibration_info`,
#'   `temp_status`, `excluded`, `tz_mismatch` (flag), `tz_note` (string or NULL), `unit_notes`.
#' @keywords internal
#' @noRd
read_cats <- function(folder,
                      sensor.subdirectory,
                      active_mapping,
                      active_mapping_norm,
                      temp_blacklist_norm,
                      required.sensors,
                      timezone,
                      import.calibration = TRUE,
                      exclude.channels = character(0),
                      verbose = FALSE) {

  # ---- locate, probe and assemble the deployment's sensor CSVs -------------------------------------
  # probe every candidate CSV, keep the schema group of the largest usable file, and merge it using a
  # maximal-unique-coverage policy (resilient to overlapping fragments, duplicate exports and corrupt
  # trailing rows). Returns an assembled data.table with a parsed `datetime` column and renamed columns.
  assembly <- .assembleSensorData(
    folder = folder, sensor.subdirectory = sensor.subdirectory,
    active_mapping = active_mapping, active_mapping_norm = active_mapping_norm,
    temp_blacklist_norm = temp_blacklist_norm,
    required.sensors = required.sensors, timezone = timezone, verbose = verbose)

  # nothing usable: hand the reason back verbatim (the caller string-matches it to tell a pre-flighted
  # "no sensor CSV" folder from a genuine failure, so these strings are part of the contract)
  if (is.null(assembly$data)) {
    return(list(data = NULL, reason = assembly$reason, assembly = assembly, mapping = NULL,
                selected_cols = NULL, calibration_info = NULL, temp_status = "none",
                excluded = character(0), tz_mismatch = FALSE, tz_note = NULL,
                unit_notes = character(0)))
  }

  sensor_data   <- assembly$data
  file_mapping  <- assembly$mapping
  selected_cols <- file_mapping$colname_in_csv

  # ---- drop channels the caller flagged unusable for this deployment -------------------------------
  # Dropping - rather than NA-ing - makes the channel simply ABSENT, so every downstream analysis skips
  # it through the package's partial-sensor support. Done BEFORE unit conversion so a dropped channel
  # never contributes to the conversion notes.
  excluded_channels <- intersect(exclude.channels, names(sensor_data))
  if (length(excluded_channels)) sensor_data[, (excluded_channels) := NULL]

  # ---- temperature reliability (returned, never collected here) ------------------------------------
  temp_status <- attr(file_mapping, "temp_status") %||% "none"

  # ---- calibration sidecar paired with the primary CSV ---------------------------------------------
  # parsed and stored for provenance; values are NOT applied to the sensor data at this stage
  calibration_info <- NULL
  tz_mismatch <- FALSE
  tz_note <- NULL
  if (isTRUE(import.calibration)) {
    calibration_info <- .readSidecarCalibration(assembly$primary_file)

    # cross-check the recording zone: if the sidecar reports a UTC offset (assumed hours) that disagrees
    # with `timezone`, the recorded clock is probably local (we never silently shift it).
    off <- if (!is.null(calibration_info)) calibration_info$device$utc_offset else NA_real_
    if (length(off) == 1L && is.finite(off) && off != 0) {
      tz_off <- .tzOffsetHours(timezone, sensor_data$datetime[1])
      if (abs(off - tz_off) > 0.5) {
        tz_mismatch <- TRUE
        tz_note <- sprintf(
          "timezone: sidecar UTC offset %gh vs timestamps read as %s (offset %gh); if logged in local time, set `timezone` to match.",
          off, timezone, round(tz_off, 1))
      }
    }
  }

  # ---- convert every sensor group to its canonical unit --------------------------------------------
  # Note: `datetime` was already parsed to POSIXct during assembly (.addDatetime), where a tiny +0.0001 s
  # offset is added as a floating-point safeguard against off-by-one-second rounding downstream.
  sensor_groups <- list(
    accel = list(cols = c("ax", "ay", "az"), standard_unit = "g", label = "accel"),
    gyro  = list(cols = c("gx", "gy", "gz"), standard_unit = "rad/s", label = "gyro"),
    mag   = list(cols = c("mx", "my", "mz"), standard_unit = "uT", label = "mag")
  )
  unit_notes <- character(0)
  for (group_name in names(sensor_groups)) {
    group_info  <- sensor_groups[[group_name]]
    group_cols  <- group_info$cols
    group_label <- group_info$label
    group_units <- group_info$standard_unit

    # filter `file_mapping` to only the relevant columns present
    rows_to_select  <- file_mapping$sensor_name_out %in% group_cols & file_mapping$sensor_name_out %in% names(sensor_data)
    cols_to_process <- file_mapping[rows_to_select, c("sensor_name_out", "original_units_map")]
    if (nrow(cols_to_process) == 0) next

    cols_to_process$target_unit <- group_units
    conversion_needed <- any(
      cols_to_process$original_units_map != cols_to_process$target_unit &
        cols_to_process$target_unit != "" & !is.na(cols_to_process$target_unit))

    if (conversion_needed) {
      for (i_col in 1:nrow(cols_to_process)) {
        col_to_convert <- cols_to_process$sensor_name_out[i_col]
        original_unit  <- cols_to_process$original_units_map[i_col]
        target_unit    <- cols_to_process$target_unit[i_col]
        if (original_unit != target_unit && target_unit != "" && !is.na(target_unit)) {
          sensor_data[, (col_to_convert) := .convertUnits(get(col_to_convert), from.unit = original_unit,
                                                          to.unit = target_unit)]
        }
      }
      # record the unit(s) actually converted from, for a single merged note the caller renders
      src_units <- unique(cols_to_process$original_units_map[cols_to_process$original_units_map != group_units])
      unit_notes <- c(unit_notes, sprintf("%s %s -> %s", group_label, paste(src_units, collapse = "/"), group_units))
    }
  }

  list(data = sensor_data, reason = NULL, assembly = assembly, mapping = file_mapping,
       selected_cols = selected_cols, calibration_info = calibration_info,
       temp_status = temp_status, excluded = excluded_channels,
       tz_mismatch = tz_mismatch, tz_note = tz_note, unit_notes = unit_notes)
}
