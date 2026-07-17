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
