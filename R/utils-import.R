# Import normalization and manufacturer-specific readers.

#' @import utils
#' @import stats
#' @import graphics
#' @import grDevices
#' @import data.table
NULL


#####################################################################################
## Coerce strings to UTF-8 ##########################################################
#####################################################################################

#' Coerce character vector to valid UTF-8
#'
#' @description Returns a UTF-8 version of `x`. Elements that are not already valid
#' UTF-8 are assumed to be Latin-1 (the encoding used by several tag exporters, e.g.
#' CATS mini-diary CSVs) and re-encoded accordingly. This makes downstream string
#' matching robust to mixed-encoding inputs.
#' @param x A character vector.
#' @return A UTF-8 character vector of the same length.
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.toUTF8 <- function(x) {
  x <- as.character(x)
  # flag elements whose bytes are NOT valid UTF-8 (is.na marks invalid)
  invalid <- is.na(iconv(x, "UTF-8", "UTF-8"))
  # Valid UTF-8 bytes are often flagged "unknown" by readers (e.g. data.table::fread).
  # Mark them as UTF-8 explicitly rather than re-encoding from the native charset,
  # which would corrupt them in a non-UTF-8 (e.g. C) locale.
  Encoding(x[!invalid]) <- "UTF-8"
  # Remaining bytes are assumed Latin-1 (used by some tag exporters) and converted.
  if (any(invalid)) x[invalid] <- iconv(x[invalid], from = "Latin1", to = "UTF-8", sub = "byte")
  x
}


#####################################################################################
## Normalize column headers for robust matching ####################################
#####################################################################################

#' Normalize a column header for robust, encoding-tolerant matching
#'
#' @description Canonicalizes raw CSV column names so that headers exported by
#' different tag firmwares/locales compare equal. It folds visually-confusable
#' Unicode symbols that frequently differ between exporters, collapses whitespace,
#' and lower-cases the result. Specifically it maps:
#' \itemize{
#'   \item the degree sign (U+00B0) and the masculine ordinal indicator (U+00BA,
#'     a common mojibake/typo for degrees) to the token \code{"deg"};
#'   \item the micro sign (U+00B5) and the Greek small mu (U+03BC) to \code{"u"};
#'   \item the superscript two (U+00B2) to \code{"2"}.
#' }
#' Inputs are first coerced to UTF-8 via `.toUTF8`, so Latin-1 headers are handled.
#' @param x A character vector of column names.
#' @return A normalized character vector suitable for equality comparison.
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.normalizeHeader <- function(x) {
  x <- .toUTF8(x)
  # fold visually-confusable symbols; build them from code points so the source
  # stays ASCII, and use fixed = TRUE so matching is locale-independent
  deg   <- intToUtf8(0x00B0)  # degree sign
  ord   <- intToUtf8(0x00BA)  # masculine ordinal indicator (common typo/mojibake for degrees)
  micro <- intToUtf8(0x00B5)  # micro sign
  mu    <- intToUtf8(0x03BC)  # Greek small letter mu
  sup2  <- intToUtf8(0x00B2)  # superscript two
  x <- gsub(deg,   "deg", x, fixed = TRUE)
  x <- gsub(ord,   "deg", x, fixed = TRUE)
  x <- gsub(micro, "u",   x, fixed = TRUE)
  x <- gsub(mu,    "u",   x, fixed = TRUE)
  x <- gsub(sup2,  "2",   x, fixed = TRUE)
  x <- tolower(trimws(x))                 # case-insensitive, trim ends
  x <- gsub("\\s+", " ", x)               # collapse internal whitespace
  x
}


#####################################################################################
## Convert sensor units  ############################################################
#####################################################################################

#' Convert Sensor Values Between Common Measurement Units
#'
#' Standardizes sensor measurements by converting between different units, ensuring consistent
#' units for downstream analysis. All gyroscope values are converted to rad/s and all
#' acceleration values to g (standard gravity) by default.
#'
#' @param value Numeric vector of values to convert
#' @param from.unit Character string specifying the current unit of measurement.
#'   Supported acceleration units: "m/s2", "g"
#'   Supported gyroscope units: "rad/s", "deg/s", "mrad/s"
#'   Supported temperature units: "C" (Celsius), "F" (Fahrenheit), "K" (Kelvin)
#'   Supported depth/pressure units: "m" (meters), "dbar" (decibar), "bar")
#'   Supported magnetic units: "uT" (microtesla), "nT" (nanotesla), "mG" (milligauss)
#'   Supported speed units: "m/s", "km/h", "knot", "mph"
#'   Other supported units: "" (unitless)
#' @param to.unit Character string specifying the target unit of measurement.
#'   Use "" or NA for unitless values. If NULL, converts to standard units:
#'   - Accelerometer data → "g"
#'   - Gyroscope data → "rad/s"
#'   - Temperature data → "C"
#'   - Depth/Pressure data → "m"
#'   - Magnetic data → "uT"
#'   - Speed data → "m/s"
#'   - Other data → original units
#' @param verbose Logical. If `TRUE`, prints messages about the conversions performed.
#'
#' @return Numeric vector of converted values with the following guarantees:
#'   - Acceleration always returned in g (standard gravity, 9.80665 m/s²)
#'   - Gyroscopic data always returned in rad/s
#'   - Magnetometer data always returned in µT
#'   - Temperature always returned in °C
#'   - Depth always returned in meters
#'
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal


.convertUnits <- function(value, from.unit, to.unit = NULL, verbose = FALSE) {

  # Ensure input units are character and not empty/NA if not handled by default
  from.unit <- as.character(from.unit)
  if (length(from.unit) == 0 || is.na(from.unit) || from.unit == "") {
    if (verbose) cat(paste0("Warning: `from.unit` is missing or empty. No conversion performed.\n"))
    return(value)
  }

  # Handle NULL to.unit by converting to standard units
  if (is.null(to.unit)) {
    if (verbose) cat(paste0("Converting from '", from.unit, "' to standard unit.\n"))
    # standard unit for each recognized input; an unrecognized unit passes through unchanged (the former
    # fall-through case). A named-vector lookup replaces the former case_when call (base R, no dependency).
    standard_unit <- c(
      "m/s2" = "g",     "g"    = "g",
      "rad/s" = "rad/s", "deg/s" = "rad/s", "mrad/s" = "rad/s",
      "C" = "C",         "F"    = "C",      "K"    = "C",
      "m" = "m",         "dbar" = "m",      "bar"  = "m",
      "uT" = "uT",       "nT"   = "uT",     "mG"   = "uT",
      "m/s" = "m/s",     "km/h" = "m/s",    "knot" = "m/s", "mph" = "m/s")
    mapped  <- unname(standard_unit[from.unit])
    to.unit <- ifelse(is.na(mapped), from.unit, mapped)
  } else {
    to.unit <- as.character(to.unit)
    if (is.na(to.unit) || to.unit == "") {
      if (verbose) cat(paste0("Target unit is empty or NA. Returning original values (unitless).\n"))
      if (!is.numeric(value)) {
        warning(paste0("Attempting to convert to unitless, but value is not numeric. Returning original values."), call. = FALSE, immediate = TRUE)
      }
      return(value)
    }
  }

  # Return unchanged if units match
  if (from.unit == to.unit) {
    if (verbose) cat(paste0("Units match ('", from.unit, "'). No conversion needed.\n"))
    return(value)
  }

  # --- Acceleration conversions ---
  if (from.unit == "m/s2" && to.unit == "g") {
    if (verbose) cat(paste0("Converting acceleration from m/s", "\U00B2", " to g.\n"))
    return(value / 9.80665)
  } else if (from.unit == "g" && to.unit == "m/s2") {
    if (verbose) cat(paste0("Converting acceleration from g to m/s", "\U00B2", ".\n"))
    return(value * 9.80665)

    # --- Gyroscope conversions ---
  } else if (from.unit == "deg/s" && to.unit == "rad/s") {
    if (verbose) cat(paste0("Converting gyroscope from deg/s to rad/s.\n"))
    return(value * pi / 180)
  } else if (from.unit == "rad/s" && to.unit == "deg/s") {
    if (verbose) cat(paste0("Converting gyroscope from rad/s to deg/s.\n"))
    return(value * 180 / pi)
  } else if (from.unit == "mrad/s" && to.unit == "rad/s") {
    if (verbose) cat(paste0("Converting gyroscope from mrad/s to rad/s.\n"))
    return(value / 1000)
  } else if (from.unit == "rad/s" && to.unit == "mrad/s") {
    if (verbose) cat(paste0("Converting gyroscope from rad/s to mrad/s.\n"))
    return(value * 1000)
  } else if (from.unit == "mrad/s" && to.unit == "deg/s") {
    if (verbose) cat(paste0("Converting gyroscope from mrad/s to deg/s.\n"))
    return(value * (180 / pi) / 1000)
  } else if (from.unit == "deg/s" && to.unit == "mrad/s") {
    if (verbose) cat(paste0("Converting gyroscope from deg/s to mrad/s.\n"))
    return(value * (pi / 180) * 1000)

    # --- Temperature conversions ---
  } else if (from.unit == "C" && to.unit == "F") {
    if (verbose) cat(paste0("Converting temperature from Celsius to Fahrenheit.\n"))
    return((value * 9/5) + 32)
  } else if (from.unit == "F" && to.unit == "C") {
    if (verbose) cat(paste0("Converting temperature from Fahrenheit to Celsius.\n"))
    return((value - 32) * 5/9)
  } else if (from.unit == "C" && to.unit == "K") {
    if (verbose) cat(paste0("Converting temperature from Celsius to Kelvin.\n"))
    return(value + 273.15)
  } else if (from.unit == "K" && to.unit == "C") {
    if (verbose) cat(paste0("Converting temperature from Kelvin to Celsius.\n"))
    return(value - 273.15)
  } else if (from.unit == "F" && to.unit == "K") {
    if (verbose) cat(paste0("Converting temperature from Fahrenheit to Kelvin.\n"))
    return((value - 32) * 5/9 + 273.15)
  } else if (from.unit == "K" && to.unit == "F") {
    if (verbose) cat(paste0("Converting temperature from Kelvin to Fahrenheit.\n"))
    return((value - 273.15) * 9/5 + 32)

    # --- Pressure/Depth conversions ---
    # Hydrostatic approximation depth(m) = pressure(dbar) / (rho * g) * 1e4, with seawater
    # density rho ~ 1025 kg/m^3 and g ~ 9.81 m/s^2, giving 1 dbar ~ 0.9945 m. This is a
    # constant-factor approximation (true density varies with temperature/salinity/depth, and
    # the exact relationship is latitude-dependent), but is accurate to well under 1%.
  } else if (from.unit == "dbar" && to.unit == "m") {
    if (verbose) cat(paste0("Converting depth/pressure from decibar to meters (approx.).\n"))
    return(value * 0.9945)
  } else if (from.unit == "m" && to.unit == "dbar") {
    if (verbose) cat(paste0("Converting depth/pressure from meters to decibar (approx.).\n"))
    return(value / 0.9945)
  } else if (from.unit == "bar" && to.unit == "m") {
    if (verbose) cat(paste0("Converting depth/pressure from bar to meters (approx.).\n"))
    return(value * 9.945)
  } else if (from.unit == "m" && to.unit == "bar") {
    if (verbose) cat(paste0("Converting depth/pressure from meters to bar (approx.).\n"))
    return(value / 9.945)
  } else if (from.unit == "dbar" && to.unit == "bar") {
    if (verbose) cat(paste0("Converting pressure from decibar to bar.\n"))
    return(value / 10)
  } else if (from.unit == "bar" && to.unit == "dbar") {
    if (verbose) cat(paste0("Converting pressure from bar to decibar.\n"))
    return(value * 10)

    # --- Magnetic field conversions ---
  } else if (from.unit == "nT" && to.unit == "uT") {
    if (verbose) cat(paste0("Converting magnetic field from nanotesla to microtesla.\n"))
    return(value / 1000)
  } else if (from.unit == "uT" && to.unit == "nT") {
    if (verbose) cat(paste0("Converting magnetic field from microtesla to nanotesla.\n"))
    return(value * 1000)
  } else if (from.unit == "mG" && to.unit == "uT") {
    if (verbose) cat(paste0("Converting magnetic field from milligauss to microtesla.\n"))
    return(value / 10)
  } else if (from.unit == "uT" && to.unit == "mG") {
    if (verbose) cat(paste0("Converting magnetic field from microtesla to milligauss.\n"))
    return(value * 10)

    # --- Speed conversions ---
  } else if (from.unit == "km/h" && to.unit == "m/s") {
    if (verbose) cat(paste0("Converting speed from km/h to m/s.\n"))
    return(value / 3.6)
  } else if (from.unit == "m/s" && to.unit == "km/h") {
    if (verbose) cat(paste0("Converting speed from m/s to km/h.\n"))
    return(value * 3.6)
  } else if (from.unit == "knot" && to.unit == "m/s") {
    if (verbose) cat(paste0("Converting speed from knots to m/s.\n"))
    return(value * 0.514444)
  } else if (from.unit == "m/s" && to.unit == "knot") {
    if (verbose) cat(paste0("Converting speed from m/s to knots.\n"))
    return(value / 0.514444)
  } else if (from.unit == "mph" && to.unit == "m/s") {
    if (verbose) cat(paste0("Converting speed from mph to m/s.\n"))
    return(value * 0.44704)
  } else if (from.unit == "m/s" && to.unit == "mph") {
    if (verbose) cat(paste0("Converting speed from m/s to mph.\n"))
    return(value / 0.44704)

    # --- Fallback for unsupported conversions ---
  } else {
    warning(paste("No conversion defined from", from.unit, "to", to.unit,
                  "- returning original values for column."), call. = FALSE, immediate = TRUE)
    return(value)
  }
}


##################################################################################################
##################################################################################################
##################################################################################################

#######################################################################################################
# Shared coercion contract for user-supplied columns ##################################################
#######################################################################################################

#' Coerce user-supplied table columns to numeric, reporting any that arrived as FACTORS.
#'
#' A factor here is almost always a symptom, not an intent: the table was read with
#' `stringsAsFactors = TRUE` (the default before R 4.0, and still reachable via `read.csv`), which means
#' other columns in the same file may be affected too. Coercing via labels gives the right answer, but
#' the user is told, because the underlying read is what needs fixing.
#'
#' @param df data.frame. @param cols character. Columns to coerce (missing ones are skipped).
#' @param what character. What to call the table in the message.
#' @keywords internal
#' @noRd
.coerceNumericCols <- function(df, cols, what) {
  cols <- intersect(cols, names(df))
  fac <- cols[vapply(cols, function(c) is.factor(df[[c]]), logical(1))]
  for (c in cols) df[[c]] <- .asNumericSafe(df[[c]])
  if (length(fac))
    cli::cli_warn(c("{length(fac)} numeric column{?s} in {.arg {what}} arrived as {.cls factor}: {.val {fac}}.",
                    "i" = "Read with {.code stringsAsFactors = FALSE}; other columns in that file may be affected too."))
  df
}

#' Numeric coercion for a user-supplied data column.
#'
#' `as.numeric()` on a FACTOR returns its integer LEVEL CODES, not its values. Applied blind to a column
#' the user named, that silently substitutes 1..nlevels for the real measurements: a factor depth column
#' of 100/200/300 m becomes 1/2/3, and every figure and summary built on it is wrong while looking
#' entirely plausible. Factors therefore go via their labels; everything else is coerced directly.
#'
#' Text that does not parse becomes `NA` rather than an error, so callers can decide whether an unusable
#' column is fatal or merely skipped - but they must decide, rather than plotting the NAs.
#' @keywords internal
#' @noRd
.asNumericSafe <- function(z) {
  if (is.factor(z)) z <- as.character(z)
  suppressWarnings(as.numeric(z))
}

# CATS / CEiiA sensor files

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
# reads the paired sidecar and converts every channel to canonical units (g / rad/s / uT). What it
# returns is exactly what `buildTagData()`/the tag assembly needs as input.
#
# Two rules keep it honest, both learned from the code it replaces:
#  1. It NEVER writes to the caller's deferred-issue collectors. Those are locals in importTagData()
#     mutated via `<-` and read by an on.exit closure; assigning to them from inside another function
#     frame would silently vanish and the verbose=0 warning channel would go quiet. Status is RETURNED
#     (`temp_status`, `timezone_mismatch`, `device_clock_mismatch`) and the caller collects.
#  2. It NEVER prints the per-deployment report. Logging in the original is interleaved with parsing
#     (exclusion -> attrs -> assembly -> sensors -> temp -> span -> sidecar -> timezone -> units), so
#     the reader returns report DATA (`assembly`, `unit_notes`, clock notes, ...) and the caller renders it
#     in that order - the pattern `.reportAssembly()` / `.reportSidecar()` already established.


#' The CATS column dialect: `colname -> sensor role + original unit`
#'
#' The header CATS multi-sensor tags write: separate `Date`/`Time` columns, SI-ish units (m/s2, mrad/s),
#' and paired depth/temperature channels. Kept as its own helper so the two dialects `read_cats()` handles
#' are readable side by side, but they compose into one table (\code{.defaultMappings}) because the reader
#' resolves any header against all of them at once - it does not first decide which dialect a folder is.
#' @return A character matrix of `colname, sensor, units` rows.
#' @keywords internal
#' @noRd
.catsMappings <- function() {
  rbind(
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
}


#' The CEiiA column dialect: `colname -> sensor role + original unit`
#'
#' The header CEiiA archival tags write: a single `Date` datetime column, g / deg/s units, and the paddle
#' channels (`Ticks/s`, `Velocity`) that CATS tags lack. Shares no header literal with the CATS dialect
#' (CATS writes `Date (UTC)` / `Accelerometer X [m/s2]`, CEiiA writes `Date` / `Ax (g)`), which is why the
#' format can be identified by resolving a header through the mappings rather than matching any one string:
#' see \code{.catsConfirm}.
#' @return A character matrix of `colname, sensor, units` rows.
#' @keywords internal
#' @noRd
.ceiiaMappings <- function() {
  rbind(
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
}


#' The built-in CATS + CEiiA column mappings, combined
#'
#' The single table `read_cats()` resolves every header against, and the basis of the format detector
#' (\code{.catsConfirm}). The two dialects (\code{.catsMappings}, \code{.ceiiaMappings}) are separate for
#' readability but concatenated here on purpose: the reader identifies neither dialect explicitly, it just
#' maps whatever columns a folder's CSV carries onto sensor roles - so a mixed or partial header resolves
#' as far as it can against the union. Order is CATS-then-CEiiA; nothing downstream depends on it, and the
#' dialects share no header literal, so no row can shadow another.
#' @return A data.frame with columns `colname`, `sensor`, `units`.
#' @keywords internal
#' @noRd
.defaultMappings <- function() {
  out <- as.data.frame(rbind(.catsMappings(), .ceiiaMappings()), stringsAsFactors = FALSE)
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


#' Determine the clock domain of the selected CATS timestamp columns
#'
#' The CATS export can carry two clocks: the sensor logging clock and a device/camera clock. The selected
#' CSV headers and `[logging]` sidecar block describe the former; `[device] utc_offset` describes the
#' latter and must never be used to reinterpret or shift sensor rows.
#' @keywords internal
#' @noRd

.catsClockStatus <- function(file_mapping, sidecar, timezone, ref) {
  roles <- file_mapping$sensor_name_out %in% c("date", "time", "datetime")
  time_headers <- file_mapping$colname_in_csv[roles]
  header_utc <- length(time_headers) > 0L &&
    all(grepl("(^|[^[:alpha:]])UTC([^[:alpha:]]|$)", time_headers, ignore.case = TRUE))
  header_local <- length(time_headers) > 0L &&
    any(grepl("(^|[^[:alpha:]])local([^[:alpha:]]|$)", time_headers, ignore.case = TRUE))

  logging_offset <- if (!is.null(sidecar) && !is.null(sidecar$logging)) {
    sidecar$logging$utc_offset
  } else {
    NA_real_
  }
  if (length(logging_offset) != 1L || !is.finite(logging_offset)) logging_offset <- NA_real_

  # An explicit selected CSV header wins over general logging metadata. A selected local-time column is
  # intentionally governed by the public `timezone` argument; otherwise an explicit logging zone is the
  # best available source declaration.
  if (header_utc) {
    recording_offset <- 0
    source_label <- "Sensor columns are UTC"
  } else if (header_local) {
    recording_offset <- .tzOffsetHours(timezone, ref)
    source_label <- NULL
  } else if (is.finite(logging_offset)) {
    recording_offset <- logging_offset
    source_label <- if (logging_offset == 0) "[logging] is UTC" else "[logging] declares the sensor offset"
  } else {
    recording_offset <- NA_real_
    source_label <- NULL
  }

  requested_offset <- .tzOffsetHours(timezone, ref)
  timezone_mismatch <- is.finite(recording_offset) &&
    abs(recording_offset - requested_offset) > 1 / 3600
  timezone_note <- if (timezone_mismatch) {
    if (recording_offset == 0) {
      sprintf("%s but timezone is %s. Use timezone = \"UTC\".", source_label, timezone)
    } else {
      sprintf("Sensor offset is %gh but timezone %s is %gh.",
              recording_offset, timezone, round(requested_offset, 1))
    }
  } else {
    NULL
  }

  device_offset <- if (!is.null(sidecar) && !is.null(sidecar$device)) {
    sidecar$device$utc_offset
  } else {
    NA_real_
  }
  if (length(device_offset) != 1L || !is.finite(device_offset)) device_offset <- NA_real_

  # Only the sidecar can establish a device-versus-logging difference. Keep this separate from the
  # sensor/source warning: it is actionable for video alignment, not evidence that sensor rows are wrong.
  device_clock_mismatch <- is.finite(logging_offset) && is.finite(device_offset) &&
    abs(logging_offset - device_offset) > 1 / 3600
  # A device-clock timestamp needs this value added to place it in the logging clock domain. Return the
  # fact, not a console sentence: importTagData() deliberately uses a terse inline diagnostic and a more
  # explanatory final issue row, while getVideoClockCorrections() consumes the same sidecar provenance.
  suggested_video_shift_h <- if (device_clock_mismatch) logging_offset - device_offset else NA_real_

  list(recording_utc_offset = recording_offset,
       timezone_mismatch = timezone_mismatch, timezone_note = timezone_note,
       logging_utc_offset = logging_offset, device_utc_offset = device_offset,
       device_clock_mismatch = device_clock_mismatch,
       suggested_video_shift_h = suggested_video_shift_h)
}


#' Read a CATS/CEiiA deployment folder into a canonical sensor frame
#'
#' @param folder Path to the deployment folder.
#' @param sensor.subdirectory Subdirectory holding the sensor CSVs.
#' @param active_mapping,active_mapping_norm The resolved import mapping and its normalised header.
#' @param temp_blacklist_norm Normalised names of blacklisted (electronics) temperature columns.
#' @param required.sensors Channels that must be present, or NULL.
#' @param timezone Time zone the tag recorded its clock in.
#' @param import.sidecar Logical; read the paired sensor sidecar.
#' @param exclude.channels Character vector of channels to drop (resolved from deployment metadata by
#'   the caller). Applied BEFORE unit conversion, so the unit notes describe only retained channels -
#'   exactly as the original ordering did.
#' @param verbose Logical; passed to `.assembleSensorData()` for its own inline reporting.
#' @return A list: `data` (canonical frame, or NULL when unusable), `reason` (why, when `data` is NULL),
#'   `assembly` (pass-through for `.reportAssembly()`), `mapping`, `selected_cols`, `sidecar_info`,
#'   `temp_status`, `excluded`, sensor/source and device/logging clock diagnostics,
#'   `recording_utc_offset`, and `unit_notes`.
#' @keywords internal
#' @noRd
read_cats <- function(folder,
                      sensor.subdirectory,
                      active_mapping,
                      active_mapping_norm,
                      temp_blacklist_norm,
                      required.sensors,
                      timezone,
                      import.sidecar = TRUE,
                      exclude.channels = character(0),
                      verbose = FALSE, ambiguous = "error") {

  # ---- locate, probe and assemble the deployment's sensor CSVs -------------------------------------
  # probe every candidate CSV, keep the schema group of the largest usable file, and merge it using a
  # maximal-unique-coverage policy (resilient to overlapping fragments, duplicate exports and corrupt
  # trailing rows). Returns an assembled data.table with a parsed `datetime` column and renamed columns.
  assembly <- .assembleSensorData(
    folder = folder, sensor.subdirectory = sensor.subdirectory,
    active_mapping = active_mapping, active_mapping_norm = active_mapping_norm,
    temp_blacklist_norm = temp_blacklist_norm,
    required.sensors = required.sensors, timezone = timezone, verbose = verbose, ambiguous = ambiguous)

  # nothing usable: hand the reason back verbatim (the caller string-matches it to tell a pre-flighted
  # "no sensor CSV" folder from a genuine failure, so these strings are part of the contract)
  if (is.null(assembly$data)) {
    return(list(data = NULL, reason = assembly$reason, assembly = assembly, mapping = NULL,
                selected_cols = NULL, sidecar_info = NULL, temp_status = "none",
                excluded = character(0), timezone_mismatch = FALSE, timezone_note = NULL,
                device_clock_mismatch = FALSE, logging_utc_offset = NA_real_,
                device_utc_offset = NA_real_, suggested_video_shift_h = NA_real_,
                recording_utc_offset = NA_real_,
                unit_notes = character(0)))
  }

  sensor_data   <- assembly$data
  file_mapping  <- assembly$mapping
  # the copy that gets RECORDED. `file_mapping$colname_in_csv` itself must keep the file's own bytes -
  # it is what fread(select=) matches against - so normalise the record, never the key.
  selected_cols <- .toUTF8(file_mapping$colname_in_csv)

  # ---- drop channels the caller flagged unusable for this deployment -------------------------------
  # Dropping - rather than NA-ing - makes the channel simply ABSENT, so every downstream analysis skips
  # it through the package's partial-sensor support. Done BEFORE unit conversion so a dropped channel
  # never contributes to the conversion notes.
  excluded_channels <- intersect(exclude.channels, names(sensor_data))
  if (length(excluded_channels)) sensor_data[, (excluded_channels) := NULL]

  # ---- temperature reliability (returned, never collected here) ------------------------------------
  temp_status <- attr(file_mapping, "temp_status") %||% "none"

  # ---- sidecar + clock domains ---------------------------------------------------------------------
  # The sensor log and the device/camera clock are distinct. Sidecar calibration constants are retained
  # as provenance but never applied; clock metadata validate the caller's timezone and never shift rows.
  sidecar_info <- if (isTRUE(import.sidecar)) .readSidecar(assembly$primary_file) else NULL
  clock <- .catsClockStatus(file_mapping, sidecar_info, timezone, sensor_data$datetime[1])

  # ---- convert every sensor group to its canonical unit --------------------------------------------
  # `datetime` was resolved during assembly; unit conversion does not change its instants.
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
       selected_cols = selected_cols, sidecar_info = sidecar_info,
       temp_status = temp_status, excluded = excluded_channels,
       timezone_mismatch = clock$timezone_mismatch, timezone_note = clock$timezone_note,
       device_clock_mismatch = clock$device_clock_mismatch,
       logging_utc_offset = clock$logging_utc_offset,
       device_utc_offset = clock$device_utc_offset,
       suggested_video_shift_h = clock$suggested_video_shift_h,
       recording_utc_offset = clock$recording_utc_offset,
       unit_notes = unit_notes, clock = assembly$clock)
}

# Little Leonardo sensor files

#######################################################################################################
# read_little_leonardo(): the Little Leonardo format reader ###########################################
#######################################################################################################
#
# The second per-manufacturer reader, written against the contract read_cats() established: files on
# disk -> a canonical sensor frame, returning status and report DATA rather than printing or collecting.
#
# Little Leonardo archival loggers differ from the CATS/CEiiA exports in four ways that between them
# explain why importTagData() could never read them, and why the reader (not the caller) must absorb it:
#
#  1. The channels arrive in TWO files at DIFFERENT rates - `<id>_A.txt` (tri-axial acceleration, high
#     rate) and `<id>_DT.txt` (depth / temperature / video flag, typically 1 Hz) - and must be merged
#     onto one grid.
#  2. Neither file carries a real clock: the header reads `START DATE 0000/00/00`. The absolute start
#     comes from the deployment metadata (`start`), and every timestamp is synthesised from it plus the
#     rate. This is why `start` is part of this reader's signature and not of read_cats().
#  3. The rate is declared in the header as "N msec/point", not implied by timestamps.
#  4. The layout is a fixed header block followed by comma-separated columns with padding, a trailing
#     comma and no-leading-zero decimals (" .14,-.05, 1.34,").
#
# Acceleration is already recorded in g and depth/temperature in m/degC, so no unit conversion is needed
# - the frame is canonical as read.


#' Parse the sampling rate declared in a Little Leonardo header ("N msec/point" -> Hz)
#' @return Rate in Hz, or NA_real_ if the header does not declare one.
#' @keywords internal
#' @noRd
.llHeaderRate <- function(lines) {
  hit <- grep("msec/point", lines, fixed = TRUE, value = TRUE)
  if (!length(hit)) return(NA_real_)
  ms <- suppressWarnings(as.numeric(gsub("[^0-9.]", "", hit[1])))
  if (!length(ms) || !is.finite(ms) || ms <= 0) return(NA_real_)
  1000 / ms
}

#' Find the data start line of a Little Leonardo file (the line after the column header)
#'
#' The pattern must be anchored to the WHOLE column-header line, not just its first field: the title line
#' of a depth file reads "Depth, Temp, and Video Data", which a loose "^Depth," would match, silently
#' skipping to the wrong line and shifting every depth/temperature value. The last match inside the
#' header window is taken as a second guard (the column header is always the last non-data header line).
#' @return Number of lines to skip (up to and including the column header), or NA if none is found.
#' @keywords internal
#' @noRd
.llSkip <- function(lines, header_pattern) {
  hit <- grep(header_pattern, lines)
  if (!length(hit)) return(NA_integer_)
  as.integer(hit[length(hit)])   # skip up to AND including the column-header line
}

#' Split a Little Leonardo column-header line into trimmed channel names
#'
#' The header row - not `ncol()` - is what says how many channels a file really carries. Data rows in the
#' acceleration file end in a trailing comma, so `fread()` returns one more (all-NA) column than there are
#' channels; counting columns would therefore report an extra channel on every healthy file. The header
#' has no trailing field, so splitting it gives the true count and, more usefully, the names.
#' @return Character vector of non-empty column names (possibly length 0).
#' @keywords internal
#' @noRd
.llColumnNames <- function(line) {
  if (!length(line) || is.na(line[1])) return(character(0))
  nm <- trimws(strsplit(line[1], ",", fixed = TRUE)[[1]])
  nm[nzchar(nm)]
}

#' Locate a deployment's Little Leonardo files
#' @return A list with `accel` and `depth` paths (NA when absent).
#' @keywords internal
#' @noRd
.llDiscover <- function(folder, sensor.subdirectory = NULL) {
  dir <- if (!is.null(sensor.subdirectory) && nzchar(sensor.subdirectory) &&
             dir.exists(file.path(folder, sensor.subdirectory))) {
    file.path(folder, sensor.subdirectory)
  } else folder
  pick <- function(pat) {
    f <- list.files(dir, pattern = pat, full.names = TRUE, ignore.case = TRUE)
    if (length(f)) f[1] else NA_character_
  }
  list(accel = pick("_A\\.txt$"), depth = pick("_DT\\.txt$"))
}

#' Does this folder look like a Little Leonardo deployment?
#'
#' The reader's own discovery probe (`has_data` in `.readerFormats()`), kept beside it so a new format never
#' edits a central sniffer. It requires the accel file AND a signature header line, so it cannot claim a
#' folder in which it would find no file at all.
#'
#' The window is 12 lines, matching the reader's own (`read_little_leonardo` reads 12): a shorter one would
#' skip a folder the reader can read perfectly well, which under `format = "auto"` would make auto weaker
#' than an explicit `format = "little_leonardo"`.
#' @keywords internal
#' @noRd
.llDetect <- function(folder, sensor.subdirectory = NULL) {
  f <- .llDiscover(folder, sensor.subdirectory)
  if (is.na(f$accel)) return(FALSE)
  head_lines <- tryCatch(readLines(f$accel, n = 12L, warn = FALSE), error = function(e) character(0))
  any(grepl("ACCELERATION DATA", head_lines, fixed = TRUE)) ||
    any(grepl("msec/point", head_lines, fixed = TRUE))
}


#' Is this positively a Little Leonardo deployment?
#'
#' The `confirm` half of the `little_leonardo` entry (see `.readerFormats`). It asks the READER'S OWN
#' question rather than inventing cues: can the declared sampling rate be parsed? `importTagData()` never
#' passes `sampling.rate`, so a folder whose rate cannot be read is one `read_little_leonardo()` refuses
#' outright ("could not determine the sampling rate") - which makes this predicate never stricter than the
#' explicit format, and impossible to overfit to the single export available.
#'
#' It also cannot drift from the parser, because it IS the parser: if a sibling model declares its rate
#' differently, fixing `.llHeaderRate()` fixes the reader and detection together.
#'
#' This is what separates a real LL export from a look-alike: another vendor's file whose header merely says
#' "ACCELERATION DATA" passes `.llDetect()` (the title cue) but has no `msec/point` rate, so `confirm` - and
#' therefore `detect` - is FALSE.
#' @param folder,sensor.subdirectory As passed to `importTagData()`.
#' @return TRUE / FALSE. Never errors.
#' @keywords internal
#' @noRd
.llConfirm <- function(folder, sensor.subdirectory = NULL) {
  f <- .llDiscover(folder, sensor.subdirectory)$accel     # the same discovery; never widened
  if (is.na(f)) return(FALSE)
  head_lines <- tryCatch(readLines(f, n = 12L, warn = FALSE), error = function(e) character(0))
  hz <- .llHeaderRate(head_lines)
  is.finite(hz) && hz > 0
}


#' Read a Little Leonardo deployment folder into a canonical sensor frame
#'
#' @param folder Path to the deployment folder.
#' @param sensor.subdirectory Optional subdirectory holding the logger files; the folder itself is used
#'   when absent (Little Leonardo exports usually sit directly in the deployment folder).
#' @param start POSIXct. The instant the logger began recording (the raw carries no clock). Comes from
#'   the deployment metadata; without it the record cannot be placed in time and the read is refused.
#' @param timezone Display time zone; the POSIXct `start` always preserves its absolute instant.
#' @param sampling.rate Optional override (Hz); by default it is read from the file header.
#' @param exclude.channels Channels to drop (resolved from metadata by the caller).
#' @param verbose Unused; present for reader-contract symmetry with read_cats().
#' @return The reader contract: `data` (canonical frame, or NULL), `reason`, `assembly`, `mapping`,
#'   `selected_cols`, `sidecar_info`, `temp_status`, `excluded`, clock diagnostics,
#'   `recording_utc_offset`, `unit_notes`, `unread_columns` (channels the files declare but this reader does not read - one
#'   entry per file, `"<file>: <names>"`; the caller reports them), and
#'   `ancillary` - a named list of primary-tag ancillary streams parsed in-band, or
#'   `NULL`. Currently this carries `video` (the DT file's per-second camera-on flag, transition-encoded
#'   into `meta$ancillary$video` with the same shape as the WC wet/dry stream) when a video column is
#'   present and the camera recorded at all.
#' @keywords internal
#' @noRd
read_little_leonardo <- function(folder,
                                 sensor.subdirectory = NULL,
                                 start = NULL,
                                 timezone = "UTC",
                                 sampling.rate = NULL,
                                 exclude.channels = character(0),
                                 verbose = FALSE) {

  # the contract's failure shape: data = NULL + a reason the caller reports verbatim
  fail <- function(reason, assembly = NULL) {
    list(data = NULL, reason = reason, assembly = assembly, mapping = NULL, selected_cols = NULL,
         sidecar_info = NULL, temp_status = "none", excluded = character(0),
         timezone_mismatch = FALSE, timezone_note = NULL,
         device_clock_mismatch = FALSE, logging_utc_offset = NA_real_,
         device_utc_offset = NA_real_, suggested_video_shift_h = NA_real_,
         recording_utc_offset = NA_real_,
         unit_notes = character(0),
         unread_columns = character(0))
  }

  files <- .llDiscover(folder, sensor.subdirectory)
  if (is.na(files$accel)) return(fail("no sensor file: no Little Leonardo '_A.txt' acceleration file found"))

  head_lines <- tryCatch(readLines(files$accel, n = 12L, warn = FALSE), error = function(e) character(0))
  if (!length(head_lines)) return(fail(sprintf("could not read %s", basename(files$accel))))

  # rate: declared in the header ("N msec/point") unless the caller overrides it
  hz <- sampling.rate %||% .llHeaderRate(head_lines)
  if (!is.finite(hz) || hz <= 0) {
    return(fail("could not determine the sampling rate: no 'msec/point' header line (pass `sampling.rate`)"))
  }

  # the raw has no clock (START DATE 0000/00/00): the absolute start must be supplied
  if (is.null(start) || !inherits(start, "POSIXct") || length(start) != 1L || !is.finite(as.numeric(start))) {
    return(fail("no recording start time: Little Leonardo files carry no clock, so a `data_start` is required"))
  }

  # ---- acceleration (X/Y/Z, in g), skipping the header block ---------------------------------------
  skip_a <- .llSkip(head_lines, "^\\s*X\\s*,")
  if (is.na(skip_a)) return(fail(sprintf("unrecognised header in %s: no 'X , Y , Z' column row", basename(files$accel))))
  accel_names <- .llColumnNames(head_lines[skip_a])
  acc <- tryCatch(data.table::fread(files$accel, skip = skip_a, header = FALSE, fill = TRUE,
                                    showProgress = FALSE),
                  error = function(e) NULL)
  if (is.null(acc) || !nrow(acc) || ncol(acc) < 3L) return(fail(sprintf("could not parse %s", basename(files$accel))))
  acc <- acc[, 1:3]                                   # a trailing comma yields a 4th, empty column
  data.table::setnames(acc, c("ax", "ay", "az"))

  # Channels beyond X/Y/Z are NOT read. Some Little Leonardo models ("3M" in the model name) write the
  # tri-axial compass into columns 4-6 of this same file. Reading them would mean guessing their units,
  # which the downstream magnetometer calibration cannot do safely, so they are reported as unread rather
  # than silently dropped (the caller renders this) or silently mapped.
  a_unread <- if (length(accel_names) > 3L) accel_names[-(1:3)] else character(0)

  n <- nrow(acc)
  dt <- acc
  # Resolve the supplied instant once; elapsed seconds preserve cadence through DST transitions.
  resolved <- .resolveTagTime(start, timezone)
  resolved$clock$generated <- TRUE
  dt[, datetime := resolved$datetime + (seq_len(.N) - 1L) / hz]
  .validateInstants(dt$datetime)

  # ---- depth / temperature (lower rate, separate file) ---------------------------------------------
  # expanded onto the acceleration grid by whole-second index, which is how the two streams relate: the
  # DT file carries one row per second of the same recording.
  n_dt <- 0L
  video_anc <- NULL                                   # video-coverage ancillary (meta$ancillary$video), if any
  d_unread <- character(0)
  if (!is.na(files$depth)) {
    dhead <- tryCatch(readLines(files$depth, n = 10L, warn = FALSE), error = function(e) character(0))
    skip_d <- .llSkip(dhead, "^\\s*Depth\\s*,")
    if (!is.na(skip_d)) {
      depth_names <- .llColumnNames(dhead[skip_d])
      # The third column is a camera flag ONLY when the header says so. A propeller ("P" in the model
      # name) logger puts its own channel here, and a column count cannot tell the two apart - both are
      # three wide. Encoding a speed trace as camera-on coverage would fabricate a result, so the name
      # decides, and anything unrecognised is left unread and reported.
      third_is_video <- length(depth_names) >= 3L && grepl("video", depth_names[3L], ignore.case = TRUE)
      dtemp <- tryCatch(data.table::fread(files$depth, skip = skip_d, header = FALSE, fill = TRUE,
                                          showProgress = FALSE),
                        error = function(e) NULL)
      if (!is.null(dtemp) && nrow(dtemp) && ncol(dtemp) >= 2L) {
        n_dt <- nrow(dtemp)
        keep <- min(ncol(dtemp), 3L)
        dtemp <- dtemp[, seq_len(keep), with = FALSE]
        data.table::setnames(dtemp, c("depth", "temp", if (third_is_video) "video" else "col3")[seq_len(keep)])
        if (length(depth_names) >= 3L && !third_is_video) d_unread <- c(d_unread, depth_names[3L])
        if (length(depth_names) > 3L) d_unread <- c(d_unread, depth_names[-(1:3)])
        idx <- ((seq_len(n) - 1L) %/% as.integer(round(hz))) + 1L
        idx[idx > n_dt] <- n_dt                       # a short DT file holds the last value
        dt[, depth := dtemp$depth[idx]]
        if ("temp" %in% names(dtemp)) dt[, temp := dtemp$temp[idx]]

        # The DT file's third column is a per-second 0/1 flag: was the on-board camera recording? It is
        # coverage metadata, NOT a measurand, so it is preserved as a transition-encoded ancillary stream
        # (meta$ancillary$video) - the same shape as the WC wet/dry stream - rather than a sensor channel.
        # Mapped onto the sensor timeline by the same whole-second index as depth/temp, then encoded.
        if ("video" %in% names(dtemp)) {
          von <- suppressWarnings(as.numeric(dtemp$video[idx])) != 0
          von[is.na(von)] <- FALSE
          if (any(von)) {
            enc <- .transitionEncode(dt$datetime, von)
            video_anc <- list(source = "Little Leonardo video flag", encoding = "transitions",
                              data = data.frame(datetime = enc$datetime, video = enc$state))
          }
        }
      }
    }
  }

  # ---- caller-specified channel drops (before any conversion, per the reader contract) -------------
  excluded_channels <- intersect(exclude.channels, names(dt))
  if (length(excluded_channels)) dt[, (excluded_channels) := NULL]

  present <- intersect(.sensorChannels(), names(dt))
  if (!length(present)) return(fail("no usable sensor channels were read"))
  data.table::setcolorder(dt, c("datetime", present))

  # report data the caller renders (mirrors read_cats' shape). Acceleration is recorded in g and depth in
  # metres, so nothing is converted - the note records the synthesis instead, which is the fact a user
  # needs to see for a logger whose timestamps nautilus invented.
  assembly <- list(primary_file = files$accel,
                   decisions = sprintf("%s (%s rows) + %s (%s rows)",
                                       basename(files$accel), format(n, big.mark = ","),
                                       if (is.na(files$depth)) "no _DT.txt" else basename(files$depth),
                                       format(n_dt, big.mark = ",")),
                   n_files = sum(!is.na(c(files$accel, files$depth))), n_excluded = 0L,
                   duplicates_removed = 0L, reason = NULL)

  # channels the file declares but this reader does not yet read, named per file so the caller can say
  # exactly what was left behind (see `unread_columns` in the return contract)
  unread_columns <- c(
    if (length(a_unread)) sprintf("%s: %s", basename(files$accel), paste(a_unread, collapse = ", ")),
    if (length(d_unread)) sprintf("%s: %s", basename(files$depth), paste(d_unread, collapse = ", "))
  )

  list(data = dt, reason = NULL, assembly = assembly, mapping = NULL,
       selected_cols = c("X", "Y", "Z", if (!is.na(files$depth)) c("Depth", "Temp")),
       sidecar_info = NULL, temp_status = "none", excluded = excluded_channels,
       timezone_mismatch = FALSE, timezone_note = NULL,
       device_clock_mismatch = FALSE, logging_utc_offset = NA_real_,
       device_utc_offset = NA_real_, suggested_video_shift_h = NA_real_,
       recording_utc_offset = .tzOffsetHours(resolved$clock$source_timezone, start),
       clock = resolved$clock,
       ancillary = if (!is.null(video_anc)) list(video = video_anc) else NULL,
       unread_columns = unread_columns,
       unit_notes = sprintf("timestamps synthesised from data_start + %g Hz (header)", hz))
}

# Wildlife Computers ancillary streams

#######################################################################################################
# read_wc() / attachAncillary(): the ancillary axis ###################################################
#######################################################################################################
#
# The SECOND axis of ingestion, orthogonal to the primary-format readers (read_cats / read_little_leonardo).
#
# A co-deployed Wildlife Computers tag is not a "format" of the primary logger - it is a separate device,
# with its own clock, contributing supporting streams (position fixes, a wet/dry signal). Folding it into
# the primary reader would couple every format to every ancillary source (a CATS+WC reader, an LL+WC
# reader, a CATS+acoustic reader...), which is exactly the combinatorial explosion the reader seam exists
# to prevent. So it gets its own reader, and attaches to an ALREADY-BUILT tag:
#
#     read_wc(files)  ->  ancillary records  ->  attachAncillary(tag, anc, align = ...)  ->  tag
#
# attachAncillary() is the single home for clock alignment. Adding another ancillary source (an acoustic
# receiver, a light logger) means a new reader plus, usually, an existing align strategy - and no change
# to any primary reader, to buildTagData(), or to the alignment logic itself.
#
# Layout, matching read_cats(): this file holds the SEAM - the reader entry point and the attach - while the
# low-level parsing/alignment helpers it calls (.readPositionsAncillary(), .readDryAncillary(),
# .alignWCClocks(), .estimateClockOffset()) stay beside their CATS counterparts in importTagData.R.
#
# `kind` is TRANSPORT metadata used to dispatch the attach; it is deliberately NOT stored. What lands in
# `meta$ancillary$<name>` keeps the exact shape downstream already reads (processTagData()'s depth-drift
# correction consumes `meta$ancillary$dry$data`), so this extraction changes no stored contract.


#' Validate an in-flight ancillary record.
#'
#' A lightweight typed list rather than an S3 class: dispatch is a `switch` over a handful of kinds,
#' validation is this function, and the record is internal plumbing between a reader and the attach - so a
#' class would add ceremony to the object model without buying anything (see DESIGN_INGESTION.md 8).
#' @param x A list with `kind` ("positions" / "events" / "channel"), `source`, and `data`.
#' @keywords internal
#' @noRd
.validateAncillary <- function(x, arg = "ancillary") {
  if (!is.list(x) || is.null(x$kind) || is.null(x$data)) {
    .abort("{.arg {arg}} must be a list with at least {.field kind} and {.field data}.")
  }
  if (!x$kind %in% c("positions", "events", "channel")) {
    .abort(c("Unknown ancillary kind {.val {x$kind}}.",
             "i" = "Supported: {.val {c('positions', 'events', 'channel')}}."))
  }
  invisible(TRUE)
}


#' Read a co-deployed Wildlife Computers tag's ancillary streams
#'
#' @param locations_file Path to a WC `...Locations.csv` (or NA / nonexistent).
#' @param archive_file Path to a WC `...-Archive.csv` (or NA / nonexistent); supplies the wet/dry signal
#'   and the shared depth channel the clock alignment cross-correlates against.
#' @return A list of typed ancillary records (possibly empty): `positions` (kind "positions" - the COMPLETE
#'   fix history at its own cadence, never snapped to sensor rows) and `dry` (kind "events" -
#'   transition-encoded wet/dry). `archive_file` rides along for the aligner.
#' @keywords internal
#' @noRd
read_wc <- function(locations_file, archive_file) {
  # tag the record by assignment, not `c(list(kind = ...), rec)`: prepending via c() silently drops the
  # record's attributes (and class), so a reader that ever attaches one would lose it here without a word.
  tag_kind <- function(rec, kind) { if (is.null(rec)) return(NULL); rec$kind <- kind; rec }

  out <- list()
  pos <- .readPositionsAncillary(locations_file)
  if (!is.null(pos)) out$positions <- tag_kind(pos, "positions")
  dry <- .readDryAncillary(archive_file)
  if (!is.null(dry)) out$dry <- tag_kind(dry, "events")
  for (nm in names(out)) .validateAncillary(out[[nm]], nm)
  attr(out, "archive_file") <- archive_file
  out
}


#' Align a co-deployed device's streams onto the primary tag's clock and attach them
#'
#' The one place clock alignment lives. A co-deployed tag runs its own clock, offset from the primary's by
#' anything from seconds to many minutes; that offset is invisible without a shared signal, yet it silently
#' corrupts every step that combines the streams (the depth zero-offset correction, dead-reckoning anchors).
#'
#' One strategy exists today: `"depth-xcorr"` (WC) cross-correlates the two devices' depth traces - the same
#' physical quantity logged by both - and shifts the WC-clock streams onto the primary timeline. It is
#' conservative: it abstains and shifts nothing when the evidence is weak, recording why.
#'
#' The argument is the seam for future sources whose clock is trustworthy enough to join as-is (GPS/UTC), or
#' that share some other signal. Anything else is an error rather than a silent pass-through: a strategy that
#' quietly stored streams WITHOUT a provenance record would repeat the exact bug this attach was written to
#' avoid. Implementing a new one means emitting an `info` record too - not skipping it.
#'
#' To disable alignment for a WC deployment, use the control (`alignmentControl(method = "none")`), which
#' still records that the aligner was switched off.
#' @param data The primary sensor frame (needs `datetime` and `depth`) - the alignment reference.
#' @param anc A list of ancillary records from `read_wc()` (or empty).
#' @param align A `nautilus_alignment` control (see \code{alignmentControl}).
#' @param strategy Alignment strategy; see Details.
#' @return A list: `ancillary` (a named list ready to merge into `meta$ancillary`, in the exact shape
#'   downstream reads - `kind` stripped) and `info` (the alignment provenance, for reporting).
#' @keywords internal
#' @noRd
attachAncillary <- function(data, anc, align, strategy = "depth-xcorr") {
  # NOTE: no early return on an empty `anc`. The aligner is run even when there are no streams to shift,
  # because it still returns a provenance record saying why it abstained - and that record is stored for
  # EVERY deployment, including those with no co-deployed device. Short-circuiting here silently dropped
  # meta$ancillary$alignment from every WC-less tag.
  positions_anc <- anc$positions
  dry_anc       <- anc$dry
  archive_file  <- attr(anc, "archive_file")

  # `kind` is transport-only: the aligner and the stored record both use the original shape
  strip <- function(x) { if (is.null(x)) return(NULL); x$kind <- NULL; x }
  positions_anc <- strip(positions_anc); dry_anc <- strip(dry_anc)

  if (!identical(strategy, "depth-xcorr")) {
    .abort(c("Unsupported alignment {.arg strategy}: {.val {strategy}}.",
             "i" = "Supported: {.val {'depth-xcorr'}}.",
             "i" = "To disable alignment, use {.code alignmentControl(method = \"none\")}."))
  }
  aln <- .alignWCClocks(data, positions_anc, dry_anc, archive_file, align)
  positions_anc <- aln$positions_anc
  dry_anc       <- aln$dry_anc
  info          <- aln$info

  out <- list()
  if (!is.null(dry_anc))       out$dry       <- dry_anc
  if (!is.null(positions_anc)) out$positions <- positions_anc
  if (!is.null(info))          out$alignment <- info
  list(ancillary = out, info = info)
}

# Import preflight, file assembly, ancillary input and clock alignment.

# importTagData helpers ----

#######################################################################################################
# Import and standardise archival tag data ############################################################
#######################################################################################################

# The import loop records facts; these helpers own their presentation. Keeping the two separate lets a
# detailed deployment block stay terse while the final cohort report explains the same finding fully.
# Category and reason codes are internal contracts: tests and renderers may rely on them, but user-facing
# wording remains free to improve without changing the readers.
.importIssueSpecs <- function() {
  list(
    skipped_folder = list(title = "Skipped folders: no sensor file found", order = 1L),
    missing_data = list(title = "Missing data: no matching folder in data.folders", order = 2L),
    failed_import = list(title = "Folders could not be imported", order = 3L),
    sensor_timezone = list(title = "Sensor/source time-zone mismatch", order = 4L),
    clock_alignment = list(title = "Clock alignment not applied", order = 5L),
    temperature_discarded = list(title = "Temperature discarded: electronics sensor only", order = 6L),
    temperature_override = list(title = "Temperature override: electronics sensor used", order = 7L),
    device_clock = list(title = "Device clock mismatch", order = 8L),
    unread_channels = list(title = "Unread channels", order = 9L)
  )
}

.newImportIssue <- function(category, id, reason_code = NA_character_, details = list(), key = id) {
  list(category = as.character(category), id = as.character(id), key = as.character(key),
       reason_code = as.character(reason_code %||% NA_character_), details = details)
}

.formatClockOffset <- function(x) {
  if (length(x) != 1L || !is.finite(x)) return("unknown")
  x <- .noNegZero(x, 6)
  paste0(if (x > 0) "+" else "", format(x, trim = TRUE, scientific = FALSE), "h")
}

.formatImportIssueReason <- function(issue) {
  d <- issue$details
  switch(
    issue$category,
    failed_import = as.character(d$reason %||% "Unspecified import failure"),
    sensor_timezone = sub("[.]$", "", as.character(d$note %||% "Time-zone declarations disagree")),
    clock_alignment = switch(
      issue$reason_code,
      no_shared_depth_samples = "No shared depth samples",
      insufficient_overlap = {
        if (is.finite(d$overlap_seconds %||% NA_real_) && is.finite(d$minimum_minutes %||% NA_real_))
          sprintf("Insufficient depth overlap (%s < %g min)",
                  .fmt_duration(max(d$overlap_seconds, 0)), d$minimum_minutes)
        else "Insufficient depth overlap"
      },
      flat_reference_depth = "Reference depth too flat to align",
      degenerate_correlation = "Degenerate cross-correlation",
      search_edge = "Best lag at search edge (increase alignment max.lag)",
      no_wc_streams = "No Wildlife Computers streams to align",
      primary_depth_missing = "Primary tag has no depth channel",
      no_archive_depth = "No shared depth channel",
      alignment_disabled = "Alignment disabled",
      low_correlation = {
        if (is.finite(d$correlation %||% NA_real_) && is.finite(d$threshold %||% NA_real_))
          sprintf("Peak correlation too low (%.2f < %.2f)", d$correlation, d$threshold)
        else "Peak correlation too low"
      },
      sub("[.]$", "", as.character(d$reason %||% "Unspecified reason"))
    ),
    device_clock = {
      logging <- .formatClockOffset(d$logging_utc_offset %||% NA_real_)
      if (identical(logging, "0h")) logging <- "UTC"
      device <- .formatClockOffset(d$device_utc_offset %||% NA_real_)
      shift <- .formatClockOffset(d$suggested_video_shift_h %||% NA_real_)
      hint <- if (!identical(shift, "unknown")) paste0(" (Video may need ", shift, " shift)") else ""
      paste0("[logging] ", logging, " vs [device] ", device, hint)
    },
    unread_channels = as.character(d$columns %||% "Unspecified columns"),
    ""
  )
}

.groupImportIssues <- function(issues) {
  if (!length(issues)) return(list())
  specs <- .importIssueSpecs()
  rows <- data.frame(
    category = vapply(issues, `[[`, "", "category"),
    id = vapply(issues, `[[`, "", "id"),
    key = vapply(issues, `[[`, "", "key"),
    reason = vapply(issues, .formatImportIssueReason, ""),
    stringsAsFactors = FALSE
  )
  rows <- unique(rows)
  known <- names(sort(vapply(specs, `[[`, integer(1), "order")))
  categories <- unique(rows$category)
  categories <- c(known[known %in% categories], setdiff(categories, known))
  lapply(categories, function(category) {
    x <- rows[rows$category == category, , drop = FALSE]
    reasons <- unique(x$reason)
    detail <- vapply(reasons, function(reason) {
      ids <- unique(x$id[x$reason == reason])
      ids <- paste(ids, collapse = ", ")
      if (nzchar(reason)) paste0(reason, ": ", ids) else ids
    }, "")
    list(category = category,
         title = specs[[category]]$title %||% category,
         count = length(unique(x$key)),
         detail = unname(detail))
  })
}

.formatImportPreflight <- function(issues) {
  groups <- .groupImportIssues(Filter(function(x) x$category %in% c("missing_data", "skipped_folder"), issues))
  vapply(groups, function(g) {
    # These two categories deliberately have no sub-reason: their detail rows are plain ID lists.
    ids <- paste(g$detail, collapse = ", ")
    if (identical(g$category, "missing_data")) {
      sprintf("Missing data: %d metadata deployment%s had no matching folder in data.folders (%s)",
              g$count, if (g$count != 1L) "s" else "", ids)
    } else {
      sprintf("skipping %d folder%s (no sensor file found): %s",
              g$count, if (g$count != 1L) "s" else "", ids)
    }
  }, "")
}

.renderImportIssues <- function(lvl, issues) {
  groups <- .groupImportIssues(issues)
  if (lvl < 1L || !length(groups)) return(invisible(NULL))
  cli::cli_text("")
  .log_h2(lvl, "ISSUES", min_level = 1L)
  for (i in seq_along(groups)) {
    if (i > 1L) cli::cli_text("")
    heading <- sprintf("%s (%d)", groups[[i]]$title, groups[[i]]$count)
    cli::cli_alert_warning("{.strong {heading}}")
    for (detail in groups[[i]]$detail) .log_issue_detail(lvl, detail)
  }
  invisible(NULL)
}

.warnImportIssues <- function(issues) {
  for (group in .groupImportIssues(issues)) {
    warning(paste(c(sprintf("%s (%d)", group$title, group$count),
                    paste0("  - ", group$detail)), collapse = "\n"), call. = FALSE)
  }
  invisible(NULL)
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

#' Offset (in hours) of a civil zone at an instant, or a fixed UTC offset. Unknown labels stay unknown.
#' @keywords internal
#' @noRd
.tzOffsetHours <- function(tz, ref = Sys.time()) {
  zone <- tryCatch(.timeZoneSpec(tz), error = function(e) NULL)
  if (is.null(zone)) return(NA_real_)
  if (!is.null(zone$offset)) return(zone$offset / 3600)
  if (!length(ref) || !inherits(ref, "POSIXct") || !is.finite(as.numeric(ref[1]))) return(NA_real_)
  # Offset formatting avoids dropping the reference's fractional second and treating that loss as drift.
  offset <- format(ref[1], "%z", tz = zone$display)
  .timeZoneSpec(offset)$offset / 3600
}

#' Parse and attach a POSIXct `datetime` column to a freshly-read sensor table
#'
#' Handles a single datetime column or separate date + time columns. Raw strings are interpreted once;
#' POSIXct values already denote instants. No artificial epsilon or timezone shift is added.
#' @keywords internal
#' @noRd

.addDatetime <- function(d, file_mapping, timezone, ambiguous = "error") {
  sensors <- file_mapping$sensor_name_out
  if ("datetime" %in% sensors) {
    parsed <- .parseTagClock(d$datetime, timezone, ambiguous)
    d[, datetime := parsed$datetime]
  } else if (all(c("date", "time") %in% sensors)) {
    parsed <- .parseTagClock(paste(d$date, d$time), timezone, ambiguous)
    d[, datetime := parsed$datetime]
    d[, c("date", "time") := NULL]
  } else .abort("No datetime or date + time columns were mapped.")
  data.table::setattr(d, "nautilus.clock", parsed$clock)
  d
}

#' Locate, probe and assemble all sensor CSVs for one individual
#'
#' Implements robust multi-CSV ingestion: it probes every candidate CSV
#' (header schema, datetime capability, required sensors, size), keeps the
#' schema group of the largest usable file (other schemas are treated as
#' separate devices and skipped), then assembles the group using a
#' maximal-unique-coverage policy - reading files largest-first and keeping
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
                                verbose = TRUE, ambiguous = "error") {

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
  # `.probeSensorCSV()` (R/utils-import.R) is shared with the format detector, so "a usable CATS file" has one
  # definition. Only `has_required` is added here: it depends on this call's `required.sensors`, which
  # detection deliberately never sees.
  probes <- lapply(files, function(f) {
    p <- .probeSensorCSV(f, active_mapping, active_mapping_norm, temp_blacklist_norm)
    if (is.null(p)) return(NULL)
    p$has_required <- .meetsRequirement(p$sensors)
    p
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
  clock_sources <- list()
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
    d <- tryCatch(.addDatetime(d, fm, timezone, ambiguous), error = function(e) e)
    if (inherits(d, "error"))
      return(list(data = NULL, reason = paste0("invalid timestamps in ", basename(p$file), ": ",
                                              conditionMessage(d)), decisions = decisions))
    clock <- attr(d, "nautilus.clock", exact = TRUE)
    data.table::setattr(d, "nautilus.clock", NULL)
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
    if (nrow(d_keep) > 0) {
      assembled[[length(assembled) + 1]] <- d_keep
      clock_sources[[p$file]] <- clock
    }
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

  source_zones <- unique(vapply(clock_sources, function(x) x$source_timezone, character(1)))
  clock <- list(mode = "raw_clock", source_timezone = source_zones,
                requested_timezone = timezone, display_timezone = .timeZoneSpec(timezone)$display, generated = FALSE,
                ambiguous = ambiguous,
                explicit_offsets = unique(unlist(lapply(clock_sources, `[[`, "explicit_offsets"))),
                sources = clock_sources)
  list(data = out, mapping = primary$mapping, primary_file = primary$file, clock = clock,
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
# Internal helpers for sidecar ingestion #######################################
################################################################################

#' Parse a comma-separated numeric list (e.g. "-37.8, -1") into a numeric vector
#' @keywords internal
#' @noRd

.parseNumList <- function(s) {
  if (length(s) != 1 || is.na(s) || !nzchar(trimws(s))) return(numeric(0))
  suppressWarnings(as.numeric(trimws(strsplit(s, ",")[[1]])))
}

#' Parse one CATS `[logging]` timestamp
#'
#' Recognises only the unambiguous UTC/GMT labels. Unknown labels and malformed values are preserved as
#' raw provenance but are not guessed into an instant or UTC offset.
#' @keywords internal
#' @noRd

.parseCATSLoggingTime <- function(x) {
  missing_time <- as.POSIXct(NA_real_, origin = "1970-01-01", tz = "UTC")
  out <- list(raw = x, datetime = missing_time, timezone_label = NA_character_, utc_offset = NA_real_)
  if (length(x) != 1L || is.na(x) || !nzchar(trimws(x))) return(out)

  match <- regmatches(x, regexec("^\\s*(.*?)\\s*\\(([^()]*)\\)\\s*$", x))[[1]]
  if (length(match) != 3L) return(out)
  clock <- trimws(match[2])
  label <- toupper(trimws(match[3]))
  out$timezone_label <- label
  if (!label %in% c("UTC", "GMT")) return(out)

  out$timezone_label <- "UTC"
  out$utc_offset <- 0
  parsed <- suppressWarnings(as.POSIXct(clock, format = "%d.%m.%Y %H:%M:%OS", tz = "UTC"))
  if (length(parsed) == 1L) out$datetime <- parsed
  out
}

#' Parse a CATS mini-diary `.txt` sidecar
#'
#' Reads the INI-style device/logging/calibration file written alongside CATS multisensor CSVs. Returns
#' NULL if the file is not a CATS sidecar (e.g. a Wildlife Computers `version.txt`/`sources.txt`). A
#' `[device]` section plus either `[logging]` or `[activated sensors]` is required. Channels are identified
#' by their `NN_name`
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

  # must look like a CATS sidecar rather than an unrelated text file
  if (!any(low == "[device]") ||
      !any(low %in% c("[logging]", "[activated sensors]"))) return(NULL)

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

  # --- sensor logging block ---
  log_rng <- section_range("[logging]")
  logging <- NULL
  if (length(log_rng)) {
    first <- .parseCATSLoggingTime(kv(log_rng, "first_entry"))
    last  <- .parseCATSLoggingTime(kv(log_rng, "last_entry"))
    labels <- unique(stats::na.omit(c(first$timezone_label, last$timezone_label)))
    raw_entries <- c(first$raw, last$raw)
    present <- !is.na(raw_entries) & nzchar(trimws(raw_entries))
    entry_offsets <- c(first$utc_offset, last$utc_offset)
    offsets <- unique(stats::na.omit(entry_offsets))
    logging_offset <- if (any(present) && all(is.finite(entry_offsets[present])) &&
                              length(offsets) == 1L) offsets else NA_real_
    logging <- list(
      first_entry = first$datetime,
      last_entry = last$datetime,
      first_entry_raw = first$raw,
      last_entry_raw = last$raw,
      timezone_label = if (length(labels) == 1L) labels else NA_character_,
      utc_offset = logging_offset
    )
  }

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
  list(source = path, source_type = "cats_diary_txt",
       device = device, logging = logging, sample_rate = NA_real_,
       calibration = if (length(cal)) cal else NULL)
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
       device = list(), logging = NULL, sample_rate = sr, calibration = NULL)
}

#' Read the sidecar paired with a sensor CSV
#'
#' Looks for a sibling `<basename>.txt` (CATS mini-diary metadata) and, failing
#' that, a `<basename>_resume.json` (CATS camera metadata). Returns NULL when no
#' valid sidecar is found.
#' @keywords internal
#' @noRd

.readSidecar <- function(csv_path) {
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

#' Report an ingested sidecar at detailed verbosity
#'
#' Values are retained for provenance but not printed or re-applied during import.
#' @keywords internal
#' @noRd

.reportSidecar <- function(sidecar, lvl = 1L) {
  if (lvl < 2L) return(invisible(NULL))
  if (is.null(sidecar)) { .log_detail(lvl, "no sidecar found"); return(invisible(NULL)) }
  # Confirm the source without reciting constants that the exporter has already applied. Clock findings
  # are reported separately because they are actionable rather than provenance noise.
  .log_detail(lvl, "sidecar: ", basename(sidecar$source))
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

#' Best-effort Wildlife Computers tag serial from the `.wch` archive filename in the WC directory
#' (e.g. "PIN_01_17A0269.wch" -> "17A0269", "19U0836.wch" -> "19U0836"). The serial is the physical-tag
#' identity (reused across redeployments), and unlike the Summary.csv model/PTT it is present on every
#' deployment in practice, so it anchors the ancillary-tag label. Returns NULL when no `.wch` is found.
#' @keywords internal
#' @noRd
.wcSerial <- function(wc_dir) {
  if (is.null(wc_dir) || is.na(wc_dir) || !dir.exists(wc_dir)) return(NULL)
  f <- list.files(wc_dir, pattern = "\\.wch$", full.names = FALSE, ignore.case = TRUE)
  if (!length(f)) return(NULL)
  b <- sub("\\.wch$", "", f[1], ignore.case = TRUE)
  parts <- strsplit(b, "_", fixed = TRUE)[[1]]     # PIN_01_17A0269 -> last token is the serial
  s <- trimws(parts[length(parts)])
  if (nzchar(s)) s else NULL
}

#' Format the position-fix type breakdown for the verbose ancillary line: a single type collapses to
#' "N <Type> fixes"; a mix reads "N fixes (a <T1>, b <T2>)". `types` is the position record's `type`
#' column (User rows are already excluded upstream in `.readPositionsAncillary`).
#' @keywords internal
#' @noRd
.fmtFixTypes <- function(types) {
  types <- types[!is.na(types)]
  n <- length(types)
  plur <- if (n != 1L) "es" else ""
  if (!n) return("0 fixes")
  tab <- sort(table(types), decreasing = TRUE)
  if (length(tab) == 1L) return(sprintf("%s %s fix%s", .formatLargeNumber(n), names(tab), plur))
  parts <- paste(sprintf("%d %s", as.integer(tab), names(tab)), collapse = ", ")
  sprintf("%s fix%s (%s)", .formatLargeNumber(n), plur, parts)
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

#' Read the Wildlife Computers tracking-fix record (`...Locations.csv`) into an ancillary entry.
#'
#' The full tracking-fix history at its own cadence - NOT snapped to sensor rows and NOT trimmed to the
#' recording window - so downstream analyses (tracks, maps, post-deployment drift) keep every fix. Carries
#' the useful WC columns (type, quality). "User"-type rows (deploy/pop-up coordinates, held in
#' meta$deployment) are excluded - they are not tracking fixes and inflate the record; the depth-drift
#' correction ignores them regardless.
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
  # Drop "User" rows: these are the deploy/pop-up coordinates (already held in meta$deployment), not
  # tracking fixes. The depth-drift correction ignores them (.gatherSurfaceEvidence uses FastGPS/Argos
  # only), and counting them inflates and mislabels the location record - a deploy-only tag would import
  # as a 2-fix "track". Genuine fix types (FastGPS, Argos, ...) are kept.
  out <- out[is.na(out$type) | out$type != "User", , drop = FALSE]
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
#'   reason_code, reason). `lag` is present (best estimate) even on abstain for diagnostics.
#' @keywords internal
#' @noRd
.estimateClockOffset <- function(ref_t, ref_d, wc_t, wc_d, control) {
  abstain <- function(reason_code, reason, ...) {
    c(list(status = "abstained", reason_code = reason_code, reason = reason), list(...))
  }
  okr <- is.finite(ref_t) & is.finite(ref_d); ref_t <- ref_t[okr]; ref_d <- ref_d[okr]
  okw <- is.finite(wc_t)  & is.finite(wc_d);  wc_t  <- wc_t[okw];  wc_d  <- wc_d[okw]
  if (length(ref_t) < 10L || length(wc_t) < 10L)
    return(abstain("no_shared_depth_samples", "no shared depth samples"))

  # overlap window shared by both records
  lo <- max(min(ref_t), min(wc_t)); hi <- min(max(ref_t), max(wc_t))
  overlap <- hi - lo
  if (!is.finite(overlap) || overlap < control$min.overlap * 60)
    return(abstain("insufficient_overlap",
                   sprintf("depth overlap %s < %g min", .fmt_duration(max(overlap, 0)), control$min.overlap),
                   overlap.sec = max(overlap, 0)))

  # decimate a very dense reference (e.g. 100 Hz over days) so the one-off interpolation stays cheap;
  # the target is only a ~1 Hz comparison grid, so a few hundred thousand reference points is ample
  if (length(ref_t) > 5e5L) { idx <- unique(round(seq(1, length(ref_t), length.out = 5e5L)))
                              ref_t <- ref_t[idx]; ref_d <- ref_d[idx] }
  g   <- seq(lo, hi, by = 2)                                   # 1 Hz is overkill; 0.5 Hz grid
  ref <- stats::approx(ref_t, ref_d, xout = g, rule = 2)$y
  if (!any(is.finite(ref)) || stats::sd(ref, na.rm = TRUE) < 0.5)
    return(abstain("flat_reference_depth", "reference depth too flat to align (no dives to lock onto)"))

  corAt <- function(L) {
    w <- stats::approx(wc_t + L, wc_d, xout = g, rule = 2)$y
    suppressWarnings(stats::cor(ref, w, use = "complete.obs"))
  }
  span   <- control$max.lag
  coarse <- seq(-span, span, by = 10)
  cc     <- vapply(coarse, corAt, numeric(1))
  if (!any(is.finite(cc))) return(abstain("degenerate_correlation", "degenerate cross-correlation"))
  bi <- which.max(cc)
  if (bi == 1L || bi == length(coarse))
    return(abstain("search_edge", "best lag at search edge (increase alignment max.lag)",
                   lag = coarse[bi], correlation = cc[bi], overlap.sec = overlap))
  c0   <- coarse[bi]
  fine <- seq(c0 - 9, c0 + 9, by = 1)
  ccf  <- vapply(fine, corAt, numeric(1))
  bestL <- fine[which.max(ccf)]; bestcor <- max(ccf, na.rm = TRUE)
  cor0  <- corAt(0)
  if (!is.finite(bestcor) || bestcor < control$min.correlation)
    return(abstain("low_correlation",
                   sprintf("peak correlation %.2f < %.2f", bestcor, control$min.correlation),
                   lag = bestL, correlation = bestcor, correlation.unaligned = cor0, overlap.sec = overlap))
  list(status = "aligned", lag = bestL, correlation = bestcor,
       correlation.unaligned = cor0, overlap.sec = overlap,
       reason_code = NA_character_, reason = NA_character_)
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
  none <- function(status, reason_code, reason) list(positions_anc = positions_anc, dry_anc = dry_anc,
    info = list(method = control$method, status = status, reason_code = reason_code, reason = reason,
                offset.seconds = 0, correlation = NA_real_))
  if (identical(control$method, "none"))
    return(none("disabled", "alignment_disabled", "alignment method = none"))
  if (is.null(positions_anc) && is.null(dry_anc))
    return(none("abstained", "no_wc_streams", "no Wildlife Computers streams to align"))
  if (is.null(sensor_data[["depth"]]) || !any(is.finite(sensor_data$depth)))
    return(none("abstained", "primary_depth_missing", "primary tag has no depth channel"))
  arch <- .readArchiveDepth(archive_file)
  if (is.null(arch))
    return(none("abstained", "no_archive_depth", "no shared depth channel in WC archive"))

  est <- .estimateClockOffset(as.numeric(sensor_data$datetime), sensor_data$depth,
                              arch$t, arch$depth, control)
  info <- list(method = control$method, status = est$status,
               reason_code = est$reason_code %||% NA_character_, reason = est$reason %||% NA_character_,
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
  # nested (level-2) beneath the "ancillary tag" line, as the operation applied to that device's streams;
  # an abstention is ALSO collected for the run summary independently, so the plain nested line is enough.
  if (identical(info$status, "aligned")) {
    .log_subdetail(lvl, "clock alignment: shifted streams by ",
                sprintf("%+d s", as.integer(round(info$offset.seconds))),
                " (cor = ", sprintf("%.2f", info$correlation), ")")
  } else {
    .log_subdetail(lvl, "clock alignment: not applied \u2014 ", info$reason)
  }
  invisible()
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
