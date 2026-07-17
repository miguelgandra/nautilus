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
#' @param timezone Time zone `start` is expressed in / the record is placed in.
#' @param sampling.rate Optional override (Hz); by default it is read from the file header.
#' @param exclude.channels Channels to drop (resolved from metadata by the caller).
#' @param verbose Unused; present for reader-contract symmetry with read_cats().
#' @return The reader contract: `data` (canonical frame, or NULL), `reason`, `assembly`, `mapping`,
#'   `selected_cols`, `calibration_info`, `temp_status`, `excluded`, `tz_mismatch`, `tz_note`,
#'   `unit_notes`.
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
         calibration_info = NULL, temp_status = "none", excluded = character(0),
         tz_mismatch = FALSE, tz_note = NULL, unit_notes = character(0))
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
  if (is.null(start) || !inherits(start, "POSIXct") || length(start) != 1L || is.na(start)) {
    return(fail("no recording start time: Little Leonardo files carry no clock, so a `data_start` is required"))
  }

  # ---- acceleration (X/Y/Z, in g), skipping the header block ---------------------------------------
  skip_a <- .llSkip(head_lines, "^\\s*X\\s*,")
  if (is.na(skip_a)) return(fail(sprintf("unrecognised header in %s: no 'X , Y , Z' column row", basename(files$accel))))
  acc <- tryCatch(data.table::fread(files$accel, skip = skip_a, header = FALSE, fill = TRUE,
                                    showProgress = FALSE),
                  error = function(e) NULL)
  if (is.null(acc) || !nrow(acc) || ncol(acc) < 3L) return(fail(sprintf("could not parse %s", basename(files$accel))))
  acc <- acc[, 1:3]                                   # a trailing comma yields a 4th, empty column
  data.table::setnames(acc, c("ax", "ay", "az"))

  n <- nrow(acc)
  dt <- acc
  # timestamps synthesised from the supplied start + the declared rate, then interpreted in `timezone`
  dt[, datetime := start + (seq_len(.N) - 1L) / hz]
  dt[, datetime := lubridate::force_tz(datetime, timezone)]

  # ---- depth / temperature (lower rate, separate file) ---------------------------------------------
  # expanded onto the acceleration grid by whole-second index, which is how the two streams relate: the
  # DT file carries one row per second of the same recording.
  n_dt <- 0L
  if (!is.na(files$depth)) {
    dhead <- tryCatch(readLines(files$depth, n = 10L, warn = FALSE), error = function(e) character(0))
    skip_d <- .llSkip(dhead, "^\\s*Depth\\s*,")
    if (!is.na(skip_d)) {
      dtemp <- tryCatch(data.table::fread(files$depth, skip = skip_d, header = FALSE, fill = TRUE,
                                          showProgress = FALSE),
                        error = function(e) NULL)
      if (!is.null(dtemp) && nrow(dtemp) && ncol(dtemp) >= 2L) {
        n_dt <- nrow(dtemp)
        keep <- min(ncol(dtemp), 3L)
        dtemp <- dtemp[, seq_len(keep), with = FALSE]
        data.table::setnames(dtemp, c("depth", "temp", "video")[seq_len(keep)])
        idx <- ((seq_len(n) - 1L) %/% as.integer(round(hz))) + 1L
        idx[idx > n_dt] <- n_dt                       # a short DT file holds the last value
        dt[, depth := dtemp$depth[idx]]
        if ("temp" %in% names(dtemp)) dt[, temp := dtemp$temp[idx]]
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

  list(data = dt, reason = NULL, assembly = assembly, mapping = NULL,
       selected_cols = c("X", "Y", "Z", if (!is.na(files$depth)) c("Depth", "Temp")),
       calibration_info = NULL, temp_status = "none", excluded = excluded_channels,
       tz_mismatch = FALSE, tz_note = NULL,
       unit_notes = sprintf("timestamps synthesised from data_start + %g Hz (header)", hz))
}
