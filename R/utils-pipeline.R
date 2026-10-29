#######################################################################################################
# Shared pipeline helpers #############################################################################
#######################################################################################################
#
# Small internal helpers that standardise behaviour across the user-facing workflow functions
# (input handling, console output, file saving), replacing copy-pasted blocks and enforcing the
# package "house style".


#' Conditionally print to the console (verbose gate)
#'
#' The single rule for console chatter: it is shown only when `verbose` is TRUE. Real notices use
#' `warning()`/`message()` and ignore `verbose`.
#' @keywords internal
#' @noRd
.say <- function(verbose, ...) if (isTRUE(verbose)) cat(...)


#' Standard section banner
#' @keywords internal
#' @noRd
.banner <- function(verbose, title, subtitle = NULL) {
  if (!isTRUE(verbose)) return(invisible(NULL))
  bar <- paste(rep("=", max(nchar(title) + 6L, 40L)), collapse = "")
  cat(cli::style_bold(paste0("\n", bar, "\n  ", title, "\n", bar, "\n")))
  if (!is.null(subtitle)) cat(subtitle, "\n")
  cat("\n")
  invisible(NULL)
}


#' Resolve a workflow function's `data` argument into a uniform iterable
#'
#' Accepts the package's canonical input forms and returns a uniform handle so every function can
#' iterate individuals identically:
#'   * a character vector of `.rds` file paths (loaded lazily, one at a time);
#'   * a single data.frame/data.table (split by `id.col`);
#'   * a named list of per-individual tables.
#' Each individual is returned as a `nautilus_tag` (metadata ensured / migrated from legacy attrs).
#'
#' @param data The user-supplied `data` argument.
#' @param id.col Name of the ID column (used when splitting a single table).
#' @return A list with `n`, `ids`, `is_filepaths`, `paths` (or NULL), and
#'   `get(i, writable = FALSE)`. The default accessor is read-only: a canonical in-memory tag may be
#'   borrowed. Request `writable = TRUE` before changing columns, class or attributes, or returning a
#'   dataset that the caller may edit. Each writable access is independent, including aliased list slots.
#'   Files are loaded lazily into owned tables and do not need another copy.
#' @keywords internal
#' @noRd

.resolveInput <- function(data, id.col = "ID") {

  # Empty input almost always signals a mistake - most often a mistyped directory passed through
  # list.files() (which returns character(0)). Fail loudly rather than silently resolving to zero
  # individuals (which would return an empty result and mask the error). Shared guard - see validate.R.
  .assert_nonempty(data, "data")

  # (i) character vector of file paths
  if (is.character(data)) {
    missing_files <- data[!file.exists(data)]
    if (length(missing_files) > 0) {
      .abort(c("Some {.arg data} files were not found:", "x" = "{.path {missing_files}}"))
    }
    ids <- tools::file_path_sans_ext(basename(data))
    return(list(n = length(data), ids = ids, is_filepaths = TRUE, paths = data,
                get = function(i, writable = FALSE) .ensureMeta(.workingTable(readRDS(data[i]), owned = TRUE))))
  }

  # (ii) single data.frame/data.table -> split by id.col
  if (is.data.frame(data)) {
    if (!id.col %in% names(data)) {
      .abort("Input {.arg data} must contain the {.val {id.col}} column when not provided as a list.")
    }
    # `split()` keeps a factor's UNUSED levels, so an id column that has been subset (the usual way a
    # cohort loses an animal) produced a zero-row group per dropped level: a phantom deployment that was
    # counted, reported and carried through every downstream summary. droplevels() keeps only ids the
    # data actually contains, matching what a character id column has always done.
    ids_col <- data[[id.col]]
    if (is.factor(ids_col)) ids_col <- droplevels(ids_col)
    data <- split(data, ids_col)
  } else if (!is.list(data)) {
    .abort("{.arg data} must be a data.frame/data.table, a list of them, or a character vector of file paths.")
  }

  # (iii) named list of per-individual tables
  ids <- names(data)
  if (is.null(ids)) ids <- as.character(seq_along(data))
  list(n = length(data), ids = ids, is_filepaths = FALSE, paths = NULL,
       get = function(i, writable = FALSE) {
         if (writable) .ensureMeta(.workingTable(data[[i]])) else .ensureMetaSafely(data[[i]])
       })
}

#' Acquire an owned table before editing a public input
#'
#' Copy borrowed tables, including nested mutable metadata, once per active deployment. `owned = TRUE`
#' is reserved for fresh file reads or private intermediates, never inferred from class or list names.
#' Low-level helpers may then use setDT/setattr/set/`:=` without reaching a caller's aliases. This helper
#' does not add metadata: older entry points decide separately when to migrate their legacy attributes.
#' @keywords internal
#' @noRd
.workingTable <- function(x, owned = FALSE) {
  if (!owned) x <- data.table::copy(x)
  if (!data.table::is.data.table(x)) data.table::setDT(x)
  x
}

#' `.ensureMeta()` for CALLER-SUPPLIED tables: same result, without modifying the caller's object.
#'
#' `.ensureMeta()` uses `setDT()`/`setattr()`, which act BY REFERENCE - fine for a table we just read
#' from disk or built ourselves, but not for one the user handed us: passing a list of data.frames used
#' to convert them to data.tables in the caller's own environment, so a later call on the same objects
#' could behave differently (or fail) for no visible reason.
#'
#' A copy is made only when a mutation would actually happen, so the common pipeline case - data.tables
#' already carrying metadata from `importTagData()` - still costs nothing. The result is read-only;
#' this helper protects migration, not subsequent edits. Use a writable resolver access for those.
#' @keywords internal
#' @noRd
.ensureMetaSafely <- function(x) {
  if (!data.table::is.data.table(x)) return(.ensureMeta(.workingTable(x)))
  if (is.null(attr(x, "nautilus", exact = TRUE))) return(.ensureMeta(data.table::copy(x)))
  x                                                                                      # nothing to do
}


#' Validate that required columns exist (clear, consistent error)
#' @keywords internal
#' @noRd
.validateColumns <- function(x, required, where = NULL) {
  missing_cols <- setdiff(required, names(x))
  if (length(missing_cols) > 0) {
    where_txt <- if (!is.null(where)) " in {.file {where}}" else ""
    .abort(paste0("Missing required column(s): {.val {missing_cols}}", where_txt, "."))
  }
  invisible(TRUE)
}


#' Persist a processed object to an `.rds` file (shared saving logic).
#'
#' The single output-persistence primitive for the package. Saving is triggered SOLELY by a non-NULL
#' `output.dir` (the "path is the save switch" convention shared with the plotters' `plot.file`): a NULL
#' directory is a no-op. Writes `<output.dir>/<id><output.suffix>.rds` and returns that path (or NULL when
#' nothing was written), so callers can accumulate the paths and hand them back when `return.data = FALSE`.
#'
#' @param obj Object to save.
#' @param id Individual ID (used for the file name).
#' @param output.dir Output directory, or NULL to write nothing.
#' @param output.suffix Optional file-name suffix (before `.rds`).
#' @param compress `saveRDS()` compression.
#' @param verbose Logical; print a confirmation line.
#' @return The written file path, or NULL when `output.dir` is NULL.
#' @keywords internal
#' @noRd

.saveOutput <- function(obj, id, output.dir = NULL, output.suffix = NULL, compress = TRUE, verbose = FALSE) {
  if (is.null(output.dir)) return(invisible(NULL))              # a NULL directory = do not persist
  suffix <- output.suffix %||% ""
  output_file <- file.path(output.dir, paste0(id, suffix, ".rds"))
  saveRDS(obj, output_file, compress = compress)
  if (isTRUE(verbose)) cat(sprintf("\u2713 Saved: %s\n", basename(output_file)))
  output_file
}


#' Guard the one illegal output request: keep nothing AND write nothing.
#'
#' The unified output contract has exactly two sinks - the in-memory return (`return.data`) and the disk
#' copy (a non-NULL `output.dir`). "Persist to nowhere" is unrepresentable (no `output.dir` = no write),
#' so the sole remaining mistake is `return.data = FALSE` with no `output.dir`: the results would be
#' computed and then discarded. This names that real dependency, replacing the old "at least one of two
#' booleans" guard.
#' @keywords internal
#' @noRd
.assert_output <- function(return.data, output.dir) {
  if (!isTRUE(return.data) && is.null(output.dir)) {
    .abort(c("{.code return.data = FALSE} needs an {.arg output.dir} to write to.",
             "i" = "Otherwise the results are computed and then discarded.",
             "i" = "Provide an {.arg output.dir}, or keep {.code return.data = TRUE}."))
  }
  invisible(TRUE)
}


#' Assemble a data function's return value under the unified output contract.
#'
#' `return.data = TRUE` -> the processed objects (a named list, keyed by id). `return.data = FALSE` -> the
#' written file paths as a character vector, returned **invisibly** so a top-level call does not auto-print
#' a wall of paths, while the value stays available to chain into the next step's `data` argument (a
#' memory-free pipeline) or to capture in scripts and tests. `saved` is the per-item vector of paths from
#' `.saveOutput` (NULLs for un-saved items are dropped).
#'
#' Invisibility propagates only if the caller ends with this call as its LAST expression; a caller that
#' assigns the result and returns the variable (e.g. to attach an attribute) must re-wrap - see
#' \code{applyAxisMapping}.
#' @keywords internal
#' @noRd
.collectOutput <- function(results, saved, return.data, ids) {
  if (isTRUE(return.data)) {
    # Drop the slots of deployments that produced nothing. Every batch function pre-sizes `results` to
    # the input count and `next`s past a deployment it cannot process, so without this the caller gets
    # NULL elements named after tags that were skipped - which breaks anything that maps over the
    # result, and (in the axis-mapping subsystem) made the whole object unrecognisable downstream.
    # This also makes the two branches agree: the `saved` branch below has always compacted, because
    # unlist() drops NULLs.
    keep <- !vapply(results, is.null, logical(1))
    return(stats::setNames(results, ids)[keep])
  }
  invisible(unlist(saved, use.names = FALSE))
}


#######################################################################################################
#######################################################################################################
#######################################################################################################


#' Explain WHY required columns are absent, distinguishing a QC exclusion from a malformed input.
#'
#' `checkSensorIntegrity()` and `importTagData()` both legitimately drop channels - the first when a
#' channel fails an integrity check, the second per the `exclude_sensors` metadata column - and record
#' what they dropped in `meta$sensors$excluded`. Until now nothing downstream consulted that record, so
#' a deliberately curated deployment was indistinguishable from a corrupt one: both produced a bare
#' "missing required column(s)". Reading the provenance turns an accusation into an explanation, and
#' tells the user whether to investigate the file or accept the exclusion.
#' @return A single human-readable clause naming the missing channels and, where known, their origin.
#' @keywords internal
#' @noRd
#' A display label for a deployment whose data may be unusable.
#'
#' Every deployment gets its own delimited console block, including the ones that are skipped - so a
#' label has to be resolvable BEFORE the data is known to be valid, and the usual
#' `unique(x$ID)[1]` is not available when the object is empty, NULL, or missing its ID column.
#' Falls back through: the ID column -> the file name (or list name) -> the slot index.
#'
#' @param x The deployment data (may be NULL or empty).
#' @param source A file path or list name for this slot (may be NA/NULL).
#' @param i The slot index, used as the last resort so a label is never empty.
#' @return A single character label.
#' @keywords internal
#' @noRd
.deploymentLabel <- function(x, source = NULL, i = NA_integer_) {
  id <- tryCatch({
    v <- if (!is.null(x) && NROW(x) && "ID" %in% names(x)) unique(x$ID) else NULL
    v <- v[!is.na(v)]
    if (length(v)) as.character(v[1]) else NULL
  }, error = function(e) NULL)
  if (!is.null(id) && nzchar(id)) return(id)
  if (!is.null(source) && length(source) == 1L && !is.na(source) && nzchar(source))
    return(tools::file_path_sans_ext(basename(source)))
  sprintf("slot %s", i)
}


.explainMissingColumns <- function(missing, meta = NULL) {
  excluded <- meta$sensors$excluded %||% character(0)
  by_qc <- intersect(missing, excluded)
  absent <- setdiff(missing, excluded)
  parts <- c(
    if (length(by_qc))
      sprintf("%s excluded by an earlier QC step", paste(.channelsToFamilies(by_qc), collapse = ", ")),
    if (length(absent))
      sprintf("%s not present", paste(absent, collapse = ", ")))
  paste(parts, collapse = "; ")
}


#' Which metrics of a heading are meaningful only in a GEOGRAPHIC frame?
#'
#' A magnetic heading differs from a geographic one by a constant offset (the magnetic declination:
#' about -7.6 degrees in the Azores, but exceeding 25 degrees of either sign elsewhere). That offset
#' cancels exactly in anything built from angle DIFFERENCES or from the length of a resultant vector -
#' turning rate, angular velocity, circular variance / sd / mrl, heading autocorrelation, u-turn and
#' circling detection - so those are valid on a magnetic heading and must NOT warn. It does not cancel
#' where a single absolute direction is reported: a circular mean or median rotates with it.
#'
#' The point of naming the DIRECTIONAL metrics rather than flagging the tag is noise: a deployment with
#' a magnetic heading is perfectly usable for the rotation-invariant majority, and a warning that fires
#' regardless of what was asked for is one users learn to ignore.
#' @keywords internal
#' @noRd
.directionalHeadingMetrics <- function() c("mean", "median")

#' Warn once when an ABSOLUTE heading statistic was computed from a magnetic-referenced heading.
#'
#' Silent unless the frame is recorded as "magnetic" AND a directional metric was actually requested.
#' "unknown" (a tag processed before the frame was recorded) is left alone rather than guessed at.
#' @keywords internal
#' @noRd
.warnMagneticHeading <- function(ids, metrics, what) {
  ids <- unique(ids[!is.na(ids)])
  hit <- intersect(metrics, .directionalHeadingMetrics())
  if (!length(ids) || !length(hit)) return(invisible(NULL))
  cli::cli_warn(c(
    "{what} computed from a MAGNETIC heading for {length(ids)} deployment{?s}: {.val {utils::head(ids, 8)}}.",
    "!" = "{.val {hit}} report an absolute direction, so they are rotated by the uncorrected magnetic declination.",
    "i" = "Rotation-invariant measures (turning rate, circular variance, heading change) are unaffected - a constant offset cancels.",
    "i" = "Set the deployment position and re-run {.fn processTagData}; see {.code meta$deployment$heading_reference}."))
  invisible(NULL)
}

# Additional shared pipeline helpers



#####################################################################################
## Canonical sensor channels ########################################################
#####################################################################################

#' Canonical recognized sensor channel names
#'
#' @description Single source of truth for the sensor channels nautilus recognizes,
#' in canonical column order. Used to order columns and to select which channels to
#' check/interpolate, so partial sensor sets (e.g. accelerometer-only or TDR
#' depth/temperature data) are handled consistently across functions.
#' @return Character vector of channel names.
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.sensorChannels <- function() {
  c("ax", "ay", "az", "gx", "gy", "gz", "mx", "my", "mz",
    "depth", "temp", "paddle_speed", "paddle_freq")
}


#' Transition (run-length) encode a state series, keeping only the rows where the state changes.
#'
#' Compact, lossless storage for a slowly-changing state (e.g. a wet/dry signal) carried in the tag
#' metadata `ancillary` tier: each kept row's state holds from that timestamp until the next row.
#'
#' @param datetime POSIXct timestamps (assumed ordered).
#' @param state Logical (or 0/1) state at each timestamp; NA is treated as FALSE.
#' @return A data.frame with `datetime` and `state` (one row per run).
#' @keywords internal
#' @noRd
.transitionEncode <- function(datetime, state) {
  state <- as.logical(state); state[is.na(state)] <- FALSE
  n <- length(state)
  if (n == 0L) return(data.frame(datetime = datetime[0], state = logical(0)))
  keep <- c(TRUE, state[-1] != state[-n])
  data.frame(datetime = datetime[keep], state = state[keep])
}


#' Expand sensor family / channel tokens to canonical channel names.
#'
#' Accepts a comma-separated string of family names ("accel"/"gyro"/"mag") and/or individual channels
#' ("mx", "gz", "depth", ...) - e.g. the value of an `exclude_sensors` metadata column - and returns the
#' de-duplicated set of canonical channels. Aborts on an unrecognised token. `NA`/empty -> character(0).
#' @keywords internal
#' @noRd
.expandSensorTokens <- function(x, arg = "sensors") {
  if (is.null(x) || (length(x) == 1L && is.na(x)) || !nzchar(trimws(as.character(x)))) return(character(0))
  families <- list(accel = c("ax", "ay", "az"), gyro = c("gx", "gy", "gz"), mag = c("mx", "my", "mz"))
  channels <- .sensorChannels()
  toks <- trimws(strsplit(as.character(x), ",", fixed = TRUE)[[1]])
  toks <- toks[nzchar(toks)]
  out <- character(0)
  for (tk in toks) {
    if (tk %in% names(families)) out <- c(out, families[[tk]])
    else if (tk %in% channels)  out <- c(out, tk)
    else .abort(c("{.arg {arg}} has an unrecognised sensor token {.val {tk}}.",
                  "i" = "Use a family ({.val {names(families)}}) or a channel ({.val {channels}})."))
  }
  unique(out)
}

#' Collapse a set of channels to family names where a family is complete (mx,my,mz -> "mag").
#' @keywords internal
#' @noRd
.channelsToFamilies <- function(chans) {
  families <- list(accel = c("ax", "ay", "az"), gyro = c("gx", "gy", "gz"), mag = c("mx", "my", "mz"))
  out <- character(0); remaining <- unique(chans)
  for (fam in names(families)) {
    if (all(families[[fam]] %in% remaining)) { out <- c(out, fam); remaining <- setdiff(remaining, families[[fam]]) }
  }
  c(out, remaining)
}


# ---------------------------------------------------------------------------------------------------
# group.by: one consistent grouping-key API shared by every function that pools by metadata keys
# (consensusAxisMapping, calibrateMagnetometer). A single interface: `group.by` is one or more of the
# canonical tag keys, combined into a composite key.
# ---------------------------------------------------------------------------------------------------

#' Validate a `group.by` spec (one or more metadata field names) against the allowed keys.
#'
#' Coerces to character, aborts on empty input or unknown field names, and returns the vector. Shared by
#' every `group.by` argument so they validate identically.
#' @keywords internal
#' @noRd
.validateGroupBy <- function(group.by, allowed = c("package_id", "logger_id", "tag", "type"),
                             arg = "group.by") {
  group.by <- as.character(group.by)
  bad <- setdiff(group.by, allowed)
  if (!length(group.by) || length(bad))
    .abort(c("{.arg {arg}} must be one or more of {.val {allowed}}.",
             if (length(bad)) c("x" = "unknown field{?s}: {.val {bad}}") else NULL))
  group.by
}

#' The composite grouping key for one item, from a field lookup `get(field)`.
#'
#' Any missing / blank / non-scalar value in ANY requested field yields `NA_character_`, so the item
#' becomes its own standalone group (never silently pooled with others). Present fields are joined with
#' `" | "`. Shared so a `group.by` composite key is assembled identically wherever it is used.
#' @keywords internal
#' @noRd
.compositeGroupKey <- function(group.by, get) {
  vals <- vapply(group.by, function(f) {
    v <- get(f)
    if (is.null(v) || length(v) != 1L || is.na(v) || !nzchar(as.character(v))) NA_character_ else as.character(v)
  }, character(1))
  if (anyNA(vals)) NA_character_ else paste(vals, collapse = " | ")
}
