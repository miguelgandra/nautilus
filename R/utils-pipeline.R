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
#' @return A list with `n`, `ids`, `is_filepaths`, `paths` (or NULL), and `get(i)` accessor.
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
                get = function(i) .ensureMeta(readRDS(data[i]))))
  }

  # (ii) single data.frame/data.table -> split by id.col
  if (is.data.frame(data)) {
    if (!id.col %in% names(data)) {
      .abort("Input {.arg data} must contain the {.val {id.col}} column when not provided as a list.")
    }
    data <- split(data, data[[id.col]])
  } else if (!is.list(data)) {
    .abort("{.arg data} must be a data.frame/data.table, a list of them, or a character vector of file paths.")
  }

  # (iii) named list of per-individual tables
  ids <- names(data)
  if (is.null(ids)) ids <- as.character(seq_along(data))
  list(n = length(data), ids = ids, is_filepaths = FALSE, paths = NULL,
       get = function(i) .ensureMeta(data[[i]]))
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
  if (isTRUE(return.data)) return(stats::setNames(results, ids))
  invisible(unlist(saved, use.names = FALSE))
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
