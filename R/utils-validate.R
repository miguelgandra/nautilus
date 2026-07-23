#######################################################################################################
# Internal argument-validation helpers ################################################################
#######################################################################################################
#
# A single, consistent set of assertions used at the top of every exported function. Centralising them
# keeps the error grammar uniform ("`arg` must be ...", names the offending value, no call trace) and
# removes the ad-hoc per-call-site checks that had drifted out of sync. All abort via .abort() (a thin
# cli wrapper) for consistent formatting. Directory handling is FAIL-FAST: a missing directory is an
# error, never silently created (see .assert_dir / .assert_writable_file).


#' Throw a concise, cli-formatted error.
#'
#' The single error entry point for the package. Wraps `cli::cli_abort()` with `call = NULL` so the
#' user sees a plain "Error: <message>" (no "Error in `fn()`:" prefix and, together with the
#' `rlang_backtrace_on_error` option set in .onLoad, no "Run `rlang::last_trace()`" footer). `message`
#' follows the cli convention: a character vector whose element names control the bullet
#' ("" = leading line, "x" = cross, "i" = info/hint). Inline markup such as `{.arg {arg}}`,
#' `{.path {p}}` and `{.val {x}}` is interpolated in the calling frame.
#' @keywords internal
#' @noRd
.abort <- function(message, ..., .envir = parent.frame()) {
  cli::cli_abort(message, ..., call = NULL, .envir = .envir)
}


#' Raise one grouped warning for a fault that recurs across deployments.
#'
#' Warning per deployment floods the console on a batch run: the same multi-line caveat is repeated once
#' per tag, and R's warning buffer then silently truncates the tail (only the first 50 are kept). This
#' raises the caveat ONCE - headline, the shared hints, then one bullet per affected deployment carrying
#' only the part that actually differs (the ID and its magnitude) - so the diagnosis and the recommendation
#' are read once and the per-deployment evidence stays intact however large the batch.
#'
#' `headline` and `hints` are cli format strings, interpolated in `.envir` like any other cli message.
#' `items` are data-derived and are inserted VERBATIM: braces are escaped so an ID containing `{` cannot be
#' mistaken for a glue expression. Nothing is emitted when `items` is empty, so callers can pass a filtered
#' vector without guarding first.
#' @keywords internal
#' @noRd
.warn_grouped <- function(headline, items, hints = NULL, items.header = "Affected deployments:",
                          .envir = parent.frame()) {
  if (!length(items)) return(invisible(NULL))
  items <- gsub("}", "}}", gsub("{", "{{", items, fixed = TRUE), fixed = TRUE)
  cli::cli_warn(c(headline,
                  if (length(hints)) stats::setNames(hints, rep("i", length(hints))),
                  stats::setNames(c(items.header, items), c("", rep("*", length(items))))),
                .envir = .envir)
  invisible(NULL)
}


#' Validate a single logical flag.
#' @keywords internal
#' @noRd
.assert_flag <- function(x, arg) {
  if (!is.logical(x) || length(x) != 1L || is.na(x)) {
    .abort("{.arg {arg}} must be a single {.code TRUE} or {.code FALSE} value, not {.obj_type_friendly {x}}.")
  }
  invisible(x)
}

#' Validate a single character string (optionally allowing NULL).
#' @keywords internal
#' @noRd
.assert_string <- function(x, arg, null_ok = FALSE) {
  if (is.null(x)) {
    if (null_ok) return(invisible(x))
    .abort("{.arg {arg}} must be a single string, not {.code NULL}.")
  }
  if (!is.character(x) || length(x) != 1L || is.na(x)) {
    .abort("{.arg {arg}} must be a single, non-missing string.")
  }
  invisible(x)
}

#' Validate a single finite number within an (inclusive) range.
#' @keywords internal
#' @noRd
.assert_number <- function(x, arg, min = -Inf, max = Inf, null_ok = FALSE) {
  if (is.null(x)) {
    if (null_ok) return(invisible(x))
    .abort("{.arg {arg}} must be a single number, not {.code NULL}.")
  }
  if (!is.numeric(x) || length(x) != 1L || !is.finite(x)) {
    .abort("{.arg {arg}} must be a single, finite number.")
  }
  if (x < min || x > max) {
    .abort("{.arg {arg}} must be between {min} and {max}, not {x}.")
  }
  invisible(x)
}

#' Validate a single positive whole number (count).
#' @keywords internal
#' @noRd
.assert_count <- function(x, arg, min = 1L, null_ok = FALSE) {
  if (is.null(x) && null_ok) return(invisible(x))
  if (!is.numeric(x) || length(x) != 1L || !is.finite(x) || x != round(x) || x < min) {
    .abort("{.arg {arg}} must be a single whole number >= {min}.")
  }
  invisible(x)
}

#' Validate a value against a set of allowed choices (like match.arg, with a clear message).
#' @keywords internal
#' @noRd
.assert_choice <- function(x, arg, choices) {
  if (!is.character(x) || length(x) != 1L || is.na(x) || !x %in% choices) {
    .abort(c("{.arg {arg}} must be one of {.or {.val {choices}}}.",
                     "x" = "You supplied {.val {x}}."))
  }
  invisible(x)
}

#' Validate that a data.frame contains the required columns.
#' @keywords internal
#' @noRd
.assert_columns <- function(df, cols, arg) {
  if (!is.data.frame(df)) .abort("{.arg {arg}} must be a data.frame or data.table.")
  missing <- setdiff(cols, names(df))
  if (length(missing)) {
    .abort(c("{.arg {arg}} is missing required column{?s} {.val {missing}}.",
                     "i" = "Columns present: {.val {names(df)}}."))
  }
  invisible(df)
}

#' Check that the optional packages needed for parallel computing are available and that the
#' requested core count is sensible. A no-op when `n.cores <= 1`.
#' @keywords internal
#' @noRd
.assert_parallel <- function(n.cores, arg = "n.cores") {
  if (is.null(n.cores) || n.cores <= 1) return(invisible(n.cores))
  for (pkg in c("foreach", "doSNOW", "parallel")) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      .abort(c("The {.pkg {pkg}} package is required for parallel computing but is not installed.",
               "i" = "Install it with {.code install.packages({.str {pkg}})}, or set {.arg {arg}} to 1."))
    }
  }
  available <- parallel::detectCores()
  if (is.finite(available) && n.cores > available) {
    .abort("{.arg {arg}} ({n.cores}) exceeds the number of available cores ({available}).")
  }
  invisible(n.cores)
}

#' Validate a saveRDS() compression argument: a single TRUE/FALSE, or one of "gzip"/"bzip2"/"xz".
#' @keywords internal
#' @noRd
.assert_compress <- function(x, arg = "compress") {
  ok <- (is.logical(x) && length(x) == 1L && !is.na(x)) ||
        (is.character(x) && length(x) == 1L && x %in% c("gzip", "bzip2", "xz"))
  if (!ok) .abort("{.arg {arg}} must be {.code TRUE}/{.code FALSE} or one of {.or {.val {c('gzip','bzip2','xz')}}}.")
  invisible(x)
}

#' Fail-fast directory check: the directory must already exist.
#'
#' Nautilus never silently creates output directories - a missing path is far more often a typo than
#' an intent, and silent creation harms reproducibility. Callers must create directories explicitly.
#' @keywords internal
#' @noRd
.assert_dir <- function(path, arg, null_ok = TRUE) {
  if (is.null(path)) {
    if (null_ok) return(invisible(path))
    .abort("{.arg {arg}} must be a directory path, not {.code NULL}.")
  }
  .assert_string(path, arg)
  if (!dir.exists(path)) {
    .abort(c("{.arg {arg}} points to a directory that does not exist: {.path {path}}.",
                     "i" = "Create it first, e.g. {.code dir.create({.str {path}}, recursive = TRUE)}."))
  }
  invisible(path)
}

#' Fail-fast output-file check: the parent directory must exist, and (optionally) the extension match.
#' @keywords internal
#' @noRd
.assert_writable_file <- function(path, arg, ext = NULL, null_ok = TRUE) {
  if (is.null(path)) {
    if (null_ok) return(invisible(path))
    .abort("{.arg {arg}} must be a file path, not {.code NULL}.")
  }
  .assert_string(path, arg)
  parent <- dirname(path)
  if (!dir.exists(parent)) {
    .abort(c("{.arg {arg}} is in a directory that does not exist: {.path {parent}}.",
                     "i" = "Create it first, e.g. {.code dir.create({.str {parent}}, recursive = TRUE)}."))
  }
  if (!is.null(ext)) {
    actual <- tolower(tools::file_ext(path))
    if (!actual %in% tolower(ext)) {
      .abort("{.arg {arg}} must end in {.or {.val {ext}}}, not {.val {actual}}.")
    }
  }
  invisible(path)
}

#' Fail loudly when an input resolved to nothing (empty vector/list, or a 0-row data.frame)
#'
#' The package standard for file/data inputs: a mistyped directory passed through `list.files()`
#' silently yields `character(0)`, which - left unchecked - makes a workflow function no-op and return
#' an empty result, masking the typo. This is the shared guard for functions that resolve their file
#' paths themselves (the ones routed through `.resolveInput()` are guarded there, which calls this).
#' @keywords internal
#' @noRd
.assert_nonempty <- function(x, arg = "data", what = "input datasets or files") {
  n <- if (is.data.frame(x)) nrow(x) else length(x)
  if (n == 0L) {
    .abort(c("{.arg {arg}} is empty - found no {what} to process.",
             "i" = "A mistyped directory passed through {.code list.files()} silently yields {.code character(0)}; check the path.",
             "i" = "{.arg {arg}} must be one or more items (a list/data.frame) or existing file path(s)."))
  }
  invisible(TRUE)
}
