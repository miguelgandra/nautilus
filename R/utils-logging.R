#######################################################################################################
# Internal structured-logging helpers (cli backend) ###################################################
#######################################################################################################
#
# A single logging vocabulary shared by the workflow functions, built on cli (consistent symbols,
# alignment, colour, and automatic ASCII fallback on terminals that cannot render Unicode). Output is
# gated by a verbosity LEVEL so the same calls serve quiet / normal / detailed runs:
#
#   level 0  "quiet"     warnings + errors only
#   level 1  "normal"    section header, one line per individual, final summary   (default)
#   level 2  "detailed"  adds low-level diagnostics (assembly decisions, per-file saves, ...)
#
# warning()/message() for genuine notices always fire, independent of the level.


#' Normalise a `verbose` argument to an integer level (0/1/2).
#'
#' Accepts a logical (FALSE -> 0, TRUE -> 1), an integer 0:2, or a string
#' ("quiet"/"normal"/"detailed").
#' @keywords internal
#' @noRd
.verbosity <- function(verbose) {
  if (is.null(verbose)) return(1L)
  if (is.logical(verbose) && length(verbose) == 1L && !is.na(verbose)) return(if (verbose) 1L else 0L)
  if (is.numeric(verbose) && length(verbose) == 1L && !is.na(verbose)) return(as.integer(max(0L, min(2L, round(verbose)))))
  if (is.character(verbose) && length(verbose) == 1L) {
    lev <- c(quiet = 0L, normal = 1L, detailed = 2L)[match.arg(verbose, names(c(quiet = 0L, normal = 1L, detailed = 2L)))]
    return(unname(lev))
  }
  .abort("{.arg verbose} must be a logical, an integer 0-2, or one of {.val {c('quiet','normal','detailed')}}.")
}

#' Section header rule: "-- fn ----------------------------------- nautilus"
#' @keywords internal
#' @noRd
.log_h1 <- function(lvl, title) {
  if (lvl >= 1L) cli::cli_rule(left = "{.strong {title}}", right = "nautilus")
  invisible(NULL)
}

#' A lighter sub-section rule (e.g. one per individual). The caller decides the gating level
#' (per-individual sub-headers are typically a level-2 detail).
#' @keywords internal
#' @noRd
.log_h2 <- function(lvl, title, min_level = 2L) {
  if (lvl >= min_level) cli::cli_rule(left = "{.strong {title}}")
  invisible(NULL)
}

#' A blank spacer line (level-gated), used to separate per-individual blocks.
#' @keywords internal
#' @noRd
.log_gap <- function(lvl, min_level = 2L) {
  if (lvl >= min_level) cli::cli_text("")
  invisible(NULL)
}

#' A full-width heavy divider that frames a header/summary block. Uses a double line on UTF-8 consoles
#' and a plain "=" elsewhere - cli byte-escapes a raw glyph on non-UTF or non-interactive output, so we
#' pick the character the same way cli's own symbols choose their ASCII fallbacks.
#' @keywords internal
#' @noRd
.log_frame <- function(lvl, min_level = 1L) {
  if (lvl < min_level) return(invisible(NULL))
  bar <- strrep(if (cli::is_utf8_output()) "\u2550" else "=", cli::console_width())
  cli::cli_text("{bar}")
  invisible(NULL)
}

#' The runtime line of a summary block: a stopwatch glyph on UTF-8 (cli has no stopwatch symbol),
#' falling back to the standard bullet elsewhere, followed by the elapsed wall-clock time.
#' @keywords internal
#' @noRd
.log_runtime <- function(lvl, start, min_level = 1L) {
  if (lvl < min_level) return(invisible(NULL))
  sw <- if (cli::is_utf8_output()) "\u23f1" else cli::symbol$bullet
  rt <- .fmt_elapsed(start)
  cli::cli_text("{sw} runtime: {rt}")
  invisible(NULL)
}

#' The framed function header shared by every workflow function. A full-width frame, the
#' `-- fn ---- nautilus` rule, an intro line with a touch of personality, then the run's
#' Input / Output configuration and a closing Mode/Method pointer, all inside a second frame.
#' Centralised here so the layout and block spacing stay identical across the package.
#'
#' `intro` is the personality line (e.g. "Welcome aboard: importing biologging data").
#' `bullets` are pre-formatted `* ` configuration lines (e.g. "Input: 5 tags", "Output: <dir>").
#' `arrow` is an optional single `-> ` pointer (e.g. "Mode: batch run", "Method: peak detection").
#' Strings are inserted as values (injection-safe), so paths with odd characters are fine.
#' @keywords internal
#' @noRd
.log_header <- function(lvl, title, intro, bullets = NULL, arrow = NULL) {
  if (lvl < 1L) return(invisible(NULL))
  .log_frame(lvl)
  .log_h1(lvl, title)
  cli::cli_text("")                                       # blank: rule -> intro
  cli::cli_alert_info("{intro}")
  for (b in bullets) cli::cli_text("{cli::symbol$bullet} {b}")
  for (a in arrow) cli::cli_text("{cli::symbol$arrow_right} {a}")
  cli::cli_text("")                                       # blank: body -> bottom frame
  .log_frame(lvl)
  cli::cli_text("")                                       # blank: header -> first block
  invisible(NULL)
}

#' Open the final SUMMARY block: a blank-line separator and the `-- SUMMARY --` rule (shown from
#' level 1). The caller follows it with the run tally (`.log_done`), any output/plot pointers
#' (`.log_arrow`) and the runtime footer (`.log_runtime`), in that order, for a uniform close.
#' @keywords internal
#' @noRd
.log_summary <- function(lvl) {
  if (lvl < 1L) return(invisible(NULL))
  cli::cli_text("")
  .log_h2(lvl, "SUMMARY", min_level = 1L)
  invisible(NULL)
}

#' An informational line (level >= 1), e.g. counts / source.
#' @keywords internal
#' @noRd
.log_info <- function(lvl, ...) {
  if (lvl >= 1L) cli::cli_alert_info("{paste0(...)}")
  invisible(NULL)
}

#' A per-individual success line (level >= 1).
#' @keywords internal
#' @noRd
.log_ok <- function(lvl, ...) {
  if (lvl >= 1L) cli::cli_alert_success("{paste0(...)}")
  invisible(NULL)
}

#' Optional live progress bar for a per-item loop, shown only at the verbosity level(s) where the loop
#' would otherwise be SILENT.
#'
#' Returns a bar id to feed `.log_progress_step()` / `.log_progress_done()`, or `NULL` when it should not
#' render (out of the level window, or nothing to iterate). The default window `[2, Inf]` suits the lighter
#' reporting/plotting functions, which are silent between header and summary at every level, so the bar
#' becomes "detailed" mode's live feedback. The heavy per-deployment functions (reconstructTrack /
#' crossValidateTrack) instead STREAM per-deployment blocks at detailed, so they pass `min.level = max.level
#' = 1L` to show the bar only at NORMAL (their one silent level). cli auto-suppresses the bar for fast loops
#' (below `cli.progress_show_after`, ~2 s), so quick runs stay clutter-free; it is tied to the CALLER's frame
#' and cleaned up automatically if the function exits early.
#' @keywords internal
#' @noRd
.log_progress_start <- function(lvl, total, name = "Processing", min.level = 2L, max.level = Inf,
                                .envir = parent.frame()) {
  if (lvl < min.level || lvl > max.level || !is.finite(total) || total < 1L) return(NULL)
  cli::cli_progress_bar(name, total = as.integer(total), .envir = .envir)
}
#' @keywords internal
#' @noRd
.log_progress_step <- function(id) if (!is.null(id)) cli::cli_progress_update(id = id)
#' @keywords internal
#' @noRd
.log_progress_done <- function(id) if (!is.null(id)) cli::cli_progress_done(id = id)

#' A per-individual "skipped / attention" line (level >= 1). Not a warning() - the run continues.
#' @keywords internal
#' @noRd
.log_skip <- function(lvl, ...) {
  if (lvl >= 1L) cli::cli_alert_warning("{paste0(...)}")
  invisible(NULL)
}

#' A low-level diagnostic line, shown only at the detailed level (>= 2).
#' @keywords internal
#' @noRd
.log_detail <- function(lvl, ...) {
  if (lvl >= 2L) cli::cli_alert("{paste0(...)}")   # bullet; wrap so literal { } in the text are NOT glue
  invisible(NULL)
}

#' The final summary line (level >= 1), typically counts + elapsed time.
#' @keywords internal
#' @noRd
.log_done <- function(lvl, ...) {
  if (lvl >= 1L) cli::cli_alert_success("{.strong {paste0(...)}}")
  invisible(NULL)
}

#' A "saved/output" pointer line (level >= 1).
#' @keywords internal
#' @noRd
.log_arrow <- function(lvl, ...) {
  if (lvl >= 1L) cli::cli_text("{cli::symbol$arrow_right} {paste0(...)}")
  invisible(NULL)
}

#' Format an elapsed difftime compactly (e.g. "3m 12s", "0.4s").
#' @keywords internal
#' @noRd
.fmt_elapsed <- function(start) {
  secs <- as.numeric(difftime(Sys.time(), start, units = "secs"))
  if (secs < 60) return(sprintf("%.1fs", secs))
  m <- floor(secs / 60); s <- round(secs - 60 * m)
  if (m < 60) return(sprintf("%dm %02ds", m, s))
  h <- floor(m / 60); m <- m - 60 * h
  sprintf("%dh %02dm", h, m)
}

#' Format a duration in seconds at a human scale (e.g. "2.3 d", "5.1 h", "45 m").
#' @keywords internal
#' @noRd
.fmt_duration <- function(secs) {
  if (!is.finite(secs)) return("?")
  if (secs >= 86400) return(sprintf("%.1f d", secs / 86400))
  if (secs >= 3600)  return(sprintf("%.1f h", secs / 3600))
  if (secs >= 60)    return(sprintf("%.0f m", secs / 60))
  sprintf("%.0f s", secs)
}
