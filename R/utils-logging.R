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
.log_header <- function(lvl, title, intro, bullets = NULL, arrow = NULL, sub = NULL, close = TRUE) {
  if (lvl < 1L) return(invisible(NULL))
  .log_frame(lvl)
  .log_h1(lvl, title)
  cli::cli_text("")                                       # blank: rule -> intro
  cli::cli_alert_info("{intro}")
  for (b in bullets) cli::cli_text("{cli::symbol$bullet} {b}")
  # `sub`: indented arrows hanging off the LAST bullet (e.g. a list of thresholds under "Criteria:").
  # Rendered like .log_subdetail - a blank-name cli bullet indents without adding its own marker - and
  # the text is built first so braces in a value stay literal.
  for (s in sub) { txt <- paste0(cli::symbol$arrow_right, " ", s); cli::cli_bullets(stats::setNames("{txt}", " ")) }
  for (a in arrow) cli::cli_text("{cli::symbol$arrow_right} {a}")
  # `close = FALSE` leaves the frame open so the caller can add a section that is only knowable after
  # it has looked at the data - a settings block whose values are derived from the cohort, say. Any
  # progress bar drawn in between erases itself, so the rendered header still reads as one unit.
  if (close) .log_header_close(lvl)
  invisible(NULL)
}

#' Close a header opened with `close = FALSE`.
#' @param lvl Resolved verbosity.
#' @keywords internal
#' @noRd
.log_header_close <- function(lvl) {
  if (lvl < 1L) return(invisible(NULL))
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

#' A cohort-level "attention" line (level >= 1): a `!` rollup of QC concerns worth a second look. Same
#' glyph as `.log_skip` but named for intent - it flags the batch, not one individual.
#' @keywords internal
#' @noRd
.log_attention <- function(lvl, ...) {
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

#' An indented sub-line beneath a `.log_detail()` line (level >= 2 by default): supporting evidence
#' subordinate to the finding above it, marked with a corner arrow so it reads as detail, not a peer.
#' `min_level` lowers the gate for a block that itself shows at "normal" verbosity - the SUMMARY, whose
#' heading would otherwise be left dangling above suppressed sub-lines.
#' @keywords internal
#' @noRd
.log_subdetail <- function(lvl, ..., min_level = 2L) {
  if (lvl < min_level) return(invisible(NULL))
  txt <- paste0("\u21b3 ", paste0(...))
  cli::cli_bullets(stats::setNames("{txt}", " "))   # blank-name bullet = indent, no marker; {txt} keeps literal braces literal
  invisible(NULL)
}

#' An indented action/detail row in a final issues block.
#'
#' Unlike `.log_subdetail()`, this uses the ordinary right arrow: issue rows are alternatives within a
#' category, not diagnostic evidence nested below an individual sensor line. The blank-name cli bullet
#' supplies the indentation and preserves a hanging indent when a long list of deployment IDs wraps.
#' @keywords internal
#' @noRd
.log_issue_detail <- function(lvl, ..., min_level = 1L) {
  if (lvl < min_level) return(invisible(NULL))
  txt <- paste0(cli::symbol$arrow_right, " ", paste0(...))
  cli::cli_bullets(stats::setNames("{txt}", " "))
  invisible(NULL)
}

#' `.log_subdetail()` for rows whose columns must line up.
#'
#' Same glyph and indent, so the two are interchangeable within a block, but routed through
#' `cli_verbatim`: `cli_text`/`cli_bullets` normalise runs of whitespace for prose, which silently
#' collapses any padding used to align values into a column. Verbatim also passes literal `{}` through
#' untouched, so no escaping dance is needed.
#' @keywords internal
#' @noRd
#' A titled group inside a SUMMARY block: a blank line, then the heading in bold.
#'
#' Sections are what let a summary be scanned rather than read. The heading is deliberately plain text
#' rather than a `cli_rule()`: a SUMMARY already sits under one rule, and nesting a second set inside it
#' reads as a new stage of the run rather than a group within the same block.
#' @param lvl Resolved verbosity.
#' @param title Section heading.
#' @param min_level Lowest verbosity at which the section appears.
#' @keywords internal
#' @noRd
.log_section <- function(lvl, title, min_level = 1L) {
  if (lvl < min_level) return(invisible(NULL))
  cli::cli_text("")
  cli::cli_text("{.strong {title}}")
  invisible(NULL)
}

#' Aligned label/value rows for a summary section.
#'
#' `cli_verbatim()` rather than `cli_text()`, because the latter normalises runs of whitespace and would
#' collapse exactly the padding that makes the column line up. Values are formatted together so integers
#' share a width and the numbers read as a column.
#' @param lvl Resolved verbosity.
#' @param rows A named vector: names become the labels, values the right-hand column.
#' @param symbols One symbol per row, recycled; defaults to a bullet.
#' @param min_level Lowest verbosity at which the rows appear.
#' @keywords internal
#' @noRd
.log_rows <- function(lvl, rows, symbols = cli::symbol$bullet, min_level = 1L) {
  if (lvl < min_level || !length(rows)) return(invisible(NULL))
  labs <- paste0(names(rows), ":")
  pad  <- formatC(labs, width = -max(nchar(labs)))
  # numbers are formatted together so they right-align into a column; strings are left exactly as
  # given, since format() would pad them all to the longest and leave a ragged tail of whitespace
  vals <- if (is.numeric(rows)) format(rows, big.mark = ",") else as.character(rows)
  sym  <- rep(symbols, length.out = length(rows))
  for (i in seq_along(rows)) cli::cli_verbatim(paste0("  ", sym[i], " ", pad[i], "  ", vals[i]))
  invisible(NULL)
}

#' An aligned table inside a summary section.
#'
#' Some summary content is naturally tabular - one row per metric, a few counts each - and reads far
#' better as a grid than as one sentence per row. Columns are right-aligned except the first, which
#' names the row; `cli_verbatim()` again, so the padding survives.
#' @param lvl Resolved verbosity.
#' @param df A data frame; its names become the header row.
#' @param min_level Lowest verbosity at which the table appears.
#' @keywords internal
#' @noRd
.log_table <- function(lvl, df, min_level = 1L) {
  if (lvl < min_level || !NROW(df)) return(invisible(NULL))
  # the first column names the row and reads left-aligned; the rest are counts and line up on the right
  cells <- lapply(seq_along(df), function(j)
    format(as.character(df[[j]]), justify = if (j == 1L) "left" else "right"))
  w <- vapply(seq_along(cells), function(j)
    max(nchar(c(names(df)[j], cells[[j]]))), integer(1))
  pad <- function(x, j) formatC(x, width = if (j == 1L) -w[j] else w[j])
  line <- function(vals) paste0("  ", paste(vapply(seq_along(vals), function(j) pad(vals[j], j), ""),
                                            collapse = "   "))
  cli::cli_verbatim(cli::style_bold(line(names(df))))
  for (i in seq_len(nrow(df)))
    cli::cli_verbatim(line(vapply(cells, function(col) col[i], "")))
  invisible(NULL)
}

#' A free-text block inside a summary section, wrapped to the console and indented under its heading.
#' @param lvl Resolved verbosity.
#' @param text One string; wrapped at the console width.
#' @param min_level Lowest verbosity at which the block appears.
#' @keywords internal
#' @noRd
.log_block <- function(lvl, text, min_level = 1L) {
  if (lvl < min_level) return(invisible(NULL))
  width <- max(40L, min(getOption("width", 80L), 100L) - 4L)
  for (ln in strwrap(text, width = width)) cli::cli_verbatim(paste0("  ", ln))
  invisible(NULL)
}

#' A footnote below a summary table: indented, wrapped, and marked with the info symbol.
#'
#' A table cell cannot carry its own caveat, and a column heading wide enough to hold one
#' ("Off-scale (drawn at the axis edge, not removed)") destroys the grid it heads. The qualification
#' therefore sits below the table, hanging-indented so continuation lines do not read as new points.
#' @param lvl Resolved verbosity.
#' @param text One string; wrapped at the console width.
#' @param min_level Lowest verbosity at which the note appears.
#' @keywords internal
#' @noRd
.log_note <- function(lvl, text, min_level = 1L) {
  if (lvl < min_level) return(invisible(NULL))
  width <- max(40L, min(getOption("width", 80L), 100L) - 6L)
  ln <- strwrap(text, width = width)
  cli::cli_verbatim(paste0("  ", cli::symbol$info, " ", cli::col_grey(ln[1])))
  for (k in seq_along(ln)[-1]) cli::cli_verbatim(paste0("    ", cli::col_grey(ln[k])))
  invisible(NULL)
}

.log_subdetail_aligned <- function(lvl, ...) {
  if (lvl < 2L) return(invisible(NULL))
  cli::cli_verbatim(paste0("  \u21b3 ", paste0(...)))
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

#' `.log_arrow()` for rows whose columns must line up.
#'
#' Same glyph, but routed through `cli_verbatim` for the reason given at
#' `.log_subdetail_aligned()`: `cli_text` normalises runs of whitespace for prose, so any padding used
#' to align values into a column is silently collapsed.
#' @keywords internal
#' @noRd
.log_arrow_aligned <- function(lvl, ...) {
  if (lvl >= 1L) cli::cli_verbatim(paste0(cli::symbol$arrow_right, " ", paste0(...)))
  invisible(NULL)
}

#' A duration in words, spelling out the unit.
#'
#' The compact forms abbreviate minutes to "m", which reads as metres beside a speed in m/s or a gap
#' beside a depth. Console lines that sit next to such quantities spell the unit out instead; the
#' space-constrained plot labels keep the compact form.
#' @param secs Seconds.
#' @keywords internal
#' @noRd
.fmtSecondsSpelled <- function(secs) {
  if (!is.finite(secs)) return("unknown")
  if (secs < 90) return(sprintf("%.0f s", secs))
  if (secs < 5400) return(sprintf("%.1f min", secs / 60))
  sprintf("%.1f h", secs / 3600)
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

#' Round to `digits` and strip negative zero, so a residual tiny-negative value never prints as "-0".
#'
#' A value that rounds to zero from below is stored as IEEE negative zero, which `sprintf` renders with a
#' minus ("-0", "-0.00"). Since `-0 == 0` is TRUE in R, reassigning 0 replaces it with a positive zero.
#' Vectorised and NA-safe; returns a numeric to feed straight into a `sprintf`/`%f` format.
#' @keywords internal
#' @noRd
.noNegZero <- function(x, digits = 2) {
  r <- round(x, digits)
  r[!is.na(r) & r == 0] <- 0
  r
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
