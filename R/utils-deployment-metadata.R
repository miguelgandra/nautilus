#######################################################################################################
# Normalisation #######################################################################################
#######################################################################################################

#' Normalise a raw metadata table into the canonical role-named form.
#'
#' Renames each mapped, present role to its canonical name, trims/coerces identifier and datetime
#' columns, drops passthrough columns that would collide with canonical names, and sorts rows
#' chronologically by deployment datetime. Returns the canonical data.frame, the vector of mapped roles,
#' an identity schema (each present role mapped to its own canonical name, for importTagData), and a
#' data.frame of normalisation notes (info-severity issues).
#' @keywords internal
#' @noRd
.qcNormalizeMetadata <- function(metadata, columns) {
  # `traits` is NOT a role: it names a SET of passive attribute columns carried through VERBATIM (under
  # their own names). Keep it out of the single-column role machinery; validate its columns separately.
  traits <- columns$traits
  role_cols <- columns[setdiff(names(columns), "traits")]

  # which roles are mapped (non-NULL) AND present as columns in `metadata`
  mapped <- role_cols[!vapply(role_cols, is.null, logical(1))]
  missing_cols <- vapply(names(mapped), function(r) !mapped[[r]] %in% names(metadata), logical(1))
  if (any(missing_cols)) {
    bad <- names(mapped)[missing_cols]; src <- unlist(mapped[missing_cols]); n_bad <- length(bad)
    .abort(c("{cli::qty(n_bad)}Column{?s} {.val {unname(src)}} (set via {.code columns${bad}}) {?was/were} not found in {.arg metadata}.",
             "i" = "Columns present: {.val {names(metadata)}}."))
  }
  miss_tr <- setdiff(traits, names(metadata))
  if (length(miss_tr)) {
    .abort(c("{cli::qty(length(miss_tr))}Trait column{?s} {.val {miss_tr}} (set via {.code columns$traits}) {?was/were} not found in {.arg metadata}.",
             "i" = "Columns present: {.val {names(metadata)}}."))
  }
  roles <- names(mapped)

  # build the canonical table from the mapped roles
  d <- data.frame(row.names = seq_len(nrow(metadata)))
  for (r in roles) d[[r]] <- metadata[[mapped[[r]]]]
  # carry through any unmapped source columns under their original names (no canonical clash)
  passthrough <- setdiff(names(metadata), c(unlist(mapped), roles))
  for (cn in passthrough) d[[cn]] <- metadata[[cn]]

  notes <- .empty_issues()

  # Every character cell here came out of the user's spreadsheet, and field metadata carries accented
  # text as a matter of course (site names, observer names, free-text notes). Readers hand those back
  # with no declared encoding, so a value saved from one locale is ambiguous in another - warnings on
  # read, or mojibake in a figure label. Normalise the whole table once, at the point it enters the
  # package, so every consumer downstream sees unambiguous UTF-8.
  for (cn in names(d)) if (is.character(d[[cn]])) d[[cn]] <- .toUTF8(d[[cn]])

  # coerce / trim identifier-like roles, recording any value that was changed by trimming
  for (r in intersect(c("id", "package_id", "logger_id"), roles)) {
    raw <- as.character(d[[r]])
    trimmed <- trimws(raw)
    trimmed <- gsub("[[:space:]]+", " ", trimmed)
    changed <- which(!is.na(raw) & raw != trimmed)
    d[[r]] <- trimmed
    if (length(changed)) {
      notes <- rbind(notes, .issue(NA_character_, "identifier_normalized", "info",
        sprintf("%s: %d value%s normalised (whitespace trimmed)", r, length(changed),
                if (length(changed) != 1L) "s" else "")))
    }
  }

  # datetime roles must be POSIXct (fail-fast, consistent with importTagData)
  for (r in intersect(c("deploy_datetime", "recovery_datetime", "popup_datetime"), roles)) {
    if (!inherits(d[[r]], "POSIXct")) {
      .abort(c("Column {.val {mapped[[r]]}} ({.code columns${r}}) must be of class {.cls POSIXct}.",
               "i" = "Parse it first, e.g. with {.fn as.POSIXct} or {.fn lubridate::ymd_hms}."))
    }
  }
  # numeric coordinate roles
  for (r in intersect(c("deploy_lon", "deploy_lat", "popup_lon", "popup_lat"), roles)) {
    d[[r]] <- .asNumericSafe(d[[r]])          # factor coords became level codes (lon 1, lat 2)
  }

  # chronological order (stable) by deployment datetime when available
  if ("deploy_datetime" %in% roles) d <- d[order(d[["deploy_datetime"]], method = "radix"), , drop = FALSE]
  rownames(d) <- NULL

  # identity schema: each present role mapped to its own canonical name; traits kept under their ORIGINAL
  # column names (they are carried through verbatim above), so importTagData reads the right columns.
  schema_args <- stats::setNames(as.list(roles), roles)
  if (length(traits)) schema_args$traits <- traits
  schema <- do.call(metadataColumns, schema_args)

  list(data = d, roles = roles, schema = schema, notes = notes)
}


#######################################################################################################
# Check engine (shared with importTagData's inline guard) #############################################
#######################################################################################################

#' Run the role-gated metadata checks on a canonical (normalised) table.
#'
#' `d` is a canonical table as produced by .qcNormalizeMetadata; `roles` is the vector of mapped
#' roles. Each check runs only if the roles it needs are present. Returns a data.frame of issues
#' (columns: id, check, severity, message); zero rows when the metadata is clean. This is the single
#' source of truth for both checkDeploymentMetadata() and importTagData()'s pre-flight guard.
#' @keywords internal
#' @noRd
.runMetadataQC <- function(d, roles, future.tolerance = 1) {
  iss <- .empty_issues()
  has <- function(...) all(c(...) %in% roles)
  ids <- if ("id" %in% roles) d[["id"]] else rep(NA_character_, nrow(d))

  # --- universal core: id + deploy_datetime --------------------------------------------------------
  if ("id" %in% roles) {
    blank <- is.na(ids) | !nzchar(ids)
    if (any(blank))
      iss <- rbind(iss, .issue(NA_character_, "missing_id", "error",
        sprintf("%d deployment%s with a missing/blank ID", sum(blank), if (sum(blank) != 1L) "s" else "")))
    dup <- ids[!blank][duplicated(ids[!blank])]
    for (v in unique(dup))
      iss <- rbind(iss, .issue(v, "duplicate_id", "error", sprintf("duplicate deployment ID: %s", v)))
  }

  if ("deploy_datetime" %in% roles) {
    dep <- d[["deploy_datetime"]]
    na_dep <- is.na(dep)
    for (i in which(na_dep))
      iss <- rbind(iss, .issue(ids[i], "missing_deploy", "error",
        sprintf("%s: missing deployment datetime", ids[i] %||% "<row>")))
    future_cut <- Sys.time() + future.tolerance * 86400
    for (i in which(!na_dep & dep > future_cut))
      iss <- rbind(iss, .issue(ids[i], "future_deploy", "warning",
        sprintf("%s: deployment datetime is in the future (%s)", ids[i], format(dep[i], "%Y-%m-%d"))))
  }

  # --- location plausibility: deploy_lon + deploy_lat ----------------------------------------------
  if (has("deploy_lon", "deploy_lat")) {
    lon <- d[["deploy_lon"]]; lat <- d[["deploy_lat"]]
    bad <- which((!is.na(lon) & abs(lon) > 180) | (!is.na(lat) & abs(lat) > 90))
    for (i in bad)
      iss <- rbind(iss, .issue(ids[i], "implausible_location", "error",
        sprintf("%s: coordinates out of range (lon %s, lat %s)", ids[i], lon[i], lat[i])))
  }

  # --- recovery vs deployment: needs recovery_datetime + deploy_datetime ---------------------------
  if (has("deploy_datetime", "recovery_datetime")) {
    dep <- d[["deploy_datetime"]]; rec <- d[["recovery_datetime"]]
    both <- which(!is.na(dep) & !is.na(rec))
    for (i in both[rec[both] < dep[both]])
      iss <- rbind(iss, .issue(ids[i], "recovery_before_deploy", "error",
        sprintf("%s: recovery (%s) precedes deployment (%s)", ids[i],
                format(rec[i], "%Y-%m-%d"), format(dep[i], "%Y-%m-%d"))))
  }

  # --- package overlap: two deployments on one package at the same time ----------------------------
  #     needs package_id (the constant-orientation housing) + both window ends
  if (has("package_id", "deploy_datetime", "recovery_datetime")) {
    iss <- rbind(iss, .checkWindowOverlap(d, ids, "package_id", "package"))
  }

  # --- logger reuse: informational (a logger reused across deployments is normal, but worth noting) -
  if ("logger_id" %in% roles) {
    lg <- d[["logger_id"]]; lg <- lg[!is.na(lg) & nzchar(lg)]
    reused <- names(which(table(lg) > 1L))
    if (length(reused))
      iss <- rbind(iss, .issue(NA_character_, "logger_reuse", "info",
        sprintf("%d logger%s reused across deployments (%s)", length(reused),
                if (length(reused) != 1L) "s" else "", paste(reused, collapse = ", "))))
  }

  rownames(iss) <- NULL
  iss
}


#' Flag overlapping deploy/recovery windows within each group of a key column.
#' @keywords internal
#' @noRd
.checkWindowOverlap <- function(d, ids, key, key_label) {
  iss <- .empty_issues()
  dep <- d[["deploy_datetime"]]; rec <- d[["recovery_datetime"]]
  grp <- d[[key]]
  usable <- !is.na(grp) & nzchar(as.character(grp)) & !is.na(dep) & !is.na(rec)
  for (g in unique(grp[usable])) {
    idx <- which(usable & grp == g)
    if (length(idx) < 2L) next
    idx <- idx[order(dep[idx])]
    for (j in seq_len(length(idx) - 1L)) {
      a <- idx[j]; b <- idx[j + 1L]
      # same-id rows overlapping is a duplicated record, already flagged by duplicate_id - skip here
      if (!is.na(ids[a]) && !is.na(ids[b]) && identical(ids[a], ids[b])) next
      if (dep[b] < rec[a]) {                            # next deploy starts before current recovery
        iss <- rbind(iss, .issue(NA_character_, paste0(key_label, "_overlap"), "error",
          sprintf("overlapping deployments on %s %s: %s and %s", key_label, g,
                  ids[a] %||% "<row>", ids[b] %||% "<row>")))
      }
    }
  }
  iss
}


#######################################################################################################
# Issue helpers, object constructor and accessors #####################################################
#######################################################################################################

#' An empty issues data.frame (the canonical issue schema).
#' @keywords internal
#' @noRd
.empty_issues <- function() {
  data.frame(id = character(0), check = character(0), severity = character(0),
             message = character(0), stringsAsFactors = FALSE)
}


#' Parse the `exclude_sensors` metadata column into known-unusable channels per deployment.
#'
#' Returns a list with `excluded` (tidy id/channel rows) and `issues` (error findings for any
#' unparseable token value - lenient, so a typo is reported rather than aborting the whole QC).
#' @keywords internal
#' @noRd
.parseExcludeSensors <- function(d) {
  excluded <- data.frame(id = character(0), channel = character(0), stringsAsFactors = FALSE)
  bad <- .empty_issues()
  if (!"exclude_sensors" %in% names(d)) return(list(excluded = excluded, issues = bad))
  ids <- if ("id" %in% names(d)) d$id else as.character(seq_len(nrow(d)))
  for (i in seq_len(nrow(d))) {
    chans <- tryCatch(.expandSensorTokens(d$exclude_sensors[i]),
                      error = function(e) { bad <<- rbind(bad, .issue(ids[i], "invalid_exclude_sensors", "error",
                        sprintf("%s: invalid exclude_sensors value %s", ids[i],
                                encodeString(as.character(d$exclude_sensors[i]), quote = "'")))); character(0) })
    if (length(chans)) excluded <- rbind(excluded, data.frame(id = ids[i], channel = chans, stringsAsFactors = FALSE))
  }
  rownames(excluded) <- NULL
  list(excluded = excluded, issues = bad)
}


#' Turn the per-deployment excluded channels into info issues (one row per deployment).
#' @keywords internal
#' @noRd
.validityIssues <- function(excluded) {
  if (is.null(excluded) || nrow(excluded) == 0L) return(.empty_issues())
  rows <- lapply(split(excluded, excluded$id), function(g) {
    .issue(g$id[1], "known_bad_sensor", "info",
           sprintf("%s: %s unusable", g$id[1], paste(.channelsToFamilies(g$channel), collapse = ", ")))
  })
  out <- do.call(rbind, rows); rownames(out) <- NULL; out
}


#' A single issue row.
#' @keywords internal
#' @noRd
.issue <- function(id, check, severity, message) {
  data.frame(id = id %||% NA_character_, check = check, severity = severity, message = message,
             stringsAsFactors = FALSE)
}


#' Construct a nautilus_deployments object (a data.frame carrying the QC stamp, issues and schema).
#' @keywords internal
#' @noRd
.new_nautilus_deployments <- function(d, schema, stamp, issues, roles) {
  attr(d, "nautilus.columns") <- schema
  attr(d, "nautilus.qc")      <- stamp
  attr(d, "nautilus.qc.issues") <- issues
  attr(d, "nautilus.qc.roles")  <- roles
  class(d) <- c("nautilus_deployments", "data.frame")
  d
}


#' Is `x` a QC'd deployments table?
#' @keywords internal
#' @noRd
is_nautilus_deployments <- function(x) inherits(x, "nautilus_deployments")


#######################################################################################################
# Console reporting and S3 print ######################################################################
#######################################################################################################

#' Render the QC console summary (header + detail notes + SUMMARY block), level-gated.
#' @keywords internal
#' @noRd
.qcReportConsole <- function(x, lvl, start.time) {
  if (lvl < 1L) return(invisible(NULL))
  roles  <- attr(x, "nautilus.qc.roles")
  stamp  <- attr(x, "nautilus.qc")
  issues <- attr(x, "nautilus.qc.issues")
  n <- nrow(x)

  # header bullets: deployment count, and package/logger counts when those roles are mapped
  bullets <- sprintf("Deployments: %d", n)
  counts <- character(0)
  if ("package_id" %in% roles) counts <- c(counts, sprintf("packages: %d", .nDistinct(x[["package_id"]])))
  if ("logger_id" %in% roles)  counts <- c(counts, sprintf("loggers: %d", .nDistinct(x[["logger_id"]])))
  if (length(counts)) bullets <- c(bullets, paste(counts, collapse = paste0(" ", .mid_dot(), " ")))
  .log_header(lvl, "checkDeploymentMetadata", "Validating deployment metadata",
              bullets = bullets, arrow = paste0("Roles mapped: ", paste(roles, collapse = ", ")))

  # detail notes (lvl >= 2): normalisation + informational findings
  notes <- issues[issues$severity == "info", , drop = FALSE]
  if (lvl >= 2L) for (m in notes$message) .log_detail(lvl, m)

  # SUMMARY block
  .log_summary(lvl)
  n_err <- stamp$n_errors; n_warn <- stamp$n_warnings
  tally <- sprintf("%d deployment%s validated %s %d error%s %s %d warning%s",
                   n, if (n != 1L) "s" else "", .mid_dot(), n_err, if (n_err != 1L) "s" else "",
                   .mid_dot(), n_warn, if (n_warn != 1L) "s" else "")
  if (n_err > 0L)        cli::cli_alert_danger(tally)
  else if (n_warn > 0L)  cli::cli_alert_warning(tally)
  else                   cli::cli_alert_success(tally)

  # itemise errors then warnings as nested sub-lines
  sub <- .qc_sub()
  for (m in issues$message[issues$severity == "error"])   cli::cli_text("{sub} {.strong {m}}")
  for (m in issues$message[issues$severity == "warning"]) cli::cli_text("{sub} {m}")

  # known-bad sensors (exclude_sensors column): a data-quality note surfaced at any level
  n_badsens <- length(unique(issues$id[issues$check == "known_bad_sensor" & !is.na(issues$id)]))
  if (n_badsens > 0L)
    cli::cli_alert_info("{n_badsens} deployment{?s} with known-unusable sensors (excluded at import)")

  # verdict line
  if (stamp$status == "failed")
    cli::cli_alert_danger("metadata QC: FAILED ({n_err} error{?s} to fix before import)")
  else if (stamp$status == "passed_with_warnings")
    cli::cli_alert_warning("metadata QC: passed with warnings (review the {cli::qty(n_warn)}item{?s} above)")
  else
    cli::cli_alert_success("metadata QC: passed")
  .log_runtime(lvl, start.time)
  invisible(NULL)
}


#' Number of distinct non-missing values.
#' @keywords internal
#' @noRd
.nDistinct <- function(v) { v <- v[!is.na(v) & nzchar(as.character(v))]; length(unique(v)) }


#' Locale-safe middle dot / nested-arrow markers (UTF-8 glyph, ASCII fallback).
#' @keywords internal
#' @noRd
.mid_dot <- function() if (cli::is_utf8_output()) "\u00b7" else "-"


#' @keywords internal
#' @noRd
.qc_sub  <- function() if (cli::is_utf8_output()) "  \u21b3" else paste0("  ", cli::symbol$arrow_right)


#' Print a nautilus_deployments object
#'
#' Shows the QC verdict, deployment/package/logger counts and the issue tally, then a preview of the
#' cleaned table.
#'
#' @param x A `nautilus_deployments` object.
#' @param ... Passed to the data.frame print method for the row preview.
#' @return `x`, invisibly.
#' @exportS3Method print nautilus_deployments
print.nautilus_deployments <- function(x, ...) {
  stamp <- attr(x, "nautilus.qc"); roles <- attr(x, "nautilus.qc.roles")
  cat(cli::style_bold(sprintf("<nautilus_deployments> %d deployments", nrow(x))), "\n")
  verdict <- switch(stamp$status,
                    failed = cli::col_red(sprintf("FAILED (%d error%s, %d warning%s)",
                              stamp$n_errors, if (stamp$n_errors != 1L) "s" else "",
                              stamp$n_warnings, if (stamp$n_warnings != 1L) "s" else "")),
                    passed_with_warnings = cli::col_yellow(sprintf("passed with %d warning%s",
                              stamp$n_warnings, if (stamp$n_warnings != 1L) "s" else "")),
                    cli::col_green("passed"))
  cat(sprintf("  QC       : %s\n", verdict))
  counts <- character(0)
  if ("package_id" %in% roles) counts <- c(counts, sprintf("%d packages", .nDistinct(x[["package_id"]])))
  if ("logger_id" %in% roles)  counts <- c(counts, sprintf("%d loggers", .nDistinct(x[["logger_id"]])))
  if (length(counts)) cat(sprintf("  grouping : %s\n", paste(counts, collapse = ", ")))
  cat(cli::col_silver("  --- first rows ---\n"))
  print(utils::head(as.data.frame(x), 5L), ...)
  invisible(x)
}
