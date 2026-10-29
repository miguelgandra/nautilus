# Shared ingestion contracts. These helpers return values; callers own any table they edit.

.normalizeIdentifier <- function(x, field) {
  if (!(is.character(x) || is.factor(x) || is.numeric(x) || (is.logical(x) && all(is.na(x)))) ||
      inherits(x, c("Date", "POSIXt", "difftime")) || !is.null(dim(x)))
    .abort("{.field {field}} must contain character or numeric identifiers, not nested objects.")
  if (is.numeric(x) && any(!is.na(x) & !is.finite(x)))
    .abort("{.field {field}} must not contain non-finite numeric identifiers.")
  gsub("[[:space:]]+", " ", trimws(.toUTF8(as.character(x))))
}

# All known identity sources must agree. An argument supplies a missing identity; it is not a relabel.
.resolveDeploymentId <- function(data.ids = NULL, id = NULL, metadata = NULL, stored = NULL,
                                  fallback = NULL) {
  candidates <- character(0)
  add <- function(value, field, optional = TRUE) {
    if (is.null(value)) return(invisible(NULL))
    if (length(value) != 1L) .abort("{.field {field}} must be a single deployment ID.")
    value <- .normalizeIdentifier(value, field)
    if (optional && is.na(value)) return(invisible(NULL))
    if (is.na(value) || !nzchar(value)) .abort("{.field {field}} must be one non-missing, non-empty deployment ID.")
    candidates[field] <<- value
  }
  if (!is.null(data.ids)) {
    values <- unique(.normalizeIdentifier(data.ids, "data ID"))
    if (!length(values) || anyNA(values) || any(!nzchar(values)))
      .abort("Each tag dataset must contain one non-missing, non-empty deployment ID in its ID column.")
    if (length(values) > 1L)
      .abort(c("Each tag dataset must contain one non-missing deployment ID; the dataset contains several IDs ({.val {values}}).",
               "i" = "Build ONE deployment at a time; split the data by ID first."))
    add(values, "data ID")
  }
  add(id, "id", optional = FALSE)
  add(metadata[["id"]], "metadata$id")
  add(metadata[["ID"]], "metadata$ID")
  add(stored, "stored metadata ID")
  if (length(unique(candidates)) > 1L)
    .abort(c("A deployment ID does not match the other identity sources: {paste(names(candidates), candidates, sep = ' = ', collapse = '; ')}.",
             "i" = "An explicit id cannot relabel existing data or metadata; correct the source identities first."))
  if (length(candidates)) return(unname(candidates[1L]))
  if (!is.null(fallback)) {
    add(fallback, "deployment ID", optional = FALSE)
    return(unname(candidates[1L]))
  }
  .abort("No deployment {.arg id}: pass {.arg id}, an {.val ID} column, or an {.code id} in {.arg metadata}.")
}

# Normalize only mapped roles; passive traits keep their scientific values and types.
# Invalid numeric text is reported before coercion can conceal it as an ordinary missing value.
.normalizeMetadataValues <- function(d, roles) {
  notes <- .empty_issues()
  for (r in roles) {
    v <- d[[r]]
    if (!is.atomic(v) || !is.null(dim(v)) || length(v) != nrow(d))
      .abort("Metadata field {.val {r}} must contain one scalar value per deployment row.")
  }
  ids <- if ("id" %in% roles) .normalizeIdentifier(d$id, "id") else rep(NA_character_, nrow(d))
  for (r in intersect(c("id", "animal_id", "package_id", "logger_id"), roles)) {
    raw <- as.character(d[[r]])
    value <- .normalizeIdentifier(d[[r]], r)
    changed <- which(!is.na(raw) & raw != value)
    if (r != "id") value[!is.na(value) & !nzchar(value)] <- NA_character_
    d[[r]] <- value
    if (length(changed))
      notes <- rbind(notes, .issue(NA_character_, "identifier_normalized", "info",
        sprintf("%s: %d value(s) normalised (whitespace trimmed)", r, length(changed))))
  }
  for (r in intersect(c("deploy_datetime", "recovery_datetime", "popup_datetime", "data_start"), roles)) {
    if (!inherits(d[[r]], "POSIXct"))
      .abort("Metadata field {.val {r}} must be of class {.cls POSIXct}; parse it before ingestion.")
  }
  for (r in intersect(c("deploy_lon", "deploy_lat", "popup_lon", "popup_lat"), roles)) {
    raw <- d[[r]]
    if (!(is.numeric(raw) || is.character(raw) || is.factor(raw) || (is.logical(raw) && all(is.na(raw)))) ||
        inherits(raw, c("Date", "POSIXt", "difftime")) || !is.null(dim(raw)))
      .abort("Metadata field {.val {r}} must contain numeric coordinates or numeric text.")
    value <- .asNumericSafe(raw)
    for (i in which(!is.na(raw) & is.na(value)))
      notes <- rbind(notes, .issue(ids[i], "invalid_coordinate", "error",
        sprintf("%s: %s is not a numeric coordinate (%s)", ids[i], r, as.character(raw[i]))))
    d[[r]] <- value
  }
  if ("paddle_wheel" %in% roles) {
    v <- d$paddle_wheel
    if (!is.logical(v) && !(is.numeric(v) && all(is.na(v) | v %in% c(0, 1))))
      .abort("Metadata field {.val paddle_wheel} must be logical or numeric 0/1.")
    d$paddle_wheel <- as.logical(v)
  }
  text <- intersect(c("tag_model", "tag_type", "deploy_site", "axis_config", "attachment_site",
                      "deployment_type", "tag_format", "exclude_sensors"), roles)
  for (r in text) if (is.factor(d[[r]])) d[[r]] <- as.character(d[[r]])
  for (cn in names(d)) if (is.character(d[[cn]])) d[[cn]] <- .toUTF8(d[[cn]])
  list(data = d, notes = notes)
}

# A constructor row is partial metadata, not a required study roster. Supplied fields still obey
# the same numeric, geographic and chronology rules as QC/import; absent fields stay unknown.
.normalizeMetadataRow <- function(metadata, traits = NULL) {
  md <- .asMetadataRow(metadata)
  if (!is.null(traits) && (!is.character(traits) || anyNA(traits) || any(!nzchar(traits))))
    .abort("{.arg traits} must be a character vector of metadata field names.")
  missing <- setdiff(traits, names(md))
  if (length(missing)) .abort("Trait fields {.val {missing}} were not found in {.arg metadata}.")
  if (is.null(md)) return(NULL)
  if ("ID" %in% names(md) && !"id" %in% names(md)) md$id <- md$ID
  roles <- intersect(setdiff(names(formals(metadataColumns)), "traits"), names(md))
  d <- as.data.frame(md, optional = TRUE, stringsAsFactors = FALSE)
  norm <- .normalizeMetadataValues(d, roles)
  iss <- rbind(norm$notes, .runMetadataQC(norm$data, roles, require.deployment = FALSE))
  errors <- iss$message[iss$severity == "error"]
  if (length(errors)) .abort(c("Invalid deployment metadata:", stats::setNames(errors, rep("x", length(errors)))))
  as.list(norm$data)
}

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
  if (anyDuplicated(names(metadata))) .abort("Metadata column names must be unique.")
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

  norm <- .normalizeMetadataValues(d, roles)
  d <- norm$data
  notes <- norm$notes

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
.runMetadataQC <- function(d, roles, future.tolerance = 1, require.deployment = TRUE) {
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
    for (i in which(na_dep & require.deployment))
      iss <- rbind(iss, .issue(ids[i], "missing_deploy", "error",
        sprintf("%s: missing deployment datetime", ids[i] %||% "<row>")))
    future_cut <- Sys.time() + future.tolerance * 86400
    for (i in which(!na_dep & dep > future_cut))
      iss <- rbind(iss, .issue(ids[i], "future_deploy", "warning",
        sprintf("%s: deployment datetime is in the future (%s)", ids[i], format(dep[i], "%Y-%m-%d"))))
  }

  # Geographic pairs are optional for partial constructor metadata. A supplied pair must be
  # complete and finite; required roster positions must also be present.
  for (prefix in c("deploy", "popup")) {
    lon_role <- paste0(prefix, "_lon"); lat_role <- paste0(prefix, "_lat")
    if (!any(c(lon_role, lat_role) %in% roles)) next
    lon <- if (lon_role %in% roles) d[[lon_role]] else rep(NA_real_, nrow(d))
    lat <- if (lat_role %in% roles) d[[lat_role]] else rep(NA_real_, nrow(d))
    bad <- which((!is.na(lon) & (!is.finite(lon) | abs(lon) > 180)) |
                 (!is.na(lat) & (!is.finite(lat) | abs(lat) > 90)) |
                 is.nan(lon) | is.nan(lat))
    for (i in bad)
      iss <- rbind(iss, .issue(ids[i], "implausible_location", "error",
        sprintf("%s: %s coordinates out of range or non-finite (lon %s, lat %s)", ids[i], prefix, lon[i], lat[i])))
    for (i in which(xor(is.na(lon), is.na(lat))))
      iss <- rbind(iss, .issue(ids[i], "incomplete_location", "error",
        sprintf("%s: %s longitude and latitude must be supplied together", ids[i], prefix)))
    if (prefix == "deploy" && require.deployment) {
      for (i in which(is.na(lon) & is.na(lat)))
        iss <- rbind(iss, .issue(ids[i], "missing_location", "error",
          sprintf("%s: missing deployment coordinates", ids[i])))
    }
  }

  for (r in intersect(c("deploy_datetime", "recovery_datetime", "popup_datetime", "data_start"), roles)) {
    v <- as.numeric(d[[r]])
    for (i in which(is.nan(v) | (!is.na(v) & !is.finite(v))))
      iss <- rbind(iss, .issue(ids[i], "invalid_datetime", "error",
        sprintf("%s: %s must be a finite POSIXct instant or NA", ids[i], r)))
  }
  for (r in intersect(c("recovery_datetime", "popup_datetime"), roles)) {
    if (!"deploy_datetime" %in% roles) next
    dep <- d[["deploy_datetime"]]; event <- d[[r]]
    before <- which(is.finite(as.numeric(dep)) & is.finite(as.numeric(event)) & event < dep)
    for (i in before)
      iss <- rbind(iss, .issue(ids[i], sub("_datetime$", "_before_deploy", r), "error",
        sprintf("%s: %s (%s) precedes deployment (%s)", ids[i], r,
                format(event[i], "%Y-%m-%d"), format(dep[i], "%Y-%m-%d"))))
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
