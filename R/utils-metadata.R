# Input resolution without converting or mutating caller-supplied tables on read.
.metadataInput <- function(data, arg) {
  if (is.null(data)) .abort("{.arg {arg}} is {.code NULL}; expected a tag dataset or an .rds file path.")
  .assert_nonempty(data, arg)
  single <- is.data.frame(data) || (is.character(data) && length(data) == 1L)
  if (is.character(data)) {
    if (anyNA(data) || any(!nzchar(data))) .abort("{.arg {arg}} contains a missing or empty file path.")
    missing <- data[!file.exists(data)]
    if (length(missing)) .abort("Some files were not found in {.arg {arg}}: {.path {missing}}.")
    ids <- tools::file_path_sans_ext(basename(data))
    get <- function(i) readRDS(data[i])
  } else {
    if (is.data.frame(data)) data <- list(data)
    if (!is.list(data)) .abort("{.arg {arg}} must be a tag dataset, a list of datasets, or .rds file paths.")
    ids <- names(data) %||% as.character(seq_along(data))
    get <- function(i) data[[i]]
  }
  list(n = length(ids), ids = ids, single = single, get = function(i) {
    tag <- get(i)
    if (!is.data.frame(tag)) .abort("Element {i} of {.arg {arg}} is not a tag data.frame/data.table.")
    tag
  })
}


# An identity check prevents a stale attribute from directing edits to the wrong deployment.
.metadataTagId <- function(tag, meta, fallback, id.col = "ID") {
  .resolveDeploymentId(if (id.col %in% names(tag)) tag[[id.col]] else NULL,
                       stored = meta$id, fallback = fallback)
}


# Validate a partial patch before modifying any objects or writing files.
.metadataPatch <- function(updates) {
  named_list <- function(x, where) {
    nms <- names(x)
    if (!is.list(x) || is.data.frame(x) || !length(x) || is.null(nms) || anyNA(nms) ||
        any(!nzchar(trimws(nms))) || anyDuplicated(nms)) {
      .abort("{.arg updates} must contain non-empty lists with unique field names ({where}).")
    }
  }
  scalar <- function(value, field, text = FALSE) {
    if (is.factor(value)) value <- as.character(value)
    if (is.null(value) || !is.atomic(value) || length(value) != 1L || !is.null(dim(value))) {
      .abort("Update {.field {field}} must be a scalar value, not NULL or a nested object.")
    }
    if (text && !is.character(value)) .abort("Update {.field {field}} must be character (use NA_character_ for unknown).")
    if (is.numeric(value) && !is.na(value) && !is.finite(value)) .abort("Update {.field {field}} must not be infinite.")
    value
  }
  named_list(updates, "top level")
  protected <- setdiff(names(updates), c("animal_id", "deployment", "biometrics", "user"))
  if (length(protected)) .abort("Metadata fields {.val {protected}} are protected or unsupported.")
  patch <- updates
  if ("animal_id" %in% names(patch)) {
    patch$animal_id <- .normalizeIdentifier(scalar(patch$animal_id, "animal_id", text = TRUE), "animal_id")
    if (!is.na(patch$animal_id) && !nzchar(patch$animal_id)) patch$animal_id <- NA_character_
  }
  for (block in intersect(c("deployment", "biometrics", "user"), names(patch))) {
    named_list(patch[[block]], block)
    if (block == "deployment" && any(names(patch[[block]]) != "site")) {
      .abort("Only {.field deployment$site} is editable; other deployment fields are protected.")
    }
    for (field in names(patch[[block]])) {
      patch[[block]][[field]] <- scalar(patch[[block]][[field]], paste(block, field, sep = "$"), text = block == "deployment")
    }
  }
  patch
}


.metadataTableUpdates <- function(updates, columns) {
  updates <- as.data.frame(updates)
  if (anyDuplicated(names(updates))) .abort("Update table column names must be unique.")
  columns <- .as_metadata_columns(columns)
  required <- c(columns$id, columns$animal_id, columns$deploy_site, columns$traits)
  missing <- setdiff(required, names(updates))
  if (length(missing)) .abort("Columns {.val {missing}} not found in {.arg updates}.")
  if (!length(c(columns$animal_id, columns$deploy_site, columns$traits))) {
    .abort(c("{.arg columns} names no editable fields or traits.",
             "i" = "Map animal_id, deploy_site, or traits with metadataColumns()."))
  }
  id_values <- updates[[columns$id]]
  if (!is.atomic(id_values) || !is.null(dim(id_values))) .abort("Update table deployment IDs must be a scalar column, not nested objects.")
  ids <- .normalizeIdentifier(id_values, "update table ID")
  if (!length(ids) || anyNA(ids) || any(!nzchar(trimws(ids))) || anyDuplicated(ids)) {
    .abort("Update table deployment IDs must be non-missing, non-empty, and unique.")
  }
  patches <- lapply(seq_len(nrow(updates)), function(i) {
    patch <- list()
    if (!is.null(columns$animal_id)) patch$animal_id <- updates[[columns$animal_id]][i]
    if (!is.null(columns$deploy_site)) patch$deployment <- list(site = updates[[columns$deploy_site]][i])
    if (length(columns$traits)) patch$biometrics <- lapply(updates[columns$traits], function(v) v[i])
    .metadataPatch(patch)
  })
  stats::setNames(patches, ids)
}


#' Print a tag metadata record
#'
#' @description
#' Displays a compact summary of recorded identity, sensors, processing status, calibration,
#' clocks, ancillary data, and recent processing steps. The underlying named list is unchanged.
#' @param x A `nautilus_metadata` record returned by [getTagMetadata()].
#' @param ... Unused.
#' @return `x`, invisibly.
#' @seealso [getTagMetadata()], [processingHistory()]
#' @exportS3Method print nautilus_metadata
print.nautilus_metadata <- function(x, ...) {
  # Never expand tables, matrices, or long histories; cap each line to the console width.
  width <- max(40L, getOption("width", 80L))
  tz <- x$sensors$timezone
  if (!is.character(tz) || length(tz) != 1L || is.na(tz) || !nzchar(tz)) tz <- "UTC"
  compact <- function(value, limit = 4L) {
    if (is.null(value) || !length(value)) return("not recorded")
    if (is.data.frame(value)) return(sprintf("%d rows", nrow(value)))
    if (is.list(value)) return(sprintf("%d fields", length(value)))
    if (!is.null(dim(value))) return(paste(dim(value), collapse = " x "))
    v <- if (inherits(value, "POSIXt")) format(value, "%Y-%m-%d %H:%M:%S", tz = tz) else as.character(value)
    v[is.na(v)] <- "unknown"
    paste0(paste(utils::head(v, limit), collapse = ", "), if (length(v) > limit) sprintf(" (+%d)", length(v) - limit) else "")
  }
  line <- function(label, value) {
    text <- gsub("[\r\n\t]", " ", sprintf("  %-12s: %s", label, value))
    if (nchar(text, type = "width") > width) text <- paste0(substr(text, 1L, width - 3L), "...")
    cat(text, "\n", sep = "")
  }
  title <- gsub("[\r\n\t]", " ", sprintf("<nautilus_metadata> %s", compact(x$id)))
  if (nchar(title, type = "width") > width) title <- paste0(substr(title, 1L, width - 3L), "...")
  cat(title, "\n", sep = "")
  line("animal", compact(x$animal_id))
  line("tag", paste(compact(x$tag$model), compact(x$tag$type), sep = " / "))
  line("hardware", sprintf("package %s; logger %s", compact(x$tag$package_id), compact(x$tag$logger_id)))
  line("deployment", sprintf("%s; %s", compact(x$deployment$site), compact(x$deployment$datetime)))
  line("position", sprintf("lon %s; lat %s", compact(x$deployment$lon), compact(x$deployment$lat)))
  line("span", sprintf("%s -> %s", compact(x$span$first_datetime), compact(x$span$last_datetime)))
  line("sampling", sprintf("original %s; processed %s Hz", compact(x$sensors$sampling_hz_original), compact(x$sensors$sampling_hz_processed)))
  line("sensors", paste0("present: ", compact(x$sensors$present, 6L)))
  if (length(x$sensors$excluded)) line("excluded", compact(x$sensors$excluded, 6L))

  history <- x$processing %||% list()
  processed <- Filter(function(p) identical(p$step, "processTagData"), history)
  if (length(processed)) {
    p <- processed[[length(processed)]]
    line("status", paste0(compact(p$status), if (identical(p$status, "partial")) paste0("; ", compact(p$partial_reason)) else ""))
  }
  mapping <- x$axis_mapping
  line("axis mapping", paste0(if (is.null(mapping)) "not recorded" else if (isTRUE(mapping$applied)) "applied" else "not applied",
                              if (isTRUE(mapping$applied)) paste0("; ", compact(mapping$source)) else ""))
  mc <- x$mag_calibration
  state <- if (is.null(mc)) "not recorded" else if (isTRUE(mc$applied)) "applied" else if (!is.null(mc$proposed)) "proposed, not applied" else "not applied"
  confidence <- if (isTRUE(mc$applied)) mc$qc$confidence else mc$proposed$qc$confidence
  line("mag calib", paste0(compact(mc$status), "; ", state, if (!is.null(confidence)) paste0("; ", compact(confidence), " confidence") else ""))
  line("heading", compact(x$deployment$heading_reference))
  device <- x$sidecar$device$utc_offset
  logging <- x$sidecar$logging$utc_offset
  source_clock <- x$sensors$clock$source_timezone
  if (all(is.na(source_clock)) && length(x$sensors$clock$explicit_offsets))
    source_clock <- x$sensors$clock$explicit_offsets
  line("clocks", paste0("display ", compact(x$sensors$timezone),
                        if (!is.null(x$sensors$clock)) paste0("; source ", compact(source_clock),
                                                             " (", compact(x$sensors$clock$mode), ")") else "",
                        if (!is.null(logging)) paste0("; logging ", compact(logging), "h") else "",
                        if (!is.null(device)) paste0("; device ", compact(device), "h") else ""))
  if (length(x$ancillary)) {
    counts <- vapply(x$ancillary, function(a) compact(if (is.list(a) && !is.data.frame(a) && !is.null(a$data)) a$data else a), character(1))
    line("ancillary", paste(names(counts), counts, sep = " ", collapse = "; "))
  }
  fields <- function(block) {
    values <- utils::head(block, 4L)
    paste0(paste(names(values), vapply(values, compact, character(1)), sep = "=", collapse = "; "),
           if (length(block) > 4L) sprintf(" (+%d fields)", length(block) - 4L) else "")
  }
  if (length(x$biometrics)) line("biometrics", fields(x$biometrics))
  if (length(x$user)) line("user", fields(x$user))
  steps <- vapply(history, function(p) compact(p$step), character(1))
  line("history", sprintf("%d step%s%s", length(history), if (length(history) != 1L) "s" else "",
                          if (length(steps)) paste0("; latest: ", paste(utils::tail(steps, 3L), collapse = " -> ")) else ""))
  cat("  Full fields: $ or str(); processing records: processingHistory(tag)\n")
  invisible(x)
}
