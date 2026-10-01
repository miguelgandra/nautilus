#######################################################################################################
# Public metadata inspection and editing ##############################################################
#######################################################################################################

#' Retrieve the metadata of a tag deployment
#'
#' @description
#' Returns the consolidated metadata carried by a tag dataset, including deployment details,
#' biological traits, sensor and clock information, calibration state, ancillary streams, and
#' processing history. The metadata is returned as a named list with a compact print method;
#' individual fields remain accessible with `$` and the complete structure with [utils::str()].
#'
#' @param x A `nautilus_tag`, a legacy data.frame/data.table produced by nautilus, a list of such
#'   objects, or a character vector of `.rds` file paths. Each dataset must describe one deployment.
#'
#' @details
#' The returned record is an independent snapshot. Editing it does not update the source dataset;
#' use [updateTagMetadata()] to change supported descriptive fields. Legacy flat attributes are
#' interpreted using the current schema without modifying the input object or file.
#'
#' The print method reports recorded metadata only: it does not recompute sensor statistics or
#' validate a dataset. Calibration estimates are distinguished from applied corrections, and the
#' latest recorded processing status is shown when available. Matrices, ancillary tables, and full
#' processing records are retained in the list but not expanded in the console summary.
#'
#' @return For a single object or file, a named list of class `nautilus_metadata`. For a list of
#'   objects or multiple file paths, a named list of these records, keyed by deployment ID.
#' @seealso [updateTagMetadata()], [processingHistory()], [metadataColumns()]
#' @examples
#' \dontrun{
#' meta <- getTagMetadata(tag)
#' meta$biometrics$sex
#' meta$mag_calibration$proposed$qc
#' getTagMetadata("data/PIN_01.rds")
#' }
#' @export
getTagMetadata <- function(x) {
  input <- .metadataInput(x, "x")
  records <- vector("list", input$n)
  ids <- character(input$n)
  for (i in seq_len(input$n)) {
    tag <- input$get(i)
    meta <- .getMeta(tag) %||% .metaFromFlatAttrs(tag)
    ids[i] <- .metadataTagId(tag, meta, input$ids[i])
    records[[i]] <- structure(data.table::copy(meta), class = "nautilus_metadata")
  }
  if (input$single) return(records[[1L]])
  if (anyDuplicated(ids)) .abort("Input {.arg x} contains duplicate deployment IDs.")
  stats::setNames(records, ids)
}


#' Update descriptive metadata on tag datasets
#'
#' @description
#' Adds or corrects descriptive metadata without re-importing sensor data. Updates are restricted
#' to the animal identifier, deployment site name, passive biological traits, and user annotations.
#' Sensor values and processing history are unchanged, and caller-supplied objects are not modified
#' by reference.
#'
#' @param data A `nautilus_tag`, a legacy data.frame/data.table, a list of such objects, or a
#'   character vector of `.rds` file paths. Each dataset must describe one deployment.
#' @param updates A named list of partial metadata updates, applied to every supplied deployment,
#'   or a data.frame with one row per deployment, matched by ID. See Details.
#' @param columns A [metadataColumns()] object describing a table supplied to `updates`.
#'   Only `id`, `animal_id`, `deploy_site`, and `traits` are used. Other roles are not imported or
#'   changed. Ignored for list updates. Default `metadataColumns()`.
#' @param id.col Name of the deployment-ID column in the sensor datasets. Default `"ID"`.
#'   If absent, the stored metadata ID is used, then the list name or file basename.
#' @param return.data Logical. Return updated datasets in memory (default `TRUE`). When `FALSE`,
#'   return the written file paths invisibly; requires `output.dir`.
#' @param output.dir Directory in which to save `<id>.rds` files, or `NULL` (default) to write
#'   nothing. The directory must already exist. Source files are not overwritten unless explicitly
#'   targeted by this directory and `output.suffix`.
#' @param output.suffix Optional suffix appended to each saved filename before `.rds`.
#'   Default `NULL`.
#' @param compress Compression passed to [base::saveRDS()]: `TRUE` (default), `FALSE`,
#'   `"gzip"`, `"bzip2"`, or `"xz"`.
#' @param verbose Verbosity: `FALSE`/`0`/`"quiet"`, `TRUE`/`1`/`"normal"`, or
#'   `2`/`"detailed"` (default).
#'
#' @details
#' List updates use the metadata structure, for example
#' `list(animal_id = "A12", biometrics = list(sex = "F"))`. The editable fields are exactly
#' `animal_id`, `deployment$site`, named scalar fields within `biometrics`, and named scalar
#' fields within `user`. Nested blocks are merged field by field, not replaced. Omitted fields
#' are preserved; use a typed `NA` to mark an unknown value. `NULL` and non-scalar values are
#' not accepted. Factors are stored as character values.
#'
#' For table updates, select traits with `metadataColumns(traits = c("sex", "length_cm"))`
#' and optionally map `animal_id` and `deploy_site`. Column names within `traits` become field
#' names within `biometrics`. Deployment IDs must be non-missing, non-empty, and unique.
#' Datasets without a matching row are retained unchanged and reported in a warning. They are
#' also saved when `output.dir` is supplied.
#'
#' Deployment IDs, dates, coordinates, attachment configuration, tag identity, sensors, clocks,
#' calibration, axis mapping, ancillary streams, and processing records are protected: changing
#' them could invalidate existing calculations. Correct computational inputs upstream and rerun
#' the affected processing steps instead. This utility does not add a processing-history entry;
#' the calling analysis script records manual descriptive edits.
#'
#' @return When `return.data = TRUE`, an updated `nautilus_tag` for a single object or file,
#'   or a named list of updated tags for collections. Otherwise, a character vector of saved
#'   `.rds` paths, invisibly. Stored metadata remains a plain list.
#' @seealso [getTagMetadata()], [metadataColumns()], [importTagData()]
#' @examples
#' \dontrun{
#' tag <- updateTagMetadata(tag, list(biometrics = list(sex = "F", length_cm = 612)))
#' corrections <- data.frame(ID = c("PIN_01", "PIN_02"), sex = c("F", "M"))
#' tags <- updateTagMetadata(processed, corrections,
#'                           columns = metadataColumns(traits = "sex"))
#' # Reading a file does not overwrite it; saving requires an explicit directory.
#' tag <- updateTagMetadata("data/PIN_01.rds", list(deployment = list(site = "North reef")))
#' }
#' @export
updateTagMetadata <- function(data, updates, columns = metadataColumns(), id.col = "ID",
                              return.data = TRUE, output.dir = NULL, output.suffix = NULL,
                              compress = TRUE, verbose = "detailed") {
  start.time <- Sys.time(); lvl <- .verbosity(verbose)
  .assert_string(id.col, "id.col"); .assert_flag(return.data, "return.data")
  .assert_dir(output.dir, "output.dir"); .assert_string(output.suffix, "output.suffix", null_ok = TRUE)
  .assert_compress(compress); .assert_output(return.data, output.dir)
  input <- .metadataInput(data, "data")
  table_updates <- is.data.frame(updates)
  patches <- if (table_updates) .metadataTableUpdates(updates, columns) else list(.metadataPatch(updates))
  update_ids <- if (table_updates) names(patches) else NULL

  .log_header(lvl, "updateTagMetadata", "Updating descriptive metadata",
              bullets = sprintf("Input: %d dataset%s", input$n, if (input$n != 1L) "s" else ""))
  results <- if (return.data) vector("list", input$n) else NULL
  saved <- vector("list", input$n)
  ids <- character(input$n); unmatched <- character(0); n_updated <- 0L
  for (i in seq_len(input$n)) {
    # copy() isolates both sensor columns and nested metadata data.tables from aliases.
    tag <- .ensureMeta(data.table::copy(input$get(i)))
    meta <- .getMeta(tag)
    id <- .metadataTagId(tag, meta, input$ids[i], id.col)
    if (id %in% ids[seq_len(i - 1L)]) .abort("Input {.arg data} contains duplicate deployment ID {.val {id}}.")
    ids[i] <- id
    j <- if (table_updates) match(id, update_ids) else 1L
    if (is.na(j)) {
      unmatched <- c(unmatched, id)
    } else {
      patch <- patches[[j]]
      if ("animal_id" %in% names(patch)) meta$animal_id <- patch$animal_id
      for (block in intersect(c("deployment", "biometrics", "user"), names(patch))) {
        for (field in names(patch[[block]])) meta[[block]][[field]] <- patch[[block]][[field]]
      }
      n_updated <- n_updated + 1L
    }
    tag <- .restoreMeta(tag, meta)
    saved[i] <- list(.saveOutput(tag, id, output.dir, output.suffix, compress))
    if (return.data) results[[i]] <- tag
  }
  if (length(unmatched)) {
    cli::cli_warn("{length(unmatched)} dataset{?s} had no matching update row; retained unchanged: {.val {utils::head(unmatched, 6L)}}.")
  }
  if (lvl >= 1L) {
    .log_summary(lvl); .log_done(lvl, n_updated, " of ", input$n, " dataset", if (input$n != 1L) "s", " updated")
    .log_runtime(lvl, start.time)
  }
  if (return.data && input$single) return(results[[1L]])
  .collectOutput(results, saved, return.data, ids)
}


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
  data_ids <- if (id.col %in% names(tag)) unique(as.character(tag[[id.col]])) else character(0)
  if (length(data_ids) && (anyNA(data_ids) || any(!nzchar(trimws(data_ids))) || length(data_ids) != 1L)) {
    .abort("Each tag dataset must contain one non-missing deployment ID in {.val {id.col}}.")
  }
  meta_id <- meta$id
  if (length(meta_id) == 1L && !is.na(meta_id) && nzchar(trimws(as.character(meta_id)))) {
    meta_id <- as.character(meta_id)
    if (length(data_ids) && !identical(data_ids, meta_id)) {
      .abort("Stored metadata ID {.val {meta_id}} does not match the dataset ID {.val {data_ids}}.")
    }
    return(meta_id)
  }
  if (length(data_ids)) return(data_ids)
  .assert_string(fallback, "deployment ID")
  if (!nzchar(trimws(fallback))) .abort("Each dataset needs a non-empty deployment ID.")
  fallback
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
  if ("animal_id" %in% names(patch)) patch$animal_id <- scalar(patch$animal_id, "animal_id", text = TRUE)
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
  ids <- as.character(id_values)
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
  line("clocks", paste0("sensor ", compact(x$sensors$timezone),
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
