

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
