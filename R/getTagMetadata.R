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
