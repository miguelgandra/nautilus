#######################################################################################################
# Public metadata accessors ###########################################################################
#######################################################################################################

#' Retrieve the processing history of a nautilus tag object
#'
#' Each nautilus pipeline function appends a record to the object's processing audit trail. This
#' accessor returns that trail as a tidy data.frame, one row per processing step, in the order the
#' steps were applied.
#'
#' @param x A single \code{nautilus_tag}, a legacy data.frame/data.table, or an \code{.rds} file path.
#' @return A data.frame with columns \code{step} (function name), \code{time} (when it ran),
#'   \code{nautilus_version}, and \code{details} (a compact summary of the parameters recorded for
#'   that step). Returns a zero-row data.frame if no history is present.
#' @seealso \code{\link{getTagMetadata}} for the full metadata record.
#' @examples
#' \dontrun{
#' processingHistory(tag)
#' }
#' @export

processingHistory <- function(x) {
  meta <- getTagMetadata(x)
  if (!inherits(meta, "nautilus_metadata")) .abort("{.arg x} must be a single tag dataset or .rds file path.")
  proc <- meta$processing %||% list()

  empty <- data.frame(step = character(0), time = as.POSIXct(character(0)),
                      nautilus_version = character(0), details = character(0),
                      stringsAsFactors = FALSE)
  if (!length(proc)) return(empty)

  # compactly format a single recorded value (summarise long vectors by count)
  fmt1 <- function(v) {
    if (length(v) > 3) return(sprintf("<%d values>", length(v)))
    paste(format(v), collapse = ", ")
  }

  rows <- lapply(proc, function(p) {
    extra <- p[setdiff(names(p), c("step", "time", "nautilus_version"))]
    details <- if (length(extra)) {
      paste(names(extra), vapply(extra, fmt1, character(1)), sep = " = ", collapse = "; ")
    } else ""
    data.frame(step = p$step %||% NA_character_,
               time = if (is.null(p$time)) as.POSIXct(NA) else p$time,
               nautilus_version = p$nautilus_version %||% NA_character_,
               details = details,
               stringsAsFactors = FALSE)
  })
  do.call(rbind, rows)
}
