#' Inspect the problems found in deployment metadata
#'
#' @description
#' [checkDeploymentMetadata()] reports rather than stops, so its verdict travels with the returned
#' object instead of interrupting your session. `issues()` opens that verdict: one row per flagged
#' record, saying which deployment, which check, and what is wrong.
#'
#' Use it to triage a table before importing - filter to `severity = "error"` to see what would block
#' the import, and to `"warning"` for what is merely suspect.
#'
#' @param x A `nautilus_deployments` object returned by [checkDeploymentMetadata()].
#' @param severity Optional character vector to filter by severity (`"error"`, `"warning"`, `"info"`).
#'   Default `NULL` (all).
#' @return A data.frame with columns \code{id}, \code{check}, \code{severity}, \code{message} - one row
#'   per flagged record. Zero rows when the metadata is clean.
#' @seealso \code{\link{checkDeploymentMetadata}}
#' @examples
#' \dontrun{
#' issues(meta)
#' issues(meta, severity = "error")
#' }
#' @export
issues <- function(x, severity = NULL) {
  if (!is_nautilus_deployments(x))
    .abort("{.arg x} must be a {.cls nautilus_deployments} object from {.fn checkDeploymentMetadata}.")
  iss <- attr(x, "nautilus.qc.issues") %||% .empty_issues()
  if (!is.null(severity)) iss <- iss[iss$severity %in% severity, , drop = FALSE]
  rownames(iss) <- NULL
  iss
}
