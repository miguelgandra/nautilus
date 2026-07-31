#######################################################################################################
# Resolve which tail-beat backend's column to read #####################################################
#######################################################################################################

#' Find the tail-beat column to use
#'
#' @description
#' \code{\link{calculateTailBeats}} names its estimates after the backend that produced them --
#' `tbf_hz_peaks`, `tbf_hz_wavelet`, and the matching `tbf_amplitude_*` -- so that a value's provenance
#' travels with it. This helper answers the question that naming raises: *given a table, which of those
#' columns should I read?* It is what lets a script stay backend-agnostic without giving up the
#' provenance the names carry.
#'
#' @details
#' The answer is taken from the data: whichever `tbf_<quantity>_*` columns actually carry values. If
#' exactly one backend produced this quantity, that is the answer. If several did, the package's
#' documented order (`peaks`, then `wavelet`) breaks the tie -- a reporting convention, not a claim that
#' one backend is better. Pass `method` to override it.
#'
#' Resolution deliberately ignores the object's processing metadata. That metadata does not survive the
#' operations a cohort analysis performs -- `rbind`, `data.table::rbindlist`, a CSV round trip,
#' `dplyr::mutate` and `dplyr::select` all discard it -- and pooling deployments keeps the *first*
#' deployment's record and applies it to rows produced by the other backend. Column contents survive all
#' of that, and describe each row honestly.
#'
#' Because resolution is per table, a pooled cohort whose deployments used different backends will have
#' both columns populated and will fall to the tie-break. \code{\link{summarizeTagData}} therefore
#' reports `tbf_method` alongside `tbf_mean`, so a mixed cohort is visible rather than silently blended.
#'
#' @param x A `nautilus_tag`, data.frame or data.table carrying tail-beat columns (or a character vector
#'   of column names).
#' @param quantity Which quantity to resolve: `"hz"` (default) or `"amplitude"`.
#' @param method Optionally force a backend, e.g. `"wavelet"`. Errors if that backend produced nothing,
#'   naming what is available, so a typo fails loudly rather than quietly resolving to the other backend.
#'
#' @return The column name as a string, or `NULL` if no backend produced this quantity (for example
#'   because \code{\link{calculateTailBeats}} has not been run).
#'
#' @seealso \code{\link{calculateTailBeats}}, \code{\link{summarizeTagData}}
#' @examples
#' \dontrun{
#' tags <- calculateTailBeats(processed)          # runs both backends by default
#'
#' # backend-agnostic: works whichever backend(s) were run
#' col <- tailBeatColumn(tags[[1]])
#' mean(tags[[1]][[col]], na.rm = TRUE)
#'
#' tailBeatColumn(tags[[1]])                       # "tbf_hz_peaks"   (documented tie-break)
#' tailBeatColumn(tags[[1]], method = "wavelet")   # "tbf_hz_wavelet"
#' tailBeatColumn(tags[[1]], quantity = "amplitude")
#' }
#' @export
tailBeatColumn <- function(x, quantity = c("hz", "amplitude"), method = NULL) {
  quantity <- match.arg(quantity)
  if (!is.character(x)) .assert_nonempty(x, "x")
  .tbfResolve(x, quantity = quantity, method = method)
}
