#######################################################################################################
# Resolve which tail-beat backend's column to read #####################################################
#######################################################################################################

#' Find the tail-beat column to read
#'
#' @description
#' [calculateTailBeats()] names its estimates after the backend that produced them - `tbf_hz_peaks`,
#' `tbf_hz_wavelet`, and the matching `tbf_amplitude_*` columns - so that a value's provenance always
#' travels with it. That naming raises an obvious question for anyone writing analysis code: given a
#' table, which of those columns should I read?
#'
#' This helper answers it, so a script can stay backend-agnostic without giving up the provenance the
#' names carry.
#'
#' @param x A tag object, data frame or data table carrying tail-beat columns, or simply a character
#'   vector of column names.
#' @param quantity Which quantity to resolve: `"hz"` (default) or `"amplitude"`.
#' @param method Force a particular backend, for example `"wavelet"`. It is an error if that backend
#'   produced nothing, and the message names what is available - so a typo fails loudly rather than
#'   quietly resolving to the other backend.
#'
#' @details
#' The answer is taken from the data itself: whichever `tbf_<quantity>_*` columns actually carry values.
#' If exactly one backend produced this quantity, that is the answer. If several did, the documented
#' order - peaks, then wavelet - breaks the tie. That order is a reporting convention, not a claim that
#' one backend is better than the other; pass `method` to override it.
#'
#' Resolution deliberately ignores the object's processing metadata, which does not survive the
#' operations a cohort analysis performs. Binding tables together, a round trip through CSV, and several
#' common data-manipulation verbs all discard it, and pooling deployments keeps the *first* deployment's
#' record and then applies it to rows produced by the other backend. Column contents survive all of that,
#' and describe each row honestly.
#'
#' Because resolution happens per table, a pooled cohort whose deployments used different backends will
#' have both columns populated and will fall to the tie-break. [summarizeTagData()] therefore reports
#' `tbf_method` alongside `tbf_mean`, so a mixed cohort is visible rather than silently blended.
#'
#' @return The column name, as a string, or `NULL` where no backend produced this quantity - for
#'   instance because [calculateTailBeats()] has not been run.
#'
#' @seealso [calculateTailBeats()] for producing the columns; [summarizeTagData()] for the cohort
#'   summary that reports which backend each deployment used.
#'
#' @examples
#' \dontrun{
#' tags <- calculateTailBeats(processed)          # runs both backends by default
#'
#' # backend-agnostic: works whichever backend or backends were run
#' col <- tailBeatColumn(tags[[1]])
#' mean(tags[[1]][[col]], na.rm = TRUE)
#'
#' tailBeatColumn(tags[[1]])                       # "tbf_hz_peaks", by the documented tie-break
#' tailBeatColumn(tags[[1]], method = "wavelet")   # "tbf_hz_wavelet"
#' tailBeatColumn(tags[[1]], quantity = "amplitude")
#' }
#' @export
tailBeatColumn <- function(x, quantity = c("hz", "amplitude"), method = NULL) {
  quantity <- match.arg(quantity)
  if (!is.character(x)) .assert_nonempty(x, "x")
  .tbfResolve(x, quantity = quantity, method = method)
}
