#' Download the camera-tag timestamp-recognition model
#'
#' @description
#' The model that reads camera-tag on-screen timestamps is about 11 MB, which is too large to bundle
#' with the package, so it is downloaded on first use and cached thereafter.
#'
#' [getVideoMetadata()] fetches it automatically the first time it actually has to read a timestamp off
#' the picture. Call this to fetch it in advance - before working offline, or before a long batch run
#' where an unexpected download would be unwelcome.
#'
#' @param quiet Whether to suppress the progress messages. Default `FALSE`.
#'
#' @details
#' The model is cached in the per-user cache directory, `tools::R_user_dir("nautilus", "cache")`, and
#' reused from there, so the download happens once per machine rather than once per session. Calling
#' this when the model is already cached does nothing but confirm it.
#'
#' @return The path to the cached model, invisibly, or `NULL` where it could not be downloaded. In that
#'   case timestamp recognition falls back to Tesseract's generic English model, which still works but
#'   is noticeably less accurate on the camera-tag timestamp font.
#'
#' @seealso [getVideoMetadata()] for what uses the model; [ocrControl()] for selecting a different one.
#'
#' @examples
#' \dontrun{
#' installCamOcrModel()
#' }
#' @export
installCamOcrModel <- function(quiet = FALSE) {
  .assert_flag(quiet, "quiet")
  p <- .camModelPath(download = TRUE, quiet = quiet)
  if (!quiet) {
    if (is.null(p)) cli::cli_alert_danger("cam-tag OCR model not installed (see the warning above).")
    else            cli::cli_alert_success("cam-tag OCR model ready at {.file {p}}.")
  }
  invisible(p)
}
