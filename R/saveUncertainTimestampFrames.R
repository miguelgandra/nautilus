################################################################################
# Save frames for videos with uncertain timestamps (manual review) ############
################################################################################

#' Save frames for videos with uncertain timestamps, for manual review
#'
#' @description
#' Takes the output of \code{\link{getVideoMetadata}} and, for every video whose recording start time is
#' *uncertain*, extracts and saves the on-screen timestamp region (both the raw crop and the processed,
#' OCR-ready image) plus a small text report, so the timestamp can be checked by eye. A video is treated
#' as uncertain when its start time:
#' \itemize{
#'   \item came from OCR rather than the file name (`timestamp_source == "ocr"`) - OCR is a fallback and
#'     more error-prone than a file-name timestamp;
#'   \item could not be obtained at all (`timestamp_source` is `NA`); or
#'   \item disagrees with the OCR cross-check (`ocr_flag == TRUE`, when `getVideoMetadata` was run with
#'     `cross.check = TRUE`).
#' }
#'
#' @param video.metadata Data frame returned by \code{\link{getVideoMetadata}} (must contain `ID`,
#'   `video`, `file`, `start` and `timestamp_source`).
#' @param output.dir Character. Directory to save frames and reports. Created if needed.
#' @param ocr An \code{\link{ocrControl}} object (or a named list of its fields) giving the OCR model and
#'   the on-screen timestamp box geometry used to crop and re-read the frames. Defaults to `ocrControl()`.
#' @param overwrite Logical. Whether to overwrite existing saved frames. Defaults to `FALSE`.
#' @param verbose Logical. Whether to print progress. Defaults to `TRUE`.
#'
#' @return `video.metadata` with three added columns - `saved_frame_path`, `saved_original_path` and
#'   `saved_metadata_path` - populated for the uncertain rows (and `NA` elsewhere).
#'
#' @seealso \code{\link{getVideoMetadata}}, \code{\link{ocrControl}}
#'
#' @examples
#' \dontrun{
#' meta <- getVideoMetadata("./videos/PIN_CAM_01", cross.check = TRUE)
#' # save the on-screen timestamp crops for any uncertain videos, to check by eye
#' meta <- saveUncertainTimestampFrames(meta, output.dir = "./review/frames")
#' }
#'
#' @export

saveUncertainTimestampFrames <- function(video.metadata,
                                         output.dir = "uncertain_timestamp_frames",
                                         ocr = ocrControl(),
                                         overwrite = FALSE,
                                         verbose = TRUE) {

  # validate inputs
  if (!is.data.frame(video.metadata)) .abort("{.arg video.metadata} must be a data frame from {.fn getVideoMetadata}.")
  .assert_columns(video.metadata, c("ID", "video", "file", "start", "timestamp_source"), "video.metadata")
  .assert_flag(overwrite, "overwrite"); .assert_flag(verbose, "verbose")
  ocr <- .as_control(ocr, ocrControl, "nautilus_ocr", "ocr")
  for (pkg in c("tesseract", "magick"))
    if (!requireNamespace(pkg, quietly = TRUE)) .abort("The {.pkg {pkg}} package is required: {.code install.packages('{pkg}')}.")
  ffmpeg_bin <- .ffmpegBin()

  # add output columns
  video.metadata$saved_frame_path    <- NA_character_
  video.metadata$saved_original_path <- NA_character_
  video.metadata$saved_metadata_path <- NA_character_

  # classify uncertain rows + the reason (OCR-sourced, no timestamp, or cross-check discrepancy)
  src    <- video.metadata$timestamp_source
  reason <- rep(NA_character_, nrow(video.metadata))
  reason[!is.na(src) & src == "ocr"] <- "OCR-sourced"
  reason[is.na(src)]                 <- "no timestamp"
  if (!is.null(video.metadata$ocr_flag))
    reason[!is.na(video.metadata$ocr_flag) & video.metadata$ocr_flag] <- "cross-check discrepancy"
  rows <- which(!is.na(reason))

  if (!length(rows)) {
    if (verbose) cli::cli_alert_success("No uncertain timestamps found - nothing to save.")
    return(video.metadata)
  }

  if (!dir.exists(output.dir)) dir.create(output.dir, recursive = TRUE)

  # OCR engine (to record what OCR reads for each saved frame)
  prepared   <- .prepareOcrModel(ocr$model, if (verbose) 1L else 0L)
  whitelist  <- if (!is.null(ocr$char.whitelist)) ocr$char.whitelist else prepared$whitelist
  ocr_engine <- tesseract::tesseract(language = prepared$model,
    options = list(tessedit_pageseg_mode = 7, tessedit_char_whitelist = whitelist))

  if (verbose) cli::cli_alert_info("Saving frames for {length(rows)} uncertain timestamp{?s} to {.file {output.dir}}.")
  pb_id <- if (verbose) cli::cli_progress_bar("Saving frames", total = length(rows), clear = TRUE) else NULL

  for (i in seq_along(rows)) {
    row_idx <- rows[i]
    saved <- .saveReviewFrame(video.metadata[row_idx, , drop = FALSE], reason[row_idx],
                              ocr, ocr_engine, output.dir, ffmpeg_bin, overwrite)
    video.metadata$saved_frame_path[row_idx]    <- saved$processed_path
    video.metadata$saved_original_path[row_idx] <- saved$original_path
    video.metadata$saved_metadata_path[row_idx] <- saved$metadata_path
    if (!is.null(pb_id)) cli::cli_progress_update(id = pb_id, set = i)
  }
  if (!is.null(pb_id)) cli::cli_progress_done(id = pb_id)

  # summary report
  summary_cols <- c("ID", "video", "timestamp_source", "start", "saved_frame_path")
  if (!is.null(video.metadata$ocr_offset_s)) summary_cols <- append(summary_cols, "ocr_offset_s", after = 3)
  summary_data <- data.frame(reason = reason[rows], video.metadata[rows, summary_cols], stringsAsFactors = FALSE)
  summary_path <- file.path(output.dir, "uncertain_timestamps_summary.csv")
  utils::write.csv(summary_data, summary_path, row.names = FALSE)

  if (verbose) {
    cli::cli_alert_success("Saved {length(rows)} frame{?s} to {.file {output.dir}}.")
    cli::cli_alert_info("Summary report: {.file {summary_path}}.")
  }

  video.metadata
}


#' Extract, save and OCR one on-screen timestamp frame for manual review
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.saveReviewFrame <- function(row, reason, ocr, ocr_engine, output.dir, ffmpeg_bin, overwrite = FALSE) {

  video      <- row$file
  id         <- row$ID
  video_name <- tools::file_path_sans_ext(basename(video))
  tag        <- gsub("[^A-Za-z0-9]+", "-", reason)

  processed_path <- file.path(output.dir, sprintf("%s_%s_%s_processed.jpg", id, video_name, tag))
  original_path  <- file.path(output.dir, sprintf("%s_%s_%s_original.jpg",  id, video_name, tag))
  metadata_path  <- file.path(output.dir, sprintf("%s_%s_%s_report.txt",    id, video_name, tag))

  # honour overwrite = FALSE (skip work if the processed frame already exists)
  if (!overwrite && file.exists(processed_path))
    return(list(processed_path = processed_path, original_path = original_path, metadata_path = metadata_path))

  # extract the first frame (normalised to the reference height so the box coordinates apply)
  temp_frame_path <- file.path(tempdir(), sprintf("%s-review-frame.jpg", id))
  system2(ffmpeg_bin, c("-y", "-ss", "0", "-i", shQuote(video), "-vframes", "1", "-q:v", "1",
                        "-vf", sprintf("scale=-1:%d", ocr$frame.height), shQuote(temp_frame_path)),
          stdout = FALSE, stderr = FALSE)
  if (!file.exists(temp_frame_path))
    return(list(processed_path = NA_character_, original_path = NA_character_, metadata_path = NA_character_))

  # crop the configured timestamp box; save the raw crop
  input_frame   <- magick::image_read(temp_frame_path)
  crop_geometry <- sprintf("%dx%d+%d+%d", ocr$box[3], ocr$box[4], ocr$box[1], ocr$box[2])
  cropped_image <- magick::image_crop(input_frame, geometry = crop_geometry)
  magick::image_write(cropped_image, path = original_path, format = "jpg", quality = 95)

  # same preprocessing pipeline as getVideoMetadata(); save the processed image
  processed_image <- magick::image_convert(cropped_image, colorspace = "gray")
  processed_image <- magick::image_negate(processed_image)
  processed_image <- magick::image_contrast(processed_image, sharpen = 1)
  processed_image <- magick::image_resize(processed_image, geometry = "300%")
  processed_image <- magick::image_morphology(processed_image, method = "Close", kernel = "Diamond", iterations = 1)
  processed_image <- magick::image_morphology(processed_image, method = "Open", kernel = "Disk", iterations = 1)
  processed_image <- magick::image_threshold(processed_image, type = "white", threshold = "70%")
  processed_image <- magick::image_threshold(processed_image, type = "black", threshold = "60%")
  magick::image_write(processed_image, path = processed_path, format = "jpg", quality = 95)

  # what OCR reads from this frame (recorded in the report)
  ocr_text <- invisible(tesseract::ocr(processed_image, engine = ocr_engine))

  offset_line <- if (!is.null(row$ocr_offset_s) && !is.na(row$ocr_offset_s))
    sprintf("OCR offset (start - OCR): %.1f s\n", row$ocr_offset_s) else ""

  metadata_content <- paste0(
    "=== UNCERTAIN TIMESTAMP - MANUAL REVIEW ===\n",
    "Video: ", basename(video), "\n",
    "ID: ", id, "\n",
    "Reason flagged: ", reason, "\n",
    "Timestamp source: ", ifelse(is.na(row$timestamp_source), "none", row$timestamp_source), "\n",
    "Recorded start: ", ifelse(is.na(row$start), "FAILED", as.character(row$start)), "\n",
    offset_line,
    "Raw OCR text: '", trimws(ocr_text), "'\n",
    "Generated: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n",
    "=== HOW TO REVIEW ===\n",
    "1. Open the *_original.jpg crop to read the on-screen timestamp directly.\n",
    "2. Compare it with the *_processed.jpg to see what OCR 'saw'.\n",
    "3. If the recorded start is wrong, note the correct value.\n",
    "   Common OCR issues: 0/O, 1/I, 5/S confusion, poor contrast.\n")
  writeLines(metadata_content, metadata_path)

  unlink(temp_frame_path)
  list(processed_path = processed_path, original_path = original_path, metadata_path = metadata_path)
}
