################################################################################
# Function to save processed frames for low-confidence timestamp extractions ###
################################################################################

#' Save processed frames for low-confidence timestamp extractions
#'
#' This function takes the output from getVideoMetadata and saves processed frames
#' for entries with confidence scores below a specified threshold.
#'
#' @param video.metadata Data frame output from getVideoMetadata
#' @param confidence.threshold Numeric. Confidence threshold below which frames are saved
#' @param output.dir Character. Directory to save frames and metadata
#' @param tesseract.model Character. OCR language/model to use
#' @param overwrite Logical. Whether to overwrite existing files
#'
#' @return Updated video.metadata with saved_frame_path column
#' @export

saveLowConfidenceFrames <- function(video.metadata,
                                    confidence.threshold = 0.7,
                                    output.dir = "low_confidence_frames",
                                    tesseract.model = "cam",
                                    overwrite = FALSE) {

  # Create output directory
  if(!dir.exists(output.dir)) {
    dir.create(output.dir, recursive = TRUE)
  }

  # Initialize saved_frame_path column
  video.metadata$saved_frame_path <- NA
  video.metadata$saved_original_path <- NA
  video.metadata$saved_metadata_path <- NA

  # Filter low-confidence entries
  low_confidence_rows <- which(video.metadata$validation_confidence < confidence.threshold)

  if(length(low_confidence_rows) == 0) {
    cat("No low-confidence entries found (threshold =", confidence.threshold, ")\n")
    return(video.metadata)
  }

  cat(sprintf("Saving frames for %d low-confidence entries...\n", length(low_confidence_rows)))

  # Setup OCR engine
  month_chars <- sort(unique(unlist(strsplit(paste(month.abb, collapse = ""), ""))))
  allowed_chars <- c(0:9, month_chars, ":", ".", " ")
  whitelist <- paste(allowed_chars, collapse = "")

  ocr_engine <- tesseract::tesseract(
    language = tesseract.model,
    options = list(
      tessedit_pageseg_mode = 7,
      tessedit_char_whitelist = whitelist
    )
  )

  # Process each low-confidence entry
  pb <- txtProgressBar(min = 0, max = length(low_confidence_rows), style = 3)

  for(i in seq_along(low_confidence_rows)) {
    row_idx <- low_confidence_rows[i]
    row_data <- video.metadata[row_idx, ]

    # Extract frame and save
    frame_result <- .extractAndProcessFrameForReview(
      video = row_data$file,
      id = row_data$ID,
      time_offset = 0,  # Always use start of video
      ocr_engine = ocr_engine,
      output.dir = output.dir,
      video.metadata = row_data,
      overwrite = overwrite
    )

    # Update metadata with saved paths
    video.metadata$saved_frame_path[row_idx] <- frame_result$processed_path
    video.metadata$saved_original_path[row_idx] <- frame_result$original_path
    video.metadata$saved_metadata_path[row_idx] <- frame_result$metadata_path

    setTxtProgressBar(pb, i)
  }

  close(pb)

  # Create summary report
  summary_path <- file.path(output.dir, "low_confidence_summary.csv")
  summary_data <- video.metadata[low_confidence_rows, c("ID", "video", "validation_confidence",
                                                        "start", "saved_frame_path")]
  write.csv(summary_data, summary_path, row.names = FALSE)

  cat(sprintf("\nSaved %d frames to: %s\n", length(low_confidence_rows), output.dir))
  cat(sprintf("Summary report: %s\n", summary_path))

  return(video.metadata)
}



#' Helper function to extract and save frame for review
#' @keywords internal
#' @noRd
.extractAndProcessFrameForReview <- function(video, id, time_offset, ocr_engine,
                                             output.dir, video.metadata, overwrite = FALSE) {

  # Create descriptive filenames
  video_name <- tools::file_path_sans_ext(basename(video))
  confidence_str <- sprintf("conf%.2f", video.metadata$validation_confidence)

  processed_filename <- sprintf("%s_%s_%s_processed.jpg", id, video_name, confidence_str)
  original_filename <- sprintf("%s_%s_%s_original.jpg", id, video_name, confidence_str)
  metadata_filename <- sprintf("%s_%s_%s_metadata.txt", id, video_name, confidence_str)

  processed_path <- file.path(output.dir, processed_filename)
  original_path <- file.path(output.dir, original_filename)
  metadata_path <- file.path(output.dir, metadata_filename)

  # Check if files exist and overwrite is FALSE
  if(!overwrite && file.exists(processed_path)) {
    return(list(
      processed_path = processed_path,
      original_path = original_path,
      metadata_path = metadata_path
    ))
  }

  # Extract and process frame (similar to main function)
  frame_id <- paste0(id, "-", gsub("\\.", "_", as.character(time_offset)))
  temp_frame_path <- file.path(tempdir(), sprintf("%s-frame.jpg", frame_id))

  # Extract frame
  system(sprintf('ffmpeg -y -i "%s" -ss %.3f -vframes 1 -q:v 1 -vf "scale=-1:2160" "%s"',
                 video, time_offset, temp_frame_path),
         ignore.stdout = TRUE, ignore.stderr = TRUE)

  # Process frame
  input_frame <- magick::image_read(temp_frame_path)
  cropped_image <- magick::image_crop(input_frame, geometry = "325x28+3249+2120")

  # Save original crop
  magick::image_write(cropped_image, path = original_path, format = "jpg", quality = 95)

  # Apply processing pipeline
  processed_image <- magick::image_convert(cropped_image, colorspace = "gray")
  processed_image <- magick::image_negate(processed_image)
  processed_image <- magick::image_contrast(processed_image, sharpen = 1)
  processed_image <- magick::image_resize(processed_image, geometry = "300%")
  processed_image <- magick::image_morphology(processed_image, method = "Close", kernel = "Diamond", iterations = 1)
  processed_image <- magick::image_morphology(processed_image, method = "Open", kernel = "Disk", iterations = 1)
  processed_image <- magick::image_threshold(processed_image, type = "white", threshold = "70%")
  processed_image <- magick::image_threshold(processed_image, type = "black", threshold = "60%")

  # Save processed frame
  magick::image_write(processed_image, path = processed_path, format = "jpg", quality = 95)

  # Perform OCR for metadata
  invisible(capture.output({
    ocr_text <- tesseract::ocr(processed_image, engine = ocr_engine)
  }))

  # Create metadata file
  metadata_content <- paste0(
    "=== LOW CONFIDENCE FRAME REVIEW ===\n",
    "Video: ", basename(video), "\n",
    "ID: ", id, "\n",
    "Confidence: ", sprintf("%.3f", video.metadata$validation_confidence), "\n",
    "Parsed timestamp: ", ifelse(is.na(video.metadata$start), "FAILED", as.character(video.metadata$start)), "\n",
    "Time offset: ", time_offset, " seconds\n",
    "Raw OCR text: '", ocr_text, "'\n",
    "Frames validated: ", video.metadata$frames_validated, "\n",
    "Gap corrected: ", video.metadata$gap_corrected, "\n",
    "Generated: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n",
    "=== INSTRUCTIONS FOR MANUAL REVIEW ===\n",
    "1. Open the original frame to see the raw timestamp\n",
    "2. Check the processed frame to see what OCR 'saw'\n",
    "3. If timestamp is incorrect, note the correct value\n",
    "4. Common issues: 0/O confusion, I/1 confusion, poor contrast\n"
  )

  writeLines(metadata_content, metadata_path)

  # Cleanup
  unlink(temp_frame_path)

  return(list(
    processed_path = processed_path,
    original_path = original_path,
    metadata_path = metadata_path
  ))
}
