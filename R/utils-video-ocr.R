################################################################################
# OCR model preparation ######################################################
################################################################################

# The fine-tuned cam-tag OCR model is ~11 MB, so it is NOT bundled with the package (CRAN keeps packages
# small); it is hosted as a GitHub release asset and fetched on demand into the per-user cache. Update all
# three constants together if the model is re-trained (md5 is the base-R integrity check; the sha256 for
# reference is 4dbab93bf5602352d037c7c0454278abe40a6a0fa47f242c2dca9c1e00e852d3).
.CAM_MODEL_URL   <- "https://github.com/miguelgandra/nautilus/releases/download/ocr-model-v1/cam.traineddata"


.CAM_MODEL_MD5   <- "139317158031f17b0ab26536cd99b1c9"


.CAM_MODEL_BYTES <- 11697722L


#' Resolve the cached cam-tag OCR model, downloading it on first use.
#'
#' Looks for a verified copy in the per-user cache (\code{tools::R_user_dir("nautilus", "cache")}); if it is
#' absent or corrupt and \code{download = TRUE}, fetches it from the GitHub release, verifies its md5, and
#' caches it for reuse.
#' @param download Fetch the model when the cache is empty/stale (FALSE = only report an existing cache hit).
#' @param quiet Suppress the progress/status messages.
#' @return Path to a verified `cam.traineddata` (the cached copy, or a session temp file if the cache is not
#'   writable), or NULL when it is absent and cannot be fetched.
#' @keywords internal
#' @noRd
.camModelPath <- function(download = TRUE, quiet = FALSE) {
  ok        <- function(p) is.character(p) && length(p) == 1L && file.exists(p) &&
                           identical(unname(tools::md5sum(p)), .CAM_MODEL_MD5)
  cache_dir <- tools::R_user_dir("nautilus", "cache")
  dest      <- file.path(cache_dir, "cam.traineddata")
  if (ok(dest)) return(dest)                                  # verified cache hit
  if (!download) return(NULL)
  if (!quiet) cli::cli_alert_info(
    "Downloading the cam-tag OCR model (~{round(.CAM_MODEL_BYTES / 1e6)} MB) from the {.pkg nautilus} GitHub release ...")
  tmp <- tempfile(fileext = ".traineddata")
  got <- tryCatch(suppressWarnings(utils::download.file(.CAM_MODEL_URL, tmp, mode = "wb", quiet = TRUE)) == 0L,
                  error = function(e) FALSE)
  if (!isTRUE(got) || !ok(tmp)) {
    unlink(tmp)
    if (!quiet) cli::cli_alert_warning(
      "Could not download or verify the cam-tag OCR model (offline, or the download was corrupt).")
    return(NULL)
  }
  cached <- tryCatch({                                        # cache for next time
    if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
    isTRUE(file.copy(tmp, dest, overwrite = TRUE)) && ok(dest)
  }, error = function(e) FALSE)
  if (isTRUE(cached)) { unlink(tmp); return(dest) }
  tmp                                                         # cache not writable -> valid for this session
}


#' Ensure the OCR model is available and build the default character whitelist
#'
#' The fine-tuned cam-tag OCR model is fetched on demand (\code{.camModelPath}, exposed to users as
#' [installCamOcrModel()]) and installed into the tesseract data directory automatically (no
#' interactive prompt - batch-safe). If that is not possible (offline, or the directory is not writable),
#' fall back to the generic `eng` model. Returns the (possibly adjusted) model name and the default
#' whitelist for the DDMmmYY on-screen timestamp format.
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.prepareOcrModel <- function(model, lvl) {

  # default whitelist for the "DDMmmYY HH:MM:SS" on-screen format: digits, month-abbreviation letters,
  # and the ":", "." and " " separators
  month_chars <- sort(unique(unlist(strsplit(paste(month.abb, collapse = ""), ""))))
  whitelist_default <- paste(c(0:9, month_chars, ":", ".", " "), collapse = "")

  if (identical(model, "cam")) {
    tesseract_path <- tesseract::tesseract_info()$datapath
    cam_data_path  <- file.path(tesseract_path, "cam.traineddata")

    if (!file.exists(cam_data_path)) {
      src <- .camModelPath(download = TRUE, quiet = lvl < 1L)     # cached or freshly downloaded model, or NULL
      installed <- !is.null(src) &&
        isTRUE(tryCatch(file.copy(src, cam_data_path, overwrite = TRUE), error = function(e) FALSE)) &&
        file.exists(cam_data_path)
      if (installed) {
        if (lvl >= 1L) cli::cli_alert_info("Installed the fine-tuned {.file cam.traineddata} OCR model.")
      } else {
        if (lvl >= 1L) cli::cli_alert_warning("Could not obtain {.file cam.traineddata} - using the {.val eng} model (less accurate for cam-tag timestamps).")
        model <- "eng"
      }
    }
  }

  list(model = model, whitelist = whitelist_default)
}


################################################################################
# Parallel-safe single-video analysis ########################################
################################################################################

#' Analyse a single video: duration, frame rate and start time (file name and/or OCR)
#'
#' Reads duration and frame rate with `ffprobe`, then resolves the recording start time according to
#' `timestamp.source`: from the pre-parsed file-name time `fn_start`, and/or by OCR of the on-screen
#' display. OCR is invoked only when required (no file-name time, or a cross-check), and the Tesseract
#' engine is built lazily - so a pure file-name deployment does no image work at all. Safe for parallel
#' workers (the engine is created inside the worker when needed).
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.analyseVideo <- function(video, id, fn_start, timestamp.source, cross.check,
                          ocr, ocr_model, whitelist, ocr_engine = NULL, ffmpeg_bin, ffprobe_bin) {

  # --- duration + frame rate (ffprobe); guard empty output (corrupt / non-video files) ---
  duration <- suppressWarnings(as.numeric(system2(ffprobe_bin,
    c("-i", shQuote(video), "-show_entries", "format=duration", "-v", "quiet", "-of", "csv=p=0"),
    stdout = TRUE, stderr = FALSE)))
  if (length(duration) != 1) duration <- NA_real_

  fr_raw <- system2(ffprobe_bin,
    c("-i", shQuote(video), "-select_streams", "v:0", "-show_entries", "stream=r_frame_rate", "-v", "quiet", "-of", "csv=p=0"),
    stdout = TRUE, stderr = FALSE)
  rate_parts <- suppressWarnings(as.numeric(strsplit(paste0(fr_raw, collapse = ""), "/")[[1]]))
  frame_rate <- if (length(rate_parts) == 2 && !is.na(rate_parts[2]) && rate_parts[2] != 0) rate_parts[1] / rate_parts[2] else NA_real_

  # --- lazy OCR: build the engine once (per worker) and read the on-screen start time only when asked ---
  engine_holder <- ocr_engine
  ocr_start_time <- function() {
    if (is.null(engine_holder))
      engine_holder <<- tesseract::tesseract(language = ocr_model,
        options = list(tessedit_pageseg_mode = 7, tessedit_char_whitelist = whitelist))
    .extractAndProcessFrame(video, id, 0, engine_holder, ocr, frame_rate = frame_rate,
                            ffmpeg_bin = ffmpeg_bin, ffprobe_bin = ffprobe_bin)$timestamp
  }

  # --- resolve start time + provenance ---
  have_fn   <- !is.na(fn_start)
  start     <- as.POSIXct(NA, tz = "UTC")
  source    <- NA_character_
  ocr_start <- as.POSIXct(NA, tz = "UTC")

  if (timestamp.source == "filename") {
    if (have_fn) { start <- fn_start; source <- "filename" }
  } else if (timestamp.source == "ocr") {
    ocr_start <- ocr_start_time()
    if (!is.na(ocr_start)) { start <- ocr_start; source <- "ocr" }
  } else {                                   # auto: file name first, OCR only if the name has no time
    if (have_fn) {
      start <- fn_start; source <- "filename"
    } else {
      ocr_start <- ocr_start_time()
      if (!is.na(ocr_start)) { start <- ocr_start; source <- "ocr" }
    }
  }

  # --- optional OCR cross-check (only meaningful when start came from the file name) ---
  ocr_offset_s <- NA_real_; ocr_flag <- NA
  if (cross.check && identical(source, "filename")) {
    if (is.na(ocr_start)) ocr_start <- ocr_start_time()
    if (!is.na(ocr_start)) {
      ocr_offset_s <- as.numeric(difftime(start, ocr_start, units = "secs"))
      ocr_flag <- abs(ocr_offset_s) > 2
    }
  }

  video_end <- if (!is.na(start) && !is.na(duration)) start + duration else as.POSIXct(NA, tz = "UTC")

  out <- data.frame(
    ID = id, video = basename(video), start = start, end = video_end,
    duration = duration, frame_rate = frame_rate, file = video,
    timestamp_source = source, stringsAsFactors = FALSE)

  if (cross.check) {
    out$ocr_start    <- ocr_start
    out$ocr_offset_s <- ocr_offset_s
    out$ocr_flag     <- ocr_flag
  }
  out
}


################################################################################
# Extract and OCR a single frame, with a blank-frame search fallback ##########
################################################################################

#' Extract and process a video frame to retrieve the embedded on-screen timestamp
#'
#' Attempts to extract a frame at a given time offset and read a timestamp by OCR. If the first frame has
#' no recognisable timestamp (partially blank, glitched, or missing overlay), it searches subsequent
#' frames (up to `ocr$max.search.frames`) and, on success, back-calculates the value to the requested
#' offset using the frame rate.
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.extractAndProcessFrame <- function(video, id, time_offset, ocr_engine, ocr, frame_rate = NULL,
                                    ffmpeg_bin, ffprobe_bin) {

  # process the requested frame first
  result <- .processSingleFrame(time_offset, video, id, ocr_engine, ocr, ffmpeg_bin = ffmpeg_bin)

  # if no timestamp was retrieved, re-try after locating the box within a search radius (overlay drift)
  if (is.na(result$timestamp)) {
    frame_id   <- paste0(id, "-", gsub("\\.", "_", as.character(time_offset)))
    frame_path <- file.path(tempdir(), sprintf("%s-frame.jpg", frame_id))
    system2(ffmpeg_bin, c("-y", "-ss", sprintf("%.3f", time_offset), "-i", shQuote(video),
                          "-vframes", "1", "-q:v", "1", "-vf", sprintf("scale=-1:%d", ocr$frame.height), shQuote(frame_path)),
            stdout = FALSE, stderr = FALSE)
    if (file.exists(frame_path)) {
      input_frame     <- magick::image_read(frame_path)
      detected_coords <- .detectTimestampBox(input_frame,
                                             expected_x = ocr$box[1], expected_y = ocr$box[2],
                                             expected_width = ocr$box[3], expected_height = ocr$box[4],
                                             search_radius = ocr$search.radius)
      result <- .processSingleFrame(time_offset, video, id, ocr_engine, ocr, box.coords = detected_coords, ffmpeg_bin = ffmpeg_bin)
      unlink(frame_path)
    }
  }

  # if we found a timestamp, or this is not the first frame, return the result
  if (!is.na(result$timestamp) || time_offset != 0)
    return(list(timestamp = result$timestamp, raw_ocr = result$raw_ocr))

  # first frame with no timestamp: search subsequent frames, correcting a hit back to the start time
  if (is.null(frame_rate) || is.na(frame_rate)) {
    fr_raw <- system2(ffprobe_bin,
      c("-i", shQuote(video), "-select_streams", "v:0", "-show_entries", "stream=r_frame_rate", "-v", "quiet", "-of", "csv=p=0"),
      stdout = TRUE, stderr = FALSE)
    rate_parts <- suppressWarnings(as.numeric(strsplit(paste0(fr_raw, collapse = ""), "/")[[1]]))
    frame_rate <- if (length(rate_parts) == 2 && !is.na(rate_parts[2]) && rate_parts[2] != 0) rate_parts[1] / rate_parts[2] else NA_real_
  }
  if (is.na(frame_rate) || frame_rate <= 0) return(list(timestamp = NA, raw_ocr = NA))

  time_per_frame <- 1 / frame_rate
  for (frame_num in seq_len(ocr$max.search.frames)) {
    search_time_offset <- frame_num * time_per_frame
    search_result <- .processSingleFrame(search_time_offset, video, id, ocr_engine, ocr, ffmpeg_bin = ffmpeg_bin)
    if (!is.na(search_result$timestamp))
      return(list(timestamp = search_result$timestamp - search_time_offset, raw_ocr = search_result$raw_ocr))
  }
  list(timestamp = NA, raw_ocr = NA)
}


################################################################################
# Extract + preprocess + OCR one frame at a given time ########################
################################################################################

#' Extract a single frame and OCR the on-screen timestamp box
#'
#' Extracts a frame at a given time offset with `ffmpeg`, crops the configured timestamp box, applies a
#' preprocessing pipeline to improve recognition, and OCRs it. If the cropped box has no dark pixels
#' (usually indicating no text) OCR is skipped to save time.
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.processSingleFrame <- function(current_time_offset, video, id, ocr_engine, ocr, box.coords = NULL, ffmpeg_bin) {

  # extract frame at the requested offset (fast seek), normalised to the reference frame height so the
  # box coordinates always apply
  frame_id         <- paste0(id, "-", gsub("\\.", "_", as.character(current_time_offset)))
  first_frame_path <- file.path(tempdir(), sprintf("%s-frame.jpg", frame_id))
  system2(ffmpeg_bin, c("-y", "-ss", sprintf("%.3f", current_time_offset), "-i", shQuote(video),
                        "-vframes", "1", "-q:v", "1", "-vf", sprintf("scale=-1:%d", ocr$frame.height), shQuote(first_frame_path)),
          stdout = FALSE, stderr = FALSE)

  if (!file.exists(first_frame_path))
    return(list(timestamp = NA, raw_ocr = NA, contains_text = FALSE))

  input_frame <- magick::image_read(first_frame_path)

  # timestamp box: the configured location, unless caller passed detected coordinates
  if (is.null(box.coords))
    box.coords <- list(x = ocr$box[1], y = ocr$box[2], width = ocr$box[3], height = ocr$box[4])
  crop_geometry <- sprintf("%dx%d+%d+%d", box.coords$width, box.coords$height, box.coords$x, box.coords$y)
  cropped_image <- magick::image_crop(input_frame, geometry = crop_geometry)

  # pad, then enhance/binarise to optimise recognition of the overlay digits
  processed_image <- magick::image_extent(cropped_image,
                                          geometry = sprintf("%dx%d", box.coords$width + 20, box.coords$height + 21),
                                          color = "black", gravity = "center")
  processed_image <- magick::image_convert(processed_image, colorspace = "gray")
  processed_image <- magick::image_negate(processed_image)
  processed_image <- magick::image_contrast(processed_image, sharpen = 1)
  processed_image <- magick::image_resize(processed_image, geometry = "300%")
  processed_image <- magick::image_morphology(processed_image, method = "Close", kernel = "Diamond", iterations = 1)
  processed_image <- magick::image_morphology(processed_image, method = "Open", kernel = "Disk", iterations = 1)
  processed_image <- magick::image_threshold(processed_image, type = "white", threshold = "70%")
  processed_image <- magick::image_threshold(processed_image, type = "black", threshold = "60%")

  # skip OCR when the box has no dark pixels (no text)
  img_data       <- magick::image_data(processed_image, channels = "gray")
  contains_black <- any(as.integer(img_data[1, , ]) == 0)
  unlink(first_frame_path)
  if (!contains_black)
    return(list(timestamp = NA, raw_ocr = NA, contains_text = FALSE))

  # OCR + parse
  ocr_text  <- tesseract::ocr(processed_image, engine = ocr_engine)
  timestamp <- .parseTimestamp(ocr_text, id, current_time_offset)
  list(timestamp = timestamp, raw_ocr = ocr_text, contains_text = TRUE)
}


################################################################################
# Auto-detect the timestamp box coordinates within a search radius ############
################################################################################

#' Detect the white timestamp box within a frame
#'
#' Searches around the expected box location for the bright timestamp panel and returns the bounding box
#' of its white pixels; if none are found, the expected coordinates are returned unchanged.
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.detectTimestampBox <- function(frame_image,
                                expected_x = 3249,
                                expected_y = 2120,
                                expected_width = 325,
                                expected_height = 28,
                                search_radius = 80) {

  # calculate search region, clamped to the frame
  search_width  <- expected_width  + (search_radius * 2)
  search_height <- expected_height + (search_radius * 2)
  search_x      <- expected_x - search_radius
  search_y      <- expected_y - search_radius

  frame_info    <- magick::image_info(frame_image)
  search_x      <- max(0, search_x)
  search_y      <- max(0, search_y)
  search_width  <- min(search_width,  frame_info$width  - search_x)
  search_height <- min(search_height, frame_info$height - search_y)

  search_geometry <- sprintf("%dx%d+%d+%d", search_width, search_height, search_x, search_y)
  search_region   <- magick::image_crop(frame_image, geometry = search_geometry)

  # binarise to isolate the white timestamp box
  gray_region <- magick::image_convert(search_region, colorspace = "gray")
  enhanced    <- magick::image_contrast(gray_region, sharpen = 2)
  binary      <- magick::image_threshold(enhanced, type = "white", threshold = "30%")
  binary      <- magick::image_negate(binary)
  cleaned     <- magick::image_morphology(binary, method = "Close", kernel = "Rectangle:10x3")
  cleaned     <- magick::image_morphology(cleaned, method = "Open", kernel = "Rectangle:5x2")

  # pixel matrix (0-255)
  pixel_data <- magick::image_data(cleaned, channels = "gray")
  if (is.character(pixel_data)) {
    pixel_matrix <- apply(pixel_data[1, , ], c(1, 2), function(x) strtoi(x, base = 16))
  } else {
    pixel_matrix <- as.numeric(pixel_data[1, , ])
    dim(pixel_matrix) <- dim(pixel_data)[2:3]
  }

  # near-white pixels (>= 252); if none, keep the expected coordinates
  white_coords <- which(pixel_matrix >= 252, arr.ind = TRUE)
  if (nrow(white_coords) == 0)
    return(list(x = expected_x, y = expected_y, width = expected_width, height = expected_height))

  # bounding box of white pixels (rows/cols map to y/x); convert back to frame coordinates
  min_x <- min(white_coords[, 1]); max_x <- max(white_coords[, 1])
  min_y <- min(white_coords[, 2]); max_y <- max(white_coords[, 2])
  list(x = search_x + min_x - 1, y = search_y + min_y - 1,
       width = max_x - min_x + 1, height = max_y - min_y + 1)
}


################################################################################
# On-screen timestamp parsing (DDMmmYY HH:MM:SS overlay) ######################
################################################################################

#' Parse an OCR-read on-screen timestamp string to POSIXct
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.parseTimestamp <- function(ocr_text, id, time_offset = 0) {

  # normalise whitespace
  ocr_text <- gsub("\\s+", " ", ocr_text)
  ocr_text <- gsub("\n", "", ocr_text)
  ocr_text <- trimws(ocr_text)

  # character-level OCR cleaning
  ocr_text <- .cleanCharacters(ocr_text)

  # fix stray dots in the time portion (e.g. "17:.00:20.152")
  if (nchar(ocr_text) >= 9) {
    time_part <- substr(ocr_text, 9, nchar(ocr_text))
    time_part <- gsub(":\\.", ":", time_part)
    time_part <- gsub("\\.(?=\\d{2}\\.)", ":", time_part, perl = TRUE)
    ocr_text  <- paste0(substr(ocr_text, 1, 8), time_part)
  }

  if (nchar(ocr_text) < 8) return(NA)

  day_str   <- .cleanNumericString(substr(ocr_text, 1, 2))
  month_str <- .cleanMonthString(substr(ocr_text, 3, 5))
  year_str  <- .cleanNumericString(substr(ocr_text, 6, 7))
  time_str  <- .cleanTimeString(substr(ocr_text, 9, nchar(ocr_text)))

  datetime_str <- paste0(day_str, month_str, year_str, " ", time_str)
  datetime_str <- .validateAndCorrectTime(datetime_str, time_offset)

  as.POSIXct(datetime_str, format = "%d%b%y %H:%M:%OS", tz = "UTC")
}


################################################################################
# Character-cleaning helpers for OCR output ###################################
################################################################################

#' @note These functions are intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.cleanCharacters <- function(text) {
  # common OCR misreads of digits (0<->O/Q/D, 1<->I/l/|, 2<->Z, 5<->S, 6<->G/b, 8<->B)
  text <- gsub("[OQD](?=\\d|$)", "0", text, perl = TRUE)
  text <- gsub("[Il|](?=\\d|:)", "1", text, perl = TRUE)
  text <- gsub("Z(?=\\d|:)",     "2", text, perl = TRUE)
  text <- gsub("S(?=\\d|:)",     "5", text, perl = TRUE)
  text <- gsub("[Gb](?=\\d|:)",  "6", text, perl = TRUE)
  text <- gsub("B(?=\\d|:)",     "8", text, perl = TRUE)
  # punctuation misreads
  text <- gsub(",", ".", text, fixed = TRUE)
  text <- gsub(";", ":", text, fixed = TRUE)
  text
}


.cleanNumericString <- function(str) {
  str <- gsub("[BQ]", "0", str)
  str <- gsub("[Il|]", "1", str)
  str <- gsub("Z", "2", str)
  str <- gsub("S", "5", str)
  str <- gsub("[Gb]", "6", str)
  str <- gsub(",", ".", str, fixed = TRUE)
  str <- gsub(" ", "", str, fixed = TRUE)
  str
}


.cleanTimeString <- function(str) {
  str <- .cleanNumericString(str)
  str <- gsub(";", ":", str, fixed = TRUE)
  str
}


.cleanMonthString <- function(str) {
  str <- gsub("0", "O", str, fixed = TRUE)   # 0 should be O in a month abbreviation
  str <- gsub("1", "I", str, fixed = TRUE)   # 1 should be I (rare)
  str <- gsub("S5ep", "Sep", str, fixed = TRUE)
  str
}


.validateAndCorrectTime <- function(datetime_str, expected_offset = 0) {

  parts <- strsplit(datetime_str, " ")[[1]]
  if (length(parts) < 2) return(datetime_str)

  time_components <- strsplit(parts[2], ":")[[1]]
  if (length(time_components) < 2) return(datetime_str)

  hour   <- time_components[1]
  minute <- time_components[2]

  # hours 00-23
  hour_num <- as.numeric(hour)
  if (!is.na(hour_num) && hour_num > 23) {
    if (hour_num >= 26 && hour_num <= 29) hour <- "20"
    else if (hour_num >= 30)              hour <- paste0("0", substr(hour, 2, 2))
  }

  # minutes 00-59
  minute_num <- as.numeric(minute)
  if (!is.na(minute_num) && minute_num >= 60) {
    tens_digit <- substr(minute, 1, 1)
    if (!is.na(as.numeric(tens_digit)) && as.numeric(tens_digit) > 5) minute <- paste0("0", substr(minute, 2, 2))
  }

  # seconds 00-59 (if present)
  if (length(time_components) >= 3) {
    second_part <- time_components[3]
    second_num  <- suppressWarnings(as.numeric(strsplit(second_part, "\\.")[[1]][1]))
    if (!is.na(second_num) && second_num >= 60) {
      tens_digit <- substr(second_part, 1, 1)
      if (!is.na(as.numeric(tens_digit)) && as.numeric(tens_digit) > 5)
        time_components[3] <- paste0("0", substr(second_part, 2, nchar(second_part)))
    }
  }

  time_components[1] <- hour
  time_components[2] <- minute
  parts[2] <- paste(time_components, collapse = ":")
  paste(parts, collapse = " ")
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
