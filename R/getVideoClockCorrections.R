#######################################################################################################
# Extract recording timestamps and metadata from biologging tag videos ################################
#######################################################################################################

#' Derive video-clock corrections from imported tag metadata
#'
#' @description
#' Builds a small, reviewable correction table from the device and sensor-logging clock metadata retained
#' by [importTagData()]. It does not modify either the sensor data or any video timestamps. Pass the result
#' explicitly to the `clock.corrections` argument of [getVideoMetadata()] to apply it.
#'
#' @param data Imported sensor data in any standard pipeline form: a `nautilus_tag`, a named list of tag
#'   objects, or paths to saved `.rds` objects.
#'
#' @details
#' A correction is derived only for the unambiguous CATS sidecar case: `[logging]` explicitly states that
#' the sensor stream is UTC, source-clock provenance is compatible with UTC, and
#' `[device] utc_offset` is finite and non-zero. The device clock is converted to UTC by adding
#' `-utc_offset * 3600` seconds. Thus a device offset of `-1` produces a video-clock correction of `+3600`
#' seconds.
#'
#' A non-zero device offset without an explicit UTC logging declaration is reported but omitted. The
#' function deliberately does not infer corrections from ambiguous or non-UTC logging metadata.
#' A UTC display alone does not establish a UTC source clock. Legacy objects without separate clock
#' provenance retain the conservative fixed-UTC display-zone guard.
#'
#' @return A data frame with one row per suggested deployment correction and columns `ID`,
#'   `clock_correction_s`, `clock_correction_source`, `device_utc_offset_h`, and
#'   `logging_utc_offset_h`. A deployment needing no correction is omitted.
#'
#' @seealso [getVideoMetadata()] for applying the returned corrections while extracting video timing;
#'   [getTagMetadata()] for inspecting the underlying sidecar provenance.
#'
#' @examples
#' \dontrun{
#' corrections <- getVideoClockCorrections(imported_tags)
#' corrections
#'
#' video_metadata <- getVideoMetadata("./videos", clock.corrections = corrections)
#' }
#' @export

getVideoClockCorrections <- function(data) {
  src <- .resolveInput(data)
  rows <- list()
  ambiguous <- character(0)

  scalar_offset <- function(x) {
    if (length(x) != 1L) return(NA_real_)
    value <- suppressWarnings(as.numeric(x))
    if (length(value) != 1L || !is.finite(value)) NA_real_ else value
  }
  utc_zones <- c("UTC", "GMT", "UCT", "UNIVERSAL", "ZULU", "UTC0", "GMT0",
                 "ETC/UTC", "ETC/GMT", "ETC/UCT", "ETC/UNIVERSAL", "ETC/ZULU")

  for (i in seq_len(src$n)) {
    tag <- src$get(i)
    meta <- .getMeta(tag)
    id <- meta$id %||% src$ids[i]
    if (length(id) != 1L || is.na(id) || !nzchar(as.character(id)))
      .abort("Every item in {.arg data} must have a non-missing deployment ID in its tag metadata.")
    id <- as.character(id)

    sidecar <- meta$sidecar
    if (is.null(sidecar) || !identical(sidecar$source_type, "cats_diary_txt") ||
        is.null(sidecar$device)) next
    device_offset <- scalar_offset(sidecar$device$utc_offset)
    if (!is.finite(device_offset) || abs(device_offset) < sqrt(.Machine$double.eps)) next

    logging_offset <- if (!is.null(sidecar$logging)) scalar_offset(sidecar$logging$utc_offset) else NA_real_
    if (!is.finite(logging_offset) || abs(logging_offset) >= sqrt(.Machine$double.eps)) {
      reason <- if (!is.finite(logging_offset)) "no explicit [logging] UTC declaration" else
        sprintf("[logging] offset is %gh, not UTC", logging_offset)
      ambiguous <- c(ambiguous, sprintf("%s (%s)", id, reason))
      next
    }

    # Displaying a resolved stream in UTC does not establish a UTC source clock. New objects carry
    # their interpretation separately; legacy objects retain the older conservative zone guard.
    clock <- meta$sensors$clock
    if (!is.null(clock)) {
      fixed_utc <- function(tz) {
        if (is.na(tz)) return(FALSE)
        spec <- tryCatch(.timeZoneSpec(tz), error = function(e) NULL)
        !is.null(spec) && (toupper(tz) %in% utc_zones || (!is.null(spec$offset) && spec$offset == 0))
      }
      zones <- clock$source_timezone
      zones <- zones[!is.na(zones)]
      offsets <- clock$explicit_offsets
      source_known <- length(zones) + length(offsets) > 0L
      source_utc <- identical(clock$mode, "instant") ||
        (source_known && all(vapply(zones, fixed_utc, logical(1))) &&
           all(vapply(offsets, function(x) identical(x, "Z") || fixed_utc(x), logical(1))))
      if (!source_utc) {
        ambiguous <- c(ambiguous, sprintf("%s (source clock is not established as fixed UTC)", id))
        next
      }
    }
    sensor_timezone <- if (!is.null(clock)) "UTC" else meta$sensors$timezone
    sensor_timezone <- if (length(sensor_timezone) == 1L && !is.na(sensor_timezone))
      toupper(as.character(sensor_timezone)) else NA_character_
    if (is.na(sensor_timezone) || !sensor_timezone %in% utc_zones) {
      label <- if (is.na(sensor_timezone)) "unknown" else sensor_timezone
      ambiguous <- c(ambiguous, sprintf("%s (sensor data use timezone %s, not fixed UTC)", id, label))
      next
    }

    rows[[length(rows) + 1L]] <- data.frame(
      ID = id,
      clock_correction_s = -device_offset * 3600,
      clock_correction_source = "cats_sidecar_device_to_utc",
      device_utc_offset_h = device_offset,
      logging_utc_offset_h = logging_offset,
      stringsAsFactors = FALSE
    )
  }

  if (length(ambiguous)) {
    ambiguous <- unique(ambiguous)
    .warn_grouped(
      "A video-clock correction could not be derived for {length(ambiguous)} deployment{?s}.",
      ambiguous,
      hints = "Automatic correction requires explicit UTC [logging] metadata and sensor data imported in UTC.",
      items.header = "Not corrected:"
    )
  }

  if (!length(rows)) {
    return(data.frame(
      ID = character(0), clock_correction_s = numeric(0),
      clock_correction_source = character(0), device_utc_offset_h = numeric(0),
      logging_utc_offset_h = numeric(0), stringsAsFactors = FALSE
    ))
  }

  out <- do.call(rbind, rows)
  duplicate_ids <- unique(out$ID[duplicated(out$ID)])
  if (length(duplicate_ids))
    .abort("{.arg data} contains more than one applicable clock record for deployment{?s} {.val {duplicate_ids}}.")
  rownames(out) <- NULL
  out
}
