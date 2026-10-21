# Time boundaries: absolute instants, unqualified logger clocks and explicit reinterpretation.
# Calendar interpretation happens once at ingestion; downstream code operates on elapsed seconds.

#' Validate a named civil zone or a fixed UTC offset
#' @keywords internal
#' @noRd
.timeZoneSpec <- function(timezone) {
  .assert_string(timezone, "timezone")
  if (grepl("^[+-][0-9]{2}:?[0-9]{2}$", timezone)) {
    digits <- gsub(":", "", substring(timezone, 2L))
    hours <- as.integer(substr(digits, 1L, 2L)); minutes <- as.integer(substr(digits, 3L, 4L))
    if (hours > 23L || minutes > 59L) .abort("Invalid fixed UTC offset: {.val {timezone}}.")
    seconds <- (hours * 3600 + minutes * 60) * if (startsWith(timezone, "-")) -1 else 1
    return(list(source = timezone, display = "UTC", offset = seconds))
  }
  if (!nzchar(timezone) || !timezone %in% OlsonNames())
    .abort(c("{.arg timezone} is not a valid time zone: {.val {timezone}}.",
             "i" = "Use an Olson name (see OlsonNames()) or a fixed UTC offset such as +01:00."))
  list(source = timezone, display = timezone, offset = NULL)
}

#' Read a POSIXct's declared display zone without consulting the session zone
#' @keywords internal
#' @noRd
.declaredTimeZone <- function(x) {
  tz <- attr(x, "tzone", exact = TRUE)
  if (is.null(tz) || !length(tz) || is.na(tz[1L]) || !nzchar(tz[1L])) NA_character_ else tz[1L]
}

#' Reject invalid instants at the boundary, including failures introduced by calendar interpretation
#' @keywords internal
#' @noRd
.validateInstants <- function(x, what = "Timestamps") {
  if (!inherits(x, "POSIXct") || any(!is.finite(as.numeric(x))))
    .abort("{what} must contain finite POSIXct instants; NA, NaN and infinite times are not allowed.")
  invisible(x)
}

#' Resolve civil-clock fields carried provisionally in UTC
#' @keywords internal
#' @noRd
.resolveCivilClock <- function(x, zone, ambiguous) {
  if (!is.null(zone$offset)) return(.POSIXct(as.numeric(x) - zone$offset, "UTC"))
  if (zone$display %in% c("UTC", "GMT", "Etc/UTC", "Etc/GMT"))
    return(.POSIXct(as.numeric(x), zone$display))
  repeated <- switch(ambiguous, error = "NA", earliest = "pre", latest = "post")
  out <- lubridate::force_tz(x, zone$display, roll_dst = c("NA", repeated))
  if (any(!is.finite(as.numeric(out))))
    .abort(c("A logger clock time is nonexistent or ambiguous in {.val {zone$source}}.",
             "i" = "Nonexistent DST times must be corrected at the source. For repeated times, supply explicit UTC offsets or choose ambiguous = 'earliest'/'latest'."))
  out
}

#' Resolve supplied POSIXct values, preserving instants unless reinterpretation was explicitly requested
#' @keywords internal
#' @noRd
.resolveTagTime <- function(x, timezone = "UTC", mode = "instant", ambiguous = "error") {
  zone <- .timeZoneSpec(timezone)
  .validateInstants(x)
  input_zone <- .declaredTimeZone(x)
  if (identical(mode, "clock")) {
    if (is.na(input_zone))
      .abort("Explicit clock reinterpretation requires a declared input tzone; an unlabelled POSIXct is still an instant.")
    # Preserve the input's calendar fields, then interpret them in the declared logger clock basis.
    input_spec <- .timeZoneSpec(input_zone)
    civil <- if (!is.null(input_spec$offset)) .POSIXct(as.numeric(x) + input_spec$offset, "UTC") else
      lubridate::force_tz(x, "UTC", roll_dst = c("NA", "NA"))
    out <- .resolveCivilClock(civil, zone, ambiguous)
  } else out <- .POSIXct(as.numeric(x), zone$display)
  .validateInstants(out)
  delta <- if (mode == "clock") as.numeric(out) - as.numeric(x) else NULL
  list(datetime = out, clock = list(
    mode = mode, source_timezone = if (mode == "clock") timezone else input_zone,
    input_timezone = input_zone, requested_timezone = timezone, display_timezone = zone$display, generated = FALSE,
    ambiguous = if (mode == "clock") ambiguous else NULL,
    reinterpretation = if (mode == "clock") list(min_shift_seconds = min(delta),
                                                 max_shift_seconds = max(delta)) else NULL))
}

#' Strictly parse raw clock text, honoring explicit offsets rather than accepting date prefixes
#'
#' Supported layouts are year-month-day (hyphens or slashes) with an optional time, and the CATS
#' day.month.year + time layout. ISO datetime offsets are Z, +HH:MM or +HHMM. All patterns match the
#' complete value. Parse calendar fields in UTC first so the platform never guesses a DST fold/gap.
#' @keywords internal
#' @noRd
.parseTagClock <- function(x, timezone = "UTC", ambiguous = "error") {
  zone <- .timeZoneSpec(timezone)
  if (inherits(x, "POSIXct")) return(.resolveTagTime(x, timezone))
  if (!is.character(x)) .abort("Raw logger timestamps must be character clock readings or POSIXct instants.")
  text <- trimws(x)
  if (anyNA(text) || any(!nzchar(text))) .abort("Raw logger timestamps contain missing or blank values.")
  has_offset <- grepl("[T ][0-9]{2}:[0-9]{2}(:[0-9]{2}(\\.[0-9]+)?)?(Z|[+-][0-9]{2}:?[0-9]{2})$", text)
  suffix <- rep(NA_character_, length(text))
  suffix[has_offset] <- sub("^.*?(Z|[+-][0-9]{2}:?[0-9]{2})$", "\\1", text[has_offset], perl = TRUE)
  civil_text <- text
  civil_text[has_offset] <- sub("(Z|[+-][0-9]{2}:?[0-9]{2})$", "", text[has_offset])
  civil_text <- sub("T", " ", civil_text, fixed = TRUE)
  day <- "[0-9]{4}-[0-9]{2}-[0-9]{2}"
  slash <- "[0-9]{4}/[0-9]{2}/[0-9]{2}"
  dotted <- "[0-9]{2}\\.[0-9]{2}\\.[0-9]{4}"
  hm <- " ([01][0-9]|2[0-3]):[0-5][0-9]"
  sec <- ":[0-5][0-9](\\.[0-9]+)?"
  layouts <- list(
    c(paste0(day, hm, sec), "%Y-%m-%d %H:%M:%OS"),
    c(paste0(slash, hm, sec), "%Y/%m/%d %H:%M:%OS"),
    c(paste0(dotted, hm, sec), "%d.%m.%Y %H:%M:%OS"),
    c(paste0(day, hm), "%Y-%m-%d %H:%M"),
    c(paste0(slash, hm), "%Y/%m/%d %H:%M"),
    c(day, "%Y-%m-%d"), c(slash, "%Y/%m/%d"))
  civil <- .POSIXct(rep(NA_real_, length(text)), "UTC")
  matched <- rep(FALSE, length(text))
  for (layout in layouts) {
    remaining <- which(!matched)
    if (!length(remaining)) break
    take <- remaining[grepl(paste0("^", layout[1L], "$"), civil_text[remaining])]
    if (length(take)) {
      civil[take] <- suppressWarnings(as.POSIXct(civil_text[take], format = layout[2L], tz = "UTC"))
      matched[take] <- TRUE
    }
  }
  date_text <- sub(" .*", "", civil_text)
  date_text <- gsub("/", "-", date_text, fixed = TRUE)
  date_text <- sub("^([0-9]{2})\\.([0-9]{2})\\.([0-9]{4})$", "\\3-\\2-\\1", date_text)
  bad <- !matched | !is.finite(as.numeric(civil)) |
    is.na(civil) | format(civil, "%Y-%m-%d", tz = "UTC") != date_text
  bad[is.na(bad)] <- TRUE
  if (any(bad))
    .abort(c("Invalid or unsupported logger timestamp: {.val {utils::head(text[bad], 3L)}}.",
             "i" = "Use a complete year-month-day clock (with optional ISO UTC offset), or CATS date + time; trailing text is not accepted."))
  out <- .POSIXct(rep(NA_real_, length(text)), zone$display)
  if (any(!has_offset)) out[!has_offset] <- .resolveCivilClock(civil[!has_offset], zone, ambiguous)
  for (offset in unique(suffix[has_offset])) {
    take <- has_offset & !is.na(suffix) & suffix == offset
    seconds <- if (offset == "Z") 0 else .timeZoneSpec(offset)$offset
    out[take] <- .POSIXct(as.numeric(civil[take]) - seconds, zone$display)
  }
  .validateInstants(out)
  list(datetime = out, clock = list(
    mode = "raw_clock", source_timezone = if (any(!has_offset)) timezone else NA_character_,
    input_timezone = NULL, requested_timezone = timezone, display_timezone = zone$display, generated = FALSE,
    ambiguous = if (any(!has_offset)) ambiguous else NULL,
    explicit_offsets = unique(suffix[has_offset]), reinterpretation = NULL))
}
