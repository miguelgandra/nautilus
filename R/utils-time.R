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

# Additional shared time helpers

#' Seconds-since-epoch for a user-supplied time column, or `NULL` if it is not a time at all.
#'
#' Elapsed-time arithmetic across the package assumes SECONDS. `as.numeric()` was applied blind, so a
#' `Date` column - which counts DAYS - came out 86400x too small (a 300-day record drawn on a five-minute
#' axis), and a character column came out all-`NA`, producing a complete but empty figure. Returning
#' `NULL` for a non-time column lets each caller raise an error that names its own argument.
#' @keywords internal
#' @noRd
.asTimeSeconds <- function(z) {
  if (inherits(z, "POSIXct")) return(as.numeric(z))
  if (inherits(z, "POSIXlt")) return(as.numeric(as.POSIXct(z)))
  if (inherits(z, "Date")) return(as.numeric(z) * 86400)          # Date counts DAYS, not seconds
  if (is.numeric(z)) return(as.numeric(z))                        # already epoch seconds
  NULL
}

#' Is `z` usable as a time column at all? (cheap guard for callers that only need to reject.)
#' @keywords internal
#' @noRd
.isTimeColumn <- function(z) !is.null(.asTimeSeconds(z))

#' Tracked seconds spanned by one deployment's time column.
#'
#' The cohort figure reported by importTagData/processTagData is the SUM of these, i.e. total
#' animal-time, not the calendar window the cohort covers - deployments overlap in time, so a span
#' across the whole batch would be a different (and much larger) quantity.
#'
#' Returns 0 rather than NA for an unusable column, so a running total is never poisoned by one tag.
#' @keywords internal
#' @noRd
.tagSpanSeconds <- function(z) {
  t <- .asTimeSeconds(z)                                  # shared coercion contract (Date is in DAYS)
  if (is.null(t)) return(0)
  t <- t[is.finite(t)]
  if (length(t) < 2L) return(0)
  as.numeric(max(t) - min(t))
}

# Solar-event calculation for diel classification.

# getDielPhase helpers ----

#' Solar event times (sunrise / sunset / dawn / dusk) without the sf / GDAL stack
#'
#' Computes the time at which the sun crosses a given altitude on the day of each `datetime`, replacing
#' `suntools::sunriset()` / `suntools::crepuscule()` (which now depend on `sf` and its GDAL/GEOS/PROJ system
#' libraries). Implements the NOAA Solar Calculator equations (after Meeus, "Astronomical Algorithms";
#' NOAA ESRL Global Monitoring Laboratory), with a two-pass refinement (solar parameters re-evaluated at the
#' estimated event time). Verified against `suntools` 1.1.0: sub-second agreement for sunrise/sunset and
#' within ~3 s for civil/nautical twilight.
#'
#' @param lon,lat Longitude / latitude in decimal degrees (East / North positive); length 1 or `length(datetimes)`.
#' @param datetimes POSIXct instants; the event is returned for the UTC calendar date of each.
#' @param solarDep Sun depression below the horizon (degrees): 0.833 for sunrise/sunset (refraction +
#'   solar semidiameter), or 6 / 12 / 18 for civil / nautical / astronomical twilight.
#' @param morning `TRUE` for the morning crossing (sunrise / dawn), `FALSE` for the evening (sunset / dusk).
#' @return POSIXct (UTC) event times; `NA` where the sun never reaches that altitude (polar day / night).
#' @keywords internal
#' @noRd
.solarEventTime <- function(lon, lat, datetimes, solarDep, morning) {
  rad  <- pi / 180
  n    <- length(datetimes)
  lon  <- rep_len(lon, n); lat <- rep_len(lat, n)
  day0 <- as.numeric(as.Date(datetimes, tz = "UTC"))          # days since 1970-01-01 (UTC calendar date)
  jd0  <- day0 + 2440587.5                                     # Julian Day at 00:00 UTC

  # solar declination (deg) + equation of time (min) at a Julian Day (NOAA Solar Calculator / Meeus)
  .solpar <- function(jd) {
    T  <- (jd - 2451545) / 36525
    L0 <- (280.46646 + T * (36000.76983 + T * 0.0003032)) %% 360
    M  <- 357.52911 + T * (35999.05029 - 0.0001537 * T); Mr <- M * rad
    e  <- 0.016708634 - T * (0.000042037 + 0.0000001267 * T)
    C  <- sin(Mr) * (1.914602 - T * (0.004817 + 0.000014 * T)) +
          sin(2 * Mr) * (0.019993 - 0.000101 * T) + sin(3 * Mr) * 0.000289
    lambda <- (L0 + C) - 0.00569 - 0.00478 * sin((125.04 - 1934.136 * T) * rad)
    eps0 <- 23 + (26 + (21.448 - T * (46.815 + T * (0.00059 - T * 0.001813))) / 60) / 60
    eps  <- eps0 + 0.00256 * cos((125.04 - 1934.136 * T) * rad)
    decl <- asin(sin(eps * rad) * sin(lambda * rad)) / rad
    y    <- tan(eps / 2 * rad)^2
    eqt  <- 4 / rad * (y * sin(2 * L0 * rad) - 2 * e * sin(Mr) + 4 * e * y * sin(Mr) * cos(2 * L0 * rad) -
                       0.5 * y^2 * sin(4 * L0 * rad) - 1.25 * e^2 * sin(2 * Mr))
    list(decl = decl, eqt = eqt)
  }
  # event time (minutes from 00:00 UTC) for solar parameters evaluated at Julian Day `jd`
  .eventMin <- function(jd) {
    s    <- .solpar(jd)
    cosH <- (cos((90 + solarDep) * rad) - sin(lat * rad) * sin(s$decl * rad)) /
            (cos(lat * rad) * cos(s$decl * rad))
    ha   <- ifelse(abs(cosH) > 1, NA_real_, acos(pmin(pmax(cosH, -1), 1)) / rad)  # hour angle (deg)
    noon <- 720 - 4 * lon - s$eqt                             # solar noon (min UTC); lon East positive
    if (morning) noon - 4 * ha else noon + 4 * ha             # 4 min per degree of hour angle
  }
  m <- .eventMin(jd0 + 0.5)                                   # first guess at 12:00 UTC
  m <- .eventMin(jd0 + m / 1440)                             # refine at the estimated event time
  as.POSIXct(day0 * 86400 + m * 60, origin = "1970-01-01", tz = "UTC")
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
