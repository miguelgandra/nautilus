#######################################################################################################
# Find candidate segments for orientation validation ##################################################
#######################################################################################################

#' Find candidate segments for orientation / axis-mapping validation
#'
#' @description
#' Scans processed tag data for the clearest, most validation-worthy events and returns a tidy table of
#' short time windows to inspect (e.g. against synchronised video). Manually scrubbing hours of footage
#' for a clean manoeuvre is the main cost of video-based validation; this picks the handful of moments
#' where the animal most strongly **turned**, **rolled**, or **dived**, so the computed orientation can
#' be checked against the observed behaviour at exactly those instants.
#'
#' For each requested event type the per-sample signal is reduced to a centred rolling mean over
#' `window` seconds (so brief jitter and wiggles cancel and only sustained, net manoeuvres score), and
#' the top `n` non-overlapping peaks are returned as `[start, end]` windows centred on each peak.
#'
#' @param data A list of processed datasets (one per individual), a single aggregated
#'   data.table/data.frame with an `id.col`, or a character vector of `.rds` file paths. The output of
#'   \link{processTagData} is expected.
#' @param types Character vector of event types to detect, any of `"turn"` (net heading change, for the
#'   heading/sway sign), `"roll"` (sustained roll excursion, for the roll sign) and `"dive"` (sustained
#'   vertical speed, for pitch/surge). Default: all three. A type whose required column is absent is
#'   skipped with a note.
#' @param n Integer. Number of (non-overlapping) candidate segments to return per type, per individual.
#'   Default 5.
#' @param window Numeric. Segment length in seconds (also the smoothing window for the event signal).
#'   Default 20.
#' @param id.col,datetime.col Column names for the animal ID and datetime. Defaults `"ID"`/`"datetime"`.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal", or `2`/"detailed" (default).
#'
#' @return A data frame of candidate segments, sorted by individual, type and rank, with columns:
#' \itemize{
#'   \item \strong{id}: the individual.
#'   \item \strong{type}: `"turn"` / `"roll"` / `"dive"`.
#'   \item \strong{rank}: 1 = clearest event of that type for that individual.
#'   \item \strong{start}, \strong{end}: the segment window (POSIXct), `window` seconds wide.
#'   \item \strong{peak_time}: the instant of strongest signal within the segment.
#'   \item \strong{value}, \strong{unit}: the interpretable magnitude (net heading change in deg for
#'     turns; peak \|roll\| in deg for rolls; net depth change in m for dives).
#' }
#'
#' @seealso \link{processTagData}, \link{checkTagMapping}.
#' @examples
#' \dontrun{
#' processed <- processTagData(imported)
#' # The 5 clearest turns/rolls/dives per animal, as 20-s windows to check against video
#' segs <- findValidationSegments(processed, types = c("turn", "roll", "dive"), n = 5, window = 20)
#' subset(segs, rank == 1)   # the single best manoeuvre of each type per individual
#' }
#' @export

findValidationSegments <- function(data,
                                   types = c("turn", "roll", "dive"),
                                   n = 5,
                                   window = 20,
                                   id.col = "ID",
                                   datetime.col = "datetime",
                                   verbose = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  .assert_number(n, "n", min = 1); .assert_number(window, "window", min = 0)
  types <- match.arg(types, c("turn", "roll", "dive"), several.ok = TRUE)

  # resolve input: a character vector of RDS paths, or an in-memory list / single data.frame
  is_filepaths <- is.character(data)
  .assert_nonempty(data, "data")             # loud failure on empty input (e.g. a typo'd list.files() -> character(0))
  if (is_filepaths) {
    missing <- data[!file.exists(data)]
    if (length(missing)) .abort(c("These input files were not found:", stats::setNames(missing, rep("*", length(missing)))))
  } else if (!is.list(data) || inherits(data, "data.frame")) {
    .assert_columns(data, id.col, "data")
    data <- split(data, f = data[[id.col]])
  }
  n_tags <- length(data)
  if (n_tags == 0) return(.emptySegments())

  .log_header(lvl, "findValidationSegments", "Locating the clearest validation manoeuvres",
              bullets = sprintf("Input: %d tag%s", n_tags, if (n_tags != 1) "s" else ""),
              arrow = paste0("Events: ", paste(types, collapse = ", "),
                             sprintf(" \u00b7 top %d \u00b7 %gs windows", n, window)))

  out <- vector("list", n_tags)
  for (i in seq_len(n_tags)) {
    dt <- if (is_filepaths) readRDS(data[i]) else data[[i]]
    if (is.null(dt) || nrow(dt) == 0) next
    if (!data.table::is.data.table(dt)) dt <- data.table::as.data.table(dt)
    id <- as.character(unique(dt[[id.col]])[1])
    seg <- .findSegmentsForTag(dt, id, types, n, window, datetime.col)
    if (lvl >= 2L) {
      .log_h2(lvl, sprintf("%s (%d/%d)", id, i, n_tags))
      for (ty in types) {
        k <- sum(seg$type == ty)
        if (!ty %in% .availableTypes(dt)) .log_skip(lvl, ty, ": skipped (missing column)")
        else .log_detail(lvl, sprintf("%s: %d segment%s", ty, k, if (k != 1) "s" else ""))
      }
    }
    out[[i]] <- seg
  }

  result <- do.call(rbind, out)
  if (is.null(result)) result <- .emptySegments()
  rownames(result) <- NULL

  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, nrow(result), " candidate segment", if (nrow(result) != 1) "s", " across ", n_tags, " tag", if (n_tags != 1) "s")
    .log_runtime(lvl, start.time)
  }
  result
}


################################################################################
# Internal helpers #############################################################
################################################################################

#' Empty segment table with the canonical schema.
#' @keywords internal
#' @noRd
.emptySegments <- function() {
  data.frame(id = character(0), type = character(0), rank = integer(0),
             start = as.POSIXct(character(0)), end = as.POSIXct(character(0)),
             peak_time = as.POSIXct(character(0)), value = numeric(0), unit = character(0),
             stringsAsFactors = FALSE)
}

#' Which requested event types have the columns they need in `dt`.
#' @keywords internal
#' @noRd
.availableTypes <- function(dt) {
  nm <- names(dt)
  c(if ("turning_angle" %in% nm || "heading" %in% nm) "turn",
    if ("roll" %in% nm) "roll",
    if ("vertical_velocity" %in% nm) "dive")
}

#' Candidate segments for a single deployment.
#' @keywords internal
#' @noRd
.findSegmentsForTag <- function(dt, id, types, n, window, datetime.col) {
  t <- dt[[datetime.col]]
  fs <- .tagFs(dt, datetime.col)
  if (!is.finite(fs) || fs <= 0) return(.emptySegments())
  w <- max(1L, as.integer(round(window * fs)))

  # per-type event signal (a clear manoeuvre = a large SUSTAINED net signal over the window)
  signal_of <- function(type) {
    if (type == "turn") {
      if ("turning_angle" %in% names(dt)) dt[["turning_angle"]]
      else { h <- dt[["heading"]]; c(NA_real_, ((diff(h) + 180) %% 360) - 180) }  # per-step heading change
    } else if (type == "roll") {
      r <- dt[["roll"]]; r - stats::median(r, na.rm = TRUE)                        # deviation from resting roll
    } else if (type == "dive") {
      dt[["vertical_velocity"]]
    }
  }

  segs <- lapply(intersect(types, .availableTypes(dt)), function(type) {
    sig <- signal_of(type)
    intensity <- abs(data.table::frollmean(sig, n = w, align = "center", na.rm = TRUE))
    picks <- .greedyPeaks(intensity, t, n, window)                                 # non-overlapping top peaks
    if (!length(picks)) return(NULL)
    do.call(rbind, lapply(seq_along(picks), function(r) {
      pk <- picks[r]
      win <- which(t >= t[pk] - window / 2 & t <= t[pk] + window / 2)
      v <- .segmentValue(dt, type, win)
      data.frame(id = id, type = type, rank = r,
                 start = t[pk] - window / 2, end = t[pk] + window / 2, peak_time = t[pk],
                 value = round(v$value, 1), unit = v$unit, stringsAsFactors = FALSE)
    }))
  })
  segs <- do.call(rbind, segs)
  if (is.null(segs)) .emptySegments() else segs
}

#' Greedily pick the top `n` peaks of `intensity` that are >= `window` seconds apart.
#' @keywords internal
#' @noRd
.greedyPeaks <- function(intensity, t, n, window) {
  ord <- order(intensity, decreasing = TRUE, na.last = NA)                         # drops NA
  picked <- integer(0)
  tn <- as.numeric(t)
  for (i in ord) {
    if (!length(picked) || all(abs(tn[i] - tn[picked]) >= window)) {
      picked <- c(picked, i)
      if (length(picked) >= n) break
    }
  }
  picked
}

#' Interpretable magnitude + unit for a segment of a given type.
#' @keywords internal
#' @noRd
.segmentValue <- function(dt, type, win) {
  if (type == "turn") {
    ta <- if ("turning_angle" %in% names(dt)) dt[["turning_angle"]][win]
          else { h <- dt[["heading"]][win]; c(0, ((diff(h) + 180) %% 360) - 180) }
    list(value = abs(sum(ta, na.rm = TRUE)), unit = "deg")                          # net heading change
  } else if (type == "roll") {
    list(value = max(abs(dt[["roll"]][win]), na.rm = TRUE), unit = "deg")           # peak |roll|
  } else if (type == "dive") {
    if ("depth" %in% names(dt)) {
      d <- dt[["depth"]][win]; d <- d[is.finite(d)]
      list(value = if (length(d)) abs(d[length(d)] - d[1]) else NA_real_, unit = "m")  # net depth change
    } else {
      list(value = max(abs(dt[["vertical_velocity"]][win]), na.rm = TRUE), unit = "m/s")
    }
  }
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
