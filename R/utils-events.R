# Small shared temporal contracts for detectors, annotations and plotting layers.

#' @keywords internal
#' @noRd
.eventTime <- function(t) as.POSIXct(t, origin = "1970-01-01", tz = "UTC")

#' @keywords internal
#' @noRd
.emptyEventIntervals <- function() {
  data.frame(ID = character(0), start = .eventTime(numeric(0)), end = .eventTime(numeric(0)))
}

#' Normalise generic event/assessment windows without changing the caller's table.
#' @keywords internal
#' @noRd
.eventIntervals <- function(x, arg = "events", require.event = TRUE) {
  if (is.null(x)) return(NULL)
  if (!is.data.frame(x)) .abort("{.arg {arg}} must be a data frame of event windows.")
  need <- c("ID", "start", "end", if (require.event) "event")
  .assert_columns(x, need, arg)
  for (nm in c("start", "end"))
    if (!inherits(x[[nm]], "POSIXct")) .abort("{.field {nm}} in {.arg {arg}} must be POSIXct.")
  out <- as.data.frame(x)[, need, drop = FALSE]
  out$ID <- as.character(out$ID)
  if (anyNA(out$ID) || any(!nzchar(out$ID))) .abort("{.arg {arg}} contains a missing deployment ID.")
  if (require.event) {
    out$event <- as.character(out$event)
    if (anyNA(out$event) || any(!nzchar(out$event))) .abort("{.arg {arg}} contains a missing event type.")
  }
  if (any(!is.finite(as.numeric(out$start))) || any(!is.finite(as.numeric(out$end))))
    .abort("{.arg {arg}} contains missing or non-finite interval endpoints.")
  if (any(out$end < out$start)) .abort("{.arg {arg}} contains an end before its start.")
  rownames(out) <- NULL
  attr(out, "deployment_ids") <- unique(c(out$ID,
    attr(x, "circling_detection", exact = TRUE)$deployments$ID))
  out
}

#' @keywords internal
#' @noRd
.eventPalette <- function(events, theme) {
  types <- sort(unique(events$event))
  if (!length(types)) return(stats::setNames(character(0), character(0)))
  stats::setNames(.themePalette(theme$palette, length(types)), types)
}

#' @keywords internal
#' @noRd
.eventsForDeployment <- function(events, id) {
  if (is.null(events)) return(NULL)
  events[events$ID == id, , drop = FALSE]
}

#' @keywords internal
#' @noRd
.warnUnmatchedEvents <- function(events, ids) {
  if (is.null(events)) return(invisible(NULL))
  available <- attr(events, "deployment_ids", exact = TRUE) %||% unique(events$ID)
  # A cohort-wide event table may intentionally accompany a single-dive/deployment plot.
  if (length(available) && !length(intersect(available, ids)))
    .warn_grouped("No event deployments match the supplied plotting data.", available, style = "inline")
}

#' Event bands are drawn from exact interval endpoints, independently of trace binning.
#' @keywords internal
#' @noRd
.drawEventBands <- function(events, palette, time, theme, cex) {
  if (is.null(events) || !nrow(events)) return(invisible(NULL))
  bounds <- range(as.numeric(time), finite = TRUE)
  a <- pmax(as.numeric(events$start), bounds[1]); b <- pmin(as.numeric(events$end), bounds[2])
  keep <- a <= b
  if (!any(keep)) return(invisible(NULL))
  usr <- graphics::par("usr")
  graphics::rect(a[keep], usr[3], b[keep], usr[4],
                 col = grDevices::adjustcolor(unname(palette[events$event[keep]]), alpha.f = 0.18), border = NA)
  types <- unique(events$event[keep])
  graphics::legend("topright", legend = types, fill = unname(palette[types]), border = NA,
                   bty = "n", text.col = theme$ink, cex = cex * 0.65)
  invisible(NULL)
}

#' Gather event paths before background thinning, splitting at missing positions or long gaps.
#' @keywords internal
#' @noRd
.eventTrackPaths <- function(x, events, datetime.col, max.points) {
  paths <- list(); matched <- logical(if (is.null(events)) 0L else nrow(events))
  finish <- function() list(paths = paths, matched = matched)
  if (!length(matched) || !all(c("pseudo_lon", "pseudo_lat", datetime.col) %in% names(x))) return(finish())
  time <- .asTimeSeconds(x[[datetime.col]])
  if (is.null(time)) return(finish())
  ord <- order(time); time <- time[ord]
  lon <- .asNumericSafe(x$pseudo_lon)[ord]; lat <- .asNumericSafe(x$pseudo_lat)[ord]
  ok <- is.finite(time) & is.finite(lon) & is.finite(lat)
  dt <- diff(time); positive <- dt[is.finite(dt) & dt > 0]
  gap <- if (length(positive)) 4 * stats::median(positive) else Inf
  edge <- ok[-length(ok)] & ok[-1] & is.finite(dt) & dt > 0 & dt <= gap
  group <- cumsum(c(TRUE, !edge))
  for (j in seq_len(nrow(events))) {
    inside <- ok & time >= as.numeric(events$start[j]) & time <= as.numeric(events$end[j])
    runs <- split(which(inside), group[inside])
    matched[j] <- any(inside)
    for (idx in runs) {
      n <- length(idx)
      take <- if (n > max.points) unique(c(seq(1L, n, by = ceiling(n / max.points)), n)) else seq_len(n)
      ii <- idx[take]
      paths[[length(paths) + 1L]] <- list(event = events$event[j],
                                       data = data.frame(datetime = .eventTime(time[ii]), lon = lon[ii], lat = lat[ii]))
    }
  }
  finish()
}

#' @keywords internal
#' @noRd
.drawEventTracks <- function(paths, palette) {
  for (path in paths) {
    d <- path$data; col <- unname(palette[path$event])
    if (nrow(d) == 1L) graphics::points(d$lon, d$lat, pch = 16, col = col, cex = 0.8)
    else graphics::lines(d$lon, d$lat, col = col, lwd = 3)
  }
  invisible(NULL)
}
