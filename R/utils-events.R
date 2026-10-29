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

# Circling calculations ----

# Internal circling calculations: no sample annotation or reconstruction dependencies.

#' @keywords internal
#' @noRd
.circlingSchema <- function(variables, circular.variables, statistics) {
  out <- data.frame(ID = character(0), event = character(0), circling_id = integer(0),
                    start = as.POSIXct(character(0), tz = "UTC"), end = as.POSIXct(character(0), tz = "UTC"),
                    duration_s = numeric(0), direction = character(0), n_rotations = numeric(0),
                    directionality = numeric(0), mean_turn_rate_deg_s = numeric(0),
                    rotation_period_s = numeric(0), turn_rate_cv = numeric(0), censored = logical(0))
  for (v in variables) {
    suffix <- if (v %in% circular.variables) c("mean_angle", "mrl") else statistics
    for (s in suffix) out[[paste0(v, "_", s)]] <- numeric(0)
  }
  out
}

#' Fully supported time-weighted averages of a piecewise-linear unwrapped heading.
#' @keywords internal
#' @noRd
.circlingSmooth <- function(h, t, window) {
  if (window == 0) return(h)
  n <- length(t); t <- t - t[1]
  answer <- rep(NA_real_, n)
  a <- t - window / 2; b <- t + window / 2
  ok <- a >= 0 & b <= t[n]
  if (!any(ok)) return(answer)
  dt <- diff(t); slope <- diff(h) / dt
  integral <- c(0, cumsum(dt * (h[-n] + h[-1]) / 2))
  at <- function(u) {
    k <- pmax(1L, pmin(n - 1L, findInterval(u, t)))
    d <- u - t[k]
    integral[k] + h[k] * d + slope[k] * d^2 / 2
  }
  answer[ok] <- (at(b[ok]) - at(a[ok])) / window
  answer
}

#' Candidate edges; short unsupported runs are tolerated, never removed from the geometry.
#' @keywords internal
#' @noRd
.circlingCandidates <- function(t, delta, min.rate, pause) {
  rate <- delta / diff(t)
  support <- ifelse(abs(rate) >= min.rate & rate != 0, sign(rate), 0)
  r <- rle(support); ends <- cumsum(r$lengths); starts <- ends - r$lengths + 1L
  rows <- list(); active <- 0; first <- last <- pending.first <- pending.last <- NA_integer_
  emit <- function(a, b) {
    rows[[length(rows) + 1L]] <<- data.frame(first = a, last = b)
  }
  for (j in seq_along(ends)) {
    direction <- r$values[j]; a <- starts[j]; b <- ends[j]
    if (active == 0) {
      if (direction != 0) { active <- direction; first <- a; last <- b }
      next
    }
    if (direction == active) {
      last <- b; pending.first <- pending.last <- NA_integer_
      next
    }
    if (direction == -active) {
      if (is.na(pending.first)) pending.first <- a
      pending.last <- b
    }
    if (t[b + 1L] - t[last + 1L] > pause) {
      emit(first, last)
      if (!is.na(pending.first)) {
        first <- pending.first; last <- pending.last; active <- -active
        if (t[b + 1L] - t[last + 1L] > pause) { emit(first, last); active <- 0 }
      } else active <- 0
      pending.first <- pending.last <- NA_integer_
    }
  }
  if (active != 0) emit(first, last)
  if (!length(rows)) return(data.frame(first = integer(0), last = integer(0)))
  do.call(rbind, rows)
}

#' Analyse one deployment, retaining assessment windows even when no event qualifies.
#' @keywords internal
#' @noRd
.circlingOne <- function(x, id, p, schema) {
  assessment <- .emptyEventIntervals(); out <- schema
  report <- data.frame(ID = id, status = "not_assessed", duration_s = NA_real_,
                       assessed_duration_s = 0, n_events = NA_integer_, max_gap_s = NA_real_,
                       pitch_offset_deg = NA_real_, reason = "", stringsAsFactors = FALSE)
  finish <- function(reason) {
    report$reason <- reason
    list(events = out, assessment = assessment, report = report)
  }
  required <- c("ID", "datetime", "heading", if (!is.null(p$max.abs.pitch)) "pitch")
  missing <- setdiff(required, names(x))
  if (length(missing)) return(finish(paste("missing", paste(missing, collapse = ", "))))
  ids <- unique(as.character(x$ID))
  if (length(ids) != 1L || is.na(ids) || !nzchar(ids) || ids != id)
    return(finish("dataset ID is missing, mixed or inconsistent with metadata"))
  if (!inherits(x$datetime, "POSIXct")) return(finish("datetime is not POSIXct"))
  t <- as.numeric(x$datetime)
  if (length(t) < 2L) return(finish("fewer than two observations"))
  if (any(!is.finite(t)) || anyDuplicated(t)) return(finish("missing, non-finite or duplicate timestamps"))
  ord <- order(t); t <- t[ord]
  report$duration_s <- t[length(t)] - t[1]
  gap <- p$max.gap %||% (1.5 * stats::median(diff(t)))
  report$max_gap_s <- gap
  h <- .asNumericSafe(x$heading)[ord]
  valid <- is.finite(h)
  if ("heading" %in% (.getMeta(x)$sensors$excluded %||% character(0))) valid[] <- FALSE
  if (!is.null(p$max.abs.pitch)) {
    offset <- .lastProcessingRecord(.getMeta(x), "processTagData")$pitch_offset_deg
    if (is.null(offset) || length(offset) != 1L || !is.finite(offset)) offset <- 0
    report$pitch_offset_deg <- offset
    pitch <- .asNumericSafe(x$pitch)[ord] + offset
    valid <- valid & is.finite(pitch) & abs(pitch) <= p$max.abs.pitch
  }
  raw <- ((diff(h) + 180) %% 360) - 180
  edge <- valid[-length(valid)] & valid[-1] & diff(t) <= gap & is.finite(raw) & abs(raw) < 180 - 1e-8
  if (!is.null(p$max.turn.rate)) edge <- edge & abs(raw / diff(t)) <= p$max.turn.rate
  # Every unavailable sample or step starts a separate block; unwrapping never spans a gap.
  group <- cumsum(c(TRUE, !edge))
  blocks <- split(which(valid), group[valid])
  result <- windows <- list()
  for (idx in blocks) {
    if (length(idx) < 2L) next
    ht <- c(0, cumsum(raw[idx[-length(idx)]]))
    hs <- .circlingSmooth(ht, t[idx], p$smooth.window)
    eligible <- which(is.finite(hs))
    if (length(eligible) < 2L) next
    ii <- idx[eligible]; tt <- t[ii]; hh <- hs[eligible]
    windows[[length(windows) + 1L]] <- data.frame(ID = id, start = .eventTime(tt[1]),
                                                end = .eventTime(tt[length(tt)]))
    delta <- diff(hh); candidates <- .circlingCandidates(tt, delta, p$min.turn.rate, p$max.pause)
    for (j in seq_len(nrow(candidates))) {
      a <- candidates$first[j]; b <- candidates$last[j]
      change <- delta[a:b]; net <- sum(change); gross <- sum(abs(change))
      rotations <- abs(net) / 360; directionality <- if (gross > 0) abs(net) / gross else 0
      if (rotations + 1e-9 < p$min.rotations || directionality + 1e-9 < p$min.directionality) next
      dt <- diff(tt)[a:b]; duration <- tt[b + 1L] - tt[a]; mean.rate <- net / duration
      variance <- sum(dt * (change / dt - mean.rate)^2) / duration
      row <- data.frame(ID = id, event = "circling", circling_id = length(result) + 1L,
                        start = .eventTime(tt[a]), end = .eventTime(tt[b + 1L]), duration_s = duration,
                        direction = if (net > 0) "clockwise" else "counterclockwise",
                        n_rotations = rotations, directionality = directionality,
                        mean_turn_rate_deg_s = mean.rate, rotation_period_s = duration / rotations,
                        turn_rate_cv = sqrt(max(0, variance)) / abs(mean.rate),
                        censored = a == 1L || b == length(delta), stringsAsFactors = FALSE)
      original <- ord[ii[a:(b + 1L)]]
      for (v in p$variables) {
        z <- if (v %in% names(x)) .asNumericSafe(x[[v]])[original] else rep(NA_real_, length(original))
        z[!is.finite(z)] <- NA_real_
        if (v %in% p$circular.variables) {
          cs <- .diveCircular(z)
          row[[paste0(v, "_mean_angle")]] <- cs[["mean_angle"]]
          row[[paste0(v, "_mrl")]] <- cs[["mrl"]]
        } else {
          if ("mean" %in% p$statistics) row[[paste0(v, "_mean")]] <- if (any(!is.na(z))) mean(z, na.rm = TRUE) else NA_real_
          if ("sd" %in% p$statistics) row[[paste0(v, "_sd")]] <- stats::sd(z, na.rm = TRUE)
        }
      }
      result[[length(result) + 1L]] <- row
    }
  }
  if (!length(windows)) return(finish("no continuous heading/posture block supports assessment"))
  assessment <- do.call(rbind, windows)
  if (length(result)) out <- do.call(rbind, result)
  report$assessed_duration_s <- sum(as.numeric(assessment$end) - as.numeric(assessment$start))
  report$n_events <- nrow(out)
  report$status <- if (report$assessed_duration_s + 1e-8 < report$duration_s) "partial" else "assessed"
  finish(if (report$status == "partial") "gaps, invalid posture/heading or smoothing support limits" else "")
}
