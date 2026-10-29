# Shared dashboard drawing and synchronized frame state.

# .drawPreparedOverlayDashboard
.drawPreparedOverlayDashboard <- function(fd, clip, current.time, depth.window, activity.window, theme,
    caption, candidates, plan, context, model, motion) {
    theme <- .completeOverlayTheme(theme)
    if (plan$dashboard == "validation") {
        graphics::layout(matrix(1:4, ncol = 1L), heights = c(2.8, 1.3, 1.2, 0.5))
        op <- graphics::par(bg = theme$canvas, mar = c(1, 1, 2.2, 1), oma = c(0, 0, 0.3, 0))
        on.exit(graphics::par(op), add = TRUE)
        .drawPreparedAttitudeModel(fd, theme, model, NULL, show.heading = FALSE)
        graphics::par(mar = c(1.6, 3.2, 2, 1))
        .drawTriTrace(clip, current.time, activity.window, c("gx", "gy", "gz"), c("roll-rate", "pitch-rate",
            "yaw-rate"), "Gyroscope (body)", theme, fd)
        .drawSeriesPanel(clip, current.time, depth.window, "depth", "Depth", "m", fd$depth, theme, invert = TRUE,
            fill = "#1f4e8c")
        graphics::par(mar = c(0, 0, 0, 0))
        .drawCaption(fd$datetime, caption, theme)
        return(invisible(NULL))
    }
    if (plan$dashboard == "validation-compare") {
        n <- nrow(candidates)
        pal <- .candidatePalette(n)
        graphics::layout(rbind(rep(1L, n), 1L + seq_len(n), rep(n + 2L, n), rep(n + 3L, n)), heights = c(0.55,
            2.7, 1, 0.32))
        op <- graphics::par(bg = theme$canvas, oma = c(0, 0, 0.3, 0))
        on.exit(graphics::par(op), add = TRUE)
        graphics::par(mar = c(0, 1, 0, 1))
        .drawHeader(caption, theme)
        graphics::par(mar = c(1, 1, 2.4, 1))
        for (i in seq_len(n)) .drawPreparedAttitudeModel(fd, theme, model, NULL, label = candidates$label[i],
            body.col = pal[i], show.heading = FALSE, pitch.col = candidates$pitch[i], roll.col = candidates$roll[i])
        graphics::par(mar = c(1.6, 3.2, 2, 1))
        .drawSeriesPanel(clip, current.time, depth.window, "depth", "Depth", "m", fd$depth, theme, invert = TRUE,
            fill = "#1f4e8c")
        graphics::par(mar = c(0, 0, 0, 0))
        .drawTimestamp(fd$datetime, theme)
        return(invisible(NULL))
    }
    draw.module <- function(metric) .drawMetricModule(metric, fd, clip, current.time, theme, plan, context,
        model, motion)
    metrics <- plan$metrics
    if (plan$dashboard == "ribbon") {
        n <- length(metrics)
        graphics::layout(rbind(seq_len(n), rep(n + 1L, n)), heights = c(1, 0.22))
        op <- graphics::par(bg = theme$canvas, oma = c(0, 0, 0, 0))
        on.exit(graphics::par(op), add = TRUE)
        for (metric in metrics) draw.module(metric)
        graphics::par(mar = c(0, 0, 0, 0))
        .drawTimestamp(fd$datetime, theme)
        return(invisible(NULL))
    }
    groups <- .presentationGroups(metrics, plan$dashboard)
    mats <- list()
    heights <- numeric()
    next.id <- 1L
    for (i in seq_along(groups$metrics)) {
        group <- groups$metrics[[i]]
        ids <- next.id + seq_along(group) - 1L
        next.id <- next.id + length(group)
        mats[[i]] <- rep(ids, each = 6L/length(ids))
        heights[i] <- if (groups$tiles[i])
            0.72
        else max(vapply(group, .moduleHeight, numeric(1)))
    }
    mats[[length(mats) + 1L]] <- rep(next.id, 6L)
    heights <- c(heights, 0.22)
    gap <- isTRUE(theme$overlay) && plan$dashboard == "general"
    if (gap) {
        mats[[length(mats) + 1L]] <- rep(next.id + 1L, 6L)
        heights <- c(heights, 0.34)
    }
    graphics::layout(do.call(rbind, mats), heights = heights)
    op <- graphics::par(bg = theme$canvas, oma = c(0, 0, 0.3, 0))
    on.exit(graphics::par(op), add = TRUE)
    for (i in seq_along(groups$metrics)) for (metric in groups$metrics[[i]]) {
        if (groups$tiles[i])
            .drawMetricTile(metric, fd, theme)
        else draw.module(metric)
    }
    graphics::par(mar = c(0, 3.1, 0, 0.8))
    .drawTimestamp(fd$datetime, theme)
    if (gap) {
        graphics::par(mar = c(0, 0, 0, 0))
        .drawTransparentSpacer()
    }
    invisible(NULL)
}

# .interpolateOverlayColumn
.interpolateOverlayColumn <- function(t, x, at, max.gap, circular = FALSE) {
    out <- rep(NA_real_, length(at))
    i <- findInterval(at, t)
    inside <- i > 0L & i <= length(t)
    exact <- inside & at == t[pmax(1L, i)]
    out[exact] <- x[i[exact]]
    between <- which(inside & !exact & i < length(t))
    if (length(between)) {
        left <- i[between]
        right <- left + 1L
        ok <- t[right] - t[left] <= max.gap & is.finite(x[left]) & is.finite(x[right])
        between <- between[ok]
        left <- left[ok]
        right <- right[ok]
        delta <- x[right] - x[left]
        if (circular)
            delta <- (delta + 180)%%360 - 180
        out[between] <- x[left] + delta * (at[between] - t[left])/(t[right] - t[left])
    }
    out[!is.finite(out)] <- NA_real_
    out
}

# .prepareOverlayFrameState
.prepareOverlayFrameState <- function(data, frame.times, motion = NULL, max.gap = 1, pitch.cols = "pitch",
    roll.cols = "roll") {
    .assert_number(max.gap, "max.sensor.gap", min = .Machine$double.eps)
    .assert_columns(data, "datetime", "data")
    t <- as.numeric(data$datetime)
    at <- as.numeric(frame.times)
    if (!inherits(data$datetime, "POSIXct") || !length(t) || any(!is.finite(t)) || any(diff(t) <= 0))
        .abort("Sensor timestamps must be finite POSIXct values in strictly increasing order; resolve duplicate timestamps first.")
    if (!inherits(frame.times, "POSIXct") || any(!is.finite(at)))
        .abort("Frame times must be finite POSIXct values.")
    rows <- findInterval(at, t)
    frames <- data.table::data.table(datetime = frame.times)
    angle.cols <- intersect(unique(c(pitch.cols, roll.cols, "heading")), names(data))
    for (col in angle.cols) {
        if (!is.numeric(data[[col]]))
            .abort("Orientation column {.val {col}} must be numeric.")
        value <- .interpolateOverlayColumn(t, data[[col]], at, max.gap, circular = col %in% c(roll.cols,
            "heading"))
        if (col == "heading")
            value <- value%%360
        else if (col %in% roll.cols)
            value <- (value + 180)%%360 - 180
        data.table::set(frames, j = col, value = value)
    }
    phase <- strength <- frequency <- rep(0, length(at))
    status <- rep(if (is.null(motion)) "disabled" else "unavailable", length(at))
    if (!is.null(motion)) {
        f <- data[[motion$columns[1]]]
        a <- data[[motion$columns[2]]]
        swimming <- if ("tbf_swimming" %in% names(data))
            data$tbf_swimming
        else rep(NA, length(t))
        valid <- is.finite(f) & f > 0 & is.finite(a) & a >= 0 & (is.na(swimming) | swimming)
        step <- c(0, diff(t))
        continuation <- valid & c(FALSE, utils::head(valid, -1L)) & step <= max.gap
        increments <- rep(0, length(t))
        k <- which(continuation)
        increments[k] <- pi * (f[k - 1L] + f[k]) * step[k]
        cumulative <- cumsum(increments)
        starts <- which(valid & !continuation)
        segment <- cumsum(valid & !continuation)
        sample.phase <- rep(NA_real_, length(t))
        if (length(starts))
            sample.phase[valid] <- motion$phase.offset + cumulative[valid] - cumulative[starts[segment[valid]]]
        inside <- rows > 0L & at <= utils::tail(t, 1L)
        j <- which(inside)
        if (length(j)) {
            left <- rows[j]
            exact <- at[j] == t[left]
            right <- pmin(left + 1L, length(t))
            supported <- valid[left] & (exact | (right > left & continuation[right]))
            glide <- !is.na(swimming[left]) & !swimming[left] & (exact | (right > left & t[right] - t[left] <=
                max.gap & !is.na(swimming[right]) & !swimming[right]))
            status[j[glide]] <- "gliding"
            j <- j[supported]
            left <- left[supported]
            right <- right[supported]
            dt <- at[j] - t[left]
            span <- t[right] - t[left]
            slope <- ifelse(span > 0, (f[right] - f[left])/span, 0)
            slope[dt == 0] <- 0
            phase[j] <- sample.phase[left] + 2 * pi * (f[left] * dt + 0.5 * slope * dt^2)
            fraction <- ifelse(span > 0, dt/span, 0)
            amp <- a[left]
            interp <- dt > 0
            amp[interp] <- a[left[interp]] + fraction[interp] * (a[right[interp]] - a[left[interp]])
            strength[j] <- pmin(1, pmax(0, amp/motion$amplitude.reference))
            frequency[j] <- f[left] + slope * dt
            status[j] <- "estimated"
        }
    }
    frames[, `:=`(.motion.phase = phase, .motion.strength = strength, .motion.frequency = frequency,
        .motion.status = status)]
    list(frames = frames, rows = rows, angle.columns = angle.cols, motion = motion)
}

# .overlayFrameData
.overlayFrameData <- function(state, i, data) {
    row <- state$rows[i]
    fd <- if (row > 0L)
        as.list(data[row])
    else as.list(data[NA_integer_])
    frame <- as.list(state$frames[i])
    for (name in names(frame)) fd[[name]] <- frame[[name]]
    fd
}

# .checkOverlayMotionRate
.checkOverlayMotionRate <- function(state, overlay.fps, video.fps) {
    if (is.null(state$motion) || state$motion$max.angle == 0)
        return(invisible(NULL))
    f <- state$frames$.motion.frequency
    if (!any(f > 0))
        return(invisible(NULL))
    rate <- min(overlay.fps, video.fps)
    peak <- max(f)
    if (rate <= 2 * peak)
        cli::cli_warn("Schematic motion is undersampled: the effective {rate} fps does not exceed twice the maximum {round(peak, 2)} Hz beat frequency.")
    else if (rate < 8 * peak)
        cli::cli_warn("Schematic motion has fewer than eight frames per fastest beat; increase {.arg overlay.fps} and use a sufficiently fast source video for smoother motion.")
    invisible(NULL)
}




################################################################################
# Dashboards (internal) ########################################################
################################################################################

#' Registry of presentation modules and their data contracts.
#' @keywords internal
#' @noRd
.overlayMetricRegistry <- function() {
  list(
    orientation       = list(columns = c("heading", "pitch", "roll"), type = "orientation",
                             label = "Orientation", unit = "", colour = "#ff453a", window = NA_real_),
    heading           = list(columns = "heading", type = "dial", label = "Heading", unit = "\u00b0",
                             colour = "#ff453a", window = NA_real_),
    pitch             = list(columns = "pitch", type = "dial", label = "Pitch", unit = "\u00b0",
                             colour = "#34c8c8", window = NA_real_),
    roll              = list(columns = "roll", type = "dial", label = "Roll", unit = "\u00b0",
                             colour = "#34c8c8", window = NA_real_),
    depth             = list(columns = "depth", type = "depth", label = "Depth", unit = "m",
                             colour = "#0a84ff", window = "depth"),
    vedba             = list(columns = "vedba", type = "vedba", label = "VeDBA", unit = "g",
                             colour = "#ffd60a", window = "activity"),
    vertical_velocity = list(columns = "vertical_velocity", type = "vertical_velocity",
                             label = "Vertical velocity", unit = "m/s", colour = "#ff9f0a",
                             window = "activity"),
    paddle_speed      = list(columns = "paddle_speed", type = "series", label = "Paddle speed",
                             unit = "m/s", colour = "#30d158", window = "activity"),
    paddle_freq       = list(columns = "paddle_freq", type = "series", label = "Paddle frequency",
                             unit = "Hz", colour = "#64d2ff", window = "activity"),
    tbf_hz_peaks      = list(columns = "tbf_hz_peaks", type = "series", label = "Tailbeat frequency",
                             unit = "Hz", colour = "#bf5af2", window = "activity"),
    tbf_hz_wavelet    = list(columns = "tbf_hz_wavelet", type = "series", label = "Wavelet tailbeat",
                             unit = "Hz", colour = "#af52de", window = "activity"),
    pseudo_trajectory = list(columns = c("pseudo_lon", "pseudo_lat", "pseudo_depth"),
                             type = "trajectory", label = "Pseudo-trajectory", unit = "m",
                             colour = "#30d158", window = 120)
  )
}


#' Resolve a dashboard preset, orientation representation and per-module windows.
#' @keywords internal
#' @noRd
.resolveDashboardPlan <- function(dashboard, metrics = NULL, orientation = "model",
                                  orientation.model = "blue_shark", candidates = NULL,
                                  depth.window = 300, activity.window = 30, metric.windows = NULL) {
  if (dashboard %in% c("validation", "validation-compare"))
    return(list(dashboard = dashboard, metrics = character(), windows = numeric(),
                orientation = orientation, orientation.model = orientation.model))

  presets <- list(
    general  = c("orientation", "depth", "vedba", "vertical_velocity"),
    compact  = c("orientation", "depth", "vedba"),
    expanded = c("pseudo_trajectory", "orientation", "depth", "vedba", "vertical_velocity"),
    ribbon   = c("depth", "vedba", "vertical_velocity"),
    focus    = c("depth", "orientation", "vedba", "vertical_velocity")
  )
  if (is.null(metrics)) metrics <- presets[[dashboard]]
  if (!is.character(metrics) || !length(metrics) || anyNA(metrics) || any(!nzchar(metrics)))
    .abort("{.arg metrics} must be a non-empty character vector of module names.")

  registry <- .overlayMetricRegistry()
  unknown <- setdiff(metrics, names(registry))
  if (length(unknown))
    .abort(c("Unknown dashboard {cli::qty(unknown)}module{?s}: {.val {unknown}}.",
             "i" = "Choose from {.val {names(registry)}}."))
  if (orientation == "dials")
    metrics <- unlist(lapply(metrics, function(x) if (x == "orientation") c("heading", "pitch", "roll") else x),
                      use.names = FALSE)
  if (anyDuplicated(metrics))
    .abort("{.arg metrics} resolves to duplicated modules: {.val {unique(metrics[duplicated(metrics)])}}.")
  if (dashboard == "ribbon" && length(metrics) > 4L)
    .abort("The {.val ribbon} dashboard supports at most four modules.")

  windows <- vapply(metrics, function(metric) {
    value <- registry[[metric]]$window
    if (is.character(value) && value == "depth") depth.window else
      if (is.character(value) && value == "activity") activity.window else
        if (is.numeric(value)) value else NA_real_
  }, numeric(1))
  names(windows) <- metrics

  if (!is.null(metric.windows)) {
    if (!is.numeric(metric.windows) || is.null(names(metric.windows)) ||
        anyNA(names(metric.windows)) || any(!nzchar(names(metric.windows))) || anyDuplicated(names(metric.windows)) ||
        any(!is.finite(metric.windows)) || any(metric.windows <= 0))
      .abort("{.arg metric.windows} must be a named numeric vector of positive finite seconds.")
    bad <- setdiff(names(metric.windows), names(windows)[is.finite(windows)])
    if (length(bad))
      .abort("{.arg metric.windows} names must identify selected scrolling modules: {.val {bad}} does not.")
    windows[names(metric.windows)] <- metric.windows
  }
  list(dashboard = dashboard, metrics = metrics, windows = windows, orientation = orientation,
       orientation.model = orientation.model)
}


#' Columns a given dashboard requires (alongside `datetime`).
#' @keywords internal
#' @noRd
.dashboardColumns <- function(dashboard, candidates = NULL, metrics = NULL, orientation = "model") {
  if (dashboard == "validation") return(c("datetime", "depth", "pitch", "roll"))
  if (dashboard == "validation-compare")
    return(c("datetime", "depth", unique(c(candidates$pitch, candidates$roll))))
  if (is.null(metrics))
    metrics <- .resolveDashboardPlan(dashboard, orientation = orientation)$metrics
  registry <- .overlayMetricRegistry()
  unique(c("datetime", unlist(lapply(metrics, function(x) registry[[x]]$columns), use.names = FALSE)))
}


#' Prepare display-only smoothers, stable scales and projected trajectory geometry once per clip.
#' @keywords internal
#' @noRd
.prepareOverlayContext <- function(clip, plan) {
  clip <- data.table::copy(clip)
  if ("vedba" %in% plan$metrics) {
    hz <- tryCatch(.estimateHz(clip$datetime), error = function(e) NA_real_)
    n <- if (is.finite(hz)) max(1L, as.integer(round(hz))) else 1L
    n <- min(n, nrow(clip))
    smooth <- data.table::frollmean(clip$vedba, n = n, align = "center", fill = NA_real_, na.rm = TRUE)
    smooth[!is.finite(smooth)] <- clip$vedba[!is.finite(smooth)]
    clip[, (".overlay_vedba") := smooth]
  }

  scalar <- intersect(plan$metrics, c("depth", "vedba", "vertical_velocity", "paddle_speed",
                                      "paddle_freq", "tbf_hz_peaks", "tbf_hz_wavelet"))
  ranges <- setNames(vector("list", length(scalar)), scalar)
  for (metric in scalar) {
    values <- if (metric == "vedba" && ".overlay_vedba" %in% names(clip)) clip$.overlay_vedba else clip[[metric]]
    ranges[[metric]] <- .overlayRange(values, metric)
  }
  trajectory <- if ("pseudo_trajectory" %in% plan$metrics) .preparePseudoTrajectory(clip) else NULL
  list(clip = clip, ranges = ranges, trajectory = trajectory)
}


#' Arrange modules into rows; dial trios share a row and focus support modules become tiles.
#' @keywords internal
#' @noRd
.presentationGroups <- function(metrics, dashboard) {
  if (dashboard == "focus") {
    rest <- if (length(metrics) > 1L) split(metrics[-1L], ceiling(seq_along(metrics[-1L]) / 3)) else list()
    return(list(metrics = c(list(metrics[1L]), unname(rest)),
                tiles = c(FALSE, rep(TRUE, length(rest)))))
  }
  groups <- list(); i <- 1L
  while (i <= length(metrics)) {
    if (metrics[i] %in% c("heading", "pitch", "roll")) {
      j <- i
      while (j < length(metrics) && metrics[j + 1L] %in% c("heading", "pitch", "roll")) j <- j + 1L
      groups[[length(groups) + 1L]] <- metrics[i:j]
      i <- j + 1L
    } else {
      groups[[length(groups) + 1L]] <- metrics[i]
      i <- i + 1L
    }
  }
  list(metrics = groups, tiles = rep(FALSE, length(groups)))
}


#' Relative row height for a full module.
#' @keywords internal
#' @noRd
.moduleHeight <- function(metric) {
  type <- .overlayMetricRegistry()[[metric]]$type
  switch(type, trajectory = 3.1, orientation = 2.1, dial = 2.1, 1.15)
}


#' Draw one registered module.
#' @keywords internal
#' @noRd
.drawMetricModule <- function(metric, fd, clip, current_time, theme, plan, context, model, motion) {
  spec <- .overlayMetricRegistry()[[metric]]
  if (spec$type %in% c("orientation", "dial")) graphics::par(mar = c(0.5, 3.1, 1.3, 0.8)) else
    if (spec$type == "trajectory") graphics::par(mar = c(0.5, 0.5, 1.3, 0.5)) else
    graphics::par(mar = c(1.2, 3.1, 1.9, 0.8))
  if (spec$type == "orientation")
    return(.drawPreparedAttitudeModel(fd, theme, model, motion, label = spec$label))
  if (spec$type == "dial") return(.drawDial(metric, .scalarValue(fd, metric), theme))
  if (spec$type == "trajectory")
    return(.drawPseudoTrajectory(context$trajectory, current_time, plan$windows[[metric]],
                                 .scalarValue(fd, "heading"), theme))
  if (spec$type == "vedba")
    return(.drawVedbaPanel(clip, current_time, plan$windows[[metric]], .scalarValue(fd, metric),
                           theme, context$ranges[[metric]]))
  if (spec$type == "vertical_velocity")
    return(.drawVerticalVelocityPanel(clip, current_time, plan$windows[[metric]], .scalarValue(fd, metric),
                                      theme, context$ranges[[metric]]))
  .drawSeriesPanel(clip, current_time, plan$windows[[metric]], metric, spec$label, spec$unit,
                   .scalarValue(fd, metric), theme, invert = spec$type == "depth", fill = spec$colour,
                   fixed.range = context$ranges[[metric]])
}


#' Draw a compact current-value tile for a focus-dashboard support metric.
#' @keywords internal
#' @noRd
.drawMetricTile <- function(metric, fd, theme) {
  spec <- .overlayMetricRegistry()[[metric]]
  graphics::par(mar = c(0.4, 0.4, 0.4, 0.4))
  plot(0, 0, type = "n", ann = FALSE, axes = FALSE, xlim = c(0, 1), ylim = c(0, 1))
  .drawPanelBackground(theme)
  if (metric == "orientation") {
    value <- sprintf("H %s\u00b0  P %s\u00b0  R %s\u00b0", .formatOverlayValue(.scalarValue(fd, "heading"), 0),
                     .formatOverlayValue(.scalarValue(fd, "pitch"), 0),
                     .formatOverlayValue(.scalarValue(fd, "roll"), 0))
  } else if (metric == "pseudo_trajectory") {
    value <- "3-D local track"
  } else {
    value <- paste(.formatOverlayValue(.scalarValue(fd, metric), 2), spec$unit)
  }
  graphics::text(0.06, 0.70, spec$label, adj = 0, col = theme$muted, cex = 0.75, font = 2)
  graphics::text(0.06, 0.35, value, adj = 0, col = theme$value, cex = 1.05, font = 2)
  invisible()
}


#' A prominent, auto-sized guidance header (the "what to look for" instruction) spanning the panel top.
#' @keywords internal
#' @noRd
.drawHeader <- function(caption, theme) {
  plot(0, 0, type = "n", ann = FALSE, axes = FALSE, xlim = c(0, 1), ylim = c(0, 1))
  .drawPanelBackground(theme)
  if (is.null(caption) || !nzchar(caption)) return(invisible())
  w <- graphics::strwidth(caption, units = "inches", cex = 1)
  cex <- if (w > 0) min(1.3, 0.97 * graphics::par("pin")[1] / w) else 1.1
  graphics::text(0.5, 0.5, caption, col = theme$text, font = 2, cex = cex, xpd = NA)
}



################################################################################
# Panel drawers (internal) #####################################################
################################################################################

#' Circular orientation gauge for heading / pitch / roll.
#' @keywords internal
#' @noRd
.drawDial <- function(metric, value, theme) {
  plot(0, 0, type = "n", xlim = c(-1.15, 1.15), ylim = c(-1.15, 1.15), axes = FALSE, ann = FALSE, asp = 1)
  .drawPanelBackground(theme)
  graphics::symbols(0, 0, circles = 1, inches = FALSE, add = TRUE,
                    bg = grDevices::adjustcolor("black", 0.62), fg = theme$muted)
  if (identical(metric, "heading")) {
    ang <- c(0, pi/2, pi, -pi/2) - pi/2
    graphics::text(-1.34 * cos(ang), -1.34 * sin(ang), c("N","W","S","E"), cex = 0.72, col = theme$text)
    for (a in ang) graphics::lines(c(0, -1.05 * cos(a)), c(0, 1.05 * sin(a)), col = "grey45", lwd = 0.5)
    if (is.finite(value)) {
      rad <- (90 - value) * pi/180
      graphics::arrows(cos(rad) * -0.62, sin(rad) * -0.62, cos(rad) * 0.62, sin(rad) * 0.62, col = "#ff3b30", lwd = 2.4, length = 0.13)
    }
  } else {
    grid_a <- if (identical(metric, "pitch")) seq(-90, 90, 30) else seq(-150, 180, 30)
    for (a in grid_a * pi/180) graphics::lines(c(0, -1.05 * cos(a)), c(0, 1.05 * sin(a)), col = "grey45", lwd = 0.5)
    for (a in grid_a) graphics::text(-1.30 * cos(a * pi/180), 1.30 * sin(a * pi/180), a, cex = 0.58, col = theme$text)
    if (is.finite(value)) {
      rad <- value * pi/180
      graphics::segments(0.62 * cos(rad), -0.62 * sin(rad), -0.62 * cos(rad), 0.62 * sin(rad), col = "#34c8c8", lwd = 2.6)
      graphics::points(0, 0, pch = 16, col = "#34c8c8", cex = 1.2)
    }
  }
  graphics::text(0, 1.74, tools::toTitleCase(metric), cex = 1.1, font = 2, xpd = NA, col = theme$text)
  graphics::text(0, 1.46, if (is.finite(value)) sprintf("%.0f\u00b0", value) else "NA", cex = 0.95, xpd = NA, col = theme$value)
}


#' Draw a consistently styled metric title and current value.
#'
#' The complete title is rendered in one explicit font and at one shared target size across scalar
#' modules. Long combinations are reduced only enough to fit the available plot width.
#' @keywords internal
#' @noRd
.drawMetricTitle <- function(label, value, theme, line = 0.65, cex = 1.02) {
  text <- paste0(label, "  \u00b7  ", value)
  available <- graphics::par("pin")[1] * 0.96
  width_at_target <- graphics::strwidth(text, units = "inches", cex = cex,
                                        family = "sans", font = 2)
  if (is.finite(width_at_target) && width_at_target > available)
    cex <- cex * available / width_at_target
  graphics::mtext(text, side = 3, line = line, at = mean(graphics::par("usr")[1:2]),
                  adj = 0.5, cex = cex, col = theme$text, family = "sans", font = 2)
  invisible()
}


#' Scrolling time-series panel with a clip-stable scale and the current value marked.
#' @keywords internal
#' @noRd
.drawSeriesPanel <- function(clip, current_time, win, col, label, unit, cur_val, theme, invert, fill,
                             fixed.range = NULL) {
  wd <- clip[abs(as.numeric(difftime(clip$datetime, current_time, units = "secs"))) <= win, ]
  y <- wd[[col]]
  rng <- if (is.null(fixed.range)) .overlayRange(y, if (col == "depth") "depth" else "series") else fixed.range
  ylim <- if (invert) c(rng[2], rng[1]) else rng
  xlim <- current_time + c(-win, win)
  plot(wd$datetime, y, type = "n", xlab = "", ylab = "", xaxs = "i", yaxs = "i",
       axes = FALSE, ylim = ylim, xlim = xlim)
  .drawPanelBackground(theme)
  usr <- graphics::par("usr"); base <- if (invert) usr[4] else usr[3]
  graphics::polygon(c(wd$datetime, rev(wd$datetime)), c(y, rep(base, nrow(wd))), col = grDevices::adjustcolor(fill, 0.35), border = NA)
  graphics::lines(wd$datetime, y, col = fill, lwd = 1.25)
  graphics::abline(v = current_time, col = grDevices::adjustcolor(theme$text, 0.35), lwd = 0.7)
  if (is.finite(cur_val)) graphics::points(current_time, cur_val, col = "#ff3b30", pch = 16, cex = 1.5)
  current_label <- if (is.finite(cur_val)) sprintf("%s %s", .formatOverlayValue(cur_val, 2), unit) else "NA"
  .drawMetricTitle(label, current_label, theme)
  graphics::axis(2, at = pretty(rng), las = 1, cex.axis = 0.65, col = "grey60", col.axis = theme$text)
  invisible()
}


#' VeDBA activity envelope: raw trace plus a one-second display-only smoother.
#' @keywords internal
#' @noRd
.drawVedbaPanel <- function(clip, current_time, win, cur_val, theme, fixed.range) {
  wd <- clip[abs(as.numeric(difftime(clip$datetime, current_time, units = "secs"))) <= win, ]
  smooth <- if (".overlay_vedba" %in% names(wd)) wd$.overlay_vedba else wd$vedba
  xlim <- current_time + c(-win, win)
  plot(wd$datetime, smooth, type = "n", xlab = "", ylab = "", xaxs = "i", yaxs = "i",
       axes = FALSE, ylim = fixed.range, xlim = xlim)
  .drawPanelBackground(theme)
  graphics::polygon(c(wd$datetime, rev(wd$datetime)), c(smooth, rep(0, nrow(wd))),
                    col = grDevices::adjustcolor("#ffd60a", 0.24), border = NA)
  graphics::lines(wd$datetime, wd$vedba, col = grDevices::adjustcolor(theme$muted, 0.48), lwd = 0.65)
  graphics::lines(wd$datetime, smooth, col = "#ffd60a", lwd = 1.8)
  graphics::abline(v = current_time, col = grDevices::adjustcolor(theme$text, 0.35), lwd = 0.7)
  if (is.finite(cur_val)) graphics::points(current_time, cur_val, col = "#ffffff", bg = "#ffd60a",
                                           pch = 21, cex = 1.3, lwd = 1.2)
  current_label <- if (is.finite(cur_val)) sprintf("%s g", .formatOverlayValue(cur_val, 3)) else "NA"
  .drawMetricTitle("VeDBA activity", current_label, theme)
  graphics::axis(2, at = pretty(fixed.range), las = 1, cex.axis = 0.65,
                 col = theme$muted, col.axis = theme$text)
  invisible()
}


#' Signed vertical velocity: ascent above, descent below, with a stable symmetric scale and zero line.
#' @keywords internal
#' @noRd
.drawVerticalVelocityPanel <- function(clip, current_time, win, cur_val, theme, fixed.range) {
  wd <- clip[abs(as.numeric(difftime(clip$datetime, current_time, units = "secs"))) <= win, ]
  displayed <- -wd$vertical_velocity
  ylim <- c(-fixed.range[2], -fixed.range[1])
  xlim <- current_time + c(-win, win)
  plot(wd$datetime, displayed, type = "n", xlab = "", ylab = "", xaxs = "i", yaxs = "i",
       axes = FALSE, ylim = ylim, xlim = xlim)
  .drawPanelBackground(theme)
  ascent <- pmax(displayed, 0)
  descent <- pmin(displayed, 0)
  graphics::polygon(c(wd$datetime, rev(wd$datetime)), c(ascent, rep(0, nrow(wd))),
                    col = grDevices::adjustcolor("#64d2ff", 0.38), border = NA)
  graphics::polygon(c(wd$datetime, rev(wd$datetime)), c(descent, rep(0, nrow(wd))),
                    col = grDevices::adjustcolor("#ff9f0a", 0.42), border = NA)
  ascent_line <- displayed; ascent_line[ascent_line < 0] <- NA_real_
  descent_line <- displayed; descent_line[descent_line > 0] <- NA_real_
  graphics::lines(wd$datetime, ascent_line, col = "#64d2ff", lwd = 1.5)
  graphics::lines(wd$datetime, descent_line, col = "#ff9f0a", lwd = 1.5)
  graphics::abline(h = 0, col = theme$text, lwd = 1.0)
  graphics::abline(v = current_time, col = grDevices::adjustcolor(theme$text, 0.35), lwd = 0.7)
  if (is.finite(cur_val)) graphics::points(current_time, -cur_val, col = "#ffffff",
                                           bg = if (cur_val >= 0) "#ff9f0a" else "#64d2ff",
                                           pch = 21, cex = 1.3, lwd = 1.2)
  direction <- if (!is.finite(cur_val) || abs(cur_val) < 1e-9) "" else if (cur_val > 0) " \u00b7 descent" else " \u00b7 ascent"
  current_label <- if (is.finite(cur_val))
    sprintf("%s m/s%s", .formatOverlayValue(cur_val, 2, signed = TRUE), direction) else "NA"
  .drawMetricTitle("Vertical velocity", current_label, theme)
  ticks <- pretty(ylim)
  graphics::axis(2, at = ticks, labels = .formatOverlayValue(-ticks, 1, signed = TRUE), las = 1,
                 cex.axis = 0.62, col = theme$muted, col.axis = theme$text)
  graphics::text(graphics::par("usr")[1], ylim[2] * 0.82, "ASCENT", adj = 0,
                 col = "#64d2ff", font = 2, cex = 0.62)
  graphics::text(graphics::par("usr")[1], ylim[1] * 0.82, "DESCENT", adj = 0,
                 col = "#ff9f0a", font = 2, cex = 0.62)
  invisible()
}


#' Project the whole pseudo-track once into a fixed local 3-D display coordinate system.
#' @keywords internal
#' @noRd
.preparePseudoTrajectory <- function(clip) {
  ok <- is.finite(clip$pseudo_lon) & is.finite(clip$pseudo_lat) & is.finite(clip$pseudo_depth)
  if (sum(ok) < 2L) return(list(available = FALSE))
  lon0 <- stats::median(clip$pseudo_lon[ok]); lat0 <- stats::median(clip$pseudo_lat[ok])
  xy <- .projLocal(clip$pseudo_lon, clip$pseudo_lat, lon0, lat0, 6371000)
  east <- xy$e; north <- xy$n; depth <- clip$pseudo_depth
  horizontal_span <- max(diff(range(east[ok])), diff(range(north[ok])), 1)
  depth_span <- diff(range(depth[ok]))
  vertical_exaggeration <- if (is.finite(depth_span) && depth_span > 0)
    min(8, max(1, 0.38 * horizontal_span / depth_span)) else 1

  # Fixed oblique camera: east runs mostly right, north recedes up-right, depth points down.
  screen_x <- 0.866 * east - 0.5 * north
  surface_y <- 0.25 * east + 0.433 * north
  screen_y <- surface_y - vertical_exaggeration * depth
  ok_screen <- ok & is.finite(screen_x) & is.finite(screen_y)
  time_step <- diff(as.numeric(clip$datetime))
  typical_step <- stats::median(time_step[is.finite(time_step) & time_step > 0], na.rm = TRUE)
  if (!is.finite(typical_step)) typical_step <- 1
  discontinuity <- c(TRUE, !head(ok_screen, -1L) | !tail(ok_screen, -1L) |
                       !is.finite(time_step) | time_step > max(2, 5 * typical_step))
  segment <- cumsum(discontinuity)
  xlim <- .paddedRange(c(screen_x[ok_screen], 0), 0.10)
  ylim <- .paddedRange(c(screen_y[ok_screen], surface_y[ok_screen], 0), 0.10)
  scale_m <- .niceScale(horizontal_span / 4)
  list(available = TRUE, datetime = clip$datetime, east = east, north = north, depth = depth,
       x = screen_x, y = screen_y, surface_y = surface_y, ok = ok_screen, xlim = xlim, ylim = ylim,
       segment = segment, vertical_exaggeration = vertical_exaggeration, scale_m = scale_m)
}


#' Draw a stable pseudo-trajectory with past/future context, current point and a surface tie-line.
#' @keywords internal
#' @noRd
.drawPseudoTrajectory <- function(track, current_time, win, heading, theme) {
  if (is.null(track) || !isTRUE(track$available)) {
    plot(0, 0, type = "n", axes = FALSE, ann = FALSE, xlim = c(0, 1), ylim = c(0, 1))
    .drawPanelBackground(theme)
    graphics::text(0.5, 0.5, "Pseudo-trajectory unavailable", col = theme$muted, cex = 0.9)
    return(invisible())
  }
  plot(0, 0, type = "n", axes = FALSE, ann = FALSE, xlim = track$xlim, ylim = track$ylim,
       xaxs = "i", yaxs = "i", asp = 1)
  .drawPanelBackground(theme)
  delta <- as.numeric(difftime(track$datetime, current_time, units = "secs"))
  past <- which(track$ok & delta >= -win & delta <= 0)
  future <- which(track$ok & delta > 0 & delta <= win)
  visible <- c(past, future)
  .drawSegmentedTrack(track$x, track$surface_y, visible, track$segment,
                      col = grDevices::adjustcolor("#64d2ff", 0.28), lwd = 0.8, lty = 2)
  .drawSegmentedTrack(track$x, track$y, future, track$segment, col = theme$muted, lwd = 1.2, lty = 3)
  .drawSegmentedTrack(track$x, track$y, past, track$segment, col = "#30d158", lwd = 2.6, lty = 1)
  current <- which.min(abs(delta))
  if (length(current) && track$ok[current]) {
    graphics::segments(track$x[current], track$surface_y[current], track$x[current], track$y[current],
                       col = grDevices::adjustcolor("#64d2ff", 0.55), lty = 2, lwd = 1)
    graphics::points(track$x[current], track$surface_y[current], pch = 1, cex = 0.7, col = "#64d2ff")
    graphics::points(track$x[current], track$y[current], pch = 21, cex = 1.45,
                     col = "white", bg = "#ff453a", lwd = 1.2)
    if (is.finite(heading)) {
      arrow_m <- max(track$scale_m * 0.6, 1)
      de <- sin(heading * pi / 180) * arrow_m
      dn <- cos(heading * pi / 180) * arrow_m
      graphics::arrows(track$x[current], track$y[current],
                       track$x[current] + 0.866 * de - 0.5 * dn,
                       track$y[current] + 0.25 * de + 0.433 * dn,
                       col = "#ffd60a", lwd = 1.8, length = 0.09)
    }
  }
  usr <- graphics::par("usr")
  sx <- usr[1] + diff(usr[1:2]) * 0.07; sy <- usr[3] + diff(usr[3:4]) * 0.09
  dx <- 0.866 * track$scale_m; dy <- 0.25 * track$scale_m
  graphics::segments(sx, sy, sx + dx, sy + dy, col = theme$text, lwd = 2)
  graphics::text(sx + dx / 2, sy + dy + diff(usr[3:4]) * 0.035,
                 sprintf("%s m", format(track$scale_m, trim = TRUE, scientific = FALSE)),
                 col = theme$text, cex = 0.62)
  graphics::text(usr[2] - diff(usr[1:2]) * 0.03, usr[4] - diff(usr[3:4]) * 0.06,
                 sprintf("Depth \u00d7%.1f", track$vertical_exaggeration), adj = 1,
                 col = theme$muted, cex = 0.65)
  graphics::legend("bottomright", legend = c("past", "future", "surface"),
                   col = c("#30d158", theme$muted, grDevices::adjustcolor("#64d2ff", 0.55)),
                   lty = c(1, 3, 2), lwd = c(2.2, 1.2, 0.8), bty = "n",
                   text.col = theme$text, cex = 0.54, horiz = TRUE, inset = 0.015)
  graphics::title(main = "3-D pseudo-trajectory", line = 0.5, cex.main = 1.15,
                  col.main = theme$text, xpd = NA)
  invisible()
}


#' Draw only within contiguous valid pseudo-track segments, never across a sensor gap.
#' @keywords internal
#' @noRd
.drawSegmentedTrack <- function(x, y, index, segment, col, lwd, lty) {
  if (length(index) < 2L) return(invisible())
  groups <- split(index, segment[index])
  for (group in groups)
    if (length(group) > 1L) graphics::lines(x[group], y[group], col = col, lwd = lwd, lty = lty)
  invisible()
}


#' Scrolling three-axis trace (e.g. body gyroscope or acceleration) with a now-marker and legend.
#' Axes whose column is absent are skipped; the panel degrades to a placeholder if none are present.
#' @keywords internal
#' @noRd
.drawTriTrace <- function(clip, current_time, win, cols, labels, title, theme, fd) {
  present <- cols %in% names(clip)
  line.cols <- c("#ff453a", "#34c8c8", "#ffd60a")
  wd <- clip[abs(as.numeric(difftime(clip$datetime, current_time, units = "secs"))) <= win, ]
  if (!any(present) || !nrow(wd)) {
    plot(0, 0, type = "n", axes = FALSE, ann = FALSE, xlim = c(0, 1), ylim = c(0, 1))
    .drawPanelBackground(theme)
    graphics::title(main = title, line = 1.5, cex.main = 1.15, col.main = theme$text, xpd = NA)
    graphics::text(0.5, 0.5, "unavailable", col = "grey55", cex = 0.95)
    return(invisible())
  }
  ys <- lapply(cols[present], function(c) wd[[c]])
  rng <- range(unlist(ys), na.rm = TRUE); if (!all(is.finite(rng))) rng <- c(-1, 1)
  plot(wd$datetime, ys[[1]], type = "n", xlab = "", ylab = "", xaxs = "i", axes = FALSE, ylim = rng)
  .drawPanelBackground(theme)
  graphics::abline(h = 0, col = "grey35", lwd = 0.6)
  cl <- line.cols[present]
  for (j in seq_along(ys)) graphics::lines(wd$datetime, ys[[j]], col = cl[j], lwd = 1.1)
  for (j in seq_along(ys)) {
    cv <- fd[[cols[present][j]]]
    if (is.finite(cv)) graphics::points(current_time, cv, col = cl[j], pch = 16, cex = 1.1)
  }
  graphics::title(main = title, line = 1.5, cex.main = 1.15, col.main = theme$text, xpd = NA)
  graphics::axis(2, at = pretty(rng), las = 1, cex.axis = 0.62, col = "grey60", col.axis = theme$text)
  graphics::legend("topright", legend = labels[present], col = cl, lwd = 1.6, bty = "n",
                   text.col = theme$text, cex = 0.62, horiz = TRUE, xpd = NA, inset = c(0, -0.06))
}


#' Bottom strip: timestamp only (general dashboard).
#' @keywords internal
#' @noRd
.drawTimestamp <- function(datetime, theme) {
  plot(0, 0, type = "n", ann = FALSE, axes = FALSE, xlim = c(0, 1), ylim = c(0, 1))
  .drawPanelBackground(theme)
  graphics::text(0.5, 0.52, format(datetime, "%Y-%m-%d %H:%M:%OS1", tz = "UTC"),
                 col = theme$text, cex = 1.0, font = 2, family = "mono")
}


#' Consume a deliberately empty layout row without painting over the source video.
#' @keywords internal
#' @noRd
.drawTransparentSpacer <- function() {
  plot(0, 0, type = "n", ann = FALSE, axes = FALSE, xlim = c(0, 1), ylim = c(0, 1))
  invisible()
}


#' Bottom strip: timestamp plus an optional caption line (validation dashboards).
#' @keywords internal
#' @noRd
.drawCaption <- function(datetime, caption, theme) {
  plot(0, 0, type = "n", ann = FALSE, axes = FALSE, xlim = c(0, 1), ylim = c(0, 1))
  .drawPanelBackground(theme)
  graphics::text(0.5, 0.72, format(datetime, "%Y-%m-%d %H:%M:%OS1", tz = "UTC"),
                 col = theme$text, cex = 1.0, font = 2, family = "mono")
  if (!is.null(caption) && nzchar(caption))
    graphics::text(0.5, 0.26, caption, col = "grey70", cex = 0.72, xpd = NA)
}



################################################################################
# Small helpers (internal) #####################################################
################################################################################

#' Complete a caller-supplied theme, preserving compatibility with older internal tests.
#' @keywords internal
#' @noRd
.completeOverlayTheme <- function(theme) {
  if (is.null(theme$bg)) theme$bg <- "#080b0e"
  if (is.null(theme$canvas)) theme$canvas <- theme$bg
  if (is.null(theme$module)) theme$module <- grDevices::adjustcolor(theme$bg, 0.96)
  if (is.null(theme$border)) theme$border <- grDevices::adjustcolor("white", 0.25)
  if (is.null(theme$text)) theme$text <- "grey92"
  if (is.null(theme$muted)) theme$muted <- "grey65"
  if (is.null(theme$value)) theme$value <- "#ff453a"
  if (is.null(theme$overlay)) theme$overlay <- FALSE
  theme
}


#' Theme for opaque beside panels or transparent on-video modules.
#' @keywords internal
#' @noRd
.overlayTheme <- function(composition = "beside", background.alpha = 0.72) {
  if (composition == "overlay") {
    .completeOverlayTheme(list(canvas = "transparent", bg = "transparent",
                               module = grDevices::adjustcolor("#071017", background.alpha),
                               border = grDevices::adjustcolor("white", min(0.42, background.alpha)),
                               text = "#f5f7fa", muted = "#aab4bd", value = "#ffffff",
                               overlay = TRUE))
  } else {
    .completeOverlayTheme(list(canvas = "#070b0f", bg = "#070b0f", module = "#101820",
                               border = "#293744", text = "#f5f7fa", muted = "#9aa6b2",
                               value = "#ffffff", overlay = FALSE))
  }
}


#' Paint the current panel's local backplate.
#' @keywords internal
#' @noRd
.drawPanelBackground <- function(theme) {
  usr <- graphics::par("usr")
  graphics::rect(usr[1], usr[3], usr[2], usr[4], col = theme$module,
                 border = theme$border, lwd = 0.7, xpd = FALSE)
  invisible()
}


#' A robust clip-stable y range for a dashboard metric.
#' @keywords internal
#' @noRd
.overlayRange <- function(x, metric = "series") {
  x <- x[is.finite(x)]
  if (!length(x)) return(if (metric == "vertical_velocity") c(-1, 1) else c(0, 1))
  if (metric == "vedba") return(c(0, max(0.05, unname(stats::quantile(x, 0.995, na.rm = TRUE)))))
  if (metric == "vertical_velocity") {
    lim <- max(0.05, unname(stats::quantile(abs(x), 0.995, na.rm = TRUE)))
    return(c(-lim, lim))
  }
  probs <- if (length(x) > 20L) c(0.005, 0.995) else c(0, 1)
  rng <- as.numeric(stats::quantile(x, probs, na.rm = TRUE, names = FALSE))
  if (metric != "depth" && rng[1] >= 0) rng[1] <- 0
  .paddedRange(rng, 0.06)
}


#' Pad a numeric range and safely expand constant inputs.
#' @keywords internal
#' @noRd
.paddedRange <- function(x, fraction = 0.08) {
  rng <- range(x[is.finite(x)], na.rm = TRUE)
  if (!all(is.finite(rng))) return(c(0, 1))
  span <- diff(rng)
  if (!is.finite(span) || span <= 0) span <- max(abs(rng), 1) * 0.1
  rng + c(-1, 1) * span * fraction
}


#' Select a readable metric scale length no larger than the requested target.
#' @keywords internal
#' @noRd
.niceScale <- function(target) {
  if (!is.finite(target) || target <= 0) return(1)
  power <- 10^floor(log10(target))
  choices <- c(1, 2, 5, 10) * power
  max(choices[choices <= target])
}


#' Return one finite scalar from a dashboard row, otherwise NA.
#' @keywords internal
#' @noRd
.scalarValue <- function(fd, name) {
  value <- fd[[name]]
  if (is.null(value) || !length(value) || !is.finite(value[1])) NA_real_ else as.numeric(value[1])
}


#' Format a numeric dashboard value without leaking NaN/Inf.
#' @keywords internal
#' @noRd
.formatOverlayValue <- function(x, digits = 1, signed = FALSE) {
  ans <- rep("NA", length(x))
  ok <- is.finite(x)
  fmt <- if (signed) paste0("%+.", digits, "f") else paste0("%.", digits, "f")
  ans[ok] <- sprintf(fmt, x[ok])
  ans
}


#' Return an even positive dimension for codecs that require chroma-aligned frames.
#' @keywords internal
#' @noRd
.evenDimension <- function(x) {
  x <- max(2L, as.integer(round(x)))
  x - (x %% 2L)
}


#' Distinct body-glyph colours for up to several candidate mappings.
#' @keywords internal
#' @noRd
.candidatePalette <- function(n) {
  pal <- c("#ff453a", "#34c8c8", "#ffd60a", "#bf5af2", "#30d158")
  pal[((seq_len(max(n, 1)) - 1L) %% length(pal)) + 1L]
}
