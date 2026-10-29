# Deployment interval refinement and diagnostic plotting.

# filterDeploymentData helpers ----

#######################################################################################################
# Internal: temperature corroboration #################################################################
#######################################################################################################

# Strictly-additive temperature refinement of a depth-based deployment window. `temp` is the 1 Hz
# temperature vector (NA in the padded ends); `attach`/`pop` are the depth-based window indices (NA when
# depth found nothing); `found` says whether depth found a deployment; `min_run` is the minimum sustained
# in-water length (samples) required to trust temperature. Returns a list(attach, pop, valid) or NULL
# (leave the depth result unchanged). It can only widen or rescue a window, never shrink one.
#' @keywords internal
#' @noRd
.refineWithTemperature <- function(temp, attach, pop, found, min_run,
                                   base_win = 300L, tol_floor = 1.0, k = 4) {
  n <- length(temp)
  if (n < 3L) return(NULL)

  # out-of-water reference: when depth gave a window, use the regions outside it; otherwise the first
  # and last `base_win` FINITE samples (assumed to bracket the boat / float-and-recovery phases). Using
  # finite samples skips the NA-filled padding at the series ends.
  if (found && is.finite(attach) && is.finite(pop) && attach > 1L && pop < n) {
    out_idx <- c(seq_len(attach - 1L), (pop + 1L):n)
    base <- temp[out_idx]; base <- base[is.finite(base)]
  } else {
    fin <- which(is.finite(temp))
    if (length(fin) < 10L) return(NULL)
    w <- min(base_win, length(fin) %/% 3L)
    base <- temp[c(utils::head(fin, w), utils::tail(fin, w))]
  }
  if (length(base) < 5L) return(NULL)

  # a sample is "in water" if its temperature departs from the out-of-water baseline by more than a
  # robust tolerance (at least `tol_floor` degrees, or k robust SDs of the baseline)
  T0 <- stats::median(base); scale <- stats::mad(base)
  tol <- max(tol_floor, k * scale)
  in_water <- is.finite(temp) & abs(temp - T0) >= tol
  if (!any(in_water)) return(NULL)

  # require a sustained in-water run before trusting temperature at all
  run <- .longestTrueRun(in_water)
  if (is.null(run) || (run[2] - run[1] + 1L) < min_run) return(NULL)

  if (found) {
    # extend the depth window outward across contiguous in-water samples (shallow in-water edges)
    a <- attach; p <- pop
    while (a > 1L && isTRUE(in_water[a - 1L])) a <- a - 1L
    while (p < n  && isTRUE(in_water[p + 1L])) p <- p + 1L
    list(attach = a, pop = p, valid = TRUE)
  } else {
    # rescue: depth found nothing, but temperature shows a sustained in-water period
    list(attach = run[1], pop = run[2], valid = TRUE)
  }
}

# Start/end indices of the longest contiguous run of TRUE in a logical vector (NA treated as FALSE);
# NULL if there are none.
#' @keywords internal
#' @noRd
.longestTrueRun <- function(x) {
  x[is.na(x)] <- FALSE
  if (!any(x)) return(NULL)
  r <- rle(x)
  ends <- cumsum(r$lengths); starts <- ends - r$lengths + 1L
  tr <- which(r$values)
  best <- tr[which.max(r$lengths[tr])]
  c(starts[best], ends[best])
}

#######################################################################################################
# Internal: metric axis labels ########################################################################
#######################################################################################################

# Human-readable "Name (unit)" labels for the diagnostic-plot metrics, generated from a lookup of the
# standard nautilus channels (units as produced by processTagData). Unknown / user-derived columns
# fall back to the raw column name. Vectorised over `col`.
#' @keywords internal
#' @noRd
.metricLabel <- function(col) {
  map <- c(
    ax = "Acc X (g)", ay = "Acc Y (g)", az = "Acc Z (g)",
    gx = "Gyro X (rad/s)", gy = "Gyro Y (rad/s)", gz = "Gyro Z (rad/s)",
    mx = "Mag X (\u00b5T)", my = "Mag Y (\u00b5T)", mz = "Mag Z (\u00b5T)",
    depth = "Depth (m)", temp = "Temperature (\u00b0C)",
    pitch = "Pitch (\u00b0)", roll = "Roll (\u00b0)", yaw = "Yaw (\u00b0)", heading = "Heading (\u00b0)",
    vedba = "VeDBA (g)", odba = "ODBA (g)", odba_smooth = "ODBA (g)", vedba_smooth = "VeDBA (g)",
    speed = "Speed (m/s)", paddle_speed = "Paddle speed (m/s)", paddle_freq = "Paddle freq (Hz)")
  lab <- unname(map[tolower(col)])
  ifelse(is.na(lab), col, lab)
}

#######################################################################################################
# Internal: deployment-filtering diagnostic panel #####################################################
#######################################################################################################

# One page per individual, designed for fast visual validation of the inferred deployment window:
#   * a title row with the ID and a colour-coded method/outcome BADGE (automatic / custom / discarded)
#     plus a two-line inline stat block (attached / released / duration / retained) with full datetimes;
#   * an inverted depth panel where the retained window is the figure (coloured) and the discarded ends
#     recede (grey); the retained boundaries (solid red) and the metadata deploy/release times (dashed
#     green) are marked and explained by a legend; optional Wildlife Computers fixes sit along the top;
#   * the detection-relevant sensor strips (temperature first by default), sharing a dated x-axis that
#     includes the YEAR; and
#   * a footer caption stating the data span and the metadata deploy/release times.
# The x-axis is clipped to the real data span (the 1 h zero-depth changepoint padding is excluded).
# `pd` is the list assembled in filterDeploymentData().

#' @keywords internal
#' @noRd
.drawDeploymentPanel <- function(pd) {

  d  <- pd$data
  tt <- d[[pd$datetime.col]]
  n  <- nrow(d)
  ai <- pd$attach_idx; pp <- pd$pop_idx
  has_window <- isTRUE(pd$valid) && is.finite(ai) && is.finite(pp) && ai <= n && pp <= n && ai < pp
  kept <- if (has_window) c(tt[ai], tt[pp]) else NULL
  # A window that was detected and then rejected on duration is drawn too, but in the DISCARD palette
  # and with dashed bounds: the reader has to be able to see the shape that was thrown away without
  # ever mistaking it for something that was kept.
  rej <- if (!is.null(pd$rejected)) c(pd$rejected$from, pd$rejected$to) else NULL
  xlim <- c(pd$first_datetime, pd$last_datetime)   # real data span (excludes internal padding)

  # palette
  col_kept <- "#185FA5"; col_disc <- "#9AA0A6"; band <- "#378ADD22"; col_meta <- "#1D9E75"
  col_auto <- "#A32D2D"; col_custom <- "#E8A33D"          # retained boundary, coloured by method
  rej_col  <- "#B23A3A"; rej_band <- "#B23A3A18"          # detected-then-rejected window (discard red)
  col_fast <- "#2AA7A0"; col_user <- "#7E57C2"            # location-fix tones (distinct from depth blue)
  badge_col <- if (!has_window) "#B23A3A" else if (identical(pd$method_kind, "custom")) "#E8A33D" else "#1D9E75"
  attach_col  <- if (identical(pd$attach_method,  "custom")) col_custom else col_auto
  release_col <- if (identical(pd$release_method, "custom")) col_custom else col_auto

  fmt_dt <- function(x) if (length(x) && is.finite(x)) strftime(x, "%Y-%m-%d %H:%M", tz = "UTC") else "NA"

  nmet <- length(pd$metrics)
  has_fast <- isTRUE(pd$positions_available) && !is.null(pd$fastloc_pos) && nrow(pd$fastloc_pos) > 0
  has_user <- isTRUE(pd$positions_available) && !is.null(pd$user_pos)    && nrow(pd$user_pos)    > 0
  have_pos <- has_fast || has_user

  # layout: header, [location strip], depth, metric strips, footer
  heights <- c(1.15, if (have_pos) 0.26, 2.2, rep(1.0, nmet), 0.34)
  graphics::layout(matrix(seq_along(heights), ncol = 1), heights = heights)

  # ---- title + status badge + inline stats (labels same size as values; rows kept close with clear
  #      space below, and the attach/release labels live inside the depth panel, never up here) -------
  graphics::par(mar = c(0.2, 4.4, 0.4, 1.2))
  graphics::plot(0:1, 0:1, type = "n", axes = FALSE, xlab = "", ylab = "", xaxs = "i", yaxs = "i")
  graphics::text(0, 0.86, pd$id, adj = c(0, 0.5), font = 2, cex = 1.7)
  btxt <- if (!has_window) paste0("DISCARDED  \u00b7  ", pd$method_label)
          else paste0(toupper(pd$method_kind), "  \u00b7  ", pd$method_label)
  bx0 <- graphics::strwidth(pd$id, cex = 1.7) + graphics::strwidth("  ", cex = 1.7)
  bw  <- graphics::strwidth(btxt, cex = 0.92) + graphics::strwidth("MM", cex = 0.92)
  graphics::rect(bx0, 0.72, bx0 + bw, 1.0, col = badge_col, border = NA)
  graphics::text(bx0 + bw / 2, 0.86, btxt, col = "white", font = 2, cex = 0.92, adj = c(0.5, 0.5))

  draw_pairs <- function(prs, ly) {
    x <- 0; cx <- 1.0                               # label and value share the same size
    for (k in seq_along(prs)) {
      lab <- prs[[k]][1]; val <- prs[[k]][2]
      graphics::text(x, ly, lab, adj = c(0, 0.5), cex = cx, col = col_disc); x <- x + graphics::strwidth(lab, cex = cx) + graphics::strwidth(" ", cex = cx)
      graphics::text(x, ly, val, adj = c(0, 0.5), cex = cx, font = 2);        x <- x + graphics::strwidth(val, cex = cx)
      if (k < length(prs)) { sep <- "      \u00b7     "; graphics::text(x, ly, sep, adj = c(0, 0.5), cex = cx, col = col_disc); x <- x + graphics::strwidth(sep, cex = cx) }
    }
  }
  if (has_window) {
    draw_pairs(list(c("Attached", fmt_dt(pd$attachtime)), c("Released", fmt_dt(pd$poptime))), 0.52)
    draw_pairs(list(c("Duration", pd$deploy_duration),
                    c("Retained", sprintf("%.0f%% (%s)", pd$deploy_percentage, .formatLargeNumber(pd$kept_rows)))), 0.30)
  } else {
    if (is.null(pd$rejected)) {
      draw_pairs(list(c("Status", "no deployment detected"),
                      c("Discarded", paste0(.formatLargeNumber(pd$rows_discarded), " rows"))), 0.40)
    } else {
      draw_pairs(list(c("Detected", paste0(fmt_dt(pd$rejected$from), "  ", cli::symbol$arrow_right, "  ",
                                           strftime(pd$rejected$to, "%H:%M", tz = "UTC"))),
                      c("Duration", sprintf("%.2f h  (floor %g h)", pd$rejected$hours, pd$min_hours))), 0.52)
      draw_pairs(list(c("Status", "rejected on duration; nothing retained"),
                      c("Discarded", paste0(.formatLargeNumber(pd$rows_discarded), " rows"))), 0.30)
    }
  }

  # helper: draw a series with the retained window shaded, kept (colour) over discarded (grey), plus
  # the metadata (dashed green) reference lines and the retained boundaries coloured by their method.
  panel <- function(y, ylab, ylim = NULL, lwd = 1.0, draw_axis = FALSE) {
    if (is.null(ylim)) ylim <- range(y, na.rm = TRUE)
    graphics::plot(tt, y, type = "n", xlim = xlim, ylim = ylim, xaxt = "n", xlab = "", ylab = ylab,
                   las = 1, cex.axis = 0.95, cex.lab = 1.05)
    if (has_window) graphics::rect(kept[1], graphics::par("usr")[3], kept[2], graphics::par("usr")[4], col = band, border = NA)
    if (!is.null(rej)) {
      graphics::rect(rej[1], graphics::par("usr")[3], rej[2], graphics::par("usr")[4], col = rej_band, border = NA)
      graphics::abline(v = rej, col = rej_col, lty = 2, lwd = 1.4)
    }
    graphics::lines(tt, y, col = col_disc, lwd = lwd * 0.8)
    if (has_window) {
      keep <- tt >= kept[1] & tt <= kept[2]
      graphics::lines(tt[keep], y[keep], col = col_kept, lwd = lwd)
    } else if (!is.null(rej)) {
      inr <- tt >= rej[1] & tt <= rej[2]
      graphics::lines(tt[inr], y[inr], col = rej_col, lwd = lwd)
    }
    if (is.finite(pd$meta_deploy))  graphics::abline(v = pd$meta_deploy,  col = col_meta, lty = 2, lwd = 1.0)
    if (is.finite(pd$meta_release)) graphics::abline(v = pd$meta_release, col = col_meta, lty = 2, lwd = 1.0)
    if (has_window) {
      graphics::abline(v = kept[1], col = attach_col,  lty = 1, lwd = 1.4)
      graphics::abline(v = kept[2], col = release_col, lty = 1, lwd = 1.4)
    }
    graphics::box(col = "#CCCCCC")
    if (draw_axis)
      # `xlim` is built with c(), which has not always preserved the timezone attribute; .axisTime
      # resolves the zone explicitly and falls back to UTC rather than to the analyst's session
      .axisTime(xlim, n = 6, tz = attr(tt, "tzone"), fmt = "%d %b %Y\n%H:%M",
                cex.axis = 0.82, padj = 0.5)
  }

  # ---- location strip (narrow band above depth; FastGPS / User distinguished by colour) -------
  if (have_pos) {
    graphics::par(mar = c(0.15, 4.4, 0.15, 1.2))
    graphics::plot(xlim, c(0, 1), type = "n", xlim = xlim, ylim = c(0, 1), axes = FALSE, xlab = "", ylab = "")
    if (has_window) graphics::rect(kept[1], 0, kept[2], 1, col = band, border = NA)
    if (has_fast) graphics::points(pd$fastloc_pos[[pd$datetime.col]], rep(0.5, nrow(pd$fastloc_pos)), pch = 16, col = paste0(col_fast, "CC"), cex = 1.04)
    if (has_user) graphics::points(pd$user_pos[[pd$datetime.col]],    rep(0.5, nrow(pd$user_pos)),    pch = 16, col = paste0(col_user, "CC"), cex = 1.04)
    graphics::box(col = "#CCCCCC")
    graphics::mtext("fixes", side = 2, las = 1, line = 0.4, cex = 0.7, col = col_disc)
  }

  # ---- depth panel (inverted) -----------------------------------------------------------------
  graphics::par(mar = c(0.5, 4.4, 0.6, 1.2), mgp = c(2.7, 0.7, 0))
  dmax <- max(d[[pd$depth.col]], na.rm = TRUE)
  dmin <- min(-2, min(d[[pd$depth.col]], na.rm = TRUE))
  headroom <- 0.10 * (dmax - dmin)              # dedicated space above the trace for the boundary labels
  ytop <- dmin - headroom
  panel(d[[pd$depth.col]], "Depth (m)", ylim = c(dmax, ytop), lwd = 1.1)
  if (has_window) {
    y_lab <- ytop + 0.5 * headroom              # centred in the headroom band, always above the depth trace
    graphics::text(kept[1], y_lab, "attach",  col = attach_col,  cex = 0.92, font = 2, pos = 4, offset = 0.2)
    graphics::text(kept[2], y_lab, "release", col = release_col, cex = 0.92, font = 2, pos = 2, offset = 0.2)
  }

  # legend: boundaries by method, metadata reference, location tones (filled circles)
  lab <- character(0); lty <- integer(0); lcol <- character(0); lpch <- integer(0)
  add <- function(l, ty, co, pc = NA_integer_) { lab <<- c(lab, l); lty <<- c(lty, ty); lcol <<- c(lcol, co); lpch <<- c(lpch, pc) }
  if (has_window) {
    if (attach_col == col_auto   || release_col == col_auto)   add("automatic boundary", 1L, col_auto)
    if (attach_col == col_custom || release_col == col_custom) add("custom boundary",    1L, col_custom)
  }
  if (!is.null(rej)) add("detected, then rejected", 2L, rej_col)
  if (is.finite(pd$meta_deploy) || is.finite(pd$meta_release)) add("metadata deploy/release", 2L, col_meta)
  if (has_fast) add("FastGPS fix",   NA_integer_, col_fast, 16L)
  if (has_user) add("User position", NA_integer_, col_user, 16L)
  if (length(lab)) graphics::legend("bottomright", legend = lab, lty = lty, col = lcol, pch = lpch,
                                    bty = "o", bg = "#FFFFFFCC", box.col = "#CCCCCC", cex = 0.8, seg.len = 1.6, inset = 0.01)

  # ---- metric strips --------------------------------------------------------------------------
  for (v in seq_along(pd$metrics)) {
    last <- v == nmet
    graphics::par(mar = c(if (last) 2.9 else 0.5, 4.4, 0.6, 1.2))
    if (pd$metrics[v] %in% names(d)) panel(d[[pd$metrics[v]]], pd$metric.labels[v], lwd = 0.9, draw_axis = last)
    else { graphics::plot.new(); graphics::box(col = "#CCCCCC") }
  }

  # ---- footer caption (rest of the deployment timeline; data span and metadata clearly separated) ---
  graphics::par(mar = c(0.2, 4.4, 0.2, 1.2))
  graphics::plot(0:1, 0:1, type = "n", axes = FALSE, xlab = "", ylab = "")
  graphics::text(0, 0.5, adj = c(0, 0.5), cex = 0.82, col = "#555555",
                 labels = paste0("Data span  ", fmt_dt(pd$first_datetime), " -> ", fmt_dt(pd$last_datetime),
                                 "          |          Metadata deploy  ", fmt_dt(pd$meta_deploy),
                                 "   |   Metadata release  ", fmt_dt(pd$meta_release)))
  invisible(NULL)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################


#' The fixed-schema table of deployments that left without data.
#'
#' Zero rows rather than NULL when nothing was set aside, so a caller can bind, filter and count it
#' without first testing whether anything went wrong - the same contract the issues table uses.
#'
#' `detected_start`/`detected_end`/`detected_duration_h` are populated only for the one reason that HAS
#' a window: a deployment found and then rejected for being shorter than `min.deployment.hours`. That
#' distinction is the point of the table. A deployment where nothing was detected at all has no window
#' to report, and inventing one would make the two indistinguishable.
#' @keywords internal
#' @noRd
