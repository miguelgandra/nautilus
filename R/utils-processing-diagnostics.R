#######################################################################################################
# processTagData() diagnostic rendering ###############################################################
#######################################################################################################
#
# High-fidelity, opt-in diagnostics for the corrections processTagData() applies. The heavy processing
# stays headless by default; when the caller supplies `plot.file`, processTagData GATHERS a small,
# decimated per-deployment bundle while the raw data is in memory, and hands the bundles here to be
# RENDERED into a single multi-page PDF (one summary page + two pages per deployment). Gather and render
# are deliberately separate: processTagData never draws, and this file never processes.
#
# Panels (build order): (1) magnetometer calibration - REUSES the calibrateMagnetometer detail page so
# the raw cloud is shown collapsing onto a sphere; (2) depth zero-offset drift; (3) pitch/roll offsets.


#' Capture a decimated magnetometer-calibration diagnostic bundle
#'
#' Taken inside processTagData() at the moment the (inline or stored) hard/soft-iron transform is applied,
#' while the raw cloud is still in memory. `mag_raw` is the cloud the calibration operated on; `center` /
#' `soft_iron` are whatever was actually applied (an identity transform - center 0, soft_iron I - encodes
#' "no calibration applied", so the panel shows the uncorrected band and the reason in `source`).
#'
#' @param mag_raw n x 3 raw magnetometer matrix (the calibration input).
#' @param accel n x 3 raw acceleration matrix (for the geomagnetic-dip reference), or NULL.
#' @param center length-3 applied hard-iron centre.
#' @param soft_iron 3 x 3 applied soft-iron matrix (identity when not applied).
#' @param coverage_ok Did the orientation-coverage gate pass?
#' @param source Human-readable provenance label (shown on the panel).
#' @param igrf `list(intensity, inclination)` from `.magIGRF()`, or NULL.
#' @param fs Sampling frequency (Hz), for the gravity low-pass.
#' @param target.n Decimation budget for the scatter cloud.
#' @return A bundle list (cloud + applied transform + QC), or NULL when there is too little mag data.
#' @keywords internal
#' @noRd
.captureMagDiag <- function(mag_raw, accel = NULL, center, soft_iron, coverage_ok, source, igrf,
                            fs = NA_real_, target.n = 4000L, confidence = NA_character_, status = NA_character_) {
  M <- as.matrix(mag_raw)
  if (sum(stats::complete.cases(M)) < 200L) return(NULL)
  n <- nrow(M)
  idx <- seq(1L, n, by = max(1L, n %/% as.integer(target.n)))
  grav <- NULL
  if (!is.null(accel) && is.finite(fs) && fs > 0) {
    A <- as.matrix(accel); w <- max(2L, as.integer(round(2 * fs)))                # ~2 s static-accel low-pass
    lp <- function(v) data.table::frollmean(v, w, fill = NA, align = "center")
    grav <- cbind(lp(A[, 1]), lp(A[, 2]), lp(A[, 3]))[idx, , drop = FALSE]
  }
  cloud     <- list(mag = M[idx, , drop = FALSE], grav = grav)
  corrected <- .applyMagCal(cloud$mag, center, soft_iron)
  igrf_incl <- if (is.null(igrf)) NA_real_ else igrf$inclination
  list(cloud = cloud, center = center, soft_iron = soft_iron, coverage_ok = isTRUE(coverage_ok),
       coverage  = .magCalCoverage(sweep(cloud$mag, 2, center, "-")),   # true equal-area sphere-coverage fraction
       source = source, igrf = igrf, confidence = confidence, status = status,   # the engine's verdict (not recomputed)
       radcv     = .magRadcv(corrected),
       dip_resid = .magDipResidual(corrected, cloud$grav, igrf_incl)$residual,
       applied   = !(sum(abs(center)) == 0 && isTRUE(all.equal(unname(soft_iron), diag(3)))))
}


#' Capture a decimated depth zero-offset-drift diagnostic bundle
#'
#' Taken inside processTagData() around the drift correction, before `depth` is overwritten. The corrected
#' depth is in `drift_res$depth`, the raw is the un-overwritten `raw_depth`; the offset the correction
#' subtracted is `drift_res$offset` (raw - corrected). The offset curve is thus visible as the gap between
#' the raw and corrected traces, and the surface anchors that pinned it come from `drift_res$anchor_table`.
#'
#' @param raw_depth Depth vector BEFORE the correction (the correction input).
#' @param datetime POSIXct vector, same length.
#' @param drift_res Return value of `.correctDepthDrift()`.
#' @param target.n Decimation budget for the depth traces.
#' @return A bundle list, or NULL when the correction was disabled.
#' @keywords internal
#' @noRd
.captureDepthDiag <- function(raw_depth, datetime, drift_res, target.n = 2000L) {
  if (is.null(drift_res) || identical(drift_res$status, "disabled")) return(NULL)
  n <- length(raw_depth)
  idx <- seq(1L, n, by = max(1L, n %/% as.integer(target.n)))
  a <- drift_res$anchor_table
  anchors <- if (!is.null(a) && nrow(a) > 0)
               data.frame(t = .POSIXct(a$time, "UTC"), offset_m = a$offset, source = a$source,
                          stringsAsFactors = FALSE)
             else data.frame(t = .POSIXct(numeric(0), "UTC"), offset_m = numeric(0),
                             source = character(0), stringsAsFactors = FALSE)
  list(t = datetime[idx], raw = raw_depth[idx], corrected = drift_res$depth[idx], offset = drift_res$offset[idx],
       anchors = anchors, status = drift_res$status, residual_m = drift_res$outcome$residual_m,
       offset_range_m = drift_res$outcome$offset_range_m, n_anchors = drift_res$n_anchors,
       low_confidence = drift_res$low_confidence)
}


#' Draw the depth zero-offset-drift panel (raw vs corrected + surface anchors) into the current region
#' @keywords internal
#' @noRd
.drawDepthDriftPanel <- function(d, use_unicode = TRUE) {
  RAWC <- "grey58"; CALC <- "#1565c0"; GRID <- "grey88"; BG <- "grey97"
  GAPFILL <- grDevices::adjustcolor("#c62828", 0.10)                          # light-red low-confidence shading
  srccol <- c(dry = "#a11a1a", gps = "#2e7d32", depth = "#6a1b9a")            # dry = dark red (distinct from blue trace)
  yr <- range(c(d$raw, d$corrected), na.rm = TRUE)
  if (!all(is.finite(yr))) yr <- c(0, 1)
  graphics::par(mar = c(3.4, 3.8, 3.4, 1.0))                                  # extra top room for the two-line title
  graphics::plot(d$t, d$corrected, type = "n", ylim = rev(yr), xlab = "", ylab = "depth (m)")
  usr <- graphics::par("usr")
  graphics::rect(usr[1], usr[3], usr[2], usr[4], col = BG, border = NA)       # light-grey panel fill
  graphics::grid(col = GRID)
  has_gap <- !is.null(d$low_confidence) && nrow(d$low_confidence) > 0
  if (has_gap)                                                                # low-confidence spans (wide anchor gaps)
    for (k in seq_len(nrow(d$low_confidence)))
      graphics::rect(d$low_confidence$start[k], yr[2], d$low_confidence$end[k], yr[1], col = GAPFILL, border = NA)
  graphics::lines(d$t, d$raw, col = RAWC, lwd = 1)                            # raw (uncorrected)
  graphics::lines(d$t, d$corrected, col = CALC, lwd = 1.5)                    # corrected (gap = offset subtracted)
  graphics::abline(h = 0, col = "grey55", lty = 3)                           # the surface
  if (nrow(d$anchors) > 0) {                                                  # surface anchors, coloured by source
    ac <- unname(srccol[d$anchors$source]); ac[is.na(ac)] <- "grey40"
    graphics::points(d$anchors$t, rep(0, nrow(d$anchors)), pch = 25, bg = ac, col = ac, cex = 1.0)
  }
  off <- d$offset_range_m
  cap <- sprintf("%s  |  offset %.2f to %.2f m  |  surface residual %s  |  %d anchor%s", d$status,
                 off[1], off[2], if (isTRUE(is.finite(d$residual_m))) sprintf("%.2f m", d$residual_m) else "n/a",
                 d$n_anchors, if (d$n_anchors != 1L) "s" else "")
  graphics::mtext("Depth zero-offset drift correction", side = 3, line = 1.6, adj = 0, font = 2, cex = 1.0)
  graphics::mtext(cap, side = 3, line = 0.45, adj = 0, cex = 0.8, col = "grey30")   # subtitle, clear of the title
  leg  <- c("raw", "corrected", paste("anchor:", names(srccol)), if (has_gap) "low-confidence (gap)")
  graphics::legend("bottomright", bty = "n", cex = 0.72, legend = leg,
                   col   = c(RAWC, CALC, unname(srccol), if (has_gap) GAPFILL),
                   lwd   = c(1, 1.5, NA, NA, NA, if (has_gap) NA),
                   pch   = c(NA, NA, 25, 25, 25, if (has_gap) 15),
                   pt.bg = c(NA, NA, unname(srccol), if (has_gap) NA),
                   pt.cex = c(NA, NA, 1, 1, 1, if (has_gap) 1.6))
  graphics::box()                                                            # frame, matching the other panels
  if (identical(d$status, "abstained"))
    graphics::mtext("depth left untouched - insufficient surface evidence", side = 1, line = -1.5,
                    col = "#c62828", font = 2, cex = 0.9)
  invisible()
}


#' Capture the pitch mounting-offset (Kawatsu) fit for the diagnostic panel
#'
#' `fit_data` holds the diving samples the fit used (`vv_smooth`, `pitch_rad`, PRE-correction); the line is
#' `pitch_model`. Both are captured before processTagData() cleans them up. The intercept (offset) is kept
#' even when the gate rejected it, so the panel can show the fit that was declined and why.
#' @keywords internal
#' @noRd
.capturePitchDiag <- function(fit_data, pitch_model, r2, applied, min_r2, threshold, note, target.n = 2000L) {
  if (is.null(fit_data) || !nrow(fit_data)) return(NULL)
  ns <- nrow(fit_data); si <- seq(1L, ns, by = max(1L, ns %/% as.integer(target.n)))
  slope <- if (!is.null(pitch_model)) unname(stats::coef(pitch_model)[2]) else NA_real_          # d(pitch_rad)/d(vv)
  intercept_deg <- if (!is.null(pitch_model)) unname(stats::coef(pitch_model)[1]) * (180 / pi) else NA_real_
  list(vv = fit_data$vv_smooth[si], pitch_deg = fit_data$pitch_rad[si] * (180 / pi),
       slope_deg = slope * (180 / pi), intercept_deg = intercept_deg, r2 = r2, applied = isTRUE(applied),
       min_r2 = min_r2, threshold_deg = threshold, note = note)
}


#' Capture the roll mounting-offset (median over the most-level swimming) for the diagnostic panel
#' @keywords internal
#' @noRd
.captureRollDiag <- function(level_roll, median_deg, applied, threshold, target.n = 2000L) {
  level_roll <- level_roll[is.finite(level_roll)]
  if (!length(level_roll)) return(NULL)
  ns <- length(level_roll); si <- seq(1L, ns, by = max(1L, ns %/% as.integer(target.n)))
  list(level_roll = level_roll[si], median_deg = median_deg, applied = isTRUE(applied), threshold_deg = threshold)
}


#' A blank panel carrying a short "not available" note (keeps the page-B grid consistent)
#' @keywords internal
#' @noRd
.drawEmptyPanel <- function(msg) {
  graphics::par(mar = c(1, 1, 1, 1)); graphics::plot.new()
  graphics::text(0.5, 0.5, msg, col = "grey55", cex = 0.9); invisible()
}


#' Draw the Kawatsu pitch-offset scatter (vertical velocity x pitch, with the fitted line) into the region
#' @keywords internal
#' @noRd
.drawPitchScatterPanel <- function(p, use_unicode = TRUE) {
  if (is.null(p) || !any(is.finite(p$vv)) || !any(is.finite(p$pitch_deg)))   # no plottable range -> placeholder
    return(.drawEmptyPanel("Pitch offset: not estimated"))
  GRID <- "grey88"; BG <- "grey97"; PTS <- grDevices::adjustcolor("grey40", 0.35); LINE <- "#1565c0"
  graphics::par(mar = c(4.2, 4.0, 3.4, 1.0))                                  # +top (2-line title), +bottom (y labels)
  graphics::plot(p$vv, p$pitch_deg, type = "n", xlab = "vertical velocity (m/s)", ylab = "pitch (deg)")
  usr <- graphics::par("usr"); graphics::rect(usr[1], usr[3], usr[2], usr[4], col = BG, border = NA)
  graphics::grid(col = GRID)
  graphics::points(p$vv, p$pitch_deg, pch = 16, cex = 0.4, col = PTS)
  if (is.finite(p$slope_deg) && is.finite(p$intercept_deg))
    graphics::abline(a = p$intercept_deg, b = p$slope_deg, col = LINE, lwd = 2)   # pitch_deg = a + b * vv
  graphics::abline(v = 0, col = "grey70", lty = 3)
  cap <- if (is.finite(p$intercept_deg))
           sprintf("offset %+.2f deg (R2 %.2f, min %.2f) - %s", p$intercept_deg, p$r2, p$min_r2,
                   if (p$applied) "applied" else "gated")
         else "insufficient diving signal"
  graphics::mtext("Pitch mounting-offset (Kawatsu fit)", side = 3, line = 1.6, adj = 0, font = 2, cex = 1.0)
  graphics::mtext(cap, side = 3, line = 0.5, adj = 0, cex = 0.75, col = if (isTRUE(p$applied)) "#2e7d32" else "#c62828")
  graphics::box()
  invisible()
}


#' Draw the roll-offset panel (level-swimming roll distribution + the subtracted median) into the region
#' @keywords internal
#' @noRd
.drawRollPanel <- function(r, use_unicode = TRUE) {
  if (is.null(r)) return(.drawEmptyPanel("Roll offset: not estimated"))
  BG <- "grey97"
  graphics::par(mar = c(4.2, 4.0, 3.4, 1.0))                                  # +top (2-line title), +bottom (y labels)
  h <- graphics::hist(r$level_roll, breaks = 40, plot = FALSE)
  graphics::plot(h, main = "", xlab = "roll, level swimming (deg)", ylab = "Frequency", col = NA, border = NA)  # axes first
  usr <- graphics::par("usr"); graphics::rect(usr[1], usr[3], usr[2], usr[4], col = BG, border = NA)
  graphics::grid(col = "grey88")
  graphics::plot(h, add = TRUE, col = "grey85", border = "grey60")           # bars over the fill + grid
  graphics::abline(v = r$median_deg, col = if (isTRUE(r$applied)) "#2e7d32" else "#c62828", lwd = 2)
  graphics::abline(v = c(-r$threshold_deg, r$threshold_deg), col = "grey70", lty = 3)   # anomaly threshold
  graphics::mtext("Roll mounting-offset", side = 3, line = 1.6, adj = 0, font = 2, cex = 1.0)
  graphics::mtext(sprintf("offset %+.2f deg - %s", r$median_deg, if (isTRUE(r$applied)) "applied" else "gated"),
                  side = 3, line = 0.5, adj = 0, cex = 0.75, col = if (isTRUE(r$applied)) "#2e7d32" else "#c62828")
  graphics::box()                                                            # frame, matching the other panels
  invisible()
}


#' Reduce one diagnostic bundle to the scalar record shown on the fleet summary page
#' @keywords internal
#' @noRd
.processingDiagRecord <- function(b, radcv.max = 0.1, dip.max = 15) {
  m <- b$mag; d <- b$depth; pr <- b$pitchroll
  pit <- if (!is.null(pr)) pr$pitch else NULL; rol <- if (!is.null(pr)) pr$roll else NULL
  list(id            = b$id,
       mag_source    = if (!is.null(m)) m$source else "no magnetometer",
       mag_conf      = if (!is.null(m)) (m$confidence %||% .magConfidence(m$radcv, m$dip_resid, m$coverage_ok, radcv.max, dip.max)) else NA_character_,
       mag_off       = if (!is.null(m)) sqrt(sum(m$center^2)) else NA_real_,
       depth_status  = if (!is.null(d)) d$status else "disabled",
       depth_resid   = if (!is.null(d)) d$residual_m else NA_real_,
       pitch_off     = if (!is.null(pit)) pit$intercept_deg else NA_real_,
       pitch_r2      = if (!is.null(pit)) pit$r2 else NA_real_,
       pitch_applied = if (!is.null(pit)) isTRUE(pit$applied) else NA,
       roll_off      = if (!is.null(rol)) rol$median_deg else NA_real_,
       roll_applied  = if (!is.null(rol)) isTRUE(rol$applied) else NA)
}


#' Draw the fleet summary page: one row per deployment x the three corrections, sorted worst-first
#' @keywords internal
#' @noRd
.drawProcessingSummaryPage <- function(bundles, use_unicode = TRUE, radcv.max = 0.1, dip.max = 15) {
  oldpar <- graphics::par(no.readonly = TRUE); on.exit(graphics::par(oldpar), add = TRUE)
  graphics::layout(1)
  recs <- lapply(bundles, .processingDiagRecord, radcv.max = radcv.max, dip.max = dip.max)

  confcol  <- function(c) .magConfColour(c)
  depthcol <- function(s) switch(as.character(s), applied = "#2e7d32", constant_offset = "#2e7d32",
                                 applied_with_gaps = "#e08a00", abstained = "#c62828", "grey55")
  appcol   <- function(a) if (isTRUE(a)) "#2e7d32" else if (isFALSE(a)) "#e08a00" else "grey55"
  ell <- if (use_unicode) "\u2026" else ".."
  truncr <- function(s, k) if (is.na(s)) "-" else if (nchar(s) > k) paste0(substr(s, 1, k - 1L), ell) else s

  attn <- function(r) identical(r$mag_conf, "low") + (r$depth_status %in% c("abstained", "applied_with_gaps")) +
                       isFALSE(r$pitch_applied) + isFALSE(r$roll_applied)
  if (length(recs)) recs <- recs[order(-vapply(recs, attn, numeric(1)))]                 # worst-first
  n <- length(recs)
  cnt <- function(f) sum(vapply(recs, f, logical(1)))
  n_low <- cnt(function(r) identical(r$mag_conf, "low")); n_abs <- cnt(function(r) identical(r$depth_status, "abstained"))
  n_pg  <- cnt(function(r) isFALSE(r$pitch_applied))

  cx  <- c(id = 0.004, mag = 0.20, depth = 0.44, pitch = 0.68, roll = 0.87)
  hdr <- c(id = "Deployment", mag = "Magnetometer", depth = "Depth drift", pitch = "Pitch offset", roll = "Roll offset")
  hcy <- 0.855
  header <- function() {
    graphics::par(mar = c(1.5, 0.6, 1, 0.6)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
    graphics::text(0, 0.985, "processTagData  -  correction diagnostics", adj = c(0, 1), font = 2, cex = 1.4)
    graphics::text(1, 0.985, format(Sys.time(), "%Y-%m-%d %H:%M"), adj = c(1, 1), cex = 0.75, col = "grey40")
    graphics::text(0, 0.935, sprintf("%d deployment%s  |  %d low-confidence mag   %d depth abstained   %d pitch gated",
                   n, if (n != 1) "s" else "", n_low, n_abs, n_pg), adj = c(0, 1), cex = 0.95)
    graphics::text(0, 0.905, "sorted worst-first  -  green = applied/high, amber = gated/medium, red = abstained/low",
                   adj = c(0, 1), cex = 0.72, col = "grey45")
    graphics::rect(0, hcy - 0.014, 1, hcy + 0.014, col = "grey90", border = NA)
    for (k in names(cx)) graphics::text(cx[[k]], hcy, hdr[[k]], adj = c(0, 0.5), font = 2, cex = 0.62, col = "grey15")
    graphics::segments(0, hcy - 0.016, 1, hcy - 0.016, col = "grey55")
  }

  rpp <- 38L; npg <- max(1L, ceiling(max(1L, n) / rpp))
  for (pg in seq_len(npg)) {
    header()
    idx <- if (!n) integer(0) else ((pg - 1L) * rpp + 1L):min(pg * rpp, n)
    row_top <- hcy - 0.030; dy <- 0.0205
    for (j in seq_along(idx)) {
      r <- recs[[idx[j]]]; y <- row_top - dy * (j - 1L)
      if (j %% 2L == 0L) graphics::rect(0, y - dy / 2, 1, y + dy / 2, col = "grey96", border = NA)
      graphics::text(cx[["id"]], y, truncr(r$id, 16), adj = c(0, 0.5), cex = 0.6, font = 2, col = "grey20")
      msrc <- if (grepl("^stored", r$mag_source)) "stored" else if (grepl("^inline", r$mag_source)) "inline" else
              if (grepl("no calibration", r$mag_source)) "none" else if (grepl("no magnetometer", r$mag_source)) "no mag" else "-"
      graphics::text(cx[["mag"]], y, if (is.na(r$mag_conf)) msrc else sprintf("%s / %s", msrc, r$mag_conf),
                     adj = c(0, 0.5), cex = 0.58, col = confcol(r$mag_conf))
      graphics::text(cx[["depth"]], y, sprintf("%s  %s", r$depth_status,
                     if (is.finite(r$depth_resid)) sprintf("%.2fm", r$depth_resid) else ""),
                     adj = c(0, 0.5), cex = 0.58, col = depthcol(r$depth_status))
      ptxt <- if (isTRUE(r$pitch_applied)) sprintf("%+.2f (R2 %.2f)", r$pitch_off, r$pitch_r2)
              else if (isFALSE(r$pitch_applied)) sprintf("gated (%s)", if (is.finite(r$pitch_off)) sprintf("%+.1f", r$pitch_off) else "weak")
              else "-"
      graphics::text(cx[["pitch"]], y, ptxt, adj = c(0, 0.5), cex = 0.56, col = appcol(r$pitch_applied))
      graphics::text(cx[["roll"]], y, if (isTRUE(r$roll_applied)) sprintf("%+.2f", r$roll_off) else if (isFALSE(r$roll_applied)) "gated" else "-",
                     adj = c(0, 0.5), cex = 0.56, col = appcol(r$roll_applied))
    }
    if (!n) graphics::text(0.5, 0.5, "no deployments to report", cex = 1, col = "grey50")
    if (npg > 1) graphics::mtext(sprintf("summary page %d / %d", pg, npg), side = 1, cex = 0.7, col = "grey50")
  }
  invisible(NULL)
}


#' Render the processTagData diagnostic bundles into a multi-page PDF (or active device)
#'
#' Reuses the calibrateMagnetometer detail page (`.plotMagCalIndividual`) for the magnetometer panel so the
#' cloud-onto-sphere view is identical to the standalone tool. Depth-drift and pitch/roll panels (page B)
#' and the fleet summary page are added in later build steps.
#'
#' @param bundles A list of per-deployment bundles (each `list(id, paddle, mag = <.captureMagDiag>)`).
#' @param plot Logical; draw to the active device.
#' @param plot.file Single multi-page PDF path, or NULL.
#' @param width,height Device size (inches).
#' @param radcv.max,dip.max Confidence thresholds (radius CV; IGRF dip residual, deg).
#' @keywords internal
#' @noRd
.renderProcessingDiagnostic <- function(bundles, plot = FALSE, plot.file = NULL,
                                        width = 10, height = 7.5, radcv.max = 0.1, dip.max = 15) {
  bundles <- Filter(function(b) !is.null(b) && !is.null(b$id), bundles)
  if (!length(bundles)) return(invisible(NULL))
  # one page-set per deployment id: if the same id was processed more than once (e.g. a duplicate input
  # file sharing an ID), keep only the last so the report never repeats a deployment (summary + pages).
  ids <- vapply(bundles, function(b) as.character(b$id), character(1))
  bundles <- bundles[!duplicated(ids, fromLast = TRUE)]

  draw <- function(to.file = FALSE, unicode = TRUE) {
    uni <- unicode                                                 # Unicode only where the target device supports it
    .drawProcessingSummaryPage(bundles, use_unicode = uni, radcv.max = radcv.max, dip.max = dip.max)  # page 1
    for (b in bundles) {
      m <- b$mag
      if (!is.null(m)) {                                           # page A: magnetometer calibration (reused)
        conf <- m$confidence %||% .magConfidence(m$radcv, m$dip_resid, m$coverage_ok, radcv.max, dip.max)
        cal  <- list(center = m$center, soft_iron = m$soft_iron, radcv = m$radcv,
                     coverage_ok = m$coverage_ok, confidence = conf, method = "processTagData",
                     source = m$source, group = NA_character_, n_deployments = NA_integer_)
        p <- .magCalPayload(b$id, m$cloud, cal, m$igrf, paddle = isTRUE(b$paddle),
                            coverage = m$coverage, radcv.max = radcv.max, dip.max = dip.max)  # true fraction, not the gate
        .plotMagCalIndividual(p, use_unicode = uni)
      }
      if (!is.null(b$depth) || !is.null(b$pitchroll)) {            # page B: depth (top) + pitch/roll (bottom)
        graphics::par(oma = c(0, 0, 2.4, 0))
        graphics::layout(matrix(c(1, 1, 2, 3), nrow = 2L, byrow = TRUE), heights = c(1.1, 1.05))
        if (!is.null(b$depth)) .drawDepthDriftPanel(b$depth, use_unicode = uni)
        else                   .drawEmptyPanel("Depth drift: correction disabled")
        pr <- b$pitchroll
        .drawPitchScatterPanel(if (!is.null(pr)) pr$pitch else NULL, use_unicode = uni)
        .drawRollPanel(if (!is.null(pr)) pr$roll else NULL, use_unicode = uni)
        graphics::mtext(b$id, outer = TRUE, side = 3, line = 0.6, at = 0.01, adj = 0, font = 2, cex = 1.5)  # top-left, matches page A
        graphics::par(oma = c(0, 0, 0, 0)); graphics::layout(1)
      }
    }
  }
  .renderToDevices(draw, plot = plot, plot.file = plot.file, width = width, height = height, cairo = TRUE)
}
