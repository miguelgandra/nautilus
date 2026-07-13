#######################################################################################################
# Held-out fix cross-validation of reconstructTrack ###################################################
#######################################################################################################

#' Cross-validate a reconstructed track against its own position fixes
#'
#' @description
#' Quantifies how accurately \code{\link{reconstructTrack}} predicts position, by leave-one-out
#' cross-validation against the deployment's genuine position fixes. Each fix in turn is withheld, the track
#' is reconstructed from the remaining anchors (so neither the Verified Position Correction nor the VeDBA
#' speed calibration sees the withheld fix), and the reconstructed position at that time is compared with the
#' true fix. The result is a per-fix table of held-out errors - the closest thing to ground-truth accuracy
#' available for an underwater track, since GPS/Argos fixes are the only true positions and they exist only
#' at the surface.
#'
#' @details
#' ## Why this is the headline validation
#' A marine pseudo-track has no underwater ground truth, so it cannot be validated directly between fixes.
#' The one thing you *can* do is withhold a known fix and ask how close the reconstruction lands to it using
#' only the other information - exactly the strategy used to validate dead-reckoned whale tracks against
#' sparse Fastloc-GPS (Wensveen et al. 2015). The withheld error at a fix `t` seconds from the nearest
#' retained fix is a direct, honest estimate of the track's accuracy over a `t`-second reckoning gap.
#'
#' ## What it reports, and how to read it
#' For every withheld fix the function records the great-circle `error_m` between the reconstructed and the
#' true position, the `gap_h` (hours to the nearest retained fix), whether the fix was *interpolated*
#' (retained anchors both before and after) or *extrapolated* (only on one side, e.g. the pop-up), and the
#' fix's own quality radius for context. The key diagnostic is **error vs gap**: if it grows roughly linearly
#' with the gap, its slope is an empirical drift rate in **m/h** - divide by 3600 to get the `drift.rate`
#' (m/s) for \code{\link{reconstructTrackControl}} (the summary line prints this m/s value for you). Run it
#' with `control` set to different `vpc.method` / `speed.method` and stack the results to see which settings
#' actually help on *your* data.
#'
#' ## Honest limitations
#' The held-out error \strong{conflates} heading error, speed error and unmodelled current - it tells you the
#' pipeline's net accuracy, not which component failed (use the magnetometer-vs-geomagnetic-model and
#' gyro-vs-magnetometer checks to isolate those). It can only be computed where a deployment has genuine
#' surfacing fixes beyond the deployment origin; sparse surfacing means few points per deployment, so pool
#' across the fleet. A withheld fix carries its own measurement error (tens of m for FastGPS, km for coarse
#' Argos), which sets a floor on the achievable `error_m`.
#'
#' @param data The processed data (output of \code{\link{processTagData}}): a `nautilus_tag` / data.frame, a
#'   (named) list of them, a single aggregated data.frame with an `id.col`, or a character vector of `.rds`
#'   paths. Must carry the columns \code{\link{reconstructTrack}} needs, and have genuine position fixes.
#' @param control A \code{\link{reconstructTrackControl}} object (or a named list of its fields); the same
#'   settings whose accuracy you want to assess.
#' @param id.col,datetime.col Column names for the animal ID and timestamp. Defaults `"ID"` / `"datetime"`.
#' @param plot,plot.file Draw the diagnostic report (error-vs-gap scatter + error distribution) to the active
#'   device and/or a PDF. Defaults `FALSE` / `NULL`.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet" (silent), `TRUE`/`1`/"normal" (header + a live progress
#'   bar across deployments + summary), or `2`/"detailed" (default; streams a per-deployment block - fixes
#'   validated, median error - instead of the bar). cli auto-hides the bar for fast runs.
#'
#' @return A `data.frame`, one row per withheld fix, with columns `id`, `datetime`, `quality`, `error_m`,
#'   `gap_h`, `interpolated`, `fix_radius_m`, `n_anchors_used`, `speed_method`, `vpc_method`. Empty if no
#'   deployment has a fix to withhold.
#' @references
#' Wensveen PJ, Thomas L, Miller PJO (2015) A path reconstruction method integrating dead-reckoning and
#' position fixes applied to humpback whales. *Movement Ecology*. 3:31. \doi{10.1186/s40462-015-0061-6}
#' @seealso \code{\link{reconstructTrack}}, \code{\link{reconstructTrackControl}}
#' @examples
#' \dontrun{
#' cv <- crossValidateTrack(processed, plot = TRUE)
#' # compare correction methods on your own data:
#' methods <- c("none", "error_weighted", "scale_rotate")
#' comp <- do.call(rbind, lapply(methods, function(m)
#'   crossValidateTrack(processed, reconstructTrackControl(vpc.method = m), verbose = FALSE)))
#' aggregate(error_m ~ vpc_method, comp, median)
#' }
#' @export
crossValidateTrack <- function(data,
                               control = reconstructTrackControl(),
                               id.col = "ID",
                               datetime.col = "datetime",
                               plot = FALSE,
                               plot.file = NULL,
                               verbose = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  control <- .as_control(control, reconstructTrackControl, "nautilus_reconstruct_track", "control")
  .assert_flag(plot, "plot"); .assert_writable_file(plot.file, "plot.file", ext = "pdf")
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")

  r <- .resolveInput(data, id.col = id.col)
  make_plots <- plot || !is.null(plot.file)
  .log_header(lvl, "crossValidateTrack", "Held-out fix cross-validation",
              bullets = sprintf("Input: %d dataset%s", r$n, if (r$n != 1) "s" else ""),
              arrow = sprintf("%s DR + %s VPC", control$speed.method, control$vpc.method))

  out <- vector("list", r$n)
  pb <- .log_progress_start(lvl, r$n, "Cross-validating", min.level = 1L, max.level = 1L)   # NORMAL only (detailed streams)
  for (i in seq_len(r$n)) {
    .log_progress_step(pb)
    x <- r$get(i)
    if (!data.table::is.data.table(x)) x <- data.table::as.data.table(x)
    who <- tryCatch(as.character(unique(x[[id.col]])[1]), error = function(e) NA_character_)
    if (length(who) != 1L || is.na(who) || !nzchar(who)) who <- r$ids[i]
    .log_h2(lvl, sprintf("%s (%d/%d)", who, i, r$n))

    tab <- tryCatch(.crossValidateOne(x, control, datetime.col, who),
                    error = function(e) { .log_skip(lvl, conditionMessage(e)); NULL })
    if (is.null(tab) || !nrow(tab)) {
      .log_detail(lvl, "no fixes to withhold (needs a genuine fix or pop-up beyond the deployment origin)")
    } else {
      out[[i]] <- tab
      .log_detail(lvl, sprintf("%d fix%s validated - median error %.0f m", nrow(tab),
                               if (nrow(tab) != 1) "es" else "", stats::median(tab$error_m, na.rm = TRUE)))
    }
    .log_gap(lvl); rm(x)
  }
  .log_progress_done(pb)

  kept <- Filter(Negate(is.null), out)
  res <- if (length(kept)) as.data.frame(data.table::rbindlist(kept)) else
    data.frame(id = character(), datetime = as.POSIXct(character()), quality = character(), error_m = numeric(),
               gap_h = numeric(), interpolated = logical(), fix_radius_m = numeric(), n_anchors_used = integer(),
               speed_method = character(), vpc_method = character(), stringsAsFactors = FALSE)

  if (lvl >= 1L) {
    .log_summary(lvl)
    if (nrow(res)) {
      gp <- res$gap_h > 0 & is.finite(res$gap_h)
      drift <- if (any(gp)) stats::median(res$error_m[gp] / res$gap_h[gp], na.rm = TRUE) else NA_real_
      .log_done(lvl, sprintf("%d fix%s cross-validated across %d deployment%s", nrow(res),
                             if (nrow(res) != 1) "es" else "", length(unique(res$id)),
                             if (length(unique(res$id)) != 1) "s" else ""))
      .log_arrow(lvl, sprintf("median error %.0f m (90th pct %.0f m) - drift ~ %.0f m/h (drift.rate ~ %.2f m/s)",
                              stats::median(res$error_m), stats::quantile(res$error_m, 0.9, names = FALSE),
                              drift, drift / 3600))
    } else {
      .log_done(lvl, "no fixes available for cross-validation")
    }
    .log_runtime(lvl, start.time)
  }

  if (make_plots && nrow(res)) {
    draw <- function(to.file = FALSE, unicode = TRUE) .drawCrossValidation(res, control)
    .renderToDevices(draw, plot = plot, plot.file = plot.file, width = 10, height = 6)
  }
  res
}

#' Leave-one-out cross-validation for a single deployment: withhold each genuine fix (and the pop-up) in
#' turn, reconstruct from the rest, and score the reconstructed position against the withheld fix.
#' @keywords internal
#' @noRd
.crossValidateOne <- function(dt, control, datetime.col, id) {
  dtp <- .withPositionColumns(data.table::as.data.table(dt))
  meta <- .getMeta(dtp)
  deploy_lat <- meta$deployment$lat; deploy_lon <- meta$deployment$lon
  if (is.null(deploy_lat) || is.null(deploy_lon) || is.na(deploy_lat) || is.na(deploy_lon)) return(NULL)
  if (!all(c(datetime.col, "heading", "pitch") %in% names(dtp))) return(NULL)

  pos_anchors <- .positionAnchors(dtp, meta, datetime.col, deploy_lat, deploy_lon)
  holdable <- pos_anchors[quality != "Deploy"]          # pop-up + genuine GPS/Argos fixes (never the origin)
  if (!nrow(holdable)) return(NULL)
  atime <- as.numeric(pos_anchors$time)

  rows <- vector("list", nrow(holdable))
  for (k in seq_len(nrow(holdable))) {
    h <- holdable$idx[k]
    res <- tryCatch(.reconstructTrackOne(data.table::copy(dtp), control, datetime.col, lvl = 0L, id = id,
                                         make_plots = FALSE, holdout = h),
                    error = function(e) NULL)
    if (is.null(res)) next
    pred_lon <- res$pseudo_lon[h]; pred_lat <- res$pseudo_lat[h]
    if (!is.finite(pred_lon) || !is.finite(pred_lat)) next
    err_m <- .trackDistance(pred_lon, pred_lat, holdable$lon[k], holdable$lat[k]) * 1000
    h_t <- as.numeric(holdable$time[k]); other_t <- atime[pos_anchors$idx != h]
    gap_h <- if (length(other_t)) min(abs(other_t - h_t)) / 3600 else NA_real_
    q <- as.character(holdable$quality[k])
    rows[[k]] <- data.frame(
      id = id, datetime = holdable$time[k], quality = q, error_m = err_m, gap_h = gap_h,
      interpolated = any(other_t < h_t) && any(other_t > h_t),
      fix_radius_m = .anchorRadius(q, control$anchor.error.radii),
      n_anchors_used = nrow(pos_anchors) - 1L,
      speed_method = control$speed.method, vpc_method = control$vpc.method, stringsAsFactors = FALSE)
  }
  data.table::rbindlist(Filter(Negate(is.null), rows))
}

#' Diagnostic page: held-out error vs the reckoning gap (with an empirical drift slope), plus the error
#' distribution split by interpolated / extrapolated fixes.
#' @keywords internal
#' @noRd
.drawCrossValidation <- function(res, control) {
  oldpar <- graphics::par(no.readonly = TRUE); on.exit({ graphics::layout(1); graphics::par(oldpar) }, add = TRUE)
  IN <- "#1565c0"; EX <- "#e08a00"
  graphics::layout(matrix(c(1, 1, 2, 3), nrow = 2L, byrow = TRUE), heights = c(0.32, 1))

  gp <- res$gap_h > 0 & is.finite(res$gap_h)
  drift <- if (any(gp)) stats::median(res$error_m[gp] / res$gap_h[gp], na.rm = TRUE) else NA_real_
  graphics::par(mar = c(0.2, 1, 0.4, 1)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
  graphics::text(0, 0.72, "crossValidateTrack  -  held-out fix accuracy", adj = c(0, 0.5), font = 2, cex = 1.4)
  graphics::text(0, 0.24, sprintf("%d fixes / %d deployments   |   median %.0f m, 90th pct %.0f m   |   drift ~ %.0f m/h (%.2f m/s)   |   %s / %s",
                 nrow(res), length(unique(res$id)), stats::median(res$error_m),
                 stats::quantile(res$error_m, 0.9, names = FALSE), drift, drift / 3600,
                 control$speed.method, control$vpc.method),
                 adj = c(0, 0.5), cex = 0.9, col = "grey30")

  # --- error vs gap (the drift diagnostic) ---
  graphics::par(mar = c(3.7, 3.9, 1.8, 1), mgp = c(2.4, 0.7, 0), cex.axis = 0.85)
  cols <- ifelse(res$interpolated, IN, EX)
  graphics::plot(res$gap_h, res$error_m, pch = 19, cex = 0.9, col = grDevices::adjustcolor(cols, 0.7),
                 xlab = "gap to nearest retained fix (h)", ylab = "held-out error (m)",
                 main = "error vs reckoning gap", font.main = 1, cex.main = 1.0)
  if (sum(gp) >= 2L) {                                     # drift-through-origin slope (m per hour)
    sl <- stats::coef(stats::lm(error_m ~ 0 + gap_h, data = res[gp, , drop = FALSE]))[[1]]
    graphics::abline(0, sl, col = "grey45", lty = 2)
    graphics::mtext(sprintf("slope ~ %.0f m/h", sl), side = 3, line = -1.1, adj = 0.98, cex = 0.7, col = "grey45")
  }
  graphics::legend("topleft", bty = "n", cex = 0.75, pch = 19, col = c(IN, EX),
                   legend = c("interpolated", "extrapolated"))

  # --- error distribution (ECDF) ---
  graphics::par(mar = c(3.7, 3.9, 1.8, 1))
  e <- sort(res$error_m[is.finite(res$error_m)])
  if (length(e)) {
    graphics::plot(e, seq_along(e) / length(e), type = "s", col = IN, lwd = 1.8,
                   xlab = "held-out error (m)", ylab = "cumulative fraction", main = "error distribution",
                   font.main = 1, cex.main = 1.0, ylim = c(0, 1))
    graphics::abline(v = stats::median(e), col = "grey45", lty = 3)
    graphics::mtext(sprintf("median %.0f m", stats::median(e)), side = 3, line = -1.1, adj = 0.98, cex = 0.7, col = "grey45")
  } else { graphics::plot.new(); graphics::text(0.5, 0.5, "no finite errors", col = "grey50") }
  invisible(NULL)
}
