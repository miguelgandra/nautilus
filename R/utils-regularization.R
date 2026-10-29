# Regularization diagnostics, classification and reporting.

# regularizeTimeSeries helpers ----

#######################################################################################################
# Internal: regularization diagnostic panel ###########################################################
#######################################################################################################

# Default review thresholds (dimensionless metrics generalise across sampling rates; the absolute
# `large_gap_seconds` trigger is off by default). User overrides are merged on top.
# @keywords internal
# @noRd
#' Estimate a channel's own sampling cadence, in grid rows per observation
#'
#' A tag may log a channel more slowly than the inertial grid it is being placed on - CEiiA writes
#' `Velocity (m/s)` and `Ticks/s` once a second against a 20 Hz grid, leaving 19 of every 20 rows empty for
#' those columns. Those empty rows are the channel's DESIGN, not missing data, so they must not be counted
#' as gaps or filled by interpolation.
#'
#' The cadence is the median spacing between consecutive observations, which is robust to real dropouts
#' (a handful of long gaps cannot move the median) and returns 1 for any channel sampled on every row.
#' @param x A numeric channel, possibly containing NAs.
#' @return Median rows per observation; 1 when the channel is sampled at (or near) the grid rate, or when
#'   there are too few observations to judge - the conservative answer, since it preserves existing
#'   behaviour rather than silently disabling interpolation.
#' @keywords internal
#' @noRd
.channelCadence <- function(x) {
  idx <- which(!is.na(x))
  if (length(idx) < 3L) return(1)
  s <- stats::median(diff(idx))
  if (!is.finite(s) || s < 1) 1 else s
}

.regularizationThresholds <- function(user = NULL) {
  d <- list(gap_pct_review = 1, gap_pct_critical = 5,
            interp_pct_review = 5, interp_pct_critical = 20,
            rows_added_pct_review = 10, rows_added_pct_critical = 100,
            large_gap_seconds = NULL)
  if (!is.null(user)) {
    if (!is.list(user) || is.null(names(user))) .abort("{.arg review.thresholds} must be a named list.")
    unknown <- setdiff(names(user), names(d))
    if (length(unknown)) .abort("Unknown {.arg review.thresholds} field{?s}: {.val {unknown}}.")
    for (nm in names(user)) d[[nm]] <- user[[nm]]
  }
  d
}

# Gap statistics from the final NA mask (single rle pass). Durations in seconds; run start/end
# indices retained for the coverage markers and the targeted zoom.
#' @keywords internal
#' @noRd
.regularizationGapStats <- function(na_final, nominal_interval) {
  r <- rle(na_final)
  ends <- cumsum(r$lengths); starts <- ends - r$lengths + 1L
  g <- r$values
  durs <- r$lengths[g] * nominal_interval
  list(n_gaps = sum(g), durations_s = durs, starts = starts[g], ends = ends[g],
       largest_s = if (length(durs)) max(durs) else 0,
       median_s  = if (length(durs)) stats::median(durs) else 0,
       total_s   = sum(durs))
}

# Classify a deployment as ok / review / critical from its metrics and thresholds.
#' @keywords internal
#' @noRd
.classifyRegularization <- function(m, thr) {
  crit <- m$pct_gap >= thr$gap_pct_critical ||
          m$pct_interp >= thr$interp_pct_critical ||
          m$rows_added_pct >= thr$rows_added_pct_critical ||
          (!is.null(thr$large_gap_seconds) && m$largest_gap_s >= thr$large_gap_seconds)
  if (crit) return("critical")
  rev <- m$pct_gap >= thr$gap_pct_review ||
         m$pct_interp >= thr$interp_pct_review ||
         m$rows_added_pct >= thr$rows_added_pct_review
  if (rev) "review" else "ok"
}

# Per-deployment metrics + status, computed once from vectors already in hand (cheap; the only extra
# pass is the gap rle). `gap_stats` is attached for payload building and stripped before storing.
#' @keywords internal
#' @noRd
.regularizationMetrics <- function(id, n_original, na_after_join, na_final, nominal_interval,
                                   nominal_hz, jitter_mad_ms, disp_median_ms, disp_p95_ms, thr) {
  n_regular <- length(na_final)
  n_interp  <- sum(na_after_join & !na_final)
  n_gap     <- sum(na_final)
  n_obs     <- n_regular - n_interp - n_gap
  gs <- .regularizationGapStats(na_final, nominal_interval)
  m <- list(id = id, n_original = n_original, n_regular = n_regular,
            rows_added = n_regular - n_original,
            rows_added_pct = if (n_original > 0) 100 * (n_regular - n_original) / n_original else 0,
            n_observed = n_obs, n_interp = n_interp, n_gap = n_gap,
            pct_obs    = if (n_regular > 0) 100 * n_obs / n_regular else 0,
            pct_interp = if (n_regular > 0) 100 * n_interp / n_regular else 0,
            pct_gap    = if (n_regular > 0) 100 * n_gap / n_regular else 0,
            n_gaps = gs$n_gaps, largest_gap_s = gs$largest_s,
            median_gap_s = gs$median_s, total_missing_s = gs$total_s,
            disp_median_ms = disp_median_ms, disp_p95_ms = disp_p95_ms,
            nominal_hz = nominal_hz, jitter_mad_ms = jitter_mad_ms, gap_stats = gs)
  m$status <- .classifyRegularization(m, thr)
  m
}

# A skipped/unchanged deployment still appears in the triage table (no detail page).
#' @keywords internal
#' @noRd
.regularizationStub <- function(id, status, n_original = NA_integer_) {
  list(id = id, status = status, n_original = n_original, n_regular = n_original,
       rows_added = 0L, rows_added_pct = if (isTRUE(status == "unchanged")) 0 else NA_real_,
       n_observed = NA_integer_, n_interp = NA_integer_, n_gap = NA_integer_,
       pct_obs = NA_real_, pct_interp = NA_real_, pct_gap = NA_real_,
       n_gaps = NA_integer_, largest_gap_s = NA_real_, median_gap_s = NA_real_,
       total_missing_s = NA_real_, disp_median_ms = NA_real_, disp_p95_ms = NA_real_,
       nominal_hz = NA_real_, jitter_mad_ms = NA_real_)
}

# Numeric severity score for worst-first ordering (status tier, then gap%, interp%, rows-added%).
#' @keywords internal
#' @noRd
.regularizationSeverity <- function(metrics) {
  tier <- c(empty = 0, skipped = 0, unchanged = 1, ok = 2, review = 3, critical = 4)
  vapply(metrics, function(m) {
    t <- tier[[m$status]]; if (length(t) == 0) t <- 0
    g <- if (is.na(m$pct_gap)) 0 else m$pct_gap
    i <- if (is.na(m$pct_interp)) 0 else m$pct_interp
    r <- if (is.na(m$rows_added_pct)) 0 else min(m$rows_added_pct, 9999)
    t * 1e6 + g * 1000 + i * 10 + r / 10000
  }, numeric(1))
}

# Downsampled coverage status (<= nbin bins; worst status per bin so rare gaps stay visible).
#' @keywords internal
#' @noRd
.regularizationCoverageBins <- function(na_after_join, na_final, nbin = 1500L) {
  n <- length(na_final)
  status <- ifelse(na_final, 2L, ifelse(na_after_join, 1L, 0L))
  nbin <- as.integer(min(n, nbin))
  bin  <- findInterval(seq_len(n), seq(1, n, length.out = nbin + 1)[-(nbin + 1)])
  as.integer(tapply(status, bin, max))
}

# Targeted zoom window for a FLAGGED deployment: the largest unresolved gap (shown as a split window
# with the empty span collapsed and annotated), else the largest interpolation run, else NULL.
#' @keywords internal
#' @noRd
.regularizationZoomData <- function(individual_data, complete_data, datetime.col, sensor_cols,
                                    na_after_join, na_final, gap_stats, nominal_interval) {
  num <- sensor_cols[vapply(sensor_cols, function(c) is.numeric(complete_data[[c]]), logical(1))]
  col <- if ("depth" %in% num) "depth" else if (length(num)) num[1] else NA_character_
  if (is.na(col)) return(NULL)
  tt <- complete_data[[datetime.col]]; n <- length(tt); gv <- complete_data[[col]]
  interp <- na_after_join & !na_final
  raw_in <- function(rng) {
    if (is.null(rng)) return(list(t = numeric(0), v = numeric(0)))
    s <- individual_data[[datetime.col]] >= rng[1] & individual_data[[datetime.col]] <= rng[2]
    list(t = individual_data[[datetime.col]][s], v = individual_data[[col]][s])
  }
  # choose the temporally largest event so a trivial gap never wins over a substantial interpolation
  interp_rle <- if (any(interp)) rle(interp) else NULL
  interp_run_s <- if (!is.null(interp_rle)) max(interp_rle$lengths[interp_rle$values]) * nominal_interval else 0
  gap_s <- if (gap_stats$n_gaps > 0) gap_stats$largest_s else 0

  if (gap_s > 0 && gap_s >= interp_run_s) {
    k <- which.max(gap_stats$durations_s); gs <- gap_stats$starts[k]; ge <- gap_stats$ends[k]
    m <- 200L
    li <- seq.int(max(1L, gs - m), max(1L, gs - 1L)); ri <- seq.int(min(n, ge + 1L), min(n, ge + m))
    if (gs <= 1L) li <- integer(0)
    if (ge >= n)  ri <- integer(0)
    return(list(type = "gap", col = col,
                pre_t = tt[li], pre_v = gv[li], post_t = tt[ri], post_v = gv[ri],
                raw_pre  = raw_in(if (length(li)) range(tt[li]) else NULL),
                raw_post = raw_in(if (length(ri)) range(tt[ri]) else NULL),
                gap_s = gap_stats$durations_s[k],
                gap_t0 = tt[max(1L, gs - 1L)], gap_t1 = tt[min(n, ge + 1L)]))
  }
  if (interp_run_s > 0) {
    r <- interp_rle; ends <- cumsum(r$lengths); starts <- ends - r$lengths + 1L
    k <- which(r$values)[which.max(r$lengths[r$values])]
    center <- round((starts[k] + ends[k]) / 2)
    w <- as.integer(min(n %/% 2L, max(50, round(60 / nominal_interval))))
    win <- max(1L, center - w):min(n, center + w)
    rw <- raw_in(range(tt[win]))
    return(list(type = "interp", col = col, gt = tt[win], gv = gv[win], gi = interp[win],
                ot = rw$t, ov = rw$v))
  }
  NULL
}

# Assemble the small plotting payload for a flagged deployment (drawn after the loop).
#' @keywords internal
#' @noRd
.regularizationPayload <- function(m, individual_data, complete_data, datetime.col, sensor_cols,
                                   na_after_join, na_final, nominal_interval) {
  tt <- complete_data[[datetime.col]]; n <- length(tt)
  gs <- m$gap_stats
  topk <- if (gs$n_gaps) order(-gs$durations_s)[seq_len(min(5L, gs$n_gaps))] else integer(0)
  gap_marks <- if (length(topk))
    data.frame(frac = ((gs$starts[topk] + gs$ends[topk]) / 2) / n, dur = gs$durations_s[topk]) else NULL
  list(m = m,
       cov = .regularizationCoverageBins(na_after_join, na_final),
       zoom = .regularizationZoomData(individual_data, complete_data, datetime.col, sensor_cols,
                                      na_after_join, na_final, gs, nominal_interval),
       gap_marks = gap_marks,
       t_start = tt[1], t_mid = tt[round(n / 2)], t_end = tt[n])
}

# Compact duration label for tables / strips ("12.4m", "3.2h", "45s").
#' @keywords internal
#' @noRd
.formatDurationShort <- function(s) {
  if (length(s) == 0 || is.na(s) || s <= 0) return("-")
  if (s >= 86400) sprintf("%.1fd", s / 86400)
  else if (s >= 3600) sprintf("%.1fh", s / 3600)
  else if (s >= 60)   sprintf("%.1fm", s / 60)
  else sprintf("%.0fs", s)
}

# Console triage table (level >= 1): one row per deployment, worst-first, via cli_verbatim so the
# fixed-width columns survive. The status token is colourised; alignment is computed on plain text.
#' @keywords internal
#' @noRd
.printRegularizationTriage <- function(lvl, metrics, output.dir = NULL, plot.file = NULL,
                                       exclusions.file = NULL, max.listed = 10L) {
  if (lvl < 1L || !length(metrics)) return(invisible(NULL))
  num <- function(f) vapply(metrics, function(m) {
    v <- m[[f]]; if (is.null(v) || !length(v)) NA_real_ else as.numeric(v) }, numeric(1))
  status <- vapply(metrics, function(m) m$status %||% "ok", character(1))

  ## ---- cohort roll-ups, in the calculateTailBeats form ------------------------------------------
  # the qualifier rides the median, not the parenthesis: "median 0.6% of grid points (IQR ...)"
  spread <- function(v, unit = "", d = 1, of = "") {
    v <- v[is.finite(v)]
    if (!length(v)) return(NULL)
    q <- stats::quantile(v, c(0.25, 0.5, 0.75), names = FALSE)
    sprintf("median %.*f%s%s (IQR %.*f\u2013%.*f, range %.*f\u2013%.*f)",
            d, q[2], unit, of, d, q[1], d, q[3], d, min(v), d, max(v))
  }
  ra <- spread(num("rows_added_pct"), "%")
  if (!is.null(ra)) .log_arrow(lvl, "rows added: ", ra)
  it <- spread(num("pct_interp"), "%", of = " of grid points")
  if (!is.null(it)) .log_arrow(lvl, "interpolated: ", it)

  # Gaps get their own line because they are what most warrants catching: how many records came
  # through gap-free, and the single worst gap with the deployment that owns it.
  gp <- num("pct_gap"); lg <- num("largest_gap_s")
  if (any(is.finite(gp)) || any(is.finite(lg))) {
    free <- sum(is.finite(gp) & gp == 0)
    parts <- sprintf("%d deployment%s gap-free", free, if (free != 1L) "s" else "")
    if (any(is.finite(lg)) && max(lg, na.rm = TRUE) > 0) {
      w <- which.max(replace(lg, !is.finite(lg), -Inf))
      parts <- paste0(parts, " \u00b7 largest ", .fmtSecondsSpelled(lg[w]),
                      " (", metrics[[w]]$id, ")")
    }
    .log_arrow(lvl, "gaps: ", parts)
  }

  ## ---- the outcome tally, in a fixed order so the rows visibly sum to the cohort ----------------
  ord <- c("ok", "unchanged", "review", "critical", "empty", "skipped")
  tally <- table(factor(status, levels = ord))
  keep <- as.integer(tally) > 0L
  if (any(keep)) {
    sym <- c(ok = cli::col_green(cli::symbol$tick), unchanged = cli::symbol$bullet,
             review = cli::symbol$bullet, critical = cli::col_red("!"),
             empty = cli::symbol$bullet, skipped = cli::symbol$bullet)
    .log_section(lvl, "Status")
    .log_rows(lvl, stats::setNames(as.integer(tally)[keep], ord[keep]),
              symbols = unname(sym[ord[keep]]))
  }

  ## ---- only the deployments the run is asking someone to look at -------------------------------
  flag <- which(status %in% c("review", "critical"))
  if (length(flag)) {
    flag <- flag[order(-.regularizationSeverity(metrics[flag]))]
    shown <- utils::head(flag, max.listed)
    .log_section(lvl, "Needs review")
    for (i in shown) {
      m <- metrics[[i]]
      mark <- if (identical(m$status, "critical")) cli::col_red("!") else cli::symbol$bullet
      pc <- function(x) if (is.na(x)) "-" else sprintf("%.1f%%", x)
      cli::cli_verbatim(sprintf("  %s %-12s %-9s \u00b7 +%s rows \u00b7 %s gaps \u00b7 largest %s",
                                mark, substr(m$id, 1, 12), m$status,
                                pc(m$rows_added_pct), pc(m$pct_gap),
                                .fmtSecondsSpelled(m$largest_gap_s)))
    }
    if (length(flag) > length(shown))
      cli::cli_verbatim(sprintf("    and %d more", length(flag) - length(shown)))
  }

  out_rows <- c(if (!is.null(output.dir)) c(directory = output.dir),
                if (!is.null(plot.file)) c(plots = plot.file),
                if (!is.null(exclusions.file)) c(exclusions = exclusions.file))
  if (length(out_rows)) { .log_section(lvl, "Output"); .log_rows(lvl, out_rows) }
  invisible(NULL)
}

# PDF page 1: deployment-level overview (run totals + a worst-first table whose rows carry an inline
# observed/interpolated/gap composition bar). Auto-paginates for large runs.
#' @keywords internal
#' @noRd
.drawRegularizationSummaryPage <- function(metrics, interp_method) {
  col_obs <- "#639922"; col_int <- "#EF9F27"; col_gap <- "#E24B4A"
  scol <- c(ok = col_obs, review = col_int, critical = col_gap,
            unchanged = "#888780", empty = "#888780", skipped = "#888780")
  metrics <- metrics[order(-.regularizationSeverity(metrics))]
  n <- length(metrics)
  is_flag <- vapply(metrics, function(m) m$status %in% c("review", "critical"), logical(1))
  n_flag <- sum(is_flag); n_crit <- sum(vapply(metrics, function(m) identical(m$status, "critical"), logical(1)))
  tot_in  <- sum(vapply(metrics, function(m) if (is.na(m$n_original)) 0 else m$n_original, numeric(1)))
  tot_out <- sum(vapply(metrics, function(m) if (is.na(m$n_regular)) 0 else m$n_regular, numeric(1)))
  per_page <- 30L
  chunks <- split(seq_len(n), ceiling(seq_len(n) / per_page))
  cx <- c(tag = 0.0, status = 0.18, rows = 0.42, interp = 0.52, gap = 0.62, maxgap = 0.74, gaps = 0.82)
  for (ci in seq_along(chunks)) {
    idx <- chunks[[ci]]
    graphics::par(mar = c(1, 1.5, 3, 1.5)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
    graphics::text(0, 1.10, "regularizeTimeSeries  -  run summary", adj = c(0, 1), font = 2, cex = 1.3, xpd = NA)
    graphics::text(1, 1.10, sprintf("%s interpolation  -  %d deployment%s%s", interp_method, n,
                   if (n != 1) "s" else "", if (length(chunks) > 1) sprintf("  (page %d/%d)", ci, length(chunks)) else ""),
                   adj = c(1, 1), cex = 0.8, col = "#5F5E5A", xpd = NA)
    graphics::text(0, 1.04, sprintf("%d flagged (%d critical)    rows %s -> %s", n_flag, n_crit,
                   .formatLargeNumber(tot_in), .formatLargeNumber(tot_out)), adj = c(0, 1), cex = 0.85, col = "#5F5E5A", xpd = NA)
    yh <- 0.95
    hd <- function(key, lab, a) graphics::text(cx[key], yh, lab, adj = c(a, 0.5), cex = 0.78, col = "#888780")
    hd("tag", "tag", 0); hd("status", "status", 0); hd("rows", "+rows", 1); hd("interp", "interp", 1)
    hd("gap", "gap", 1); hd("maxgap", "max gap", 1); hd("gaps", "gaps", 1)
    graphics::text(0.92, yh, "composition", adj = c(0.5, 0.5), cex = 0.78, col = "#888780")
    graphics::segments(0, yh - 0.02, 1, yh - 0.02, col = "#CCCCCC", lwd = 0.5)
    # fixed row height so a few deployments cluster at the top instead of stretching down the page
    rh <- min(0.03, (yh - 0.06) / per_page)
    ys <- (yh - 0.05) - (seq_along(idx) - 1) * rh
    for (j in seq_along(idx)) {
      m <- metrics[[idx[j]]]; y <- ys[j]
      graphics::text(cx["tag"], y, substr(m$id, 1, 18), adj = c(0, 0.5), cex = 0.82)
      graphics::text(cx["status"], y, m$status, adj = c(0, 0.5), cex = 0.82, font = 2,
                     col = if (is.na(scol[m$status])) "#888780" else scol[m$status])
      pc <- function(x) if (is.na(x)) "-" else sprintf("%.1f%%", x)
      graphics::text(cx["rows"], y, pc(m$rows_added_pct), adj = c(1, 0.5), cex = 0.82)
      graphics::text(cx["interp"], y, pc(m$pct_interp), adj = c(1, 0.5), cex = 0.82)
      graphics::text(cx["gap"], y, pc(m$pct_gap), adj = c(1, 0.5), cex = 0.82,
                     col = if (!is.na(m$pct_gap) && m$pct_gap >= 5) col_gap else "black")
      graphics::text(cx["maxgap"], y, .formatDurationShort(m$largest_gap_s), adj = c(1, 0.5), cex = 0.82)
      graphics::text(cx["gaps"], y, if (is.na(m$n_gaps)) "-" else as.character(m$n_gaps), adj = c(1, 0.5), cex = 0.82)
      if (!is.na(m$pct_obs)) {
        bx0 <- 0.85; bw <- 0.14; comp <- c(m$pct_obs, m$pct_interp, m$pct_gap) / 100
        xs <- bx0 + bw * cumsum(c(0, comp)); bb <- min(rh * 0.34, 0.012)
        for (k in 1:3) graphics::rect(xs[k], y - bb, xs[k + 1], y + bb, col = c(col_obs, col_int, col_gap)[k], border = NA)
      }
    }
    if (ci == length(chunks))
      graphics::text(0, 0.0, sprintf("%d flagged -> detail pages follow    -    %d healthy -> no page",
                     n_flag, n - n_flag), adj = c(0, 1), cex = 0.72, col = "#888780", xpd = NA)
  }
  invisible(NULL)
}

# PDF detail page for one flagged deployment: regularization impact, gap diagnostics (sorted gap
# durations, replacing the old interval histogram), an annotated coverage strip, and a zoom on the
# most severe event. Drawn only for review/critical deployments (or all, under force.plots).
#' @keywords internal
#' @noRd
.drawRegularizationDetail <- function(p) {
  col_obs <- "#639922"; col_int <- "#EF9F27"; col_gap <- "#E24B4A"; col_grid <- "#185FA5"; col_raw <- "#444441"
  m <- p$m
  scol <- c(ok = col_obs, review = col_int, critical = col_gap, unchanged = "#888780", empty = "#888780")[m$status]
  if (is.na(scol)) scol <- "#888780"
  graphics::layout(matrix(c(1, 1, 2, 3, 4, 4, 5, 5), ncol = 2, byrow = TRUE), heights = c(0.4, 1.35, 0.72, 1.4))

  # ---- header ----
  graphics::par(mar = c(0.2, 1, 0.6, 1)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
  graphics::text(0, 0.55, m$id, adj = c(0, 0.5), font = 2, cex = 1.4)
  graphics::text(graphics::strwidth(m$id, cex = 1.4) + 0.015, 0.55, toupper(m$status), adj = c(0, 0.5),
                 font = 2, cex = 0.9, col = scol)
  graphics::text(1, 0.55, sprintf("%g Hz   %s -> %s rows   jitter %s ms", m$nominal_hz,
                 .formatLargeNumber(m$n_original), .formatLargeNumber(m$n_regular), m$jitter_mad_ms),
                 adj = c(1, 0.5), cex = 0.85, col = "#5F5E5A")

  # ---- regularization impact (left) ----
  graphics::par(mar = c(0.5, 1, 1.6, 1)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
  graphics::title(main = "regularization impact", cex.main = 0.95, adj = 0)
  rows <- c(sprintf("original rows      %s", .formatNumber(m$n_original)),
            sprintf("regularized rows   %s", .formatNumber(m$n_regular)),
            sprintf("rows added         +%s (%+.1f%%)", .formatNumber(m$rows_added), m$rows_added_pct))
  for (k in seq_along(rows)) graphics::text(0, 0.95 - (k - 1) * 0.15, rows[k], adj = c(0, 1), cex = 0.9, family = "mono")
  yb <- 0.32; comp <- c(m$pct_obs, m$pct_interp, m$pct_gap) / 100; xs <- cumsum(c(0, comp))
  for (k in 1:3) graphics::rect(xs[k], yb - 0.06, xs[k + 1], yb + 0.06, col = c(col_obs, col_int, col_gap)[k], border = NA)
  graphics::text(0, yb - 0.15, sprintf("observed %.1f%%    interp %.1f%%    gap %.1f%%",
                 m$pct_obs, m$pct_interp, m$pct_gap), adj = c(0, 1), cex = 0.8, col = "#5F5E5A")

  # ---- gap diagnostics (right) ----
  graphics::par(mar = c(0.5, 1, 1.6, 1)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
  graphics::title(main = "gap diagnostics", cex.main = 0.95, adj = 0)
  graphics::text(0, 0.95, sprintf("gaps %d     largest %s     median %s     missing %s",
                 m$n_gaps, .formatDurationShort(m$largest_gap_s), .formatDurationShort(m$median_gap_s),
                 .formatDurationShort(m$total_missing_s)), adj = c(0, 1), cex = 0.82, col = "#5F5E5A")
  gs <- m$gap_stats
  if (!is.null(gs) && gs$n_gaps > 0) {
    top <- order(-gs$durations_s)[seq_len(min(6L, gs$n_gaps))]; dd <- gs$durations_s[top]; mx <- max(dd)
    yk <- seq(0.78, 0.10, length.out = length(dd)); bh <- min(0.06, 0.55 / length(dd))
    for (k in seq_along(dd)) {
      graphics::rect(0, yk[k] - bh, 0.78 * dd[k] / mx, yk[k] + bh, col = col_gap, border = NA)
      graphics::text(0.80, yk[k], .formatDurationShort(dd[k]), adj = c(0, 0.5), cex = 0.78)
    }
  } else {
    graphics::text(0.5, 0.4, "no unresolved gaps", adj = c(0.5, 0.5), cex = 0.9, col = "#888780")
  }

  # ---- coverage strip ----
  graphics::par(mar = c(2.4, 1, 2.0, 1)); nb <- length(p$cov)
  graphics::plot(c(0, nb), c(0, 1), type = "n", axes = FALSE, xlab = "", ylab = "",
                 main = "coverage over deployment", cex.main = 0.92)
  pal <- c(col_obs, col_int, col_gap)
  graphics::rect(seq_len(nb) - 1, 0, seq_len(nb), 1, col = pal[p$cov + 1L], border = NA)
  graphics::rect(0, 0, nb, 1, border = "#888780", lwd = 0.5)
  if (!is.null(p$gap_marks)) for (k in seq_len(nrow(p$gap_marks))) {
    xx <- p$gap_marks$frac[k] * nb
    graphics::segments(xx, 1, xx, 1.12, col = col_gap, lwd = 0.9, xpd = NA)
    graphics::text(xx, 1.16, .formatDurationShort(p$gap_marks$dur[k]), cex = 0.68, col = col_gap, adj = c(0.5, 0), xpd = NA)
  }
  # x is the coverage BIN INDEX, not time: three fixed positions carrying three named instants
  .axisTime(c(p$t_start, p$t_mid, p$t_end), at = c(0, nb / 2, nb), fmt = "%d-%b %H:%M",
            cex.axis = 0.72, tcl = -0.3)

  # ---- targeted zoom ----
  graphics::par(mar = c(3.2, 3.6, 2.0, 1), mgp = c(2.1, 0.6, 0))
  z <- p$zoom
  if (is.null(z)) {
    graphics::plot.new(); graphics::title("targeted zoom: nothing noteworthy", cex.main = 0.9)
  } else if (z$type == "gap") {
    nL <- length(z$pre_v); nR <- length(z$post_v); gw <- max(2L, round((nL + nR) * 0.2))
    xL <- seq_len(nL); xR <- if (nR) nL + gw + seq_len(nR) else integer(0); xmax <- max(2L, nL + gw + nR)
    yl <- range(c(z$pre_v, z$post_v, z$raw_pre$v, z$raw_post$v), na.rm = TRUE)
    if (!all(is.finite(yl))) yl <- c(0, 1)
    graphics::plot(NA, xlim = c(1, xmax), ylim = yl, axes = FALSE, xlab = "", ylab = z$col,
                   main = "largest gap (zoom)", cex.main = 0.9, cex.lab = 0.85)
    graphics::rect(nL + 0.5, yl[1], nL + gw + 0.5, yl[2], col = "#E24B4A22", border = NA)
    graphics::text(nL + gw / 2 + 0.5, yl[2], .formatDurationShort(z$gap_s), col = col_gap, cex = 0.82, adj = c(0.5, 1))
    if (nL) graphics::lines(xL, z$pre_v, col = col_grid, lwd = 1.1)
    if (nR) graphics::lines(xR, z$post_v, col = col_grid, lwd = 1.1)
    if (nL && length(z$raw_pre$t))
      graphics::points(stats::approx(as.numeric(z$pre_t), xL, xout = as.numeric(z$raw_pre$t), rule = 2)$y,
                       z$raw_pre$v, pch = 16, col = col_raw, cex = 0.5)
    if (nR && length(z$raw_post$t))
      graphics::points(stats::approx(as.numeric(z$post_t), xR, xout = as.numeric(z$raw_post$t), rule = 2)$y,
                       z$raw_post$v, pch = 16, col = col_raw, cex = 0.5)
    graphics::axis(2, cex.axis = 0.8, las = 1); graphics::box(col = "#CCCCCC")
    .axisTime(c(z$gap_t0, z$gap_t1), at = c(1, xmax), fmt = "%H:%M", cex.axis = 0.78)
  } else {
    yl <- range(c(z$gv, z$ov), na.rm = TRUE); if (!all(is.finite(yl))) yl <- c(0, 1)
    graphics::plot(z$gt, z$gv, type = "l", col = col_grid, lwd = 1.1, ylim = yl, xlab = "time", ylab = z$col,
                   main = "largest interpolation (zoom)", cex.main = 0.9, cex.axis = 0.8, cex.lab = 0.85, las = 1)
    if (any(z$gi)) graphics::points(z$gt[z$gi], z$gv[z$gi], pch = 1, col = col_int, cex = 0.9, lwd = 1.2)
    graphics::points(z$ot, z$ov, pch = 16, col = col_raw, cex = 0.5)
    graphics::legend("topright", legend = c("raw", "interpolated"), pch = c(16, 1),
                     col = c(col_raw, col_int), bty = "n", cex = 0.72)
  }
  invisible(NULL)
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
