#######################################################################################################
# Per-dive metric comparison across deployments #######################################################
#######################################################################################################
#
# UNIT DRAWN = the dive. UNIT COMPARED = the individual. That pair is the whole design.
#
# WHY NOT A BAR OF PER-INDIVIDUAL MAXIMA (the shape originally parked for this function). A bar encodes
# magnitude as a length from a true zero, and under `reference = "baseline"` no such zero exists at the
# surface: the level the excursion departs from is the seabed, or whatever depth the animal was holding.
# Measured over 52 real deployments / 6,512 dives, `reference = "auto"` resolved MIXED - 4,572 baseline,
# 1,940 surface - and median `max_depth_m` was 23.02 m under baseline against 15.49 m under surface,
# while median `amplitude_m` went the other way (13.67 vs 15.49). The median |max_depth_m - amplitude_m|
# was 6.01 m under baseline and exactly 0.00 m under surface. So a bar of max depth would have drawn a
# seabed offset as dive depth, on two thirds of the dives, silently.
#
# A per-individual MAXIMUM is additionally an extreme whose expectation grows with n, and n ranged from
# 1 to 424 dives per deployment in that same cohort. Hence: every dive is a point, the marker is a
# median with an IQR, and `amplitude_m` (measured from the baseline, so direction-agnostic) is the
# default metric for every taxon.




#' Built-in axis label for a dive metric.
#'
#' Falls back to the column name, so an unknown or user-added metric is still drawable.
#' @keywords internal
#' @noRd
.diveLabel <- function(metric, reference = NULL) {
  base <- .diveLabelBase(metric)
  # An ABSOLUTE depth compared across deployments that were detected against different zeros is not one
  # quantity. Under `reference = "baseline"` the zero is the level the animal was holding, so 30 m on a
  # shelf animal and 30 m on a slope animal are different measurements wearing the same number. Say so
  # in the label rather than in a footnote nobody reads.
  mixed <- !is.null(reference) && length(stats::na.omit(unique(reference))) > 1L
  ifelse(.diveIsDepth(metric) & mixed, paste0(base, " [mixed reference]"), base)
}

#' @keywords internal
#' @noRd
.diveLabelBase <- function(metric) {
  lab <- c(
    amplitude_m         = "Dive amplitude (m)",
    max_depth_m         = "Max. depth (m)",
    mean_depth_m        = "Mean depth (m)",
    sd_depth_m          = "Depth SD (m)",
    baseline_depth_m    = "Baseline depth (m)",
    bottom_depth_mean_m = "Bottom depth (m)",
    duration_s          = "Dive duration (s)",
    descent_duration_s  = "Descent duration (s)",
    bottom_duration_s   = "Bottom duration (s)",
    ascent_duration_s   = "Ascent duration (s)",
    inter_dive_s        = "Inter-dive interval (s)",
    descent_rate_mean   = "Descent rate (m/s)",
    descent_rate_q90    = "Descent rate, q90 (m/s)",
    ascent_rate_mean    = "Ascent rate (m/s)",
    ascent_rate_q90     = "Ascent rate, q90 (m/s)",
    vertical_distance_m = "Vertical distance (m)",
    n_reversals         = "Reversals per dive",
    depth_coverage      = "Depth coverage",
    depth_attenuation   = "Depth attenuation")[metric]
  unname(ifelse(is.na(lab), metric, lab))
}

#' Is this metric an ABSOLUTE depth (measured from a real zero) or a magnitude (no origin to mark)?
#'
#' A property of the metric, never of `direction` - which is why no direction-dependent axis code
#' exists anywhere in this function.
#' @keywords internal
#' @noRd
.diveIsDepth <- function(metric) {
  metric %in% c("max_depth_m", "mean_depth_m", "baseline_depth_m", "bottom_depth_mean_m")
}

#' Which dives may enter the statistics for THIS metric.
#'
#' Two flags, not one. `inter_dive_s` describes the interval BETWEEN two dives and has its own
#' censoring column; every other metric describes the dive itself and follows `complete`. Using
#' `complete` for the interval keeps precisely the row diveMetrics() exists to flag: two uncensored
#' dives with a blackout between them, whose "surface interval" is a property of the sensor.
#' @keywords internal
#' @noRd
.diveUsable <- function(dm, metric) {
  # .asNumericSafe, not as.numeric: a factor coerces to its LEVEL CODES without warning, which is the
  # package's shared coercion contract precisely because plotDistributions was bitten by it - a factored
  # amplitude column came back with median 3 instead of 30 and a perfectly plausible figure.
  v <- .asNumericSafe(dm[[metric]])
  ok <- is.finite(v)
  cens_ok <- if (identical(metric, "inter_dive_s")) {
    if ("inter_dive_censored" %in% names(dm)) !.isTrueVec(dm$inter_dive_censored) else rep(TRUE, nrow(dm))
  } else {
    if ("complete" %in% names(dm)) .isTrueVec(dm$complete) else rep(TRUE, nrow(dm))
  }
  # CENSORING IS DECIDED FIRST. The two exclusions are not independent - a boundary-truncated dive is
  # exactly the dive whose ascent phase fails to resolve - so requiring a finite value before counting a
  # dive as censored moves every censored-AND-valueless dive into "unsupported". Measured on 8 real
  # deployments that reported n_censored = 0 for ascent_rate_q90 while all four censored dives sat in
  # the other bucket, inflating it by 18%: the small number read as zero and contaminated the big one.
  list(value = v,
       used        = ok & cens_ok,
       censored    = !cens_ok,
       unsupported = cens_ok & !ok)
}

#' NA-safe elementwise isTRUE. `NA` is not evidence that a dive is complete, so it must not pass a
#' filter that asks whether the dive is complete.
#' @keywords internal
#' @noRd
.isTrueVec <- function(z) {
  z <- suppressWarnings(as.logical(z))   # as.logical("yes") is a silent NA, not an error
  !is.na(z) & z
}


#' Reduce a dive-metrics table to one row per deployment and metric.
#'
#' The whole honesty contract of plotDives() lives here, with no graphics to argue about: which dives
#' count for which metric, how many were dropped and for which of the two distinct reasons, and whether
#' there is enough left to justify drawing a marker at all.
#' @keywords internal
#' @noRd
.diveSummaryTable <- function(dm, metrics, id.col = "ID", min.n = 5L, groups = NULL) {
  ids <- unique(as.character(dm[[id.col]]))
  rows <- list()
  for (id in ids) {
    sub <- dm[as.character(dm[[id.col]]) == id, , drop = FALSE]
    for (m in metrics) {
      if (!m %in% names(sub)) {
        rows[[length(rows) + 1L]] <- .diveSummaryRow(id, m, groups, sub, NULL, min.n)
        next
      }
      rows[[length(rows) + 1L]] <- .diveSummaryRow(id, m, groups, sub, .diveUsable(sub, m), min.n)
    }
  }
  out <- do.call(rbind, rows)
  rownames(out) <- NULL
  out
}

#' @keywords internal
#' @noRd
.diveSummaryRow <- function(id, metric, groups, sub, u, min.n) {
  n_dives <- nrow(sub)
  vals <- if (is.null(u)) numeric(0) else u$value[u$used]
  q <- if (length(vals)) stats::quantile(vals, c(0.25, 0.5, 0.75), names = FALSE, na.rm = TRUE)
       else rep(NA_real_, 3)
  first_chr <- function(col) {
    if (!col %in% names(sub)) return(NA_character_)
    z <- unique(as.character(sub[[col]])); z <- z[!is.na(z)]
    if (!length(z)) NA_character_ else if (length(z) == 1L) z else paste(sort(z), collapse = "/")
  }
  data.frame(
    id = id,
    # %||% only catches NULL; an id absent from `groups` subscripts to NA, which must stay NA rather
    # than silently become the string "NA" downstream
    group = if (is.null(groups)) NA_character_
            else { g <- unname(groups[id]); if (length(g) != 1L || is.na(g)) NA_character_ else as.character(g) },
    metric = metric,
    n_dives = as.integer(n_dives),
    n_used = if (is.null(u)) 0L else as.integer(sum(u$used)),
    n_censored = if (is.null(u)) 0L else as.integer(sum(u$censored)),
    n_unsupported = if (is.null(u)) as.integer(n_dives) else as.integer(sum(u$unsupported)),
    median = q[2], q25 = q[1], q75 = q[3],
    min = if (length(vals)) min(vals) else NA_real_,
    max = if (length(vals)) max(vals) else NA_real_,
    reference = first_chr("reference"),
    direction = first_chr("direction"),
    # a marker asserts a central tendency; too few dives and the points are the honest answer on their own
    drawn = !is.null(u) && sum(u$used) >= min.n,
    stringsAsFactors = FALSE)
}


#######################################################################################################
# Rendering ###########################################################################################
#######################################################################################################

#' Report the decisions that shaped the figure, before any of them is applied.
#'
#' Every one of these changes what the reader sees - which dives counted, where the axis was cut,
#' whether a median was drawn at all - so they belong in the header, not inferred afterwards from the
#' figure.
#' @keywords internal
#' @noRd
.reportDivePlotSettings <- function(lvl, metrics, trim, min.n, order.by, group.by, n_ids,
                                    max.per.page, n_pages) {
  .log_section(lvl, "Plot settings")
  rows <- c("Metrics"            = paste(metrics, collapse = ", "),
            "Axis trim"          = sprintf("%.3g (values above the %s percentile sit at the edge)",
                                           trim, .ordinalSuffix(round(trim * 100))),
            "Median drawn from"  = sprintf("%d usable dive%s", min.n, if (min.n != 1) "s" else ""),
            "Order"              = switch(order.by,
                                          id = "deployment id", input = "order of appearance",
                                          median = "median of the ordering metric"),
            "Grouping"           = group.by %||% "none",
            "Layout"             = if (n_pages > 1L)
                                     sprintf("%d pages, up to %d deployments each", n_pages, max.per.page)
                                   else sprintf("1 page, %d deployment%s", n_ids,
                                                if (n_ids != 1) "s" else ""))
  .log_rows(lvl, rows)
}


#' Ordinal suffix for a whole percentile ("95th", "90th", "1st").
#' @keywords internal
#' @noRd
.ordinalSuffix <- function(n) {
  s <- if (n %% 100 %in% 11:13) "th" else switch(as.character(n %% 10), "1" = "st", "2" = "nd",
                                                 "3" = "rd", "th")
  paste0(n, s)
}


#' The closing summary: what was drawn, what was left out, and where it went.
#' @keywords internal
#' @noRd
.reportDivePlotSummary <- function(lvl, st, metrics, ids, pages, trim, min.n, plot.file, start.time) {
  .log_summary(lvl)
  agg <- function(f) vapply(metrics, function(m) sum(f(st[st$metric == m, ]), na.rm = TRUE), numeric(1))
  used <- agg(function(z) z$n_used)
  excl <- agg(function(z) z$n_censored + z$n_unsupported)
  offs <- agg(function(z) z$n_trimmed)
  # a dash rather than a zero: a column of zeros reads as a finding, when it means nothing happened
  num <- function(x) ifelse(x > 0, format(x, big.mark = ",", trim = TRUE), "-")

  .log_section(lvl, "Dives per metric")
  .log_table(lvl, data.frame(Metric = metrics, Used = num(used), Excluded = num(excl),
                             `Off-scale` = num(offs), check.names = FALSE,
                             stringsAsFactors = FALSE))
  if (any(excl > 0))
    .log_note(lvl, paste("Excluded dives were censored by the record's start or end, or had no",
                         "descent/bottom/ascent resolved. They are drawn in outline."))
  if (any(offs > 0))
    .log_note(lvl, sprintf(paste("Off-scale dives lie beyond the trimmed axis (trim = %.3g). They are",
                                 "drawn at the axis edge, not removed."), trim))

  # said ONCE for the cohort, not repeated under every metric: it is a property of the deployment
  no_med <- unique(st$id[!st$drawn])
  .log_section(lvl, "Deployments")
  rows <- c("Drawn" = format(length(ids), big.mark = ","))
  if (length(no_med)) {
    shown <- utils::head(no_med, 8L)
    rows <- c(rows, stats::setNames(
      sprintf("%d (%s%s)", length(no_med), paste(shown, collapse = ", "),
              if (length(no_med) > length(shown)) sprintf(", +%d more", length(no_med) - length(shown)) else ""),
      sprintf("Without a median (< %d dives)", min.n)))
  }
  if (length(pages) > 1L) rows <- c(rows, "Pages" = format(length(pages)))
  .log_rows(lvl, rows)

  if (!is.null(plot.file)) {
    .log_section(lvl, "Output")
    .log_rows(lvl, c("File" = plot.file))
  }
  .log_runtime(lvl, start.time)
}


#' Split one metric's dives per deployment into the ones that count and the ones only drawn.
#' @keywords internal
#' @noRd
.pdPerId <- function(dm, metric, ids, id.col) {
  lapply(ids, function(id) {
    sub <- dm[as.character(dm[[id.col]]) == id, , drop = FALSE]
    if (!metric %in% names(sub)) return(list(used = numeric(0), excl = numeric(0)))
    u <- .diveUsable(sub, metric)
    # Non-finite values are dropped HERE, not later: `censored` is decided before finiteness (see
    # .diveUsable), so a boundary-truncated dive whose ascent never resolved is censored AND valueless.
    # Such a dive has no position on the axis, and carrying its NA into the placement arithmetic turns
    # the off-scale test into NA and aborts the panel. It is still counted in the summary, from `st`.
    list(used = u$value[u$used], excl = u$value[u$censored & is.finite(u$value)])
  })
}


#' Value-axis range for one metric, shared by every page.
#'
#' The axis is governed by the dives that COUNT - an excluded outlier must not set the scale for a
#' median it took no part in - but it must still CONTAIN every marker the figure draws, or a deployment
#' whose median sits above the pooled quantile has its marker clipped away silently while the returned
#' table reports `drawn = TRUE`. The floor likewise has to reach the excluded points, or they are
#' clipped and never counted, contradicting the promise that excluded dives are drawn, not removed.
#' @keywords internal
#' @noRd
.pdMetricRange <- function(per_id, st, metric, ids, trim) {
  used_all <- unlist(lapply(per_id, `[[`, "used"))
  excl_all <- unlist(lapply(per_id, `[[`, "excl"))
  if (!length(used_all)) used_all <- 0
  hi <- stats::quantile(used_all, trim, names = FALSE, na.rm = TRUE)
  mk <- st[st$metric == metric & st$id %in% ids & st$drawn, , drop = FALSE]
  if (nrow(mk)) hi <- max(hi, max(mk$q75, mk$median, na.rm = TRUE), na.rm = TRUE)
  lo <- min(c(used_all, excl_all, if (nrow(mk)) mk$q25 else NULL, 0), na.rm = TRUE)
  if (!is.finite(hi) || hi <= lo) hi <- lo + 1
  c(lo, hi)
}


#' Clip the group blocks of a grouped layout to the deployments on ONE page, in panel coordinates.
#' @keywords internal
#' @noRd
.pdPageBlocks <- function(blocks, xp) {
  if (is.null(blocks)) return(NULL)
  top <- max(xp); rng <- range(xp)
  out <- lapply(blocks, function(b) {
    s <- max(b[1], rng[1]); e <- min(b[2], rng[2])
    if (s > e) NULL else c(top - e + 1, top - s + 1)
  })
  out <- out[!vapply(out, is.null, logical(1))]
  if (length(out)) out else NULL
}


#' Fold one page's draw record into the cohort-wide record.
#'
#' The record is the figure's audit trail and describes the whole cohort, so pagination must not make it
#' describe only the last page drawn.
#' @keywords internal
#' @noRd
.pdMergeRecords <- function(acc, rec) {
  if (is.null(acc)) return(rec)
  acc$ids     <- c(acc$ids, rec$ids)
  acc$ypos    <- c(acc$ypos, rec$ypos)
  acc$trimmed <- c(acc$trimmed, rec$trimmed)
  acc$clipped <- acc$clipped + rec$clipped
  acc$markers <- c(acc$markers, rec$markers)
  acc
}


#' Draw ONE metric panel into the current plot region.
#'
#' Every dive is drawn. Dives excluded from the statistics are drawn in outline rather than removed,
#' because a reader cannot judge a median they cannot see the basis of - and the two reasons a dive is
#' excluded (censored, phase-unsupported) are not interchangeable, so the panel note names both.
#' @keywords internal
#' @noRd
.pdDrawPanel <- function(dm, st, metric, ids, theme, trim, id.col, show.labels = TRUE,
                         label = NULL, ypos = NULL, cols = NULL, blocks = NULL, gutter = TRUE,
                         xlim = NULL) {
  # ONE ROW PER DEPLOYMENT, metric value on x. The transpose is what makes a large cohort readable: the
  # deployment count now drives HEIGHT, which can grow and paginate, instead of WIDTH, which cannot.
  # At 49 deployments the old geometry asked for a 56-inch canvas, so every glyph and label was scaled
  # into illegibility by whatever rendered it.
  ny   <- length(ids)
  ypos <- ypos %||% rev(seq_len(ny))                 # first id at the TOP, as in plotDistributions
  cols <- cols %||% rep(theme$axis, ny)

  per_id <- .pdPerId(dm, metric, ids, id.col)

  # A paginated figure whose pages carry different axes is not a comparison, so the caller fixes the
  # range across the whole cohort once and passes it in; computing it here is the single-page fallback.
  rng <- xlim %||% .pdMetricRange(per_id, st, metric, ids, trim)
  lo <- rng[1]; hi <- rng[2]

  graphics::plot.new()
  # the y span follows `ypos`, not the deployment count, so the gaps a grouped layout inserts between
  # blocks are honoured instead of pushing the last rows off the panel
  graphics::plot.window(xlim = c(lo, hi), ylim = c(min(ypos) - 0.6, max(ypos) + 0.6),
                        xaxs = "i", yaxs = "i")
  usr <- graphics::par("usr")
  graphics::rect(usr[1], usr[3], usr[2], usr[4], col = theme$panel, border = NA)
  ticks <- graphics::axTicks(1)
  graphics::abline(v = ticks, col = theme$grid, lwd = 0.8)      # vertical only: y is categorical now
  # An ABSOLUTE depth is measured from a zero that means something (the surface, or the baseline the
  # animal was holding), so that zero is marked. The axis is NOT inverted: inverting depth is a
  # convention for depth on the VERTICAL axis, and here the value runs left to right like every other
  # metric - flipping it would put the shallowest deployment on the right of the deepest.
  zero_line <- .diveIsDepth(metric)
  if (zero_line) graphics::abline(v = 0, col = theme$ink, lty = 2, lwd = 0.9)

  clipped <- 0L
  trimmed <- integer(ny)
  for (i in seq_len(ny)) {
    d <- per_id[[i]]
    n <- length(d$used)
    # a dense cloud and a sparse one must both stay readable, so alpha tracks n rather than being fixed
    al <- max(0.25, min(0.75, 0.75 * sqrt(30 / max(n, 1))))
    yi <- ypos[i]
    # Deterministic spread, not runif(): a published figure must redraw identically, and a golden-ratio
    # sequence scatters more evenly across the band than random jitter does at small n.
    jit <- function(k) if (!k) numeric(0) else yi + (((seq_len(k) * 0.6180339887498949) %% 1) - 0.5) * 0.54
    # na.rm even though .pdPerId already filters: a single NA reaching here would make the off-scale
    # test NA and abort the whole panel, which is a poor failure mode for a plotting function
    pin <- function(v) list(inside = v[!is.na(v) & v >= lo & v <= hi],
                            edge = sum(v > hi | v < lo, na.rm = TRUE))
    pu <- pin(d$used); pe <- pin(d$excl)
    clipped <- clipped + pu$edge + pe$edge
    if (length(pe$inside))
      graphics::points(pe$inside, jit(length(pe$inside)), pch = 1, cex = 0.62,
                       col = grDevices::adjustcolor(theme$ink, alpha.f = 0.5))
    if (length(pu$inside))
      graphics::points(pu$inside, jit(length(pu$inside)), pch = 16, cex = 0.7,
                       col = grDevices::adjustcolor(cols[i], alpha.f = al))
    if (pu$edge + pe$edge > 0) {
      # chevrons rather than the old up/down triangles: the axis runs left-to-right now, so the marker
      # has to point the way the values went
      above <- sum(d$used > hi, na.rm = TRUE) + sum(d$excl > hi, na.rm = TRUE)
      below <- (pu$edge + pe$edge) - above
      # inset by a hair so the glyph sits INSIDE the panel: centred exactly on the limit, half of it
      # spills into the gutter and collides with that row's n=
      pad <- 0.014 * (hi - lo)
      if (above) graphics::points(hi - pad, yi, pch = 62, cex = 0.95, col = theme$ink)
      if (below) graphics::points(lo + pad, yi, pch = 60, cex = 0.95, col = theme$ink)
    }
    trimmed[i] <- pu$edge + pe$edge

    s <- st[st$id == ids[i] & st$metric == metric, ]
    if (nrow(s) && isTRUE(s$drawn)) {
      graphics::segments(s$q25, yi, s$q75, yi, lwd = 1.9, col = theme$ink)
      graphics::segments(c(s$q25, s$q75), c(yi, yi) - 0.13, c(s$q25, s$q75), c(yi, yi) + 0.13,
                         lwd = 1.9, col = theme$ink)          # caps, so the IQR reads as a range
      graphics::segments(s$median, yi - 0.26, s$median, yi + 0.26, lwd = 2.8, col = theme$ink)
    }
    # the usable count, in a fixed right-hand gutter. Always the same place on every row, so it can be
    # scanned or ignored; under each column (as before) it was unreadable at any realistic figure size.
    if (gutter && nrow(s))
      graphics::mtext(paste0("n=", s$n_used), side = 4, at = yi, line = 0.35, las = 1,
                      cex = 0.62 * theme$cex, col = grDevices::adjustcolor(theme$ink, alpha.f = 0.85))
  }

  # The metric is a COLUMN TITLE and the value axis carries bare ticks: with one panel per metric the
  # units belong at the head of the column, not repeated beneath every one of them.
  graphics::axis(1, at = ticks, cex.axis = 0.78 * theme$cex, col = NA, col.ticks = theme$axis,
                 col.axis = theme$ink)
  graphics::mtext(label %||% .diveLabel(metric), side = 3, line = 0.5, font = 2,
                  cex = 0.82 * theme$cex, col = theme$ink)
  # deployment labels once per page, on the leftmost panel: repeating them per column would spend the
  # width the metrics need
  if (show.labels) {
    graphics::axis(2, at = ypos, labels = ids, las = 1, tick = FALSE, line = -0.4,
                   cex.axis = 0.76 * theme$cex, col.axis = theme$ink)
    if (!is.null(blocks))
      for (l in names(blocks))
        graphics::mtext(l, side = 2, line = 5.1, at = mean(blocks[[l]]), font = 2,
                        cex = 0.8 * theme$cex, col = theme$ink)
  }
  graphics::box(col = theme$grid)
  invisible(list(metric = metric, xlim = c(lo, hi), lo = lo, hi = hi, zero_line = zero_line,
                 ylim = c(min(ypos) - 0.6, max(ypos) + 0.6),
                 ids = ids, ypos = ypos, trimmed = stats::setNames(trimmed, ids),
                 clipped = clipped,
                 markers = stats::setNames(lapply(seq_along(ids), function(i) {
                   z <- st[st$id == ids[i] & st$metric == metric, ]
                   if (!nrow(z) || !isTRUE(z$drawn)) NULL
                   else c(median = z$median, q25 = z$q25, q75 = z$q75)
                 }), ids)))
}


#' Draw the figure key into the bottom outer margin.
#'
#' The glyphs carry meaning that no axis states (which dives counted, which were only drawn, where the
#' axis was cut), so the key is drawn with the ACTUAL symbols rather than described in a subtitle - a
#' reader should not have to translate "open = excluded" into what is on the panel.
#' @keywords internal
#' @noRd
.pdDrawKey <- function(theme) {
  op <- graphics::par(fig = c(0, 1, 0, 1), oma = c(0, 0, 0, 0), mar = c(0, 0, 0, 0), new = TRUE)
  on.exit(graphics::par(op), add = TRUE)
  graphics::plot.new()
  faded <- grDevices::adjustcolor(theme$ink, alpha.f = 0.6)
  graphics::legend("bottom", horiz = TRUE, bty = "n", xpd = NA, inset = c(0, 0),
                   cex = 0.72 * theme$cex, text.col = theme$ink,
                   legend = c("dive (used)", "dive (excluded)", "beyond axis", "median with IQR"),
                   pch = c(16, 1, 62, NA), lty = c(NA, NA, NA, 1), lwd = c(NA, NA, NA, 2.2),
                   pt.cex = c(0.9, 0.8, 0.9, NA), col = c(faded, faded, theme$ink, theme$ink))
  invisible(NULL)
}


#' Compare deployments on per-dive metrics
#'
#' @description
#' Comparing deployments on how deep or how long they dived usually collapses to a bar of means, which
#' hides both the spread and the individual dives that produced it - and dive metrics are skewed often
#' enough that the mean is rarely the interesting number.
#'
#' This function draws every dive as a point in its deployment's column, with a median and
#' interquartile marker over the top, for one or more metrics from [diveMetrics()]. The cohort
#' comparison and the raw material behind it are then visible together.
#'
#' It computes nothing [diveMetrics()] has not already computed. For a per-sample distribution see
#' [plotDistributions()], for a time budget [plotTimeAtDepth()], for the depth trace itself
#' [plotDepthProfiles()], and for detector quality control the `plot.file` argument of
#' [detectDives()].
#'
#' @param data A `nautilus_dive_metrics` table from [diveMetrics()], or a list of them.
#' @param metrics Character. Columns to draw, one panel each. `NULL` (default) uses
#'   `c("amplitude_m", "duration_s")`.
#' @param labels Named character mapping a metric to its axis label, overriding the built-in ones.
#' @param group.by Grouping for the deployments: the name of a column in the metrics table, a named
#'   `id -> group` vector, or a two-column `data.frame`. A trait held only in the tag metadata, such as
#'   species or sex, is not reachable from here - [diveMetrics()] returns a plain table - so pass it as
#'   a vector or a `data.frame`. `NULL` (default) draws one
#'   ungrouped block. Deployments the grouping does not cover are drawn in a trailing `(ungrouped)`
#'   block rather than dropped - see Details.
#' @param order.by How to order the deployment slots - one order, shared by every panel:
#'   `"id"` (default, alphabetical), `"input"` (order of first appearance), or `"median"` (largest
#'   median first, on `order.metric`).
#' @param order.metric Metric whose per-deployment median drives `order.by = "median"`. `NULL`
#'   (default) uses the first entry of `metrics`.
#' @param trim Numeric in (0, 1]. Upper quantile bounding the value axis; `1` shows the full range.
#'   Points beyond it are drawn at the axis edge and counted in the summary, never dropped silently.
#'   Defaults to `0.95`, rather than the `0.995` the rest of the plotting family uses, because per-dive metrics
#'   are far more skewed than the per-sample kinematics that default was chosen for: on a real cohort
#'   `amplitude_m` had a median of 12.2 m against a maximum of 1414.1 m. At `0.995` the axis reached
#'   417 m and every median and IQR in the figure collapsed onto the baseline; at `0.95` it reaches
#'   92.8 m and the between-deployment differences the plot exists to show become legible. The 5% that
#'   moves is pinned and counted, and the untrimmed extremes remain in the returned `max` column.
#' @param min.n Integer. Dives needed before a median/IQR marker is drawn. Points are always drawn.
#' @param max.per.page Integer. Deployments per page; the cohort is split across as many pages as it
#'   needs. Each deployment occupies a row, so this caps how tall one page becomes: beyond roughly
#'   thirty rows the labels and the point cloud of a single deployment both start to compress. Raise it
#'   to keep a mid-sized cohort on one page, lower it for a denser cloud per deployment. The value axis
#'   of each metric is held fixed across pages, so deployments remain comparable between them. Only a
#'   PDF (`plot.file`) can hold more than one page; on a screen device the pages are drawn in turn.
#' @param theme A [plotTheme()] object, or a list of overrides.
#' @param plot Logical. Draw to the active device.
#' @param plot.file Character. Path to a PDF.
#' @param id.col Character. Deployment id column.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal", or `2`/"detailed" (default).
#'
#' @details
#' **Why `amplitude_m` is the default and `max_depth_m` is not.** `amplitude_m` is measured from
#' the dive's own baseline, so it means the same thing under `reference = "surface"`, under
#' `reference = "baseline"` and under `direction = "up"` - one number, one code path, every taxon.
#' `max_depth_m` is an absolute depth: for a surface-referenced air-breather it is the depth of the
#' dive; for a benthic rester excursing upward it is the depth the animal rested at. Both are
#' available, but only one is safe to default to. Measured over 52 real deployments and 6,512 dives,
#' `reference = "auto"` resolved to a mixture - 4,572 baseline, 1,940 surface - and median
#' `max_depth_m` was 23.02 m under baseline against 15.49 m under surface, while `amplitude_m` went the
#' other way. The median absolute difference between the two columns was 6.01 m under baseline and
#' exactly 0 under surface: that difference is seabed, not diving.
#'
#' **Every dive is a point; the marker is only a marker.** No bar is drawn. A bar encodes
#' magnitude as a length from a true zero, which under `reference = "baseline"` does not exist, and a
#' per-individual maximum is an extreme whose expectation grows with n - which ranged from 1 to 424
#' dives per deployment in that cohort. The median is drawn only once `min.n` dives support it.
#'
#' **Censoring is applied per metric.** `inter_dive_s` is included on `!inter_dive_censored`;
#' every other metric on `complete`. Excluded dives are drawn in outline and counted; there is no
#' argument to fold them back into the statistics. A separate and usually larger loss is reported
#' separately: dives whose phase structure did not support the metric at all.
#'
#' @return Invisibly, a `data.frame` with one row per deployment and metric: `id`, `group`, `metric`,
#'   `n_dives`, `n_used`, `n_censored`, `n_unsupported`, `median`, `q25`, `q75`, `min`, `max`,
#'   `reference`, `direction`, `drawn`, plus `n_trimmed` and `axis_max` recording how many dives `trim`
#'   pinned to the axis edge and where that edge fell - so the figure can be reproduced from the table.
#' @seealso [diveMetrics()], [detectDives()], [plotDistributions()], [plotTimeAtDepth()]
#' @examples
#' \dontrun{
#' tag <- detectDives(processed, control = diveControl(depth.threshold = 5))
#' dm  <- diveMetrics(tag)
#' plotDives(dm, metrics = c("amplitude_m", "duration_s"))
#' plotDives(dm, metrics = "max_depth_m", plot = FALSE, plot.file = "./plots/dives.pdf")
#' }
#' @export

plotDives <- function(data,
                      metrics      = NULL,
                      labels       = NULL,
                      group.by        = NULL,
                      order.by     = c("id", "input", "median"),
                      order.metric = NULL,
                      trim      = 0.95,
                      min.n     = 5L,
                      max.per.page = 30L,
                      theme     = plotTheme(),
                      plot      = TRUE,
                      plot.file = NULL,
                      id.col    = "ID",
                      verbose   = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  .assert_string(id.col, "id.col")
  .assert_string(order.metric, "order.metric", null_ok = TRUE)
  order.by <- match.arg(order.by)
  .assert_flag(plot, "plot")
  .assert_number(trim, "trim", min = 0, max = 1)
  if (trim <= 0) .abort("{.arg trim} must be greater than zero.")
  .assert_count(min.n, "min.n")
  .assert_count(max.per.page, "max.per.page")
  .assert_writable_file(plot.file, "plot.file", ext = "pdf")
  theme <- .as_control(theme, plotTheme, "nautilus_theme", "theme")
  if (!isTRUE(plot) && is.null(plot.file))
    .abort(c("Nothing to draw to: {.arg plot} is {.code FALSE} and {.arg plot.file} is {.code NULL}.",
             "i" = "Set one of them."))
  if (!is.null(metrics) && (!is.character(metrics) || !length(metrics)))
    .abort("{.arg metrics} must be a non-empty character vector of column names, or {.code NULL}.")
  metrics <- metrics %||% c("amplitude_m", "duration_s")
  if (!is.null(labels) && (!is.character(labels) || is.null(names(labels))))
    .abort("{.arg labels} must be a NAMED character vector mapping a metric to its axis label.")
  if (!is.null(order.metric) && !order.metric %in% metrics)
    .abort("{.arg order.metric} ({.val {order.metric}}) must be one of {.arg metrics}: {.val {metrics}}.")

  dm <- .pdBindMetrics(data, id.col)

  n_ids <- length(unique(dm[[id.col]]))
  .log_header(lvl, "plotDives", "Comparing deployments on per-dive metrics",
              bullets = sprintf("Input: %s dive%s across %d deployment%s",
                                format(nrow(dm), big.mark = ","), if (nrow(dm) != 1) "s" else "",
                                n_ids, if (n_ids != 1) "s" else ""),
              close = FALSE)
  .reportDivePlotSettings(lvl, metrics, trim, min.n, order.by,
                          if (is.character(group.by) && length(group.by) == 1L) group.by
                          else if (!is.null(group.by)) "supplied lookup" else NULL,
                          n_ids, max.per.page, ceiling(n_ids / max.per.page))
  .log_header_close(lvl)

  st <- .diveSummaryTable(dm, metrics, id.col = id.col, min.n = min.n)

  # A metric no deployment can support is dropped BY NAME rather than drawn as a fully-formed empty
  # panel - the same rule plotDistributions applies, and for the same reason: nothing would distinguish
  # a typo from a metric that is genuinely flat.
  has_any <- vapply(metrics, function(m) any(st$n_used[st$metric == m] > 0L), logical(1))
  if (!all(has_any)) {
    dropped <- metrics[!has_any]
    if (!any(has_any))
      .abort(c("No requested metric has a usable dive in any deployment: {.val {dropped}}.",
               "i" = "Check {.arg metrics} against the columns {.fn diveMetrics} returned."))
    cli::cli_warn(c("No usable dives for {.val {dropped}}; {?it was/they were} dropped.",
                    "i" = "Plotting {.val {metrics[has_any]}}."))
    metrics <- metrics[has_any]
  }

  # a metric dropped above must not remain the thing the cohort is ordered by
  if (!is.null(order.metric) && !order.metric %in% metrics) order.metric <- NULL
  grp <- .pdGroups(dm, id.col, group.by)
  ids <- .pdOrder(as.character(dm[[id.col]]), order.by, st, order.metric %||% metrics[1])
  lay <- .pdLayout(ids, grp, theme$palette)
  ids <- lay$ids
  st  <- .diveSummaryTable(dm, metrics, id.col = id.col, min.n = min.n, groups = grp)
  # the returned table is the figure's audit trail, so it reads in the order the panels do - a caller
  # reproducing the plot from it should not have to re-derive the ordering rule
  st <- st[order(match(st$id, ids), match(st$metric, metrics)), , drop = FALSE]
  rownames(st) <- NULL
  refs <- stats::na.omit(unique(as.character(dm$reference)))
  lab_of <- function(m) .pdResolveLabel(m, labels, refs)
  if (length(refs) > 1L && any(vapply(metrics, .diveIsDepth, logical(1))))
    cli::cli_warn(c("The cohort mixes {.val {sort(refs)}} references, and an absolute-depth metric is drawn.",
                    "i" = "Depths measured against different zeros are not comparable between deployments.",
                    "i" = "Consider {.val amplitude_m}, which is measured from each dive's own baseline."))
  # Deployments paginate, as in plotDepthProfiles: rows drive the figure's height, and past a point a
  # single page stops being readable however tall it is made.
  pages <- split(seq_along(ids), ceiling(seq_along(ids) / max.per.page))
  # Each metric's value axis is fixed ACROSS pages. Pages carrying different axes would not be a
  # comparison, which is the whole purpose of the figure.
  ranges <- stats::setNames(lapply(metrics, function(m)
    .pdMetricRange(.pdPerId(dm, m, ids, id.col), st, m, ids, trim)), metrics)

  # `.renderToDevices` calls this once per target and tells it whether the device can render Unicode;
  # the separator degrades to ASCII on a device that cannot, rather than emitting a <U+00B7> box.
  # `trim` is a deliberate, non-family-default decision, so how many dives it moved must not exist only
  # as text on the figure: a caller reproducing the plot from the returned table has to be able to see
  # where the axis was cut.
  drawn_rec <- list()
  draw <- function(to.file = FALSE, unicode = TRUE) {
    sep <- if (unicode) " \u00b7 " else " | "

    old <- graphics::par(family = theme$font.family, mar = c(3.4, 0.9, 2.4, 3.2),
                         oma = c(2.4, 5.6, 3.2, 0))
    on.exit(graphics::par(old), add = TRUE)
    for (pg in seq_along(pages)) {
      idx <- pages[[pg]]
      # positions are re-based per page so a page always fills its own height, and reversed so the
      # first deployment sits at the TOP - reading order, as in plotDistributions
      xp   <- lay$xpos[idx]
      ypos <- max(xp) - xp + 1
      blk  <- .pdPageBlocks(lay$blocks, xp)
      graphics::layout(matrix(seq_along(metrics), nrow = 1))
      for (k in seq_along(metrics)) {
        rec <- .pdDrawPanel(dm, st, metrics[k], ids[idx], theme, trim, id.col,
                            show.labels = k == 1L, label = lab_of(metrics[k]),
                            ypos = ypos, cols = lay$cols[idx], blocks = blk,
                            xlim = ranges[[metrics[k]]])
        # the record describes the whole cohort, so it is assembled across pages rather than overwritten
        drawn_rec[[metrics[k]]] <<- .pdMergeRecords(drawn_rec[[metrics[k]]], rec)
      }
      graphics::mtext(paste0("Dive metrics by deployment",
                             if (length(pages) > 1L) sprintf("  (%d/%d)", pg, length(pages)) else ""),
                      outer = TRUE, side = 3, line = 1.2, font = 2,
                      cex = 1.1 * theme$cex, col = theme$ink)
      if (length(refs))
        graphics::mtext(paste0("depth reference: ", paste(sort(refs), collapse = sep)),
                        outer = TRUE, side = 3, line = 0.1, cex = 0.72 * theme$cex,
                        col = grDevices::adjustcolor(theme$ink, alpha.f = 0.75))
      .pdDrawKey(theme)
    }
  }
  # Width comes from the METRIC count and height from the deployments on a page. That is the whole point
  # of the transpose: the cohort used to drive WIDTH, which cannot grow indefinitely - at 49 deployments
  # it asked for a 56-inch canvas and every label was scaled into illegibility.
  .renderToDevices(draw, plot = plot, plot.file = plot.file,
                   width  = max(6, 3.2 * length(metrics) + 2.2),
                   height = max(4.5, 2.4 + 0.24 * max(vapply(pages, length, integer(1)))))

  # fold the render record back into the audit trail
  st$n_trimmed <- 0L; st$axis_max <- NA_real_
  for (m in names(drawn_rec)) {
    r <- drawn_rec[[m]]
    w <- st$metric == m
    st$n_trimmed[w] <- as.integer(r$trimmed[st$id[w]])
    st$axis_max[w]  <- r$hi
  }

  if (lvl >= 1L) .reportDivePlotSummary(lvl, st, metrics, ids, pages, trim, min.n, plot.file, start.time)
  invisible(st)
}

#' Accept a metrics table, or a list of them, and return one bound table.
#'
#' Deliberately does NOT accept processed tags and run the detector for you: the dive threshold is a
#' scientific decision that must be visible in the call that made it, not buried in a plotting call.
#' @keywords internal
#' @noRd
.pdBindMetrics <- function(data, id.col) {
  # BEFORE the generic emptiness guard, whose advice is about mistyped directories and would be wrong
  # here. A zero-row dive table is not a path error; it is a cohort in which nothing was detected, and
  # that deserves its own message. Checked first, or this branch is unreachable.
  if (is.data.frame(data) && !nrow(data))
    .abort(c("{.arg data} has no rows: there are no dives to plot.",
             "i" = "A cohort with no detected dives is a result, not a failure - check {.fn detectDives}.",
             "i" = "If the detection floor is too high, lower {.code diveControl(depth.threshold = )}."))
  .assert_nonempty(data, "data")
  if (is.data.frame(data)) dm <- data
  else if (is.list(data)) {
    parts <- Filter(function(z) is.data.frame(z) && nrow(z), data)
    if (!length(parts))
      .abort(c("{.arg data} holds no dive-metrics rows.",
               "i" = "Pass the result of {.fn diveMetrics}."))
    dm <- do.call(rbind, parts)
  } else {
    .abort(c("{.arg data} must be a {.fn diveMetrics} table, or a list of them.",
             "i" = "Got {.cls {class(data)[1]}}."))
  }
  # `x == NA` is NA and `df[NA, ]` returns a row of NAs rather than none, so an NA id would both invent
  # a phantom deployment and inflate every real one's counts. Drop them loudly.
  if (id.col %in% names(dm) && anyNA(dm[[id.col]])) {
    n_na <- sum(is.na(dm[[id.col]]))
    cli::cli_warn(c("{n_na} row{?s} ha{?ve/s} no {.field {id.col}} and {?was/were} dropped.",
                    "i" = "A dive with no deployment id cannot be placed on the figure."))
    dm <- dm[!is.na(dm[[id.col]]), , drop = FALSE]
    if (!nrow(dm)) .abort("Every row lacks a {.field {id.col}}; there is nothing to plot.")
  }
  need <- c(id.col, "dive_id", "duration_s")
  miss <- setdiff(need, names(dm))
  if (length(miss))
    .abort(c("{.arg data} is missing {.field {miss}}, so it is not a {.fn diveMetrics} table.",
             "i" = "Run {.fn diveMetrics} on the output of {.fn detectDives}."))
  dm
}


#' Order the deployment slots. One order, shared by every panel.
#'
#' Panels that ordered themselves would let a reader compare the wrong columns between them, so the
#' order is decided once from `order.metric` and applied everywhere - the same rule plotDistributions
#' uses, and the same direction (largest median first).
#' @keywords internal
#' @noRd
.pdOrder <- function(id_vec, order.by, st, order.metric) {
  ids <- unique(id_vec)                              # order of first appearance = "input"
  if (identical(order.by, "input")) return(ids)
  if (identical(order.by, "id")) return(sort(ids))
  meds <- vapply(ids, function(id) {
    v <- st$median[st$id == id & st$metric == order.metric]
    if (!length(v)) NA_real_ else v[1]
  }, numeric(1))
  ids[order(meds, decreasing = TRUE, na.last = TRUE)]
}


#' Resolve a panel's axis label: an explicit `labels` entry wins, otherwise the built-in one.
#'
#' Extracted rather than left inline so it can be tested directly. A label only ever reaches the figure,
#' never the returned table, so a test that calls plotDives() and checks for no error proves nothing
#' about whether the override was honoured - which is how a broken override survived a first pass here.
#' @keywords internal
#' @noRd
.pdResolveLabel <- function(metric, labels = NULL, reference = NULL) {
  if (!is.null(labels) && metric %in% names(labels)) unname(labels[[metric]])
  else .diveLabel(metric, reference)
}


#' Resolve a per-deployment grouping label from the flexible `group` spec.
#'
#' Blank strings are not group labels - they arrive from spreadsheets constantly and would otherwise
#' become a group called "".
#' @keywords internal
#' @noRd
.pdGroups <- function(dm, id.col, group) {
  if (is.null(group)) return(NULL)
  ids <- unique(as.character(dm[[id.col]]))
  g <- vapply(ids, function(id) {
    sub <- dm[as.character(dm[[id.col]]) == id, , drop = FALSE]
    as.character(.deploymentGroup(sub, id, group))[1]
  }, character(1))
  g[!is.na(g) & !nzchar(trimws(g))] <- NA_character_
  if (all(is.na(g)))
    cli::cli_warn(c("No deployment has a usable {.arg group.by} value; drawing one ungrouped block.",
                    "i" = "Check that the grouping column or lookup covers the deployment ids."))
  stats::setNames(g, ids)
}

#' Slot positions, colours and block spans for the categorical axis.
#'
#' Deployments a grouping does not cover go into a trailing `(ungrouped)` block rather than being
#' dropped. Every deployment owns a slot here - unlike a facetted plotter, where an ungrouped
#' deployment has no panel to live in - so silently omitting one would mean a figure that claims to
#' show a cohort while showing part of it.
#' @keywords internal
#' @noRd
.pdLayout <- function(ids, grp, palette = NULL, gap = 0.75) {
  if (is.null(grp)) {
    return(list(ids = ids, xpos = seq_along(ids), cols = NULL, blocks = NULL))
  }
  g <- grp[ids]
  lev <- sort(unique(stats::na.omit(g)))
  ung <- any(is.na(g))
  ord <- unlist(lapply(lev, function(l) ids[!is.na(g) & g == l]), use.names = FALSE)
  if (ung) ord <- c(ord, ids[is.na(g)])
  gl <- c(lev, if (ung) "(ungrouped)")
  key <- ifelse(is.na(grp[ord]), "(ungrouped)", grp[ord])

  x <- numeric(length(ord)); at <- 0
  blocks <- list()
  for (l in gl) {
    w <- which(key == l)
    x[w] <- at + seq_along(w)
    blocks[[l]] <- c(at + 1, at + length(w))
    at <- at + length(w) + gap
  }
  pal <- .themePalette(palette, length(gl))
  list(ids = ord, xpos = x, cols = stats::setNames(pal, gl)[key],
       blocks = blocks)
}
