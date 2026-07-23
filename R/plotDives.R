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

#' Is this metric an ABSOLUTE depth (inverted axis, zero at the top) or a magnitude (upright)?
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

#' Draw ONE metric panel into the current plot region.
#'
#' Every dive is drawn. Dives excluded from the statistics are drawn in outline rather than removed,
#' because a reader cannot judge a median they cannot see the basis of - and the two reasons a dive is
#' excluded (censored, phase-unsupported) are not interchangeable, so the panel note names both.
#' @keywords internal
#' @noRd
.pdDrawPanel <- function(dm, st, metric, ids, theme, trim, id.col, show.axis = TRUE,
                         label = NULL, xpos = NULL, cols = NULL, blocks = NULL) {
  xpos <- xpos %||% seq_along(ids)
  cols <- cols %||% rep(theme$axis, length(ids))

  is_depth <- .diveIsDepth(metric)
  per_id <- lapply(ids, function(id) {
    sub <- dm[as.character(dm[[id.col]]) == id, , drop = FALSE]
    if (!metric %in% names(sub)) return(list(used = numeric(0), excl = numeric(0)))
    u <- .diveUsable(sub, metric)
    list(used = u$value[u$used], excl = u$value[u$censored])   # unsupported are NA: nothing to place
  })

  used_all <- unlist(lapply(per_id, `[[`, "used"))
  excl_all <- unlist(lapply(per_id, `[[`, "excl"))
  if (!length(used_all)) used_all <- 0
  # The axis is governed by the dives that COUNT - an excluded outlier must not set the scale for a
  # median it took no part in - but it must still CONTAIN every marker the figure draws, or a
  # deployment whose median sits above the pooled quantile has its marker clipped away silently while
  # the returned table reports drawn = TRUE. Measured: two deployments at 5-15 m beside one at
  # 800-1200 m gave an axis top of 14.9 m and a deep marker that rendered to nothing.
  hi <- stats::quantile(used_all, trim, names = FALSE, na.rm = TRUE)
  mk <- st[st$metric == metric & st$id %in% ids & st$drawn, , drop = FALSE]
  if (nrow(mk)) hi <- max(hi, max(mk$q75, mk$median, na.rm = TRUE), na.rm = TRUE)
  # ...and the floor must reach the excluded points too, or they are clipped below and never counted,
  # which contradicts the promise that excluded dives are drawn rather than removed.
  lo <- min(c(used_all, excl_all, if (nrow(mk)) mk$q25 else NULL, 0), na.rm = TRUE)
  if (!is.finite(hi) || hi <= lo) hi <- lo + 1
  ylim <- if (is_depth) c(hi, lo) else c(lo, hi)                # depth: zero at the top

  graphics::plot.new()
  graphics::plot.window(xlim = c(min(xpos) - 0.6, max(xpos) + 0.6), ylim = ylim, xaxs = "i")
  graphics::rect(graphics::par("usr")[1], graphics::par("usr")[3],
                 graphics::par("usr")[2], graphics::par("usr")[4],
                 col = theme$panel, border = NA)
  ticks <- graphics::axTicks(2)
  graphics::abline(h = ticks, col = theme$grid, lwd = 0.8)      # horizontal only: x is categorical
  if (is_depth) graphics::abline(h = 0, col = theme$ink, lty = 2, lwd = 0.9)

  clipped <- 0L
  trimmed <- integer(length(ids))
  for (i in seq_along(ids)) {
    d <- per_id[[i]]
    n <- length(d$used)
    # a dense cloud and a sparse one must both stay readable, so alpha tracks n rather than being fixed
    al <- max(0.22, min(0.70, 0.70 * sqrt(30 / max(n, 1))))
    # Deterministic spread, not runif(): a published figure must redraw identically, and a golden-ratio
    # sequence scatters more evenly across the slot than random jitter does at small n.
    xi <- xpos[i]
    jit <- function(k) if (!k) numeric(0) else xi + (((seq_len(k) * 0.6180339887498949) %% 1) - 0.5) * 0.56
    pin <- function(v) list(inside = v[v >= lo & v <= hi], edge = sum(v > hi | v < lo))
    pu <- pin(d$used); pe <- pin(d$excl)
    clipped <- clipped + pu$edge + pe$edge
    if (length(pe$inside))
      graphics::points(jit(length(pe$inside)), pe$inside, pch = 1, cex = 0.5,
                       col = grDevices::adjustcolor(theme$ink, alpha.f = 0.45))
    if (length(pu$inside))
      graphics::points(jit(length(pu$inside)), pu$inside, pch = 16, cex = 0.55,
                       col = grDevices::adjustcolor(cols[i], alpha.f = al))
    if (pu$edge + pe$edge > 0) {
      above <- sum(d$used > hi, na.rm = TRUE) + sum(d$excl > hi, na.rm = TRUE)
      below <- (pu$edge + pe$edge) - above
      if (above) graphics::points(xi, hi, pch = 2, cex = 0.8, col = theme$ink)
      if (below) graphics::points(xi, lo, pch = 6, cex = 0.8, col = theme$ink)
    }
    trimmed[i] <- pu$edge + pe$edge

    s <- st[st$id == ids[i] & st$metric == metric, ]
    if (nrow(s) && isTRUE(s$drawn)) {
      graphics::segments(xi, s$q25, xi, s$q75, lwd = 1.7, col = theme$ink)
      graphics::segments(c(xi, xi) - 0.10, c(s$q25, s$q75), c(xi, xi) + 0.10, c(s$q25, s$q75),
                         lwd = 1.7, col = theme$ink)                       # caps, so the IQR reads as a range
      graphics::segments(xi - 0.22, s$median, xi + 0.22, s$median, lwd = 2.6, col = theme$ink)
    }
    # this panel's own n, under this panel. Usable count, not dive count - they differ by metric, and
    # a single row of counts under a shared axis would attribute the last panel's n to all of them.
    if (nrow(s))
      graphics::mtext(paste0("n=", s$n_used), side = 1, at = xi, line = 0.15,
                      cex = 0.6 * theme$cex, col = grDevices::adjustcolor(theme$ink, alpha.f = 0.8))
  }

  graphics::axis(2, at = ticks, las = 1, cex.axis = 0.85 * theme$cex, col = NA, col.axis = theme$ink)
  graphics::mtext(label %||% .diveLabel(metric), side = 2, line = 2.6,
                  cex = 0.9 * theme$cex, col = theme$ink)
  if (show.axis) {
    graphics::axis(1, at = xpos, labels = ids, las = 2, tick = FALSE,
                   cex.axis = 0.8 * theme$cex, col.axis = theme$ink)
    if (!is.null(blocks))
      for (l in names(blocks))
        graphics::mtext(l, side = 1, line = 3.9, at = mean(blocks[[l]]), font = 2,
                        cex = 0.72 * theme$cex, col = theme$ink)
  }
  if (clipped > 0)
    graphics::mtext(sprintf("%d dive%s beyond the axis", clipped, if (clipped != 1) "s" else ""),
                    side = 3, adj = 1, line = -1.1, cex = 0.62 * theme$cex,
                    col = grDevices::adjustcolor(theme$ink, alpha.f = 0.7))
  graphics::box(col = theme$grid)
  # A structured record of what was actually drawn. Without it the whole rendering layer is unverifiable
  # from a test - every mutation planted in this function (trim removed, the depth inversion removed,
  # the median drawn at q25, the IQR drawn as min..max) passed a green suite.
  invisible(list(metric = metric, ylim = ylim, lo = lo, hi = hi, inverted = is_depth,
                 ids = ids, xpos = xpos, trimmed = stats::setNames(trimmed, ids),
                 clipped = clipped,
                 markers = stats::setNames(lapply(seq_along(ids), function(i) {
                   z <- st[st$id == ids[i] & st$metric == metric, ]
                   if (!nrow(z) || !isTRUE(z$drawn)) NULL
                   else c(median = z$median, q25 = z$q25, q75 = z$q75)
                 }), ids)))
}


#' Compare deployments on per-dive metrics
#'
#' @description
#' Draws every dive as a point in its deployment's column, with a median and interquartile marker over
#' it, for one or more metrics from \code{\link{diveMetrics}}.
#'
#' It is not a per-sample distribution plot (\code{\link{plotDistributions}}), not a time budget
#' (\code{\link{plotTimeAtDepth}}), not a depth trace (\code{\link{plotDepthProfiles}}) and not detector
#' QC (\code{\link{detectDives}}'s `plot.file`). Nothing it draws is computed here that
#' \code{\link{diveMetrics}} did not already compute.
#'
#' @param data A `nautilus_dive_metrics` table from \code{\link{diveMetrics}}, or a list of them.
#' @param metrics Character. Columns to draw, one panel each. `NULL` (default) uses
#'   `c("amplitude_m", "duration_s")`.
#' @param labels Named character mapping a metric to its axis label, overriding the built-in ones.
#' @param group.by Grouping for the deployments: a column name (resolved from the table or the tag
#'   metadata), a named `id -> group` vector, or a two-column `data.frame`. `NULL` (default) draws one
#'   ungrouped block. Deployments the grouping does not cover are drawn in a trailing `(ungrouped)`
#'   block rather than dropped - see Details.
#' @param order.by How to order the deployment slots - one order, shared by every panel:
#'   `"id"` (default, alphabetical), `"input"` (order of first appearance), or `"median"` (largest
#'   median first, on `order.metric`).
#' @param order.metric Metric whose per-deployment median drives `order.by = "median"`. `NULL`
#'   (default) uses the first entry of `metrics`.
#' @param trim Numeric in (0, 1]. Upper quantile bounding the value axis; `1` shows the full range.
#'   Points beyond it are pinned to the axis edge and counted in a panel note, never dropped silently.
#'   Defaults to `0.95`, NOT the `0.995` the rest of the plotting family uses, because per-dive metrics
#'   are far more skewed than the per-sample kinematics that default was chosen for: on a real cohort
#'   `amplitude_m` had a median of 12.2 m against a maximum of 1414.1 m. At `0.995` the axis reached
#'   417 m and every median and IQR in the figure collapsed onto the baseline; at `0.95` it reaches
#'   92.8 m and the between-deployment differences the plot exists to show become legible. The 5% that
#'   moves is pinned and counted, and the untrimmed extremes remain in the returned `max` column.
#' @param min.n Integer. Dives needed before a median/IQR marker is drawn. Points are always drawn.
#' @param theme A \link{plotTheme} object, or a list of overrides.
#' @param plot Logical. Draw to the active device.
#' @param plot.file Character. Path to a PDF.
#' @param id.col Character. Deployment id column.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal", or `2`/"detailed" (default).
#'
#' @details
#' \strong{Why `amplitude_m` is the default and `max_depth_m` is not.} `amplitude_m` is measured from
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
#' \strong{Every dive is a point; the marker is only a marker.} No bar is drawn. A bar encodes
#' magnitude as a length from a true zero, which under `reference = "baseline"` does not exist, and a
#' per-individual maximum is an extreme whose expectation grows with n - which ranged from 1 to 424
#' dives per deployment in that cohort. The median is drawn only once `min.n` dives support it.
#'
#' \strong{Censoring is applied per metric.} `inter_dive_s` is included on `!inter_dive_censored`;
#' every other metric on `complete`. Excluded dives are drawn in outline and counted; there is no
#' argument to fold them back into the statistics. A separate and usually larger loss is reported
#' separately: dives whose phase structure did not support the metric at all.
#'
#' @return Invisibly, a `data.frame` with one row per deployment and metric: `id`, `group`, `metric`,
#'   `n_dives`, `n_used`, `n_censored`, `n_unsupported`, `median`, `q25`, `q75`, `min`, `max`,
#'   `reference`, `direction`, `drawn`, plus `n_trimmed` and `axis_max` recording how many dives `trim`
#'   pinned to the axis edge and where that edge fell - so the figure can be reproduced from the table.
#' @seealso \link{diveMetrics}, \link{detectDives}, \link{plotDistributions}, \link{plotTimeAtDepth}
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

  .log_header(lvl, "plotDives", "Comparing deployments on per-dive metrics",
              bullets = sprintf("Input: %d dive%s across %d deployment%s \u00b7 %d metric%s",
                                nrow(dm), if (nrow(dm) != 1) "s" else "",
                                length(unique(dm[[id.col]])),
                                if (length(unique(dm[[id.col]])) != 1) "s" else "",
                                length(metrics), if (length(metrics) != 1) "s" else ""))

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
  # `.renderToDevices` calls this once per target and tells it whether the device can render Unicode;
  # the separator degrades to ASCII on a device that cannot, rather than emitting a <U+00B7> box.
  # `trim` is a deliberate, non-family-default decision, so how many dives it moved must not exist only
  # as text on the figure: a caller reproducing the plot from the returned table has to be able to see
  # where the axis was cut.
  drawn_rec <- list()
  draw <- function(to.file = FALSE, unicode = TRUE) {
    sep <- if (unicode) " \u00b7 " else " | "

    old <- graphics::par(family = theme$font.family, mar = c(1.9, 4.4, 0.8, 1.2), oma = c(3.6, 0, 2.2, 0))
    on.exit(graphics::par(old), add = TRUE)
    graphics::layout(matrix(seq_along(metrics), ncol = 1))
    for (k in seq_along(metrics)) {
      rec <- .pdDrawPanel(dm, st, metrics[k], ids, theme, trim, id.col,
                          show.axis = k == length(metrics), label = lab_of(metrics[k]),
                          xpos = lay$xpos, cols = lay$cols, blocks = lay$blocks)
      drawn_rec[[metrics[k]]] <<- rec
    }
    graphics::mtext("Dive metrics by deployment", outer = TRUE, side = 3, line = 0.5,
                    font = 2, cex = 1.15 * theme$cex, col = theme$ink)
    refs <- unique(stats::na.omit(dm$reference))
    if (length(refs))
      graphics::mtext(paste0("reference: ", paste(sort(refs), collapse = " + "), sep,
                             "filled = used", sep, "open = excluded", sep,
                             "triangle = beyond axis", sep, "bar = median with IQR"),
                      outer = TRUE, side = 3, line = -0.7, cex = 0.72 * theme$cex,
                      col = grDevices::adjustcolor(theme$ink, alpha.f = 0.75))
  }
  .renderToDevices(draw, plot = plot, plot.file = plot.file,
                   width = max(6, 1.1 * length(ids) + 2.5),
                   height = max(4, 2.6 * length(metrics) + 1.6))

  # fold the render record back into the audit trail
  st$n_trimmed <- 0L; st$axis_max <- NA_real_
  for (m in names(drawn_rec)) {
    r <- drawn_rec[[m]]
    w <- st$metric == m
    st$n_trimmed[w] <- as.integer(r$trimmed[st$id[w]])
    st$axis_max[w]  <- r$hi
  }

  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, length(metrics), " metric", if (length(metrics) != 1) "s",
              " over ", length(ids), " deployment", if (length(ids) != 1) "s")
    for (m in metrics) {
      sm <- st[st$metric == m, ]
      .log_arrow(lvl, sprintf("%s: %s of %s dives used", lab_of(m),
                              format(sum(sm$n_used), big.mark = ","),
                              format(sum(sm$n_dives), big.mark = ",")))
      # the two exclusions are reported apart because they mean different things and differ in size by
      # more than an order of magnitude on real data
      if (sum(sm$n_censored) > 0 || sum(sm$n_unsupported) > 0)
        .log_detail(lvl, sprintf("excluded: %d censored \u00b7 %d not supported by the dive's phases",
                                 sum(sm$n_censored), sum(sm$n_unsupported)))
      if (sum(sm$n_trimmed, na.rm = TRUE) > 0)
        .log_detail(lvl, sprintf("%d dive%s beyond the axis at trim = %.3g (pinned, not dropped)",
                                 sum(sm$n_trimmed, na.rm = TRUE),
                                 if (sum(sm$n_trimmed, na.rm = TRUE) != 1) "s" else "", trim))
      if (any(!sm$drawn))
        .log_detail(lvl, sprintf("no median drawn for %s (fewer than %d usable dives)",
                                 paste(sm$id[!sm$drawn], collapse = ", "), min.n))
    }
    if (!is.null(plot.file)) .log_arrow(lvl, "output: ", plot.file)
    .log_runtime(lvl, start.time)
  }
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
