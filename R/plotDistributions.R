#######################################################################################################
# Plot per-individual distributions of kinematic / effort metrics #####################################
#######################################################################################################

#' Plot per-individual distributions of kinematic and effort metrics
#'
#' @description
#' Summarises and plots the DISTRIBUTION of one or more per-sample metrics (tail-beat frequency, swimming
#' speed, activity, ...) across a cohort of deployments. Each metric becomes a column: a stack of
#' per-individual horizontal violins (one row per deployment), a pooled "population" density strip on
#' top, and a reference line (the population median by default). This surfaces the multimodal, long-tailed
#' structure that a single mean hides - the natural companion to the scalar summaries from
#' \link{summarizeTagData}.
#'
#' Where \link{summarizeTagData} returns one number per deployment, `plotDistributions()` shows the whole
#' shape and returns a tidy per-deployment, per-metric distribution summary (invisibly) alongside the plot.
#'
#' Like the other workflow functions, `data` accepts the package's canonical input forms - a character
#' vector of `.rds` paths, a single `nautilus_tag` / data.frame, or a list of them - and the deployment
#' id is read from each object's metadata.
#'
#' @param data Processed data: a character vector of `.rds` file paths, a single `nautilus_tag` /
#'   data.frame, or a list of them (see \link{processTagData}).
#' @param metrics Character vector of metric columns to plot (one panel each), in order. `NULL` (default)
#'   auto-selects the kinematic / effort metrics present in the data
#'   (`tbf_hz`, `paddle_speed`, `speed`, `vedba`, `odba`, `vertical_velocity`).
#' @param labels Named character vector mapping a metric to its axis label. Any metric not named here
#'   falls back to a built-in label (e.g. `"Tail-beat frequency (Hz)"`) or the column name.
#' @param order.by How to order deployments down the y-axis (one order, shared by every panel):
#'   `"id"` (default, alphabetical), `"input"` (as supplied), or `"median"` (by the median of
#'   `order.metric`).
#' @param order.metric Metric whose per-deployment median drives `order.by = "median"`. `NULL` (default)
#'   uses the first metric. Ignored otherwise.
#' @param reference Population reference line(s) drawn per panel: any of `"median"` (default) and
#'   `"mean"`, or `NULL` for none.
#' @param show.marginal Logical. Draw the pooled population-density strip above each panel. Default `TRUE`.
#' @param trim Numeric in (0, 1]. Upper quantile of the pooled data used for the x-axis limit, so long
#'   right tails do not compress the bulk. Default `0.995`; set `1` for the full range.
#' @param min.n Integer. Minimum finite samples a deployment needs for a violin to be drawn; below it the
#'   row is left blank (but still summarised). Default `30`.
#' @param colors Character vector of per-metric fill colours (recycled). `NULL` (default) uses a built-in
#'   qualitative palette.
#' @param plot Logical. If `TRUE` (default), draw to the active graphics device.
#' @param plot.file Character. Path to a PDF to draw into (independent of `plot`; set either or both).
#'   The function manages the device itself. Default `NULL`.
#' @param cex Numeric. Master text-scaling factor. Default 1.
#' @param id.col Character. Name of the deployment id column. Default `"ID"`.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet" (silent), `TRUE`/`1`/"normal" (header + summary), or
#'   `2`/"detailed" (default): additionally reports per-metric drawn/sample counts and shows a live
#'   progress bar while the tags are read (cli auto-hides it for fast runs).
#'
#' @return Invisibly, a tidy data frame with one row per deployment and metric and columns `id`,
#'   `metric`, `n`, `mean`, `median`, `sd`, `q05`, `q25`, `q75`, `q95`, `min`, `max` (called mainly for
#'   the side-effect plot). Deployments lacking a metric appear with `n = 0` and `NA` statistics.
#'
#' @seealso \link{summarizeTagData}, \link{calculateTailBeats}, \link{processTagData}
#' @examples
#' \dontrun{
#' # Compare tail-beat frequency and swimming speed across the cohort, into a PDF
#' plotDistributions(list.files("./processed", full.names = TRUE),
#'                   metrics = c("tbf_hz", "paddle_speed"),
#'                   plot = FALSE, plot.file = "./plots/distributions.pdf")
#' }
#' @export

plotDistributions <- function(data,
                              metrics       = NULL,
                              labels        = NULL,
                              order.by      = c("id", "input", "median"),
                              order.metric  = NULL,
                              reference     = "median",
                              show.marginal = TRUE,
                              trim          = 0.995,
                              min.n         = 30,
                              colors        = NULL,
                              plot          = TRUE,
                              plot.file     = NULL,
                              cex           = 1,
                              id.col        = "ID",
                              verbose       = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)

  ##############################################################################
  # Validate arguments #########################################################
  ##############################################################################

  order.by <- match.arg(order.by)
  reference <- if (is.null(reference)) character(0) else match.arg(reference, c("median", "mean"), several.ok = TRUE)
  .assert_flag(show.marginal, "show.marginal"); .assert_flag(plot, "plot")
  .assert_number(trim, "trim", min = 0); if (trim > 1) .abort("{.arg trim} must be in (0, 1].")
  .assert_count(min.n, "min.n", min = 1)
  .assert_number(cex, "cex", min = 0)
  .assert_string(id.col, "id.col"); .assert_string(order.metric, "order.metric", null_ok = TRUE)
  .assert_writable_file(plot.file, "plot.file", ext = "pdf")     # fail-fast: parent dir must exist
  if (!is.null(metrics) && (!is.character(metrics) || !length(metrics)))
    .abort("{.arg metrics} must be a non-empty character vector of column names, or {.code NULL}.")
  if (!is.null(labels) && (!is.character(labels) || is.null(names(labels))))
    .abort("{.arg labels} must be a NAMED character vector (metric -> label).")
  if (!is.null(colors) && (!is.character(colors) || !length(colors)))
    .abort("{.arg colors} must be a non-empty character vector of colours, or {.code NULL}.")
  if (!plot && is.null(plot.file))
    .abort(c("Nothing to plot.", "i" = "Set {.arg plot = TRUE} or provide a {.arg plot.file}."))

  src <- .resolveInput(data, id.col)

  ##############################################################################
  # Resolve which metrics to plot (auto-detect from the first-seen columns) ####
  ##############################################################################

  first_cols <- names(data.table::as.data.table(src$get(1)))
  if (is.null(metrics)) {
    metrics <- intersect(.distAutoMetrics(), first_cols)
    if (!length(metrics))
      .abort(c("No kinematic / effort metrics found to plot.",
               "i" = "Provide {.arg metrics} explicitly (available numeric columns include {.val {utils::head(setdiff(first_cols, c(id.col, 'datetime')), 8)}})."))
  }
  if (!is.null(order.metric) && !order.metric %in% metrics)
    .abort("{.arg order.metric} ({.val {order.metric}}) must be one of {.arg metrics}: {.val {metrics}}.")

  metric_labels <- vapply(metrics, function(m) .distLabel(m, labels), character(1))
  metric_cols   <- if (is.null(colors)) .qualitativePalette(length(metrics)) else rep(colors, length.out = length(metrics))

  .log_header(lvl, "plotDistributions", "Plotting metric distributions",
              bullets = sprintf("Input: %d deployment%s \u00b7 %d metric%s (%s)",
                                src$n, if (src$n != 1) "s" else "",
                                length(metrics), if (length(metrics) != 1) "s" else "",
                                paste(metrics, collapse = ", ")))

  ##############################################################################
  # Collect per-deployment values + a tidy distribution summary ################
  ##############################################################################

  values  <- stats::setNames(vector("list", length(metrics)), metrics)   # metric -> named list (id -> numeric)
  summary_rows <- list()
  ids <- character(0)
  pb <- .log_progress_start(lvl, src$n, "Loading")                       # live bar at detailed verbosity (lvl >= 2)
  for (i in seq_len(src$n)) {
    .log_progress_step(pb)
    dt <- data.table::as.data.table(src$get(i))
    id <- as.character(.getMeta(dt)$id %||% src$ids[i])
    ids <- c(ids, id)
    for (m in metrics) {
      x <- if (m %in% names(dt)) suppressWarnings(as.numeric(dt[[m]])) else numeric(0)
      x <- x[is.finite(x)]
      values[[m]][[id]] <- x
      summary_rows[[length(summary_rows) + 1L]] <- .distSummaryRow(id, m, x)
    }
  }
  .log_progress_done(pb)
  summary <- do.call(rbind, summary_rows); rownames(summary) <- NULL

  ##############################################################################
  # Deployment order (shared across panels), geometry, densities ###############
  ##############################################################################

  ord_ids <- .distOrder(ids, order.by, values[[order.metric %||% metrics[1]]])
  ny <- length(ord_ids)

  # per-metric geometry: pooled samples -> x-limits, reference values, and the pooled marginal density
  geom <- lapply(metrics, function(m) {
    pooled <- unlist(values[[m]], use.names = FALSE)
    xlim <- .distXlim(pooled, trim)
    refs <- .distReferences(pooled, reference)
    marg <- if (show.marginal && length(pooled) >= min.n) .distDensity(pooled, xlim) else NULL
    list(pooled_n = length(pooled), xlim = xlim, refs = refs, marginal = marg)
  })
  names(geom) <- metrics

  n_drawn <- vapply(metrics, function(m) sum(vapply(ord_ids, function(id) length(values[[m]][[id]]) >= min.n, logical(1))), integer(1))

  ##############################################################################
  # Draw #######################################################################
  ##############################################################################

  M <- length(metrics)
  dims <- .distDeviceSize(M, ny, show.marginal, cex)

  draw <- function(to.file = FALSE, unicode = TRUE) {
    lay <- if (show.marginal) matrix(c(seq_len(M), M + seq_len(M)), nrow = 2, byrow = TRUE) else matrix(seq_len(M), nrow = 1)
    # fixed-height marginal strip (absolute cm), so it never collapses under its title margin when there
    # are few deployments; the violin row takes the remaining height.
    graphics::layout(lay, heights = if (show.marginal) c(graphics::lcm(2.4), 1) else 1)
    graphics::par(oma = c(0, 0, 0, 0), mgp = c(3, 0.7, 0))

    left1 <- max(4.5, 0.55 * max(nchar(ord_ids)) * cex)          # room for the id labels in the first column
    # top row: pooled marginal densities
    if (show.marginal) for (j in seq_len(M)) {
      m <- metrics[j]
      .drawMarginalPanel(geom[[m]], metric_labels[j], metric_cols[j],
                         mar = c(0.4, if (j == 1) left1 else 0.8, 2.7, 1.2), cex = cex, first = (j == 1))
    }
    # bottom row: per-deployment violins
    for (j in seq_len(M)) {
      m <- metrics[j]
      .drawViolinPanel(values[[m]], ord_ids, geom[[m]], metric_cols[j], min.n,
                       mar = c(4, if (j == 1) left1 else 0.8, 0.4, 1.2), cex = cex,
                       first = (j == 1), title = if (show.marginal) NULL else metric_labels[j])
    }
  }

  .renderToDevices(draw, plot = plot, plot.file = plot.file, width = dims$width, height = dims$height)

  ##############################################################################
  # Summary ####################################################################
  ##############################################################################

  if (lvl >= 1L) {
    .log_summary(lvl)
    for (j in seq_len(M))
      .log_detail(lvl, sprintf("%s: %d/%d deployment%s drawn (%s samples)", metrics[j], n_drawn[j], ny,
                               if (ny != 1) "s" else "", format(geom[[metrics[j]]]$pooled_n, big.mark = ",")))
    .log_done(lvl, M, " metric", if (M != 1) "s", " across ", ny, " deployment", if (ny != 1) "s", " plotted")
    if (!is.null(plot.file)) .log_arrow(lvl, "plots: ", plot.file)
    .log_runtime(lvl, start.time)
  }

  invisible(summary)
}


################################################################################
# Internal helpers ###########################################################
################################################################################

#' The metrics auto-selected when `metrics = NULL` (kinematic / effort, not environmental).
#' @keywords internal
#' @noRd
.distAutoMetrics <- function() c("tbf_hz", "paddle_speed", "speed", "vedba", "odba", "vertical_velocity")

#' Axis label for a metric: an explicit `labels` entry, else a built-in default, else the column name.
#' @keywords internal
#' @noRd
.distLabel <- function(metric, labels = NULL) {
  known <- c(tbf_hz = "Tail-beat frequency (Hz)", tbf_amplitude = "Tail-beat amplitude",
             paddle_speed = "Paddle speed (m/s)", speed = "Speed (m/s)", vedba = "VeDBA (g)",
             odba = "ODBA (g)", vertical_velocity = "Vertical velocity (m/s)",
             depth = "Depth (m)", temp = "Temperature (\u00b0C)")
  if (!is.null(labels) && metric %in% names(labels)) unname(labels[[metric]])
  else if (metric %in% names(known)) unname(known[metric])
  else metric
}

#' One tidy distribution-summary row for a deployment x metric (NA statistics when empty).
#' @keywords internal
#' @noRd
.distSummaryRow <- function(id, metric, x) {
  if (!length(x)) {
    q <- rep(NA_real_, 6)
    return(data.frame(id = id, metric = metric, n = 0L, mean = NA_real_, median = NA_real_, sd = NA_real_,
                      q05 = q[1], q25 = q[2], q75 = q[3], q95 = q[4], min = q[5], max = q[6],
                      stringsAsFactors = FALSE))
  }
  qs <- stats::quantile(x, c(.05, .25, .75, .95), na.rm = TRUE, names = FALSE)
  data.frame(id = id, metric = metric, n = length(x), mean = mean(x), median = stats::median(x),
             sd = stats::sd(x), q05 = qs[1], q25 = qs[2], q75 = qs[3], q95 = qs[4],
             min = min(x), max = max(x), stringsAsFactors = FALSE)
}

#' Resolve the shared deployment order (returns ids top-to-bottom, i.e. first = top row).
#' @keywords internal
#' @noRd
.distOrder <- function(ids, order.by, order_values) {
  ids <- unique(ids)
  if (order.by == "input") return(ids)
  if (order.by == "id")    return(sort(ids))
  meds <- vapply(ids, function(id) { x <- order_values[[id]]; if (length(x)) stats::median(x, na.rm = TRUE) else NA_real_ }, numeric(1))
  ids[order(meds, decreasing = TRUE, na.last = TRUE)]          # highest median at the top
}

#' Upper-trimmed x-axis limits for a metric's pooled samples (robust to long tails).
#' @keywords internal
#' @noRd
.distXlim <- function(pooled, trim) {
  if (!length(pooled)) return(c(0, 1))
  lo <- min(pooled, na.rm = TRUE)
  hi <- if (trim >= 1) max(pooled, na.rm = TRUE) else stats::quantile(pooled, trim, na.rm = TRUE, names = FALSE)
  if (!is.finite(lo) || !is.finite(hi) || hi <= lo) hi <- lo + 1
  c(lo, hi)
}

#' Named vector of population reference values ("median"/"mean") for a metric's pooled samples.
#' @keywords internal
#' @noRd
.distReferences <- function(pooled, which) {
  out <- numeric(0)
  if (!length(pooled)) return(out)
  if ("median" %in% which) out["median"] <- stats::median(pooled, na.rm = TRUE)
  if ("mean"   %in% which) out["mean"]   <- mean(pooled, na.rm = TRUE)
  out
}

#' Colours for the reference lines.
#' @keywords internal
#' @noRd
.distRefStyle <- function() list(median = list(col = "#2C5F8A", lty = 2), mean = list(col = "#B03A2E", lty = 3))

#' Kernel density for the pooled marginal, spanning a slightly padded x-window.
#' @keywords internal
#' @noRd
.distDensity <- function(x, xlim) {
  pad <- 0.04 * diff(xlim)
  stats::density(x, n = 512, from = xlim[1] - pad, to = xlim[2] + pad, na.rm = TRUE)
}

#' Restrict a density estimate to an x-window (so a violin ends cleanly at the axis, not beyond it).
#' @keywords internal
#' @noRd
.clampDensity <- function(d, xlim) {
  keep <- d$x >= xlim[1] & d$x <= xlim[2]
  if (!any(keep)) return(d)
  list(x = d$x[keep], y = d$y[keep])
}

#' PDF device size (inches) for M metric columns x ny deployment rows. The height budgets a fixed
#' marginal strip (~0.95 in, matching the layout's lcm(2.4)), the violin-row chrome (x-axis), and a
#' per-deployment row height.
#' @keywords internal
#' @noRd
.distDeviceSize <- function(M, ny, show.marginal, cex) {
  marg_in <- if (show.marginal) 0.95 else 0
  list(width  = max(4, 3.1 * M + 1.2) * max(1, cex * 0.9),
       height = (marg_in + 0.95 + 0.19 * ny) * max(1, cex * 0.9))
}

#' Draw one reference line into the current panel (vertical, styled by kind).
#' @keywords internal
#' @noRd
.distRefLines <- function(refs, cex) {
  sty <- .distRefStyle()
  for (k in names(refs))
    graphics::abline(v = refs[[k]], col = sty[[k]]$col, lty = sty[[k]]$lty, lwd = 1.6)
}

#' Subtle panel chrome shared by both rows: a very light fill (grey97) with thin, unobtrusive vertical
#' grid lines at the metric ticks (grey88, one shade darker than the fill), drawn BEFORE the data so the
#' distributions stay the focus. Vertical only - horizontal lines would clutter the stacked-violin rows.
#' @keywords internal
#' @noRd
.distPanelBackground <- function(xlim) {
  usr <- graphics::par("usr")
  graphics::rect(usr[1], usr[3], usr[2], usr[4], col = "grey97", border = NA)
  gx <- pretty(xlim, n = 5); gx <- gx[gx > xlim[1] & gx < xlim[2]]      # interior ticks only (not on the border)
  if (length(gx)) graphics::abline(v = gx, col = "grey88", lwd = 0.5)
}

#' Top-row panel: the pooled population density strip for one metric.
#' @keywords internal
#' @noRd
.drawMarginalPanel <- function(g, label, fill, mar, cex, first) {
  graphics::par(mar = mar)
  d <- g$marginal
  ymax <- if (is.null(d)) 1 else max(d$y) * 1.08
  plot(NA, xlim = g$xlim, ylim = c(0, ymax), axes = FALSE, xaxs = "i", yaxs = "i", xlab = "", ylab = "")
  .distPanelBackground(g$xlim)                                    # subtle fill + grid, behind the data
  if (!is.null(d)) {
    xx <- c(d$x[1], d$x, d$x[length(d$x)]); yy <- c(0, d$y, 0)
    graphics::polygon(xx, yy, col = grDevices::adjustcolor(fill, 0.55), border = fill, lwd = 1.4)
  }
  .distRefLines(g$refs, cex)
  if (first) {
    graphics::axis(2, at = pretty(c(0, ymax), n = 2), las = 1, cex.axis = cex * 0.7, tcl = -0.25)
    graphics::mtext("Density", side = 2, line = 2.3, cex = cex * 0.7)
  }
  graphics::mtext(label, side = 3, line = 0.5, font = 2, cex = cex * 0.92)   # column title
  graphics::box()                                                # full border around the panel
}

#' Bottom-row panel: stacked per-deployment horizontal violins for one metric.
#' @keywords internal
#' @noRd
.drawViolinPanel <- function(vals, ord_ids, g, fill, min.n, mar, cex, first, title) {
  ny <- length(ord_ids)
  graphics::par(mar = mar)
  # first id at the TOP: row r (from top) sits at y = ny - r + 1
  plot(NA, xlim = g$xlim, ylim = c(0.4, ny + 0.6), axes = FALSE, xaxs = "i", yaxs = "i", xlab = "", ylab = "")
  .distPanelBackground(g$xlim)                                    # subtle fill + grid, behind the violins
  fill_t <- grDevices::adjustcolor(fill, 0.7)
  for (r in seq_len(ny)) {
    y0 <- ny - r + 1
    x <- vals[[ord_ids[r]]]
    if (length(x) < min.n) next
    d <- stats::density(x, n = 512, na.rm = TRUE)                 # natural support: the violin ends at
    d <- .clampDensity(d, g$xlim)                                 # this deployment's own range (clipped to xlim)
    hw <- 0.44 * d$y / max(d$y)                                   # per-row normalised half-width
    graphics::polygon(c(d$x, rev(d$x)), c(y0 + hw, rev(y0 - hw)), col = fill_t, border = fill, lwd = 0.7)
    graphics::segments(stats::median(x), y0 - 0.34, stats::median(x), y0 + 0.34, col = "grey20", lwd = 0.9)  # median tick
  }
  .distRefLines(g$refs, cex)
  graphics::axis(1, at = pretty(g$xlim, n = 5), cex.axis = cex * 0.85, tcl = -0.3)
  if (first) {
    graphics::axis(2, at = ny:1, labels = ord_ids, las = 1, cex.axis = cex * 0.72, tick = FALSE, line = -0.4)
    graphics::mtext("Deployment", side = 2, line = mar[2] - 1.4, cex = cex * 0.95, font = 2)
  }
  if (!is.null(title)) graphics::mtext(title, side = 3, line = 0.4, font = 2, cex = cex * 0.92)
  graphics::box()                                                # full border around the panel
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
