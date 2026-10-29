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
