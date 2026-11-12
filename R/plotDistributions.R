#######################################################################################################
# Plot per-individual distributions of kinematic / effort metrics #####################################
#######################################################################################################

#' Plot cohort and per-deployment distributions of kinematic and effort metrics
#'
#' @description
#' A mean tail-beat frequency, or a mean swimming speed, describes a cohort badly. These quantities are
#' typically multimodal and long-tailed - an animal alternates between gaits, and rare bursts sit far
#' from the middle - so one number per deployment can hide the very structure the analysis is about.
#'
#' Each metric has its own panel. The default view pairs a cohort density strip with a stack of
#' per-deployment violins. The population view shows the same cohort density in a full panel, and
#' the deployments view shows the violins with cohort reference lines.
#'
#' A single table is split by `id.col`. For lists and files, deployment IDs come from tag metadata
#' when available, with list names, file basenames, or input positions as fallbacks. Each input
#' in a list or file vector must have a unique deployment ID.
#'
#' @param data Processed data: a character vector of `.rds` file paths, a single `nautilus_tag` /
#'   data.frame, or a list of them (see [processTagData()]).
#' @param metrics Character vector of metric columns to plot (one panel each), in order. `NULL` (default)
#'   auto-selects the kinematic / effort metrics present in the first deployment
#'   (whichever `tbf_hz_*` backends ran, `paddle_speed`, `speed`, `vedba`, `odba`, `vertical_velocity`).
#' @param labels Named character vector mapping a metric to its axis label. Any metric not named here
#'   falls back to a built-in label (e.g. `"Tail-beat frequency (Hz)"`) or the column name.
#' @param view Plot layout. `"stacked"` (default) draws cohort density strips above the deployment
#'   violins, `"population"` draws full cohort density panels, and `"deployments"` draws the
#'   deployment violins. All views use the same cohort reference values.
#' @param population.weight How to weight the observed rows for each cohort metric. `"deployment"`
#'   (default) gives equal total weight to each deployment with finite values for that metric.
#'   `"row"` gives equal weight to each finite row across deployments.
#' @param order.by How to order deployments down the y-axis (one order, shared by every panel):
#'   `"id"` (default, alphabetical), `"input"` (as supplied), or `"median"` (by the median of
#'   `order.metric`). Used in views with deployment rows.
#' @param order.metric Metric whose per-deployment median drives `order.by = "median"`. `NULL` (default)
#'   uses the first metric. Applies when sorting deployment rows by their medians.
#' @param reference Population reference line(s) drawn per panel: any of `"median"` (default) and
#'   `"mean"`, or `NULL` for none.
#' @param trim Numeric in (0, 1]. Weighted upper cohort quantile used for the x-axis limit.
#'   Default `0.995`. Use `1` for the full range. If trimming would leave a single x value, the
#'   full range is shown. Summaries and density estimates use all finite rows.
#' @param min.n Integer. Minimum finite rows for a deployment violin. Default `30`. A deployment
#'   below this threshold or with a constant metric is shown with a median tick and count when it
#'   has data. The threshold only controls how deployment rows are drawn.
#' @param theme A [plotTheme()] object, or a list of overrides, controlling the visual style.
#' @param plot Logical. If `TRUE` (default), draw to the active graphics device. Set `FALSE` with
#'   `plot.file = NULL` to compute the summaries alone.
#' @param plot.file Character. Path to a PDF to draw into (independent of `plot`; set either or both).
#'   The function manages the device itself. Default `NULL`.
#' @param id.col Character. Name of the deployment id column. Default `"ID"`.
#' @param verbose Verbosity. Use `FALSE`/`0`/"quiet" for silence or `TRUE`/`1`/"normal" for a
#'   header and summary. The default `2`/"detailed" also reports the deployments and rows
#'   contributing to each metric and shows a loading progress bar for longer runs.
#'
#' @details A deployment's share is divided evenly among its finite rows. The cohort density uses
#'   a Gaussian kernel and a bandwidth set to one quarter of the smaller positive value of the
#'   weighted standard deviation and the weighted IQR divided by 1.349. A constant cohort metric
#'   is shown as a point. Cohort quantiles use the weighted empirical CDF, with the midpoint
#'   between adjacent values when the probability falls exactly on a boundary.
#'
#' @return Invisibly, a list with two data frames, independent of `view`.
#'   - `deployments` has one row per deployment and metric, with columns `id`, `metric`, `n`,
#'     `mean`, `median`, `sd`, `q05`, `q25`, `q75`, `q95`, `min`, `max`. Deployments lacking a
#'     metric appear with `n = 0` and `NA` statistics.
#'   - `population` has one row per metric, with columns `metric`, `weighting`, `n_deployments`,
#'     `n`, `mean`, `median`, `sd`, `q05`, `q25`, `q75`, `q95`, `min`, `max`. `n_deployments`
#'     counts contributing deployments, `n` counts their finite rows, and `sd` is the weighted
#'     population standard deviation.
#'
#' @seealso [plotDives()], [plotTimeAtDepth()], [summarizeTagData()],
#'   [calculateTailBeats()], [processTagData()]
#' @examples
#' \dontrun{
#' # Compare tail-beat frequency and swimming speed across the cohort, into a PDF
#' tags <- list.files("./processed", pattern = "\\.rds$", full.names = TRUE)
#' plotDistributions(tags,
#'                   metrics = c("tbf_hz_peaks", "paddle_speed"),
#'                   plot = FALSE, plot.file = "./plots/distributions.pdf")
#'
#' # Draw just the cohort distribution with equal deployment weights
#' plotDistributions(tags, metrics = "paddle_speed", view = "population")
#' }
#' @export

plotDistributions <- function(data,
                              metrics           = NULL,
                              labels            = NULL,
                              view              = c("stacked", "population", "deployments"),
                              population.weight = c("deployment", "row"),
                              order.by          = c("id", "input", "median"),
                              order.metric      = NULL,
                              reference         = "median",
                              trim              = 0.995,
                              min.n             = 30,

                              plot              = TRUE,
                              plot.file         = NULL,
                              theme             = plotTheme(),
                              id.col            = "ID",
                              verbose           = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)

  ##############################################################################
  # Validate arguments #########################################################
  ##############################################################################

  view <- match.arg(view)
  population.weight <- match.arg(population.weight)
  order.by <- match.arg(order.by)
  reference <- if (is.null(reference)) character(0) else match.arg(reference, c("median", "mean"), several.ok = TRUE)
  .assert_flag(plot, "plot")
  .assert_number(trim, "trim", min = 0, max = 1)
  if (trim == 0) .abort("{.arg trim} must be in (0, 1].")
  .assert_count(min.n, "min.n", min = 1)
  theme <- .as_control(theme, plotTheme, "nautilus_theme", "theme")
  .assert_string(id.col, "id.col"); .assert_string(order.metric, "order.metric", null_ok = TRUE)
  .assert_writable_file(plot.file, "plot.file", ext = "pdf")     # fail-fast: parent dir must exist
  if (!is.null(metrics) && (!is.character(metrics) || !length(metrics) || anyNA(metrics) ||
                           any(!nzchar(metrics)) || anyDuplicated(metrics)))
    .abort("{.arg metrics} must be a non-empty character vector of unique column names, or {.code NULL}.")
  if (!is.null(labels) && (!is.character(labels) || is.null(names(labels))))
    .abort("{.arg labels} must be a NAMED character vector (metric -> label).")
  render <- plot || !is.null(plot.file)
  population.only <- view == "population"
  stacked <- view == "stacked"

  # This figure was tuned at cex = 1.15 before the theme existed. Folding that base in here means
  # theme$cex = 1 reproduces the tuned layout exactly and scales from there, rather than silently
  # shrinking every label by 13% the moment the function joined the family.
  cex <- theme$cex * 1.15

  src <- .resolveInput(data, id.col)

  ##############################################################################
  # Resolve which metrics to plot (auto-detect from the first-seen columns) ####
  ##############################################################################

  if (is.null(metrics)) {
    first_cols <- names(src$get(1))
    metrics <- intersect(.distAutoMetrics(first_cols), first_cols)
    if (!length(metrics))
      .abort(c("No kinematic / effort metrics found to plot.",
               "i" = "Provide {.arg metrics} explicitly (available numeric columns include {.val {utils::head(setdiff(first_cols, c(id.col, 'datetime')), 8)}})."))
  }
  if (!population.only && order.by == "median" && !is.null(order.metric) && !order.metric %in% metrics)
    .abort("{.arg order.metric} ({.val {order.metric}}) must be one of {.arg metrics}: {.val {metrics}}.")

  metric_labels <- vapply(metrics, function(m) .distLabel(m, labels), character(1))
  metric_cols   <- .themePalette(theme$palette, length(metrics))

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
  ids <- character(src$n)
  pb <- .log_progress_start(lvl, src$n, "Loading")                       # live bar at detailed verbosity (lvl >= 2)
  for (i in seq_len(src$n)) {
    .log_progress_step(pb)
    dt <- data.table::as.data.table(src$get(i))
    id <- if (is.data.frame(data)) src$ids[i] else as.character(.getMeta(dt)$id)
    if (length(id) != 1L || is.na(id) || !nzchar(id)) id <- src$ids[i]
    if (is.na(id) || !nzchar(id)) id <- as.character(i)
    if (id %in% ids)
      .abort("Duplicate deployment ID {.val {id}}. Supply one input per deployment.")
    ids[i] <- id
    for (m in metrics) {
      # shared coercion contract: a FACTOR metric used to become its level codes, which silently
      # replaced the measurements (and inverted the cohort order under order.by = "median")
      x <- if (m %in% names(dt)) .asNumericSafe(dt[[m]]) else numeric(0)
      x <- x[is.finite(x)]
      values[[m]][[id]] <- x
      summary_rows[[length(summary_rows) + 1L]] <- .distSummaryRow(id, m, x)
    }
  }
  .log_progress_done(pb)
  summary <- do.call(rbind, summary_rows); rownames(summary) <- NULL

  # A metric no deployment carries - a typo, most often - used to be drawn as a fully-formed empty panel
  # (title, axes, grid, both deployment labels) over the 0-1 fallback x range, and reported as plotted.
  # Nothing distinguished that from a metric that is genuinely flat, so drop it by name.
  has_any <- vapply(metrics, function(m) any(vapply(values[[m]], length, integer(1)) > 0L), logical(1))
  if (!all(has_any)) {
    empty_metrics <- metrics[!has_any]
    if (!any(has_any))
      .abort(c("No requested metric is present in any deployment: {.val {empty_metrics}}.",
               "i" = "Check {.arg metrics} against the column names in the data."))
    cli::cli_warn(c("No data for {.val {empty_metrics}} in any deployment; {?it was/they were} dropped.",
                    "i" = "Plotting {.val {metrics[has_any]}}."))
    if (!is.null(order.metric) && !order.metric %in% metrics[has_any]) order.metric <- NULL
    metric_labels <- metric_labels[has_any]; metric_cols <- metric_cols[has_any]
    metrics <- metrics[has_any]
    summary <- summary[summary$metric %in% metrics, , drop = FALSE]
  }

  ##############################################################################
  # Cohort summaries and geometry ##############################################
  ##############################################################################

  geom <- lapply(metrics, function(m) .distPopulation(values[[m]], m, population.weight, trim,
                                                     reference, render && view != "deployments"))
  population <- do.call(rbind, lapply(geom, "[[", "summary"))

  ##############################################################################
  # Draw #######################################################################
  ##############################################################################

  M <- length(metrics)

  draw <- function(to.file = FALSE, unicode = TRUE) {
    lay <- matrix(seq_len(if (stacked) 2 * M else M), ncol = M, byrow = TRUE)
    # fixed-height marginal strip (absolute cm), so it never collapses under its title margin when there
    # are few deployments; the violin row takes the remaining height.
    left1 <- if (population.only) 4.5 else max(4.5, 0.55 * max(nchar(ord_ids)) * cex)

    # Every panel must end up the SAME size, whatever metrics are drawn. The first column carries the
    # deployment labels in its own left margin, so with equal cells its plot region is narrower than
    # its neighbours' by exactly that margin - which is what made a two-metric figure look lopsided.
    # Widening the first CELL by the same amount cancels it: region = cell - margins is then constant.
    # The layout is set TWICE, deliberately. layout() shrinks par("cex") according to how many figures
    # it holds (0.83 for a 2x2), and margins are measured in lines of that scaled text - so the
    # inches-per-line needed to size the gutter is only knowable once the layout exists. Reading
    # par("csi") beforehand returns the unscaled 0.2 and over-widens the first column by a fifth.
    heights <- if (stacked) c(graphics::lcm(2.4), 1) else 1
    graphics::par(oma = c(0, 0, 0, 0), mgp = c(3, 0.7, 0))       # before layout, so the region is known
    graphics::layout(lay, heights = heights)                     # establishes the per-figure cex
    lh <- graphics::par("csi")                                   # true inches per margin line
    # Measure the DEVICE, not the size this function would have asked for: with plot = TRUE the figure
    # goes to whatever device is already open, which is rarely dims$width. Sizing the correction from
    # the intended width instead scales it by the ratio of the two and under-corrects.
    avail <- graphics::par("din")[1] - sum(graphics::par("omi")[c(2, 4)])
    gut   <- if (population.only) 0 else (left1 - 0.8) * lh       # first column's extra margin, in inches
    panel <- (avail - gut) / M                                   # the width every panel cell then shares
    # layout() allocates `widths` proportionally, so passing inches that sum to the figure width makes
    # the allocation literal.
    graphics::layout(lay, widths = c(panel + gut, rep(panel, M - 1L)), heights = heights)
    if (stacked || population.only) for (j in seq_len(M)) {
      left <- if (population.only || j == 1L) left1 else 0.8
      mar <- if (population.only) c(3, left, 3.2, 1.2) else c(0.4, left, 2.7, 1.2)
      .drawPopulationPanel(geom[[j]], metric_labels[j], metric_cols[j], mar, cex, j == 1L,
                           theme, population.only, length(ids))
    }
    if (!population.only) for (j in seq_len(M)) {
      top <- if (stacked) 0.4 else 2.3
      .drawViolinPanel(values[[metrics[j]]], ord_ids, geom[[j]], metric_cols[j], min.n,
                       mar = c(4, if (j == 1) left1 else 0.8, top, 1.2), cex = cex,
                       first = (j == 1), title = if (stacked) NULL else metric_labels[j],
                       theme = theme)
    }
  }

  if (render) {
    ord_ids <- if (population.only) character(0) else .distOrder(ids, order.by, values[[order.metric %||% metrics[1]]])
    dims <- .distDeviceSize(M, length(ord_ids), view, cex)
    .renderToDevices(draw, plot = plot, plot.file = plot.file, width = dims$width, height = dims$height)
  }

  ##############################################################################
  # Summary ####################################################################
  ##############################################################################

  if (lvl >= 1L) {
    .log_summary(lvl)
    for (j in seq_len(M))
      .log_detail(lvl, sprintf("%s: %d/%d deployments contributing (%s rows)", metrics[j],
                               population$n_deployments[j], length(ids), format(population$n[j], big.mark = ",")))
    .log_done(lvl, M, " metric", if (M != 1) "s", " summarised with equal ", population.weight, " weights")
    if (!is.null(plot.file)) .log_arrow(lvl, "plots: ", plot.file)
    .log_runtime(lvl, start.time)
  }

  invisible(list(deployments = summary, population = population))
}
