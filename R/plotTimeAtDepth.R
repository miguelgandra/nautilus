#######################################################################################################
# Time-at-depth / time-at-temperature (profile-grid model) ############################################
#######################################################################################################

#' Plot time-at-depth and time-at-temperature
#'
#' @description
#' Knowing that an animal reached 300 m says little about how it used the water column. It may have
#' touched that depth once and spent the deployment at 20 m, or lived at 250 m throughout. The
#' distinction is ecological, and a minimum, mean and maximum cannot express it.
#'
#' This function shows where the time actually went. The depth or temperature range is binned, and each
#' bin's share of the record is drawn as a horizontal profile, so the shape of the occupancy is the
#' thing you read. It complements the scalar summaries of [summarizeTagData()] and the raw traces of
#' [plotDepthProfiles()].
#'
#' One composable geometry covers the common needs, so the API stays small:
#' \itemize{
#'   \item **variable** - `"depth"`, `"temp"`, or both (`c("depth","temp")`) side by side.
#'   \item **group.by** - split the cohort by a metadata factor (species, sex, ...); each group becomes
#'     a faceted panel of the group mean +/- SE across its individuals. `NULL` pools all individuals.
#'   \item **diel** - `TRUE` mirrors each bin into night (left) vs day (right) back-to-back bars.
#' }
#'
#' Time is weighted by each sample's TRUE duration (not row counts), with long gaps capped, so irregular
#' or gappy records are not distorted (a gap is credited only the record's median sampling interval).
#' Depth bins default to a surface-fine, deep-coarse scheme (most animals spend most time near the
#' surface); every bin is drawn as an equal-height band so all strata stay legible regardless of width.
#' The look is set by a shared [plotTheme()] object.
#'
#' @param data Processed data: `.rds` paths, a single `nautilus_tag` / data.frame, or a list of them.
#' @param variable Character. `"depth"` (default), `"temp"`, or `c("depth","temp")` for both. A depth
#'   axis is drawn downwards, shallowest at the top; every other variable keeps the conventional
#'   orientation, so a temperature panel runs coldest at the bottom.
#' @param group.by Grouping factor for faceting: a column name (in the data or the tag metadata, e.g. mapped
#'   at import via [metadataColumns()]), a named `id -> group` vector, or a two-column
#'   `data.frame(id, group)`. `NULL` (default) pools all individuals into one profile.
#' @param diel Logical. Split each bin into night vs day back-to-back bars (needs coordinates; see
#'   `coords`). Default `FALSE`. Ignored by `style = "heatmap"`.
#' @param style `"profile"` (default) or `"heatmap"` (a deployment x bin heatmap - compact for many
#'   individuals).
#' @param bin.width,breaks,n.bins Bin definition. `breaks` gives explicit (possibly non-uniform) edges;
#'   else `bin.width` (uniform) or `n.bins` pretty bins; else a smart per-variable default (surface-fine
#'   for depth, uniform for temperature).
#' @param gap.factor Numeric >= 1. A sample-to-next interval beyond `gap.factor` x the median interval
#'   is treated as a gap and credited only that median interval. Default 3.
#' @param order.by Ordering of groups / heatmap columns: `"id"`, `"input"`, or `"median"`.
#' @param same.scale Logical. Give every panel of a variable the same x range, so bar lengths are
#'   comparable across group facets (the comparison the faceting exists to support). Default `TRUE`.
#'   `FALSE` lets each panel autoscale, which resolves detail in a group whose time budget is much
#'   flatter than the others - at the cost of equal-length bars meaning different values. Depth and
#'   temperature always keep separate scales.
#' @param coords For `diel`, a fallback position: a `c(lon, lat)`, a named `id -> c(lon,lat)` list, or
#'   a `data.frame(id, lon, lat)`. It is consulted only where the data carries no usable `lon`/`lat`
#'   columns of its own, which take precedence; the tag metadata is the last resort. Supply it for
#'   deployments that never produced a position.
#' @param theme A [plotTheme()] object (or a list of overrides) controlling the visual style.
#' @param plot Logical. Draw to the active device. Default `TRUE`.
#' @param plot.file Character. Path to a PDF to draw into. Default `NULL`.
#' @param id.col,datetime.col Column names for the deployment id and datetime.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet" (silent), `TRUE`/`1`/"normal" (header + summary), or
#'   `2`/"detailed" (default): additionally reports the modal bin per variable and any skipped
#'   deployments, and shows a live progress bar while the tags are read (cli auto-hides it for fast runs).
#'
#' @return Invisibly, a tidy data frame with one row per deployment x variable x diel-phase x bin:
#'   `id`, `group`, `variable`, `phase`, `bin_min`, `bin_max`, `bin_mid`, `hours`, `pct` (percent of that
#'   deployment/phase's time; sums to 100 per `id`/`variable`/`phase`).
#'
#' @seealso [plotTheme()], [summarizeTagData()], [plotDepthProfiles()],
#'   [plotDistributions()], [plotDives()]
#' @examples
#' \dontrun{
#' files <- list.files("./processed", full.names = TRUE)
#' plotTimeAtDepth(files, variable = c("depth", "temp"), diel = TRUE)          # ref: pooled mirrored
#' plotTimeAtDepth(files, group.by = "species", plot.file = "./plots/tad.pdf")    # faceted by species
#' }
#' @export

plotTimeAtDepth <- function(data,
                            variable     = "depth",
                            group.by        = NULL,
                            diel         = FALSE,
                            style        = c("profile", "heatmap"),
                            bin.width    = NULL,
                            breaks       = NULL,
                            n.bins       = NULL,
                            gap.factor   = 3,
                            order.by     = c("id", "input", "median"),
                            same.scale   = TRUE,
                            coords  = NULL,
                            theme        = plotTheme(),
                            plot         = TRUE,
                            plot.file    = NULL,
                            id.col       = "ID",
                            datetime.col = "datetime",
                            verbose      = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)

  ## ---- validate ----
  style <- match.arg(style); order.by <- match.arg(order.by)
  variable <- unique(as.character(variable))
  if (!length(variable) || length(variable) > 2) .abort("{.arg variable} must be one or two column names.")
  theme <- .as_control(theme, plotTheme, "nautilus_theme", "theme")
  .assert_flag(diel, "diel"); .assert_flag(plot, "plot"); .assert_flag(same.scale, "same.scale")
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  .assert_number(gap.factor, "gap.factor", min = 1)
  # min = 0 was inclusive, so bin.width = 0 passed and was then silently ignored by .tadBreaks,
  # falling through to a completely different (pretty-n) bin scheme
  if (!is.null(bin.width)) .assert_number(bin.width, "bin.width", min = 0)
  if (!is.null(bin.width) && bin.width <= 0) .abort("{.arg bin.width} must be greater than zero.")
  if (!is.null(n.bins)) .assert_count(n.bins, "n.bins", min = 2)
  if (!is.null(breaks)) {
    # a degenerate `breaks` used to collapse nb to 0 and surface as "arguments imply differing
    # number of rows" from the summary data.frame, far from the cause
    if (!is.numeric(breaks)) .abort("{.arg breaks} must be numeric bin edges.")
    if (anyNA(breaks) || !all(is.finite(breaks)))
      .abort("{.arg breaks} must be finite (no {.val NA} or {.val Inf}).")
    if (length(unique(breaks)) < 2L) {
      n_edge <- length(unique(breaks))
      .abort(c("{.arg breaks} must give at least two distinct edges (it defines the bins).",
               "i" = "Received {length(breaks)} value{?s} spanning {n_edge} distinct edge{?s}."))
    }
  }
  if (!is.null(coords)) .tadCheckCoordinates(coords)
  .assert_writable_file(plot.file, "plot.file", ext = "pdf")
  if (!plot && is.null(plot.file))
    .abort(c("Nothing to plot.", "i" = "Set {.arg plot = TRUE} or provide a {.arg plot.file}."))
  if (style == "heatmap" && diel) { if (lvl >= 1L) cli::cli_alert_info("diel split is ignored for {.val heatmap} style."); diel <- FALSE }
  if (style == "heatmap" && !is.null(group.by) && lvl >= 1L)
    cli::cli_alert_info("{.arg group.by} is ignored for {.val heatmap} style (every deployment gets its own column).")

  src <- .resolveInput(data, id.col)

  # header BEFORE the loading bar, so the bar has context while it runs (matching the other plot* functions).
  # Only what is knowable pre-load is reported: the group LEVELS are not resolved until the tags are read,
  # so the bullet names the grouping column rather than counting facets.
  grp_desc <- if (is.null(group.by)) "pooled"
              else if (is.character(group.by) && length(group.by) == 1L) sprintf("grouped by %s", group.by)
              else "grouped"
  .log_header(lvl, "plotTimeAtDepth",
              sprintf("Binning %s over time", paste(vapply(variable, .tadTitle, ""), collapse = " + ")),
              bullets = sprintf("Input: %d deployment%s \u00b7 %s%s", src$n, if (src$n != 1) "s" else "",
                                grp_desc, if (diel) " \u00b7 diel split" else ""))

  ## ---- load each deployment: per-variable series, time, group, diel phase ----
  loaded <- list(); n_missing <- 0L; dup_ids <- character(0)
  pb <- .log_progress_start(lvl, src$n, "Loading")                  # live bar at detailed verbosity (lvl >= 2)
  for (i in seq_len(src$n)) {
    .log_progress_step(pb)
    x <- data.table::as.data.table(src$get(i))
    id <- as.character(.getMeta(x)$id %||% src$ids[i])
    if (!(datetime.col %in% names(x))) { n_missing <- n_missing + 1L; next }
    tnum <- .tadTimeSeconds(x[[datetime.col]], datetime.col, id)
    series <- lapply(variable, function(v) if (v %in% names(x)) .asNumericSafe(x[[v]]) else NULL)
    names(series) <- variable
    # a column that is present but yields nothing finite (all-NA, or character text) is treated as
    # ABSENT rather than carried forward: it used to reach the binner and die on `rep(0, nb)`
    series <- lapply(series, function(z) if (is.null(z) || !any(is.finite(z))) NULL else z)
    if (all(vapply(series, is.null, logical(1)))) { n_missing <- n_missing + 1L; next }
    phase <- if (diel) .tadDielPhase(x, coords, id, datetime.col, lvl) else NULL
    # `loaded` is keyed by id, so a repeated id would overwrite an earlier record and drop it from the
    # figure without a word. Ids are made unique instead, and the collision is reported.
    if (!is.null(loaded[[id]])) { dup_ids <- c(dup_ids, id); id <- .tadUniqueId(id, names(loaded)) }
    loaded[[id]] <- list(id = id, group = .deploymentGroup(x, id, group.by), t = tnum,
                         series = series, phase = phase)
  }
  .log_progress_done(pb)
  if (!length(loaded)) .abort(c("No deployment has usable {.field {variable}} + {.field {datetime.col}} data.",
                                "i" = "Check the {.arg variable} / {.arg datetime.col} column names."))
  if (length(dup_ids)) {
    dups <- unique(dup_ids)
    n_dup <- length(dups); suffixed <- paste0(dups[1], "_2")
    cli::cli_warn(c("{n_dup} deployment id{?s} appeared more than once.",
                    "i" = "Repeated: {.val {dups}}. Each extra record was kept under a suffixed id ({.val {suffixed}}), so none is lost."))
  }

  # A requested variable that NO deployment carries used to survive to the renderer and fail there with a
  # bare 'invalid times argument'. It is dropped here, by name, and only an empty set aborts.
  # `diel` needs coordinates. When NONE resolve, every phase subset is empty, so `binned` stays empty and
  # the run used to die in the SUMMARY block on `do.call(cbind, NULL)` - or, quietly, return NULL and
  # write a blank figure. Fall back to the pooled profile, which is what the data actually supports.
  if (diel && !any(vapply(loaded, function(d) !is.null(d$phase) && any(!is.na(d$phase)), logical(1)))) {
    cli::cli_warn(c("{.arg diel = TRUE} needs coordinates, and none could be resolved for any deployment.",
                    "i" = "Plotting the pooled profile instead. Supply {.arg coords}, or lon/lat columns/metadata."))
    diel <- FALSE
  }

  # (the all-absent case cannot arrive here: a deployment carrying none of the variables is skipped above,
  # so `loaded` would be empty and the guard on the previous line has already aborted.)
  has_data <- vapply(variable, function(v) any(vapply(loaded, function(d) !is.null(d$series[[v]]), logical(1))), logical(1))
  if (!all(has_data)) {
    absent <- variable[!has_data]; kept <- variable[has_data]
    cli::cli_warn(c("Absent from every deployment: {.val {absent}}.",
                    "i" = "Plotting {.val {kept}} only."))
    variable <- kept
  }

  ## ---- shared bins per variable, then bin every deployment (duration-weighted, per phase) ----
  phases <- if (diel) c("night", "day") else "all"
  breaks_by_var <- list(); binned <- list(); summ <- list(); thin_ids <- character(0); oor <- list()
  for (v in variable) {
    vals <- Filter(Negate(is.null), lapply(loaded, function(d) d$series[[v]]))
    if (!length(vals)) next
    rng <- range(unlist(lapply(vals, function(z) z[is.finite(z)])), na.rm = TRUE)
    br <- .tadResolveBreaks(v, rng, bin.width, breaks, n.bins); breaks_by_var[[v]] <- br
    nb <- length(br) - 1L
    # .timeAtDepthBins uses findInterval(all.inside = TRUE), which CLAMPS out-of-range samples into the
    # edge bins rather than dropping them. Silently, that invents a distribution: user breaks above the
    # data reported "100% of time" in a bin holding nothing. Count the clamped samples and report them.
    zz <- unlist(lapply(vals, function(z) z[is.finite(z)]))
    n_out <- sum(zz < br[1] | zz > br[length(br)])
    if (n_out > 0) oor[[v]] <- list(n = n_out, pct = 100 * n_out / length(zz), rng = rng, br = range(br))
    for (id in names(loaded)) {
      d <- loaded[[id]]; sv <- d$series[[v]]; if (is.null(sv)) next
      for (ph in phases) {
        keep <- if (ph == "all") rep(TRUE, length(sv)) else !is.na(d$phase) & d$phase == ph
        # a deployment with no usable samples for this phase (e.g. no coords -> no diel split, or a
        # record spanning only one phase) is EXCLUDED from that phase's aggregate, not counted as all-zero.
        if (ph != "all" && sum(keep) < 2L) next
        tb <- .timeAtDepthBins(sv[keep], d$t[keep], br, gap.factor)
        # a deployment with under two usable samples yields an all-zero column. Stored, it would be
        # averaged in like any other and drag the cohort mean down (and its own pct would sum to 0,
        # breaking the documented per-id contract), so it is excluded and reported instead.
        if (!sum(tb$time) > 0) { thin_ids <- c(thin_ids, id); next }
        binned[[v]][[ph]][[id]] <- tb$pct
        summ[[length(summ) + 1L]] <- data.frame(id = id, group = d$group %||% NA_character_, variable = v,
                                                phase = ph, bin_min = br[-(nb + 1L)], bin_max = br[-1],
                                                bin_mid = (br[-1] + br[-(nb + 1L)]) / 2,
                                                hours = tb$time / 3600, pct = tb$pct, stringsAsFactors = FALSE)
      }
    }
  }
  summary <- do.call(rbind, summ); rownames(summary) <- NULL
  if (length(thin_ids)) {
    thin <- unique(thin_ids); n_thin <- length(thin)
    cli::cli_warn(c("{n_thin} deployment{?s} had too few usable samples to bin and {?was/were} excluded.",
                    "i" = "Excluded: {.val {thin}}."))
  }
  for (v in names(oor)) {
    o <- oor[[v]]
    cli::cli_warn(c("{.val {v}}: {o$n} sample{?s} ({round(o$pct, 1)}%) fall outside the bin range and were counted in the edge bins.",
                    "i" = "Data span {round(o$rng[1], 1)} to {round(o$rng[2], 1)}; bins span {round(o$br[1], 1)} to {round(o$br[2], 1)}."))
  }

  ## ---- group levels + colours ----
  grp_of <- vapply(loaded, function(d) d$group %||% NA_character_, character(1))
  grp_of[!is.na(grp_of) & !nzchar(trimws(grp_of))] <- NA_character_     # "" / "   " are not group labels
  grouped <- !is.null(group.by) && any(!is.na(grp_of))
  glevels <- if (grouped) .tadGroupLevels(loaded, order.by, variable) else "All"
  if (!is.null(group.by) && !grouped)
    # the header has already announced "grouped by X"; without this the run just silently pools
    cli::cli_warn(c("No deployment has a usable {.arg group.by} value; plotting a single pooled profile.",
                    "i" = "Check that the grouping column or lookup covers the deployment ids."))
  if (grouped && any(is.na(grp_of))) {
    # such deployments appear in the returned table but on no facet - they used to vanish from the
    # figure while still being counted as "plotted"
    ungrouped <- names(grp_of)[is.na(grp_of)]; n_ung <- length(ungrouped)
    cli::cli_warn(c("{n_ung} deployment{?s} ha{?s/ve} no {.arg group.by} value and {?is/are} not shown in any panel.",
                    "i" = "Ungrouped: {.val {ungrouped}}."))
  }
  gcols <- stats::setNames(.themePalette(theme$palette, length(glevels)), glevels)

  ## ---- render ----
  # the canvas is sized from what will actually be drawn: the widest bin ladder (so bands stay legible and
  # every bin can keep its label) and the banner text (so the subtitle is not truncated mid-word).
  nb_max <- max(vapply(breaks_by_var, function(b) length(b) - 1L, integer(1)), 1L)
  banner_in <- if (style == "heatmap") 0 else
    .tadBannerWidth(.tadBannerText(variable, grouped, diel), diel, theme$cex)
  dims <- .tadFigSize(length(variable), length(glevels), style, theme$cex,
                      nb_max = nb_max, n_dep = length(loaded), banner_in = banner_in)
  lay <- .tadPanelGrid(length(variable), length(glevels))
  draw <- function(to.file = FALSE, unicode = TRUE) {
    old <- graphics::par(family = theme$font.family); on.exit(graphics::par(old), add = TRUE)
    # `plot = TRUE` draws into whatever device is already open, at ITS size - the computed page size only
    # reaches the PDF. A tall facet grid therefore fails on a small screen device with base R's bare
    # "figure margins too large", which says nothing about the cause or the remedy.
    tryCatch({
      if (style == "heatmap") .tadRenderHeatmaps(binned, breaks_by_var, variable, loaded, order.by, theme, grouped)
      else .tadRenderProfiles(binned, breaks_by_var, variable, glevels, gcols, grp_of, phases, diel, grouped, theme,
                              same.scale = same.scale)
    }, error = function(e) {
      if (!grepl("margins too large|region too large", conditionMessage(e))) stop(e)
      .abort(c("This figure needs a larger device than the one it is being drawn to.",
               "i" = "{lay$nr} x {lay$nc} panels need about {round(dims$width, 1)} x {round(dims$height, 1)} in.",
               "i" = "Pass {.arg plot.file} (the page is then sized automatically), or plot fewer groups/variables."))
    })
  }
  .renderToDevices(draw, plot = plot, plot.file = plot.file, width = dims$width, height = dims$height)

  ## ---- summary ----
  if (lvl >= 1L) {
    .log_summary(lvl)
    for (v in variable) {
      pooled <- unlist(binned[[v]], recursive = FALSE)                  # pool ALL deployments x phases
      if (!length(pooled)) next                                         # nothing binned for this variable
      cols <- do.call(cbind, pooled)
      if (is.null(cols) || !length(cols)) next
      pool <- rowMeans(cols, na.rm = TRUE)
      # report the modal BIN (both edges + unit), not the bin's left edge alone: "most time near 0" read as
      # a point estimate when what is drawn is a band. The share is of TIME - bins are duration-weighted
      # (see .timeAtDepthBins), so calling it a share of observations would misdescribe it.
      i <- which.max(pool)
      .log_detail(lvl, sprintf("%s: modal bin %s (%.0f%% of time)",
                               .tadVarName(v), .tadBinRange(breaks_by_var[[v]], i, v), max(pool, na.rm = TRUE)))
    }
    if (n_missing > 0) .log_detail(lvl, sprintf("skipped (no data): %d deployment%s", n_missing, if (n_missing != 1) "s" else ""))
    .log_done(lvl, length(loaded), " deployment", if (length(loaded) != 1) "s", " plotted (", style, ")")
    if (!is.null(plot.file)) .log_arrow(lvl, "plots: ", plot.file)
    .log_runtime(lvl, start.time)
  }
  invisible(summary)
}
