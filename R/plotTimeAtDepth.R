#######################################################################################################
# Time-at-depth / time-at-temperature (profile-grid model) ############################################
#######################################################################################################

#' Plot time-at-depth and time-at-temperature
#'
#' @description
#' Shows HOW a cohort allocates time across the depth (or temperature) envelope. The variable is binned
#' and each bin's share of time is drawn as a horizontal **profile** - the ecological complement to the
#' scalar min / mean / max of \link{summarizeTagData} and the raw traces of \link{plotDepthProfiles}.
#'
#' One composable geometry covers the common needs, so the API stays small:
#' \itemize{
#'   \item \strong{variable} - `"depth"`, `"temp"`, or both (`c("depth","temp")`) side by side.
#'   \item \strong{group} - split the cohort by a metadata factor (species, sex, ...); each group becomes
#'     a faceted panel of the group mean +/- SE across its individuals. `NULL` pools all individuals.
#'   \item \strong{diel} - `TRUE` mirrors each bin into night (left) vs day (right) back-to-back bars.
#' }
#'
#' Time is weighted by each sample's TRUE duration (not row counts), with long gaps capped, so irregular
#' or gappy records are not distorted (a gap is credited only the record's median sampling interval).
#' Depth bins default to a surface-fine, deep-coarse scheme (most animals spend most time near the
#' surface); every bin is drawn as an equal-height band so all strata stay legible regardless of width.
#' The look is set by a shared \link{plotTheme} object.
#'
#' @param data Processed data: `.rds` paths, a single `nautilus_tag` / data.frame, or a list of them.
#' @param variable Character. `"depth"` (default), `"temp"`, or `c("depth","temp")` for both.
#' @param group Grouping factor for faceting: a column name (in the data or the tag metadata, e.g. mapped
#'   at import via \link{metadataColumns}), a named `id -> group` vector, or a two-column
#'   `data.frame(id, group)`. `NULL` (default) pools all individuals into one profile.
#' @param diel Logical. Split each bin into night vs day back-to-back bars (needs coordinates; see
#'   `coordinates`). Default `FALSE`. Ignored by `style = "heatmap"`.
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
#' @param coordinates For `diel`: a `c(lon, lat)`, a named `id -> c(lon,lat)` list, or a
#'   `data.frame(id, lon, lat)`. If `NULL`, longitude/latitude are read from the data or tag metadata.
#' @param theme A \link{plotTheme} object (or a list of overrides) controlling the visual style.
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
#' @seealso \link{plotTheme}, \link{summarizeTagData}, \link{plotDepthProfiles}, \link{plotDistributions}
#' @examples
#' \dontrun{
#' files <- list.files("./processed", full.names = TRUE)
#' plotTimeAtDepth(files, variable = c("depth", "temp"), diel = TRUE)          # ref: pooled mirrored
#' plotTimeAtDepth(files, group = "species", plot.file = "./plots/tad.pdf")    # faceted by species
#' }
#' @export

plotTimeAtDepth <- function(data,
                            variable     = "depth",
                            group        = NULL,
                            diel         = FALSE,
                            style        = c("profile", "heatmap"),
                            bin.width    = NULL,
                            breaks       = NULL,
                            n.bins       = NULL,
                            gap.factor   = 3,
                            order.by     = c("id", "input", "median"),
                            same.scale   = TRUE,
                            coordinates  = NULL,
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
  if (!is.null(bin.width)) .assert_number(bin.width, "bin.width", min = 0)
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
  .assert_writable_file(plot.file, "plot.file", ext = "pdf")
  if (!plot && is.null(plot.file))
    .abort(c("Nothing to plot.", "i" = "Set {.arg plot = TRUE} or provide a {.arg plot.file}."))
  if (style == "heatmap" && diel) { if (lvl >= 1L) cli::cli_alert_info("diel split is ignored for {.val heatmap} style."); diel <- FALSE }

  src <- .resolveInput(data, id.col)

  # header BEFORE the loading bar, so the bar has context while it runs (matching the other plot* functions).
  # Only what is knowable pre-load is reported: the group LEVELS are not resolved until the tags are read,
  # so the bullet names the grouping column rather than counting facets.
  grp_desc <- if (is.null(group)) "pooled"
              else if (is.character(group) && length(group) == 1L) sprintf("grouped by %s", group)
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
    series <- lapply(variable, function(v) if (v %in% names(x)) suppressWarnings(as.numeric(x[[v]])) else NULL)
    names(series) <- variable
    if (all(vapply(series, is.null, logical(1)))) { n_missing <- n_missing + 1L; next }
    phase <- if (diel) .tadDielPhase(x, coordinates, id, datetime.col, lvl) else NULL
    # `loaded` is keyed by id, so a repeated id would overwrite an earlier record and drop it from the
    # figure without a word. Ids are made unique instead, and the collision is reported.
    if (!is.null(loaded[[id]])) { dup_ids <- c(dup_ids, id); id <- .tadUniqueId(id, names(loaded)) }
    loaded[[id]] <- list(id = id, group = .deploymentGroup(x, id, group), t = as.numeric(x[[datetime.col]]),
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
  breaks_by_var <- list(); binned <- list(); summ <- list()
  for (v in variable) {
    vals <- Filter(Negate(is.null), lapply(loaded, function(d) d$series[[v]]))
    if (!length(vals)) next
    rng <- range(unlist(lapply(vals, function(z) z[is.finite(z)])), na.rm = TRUE)
    br <- .tadResolveBreaks(v, rng, bin.width, breaks, n.bins); breaks_by_var[[v]] <- br
    nb <- length(br) - 1L
    for (id in names(loaded)) {
      d <- loaded[[id]]; sv <- d$series[[v]]; if (is.null(sv)) next
      for (ph in phases) {
        keep <- if (ph == "all") rep(TRUE, length(sv)) else !is.na(d$phase) & d$phase == ph
        # a deployment with no usable samples for this phase (e.g. no coordinates -> no diel split, or a
        # record spanning only one phase) is EXCLUDED from that phase's aggregate, not counted as all-zero.
        if (ph != "all" && sum(keep) < 2L) next
        tb <- .timeAtDepthBins(sv[keep], d$t[keep], br, gap.factor)
        binned[[v]][[ph]][[id]] <- tb$pct
        summ[[length(summ) + 1L]] <- data.frame(id = id, group = d$group %||% NA_character_, variable = v,
                                                phase = ph, bin_min = br[-(nb + 1L)], bin_max = br[-1],
                                                bin_mid = (br[-1] + br[-(nb + 1L)]) / 2,
                                                hours = tb$time / 3600, pct = tb$pct, stringsAsFactors = FALSE)
      }
    }
  }
  summary <- do.call(rbind, summ); rownames(summary) <- NULL

  ## ---- group levels + colours ----
  grp_of <- vapply(loaded, function(d) d$group %||% NA_character_, character(1))
  grouped <- !is.null(group) && any(!is.na(grp_of))
  glevels <- if (grouped) .tadGroupLevels(loaded, order.by, variable) else "All"
  gcols <- stats::setNames(.themePalette(theme$palette, length(glevels)), glevels)

  ## ---- render ----
  # the canvas is sized from what will actually be drawn: the widest bin ladder (so bands stay legible and
  # every bin can keep its label) and the banner text (so the subtitle is not truncated mid-word).
  nb_max <- max(vapply(breaks_by_var, function(b) length(b) - 1L, integer(1)), 1L)
  banner_in <- if (style == "heatmap") 0 else
    .tadBannerWidth(.tadBannerText(variable, grouped, diel), diel, theme$cex)
  dims <- .tadFigSize(length(variable), length(glevels), style, theme$cex,
                      nb_max = nb_max, n_dep = length(loaded), banner_in = banner_in)
  draw <- function(to.file = FALSE, unicode = TRUE) {
    old <- graphics::par(family = theme$font.family); on.exit(graphics::par(old), add = TRUE)
    if (style == "heatmap") .tadRenderHeatmaps(binned, breaks_by_var, variable, loaded, order.by, theme, grouped)
    else .tadRenderProfiles(binned, breaks_by_var, variable, glevels, gcols, grp_of, phases, diel, grouped, theme,
                            same.scale = same.scale)
  }
  .renderToDevices(draw, plot = plot, plot.file = plot.file, width = dims$width, height = dims$height)

  ## ---- summary ----
  if (lvl >= 1L) {
    .log_summary(lvl)
    for (v in variable) {
      cols <- do.call(cbind, unlist(binned[[v]], recursive = FALSE))    # pool ALL deployments x phases
      if (is.null(cols)) next
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


################################################################################
# Binning (duration-weighted) #################################################
################################################################################

#' Duration-weighted time and percent per bin for one deployment (gaps capped to the median interval).
#' @keywords internal
#' @noRd
.timeAtDepthBins <- function(v, tnum, breaks, gap.factor) {
  ok <- is.finite(v) & is.finite(tnum); v <- v[ok]; tnum <- tnum[ok]
  nb <- length(breaks) - 1L
  if (length(v) < 2L) return(list(time = rep(0, nb), pct = rep(0, nb)))
  o <- order(tnum); v <- v[o]; tnum <- tnum[o]
  gaps <- diff(tnum); modal <- stats::median(gaps[gaps > 0]); if (!is.finite(modal) || modal <= 0) modal <- 1
  w <- c(gaps, modal); w[!is.finite(w) | w <= 0 | w > gap.factor * modal] <- modal
  idx <- findInterval(v, breaks, rightmost.closed = TRUE, all.inside = TRUE)
  tsum <- as.numeric(tapply(w, factor(idx, levels = seq_len(nb)), sum)); tsum[is.na(tsum)] <- 0
  list(time = tsum, pct = if (sum(w) > 0) 100 * tsum / sum(w) else rep(0, nb))
}

#' Bin breaks: explicit `breaks`, else `bin.width`/`n.bins`, else a smart per-variable default.
#' @keywords internal
#' @noRd
.tadResolveBreaks <- function(variable, rng, bin.width, breaks, n.bins) {
  if (!is.null(breaks)) return(sort(unique(as.numeric(breaks))))
  if (!is.null(bin.width) || !is.null(n.bins)) return(.tadBreaks(rng, bin.width, n.bins %||% 20L))
  .tadDefaultBreaks(variable, rng)
}

#' ~`n.bins` pretty bins, or an exact `bin.width`.
#' @keywords internal
#' @noRd
.tadBreaks <- function(rng, bin.width, n.bins) {
  if (!is.null(bin.width) && bin.width > 0) {
    lo <- floor(rng[1] / bin.width) * bin.width; hi <- ceiling(rng[2] / bin.width) * bin.width
    return(seq(lo, if (hi <= lo) lo + bin.width else hi, by = bin.width))
  }
  b <- pretty(rng, n = n.bins); if (length(b) < 2) b <- range(rng) + c(0, if (diff(rng) == 0) 1 else 0); b
}

#' Smart default bins: surface-fine / deep-coarse for depth; uniform pretty bins otherwise.
#' @keywords internal
#' @noRd
.tadDefaultBreaks <- function(variable, rng) {
  if (!identical(variable, "depth")) return(pretty(rng, 15))
  tmpl <- c(0, 2, 4, 6, 8, 10, 15, 20, 30, 50, 75, 100, 150, 200, 300, 400, 600, 800, 1000, 1500, 2000, 3000, 5000)
  below <- tmpl[tmpl < rng[2]]; cover <- tmpl[tmpl >= rng[2]][1]
  b <- c(below, if (!is.na(cover)) cover else max(tmpl))
  b <- unique(b[b >= floor(min(rng[1], 0))])
  if (length(b) < 2) pretty(rng, 12) else b
}


################################################################################
# Grouping + diel helpers ######################################################
################################################################################

#' Ordered group levels for faceting.
#' @keywords internal
#' @noRd
.tadGroupLevels <- function(loaded, order.by, variable) {
  g <- vapply(loaded, function(d) d$group %||% NA_character_, character(1)); g <- g[!is.na(g)]
  lv <- unique(g)
  if (order.by == "median") {
    # a deployment that lacks the ordering variable contributes nothing here; unlist() on all-NULL used
    # to yield numeric(0), which vapply rejects with an opaque length error. Such a group sorts last.
    med <- vapply(lv, function(gg) {
      ids <- names(g)[g == gg]
      z <- unlist(lapply(loaded[ids], function(d) d$series[[variable[1]]]))
      z <- z[is.finite(z)]
      if (!length(z)) NA_real_ else stats::median(z)
    }, numeric(1))
    lv <- lv[order(med, na.last = TRUE)]
  } else if (order.by == "id") lv <- sort(lv)
  lv
}

#' A single representative day/night phase vector for one deployment (or NULL if no coordinates).
#' @keywords internal
#' @noRd
.tadDielPhase <- function(x, coordinates, id, datetime.col, lvl) {
  co <- .tadCoords(x, coordinates, id)
  if (is.null(co)) { if (lvl >= 1L) cli::cli_alert_warning("{.val {id}}: no coordinates for diel split; skipped."); return(NULL) }
  as.character(getDielPhase(x[[datetime.col]], matrix(co, ncol = 2), phases = 2))
}

#' Resolve a single representative c(lon, lat) for a deployment (data cols -> arg -> metadata).
#' @keywords internal
#' @noRd
.tadCoords <- function(x, coordinates, id) {
  ln <- intersect(c("lon", "longitude"), names(x)); la <- intersect(c("lat", "latitude"), names(x))
  if (length(ln) && length(la)) {
    lo <- stats::median(x[[ln[1]]], na.rm = TRUE); aa <- stats::median(x[[la[1]]], na.rm = TRUE)
    if (is.finite(lo) && is.finite(aa)) return(c(lo, aa))
  }
  if (!is.null(coordinates)) {
    if (is.numeric(coordinates) && length(coordinates) == 2) return(coordinates)
    if (is.list(coordinates) && !is.data.frame(coordinates) && !is.null(coordinates[[id]])) return(as.numeric(coordinates[[id]]))
    if (is.data.frame(coordinates)) { hit <- coordinates[as.character(coordinates[[1]]) == id, , drop = FALSE]
      if (nrow(hit)) return(as.numeric(hit[1, 2:3])) }
  }
  m <- tryCatch(.getMeta(x)$deployment, error = function(e) NULL)
  lo <- m$lon %||% m$longitude; aa <- m$lat %||% m$latitude          # canonical fields are lon/lat
  if (!is.null(lo) && !is.null(aa) && is.finite(lo[1]) && is.finite(aa[1])) return(c(lo[1], aa[1]))
  NULL
}


################################################################################
# Labels ######################################################################
################################################################################

#' @keywords internal
#' @noRd
.tadLabel <- function(v) switch(v, depth = "Depth bin (m)", temp = "Temperature bin (\u00b0C)", sprintf("%s bin", v))
#' @keywords internal
#' @noRd
.tadTitle <- function(v) switch(v, depth = "time-at-depth", temp = "time-at-temperature", sprintf("time-at-%s", v))

#' Spelled-out variable name for prose (console summary): "temp" is a column name, not a word.
#' First free `id_2`, `id_3`, ... for a deployment id that collides with one already loaded.
#' @keywords internal
#' @noRd
.tadUniqueId <- function(id, taken) {
  k <- 2L
  while (paste0(id, "_", k) %in% taken) k <- k + 1L
  paste0(id, "_", k)
}

#' Capitalise the first character (panel/banner titles are sentence case package-wide).
#' @keywords internal
#' @noRd
.tadCap1 <- function(s) { substr(s, 1, 1) <- toupper(substr(s, 1, 1)); s }

#' @keywords internal
#' @noRd
.tadVarName <- function(v) switch(v, depth = "depth", temp = "temperature", v)

#' Measurement unit suffix for a variable, "" when unknown.
#' @keywords internal
#' @noRd
.tadUnit <- function(v) switch(v, depth = " m", temp = " \u00b0C", "")

#' One bin's range as prose, e.g. "0-2 m" / "22-24 degrees C" (en dash), for the console summary.
#' The top bin is open-ended in the plot's own labels, so match that here rather than implying a
#' ceiling the data may exceed.
#' @keywords internal
#' @noRd
.tadBinRange <- function(breaks, i, v) {
  u <- .tadUnit(v)
  if (i >= length(breaks) - 1L) return(sprintf("> %g%s", breaks[i], u))
  sprintf("%g\u2013%g%s", breaks[i], breaks[i + 1L], u)
}
#' @keywords internal
#' @noRd
.tadXlab <- function(v) sprintf("Time at %s (%% per deployment)", .tadVarName(v))

#' Bin-range labels ("0-10"); the last bin becomes ">X" (open-ended top).
#' @keywords internal
#' @noRd
.binLabels <- function(breaks) {
  lo <- breaks[-length(breaks)]; hi <- breaks[-1]
  lab <- sprintf("%g-%g", lo, hi); lab[length(lab)] <- sprintf(">%g", lo[length(lo)]); lab
}


################################################################################
# Rendering: profiles ##########################################################
################################################################################

#' Figure size (inches).
#'
#' Height follows the BIN COUNT rather than a flat per-row constant: a panel showing 20 depth strata needs
#' more height than one showing 6 temperature strata, and the old fixed 2.7 in/row squeezed the default
#' depth ladder into ~7 pt bands - which is why the bin labels used to be thinned to every second one.
#' Growth is capped so a grouped x 2-variable figure cannot run to a page metres long.
#'
#' Width is floored by the BANNER, not just the panel grid: the title strip spans the whole figure, so a
#' narrow single-variable canvas used to truncate its own subtitle mid-word.
#' @keywords internal
#' @noRd
.tadFigSize <- function(n_var, n_grp, style, cex, nb_max = 12L, n_dep = 1L, banner_in = 0) {
  s <- max(1, cex * 0.92)
  if (style == "heatmap") {
    return(list(width  = max(1.5 + 3.6 * n_var, 1.5 + 0.30 * n_dep) * s,
                height = max(5.4, 1.5 + 0.20 * nb_max) * s))
  }
  lay <- .tadPanelGrid(n_var, n_grp)
  # data region grows with bins; chrome (axes + panel titles) is roughly constant per row
  data_in <- min(max(2.30, 0.135 * nb_max), 4.5)
  row_in  <- data_in + 1.30
  list(width  = max(1.1 + 3.4 * lay$nc, banner_in) * s,
       height = (0.9 + row_in * lay$nr) * s)
}

#' Banner strings, needed both to draw the strip and to size the canvas that must hold it.
#' @keywords internal
#' @noRd
.tadBannerText <- function(variable, grouped, diel) {
  cap1 <- function(s) { substr(s, 1, 1) <- toupper(substr(s, 1, 1)); s }
  ttl <- paste0(cap1(paste(vapply(variable, .tadTitle, ""), collapse = " and ")), if (grouped) " by group" else "")
  sub <- if (grouped) "Group means \u00b1 SE across individuals" else "Pooled across individuals (mean per deployment \u00b1 SE)"
  if (diel) sub <- paste0(sub, " \u00b7 night vs day")
  list(title = ttl, subtitle = sub)
}

#' Inches of canvas the banner needs (title at 1.5 cex, subtitle at 1.0, plus the diel key).
#' strwidth needs a device, so measure on a throwaway one rather than guessing character widths.
#' @keywords internal
#' @noRd
.tadBannerWidth <- function(banner, diel, cex) {
  w <- tryCatch({
    grDevices::pdf(NULL, width = 7, height = 7); on.exit(grDevices::dev.off(), add = TRUE)
    graphics::par(mar = c(0, 0, 0, 0)); graphics::plot.new()
    max(graphics::strwidth(banner$title, units = "inches", cex = 1.5 * cex),
        graphics::strwidth(banner$subtitle, units = "inches", cex = 1.0 * cex))
  }, error = function(e) NA_real_)
  if (!is.finite(w)) return(0)
  w + 0.30 + if (diel) 1.45 else 0                     # left inset + room for the night/day key
}

#' Panel grid dimensions (nr x nc) for the profile layout.
#' @keywords internal
#' @noRd
.tadPanelGrid <- function(n_var, n_grp) {
  if (n_grp == 1) return(list(nr = 1L, nc = n_var))            # pooled: variables in a row
  if (n_var == 1) { nc <- min(n_grp, 4L); return(list(nr = ceiling(n_grp / nc), nc = nc)) }
  list(nr = n_grp, nc = n_var)                                 # grouped x 2 vars: group rows, var cols
}

#' Render the profile grid: one panel per (group x variable), title strip + optional diel legend.
#' @keywords internal
#' @noRd
.tadRenderProfiles <- function(binned, breaks_by_var, variable, glevels, gcols, grp_of, phases, diel, grouped, theme,
                               same.scale = TRUE) {
  n_var <- length(variable); n_grp <- length(glevels); lay <- .tadPanelGrid(n_var, n_grp)
  panels <- if (n_grp == 1) lapply(variable, function(v) list(g = glevels[1], v = v))
            else if (n_var == 1) lapply(glevels, function(g) list(g = g, v = variable[1]))
            else unlist(lapply(glevels, function(g) lapply(variable, function(v) list(g = g, v = v))), recursive = FALSE)

  # Aggregate EVERY panel up front: the group facets only support comparison if they share an x scale, and
  # that maximum is not knowable until all panels are aggregated. Per variable, not globally, so depth and
  # temperature keep their own (unrelated) ranges.
  aggs <- lapply(panels, function(p) {
    ids <- names(grp_of)[if (grouped) grp_of == p$g else TRUE]; ids <- ids[!is.na(ids)]
    list(ids = ids, agg = .tadAggregate(binned[[p$v]], ids, phases, length(breaks_by_var[[p$v]]) - 1L))
  })
  xmax_by_var <- NULL
  if (isTRUE(same.scale)) {
    xmax_by_var <- stats::setNames(vapply(variable, function(v) {
      w <- which(vapply(panels, function(p) identical(p$v, v), logical(1)))
      max(vapply(w, function(i) {
        a <- aggs[[i]]$agg
        max(unlist(lapply(a, function(z) z$mean + z$se)), 0, na.rm = TRUE)
      }, numeric(1)), 0.001, na.rm = TRUE) * 1.04
    }, numeric(1)), variable)
  }

  # region 1 = a full-width title strip; regions 2.. = the panel grid (row-major, matching the loop)
  m <- matrix(1L + seq_len(lay$nr * lay$nc), lay$nr, lay$nc, byrow = TRUE)
  graphics::layout(rbind(matrix(1L, 1, lay$nc), m),
                   heights = c(graphics::lcm(1.9 * max(1, theme$cex)), rep(1, lay$nr)))
  graphics::par(mar = c(0, 0, 0, 0)); graphics::plot.new()
  banner <- .tadBannerText(variable, grouped, diel)
  graphics::text(0.012, 0.66, banner$title, adj = 0, font = 2, cex = 1.5 * theme$cex, col = theme$ink)
  graphics::text(0.012, 0.24, banner$subtitle, adj = 0, cex = 1.0 * theme$cex, col = theme$subtitle)
  if (diel) {
    # right-anchored off its own measured width: a hardcoded x clipped the key on narrow canvases. Each
    # swatch carries the border the PANEL draws it with, so the key matches the encoding it explains.
    key <- graphics::legend(0, 0, c("Night", "Day"), fill = c(theme$night, theme$day), horiz = TRUE,
                            bty = "n", cex = 1.02 * theme$cex, plot = FALSE)
    graphics::legend(max(0.5, 0.985 - key$rect$w), 0.94, c("Night", "Day"), fill = c(theme$night, theme$day),
                     border = c(theme$bar.border, theme$day.border), horiz = TRUE, bty = "n",
                     cex = 1.02 * theme$cex, text.col = theme$axis)
  }

  for (k in seq_along(panels)) {
    p <- panels[[k]]; v <- p$v; br <- breaks_by_var[[v]]; row <- ((k - 1L) %/% lay$nc) + 1L; col <- ((k - 1L) %% lay$nc) + 1L
    ids <- aggs[[k]]$ids; agg <- aggs[[k]]$agg
    y.axis <- col == 1L || !identical(p$v, panels[[k - 1L]]$v)   # draw the bin axis when the variable changes
    x.axis <- row == lay$nr || (k + lay$nc) > length(panels)     # bottom of its column
    # pooled single-variable figures would repeat the banner verbatim, so the panel title is dropped there;
    # with two variables it identifies the column and is kept (sentence case, matching the banner).
    ttl_p <- if (grouped) p$g else if (n_var == 1L) NULL else .tadCap1(.tadVarName(v))
    ttl_col <- if (grouped) gcols[[p$g]] else theme$ink
    sub_p <- if (grouped) sprintf("n = %d", length(ids)) else NULL
    # theme$night MEANS night once diel is on; a pooled bar is not night, so it takes the palette instead
    fill <- if (grouped) gcols[[p$g]] else .themePalette(theme$palette, 1L)[1]
    .tadProfilePanel(br, agg, diel, theme, fill, .tadLabel(v), .tadXlab(v), ttl_p, ttl_col, sub_p, y.axis, x.axis,
                     xmax = if (is.null(xmax_by_var)) NULL else xmax_by_var[[v]])
  }
}

#' Mean +/- SE per bin (length `nb`) across a set of deployments, per phase.
#' @keywords internal
#' @noRd
.tadAggregate <- function(binned_v, ids, phases, nb) {
  out <- list()
  for (ph in phases) {
    cols <- binned_v[[ph]][ids]; cols <- cols[!vapply(cols, is.null, logical(1))]
    if (!length(cols)) { out[[ph]] <- list(mean = rep(0, nb), se = rep(0, nb)); next }
    M <- do.call(cbind, cols)                                   # nb x n_deployment
    out[[ph]] <- list(mean = rowMeans(M, na.rm = TRUE),
                      se = if (ncol(M) > 1) apply(M, 1, stats::sd, na.rm = TRUE) / sqrt(ncol(M)) else rep(0, nb))
  }
  out
}

#' One profile panel: equal-height bin bands, single or diel-mirrored, mean +/- SE.
#' @keywords internal
#' @noRd
.tadProfilePanel <- function(breaks, agg, diel, theme, fill, ylab, xlab, title, title.col, subtitle, y.axis, x.axis,
                             xmax = NULL) {
  nb <- length(breaks) - 1L; cex <- theme$cex
  if (diel) { L <- agg[["night"]]; R <- agg[["day"]] } else { L <- NULL; R <- agg[["all"]] }
  if (is.null(xmax)) xmax <- max(c(R$mean + R$se, if (!is.null(L)) L$mean + L$se), 0.001, na.rm = TRUE) * 1.04
  xlim <- if (diel) c(-xmax, xmax) else c(0, xmax)
  # mar is CONSTANT across every panel of a figure - varying it by which axes get drawn made the plot
  # regions different sizes, so the same bin was a different physical height in adjacent panels and the
  # frames did not line up. Only the drawing calls below are gated on y.axis / x.axis.
  graphics::par(mar = c(3.8, 5.4, 2.7, 1.8), mgp = c(3, 0.55, 0), tcl = -0.22)      # mgp[1] unused: titles are mtext-placed
  graphics::plot(NA, xlim = xlim, ylim = c(nb + 0.5, 0.5), axes = FALSE, xlab = "", ylab = "", xaxs = "i", yaxs = "i")
  gx <- pretty(c(0, xmax), 4); gx <- gx[gx <= xmax]                                  # pretty() overshoots xlim
  at <- if (diel) sort(unique(c(-gx, gx))) else gx                                   # sorted, no duplicated 0
  graphics::rect(xlim[1], 0.5, xlim[2], nb + 0.5, col = theme$panel, border = NA)
  graphics::abline(v = at[at != 0], col = theme$grid, lwd = 0.6)                     # subtle grid
  bar <- function(x0, x1, b, col, bord) graphics::rect(x0, b - 0.5, x1, b + 0.5, col = grDevices::adjustcolor(col, theme$bar.alpha), border = bord, lwd = 0.6)
  for (b in seq_len(nb)) {
    if (diel) { bar(-L$mean[b], 0, b, theme$night, theme$bar.border); bar(0, R$mean[b], b, theme$day, theme$day.border) }
    else bar(0, R$mean[b], b, fill, theme$bar.border)
  }
  # SE whiskers, drawn as conventional T-bars (stem + a cap at each end). Bins with no spread are SKIPPED
  # rather than drawn as a bare cap on the bar's edge, which would read as a measured zero-width interval
  # when it actually means a single deployment contributed (.tadAggregate sets se = 0 when ncol == 1).
  err <- function(m, se, sgn) {
    ok <- is.finite(m) & is.finite(se) & se > 0
    if (!any(ok)) return(invisible(NULL))
    y  <- seq_len(nb)[ok]
    lo <- sgn * pmax(0, (m - se)[ok]); hi <- sgn * (m + se)[ok]
    cap <- 0.16                                              # half-height of the caps, in bin units
    graphics::segments(lo, y, hi, y, col = theme$ink, lwd = 1.1)
    graphics::segments(lo, y - cap, lo, y + cap, col = theme$ink, lwd = 1.1)
    graphics::segments(hi, y - cap, hi, y + cap, col = theme$ink, lwd = 1.1)
  }
  if (diel) { err(L$mean, L$se, -1); err(R$mean, R$se, 1) } else err(R$mean, R$se, 1)
  if (diel) graphics::abline(v = 0, col = theme$axis, lwd = 1.2)
  # axes (type sizes lifted onto the package band - these were the smallest in the plot* family)
  graphics::axis(1, at = at, labels = if (x.axis) paste0(abs(at), "%") else FALSE,
                 col = NA, col.ticks = theme$axis, col.axis = theme$axis, cex.axis = 0.85 * cex)
  if (y.axis) {
    # every bin keeps its label: the figure height now scales with nb (see .tadFigSize), so the old
    # every-other-bin thinning is unnecessary - and it used to delete the open-top ">X" label whenever
    # nb was even, leaving the deepest stratum anonymous.
    graphics::axis(2, at = seq_len(nb), labels = .binLabels(breaks), las = 1, col = NA,
                   col.ticks = theme$axis, col.axis = theme$axis, cex.axis = 0.72 * cex)
    graphics::mtext(ylab, 2, line = 4.2, col = theme$ink, cex = 0.95 * cex, font = 2)
  }
  if (x.axis) graphics::mtext(xlab, 1, line = 2.4, col = theme$axis, cex = 0.95 * cex)
  if (!is.null(title))
    graphics::mtext(title, 3, line = if (is.null(subtitle)) 0.5 else 0.95, adj = 0, font = 2, col = title.col, cex = 1.0 * cex)
  if (!is.null(subtitle)) graphics::mtext(subtitle, 3, line = 0.1, adj = 0, col = theme$subtitle, cex = 0.72 * cex)
  graphics::box(col = theme$axis)                                # full border, matching the themed axes
}


################################################################################
# Rendering: heatmap (compact all-individuals option) ##########################
################################################################################

#' Render a themed deployment x bin heatmap per variable, sharing a colour bar.
#' @keywords internal
#' @noRd
.tadRenderHeatmaps <- function(binned, breaks_by_var, variable, loaded, order.by, theme, grouped) {
  pal <- grDevices::colorRampPalette(theme$sequential)(100)
  ids0 <- names(loaded)
  ord <- switch(order.by,
                median = ids0[order(vapply(loaded, function(d) {
                  z <- d$series[[variable[1]]]; z <- z[is.finite(z)]
                  if (!length(z)) NA_real_ else stats::median(z)
                }, numeric(1)), na.last = TRUE)],
                id = sort(ids0), ids0)
  mats <- lapply(variable, function(v) do.call(cbind, binned[[v]][["all"]][ord]))
  zmax <- max(vapply(mats, function(M) max(M, na.rm = TRUE), numeric(1)), 1)
  n_var <- length(variable)
  graphics::layout(matrix(seq_len(n_var + 1L), 1), widths = c(rep(1, n_var), graphics::lcm(3.0)))
  for (vi in seq_len(n_var)) {
    v <- variable[vi]; br <- breaks_by_var[[v]]; nb <- length(br) - 1L; M <- mats[[vi]]
    graphics::par(mar = c(6.2, 5.0, 2.6, 0.7), mgp = c(3, 0.6, 0))   # left margin on every panel: each has its own axis
    graphics::plot(NA, xlim = c(0.5, ncol(M) + 0.5), ylim = c(nb + 0.5, 0.5), axes = FALSE, xaxs = "i", yaxs = "i", xlab = "", ylab = "")
    for (j in seq_len(ncol(M))) for (b in seq_len(nb)) {
      idx <- max(1L, min(100L, round(.rescale(M[b, j], from = c(0, zmax), to = c(1, 100)))))
      graphics::rect(j - 0.5, b - 0.5, j + 0.5, b + 0.5, col = pal[idx], border = NA)
    }
    # EVERY variable gets its own bin axis. Sharing one axis was wrong, not merely terse: the panels hold
    # different bin counts (e.g. 19 depth strata vs 12 temperature strata) drawn as equal-height rows, so a
    # temperature row was read against a depth label - a band at 22 degrees C sat against ">300 m".
    graphics::axis(2, at = seq_len(nb), labels = .binLabels(br), las = 1, col = NA, col.ticks = theme$axis,
                   col.axis = theme$axis, cex.axis = 0.68 * theme$cex)
    graphics::mtext(.tadLabel(v), 2, line = 3.9, font = 2, col = theme$ink, cex = 0.92 * theme$cex)
    graphics::text(seq_len(ncol(M)), graphics::grconvertY(0, "npc", "user"), labels = ord, srt = 45, adj = c(1, 1.3),
                   xpd = NA, cex = theme$cex * min(0.8, 22 / ncol(M)), col = theme$axis)
    graphics::mtext(.tadCap1(.tadVarName(v)), 3, line = 0.6, adj = 0, font = 2, col = theme$ink, cex = 0.95 * theme$cex)
    graphics::box(col = theme$axis)                              # frame the heatmap grid
  }
  graphics::par(mar = c(6.2, 0.6, 2.6, 3.0))
  graphics::plot(NA, xlim = c(0, 1), ylim = c(0, zmax), axes = FALSE, xaxs = "i", yaxs = "i", xlab = "", ylab = "")
  ys <- seq(0, zmax, length.out = 101); graphics::rect(0, ys[-101], 1, ys[-1], col = pal, border = NA); graphics::rect(0, 0, 1, zmax, border = theme$axis)
  graphics::axis(4, at = pretty(c(0, zmax), 5), las = 1, col = NA, col.ticks = theme$axis, col.axis = theme$axis, cex.axis = 0.7 * theme$cex)
  graphics::mtext("% time", 3, line = 0.3, cex = 0.78 * theme$cex, font = 2, col = theme$ink)
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
