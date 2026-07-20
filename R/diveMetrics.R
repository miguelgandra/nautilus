#######################################################################################################
# Per-dive metrics ####################################################################################
#######################################################################################################

#' Summarise each detected dive
#'
#' @description
#' Reduces the per-sample output of \code{\link{detectDives}} to one row per dive: timing, depth,
#' phase structure, kinematics, and a quality block saying what each row can and cannot support.
#'
#' `variables` summarises ANY per-sample channel over each dive - present or future - with correct
#' circular handling for headings and roll. That single argument is what makes this a general reducer
#' rather than a fixed list of depth statistics.
#'
#' @param data Data annotated by \code{\link{detectDives}}: `.rds` paths, a `nautilus_tag` /
#'   data.frame, or a list of them.
#' @param variables Character vector of per-sample columns to summarise per dive (e.g.
#'   `c("temp", "odba", "tbf_hz")`). `NULL` (default) adds none. Each costs 2 columns, or 8 with
#'   `by.phase = TRUE`.
#' @param circular.variables Character. Which of `variables` are angles in degrees, summarised as a
#'   mean angle and a mean resultant length. Default `c("heading", "roll")`, matching
#'   \code{\link{extractFeatures}}.
#' @param statistics Character. Which statistics to compute for `variables`: any of `"mean"`, `"sd"`.
#' @param by.phase Logical. Also summarise `variables` within descent / bottom / ascent. Default `FALSE`.
#' @param id.col,datetime.col,depth.col Column names for the deployment id, timestamp and depth.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal", or `2`/"detailed" (default).
#'
#' @details
#' \strong{NA semantics, one rule per block.} A NA in a phase or kinematics column means "not supported
#' for this dive", and `shape_supported` says so explicitly. A NA in `inter_dive_s` means "last dive in
#' this deployment". A NA in an auxiliary column means the source channel was absent or entirely NA over
#' that dive. Column presence never varies with anything except `variables` and `by.phase`, so `rbind`
#' across a mixed cohort always succeeds.
#'
#' \strong{Rates are reported as `_q90`, not `_max`.} The maximum of a smoothed series is an artefact of
#' the smoothing window, and its magnitude depends on dive duration, so maxima are not comparable
#' between a short dive and a long one even within one animal.
#'
#' \strong{The threshold travels with every row.} `reference`, `direction`, `depth_threshold_m` and
#' `surface_band_m` are columns, not metadata, so a bound cohort table is self-documenting and a
#' published dive count is reproducible from the table alone. When `detectDives(reference = "auto")`
#' resolved differently across deployments, the `reference` column makes that mixture visible.
#'
#' \strong{What is deliberately not computed.} Dive efficiency, aerobic dive limit and dive:pause ratio
#' are air-breather constructs that assume the surface interval is a recovery period. `bottom_duration_s`
#' and `inter_dive_s` are provided; if those constructs are meaningful for your animal, form them
#' yourself rather than have the package assert they apply.
#'
#' @return A `data.frame` (class `nautilus_dive_metrics`), one row per dive, with a fixed schema.
#' @seealso \link{detectDives}, \link{diveControl}, \link{summarizeTagData}, \link{plotDistributions}
#' @examples
#' \dontrun{
#' tag <- detectDives(processed, control = diveControl(depth.threshold = 5))
#' dt  <- diveMetrics(tag, variables = c("temp", "odba"), by.phase = TRUE)
#' plotDistributions(dt, metrics = c("max_depth_m", "duration_s"))
#' }
#' @export

diveMetrics <- function(data,
                        variables          = NULL,
                        circular.variables = c("heading", "roll"),
                        statistics         = c("mean", "sd"),
                        by.phase           = FALSE,
                        id.col             = "ID",
                        datetime.col       = "datetime",
                        depth.col          = "depth",
                        verbose            = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  statistics <- match.arg(statistics, c("mean", "sd"), several.ok = TRUE)
  .assert_flag(by.phase, "by.phase")
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  .assert_string(depth.col, "depth.col")
  if (!is.null(variables) && (!is.character(variables) || !length(variables)))
    .abort("{.arg variables} must be a non-empty character vector of column names, or {.code NULL}.")
  if (!is.null(circular.variables) && !is.character(circular.variables))
    .abort("{.arg circular.variables} must be a character vector, or {.code NULL}.")
  if (length(variables) > 10)
    cli::cli_warn(c("{length(variables)} variables requested; the table gains {length(variables) * (if (by.phase) 8 else 2)} columns.",
                    "i" = "Consider summarising a subset."))

  src <- .resolveInput(data, id.col)
  .log_header(lvl, "diveMetrics", "Summarising each detected dive",
              bullets = sprintf("Input: %d deployment%s\u00b7%s", src$n, if (src$n != 1) "s " else " ",
                                if (is.null(variables)) " depth and phase metrics only"
                                else sprintf(" plus %d channel%s", length(variables),
                                             if (length(variables) != 1) "s" else "")))

  rows <- list(); n_dep <- 0L; n_missing <- 0L
  pb <- .log_progress_start(lvl, src$n, "Reducing")
  for (i in seq_len(src$n)) {
    .log_progress_step(pb)
    x <- data.table::as.data.table(src$get(i))
    id <- as.character(.getMeta(x)$id %||% src$ids[i])
    if (!all(c("dive_id", "dive_phase", datetime.col, depth.col) %in% names(x))) {
      n_missing <- n_missing + 1L; next
    }
    r <- .diveMetricsOne(x, id, datetime.col, depth.col, variables, circular.variables,
                         statistics, by.phase)
    if (!is.null(r) && nrow(r)) { rows[[length(rows) + 1L]] <- r; n_dep <- n_dep + 1L }
  }
  .log_progress_done(pb)

  if (n_missing > 0)
    cli::cli_warn(c("{n_missing} deployment{?s} lack{?s/} the {.field dive_id} column and {?was/were} skipped.",
                    "i" = "Run {.fn detectDives} first."))
  if (!length(rows)) {
    if (lvl >= 1L) {
      .log_summary(lvl); .log_done(lvl, 0L, " dives summarised")
      .log_runtime(lvl, start.time)
    }
    return(structure(.diveMetricsSchema(variables, circular.variables, statistics, by.phase),
                     class = c("nautilus_dive_metrics", "data.frame")))
  }
  out <- do.call(rbind, rows); rownames(out) <- NULL

  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, nrow(out), " dive", if (nrow(out) != 1) "s", " summarised across ", n_dep,
              " deployment", if (n_dep != 1) "s")
    ok <- sum(out$shape_supported, na.rm = TRUE)
    .log_arrow(lvl, sprintf("phase structure resolved for %s of %s dive%s",
                            format(ok, big.mark = ","), format(nrow(out), big.mark = ","),
                            if (nrow(out) != 1) "s" else ""))
    if (length(unique(out$reference)) > 1)
      .log_detail(lvl, sprintf("mixed reference across the cohort: %s",
                               paste(sprintf("%s x%d", names(table(out$reference)),
                                             as.integer(table(out$reference))), collapse = " \u00b7 ")))
    n_trunc <- sum(out$truncated_start | out$truncated_end, na.rm = TRUE)
    n_gapped <- sum(out$n_gaps > 0, na.rm = TRUE)
    if (n_trunc + n_gapped > 0)
      .log_detail(lvl, sprintf("censored: %d truncated at a record boundary \u00b7 %d gap-interrupted",
                               n_trunc, n_gapped))
    .log_runtime(lvl, start.time)
  }
  structure(out, class = c("nautilus_dive_metrics", "data.frame"))
}
