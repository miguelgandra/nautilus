#######################################################################################################
# Detect and repair signal-quality anomalies in sensor channels #######################################
#######################################################################################################

#' Detect and repair signal-quality anomalies in sensor channels
#'
#' @description
#' Screens one or more sensor channels (e.g. depth, temperature) for transient signal-quality anomalies
#' and, optionally, repairs them. Two kinds are detected per channel: **isolated outliers** - single
#' spikes whose rate of change exceeds a threshold (accounting for sensor resolution), replaced with
#' `NA` or linear interpolation - and **sensor-malfunction periods** - clusters of outliers, or prolonged
#' runs of constant readings (stalls), which are removed as blocks (with any small stranded islands of
#' valid data between them) and never interpolated.
#'
#' This is the signal-quality half of the sensor-QC pair. It repairs the recorded time series of a valid
#' channel; \link{checkSensorIntegrity} is its structural counterpart, which validates whether a channel
#' is a trustworthy instance of its sensor at all. Run \link{checkSensorIntegrity} first, so that
#' structurally corrupt channels are excluded before their symptoms are cosmetically repaired here.
#'
#' @details
#' Two kinds of anomaly are detected per channel:
#' \itemize{
#'   \item \strong{Isolated outliers} (`info`) - single spikes whose sample-to-sample rate of change
#'     exceeds the channel's `rate.threshold`, gated by its `sensor.resolution` so ordinary quantisation
#'     noise is not flagged. These are transient glitches; with `interpolate = TRUE` they are linearly
#'     interpolated from their neighbours, otherwise left as `NA`.
#'   \item \strong{Malfunction / stall periods} (`warning`) - clusters of outliers within `outlier.window`
#'     minutes of one another, or prolonged runs of a constant non-zero reading longer than
#'     `stall.threshold` minutes (a stuck sensor). These signal a sustained sensor failure and are removed
#'     as whole blocks (together with any small islands of valid data stranded between them); they are
#'     never interpolated.
#' }
#' All thresholds are supplied per channel via \link{anomalyControl}.
#'
#' \strong{Report vs. repair.} Like \link{checkSensorIntegrity}, the function is report-first: with
#' `apply = FALSE` (default) it only *detects* the anomalies and returns them in `issues`, leaving the data
#' untouched; with `apply = TRUE` it additionally *writes* the repairs into the returned/saved data. This
#' lets you review what would change before committing to it.
#'
#' \strong{Diagnostic report.} When `plot` or `plot.file` is set, an overview page tables every tag and its
#' anomaly count per channel (sorted worst-first), followed by one page per tag with a stall/malfunction
#' block - each anomalous channel's trace with the removed spikes (red), interpolated values (cyan) and
#' shaded block spans, plus an interpretation note. Tags with only isolated spikes appear in the overview
#' but do not get their own page.
#'
#' @param data Sensor data in any of the pipeline forms: a list of `nautilus_tag` objects (one per
#'   individual), a single aggregated data.table/data.frame with an `id.col`, or a character vector of
#'   `.rds` file paths (loaded lazily). The output of \link{importTagData} is expected.
#' @param sensors A named list mapping each channel to screen (e.g. `depth`, `temp`) to an
#'   \link{anomalyControl} object (or a named list of its fields) carrying that channel's thresholds.
#'   Channels absent from a given deployment are skipped for that individual.
#' @param apply Logical. If `FALSE` (default), the function only reports the detected anomalies (the data
#'   passes through unchanged). If `TRUE`, the repairs are written into the returned/saved data (isolated
#'   spikes interpolated or set to `NA` per `interpolate`; malfunction/stall blocks removed).
#' @param interpolate Logical. If `TRUE` (default), isolated outliers are linearly interpolated;
#'   otherwise they are left as `NA`. Malfunction/stall blocks are always removed, never interpolated.
#'   Only consulted when `apply = TRUE`.
#' @param id.col,datetime.col Column names for the animal ID and datetime (POSIXct). Defaults
#'   `"ID"`/`"datetime"`.
#' @param plot Logical. If `TRUE`, draw the diagnostic report (overview page + one page per flagged
#'   deployment) to the active graphics device. Default `FALSE`.
#' @param plot.file Character. Path to a single multi-page PDF for the report. The parent directory must
#'   exist; the name must end in `.pdf`. `NULL` (default) writes none. Independent of `plot`.
#' @param return.data Logical. Return the curated data in memory (default `TRUE`). When `FALSE` and
#'   `apply = TRUE`, the function instead returns the paths of the `.rds` files it wrote, which feed
#'   directly into the next step's `data` argument -- so a large fleet can be curated without ever holding
#'   it all in memory; `return.data = FALSE` then requires an `output.dir`. A report-only run
#'   (`apply = FALSE`) still returns the `issues` report regardless.
#' @param output.dir Character. Directory in which to write one curated `<id>.rds` file per deployment.
#'   Providing a directory is what triggers saving; `NULL` (default) writes nothing. Requires `apply = TRUE`
#'   (a report-only run does not repair anything, so writing is refused to avoid files that look curated
#'   but aren't). The directory must already exist.
#' @param output.suffix Character. Optional suffix appended to each saved file name (before `.rds`), e.g.
#'   to tag a processing run or avoid clashes. Only used when `output.dir` is set. Default `NULL`.
#' @param compress Compression for the saved `.rds` files (only used when `output.dir` is set): `TRUE`
#'   (default, gzip), `FALSE`, or one of `"gzip"`/`"bzip2"`/`"xz"`. See \code{\link[base]{saveRDS}}.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal", or `2`/"detailed" (default).
#'
#' @return A list with:
#' \itemize{
#'   \item \code{curated_data}: with `return.data = TRUE` (the default), a named list of `data.table`s,
#'     one per deployment. **It is repaired ONLY when `apply = TRUE`.** With `apply = FALSE` - which is
#'     the default - you get the input back UNCHANGED, under a name that says curated. Nothing is
#'     repaired, and the anomalies live in `issues` instead. That combination is easy to misread as
#'     screened data, so the verbose summary says so explicitly; read it, or test
#'     `attr(x, "processing")`'s `applied` flag. With `return.data = FALSE` it is a character vector of
#'     written paths (which needs `apply = TRUE`), and `NULL` for a report-only run that wrote nothing.
#'     Each returned object carries a `checkSensorQuality` entry in its metadata audit trail recording
#'     whether the repairs were `applied`.
#'   \item \code{issues}: a data.frame of per-channel findings (zero rows when everything is clean), with
#'     columns \code{id}, \code{channel}, \code{severity} (`"warning"` for a block/stall, `"info"` for
#'     spikes only), \code{n_corrected}, \code{spikes}, \code{blocks}, \code{pct_affected} and \code{message}.
#' }
#'
#' @section Known limitation - the rate gate is sampling-rate dependent:
#' The spike test gates on `abs(diff) > sensor.resolution / dt`, which compares a value against a rate.
#' The effective floor is therefore `sensor.resolution / dt`, so sensitivity SCALES WITH THE SAMPLING
#' INTERVAL: at 100 Hz with `sensor.resolution = 0.5` a step must exceed 50 units to be considered at
#' all, where at 1 Hz it need only exceed 0.5. This does NOT affect gross sensor failure - validated on
#' six real deployments carrying impossible depth (to 3059 m) and temperature (to 52,657 degrees)
#' excursions, every one detected and fully repaired - but a SUBTLE in-range transient is harder to
#' catch on a fast record than on a slow one. The dimensionally-obvious correction is worse and
#' mass-flags noise; a proper fix needs a noise-floor estimator and is deliberately deferred. Treat the
#' rate test as a tripwire for gross failure rather than a fine screen, and set `sensor.resolution`
#' from the channel's true quantum.
#'
#' @seealso \link{checkSensorIntegrity}, \link{anomalyControl}, \link{importTagData}.
#' @examples
#' \dontrun{
#' checked <- checkSensorIntegrity(regularized, apply = TRUE)$curated_data
#' # Repair transient glitches on the surviving channels (spikes interpolated, stalls removed):
#' quality <- checkSensorQuality(
#'   checked,
#'   sensors = list(
#'     depth = anomalyControl(rate.threshold = 7, sensor.resolution = 0.5),
#'     temp  = anomalyControl(rate.threshold = 1, sensor.resolution = 0.05)),
#'   apply = TRUE)
#' quality$issues
#' }
#' @export

checkSensorQuality <- function(data,
                               sensors,
                               apply = FALSE,
                               interpolate = TRUE,
                               id.col = "ID",
                               datetime.col = "datetime",
                               plot = FALSE,
                               plot.file = NULL,
                               return.data = TRUE,
                               output.dir = NULL,
                               output.suffix = NULL,
                               compress = TRUE,
                               verbose = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  .assert_flag(apply, "apply"); .assert_flag(return.data, "return.data")
  .assert_flag(plot, "plot"); .assert_flag(interpolate, "interpolate")
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  .assert_writable_file(plot.file, "plot.file", ext = "pdf")
  .assert_dir(output.dir, "output.dir")
  .assert_string(output.suffix, "output.suffix", null_ok = TRUE)
  .assert_compress(compress)
  # a report-only run (apply = FALSE) leaves the data untouched, so it need not be returned or saved -
  # you still get the `issues` report. An output sink is only required when apply = TRUE writes the repairs.
  if (apply) .assert_output(return.data, output.dir)
  # the mirror-image guard: writing in report-only mode would write byte-identical copies of the input
  # that merely LOOK curated - the single most misleading thing this function could do to disk.
  if (!is.null(output.dir) && !apply)
    .abort(c("Writing curated data (an {.arg output.dir}) needs {.code apply = TRUE}.",
             "i" = "A report-only run ({.code apply = FALSE}) does not repair the detected anomalies, so the saved files would carry the same uncurated sensor data as the input - a file that looks curated but isn't.",
             "i" = "To write curated data, set {.code apply = TRUE}. To review first, keep {.code apply = FALSE}, drop {.code output.dir}, and inspect the returned {.field issues} table."))

  # resolve the per-channel controls (an object, or a named list of anomalyControl fields, per channel)
  if (!is.list(sensors) || !length(sensors) || is.null(names(sensors)) || any(!nzchar(names(sensors))))
    .abort("{.arg sensors} must be a non-empty NAMED list mapping each channel to an {.fn anomalyControl}.")
  sensors <- stats::setNames(lapply(names(sensors), function(nm)
    .as_control(sensors[[nm]], anomalyControl, "nautilus_anomaly", paste0("sensors$", nm))), names(sensors))
  channels <- names(sensors)

  r <- .resolveInput(data, id.col = id.col)
  if (r$n == 0) return(list(curated_data = if (return.data) list() else NULL, issues = .emptyQualityIssues()))
  make_plots <- plot || !is.null(plot.file)

  hdr_bullets <- sprintf("Input: %d tag%s", r$n, if (r$n != 1) "s" else "")
  if (apply && !is.null(output.dir)) hdr_bullets <- c(hdr_bullets, paste0("Output: ", output.dir))
  .log_header(lvl, "checkSensorQuality", "Scanning sensor channels for signal-quality anomalies",
              bullets = hdr_bullets,
              arrow = c(sprintf("Channels: %s", paste(channels, collapse = " \u00b7 ")),
                        sprintf("Mode: %s", if (!apply) "report only" else
                                            if (interpolate) "repair (interpolate spikes, remove blocks)" else "repair (remove, no interpolation)")))


  results         <- if (return.data) vector("list", r$n) else NULL
  saved           <- vector("list", r$n)
  all_issues      <- vector("list", r$n)
  summary_records <- if (make_plots) vector("list", r$n) else NULL
  payloads        <- list()
  n_flagged <- 0L

  for (i in seq_len(r$n)) {
    x <- r$get(i)
    id <- tryCatch(as.character(unique(x[[id.col]])[1]), error = function(e) NA_character_)
    if (is.na(id)) id <- as.character(r$ids[i])
    .log_h2(lvl, sprintf("%s (%d/%d)", id, i, r$n))

    if (is.null(x) || nrow(x) == 0) {
      .log_skip(lvl, id, "  empty dataset ", cli::symbol$bullet, " skipped"); .log_gap(lvl)
      all_issues[[i]] <- .emptyQualityIssues()
      if (return.data) { results[[i]] <- x; names(results)[i] <- id }; next
    }
    if (!data.table::is.data.table(x)) x <- data.table::as.data.table(x)
    data.table::setorderv(x, cols = datetime.col)

    # preserve non-internal attributes across the data.table edits (the metadata rides on these)
    discard_attrs <- c("row.names", "class", ".internal.selfref", "names")
    keep_attrs <- attributes(x)[!names(attributes(x)) %in% discard_attrs]

    # detect per channel; collect an issue row + (when plotting) a panel; write the repair only if apply
    tag_issues <- .emptyQualityIssues(); panels <- list()
    for (ch in channels) {
      if (!(ch %in% names(x))) { if (lvl >= 2L) .log_detail(lvl, sprintf("%s: absent (skipped)", ch)); next }
      res <- .detectChannelAnomalies(x, ch, datetime.col, sensors[[ch]], interpolate)
      if (res$anomalies) {
        tag_issues <- rbind(tag_issues, .qualityIssue(id, ch, res, interpolate))
        if (make_plots) panels[[length(panels) + 1L]] <- .qualityChannelPanel(x, ch, datetime.col, res)
        if (apply) x[, (ch) := res$repaired]
      }
      if (lvl >= 2L) .log_detail(lvl, .qualityChannelLine(ch, res))
    }
    all_issues[[i]] <- tag_issues
    any_anom <- nrow(tag_issues) > 0
    if (any_anom) n_flagged <- n_flagged + 1L

    if (make_plots) {
      summary_records[[i]] <- .qualitySummaryRecord(id, channels, tag_issues)
      if (any(tag_issues$severity == "warning"))                          # detail page: block/stall tags only
        payloads[[length(payloads) + 1L]] <- list(id = id, summary = .qualityVerdict(tag_issues), panels = panels)
    }

    # restore attributes + audit trail (records whether the repairs were actually applied)
    for (a in names(keep_attrs)) attr(x, a) <- keep_attrs[[a]]
    meta <- .getMeta(x)
    if (!is.null(meta)) x <- .restoreMeta(x, .appendProcessing(meta, "checkSensorQuality",
                                          channels = paste(channels, collapse = ","),
                                          n_anomalies = sum(tag_issues$n_corrected), applied = apply, interpolate = interpolate))

    saved[i] <- list(.saveOutput(x, id, output.dir = output.dir, output.suffix = output.suffix, compress = compress))

    # ---- per-tag tick ----
    if (lvl == 1L) {
      if (any_anom) {
        n_tot <- sum(tag_issues$n_corrected); nw <- sum(tag_issues$severity == "warning")
        .log_skip(lvl, id, "  ", .formatNumber(n_tot), " anomal", if (n_tot == 1) "y" else "ies",
                  if (apply) " corrected" else " detected",
                  if (nw) sprintf("  %s %d block channel%s", cli::symbol$bullet, nw, if (nw != 1) "s" else "") else "")
      } else .log_ok(lvl, id, "  no anomalies")
    } else if (lvl >= 2L) {
      .log_ok(lvl, if (any_anom && apply) "repaired" else if (any_anom) "flagged (report only)" else "clean")
    }
    .log_gap(lvl)

    if (return.data) { results[[i]] <- x; names(results)[i] <- id }
    rm(x)
  }

  issues <- do.call(rbind, all_issues)
  if (is.null(issues)) issues <- .emptyQualityIssues()
  rownames(issues) <- NULL

  # ---- draw the report: overview page (all tags), then one page per flagged (warning) tag ----
  if (make_plots) {
    run_stats <- list(n = r$n, n_flagged = n_flagged, channels = channels, apply = apply,
                      n_warn = length(unique(issues$id[issues$severity == "warning"])))
    draw <- function(to.file = FALSE, unicode = TRUE) {
      uni <- unicode                                             # Unicode only where the target device supports it
      .drawQualitySummaryPage(Filter(Negate(is.null), summary_records), channels, run_stats, use_unicode = uni)
      for (p in payloads) .plotQualityIndividual(p, use_unicode = uni)
    }
    .renderToDevices(draw, plot = plot, plot.file = plot.file, width = 10, height = 7.5, cairo = TRUE)
  }

  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, n_flagged, " of ", r$n, " tag", if (r$n != 1) "s", if (apply) " repaired" else " flagged")
    # A report-only run that FOUND something is the dangerous case: `curated_data` comes back under a
    # name that promises curation, holding the input untouched. Say it plainly and say what to do next.
    # (Written after falling into it during this package's own validation: the function was run on
    # records with 84,419 impossible depth samples, the samples were still there afterwards, and the
    # detector was briefly believed to have failed. It had not - `apply` was FALSE.)
    if (!apply && n_flagged > 0) {
      .log_arrow(lvl, "REPORT ONLY: nothing was repaired and the data is unchanged")
      .log_detail(lvl, "the anomalies are in $issues; re-run with apply = TRUE to write the repairs")
    }
    if (apply && !is.null(output.dir)) .log_arrow(lvl, "output: ", output.dir)
    if (!is.null(plot.file)) .log_arrow(lvl, "plots: ", plot.file)
    .log_runtime(lvl, start.time)
  }

  curated <- if (return.data) results else if (apply) unlist(saved, use.names = FALSE) else NULL
  list(curated_data = curated, issues = issues)
}


################################################################################
# Per-channel detection engine (internal) ######################################
################################################################################

#' Detect anomalies in one channel and return a repair PLAN, WITHOUT touching `dt`.
#'
#' The exact detection + repair algorithm runs on a working copy of the channel vector; the function
#' returns the repaired vector plus the flagged / interpolated / block indices and counts. The driver
#' writes the repaired vector back only when `apply = TRUE`, so a report-only run leaves the data
#' untouched. `severity` is `"warning"` when a malfunction/stall block was found (a sustained failure) and
#' `"info"` when only isolated spikes were.
#' @keywords internal
#' @noRd
.detectChannelAnomalies <- function(dt, sensor.col, datetime.col, ctrl, interpolate) {
  rate.threshold    <- ctrl$rate.threshold
  sensor.resolution <- ctrl$sensor.resolution
  outlier.window    <- ctrl$outlier.window
  stall.threshold   <- ctrl$stall.threshold

  times <- dt[[datetime.col]]
  v <- dt[[sensor.col]]                              # WORKING COPY - the algorithm mutates this, never dt

  time_diffs <- round(as.numeric(diff(times), units = "secs"), 6)
  nominal_interval <- stats::median(time_diffs, na.rm = TRUE)
  sampling_freq <- round(1 / nominal_interval, 1)
  dt_secs <- c(NA, as.numeric(diff(times), units = "secs"))
  value_diff <- c(NA, diff(v))

  # Rate of change, gated so ordinary quantisation and high-frequency noise are not read as events.
  #
  # CAUTION, and read this before "fixing" the line below. It compares a VALUE (units) against
  # `sensor.resolution / dt` (units per second), which is dimensionally incoherent, and the obvious
  # correction - comparing like with like, `abs(value_diff) > sensor.resolution` - is WORSE. It was
  # tried and reverted. Measurement noise of standard deviation s produces apparent rates of about
  # s*sqrt(2)/dt, which grows without bound as the sampling interval shrinks, so at a high rate ANY
  # fixed physical rate threshold is exceeded by noise alone. Measured on the 16 Hz / sd 0.3 fixture in
  # test-checkSensorQuality.R: typical |diff| is 0.42, an apparent 6.8 units/s against a 2 units/s
  # threshold, and the dimensionally-correct gate mass-flagged the whole series.
  #
  # So the `/ dt` is doing real work: it is the only term that scales the floor with the sampling
  # interval. It is the wrong QUANTITY for that job - the thing that should scale is the measured NOISE
  # of the channel, not its quantisation step - but replacing it needs a noise-floor estimator and a
  # decision about how to expose it, not a units patch. Left as is, deliberately, and flagged.
  rate_uncertainty <- sensor.resolution / dt_secs
  rate_of_change <- ifelse(abs(value_diff) > rate_uncertainty, value_diff / dt_secs, NA)

  # isolated jumps + prolonged constant runs (stalls; zero values excused, e.g. depth at the surface)
  jump_indices <- which(abs(rate_of_change) > rate.threshold & !is.na(rate_of_change))
  stall_threshold_samples <- sampling_freq * 60 * stall.threshold
  stall_indices <- c()
  rle_sensor <- rle(v)
  stall_periods <- rle_sensor$lengths >= stall_threshold_samples & (rle_sensor$values > 0)
  if (any(stall_periods)) {
    rle_indices <- cumsum(rle_sensor$lengths)
    start_indices <- rle_indices[stall_periods] - rle_sensor$lengths[stall_periods] + 1
    end_indices <- rle_indices[stall_periods]
    for (a in seq_along(start_indices)) stall_indices <- c(stall_indices, start_indices[a]:end_indices[a])
  }
  outlier_indices <- unique(sort(c(jump_indices, stall_indices)))

  flagged_outliers <- c(); interpolated_outliers <- c(); block_indices <- c()
  n_corr <- 0L; affected_secs <- 0; pct_affected <- 0
  isolated_outliers_count <- 0; malfunction_blocks_count <- 0; malfunction_duration <- 0
  anomalies <- length(outlier_indices) > 0

  if (anomalies) {
    anomaly_window <- outlier.window * 60 * sampling_freq
    outlier_groups <- split(outlier_indices, cumsum(c(1, diff(outlier_indices) > anomaly_window)))
    for (group in outlier_groups) {
      if (length(group) == 2) {
        idx1 <- group[1]; idx2 <- group[2]
        remove_idx <- if (abs(v[idx1]) > abs(v[idx2])) idx1 else idx2
        flagged_outliers <- c(flagged_outliers, remove_idx)
        v[remove_idx] <- NA
        isolated_outliers_count <- isolated_outliers_count + 1
        if (interpolate) {
          interpolated_outliers <- c(interpolated_outliers, remove_idx)
          window <- max(1, remove_idx - 1):min(length(v), remove_idx + 1)
          local_y <- v[window]
          v[remove_idx] <- zoo::na.approx(local_y, na.rm = FALSE)[which(window == remove_idx)]
        }
      } else {
        flagged_outliers <- c(flagged_outliers, group); block_indices <- c(block_indices, group)
        v[group] <- NA
        malfunction_blocks_count <- malfunction_blocks_count + length(group)
        duration <- difftime(max(times[group]), min(times[group]), units = "secs")
        malfunction_duration <- malfunction_duration + as.numeric(duration)
        if (!interpolate) {
          isolated_flags <- which(.findIsolated(v))
          if (length(isolated_flags) > 0) { flagged_outliers <- c(flagged_outliers, isolated_flags); v[isolated_flags] <- NA }
        }
      }
    }
    if (interpolate && malfunction_blocks_count > 0) {
      isolated_flags <- which(.findIsolated(v))
      if (length(isolated_flags) > 0) { flagged_outliers <- c(flagged_outliers, isolated_flags); v[isolated_flags] <- NA }
    }
    n_corr <- isolated_outliers_count + malfunction_blocks_count
    affected_secs <- malfunction_duration + (if (sampling_freq > 0) isolated_outliers_count / sampling_freq else 0)
    span_secs <- as.numeric(difftime(max(times), min(times), units = "secs"))
    pct_affected <- if (is.finite(span_secs) && span_secs > 0) 100 * affected_secs / span_secs else 0
  }

  severity <- if (malfunction_blocks_count > 0) "warning" else if (isolated_outliers_count > 0) "info" else NA_character_
  list(anomalies = anomalies, n_corr = n_corr, affected_secs = affected_secs, pct_affected = pct_affected,
       spikes = isolated_outliers_count, block_values = malfunction_blocks_count, severity = severity,
       sampling_freq = sampling_freq, flagged_outliers = sort(unique(flagged_outliers)),
       interpolated_outliers = sort(unique(interpolated_outliers)), block_indices = sort(unique(block_indices)),
       repaired = v)
}

#' One detailed-verbose line for a channel's outcome.
#' @keywords internal
#' @noRd
.qualityChannelLine <- function(ch, res) {
  if (!res$anomalies) return(sprintf("%s: clean", ch))
  sprintf("%s: %s anomal%s \u00b7 %s (%.1f%%)", ch, .formatNumber(res$n_corr),
          if (res$n_corr == 1) "y" else "ies", .formatDuration(res$affected_secs), res$pct_affected)
}

#' Display label for a channel (title-cased, separators to spaces), e.g. "depth" -> "Depth".
#' @keywords internal
#' @noRd
.sensorLabel <- function(ch) tools::toTitleCase(gsub("[._]+", " ", ch))

#' Split a sorted index vector into contiguous runs (each a run of consecutive indices).
#' @keywords internal
#' @noRd
.contiguousRuns <- function(idx) {
  if (!length(idx)) return(list())
  idx <- sort(unique(idx))
  brk <- c(0L, which(diff(idx) > 1L), length(idx))
  lapply(seq_len(length(brk) - 1L), function(k) idx[(brk[k] + 1L):brk[k + 1L]])
}

#' Empty quality-issues table with the canonical schema.
#' @keywords internal
#' @noRd
.emptyQualityIssues <- function() {
  data.frame(id = character(0), channel = character(0), severity = character(0),
             n_corrected = integer(0), spikes = integer(0), blocks = integer(0),
             pct_affected = numeric(0), message = character(0), stringsAsFactors = FALSE)
}

#' One quality-issues row for a flagged channel.
#' @keywords internal
#' @noRd
.qualityIssue <- function(id, ch, res, interpolate) {
  n_blocks <- length(.contiguousRuns(res$block_indices))
  parts <- c(if (res$spikes > 0) sprintf("%d isolated spike%s", res$spikes, if (res$spikes != 1) "s" else ""),
             if (n_blocks > 0) sprintf("%d malfunction/stall block%s", n_blocks, if (n_blocks != 1) "s" else ""))
  treat <- if (interpolate && res$spikes > 0 && n_blocks == 0) "interpolated" else "removed"
  data.frame(id = id, channel = ch, severity = res$severity,
             n_corrected = as.integer(res$n_corr), spikes = as.integer(res$spikes), blocks = as.integer(n_blocks),
             pct_affected = round(res$pct_affected, 3),
             message = sprintf("%s (%s, %.1f%% of the record; %s)", paste(parts, collapse = " + "),
                               .formatDuration(res$affected_secs), res$pct_affected, treat),
             stringsAsFactors = FALSE)
}

#' A short one-line verdict for a tag (overview cell / page title).
#' @keywords internal
#' @noRd
.qualityVerdict <- function(iss) {
  if (!nrow(iss)) return("clean")
  n_tot <- sum(iss$n_corrected)
  sprintf("%s anomal%s \u00b7 %.1f%% affected", .formatNumber(n_tot), if (n_tot == 1) "y" else "ies", max(iss$pct_affected))
}

#' One overview-table record for a tag: per-channel severity + count, plus the overall verdict.
#' @keywords internal
#' @noRd
.qualitySummaryRecord <- function(id, channels, iss) {
  per_ch <- lapply(channels, function(ch) {
    h <- iss[iss$channel == ch, , drop = FALSE]
    if (!nrow(h)) NULL else list(severity = h$severity[1], n = h$n_corrected[1], pct = h$pct_affected[1])
  })
  names(per_ch) <- channels
  worst <- if (any(iss$severity == "warning")) "warning" else if (nrow(iss)) "info" else "pass"
  list(id = id, per_ch = per_ch, worst = worst, verdict = .qualityVerdict(iss))
}

#' Build the small draw payload for one flagged channel: a decimated trace plus the removed-spike /
#' interpolated points (at their original values) and the malfunction/stall block spans (kept tiny so
#' the report can be drawn after the main loop). Built from the ORIGINAL channel (detection never mutates).
#' @keywords internal
#' @noRd
.qualityChannelPanel <- function(x, ch, datetime.col, res) {
  times <- as.numeric(x[[datetime.col]]); tz <- attr(x[[datetime.col]], "tzone"); v <- x[[ch]]
  tr <- .decimateForPlot(times, v, 3000L)
  interp_idx  <- res$interpolated_outliers
  block_idx   <- res$block_indices
  removed_idx <- setdiff(res$flagged_outliers, c(block_idx, interp_idx))   # isolated removed spikes only
  spans <- lapply(.contiguousRuns(block_idx), function(g) c(times[g[1]], times[g[length(g)]]))
  list(check = "quality", ch = ch, sev = res$severity, is_depth = grepl("depth", ch, ignore.case = TRUE),
       tz = if (is.null(tz) || !nzchar(tz)) "UTC" else tz, trace = tr,
       removed = list(t = times[removed_idx], y = v[removed_idx]),
       interp  = list(t = times[interp_idx],  y = v[interp_idx]),
       spans = spans, pct = res$pct_affected, n = res$n_corr,
       label = .sensorLabel(ch))
}


################################################################################
# Diagnostic report (internal) - overview page + per-flagged-tag pages #########
################################################################################

#' Overview summary page: one row per tag (worst-first), a per-channel cell showing the anomaly count
#' (amber = a stall/malfunction block, grey = spikes only, dot = clean), and a plain-language verdict.
#' Mirrors the checkSensorIntegrity run-summary page.
#' @keywords internal
#' @noRd
.drawQualitySummaryPage <- function(records, channels, stats, use_unicode = TRUE) {
  oldpar <- graphics::par(no.readonly = TRUE); on.exit(graphics::par(oldpar), add = TRUE)
  rankv <- c(warning = 0, info = 1, pass = 2)
  worst_pct <- function(r) { v <- vapply(r$per_ch, function(c) if (is.null(c)) 0 else c$pct, numeric(1)); if (length(v)) max(v) else 0 }
  if (length(records))
    records <- records[order(vapply(records, function(r) rankv[[r$worst]], numeric(1)), -vapply(records, worst_pct, numeric(1)))]
  ell <- if (use_unicode) "\u2026" else ".."; g_pass <- if (use_unicode) "\u00b7" else "."
  sevcol <- c(warning = "darkorange", info = "grey45", pass = "grey80")
  idcol  <- function(w) switch(w, warning = "darkorange", info = "grey30", "grey55")
  truncr <- function(s, n) if (nchar(s) > n) paste0(substr(s, 1, n - 1L), ell) else s

  cx_id <- 0.004; cx_verd <- 0.62
  cx_ch <- stats::setNames(seq(0.16, 0.55, length.out = length(channels)), channels)
  cex_h <- 0.62; cex_d <- 0.60
  rpp <- 40L; npg <- max(1L, ceiling(max(1L, length(records)) / rpp))
  for (pg in seq_len(npg)) {
    idx <- if (!length(records)) integer(0) else ((pg - 1L) * rpp + 1L):min(pg * rpp, length(records))
    graphics::par(mar = c(1.5, 0.6, 1, 0.6)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
    graphics::text(0, 0.985, "checkSensorQuality  -  run summary", adj = c(0, 1), font = 2, cex = 1.4)
    graphics::text(1, 0.985, format(Sys.time(), "%Y-%m-%d %H:%M"), adj = c(1, 1), cex = 0.75, col = "grey40")
    graphics::text(0, 0.93, sprintf("%d tags  |  %d with anomalies   %d with a stall/malfunction block   (%s)",
                                    stats$n, stats$n_flagged, stats$n_warn, if (stats$apply) "repaired" else "report only"),
                   adj = c(0, 1), cex = 0.95)
    graphics::text(0, 0.90, "sorted worst-first  -  cell = anomaly count (amber = a block/stall, grey = spikes only, dot = clean)",
                   adj = c(0, 1), cex = 0.72, col = "grey45")
    hcy <- 0.855
    graphics::rect(0, hcy - 0.014, 1, hcy + 0.014, col = "grey90", border = NA)
    graphics::text(cx_id, hcy, "Tag ID", adj = c(0, 0.5), font = 2, cex = cex_h, col = "grey15")
    for (ch in channels) graphics::text(cx_ch[[ch]], hcy, .sensorLabel(ch), adj = c(0.5, 0.5), font = 2, cex = cex_h * 0.9, col = "grey15")
    graphics::text(cx_verd, hcy, "Verdict", adj = c(0, 0.5), font = 2, cex = cex_h, col = "grey15")
    graphics::segments(0, hcy - 0.016, 1, hcy - 0.016, col = "grey55")
    row_top <- hcy - 0.030; dy <- 0.0195
    for (j in seq_along(idx)) {
      r <- records[[idx[j]]]; y <- row_top - dy * (j - 1L)
      if (j %% 2L == 0L) graphics::rect(0, y - dy / 2, 1, y + dy / 2, col = "grey96", border = NA)
      graphics::text(cx_id, y, truncr(r$id, 16), adj = c(0, 0.5), cex = cex_d, font = 2, col = idcol(r$worst))
      for (ch in channels) {
        cc <- r$per_ch[[ch]]
        if (is.null(cc)) graphics::text(cx_ch[[ch]], y, g_pass, adj = c(0.5, 0.5), cex = cex_d, col = sevcol[["pass"]])
        else graphics::text(cx_ch[[ch]], y, .formatNumber(cc$n), adj = c(0.5, 0.5), cex = cex_d, font = 2, col = sevcol[[cc$severity]])
      }
      graphics::text(cx_verd, y, r$verdict, adj = c(0, 0.5), cex = cex_d, col = idcol(r$worst), font = if (r$worst == "warning") 2 else 1)
    }
    if (npg > 1) graphics::mtext(sprintf("summary page %d / %d", pg, npg), side = 1, cex = 0.7, col = "grey50")
  }
  invisible(NULL)
}

#' Draw one flagged tag's page: its anomalous channels in a dynamic layout, each an annotated trace.
#' @keywords internal
#' @noRd
.plotQualityIndividual <- function(pd, use_unicode = TRUE) {
  oldpar <- graphics::par(no.readonly = TRUE); on.exit(graphics::par(oldpar), add = TRUE)
  n <- length(pd$panels); if (!n) return(invisible())
  lay <- .dynamicPanelLayout(n)
  graphics::layout(lay$mat, widths = lay$widths, heights = lay$heights)
  graphics::par(oma = c(0, 0, 2.4, 0), mgp = c(2.4, 0.7, 0))
  for (pan in pd$panels) .qpanelChannel(pan, use_unicode)
  graphics::mtext(sprintf("%s   -   sensor quality   [%s]", pd$id, pd$summary), outer = TRUE, font = 2, cex = 1)
  invisible(NULL)
}

#' One annotated-trace panel: the (decimated) sensor series with removed spikes (red), interpolated
#' values (cyan) and stall/malfunction blocks (shaded red spans), a status border and an interpretation note.
#' @keywords internal
#' @noRd
.qpanelChannel <- function(pan, uni) {
  st <- .isevStatus(pan$sev, uni)
  op <- graphics::par(mar = c(4.4, 4.4, 2.6, 1)); on.exit(graphics::par(op), add = TRUE)
  tr <- pan$trace; tp <- as.POSIXct(tr$x, origin = "1970-01-01", tz = pan$tz)
  ylim <- range(c(tr$y, pan$removed$y, pan$interp$y), na.rm = TRUE)
  if (!all(is.finite(ylim))) ylim <- c(0, 1)
  if (pan$is_depth) ylim <- rev(ylim)                                    # depth: surface at the top
  plot(tp, tr$y, type = "n", axes = FALSE, xlab = "", ylab = pan$label, ylim = ylim,
       main = paste0(st$pre, pan$label, sprintf(" - %s anomal%s (%.1f%% affected)",
                     .formatNumber(pan$n), if (pan$n == 1) "y" else "ies", pan$pct)),
       cex.main = 0.92, cex.lab = 0.9)
  usr <- graphics::par("usr")
  for (sp in pan$spans) graphics::rect(sp[1], usr[3], sp[2], usr[4], col = "#e4181820", border = NA)   # block spans
  graphics::lines(tp, tr$y, col = "grey30", lwd = 0.7)
  if (length(pan$removed$t)) graphics::points(as.POSIXct(pan$removed$t, origin = "1970-01-01", tz = pan$tz), pan$removed$y, pch = 16, col = "red3", cex = 0.8)
  if (length(pan$interp$t))  graphics::points(as.POSIXct(pan$interp$t,  origin = "1970-01-01", tz = pan$tz), pan$interp$y,  pch = 16, col = "cyan3", cex = 0.8)
  rng <- range(tp, na.rm = TRUE)
  graphics::axis.POSIXct(1, at = pretty(rng, 6),
                         format = if (as.numeric(diff(rng), units = "secs") > 86400) "%d/%b" else "%H:%M", cex.axis = 0.8)
  graphics::axis(2, cex.axis = 0.8, las = 1); graphics::box()
  leg <- character(0); lcol <- character(0)
  if (length(pan$spans))     { leg <- c(leg, "stall / malfunction block"); lcol <- c(lcol, "#e41818") }
  if (length(pan$removed$t)) { leg <- c(leg, "removed spike");             lcol <- c(lcol, "red3") }
  if (length(pan$interp$t))  { leg <- c(leg, "interpolated");              lcol <- c(lcol, "cyan3") }
  if (length(leg)) graphics::legend("topright", legend = leg, col = lcol, pch = 15, bty = "n", cex = 0.72)
  .panelHint("Sensor trace with detected anomalies: red = removed spikes, cyan = interpolated, shaded spans = sustained sensor failures (stalls / malfunction blocks).")
  if (!is.na(st$col)) graphics::box(col = st$col, lwd = 2)
}


################################################################################
# Isolated-value helper (internal) #############################################
################################################################################

#' Logical mask of "islands": non-NA runs surrounded by NA on both sides (leading/trailing NAs ignored).
#' @keywords internal
#' @noRd
.findIsolated <- function(vec) {
  rle_na <- rle(is.na(vec))
  island_positions <- which(!rle_na$values &
    c(FALSE, rle_na$values[-length(rle_na$values)]) &
    c(rle_na$values[-1], FALSE))
  is_island <- rep(FALSE, length(vec))
  if (length(island_positions)) {
    ends   <- cumsum(rle_na$lengths)
    starts <- ends - rle_na$lengths + 1L
    for (pos in island_positions) is_island[starts[pos]:ends[pos]] <- TRUE
  }
  is_island
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
