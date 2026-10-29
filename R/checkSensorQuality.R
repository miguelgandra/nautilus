#######################################################################################################
# Detect and repair signal-quality anomalies in sensor channels #######################################
#######################################################################################################

#' Detect and repair signal-quality anomalies in sensor channels
#'
#' @description
#' Screens selected sensor channels for abrupt changes and prolonged constant readings using
#' channel-specific thresholds. Detected anomalies are reported for review; optional repairs replace
#' affected values with interpolated estimates or missing values without deleting rows from the sensor
#' time series.
#'
#' The function complements [checkSensorIntegrity()], which assesses whether an entire sensor stream
#' is usable. Apply integrity checks first, then use this function to assess transient anomalies in the
#' surviving channels before deriving movement, dive or environmental metrics.
#'
#' @param data A tag dataset, a list of tag datasets, a data frame containing multiple deployments
#'   identified by \code{id.col}, or a character vector of \code{.rds} file paths. The output of
#'   [importTagData()] followed by time-series regularisation and sensor-integrity checks is
#'   recommended. File inputs are read one deployment at a time.
#' @param sensors A non-empty named list specifying the channels to screen, such as \code{depth} and
#'   \code{temp}. Each entry must be an [anomalyControl()] object or a named list of arguments to that
#'   constructor. Thresholds are expressed in the units of the corresponding numeric channel.
#'   Channels absent from a deployment are skipped.
#' @param apply Logical; whether to apply the proposed repairs (default \code{FALSE}). A report-only
#'   run preserves sensor values, although returned records may be sorted chronologically and receive
#'   a processing-history entry. With \code{TRUE}, affected values are replaced in the returned or saved
#'   datasets.
#' @param interpolate Logical; whether isolated spikes are linearly interpolated from adjacent samples
#'   (default \code{TRUE}) rather than left as \code{NA}. Sustained anomalies are never interpolated.
#'   This also determines the proposed treatment described in a report-only run.
#' @param id.col,datetime.col Names of the deployment-identifier and timestamp columns (defaults
#'   \code{"ID"} and \code{"datetime"}). Timestamps must be \code{POSIXct}, with a usable, approximately
#'   regular sampling interval.
#' @param plot Logical; whether to display the diagnostic report on the active graphics device (default
#'   \code{FALSE}). The report contains an overview and detail pages for deployments with sustained
#'   anomalies.
#' @param plot.file Optional path to a multi-page diagnostic PDF, independent of \code{plot}. The name
#'   must end in \code{.pdf} and the parent directory must exist. Default \code{NULL}.
#' @param return.data Logical; whether to include datasets in the returned \code{curated_data} element
#'   (default \code{TRUE}). With \code{FALSE}, that element contains saved paths when
#'   \code{apply = TRUE}, or \code{NULL} in a report-only run. Applying repairs with
#'   \code{return.data = FALSE} requires \code{output.dir}. The \code{issues} table is always returned.
#' @param output.dir An existing directory in which repaired datasets are saved as individual
#'   \code{<id>.rds} files. Supplying a directory triggers saving and requires \code{apply = TRUE};
#'   report-only copies cannot be written through this argument. Default \code{NULL}.
#' @param output.suffix Optional string appended to saved deployment identifiers before \code{.rds},
#'   to label a processing run or avoid overwriting earlier files. Default \code{NULL}.
#' @param compress Compression used when saving \code{.rds} files: \code{TRUE} (default, gzip),
#'   \code{FALSE}, or one of \code{"gzip"}, \code{"bzip2"} or \code{"xz"}. Only used when
#'   \code{output.dir} is specified. See [base::saveRDS()].
#' @param verbose How much detail to print: \code{0}/\code{"quiet"}, \code{1}/\code{"normal"}, or
#'   \code{2}/\code{"detailed"} (default).
#'
#' @details
#' ## Detection and repair
#'
#' Each channel is assessed independently using its [anomalyControl()] settings:
#'
#' \describe{
#'   \item{Abrupt changes}{Sample-to-sample changes exceeding the resolution gate and
#'     \code{rate.threshold} are grouped when separated by no more than \code{outlier.window} minutes,
#'     using the nominal sampling frequency. A group of exactly two flagged changes is treated as an
#'     isolated spike; the reading with the larger absolute value is selected for replacement.}
#'   \item{Constant readings}{Runs of identical, strictly positive values lasting at least
#'     \code{stall.threshold} minutes, as measured by nominal sample count, are flagged as stalls.
#'     Constant zero or negative readings are not detected by this criterion.}
#'   \item{Sustained anomalies}{Other groups of flagged changes and stall samples are replaced by
#'     \code{NA}. When sustained anomalies occur, finite runs bracketed by missing values are also
#'     removed; this cleanup has no maximum run-length limit and can include runs between pre-existing
#'     gaps.}
#' }
#'
#' With \code{apply = TRUE}, isolated spikes are replaced by \code{NA} and, if requested, interpolated
#' linearly within a three-sample neighbourhood. Interpolation uses sample position rather than elapsed
#' time and does not extrapolate at record boundaries. Sustained anomalies remain missing. Rows,
#' timestamps and unscreened channels are retained.
#'
#' ## Sampling and scientific limitations
#'
#' The current rate detector first requires
#' \code{abs(diff(value)) > sensor.resolution / diff(time)}, then compares the absolute rate with
#' \code{rate.threshold}, with time differences expressed in seconds. The initial gate compares a value
#' difference with a rate, making sensitivity
#' sampling-interval dependent: with a resolution of 0.5 units, the gate is 50 units at 100 Hz but
#' 0.5 units at 1 Hz. It should therefore be treated as a screen for gross anomalies, not as a
#' sampling-invariant noise or uncertainty model.
#'
#' Choose thresholds from instrument specifications and plausible changes in the measured quantity,
#' then validate them against representative traces. Slowly varying drift, subtle in-range faults and
#' biologically unusual but valid readings are not distinguished reliably by these heuristics.
#' Irregular sampling, duplicate timestamps and existing gaps require particular care; use
#' [regularizeTimeSeries()] where appropriate before screening. Sensor-accuracy fields in
#' [anomalyControl()] do not enter the detector.
#'
#' ## Reporting and processing history
#'
#' Review \code{issues} before enabling repairs. Its messages describe the proposed treatment even
#' when \code{apply = FALSE}; neither the name \code{curated_data} nor a message stating
#' \code{"interpolated"} confirms that sensor values were changed. Report-only runs add no processing
#' history. Caller-owned sensor data and metadata are never modified; returned datasets are independent
#' copies, ordered by timestamp, so later edits to them cannot change the input.
#'
#' With \code{apply = TRUE}, each checked, non-empty deployment receives a history entry recording the
#' screened channels, detector count, application mode and interpolation setting. The full channel controls
#' and a sample-level anomaly mask are not stored in that entry. No deployment-exclusion log is written.
#'
#' ## Diagnostic report
#'
#' The overview lists findings by deployment and channel, ordered by severity and affected duration.
#' Detail pages are drawn for deployments with stall or malfunction blocks, and include all anomalous
#' channels in those deployments. Deployments with isolated spikes only appear in the overview.
#' Traces and markers show the original readings and proposed treatment, not a verification of the
#' repaired signal.
#'
#' @return A list with two elements:
#' \describe{
#'   \item{\code{curated_data}}{With \code{return.data = TRUE}, a named list of deployment datasets.
#'     Sensor values are repaired only when \code{apply = TRUE}. With \code{return.data = FALSE}, a
#'     character vector of written \code{.rds} paths for a repair run, or \code{NULL} for a report-only
#'     run.}
#'   \item{\code{issues}}{A data frame with one row per flagged deployment and channel, or a typed
#'     zero-row table when no anomalies are found. Columns are \code{id}, \code{channel},
#'     \code{severity}, \code{n_corrected}, \code{spikes}, \code{blocks}, \code{pct_affected} and
#'     \code{message}.}
#' }
#'
#' In \code{issues}, \code{severity} is \code{"warning"} for sustained anomalies and \code{"info"}
#' for isolated spikes only; these labels do not themselves issue R warnings. \code{n_corrected} counts
#' selected spike samples and initially flagged block samples, including in report-only runs. It
#' excludes additional missing-gap cleanup and does not confirm successful interpolation.
#' \code{spikes} counts isolated-spike groups and \code{blocks} counts contiguous flagged block runs.
#' \code{pct_affected} estimates affected duration relative to the full recording span; it is not the
#' percentage of rows replaced.
#'
#' @seealso [checkSensorIntegrity()], [anomalyControl()], [regularizeTimeSeries()],
#'   [importTagData()], [processingHistory()].
#'
#' @examples
#' # Illustrative thresholds, not general biological limits
#' n <- 120L
#' recording <- data.frame(
#'   ID = "deployment_01",
#'   datetime = as.POSIXct("2023-01-01", tz = "UTC") + seq_len(n) - 1,
#'   depth = 10 + sin(seq_len(n) / 12))
#' recording$depth[60] <- 100
#' sensors <- list(depth = anomalyControl(
#'   rate.threshold = 7, sensor.resolution = 0.5))
#'
#' report <- checkSensorQuality(recording, sensors = sensors, verbose = "quiet")
#' report$issues
#' repaired <- checkSensorQuality(
#'   recording, sensors = sensors, apply = TRUE, verbose = "quiet")
#' processingHistory(repaired$curated_data[[1]])
#'
#' \dontrun{
#' # Run integrity checks before repairing surviving channels.
#' checked <- checkSensorIntegrity(regularized, apply = TRUE)$curated_data
#' quality <- checkSensorQuality(
#'   checked,
#'   sensors = list(
#'     depth = anomalyControl(rate.threshold = 7, sensor.resolution = 0.5),
#'     temp = anomalyControl(rate.threshold = 1, sensor.resolution = 0.05)),
#'   apply = TRUE,
#'   plot.file = "sensor_quality.pdf",
#'   output.dir = "./data interim/03_quality",
#'   return.data = FALSE)
#' files <- quality$curated_data
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
    x <- r$get(i, writable = apply || return.data)
    id <- tryCatch(as.character(unique(x[[id.col]])[1]), error = function(e) NA_character_)
    if (is.na(id)) id <- as.character(r$ids[i])
    .log_h2(lvl, sprintf("%s (%d/%d)", id, i, r$n))

    if (is.null(x) || nrow(x) == 0) {
      .log_skip(lvl, id, "  empty dataset ", cli::symbol$bullet, " skipped"); .log_gap(lvl)
      all_issues[[i]] <- .emptyQualityIssues()
      if (return.data) { results[[i]] <- x; names(results)[i] <- id }; next
    }
    # Report-only scans that discard the dataset may borrow it, but sorting still needs a private table.
    # Returned datasets and apply runs are already owned, including nested metadata and no-op results.
    copied <- apply || return.data || r$is_filepaths
    if (is.unsorted(x[[datetime.col]], na.rm = TRUE)) {
      if (!copied) x <- .workingTable(x)
      data.table::setorderv(x, cols = datetime.col)
    }

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
        if (apply) {
          x[, (ch) := res$repaired]
        }
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

    # Report-only checks return findings without changing processing history.
    if (apply) {
      for (a in names(keep_attrs)) attr(x, a) <- keep_attrs[[a]]
      meta <- .getMeta(x)
      if (!is.null(meta)) x <- .restoreMeta(x, .appendProcessing(meta, "checkSensorQuality",
                                          channels = paste(channels, collapse = ","),
                                          n_anomalies = sum(tag_issues$n_corrected), applied = apply, interpolate = interpolate))
    }

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
