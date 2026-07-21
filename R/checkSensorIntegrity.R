#######################################################################################################
# Validate the structural integrity of sensor channels ################################################
#######################################################################################################

#' Validate the structural integrity of sensor channels
#'
#' @description
#' Screens each deployment's sensor channels for structural, hardware/firmware-level faults - as opposed
#' to per-sample signal glitches, which are the job of \link{checkSensorQuality}. It answers a single
#' question per channel: *is this a trustworthy instance of the sensor it claims to be?* Faulty channels
#' are reported (and, with `apply = TRUE`, dropped) rather than repaired, since interpolating a corrupt
#' channel would only hide the fault.
#'
#' This is the data-driven complement to the metadata `exclude_sensors` catalogue handled at import:
#' the catalogue records *known*-bad channels, while this function *detects* them from the data, catching
#' undocumented cases. Run it after \link{regularizeTimeSeries} and before \link{checkSensorQuality} and
#' \link{checkTagMapping}, so corrupt channels are removed before any repair, orientation or downstream
#' analysis sees them.
#'
#' @details
#' The checks form two tiers. Two **error**-severity checks are trustworthy enough to drop a channel (with
#' `apply = TRUE`); the rest are **advisory** (`warning`/`info`) - reported but never dropped. Four checks
#' run by default (`duplication`, `dead`, and the advisory `accel.scale` and `saturation`); the remaining
#' advisory checks are opt-in via `checks`.
#' Every threshold below lives in \code{\link{integrityControl}}, and the reported `metric` is noted in
#' parentheses.
#'
#' \strong{duplication} (error). A gyroscope or magnetometer triplet whose three axes are each a near-exact
#' copy of the accelerometer (per-axis \code{|r| > dup.cor} on all three). Distinct sensor families cannot
#' track each other this closely unless one is literally a copy - the signature of a firmware bug that
#' duplicates the accelerometer into an absent channel. The channel carries no real information and should
#' be excluded. (metric: the minimum per-axis \code{|r|}.)
#'
#' \strong{dead} (error). A channel exactly constant over the whole deployment - the sensor never produced
#' a signal. Depth is exempt (a never-submerged tag legitimately reads a constant). (metric: 0.)
#'
#' \strong{saturation} (warning). A channel pinned at its exact minimum or maximum for a sustained fraction
#' of samples (\code{> saturation.frac}): the signal is clipping against the sensor's dynamic-range rail,
#' so the true values beyond it are lost. Typically a mis-set measurement range or an over-driven (e.g.
#' mis-scaled) channel. Like every check here it covers the accelerometer, gyroscope and magnetometer
#' only - depth and temperature are screened for physical-range faults by \code{\link{checkSensorQuality}}
#' instead, which is the right place for them (a depth channel resting at the surface is not clipping).
#' (metric: the clipped fraction.)
#'
#' \strong{mag.plausibility} (warning). The geomagnetic field magnitude \code{|B|} is orientation-invariant,
#' so after provisional hard-iron centring it should stay near-constant. A high robust coefficient of
#' variation (\code{> mag.cv}) points to a mis-scaled or soft-iron-distorted magnetometer, or a corrupt
#' channel (a magnetometer duplicated from the accelerometer tracks motion and fails here too). The check
#' abstains when the animal did not rotate through enough orientations to trust the centring. (metric: the
#' robust CV.)
#'
#' \strong{accel.scale} (warning). Checks whether the overall accelerometer magnitude is consistent with
#' gravity. The static (gravity) component is taken as a low-pass of the three axes, and its magnitude
#' should sit near 1 g across the record; the check flags \code{|median - 1| > accel.scale.tol}. It needs
#' no quiescent periods - the low-pass averages the animal's own motion away - so it applies equally to a
#' continuously active animal. It detects uniform scaling errors, for example acceleration left in m/s^2
#' and never converted to g, but may not detect errors affecting only individual axes: on a near-level
#' animal a gain error on a single axis barely moves the vector magnitude while still distorting roll.
#' Users working with unusual sensor configurations or unexpected results should inspect individual sensor
#' axes in addition to this summary metric. (metric: the median static magnitude, g.)
#'
#' \strong{gyro.bias} (info). Over a long record the animal's rotations average out, so each gyroscope axis
#' should have a near-zero median. A persistent offset that is both a large fraction of the rotational
#' scale (\code{> gyro.bias.frac}) and absolutely meaningful (\code{> gyro.bias.min}) is a sensor bias.
#' Gyroscope bias is not explicitly corrected. The default orientation algorithm in
#' \code{\link{processTagData}} (\code{"tilt_compass"}) does not use the gyroscope, so gyroscope bias does
#' not affect it. The \code{"madgwick"} algorithm does use the gyroscope; small biases are largely absorbed
#' by the accelerometer and magnetometer, but larger biases may affect orientation estimates and should be
#' inspected before relying on derived movement metrics. (metric: the largest \code{|median|}, rad/s.)
#'
#' \strong{paddle.contamination} (warning). A magnetic paddle wheel spins with the animal's speed and
#' induces a narrow-band peak in the magnetometer at its rotation frequency. The check scans the
#' magnetometer spectrum for such a peak, but only ABOVE the tail-beat fundamental and its harmonics (a
#' swimming animal's body oscillation modulates the magnetometer at ~0.2-0.5 Hz and would otherwise be
#' flagged as a paddle) and BELOW Nyquist (to reject aliasing); see \code{\link{integrityControl}}. It
#' fires only when the deployment is not already marked as carrying a paddle wheel - i.e. an UNDOCUMENTED
#' paddle effect - and a hit is worth confirming against the spectrum panel. (metric: the peak prominence,
#' peak / median band power.)
#'
#' \strong{dropout} (info). A channel missing (NA) for more than \code{dropout.frac} of the deployment, so
#' effectively absent. (metric: the missing fraction.)
#'
#' \strong{Diagnostic report.} When `plot` or `plot.file` is set, the function draws an overview page
#' tabling every tag and the checks it triggered (sorted worst-first), then one page per flagged
#' (warning/error) tag aggregating a diagnostic panel for each of that tag's findings - each panel with a
#' coloured status border and a short interpretation note. Tags with only `info`-level findings appear in
#' the overview but do not get their own page.
#'
#' @param data Sensor data in any of the pipeline forms: a list of `nautilus_tag` objects (one per
#'   individual), a single aggregated data.table/data.frame with an `id.col`, or a character vector of
#'   `.rds` file paths (loaded lazily). The output of \link{importTagData} (optionally regularized) is
#'   expected.
#' @param checks Character vector of integrity checks to run. Defaults to the two high-confidence,
#'   error-severity checks - `"duplication"` (a channel that is a near-exact copy of the accelerometer,
#'   the firmware accel-duplication signature) and `"dead"` (a channel constant over the whole
#'   deployment) - plus two advisory checks that earn their place by catching real hardware faults at no
#'   measurable cost: `"accel.scale"` (a static acceleration magnitude far from 1 g, which catches unit
#'   and scaling mistakes) and `"saturation"` (a channel pinned at a range limit, which catches a railed
#'   axis, a clipping gyroscope and sentinel values). Both are warning-severity, so `apply` never drops
#'   anything because of them. The remaining checks are opt-in and advisory (`"warning"`/`"info"`, never
#'   dropped by `apply`): `"mag.plausibility"` (an unstable magnetometer field magnitude), `"gyro.bias"`
#'   (a persistent gyroscope offset), `"paddle.contamination"` (a narrow-band magnetometer peak
#'   suggesting an undocumented paddle wheel) and `"dropout"` (a channel missing for most of the
#'   deployment).
#' @param control An \code{\link{integrityControl}} object (or a named list of its fields) bundling the
#'   per-check detection thresholds. Defaults to `integrityControl()` (values calibrated on real
#'   whale-shark deployments).
#' @param apply Logical. If `FALSE` (default), the function only reports findings (the data passes through
#'   unchanged). If `TRUE`, channels flagged at `"error"` severity are dropped from the returned data and
#'   recorded in the tag metadata (`meta$sensors$excluded`); the per-channel reasons remain in `issues`.
#' @param id.col,datetime.col Column names for the animal ID and datetime. Defaults `"ID"`/`"datetime"`.
#' @param plot Logical. If `TRUE`, draw the diagnostic report (overview page + one page per flagged
#'   deployment) to the active graphics device. Default `FALSE`.
#' @param plot.file Character. Path to a single multi-page PDF for the report. The parent directory must
#'   exist; the name must end in `.pdf`. `NULL` (default) writes none.
#' @param return.data Logical. Return the (possibly curated) data in memory (default `TRUE`). When `FALSE`
#'   and `apply = TRUE`, the function instead returns the paths of the `.rds` files it wrote, which feed
#'   directly into the next step's `data` argument -- so a large fleet can be processed without ever holding
#'   it all in memory; `return.data = FALSE` then requires an `output.dir`. A report-only run
#'   (`apply = FALSE`) still returns the `issues` report regardless.
#' @param output.dir Character. Directory in which to write one `<id>.rds` file per deployment. Providing a
#'   directory is what triggers saving; `NULL` (default) writes nothing. Requires `apply = TRUE` (a
#'   report-only run drops nothing, so writing is refused to avoid files that look curated but aren't). The
#'   directory must already exist.
#' @param output.suffix Character. Optional suffix appended to each saved file name (before `.rds`), e.g.
#'   to tag a processing run or avoid clashes. Only used when `output.dir` is set. Default `NULL`.
#' @param compress Compression for the saved `.rds` files (only used when `output.dir` is set): `TRUE`
#'   (default, gzip), `FALSE`, or one of `"gzip"`/`"bzip2"`/`"xz"`. See \code{\link[base]{saveRDS}}.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal", or `2`/"detailed" (default).
#'
#' @return A list with:
#' \itemize{
#'   \item \code{curated_data}: a named list of `data.table`s (one per deployment) when `return.data = TRUE`
#'     (with `apply = TRUE`, error-severity channels are dropped; otherwise unchanged); a character vector
#'     of the written file paths when `return.data = FALSE` (with `apply = TRUE`); or `NULL` for a
#'     report-only run.
#'   \item \code{issues}: a data.frame of findings (zero rows when everything passes), with columns
#'     \code{id}, \code{channel} (a sensor family such as `"gyro"`, or a single channel such as `"gz"`),
#'     \code{check}, \code{severity} (`"error"`/`"warning"`/`"info"`), \code{metric} (the test statistic)
#'     and \code{message}.
#' }
#'
#' @seealso \link{checkSensorQuality}, \link{qcDeploymentMetadata}, \link{checkTagMapping}.
#' @examples
#' \dontrun{
#' regularized <- regularizeTimeSeries(filtered)
#' # Report structural faults first; re-run with apply = TRUE to drop the error-flagged channels:
#' integrity <- checkSensorIntegrity(regularized, apply = FALSE)
#' integrity$issues                       # one row per flagged channel
#'
#' # Add the opt-in advisory checks (never dropped by apply, worth eyeballing once per fleet):
#' checkSensorIntegrity(regularized,
#'                      checks = c("duplication", "dead", "accel.scale", "saturation",
#'                                 "mag.plausibility", "gyro.bias"),
#'                      apply  = FALSE)
#' }
#' @export

checkSensorIntegrity <- function(data,
                                 checks = c("duplication", "dead", "accel.scale", "saturation"),
                                 control = integrityControl(),
                                 apply = FALSE,
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
  .assert_flag(apply, "apply"); .assert_flag(plot, "plot")
  .assert_flag(return.data, "return.data")
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  checks <- match.arg(checks, .integrityChecks(), several.ok = TRUE)
  control <- .as_control(control, integrityControl, "nautilus_integrity", "control")
  .assert_writable_file(plot.file, "plot.file", ext = "pdf")
  .assert_dir(output.dir, "output.dir")
  .assert_string(output.suffix, "output.suffix", null_ok = TRUE)
  .assert_compress(compress)
  # a report-only run (apply = FALSE) leaves the data untouched, so it need not be returned or saved -
  # you still get the `issues` report. An output sink is only required when apply = TRUE actually drops channels.
  if (apply) .assert_output(return.data, output.dir)
  # the mirror-image guard: writing in report-only mode would write byte-identical copies of the input
  # that merely LOOK curated - the single most misleading thing this function could do to disk.
  if (!is.null(output.dir) && !apply)
    .abort(c("Writing curated data (an {.arg output.dir}) needs {.code apply = TRUE}.",
             "i" = "A report-only run ({.code apply = FALSE}) does not drop the flagged channels, so the saved files would carry the same uncurated sensor data as the input - a file that looks curated but isn't.",
             "i" = "To write curated data, set {.code apply = TRUE}. To review first, keep {.code apply = FALSE}, drop {.code output.dir}, and inspect the returned {.field issues} table."))

  r <- .resolveInput(data, id.col = id.col)
  if (r$n == 0) return(list(curated_data = if (return.data) list() else NULL, issues = .emptyIntegrityIssues()))

  .log_header(lvl, "checkSensorIntegrity", "Validating sensor-channel integrity",
              bullets = sprintf("Input: %d tag%s", r$n, if (r$n != 1) "s" else ""),
              arrow = c(sprintf("Checks: %s", paste(checks, collapse = " \u00b7 ")),
                        sprintf("Mode: %s", if (apply) "report + drop flagged channels" else "report only")))

  # ONE multi-page landscape report (an overview page, then one page per flagged individual), drawn to
  # the active device and/or a PDF via the shared device helper. cairo (when available) renders the
  # status glyphs on the PDF; ASCII fallback otherwise.
  make_plots <- plot || !is.null(plot.file)

  results         <- if (return.data) vector("list", r$n) else NULL
  saved           <- vector("list", r$n)
  all_issues      <- vector("list", r$n)
  summary_records <- if (make_plots) vector("list", r$n) else NULL     # one overview-table row per tag
  payloads        <- list()                                            # a small draw payload per flagged tag
  n_flagged <- 0L

  for (i in seq_len(r$n)) {
    x <- r$get(i)
    if (!data.table::is.data.table(x)) x <- data.table::as.data.table(x)
    id <- tryCatch(as.character(unique(x[[id.col]])[1]), error = function(e) NA_character_)
    if (is.na(id)) id <- as.character(r$ids[i])
    fs <- tryCatch(.tagFs(x, datetime.col), error = function(e) NA_real_)

    # metadata (paddle-wheel context + provenance), then run the requested checks
    meta <- .getMeta(.ensureMeta(x))
    iss <- .runIntegrityChecks(x, fs, checks, control, paddle_wheel = if (!is.null(meta)) meta$tag$paddle_wheel else NA)
    if (nrow(iss)) iss$id <- id
    all_issues[[i]] <- iss

    # error-severity channels are the only ones recommended for exclusion (warnings/info are advisory)
    err_tokens <- unique(iss$channel[iss$severity == "error"])
    err_chans  <- if (length(err_tokens)) intersect(.expandSensorTokens(paste(err_tokens, collapse = ",")), names(x)) else character(0)
    has_findings <- nrow(iss) > 0
    if (has_findings) n_flagged <- n_flagged + 1L

    # ---- verbose (per deployment) ----
    present_fams <- .channelsToFamilies(intersect(.sensorChannels(), names(x)))
    .log_h2(lvl, sprintf("%s (%d/%d)", id, i, r$n))
    if (lvl >= 2L) {
      .log_detail(lvl, paste0("channels: ", paste(present_fams, collapse = " \u00b7 ")))
      for (ck in checks) .log_detail(lvl, .integrityCheckLine(ck, iss))
    }
    if (length(err_chans)) {
      .log_skip(lvl, id, "  flagged for exclusion: ", paste(.channelsToFamilies(err_chans), collapse = ", "),
                if (apply) "  (dropped)" else "  (review; apply = TRUE to drop)")
    } else if (has_findings) {
      nadv <- nrow(iss)
      .log_skip(lvl, id, "  ", nadv, " advisory finding", if (nadv != 1) "s", " (warning/info; not excluded)")
    } else {
      .log_ok(lvl, id, "  all channels pass")
    }
    .log_gap(lvl)

    # ---- diagnostics: a summary row for every tag; a small per-page payload for flagged tags (built
    # BEFORE any apply-drop, so the duplication / saturation panels still show the offending channel) ----
    if (make_plots) {
      summary_records[[i]] <- .integritySummaryRecord(id, present_fams, checks, iss)
      if (any(iss$severity %in% c("error", "warning")))
        payloads[[length(payloads) + 1L]] <- .integrityPayload(x, id, iss, fs, control)
    }

    # ---- apply (drop error-severity channels) + record provenance ----
    if (apply && length(err_chans)) {
      x[, (err_chans) := NULL]
      if (!is.null(meta)) meta$sensors$excluded <- unique(c(meta$sensors$excluded, err_chans))
    }
    if (!is.null(meta)) {
      meta <- .appendProcessing(meta, "checkSensorIntegrity", checks = paste(checks, collapse = ","),
                                n_issues = nrow(iss), excluded = if (apply) paste(err_chans, collapse = ",") else "")
      x <- .restoreMeta(x, meta)
    }

    saved[i] <- list(.saveOutput(x, id, output.dir = output.dir, output.suffix = output.suffix, compress = compress))
    if (return.data) { results[[i]] <- x; names(results)[i] <- id }
    rm(x)
  }

  issues <- do.call(rbind, all_issues)
  if (is.null(issues)) issues <- .emptyIntegrityIssues()
  rownames(issues) <- NULL

  # ---- draw the report: overview page (all tags), then one page per flagged (warning/error) tag ----
  if (make_plots) {
    run_stats <- list(n = r$n, n_flagged = n_flagged, checks = checks,
                      n_err = sum(issues$severity == "error"),
                      n_warn = length(unique(issues$id[issues$severity == "warning"])))
    draw <- function(to.file = FALSE, unicode = TRUE) {
      uni <- unicode                                             # Unicode only where the target device supports it
      .drawIntegritySummaryPage(Filter(Negate(is.null), summary_records), checks, run_stats, use_unicode = uni)
      for (p in payloads) .plotIntegrityIndividual(p, use_unicode = uni)
    }
    .renderToDevices(draw, plot = plot, plot.file = plot.file, width = 10, height = 7.5, cairo = TRUE)
  }

  if (lvl >= 1L) {
    .log_summary(lvl)
    n_err <- sum(issues$severity == "error")
    .log_done(lvl, n_flagged, " of ", r$n, " deployment", if (r$n != 1) "s", " flagged",
              if (n_err) sprintf(" (%d error-severity finding%s)", n_err, if (n_err != 1) "s" else "") else "")
    if (!is.null(plot.file)) .log_arrow(lvl, "plots: ", plot.file)
    .log_runtime(lvl, start.time)
  }

  curated <- if (return.data) results else if (apply) unlist(saved, use.names = FALSE) else NULL
  list(curated_data = curated, issues = issues)
}


################################################################################
# Check registry (internal) ####################################################
################################################################################

#' The integrity checks available: the two default error-severity checks, plus opt-in plausibility checks.
#' @keywords internal
#' @noRd
.integrityChecks <- function() c("duplication", "dead", "saturation", "mag.plausibility",
                                 "accel.scale", "gyro.bias", "paddle.contamination", "dropout")

#' The sensor families (channel triplets) used across the integrity checks.
#' @keywords internal
#' @noRd
.imuFamilies <- function() list(accel = c("ax", "ay", "az"), gyro = c("gx", "gy", "gz"), mag = c("mx", "my", "mz"))

#' Run the requested checks on one deployment; returns a (possibly empty) findings table. Each check is
#' `f(x, ctx) -> rows`, where `ctx` carries the families, sampling rate and paddle-wheel flag.
#' @keywords internal
#' @noRd
.runIntegrityChecks <- function(x, fs, checks, control, paddle_wheel = NA) {
  ctx <- list(fams = .imuFamilies(), fs = fs, paddle_wheel = paddle_wheel, control = control, psd = NULL)
  # the magnetometer Welch PSD is computed ONCE here and reused by the paddle check (and available to the
  # spectrum plot), rather than a full-series FFT recomputed per axis - the main speed + legibility fix.
  if ("paddle.contamination" %in% checks && is.finite(fs) && fs > 0 && !isTRUE(paddle_wheel)) {
    magcols <- intersect(ctx$fams$mag, names(x))
    ctx$psd <- stats::setNames(lapply(magcols, function(ch) tryCatch(.welchPSD(x[[ch]], fs), error = function(e) NULL)), magcols)
  }
  engine <- list(duplication = .icheckDuplication, dead = .icheckDead, saturation = .icheckSaturation,
                 mag.plausibility = .icheckMagPlausibility, accel.scale = .icheckAccelScale,
                 gyro.bias = .icheckGyroBias, paddle.contamination = .icheckPaddle, dropout = .icheckDropout)
  rows <- lapply(intersect(names(engine), checks), function(ck) engine[[ck]](x, ctx))
  out <- do.call(rbind, rows)
  if (is.null(out)) .emptyIntegrityIssues() else out
}

#' Duplication (error): a gyro/mag triplet that is a near-exact copy of the accelerometer (per-axis
#' |r| > 0.999 on all three axes - physically impossible between distinct sensor families unless a copy).
#' @keywords internal
#' @noRd
.icheckDuplication <- function(x, ctx) {
  fams <- ctx$fams; rows <- .emptyIntegrityIssues()
  if (!all(fams$accel %in% names(x))) return(rows)
  for (fam in c("gyro", "mag")) {
    cols <- fams[[fam]]
    if (!all(cols %in% names(x))) next
    corrs <- vapply(seq_len(3L), function(k) {
      a <- x[[fams$accel[k]]]; b <- x[[cols[k]]]
      ok <- is.finite(a) & is.finite(b)
      if (sum(ok) < 10L || stats::sd(a[ok]) == 0 || stats::sd(b[ok]) == 0) return(NA_real_)
      abs(stats::cor(a[ok], b[ok]))
    }, numeric(1))
    if (all(is.finite(corrs)) && all(corrs > ctx$control$dup.cor)) {
      rows <- rbind(rows, .integrityIssue(fam, "duplication", "error", round(min(corrs), 4),
                    sprintf("%s is a near-exact copy of the accelerometer (r %.4f) - likely firmware accel-duplication", fam, min(corrs))))
    }
  }
  rows
}

#' Dead/stuck (error): a channel exactly constant over the whole deployment (the sensor never produced a
#' real signal). Depth is excluded (a never-submerged tag legitimately reads a constant).
#' @keywords internal
#' @noRd
.icheckDead <- function(x, ctx) {
  rows <- .emptyIntegrityIssues()
  cand <- intersect(c(unlist(unname(ctx$fams)), "temp"), names(x))
  for (ch in cand) {
    v <- x[[ch]]; v <- v[is.finite(v)]
    if (length(v) < 10L) next
    if (diff(range(v)) < 1e-9) {
      rows <- rbind(rows, .integrityIssue(ch, "dead", "error", 0,
                    sprintf("%s is constant over the whole deployment (dead/stuck sensor)", ch)))
    }
  }
  rows
}

#' Saturation (warning): a channel pinned at its dynamic-range rail (its exact min or max) for a
#' sustained fraction of samples - clipping. Scoped to the IMU/mag channels via `ctx$fams`
#' (\code{.imuFamilies()}), which is what makes the two-sided min/max test safe: depth would fail it
#' spuriously, since a depth series is floored at the surface and `mean(v == min(v))` would measure time
#' spent at the surface (up to 70% of a real whale-shark record) rather than clipping. Physical-range
#' faults on depth and temperature are \code{\link{checkSensorQuality}}'s job, not this one's.
#' @keywords internal
#' @noRd
.icheckSaturation <- function(x, ctx) {
  rows <- .emptyIntegrityIssues()
  for (ch in intersect(unlist(unname(ctx$fams)), names(x))) {
    v <- x[[ch]]; v <- v[is.finite(v)]
    if (length(v) < 50L) next
    frac <- max(mean(v == max(v)), mean(v == min(v)))
    if (frac > ctx$control$saturation.frac)
      rows <- rbind(rows, .integrityIssue(ch, "saturation", "warning", round(frac, 4),
                    sprintf("%s is pinned at its range limit for %.1f%% of samples (clipping)", ch, 100 * frac)))
  }
  rows
}

#' Magnetometer plausibility (warning): after provisional hard-iron centering, the field magnitude |B|
#' should be roughly constant (the geomagnetic field is orientation-invariant). A high robust CV points
#' to mis-scaling, a soft-iron problem or a corrupt channel (a mag duplicated from the accelerometer
#' tracks motion and fails here too).
#' @keywords internal
#' @noRd
.icheckMagPlausibility <- function(x, ctx) {
  rows <- .emptyIntegrityIssues()
  m <- ctx$fams$mag
  if (!all(m %in% names(x))) return(rows)
  ok <- is.finite(x[[m[1]]]) & is.finite(x[[m[2]]]) & is.finite(x[[m[3]]])
  if (sum(ok) < 50L) return(rows)
  # abstain unless the animal rotated through enough orientations to trust the hard-iron centre; on
  # poor coverage the offset is biased and |B| would appear unstable even for a healthy magnetometer.
  hi <- tryCatch(.hardIronOffset(x[[m[1]]][ok], x[[m[2]]][ok], x[[m[3]]][ok]), error = function(e) NULL)
  if (is.null(hi) || !isTRUE(hi$coverage_ok)) return(rows)
  off <- hi$offset
  B <- sqrt((x[[m[1]]][ok] - off[1])^2 + (x[[m[2]]][ok] - off[2])^2 + (x[[m[3]]][ok] - off[3])^2)
  med <- stats::median(B)
  cv <- if (is.finite(med) && med > 0) stats::mad(B) / med else NA_real_
  if (is.finite(cv) && cv > ctx$control$mag.cv)
    rows <- rbind(rows, .integrityIssue("mag", "mag.plausibility", "warning", round(cv, 3),
                  sprintf("magnetometer field |B| is unstable (robust CV %.2f) - possible mis-scaling or a corrupt channel", cv)))
  rows
}

#' Accelerometer scale (warning): the static (gravity) acceleration magnitude should be ~1 g. A
#' systematic departure indicates a scaling or unit error (e.g. m/s^2 not converted to g).
#' @keywords internal
#' @noRd
.icheckAccelScale <- function(x, ctx) {
  rows <- .emptyIntegrityIssues()
  a <- ctx$fams$accel
  if (!all(a %in% names(x))) return(rows)
  win <- if (is.finite(ctx$fs) && ctx$fs > 0) max(1L, round(2 * ctx$fs)) else 21L
  sd <- .staticDynamicAccel(x[[a[1]]], x[[a[2]]], x[[a[3]]], win)
  g <- sqrt(sd$static$x^2 + sd$static$y^2 + sd$static$z^2)
  med <- stats::median(g, na.rm = TRUE)
  if (is.finite(med) && abs(med - 1) > ctx$control$accel.scale.tol)
    rows <- rbind(rows, .integrityIssue("accel", "accel.scale", "warning", round(med, 3),
                  sprintf("static acceleration magnitude is %.2f g (expected ~1 g) - possible scaling/unit error", med)))
  rows
}

#' Gyroscope bias (info): a persistent per-axis offset (the median should sit near zero once rotations
#' average out); flagged when it is a notable fraction of the rotational signal scale.
#' @keywords internal
#' @noRd
.icheckGyroBias <- function(x, ctx) {
  rows <- .emptyIntegrityIssues()
  g <- ctx$fams$gyro
  if (!all(g %in% names(x))) return(rows)
  meds <- vapply(g, function(c) stats::median(x[[c]], na.rm = TRUE), numeric(1))
  scale <- max(vapply(g, function(c) stats::mad(x[[c]], na.rm = TRUE), numeric(1)), na.rm = TRUE)
  if (is.finite(scale) && scale > 0) {
    rel <- max(abs(meds)) / scale
    if (is.finite(rel) && rel > ctx$control$gyro.bias.frac && max(abs(meds)) > ctx$control$gyro.bias.min) {
      k <- which.max(abs(meds))
      rows <- rbind(rows, .integrityIssue("gyro", "gyro.bias", "info", round(max(abs(meds)), 4),
                    sprintf("gyroscope has a persistent offset (%s bias %.3g, %.0f%% of the signal scale)", g[k], meds[k], 100 * rel)))
    }
  }
  rows
}

#' Paddle-wheel contamination (warning): a strong narrow-band peak in a MAGNETOMETER axis - the rotating
#' paddle magnet's signature. Axis-agnostic (this runs before axis mapping): all three mag axes are
#' scanned (from a precomputed Welch PSD) and the strongest candidate reported. The search band starts
#' ABOVE the tail-beat fundamental and its harmonics (the animal's own body oscillation modulates the
#' magnetometer at ~0.2-0.5 Hz and would otherwise be mistaken for a paddle) and stops below Nyquist (to
#' reject aliasing). Flagged only when the deployment is not already marked as carrying a paddle wheel.
#' @keywords internal
#' @noRd
.icheckPaddle <- function(x, ctx) {
  rows <- .emptyIntegrityIssues()
  cols <- intersect(ctx$fams$mag, names(x))
  if (!length(cols) || !is.finite(ctx$fs) || ctx$fs <= 0 || isTRUE(ctx$paddle_wheel)) return(rows)
  cc  <- ctx$control
  hi  <- cc$paddle.max.freq.frac * (ctx$fs / 2)              # ceiling below Nyquist (reject aliasing)
  best <- list(axis = NA_character_, freq = NA_real_, prom = 0, floor = NA_real_)
  for (ch in cols) {
    pg <- ctx$psd[[ch]]                                       # reuse the precomputed Welch PSD
    if (is.null(pg)) pg <- tryCatch(.welchPSD(x[[ch]], ctx$fs), error = function(e) NULL)
    if (is.null(pg)) next
    # tail-beat fundamental = the dominant low-frequency peak (the swimming signal); the paddle search
    # floor is set above its harmonics so the body oscillation is never counted as paddle contamination.
    lo   <- pg$freq >= 0.1 & pg$freq <= 2
    f_tb <- if (any(lo)) pg$freq[lo][which.max(pg$power[lo])] else NA_real_
    floor <- max(cc$paddle.min.freq, if (is.finite(f_tb)) cc$paddle.harmonic.guard * f_tb else 0)
    band <- pg$freq >= floor & pg$freq <= hi
    if (sum(band) < 5L) next
    p <- pg$power[band]; f <- pg$freq[band]
    med <- stats::median(p)
    if (!is.finite(med) || med <= 0) next
    prom <- max(p) / med
    if (prom > best$prom) best <- list(axis = ch, freq = f[which.max(p)], prom = prom, floor = floor)
  }
  if (is.finite(best$prom) && best$prom > cc$paddle.prominence)
    rows <- rbind(rows, .integrityIssue(best$axis, "paddle.contamination", "warning", round(best$prom, 1),
                  sprintf("narrow-band peak on %s at %.2f Hz (prominence %.0fx, above the %.1f Hz tail-beat band) - possible undocumented paddle-wheel contamination",
                          best$axis, best$freq, best$prom, best$floor)))
  rows
}

#' Dropout (info): a channel that is missing (NA) for most of the deployment, so effectively absent.
#' @keywords internal
#' @noRd
.icheckDropout <- function(x, ctx) {
  rows <- .emptyIntegrityIssues()
  if (nrow(x) < 10L) return(rows)
  for (ch in intersect(c(unlist(unname(ctx$fams)), "depth", "temp"), names(x))) {
    frac <- mean(is.na(x[[ch]]))
    if (frac > ctx$control$dropout.frac)
      rows <- rbind(rows, .integrityIssue(ch, "dropout", "info", round(frac, 3),
                    sprintf("%s is missing (NA) for %.0f%% of the deployment", ch, 100 * frac)))
  }
  rows
}


################################################################################
# Findings helpers + verbose + plot (internal) #################################
################################################################################

#' Empty findings table with the canonical schema.
#' @keywords internal
#' @noRd
.emptyIntegrityIssues <- function() {
  data.frame(id = character(0), channel = character(0), check = character(0),
             severity = character(0), metric = numeric(0), message = character(0), stringsAsFactors = FALSE)
}

#' One findings row (the `id` is filled in by the driver).
#' @keywords internal
#' @noRd
.integrityIssue <- function(channel, check, severity, metric, message) {
  data.frame(id = NA_character_, channel = channel, check = check, severity = severity,
             metric = as.numeric(metric), message = message, stringsAsFactors = FALSE)
}

#' One detailed-verbose line per check, summarising its findings (or "none").
#' @keywords internal
#' @noRd
.integrityCheckLine <- function(check, iss) {
  hit <- iss[iss$check == check, , drop = FALSE]
  label <- switch(check, mag.plausibility = "mag plausibility", accel.scale = "accel scale",
                  gyro.bias = "gyro bias", paddle.contamination = "paddle", check)
  if (!nrow(hit)) return(sprintf("%s: none", label))
  switch(check,
    duplication      = paste0("duplication: ", paste(sprintf("%s \u2261 accel (r %.4f)", hit$channel, hit$metric), collapse = " \u00b7 ")),
    dead             = paste0("dead: ", paste(.channelsToFamilies(hit$channel), collapse = ", ")),
    saturation       = paste0("saturation: ", paste(sprintf("%s (%.1f%%)", hit$channel, 100 * hit$metric), collapse = ", ")),
    mag.plausibility = sprintf("mag plausibility: |B| CV %.2f", hit$metric[1]),
    accel.scale      = sprintf("accel scale: %.2f g", hit$metric[1]),
    gyro.bias        = sprintf("gyro bias: %.3g (%s)", hit$metric[1], hit$channel[1]),
    paddle.contamination = sprintf("paddle: %s peak (prominence %.0fx)", hit$channel[1], hit$metric[1]),
    dropout          = paste0("dropout: ", paste(sprintf("%s (%.0f%%)", hit$channel, 100 * hit$metric), collapse = ", ")),
    sprintf("%s: %d finding%s", check, nrow(hit), if (nrow(hit) != 1) "s" else ""))
}

#' Per-check display metadata: a short overview-table label, a panel title, and an interpretation note
#' (what is shown / why the check exists / how to read an abnormal pattern).
#' @keywords internal
#' @noRd
.integrityCheckMeta <- function() list(
  duplication = list(label = "Dup", title = "Cross-channel correlation",
    hint = "Each cell is |r| between two channels; a red off-diagonal block near 1 means a channel is a copy of the accelerometer (firmware duplication)."),
  dead = list(label = "Dead", title = "Constant (dead) channel",
    hint = "A dead sensor never varies - the trace is a flat line; a working channel fluctuates with the animal's motion."),
  saturation = list(label = "Sat", title = "Range clipping",
    hint = "The channel is pinned at its dynamic-range rail (dashed) for long stretches; values beyond the rail are lost (clipping)."),
  mag.plausibility = list(label = "Mag", title = "Magnetometer field |B|",
    hint = "The geomagnetic field is orientation-invariant, so |B| should stay near-constant (dashed median); a drifting or spiky trace flags mis-scaling or a corrupt channel."),
  accel.scale = list(label = "aScl", title = "Static acceleration magnitude",
    hint = "Static (gravity) acceleration should centre on 1 g (green); a shifted distribution indicates a scaling or unit error."),
  gyro.bias = list(label = "gBias", title = "Gyroscope offset",
    hint = "Rotations average out, so each gyro axis should sit near 0 (dotted); a persistent offset (solid line off zero) is a sensor bias."),
  paddle.contamination = list(label = "Pad", title = "Magnetometer spectrum",
    hint = "A sharp peak in the amber paddle band (above the grey tail-beat band, below Nyquist) can be an undocumented paddle wheel; the tail-beat band is expected and ignored."),
  dropout = list(label = "Drop", title = "Channel availability",
    hint = "A channel missing (NA) for most of the deployment is effectively absent; bars past the dashed line are unusable channels."))

#' Map a finding severity to a panel border colour + title prefix (extends .panelStatus with an info tier).
#' @keywords internal
#' @noRd
.isevStatus <- function(sev, use_unicode) {
  switch(sev, error = .panelStatus("fail", use_unicode), warning = .panelStatus("warn", use_unicode),
         list(col = "grey55", pre = "[i] "))
}

#' One overview-table row for a tag: the worst severity that fired per check, plus an overall verdict.
#' @keywords internal
#' @noRd
.integritySummaryRecord <- function(id, families, checks, iss) {
  sevrank <- c(info = 1L, warning = 2L, error = 3L)
  per_check <- lapply(checks, function(ck) {
    h <- iss[iss$check == ck, , drop = FALSE]
    if (!nrow(h)) NA_character_ else h$severity[which.max(sevrank[h$severity])]
  })
  names(per_check) <- checks
  worst <- if (any(iss$severity == "error")) "error" else if (any(iss$severity == "warning")) "warning" else
           if (any(iss$severity == "info")) "info" else "pass"
  verdict <- switch(worst,
    error   = sprintf("exclude: %s", paste(.channelsToFamilies(unique(iss$channel[iss$severity == "error"])), collapse = ", ")),
    warning = { nw <- sum(iss$severity == "warning"); sprintf("%d warning%s", nw, if (nw != 1) "s" else "") },
    info    = { ni <- sum(iss$severity == "info");    sprintf("%d info", ni) },
    "pass")
  list(id = id, families = families, per_check = per_check, worst = worst, verdict = verdict, n = nrow(iss))
}

#' Build the small per-page draw payload for one flagged tag: one panel spec (in check order) per firing
#' check, each carrying only the decimated series / spectrum / matrix it needs (kept tiny so the whole
#' report can be drawn after the main loop without holding any full-resolution data).
#' @keywords internal
#' @noRd
.integrityPayload <- function(x, id, iss, fs, control) {
  fams <- .imuFamilies(); idx <- seq_len(nrow(x))
  order_ck <- c("duplication", "dead", "saturation", "mag.plausibility", "accel.scale", "gyro.bias", "paddle.contamination", "dropout")
  panels <- list()
  for (ck in intersect(order_ck, unique(iss$check))) {
    h <- iss[iss$check == ck, , drop = FALSE]; sev <- h$severity[1]
    pan <- switch(ck,
      duplication = {
        ch <- intersect(unlist(unname(fams)), names(x))
        M <- suppressWarnings(stats::cor(as.matrix(as.data.frame(lapply(ch, function(c) x[[c]]))), use = "pairwise.complete.obs"))
        M[!is.finite(M)] <- 0
        list(check = ck, sev = sev, chans = ch, corr = M)
      },
      dead = list(check = ck, sev = sev, chans = h$channel,
                  traces = lapply(h$channel, function(c) .decimateForPlot(idx, x[[c]], 3000L))),
      saturation = list(check = ck, sev = sev, chans = h$channel, frac = h$metric,
                        traces = lapply(h$channel, function(c) .decimateForPlot(idx, x[[c]], 3000L)),
                        rails  = lapply(h$channel, function(c) range(x[[c]], na.rm = TRUE))),
      mag.plausibility = {
        m <- fams$mag; ok <- is.finite(x[[m[1]]]) & is.finite(x[[m[2]]]) & is.finite(x[[m[3]]])
        off <- tryCatch(.hardIronOffset(x[[m[1]]][ok], x[[m[2]]][ok], x[[m[3]]][ok])$offset, error = function(e) c(0, 0, 0))
        B <- sqrt((x[[m[1]]] - off[1])^2 + (x[[m[2]]] - off[2])^2 + (x[[m[3]]] - off[3])^2)
        list(check = ck, sev = sev, B = .decimateForPlot(idx, B, 3000L), med = stats::median(B, na.rm = TRUE), cv = h$metric[1])
      },
      accel.scale = {
        a <- fams$accel; win <- if (is.finite(fs) && fs > 0) max(1L, round(2 * fs)) else 21L
        sdc <- tryCatch(.staticDynamicAccel(x[[a[1]]], x[[a[2]]], x[[a[3]]], win), error = function(e) NULL)
        g <- if (!is.null(sdc)) sqrt(sdc$static$x^2 + sdc$static$y^2 + sdc$static$z^2) else numeric(0)
        # the panel is a 60-bin histogram, so a systematic subsample carries the same shape. Keeping the
        # full-resolution vector would pin ~90 MB per flagged tag for the rest of the fleet loop (payloads
        # are only drawn once the loop ends), and accel.scale is now a DEFAULT check.
        g <- g[is.finite(g)]
        if (length(g) > 50000L) g <- g[seq.int(1L, length(g), length.out = 50000L)]
        list(check = ck, sev = sev, g = g, med = h$metric[1])
      },
      gyro.bias = {
        g <- fams$gyro
        list(check = ck, sev = sev, axes = g, meds = vapply(g, function(c) stats::median(x[[c]], na.rm = TRUE), numeric(1)),
             traces = lapply(g, function(c) .decimateForPlot(idx, x[[c]], 3000L)))
      },
      paddle.contamination = {
        ax <- h$channel[1]; pg <- tryCatch(.welchPSD(x[[ax]], fs), error = function(e) NULL)
        floor <- NA_real_; hi <- NA_real_; pk_f <- NA_real_; pk_p <- NA_real_
        if (!is.null(pg)) {
          lo <- pg$freq >= 0.1 & pg$freq <= 2; f_tb <- if (any(lo)) pg$freq[lo][which.max(pg$power[lo])] else NA_real_
          floor <- max(control$paddle.min.freq, if (is.finite(f_tb)) control$paddle.harmonic.guard * f_tb else 0)
          hi <- control$paddle.max.freq.frac * (fs / 2)
          band <- pg$freq >= floor & pg$freq <= hi
          if (any(band)) { pk <- which(band)[which.max(pg$power[band])]; pk_f <- pg$freq[pk]; pk_p <- pg$power[pk] }
        }
        list(check = ck, sev = sev, axis = ax, psd = pg, floor = floor, hi = hi,
             peak_freq = pk_f, peak_pow = pk_p, prom = h$metric[1])
      },
      dropout = list(check = ck, sev = sev, chans = h$channel, fracs = h$metric))
    panels[[length(panels) + 1L]] <- pan
  }
  worst <- if (any(iss$severity == "error")) sprintf("exclude: %s", paste(.channelsToFamilies(unique(iss$channel[iss$severity == "error"])), collapse = ", "))
           else { nw <- sum(iss$severity == "warning"); sprintf("%d warning%s%s", nw, if (nw != 1) "s" else "",
                  { ni <- sum(iss$severity == "info"); if (ni) sprintf(", %d info", ni) else "" }) }
  list(id = id, summary = worst, panels = panels)
}

#' Dynamic panel layout for `n` diagnostic panels on one landscape page: 1 -> full page, 2 -> stacked,
#' odd counts get a bottom panel spanning the two columns (no empty cells), otherwise a 2-column grid.
#' @keywords internal
#' @noRd
.dynamicPanelLayout <- function(n) {
  if (n <= 1L) return(list(mat = matrix(1L), widths = 1, heights = 1))
  if (n == 2L) return(list(mat = matrix(c(1L, 2L), 2L, 1L), widths = 1, heights = c(1, 1)))
  ncol <- 2L; nrow <- as.integer(ceiling(n / ncol))
  mat <- matrix(0L, nrow, ncol); k <- 1L
  for (rr in seq_len(nrow)) for (cc in seq_len(ncol)) if (k <= n) { mat[rr, cc] <- k; k <- k + 1L }
  if (n %% 2L == 1L) mat[nrow, ] <- n                         # odd -> last panel spans the full row
  list(mat = mat, widths = rep(1, ncol), heights = rep(1, nrow))
}

#' Draw one flagged tag's page: the firing checks' panels in a dynamic layout, each with a coloured
#' status border and an interpretation note, under a title carrying the overall verdict.
#' @keywords internal
#' @noRd
.plotIntegrityIndividual <- function(pd, use_unicode = TRUE) {
  oldpar <- graphics::par(no.readonly = TRUE); on.exit(graphics::par(oldpar), add = TRUE)
  n <- length(pd$panels); if (!n) return(invisible())
  lay <- .dynamicPanelLayout(n)
  graphics::layout(lay$mat, widths = lay$widths, heights = lay$heights)
  graphics::par(oma = c(0, 0, 2.4, 0), mgp = c(2.2, 0.7, 0))
  for (pan in pd$panels) .ipanelDraw(pan, use_unicode)
  graphics::mtext(sprintf("%s   -   sensor integrity   [%s]", pd$id, pd$summary), outer = TRUE, font = 2, cex = 1)
  invisible(NULL)
}

#' Dispatch one panel to its drawer by check type.
#' @keywords internal
#' @noRd
.ipanelDraw <- function(pan, uni) switch(pan$check,
  duplication = .ipanelCorr(pan, uni), dead = .ipanelDead(pan, uni), saturation = .ipanelSaturation(pan, uni),
  mag.plausibility = .ipanelMagB(pan, uni), accel.scale = .ipanelAccelScale(pan, uni),
  gyro.bias = .ipanelGyroBias(pan, uni), paddle.contamination = .ipanelSpectrum(pan, uni),
  dropout = .ipanelDropout(pan, uni))

#' A vertical colour-scale bar drawn in the right margin of the current plot (fixes the "no scale" gap).
#' @keywords internal
#' @noRd
.drawImageColorbar <- function(pal, zlim, label) {
  usr <- graphics::par("usr"); np <- length(pal); dx <- usr[2] - usr[1]; dy <- usr[4] - usr[3]
  x0 <- usr[2] + dx * 0.07; x1 <- x0 + dx * 0.06; ys <- seq(usr[3], usr[4], length.out = np + 1)
  graphics::rect(x0, ys[-(np + 1)], x1, ys[-1], col = pal, border = NA, xpd = NA)
  graphics::rect(x0, usr[3], x1, usr[4], xpd = NA)
  yat <- seq(usr[3], usr[4], length.out = 3)
  graphics::text(x1 + dx * 0.02, yat, sprintf("%.0f", seq(zlim[1], zlim[2], length.out = 3)), adj = 0, cex = 0.6, xpd = NA)
  graphics::text((x0 + x1) / 2, usr[4] + dy * 0.05, label, cex = 0.7, font = 2, xpd = NA)
}

# ---- per-check panel drawers (each: figure + interpretation note + coloured status border) -----------

#' @keywords internal
#' @noRd
.ipanelCorr <- function(pan, uni) {
  meta <- .integrityCheckMeta()$duplication; st <- .isevStatus(pan$sev, uni)
  cm <- pan$corr; ch <- pan$chans; n <- length(ch)
  # bottom margin matches the other panels (4.4 lines) so the interpretation note at line 3.15 is not
  # clipped by the figure edge; pty = "s" keeps the correlation matrix square
  op <- graphics::par(mar = c(4.4, 3.2, 2.6, 5.4), pty = "s"); on.exit(graphics::par(op), add = TRUE)
  pal <- grDevices::colorRampPalette(c("#2166ac", "white", "#b2182b"))(101)
  graphics::image(seq_len(n), seq_len(n), t(cm[n:1, , drop = FALSE]), zlim = c(-1, 1), col = pal, axes = FALSE,
                  xlab = "", ylab = "", main = paste0(st$pre, meta$title), cex.main = 0.95)
  graphics::axis(1, at = seq_len(n), labels = ch, las = 2, cex.axis = 0.65)
  graphics::axis(2, at = seq_len(n), labels = rev(ch), las = 2, cex.axis = 0.65)
  for (i in seq_len(n)) for (j in seq_len(n)) {
    v <- cm[n - j + 1L, i]
    graphics::text(i, j, sprintf("%.2f", v), cex = 0.5, col = if (abs(v) > 0.6) "white" else "grey25")
  }
  graphics::box(); .drawImageColorbar(pal, c(-1, 1), "r"); .panelHint(meta$hint)
  if (!is.na(st$col)) graphics::box(col = st$col, lwd = 2)
}

#' @keywords internal
#' @noRd
.ipanelDead <- function(pan, uni) {
  meta <- .integrityCheckMeta()$dead; st <- .isevStatus(pan$sev, uni)
  op <- graphics::par(mar = c(4.4, 4, 2.6, 1)); on.exit(graphics::par(op), add = TRUE)
  tr <- pan$traces[[1]]
  graphics::plot(tr$x, tr$y, type = "l", col = "red3", xlab = "Sample", ylab = pan$chans[1],
                 main = paste0(st$pre, meta$title, " - ", paste(pan$chans, collapse = ", ")),
                 cex.main = 0.95, cex.axis = 0.8, cex.lab = 0.9, las = 1)
  .panelHint(meta$hint); if (!is.na(st$col)) graphics::box(col = st$col, lwd = 2)
}

#' @keywords internal
#' @noRd
.ipanelSaturation <- function(pan, uni) {
  meta <- .integrityCheckMeta()$saturation; st <- .isevStatus(pan$sev, uni)
  op <- graphics::par(mar = c(4.4, 4, 2.6, 1)); on.exit(graphics::par(op), add = TRUE)
  tr <- pan$traces[[1]]; rail <- pan$rails[[1]]
  graphics::plot(tr$x, tr$y, type = "l", col = "grey30", xlab = "Sample", ylab = pan$chans[1],
                 main = paste0(st$pre, meta$title, " - ", pan$chans[1], sprintf(" (%.1f%% clipped)", 100 * pan$frac[1])),
                 cex.main = 0.95, cex.axis = 0.8, cex.lab = 0.9, las = 1)
  graphics::abline(h = rail, col = "red3", lty = 2, lwd = 1.4)
  .panelHint(meta$hint); if (!is.na(st$col)) graphics::box(col = st$col, lwd = 2)
}

#' @keywords internal
#' @noRd
.ipanelMagB <- function(pan, uni) {
  meta <- .integrityCheckMeta()$mag.plausibility; st <- .isevStatus(pan$sev, uni)
  op <- graphics::par(mar = c(4.4, 4, 2.6, 1)); on.exit(graphics::par(op), add = TRUE)
  graphics::plot(pan$B$x, pan$B$y, type = "l", col = "grey30", xlab = "Sample", ylab = "|B| (hard-iron centred)",
                 main = paste0(st$pre, meta$title, sprintf(" (robust CV %.2f)", pan$cv)),
                 cex.main = 0.95, cex.axis = 0.8, cex.lab = 0.9, las = 1)
  graphics::abline(h = pan$med, col = "red3", lty = 2, lwd = 1.5)
  graphics::legend("topright", legend = "median", col = "red3", lty = 2, bty = "n", cex = 0.75)
  .panelHint(meta$hint); if (!is.na(st$col)) graphics::box(col = st$col, lwd = 2)
}

#' @keywords internal
#' @noRd
.ipanelAccelScale <- function(pan, uni) {
  meta <- .integrityCheckMeta()$accel.scale; st <- .isevStatus(pan$sev, uni)
  op <- graphics::par(mar = c(4.4, 4, 2.6, 1)); on.exit(graphics::par(op), add = TRUE)
  if (!length(pan$g)) { graphics::plot.new(); graphics::title(paste0(st$pre, meta$title), cex.main = 0.95); return(invisible()) }
  graphics::hist(pan$g, breaks = 60, col = "grey85", border = "grey60", xlab = "Static |a| (g)", ylab = "Count",
                 main = paste0(st$pre, meta$title, sprintf(" (median %.2f g)", pan$med)),
                 cex.main = 0.95, cex.axis = 0.8, cex.lab = 0.9, las = 1)
  graphics::abline(v = 1, col = "green4", lty = 2, lwd = 1.6)
  graphics::abline(v = pan$med, col = "red3", lty = 1, lwd = 1.6)
  graphics::legend("topright", legend = c("1 g (expected)", "median"), col = c("green4", "red3"),
                   lty = c(2, 1), lwd = 1.6, bty = "n", cex = 0.7)
  .panelHint(meta$hint); if (!is.na(st$col)) graphics::box(col = st$col, lwd = 2)
}

#' @keywords internal
#' @noRd
.ipanelGyroBias <- function(pan, uni) {
  meta <- .integrityCheckMeta()$gyro.bias; st <- .isevStatus(pan$sev, uni)
  op <- graphics::par(mar = c(4.4, 4, 2.6, 1)); on.exit(graphics::par(op), add = TRUE)
  cols <- c("#cc3333", "#3399cc", "#555555"); tr <- pan$traces; meds <- pan$meds
  yl <- range(unlist(lapply(tr, function(t) range(t$y, na.rm = TRUE))), na.rm = TRUE)
  graphics::plot(tr[[1]]$x, tr[[1]]$y, type = "l", col = paste0(cols[1], "80"), ylim = yl, xlab = "Sample",
                 ylab = "Angular velocity (rad/s)", main = paste0(st$pre, meta$title),
                 cex.main = 0.95, cex.axis = 0.8, cex.lab = 0.9, las = 1)
  for (k in 2:3) graphics::lines(tr[[k]]$x, tr[[k]]$y, col = paste0(cols[k], "80"))
  graphics::abline(h = 0, col = "black", lty = 3)
  for (k in 1:3) graphics::abline(h = meds[k], col = cols[k], lwd = 1.5)
  graphics::legend("topright", legend = sprintf("%s median %+.3f", pan$axes, meds), col = cols, lwd = 1.5, bty = "n", cex = 0.7)
  .panelHint(meta$hint); if (!is.na(st$col)) graphics::box(col = st$col, lwd = 2)
}

#' @keywords internal
#' @noRd
.ipanelSpectrum <- function(pan, uni) {
  meta <- .integrityCheckMeta()$paddle.contamination; st <- .isevStatus(pan$sev, uni)
  op <- graphics::par(mar = c(4.4, 4, 2.6, 1)); on.exit(graphics::par(op), add = TRUE)
  pg <- pan$psd
  if (is.null(pg)) { graphics::plot.new(); graphics::title(paste0(st$pre, meta$title, ": unavailable"), cex.main = 0.95); return(invisible()) }
  logp <- 10 * log10(pmax(pg$power, .Machine$double.eps))
  graphics::plot(pg$freq, logp, type = "n", xlab = "Frequency (Hz)", ylab = "Power (dB)",
                 main = paste0(st$pre, meta$title, " - ", pan$axis, sprintf(" (peak %.1f Hz, %.0fx)", pan$peak_freq, pan$prom)),
                 cex.main = 0.9, cex.axis = 0.8, cex.lab = 0.9, las = 1)
  yl <- graphics::par("usr")[3:4]
  if (is.finite(pan$floor)) graphics::rect(0, yl[1], pan$floor, yl[2], col = "#00000010", border = NA)   # tail-beat band
  if (is.finite(pan$floor) && is.finite(pan$hi)) graphics::rect(pan$floor, yl[1], pan$hi, yl[2], col = "#f0a80020", border = NA)  # paddle band
  graphics::lines(pg$freq, logp, col = "grey40")
  if (is.finite(pan$peak_freq)) graphics::points(pan$peak_freq, 10 * log10(pan$peak_pow), col = "red3", pch = 16, cex = 1.2)
  graphics::legend("topright", legend = c("tail-beat band (ignored)", "paddle search band"),
                   fill = c("#0000001f", "#f0a80055"), border = NA, bty = "n", cex = 0.68)
  .panelHint(meta$hint); if (!is.na(st$col)) graphics::box(col = st$col, lwd = 2)
}

#' @keywords internal
#' @noRd
.ipanelDropout <- function(pan, uni) {
  meta <- .integrityCheckMeta()$dropout; st <- .isevStatus(pan$sev, uni)
  op <- graphics::par(mar = c(4.4, 5.5, 2.6, 1)); on.exit(graphics::par(op), add = TRUE)
  graphics::barplot(rev(100 * pan$fracs), horiz = TRUE, names.arg = rev(pan$chans), las = 1, col = "grey70",
                    border = "grey50", xlab = "% missing (NA)", xlim = c(0, 100),
                    main = paste0(st$pre, meta$title), cex.main = 0.95, cex.axis = 0.8, cex.names = 0.8)
  graphics::abline(v = 50, col = "red3", lty = 2)
  .panelHint(meta$hint); if (!is.na(st$col)) graphics::box(col = st$col, lwd = 2)
}

#' Draw the overview summary page: one row per tag (worst-first), a per-check severity-glyph grid, and a
#' plain-language verdict. Mirrors the checkTagMapping run-summary page.
#' @keywords internal
#' @noRd
.drawIntegritySummaryPage <- function(records, checks, stats, use_unicode = TRUE) {
  oldpar <- graphics::par(no.readonly = TRUE); on.exit(graphics::par(oldpar), add = TRUE)
  rankv <- c(error = 0, warning = 1, info = 2, pass = 3)
  if (length(records))
    records <- records[order(vapply(records, function(r) rankv[[r$worst]], numeric(1)),
                             -vapply(records, function(r) r$n, numeric(1)))]
  g_err <- if (use_unicode) "\u2717" else "x"; g_pass <- if (use_unicode) "\u00b7" else "."
  ell <- if (use_unicode) "\u2026" else ".."
  meta <- .integrityCheckMeta()
  sevcol <- c(error = "red3", warning = "darkorange", info = "grey45", pass = "grey80")
  idcol  <- function(w) switch(w, error = "red3", warning = "darkorange", info = "grey30", "grey55")
  glyph  <- function(s) if (is.na(s)) g_pass else switch(s, error = g_err, warning = "!", info = "i")
  truncr <- function(s, n) if (nchar(s) > n) paste0(substr(s, 1, n - 1L), ell) else s

  cx_id <- 0.004; cx_verd <- 0.66
  cx_ck <- stats::setNames(seq(0.150, 0.600, length.out = length(checks)), checks)
  cex_h <- 0.62; cex_d <- 0.60
  rpp <- 40L; npg <- max(1L, ceiling(max(1L, length(records)) / rpp))
  for (pg in seq_len(npg)) {
    idx <- if (!length(records)) integer(0) else ((pg - 1L) * rpp + 1L):min(pg * rpp, length(records))
    graphics::par(mar = c(1.5, 0.6, 1, 0.6)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
    graphics::text(0, 0.985, "checkSensorIntegrity  -  run summary", adj = c(0, 1), font = 2, cex = 1.4)
    graphics::text(1, 0.985, format(Sys.time(), "%Y-%m-%d %H:%M"), adj = c(1, 1), cex = 0.75, col = "grey40")
    graphics::text(0, 0.93, sprintf("%d tags  |  %d flagged   %d error-severity   %d with warnings",
                                    stats$n, stats$n_flagged, stats$n_err, stats$n_warn), adj = c(0, 1), cex = 0.95)
    graphics::text(0, 0.90, "sorted worst-first  -  marks: cross = error, ! = warning, i = info, dot = pass",
                   adj = c(0, 1), cex = 0.72, col = "grey45")
    hcy <- 0.855
    graphics::rect(0, hcy - 0.014, 1, hcy + 0.014, col = "grey90", border = NA)
    graphics::text(cx_id, hcy, "Tag ID", adj = c(0, 0.5), font = 2, cex = cex_h, col = "grey15")
    for (ck in checks) graphics::text(cx_ck[[ck]], hcy, meta[[ck]]$label, adj = c(0.5, 0.5), font = 2, cex = cex_h * 0.9, col = "grey15")
    graphics::text(cx_verd, hcy, "Verdict", adj = c(0, 0.5), font = 2, cex = cex_h, col = "grey15")
    graphics::segments(0, hcy - 0.016, 1, hcy - 0.016, col = "grey55")
    row_top <- hcy - 0.030; dy <- 0.0195
    for (j in seq_along(idx)) {
      r <- records[[idx[j]]]; y <- row_top - dy * (j - 1L)
      if (j %% 2L == 0L) graphics::rect(0, y - dy / 2, 1, y + dy / 2, col = "grey96", border = NA)
      graphics::text(cx_id, y, truncr(r$id, 16), adj = c(0, 0.5), cex = cex_d, font = 2, col = idcol(r$worst))
      for (ck in checks) {
        s <- r$per_check[[ck]]
        graphics::text(cx_ck[[ck]], y, glyph(s), adj = c(0.5, 0.5), cex = cex_d, font = if (!is.na(s)) 2 else 1,
                       col = if (is.na(s)) sevcol[["pass"]] else sevcol[[s]])
      }
      graphics::text(cx_verd, y, r$verdict, adj = c(0, 0.5), cex = cex_d, col = idcol(r$worst), font = if (r$worst == "error") 2 else 1)
    }
    if (npg > 1) graphics::mtext(sprintf("summary page %d / %d", pg, npg), side = 1, cex = 0.7, col = "grey50")
  }
  invisible(NULL)
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
