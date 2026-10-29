#######################################################################################################
# Validate the structural integrity of sensor channels ################################################
#######################################################################################################

#' Validate the structural integrity of sensor channels
#'
#' @description
#' Screens each deployment's sensor channels for structural, hardware/firmware-level faults - as opposed
#' to per-sample signal glitches, which are the job of [checkSensorQuality()]. It answers a single
#' question per channel: *is this a trustworthy instance of the sensor it claims to be?* Faulty channels
#' are reported (and, with `apply = TRUE`, dropped) rather than repaired, since interpolating a corrupt
#' channel would only hide the fault.
#'
#' This is the data-driven complement to the metadata `exclude_sensors` catalogue handled at import:
#' the catalogue records *known*-bad channels, while this function *detects* them from the data, catching
#' undocumented cases. Run it after [regularizeTimeSeries()] and before [checkSensorQuality()] and
#' [checkTagMapping()], so corrupt channels are removed before any repair, orientation or downstream
#' analysis sees them.
#'
#' @details
#' ## How severity is decided
#' Severity is graded from the measurement, not fixed by the check. Each check computes a metric,
#' and that metric is classified `"info"` / `"warning"` / `"error"` against the thresholds in
#' [integrityControl()], named `<check>.<severity>`. So the same check reports different grades
#' for different magnitudes: an accelerometer clipping on 1% of samples is a `"warning"` worth a look,
#' while one clipping on 99% is an `"error"` - it has lost the dynamic range that orientation, dynamic
#' acceleration and tail-beat detection all depend on. This is what separates a minor blemish from a
#' compromised channel, which a per-check severity cannot do.
#'
#' ## Why some checks cannot report an error
#' An automatic error verdict is defensible only where a broken channel is clearly separated from a
#' healthy one, rather than sitting at one end of a continuum. Clipping and static-acceleration magnitude
#' behave that way - faulty channels stand well apart from sound ones - so both offer an error grade.
#' Magnetometer field variability does not: it varies continuously between deployments, so
#' `mag.plausibility` exposes a warning threshold only, and any error cut-off would be false precision.
#' `dead` and `duplication` are degenerate by definition - a constant or copied channel carries no
#' information at all - and are always graded error.
#'
#' ## Classification is separate from action
#' A grade describes the finding; `apply` and
#' `apply.severity` decide what happens. Nothing is modified unless `apply = TRUE`, and findings at or
#' above `apply.severity` then have their channels excluded. Error-severity findings are always warned
#' about, whether or not `apply` acted, so a compromised channel cannot pass through unmentioned.
#'
#' One exception, by design: `mag.break` describes the record - one calibration cannot span it - rather
#' than the trustworthiness of a channel, so `apply` never drops anything on its account at any
#' `apply.severity`. The magnetometer is sound, and the data before the break is fully usable.
#'
#' ## The checks
#' Four run by default - `duplication`, `dead`, `accel.scale` and `saturation`; the rest are opt-in via
#' `checks`. Every threshold lives in [integrityControl()], and the metric each check reports is noted in
#' parentheses.
#'
#' **duplication** (error). A gyroscope or magnetometer triplet whose three axes are each a near-exact
#' copy of the accelerometer (per-axis \code{|r| > duplication.error} on all three). Distinct sensor families cannot
#' track each other this closely unless one is literally a copy - the signature of a firmware bug that
#' duplicates the accelerometer into an absent channel. The channel carries no real information and should
#' be excluded. (metric: the minimum per-axis \code{|r|}.)
#'
#' **dead** (error). A channel exactly constant over the whole deployment - the sensor never produced
#' a signal. Depth is exempt (a never-submerged tag legitimately reads a constant). Covers the IMU
#' triplets, temperature, and the imported paddle channels \code{paddle_speed} / \code{paddle_freq}: a
#' paddle column holding one fixed value is a dead, jammed or absent paddle wheel rather than a
#' measurement, and left in place it would count as that many genuine speed samples in any pooled
#' distribution. The message names which of those it is, from the tag's documented `paddle_wheel` flag;
#' the grade does not change, because the consequence for the data is the same either way. (metric: 0.)
#'
#' **saturation** (warning / error). A channel pinned at its exact minimum or maximum for a sustained
#' fraction of samples (\code{> saturation.warning}, escalating to an error above \code{saturation.error}):
#' the signal is clipping against the sensor's dynamic-range rail,
#' so the true values beyond it are lost. Typically a mis-set measurement range or an over-driven (e.g.
#' mis-scaled) channel. Like every check here it covers the accelerometer, gyroscope and magnetometer
#' only - depth and temperature are screened for physical-range faults by [checkSensorQuality()]
#' instead, which is the right place for them (a depth channel resting at the surface is not clipping).
#' (metric: the clipped fraction.)
#'
#' **mag.plausibility** (warning). The geomagnetic field magnitude \code{|B|} is orientation-invariant,
#' so after provisional hard-iron centring it should stay near-constant. A high robust coefficient of
#' variation (\code{> mag.plausibility.warning}) points to a mis-scaled or soft-iron-distorted magnetometer, or a corrupt
#' channel (a magnetometer duplicated from the accelerometer tracks motion and fails here too). The check
#' abstains when the animal did not rotate through enough orientations to trust the centring. (metric: the
#' robust CV.)
#'
#' **mag.break** (warning). A persistent step in the magnetometer's field magnitude partway through
#' the deployment: the local magnetic environment changed - contamination attaching or shedding - so a
#' single calibration cannot serve the whole record. Detection only; nothing is corrected or segmented,
#' and the finding never excludes a channel. The message names the time of the break, so the record can
#' be split, re-calibrated per segment, or the affected part excluded by hand.
#'
#' The metric is a rank separation, not a step size, and that distinction is what makes the check usable.
#' A contaminated magnetometer's field magnitude varies with heading, so an animal that keeps turning
#' swings it between levels for the whole deployment, and any unbalanced swing produces a large
#' difference between the two halves' medians - which is why step size alone flags far more deployments
#' than have genuinely broken. What marks a break is that the level changes and *does not come back*, so
#' the metric is the Mann-Whitney probability of superiority between the two segments'
#' window medians (1 = the levels never overlap, 0.5 = indistinguishable). Being rank-based it is
#' unit-free and independent of the noise amplitude, and a repeatedly-oscillating record scores near 0.5
#' however large its swings - the worst oscillator in the fleet scores 0.74, stable to within 0.01 across
#' window counts. At the default 0.96 the check flags 3 of the 39 deployments long enough to evaluate,
#' with the next-ranked at 0.94.
#'
#' Warning only: below the flagged group the metric runs continuously from 0.94 down to 0.59, so an
#' automatic error grade would be false precision - the same reasoning applied to \code{mag.plausibility}.
#'
#' Two limits are structural rather than incidental. Each side of a break must be at least 15% of the
#' record for it to count as persistent, so **a break in the first or last 15% cannot be seen**; and a
#' record yielding fewer than 30 ten-minute windows - about 5 hours - makes the check **abstain** rather
#' than guess, which is common on short deployments. (metric: the separation, in
#' \[0.5, 1\].)
#'
#' **accel.scale** (warning / error). Checks whether the overall accelerometer magnitude is consistent with
#' gravity. The static (gravity) component is taken as a low-pass of the three axes, and its magnitude
#' should sit near 1 g across the record; the check grades the deviation \code{|median - 1|} against
#' \code{accel.scale.warning} and \code{accel.scale.error}. It needs
#' no quiescent periods - the low-pass averages the animal's own motion away - so it applies equally to a
#' continuously active animal. It detects uniform scaling errors, for example acceleration left in m/s^2
#' and never converted to g, but may not detect errors affecting only individual axes: on a near-level
#' animal a gain error on a single axis barely moves the vector magnitude while still distorting roll.
#' Users working with unusual sensor configurations or unexpected results should inspect individual sensor
#' axes in addition to this summary metric.
#'
#' A uniform scale error is worth flagging even though it leaves roll and pitch exactly unchanged (a
#' common factor cancels in the arctangent). It survives into every magnitude-derived channel instead:
#' ODBA, VeDBA and tail-beat amplitude are all proportional to it, so a cohort whose tags are calibrated
#' a few per cent apart carries a silent, systematic bias in any comparison of effort between animals.
#' That is why the warning level sits at a few per cent rather than at the point where the units are
#' obviously wrong. (metric: the median static magnitude, g.)
#'
#' **gyro.bias** (info). Over a long record the animal's rotations average out, so each gyroscope axis
#' should have a near-zero median. A persistent offset that is both a large fraction of the rotational
#' scale (\code{> gyro.bias.info}) and absolutely meaningful (against an internal floor) is a sensor bias.
#' Gyroscope bias is not explicitly corrected. The default orientation algorithm in
#' [processTagData()] (\code{"tilt_compass"}) does not use the gyroscope, so gyroscope bias does
#' not affect it. The \code{"madgwick"} algorithm does use the gyroscope; small biases are largely absorbed
#' by the accelerometer and magnetometer, but larger biases may affect orientation estimates and should be
#' inspected before relying on derived movement metrics. (metric: the largest \code{|median|}, rad/s.)
#'
#' **paddle.contamination** (warning). A magnetic paddle wheel spins with the animal's speed and
#' induces a narrow-band peak in the magnetometer at its rotation frequency. The check scans the
#' magnetometer spectrum for such a peak, but only above the tail-beat fundamental and its harmonics - a
#' swimming animal's body oscillation modulates the magnetometer at around 0.2-0.5 Hz and would otherwise
#' be flagged as a paddle - and below the Nyquist frequency, to reject aliasing; see [integrityControl()].
#' It fires only where the deployment is not already recorded as carrying a paddle wheel, so a hit means
#' an undocumented paddle effect, worth confirming against the spectrum panel. (metric: the peak prominence,
#' peak / median band power.)
#'
#' **dropout** (info). A channel missing (NA) for more than \code{dropout.info} of the deployment, so
#' effectively absent. (metric: the missing fraction.)
#'
#' ## The diagnostic report
#' When `plot` or `plot.file` is set, the function draws an overview page
#' tabling every tag and the checks it triggered (sorted worst-first), then one page per flagged
#' (warning/error) tag aggregating a diagnostic panel for each of that tag's findings - each panel with a
#' coloured status border and a short interpretation note. Tags with only `info`-level findings appear in
#' the overview but do not get their own page.
#'
#' @param data Sensor data in any of the pipeline forms: a list of `nautilus_tag` objects (one per
#'   individual), a single aggregated data.table/data.frame with an `id.col`, or a character vector of
#'   `.rds` file paths (loaded lazily). The output of [importTagData()] (optionally regularized) is
#'   expected.
#' @param checks Character vector of integrity checks to run. Defaults to the two high-confidence,
#'   error-severity checks - `"duplication"` (a channel that is a near-exact copy of the accelerometer,
#'   the firmware accel-duplication signature) and `"dead"` (a channel constant over the whole
#'   deployment) - plus two advisory checks that earn their place by catching real hardware faults at no
#'   measurable cost: `"accel.scale"` (a static acceleration magnitude far from 1 g, which catches unit
#'   and scaling mistakes) and `"saturation"` (a channel pinned at a range limit, which catches a railed
#'   axis, a clipping gyroscope and sentinel values). Both are GRADED, so either can report a warning or an
#'   error depending on the magnitude measured. The remaining checks are opt-in: `"mag.plausibility"` (an
#'   unstable magnetometer field magnitude), `"mag.break"` (a persistent mid-deployment step in the
#'   magnetometer field, i.e. the magnetic environment changed and one calibration no longer covers the
#'   record), `"gyro.bias"` (a persistent gyroscope offset), `"paddle.contamination"` (a narrow-band
#'   magnetometer peak suggesting an undocumented paddle wheel) and `"dropout"` (a channel missing for
#'   most of the deployment).
#' @param control An [integrityControl()] object (or a named list of its fields) bundling the
#'   per-check detection thresholds. Defaults to `integrityControl()` (values calibrated on real
#'   whale-shark deployments).
#' @param apply Logical. Whether to act on the findings. If `FALSE` (default) the function only reports
#'   and the data passes through unchanged. If `TRUE`, the channels of every finding at or above
#'   `apply.severity` are dropped from the returned data and recorded in the tag metadata
#'   (`meta$sensors$excluded`); the per-channel reasons remain in `issues`. Caller-owned data and metadata
#'   are never modified. Report-only runs add no processing history; returned datasets are independent
#'   copies, so later edits to them cannot change the input.
#' @param apply.severity The severity from which `apply` intervenes: `"error"` (default - only findings
#'   graded error) or `"warning"` (also drop warning-level findings, for a cautious run). Kept separate
#'   from `apply` so the diagnostic classification and the action stay independent concepts.
#'   Ignored when `apply = FALSE`.
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
#' @seealso [checkSensorQuality()], [checkDeploymentMetadata()], [checkTagMapping()].
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
                                 apply.severity = c("error", "warning"),
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
  apply.severity <- match.arg(apply.severity)
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
                        sprintf("Mode: %s", if (apply) sprintf("report + drop channels graded %s or worse", apply.severity)
                                            else "report only")))

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
  error_items <- character(0)   # one bullet per deployment with an error-severity finding (warned at the end)

  for (i in seq_len(r$n)) {
    x <- r$get(i, writable = apply || return.data)
    id <- tryCatch(as.character(unique(x[[id.col]])[1]), error = function(e) NA_character_)
    if (is.na(id)) id <- as.character(r$ids[i])
    fs <- tryCatch(.tagFs(x, datetime.col), error = function(e) NA_real_)

    # metadata (paddle-wheel context + provenance), then run the requested checks
    meta <- .getMeta(x)
    iss <- .runIntegrityChecks(x, fs, checks, control, paddle_wheel = if (!is.null(meta)) meta$tag$paddle_wheel else NA)
    if (nrow(iss)) iss$id <- id
    all_issues[[i]] <- iss

    # findings AT OR ABOVE apply.severity are the ones eligible for exclusion; error-severity findings are
    # tracked separately because they are always warned about, whether or not `apply` acted on them.
    act_sel    <- .severityRank(iss$severity) >= .severityRank(apply.severity)
    act_sel[is.na(act_sel)] <- FALSE
    # ADVISORY checks describe the RECORD, not the trustworthiness of a channel, so `apply` must never
    # drop anything on their account. `mag.break` says one calibration cannot span the record - the
    # magnetometer itself is fine and the data before the break is fully usable, so excluding it would
    # destroy far more than it protects. (Severity still grades the finding; only the ACTION is barred.)
    act_sel <- act_sel & !(iss$check %in% .integrityAdvisoryChecks())
    err_tokens <- unique(iss$channel[act_sel])
    err_chans  <- if (length(err_tokens)) intersect(.expandSensorTokens(paste(err_tokens, collapse = ",")), names(x)) else character(0)
    if (any(iss$severity == "error")) {
      err_fams <- .channelsToFamilies(intersect(.expandSensorTokens(paste(unique(iss$channel[iss$severity == "error"]), collapse = ",")), names(x)))
      error_items <- c(error_items, sprintf("%s: %s (%s)", id, paste(err_fams, collapse = ", "),
                                            paste(unique(iss$check[iss$severity == "error"]), collapse = ", ")))
    }
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
      .log_skip(lvl, id, "  ", nadv, " advisory finding", if (nadv != 1) "s",
                sprintf(" (below %s; not excluded)", apply.severity))
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

    # Only apply mode changes data or processing history; the resolver already owns that table.
    if (apply && length(err_chans)) {
      x[, (err_chans) := NULL]
      if (!is.null(meta)) meta$sensors$excluded <- unique(c(meta$sensors$excluded, err_chans))
    }
    if (apply && !is.null(meta)) {
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

  # An error-severity finding means a channel cannot support quantitative use. It is warned about ALWAYS -
  # whether or not `apply` excluded it - because the alternative is a compromised channel flowing into
  # orientation, dynamic acceleration and tail-beat estimation with nothing but a console line to show it.
  .warn_grouped(
    "{length(error_items)} deployment{?s} {?has/have} a sensor channel with a serious integrity problem.",
    items = error_items,
    hints = c(if (apply) "Their channels were EXCLUDED from the returned data (see {.code meta$sensors$excluded})."
              else "Nothing was modified: re-run with {.code apply = TRUE} to exclude them, or exclude these deployments from analyses that use the affected sensors.",
              "A channel graded {.val error} is not suitable for quantitative analyses - orientation, dynamic acceleration and tail-beat estimates all depend on it."))

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
    # Name the compromised deployments on the console as well as in the warning. R keeps only the FIRST 50
    # warnings of a top-level call, and reading a large batch of tags can easily emit that many on its own
    # (encoding notices from stored channel labels, for one) - which would silently discard the very
    # warning this redesign exists to guarantee. A console block cannot be truncated that way.
    if (length(error_items)) {
      .log_attention(lvl, "severity ", cli::style_bold("error"), " - not suitable for quantitative analyses:")
      # forced to level 2 so the affected deployments are NAMED even on a normal-verbosity run: these lines
      # are the actionable part of the block, not an optional diagnostic.
      for (it in error_items) .log_subdetail(max(lvl, 2L), it)
      .log_arrow(lvl, if (apply) "excluded from the returned data" else
                      "nothing modified - re-run with apply = TRUE to exclude them")
    }
    if (!is.null(plot.file)) .log_arrow(lvl, "plots: ", plot.file)
    .log_runtime(lvl, start.time)
  }

  curated <- if (return.data) results else if (apply) unlist(saved, use.names = FALSE) else NULL
  list(curated_data = curated, issues = issues)
}
