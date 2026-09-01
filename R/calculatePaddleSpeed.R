#######################################################################################################
# Paddle-wheel swimming speed #########################################################################
#######################################################################################################

#' Turn paddle-wheel rotation into swimming speed
#'
#' @description
#' A paddle-wheel tag carries a small magnetic rotor that spins as water flows past it: the faster the
#' animal swims, the faster the rotor turns. [processTagData()] recovers that rotation rate from the
#' magnetometer and stores it as `paddle_freq`, in hertz. This function converts it into a swimming
#' speed, in metres per second.
#'
#' The conversion is a single number per tag, the *calibration slope*, applied as
#' `speed = slope x frequency` with no intercept. The slope belongs to the physical tag rather than to
#' the animal, and comes from calibrating the tag before deployment - typically a controlled vertical
#' drop, in which the tag is released through the water column under a range of weights and its
#' rotation rate recorded against the known descent speed.
#'
#' Calibration records are rarely complete: not every tag is calibrated, and rarely in every year it was
#' deployed. This function therefore also estimates the slopes that are missing, and can check the slope
#' it applied against the animal's own diving.
#'
#' @param data Processed deployments, in any of the forms used across the pipeline: a list of datasets,
#'   a single table with an `id.col`, or a character vector of `.rds` paths. The output of
#'   [processTagData()] is expected.
#' @param calibration A table of calibrations with columns `year`, `package_id` and `slope`. A row
#'   matching a deployment's own tag and season is used exactly as measured. `NULL` (default) supplies
#'   no calibration at all, in which case only the in-situ methods can produce a speed.
#' @param method How to fill a gap when a deployment has no calibration of its own. The three
#'   `"projected-*"` methods carry the calibrations you do have forward in time, following
#'   [imputePaddleCalibration()]; the two `"in-situ-*"` methods estimate the slope from the animal's own
#'   diving. A calibration is never overridden, whichever method is chosen. See Details.
#' @param degradation.rate The annual increase in the calibration slope, describing how fast the paddle
#'   wheel loses efficiency. Required for `method = "projected-fixed"`; for the other methods it is an
#'   optional fallback, used only when there are too few repeat calibrations to estimate a rate from.
#'   `NULL` (default) supplies none.
#' @param validate Whether to also check each calibration against the in-situ estimate and report how
#'   well the two agree (default `FALSE`). Needs `pitch` and `vertical_velocity`, and is skipped with a
#'   note where they are absent. Validation only reports; it never changes the speed.
#' @param agreement.threshold How far the applied calibration and the in-situ estimate may differ before
#'   the tag is flagged, as a proportion (default `0.35`, so more than 35% apart in either direction).
#'   Only used when `validate = TRUE`. The default is deliberately loose: the in-situ estimate cannot
#'   resolve differences much below this, so a tighter setting flags tags that simply cannot be told
#'   apart.
#' @param smoothing Window, in seconds, applied to the rotation frequency before converting it to a
#'   speed. Default `1`; `NULL` disables it. On data downsampled to 1 Hz or coarser the binning has
#'   already smoothed at least this much, so the window has little left to do.
#' @param max.speed Speeds above this many km/h are treated as implausible and set to `NA` (default
#'   `10`). `NULL` disables the check.
#' @param min.pitch Steepest-swimming threshold, in degrees, for the in-situ estimate (default `10`).
#'   Shallower samples are excluded: the closer the animal is to level, the less its depth change says
#'   about how fast it is going.
#' @param id.col Which column identifies the animal (default `"ID"`).
#' @param plot Whether to draw the diagnostics to the active graphics device. Default `FALSE`.
#' @param plot.file Path to a PDF holding one panel per tag, comparing the applied calibration with the
#'   in-situ estimate. Default `NULL`.
#' @param return.data Whether to return the annotated data (default `TRUE`) or the written file paths.
#' @param output.dir Directory in which to write one annotated `.rds` file per deployment. Providing a
#'   directory is what triggers saving; `NULL` (default) writes nothing.
#' @param output.suffix Optional suffix appended to each saved file name, before `.rds`.
#' @param compress Compression for the saved `.rds` files: `TRUE` (default, gzip), `FALSE`, or one of
#'   `"gzip"`, `"bzip2"` or `"xz"`. See [base::saveRDS()].
#' @param verbose How much detail to print: `0`/`"quiet"`, `1`/`"normal"`, or `2`/`"detailed"`
#'   (default).
#'
#' @details
#' ## Where a calibration comes from
#'
#' Every deployment's speed rests on one slope, and `slope_source` records where that slope came from:
#'
#' \itemize{
#'   \item `"calibrated"` - a calibration exists for that tag and that season, and is used exactly as
#'     measured. This is the one to prefer.
#'   \item `"projected-from-tag"` - no calibration for that tag and season, but the tag was calibrated
#'     in other years, so its slope is carried forward from its own record.
#'   \item `"projected-from-fleet"` - the tag was never calibrated, so its slope starts from the level
#'     typical of the other tags and is projected from there.
#'   \item `"in-situ-deployment"` - fitted to this deployment's own steep swimming.
#'   \item `"in-situ-pooled"` - fitted to the steep swimming of every deployment sharing that tag and
#'     season. Also what a deployment falls back to when its own record is too short.
#'   \item `"as-recorded"` - the tag wrote a speed directly and recorded no rotation rate, so its values
#'     are kept untouched and no slope applies.
#' }
#'
#' A calibration is always used as it stands: estimation fills gaps, it does not smooth or revise what
#' you observed.
#'
#' ## Choosing a method
#'
#' `method` decides only how a *missing* slope is filled, and the choice matters most where the
#' calibration record is thin.
#'
#' The first three read the calibration table and nothing else. They rest on the observation that
#' paddle wheels wear: efficiency declines with age, so the slope rises over time, and a slope measured
#' three years earlier is not the slope that applied.
#'
#' \itemize{
#'   \item `"projected-shared"` (default) pools one annual wear rate across every tag calibrated more than
#'     once, while keeping a separate level for each tag. It is the steadiest choice on the short
#'     calibration series a tag record usually holds.
#'   \item `"projected-per-tag"` fits a separate line to each tag with at least two calibrations and averages
#'     those rates for the rest. It follows an individual tag more closely, but a rate fitted to two or
#'     three points is mostly noise, so prefer it only with long, clean series.
#'   \item `"projected-fixed"` applies the wear rate you pass as `degradation.rate` rather than estimating
#'     one. Use it when the calibration record is too thin to carry a trend of its own but a rate is
#'     available from elsewhere - a longer record, or other tags of the same design.
#' }
#'
#' The two `"in-situ-*"` methods are different in kind. They ignore the other tags entirely and
#' estimate the slope from the animal's own diving, so they are the only option when there is no
#' calibration table at all. They buy that independence with a stronger assumption about the animal,
#' described next. They differ only in which data the fit rests on: `"in-situ-pooled"` combines every
#' deployment of a tag and season into one slope, while `"in-situ-deployment"` gives each deployment
#' its own, falling back to the pooled fit where a record holds too little steep swimming.
#'
#' Prefer `"in-situ-pooled"` where the slope is expected to be a stable property of the tag, and
#' `"in-situ-deployment"` where it may not be - a tag refouled, remounted or damaged between
#' deployments. The between-deployment spread reported for each tag and season is what tells the two
#' situations apart.
#'
#' Put simply: the first three ask what tags like this one have done, and are limited by how much
#' calibration history exists; the in-situ methods ask what this animal is doing, and are limited by how much
#' steep swimming it did.
#'
#' ## The in-situ estimate
#'
#' While an animal swims steeply up or down, how fast its depth changes depends on both how fast it is
#' swimming and how steeply it is angled, as `vertical speed = swimming speed x sin(pitch)`. Reading
#' depth change against pitch therefore gives a speed that owes nothing to the paddle wheel, and the
#' rotation rate recorded at the same moments gives the slope that would reproduce it.
#'
#' Only samples steeper than `min.pitch` contribute: near level, depth change says almost nothing about
#' speed, and small pitch errors are magnified. The estimate needs `pitch` and `vertical_velocity`, both
#' produced by [processTagData()], and enough steep swimming to measure. Where they are missing, or too
#' sparse, the estimate is withheld rather than guessed.
#'
#' ## Checking a calibration against the animal's own diving
#'
#' With `validate = TRUE` the in-situ estimate is computed for every tag, including those with a
#' measured calibration, and reported beside the slope actually applied. `agreement` is their ratio: `1`
#' means the two say the same thing, `1.5` that the applied slope is half again the in-situ estimate,
#' and `0.7` that it is nearly a third below.
#'
#' Read a disagreement as a reason to look, not as a verdict, and note that it does not say which of the
#' two is wrong. A sharp disagreement is worth checking against a fouled or damaged rotor, a calibration
#' matched to the wrong season, and whether the animal did enough steep swimming for the comparison to
#' carry any weight. Small differences are expected and are not worth acting on.
#'
#' ## When a deployment gets no speed
#'
#' `paddle_speed` is still added, filled with `NA`, and the run reports which of these applied:
#'
#' \itemize{
#'   \item `"no paddle wheel"` - the tag never carried one.
#'   \item `"no paddle data"` - it carried one, but no rotation rate was recorded.
#'   \item `"no calibration"` - a rotation rate exists, but no slope could be found or estimated for it.
#' }
#'
#' No slope is estimated for a deployment that has no rotation rate for it to convert, so a tag that
#' recorded speed directly, or recorded nothing at all, is never given a number it cannot use. Each
#' outcome is recorded in the deployment's own processing record.
#'
#' ## Assumptions and limitations
#'
#' \itemize{
#'   \item Speed is measured through the water, not over the ground. In a current, the animal's track
#'     over the seabed will not match the distance this speed implies.
#'   \item The conversion is linear with no intercept, and every rotor has a speed below which it does
#'     not turn at all. Very slow swimming therefore reads as zero rather than as slow, and time spent
#'     below that threshold is indistinguishable from rest.
#'   \item Wear is real and is only partly modelled. Treat a heavily projected slope, and any
#'     fleet-projected slope, as approximate.
#'   \item The slope describes the tag as it was calibrated. Biofouling, a bent guard, or a different
#'     mounting position all change it, and none of them are visible in the rotation rate alone.
#'   \item The in-situ estimate assumes the animal travels along the direction it is pointing. A body
#'     angled away from the swim path biases it, and that bias cannot be told apart from a genuine
#'     calibration error.
#'   \item An error in the slope scales the whole speed series by a constant. A track reconstructed from
#'     an approximate slope will tend to have the right shape at the wrong size, which is worth
#'     remembering before reading distances off it.
#' }
#'
#' @return The input with `paddle_speed` added, in metres per second, or, when `return.data = FALSE`,
#'   the written file paths, invisibly.
#'
#'   Either way the resolved calibration is attached as the `"calibration"` attribute, one row per tag
#'   and season, with:
#'
#'   - `slope` and `slope_source` - the slope applied and where it came from.
#'   - `n_deployments` - how many deployments that slope covers.
#'   - `in_situ_slope`, `in_situ_lo`, `in_situ_hi` - the in-situ estimate and its 95% interval, present
#'     when the estimate was computed.
#'   - `in_situ_n` and `in_situ_r` - the number of steep-swimming samples behind the estimate, and how
#'     closely depth change tracked rotation rate and pitch across them.
#'   - `agreement` and `flag` - the ratio of the two slopes, and whether it falls outside
#'     `agreement.threshold`. Reported only where the applied slope is independent of the animal's
#'     diving, so it is empty for a slope that came from an in-situ fit.
#'   - `slope_k`, `slope_cv`, `slope_ratio`, `slope_rel_se` - the between-deployment spread of the
#'     per-deployment in-situ fits within that tag and season: how many deployments contributed, their
#'     coefficient of variation, their largest-to-smallest ratio, and the median precision of an
#'     individual fit. Reported, never flagged; a spread far exceeding that precision means the slope
#'     is not behaving as a fixed property of the tag.
#'
#'   Each deployment also carries the slope it used, where that slope came from and how it ended up in
#'   its own processing record, so the provenance survives being saved and reloaded.
#'
#' @seealso [processTagData()] for the step that must come first, which recovers `paddle_freq`;
#'   [imputePaddleCalibration()] for building or inspecting a calibration table on its own;
#'   [reconstructTrack()] for what the resulting speed feeds into.
#'
#' @examples
#' \dontrun{
#' # A measured calibration for every tag and season.
#' tags <- calculatePaddleSpeed(processed, calibration = paddle_cal)
#'
#' # Gaps filled from the calibrations that do exist, and every tag checked against its own diving.
#' tags <- calculatePaddleSpeed(processed, calibration = paddle_cal, method = "projected-shared",
#'                              validate = TRUE)
#' attr(tags, "calibration")
#'
#' # No calibration record at all: estimate every slope from the animals themselves, pooling the
#' # deployments of each tag and season.
#' tags <- calculatePaddleSpeed(processed, method = "in-situ-pooled")
#'
#' # A tag suspected of changing between deployments: give each deployment its own slope.
#' tags <- calculatePaddleSpeed(processed, method = "in-situ-deployment")
#'
#' # Reapply a corrected calibration table, saving one annotated file per deployment.
#' calculatePaddleSpeed(list.files("./processed", full.names = TRUE), calibration = paddle_cal_v2,
#'                      plot.file = "./plots/paddle_calibration.pdf",
#'                      return.data = FALSE, output.dir = "./speed")
#' }
#' @export

calculatePaddleSpeed <- function(data,
                                 calibration = NULL,
                                 method = c("projected-shared", "projected-fixed", "projected-per-tag",
                                            "in-situ-deployment", "in-situ-pooled"),
                                 degradation.rate = NULL,
                                 validate = FALSE,
                                 agreement.threshold = 0.35,
                                 smoothing = 1,
                                 max.speed = 10,
                                 min.pitch = 10,
                                 id.col = "ID",
                                 plot = FALSE,
                                 plot.file = NULL,
                                 return.data = TRUE,
                                 output.dir = NULL,
                                 output.suffix = NULL,
                                 compress = TRUE,
                                 verbose = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  method <- match.arg(method)
  .assert_string(id.col, "id.col")
  .assert_flag(validate, "validate"); .assert_flag(plot, "plot")
  .assert_number(agreement.threshold, "agreement.threshold", min = 0)
  if (agreement.threshold <= 0)
    .abort("{.arg agreement.threshold} must be greater than zero; got {.val {agreement.threshold}}.")
  .assert_flag(return.data, "return.data")
  .assert_writable_file(plot.file, "plot.file", ext = "pdf")
  if (!is.null(smoothing)) .assert_number(smoothing, "smoothing", min = 0)
  if (!is.null(max.speed)) .assert_number(max.speed, "max.speed", min = 0)
  .assert_number(min.pitch, "min.pitch", min = 0)
  if (min.pitch >= 90) .abort("{.arg min.pitch} must be below 90 degrees; got {.val {min.pitch}}.")
  if (!is.null(degradation.rate)) .assert_number(degradation.rate, "degradation.rate")
  in_situ_method <- method %in% c("in-situ-deployment", "in-situ-pooled")
  if (identical(method, "projected-fixed") && is.null(degradation.rate))
    .abort(c("{.code method = \"projected-fixed\"} needs {.arg degradation.rate} to be supplied.",
             "i" = "It is the annual increase in the calibration slope. The other methods estimate it
                    from the calibrations you already have."))
  calibration <- .assert_calibration(calibration)
  if (is.null(calibration) && !in_situ_method)
    .abort(c("No {.arg calibration} was supplied, so there is nothing to fill the gaps from.",
             "i" = "Pass a calibration table, or use {.code method = \"in-situ-pooled\"} to estimate
                    the slope from the deployments themselves."))

  src <- .resolveInput(data, id.col)
  # header config, one fact per line, shown once and never repeated per deployment. The calibration
  # itself is a RESULT - it is resolved from the cohort further down - so it belongs in the summary.
  .log_header(lvl, "calculatePaddleSpeed", "Converting paddle rotation into swimming speed",
              bullets = sprintf("Input: %d deployment%s", src$n, if (src$n != 1) "s" else ""),
              arrow = c(
                if (identical(method, "in-situ-deployment"))
                  "Calibration: in situ, from each deployment's own diving (pooled where too little)"
                else if (identical(method, "in-situ-pooled"))
                  "Calibration: in situ, pooled across each tag-season's diving"
                else sprintf("Calibration: %s (missing slopes projected from the calibrated ones)",
                             method),
                if (!is.null(degradation.rate))
                  sprintf("Wear rate: %g per year", degradation.rate),
                if (!is.null(smoothing) && smoothing > 0)
                  sprintf("Smoothing: %g s on the rotation frequency", smoothing) else "Smoothing: none",
                if (!is.null(max.speed)) sprintf("Speed cap: %g km/h", max.speed) else "Speed cap: none",
                if (isTRUE(validate))
                  sprintf("Validation: in situ, from pitch and vertical velocity (pitch >= %g deg)",
                          min.pitch)
                else "Validation: off (validate = FALSE)"))

  ## ---- pass 1: what each deployment carries, and its in-situ sufficient statistics ---------------
  # Accumulated rather than stored: the in-situ slope is a through-origin fit, so its sums add across
  # the deployments of one tag and the fit can be done per tag without holding any of the data.
  scan <- vector("list", src$n)
  pb <- .log_progress_start(lvl, src$n, "Scanning")
  for (i in seq_len(src$n)) {
    .log_progress_step(pb)
    scan[[i]] <- .paddleScanOne(data.table::as.data.table(src$get(i)), src$ids[i], min.pitch,
                                need.insitu = validate || in_situ_method)
  }
  .log_progress_done(pb)

  ## ---- resolve one slope per tag and season -----------------------------------------------------
  res_cal <- .paddleResolve(scan, calibration, method, degradation.rate, agreement.threshold, lvl)
  cal <- res_cal$cal; dep <- res_cal$dep

  ## ---- pass 2: apply -----------------------------------------------------------------------------
  data_list <- vector("list", src$n); saved <- vector("list", src$n)
  ids <- rep(NA_character_, src$n); statuses <- character(0)
  speeds <- rep(NA_real_, src$n)          # per-deployment median speed, for the cohort roll-up

  for (i in seq_len(src$n)) {
    x <- data.table::as.data.table(src$get(i))
    id <- as.character(.getMeta(x)$id %||% src$ids[i]); ids[i] <- id
    .log_h2(lvl, sprintf("%s (%d/%d)", id, i, src$n))
    # the slope a deployment uses is its own row: under `in-situ-deployment` it can differ from the
    # tag-season value, which is why the resolver returns a per-deployment table as well
    drow <- dep[match(scan[[i]]$id, dep$id), , drop = FALSE]
    res <- .paddleApplyOne(x, scan[[i]], drow, smoothing, max.speed)
    statuses <- c(statuses, res$status)
    if (!is.null(res$speed)) speeds[i] <- res$speed[["med"]]
    .logPaddleDeployment(lvl, id, scan[[i]], res, drow)

    meta <- .getMeta(res$data)
    meta <- .appendProcessing(meta, "calculatePaddleSpeed",
                              slope = res$slope, slope_source = res$slope_source,
                              method = method, degradation_rate = degradation.rate %||% NA_real_,
                              smoothing_s = smoothing %||% NA_real_,
                              max_speed_kmh = max.speed %||% NA_real_,
                              # this deployment's own in-situ fit, not the tag-season's: a
                              # per-deployment record should describe the deployment
                              in_situ_slope = if (nrow(drow)) drow$own_slope else NA_real_,
                              in_situ_n = if (nrow(drow)) drow$own_n else NA_integer_,
                              in_situ_seconds = if (nrow(drow)) drow$own_secs else NA_real_,
                              agreement = if (nrow(drow)) drow$agreement else NA_real_,
                              status = res$status)
    x <- .restoreMeta(res$data, meta)

    saved[i] <- list(.saveOutput(x, id, output.dir = output.dir,
                                 output.suffix = output.suffix, compress = compress))
    data_list[[i]] <- x
    .log_gap(lvl)
  }

  if (lvl >= 1L) {
    .log_summary(lvl)
    .reportPaddleCohort(lvl, cal, dep, statuses, speeds, agreement.threshold, output.dir, plot.file)
    .log_runtime(lvl, start.time)
  }
  if (isTRUE(plot) || !is.null(plot.file)) .renderPaddleDiagnostic(cal, plot = plot, plot.file = plot.file)

  out <- .collectOutput(data_list, saved, return.data, ids)
  # `return.data = FALSE` with no `output.dir` leaves nothing to return, and NULL takes no attributes -
  # which is exactly the run where the calibration table is the only thing the caller wanted.
  if (is.null(out)) out <- character(0)
  attr(out, "calibration") <- cal[, setdiff(names(cal),
                                          c("key", "has_paddle", "needs_slope", "as_recorded",
                                            "in_situ_viable")), drop = FALSE]
  out
}
