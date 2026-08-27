#######################################################################################################
# Paddle-wheel swimming speed #########################################################################
#######################################################################################################

#' Turn paddle-wheel rotation into swimming speed
#'
#' @description
#' Tags fitted with a paddle wheel record how fast it spins. [processTagData()] recovers that rotation
#' rate from the magnetometer and stores it as `paddle_freq`; this function turns it into a speed.
#'
#' The conversion is one number per tag - `speed = slope x frequency` - and that number comes from
#' calibrating the tag before it goes in the water. Calibrations are not always available for every tag
#' and every season, so this function also fills the gaps, and can check the result against the animal's
#' own behaviour.
#'
#' Keeping this separate from [processTagData()] means a calibration can be revised, or checked, without
#' reprocessing the raw sensor data: only one column is affected, and it is recomputed in seconds.
#'
#' @param data Processed deployments, in any of the forms used across the pipeline: a list of datasets,
#'   a single table with an `id.col`, or a character vector of `.rds` paths. The output of
#'   [processTagData()] is expected.
#' @param calibration A table of calibration values with columns `year`, `package_id` and `slope`.
#'   Rows matching a deployment's own tag and season are used as they are. `NULL` (default) means no
#'   calibration is supplied, in which case `method = "in-situ"` is the only way to get a speed.
#' @param method How to fill a gap when a deployment has no calibration of its own: `"shared-rate"`
#'   (default), `"fixed-rate"` and `"per-tag"` estimate it from the calibrations you do have, following
#'   [imputePaddleCalibration()]; `"in-situ"` estimates it from the deployment itself, using how fast
#'   the animal changed depth while swimming at a steep angle. See Details.
#' @param validate Whether to also check each calibration against the in-situ estimate and report the
#'   agreement (default `FALSE`). The function's job is to calculate speed; checking the calibration is
#'   a separate question, so ask for it explicitly. Needs `pitch` and `vertical_velocity`, and is
#'   skipped with a note where they are absent.
#' @param agreement.threshold How far the applied calibration and the in-situ estimate may differ
#'   before the tag is flagged, as a proportion (default `0.35`, so more than 35% apart in either
#'   direction). Only used when `validate = TRUE`. The default is deliberately loose: the in-situ
#'   estimate cannot resolve differences much below this, so a tighter setting flags tags that simply
#'   cannot be told apart.
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
#' Every deployment's speed rests on one slope, and the table reports where that slope came from:
#'
#' \itemize{
#'   \item `measured` - the tag was calibrated for that season and the value is used unchanged. This is
#'     the one to prefer.
#'   \item `tag-model` / `baseline` - no calibration for that tag and season, so it was estimated from
#'     the calibrations of other tags, or of the same tag in other seasons. See
#'     [imputePaddleCalibration()].
#'   \item `in-situ` - estimated from the deployment itself, with `method = "in-situ"`.
#'   \item `as-recorded` - the tag wrote a speed directly and there is no frequency to calibrate, so the
#'     recorded values are kept untouched.
#' }
#'
#' ## The in-situ estimate
#'
#' While an animal swims steeply up or down, how fast its depth changes depends on how fast it is
#' swimming and how steeply it is angled. Comparing the two gives a speed that owes nothing to the
#' paddle wheel, so it can be used to fill a missing calibration or to check one you already have.
#'
#' It is the weaker of the two: it assumes the animal travels along the direction it is pointing, and it
#' needs enough steep swimming to measure. Treat it as a way to catch a calibration that is clearly
#' wrong rather than as a replacement for calibrating the tag, and prefer a measured value wherever one
#' exists.
#'
#' ## Checking a calibration
#'
#' With `validate = TRUE` the in-situ estimate is computed for every tag, including those with a
#' measured calibration, and reported beside the slope actually applied. Agreement is their ratio: `1`
#' means they say the same thing. Small differences are expected and are not worth acting on; a tag
#' whose two estimates differ by more than `agreement.threshold` is flagged as worth looking into.
#'
#' @return The input with `paddle_speed` added, or, when `return.data = FALSE`, the written file paths,
#'   invisibly. Either way the resolved calibration is attached as the `"calibration"` attribute: one
#'   row per tag and season, with the slope applied, where it came from, and - when validated - the
#'   in-situ estimate, its confidence interval and the agreement. Each deployment also carries the same
#'   information in its own processing record, so it survives being saved and reloaded.
#'
#' @seealso [processTagData()], which recovers `paddle_freq`; [imputePaddleCalibration()] for the
#'   calibration table on its own; [reconstructTrack()], which can use the speed produced here.
#'
#' @examples
#' \dontrun{
#' # A calibration for every tag.
#' tags <- calculatePaddleSpeed(processed, calibration = paddle_cal)
#'
#' # Gaps filled from the calibrations that do exist, and every tag checked against its own diving.
#' tags <- calculatePaddleSpeed(processed, calibration = paddle_cal, method = "shared-rate",
#'                              validate = TRUE)
#' attr(tags, "calibration")
#'
#' # Gaps filled from the animals themselves.
#' tags <- calculatePaddleSpeed(processed, calibration = paddle_cal, method = "in-situ")
#' }
#' @export

calculatePaddleSpeed <- function(data,
                                 calibration = NULL,
                                 method = c("shared-rate", "fixed-rate", "per-tag", "in-situ"),
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
  calibration <- .assert_calibration(calibration)
  if (is.null(calibration) && !identical(method, "in-situ"))
    .abort(c("No {.arg calibration} was supplied, so there is nothing to fill the gaps from.",
             "i" = "Pass a calibration table, or use {.code method = \"in-situ\"} to estimate the slope
                    from the deployments themselves."))

  src <- .resolveInput(data, id.col)
  .log_header(lvl, "calculatePaddleSpeed", "Converting paddle rotation into swimming speed",
              bullets = sprintf("Input: %d deployment%s", src$n, if (src$n != 1) "s" else ""),
              close = FALSE)

  ## ---- pass 1: what each deployment carries, and its in-situ sufficient statistics ---------------
  # Accumulated rather than stored: the in-situ slope is a through-origin fit, so its sums add across
  # the deployments of one tag and the fit can be done per tag without holding any of the data.
  scan <- vector("list", src$n)
  pb <- .log_progress_start(lvl, src$n, "Scanning")
  for (i in seq_len(src$n)) {
    .log_progress_step(pb)
    scan[[i]] <- .paddleScanOne(data.table::as.data.table(src$get(i)), src$ids[i], min.pitch,
                                need.insitu = validate || identical(method, "in-situ"))
  }
  .log_progress_done(pb)

  ## ---- resolve one slope per tag and season -----------------------------------------------------
  cal <- .paddleResolve(scan, calibration, method, agreement.threshold, lvl)
  .reportPaddleCalibration(lvl, cal, method, validate)
  .log_header_close(lvl)

  ## ---- pass 2: apply -----------------------------------------------------------------------------
  data_list <- vector("list", src$n); saved <- vector("list", src$n)
  ids <- rep(NA_character_, src$n); statuses <- character(0)

  for (i in seq_len(src$n)) {
    x <- data.table::as.data.table(src$get(i))
    id <- as.character(.getMeta(x)$id %||% src$ids[i]); ids[i] <- id
    row <- cal[match(scan[[i]]$key, cal$key), , drop = FALSE]
    res <- .paddleApplyOne(x, scan[[i]], row, smoothing, max.speed)
    statuses <- c(statuses, res$status)

    meta <- .getMeta(res$data)
    meta <- .appendProcessing(meta, "calculatePaddleSpeed",
                              slope = res$slope, slope_source = res$slope_source,
                              method = method, smoothing_s = smoothing %||% NA_real_,
                              max_speed_kmh = max.speed %||% NA_real_,
                              in_situ_slope = if (nrow(row)) row$in_situ_slope else NA_real_,
                              in_situ_n = if (nrow(row)) row$in_situ_n else NA_integer_,
                              agreement = if (nrow(row)) row$agreement else NA_real_,
                              status = res$status)
    x <- .restoreMeta(res$data, meta)

    saved[i] <- list(.saveOutput(x, id, output.dir = output.dir,
                                 output.suffix = output.suffix, compress = compress))
    data_list[[i]] <- x
  }

  if (lvl >= 1L) {
    .log_summary(lvl)
    .reportPaddleCohort(lvl, cal, statuses, output.dir)
    .log_runtime(lvl, start.time)
  }
  if (isTRUE(plot) || !is.null(plot.file)) .renderPaddleDiagnostic(cal, plot = plot, plot.file = plot.file)

  out <- .collectOutput(data_list, saved, return.data, ids)
  # `return.data = FALSE` with no `output.dir` leaves nothing to return, and NULL takes no attributes -
  # which is exactly the run where the calibration table is the only thing the caller wanted.
  if (is.null(out)) out <- character(0)
  attr(out, "calibration") <- cal[, setdiff(names(cal), "key"), drop = FALSE]
  out
}
