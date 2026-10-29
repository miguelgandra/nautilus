#######################################################################################################
# Measure reconstructTrack accuracy by holding out real position fixes ################################
#######################################################################################################

#' Assess reconstructed tracks using held-out position fixes
#'
#' @description
#' Evaluates horizontal track reconstruction by leave-one-fix-out cross-validation. Each eligible
#' recorded position or metadata pop-up is withheld in turn, the path is reconstructed from the
#' remaining anchors, and the predicted position is compared with the withheld coordinates.
#'
#' The function uses the processed sensor datasets and the same [reconstructTrackControl()]
#' settings as [reconstructTrack()]. It returns one diagnostic record per successful holdout.
#' These errors quantify prediction at available fixes; they do not provide underwater ground
#' truth or directly validate every position along the reconstructed path.
#'
#' @param data A tag dataset, a list of tag datasets, a data frame containing deployments identified
#'   by \code{id.col}, or a character vector of \code{.rds} file paths. Supply the processed inputs
#'   used for [reconstructTrack()], with its required orientation and speed channels, deployment
#'   coordinates, and at least one eligible position or pop-up beyond the origin. Files are read
#'   one deployment at a time.
#' @param control A control object created by [reconstructTrackControl()], specifying the
#'   reconstruction settings to evaluate. Default settings use constant speed at 0.5 m/s and
#'   \code{"error_weighted"} position correction. Use the same control for reconstruction and
#'   validation when assessing a particular workflow.
#' @param id.col Character. Name of the column identifying deployments, not animals. Default
#'   \code{"ID"}.
#' @param datetime.col Character. Name of the \code{POSIXct} timestamp column. Default
#'   \code{"datetime"}. Observations must be in chronological order.
#' @param plot Logical. Draw the diagnostic report on the active graphics device. Default
#'   \code{FALSE}.
#' @param plot.file Character. Path to a diagnostic PDF, or \code{NULL} (default). Providing a
#'   path writes the report independently of \code{plot} when at least one holdout is scored;
#'   the parent directory must exist.
#' @param verbose Verbosity: \code{FALSE}/\code{0}/\code{"quiet"},
#'   \code{TRUE}/\code{1}/\code{"normal"}, or \code{2}/\code{"detailed"} (default).
#'   Normal output uses a deployment-level progress bar; detailed output reports each deployment.
#'
#' @details
#' ## Validation procedure
#'
#' Position anchors are prepared in the same way as for [reconstructTrack()]: the deployment
#' coordinates define the first observation, recorded fixes are resolved onto the sensor time
#' grid, and a metadata pop-up is aligned to its nearest timestamp. Only the retained anchor at
#' each sensor row is eligible; the function does not validate every row of the original location
#' archive. The deployment origin is never withheld.
#' As in reconstruction, ancillary fix alignment requires the canonical \code{datetime} column.
#'
#' For each holdout, the corresponding non-deployment anchor is removed before both position
#' correction and automatic VeDBA speed calibration. If \code{control$speed.method = "vedba"} and
#' no model is supplied, the model is fitted again using only the retained anchors, subject to the
#' usual calibration and constant-speed fallback rules. A supplied \code{control$vedba.model} or an
#' upstream paddle calibration is not refitted; such calibrations must be established independently
#' of the held-out fixes if an independent performance assessment is required.
#'
#' Previously reconstructed coordinates are not scored directly: the path is rebuilt from heading,
#' pitch and the selected speed channel in each fold. The endpoint reconstructability heuristic is
#' disabled for holdouts because the reduced anchor set is artificial. No processing-history records
#' from these temporary reconstructions are appended to the supplied datasets.
#'
#' A deployment with only an origin and pop-up can contribute one extrapolated endpoint error.
#' A deployment with no eligible non-origin anchor contributes no rows. Fold reconstruction failures
#' or non-finite predicted coordinates are omitted; the result therefore describes successful
#' holdouts, not every attempted fold. Per-deployment errors are reported when verbosity permits.
#'
#' ## Error and temporal gap
#'
#' \code{error_m} is the great-circle horizontal distance, in metres, between the predicted and
#' withheld positions. It includes effects of orientation error, speed assumptions, unmodelled
#' currents, position-measurement error and alignment to the sensor time grid. It does not identify
#' the contribution of each component.
#'
#' \code{gap_h} is the absolute time difference, in hours, to the nearest retained anchor, including
#' the deployment origin. It is not the full duration between bracketing anchors. The
#' \code{interpolated} indicator is \code{TRUE} when retained anchors occur both before and after
#' the holdout, and \code{FALSE} otherwise. These labels describe anchor availability; they do not
#' imply that a particular interpolation model was fitted.
#'
#' Gap and retained-anchor counts are reported even with \code{vpc.method = "none"}, when these
#' anchors do not correct the path. In that case, \code{gap_h} need not equal the time over which
#' uncorrected error has accumulated from the origin.
#'
#' ## Drift diagnostics
#'
#' Console and report summaries give median and 90th-percentile held-out error. Their descriptive
#' drift statistic is the median of \code{error_m / gap_h} for positive, finite gaps, in metres per
#' hour. Dividing by 3600 expresses this ratio in m/s, the units of \code{control$drift.rate}.
#'
#' The error-versus-gap plot also draws a least-squares line constrained through the origin when
#' at least two positive, finite gaps are available. Its slope is a different statistic from the
#' reported median ratio. Neither quantity is an automatic estimate of the physical drift process
#' or a fitted uncertainty model. A non-zero fix-error floor, anchor geometry and correction method
#' can all affect the apparent relationship.
#'
#' Use these diagnostics to inform comparisons of speed and correction settings, while inspecting
#' error by deployment, gap, fix quality and extrapolation status. They may motivate a study-specific
#' drift model, but do not validate the nominal \code{pseudo_error} scale returned by
#' [reconstructTrack()].
#'
#' ## Scientific interpretation
#'
#' Held-out fixes sample locations where a position was obtained; they may not represent long
#' submerged intervals or other unsampled behaviour. Closely spaced or temporally correlated fixes
#' can make single-fix holdouts easier to predict than long gaps. The uncertainty of the withheld
#' fix itself also contributes to \code{error_m}; \code{fix_radius_m} is its configured quality
#' radius, not a measured error for that observation.
#'
#' This implementation uses individual-fix holdouts, rather than block or deployment-level
#' validation. Wensveen et al. (2015) provide methodological context for held-out-position validation,
#' but the procedure here is not a reproduction of their cross-validation design.
#'
#' Comparing settings on the same holdouts is useful for development, but performance reported after
#' selecting settings on those errors is not an independent validation. Interpret results together
#' with calibration provenance and sampling coverage; retain a separate assessment dataset where
#' possible. A low held-out error does not establish the accuracy of the entire underwater path.
#'
#' ## Diagnostics and side effects
#'
#' The optional report contains an error-versus-gap scatter plot, coloured by interpolation or
#' extrapolation status, and a pooled empirical cumulative distribution of held-out errors.
#' The function does not save reconstructed datasets or a results table; save the returned data
#' frame explicitly when required. Supplied sensor data and processing history are unchanged.
#'
#' @return A data frame with one row per successfully scored holdout and the following columns:
#'   \describe{
#'     \item{\code{id}}{Deployment identifier.}
#'     \item{\code{datetime}}{Timestamp of the held-out anchor on the sensor time grid, as
#'       \code{POSIXct}.}
#'     \item{\code{quality}}{Anchor quality label, including \code{"Popup"} for metadata pop-ups.}
#'     \item{\code{error_m}}{Great-circle horizontal prediction error, in metres.}
#'     \item{\code{gap_h}}{Time to the nearest retained anchor, in hours.}
#'     \item{\code{interpolated}}{Whether retained anchors bracket the held-out timestamp.}
#'     \item{\code{fix_radius_m}}{Assumed error radius from \code{control$anchor.error.radii}, in
#'       metres; unrecognised quality labels use the reconstruction fallback of 1500 m.}
#'     \item{\code{n_anchors_used}}{Number of retained anchors available to the fold, including the
#'       deployment origin. They are not necessarily all used for positional correction.}
#'     \item{\code{speed_method}, \code{vpc_method}}{Speed and position-correction methods evaluated.}
#'   }
#'   Returns a zero-row data frame with these columns when no holdout can be scored.
#'
#' @references
#' Wensveen PJ, Thomas L, Miller PJO (2015) A path reconstruction method integrating dead-reckoning and
#' position fixes applied to humpback whales. \emph{Movement Ecology} 3:31.
#' \doi{10.1186/s40462-015-0061-6}
#'
#' @seealso [reconstructTrack()] for path reconstruction and its assumptions;
#'   [reconstructTrackControl()] for the settings evaluated; [filterLocations()] for location
#'   screening; [calibrateMagnetometer()] and [applyAxisMapping()] for upstream orientation checks.
#'
#' @examples
#' \dontrun{
#' # Use the same settings for reconstruction and held-out validation.
#' control <- reconstructTrackControl(speed.method = "constant", constant.speed = 0.6)
#' tracks <- reconstructTrack(processed, control = control)
#' cv <- crossValidateTrack(processed, control = control, plot.file = "./track_validation.pdf")
#'
#' # Compare positional correction methods with other settings held fixed.
#' methods <- c("none", "error_weighted", "scale_rotate")
#' comparison <- do.call(rbind, lapply(methods, function(method) {
#'   crossValidateTrack(processed,
#'                      control = reconstructTrackControl(constant.speed = 0.6,
#'                                                        vpc.method = method),
#'                      verbose = FALSE)
#' }))
#' if (nrow(comparison)) {
#'   aggregate(error_m ~ vpc_method, data = comparison, FUN = median)
#' }
#'
#' # Save the diagnostics explicitly; the function does not persist the results table.
#' write.csv(cv, "./track_validation.csv", row.names = FALSE)
#' }
#' @export
crossValidateTrack <- function(data,
                               control = reconstructTrackControl(),
                               id.col = "ID",
                               datetime.col = "datetime",
                               plot = FALSE,
                               plot.file = NULL,
                               verbose = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  control <- .as_control(control, reconstructTrackControl, "nautilus_reconstruct_track", "control")
  .assert_flag(plot, "plot"); .assert_writable_file(plot.file, "plot.file", ext = "pdf")
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")

  r <- .resolveInput(data, id.col = id.col)
  make_plots <- plot || !is.null(plot.file)
  .log_header(lvl, "crossValidateTrack", "Held-out fix cross-validation",
              bullets = sprintf("Input: %d dataset%s", r$n, if (r$n != 1) "s" else ""),
              arrow = sprintf("%s DR + %s VPC", control$speed.method, control$vpc.method))

  out <- vector("list", r$n)
  pb <- .log_progress_start(lvl, r$n, "Cross-validating", min.level = 1L, max.level = 1L)   # NORMAL only (detailed streams)
  for (i in seq_len(r$n)) {
    .log_progress_step(pb)
    x <- r$get(i)
    if (!data.table::is.data.table(x)) x <- data.table::as.data.table(x)
    who <- tryCatch(as.character(unique(x[[id.col]])[1]), error = function(e) NA_character_)
    if (length(who) != 1L || is.na(who) || !nzchar(who)) who <- r$ids[i]
    .log_h2(lvl, sprintf("%s (%d/%d)", who, i, r$n))

    tab <- tryCatch(.crossValidateOne(x, control, datetime.col, who),
                    error = function(e) { .log_skip(lvl, conditionMessage(e)); NULL })
    if (is.null(tab) || !nrow(tab)) {
      .log_detail(lvl, "no fixes to withhold (needs a genuine fix or pop-up beyond the deployment origin)")
    } else {
      out[[i]] <- tab
      .log_detail(lvl, sprintf("%d fix%s validated - median error %.0f m", nrow(tab),
                               if (nrow(tab) != 1) "es" else "", stats::median(tab$error_m, na.rm = TRUE)))
    }
    .log_gap(lvl); rm(x)
  }
  .log_progress_done(pb)

  kept <- Filter(Negate(is.null), out)
  res <- if (length(kept)) as.data.frame(data.table::rbindlist(kept)) else
    data.frame(id = character(), datetime = as.POSIXct(character()), quality = character(), error_m = numeric(),
               gap_h = numeric(), interpolated = logical(), fix_radius_m = numeric(), n_anchors_used = integer(),
               speed_method = character(), vpc_method = character(), stringsAsFactors = FALSE)

  if (lvl >= 1L) {
    .log_summary(lvl)
    if (nrow(res)) {
      gp <- res$gap_h > 0 & is.finite(res$gap_h)
      drift <- if (any(gp)) stats::median(res$error_m[gp] / res$gap_h[gp], na.rm = TRUE) else NA_real_
      .log_done(lvl, sprintf("%d fix%s cross-validated across %d deployment%s", nrow(res),
                             if (nrow(res) != 1) "es" else "", length(unique(res$id)),
                             if (length(unique(res$id)) != 1) "s" else ""))
      .log_arrow(lvl, sprintf("median error %.0f m (90th pct %.0f m) - drift ~ %.0f m/h (drift.rate ~ %.2f m/s)",
                              stats::median(res$error_m), stats::quantile(res$error_m, 0.9, names = FALSE),
                              drift, drift / 3600))
    } else {
      .log_done(lvl, "no fixes available for cross-validation")
    }
    .log_runtime(lvl, start.time)
  }

  if (make_plots && nrow(res)) {
    draw <- function(to.file = FALSE, unicode = TRUE) .drawCrossValidation(res, control)
    .renderToDevices(draw, plot = plot, plot.file = plot.file, width = 10, height = 6)
  }
  res
}
