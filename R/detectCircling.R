#######################################################################################################
# Candidate circling events from sustained heading rotations #########################################
#######################################################################################################

#' Detect sustained heading rotations
#'
#' @description
#' Identifies candidate circling events from consecutive, predominantly same-direction changes in
#' body heading. Each event is returned as one row with its timing, direction, net rotations and
#' angular-rate diagnostics. Optional sensor variables can be summarised over the event.
#'
#' The function is applied after [processTagData()]. It does not require dive annotations or a
#' reconstructed track, alter the sensor datasets, or write deployment exclusions. Detected heading
#' rotations are kinematic candidates, not proof of closed spatial paths or a particular behaviour.
#'
#' @param data A processed tag dataset, a list of datasets, a data frame containing deployments
#'   identified by `ID`, or a character vector of `.rds` paths. File inputs are read sequentially.
#'   Each deployment requires `ID`, `datetime` (`POSIXct`) and `heading` in degrees, clockwise from
#'   north. Default posture screening also requires `pitch` in degrees.
#' @param min.rotations Minimum absolute net heading change, expressed in complete rotations
#'   (default `2`, at least 720 degrees). Equality is eligible; fractional values are accepted.
#' @param min.directionality Minimum absolute net angular change divided by total absolute angular
#'   change (default `0.80`, in `(0, 1]`). A value of 0.80 permits 10 percent of the angular movement
#'   to oppose the dominant direction. This is a geometric measure, not a confidence probability.
#' @param smooth.window Centred, time-weighted moving-average window for unwrapped heading, in
#'   seconds (default `5`). Only fully supported windows are assessed. Set `0` to disable smoothing.
#' @param min.turn.rate Angular-rate deadband in degrees/second (default `0.5`). Changes below this
#'   magnitude do not actively support a turning direction. Set `0` to disable the deadband.
#' @param max.turn.rate Optional maximum absolute unsmoothed angular rate in degrees/second.
#'   Steps exceeding it break the record before smoothing. Default `NULL` applies no upper cutoff.
#' @param max.pause Maximum unsupported interval within an event, in seconds (default `5`). Short
#'   pauses or opposite-direction changes may be retained, but their angular changes still contribute
#'   to net rotations and directionality. Longer interruptions end the event at its last supported step.
#' @param max.gap Maximum elapsed time between adjacent observations, in seconds. With `NULL`
#'   (default), each deployment uses 1.5 times its median positive sampling interval. Longer gaps
#'   break the record; missing heading or required pitch is never interpolated across.
#' @param max.abs.pitch Maximum absolute pitch eligible for assessment, in degrees (default `80`).
#'   Where recorded, an applied processing pitch offset is added back for this screen. Set `NULL`
#'   to explicitly disable posture screening, including for heading-only inputs.
#' @param variables Optional character vector of numeric channels to summarise over each event.
#'   Default `NULL` adds no covariate columns. Missing channels produce typed `NA` summaries.
#' @param circular.variables Channels summarised by circular mean angle and mean resultant length,
#'   following [diveMetrics()] (default `c("heading", "roll")`). Only requested variables are used.
#' @param statistics Linear covariate statistics: `"mean"`, `"sd"`, or both (default).
#' @param verbose Console detail: `0`/`"quiet"`, `1`/`"normal"` (default), or `2`/`"detailed"`.
#'
#' @details
#' ## Angular calculations and event boundaries
#'
#' Observations are ordered by timestamp without modifying the input. Duplicate, missing or
#' non-finite timestamps make a deployment unassessable. Heading is wrapped modulo 360; successive
#' differences use the shortest signed arc in `[-180, 180)`. Exact half-turn steps are ambiguous
#' and break the record. True movement between adjacent samples must be less than 180 degrees:
#' rotations lost through insufficient sampling cannot be recovered from wrapped heading.
#'
#' Heading is unwrapped separately within each valid block, then smoothed using elapsed time rather
#' than sample counts. Angular rates are differences of the smoothed heading divided by actual
#' elapsed seconds. The existing `turning_angle` channel is not used. A constant magnetic declination
#' offset cancels; geographic north is therefore not required for this relative analysis. Optional
#' mean-heading covariates retain their original north reference and warn when recorded as magnetic.
#'
#' Sustained opposite turning starts a separate candidate. Net rotations and directionality include
#' all angular changes between the retained endpoints, including tolerated short reversals. Events
#' meeting both thresholds are retained. Censoring indicates that a retained endpoint coincides with
#' an assessment-block boundary, including record boundaries and smoothing support limits; unseen
#' rotations are never inferred. Events are detected across the deployment, not independently per dive.
#'
#' ## Quality control and interpretation
#'
#' Near-vertical forward-axis azimuth is poorly defined. The pitch screen is an abstention rule,
#' not a correction for gimbal lock or faulty orientation. Recorded processing offsets permit an
#' approximate return to the pitch frame used for heading estimation; without that provenance the
#' supplied pitch is used as-is. Finite but poorly calibrated heading can still produce artefacts.
#' Recorded raw, uncalibrated magnetometer use raises a warning without automatically rejecting data.
#'
#' Defaults are explicit, taxon-agnostic heuristics, not a validated ecological classifier or an exact
#' replication of published circling definitions. Slow circling may require a smaller deadband;
#' rapid circling may require shorter smoothing and finer sampling. Review representative heading,
#' depth and video records and assess threshold sensitivity before ecological interpretation.
#'
#' ## Integration and assessment coverage
#'
#' Every result, including a zero-event table, retains valid assessment windows in the
#' `assessed_intervals` attribute (`ID`, `start`, `end`) and deployment diagnostics in
#' `circling_detection`. These distinguish no detected event from unavailable evidence. Save with
#' [base::saveRDS()] to retain attributes; CSV does not preserve them.
#'
#' Supply the table directly to [plotDepthProfiles()] or [plotTracks()] through `events`, or to
#' [annotateData()] through `annotations`. The latter recognises assessment provenance and assigns
#' `NA` outside assessed intervals. Matching is by deployment and time; an event can overlap multiple
#' dives or phases. Reconstructed loops share heading information with detection and are not independent
#' validation. Covariate means and standard deviations are sample-based, not time-budget estimators.
#'
#' @return A `nautilus_circling_events` data frame, with one row per retained event and columns:
#'   \describe{
#'     \item{`ID`, `event`, `circling_id`}{Deployment, event type (`"circling"`) and integer event
#'       identifier, unique within a deployment.}
#'     \item{`start`, `end`, `duration_s`}{Observed endpoints (`POSIXct`, displayed in UTC) and elapsed
#'       duration in seconds.}
#'     \item{`direction`}{`"clockwise"` for increasing heading; `"counterclockwise"` for decreasing heading.}
#'     \item{`n_rotations`, `directionality`}{Absolute net angular change divided by 360 degrees,
#'       and absolute net change divided by total absolute change.}
#'     \item{`mean_turn_rate_deg_s`, `rotation_period_s`, `turn_rate_cv`}{Signed net rate, duration per
#'       net rotation, and time-weighted population standard deviation of signed rates divided by
#'       absolute mean rate. Rate variability is reported, not independently thresholded.}
#'     \item{`censored`}{Logical; either endpoint coincides with its assessment-block boundary.}
#'   }
#'   Requested linear covariates add `<variable>_mean` and/or `<variable>_sd`; circular covariates add
#'   `<variable>_mean_angle` and `<variable>_mrl`. The `circling_detection` attribute records method
#'   version, parameters and deployment assessment status, duration, event count and reason.
#'   Unassessable deployments warn and have `NA` event counts, not biological zero counts.
#'
#' @seealso [processTagData()], [annotateData()], [plotDepthProfiles()], [plotTracks()], [diveMetrics()]
#' @examples
#' # Three steady heading rotations; no files, calibration or track reconstruction needed
#' t <- 0:180
#' tag <- data.frame(ID = "example", datetime = as.POSIXct("2023-01-01", tz = "UTC") + t,
#'                   heading = (6 * t) %% 360, pitch = 0, depth = 20)
#' events <- detectCircling(tag, variables = "depth", verbose = "quiet")
#' events[, c("ID", "direction", "n_rotations", "directionality")]
#' labelled <- annotateData(tag, events, verbose = "quiet")
#' table(labelled[[1]]$circling, useNA = "ifany")
#' @export
detectCircling <- function(data,
                           min.rotations      = 2,
                           min.directionality = 0.80,
                           smooth.window      = 5,
                           min.turn.rate      = 0.5,
                           max.turn.rate      = NULL,
                           max.pause          = 5,
                           max.gap            = NULL,
                           max.abs.pitch      = 80,
                           variables          = NULL,
                           circular.variables = c("heading", "roll"),
                           statistics         = c("mean", "sd"),
                           verbose            = "normal") {
  started <- Sys.time(); lvl <- .verbosity(verbose)
  .assert_number(min.rotations, "min.rotations", min = 0)
  .assert_number(min.directionality, "min.directionality", min = 0, max = 1)
  if (min.rotations == 0 || min.directionality == 0)
    .abort("{.arg min.rotations} and {.arg min.directionality} must be greater than zero.")
  for (nm in c("smooth.window", "min.turn.rate", "max.pause"))
    .assert_number(get(nm), nm, min = 0)
  for (nm in c("max.turn.rate", "max.gap")) {
    value <- get(nm); .assert_number(value, nm, min = 0, null_ok = TRUE)
    if (!is.null(value) && value == 0) .abort("{.arg {nm}} must be greater than zero or NULL.")
  }
  if (!is.null(max.turn.rate) && max.turn.rate < min.turn.rate)
    .abort("{.arg max.turn.rate} must be at least {.arg min.turn.rate}.")
  .assert_number(max.abs.pitch, "max.abs.pitch", min = 0, max = 90, null_ok = TRUE)
  if (!is.null(max.abs.pitch) && max.abs.pitch >= 90)
    .abort("{.arg max.abs.pitch} must be less than 90 degrees or NULL.")
  for (nm in c("variables", "circular.variables")) {
    value <- get(nm)
    if (!is.null(value) && (!is.character(value) || anyNA(value) || any(!nzchar(value))))
      .abort("{.arg {nm}} must contain non-missing column names or be NULL.")
  }
  variables <- unique(variables)
  statistics <- match.arg(statistics, c("mean", "sd"), several.ok = TRUE)
  pars <- list(min.rotations = min.rotations, min.directionality = min.directionality,
               smooth.window = smooth.window, min.turn.rate = min.turn.rate,
               max.turn.rate = max.turn.rate, max.pause = max.pause, max.gap = max.gap,
               max.abs.pitch = max.abs.pitch, variables = variables,
               circular.variables = circular.variables, statistics = statistics)
  schema <- .circlingSchema(variables, circular.variables, statistics)
  src <- .resolveInput(data)
  .log_header(lvl, "detectCircling", "Detecting sustained heading rotations",
              bullets = sprintf("Input: %d deployments", src$n))
  rows <- reports <- windows <- vector("list", src$n)
  untrusted <- magnetic <- character(0); seen <- character(0)
  pb <- .log_progress_start(lvl, src$n, "Detecting")
  for (i in seq_len(src$n)) {
    .log_progress_step(pb)
    x <- src$get(i)
    id <- as.character(.getMeta(x)$id %||% src$ids[i])
    if (length(id) != 1L || is.na(id) || !nzchar(id)) .abort("Each dataset needs a non-missing deployment ID.")
    if (id %in% seen) .abort("Duplicate deployment {.val {id}} in {.arg data}; supply each deployment once.")
    seen <- c(seen, id)
    if (identical(.headingTrust(.getMeta(x)), "untrusted")) untrusted <- c(untrusted, id)
    .log_h2(lvl, sprintf("%s (%d/%d)", id, i, src$n))
    res <- .circlingOne(x, id, pars, schema)
    rows[[i]] <- res$events; windows[[i]] <- res$assessment; reports[[i]] <- res$report
    if (nrow(res$events) && "heading" %in% variables &&
        ("heading" %in% circular.variables || "mean" %in% statistics) &&
        identical(.headingReference(.getMeta(x)), "magnetic")) magnetic <- c(magnetic, id)
    if (identical(res$report$status, "not_assessed")) .log_skip(lvl, res$report$reason)
    else .log_detail(lvl, sprintf("%d events; %.1f s assessed", nrow(res$events), res$report$assessed_duration_s))
  }
  .log_progress_done(pb)
  out <- do.call(rbind, c(list(schema), rows)); rownames(out) <- NULL
  report <- do.call(rbind, reports); rownames(report) <- NULL
  attr(out, "circling_detection") <- list(method_version = 1L, parameters = pars, deployments = report)
  attr(out, "assessed_intervals") <- do.call(rbind, c(list(.emptyEventIntervals()), windows))
  attr(out, "event_types") <- "circling"
  class(out) <- c("nautilus_circling_events", "data.frame")
  missing <- report$status == "not_assessed"
  .warn_grouped("Some deployments could not be assessed for circling.",
                if (any(missing)) paste0(report$ID[missing], ": ", report$reason[missing]) else character(0))
  .warn_grouped("Circling uses heading from an uncalibrated magnetometer; inspect candidate events.",
                untrusted, style = "inline")
  .warnMagneticHeading(magnetic, "mean", "Per-event mean heading")
  .log_summary(lvl)
  .log_done(lvl, sprintf("%d events; %d of %d deployments assessable", nrow(out), sum(!missing), src$n))
  .log_note(lvl, "Candidate heading rotations are not independently verified spatial circles or behaviours.")
  .log_runtime(lvl, started)
  out
}
