#######################################################################################################
# Fill the gaps in a paddle-wheel speed-calibration table #############################################
#######################################################################################################

#' Fill the gaps in a paddle-wheel speed-calibration table
#'
#' @description
#' A magnetic paddle-wheel tag measures swimming speed from how fast the wheel turns, converted through
#' a calibration slope that is specific to the physical tag: `speed = slope x frequency`, with no
#' intercept. [processTagData()] reads those slopes from the table you pass it.
#'
#' In practice only some tags are ever calibrated, and rarely in every year they were deployed, so the
#' table has holes - and a deployment whose slope is missing gets no speed at all. Paddle wheels also
#' wear: efficiency declines with age, so the slope rises over time, which means a slope measured three
#' years earlier is not the slope that applied.
#'
#' This function fills the gaps. It learns how the slope drifts with time from the calibrations you do
#' have, projects a value for every deployed tag-year, and returns a complete table ready to hand
#' straight to [processTagData()], with each row labelled by where its number came from.
#'
#' @param calibration A table of measured calibrations, one row per calibration, carrying at least the
#'   tag identifier, the calibration year and the slope.
#' @param deployments A table of deployments, such as your animal metadata, giving the tag identifier
#'   and year of every deployment a slope is needed for.
#' @param id.col,year.col,slope.col Which columns hold the tag identifier, the year and the calibration
#'   slope. Defaults `"package_id"`, `"year"` and `"slope"`.
#' @param paddle.col A logical column in `deployments` marking the paddle-wheel tags, so that only those
#'   rows are kept. Default `"paddle_wheel"`, which is a no-op with a warning if the column is absent.
#'   `NULL` uses every deployment.
#' @param weights.col An optional column in `calibration` used to weight the fit, so that poorer
#'   calibrations count for less - an r-squared column, for instance. `NULL` (default) weights every
#'   calibration equally.
#' @param method Where the degradation rate comes from: `"shared-rate"` (default) pools one rate across
#'   every tag calibrated more than once, `"fixed-rate"` uses the `degradation.rate` you supply, and
#'   `"per-tag"` fits a separate line to each tag with at least two calibrations and then uses the
#'   average of those per-tag rates for every other tag. Prefer the default unless you have long, clean
#'   series per tag: `"per-tag"` is more flexible but unstable on the few points a calibration record
#'   usually holds, and averaging its rates gives every tag equal weight however little data it rests
#'   on.
#' @param degradation.rate The annual increase in slope. Required for `method = "fixed-rate"`; for the
#'   other methods it is an optional fallback, used only when the data cannot support an estimate.
#' @param keep.measured Whether a measured calibration is returned exactly as measured (default `TRUE`)
#'   rather than replaced by the model's fitted value. Leave it on unless you specifically want a
#'   smoothed series.
#' @param non.negative.rate Whether to floor a negative estimated degradation rate at zero (default
#'   `TRUE`), on the assumption that a paddle wheel does not become more efficient with age. A warning
#'   is raised when this bites, which is itself a sign the calibrations are too noisy to carry a trend.
#' @param slope.range An optional `c(min, max)` clamping imputed slopes to a physically plausible band,
#'   with a warning saying how many were clamped. Measured slopes are left exactly as measured, whether
#'   or not they fall inside the band - it constrains what the model may invent, not what you observed.
#'   `NULL` disables clamping. Set it from the range your tags have historically produced, so that a
#'   projection cannot wander somewhere impossible.
#' @param max.extrapolation An optional number of years beyond a tag's own calibration range after which
#'   a projection is warned about. `NULL` disables the warning. It does not stop the projection; it
#'   tells you which rows to distrust.
#' @param verbose How much detail to print: `0`/`"quiet"`, `1`/`"normal"`, or `2`/`"detailed"`
#'   (default).
#'
#' @details
#' ## How the drift is modelled
#'
#' Degradation is treated as a linear drift in the slope over calendar time. The default
#' `method = "shared-rate"` fits one fixed-slope, per-tag-intercept model to the measured calibrations:
#' a single annual degradation rate pooled across every tag calibrated more than once, plus a baseline
#' level for each tag. Each tag is then projected forward from its own observed level using the shared
#' rate, and a tag that was never calibrated starts from the global baseline.
#'
#' Pooling the rate while keeping the levels separate is what makes this stable. Fitting an independent
#' line to each tag would respect its measurements more closely, but calibration series are short, and a
#' rate estimated from two or three points is mostly noise.
#'
#' ## Knowing which numbers to trust
#'
#' The `slope_source` column records where each row's value came from: `"measured"` for a real
#' calibration, `"tag-model"` for a tag with at least one calibration of its own, and `"baseline"` for a
#' tag with none.
#'
#' At the sample sizes typical of calibration data, this projection cannot manufacture certainty. What
#' it does is make the table consistent, reproducible, checked and labelled. Treat heavily extrapolated
#' and baseline slopes as approximate, and remember that a speed error scales the whole reconstructed
#' track rather than distorting its shape.
#'
#' @return A data frame with one row per deployed tag-year and columns `year`, `package_id` and `slope`,
#'   usable directly as `processTagData(paddle.calibration = )`, plus `slope_source` and
#'   `n_calibrations`, the number of measured calibrations available for that tag. The estimated
#'   degradation rate, its standard error and the method used are attached as attributes
#'   (`degradation.rate`, `degradation.rate.se`, `method`).
#'
#' @seealso [processTagData()] for the function that consumes the table; [reconstructTrack()] for what
#'   the resulting speed feeds into.
#'
#' @examples
#' cal <- data.frame(package_id = c("51", "51", "52", "91"),
#'                   year = c(2019, 2021, 2019, 2021),
#'                   slope = c(0.074, 0.088, 0.072, 0.096))
#' dep <- data.frame(package_id = c("51", "52", "91", "134"),
#'                   year = c(2022, 2021, 2021, 2023),
#'                   paddle_wheel = TRUE)
#' imputePaddleCalibration(cal, dep, verbose = FALSE)
#' @export

imputePaddleCalibration <- function(calibration,
                                  deployments,
                                  id.col = "package_id",
                                  year.col = "year",
                                  slope.col = "slope",
                                  paddle.col = "paddle_wheel",
                                  weights.col = NULL,
                                  method = c("shared-rate", "fixed-rate", "per-tag"),
                                  degradation.rate = NULL,
                                  keep.measured = TRUE,
                                  non.negative.rate = TRUE,
                                  slope.range = NULL,
                                  max.extrapolation = NULL,
                                  verbose = "detailed") {

  ##############################################################################
  # Validate arguments #########################################################
  ##############################################################################

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  method <- match.arg(method)

  .assert_string(id.col, "id.col"); .assert_string(year.col, "year.col"); .assert_string(slope.col, "slope.col")
  .assert_string(paddle.col, "paddle.col", null_ok = TRUE)
  .assert_string(weights.col, "weights.col", null_ok = TRUE)
  .assert_flag(keep.measured, "keep.measured"); .assert_flag(non.negative.rate, "non.negative.rate")
  if (!is.null(degradation.rate)) .assert_number(degradation.rate, "degradation.rate")
  if (!is.null(max.extrapolation)) .assert_number(max.extrapolation, "max.extrapolation", min = 0)
  if (!is.null(slope.range) &&
      (!is.numeric(slope.range) || length(slope.range) != 2L || anyNA(slope.range) || slope.range[1] >= slope.range[2])) {
    .abort("{.arg slope.range} must be a numeric vector {.code c(min, max)} with min < max.")
  }
  if (method == "fixed-rate" && is.null(degradation.rate)) {
    .abort("{.arg method = \"fixed-rate\"} requires {.arg degradation.rate} to be supplied.")
  }

  if (data.table::is.data.table(calibration)) calibration <- as.data.frame(calibration)
  if (data.table::is.data.table(deployments)) deployments <- as.data.frame(deployments)
  .assert_columns(calibration, c(id.col, year.col, slope.col), "calibration")
  .assert_columns(deployments, c(id.col, year.col), "deployments")
  if (!is.null(weights.col)) .assert_columns(calibration, weights.col, "calibration")

  ##############################################################################
  # Normalise the measured calibrations ########################################
  ##############################################################################

  # A FACTOR year/slope used to become its integer LEVEL CODES. Because both columns then held the same
  # small codes, the shared-rate fit below became the IDENTITY (slope ~ year, rate 1.0/yr) and predicting
  # at a real deployment year returned that year AS THE SLOPE - 2020 instead of 0.35. The `bad` filter
  # further down cannot catch it: a factor coerces cleanly to a code, so nothing is ever NA.
  calibration <- .coerceNumericCols(calibration, c(year.col, slope.col, weights.col), "calibration")
  cal <- data.frame(
    package_id = as.character(calibration[[id.col]]),
    year       = .asNumericSafe(calibration[[year.col]]),
    slope      = .asNumericSafe(calibration[[slope.col]]),
    w          = if (!is.null(weights.col)) .asNumericSafe(calibration[[weights.col]]) else 1,
    stringsAsFactors = FALSE)
  cal$w[is.na(cal$w) | cal$w <= 0] <- 1                      # missing/invalid weights -> neutral

  bad <- is.na(cal$package_id) | !nzchar(cal$package_id) | is.na(cal$year) | is.na(cal$slope)
  if (any(bad)) {
    cli::cli_warn("Dropped {sum(bad)} calibration row{?s} with a missing id, year or slope.")
    cal <- cal[!bad, , drop = FALSE]
  }
  if (!nrow(cal)) .abort("No usable rows remain in {.arg calibration} after dropping missing id / year / slope.")

  # collapse repeat calibrations of the same tag-year into a single (weighted) value
  key <- paste(cal$package_id, cal$year)
  if (anyDuplicated(key)) {
    n_dup <- sum(duplicated(key))
    cal <- do.call(rbind, lapply(split(cal, key), function(d) data.frame(
      package_id = d$package_id[1], year = d$year[1],
      slope = stats::weighted.mean(d$slope, d$w), w = sum(d$w), stringsAsFactors = FALSE)))
    rownames(cal) <- NULL
    cli::cli_warn("Averaged {n_dup} duplicate tag-year calibration{?s} (same {.field {id.col}} and {.field {year.col}}).")
  }

  ##############################################################################
  # Build the deployment grid to fill ##########################################
  ##############################################################################

  deployments <- .coerceNumericCols(deployments, year.col, "deployments")
  dep <- data.frame(package_id = as.character(deployments[[id.col]]),
                    year = .asNumericSafe(deployments[[year.col]]),
                    stringsAsFactors = FALSE)
  if (!is.null(paddle.col)) {
    if (paddle.col %in% names(deployments)) {
      flag <- suppressWarnings(as.logical(deployments[[paddle.col]])); flag[is.na(flag)] <- FALSE
      dep <- dep[flag, , drop = FALSE]
    } else {
      cli::cli_warn(c("No {.field {paddle.col}} column in {.arg deployments}; using every deployment.",
                      "i" = "Pass {.code paddle.col = NULL} to silence this, or name the paddle-wheel flag column."))
    }
  }
  dep <- unique(dep[!is.na(dep$package_id) & nzchar(dep$package_id) & !is.na(dep$year), , drop = FALSE])
  if (!nrow(dep)) .abort("No deployments to calibrate (after the {.arg paddle.col} filter and dropping missing id / year).")

  n_tags <- length(unique(dep$package_id))
  n_per_tag <- table(cal$package_id)
  multi <- names(n_per_tag)[n_per_tag >= 2L]

  .log_header(lvl, "imputePaddleCalibration", "Filling paddle-wheel speed-calibration slopes",
              bullets = c(sprintf("Input: %d deployment%s across %d tag%s", nrow(dep), if (nrow(dep) != 1) "s" else "",
                                  n_tags, if (n_tags != 1) "s" else ""),
                          sprintf("Calibrations: %d measured across %d tag%s (%d with repeats)",
                                  nrow(cal), length(n_per_tag), if (length(n_per_tag) != 1) "s" else "", length(multi))),
              arrow = sprintf("Method: %s", method))

  ##############################################################################
  # Estimate the annual degradation rate #######################################
  ##############################################################################

  rate <- NA_real_; rate_se <- NA_real_
  if (method == "fixed-rate") {
    rate <- degradation.rate

  } else if (method == "shared-rate") {
    # the year coefficient is only estimable when at least one tag spans >= 2 distinct years
    fit_ok <- length(multi) >= 1L &&
      any(vapply(multi, function(t) length(unique(cal$year[cal$package_id == t])) >= 2L, logical(1)))
    if (fit_ok) {
      # per-tag intercepts need a >= 2-level factor; with a single calibrated tag drop the factor term
      form <- if (length(unique(cal$package_id)) >= 2L) slope ~ year + factor(package_id) else slope ~ year
      fit <- stats::lm(form, data = cal, weights = cal$w)
      co  <- stats::coef(summary(fit))
      rate <- co["year", "Estimate"]; rate_se <- co["year", "Std. Error"]
    } else {
      rate <- degradation.rate %||% 0.01
      cli::cli_warn(c("Too few repeat calibrations to estimate a degradation rate.",
                      "i" = "Using {.val {rate}} per year - supply {.arg degradation.rate} or more calibrations."))
    }

  } else {  # per-tag (legacy): average of the per-tag two-point-or-more slopes
    es <- vapply(multi, function(t) {
      d <- cal[cal$package_id == t, , drop = FALSE]
      stats::coef(stats::lm(slope ~ year, data = d, weights = d$w))[["year"]]
    }, numeric(1))
    es <- es[is.finite(es)]
    rate <- if (length(es)) mean(es) else (degradation.rate %||% 0.01)
  }

  if (non.negative.rate && is.finite(rate) && rate < 0) {
    cli::cli_warn("Estimated degradation rate was negative ({.val {round(rate, 5)}}/year); flooring at 0 (efficiency assumed not to improve with age).")
    rate <- 0
  }

  # per-tag baseline levels (de-trended) and the global baseline for never-calibrated tags. For the
  # shared-slope model these de-trend means are exactly the per-tag intercepts.
  detrend <- cal$slope - rate * cal$year
  tag_level <- vapply(split(seq_len(nrow(cal)), cal$package_id),
                      function(ix) stats::weighted.mean(detrend[ix], cal$w[ix]), numeric(1))
  global_level <- mean(tag_level)
  ref_year <- round(stats::median(cal$year))

  ##############################################################################
  # Fill every deployment tag-year #############################################
  ##############################################################################

  meas <- stats::setNames(cal$slope, paste(cal$package_id, cal$year))
  yr_range <- lapply(split(cal$year, cal$package_id), range)
  global_range <- range(cal$year)

  fill_one <- function(pid, yr) {
    k <- paste(pid, yr)
    if (keep.measured && k %in% names(meas)) return(c(meas[[k]], NA))    # source code 0 = measured
    if (method == "per-tag" && pid %in% multi) {
      d <- cal[cal$package_id == pid, , drop = FALSE]
      sl <- as.numeric(stats::predict(stats::lm(slope ~ year, data = d, weights = d$w), newdata = data.frame(year = yr)))
      return(c(sl, 1))                                                   # 1 = tag-model
    }
    if (pid %in% names(tag_level)) return(c(tag_level[[pid]] + rate * yr, 1))
    c(global_level + rate * yr, 2)                                       # 2 = baseline
  }

  out <- dep[order(dep$package_id, dep$year), , drop = FALSE]
  filled <- mapply(fill_one, out$package_id, out$year)
  out$slope <- as.numeric(filled[1, ])
  src_code <- filled[2, ]
  out$slope_source <- ifelse(is.na(src_code), "measured", ifelse(src_code == 1, "tag-model", "baseline"))
  out$n_calibrations <- as.integer(n_per_tag[out$package_id]); out$n_calibrations[is.na(out$n_calibrations)] <- 0L

  # clamp imputed slopes to the plausible band (measured values are flagged, never silently clamped)
  imputed <- out$slope_source != "measured"
  if (!is.null(slope.range)) {
    hit <- imputed & (out$slope < slope.range[1] | out$slope > slope.range[2])
    if (any(hit)) {
      out$slope[hit] <- pmin(pmax(out$slope[hit], slope.range[1]), slope.range[2])
      cli::cli_warn("Clamped {sum(hit)} imputed slope{?s} to the {.arg slope.range} band [{slope.range[1]}, {slope.range[2]}].")
    }
  }
  if (any(out$slope <= 0, na.rm = TRUE)) {
    cli::cli_warn("{sum(out$slope <= 0, na.rm = TRUE)} filled slope{?s} are <= 0 (not physically plausible); inspect or set {.arg slope.range}.")
  }

  # warn on heavy extrapolation beyond each tag's calibration range
  if (!is.null(max.extrapolation)) {
    ext <- mapply(function(pid, yr, imp) {
      if (!imp) return(0)
      rg <- if (pid %in% names(yr_range)) yr_range[[pid]] else global_range
      max(0, yr - rg[2], rg[1] - yr)
    }, out$package_id, out$year, imputed)
    if (any(ext > max.extrapolation)) {
      cli::cli_warn("{sum(ext > max.extrapolation)} slope{?s} projected more than {max.extrapolation} year{?s} beyond a tag's calibration range; treat as approximate.")
    }
  }

  ##############################################################################
  # Report + return ############################################################
  ##############################################################################

  if (lvl >= 2L) {
    rate_txt <- if (is.finite(rate_se)) sprintf("%.4f/year (SE %.4f)", rate, rate_se) else sprintf("%.4f/year", rate)
    .log_detail(lvl, "degradation rate: ", rate_txt)
    .log_detail(lvl, sprintf("baseline slope: %.4f at %d (for never-calibrated tags)", global_level + rate * ref_year, as.integer(ref_year)))
    n_meas <- sum(out$slope_source == "measured"); n_tm <- sum(out$slope_source == "tag-model"); n_bl <- sum(out$slope_source == "baseline")
    .log_detail(lvl, sprintf("filled: measured %d \u00b7 tag-model %d \u00b7 baseline %d", n_meas, n_tm, n_bl))
  }
  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, nrow(out), " tag-year slope", if (nrow(out) != 1) "s", " across ", n_tags, " tag", if (n_tags != 1) "s")
    .log_runtime(lvl, start.time)
  }

  out <- out[, c("year", "package_id", "slope", "slope_source", "n_calibrations")]
  rownames(out) <- NULL
  attr(out, "degradation.rate") <- rate
  attr(out, "degradation.rate.se") <- rate_se
  attr(out, "method") <- method
  out
}
