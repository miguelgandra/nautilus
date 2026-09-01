#######################################################################################################
# calculatePaddleSpeed() internals ####################################################################
#######################################################################################################

#' Validate a calibration table, or pass NULL straight through.
#' @keywords internal
#' @noRd
.assert_calibration <- function(calibration) {
  if (is.null(calibration)) return(NULL)
  if (data.table::is.data.table(calibration)) calibration <- as.data.frame(calibration)
  if (!is.data.frame(calibration)) .abort("{.arg calibration} must be a data frame.")
  miss <- setdiff(c("year", "package_id", "slope"), names(calibration))
  if (length(miss))
    .abort(c("{.arg calibration} is missing the column{?s} {.field {miss}}.",
             "i" = "It needs {.field year}, {.field package_id} and {.field slope}."))
  if (!is.numeric(calibration$year))  .abort("{.field year} in {.arg calibration} must be numeric.")
  if (!is.numeric(calibration$slope)) .abort("{.field slope} in {.arg calibration} must be numeric.")
  calibration
}

#' What one deployment carries, plus the sums the in-situ fit needs.
#'
#' The in-situ slope is a through-origin fit of vertical velocity on `paddle_freq * sin(pitch)`, so its
#' sums add across the deployments of one tag. Accumulating them here means the fit can be done per tag
#' without holding any deployment in memory.
#'
#' The sign flip is the depth convention: depth increases downwards, so a descending animal has a
#' positive vertical velocity and a negative (nose-down) pitch. Negating one of them makes the fitted
#' slope positive, as a speed per rotation should be.
#' @keywords internal
#' @noRd
.paddleScanOne <- function(dt, fallback_id, min.pitch, need.insitu) {
  meta <- .getMeta(.ensureMeta(dt))
  id  <- as.character(meta$id %||% fallback_id)
  pkg <- as.character(meta$tag$package_id %||% NA_character_)
  yr  <- suppressWarnings(as.integer(format(meta$deployment$datetime %||% NA, "%Y")))
  if (!is.finite(yr) && "datetime" %in% names(dt) && nrow(dt))
    yr <- suppressWarnings(as.integer(format(dt$datetime[1], "%Y")))

  has_freq  <- "paddle_freq"  %in% names(dt) && any(is.finite(dt$paddle_freq))
  has_speed <- "paddle_speed" %in% names(dt) && any(is.finite(dt$paddle_speed))
  out <- list(id = id, pkg = pkg, year = yr,
              key = paste0(yr, "/", pkg),
              has_freq = has_freq, has_speed = has_speed,
              paddle_flag = isTRUE(meta$tag$paddle_wheel),
              # `fs` and `n_rows` turn a sample count into a duration and a share of the record, which
              # is what the viability floor tests and what the per-deployment log reports
              fs = .estimateHz(dt$datetime), n_rows = nrow(dt), pitch_iqr = NA_real_,
              Sxy = NA_real_, Sxx = NA_real_, Syy = NA_real_, n = 0L)

  if (!need.insitu || !has_freq) return(out)
  if (!all(c("pitch", "vertical_velocity") %in% names(dt))) return(out)
  ok <- is.finite(dt$paddle_freq) & is.finite(dt$pitch) & is.finite(dt$vertical_velocity) &
        abs(dt$pitch) >= min.pitch
  if (!any(ok)) return(out)
  xv <- dt$paddle_freq[ok] * sin(-dt$pitch[ok] * pi / 180)
  yv <- dt$vertical_velocity[ok]
  out$Sxy <- sum(xv * yv); out$Sxx <- sum(xv * xv); out$Syy <- sum(yv * yv); out$n <- sum(ok)
  ap <- abs(dt$pitch[ok])
  q <- stats::quantile(ap, c(0.25, 0.75), names = FALSE, na.rm = TRUE)
  out$pitch_iqr <- q[2] - q[1]
  out
}


#' The through-origin fit and its precision, from accumulated sums.
#'
#' One estimator for both scopes: the sums are a deployment's own, or a tag-season's pooled. Because
#' the fit is `Sxy / Sxx`, pooling weights each deployment by its own `Sxx` rather than treating every
#' deployment alike, so a four-minute record cannot outvote a thirty-hour one.
#' @keywords internal
#' @noRd
.paddleFit <- function(Sxy, Sxx, Syy, n) {
  ok <- is.finite(Sxy) && is.finite(Sxx) && Sxx > 0 && is.finite(n) && n > 0
  slope <- if (ok) Sxy / Sxx else NA_real_
  r  <- if (ok && is.finite(Syy) && Syy > 0) Sxy / sqrt(Sxx * Syy) else NA_real_
  se <- if (ok && n > 2 && is.finite(slope) && is.finite(Syy))
          sqrt(max(0, Syy - slope * Sxy) / ((n - 1) * Sxx)) else NA_real_
  list(slope = slope, se = se, r = r, n = as.integer(n))
}

#' A duration in words, spelling out the unit.
#'
#' `.fmt_duration()` abbreviates minutes to "m", which reads as metres on a line that also carries a
#' speed in m/s, so this block spells the unit out.
#' @keywords internal
#' @noRd
.paddleFmtSecs <- function(secs) {
  if (!is.finite(secs)) return("unknown")
  if (secs < 90) return(sprintf("%.0f s", secs))
  if (secs < 5400) return(sprintf("%.1f min", secs / 60))
  sprintf("%.1f h", secs / 3600)
}

#' The least steep swimming an in-situ fit may rest on, in seconds.
#' @keywords internal
#' @noRd
.paddleMinSeconds <- 60

#' Whether an in-situ fit rests on enough data to be used.
#'
#' Viability, not quality. Neither the fit's precision nor the spread of pitch angles behind it
#' predicts how close the estimate lands to a measured calibration - both were tested against the two
#' tag-seasons that have one, and neither separates an accurate estimate from an inaccurate one, since
#' the error is between-deployment bias rather than sampling noise. So this excludes only fits that are
#' arithmetically undefined or rest on less steep swimming than `.paddleMinSeconds`; everything else is
#' reported and left to the analyst.
#' @keywords internal
#' @noRd
.paddleViable <- function(fit, fs) {
  if (!is.finite(fit$slope) || is.na(fit$n) || fit$n <= 2L) return(FALSE)
  secs <- if (is.finite(fs) && fs > 0) fit$n / fs else NA_real_
  isTRUE(is.finite(secs) && secs >= .paddleMinSeconds)
}

#' Resolve the slope every deployment will use, and where it came from.
#'
#' Returns two tables. `cal` is the tag-season summary that travels on the result as the
#' `"calibration"` attribute; `dep` is one row per deployment, holding the slope actually applied and
#' its own in-situ fit, which is what the per-deployment log and the agreement check read.
#' @keywords internal
#' @noRd
.paddleResolve <- function(scan, calibration, method, degradation.rate, agreement.threshold, lvl) {
  keys <- unique(vapply(scan, function(z) z$key, character(1)))
  in_situ_method <- method %in% c("in-situ-deployment", "in-situ-pooled")

  ## ---- one fit per deployment: the raw material for the pooled fit, the fallback and the spread ---
  dep <- do.call(rbind, lapply(scan, function(z) {
    f <- .paddleFit(z$Sxy, z$Sxx, z$Syy, z$n)
    secs <- if (is.finite(z$fs) && z$fs > 0) f$n / z$fs else NA_real_
    data.frame(id = z$id, key = z$key,
               own_slope = f$slope, own_se = f$se, own_r = f$r, own_n = f$n,
               own_secs = secs, own_pct = 100 * f$n / max(1L, z$n_rows),
               pitch_iqr = z$pitch_iqr %||% NA_real_,
               own_viable = .paddleViable(f, z$fs), stringsAsFactors = FALSE)
  }))

  ## ---- one fit per tag and season, pooled over its deployments ------------------------------------
  cal <- do.call(rbind, lapply(keys, function(k) {
    w  <- Filter(function(z) identical(z$key, k), scan)
    hf <- vapply(w, function(z) z$has_freq, logical(1))
    hs <- vapply(w, function(z) z$has_speed, logical(1))
    f  <- .paddleFit(sum(vapply(w, function(z) z$Sxy %||% NA_real_, numeric(1)), na.rm = TRUE),
                     sum(vapply(w, function(z) z$Sxx %||% NA_real_, numeric(1)), na.rm = TRUE),
                     sum(vapply(w, function(z) z$Syy %||% NA_real_, numeric(1)), na.rm = TRUE),
                     sum(vapply(w, function(z) z$n, integer(1))))
    fs   <- stats::median(vapply(w, function(z) z$fs %||% NA_real_, numeric(1)), na.rm = TRUE)
    # between-deployment spread, over the deployments whose own fit is usable. Reported, never
    # flagged: nothing in these data establishes how much spread is too much, and the response to a
    # large CV is to look at the tag - which is also the response to the number itself.
    v  <- dep$own_slope[dep$key == k & dep$own_viable]
    rs <- dep$own_se[dep$key == k & dep$own_viable] / v
    data.frame(key = k, year = w[[1]]$year, package_id = w[[1]]$pkg,
               n_deployments = length(w),
               has_paddle = any(hf | hs | vapply(w, function(z) z$paddle_flag, logical(1))),
               # a slope only converts a rotation rate, so only a tag that recorded one has a use for
               # it; `as_recorded` separates "finished without a slope" from "nothing was recorded"
               needs_slope = any(hf),
               as_recorded = !any(hf) && any(hs),
               in_situ_slope = f$slope, in_situ_r = f$r,
               in_situ_lo = f$slope - 1.96 * f$se, in_situ_hi = f$slope + 1.96 * f$se,
               in_situ_n = f$n, in_situ_viable = .paddleViable(f, fs),
               slope_k = length(v),
               slope_cv = if (length(v) >= 2L) stats::sd(v) / mean(v) else NA_real_,
               slope_ratio = if (length(v) >= 2L) max(v) / min(v) else NA_real_,
               slope_rel_se = if (length(rs)) stats::median(rs, na.rm = TRUE) else NA_real_,
               stringsAsFactors = FALSE)
  }))

  ## ---- a calibration for this tag and season always wins ------------------------------------------
  cal$slope <- NA_real_; cal$slope_source <- NA_character_
  if (!is.null(calibration)) {
    m <- match(paste0(calibration$year, "/", as.character(calibration$package_id)), cal$key)
    ok <- !is.na(m)
    cal$slope[m[ok]] <- calibration$slope[ok]
    cal$slope_source[m[ok]] <- "calibrated"
  }

  ## ---- fill what is left, for the tags that have a rotation rate to convert -----------------------
  # Estimating a slope for a tag that never recorded a rotation rate manufactures a number that gets
  # reported in the calibration table and then never applied: the deployments that wrote a speed
  # directly are already finished, and the ones that recorded nothing cannot be rescued by a slope. A
  # real calibration is left alone even so - it is an observation, not a guess.
  gap <- is.na(cal$slope) & cal$needs_slope
  if (any(gap)) {
    if (in_situ_method) {
      # the pooled fit answers `in-situ-pooled` outright, and is the fallback for `in-situ-deployment`
      use <- gap & cal$in_situ_viable
      cal$slope[use] <- cal$in_situ_slope[use]
      cal$slope_source[use] <- "in-situ-pooled"
    } else if (!is.null(calibration)) {
      cal <- .paddleImputeGaps(cal, calibration, method, degradation.rate, gap, lvl)
    }
  }
  # left without a slope, but for a reason worth naming: the logger reported speed itself
  cal$slope_source[is.na(cal$slope) & cal$as_recorded] <- "as-recorded"

  ## ---- carry the tag-season answer down to each deployment ---------------------------------------
  m <- match(dep$key, cal$key)
  dep$slope <- cal$slope[m]; dep$slope_source <- cal$slope_source[m]

  # `in-situ-deployment` prefers a deployment's own fit, but only where the tag-season had no
  # calibration to apply: a real calibration is never displaced by an estimate.
  if (identical(method, "in-situ-deployment")) {
    fillable <- is.na(dep$slope_source) | dep$slope_source %in% "in-situ-pooled"
    own <- fillable & dep$own_viable
    dep$slope[own] <- dep$own_slope[own]
    dep$slope_source[own] <- "in-situ-deployment"
    # where slopes now vary within a tag-season, no single value describes the stratum; the pooled
    # estimate stays visible in `in_situ_slope` and the spread in `slope_cv`
    varies <- cal$key %in% unique(dep$key[own])
    cal$slope[varies] <- NA_real_
    cal$slope_source[varies] <- "in-situ-deployment"
  }

  ## ---- agreement: only where the applied slope did not come from the data it is compared against --
  independent <- cal$slope_source %in% c("calibrated", "projected-from-tag", "projected-from-fleet")
  cal$agreement <- ifelse(independent, cal$slope / cal$in_situ_slope, NA_real_)
  # `agreement.threshold` is a proportion, but agreement is a RATIO, so the band is applied
  # multiplicatively rather than as 1 +/- t. A slope 35% above the in-situ estimate and one 35% below it
  # are the same size of disagreement, and only 1/(1+t) .. (1+t) treats them that way; 1 - t would make
  # the low side stricter than the high side purely as an artefact of the arithmetic.
  hi <- 1 + agreement.threshold
  cal$flag <- is.finite(cal$agreement) & (cal$agreement > hi | cal$agreement < 1 / hi)

  # per deployment, the applied slope against this deployment's own diving. Independent for a
  # calibrated or projected slope; a heterogeneity check against a pooled one; circular, and so
  # withheld, where the slope IS this deployment's own fit.
  dep$agreement <- ifelse(dep$own_viable, dep$slope / dep$own_slope, NA_real_)
  dep$agreement[dep$slope_source %in% "in-situ-deployment"] <- NA_real_
  # a pooled fit with one viable contributor is that contributor's own fit, so the comparison is
  # circular too - the heterogeneity check needs someone else in the stratum to be a check at all
  sole <- dep$slope_source %in% "in-situ-pooled" & cal$slope_k[match(dep$key, cal$key)] <= 1L
  dep$agreement[sole %in% TRUE] <- NA_real_

  list(cal = cal[order(cal$year, cal$package_id), , drop = FALSE], dep = dep)
}

#' Fill the remaining gaps through imputePaddleCalibration(), keeping its provenance labels.
#' @keywords internal
#' @noRd
.paddleImputeGaps <- function(cal, calibration, method, degradation.rate, gap, lvl) {
  # `paddle_wheel` is passed explicitly: imputePaddleCalibration() uses it to skip tags that never had a
  # paddle, and warns when it is absent. This function already knows the answer from each tag's own
  # metadata, so there is nothing to guess at.
  need <- data.frame(package_id = cal$package_id[gap], year = cal$year[gap],
                     paddle_wheel = cal$has_paddle[gap], stringsAsFactors = FALSE)
  filled <- try(suppressMessages(
    imputePaddleCalibration(calibration = calibration, deployments = need, method = method,
                            degradation.rate = degradation.rate, verbose = 0)), silent = TRUE)
  if (inherits(filled, "try-error") || !is.data.frame(filled)) {
    cli::cli_warn(c("Could not estimate the missing calibration{?s} with {.val {method}}.",
                    "i" = "Those deployments get no speed. {.code method = \"in-situ-pooled\"}
                           estimates the slope from the deployments themselves."))
    return(cal)
  }
  k <- paste0(filled$year, "/", as.character(filled$package_id))
  m <- match(cal$key[gap], k)
  cal$slope[gap] <- filled$slope[m]
  src <- if ("slope_source" %in% names(filled)) filled$slope_source[m] else rep("imputed", length(m))
  cal$slope_source[gap] <- ifelse(is.na(cal$slope[gap]), NA_character_, src)
  cal
}

#' Apply one tag's slope to one deployment.
#'
#' Also returns what the per-deployment log needs - the row count, the sampling rate and the resulting
#' speed distribution - so the caller can report the deployment without walking the data a second time.
#' @keywords internal
#' @noRd
.paddleApplyOne <- function(dt, sc, row, smoothing, max.speed) {
  # every early return goes through out(), so the fields the per-deployment log reads are always present
  out <- function(data, status, slope = NA_real_, slope_source = NA_character_, speed = NULL)
    list(data = data, status = status, slope = slope, slope_source = slope_source,
         speed = speed, n_rows = nrow(data), fs = .estimateHz(data$datetime))

  if (!sc$has_freq) {
    if (sc$has_speed) {
      # the logger wrote a speed itself, so there is no rotation rate to calibrate: keep what it recorded
      return(out(dt, "as-recorded", slope_source = "as-recorded",
                 speed = .paddleSpeedStats(dt$paddle_speed)))
    }
    if (!"paddle_speed" %in% names(dt)) dt[, paddle_speed := NA_real_]
    return(out(dt, if (sc$paddle_flag) "no paddle data" else "no paddle wheel"))
  }

  if (!nrow(row) || !is.finite(row$slope)) {
    dt[, paddle_speed := NA_real_]
    return(out(dt, "no calibration"))
  }
  fq <- dt$paddle_freq
  slope <- row$slope
  fs <- .estimateHz(dt$datetime)      # also reported as the deployment's sampling rate
  if (!is.null(smoothing) && smoothing > 0 && nrow(dt) > 2) {
    k <- if (is.finite(fs)) max(1L, round(fs * smoothing)) else 1L
    if (k > 1L && k <= nrow(dt)) fq <- data.table::frollmean(fq, n = k, fill = NA, align = "center")
  }
  sp <- fq * slope
  if (!is.null(max.speed)) sp[is.finite(sp) & sp > max.speed / 3.6] <- NA_real_
  dt[, paddle_speed := sp]
  list(data = dt, status = "applied", slope = row$slope, slope_source = row$slope_source,
       speed = .paddleSpeedStats(sp), n_rows = nrow(dt), fs = fs)
}

#' Median and range of a speed column, or NULL when it holds nothing finite.
#' @keywords internal
#' @noRd
.paddleSpeedStats <- function(v) {
  if (is.null(v)) return(NULL)
  ok <- is.finite(v)
  if (!any(ok)) return(NULL)
  c(med = stats::median(v[ok]), lo = min(v[ok]), hi = max(v[ok]))
}

#' Where a slope came from, in words.
#'
#' Two registers for the same fact: the long form reads as prose in a per-deployment line, the short one
#' fits a table column. Keeping them in one place stops the two from drifting apart.
#' @keywords internal
#' @noRd
.paddleSourceLabel <- function(src, long = TRUE) {
  if (length(src) != 1L || is.na(src)) return(NA_character_)
  tab <- if (long)
    c(calibrated             = "calibrated",
      `projected-from-tag`   = "projected from this tag",
      `projected-from-fleet` = "projected from other tags",
      `in-situ-deployment`   = "in situ, this deployment",
      `in-situ-pooled`       = "in situ, pooled over the tag-season",
      `as-recorded`          = "as recorded")
  else
    c(calibrated             = "calibrated",
      `projected-from-tag`   = "from tag",
      `projected-from-fleet` = "from fleet",
      `in-situ-deployment`   = "in situ (deployment)",
      `in-situ-pooled`       = "in situ (pooled)",
      `as-recorded`          = "as recorded")
  if (src %in% names(tab)) unname(tab[src]) else "estimated"
}

#' One deployment's block: what came in, which slope was applied, and what speed came out.
#'
#' Mirrors the per-deployment layout used by calculateTailBeats(): an `input:` line, the setting that
#' actually varies between deployments (here the slope and its provenance), then the results indented
#' under it. A deployment that gets no speed prints the skip line only, as the tail-beat blocks do.
#' @keywords internal
#' @noRd
.logPaddleDeployment <- function(lvl, id, sc, res, dep_row = NULL) {
  if (lvl < 1L) return(invisible(NULL))
  # `\u00b7` separates facts WITHIN a line and the heavier bullet marks the skip line, as in
  # calculateTailBeats(): the two glyphs are doing different jobs and are not interchangeable.
  dot <- "\u00b7"
  if (!res$status %in% c("applied", "as-recorded")) {
    why <- switch(res$status,
                  "no calibration" = "no calibration for this tag and season",
                  "no paddle data" = "no paddle data",
                  "no paddle wheel" = "no paddle wheel", res$status)
    .log_skip(lvl, id, "  ", why, " ", cli::symbol$bullet, " skipped")
    return(invisible(NULL))
  }

  key <- if (!is.na(sc$pkg) && length(sc$year) == 1L && is.finite(sc$year))
    sprintf(" %s package %s %s %d", dot, sc$pkg, dot, sc$year) else ""
  hz <- if (is.finite(res$fs)) sprintf("%g Hz", res$fs) else "rate unknown"
  .log_detail(lvl, sprintf("input: %s rows %s %s%s", .formatLargeNumber(res$n_rows), dot, hz, key))

  if (identical(res$status, "as-recorded"))
    .log_detail(lvl, sprintf("calibration: not needed %s speed recorded by the logger", dot))
  else
    .log_detail(lvl, sprintf("calibration: %.4f m/s per Hz %s %s", res$slope, dot,
                             .paddleSourceLabel(res$slope_source, long = TRUE)))

  if (!is.null(res$speed))
    .log_subdetail_aligned(lvl, sprintf("speed:      median %.2f m/s (%.2f \u2013 %.2f)",
                                        res$speed[["med"]], res$speed[["lo"]], res$speed[["hi"]]))
  else
    .log_subdetail_aligned(lvl, "speed:      no finite values")

  # the in-situ lines appear only when an in-situ fit was asked for at all
  if (!is.null(dep_row) && nrow(dep_row) == 1L && isTRUE(dep_row$own_n > 0L)) {
    if (isTRUE(dep_row$own_viable) && is.finite(dep_row$agreement))
      .log_subdetail_aligned(lvl, sprintf("check:      in situ %.4f m/s per Hz %s agreement %.2f",
                                          dep_row$own_slope, dot, dep_row$agreement))
    else if (identical(res$slope_source, "in-situ-deployment"))
      .log_subdetail_aligned(lvl, "check:      not independent (slope came from this deployment)")
    else if (!isTRUE(dep_row$own_viable))
      .log_subdetail_aligned(lvl, "check:      not enough steep swimming to check")
    if (is.finite(dep_row$own_secs))
      .log_subdetail_aligned(lvl, sprintf("based on:   %s of steep swimming (%.1f%% of record)",
                                          .paddleFmtSecs(dep_row$own_secs), dep_row$own_pct))
  }
  .log_ok(lvl, id, " processed")
  invisible(NULL)
}

#' A compact provenance line for the calibration block.
#'
#' The full tag-season table travels on the result as the `"calibration"` attribute; printing it grew
#' past a console width and past the point of being readable, so the summary states how many tags rest
#' on each kind of slope and the range actually applied, and points at the attribute for the rest.
#' @keywords internal
#' @noRd
.paddleCalSummary <- function(cal, applied = NULL) {
  src <- cal$slope_source[!is.na(cal$slope_source)]
  parts <- if (length(src)) {
    tb <- table(src)
    ord <- c("calibrated", "projected-from-tag", "projected-from-fleet",
             "in-situ-deployment", "in-situ-pooled", "as-recorded")
    tb <- tb[order(match(names(tb), ord), names(tb))]
    paste(sprintf("%d %s", as.integer(tb),
                  vapply(names(tb), .paddleSourceLabel, character(1), long = FALSE,
                         USE.NAMES = FALSE)), collapse = ", ")
  } else NULL
  n_without <- sum(is.na(cal$slope_source))
  if (n_without) parts <- paste(c(parts, sprintf("%d without", n_without)), collapse = ", ")
  # the range of slopes actually APPLIED, taken per deployment: under `in-situ-deployment` the
  # tag-season row carries no single slope, so reading `cal` would under-report the spread
  a <- if (is.null(applied)) cal$slope else applied
  a <- a[is.finite(a)]
  list(tags = sprintf("%d (%s)", nrow(cal), parts %||% "none resolved"),
       range = if (length(a))
         sprintf("%.4f - %.4f m/s per Hz", min(a), max(a)) else NULL)
}

#' The SUMMARY block: the outcome tally, the cohort roll-ups, and where it was written.
#'
#' The calibration is a RESULT - one slope per tag and season, resolved by looking at the cohort - so
#' it reads here rather than in the header, which carries settings only.
#' @keywords internal
#' @noRd
.reportPaddleCohort <- function(lvl, cal, dep, statuses, speeds, agreement.threshold,
                                output.dir, plot.file) {
  if (lvl < 1L) return(invisible(NULL))
  n <- length(statuses)
  n_speed <- sum(statuses %in% c("applied", "as-recorded"))
  .log_done(lvl, sprintf("%d of %d deployment%s given a speed", n_speed, n, if (n != 1) "s" else ""))

  # cohort roll-ups, in the calculateTailBeats() form: median, IQR and range across deployments
  spread <- function(v, unit = "", d = 2) {
    v <- v[is.finite(v)]
    if (!length(v)) return(NULL)
    q <- stats::quantile(v, c(0.25, 0.5, 0.75), names = FALSE)
    sprintf("median %.*f%s (IQR %.*f\u2013%.*f, range %.*f\u2013%.*f%s)",
            d, q[2], unit, d, q[1], d, q[3], d, min(v), d, max(v), unit)
  }
  sp <- spread(speeds, " m/s")
  if (!is.null(sp)) .log_arrow_aligned(lvl, "speed:          ", sp)
  ag <- spread(dep$agreement, "")
  if (!is.null(ag)) .log_arrow_aligned(lvl, "agreement:      ", ag, ", ",
                                       sum(is.finite(dep$agreement)), " deployments")
  st <- dep$own_pct[dep$own_viable %in% TRUE]
  if (any(is.finite(st)))
    .log_arrow_aligned(lvl, "steep swimming: ",
                       sprintf("median %.1f%% of record (range %.1f\u2013%.1f%%)",
                               stats::median(st, na.rm = TRUE), min(st, na.rm = TRUE),
                               max(st, na.rm = TRUE)))

  # A mutually exclusive tally in a fixed order, so the rows visibly sum to the cohort and the block
  # looks the same on every run. "as-recorded" is one of the ways a deployment gets a speed, so it is
  # listed as its own outcome rather than counted twice.
  labs <- c(applied = "Speed calculated", `as-recorded` = "Speed as recorded",
            `no calibration` = "No calibration", `no paddle data` = "No paddle data",
            `no paddle wheel` = "No paddle wheel")
  tally <- table(factor(statuses, levels = names(labs)))
  keep <- as.integer(tally) > 0L
  if (any(keep)) {
    rows <- stats::setNames(as.integer(tally)[keep], unname(labs[names(labs)[keep]]))
    .log_section(lvl, "Deployments")
    .log_rows(lvl, rows, symbols = c(cli::col_green(cli::symbol$tick),
                                     rep(cli::symbol$bullet, max(0L, sum(keep) - 1L))))
  }

  if (nrow(cal)) {
    cs <- .paddleCalSummary(cal, dep$slope)
    rows <- c(`Tag-seasons` = cs$tags,
              if (!is.null(cs$range)) c(`Slopes applied` = cs$range),
              `Full table` = "attr(x, \"calibration\")")
    .log_section(lvl, "Calibration")
    .log_rows(lvl, rows)

    # Between-deployment spread. Reported, never flagged: nothing in these data establishes how much
    # spread is too much. Listed per tag-season while that stays readable, rolled up beyond it, so a
    # large fleet does not reproduce the wall of rows this block replaced.
    het <- cal[is.finite(cal$slope_cv), , drop = FALSE]
    if (nrow(het) && nrow(het) <= 3L) {
      .log_rows(lvl, stats::setNames(
        sprintf("%.0f%% across %d deployments (max/min %.2f)",
                100 * het$slope_cv, het$slope_k, het$slope_ratio),
        sprintf("Spread %s/pkg %s", het$year, het$package_id)))
    } else if (nrow(het)) {
      .log_rows(lvl, stats::setNames(
        sprintf("median %.0f%% (range %.0f-%.0f%%) across %d tag-seasons",
                100 * stats::median(het$slope_cv), 100 * min(het$slope_cv),
                100 * max(het$slope_cv), nrow(het)),
        "Between-deployment spread"))
    }

    if (any(cal$flag %in% TRUE)) {
      w <- which(cal$flag %in% TRUE)
      cli::cli_text("")
      .log_attention(lvl, sprintf(
        "%d calibration%s by more than %g%% from the in-situ estimate: %s",
        length(w), if (length(w) != 1) "s differ" else " differs", 100 * agreement.threshold,
        paste(sprintf("%s/pkg %s", cal$year[w], cal$package_id[w]), collapse = ", ")))
    }
  }

  out_rows <- c(if (!is.null(output.dir)) c(Directory = output.dir),
                if (!is.null(plot.file)) c(Plots = plot.file))
  if (length(out_rows)) { .log_section(lvl, "Output"); .log_rows(lvl, out_rows) }
  cli::cli_text("")
  invisible(NULL)
}

#' One panel per tag: the slope applied against the in-situ estimate, with its interval.
#' @keywords internal
#' @noRd
.renderPaddleDiagnostic <- function(cal, plot = FALSE, plot.file = NULL) {
  keep <- cal[is.finite(cal$in_situ_slope) | is.finite(cal$slope), , drop = FALSE]
  if (!nrow(keep)) return(invisible(NULL))
  draw <- function(to.file = FALSE, unicode = TRUE) {
    theme <- plotTheme()
    op <- graphics::par(family = theme$font.family, mar = c(4.4, 8.5, 3.2, 1.2), no.readonly = TRUE)
    on.exit(graphics::par(op), add = TRUE)
    y <- seq_len(nrow(keep))
    xr <- range(c(keep$slope, keep$in_situ_slope, keep$in_situ_lo, keep$in_situ_hi), na.rm = TRUE)
    if (!all(is.finite(xr))) xr <- c(0, 1)
    graphics::plot(NA, xlim = xr * c(0.9, 1.1), ylim = c(0.5, nrow(keep) + 0.5), yaxt = "n",
                   xlab = "calibration slope (m/s per Hz)", ylab = "", las = 1)
    graphics::segments(keep$in_situ_lo, y, keep$in_situ_hi, y, col = "grey60")
    graphics::points(keep$in_situ_slope, y, pch = 1, col = "grey30")
    graphics::points(keep$slope, y, pch = 19,
                     col = ifelse(!is.na(keep$slope_source) & keep$slope_source == "measured",
                                  "#1F6FB4", "#C8892A"))
    graphics::axis(2, y, sprintf("%s / pkg %s", keep$year, keep$package_id), las = 1, cex.axis = 0.8,
                   tick = FALSE)
    if (any(keep$flag, na.rm = TRUE))
      graphics::points(keep$slope[which(keep$flag)], y[which(keep$flag)], pch = 1, cex = 2.4, col = "#B22222")
    graphics::legend("topright", c("measured", "estimated", "in situ"), pch = c(19, 19, 1),
                     col = c("#1F6FB4", "#C8892A", "grey30"), bty = "n", cex = 0.8)
    graphics::mtext("Paddle-wheel calibration", side = 3, line = 1.4, adj = 0, font = 2)
  }
  .renderToDevices(draw, plot = plot, plot.file = plot.file, cairo = TRUE,
                   width = 8, height = max(3.2, 1.6 + 0.32 * nrow(keep)))
  invisible(NULL)
}
