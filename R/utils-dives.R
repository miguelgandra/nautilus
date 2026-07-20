#######################################################################################################
# Dive detection internals ############################################################################
#######################################################################################################
#
# The primitive: a dive is a vertical excursion of the depth trace away from a REFERENCE LEVEL b(t),
# detected by two-threshold hysteresis with a prominence criterion, and bounded by a return to within
# a band of that reference.
#
# Everything taxon-specific is expressed as a choice of b(t) and a sign, never as a special case:
#   reference = "surface"  -> b(t) = 0                (air-breathers; zero anchored by surfacing)
#   reference = "baseline" -> b(t) = running estimate (fish that never surface; benthic resters)
#   direction = "down"/"up"/"both"                    (excursion sign relative to b(t))
#
# WHY HYSTERESIS IS NOT OPTIONAL. With a single threshold, sensor noise at the crossing splits one
# excursion into many and the dive count becomes a property of the pressure transducer. Measured on a
# real deployment: naive crossing counting gave 163 "dives" at 5 m but 222 at 20 m - impossible for
# nested thresholds unless wiggles inside one deep excursion are being counted separately.


#' Running baseline b(t) for the "baseline" reference.
#'
#' @section Choosing the estimator:
#' The two estimators fail in OPPOSITE regimes and neither is universally correct, which is why the
#' choice is exposed rather than hidden:
#'
#' \itemize{
#'   \item \strong{median} follows a baseline that drifts during a deployment (an animal moving from
#'     shelf to slope). Measured: 13/13 excursions recovered on a 20->220 m drift, where a low
#'     quantile recovered 2/13. But it migrates INTO the excursions once they occupy more than about
#'     half the record (2/13 at 51 percent duty cycle, 0/13 at 67 percent).
#'   \item \strong{quantile} is immune to duty cycle, but on a trending baseline a low quantile tracks
#'     the window's TRAILING EDGE rather than the local level - on a rising trend it reports the depth
#'     the animal held a half-window ago, so the residual never returns to the band and dives never end.
#' }
#'
#' `.diveBaselineRisk()` measures both conditions so the caller can warn when the chosen estimator is
#' in its failing regime.
#' @keywords internal
#' @noRd
.diveBaseline <- function(depth, tnum, control, direction) {
  n <- length(depth)
  if (n < 3L) return(rep(stats::median(depth, na.rm = TRUE), n))
  dt <- stats::median(diff(tnum), na.rm = TRUE)
  if (!is.finite(dt) || dt <= 0) dt <- 1
  k <- max(3L, round((control$baseline.window * 3600) / dt / 2))     # half-width in samples

  if (identical(control$baseline.stat, "median")) {
    b <- .diveRunStat(depth, k, stats::median)
  } else {
    p <- control$baseline.quantile %||% switch(direction, down = 0.10, up = 0.90, 0.50)
    b <- .diveRunStat(depth, k, function(z) stats::quantile(z, p, names = FALSE))
  }
  b[!is.finite(b)] <- stats::median(depth, na.rm = TRUE)
  b
}

#' Centred running statistic, NA-tolerant, with a stride so long records stay tractable.
#'
#' A full per-sample rolling quantile over millions of samples is O(n*k) and unusable at 20 Hz. The
#' statistic is evaluated on a coarse grid and linearly interpolated - the baseline is by construction
#' a slow quantity (hours), so nothing is lost.
#' @keywords internal
#' @noRd
.diveRunStat <- function(x, k, f) {
  n <- length(x)
  stride <- max(1L, floor(k / 8))                       # >= 8 knots per window
  at <- unique(c(seq(1L, n, by = stride), n))
  v <- vapply(at, function(i) {
    z <- x[max(1L, i - k):min(n, i + k)]
    z <- z[is.finite(z)]
    if (!length(z)) NA_real_ else f(z)
  }, numeric(1))
  ok <- is.finite(v)
  if (!any(ok)) return(rep(NA_real_, n))
  stats::approx(at[ok], v[ok], xout = seq_len(n), rule = 2)$y
}

#' Diagnose whether the chosen baseline estimator is in its failing regime.
#'
#' Returns the excursion duty cycle (kills the median above ~0.5) and the baseline trend expressed in
#' window-widths (kills a low quantile once the level moves by more than roughly one threshold per
#' window). Reported, not acted on: the caller warns and the user decides.
#' @keywords internal
#' @noRd
.diveBaselineRisk <- function(depth, baseline, threshold, control, tnum) {
  resid <- depth - baseline
  duty  <- mean(abs(resid) > threshold, na.rm = TRUE)
  span  <- diff(range(tnum, na.rm = TRUE))
  win_s <- control$baseline.window * 3600
  drift <- if (is.finite(span) && span > 0 && length(baseline) > 1) {
    abs(diff(range(baseline, na.rm = TRUE))) * (win_s / span)      # baseline movement per window
  } else 0
  list(duty_cycle = duty, drift_per_window_m = drift,
       median_at_risk   = identical(control$baseline.stat, "median")   && is.finite(duty)  && duty  > 0.5,
       quantile_at_risk = identical(control$baseline.stat, "quantile") && is.finite(drift) && drift > threshold)
}


#' Two-threshold hysteresis over the residual, with prominence, duration and gap handling.
#'
#' Returns a data.frame of candidate excursions with the index span, the sign, and the flags a caller
#' needs to judge them. Contains NO ecology: it is a run-finder over a residual series.
#'
#' Gap handling: a sampling gap longer than `max.gap` SPLITS a dive rather than being interpolated
#' across, and both parts carry `n_gaps`/`gap_s`. Interpolating would invent an excursion shape that
#' was never measured; dropping the dive would bias against exactly the long dives that gaps tend to
#' interrupt. Truncated dives at the record boundary are RETAINED and flagged, following tagtools'
#' `findall`, because discarding them biases the duration distribution.
#' @keywords internal
#' @noRd
.diveRuns <- function(resid, tnum, threshold, band, sign = 1) {
  r <- sign * resid
  n <- length(r)
  if (!n) return(.diveEmptyRuns())
  inside <- rep(FALSE, n)
  state <- FALSE
  # single pass; NA is "no information" and neither opens nor closes a dive
  for (i in seq_len(n)) {
    ri <- r[i]
    if (is.na(ri)) { inside[i] <- state; next }
    if (!state && ri > threshold) state <- TRUE
    else if (state && ri < band)  state <- FALSE
    inside[i] <- state
  }
  if (!any(inside)) return(.diveEmptyRuns())
  d <- diff(c(FALSE, inside, FALSE))
  starts <- which(d == 1); ends <- which(d == -1) - 1L
  data.frame(start_i = starts, end_i = ends, sign = sign, stringsAsFactors = FALSE)
}

#' @keywords internal
#' @noRd
.diveEmptyRuns <- function() data.frame(start_i = integer(0), end_i = integer(0), sign = numeric(0))

#' Split a candidate run wherever the sampling gap exceeds `max.gap`.
#' @keywords internal
#' @noRd
.diveSplitOnGaps <- function(runs, tnum, max.gap) {
  if (!nrow(runs)) return(cbind(runs, n_gaps = integer(0), gap_s = numeric(0)))
  out <- list()
  for (k in seq_len(nrow(runs))) {
    i0 <- runs$start_i[k]; i1 <- runs$end_i[k]
    if (i1 <= i0) { out[[length(out) + 1L]] <- data.frame(start_i = i0, end_i = i1, sign = runs$sign[k],
                                                          n_gaps = 0L, gap_s = 0); next }
    dtv <- diff(tnum[i0:i1])
    brk <- which(dtv > max.gap)
    if (!length(brk)) {
      out[[length(out) + 1L]] <- data.frame(start_i = i0, end_i = i1, sign = runs$sign[k],
                                            n_gaps = 0L, gap_s = 0)
    } else {
      seg_start <- i0
      for (b in brk) {
        out[[length(out) + 1L]] <- data.frame(start_i = seg_start, end_i = i0 + b - 1L,
                                              sign = runs$sign[k], n_gaps = 1L, gap_s = dtv[b])
        seg_start <- i0 + b
      }
      out[[length(out) + 1L]] <- data.frame(start_i = seg_start, end_i = i1, sign = runs$sign[k],
                                            n_gaps = 1L, gap_s = dtv[brk[length(brk)]])
    }
  }
  do.call(rbind, out)
}

#' Apply the prominence and duration criteria, and flag boundary truncation.
#' @keywords internal
#' @noRd
.diveScreenRuns <- function(runs, resid, tnum, min.prominence, min.duration, n_total) {
  if (!nrow(runs)) return(cbind(runs, amplitude = numeric(0), duration_s = numeric(0),
                                truncated_start = logical(0), truncated_end = logical(0)))
  amp <- vapply(seq_len(nrow(runs)), function(k) {
    z <- runs$sign[k] * resid[runs$start_i[k]:runs$end_i[k]]
    z <- z[is.finite(z)]
    if (!length(z)) NA_real_ else max(z)
  }, numeric(1))
  dur <- tnum[runs$end_i] - tnum[runs$start_i]
  runs$amplitude <- amp
  runs$duration_s <- dur
  runs$truncated_start <- runs$start_i == 1L
  runs$truncated_end   <- runs$end_i == n_total
  keep <- is.finite(amp) & amp >= min.prominence & is.finite(dur) & dur >= min.duration
  runs[keep, , drop = FALSE]
}


#######################################################################################################
# Driver internals ####################################################################################
#######################################################################################################

#' Cheap first pass over one deployment: everything the DERIVED settings need, and nothing else.
#' @keywords internal
#' @noRd
.diveScanOne <- function(x, id.col, datetime.col, depth.col, fallback_id) {
  id <- as.character(.getMeta(x)$id %||% fallback_id)
  out <- list(id = id, usable = FALSE, n = nrow(x))
  if (!all(c(datetime.col, depth.col) %in% names(x))) return(out)
  tnum <- .asTimeSeconds(x[[datetime.col]])
  d <- .asNumericSafe(x[[depth.col]])
  if (is.null(tnum) || !any(is.finite(d)) || sum(is.finite(tnum)) < 3L) return(out)

  dt <- stats::median(diff(tnum[is.finite(tnum)]), na.rm = TRUE)
  # noise of the series ACTUALLY STORED, via second differences (Rice 1984; Gasser et al. 1986).
  # Var(2nd diff) = 6 sigma^2 for an iid residual; mad() is already sd-consistent. Reported, never
  # gated on: the stored series may be smoothed, which biases this LOW, so a floor built on it
  # would fail open.
  fin <- d[is.finite(d)]
  noise <- if (length(fin) > 3L) stats::mad(diff(fin, differences = 2)) / sqrt(6) else NA_real_

  # zero-offset provenance: the honest answer to "how far from zero is this record's zero"
  pr <- Filter(function(r) identical(r$step, "depth_drift"), .getMeta(x)$processing)
  zoc <- if (length(pr)) pr[[length(pr)]] else NULL
  pt <- Filter(function(r) identical(r$step, "processTagData"), .getMeta(x)$processing)
  smooth_s <- if (length(pt)) suppressWarnings(as.numeric(pt[[length(pt)]]$depth_smoothing %||% NA)) else NA_real_

  utils::modifyList(out, list(
    usable = TRUE, tnum = NULL, dt = dt, noise = noise,
    depth_range = range(fin), depth_q = stats::quantile(fin, c(.5, .75, .9, .95), names = FALSE),
    zoc_status = as.character(zoc$status %||% NA_character_),
    zoc_residual = suppressWarnings(as.numeric(zoc$outcome$residual_m %||% NA_real_)),
    depth_smoothing = if (is.finite(smooth_s)) smooth_s else NA_real_))
}

#' Derive the settings the user did not supply, ONCE across the cohort, and teach the derivation.
#' @keywords internal
#' @noRd
.diveDeriveSettings <- function(scans, control, lvl) {
  resid <- vapply(scans, function(z) z$zoc_residual %||% NA_real_, numeric(1))
  noise <- vapply(scans, function(z) z$noise %||% NA_real_, numeric(1))
  dts   <- vapply(scans, function(z) z$dt %||% NA_real_, numeric(1))
  smo   <- vapply(scans, function(z) z$depth_smoothing %||% NA_real_, numeric(1))
  zst   <- vapply(scans, function(z) z$zoc_status %||% NA_character_, character(1))

  r_max <- suppressWarnings(max(resid, na.rm = TRUE)); if (!is.finite(r_max)) r_max <- NA_real_
  n_med <- suppressWarnings(stats::median(noise, na.rm = TRUE)); if (!is.finite(n_med)) n_med <- NA_real_
  dt_med <- suppressWarnings(stats::median(dts, na.rm = TRUE)); if (!is.finite(dt_med)) dt_med <- 1

  # THRESHOLD first, then the band FROM it. The band answers "has the animal returned?", which is a
  # question about the scale of the dive, not only about how uncertain the zero is. Deriving it from the
  # ZOC residual alone gave 0.75 m on a real record, and an animal oscillating to ~2 m never re-entered
  # it: one plunge plus 1,700 s of shallow oscillation was reported as a SINGLE 2,016 s dive. Scaling
  # with the threshold closes those, while the residual term keeps the band above the zero's own noise.
  thr_src <- if (is.null(control$depth.threshold)) "derived floor" else "user"
  thr <- control$depth.threshold %||% max(3 * (if (is.na(r_max)) 0.34 else r_max), 1.0)
  band <- control$surface.band %||% max(2 * (if (is.na(r_max)) 0.25 else r_max), thr / 10, 0.5)
  if (band >= thr) band <- thr / 2                       # keep hysteresis meaningful after derivation
  prom <- control$min.prominence %||% (thr - band)

  smo_max <- suppressWarnings(max(smo, na.rm = TRUE)); if (!is.finite(smo_max)) smo_max <- 0
  dur_src <- if (is.null(control$min.duration)) "derived" else "user"
  dur <- control$min.duration %||% max(4 * smo_max, 4 * dt_med, 10)

  gap <- control$max.gap %||% max(60, 10 * dt_med)
  wig <- control$wiggle.amplitude %||% max(0.5, 3 * (if (is.na(n_med)) 0.1 else n_med))

  # reference resolution: "auto" needs the ZOC provenance AND evidence the animal visits the band
  # The THRESHOLD is cohort-wide so dive counts stay comparable. The REFERENCE is not: whether a
  # record's zero can be trusted is a property of THAT deployment's zero-offset correction. Deciding it
  # once for the cohort gave a deployment whose ZOC abstained (surface sitting at +1.1 m) a single dive
  # spanning its whole record, because it never returns to a 0-referenced band.
  ref_note <- ""
  ref <- control$reference
  if (identical(ref, "auto")) {
    ok <- zst %in% c("applied", "applied_with_gaps", "constant_offset")
    ok[is.na(ok)] <- FALSE
    ref <- "per-deployment"
    ref_note <- if (all(ok)) "surface (auto: ZOC anchored on every deployment)"
                else if (!any(ok)) "baseline (auto: ZOC abstained or absent on every deployment)"
                else sprintf("mixed (auto: surface on %d, baseline on %d - ZOC anchored per deployment)",
                             sum(ok), sum(!ok))
  } else {
    ref_note <- sprintf("%s (user)", ref)
    if (identical(ref, "surface") && !any(zst %in% c("applied", "applied_with_gaps", "constant_offset"), na.rm = TRUE)) {
      msg <- c("{.arg reference = \"surface\"} was requested but the zero-offset correction did not anchor on any deployment.",
               "i" = "Depths are not referenced to a known zero, so a surface threshold may be meaningless. Consider {.code reference = \"baseline\"}.")
      if (identical(control$require.zoc, "error")) .abort(msg)
      else if (identical(control$require.zoc, "warn")) cli::cli_warn(msg)
    }
  }

  if (lvl >= 1L && is.null(control$depth.threshold)) {
    .log_arrow(lvl, "depth.threshold not supplied; using the derived FLOOR for this cohort")
    .log_detail(lvl, sprintf("ZOC residual (p95 |depth| at surface): %s",
                             if (is.na(r_max)) "unavailable" else sprintf("%.2f m", r_max)))
    .log_detail(lvl, sprintf("stored-series noise (MAD, 2nd diff):   %s",
                             if (is.na(n_med)) "unavailable" else sprintf("%.3f m", n_med)))
    .log_detail(lvl, sprintf("derived floor = %.2f m", thr))
    .log_subdetail(lvl, "This is the smallest excursion the RECORD can support, not an estimate of what")
    .log_subdetail(lvl, "the ANIMAL treats as a dive. Set diveControl(depth.threshold = ) from your study")
    .log_subdetail(lvl, "system, and choose it BEFORE looking at your response variable.")
  }
  if (lvl >= 1L && smo_max > 0) {
    .log_detail(lvl, sprintf("depth_smoothing %.0f s -> min duration floor %.0f s; excursions shorter than that are attenuated",
                             smo_max, dur))
  }

  list(reference = ref, reference_note = ref_note,
       depth.threshold = thr, surface.band = band, min.prominence = prom, min.duration = dur,
       max.gap = gap, wiggle.amplitude = wig, threshold_source = thr_src, duration_source = dur_src,
       noise = n_med)
}

#' Detect dives in ONE deployment. Returns the three per-sample columns plus a status.
#' @keywords internal
#' @noRd
.detectDivesOne <- function(x, scan, settings, control, datetime.col, depth.col, lvl, id) {
  n <- nrow(x)
  empty <- list(dive_id = rep(0L, n),
                dive_phase = factor(rep("inter_dive", n),
                                    levels = c("descent", "bottom", "ascent", "inter_dive")),
                baseline = rep(NA_real_, n), n_dives = 0L,
                reference = settings$reference, status = "abstained_no_depth")
  if (!isTRUE(scan$usable)) return(empty)

  tnum <- .asTimeSeconds(x[[datetime.col]])
  d <- .asNumericSafe(x[[depth.col]])

  # resolve THIS deployment's reference (the cohort-level value may be "per-deployment")
  ref <- settings$reference
  if (identical(ref, "per-deployment")) {
    ok <- isTRUE(scan$zoc_status %in% c("applied", "applied_with_gaps", "constant_offset"))
    ref <- if (ok) "surface" else "baseline"
  }
  b <- if (identical(ref, "surface")) rep(0, n)
       else .diveBaseline(d, tnum, control, control$direction)
  resid <- d - b

  # warn when the chosen baseline estimator sits in its own failing regime
  if (identical(ref, "baseline") && lvl >= 1L) {
    risk <- .diveBaselineRisk(d, b, settings$depth.threshold, control, tnum)
    if (isTRUE(risk$median_at_risk))
      cli::cli_warn(c("{.val {id}}: excursions occupy {round(100*risk$duty_cycle)}% of the record, so the running MEDIAN baseline sits inside them.",
                      "i" = "Use {.code diveControl(baseline.stat = \"quantile\")} for a duty cycle above ~50%."))
    if (isTRUE(risk$quantile_at_risk))
      cli::cli_warn(c("{.val {id}}: the baseline moves {round(risk$drift_per_window_m,1)} m per window, so a low QUANTILE tracks the window edge, not the local level.",
                      "i" = "Use {.code diveControl(baseline.stat = \"median\")} on a drifting baseline."))
  }

  signs <- switch(control$direction, down = 1, up = -1, both = c(1, -1))
  runs <- do.call(rbind, lapply(signs, function(s)
    .diveRuns(resid, tnum, settings$depth.threshold, settings$surface.band, sign = s)))
  if (is.null(runs) || !nrow(runs)) {
    e <- empty; e$baseline <- b; e$status <- "applied_no_dives"; return(e)
  }
  runs <- .diveSplitOnGaps(runs, tnum, settings$max.gap)
  runs <- .diveScreenRuns(runs, resid, tnum, settings$min.prominence, settings$min.duration, n)
  if (!nrow(runs)) { e <- empty; e$baseline <- b; e$status <- "applied_no_dives"; return(e) }
  runs <- runs[order(runs$start_i), , drop = FALSE]

  dive_id <- rep(0L, n)
  phase <- rep("inter_dive", n)
  for (k in seq_len(nrow(runs))) {
    idx <- runs$start_i[k]:runs$end_i[k]
    dive_id[idx] <- k
    phase[idx] <- .divePhases(resid[idx] * runs$sign[k], tnum[idx], control)
  }
  list(dive_id = dive_id,
       dive_phase = factor(phase, levels = c("descent", "bottom", "ascent", "inter_dive")),
       baseline = b, n_dives = nrow(runs),
       reference = ref, status = "applied")
}

#' Split one dive into descent / bottom / ascent.
#'
#' The rate rule normalises by the dive's own `rate.quantile` quantile of |vertical rate|, NOT its
#' maximum: the maximum of a smoothed series is an artefact of the smoothing window, and its magnitude
#' depends on dive duration, so a max-normalised criterion is not comparable between a short dive and a
#' long one within the same animal.
#' @keywords internal
#' @noRd
.divePhases <- function(z, tnum, control) {
  m <- length(z)
  if (m < 3L) return(rep("bottom", m))
  if (identical(control$phase.method, "prop.depth")) {
    peak <- max(z, na.rm = TRUE)
    if (!is.finite(peak) || peak <= 0) return(rep("bottom", m))
    deep <- which(z >= control$bottom.prop * peak)
    if (!length(deep)) return(rep("bottom", m))
    ph <- rep("bottom", m)
    if (min(deep) > 1L) ph[seq_len(min(deep) - 1L)] <- "descent"
    if (max(deep) < m)  ph[(max(deep) + 1L):m] <- "ascent"
    return(ph)
  }
  dt <- diff(tnum); dt[!is.finite(dt) | dt <= 0] <- NA_real_
  rate <- c(NA_real_, diff(z) / dt)
  aq <- suppressWarnings(stats::quantile(abs(rate), control$rate.quantile, na.rm = TRUE, names = FALSE))
  if (!is.finite(aq) || aq <= 0) return(rep("bottom", m))
  crit <- control$rate.crit * aq
  i_peak <- which.max(z)
  ph <- rep("bottom", m)
  # A boundary must be SUSTAINED. Taking the first slow sample to end descent (or the last slow one to
  # begin ascent) makes the boundary hostage to a single hesitation: rendered on real profiles, descent
  # stopped at 25 m on a dive that continued to 145 m, and the entire ascent of a clean V-dive was
  # labelled bottom. Require the criterion to hold over a run before committing.
  run_len <- max(3L, ceiling(0.05 * m))
  sustained <- function(ok) {
    # index of the first position where `ok` holds for run_len consecutive samples, else NA
    if (length(ok) < run_len) return(NA_integer_)
    r <- rle(ok)
    e <- cumsum(r$lengths); st <- e - r$lengths + 1L
    w <- which(r$values & r$lengths >= run_len)
    if (!length(w)) NA_integer_ else st[w[1]]
  }
  ok_rate <- is.finite(rate)
  # descent: ends at the start of the first sustained NOT-descending run after descent has begun
  before <- seq_len(max(1L, i_peak - 1L))
  if (length(before) >= run_len) {
    began <- which(ok_rate[before] & rate[before] > crit)
    if (length(began)) {
      tail_idx <- before[before > min(began)]
      k <- sustained(ok_rate[tail_idx] & rate[tail_idx] <= crit)
      d_end <- if (is.na(k)) i_peak - 1L else tail_idx[k] - 1L
      if (d_end >= 1L) ph[seq_len(d_end)] <- "descent"
    }
  }
  # ascent: begins at the start of the LAST sustained ascending run
  after <- if (i_peak < m) (i_peak + 1L):m else integer(0)
  if (length(after) >= run_len) {
    ok_asc <- ok_rate[after] & rate[after] < -crit
    r <- rle(ok_asc); e <- cumsum(r$lengths); st <- e - r$lengths + 1L
    w <- which(r$values & r$lengths >= run_len)
    if (length(w)) {
      a_start <- after[st[w[1]]]
      if (a_start <= m) ph[a_start:m] <- "ascent"
    }
  }
  ph
}


#######################################################################################################
# Per-dive reduction ##################################################################################
#######################################################################################################

#' Circular mean (degrees) and mean resultant length. `mean_angle` is NA when the resultant is too
#' short to have a direction worth reporting.
#' @keywords internal
#' @noRd
.diveCircular <- function(deg) {
  z <- deg[is.finite(deg)]
  if (!length(z)) return(c(mean_angle = NA_real_, mrl = NA_real_))
  r <- z * pi / 180
  C <- mean(cos(r)); S <- mean(sin(r))
  mrl <- sqrt(C^2 + S^2)
  ang <- if (mrl < 0.1) NA_real_ else (atan2(S, C) * 180 / pi) %% 360
  c(mean_angle = ang, mrl = mrl)
}

#' The empty table with the full fixed schema, so a zero-dive run still rbinds with a non-empty one.
#' @keywords internal
#' @noRd
.diveMetricsSchema <- function(variables, circular.variables, statistics, by.phase) {
  base <- data.frame(
    ID = character(0), dive_id = integer(0),
    start = as.POSIXct(character(0)), end = as.POSIXct(character(0)),
    reference = character(0), direction = character(0),
    depth_threshold_m = numeric(0), surface_band_m = numeric(0), phase_method = character(0),
    duration_s = numeric(0), n_samples = integer(0),
    max_depth_m = numeric(0), max_depth_time = as.POSIXct(character(0)),
    baseline_depth_m = numeric(0), amplitude_m = numeric(0), prominence_m = numeric(0),
    mean_depth_m = numeric(0), sd_depth_m = numeric(0),
    descent_duration_s = numeric(0), bottom_duration_s = numeric(0), ascent_duration_s = numeric(0),
    descent_rate_mean = numeric(0), descent_rate_q90 = numeric(0),
    ascent_rate_mean = numeric(0), ascent_rate_q90 = numeric(0),
    bottom_depth_mean_m = numeric(0), bottom_depth_sd_m = numeric(0),
    phase_structure = character(0),
    vertical_distance_m = numeric(0), n_reversals = integer(0),
    inter_dive_s = numeric(0), inter_dive_censored = logical(0),
    complete = logical(0), truncated_start = logical(0), truncated_end = logical(0),
    n_gaps = integer(0), gap_s = numeric(0), depth_attenuation = numeric(0),
    shape_supported = logical(0), stringsAsFactors = FALSE)
  for (v in variables) {
    circ <- v %in% circular.variables
    nms <- if (circ) c(paste0(v, "_mean_angle"), paste0(v, "_mrl"))
           else paste0(v, "_", statistics)
    if (by.phase) nms <- c(nms, as.vector(outer(paste0(v, c("_descent", "_bottom", "_ascent")),
                                                if (circ) c("_mean_angle") else paste0("_", statistics),
                                                paste0)))
    for (nm in nms) base[[nm]] <- numeric(0)
  }
  base
}

#' Reduce ONE annotated deployment to one row per dive.
#' @keywords internal
#' @noRd
.diveMetricsOne <- function(x, id, datetime.col, depth.col, variables, circular.variables,
                            statistics, by.phase) {
  did <- x[["dive_id"]]
  if (!any(did > 0, na.rm = TRUE))
    return(.diveMetricsSchema(variables, circular.variables, statistics, by.phase))

  tnum <- .asTimeSeconds(x[[datetime.col]])
  tpos <- x[[datetime.col]]
  d <- .asNumericSafe(x[[depth.col]])
  b <- if ("depth_baseline" %in% names(x)) .asNumericSafe(x[["depth_baseline"]]) else rep(0, nrow(x))
  ph <- as.character(x[["dive_phase"]])
  n_total <- nrow(x)

  # provenance: the settings that produced these dives travel with every row
  pr <- Filter(function(r) identical(r$step, "detectDives"), .getMeta(x)$processing)
  p <- if (length(pr)) pr[[length(pr)]] else list()
  smo <- Filter(function(r) identical(r$step, "processTagData"), .getMeta(x)$processing)
  smooth_s <- if (length(smo)) suppressWarnings(as.numeric(smo[[length(smo)]]$depth_smoothing %||% NA)) else NA_real_

  ids <- sort(unique(did[did > 0]))
  rows <- lapply(ids, function(k) {
    idx <- which(did == k)
    i0 <- min(idx); i1 <- max(idx)
    tt <- tnum[idx]; dd <- d[idx]; pp <- ph[idx]
    dur <- suppressWarnings(max(tt, na.rm = TRUE) - min(tt, na.rm = TRUE))
    fin <- is.finite(dd)
    amp <- suppressWarnings(max(abs(dd - b[idx])[fin], na.rm = TRUE))
    i_ext <- if (any(fin)) idx[which.max(abs(dd - b[idx]))] else i0

    # phase spans, measured from real timestamps (never n * dt)
    pdur <- vapply(c("descent", "bottom", "ascent"), function(q) {
      w <- which(pp == q)
      if (length(w) < 2L) 0 else suppressWarnings(max(tt[w], na.rm = TRUE) - min(tt[w], na.rm = TRUE))
    }, numeric(1))
    present <- c(descent = pdur[["descent"]] > 0, bottom = pdur[["bottom"]] > 0, ascent = pdur[["ascent"]] > 0)
    structure_code <- paste0(if (present[["descent"]]) "D" else "",
                             if (present[["bottom"]]) "B" else "",
                             if (present[["ascent"]]) "A" else "")
    if (!nzchar(structure_code)) structure_code <- "X"
    shape_ok <- sum(present) >= 2L

    # vertical rates within each phase, from the depth series itself
    rate <- c(NA_real_, diff(dd) / diff(tt))
    rq <- function(q, f) { w <- which(pp == q & is.finite(rate)); if (!length(w)) NA_real_ else f(rate[w]) }
    q90 <- function(z) suppressWarnings(stats::quantile(abs(z), 0.90, na.rm = TRUE, names = FALSE))

    # amplitude-filtered reversals: direction changes exceeding the wiggle amplitude
    wig <- suppressWarnings(as.numeric(p$wiggle_amplitude_m %||% NA))
    if (!is.finite(wig)) wig <- 0.5
    n_rev <- .diveReversals(dd, wig)

    gap_v <- suppressWarnings(max(c(0, diff(tt)), na.rm = TRUE))
    med_dt <- suppressWarnings(stats::median(diff(tt), na.rm = TRUE))
    max_gap <- suppressWarnings(as.numeric(p$max_gap_s %||% NA))
    if (!is.finite(max_gap)) max_gap <- max(60, 10 * (if (is.finite(med_dt)) med_dt else 1))
    n_gaps <- sum(diff(tt) > max_gap, na.rm = TRUE)

    att <- if (is.finite(smooth_s) && smooth_s > 0 && is.finite(dur) && dur > 0)
             max(0, min(1, 1 - smooth_s / (2 * dur))) else 1

    row <- data.frame(
      ID = id, dive_id = as.integer(k),
      start = tpos[i0], end = tpos[i1],
      reference = as.character(p$reference %||% NA_character_),
      direction = as.character(p$direction %||% NA_character_),
      depth_threshold_m = suppressWarnings(as.numeric(p$depth_threshold_m %||% NA)),
      surface_band_m = suppressWarnings(as.numeric(p$surface_band_m %||% NA)),
      phase_method = as.character(p$phase_method %||% NA_character_),
      duration_s = dur, n_samples = length(idx),
      max_depth_m = suppressWarnings(max(dd, na.rm = TRUE)),
      max_depth_time = tpos[i_ext],
      baseline_depth_m = b[i0], amplitude_m = amp, prominence_m = amp,
      mean_depth_m = mean(dd, na.rm = TRUE), sd_depth_m = stats::sd(dd, na.rm = TRUE),
      descent_duration_s = if (shape_ok) pdur[["descent"]] else NA_real_,
      bottom_duration_s  = if (shape_ok) pdur[["bottom"]]  else NA_real_,
      ascent_duration_s  = if (shape_ok) pdur[["ascent"]]  else NA_real_,
      descent_rate_mean = if (shape_ok) rq("descent", function(z) mean(z, na.rm = TRUE)) else NA_real_,
      descent_rate_q90  = if (shape_ok) rq("descent", q90) else NA_real_,
      ascent_rate_mean  = if (shape_ok) rq("ascent",  function(z) mean(z, na.rm = TRUE)) else NA_real_,
      ascent_rate_q90   = if (shape_ok) rq("ascent",  q90) else NA_real_,
      bottom_depth_mean_m = if (shape_ok && present[["bottom"]]) mean(dd[pp == "bottom"], na.rm = TRUE) else NA_real_,
      bottom_depth_sd_m   = if (shape_ok && present[["bottom"]]) stats::sd(dd[pp == "bottom"], na.rm = TRUE) else NA_real_,
      phase_structure = structure_code,
      vertical_distance_m = sum(abs(diff(dd)), na.rm = TRUE),
      n_reversals = if (shape_ok) n_rev else NA_integer_,
      inter_dive_s = NA_real_, inter_dive_censored = NA,
      complete = i0 > 1L && i1 < n_total && n_gaps == 0L,
      truncated_start = i0 == 1L, truncated_end = i1 == n_total,
      n_gaps = as.integer(n_gaps), gap_s = if (n_gaps > 0) gap_v else 0,
      depth_attenuation = att, shape_supported = shape_ok,
      stringsAsFactors = FALSE)

    for (v in variables) {
      circ <- v %in% circular.variables
      if (!v %in% names(x)) {
        nms <- if (circ) c(paste0(v, "_mean_angle"), paste0(v, "_mrl")) else paste0(v, "_", statistics)
        for (nm in nms) row[[nm]] <- NA_real_
        if (by.phase) for (q in c("descent", "bottom", "ascent"))
          for (st in (if (circ) "mean_angle" else statistics)) row[[paste0(v, "_", q, "_", st)]] <- NA_real_
        next
      }
      vv <- .asNumericSafe(x[[v]])[idx]
      if (circ) {
        cs <- .diveCircular(vv)
        row[[paste0(v, "_mean_angle")]] <- cs[["mean_angle"]]; row[[paste0(v, "_mrl")]] <- cs[["mrl"]]
      } else {
        if ("mean" %in% statistics) row[[paste0(v, "_mean")]] <- mean(vv, na.rm = TRUE)
        if ("sd"   %in% statistics) row[[paste0(v, "_sd")]]   <- stats::sd(vv, na.rm = TRUE)
      }
      if (by.phase) for (q in c("descent", "bottom", "ascent")) {
        w <- pp == q
        if (circ) row[[paste0(v, "_", q, "_mean_angle")]] <- .diveCircular(vv[w])[["mean_angle"]]
        else {
          if ("mean" %in% statistics) row[[paste0(v, "_", q, "_mean")]] <- mean(vv[w], na.rm = TRUE)
          if ("sd"   %in% statistics) row[[paste0(v, "_", q, "_sd")]]   <- stats::sd(vv[w], na.rm = TRUE)
        }
      }
    }
    row
  })
  out <- do.call(rbind, rows)
  # inter-dive interval: end of this dive to start of the next, censored when a gap or boundary intrudes
  if (nrow(out) > 1L) {
    nxt <- c(as.numeric(out$start[-1]), NA_real_)
    out$inter_dive_s <- nxt - as.numeric(out$end)
    out$inter_dive_censored <- c(out$truncated_end[-nrow(out)] | out$truncated_start[-1], NA)
  }
  out
}

#' Count direction reversals whose amplitude exceeds `min.amp` (wiggles within a dive).
#' @keywords internal
#' @noRd
.diveReversals <- function(z, min.amp) {
  z <- z[is.finite(z)]
  if (length(z) < 3L || !is.finite(min.amp) || min.amp <= 0) return(0L)
  # walk the series, committing a turning point only once the move away from it clears min.amp
  n <- 0L; last_ext <- z[1]; dir <- 0L
  for (i in 2:length(z)) {
    delta <- z[i] - last_ext
    if (abs(delta) < min.amp) next
    d_now <- if (delta > 0) 1L else -1L
    if (dir != 0L && d_now != dir) n <- n + 1L
    dir <- d_now; last_ext <- z[i]
  }
  as.integer(n)
}


#######################################################################################################
# Dive diagnostics ####################################################################################
#######################################################################################################
#
# Opt-in gather inside the loop, decoupled render afterwards - the architecture already used by
# processTagData's correction diagnostics (R/utils-processing-diagnostics.R). Nothing is computed
# unless the caller asked for a plot.

#' Gather everything the dive panels need for ONE deployment, decimated for drawing.
#' @keywords internal
#' @noRd
.captureDiveDiag <- function(id, tnum, depth, baseline, dive_id, phase, settings, resid,
                             control, target.n = 4000L) {
  n <- length(depth)
  if (!n) return(NULL)
  idx <- seq(1L, n, by = max(1L, n %/% as.integer(target.n)))

  # threshold sensitivity: how many dives would each candidate threshold have produced? This is the
  # panel that tells a user whether their choice sits on a plateau or on a cliff.
  sweep <- NULL
  if (isTRUE(control$sensitivity.sweep > 0) || is.null(control$sensitivity.sweep)) {
    k <- 12L
    hi <- suppressWarnings(stats::quantile(abs(resid), 0.995, na.rm = TRUE, names = FALSE))
    if (is.finite(hi) && hi > 0) {
      cand <- seq(max(settings$surface.band * 1.5, hi / 60), hi, length.out = k)
      cnt <- vapply(cand, function(th) {
        band <- min(settings$surface.band, th / 2)
        r <- .diveRuns(resid, tnum, th, band, sign = if (identical(control$direction, "up")) -1 else 1)
        if (!nrow(r)) return(0)
        r <- .diveScreenRuns(.diveSplitOnGaps(r, tnum, settings$max.gap), resid, tnum,
                             th - band, settings$min.duration, n)
        nrow(r)
      }, numeric(1))
      sweep <- data.frame(threshold = cand, n_dives = cnt)
    }
  }

  # a handful of representative dives, at full resolution, for the phase-boundary panel: the shortest,
  # the median and the longest, because a phase rule that works on one often fails the others
  ex <- list()
  ids <- sort(unique(dive_id[dive_id > 0]))
  if (length(ids)) {
    durs <- vapply(ids, function(k) { w <- which(dive_id == k); diff(range(tnum[w])) }, numeric(1))
    pick <- unique(ids[c(which.min(durs), which.min(abs(durs - stats::median(durs))), which.max(durs))])
    ex <- lapply(pick, function(k) {
      w <- which(dive_id == k)
      w <- max(1L, min(w) - 20L):min(n, max(w) + 20L)          # a little context either side
      list(id = k, t = tnum[w] - tnum[w][1], depth = depth[w], baseline = baseline[w],
           phase = as.character(phase)[w], in_dive = dive_id[w] == k)
    })
  }
  list(id = id, t = tnum[idx], depth = depth[idx], baseline = baseline[idx],
       dive_id = dive_id[idx], phase = as.character(phase)[idx],
       settings = settings, sweep = sweep, examples = ex,
       n_dives = length(ids), direction = control$direction)
}

#' Panel 1: the depth trace with the reference overlaid and detected dives shaded.
#' @keywords internal
#' @noRd
.drawDiveTracePanel <- function(d, theme) {
  t <- (d$t - d$t[1]) / 3600
  graphics::par(mar = c(3.8, 5.0, 2.6, 1.2), mgp = c(3, 0.55, 0), tcl = -0.22)
  ylim <- rev(range(c(d$depth, d$baseline), na.rm = TRUE))
  graphics::plot(NA, xlim = range(t, na.rm = TRUE), ylim = ylim, axes = FALSE, xlab = "", ylab = "")
  graphics::rect(graphics::par("usr")[1], graphics::par("usr")[3], graphics::par("usr")[2],
                 graphics::par("usr")[4], col = theme$panel, border = NA)
  # shade each detected dive so the reader sees WHAT was called a dive, not just the count
  runs <- rle(d$dive_id > 0)
  e <- cumsum(runs$lengths); s <- e - runs$lengths + 1L
  for (i in which(runs$values)) {
    graphics::rect(t[s[i]], ylim[1], t[e[i]], ylim[2],
                   col = grDevices::adjustcolor(theme$night, 0.16), border = NA)
  }
  graphics::lines(t, d$depth, col = theme$ink, lwd = 0.8)
  graphics::lines(t, d$baseline, col = "#c62828", lwd = 1.3, lty = 2)
  graphics::axis(1, col = NA, col.ticks = theme$axis, col.axis = theme$axis, cex.axis = 0.8 * theme$cex)
  graphics::axis(2, las = 1, col = NA, col.ticks = theme$axis, col.axis = theme$axis, cex.axis = 0.8 * theme$cex)
  graphics::mtext("Time (h)", 1, line = 2.2, col = theme$axis, cex = 0.85 * theme$cex)
  graphics::mtext("Depth (m)", 2, line = 3.4, col = theme$ink, cex = 0.9 * theme$cex, font = 2)
  graphics::mtext(sprintf("%s  \u00b7  %d dive%s  \u00b7  reference shown dashed", d$id, d$n_dives,
                          if (d$n_dives != 1) "s" else ""),
                  3, line = 0.5, adj = 0, font = 2, col = theme$ink, cex = 0.95 * theme$cex)
  graphics::box(col = theme$axis)
}

#' Panel 2: threshold sensitivity. A choice on a cliff is a choice worth revisiting.
#' @keywords internal
#' @noRd
.drawDiveSweepPanel <- function(d, theme) {
  graphics::par(mar = c(3.8, 5.0, 2.6, 1.2), mgp = c(3, 0.55, 0), tcl = -0.22)
  if (is.null(d$sweep) || !nrow(d$sweep)) { .drawEmptyPanel("no sensitivity sweep available"); return(invisible()) }
  s <- d$sweep
  graphics::plot(NA, xlim = range(s$threshold), ylim = c(0, max(s$n_dives, 1)), axes = FALSE, xlab = "", ylab = "")
  graphics::rect(graphics::par("usr")[1], graphics::par("usr")[3], graphics::par("usr")[2],
                 graphics::par("usr")[4], col = theme$panel, border = NA)
  graphics::lines(s$threshold, s$n_dives, col = theme$ink, lwd = 1.6)
  graphics::points(s$threshold, s$n_dives, pch = 16, cex = 0.7, col = theme$ink)
  graphics::abline(v = d$settings$depth.threshold, col = "#c62828", lwd = 1.5)
  graphics::axis(1, col = NA, col.ticks = theme$axis, col.axis = theme$axis, cex.axis = 0.8 * theme$cex)
  graphics::axis(2, las = 1, col = NA, col.ticks = theme$axis, col.axis = theme$axis, cex.axis = 0.8 * theme$cex)
  graphics::mtext("Depth threshold (m)", 1, line = 2.2, col = theme$axis, cex = 0.85 * theme$cex)
  graphics::mtext("Dives detected", 2, line = 3.4, col = theme$ink, cex = 0.9 * theme$cex, font = 2)
  graphics::mtext("Threshold sensitivity", 3, line = 0.9, adj = 0, font = 2, col = theme$ink, cex = 0.95 * theme$cex)
  graphics::mtext("choose the threshold BEFORE looking at your response variable", 3, line = 0.05, adj = 0,
                  col = theme$subtitle, cex = 0.72 * theme$cex)
  graphics::box(col = theme$axis)
}

#' Panel 3: representative dive profiles with the phase boundaries drawn on.
#'
#' This is the panel that makes the phase rule auditable: a reader sees where descent was judged to end
#' and ascent to begin, on real profiles, rather than trusting a summary duration.
#' @keywords internal
#' @noRd
.drawDivePhasePanel <- function(d, theme) {
  ex <- d$examples
  graphics::par(mar = c(3.6, 5.0, 2.4, 1.0), mgp = c(3, 0.55, 0), tcl = -0.22)
  if (!length(ex)) { .drawEmptyPanel("no dives to illustrate"); return(invisible()) }
  cols <- c(descent = "#1565c0", bottom = "#2e7d32", ascent = "#ef6c00", inter_dive = "grey70")
  graphics::layout(matrix(seq_len(length(ex)), 1, length(ex)))
  for (e in ex) {
    graphics::par(mar = c(3.6, 4.4, 2.4, 0.8))
    graphics::plot(NA, xlim = range(e$t), ylim = rev(range(e$depth, na.rm = TRUE)),
                   axes = FALSE, xlab = "", ylab = "")
    graphics::rect(graphics::par("usr")[1], graphics::par("usr")[3], graphics::par("usr")[2],
                   graphics::par("usr")[4], col = theme$panel, border = NA)
    graphics::lines(e$t, e$baseline, col = "#c62828", lwd = 1.1, lty = 2)
    # colour each sample by the phase it was assigned
    for (q in names(cols)) {
      w <- which(e$phase == q)
      if (length(w)) graphics::points(e$t[w], e$depth[w], col = cols[[q]], pch = 16, cex = 0.45)
    }
    graphics::axis(1, col = NA, col.ticks = theme$axis, col.axis = theme$axis, cex.axis = 0.75 * theme$cex)
    graphics::axis(2, las = 1, col = NA, col.ticks = theme$axis, col.axis = theme$axis, cex.axis = 0.75 * theme$cex)
    graphics::mtext("Time (s)", 1, line = 2.1, col = theme$axis, cex = 0.78 * theme$cex)
    graphics::mtext(sprintf("dive %d", e$id), 3, line = 0.4, adj = 0, font = 2,
                    col = theme$ink, cex = 0.85 * theme$cex)
    graphics::box(col = theme$axis)
  }
  graphics::layout(1)
}

#' Render every gathered dive bundle to the requested devices.
#' @keywords internal
#' @noRd
.renderDiveDiagnostic <- function(bundles, plot = FALSE, plot.file = NULL, theme = plotTheme()) {
  bundles <- Filter(Negate(is.null), bundles)
  if (!length(bundles)) return(invisible(NULL))
  draw <- function(to.file = FALSE, unicode = TRUE) {
    old <- graphics::par(family = theme$font.family, no.readonly = TRUE)
    on.exit(graphics::par(old), add = TRUE)
    for (b in bundles) {
      graphics::layout(matrix(c(1, 2), 2, 1), heights = c(1.25, 1))
      .drawDiveTracePanel(b, theme)
      .drawDiveSweepPanel(b, theme)
      graphics::layout(1)
      .drawDivePhasePanel(b, theme)
    }
  }
  .renderToDevices(draw, plot = plot, plot.file = plot.file, width = 11, height = 8.5, cairo = TRUE)
}
