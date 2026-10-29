# Sensor integrity and quality checks, diagnostics and reporting.

# checkSensorIntegrity helpers ----

################################################################################
# Check registry (internal) ####################################################
################################################################################

#' The integrity checks available: the two default error-severity checks, plus opt-in plausibility checks.
#' @keywords internal
#' @noRd
.integrityChecks <- function() c("duplication", "dead", "saturation", "mag.plausibility",
                                 "mag.break", "accel.scale", "gyro.bias", "paddle.contamination",
                                 "dropout")

#' Checks whose findings describe the RECORD rather than a channel's trustworthiness, and which `apply`
#' therefore never acts on. They are still graded and reported; only the exclusion is barred.
#' @keywords internal
#' @noRd
.integrityAdvisoryChecks <- function() c("mag.break")

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
                 mag.plausibility = .icheckMagPlausibility, mag.break = .icheckMagBreak,
                 accel.scale = .icheckAccelScale,
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
    if (all(is.finite(corrs)) && all(corrs > ctx$control$duplication.error)) {
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
  # The paddle channels are screened here and NOT by adding them to `ctx$fams`. `.icheckSaturation`
  # (:503) and `.icheckDropout` (:719) build their candidate sets from the same object, and a paddle
  # column is legitimately sparse - CATS logs Velocity at 1 Hz onto a 20 Hz grid and
  # regularizeTimeSeries leaves sub-rate channels uninterpolated, so a HEALTHY paddle is ~95% NA
  # (measured on PIN_CAM_41: 88,710 finite values in 1,773,823 rows). Widening the shared families
  # would make `dropout` fire on every working paddle wheel. This check is safe because it filters to
  # the finite values before testing them.
  paddle <- c("paddle_speed", "paddle_freq")
  cand <- intersect(c(unlist(unname(ctx$fams)), "temp", paddle), names(x))
  for (ch in cand) {
    v <- x[[ch]]; v <- v[is.finite(v)]
    if (length(v) < 10L) next
    if (diff(range(v)) < 1e-9) {
      msg <- if (ch %in% paddle) {
        # the two cases differ in cause, not in consequence: either way the column is a placeholder
        # rather than a measurement, and left in place it would count as that many genuine speed
        # samples in any pooled statistic
        sprintf("%s holds one fixed value (%g) for the whole deployment - %s", ch, v[1],
                if (isTRUE(ctx$paddle_wheel)) "a dead or jammed paddle wheel, not a measurement"
                else "no paddle wheel is recorded for this tag, so this is a constant placeholder")
      } else {
        sprintf("%s is constant over the whole deployment (dead/stuck sensor)", ch)
      }
      rows <- rbind(rows, .integrityIssue(ch, "dead", "error", 0, msg))
    }
  }
  rows
}

#' Saturation (warning): a channel pinned at its dynamic-range rail (its exact min or max) for a
#' sustained fraction of samples - clipping. Scoped to the IMU/mag channels via `ctx$fams`
#' (\code{.imuFamilies()}), which is what makes the two-sided min/max test safe: depth would fail it
#' spuriously, since a depth series is floored at the surface and `mean(v == min(v))` would measure time
#' spent at the surface (up to 70% of a real whale-shark record) rather than clipping. Physical-range
#' faults on depth and temperature are [checkSensorQuality()]'s job, not this one's.
#' @keywords internal
#' @noRd
.icheckSaturation <- function(x, ctx) {
  rows <- .emptyIntegrityIssues()
  for (ch in intersect(unlist(unname(ctx$fams)), names(x))) {
    v <- x[[ch]]; v <- v[is.finite(v)]
    if (length(v) < 50L) next
    frac <- max(mean(v == max(v)), mean(v == min(v)))
    sev <- .integrityGrade(frac, warning = ctx$control$saturation.warning,
                                 error   = ctx$control$saturation.error)
    if (!is.na(sev))
      rows <- rbind(rows, .integrityIssue(ch, "saturation", sev, round(frac, 4),
                    sprintf("%s is pinned at its range limit for %.1f%% of samples (clipping)%s", ch, 100 * frac,
                            if (identical(sev, "error")) " - the channel has lost the dynamic range quantitative use requires" else "")))
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
  # warning only: across a 52-deployment fleet this metric is continuous (0.155-0.460, no break), so an
  # automatic error grade would be false precision - see the Details section of checkSensorIntegrity().
  if (!is.na(.integrityGrade(cv, warning = ctx$control$mag.plausibility.warning)))
    rows <- rbind(rows, .integrityIssue("mag", "mag.plausibility", "warning", round(cv, 3),
                  sprintf("magnetometer field |B| is unstable (robust CV %.2f) - possible mis-scaling or a corrupt channel", cv)))
  rows
}

#' Magnetometer break (warning): a persistent step in the field magnitude mid-deployment, i.e. the local
#' magnetic environment changed - contamination attaching or shedding - so ONE calibration cannot serve
#' the whole record. Detection only; nothing is corrected or segmented.
#'
#' THE METRIC IS RANK SEPARATION, NOT STEP SIZE, and that choice is the whole check. Calibrated on a
#' 52-deployment fleet: a best-split difference in median |m| does NOT separate a real break from a
#' healthy record, because a badly contaminated magnetometer's |m| depends strongly on heading, so an
#' animal that keeps changing heading swings |m| between levels all deployment and any unbalanced swing
#' produces a large median gap. Thirteen deployments outranked the one known true positive on that
#' statistic. What distinguishes a break is that the level changes and does NOT come back. So the metric
#' is the Mann-Whitney probability of superiority (AUC) between the two segments' window medians:
#' 1.0 = the two levels do not overlap at all, 0.5 = indistinguishable. It is rank-based, hence unit-free
#' and independent of the noise amplitude, and an oscillating series scores near 0.5 however large its
#' swings (the worst oscillator in the fleet scores 0.74, stable to +/-0.01 across window counts).
#'
#' Deliberately computed on RAW |m|, not the hard-iron-corrected field the `mag.plausibility` check uses:
#' a whole-record centre fitted across a step lands between the two states and absorbs part of it (on the
#' known true positive that reduced an ~11 uT change to 1.3 uT). For the same reason the per-segment
#' centres are not compared - a whale shark does not rotate through enough headings in half a deployment
#' to fit them, and the known true positive fails the coverage gate on both sides.
#' @keywords internal
#' @noRd
.icheckMagBreak <- function(x, ctx) {
  rows <- .emptyIntegrityIssues()
  sc <- .magBreakScan(x, ctx$fams$mag)
  if (is.null(sc)) return(rows)
  # a step must also be big enough to matter: perfect separation across a negligible shift is a
  # stable sensor drifting, not a change of magnetic environment
  if (!is.finite(sc$rel) || sc$rel < .integrityMethod()$mag.break.min.rel) return(rows)
  # warning only: across the fleet this metric is continuous below the flagged group (0.94 down to
  # 0.59, no break), so an automatic error grade would be false precision - the same reasoning that
  # leaves mag.plausibility warning-only. See the Details section of checkSensorIntegrity().
  if (!is.na(.integrityGrade(sc$auc, warning = ctx$control$mag.break.warning))) {
    when <- format(sc$break_time, "%Y-%m-%d %H:%M")
    rows <- rbind(rows, .integrityIssue("mag", "mag.break", "warning", round(sc$auc, 3),
                  sprintf(paste0("magnetometer field steps %.1f %s (%.0f%%) around %s UTC and does not return ",
                                 "(separation %.2f) - the local magnetic environment changed, so one ",
                                 "calibration cannot serve the whole record"),
                          sc$step, "\u00b5T", 100 * sc$rel, when, sc$auc)))
  }
  rows
}

#' Shared scan behind the `mag.break` check AND its diagnostic panel, so the number reported and the
#' picture drawn can never disagree. Returns NULL when the record cannot support the test (no
#' magnetometer, no datetime, too few windows) - abstaining rather than guessing.
#' @keywords internal
#' @noRd
.magBreakScan <- function(x, magcols) {
  if (!all(magcols %in% names(x)) || !"datetime" %in% names(x)) return(NULL)
  mp <- .integrityMethod()
  ok <- is.finite(x[[magcols[1]]]) & is.finite(x[[magcols[2]]]) & is.finite(x[[magcols[3]]]) & !is.na(x$datetime)
  if (sum(ok) < 500L) return(NULL)
  tt <- as.numeric(x$datetime[ok])
  B  <- sqrt(x[[magcols[1]]][ok]^2 + x[[magcols[2]]][ok]^2 + x[[magcols[3]]][ok]^2)
  # fixed window DURATION, not a fixed count, so the statistic means the same thing on a 5 h and a
  # 50 h record (with a fixed count the window length - and hence the median's noise - scales with
  # the deployment, and the threshold would silently mean something different for each)
  w   <- floor((tt - min(tt)) / mp$mag.break.window)
  agg <- stats::aggregate(list(b = B, t = tt), by = list(w = w), FUN = stats::median)
  agg <- agg[as.vector(table(w)) >= 10L, , drop = FALSE]
  agg <- agg[order(agg$w), , drop = FALSE]
  n <- nrow(agg)
  # abstain on a record too short to show persistence on BOTH sides of a break
  if (n < mp$mag.break.min.windows) return(NULL)
  v  <- agg$b
  lo <- max(2L, ceiling(mp$mag.break.min.frac * n)); hi <- n - lo
  if (hi <= lo) return(NULL)
  best <- NULL
  for (k in lo:hi) {
    r <- rank(c(v[seq_len(k)], v[(k + 1L):n])); na <- k; nb <- n - k
    p <- (sum(r[seq_len(na)]) - na * (na + 1) / 2) / (na * nb)
    s <- max(p, 1 - p)
    if (is.null(best) || s > best$s) best <- list(k = k, s = s)
  }
  mA <- stats::median(v[seq_len(best$k)]); mB <- stats::median(v[(best$k + 1L):n])
  fld <- stats::median(v)
  list(t = as.POSIXct(agg$t, origin = "1970-01-01", tz = "UTC"), b = v, k = best$k, auc = best$s,
       step = abs(mA - mB), rel = if (is.finite(fld) && fld > 0) abs(mA - mB) / fld else NA_real_,
       level_a = mA, level_b = mB,
       break_time = as.POSIXct(agg$t[best$k], origin = "1970-01-01", tz = "UTC"))
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
  # graded on the DEVIATION from 1 g (the metric reported stays the magnitude itself, which is what a
  # user needs to see): a small departure is an imperfect calibration, a large one a unit/scale error.
  sev <- .integrityGrade(abs(med - 1), warning = ctx$control$accel.scale.warning,
                                       error   = ctx$control$accel.scale.error)
  if (!is.na(sev))
    rows <- rbind(rows, .integrityIssue("accel", "accel.scale", sev, round(med, 3),
                  sprintf("static acceleration magnitude is %.2f g (expected ~1 g) - %s", med,
                          if (identical(sev, "error")) "a scaling or unit error (e.g. left in m/s^2), not a calibration offset"
                          else "possible scaling/calibration error")))
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
    if (is.finite(rel) && rel > ctx$control$gyro.bias.info && max(abs(meds)) > .integrityMethod()$gyro.bias.min) {
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
  mp  <- .integrityMethod()                                  # internal search-band parameters
  hi  <- mp$paddle.max.freq.frac * (ctx$fs / 2)              # ceiling below Nyquist (reject aliasing)
  best <- list(axis = NA_character_, freq = NA_real_, prom = 0, floor = NA_real_)
  for (ch in cols) {
    pg <- ctx$psd[[ch]]                                       # reuse the precomputed Welch PSD
    if (is.null(pg)) pg <- tryCatch(.welchPSD(x[[ch]], ctx$fs), error = function(e) NULL)
    if (is.null(pg)) next
    # tail-beat fundamental = the dominant low-frequency peak (the swimming signal); the paddle search
    # floor is set above its harmonics so the body oscillation is never counted as paddle contamination.
    lo   <- pg$freq >= 0.1 & pg$freq <= 2
    f_tb <- if (any(lo)) pg$freq[lo][which.max(pg$power[lo])] else NA_real_
    floor <- max(mp$paddle.min.freq, if (is.finite(f_tb)) mp$paddle.harmonic.guard * f_tb else 0)
    band <- pg$freq >= floor & pg$freq <= hi
    if (sum(band) < 5L) next
    p <- pg$power[band]; f <- pg$freq[band]
    med <- stats::median(p)
    if (!is.finite(med) || med <= 0) next
    prom <- max(p) / med
    if (prom > best$prom) best <- list(axis = ch, freq = f[which.max(p)], prom = prom, floor = floor)
  }
  if (!is.na(.integrityGrade(best$prom, warning = cc$paddle.warning)))
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
    if (frac > ctx$control$dropout.info)
      rows <- rbind(rows, .integrityIssue(ch, "dropout", "info", round(frac, 3),
                    sprintf("%s is missing (NA) for %.0f%% of the deployment", ch, 100 * frac)))
  }
  rows
}

################################################################################
# Findings helpers + verbose + plot (internal) #################################
################################################################################

# Empty findings table with the canonical schema.
# @keywords internal
# @noRd
#' Grade a measured metric into a severity, from ordered thresholds.
#'
#' The single place severity is decided, and the reason it is a property of the MEASUREMENT rather than of
#' the check type: the same check yields `"warning"` or `"error"` depending on how large its metric is.
#' Thresholds are exclusive (`>`), checked from the most severe down, and a `NULL` threshold means the
#' check does not offer that grade - so a continuous metric with no defensible error cut-off simply passes
#' `error = NULL` and can never be graded one.
#' @param metric The measured value. @param info,warning,error Thresholds, or NULL where not offered.
#' @return `"error"`, `"warning"`, `"info"`, or `NA_character_` for "no finding".
#' @keywords internal
#' @noRd
.integrityGrade <- function(metric, info = NULL, warning = NULL, error = NULL) {
  if (!is.finite(metric)) return(NA_character_)
  if (!is.null(error)   && metric > error)   return("error")
  if (!is.null(warning) && metric > warning) return("warning")
  if (!is.null(info)    && metric > info)    return("info")
  NA_character_
}

#' Severity rank, for comparing a finding against an `apply.severity` floor.
#' @keywords internal
#' @noRd
.severityRank <- function(x) match(x, c("info", "warning", "error"))

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
  label <- switch(check, mag.plausibility = "mag plausibility", mag.break = "mag break",
                  accel.scale = "accel scale",
                  gyro.bias = "gyro bias", paddle.contamination = "paddle", check)
  if (!nrow(hit)) return(sprintf("%s: none", label))
  switch(check,
    duplication      = paste0("duplication: ", paste(sprintf("%s \u2261 accel (r %.4f)", hit$channel, hit$metric), collapse = " \u00b7 ")),
    dead             = paste0("dead: ", paste(.channelsToFamilies(hit$channel), collapse = ", ")),
    saturation       = paste0("saturation: ", paste(sprintf("%s (%.1f%%)", hit$channel, 100 * hit$metric), collapse = ", ")),
    mag.plausibility = sprintf("mag plausibility: |B| CV %.2f", hit$metric[1]),
    mag.break        = sprintf("mag break: separation %.2f", hit$metric[1]),
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
  mag.break = list(label = "mBrk", title = "Magnetometer field over time",
    hint = "Window medians of raw |m|. A healthy record is noisy but stationary; a step that persists to the end (and never returns) means the local magnetic environment changed, so one calibration cannot serve the whole record."),
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
  order_ck <- c("duplication", "dead", "saturation", "mag.plausibility", "mag.break", "accel.scale", "gyro.bias", "paddle.contamination", "dropout")
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
      # recomputed through the SAME helper the check used, so the panel cannot show a different
      # break from the one that was reported
      mag.break = {
        sc <- .magBreakScan(x, fams$mag)
        if (is.null(sc)) NULL else list(check = ck, sev = sev, t = sc$t, b = sc$b, k = sc$k,
                                        auc = sc$auc, step = sc$step, rel = sc$rel,
                                        level_a = sc$level_a, level_b = sc$level_b)
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
          mp <- .integrityMethod()
          floor <- max(mp$paddle.min.freq, if (is.finite(f_tb)) mp$paddle.harmonic.guard * f_tb else 0)
          hi <- mp$paddle.max.freq.frac * (fs / 2)
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
  mag.plausibility = .ipanelMagB(pan, uni), mag.break = .ipanelMagBreak(pan, uni),
  accel.scale = .ipanelAccelScale(pan, uni),
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
.ipanelMagBreak <- function(pan, uni) {
  meta <- .integrityCheckMeta()$mag.break; st <- .isevStatus(pan$sev, uni)
  op <- graphics::par(mar = c(4.4, 4, 2.6, 1)); on.exit(graphics::par(op), add = TRUE)
  hrs <- as.numeric(difftime(pan$t, min(pan$t), units = "hours"))
  graphics::plot(hrs, pan$b, type = "l", col = "grey30", xlab = "Hours into deployment",
                 ylab = "median |m| per window", cex.main = 0.95, cex.axis = 0.8, cex.lab = 0.9, las = 1,
                 main = paste0(st$pre, meta$title, sprintf(" (separation %.2f)", pan$auc)))
  # the two levels, and where they part: this is what makes a step distinguishable by eye from the
  # repeated swings a contaminated magnetometer shows on a heading-changing animal
  graphics::segments(hrs[1], pan$level_a, hrs[pan$k], pan$level_a, col = "#2e7d32", lwd = 2)
  graphics::segments(hrs[pan$k], pan$level_b, hrs[length(hrs)], pan$level_b, col = "#c62828", lwd = 2)
  graphics::abline(v = hrs[pan$k], col = "grey45", lty = 2, lwd = 1.5)
  graphics::legend("topright", bty = "n", cex = 0.7, lty = c(1, 1, 2), lwd = c(2, 2, 1.5),
                   col = c("#2e7d32", "#c62828", "grey45"),
                   legend = c(sprintf("before (%.1f)", pan$level_a),
                              sprintf("after (%.1f)", pan$level_b),
                              sprintf("break (%.0f%% step)", 100 * pan$rel)))
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

# checkSensorQuality helpers ----

################################################################################
# Per-channel detection engine (internal) ######################################
################################################################################

#' Detect anomalies in one channel and return a repair PLAN, WITHOUT touching `dt`.
#'
#' The exact detection + repair algorithm runs on a working copy of the channel vector; the function
#' returns the repaired vector plus the flagged / interpolated / block indices and counts. The driver
#' writes the repaired vector back only when `apply = TRUE`, so a report-only run leaves the data
#' untouched. `severity` is `"warning"` when a malfunction/stall block was found (a sustained failure) and
#' `"info"` when only isolated spikes were.
#' @keywords internal
#' @noRd
.detectChannelAnomalies <- function(dt, sensor.col, datetime.col, ctrl, interpolate) {
  rate.threshold    <- ctrl$rate.threshold
  sensor.resolution <- ctrl$sensor.resolution
  outlier.window    <- ctrl$outlier.window
  stall.threshold   <- ctrl$stall.threshold

  times <- dt[[datetime.col]]
  v <- dt[[sensor.col]]                              # WORKING COPY - the algorithm mutates this, never dt

  time_diffs <- round(as.numeric(diff(times), units = "secs"), 6)
  nominal_interval <- stats::median(time_diffs, na.rm = TRUE)
  sampling_freq <- round(1 / nominal_interval, 1)
  dt_secs <- c(NA, as.numeric(diff(times), units = "secs"))
  value_diff <- c(NA, diff(v))

  # Rate of change, gated so ordinary quantisation and high-frequency noise are not read as events.
  #
  # CAUTION, and read this before "fixing" the line below. It compares a VALUE (units) against
  # `sensor.resolution / dt` (units per second), which is dimensionally incoherent, and the obvious
  # correction - comparing like with like, `abs(value_diff) > sensor.resolution` - is WORSE. It was
  # tried and reverted. Measurement noise of standard deviation s produces apparent rates of about
  # s*sqrt(2)/dt, which grows without bound as the sampling interval shrinks, so at a high rate ANY
  # fixed physical rate threshold is exceeded by noise alone. Measured on the 16 Hz / sd 0.3 fixture in
  # test-checkSensorQuality.R: typical |diff| is 0.42, an apparent 6.8 units/s against a 2 units/s
  # threshold, and the dimensionally-correct gate mass-flagged the whole series.
  #
  # So the `/ dt` is doing real work: it is the only term that scales the floor with the sampling
  # interval. It is the wrong QUANTITY for that job - the thing that should scale is the measured NOISE
  # of the channel, not its quantisation step - but replacing it needs a noise-floor estimator and a
  # decision about how to expose it, not a units patch. Left as is, deliberately, and flagged.
  rate_uncertainty <- sensor.resolution / dt_secs
  rate_of_change <- ifelse(abs(value_diff) > rate_uncertainty, value_diff / dt_secs, NA)

  # isolated jumps + prolonged constant runs (stalls; zero values excused, e.g. depth at the surface)
  jump_indices <- which(abs(rate_of_change) > rate.threshold & !is.na(rate_of_change))
  stall_threshold_samples <- sampling_freq * 60 * stall.threshold
  stall_indices <- c()
  rle_sensor <- rle(v)
  stall_periods <- rle_sensor$lengths >= stall_threshold_samples & (rle_sensor$values > 0)
  if (any(stall_periods)) {
    rle_indices <- cumsum(rle_sensor$lengths)
    start_indices <- rle_indices[stall_periods] - rle_sensor$lengths[stall_periods] + 1
    end_indices <- rle_indices[stall_periods]
    for (a in seq_along(start_indices)) stall_indices <- c(stall_indices, start_indices[a]:end_indices[a])
  }
  outlier_indices <- unique(sort(c(jump_indices, stall_indices)))

  flagged_outliers <- c(); interpolated_outliers <- c(); block_indices <- c()
  n_corr <- 0L; affected_secs <- 0; pct_affected <- 0
  isolated_outliers_count <- 0; malfunction_blocks_count <- 0; malfunction_duration <- 0
  anomalies <- length(outlier_indices) > 0

  if (anomalies) {
    anomaly_window <- outlier.window * 60 * sampling_freq
    outlier_groups <- split(outlier_indices, cumsum(c(1, diff(outlier_indices) > anomaly_window)))
    for (group in outlier_groups) {
      if (length(group) == 2) {
        idx1 <- group[1]; idx2 <- group[2]
        remove_idx <- if (abs(v[idx1]) > abs(v[idx2])) idx1 else idx2
        flagged_outliers <- c(flagged_outliers, remove_idx)
        v[remove_idx] <- NA
        isolated_outliers_count <- isolated_outliers_count + 1
        if (interpolate) {
          interpolated_outliers <- c(interpolated_outliers, remove_idx)
          window <- max(1, remove_idx - 1):min(length(v), remove_idx + 1)
          local_y <- v[window]
          v[remove_idx] <- zoo::na.approx(local_y, na.rm = FALSE)[which(window == remove_idx)]
        }
      } else {
        flagged_outliers <- c(flagged_outliers, group); block_indices <- c(block_indices, group)
        v[group] <- NA
        malfunction_blocks_count <- malfunction_blocks_count + length(group)
        duration <- difftime(max(times[group]), min(times[group]), units = "secs")
        malfunction_duration <- malfunction_duration + as.numeric(duration)
        if (!interpolate) {
          isolated_flags <- which(.findIsolated(v))
          if (length(isolated_flags) > 0) { flagged_outliers <- c(flagged_outliers, isolated_flags); v[isolated_flags] <- NA }
        }
      }
    }
    if (interpolate && malfunction_blocks_count > 0) {
      isolated_flags <- which(.findIsolated(v))
      if (length(isolated_flags) > 0) { flagged_outliers <- c(flagged_outliers, isolated_flags); v[isolated_flags] <- NA }
    }
    n_corr <- isolated_outliers_count + malfunction_blocks_count
    affected_secs <- malfunction_duration + (if (sampling_freq > 0) isolated_outliers_count / sampling_freq else 0)
    span_secs <- as.numeric(difftime(max(times), min(times), units = "secs"))
    pct_affected <- if (is.finite(span_secs) && span_secs > 0) 100 * affected_secs / span_secs else 0
  }

  severity <- if (malfunction_blocks_count > 0) "warning" else if (isolated_outliers_count > 0) "info" else NA_character_
  list(anomalies = anomalies, n_corr = n_corr, affected_secs = affected_secs, pct_affected = pct_affected,
       spikes = isolated_outliers_count, block_values = malfunction_blocks_count, severity = severity,
       sampling_freq = sampling_freq, flagged_outliers = sort(unique(flagged_outliers)),
       interpolated_outliers = sort(unique(interpolated_outliers)), block_indices = sort(unique(block_indices)),
       repaired = v)
}

#' One detailed-verbose line for a channel's outcome.
#' @keywords internal
#' @noRd
.qualityChannelLine <- function(ch, res) {
  if (!res$anomalies) return(sprintf("%s: clean", ch))
  sprintf("%s: %s anomal%s \u00b7 %s (%.1f%%)", ch, .formatNumber(res$n_corr),
          if (res$n_corr == 1) "y" else "ies", .formatDuration(res$affected_secs), res$pct_affected)
}

#' Display label for a channel (title-cased, separators to spaces), e.g. "depth" -> "Depth".
#' @keywords internal
#' @noRd
.sensorLabel <- function(ch) tools::toTitleCase(gsub("[._]+", " ", ch))

#' Split a sorted index vector into contiguous runs (each a run of consecutive indices).
#' @keywords internal
#' @noRd
.contiguousRuns <- function(idx) {
  if (!length(idx)) return(list())
  idx <- sort(unique(idx))
  brk <- c(0L, which(diff(idx) > 1L), length(idx))
  lapply(seq_len(length(brk) - 1L), function(k) idx[(brk[k] + 1L):brk[k + 1L]])
}

#' Empty quality-issues table with the canonical schema.
#' @keywords internal
#' @noRd
.emptyQualityIssues <- function() {
  data.frame(id = character(0), channel = character(0), severity = character(0),
             n_corrected = integer(0), spikes = integer(0), blocks = integer(0),
             pct_affected = numeric(0), message = character(0), stringsAsFactors = FALSE)
}

#' One quality-issues row for a flagged channel.
#' @keywords internal
#' @noRd
.qualityIssue <- function(id, ch, res, interpolate) {
  n_blocks <- length(.contiguousRuns(res$block_indices))
  parts <- c(if (res$spikes > 0) sprintf("%d isolated spike%s", res$spikes, if (res$spikes != 1) "s" else ""),
             if (n_blocks > 0) sprintf("%d malfunction/stall block%s", n_blocks, if (n_blocks != 1) "s" else ""))
  treat <- if (interpolate && res$spikes > 0 && n_blocks == 0) "interpolated" else "removed"
  data.frame(id = id, channel = ch, severity = res$severity,
             n_corrected = as.integer(res$n_corr), spikes = as.integer(res$spikes), blocks = as.integer(n_blocks),
             pct_affected = round(res$pct_affected, 3),
             message = sprintf("%s (%s, %.1f%% of the record; %s)", paste(parts, collapse = " + "),
                               .formatDuration(res$affected_secs), res$pct_affected, treat),
             stringsAsFactors = FALSE)
}

#' A short one-line verdict for a tag (overview cell / page title).
#' @keywords internal
#' @noRd
.qualityVerdict <- function(iss) {
  if (!nrow(iss)) return("clean")
  n_tot <- sum(iss$n_corrected)
  sprintf("%s anomal%s \u00b7 %.1f%% affected", .formatNumber(n_tot), if (n_tot == 1) "y" else "ies", max(iss$pct_affected))
}

#' One overview-table record for a tag: per-channel severity + count, plus the overall verdict.
#' @keywords internal
#' @noRd
.qualitySummaryRecord <- function(id, channels, iss) {
  per_ch <- lapply(channels, function(ch) {
    h <- iss[iss$channel == ch, , drop = FALSE]
    if (!nrow(h)) NULL else list(severity = h$severity[1], n = h$n_corrected[1], pct = h$pct_affected[1])
  })
  names(per_ch) <- channels
  worst <- if (any(iss$severity == "warning")) "warning" else if (nrow(iss)) "info" else "pass"
  list(id = id, per_ch = per_ch, worst = worst, verdict = .qualityVerdict(iss))
}

#' Build the small draw payload for one flagged channel: a decimated trace plus the removed-spike /
#' interpolated points (at their original values) and the malfunction/stall block spans (kept tiny so
#' the report can be drawn after the main loop). Built from the ORIGINAL channel (detection never mutates).
#' @keywords internal
#' @noRd
.qualityChannelPanel <- function(x, ch, datetime.col, res) {
  times <- as.numeric(x[[datetime.col]]); tz <- attr(x[[datetime.col]], "tzone"); v <- x[[ch]]
  tr <- .decimateForPlot(times, v, 3000L)
  interp_idx  <- res$interpolated_outliers
  block_idx   <- res$block_indices
  removed_idx <- setdiff(res$flagged_outliers, c(block_idx, interp_idx))   # isolated removed spikes only
  spans <- lapply(.contiguousRuns(block_idx), function(g) c(times[g[1]], times[g[length(g)]]))
  list(check = "quality", ch = ch, sev = res$severity, is_depth = grepl("depth", ch, ignore.case = TRUE),
       tz = if (is.null(tz) || !nzchar(tz)) "UTC" else tz, trace = tr,
       removed = list(t = times[removed_idx], y = v[removed_idx]),
       interp  = list(t = times[interp_idx],  y = v[interp_idx]),
       spans = spans, pct = res$pct_affected, n = res$n_corr,
       label = .sensorLabel(ch))
}

################################################################################
# Diagnostic report (internal) - overview page + per-flagged-tag pages #########
################################################################################

#' Overview summary page: one row per tag (worst-first), a per-channel cell showing the anomaly count
#' (amber = a stall/malfunction block, grey = spikes only, dot = clean), and a plain-language verdict.
#' Mirrors the checkSensorIntegrity run-summary page.
#' @keywords internal
#' @noRd
.drawQualitySummaryPage <- function(records, channels, stats, use_unicode = TRUE) {
  oldpar <- graphics::par(no.readonly = TRUE); on.exit(graphics::par(oldpar), add = TRUE)
  rankv <- c(warning = 0, info = 1, pass = 2)
  worst_pct <- function(r) { v <- vapply(r$per_ch, function(c) if (is.null(c)) 0 else c$pct, numeric(1)); if (length(v)) max(v) else 0 }
  if (length(records))
    records <- records[order(vapply(records, function(r) rankv[[r$worst]], numeric(1)), -vapply(records, worst_pct, numeric(1)))]
  ell <- if (use_unicode) "\u2026" else ".."; g_pass <- if (use_unicode) "\u00b7" else "."
  sevcol <- c(warning = "darkorange", info = "grey45", pass = "grey80")
  idcol  <- function(w) switch(w, warning = "darkorange", info = "grey30", "grey55")
  truncr <- function(s, n) if (nchar(s) > n) paste0(substr(s, 1, n - 1L), ell) else s

  cx_id <- 0.004; cx_verd <- 0.62
  cx_ch <- stats::setNames(seq(0.16, 0.55, length.out = length(channels)), channels)
  cex_h <- 0.62; cex_d <- 0.60
  rpp <- 40L; npg <- max(1L, ceiling(max(1L, length(records)) / rpp))
  for (pg in seq_len(npg)) {
    idx <- if (!length(records)) integer(0) else ((pg - 1L) * rpp + 1L):min(pg * rpp, length(records))
    graphics::par(mar = c(1.5, 0.6, 1, 0.6)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))
    graphics::text(0, 0.985, "checkSensorQuality  -  run summary", adj = c(0, 1), font = 2, cex = 1.4)
    graphics::text(1, 0.985, format(Sys.time(), "%Y-%m-%d %H:%M"), adj = c(1, 1), cex = 0.75, col = "grey40")
    graphics::text(0, 0.93, sprintf("%d tags  |  %d with anomalies   %d with a stall/malfunction block   (%s)",
                                    stats$n, stats$n_flagged, stats$n_warn, if (stats$apply) "repaired" else "report only"),
                   adj = c(0, 1), cex = 0.95)
    graphics::text(0, 0.90, "sorted worst-first  -  cell = anomaly count (amber = a block/stall, grey = spikes only, dot = clean)",
                   adj = c(0, 1), cex = 0.72, col = "grey45")
    hcy <- 0.855
    graphics::rect(0, hcy - 0.014, 1, hcy + 0.014, col = "grey90", border = NA)
    graphics::text(cx_id, hcy, "Tag ID", adj = c(0, 0.5), font = 2, cex = cex_h, col = "grey15")
    for (ch in channels) graphics::text(cx_ch[[ch]], hcy, .sensorLabel(ch), adj = c(0.5, 0.5), font = 2, cex = cex_h * 0.9, col = "grey15")
    graphics::text(cx_verd, hcy, "Verdict", adj = c(0, 0.5), font = 2, cex = cex_h, col = "grey15")
    graphics::segments(0, hcy - 0.016, 1, hcy - 0.016, col = "grey55")
    row_top <- hcy - 0.030; dy <- 0.0195
    for (j in seq_along(idx)) {
      r <- records[[idx[j]]]; y <- row_top - dy * (j - 1L)
      if (j %% 2L == 0L) graphics::rect(0, y - dy / 2, 1, y + dy / 2, col = "grey96", border = NA)
      graphics::text(cx_id, y, truncr(r$id, 16), adj = c(0, 0.5), cex = cex_d, font = 2, col = idcol(r$worst))
      for (ch in channels) {
        cc <- r$per_ch[[ch]]
        if (is.null(cc)) graphics::text(cx_ch[[ch]], y, g_pass, adj = c(0.5, 0.5), cex = cex_d, col = sevcol[["pass"]])
        else graphics::text(cx_ch[[ch]], y, .formatNumber(cc$n), adj = c(0.5, 0.5), cex = cex_d, font = 2, col = sevcol[[cc$severity]])
      }
      graphics::text(cx_verd, y, r$verdict, adj = c(0, 0.5), cex = cex_d, col = idcol(r$worst), font = if (r$worst == "warning") 2 else 1)
    }
    if (npg > 1) graphics::mtext(sprintf("summary page %d / %d", pg, npg), side = 1, cex = 0.7, col = "grey50")
  }
  invisible(NULL)
}

#' Draw one flagged tag's page: its anomalous channels in a dynamic layout, each an annotated trace.
#' @keywords internal
#' @noRd
.plotQualityIndividual <- function(pd, use_unicode = TRUE) {
  oldpar <- graphics::par(no.readonly = TRUE); on.exit(graphics::par(oldpar), add = TRUE)
  n <- length(pd$panels); if (!n) return(invisible())
  lay <- .dynamicPanelLayout(n)
  graphics::layout(lay$mat, widths = lay$widths, heights = lay$heights)
  graphics::par(oma = c(0, 0, 2.4, 0), mgp = c(2.4, 0.7, 0))
  for (pan in pd$panels) .qpanelChannel(pan, use_unicode)
  graphics::mtext(sprintf("%s   -   sensor quality   [%s]", pd$id, pd$summary), outer = TRUE, font = 2, cex = 1)
  invisible(NULL)
}

#' One annotated-trace panel: the (decimated) sensor series with removed spikes (red), interpolated
#' values (cyan) and stall/malfunction blocks (shaded red spans), a status border and an interpretation note.
#' @keywords internal
#' @noRd
.qpanelChannel <- function(pan, uni) {
  st <- .isevStatus(pan$sev, uni)
  op <- graphics::par(mar = c(4.4, 4.4, 2.6, 1)); on.exit(graphics::par(op), add = TRUE)
  tr <- pan$trace; tp <- as.POSIXct(tr$x, origin = "1970-01-01", tz = pan$tz)
  ylim <- range(c(tr$y, pan$removed$y, pan$interp$y), na.rm = TRUE)
  if (!all(is.finite(ylim))) ylim <- c(0, 1)
  if (pan$is_depth) ylim <- rev(ylim)                                    # depth: surface at the top
  plot(tp, tr$y, type = "n", axes = FALSE, xlab = "", ylab = pan$label, ylim = ylim,
       main = paste0(st$pre, pan$label, sprintf(" - %s anomal%s (%.1f%% affected)",
                     .formatNumber(pan$n), if (pan$n == 1) "y" else "ies", pan$pct)),
       cex.main = 0.92, cex.lab = 0.9)
  usr <- graphics::par("usr")
  for (sp in pan$spans) graphics::rect(sp[1], usr[3], sp[2], usr[4], col = "#e4181820", border = NA)   # block spans
  graphics::lines(tp, tr$y, col = "grey30", lwd = 0.7)
  if (length(pan$removed$t)) graphics::points(as.POSIXct(pan$removed$t, origin = "1970-01-01", tz = pan$tz), pan$removed$y, pch = 16, col = "red3", cex = 0.8)
  if (length(pan$interp$t))  graphics::points(as.POSIXct(pan$interp$t,  origin = "1970-01-01", tz = pan$tz), pan$interp$y,  pch = 16, col = "cyan3", cex = 0.8)
  .axisTime(tp, n = 6, tz = pan$tz, cex.axis = 0.8)
  graphics::axis(2, cex.axis = 0.8, las = 1); graphics::box()
  leg <- character(0); lcol <- character(0)
  if (length(pan$spans))     { leg <- c(leg, "stall / malfunction block"); lcol <- c(lcol, "#e41818") }
  if (length(pan$removed$t)) { leg <- c(leg, "removed spike");             lcol <- c(lcol, "red3") }
  if (length(pan$interp$t))  { leg <- c(leg, "interpolated");              lcol <- c(lcol, "cyan3") }
  if (length(leg)) graphics::legend("topright", legend = leg, col = lcol, pch = 15, bty = "n", cex = 0.72)
  .panelHint("Sensor trace with detected anomalies: red = removed spikes, cyan = interpolated, shaded spans = sustained sensor failures (stalls / malfunction blocks).")
  if (!is.na(st$col)) graphics::box(col = st$col, lwd = 2)
}

################################################################################
# Isolated-value helper (internal) #############################################
################################################################################

#' Logical mask of "islands": non-NA runs surrounded by NA on both sides (leading/trailing NAs ignored).
#' @keywords internal
#' @noRd
.findIsolated <- function(vec) {
  rle_na <- rle(is.na(vec))
  island_positions <- which(!rle_na$values &
    c(FALSE, rle_na$values[-length(rle_na$values)]) &
    c(rle_na$values[-1], FALSE))
  is_island <- rep(FALSE, length(vec))
  if (length(island_positions)) {
    ends   <- cumsum(rle_na$lengths)
    starts <- ends - rle_na$lengths + 1L
    for (pos in island_positions) is_island[starts[pos]:ends[pos]] <- TRUE
  }
  is_island
}


#######################################################################################################
#######################################################################################################
#######################################################################################################

# Internal integrity-method defaults ----

#' Internal method parameters for the integrity checks (NOT user-facing).
#'
#' These govern HOW a metric is computed, not how it is interpreted: the paddle spectral search band and
#' the gyro-bias absolute floor. They are implementation details of the detectors - deliberately kept out
#' of `integrityControl()` so the algorithms can be improved without an API change - whereas everything a
#' user should reasonably tune (the metric -> severity thresholds) is public there.
#'   \itemize{
#'     \item `gyro.bias.min` - absolute floor (rad/s) a median offset must also clear, so a negligible
#'       offset is not flagged merely because the animal barely rotated (a tiny MAD inflates the ratio).
#'     \item `paddle.min.freq`, `paddle.harmonic.guard` - the search floor is
#'       `max(paddle.min.freq, paddle.harmonic.guard * f_tailbeat)` Hz, keeping it clear of the tail-beat
#'       fundamental and its harmonics (the main source of false positives).
#'     \item `paddle.max.freq.frac` - ceiling as a fraction of Nyquist, avoiding aliasing artefacts.
#'     \item `mag.break.window` - window DURATION (s) the field magnitude is summarised over. A duration
#'       rather than a count, so the statistic means the same thing on a 5 h and a 50 h record.
#'     \item `mag.break.min.frac` - each side of a candidate break must be at least this fraction of the
#'       record. This is what "persistent" means operationally, and it sets the blind spot: a break in
#'       the first or last `mag.break.min.frac` of a record cannot be seen.
#'     \item `mag.break.min.windows` - fewer windows than this and the check abstains rather than guess.
#'     \item `mag.break.min.rel` - the step must also be at least this fraction of the field magnitude,
#'       so perfect rank separation across a negligible shift (a stable sensor drifting) is not flagged.
#'   }
#' @keywords internal
#' @noRd
.integrityMethod <- function() {
  list(gyro.bias.min = 0.02, paddle.min.freq = 3.5, paddle.harmonic.guard = 6, paddle.max.freq.frac = 0.85,
       mag.break.window = 600, mag.break.min.frac = 0.15, mag.break.min.windows = 30L,
       mag.break.min.rel = 0.05)
}
