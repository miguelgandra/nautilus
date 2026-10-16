#' Configure schematic motion for the video renderer
#'
#' @param method Frequency/amplitude backend: `"auto"` (default) selects peaks when
#'   it carries finite positive frequency estimates, otherwise wavelet. `"peaks"` or
#'   `"wavelet"` fixes the backend for the entire render; there is no per-frame switching.
#' @param amplitude.reference Positive peak-to-trough signal amplitude corresponding
#'   to full visual motion, in the selected motion channel's units. `NULL` uses the
#'   95th percentile of valid amplitudes across the full supplied deployment.
#'   Supply a calibrated reference for comparisons across deployments.
#' @param max.angle Maximum schematic bend angle in degrees, between 0 and 45
#'   (default 20). This is a display setting, not a measured anatomical angle.
#' @param phase.offset Initial phase in radians (default 0), reset at each valid segment.
#' @param phase.lag Phase delay in radians toward higher-weight vertices (default pi/3).
#'
#' @details
#' Pass this object as `motion` to [renderOverlayVideo()] to enable motion.
#' The default `motion = NULL` keeps models rigid. Phase is integrated from frequency;
#' it does not recover measured tail position. Peak-to-trough signal amplitude is
#' mapped linearly onto a fraction of `max.angle`, with saturation at `amplitude.reference`.
#' Unavailable estimates show a neutral model labelled as unavailable. An explicit
#' `tbf_swimming = FALSE` shows a neutral model labelled as gliding. Unknown swimming
#' classification does not suppress otherwise valid estimates.
#'
#' @return A validated `nautilus_overlay_motion` control list.
#' @export
overlayMotionControl <- function(method = c("auto", "peaks", "wavelet"),
                                 amplitude.reference = NULL, max.angle = 20,
                                 phase.offset = 0, phase.lag = pi / 3) {
  method <- match.arg(method)
  .assert_number(amplitude.reference, "amplitude.reference", min = .Machine$double.eps, null_ok = TRUE)
  .assert_number(max.angle, "max.angle", min = 0, max = 45)
  .assert_number(phase.offset, "phase.offset")
  .assert_number(phase.lag, "phase.lag", min = 0, max = 2*pi)
  structure(list(method = method, amplitude.reference = amplitude.reference, max.angle = max.angle,
                 phase.offset = phase.offset, phase.lag = phase.lag), class = "nautilus_overlay_motion")
}

.resolveOverlayMotion <- function(motion, data, model) {
  if (is.null(motion)) return(NULL)
  if (!inherits(motion, "nautilus_overlay_motion") || !is.list(motion))
    .abort("{.arg motion} must be {.code NULL} or created by {.fn overlayMotionControl}.")
  motion <- overlayMotionControl(motion$method, motion$amplitude.reference, motion$max.angle,
                                  motion$phase.offset, motion$phase.lag)
  if (model$rig$type == "none")
    .abort("This model has no motion profile. Supply motion weights through {.fn overlayModel}.")
  method <- motion$method
  if (method == "auto") {
    candidates <- c("peaks", "wavelet")
    present <- vapply(candidates, function(m) paste0("tbf_hz_", m) %in% names(data), logical(1))
    positive <- vapply(candidates, function(m) {
      x <- data[[paste0("tbf_hz_", m)]]
      is.numeric(x) && any(is.finite(x) & x > 0)
    }, logical(1))
    if (!any(present)) .abort("Schematic motion requires tail-beat frequency and amplitude columns.")
    method <- candidates[which(if (any(positive)) positive else present)[1L]]
  }
  cols <- paste0(c("tbf_hz_", "tbf_amplitude_"), method)
  for (col in cols) {
    if (!col %in% names(data) || !is.numeric(data[[col]]))
      .abort("Schematic motion requires the numeric column {.val {col}} from the selected backend.")
  }
  if ("tbf_swimming" %in% names(data) && !is.logical(data$tbf_swimming))
    .abort("{.field tbf_swimming} must be logical when supplied.")
  valid <- is.finite(data[[cols[1]]]) & data[[cols[1]]] > 0 &
    is.finite(data[[cols[2]]]) & data[[cols[2]]] >= 0
  if ("tbf_swimming" %in% names(data)) valid <- valid & (is.na(data$tbf_swimming) | data$tbf_swimming)
  reference <- motion$amplitude.reference
  if (is.null(reference)) {
    amp <- data[[cols[2]]][valid & data[[cols[2]]] > 0]
    reference <- if (length(amp)) unname(stats::quantile(amp, .95)) else 1
  }
  motion$method <- method
  motion$amplitude.reference <- reference
  motion$columns <- cols
  motion
}

# Angle interpolation follows the shortest arc; interpolation never bridges invalid samples or long gaps.
.interpolateOverlayColumn <- function(t, x, at, max.gap, circular = FALSE) {
  out <- rep(NA_real_, length(at))
  i <- findInterval(at, t)
  inside <- i > 0L & i <= length(t)
  exact <- inside & at == t[pmax(1L, i)]
  out[exact] <- x[i[exact]]
  between <- which(inside & !exact & i < length(t))
  if (length(between)) {
    left <- i[between]; right <- left + 1L
    ok <- t[right] - t[left] <= max.gap & is.finite(x[left]) & is.finite(x[right])
    between <- between[ok]; left <- left[ok]; right <- right[ok]
    delta <- x[right] - x[left]
    if (circular) delta <- (delta + 180) %% 360 - 180
    out[between] <- x[left] + delta * (at[between] - t[left]) / (t[right] - t[left])
  }
  out[!is.finite(out)] <- NA_real_
  out
}

.prepareOverlayFrameState <- function(data, frame.times, motion = NULL, max.gap = 1,
                                      pitch.cols = "pitch", roll.cols = "roll") {
  .assert_number(max.gap, "max.sensor.gap", min = .Machine$double.eps)
  .assert_columns(data, "datetime", "data")
  t <- as.numeric(data$datetime); at <- as.numeric(frame.times)
  if (!inherits(data$datetime, "POSIXct") || !length(t) || any(!is.finite(t)) || any(diff(t) <= 0))
    .abort("Sensor timestamps must be finite POSIXct values in strictly increasing order; resolve duplicate timestamps first.")
  if (!inherits(frame.times, "POSIXct") || any(!is.finite(at)))
    .abort("Frame times must be finite POSIXct values.")
  rows <- findInterval(at, t)
  frames <- data.table::data.table(datetime = frame.times)
  angle.cols <- intersect(unique(c(pitch.cols, roll.cols, "heading")), names(data))
  for (col in angle.cols) {
    if (!is.numeric(data[[col]])) .abort("Orientation column {.val {col}} must be numeric.")
    value <- .interpolateOverlayColumn(t, data[[col]], at, max.gap,
                                       circular = col %in% c(roll.cols, "heading"))
    if (col == "heading") value <- value %% 360
    else if (col %in% roll.cols) value <- (value + 180) %% 360 - 180
    data.table::set(frames, j = col, value = value)
  }
  phase <- strength <- frequency <- rep(0, length(at))
  status <- rep(if (is.null(motion)) "disabled" else "unavailable", length(at))
  if (!is.null(motion)) {
    f <- data[[motion$columns[1]]]; a <- data[[motion$columns[2]]]
    swimming <- if ("tbf_swimming" %in% names(data)) data$tbf_swimming else rep(NA, length(t))
    valid <- is.finite(f) & f > 0 & is.finite(a) & a >= 0 & (is.na(swimming) | swimming)
    # Integrate once over the complete supplied timeline, before clipping a video interval.
    step <- c(0, diff(t))
    continuation <- valid & c(FALSE, utils::head(valid, -1L)) & step <= max.gap
    increments <- rep(0, length(t))
    k <- which(continuation)
    increments[k] <- pi * (f[k - 1L] + f[k]) * step[k]
    cumulative <- cumsum(increments)
    starts <- which(valid & !continuation)
    segment <- cumsum(valid & !continuation)
    sample.phase <- rep(NA_real_, length(t))
    if (length(starts)) sample.phase[valid] <- motion$phase.offset + cumulative[valid] - cumulative[starts[segment[valid]]]
    inside <- rows > 0L & at <= utils::tail(t, 1L)
    j <- which(inside)
    if (length(j)) {
      left <- rows[j]
      exact <- at[j] == t[left]
      right <- pmin(left + 1L, length(t))
      supported <- valid[left] & (exact | (right > left & continuation[right]))
      glide <- !is.na(swimming[left]) & !swimming[left] &
        (exact | (right > left & t[right] - t[left] <= max.gap & !is.na(swimming[right]) & !swimming[right]))
      status[j[glide]] <- "gliding"
      j <- j[supported]; left <- left[supported]; right <- right[supported]
      dt <- at[j] - t[left]
      span <- t[right] - t[left]
      slope <- ifelse(span > 0, (f[right] - f[left]) / span, 0)
      # Exact samples do not depend on the following sample, which may be missing.
      slope[dt == 0] <- 0
      phase[j] <- sample.phase[left] + 2*pi * (f[left]*dt + .5*slope*dt^2)
      fraction <- ifelse(span > 0, dt / span, 0)
      amp <- a[left]
      interp <- dt > 0
      amp[interp] <- a[left[interp]] + fraction[interp] * (a[right[interp]] - a[left[interp]])
      strength[j] <- pmin(1, pmax(0, amp / motion$amplitude.reference))
      frequency[j] <- f[left] + slope*dt
      status[j] <- "estimated"
    }
  }
  frames[, `:=`(.motion.phase = phase, .motion.strength = strength,
                 .motion.frequency = frequency, .motion.status = status)]
  list(frames = frames, rows = rows, angle.columns = angle.cols, motion = motion)
}

.overlayFrameData <- function(state, i, data) {
  row <- state$rows[i]
  fd <- if (row > 0L) as.list(data[row]) else as.list(data[NA_integer_])
  frame <- as.list(state$frames[i])
  for (name in names(frame)) fd[[name]] <- frame[[name]]
  fd
}

.checkOverlayMotionRate <- function(state, overlay.fps, video.fps) {
  if (is.null(state$motion) || state$motion$max.angle == 0) return(invisible(NULL))
  f <- state$frames$.motion.frequency
  if (!any(f > 0)) return(invisible(NULL))
  rate <- min(overlay.fps, video.fps)
  peak <- max(f)
  if (rate <= 2 * peak)
    cli::cli_warn("Schematic motion is undersampled: the effective {rate} fps does not exceed twice the maximum {round(peak, 2)} Hz beat frequency.")
  else if (rate < 8 * peak)
    cli::cli_warn("Schematic motion has fewer than eight frames per fastest beat; increase {.arg overlay.fps} and use a sufficiently fast source video for smoother motion.")
  invisible(NULL)
}
