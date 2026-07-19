#######################################################################################################
# Peak-detection tail-beat engine #####################################################################
#######################################################################################################
#
# The default tail-beat method. Detects individual beats as peak/trough oscillations in the band-passed
# lateral motion signal, which yields - unlike the spectral (wavelet) approach - the three quantities a
# user actually wants per beat: instantaneous frequency (1 / peak-to-peak interval), amplitude
# (peak-to-trough excursion), and timing. Periods of low-amplitude motion (gliding / rest) produce no
# beats and are returned as NA. Output is a per-row series plus a per-beat table.


#' Detect tail beats by peak/trough analysis of a (band-passed) motion channel.
#'
#' @param motion Numeric motion vector (e.g. sway acceleration or yaw rate), one row per sample.
#' @param fs Sampling frequency (Hz).
#' @param min.freq,max.freq Accepted tail-beat frequency range (Hz); beats outside are discarded.
#' @param filter.low,filter.high Band-pass edges (Hz).
#' @param filter.order Butterworth order. @param bandpass Logical; band-pass before detection.
#' @return A list with per-row vectors `tbf_hz` and `tbf_amplitude`, the `bandpassed` signal (for
#'   diagnostics), and a `beats` data.frame (peak index, `freq_hz`, `amplitude`).
#' @keywords internal
#' @noRd
.tailBeatsPeaks <- function(motion, fs, min.freq, max.freq, filter.low, filter.high,
                            filter.order = 4, bandpass = TRUE) {

  n <- length(motion)
  bp <- if (bandpass) .bandpassSegments(motion, fs, filter.low, filter.high, filter.order)
        else motion - mean(motion, na.rm = TRUE)

  out <- list(tbf_hz = rep(NA_real_, n), tbf_amplitude = rep(NA_real_, n),
              bandpassed = bp,
              beats = data.frame(peak = integer(0), freq_hz = numeric(0), amplitude = numeric(0)))

  # Detection sensitivity is data-driven, never the user's min.amplitude. Coupling the two silently
  # corrupted the estimate: min.amplitude is an envelope semi-amplitude chosen to classify behaviour,
  # whereas this delta is a peak-to-trough swing, so a classification threshold set above the smaller
  # beats made the detector skip them and report half the true frequency. The frequency estimate must
  # not depend on an interpretation-layer knob.
  scale <- stats::mad(bp, na.rm = TRUE)
  if (!is.finite(scale) || scale == 0) scale <- stats::sd(bp, na.rm = TRUE)
  delta <- 0.5 * scale
  if (!is.finite(delta) || delta <= 0) return(out)

  pk <- .peakdet(bp, delta)
  peaks <- pk$peaks; troughs <- pk$troughs
  if (length(peaks) < 2L) return(out)

  # one beat per consecutive peak pair: period = peak-to-peak, amplitude = peak minus the trough between
  np <- length(peaks)
  beat_peak <- integer(np - 1L); beat_end <- integer(np - 1L)
  beat_freq <- rep(NA_real_, np - 1L); beat_amp <- rep(NA_real_, np - 1L)
  for (i in seq_len(np - 1L)) {
    p1 <- peaks[i]; p2 <- peaks[i + 1L]
    f <- fs / (p2 - p1)
    beat_peak[i] <- p1; beat_end[i] <- p2
    if (f < min.freq || f > max.freq) next                      # implausible interval -> not a beat
    tb <- troughs[troughs > p1 & troughs < p2]
    beat_freq[i] <- f
    beat_amp[i]  <- if (length(tb)) bp[p1] - min(bp[tb]) else NA_real_
  }
  # Keep every frequency-plausible beat. Whether a beat counts as swimming is NOT decided here: that
  # call belongs to the shared activity classifier (.classifyActivity), which judges against an absolute
  # reference. Gating here on a fraction of the median beat amplitude was circular -- the beats being
  # measured set the bar the beats had to clear -- and collapsed once gliding supplied most of them.
  keep <- !is.na(beat_freq) & is.finite(beat_amp)
  if (!any(keep)) return(out)
  beat_peak <- beat_peak[keep]; beat_end <- beat_end[keep]
  beat_freq <- beat_freq[keep]; beat_amp <- beat_amp[keep]

  # map each row to the beat interval [peak, next-peak] it falls in (NA in rejected-beat gaps)
  iv <- findInterval(seq_len(n), beat_peak)
  covered <- iv >= 1L & iv <= length(beat_peak)
  covered[covered] <- seq_len(n)[covered] <= beat_end[iv[covered]]
  out$tbf_hz[covered]       <- beat_freq[iv[covered]]
  out$tbf_amplitude[covered] <- beat_amp[iv[covered]]
  out$beats <- data.frame(peak = beat_peak, freq_hz = beat_freq, amplitude = beat_amp)
  out
}


#' Sampling frequency of a tag dataset (metadata if available, else from timestamps).
#' @keywords internal
#' @noRd
.tagFs <- function(dt, datetime.col) {
  a <- attributes(dt)
  if (!is.null(a$processed.sampling.frequency) && is.finite(a$processed.sampling.frequency)) return(a$processed.sampling.frequency)
  m <- tryCatch(.getMeta(dt), error = function(e) NULL)
  hz <- m$sensors$sampling_hz_processed %||% m$sensors$sampling_hz_original
  if (!is.null(hz) && is.finite(hz)) return(hz)
  d <- as.numeric(diff(as.numeric(dt[[datetime.col]])))
  d <- d[is.finite(d) & d > 0]
  if (!length(d)) return(NA_real_)
  1 / stats::median(d)
}

#' Dominant in-band frequency of a signal (Hz): the WELCH-averaged spectral peak within `[low, high]`.
#'
#' A single full-length periodogram is a poor peak estimator here: its variance is high and its argmax is
#' easily captured by residual low-frequency drift rather than the (sharper but lower-total-power) locomotor
#' tone. Averaging overlapping Hann-windowed segments (Welch) collapses that broadband variance and lets the
#' tonal peak stand out -- on real manta surge this is the difference between reporting 0.23 Hz (drift) and
#' the true 0.44 Hz wingbeat. Each segment is demeaned so per-window DC never leaks into the low band.
#' @param seglen Target segment length (samples); the record is split into ~50%-overlap windows of this
#'   size (capped at the series length). ~4096 gives sub-0.005 Hz resolution at 20 Hz with heavy averaging.
#' @return The peak frequency, or NA if there is too little data or no in-band content.
#' @keywords internal
#' @noRd
.dominantInbandFreq <- function(x, fs, low, high, seglen = 4096L) {
  x <- x[is.finite(x)]
  n <- length(x)
  if (n < 32L) return(NA_real_)
  seglen <- as.integer(min(seglen, n))
  step   <- max(1L, seglen %/% 2L)
  w      <- 0.5 - 0.5 * cos(2 * pi * (0:(seglen - 1L)) / (seglen - 1L))   # Hann
  starts <- seq(1L, n - seglen + 1L, by = step)
  if (!length(starts)) starts <- 1L
  nfreq <- seglen %/% 2L
  P <- numeric(nfreq)
  for (s in starts) {
    seg <- x[s:(s + seglen - 1L)]
    seg <- (seg - mean(seg)) * w
    P <- P + (Mod(stats::fft(seg))[2:(nfreq + 1L)])^2
  }
  freq <- (1:nfreq) * fs / seglen
  inb  <- freq >= low & freq <= high
  # A flat / all-zero / stuck channel demeans to zeros, so its whole spectrum is zero; `which.max` would
  # then silently return the first (lowest) in-band bin. Report NA instead, per the contract: no in-band
  # energy means no dominant frequency -- otherwise two dead axes would "agree" at the band edge and form
  # a spurious cross-axis consensus in .selectMotionAxis.
  if (!any(inb) || max(P[inb]) <= 0) return(NA_real_)
  freq[inb][which.max(P[inb])]
}

#' Choose the motion axis among candidate columns present in `dt`, by CROSS-AXIS FREQUENCY CONSENSUS.
#'
#' Raw in-band power is not a safe selector: a non-locomotor artefact -- classically the tow-pendulum
#' swing of a towed tag, a strong sharp peak in the lateral (sway/yaw) axis a little below the true beat
#' -- can carry more in-band power than the propulsion signal and hijack the choice (verified on
#' video-ground-truthed reef-manta data: sway/0.33 Hz pendulum outscored the surge/0.44 Hz wingbeat).
#'
#' The physical discriminator: coordinated propulsion drives MULTIPLE axes at one frequency (a wingbeat
#' shows up on surge AND heave together), whereas a tether swing is a single-DOF oscillation confined to
#' one axis. So a locomotor frequency is corroborated across axes; an artefact is an axis-isolated
#' outlier. We therefore pick, among axes whose dominant in-band frequency agrees with at least one other
#' axis (within `rel.tol`), the one with the most in-band power. If no two candidates agree we fall back
#' to plain in-band power and flag the choice as unsupported (`agree = FALSE`), so the caller can warn.
#'
#' @return A list: `axis` (chosen column or NA), `reason` (`"single"`, `"consensus"`, `"power"` =
#'   unsupported fallback, or `"none"`), `freqs`/`powers` (named per present candidate), `agree` (TRUE if
#'   the pick is cross-axis corroborated, NA for a single candidate), and `harmonic_alt` (the name of an
#'   axis carrying a comparable-or-stronger peak at ~half the chosen frequency -- a possible true
#'   fundamental if the chosen axis is a 2f harmonic -- or NA).
#' @keywords internal
#' @noRd
.selectMotionAxis <- function(dt, candidates, fs, low, high, rel.tol = 0.15) {
  none <- function(freqs = numeric(0), powers = numeric(0))
    list(axis = NA_character_, reason = "none", freqs = freqs, powers = powers,
         agree = NA, harmonic_alt = NA_character_)
  present <- candidates[candidates %in% names(dt)]
  if (length(present) == 0L) return(none())
  if (length(present) == 1L) return(list(axis = present, reason = "single",
                                         freqs = numeric(0), powers = numeric(0),
                                         agree = NA, harmonic_alt = NA_character_))

  powers <- vapply(present, function(cc) {
    x <- dt[[cc]]; x <- x[is.finite(x)]
    if (length(x) < 16L) return(-Inf)
    p <- .bandPower(x, fs, c(low, high)); if (is.finite(p)) p else -Inf
  }, numeric(1))
  freqs <- vapply(present, function(cc) .dominantInbandFreq(dt[[cc]], fs, low, high), numeric(1))
  named <- function(v) stats::setNames(v, present)

  # Exclude dead / near-dead axes with a RELATIVE power floor. A perfectly-flat channel has in-band power
  # exactly 0, but a stuck sensor with residual dither (quantisation, electronic noise) has a tiny POSITIVE
  # power and a spurious Welch peak, so an absolute `> 0` test would let two near-dead axes corroborate each
  # other and beat a real axis. Require a meaningful share of the strongest candidate's in-band power.
  maxp <- suppressWarnings(max(powers[is.finite(powers) & powers > 0]))
  if (!is.finite(maxp)) return(none(named(freqs), named(powers)))
  usable <- is.finite(powers) & powers >= maxp * 1e-3

  # each usable axis is "corroborated" if another usable axis peaks at ~the same frequency
  corrob <- vapply(seq_along(present), function(i) {
    fi <- freqs[i]
    if (!usable[i] || !is.finite(fi)) return(FALSE)
    any(vapply(seq_along(present), function(j) {
      if (j == i || !usable[j]) return(FALSE)
      fj <- freqs[j]
      is.finite(fj) && abs(fi - fj) / ((fi + fj) / 2) <= rel.tol
    }, logical(1)))
  }, logical(1))

  if (any(corrob)) {
    idx <- which(corrob)
    pick <- idx[which.max(powers[idx])]                  # strongest corroborated axis
    reason <- "consensus"; agree <- TRUE
  } else {
    idx <- which(usable)
    pick <- idx[which.max(powers[idx])]                  # fall back to plain power, unsupported
    reason <- "power"; agree <- FALSE
  }

  # Harmonic ambiguity -- DETECT, never resolve. If a usable axis carries a comparable-or-stronger peak at
  # ~half the chosen frequency, the chosen peak MIGHT be that axis's second harmonic: a lateral swimmer's
  # beat sits on one axis while surge+heave carry its 2f, so a corroborated 2f pair would be double the true
  # rate. But the SAME spectrum arises from a genuine fundamental consensus (a ray wingbeat) contaminated by
  # a strong artefact near half the beat -- the two are indistinguishable from frequency and power alone.
  # Re-pointing would silently HALVE a correct estimate in the second case, so we do NOT re-point: we report
  # the chosen axis and record the alternative for the caller to warn about, leaving the call to the user.
  harmonic_alt <- NA_character_
  fc <- freqs[pick]
  if (is.finite(fc)) {
    sub <- which(usable & is.finite(freqs) &
                 abs(freqs - fc / 2) / (fc / 2) <= rel.tol & powers >= powers[pick])
    if (length(sub)) harmonic_alt <- present[sub[which.max(powers[sub])]]
  }
  list(axis = present[pick], reason = reason, freqs = named(freqs), powers = named(powers),
       agree = agree, harmonic_alt = harmonic_alt)
}

#' Centred moving average of a per-row series, NA-aware: a gap is ignored rather than propagated over the
#' whole window. Shared by both tail-beat methods so the two paths cannot drift apart on NA handling.
#' Returns `x` unchanged when smoothing is disabled.
#' @keywords internal
#' @noRd
.smoothSeries <- function(x, smooth.window, fs) {
  if (!isTRUE(smooth.window > 0) || !isTRUE(is.finite(fs))) return(x)
  w <- max(1L, round(smooth.window * fs))       # a sub-sample window rounds to 1 (a no-op), never to 0
  if (w %% 2L == 0L) w <- w + 1L                # an odd width keeps the average centred on its row
  out <- data.table::frollmean(x, w, align = "center", na.rm = TRUE)
  out[is.nan(out)] <- NA_real_                  # frollmean yields NaN, not NA, for an all-NA window
  out
}


#' Physical unit of a motion axis, for labelling amplitudes.
#'
#' `tbf_amplitude` is in the units of whichever channel was analysed, so the label has to follow the
#' axis rather than assume an accelerometer.
#' @keywords internal
#' @noRd
.tbAxisUnits <- function(axis) {
  if (length(axis) != 1L || is.na(axis)) return("units")
  if (axis %in% c("gx", "gy", "gz")) "deg/s" else if (axis %in% c("mx", "my", "mz")) "uT" else "g"
}


#######################################################################################################
# Cross-checks ########################################################################################
#######################################################################################################

#' Fraction of estimates sitting on a band edge.
#'
#' A frequency track pinned to `min.freq` or `max.freq` is the signature of a truth that lies OUTSIDE
#' the analysis band: the estimator, forbidden from reporting it, reports the nearest thing it is
#' allowed to. This catches that, and it catches it in the one case where backend agreement is useless
#' -- an out-of-band truth captures both backends, which then agree with each other and are both wrong.
#' It is blind to the opposite case (an in-band contaminant), which is why `.tbAgreement` also ships.
#' @param freq Estimated frequency. @param min.freq,max.freq Band edges (Hz).
#' @param tol Relative distance from an edge that counts as "on" it.
#' @return Fraction of non-NA estimates on either edge.
#' @keywords internal
#' @noRd
.tbEdgeOccupancy <- function(freq, min.freq, max.freq, tol = 0.02) {
  f <- freq[!is.na(freq)]
  if (!length(f)) return(NA_real_)
  max(mean(abs(f - min.freq) / min.freq < tol), mean(abs(f - max.freq) / max.freq < tol))
}

#' Per-row agreement between two independent frequency estimates.
#'
#' The two backends fail in opposite directions -- peak detection can lock onto a harmonic and report a
#' multiple of the true frequency, while a spectral method can be captured by a strong low-frequency
#' component that peak detection ignores -- so their bias is largely independent and agreement is
#' evidence. Agreement is the certificate, not disagreement the error flag: where they agree they are
#' almost never both wrong, but where they differ neither the flag nor anything else says which one to
#' believe, so it means "unresolved" rather than "bad".
#' @param a,b Frequency estimates. @param tol Relative tolerance.
#' @return Logical vector; NA where either estimate is missing.
#' @keywords internal
#' @noRd
.tbAgreement <- function(a, b, tol = 0.1) {
  out <- abs(a - b) / pmax(a, b) < tol
  out[is.na(a) | is.na(b)] <- NA
  out
}


#######################################################################################################
# Activity classification (optional; see note) ########################################################
#######################################################################################################

#' Classify swimming vs gliding, but ONLY against a caller-supplied amplitude reference.
#'
#' This function deliberately does NOT infer swimming from the signal alone. The reason is empirical.
#' Every self-referential rule that was tried -- a fraction of the median beat amplitude, an out-of-band
#' noise floor, an Otsu split of the amplitude envelope, a spectral-flatness (tonality) test -- was
#' refuted on video-validated real data. On the whale-shark towed-tag archive the tag oscillates in the
#' tail-beat band whether or not the animal is propelling: across human-annotated feeding events the
#' classifier could not separate a stationary, near-motionless feeding posture from active ram feeding
#' (Wilcoxon p = 0.94), and every in-band artefact tested -- ocean swell, tag strum, a passing vessel --
#' was reported as continuous swimming. The failure persisted in clean, fully-submerged water, so it is
#' not a surface-wobble artefact that a depth filter could remove. A single band-passed sway axis simply
#' does not carry the information needed to tell propulsion from non-propulsion on a towed tag.
#'
#' So the behavioural call is treated as an interpretation layer, not a core output. With no
#' `min.amplitude` the swimming flag is NA -- the honest answer -- while frequency and amplitude are
#' still reported wherever an oscillation is measurable. When the caller supplies an absolute amplitude
#' reference (e.g. calibrated from a species/tag combination or from validated footage), the envelope is
#' thresholded against it, with hysteresis and a minimum-bout filter to stop the flag chattering.
#'
#' @param bp Band-passed motion. @param fs Sampling frequency (Hz).
#' @param min.amplitude Absolute envelope threshold in the units of `bp`, or NULL to withhold the call.
#' @param win.s Envelope smoothing (s). @param min.bout.s Shortest bout kept (s); shorter runs are
#'   absorbed into their neighbour.
#' @return A list with `swimming` (logical, or all NA when unclassified), the `threshold` used, its
#'   `source`, and `envelope.median` (a scale the caller can size a `min.amplitude` from).
#' @keywords internal
#' @noRd
.classifyActivity <- function(bp, fs, min.amplitude = NULL, win.s = 1, min.bout.s = 2) {
  n <- length(bp)
  env <- .envelope(bp, fs, win.s)
  env_med <- stats::median(env, na.rm = TRUE)

  if (is.null(min.amplitude)) {
    return(list(swimming = rep(NA, n), threshold = NA_real_, source = "not classified",
                envelope.median = env_med))
  }

  # hysteresis: cross the threshold to start swimming, but only drop out below 0.6x it, so a
  # beat-to-beat dip through the threshold does not chop one bout into many
  sw <- .hysteresis(env, min.amplitude, 0.6 * min.amplitude)
  sw <- .dropShortRuns(sw, max(1L, round(min.bout.s * fs)))
  list(swimming = sw, threshold = min.amplitude, source = "min.amplitude", envelope.median = env_med)
}

#' Two-threshold (Schmitt) classification: rises above `hi` to enter, falls below `lo` to leave.
#' @keywords internal
#' @noRd
.hysteresis <- function(x, hi, lo) {
  above <- !is.na(x) & x >= hi
  live <- !is.na(x) & x >= lo                     # eligible to remain, once entered
  # a sample is TRUE if the last decisive crossing at or before it was an entry
  state <- ifelse(above, 1L, ifelse(live, NA_integer_, 0L))
  state[1] <- if (is.na(state[1])) 0L else state[1]
  state <- .locfInt(state)
  out <- state == 1L
  out[is.na(x)] <- NA
  out
}

#' Last-observation-carried-forward over an integer vector (NA = carry the previous state).
#' @keywords internal
#' @noRd
.locfInt <- function(v) {
  idx <- which(!is.na(v))
  if (!length(idx)) return(v)
  v[idx[findInterval(seq_along(v), idx)]]
}

#' Absorb runs shorter than `min.len` into the surrounding state, so the flag cannot chatter.
#' @keywords internal
#' @noRd
.dropShortRuns <- function(x, min.len) {
  if (min.len <= 1L || !any(!is.na(x))) return(x)
  r <- rle(ifelse(is.na(x), -1L, as.integer(x)))
  orig <- r$values                                     # read neighbours from the ORIGINAL run values,
  short <- which(r$lengths < min.len & r$values >= 0L) # not ones a previous iteration already absorbed
  for (k in short) {
    nb <- c(if (k > 1L) orig[k - 1L], if (k < length(orig)) orig[k + 1L])
    nb <- nb[nb >= 0L]
    if (length(nb)) r$values[k] <- nb[1]
  }
  y <- inverse.rle(r)
  out <- y == 1L
  out[y < 0L] <- NA
  out
}


#######################################################################################################
# Per-individual driver for the peak method ###########################################################
#######################################################################################################

#' Run the peak-detection tail-beat method on one individual; writes tbf_hz / tbf_amplitude /
#' tbf_swimming, draws the diagnostic panel (if requested), and saves to disk (if requested).
#' @keywords internal
#' @noRd
.runPeaks <- function(dt, animal_id, datetime.col, motion.col, fs, min.freq, max.freq,
                      bandpass, filter.low, filter.high, filter.order, min.amplitude, smooth.window,
                      draw.devices = integer(0), lvl = 1L) {

  if (!data.table::is.data.table(dt)) dt <- data.table::as.data.table(dt)

  raw <- dt[[motion.col]]
  r <- .tailBeatsPeaks(raw, fs, min.freq, max.freq, filter.low, filter.high,
                       filter.order, bandpass = bandpass)

  # swimming/gliding is only classified against a caller-supplied reference; see .classifyActivity for
  # why it is not inferred from the signal alone. Shared with the wavelet method.
  g <- .classifyActivity(r$bandpassed, fs, min.amplitude = min.amplitude)

  # `detection` is a real per-deployment finding (how many beats the detector located), so it stays. The
  # analysis band is fixed config already shown in the header, and the swimming outcome is reported once by
  # the driver's merged "swimming:" line -- neither is echoed here.
  if (lvl >= 2L) {
    .log_detail(lvl, if (nrow(r$beats) > 0L)
                  sprintf("detection: %s beats", .formatLargeNumber(nrow(r$beats)))
                else "detection: no beats in band")
  }

  # Undo the band-pass attenuation, at each beat's own frequency. Same |H|^2 correction as the wavelet
  # path (filtfilt applies the filter twice). Verified on both: it restores a unit tone at the band
  # floor from 1.42 to 2.00 peak-to-trough. It degrades at the very top of the band -- where the filter
  # response is steepest and this method's frequency is quantised to the fs/k grid, so the gain gets
  # evaluated at the wrong frequency -- but that regime is already marginal for peak detection.
  amp_raw <- r$tbf_amplitude
  if (bandpass && any(!is.na(amp_raw))) {
    gain_at <- seq(min.freq, max.freq, length.out = 512L)
    gain <- .bandpassPowerGain(gain_at, fs, filter.low, filter.high, filter.order)
    amp_raw <- amp_raw / stats::approx(gain_at, gain, xout = r$tbf_hz, rule = 2)$y
  }

  tbf <- .smoothSeries(r$tbf_hz, smooth.window, fs)
  amp <- .smoothSeries(amp_raw, smooth.window, fs)

  # When the caller supplied a reference and classification ran, null the frequency/amplitude on
  # not-swimming rows so summarizeTagData()'s average is over swimming only. When there was no reference
  # the swimming flag is all NA, glide is all FALSE, and nothing is nulled: the oscillation is reported
  # wherever it was measured, with the behavioural call left to the caller.
  glide <- !is.na(g$swimming) & !g$swimming
  tbf[glide] <- NA_real_
  amp[glide] <- NA_real_

  data.table::set(dt, j = "tbf_hz", value = tbf)
  data.table::set(dt, j = "tbf_amplitude", value = amp)
  data.table::set(dt, j = "tbf_swimming", value = g$swimming)

  # diagnostic panel (one page per requested device)
  if (length(draw.devices) > 0) {
    # the activity flag comes from .classifyActivity, not the detector: .tailBeatsPeaks does not return one
    # when the behavioural call moved out of it, which left every page titled "NA% swimming"
    pd <- list(id = animal_id, t = dt[[datetime.col]], bandpassed = r$bandpassed,
               beats = r$beats, tbf = tbf, amplitude = amp, swimming = g$swimming,
               axis = motion.col, fs = fs)
    for (dv in draw.devices) { grDevices::dev.set(dv); .drawTailBeatsPeaks(pd) }
  }

  # saving is handled by the calculateTailBeats() driver (after QC metadata is appended)
  dt
}


#######################################################################################################
# Internal: peak-method diagnostic panel ##############################################################
#######################################################################################################

# Three stacked panels per individual: (1) a zoom of the band-passed signal with detected beats marked
# (verify detection by eye), (2) the tail-beat frequency track over the deployment, (3) the amplitude
# track. Full-deployment tracks are down-sampled to a page-friendly width.

#' @keywords internal
#' @noRd
.drawTailBeatsPeaks <- function(pd) {
  oldpar <- graphics::par(no.readonly = TRUE); on.exit(graphics::par(oldpar), add = TRUE)
  graphics::layout(matrix(1:3, ncol = 1), heights = c(1.3, 1, 1))
  col_sig <- "#185FA5"; col_beat <- "#CC3333"; col_amp <- "#1D9E75"
  n <- length(pd$t); fs <- pd$fs

  # panel 1: band-passed signal zoom with detected beats
  graphics::par(mar = c(2.6, 4, 3.2, 1), mgp = c(2.2, 0.6, 0))
  c0 <- if (nrow(pd$beats) > 0) pd$beats$peak[which.min(abs(pd$beats$peak - n / 2))] else n %/% 2L
  w <- as.integer(min(n %/% 2L, max(50, round(60 * fs))))
  win <- max(1L, c0 - w):min(n, c0 + w)
  graphics::plot(pd$t[win], pd$bandpassed[win], type = "l", col = col_sig, lwd = 0.8,
                 xlab = "", ylab = paste0(pd$axis, " (band-passed)"), las = 1, cex.axis = 0.8, cex.lab = 0.9)
  bw <- pd$beats$peak[pd$beats$peak >= win[1] & pd$beats$peak <= win[length(win)]]
  if (length(bw)) graphics::points(pd$t[bw], pd$bandpassed[bw], pch = 16, col = col_beat, cex = 0.7)
  graphics::title(main = sprintf("%s   -   tail beats [%s]", pd$id, pd$axis), font.main = 2, cex.main = 1.15, line = 2)
  # the classifier reports NA when unclassified, so this has to say "not determined" rather than
  # print NA% as though it were a measurement
  sw <- mean(pd$swimming, na.rm = TRUE)
  graphics::mtext(sprintf("median %.2f Hz  |  median amplitude %.2f  |  %s swimming  |  %d beats",
                          stats::median(pd$tbf, na.rm = TRUE), stats::median(pd$amplitude, na.rm = TRUE),
                          if (is.finite(sw)) sprintf("%.0f%%", 100 * sw) else "not determined",
                          nrow(pd$beats)), side = 3, line = 0.4, cex = 0.78)

  ds <- if (n > 4000L) round(seq(1, n, length.out = 4000L)) else seq_len(n)

  # panel 2: tail-beat frequency track
  graphics::par(mar = c(2.6, 4, 1, 1))
  graphics::plot(pd$t[ds], pd$tbf[ds], type = "l", col = col_sig, lwd = 0.8, xlab = "",
                 ylab = "TBF (Hz)", las = 1, cex.axis = 0.8, cex.lab = 0.9)

  # panel 3: amplitude track
  graphics::par(mar = c(3.6, 4, 1, 1))
  graphics::plot(pd$t[ds], pd$amplitude[ds], type = "l", col = col_amp, lwd = 0.8, xlab = "Time",
                 ylab = "amplitude (peak-to-trough)", las = 1, cex.axis = 0.8, cex.lab = 0.9)
  invisible(NULL)
}
