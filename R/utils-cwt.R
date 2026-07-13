#######################################################################################################
# Continuous wavelet transform (Morlet) ###############################################################
#######################################################################################################

# A self-contained FFT-based Morlet CWT following Torrence & Compo (1998), used to estimate the
# dominant in-band frequency and its absolute amplitude. It replaces the WaveletComp dependency, whose
# analyze.wavelet() standardises the input series internally: that made power scale-invariant (measured:
# identical power across a 1000x amplitude change), so amplitude could not be recovered at all, and
# because the standardisation was applied per batch it let a memory knob change the scientific result.
# Nothing here normalises by the data, so both defects are absent by construction rather than by repair.
#
# Reference: Torrence, C. & Compo, G.P. (1998) A Practical Guide to Wavelet Analysis.
#            Bulletin of the American Meteorological Society 79(1):61-78. Equation numbers below refer
#            to that paper.


#' Scale-to-period factor for a Morlet wavelet, matched to ridging |W|^2.
#'
#' For x(t) = A*cos(w0*t), the eq-6 normalised transform has |W(s)| = (A/2)*norm(s)*exp(-(s*w0-omega0)^2/2)
#' with norm(s) = sqrt(2*pi*s/dt)*pi^(-1/4). The sqrt(s) in norm(s) tilts the peak, so the argmax of
#' |W|^2 over s is NOT at s*w0 = omega0: maximising log|W|^2 gives 2u^2 - 2*omega0*u - 1 = 0 with
#' u = s*w0, hence u_pk = (omega0 + sqrt(omega0^2+2))/2. This factor is that argmax location, which is
#' also T&C Table 1's Fourier factor, so mapping the |W|^2 ridge through it is exactly unbiased for a
#' tone. Ridging a different quantity (e.g. the normalised amplitude 2|W|/norm, whose peak sits at
#' 2*pi/omega0) through this factor would introduce a silent frequency bias at every frequency, so the
#' pairing is fixed here rather than exposed as an option.
#' @keywords internal
#' @noRd
.cwtFlambda <- function(omega0) 4 * pi / (omega0 + sqrt(omega0^2 + 2))


#' One block of the CWT: eq-6 normalised power |W|^2 for the block's own samples.
#'
#' `npad >= m + 2*guard` removes circular wrap-around, so this is the linear convolution of the
#' zero-extended block with each daughter wavelet.
#' @keywords internal
#' @noRd
.cwtBlockPower <- function(seg, dt, scales, omega0, npad) {
  m <- length(seg)
  xp <- c(seg, rep(0, npad - m))
  xhat <- stats::fft(xp) / npad                          # eq 3
  kk <- 0:(npad - 1)
  omega <- 2 * pi * kk / (npad * dt)                     # eq 5
  omega[kk > npad / 2] <- -omega[kk > npad / 2]
  pos <- omega > 0                                       # Heaviside: analytic wavelet, exactly 0 at DC
  nrm <- sqrt(2 * pi * scales / dt) * pi^(-0.25)         # eq 6
  P <- matrix(0, length(scales), m)
  d <- numeric(npad)
  for (j in seq_along(scales)) {
    d[] <- 0
    d[pos] <- nrm[j] * exp(-(scales[j] * omega[pos] - omega0)^2 / 2)
    P[j, ] <- Mod(stats::fft(xhat * d, inverse = TRUE)[seq_len(m)])^2   # eq 4
  }
  list(P = P, norm = nrm)
}


#' Row index of each column's maximum. Avoids transposing (memory) and apply (speed); strict `>` takes
#' the first index on ties, so the result is deterministic.
#' @keywords internal
#' @noRd
.colArgMax <- function(M) {
  k <- rep(1L, ncol(M))
  best <- M[1, ]
  if (nrow(M) >= 2L) {
    for (j in 2:nrow(M)) {
      hit <- M[j, ] > best
      hit[is.na(hit)] <- FALSE
      best[hit] <- M[j, hit]
      k[hit] <- j
    }
  }
  k
}


#' Sub-bin peak offset from a 3-point parabola, in grid units on [-1, 1].
#'
#' Without this the reported frequency is quantised to the discrete scale grid (~79 values over a
#' 0.1-3 Hz band at dj = 0.05, i.e. ~3.5% steps, which is comparable to the errors being measured).
#' @keywords internal
#' @noRd
.parabolicDelta <- function(ym, y0, yp) {
  den <- ym - 2 * y0 + yp
  d <- 0.5 * (ym - yp) / den
  d[!is.finite(d)] <- 0
  d[den >= 0] <- 0                                       # not a strict local max: no refinement
  pmin(pmax(d, -1), 1)
}


#' Dominant in-band frequency and absolute amplitude via a Morlet CWT ridge.
#'
#' Estimates, for every sample, the frequency of the strongest oscillation within `[min.freq, max.freq]`
#' and its absolute semi-amplitude, by taking the per-column maximum of the wavelet power surface
#' (refined to sub-bin resolution by a parabola in log-power) and mapping it through the Morlet
#' scale-period relation.
#'
#' Per-column maxima are used rather than a continuity-penalised ridge path: a penalised path buys
#' accuracy only when a competing component periodically dominates the target, and buying that requires
#' a prior strong enough to flatten a genuinely non-stationary frequency. Tail-beat frequency is
#' non-stationary (measured: the within-window spread of instantaneous frequency is ~39% of f over 15 s
#' on real records, against ~6% for a stationary tone at matched noise), so the two cannot be had at
#' once and the penalty is a net loss here.
#'
#' Batching bounds memory at O(scales x batch) rather than O(scales x n). Each block is transformed with
#' a guard band of `guard.factor * s_max` on each side and only its own samples are kept; the Morlet
#' envelope is exp(-(t/s)^2/2), so data beyond 8 sigma contributes < 1.3e-14 and batch boundaries are
#' interior to the transform. The result is therefore invariant to `batch.s`, which is a pure memory
#' knob and cannot move a number.
#'
#' @param x Numeric signal (NA tolerated). @param fs Sampling frequency (Hz).
#' @param min.freq,max.freq Analysis band (Hz). @param omega0 Morlet non-dimensional frequency.
#' @param dj Scale spacing in log2. @param batch.s Block length (s); memory only, never affects output.
#' @param guard.factor Guard band in multiples of the largest scale.
#' @param coi Mask estimates whose wavelet support reaches a record end.
#' @param spectrogram Accumulate a max-pooled power surface for plotting (off by default: it is the
#'   dominant memory term and is useless unless a figure is being drawn).
#' @param spec.max.cols Column budget for that surface.
#' @return A list with `freq` and `amp` (both length(x), NA where unavailable) and `meta`.
#' @keywords internal
#' @noRd
.cwtRidge <- function(x, fs, min.freq, max.freq, omega0 = 8, dj = 0.05, batch.s = 900,
                      guard.factor = 8, coi = TRUE, spectrogram = FALSE, spec.max.cols = 4000) {

  n <- length(x)
  dt <- 1 / fs
  isna <- is.na(x)
  if (all(isna)) return(list(freq = rep(NA_real_, n), amp = rep(NA_real_, n),
                             meta = list(note = "all input NA")))

  # T&C sec 3.b: remove the mean ONLY, never divide by sd (that is the WaveletComp defect). Interior
  # gaps are linearly interpolated and the ends extended: filling with 0 would inject a broadband step
  # at every gap edge, whereas a linear segment is mostly low-frequency and the analytic Morlet
  # attenuates it. This is a mitigation, not a claim -- estimates near a long gap are still degraded.
  xf <- x - mean(x, na.rm = TRUE)
  if (any(isna)) xf <- stats::approx(which(!isna), xf[!isna], xout = seq_len(n), rule = 2)$y

  flambda <- .cwtFlambda(omega0)
  smin <- 1 / (max.freq * flambda)
  smax <- 1 / (min.freq * flambda)
  J <- floor(log2(smax / smin) / dj)
  jext <- (-1):(J + 1)                                   # one spare row each side, for interpolation
  scales <- smin * 2^(jext * dj)                         # support at the band edges
  band <- 2:(J + 2)                                      # rows allowed to win the argmax
  log2period0 <- log2(flambda * smin)

  G <- as.integer(ceiling(guard.factor * max(scales) * fs))
  batch <- max(as.integer(round(batch.s * fs)), 1L)
  starts <- seq.int(1L, n, by = batch)

  freq <- rep(NA_real_, n)
  amp <- rep(NA_real_, n)
  n_coi <- 0L

  # At the interpolated peak the matched scale obeys s_pk * w0 = 2*pi/flambda by construction, so the
  # wavelet's amplitude response there is a fixed constant and the amplitude below needs no empirical
  # calibration term.
  gain <- exp(-((2 * pi / flambda) - omega0)^2 / 2)

  spec <- NULL
  spec_pf <- NULL
  if (isTRUE(spectrogram)) {
    spec_pf <- max(1L, as.integer(ceiling(n / spec.max.cols)))
    spec <- matrix(0, length(scales), as.integer(ceiling(n / spec_pf)))
  }

  for (b0 in starts) {
    b1 <- min(b0 + batch - 1L, n)
    e0 <- max(1L, b0 - G)
    e1 <- min(n, b1 + G)
    seg <- xf[e0:e1]
    m <- length(seg)
    bl <- .cwtBlockPower(seg, dt, scales, omega0, as.integer(2^ceiling(log2(m + 2 * G))))

    P <- bl$P
    LP <- log(P + 1e-300)                                # an absolute floor, so batching cannot shift it
    kb <- .colArgMax(P[band, , drop = FALSE]) + (band[1] - 1L)

    cols <- seq_len(m)
    y0 <- LP[cbind(kb, cols)]
    ym <- LP[cbind(kb - 1L, cols)]
    yp <- LP[cbind(kb + 1L, cols)]
    delta <- .parabolicDelta(ym, y0, yp)
    lp_pk <- y0 - 0.25 * (ym - yp) * delta                # peak value of the fitted parabola

    fb <- 1 / 2^(log2period0 + (jext[kb] + delta) * dj)   # period (s) -> Hz

    # keep only this batch's own samples; the guard band was scaffolding
    kp <- (b0 - e0 + 1L):(b1 - e0 + 1L)
    gi <- b0:b1
    fb2 <- pmin(pmax(fb[kp], min.freq), max.freq)
    lqp <- lp_pk[kp]

    # Absolute semi-amplitude of the dominant in-band A*cos(w*t), exact for a tone:
    #   A = 2*sqrt(P_pk) / (norm(s_pk) * gain)
    s_pk <- (1 / fb2) / flambda
    ab <- 2 * sqrt(exp(lqp)) / (sqrt(2 * pi * s_pk / dt) * pi^(-0.25) * gain)

    # A segment with no in-band oscillation (a constant / DC run, or an all-zero band-passed stretch)
    # has a flat power surface; the argmax then ties to the smallest scale and would report the band
    # ceiling. Its peak power sits at the log-floor of log(P + 1e-300), orders of magnitude below any
    # real signal, so report NA rather than a fabricated ceiling frequency.
    dead <- !is.finite(lqp) | lqp < log(1e-300) + 50
    fb2[dead] <- NA_real_; ab[dead] <- NA_real_

    # Cone of influence, from theory: the Morlet e-folding time of the wavelet-power autocorrelation is
    # sqrt(2)*s (T&C sec 3.g), so at distance d from a boundary the largest trustworthy scale is
    # d/sqrt(2). Compared in SCALE units against a distance in seconds -- there is no log2 term here, so
    # the units cannot silently disagree -- and always against the RECORD end, never a batch edge, since
    # the guard bands make batch boundaries interior.
    tb <- (gi - 1) * dt
    coib <- s_pk > (pmin(tb, (n - 1) * dt - tb) / sqrt(2))
    n_coi <- n_coi + sum(coib)
    if (isTRUE(coi)) { fb2[coib] <- NA_real_; ab[coib] <- NA_real_ }

    freq[gi] <- fb2
    amp[gi] <- ab

    if (isTRUE(spectrogram)) {
      grp <- (gi - 1L) %/% spec_pf + 1L
      Pk <- P[, kp, drop = FALSE]
      for (g in unique(grp)) {
        cc <- which(grp == g)
        v <- if (length(cc) == 1L) Pk[, cc] else do.call(pmax, asplit(Pk[, cc, drop = FALSE], 2))
        spec[, g] <- pmax(spec[, g], as.numeric(v))
      }
    }
    rm(bl, P, LP)
  }

  freq[isna] <- NA_real_                                 # rows the method cannot speak for
  amp[isna] <- NA_real_

  meta <- list(
    method = "morlet-cwt", omega0 = omega0, dj = dj, flambda = flambda,
    n_scales = length(band), scale_range_s = range(scales),
    batch = batch, n_batches = length(starts), guard = G,
    coi_applied = isTRUE(coi), pct_masked_coi = 100 * n_coi / n, pct_na_in = 100 * mean(isna),
    amp_gain_correction = gain
  )
  if (isTRUE(spectrogram)) {
    meta$spectrogram <- list(
      power = spec, freqs = 1 / (flambda * scales),
      time_s = ((seq_len(ncol(spec)) - 1) * spec_pf + (spec_pf - 1) / 2) * dt,
      pool = spec_pf, pooling = "max"
    )
  }
  list(freq = freq, amp = amp, meta = meta)
}
