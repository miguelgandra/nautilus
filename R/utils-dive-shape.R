#######################################################################################################
# Optional geometric classification of retained dive profiles #########################################
#######################################################################################################

# Shape rules deliberately do not use phase labels, phase support or the whole-dive reversal count.
# All preparation is local to classification: source depth, annotations and detection history survive.

.diveShapeResolution <- function(depth) {
  d2 <- diff(depth, differences = 2L)
  d2 <- d2[is.finite(d2)]
  noise <- if (length(d2)) stats::mad(d2) / sqrt(6) else 0
  quantum <- .diveQuantum(depth)
  max(3 * noise, if (is.finite(quantum)) 2 * quantum else 0, 0)
}

# Centred box average of a piecewise-linear profile, integrated in real time rather than sample count.
# Windows shorten at the endpoints; there is no zero padding or change to the original depth record.
.diveShapeSmooth <- function(z, t, window) {
  if (window <= 0) return(z)
  n <- length(z)
  dt <- diff(t)
  area <- c(0, cumsum(dt * (z[-n] + z[-1L]) / 2))
  integral <- function(at) {
    i <- pmax(1L, pmin(n - 1L, findInterval(at, t)))
    delta <- at - t[i]
    area[i] + z[i] * delta + (z[i + 1L] - z[i]) * delta^2 / (2 * dt[i])
  }
  lo <- pmax(t[1L], t - window / 2)
  hi <- pmin(t[n], t + window / 2)
  (integral(hi) - integral(lo)) / (hi - lo)
}

# A peak must have both a rise and a subsequent fall of at least the prominence criterion.
# This linear-time hysteretic peak finder suppresses nested fluctuations below that amplitude.
# Flat summits are represented by their temporal midpoint, not whichever sample appears first.
.diveShapePeaks <- function(z, t, prominence, separation) {
  n <- length(z)
  candidates <- integer(n)
  n_found <- 0L
  rising <- TRUE
  low <- z[1L]
  high <- z[1L]
  peak_start <- peak_end <- 1L
  for (i in seq.int(2L, n)) {
    if (rising) {
      if (z[i] > high) {
        high <- z[i]; peak_start <- peak_end <- i
      } else if (z[i] == high) {
        # Only extend a CONTIGUOUS flat summit. Equal-height returns after a shallow dip must not
        # place its midpoint in the intervening valley.
        if (peak_end == i - 1L) peak_end <- i
      } else if (high - z[i] >= prominence) {
        if (high - low >= prominence) {
          midpoint <- (t[peak_start] + t[peak_end]) / 2
          p <- peak_start + which.min(abs(t[peak_start:peak_end] - midpoint)) - 1L
          n_found <- n_found + 1L; candidates[n_found] <- p
        }
        rising <- FALSE; low <- z[i]
      }
    } else {
      if (z[i] < low) low <- z[i]
      else if (z[i] - low >= prominence) {
        rising <- TRUE; high <- z[i]; peak_start <- peak_end <- i
      }
    }
  }
  if (!n_found) return(integer(0))
  # Resolve successive temporal conflicts in favour of the taller peak (earlier on exact ties).
  kept <- integer(n_found); n_kept <- 0L
  for (p in candidates[seq_len(n_found)]) {
    while (n_kept > 0L && t[p] - t[kept[n_kept]] < separation &&
           z[p] > z[kept[n_kept]]) n_kept <- n_kept - 1L
    if (n_kept > 0L && t[p] - t[kept[n_kept]] < separation) next
    n_kept <- n_kept + 1L; kept[n_kept] <- p
  }
  kept[seq_len(n_kept)]
}

.classifyDiveShapeOne <- function(depth, baseline, tnum, direction, complete, control,
                                   resolution = 0, contiguous = TRUE) {
  out <- list(dive_shape = NA_character_, dive_shape_status = "insufficient_samples",
              shape_broadness = NA_real_, shape_n_peaks = NA_integer_,
              shape_prominence_m = NA_real_)
  abstain <- function(status) { out$dive_shape_status <- status; out }
  if (!isTRUE(complete)) return(abstain("censored"))
  if (!isTRUE(contiguous)) return(abstain("noncontiguous"))
  n <- length(depth)
  if (n < control$min.samples) return(out)
  if (length(tnum) != n || any(!is.finite(tnum)) || any(diff(tnum) <= 0))
    return(abstain("invalid_time"))
  if (length(baseline) != n || any(!is.finite(baseline)))
    return(abstain("missing_reference"))
  ok <- is.finite(depth)
  if (mean(ok) < control$min.coverage) return(abstain("low_coverage"))
  if (sum(ok) < control$min.samples) return(out)
  # Missing endpoints conceal the opening or return limb; never extrapolate them.
  if (!ok[1L] || !ok[n]) return(abstain("insufficient_limbs"))
  t <- tnum - tnum[1L]
  dt <- stats::median(diff(t))
  gap <- control$max.gap %||% max(5, 3 * dt)
  if (any(diff(t[ok]) > gap)) return(abstain("gap"))

  # Only short, bracketed gaps reach here. Interpolate the residual, not separate depth/reference
  # channels, so a changing reference is represented consistently on the same timestamps.
  r <- depth - baseline
  if (!all(ok)) r <- stats::approx(t[ok], r[ok], xout = t, ties = "ordered")$y
  duration <- t[n]
  if (control$smooth.window > duration / 4) return(abstain("insufficient_resolution"))
  r <- .diveShapeSmooth(r, t, control$smooth.window)
  resolution <- max(resolution, control$min.peak.amplitude, .Machine$double.eps)

  # "both" is a configuration, not this dive's realised direction. Never fold positive and negative
  # departures with abs(): that can turn a crossing of the reference into a spurious W profile.
  if (identical(direction, "down")) sign <- 1
  else if (identical(direction, "up")) sign <- -1
  else if (identical(direction, "both")) {
    pos <- max(r); neg <- -min(r)
    if (min(pos, neg) > max(resolution, control$max.opposite.prop * max(pos, neg)))
      return(abstain("ambiguous_direction"))
    sign <- if (pos >= neg) 1 else -1
  } else {
    # Hand-labelled dives may have no detection history. Infer the dominant departure from the chord,
    # and abstain if both sides are substantial rather than silently choosing a mixed profile.
    chord <- r[1L] + (r[n] - r[1L]) * t / duration
    departure <- r - chord
    pos <- max(departure); neg <- -min(departure)
    if (min(pos, neg) > max(resolution, control$max.opposite.prop * max(pos, neg)))
      return(abstain("ambiguous_direction"))
    sign <- if (pos >= neg) 1 else -1
  }
  r <- sign * r
  span <- diff(range(r))
  if (span <= resolution) return(abstain("insufficient_resolution"))
  # The chord cannot substitute for an observed departure and return. Check both limbs BEFORE
  # detrending, independently of the detector's phase rule and shape_supported summary.
  peak <- max(r)
  limb <- max(resolution, control$min.limb.prop * span)
  if (peak - r[1L] < limb || peak - r[n] < limb)
    return(abstain("insufficient_limbs"))

  chord <- r[1L] + (r[n] - r[1L]) * t / duration
  z <- r - chord
  amplitude <- max(z)
  if (amplitude <= resolution) return(abstain("insufficient_resolution"))
  # Study-scale eligibility is separate from sensor resolution and internal-peak prominence.
  # Use the same prepared height that normalises broadness, not absolute depth or the raw range.
  if (!is.null(control$min.excursion.amplitude) && amplitude < control$min.excursion.amplitude)
    return(abstain("below_min_amplitude"))
  # Substantial movement below the endpoint chord is a complex profile, not a folded excursion.
  if (-min(z) > max(resolution, control$max.opposite.prop * amplitude)) {
    out$dive_shape <- "other"
    return(abstain("complex_profile"))
  }
  z <- pmax(0, z)
  broadness <- sum(diff(t) * (z[-n] + z[-1L]) / 2) / (duration * amplitude)
  prominence <- max(control$peak.prominence * amplitude, resolution)
  peaks <- .diveShapePeaks(z, t, prominence, max(control$min.peak.separation, 2 * dt))
  out$shape_broadness <- max(0, min(1, broadness))
  out$shape_n_peaks <- length(peaks)
  out$shape_prominence_m <- prominence
  if (!length(peaks)) return(abstain("insufficient_resolution"))
  out$dive_shape <- if (length(peaks) >= 2L) "W"
                    else if (broadness <= control$v.max.broadness) "V"
                    else if (broadness >= control$u.min.broadness) "U"
                    else "other"
  out$dive_shape_status <- if (out$dive_shape == "other") "intermediate" else "classified"
  out
}

# Only shape-enabled results carry this contract; default tables remain byte-for-byte compatible.
.diveShapeResult <- function(x, control) {
  if (!is.null(control))
    attr(x, "shape_classification") <- list(method = "profile_rules", version = 1L, control = control)
  structure(x, class = c("nautilus_dive_metrics", "data.frame"))
}
