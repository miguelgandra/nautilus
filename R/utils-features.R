# Internal window planning and statistics for extractFeatures().
# Bounds are shared by every channel, metric and response at a given scale.

.featureFinite <- function(x) if (length(x) != 1L || !is.finite(x)) NA_real_ else as.numeric(x)

.featureLayout <- function(data, times, sampling_freq, stride, segment_col = NULL) {
  n <- length(times)
  breaks <- c(TRUE, diff(times) > 1.5 / sampling_freq)
  available <- rep(TRUE, n)
  if (!is.null(segment_col)) {
    key <- data[[segment_col]]
    available <- !is.na(key)
    changed <- c(TRUE, is.na(key[-1L]) | is.na(key[-n]) | key[-1L] != key[-n])
    breaks <- breaks | changed
  }
  segment <- cumsum(breaks)
  starts <- which(breaks)
  ends <- c(starts[-1L] - 1L, n)
  anchors <- unlist(Map(function(a, b) seq.int(a, b, by = stride), starts, ends), use.names = FALSE)
  list(anchors = anchors, segment = segment, starts = starts, ends = ends, available = available)
}

.featureBounds <- function(layout, width, aggregate = FALSE) {
  a <- layout$anchors
  segment_start <- layout$starts[layout$segment[a]]
  segment_end <- layout$ends[layout$segment[a]]
  # Same convention as zoo: for width 4, the window at row i is (i-1):(i+2).
  start <- if (aggregate) a else a - (width - 1L) %/% 2L
  end <- start + width - 1L
  valid <- layout$available[a] & start >= segment_start
  if (aggregate) end <- pmin(end, segment_end) else valid <- valid & end <= segment_end
  list(start = start, end = end, valid = valid)
}

.featureCoverage <- function(channel, thresholds) {
  if (is.null(names(thresholds))) return(thresholds)
  if (channel %in% names(thresholds)) thresholds[[channel]] else 0
}

.featureResponse <- function(x, method) {
  if (is.factor(x)) x <- as.character(x)
  x <- as.numeric(x)
  x <- x[is.finite(x)]
  if (!length(x)) return(NA_integer_)
  as.integer(if (method == "any") any(x == 1) else mean(x) > 0.5)
}

.featureSteps <- function(x, circular = FALSE, sampling_freq = 1) {
  dx <- diff(x)
  if (circular) dx <- (dx + 180) %% 360 - 180
  dx * sampling_freq
}

.featureCV <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) < 2L) return(NA_real_)
  m <- mean(x)
  if (m == 0) 0 else stats::sd(x) / m
}

.featureAutocorr <- function(x, circular = FALSE, average = FALSE) {
  if (circular) x <- .unwrapDegrees(x)
  n_valid <- sum(is.finite(x))
  if (n_valid < if (average) 10L else 4L) return(NA_real_)
  if (length(unique(x[is.finite(x)])) < 2L) return(NA_real_)
  # Retain NA positions: deleting them changes which observations are one lag apart.
  lag_max <- if (average) min(5L, length(x) - 1L) else 1L
  ans <- stats::acf(x, plot = FALSE, lag.max = lag_max, na.action = stats::na.pass)$acf[-1L]
  .featureFinite(mean(ans, na.rm = TRUE))
}

.circularMetric <- function(x_window, metric, sampling_freq = 1) {
  x_window[!is.finite(x_window)] <- NA_real_
  # Rates use only adjacent observed samples; never join across a missing sample.
  if (metric == "rate")
    return(.featureFinite(mean(abs(.featureSteps(x_window, TRUE, sampling_freq)), na.rm = TRUE)))
  x <- x_window[is.finite(x_window)] %% 360
  if (!length(x) || (metric %in% c("sd", "range", "iqr") && length(x) < 2L)) return(NA_real_)
  radians <- x * pi / 180
  C <- mean(cos(radians)); S <- mean(sin(radians))
  R <- min(1, sqrt(C^2 + S^2))
  switch(metric,
    mean = if (R < 1e-12) NA_real_ else atan2(S, C) * 180 / pi,
    median = {
      distance <- function(y) sum(abs((x - y + 180) %% 360 - 180))
      # Order of observations resolves ties without privileging an absolute bearing.
      x[which.min(vapply(x, distance, numeric(1)))]
    },
    sd = if (R < 1e-12) NA_real_ else sqrt(-2 * log(R)) * 180 / pi,
    range = {
      y <- sort(x)
      360 - max(c(diff(y), 360 - (tail(y, 1L) - y[1L])))
    },
    iqr = {
      # Cut within the largest empty arc before taking type-7 linear quartiles.
      # A tied cut is resolved by the minimum IQR over all tied arcs, which is
      # invariant to rotations even for symmetric or multimodal observations.
      y <- sort(x)
      gaps <- c(diff(y), 360 - (tail(y, 1L) - y[1L]))
      cuts <- which(abs(gaps - max(gaps)) < 1e-8)
      min(vapply(cuts, function(k) {
        origin <- y[k %% length(y) + 1L]
        stats::IQR((x - origin) %% 360, type = 7)
      }, numeric(1)))
    },
    mrl = R,
    stop(paste("Unknown circular metric:", metric))
  )
}

.featureLinearMetric <- function(x, metric, sampling_freq = 1) {
  observed <- x[is.finite(x)]
  if (!length(observed)) return(NA_real_)
  result <- switch(metric,
    mean = mean(observed), median = stats::median(observed), sd = stats::sd(observed),
    range = diff(range(observed)), min = min(observed), max = max(observed),
    iqr = stats::IQR(observed), mad = stats::mad(observed), sum = sum(observed),
    energy = sum(observed^2),
    rate = mean(abs(.featureSteps(x, FALSE, sampling_freq)), na.rm = TRUE),
    skewness = moments::skewness(observed), kurtosis = moments::kurtosis(observed),
    entropy = {
      if (length(unique(observed)) < 2L) return(NA_real_)
      h <- graphics::hist(observed, breaks = "Sturges", plot = FALSE)
      entropy::entropy(h$density / sum(h$density))
    },
    stop(paste("Unknown metric:", metric))
  )
  .featureFinite(result)
}

# Retain native rolling kernels for dense sliding output. Sparse strides evaluate
# only their requested windows below, so a 1 Hz prediction grid need not build
# every overlapping native-rate window first.
.featureFastSummary <- function(x, metric, width, bounds, coverage, circular, sampling_freq) {
  n <- length(x)
  answer <- rep(NA_real_, length(bounds$valid))
  if (width > n) return(answer)
  observed <- data.table::frollsum(as.numeric(is.finite(x)), width, align = "left")
  rows <- which(bounds$valid)
  rows <- rows[observed[bounds$start[rows]] > 0 & observed[bounds$start[rows]] / width >= coverage]
  if (!length(rows)) return(answer)
  if (circular) {
    radians <- (x %% 360) * pi / 180
    C <- data.table::frollmean(cos(radians), width, na.rm = TRUE, align = "left")
    S <- data.table::frollmean(sin(radians), width, na.rm = TRUE, align = "left")
    R <- pmin(1, sqrt(C^2 + S^2))
    value <- switch(metric,
      mean = ifelse(R < 1e-12, NA_real_, atan2(S, C) * 180 / pi),
      sd = ifelse(R < 1e-12 | observed < 2, NA_real_, sqrt(-2 * log(R)) * 180 / pi),
      mrl = R, circular_variance_heading = 1 - R)
  } else {
    value <- switch(metric,
      mean = data.table::frollmean(x, width, na.rm = TRUE, align = "left"),
      sum = data.table::frollsum(x, width, na.rm = TRUE, align = "left"),
      energy = data.table::frollsum(x^2, width, na.rm = TRUE, align = "left"),
      rate = if (width < 2L) rep(NA_real_, n) else
        c(data.table::frollmean(abs(diff(x)) * sampling_freq, width - 1L, na.rm = TRUE, align = "left"), NA_real_))
  }
  answer[rows] <- value[bounds$start[rows]]
  answer[!is.finite(answer)] <- NA_real_
  answer
}

.calculateMetricEnhanced <- function(data, var, metric, sampling_freq, window_seconds,
                                     aggregate, circular_variables, enhanced = FALSE,
                                     layout = NULL, min_coverage = 0) {
  width <- max(1L, round(window_seconds * sampling_freq))
  if (is.null(layout)) layout <- .featureLayout(data, seq_len(nrow(data)) / sampling_freq,
                                                sampling_freq, if (aggregate) width else 1L)
  bounds <- .featureBounds(layout, width, aggregate)
  deps <- switch(metric, posture_stability = c("pitch", "roll"),
                 activity_index = c("pitch", "roll", "heading"), var)
  if (!all(deps %in% names(data))) return(rep(NA_real_, length(layout$anchors)))
  numeric_channel <- vapply(deps, function(v) {
    z <- data[[v]]
    is.numeric(z) || is.logical(z) || all(is.na(z))
  }, logical(1))
  if (!all(numeric_channel)) .abort("Feature channels must be numeric: {.val {deps}}.")
  heading_metrics <- c("net_heading_change", "cumulative_heading_change", "circular_variance_heading",
                       "uturn_flag", "heading_autocorr_avg", "turning_rate_variability", "circling_behavior")
  if (metric %in% heading_metrics && var != "heading")
    .abort("Metric {.val {metric}} can only be applied to heading.")
  if ((metric == "posture_stability" && var != "posture") ||
      (metric == "activity_index" && var != "activity") ||
      (metric %in% c("depth_change_rate", "depth_change_consistency") && var != "depth"))
    .abort("Metric {.val {metric}} has an incompatible variable {.val {var}}.")
  channels <- lapply(deps, function(v) {
    x <- as.numeric(data[[v]]); x[!is.finite(x)] <- NA_real_; x
  })
  names(channels) <- deps

  fast_metrics <- if (var %in% circular_variables) c("mean", "sd", "mrl", "circular_variance_heading") else
    c("mean", "sum", "energy", "rate")
  if (!aggregate && length(layout$anchors) >= nrow(data) / 4 && metric %in% fast_metrics)
    return(.featureFastSummary(channels[[var]], metric, width, bounds,
                              .featureCoverage(var, min_coverage), var %in% circular_variables, sampling_freq))

  # The two legacy second-order descriptors summarise rolling means/SDs.
  # Compute their input on each segment using the same centred bounds.
  smoothed <- NULL
  if (metric %in% c("movement_predictability", "movement_consistency")) {
    inner_layout <- layout; inner_layout$anchors <- seq_len(nrow(data))
    inner <- .featureBounds(inner_layout, width)
    signal <- channels[[var]]
    if (var %in% circular_variables) {
      signal <- rep(NA_real_, nrow(data))
      for (k in seq_along(layout$starts)) {
        ii <- seq.int(layout$starts[k], layout$ends[k])
        signal[ii] <- .unwrapDegrees(channels[[var]][ii])
      }
    }
    smoothed <- vapply(seq_len(nrow(data)), function(i) {
      if (!inner$valid[i]) return(NA_real_)
      x <- signal[seq.int(inner$start[i], inner$end[i])]
      if (!any(is.finite(x)) || sum(is.finite(x)) / width < .featureCoverage(var, min_coverage))
        return(NA_real_)
      if (metric == "movement_predictability") mean(x, na.rm = TRUE) else stats::sd(x, na.rm = TRUE)
    }, numeric(1))
  }

  vapply(seq_along(layout$anchors), function(i) {
    if (!bounds$valid[i]) return(NA_real_)
    ii <- seq.int(bounds$start[i], bounds$end[i])
    supported <- vapply(deps, function(v) {
      valid <- sum(is.finite(channels[[v]][ii]))
      valid > 0L && valid / width >= .featureCoverage(v, min_coverage)
    }, logical(1))
    if (!all(supported)) return(NA_real_)
    x <- channels[[1L]][ii]
    circular <- var %in% circular_variables
    turns <- function() abs(.featureSteps(x, TRUE, sampling_freq = 1))
    rate <- function() abs(.featureSteps(x, circular, sampling_freq))
    result <- switch(metric,
      net_heading_change = if (length(x) < 2L) NA_real_ else abs((tail(x, 1L) - x[1L] + 180) %% 360 - 180),
      cumulative_heading_change = if (any(is.finite(turns()))) sum(turns(), na.rm = TRUE) else NA_real_,
      circular_variance_heading = 1 - .circularMetric(x, "mrl"),
      uturn_flag = if (length(x) < 2L) NA_real_ else as.numeric(abs((tail(x, 1L) - x[1L] + 180) %% 360 - 180) > 120),
      heading_autocorr_avg = .featureAutocorr(x, circular = TRUE, average = TRUE),
      turning_rate_variability = .featureCV(turns() * sampling_freq),
      circling_behavior = {
        total <- if (any(is.finite(turns()))) sum(turns(), na.rm = TRUE) else NA_real_
        net <- abs((tail(x, 1L) - x[1L] + 180) %% 360 - 180)
        total / (net + 1)
      },
      posture_stability = {
        pitch_sd <- stats::sd(channels$pitch[ii], na.rm = TRUE)
        roll_sd <- if ("roll" %in% circular_variables) .circularMetric(channels$roll[ii], "sd") else
          stats::sd(channels$roll[ii], na.rm = TRUE)
        1 / (1 + pitch_sd + roll_sd)
      },
      activity_index = {
        rates <- lapply(deps, function(v) abs(.featureSteps(channels[[v]][ii], v %in% circular_variables, sampling_freq)))
        mean(Reduce(`+`, rates), na.rm = TRUE)
      },
      movement_jerk = sqrt(mean(rate()^2, na.rm = TRUE)),
      movement_smoothness = sqrt(mean(diff(.featureSteps(x, circular, sampling_freq))^2, na.rm = TRUE)) * sampling_freq,
      rolling_autocorrelation = .featureAutocorr(x, circular),
      zero_crossing_rate = {
        if (circular) x <- .unwrapDegrees(x)
        signs <- sign(x - mean(x, na.rm = TRUE))
        if (!any(is.finite(diff(signs)))) NA_real_ else sum(diff(signs) != 0, na.rm = TRUE) / length(x)
      },
      oscillation_regularity = .featurePeakCV(if (circular) .unwrapDegrees(x) else x),
      movement_predictability = {
        z <- smoothed[ii]
        if (sum(is.finite(z)) < width / 2) NA_real_ else stats::sd(z, na.rm = TRUE) / (abs(mean(z, na.rm = TRUE)) + 0.001)
      },
      movement_consistency = {
        z <- smoothed[ii]
        if (sum(is.finite(z)) < width / 2) NA_real_ else stats::sd(z, na.rm = TRUE) / (mean(z, na.rm = TRUE) + 0.001)
      },
      depth_change_rate = mean(rate(), na.rm = TRUE),
      depth_change_consistency = .featureCV(rate()),
      if (circular) .circularMetric(x, metric, sampling_freq) else .featureLinearMetric(x, metric, sampling_freq)
    )
    # Neutral shape values apply only to observed constant surface channels.
    # Never overwrite absent data or coverage failures.
    if (!is.finite(result) && var %in% c("depth", "vertical_speed") &&
        metric %in% c("skewness", "kurtosis", "entropy") && "depth" %in% names(data) &&
        any(is.finite(data$depth[ii])) && all(data$depth[ii][is.finite(data$depth[ii])] == 0) &&
        length(unique(x[is.finite(x)])) == 1L)
      result <- switch(metric, skewness = 0, kurtosis = 3, entropy = 0)
    .featureFinite(result)
  }, numeric(1))
}

.featurePeakCV <- function(x) {
  if (length(x) < 10L || sum(is.finite(x)) < 10L) return(NA_real_)
  x <- zoo::rollmean(x, k = 3, fill = "extend")
  peaks <- which(diff(sign(diff(x))) == -2) + 1L
  if (length(peaks) < 3L) return(NA_real_)
  .featureCV(diff(peaks))
}

# Internal rolling primitives retained for package callers and direct regression tests.
.featureRoll <- function(x, window, FUN) {
  if (length(x) < window) return(rep(NA_real_, length(x)))
  as.numeric(zoo::rollapply(x, window, FUN, align = "center", fill = NA_real_))
}
.net_heading_change <- function(heading, window = 10) .featureRoll(heading, window, function(x)
  if (length(x) < 2L) NA_real_ else abs((tail(x, 1L) - x[1L] + 180) %% 360 - 180))
.heading_autocorr_avg <- function(heading, window = 60) .featureRoll(heading, window, function(x)
  .featureAutocorr(x, circular = TRUE, average = TRUE))
.oscillation_regularity <- function(signal, window = 60) .featureRoll(signal, window, .featurePeakCV)
.movement_jerk <- function(signal, window = 30, sampling_freq = 1) {
  if (length(signal) < 2L) return(rep(NA_real_, length(signal)))
  # Legacy internal primitive returns a value at every observed derivative; the public
  # extractor uses complete, correctly aligned sensor windows instead.
  dx <- .featureSteps(signal, sampling_freq = sampling_freq)
  c(NA_real_, sqrt(zoo::rollapply(dx^2, min(window, length(dx)), function(z)
    .featureFinite(mean(z, na.rm = TRUE)), fill = "extend", align = "center")))
}

.create_feature_name <- function(variable, metric, window_seconds, default_window_size) {
  mapping <- c(depth_change_rate = "change_rate", depth_change_consistency = "change_consistency",
               posture_stability = "stability", activity_index = "index", net_heading_change = "net_change",
               cumulative_heading_change = "cumulative_change", circular_variance_heading = "circular_variance",
               turning_rate_variability = "turning_variability", circling_behavior = "circling",
               uturn_flag = "uturn", heading_autocorr_avg = "autocorr_avg")
  clean <- if (metric %in% names(mapping)) mapping[[metric]] else metric
  paste0(variable, "_", clean, if (window_seconds != default_window_size) paste0("_", window_seconds, "s"))
}
