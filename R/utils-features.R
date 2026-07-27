#######################################################################################################
# Internal helpers for extractFeatures() ##############################################################
#######################################################################################################
#
# The windowed feature primitives. They were top-level, UN-DOT-PREFIXED functions in extractFeatures.R -
# the only file in the package that exposed internals that way - which made them look like part of the
# public surface and forced the parallel path to hand-enumerate 21 names in a foreach `.export` list
# that had to be edited in lockstep with this roster. Dot-prefixed and gathered here, the export list
# is unnecessary: the workers load the package namespace and find them.

################################################################################
# 1. HEADING CHANGE FEATURES (Enhanced) ########################################
################################################################################

# Net change in heading over a centered window
.net_heading_change <- function(heading, window = 10) {
  n <- length(heading)
  if (n < window * 2) return(rep(NA, n))  # Need enough data for lead/lag
  h <- heading   # (circular::circular() was a no-op here - see the note in DESCRIPTION/docs)
  # Use smaller shifts - the original was using full window size
  half_window <- window %/% 2
  h_lead <- data.table::shift(h, n = half_window, type = "lead")
  h_lag  <- data.table::shift(h, n = half_window, type = "lag")
  diff_heading <- abs(as.numeric(h_lead) - as.numeric(h_lag))
  idx <- !is.na(diff_heading) & diff_heading > 180
  diff_heading[idx] <- 360 - diff_heading[idx]
  return(diff_heading)
}

# Cumulative sum of absolute heading changes
.cumulative_heading_change <- function(heading, window = 10) {
  n <- length(heading)
  if (n < 2) return(rep(NA, n))
  h <- heading   # (circular::circular() was a no-op here - see the note in DESCRIPTION/docs)
  diffs <- diff(h)
  diffs <- as.numeric(diffs)
  diffs[diffs > 180] <- diffs[diffs > 180] - 360
  diffs[diffs < -180] <- diffs[diffs < -180] + 360
  diffs <- abs(diffs)
  # Ensure we don't use a window larger than available data
  actual_window <- min(window, length(diffs))
  if (actual_window < 1) return(rep(NA, n))
  cum_change <- zoo::rollapply(diffs, width = actual_window, FUN = sum,
                               fill = NA, align = "center", partial = TRUE)
  return(c(NA, cum_change))
}

# Fixed circular variance of heading function
.circular_variance_heading <- function(heading, window = 60) {
  n <- length(heading)
  if (n < window) return(rep(NA, n))

  # Convert to radians once
  h_rad <- heading * pi / 180

  # Pre-allocate result vector
  result <- rep(NA_real_, n)

  # Calculate for each position
  half_window <- window %/% 2
  for (i in (half_window + 1):(n - half_window)) {
    start_idx <- i - half_window
    end_idx <- start_idx + window - 1

    # Get window data
    window_data <- h_rad[start_idx:end_idx]
    valid_data <- window_data[!is.na(window_data)]

    if (length(valid_data) >= window/2) {
      # Calculate mean resultant length directly
      cos_sum <- sum(cos(valid_data))
      sin_sum <- sum(sin(valid_data))
      rho <- sqrt(cos_sum^2 + sin_sum^2) / length(valid_data)
      result[i] <- 1 - rho
    }
  }

  return(result)
}

# U-turn detection flag
.uturn_flag <- function(heading, window = 60) {
  # Handle edge cases but still return 0s/1s where possible
  n <- length(heading)
  if (n < 10) return(rep(0, n))  # Too little data - assume no U-turns
  # Adjust window if needed but don't make it too small
  actual_window <- min(window, n)
  if (actual_window < 5) actual_window <- min(5, n)
  h <- heading   # (circular::circular() was a no-op here - see the note in DESCRIPTION/docs)
  # Calculate net heading change over window
  net_changes <- zoo::rollapply(h, width = actual_window, FUN = function(x) {
    # Remove NAs first
    x_clean <- x[!is.na(x)]
    if (length(x_clean) < 2) return(0)
    start_heading <- as.numeric(x_clean[1])
    end_heading <- as.numeric(x_clean[length(x_clean)])
    # Handle potential NA values
    if (is.na(start_heading) || is.na(end_heading)) return(0)
    # Calculate angular difference
    diff_heading <- abs(end_heading - start_heading)
    if (diff_heading > 180) diff_heading <- 360 - diff_heading
    # Flag as U-turn if net change > 120 degrees - return 1 or 0, never NA
    return(as.numeric(diff_heading > 120))
  }, fill = 0, align = "center", partial = TRUE)
  # Ensure correct length and fill with 0s if needed
  if (length(net_changes) != n) {
    result <- rep(0, n)  # Default to 0, not NA
    if (length(net_changes) > 0) {
      copy_length <- min(length(net_changes), n)
      # Handle the centering offset
      if (actual_window > 1) {
        offset <- (actual_window - 1) %/% 2
        start_idx <- offset + 1
        end_idx <- min(start_idx + copy_length - 1, n)
        result[start_idx:end_idx] <- net_changes[1:(end_idx - start_idx + 1)]
      } else {
        result[1:copy_length] <- net_changes[1:copy_length]
      }
    }
    return(result)
  }

  return(net_changes)
}



# Heading autocorrelation average
.heading_autocorr_avg <- function(heading, window = 60) {
  n <- length(heading)
  if (n < window) {
    if (n < 10) return(rep(NA, n))
    window <- max(10, n %/% 2)  # Use smaller window
  }
  # UNWRAP before correlating. The previous code built a circular::circular object and then discarded
  # it with as.numeric() on the next line, so acf() ran LINEARLY on degrees in [0, 360): an animal
  # holding a steady heading near north oscillates 358 -> 2, which reads as a 356-degree excursion and
  # destroys the correlation, while identical behaviour near south is unaffected. Measured on the same
  # wobble: 0.49 near north vs 0.65 near south. Unwrapping to a continuous angle removes the dependence
  # on where the branch cut happens to fall (verified: both give 0.6457).
  h <- .unwrapDegrees(heading)
  autocorr_values <- zoo::rollapply(h, width = window, FUN = function(x) {
    # Remove NAs and check for sufficient valid data
    x_clean <- x[!is.na(x)]
    if (length(unique(x_clean)) <= 1 || length(x_clean) < 10) return(NA)
    tryCatch({
      # Calculate autocorrelation for lags 1-5 and take mean
      max_lag <- min(5, length(x_clean) %/% 4)  # Adjust max lag based on data
      if (max_lag < 1) return(NA)
      acf_result <- acf(x_clean, plot = FALSE, lag.max = max_lag, na.action = na.pass)
      if (length(acf_result$acf) < 2) return(NA)
      mean(acf_result$acf[2:min(6, length(acf_result$acf))], na.rm = TRUE)
    }, error = function(e) NA)
  }, fill = NA, align = "center", partial = TRUE)

  # Ensure correct length
  if (length(autocorr_values) != n) {
    result <- rep(NA, n)
    if (length(autocorr_values) > 0) {
      copy_length <- min(length(autocorr_values), n)
      result[1:copy_length] <- autocorr_values[1:copy_length]
    }
    return(result)
  }

  return(autocorr_values)
}


################################################################################
# 2. OSCILLATION REGULARITY FEATURES (Adapted for 1Hz) #########################
################################################################################

# Simplified oscillation regularity using existing means/SDs
.oscillation_regularity <- function(signal, window = 60) {
  # This is a WINDOWED feature and used to return a single scalar for the entire deployment, silently
  # recycled across every row - `window` was consulted only as a minimum-length gate, so a "rolling"
  # feature never rolled and could not distinguish a regularly-beating period from an erratic one.
  n <- length(signal)
  if (n < window || sum(!is.na(signal)) < 10) return(rep(NA_real_, n))

  # CV of the inter-peak interval within each window: low = metronomic, high = irregular.
  cv_of_peaks <- function(x) {
    x <- zoo::rollmean(x, k = 3, fill = "extend")
    pk <- which(diff(sign(diff(x))) == -2) + 1
    if (length(pk) < 3) return(NA_real_)
    iv <- diff(pk)
    if (length(iv) < 2) return(NA_real_)
    m <- mean(iv, na.rm = TRUE)
    if (!is.finite(m) || m == 0) return(NA_real_)
    stats::sd(iv, na.rm = TRUE) / m
  }
  as.numeric(zoo::rollapply(signal, width = window, FUN = cv_of_peaks,
                            fill = NA_real_, align = "center", partial = FALSE))
}

################################################################################
# 3. MOVEMENT PREDICTABILITY AND CONSISTENCY ###################################
################################################################################

# Movement predictability using ratio of rolling SD to mean
.movement_predictability <- function(signal_mean, signal_sd, window = 60) {

  if (length(signal_mean) < window) return(NA)
  rolling_cv <- zoo::rollapply(signal_mean, width = window,
                               FUN = function(x) {
                                 if (sum(!is.na(x)) < window/2) return(NA)
                                 sd(x, na.rm = TRUE) / (abs(mean(x, na.rm = TRUE)) + 0.001)
                               }, fill = "extend", align = "center")
  return(rolling_cv)
}

# Movement consistency using existing SD metrics
.movement_consistency <- function(signal_sd, window = 60) {

  rolling_cv_sd <- zoo::rollapply(signal_sd, width = window,
                                  FUN = function(x) {
                                    if (sum(!is.na(x)) < window/2) return(NA)
                                    sd(x, na.rm = TRUE) / (mean(x, na.rm = TRUE) + 0.001)
                                  }, fill = "extend", align = "center")
  return(rolling_cv_sd)
}

################################################################################
# 4. SMOOTHNESS FEATURES (Adapted for 1Hz) #####################################
################################################################################

# Movement smoothness using rate of change
.movement_smoothness <- function(signal, window = 30) {
  n <- length(signal)
  if (n < 3) return(rep(NA, n))

  velocity <- diff(signal)  # First derivative
  acceleration <- diff(velocity)  # Second derivative

  # Rolling RMS of acceleration
  acc_squared <- acceleration^2

  if (window > length(acc_squared)) {
    # If window is larger than available data, return constant value
    rms_val <- sqrt(mean(acc_squared, na.rm = TRUE))
    return(rep(rms_val, n))
  }

  rms_acc <- sqrt(zoo::rollapply(acc_squared, width = window,
                                 FUN = mean, na.rm = TRUE, fill = NA, align = "center"))

  # Pad to match original length
  result <- rep(NA, n)
  result[3:(length(rms_acc) + 2)] <- rms_acc

  return(result)
}



# Windowed RMS "jerk" of a movement signal: the root-mean-square of its FIRST difference (rate of change)
# over a rolling window. Applied to an acceleration-like input (ODBA, VeDBA, surge/sway/heave) this is a
# per-variable jerkiness feature for behavioural classification. It is DISTINCT from - and should not be
# confused with - the core `jerk` channel that processTagData() computes as the rotation-invariant
# norm-jerk ||d a / dt|| at the native sampling rate; use that channel for physical jerk. (This previously
# took a triple difference, i.e. the third derivative of the input, which for an acceleration signal is two
# derivative orders too high; corrected to a single difference here.)
.movement_jerk <- function(signal, window = 30) {
  if (length(signal) < 2) return(rep(NA, length(signal)))

  jerk <- diff(signal)                 # first difference: rate of change (jerk when the input is acceleration)
  jerk_squared <- jerk^2

  rms_jerk <- sqrt(zoo::rollapply(jerk_squared, width = min(window, length(jerk_squared)),
                                  FUN = mean, na.rm = TRUE, fill = "extend", align = "center"))

  # Pad to match original length (a first difference loses one leading sample)
  result <- rep(NA, length(signal))
  result[2:length(result)] <- rms_jerk
  return(result)
}


################################################################################
# 5. POSTURE STABILITY (Using existing SD metrics) #############################
################################################################################

.posture_stability_from_sd <- function(data, window = 60) {
  # Check for required columns - use raw pitch and roll data
  if (!all(c("pitch", "roll") %in% names(data))) {
    stop("posture_stability_from_sd requires pitch and roll columns in data")
  }

  n <- nrow(data)
  if (n < window) {
    return(rep(NA, n))
  }

  # Calculate rolling standard deviations
  pitch_instability <- zoo::rollapply(data$pitch, width = window, FUN = sd,
                                      na.rm = TRUE, fill = NA, align = "center")
  roll_instability <- zoo::rollapply(data$roll, width = window, FUN = sd,
                                     na.rm = TRUE, fill = NA, align = "center")

  # Combined stability (inverse of instability)
  stability <- 1 / (1 + pitch_instability + roll_instability)

  # Ensure result has same length as input
  if (length(stability) != n) {
    result <- rep(NA, n)
    valid_indices <- !is.na(stability)
    result[valid_indices] <- stability[valid_indices]
    return(result)
  }

  return(stability)
}



################################################################################
# 6. TURNING BEHAVIOR ##########################################################
################################################################################

.turning_rate_variability <- function(heading, window = 60) {

  h <- heading   # (circular::circular() was a no-op here - see the note in DESCRIPTION/docs)
  turning_rates <- abs(diff(as.numeric(h)))
  turning_rates[turning_rates > 180] <- 360 - turning_rates[turning_rates > 180]

  cv_turning <- zoo::rollapply(turning_rates, width = window,
                               FUN = function(x) {
                                 if (sum(!is.na(x)) < 2) return(NA)
                                 mean_val <- mean(x, na.rm = TRUE)
                                 if (mean_val == 0) return(0)
                                 sd(x, na.rm = TRUE) / mean_val
                               },
                               fill = "extend", align = "center")
  return(c(NA, cv_turning))
}

################################################################################
# 7. ACTIVITY INDICES (Enhanced) ###############################################
################################################################################

# Activity index using existing rate metrics
.activity_index <- function(data, window = 60) {
  # Check for required columns - use raw sensor data
  required_cols <- c("pitch", "roll", "heading")
  if (!all(required_cols %in% names(data))) {
    stop(paste("activity_index requires columns:", paste(required_cols, collapse = ", ")))
  }

  n <- nrow(data)
  if (n < 2) {
    return(rep(NA, n))
  }

  # Calculate rates from raw data
  pitch_rate <- abs(c(NA, diff(data$pitch)))
  roll_rate <- abs(c(NA, diff(data$roll)))

  # For heading, handle circular differences
  heading_diffs <- diff(data$heading)
  heading_diffs[heading_diffs > 180] <- heading_diffs[heading_diffs > 180] - 360
  heading_diffs[heading_diffs < -180] <- heading_diffs[heading_diffs < -180] + 360
  heading_rate <- abs(c(NA, heading_diffs))

  # Combine rates (simple approach - avoid standardization issues)
  combined_rate <- pitch_rate + roll_rate + heading_rate

  # Calculate rolling mean activity level
  if (window > n) {
    return(rep(mean(combined_rate, na.rm = TRUE), n))
  }

  activity_level <- zoo::rollapply(combined_rate, width = window, FUN = mean,
                                   na.rm = TRUE, fill = NA, align = "center")

  # Ensure result has same length as input
  if (length(activity_level) != n) {
    result <- rep(NA, n)
    valid_length <- min(length(activity_level), n)
    result[1:valid_length] <- activity_level[1:valid_length]
    return(result)
  }

  return(activity_level)
}

################################################################################
# 8. ADDITIONAL UTILITY FEATURES ###############################################
################################################################################

# Rolling autocorrelation
.rolling_autocorrelation <- function(signal, window = 60) {
  zoo::rollapply(signal, width = window, FUN = function(x) {
    if (length(unique(x)) > 1 && sum(!is.na(x)) > 3) {
      tryCatch({
        acf(x, plot = FALSE, lag.max = 1, na.action = na.pass)$acf[2]
      }, error = function(e) NA)
    } else NA
  }, fill = NA, align = "center")
}

# Zero-crossing rate
.zero_crossing_rate <- function(signal, window = 60) {
  zoo::rollapply(signal, width = window, FUN = function(x) {
    if (sum(!is.na(x)) < window/2) return(NA)
    x_centered <- x - mean(x, na.rm = TRUE)
    signs <- sign(x_centered)
    sum(diff(signs) != 0, na.rm = TRUE) / length(x)
  }, fill = NA, align = "center")
}

# Depth change patterns
.depth_change_metrics <- function(depth_mean, window = 60) {
  depth_change_rate <- abs(c(NA, diff(depth_mean)))
  depth_change_consistency <- zoo::rollapply(depth_change_rate, width = window,
                                             FUN = function(x) {
                                               if (sum(!is.na(x)) < window/2) return(NA)
                                               mean_val <- mean(x, na.rm = TRUE)
                                               if (mean_val == 0) return(0)
                                               sd(x, na.rm = TRUE) / mean_val
                                             },
                                             fill = "extend", align = "center")
  return(list(rate = depth_change_rate, consistency = depth_change_consistency))
}


################################################################################
# 9. FEEDING-SPECIFIC FEATURES #################################################
################################################################################

# Circling behavior detection
.circling_behavior <- function(heading, window = 120) {
  h <- heading   # (circular::circular() was a no-op here - see the note in DESCRIPTION/docs)

  # Calculate net rotation over window
  net_rotation <- zoo::rollapply(h, width = window, FUN = function(x) {
    if (length(x) < window/2) return(NA)
    total_change <- sum(abs(diff(as.numeric(x))), na.rm = TRUE)
    net_change <- abs(as.numeric(x[length(x)]) - as.numeric(x[1]))
    if (net_change > 180) net_change <- 360 - net_change

    # High total change with low net change indicates circling
    if (total_change == 0) return(0)
    circling_index <- total_change / (net_change + 1)
    return(circling_index)
  }, fill = "extend", align = "center")

  return(net_rotation)
}


################################################################################
################################################################################
################################################################################

# Helper function to ensure consistent vector lengths
.ensure_length <- function(result_vector, target_length) {
  if (length(result_vector) == target_length) {
    return(result_vector)
  } else if (length(result_vector) == 1) {
    # If single value, replicate it
    return(rep(result_vector, target_length))
  } else if (length(result_vector) > target_length) {
    # If too long, truncate
    return(result_vector[1:target_length])
  } else {
    # If too short, pad with NAs
    result <- rep(NA, target_length)
    result[1:length(result_vector)] <- result_vector
    return(result)
  }
}

################################################################################
################################################################################
################################################################################

# Add this helper function to avoid redundant naming
.create_feature_name <- function(variable, metric, window_seconds, default_window_size) {

  # Define metrics that already contain the variable name and their clean versions
  metric_name_mapping <- list(
    "depth_change_rate" = "change_rate",
    "depth_change_consistency" = "change_consistency",
    "posture_stability" = "stability",
    "activity_index" = "index",
    "net_heading_change" = "net_change",
    "cumulative_heading_change" = "cumulative_change",
    "circular_variance_heading" = "circular_variance",
    "turning_rate_variability" = "turning_variability",
    "circling_behavior" = "circling",
    "uturn_flag" = "uturn",
    "heading_autocorr_avg" = "autocorr_avg"
  )

  # Clean the metric name if it's redundant
  if (metric %in% names(metric_name_mapping)) {
    clean_metric <- metric_name_mapping[[metric]]
    base_name <- paste0(variable, "_", clean_metric)
  } else {
    # For regular metrics, combine variable + metric normally
    base_name <- paste0(variable, "_", metric)
  }

  # Check if metric already contains the variable name
  if (metric %in% names(metric_name_mapping)) {
    clean_metric <- metric_name_mapping[[metric]]
    base_name <- paste0(variable, "_", clean_metric)
  } else {
    # For regular metrics, combine variable + metric
    base_name <- paste0(variable, "_", metric)
  }

  # Add window suffix if different from default
  if (window_seconds != default_window_size) {
    final_name <- paste0(base_name, "_", window_seconds, "s")
  } else {
    final_name <- base_name
  }

  return(final_name)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
