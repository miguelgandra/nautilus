#######################################################################################################
# Native-R Madgwick AHRS orientation filter ###########################################################
#######################################################################################################

#' Madgwick AHRS orientation filter (native R implementation)
#'
#' @description
#' Pure-R implementation of Madgwick's gradient-descent AHRS filter (Madgwick, 2010),
#' replacing the previous dependency on Python's `ahrs` package via `reticulate`.
#' Estimates orientation quaternions from gyroscope, accelerometer and (optionally)
#' magnetometer data. When magnetometer data is supplied the MARG update is used
#' (full orientation incl. heading); otherwise the IMU update is used (roll/pitch only).
#'
#' @param gyr Numeric matrix (n x 3) of gyroscope readings in **rad/s** (x, y, z).
#' @param acc Numeric matrix (n x 3) of accelerometer readings (x, y, z); any consistent
#'   unit is fine since the vector is normalised internally.
#' @param mag Optional numeric matrix (n x 3) of magnetometer readings (x, y, z), or NULL
#'   for IMU-only mode. Normalised internally.
#' @param frequency Sampling frequency in Hz (used as a fixed time step `dt = 1/frequency`).
#' @param beta Filter gain (default 0.1). Larger values track faster but are noisier.
#'
#' @return A numeric matrix `Q` with n rows and 4 columns, the orientation quaternion
#'   `(w, x, y, z)` per sample (scalar-first convention; the initial quaternion is seeded
#'   from the first valid sample's measured orientation, see Details).
#'
#' @details
#' The initial quaternion is seeded from the first valid sample (roll/pitch from the accelerometer's
#' gravity direction, and heading from the tilt-compensated magnetometer in MARG mode) so the filter
#' starts at the measured orientation instead of the identity. This removes the slow start-of-record
#' convergence transient that a low `beta` would otherwise produce (the opening seconds would drift
#' from "level, north" toward the true orientation).
#'
#' Rows containing non-finite gyroscope or accelerometer values are skipped (the previous
#' quaternion is carried forward), so gaps do not corrupt the estimate. The implementation
#' follows Madgwick's reference equations; the sequential dependence between samples means
#' it is an inherently scalar loop (not vectorisable). For very large high-frequency series,
#' consider downsampling first or using `orientation.algorithm = "tilt_compass"`.
#'
#' @references
#' Madgwick, S. O. H. (2010). An efficient orientation filter for inertial and
#' inertial/magnetic sensor arrays. Technical report, University of Bristol.
#'
#' @note This function is intended for internal use within the `nautilus` package.
#' @keywords internal
#' @noRd

.madgwickAHRS <- function(gyr, acc, mag = NULL, frequency, beta = 0.1) {

  n <- nrow(acc)
  dt <- 1 / frequency
  use_mag <- !is.null(mag)

  # output quaternions, scalar-first (w, x, y, z). Seed the initial orientation from the first valid
  # sample's measured gravity (tilt) and field (heading) instead of the identity, so the filter starts
  # at the correct orientation - removing the slow start-of-record convergence transient that would
  # otherwise corrupt the opening seconds (especially at low beta).
  Q <- matrix(NA_real_, nrow = n, ncol = 4)
  q <- .madgwickSeed(acc, mag, use_mag)
  q0 <- q[1]; q1 <- q[2]; q2 <- q[3]; q3 <- q[4]

  for (i in seq_len(n)) {

    gx <- gyr[i, 1]; gy <- gyr[i, 2]; gz <- gyr[i, 3]
    ax <- acc[i, 1]; ay <- acc[i, 2]; az <- acc[i, 3]

    # skip the update on non-finite gyro/accel (carry the previous quaternion forward)
    if (!all(is.finite(c(gx, gy, gz, ax, ay, az)))) {
      Q[i, ] <- c(q0, q1, q2, q3)
      next
    }

    # rate of change of quaternion from the gyroscope
    qDot1 <- 0.5 * (-q1 * gx - q2 * gy - q3 * gz)
    qDot2 <- 0.5 * ( q0 * gx + q2 * gz - q3 * gy)
    qDot3 <- 0.5 * ( q0 * gy - q1 * gz + q3 * gx)
    qDot4 <- 0.5 * ( q0 * gz + q1 * gy - q2 * gx)

    # only apply the accelerometer/magnetometer feedback if the accel vector is non-zero
    if (!(ax == 0 && ay == 0 && az == 0)) {

      # normalise accelerometer
      recipNorm <- 1 / sqrt(ax * ax + ay * ay + az * az)
      ax <- ax * recipNorm; ay <- ay * recipNorm; az <- az * recipNorm

      mx <- if (use_mag) mag[i, 1] else NA_real_
      my <- if (use_mag) mag[i, 2] else NA_real_
      mz <- if (use_mag) mag[i, 3] else NA_real_
      marg <- use_mag && all(is.finite(c(mx, my, mz))) && !(mx == 0 && my == 0 && mz == 0)

      if (marg) {
        # ---- MARG update (accel + mag) ----
        # normalise magnetometer
        recipNorm <- 1 / sqrt(mx * mx + my * my + mz * mz)
        mx <- mx * recipNorm; my <- my * recipNorm; mz <- mz * recipNorm

        # auxiliary variables
        t2q0mx <- 2 * q0 * mx; t2q0my <- 2 * q0 * my; t2q0mz <- 2 * q0 * mz; t2q1mx <- 2 * q1 * mx
        t2q0 <- 2 * q0; t2q1 <- 2 * q1; t2q2 <- 2 * q2; t2q3 <- 2 * q3
        t2q0q2 <- 2 * q0 * q2; t2q2q3 <- 2 * q2 * q3
        q0q0 <- q0 * q0; q0q1 <- q0 * q1; q0q2 <- q0 * q2; q0q3 <- q0 * q3
        q1q1 <- q1 * q1; q1q2 <- q1 * q2; q1q3 <- q1 * q3
        q2q2 <- q2 * q2; q2q3 <- q2 * q3; q3q3 <- q3 * q3

        # reference direction of Earth's magnetic field
        hx <- mx * q0q0 - t2q0my * q3 + t2q0mz * q2 + mx * q1q1 + t2q1 * my * q2 + t2q1 * mz * q3 - mx * q2q2 - mx * q3q3
        hy <- t2q0mx * q3 + my * q0q0 - t2q0mz * q1 + t2q1mx * q2 - my * q1q1 + my * q2q2 + t2q2 * mz * q3 - my * q3q3
        t2bx <- sqrt(hx * hx + hy * hy)
        t2bz <- -t2q0mx * q2 + t2q0my * q1 + mz * q0q0 + t2q1mx * q3 - mz * q1q1 + t2q2 * my * q3 - mz * q2q2 + mz * q3q3
        t4bx <- 2 * t2bx; t4bz <- 2 * t2bz

        # gradient descent corrective step
        s0 <- -t2q2 * (2 * q1q3 - t2q0q2 - ax) + t2q1 * (2 * q0q1 + t2q2q3 - ay) -
          t2bz * q2 * (t2bx * (0.5 - q2q2 - q3q3) + t2bz * (q1q3 - q0q2) - mx) +
          (-t2bx * q3 + t2bz * q1) * (t2bx * (q1q2 - q0q3) + t2bz * (q0q1 + q2q3) - my) +
          t2bx * q2 * (t2bx * (q0q2 + q1q3) + t2bz * (0.5 - q1q1 - q2q2) - mz)
        s1 <- t2q3 * (2 * q1q3 - t2q0q2 - ax) + t2q0 * (2 * q0q1 + t2q2q3 - ay) -
          4 * q1 * (1 - 2 * q1q1 - 2 * q2q2 - az) +
          t2bz * q3 * (t2bx * (0.5 - q2q2 - q3q3) + t2bz * (q1q3 - q0q2) - mx) +
          (t2bx * q2 + t2bz * q0) * (t2bx * (q1q2 - q0q3) + t2bz * (q0q1 + q2q3) - my) +
          (t2bx * q3 - t4bz * q1) * (t2bx * (q0q2 + q1q3) + t2bz * (0.5 - q1q1 - q2q2) - mz)
        s2 <- -t2q0 * (2 * q1q3 - t2q0q2 - ax) + t2q3 * (2 * q0q1 + t2q2q3 - ay) -
          4 * q2 * (1 - 2 * q1q1 - 2 * q2q2 - az) +
          (-t4bx * q2 - t2bz * q0) * (t2bx * (0.5 - q2q2 - q3q3) + t2bz * (q1q3 - q0q2) - mx) +
          (t2bx * q1 + t2bz * q3) * (t2bx * (q1q2 - q0q3) + t2bz * (q0q1 + q2q3) - my) +
          (t2bx * q0 - t4bz * q2) * (t2bx * (q0q2 + q1q3) + t2bz * (0.5 - q1q1 - q2q2) - mz)
        s3 <- t2q1 * (2 * q1q3 - t2q0q2 - ax) + t2q2 * (2 * q0q1 + t2q2q3 - ay) +
          (-t4bx * q3 + t2bz * q1) * (t2bx * (0.5 - q2q2 - q3q3) + t2bz * (q1q3 - q0q2) - mx) +
          (-t2bx * q0 + t2bz * q2) * (t2bx * (q1q2 - q0q3) + t2bz * (q0q1 + q2q3) - my) +
          t2bx * q1 * (t2bx * (q0q2 + q1q3) + t2bz * (0.5 - q1q1 - q2q2) - mz)

      } else {
        # ---- IMU update (accel only) ----
        t2q0 <- 2 * q0; t2q1 <- 2 * q1; t2q2 <- 2 * q2; t2q3 <- 2 * q3
        t4q0 <- 4 * q0; t4q1 <- 4 * q1; t4q2 <- 4 * q2
        t8q1 <- 8 * q1; t8q2 <- 8 * q2
        q0q0 <- q0 * q0; q1q1 <- q1 * q1; q2q2 <- q2 * q2; q3q3 <- q3 * q3

        s0 <- t4q0 * q2q2 + t2q2 * ax + t4q0 * q1q1 - t2q1 * ay
        s1 <- t4q1 * q3q3 - t2q3 * ax + 4 * q0q0 * q1 - t2q0 * ay - t4q1 + t8q1 * q1q1 + t8q1 * q2q2 + t4q1 * az
        s2 <- 4 * q0q0 * q2 + t2q0 * ax + t4q2 * q3q3 - t2q3 * ay - t4q2 + t8q2 * q1q1 + t8q2 * q2q2 + t4q2 * az
        s3 <- 4 * q1q1 * q3 - t2q1 * ax + 4 * q2q2 * q3 - t2q2 * ay
      }

      # normalise the gradient step and apply the feedback
      recipNorm <- 1 / sqrt(s0 * s0 + s1 * s1 + s2 * s2 + s3 * s3)
      if (is.finite(recipNorm)) {
        s0 <- s0 * recipNorm; s1 <- s1 * recipNorm; s2 <- s2 * recipNorm; s3 <- s3 * recipNorm
        qDot1 <- qDot1 - beta * s0
        qDot2 <- qDot2 - beta * s1
        qDot3 <- qDot3 - beta * s2
        qDot4 <- qDot4 - beta * s3
      }
    }

    # integrate and normalise the quaternion
    q0 <- q0 + qDot1 * dt
    q1 <- q1 + qDot2 * dt
    q2 <- q2 + qDot3 * dt
    q3 <- q3 + qDot4 * dt
    recipNorm <- 1 / sqrt(q0 * q0 + q1 * q1 + q2 * q2 + q3 * q3)
    q0 <- q0 * recipNorm; q1 <- q1 * recipNorm; q2 <- q2 * recipNorm; q3 <- q3 * recipNorm

    Q[i, ] <- c(q0, q1, q2, q3)
  }

  Q
}


#' Seed the Madgwick filter's initial quaternion from the first valid sample.
#'
#' Computes roll/pitch from the first usable accelerometer reading (gravity direction) and, in MARG
#' mode, heading from the tilt-compensated magnetometer, then builds the scalar-first quaternion via
#' the ZYX (yaw-pitch-roll) Euler->quaternion that is the exact inverse of the quaternion->Euler
#' extraction used downstream (in `processTagData`). This makes the filter start at the measured
#' orientation rather than the identity, eliminating the start-of-record convergence transient.
#' Falls back to the identity quaternion when no usable accelerometer sample exists.
#' @param acc,mag Sensor matrices (n x 3); `mag` may be NULL.
#' @param use_mag Logical; whether magnetometer (MARG) seeding of the heading is possible.
#' @return Length-4 numeric quaternion `(w, x, y, z)`.
#' @keywords internal
#' @noRd
.madgwickSeed <- function(acc, mag, use_mag) {
  anorm <- acc[, 1]^2 + acc[, 2]^2 + acc[, 3]^2
  i0 <- which(is.finite(anorm) & anorm > 0)[1]
  if (is.na(i0)) return(c(1, 0, 0, 0))                 # no usable accelerometer sample -> identity

  ax <- acc[i0, 1]; ay <- acc[i0, 2]; az <- acc[i0, 3]
  roll  <- atan2(ay, az)                               # same aerospace convention as .tiltFromAccel
  pitch <- atan2(-ax, sqrt(ay^2 + az^2))
  yaw   <- 0
  if (use_mag) {
    mx <- mag[i0, 1]; my <- mag[i0, 2]; mz <- mag[i0, 3]
    if (all(is.finite(c(mx, my, mz))) && !(mx == 0 && my == 0 && mz == 0)) {
      mxc <- mx * cos(pitch) + my * sin(pitch) * sin(roll) + mz * sin(pitch) * cos(roll)
      myc <- my * cos(roll) - mz * sin(roll)
      yaw <- atan2(-myc, mxc)                          # tilt-compensated magnetic heading
    }
  }

  # ZYX Euler -> quaternion (inverse of the (roll, pitch, yaw) extraction in processTagData)
  cr <- cos(roll / 2); sr <- sin(roll / 2)
  cp <- cos(pitch / 2); sp <- sin(pitch / 2)
  cy <- cos(yaw / 2); sy <- sin(yaw / 2)
  c(cr * cp * cy + sr * sp * sy,
    sr * cp * cy - cr * sp * sy,
    cr * sp * cy + sr * cp * sy,
    cr * cp * sy - sr * sp * cy)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
