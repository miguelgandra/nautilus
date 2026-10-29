# Axis-mapping configuration, scoring, review and validation helpers.

# applyAxisMapping helpers ----

#######################################################################################################
# Internal: the axis-config dictionary (shared by applyAxisMapping + checkTagMapping) #################
#######################################################################################################

# A `configs` dictionary maps a config NAME (e.g. "CATS Camera") to a from/to mapping. Each deployment
# carries its config name in `tag$axis_config` (set at import from the `axis_config` metadata column);
# the expander looks it up, with a clear error on a name that is not in the dictionary (a typo). Used by
# both applyAxisMapping() (to apply) and checkTagMapping() (to validate against the data).

#' Validate a `configs` dictionary: a named list of from/to data.frames over IMU axes.
#' @keywords internal
#' @noRd
.validateConfigs <- function(configs) {
  if (!is.list(configs) || is.null(names(configs)) || any(!nzchar(names(configs))) || anyDuplicated(names(configs))) {
    .abort("{.arg configs} must be a uniquely-named list mapping a config name to a from/to data.frame.")
  }
  imu <- c("ax", "ay", "az", "gx", "gy", "gz", "mx", "my", "mz")
  for (nm in names(configs)) {
    ft <- configs[[nm]]
    if (!is.data.frame(ft) || !all(c("from", "to") %in% names(ft)) || nrow(ft) == 0L) {
      .abort("{.arg configs[[{.val {nm}}]]} must be a non-empty data.frame with columns {.val {c('from','to')}}.")
    }
    bad <- setdiff(as.character(ft$from), imu)
    if (length(bad)) .abort("{.arg configs[[{.val {nm}}]]}$from has non-IMU axis{?es} {.val {bad}}.")
  }
  invisible(configs)
}

#' Resolve one tag's config name (from its metadata) to its from/to mapping via the dictionary.
#'
#' Returns the from/to data.frame, or NULL when the tag has no documented config (blank/NA). Aborts with
#' an informative message if the named config is not in the dictionary.
#' @keywords internal
#' @noRd
.expandAxisConfigForTag <- function(axis_config, configs, id) {
  if (is.null(axis_config) || length(axis_config) == 0L || is.na(axis_config[1]) ||
      !nzchar(trimws(as.character(axis_config[1])))) return(NULL)
  cfg <- trimws(as.character(axis_config[1]))
  if (!cfg %in% names(configs)) {
    .abort(c("{.val {id}} names axis config {.val {cfg}}, which is not in {.arg configs}.",
             "i" = "Configs provided: {.val {names(configs)}}."))
  }
  configs[[cfg]]
}

#######################################################################################################
# Internal: normalise any accepted mapping shape into a routing set ###################################
#######################################################################################################

# Returns list(single, by_id, producer, provenance):
#   single     - a from/to data.frame applied to every dataset, or NULL
#   by_id      - a named list of per-deployment from/to data.frames, or NULL
#   producer   - "manual" | "checkTagMapping" | "consensusAxisMapping"
#   provenance - NULL (apply-to-all), or a named (by id) list of per-family origin vectors
#' @keywords internal
#' @noRd
.asAxisMappingSet <- function(mapping) {
  # (0) a reviewed decision sheet: overlay the human choices onto its embedded base mapping
  if (inherits(mapping, "nautilus_review")) return(.resolveReviewSet(mapping))

  fam_axes <- list(accel = c("ax", "ay", "az"), gyro = c("gx", "gy", "gz"), mag = c("mx", "my", "mz"))
  fams_in <- function(ft) {                                     # families present (non-empty) in a from/to df
    if (is.null(ft) || !is.data.frame(ft) || !nrow(ft)) return(character(0))
    names(Filter(function(ax) any(ft$from %in% ax), fam_axes))
  }

  # (1) a single from/to data.frame -> apply to every dataset
  if (is.data.frame(mapping)) {
    if (!all(c("from", "to") %in% names(mapping)) || !nrow(mapping)) {
      .abort("{.arg mapping} data.frame must have non-empty {.field from} and {.field to} columns.")
    }
    return(list(single = as.data.frame(mapping[, c("from", "to")], stringsAsFactors = FALSE),
                by_id = NULL, producer = "manual", provenance = NULL))
  }

  if (!is.list(mapping)) {
    .abort("{.arg mapping} must be a from/to data.frame, a named list of them, or the output of {.fn checkTagMapping} / {.fn consensusAxisMapping}.")
  }

  # (2) consensusAxisMapping() result: $mappings (reconciled) + $provenance (per-family origin)
  if (!is.null(mapping$mappings) && is.data.frame(mapping$provenance)) {
    by_id <- mapping$mappings
    pv    <- mapping$provenance
    provenance <- stats::setNames(lapply(names(by_id), function(id) {
      row <- pv[pv$id == id, , drop = FALSE]
      if (!nrow(row)) return(NULL)
      stats::setNames(c(row$accel[1], row$gyro[1], row$mag[1]), c("accel", "gyro", "mag"))
    }), names(by_id))
    return(list(single = NULL, by_id = by_id, producer = "consensusAxisMapping", provenance = provenance))
  }

  # (3) checkTagMapping() result: a named list whose elements carry $proposal (each deployment's own evidence)
  has_prop <- vapply(mapping, function(e) is.list(e) && is.data.frame(e$proposal), logical(1))
  if (length(has_prop) && all(has_prop)) {
    ids   <- names(mapping) %||% vapply(mapping, function(e) as.character(e$id %||% NA), character(1))
    by_id <- stats::setNames(lapply(mapping, function(e) e$proposal), ids)
    provenance <- stats::setNames(lapply(by_id, function(ft) {
      f <- fams_in(ft); if (!length(f)) NULL else stats::setNames(rep("self", length(f)), f)
    }), ids)
    return(list(single = NULL, by_id = by_id, producer = "checkTagMapping", provenance = provenance))
  }

  # (4) a plain named list of from/to data.frames (hand-built, one per deployment id)
  is_ft <- vapply(mapping, function(e) is.data.frame(e) && all(c("from", "to") %in% names(e)), logical(1))
  if (length(is_ft) && all(is_ft)) {
    if (is.null(names(mapping))) .abort("A list of from/to tables must be named by deployment {.field id}.")
    provenance <- stats::setNames(lapply(mapping, function(ft) {
      f <- fams_in(ft); if (!length(f)) NULL else stats::setNames(rep("manual", length(f)), f)
    }), names(mapping))
    return(list(single = NULL, by_id = mapping, producer = "manual", provenance = provenance))
  }

  .abort("{.arg mapping} is not a recognised mapping object (a from/to data.frame, a named list of them, or the output of {.fn checkTagMapping} / {.fn consensusAxisMapping}).")
}

#' Resolve a `nautilus_review` into a routing set: overlay each decided candidate onto the base mapping.
#'
#' Un-reviewed deployments keep the base mapping (and its provenance); a reviewed deployment with a
#' decision is replaced by the chosen candidate's from/to (provenance "review"). A reviewable deployment
#' (clips were rendered) that offers a genuine choice but carries no decision is an error - the workflow
#' refuses to apply a handedness the reviewer has not confirmed.
#' @keywords internal
#' @noRd
.resolveReviewSet <- function(review) {
  base  <- attr(review, "review_base")
  cands <- attr(review, "review_candidates") %||% list()
  if (is.null(base)) .abort("This {.cls nautilus_review} carries no base mapping; re-run {.fn reviewTagMapping}.")
  by_id <- base$by_id %||% list()
  prov  <- base$provenance %||% stats::setNames(vector("list", length(by_id)), names(by_id))
  fam_axes <- list(accel = c("ax", "ay", "az"), gyro = c("gx", "gy", "gz"), mag = c("mx", "my", "mz"))
  fams_in  <- function(ft) if (is.null(ft) || !nrow(ft)) character(0) else names(Filter(function(ax) any(ft$from %in% ax), fam_axes))

  unresolved <- character(0); excluded <- character(0)
  for (i in seq_len(nrow(review))) {
    id <- as.character(review$id[i]); dec <- review$decision[i]
    # "exclude" is a universal disposition (valid on any row): the deployment is dropped from the output
    # entirely - the orientation cannot be trusted and no candidate is correct. Checked first, so it
    # overrides the undecided-reviewable guard and needs no candidate.
    if (!is.na(dec) && identical(.normDecision(dec), "exclude")) {
      excluded <- c(excluded, id); by_id[[id]] <- NULL; prov[[id]] <- NULL; next
    }
    cand_i <- cands[[id]]
    if (is.null(cand_i) || !length(cand_i)) next                  # single-path / no choice -> base applies as-is
    if (is.na(dec) || !nzchar(dec)) {
      if (isTRUE(review$n_clips[i] > 0)) unresolved <- c(unresolved, id)   # reviewable but undecided
      next
    }
    labs <- vapply(cand_i, `[[`, "", "label")
    k <- match(.normDecision(dec), .normDecision(labs))           # tolerant of the casing the user typed
    if (is.na(k)) .abort(c("Review decision {.val {dec}} for {.val {id}} is not one of its options.",
                           "i" = "Options: {.val {labs}}, or {.val Exclude} to drop the deployment."))
    ft <- cand_i[[k]]$from_to
    if (is.null(ft) || !nrow(ft)) { by_id[[id]] <- NULL; prov[[id]] <- NULL; next }   # e.g. "Raw" -> leave unmapped
    by_id[[id]] <- as.data.frame(ft[, c("from", "to")], stringsAsFactors = FALSE)
    prov[[id]]  <- stats::setNames(rep("review", length(fams_in(ft))), fams_in(ft))
  }
  if (length(unresolved))
    .abort(c("{length(unresolved)} reviewed deployment{?s} {?has/have} no decision yet: {.val {unresolved}}.",
             "i" = "Fill {.code review$decision} (see {.code print(review)}), then apply again."))
  list(single = NULL, by_id = by_id, producer = "reviewTagMapping", provenance = prov, excluded = excluded)
}

# checkTagMapping helpers ----

# A canonical string key for a signed-permutation matrix (body = M %*% raw), in the same
# X-col|X-sign|Y-col|Y-sign|Z-col|Z-sign order used for the `survivors` rows, so a documented config's
# matrix can be tested for membership in the candidate set by a simple string match.
#' @keywords internal
#' @noRd
.permMatrixKey <- function(M, axes) {
  parts <- character(0)
  for (i in 1:3) { j <- which(M[i, ] != 0)[1]; parts <- c(parts, axes[j], as.character(M[i, j])) }
  paste(parts, collapse = "|")
}

################################################################################
# Internal function for evaluate a given mapping ###############################
################################################################################

# Define a helper function to calculate mapping score

#
# Score ALL candidate mappings for one individual in a single pass. A signed permutation only
# reorders / sign-flips the three raw axes, so the expensive parts are rotation-invariant and are
# computed ONCE here rather than 24x: the three raw-axis static (gravity) components, the static
# vector magnitude, the static-period mask, and the per-axis static medians. Each candidate is then
# scored by indexing + sign + two atan2 calls. The score is algebraically identical to the previous
# per-candidate implementation (staticX_mapped = sign * static[perm]; magnitude is sign/perm
# invariant), so results are unchanged - only far cheaper. Returns a data.frame (one row per grid
# row, in grid order) with median_abs_roll / median_abs_pitch / median_static_az / score.
.scoreAxisMappings <- function(individual_data, permutations_grid, ax.col, ay.col, az.col,
                               window_size, pad_length, static.threshold, g.value,
                               posture.metric = "median") {

  epsilon <- 1e-7
  raw <- c(ax.col, ay.col, az.col)
  # posture term: "median" rewards near-zero roll/pitch (level resting posture; rigid mounts);
  # "mad" rewards stability regardless of the resting offset (towed / fin-clamp mounts, or unknown)
  posture <- if (identical(posture.metric, "mad"))
    function(x) stats::mad(x, na.rm = TRUE) else function(x) median(abs(x), na.rm = TRUE)

  # the three raw-axis static (gravity) components, computed once and reused by every candidate
  s <- stats::setNames(lapply(raw, function(cc) .pad_rollmean(individual_data[[cc]], window_size, pad_length)), raw)
  if (all(is.na(s[[ax.col]])) || all(is.na(s[[ay.col]])) || all(is.na(s[[az.col]]))) {
    .abort(c("All static acceleration values are NA after the rolling mean.",
             "i" = "The data is likely too short for {.arg dba.window} at this sampling rate, or contains extensive NAs (expected minimum {window_size} samples)."))
  }

  # rotation-invariant static magnitude + static-period mask (identical for all 24 candidates)
  accel_mag <- sqrt(s[[ax.col]]^2 + s[[ay.col]]^2 + s[[az.col]]^2 + epsilon)
  static_mask <- abs(accel_mag - g.value) < static.threshold
  static_mask[is.na(static_mask)] <- FALSE

  # normalized static components per raw axis; finiteness is the same for every candidate (a signed
  # permutation of finite values stays finite), so check it once
  norm <- stats::setNames(lapply(raw, function(cc) s[[cc]] / accel_mag), raw)
  finite_ok <- all(vapply(norm, function(v) all(is.finite(v)), logical(1)))

  # per-raw-axis median of the static component over the static periods (drives the static-az term)
  med_static <- if (any(static_mask))
    stats::setNames(vapply(raw, function(cc) stats::median(s[[cc]][static_mask], na.rm = TRUE), numeric(1)), raw) else NULL

  np <- nrow(permutations_grid)
  out <- data.frame(median_abs_roll = rep(NA_real_, np), median_abs_pitch = rep(NA_real_, np),
                    median_static_az = rep(NA_real_, np), score = rep(NA_real_, np))

  for (p in seq_len(np)) {
    px <- permutations_grid$newX_col[p]; py <- permutations_grid$newY_col[p]; pz <- permutations_grid$newZ_col[p]
    sx <- permutations_grid$newX_sign[p]; sy <- permutations_grid$newY_sign[p]; sz <- permutations_grid$newZ_sign[p]

    # reject (score Inf) when the static vector is degenerate or there are no static periods, exactly
    # as the previous implementation did (it then left the medians NA)
    if (!finite_ok || is.null(med_static)) { out$score[p] <- Inf; next }

    ax_norm <- sx * norm[[px]]; ay_norm <- sy * norm[[py]]; az_norm <- sz * norm[[pz]]
    roll_deg  <- atan2(ay_norm, az_norm) * (180 / pi)
    pitch_deg <- atan2(-ax_norm, sqrt(ay_norm^2 + az_norm^2 + epsilon)) * (180 / pi)

    mar <- posture(roll_deg)
    map <- posture(pitch_deg)
    msz <- sz * med_static[[pz]]                          # median(staticZ_mapped) over static periods
    sc  <- mar + map + abs(msz - g.value)

    out$median_abs_roll[p]  <- mar
    out$median_abs_pitch[p] <- map
    out$median_static_az[p] <- msz
    out$score[p] <- if (is.na(sc)) Inf else sc
  }

  out
}

################################################################################
# Internal function: break the surge/sway tie using diving dynamics ############
################################################################################

# Pearson correlation that degrades gracefully instead of erroring. stats::cor(use = "complete.obs")
# throws "no complete element pairs" when two series never overlap on finite values (e.g. a camera tag
# whose gyro columns are present but all-NA), which suppressWarnings cannot catch. Returns NA_real_
# when there are fewer than 3 complete pairs or either series is constant; every caller already treats
# a non-finite correlation as "unresolved".
#' @keywords internal
#' @noRd
.safeCor <- function(a, b) {
  ok <- is.finite(a) & is.finite(b)
  if (sum(ok) < 3L) return(NA_real_)
  a <- a[ok]; b <- b[ok]
  if (stats::sd(a) < 1e-9 || stats::sd(b) < 1e-9) return(NA_real_)
  stats::cor(a, b)
}

# Block-mean decimation to `target_fs` Hz: groups rows into fixed time buckets and averages each named
# numeric column. This is an anti-aliasing low-pass that preserves all structure below target_fs / 2,
# letting the (low-frequency) axis resolution run on far fewer rows. Returns a data.table with the
# datetime column (at bucket starts) + the averaged value columns.
#' @keywords internal
#' @noRd
.decimate <- function(dt, value.cols, datetime.col, target_fs) {
  tnum <- as.numeric(dt[[datetime.col]])
  bucket <- floor((tnum - tnum[1]) * target_fs)
  out <- dt[, lapply(.SD, mean, na.rm = TRUE), by = list(.bucket = bucket), .SDcols = value.cols]
  out[, (datetime.col) := dt[[datetime.col]][1] + .bucket / target_fs]
  out[, .bucket := NULL]
  data.table::setcolorder(out, c(datetime.col, value.cols))
  out[]
}

# Tail-beat corroboration of the chosen surge/sway/heave assignment: the body `loco.axis` (the
# expected propulsive axis) should carry the most locomotor-band power of the three. Runs on the
# tail-beat-rate copy (`tb_data`, decimated only far enough to keep the locomotor band below Nyquist),
# not the 1 Hz resolution copy. Returns the per-axis band powers (named surge/sway/heave), the
# corroboration flag, and the signed dynamic axes (reused by the diagnostic panel), or NULL if too short.
#' @keywords internal
#' @noRd
.tailBeatCorroboration <- function(tb_data, best, ax.col, ay.col, az.col, fs, locomotor.band, loco.axis = "sway") {
  win <- max(round(2.5 * fs), 3L); if (win %% 2L == 0L) win <- win + 1L
  sd <- .staticDynamicAccel(tb_data[[ax.col]], tb_data[[ay.col]], tb_data[[az.col]], win)
  dyn_by_raw <- stats::setNames(list(sd$dynamic$x, sd$dynamic$y, sd$dynamic$z), c(ax.col, ay.col, az.col))
  pw <- c(surge = .bandPower(dyn_by_raw[[best$newX_col]], fs, locomotor.band),
          sway  = .bandPower(dyn_by_raw[[best$newY_col]], fs, locomotor.band),
          heave = .bandPower(dyn_by_raw[[best$newZ_col]], fs, locomotor.band))
  if (all(!is.finite(pw))) return(NULL)
  corroborated <- is.finite(pw[[loco.axis]]) && pw[[loco.axis]] >= max(pw, na.rm = TRUE)
  list(power = pw, corroborated = corroborated, dynamic = dyn_by_raw, fs = fs)
}

# Among the static-tied candidates (which share the vertical axis but differ in the horizontal
# assignment), the correct surge axis is the one whose body pitch tracks vertical velocity during
# dives. Returns the chosen candidate (index into `near`), the pitch<->depth-rate correlation, and a
# decisiveness flag -- or NULL if there is not enough diving signal to decide. The static/dynamic
# decomposition is passed in (`sd`, computed once by the caller) to avoid recomputing it. Tail-beat
# corroboration is handled separately by .tailBeatCorroboration on the higher-rate copy.

#' @keywords internal
#' @noRd
.resolveSurge <- function(full_data, near, sd, ax.col, ay.col, az.col,
                          dive.speed.threshold, min.dynamic.corr, max.vertical.speed) {

  vs <- full_data[["vertical_speed"]]
  # Diving samples only, AND physically plausible. A depth-sensor artifact (e.g. a stuck / saturated
  # block) injects |vertical velocity| of tens to hundreds of m/s; being extreme outliers, those few
  # samples dominate the Pearson pitch <-> depth-rate correlation below and silently destroy the surge
  # decision (the correlation collapses toward zero). A swimming animal cannot move vertically faster
  # than `max.vertical.speed`, so anything beyond it is a depth artifact, not signal, and is excluded.
  mask <- is.finite(vs) & abs(vs) > dive.speed.threshold & abs(vs) <= max.vertical.speed
  if (sum(mask, na.rm = TRUE) < 30L) return(NULL)            # not enough diving signal to decide

  static_by_raw <- stats::setNames(list(sd$static$x, sd$static$y, sd$static$z), c(ax.col, ay.col, az.col))

  # pitch<->vertical-speed correlation for each tied candidate (most negative = correct surge,
  # since a descending animal (vs > 0) pitches nose-down (pitch < 0))
  corrs <- vapply(seq_len(nrow(near)), function(k) {
    surge_s <- near$newX_sign[k] * static_by_raw[[near$newX_col[k]]]
    sway_s  <- near$newY_sign[k] * static_by_raw[[near$newY_col[k]]]
    heave_s <- near$newZ_sign[k] * static_by_raw[[near$newZ_col[k]]]
    pitch <- .tiltFromAccel(surge_s, sway_s, heave_s)$pitch
    .safeCor(pitch[mask], vs[mask])
  }, numeric(1))

  ord <- order(corrs)                                        # most negative first
  best <- ord[1]; best_corr <- corrs[best]
  separation <- if (length(ord) > 1 && is.finite(corrs[ord[2]])) corrs[ord[2]] - best_corr else Inf
  decisive <- is.finite(best_corr) && best_corr <= -min.dynamic.corr && separation >= 0.15

  list(best_index = best, corr = corrs, best_corr = best_corr, separation = separation,
       decisive = decisive, n_dynamic = sum(mask, na.rm = TRUE))
}

################################################################################
# Internal function: resolve the GYROSCOPE mapping vs the body frame ###########
################################################################################

# Given the resolved accelerometer body frame (its pitch / roll time series), the gyroscope axis
# whose signal tracks roll-rate is body X (surge), the one tracking pitch-rate is body Y (sway), and
# the remainder is body Z (yaw); signs come from the correlation signs, with Z closed by handedness.
# Returns a from/to mapping (raw gyro -> body) + diagnostics, or NULL if gyro is absent / motion is
# insufficient / the assignment is ambiguous.

#' @keywords internal
#' @noRd
.resolveGyro <- function(full_data, pitch_t, roll_t, fs, min.dynamic.corr) {
  if (!all(c("gx", "gy", "gz") %in% names(full_data))) return(NULL)
  dt <- 1 / fs
  pitch_rate <- c(NA, diff(pitch_t)) / dt
  roll_rate  <- c(NA, diff(roll_t))  / dt
  # NA-safe variance gate: sd() is NA with < 2 finite values, so guard against that too (an all-NA
  # tilt would otherwise make the comparison NA and error in `if`).
  sd_p <- stats::sd(pitch_rate, na.rm = TRUE); sd_r <- stats::sd(roll_rate, na.rm = TRUE)
  if (!is.finite(sd_p) || !is.finite(sd_r) || sd_p < 1e-6 || sd_r < 1e-6) return(NULL)

  G <- list(gx = full_data$gx, gy = full_data$gy, gz = full_data$gz)
  cor_roll  <- vapply(G, function(g) .safeCor(g, roll_rate),  numeric(1))
  cor_pitch <- vapply(G, function(g) .safeCor(g, pitch_rate), numeric(1))
  # We only need the axis that best tracks roll-rate (surge) and the one tracking pitch-rate (sway).
  # The remaining (yaw) axis legitimately need NOT correlate with either - and a near-constant yaw
  # channel (e.g. after downsampling removes its high-frequency content) yields an NA correlation - so
  # require only that at least one axis correlates with each, never that all nine are finite.
  if (all(!is.finite(cor_roll)) || all(!is.finite(cor_pitch))) return(NULL)

  x_axis <- names(G)[which.max(abs(cor_roll))]      # surge: tracks roll-rate (which.max ignores NA)
  y_axis <- names(G)[which.max(abs(cor_pitch))]     # sway:  tracks pitch-rate
  if (x_axis == y_axis) return(list(decisive = FALSE, reason = "gyro surge/sway map to the same raw axis"))
  if (!is.finite(cor_roll[[x_axis]]) || !is.finite(cor_pitch[[y_axis]])) return(NULL)
  z_axis <- setdiff(names(G), c(x_axis, y_axis))
  x_sign <- sign(cor_roll[[x_axis]]); y_sign <- sign(cor_pitch[[y_axis]])

  # build body = M %*% raw with z sign chosen to make a proper rotation (det +1)
  axes <- c("gx", "gy", "gz")
  M <- matrix(0, 3, 3)
  M[1, match(x_axis, axes)] <- x_sign
  M[2, match(y_axis, axes)] <- y_sign
  M[3, match(z_axis, axes)] <- 1
  if (.signedPermDet(M) == -1L) M[3, match(z_axis, axes)] <- -1
  mapping <- .matrixToFromTo(M, axes, axes)

  strength <- min(abs(cor_roll[[x_axis]]), abs(cor_pitch[[y_axis]]))
  list(decisive = strength >= min.dynamic.corr, mapping = mapping,
       cor_roll = cor_roll[[x_axis]], cor_pitch = cor_pitch[[y_axis]], strength = strength)
}

################################################################################
# Internal function: frame-agnostic GYRO validator (strapdown consistency) #####
################################################################################

# Frame-agnostic gyroscope RESOLVER + cross-check, via the strapdown identity for the gravity DIRECTION,
# dghat/dt = -omega x ghat. For each of the 24 proper rotations R (gyro-chip -> accel-chip) it maps the
# gyro into the accel-chip frame, predicts dghat/dt from (R . omega) and the measured ghat, and
# correlates the prediction with the MEASURED dghat/dt; the true R satisfies the identity and wins.
#
# CRITICAL (learned from real data): both sides must be band-limited to the POSTURE timescale. The
# gravity direction is the static (heavily smoothed) accel, so dghat/dt lives in a sub-Hz band; the raw
# gyro is full-bandwidth and dominated by tail-beat / yaw - rotation ABOUT gravity, which omega x ghat
# cannot see. Correlating the two directly buries the signal (best r ~ 0.01 on real tags). Low-passing
# the gyro to the SAME posture band before the cross product, using a clean +/- stencil derivative, and
# restricting the correlation to the most-rotating samples lifts the correct R to r ~ 0.66-0.83 with
# clear separation on tags with posture signal, while flat swimmers stay low and ABSTAIN. The estimator
# is SELF-CONTAINED (re-derives the posture-band gravity from the raw accel at a fixed window,
# independent of the caller's dba.window) and TILT-INVARIANT (a whole-tag tilt cancels in the relative
# mapping). It resolves the gyro-vs-accel RELATIVE frame only - NOTHING about the accel vertical.
# Returns list(decisive, motion_ok, R, corr, separation, n), or NULL if the gyro/accel is absent or the
# series is too short.
#' @keywords internal
#' @noRd
.resolveGyroFrame <- function(full_data, fs, accel.cols, posture.window = 5, deriv.window = 1,
                              min.corr = 0.5, min.separation = 0.1, motion.quantile = 0.6,
                              min.motion = 1e-5) {
  if (!all(c("gx", "gy", "gz") %in% names(full_data))) return(NULL)
  if (!all(accel.cols %in% names(full_data))) return(NULL)
  n <- nrow(full_data); if (is.null(n) || n < 60L) return(NULL)
  w <- max(3L, round(posture.window * fs))                     # posture-band smoothing window (samples)
  lp <- function(x) data.table::frollmean(x, w, fill = NA, align = "center")

  # posture-band gravity DIRECTION from the accel (self-contained), and the gyro low-passed to the SAME
  # band - this is the key step that removes the tail-beat / yaw energy swamping the strapdown signal.
  sx <- lp(full_data[[accel.cols[1]]]); sy <- lp(full_data[[accel.cols[2]]]); sz <- lp(full_data[[accel.cols[3]]])
  gmag <- sqrt(sx^2 + sy^2 + sz^2)
  ghat <- cbind(sx, sy, sz) / gmag
  Wf <- cbind(lp(full_data$gx), lp(full_data$gy), lp(full_data$gz))

  # clean central difference of ghat over a +/- (deriv.window/2) stencil (a 1-sample diff of a smoothed
  # signal is noise-dominated at high rate)
  k <- max(1L, round(0.5 * deriv.window * fs))
  dgh <- (rbind(ghat[-(1:k), , drop = FALSE], matrix(NA_real_, k, 3)) -
          rbind(matrix(NA_real_, k, 3), ghat[seq_len(n - k), , drop = FALSE])) / (2 * k / fs)

  # restrict to the most-rotating samples (high SNR for the gravity-direction rate)
  mag <- sqrt(rowSums(dgh^2))
  ok0 <- is.finite(rowSums(Wf)) & is.finite(rowSums(ghat)) & is.finite(mag)
  if (sum(ok0) < 30L) return(NULL)
  thr <- stats::quantile(mag[ok0], motion.quantile, na.rm = TRUE)
  ok <- ok0 & mag >= thr
  if (sum(ok) < 30L) return(NULL)
  motion <- mean(mag[ok]^2, na.rm = TRUE)
  if (!is.finite(motion) || motion < min.motion)
    return(list(decisive = FALSE, motion_ok = FALSE, corr = NA_real_, corr_identity = NA_real_,
                separation = NA_real_, n = sum(ok)))

  Wk <- Wf[ok, , drop = FALSE]; gk <- ghat[ok, , drop = FALSE]; meas <- as.vector(dgh[ok, , drop = FALSE])
  rots <- .properRotations()
  rowcross <- function(A, B) cbind(A[, 2] * B[, 3] - A[, 3] * B[, 2],
                                   A[, 3] * B[, 1] - A[, 1] * B[, 3],
                                   A[, 1] * B[, 2] - A[, 2] * B[, 1])
  # co-die (R = identity) co-registration score: whether the RAW gyro already tracks the RAW accel's
  # gravity-direction rate. By signed-permutation invariance this equals the body-frame co-registration
  # of the derived map (accel via M, gyro via det(M)*M), so it directly scores the co-die default.
  corr_identity <- .safeCor(as.vector(-rowcross(Wk, gk)), meas)
  corrs <- vapply(rots, function(R) .safeCor(as.vector(-rowcross(Wk %*% t(R), gk)), meas), numeric(1))
  o <- order(corrs, decreasing = TRUE)
  best_corr <- corrs[o[1]]
  sep <- if (length(o) > 1 && is.finite(corrs[o[2]])) best_corr - corrs[o[2]] else Inf
  decisive <- is.finite(best_corr) && best_corr >= min.corr && sep >= min.separation
  list(decisive = decisive, motion_ok = TRUE, R = rots[[o[1]]], corr = best_corr,
       corr_identity = corr_identity, separation = sep, n = sum(ok))
}

################################################################################
# Internal function: resolve the MAGNETOMETER mapping vs the body frame ########
################################################################################

# The angle between the (earth-referenced) magnetic field and gravity is the geomagnetic dip
# complement -- constant in time only when the magnetometer is correctly aligned to the body frame.
# A mis-aligned VERTICAL makes that angle vary with posture. BUT the angle is invariant to rotations
# about gravity, so the dip alone cannot fix the magnetometer's HEADING axes (a 4-fold ambiguity,
# analogous to the accelerometer surge/sway tie -- there is no absolute heading reference).
#
# We therefore validate the common-frame hypothesis: apply the resolved ACCELEROMETER mapping to the
# magnetometer and test whether it yields a constant dip. If so, the magnetometer is consistent with
# (and adopts) the accelerometer/body frame. If a different vertical fits far better, we flag the
# magnetometer as inconsistent (a genuinely different mag frame) rather than guess its heading.
# Returns a from/to mapping + measured dip + diagnostics, or NULL if mag is absent / posture too static.

#' @keywords internal
#' @noRd
.resolveMag <- function(full_data, gbody, M_acc, fs, window_size, min.angle.sd = 8, hard.iron = TRUE) {
  if (!all(c("mx", "my", "mz") %in% names(full_data))) return(NULL)

  # provisional hard-iron (spherical) correction for the dip diagnostic only - never persisted. Many
  # deployments lack a magnetometer calibration; an uncorrected hard-iron bias rotates relative to the
  # field as the animal turns, so the dip varies even for a correctly-aligned sensor and the
  # common-frame test yields false "inconsistent" flags. Centring the sphere removes that, but only
  # when orientation coverage is sufficient (else the estimate is unreliable and is skipped).
  mxv <- full_data$mx; myv <- full_data$my; mzv <- full_data$mz
  hi <- if (hard.iron) .hardIronOffset(mxv, myv, mzv) else NULL
  hi_applied <- !is.null(hi) && isTRUE(hi$coverage_ok)
  if (hi_applied) { mxv <- mxv - hi$offset[1]; myv <- myv - hi$offset[2]; mzv <- mzv - hi$offset[3] }

  # smooth the magnetometer to suppress high-frequency noise, then unit-normalise gravity
  smx <- .rollmeanCentered(mxv, window_size)
  smy <- .rollmeanCentered(myv, window_size)
  smz <- .rollmeanCentered(mzv, window_size)
  gmag <- sqrt(rowSums(gbody^2)); ghat <- gbody / gmag
  ok <- is.finite(smx) & is.finite(smy) & is.finite(smz) & is.finite(gmag) & gmag > 0
  if (sum(ok) < 50L) return(NULL)
  mm <- cbind(smx, smy, smz)[ok, , drop = FALSE]
  gh <- ghat[ok, , drop = FALSE]

  angle_stats <- function(M) {
    rm <- mm %*% t(M)
    rn <- sqrt(rowSums(rm^2)); keep <- rn > 0
    dotp <- rowSums((rm[keep, , drop = FALSE] / rn[keep]) * gh[keep, , drop = FALSE])
    ang <- acos(pmax(-1, pmin(1, dotp))) * 180 / pi
    list(sd = stats::sd(ang, na.rm = TRUE), mean = mean(ang, na.rm = TRUE))
  }

  # the data must actually discriminate verticals (posture varied enough)
  sds <- vapply(.properRotations(), function(M) angle_stats(M)$sd, numeric(1))
  if (!is.finite(min(sds)) || (max(sds) - min(sds)) < min.angle.sd) {
    return(list(decisive = FALSE, reason = "insufficient posture variation to validate magnetometer",
                hard_iron_applied = hi_applied, hard_iron_offset = if (hi_applied) hi$offset else NULL))
  }

  # validate the accelerometer frame on the magnetometer
  acc <- angle_stats(M_acc)
  consistent <- is.finite(acc$sd) && acc$sd <= min.angle.sd && acc$sd <= min(sds) + min.angle.sd
  mapping <- .matrixToFromTo(M_acc, c("mx", "my", "mz"), c("mx", "my", "mz"))
  list(decisive = consistent, mapping = if (consistent) mapping else NULL,
       angle_sd = acc$sd, best_possible_sd = min(sds),
       measured_inclination = 90 - acc$mean, consistent_with_accel = consistent,
       hard_iron_applied = hi_applied, hard_iron_offset = if (hi_applied) hi$offset else NULL)
}

################################################################################
# Internal helpers: keep the diagnostic panels small + fast to render ##########
################################################################################

# Welch-averaged power spectrum: split the (finite) series into fixed-length, 50%-overlapping,
# Hann-windowed segments and average their periodograms. The number of frequency bins is bounded by
# the segment length (not the series length), so the spectra panel has a constant, small vector
# footprint regardless of deployment duration - and the averaged estimate is smoother than a single
# full-length FFT. Returns list(freq, power) or NULL if the series is too short.
#' @keywords internal
#' @noRd
.welchSpectrum <- function(x, fs, seg = 1024L, overlap = 0.5) {
  x <- x[is.finite(x)]
  n <- length(x)
  if (n < 16L) return(NULL)
  seg <- as.integer(min(seg, n))
  step <- max(1L, floor(seg * (1 - overlap)))
  starts <- seq.int(1L, n - seg + 1L, by = step); if (!length(starts)) starts <- 1L
  w <- 0.5 - 0.5 * cos(2 * pi * (seq_len(seg) - 1L) / (seg - 1L))   # Hann window
  wnorm <- sum(w^2)
  half <- 2:(seg %/% 2L + 1L)                                       # drop DC; positive frequencies
  acc <- numeric(length(half))
  for (s in starts) {
    seg_x <- x[s:(s + seg - 1L)]
    seg_x <- (seg_x - mean(seg_x)) * w
    acc <- acc + (Mod(stats::fft(seg_x))^2)[half] / wnorm
  }
  list(freq = (half - 1L) * fs / seg, power = acc / length(starts))
}

# Min/max-envelope decimation of a line series for plotting: bins the points and keeps the per-bin
# minimum and maximum (so spikes / the visible envelope survive), reducing a multi-thousand-point path
# to ~`n_target` points. No-op when already short enough.
#' @keywords internal
#' @noRd
.decimateForPlot <- function(x, y, n_target = 2000L) {
  n <- length(x)
  if (n <= n_target) return(list(x = x, y = y))
  bin <- as.integer(cut(seq_len(n), breaks = n_target %/% 2L, labels = FALSE))
  keep <- unlist(lapply(split(seq_len(n), bin), function(ii) {
    yy <- y[ii]; if (all(is.na(yy))) return(ii[1])
    unique(c(ii[which.min(yy)], ii[which.max(yy)]))
  }), use.names = FALSE)
  keep <- sort(keep)
  list(x = x[keep], y = y[keep])
}

# Draw an "Expected:" guide-rail caption along the bottom of the current panel, auto-sizing the text
# so it always fits the panel width (long strings shrink rather than overrun into neighbours).
#' @keywords internal
#' @noRd
.panelHint <- function(hint) {
  w <- graphics::strwidth(hint, units = "inches", cex = 1)
  cex <- if (w > 0) min(0.62, (graphics::par("pin")[1] * 0.98) / w) else 0.62
  graphics::mtext(hint, side = 1, line = 3.15, cex = cex, col = "grey45", font = 3)
}

################################################################################
# Internal function: visual validation panel for an axis-mapping result ########
################################################################################

# A 2x2 diagnostic panel: (1) the surge anchor (depth-rate vs body pitch), (2) the gravity partition
# over the strongest dive with a depth-profile backdrop, (3) the per-axis dynamic spectra (the sway
# axis should carry the tail-beat peak), and (4) the magnetometer dip angle through time. Each panel
# carries an "Expected:" guide-rail caption, and body axes are labelled surge (X) / sway (Y) /
# heave (Z). The series are subsampled / envelope-decimated / Welch-averaged so the PDF stays small.

# Map an algorithm verdict to a panel colour + title prefix (pass/fail/warn; "skip" = no border).
#' @keywords internal
#' @noRd
.panelStatus <- function(state, use_unicode = TRUE) {
  switch(state,
    pass = list(col = "green4",     pre = if (use_unicode) "[\u2714] " else "[v] "),
    fail = list(col = "red3",       pre = if (use_unicode) "[\u2716] " else "[x] "),
    warn = list(col = "darkorange", pre = "[!] "),
    list(col = NA, pre = ""))   # skip / absent -> no border, no prefix
}

#' @keywords internal
#' @noRd
.plotTagMappingPanel <- function(pd, use_unicode = TRUE) {
  oldpar <- graphics::par(no.readonly = TRUE); on.exit(graphics::par(oldpar), add = TRUE)
  # extra bottom margin for the guide-rail caption; extra right margin for the depth axis in panel 2
  graphics::par(mfrow = c(2, 2), mar = c(4.4, 4, 2.6, 3), mgp = c(2.2, 0.7, 0), oma = c(0, 0, 2.4, 0))
  fl <- pd$flags
  drawbox <- function(st) if (!is.na(st$col)) graphics::box(col = st$col, lwd = 2)

  # 1. surge anchor: pitch vs depth-rate (pass when surge resolved). Pre-thinned scatter; r on all.
  # When the surge frame is provisional (vertical not yet pinned), the correlation can look strong but
  # was NOT used to resolve - say so, so the plotted r never contradicts the red border.
  st <- .panelStatus(if (isTRUE(fl$surge_resolved)) "pass" else "fail", use_unicode)
  sa_sub <- if (!isTRUE(fl$surge_resolved) && !isTRUE(fl$vertical_resolved)) " (provisional frame: vertical unresolved)" else ""
  if (!is.null(pd$sa) && pd$sa$n > 5) {
    graphics::plot(pd$sa$pitch, pd$sa$vs, pch = 16, cex = 0.3, col = "#3366aa55",
                   xlab = "Body pitch (deg)", ylab = "Vertical speed (m/s)",
                   main = paste0(st$pre, "Surge anchor: pitch vs depth-rate", sa_sub),
                   cex.main = 0.95, cex.axis = 0.8, cex.lab = 0.9, las = 1)
    graphics::abline(h = 0, col = "grey70", lty = 3)
    graphics::legend("topright", legend = sprintf("r = %.2f", pd$sa$r), bty = "n", cex = 0.9)
    .panelHint("Expected: Tight negative correlation (animal pitches nose-down to descend)")
    drawbox(st)
  } else { graphics::plot.new(); graphics::title("Surge anchor: insufficient diving signal", cex.main = 0.95) }

  # 2. gravity partition over the strongest dive, with the depth profile as a soft filled backdrop on
  # a secondary (inverted) axis (pass when vertical resolved). Upper ylim padded so the legend clears.
  st <- .panelStatus(if (isTRUE(fl$vertical_resolved)) "pass" else "fail", use_unicode)
  gp <- pd$gp; tt <- gp$t; dep <- gp$depth
  if (length(tt) >= 2) {
    yl <- range(c(gp$surge, gp$sway, gp$heave), na.rm = TRUE); yl[2] <- yl[2] + 0.18 * diff(yl)
    if (!is.null(dep) && any(is.finite(dep))) {
      dr <- range(dep, na.rm = TRUE); if (diff(dr) < 1e-6) dr <- dr + c(-0.5, 0.5)
      graphics::plot(tt, dep, type = "n", axes = FALSE, ann = FALSE, ylim = c(dr[2], dr[1]))  # surface at top
      fin <- is.finite(dep)
      graphics::polygon(c(tt[fin][1], tt[fin], tt[fin][sum(fin)]), c(dr[1], dep[fin], dr[1]),
                        col = "#9ecae133", border = NA)                  # water column above the animal
      graphics::axis(4, col = "#4a78a8", col.axis = "#4a78a8", cex.axis = 0.75)
      graphics::mtext("Depth (m)", side = 4, line = 1.8, cex = 0.7, col = "#4a78a8")
      graphics::par(new = TRUE)
    }
    graphics::plot(tt, gp$heave, type = "l", col = "black", lwd = 1.2, ylim = yl,
                   xlab = "Time (s)", ylab = "Static accel (g)",
                   main = paste0(st$pre, "Gravity partition over a dive"),
                   cex.main = 0.95, cex.axis = 0.8, cex.lab = 0.9, las = 1)
    graphics::lines(tt, gp$surge, col = "#cc3333", lwd = 1.2)
    graphics::lines(tt, gp$sway,  col = "#3399cc", lwd = 1.2)
    graphics::legend("topright", legend = c("heave (Z)", "surge (X)", "sway (Y)"),
                     col = c("black", "#cc3333", "#3399cc"), lwd = 1.2, bty = "n", cex = 0.8, horiz = TRUE)
    .panelHint("Expected: Gravity shifts from Heave (Z) to Surge (X) during a dive; Sway (Y) stays near 0g.")
    drawbox(st)
  } else { graphics::plot.new(); graphics::title("Gravity partition: insufficient data", cex.main = 0.95) }

  # 3. per-axis dynamic spectra (the locomotor axis should carry the tail beat). Welch-averaged.
  st <- .panelStatus(if (isTRUE(fl$tb_corroborated)) "pass" else "warn", use_unicode)
  loco_title <- sprintf("%s (%s)", tools::toTitleCase(pd$loco.axis), pd$loco_role)
  loco_note  <- c(sway = "tail beats are lateral", heave = "dorso-ventral oscillation",
                  surge = "fore-aft oscillation")[[pd$loco.axis]]
  if (!is.null(pd$sp)) {
    sp <- pd$sp; cols <- c(surge = "#cc3333", sway = "#3399cc", heave = "grey40")
    lwds <- c(surge = 1, sway = 1, heave = 1); lwds[pd$loco.axis] <- 2.4   # emphasise the locomotor axis
    band_mask <- sp$freq >= pd$locomotor.band[1] & sp$freq <= pd$locomotor.band[2] * 1.6
    ymax <- max(c(sp$surge[band_mask], sp$sway[band_mask], sp$heave[band_mask]), na.rm = TRUE) * 1.1
    if (!is.finite(ymax) || ymax <= 0) ymax <- max(c(sp$surge, sp$sway, sp$heave), na.rm = TRUE)
    graphics::plot(sp$freq, sp$surge, type = "l", col = cols[["surge"]], lwd = lwds[["surge"]], ylim = c(0, ymax),
                   xlab = "Frequency (Hz)", ylab = "Power",
                   main = paste0(st$pre, "Per-axis spectra (", loco_title, " = tail beat)"),
                   cex.main = 0.95, cex.axis = 0.8, cex.lab = 0.9, las = 1)
    graphics::rect(pd$locomotor.band[1], 0, pd$locomotor.band[2], ymax, col = "#00000010", border = NA)
    graphics::lines(sp$freq, sp$sway,  col = cols[["sway"]],  lwd = lwds[["sway"]])
    graphics::lines(sp$freq, sp$heave, col = cols[["heave"]], lwd = lwds[["heave"]])
    graphics::legend("topright", legend = c("surge (X)", "sway (Y)", "heave (Z)"), col = cols,
                     lwd = lwds[c("surge", "sway", "heave")], bty = "n", cex = 0.8)
    .panelHint(sprintf("Expected: Tallest peak on %s in the shaded locomotor band (%s).", loco_title, loco_note))
    drawbox(st)
  } else { graphics::plot.new(); graphics::title("Per-axis spectra: too short", cex.main = 0.95) }

  # 4. magnetometer dip angle over time (pass = resolved, fail = unresolved/inconsistent, no border = absent)
  ms <- fl$mag_status %||% "absent/insufficient"
  st <- .panelStatus(if (identical(ms, "resolved")) "pass"
                     else if (ms %in% c("unresolved", "inconsistent")) "fail" else "skip", use_unicode)
  if (!is.null(pd$md)) {
    md <- pd$md
    graphics::plot(md$t, md$angle, type = "l", col = "#996600", xlab = "Time (min)",
                   ylab = "angle(g, mag) (deg)", main = paste0(st$pre, "Magnetometer dip"),
                   cex.main = 0.95, cex.axis = 0.8, cex.lab = 0.9, las = 1)
    graphics::abline(h = md$mean, col = "grey60", lty = 3)
    if (is.finite(md$expected)) {
      graphics::abline(h = md$expected, col = "red2", lty = 2)
      graphics::legend("topright", legend = "IGRF expected", col = "red2", lty = 2, bty = "n", cex = 0.75)
    }
    .panelHint("Expected: Flat line near the dashed IGRF reference. Wild swings indicate an unresolved frame.")
    drawbox(st)
  } else { graphics::plot.new(); graphics::title("Magnetometer: absent", cex.main = 0.95) }

  # overall title with the per-axis resolution summary
  res <- pd$resolution
  summ <- paste(sprintf("%s:%s", res$body_axis, substr(res$status, 1, 3)), collapse = "  ")
  graphics::mtext(sprintf("%s   -   axis mapping   [%s]", pd$id, summ), outer = TRUE, font = 2, cex = 1)
  invisible(NULL)
}

################################################################################
# Internal function: run-summary page (page 1 of the diagnostic PDF) ###########
################################################################################

# A text/table page: aggregate run stats + a per-tag table sorted worst-first (failed, then unresolved,
# partial, resolved) so the tags needing manual review are at the top. Auto-paginated.
#' @keywords internal
#' @noRd
.drawTagMappingSummaryPage <- function(records, stats, use_unicode = TRUE) {
  oldpar <- graphics::par(no.readonly = TRUE); on.exit(graphics::par(oldpar), add = TRUE)
  records <- Filter(Negate(is.null), records)
  rankv <- c(failed = 0, skipped = 1, unresolved = 2, partial = 3, resolved = 4)
  if (length(records)) {
    ord <- order(vapply(records, function(r) rankv[[r$status]], numeric(1)),
                 vapply(records, function(r) { v <- r$n_resolved; if (is.null(v) || is.na(v)) -1 else v }, numeric(1)))
    records <- records[ord]
  }

  # ---- glyphs (with the established ASCII fallback for fonts lacking the marks) -------------------
  g_tick <- if (use_unicode) "\u2713" else "v"
  g_cross <- if (use_unicode) "\u2717" else "x"
  g_dash <- if (use_unicode) "\u2013" else "-"
  ell <- if (use_unicode) "\u2026" else ".."
  deg <- if (use_unicode) "\u00b0" else " deg"
  dot <- if (use_unicode) "\u00b7" else "|"

  # ---- small formatting helpers ------------------------------------------------------------------
  nz  <- function(s) !is.null(s) && length(s) == 1 && !is.na(s) && nzchar(as.character(s))
  truncr <- function(s, n) { if (!nz(s)) return("-"); s <- as.character(s); if (nchar(s) > n) paste0(substr(s, 1, n - 1), ell) else s }
  truncl <- function(s, n) { if (!nz(s)) return("-"); s <- as.character(s); if (nchar(s) > n) paste0(ell, substr(s, nchar(s) - n + 2, nchar(s))) else s }
  model_pkg <- function(r) { if (!nz(r$model) && !nz(r$package_id)) return("-")
    paste0(truncr(r$model, 7), "/", truncr(r$package_id, 8)) }
  site_fmt <- function(s) { if (!nz(s)) return("-")
    if (grepl("left",  s, ignore.case = TRUE) && grepl("pect", s, ignore.case = TRUE)) return("L pect")
    if (grepl("right", s, ignore.case = TRUE) && grepl("pect", s, ignore.case = TRUE)) return("R pect")
    truncr(s, 7) }
  gyro_word <- function(s) if (!nz(s)) "-" else switch(s, resolved = "resolved", unresolved = "unresolved",
                                                       "absent/insufficient" = "absent", s)
  mag_word  <- function(s) if (!nz(s)) "-" else switch(s, resolved = "consistent", inconsistent = "inconsistent",
                                                       unresolved = "unresolved", "absent/insufficient" = "absent", s)
  word_col  <- function(s) if (!nz(s)) "grey60" else switch(s, resolved = "green4", inconsistent = "red3",
                                                            unresolved = "darkorange", "absent/insufficient" = "grey55", "grey30")
  remap_col <- function(s) switch(s %||% "None", Confirmed = "green4", Proposed = "darkorange", "grey55")
  id_col    <- function(s) switch(s, resolved = "grey10", partial = "darkorange", unresolved = "red3",
                                  failed = "red3", skipped = "grey55", "grey10")
  # numeric cells -> list(text, colour, font); bold-red when outside healthy parameters
  f_static <- function(s) { if (!nz(s)) return(list("-", "grey60", 1L)); s <- as.numeric(s)
    txt <- if (s < 600) sprintf("%.0fs", s) else sprintf("%.0fm", s / 60)
    if (s < 30) list(txt, "red3", 2L) else if (s < 60) list(txt, "darkorange", 1L) else list(txt, "grey20", 1L) }
  f_dive <- function(p) { if (!nz(p)) return(list("-", "grey60", 1L)); p <- as.numeric(p)
    txt <- sprintf("%.0f%%", p)
    if (p < 5) list(txt, "red3", 2L) else if (p < 10) list(txt, "darkorange", 1L) else list(txt, "grey20", 1L) }
  f_surge <- function(r) { if (!nz(r)) return(list("-", "grey60", 1L)); r <- as.numeric(r)
    if (abs(r) < 0.4) list(sprintf("%+.2f", r), "red3", 2L) else list(sprintf("%+.2f", r), "grey20", 1L) }
  f_magd <- function(d) { if (!nz(d)) return(list("-", "grey60", 1L)); d <- as.numeric(d)
    txt <- sprintf("%+.0f%s", d, deg)
    if (abs(d) > 10) list(txt, "red3", 2L) else list(txt, "grey20", 1L) }
  cell <- function(x, y, v, cx_) graphics::text(x, y, v[[1]], adj = c(0, 0.5), cex = cx_, col = v[[2]], font = v[[3]])

  # ---- column anchors (normalized x; tuned for the 10 in landscape canvas) ------------------------
  cx <- c(id = 0.004, model = 0.118, ax = 0.248, ay = 0.270, az = 0.292, remap = 0.318,
          gyro = 0.420, mag = 0.520, tail = 0.638, site = 0.716, static = 0.798,
          dive = 0.850, surge = 0.898, magd = 0.958)
  hdr_lab <- c(id = "Tag ID", model = "Model / Pkg", ax = "X", ay = "Y", az = "Z", remap = "Remapping",
               gyro = "Gyro", mag = "Mag", tail = "Tail-Beat", site = "Site", static = "Static",
               dive = "Dive", surge = "Surge r", magd = if (use_unicode) "Mag \u0394" else "Mag d")
  cex_h <- 0.62; cex_d <- 0.58

  rpp <- 29L
  npg <- max(1L, ceiling(max(1L, length(records)) / rpp))
  for (pg in seq_len(npg)) {
    idx <- if (length(records) == 0) integer(0) else ((pg - 1L) * rpp + 1L):min(pg * rpp, length(records))
    graphics::par(mar = c(1.5, 0.6, 1, 0.6)); graphics::plot.new(); graphics::plot.window(c(0, 1), c(0, 1))

    graphics::text(0, 0.985, "checkTagMapping  -  run summary", adj = c(0, 1), font = 2, cex = 1.4)
    graphics::text(1, 0.985, format(Sys.time(), "%Y-%m-%d %H:%M"), adj = c(1, 1), cex = 0.75, col = "grey40")
    agg <- sprintf("%d tags  |  resolved %d   partial %d   unresolved %d%s%s",
                   stats$n_animals, stats$n_full, stats$n_part, stats$n_unres,
                   if (stats$n_skip > 0) sprintf("   skipped %d", stats$n_skip) else "",
                   if (stats$n_fail > 0) sprintf("   failed %d", stats$n_fail) else "")
    graphics::text(0, 0.93, agg, adj = c(0, 1), cex = 0.95)
    graphics::text(0, 0.90, sprintf("locomotor axis: %s   -   sorted worst-first for review", stats$locomotor.axis),
                   adj = c(0, 1), cex = 0.72, col = "grey45")

    # header band + bold header + rule
    hcy <- 0.855; dy <- 0.0265
    graphics::rect(0, hcy - 0.016, 1, hcy + 0.016, col = "grey90", border = NA)
    for (k in names(cx)) {
      ax_axis <- k %in% c("ax", "ay", "az")
      graphics::text(cx[[k]], hcy, hdr_lab[[k]], adj = if (ax_axis) c(0.5, 0.5) else c(0, 0.5),
                     font = 2, cex = cex_h, col = "grey15")
    }
    graphics::segments(0, hcy - 0.018, 1, hcy - 0.018, col = "grey55")

    row_top <- hcy - 0.040
    for (j in seq_along(idx)) {
      r <- records[[idx[j]]]; y <- row_top - dy * (j - 1L)
      if (j %% 2L == 0L) graphics::rect(0, y - dy / 2, 1, y + dy / 2, col = "grey96", border = NA)  # zebra
      graphics::text(cx[["id"]], y, truncl(r$id, 13), adj = c(0, 0.5), cex = cex_d, font = 2, col = id_col(r$status))
      graphics::text(cx[["model"]], y, model_pkg(r), adj = c(0, 0.5), cex = cex_d, col = "grey30")

      # failed / skipped: no per-axis data -> a single spanning status note
      if (r$status %in% c("failed", "skipped")) {
        msg <- r$message %||% r$status
        graphics::text(cx[["ax"]], y, paste0(r$status, " ", g_dash, " ", truncr(msg, 70)),
                       adj = c(0, 0.5), cex = cex_d, font = 3, col = id_col(r$status))
        next
      }

      # per-axis X / Y / Z verdict
      for (a in c("ax", "ay", "az")) {
        ax_name <- toupper(substr(a, 2, 2))
        ok <- isTRUE(r$axes[[ax_name]])
        graphics::text(cx[[a]], y, if (ok) g_tick else g_cross, adj = c(0.5, 0.5), cex = cex_d,
                       font = 2, col = if (ok) "green4" else "red3")
      }
      graphics::text(cx[["remap"]], y, r$remap %||% "None", adj = c(0, 0.5), cex = cex_d, col = remap_col(r$remap))
      graphics::text(cx[["gyro"]], y, gyro_word(r$gyro), adj = c(0, 0.5), cex = cex_d, col = word_col(r$gyro))
      graphics::text(cx[["mag"]],  y, mag_word(r$mag),   adj = c(0, 0.5), cex = cex_d, col = word_col(r$mag))
      tb <- r$tailbeat
      graphics::text(cx[["tail"]], y, if (!nz(tb)) g_dash else if (isTRUE(tb)) g_tick else g_cross,
                     adj = c(0.5, 0.5), cex = cex_d, font = 2,
                     col = if (isTRUE(tb)) "green4" else if (identical(tb, FALSE)) "grey55" else "grey60")
      graphics::text(cx[["site"]], y, site_fmt(r$site), adj = c(0, 0.5), cex = cex_d, col = "grey30")
      cell(cx[["static"]], y, f_static(r$static_secs), cex_d)
      cell(cx[["dive"]],   y, f_dive(r$diving_pct),    cex_d)
      cell(cx[["surge"]],  y, f_surge(r$surge_corr),   cex_d)
      cell(cx[["magd"]],   y, f_magd(r$mag_resid),     cex_d)
    }

    # footer legend + pagination
    graphics::text(0, 0.018, sprintf("bold red = outside healthy range:  static <60s  %s  dive <10%%  %s  |surge r| <0.4  %s  mag residual >10%s", dot, dot, dot, deg),
                   adj = c(0, 0), cex = 0.62, col = "grey45")
    if (npg > 1) graphics::mtext(sprintf("summary page %d / %d", pg, npg), side = 1, cex = 0.7, col = "grey50")
  }
  invisible(NULL)
}

################################################################################
# Helper function for padded rolling sum #######################################
################################################################################

.pad_rollsum <- function(x, window, pad_len) {
  if (length(x) == 0) return(numeric(0))
  if (length(x) < window) {
    # If data is shorter than window, return NAs for the entire sequence
    return(rep(NA_real_, length(x)))
  }

  # Symmetric padding using first/last values
  padded <- c(rep(x[1], pad_len), x, rep(x[length(x)], pad_len))
  # Compute centered rolling sum on padded data
  rolled <- data.table::frollsum(padded, n = window, align = "center", na.rm = FALSE) # na.rm=FALSE if sum should be NA if any in window is NA
  # Trim to original length
  return(rolled[(pad_len + 1):(pad_len + length(x))])
}

#######################################################################################################
#######################################################################################################
#######################################################################################################

# consensusAxisMapping helpers ----

#######################################################################################################
# Internal helpers ####################################################################################
#######################################################################################################

#' Recursively collect checkTagMapping deployment nodes from a (possibly nested) results object.
#'
#' A node is any list element carrying a \code{$proposal} data.frame (a checkTagMapping result). The
#' node's \code{id} is taken from \code{$id}, else the enclosing list name. The grouping keys
#' (\code{package_id}, \code{tag}, \code{type}) are carried through (NA if absent), along with
#' \code{candidates} / \code{resolution}.
#' @keywords internal
#' @noRd
.collectMappingNodes <- function(x, nm = NA_character_) {
  if (is.null(x) || !is.list(x) || is.data.frame(x)) return(list())
  if (!is.null(x$proposal) && is.data.frame(x$proposal)) {
    scal <- function(v) { v <- v %||% NA_character_; if (length(v) != 1) NA_character_ else as.character(v) }
    return(list(list(
      id         = as.character(x$id %||% nm %||% NA_character_),
      package_id = scal(x$package_id),
      logger_id  = scal(x$logger_id),
      tag        = scal(x$tag),
      type       = scal(x$type),
      proposal   = x$proposal,
      candidates = x$candidates,
      resolution = x$resolution)))
  }
  nms <- names(x); if (is.null(nms)) nms <- rep(NA_character_, length(x))
  out <- list()
  for (i in seq_along(x)) out <- c(out, .collectMappingNodes(x[[i]], nms[i]))
  out
}

#' Pool a flat list of proposal data.frames into a per-family consensus signed permutation.
#'
#' For each sensor family (accel/gyro/mag) it tallies the signed-permutation matrices implied by the
#' proposals that carry a complete triplet for that family, and returns the modal matrix with its
#' support. This is the pure pooler underneath \code{consensusAxisMapping()} (no grouping, no
#' propagation), kept separate so it can be unit-tested in isolation.
#' @param proposals A list of \code{from}/\code{to} data.frames (one per deployment).
#' @param min.agreement Fraction at / above which a family's modal mapping is emitted as \code{mapping}.
#' @return A named list keyed by family letter ("a"/"g"/"m"); each element carries \code{n} (voters),
#'   \code{agreement}, \code{n_distinct}, \code{counts} (table of solutions), \code{M} (modal matrix),
#'   and \code{mapping} (from/to of the modal matrix, or NULL below \code{min.agreement}).
#' @keywords internal
#' @noRd
.poolAxisMappings <- function(proposals, min.agreement = 0.6) {
  fam_letters <- c("a", "g", "m")
  axis_names  <- list(a = c("ax", "ay", "az"), g = c("gx", "gy", "gz"), m = c("mx", "my", "mz"))
  fam_of <- function(axis) substr(sub("^-", "", axis), 1, 1)

  per_family <- list()
  for (f in fam_letters) {
    mats <- list(); keys <- character(0)
    for (p in proposals) {
      if (is.null(p) || !is.data.frame(p) || !nrow(p)) next
      fam_rows <- p[fam_of(p$to) == f, , drop = FALSE]
      if (nrow(fam_rows) != 3L) next
      M <- .mappingToSignedPerm(fam_rows, axis_names[[f]])
      if (is.null(M)) next
      key <- paste(M, collapse = ",")
      keys <- c(keys, key); mats[[key]] <- M
    }
    if (!length(keys)) next
    tab  <- sort(table(keys), decreasing = TRUE)
    top  <- names(tab)[1]
    frac <- as.numeric(tab[1]) / length(keys)
    per_family[[f]] <- list(
      n = length(keys), agreement = frac, n_distinct = length(tab), counts = tab,
      M = mats[[top]],
      mapping = if (frac >= min.agreement) .matrixToFromTo(mats[[top]], axis_names[[f]], axis_names[[f]]) else NULL)
  }
  per_family
}

#' The signed-permutation matrix implied by one family's rows in a proposal, or NULL if not a full triplet.
#' @keywords internal
#' @noRd
.familyMatrixFromProposal <- function(proposal, f, axes) {
  rows <- .familyRowsFromProposal(proposal, f)
  if (is.null(rows) || nrow(rows) != 3L) return(NULL)
  .mappingToSignedPerm(rows, axes)
}

#' One family's from/to rows from a proposal data.frame (empty data.frame if none).
#' @keywords internal
#' @noRd
.familyRowsFromProposal <- function(proposal, f) {
  if (is.null(proposal) || !is.data.frame(proposal) || !nrow(proposal)) {
    return(data.frame(from = character(0), to = character(0), stringsAsFactors = FALSE))
  }
  proposal[substr(sub("^-", "", proposal$to), 1, 1) == f, c("from", "to"), drop = FALSE]
}

#' The signed-permutation matrices of a checkTagMapping $candidates table (the accel tied set).
#' @keywords internal
#' @noRd
.candidateMatrices <- function(candidates) {
  if (is.null(candidates) || !nrow(candidates)) return(list())
  needed <- c("newX_col", "newX_sign", "newY_col", "newY_sign", "newZ_col", "newZ_sign")
  if (!all(needed %in% names(candidates))) return(list())
  axes <- c("ax", "ay", "az")
  sgn  <- function(s, ax) paste0(if (isTRUE(s < 0)) "-" else "", ax)
  out <- list()
  for (i in seq_len(nrow(candidates))) {
    r  <- candidates[i, ]
    ft <- data.frame(
      from = c(as.character(r$newX_col), as.character(r$newY_col), as.character(r$newZ_col)),
      to   = c(sgn(r$newX_sign, "ax"), sgn(r$newY_sign, "ay"), sgn(r$newZ_sign, "az")),
      stringsAsFactors = FALSE)
    M <- .mappingToSignedPerm(ft, axes)
    if (!is.null(M)) out[[length(out) + 1L]] <- M
  }
  out
}

#' Equality of two signed-permutation matrices (NULL-safe).
#' @keywords internal
#' @noRd
.matEq <- function(a, b) {
  !is.null(a) && !is.null(b) && all(dim(a) == dim(b)) && all(a == b)
}

#' One warning for every axis-mapping conflict found in a run.
#'
#' A conflict means two confidently-resolved deployments sharing a unit disagree about how its axes map,
#' which is a hardware question (a swapped or remounted board, or two units filed under one id) and not
#' something the reconciliation can settle. It therefore warns regardless of verbosity.
#'
#' Emitted ONCE, not per group and per sensor family. A cohort where one unit conflicts on accelerometer
#' and gyroscope produced two near-identical warnings for the same physical cause; on a large run that
#' becomes a wall of text, and R keeps only the first 50 warnings, so the tail is dropped in silence.
#' Grouping by unit also puts the useful comparison - which families disagree for THIS unit - on one line.
#' @param conflicts Named list, one entry per group label, each a list of
#'   `list(family, n_distinct, ids)` records.
#' @param gb_desc How the grouping fields read in prose, e.g. `"package_id x logger_id"`.
#' @keywords internal
#' @noRd
.warnAxisConflicts <- function(conflicts, gb_desc) {
  if (!length(conflicts)) return(invisible(NULL))
  cap <- function(x, n = 10L) if (length(x) > n) c(utils::head(x, n), sprintf("(+%d more)", length(x) - n)) else x

  lines <- vapply(names(conflicts), function(g) {
    recs <- conflicts[[g]]
    fams <- paste(vapply(recs, function(r) sprintf("%s (%d solutions)", r$family, r$n_distinct), ""),
                  collapse = ", ")
    ids  <- unique(unlist(lapply(recs, `[[`, "ids"), use.names = FALSE))
    sprintf("%s: %s -- %s", g, fams, paste(cap(ids, 6L), collapse = ", "))
  }, character(1))

  # one bullet PER unit: passing the vector to a single bullet would let cli collapse them into an
  # English list ("A and B"), which is unreadable once more than two units conflict. Braces are escaped
  # because each line is a glue template to cli and an id containing one would abort the warning.
  esc   <- function(x) gsub("}", "}}", gsub("{", "{{", x, fixed = TRUE), fixed = TRUE)
  bul   <- stats::setNames(esc(cap(lines)), rep("*", length(cap(lines))))

  cli::cli_warn(c(
    # cli takes the pluralising quantity from the LAST substitution, which is {gb_desc} (length 1) -
    # so the count is pinned explicitly with qty() immediately before each plural marker
    "Conflicting axis mappings across confidently-resolved deployments, for {length(conflicts)} {gb_desc} combination{cli::qty(length(conflicts))}{?s}. No consensus was propagated for {cli::qty(length(conflicts))}{?it/them}.",
    bul,
    "i" = "Check whether the circuit board was swapped or remounted inside the housing between deployments, or whether two physical units share one {gb_desc}.",
    "i" = "Each deployment keeps whatever mapping it resolved on its own; only the group consensus is withheld."))
  invisible(NULL)
}

# findValidationSegments helpers ----

################################################################################
# Internal helpers #############################################################
################################################################################

#' Empty segment table with the canonical schema.
#' @keywords internal
#' @noRd
.emptySegments <- function() {
  data.frame(id = character(0), type = character(0), rank = integer(0),
             start = as.POSIXct(character(0)), end = as.POSIXct(character(0)),
             peak_time = as.POSIXct(character(0)), value = numeric(0), unit = character(0),
             stringsAsFactors = FALSE)
}

#' Which requested event types have the columns they need in `dt`.
#' @keywords internal
#' @noRd
.availableTypes <- function(dt) {
  nm <- names(dt)
  c(if ("turning_angle" %in% nm || "heading" %in% nm) "turn",
    if ("roll" %in% nm) "roll",
    if ("vertical_velocity" %in% nm) "dive")
}

#' Candidate segments for a single deployment.
#' @keywords internal
#' @noRd
.findSegmentsForTag <- function(dt, id, types, n, window, datetime.col) {
  t <- dt[[datetime.col]]
  fs <- .tagFs(dt, datetime.col)
  if (!is.finite(fs) || fs <= 0) return(.emptySegments())
  w <- max(1L, as.integer(round(window * fs)))

  # per-type event signal (a clear manoeuvre = a large SUSTAINED net signal over the window)
  signal_of <- function(type) {
    if (type == "turn") {
      if ("turning_angle" %in% names(dt)) dt[["turning_angle"]]
      else { h <- dt[["heading"]]; c(NA_real_, ((diff(h) + 180) %% 360) - 180) }  # per-step heading change
    } else if (type == "roll") {
      r <- dt[["roll"]]; r - stats::median(r, na.rm = TRUE)                        # deviation from resting roll
    } else if (type == "dive") {
      dt[["vertical_velocity"]]
    }
  }

  segs <- lapply(intersect(types, .availableTypes(dt)), function(type) {
    sig <- signal_of(type)
    intensity <- abs(data.table::frollmean(sig, n = w, align = "center", na.rm = TRUE))
    picks <- .greedyPeaks(intensity, t, n, window)                                 # non-overlapping top peaks
    if (!length(picks)) return(NULL)
    do.call(rbind, lapply(seq_along(picks), function(r) {
      pk <- picks[r]
      win <- which(t >= t[pk] - window / 2 & t <= t[pk] + window / 2)
      v <- .segmentValue(dt, type, win)
      data.frame(id = id, type = type, rank = r,
                 start = t[pk] - window / 2, end = t[pk] + window / 2, peak_time = t[pk],
                 value = round(v$value, 1), unit = v$unit, stringsAsFactors = FALSE)
    }))
  })
  segs <- do.call(rbind, segs)
  if (is.null(segs)) .emptySegments() else segs
}

#' Greedily pick the top `n` peaks of `intensity` that are >= `window` seconds apart.
#' @keywords internal
#' @noRd
.greedyPeaks <- function(intensity, t, n, window) {
  ord <- order(intensity, decreasing = TRUE, na.last = NA)                         # drops NA
  picked <- integer(0)
  tn <- as.numeric(t)
  for (i in ord) {
    if (!length(picked) || all(abs(tn[i] - tn[picked]) >= window)) {
      picked <- c(picked, i)
      if (length(picked) >= n) break
    }
  }
  picked
}

#' Interpretable magnitude + unit for a segment of a given type.
#' @keywords internal
#' @noRd
.segmentValue <- function(dt, type, win) {
  if (type == "turn") {
    ta <- if ("turning_angle" %in% names(dt)) dt[["turning_angle"]][win]
          else { h <- dt[["heading"]][win]; c(0, ((diff(h) + 180) %% 360) - 180) }
    list(value = abs(sum(ta, na.rm = TRUE)), unit = "deg")                          # net heading change
  } else if (type == "roll") {
    list(value = max(abs(dt[["roll"]][win]), na.rm = TRUE), unit = "deg")           # peak |roll|
  } else if (type == "dive") {
    if ("depth" %in% names(dt)) {
      d <- dt[["depth"]][win]; d <- d[is.finite(d)]
      list(value = if (length(d)) abs(d[length(d)] - d[1]) else NA_real_, unit = "m")  # net depth change
    } else {
      list(value = max(abs(dt[["vertical_velocity"]][win]), na.rm = TRUE), unit = "m/s")
    }
  }
}


#######################################################################################################
#######################################################################################################
#######################################################################################################

# reviewTagMapping helpers ----

################################################################################
# Triage (pure) ################################################################
################################################################################

#' Build the review queue: one row per flagged deployment (id, review_reason, priority), sorted.
#' @keywords internal
#' @noRd
.triageMappings <- function(mapping, data.ids, video.ids, include, ids.requested) {
  prio_of <- c(user_requested = 0L, conflict = 1L, unanalysed = 1L, coreg_fail = 2L,
               ambiguous = 3L, gyro_inconsistent = 4L, low_confidence = 5L)

  rows <- list()
  if (!is.null(ids.requested)) {
    # explicit request: review exactly these (a suspect reason wins over the generic label)
    for (id in as.character(ids.requested)) {
      rsn <- if (id %in% names(mapping)) .reviewReason(mapping[[id]], include) else NA_character_
      if (is.na(rsn)) rsn <- "user_requested"
      rows[[length(rows) + 1L]] <- data.frame(id = id, review_reason = rsn, stringsAsFactors = FALSE)
    }
  } else {
    for (id in names(mapping)) {
      rsn <- .reviewReason(mapping[[id]], include)
      if (!is.na(rsn)) rows[[length(rows) + 1L]] <- data.frame(id = id, review_reason = rsn, stringsAsFactors = FALSE)
    }
    # unanalysed: present in the data AND in the footage, but the automated check produced no result
    unanalysed <- setdiff(intersect(data.ids, video.ids), names(mapping))
    for (id in unanalysed)
      rows[[length(rows) + 1L]] <- data.frame(id = id, review_reason = "unanalysed", stringsAsFactors = FALSE)
  }
  if (!length(rows)) return(.emptyQueue())

  q <- do.call(rbind, rows)
  q$priority <- unname(prio_of[q$review_reason])
  q <- q[order(q$priority, q$id), , drop = FALSE]
  rownames(q) <- NULL
  q
}

#' The single most-severe in-scope review reason for one deployment, or NA.
#' @keywords internal
#' @noRd
.reviewReason <- function(elem, include) {
  fs <- elem$frame_state; fam <- elem$families
  if (is.null(fs)) return(NA_character_)
  cand <- character(0)
  if (length(fs$conflicts) > 0) cand <- c(cand, "conflict")
  # gyro/accel co-registration failure (the co-die default was decisively rejected by the data); a
  # benign reflection is NOT flagged (it co-registers and is handled automatically).
  if (identical(fs$coreg$status, "fail")) cand <- c(cand, "coreg_fail")
  if (!is.null(fs$survivors) && nrow(fs$survivors) > 1 &&
      isTRUE(fs$prior$status %in% c("absent", "unverifiable", "consistent"))) cand <- c(cand, "ambiguous")
  # the two fallback gyro estimators disagree. This only arises once the co-die default has already
  # FAILED co-registration, so such an element also carries `coreg_fail` (higher priority, which wins
  # under the default `include`); `gyro_inconsistent` is surfaced only when `coreg_fail` is excluded.
  if (identical(fam$gyro$status, "inconsistent")) cand <- c(cand, "gyro_inconsistent")
  if (identical(fs$vertical$status, "resolved") && identical(fs$surge$status, "ambiguous")) cand <- c(cand, "low_confidence")
  cand <- intersect(c("conflict", "coreg_fail", "ambiguous", "gyro_inconsistent", "low_confidence"),
                    intersect(cand, include))
  if (length(cand)) cand[1] else NA_character_
}

################################################################################
# Footage matching (pure) ######################################################
################################################################################

#' The clip (row of `video.metadata`) covering `peak_time` for deployment `id`, or NULL.
#' @keywords internal
#' @noRd
.matchClip <- function(video.metadata, id, peak_time) {
  cand <- video.metadata[as.character(video.metadata$ID) == id, , drop = FALSE]
  if (!nrow(cand)) return(NULL)
  hit <- cand[peak_time >= cand$start & peak_time <= cand$end, , drop = FALSE]
  if (!nrow(hit)) return(NULL)
  hit[1, , drop = FALSE]
}

################################################################################
# Per-deployment review (heavy: transform + orient + render) ###################
################################################################################

#' Review a single deployment end to end: resolve candidates, render clips, return manifest rows.
#' @keywords internal
#' @noRd
.reviewOneDeployment <- function(id, reason, priority, r, mapping, video.metadata, configs, output.dir,
                                 types, n, window, clips.per.deployment, max.candidates, side, overlay.fps, codec,
                                 id.col, datetime.col) {
  # footage FIRST (cheap): skip the expensive load + orientation for deployments with no video at all
  vmi <- video.metadata[as.character(video.metadata$ID) == id, , drop = FALSE]
  if (!nrow(vmi)) return(.reviewOut(.manifestRow(id, reason, priority, .mappingSourceFor(reason), status = "no_video")))

  i <- match(id, as.character(r$ids))
  if (is.na(i)) return(.reviewOut(.manifestRow(id, reason, priority, .mappingSourceFor(reason), status = "no_video")))
  raw <- r$get(i)
  if (!data.table::is.data.table(raw)) raw <- data.table::as.data.table(raw)
  axis_config <- tryCatch(.getMeta(raw)$tag$axis_config, error = function(e) NULL)   # for the "Documented" candidate
  fs <- if (id %in% names(mapping)) mapping[[id]]$frame_state else NULL

  # restrict to the footage-covered window (+ padding for orientation context) BEFORE orienting: only these
  # moments are reviewable, and orienting just this window rather than the whole multi-hour record is the
  # main speed-up.
  pad <- window + 5
  raw <- raw[.inAnyInterval(raw[[datetime.col]], vmi$start - pad, vmi$end + pad), ]
  if (!nrow(raw)) return(.reviewOut(.manifestRow(id, reason, priority, .mappingSourceFor(reason), status = "no_coverage")))

  # ---- resolve the candidate mapping(s) and the dashboard ----
  plan  <- .candidatePlan(reason, fs, configs, max.candidates)
  cands <- .reviewApplyCandidates(plan, id, mapping, axis_config, configs)   # concrete from/to per candidate (sheet + apply)
  fs_data <- raw                                      # the data used to FIND segments + RENDER

  if (plan$dashboard == "validation-compare") {
    base <- data.table::data.table(datetime = raw[[datetime.col]], depth = .col_or_na(raw, "depth"))
    cand_df <- data.frame(label = character(0), pitch = character(0), roll = character(0), stringsAsFactors = FALSE)
    ref <- NULL
    for (k in seq_along(plan$candidates)) {
      applied <- .applyCandidate(raw, plan$candidates[[k]]$apply, configs, mapping, id.col)
      o <- .liteOrientation(applied, .tagFs(applied, datetime.col), datetime.col = datetime.col)
      base[, paste0("pitch.c", k)] <- o$pitch
      base[, paste0("roll.c", k)]  <- o$roll
      cand_df <- rbind(cand_df, data.frame(label = plan$candidates[[k]]$label,
                                           pitch = paste0("pitch.c", k), roll = paste0("roll.c", k),
                                           stringsAsFactors = FALSE))
      if (k == 1L) { ref <- data.table::copy(applied); ref[, `:=`(roll = o$roll, heading = o$heading, vertical_velocity = o$vertical_velocity)] }
    }
    fs_data <- ref; render_data <- base; render_candidates <- cand_df
  } else {
    applied <- .applyCandidate(raw, plan$apply, configs, mapping, id.col)
    o <- .liteOrientation(applied, .tagFs(applied, datetime.col), datetime.col = datetime.col)
    applied[, `:=`(pitch = o$pitch, roll = o$roll, heading = o$heading, vertical_velocity = o$vertical_velocity)]
    fs_data <- applied; render_data <- applied; render_candidates <- NULL
  }

  # ---- restrict the manoeuvre search to the video-covered windows ----
  # Only footage-covered moments are reviewable, and a deployment usually films a small fraction of its
  # span; searching the whole record would pick the globally-clearest manoeuvres, almost all of them
  # off-camera. matchClip() below is the final guard for a peak that still falls in a between-clip gap.
  search <- fs_data[.inAnyInterval(fs_data[[datetime.col]], vmi$start, vmi$end), ]
  if (!nrow(search))
    return(.reviewOut(.manifestRow(id, reason, priority, plan$mapping_source, status = "no_segment"), cands))
  if (!(id.col %in% names(search))) search[[id.col]] <- id
  segs <- findValidationSegments(search, types = types, n = max(n, clips.per.deployment),
                                 window = window, id.col = id.col, datetime.col = datetime.col, verbose = FALSE)
  segs <- .selectClips(segs, types, clips.per.deployment)
  if (!nrow(segs))
    return(.reviewOut(.manifestRow(id, reason, priority, plan$mapping_source, status = "no_segment"), cands))

  caption <- .reviewCaption(reason, plan)
  rows <- vector("list", nrow(segs))
  for (j in seq_len(nrow(segs))) {
    seg <- segs[j, ]
    clip <- .matchClip(video.metadata, id, seg$peak_time)
    if (is.null(clip)) {
      rows[[j]] <- .manifestRow(id, reason, priority, plan$mapping_source, type = seg$type, rank = seg$rank,
                                peak_time = seg$peak_time, start = seg$start, end = seg$end, status = "no_coverage")
      next
    }
    out <- file.path(output.dir, sprintf("%s_%s_%d.mp4", id, seg$type, seg$rank))
    ok <- tryCatch({
      renderOverlayVideo(video = clip$file, data = render_data, output = out, dashboard = plan$dashboard,
                         video.start = clip$start, start = seg$start, end = seg$end,
                         caption = caption, candidates = render_candidates,
                         side = side, overlay.fps = overlay.fps, codec = codec, verbose = FALSE)
      TRUE
    }, error = function(e) FALSE)
    status <- if (ok && file.exists(out)) "rendered" else "render_failed"
    rows[[j]] <- .manifestRow(id, reason, priority, plan$mapping_source, type = seg$type, rank = seg$rank,
                              peak_time = seg$peak_time, start = seg$start, end = seg$end,
                              source_video = basename(clip$file), clip = if (status == "rendered") out else NA_character_,
                              status = status)
  }
  .reviewOut(do.call(rbind, rows), cands)
}

################################################################################
# Candidate / mapping resolution (internal) ####################################
################################################################################

#' Decide the dashboard, the mapping(s) to apply, and (for comparisons) the labelled candidate frames.
#'
#' The common flagged reasons become a SIDE-BY-SIDE comparison so the reviewer can pick the mapping that
#' banks like the animal in the video (rather than judging a single indicator in isolation):
#'   * conflict: the DOCUMENTED config (as recorded) vs the DATA-PREFERRED right-handed frame
#'     (the corrected candidate `fs$survivors[1,]`);
#'   * ambiguous: the distinct surviving right-handed frames (capped at `max.candidates`).
#' The remaining reasons show a single indicator (no competing accel-frame candidate): `unanalysed` -> the
#' raw frame; `coreg_fail` and everything else -> the resolved/proposed mapping.
#' @keywords internal
#' @noRd
.candidatePlan <- function(reason, fs, configs, max.candidates) {
  # conflict -> documented (as recorded) vs the data-preferred (corrected) frame
  if (identical(reason, "conflict") && !is.null(configs) &&
      !is.null(fs) && !is.null(fs$survivors) && nrow(fs$survivors) >= 1L) {
    best <- fs$survivors[1, , drop = FALSE]
    cands <- list(list(apply = list(type = "configs"),                        label = "Documented"),
                  list(apply = list(type = "ft", ft = .survivorToAccelFt(best)), label = "Proposed"))
    return(list(dashboard = "validation-compare", mapping_source = "documented_vs_data", candidates = cands))
  }
  # ambiguous -> the distinct surviving right-handed frames, side by side
  if (reason == "ambiguous" && !is.null(fs) && !is.null(fs$survivors)) {
    s <- fs$survivors
    reps <- s[!duplicated(paste(s$newZ_col, s$newZ_sign)), , drop = FALSE]
    if (nrow(reps) >= 2L) {
      reps <- utils::head(reps, max.candidates)
      cands <- lapply(seq_len(nrow(reps)), function(k) {
        sr <- reps[k, ]
        list(apply = list(type = "ft", ft = .survivorToAccelFt(sr)),
             label = sprintf("Z=%s%s", if (sr$newZ_sign < 0) "-" else "+", sr$newZ_col))
      })
      return(list(dashboard = "validation-compare", mapping_source = "compare", candidates = cands))
    }
  }
  if (reason == "unanalysed")
    return(list(dashboard = "validation", mapping_source = "identity_raw", apply = list(type = "identity")))
  # gyro_inconsistent / low_confidence / user_requested / degenerate cases: the resolved mapping, single
  list(dashboard = "validation", mapping_source = "proposal", apply = list(type = "mapping"))
}

#' Apply one candidate mapping to a fresh copy of the raw data (delegates to applyAxisMapping; silent).
#' @keywords internal
#' @noRd
.applyCandidate <- function(raw, spec, configs, mapping, id.col) {
  copy <- data.table::copy(raw)
  if (spec$type == "identity") return(copy)
  args <- list(data = list(copy), id.col = id.col, return.data = TRUE,
               check.handedness = FALSE, verbose = FALSE)
  if (spec$type == "configs") {
    if (is.null(configs)) return(copy)            # nothing to apply; fall back to raw frame
    args$configs <- configs
  } else if (spec$type == "ft") {
    args$mapping <- spec$ft
  } else {                                          # "mapping": the checkTagMapping/consensus object
    args$mapping <- mapping
  }
  out <- tryCatch(do.call(applyAxisMapping, args)[[1]], error = function(e) copy)
  if (!data.table::is.data.table(out)) out <- data.table::as.data.table(out)
  out
}

#' A survivor row -> an accelerometer from/to mapping (raw axis -> signed body channel).
#' @keywords internal
#' @noRd
.survivorToAccelFt <- function(s) {
  sgn <- function(name, v) paste0(if (v < 0) "-" else "", name)
  data.frame(from = c(s$newX_col, s$newY_col, s$newZ_col),
             to   = c(sgn("ax", s$newX_sign), sgn("ay", s$newY_sign), sgn("az", s$newZ_sign)),
             stringsAsFactors = FALSE)
}

#' Default mapping_source label for a reason (used on degenerate manifest rows).
#' @keywords internal
#' @noRd
.mappingSourceFor <- function(reason) {
  switch(reason,
         conflict = "documented_config",
         ambiguous = "compare",
         unanalysed = "identity_raw",
         "proposal")
}

#' One-line caption for the validation dashboards (the prominent "what to look for" guidance).
#' @keywords internal
#' @noRd
.reviewCaption <- function(reason, plan) {
  if (identical(plan$dashboard, "validation-compare")) {
    lead <- if (identical(reason, "conflict"))
      "Watch a clear roll: the indicator that banks the SAME way as the shark is the correct mapping"
    else "Ambiguous frame: which indicator banks the same way as the shark?"
    return(lead)
  }
  if (identical(reason, "coreg_fail"))
    return("Gyroscope does not co-register with the accelerometer - verify the proposed gyro mapping banks the same way as the shark (or re-check the tag).")
  sprintf("Confirm the indicator banks the same way as the shark (flagged: %s)", reason)
}

################################################################################
# Clip selection + manifest (internal) #########################################
################################################################################

#' Pick up to `k` segments, round-robin across types by rank (a diverse turn/roll/dive set).
#' @keywords internal
#' @noRd
.selectClips <- function(segs, types, k) {
  if (is.null(segs) || !nrow(segs)) return(segs)
  picked <- list()
  ranks <- sort(unique(segs$rank))
  for (rk in ranks) {
    for (ty in types) {
      hit <- segs[segs$type == ty & segs$rank == rk, , drop = FALSE]
      if (nrow(hit)) picked[[length(picked) + 1L]] <- hit[1, , drop = FALSE]
      if (length(picked) >= k) break
    }
    if (length(picked) >= k) break
  }
  out <- do.call(rbind, picked)
  rownames(out) <- NULL
  out
}

#' Build one manifest row with the canonical schema.
#' @keywords internal
#' @noRd
.manifestRow <- function(id, review_reason, priority, mapping_source,
                         type = NA_character_, rank = NA_integer_, peak_time = as.POSIXct(NA),
                         start = as.POSIXct(NA), end = as.POSIXct(NA),
                         source_video = NA_character_, clip = NA_character_, status) {
  data.frame(id = id, review_reason = review_reason, priority = as.integer(priority),
             mapping_source = mapping_source, type = type, rank = as.integer(rank),
             peak_time = peak_time, start = start, end = end,
             source_video = source_video, clip = clip, status = status, stringsAsFactors = FALSE)
}

#' Empty manifest / queue with the canonical schema.
#' @keywords internal
#' @noRd
.emptyManifest <- function() .manifestRow(character(0), character(0), integer(0), character(0), status = character(0))

#' @keywords internal
#' @noRd
.emptyQueue <- function() data.frame(id = character(0), review_reason = character(0), priority = integer(0), stringsAsFactors = FALSE)

#' A column from a data.table, or an NA vector of the right length if absent.
#' @keywords internal
#' @noRd
.col_or_na <- function(dt, col) if (col %in% names(dt)) dt[[col]] else rep(NA_real_, nrow(dt))

#' Compact a long id vector for a one-line summary: the first `k`, then "+N more".
#' @keywords internal
#' @noRd
.truncList <- function(x, k = 8L) {
  if (length(x) <= k) return(paste(x, collapse = ", "))
  paste0(paste(utils::head(x, k), collapse = ", "), sprintf(" +%d more", length(x) - k))
}

################################################################################
# Review object (nautilus_review): the per-deployment decision sheet ###########
################################################################################

#' Wrap one deployment's per-clip manifest rows together with its concrete candidate mappings.
#' @keywords internal
#' @noRd
.reviewOut <- function(rows, candidates = list()) list(rows = rows, candidates = candidates)

#' Normalise a decision string for tolerant (case- and whitespace-insensitive) matching.
#' @keywords internal
#' @noRd
.normDecision <- function(x) tolower(trimws(as.character(x)))

#' Concrete apply-time from/to for each candidate of a compare plan (empty list for single-path plans).
#'
#' The decision a reviewer makes is which candidate's mapping to APPLY, so each candidate carries a
#' COMPLETE from/to - identical to what `applyAxisMapping()` would receive directly: `Documented` -> the
#' recorded config; `Proposed` -> the deployment's full inferred proposal; an ambiguous `Z=...` -> that
#' survivor's accelerometer frame. Single-path reasons (gyro_inconsistent / low_confidence / unanalysed)
#' return nothing: they carry no A/B choice, so the base mapping applies unchanged.
#' @keywords internal
#' @noRd
.reviewApplyCandidates <- function(plan, id, mapping, axis_config, configs) {
  if (!identical(plan$dashboard, "validation-compare")) return(list())
  lapply(plan$candidates, function(cd) {
    ft <- if (identical(cd$apply$type, "configs")) {
            if (is.null(configs) || is.null(axis_config)) NULL else .expandAxisConfigForTag(axis_config, configs, id)
          } else if (identical(cd$label, "Proposed") && id %in% names(mapping) && !is.null(mapping[[id]]$proposal)) {
            mapping[[id]]$proposal
          } else if (identical(cd$apply$type, "ft")) {
            cd$apply$ft
          } else NULL
    list(label = cd$label,
         from_to = if (!is.null(ft) && nrow(ft)) as.data.frame(ft[, c("from", "to")], stringsAsFactors = FALSE) else NULL)
  })
}

#' Collapse the per-clip manifest to one row per flagged deployment (the human-facing decision sheet).
#' @keywords internal
#' @noRd
.buildReviewSheet <- function(queue, manifest, cand_store) {
  rows <- lapply(seq_len(nrow(queue)), function(i) {
    id <- queue$id[i]
    mr <- if (!is.null(manifest)) manifest[manifest$id == id, , drop = FALSE] else NULL
    st <- if (!is.null(mr) && any(mr$status == "rendered")) "rendered"
          else if (!is.null(mr) && nrow(mr)) mr$status[1] else "no_video"
    cs <- cand_store[[id]]
    opts <- if (length(cs)) paste(vapply(cs, `[[`, "", "label"), collapse = " | ") else ""
    data.frame(id = id, review_reason = queue$review_reason[i], priority = queue$priority[i],
               n_clips = as.integer(if (!is.null(mr)) sum(mr$status == "rendered") else 0L),
               status = st, options = opts, decision = NA_character_, stringsAsFactors = FALSE)
  })
  out <- do.call(rbind, rows); rownames(out) <- NULL; out
}

#' The reviewable deployments (clips rendered) that still offer a genuine A/B choice.
#' @keywords internal
#' @noRd
.reviewDecisionsNeeded <- function(sheet, cand_store) {
  ids <- sheet$id[sheet$status == "rendered"]
  ids[vapply(ids, function(i) length(cand_store[[i]]) >= 2L, logical(1))]
}

#' Construct a `nautilus_review`: the decision sheet, plus everything applyAxisMapping() needs (as
#' attributes: the concrete candidates, the base routing set, and the per-clip manifest).
#' @keywords internal
#' @noRd
.newReview <- function(sheet, candidates, base_mapping, manifest, output.dir) {
  structure(sheet, class = c("nautilus_review", "data.frame"),
            review_candidates = candidates, review_base = base_mapping,
            review_manifest = manifest, review_output = output.dir)
}

#' Empty review (nothing flagged), still carrying the base so applyAxisMapping() maps every deployment.
#' @keywords internal
#' @noRd
.emptyReview <- function(base_mapping = NULL, output.dir = NULL) {
  sheet <- data.frame(id = character(0), review_reason = character(0), priority = integer(0),
                      n_clips = integer(0), status = character(0), options = character(0),
                      decision = character(0), stringsAsFactors = FALSE)
  .newReview(sheet, candidates = list(), base_mapping = base_mapping,
             manifest = .emptyManifest(), output.dir = output.dir)
}

#' Print a nautilus axis-mapping review
#'
#' Shows the decision sheet: one row per flagged deployment, the candidate `options`, and which rows
#' still need a `decision`. Fill the `decision` column (e.g. `x$decision[x$id == "PIN_CAM_04"] <-
#' "Documented"`) and pass the object to \link{applyAxisMapping} as `mapping`.
#' @param x A `nautilus_review` object returned by \link{reviewTagMapping}.
#' @param ... Ignored.
#' @return `x`, invisibly.
#' @export
print.nautilus_review <- function(x, ...) {
  cli::cli_h1("nautilus axis-mapping review")
  n <- nrow(x)
  if (!n) { cli::cli_alert_info("No deployments were flagged for review."); return(invisible(x)) }
  required <- .reviewDecisionsNeeded(x, attr(x, "review_candidates") %||% list())    # rows that require a call
  dec_req  <- x$decision[match(required, x$id)]
  pending  <- required[is.na(dec_req) | !nzchar(dec_req)]                             # required and still undecided
  n_excl   <- sum(!is.na(x$decision) & .normDecision(x$decision) == "exclude")
  excl_note <- if (n_excl > 0L) sprintf(" (%d marked Exclude)", n_excl) else ""
  cli::cli_text("{n} flagged deployment{?s}; {length(pending)} still need{?s/} a decision.{excl_note}")
  cli::cli_text("Fill {.field decision} with an option (or {.val Exclude}), then pass this to {.fn applyAxisMapping} as {.arg mapping}.")
  disp <- data.frame(id = x$id, reason = x$review_reason, clips = x$n_clips, status = x$status,
                     options = ifelse(nzchar(x$options), x$options, "-"),
                     decision = ifelse(is.na(x$decision) | !nzchar(x$decision),
                                       ifelse(x$id %in% pending, "<choose>", "-"), x$decision),
                     stringsAsFactors = FALSE)
  print(disp, row.names = FALSE)
  od <- attr(x, "review_output")
  if (!is.null(od)) cli::cli_text("Clips in {.file {od}}; per-clip detail in {.code attr(x, \"review_manifest\")}.")
  invisible(x)
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
