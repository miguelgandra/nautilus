#######################################################################################################
# calculatePaddleSpeed() internals ####################################################################
#######################################################################################################

#' Validate a calibration table, or pass NULL straight through.
#' @keywords internal
#' @noRd
.assert_calibration <- function(calibration) {
  if (is.null(calibration)) return(NULL)
  if (data.table::is.data.table(calibration)) calibration <- as.data.frame(calibration)
  if (!is.data.frame(calibration)) .abort("{.arg calibration} must be a data frame.")
  miss <- setdiff(c("year", "package_id", "slope"), names(calibration))
  if (length(miss))
    .abort(c("{.arg calibration} is missing the column{?s} {.field {miss}}.",
             "i" = "It needs {.field year}, {.field package_id} and {.field slope}."))
  if (!is.numeric(calibration$year))  .abort("{.field year} in {.arg calibration} must be numeric.")
  if (!is.numeric(calibration$slope)) .abort("{.field slope} in {.arg calibration} must be numeric.")
  calibration
}

#' What one deployment carries, plus the sums the in-situ fit needs.
#'
#' The in-situ slope is a through-origin fit of vertical velocity on `paddle_freq * sin(pitch)`, so its
#' sums add across the deployments of one tag. Accumulating them here means the fit can be done per tag
#' without holding any deployment in memory.
#'
#' The sign flip is the depth convention: depth increases downwards, so a descending animal has a
#' positive vertical velocity and a negative (nose-down) pitch. Negating one of them makes the fitted
#' slope positive, as a speed per rotation should be.
#' @keywords internal
#' @noRd
.paddleScanOne <- function(dt, fallback_id, min.pitch, need.insitu) {
  meta <- .getMeta(.ensureMeta(dt))
  id  <- as.character(meta$id %||% fallback_id)
  pkg <- as.character(meta$tag$package_id %||% NA_character_)
  yr  <- suppressWarnings(as.integer(format(meta$deployment$datetime %||% NA, "%Y")))
  if (!is.finite(yr) && "datetime" %in% names(dt) && nrow(dt))
    yr <- suppressWarnings(as.integer(format(dt$datetime[1], "%Y")))

  has_freq  <- "paddle_freq"  %in% names(dt) && any(is.finite(dt$paddle_freq))
  has_speed <- "paddle_speed" %in% names(dt) && any(is.finite(dt$paddle_speed))
  out <- list(id = id, pkg = pkg, year = yr,
              key = paste0(yr, "/", pkg),
              has_freq = has_freq, has_speed = has_speed,
              paddle_flag = isTRUE(meta$tag$paddle_wheel),
              Sxy = NA_real_, Sxx = NA_real_, Syy = NA_real_, n = 0L)

  if (!need.insitu || !has_freq) return(out)
  if (!all(c("pitch", "vertical_velocity") %in% names(dt))) return(out)
  ok <- is.finite(dt$paddle_freq) & is.finite(dt$pitch) & is.finite(dt$vertical_velocity) &
        abs(dt$pitch) >= min.pitch
  if (!any(ok)) return(out)
  xv <- dt$paddle_freq[ok] * sin(-dt$pitch[ok] * pi / 180)
  yv <- dt$vertical_velocity[ok]
  out$Sxy <- sum(xv * yv); out$Sxx <- sum(xv * xv); out$Syy <- sum(yv * yv); out$n <- sum(ok)
  out
}

#' One slope per tag and season, and where it came from.
#' @keywords internal
#' @noRd
.paddleResolve <- function(scan, calibration, method, agreement.threshold, lvl) {
  keys <- unique(vapply(scan, function(z) z$key, character(1)))
  cal <- do.call(rbind, lapply(keys, function(k) {
    w <- Filter(function(z) identical(z$key, k), scan)
    Sxy <- sum(vapply(w, function(z) z$Sxy %||% NA_real_, numeric(1)), na.rm = TRUE)
    Sxx <- sum(vapply(w, function(z) z$Sxx %||% NA_real_, numeric(1)), na.rm = TRUE)
    Syy <- sum(vapply(w, function(z) z$Syy %||% NA_real_, numeric(1)), na.rm = TRUE)
    n   <- sum(vapply(w, function(z) z$n, integer(1)))
    ins <- if (n > 0 && is.finite(Sxx) && Sxx > 0) Sxy / Sxx else NA_real_
    r   <- if (n > 0 && is.finite(Sxx) && Sxx > 0 && Syy > 0) Sxy / sqrt(Sxx * Syy) else NA_real_
    # standard error of a through-origin slope, from the residual sum of squares
    se  <- if (n > 2 && is.finite(ins)) sqrt(max(0, (Syy - ins * Sxy)) / ((n - 1) * Sxx)) else NA_real_
    data.frame(key = k, year = w[[1]]$year, package_id = w[[1]]$pkg,
               n_deployments = length(w),
               has_paddle = any(vapply(w, function(z) z$has_freq || z$has_speed || z$paddle_flag,
                                       logical(1))),
               in_situ_slope = ins, in_situ_r = r,
               in_situ_lo = ins - 1.96 * se, in_situ_hi = ins + 1.96 * se,
               in_situ_n = n, stringsAsFactors = FALSE)
  }))

  cal$slope <- NA_real_; cal$slope_source <- NA_character_
  if (!is.null(calibration)) {
    m <- match(paste0(calibration$year, "/", as.character(calibration$package_id)), cal$key)
    ok <- !is.na(m)
    cal$slope[m[ok]] <- calibration$slope[ok]
    cal$slope_source[m[ok]] <- "measured"
  }

  gap <- is.na(cal$slope)
  if (any(gap)) {
    if (identical(method, "in-situ")) {
      cal$slope[gap] <- cal$in_situ_slope[gap]
      cal$slope_source[gap] <- ifelse(is.finite(cal$in_situ_slope[gap]), "in-situ", NA_character_)
    } else if (!is.null(calibration)) {
      cal <- .paddleImputeGaps(cal, calibration, method, gap, lvl)
    }
  }
  cal$agreement <- cal$slope / cal$in_situ_slope
  # `agreement.threshold` is a proportion, but agreement is a RATIO, so the band is applied
  # multiplicatively rather than as 1 +/- t. A slope 35% above the in-situ estimate and one 35% below it
  # are the same size of disagreement, and only 1/(1+t) .. (1+t) treats them that way; 1 - t would make
  # the low side stricter than the high side purely as an artefact of the arithmetic.
  hi <- 1 + agreement.threshold
  cal$flag <- is.finite(cal$agreement) & (cal$agreement > hi | cal$agreement < 1 / hi)
  cal[order(cal$year, cal$package_id), , drop = FALSE]
}

#' Fill the remaining gaps through imputePaddleCalibration(), keeping its provenance labels.
#' @keywords internal
#' @noRd
.paddleImputeGaps <- function(cal, calibration, method, gap, lvl) {
  # `paddle_wheel` is passed explicitly: imputePaddleCalibration() uses it to skip tags that never had a
  # paddle, and warns when it is absent. This function already knows the answer from each tag's own
  # metadata, so there is nothing to guess at.
  need <- data.frame(package_id = cal$package_id[gap], year = cal$year[gap],
                     paddle_wheel = cal$has_paddle[gap], stringsAsFactors = FALSE)
  filled <- try(suppressMessages(
    imputePaddleCalibration(calibration = calibration, deployments = need, method = method,
                            verbose = 0)), silent = TRUE)
  if (inherits(filled, "try-error") || !is.data.frame(filled)) {
    cli::cli_warn(c("Could not estimate the missing calibration{?s} with {.val {method}}.",
                    "i" = "Those deployments get no speed. {.code method = \"in-situ\"} estimates the
                           slope from the deployments themselves."))
    return(cal)
  }
  k <- paste0(filled$year, "/", as.character(filled$package_id))
  m <- match(cal$key[gap], k)
  cal$slope[gap] <- filled$slope[m]
  src <- if ("slope_source" %in% names(filled)) filled$slope_source[m] else rep("imputed", length(m))
  cal$slope_source[gap] <- ifelse(is.na(cal$slope[gap]), NA_character_, src)
  cal
}

#' Apply one tag's slope to one deployment.
#' @keywords internal
#' @noRd
.paddleApplyOne <- function(dt, sc, row, smoothing, max.speed) {
  status <- "applied"; slope <- NA_real_; src <- NA_character_

  if (!sc$has_freq) {
    if (sc$has_speed) {
      # the logger wrote a speed itself, so there is no rotation rate to calibrate: keep what it recorded
      status <- "as-recorded"; src <- "as-recorded"
    } else {
      if (!"paddle_speed" %in% names(dt)) dt[, paddle_speed := NA_real_]
      status <- if (sc$paddle_flag) "no paddle data" else "no paddle wheel"
    }
    return(list(data = dt, status = status, slope = slope, slope_source = src))
  }

  if (!nrow(row) || !is.finite(row$slope)) {
    dt[, paddle_speed := NA_real_]
    return(list(data = dt, status = "no calibration", slope = NA_real_, slope_source = NA_character_))
  }
  slope <- row$slope; src <- row$slope_source

  fq <- dt$paddle_freq
  if (!is.null(smoothing) && smoothing > 0 && nrow(dt) > 2) {
    fs <- .estimateHz(dt$datetime)
    k <- if (is.finite(fs)) max(1L, round(fs * smoothing)) else 1L
    if (k > 1L && k <= nrow(dt)) fq <- data.table::frollmean(fq, n = k, fill = NA, align = "center")
  }
  sp <- fq * slope
  if (!is.null(max.speed)) sp[is.finite(sp) & sp > max.speed / 3.6] <- NA_real_
  dt[, paddle_speed := sp]
  list(data = dt, status = status, slope = slope, slope_source = src)
}


#' The calibration block: one line per tag and season, saying where its slope came from.
#' @keywords internal
#' @noRd
.reportPaddleCalibration <- function(lvl, cal, method, validate) {
  if (lvl < 1L || !nrow(cal)) return(invisible(NULL))
  lab <- c(measured = "measured", `in-situ` = "in situ", `tag-model` = "estimated (tag model)",
           baseline = "estimated (baseline)", imputed = "estimated", `as-recorded` = "as recorded")
  rows <- vapply(seq_len(nrow(cal)), function(i) {
    s <- cal$slope_source[i]
    if (is.na(cal$slope[i])) return("no calibration")
    sprintf("%.4f  %s", cal$slope[i], if (!is.na(s) && s %in% names(lab)) unname(lab[s]) else "estimated")
  }, character(1))
  .log_section(lvl, "Calibration")
  .log_rows(lvl, stats::setNames(rows, sprintf("%s / package %s", cal$year, cal$package_id)))

  if (isTRUE(validate) && any(is.finite(cal$in_situ_slope))) {
    v <- vapply(seq_len(nrow(cal)), function(i) {
      if (!is.finite(cal$in_situ_slope[i])) return("not enough steep swimming to check")
      sprintf("in situ %.4f %s agreement %.2f%s", cal$in_situ_slope[i], cli::symbol$bullet,
              cal$agreement[i], if (isTRUE(cal$flag[i])) "  (worth a look)" else "")
    }, character(1))
    .log_section(lvl, "Check against the animal's own diving")
    .log_rows(lvl, stats::setNames(v, sprintf("%s / package %s", cal$year, cal$package_id)))
  }
  invisible(NULL)
}

#' The summary block.
#' @keywords internal
#' @noRd
.reportPaddleCohort <- function(lvl, cal, statuses, output.dir) {
  if (lvl < 1L) return(invisible(NULL))
  tick <- cli::col_green(cli::symbol$tick)
  n_ok <- sum(statuses %in% c("applied", "as-recorded"))
  rows <- c(`Speed calculated` = sprintf("%d/%d", n_ok, length(statuses)))
  other <- statuses[!statuses %in% c("applied", "as-recorded")]
  if (length(other)) {
    tb <- table(other)
    rows <- c(rows, Skipped = paste(sprintf("%d (%s)", as.integer(tb), names(tb)), collapse = ", "))
  }
  if (any(statuses == "as-recorded"))
    rows <- c(rows, `Speed as recorded` = format(sum(statuses == "as-recorded")))
  src <- stats::na.omit(cal$slope_source)
  if (length(src)) {
    tb <- table(src)
    rows <- c(rows, Calibrations = paste(sprintf("%d %s", as.integer(tb), names(tb)), collapse = ", "))
  }
  if (any(cal$flag, na.rm = TRUE))
    rows <- c(rows, `Worth a look` = paste(sprintf("%s/%s", cal$year[which(cal$flag)],
                                                   cal$package_id[which(cal$flag)]), collapse = ", "))
  .log_section(lvl, "Results")
  .log_rows(lvl, rows, symbols = c(tick, rep(cli::symbol$bullet, length(rows) - 1L)))
  if (!is.null(output.dir)) { .log_section(lvl, "Output"); .log_rows(lvl, c(Directory = output.dir)) }
  cli::cli_text("")
  invisible(NULL)
}

#' One panel per tag: the slope applied against the in-situ estimate, with its interval.
#' @keywords internal
#' @noRd
.renderPaddleDiagnostic <- function(cal, plot = FALSE, plot.file = NULL) {
  keep <- cal[is.finite(cal$in_situ_slope) | is.finite(cal$slope), , drop = FALSE]
  if (!nrow(keep)) return(invisible(NULL))
  draw <- function(to.file = FALSE, unicode = TRUE) {
    theme <- plotTheme()
    op <- graphics::par(family = theme$font.family, mar = c(4.4, 8.5, 3.2, 1.2), no.readonly = TRUE)
    on.exit(graphics::par(op), add = TRUE)
    y <- seq_len(nrow(keep))
    xr <- range(c(keep$slope, keep$in_situ_slope, keep$in_situ_lo, keep$in_situ_hi), na.rm = TRUE)
    if (!all(is.finite(xr))) xr <- c(0, 1)
    graphics::plot(NA, xlim = xr * c(0.9, 1.1), ylim = c(0.5, nrow(keep) + 0.5), yaxt = "n",
                   xlab = "calibration slope (m/s per Hz)", ylab = "", las = 1)
    graphics::segments(keep$in_situ_lo, y, keep$in_situ_hi, y, col = "grey60")
    graphics::points(keep$in_situ_slope, y, pch = 1, col = "grey30")
    graphics::points(keep$slope, y, pch = 19,
                     col = ifelse(!is.na(keep$slope_source) & keep$slope_source == "measured",
                                  "#1F6FB4", "#C8892A"))
    graphics::axis(2, y, sprintf("%s / pkg %s", keep$year, keep$package_id), las = 1, cex.axis = 0.8,
                   tick = FALSE)
    if (any(keep$flag, na.rm = TRUE))
      graphics::points(keep$slope[which(keep$flag)], y[which(keep$flag)], pch = 1, cex = 2.4, col = "#B22222")
    graphics::legend("topright", c("measured", "estimated", "in situ"), pch = c(19, 19, 1),
                     col = c("#1F6FB4", "#C8892A", "grey30"), bty = "n", cex = 0.8)
    graphics::mtext("Paddle-wheel calibration", side = 3, line = 1.4, adj = 0, font = 2)
  }
  .renderToDevices(draw, plot = plot, plot.file = plot.file, cairo = TRUE,
                   width = 8, height = max(3.2, 1.6 + 0.32 * nrow(keep)))
  invisible(NULL)
}
