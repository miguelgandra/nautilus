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
.paddleResolve <- function(scan, calibration, method, degradation.rate, agreement.threshold, lvl) {
  keys <- unique(vapply(scan, function(z) z$key, character(1)))
  cal <- do.call(rbind, lapply(keys, function(k) {
    w <- Filter(function(z) identical(z$key, k), scan)
    hf <- vapply(w, function(z) z$has_freq, logical(1))
    hs <- vapply(w, function(z) z$has_speed, logical(1))
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
               has_paddle = any(hf | hs | vapply(w, function(z) z$paddle_flag, logical(1))),
               # a slope only converts a rotation rate, so only a tag that recorded one has a use for
               # it; `as_recorded` separates "finished without a slope" from "nothing was recorded"
               needs_slope = any(hf),
               as_recorded = !any(hf) && any(hs),
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

  # Estimating a slope for a tag that never recorded a rotation rate manufactures a number that gets
  # reported in the calibration table and then never applied: the deployments that wrote a speed
  # directly are already finished, and the ones that recorded nothing cannot be rescued by a slope.
  # This is the same defect as imputing for a tag with no paddle, reached by a different route. A
  # MEASURED calibration is left alone even so - it is a real observation, not a guess.
  gap <- is.na(cal$slope) & cal$needs_slope
  if (any(gap)) {
    if (identical(method, "in-situ")) {
      cal$slope[gap] <- cal$in_situ_slope[gap]
      cal$slope_source[gap] <- ifelse(is.finite(cal$in_situ_slope[gap]), "in-situ", NA_character_)
    } else if (!is.null(calibration)) {
      cal <- .paddleImputeGaps(cal, calibration, method, degradation.rate, gap, lvl)
    }
  }
  # left without a slope, but for a reason worth naming: the logger reported speed itself
  cal$slope_source[is.na(cal$slope) & cal$as_recorded] <- "as-recorded"

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
.paddleImputeGaps <- function(cal, calibration, method, degradation.rate, gap, lvl) {
  # `paddle_wheel` is passed explicitly: imputePaddleCalibration() uses it to skip tags that never had a
  # paddle, and warns when it is absent. This function already knows the answer from each tag's own
  # metadata, so there is nothing to guess at.
  need <- data.frame(package_id = cal$package_id[gap], year = cal$year[gap],
                     paddle_wheel = cal$has_paddle[gap], stringsAsFactors = FALSE)
  filled <- try(suppressMessages(
    imputePaddleCalibration(calibration = calibration, deployments = need, method = method,
                            degradation.rate = degradation.rate, verbose = 0)), silent = TRUE)
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
#'
#' Also returns what the per-deployment log needs - the row count, the sampling rate and the resulting
#' speed distribution - so the caller can report the deployment without walking the data a second time.
#' @keywords internal
#' @noRd
.paddleApplyOne <- function(dt, sc, row, smoothing, max.speed) {
  # every early return goes through out(), so the fields the per-deployment log reads are always present
  out <- function(data, status, slope = NA_real_, slope_source = NA_character_, speed = NULL)
    list(data = data, status = status, slope = slope, slope_source = slope_source,
         speed = speed, n_rows = nrow(data), fs = .estimateHz(data$datetime))

  if (!sc$has_freq) {
    if (sc$has_speed) {
      # the logger wrote a speed itself, so there is no rotation rate to calibrate: keep what it recorded
      return(out(dt, "as-recorded", slope_source = "as-recorded",
                 speed = .paddleSpeedStats(dt$paddle_speed)))
    }
    if (!"paddle_speed" %in% names(dt)) dt[, paddle_speed := NA_real_]
    return(out(dt, if (sc$paddle_flag) "no paddle data" else "no paddle wheel"))
  }

  if (!nrow(row) || !is.finite(row$slope)) {
    dt[, paddle_speed := NA_real_]
    return(out(dt, "no calibration"))
  }
  fq <- dt$paddle_freq
  slope <- row$slope
  fs <- .estimateHz(dt$datetime)      # also reported as the deployment's sampling rate
  if (!is.null(smoothing) && smoothing > 0 && nrow(dt) > 2) {
    k <- if (is.finite(fs)) max(1L, round(fs * smoothing)) else 1L
    if (k > 1L && k <= nrow(dt)) fq <- data.table::frollmean(fq, n = k, fill = NA, align = "center")
  }
  sp <- fq * slope
  if (!is.null(max.speed)) sp[is.finite(sp) & sp > max.speed / 3.6] <- NA_real_
  dt[, paddle_speed := sp]
  list(data = dt, status = "applied", slope = row$slope, slope_source = row$slope_source,
       speed = .paddleSpeedStats(sp), n_rows = nrow(dt), fs = fs)
}

#' Median and range of a speed column, or NULL when it holds nothing finite.
#' @keywords internal
#' @noRd
.paddleSpeedStats <- function(v) {
  if (is.null(v)) return(NULL)
  ok <- is.finite(v)
  if (!any(ok)) return(NULL)
  c(med = stats::median(v[ok]), lo = min(v[ok]), hi = max(v[ok]))
}

#' Where a slope came from, in words.
#'
#' Two registers for the same fact: the long form reads as prose in a per-deployment line, the short one
#' fits a table column. Keeping them in one place stops the two from drifting apart.
#' @keywords internal
#' @noRd
.paddleSourceLabel <- function(src, long = TRUE) {
  if (length(src) != 1L || is.na(src)) return(NA_character_)
  tab <- if (long)
    c(measured = "measured", `in-situ` = "estimated (in situ)", `tag-model` = "estimated (tag model)",
      baseline = "estimated (baseline)", imputed = "estimated", `as-recorded` = "as recorded")
  else
    c(measured = "measured", `in-situ` = "in situ", `tag-model` = "tag model",
      baseline = "baseline", imputed = "estimated", `as-recorded` = "as recorded")
  if (src %in% names(tab)) unname(tab[src]) else "estimated"
}

#' One deployment's block: what came in, which slope was applied, and what speed came out.
#'
#' Mirrors the per-deployment layout used by calculateTailBeats(): an `input:` line, the setting that
#' actually varies between deployments (here the slope and its provenance), then the result indented
#' under it. A deployment that gets no speed prints the skip line only, as the tail-beat blocks do.
#' @keywords internal
#' @noRd
.logPaddleDeployment <- function(lvl, id, sc, res) {
  if (lvl < 1L) return(invisible(NULL))
  # `\u00b7` separates facts WITHIN a line and the heavier bullet marks the skip line, as in
  # calculateTailBeats(): the two glyphs are doing different jobs and are not interchangeable.
  dot <- "\u00b7"
  if (!res$status %in% c("applied", "as-recorded")) {
    why <- switch(res$status,
                  "no calibration" = "no calibration for this tag and season",
                  "no paddle data" = "no paddle data",
                  "no paddle wheel" = "no paddle wheel", res$status)
    .log_skip(lvl, id, "  ", why, " ", cli::symbol$bullet, " skipped")
    return(invisible(NULL))
  }

  key <- if (!is.na(sc$pkg) && length(sc$year) == 1L && is.finite(sc$year))
    sprintf(" %s package %s %s %d", dot, sc$pkg, dot, sc$year) else ""
  hz <- if (is.finite(res$fs)) sprintf("%g Hz", res$fs) else "rate unknown"
  .log_detail(lvl, sprintf("input: %s rows %s %s%s", .formatLargeNumber(res$n_rows), dot, hz, key))

  if (identical(res$status, "as-recorded"))
    .log_detail(lvl, sprintf("calibration: not needed %s speed recorded by the logger", dot))
  else
    .log_detail(lvl, sprintf("calibration: %.4f m/s per Hz %s %s", res$slope, dot,
                             .paddleSourceLabel(res$slope_source, long = TRUE)))

  if (!is.null(res$speed))
    .log_subdetail_aligned(lvl, sprintf("speed:   median %.2f m/s (%.2f \u2013 %.2f)",
                                        res$speed[["med"]], res$speed[["lo"]], res$speed[["hi"]]))
  else
    .log_subdetail_aligned(lvl, "speed:   no finite values")
  .log_ok(lvl, id, " processed")
  invisible(NULL)
}

#' The calibration roll-up, as a table: one row per tag and season.
#'
#' The in-situ columns appear only when there is an in-situ estimate to show, so a run without
#' `validate` gets a three-column provenance table rather than two columns of dashes.
#' @keywords internal
#' @noRd
.paddleCalTable <- function(cal) {
  num <- function(x, d = 4) ifelse(is.finite(x), formatC(x, format = "f", digits = d), "-")
  tab <- data.frame(Tag = sprintf("%s / pkg %s", cal$year, cal$package_id),
                    Slope = num(cal$slope),
                    Source = vapply(cal$slope_source, .paddleSourceLabel, character(1),
                                    long = FALSE, USE.NAMES = FALSE),
                    stringsAsFactors = FALSE, check.names = FALSE)
  tab$Source[is.na(tab$Source)] <- "-"
  if (any(is.finite(cal$in_situ_slope))) {
    tab[["In situ"]] <- num(cal$in_situ_slope)
    agr <- ifelse(is.finite(cal$agreement), formatC(cal$agreement, format = "f", digits = 2), "-")
    # the flag rides in the agreement column: it is a statement ABOUT that number, and a separate
    # column of mostly-blank cells would cost a column's width to say the same thing
    tab[["Agreement"]] <- ifelse(cal$flag %in% TRUE, paste0(agr, " !"), agr)
  }
  tab[["Deployments"]] <- format(cal$n_deployments)
  tab
}

#' The SUMMARY block: the outcome tally, then the calibration behind it, then where it was written.
#'
#' The calibration belongs here rather than in the header because it is a RESULT - one slope per tag and
#' season, resolved by looking at the cohort - and nautilus headers carry settings only. It is also a
#' cohort-level fact, so it reads once at the end rather than being repeated under every deployment.
#' @keywords internal
#' @noRd
.reportPaddleCohort <- function(lvl, cal, statuses, agreement.threshold, output.dir, plot.file) {
  if (lvl < 1L) return(invisible(NULL))
  n <- length(statuses)
  n_speed <- sum(statuses %in% c("applied", "as-recorded"))
  .log_done(lvl, sprintf("%d of %d deployment%s given a speed", n_speed, n, if (n != 1) "s" else ""))

  # A mutually exclusive tally in a fixed order, so the rows visibly sum to the cohort and the block
  # looks the same on every run. "as-recorded" is one of the ways a deployment gets a speed, so it is
  # listed as its own outcome rather than counted twice.
  labs <- c(applied = "Speed calculated", `as-recorded` = "Speed as recorded",
            `no calibration` = "No calibration", `no paddle data` = "No paddle data",
            `no paddle wheel` = "No paddle wheel")
  tally <- table(factor(statuses, levels = names(labs)))
  keep <- as.integer(tally) > 0L
  if (any(keep)) {
    rows <- stats::setNames(as.integer(tally)[keep], unname(labs[names(labs)[keep]]))
    .log_section(lvl, "Deployments")
    .log_rows(lvl, rows, symbols = c(cli::col_green(cli::symbol$tick),
                                     rep(cli::symbol$bullet, max(0L, sum(keep) - 1L))))
  }

  if (nrow(cal)) {
    .log_section(lvl, "Calibration")
    .log_table(lvl, .paddleCalTable(cal))
    if (any(cal$flag %in% TRUE)) {
      w <- which(cal$flag %in% TRUE)
      cli::cli_text("")
      .log_attention(lvl, sprintf(
        "%d calibration%s by more than %g%% from the in-situ estimate: %s",
        length(w), if (length(w) != 1) "s differ" else " differs", 100 * agreement.threshold,
        paste(sprintf("%s/pkg %s", cal$year[w], cal$package_id[w]), collapse = ", ")))
    }
  }

  out_rows <- c(if (!is.null(output.dir)) c(Directory = output.dir),
                if (!is.null(plot.file)) c(Plots = plot.file))
  if (length(out_rows)) { .log_section(lvl, "Output"); .log_rows(lvl, out_rows) }
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
