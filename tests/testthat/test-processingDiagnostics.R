# The opt-in processTagData() diagnostic renderer. Step 1: the magnetometer panel, which REUSES
# calibrateMagnetometer's .plotMagCalIndividual (raw cloud -> corrected sphere), fed by .captureMagDiag.
# The gather/render split is tested here in isolation (no processTagData run needed).

.mkMagCloud <- function(n = 5000, hard = c(5, -3, 2), soft = c(1.2, 0.8, 1.0), seed = 1) {
  set.seed(seed)
  ph <- stats::runif(n, 0, 2 * pi); th <- acos(stats::runif(n, -1, 1))
  B <- cbind(sin(th) * cos(ph), sin(th) * sin(ph), cos(th)) * 50   # a well-covered sphere (radius 50)
  sweep(B %*% diag(soft), 2, hard, "+")                            # inject hard + (diagonal) soft iron
}
.grav <- function(n) matrix(rep(c(0, 0, -1), each = n), n, 3)

test_that(".captureMagDiag returns a decimated bundle and the applied transform sphere-izes the cloud", {
  raw <- .mkMagCloud(); n <- nrow(raw)
  b <- nautilus:::.captureMagDiag(raw, .grav(n), center = c(5, -3, 2), soft_iron = diag(1 / c(1.2, 0.8, 1.0)),
                                  coverage_ok = TRUE, source = "inline hard+soft-iron",
                                  igrf = list(intensity = 50, inclination = 60), fs = 10, target.n = 2000L)
  expect_true(is.list(b))
  expect_lt(nrow(b$cloud$mag), n)                         # decimated (stride-based, ~n/stride points)
  expect_lte(nrow(b$cloud$mag), 3000L)
  expect_equal(ncol(b$cloud$mag), 3L)
  expect_true(b$applied)
  expect_lt(b$radcv, 0.02)                               # a correctly-calibrated cloud is ~spherical
  expect_true(is.numeric(b$coverage) && b$coverage > 0.8)   # a full sphere -> high equal-area coverage fraction
})

test_that(".captureMagDiag reports the TRUE sphere-coverage fraction (not the coverage_ok gate)", {
  set.seed(3); n <- 4000
  ph <- stats::runif(n, 0, 2 * pi); th <- acos(stats::runif(n, 0.6, 1))   # a patch near one pole -> poor coverage
  raw <- sweep(cbind(sin(th) * cos(ph), sin(th) * sin(ph), cos(th)) * 50, 2, c(5, -3, 2), "+")
  b <- nautilus:::.captureMagDiag(raw, .grav(n), center = c(5, -3, 2), soft_iron = diag(3),
                                  coverage_ok = TRUE, source = "inline", igrf = NULL, fs = 10)
  expect_true(is.numeric(b$coverage))
  expect_lt(b$coverage, 0.6)                              # a thin band is NOT 100% covered (the old bug showed 1.0)
})

test_that("a not-applied (identity) transform leaves the uncorrected band visible (higher radCV)", {
  raw <- .mkMagCloud()
  b <- nautilus:::.captureMagDiag(raw, .grav(nrow(raw)), center = c(0, 0, 0), soft_iron = diag(3),
                                  coverage_ok = FALSE, source = "no calibration", igrf = NULL, fs = 10)
  expect_false(b$applied)                                # identity encodes "nothing applied"
  expect_gt(b$radcv, 0.05)                               # the uncorrected ellipsoid is non-spherical
})

test_that(".captureMagDiag returns NULL when there is too little magnetometer data", {
  expect_null(nautilus:::.captureMagDiag(matrix(NA_real_, 50, 3), NULL, c(0, 0, 0), diag(3), FALSE, "x", NULL))
})

test_that(".renderProcessingDiagnostic writes a multi-page PDF reusing the mag detail page", {
  raw <- .mkMagCloud()
  b <- nautilus:::.captureMagDiag(raw, .grav(nrow(raw)), center = c(5, -3, 2), soft_iron = diag(1 / c(1.2, 0.8, 1.0)),
                                  coverage_ok = TRUE, source = "inline",
                                  igrf = list(intensity = 50, inclination = 60), fs = 10)
  bundles <- list(list(id = "A", paddle = FALSE, mag = b), list(id = "B", paddle = FALSE, mag = b))
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f), add = TRUE)
  nautilus:::.renderProcessingDiagnostic(bundles, plot = FALSE, plot.file = f)
  expect_true(file.exists(f) && file.size(f) > 0)
})

test_that(".renderProcessingDiagnostic is a no-op (no device opened) on empty bundles", {
  f <- file.path(tempdir(), "should_not_exist_proc_diag.pdf")
  if (file.exists(f)) unlink(f)
  expect_null(nautilus:::.renderProcessingDiagnostic(list(), plot.file = f))
  expect_false(file.exists(f))                            # returns before opening any device
})

# --- depth-drift panel -------------------------------------------------------------------------------
.mkDrift <- function(n = 3000, seed = 4) {
  set.seed(seed)
  t0 <- as.POSIXct("2020-01-01", tz = "UTC"); dtv <- t0 + seq_len(n)
  drift <- seq(0, 2, length.out = n)                                  # +2 m slow zero-offset drift
  raw <- pmax(0, 10 + 8 * sin(seq_len(n) / 300)) + drift
  raw[201:260] <- drift[201:260]; raw[(n - 399):(n - 340)] <- drift[(n - 399):(n - 340)]   # two surfacings
  dry <- data.frame(datetime = t0 + c(200, 260, n - 400, n - 340), dry = c(TRUE, FALSE, TRUE, FALSE))
  list(raw = raw, dtv = dtv, dry = dry)
}

test_that(".captureDepthDiag decimates and the offset reconstructs exactly (raw - corrected)", {
  x <- .mkDrift()
  dr <- nautilus:::.correctDepthDrift(x$raw, x$dtv, dry = x$dry)
  d <- nautilus:::.captureDepthDiag(x$raw, x$dtv, dr, target.n = 1000L)
  expect_true(is.list(d))
  expect_lt(length(d$t), length(x$raw))                              # decimated
  expect_equal(d$raw - d$corrected, d$offset, tolerance = 1e-9)      # the offset IS the raw/corrected gap
  expect_true(d$status %in% c("applied", "applied_with_gaps", "constant_offset"))
  expect_gt(nrow(d$anchors), 0L)
  expect_true(all(d$anchors$source %in% c("dry", "gps", "depth")))
})

test_that(".captureDepthDiag flags the abstained state and returns NULL when the correction is disabled", {
  n <- 500; dtv <- as.POSIXct("2020-01-01", tz = "UTC") + seq_len(n)
  d <- nautilus:::.captureDepthDiag(rep(5, n), dtv, nautilus:::.correctDepthDrift(rep(5, n), dtv))
  expect_equal(d$status, "abstained")                                # no surface evidence -> untouched
  expect_equal(nrow(d$anchors), 0L)
  dr0 <- nautilus:::.correctDepthDrift(rep(5, n), dtv, control = depthDriftControl(method = "none"))
  expect_null(nautilus:::.captureDepthDiag(rep(5, n), dtv, dr0))
})

test_that(".drawDepthDriftPanel renders both the applied and abstained states without error", {
  x <- .mkDrift()
  d_app <- nautilus:::.captureDepthDiag(x$raw, x$dtv, nautilus:::.correctDepthDrift(x$raw, x$dtv, dry = x$dry))
  d_abs <- nautilus:::.captureDepthDiag(rep(5, 500), as.POSIXct("2020-01-01", tz = "UTC") + seq_len(500),
                                        nautilus:::.correctDepthDrift(rep(5, 500), as.POSIXct("2020-01-01", tz = "UTC") + seq_len(500)))
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f), add = TRUE)
  grDevices::pdf(f)
  expect_no_error(nautilus:::.drawDepthDriftPanel(d_app))
  expect_no_error(nautilus:::.drawDepthDriftPanel(d_abs))
  grDevices::dev.off()
  expect_gt(file.size(f), 0)
})

# --- pitch/roll panels -------------------------------------------------------------------------------

test_that(".capturePitchDiag decimates the fit scatter and recovers the fitted line (intercept + slope)", {
  set.seed(6); vv <- stats::rnorm(5000, 0, 0.3)
  fd <- data.frame(vv_smooth = vv, pitch_rad = 0.1 + 0.8 * vv + stats::rnorm(5000, 0, 0.02))  # intercept 0.1 rad
  mdl <- stats::lm(pitch_rad ~ vv_smooth, data = fd)
  p <- nautilus:::.capturePitchDiag(fd, mdl, summary(mdl)$r.squared, TRUE, 0.5, 20, "pitch +5.7 (R2 0.99)")
  expect_true(is.list(p))
  expect_lt(length(p$vv), nrow(fd))                                # decimated
  expect_equal(length(p$vv), length(p$pitch_deg))
  expect_equal(p$intercept_deg, 0.1 * 180 / pi, tolerance = 0.5)   # intercept recovered in degrees
  expect_equal(p$slope_deg, 0.8 * 180 / pi, tolerance = 1)         # slope recovered (deg per m/s)
  expect_true(p$applied)
})

test_that(".capturePitchDiag keeps the scatter + NA line when the fit is degenerate (NULL model); NULL fit_data -> NULL", {
  fd <- data.frame(vv_smooth = rep(0, 120), pitch_rad = rep(0.2, 120))
  p <- nautilus:::.capturePitchDiag(fd, NULL, NA_real_, FALSE, 0.5, 20, "insufficient diving signal")
  expect_true(is.na(p$slope_deg) && is.na(p$intercept_deg))
  expect_gt(length(p$vv), 0L)
  expect_false(p$applied)
  expect_null(nautilus:::.capturePitchDiag(fd[0, ], NULL, NA, FALSE, 0.5, 20, "x"))
})

test_that(".captureRollDiag decimates and carries the median/threshold; empty -> NULL", {
  r <- nautilus:::.captureRollDiag(stats::rnorm(5000, 3, 5), 3.1, TRUE, 20)
  expect_true(is.list(r)); expect_lt(length(r$level_roll), 5000L)
  expect_equal(r$median_deg, 3.1); expect_true(r$applied)
  expect_null(nautilus:::.captureRollDiag(numeric(0), NA, FALSE, 20))
  expect_null(nautilus:::.captureRollDiag(c(NA, NaN), NA, FALSE, 20))
})

test_that("the pitch/roll/empty panels render without error (applied, gated, and absent)", {
  set.seed(7); vv <- stats::rnorm(600, 0, 0.3)
  fd <- data.frame(vv_smooth = vv, pitch_rad = 0.1 + 0.8 * vv + stats::rnorm(600, 0, 0.02))
  mdl <- stats::lm(pitch_rad ~ vv_smooth, data = fd)
  p_app  <- nautilus:::.capturePitchDiag(fd, mdl, summary(mdl)$r.squared, TRUE, 0.5, 20, "applied")
  p_gate <- nautilus:::.capturePitchDiag(fd, mdl, 0.1, FALSE, 0.5, 20, "weak fit")
  r_diag <- nautilus:::.captureRollDiag(stats::rnorm(600, 3, 5), 3.0, TRUE, 20)
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f), add = TRUE)
  grDevices::pdf(f)
  expect_no_error(nautilus:::.drawPitchScatterPanel(p_app))
  expect_no_error(nautilus:::.drawPitchScatterPanel(p_gate))
  expect_no_error(nautilus:::.drawPitchScatterPanel(NULL))         # -> empty panel
  p_inf <- list(vv = rep(Inf, 100), pitch_deg = rep(Inf, 100), slope_deg = NA, intercept_deg = NA,
                r2 = NA, applied = FALSE, min_r2 = 0.5, threshold_deg = 20, note = "x")
  expect_no_error(nautilus:::.drawPitchScatterPanel(p_inf))        # non-finite range -> empty panel, not a crash
  expect_no_error(nautilus:::.drawRollPanel(r_diag))
  expect_no_error(nautilus:::.drawRollPanel(NULL))
  grDevices::dev.off()
  expect_gt(file.size(f), 0)
})

# --- fleet summary page ------------------------------------------------------------------------------

test_that(".processingDiagRecord reduces a bundle to scalars; .drawProcessingSummaryPage renders", {
  t0 <- as.POSIXct("2020-01-01", tz = "UTC"); dtv <- t0 + seq_len(2000)
  drift <- seq(0, 1, length.out = 2000)
  raw <- pmax(0, 10 + 8 * sin(seq_len(2000) / 300)) + drift
  raw[100:180] <- drift[100:180]; raw[1800:1880] <- drift[1800:1880]   # the animal IS at the surface during the dry intervals
  dd <- nautilus:::.captureDepthDiag(raw, dtv, nautilus:::.correctDepthDrift(raw, dtv,
        dry = data.frame(datetime = t0 + c(100, 180, 1800, 1880), dry = c(TRUE, FALSE, TRUE, FALSE))))
  set.seed(9); vv <- stats::rnorm(3000, 0, 0.3)
  fd <- data.frame(vv_smooth = vv, pitch_rad = 0.1 + 0.8 * vv + stats::rnorm(3000, 0, 0.02))
  mdl <- stats::lm(pitch_rad ~ vv_smooth, data = fd)
  b <- list(id = "A", paddle = FALSE, mag = NULL, depth = dd,
            pitchroll = list(pitch = nautilus:::.capturePitchDiag(fd, mdl, summary(mdl)$r.squared, TRUE, 0.5, 20, "ok"),
                             roll  = nautilus:::.captureRollDiag(stats::rnorm(3000, 3, 5), 3, TRUE, 20)))
  rec <- nautilus:::.processingDiagRecord(b)
  expect_equal(rec$id, "A")
  expect_true(rec$depth_status %in% c("applied", "applied_with_gaps", "constant_offset"))
  expect_true(isTRUE(rec$pitch_applied))
  expect_equal(rec$mag_source, "no magnetometer")                # mag panel absent on this bundle
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f), add = TRUE)
  grDevices::pdf(f); expect_no_error(nautilus:::.drawProcessingSummaryPage(list(b, b))); grDevices::dev.off()
  expect_gt(file.size(f), 0)
})

# --- end-to-end through processTagData() -------------------------------------------------------------
.mkMagTag <- function(n = 2500, fs = 10, seed = 2) {
  set.seed(seed)
  t0 <- as.POSIXct("2020-01-01", tz = "UTC")
  ph <- stats::runif(n, 0, 2 * pi); th <- acos(stats::runif(n, -1, 1))
  S <- cbind(sin(th) * cos(ph), sin(th) * sin(ph), cos(th)) * 50   # well-covered sphere -> calibration applies
  dt <- data.table::data.table(                                                       # varying posture (non-degenerate)
    ID = "A", datetime = t0 + (seq_len(n) - 1) / fs,
    ax = sin(seq_len(n) / 50) * 0.2, ay = cos(seq_len(n) / 70) * 0.2, az = -1 + stats::rnorm(n, 0, 0.02),
    depth = pmax(0, 20 + 15 * sin(seq_len(n) / 300)),
    mx = S[, 1] * 1.2 + 5, my = S[, 2] * 0.8 - 3, mz = S[, 3] + 2)                    # hard + soft iron injected
  m <- nautilus:::.newNautilusMeta(); m$id <- "A"; m$tag$paddle_wheel <- FALSE
  m$deployment$lat <- 38; m$deployment$lon <- -25; m$deployment$datetime <- t0
  nautilus:::new_nautilus_tag(dt, m)
}

test_that("processTagData(plot.file=) writes the diagnostic PDF; the headless default does not plot", {
  tag <- .mkMagTag()
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f), add = TRUE)
  out <- suppressWarnings(suppressMessages(processTagData(list(A = tag), plot.file = f, verbose = FALSE)))
  expect_length(out, 1L)                                  # processing result unchanged
  expect_true(file.exists(f) && file.size(f) > 0)         # diagnostic rendered
  # headless default: returns data, opens no device (dev.list unchanged)
  before <- grDevices::dev.list()
  out2 <- suppressWarnings(suppressMessages(processTagData(list(A = tag), verbose = FALSE)))
  expect_length(out2, 1L)
  expect_identical(grDevices::dev.list(), before)
})

test_that("plot.file must be a .pdf path (writable-file validation)", {
  tag <- .mkMagTag(n = 500)
  expect_error(suppressMessages(processTagData(list(A = tag), plot.file = file.path(tempdir(), "x.txt"),
                                               verbose = FALSE)), "pdf", ignore.case = TRUE)
})
