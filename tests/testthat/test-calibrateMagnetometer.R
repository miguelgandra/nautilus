# Tests for the magnetometer hard/soft-iron calibration engine (.ellipsoidFit / .diagFit / .calibrateMag)
# and the exported calibrateMagnetometer() (per-tag + per-package pooling, QC, stored-not-applied).

# a well-covered sphere of true field directions (magnitude 50)
.magSphere <- function(n = 4000, seed = 1) {
  set.seed(seed)
  ph <- stats::runif(n, 0, 2 * pi); th <- acos(stats::runif(n, -1, 1))
  cbind(sin(th) * cos(ph), sin(th) * sin(ph), cos(th)) * 50
}
# wrap a raw mag cloud (+ optional injected hard/soft-iron) into a nautilus_tag. The deployment sits on
# the magnetic equator (lon -25, lat 11 -> IGRF inclination ~0 deg), so an isotropic corrected cloud (dip
# ~0 vs the constant up-gravity below) matches IGRF and a well-covered fit earns "high" confidence via a
# genuine dip check; a band still fails on coverage. (A poorly-covered or dip-less fit is capped at
# "medium" - see .magConfidence.)
.magTag <- function(id, B, off = c(5, -3, 2), sc = c(1.2, 0.8, 1.0), pkg = NA_character_, paddle = FALSE) {
  n <- nrow(B)
  dt <- data.table::data.table(
    ID = id, datetime = as.POSIXct("2019-09-11 12:00:00", tz = "UTC") + seq_len(n), depth = 1,
    ax = 0, ay = 0, az = 1,
    mx = B[, 1] * sc[1] + off[1], my = B[, 2] * sc[2] + off[2], mz = B[, 3] * sc[3] + off[3])
  m <- nautilus:::.newNautilusMeta(); m$id <- id
  if (!is.na(pkg)) m$tag$package_id <- pkg
  m$tag$paddle_wheel <- paddle
  m$deployment$lon <- -25; m$deployment$lat <- 11
  m$deployment$datetime <- as.POSIXct("2019-09-11 12:00:00", tz = "UTC")
  nautilus:::new_nautilus_tag(dt, m)
}

# calibrateMagnetometer writes the ESTIMATE into meta$mag_calibration$proposed (nested contract). This
# flattens that estimate's params/qc/provenance (+ the top-level applied/status/proposed) so the
# assertions can read fields by name (center, confidence, source, radcv, ...).
mcal <- function(x) {
  mc <- tagMetadata(x)$mag_calibration
  c(mc$proposed$params, mc$proposed$qc, mc$proposed$provenance,
    list(applied = mc$applied, status = mc$status, proposed = mc$proposed))
}

# ---- engine ---------------------------------------------------------------------------

# a nearly-planar magnetometer cloud (a flat x-y disk): does not enclose a 3-D ellipsoid, so the
# ellipsoid fit is non-positive-definite / ill-conditioned - the under-determined field case.
.magDisk <- function(n = 3000, seed = 3) {
  set.seed(seed)
  rr <- sqrt(stats::runif(n)); aa <- stats::runif(n, 0, 2 * pi)
  cbind(rr * cos(aa) * 50, rr * sin(aa) * 50, stats::rnorm(n, 0, 0.3))
}

test_that(".ellipsoidFit recovers a hard + full cross-axis soft-iron; guards non-enclosing clouds", {
  B <- .magSphere()
  A <- matrix(c(1.15, 0.12, 0.05, 0.12, 0.85, -0.08, 0.05, -0.08, 1.05), 3, byrow = TRUE)   # SPD
  off <- c(4, -6, 3)
  M <- sweep(B %*% t(A), 2, off, "+")
  ef <- nautilus:::.ellipsoidFit(M)
  expect_true(isTRUE(ef$ok))
  expect_equal(ef$center, off, tolerance = 1)
  expect_lt(nautilus:::.magRadcv(nautilus:::.applyMagCal(M, ef$center, ef$soft_iron)), 0.01)   # corrected to a sphere
  # a nearly-planar cloud does not enclose an ellipsoid -> weak flag, not a fit
  wf <- nautilus:::.ellipsoidFit(sweep(.magDisk(), 2, off, "+"))
  expect_false(isTRUE(wf$ok))
})

test_that(".ellipsoidFit stays conditioned under a large hard-iron offset (pre-centering)", {
  B <- .magSphere()
  A <- matrix(c(1.15, 0.12, 0.05, 0.12, 0.85, -0.08, 0.05, -0.08, 1.05), 3, byrow = TRUE)
  off <- c(1500, -900, 1200)                                   # offset >> field radius (raw-ADC-like scale)
  ef <- nautilus:::.ellipsoidFit(sweep(B %*% t(A), 2, off, "+"))
  expect_true(isTRUE(ef$ok))                                    # not spuriously abandoned by ill-conditioning
  expect_equal(ef$center, off, tolerance = 1)                   # centre still recovered
})

test_that(".calibrateMag: a non-enclosing (planar) cloud gets the regularized 2D fallback, never a reject", {
  cal <- nautilus:::.calibrateMag(sweep(.magDisk() %*% diag(c(1.2, 0.8, 1)), 2, c(5, -3, 2), "+"))
  expect_equal(cal$status, "calibrated_2d_fallback")           # the ridge NEVER rejects; flagged, not dropped
  expect_match(cal$method_used, "2d fallback")
  expect_false(isTRUE(cal$coverage_ok))                        # still an under-covered fit
  expect_true(is.finite(cal$center[1]))                        # but a usable centre was produced
})

test_that(".calibrateMag keeps the native field magnitude and reports the dip residual", {
  B <- .magSphere()
  M <- sweep(B %*% diag(c(1.2, 0.8, 1)), 2, c(5, -3, 2), "+")
  cal <- nautilus:::.calibrateMag(M, method = "ellipsoid", target.radius = NA_real_)
  Mc <- nautilus:::.applyMagCal(M, cal$center, cal$soft_iron)
  expect_equal(stats::median(nautilus:::.rowNorm3(Mc)), 50, tolerance = 3)   # native ~50 preserved
})

# ---- exported function ----------------------------------------------------------------

test_that("calibrateMagnetometer stores a per-tag calibration and does NOT touch mx/my/mz", {
  tg <- .magTag("A01", .magSphere())
  raw_mx <- tg$mx
  out <- calibrateMagnetometer(list(A01 = tg), verbose = FALSE)
  cal <- mcal(out$A01)
  expect_equal(cal$source, "per_tag")
  expect_equal(cal$center, c(5, -3, 2), tolerance = 1)         # hard-iron recovered
  expect_lt(cal$radcv, 0.05)
  expect_equal(cal$confidence, "high")
  expect_false(isTRUE(cal$applied))                            # stored, not applied
  expect_equal(out$A01$mx, raw_mx)                             # raw channel untouched
  expect_equal(dim(cal$soft_iron), c(3L, 3L))
})

test_that("a full-azimuth thin band earns medium confidence (hard-iron heading is trustworthy)", {
  big <- .magSphere(30000)
  band <- big[abs(big[, 3]) < 8, , drop = FALSE][1:4000, ]
  out <- calibrateMagnetometer(list(B1 = .magTag("B1", band)), verbose = FALSE)
  cal <- mcal(out$B1)
  expect_false(isTRUE(cal$coverage_ok))                 # under-covered perpendicular (a thin band)
  # a full swept azimuth determines the in-plane hard-iron centre -> trustworthy HEADING (GPS dead-reckoning
  # CV), even though the perpendicular/soft-iron is unobservable; capped at medium (never full-3D "high").
  expect_equal(cal$confidence, "medium")
})

test_that("per-package pooling lifts complementary bands from medium to high confidence", {
  big <- .magSphere(30000)
  b1 <- big[abs(big[, 3]) < 12, , drop = FALSE][1:4000, ]      # poor z coverage
  b2 <- big[abs(big[, 2]) < 12, , drop = FALSE][1:4000, ]      # poor y coverage (complementary)
  p1 <- .magTag("P1", b1, pkg = "PKG1"); p2 <- .magTag("P2", b2, pkg = "PKG1")
  alone <- mcal(calibrateMagnetometer(list(P1 = p1), verbose = FALSE)$P1)
  expect_equal(alone$confidence, "medium")                     # each band alone: in-plane hard-iron OK (medium),
                                                               # but soft-iron under-determined -> not yet high
  pooled <- calibrateMagnetometer(list(P1 = p1, P2 = p2), group.by = "package_id", verbose = FALSE)
  cp <- mcal(pooled$P1)
  expect_equal(cp$source, "per_package")
  expect_equal(cp$group, "PKG1")
  expect_equal(cp$n_deployments, 2L)
  expect_true(isTRUE(cp$coverage_ok))                          # pooling widened the coverage
  expect_lt(cp$radcv, 0.1)
  expect_equal(cp$confidence, "high")
})

test_that("paddle-wheel and non-paddle deployments of one package are NOT pooled together", {
  big <- .magSphere(30000)
  b1 <- big[abs(big[, 3]) < 12, , drop = FALSE][1:4000, ]
  b2 <- big[abs(big[, 2]) < 12, , drop = FALSE][1:4000, ]
  p1 <- .magTag("P1", b1, pkg = "PKG1", paddle = FALSE)
  p2 <- .magTag("P2", b2, pkg = "PKG1", paddle = TRUE)               # same package, but carries a paddle wheel
  out <- calibrateMagnetometer(list(P1 = p1, P2 = p2), group.by = "package_id", verbose = FALSE)
  # different ferromagnetic environments -> separate groups -> each calibrated on its own, never pooled
  expect_equal(mcal(out$P1)$source, "per_tag")
  expect_equal(mcal(out$P2)$source, "per_tag")
  # a matched pair (both paddle) of the same package DOES pool
  p2b <- .magTag("P2", b2, pkg = "PKG1", paddle = FALSE)
  out2 <- calibrateMagnetometer(list(P1 = p1, P2 = p2b), group.by = "package_id", verbose = FALSE)
  expect_equal(mcal(out2$P1)$source, "per_package")
})

test_that("group.by accepts a composite key and pools per unique combination (like consensusAxisMapping)", {
  big <- .magSphere(30000)
  b1 <- big[abs(big[, 3]) < 12, , drop = FALSE][1:4000, ]
  b2 <- big[abs(big[, 2]) < 12, , drop = FALSE][1:4000, ]                     # complementary band
  wl <- function(tag, lg) { m <- nautilus:::.getMeta(tag); m$tag$logger_id <- lg
                            data.table::setattr(tag, "nautilus", m); tag }

  a <- wl(.magTag("A", b1, pkg = "PKG1"), "LOGA")
  b <- wl(.magTag("B", b2, pkg = "PKG1"), "LOGB")                             # same package, DIFFERENT logger
  # single key -> shared package_id -> pooled
  s <- mcal(calibrateMagnetometer(list(A = a, B = b), group.by = "package_id", verbose = FALSE)$A)
  expect_equal(s$source, "per_package"); expect_equal(s$n_deployments, 2L)
  # composite key -> PKG1|LOGA vs PKG1|LOGB are distinct -> NOT pooled
  cmp <- mcal(calibrateMagnetometer(list(A = a, B = b), group.by = c("package_id", "logger_id"), verbose = FALSE)$A)
  expect_equal(cmp$source, "per_tag")

  # same package AND same logger -> the composite key pools them together
  b_same <- wl(.magTag("B", b2, pkg = "PKG1"), "LOGA")
  cc <- mcal(calibrateMagnetometer(list(A = a, B = b_same), group.by = c("package_id", "logger_id"), verbose = FALSE)$A)
  expect_equal(cc$source, "per_package"); expect_equal(cc$n_deployments, 2L)
})

test_that("group.by rejects unknown fields (shared validation)", {
  expect_error(calibrateMagnetometer(list(S = .magTag("S", .magSphere())), group.by = "nonsense", verbose = FALSE),
               "must be one or more")
})

test_that("a magnetometer-less tag is handled gracefully (no crash, no calibration)", {
  dt <- data.table::data.table(ID = "N1", datetime = as.POSIXct("2020-01-01", tz = "UTC") + 1:100,
                               depth = 1, ax = 0, ay = 0, az = 1)
  m <- nautilus:::.newNautilusMeta(); m$id <- "N1"
  tg <- nautilus:::new_nautilus_tag(dt, m)
  out <- expect_no_error(calibrateMagnetometer(list(N1 = tg), verbose = FALSE))
  expect_null(mcal(out$N1)$proposed)                             # no magnetometer -> nothing proposed
})

test_that("file-path input round-trips and persists $mag_calibration", {
  d <- file.path(tempdir(), paste0("mc_", as.integer(runif(1, 1, 1e7))))
  dir.create(d, showWarnings = FALSE); on.exit(unlink(d, recursive = TRUE), add = TRUE)
  saveRDS(.magTag("Z9", .magSphere()), file.path(d, "Z9.rds"))
  out <- calibrateMagnetometer(file.path(d, "Z9.rds"), output.dir = d,
                               output.suffix = "-cal", verbose = FALSE)
  expect_named(out, "Z9")
  saved <- readRDS(file.path(d, "Z9-cal.rds"))
  expect_equal(mcal(saved)$source, "per_tag")
  expect_false(isTRUE(mcal(saved)$applied))
})

test_that("input validation errors are clear", {
  tg <- .magTag("A01", .magSphere(200))
  expect_error(calibrateMagnetometer(list(A01 = tg), return.data = FALSE), "output.dir")
  expect_error(calibrateMagnetometer(list(A01 = tg), control = magCalibrationControl(method = "bogus")))
  expect_error(calibrateMagnetometer(list(A01 = tg), plot.file = "/no/such/dir/report.pdf"))
})

test_that("detailed console output puts the calibration method and the QC metrics on separate lines", {
  out <- cli::cli_fmt(calibrateMagnetometer(list(PIN_CAM_21 = .magTag("PIN_CAM_21", .magSphere())),
                                            verbose = "detailed"))
  metric_line <- grep("radCV", out, value = TRUE)
  method_line <- grep("per_tag", out, value = TRUE)                 # source + method, its own detail line
  expect_length(metric_line, 1L)
  expect_length(method_line, 1L)
  expect_match(metric_line, "confidence")                          # metrics + confidence grouped together
  expect_false(grepl("per_tag|per_package", metric_line))          # method no longer shares the metrics line
  expect_false(grepl("radCV", method_line))                        # and vice versa (the two-line split)
})

# ---- diagnostic report -----------------------------------------------------------------

# count pages in an R-generated PDF without a heavy dependency (mirrors the regularize test helper)
.pdf_pages <- function(f) {
  b <- readBin(f, "raw", file.info(f)$size)
  length(grepRaw(charToRaw("/Type /Page"), b, all = TRUE)) -
    length(grepRaw(charToRaw("/Type /Pages"), b, all = TRUE))
}
.magBand <- function(id, seed = 2, pkg = NA, paddle = FALSE) {          # under-sampled -> low confidence
  big <- .magSphere(30000, seed); band <- big[abs(big[, 3]) < 8, , drop = FALSE][1:4000, ]
  .magTag(id, band, pkg = pkg, paddle = paddle)
}

test_that("plot.file draws a summary page plus a detail page per flagged (low/medium) deployment", {
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f), add = TRUE)
  tags <- list(GOOD = .magTag("GOOD", .magSphere()), BAND = .magBand("BAND"))
  out <- calibrateMagnetometer(tags, plot.file = f, verbose = FALSE)
  expect_equal(mcal(out$GOOD)$confidence, "high")
  expect_equal(mcal(out$BAND)$confidence, "medium")     # full-azimuth thin band -> hard-iron trustworthy
  expect_true(file.exists(f) && file.size(f) > 0)
  expect_equal(.pdf_pages(f), 2L)                       # summary + BAND (medium, flagged); GOOD (high) gets no page
})

test_that("force.plots draws a detail page for every deployment; all-high draws summary only", {
  f1 <- tempfile(fileext = ".pdf"); f2 <- tempfile(fileext = ".pdf")
  on.exit({ unlink(f1); unlink(f2) }, add = TRUE)
  tags <- list(GOOD = .magTag("GOOD", .magSphere()), BAND = .magBand("BAND"))
  calibrateMagnetometer(tags, plot.file = f1, force.plots = TRUE, verbose = FALSE)
  expect_equal(.pdf_pages(f1), 3L)                      # summary + 2 detail
  # two well-covered tags -> both high -> no flagged detail pages -> summary only
  hi <- list(H1 = .magTag("H1", .magSphere(4000, 1)), H2 = .magTag("H2", .magSphere(4000, 5)))
  calibrateMagnetometer(hi, plot.file = f2, verbose = FALSE)
  expect_equal(.pdf_pages(f2), 1L)
})

test_that("a single-deployment run always gets its detail page (even at high confidence)", {
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f), add = TRUE)
  calibrateMagnetometer(list(S = .magTag("S", .magSphere())), plot.file = f, verbose = FALSE)
  expect_equal(.pdf_pages(f), 2L)                       # summary + the lone deployment
})

test_that("the report renders with a magnetometer-less deployment in the run (no crash)", {
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f), add = TRUE)
  dt <- data.table::data.table(ID = "N1", datetime = as.POSIXct("2020-01-01", tz = "UTC") + 1:100,
                               depth = 1, ax = 0, ay = 0, az = 1)
  m <- nautilus:::.newNautilusMeta(); m$id <- "N1"; nomag <- nautilus:::new_nautilus_tag(dt, m)
  tags <- list(BAND = .magBand("BAND"), N1 = nomag)
  expect_no_error(calibrateMagnetometer(tags, plot.file = f, verbose = FALSE))
  expect_gte(.pdf_pages(f), 1L)                         # a summary listing both, plus BAND's detail
})

test_that("plot = TRUE renders to the active device without error", {
  grDevices::pdf(NULL); on.exit(grDevices::dev.off(), add = TRUE)
  expect_no_error(calibrateMagnetometer(list(BAND = .magBand("BAND")), plot = TRUE, verbose = FALSE))
})

test_that(".magCalCoverage separates a full sphere from a thin band", {
  expect_gt(nautilus:::.magCalCoverage(.magSphere(6000)), 0.9)
  big <- .magSphere(40000); band <- big[abs(big[, 3]) < 6, , drop = FALSE]
  expect_lt(nautilus:::.magCalCoverage(band), 0.4)
})

test_that("the detail-page dip reference line equals the stored dip residual (header/panel agree)", {
  M <- sweep(.magSphere() %*% diag(c(1.2, 0.8, 1.0)), 2, c(5, -3, 2), "+")
  grav <- matrix(rep(c(0, 0, 1), each = nrow(M)), ncol = 3)          # constant gravity direction
  fit <- nautilus:::.calibrateMag(M, grav = grav, igrf.incl = NA_real_)
  filled <- nautilus:::.fillMagCal(fit, source = "per_tag", group = "P", n = 1L)
  cov <- nautilus:::.magCalCoverage(sweep(M, 2, filled$center, "-"))
  p <- nautilus:::.magCalPayload("P", list(mag = M, grav = grav), filled, NULL, FALSE, coverage = cov)
  expect_equal(p$coverage, cov)                                    # coverage is the value passed in, not recomputed
  expect_true(is.finite(p$dip) && length(p$dips) > 100L)
  expect_equal(mean(p$dips), p$dip, tolerance = 1e-6)               # the panel line is the stored mean dip
  # drawer handles both a present and an absent gravity reference
  grDevices::pdf(NULL); on.exit(grDevices::dev.off(), add = TRUE)
  expect_no_error(nautilus:::.plotMagCalIndividual(p))
  p0 <- nautilus:::.magCalPayload("P", list(mag = M, grav = NULL), filled, NULL, FALSE, coverage = cov)
  expect_no_error(nautilus:::.plotMagCalIndividual(p0))             # dip-unavailable branch
})

test_that(".drawMagCalSummaryPage renders a mixed table (incl. a no-mag row) without error", {
  recs <- list(
    nautilus:::.magCalSummaryRecord("A", NULL, NA_real_),           # no-mag row
    nautilus:::.magCalSummaryRecord("B", list(confidence = "low", source = "per_tag", method = "ellipsoid",
      radcv = 0.02, igrf_residual = 8, n_deployments = 1L), 0.18),
    nautilus:::.magCalSummaryRecord("C", list(confidence = "high", source = "per_package", method = "diagonal",
      radcv = 0.05, igrf_residual = -1, n_deployments = 3L), 0.6))
  st <- list(n = 3L, n_cal = 2L, n_high = 1L, n_medium = 0L, n_low = 1L, n_nomag = 1L)
  grDevices::pdf(NULL); on.exit(grDevices::dev.off(), add = TRUE)
  expect_no_error(nautilus:::.drawMagCalSummaryPage(recs, st))
  expect_no_error(nautilus:::.drawMagCalSummaryPage(list(), st))    # empty run
})

test_that("the interpretation names the real cause and never gives a low verdict medium wording", {
  msg <- nautilus:::.magCalMessage
  base <- function(...) utils::modifyList(list(confidence = "low", coverage_ok = TRUE, radcv = 0.02,
                                               dip_resid = 2, dips = NULL, radcv_max = 0.1, dip_max = 15), list(...))
  # a LOW driven by a non-spherical corrected cloud (high radCV) must say 'do not trust', not MEDIUM's text
  low  <- msg(base(confidence = "low", radcv = 0.3))
  med  <- msg(base(confidence = "medium", radcv = 0.15))
  expect_match(low, "not spherical enough")
  expect_match(low, "do not trust")
  expect_false(grepl("usable but not clean", low))                 # LOW must NOT borrow MEDIUM's softer text
  expect_match(med, "usable but not clean")
  # non-spherical is reported BEFORE the dip (a bad fit makes the dip meaningless)
  expect_match(msg(base(confidence = "low", radcv = 0.66, dip_resid = -37)), "not spherical enough")
  # under-coverage and a dip-mismatch on a tight fit keep their specific explanations
  expect_match(msg(base(confidence = "low", coverage_ok = FALSE)), "patch of the sphere")
  expect_match(msg(base(confidence = "low", radcv = 0.04, dip_resid = 57)), "far from")
  # a widely-scattered dip is a caveat for flagged tags, but never contradicts a HIGH 'trustworthy' verdict
  scattered <- rnorm(500, 45, 35)
  expect_match(msg(base(confidence = "low", radcv = 0.3, dips = scattered)), "widely scattered")
  hi <- msg(base(confidence = "high", radcv = 0.02, dip_resid = 0, dips = scattered))
  expect_match(hi, "trustworthy")
  expect_false(grepl("widely scattered", hi))                      # no scary caveat glued onto 'trustworthy'
})

test_that("sphere coverage on the summary row and the detail page agree (single source)", {
  # a cloud larger than max.pts so thinning would otherwise change the coverage count
  big <- .magSphere(20000); grav <- matrix(rep(c(0, 0, 1), each = nrow(big)), ncol = 3)
  fit <- nautilus:::.calibrateMag(big, grav = grav, igrf.incl = NA_real_)
  filled <- nautilus:::.fillMagCal(fit, source = "per_tag", group = "P", n = 1L)
  cov_full <- nautilus:::.magCalCoverage(sweep(big, 2, filled$center, "-"))
  p <- nautilus:::.magCalPayload("P", list(mag = big, grav = grav), filled, NULL, FALSE,
                                 coverage = cov_full, max.pts = 3000L)
  expect_lt(nrow(p$raw), nrow(big))                                # the scatter WAS thinned
  expect_equal(p$coverage, cov_full)                               # but coverage is the un-thinned value
})

# ---- external calibration sources (calibration.data) ----------------------------------------------
# a thin equatorial band: enough to pin a hard-iron centre but too little coverage to trust the fit.
.magBandCloud <- function(n = 3000, seed = 7) {
  set.seed(seed)
  ph <- stats::runif(n, 0, 2 * pi); th <- acos(stats::runif(n, -0.15, 0.15))
  cbind(sin(th) * cos(ph), sin(th) * sin(ph), cos(th)) * 50
}

test_that("calibration.data = NULL is byte-identical to omitting it (golden regression)", {
  tg <- .magTag("T", .magBandCloud(), pkg = "PKG1")
  a  <- tagMetadata(calibrateMagnetometer(list(T = .magTag("T", .magBandCloud(), pkg = "PKG1")), verbose = FALSE)$T)$mag_calibration
  b  <- tagMetadata(calibrateMagnetometer(list(T = .magTag("T", .magBandCloud(), pkg = "PKG1")), calibration.data = NULL, verbose = FALSE)$T)$mag_calibration
  expect_equal(a, b)                                              # full nested state identical (proposed only; unprocessed)
})

test_that("an external source lifts an under-covered deployment from medium to high confidence", {
  tgt <- .magTag("TGT", .magBandCloud(), pkg = "PKG1")             # thin band -> hard-iron only (medium) alone
  src <- .magTag("SRC", .magSphere(4000), pkg = "PKG1")            # full sphere, SAME hard/soft-iron
  insitu <- mcal(calibrateMagnetometer(list(TGT = .magTag("TGT", .magBandCloud(), pkg = "PKG1")), verbose = FALSE)$TGT)
  expect_equal(insitu$confidence, "medium")                       # in-plane hard-iron OK; soft-iron under-determined
  out <- mcal(calibrateMagnetometer(list(TGT = tgt), calibration.data = list(SRC = src), verbose = FALSE)$TGT)
  expect_equal(out$source, "external")
  expect_equal(out$confidence, "high")                            # the source supplies the coverage
  expect_equal(out$center, c(5, -3, 2), tolerance = 1)            # hard-iron recovered
  expect_equal(out$source_ids, "SRC")
  expect_equal(out$center_source, "source")
  expect_lt(out$center_delta, 0.05)                               # agrees with the deployment's in-situ centre
  # and it actually spheres the deployment's own cloud
  M <- cbind(tgt$mx, tgt$my, tgt$mz)
  expect_lt(nautilus:::.magRadcv(nautilus:::.applyMagCal(M, out$center, out$soft_iron)), 0.01)
})

test_that("a magnetically contaminated source (centre far from in-situ) is rejected", {
  tgt  <- .magTag("TGT", .magBandCloud(), pkg = "PKG1")
  bad  <- .magTag("SRC", .magSphere(4000), off = c(30, -3, 2), pkg = "PKG1")   # +25 uT of 'boat iron' on x
  # default fallback: reject the source, fall back to the in-situ fit
  out <- mcal(calibrateMagnetometer(list(TGT = tgt), calibration.data = list(SRC = bad), verbose = FALSE)$TGT)
  expect_equal(out$source, "per_tag")
  # strict mode aborts loudly
  expect_error(calibrateMagnetometer(list(TGT = .magTag("TGT", .magBandCloud(), pkg = "PKG1")),
                                     calibration.data = list(SRC = bad),
                                     calibration.on.missing = "error", verbose = FALSE), "contamination|centre")
})

test_that("paddle deployments take the soft-iron shape from the source but the hard-iron centre from in-situ", {
  tgt <- .magTag("TGTP", .magBandCloud(), pkg = "PKGP", paddle = TRUE)
  src <- .magTag("SRCP", .magSphere(4000), pkg = "PKGP", paddle = TRUE)
  out <- mcal(calibrateMagnetometer(list(TGTP = tgt), calibration.data = list(SRCP = src), verbose = FALSE)$TGTP)
  expect_equal(out$source, "external")
  expect_equal(out$center_source, "in_situ")                      # never bake in a static-paddle centre
})

test_that("paddle-state and package mismatches never bind (fall back / skip / error)", {
  tgt <- .magTag("TGT", .magBandCloud(), pkg = "PKG1", paddle = FALSE)
  # (a) different package
  srcX <- .magTag("SRCX", .magSphere(4000), pkg = "PKG2")
  expect_equal(mcal(calibrateMagnetometer(list(TGT = tgt), calibration.data = list(SRCX = srcX), verbose = FALSE)$TGT)$source, "per_tag")
  # (b) paddle-state mismatch, same package
  srcP <- .magTag("SRCP", .magSphere(4000), pkg = "PKG1", paddle = TRUE)
  expect_equal(mcal(calibrateMagnetometer(list(TGT = .magTag("TGT", .magBandCloud(), pkg = "PKG1")),
                                          calibration.data = list(SRCP = srcP), verbose = FALSE)$TGT)$source, "per_tag")
  # (c) on.missing = "skip" leaves it uncalibrated
  skipped <- calibrateMagnetometer(list(TGT = .magTag("TGT", .magBandCloud(), pkg = "PKG1")),
                                   calibration.data = list(SRCX = srcX), calibration.on.missing = "skip", verbose = FALSE)$TGT
  expect_null(mcal(skipped)$proposed)                            # left uncalibrated -> nothing proposed
  # (d) on.missing = "error" aborts
  expect_error(calibrateMagnetometer(list(TGT = .magTag("TGT", .magBandCloud(), pkg = "PKG1")),
                                     calibration.data = list(SRCX = srcX), calibration.on.missing = "error", verbose = FALSE),
               "No usable external calibration")
})

test_that("logger_id is an explicit fallback match key; the user-declared context is recorded", {
  tgt <- .magTag("TGT", .magBandCloud())                          # no package_id
  m <- nautilus:::.getMeta(tgt); m$tag$logger_id <- "LOG9"; tgt <- nautilus:::.restoreMeta(tgt, m)
  src <- .magTag("SRC", .magSphere(4000))
  ms <- nautilus:::.getMeta(src); ms$tag$logger_id <- "LOG9"; src <- nautilus:::.restoreMeta(src, ms)
  out <- mcal(calibrateMagnetometer(list(TGT = tgt), calibration.data = list(SRC = src),
                                    calibration.match = "logger_id",
                                    calibration.context = "in_water", verbose = FALSE)$TGT)
  expect_equal(out$source, "external")
  expect_equal(out$context, "in_water")
})

test_that("calibration option arguments are validated", {
  tg <- list(T = .magTag("T", .magBandCloud(), pkg = "PKG1"))
  expect_error(calibrateMagnetometer(tg, calibration.match = "serial_number"), "invalid key")
  expect_error(calibrateMagnetometer(tg, calibration.on.missing = "bogus"))
  expect_error(magCalibrationControl(center.reject = 0.05, center.warn = 0.2), "center.reject")
})

test_that(".magAxisSpan measures the per-axis angular coverage; the limiting axis collapses for a band", {
  full <- nautilus:::.magAxisSpan(.magSphere(4000))
  band <- nautilus:::.magAxisSpan(.magBandCloud())
  expect_true(all(full > 150))                                    # a tumbled cloud spans a wide arc on every axis
  expect_gt(min(full), min(band))                                 # the band is worse-covered somewhere
  expect_lt(min(band), 60)                                        # the axis perpendicular to the band collapses
})

test_that("axis_span is stored in $mag_calibration for in-situ and source fits", {
  sphere <- mcal(calibrateMagnetometer(list(S = .magTag("S", .magSphere())), verbose = FALSE)$S)
  expect_length(sphere$axis_span, 3L)
  expect_true(min(sphere$axis_span) > 150)                        # well covered
  band <- mcal(calibrateMagnetometer(list(B = .magTag("B", .magBandCloud())), verbose = FALSE)$B)
  expect_lt(min(band$axis_span), 60)                              # under-covered -> also why it is "low"
  # a source fit stores the SOURCE cloud's (better) coverage
  out <- mcal(calibrateMagnetometer(list(TGT = .magTag("TGT", .magBandCloud(), pkg = "PKG1")),
                                    calibration.data = list(SRC = .magTag("SRC", .magSphere(), pkg = "PKG1")),
                                    verbose = FALSE)$TGT)
  expect_true(min(out$axis_span) > 150)
})

test_that("a source that does not out-cover the in-situ cloud is applied but flagged", {
  tgt <- .magTag("TGT", .magSphere(), pkg = "PKG1")               # deployment already well covered
  src <- .magTag("SRC", .magBandCloud(), pkg = "PKG1")            # source is a thin band (no coverage gain)
  # capture at the cli layer with cli_fmt(): robust to how the active reporter (testthat 3e, interactive
  # session, ...) routes stdout/stderr, which capture.output() cannot see through.
  txt <- cli::cli_fmt(
    cal <- calibrateMagnetometer(list(TGT = tgt), calibration.data = list(SRC = src), verbose = "normal")$TGT)
  expect_equal(mcal(cal)$source, "external")   # still applied
  expect_match(paste(txt, collapse = " "), "no better-covered")  # but flagged
})

# ---- exact-ellipsoid fit + dip/coverage-gated 2D fallback + IGRF perpendicular pin -----------------
# a physically-consistent thin band at a chosen dip I: mag + gravity co-rotate (yaw only), so the field
# sits at constant dip I to gravity; a known hard-iron `h` is injected. Returns list(M, grav).
.geoBand <- function(n = 6000, incl_deg = 60, h = c(120, -60, 35), band_pitch = 1, F = 45, seed = 7) {
  set.seed(seed); deg <- pi / 180; I <- incl_deg * deg
  Be <- F * c(cos(I), 0, sin(I))
  Rbe <- function(psi, th) {
    Rz <- rbind(c(cos(psi), sin(psi), 0), c(-sin(psi), cos(psi), 0), c(0, 0, 1))
    Ry <- rbind(c(cos(th), 0, -sin(th)), c(0, 1, 0), c(sin(th), 0, cos(th))); Ry %*% Rz }
  psi <- stats::runif(n, 0, 2 * pi); th <- stats::rnorm(n, 0, band_pitch * deg)
  mag  <- t(vapply(seq_len(n), function(i) Rbe(psi[i], th[i]) %*% Be, numeric(3)))
  grav <- t(vapply(seq_len(n), function(i) Rbe(psi[i], th[i]) %*% c(0, 0, 1), numeric(3)))
  list(M = sweep(mag, 2, h, "+") + matrix(stats::rnorm(n * 3, 0, 0.3), n, 3), grav = grav)
}

test_that(".calibrateMag: thin band -> 2D fallback, IGRF pin recovers the perpendicular hard-iron", {
  g <- .geoBand(incl_deg = 60, h = c(120, -60, 35))
  cal <- nautilus:::.calibrateMag(g$M, grav = g$grav, igrf.incl = 60, target.radius = 45)
  expect_equal(cal$status, "calibrated_2d_fallback")
  expect_equal(cal$perp_source, "igrf_pin")
  expect_lt(max(abs(cal$center - c(120, -60, 35))), 2)              # centre recovered incl. the perpendicular
  expect_true(cal$recommend_apply)
  expect_equal(cal$confidence, "medium")                            # never "high" for a fallback
  # WITHOUT IGRF the perpendicular pin declines (band-plane midpoint), but the in-plane hard-iron centre is
  # still determined by the full swept azimuth -> HEADING stays trustworthy (medium). The pin only fixes the
  # VERTICAL, which is irrelevant to heading (GPS dead-reckoning CV), so confidence does not depend on it.
  cal0 <- nautilus:::.calibrateMag(g$M, target.radius = 45)
  expect_equal(cal0$perp_source, "prior_midpoint"); expect_equal(cal0$confidence, "medium")
})

test_that(".calibrateMag: a well-covered but dip-INCONSISTENT ellipsoid is routed to the 2D fallback, not 3D", {
  # a well-covered sphere (coverage_ok TRUE, ellipsoid-fittable) at the magnetic equator: with the RIGHT
  # inclination the corrected field is dip-consistent -> full 3D; claiming an inclination the data cannot
  # match forces the dip cross-check to fail, so the full soft-iron is refused and the fit routes to 2D.
  M <- sweep(.magSphere(8000), 2, c(5, -3, 2), "+")
  g <- matrix(rep(c(0, 0, 1), each = nrow(M)), ncol = 3)             # constant body-down gravity (unit-test rig)
  ok3d <- nautilus:::.calibrateMag(M, grav = g, igrf.incl = 0, target.radius = 50)
  expect_equal(ok3d$status, "calibrated_3d")                        # dip-consistent + well-covered -> full ellipsoid
  off <- nautilus:::.calibrateMag(M, grav = g, igrf.incl = 60, target.radius = 50)
  expect_equal(off$status, "calibrated_2d_fallback")                # dip cross-check fails -> soft-iron refused
  expect_false(identical(off$confidence, "high"))                   # never full-3D "high" once routed to the fallback
})

test_that(".calibrateMag: a near-1-D arc (single heading) aborts; spikes do not wreck the fit", {
  set.seed(3); n <- 4000
  # a near-stationary blob (the animal barely re-orients): tiny angular extent -> the abort gate fires
  blob <- matrix(rep(c(20, -10, 30), each = n), n, 3) + matrix(stats::rnorm(n * 3, 0, 0.5), n, 3)
  ab <- nautilus:::.calibrateMag(blob, grav = matrix(rep(c(0, 0, 1), each = n), n, 3), igrf.incl = 55, target.radius = 45)
  expect_false(ab$confidence %in% c("high", "medium"))              # a solid ball -> high radcv -> low conf -> not applied
  # spikes: a well-covered cloud with a few gross outliers still recovers the centre (robust trim)
  g <- .magSphere(6000); Msp <- sweep(g, 2, c(5, -3, 2), "+"); Msp[c(1, 100, 3000), 1] <- 400
  cal <- nautilus:::.calibrateMag(Msp)
  expect_equal(cal$center, c(5, -3, 2), tolerance = 1)              # outliers trimmed, not fitted
})
