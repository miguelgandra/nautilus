# Tests for reconstructTrack() (renamed + modernized from deriveTrack): the dead-reckoning + VPC path
# produces pseudo coordinates anchored to the deployment/pop-up metadata, adds the 3-D vertical axis, and
# runs at the default verbosity to completion.

.mk_track_tag <- function(id = "A01", n = 300) {
  set.seed(42)
  t0 <- as.POSIXct("2021-01-01 00:00:00", tz = "UTC")
  dt <- data.table::data.table(
    ID = id, datetime = t0 + seq_len(n),
    heading = cumsum(rnorm(n, 0, 2)) %% 360, pitch = rnorm(n, -5, 8),
    vertical_velocity = rnorm(n, 0, 0.15), depth = 20 + cumsum(rnorm(n, 0, 0.05)),
    paddle_speed = pmax(0, rnorm(n, 0.6, 0.15)))
  m <- nautilus:::.newNautilusMeta(); m$id <- id
  m$deployment$lat <- -17.00; m$deployment$lon <- 42.00; m$deployment$datetime <- t0
  m$deployment$popup_lat <- -17.02; m$deployment$popup_lon <- 42.04
  m$deployment$popup_datetime <- t0 + n
  nautilus:::new_nautilus_tag(dt, m)
}
.rt <- function(tag, ...) suppressWarnings(suppressMessages(reconstructTrack(list(A01 = tag), ...)))

# A straight, eastward swim at a constant `pitch` whose true ALONG-BODY speed is a + b * VeDBA, with GPS
# fixes dropped exactly on the integrated (horizontal) positions - so the VeDBA auto-calibration should
# recover the along-body (a, b) from the deployment itself, independent of pitch.
.mk_vedba_tag <- function(n = 3600L, a = 0.2, b = 2.0, pitch = 0, id = "V1") {
  set.seed(7)
  t0 <- as.POSIXct("2021-01-01", tz = "UTC")
  vedba <- pmax(0.01, 0.15 + 0.05 * sin(seq(0, 20, length.out = n)) + rnorm(n, 0, 0.01))
  true_sp <- a + b * vedba                                  # along-body m/s
  disp_e <- cumsum(true_sp * cos(pitch * pi / 180))         # horizontal metres east at 1 Hz (heading 90)
  deploy_lat <- -17; deploy_lon <- 42; R <- 6371000
  lon_true <- deploy_lon + (disp_e / (R * cos(deploy_lat * pi / 180))) * (180 / pi)
  dt <- data.table::data.table(ID = id, datetime = t0 + seq_len(n), heading = 90, pitch = pitch,
    vedba = vedba, vertical_velocity = true_sp * sin(pitch * pi / 180), paddle_speed = pmax(0, true_sp),
    depth = 5, lat = NA_real_, lon = NA_real_, position_type = NA_character_, quality = NA_character_)
  for (r in seq(300L, n, by = 300L)) dt[r, `:=`(lat = deploy_lat, lon = lon_true[r],
    position_type = "FastGPS", quality = "FastGPS")]                                   # a fix every 5 min
  m <- nautilus:::.newNautilusMeta(); m$id <- id
  m$deployment$lat <- deploy_lat; m$deployment$lon <- deploy_lon; m$deployment$datetime <- t0
  nautilus:::new_nautilus_tag(dt, m)
}
.proc_rec <- function(x) Filter(function(p) identical(p$step, "reconstructTrack"),
                                nautilus:::.getMeta(x)$processing)[[1]]

test_that("reconstructTrack runs at the DEFAULT verbosity and returns a 3-D pseudo-track", {
  out <- .rt(.mk_track_tag())                                              # verbose = 'detailed' default
  a <- out$A01
  expect_true(all(c("pseudo_lon", "pseudo_lat", "speed_dr", "pseudo_depth") %in% names(a)))
  expect_true(any(!is.na(a$pseudo_lon)))
  expect_equal(a$pseudo_lat[1], -17.00, tolerance = 0.02)                  # first anchor ~ deployment
  expect_equal(a$pseudo_depth, a$depth)                                    # vertical axis is the measured depth
})

test_that("speed.method and vpc.method variants run and honour include.depth", {
  tag <- .mk_track_tag()
  for (sm in c("paddle", "constant", "depth_rate")) {
    out <- .rt(tag, control = reconstructTrackControl(speed.method = sm), verbose = FALSE)
    expect_true("pseudo_lon" %in% names(out$A01))
  }
  for (vm in c("linear", "scale_rotate", "none")) {
    out <- .rt(tag, control = reconstructTrackControl(vpc.method = vm), verbose = FALSE)
    expect_true("pseudo_lon" %in% names(out$A01))
  }
  # include.depth = FALSE -> no 3-D column
  flat <- .rt(tag, control = reconstructTrackControl(include.depth = FALSE), verbose = FALSE)
  expect_false("pseudo_depth" %in% names(flat$A01))
})

test_that("a tag missing required columns or deployment coordinates is skipped, not fatal", {
  no_head <- data.table::copy(.mk_track_tag())[, heading := NULL]
  expect_equal(length(.rt(no_head, verbose = FALSE)), 0L)                   # missing 'heading' -> skipped
  m <- nautilus:::.newNautilusMeta(); m$id <- "B"
  bad <- nautilus:::new_nautilus_tag(data.table::data.table(ID = "B",
           datetime = as.POSIXct("2021-01-01", tz = "UTC") + 1:50, heading = 1, pitch = 0), m)
  expect_equal(length(.rt(bad, verbose = FALSE)), 0L)                       # no deployment coords -> skipped
})

test_that("reconstructTrackControl validates its fields", {
  expect_error(reconstructTrackControl(speed.method = "bogus"))
  expect_error(reconstructTrackControl(max.speed = 0.1, constant.speed = 0.5), "between")   # max < constant
  expect_error(reconstructTrackControl(anchor.error.radii = c(1, 2, 3)), "NAMED")
  expect_error(reconstructTrackControl(vedba.model = c(1, 2, 3)), "length-2")               # must be c(a, b)
  expect_error(reconstructTrackControl(depth.rate.min.pitch = 120), "between")              # 0-90 deg
  expect_error(reconstructTrackControl(rest.quantile = 1.5), "between")                     # 0-1
  expect_s3_class(reconstructTrackControl(speed.method = "vedba"), "nautilus_reconstruct_track")
  expect_null(reconstructTrackControl()$rest.quantile)                     # rest gating off by default
  expect_s3_class(reconstructTrackControl(), "nautilus_reconstruct_track")
  expect_equal(reconstructTrackControl()$speed.method, "constant")         # depth_rate/vedba not the default
})

test_that("speed.method = 'vedba' with a supplied model applies speed = intercept + slope * VeDBA", {
  out <- .rt(.mk_vedba_tag(), verbose = FALSE,
             control = reconstructTrackControl(speed.method = "vedba", vedba.model = c(0.1, 3.0),
                                               max.speed = 2.5, vpc.method = "none"))[[1]]
  expect_equal(out$speed_dr, pmin(2.5, pmax(0, 0.1 + 3.0 * out$vedba)), tolerance = 1e-8)
  expect_equal(.proc_rec(out)$vedba_speed_source, "user")
})

test_that("speed.method = 'vedba' auto-calibrates from the deployment's own fixes and records the fit", {
  out <- .rt(.mk_vedba_tag(a = 0.2, b = 2.0), verbose = FALSE,
             control = reconstructTrackControl(speed.method = "vedba"))[[1]]
  pr <- .proc_rec(out)
  expect_equal(pr$vedba_speed_source, "auto")
  expect_equal(pr$vedba_speed_slope, 2.0, tolerance = 0.15)                 # recovered ~ the injected model
  expect_equal(pr$vedba_speed_intercept, 0.2, tolerance = 0.15)
  expect_gt(pr$vedba_speed_r2, 0.9)
  expect_gte(pr$vedba_speed_n_seg, 4L)
})

test_that("VeDBA speed is along-body: reconstructs to the true horizontal scale at steep pitch (no double cos)", {
  P <- 30                                                                    # constant 30-deg dive angle
  out <- .rt(.mk_vedba_tag(a = 0.2, b = 2.0, pitch = P), verbose = FALSE,
             control = reconstructTrackControl(speed.method = "vedba", vpc.method = "none"))[[1]]
  pr <- .proc_rec(out)
  expect_equal(pr$vedba_speed_slope, 2.0, tolerance = 0.15)                 # recovers ALONG-BODY (a, b),
  expect_equal(pr$vedba_speed_intercept, 0.2, tolerance = 0.15)             # not cos(P) * them
  R <- 6371000; dl <- -17; dO <- 42
  e <- (out$pseudo_lon - dO) * (pi / 180) * R * cos(dl * pi / 180)          # reconstructed east metres
  true_e <- cumsum((0.2 + 2.0 * out$vedba) * cos(P * pi / 180))             # true horizontal displacement
  expect_equal(e[length(e)] / true_e[length(true_e)], 1, tolerance = 0.03)  # ~1, not cos(30) = 0.866
})

test_that("rest.quantile without a VeDBA channel leaves speeds ungated (no error)", {
  out <- .rt(.mk_track_tag(), verbose = FALSE,                              # .mk_track_tag has no vedba column
             control = reconstructTrackControl(speed.method = "constant", constant.speed = 0.5,
                                               rest.quantile = 0.2, vpc.method = "none"))[[1]]
  expect_true(all(out$speed_dr == 0.5))                                     # gating silently skipped
})

test_that("VeDBA falls back to constant speed when there are too few fixes to calibrate", {
  tag <- data.table::copy(.mk_vedba_tag())
  tag[, `:=`(lat = NA_real_, lon = NA_real_, position_type = NA_character_, quality = NA_character_)]
  out <- .rt(tag, verbose = FALSE,
             control = reconstructTrackControl(speed.method = "vedba", constant.speed = 0.5))[[1]]
  expect_true(all(out$speed_dr == 0.5))                                     # fell back to the constant
  expect_equal(.proc_rec(out)$vedba_speed_source, "constant_fallback")
})

test_that("depth_rate uses steep-pitch samples and back-fills shallow ones with the constant", {
  t0 <- as.POSIXct("2021-01-01", tz = "UTC"); n <- 200L
  pitch <- rep(c(60, 10), length.out = n)                                   # alternating steep / near-flat
  dt <- data.table::data.table(ID = "D", datetime = t0 + seq_len(n), heading = 90, pitch = pitch,
                               vertical_velocity = 0.5, depth = 10)
  m <- nautilus:::.newNautilusMeta(); m$id <- "D"
  m$deployment$lat <- 0; m$deployment$lon <- 0; m$deployment$datetime <- t0
  out <- .rt(nautilus:::new_nautilus_tag(dt, m), verbose = FALSE,
             control = reconstructTrackControl(speed.method = "depth_rate", depth.rate.min.pitch = 45,
                                               constant.speed = 0.3, vpc.method = "none"))[[1]]
  expect_equal(out$speed_dr[pitch >= 45][1], 0.5 / sin(60 * pi / 180), tolerance = 1e-6)   # vv / sin(pitch)
  expect_equal(out$speed_dr[pitch < 45][1], 0.3)                                           # shallow -> constant
})

test_that("rest.quantile zeros the speed during the low-VeDBA tail", {
  out <- .rt(.mk_vedba_tag(), verbose = FALSE,
             control = reconstructTrackControl(speed.method = "constant", constant.speed = 0.5,
                                               rest.quantile = 0.2, vpc.method = "none"))[[1]]
  thr <- stats::quantile(out$vedba, 0.2, na.rm = TRUE)
  expect_true(all(out$speed_dr[out$vedba <  thr] == 0))                     # rest -> held at zero
  expect_true(all(out$speed_dr[out$vedba >= thr] == 0.5))                   # active -> the constant
})

test_that("the diagnostic report renders (summary + a track-map detail page)", {
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f), add = TRUE)
  .pdf_pages <- function(p) { b <- readBin(p, "raw", file.info(p)$size)
    length(grepRaw(charToRaw("/Type /Page"), b, all = TRUE)) - length(grepRaw(charToRaw("/Type /Pages"), b, all = TRUE)) }
  out <- suppressWarnings(suppressMessages(reconstructTrack(list(A01 = .mk_track_tag()), plot.file = f, verbose = FALSE)))
  expect_true(file.exists(f) && file.size(f) > 0)
  expect_equal(.pdf_pages(f), 2L)                                          # summary + the lone deployment's detail
  expect_true("pseudo_lon" %in% names(out$A01))                           # data still returned alongside the plot
})

# a straight eastward run with a single interior fix, used to probe the VPC weighting / drift / uncertainty
.mk_vpc_tag <- function(n = 400L, fix_row = 200L, fix_lon = 0.02, speed_col = NULL) {
  t0 <- as.POSIXct("2021-01-01", tz = "UTC")
  dt <- data.table::data.table(ID = "A", datetime = t0 + seq_len(n), heading = 90, pitch = 0, depth = 10,
    lat = NA_real_, lon = NA_real_, position_type = NA_character_, quality = NA_character_)
  if (!is.null(speed_col)) dt[, paddle_speed := speed_col]
  dt[fix_row, `:=`(lat = 0.0, lon = fix_lon, position_type = "FastGPS", quality = "FastGPS")]
  m <- nautilus:::.newNautilusMeta(); m$id <- "A"
  m$deployment$lat <- 0; m$deployment$lon <- 0; m$deployment$datetime <- t0
  nautilus:::new_nautilus_tag(dt, m)
}

test_that("pseudo_error is emitted, ~ the fix radius at anchors, and swells between them", {
  a <- .rt(.mk_vpc_tag(), verbose = FALSE,
           control = reconstructTrackControl(speed.method = "constant", vpc.method = "error_weighted",
                                             drift.rate = 5))[[1]]
  expect_true("pseudo_error" %in% names(a))
  expect_true(all(a$pseudo_error > 0))
  r_fix <- reconstructTrackControl()$anchor.error.radii[["FastGPS"]]                # 50 m
  expect_lt(a$pseudo_error[200], 1.2 * r_fix)                                       # ~ fix radius at the fix
  expect_gt(a$pseudo_error[100], 3 * a$pseudo_error[200])                           # bulges mid-segment
  expect_gt(a$pseudo_error[400], a$pseudo_error[300])                              # grows after the last fix
})

test_that("vpc.method = 'none' gives a monotonically growing pseudo_error from the deployment radius", {
  a <- .rt(.mk_vpc_tag(), verbose = FALSE,
           control = reconstructTrackControl(speed.method = "constant", vpc.method = "none"))[[1]]
  expect_equal(a$pseudo_error[1], 50, tolerance = 1)                                # deploy radius
  expect_false(is.unsorted(a$pseudo_error))                                         # never shrinks
})

test_that("the corrected track continues continuously past the last fix (no snap back to raw DR)", {
  a <- .rt(.mk_vpc_tag(), verbose = FALSE,                              # fix at row 200, tail 201:400
           control = reconstructTrackControl(speed.method = "constant", constant.speed = 0.5,
                                             vpc.method = "linear"))[[1]]
  east <- a$pseudo_lon * (pi / 180) * 6371000                          # deploy lon = 0 -> east metres
  expect_lt(abs(east[201] - east[200]), 1)                             # ~0.5 m step, not a ~2 km jump back
  expect_equal(east[400] - east[200], (400 - 200) * 0.5, tolerance = 1)# tail dead-reckons on from the fix
})

test_that("pseudo_error does not jump after a coarse last fix (tail seeds from the smoothed value)", {
  n <- 600L; t0 <- as.POSIXct("2021-01-01", tz = "UTC")
  dt <- data.table::data.table(ID = "A", datetime = t0 + seq_len(n), heading = 90, pitch = 0, depth = 10,
    lat = NA_real_, lon = NA_real_, position_type = NA_character_, quality = NA_character_)
  dt[300, `:=`(lat = 0, lon = 0.02, position_type = "FastGPS", quality = "FastGPS")]  # precise (r = 50)
  dt[500, `:=`(lat = 0, lon = 0.03, position_type = "Argos", quality = "Z")]          # coarse (r = 50000)
  m <- nautilus:::.newNautilusMeta(); m$id <- "A"
  m$deployment$lat <- 0; m$deployment$lon <- 0; m$deployment$datetime <- t0
  a <- .rt(nautilus:::new_nautilus_tag(dt, m), verbose = FALSE,
           control = reconstructTrackControl(vpc.method = "error_weighted", drift.rate = 0.5))[[1]]
  expect_lt(a$pseudo_error[501] / a$pseudo_error[500], 1.2)             # continuous, not a ~1000x jump to 50 km
})

test_that("drift.diffusion = 0 is identical to the linear model; a positive value pulls harder to the fix", {
  base <- .rt(.mk_vpc_tag(), verbose = FALSE,
              control = reconstructTrackControl(speed.method = "constant", vpc.method = "error_weighted",
                                                drift.rate = 0.5, drift.diffusion = 0))[[1]]
  # diffusion = 0 leaves the fitted lon unchanged vs not specifying it (default is 0)
  ref <- .rt(.mk_vpc_tag(), verbose = FALSE,
             control = reconstructTrackControl(speed.method = "constant", vpc.method = "error_weighted",
                                               drift.rate = 0.5))[[1]]
  expect_equal(base$pseudo_lon[200], ref$pseudo_lon[200], tolerance = 1e-12)
  # a large diffusion inflates the reckoning error -> higher gain -> lands closer to the fix at (0.02, 0)
  diff <- .rt(.mk_vpc_tag(), verbose = FALSE,
              control = reconstructTrackControl(speed.method = "constant", vpc.method = "error_weighted",
                                                drift.rate = 0.5, drift.diffusion = 500))[[1]]
  gap <- function(x) nautilus:::.trackDistance(x$pseudo_lon[200], x$pseudo_lat[200], 0.02, 0.0) * 1000
  expect_lt(gap(diff), gap(base))
})

test_that("vpc.weighting distance vs time differ under variable speed but both still hit the fix", {
  sp <- c(rep(0.1, 100L), rep(2.0, 100L), rep(2.0, 200L))[seq_len(400L)]             # slow then fast
  tag <- function() .mk_vpc_tag(speed_col = sp)
  ctrl <- function(w) reconstructTrackControl(speed.method = "paddle", vpc.method = "linear", vpc.weighting = w)
  d <- .rt(tag(), verbose = FALSE, control = ctrl("distance"))[[1]]
  t <- .rt(tag(), verbose = FALSE, control = ctrl("time"))[[1]]
  gap <- function(x) nautilus:::.trackDistance(x$pseudo_lon[200], x$pseudo_lat[200], 0.02, 0.0) * 1000
  expect_lt(gap(d), 0.5); expect_lt(gap(t), 0.5)                                     # both forced through the fix
  expect_gt(abs(d$pseudo_lon[100] - t$pseudo_lon[100]), 1e-6)                        # but distributed differently
})

test_that("reconstructTrackControl validates the new VPC fields", {
  expect_error(reconstructTrackControl(drift.diffusion = -1), "between")
  expect_error(reconstructTrackControl(vpc.weighting = "bogus"))
  expect_error(reconstructTrackControl(vpc.method = "bogus"))
  expect_error(reconstructTrackControl(reconstructability.min = -0.1), "between")
  expect_equal(reconstructTrackControl()$vpc.weighting, "distance")                  # distance is the default
  expect_equal(reconstructTrackControl()$drift.diffusion, 0)
  expect_equal(reconstructTrackControl()$reconstructability.min, 0.1)                # gate on by default
  expect_s3_class(reconstructTrackControl(vpc.method = "scale_rotate"), "nautilus_reconstruct_track")
})

# a track anchored ONLY by its endpoints (no interior fixes), for the reconstructability gate
.mk_gate_tag <- function(heading, popup_lon = 0.05, popup_lat = 0, interior_fix = FALSE, n = 600L, id = "A") {
  t0 <- as.POSIXct("2021-01-01", tz = "UTC")
  dt <- data.table::data.table(ID = id, datetime = t0 + seq_len(n), heading = heading, pitch = 0, depth = 5,
    paddle_speed = 1, lat = NA_real_, lon = NA_real_, position_type = NA_character_, quality = NA_character_)
  if (interior_fix) data.table::set(dt, 300L, c("lat", "lon", "position_type", "quality"),
                                    list(0, 0.002, "FastGPS", "FastGPS"))
  m <- nautilus:::.newNautilusMeta(); m$id <- id
  m$deployment$lat <- 0; m$deployment$lon <- 0; m$deployment$datetime <- t0
  if (!is.null(popup_lon)) { m$deployment$popup_lat <- popup_lat; m$deployment$popup_lon <- popup_lon
    m$deployment$popup_datetime <- t0 + n }
  nautilus:::new_nautilus_tag(dt, m)
}
.gate_of <- function(x) nautilus:::.getMeta(x)$sensors$reconstructability

test_that("the reconstructability gate warns and flags a tortuous endpoints-only track", {
  tortuous <- .mk_gate_tag(seq(1, 360, length.out = 600), popup_lon = 0.0002)       # near-closed loop, pop-up near start
  expect_warning(out <- reconstructTrack(list(A = tortuous), verbose = FALSE), "interior geometry is unreliable")
  g <- .gate_of(out$A)
  expect_equal(g$gate, "warn"); expect_true(g$endpoint_bounded); expect_lt(g$directedness, 0.1)
})

test_that("the reconstructability gate passes a directed endpoints-only track (directedness capped at 1)", {
  directed <- .mk_gate_tag(90, popup_lon = 0.05)                                    # straight east, pop-up far
  out <- suppressWarnings(reconstructTrack(list(A = directed), verbose = FALSE))
  g <- .gate_of(out$A)
  expect_equal(g$gate, "pass"); expect_lte(g$directedness, 1)                       # capped, not > 1
})

test_that("the gate does not fire when interior fixes are present, and is disabled by min = 0", {
  withfix <- .mk_gate_tag(seq(1, 360, length.out = 600), popup_lon = 0.0002, interior_fix = TRUE)
  out <- suppressWarnings(reconstructTrack(list(A = withfix), verbose = FALSE))
  expect_null(.gate_of(out$A))                                                      # an interior fix constrains it
  off <- suppressWarnings(reconstructTrack(list(A = .mk_gate_tag(seq(1, 360, length.out = 600), popup_lon = 0.0002)),
           control = reconstructTrackControl(reconstructability.min = 0), verbose = FALSE))
  expect_null(.gate_of(off$A))                                                      # gate disabled
})

test_that("a fully unbounded track (no pop-up, no fixes) is flagged 'unbounded' with a full schema", {
  tag <- .mk_gate_tag(90, popup_lon = NULL)
  expect_warning(out <- reconstructTrack(list(A = tag), verbose = FALSE), "fully unbounded")
  g <- .gate_of(out$A)
  expect_equal(g$gate, "unbounded")
  expect_setequal(names(g), c("endpoint_bounded", "directedness", "net_km", "path_km", "gate"))  # consistent schema
})

test_that("a zero-reckoned-path track with a distant pop-up is treated as directed (path -> 0+ limit)", {
  d <- data.table::copy(.mk_gate_tag(90, popup_lon = 0.05))[, paddle_speed := 0]                 # zero reckoned path
  out <- suppressWarnings(reconstructTrack(list(A = d), control = reconstructTrackControl(speed.method = "paddle"),
                                           verbose = FALSE))
  g <- .gate_of(out$A)
  expect_equal(g$gate, "pass"); expect_equal(g$directedness, 1)                                  # not a false 'warn'
})

test_that("vpc.method='scale_rotate' forces through fixes and recovers a systematic heading bias on a curve", {
  # TRUE path curves; the DR carries a constant +15 deg heading bias. scale_rotate should un-rotate the whole
  # segment (recovering the truth), whereas the additive 'linear' can only ramp the endpoint error.
  t0 <- as.POSIXct("2021-01-01", tz = "UTC"); n <- 300L; s <- 1; R <- 6371000
  h_true <- 90 + 30 * sin(seq(0, 2 * pi, length.out = n)); h_dr <- h_true + 15
  de <- cumsum(s * sin(h_true * pi / 180)); dn <- cumsum(s * cos(h_true * pi / 180))
  true_lon <- de / R * (180 / pi); true_lat <- dn / R * (180 / pi)                    # deployment at (0, 0)
  dt <- data.table::data.table(ID = "A", datetime = t0 + seq_len(n), heading = h_dr, pitch = 0, depth = 5,
    paddle_speed = s, lat = NA_real_, lon = NA_real_, position_type = NA_character_, quality = NA_character_)
  dt[n, `:=`(lat = true_lat[n], lon = true_lon[n], position_type = "FastGPS", quality = "FastGPS")]
  m <- nautilus:::.newNautilusMeta(); m$id <- "A"
  m$deployment$lat <- 0; m$deployment$lon <- 0; m$deployment$datetime <- t0
  run <- function(vm) .rt(nautilus:::new_nautilus_tag(data.table::copy(dt), m), verbose = FALSE,
                          control = reconstructTrackControl(speed.method = "paddle", vpc.method = vm))$A
  gc <- function(lo, la, LO, LA) nautilus:::.trackDistance(lo, la, LO, LA) * 1000
  sr <- run("scale_rotate"); lin <- run("linear")
  expect_lt(gc(sr$pseudo_lon[n], sr$pseudo_lat[n], true_lon[n], true_lat[n]), 0.5)   # lands exactly on the fix
  interior <- function(x) max(vapply(50:250, function(k)
    gc(x$pseudo_lon[k], x$pseudo_lat[k], true_lon[k], true_lat[k]), numeric(1)))
  expect_lt(interior(sr), 2)                                                          # recovers the true curve
  expect_gt(interior(lin), 4 * interior(sr))                                          # additive is much worse here
})

test_that("scale_rotate stays finite on a stationary (zero-movement) segment and still hits its fixes", {
  t0 <- as.POSIXct("2021-01-01", tz = "UTC"); n <- 300L
  dt <- data.table::data.table(ID = "A", datetime = t0 + seq_len(n), heading = 90, pitch = 0, depth = 5,
    paddle_speed = 0, lat = NA_real_, lon = NA_real_, position_type = NA_character_, quality = NA_character_)
  dt[150, `:=`(lat = 0.0005, lon = 0.0005, position_type = "FastGPS", quality = "FastGPS")]
  dt[300, `:=`(lat = 0.0008, lon = 0.0010, position_type = "FastGPS", quality = "FastGPS")]
  m <- nautilus:::.newNautilusMeta(); m$id <- "A"
  m$deployment$lat <- 0; m$deployment$lon <- 0; m$deployment$datetime <- t0
  a <- .rt(nautilus:::new_nautilus_tag(dt, m), verbose = FALSE,
           control = reconstructTrackControl(speed.method = "paddle", vpc.method = "scale_rotate"))$A
  expect_true(all(is.finite(a$pseudo_lon)) && all(is.finite(a$pseudo_lat)))           # no Inf from |net_DR| = 0
  gc <- function(lo, la, LO, LA) nautilus:::.trackDistance(lo, la, LO, LA) * 1000
  expect_lt(gc(a$pseudo_lon[150], a$pseudo_lat[150], 0.0005, 0.0005), 0.5)            # fallback still lands on fix
})

test_that("scale_rotate stays bounded on a tortuous (near-closed) segment instead of magnifying it", {
  # a near-closed reckoned loop (net ~ 0) between two distant fixes would blow up under a naive scale factor;
  # the ill-conditioned guard falls back to additive redistribution -> bounded interior, fix still hit
  t0 <- as.POSIXct("2021-01-01", tz = "UTC"); n <- 360L
  dt <- data.table::data.table(ID = "A", datetime = t0 + seq_len(n),
    heading = seq(1, 360, length.out = n), pitch = 0, depth = 5, paddle_speed = 1,   # a full-circle sweep
    lat = NA_real_, lon = NA_real_, position_type = NA_character_, quality = NA_character_)
  fix_lon <- 0.005                                                          # ~556 m east, far from the ~0 net loop
  dt[n, `:=`(lat = 0, lon = fix_lon, position_type = "FastGPS", quality = "FastGPS")]
  m <- nautilus:::.newNautilusMeta(); m$id <- "A"
  m$deployment$lat <- 0; m$deployment$lon <- 0; m$deployment$datetime <- t0
  a <- .rt(nautilus:::new_nautilus_tag(dt, m), verbose = FALSE,
           control = reconstructTrackControl(speed.method = "paddle", vpc.method = "scale_rotate"))$A
  expect_true(all(is.finite(a$pseudo_lon)))
  east <- a$pseudo_lon * (pi / 180) * 6371000
  expect_lt(max(abs(east)), 10 * fix_lon * (pi / 180) * 6371000)           # bounded near the fix scale (not ~100x)
  gc <- function(lo, la, LO, LA) nautilus:::.trackDistance(lo, la, LO, LA) * 1000
  expect_lt(gc(a$pseudo_lon[n], a$pseudo_lat[n], fix_lon, 0), 0.5)          # still lands on the fix
})

test_that("scale_rotate continues continuously past the last fix (no discontinuity)", {
  a <- .rt(.mk_vpc_tag(), verbose = FALSE,
           control = reconstructTrackControl(speed.method = "constant", constant.speed = 0.5,
                                             vpc.method = "scale_rotate"))[[1]]
  east <- a$pseudo_lon * (pi / 180) * 6371000
  step_at  <- abs(east[201] - east[200])                    # step across the last fix (row 200)
  step_typ <- stats::median(abs(diff(east[210:300])))       # ordinary tail step
  expect_lt(step_at, 3 * step_typ + 1e-6)                   # continuous with the extrapolated tail
})

test_that(".projLocal / .unprojLocal are exact inverses (local per-point-latitude equirectangular map)", {
  set.seed(3); R <- 6371000; lon <- 5 + rnorm(50, 0, 0.5); lat <- 61 + rnorm(50, 0, 0.5)
  p <- nautilus:::.projLocal(lon, lat, 5, 61, R); b <- nautilus:::.unprojLocal(p$e, p$n, 5, 61, R)
  expect_equal(b$lon, lon, tolerance = 1e-9); expect_equal(b$lat, lat, tolerance = 1e-9)
})

test_that("spherical DR integration matches the full destination-point formula over a latitude excursion", {
  R <- 6371000; lat0 <- 60; lon0 <- 5; n <- 2000L; set.seed(11)
  hdg <- (30 + cumsum(rnorm(n, 0, 3))) %% 360; step <- 90                   # ~1.5 m/s * 60 s per step
  d_e <- step * sin(hdg * pi / 180); d_n <- step * cos(hdg * pi / 180); d_e[1] <- 0; d_n[1] <- 0
  vec <- nautilus:::.deadReckonSpherical(d_e, d_n, lon0, lat0, R)
  rlat <- numeric(n); rlon <- numeric(n); rlat[1] <- lat0 * pi / 180; rlon[1] <- lon0 * pi / 180
  for (i in 2:n) {                                                          # full destination-point recursion
    q <- sqrt(d_e[i]^2 + d_n[i]^2) / R; h <- atan2(d_e[i], d_n[i])
    rlat[i] <- asin(sin(rlat[i - 1]) * cos(q) + cos(rlat[i - 1]) * sin(q) * cos(h))
    rlon[i] <- rlon[i - 1] + atan2(sin(h) * sin(q) * cos(rlat[i - 1]), cos(q) - sin(rlat[i - 1]) * sin(rlat[i]))
  }
  gc <- function(lo, la, LO, LA) nautilus:::.trackDistance(lo, la, LO, LA) * 1000
  err <- max(vapply(seq_len(n), function(k)
    gc(vec$lon[k], vec$lat[k], rlon[k] * 180 / pi, rlat[k] * 180 / pi), numeric(1)))
  expect_gt(diff(range(vec$lat)), 0.5)                                      # a real latitude excursion, and
  expect_lt(err, 5)                                                         # the vectorised form tracks it to a few m
})

test_that("a deployment/fix straddling the antimeridian projects the short way and stays in [-180, 180]", {
  R <- 6371000
  tp <- nautilus:::.projLocal(-178.1, 5, 179.9, 5, R)                     # fix 2 deg E of deploy, across +/-180
  hav <- 2 * R * asin(sqrt(cos(5 * pi / 180)^2 * sin((-178.1 - 179.9) * pi / 180 / 2)^2))
  expect_equal(tp$e, hav, tolerance = 1)                                  # ~ +221 km short-way, not ~ -39,656 km
  n <- 500L; t0 <- as.POSIXct("2021-01-01", tz = "UTC")
  dt <- data.table::data.table(ID = "A", datetime = t0 + 60 * seq_len(n), heading = 90, pitch = 0, depth = 5,
    paddle_speed = 2, lat = NA_real_, lon = NA_real_, position_type = NA_character_, quality = NA_character_)
  dt[n, `:=`(lat = 5, lon = -178.1, position_type = "FastGPS", quality = "FastGPS")]
  m <- nautilus:::.newNautilusMeta(); m$id <- "A"
  m$deployment$lat <- 5; m$deployment$lon <- 179.9; m$deployment$datetime <- t0
  a <- .rt(nautilus:::new_nautilus_tag(dt, m), verbose = FALSE,
           control = reconstructTrackControl(speed.method = "paddle", vpc.method = "linear"))$A
  gc <- function(lo, la, LO, LA) nautilus:::.trackDistance(lo, la, LO, LA) * 1000
  expect_lt(gc(a$pseudo_lon[n], a$pseudo_lat[n], -178.1, 5), 0.5)         # lands on the cross-seam fix
  expect_true(all(abs(a$pseudo_lon) <= 180))                             # canonical longitudes, no seam blow-up
})

test_that("fixes are hit exactly at high latitude under the local projection (VPC round-trip)", {
  n <- 500L; t0 <- as.POSIXct("2021-01-01", tz = "UTC")
  dt <- data.table::data.table(ID = "A", datetime = t0 + 60 * seq_len(n), heading = 40, pitch = 0, depth = 5,
    paddle_speed = 1.5, lat = NA_real_, lon = NA_real_, position_type = NA_character_, quality = NA_character_)
  fix_lon <- 5.4; fix_lat <- 61.3
  dt[n, `:=`(lat = fix_lat, lon = fix_lon, position_type = "FastGPS", quality = "FastGPS")]
  m <- nautilus:::.newNautilusMeta(); m$id <- "A"
  m$deployment$lat <- 61; m$deployment$lon <- 5; m$deployment$datetime <- t0
  gc <- function(lo, la, LO, LA) nautilus:::.trackDistance(lo, la, LO, LA) * 1000
  for (vm in c("linear", "scale_rotate")) {
    a <- .rt(nautilus:::new_nautilus_tag(data.table::copy(dt), m), verbose = FALSE,
             control = reconstructTrackControl(speed.method = "paddle", vpc.method = vm))$A
    expect_lt(gc(a$pseudo_lon[n], a$pseudo_lat[n], fix_lon, fix_lat), 0.5)  # hit exactly at 61 deg N
  }
})

test_that("VPC drives the track exactly through every fix (linear) and excludes metadata rows as anchors", {
  n <- 400; t0 <- as.POSIXct("2021-01-01", tz = "UTC")
  dt <- data.table::data.table(ID = "A", datetime = t0 + seq_len(n), heading = 90, pitch = 0, depth = 10,
    lat = NA_real_, lon = NA_real_, position_type = NA_character_, quality = NA_character_)
  dt[150, `:=`(lat = 0.001, lon = 0.02, position_type = "FastGPS", quality = "FastGPS")]
  dt[300, `:=`(lat = -0.001, lon = 0.03, position_type = "FastGPS", quality = "FastGPS")]
  m <- nautilus:::.newNautilusMeta(); m$id <- "A"; m$deployment$lat <- 0; m$deployment$lon <- 0; m$deployment$datetime <- t0
  a <- .rt(nautilus:::new_nautilus_tag(dt, m),
           control = reconstructTrackControl(speed.method = "constant", vpc.method = "linear"), verbose = FALSE)$A
  gc <- function(lo, la, LO, LA) nautilus:::.trackDistance(lo, la, LO, LA) * 1000        # km -> m
  expect_lt(gc(a$pseudo_lon[150], a$pseudo_lat[150], 0.02, 0.001), 0.5)                  # forced through fix 1
  expect_lt(gc(a$pseudo_lon[300], a$pseudo_lat[300], 0.03, -0.001), 0.5)                 # ... and fix 2
  pr <- Filter(function(p) identical(p$step, "reconstructTrack"), nautilus:::.getMeta(a)$processing)[[1]]
  expect_equal(pr$n_anchors, 3L)                                                         # deploy + 2 fixes (no metadata dup)
})

test_that("reconstructTrack warns when heading came from an uncalibrated magnetometer, not when calibrated", {
  warns_of <- function(status) {
    tg <- .mk_track_tag(); m <- nautilus:::.getMeta(tg)
    m$mag_calibration$status <- status; tg <- nautilus:::.restoreMeta(tg, m)
    ws <- character(0)
    withCallingHandlers(suppressMessages(reconstructTrack(list(A01 = tg), verbose = FALSE)),
                        warning = function(w) { ws <<- c(ws, conditionMessage(w)); invokeRestart("muffleWarning") })
    any(grepl("UNCALIBRATED magnetometer", ws))
  }
  expect_true(warns_of("uncalibrated_raw"))       # a raw field -> loud drift warning
  expect_false(warns_of("calibrated_3d"))         # a trusted calibration -> no such warning
})
