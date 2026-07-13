# Tests for crossValidateTrack(): leave-one-out held-out-fix cross-validation of reconstructTrack. Fixes are
# placed on the true straight-east DR line (heading 90, speed 1, dt 1) so a consistent track scores ~ 0; a
# heading bias makes the raw reckoning drift off them.

.mk_cv_tag <- function(hbias = 0, fixrows = c(150L, 300L, 450L), n = 600L, id = "A") {
  R <- 6371000; t0 <- as.POSIXct("2021-01-01", tz = "UTC")
  dt <- data.table::data.table(ID = id, datetime = t0 + seq_len(n), heading = 90 + hbias, pitch = 0, depth = 5,
    paddle_speed = 1, lat = NA_real_, lon = NA_real_, position_type = NA_character_, quality = NA_character_)
  lonv <- seq_len(n) / R * (180 / pi)                                     # true east lon per row on the DR line
  for (rr in fixrows) data.table::set(dt, rr, c("lat", "lon", "position_type", "quality"),
                                      list(0, lonv[rr], "FastGPS", "FastGPS"))   # set() avoids the := self-shadow
  m <- nautilus:::.newNautilusMeta(); m$id <- id
  m$deployment$lat <- 0; m$deployment$lon <- 0; m$deployment$datetime <- t0
  m$deployment$popup_lat <- 0; m$deployment$popup_lon <- lonv[n]; m$deployment$popup_datetime <- t0 + n
  nautilus:::new_nautilus_tag(dt, m)
}
.cvt <- function(tag, ...) suppressWarnings(suppressMessages(crossValidateTrack(list(A = tag), verbose = FALSE, ...)))

test_that("crossValidateTrack scores a self-consistent track near zero and returns the expected table", {
  cv <- .cvt(.mk_cv_tag(0), control = reconstructTrackControl(speed.method = "paddle", vpc.method = "linear"))
  expect_setequal(names(cv), c("id", "datetime", "quality", "error_m", "gap_h", "interpolated",
                               "fix_radius_m", "n_anchors_used", "speed_method", "vpc_method"))
  expect_equal(nrow(cv), 4L)                                              # 3 FastGPS fixes + the pop-up
  expect_true(all(cv$error_m < 2))                                        # fixes lie on the DR line -> ~ 0 error
  expect_equal(sum(cv$quality == "FastGPS"), 3L)
  expect_true(all(cv$n_anchors_used == 4L))                              # 5 anchors total, minus the held-out one
})

test_that("a correction method reduces held-out error vs 'none' on a drifting track", {
  tag <- .mk_cv_tag(hbias = 8)
  none <- .cvt(tag, control = reconstructTrackControl(speed.method = "paddle", vpc.method = "none"))
  lin  <- .cvt(tag, control = reconstructTrackControl(speed.method = "paddle", vpc.method = "linear"))
  expect_lt(stats::median(lin$error_m), stats::median(none$error_m))     # the VPC helps where raw DR drifts
  expect_equal(unique(lin$vpc_method), "linear")                         # method recorded for stacking/comparison
})

test_that("held-out error grows with the reckoning gap for uncorrected drift", {
  cv <- .cvt(.mk_cv_tag(hbias = 8, fixrows = c(120L, 480L)),             # sparse fixes -> raw error scales with time
             control = reconstructTrackControl(speed.method = "paddle", vpc.method = "none"))
  cv <- cv[order(cv$datetime), ]
  expect_gt(cv$error_m[nrow(cv)], cv$error_m[1])                         # later fix -> more accumulated drift
})

test_that("deployments without an interior fix are handled: pop-up only, or a typed empty table", {
  cv1 <- .cvt(.mk_cv_tag(0, fixrows = integer(0)))                        # deploy + pop-up only
  expect_equal(nrow(cv1), 1L); expect_equal(cv1$quality, "Popup")        # -> the pop-up closure alone
  tag <- .mk_cv_tag(0, fixrows = integer(0))
  m <- nautilus:::.getMeta(tag); m$deployment$popup_lat <- NA; m$deployment$popup_lon <- NA
  cv0 <- .cvt(nautilus:::.restoreMeta(data.table::copy(tag), m))          # deploy only -> nothing to withhold
  expect_equal(nrow(cv0), 0L)
  expect_true(all(c("id", "error_m", "gap_h", "vpc_method") %in% names(cv0)))   # still a typed frame
})

test_that("crossValidateTrack does not fire the reconstructability gate on its holdouts", {
  tag <- .mk_cv_tag(fixrows = c(300L))                        # 1 interior fix; LOO of it leaves endpoint-bounded
  warns <- character()
  withCallingHandlers(
    suppressMessages(crossValidateTrack(list(A = tag), verbose = FALSE)),
    warning = function(w) { warns <<- c(warns, conditionMessage(w)); invokeRestart("muffleWarning") })
  expect_false(any(grepl("interior geometry|fully unbounded", warns)))       # no spurious gate warnings
})

test_that("the diagnostic report renders", {
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f), add = TRUE)
  invisible(.cvt(.mk_cv_tag(hbias = 5), plot.file = f))
  expect_true(file.exists(f) && file.size(f) > 0)
})
