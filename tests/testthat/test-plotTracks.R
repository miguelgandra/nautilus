# Tests for plotTracks() - the movement-track map plotter. Because it draws only lightweight, optional
# base-graphics layers (no tile server / rJava / network), the full render path IS exercised here against
# a throwaway PDF device, unlike the old plotMaps().

Sys.setlocale("LC_TIME", "C")

.mk_track_tag <- function(id = "A01", n = 300, with_track = TRUE, with_fixes = TRUE) {
  t0 <- as.POSIXct("2021-01-01 00:00:00", tz = "UTC")
  lon <- -25.3 + cumsum(rep(0.0004, n)); lat <- 37 + cumsum(rep(0.0003, n))
  d <- data.table::data.table(ID = id, datetime = t0 + seq_len(n), depth = seq(0, 30, length.out = n))
  if (with_track)
    d[, `:=`(pseudo_lon = lon, pseudo_lat = lat, pseudo_depth = depth,
             pseudo_error = seq(50, 800, length.out = n), speed_dr = seq(0.3, 1.2, length.out = n))]
  m <- nautilus:::.newNautilusMeta(); m$id <- id
  m$deployment$lon <- lon[1]; m$deployment$lat <- lat[1]; m$deployment$datetime <- t0
  m$deployment$popup_lon <- lon[n]; m$deployment$popup_lat <- lat[n]; m$deployment$popup_datetime <- t0 + n
  if (with_fixes) {
    fi <- c(1, n %/% 2, n)
    m$ancillary$positions <- list(source = "test", data = data.frame(
      datetime = t0 + fi, type = c("User", "FastGPS", "Argos"),
      lon = lon[fi], lat = lat[fi], quality = c(NA, "7", NA), stringsAsFactors = FALSE))
  }
  nautilus:::new_nautilus_tag(d, m)
}

# render to a throwaway PDF so the full draw path executes without a screen device
draw_to_pdf <- function(expr) {
  pf <- tempfile(fileext = ".pdf"); grDevices::pdf(pf)
  on.exit({ if (grDevices::dev.cur() != 1L) grDevices::dev.off(); unlink(pf) }, add = TRUE)
  force(expr)
}

test_that("plotTracks returns a per-deployment summary and renders without error", {
  res <- draw_to_pdf(plotTracks(list(A01 = .mk_track_tag()), verbose = FALSE))
  expect_s3_class(res, "data.frame")
  expect_named(res, c("id", "n_fix", "n_track", "drawn"))
  expect_equal(res$n_fix, 3L)
  expect_equal(res$n_track, 300L)
  expect_true(res$drawn)
})

test_that("color.by = depth/speed and show.uncertainty run the full draw path", {
  expect_silent(draw_to_pdf(plotTracks(list(A01 = .mk_track_tag()), color.by = "depth",
                                       show.uncertainty = TRUE, verbose = FALSE)))
  expect_silent(draw_to_pdf(plotTracks(list(A01 = .mk_track_tag()), color.by = "speed", verbose = FALSE)))
})

test_that("a fixes-only deployment (no pseudo-track) still plots", {
  res <- draw_to_pdf(plotTracks(list(A01 = .mk_track_tag(with_track = FALSE)), verbose = FALSE))
  expect_equal(res$n_track, 0L)
  expect_true(res$drawn)
})

test_that("a deployment with neither fixes nor a track is skipped, not drawn", {
  res <- draw_to_pdf(plotTracks(list(A01 = .mk_track_tag(with_track = FALSE, with_fixes = FALSE)),
                                verbose = FALSE))
  expect_false(res$drawn)
})

test_that("writes a multi-page PDF and paginates across pages", {
  tags <- stats::setNames(lapply(sprintf("A%02d", 1:5), function(id) .mk_track_tag(id, n = 50)),
                          sprintf("A%02d", 1:5))
  pf <- withr::local_tempfile(fileext = ".pdf")
  res <- plotTracks(tags, ncols = 2, nrows = 2, plot = FALSE, plot.file = pf, verbose = FALSE)
  expect_true(file.exists(pf) && file.info(pf)$size > 0)
  expect_equal(nrow(res), 5L)
  expect_true(all(res$drawn))
})

test_that("argument validation is strict and cli-formatted", {
  expect_error(plotTracks(list(A01 = .mk_track_tag()), color.by = "bogus"), "color.by")
  expect_error(plotTracks(list(A01 = .mk_track_tag()), plot = FALSE), "Nothing to plot")
  expect_error(plotTracks(list(A01 = .mk_track_tag()), theme = list()), "theme")
  expect_error(plotTracks(list(A01 = .mk_track_tag()), colors = "red"), "NAMED")
  expect_error(plotTracks(character(0)), "empty|no ")   # .resolveInput fail-loud on empty
})

test_that("the pseudo-track is drawn in time order regardless of input row order", {
  # a shuffled track: .gatherPseudoTrack must re-sort by datetime so endpoints are correct
  tag <- .mk_track_tag(n = 100)
  d <- data.table::as.data.table(tag)
  shuffled <- d[sample(nrow(d))]
  tr <- nautilus:::.gatherPseudoTrack(shuffled, "datetime", NULL, 5000)
  expect_equal(tr$lon, sort(tr$lon))              # ascending lon == chronological (track goes east)
})
