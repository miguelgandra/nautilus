# Tests for reencodeVideos(): argument validation (cheap aborts run without ffmpeg; the ffmpeg-gated
# paths are skipped when ffmpeg is absent). Actual encoding is not exercised (needs real videos).

test_that("cheap argument validation aborts before touching ffmpeg", {
  d <- tempfile(); dir.create(d); on.exit(unlink(d, recursive = TRUE), add = TRUE)
  expect_error(reencodeVideos(d, crf = 99, verbose = FALSE), "crf", ignore.case = TRUE)
  expect_error(reencodeVideos(d, preset = "turbo", verbose = FALSE), "preset", ignore.case = TRUE)
  expect_error(reencodeVideos(d, video.quality = 0, verbose = FALSE), "video.quality", ignore.case = TRUE)
  expect_error(reencodeVideos("/no/such/dir", verbose = FALSE), "directory", ignore.case = TRUE)
})

test_that("aborts on an empty directory / unavailable encoder (ffmpeg-gated)", {
  skip_if(!nzchar(Sys.which("ffmpeg")), "ffmpeg not available")
  d <- tempfile(); dir.create(d); on.exit(unlink(d, recursive = TRUE), add = TRUE)
  expect_error(reencodeVideos(d, verbose = FALSE), "No .* files", ignore.case = TRUE)
  # a non-existent encoder is rejected against the local ffmpeg build
  expect_error(reencodeVideos(d, encoder = "definitely_not_an_encoder", verbose = FALSE),
               "not available", ignore.case = TRUE)
})
