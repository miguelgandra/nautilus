# The fine-tuned cam-tag OCR model is NOT bundled (11 MB > CRAN size budget); it is fetched on demand from
# a GitHub release into the per-user cache and md5-verified. The cache / verification LOGIC is tested
# deterministically with a redirected cache and no network; the real 11 MB download round-trip is gated
# (skip_on_cran + skip_if_offline + opt-in NAUTILUS_TEST_DOWNLOAD=true) so routine test runs stay fast.

test_that("the cam OCR model constants are well formed", {
  expect_match(nautilus:::.CAM_MODEL_URL,
               "^https://github\\.com/miguelgandra/nautilus/releases/download/.+/cam\\.traineddata$")
  expect_match(nautilus:::.CAM_MODEL_MD5, "^[0-9a-f]{32}$")
  expect_true(is.numeric(nautilus:::.CAM_MODEL_BYTES) && nautilus:::.CAM_MODEL_BYTES > 1e6)
})

test_that(".camModelPath returns NULL on an empty cache when download is off", {
  withr::local_envvar(R_USER_CACHE_DIR = withr::local_tempdir())
  expect_null(nautilus:::.camModelPath(download = FALSE, quiet = TRUE))
})

test_that(".camModelPath rejects a corrupt cached model (md5 mismatch), never returns it", {
  withr::local_envvar(R_USER_CACHE_DIR = withr::local_tempdir())
  dest <- file.path(tools::R_user_dir("nautilus", "cache"), "cam.traineddata")
  dir.create(dirname(dest), recursive = TRUE, showWarnings = FALSE)
  writeLines("not the real model", dest)                                  # a file with the wrong md5
  expect_false(identical(unname(tools::md5sum(dest)), nautilus:::.CAM_MODEL_MD5))
  expect_null(nautilus:::.camModelPath(download = FALSE, quiet = TRUE))    # corrupt cache is not trusted
})

test_that("installCamOcrModel downloads, verifies, caches, and reuses the model", {
  skip_on_cran()
  skip_if_offline()
  skip_if_not(identical(Sys.getenv("NAUTILUS_TEST_DOWNLOAD"), "true"),
              "set NAUTILUS_TEST_DOWNLOAD=true to run the 11 MB OCR-model download test")
  withr::local_envvar(R_USER_CACHE_DIR = withr::local_tempdir())
  p1 <- installCamOcrModel(quiet = TRUE)
  expect_true(!is.null(p1) && file.exists(p1))
  expect_identical(unname(tools::md5sum(p1)), nautilus:::.CAM_MODEL_MD5)   # bytes verified
  p2 <- nautilus:::.camModelPath(download = FALSE, quiet = TRUE)           # second call: cache hit, no network
  expect_identical(normalizePath(p2), normalizePath(p1))
})
