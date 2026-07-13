# Tests for the internal validation helpers (R/validate.R) and verbosity normaliser.

test_that(".assert_flag accepts a single logical and rejects everything else", {
  expect_invisible(nautilus:::.assert_flag(TRUE, "x"))
  expect_error(nautilus:::.assert_flag(NA, "x"), "single")
  expect_error(nautilus:::.assert_flag(c(TRUE, FALSE), "x"), "single")
  expect_error(nautilus:::.assert_flag("yes", "x"), "single")
})

test_that(".assert_string handles null_ok and rejects non-strings", {
  expect_invisible(nautilus:::.assert_string("a", "x"))
  expect_invisible(nautilus:::.assert_string(NULL, "x", null_ok = TRUE))
  expect_error(nautilus:::.assert_string(NULL, "x"), "NULL")
  expect_error(nautilus:::.assert_string(c("a", "b"), "x"), "single")
})

test_that(".assert_number enforces range", {
  expect_invisible(nautilus:::.assert_number(0.5, "x", min = 0, max = 1))
  expect_error(nautilus:::.assert_number(2, "x", min = 0, max = 1), "between")
  expect_error(nautilus:::.assert_number(Inf, "x"), "finite")
  expect_error(nautilus:::.assert_number(NULL, "x"), "NULL")
  expect_invisible(nautilus:::.assert_number(NULL, "x", null_ok = TRUE))
})

test_that(".assert_count requires positive whole numbers", {
  expect_invisible(nautilus:::.assert_count(3, "x"))
  expect_error(nautilus:::.assert_count(2.5, "x"), ">=")
  expect_error(nautilus:::.assert_count(0, "x", min = 1), ">=")
})

test_that(".assert_choice matches against allowed values", {
  expect_invisible(nautilus:::.assert_choice("linear", "method", c("linear", "spline")))
  expect_error(nautilus:::.assert_choice("cubic", "method", c("linear", "spline")), "must be one of")
})

test_that(".assert_columns reports the missing columns", {
  df <- data.frame(ID = 1, start = 2)
  expect_invisible(nautilus:::.assert_columns(df, c("ID", "start"), "x"))
  expect_error(nautilus:::.assert_columns(df, c("ID", "start", "end"), "x"), "end")
})

test_that(".assert_dir is fail-fast (never creates)", {
  d <- file.path(tempdir(), paste0("nope_", as.integer(runif(1, 1, 1e7))))
  expect_error(nautilus:::.assert_dir(d, "output.dir"), "does not exist")
  expect_false(dir.exists(d))                       # must NOT have been created
  expect_invisible(nautilus:::.assert_dir(tempdir(), "output.dir"))
  expect_invisible(nautilus:::.assert_dir(NULL, "output.dir"))
})

test_that(".assert_writable_file checks the parent dir and extension", {
  expect_invisible(nautilus:::.assert_writable_file(file.path(tempdir(), "x.pdf"), "plot.file", ext = "pdf"))
  expect_error(nautilus:::.assert_writable_file(file.path(tempdir(), "x.png"), "plot.file", ext = "pdf"), "must end in")
  bad <- file.path(tempdir(), "missing_dir_xyz", "x.pdf")
  expect_error(nautilus:::.assert_writable_file(bad, "plot.file"), "does not exist")
})

test_that(".abort throws a clean cli error with no call prefix", {
  f <- function() nautilus:::.abort("{.arg x} is bad.")
  e <- tryCatch(f(), error = function(e) e)
  expect_s3_class(e, "rlang_error")
  expect_match(conditionMessage(e), "x")            # arg name rendered
  expect_null(conditionCall(e))                      # call = NULL => no "Error in f():"
})

test_that(".abort interpolates in the caller frame and supports cli bullets", {
  g <- function(arg) nautilus:::.abort(c("bad {.arg {arg}}.", "i" = "fix it"))
  expect_error(g("weight"), "weight")
})

test_that(".assert_parallel is a no-op for <= 1 core and validates otherwise", {
  expect_invisible(nautilus:::.assert_parallel(1))
  expect_invisible(nautilus:::.assert_parallel(NULL))
  big <- parallel::detectCores() + 10L
  have_pkgs <- all(vapply(c("foreach", "doSNOW", "parallel"),
                          requireNamespace, logical(1), quietly = TRUE))
  if (have_pkgs) {
    expect_error(nautilus:::.assert_parallel(big), "exceeds")     # core-count check
  } else {
    expect_error(nautilus:::.assert_parallel(big), "not installed")  # missing-package check fires first
  }
})

test_that(".assert_compress accepts logical/codec and rejects the rest", {
  expect_invisible(nautilus:::.assert_compress(TRUE))
  expect_invisible(nautilus:::.assert_compress(FALSE))
  expect_invisible(nautilus:::.assert_compress("xz"))
  expect_error(nautilus:::.assert_compress("zstd"), "compress")
  expect_error(nautilus:::.assert_compress(NA), "compress")
  expect_error(nautilus:::.assert_compress(1), "compress")
})

test_that(".verbosity maps logical / integer / string to 0:2", {
  expect_equal(nautilus:::.verbosity(TRUE), 1L)
  expect_equal(nautilus:::.verbosity(FALSE), 0L)
  expect_equal(nautilus:::.verbosity(2), 2L)
  expect_equal(nautilus:::.verbosity(5), 2L)       # clamped
  expect_equal(nautilus:::.verbosity("quiet"), 0L)
  expect_equal(nautilus:::.verbosity("detailed"), 2L)
})
