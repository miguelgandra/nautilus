# Package-wide standard: a function that expects one or more input datasets/files must FAIL LOUDLY when
# its input resolves to nothing (empty vector/list, 0-row data.frame) - most often a mistyped directory
# passed through list.files() yielding character(0). Silently returning an empty result masks the typo.
# This covers the shared .assert_nonempty() helper and the functions that resolve file paths themselves
# (those routed through .resolveInput() are guarded there and covered in their own test files).

test_that(".assert_nonempty() aborts on empty input and passes non-empty through", {
  expect_error(nautilus:::.assert_nonempty(character(0), "data"), "empty", ignore.case = TRUE)
  expect_error(nautilus:::.assert_nonempty(list(), "data"), "empty", ignore.case = TRUE)
  expect_error(nautilus:::.assert_nonempty(data.frame(), "data"), "empty", ignore.case = TRUE)   # 0 rows
  expect_error(nautilus:::.assert_nonempty(NULL, "data"), "empty", ignore.case = TRUE)
  # the arg name and the 'what' label surface in the message (helps the user locate the mistake)
  expect_error(nautilus:::.assert_nonempty(character(0), "data.folders", "input folders"),
               "data.folders", ignore.case = TRUE)
  expect_error(nautilus:::.assert_nonempty(character(0), "data.folders", "input folders"),
               "input folders", ignore.case = TRUE)
  # non-empty inputs pass silently, returning invisibly
  expect_true(nautilus:::.assert_nonempty(list(a = 1), "data"))
  expect_true(nautilus:::.assert_nonempty(c("a.rds", "b.rds"), "data"))
  expect_true(nautilus:::.assert_nonempty(data.frame(x = 1), "data"))
})

test_that("functions that resolve file paths themselves reject an empty character vector or list", {
  # each of these has its own is.character(data) filepath branch (not routed through .resolveInput);
  # a typo'd list.files() -> character(0), or an explicit list(), must abort - not no-op to empty output.
  fns <- c("processTagData", "checkTagMapping", "calculateTailBeats",
           "filterDeploymentData", "findValidationSegments", "extractFeatures")
  for (fn in fns) {
    f <- get(fn, envir = asNamespace("nautilus"))
    expect_error(f(character(0)), "empty", ignore.case = TRUE, info = paste(fn, "on character(0)"))
    expect_error(f(list()),       "empty", ignore.case = TRUE, info = paste(fn, "on list()"))
  }
})

test_that(".resolveInput-routed functions also reject empty input (shared guard)", {
  expect_error(summarizeTagData(character(0), verbose = FALSE), "empty", ignore.case = TRUE)
  expect_error(reconstructTrack(list(), verbose = FALSE), "empty", ignore.case = TRUE)
  expect_error(trackMetrics(character(0), verbose = FALSE), "empty", ignore.case = TRUE)
  expect_error(exportForSSM(list(), verbose = FALSE), "empty", ignore.case = TRUE)
})
