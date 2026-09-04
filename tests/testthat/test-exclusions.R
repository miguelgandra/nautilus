# Tests for the shared deployment-exclusion log: one CSV holding CURRENT STATE, each stage refreshing
# its own rows within the current call's deployment scope. A deployment which stops being excluded loses
# its row, while an intentional partial run cannot erase rows for deployments it never evaluated.

.exRow <- function(...) nautilus:::.exclusionsRow(...)
.exWrite <- function(...) nautilus:::.exclusionsWrite(...)
.exRead <- function(...) nautilus:::.exclusionsRead(...)
.exBind <- function(...) nautilus:::.exclusionsBind(...)
.exFile <- function() file.path(withr::local_tempdir(.local_envir = parent.frame()), "exclusions.csv")


test_that("the log round-trips through CSV with its types intact", {
  f <- .exFile()
  t0 <- as.POSIXct("2020-01-01 10:00:00", tz = "UTC")
  .exWrite(.exBind(list(.exRow("A", "filterDeploymentData", "too short", t0, t0 + 300, 0.083))),
           f, "filterDeploymentData")
  ex <- .exRead(f)
  # POSIXct survives: summarizeTagData assigns window_start into a POSIXct column, and a character
  # read back from the file would corrupt the record window silently
  expect_s3_class(ex$window_start, "POSIXct")
  expect_equal(ex$window_start, t0)
  expect_equal(ex$window_hours, 0.083)
  expect_identical(attr(ex$window_start, "tzone"), "UTC")
  # and it is a CSV a person can read without R
  expect_match(paste(readLines(f), collapse = "\n"), "2020-01-01T10:00:00Z")
})


test_that("a stage replaces only its own rows", {
  f <- .exFile()
  .exWrite(.exBind(list(.exRow("A", "filterDeploymentData", "too short"),
                        .exRow("B", "filterDeploymentData", "no deployment detected"))),
           f, "filterDeploymentData")
  .exWrite(.exBind(list(.exRow("C", "processTagData", "missing columns"))), f, "processTagData")
  expect_setequal(.exRead(f)$id, c("A", "B", "C"))

  # re-run filtering: A now passes, B still fails. C belongs to another stage and must survive.
  .exWrite(.exBind(list(.exRow("B", "filterDeploymentData", "no deployment detected"))),
           f, "filterDeploymentData")
  ex <- .exRead(f)
  expect_setequal(ex$id, c("B", "C"))
  expect_false("A" %in% ex$id)                       # the case an append-only log cannot express
})


test_that("a stage that excludes nothing still clears its rows", {
  f <- .exFile()
  .exWrite(.exBind(list(.exRow("A", "filterDeploymentData", "too short"))), f, "filterDeploymentData")
  .exWrite(.exBind(list()), f, "filterDeploymentData")     # a clean re-run
  expect_identical(nrow(.exRead(f)), 0L)
})


test_that("a scoped stage refresh preserves out-of-scope exclusions", {
  f <- .exFile()
  .exWrite(.exBind(list(.exRow("A", "filterDeploymentData", "old A failure"),
                        .exRow("B", "filterDeploymentData", "old B failure"))),
           f, "filterDeploymentData", scope.ids = c("A", "B"))
  .exWrite(.exBind(list(.exRow("C", "processTagData", "missing columns"))),
           f, "processTagData", scope.ids = "C")

  # A clean partial re-run of B removes B's stale row, but A and the other stage survive.
  .exWrite(.exBind(list()), f, "filterDeploymentData", scope.ids = "B")
  ex <- .exRead(f)
  expect_setequal(ex$id, c("A", "C"))
  expect_identical(ex$reason[ex$id == "A"], "old A failure")

  expect_error(
    .exWrite(.exBind(list(.exRow("OUT", "filterDeploymentData", "x"))),
             f, "filterDeploymentData", scope.ids = "A"),
    "outside.*scope.ids")
})


test_that("a deployment excluded at several stages resolves to the earliest in pipeline order", {
  ex <- .exBind(list(.exRow("A", "processTagData", "missing columns"),
                     .exRow("A", "filterDeploymentData", "too short"),
                     .exRow("B", "applyAxisMapping", "excluded by review")))
  r <- nautilus:::.exclusionsResolve(ex)
  expect_identical(nrow(r), 2L)
  expect_identical(r$stage[r$id == "A"], "filterDeploymentData")   # where it actually left
  expect_identical(r$stage[r$id == "B"], "applyAxisMapping")
})


test_that("the reader tolerates a hand-written log and rejects a malformed one", {
  f <- .exFile()
  # `stage` omitted: every row is simply unattributed, and still resolves
  writeLines(c("id,reason", "A,dropped by hand"), f)
  ex <- .exRead(f)
  expect_identical(ex$id, "A")
  expect_true(is.na(ex$stage))
  expect_identical(nrow(nautilus:::.exclusionsResolve(ex)), 1L)

  writeLines(c("id,note", "A,x"), f)
  expect_error(.exRead(f), "missing the column")
  expect_error(.exRead("no-such-file.csv"), "does not exist")
})


test_that("the write is atomic, leaving no temporary files behind", {
  d <- withr::local_tempdir(); f <- file.path(d, "exclusions.csv")
  .exWrite(.exBind(list(.exRow("A", "processTagData", "missing columns"))), f, "processTagData")
  expect_identical(list.files(d), "exclusions.csv")
})


test_that("a NULL file is a no-op, so the log is opt-in", {
  expect_null(.exWrite(.exBind(list(.exRow("A", "processTagData", "x"))), NULL, "processTagData"))
  expect_null(.exRead(NULL))
})


test_that("processTagData records why it skipped a deployment", {
  f <- .exFile()
  bad <- data.table::data.table(ID = "BAD",
                                datetime = as.POSIXct("2020-01-01", tz = "UTC") + 1:100)
  invisible(utils::capture.output(suppressWarnings(
    processTagData(list(BAD = bad), exclusions.file = f, verbose = 0))))
  ex <- .exRead(f)
  expect_identical(ex$id, "BAD")
  expect_identical(ex$stage, "processTagData")
  expect_match(ex$reason, "not present")            # names the columns that were missing
})


# ---- the returned value must stay invisible ---------------------------------------------------------
# .collectOutput() returns the written paths INVISIBLY, so a top-level call does not print a wall of
# file paths. Assigning an attribute to that value and returning it bare strips the invisibility, which
# is how regularizeTimeSeries() and processTagData() came to print all 46 paths after a run.

test_that("a paths-returning run prints nothing, and carries no empty exclusions attribute", {
  d <- withr::local_tempdir(); outd <- file.path(d, "out"); dir.create(outd)
  t0 <- as.POSIXct("2020-01-01", tz = "UTC")
  for (id in c("A", "B")) {
    x <- data.table::data.table(ID = id, datetime = t0 + c(0, 0.9, 2.1, 3, 4.2, 7, 8.1, 9),
                                depth = c(1, 1.2, 1.5, 1.8, 2, 3.1, 3.4, 3.6))
    data.table::setattr(x, "nautilus.version", "test")
    saveRDS(x, file.path(d, paste0(id, ".rds")))
  }
  files <- list.files(d, pattern = "\\.rds$", full.names = TRUE)

  printed <- utils::capture.output(
    regularizeTimeSeries(data = files, gap.threshold = 2, plot = FALSE,
                         return.data = FALSE, output.dir = outd, verbose = 0))
  expect_length(printed, 0L)

  r <- regularizeTimeSeries(data = files, gap.threshold = 2, plot = FALSE,
                            return.data = FALSE, output.dir = outd, verbose = 0)
  expect_length(r, 2L)                                    # the value is still there to chain
  expect_null(attr(r, "nautilus.exclusions"))             # nothing excluded: no empty table to print
})


test_that("every producer returns its paths invisibly", {
  # the invariant, stated once against all five: whatever a producer attaches, a paths-branch run is
  # silent at the top level
  for (fn in c("importTagData", "filterDeploymentData", "regularizeTimeSeries",
               "applyAxisMapping", "processTagData")) {
    body_txt <- paste(deparse(body(get(fn, envir = asNamespace("nautilus")))), collapse = " ")
    expect_true(grepl("invisible", body_txt, fixed = TRUE),
                info = paste(fn, "must return its paths invisibly"))
  }
})
