# Tests for the shared verbose-output helpers in R/logging.R. These lock in the package-wide
# header/summary conventions every workflow function routes through (.log_header / .log_summary),
# so the uniform visual style cannot drift. cli output is captured with cli::cli_fmt().

test_that(".log_header renders the framed header with intro, bullets and arrow (level 1)", {
  out <- paste(cli::cli_fmt(nautilus:::.log_header(
    1L, "demoFn", "Welcome aboard: doing the thing",
    bullets = c("Input: 3 tags", "Output: /tmp/out"),
    arrow = "Mode: batch run")), collapse = "\n")

  expect_match(out, "demoFn")                       # title rule
  expect_match(out, "nautilus")                     # right-hand rule label
  expect_match(out, "Welcome aboard: doing the thing")
  expect_match(out, "Input: 3 tags")
  expect_match(out, "Output: /tmp/out")
  expect_match(out, "Mode: batch run")
  expect_match(out, "====|══")            # the full-width frame (ASCII or UTF-8)
})

test_that(".log_header is silent at level 0 and omits an absent arrow", {
  expect_length(cli::cli_fmt(nautilus:::.log_header(0L, "x", "y")), 0L)

  out <- paste(cli::cli_fmt(nautilus:::.log_header(
    1L, "demoFn", "An intro", bullets = "Input: 1 tag")), collapse = "\n")
  expect_match(out, "Input: 1 tag")
  expect_false(grepl("Mode:|Method:|→|->", out))   # no arrow line when arrow = NULL
})

test_that(".log_summary opens the SUMMARY block at level 1 and is silent at level 0", {
  out <- paste(cli::cli_fmt(nautilus:::.log_summary(1L)), collapse = "\n")
  expect_match(out, "SUMMARY")
  expect_length(cli::cli_fmt(nautilus:::.log_summary(0L)), 0L)
})

test_that("a workflow function emits the unified header + SUMMARY block (summarizeTagData)", {
  d <- data.table::data.table(ID = "A01",
                              datetime = as.POSIXct("2020-01-01", tz = "UTC") + 0:9,
                              depth = abs(stats::rnorm(10)))
  data.table::setattr(d, "nautilus.version", "test")

  out <- paste(cli::cli_fmt(suppressWarnings(
    summarizeTagData(list(A01 = d), verbose = 1))), collapse = "\n")

  expect_match(out, "summarizeTagData")                       # framed header rule
  expect_match(out, "Summarising the deployments")            # intro (nautical prefix removed)
  expect_match(out, "Input: 1 tag")
  expect_match(out, "SUMMARY")                                # final summary block
  expect_match(out, "summarised")
})

test_that("the alert loggers treat their text as literal - braces do NOT crash cli (glue interpolation bug)", {
  # regression: .log_detail/.log_skip/.log_info/.log_ok used to pass caller text as the cli TEMPLATE, so any
  # literal { } (e.g. a braced column name, a path, a regex) was evaluated as a glue expression and errored.
  for (fn in c(".log_detail", ".log_info", ".log_ok", ".log_skip")) {
    f <- get(fn, envir = asNamespace("nautilus"))
    expect_no_error(cli::cli_fmt(f(2L, "no {temp} data (plotted black): 3/12")))   # the reported case
    expect_no_error(cli::cli_fmt(f(2L, "reading C:/dir{x}/file.csv")))
    # the braces survive as literal text rather than being interpreted
    expect_match(paste(cli::cli_fmt(f(2L, "value {y} here")), collapse = ""), "{y}", fixed = TRUE)
  }
})

test_that(".log_progress_* honours the level window and no-ops safely", {
  # default window [2, Inf]: the light-reporter case (bar at detailed only)
  expect_null(nautilus:::.log_progress_start(1L, 10L))          # normal -> no bar
  expect_null(nautilus:::.log_progress_start(0L, 10L))          # quiet  -> no bar
  expect_null(nautilus:::.log_progress_start(2L, 0L))           # nothing to iterate -> no bar
  expect_false(is.null(nautilus:::.log_progress_start(2L, 5L))) # detailed -> a real bar
  # NORMAL-only window [1, 1]: the streaming reconstruction case (bar at normal, suppressed at detailed)
  expect_false(is.null(nautilus:::.log_progress_start(1L, 5L, min.level = 1L, max.level = 1L)))
  expect_null(nautilus:::.log_progress_start(2L, 5L, min.level = 1L, max.level = 1L))   # detailed streams instead
  expect_null(nautilus:::.log_progress_start(0L, 5L, min.level = 1L, max.level = 1L))
  expect_no_error({ nautilus:::.log_progress_step(NULL); nautilus:::.log_progress_done(NULL) })   # NULL is a no-op
  expect_no_error({                                             # a real bar, stepped + finished cleanly
    pb <- nautilus:::.log_progress_start(2L, 3L, "Testing")
    for (i in 1:3) nautilus:::.log_progress_step(pb)
    nautilus:::.log_progress_done(pb)
  })
})
