# Tests for extractFeatures() - rolling-window feature extraction.
#
# Written as the safety net for the stage 1/2 audit refactor: this function had NO test file, so the
# contract it already honours is captured here FIRST (so the refactor cannot quietly change it), and the
# audited defects are pinned alongside so the fixes are demonstrable rather than asserted.

.ef_tag <- function(id = "A01", n = 600, hz = 1, seed = 1) {
  set.seed(seed)
  t0 <- as.POSIXct("2020-08-22 12:00:00", tz = "UTC")
  d <- data.table::data.table(
    ID = id, datetime = t0 + seq_len(n) / hz,
    depth   = pmax(0, 20 + 15 * sin(seq_len(n) / 60)),
    pitch   = 10 * sin(seq_len(n) / 40),
    roll    = 5 * cos(seq_len(n) / 30),
    heading = (seq_len(n) * 0.7) %% 360,
    vedba   = abs(stats::rnorm(n, .3, .1)),
    odba    = abs(stats::rnorm(n, .4, .1)))
  data.table::setattr(d, "nautilus.version", "test")
  m <- nautilus:::.newNautilusMeta(); m$id <- id
  nautilus:::new_nautilus_tag(d, m)
}
.ef <- function(...) suppressWarnings(suppressMessages(
  { o <- NULL; invisible(utils::capture.output(o <- extractFeatures(...))); o }))


# ---- the contract that already works: lock it before refactoring ------------------------------------

test_that("the basic contract holds: one named element per deployment, features named var_metric", {
  res <- .ef(list(A01 = .ef_tag()), variables = c("depth", "pitch"),
             metrics = c("mean", "sd"), window.size = 30)
  expect_named(res, "A01")
  x <- res$A01
  expect_s3_class(x, "data.table")
  # identifier and time first, then one column per variable x metric pair
  expect_equal(names(x)[1:2], c("ID", "datetime"))
  expect_setequal(setdiff(names(x), c("ID", "datetime")),
                  c("depth_mean", "pitch_mean", "depth_sd", "pitch_sd"))
  expect_gt(nrow(x), 0L)
  expect_true(all(vapply(x[, -(1:2)], is.numeric, logical(1))))
})

test_that("a rolling mean over a constant signal returns that constant", {
  # the least interesting possible check, and the one that would catch a window/indexing regression
  tg <- .ef_tag(); dt <- data.table::copy(tg); dt[, depth := 7]
  tg2 <- nautilus:::.restoreMeta(dt, nautilus:::.getMeta(tg))
  res <- .ef(list(A01 = tg2), variables = "depth", metrics = "mean", window.size = 30)
  v <- res$A01$depth_mean
  expect_true(all(abs(v[!is.na(v)] - 7) < 1e-9))
})

test_that("multiple deployments are processed independently and keep their own ids", {
  res <- .ef(list(A01 = .ef_tag("A01", seed = 1), B02 = .ef_tag("B02", seed = 2)),
             variables = "depth", metrics = "mean", window.size = 30)
  expect_setequal(names(res), c("A01", "B02"))
  expect_equal(unique(res$A01$ID), "A01")
  expect_equal(unique(res$B02$ID), "B02")
})


# ---- audited defects: each fails before the stage-1 fix and passes after ----------------------------

test_that("id.col is honoured on output, not just accepted (audit: :495)", {
  # `id.col` is documented and exported, but the identifier column was written literally as "ID" while
  # later steps looked it up by `id.col` - so any non-default value aborted the run outright.
  tg <- .ef_tag()
  dt <- data.table::copy(tg); data.table::setnames(dt, "ID", "shark_id")
  tg2 <- nautilus:::.restoreMeta(dt, nautilus:::.getMeta(tg))
  res <- .ef(list(A01 = tg2), variables = "depth", metrics = "mean",
             window.size = 30, id.col = "shark_id")
  expect_true("shark_id" %in% names(res$A01))
  expect_false("ID" %in% names(res$A01))
  expect_equal(unique(res$A01$shark_id), "A01")
})

test_that("downsample.to is a FREQUENCY in Hz, matching processTagData (audit: :24)", {
  # the roxygen said seconds while the code computes 1/downsample.to - a 25x silent error for anyone
  # following the documentation. processTagData documents the same argument correctly as Hz.
  res <- .ef(list(A01 = .ef_tag(n = 600)), variables = "depth", metrics = "mean",
             window.size = 30, downsample.to = 0.2)
  gaps <- as.numeric(diff(res$A01$datetime), units = "secs")
  expect_true(all(abs(gaps - 5) < 1e-6))          # 0.2 Hz -> one row every 5 s
})

test_that("downsample.to rejects values that cannot be a frequency (audit: :24)", {
  tg <- list(A01 = .ef_tag())
  expect_error(extractFeatures(tg, variables = "depth", metrics = "mean", downsample.to = -1),
               "downsample.to", ignore.case = TRUE)
  expect_error(extractFeatures(tg, variables = "depth", metrics = "mean", downsample.to = "5"),
               "downsample.to", ignore.case = TRUE)
})

test_that("a linear-only metric on a circular variable is rejected up front (audit: :177)", {
  # validation was not variable-aware: the pair passed the check and failed deep in the loop
  expect_error(
    extractFeatures(list(A01 = .ef_tag()), variables = "heading", metrics = "entropy",
                    window.size = 30, circular.variables = "heading"),
    "circular", ignore.case = TRUE)
})

test_that("a requested variable absent from a deployment is reported, not a cryptic failure (audit: :674)", {
  expect_error(
    extractFeatures(list(A01 = .ef_tag()), variables = "no_such_channel", metrics = "mean",
                    window.size = 30),
    "no_such_channel")
})

test_that("the function opens no output sink (audit: :567)", {
  # sink(tempfile()) had no on.exit, so an error inside the downsample block left the CALLER's console
  # redirected to a temp file. It was also provably inert - removing it left output and results
  # byte-identical - so it is simply gone. Asserting on sink.number() around a call is NOT enough: it
  # only catches an unbalanced sink on the paths that happen to error. Assert the construct is absent.
  f <- "../../R/extractFeatures.R"
  skip_if_not(file.exists(f), "source not available (installed package)")
  expect_false(any(grepl("^\\s*sink\\(", readLines(f, warn = FALSE))))

  # and the balance holds across a run that exercises the downsampling block
  before <- sink.number()
  invisible(.ef(list(A01 = .ef_tag()), variables = "depth", metrics = "mean",
                window.size = 30, downsample.to = 0.2))
  expect_equal(sink.number(), before)
})


# ---- scientific defects: verified numerically during the audit --------------------------------------

test_that("the circular median is the median, not its antipode (audit: :827)", {
  # the implementation minimised sum(pi - distance), i.e. MAXIMISED total angular distance, returning a
  # point roughly opposite the data. Seven of these nine headings sit at ~90 deg.
  x <- c(85, 88, 89, 90, 91, 92, 95, 270, 275)
  med <- nautilus:::.circularMetric(x, "median")
  angdist <- function(a, b) { d <- abs(a - b) %% 360; pmin(d, 360 - d) }
  expect_lt(angdist(med, 90), 15)                                  # near the cluster...
  expect_lt(sum(angdist(x, med)), sum(angdist(x, 270)))            # ...and better than the antipode
})

test_that("heading autocorrelation does not depend on absolute bearing (audit: :1108)", {
  # a LINEAR acf on wrapped degrees: the same wobble scored 0.49 near north (crossing the 0/360 cut)
  # and 0.65 near south. Identical behaviour must give an identical answer.
  skip_if_not_installed("zoo")
  set.seed(1); wob <- cumsum(stats::rnorm(400, 0, 2)); wob <- wob - mean(wob)
  a <- nautilus:::.heading_autocorr_avg((wob + 0)   %% 360, window = 60)
  b <- nautilus:::.heading_autocorr_avg((wob + 180) %% 360, window = 60)
  expect_lt(abs(mean(a, na.rm = TRUE) - mean(b, na.rm = TRUE)), 0.02)
})

test_that("oscillation_regularity varies along the record (audit: :1133)", {
  # it returned ONE scalar for the whole deployment, recycled - a rolling feature that never rolled
  skip_if_not_installed("zoo")
  set.seed(1)
  sig <- c(sin(seq(0, 40 * pi, length.out = 300)),               # regular
           sin(cumsum(stats::runif(300, 0.05, 0.6))))            # irregular
  v <- nautilus:::.oscillation_regularity(sig, window = 60)
  expect_gt(length(v), 1L)
  expect_gt(stats::sd(v, na.rm = TRUE), 0)
})


# ---- stage 2: the package spine ---------------------------------------------------------------------

test_that("verbose is honoured: quiet is silent, normal prints header/blocks/summary", {
  tg <- list(A01 = .ef_tag(), B02 = .ef_tag("B02", seed = 2))
  args <- list(variables = "depth", metrics = "mean", window.size = 30)

  # invisible(): capture.output(type = "message") still evaluates its expression VISIBLY, so the
  # returned feature table would auto-print to stdout and swamp the check
  quiet <- utils::capture.output(invisible(suppressWarnings(suppressMessages(
    do.call(extractFeatures, c(list(tg), args, verbose = 0))))), type = "message")
  expect_length(quiet, 0L)      # the function printed unconditionally with cat() before

  # NOT suppressMessages here: cli writes through the condition system, so suppressing messages would
  # discard exactly the output under test - the check would then pass on an empty string
  out <- utils::capture.output(invisible(suppressWarnings(
    do.call(extractFeatures, c(list(tg), args, verbose = "normal")))), type = "message")
  txt <- paste(out, collapse = "\n")
  expect_match(txt, "extractFeatures")            # framed header, package style
  expect_match(txt, "A01 \\(1/2\\)")              # per-deployment block, numbered
  expect_match(txt, "SUMMARY")                    # a summary block exists at all
  expect_match(txt, "runtime")
})

test_that("the output carries an extractFeatures provenance record", {
  res <- .ef(list(A01 = .ef_tag()), variables = "depth", metrics = "mean", window.size = 30)
  pr <- nautilus:::.getMeta(res$A01)$processing
  steps <- vapply(pr, function(x) as.character(x$step), character(1))
  expect_true("extractFeatures" %in% steps)
  rec <- pr[[which(steps == "extractFeatures")[1]]]
  # the record must state what was actually computed, not just that something was
  expect_equal(rec$n_features, 1L)
  expect_equal(rec$window_size, 30)
  expect_equal(rec$variables, "depth")
  expect_equal(rec$metrics, "mean")
})

test_that("the feature helpers are package internals, not globals", {
  # they were top-level un-dot-prefixed functions - the only file in the package exposing internals
  # that way - which also forced the parallel path to hand-enumerate them in a foreach .export list
  expect_true(is.function(nautilus:::.net_heading_change))
  expect_true(is.function(nautilus:::.oscillation_regularity))
  expect_false(exists("net_heading_change", envir = globalenv(), inherits = FALSE))
  f <- "../../R/extractFeatures.R"
  skip_if_not(file.exists(f), "source not available")
  src <- readLines(f, warn = FALSE)
  expect_false(any(grepl("\\.export\\s*=", src)))          # the stale-prone list is gone
  expect_false(any(grepl("txtProgressBar", src)))          # progress goes through the shared helper
  expect_false(any(grepl("^\\s*cat\\(", src)))             # output goes through .log_*
})

test_that("enhanced metrics are requested by their PUBLIC name, without a dot", {
  # Regression guard for a bug introduced while dot-prefixing the internal helpers: the metric names
  # users type happen to match the helper function names, so a blanket rename also renamed 71 string
  # literals - the valid-metric vocabulary, the dispatch, the docs and the error messages. Requesting
  # `metrics = "net_heading_change"` then failed with a message offering ".net_heading_change" instead.
  # No test requested an enhanced metric BY NAME, which is exactly why it slipped through.
  valid <- utils::capture.output(try(suppressWarnings(extractFeatures(
    list(A01 = .ef_tag()), variables = "heading", metrics = "definitely_not_a_metric",
    window.size = 30, enhanced.features = TRUE, verbose = 0)), silent = TRUE), type = "message")
  txt <- paste(valid, collapse = " ")
  # whatever the vocabulary is, it must not be advertising dot-prefixed internals
  expect_false(grepl('"\\.[a-z_]+"', txt))

  # and a real enhanced metric must get PAST name validation (it may still need a Suggests package)
  err <- tryCatch({
    suppressWarnings(suppressMessages(extractFeatures(
      list(A01 = .ef_tag()), variables = "heading", metrics = "net_heading_change",
      window.size = 30, enhanced.features = TRUE, verbose = 0)))
    ""
  }, error = function(e) conditionMessage(e))
  expect_false(grepl("Invalid metric", err))       # the name is recognised...
  expect_false(grepl("\\.net_heading_change", err)) # ...and nothing offers the internal name
})

test_that("enhanced features run without the circular package, which did nothing", {
  # `circular` was a hard requirement for enhanced.features = TRUE. All five call sites wrapped a
  # heading in circular::circular() and then did the wrap-correction by hand; the wrapper is inert for
  # every operation applied to it (diff / data.table::shift / zoo::rollapply / the values themselves
  # were bit-identical with and without it, and all five affected helpers gave identical output). It
  # was blocking the whole feature set for nothing.
  skip_if_not_installed("zoo")
  set.seed(1); n <- 900
  d <- data.table::data.table(
    ID = "A01", datetime = as.POSIXct("2020-08-22", tz = "UTC") + seq_len(n),
    heading = (cumsum(stats::rnorm(n, 3, 20))) %% 360)
  data.table::setattr(d, "nautilus.version", "test")
  m <- nautilus:::.newNautilusMeta(); m$id <- "A01"
  tg <- list(A01 = nautilus:::new_nautilus_tag(d, m))

  for (mt in c("net_heading_change", "circling_behavior", "turning_rate_variability")) {
    res <- .ef(tg, variables = "heading", metrics = mt, window.size = 60,
               enhanced.features = TRUE, verbose = 0)
    expect_gt(nrow(res$A01), 0L)
  }
  # the source must not reintroduce the dependency
  f <- "../../R/utils-features.R"
  skip_if_not(file.exists(f), "source not available")
  expect_false(any(grepl("^\\s*h <- circular::", readLines(f, warn = FALSE))))
})

test_that("feeding_posture_index is not offered, because it was never deliverable", {
  # it appeared in the valid-metric list and the docs, but its dispatch branch was commented out - it
  # needs pitch_sd/roll_sd/odba_mean/vedba_mean, i.e. the OUTPUT of a previous extractFeatures pass,
  # so it could only ever have worked as a second-order feature. Requesting it validated and then
  # failed with "Unknown enhanced metric". Removed rather than left as a trap.
  expect_error(
    extractFeatures(list(A01 = .ef_tag()), variables = "pitch", metrics = "feeding_posture_index",
                    window.size = 30, enhanced.features = TRUE, verbose = 0),
    "Invalid metric", ignore.case = TRUE)
  f <- "../../R/extractFeatures.R"
  skip_if_not(file.exists(f), "source not available")
  expect_false(any(grepl("feeding_posture_index", readLines(f, warn = FALSE))))
})


# ---- stage 3: arguments are validated, and errors name the argument at fault -------------------------

test_that("structural arguments are validated up front, not deep in the loop", {
  # each of these used to fail with an error naming an internal symbol rather than the argument, or -
  # worse - to be accepted and silently produce nothing
  tg <- list(A01 = .ef_tag())
  base <- list(variables = "depth", metrics = "mean", window.size = 30, verbose = 0)

  # window.size = 0 was ACCEPTED and returned zero rows
  expect_error(do.call(extractFeatures, c(list(tg), utils::modifyList(base, list(window.size = 0)))),
               "window.size", ignore.case = TRUE)
  expect_error(do.call(extractFeatures, c(list(tg), utils::modifyList(base, list(window.size = -5)))),
               "window.size", ignore.case = TRUE)
  # n.cores <= 0 fell into the parallel branch with no backend ("could not find function %dopar%")
  expect_error(do.call(extractFeatures, c(list(tg), utils::modifyList(base, list(n.cores = 0)))),
               "n.cores", ignore.case = TRUE)
  # a mistyped response.aggregation died with "object 'lab' not found"
  expect_error(do.call(extractFeatures, c(list(tg), utils::modifyList(base,
                 list(response.col = "depth", response.aggregation = "TYPO")))),
               "majority", ignore.case = TRUE)
})

test_that("an enhanced metric named without the flag says so, rather than 'invalid'", {
  # the metric IS valid - it is gated. Reporting it as invalid sends the reader hunting for a typo.
  err <- tryCatch(extractFeatures(list(A01 = .ef_tag()), variables = "depth", metrics = "movement_jerk",
                                  window.size = 30, verbose = 0),
                  error = function(e) conditionMessage(e))
  expect_match(err, "enhanced.features", fixed = TRUE)
  expect_false(grepl("Invalid metric", err))
})

test_that("supplying parameter.grid AND variables/metrics warns that the grid wins", {
  # the grid silently took precedence, so a run could compute something other than what was asked for
  pg <- data.frame(variable = "pitch", metric = "sd", stringsAsFactors = FALSE)
  w <- character(0)
  withCallingHandlers(
    invisible(utils::capture.output(extractFeatures(
      list(A01 = .ef_tag()), variables = "depth", metrics = "mean",
      window.size = 30, parameter.grid = pg, verbose = 0))),
    warning = function(e) { w <<- c(w, conditionMessage(e)); invokeRestart("muffleWarning") })
  expect_true(any(grepl("parameter.grid", w, fixed = TRUE)))

  # ...and the grid really is what was computed
  res <- .ef(list(A01 = .ef_tag()), variables = "depth", metrics = "mean",
             window.size = 30, parameter.grid = pg, verbose = 0)
  expect_true("pitch_sd" %in% names(res$A01))
  expect_false("depth_mean" %in% names(res$A01))
})


# ---- stage 4: the documented contract is the actual contract -----------------------------------------

test_that("the documented output schema and window semantics hold", {
  # Documentation drifts from code silently. These assert the specific claims the roxygen makes, so a
  # future change to windowing or naming breaks a test rather than quietly making the docs wrong.
  res <- .ef(list(A01 = .ef_tag(n = 600)), variables = c("depth", "pitch"),
             metrics = "mean", window.size = 30, verbose = 0)
  x <- res$A01
  # "one column per grid row, named <variable>_<metric>; identifier and datetime come first"
  expect_equal(names(x)[1:2], c("ID", "datetime"))
  expect_setequal(setdiff(names(x), c("ID", "datetime")), c("depth_mean", "pitch_mean"))
  # "sliding windows are CENTRED, so roughly half a window at each end yields NA" and those rows go
  expect_equal(600 - nrow(x), 29)

  # "with aggregate = TRUE the record is tiled into non-overlapping windows"
  ra <- .ef(list(A01 = .ef_tag(n = 600)), variables = "depth", metrics = "mean",
            window.size = 30, aggregate = TRUE, verbose = 0)
  expect_equal(unique(round(as.numeric(diff(ra$A01$datetime), units = "secs"))), 30)

  # "a deployment shorter than the widest requested window loses every row"
  short <- .ef(list(A01 = .ef_tag(n = 40)), variables = "depth", metrics = "mean",
               window.size = 120, verbose = 0)
  expect_equal(nrow(short$A01), 0L)
})

test_that("the documented circular metric set is exactly what is accepted", {
  # the roxygen lists seven circular metrics; anything narrower or wider makes the table wrong
  for (mt in c("mean", "median", "sd", "range", "iqr", "mrl", "rate")) {
    expect_no_error(.ef(list(A01 = .ef_tag()), variables = "heading", metrics = mt,
                        window.size = 30, circular.variables = "heading", verbose = 0))
  }
  # and a linear-only metric is refused for a circular variable
  expect_error(extractFeatures(list(A01 = .ef_tag()), variables = "heading", metrics = "skewness",
                               window.size = 30, circular.variables = "heading", verbose = 0),
               "circular", ignore.case = TRUE)
})
