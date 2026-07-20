# Tests for plotTimeAtDepth() (profile-grid model): the tidy return table (data product), the
# duration-weighting, binning (incl. surface-fine defaults), grouping, diel split, and rendering.

.mk_tad <- function(id, depth_fun, n = 300, species = "A", lon = -8, lat = 37) {
  t0 <- as.POSIXct("2021-06-01 00:00", tz = "UTC")
  d <- pmax(0, depth_fun(n))
  data.table::data.table(ID = id, datetime = t0 + seq_len(n) * 120, depth = d,
                         temp = pmax(11, 25 - 0.03 * d), lon = lon, lat = lat, species = species)
}
.tad_cohort <- function() list(
  A01 = .mk_tad("A01", function(n) abs(rnorm(n, 8, 5)),  species = "shallow"),
  A02 = .mk_tad("A02", function(n) abs(rnorm(n, 60, 15)), species = "deep"),
  A03 = .mk_tad("A03", function(n) abs(rnorm(n, 30, 20)), species = "deep"))
.to_pdf <- function(...) {
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf), add = TRUE)
  res <- suppressMessages(plotTimeAtDepth(..., plot = FALSE, plot.file = pf, verbose = FALSE))
  list(res = res, size = file.size(pf))
}

test_that("returns a tidy id x variable x phase x bin table; pct sums to 100 per id/variable/phase", {
  out <- .to_pdf(.tad_cohort())
  s <- out$res
  expect_named(s, c("id", "group", "variable", "phase", "bin_min", "bin_max", "bin_mid", "hours", "pct"))
  expect_setequal(unique(s$id), c("A01", "A02", "A03"))
  expect_equal(unique(s$variable), "depth"); expect_equal(unique(s$phase), "all")
  sums <- tapply(s$pct, s$id, sum)
  expect_true(all(abs(sums - 100) < 1e-6))
  expect_true(out$size > 0)
})

test_that("time is DURATION-weighted, not row-counted, and gaps are capped", {
  t <- as.POSIXct("2021-01-01", tz = "UTC") + c(seq_len(100), 1e6 + seq_len(100))
  g <- data.table::data.table(ID = "G", datetime = t, depth = c(rep(5, 100), rep(80, 100)))
  s <- .to_pdf(list(G = g), bin.width = 20)$res
  shallow <- sum(s$hours[s$bin_mid < 40]); deep <- sum(s$hours[s$bin_mid >= 40])
  expect_equal(shallow, deep, tolerance = 0.02)                # gap NOT credited to the shallow bin
  expect_lt(shallow + deep, 0.2)                               # ~200 s total, not ~1e6 s
})

test_that(".timeAtDepthBins weights a variable-rate record by true durations", {
  tb <- nautilus:::.timeAtDepthBins(v = c(5, 15, 15), tnum = c(0, 10, 11), breaks = c(0, 10, 20), gap.factor = 3)
  expect_gt(tb$time[1], tb$time[2])
  expect_equal(tb$pct[1] + tb$pct[2], 100, tolerance = 1e-6)
})

test_that("two variables produce a depth AND temperature table", {
  s <- .to_pdf(.tad_cohort(), variable = c("depth", "temp"))$res
  expect_setequal(unique(s$variable), c("depth", "temp"))
  # each variable's pct still sums to 100 per deployment
  chk <- tapply(s$pct, paste(s$id, s$variable), sum)
  expect_true(all(abs(chk - 100) < 1e-6))
})

test_that("grouping by a column facets and tags rows with the group", {
  s <- .to_pdf(.tad_cohort(), group = "species")$res
  expect_setequal(unique(s$group), c("shallow", "deep"))
  expect_equal(s$group[s$id == "A02"][1], "deep")
})

test_that("grouping accepts a named id -> group vector", {
  s <- .to_pdf(.tad_cohort(), group = c(A01 = "g1", A02 = "g2", A03 = "g2"))$res
  expect_setequal(unique(s$group), c("g1", "g2"))
})

test_that("diel = TRUE splits into night/day, each summing to 100 per deployment/phase", {
  s <- .to_pdf(.tad_cohort(), diel = TRUE)$res
  expect_setequal(unique(s$phase), c("night", "day"))
  sums <- tapply(s$pct, paste(s$id, s$phase), sum)
  expect_true(all(abs(sums - 100) < 1e-6))
})

test_that("style = 'heatmap' renders and ignores diel", {
  expect_no_error(.to_pdf(.tad_cohort(), style = "heatmap"))
  expect_message(invisible(utils::capture.output(plotTimeAtDepth(.tad_cohort(), style = "heatmap", diel = TRUE,
                 plot = FALSE, plot.file = tempfile(fileext = ".pdf"), verbose = "normal"))), "ignored", ignore.case = TRUE)
})

test_that("bin.width and explicit breaks are honoured", {
  co <- .tad_cohort()
  s1 <- .to_pdf(co, bin.width = 20)$res
  expect_equal(unique(round(s1$bin_max - s1$bin_min, 6)), 20)
  s2 <- .to_pdf(co, breaks = c(0, 10, 25, 50, 200))$res
  expect_equal(sort(unique(s2$bin_min)), c(0, 10, 25, 50))
})

test_that("a theme object (or a list of overrides) is accepted", {
  expect_no_error(.to_pdf(.tad_cohort(), theme = plotTheme("minimal", day = "#EAF3FB")))
  expect_no_error(.to_pdf(.tad_cohort(), theme = list(preset = "classic")))   # coerced via .as_control
})

test_that("plot = TRUE restores the caller's device and par (no leak)", {
  caller <- tempfile(fileext = ".pdf"); grDevices::pdf(caller)
  on.exit({ grDevices::dev.off(); unlink(caller) }, add = TRUE)
  cur <- grDevices::dev.cur(); before <- graphics::par(no.readonly = TRUE)
  suppressMessages(plotTimeAtDepth(.tad_cohort(), plot = TRUE, verbose = FALSE))
  expect_identical(grDevices::dev.cur(), cur)
  expect_identical(graphics::par(no.readonly = TRUE), before)
})

test_that("argument validation aborts clearly", {
  co <- .tad_cohort()
  expect_error(plotTimeAtDepth(co, plot = FALSE, verbose = FALSE), "Nothing to plot", ignore.case = TRUE)
  expect_error(plotTimeAtDepth(co, variable = "nope", plot = FALSE, plot.file = tempfile(fileext = ".pdf"),
                               verbose = FALSE), "no deployment has usable", ignore.case = TRUE)
  expect_error(plotTimeAtDepth(co, gap.factor = 0.5, plot = FALSE, plot.file = tempfile(fileext = ".pdf"),
                               verbose = FALSE), "gap.factor", ignore.case = TRUE)
  expect_error(plotTimeAtDepth(co, variable = c("a", "b", "c"), plot = FALSE, plot.file = tempfile(fileext = ".pdf"),
                               verbose = FALSE), "one or two", ignore.case = TRUE)
})

test_that("depth defaults to surface-fine non-uniform bins; temperature to uniform", {
  d <- nautilus:::.tadDefaultBreaks("depth", c(0, 500))
  expect_true(all(diff(d) > 0))
  expect_lt(d[2] - d[1], utils::tail(diff(d), 1))              # shallow bins narrower than deep bins
  tt <- nautilus:::.tadDefaultBreaks("temp", c(12, 27))
  expect_length(unique(diff(tt)), 1L)                          # uniform
})

test_that(".binLabels ranges and an open-ended top bin", {
  expect_equal(nautilus:::.binLabels(c(0, 10, 25, 50)), c("0-10", "10-25", ">25"))
})

test_that("the verbose header precedes the loading bar, and the summary names the modal BIN", {
  # The header used to be printed after the load loop, so the progress bar appeared with no context and
  # the title arrived once the work was already done. It can only move if the bullet stops depending on
  # post-load state (it used to interpolate length(loaded)/glevels/grouped).
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf), add = TRUE)
  txt <- paste(cli::cli_fmt(suppressWarnings(
    plotTimeAtDepth(.tad_cohort(), variable = c("depth", "temp"),
                    plot = FALSE, plot.file = pf, verbose = 2))), collapse = "\n")
  expect_match(txt, "plotTimeAtDepth")
  expect_lt(regexpr("plotTimeAtDepth", txt, fixed = TRUE)[1],
            regexpr("SUMMARY", txt, fixed = TRUE)[1])          # header before summary
  expect_match(txt, "Input: 3 deployments")                    # counted pre-load, from src$n

  # the modal bin is reported as a RANGE with units, and as a share of TIME (bins are duration-weighted,
  # so "of observations" would misdescribe it)
  expect_match(txt, "depth: modal bin [-0-9.]+\u2013[-0-9.]+ m \\([0-9]+% of time\\)")
  expect_match(txt, "temperature: modal bin")                  # spelled out, not the "temp" column name
})

test_that("group facets of a variable share one x scale unless same.scale = FALSE", {
  # Faceting exists to compare groups; per-panel autoscaling drew a 23% bar and a 41% bar at the same
  # length, ranking the groups backwards. Depth and temperature still scale independently of each other.
  skip_if_not_installed("data.table")
  set.seed(4)
  flat  <- .mk_tad("F1", function(n) runif(n, 0, 120), species = "flat")   # time spread thinly over bins
  peaky <- .mk_tad("P1", function(n) rep(3, n), species = "peaky")         # ~all time in one bin
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf), add = TRUE)
  s <- suppressMessages(plotTimeAtDepth(list(F1 = flat, P1 = peaky), group = "species",
                                        plot = FALSE, plot.file = pf, verbose = FALSE))
  peak_by_group <- tapply(s$pct, s$group, max)
  expect_gt(max(peak_by_group) / min(peak_by_group), 2)        # the groups genuinely differ in scale
  expect_true(file.size(pf) > 0)
  # both settings must render; same.scale is validated as a flag
  expect_silent(suppressMessages(plotTimeAtDepth(list(F1 = flat, P1 = peaky), group = "species",
                                                 same.scale = FALSE, plot = FALSE, plot.file = pf, verbose = FALSE)))
  expect_error(suppressMessages(plotTimeAtDepth(list(F1 = flat), same.scale = "yes",
                                                plot = FALSE, plot.file = pf, verbose = FALSE)))
})

test_that("axis labelling: every bin keeps its label, and the x label names the aggregation unit", {
  # .binLabels used to be thinned to every other entry above 16 bins, which also deleted the open-top
  # ">X" label whenever nb was even, leaving the deepest stratum anonymous.
  br <- c(0, 2, 4, 6, 8, 10, 15, 20, 30, 50, 75, 100, 150, 200, 300, 400, 600, 800, 1000)
  lab <- nautilus:::.binLabels(br)
  expect_length(lab, length(br) - 1L)
  expect_match(lab[length(lab)], "^>")                          # the top bin is the open-ended one

  # species-agnostic and unconditional: the percentages are per-deployment in BOTH diel and pooled modes
  expect_equal(nautilus:::.tadXlab("depth"), "Time at depth (% per deployment)")
  expect_equal(nautilus:::.tadXlab("temp"), "Time at temperature (% per deployment)")
  expect_false(any(grepl("shark", c(nautilus:::.tadXlab("depth"), nautilus:::.tadXlab("temp")))))
})

test_that("input problems fail by name instead of surfacing as raw R errors", {
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf), add = TRUE)
  one <- function(id = "A", cols = "depth", n = 200) {
    d <- data.table::data.table(ID = id, datetime = as.POSIXct("2023-01-01", tz = "UTC") + seq_len(n) * 60)
    if ("depth" %in% cols) d$depth <- abs(stats::rnorm(n, 10, 4))
    if ("temp"  %in% cols) d$temp  <- 20 + stats::rnorm(n)
    d
  }

  # a requested variable that NO deployment carries used to reach the renderer and die on
  # "invalid 'times' argument" -- after the whole load loop had run
  expect_warning(
    s <- suppressMessages(plotTimeAtDepth(list(A = one("A"), B = one("B")), variable = c("depth", "temp"),
                                          plot = FALSE, plot.file = pf, verbose = FALSE)),
    "Absent from every deployment")
  expect_equal(unique(s$variable), "depth")                    # the usable variable is still plotted
  # when NO variable is usable anywhere the pre-existing load guard fires first, naming the columns
  expect_error(suppressMessages(plotTimeAtDepth(list(A = one("A")), variable = "temp",
                                                plot = FALSE, plot.file = pf, verbose = FALSE)),
               "No deployment has usable")

  # degenerate `breaks` used to surface as "arguments imply differing number of rows" from a data.frame
  for (b in list(c(10, 10), 10, c(1, NA), c(0, Inf))) {
    expect_error(suppressMessages(plotTimeAtDepth(list(A = one("A")), breaks = b,
                                                  plot = FALSE, plot.file = pf, verbose = FALSE)),
                 "breaks")
  }
  expect_error(suppressMessages(plotTimeAtDepth(list(A = one("A")), breaks = c("a", "b"),
                                                plot = FALSE, plot.file = pf, verbose = FALSE)),
               "must be numeric")

  # duplicate ids used to overwrite each other silently: 3 inputs returned 2 deployments
  d1 <- one("DUP"); d2 <- one("DUP"); d3 <- one("Z")
  expect_warning(
    s2 <- suppressMessages(plotTimeAtDepth(list(d1, d2, d3), plot = FALSE, plot.file = pf, verbose = FALSE)),
    "appeared more than once")
  expect_setequal(unique(s2$id), c("DUP", "DUP_2", "Z"))       # none dropped

  # order.by = "median" used to abort with an opaque vapply length error when a deployment lacked the
  # ordering variable; such a deployment now simply sorts last
  co <- list(A = one("A", c("depth", "temp")), B = one("B", "temp"))
  expect_no_error(suppressWarnings(suppressMessages(
    plotTimeAtDepth(co, variable = c("depth", "temp"), order.by = "median",
                    group = c(A = "g1", B = "g2"), plot = FALSE, plot.file = pf, verbose = FALSE))))
  expect_no_error(suppressWarnings(suppressMessages(
    plotTimeAtDepth(co, variable = c("depth", "temp"), order.by = "median", style = "heatmap",
                    plot = FALSE, plot.file = pf, verbose = FALSE))))
})
