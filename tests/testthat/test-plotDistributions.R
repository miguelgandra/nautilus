# Tests for plotDistributions(): the returned per-deployment distribution summary (the data product),
# metric auto-detection, ordering, argument validation, and device handling (rendered to a temp PDF so
# no interactive device is needed; par must not leak).

.mk_dist <- function(id, tbf_med = 0.3, spd_med = NA, n = 500, seed = 1) {
  set.seed(seed)
  t0 <- as.POSIXct("2021-01-01", tz = "UTC")
  d <- data.table::data.table(ID = id, datetime = t0 + seq_len(n),
                              tbf_hz_peaks = rlnorm(n, log(tbf_med), 0.4), vedba = rlnorm(n, log(0.1), 0.5))
  if (!is.na(spd_med)) d[, paddle_speed := rlnorm(n, log(spd_med), 0.5)]
  d
}
.cohort <- function() list(A01 = .mk_dist("A01", 0.25, 0.7, seed = 1),
                           A02 = .mk_dist("A02", 0.35, NA,  seed = 2),   # no paddle -> a gap
                           A03 = .mk_dist("A03", 0.45, 1.1, seed = 3))
.to_pdf <- function(...) {                                     # render off-screen; return the summary
  pf <- tempfile(fileext = ".pdf")
  res <- suppressMessages(plotDistributions(..., plot = FALSE, plot.file = pf, verbose = FALSE))
  on.exit(unlink(pf), add = TRUE)
  list(res = res, size = file.size(pf))
}

test_that("returns a tidy per-deployment x metric summary with the canonical schema", {
  out <- .to_pdf(.cohort(), metrics = c("tbf_hz_peaks", "paddle_speed"))
  expect_named(out$res, c("deployments", "population"))
  s <- out$res$deployments
  expect_s3_class(s, "data.frame")
  expect_named(s, c("id", "metric", "n", "mean", "median", "sd", "q05", "q25", "q75", "q95", "min", "max"))
  expect_equal(nrow(s), 3L * 2L)                               # 3 deployments x 2 metrics (rectangular)
  expect_setequal(unique(s$metric), c("tbf_hz_peaks", "paddle_speed"))
  p <- out$res$population
  expect_named(p, c("metric", "weighting", "n_deployments", "n", "mean", "median", "sd",
                    "q05", "q25", "q75", "q95", "min", "max"))
  expect_equal(p$metric, c("tbf_hz_peaks", "paddle_speed"))
  expect_equal(p$n_deployments, c(3L, 2L))
  expect_equal(p$n, c(1500L, 1000L))
  expect_true(out$size > 0)                                    # a PDF was written
})

test_that("a deployment missing a metric appears as an n = 0 row with NA statistics", {
  s <- .to_pdf(.cohort(), metrics = c("tbf_hz_peaks", "paddle_speed"))$res$deployments
  gap <- s[s$id == "A02" & s$metric == "paddle_speed", ]
  expect_equal(gap$n, 0L)
  expect_true(is.na(gap$median) && is.na(gap$mean))
  # a present metric is summarised correctly
  present <- s[s$id == "A02" & s$metric == "tbf_hz_peaks", ]
  expect_gt(present$n, 0L); expect_true(is.finite(present$median))
})

test_that("the summary statistics match a direct computation", {
  co <- .cohort()
  s <- .to_pdf(co, metrics = "tbf_hz_peaks")$res$deployments
  x <- co$A03$tbf_hz_peaks
  row <- s[s$id == "A03", ]
  expect_equal(row$median, stats::median(x), tolerance = 1e-8)
  expect_equal(row$mean, mean(x), tolerance = 1e-8)
  expect_equal(row$q95, unname(stats::quantile(x, 0.95)), tolerance = 1e-8)
})

test_that("metrics = NULL auto-detects the kinematic/effort columns present", {
  s <- .to_pdf(.cohort())$res$deployments
  expect_setequal(unique(s$metric), c("tbf_hz_peaks", "paddle_speed", "vedba"))
})

test_that("order.by controls deployment order without changing the summary rows", {
  co <- .cohort()
  s_id  <- .to_pdf(co, metrics = "tbf_hz_peaks", order.by = "id")$res$deployments
  s_med <- .to_pdf(co, metrics = "tbf_hz_peaks", order.by = "median")$res$deployments
  expect_setequal(s_id$id, s_med$id)                           # same content
  # .distOrder: median-descending puts the highest-median deployment first
  vals <- list(A01 = 1:10, A02 = 101:110, A03 = 51:60)
  expect_equal(nautilus:::.distOrder(names(vals), "median", vals), c("A02", "A03", "A01"))
  expect_equal(nautilus:::.distOrder(c("A03", "A01", "A02"), "id", vals), c("A01", "A02", "A03"))
  expect_equal(nautilus:::.distOrder(c("A03", "A01"), "input", vals), c("A03", "A01"))
})

test_that("all views render and return the same summaries", {
  co <- .cohort()
  summaries <- plotDistributions(co, plot = FALSE, verbose = FALSE)
  for (view in c("stacked", "population", "deployments")) {
    out <- .to_pdf(co, view = view, reference = c("median", "mean"))
    expect_gt(out$size, 1000)
    expect_identical(out$res, summaries)
  }
  expect_no_error(.to_pdf(co[1], metrics = "tbf_hz_peaks", reference = NULL))
})

test_that("cohort weights are computed per metric from finite rows", {
  co <- list(A = data.frame(ID = "A", speed = c(0, 2, NA)),
             B = data.frame(ID = "B", speed = c(rep(10, 8), Inf)),
             C = data.frame(ID = "C", speed = NA_real_))
  equal <- plotDistributions(co, plot = FALSE, verbose = FALSE)$population
  rows <- plotDistributions(co, population.weight = "row", plot = FALSE, verbose = FALSE)$population
  expect_equal(equal$weighting, "deployment")
  expect_equal(equal$n_deployments, 2L)
  expect_equal(equal$n, 10L)
  expect_equal(equal$mean, 5.5)
  expect_equal(equal$median, 6)  # midpoint at the 50/50 split
  expect_equal(equal$sd, sqrt(20.75))
  expect_equal(equal$q25, 1)
  expect_equal(rows$weighting, "row")
  expect_equal(rows$mean, 8.2)
  expect_equal(rows$median, 10)

  # A long run of small weights must still land on the exact 50/50 boundary.
  co <- list(A = data.frame(ID = "A", speed = 0),
             B = data.frame(ID = "B", speed = rep(10, 100000)))
  expect_equal(plotDistributions(co, plot = FALSE, verbose = FALSE)$population$median, 5)
})

test_that("cohort curves, references and trim are invariant to deployment replication", {
  co <- list(A = data.frame(ID = "A", speed = c(0, 2)),
             B = data.frame(ID = "B", speed = rep(10, 8)))
  population <- NULL
  violins <- 0L
  draw.population <- nautilus:::.drawPopulationPanel
  draw.violins <- nautilus:::.drawViolinPanel
  testthat::local_mocked_bindings(
    .drawPopulationPanel = function(g, ...) {
      population <<- g
      draw.population(g, ...)
    },
    .drawViolinPanel = function(...) {
      violins <<- violins + 1L
      draw.violins(...)
    },
    .package = "nautilus"
  )
  .to_pdf(co, view = "stacked", trim = 0.45, reference = c("median", "mean"))
  expect_equal(violins, 1L)
  stacked <- population
  expect_equal(stacked$xlim, c(0, 2))
  expect_equal(stacked$refs, c(median = 6, mean = 5.5))

  co$A <- co$A[rep(seq_len(nrow(co$A)), 20), ]
  .to_pdf(co, view = "population", trim = 0.45, reference = c("median", "mean"), min.n = 100)
  expect_equal(violins, 1L)
  expect_equal(population$xlim, stacked$xlim)
  expect_equal(population$refs, stacked$refs)
  expect_equal(population$density$bw, stacked$density$bw)
  expect_equal(population$density$x, stacked$density$x)
  expect_equal(population$density$y, stacked$density$y)
  statistics <- c("mean", "median", "sd", "q05", "q25", "q75", "q95", "min", "max")
  expect_equal(population$summary[statistics], stacked$summary[statistics])

  population <- NULL
  .to_pdf(co, view = "deployments")
  expect_null(population)
  expect_equal(violins, 2L)
})

test_that("singleton and constant metrics render in cohort and deployment panels", {
  co <- list(A = data.frame(ID = "A", speed = 0.5, vedba = 0),
             B = data.frame(ID = "B", speed = NA_real_, vedba = rep(0, 40)))
  out <- .to_pdf(co, min.n = 1)
  expect_equal(out$res$population$sd, c(0, 0))
  expect_equal(out$res$population$n, c(1L, 41L))
  expect_gt(out$size, 1000)
  expect_no_error(.to_pdf(co, view = "population"))
})

test_that("resolved deployment IDs are unique and respect id.col for a combined table", {
  co <- data.frame(ID = "animal", tag = c("A", "B"), speed = c(0, 2))
  s <- plotDistributions(co, id.col = "tag", plot = FALSE, verbose = FALSE)
  expect_equal(s$deployments$id, c("A", "B"))
  expect_equal(s$population$n_deployments, 2L)
  expect_error(plotDistributions(list(A = co[1, ], B = co[2, ]), plot = FALSE, verbose = FALSE),
               "Duplicate deployment ID")
})

test_that("plot.file writes a PDF and leaves no device open", {
  if (!is.null(grDevices::dev.list())) grDevices::graphics.off()
  out <- .to_pdf(.cohort(), metrics = c("tbf_hz_peaks", "vedba"))
  expect_true(out$size > 0)
  expect_null(grDevices::dev.list())                           # the file device was closed on exit
})

test_that("plot = TRUE restores the caller's device and par (no leak)", {
  caller <- tempfile(fileext = ".pdf"); grDevices::pdf(caller)  # a stand-in 'caller' device (headless-safe)
  on.exit({ grDevices::dev.off(); unlink(caller) }, add = TRUE)
  cur <- grDevices::dev.cur(); before <- graphics::par(no.readonly = TRUE)
  suppressMessages(plotDistributions(.cohort(), metrics = "tbf_hz_peaks", plot = TRUE, verbose = FALSE))
  expect_identical(grDevices::dev.cur(), cur)                  # caller's device restored as current
  expect_identical(graphics::par(no.readonly = TRUE), before)  # caller's par restored
})

test_that("argument validation aborts clearly", {
  co <- .cohort()
  expect_error(plotDistributions(co, metrics = "tbf_hz_peaks", trim = 1.5, plot = FALSE, plot.file = tempfile(fileext = ".pdf")),
               "trim", ignore.case = TRUE)
  expect_error(plotDistributions(co, trim = 0, plot = FALSE, verbose = FALSE), "trim")
  expect_error(plotDistributions(co, view = "bogus", plot = FALSE, verbose = FALSE), "stacked")
  expect_error(plotDistributions(co, population.weight = "bogus", plot = FALSE, verbose = FALSE), "deployment")
  expect_error(plotDistributions(co, metrics = "nope_col", order.by = "median", order.metric = "other", plot = FALSE,
                                 plot.file = tempfile(fileext = ".pdf")), "order.metric", ignore.case = TRUE)
})

test_that(".distSummaryRow handles empty input", {
  r <- nautilus:::.distSummaryRow("X", "m", numeric(0))
  expect_equal(r$n, 0L); expect_true(is.na(r$median))
})


#######################################################################################################
# Theme migration #####################################################################################
#
# plotDistributions used to carry its own `colors` and `cex = 1.15`, while the newer plotters take a
# shared `theme`. The family was briefly bilingual; this is the migration.

test_that("theme replaces the old colors/cex arguments entirely", {
  expect_false(any(c("colors", "cex") %in% names(formals(plotDistributions))))
  expect_true("theme" %in% names(formals(plotDistributions)))
})

test_that("a bad theme is rejected by name rather than failing deep inside the drawing code", {
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf))
  expect_error(plotDistributions(.cohort(), metrics = "tbf_hz_peaks", theme = plotTheme(cex = 0),
                                 plot = FALSE, plot.file = pf, verbose = FALSE), "cex")
  expect_error(plotDistributions(.cohort(), metrics = "tbf_hz_peaks", theme = list(panel = "not-a-colour"),
                                 plot = FALSE, plot.file = pf, verbose = FALSE), "panel")
})

test_that("a list of overrides is coerced like every other control object", {
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf))
  expect_silent(suppressMessages(
    plotDistributions(.cohort(), metrics = "tbf_hz_peaks", theme = list(cex = 1.4),
                      plot = FALSE, plot.file = pf, verbose = FALSE)))
  expect_true(file.size(pf) > 1000)
})

test_that("the theme actually reaches the drawing layer, values and all", {
  # NOT a source grep. A grep survives `cex <- theme$cex * 1.15` followed one line later by
  # `cex <- 1.15`, and survives every theme slot being replaced by a literal so long as the text
  # still appears somewhere in the file. Capture what the panel drawer is really handed.
  seen <- NULL
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf))
  cap <- function(vals, ord_ids, g, fill, min.n, mar, cex, first, title, theme) {
    seen <<- list(cex = cex, theme = theme, fill = fill); invisible(NULL)
  }
  testthat::local_mocked_bindings(.drawViolinPanel = cap, .package = "nautilus")
  suppressMessages(plotDistributions(.cohort(), metrics = "tbf_hz_peaks", theme = plotTheme(panel = "#123456"),
                                     plot = FALSE, plot.file = pf, verbose = FALSE))
  expect_false(is.null(seen))
  # the legacy 1.15 base is folded in, so theme$cex = 1 reproduces the tuned figure
  expect_equal(seen$cex, 1.15)
  expect_equal(seen$theme$panel, "#123456")            # chrome is the caller's, not a literal
  expect_equal(seen$fill, .themePalette(plotTheme()$palette, 1L))   # fills come from the palette

  # ...and theme$cex scales it rather than being ignored
  suppressMessages(plotDistributions(.cohort(), metrics = "tbf_hz_peaks", theme = plotTheme(cex = 2),
                                     plot = FALSE, plot.file = pf, verbose = FALSE))
  expect_equal(seen$cex, 2.3)
})

test_that("the panel chrome the theme specifies is what actually gets painted", {
  # Capturing what the DRAWER is handed is not enough: mocking the drawer leaves its internals
  # unexercised, so hardcoding the fill back to grey97 inside it survives. Watch the graphics call.
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf))
  fills <- character(0)
  testthat::with_mocked_bindings(
    rect = function(xleft, ybottom, xright, ytop, col = NA, ...) {
      fills <<- c(fills, as.character(col %||% NA)); invisible(NULL)
    },
    .package = "graphics",
    suppressMessages(plotDistributions(.cohort(), metrics = "tbf_hz_peaks",
                                       theme = plotTheme(panel = "#123456"),
                                       plot = FALSE, plot.file = pf, verbose = FALSE)))
  expect_true("#123456" %in% fills)          # the panel really is painted the theme's colour
  expect_false("grey97" %in% fills)          # ...and not the literal it replaced
})


test_that("an explicit label for the column as it is actually named wins over the backend suffix", {
  # .distAutoMetrics() selects tbf_hz_peaks / tbf_hz_wavelet, and the examples use those names, so a
  # user's labels entry keyed on the real column name must not be discarded by the suffix strip
  expect_identical(nautilus:::.distLabel("tbf_hz_peaks", c(tbf_hz_peaks = "My label")), "My label")
  expect_identical(nautilus:::.distLabel("tbf_amplitude_wavelet", c(tbf_amplitude_wavelet = "Z")), "Z")

  # the convenience path is untouched: a stem key still gets the backend appended
  expect_identical(nautilus:::.distLabel("tbf_hz_peaks", c(tbf_hz = "Stem")), "Stem, peaks")
  # and with no labels at all the built-in label is used, backend and all
  expect_identical(nautilus:::.distLabel("tbf_hz_wavelet"), "Tail-beat frequency (Hz), wavelet")
  # a non-tail-beat metric is unaffected
  expect_identical(nautilus:::.distLabel("vedba", c(vedba = "Effort")), "Effort")
})


# ---- panel geometry -------------------------------------------------------------------------

test_that("every metric panel gets exactly the same width, whatever the metrics", {
  # the first column carries the deployment labels in its own left margin, so with equal layout cells
  # its plot region came out narrower than its neighbours' - a two-metric figure looked lopsided
  mk <- function(id) {
    n <- 200
    d <- data.frame(ID = id,
                    datetime = as.POSIXct("2020-01-01", tz = "UTC") + seq_len(n),
                    tbf_hz_wavelet = rlnorm(n, -0.8, 0.3),
                    paddle_speed   = rlnorm(n, 0, 0.4),
                    vedba          = rlnorm(n, -1, 0.5))
    m <- nautilus:::.newNautilusMeta(); m$id <- id
    nautilus:::new_nautilus_tag(data.table::as.data.table(d), m)
  }
  widths_for <- function(metrics, ids) {
    tags <- lapply(ids, mk); names(tags) <- ids
    orig <- nautilus:::.drawViolinPanel
    pins <- numeric(0)
    testthat::local_mocked_bindings(
      .drawViolinPanel = function(...) { orig(...); pins <<- c(pins, graphics::par("pin")[1]) },
      .package = "nautilus")
    pdf(NULL); on.exit(dev.off(), add = TRUE)
    suppressWarnings(plotDistributions(tags, metrics = metrics, plot = TRUE, verbose = FALSE))
    pins
  }
  ids <- sprintf("PIN_%02d", 1:6)

  w2 <- widths_for(c("tbf_hz_wavelet", "paddle_speed"), ids)
  expect_length(w2, 2L)
  expect_equal(w2[1], w2[2], tolerance = 1e-6)

  w3 <- widths_for(c("vedba", "tbf_hz_wavelet", "paddle_speed"), ids)
  expect_length(w3, 3L)
  expect_equal(w3, rep(w3[1], 3L), tolerance = 1e-6)

  # and it holds when the labels are long, which is when the imbalance was worst
  wl <- widths_for(c("tbf_hz_wavelet", "paddle_speed"),
                   c("PIN_CAM_LONGNAME_01", "PIN_CAM_LONGNAME_02", "PIN_CAM_LONGNAME_03"))
  expect_equal(wl[1], wl[2], tolerance = 1e-6)
})

test_that("the default canvas is wide enough not to look vertically stretched", {
  # one metric over a modest cohort should be wider than it is tall
  d1 <- nautilus:::.distDeviceSize(1, 8, "stacked", 1)
  expect_gt(d1$width, d1$height)
  # and each added metric widens it substantially
  d2 <- nautilus:::.distDeviceSize(2, 8, "stacked", 1)
  expect_gt(d2$width - d1$width, 3)
  expect_identical(d1$height, d2$height)          # height depends on the cohort, not the metric count
})
