# Tests for plotDistributions(): the returned per-deployment distribution summary (the data product),
# metric auto-detection, ordering, argument validation, and device handling (rendered to a temp PDF so
# no interactive device is needed; par must not leak).

.mk_dist <- function(id, tbf_med = 0.3, spd_med = NA, n = 500, seed = 1) {
  set.seed(seed)
  t0 <- as.POSIXct("2021-01-01", tz = "UTC")
  d <- data.table::data.table(ID = id, datetime = t0 + seq_len(n),
                              tbf_hz = rlnorm(n, log(tbf_med), 0.4), vedba = rlnorm(n, log(0.1), 0.5))
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
  out <- .to_pdf(.cohort(), metrics = c("tbf_hz", "paddle_speed"))
  s <- out$res
  expect_s3_class(s, "data.frame")
  expect_named(s, c("id", "metric", "n", "mean", "median", "sd", "q05", "q25", "q75", "q95", "min", "max"))
  expect_equal(nrow(s), 3L * 2L)                               # 3 deployments x 2 metrics (rectangular)
  expect_setequal(unique(s$metric), c("tbf_hz", "paddle_speed"))
  expect_true(out$size > 0)                                    # a PDF was written
})

test_that("a deployment missing a metric appears as an n = 0 row with NA statistics", {
  s <- .to_pdf(.cohort(), metrics = c("tbf_hz", "paddle_speed"))$res
  gap <- s[s$id == "A02" & s$metric == "paddle_speed", ]
  expect_equal(gap$n, 0L)
  expect_true(is.na(gap$median) && is.na(gap$mean))
  # a present metric is summarised correctly
  present <- s[s$id == "A02" & s$metric == "tbf_hz", ]
  expect_gt(present$n, 0L); expect_true(is.finite(present$median))
})

test_that("the summary statistics match a direct computation", {
  co <- .cohort()
  s <- .to_pdf(co, metrics = "tbf_hz")$res
  x <- co$A03$tbf_hz
  row <- s[s$id == "A03", ]
  expect_equal(row$median, stats::median(x), tolerance = 1e-8)
  expect_equal(row$mean, mean(x), tolerance = 1e-8)
  expect_equal(row$q95, unname(stats::quantile(x, 0.95)), tolerance = 1e-8)
})

test_that("metrics = NULL auto-detects the kinematic/effort columns present", {
  s <- .to_pdf(.cohort())$res                                  # no `metrics` -> auto
  expect_setequal(unique(s$metric), c("tbf_hz", "paddle_speed", "vedba"))
})

test_that("order.by controls deployment order without changing the summary rows", {
  co <- .cohort()
  s_id  <- .to_pdf(co, metrics = "tbf_hz", order.by = "id")$res
  s_med <- .to_pdf(co, metrics = "tbf_hz", order.by = "median")$res
  expect_setequal(s_id$id, s_med$id)                           # same content
  # .distOrder: median-descending puts the highest-median deployment first
  vals <- list(A01 = 1:10, A02 = 101:110, A03 = 51:60)
  expect_equal(nautilus:::.distOrder(names(vals), "median", vals), c("A02", "A03", "A01"))
  expect_equal(nautilus:::.distOrder(c("A03", "A01", "A02"), "id", vals), c("A01", "A02", "A03"))
  expect_equal(nautilus:::.distOrder(c("A03", "A01"), "input", vals), c("A03", "A01"))
})

test_that("marginal / reference toggles and a single deployment render without error", {
  co <- .cohort()
  expect_no_error(.to_pdf(co, metrics = "vedba", show.marginal = FALSE, reference = NULL))
  expect_no_error(.to_pdf(co, metrics = "tbf_hz", reference = c("median", "mean")))
  expect_no_error(.to_pdf(co[1], metrics = "tbf_hz"))          # single deployment (no 'margins too large')
})

test_that("plot.file writes a PDF and leaves no device open", {
  if (!is.null(grDevices::dev.list())) grDevices::graphics.off()
  out <- .to_pdf(.cohort(), metrics = c("tbf_hz", "vedba"))
  expect_true(out$size > 0)
  expect_null(grDevices::dev.list())                           # the file device was closed on exit
})

test_that("plot = TRUE restores the caller's device and par (no leak)", {
  caller <- tempfile(fileext = ".pdf"); grDevices::pdf(caller)  # a stand-in 'caller' device (headless-safe)
  on.exit({ grDevices::dev.off(); unlink(caller) }, add = TRUE)
  cur <- grDevices::dev.cur(); before <- graphics::par(no.readonly = TRUE)
  suppressMessages(plotDistributions(.cohort(), metrics = "tbf_hz", plot = TRUE, verbose = FALSE))
  expect_identical(grDevices::dev.cur(), cur)                  # caller's device restored as current
  expect_identical(graphics::par(no.readonly = TRUE), before)  # caller's par restored
})

test_that("argument validation aborts clearly", {
  co <- .cohort()
  expect_error(plotDistributions(co, metrics = "tbf_hz", trim = 1.5, plot = FALSE, plot.file = tempfile(fileext = ".pdf")),
               "trim", ignore.case = TRUE)
  expect_error(plotDistributions(co, metrics = "tbf_hz", plot = FALSE, verbose = FALSE), "Nothing to plot", ignore.case = TRUE)
  expect_error(plotDistributions(co, metrics = "nope_col", order.metric = "other", plot = FALSE,
                                 plot.file = tempfile(fileext = ".pdf")), "order.metric", ignore.case = TRUE)
})

test_that(".distSummaryRow and .distXlim handle empty input", {
  r <- nautilus:::.distSummaryRow("X", "m", numeric(0))
  expect_equal(r$n, 0L); expect_true(is.na(r$median))
  expect_equal(nautilus:::.distXlim(numeric(0), 0.99), c(0, 1))
  xl <- nautilus:::.distXlim(c(1:100, 1000), 0.95)             # upper trim clips the outlier
  expect_lt(xl[2], 1000)
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
  expect_error(plotDistributions(.cohort(), metrics = "tbf_hz", theme = plotTheme(cex = 0),
                                 plot = FALSE, plot.file = pf, verbose = FALSE), "cex")
  expect_error(plotDistributions(.cohort(), metrics = "tbf_hz", theme = list(panel = "not-a-colour"),
                                 plot = FALSE, plot.file = pf, verbose = FALSE), "panel")
})

test_that("a list of overrides is coerced like every other control object", {
  pf <- tempfile(fileext = ".pdf"); on.exit(unlink(pf))
  expect_silent(suppressMessages(
    plotDistributions(.cohort(), metrics = "tbf_hz", theme = list(cex = 1.4),
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
  suppressMessages(plotDistributions(.cohort(), metrics = "tbf_hz", theme = plotTheme(panel = "#123456"),
                                     plot = FALSE, plot.file = pf, verbose = FALSE))
  expect_false(is.null(seen))
  # the legacy 1.15 base is folded in, so theme$cex = 1 reproduces the tuned figure
  expect_equal(seen$cex, 1.15)
  expect_equal(seen$theme$panel, "#123456")            # chrome is the caller's, not a literal
  expect_equal(seen$fill, .themePalette(plotTheme()$palette, 1L))   # fills come from the palette

  # ...and theme$cex scales it rather than being ignored
  suppressMessages(plotDistributions(.cohort(), metrics = "tbf_hz", theme = plotTheme(cex = 2),
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
    suppressMessages(plotDistributions(.cohort(), metrics = "tbf_hz",
                                       theme = plotTheme(panel = "#123456"),
                                       plot = FALSE, plot.file = pf, verbose = FALSE)))
  expect_true("#123456" %in% fills)          # the panel really is painted the theme's colour
  expect_false("grey97" %in% fills)          # ...and not the literal it replaced
})
