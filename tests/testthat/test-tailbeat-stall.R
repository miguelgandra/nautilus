.background_reference <- function(LP, band, k, rows) {
  B <- LP[band, , drop = FALSE]
  kk <- min(k, nrow(B) - 1L + (nrow(B) %% 2L))
  if (kk %% 2L == 0L) kk <- kk - 1L
  vapply(seq_len(ncol(B)), function(j)
    as.numeric(stats::runmed(B[, j], kk, endrule = "median")[rows[j]]), numeric(1))
}

test_that("native scale-background medians preserve every endpoint and ridge row", {
  skip_if(!"frollmedian" %in% getNamespaceExports("data.table"))
  set.seed(71)
  for (nb in c(43L, 64L, 93L)) for (k in c(3L, 5L, 11L, 21L)) {
    LP <- matrix(stats::rnorm(nb * 256L), nb)
    rows <- rep(seq_len(nb), length.out = ncol(LP))
    for (surface in list(LP, round(LP), LP + seq_len(nb), LP * 100)) {
      expect_identical(nautilus:::.cwtScaleBackgroundAt(surface, seq_len(nb), k, rows),
                       .background_reference(surface, seq_len(nb), k, rows))
    }
  }
  # Nontrivial band offsets must not shift the selected median row.
  LP <- matrix(stats::rnorm(100L * 256L), 100L)
  band <- 4:96; rows <- rep(1:93, length.out = ncol(LP))
  expect_identical(nautilus:::.cwtScaleBackgroundAt(LP, band, 21L, rows),
                   .background_reference(LP, band, 21L, rows))
})

test_that("native background retains the complete wavelet result and spectrogram", {
  skip_if(!"frollmedian" %in% getNamespaceExports("data.table"))
  fs <- 20; t <- seq_len(6000L) / fs
  x <- sin(2 * pi * 0.06 * t) + 0.04 * cos(2 * pi * 0.8 * t)
  x[1000:1300] <- NA_real_
  actual <- nautilus:::.cwtRidge(x, fs, 0.1, 2.5, spectrogram = TRUE)
  expected <- testthat::with_mocked_bindings(
    nautilus:::.cwtRidge(x, fs, 0.1, 2.5, spectrogram = TRUE),
    .cwtScaleBackgroundAt = .background_reference, .package = "nautilus")
  expect_identical(actual, expected)
})

test_that("interruption terminates computing workers without waiting for their jobs", {
  .p_workers()
  root <- withr::local_tempdir(); marker <- file.path(root, "busy-worker")
  start <- nautilus:::.startDeploymentCluster
  owned <- NULL
  testthat::local_mocked_bindings(.startDeploymentCluster = function(...) {
    owned <<- start(...)
    parallel::clusterCall(owned, function(marker) {
      ns <- asNamespace("nautilus")
      f <- function(job, config, collect.plots = FALSE) {
        if (job$index == 2L) { writeLines("busy", marker); Sys.sleep(60) }
        else {
          for (i in seq_len(500L)) {
            if (file.exists(marker)) break
            Sys.sleep(0.01)
          }
        }
        list(index = job$index, value = NULL, saved = NULL, plots = list())
      }
      environment(f) <- list2env(list(marker = marker), parent = ns)
      unlockBinding(".processTagDeployment", ns)
      assign(".processTagDeployment", f, envir = ns)
      lockBinding(".processTagDeployment", ns)
      NULL
    }, marker)
    owned
  }, .package = "nautilus")
  # Exercise the same interrupt unwinding as Esc, while the second worker is
  # inside a long job. A stopCluster-only cleanup leaves that job running.
  cancelled <- structure(list(message = "cancelled"), class = c("interrupt", "condition"))
  result <- tryCatch(nautilus:::.dispatchDeployments(
    list(A = .p_tag(), B = .p_tag("B")),
    list(lvl = 0L, is_filepaths = FALSE), ".processTagDeployment",
    accept = function(x) signalCondition(cancelled),
    execution = list(workers = 2L, threads = 1L)), interrupt = function(e) e)
  expect_s3_class(result, "interrupt")
  expect_true(file.exists(marker))
  alive <- function() any(parallelly::isNodeAlive(owned))
  for (i in seq_len(100L)) { if (!alive()) break; Sys.sleep(0.05) }
  expect_false(alive())
})

test_that("parallel progress counts completions across scheduling windows", {
  .p_workers()
  updates <- list()
  testthat::local_mocked_bindings(is_dynamic_tty = function(...) TRUE,
    cli_progress_bar = function(...) "test-progress",
    cli_progress_update = function(id, set, status, ...) {
      updates[[length(updates) + 1L]] <<- list(set = set, status = status)
    }, cli_progress_done = function(...) invisible(NULL), .package = "cli")
  tags <- lapply(1:5, function(i) .p_tag(paste0("A", i)))
  suppressWarnings(processTagData(tags, n.cores = 2, verbose = 1))
  expect_identical(vapply(updates, `[[`, integer(1), "set"), 1:5)
  expect_true(any(grepl("running; reports in input order", vapply(updates, `[[`, "", "status"))))
  expect_identical(updates[[5]]$status, "collecting plots/results")
})


test_that("unclassified tail-beat backends never compute an unused Hilbert envelope", {
  t <- seq_len(5003L) / 20  # prime length, matching the failure mode
  x <- data.table::data.table(ID = "A", datetime = as.POSIXct("2020-01-01", tz = "UTC") + t,
                              surge = sin(2 * pi * 0.7 * t))
  testthat::local_mocked_bindings(.envelope = function(...) stop("unused envelope evaluated"),
                                 .package = "nautilus")
  a <- nautilus:::.runPeaks(data.table::copy(x), "A", "datetime", "surge", 20, 0.1, 2.5,
                            TRUE, 0.09, 2.75, 4, NULL, 5, lvl = 0)
  b <- nautilus:::.runCWT(data.table::copy(x), animal_id = "A", id.col = "ID",
    datetime.col = "datetime", motion.col = "surge", min.freq.Hz = 0.1, max.freq.Hz = 2.5,
    bandpass.filter = TRUE, filter.low.freq = 0.09, filter.high.freq = 2.75, filter.order = 4,
    min.amplitude = NULL, smooth.window = 5, max.interp.gap = 10,
    plot.wavelet = FALSE, plot.diagnostic = FALSE, fs = 20, lvl = 0)
  expect_true(all(is.na(a$tbf_swimming)))
  expect_true(all(is.na(b$tbf_swimming)))
  expect_true(any(is.finite(a$tbf_hz_peaks)))
  expect_true(any(is.finite(b$tbf_hz_wavelet)))
  expect_error(nautilus:::.runPeaks(data.table::copy(x), "A", "datetime", "surge", 20, 0.1, 2.5,
                                   TRUE, 0.09, 2.75, 4, 0.2, 5, lvl = 0), "unused envelope evaluated")
})
