test_that("sequential logging matches all pre-refactor transcripts", {
  reference <- readRDS(test_path("fixtures", "parallel-logging-baseline.rds"))$cases
  raw <- .p_tag("LOG{A}", n = 2400L)
  processed <- suppressWarnings(processTagData(list("LOG{A}" = raw), downsample.to = NULL, verbose = 0))
  for (colors in c(1L, 8L)) for (width in c(60L, 100L)) for (unicode in c(FALSE, TRUE)) for (lvl in 0:2) {
    withr::local_options(list(cli.dynamic = FALSE, width = width, cli.width = width,
                              cli.unicode = unicode, cli.num_colors = colors))
    for (stage in c("process", "tailbeats")) {
      key <- sprintf("%s-v%d-width%d-unicode%s-colors%d", stage, lvl, width, unicode, colors)
      got <- if (stage == "process")
        .p_capture(processTagData(list("LOG{A}" = raw, EMPTY = raw[0]), verbose = lvl, n.cores = 1))
      else .p_capture(calculateTailBeats(processed, verbose = lvl, n.cores = 1))
      expect_identical(got$text, reference[[key]]$text, info = key)
      expect_identical(got$stdout, reference[[key]]$stdout, info = key)
      expect_identical(got$warnings, reference[[key]]$warnings, info = key)
    }
  }
})

test_that("worker budgets are validated and serial execution needs no worker backend", {
  for (bad in list(0, -1, 1.5, NA_real_, Inf, c(1, 2), "2", NULL)) {
    expect_error(processTagData(list(A = .p_tag()), n.cores = bad), "n.cores")
    expect_error(calculateTailBeats(list(A = .p_tag()), n.cores = bad), "n.cores")
  }
  testthat::local_mocked_bindings(.startDeploymentCluster = function(...) stop("worker started"), .package = "nautilus")
  expect_no_error(suppressWarnings(processTagData(list(A = .p_tag(), B = .p_tag("B")), n.cores = 1, verbose = 0)))
  .p_workers()
  testthat::local_mocked_bindings(availableCores = function(...) 2L, .package = "parallelly")
  expect_error(nautilus:::.deploymentExecution(3, 3), "budget")
  expect_error(nautilus:::.deploymentExecution(2, 2, 2), "budget")
  expect_equal(nautilus:::.deploymentExecution(2, 3), list(workers = 2L, threads = 1L))
  expect_equal(nautilus:::.deploymentExecution(8, 1)$workers, 1L)
  withr::local_options(nautilus.parallel.worker = TRUE)
  expect_error(nautilus:::.deploymentExecution(2, 2), "Nested")
})

test_that("parallel mixed processing preserves data, exclusions and ordered console output", {
  .p_workers()
  withr::local_options(list(cli.dynamic = FALSE, cli.num_colors = 1L, cli.width = 80L))
  x <- list(A = .p_tag(), EMPTY = .p_tag("EMPTY")[0], PART = .p_tag("PART"), B = .p_tag("B"), C = .p_tag("C"))
  x$PART[, c("ax", "ay", "az") := NULL]
  before <- .p_normalize(x)
  for (lvl in 0:2) {
    a <- .p_capture(processTagData(x, downsample.to = NULL, verbose = lvl))
    b <- .p_capture(processTagData(x, downsample.to = NULL, verbose = lvl, n.cores = 2))
    expect_identical(.p_normalize(a$value), .p_normalize(b$value))
    expect_identical(a$text, b$text)
    expect_identical(a$stdout, b$stdout)
    expect_identical(a$warnings, b$warnings)
  }
  expect_identical(.p_normalize(x), before)
})

test_that("parallel tail-beat backends preserve all columns, QC and reporting", {
  .p_workers()
  withr::local_options(list(cli.dynamic = FALSE, cli.num_colors = 1L, cli.width = 80L))
  raw <- list(A = .p_tag(), B = .p_tag("B"), C = .p_tag("C"))
  x <- suppressWarnings(processTagData(raw, downsample.to = NULL, verbose = 0))
  x$B[, sway := NA_real_]
  before <- .p_normalize(x)
  for (methods in list("peaks", "wavelet", c("peaks", "wavelet"), c("wavelet", "peaks"))) {
    a <- .p_capture(calculateTailBeats(x, method = methods, verbose = 2))
    b <- .p_capture(calculateTailBeats(x, method = methods, verbose = 2, n.cores = 2))
    expect_identical(.p_normalize(a$value), .p_normalize(b$value))
    expect_identical(a$text, b$text)
    expect_identical(a$stdout, b$stdout)
    expect_identical(a$warnings, b$warnings)
  }
  expect_identical(.p_normalize(x), before)
})

test_that("file-backed processing and tail-beats return the same saved objects", {
  .p_workers()
  root <- withr::local_tempdir(); input <- file.path(root, "input"); output <- file.path(root, "output")
  dir.create(input); dir.create(output)
  x <- list(A = .p_tag(), B = .p_tag("B"))
  paths <- vapply(names(x), function(id) {p <- file.path(input, paste0(id, ".rds")); saveRDS(x[[id]], p); p}, "")
  a <- suppressWarnings(processTagData(paths, downsample.to = NULL, output.dir = output, return.data = FALSE, verbose = 0))
  expected <- lapply(a, readRDS)
  visible <- withVisible(suppressWarnings(processTagData(paths, downsample.to = NULL, output.dir = output,
                                    return.data = FALSE, verbose = 0, n.cores = 2)))
  expect_false(visible$visible)
  expect_identical(a, visible$value)
  expect_identical(.p_normalize(expected), .p_normalize(lapply(a, readRDS)))
  for (cores in c(1, 2)) {
    dest <- file.path(root, paste0("tail-", cores)); dir.create(dest)
    r <- withVisible(suppressWarnings(calculateTailBeats(a, output.dir = dest, return.data = FALSE,
                                                        verbose = 0, n.cores = cores)))
    expect_false(r$visible)
    if (cores == 1) expected <- lapply(r$value, readRDS)
    else expect_identical(.p_normalize(expected), .p_normalize(lapply(r$value, readRDS)))
  }
  empty <- expected[[1]][0]
  expect_error(calculateTailBeats(list(A = empty, B = empty), output.dir = output,
                                  n.cores = 2, verbose = 0), "unique output")
  duplicate <- list(A = .p_tag(), AGAIN = .p_tag())
  expect_error(processTagData(duplicate, output.dir = output, n.cores = 2, verbose = 0), "unique output")
})

test_that("worker errors clean up sockets, scratch files, threads and foreach registration", {
  .p_workers()
  backend <- get(".foreachGlobals", asNamespace("foreach"))
  old <- as.list(backend, all.names = TRUE)
  on.exit({rm(list = ls(backend, all.names = TRUE), envir = backend); list2env(old, backend)}, add = TRUE)
  fake <- function(...) NULL; info <- function(data, item) if (item == "name") "existing-user-backend" else NULL
  foreach::setDoPar(fake, list(marker = 42), info)
  saved <- as.list(backend, all.names = TRUE)
  threads <- data.table::getDTthreads()
  env_keys <- c("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS")
  env <- Sys.getenv(env_keys, unset = NA_character_)
  scratch <- list.files(tempdir(), pattern = "^nautilus-parallel-", full.names = TRUE)
  sockets <- function() {
    c <- showConnections(all = TRUE)
    c[grepl("sock", c[, "class"]), c("description", "class", "isopen"), drop = FALSE]
  }
  before_sockets <- sockets()
  x <- list(A = .p_tag(), BAD = as.data.frame(.p_tag("BAD"))); x$BAD$ax <- rep("broken", nrow(x$BAD))
  expect_error(suppressWarnings(processTagData(x, n.cores = 2, verbose = 0)), "Deployment 2 failed")
  expect_identical(as.list(backend, all.names = TRUE), saved)
  expect_identical(sockets(), before_sockets)
  expect_identical(data.table::getDTthreads(), threads)
  expect_identical(Sys.getenv(env_keys, unset = NA_character_), env)
  expect_identical(list.files(tempdir(), pattern = "^nautilus-parallel-", full.names = TRUE), scratch)
  expect_no_error(suppressWarnings(processTagData(list(A = .p_tag(), B = .p_tag("B")), n.cores = 2, verbose = 0)))
  expect_identical(as.list(backend, all.names = TRUE), saved)
})

test_that("worker thread settings and graphics isolation are local to the pool", {
  .p_workers()
  scratch <- withr::local_tempdir()
  parent <- data.table::getDTthreads()
  cl <- nautilus:::.startDeploymentCluster(2L, 1L, .libPaths(),
      getNamespaceInfo(asNamespace("nautilus"), "path"), list(), scratch)
  on.exit(parallel::stopCluster(cl), add = TRUE)
  settings <- parallel::clusterCall(cl, function() c(threads = data.table::getDTthreads(), device = grDevices::dev.cur()))
  expect_true(all(vapply(settings, function(x) x[[1]] == 1L && x[[2]] == 1L, logical(1))))
  expect_identical(data.table::getDTthreads(), parent)
})
