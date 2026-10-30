.p_imu_tag <- function(id, n = 2400L) {
  d <- .p_tag(id, n)
  t <- (seq_len(n) - 1) / 20
  d[, `:=`(gx = 0.01 * sin(t), gy = 0.01 * cos(t), gz = 0.01 * sin(0.3 * t),
            mx = 30 + 5 * cos(0.2 * t), my = 10 + 3 * sin(0.2 * t), mz = 40 + 4 * cos(0.3 * t))]
  m <- nautilus:::.getMeta(d); m$deployment$lon <- -25; m$deployment$lat <- 11
  m$deployment$datetime <- as.POSIXct("2020-01-01", tz = "UTC")
  m$tag$model <- "IMU {literal name}"
  nautilus:::.restoreMeta(d, m)
}

test_that("parallel reporters retain caller formatting and every long message", {
  .p_workers()
  x <- list(A = .p_imu_tag("A"), B = .p_tag("B"))
  for (unicode in c(FALSE, TRUE)) {
    withr::local_options(list(cli.dynamic = FALSE, cli.unicode = unicode, cli.num_colors = 8L,
                              width = 60L, cli.width = 60L))
    a <- .p_capture(processTagData(x, verbose = 2))
    b <- .p_capture(processTagData(x, verbose = 2, n.cores = 2))
    expect_identical(a$text, b$text)
    expect_identical(a$warnings, b$warnings)
  }
  events <- list()
  collect <- function(event) events[[length(events) + 1L]] <<- event
  strings <- sprintf("message %03d: {literal braces} %s END", 1:100, strrep("full detail ", 200L))
  invisible_output <- withr::with_options(list(nautilus.report.collector = collect),
    cli::cli_fmt({
      nautilus:::.log_detail(0L, stop("quiet arguments must remain lazy"))
      for (s in strings) nautilus:::.log_detail(2L, s)
    }))
  expect_length(invisible_output, 0L)
  expect_length(events, 100L)
  expected <- cli::cli_fmt(for (s in strings) nautilus:::.log_detail(2L, s))
  actual <- cli::cli_fmt(nautilus:::.replayDeploymentEvents(events))
  expect_identical(actual, expected)
})

test_that("tail-beat plot payloads and method order are identical on parent devices", {
  .p_workers()
  root <- withr::local_tempdir()
  grDevices::pdf(file.path(root, "active.pdf"))
  device <- grDevices::dev.cur()
  on.exit(grDevices::dev.off(device), add = TRUE)
  parent_pid <- Sys.getpid(); seen <- list()
  record <- function(kind, pd) {
    seen[[length(seen) + 1L]] <<- list(kind = kind, data = pd,
                                      pid = Sys.getpid(), device = grDevices::dev.cur())
  }
  testthat::local_mocked_bindings(
    .drawTailBeatsPeaks = function(pd) record("peaks", pd),
    .drawTailBeatsCWT = function(pd) record("wavelet", pd), .package = "nautilus")
  raw <- list(LONG = .p_tag("LONG", 20000L), B = .p_tag("B"), C = .p_tag("C"),
              D = .p_tag("D"), LAST = .p_tag("LAST"))
  input <- suppressWarnings(processTagData(raw, downsample.to = NULL, verbose = 0))
  for (methods in list("peaks", "wavelet", c("peaks", "wavelet"), c("wavelet", "peaks"))) {
    seen <- list()
    suppressWarnings(calculateTailBeats(input, method = methods, plot = TRUE, verbose = 0))
    expected <- seen; seen <- list()
    suppressWarnings(calculateTailBeats(input, method = methods, plot = TRUE, verbose = 0, n.cores = 2))
    expect_identical(seen, expected)
    expect_true(all(vapply(seen, function(x) x$pid == parent_pid && x$device == device, logical(1))))
    expect_identical(vapply(seen, `[[`, "", "kind"), rep(methods, length(input)))
  }
})

test_that("diagnostic PDF pages are pixel-identical in sequential and parallel execution", {
  .p_workers()
  poppler <- Sys.which("pdftoppm")
  skip_if(!nzchar(poppler), "Poppler is needed for pixel comparison")
  root <- withr::local_tempdir()
  raw <- list(A = .p_imu_tag("A"), B = .p_imu_tag("B"))
  processed <- suppressWarnings(processTagData(raw, downsample.to = NULL, verbose = 0))
  raster <- function(path, prefix) {
    status <- system2(poppler, c("-r", "72", "-png", shQuote(path), shQuote(prefix)),
                      stdout = FALSE, stderr = FALSE)
    expect_equal(status, 0L)
    pages <- list.files(root, pattern = paste0("^", basename(prefix), "-[0-9]+\\.png$"), full.names = TRUE)
    expect_gt(length(pages), 0L)
    unname(tools::md5sum(pages))
  }
  for (stage in c("process", "tailbeats")) {
    hashes <- list()
    for (cores in c(1, 2)) {
      path <- file.path(root, paste0(stage, cores, ".pdf"))
      if (stage == "process") suppressWarnings(processTagData(raw, plot.file = path, verbose = 0, n.cores = cores))
      else suppressWarnings(calculateTailBeats(processed, method = c("wavelet", "peaks"),
                                               plot.file = path, verbose = 0, n.cores = cores))
      hashes[[as.character(cores)]] <- raster(path, file.path(root, paste0(stage, cores)))
    }
    expect_identical(hashes[[1]], hashes[[2]], info = stage)
  }
})
