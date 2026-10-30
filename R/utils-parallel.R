# Deployment execution shared by processTagData() and calculateTailBeats().
# Jobs own one deployment; terminal output, cohort reductions and graphics belong
# to the calling R process. Sequential jobs use the immediate legacy reporters.

.deploymentExecution <- function(n.cores, n, threads = NULL) {
  if (!is.null(threads)) .assert_count(threads, "data.table.threads", min = 1)
  workers <- min(n.cores, n)
  if (workers <= 1L) return(list(workers = 1L, threads = threads))
  if (isTRUE(getOption("nautilus.parallel.worker")))
    .abort("Nested nautilus worker pools are not supported; use {.code n.cores = 1} inside a worker.")
  for (pkg in c("foreach", "doSNOW", "parallelly")) {
    if (!requireNamespace(pkg, quietly = TRUE))
      .abort("Parallel deployment processing requires {.pkg {pkg}}; install it or use {.code n.cores = 1}.")
  }
  available <- as.integer(parallelly::availableCores())
  threads <- threads %||% 1L
  if (as.double(workers) * threads > available)
    .abort(c("The requested worker/thread budget exceeds the available CPU allocation ({available}).",
             "i" = "{workers} workers x {threads} data.table threads = {workers * threads}; reduce {.arg n.cores} or {.arg data.table.threads}."))
  list(workers = as.integer(workers), threads = as.integer(threads))
}

.checkDeploymentOutputs <- function(data, filepaths, id.col, output.dir, suffix, keep.empty = FALSE) {
  if (is.null(output.dir)) return(invisible(NULL))
  # RDS has no independently readable identity header. Validate destinations once
  # before workers write, so two deployments can never race on the same file.
  ids <- vapply(seq_along(data), function(i) {
    d <- if (filepaths) readRDS(data[[i]]) else data[[i]]
    if (is.null(d) || (!keep.empty && !NROW(d)) || !id.col %in% names(d)) return(NA_character_)
    id <- unique(d[[id.col]])[1L]
    if (is.na(id)) "NA" else as.character(id)
  }, character(1))
  paths <- file.path(output.dir, paste0(ids, suffix %||% "", ".rds"))
  if (anyDuplicated(paths[!is.na(ids)]))
    .abort("Parallel processing requires unique output paths; deployment IDs and {.arg output.suffix} produce duplicate file names.")
  invisible(NULL)
}

.deploymentJob <- function(data, i) list(index = i, value = data[[i]], name = names(data)[i])

.mergeDeploymentResult <- function(result, envir, append = character(), sum = character(),
                                   indexed = character()) {
  i <- result$index
  for (nm in c("data_list", "saved")) {
    x <- get(nm, envir, inherits = FALSE)
    x[i] <- list(if (nm == "data_list") result$value else result$saved)
    assign(nm, x, envir)
  }
  for (nm in append)
    assign(nm, c(get(nm, envir, inherits = FALSE), result$state[[nm]]), envir)
  for (nm in sum)
    assign(nm, get(nm, envir, inherits = FALSE) + result$state[[nm]], envir)
  for (nm in indexed) {
    x <- get(nm, envir, inherits = FALSE)
    x[i] <- if (nm == "ids") result$id else result$state[[nm]]
    assign(nm, x, envir)
  }
  invisible(NULL)
}

# Only workers install a collector. A normal call immediately takes the existing
# cli path, with no event list, buffering, progress display or stdout capture.
.captureDeploymentLog <- function(helper, args, lvl = 1L, min.level = 1L) {
  collector <- getOption("nautilus.report.collector")
  if (is.null(collector)) return(FALSE)
  if (lvl < min.level) return(TRUE)
  collector(list(kind = "log", helper = helper, args = args))
  TRUE
}

.packDeploymentCondition <- function(cnd) {
  list(message = conditionMessage(cnd), call = conditionCall(cnd), classes = class(cnd))
}

.replayDeploymentEvents <- function(events) {
  for (event in events) {
    if (event$kind == "log") {
      do.call(get(event$helper, envir = asNamespace("nautilus")), event$args)
    } else if (event$kind == "stdout") {
      cat(paste0(event$text, collapse = "\n"), "\n", sep = "")
    } else {
      cnd <- structure(list(message = event$condition$message, call = event$condition$call),
                       class = event$condition$classes)
      if (event$kind == "warning") warning(cnd) else message(cnd)
    }
  }
  invisible(NULL)
}

.runDeploymentWorker <- function(job, config, kernel, report.options, scratch) {
  events <- list()
  add <- function(event) events[[length(events) + 1L]] <<- event
  old <- options(c(report.options, list(nautilus.report.collector = add,
                                        nautilus.parallel.worker = TRUE)))
  on.exit(options(old), add = TRUE)
  stdout <- character()
  value <- NULL; failure <- NULL
  stdout <- capture.output({
    value <- tryCatch(withCallingHandlers(
      get(kernel, envir = asNamespace("nautilus"))(job, config,
                 collect.plots = !is.null(config$draw_devices) && length(config$draw_devices) > 0L),
      warning = function(w) {
        if (getOption("warn", 0L) >= 2L)
          stop(paste0("(converted from warning) ", conditionMessage(w)), call. = FALSE)
        add(list(kind = "warning", condition = .packDeploymentCondition(w)))
        invokeRestart("muffleWarning")
      },
      message = function(m) {
        add(list(kind = "message", condition = .packDeploymentCondition(m)))
        invokeRestart("muffleMessage")
      }), error = function(e) { failure <<- .packDeploymentCondition(e); NULL })
  })
  if (length(stdout)) add(list(kind = "stdout", text = stdout))
  # Keep potentially large plot arrays off the socket/result queue. At most two
  # worker-widths of deployments are in flight; the parent removes each spool.
  if (is.null(failure) && length(value$plots)) {
    failure <- tryCatch({
      path <- file.path(scratch, paste0("plots-", job$index, ".rds"))
      saveRDS(value$plots, path, compress = FALSE)
      value$plots <- path
      NULL
    }, error = .packDeploymentCondition)
  }
  list(ok = is.null(failure), result = value, error = failure, events = events, index = job$index)
}

.startDeploymentCluster <- function(workers, threads, libpaths, package.path, report.options, scratch) {
  # Environment limits apply only while spawning these R sessions. Leave any
  # user/scheduler OMP_THREAD_LIMIT untouched; data.table is set explicitly too.
  env <- c(OMP_NUM_THREADS = as.character(threads), OPENBLAS_NUM_THREADS = "1",
           MKL_NUM_THREADS = "1", VECLIB_MAXIMUM_THREADS = "1")
  old <- Sys.getenv(names(env), unset = NA_character_)
  on.exit({
    for (nm in names(old)) {
      if (is.na(old[[nm]])) Sys.unsetenv(nm) else do.call(Sys.setenv, stats::setNames(list(old[[nm]]), nm))
    }
  }, add = TRUE)
  do.call(Sys.setenv, as.list(env))
  cl <- parallel::makePSOCKcluster(workers,
      outfile = if (.Platform$OS.type == "windows") "nul:" else "/dev/null")
  ready <- FALSE
  on.exit(if (!ready) try(parallel::stopCluster(cl), silent = TRUE), add = TRUE)
  init <- function(libpaths, package.path, threads, report.options) {
    .libPaths(libpaths)
    if (!file.exists(file.path(package.path, "Meta", "package.rds"))) {
      if (!requireNamespace("pkgload", quietly = TRUE)) stop("Source-package workers require pkgload.")
      suppressPackageStartupMessages(pkgload::load_all(package.path, quiet = TRUE))
    } else suppressPackageStartupMessages(library("nautilus", character.only = TRUE))
    options(report.options)
    data.table::setDTthreads(threads)
    NULL
  }
  environment(init) <- baseenv() # never serialize the caller's datasets/closures
  parallel::clusterCall(cl, init, libpaths, package.path, threads, report.options)
  ready <- TRUE
  cl
}

.dispatchDeployments <- function(data, config, kernel, accept, execution, draw = NULL) {
  compute <- get(kernel, envir = asNamespace("nautilus"))
  if (execution$workers == 1L) {
    for (i in seq_along(data)) accept(compute(.deploymentJob(data, i), config))
    return(invisible(NULL))
  }
  scratch <- tempfile("nautilus-parallel-")
  dir.create(scratch)
  cl <- NULL
  on.exit({
    if (!is.null(cl)) try(parallel::stopCluster(cl), silent = TRUE)
    unlink(scratch, recursive = TRUE)
  }, add = TRUE)
  opts <- options()
  opts <- opts[grepl("^cli\\.|^width$|^warn$|^warning.length$", names(opts))]
  opts$cli.width <- cli::console_width()
  opts$cli.num_colors <- cli::num_ansi_colors()
  if (is.null(opts$cli.unicode)) opts$cli.unicode <- cli::is_utf8_output()
  opts$datatable.showProgress <- FALSE
  invisible(gc(verbose = FALSE)) # release file-validation buffers before starting workers
  cl <- .startDeploymentCluster(execution$workers, execution$threads, .libPaths(),
                                getNamespaceInfo(asNamespace("nautilus"), "path"), opts, scratch)
  # foreach exposes registration setters but no public registration snapshot.
  # Save these three bindings so a pre-existing user backend (including none)
  # is restored exactly, instead of leaving a stopped cluster registered.
  backend <- get(".foreachGlobals", envir = asNamespace("foreach"))
  keys <- c("fun", "data", "info")
  existed <- vapply(keys, exists, logical(1), envir = backend, inherits = FALSE)
  previous <- mget(keys[existed], envir = backend, inherits = FALSE)
  on.exit({
    for (nm in keys) if (exists(nm, envir = backend, inherits = FALSE)) rm(list = nm, envir = backend)
    list2env(previous, envir = backend)
  }, add = TRUE)
  doSNOW::registerDoSNOW(cl)
  `%dopar%` <- foreach::`%dopar%`
  pb <- if (cli::is_dynamic_tty())
    .log_progress_start(config$lvl, length(data), "Deployments", min.level = 1L) else NULL
  on.exit(.log_progress_done(pb), add = TRUE)
  finished <- 0L
  progress <- function(n, tag) {
    finished <<- finished + 1L
    if (!is.null(pb)) cli::cli_progress_update(id = pb, set = finished)
  }
  report.options <- opts
  # Bound pending full results/plot files without giving up dynamic scheduling
  # inside each window. Contiguous windows also bound input-order report queues.
  windows <- split(seq_along(data), ceiling(seq_along(data) / (2L * execution$workers)))
  for (indices in windows) {
    jobs <- lapply(indices, function(i) .deploymentJob(data, i))
    # File size is a cheap scheduling proxy when inputs are paths; avoid loading
    # whole deployments just to prioritize them. Reporting retains input order.
    cost <- vapply(jobs, function(job) {
      if (config$is_filepaths) as.numeric(file.info(job$value)$size) else as.numeric(NROW(job$value))
    }, numeric(1))
    jobs <- jobs[order(cost, decreasing = TRUE, na.last = TRUE)]
    pending <- vector("list", length(data)); next.index <- indices[1L]
    plot.files <- character(); failure <- NULL
    combine <- function(count, envelope) {
      if (inherits(envelope, "error")) {
        failure <<- envelope
        return(count + 1L)
      }
      pending[envelope$index] <<- list(envelope)
      while (next.index <= max(indices) && !is.null(pending[[next.index]])) {
        item <- pending[[next.index]]
        pending[next.index] <<- list(NULL)
        if (is.null(failure)) tryCatch({
          .replayDeploymentEvents(item$events)
          if (!item$ok) .abort("Deployment {item$index} failed: {item$error$message}")
          accept(item$result)
          if (length(item$result$plots)) plot.files <<- c(plot.files, item$result$plots)
        }, error = function(e) failure <<- e)
        next.index <<- next.index + 1L
      }
      count + 1L
    }
    job <- NULL # foreach iteration binding
    foreach::foreach(job = jobs, .combine = combine, .init = 0L, .inorder = FALSE,
                     .options.snow = list(progress = progress, preschedule = FALSE),
                     .packages = "nautilus", .errorhandling = "pass") %dopar% {
      get(".runDeploymentWorker", envir = asNamespace("nautilus"))(job, config, kernel, report.options, scratch)
    }
    if (!is.null(failure)) stop(failure)
    # Workers are idle here. All graphics calls run on the existing parent
    # devices, in deployment/method/device order, using the legacy draw helpers.
    for (path in plot.files) {
      panels <- readRDS(path)
      if (!is.null(draw)) draw(panels)
      unlink(path)
    }
  }
  invisible(NULL)
}
