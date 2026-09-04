#######################################################################################################
# The shared deployment-exclusion log #################################################################
#######################################################################################################
#
# One CSV, holding CURRENT STATE rather than history: every stage that can drop a deployment writes the
# exclusions it produced, refreshing its earlier rows for the deployments in the current call. A
# deployment that stops being excluded loses its row, while an intentional partial run leaves other
# deployments untouched. An append-only log can express neither property reliably.
#
# The log and the data products go stale together, because running a stage refreshes both. A row left
# by a stage that has not re-run therefore still describes the data that stage last wrote, which is why
# a stage never clears the rows of the stages after it: doing so would strip the explanation from
# deployments that are still missing from the data on disk.


#' The stages that can drop a deployment, in pipeline order.
#'
#' The order is what resolves a deployment carrying rows from more than one stage: it left the pipeline
#' at the earliest one. Chronology cannot do this - re-running an early stage gives it a later timestamp
#' than a downstream stage's older decision, which would attribute the deployment to a stage it never
#' reached.
#'
#' `checkSensorIntegrity()` is deliberately absent: it excludes CHANNELS, not deployments, and records
#' them in `meta$sensors$excluded`. Where a removed channel later causes an exclusion, the reason
#' `processTagData()` writes names the columns that went missing.
#' @keywords internal
#' @noRd
.exclusionStages <- function() c("importTagData", "filterDeploymentData", "regularizeTimeSeries",
                                 "applyAxisMapping", "processTagData")

#' An empty exclusions table carrying the schema.
#' @keywords internal
#' @noRd
.exclusionsEmpty <- function() data.frame(
  id = character(0), stage = character(0), reason = character(0),
  window_start = .POSIXct(numeric(0), tz = "UTC"), window_end = .POSIXct(numeric(0), tz = "UTC"),
  window_hours = numeric(0), stringsAsFactors = FALSE)

#' One exclusion record.
#'
#' The window columns describe a deployment period that was detected and then rejected, which only
#' `filterDeploymentData()` produces; every other stage leaves them missing, and
#' [summarizeTagData()] uses them to fill the record window of a deployment that has no data of its own.
#' @keywords internal
#' @noRd
.exclusionsRow <- function(id, stage, reason, window_start = NULL, window_end = NULL,
                           window_hours = NA_real_) {
  data.frame(id = as.character(id), stage = as.character(stage), reason = as.character(reason),
             window_start = if (inherits(window_start, "POSIXt")) window_start
                            else .POSIXct(NA_real_, tz = "UTC"),
             window_end   = if (inherits(window_end, "POSIXt")) window_end
                            else .POSIXct(NA_real_, tz = "UTC"),
             window_hours = as.numeric(window_hours), stringsAsFactors = FALSE)
}

#' Bind a stage's accumulated records into one table.
#' @keywords internal
#' @noRd
.exclusionsBind <- function(rows) {
  if (!length(rows)) return(.exclusionsEmpty())
  out <- do.call(rbind, rows); rownames(out) <- NULL
  out
}

#' Read the log, from a path or an in-memory table, with the schema enforced.
#'
#' Types are restored explicitly rather than left to `read.csv()`: `summarizeTagData()` assigns
#' `window_start` into a POSIXct column, and a character read back from the file would corrupt the
#' record window silently rather than failing.
#' @keywords internal
#' @noRd
.exclusionsRead <- function(x) {
  if (is.null(x)) return(NULL)
  ex <- if (is.character(x)) {
    if (length(x) != 1L)
      .abort("{.arg exclusions} must be a single file path, or a data frame.")
    if (!file.exists(x))
      .abort("{.arg exclusions} file does not exist: {.file {x}}.")
    utils::read.csv(x, stringsAsFactors = FALSE, colClasses = "character")
  } else x
  if (!is.data.frame(ex))
    .abort("{.arg exclusions} must be a data frame, or the path to the exclusions log.")
  miss <- setdiff(c("id", "reason"), names(ex))
  if (length(miss))
    .abort(c("The exclusions log is missing the column{?s} {.field {miss}}.",
             "i" = "Expected {.field {names(.exclusionsEmpty())}}."))
  if (!nrow(ex)) return(.exclusionsEmpty())

  # `stage` is what the package writes, but the log is a CSV a user may reasonably assemble by hand;
  # without it every row is simply unattributed, which orders last and still resolves.
  if (!"stage" %in% names(ex)) ex$stage <- NA_character_
  for (nm in c("id", "stage", "reason")) ex[[nm]] <- as.character(ex[[nm]])
  for (nm in c("window_start", "window_end")) {
    ex[[nm]] <- if (nm %in% names(ex) && !inherits(ex[[nm]], "POSIXt"))
      as.POSIXct(as.character(ex[[nm]]), format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
    else if (nm %in% names(ex)) ex[[nm]] else .POSIXct(rep(NA_real_, nrow(ex)), tz = "UTC")
  }
  ex$window_hours <- if ("window_hours" %in% names(ex)) suppressWarnings(as.numeric(ex$window_hours))
                     else NA_real_
  ex[, names(.exclusionsEmpty()), drop = FALSE]
}

#' Refresh one stage's in-scope rows in the log and write it back.
#'
#' Read-modify-write, through a temporary file renamed over the target: the rename is atomic, so an
#' interrupted run cannot leave the log truncated. `scope.ids` names every deployment evaluated by this
#' call. Existing rows for that stage and those IDs are removed before the current rows are appended;
#' rows for other deployments and stages survive. `NULL` retains whole-stage replacement for internal
#' maintenance and backwards compatibility with older package-internal callers.
#' @keywords internal
#' @noRd
.exclusionsWrite <- function(rows, file, stage, scope.ids = NULL) {
  if (is.null(file)) return(invisible(NULL))

  if (!is.null(scope.ids)) {
    scope.ids <- unique(as.character(scope.ids))
    scope.ids <- scope.ids[!is.na(scope.ids) & nzchar(scope.ids)]
    outside <- setdiff(unique(as.character(rows$id)), scope.ids)
    if (length(outside)) {
      .abort(c("Cannot write exclusion rows outside {.arg scope.ids}: {.val {outside}}.",
               "i" = "Every exclusion row must belong to a deployment evaluated by this call."))
    }
  }

  keep <- if (file.exists(file)) {
    old <- tryCatch(.exclusionsRead(file), error = function(e)
      .abort(c("Could not read the existing exclusions log at {.file {file}}.",
               "i" = conditionMessage(e))))
    replace <- old$stage %in% stage
    if (!is.null(scope.ids)) replace <- replace & old$id %in% scope.ids
    old[!replace, , drop = FALSE]
  } else .exclusionsEmpty()

  out <- rbind(keep, rows)
  ord <- order(match(out$stage, .exclusionStages()), out$id)     # pipeline order, then id: stable to read
  out <- out[ord, , drop = FALSE]
  for (nm in c("window_start", "window_end"))
    out[[nm]] <- format(out[[nm]], "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  out$window_hours <- round(out$window_hours, 4)      # a log is read by people; 0.36 s is enough

  tmp <- tempfile(pattern = "nautilus-exclusions-", tmpdir = dirname(file), fileext = ".csv")
  on.exit(if (file.exists(tmp)) unlink(tmp), add = TRUE)
  utils::write.csv(out, tmp, row.names = FALSE, na = "")
  if (!file.rename(tmp, file)) {                                 # rename can fail across devices
    ok <- file.copy(tmp, file, overwrite = TRUE)
    if (!ok) .abort("Could not write the exclusions log to {.file {file}}.")
  }
  invisible(file)
}

#' One row per deployment: where it left the pipeline.
#'
#' A deployment carrying rows from several stages left at the earliest of them - the later rows are
#' decisions taken in an older run, before an upstream stage started excluding it.
#' @keywords internal
#' @noRd
.exclusionsResolve <- function(ex) {
  if (is.null(ex) || !nrow(ex)) return(ex)
  ord <- order(match(ex$stage, .exclusionStages()), na.last = TRUE)
  ex <- ex[ord, , drop = FALSE]
  ex[!duplicated(ex$id), , drop = FALSE]
}
