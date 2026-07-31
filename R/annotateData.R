#######################################################################################################
# Function to annotate dataset ########################################################################
#######################################################################################################

#' Annotate tag data with scored events or behaviours
#'
#' @description
#' Marks each sample of a deployment according to whether it falls inside a scored event window.
#' Events are normally read off the tag's own camera footage - a feeding bout, a social interaction, a
#' cleaning station visit - and supplied as a table of start/end times per individual. Every sample
#' inside a window is marked `1`, every sample outside it `0`, one column per event type.
#'
#' The resulting columns are what turns a sensor record into a labelled training set:
#' [extractFeatures()] builds the predictors, and these columns supply the response.
#'
#' @details
#' **Every individual receives every event column.** The column set is fixed once, from the events
#' present in `annotations` (after `selected.events` filtering), so a deployment that was never scored
#' for a given behaviour gets that column filled with `0` rather than having it missing. This is what
#' makes the returned tables safe to row-bind and pool; it also means a `0` should be read as "not
#' scored as this event", which is not always the same as "this event did not occur".
#'
#' **What this function does not do.** It does not check that an individual was observed at all: an
#' animal absent from `annotations` is returned with all-zero event columns, and is named in the
#' console output so the difference is visible. It does not interpolate, resample, or alter any
#' existing column. It does not infer event windows - the scoring is entirely yours.
#'
#' Windows are compared inclusively at both ends (`start <= t <= end`), and overlapping windows for the
#' same event are unioned. Annotation rows with a missing `start` or `end` are skipped and reported.
#'
#' @param data A `nautilus_tag`, a (named) list of them, or a plain data.frame/data.table - either one
#'   table per individual, or a single aggregated table carrying `id.col`, which is split internally.
#'   `NULL` list elements (e.g. a deployment dropped by an upstream quality-control step) are accepted,
#'   skipped, and omitted from the result.
#' @param annotations A data.frame of scored event windows, with an individual ID column (`id.col`), an
#'   event-type column (`event.col`), and POSIXct start and end columns (`start.col`, `end.col`).
#' @param id.col Name of the column holding animal IDs, in both `data` and `annotations`.
#' @param datetime.col Name of the POSIXct timestamp column in `data`.
#' @param event.col Name of the column in `annotations` holding the event or behaviour type. Its values
#'   become the names of the new columns, so they must not clash with columns already in `data`.
#' @param start.col Name of the POSIXct column in `annotations` holding each event's start time.
#' @param end.col Name of the POSIXct column in `annotations` holding each event's end time.
#' @param selected.events Optional character vector of event types to annotate. `NULL` (default) uses
#'   every event present in `annotations`.
#' @param verbose Console detail: `"quiet"`, `"normal"` or `"detailed"` (default).
#'
#' @return A named list of tables, one per individual, each carrying one `0`/`1` column per event type.
#'   The class and metadata of the input are preserved, and the call is recorded in the processing
#'   history (see [processingHistory()]). Individuals whose input element was `NULL` are
#'   omitted, so the result may be shorter than the input.
#'
#' @seealso [extractFeatures()] for building the predictors these labels pair with;
#'   [filterVideoPeriod()] for restricting the data to the periods you scored; [launchVideo()] for
#'   checking a window against the footage.
#' @examples
#' \dontrun{
#' # windows scored from the tag's camera footage
#' annotations <- data.frame(
#'   ID    = c("shark01", "shark01", "shark02"),
#'   event = c("feeding", "social", "feeding"),
#'   start = as.POSIXct(c("2023-05-01 09:15:00", "2023-05-01 11:40:00",
#'                        "2023-05-01 14:02:00"), tz = "UTC"),
#'   end   = as.POSIXct(c("2023-05-01 09:18:00", "2023-05-01 11:42:00",
#'                        "2023-05-01 14:09:00"), tz = "UTC"))
#'
#' # `tags` is the processed output of processTagData()
#' labelled <- annotateData(tags, annotations)
#'
#' # both animals carry both columns, so the cohort can be pooled
#' lapply(labelled, function(x) colSums(x[, c("feeding", "social")]))
#' }
#' @export

annotateData <- function(data,
                         annotations,
                         id.col = "ID",
                         datetime.col = "datetime",
                         event.col = "event",
                         start.col = "start",
                         end.col = "end",
                         selected.events = NULL,
                         verbose = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  .assert_string(id.col, "id.col");    .assert_string(datetime.col, "datetime.col")
  .assert_string(event.col, "event.col"); .assert_string(start.col, "start.col")
  .assert_string(end.col, "end.col")

  ##############################################################################
  # Normalise and validate inputs ##############################################
  ##############################################################################

  # A data.frame IS a list, so `!is.list(data)` alone never fires for the documented single-table input
  # and every column gets treated as an individual. The `inherits(data, "data.frame")` half is what makes
  # the aggregated-table path work - same idiom as filterVideoPeriod().
  .assert_nonempty(data, "data")
  if (!is.list(data) || inherits(data, "data.frame")) {
    .assert_columns(data, id.col, "data")
    ids_col <- data[[id.col]]
    if (is.factor(ids_col)) ids_col <- droplevels(ids_col)   # a subset cohort must not gain phantom ids
    data <- split(data, f = ids_col)
  }
  if (is.null(names(data))) .abort("{.arg data} must be a NAMED list (one element per individual).")

  .assert_nonempty(annotations, "annotations")
  .assert_columns(annotations, c(id.col, event.col, start.col, end.col), "annotations")
  for (col in c(start.col, end.col)) {
    if (!inherits(annotations[[col]], "POSIXct"))
      .abort("Column {.field {col}} in {.arg annotations} must be POSIXct.")
  }

  # per-individual checks, naming the offending element rather than collapsing to one boolean
  for (nm in names(data)) {
    if (is.null(data[[nm]])) next
    .assert_columns(data[[nm]], c(id.col, datetime.col), sprintf("data[['%s']]", nm))
    if (!inherits(data[[nm]][[datetime.col]], "POSIXct"))
      .abort("The {.field {datetime.col}} column must be POSIXct in {.val {nm}}.")
  }

  if (!is.null(selected.events)) {
    if (!is.character(selected.events)) .abort("{.arg selected.events} must be a character vector.")
    invalid <- setdiff(selected.events, unique(as.character(annotations[[event.col]])))
    if (length(invalid))
      .abort(c("Some {.arg selected.events} are not present in {.arg annotations}:",
               "x" = "{.val {invalid}}"))
    annotations <- annotations[as.character(annotations[[event.col]]) %in% selected.events, , drop = FALSE]
  }

  # an event column is created from DATA, so it can silently overwrite a sensor channel ("depth" scored
  # as a behaviour used to replace the depth trace with 0/1). Refuse rather than destroy the record.
  events <- unique(as.character(annotations[[event.col]]))
  events <- events[!is.na(events)]
  if (!length(events)) .abort("{.arg annotations} contains no usable event types in {.field {event.col}}.")
  existing <- unique(unlist(lapply(data, function(d) if (is.null(d)) NULL else names(d))))
  clash <- intersect(events, existing)
  if (length(clash))
    .abort(c("{cli::qty(length(clash))}Event type{?s} {.val {clash}} would overwrite existing column{?s} in {.arg data}.",
             "i" = "Rename the event type in {.arg annotations}, or drop the clashing column from {.arg data}."))

  # annotation rows with no usable window cannot mark anything: drop them, but say so
  bad_window <- is.na(annotations[[start.col]]) | is.na(annotations[[end.col]])
  if (any(bad_window)) annotations <- annotations[!bad_window, , drop = FALSE]

  ##############################################################################
  # Annotate ###################################################################
  ##############################################################################

  n_tags <- length(data)
  .log_header(lvl, "annotateData", "Marking scored events onto sensor data",
              bullets = c(sprintf("Input: %d tag%s", n_tags, if (n_tags != 1) "s" else ""),
                          sprintf("Events: %s", paste(events, collapse = ", "))))

  if (any(bad_window))
    .log_skip(lvl, sprintf("%d annotation row%s dropped (missing %s or %s)",
                           sum(bad_window), if (sum(bad_window) != 1) "s" else "", start.col, end.col))

  ann_ids  <- unique(as.character(annotations[[id.col]]))
  orphans  <- setdiff(ann_ids, names(data))            # scored, but no data to mark
  unscored <- setdiff(names(data), ann_ids)            # data, but never scored

  out <- vector("list", n_tags); names(out) <- names(data)
  totals <- stats::setNames(numeric(length(events)), events)

  for (i in seq_len(n_tags)) {
    id <- names(data)[i]; d <- data[[i]]
    if (is.null(d)) next
    if (lvl >= 2L) .log_h2(lvl, sprintf("%s (%d/%d)", id, i, n_tags))

    # every individual gets every event column, so the cohort stays row-bindable
    for (event in events) d[[event]] <- 0

    id_ann <- annotations[as.character(annotations[[id.col]]) == id, , drop = FALSE]
    if (nrow(id_ann)) {
      t <- d[[datetime.col]]
      for (event in events) {
        rows <- id_ann[as.character(id_ann[[event.col]]) == event, , drop = FALSE]
        if (!nrow(rows)) next
        # .inAnyInterval unions overlapping windows, so a row inside two windows is counted once
        hit <- .inAnyInterval(t, rows[[start.col]], rows[[end.col]])
        d[[event]][hit] <- 1
        totals[[event]] <- totals[[event]] + sum(hit)
        if (lvl >= 2L && any(hit))
          .log_detail(lvl, sprintf("%s: %d window%s \u00b7 %s rows", event, nrow(rows),
                                   if (nrow(rows) != 1) "s" else "", .formatLargeNumber(sum(hit))))
      }
      if (lvl >= 2L && !sum(vapply(events, function(e) sum(d[[e]]), numeric(1))))
        .log_skip(lvl, "no sample fell inside any scored window")
    } else if (lvl >= 2L) {
      .log_skip(lvl, "not present in annotations - all event columns are 0")
    }

    # provenance: this step transformed the deployment, so it belongs in the audit trail
    meta <- .getMeta(d)
    if (!is.null(meta))
      d <- .restoreMeta(d, .appendProcessing(meta, "annotateData",
                                             events = paste(events, collapse = ","),
                                             n_windows = nrow(id_ann),
                                             n_marked = sum(vapply(events, function(e) sum(d[[e]]), numeric(1))),
                                             selected_events = if (is.null(selected.events)) "all"
                                                               else paste(selected.events, collapse = ",")))
    out[[i]] <- d
  }

  out <- Filter(Negate(is.null), out)

  ##############################################################################
  # Report #####################################################################
  ##############################################################################

  # An ID scored but absent from the data means the annotation sheet and the cohort disagree - almost
  # always a typo or a deployment that was dropped upstream. Silently ignoring it loses real scoring
  # effort, so it is surfaced rather than dropped.
  if (length(orphans))
    .warn_grouped("Some annotated individuals are not present in {.arg data}; their events were not applied.",
                  items = orphans, style = "inline")

  if (lvl >= 1L) {
    .log_summary(lvl)
    for (event in events)
      .log_done(lvl, event, ": ", .formatLargeNumber(totals[[event]]), " row",
                if (totals[[event]] != 1) "s", " marked")
    if (length(unscored))
      .log_detail(lvl, sprintf("%d tag%s carried no annotations (all-zero columns): %s",
                               length(unscored), if (length(unscored) != 1) "s" else "",
                               paste(unscored, collapse = ", ")))
    .log_done(lvl, length(out), " of ", n_tags, " tag", if (n_tags != 1) "s", " annotated")
    .log_runtime(lvl, start.time)
  }

  out
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
