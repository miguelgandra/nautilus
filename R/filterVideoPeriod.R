#######################################################################################################
# Restrict sensor data to video-covered (and optionally annotated) periods ############################
#######################################################################################################

#' Restrict tag data to video-covered (and optionally annotated) periods
#'
#' @description
#' Subsets each individual's sensor data to the periods for which video footage exists (from
#' `video.metadata`), and optionally further to manually annotated intervals. Useful for aligning
#' sensor analyses with the footage that backs them. Temporal jumps between video segments are handled
#' naturally - a row is kept if its timestamp falls within ANY video segment (and ANY annotation
#' interval, when supplied). For annotation intervals, a missing `start` is filled with the earliest,
#' and a missing `end` with the latest, video-covered time for that individual.
#'
#' @param data A list of datasets (one per individual), or a single aggregated data.table/data.frame
#'   with an `id.col`. Each must contain a POSIXct `datetime.col`.
#' @param video.metadata A data.frame/data.table of video segments with columns `id.col`, `start` and
#'   `end` (POSIXct), as returned by \link{getVideoMetadata}.
#' @param annotation.intervals Optional data.frame/data.table of annotation intervals with `id.col`,
#'   `start` and `end` (POSIXct). `NULL` (default) filters on video availability only.
#' @param id.col,datetime.col Column names for the ID and datetime. Defaults `"ID"`/`"datetime"`.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal", or `2`/"detailed" (default).
#'
#' @return A named list of filtered data.tables (individuals with no retained rows are dropped).
#' @seealso \link{getVideoMetadata}, \link{annotateData}.
#' @examples
#' \dontrun{
#' processed <- processTagData(imported)
#' meta      <- getVideoMetadata("./videos")
#' # keep only the sensor rows that fall within video coverage
#' filtered  <- filterVideoPeriod(processed, meta)
#' }
#' @export

filterVideoPeriod <- function(data,
                              video.metadata,
                              annotation.intervals = NULL,
                              id.col = "ID",
                              datetime.col = "datetime",
                              verbose = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")

  # normalise data to a named list of data.tables keyed by id
  if (!is.list(data) || inherits(data, "data.frame")) {
    .assert_columns(data, id.col, "data")
    data <- split(data, f = data[[id.col]])
  }
  data <- lapply(data, function(x) if (data.table::is.data.table(x)) x else data.table::as.data.table(x))
  for (nm in names(data)) {
    .assert_columns(data[[nm]], datetime.col, sprintf("data[['%s']]", nm))
    if (!inherits(data[[nm]][[datetime.col]], "POSIXct"))
      .abort("The {.field {datetime.col}} column must be POSIXct in {.val {nm}}.")
  }

  # validate video.metadata (and annotation.intervals when supplied)
  .assert_columns(video.metadata, c(id.col, "start", "end"), "video.metadata")
  if (!inherits(video.metadata$start, "POSIXct") || !inherits(video.metadata$end, "POSIXct"))
    .abort("Columns {.field start} and {.field end} in {.arg video.metadata} must be POSIXct.")
  if (!is.null(annotation.intervals)) {
    .assert_columns(annotation.intervals, c(id.col, "start", "end"), "annotation.intervals")
    if (!inherits(annotation.intervals$start, "POSIXct") || !inherits(annotation.intervals$end, "POSIXct"))
      .abort("Columns {.field start} and {.field end} in {.arg annotation.intervals} must be POSIXct.")
  }

  n_tags <- length(data)
  .log_header(lvl, "filterVideoPeriod", "Restricting data to video-covered periods",
              bullets = sprintf("Input: %d tag%s", n_tags, if (n_tags != 1) "s" else ""),
              arrow = if (!is.null(annotation.intervals)) "Filter: video coverage + annotation intervals" else "Filter: video coverage")

  out <- vector("list", n_tags); names(out) <- names(data)
  for (i in seq_len(n_tags)) {
    id <- names(data)[i]; d <- data[[i]]; t <- d[[datetime.col]]
    if (lvl >= 2L) .log_h2(lvl, sprintf("%s (%d/%d)", id, i, n_tags))

    if (!id %in% video.metadata[[id.col]]) { .log_skip(lvl, "no video metadata - skipped"); next }
    tv <- video.metadata[video.metadata[[id.col]] == id, ]
    keep <- .inAnyInterval(t, tv$start, tv$end)
    if (!any(keep)) { .log_skip(lvl, "no data within video coverage"); next }

    if (!is.null(annotation.intervals) && id %in% annotation.intervals[[id.col]]) {
      ap <- as.data.frame(annotation.intervals[annotation.intervals[[id.col]] == id, ])
      vt <- t[keep]
      ap$start[is.na(ap$start)] <- min(vt); ap$end[is.na(ap$end)] <- max(vt)
      keep <- keep & .inAnyInterval(t, ap$start, ap$end)
    }
    if (!any(keep)) { .log_skip(lvl, "no data within the annotation intervals"); next }

    out[[i]] <- d[keep]
    if (lvl >= 2L) {
      span <- as.numeric(difftime(max(t[keep]), min(t[keep]), units = "secs"))
      .log_detail(lvl, sprintf("retained %s rows \u00b7 ~%s", .formatLargeNumber(sum(keep)), .fmt_duration(span)))
    }
  }

  out <- Filter(Negate(is.null), out)
  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, length(out), " of ", n_tags, " tag", if (n_tags != 1) "s", " retained video-covered data")
    .log_runtime(lvl, start.time)
  }
  out
}


#' Logical mask: which of `t` fall within ANY [starts, ends] interval.
#' @keywords internal
#' @noRd
.inAnyInterval <- function(t, starts, ends) {
  m <- logical(length(t))
  for (j in seq_along(starts)) if (!is.na(starts[j]) && !is.na(ends[j])) m <- m | (t >= starts[j] & t <= ends[j])
  m
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
