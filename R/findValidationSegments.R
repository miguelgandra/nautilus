#######################################################################################################
# Find candidate segments for orientation validation ##################################################
#######################################################################################################

#' Find the clearest manoeuvres to check against video
#'
#' @description
#' Checking a computed orientation against footage means finding a moment where the animal unambiguously
#' did something - a hard turn, a roll, a steep dive - and comparing what the sensors say with what the
#' video shows. Scrubbing hours of footage for such a moment is the main cost of video validation, and
#' most of the record is uninformative because the animal is swimming steadily.
#'
#' This function does the searching. It scans processed data for the events that discriminate best and
#' returns a tidy table of short windows to inspect, so the review starts from the handful of instants
#' where the answer is legible.
#'
#' @param data A tag object, a list of them, a single table with an `id.col`, or a character vector of
#'   `.rds` paths. The output of [processTagData()] is expected.
#' @param types Which event types to look for: any of `"turn"`, a net heading change that tests the
#'   heading and sway signs; `"roll"`, a sustained roll excursion that tests the roll sign; and
#'   `"dive"`, a sustained vertical speed that tests pitch and surge. All three by default. A type whose
#'   required column is absent is skipped with a note.
#' @param n How many non-overlapping candidates to return per type, per deployment (default `5`). Raise
#'   it if the best few turn out to be ambiguous on screen.
#' @param window The length of each returned window, in seconds, which is also the smoothing window for
#'   the event signal (default `20`). Longer windows favour sustained manoeuvres over sharp ones, so
#'   match it to the timescale on which your animal actually turns.
#' @param id.col Which column identifies the animal (default `"ID"`).
#' @param datetime.col Which column holds the timestamps (default `"datetime"`).
#' @param verbose How much detail to print: `0`/`"quiet"`, `1`/`"normal"`, or `2`/`"detailed"`
#'   (default).
#'
#' @details
#' For each requested type the per-sample signal is reduced to a centred rolling mean over `window`
#' seconds, so that brief jitter and wiggles cancel and only sustained, net manoeuvres score. The top
#' `n` non-overlapping peaks are then returned as windows centred on each peak.
#'
#' The rolling mean is what makes the result useful rather than merely extreme. A raw per-sample maximum
#' finds the single noisiest instant in the record, which on inspection is usually a knock to the tag;
#' a net change over twenty seconds finds a manoeuvre the animal actually performed and that a reviewer
#' can see.
#'
#' @return A data frame of candidate segments, sorted by deployment, type and rank, with columns:
#'
#' - `id` - the deployment.
#' - `type` - `"turn"`, `"roll"` or `"dive"`.
#' - `rank` - 1 is the clearest event of that type for that deployment.
#' - `start`, `end` - the window, `window` seconds wide.
#' - `peak_time` - the instant of strongest signal within the window.
#' - `value`, `unit` - the interpretable magnitude: net heading change in degrees for a turn, peak
#'   absolute roll in degrees for a roll, and net depth change in metres for a dive.
#'
#' @seealso [processTagData()] for the step that must come first; [reviewTagMapping()], which uses this
#'   to choose its clips; [launchVideo()] for jumping straight to one of these moments.
#'
#' @examples
#' \dontrun{
#' processed <- processTagData(imported)
#'
#' # the 5 clearest turns, rolls and dives per animal, as 20 s windows to check against video
#' segs <- findValidationSegments(processed, types = c("turn", "roll", "dive"), n = 5, window = 20)
#' subset(segs, rank == 1)   # the single best manoeuvre of each type per deployment
#' }
#' @export

findValidationSegments <- function(data,
                                   types = c("turn", "roll", "dive"),
                                   n = 5,
                                   window = 20,
                                   id.col = "ID",
                                   datetime.col = "datetime",
                                   verbose = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  .assert_number(n, "n", min = 1); .assert_number(window, "window", min = 0)
  types <- match.arg(types, c("turn", "roll", "dive"), several.ok = TRUE)

  # resolve input: a character vector of RDS paths, or an in-memory list / single data.frame
  is_filepaths <- is.character(data)
  .assert_nonempty(data, "data")             # loud failure on empty input (e.g. a typo'd list.files() -> character(0))
  if (is_filepaths) {
    missing <- data[!file.exists(data)]
    if (length(missing)) .abort(c("These input files were not found:", stats::setNames(missing, rep("*", length(missing)))))
  } else if (!is.list(data) || inherits(data, "data.frame")) {
    .assert_columns(data, id.col, "data")
    data <- split(data, f = data[[id.col]])
  }
  n_tags <- length(data)
  if (n_tags == 0) return(.emptySegments())

  .log_header(lvl, "findValidationSegments", "Locating the clearest validation manoeuvres",
              bullets = sprintf("Input: %d tag%s", n_tags, if (n_tags != 1) "s" else ""),
              arrow = paste0("Events: ", paste(types, collapse = ", "),
                             sprintf(" \u00b7 top %d \u00b7 %gs windows", n, window)))

  out <- vector("list", n_tags)
  for (i in seq_len(n_tags)) {
    dt <- if (is_filepaths) readRDS(data[i]) else data[[i]]
    if (is.null(dt) || nrow(dt) == 0) next
    if (!data.table::is.data.table(dt)) dt <- data.table::as.data.table(dt)
    id <- as.character(unique(dt[[id.col]])[1])
    seg <- .findSegmentsForTag(dt, id, types, n, window, datetime.col)
    if (lvl >= 2L) {
      .log_h2(lvl, sprintf("%s (%d/%d)", id, i, n_tags))
      for (ty in types) {
        k <- sum(seg$type == ty)
        if (!ty %in% .availableTypes(dt)) .log_skip(lvl, ty, ": skipped (missing column)")
        else .log_detail(lvl, sprintf("%s: %d segment%s", ty, k, if (k != 1) "s" else ""))
      }
    }
    out[[i]] <- seg
  }

  result <- do.call(rbind, out)
  if (is.null(result)) result <- .emptySegments()
  rownames(result) <- NULL

  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, nrow(result), " candidate segment", if (nrow(result) != 1) "s", " across ", n_tags, " tag", if (n_tags != 1) "s")
    .log_runtime(lvl, start.time)
  }
  result
}
