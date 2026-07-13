#######################################################################################################
# Export a reconstructed pseudo-track for state-space modelling (aniMotum / crawl) #####################
#######################################################################################################

#' Export a reconstructed pseudo-track for state-space modelling
#'
#' @description
#' Formats the output of \code{\link{reconstructTrack}} into the tidy data frame that continuous-time
#' state-space movement models expect, so the dead-reckoned pseudo-track can be handed to a dedicated,
#' well-tested package - \pkg{aniMotum} (Jonsen et al. 2023) or \pkg{crawl} (Johnson et al. 2008) - for
#' formal smoothing and per-position uncertainty. This package deliberately does not re-implement a
#' state-space smoother; it delegates to those.
#'
#' @details
#' ## Why hand off to a state-space model?
#' \code{\link{reconstructTrack}} produces a dense, plausible path, but a *pseudo*-track is a
#' reconstruction, not a set of observations with a formal error model. A continuous-time state-space model
#' (SSM) treats each reconstructed position as a measurement with known error (here, `pseudo_error`) and
#' fits a movement process to them, returning a regularised track with credible intervals - the standard
#' currency for downstream analyses such as utilisation distributions or behavioural state estimation. The
#' community-standard tools for this are \pkg{aniMotum}'s continuous-time correlated random walk / move
#' persistence models and \pkg{crawl}'s CTCRW (Johnson et al. 2008; Jonsen et al. 2023).
#'
#' ## What is exported
#' A single tidy data frame (all deployments stacked, keyed by `id`) in the format \pkg{aniMotum}'s
#' `fit_ssm()` reads:
#' \itemize{
#'   \item `id` - deployment identifier.
#'   \item `date` - timestamp (POSIXct).
#'   \item `lc` - location class, per row: `"GL"` (generic location) where a reckoning error is supplied, so
#'     `fit_ssm()` uses it; otherwise `"G"` (GPS, the model's default error).
#'   \item `lon`, `lat` - the reconstructed position.
#'   \item `x.sd`, `y.sd` - the 1-sigma position error in \strong{metres} (the isotropic `pseudo_error`, so
#'     both are equal). These are \pkg{aniMotum}'s current per-observation error columns (they replaced the
#'     deprecated `lonerr`/`laterr` in v1.2), read as metres. They are emitted only when at least one
#'     position carries an error, so a purely-GPS export stays in the 5-column GPS format (\pkg{aniMotum}
#'     infers the data type from whether these columns are present).
#' }
#' A dense (e.g. 1 Hz) pseudo-track is both heavy for an SSM and largely redundant, so it is thinned to one
#' position per `thin.minutes` interval before export (set `thin.minutes = 0` to keep every sample).
#'
#' ## Using the result
#' With \pkg{aniMotum}, pass the frame straight to `fit_ssm()`; with \pkg{crawl}, project the coordinates and
#' supply the same metre-scale error (`y.sd`) to `crwMLE()`'s error model. See the dead-reckoning tutorial
#' for a worked example. \pkg{aniMotum} / \pkg{crawl} are optional (\code{Suggests}); this function only
#' formats a data frame and does not require them to be installed.
#'
#' @param data The output of \code{\link{reconstructTrack}}: a `nautilus_tag` / data.frame, a (named) list
#'   of them, a single aggregated data.frame with an `id.col`, or a character vector of `.rds` paths. Each
#'   must carry `datetime.col`, `lon.col` and `lat.col`.
#' @param id.col,datetime.col Column names for the animal ID and timestamp. Defaults `"ID"` / `"datetime"`.
#' @param lon.col,lat.col Reconstructed longitude / latitude columns. Defaults `"pseudo_lon"` /
#'   `"pseudo_lat"`.
#' @param error.col Column holding the per-position 1-sigma error in metres, or `NULL` to omit it. Default
#'   `"pseudo_error"`. Rows with a finite value get location class `"GL"` and `x.sd`/`y.sd`; rows without
#'   (or every row, when the column is absent / `NULL`) get `"G"`.
#' @param thin.minutes Thin the track to one position per this many minutes before export. Default 5; set
#'   `0` to keep every sample.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal", or `2`/"detailed" (default). At
#'   `"normal"` prints the header, any per-deployment skip notices, and the export summary; `"detailed"`
#'   additionally shows a live progress bar while the deployments are read and a skipped-deployment count.
#'
#' @return A `data.frame` with columns `id`, `date`, `lc`, `lon`, `lat` (plus `x.sd`, `y.sd` when any
#'   position carries an error), ready for \code{aniMotum::fit_ssm()}. Deployments lacking the required
#'   columns are skipped with a message.
#' @references
#' Johnson DS, London JM, Lea MA, Durban JW (2008) Continuous-time correlated random walk model for animal
#' telemetry data. *Ecology*. 89:1208-1215. \doi{10.1890/07-1032.1}
#'
#' Jonsen ID, Grecian WJ, Phillips L, *et al.* (2023) aniMotum, an R package for animal movement data:
#' rapid quality control, behavioural estimation and simulation. *Methods in Ecology and Evolution*.
#' 14:806-816. \doi{10.1111/2041-210X.14060}
#' @seealso \code{\link{reconstructTrack}}, \code{\link{trackMetrics}}
#' @examples
#' \dontrun{
#' tracks <- reconstructTrack(processed)
#' ssm_in <- exportForSSM(tracks, thin.minutes = 10)
#' fit <- aniMotum::fit_ssm(ssm_in, model = "crw", time.step = 2)   # 2-h regularised track
#' }
#' @export
exportForSSM <- function(data,
                         id.col = "ID",
                         datetime.col = "datetime",
                         lon.col = "pseudo_lon",
                         lat.col = "pseudo_lat",
                         error.col = "pseudo_error",
                         thin.minutes = 5,
                         verbose = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  .assert_string(lon.col, "lon.col"); .assert_string(lat.col, "lat.col")
  .assert_string(error.col, "error.col", null_ok = TRUE)
  .assert_number(thin.minutes, "thin.minutes", min = 0)

  r <- .resolveInput(data, id.col = id.col)
  .log_header(lvl, "exportForSSM", "Formatting reckoned tracks for a state-space model",
              bullets = sprintf("Input: %d deployment%s", r$n, if (r$n != 1) "s" else ""))
  out <- vector("list", r$n); n_skip <- 0L
  pb <- .log_progress_start(lvl, r$n, "Exporting")                 # live bar at detailed verbosity (lvl >= 2)

  for (i in seq_len(r$n)) {
    .log_progress_step(pb)
    x <- r$get(i)
    if (!data.table::is.data.table(x)) x <- data.table::as.data.table(x)
    who <- tryCatch(as.character(unique(x[[id.col]])[1]), error = function(e) NA_character_)
    if (length(who) != 1L || is.na(who) || !nzchar(who)) who <- r$ids[i]

    if (!all(c(datetime.col, lon.col, lat.col) %in% names(x))) {
      if (lvl >= 1L) cli::cli_alert_warning("{.val {who}}: missing required column(s); skipped.")
      n_skip <- n_skip + 1L; next
    }
    d <- x[is.finite(x[[lon.col]]) & is.finite(x[[lat.col]]) & is.finite(as.numeric(x[[datetime.col]]))]
    if (!nrow(d)) {
      if (lvl >= 1L) cli::cli_alert_warning("{.val {who}}: no finite positions; skipped.")
      n_skip <- n_skip + 1L; next
    }
    data.table::setorderv(d, datetime.col)
    if (thin.minutes > 0) {                                    # keep one position per time bin
      bin <- floor(as.numeric(d[[datetime.col]]) / (thin.minutes * 60))
      d <- d[!duplicated(bin)]
    }

    lat <- d[[lat.col]]
    err_m <- if (!is.null(error.col) && error.col %in% names(d)) d[[error.col]] else rep(NA_real_, nrow(d))
    # per-row location class: "GL" (generic location, with a supplied error) where a finite reckoning error
    # exists, else "G" (GPS, aniMotum's default error model). aniMotum picks the data type from the PRESENCE
    # of the x.sd/y.sd columns, so a purely-GPS deployment must omit them entirely (else it is read as GL).
    lc <- ifelse(is.finite(err_m), "GL", "G")
    frame <- data.frame(id = who, date = d[[datetime.col]], lc = lc,
                        lon = d[[lon.col]], lat = lat, stringsAsFactors = FALSE)
    if (any(is.finite(err_m))) {
      # aniMotum (>= 1.2) reads the x.sd (longitude) / y.sd (latitude) columns as per-obs SDs in METRES (it
      # divides them by 1000 to km internally; the degrees interpretation is tied to the deprecated
      # lonerr/laterr names, not x.sd/y.sd). pseudo_error is an isotropic radius, so both take it directly.
      frame$x.sd <- err_m
      frame$y.sd <- err_m
    }
    out[[i]] <- frame
  }

  .log_progress_done(pb)
  out <- Filter(Negate(is.null), out)
  if (!length(out)) {
    if (lvl >= 1L) cli::cli_alert_warning("exportForSSM: no exportable tracks.")
    return(data.frame(id = character(), date = as.POSIXct(character()), lc = character(),
                      lon = numeric(), lat = numeric(), stringsAsFactors = FALSE))
  }
  res <- as.data.frame(data.table::rbindlist(out, fill = TRUE))   # fill: pure-GPS deployments have no x.sd/y.sd
  if (lvl >= 1L) {
    .log_summary(lvl)
    tnote <- if (thin.minutes > 0) sprintf(" (thinned to %g min)", thin.minutes) else ""
    if (lvl >= 2L && n_skip > 0L)
      .log_detail(lvl, sprintf("skipped %d deployment%s (missing columns / no finite positions)", n_skip, if (n_skip != 1) "s" else ""))
    .log_done(lvl, sprintf("%d positions across %d deployment%s exported%s", nrow(res), length(out),
                           if (length(out) != 1) "s" else "", tnote))
    .log_runtime(lvl, start.time)
  }
  res
}
