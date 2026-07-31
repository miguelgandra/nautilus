#######################################################################################################
# Export a reconstructed pseudo-track for state-space modelling (aniMotum / crawl) #####################
#######################################################################################################

#' Export a reconstructed track for state-space modelling
#'
#' @description
#' A pseudo-track is a dense, plausible path, but it is a reconstruction rather than a set of
#' observations with a formal error model. Analyses that need credible intervals - a utilisation
#' distribution, a behavioural state estimate - require one.
#'
#' A continuous-time state-space model provides it: each reconstructed position is treated as a
#' measurement with a known error, here the `pseudo_error` [reconstructTrack()] already computes, and a
#' movement process is fitted to them, returning a regularised track with uncertainty. This package
#' deliberately does not re-implement that machinery. This function instead formats the track into the
#' tidy frame the established tools expect, so it can be handed to \pkg{aniMotum} (Jonsen et al. 2023)
#' or \pkg{crawl} (Johnson et al. 2008).
#'
#' @param data The output of [reconstructTrack()]: a tag object, a list of them, a single table with an
#'   `id.col`, or a character vector of `.rds` paths. Each must carry the timestamp, longitude and
#'   latitude columns named below.
#' @param id.col Which column identifies the animal (default `"ID"`).
#' @param datetime.col Which column holds the timestamps (default `"datetime"`).
#' @param lon.col,lat.col Which columns hold the reconstructed longitude and latitude. Defaults
#'   `"pseudo_lon"` and `"pseudo_lat"`.
#' @param error.col Which column holds the per-position one-sigma error in metres, or `NULL` to omit it
#'   entirely. Default `"pseudo_error"`. Rows with a finite value are exported with their own error;
#'   rows without it fall back to the model's default for a GPS position.
#' @param thin.minutes Keep only one position per this many minutes (default `5`); set `0` to keep every
#'   sample. A dense track is both heavy for a state-space model and largely redundant, since
#'   neighbouring samples carry almost the same information. Thin less aggressively if your analysis
#'   turns on short-lived events.
#' @param verbose How much detail to print: `0`/`"quiet"`, `1`/`"normal"`, which prints the header, any
#'   per-deployment skip notices and the export summary, or `2`/`"detailed"` (default), which adds a
#'   progress bar while the deployments are read.
#'
#' @details
#' ## What is exported
#'
#' One tidy data frame with every deployment stacked and keyed by `id`, in the layout \pkg{aniMotum}'s
#' `fit_ssm()` reads:
#'
#' - `id` - the deployment identifier.
#' - `date` - the timestamp.
#' - `lc` - the location class for each row: `"GL"`, a generic location, where a reckoning error is
#'   supplied so that the model uses it, and otherwise `"G"` for GPS, which uses the model's own default
#'   error.
#' - `lon`, `lat` - the reconstructed position.
#' - `x.sd`, `y.sd` - the one-sigma position error in **metres**. The error is isotropic, so the two are
#'   equal. They are emitted only when at least one position carries an error, so a track without one
#'   stays in the five-column form; \pkg{aniMotum} infers the data type from whether these columns are
#'   present.
#'
#' ## Using the result
#'
#' With \pkg{aniMotum}, pass the frame straight to `fit_ssm()`. With \pkg{crawl}, project the
#' coordinates and supply the same metre-scale error to `crwMLE()`'s error model. There is a worked
#' example in `vignette("movement-tracks", package = "nautilus")`.
#'
#' Both packages are optional. This function only formats a data frame and does not require either to
#' be installed.
#'
#' @return A data frame with columns `id`, `date`, `lc`, `lon` and `lat`, plus `x.sd` and `y.sd` where
#'   any position carries an error, ready for `aniMotum::fit_ssm()`. Deployments lacking the required
#'   columns are skipped with a message.
#'
#' @references
#' Johnson DS, London JM, Lea MA, Durban JW (2008) Continuous-time correlated random walk model for
#' animal telemetry data. *Ecology* 89:1208-1215. \doi{10.1890/07-1032.1}
#'
#' Jonsen ID, Grecian WJ, Phillips L, *et al.* (2023) aniMotum, an R package for animal movement data:
#' rapid quality control, behavioural estimation and simulation. *Methods in Ecology and Evolution*
#' 14:806-816. \doi{10.1111/2041-210X.14060}
#'
#' @seealso [reconstructTrack()] for producing the track; [trackMetrics()] for summarising it without a
#'   model.
#'
#' @examples
#' \dontrun{
#' tracks <- reconstructTrack(processed)
#' ssm_in <- exportForSSM(tracks, thin.minutes = 10)
#' fit <- aniMotum::fit_ssm(ssm_in, model = "crw", time.step = 2)   # a 2 h regularised track
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
