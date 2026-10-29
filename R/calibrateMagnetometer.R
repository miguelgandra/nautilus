#' Estimate magnetometer calibration parameters from tag recordings
#'
#' @description
#' Estimates hard-iron offsets and soft-iron corrections from three-axis magnetometer measurements.
#' Calibration parameters, diagnostic quality measures and source information are stored as a proposal
#' in each deployment's metadata; sensor values are not corrected by this function.
#'
#' Fits can use individual deployments, pooled deployments sharing a physical tag, or separate
#' calibration recordings. Orientation coverage and magnetic-field consistency determine whether a
#' full ellipsoid fit is supported and how the resulting proposal is classified.
#'
#' Run after [applyAxisMapping()] and before [processTagData()]. The latter can apply a stored proposal
#' when its confidence, coordinate frame and application state satisfy [calibrationControl()].
#'
#' @param data A tag dataset, a list of tag datasets, a data frame containing multiple deployments
#'   identified by \code{id.col}, or a character vector of \code{.rds} file paths. Datasets must
#'   contain uncorrected \code{mx}, \code{my} and \code{mz} measurements in consistent units
#'   (\eqn{\mu}T for geomagnetic normalisation). Body-axis-mapped data are recommended. File inputs are
#'   read sequentially to gather fitting samples, then read again when writing proposals.
#' @param control A [magCalibrationControl()] object or a named list of its arguments specifying the
#'   fitting method, target field magnitude and diagnostic thresholds.
#' @param group.by One or more tag-metadata keys used to group deployments when
#'   \code{calibration.data = NULL}: \code{"package_id"} (default), \code{"logger_id"},
#'   \code{"tag"} or \code{"type"}. Several keys define a composite group. A deployment missing any
#'   requested key is fitted separately. Use keys identifying a physical instrument with stable
#'   magnetic configuration; a shared model or type alone does not establish this.
#' @param calibration.data Optional separate calibration recordings, in the same input forms as
#'   \code{data}. Matching recordings supply the calibration fit instead of the deployment clouds.
#'   Stored axis mappings are used to reframe source measurements into the target frame. Collect
#'   rotation recordings with broad orientation coverage in a magnetically clean setting.
#'   Default \code{NULL}.
#' @param calibration.match Tag-identity fields used to match external recordings, in priority order
#'   (default \code{c("package_id", "logger_id")}). Exact, non-blank matches at the first available
#'   matching key are used; the fields are alternatives, not a composite key. Source and target
#'   \code{paddle_wheel} flags must also agree. Ignored without \code{calibration.data}.
#' @param calibration.on.missing Policy when no usable external source can be matched or fitted, or
#'   when its centre is rejected: \code{"fallback"} (default) fits the target deployment alone,
#'   \code{"error"} stops the call, or \code{"skip"} retains the deployment without writing a new
#'   proposal. Skipping does not remove any existing proposal. Ignored without
#'   \code{calibration.data}.
#' @param calibration.context Optional descriptive labels for external recordings, such as
#'   \code{"in_water"}, \code{"bench"} or \code{"lab"}, recycled over the source recordings.
#'   All matched source identifiers and the first matched source's context are recorded. Labels do not
#'   alter fitting thresholds. Ignored without \code{calibration.data}.
#' @param return.data Logical; whether to return datasets in memory (default \code{TRUE}). With
#'   \code{FALSE}, written \code{.rds} paths are returned invisibly, requiring \code{output.dir}.
#' @param output.dir An existing directory in which datasets are saved as individual
#'   \code{<id>.rds} files. Supplying a directory triggers saving, including for deployments without
#'   a new calibration proposal. Default \code{NULL}.
#' @param output.suffix Optional string appended to saved deployment identifiers before \code{.rds},
#'   to label a calibration run or avoid overwriting earlier files. Default \code{NULL}.
#' @param compress Compression used when saving \code{.rds} files: \code{TRUE} (default, gzip),
#'   \code{FALSE}, or one of \code{"gzip"}, \code{"bzip2"} or \code{"xz"}. Only used when
#'   \code{output.dir} is specified. See [base::saveRDS()].
#' @param plot Logical; whether to display the diagnostic report on the active graphics device
#'   (default \code{FALSE}).
#' @param plot.file Optional path to a multi-page diagnostic PDF, independent of \code{plot}. The name
#'   must end in \code{.pdf} and the parent directory must exist. Default \code{NULL}.
#' @param force.plots Logical; whether to include detail pages for all estimated proposals rather than
#'   only low- and medium-confidence proposals (default \code{FALSE}). A single-deployment run includes
#'   a detail page whenever an estimate is available, regardless of confidence.
#' @param id.col,datetime.col Names of the deployment-identifier and \code{POSIXct} timestamp columns
#'   (defaults \code{"ID"} and \code{"datetime"}).
#' @param verbose How much detail to print: \code{0}/\code{"quiet"}, \code{1}/\code{"normal"}, or
#'   \code{2}/\code{"detailed"} (default).
#'
#' @details
#' ## Calibration model and identifiability
#'
#' For a magnetic-field column vector \eqn{m}, the proposed correction is
#' \deqn{m_{\mathrm{corrected}} = S\,(m_{\mathrm{raw}} - c),}
#' where \eqn{c} is the three-component hard-iron centre and \eqn{S} is the \eqn{3 \times 3}
#' soft-iron matrix. Hard iron represents an additive magnetic offset; soft iron describes directional
#' distortion and scaling.
#'
#' Estimation requires rotation through sufficiently varied orientations. A near-horizontal swimmer
#' may sample a narrow band rather than a three-dimensional field cloud, leaving the perpendicular
#' centre and out-of-plane distortion poorly constrained. More samples from the same posture do not
#' resolve this lack of coverage.
#'
#' The implementation gathers a decimated fitting cloud and trims gross radial outliers. At least
#' 200 complete three-axis samples are required to gather a cloud. This trimming is not a general
#' robust-regression procedure: sensor-integrity and signal-quality checks should precede calibration.
#' Where acceleration is available, a smoothed gravity estimate supports inclination diagnostics.
#'
#' ## Fitting methods and confidence
#'
#' With \code{control$method = "ellipsoid"}, an algebraic least-squares ellipsoid is assessed for
#' positive definiteness, conditioning and coverage on all axes. Where an expected geomagnetic
#' inclination is available, the dip residual also gates acceptance.
#'
#' Unsupported ellipsoid fits are routed to a constrained hard-iron fallback for approximately planar
#' clouds. The in-plane centre is estimated from robust ranges; the perpendicular component can be
#' constrained by geomagnetic inclination and gravity when available. Without that constraint, the
#' range midpoint is used. The fallback does not estimate directional soft-iron distortion, although a
#' scalar field-magnitude normalisation may still be included in \eqn{S}. Clouds with inadequate
#' extent or unsuitable geometry can yield no proposal.
#'
#' Setting \code{control$method = "diagonal"} instead requests per-axis offset and scale estimates.
#' It is an alternative model, not evidence that an under-covered recording determines all calibration
#' parameters.
#'
#' Proposals receive \code{"high"}, \code{"medium"} or \code{"low"} confidence from coverage,
#' corrected-radius dispersion and available dip diagnostics. Full-fit confidence cannot be high
#' without an available dip check. The planar fallback is capped at medium confidence and assesses
#' support for the in-plane centre using azimuthal coverage. These are diagnostic categories, not
#' confidence intervals or guarantees of heading accuracy.
#'
#' ## Field magnitude and coordinate frame
#'
#' \code{control$target.field} overrides the target magnitude. Otherwise, with
#' \code{control$igrf.normalize = TRUE}, available deployment coordinates and time are used to obtain
#' the expected geomagnetic field through \pkg{oce}; without a usable reference, the native centred
#' field magnitude is retained. Inclination diagnostics may be unavailable when coordinates, time or
#' acceleration are missing. This calibration does not convert magnetic heading to true heading.
#'
#' Magnetometer and accelerometer measurements must use compatible coordinate frames. The proposal
#' records the target's axis mapping so that [processTagData()] can reject a stored correction whose
#' frame no longer matches. Run on uncorrected measurements, not on magnetometer values to which a
#' calibration has already been applied.
#'
#' ## Pooling and paddle-wheel recordings
#'
#' Without external recordings, eligible deployments sharing \code{group.by} keys are pooled after
#' centring and normalising their fitting clouds to a common radius. The pooled shape is shared while
#' centres and target scales are resolved per deployment. Group members must have compatible axis
#' mappings and stable sensor configuration; grouping does not automatically establish either.
#'
#' Paddle-wheel and non-paddle recordings are never pooled together. For deployments marked
#' \code{tag$paddle_wheel = TRUE}, magnetometer values are smoothed over approximately three seconds
#' for fitting, reducing high-frequency paddle-magnet variation. This does not change the raw
#' measurements in the returned dataset.
#'
#' ## External calibration sources
#'
#' Matching source recordings at the selected identity key are pooled and reframed using their stored
#' axis mappings. In this mode \code{group.by} does not pool target deployments; a fallback fits only
#' the target's own cloud.
#'
#' For non-paddle targets, the external hard-iron centre is compared with an available in-situ centre.
#' The separation, expressed relative to field radius, can cap confidence at medium above
#' \code{control$center.warn}, or reject the source above \code{control$center.reject}. Paddle targets
#' instead use their own in-situ centre where available; using a source centre when it is unavailable
#' caps confidence at medium.
#'
#' These checks cannot establish that a source recording is free of environmental magnetic
#' interference. A magnetic object rotating with the tag can produce a well-shaped cloud with a
#' displaced centre. Inspect the deployment diagnostics and calibration environment, not just the
#' confidence label. Sources that do not improve orientation coverage are reported but are not
#' automatically rejected.
#'
#' ## Stored proposal and later application
#'
#' A successful estimate replaces \code{getTagMetadata(x)$mag_calibration} with a new, unapplied
#' proposal and appends a processing-history entry describing its source, method and principal
#' diagnostics. Deployments with no estimate are retained, with earlier calibration metadata unchanged.
#' No sensor values, existing orientation columns or whole-deployment exclusion logs are modified.
#'
#' Application is a separate [processTagData()] step. Stored reuse requires appropriate
#' [calibrationControl()] settings, a finite centre, a supported matrix, matching axis metadata,
#' high or medium confidence and an unapplied state. A stored proposal is therefore not evidence that
#' calibration has been applied.
#'
#' ## Diagnostic report
#'
#' The overview summarises grouping, fitting method, coverage, corrected-radius dispersion, dip
#' residual and confidence, ordered to prioritise weaker results. Detail pages show raw and
#' hypothetically corrected clouds in three plane projections, field-magnitude distributions and
#' available inclination diagnostics. They illustrate the proposed correction rather than changes
#' made to the sensor record.
#'
#' @return With \code{return.data = TRUE}, a named list of deployment datasets with unchanged sensor
#'   measurements and any available calibration proposals. With \code{return.data = FALSE}, a
#'   character vector of written \code{.rds} paths, returned invisibly.
#'
#' New proposals are stored in \code{getTagMetadata(x)$mag_calibration$proposed}:
#' \describe{
#'   \item{\code{params}}{\code{center}, a numeric vector of length three; \code{soft_iron}, a
#'     \eqn{3 \times 3} matrix; and \code{axis_net}, the stored axis mapping.}
#'   \item{\code{qc}}{\code{confidence}, \code{coverage_ok}, \code{radcv},
#'     \code{igrf_residual} and \code{axis_span}. \code{radcv} is the standard deviation of corrected
#'     field radius divided by its median; dip residual and axis spans are in degrees. Unavailable
#'     diagnostics are missing values.}
#'   \item{\code{provenance}}{Fitting method, source (\code{"per_tag"}, \code{"per_package"} or
#'     \code{"external"}), grouping and fit labels, number of contributing deployments or external
#'     recordings, and available external-source identifiers, context and centre diagnostics.}
#' }
#'
#' The enclosing calibration record has \code{applied = FALSE} for a new proposal. Use
#' [processingHistory()] to inspect the estimation history, and the applied calibration block after
#' [processTagData()] to determine whether it was used. Diagnostic graphics and saved datasets are
#' optional side effects.
#'
#' @seealso [magCalibrationControl()], [calibrationControl()], [applyAxisMapping()],
#'   [checkSensorIntegrity()], [checkSensorQuality()], [processTagData()], [getTagMetadata()],
#'   [processingHistory()].
#'
#' @examples
#' # Synthetic orientations illustrate estimation without a geomagnetic reference.
#' n <- 1000L
#' z <- seq(-0.99, 0.99, length.out = n)
#' phi <- seq_len(n) * pi * (3 - sqrt(5))
#' field <- 50 * cbind(
#'   sqrt(1 - z^2) * cos(phi),
#'   sqrt(1 - z^2) * sin(phi),
#'   z)
#' recording <- data.frame(
#'   ID = "deployment_01",
#'   datetime = as.POSIXct("2023-01-01", tz = "UTC") + seq_len(n) - 1,
#'   mx = 1.2 * field[, 1] + 5,
#'   my = 0.8 * field[, 2] - 3,
#'   mz = field[, 3] + 2)
#' estimated <- calibrateMagnetometer(
#'   recording, control = magCalibrationControl(igrf.normalize = FALSE),
#'   verbose = "quiet")
#' getTagMetadata(estimated[[1]])$mag_calibration$proposed$qc
#'
#' \dontrun{
#' # Use uncorrected, body-axis-mapped deployment recordings.
#' files <- list.files(
#'   "./data interim/04_oriented", pattern = "\\.rds$", full.names = TRUE)
#' calibrated <- calibrateMagnetometer(
#'   files, group.by = "package_id", plot.file = "magnetometer_calibration.pdf")
#' processed <- processTagData(calibrated)
#'
#' # A separate rotation recording is matched using stored tag identifiers.
#' calibrated <- calibrateMagnetometer(
#'   files,
#'   calibration.data = "rotation_recording.rds",
#'   calibration.match = c("package_id", "logger_id"),
#'   calibration.on.missing = "error",
#'   calibration.context = "in_water")
#' }
#' @export
calibrateMagnetometer <- function(data,
                                  control = magCalibrationControl(),
                                  group.by = "package_id",
                                  calibration.data = NULL,
                                  calibration.match = c("package_id", "logger_id"),
                                  calibration.on.missing = c("fallback", "error", "skip"),
                                  calibration.context = NULL,
                                  return.data = TRUE,
                                  output.dir = NULL,
                                  output.suffix = NULL,
                                  compress = TRUE,
                                  plot = FALSE,
                                  plot.file = NULL,
                                  force.plots = FALSE,
                                  id.col = "ID",
                                  datetime.col = "datetime",
                                  verbose = "detailed") {

  lvl <- .verbosity(verbose)
  control <- .as_control(control, magCalibrationControl, "nautilus_mag_calibration", "control")
  .assert_flag(return.data, "return.data")
  .assert_flag(plot, "plot"); .assert_flag(force.plots, "force.plots")
  .assert_writable_file(plot.file, "plot.file", ext = "pdf")
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  group.by <- .validateGroupBy(group.by); .assert_string(output.suffix, "output.suffix", null_ok = TRUE)
  .assert_dir(output.dir, "output.dir"); .assert_compress(compress)
  .assert_output(return.data, output.dir)
  # external calibration-source options (all inert when calibration.data is NULL)
  calibration.on.missing <- match.arg(calibration.on.missing)
  valid_keys <- c("package_id", "logger_id")
  bad_keys <- setdiff(calibration.match, valid_keys)
  if (length(bad_keys))
    .abort(c("{.arg calibration.match} has invalid key{?s} {.val {bad_keys}}.", "i" = "Valid keys: {.val {valid_keys}}."))
  if (!length(calibration.match)) .abort("{.arg calibration.match} must name at least one key.")
  if (!is.null(calibration.context) && (!is.character(calibration.context) || !length(calibration.context)))
    .abort("{.arg calibration.context} must be a non-empty character vector (or NULL).")

  start.time <- Sys.time()
  r <- .resolveInput(data, id.col = id.col)
  # ingest external calibration recordings, if any (each is matched to a deployment below)
  sources <- if (!is.null(calibration.data))
    .ingestCalSources(calibration.data, calibration.match, datetime.col, id.col, calibration.context) else NULL
  results <- if (return.data) vector("list", r$n) else NULL
  saved <- vector("list", r$n)
  make_plots <- plot || !is.null(plot.file)
  summary_records <- if (make_plots) vector("list", r$n) else NULL   # one report-table row per deployment
  payloads <- list()                                                 # a small detail-page payload per flagged tag

  hdr <- sprintf("Input: %d dataset%s", r$n, if (r$n != 1) "s" else "")
  if (!is.null(output.dir)) hdr <- c(hdr, paste0("Output: ", output.dir))
  .log_header(lvl, "calibrateMagnetometer", "Estimating the magnetometer hard/soft-iron calibration",
              bullets = hdr, arrow = sprintf("method: %s%s, pooled by: %s", control$method,
              if (control$igrf.normalize) " (IGRF-normalised)" else "", paste(group.by, collapse = " x ")))

  # ---- Pass 1: gather a decimated cloud + gravity + metadata per deployment (bounded memory) ----
  info <- vector("list", r$n)
  for (i in seq_len(r$n)) {
    x <- r$get(i)
    if (!data.table::is.data.table(x)) x <- data.table::as.data.table(x)
    meta <- .getMeta(x)
    true_id <- tryCatch(as.character(unique(x[[id.col]])[1]), error = function(e) NA_character_)
    who <- if (!is.na(true_id)) true_id else r$ids[i]
    paddle <- isTRUE(meta$tag$paddle_wheel)
    gathered <- .magGather(x, datetime.col, paddle = paddle)
    keyval <- function(k) { v <- meta$tag[[k]]; if (is.null(v) || length(v) != 1 || is.na(v) || !nzchar(as.character(v))) NA_character_ else as.character(v) }
    key  <- .compositeGroupKey(group.by, function(f) meta$tag[[f]])   # composite key over one or more group.by fields
    info[[i]] <- list(id = who, key = if (is.na(key) || !nzchar(key)) NA_character_ else key, paddle = paddle,
                      cloud = gathered, igrf = if (!is.null(gathered)) .magIGRF(meta$deployment) else NULL,
                      keys = stats::setNames(lapply(c("package_id", "logger_id"), keyval), c("package_id", "logger_id")),
                      net = meta$axis_mapping$net)
    rm(x)
  }

  # ---- group: deployments sharing a non-blank key are pooled; blanks are their own group. A magnetic
  # paddle wheel is part of the ferromagnetic environment (and its de-noised cloud differs), so paddle
  # and non-paddle deployments of one package are NOT pooled together. ----
  has_mag <- vapply(info, function(z) !is.null(z$cloud), logical(1))
  grp_key <- vapply(seq_len(r$n), function(i)
    if (!has_mag[i]) paste0("__none__", i)
    else if (is.na(info[[i]]$key)) paste0("__tag__", info[[i]]$id, "__", i)
    else paste0("pkg:", info[[i]]$key, "|paddle:", info[[i]]$paddle), character(1))
  groups <- split(seq_len(r$n), grp_key)

  # ---- Pass 2: fit per group (default in-situ) OR from external calibration sources; write back below ----
  cals <- vector("list", r$n)              # per-deployment filled $mag_calibration
  n_cal <- 0L; n_high <- 0L; n_medium <- 0L; n_low <- 0L; n_nomag <- 0L
  src_skipped <- character(0)              # deployments where an external-source bind was not usable (reason)
  src_nogain  <- character(0)              # bound deployments whose source did not out-cover the in-situ cloud
  if (is.null(sources)) for (g in groups) {
    members <- g[has_mag[g]]
    if (!length(members)) next                              # no usable magnetometer in the group
    pooled <- length(members) > 1L
    cd <- lapply(members, function(i) info[[i]]$cloud)

    if (!pooled) {
      i <- members[[1]]
      ig <- info[[i]]$igrf
      tgt <- if (!is.null(control$target.field)) control$target.field
             else if (control$igrf.normalize && is.finite(ig$intensity)) ig$intensity else NA_real_
      cal <- .calFromControl(cd[[1]]$mag, control, target.radius = tgt, grav = cd[[1]]$grav, igrf.incl = ig$inclination)
      if (is.null(cal) || !isTRUE(cal$recommend_apply)) next   # abort gate (blob / 1-D arc): propose nothing
      cals[[i]] <- .fillMagCal(cal, source = "per_tag", group = info[[i]]$id, n = 1L)
    } else {
      # pool the per-deployment clouds, each centred + normalised to unit radius (common scale), fit the
      # shared soft-iron SHAPE, then compose a per-deployment (centre, scale) calibration from it.
      pre <- lapply(cd, function(z) {
        hi <- .hardIronOffset(z$mag[, 1], z$mag[, 2], z$mag[, 3], min.coverage = 0)
        if (is.null(hi) || !is.finite(hi$radius) || hi$radius <= 0) return(NULL)
        list(c = hi$offset, rad = hi$radius, N = sweep(z$mag, 2, hi$offset, "-") / hi$radius)
      })
      keep <- !vapply(pre, is.null, logical(1))
      if (sum(keep) < 2L) {                                    # not enough usable members -> per-tag each
        for (i in members) {
          ig <- info[[i]]$igrf
          tgt <- if (!is.null(control$target.field)) control$target.field
                 else if (control$igrf.normalize && is.finite(ig$intensity)) ig$intensity else NA_real_
          cal <- .calFromControl(info[[i]]$cloud$mag, control, target.radius = tgt, grav = info[[i]]$cloud$grav, igrf.incl = ig$inclination)
          if (!is.null(cal) && isTRUE(cal$recommend_apply)) cals[[i]] <- .fillMagCal(cal, source = "per_tag", group = info[[i]]$id, n = 1L)
        }
        next
      }
      members_k <- members[keep]; pre <- pre[keep]
      P <- do.call(rbind, lapply(pre, function(p) p$N))
      shape <- .calFromControl(P, control, target.radius = 1)   # unit sphere; QC on the pool
      if (is.null(shape)) next                               # pooled fit failed; members left uncalibrated
      grp_name <- info[[members_k[[1]]]]$key %||% info[[members_k[[1]]]]$id
      for (j in seq_along(members_k)) {
        i <- members_k[[j]]; p <- pre[[j]]; ig <- info[[i]]$igrf
        R <- if (!is.null(control$target.field)) control$target.field
             else if (control$igrf.normalize && is.finite(ig$intensity)) ig$intensity else p$rad
        center_i <- p$c + as.numeric(shape$center) * p$rad
        soft_i   <- shape$soft_iron * (R / p$rad)
        Mc <- .applyMagCal(info[[i]]$cloud$mag, center_i, soft_i)
        dipr <- .magDipResidual(Mc, info[[i]]$cloud$grav, ig$inclination)
        conf <- .magConfidence(shape$radcv, dipr$residual, shape$coverage_ok, control$radcv.max, control$igrf.residual.max,
                               status = shape$status, azimuth_span = shape$azimuth_span, azimuth.min = control$azimuth.min %||% 150)
        cals[[i]] <- .fillMagCal(list(center = center_i, soft_iron = soft_i, method_used = shape$method_used,
                                      coverage_ok = shape$coverage_ok, axis_span = shape$axis_span,
                                      radcv = shape$radcv, dip = dipr$dip, igrf_residual = dipr$residual,
                                      status = shape$status, perp_source = "data", confidence = conf),
                                 source = "per_package", group = grp_name, n = length(members_k))
      }
    }
  } else for (i in seq_len(r$n)) {
    # ---- external calibration sources: fit each deployment from its matched source(s). Targets are NOT
    # pooled with each other here - the source already supplies the coverage that target-pooling
    # compensated for; sources sharing a package are pooled inside .fitFromSource. ----
    if (!has_mag[i]) next
    matched <- .matchCalSource(info[[i]], sources, calibration.match)
    bind <- NULL; reason <- NULL
    if (!is.null(matched)) {
      res <- .fitFromSource(info[[i]], matched, control)
      if (isTRUE(res$reject)) reason <- res$reason
      else {
        bind <- .fillMagCal(res$cal, source = "external",
                            group = info[[i]]$keys[[matched$key]] %||% info[[i]]$id,
                            n = res$n, provenance = res$provenance)
        if (isTRUE(res$low_coverage_gain)) src_nogain <- c(src_nogain, info[[i]]$id)
      }
    } else reason <- "no matching calibration recording"
    if (!is.null(bind)) { cals[[i]] <- bind; next }

    # no usable source bind -> honour calibration.on.missing
    src_skipped <- c(src_skipped, sprintf("%s: %s", info[[i]]$id, reason))
    if (identical(calibration.on.missing, "error"))
      .abort(c("No usable external calibration for deployment {.val {info[[i]]$id}}: {reason}.",
               "i" = "Adjust {.arg calibration.data}, or set {.arg calibration.on.missing} to {.val fallback}/{.val skip}."))
    if (identical(calibration.on.missing, "skip")) next
    # fallback: the current in-situ behaviour (fit from the deployment's own cloud)
    ig  <- info[[i]]$igrf
    tgt <- if (!is.null(control$target.field)) control$target.field
           else if (control$igrf.normalize && is.finite(ig$intensity)) ig$intensity else NA_real_
    cal <- .calFromControl(info[[i]]$cloud$mag, control, target.radius = tgt, grav = info[[i]]$cloud$grav, igrf.incl = ig$inclination)
    if (!is.null(cal) && isTRUE(cal$recommend_apply)) cals[[i]] <- .fillMagCal(cal, source = "per_tag", group = info[[i]]$id, n = 1L)
  }

  # ---- write back + report ----
  for (i in seq_len(r$n)) {
    x <- r$get(i, writable = TRUE)
    meta <- .getMeta(x)
    who <- info[[i]]$id
    .log_h2(lvl, sprintf("%s (%d/%d)", who, i, r$n))
    cal <- cals[[i]]
    if (is.null(cal)) {
      n_nomag <- n_nomag + 1L
      .log_skip(lvl, if (!has_mag[i]) "no usable magnetometer" else "calibration not estimated")
    } else {
      cal$axis_net <- meta$axis_mapping$net                 # stamp the frame this estimate was fit in
      meta$mag_calibration <- .packProposed(cal)            # store as the nested `proposed` block (applier fills the rest)
      meta <- .appendProcessing(meta, "calibrateMagnetometer", source = cal$source, group = cal$group,
                                method = cal$method, radcv = cal$radcv, igrf_residual = cal$igrf_residual,
                                confidence = cal$confidence, n_deployments = cal$n_deployments)
      x <- .restoreMeta(x, meta)
      n_cal <- n_cal + 1L
      if (identical(cal$confidence, "high"))   n_high   <- n_high + 1L
      if (identical(cal$confidence, "medium")) n_medium <- n_medium + 1L
      if (identical(cal$confidence, "low"))    n_low    <- n_low + 1L
      resid_txt <- if (is.finite(cal$igrf_residual)) sprintf(", IGRF dip resid %+.0f\u00b0", cal$igrf_residual) else ""
      .log_detail(lvl, sprintf("%s %s", cal$source, cal$method))                    # method on its own line
      .log_detail(lvl, sprintf("radCV %.2f%s \u00b7 confidence %s",                  # quality metrics beneath it
                               cal$radcv, resid_txt, cal$confidence))
    }
    if (make_plots) {                                                # deferred-draw payload/record
      cov_i <- if (!is.null(cal) && !is.null(info[[i]]$cloud))    # computed ONCE on the full cloud, then
        .magCalCoverage(sweep(info[[i]]$cloud$mag, 2, cal$center, "-")) else NA_real_   # shared by both
      summary_records[[i]] <- .magCalSummaryRecord(who, cal, cov_i)
      detail <- !is.null(cal) && (force.plots || r$n == 1L || cal$confidence %in% c("low", "medium"))
      if (detail)
        payloads[[length(payloads) + 1L]] <- .magCalPayload(who, info[[i]]$cloud, cal, info[[i]]$igrf,
                                                            info[[i]]$paddle, coverage = cov_i,
                                                            radcv.max = control$radcv.max, dip.max = control$igrf.residual.max)
    }
    saved[i] <- list(.saveOutput(x, r$ids[i], output.dir = output.dir,
                                 output.suffix = output.suffix, compress = compress))
    if (!is.null(saved[[i]])) .log_ok(lvl, paste0("saved ", basename(saved[[i]])))
    if (return.data) results[[i]] <- x
    .log_gap(lvl); rm(x)
  }

  # ---- diagnostic report: worst-first summary page, then a detail page per flagged deployment ----
  if (make_plots) {
    stats <- list(n = r$n, n_cal = n_cal, n_high = n_high, n_medium = n_medium, n_low = n_low, n_nomag = n_nomag)
    draw <- function(to.file = FALSE, unicode = TRUE) {
      .drawMagCalSummaryPage(Filter(Negate(is.null), summary_records), stats, use_unicode = unicode)
      for (pl in payloads) .plotMagCalIndividual(pl, use_unicode = unicode)
    }
    # base pdf() for the file (ASCII glyphs, so it stays portable and parseable by the page-count tests);
    # `unicode` is driven by the device, so the caller's screen still gets Unicode when it can render it.
    .renderToDevices(draw, plot = plot, plot.file = plot.file, width = 10, height = 7.5)
  }

  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, n_cal, " of ", r$n, " dataset", if (r$n != 1) "s", " calibrated")
    if (n_high > 0L)  .log_arrow(lvl, "high heading confidence: ", n_high)
    if (n_low > 0L)   .log_arrow(lvl, "low confidence (untrusted heading): ", n_low)
    if (n_nomag > 0L) .log_arrow(lvl, "no usable magnetometer: ", n_nomag)
    if (length(src_skipped)) {
      verb <- if (identical(calibration.on.missing, "skip")) "left uncalibrated" else "fell back to in-situ"
      .log_arrow(lvl, sprintf("external calibration not bound (%s): %d", verb, length(src_skipped)))
      if (lvl >= 2L) for (s in src_skipped) .log_skip(lvl, s)
    }
    if (length(src_nogain))
      .log_arrow(lvl, sprintf("external source no better-covered than in-situ (applied, flagged): %s",
                              paste(src_nogain, collapse = ", ")))
    if (!is.null(output.dir)) .log_arrow(lvl, "output: ", output.dir)
    .log_runtime(lvl, start.time)
  }
  .collectOutput(results, saved, return.data, r$ids)
}
