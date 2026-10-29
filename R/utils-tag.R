#######################################################################################################
# The nautilus_tag S3 class ###########################################################################
#######################################################################################################
#
# A `nautilus_tag` is a data.table subclass carrying a SINGLE consolidated metadata object in the
# `"nautilus"` attribute (instead of ~20 scattered attributes). This keeps direct column access and all
# data.table (and, for the user, dplyr) verbs working, while making provenance robust to manage: save/restore is a
# one-liner (.restoreMeta), and the object gains print/summary/plot methods.
#
# Metadata schema (attr(x, "nautilus")):
#   $ id           character deployment ID
#   $ animal_id    character animal ID (optional; distinct from the deployment)
#   $ deployment   list(lon, lat, datetime, recovery_datetime, popup_lon, popup_lat, popup_datetime,
#                       magnetic_declination, attachment_site)
#   $ tag          list(model, type, package_id, logger_id, paddle_wheel, axis_config)
#   $ biometrics   list of passive animal traits (sex, length, species, ...); imported via metadataColumns(traits=)
#   $ sensors      list(present, excluded, sampling_hz_original, sampling_hz_processed, timezone,
#                       recording_utc_offset, clock). `timezone` is the display zone; `clock` separately
#                  records source interpretation. NULL clock provenance means unknown, including legacy tags.
#   $ span         list(first_datetime, last_datetime, original_rows)
#   $ sidecar      parsed device, logging and calibration metadata paired with the primary export, or
#                  NULL. Calibration values are provenance for firmware corrections already present in
#                  the CSV and are never re-applied by nautilus.
#   $ axis_mapping structured list (see .newAxisMappingMeta): applied flag, net signed-perm per
#                  sensor family, determinant (handedness), dropped axes, source, original from/to rows
#   $ mag_calibration nested list (see .newMagCalibrationMeta): status, applied, applied_params
#                  (center/soft_iron/axis_net), proposed estimate, qc (confidence + metrics), provenance
#   $ processing   append-only list of step records (audit trail)
#   $ user         optional named scalar annotations, added by updateTagMetadata()


# Null-coalescing helper (base `%||%` only exists from R 4.4; define our own for older R)
`%||%` <- function(a, b) if (is.null(a)) b else a


#' Create an empty nautilus metadata object
#' @keywords internal
#' @noRd

.newNautilusMeta <- function() {
  list(
    id           = NA_character_,
    animal_id    = NA_character_,                          # the ANIMAL; `id` is the DEPLOYMENT
    deployment   = list(lon = NA_real_, lat = NA_real_, datetime = as.POSIXct(NA),
                        recovery_datetime = as.POSIXct(NA),
                        site = NA_character_,
                        popup_lon = NA_real_, popup_lat = NA_real_, popup_datetime = as.POSIXct(NA),
                        magnetic_declination = NA_real_, heading_reference = NA_character_,
                        attachment_site = NA_character_),
    tag          = list(model = NA_character_, type = NA_character_, package_id = NA, logger_id = NA_character_,
                        paddle_wheel = NA, axis_config = NA_character_),
    biometrics   = list(),                                 # passive animal traits (sex, length, species, ...); roles ACT, traits RIDE
    sensors      = list(present = character(0), excluded = character(0), sampling_hz_original = NA_real_,
                        sampling_hz_processed = NA_real_, timezone = "UTC",
                        recording_utc_offset = NA_real_, clock = NULL),
    span         = list(first_datetime = as.POSIXct(NA), last_datetime = as.POSIXct(NA),
                        original_rows = NA_integer_),
    sidecar      = NULL,
    ancillary    = list(),
    axis_mapping = .newAxisMappingMeta(),
    mag_calibration = .newMagCalibrationMeta(),
    processing   = list()
  )
}


#' Create an empty axis-mapping metadata object
#'
#' Tracks the signed-permutation transform currently applied to the IMU axes (chip -> canonical tag
#' frame), so re-mapping composes exactly and never double-applies. `net`/`determinant` are named per
#' sensor family ("accel"/"gyro"/"mag") for the families that form a clean signed permutation; a
#' family with dropped (NA) axes has no matrix form and is recorded only via `from_to`/`dropped`.
#' @keywords internal
#' @noRd

.newAxisMappingMeta <- function() {
  list(
    applied     = FALSE,
    source      = NA_character_,   # producer: "importTagData" | "checkTagMapping" | "consensusAxisMapping" | "manual"
    provenance  = NULL,            # per-family origin, named chr: "self" | "consensus" | "manual" (or NULL)
    from_to     = NULL,           # original from/to rows (provenance), or NULL
    net         = NULL,           # named list of 3x3 signed-permutation matrices, per family
    determinant = NULL,           # named integer vector (+1/-1) per family
    dropped     = character(0),   # axes set to NA (faulty sensors)
    coreg_corr  = NA_real_,       # accel<->gyro co-registration correlation (frame-level handedness check); NA if not computed
    coreg_frac  = NA_real_        # fraction of high-rotation samples used for coreg_corr
  )
}


#' Create an empty magnetometer-calibration metadata object
#'
#' The single source of truth for a deployment's magnetometer calibration state - the formal contract
#' between the estimator (\code{calibrateMagnetometer}), the applier (\code{processTagData}) and the
#' downstream trajectory functions (\code{reconstructTrack} et al.). It deliberately separates what was
#' PROPOSED (an estimate + its QC) from what was APPLIED to the raw `mx/my/mz`, because the two can differ:
#' \code{processTagData} may reject a stored fit (wrong frame, low confidence) and apply its own.
#'
#' Structure:
#' \itemize{
#'   \item \code{status} - application-outcome enum the downstream branches on. One of: \code{"unprocessed"}
#'     (default; \code{processTagData} has not run), \code{"no_magnetometer"}, \code{"not_requested"}
#'     (correction switched off by the user), \code{"uncalibrated_raw"} (correction requested but NONE could
#'     be applied - the heading is off a raw field), \code{"calibrated_diagonal"} (per-axis only),
#'     \code{"calibrated_2d_fallback"} (hard-iron centre only, with an IGRF-pinned perpendicular and an
#'     identity soft-iron - the usual outcome for a thin swimming-band cloud), and
#'     \code{"calibrated_3d"} (a full hard + soft-iron correction).
#'   \item \code{applied} - reliable boolean: has a correction actually been applied to `mx/my/mz`? Also
#'     the idempotency guard that stops a re-run of \code{processTagData} double-correcting.
#'   \item \code{applied_params} - the exact \code{center} / \code{soft_iron} / \code{axis_net}
#'     \code{processTagData} USED (identity when nothing was applied).
#'   \item \code{proposed} - the estimate written by \code{calibrateMagnetometer} (\code{params} + \code{qc}
#'     + \code{provenance}), or NULL. Immutable once written; never overwritten by the applier.
#'   \item \code{qc} - QC of the APPLIED fit (may be sparser than \code{proposed$qc} for an inline fit).
#'   \item \code{provenance} - how the applied fit was produced (\code{method}, \code{source},
#'     \code{perp_source}).
#' }
#' @keywords internal
#' @noRd
.newMagCalStateBlock <- function() {
  list(params     = list(center = rep(NA_real_, 3), soft_iron = NULL, axis_net = NULL),
       qc         = list(confidence = NA_character_, coverage_ok = NA, radcv = NA_real_,
                         igrf_residual = NA_real_, axis_span = rep(NA_real_, 3)),
       provenance = list(method = NA_character_, source = NA_character_, group = NA_character_,
                         n_deployments = NA_integer_, source_ids = NULL, context = NA_character_,
                         center_delta = NA_real_, center_source = NA_character_))
}


.newMagCalibrationMeta <- function() {
  list(
    status         = "unprocessed",
    applied        = FALSE,
    applied_params = list(center = rep(NA_real_, 3), soft_iron = NULL, axis_net = NULL),
    proposed       = NULL,                                   # a state-block (params/qc/provenance) or NULL
    qc             = list(confidence = NA_character_, coverage_ok = NA, radcv = NA_real_,
                          igrf_residual = NA_real_, axis_span = rep(NA_real_, 3)),
    provenance     = list(method = NA_character_, source = NA_character_, perp_source = NA_character_)
  )
}


#' The reference frame of the stored `heading` column: "geographic" or "magnetic".
#'
#' Calibration quality and reference frame are INDEPENDENT properties of a heading: a perfectly
#' calibrated magnetometer still reads magnetic north, and converting to true north needs the magnetic
#' declination at the deployment position. `.headingTrust()` answers "how well was the magnetometer
#' calibrated"; this answers "north relative to what", and neither implies the other.
#'
#' The distinction matters because a magnetic heading differs from a geographic one by a CONSTANT offset
#' (-7.6 degrees in the Azores, but over 25 degrees of either sign elsewhere). Analyses built on angle
#' DIFFERENCES - turning rate, angular velocity, circular variance - are unaffected, because the offset
#' cancels. Analyses making an ABSOLUTE claim about direction - dead reckoning, comparison with GPS
#' fixes, a circular mean heading - are rotated by it, systematically and without averaging out.
#'
#' Returns "unknown" for a tag processed before this field existed, so callers can tell "not recorded"
#' apart from "recorded as magnetic" and decline to guess.
#' @return "geographic" | "magnetic" | "unknown".
#' @keywords internal
#' @noRd
.headingReference <- function(meta) {
  v <- meta$deployment$heading_reference
  if (is.null(v) || length(v) != 1L || is.na(v)) return("unknown")
  as.character(v)
}


#' Coarse heading-trust level derived from the calibration status - the shared branch for downstream
#' trajectory functions (so each does not re-implement the mapping).
#' @return "trusted" | "partial" | "untrusted" | "none" | "not_applicable" | "unknown".
#' @keywords internal
#' @noRd
.headingTrust <- function(meta) {
  s <- (meta$mag_calibration$status %||% "unprocessed")
  switch(s,
         calibrated_3d          = "trusted",
         calibrated_2d_fallback = "partial",
         calibrated_diagonal    = "partial",
         uncalibrated_raw       = "untrusted",
         no_magnetometer        = "none",
         not_requested          = "not_applicable",
         "unknown")                                          # "unprocessed" / anything unexpected
}


#' Construct a nautilus_tag (in place, no copy)
#'
#' @param x A data.table (will be classed in place).
#' @param meta A nautilus metadata list (see schema above).
#' @return `x`, classed as `nautilus_tag`.
#' @keywords internal
#' @noRd

new_nautilus_tag <- function(x, meta = .newNautilusMeta()) {
  if (!data.table::is.data.table(x)) data.table::setDT(x)
  data.table::setattr(x, "nautilus", meta)
  data.table::setattr(x, "class", c("nautilus_tag", "data.table", "data.frame"))
  x
}


#' Is `x` a nautilus_tag?
#' @keywords internal
#' @noRd
is_nautilus_tag <- function(x) inherits(x, "nautilus_tag")


#' Get / set the consolidated metadata object
#' @keywords internal
#' @noRd
.getMeta <- function(x) attr(x, "nautilus", exact = TRUE)  # exact: avoid matching "nautilus.version"


#' The canonical positions table for a nautilus_tag (from `meta$ancillary$positions`).
#'
#' Single source of truth for the tag's position fixes - the complete Wildlife Computers record, kept
#' at its own cadence (never snapped to sensor rows or trimmed to the deployment window). Returns an
#' empty, typed data.frame when the tag carries no positions, so callers treat "no fixes" and "some
#' fixes" uniformly.
#' @keywords internal
#' @noRd
.tagPositions <- function(x) {
  p <- .getMeta(x)$ancillary$positions$data
  if (is.null(p) || !nrow(p))
    return(data.frame(datetime = .POSIXct(numeric(0), "UTC"), type = character(0),
                      lon = numeric(0), lat = numeric(0), quality = character(0), stringsAsFactors = FALSE))
  p
}


#' Transient reconstruction of the legacy sparse position columns from the canonical record.
#'
#' Compatibility shim for consumers not yet migrated to read `meta$ancillary$positions` directly:
#' returns a COPY of the tag with `position_type`/`lon`/`lat`/`quality` columns rebuilt on demand (WC
#' fixes snapped to the nearest in-window sensor row, plus the metadata deploy/pop-up positions from
#' `meta$deployment`). Nothing is stored - `meta$ancillary$positions` remains the single source of truth.
#' @keywords internal
#' @noRd
.withPositionColumns <- function(x, datetime.col = "datetime") {
  if ("position_type" %in% names(x)) return(data.table::copy(x))   # already has position columns - leave untouched
  x <- data.table::copy(x)
  if (!data.table::is.data.table(x)) data.table::setDT(x)
  x[, `:=`(PTT = NA_character_, position_type = NA_character_, lat = NA_real_, lon = NA_real_, quality = NA_character_)]
  if (!nrow(x) || !datetime.col %in% names(x)) return(x)
  tn <- as.numeric(x[[datetime.col]]); lo <- min(tn, na.rm = TRUE); hi <- max(tn, na.rm = TRUE)
  snap <- function(ft) if (is.na(ft) || ft < lo || ft > hi) NA_integer_ else which.min(abs(tn - ft))
  pos <- .tagPositions(x)                                          # WC fixes -> nearest in-window row
  for (k in seq_len(nrow(pos))) {
    j <- snap(as.numeric(pos$datetime[k])); if (is.na(j)) next
    data.table::set(x, i = j, j = c("position_type", "lat", "lon", "quality"),
                    value = list(pos$type[k], pos$lat[k], pos$lon[k], pos$quality[k]))
  }
  dep <- .getMeta(x)$deployment                                    # deploy/pop-up (canonical in meta$deployment)
  add_meta <- function(dtm, lat, lon, label) {
    if (is.null(dtm) || is.na(dtm) || is.null(lat) || is.na(lat)) return(invisible())
    j <- snap(as.numeric(dtm)); if (is.na(j)) return(invisible())
    data.table::set(x, i = j, j = c("position_type", "lat", "lon"), value = list(label, lat, lon))
  }
  if (!is.null(dep)) {
    add_meta(dep$datetime,       dep$lat,       dep$lon,       "Metadata [deploy]")
    add_meta(dep$popup_datetime, dep$popup_lat, dep$popup_lon, "Metadata [popup]")
  }
  x
}


#' @keywords internal
#' @noRd
.setMeta <- function(x, meta) {
  data.table::setattr(x, "nautilus", meta)
  invisible(x)
}


#' Re-attach metadata and the nautilus_tag class to a (possibly new) data object
#'
#' Use after operations that may strip the class/attribute (rbindlist, aggregate, `[`, merge).
#' @keywords internal
#' @noRd

.restoreMeta <- function(new, meta) {
  if (!data.table::is.data.table(new)) data.table::setDT(new)
  data.table::setattr(new, "nautilus", meta)
  data.table::setattr(new, "class", c("nautilus_tag", "data.table", "data.frame"))
  new
}


#' Append a processing-step record to the metadata audit trail
#'
#' @param meta A nautilus metadata list.
#' @param step Character name of the processing step (function).
#' @param ... Named parameters/values to record for provenance.
#' @return The updated metadata list.
#' @keywords internal
#' @noRd

.appendProcessing <- function(meta, step, ...) {
  # Provenance routinely records text that came off disk verbatim - CSV headers, directory names, format
  # labels - and tag loggers write those in a single-byte encoding (CATS emits latin1 for "[m/s\u00b2]",
  # "[\u00b5T]", "[\u00b0C]"). Readers hand them back with NO declared encoding, so their meaning depends on
  # the session's locale: a tag saved from a UTF-8 session and read anywhere else warns once per string,
  # or renders as mojibake. Normalise on the way IN, so what is persisted is unambiguous.
  # This is safe precisely because provenance is a record, never a key - nothing is ever matched against
  # it. The raw bytes must survive elsewhere: `colname_in_csv` still selects columns out of the CSV, and
  # re-encoding THAT would silently drop the sensor channels it names.
  rec <- c(list(step = step,
                nautilus_version = tryCatch(as.character(utils::packageVersion("nautilus")),
                                            error = function(e) NA_character_),
                time = Sys.time()),
           rapply(list(...), .toUTF8, classes = "character", how = "replace"))
  meta$processing <- c(meta$processing %||% list(), list(rec))
  meta
}


#' Build a metadata object from the legacy flat attributes (backward compatibility)
#'
#' Old `.rds` files (and the current importTagData output, pre-migration) store metadata as many
#' separate attributes. This maps them into the consolidated schema so previously-saved data keeps
#' working.
#' @keywords internal
#' @noRd

.metaFromFlatAttrs <- function(x) {
  a <- attributes(x)
  meta <- .newNautilusMeta()
  meta$id <- a$id %||% (if (!is.null(x[["ID"]])) as.character(unique(x[["ID"]])[1]) else NA_character_)
  di <- a$deployment.info
  if (!is.null(di)) {
    meta$deployment$lon <- di$lon %||% NA_real_
    meta$deployment$lat <- di$lat %||% NA_real_
    meta$deployment$datetime <- di$datetime %||% as.POSIXct(NA)
  }
  meta$deployment$magnetic_declination <- a$magnetic.declination %||% NA_real_
  meta$deployment$attachment_site       <- a$attachment.site %||% NA_character_
  meta$tag$model       <- a$tag.model %||% NA_character_
  meta$tag$type        <- a$tag.type %||% NA_character_
  meta$tag$package_id  <- a$package.id %||% NA
  meta$tag$paddle_wheel <- a$paddle.wheel %||% NA
  meta$sensors$sampling_hz_original  <- a$original.sampling.frequency %||% NA_real_
  meta$sensors$sampling_hz_processed <- a$processed.sampling.frequency %||% NA_real_
  meta$sensors$timezone <- a$timezone %||% "UTC"
  meta$span$first_datetime <- a$first.datetime %||% as.POSIXct(NA)
  meta$span$last_datetime  <- a$last.datetime %||% as.POSIXct(NA)
  meta$span$original_rows  <- a$original.rows %||% NA_integer_
  meta$sidecar      <- a$sidecar %||% a$calibration
  meta$axis_mapping <- .normalizeAxisMappingMeta(a$axis.mapping, source = "importTagData")
  meta
}


#' Upgrade a legacy / raw axis-mapping value to the structured axis-mapping metadata object
#'
#' Accepts the new structured list (returned unchanged), the historical `from/to` data.frame, `NA`, or
#' `NULL`, and returns a structured object (see `.newAxisMappingMeta()`). When the value is a data.frame
#' of mapping rows, the net signed-permutation matrices and handedness are derived per sensor family
#' (best effort: families that are not clean permutations are recorded via `from_to`/`dropped` only).
#' @keywords internal
#' @noRd

.normalizeAxisMappingMeta <- function(x, source = NA_character_) {
  # already structured
  if (is.list(x) && !is.data.frame(x) && !is.null(x$applied)) return(x)
  # absent / "no mapping"
  if (is.null(x) || (length(x) == 1 && is.na(x))) return(.newAxisMappingMeta())
  # historical from/to data.frame
  if (is.data.frame(x) && all(c("from", "to") %in% names(x)) && nrow(x) > 0) {
    return(.axisMappingMetaFromRows(x[, c("from", "to")], source = source, applied = TRUE))
  }
  .newAxisMappingMeta()
}


#' Build axis-mapping metadata (net signed-perm + handedness) from from/to rows
#'
#' @param from_to data.frame with `from`, `to`.
#' @param source Character provenance tag.
#' @param applied Logical: whether the rows have actually been applied to the data.
#' @keywords internal
#' @noRd

.axisMappingMetaFromRows <- function(from_to, source = NA_character_, applied = TRUE) {
  m <- .newAxisMappingMeta()
  m$applied <- isTRUE(applied)
  m$source  <- source
  m$from_to <- as.data.frame(from_to[, c("from", "to")], stringsAsFactors = FALSE)

  families <- list(accel = c("ax", "ay", "az"), gyro = c("gx", "gy", "gz"), mag = c("mx", "my", "mz"))
  net <- list(); det <- integer(0); dropped <- character(0)
  for (fam in names(families)) {
    axes <- families[[fam]]
    ft <- m$from_to[m$from_to$from %in% axes, , drop = FALSE]
    if (nrow(ft) == 0) next
    dropped <- c(dropped, ft$from[ft$to == "NA"])
    M <- .mappingToSignedPerm(ft, axes)         # NULL if not a clean signed permutation
    if (!is.null(M)) { net[[fam]] <- M; det[fam] <- .signedPermDet(M) }
  }
  m$net         <- if (length(net)) net else NULL
  m$determinant <- if (length(det)) det else NULL
  m$dropped     <- unique(dropped)
  m
}


#' Ensure `x` carries a nautilus metadata object (migrating legacy attributes if needed)
#'
#' Pipeline functions call this on input so they can rely on `attr(x, "nautilus")`, regardless of
#' whether the data came from a freshly-migrated importTagData or an older saved file.
#' @keywords internal
#' @noRd

.ensureMeta <- function(x) {
  if (!data.table::is.data.table(x)) data.table::setDT(x)
  if (is.null(attr(x, "nautilus", exact = TRUE))) data.table::setattr(x, "nautilus", .metaFromFlatAttrs(x))
  x
}


#######################################################################################################
# S3 methods ##########################################################################################
#######################################################################################################

#' Print a nautilus_tag
#'
#' Shows a compact provenance card (ID, time span, sampling rate, sensors present, processing
#' history) followed by the first few rows, rather than dumping the entire high-resolution table.
#'
#' @param x A `nautilus_tag` object.
#' @param ... Passed to the data.table print method for the row preview.
#' @return `x`, invisibly.
#' @exportS3Method print nautilus_tag

print.nautilus_tag <- function(x, ...) {
  m <- .getMeta(x)
  hl <- function(s) cat(cli::style_bold(s), "\n")
  hl(sprintf("<nautilus_tag> %s", m$id %||% "?"))
  span <- m$span
  # NULL-safe: a partial-meta tag (e.g. one carrying only a deployment block) has no span; span$field is
  # then NULL and is.na(NULL) is logical(0), which would abort `if()` -- isTRUE collapses that to FALSE.
  if (!is.null(span) && isTRUE(!is.na(span$first_datetime)) && isTRUE(!is.na(span$last_datetime))) {
    cat(sprintf("  span     : %s -> %s\n",
                format(span$first_datetime, "%Y-%m-%d %H:%M", tz = m$sensors$timezone),
                format(span$last_datetime,  "%Y-%m-%d %H:%M", tz = m$sensors$timezone)))
  }
  rate <- m$sensors$sampling_hz_processed
  if (is.null(rate) || is.na(rate)) rate <- m$sensors$sampling_hz_original
  if (!is.null(rate) && !is.na(rate)) cat(sprintf("  sampling : %s Hz\n", rate))
  present <- intersect(.sensorChannels(), names(x))
  if (length(present)) cat(sprintf("  sensors  : %s\n", paste(present, collapse = ", ")))
  cat(sprintf("  rows     : %s x %d cols\n", format(nrow(x), big.mark = ","), ncol(x)))
  steps <- vapply(m$processing %||% list(), function(p) p$step %||% "?", character(1))
  if (length(steps)) cat(sprintf("  history  : %s\n", paste(steps, collapse = " -> ")))
  mc <- m$mag_calibration
  if (!is.null(mc) && !identical(mc$status %||% "unprocessed", "unprocessed")) {
    conf  <- mc$qc$confidence %||% mc$proposed$qc$confidence               # applied QC, else the proposed estimate
    state <- if (isTRUE(mc$applied)) "applied" else if (!is.null(mc$proposed)) "proposed" else "not applied"
    cat(sprintf("  mag calib: %s - %s%s\n", mc$status, state,
                if (!is.null(conf) && !is.na(conf)) sprintf(" (%s confidence)", conf) else ""))
  }
  cat(cli::col_silver("  --- first rows ---\n"))
  print(utils::head(data.table::as.data.table(x), 5L), ...)
  invisible(x)
}


#' Summarise a deployment at a glance
#'
#' @description
#' A quick check on one deployment: how long it ran, at what rate, which sensor channels it carries and
#' the range of the main ones. Useful immediately after import, to confirm a tag read as expected before
#' committing to a long processing run.
#'
#' For a whole fleet in one table, use [summarizeTagData()].
#'
#' @param object A `nautilus_tag` object.
#' @param ... Unused.
#' @return A one-row data.frame of per-deployment summary statistics.
#' @exportS3Method summary nautilus_tag

summary.nautilus_tag <- function(object, ...) {
  .newSummary(.summarize(object), error.stat = "sd")
}


#######################################################################################################
#######################################################################################################
#######################################################################################################

# Tag construction and deployment-metadata assembly helpers.

# buildTagData helpers ----

#' Assemble the consolidated nautilus metadata shared by every ingestion path.
#'
#' The single place the `nautilus_tag` meta SCHEMA is populated, so the reader path
#' (\code{importTagData()} -> \code{read_cats()}) and the in-memory path (\code{buildTagData()}) cannot
#' drift apart as new per-format readers are added. \code{buildTagData()} is its public face.
#'
#' It deliberately does NOT append a processing record: each caller appends its own, so an imported
#' tag's audit trail names \code{importTagData} - the operation the user actually invoked - rather than
#' leaking this internal step. Callers add their own extras afterwards (source sidecar, excluded
#' channels, WC ancillary streams).
#' @param data The canonical frame (read for the channels present and the recorded span).
#' @param id Deployment ID.
#' @param metadata A flattened role-named metadata row (\code{metadataColumns()} roles), or NULL.
#' @param traits Names in `metadata` to carry through as passive biometric traits.
#' @param timezone Display zone, when the data have no declared zone.
#' @param clock Source-clock interpretation record supplied by the ingestion boundary, or NULL for
#'   unknown/legacy provenance. Never infer an old object's clock interpretation from its display zone.
#' @param sampling.hz Original sampling rate, or NULL to leave it NA (importTagData does not persist a
#'   rate - it is inferred from the timestamps downstream).
#' @return A nautilus metadata list, without a processing record.
#' @keywords internal
#' @noRd
.assembleTagMeta <- function(data, id, metadata = NULL, traits = NULL, timezone = "UTC",
                             sampling.hz = NULL, clock = NULL) {
  meta <- .newNautilusMeta()
  meta$id <- as.character(id)
  md <- .asMetadataRow(metadata)
  .resolveDeploymentId(id = id, metadata = md)
  if (!is.null(md)) md$id <- as.character(id)
  meta <- .applyDeploymentMetadata(meta, .normalizeMetadataRow(md, traits), traits)
  meta$sensors$present  <- intersect(.sensorChannels(), names(data))
  meta$sensors$timezone <- .declaredTimeZone(data$datetime)
  if (is.na(meta$sensors$timezone)) meta$sensors$timezone <- .timeZoneSpec(timezone)$display
  meta$sensors["clock"] <- list(clock)
  if (!is.null(sampling.hz)) meta$sensors$sampling_hz_original <- sampling.hz
  meta$span$first_datetime <- min(data$datetime)
  meta$span$last_datetime  <- max(data$datetime)
  meta$span$original_rows  <- nrow(data)
  meta$axis_mapping <- .newAxisMappingMeta()      # raw: no axis transform is applied at assembly
  meta
}

#' Coerce `metadata` (a 1-row data.frame / nautilus_deployments / named list) to a flat named list of
#' scalars, or NULL. Multi-row input is rejected (buildTagData builds one deployment).
#' @keywords internal
#' @noRd
.asMetadataRow <- function(metadata) {
  if (is.null(metadata) || (is.list(metadata) && !length(metadata))) return(NULL)
  if (is.data.frame(metadata)) {
    if (nrow(metadata) != 1L)
      .abort("{.arg metadata} must be a single deployment row (got {nrow(metadata)}).")
    metadata <- as.list(metadata)
  }
  nms <- names(metadata)
  if (!is.list(metadata) || is.null(nms) || anyNA(nms) || any(!nzchar(trimws(nms))) || anyDuplicated(nms))
    .abort("{.arg metadata} must be a single row or a named list with unique field names.")
  for (field in nms) {
    v <- metadata[[field]]
    if (!is.null(v) && (!is.atomic(v) || length(v) != 1L || !is.null(dim(v))))
      .abort("Metadata field {.val {field}} must be a scalar value; vectors and nested objects are not supported.")
  }
  metadata[!vapply(metadata, is.null, logical(1))]
}

#' Pull a single scalar field from a flattened metadata row (NULL if absent/NA).
#' @keywords internal
#' @noRd
.metaField <- function(md, name) {
  if (is.null(md) || !name %in% names(md)) return(NULL)
  v <- md[[name]]
  if (length(v) == 0L) return(NULL)
  if (length(v) == 1L && is.na(v)) return(NULL)
  v
}

#' Map metadataColumns()-role fields from a metadata row onto the nautilus meta schema.
#' @keywords internal
#' @noRd
.applyDeploymentMetadata <- function(meta, md, traits) {
  if (is.null(md)) return(meta)
  chr <- function(x) if (is.null(x)) NULL else as.character(x)
  num <- function(x) if (is.null(x)) NULL else .asNumericSafe(x)
  # the ANIMAL, which is not the deployment: one shark can carry several tags across a season, and
  # `id` identifies the deployment. Kept at the top level rather than under `deployment`, because it is
  # a property of the animal that outlives any one attachment.
  if (!is.null(v <- chr(.metaField(md, "animal_id"))))        meta$animal_id <- v
  # deployment block
  if (!is.null(v <- num(.metaField(md, "deploy_lon"))))       meta$deployment$lon <- v
  if (!is.null(v <- num(.metaField(md, "deploy_lat"))))       meta$deployment$lat <- v
  if (!is.null(v <- .metaField(md, "deploy_datetime")))       meta$deployment$datetime <- v
  if (!is.null(v <- .metaField(md, "recovery_datetime")))     meta$deployment$recovery_datetime <- v
  if (!is.null(v <- chr(.metaField(md, "deploy_site"))))      meta$deployment$site <- v
  if (!is.null(v <- num(.metaField(md, "popup_lon"))))        meta$deployment$popup_lon <- v
  if (!is.null(v <- num(.metaField(md, "popup_lat"))))        meta$deployment$popup_lat <- v
  if (!is.null(v <- .metaField(md, "popup_datetime")))        meta$deployment$popup_datetime <- v
  if (!is.null(v <- chr(.metaField(md, "attachment_site"))))  meta$deployment$attachment_site <- v
  if (!is.null(v <- chr(.metaField(md, "deployment_type"))))  meta$deployment$deployment_type <- v
  # tag block
  if (!is.null(v <- chr(.metaField(md, "tag_model"))))        meta$tag$model <- v
  if (!is.null(v <- chr(.metaField(md, "tag_type"))))         meta$tag$type <- v
  if (!is.null(v <- chr(.metaField(md, "package_id"))))       meta$tag$package_id <- v
  if (!is.null(v <- chr(.metaField(md, "logger_id"))))        meta$tag$logger_id <- v
  if (!is.null(v <- .metaField(md, "paddle_wheel")))          meta$tag$paddle_wheel <- v
  if (!is.null(v <- chr(.metaField(md, "axis_config"))))      meta$tag$axis_config <- v
  # passive traits -> biometrics (kept verbatim, factors flattened to character so a stored trait never
  # carries the table's other levels). Unlike the role fields above, a trait is recorded even when NA:
  # the key's presence is the record that the trait was mapped, which is what importTagData() has always
  # done - so both ingestion paths agree.
  for (tr in traits) {
    if (is.null(md) || !tr %in% names(md)) next
    v <- md[[tr]]
    if (length(v) == 0L) next
    v <- v[[1]]
    meta$biometrics[[tr]] <- if (is.factor(v)) as.character(v) else v
  }
  meta
}

#' Estimate the sampling rate (Hz) from a sorted POSIXct vector; NA if it cannot be determined.
#' @keywords internal
#' @noRd
.estimateHz <- function(datetime) {
  if (length(datetime) < 2L) return(NA_real_)
  d <- as.numeric(diff(as.numeric(datetime)))
  d <- d[is.finite(d) & d > 0]
  if (!length(d)) return(NA_real_)
  hz <- 1 / stats::median(d)
  if (abs(hz - round(hz)) < 0.01 * max(1, round(hz))) round(hz) else round(hz, 2)
}
