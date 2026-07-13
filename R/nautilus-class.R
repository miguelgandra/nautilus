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
#   $ id           character deployment/animal ID
#   $ deployment   list(lon, lat, datetime, popup_lon, popup_lat, popup_datetime,
#                       magnetic_declination, attachment_site)
#   $ tag          list(model, type, package_id, logger_id, paddle_wheel, axis_config)
#   $ biometrics   list of passive animal traits (sex, length, species, ...); imported via metadataColumns(traits=)
#   $ sensors      list(present, excluded, sampling_hz_original, sampling_hz_processed, timezone)
#   $ span         list(first_datetime, last_datetime, original_rows)
#   $ calibration  sidecar calibration object or NULL
#   $ axis_mapping structured list (see .newAxisMappingMeta): applied flag, net signed-perm per
#                  sensor family, determinant (handedness), dropped axes, source, original from/to rows
#   $ mag_calibration nested list (see .newMagCalibrationMeta): status, applied, applied_params
#                  (center/soft_iron/axis_net), proposed estimate, qc (confidence + metrics), provenance
#   $ processing   append-only list of step records (audit trail)


# Null-coalescing helper (base `%||%` only exists from R 4.4; define our own for older R)
`%||%` <- function(a, b) if (is.null(a)) b else a


#' Create an empty nautilus metadata object
#' @keywords internal
#' @noRd

.newNautilusMeta <- function() {
  list(
    id           = NA_character_,
    deployment   = list(lon = NA_real_, lat = NA_real_, datetime = as.POSIXct(NA),
                        popup_lon = NA_real_, popup_lat = NA_real_, popup_datetime = as.POSIXct(NA),
                        magnetic_declination = NA_real_, attachment_site = NA_character_),
    tag          = list(model = NA_character_, type = NA_character_, package_id = NA, logger_id = NA_character_,
                        paddle_wheel = NA, axis_config = NA_character_),
    biometrics   = list(),                                 # passive animal traits (sex, length, species, ...); roles ACT, traits RIDE
    sensors      = list(present = character(0), excluded = character(0), sampling_hz_original = NA_real_,
                        sampling_hz_processed = NA_real_, timezone = "UTC"),
    span         = list(first_datetime = as.POSIXct(NA), last_datetime = as.POSIXct(NA),
                        original_rows = NA_integer_),
    calibration  = NULL,
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
  rec <- c(list(step = step,
                nautilus_version = tryCatch(as.character(utils::packageVersion("nautilus")),
                                            error = function(e) NA_character_),
                time = Sys.time()),
           list(...))
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
  meta$calibration  <- a$calibration
  meta$axis_mapping <- .normalizeAxisMappingMeta(a$axis.mapping, source = "importTagData")
  meta
}


#' Upgrade a legacy / raw axis-mapping value to the structured axis-mapping metadata object
#'
#' Accepts the new structured list (returned unchanged), the historical `from/to` data.frame, `NA`, or
#' `NULL`, and returns a structured object (see [.newAxisMappingMeta]). When the value is a data.frame
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
# Public metadata accessors ###########################################################################
#######################################################################################################

#' Access the metadata of a nautilus tag object
#'
#' Returns the consolidated metadata record carried by a \code{nautilus_tag} (deployment details,
#' tag model, sensors, time span, calibration, axis mapping, and the processing audit trail). This
#' is the supported way to read metadata: user code should never reach into the underlying
#' attribute directly, since a partial match would otherwise return the unrelated
#' \code{nautilus.version} marker.
#'
#' Objects produced by older package versions (which stored metadata as many separate attributes)
#' are migrated to the current schema on the fly, so this accessor works on both new and legacy data.
#'
#' @param x A \code{nautilus_tag} object (or a data.frame/data.table produced by an earlier version).
#' @return A named list with the metadata schema: \code{id}, \code{deployment}, \code{tag},
#'   \code{sensors}, \code{span}, \code{calibration}, \code{axis_mapping}, and \code{processing}.
#' @seealso \code{\link{processingHistory}} for a tabular view of the processing trail.
#' @examples
#' \dontrun{
#' meta <- tagMetadata(tag)
#' meta$id
#' meta$deployment$lon
#' meta$sensors$sampling_hz_processed
#' }
#' @export

tagMetadata <- function(x) {
  if (is.null(x)) .abort("{.arg x} is {.code NULL}; expected a nautilus_tag (or a data.frame produced by nautilus).")
  m <- attr(x, "nautilus", exact = TRUE)
  # migrate legacy flat-attribute objects on read (without mutating the caller's object)
  if (is.null(m)) m <- .metaFromFlatAttrs(x)
  m
}


#' Retrieve the processing history of a nautilus tag object
#'
#' Each nautilus pipeline function appends a record to the object's processing audit trail. This
#' accessor returns that trail as a tidy data.frame, one row per processing step, in the order the
#' steps were applied.
#'
#' @param x A \code{nautilus_tag} object (or a data.frame/data.table produced by an earlier version).
#' @return A data.frame with columns \code{step} (function name), \code{time} (when it ran),
#'   \code{nautilus_version}, and \code{details} (a compact summary of the parameters recorded for
#'   that step). Returns a zero-row data.frame if no history is present.
#' @seealso \code{\link{tagMetadata}} for the full metadata record.
#' @examples
#' \dontrun{
#' processingHistory(tag)
#' }
#' @export

processingHistory <- function(x) {
  proc <- tagMetadata(x)$processing %||% list()

  empty <- data.frame(step = character(0), time = as.POSIXct(character(0)),
                      nautilus_version = character(0), details = character(0),
                      stringsAsFactors = FALSE)
  if (!length(proc)) return(empty)

  # compactly format a single recorded value (summarise long vectors by count)
  fmt1 <- function(v) {
    if (length(v) > 3) return(sprintf("<%d values>", length(v)))
    paste(format(v), collapse = ", ")
  }

  rows <- lapply(proc, function(p) {
    extra <- p[setdiff(names(p), c("step", "time", "nautilus_version"))]
    details <- if (length(extra)) {
      paste(names(extra), vapply(extra, fmt1, character(1)), sep = " = ", collapse = "; ")
    } else ""
    data.frame(step = p$step %||% NA_character_,
               time = if (is.null(p$time)) as.POSIXct(NA) else p$time,
               nautilus_version = p$nautilus_version %||% NA_character_,
               details = details,
               stringsAsFactors = FALSE)
  })
  do.call(rbind, rows)
}


#' Refresh biological traits on already-processed data
#'
#' @description
#' Re-stamps the biological traits (`tagMetadata(x)$biometrics`) of already-imported / processed data
#' from a (corrected) deployment-metadata table, WITHOUT re-reading the raw sensor files. Traits are
#' normally captured once at import (via `metadataColumns(traits = ...)`), but if a value is later fixed
#' or a new trait added, this propagates it to the processed objects cheaply - so the self-contained
#' objects stay the single source of truth while remaining correctable.
#'
#' @param data A `nautilus_tag` / data.frame, a (named) list of them, or a character vector of `.rds`
#'   paths (the output of any processing step).
#' @param id.metadata The deployment-metadata table (a data.frame or a `nautilus_deployments` object),
#'   one row per deployment, holding the trait columns and the id column.
#' @param columns A \code{\link{metadataColumns}} object naming the id column (`id`) and the trait
#'   columns (`traits`). Traits not listed here are left untouched.
#' @param id.col Character. Name of the ID column used to match each dataset to its metadata row.
#'   Default `"ID"`.
#' @param return.data Logical. Return the processed data in memory (default `TRUE`). When `FALSE`, the
#'   function instead returns the paths of the `.rds` files it wrote, which feed directly into the next
#'   step's `data` argument -- so a large fleet can be processed without ever holding it all in memory.
#'   `return.data = FALSE` therefore requires an `output.dir`.
#' @param output.dir Character. Directory in which to write one `<id>.rds` file per deployment. Providing
#'   a directory is what triggers saving; `NULL` (default) writes nothing. The directory must already exist.
#' @param output.suffix Character. Optional suffix appended to each saved file name (before `.rds`), e.g.
#'   to tag a processing run or avoid clashes. Only used when `output.dir` is set. Default `NULL`.
#' @param compress Compression for the saved `.rds` files (only used when `output.dir` is set): `TRUE`
#'   (default, gzip), `FALSE`, or one of `"gzip"`/`"bzip2"`/`"xz"`. See \code{\link[base]{saveRDS}}.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal", or `2`/"detailed" (default).
#' @return If `return.data = TRUE`, a named list of updated objects; if `return.data = FALSE`, a character
#'   vector of the written `.rds` file paths.
#' @seealso \code{\link{metadataColumns}}, \code{\link{importTagData}}, \code{\link{tagMetadata}}
#' @examples
#' \dontrun{
#' # correct a trait in the deployment table, then re-stamp it onto processed data
#' id.metadata <- data.frame(ID = c("shark01", "shark02"),
#'                           sex = c("F", "M"), length_cm = c(612, 548))
#' cols <- metadataColumns(id = "ID", traits = c("sex", "length_cm"))
#' tags <- updateBiometrics(processed, id.metadata, columns = cols, id.col = "ID")
#' }
#' @export
updateBiometrics <- function(data, id.metadata, columns = metadataColumns(), id.col = "ID",
                             return.data = TRUE, output.dir = NULL,
                             output.suffix = NULL, compress = TRUE, verbose = "detailed") {
  start.time <- Sys.time(); lvl <- .verbosity(verbose)
  columns <- .as_metadata_columns(columns)
  .assert_string(id.col, "id.col"); .assert_flag(return.data, "return.data")
  .assert_dir(output.dir, "output.dir"); .assert_compress(compress)
  .assert_output(return.data, output.dir)
  if (is.data.frame(id.metadata)) id.metadata <- as.data.frame(id.metadata) else .abort("{.arg id.metadata} must be a data.frame.")
  traits <- columns$traits
  if (is.null(traits) || !length(traits)) .abort(c("{.arg columns} names no {.field traits}.",
                                                   "i" = "Set {.code metadataColumns(traits = c(...))}."))
  mid <- columns$id
  miss <- setdiff(c(mid, traits), names(id.metadata))
  if (length(miss)) .abort("Column{?s} {.val {miss}} not found in {.arg id.metadata}.")

  r <- .resolveInput(data, id.col = id.col)
  .log_header(lvl, "updateBiometrics", "Refreshing biological traits",
              bullets = sprintf("Input: %d dataset%s \u00b7 traits: %s", r$n, if (r$n != 1) "s" else "", paste(traits, collapse = ", ")))
  results <- if (return.data) vector("list", r$n) else NULL
  saved <- vector("list", r$n)
  n_updated <- 0L; unmatched <- character(0)
  for (i in seq_len(r$n)) {
    x <- r$get(i)
    id <- tryCatch(as.character(unique(x[[id.col]])[1]), error = function(e) NA_character_)
    if (!length(id) || is.na(id)) id <- as.character(.getMeta(x)$id %||% r$ids[i])   # id may live only in meta$id
    row <- id.metadata[as.character(id.metadata[[mid]]) == id, , drop = FALSE]
    if (!nrow(row)) { unmatched <- c(unmatched, id); if (return.data) results[[i]] <- x; next }
    meta <- .getMeta(.ensureMeta(x))
    for (tr in traits) { v <- row[[tr]][1]; meta$biometrics[[tr]] <- if (is.factor(v)) as.character(v) else v }
    meta <- .appendProcessing(meta, "updateBiometrics", traits = paste(traits, collapse = ","))
    x <- .restoreMeta(x, meta); n_updated <- n_updated + 1L
    saved[i] <- list(.saveOutput(x, id, output.dir = output.dir, output.suffix = output.suffix, compress = compress))
    if (return.data) results[[i]] <- x
  }
  if (length(unmatched)) cli::cli_warn("{length(unmatched)} dataset{?s} had no matching id.metadata row: {.val {utils::head(unmatched, 6)}}.")
  if (lvl >= 1L) {
    .log_summary(lvl); .log_done(lvl, n_updated, " of ", r$n, " dataset", if (r$n != 1) "s", " refreshed")
    .log_runtime(lvl, start.time)
  }
  .collectOutput(results, saved, return.data, r$ids)
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


#' Summarise a nautilus_tag
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
