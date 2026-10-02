#######################################################################################################
# Apply an IMU axis mapping to already-imported data ##################################################
#######################################################################################################

#' Apply sensor-axis mappings to deployment data
#'
#' @description
#' Applies explicit signed permutations to imported accelerometer, gyroscope and magnetometer
#' channels, establishing the axis conventions needed for body-frame movement and orientation
#' analysis. Mappings can be supplied manually, selected from documented configurations, inferred
#' with [checkTagMapping()], reconciled with [consensusAxisMapping()] or selected through
#' [reviewTagMapping()].
#'
#' Axis mapping is separate from import, sensor calibration and metric derivation. Each applied
#' mapping and its provenance are recorded in deployment metadata and processing history.
#' Use after deployment trimming and mapping assessment, before [processTagData()].
#' Datasets can be returned in memory or saved individually for subsequent processing.
#'
#' @param data A \code{nautilus_tag} object, a list of deployment datasets, a data frame containing
#'   deployments identified by \code{id.col}, or a character vector of \code{.rds} file paths.
#'   Imported, trimmed and sensor-checked data are recommended. File inputs are read sequentially.
#'   Canonical IMU column names are required for the sensor families being mapped.
#' @param mapping Mapping object to apply; provide this or \code{configs}, but not both.
#'   Accepts a \code{from}/\code{to} data frame for all deployments, a named list of such tables
#'   keyed by deployment ID, results from [checkTagMapping()] or [consensusAxisMapping()], or
#'   a completed \code{nautilus_review} from [reviewTagMapping()]. Default \code{NULL}.
#'   See Details for mapping syntax, routing and review decisions.
#' @param configs Named list of documented configurations, each a \code{from}/\code{to} data
#'   frame. Used instead of \code{mapping}; each deployment's \code{tag$axis_config} metadata
#'   selects its configuration. Missing or blank configuration metadata leave the dataset
#'   unchanged; an unknown configuration name is an error. Default \code{NULL}.
#' @param id.col Character. Name of the column identifying deployments, not animals. Default
#'   \code{"ID"}. Used to route deployment-specific mappings.
#' @param datetime.col Character. Name of the timestamp column, used to estimate the sampling
#'   rate for accelerometer--gyroscope co-registration. Default \code{"datetime"}.
#' @param relative Logical. Treat the mapping as an incremental transform of the current sensor
#'   frame rather than an absolute raw-to-target transform. Default \code{FALSE}; see Details
#'   for composition and reapplication.
#' @param check.handedness Logical. Report reflected sensor conventions and assess
#'   accelerometer--gyroscope co-registration when both families are mapped and available.
#'   Default \code{TRUE}. Reflections alone do not generate a warning; a sufficiently supported
#'   co-registration failure does. \code{FALSE} skips the reflection note and co-registration
#'   diagnostic, but net transforms and their determinants are still recorded.
#' @param return.data Whether to return the mapped datasets in memory (default \code{TRUE}).
#'   When \code{FALSE}, return the saved \code{.rds} paths invisibly; requires \code{output.dir}.
#' @param output.dir Character. Existing directory in which to write one \code{<id>.rds} file
#'   per retained deployment. Providing a directory triggers saving, including unchanged
#'   datasets; \code{NULL} (default) writes nothing.
#' @param exclusions.file Optional path to the shared deployment-exclusion CSV. This stage records
#'   explicit \code{"Exclude"} decisions from a review. Its rows are refreshed for deployments
#'   evaluated in the current call without disturbing records outside that scope or other stages.
#'   Pass the same path to subsequent stages and [summarizeTagData()]. Default \code{NULL},
#'   which writes no exclusion log.
#' @param output.suffix Optional string appended to each saved filename before \code{.rds}.
#'   Used only when \code{output.dir} is supplied. Default \code{NULL}.
#' @param compress Compression passed to [base::saveRDS()]: \code{TRUE} (default, gzip),
#'   \code{FALSE}, \code{"gzip"}, \code{"bzip2"} or \code{"xz"}.
#' @param verbose How much detail to print: \code{0}/\code{"quiet"},
#'   \code{1}/\code{"normal"}, or \code{2}/\code{"detailed"} (default). Normal output reports
#'   deployment-level outcomes; detailed output adds sensor-family transforms and diagnostics.
#'
#' @details
#' ## Reference frames and workflow
#'
#' The target body-frame convention assigns X to longitudinal motion (surge), Y to lateral motion
#' (sway) and Z to dorsoventral motion (heave). The function applies the mapping supplied by the
#' caller; it does not infer anatomical direction or verify an animal's posture by itself.
#' Signed permutations exchange and invert axes, preserving units and timestamps. They do not
#' correct an arbitrary mounting angle, sensor bias, clock offset or magnetic declination.
#'
#' A typical workflow is:
#'
#' \enumerate{
#'   \item Import data and trim them with [filterDeploymentData()], then inspect sensor quality.
#'   \item Assess raw-axis records with [checkTagMapping()], optionally providing documented
#'     configurations.
#'   \item Reconcile compatible deployments with [consensusAxisMapping()] where a shared
#'     configuration is justified.
#'   \item Review selected cases with [reviewTagMapping()], using the original diagnostic
#'     evidence and optionally the reconciled mappings as its base.
#'   \item Apply the selected mappings here, then calibrate sensors and derive metrics with
#'     [calibrateMagnetometer()] and [processTagData()] as appropriate.
#' }
#'
#' Documented mappings can instead be supplied directly through \code{configs} when the axis
#' configuration is independently established. Inference and video review are not mandatory
#' prerequisites, but application alone is not evidence that a mapping is scientifically valid.
#'
#' ## Mapping tables and deployment routing
#'
#' A mapping table has \code{from} and \code{to} columns. \code{from} identifies a source sensor
#' axis: \code{ax/ay/az}, \code{gx/gy/gz} or \code{mx/my/mz}. \code{to} identifies a destination
#' in the same family, optionally prefixed by \code{"-"} to invert its sign. For example,
#' \code{from = "ay", to = "-ax"} assigns the negative of source \code{ay} to output \code{ax}.
#' Exchanges are simultaneous, not sequential copies.
#'
#' Unspecified destinations retain their identity assignment. With these defaults included,
#' each mapped family must form a valid signed permutation: exactly one source per destination
#' and no source reused across destinations. Complete triplets are the clearest specification.
#'
#' The literal string \code{"NA"} in \code{to} instead sets the source channel to numeric
#' \code{NA} and records it as dropped. This is a non-invertible channel exclusion, not a rotation.
#' A later mapping cannot recover discarded measurements; use the original data to revise such
#' a decision. Do not use an R missing value in place of the literal \code{"NA"}.
#'
#' A single table applies to all deployments. Deployment-specific objects are routed by the
#' dataset's own \code{id.col} value where it matches a mapping, otherwise by the resolved list
#' name or file basename. An empty or unmatched mapping leaves that dataset unchanged. Non-empty
#' mappings without matching data generate a warning; no identifier overlap in a deployment-specific
#' mapping set is an error, except where review exclusions legitimately remove all supplied data.
#' Keep list names and file basenames aligned with deployment IDs for consistent output names.
#'
#' ## Absolute and relative transforms
#'
#' With \code{relative = FALSE}, the supplied mapping describes the absolute raw-to-target
#' transform. If a net mapping is already recorded, only the difference is applied. For current
#' transform \code{C} and target \code{T}, the applied transform is \code{T %*% t(C)}.
#' Reapplying the same invertible mapping therefore leaves sensor values unchanged; replacing it
#' produces the same values as applying the new target to the original raw axes.
#'
#' With \code{relative = TRUE}, \code{T} is applied to the current values and the new net transform
#' is \code{T %*% C}. Repeated relative applications accumulate and are not idempotent.
#' Correct composition depends on preserved axis-mapping metadata and does not extend to dropped
#' channels. A processing-history entry can still be added when sensor values need no change.
#'
#' ## Sensor-family completion and handedness
#'
#' An accelerometer mapping without explicit gyroscope rows derives a gyroscope map as
#' \code{det(M) * M}, where \code{M} is the effective accelerometer signed permutation.
#' Angular velocity is an axial vector, requiring the determinant factor for reflected frames.
#' Derivation also assumes the native accelerometer and gyroscope frames are co-oriented;
#' this is a hardware convention, not a universal property. Explicit gyroscope rows take precedence.
#'
#' Magnetometer mappings are never inferred from the accelerometer here. Without explicit
#' magnetometer rows, those channels remain in their current frame. Establish their convention
#' independently before interpreting heading; magnetic calibration is not a substitute for axis
#' alignment. A partially present triplet is skipped with a warning; a completely absent family
#' is not created.
#'
#' A determinant of -1 describes a reflected sensor convention and is not, by itself, a failure.
#' With \code{check.handedness = TRUE}, mapped acceleration and angular velocity are compared
#' through \code{d(ghat)/dt = -omega x ghat}. A pooled correlation below \code{0.2}, with at least
#' 200 usable high-rotation samples, generates a co-registration warning. Insufficient rotation
#' yields \code{NA}; neither the absence of a warning nor an unavailable diagnostic proves a
#' correct mapping. The diagnostic reports a mismatch but does not automatically exclude data.
#'
#' ## Review decisions, exclusions and downstream data
#'
#' A \code{nautilus_review} embeds a base mapping and concrete candidate mappings. Selected
#' decisions replace the deployment's base mapping and receive review provenance. A deployment
#' with candidate choices and rendered clips must have a decision before application; invalid
#' labels or an undecided rendered comparison stop the call. Single-indicator and unrendered
#' cases retain the base mapping unless explicitly excluded.
#'
#' A decision of \code{"Exclude"} omits the entire deployment from returned and saved data and
#' optionally records it in \code{exclusions.file}. This differs from a \code{"NA"} channel drop,
#' which retains the record. Empty mappings, absent families and skipped partial triplets do not
#' cause deployment exclusion.
#'
#' Only raw IMU channels are transformed. Previously derived pitch, roll, heading or movement
#' columns are not recomputed. Re-run the appropriate calibration and processing steps after
#' changing a frame, and check the recorded sensor-family state before using orientation-dependent
#' metrics. A retained dataset is not necessarily a fully mapped dataset.
#'
#' @return When \code{return.data = TRUE}, a named list of retained deployment datasets,
#'   normally \code{nautilus_tag} objects carrying sensor data and metadata. Mapped datasets record
#'   the source, per-family provenance, mapping table, net transforms, determinants, dropped
#'   channels and co-registration diagnostics under \code{getTagMetadata(x)$axis_mapping},
#'   together with a processing-history entry. Unmapped datasets are retained unchanged.
#'
#'   When \code{return.data = FALSE}, a character vector of written \code{.rds} paths, returned
#'   invisibly. Explicitly excluded deployments appear in neither output; where exclusions occur,
#'   output attributes \code{excluded} and \code{nautilus.exclusions} contain their identifiers
#'   and stage-specific exclusion rows, respectively.
#'
#' @seealso [importTagData()] for raw sensor data; [checkTagMapping()] for diagnostic inference;
#'   [consensusAxisMapping()] for cross-deployment reconciliation; [reviewTagMapping()] for video
#'   decisions; [calibrateMagnetometer()] and [processTagData()] for subsequent calibration and
#'   metric derivation; [getTagMetadata()] and [processingHistory()] for recorded provenance.
#'
#' @examples
#' \dontrun{
#' # Raw-axis records already trimmed and checked for sensor integrity.
#' files <- list.files("./checked", pattern = "\\.rds$", full.names = TRUE)
#' evidence <- checkTagMapping(files)
#' reconciled <- consensusAxisMapping(evidence)
#'
#' # Apply after inspecting the diagnostics and any required video review.
#' oriented <- applyAxisMapping(files, mapping = reconciled)
#' getTagMetadata(oriented[[1]])$axis_mapping
#'
#' # A documented configuration selected by each deployment's axis_config metadata.
#' # Accelerometer-only configurations derive gyro mappings, not magnetometer mappings.
#' configs <- list(camera_A = data.frame(from = c("ax", "ay", "az"),
#'                                      to = c("ay", "-ax", "az")))
#' oriented <- applyAxisMapping(files, configs = configs)
#'
#' # File-based workflow: the output directory must already exist.
#' oriented_files <- applyAxisMapping(files, mapping = reconciled,
#'                                    output.dir = "./oriented", return.data = FALSE)
#' }
#' @export

applyAxisMapping <- function(data,
                             mapping = NULL,
                             configs = NULL,
                             id.col = "ID",
                             datetime.col = "datetime",
                             relative = FALSE,
                             check.handedness = TRUE,
                             return.data = TRUE,
                             output.dir = NULL,
                             exclusions.file = NULL,
                             output.suffix = NULL,
                             compress = TRUE,
                             verbose = "detailed") {

  ##############################################################################
  # Validate arguments #########################################################
  ##############################################################################

  lvl <- .verbosity(verbose)
  if (is.null(mapping) && is.null(configs)) .abort("Provide either {.arg mapping} or {.arg configs}.")
  if (!is.null(mapping) && !is.null(configs)) .abort("Provide only one of {.arg mapping} or {.arg configs}.")
  .assert_flag(relative, "relative"); .assert_flag(check.handedness, "check.handedness")
  .assert_flag(return.data, "return.data")
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  .assert_string(output.suffix, "output.suffix", null_ok = TRUE)
  .assert_dir(output.dir, "output.dir")                         # fail-fast: must exist
  .assert_writable_file(exclusions.file, "exclusions.file", ext = "csv", null_ok = TRUE)
  .assert_compress(compress)
  .assert_output(return.data, output.dir)

  # `configs` (a named dictionary config-name -> from/to) is resolved per deployment from each tag's
  # `axis_config` metadata; `mapping` is normalised to a routing set ($single / $by_id) up front. See
  # .asAxisMappingSet() / .validateConfigs() / .expandAxisConfigForTag().
  set <- if (!is.null(configs)) {
    .validateConfigs(configs)
    list(producer = "axis_config", single = NULL, by_id = NULL, provenance = NULL)
  } else .asAxisMappingSet(mapping)
  families <- list(accel = c("ax", "ay", "az"), gyro = c("gx", "gy", "gz"), mag = c("mx", "my", "mz"))

  start.time <- Sys.time()
  r <- .resolveInput(data, id.col = id.col)
  results <- if (return.data) vector("list", r$n) else NULL
  saved   <- vector("list", r$n)                                # written .rds paths, per item

  hdr_bullets <- sprintf("Input: %d dataset%s", r$n, if (r$n != 1) "s" else "")
  if (!is.null(output.dir)) hdr_bullets <- c(hdr_bullets, paste0("Output: ", output.dir))
  .log_header(lvl, "applyAxisMapping", "Applying the IMU axis mapping",
              bullets = hdr_bullets,
              arrow = sprintf("Source: %s %s remap", set$producer, if (relative) "relative" else "absolute"))

  ##############################################################################
  # Iterate over individuals ###################################################
  ##############################################################################

  matched_ids <- character(0)                                   # mapping ids actually routed to a dataset
  scope_ids <- character(0)                                     # every deployment evaluated in this call
  excluded_ids <- set$excluded %||% character(0)                # review decision == "exclude": drop from output
  excluded_out <- character(0)
  n_remapped  <- 0L; n_nomap <- 0L; n_noimu <- 0L; n_reflect <- 0L; n_excluded <- 0L; n_coreg_fail <- 0L
  fam_lab <- function(fam) sprintf("%-6s", paste0(fam, ":"))    # gap-padded label so values line up

  for (i in seq_len(r$n)) {

    id <- r$ids[i]
    x  <- r$get(i)
    if (!data.table::is.data.table(x)) x <- data.table::as.data.table(x)
    meta <- .getMeta(x)

    # the deployment id used for routing: prefer the data's own ID column (robust to file basenames),
    # fall back to the resolved id (file basename / list name).
    true_id   <- tryCatch(as.character(unique(x[[id.col]])[1]), error = function(e) NA_character_)
    who       <- if (!is.na(true_id)) true_id else id            # best display / message id
    scope_ids <- c(scope_ids, as.character(who))
    lookup_id <- if (!is.null(set$by_id)) {
      if (!is.na(true_id) && true_id %in% names(set$by_id)) true_id
      else if (id %in% names(set$by_id)) id else NA_character_
    } else NA_character_

    # review decision == "exclude": drop the deployment from the output entirely (no mapping, no file). The
    # orientation is untrustworthy, so it never enters the oriented dataset and downstream steps skip it.
    if (who %in% excluded_ids || id %in% excluded_ids) {
      n_excluded <- n_excluded + 1L; excluded_out <- c(excluded_out, who)
      if (lvl >= 2L) { .log_h2(lvl, sprintf("%s (%d/%d)", who, i, r$n)); .log_skip(lvl, "excluded per review (no output written)") }
      else if (lvl >= 1L) .log_skip(lvl, who, "  excluded per review (no output written)")
      rm(x); next                                                 # results[[i]] left NULL -> dropped from the return
    }
    .log_h2(lvl, sprintf("%s (%d/%d)", who, i, r$n))

    # pick this deployment's mapping: from `configs` (looked up by the tag's axis_config), else the
    # apply-to-all single df or the routed per-id table.
    ft_i <- if (!is.null(configs)) .expandAxisConfigForTag(meta$tag$axis_config, configs, who)
            else if (is.null(set$by_id)) set$single
            else if (!is.na(lookup_id)) set$by_id[[lookup_id]] else NULL
    if (!is.null(lookup_id) && !is.na(lookup_id)) matched_ids <- c(matched_ids, lookup_id)

    # no mapping for this deployment (unmatched id, or an empty/unresolved mapping) -> leave unchanged
    if (is.null(ft_i) || !nrow(ft_i)) {
      n_nomap <- n_nomap + 1L
      if (lvl >= 2L) .log_skip(lvl, "left unchanged (no mapping)")     # terse: the sub-header carries the id
      else           .log_skip(lvl, who, "  left unchanged (no mapping)")
      saved[i] <- list(.saveOutput(x, id, output.dir = output.dir,
                  output.suffix = output.suffix, compress = compress))
      .log_gap(lvl)
      if (return.data) results[[i]] <- x
      rm(x); next
    }
    ft <- as.data.frame(ft_i[, c("from", "to")], stringsAsFactors = FALSE)
    # complete the family set: an accel-only spec (the common documented config) has its gyro map
    # DERIVED (det(M)*M, the co-die default) so the gyro is co-registered instead of left in the raw
    # frame. Explicit gyro rows are never overwritten; the magnetometer keeps its own strategy.
    ft <- .completeFamilies(ft)

    # current applied state (structured), tolerating legacy shapes
    am       <- .normalizeAxisMappingMeta(meta$axis_mapping)
    cur_net  <- am$net %||% list()
    new_net     <- cur_net
    new_dropped <- am$dropped %||% character(0)
    touched     <- character(0)
    fam_rec     <- stats::setNames(vector("list", length(families)), names(families))  # per-family display state

    for (fam in names(families)) {
      axes <- families[[fam]]
      ft_fam  <- ft[ft$from %in% axes, , drop = FALSE]
      present <- axes %in% names(x)
      if (nrow(ft_fam) == 0) {                                  # mapping does not cover this family
        fam_rec[[fam]] <- list(status = if (any(present)) "nomap" else "nochannels")
        next
      }
      if (!all(present)) {                                      # mapping exists but the triplet is incomplete
        if (any(present)) {
          warning(sprintf("ID %s: '%s' family only partially present; skipping its mapping.", who, fam), call. = FALSE)
          fam_rec[[fam]] <- list(status = "partial")
        } else {
          fam_rec[[fam]] <- list(status = "nochannels")
        }
        next
      }
      touched <- c(touched, fam)

      # faulty-axis drops: not invertible / not a permutation -> apply faithfully, no matrix form
      if (any(ft_fam$to == "NA")) {
        .applyAxisRemap(x, ft_fam)
        new_net[[fam]] <- NULL
        if (!relative) new_dropped <- setdiff(new_dropped, axes)
        new_dropped <- union(new_dropped, ft_fam$from[ft_fam$to == "NA"])
        fam_rec[[fam]] <- list(status = "dropped", ft = ft_fam)
        next
      }

      tgt <- .mappingToSignedPerm(ft_fam, axes)
      if (is.null(tgt)) {
        .abort("{who}: the mapping for the {.val {fam}} family is not a valid signed permutation (incomplete or duplicated axes).")
      }
      cur_mat <- cur_net[[fam]] %||% diag(3)
      # absolute: apply only the delta from the current net; relative: apply the mapping as-is
      delta <- if (relative) tgt else tgt %*% .signedPermInverse(cur_mat)
      if (!all(delta == diag(3))) {
        newcols <- .signedPermApplyCols(x, delta, axes)
        for (a in names(newcols)) data.table::set(x, j = a, value = newcols[[a]])
      }
      new_net[[fam]] <- if (relative) tgt %*% cur_mat else tgt
      if (!relative) new_dropped <- setdiff(new_dropped, axes)
      fam_rec[[fam]] <- list(status = if (all(ft_fam$to == ft_fam$from)) "verified" else "mapped",
                             ft = ft_fam, det = .signedPermDet(tgt))
    }

    ##########################################################################
    # Record the net mapping + provenance in metadata ########################
    ##########################################################################

    det_vec <- if (length(new_net)) vapply(new_net, .signedPermDet, integer(1)) else NULL
    had_reflection <- !is.null(det_vec) && any(det_vec == -1L)
    if (had_reflection) n_reflect <- n_reflect + 1L

    # per-family origin for the families actually remapped (self / consensus / manual)
    prov_fam <- if (!is.null(lookup_id) && !is.na(lookup_id)) set$provenance[[lookup_id]] else NULL
    fam_prov <- if (length(touched)) {
      if (!is.null(prov_fam)) prov_fam[touched]
      else {
        default_origin <- switch(set$producer, manual = "manual", axis_config = "axis_config", "self")
        stats::setNames(rep(default_origin, length(touched)), touched)
      }
    } else NULL

    am_new <- .newAxisMappingMeta()
    am_new$applied     <- TRUE
    am_new$source      <- set$producer
    am_new$provenance  <- fam_prov
    am_new$from_to     <- ft
    am_new$net         <- if (length(new_net)) new_net else NULL
    am_new$determinant <- det_vec
    am_new$dropped     <- unique(new_dropped)

    # Frame-level accel<->gyro co-registration check - the ACTUAL correctness signal (a per-family
    # reflection, determinant -1, is a benign device convention: the gyro carries the matching det(M) sign
    # via .completeFamilies, so a reflection stays co-registered, corr ~ +1). Runs on the MAPPED accel+gyro
    # (both already in the shared body frame). corr ~ +1 = co-registered; a decisively low/negative corr
    # with enough rotation = a genuine family mis-registration (e.g. an independent gyro die whose
    # convention the co-die default got wrong) -> a real warning. `check.handedness` gates the whole check.
    coreg <- list(corr = NA_real_, frac = NA_real_, n = 0L)
    if (check.handedness && all(c("accel", "gyro") %in% touched) &&
        all(c("ax", "ay", "az", "gx", "gy", "gz") %in% names(x))) {
      fs <- tryCatch(.tagFs(x, datetime.col), error = function(e) NA_real_)
      if (is.finite(fs)) {
        coreg <- .coregCorr(cbind(x[["ax"]], x[["ay"]], x[["az"]]),
                            cbind(x[["gx"]], x[["gy"]], x[["gz"]]), fs)
      }
    }
    am_new$coreg_corr <- coreg$corr
    am_new$coreg_frac <- coreg$frac
    coreg_fail <- is.finite(coreg$corr) && coreg$n >= 200L && coreg$corr < 0.2
    if (coreg_fail) {
      n_coreg_fail <- n_coreg_fail + 1L
      warning(sprintf(paste0("ID %s: accelerometer and gyroscope do not co-register (co-registration r = %.2f over %d ",
                             "high-rotation samples); the gyroscope axis mapping is likely wrong (an independent gyro die ",
                             "whose convention differs from the accelerometer's?). Verify the raw gyro axis convention."),
                      who, coreg$corr, coreg$n), call. = FALSE)
    }

    meta$axis_mapping <- am_new
    meta <- .appendProcessing(meta, "applyAxisMapping",
                              relative = relative,
                              source = set$producer,
                              families = paste(touched, collapse = ", "))
    x <- .restoreMeta(x, meta)

    # save first (silently) so the saved file can be named on the terse outcome line below
    saved_to <- .saveOutput(x, id, output.dir = output.dir,
                            output.suffix = output.suffix, compress = compress)
    saved[i] <- list(saved_to)

    ##########################################################################
    # Render the per-dataset block ###########################################
    ##########################################################################

    # detailed level: ALWAYS one line per sensor family (accel / gyro / mag), shown only when at least one
    # family was actually mapped (a no-IMU dataset is a single outcome line, not three "untouched" lines).
    if (lvl >= 2L && length(touched)) {
      for (fam in names(families)) {
        rec <- fam_rec[[fam]]; st <- rec$status
        origin <- if (fam %in% touched && !is.null(fam_prov)) fam_prov[[fam]] else NA_character_
        if (identical(st, "mapped")) {
          tf <- paste(sprintf("%s\u2192%s", rec$ft$from, rec$ft$to), collapse = " \u00b7 ")
          note <- if (check.handedness && identical(rec$det, -1L)) " \u00b7 reflection (left-handed convention)" else ""
          .log_detail(lvl, fam_lab(fam), " ", tf, " (", origin, ")", note)
        } else if (identical(st, "verified")) {
          .log_detail(lvl, fam_lab(fam), " verified correct (", origin, ")")
        } else if (identical(st, "dropped")) {
          .log_detail(lvl, fam_lab(fam), " ", paste(rec$ft$from, collapse = ", "), " \u2192 NA (dropped \u2014 faulty sensor)")
        } else if (identical(st, "partial")) {
          .log_skip(lvl, fam_lab(fam), " partial channels \u2014 skipped")
        } else if (identical(st, "nochannels")) {
          .log_detail(lvl, fam_lab(fam), " untouched (no channels)")
        } else {                                                # nomap
          .log_detail(lvl, fam_lab(fam), " untouched (no mapping)")
        }
      }
      if (is.finite(am_new$coreg_corr))
        .log_detail(lvl, "co-registration: accel\u2194gyro r = ", sprintf("%.2f", am_new$coreg_corr),
                    if (coreg_fail) " \u2014 MISMATCH (gyro mapping likely wrong)" else "")
    }

    # curated outcome: terse at the detailed level (the block above carries the detail), self-describing
    # at the normal level (no block above it).
    if (length(touched)) {
      n_remapped <- n_remapped + 1L
      if (lvl >= 2L) {
        .log_ok(lvl, if (!is.null(saved_to)) paste0("saved ", basename(saved_to)) else "remapped")
      } else {
        .log_ok(lvl, who, "  remapped ", paste(touched, collapse = ", "),
                if (!is.null(saved_to)) paste0(" ", cli::symbol$bullet, " saved ", basename(saved_to)))
      }
    } else {
      n_noimu <- n_noimu + 1L
      if (lvl >= 2L)      cli::cli_alert_danger("nothing remapped (no IMU channels)")
      else if (lvl >= 1L) cli::cli_alert_danger("{who}  nothing remapped (no IMU channels)")
    }
    .log_gap(lvl)

    if (return.data) results[[i]] <- x
    rm(x)
  }

  # a structured object whose ids never matched any dataset is almost certainly an id-space mistake
  # (skipped when the review excluded deployments, which legitimately empties the routed set)
  if (!is.null(set$by_id) && !length(matched_ids) && !n_excluded) {
    .abort(c("None of the mapping ids matched a dataset {.field {id.col}}.",
             "i" = "mapping ids: {.val {utils::head(names(set$by_id), 6)}}; dataset ids: {.val {utils::head(r$ids, 6)}}."))
  }
  # non-empty mappings that were never routed to a dataset: a real warning (fires at any verbosity), as
  # it usually means a file is missing from `data`.
  unmatched <- character(0)
  if (!is.null(set$by_id)) {
    nonempty <- names(set$by_id)[vapply(set$by_id, function(m) !is.null(m) && nrow(m) > 0, logical(1))]
    unmatched <- setdiff(nonempty, matched_ids)
  }
  if (length(unmatched) > 0L) {
    cli::cli_warn("{length(unmatched)} mapping{?s} had no matching dataset and {?was/were} not applied: {.val {utils::head(unmatched, 6)}}.")
  }

  # final summary
  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, n_remapped, " of ", r$n, " dataset", if (r$n != 1) "s", " remapped")
    if (n_nomap > 0L)   .log_arrow(lvl, "left unchanged (no mapping): ", n_nomap)
    if (n_excluded > 0L) .log_arrow(lvl, "excluded per review (no output): ", n_excluded)
    if (n_noimu > 0L)   .log_arrow(lvl, "nothing remapped (no IMU channels): ", n_noimu)
    if (n_coreg_fail > 0L) cli::cli_alert_danger("{n_coreg_fail} dataset{?s} failed accel/gyro co-registration (gyro mapping likely wrong)")
    if (n_reflect > 0L && lvl >= 2L) cli::cli_alert_info("{n_reflect} dataset{?s} {?uses/use} a reflection (left-handed) axis convention - gyro co-registered automatically")
    if (!is.null(output.dir)) .log_arrow(lvl, "output: ", output.dir)
    if (!is.null(exclusions.file)) .log_arrow(lvl, "exclusions: ", exclusions.file)
    .log_runtime(lvl, start.time)
  }

  # drop excluded deployments (their NULL slots), index-aligned across both accumulators
  keep <- if (return.data) !vapply(results, is.null, logical(1)) else !vapply(saved, is.null, logical(1))
  out <- .collectOutput(results[keep], saved[keep], return.data, r$ids[keep])
  if (length(excluded_out)) attr(out, "excluded") <- excluded_out
  # Refresh only this stage's rows for deployments evaluated by the current call. A review decision is
  # the only way a deployment leaves here, so the reason is the same for every one of them.
  excl <- .exclusionsBind(lapply(excluded_out, function(i)
    .exclusionsRow(i, "applyAxisMapping", "excluded by tag-mapping review")))
  .exclusionsWrite(excl, exclusions.file, "applyAxisMapping", scope.ids = scope_ids)
  if (nrow(excl)) attr(out, "nautilus.exclusions") <- excl
  # assigning to `out` and returning it bare would strip the invisibility .collectOutput set on the paths
  # branch, re-printing the wall of paths; re-apply it (the data branch stays visible, as requested).
  if (isTRUE(return.data)) out else invisible(out)
}


#######################################################################################################
# Internal: the axis-config dictionary (shared by applyAxisMapping + checkTagMapping) #################
#######################################################################################################

# A `configs` dictionary maps a config NAME (e.g. "CATS Camera") to a from/to mapping. Each deployment
# carries its config name in `tag$axis_config` (set at import from the `axis_config` metadata column);
# the expander looks it up, with a clear error on a name that is not in the dictionary (a typo). Used by
# both applyAxisMapping() (to apply) and checkTagMapping() (to validate against the data).

#' Validate a `configs` dictionary: a named list of from/to data.frames over IMU axes.
#' @keywords internal
#' @noRd
.validateConfigs <- function(configs) {
  if (!is.list(configs) || is.null(names(configs)) || any(!nzchar(names(configs))) || anyDuplicated(names(configs))) {
    .abort("{.arg configs} must be a uniquely-named list mapping a config name to a from/to data.frame.")
  }
  imu <- c("ax", "ay", "az", "gx", "gy", "gz", "mx", "my", "mz")
  for (nm in names(configs)) {
    ft <- configs[[nm]]
    if (!is.data.frame(ft) || !all(c("from", "to") %in% names(ft)) || nrow(ft) == 0L) {
      .abort("{.arg configs[[{.val {nm}}]]} must be a non-empty data.frame with columns {.val {c('from','to')}}.")
    }
    bad <- setdiff(as.character(ft$from), imu)
    if (length(bad)) .abort("{.arg configs[[{.val {nm}}]]}$from has non-IMU axis{?es} {.val {bad}}.")
  }
  invisible(configs)
}

#' Resolve one tag's config name (from its metadata) to its from/to mapping via the dictionary.
#'
#' Returns the from/to data.frame, or NULL when the tag has no documented config (blank/NA). Aborts with
#' an informative message if the named config is not in the dictionary.
#' @keywords internal
#' @noRd
.expandAxisConfigForTag <- function(axis_config, configs, id) {
  if (is.null(axis_config) || length(axis_config) == 0L || is.na(axis_config[1]) ||
      !nzchar(trimws(as.character(axis_config[1])))) return(NULL)
  cfg <- trimws(as.character(axis_config[1]))
  if (!cfg %in% names(configs)) {
    .abort(c("{.val {id}} names axis config {.val {cfg}}, which is not in {.arg configs}.",
             "i" = "Configs provided: {.val {names(configs)}}."))
  }
  configs[[cfg]]
}


#######################################################################################################
# Internal: normalise any accepted mapping shape into a routing set ###################################
#######################################################################################################

# Returns list(single, by_id, producer, provenance):
#   single     - a from/to data.frame applied to every dataset, or NULL
#   by_id      - a named list of per-deployment from/to data.frames, or NULL
#   producer   - "manual" | "checkTagMapping" | "consensusAxisMapping"
#   provenance - NULL (apply-to-all), or a named (by id) list of per-family origin vectors
#' @keywords internal
#' @noRd
.asAxisMappingSet <- function(mapping) {
  # (0) a reviewed decision sheet: overlay the human choices onto its embedded base mapping
  if (inherits(mapping, "nautilus_review")) return(.resolveReviewSet(mapping))

  fam_axes <- list(accel = c("ax", "ay", "az"), gyro = c("gx", "gy", "gz"), mag = c("mx", "my", "mz"))
  fams_in <- function(ft) {                                     # families present (non-empty) in a from/to df
    if (is.null(ft) || !is.data.frame(ft) || !nrow(ft)) return(character(0))
    names(Filter(function(ax) any(ft$from %in% ax), fam_axes))
  }

  # (1) a single from/to data.frame -> apply to every dataset
  if (is.data.frame(mapping)) {
    if (!all(c("from", "to") %in% names(mapping)) || !nrow(mapping)) {
      .abort("{.arg mapping} data.frame must have non-empty {.field from} and {.field to} columns.")
    }
    return(list(single = as.data.frame(mapping[, c("from", "to")], stringsAsFactors = FALSE),
                by_id = NULL, producer = "manual", provenance = NULL))
  }

  if (!is.list(mapping)) {
    .abort("{.arg mapping} must be a from/to data.frame, a named list of them, or the output of {.fn checkTagMapping} / {.fn consensusAxisMapping}.")
  }

  # (2) consensusAxisMapping() result: $mappings (reconciled) + $provenance (per-family origin)
  if (!is.null(mapping$mappings) && is.data.frame(mapping$provenance)) {
    by_id <- mapping$mappings
    pv    <- mapping$provenance
    provenance <- stats::setNames(lapply(names(by_id), function(id) {
      row <- pv[pv$id == id, , drop = FALSE]
      if (!nrow(row)) return(NULL)
      stats::setNames(c(row$accel[1], row$gyro[1], row$mag[1]), c("accel", "gyro", "mag"))
    }), names(by_id))
    return(list(single = NULL, by_id = by_id, producer = "consensusAxisMapping", provenance = provenance))
  }

  # (3) checkTagMapping() result: a named list whose elements carry $proposal (each deployment's own evidence)
  has_prop <- vapply(mapping, function(e) is.list(e) && is.data.frame(e$proposal), logical(1))
  if (length(has_prop) && all(has_prop)) {
    ids   <- names(mapping) %||% vapply(mapping, function(e) as.character(e$id %||% NA), character(1))
    by_id <- stats::setNames(lapply(mapping, function(e) e$proposal), ids)
    provenance <- stats::setNames(lapply(by_id, function(ft) {
      f <- fams_in(ft); if (!length(f)) NULL else stats::setNames(rep("self", length(f)), f)
    }), ids)
    return(list(single = NULL, by_id = by_id, producer = "checkTagMapping", provenance = provenance))
  }

  # (4) a plain named list of from/to data.frames (hand-built, one per deployment id)
  is_ft <- vapply(mapping, function(e) is.data.frame(e) && all(c("from", "to") %in% names(e)), logical(1))
  if (length(is_ft) && all(is_ft)) {
    if (is.null(names(mapping))) .abort("A list of from/to tables must be named by deployment {.field id}.")
    provenance <- stats::setNames(lapply(mapping, function(ft) {
      f <- fams_in(ft); if (!length(f)) NULL else stats::setNames(rep("manual", length(f)), f)
    }), names(mapping))
    return(list(single = NULL, by_id = mapping, producer = "manual", provenance = provenance))
  }

  .abort("{.arg mapping} is not a recognised mapping object (a from/to data.frame, a named list of them, or the output of {.fn checkTagMapping} / {.fn consensusAxisMapping}).")
}


#' Resolve a `nautilus_review` into a routing set: overlay each decided candidate onto the base mapping.
#'
#' Un-reviewed deployments keep the base mapping (and its provenance); a reviewed deployment with a
#' decision is replaced by the chosen candidate's from/to (provenance "review"). A reviewable deployment
#' (clips were rendered) that offers a genuine choice but carries no decision is an error - the workflow
#' refuses to apply a handedness the reviewer has not confirmed.
#' @keywords internal
#' @noRd
.resolveReviewSet <- function(review) {
  base  <- attr(review, "review_base")
  cands <- attr(review, "review_candidates") %||% list()
  if (is.null(base)) .abort("This {.cls nautilus_review} carries no base mapping; re-run {.fn reviewTagMapping}.")
  by_id <- base$by_id %||% list()
  prov  <- base$provenance %||% stats::setNames(vector("list", length(by_id)), names(by_id))
  fam_axes <- list(accel = c("ax", "ay", "az"), gyro = c("gx", "gy", "gz"), mag = c("mx", "my", "mz"))
  fams_in  <- function(ft) if (is.null(ft) || !nrow(ft)) character(0) else names(Filter(function(ax) any(ft$from %in% ax), fam_axes))

  unresolved <- character(0); excluded <- character(0)
  for (i in seq_len(nrow(review))) {
    id <- as.character(review$id[i]); dec <- review$decision[i]
    # "exclude" is a universal disposition (valid on any row): the deployment is dropped from the output
    # entirely - the orientation cannot be trusted and no candidate is correct. Checked first, so it
    # overrides the undecided-reviewable guard and needs no candidate.
    if (!is.na(dec) && identical(.normDecision(dec), "exclude")) {
      excluded <- c(excluded, id); by_id[[id]] <- NULL; prov[[id]] <- NULL; next
    }
    cand_i <- cands[[id]]
    if (is.null(cand_i) || !length(cand_i)) next                  # single-path / no choice -> base applies as-is
    if (is.na(dec) || !nzchar(dec)) {
      if (isTRUE(review$n_clips[i] > 0)) unresolved <- c(unresolved, id)   # reviewable but undecided
      next
    }
    labs <- vapply(cand_i, `[[`, "", "label")
    k <- match(.normDecision(dec), .normDecision(labs))           # tolerant of the casing the user typed
    if (is.na(k)) .abort(c("Review decision {.val {dec}} for {.val {id}} is not one of its options.",
                           "i" = "Options: {.val {labs}}, or {.val Exclude} to drop the deployment."))
    ft <- cand_i[[k]]$from_to
    if (is.null(ft) || !nrow(ft)) { by_id[[id]] <- NULL; prov[[id]] <- NULL; next }   # e.g. "Raw" -> leave unmapped
    by_id[[id]] <- as.data.frame(ft[, c("from", "to")], stringsAsFactors = FALSE)
    prov[[id]]  <- stats::setNames(rep("review", length(fams_in(ft))), fams_in(ft))
  }
  if (length(unresolved))
    .abort(c("{length(unresolved)} reviewed deployment{?s} {?has/have} no decision yet: {.val {unresolved}}.",
             "i" = "Fill {.code review$decision} (see {.code print(review)}), then apply again."))
  list(single = NULL, by_id = by_id, producer = "reviewTagMapping", provenance = prov, excluded = excluded)
}
