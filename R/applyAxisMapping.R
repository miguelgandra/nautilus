#######################################################################################################
# Apply an IMU axis mapping to already-imported data ##################################################
#######################################################################################################

#' Re-map IMU sensor axes without re-reading the raw files
#'
#' @description
#' Applies a signed-permutation axis mapping (axis swaps and/or sign flips, with the literal `"NA"`
#' to drop a faulty axis) to the accelerometer, gyroscope, and magnetometer channels of
#' already-imported data. Orientation is a single, explicit step after [importTagData] (which imports
#' raw): this is where the IMU is rotated into the animal-centric (e.g. NED) frame, on in-memory data or
#' saved `.rds` files, without re-parsing the original CSVs.
#'
#' `mapping` is polymorphic - it accepts the diagnostic objects produced upstream and routes the right
#' per-deployment mapping to each dataset automatically, so the QC workflow flows straight through:
#' \preformatted{
#' qc <- checkTagMapping(data_files)            # per-deployment proposals
#' qc <- consensusAxisMapping(qc)               # rescue unresolved ones from group consensus
#' applyAxisMapping(data = data_files, mapping = qc)
#' }
#' Accepted shapes: the output of [checkTagMapping] (each deployment's own `$proposal`), the output of
#' [consensusAxisMapping] (each deployment's reconciled `$mappings`, carrying the per-family
#' self/consensus origin), a single `from`/`to` data.frame (applied to every dataset), or a named list
#' of such data.frames (one per deployment id). The origin of each remapped family is inferred from the
#' object and written to the metadata audit trail - there is no `source` argument to set by hand.
#'
#' @details
#' The transform is tracked in the object's metadata (`tagMetadata(x)$axis_mapping`) as the *net* signed
#' permutation currently applied, so re-mapping composes exactly and is idempotent:
#' \itemize{
#'   \item \strong{Absolute} (default, `relative = FALSE`): the mapping describes the raw (chip) ->
#'     canonical tag frame. If a mapping is already applied, only the difference is applied, so
#'     re-applying the same mapping is a no-op and you never double-correct. This is the right mode for
#'     a documented `configs` dictionary and for [checkTagMapping] / [consensusAxisMapping] output.
#'   \item \strong{Relative} (`relative = TRUE`): the mapping is applied on top of the current state
#'     (for manual incremental tweaks).
#' }
#' Deployments that a structured object leaves unresolved (an empty mapping) are passed through
#' unchanged with a notice; datasets with no matching mapping are likewise left untouched.
#'
#' \strong{Family completion (gyro/mag).} A mapping that specifies only the accelerometer axes (as the
#' documented `configs` typically do) is completed automatically before it is applied: the gyroscope map
#' is \emph{derived} from the accel map as `gyro = det(M) * M` (M = the accel signed-permutation matrix).
#' The gyroscope measures an axial vector (a pseudovector), so under a reflected accel frame
#' (`det(M) = -1`) it must carry the extra sign flip to stay co-registered with the accelerometer - a
#' plain copy of the accel permutation would leave the gyro left-handed relative to it. The magnetometer
#' is \strong{not} inferred from the accel map: vendor docs show its axis convention often differs from the
#' accelerometer's, so with no explicit `mx/my/mz` rows it is left unchanged (identity), to be handled by
#' its own documented map or a dedicated magnetometer calibration. Explicit `gx/gy/gz` (or `mx/my/mz`) rows in the
#' mapping always override the derived gyro default (for hardware whose gyro sits on an independent die
#' with its own convention). The co-die gyro default is a configurable convention, not a universal law:
#' only `det(M)` (the pseudovector sign rule) is physics.
#'
#' @param data Input data: a `nautilus_tag` / data.frame, a (named) list of them, or a character
#'   vector of paths to `.rds` files (loaded one at a time). The output of [importTagData] is expected.
#' @param mapping The mapping(s) to apply (provide this **or** `configs`). One of: a `from`/`to`
#'   data.frame (applied to every dataset); a named list of `from`/`to` data.frames (one per deployment
#'   id); the result of [checkTagMapping]; the result of [consensusAxisMapping]; or a `nautilus_review`
#'   from [reviewTagMapping] (its filled decisions are overlaid on the review's base mapping; a
#'   deployment left undecided but reviewable is an error, and a decision of `"Exclude"` drops that
#'   deployment from the output entirely). For the structured forms, each deployment's
#'   mapping is routed to the dataset with the matching id. `from` is a raw axis (`ax`/`ay`/`az`,
#'   `gx`/`gy`/`gz`, `mx`/`my`/`mz`); `to` is the destination axis, optionally sign-flipped (e.g.
#'   `"-ay"`), or `"NA"` to set a faulty axis to `NA`. Default `NULL`.
#' @param configs A named dictionary of documented configurations - config name -> `from`/`to`
#'   data.frame - applied by looking up each tag's `axis_config` metadata (set at import from the
#'   `axis_config` column of \code{\link{metadataColumns}}). The apply-the-documented-config path: use it
#'   instead of `mapping` when the orientation is known and you are not inferring it from the data. A tag
#'   with a blank/absent `axis_config` is left unchanged; a config name not in the dictionary is an
#'   error. Default `NULL`.
#' @param id.col Character. Name of the ID column, used to match each dataset to its mapping. Default "ID".
#' @param datetime.col Character. Name of the timestamp column, used only to estimate the sampling rate
#'   for the accelerometer/gyroscope co-registration check (see `check.handedness`). Default "datetime".
#' @param relative Logical. If `FALSE` (default), `mapping` is the absolute raw->frame transform and only
#'   the difference from the currently-applied mapping is applied. If `TRUE`, it is applied on top of the
#'   current state. Default `FALSE`.
#' @param check.handedness Logical. If `TRUE` (default), a per-family reflection (determinant -1) is
#'   noted descriptively in the per-dataset log and recorded in `am_new$determinant`. A reflection is a
#'   benign device convention (some raw frames are genuinely left-handed), **not** an error: the gyro is
#'   co-registered automatically (its map carries the matching `det(M)` sign; see Details), so handedness
#'   is preserved - a reflection alone never warns. When accelerometer and gyroscope are both present and
#'   mapped, this also runs the frame-level **accel/gyro co-registration check** (Pearson correlation of
#'   the gravity-direction rate `d(ghat)/dt` against the gyro-predicted `-omega x ghat`), records it in
#'   `tagMetadata(x)$axis_mapping$coreg_corr`, and warns **only** on a decisive mismatch (correlation
#'   below 0.2 with enough rotation) - a genuine family mis-registration (e.g. an independent gyro die
#'   whose convention the co-die default got wrong), never a mere reflection (which scores ~ +1). Set
#'   `FALSE` to skip both the descriptive note and the co-registration check.
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
#' @param verbose Verbosity level: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal", or
#'   `2`/"detailed" (default). Defaults to `"detailed"`.
#'
#' @return If `return.data = TRUE`, a named list of remapped `nautilus_tag` objects (one per
#'   individual); if `return.data = FALSE`, a character vector of the written `.rds` file paths. The net
#'   mapping, its producer and the per-family origin are recorded in each object's
#'   `tagMetadata(x)$axis_mapping` (`source`, `provenance`). Deployments a `nautilus_review` marks
#'   `"Exclude"` are omitted entirely (no file written; absent from the returned list, which carries
#'   their ids in `attr(., "excluded")`).
#'
#' @seealso [importTagData], [checkTagMapping], [consensusAxisMapping], [reviewTagMapping], [tagMetadata]
#' @examples
#' \dontrun{
#' files <- list.files("imported", pattern = "\\.rds$", full.names = TRUE)
#'
#' # 1) route each deployment's inferred mapping straight through
#' qc <- checkTagMapping(files)
#' qc <- consensusAxisMapping(qc)          # rescue ambiguous ones from group consensus
#' oriented <- applyAxisMapping(data = files, mapping = qc)
#'
#' # 2) or apply a documented configuration via the tag's axis_config metadata
#' configs <- list("CATS Camera" = data.frame(from = c("ax", "ay", "az"),
#'                                             to   = c("ay", "-ax", "az")))
#' oriented <- applyAxisMapping(data = files, configs = configs)
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
    .log_runtime(lvl, start.time)
  }

  # drop excluded deployments (their NULL slots), index-aligned across both accumulators
  keep <- if (return.data) !vapply(results, is.null, logical(1)) else !vapply(saved, is.null, logical(1))
  out <- .collectOutput(results[keep], saved[keep], return.data, r$ids[keep])
  if (length(excluded_out)) attr(out, "excluded") <- excluded_out
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
