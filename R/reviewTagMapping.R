#######################################################################################################
# Render annotated video clips for manual review of inferred IMU orientations #########################
#######################################################################################################

#' Render annotated video clips for manual review of inferred IMU orientations
#'
#' @description
#' Companion to \link{checkTagMapping}. Where `checkTagMapping()` validates the axis mapping against the
#' sensor DATA, `reviewTagMapping()` prepares the gold-standard HUMAN check, to be run **before**
#' \link{applyAxisMapping}. It triages the deployments whose mapping is uncertain (documented configs
#' that conflict with the data, gyroscope/accelerometer co-registration failures, ambiguous frames,
#' contradictory gyroscope evidence, or deployments the automated check could not analyse at all),
#' matches each to its on-animal video, auto-selects the clearest manoeuvres with
#' \link{findValidationSegments}, and renders short side-by-side overlay clips so a reviewer can confirm
#' the orientation by eye (a visible left roll must read as a left roll on the attitude indicator).
#'
#' Every candidate mapping is applied to a **transient in-memory copy** of the raw data (the inputs are
#' never modified) and a lightweight orientation pass derives the overlay series. When a gyroscope is
#' present it is fused into the attitude indicator with a complementary filter, so the dial tracks quick
#' banks tightly instead of lagging behind the footage (the accelerometer tilt alone must smooth out the
#' animal's own motion, which blurs fast rolls); this affects only the display, never the stored data or
#' the automated frame check. Whenever a flagged
#' deployment has a competing accel-frame hypothesis - a documented/data `conflict` - the clip is a
#' **side-by-side comparison**: the documented orientation ("Documented") beside the data-preferred,
#' corrected one ("Proposed"), each driving its own attitude indicator, so the reviewer simply keeps the
#' one that banks the same way as the animal. `ambiguous` deployments compare their distinct surviving
#' frames the same way; the remaining reasons (including `coreg_fail`) show a single indicator.
#'
#' It does NOT decide whether a mapping is correct - that judgement is the reviewer's, made from the
#' clips and then applied with \link{applyAxisMapping} (see the manual decision loop in Details). It
#' returns a MANIFEST recording what was selected, why, and where each clip was written.
#'
#' @param data The same un-oriented data passed to \link{checkTagMapping}: a list of `nautilus_tag`
#'   objects, a character vector of `.rds` paths, or one aggregated data.frame with an `id.col`.
#' @param mapping The list returned by \link{checkTagMapping} (or \link{consensusAxisMapping}); each
#'   element carries the per-deployment `frame_state` the triage reads.
#' @param video.metadata A data frame of video time spans as returned by \link{getVideoMetadata}
#'   (columns `ID`, `file`, `start`, `end`); used to locate the clip covering each manoeuvre.
#' @param output.dir An existing directory to write the review clips into.
#' @param configs The documented-orientation dictionary (as passed to \link{checkTagMapping}); required
#'   to render (and later apply) the documented config for `conflict` deployments.
#' @param base The mapping actually applied to each deployment - and overridden by the review decisions
#'   for the flagged ones. The result of \link{consensusAxisMapping} or \link{checkTagMapping}. Defaults
#'   to `mapping`. Pass the reconciled `consensusAxisMapping()` object here when you triage from the
#'   per-deployment `checkTagMapping()` evidence in `mapping` but want the consensus mapping as the base.
#' @param ids Deployments to review. `NULL` (default) auto-triages the suspect set (see Details); an
#'   explicit vector reviews exactly those.
#' @param include Triage reasons to auto-select. Default the four suspect classes; add
#'   `"low_confidence"` to also review resolved-but-weakly-supported frames.
#' @param types,n,window Passed to \link{findValidationSegments}. Default `types` here is
#'   `c("roll", "dive")` (not the three-type default): **roll** is the handedness cue that decides the
#'   mapping, and `"turn"` is deliberately excluded because it keys off heading, which the lightweight
#'   uncalibrated orientation pass cannot estimate reliably (its segments would be noise-selected).
#' @param clips.per.deployment Maximum clips rendered per deployment (default 3), chosen round-robin
#'   across `types` by rank.
#' @param max.candidates Maximum candidate frames shown side by side for an ambiguous deployment
#'   (default 3).
#' @param side,overlay.fps Passed to \link{renderOverlayVideo}.
#' @param codec Output codec family for the clips, `"hevc"` (default, smaller files) or `"h264"` (larger
#'   but plays essentially everywhere); passed to \link{renderOverlayVideo}. Both are QuickTime-compatible.
#' @param id.col,datetime.col Column names for the animal ID and datetime. Defaults `"ID"`/`"datetime"`.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal", or `2`/"detailed" (default).
#'
#' @details
#' Triage assigns each deployment its single most-severe reason (highest priority first): `conflict`
#' (the documented config disagrees with the data), `coreg_fail` (the gyroscope does not co-register with
#' the accelerometer - the co-die default `gyro = det(M)*M` was decisively rejected by the data),
#' `ambiguous` (the data did not uniquely resolve the frame and no config pins it), `gyro_inconsistent`
#' (the two gyroscope estimators disagree) and, opt-in,
#' `low_confidence` (the vertical resolved but the fore-aft axis leaned on handedness alone).
#' Deployments present in `data` and `video.metadata` but absent from `mapping` (the automated check
#' could not analyse them) are flagged `unanalysed` at top priority.
#'
#' **Reading a clip.** Each clip pairs the camera footage with a sensor dashboard: one or more labelled
#' attitude indicators (a rear-view shark that banks with roll), a depth trace and a gyroscope trace.
#' Find a moment where the animal clearly rolls or banks to one side in the video; the correct mapping is
#' the indicator that banks the **same** way. Heading is deliberately omitted - the lightweight,
#' uncalibrated orientation pass cannot estimate it reliably, so roll handedness is the cue.
#'
#' **Acting on the review (the decision sheet).** The function returns a `nautilus_review` - a decision
#' sheet with one row per flagged deployment and an editable `decision` column. It is self-contained: it
#' embeds the mapping to apply (`base`), the concrete candidate mappings, and the per-clip manifest, so
#' the workflow is fill-the-sheet, then hand the whole object back to \link{applyAxisMapping}:
#' \enumerate{
#'   \item Watch the clips (named `<id>_<type>_<rank>.mp4` in `output.dir`). For a `conflict`
#'     the dashboard compares two labelled candidates; note which banks like the animal.
#'   \item Set each flagged deployment's `decision` to one of its `options`, e.g.
#'     \code{review$decision[review$id == "PIN_CAM_04"] <- "Documented"}.
#'   \item Apply: \code{applyAxisMapping(data, mapping = review)}. Un-reviewed deployments take the base
#'     mapping; decided ones take the chosen candidate (recorded with `review` provenance).
#' }
#' Only deployments that rendered a genuine comparison need a decision; single-candidate reasons
#' (`gyro_inconsistent`, `low_confidence`, `unanalysed`) and deployments with no footage fall through to
#' the base mapping. `applyAxisMapping()` ERRORS if a reviewable deployment is left undecided, so a
#' handedness is never applied without confirmation. The clips are advisory; nothing downstream changes
#' until you apply.
#'
#' Set `decision` to the reserved value `"Exclude"` (valid on any row) when the orientation cannot be
#' trusted - neither candidate is right and it is not a correctable, constant mounting offset (a genuine
#' sensor fault or a non-rigid mount). `applyAxisMapping()` then drops that deployment from its output
#' entirely (no oriented file, absent from the returned list), so downstream steps simply never see it.
#' Decision values are matched case-insensitively (`"Exclude"`, `"exclude"` and `"Documented"`,
#' `"documented"` are equivalent), so you need not worry about the exact casing you type.
#' Note this is for *orientation*-unfixable cases only; data-hygiene problems such as a mistimed
#' detachment belong to the deployment-filtering step, not here.
#'
#' @return A `nautilus_review` object (invisibly): a data frame with one row per flagged deployment -
#'   `id`, `review_reason`, `priority`, `n_clips`, `status`, `options`, `decision` - where `status` is
#'   one of `rendered`, `no_video`, `no_coverage`, `no_segment`, `render_failed`, `options` lists the
#'   candidate labels, and `decision` is `NA` until you fill it. It carries the base mapping, the concrete
#'   candidate mappings, and the full per-clip manifest (`attr(x, "review_manifest")`) as attributes;
#'   pass it to \link{applyAxisMapping} as `mapping`. See \link{print.nautilus_review}.
#'
#' @seealso \link{checkTagMapping}, \link{consensusAxisMapping}, \link{applyAxisMapping},
#'   \link{findValidationSegments}, \link{renderOverlayVideo}, \link{getVideoMetadata}.
#' @examples
#' \dontrun{
#' mapping <- checkTagMapping(data, configs = configs)
#' meta    <- getVideoMetadata("./videos")
#' review  <- reviewTagMapping(data, mapping, meta, "./review/clips", configs = configs)
#' # watch the clips, fill in the decision column, then apply the reviewed mapping
#' review$decision[review$id == "PIN_CAM_04"] <- "Documented"
#' applyAxisMapping(data, mapping = review)
#' }
#' @export

reviewTagMapping <- function(data,
                             mapping,
                             video.metadata,
                             output.dir,
                             configs = NULL,
                             base = NULL,
                             ids = NULL,
                             include = c("conflict", "coreg_fail", "ambiguous", "gyro_inconsistent"),
                             types = c("roll", "dive"),
                             n = 3,
                             window = 20,
                             clips.per.deployment = 3,
                             max.candidates = 3,
                             side = "right",
                             overlay.fps = 5,
                             codec = c("hevc", "h264"),
                             id.col = "ID",
                             datetime.col = "datetime",
                             verbose = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  .assert_number(n, "n", min = 1); .assert_number(window, "window", min = 0)
  .assert_number(clips.per.deployment, "clips.per.deployment", min = 1)
  .assert_number(max.candidates, "max.candidates", min = 1)
  .assert_dir(output.dir, "output.dir")
  if (!is.list(mapping) || !length(mapping)) .abort("{.arg mapping} must be the (non-empty) list returned by {.fn checkTagMapping}.")
  include <- match.arg(include, c("conflict", "coreg_fail", "ambiguous", "gyro_inconsistent", "low_confidence"), several.ok = TRUE)
  codec <- match.arg(codec)
  .assert_columns(video.metadata, c("ID", "file", "start", "end"), "video.metadata")

  # heavy dependencies, checked once up front (before any transform / render work)
  if (!requireNamespace("av", quietly = TRUE)) .abort("The {.pkg av} package is required: {.code install.packages('av')}.")
  invisible(.ffmpegBin())

  r <- .resolveInput(data, id.col = id.col)
  data_ids <- as.character(r$ids)
  video_ids <- unique(as.character(video.metadata$ID))

  # the mapping applied to un-reviewed deployments and overridden by the review decisions for flagged
  # ones; resolved to a compact routing set now (frame_state stays in `mapping`, used only for triage).
  base_set <- .asAxisMappingSet(base %||% mapping)

  # ---- triage: which deployments to review, why, and in what order ----
  queue <- .triageMappings(mapping, data_ids, video_ids, include, ids)
  if (!nrow(queue)) {
    if (lvl >= 1L) cli::cli_alert_info("No deployments require review (nothing matched the triage).")
    return(invisible(.emptyReview(base_set, output.dir)))
  }

  .log_header(lvl, "reviewTagMapping", "Rendering clips for manual orientation review",
              bullets = sprintf("Flagged: %d deployment%s \u00b7 %s", nrow(queue),
                                if (nrow(queue) != 1) "s" else "",
                                paste(sprintf("%d %s", table(queue$review_reason), names(table(queue$review_reason))), collapse = ", ")),
              arrow = sprintf("Output: %s", output.dir))

  # one progress bar over the queue (transient; auto-suppressed non-interactively); at the detailed level a
  # per-deployment result line prints above it ONLY for deployments that actually produced clips
  pb_id <- if (lvl >= 1L) cli::cli_progress_bar(
    format = "{cli::pb_spin} Rendering review clips {cli::pb_bar} {cli::pb_percent} \u00b7 {cli::pb_current}/{cli::pb_total} deployments",
    total = nrow(queue), clear = TRUE, .envir = environment()) else NULL

  manifest <- vector("list", nrow(queue))
  cand_store <- list()
  for (qi in seq_len(nrow(queue))) {
    id <- queue$id[qi]; reason <- queue$review_reason[qi]; prio <- queue$priority[qi]
    res <- tryCatch(
      .reviewOneDeployment(id = id, reason = reason, priority = prio, r = r, mapping = mapping,
                           video.metadata = video.metadata, configs = configs, output.dir = output.dir,
                           types = types, n = n, window = window, clips.per.deployment = clips.per.deployment,
                           max.candidates = max.candidates, side = side, overlay.fps = overlay.fps, codec = codec,
                           id.col = id.col, datetime.col = datetime.col),
      error = function(e) .reviewOut(.manifestRow(id, reason, prio, "error", status = "render_failed")))
    manifest[[qi]] <- res$rows
    if (length(res$candidates)) cand_store[[id]] <- res$candidates
    if (lvl >= 2L) {
      nr <- sum(res$rows$status == "rendered")
      if (nr > 0) {
        mode <- if (res$rows$mapping_source[1] %in% c("documented_vs_data", "compare")) "compare" else "single"
        .log_ok(lvl, sprintf("%s \u00b7 %s \u00b7 %d clip%s (%s)", id, reason, nr, if (nr != 1) "s" else "", mode))
      }
    }
    if (!is.null(pb_id)) cli::cli_progress_update(id = pb_id, set = qi)
  }
  if (!is.null(pb_id)) cli::cli_progress_done(id = pb_id)

  result <- do.call(rbind, manifest)          # per-clip manifest, kept on the review object as an attribute
  rownames(result) <- NULL
  sheet   <- .buildReviewSheet(queue, result, cand_store)
  review  <- .newReview(sheet, candidates = cand_store, base_mapping = base_set,
                        manifest = result, output.dir = output.dir)

  if (lvl >= 1L) {
    n_clips  <- sum(result$status == "rendered")
    n_deps   <- sum(sheet$status == "rendered")
    no_video <- sheet$id[sheet$status == "no_video"]
    no_seg   <- sheet$id[sheet$status %in% c("no_segment", "no_coverage")]
    failed   <- sheet$id[sheet$status == "render_failed"]
    needs    <- .reviewDecisionsNeeded(sheet, cand_store)
    .log_summary(lvl)
    .log_detail(lvl, sprintf("%d clip%s across %d deployment%s", n_clips, if (n_clips != 1) "s" else "",
                             n_deps, if (n_deps != 1) "s" else ""))
    if (length(needs))    .log_arrow(lvl, sprintf("decisions needed (fill `decision`): %d (%s)", length(needs), .truncList(needs)))
    if (length(no_seg))   .log_detail(lvl, sprintf("no clear manoeuvre on camera: %d (%s)", length(no_seg), .truncList(no_seg)))
    if (length(no_video)) .log_detail(lvl, sprintf("no footage, not reviewable: %d (%s)", length(no_video), .truncList(no_video)))
    if (length(failed))   .log_detail(lvl, sprintf("render failed: %d (%s)", length(failed), .truncList(failed)))
    .log_done(lvl, n_clips, " review clip", if (n_clips != 1) "s", " written to ", output.dir)
    .log_runtime(lvl, start.time)
  }
  invisible(review)
}


################################################################################
# Triage (pure) ################################################################
################################################################################

#' Build the review queue: one row per flagged deployment (id, review_reason, priority), sorted.
#' @keywords internal
#' @noRd
.triageMappings <- function(mapping, data.ids, video.ids, include, ids.requested) {
  prio_of <- c(user_requested = 0L, conflict = 1L, unanalysed = 1L, coreg_fail = 2L,
               ambiguous = 3L, gyro_inconsistent = 4L, low_confidence = 5L)

  rows <- list()
  if (!is.null(ids.requested)) {
    # explicit request: review exactly these (a suspect reason wins over the generic label)
    for (id in as.character(ids.requested)) {
      rsn <- if (id %in% names(mapping)) .reviewReason(mapping[[id]], include) else NA_character_
      if (is.na(rsn)) rsn <- "user_requested"
      rows[[length(rows) + 1L]] <- data.frame(id = id, review_reason = rsn, stringsAsFactors = FALSE)
    }
  } else {
    for (id in names(mapping)) {
      rsn <- .reviewReason(mapping[[id]], include)
      if (!is.na(rsn)) rows[[length(rows) + 1L]] <- data.frame(id = id, review_reason = rsn, stringsAsFactors = FALSE)
    }
    # unanalysed: present in the data AND in the footage, but the automated check produced no result
    unanalysed <- setdiff(intersect(data.ids, video.ids), names(mapping))
    for (id in unanalysed)
      rows[[length(rows) + 1L]] <- data.frame(id = id, review_reason = "unanalysed", stringsAsFactors = FALSE)
  }
  if (!length(rows)) return(.emptyQueue())

  q <- do.call(rbind, rows)
  q$priority <- unname(prio_of[q$review_reason])
  q <- q[order(q$priority, q$id), , drop = FALSE]
  rownames(q) <- NULL
  q
}

#' The single most-severe in-scope review reason for one deployment, or NA.
#' @keywords internal
#' @noRd
.reviewReason <- function(elem, include) {
  fs <- elem$frame_state; fam <- elem$families
  if (is.null(fs)) return(NA_character_)
  cand <- character(0)
  if (length(fs$conflicts) > 0) cand <- c(cand, "conflict")
  # gyro/accel co-registration failure (the co-die default was decisively rejected by the data); a
  # benign reflection is NOT flagged (it co-registers and is handled automatically).
  if (identical(fs$coreg$status, "fail")) cand <- c(cand, "coreg_fail")
  if (!is.null(fs$survivors) && nrow(fs$survivors) > 1 &&
      isTRUE(fs$prior$status %in% c("absent", "unverifiable", "consistent"))) cand <- c(cand, "ambiguous")
  # the two fallback gyro estimators disagree. This only arises once the co-die default has already
  # FAILED co-registration, so such an element also carries `coreg_fail` (higher priority, which wins
  # under the default `include`); `gyro_inconsistent` is surfaced only when `coreg_fail` is excluded.
  if (identical(fam$gyro$status, "inconsistent")) cand <- c(cand, "gyro_inconsistent")
  if (identical(fs$vertical$status, "resolved") && identical(fs$surge$status, "ambiguous")) cand <- c(cand, "low_confidence")
  cand <- intersect(c("conflict", "coreg_fail", "ambiguous", "gyro_inconsistent", "low_confidence"),
                    intersect(cand, include))
  if (length(cand)) cand[1] else NA_character_
}


################################################################################
# Footage matching (pure) ######################################################
################################################################################

#' The clip (row of `video.metadata`) covering `peak_time` for deployment `id`, or NULL.
#' @keywords internal
#' @noRd
.matchClip <- function(video.metadata, id, peak_time) {
  cand <- video.metadata[as.character(video.metadata$ID) == id, , drop = FALSE]
  if (!nrow(cand)) return(NULL)
  hit <- cand[peak_time >= cand$start & peak_time <= cand$end, , drop = FALSE]
  if (!nrow(hit)) return(NULL)
  hit[1, , drop = FALSE]
}


################################################################################
# Per-deployment review (heavy: transform + orient + render) ###################
################################################################################

#' Review a single deployment end to end: resolve candidates, render clips, return manifest rows.
#' @keywords internal
#' @noRd
.reviewOneDeployment <- function(id, reason, priority, r, mapping, video.metadata, configs, output.dir,
                                 types, n, window, clips.per.deployment, max.candidates, side, overlay.fps, codec,
                                 id.col, datetime.col) {
  # footage FIRST (cheap): skip the expensive load + orientation for deployments with no video at all
  vmi <- video.metadata[as.character(video.metadata$ID) == id, , drop = FALSE]
  if (!nrow(vmi)) return(.reviewOut(.manifestRow(id, reason, priority, .mappingSourceFor(reason), status = "no_video")))

  i <- match(id, as.character(r$ids))
  if (is.na(i)) return(.reviewOut(.manifestRow(id, reason, priority, .mappingSourceFor(reason), status = "no_video")))
  raw <- r$get(i)
  if (!data.table::is.data.table(raw)) raw <- data.table::as.data.table(raw)
  axis_config <- tryCatch(.getMeta(raw)$tag$axis_config, error = function(e) NULL)   # for the "Documented" candidate
  fs <- if (id %in% names(mapping)) mapping[[id]]$frame_state else NULL

  # restrict to the footage-covered window (+ padding for orientation context) BEFORE orienting: only these
  # moments are reviewable, and orienting just this window rather than the whole multi-hour record is the
  # main speed-up.
  pad <- window + 5
  raw <- raw[.inAnyInterval(raw[[datetime.col]], vmi$start - pad, vmi$end + pad), ]
  if (!nrow(raw)) return(.reviewOut(.manifestRow(id, reason, priority, .mappingSourceFor(reason), status = "no_coverage")))

  # ---- resolve the candidate mapping(s) and the dashboard ----
  plan  <- .candidatePlan(reason, fs, configs, max.candidates)
  cands <- .reviewApplyCandidates(plan, id, mapping, axis_config, configs)   # concrete from/to per candidate (sheet + apply)
  fs_data <- raw                                      # the data used to FIND segments + RENDER

  if (plan$dashboard == "validation-compare") {
    base <- data.table::data.table(datetime = raw[[datetime.col]], depth = .col_or_na(raw, "depth"))
    cand_df <- data.frame(label = character(0), pitch = character(0), roll = character(0), stringsAsFactors = FALSE)
    ref <- NULL
    for (k in seq_along(plan$candidates)) {
      applied <- .applyCandidate(raw, plan$candidates[[k]]$apply, configs, mapping, id.col)
      o <- .liteOrientation(applied, .tagFs(applied, datetime.col), datetime.col = datetime.col)
      base[, paste0("pitch.c", k)] <- o$pitch
      base[, paste0("roll.c", k)]  <- o$roll
      cand_df <- rbind(cand_df, data.frame(label = plan$candidates[[k]]$label,
                                           pitch = paste0("pitch.c", k), roll = paste0("roll.c", k),
                                           stringsAsFactors = FALSE))
      if (k == 1L) { ref <- data.table::copy(applied); ref[, `:=`(roll = o$roll, heading = o$heading, vertical_velocity = o$vertical_velocity)] }
    }
    fs_data <- ref; render_data <- base; render_candidates <- cand_df
  } else {
    applied <- .applyCandidate(raw, plan$apply, configs, mapping, id.col)
    o <- .liteOrientation(applied, .tagFs(applied, datetime.col), datetime.col = datetime.col)
    applied[, `:=`(pitch = o$pitch, roll = o$roll, heading = o$heading, vertical_velocity = o$vertical_velocity)]
    fs_data <- applied; render_data <- applied; render_candidates <- NULL
  }

  # ---- restrict the manoeuvre search to the video-covered windows ----
  # Only footage-covered moments are reviewable, and a deployment usually films a small fraction of its
  # span; searching the whole record would pick the globally-clearest manoeuvres, almost all of them
  # off-camera. matchClip() below is the final guard for a peak that still falls in a between-clip gap.
  search <- fs_data[.inAnyInterval(fs_data[[datetime.col]], vmi$start, vmi$end), ]
  if (!nrow(search))
    return(.reviewOut(.manifestRow(id, reason, priority, plan$mapping_source, status = "no_segment"), cands))
  if (!(id.col %in% names(search))) search[[id.col]] <- id
  segs <- findValidationSegments(search, types = types, n = max(n, clips.per.deployment),
                                 window = window, id.col = id.col, datetime.col = datetime.col, verbose = FALSE)
  segs <- .selectClips(segs, types, clips.per.deployment)
  if (!nrow(segs))
    return(.reviewOut(.manifestRow(id, reason, priority, plan$mapping_source, status = "no_segment"), cands))

  caption <- .reviewCaption(reason, plan)
  rows <- vector("list", nrow(segs))
  for (j in seq_len(nrow(segs))) {
    seg <- segs[j, ]
    clip <- .matchClip(video.metadata, id, seg$peak_time)
    if (is.null(clip)) {
      rows[[j]] <- .manifestRow(id, reason, priority, plan$mapping_source, type = seg$type, rank = seg$rank,
                                peak_time = seg$peak_time, start = seg$start, end = seg$end, status = "no_coverage")
      next
    }
    out <- file.path(output.dir, sprintf("%s_%s_%d.mp4", id, seg$type, seg$rank))
    ok <- tryCatch({
      renderOverlayVideo(video = clip$file, data = render_data, output = out, dashboard = plan$dashboard,
                         video.start = clip$start, start = seg$start, end = seg$end,
                         caption = caption, candidates = render_candidates,
                         side = side, overlay.fps = overlay.fps, codec = codec, verbose = FALSE)
      TRUE
    }, error = function(e) FALSE)
    status <- if (ok && file.exists(out)) "rendered" else "render_failed"
    rows[[j]] <- .manifestRow(id, reason, priority, plan$mapping_source, type = seg$type, rank = seg$rank,
                              peak_time = seg$peak_time, start = seg$start, end = seg$end,
                              source_video = basename(clip$file), clip = if (status == "rendered") out else NA_character_,
                              status = status)
  }
  .reviewOut(do.call(rbind, rows), cands)
}


################################################################################
# Candidate / mapping resolution (internal) ####################################
################################################################################

#' Decide the dashboard, the mapping(s) to apply, and (for comparisons) the labelled candidate frames.
#'
#' The common flagged reasons become a SIDE-BY-SIDE comparison so the reviewer can pick the mapping that
#' banks like the animal in the video (rather than judging a single indicator in isolation):
#'   * conflict: the DOCUMENTED config (as recorded) vs the DATA-PREFERRED right-handed frame
#'     (the corrected candidate `fs$survivors[1,]`);
#'   * ambiguous: the distinct surviving right-handed frames (capped at `max.candidates`).
#' The remaining reasons show a single indicator (no competing accel-frame candidate): `unanalysed` -> the
#' raw frame; `coreg_fail` and everything else -> the resolved/proposed mapping.
#' @keywords internal
#' @noRd
.candidatePlan <- function(reason, fs, configs, max.candidates) {
  # conflict -> documented (as recorded) vs the data-preferred (corrected) frame
  if (identical(reason, "conflict") && !is.null(configs) &&
      !is.null(fs) && !is.null(fs$survivors) && nrow(fs$survivors) >= 1L) {
    best <- fs$survivors[1, , drop = FALSE]
    cands <- list(list(apply = list(type = "configs"),                        label = "Documented"),
                  list(apply = list(type = "ft", ft = .survivorToAccelFt(best)), label = "Proposed"))
    return(list(dashboard = "validation-compare", mapping_source = "documented_vs_data", candidates = cands))
  }
  # ambiguous -> the distinct surviving right-handed frames, side by side
  if (reason == "ambiguous" && !is.null(fs) && !is.null(fs$survivors)) {
    s <- fs$survivors
    reps <- s[!duplicated(paste(s$newZ_col, s$newZ_sign)), , drop = FALSE]
    if (nrow(reps) >= 2L) {
      reps <- utils::head(reps, max.candidates)
      cands <- lapply(seq_len(nrow(reps)), function(k) {
        sr <- reps[k, ]
        list(apply = list(type = "ft", ft = .survivorToAccelFt(sr)),
             label = sprintf("Z=%s%s", if (sr$newZ_sign < 0) "-" else "+", sr$newZ_col))
      })
      return(list(dashboard = "validation-compare", mapping_source = "compare", candidates = cands))
    }
  }
  if (reason == "unanalysed")
    return(list(dashboard = "validation", mapping_source = "identity_raw", apply = list(type = "identity")))
  # gyro_inconsistent / low_confidence / user_requested / degenerate cases: the resolved mapping, single
  list(dashboard = "validation", mapping_source = "proposal", apply = list(type = "mapping"))
}

#' Apply one candidate mapping to a fresh copy of the raw data (delegates to applyAxisMapping; silent).
#' @keywords internal
#' @noRd
.applyCandidate <- function(raw, spec, configs, mapping, id.col) {
  copy <- data.table::copy(raw)
  if (spec$type == "identity") return(copy)
  args <- list(data = list(copy), id.col = id.col, return.data = TRUE,
               check.handedness = FALSE, verbose = FALSE)
  if (spec$type == "configs") {
    if (is.null(configs)) return(copy)            # nothing to apply; fall back to raw frame
    args$configs <- configs
  } else if (spec$type == "ft") {
    args$mapping <- spec$ft
  } else {                                          # "mapping": the checkTagMapping/consensus object
    args$mapping <- mapping
  }
  out <- tryCatch(do.call(applyAxisMapping, args)[[1]], error = function(e) copy)
  if (!data.table::is.data.table(out)) out <- data.table::as.data.table(out)
  out
}

#' A survivor row -> an accelerometer from/to mapping (raw axis -> signed body channel).
#' @keywords internal
#' @noRd
.survivorToAccelFt <- function(s) {
  sgn <- function(name, v) paste0(if (v < 0) "-" else "", name)
  data.frame(from = c(s$newX_col, s$newY_col, s$newZ_col),
             to   = c(sgn("ax", s$newX_sign), sgn("ay", s$newY_sign), sgn("az", s$newZ_sign)),
             stringsAsFactors = FALSE)
}

#' Default mapping_source label for a reason (used on degenerate manifest rows).
#' @keywords internal
#' @noRd
.mappingSourceFor <- function(reason) {
  switch(reason,
         conflict = "documented_config",
         ambiguous = "compare",
         unanalysed = "identity_raw",
         "proposal")
}

#' One-line caption for the validation dashboards (the prominent "what to look for" guidance).
#' @keywords internal
#' @noRd
.reviewCaption <- function(reason, plan) {
  if (identical(plan$dashboard, "validation-compare")) {
    lead <- if (identical(reason, "conflict"))
      "Watch a clear roll: the indicator that banks the SAME way as the shark is the correct mapping"
    else "Ambiguous frame: which indicator banks the same way as the shark?"
    return(lead)
  }
  if (identical(reason, "coreg_fail"))
    return("Gyroscope does not co-register with the accelerometer - verify the proposed gyro mapping banks the same way as the shark (or re-check the tag).")
  sprintf("Confirm the indicator banks the same way as the shark (flagged: %s)", reason)
}


################################################################################
# Clip selection + manifest (internal) #########################################
################################################################################

#' Pick up to `k` segments, round-robin across types by rank (a diverse turn/roll/dive set).
#' @keywords internal
#' @noRd
.selectClips <- function(segs, types, k) {
  if (is.null(segs) || !nrow(segs)) return(segs)
  picked <- list()
  ranks <- sort(unique(segs$rank))
  for (rk in ranks) {
    for (ty in types) {
      hit <- segs[segs$type == ty & segs$rank == rk, , drop = FALSE]
      if (nrow(hit)) picked[[length(picked) + 1L]] <- hit[1, , drop = FALSE]
      if (length(picked) >= k) break
    }
    if (length(picked) >= k) break
  }
  out <- do.call(rbind, picked)
  rownames(out) <- NULL
  out
}

#' Build one manifest row with the canonical schema.
#' @keywords internal
#' @noRd
.manifestRow <- function(id, review_reason, priority, mapping_source,
                         type = NA_character_, rank = NA_integer_, peak_time = as.POSIXct(NA),
                         start = as.POSIXct(NA), end = as.POSIXct(NA),
                         source_video = NA_character_, clip = NA_character_, status) {
  data.frame(id = id, review_reason = review_reason, priority = as.integer(priority),
             mapping_source = mapping_source, type = type, rank = as.integer(rank),
             peak_time = peak_time, start = start, end = end,
             source_video = source_video, clip = clip, status = status, stringsAsFactors = FALSE)
}

#' Empty manifest / queue with the canonical schema.
#' @keywords internal
#' @noRd
.emptyManifest <- function() .manifestRow(character(0), character(0), integer(0), character(0), status = character(0))

#' @keywords internal
#' @noRd
.emptyQueue <- function() data.frame(id = character(0), review_reason = character(0), priority = integer(0), stringsAsFactors = FALSE)

#' A column from a data.table, or an NA vector of the right length if absent.
#' @keywords internal
#' @noRd
.col_or_na <- function(dt, col) if (col %in% names(dt)) dt[[col]] else rep(NA_real_, nrow(dt))

#' Compact a long id vector for a one-line summary: the first `k`, then "+N more".
#' @keywords internal
#' @noRd
.truncList <- function(x, k = 8L) {
  if (length(x) <= k) return(paste(x, collapse = ", "))
  paste0(paste(utils::head(x, k), collapse = ", "), sprintf(" +%d more", length(x) - k))
}


################################################################################
# Review object (nautilus_review): the per-deployment decision sheet ###########
################################################################################

#' Wrap one deployment's per-clip manifest rows together with its concrete candidate mappings.
#' @keywords internal
#' @noRd
.reviewOut <- function(rows, candidates = list()) list(rows = rows, candidates = candidates)

#' Normalise a decision string for tolerant (case- and whitespace-insensitive) matching.
#' @keywords internal
#' @noRd
.normDecision <- function(x) tolower(trimws(as.character(x)))

#' Concrete apply-time from/to for each candidate of a compare plan (empty list for single-path plans).
#'
#' The decision a reviewer makes is which candidate's mapping to APPLY, so each candidate carries a
#' COMPLETE from/to - identical to what `applyAxisMapping()` would receive directly: `Documented` -> the
#' recorded config; `Proposed` -> the deployment's full inferred proposal; an ambiguous `Z=...` -> that
#' survivor's accelerometer frame. Single-path reasons (gyro_inconsistent / low_confidence / unanalysed)
#' return nothing: they carry no A/B choice, so the base mapping applies unchanged.
#' @keywords internal
#' @noRd
.reviewApplyCandidates <- function(plan, id, mapping, axis_config, configs) {
  if (!identical(plan$dashboard, "validation-compare")) return(list())
  lapply(plan$candidates, function(cd) {
    ft <- if (identical(cd$apply$type, "configs")) {
            if (is.null(configs) || is.null(axis_config)) NULL else .expandAxisConfigForTag(axis_config, configs, id)
          } else if (identical(cd$label, "Proposed") && id %in% names(mapping) && !is.null(mapping[[id]]$proposal)) {
            mapping[[id]]$proposal
          } else if (identical(cd$apply$type, "ft")) {
            cd$apply$ft
          } else NULL
    list(label = cd$label,
         from_to = if (!is.null(ft) && nrow(ft)) as.data.frame(ft[, c("from", "to")], stringsAsFactors = FALSE) else NULL)
  })
}

#' Collapse the per-clip manifest to one row per flagged deployment (the human-facing decision sheet).
#' @keywords internal
#' @noRd
.buildReviewSheet <- function(queue, manifest, cand_store) {
  rows <- lapply(seq_len(nrow(queue)), function(i) {
    id <- queue$id[i]
    mr <- if (!is.null(manifest)) manifest[manifest$id == id, , drop = FALSE] else NULL
    st <- if (!is.null(mr) && any(mr$status == "rendered")) "rendered"
          else if (!is.null(mr) && nrow(mr)) mr$status[1] else "no_video"
    cs <- cand_store[[id]]
    opts <- if (length(cs)) paste(vapply(cs, `[[`, "", "label"), collapse = " | ") else ""
    data.frame(id = id, review_reason = queue$review_reason[i], priority = queue$priority[i],
               n_clips = as.integer(if (!is.null(mr)) sum(mr$status == "rendered") else 0L),
               status = st, options = opts, decision = NA_character_, stringsAsFactors = FALSE)
  })
  out <- do.call(rbind, rows); rownames(out) <- NULL; out
}

#' The reviewable deployments (clips rendered) that still offer a genuine A/B choice.
#' @keywords internal
#' @noRd
.reviewDecisionsNeeded <- function(sheet, cand_store) {
  ids <- sheet$id[sheet$status == "rendered"]
  ids[vapply(ids, function(i) length(cand_store[[i]]) >= 2L, logical(1))]
}

#' Construct a `nautilus_review`: the decision sheet, plus everything applyAxisMapping() needs (as
#' attributes: the concrete candidates, the base routing set, and the per-clip manifest).
#' @keywords internal
#' @noRd
.newReview <- function(sheet, candidates, base_mapping, manifest, output.dir) {
  structure(sheet, class = c("nautilus_review", "data.frame"),
            review_candidates = candidates, review_base = base_mapping,
            review_manifest = manifest, review_output = output.dir)
}

#' Empty review (nothing flagged), still carrying the base so applyAxisMapping() maps every deployment.
#' @keywords internal
#' @noRd
.emptyReview <- function(base_mapping = NULL, output.dir = NULL) {
  sheet <- data.frame(id = character(0), review_reason = character(0), priority = integer(0),
                      n_clips = integer(0), status = character(0), options = character(0),
                      decision = character(0), stringsAsFactors = FALSE)
  .newReview(sheet, candidates = list(), base_mapping = base_mapping,
             manifest = .emptyManifest(), output.dir = output.dir)
}

#' Print a nautilus axis-mapping review
#'
#' Shows the decision sheet: one row per flagged deployment, the candidate `options`, and which rows
#' still need a `decision`. Fill the `decision` column (e.g. `x$decision[x$id == "PIN_CAM_04"] <-
#' "Documented"`) and pass the object to \link{applyAxisMapping} as `mapping`.
#' @param x A `nautilus_review` object returned by \link{reviewTagMapping}.
#' @param ... Ignored.
#' @return `x`, invisibly.
#' @export
print.nautilus_review <- function(x, ...) {
  cli::cli_h1("nautilus axis-mapping review")
  n <- nrow(x)
  if (!n) { cli::cli_alert_info("No deployments were flagged for review."); return(invisible(x)) }
  required <- .reviewDecisionsNeeded(x, attr(x, "review_candidates") %||% list())    # rows that require a call
  dec_req  <- x$decision[match(required, x$id)]
  pending  <- required[is.na(dec_req) | !nzchar(dec_req)]                             # required and still undecided
  n_excl   <- sum(!is.na(x$decision) & .normDecision(x$decision) == "exclude")
  excl_note <- if (n_excl > 0L) sprintf(" (%d marked Exclude)", n_excl) else ""
  cli::cli_text("{n} flagged deployment{?s}; {length(pending)} still need{?s/} a decision.{excl_note}")
  cli::cli_text("Fill {.field decision} with an option (or {.val Exclude}), then pass this to {.fn applyAxisMapping} as {.arg mapping}.")
  disp <- data.frame(id = x$id, reason = x$review_reason, clips = x$n_clips, status = x$status,
                     options = ifelse(nzchar(x$options), x$options, "-"),
                     decision = ifelse(is.na(x$decision) | !nzchar(x$decision),
                                       ifelse(x$id %in% pending, "<choose>", "-"), x$decision),
                     stringsAsFactors = FALSE)
  print(disp, row.names = FALSE)
  od <- attr(x, "review_output")
  if (!is.null(od)) cli::cli_text("Clips in {.file {od}}; per-clip detail in {.code attr(x, \"review_manifest\")}.")
  invisible(x)
}


#######################################################################################################
#######################################################################################################
#######################################################################################################
