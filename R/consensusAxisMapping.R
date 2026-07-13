#######################################################################################################
# Cross-deployment consensus axis mapping #############################################################
#######################################################################################################

#' Reconcile axis-mapping solutions across deployments of the same tag unit
#'
#' @description
#' The chip-to-housing axis orientation of a tag is a manufacturing constant: it is shared across
#' every deployment of that physical unit. For the towed / fin-clamped, hydrodynamically self-righting
#' tags this package targets, the housing-to-body orientation is also physically constrained to
#' "upright (Z up) and forward (X forward)" regardless of the attachment site (dorsal, left or right
#' pectoral), so the full raw -> body transform is a single per-unit constant.
#'
#' This function groups the per-deployment results of \code{\link{checkTagMapping}} by a metadata key
#' (\code{group.by}, default the physical-unit \code{package_id}) and, within each group, builds a
#' confidence-weighted consensus mapping from the deployments that resolved unambiguously. That
#' consensus is then propagated to weaker / ambiguous deployments of the same group - but only when it
#' is consistent with their own evidence - so a dive-rich deployment can rescue a flat-swimming or short
#' one. Disagreement between confidently-resolved deployments of the same group is flagged as a
#' \emph{conflict} (a likely sign that the circuit board was swapped, replaced, or remounted in a
#' different orientation, or that the grouping key is shared by two physically different units).
#'
#' @details
#' Only \emph{confident} deployments vote: an accelerometer vote is cast only when \code{checkTagMapping}
#' resolved all three axes (its \code{$proposal} carries a complete accelerometer triplet), and a
#' gyroscope / magnetometer vote only when that family was decisively resolved. Within a unit, the
#' modal signed permutation per family is the consensus.
#'
#' Propagation is \emph{consistency-gated}: an ambiguous deployment adopts the unit consensus for the
#' accelerometer only if that mapping lies within its own tied candidate set (\code{$candidates}); a
#' deployment that already resolved a family keeps its own mapping (never overridden). This guarantees
#' the consensus can only \emph{fill} ambiguity, never contradict a deployment's own data.
#'
#' @param results The output of \code{\link{checkTagMapping}} (a named list, one element per
#'   deployment), a list of such outputs, or any nested list whose leaves are \code{checkTagMapping}
#'   result elements (carrying \code{$proposal}). Each element should carry the grouping keys added by
#'   \code{checkTagMapping} (\code{$package_id}, \code{$tag}, \code{$type}); a leaf missing the chosen
#'   key is treated as its own standalone group.
#' @param group.by Character vector naming the metadata key(s) deployments are grouped by, one or more
#'   of \code{"package_id"} (default), \code{"logger_id"}, \code{"tag"} (model) and \code{"type"}. Pass
#'   several for a composite key (e.g. \code{c("tag", "type")}). Pick the unit whose axis orientation is
#'   actually constant: \code{"package_id"} (the physical housing/placement) is the safe default;
#'   \code{c("tag", "type")} pools all deployments of a model+type (best when a manufacturer standardises
#'   placement); \code{"logger_id"} follows the chip when boards are swapped/remounted between housings.
#'   In all cases the dissenting-outlier detection flags within-group deployments that do not fit.
#' @param min.agreement Numeric in (0, 1]. Within a group, a family's consensus is emitted only when the
#'   most common signed permutation is supported by at least this fraction of the deployments that
#'   resolved that family. Default 0.6.
#' @param min.voters Integer >= 1. A family's consensus is emitted only when at least this many
#'   deployments of the group resolved that family (so a lone resolution is never propagated). Default 2.
#' @param verbose Verbosity level: \code{FALSE}/\code{0}/"quiet", \code{TRUE}/\code{1}/"normal", or
#'   \code{2}/"detailed" (default).
#'
#' @return A list with:
#'   \itemize{
#'     \item \code{mappings}: a named list (one per deployment) of \code{from}/\code{to} data.frames
#'       giving each deployment's final, reconciled mapping, ready for \code{\link{applyAxisMapping}}
#'       (empty data.frame where still unresolved).
#'     \item \code{provenance}: a data.frame, one row per deployment, with the \code{group} key (and
#'       \code{package_id} for reference) and, per family (\code{accel}/\code{gyro}/\code{mag}), how it
#'       was resolved: \code{"self"} (own evidence), \code{"consensus"} (filled from the group),
#'       \code{"conflict"} (consensus contradicts the deployment's own candidates), \code{"ambiguous"}
#'       (unresolved), or \code{"none"} (family absent).
#'     \item \code{groups}: a per-group list (keyed by the \code{group.by} value) reporting, for each
#'       family, the number of voters, the winning \code{agreement} fraction, the number of distinct
#'       solutions, a \code{conflict} flag, the contributing deployment ids, and the consensus mapping.
#'     \item \code{n_groups}, \code{n_deployments}, \code{n_filled}, \code{n_conflicts}: run tallies.
#'   }
#' @seealso \code{\link{checkTagMapping}}, \code{\link{applyAxisMapping}}
#' @examples
#' \dontrun{
#' files <- list.files("imported", pattern = "\\.rds$", full.names = TRUE)
#'
#' # Pool per-deployment proposals across deployments of the same physical unit
#' qc  <- checkTagMapping(files)
#' rec <- consensusAxisMapping(qc, group.by = "package_id")
#' rec$provenance                 # per-family origin (self / consensus / conflict)
#' oriented <- applyAxisMapping(data = files, mapping = rec)
#' }
#' @export

consensusAxisMapping <- function(results, group.by = "package_id", min.agreement = 0.6,
                                 min.voters = 2, verbose = "detailed") {

  ##############################################################################
  # Validate arguments #########################################################
  ##############################################################################

  if (!is.numeric(min.agreement) || length(min.agreement) != 1 || is.na(min.agreement) ||
      min.agreement <= 0 || min.agreement > 1) {
    .abort("{.arg min.agreement} must be a single number in (0, 1].")
  }
  if (!is.numeric(min.voters) || length(min.voters) != 1 || is.na(min.voters) || min.voters < 1) {
    .abort("{.arg min.voters} must be a single integer >= 1.")
  }
  min.voters <- as.integer(round(min.voters))
  # group.by: one or more of the grouping keys carried by checkTagMapping (composite key if > 1).
  group.by <- .validateGroupBy(group.by)
  gb_desc <- paste(group.by, collapse = " x ")
  lvl <- .verbosity(verbose)
  start.time <- Sys.time()

  nodes <- .collectMappingNodes(results)
  if (!length(nodes)) {
    .abort("No {.fn checkTagMapping} results with a {.field $proposal} were found in {.arg results}.")
  }

  ##############################################################################
  # Group by the chosen key(s) (default package_id) ############################
  ##############################################################################

  # the grouping key is the composite of the requested `group.by` field(s). A deployment missing any
  # of them cannot be reconciled against anything: each becomes its own standalone group (reported
  # unchanged), never silently merged.
  node_grp <- vapply(nodes, function(n) .compositeGroupKey(group.by, function(f) n[[f]]), character(1))
  if (all(is.na(node_grp))) {
    cli::cli_warn(c("No deployment carries the {.arg group.by} field{?s} {.val {group.by}}.",
                    "i" = "Re-run {.fn checkTagMapping} so it records the grouping keys, or pick another {.arg group.by}."))
  }
  split_key <- ifelse(is.na(node_grp),
                      paste0("<unknown:", vapply(nodes, function(n) n$id, character(1)), ">"), node_grp)
  has_unknown <- anyNA(node_grp)
  units <- split(nodes, factor(split_key, levels = unique(split_key)))

  fam_letters <- c(accel = "a", gyro = "g", mag = "m")
  fam_axes    <- list(a = c("ax", "ay", "az"), g = c("gx", "gy", "gz"), m = c("mx", "my", "mz"))

  .log_header(lvl, "consensusAxisMapping",
              "Reconciling axis mappings across related deployments",
              bullets = c(sprintf("Deployments: %d", length(nodes)),
                          sprintf("Groups (by %s): %d", gb_desc, sum(!startsWith(names(units), "<unknown:")))),
              arrow = sprintf("Rule: per-group consensus (agreement >= %.0f%%, voters >= %d)",
                              100 * min.agreement, min.voters))

  ##############################################################################
  # Reconcile each unit ########################################################
  ##############################################################################

  out_mappings <- list(); prov_rows <- list(); group_reports <- list()
  n_filled <- 0L; n_conflicts <- 0L
  skipped_standalone <- character(0)   # labels of size-1 (standalone) groups, listed before the summary
  outlier_ids <- character(0)          # deployments that self-resolved a family against the group majority
  sub_arrow <- if (cli::is_utf8_output()) "\u21b3" else cli::symbol$arrow_right   # nested-detail marker

  # the multi-line CONFLICT block, shown in full at level >= 1 in both the detailed and compact layouts.
  # CONFLICT stays upper-case as a severity token; the rest of the prose is lower-case.
  emit_conflict_block <- function(fn, fs, gb_desc, grp_label) {
    cli::cli_alert_danger(
      "{fn}: CONFLICT - confident deployments disagree ({fs$n_distinct} distinct solutions among {fs$n} voters).")
    for (k in seq_along(fs$counts)) {
      cli::cli_text("    {cli::symbol$bullet} {.val {names(fs$counts)[k]}}: {as.integer(fs$counts)[k]} deployment(s)")
    }
    cli::cli_text(paste0(
      "    physically check whether the circuit board was swapped, replaced, or remounted in a ",
      "different orientation inside the housing for {gb_desc} {.val {grp_label}}; also confirm the ",
      "{gb_desc} is not shared by two physically different units. no consensus was propagated for this family."))
  }

  for (uk in names(units)) {
    members <- units[[uk]]
    is_singleton_unknown <- startsWith(uk, "<unknown:")
    grp_label <- if (is_singleton_unknown) members[[1]]$id else uk

    # pool the confident votes (consensusAxisMapping votes are the resolved proposals)
    proposals <- lapply(members, function(n) n$proposal)
    pool <- .poolAxisMappings(proposals, min.agreement = min.agreement)

    # classify each family's consensus status
    fam_status <- list()
    for (f in fam_letters) {
      pf <- pool[[f]]
      if (is.null(pf)) { fam_status[[f]] <- list(status = "none"); next }
      status <- if (pf$n < min.voters) "insufficient"
                else if (pf$n_distinct == 1L) "agreed"
                else if (pf$agreement >= min.agreement) "majority"
                else "conflict"
      fam_status[[f]] <- c(pf, list(status = status,
                                    emitted = status %in% c("agreed", "majority")))
    }

    is_singleton <- length(members) == 1L

    # ---- pass 1: propagate to each member + collect (no console output) ------
    # The propagation logic is unchanged; we only gather what the new layout needs - which members
    # gained a family by consensus, which dissented from an emitted consensus (outliers), and which
    # remain unresolved - so the per-group block can be rendered in one aggregated pass below.
    grp_filled_ids  <- character(0)
    grp_unresolved  <- character(0)
    grp_outliers    <- list()
    grp_fill_by_fam <- c(accel = 0L, gyro = 0L, mag = 0L)   # per-family tally of consensus fills

    for (n in members) {
      prov <- c(accel = "none", gyro = "none", mag = "none")
      map_rows <- list()
      filled_here <- FALSE

      for (fn in names(fam_letters)) {
        f    <- fam_letters[[fn]]
        axes <- fam_axes[[f]]
        fs   <- fam_status[[f]]
        own  <- .familyMatrixFromProposal(n$proposal, f, axes)
        own_rows <- .familyRowsFromProposal(n$proposal, f)

        if (!is.null(own)) {
          # this deployment resolved the family itself: keep it, never override
          prov[[fn]] <- "self"
          map_rows[[fn]] <- own_rows
          if (!is.null(fs$M) && isTRUE(fs$emitted) && !.matEq(own, fs$M)) {
            grp_outliers[[fn]] <- c(grp_outliers[[fn]], n$id)   # self-resolved, but the dissenting solution
          }
          next
        }

        if (is.null(fs$M) || !isTRUE(fs$emitted)) {
          # unresolved here and no consensus to fill it: "ambiguous" if the deployment itself has the
          # family but could not decide it (accel: a non-empty tied set), otherwise the family is absent.
          has_family <- f == "a" && !is.null(n$candidates) && nrow(n$candidates) > 0
          prov[[fn]] <- if (has_family) "ambiguous" else "none"
          next
        }

        # ambiguous deployment + an emitted unit consensus -> consistency-gated adoption
        if (f == "a") {
          cands <- .candidateMatrices(n$candidates)
          consistent <- length(cands) > 0 && any(vapply(cands, function(C) .matEq(C, fs$M), logical(1)))
          if (consistent) {
            prov[[fn]] <- "consensus"; map_rows[[fn]] <- fs$mapping; filled_here <- TRUE
            grp_fill_by_fam[[fn]] <- grp_fill_by_fam[[fn]] + 1L
          } else if (length(cands) > 0) {
            prov[[fn]] <- "conflict"   # consensus lies outside this deployment's own tied set
          } else {
            prov[[fn]] <- "ambiguous"
          }
        } else {
          # gyro / mag: no tied-candidate set; adopt the unit consensus where the family is absent
          prov[[fn]] <- "consensus"; map_rows[[fn]] <- fs$mapping; filled_here <- TRUE
          grp_fill_by_fam[[fn]] <- grp_fill_by_fam[[fn]] + 1L
        }
      }

      if (filled_here) { n_filled <- n_filled + 1L; grp_filled_ids <- c(grp_filled_ids, n$id) }
      if (any(prov[c("accel", "gyro", "mag")] %in% c("ambiguous", "conflict"))) {
        grp_unresolved <- c(grp_unresolved, n$id)
      }

      mp <- if (length(map_rows)) do.call(rbind, map_rows) else
        data.frame(from = character(0), to = character(0), stringsAsFactors = FALSE)
      rownames(mp) <- NULL
      out_mappings[[n$id]] <- mp

      prov_rows[[n$id]] <- data.frame(
        id = n$id, group = if (is_singleton_unknown) NA_character_ else uk, package_id = n$package_id,
        accel = prov[["accel"]], gyro = prov[["gyro"]], mag = prov[["mag"]],
        stringsAsFactors = FALSE)
    }

    outlier_ids <- union(outlier_ids, unlist(grp_outliers, use.names = FALSE))

    # ---- group report (recorded for every group, including standalones) ------
    group_reports[[grp_label]] <- list(
      group        = if (is_singleton_unknown) NA_character_ else uk,
      group.by     = group.by,
      deployments  = vapply(members, function(n) n$id, character(1)),
      families     = lapply(stats::setNames(names(fam_letters), names(fam_letters)), function(fn) {
        fs <- fam_status[[fam_letters[[fn]]]]
        if (identical(fs$status, "none")) return(list(status = "none", n_voters = 0L))
        list(status = fs$status, n_voters = fs$n, agreement = fs$agreement,
             n_distinct = fs$n_distinct, conflict = identical(fs$status, "conflict"),
             mapping = if (isTRUE(fs$emitted)) fs$mapping else NULL)
      }))

    # ---- standalone (size-1) groups: suppress the block; list them later -----
    if (is_singleton) { skipped_standalone <- c(skipped_standalone, grp_label); next }

    # conflicts are counted and warned regardless of verbosity (a swapped/remounted board is serious)
    conflict_fams <- Filter(function(fn) identical(fam_status[[fam_letters[[fn]]]]$status, "conflict"),
                            names(fam_letters))
    for (fn in conflict_fams) {
      n_conflicts <- n_conflicts + 1L
      fs <- fam_status[[fam_letters[[fn]]]]
      warning(sprintf(paste0("consensusAxisMapping: %s '%s' has conflicting %s axis mappings ",
                             "across confidently-resolved deployments (%d distinct solutions). Check ",
                             "whether the circuit board was swapped or remounted inside the housing, ",
                             "or whether two units share this %s. No consensus propagated."),
                      gb_desc, grp_label, fn, fs$n_distinct, gb_desc), call. = FALSE)
    }

    # ---- pass 2: render the group block --------------------------------------
    if (lvl >= 2L) {
      # detailed layout: per-family verdict + named outliers + one aggregated propagation line
      .log_gap(lvl, min_level = 1L)
      cli::cli_text("{.strong Group: {grp_label}} ({length(members)} deployment{?s})")
      # always print a line for all three families (absent ones included) for a consistent shape
      for (fn in names(fam_letters)) {
        fs <- fam_status[[fam_letters[[fn]]]]; st <- fs$status
        if (identical(st, "none")) {
          cli::cli_alert_info("{fn}: absent (0 voters).")
        } else if (identical(st, "conflict")) {
          emit_conflict_block(fn, fs, gb_desc, grp_label)
        } else if (identical(st, "agreed")) {
          cli::cli_alert_success("{fn}: unanimous consensus ({fs$n} voter{?s}).")
        } else if (identical(st, "majority")) {
          cli::cli_alert_warning(
            "{fn}: majority consensus ({round(100 * fs$agreement)}%, {as.integer(fs$counts)[1]}/{fs$n} voters).")
          if (length(grp_outliers[[fn]])) {
            out_vec <- cli::cli_vec(grp_outliers[[fn]], list("vec-trunc" = 5))
            cli::cli_text("  {sub_arrow} dissenting outlier{?s} ignored: {out_vec}")
          }
        } else if (identical(st, "insufficient")) {
          cli::cli_alert_info("{fn}: insufficient voters ({fs$n}) - no consensus.")
        }
      }
      if (length(grp_filled_ids)) {
        # total rescued, then an indented per-family breakdown (replaces the long id list)
        cli::cli_alert_success("consensus propagated to {length(grp_filled_ids)} deployment{?s}")
        breakdown <- paste(sprintf("%s (%d)", names(grp_fill_by_fam), grp_fill_by_fam), collapse = ", ")
        cli::cli_text("  {sub_arrow} {breakdown}")
      } else if (!length(grp_unresolved)) {
        cli::cli_alert_success("all {length(members)} deployment{?s} already resolved - no propagation needed.")
      } else {
        cli::cli_alert_info("no consensus to propagate ({length(grp_unresolved)} deployment{?s} still unresolved).")
      }

    } else if (lvl == 1L) {
      # compact layout: one scoreboard line per group (family verdicts + propagation outcome)
      marks <- character(0)
      for (fn in names(fam_letters)) {
        st <- fam_status[[fam_letters[[fn]]]]$status
        if (identical(st, "none")) next
        sym <- switch(st, agreed = cli::symbol$tick, majority = "!",
                      conflict = cli::symbol$cross, insufficient = "-", "?")
        marks <- c(marks, paste0(fn, " ", sym))
      }
      arrow  <- cli::symbol$arrow_right
      suffix <- if (length(grp_filled_ids)) paste(arrow, "propagated to", length(grp_filled_ids))
                else if (!length(grp_unresolved)) paste(arrow, "all resolved")
                else paste(arrow, length(grp_unresolved), "unresolved")
      line <- sprintf("%s (%d): %s %s", grp_label, length(members),
                      paste(marks, collapse = " \u00b7 "), suffix)
      if (length(conflict_fams)) cli::cli_alert_danger("{line}")
      else if (!length(grp_unresolved)) cli::cli_alert_success("{line}")
      else cli::cli_alert_warning("{line}")
      # a conflict is always shown in full, even in compact mode
      for (fn in conflict_fams) emit_conflict_block(fn, fam_status[[fam_letters[[fn]]]], gb_desc, grp_label)
    }
  }

  provenance <- do.call(rbind, prov_rows[order(names(prov_rows))])
  rownames(provenance) <- NULL

  # ---- standalone deployments skipped during the loop (size-1 groups) --------
  n_standalone <- length(skipped_standalone)
  if (lvl >= 2L && n_standalone > 0L) {
    cli::cli_text("")
    cli::cli_alert_info("Skipped {n_standalone} standalone deployment{?s}:")
    cli::cli_ul(); for (s in skipped_standalone) cli::cli_li("{s}"); cli::cli_end()
    if (has_unknown) {
      cli::cli_text("  {cli::symbol$info} some carry no {gb_desc} - set it in metadata to enable reconciliation.")
    }
  }

  # ---- final resolution status (post-propagation rescue verdict) -------------
  # a deployment is fully resolved when its accelerometer is settled (a physically absent gyro/mag is
  # fine); only accel can be left ambiguous/conflict, but we test every family for forward-compatibility.
  unresolved_mask <- provenance$accel %in% c("ambiguous", "conflict") |
                     provenance$gyro  %in% c("ambiguous", "conflict") |
                     provenance$mag   %in% c("ambiguous", "conflict")
  unresolved_ids <- provenance$id[unresolved_mask]
  n_resolved   <- sum(!unresolved_mask)
  n_unresolved <- length(unresolved_ids)
  n_outliers   <- length(outlier_ids)

  ##############################################################################
  # Summary ####################################################################
  ##############################################################################

  n_real_groups <- sum(!startsWith(names(units), "<unknown:"))
  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, length(nodes), " deployment", if (length(nodes) != 1) "s", " across ",
              n_real_groups, " group", if (n_real_groups != 1) "s", " reconciled")
    if (n_filled > 0L) cli::cli_alert_success("{n_filled} deployment{?s} enhanced via group consensus")
    if (n_outliers > 0L) cli::cli_alert_warning("{n_outliers} outlier deployment{?s} identified (kept own mapping)")
    if (n_conflicts > 0L) cli::cli_alert_warning("{n_conflicts} conflict{?s} flagged (no consensus propagated)")
    if (n_standalone > 0L) cli::cli_alert_info("{n_standalone} standalone deployment{?s} skipped")

    cli::cli_text("")
    .log_h2(lvl, "FINAL RESOLUTION STATUS", min_level = 1L)
    pct <- if (length(nodes)) round(100 * n_resolved / length(nodes)) else 0
    cli::cli_alert_success("{n_resolved} / {length(nodes)} fully resolved ({pct}%)")
    if (n_unresolved > 0L) {
      unres_vec <- cli::cli_vec(unresolved_ids, list("vec-trunc" = 10))
      cli::cli_alert_warning("{n_unresolved} deployment{?s} remain{?s/} unresolved/partial: {unres_vec}")
    }
    .log_runtime(lvl, start.time)
  }

  out <- list(mappings = out_mappings, provenance = provenance, groups = group_reports,
              n_groups = n_real_groups, n_deployments = length(nodes),
              n_filled = n_filled, n_conflicts = n_conflicts, n_outliers = n_outliers,
              n_standalone = n_standalone, n_resolved = n_resolved, n_unresolved = n_unresolved,
              unresolved_ids = unresolved_ids)
  attr(out, "nautilus.mapping.producer") <- "consensusAxisMapping"   # for applyAxisMapping() routing
  out
}


#######################################################################################################
# Internal helpers ####################################################################################
#######################################################################################################

#' Recursively collect checkTagMapping deployment nodes from a (possibly nested) results object.
#'
#' A node is any list element carrying a \code{$proposal} data.frame (a checkTagMapping result). The
#' node's \code{id} is taken from \code{$id}, else the enclosing list name. The grouping keys
#' (\code{package_id}, \code{tag}, \code{type}) are carried through (NA if absent), along with
#' \code{candidates} / \code{resolution}.
#' @keywords internal
#' @noRd
.collectMappingNodes <- function(x, nm = NA_character_) {
  if (is.null(x) || !is.list(x) || is.data.frame(x)) return(list())
  if (!is.null(x$proposal) && is.data.frame(x$proposal)) {
    scal <- function(v) { v <- v %||% NA_character_; if (length(v) != 1) NA_character_ else as.character(v) }
    return(list(list(
      id         = as.character(x$id %||% nm %||% NA_character_),
      package_id = scal(x$package_id),
      logger_id  = scal(x$logger_id),
      tag        = scal(x$tag),
      type       = scal(x$type),
      proposal   = x$proposal,
      candidates = x$candidates,
      resolution = x$resolution)))
  }
  nms <- names(x); if (is.null(nms)) nms <- rep(NA_character_, length(x))
  out <- list()
  for (i in seq_along(x)) out <- c(out, .collectMappingNodes(x[[i]], nms[i]))
  out
}


#' Pool a flat list of proposal data.frames into a per-family consensus signed permutation.
#'
#' For each sensor family (accel/gyro/mag) it tallies the signed-permutation matrices implied by the
#' proposals that carry a complete triplet for that family, and returns the modal matrix with its
#' support. This is the pure pooler underneath \code{consensusAxisMapping()} (no grouping, no
#' propagation), kept separate so it can be unit-tested in isolation.
#' @param proposals A list of \code{from}/\code{to} data.frames (one per deployment).
#' @param min.agreement Fraction at / above which a family's modal mapping is emitted as \code{mapping}.
#' @return A named list keyed by family letter ("a"/"g"/"m"); each element carries \code{n} (voters),
#'   \code{agreement}, \code{n_distinct}, \code{counts} (table of solutions), \code{M} (modal matrix),
#'   and \code{mapping} (from/to of the modal matrix, or NULL below \code{min.agreement}).
#' @keywords internal
#' @noRd
.poolAxisMappings <- function(proposals, min.agreement = 0.6) {
  fam_letters <- c("a", "g", "m")
  axis_names  <- list(a = c("ax", "ay", "az"), g = c("gx", "gy", "gz"), m = c("mx", "my", "mz"))
  fam_of <- function(axis) substr(sub("^-", "", axis), 1, 1)

  per_family <- list()
  for (f in fam_letters) {
    mats <- list(); keys <- character(0)
    for (p in proposals) {
      if (is.null(p) || !is.data.frame(p) || !nrow(p)) next
      fam_rows <- p[fam_of(p$to) == f, , drop = FALSE]
      if (nrow(fam_rows) != 3L) next
      M <- .mappingToSignedPerm(fam_rows, axis_names[[f]])
      if (is.null(M)) next
      key <- paste(M, collapse = ",")
      keys <- c(keys, key); mats[[key]] <- M
    }
    if (!length(keys)) next
    tab  <- sort(table(keys), decreasing = TRUE)
    top  <- names(tab)[1]
    frac <- as.numeric(tab[1]) / length(keys)
    per_family[[f]] <- list(
      n = length(keys), agreement = frac, n_distinct = length(tab), counts = tab,
      M = mats[[top]],
      mapping = if (frac >= min.agreement) .matrixToFromTo(mats[[top]], axis_names[[f]], axis_names[[f]]) else NULL)
  }
  per_family
}


#' The signed-permutation matrix implied by one family's rows in a proposal, or NULL if not a full triplet.
#' @keywords internal
#' @noRd
.familyMatrixFromProposal <- function(proposal, f, axes) {
  rows <- .familyRowsFromProposal(proposal, f)
  if (is.null(rows) || nrow(rows) != 3L) return(NULL)
  .mappingToSignedPerm(rows, axes)
}

#' One family's from/to rows from a proposal data.frame (empty data.frame if none).
#' @keywords internal
#' @noRd
.familyRowsFromProposal <- function(proposal, f) {
  if (is.null(proposal) || !is.data.frame(proposal) || !nrow(proposal)) {
    return(data.frame(from = character(0), to = character(0), stringsAsFactors = FALSE))
  }
  proposal[substr(sub("^-", "", proposal$to), 1, 1) == f, c("from", "to"), drop = FALSE]
}

#' The signed-permutation matrices of a checkTagMapping $candidates table (the accel tied set).
#' @keywords internal
#' @noRd
.candidateMatrices <- function(candidates) {
  if (is.null(candidates) || !nrow(candidates)) return(list())
  needed <- c("newX_col", "newX_sign", "newY_col", "newY_sign", "newZ_col", "newZ_sign")
  if (!all(needed %in% names(candidates))) return(list())
  axes <- c("ax", "ay", "az")
  sgn  <- function(s, ax) paste0(if (isTRUE(s < 0)) "-" else "", ax)
  out <- list()
  for (i in seq_len(nrow(candidates))) {
    r  <- candidates[i, ]
    ft <- data.frame(
      from = c(as.character(r$newX_col), as.character(r$newY_col), as.character(r$newZ_col)),
      to   = c(sgn(r$newX_sign, "ax"), sgn(r$newY_sign, "ay"), sgn(r$newZ_sign, "az")),
      stringsAsFactors = FALSE)
    M <- .mappingToSignedPerm(ft, axes)
    if (!is.null(M)) out[[length(out) + 1L]] <- M
  }
  out
}

#' Equality of two signed-permutation matrices (NULL-safe).
#' @keywords internal
#' @noRd
.matEq <- function(a, b) {
  !is.null(a) && !is.null(b) && all(dim(a) == dim(b)) && all(a == b)
}
