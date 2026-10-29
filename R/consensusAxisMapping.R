#######################################################################################################
# Cross-deployment consensus axis mapping #############################################################
#######################################################################################################

#' Reconcile sensor-axis mappings across compatible deployments
#'
#' @description
#' Combines deployment-level proposals from [checkTagMapping()] within groups expected to share
#' a constant sensor-axis configuration. A modal signed permutation is calculated separately for
#' the accelerometer, gyroscope and magnetometer, and eligible unresolved mappings can be filled
#' from that consensus.
#'
#' This is an optional step between [checkTagMapping()] and [applyAxisMapping()]. It returns
#' reconciled mappings and their provenance, without transforming sensor data. Existing complete
#' deployment-level mappings are retained even when they differ from the group consensus.
#' Keep the original diagnostic results for [reviewTagMapping()], which requires their
#' deployment-level frame evidence.
#'
#' @param results A named list returned by [checkTagMapping()], a list of such outputs, or a
#'   nested list whose deployment-level elements contain a \code{proposal} data frame.
#'   Each deployment must have a distinct identifier and should carry the metadata keys used
#'   by \code{group.by}. Missing grouping values isolate a deployment in its own group.
#' @param group.by Character vector of metadata keys defining compatible deployments:
#'   \code{"package_id"} (default), \code{"logger_id"}, \code{"tag"} or \code{"type"}.
#'   Multiple keys define a composite group, for example \code{c("package_id", "logger_id")}.
#'   \code{package_id} identifies the physical package, \code{logger_id} its logger,
#'   and \code{tag} and \code{type} the model and type recorded by [checkTagMapping()].
#'   Choose keys for which the full raw-to-body signed permutation is expected to remain constant.
#' @param min.agreement Numeric value in \code{(0, 1]} giving the minimum fraction of valid
#'   family votes supporting the modal mapping (default \code{0.6}). Votes are unweighted;
#'   unresolved deployments do not enter the denominator. Values greater than \code{0.5}
#'   require a unique majority.
#' @param min.voters Minimum number of deployments contributing valid complete mappings for
#'   a sensor family before its consensus can be propagated. Integer at least \code{1};
#'   default \code{2}. With the default, a single resolved deployment cannot supply consensus.
#' @param verbose How much detail to print: \code{0}/\code{"quiet"},
#'   \code{1}/\code{"normal"}, or \code{2}/\code{"detailed"} (default). Normal output provides
#'   group-level conclusions; detailed output adds family-level agreement and propagation.
#'   Group conflicts below the agreement threshold generate an R warning at any verbosity.
#'
#' @details
#' ## Grouping assumptions
#'
#' Consensus assumes that the complete raw-sensor-to-body signed permutation is shared within
#' each group, not merely that its deployments used the same tag model. A fixed circuit-board
#' orientation alone does not establish an unchanged housing-to-animal orientation. Mounting
#' changes, board replacement or reused identifiers can violate the grouping assumption.
#'
#' Group by physical unit where that assumption is justified. Pooling by model or type requires
#' independent evidence of standardised sensor and attachment conventions. An unknown value in
#' any requested key makes that deployment a standalone group rather than pooling unrelated units.
#' If no deployment carries the selected keys, a warning is issued.
#'
#' ## Voting and propagation
#'
#' Each proposal contributes one vote per sensor family only when that family contains a complete,
#' valid signed-permutation triplet. No confidence weighting or additional family-status filter is
#' applied: eligibility is determined from the proposal itself. Families are pooled independently.
#'
#' A consensus is available when there are at least \code{min.voters} valid votes and the modal
#' mapping meets \code{min.agreement}. Unanimous solutions are \code{"agreed"}; qualifying
#' non-unanimous solutions are \code{"majority"}. Multiple solutions below the agreement threshold
#' are \code{"conflict"} and are not propagated. Too few votes are \code{"insufficient"}.
#' At thresholds of \code{0.5} or lower, tied modal solutions can qualify; the first modal solution
#' in the frequency table is then used. Prefer a threshold above \code{0.5} for majority consensus.
#'
#' A deployment's own complete mapping always takes precedence. Dissenting deployments retain
#' that mapping and are counted as outliers, not overwritten. An unresolved accelerometer frame
#' adopts consensus only when it is among that deployment's surviving \code{candidates}.
#' A consensus outside a non-empty candidate set is recorded as a deployment-level conflict;
#' no accelerometer mapping is filled. Missing candidates do not authorise propagation.
#'
#' Gyroscope and magnetometer proposals do not carry equivalent candidate sets. Their consensus
#' can fill an omitted family without a deployment-level consistency or channel-presence check.
#' Consequently, family provenance describes mapping availability, not necessarily usable sensor
#' coverage. [applyAxisMapping()] subsequently applies mappings only to complete sensor triplets.
#' Inspect cross-family consistency and the diagnostic evidence before using these fills.
#'
#' ## Workflow and interpretation
#'
#' Reconciliation does not modify the original \code{results} or preserve their \code{frame_state}
#' within its output. To review diagnostic cases while retaining reconciled defaults, pass the
#' original results as \code{mapping} and this output as \code{base} to [reviewTagMapping()].
#' Consensus does not confirm a documented configuration, calibrate a sensor or establish an
#' absolute heading reference.
#'
#' Unresolved mappings can remain empty. [applyAxisMapping()] passes those datasets through
#' unchanged; their inclusion in its output does not establish a valid body frame.
#'
#' @return A list carrying a \code{nautilus.mapping.producer} attribute, with components:
#'   \describe{
#'     \item{\code{mappings}}{A named list of \code{from}/\code{to} data frames, one per deployment,
#'       accepted by [applyAxisMapping()]. An unresolved deployment can have an empty mapping.}
#'     \item{\code{provenance}}{A data frame with \code{id}, \code{group}, \code{package_id} and
#'       \code{accel}, \code{gyro}, \code{mag} columns. Family values are \code{"self"} for an
#'       existing complete proposal, \code{"consensus"} for a group fill, \code{"conflict"} for an
#'       accelerometer consensus outside the candidate set, \code{"ambiguous"} for an unfilled
#'       accelerometer frame, or \code{"none"} when no family proposal is represented.
#'       Missing grouping keys are reported as \code{NA}.}
#'     \item{\code{groups}}{A named list of group reports containing the group key, grouping fields,
#'       deployment IDs and per-family summaries: \code{status}, \code{n_voters} and, where votes
#'       exist, \code{agreement}, \code{n_distinct}, \code{conflict} and \code{mapping}.
#'       A family with no votes has status \code{"none"}; its consensus mapping is otherwise
#'       \code{NULL} when the propagation criteria are unmet.}
#'     \item{\code{n_groups}, \code{n_deployments}}{Numbers of groups with known keys and deployments
#'       evaluated, respectively. Groups with missing keys are excluded from \code{n_groups}.}
#'     \item{\code{n_filled}}{Number of deployments gaining at least one family by consensus.}
#'     \item{\code{n_conflicts}}{Number of group--family conflicts below \code{min.agreement},
#'       not the number of deployment-level candidate conflicts.}
#'     \item{\code{n_outliers}, \code{n_standalone}}{Numbers of deployments retaining a dissenting
#'       own mapping and groups containing a single deployment, respectively.}
#'     \item{\code{n_resolved}, \code{n_unresolved}, \code{unresolved_ids}}{Resolution tallies and
#'       identifiers based on whether family provenance contains \code{"ambiguous"} or
#'       \code{"conflict"}. These describe reconciliation state, not independent sensor validation.}
#'   }
#'
#' @seealso [checkTagMapping()] for deployment-level inference; [reviewTagMapping()] for video
#'   review using the original evidence and reconciled base; [applyAxisMapping()] for applying
#'   mappings; [processTagData()] for deriving body-frame metrics.
#'
#' @examples
#' \dontrun{
#' # Raw-axis records already trimmed and checked for sensor integrity.
#' files <- list.files("./checked", pattern = "\\.rds$", full.names = TRUE)
#' evidence <- checkTagMapping(files)
#' reconciled <- consensusAxisMapping(evidence, group.by = "package_id")
#' reconciled$provenance
#' reconciled$unresolved_ids
#'
#' # Restrict pooling to deployments sharing both package and logger identifiers.
#' reconciled <- consensusAxisMapping(evidence,
#'                                    group.by = c("package_id", "logger_id"),
#'                                    min.agreement = 0.8, min.voters = 2)
#'
#' # Inspect the evidence before applying the reconciled mappings.
#' oriented <- applyAxisMapping(files, mapping = reconciled)
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
  # Conflicts are ACCUMULATED, not warned as they are found. One warning per group per sensor family
  # buries a large cohort in near-identical text, and R keeps only the first 50 warnings, so on a big
  # run the tail is silently dropped - the same reasoning as .warnDiveBaseline() in detectDives().
  conflicts <- list()
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
      conflicts[[grp_label]] <- c(conflicts[[grp_label]],
                                  list(list(family = fn, n_distinct = fs$n_distinct,
                                            ids = vapply(members, function(n) n$id, character(1)))))
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

  # one warning for the whole run, naming every affected group and the families that disagree
  .warnAxisConflicts(conflicts, gb_desc)

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
