# Tests for consensusAxisMapping(): per-unit (package_id) reconciliation of checkTagMapping solutions,
# and the pure pooler .poolAxisMappings() underneath it.

# accel mapping M: body ax <- raw ay, body ay <- -raw ax, body az <- raw az
.M  <- matrix(c(0, 1, 0,  -1, 0, 0,  0, 0, 1), nrow = 3, byrow = TRUE)
.M_to   <- c("ax", "-ay", "az"); .M_from <- c("ay", "ax", "az")
.id_to  <- c("ax", "ay", "az");  .id_from <- c("ax", "ay", "az")

# a from/to proposal data.frame for a given accel mapping (optionally adding an identity gyro)
.prop <- function(from, to, gyro = FALSE) {
  d <- data.frame(from = from, to = to, stringsAsFactors = FALSE)
  if (gyro) d <- rbind(d, data.frame(from = c("gx", "gy", "gz"), to = c("gx", "gy", "gz")))
  d
}

# candidate-table row(s); each row is body X/Y/Z <- sign * raw col
.cand <- function(...) {
  rows <- list(...)
  do.call(rbind, lapply(rows, function(r) data.frame(
    newX_col = r[[1]], newX_sign = r[[2]], newY_col = r[[3]], newY_sign = r[[4]],
    newZ_col = r[[5]], newZ_sign = r[[6]], score = 0, stringsAsFactors = FALSE)))
}
# the candidate row encoding mapping M, and an identity (different) candidate sharing the same Z
.cand_M  <- list("ay", 1, "ax", -1, "az", 1)
.cand_id <- list("ax", 1, "ay",  1, "az", 1)

# a fully-resolved deployment node (accel proposal present)
.resolved <- function(id, pkg, from = .M_from, to = .M_to) {
  list(id = id, package_id = pkg, proposal = .prop(from, to),
       candidates = .cand(.cand_M), resolution = NULL)
}
# an ambiguous deployment node (no accel proposal; a tied candidate set)
.ambiguous <- function(id, pkg, cands = .cand(.cand_M, .cand_id)) {
  list(id = id, package_id = pkg,
       proposal = data.frame(from = character(0), to = character(0), stringsAsFactors = FALSE),
       candidates = cands, resolution = NULL)
}

.run <- function(...) suppressWarnings(suppressMessages(
  consensusAxisMapping(..., verbose = 0)))

# ---- the pure pooler -----------------------------------------------------------------------------

test_that(".poolAxisMappings: unanimous deployments yield that mapping with agreement 1", {
  p <- .prop(.M_from, .M_to)
  pool <- nautilus:::.poolAxisMappings(list(p, p, p))
  expect_equal(pool$a$agreement, 1)
  expect_equal(pool$a$n, 3)
  expect_equal(pool$a$M, .M)
  expect_equal(nautilus:::.mappingToSignedPerm(pool$a$mapping, .id_from), .M)
})

test_that(".poolAxisMappings: the majority wins and the minority shows in agreement / n_distinct", {
  pool <- nautilus:::.poolAxisMappings(list(.prop(.M_from, .M_to), .prop(.M_from, .M_to),
                                            .prop(.id_from, .id_to)))
  expect_equal(pool$a$M, .M)            # 2/3 wins
  expect_equal(pool$a$agreement, 2 / 3)
  expect_equal(pool$a$n_distinct, 2)
})

test_that(".poolAxisMappings: a family below min.agreement has a NULL mapping", {
  pool <- nautilus:::.poolAxisMappings(list(.prop(.id_from, .id_to), .prop(.M_from, .M_to)),
                                       min.agreement = 0.75)   # 50/50
  expect_null(pool$a$mapping)
  expect_equal(pool$a$agreement, 0.5)
})

test_that(".poolAxisMappings: multi-family proposals pool per family", {
  p <- .prop(.M_from, .M_to, gyro = TRUE)        # accel = M, gyro = identity
  pool <- nautilus:::.poolAxisMappings(list(p, p))
  expect_equal(pool$a$M, .M)
  expect_equal(pool$g$agreement, 1)
  expect_setequal(pool$g$mapping$to, c("gx", "gy", "gz"))
})

# ---- the orchestrator: grouping + propagation ----------------------------------------------------

test_that("a strong deployment rescues an ambiguous one of the same package_id", {
  res <- .run(list(D1 = .resolved("D1", "PKG_A"),
                   D2 = .resolved("D2", "PKG_A"),
                   D3 = .ambiguous("D3", "PKG_A")))
  prov <- res$provenance
  expect_equal(prov$accel[prov$id == "D1"], "self")
  expect_equal(prov$accel[prov$id == "D3"], "consensus")     # filled from the unit
  # and the propagated mapping equals the unit consensus M
  expect_equal(nautilus:::.mappingToSignedPerm(res$mappings$D3, .id_from), .M)
  expect_equal(res$n_filled, 1L)
  expect_equal(res$n_groups, 1L)
})

test_that("deployments are grouped strictly by package_id (no cross-unit leakage)", {
  # PKG_A resolves M (x2); PKG_B has a single ambiguous deployment -> nothing to learn from
  res <- .run(list(A1 = .resolved("A1", "PKG_A"), A2 = .resolved("A2", "PKG_A"),
                   B1 = .ambiguous("B1", "PKG_B")))
  prov <- res$provenance
  expect_equal(prov$accel[prov$id == "B1"], "ambiguous")     # not filled from PKG_A
  expect_equal(nrow(res$mappings$B1), 0L)
  expect_equal(res$n_groups, 2L)
})

test_that("consensus is propagated only when consistent with the deployment's own candidates", {
  # the ambiguous deployment's tied set does NOT include M -> gate refuses
  off1 <- list("az", 1, "ay", 1, "ax", 1)   # arbitrary different signed perms (not M)
  off2 <- list("ay", 1, "az", 1, "ax", 1)
  res <- .run(list(D1 = .resolved("D1", "PKG_A"), D2 = .resolved("D2", "PKG_A"),
                   D3 = .ambiguous("D3", "PKG_A", cands = .cand(off1, off2))))
  prov <- res$provenance
  expect_equal(prov$accel[prov$id == "D3"], "conflict")      # consensus outside its candidate set
  expect_equal(nrow(res$mappings$D3), 0L)                    # left unresolved
})

test_that("a gravity-ambiguous deployment is rescued when the consensus sits on a competing plausible vertical", {
  # the towed/tilted-tag case: checkTagMapping reports a candidate set spanning BOTH the gravity-best
  # vertical (Z = ax here) and the true/group vertical (Z = az = M). The consensus (Z = az) is in that
  # broadened set, so the gate must admit it and rescue the deployment.
  off_z_ax  <- list("ay", 1, "az", 1, "ax", 1)               # a perm whose vertical is ax (not the consensus)
  amb_multi <- list(id = "D3", package_id = "PKG_A",
                    proposal = data.frame(from = character(0), to = character(0), stringsAsFactors = FALSE),
                    candidates = .cand(off_z_ax, .cand_M), resolution = NULL)   # spans ax + az verticals
  res <- .run(list(D1 = .resolved("D1", "PKG_A"), D2 = .resolved("D2", "PKG_A"), D3 = amb_multi))
  expect_equal(res$provenance$accel[res$provenance$id == "D3"], "consensus")    # admitted from the az vertical
  expect_equal(nautilus:::.mappingToSignedPerm(res$mappings$D3, .id_from), .M)  # the group's M was applied
})

test_that("min.voters blocks propagation from a lone resolved deployment", {
  res <- .run(list(D1 = .resolved("D1", "PKG_A"), D2 = .ambiguous("D2", "PKG_A")),
              min.voters = 2)
  prov <- res$provenance
  expect_equal(prov$accel[prov$id == "D1"], "self")
  expect_equal(prov$accel[prov$id == "D2"], "ambiguous")     # only 1 voter -> no consensus
})

test_that("conflicting confident deployments are flagged and warn about a hardware change", {
  # two confident deployments of one unit disagree (M vs identity) -> conflict, no propagation
  dat <- list(D1 = .resolved("D1", "PKG_A"),
              D2 = .resolved("D2", "PKG_A", from = .id_from, to = .id_to),
              D3 = .ambiguous("D3", "PKG_A"))
  expect_warning(suppressMessages(consensusAxisMapping(dat, verbose = 0)),
                 "circuit board was swapped")
  res <- .run(dat)
  expect_equal(res$n_conflicts, 1L)
  expect_equal(res$provenance$accel[res$provenance$id == "D3"], "ambiguous")  # not filled under conflict
  expect_true(res$groups$PKG_A$families$accel$conflict)
})

test_that("the conflict console block tells the user to check the housing for that package_id", {
  dat <- list(D1 = .resolved("D1", "PKG_A"),
              D2 = .resolved("D2", "PKG_A", from = .id_from, to = .id_to))
  out <- paste(cli::cli_fmt(suppressWarnings(suppressMessages(
    consensusAxisMapping(dat, verbose = 1)))), collapse = "\n")
  expect_match(out, "CONFLICT")
  expect_match(out, "remounted")
  expect_match(out, "PKG_A")
})

test_that("deployments without a package_id are left standalone, never merged", {
  res <- .run(list(D1 = .resolved("D1", NA_character_), D2 = .ambiguous("D2", NA_character_)))
  prov <- res$provenance
  expect_equal(prov$accel[prov$id == "D1"], "self")
  expect_equal(prov$accel[prov$id == "D2"], "ambiguous")     # two distinct unknown units
  expect_equal(res$n_groups, 0L)                              # no real units reconciled
})

test_that("nested results (a list of checkTagMapping outputs) are flattened", {
  out1 <- list(A1 = .resolved("A1", "PKG_A"), A2 = .resolved("A2", "PKG_A"))
  out2 <- list(A3 = .ambiguous("A3", "PKG_A"))
  res <- .run(list(out1, out2))
  expect_equal(res$n_deployments, 3L)
  expect_equal(res$provenance$accel[res$provenance$id == "A3"], "consensus")
})

test_that("an empty / proposal-less input errors clearly", {
  expect_error(.run(list(list(foo = 1))), "No")
})

test_that("invalid thresholds are rejected", {
  expect_error(.run(list(D1 = .resolved("D1", "PKG_A")), min.agreement = 0), "min.agreement")
  expect_error(.run(list(D1 = .resolved("D1", "PKG_A")), min.voters = 0), "min.voters")
})

# ---- group.by: reconcile by tag / composite keys -------------------------------------------------

test_that("group.by = 'tag' reconciles across different package_ids that share a tag model", {
  # boards swapped between deployments: different package_ids, same tag model -> group by tag.
  # two confident voters (meet min.voters) rescue the ambiguous third.
  d1 <- .resolved("D1", "PKG_A"); d1$tag <- "CATS"
  d2 <- .resolved("D2", "PKG_B"); d2$tag <- "CATS"
  d3 <- .ambiguous("D3", "PKG_C"); d3$tag <- "CATS"
  res <- .run(list(D1 = d1, D2 = d2, D3 = d3), group.by = "tag")
  expect_equal(res$n_groups, 1L)                                      # grouped by tag, not package_id
  expect_equal(res$provenance$accel[res$provenance$id == "D3"], "consensus")
  expect_equal(res$provenance$group[res$provenance$id == "D3"], "CATS")
})

test_that("group.by = 'logger_id' reconciles across package_ids that share a logger (board reuse)", {
  # the chip (logger) is reused across housings: same logger_id, different package_ids -> group by logger.
  d1 <- .resolved("D1", "PKG_A"); d1$logger_id <- "LOG_1"
  d2 <- .resolved("D2", "PKG_B"); d2$logger_id <- "LOG_1"
  d3 <- .ambiguous("D3", "PKG_C"); d3$logger_id <- "LOG_1"
  res <- .run(list(D1 = d1, D2 = d2, D3 = d3), group.by = "logger_id")
  expect_equal(res$n_groups, 1L)                                      # grouped by logger, not package_id
  expect_equal(res$provenance$accel[res$provenance$id == "D3"], "consensus")
  expect_equal(res$provenance$group[res$provenance$id == "D3"], "LOG_1")
})

test_that("a composite group.by = c('tag','type') keys on the combination", {
  d1 <- .resolved("D1", "PA"); d1$tag <- "CATS"; d1$type <- "MS"
  d2 <- .ambiguous("D2", "PB"); d2$tag <- "CATS"; d2$type <- "Camera"  # same tag, different type
  res <- .run(list(D1 = d1, D2 = d2), group.by = c("tag", "type"))
  expect_equal(res$n_groups, 2L)                                      # distinct composite keys -> not merged
  expect_equal(res$provenance$accel[res$provenance$id == "D2"], "ambiguous")
})

test_that("group.by is validated against the carried keys", {
  expect_error(.run(list(D1 = .resolved("D1", "PKG_A")), group.by = "bogus"), "must be one or more")
})

test_that("a group.by field absent from all deployments warns and leaves them standalone", {
  dat <- list(D1 = .resolved("D1", "PKG_A"), D2 = .ambiguous("D2", "PKG_A"))   # nodes carry no `tag`
  expect_warning(suppressMessages(consensusAxisMapping(dat, group.by = "tag", verbose = 0)),
                 "No deployment carries")
  res <- .run(dat, group.by = "tag")
  expect_equal(res$n_groups, 0L)
  expect_equal(res$provenance$accel[res$provenance$id == "D2"], "ambiguous")   # not rescued
})

# ---- production console UI: aggregation, named outliers, condensed singletons, rescue verdict -----

.grab <- function(...) paste(cli::cli_fmt(suppressWarnings(suppressMessages(
  consensusAxisMapping(...)))), collapse = "\n")

# a resolved node carrying an identity-gyro family (so gyro can reach a majority with one dissenter)
.gyro <- function(id, pkg, gx = "gx") {
  x <- .resolved(id, pkg)
  x$proposal <- rbind(x$proposal, data.frame(from = c("gx", "gy", "gz"),
                                             to = c(gx, "gy", "gz"), stringsAsFactors = FALSE))
  x
}

test_that("successful propagation is aggregated into one group-level line (not one per deployment)", {
  dat <- list(D1 = .resolved("D1", "PKG_A"), D2 = .resolved("D2", "PKG_A"),
              D3 = .ambiguous("D3", "PKG_A"), D4 = .ambiguous("D4", "PKG_A"),
              D5 = .ambiguous("D5", "PKG_A"))
  out <- .grab(dat, verbose = 2)
  expect_match(out, "consensus propagated to 3 deployments")   # total only (ids no longer listed)
  expect_match(out, "accel \\(\\d+\\), gyro \\(\\d+\\), mag \\(\\d+\\)")   # per-family breakdown line
  expect_false(grepl("set by consensus", out, fixed = TRUE))   # old per-deployment spam is gone
})

test_that("a dissenting outlier is named under the majority family line", {
  dat <- list(D1 = .gyro("D1", "PKG_A"), D2 = .gyro("D2", "PKG_A"),
              D3 = .gyro("D3", "PKG_A"), OUT = .gyro("OUT", "PKG_A", gx = "-gx"))
  out <- .grab(dat, verbose = 2)
  expect_match(out, "gyro: majority consensus")
  expect_match(out, "dissenting outlier.*OUT")            # lower-cased
  expect_equal(.run(dat)$n_outliers, 1L)
})

test_that("standalone (size-1) groups are condensed into a skip list, not full blocks", {
  dat <- list(D1 = .resolved("D1", "PKG_A"), D2 = .resolved("D2", "PKG_A"),   # real group of 2
              S1 = .resolved("S1", "PKG_S1"), S2 = .ambiguous("S2", "PKG_S2")) # two singletons
  out <- .grab(dat, verbose = 2)
  expect_match(out, "Skipped 2 standalone deployments")
  expect_match(out, "PKG_S1"); expect_match(out, "PKG_S2")
  expect_false(grepl("Group: PKG_S1", out, fixed = TRUE))   # no header for a singleton
  expect_match(out, "Group: PKG_A")                          # the real group is still shown
  expect_equal(.run(dat)$n_standalone, 2L)
})

test_that("verbose = 1 prints a compact one-line scoreboard per group", {
  dat <- list(D1 = .resolved("D1", "PKG_A"), D2 = .resolved("D2", "PKG_A"), D3 = .ambiguous("D3", "PKG_A"))
  d1 <- .grab(dat, verbose = 1)
  expect_match(d1, "PKG_A \\(3\\):")            # scoreboard line
  expect_match(d1, "propagated to 1")
  expect_false(grepl("unanimous consensus", d1, fixed = TRUE))   # no per-family breakdown at level 1
  expect_match(.grab(dat, verbose = 2), "unanimous consensus")   # which level 2 does show
})

test_that("the final resolution status reports the rescue verdict and lists leftovers", {
  off1 <- list("az", 1, "ay", 1, "ax", 1); off2 <- list("ay", 1, "az", 1, "ax", 1)
  dat <- list(D1 = .resolved("D1", "PKG_A"), D2 = .resolved("D2", "PKG_A"),
              D3 = .ambiguous("D3", "PKG_A", cands = .cand(off1, off2)))   # consensus outside its set
  out <- .grab(dat, verbose = 2)
  expect_match(out, "FINAL RESOLUTION STATUS")
  expect_match(out, "2 / 3 fully resolved")
  expect_match(out, "remain.*unresolved/partial: D3")
  res <- .run(dat)
  expect_equal(res$n_resolved, 2L)
  expect_equal(res$n_unresolved, 1L)
  expect_setequal(res$unresolved_ids, "D3")
})
