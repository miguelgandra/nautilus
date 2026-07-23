# Tests for the plotDives() reducer: which dives count for which metric, and why a dive was left out.
# The rendering is tested separately; everything the figure ASSERTS about the data is decided here.

# A minimal dive-metrics table. Deliberately not built by running detectDives(), so the predicate is
# pinned against a hand-known truth rather than against whatever the detector happens to produce.
.pdTable <- function(...) {
  base <- data.frame(
    ID = "A", dive_id = 1:6,
    amplitude_m  = c(10, 20, 30, 40, 50, 60),
    duration_s   = c(100, 200, 300, 400, 500, 600),
    inter_dive_s = c(11, 22, 900, 44, 55, NA),   # 900 is the censored one in the test below
    ascent_rate_q90 = c(0.5, NA, NA, 0.8, NA, 1.1),
    complete = TRUE, inter_dive_censored = FALSE,
    reference = "surface", direction = "down",
    stringsAsFactors = FALSE)
  o <- list(...)
  for (nm in names(o)) base[[nm]] <- o[[nm]]
  base
}

test_that("the censoring predicate is per metric: inter_dive_s follows its OWN flag", {
  # dive 3 is complete, but the interval AFTER it is censored - a blackout between two clean dives.
  # This is the exact row diveMetrics() documents: filtering the interval on `complete` keeps it.
  dm <- .pdTable(inter_dive_censored = c(FALSE, FALSE, TRUE, FALSE, FALSE, FALSE))
  st <- .diveSummaryTable(dm, c("amplitude_m", "inter_dive_s"), min.n = 1L)

  amp <- st[st$metric == "amplitude_m", ]
  int <- st[st$metric == "inter_dive_s", ]
  # the DIVE is complete, so amplitude keeps all six...
  expect_equal(amp$n_used, 6L)
  expect_equal(amp$n_censored, 0L)
  # ...while the INTERVAL loses the censored one (and the trailing NA, which is unsupported not censored)
  expect_equal(int$n_used, 4L)
  expect_equal(int$n_censored, 1L)
  expect_equal(int$n_unsupported, 1L)
  # and the censored value is genuinely absent from the statistics
  expect_equal(int$median, stats::median(c(11, 22, 44, 55)))          # 33
  # and it is genuinely excluded, not merely absent from a tie: including the 900 s blackout would
  # move the median to 44. This assertion is worthless unless the censored value is an outlier.
  expect_false(isTRUE(all.equal(int$median, stats::median(c(11, 22, 900, 44, 55)))))
})

test_that("a dive's own censoring excludes it from every metric except the interval", {
  dm <- .pdTable(complete = c(TRUE, TRUE, FALSE, TRUE, TRUE, TRUE))
  st <- .diveSummaryTable(dm, c("amplitude_m", "duration_s"), min.n = 1L)
  expect_equal(st$n_used, c(5L, 5L))
  expect_equal(st$n_censored, c(1L, 1L))
  expect_equal(st$median[st$metric == "amplitude_m"], stats::median(c(10, 20, 40, 50, 60)))
})

test_that("censored and phase-unsupported are counted separately, never merged", {
  # 1 censored dive and 3 with no resolvable ascent. On real data the second vastly outnumbers the
  # first (27.2% vs 1.1% over 6,512 dives), so reporting them as one number would hide the big one.
  dm <- .pdTable(complete = c(TRUE, TRUE, TRUE, TRUE, TRUE, FALSE))
  st <- .diveSummaryTable(dm, "ascent_rate_q90", min.n = 1L)
  expect_equal(st$n_dives, 6L)
  expect_equal(st$n_censored, 1L)       # dive 6, which HAS a value (1.1) but is censored
  expect_equal(st$n_unsupported, 3L)    # dives 2, 3, 5 - NA, phase structure did not support it
  expect_equal(st$n_used, 2L)
  expect_equal(st$n_used + st$n_censored + st$n_unsupported, st$n_dives)
})

test_that("NA in `complete` is not treated as complete", {
  # NA is not evidence that a dive is whole; a naive as.logical() filter would let it through
  dm <- .pdTable(complete = c(TRUE, NA, TRUE, TRUE, TRUE, TRUE))
  st <- .diveSummaryTable(dm, "amplitude_m", min.n = 1L)
  expect_equal(st$n_used, 5L)
  expect_equal(st$n_censored, 1L)
})

test_that("the marker is withheld below min.n but the points are still counted", {
  dm <- .pdTable()
  expect_true(.diveSummaryTable(dm, "amplitude_m", min.n = 6L)$drawn)
  st <- .diveSummaryTable(dm, "amplitude_m", min.n = 7L)
  expect_false(st$drawn)
  expect_equal(st$n_used, 6L)            # withheld marker, NOT withheld data
  expect_false(is.na(st$median))         # the statistic is still returned for the caller
})

test_that("a metric absent from the table yields a row, not an error or a silent skip", {
  st <- .diveSummaryTable(.pdTable(), c("amplitude_m", "no_such_metric"), min.n = 1L)
  expect_equal(nrow(st), 2L)
  miss <- st[st$metric == "no_such_metric", ]
  expect_equal(miss$n_used, 0L)
  expect_equal(miss$n_unsupported, 6L)
  expect_false(miss$drawn)
  expect_true(is.na(miss$median))
})

test_that("a mixed reference within one deployment is reported, not silently reduced to the first", {
  dm <- .pdTable(reference = c(rep("surface", 3), rep("baseline", 3)))
  st <- .diveSummaryTable(dm, "amplitude_m", min.n = 1L)
  expect_equal(st$reference, "baseline/surface")
  # a single-reference deployment stays plain
  expect_equal(.diveSummaryTable(.pdTable(), "amplitude_m", min.n = 1L)$reference, "surface")
})

test_that("groups attach by id, and an ungrouped id stays NA rather than becoming \"NA\"", {
  dm <- rbind(.pdTable(), .pdTable(ID = "B"), .pdTable(ID = "C"))
  st <- .diveSummaryTable(dm, "amplitude_m", min.n = 1L, groups = c(A = "north", B = "south"))
  expect_equal(st$group[st$id == "A"], "north")
  expect_equal(st$group[st$id == "B"], "south")
  expect_true(is.na(st$group[st$id == "C"]))
  expect_false(identical(st$group[st$id == "C"], "NA"))
})

test_that("one row per deployment per metric, in a stable order", {
  dm <- rbind(.pdTable(), .pdTable(ID = "B"))
  st <- .diveSummaryTable(dm, c("amplitude_m", "duration_s"), min.n = 1L)
  expect_equal(nrow(st), 4L)
  expect_equal(st$id, c("A", "A", "B", "B"))
  expect_equal(st$metric, c("amplitude_m", "duration_s", "amplitude_m", "duration_s"))
})

test_that("absolute depths invert the axis and magnitudes do not", {
  # a property of the METRIC, never of `direction` - a benthic rester's upward excursion still has a
  # magnitude that belongs on an upright axis
  expect_true(all(vapply(c("max_depth_m", "mean_depth_m", "baseline_depth_m", "bottom_depth_mean_m"),
                         .diveIsDepth, logical(1))))
  expect_false(any(vapply(c("amplitude_m", "duration_s", "vertical_distance_m", "ascent_rate_q90"),
                          .diveIsDepth, logical(1))))
})

test_that("axis labels fall back to the column name for an unknown metric", {
  expect_equal(.diveLabel("amplitude_m"), "Dive amplitude (m)")
  expect_equal(.diveLabel("inter_dive_s"), "Inter-dive interval (s)")
  expect_equal(.diveLabel("my_custom_col"), "my_custom_col")
  expect_equal(.diveLabel(c("duration_s", "zzz")), c("Dive duration (s)", "zzz"))
})


#######################################################################################################
# Rendering ###########################################################################################

.pdCohort <- function() {
  set.seed(7)
  do.call(rbind, lapply(c("A", "B", "C"), function(id) {
    n <- switch(id, A = 40, B = 12, C = 3)
    data.frame(ID = id, dive_id = seq_len(n),
               amplitude_m = abs(stats::rnorm(n, 20, 8)),
               duration_s  = abs(stats::rnorm(n, 200, 60)),
               max_depth_m = abs(stats::rnorm(n, 30, 10)),
               inter_dive_s = abs(stats::rnorm(n, 60, 20)),
               complete = TRUE, inter_dive_censored = FALSE,
               reference = "surface", direction = "down", stringsAsFactors = FALSE)
  }))
}

test_that("plotDives writes a PDF and returns the summary invisibly", {
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f))
  st <- withVisible(plotDives(.pdCohort(), metrics = c("amplitude_m", "duration_s"),
                              plot = FALSE, plot.file = f, verbose = FALSE))
  expect_false(st$visible)                                  # returned invisibly
  expect_true(file.exists(f) && file.size(f) > 1000)
  expect_equal(nrow(st$value), 6L)                          # 3 deployments x 2 metrics
  expect_true(all(c("n_used", "n_censored", "n_unsupported", "drawn") %in% names(st$value)))
})

test_that("a run with no output target aborts instead of computing and discarding a figure", {
  expect_error(plotDives(.pdCohort(), plot = FALSE, plot.file = NULL, verbose = FALSE),
               "Nothing to draw to")
})

test_that("input that is not a diveMetrics table is refused by name", {
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f))
  expect_error(plotDives(data.frame(ID = "A", x = 1), plot = FALSE, plot.file = f, verbose = FALSE),
               "dive_id")
  expect_error(plotDives(.pdCohort()[0, ], plot = FALSE, plot.file = f, verbose = FALSE),
               "no rows")
})

test_that("a metric with no usable dive anywhere is dropped by name, or aborts if none survive", {
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f))
  dm <- .pdCohort(); dm$all_na <- NA_real_
  expect_warning(plotDives(dm, metrics = c("amplitude_m", "all_na"), plot = FALSE, plot.file = f,
                           verbose = FALSE),
                 "all_na")
  expect_error(plotDives(dm, metrics = "all_na", plot = FALSE, plot.file = f, verbose = FALSE),
               "No requested metric")
})

test_that("min.n withholds the marker for a thin deployment without withholding its points", {
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f))
  st <- plotDives(.pdCohort(), metrics = "amplitude_m", min.n = 5L,
                  plot = FALSE, plot.file = f, verbose = FALSE)
  expect_equal(st$drawn, c(TRUE, TRUE, FALSE))              # C has 3 dives
  expect_equal(st$n_used, c(40L, 12L, 3L))                  # ...but all three are counted
})

test_that("trim bounds the axis without discarding the trimmed dives from the summary", {
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f))
  dm <- .pdCohort(); dm$amplitude_m[1] <- 5000            # one extreme dive
  st <- plotDives(dm, metrics = "amplitude_m", trim = 0.5, plot = FALSE, plot.file = f, verbose = FALSE)
  expect_equal(st$n_used[st$id == "A"], 40L)              # still counted
  expect_equal(st$max[st$id == "A"], 5000)                # still reported at full value
})


test_that("order.by reorders the slots, and the returned table follows the figure", {
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f))
  dm <- .pdCohort()
  dm$amplitude_m <- ifelse(dm$ID == "A", 5, ifelse(dm$ID == "B", 50, 20))   # B > C > A
  p <- function(...) plotDives(dm, metrics = "amplitude_m", plot = FALSE, plot.file = f,
                               verbose = FALSE, ...)
  expect_equal(p(order.by = "id")$id, c("A", "B", "C"))
  expect_equal(p(order.by = "input")$id, c("A", "B", "C"))
  # largest median first, matching plotDistributions' direction
  expect_equal(p(order.by = "median")$id, c("B", "C", "A"))
  # and it really is the median driving it, not the id: reverse the values and the order reverses
  dm$amplitude_m <- ifelse(dm$ID == "A", 50, ifelse(dm$ID == "B", 5, 20))
  expect_equal(p(order.by = "median")$id, c("A", "C", "B"))
})

test_that("order.metric selects which panel drives a shared order", {
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f))
  dm <- .pdCohort()
  dm$amplitude_m <- ifelse(dm$ID == "A", 5,  ifelse(dm$ID == "B", 50, 20))  # B C A
  dm$duration_s  <- ifelse(dm$ID == "A", 50, ifelse(dm$ID == "B", 5,  20))  # A C B
  ord <- function(om) unique(plotDives(dm, metrics = c("amplitude_m", "duration_s"),
                                       order.by = "median", order.metric = om,
                                       plot = FALSE, plot.file = f, verbose = FALSE)$id)
  expect_equal(ord("amplitude_m"), c("B", "C", "A"))
  expect_equal(ord("duration_s"),  c("A", "C", "B"))     # a different panel, a different order
})

test_that("order.metric must be one of the drawn metrics", {
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f))
  expect_error(plotDives(.pdCohort(), metrics = "amplitude_m", order.by = "median",
                         order.metric = "duration_s", plot = FALSE, plot.file = f, verbose = FALSE),
               "order.metric")
})

test_that("labels override the built-in axis label and must be named", {
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f))
  # assert the RESOLUTION, not merely that the call is quiet: a label never reaches the returned table,
  # so expect_silent() around plotDives() passes just as happily when the override is ignored entirely
  expect_equal(.pdResolveLabel("amplitude_m", c(amplitude_m = "Excursion size (m)")),
               "Excursion size (m)")
  expect_equal(.pdResolveLabel("duration_s", c(amplitude_m = "Excursion size (m)")),
               "Dive duration (s)")                                  # untouched metric keeps its own
  # an override wins even over the mixed-reference annotation, because the user asked for it by name
  expect_equal(.pdResolveLabel("max_depth_m", c(max_depth_m = "Depth"), c("surface", "baseline")),
               "Depth")
  expect_equal(.pdResolveLabel("max_depth_m", NULL, c("surface", "baseline")),
               "Max. depth (m) [mixed reference]")
  expect_silent(plotDives(.pdCohort(), metrics = "amplitude_m",
                          labels = c(amplitude_m = "Excursion size (m)"),
                          plot = FALSE, plot.file = f, verbose = FALSE))
  expect_error(plotDives(.pdCohort(), metrics = "amplitude_m", labels = "Excursion size (m)",
                         plot = FALSE, plot.file = f, verbose = FALSE),
               "NAMED")
})

test_that("an absolute depth over a mixed-reference cohort is labelled and warned about", {
  # 30 m against a surface zero and 30 m against a seabed zero are not the same measurement, so the
  # label says so and the call warns once. amplitude_m is immune - it is measured from each dive's
  # own baseline - which is exactly why it is the default.
  expect_equal(.diveLabel("max_depth_m", c("surface", "baseline")), "Max. depth (m) [mixed reference]")
  expect_equal(.diveLabel("max_depth_m", "surface"), "Max. depth (m)")
  expect_equal(.diveLabel("amplitude_m", c("surface", "baseline")), "Dive amplitude (m)")

  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f))
  dm <- .pdCohort(); dm$reference[dm$ID == "B"] <- "baseline"
  expect_warning(plotDives(dm, metrics = "max_depth_m", plot = FALSE, plot.file = f, verbose = FALSE),
                 "not comparable")
  # ...and no warning when the cohort shares one reference, or when the metric is a magnitude
  expect_silent(plotDives(dm, metrics = "amplitude_m", plot = FALSE, plot.file = f, verbose = FALSE))
  expect_silent(plotDives(.pdCohort(), metrics = "max_depth_m", plot = FALSE, plot.file = f,
                          verbose = FALSE))
})


#######################################################################################################
# Grouping ############################################################################################

# Four deployments, each with a constant amplitude, so every per-deployment median is exactly known and
# a median ordering is a fact rather than a coin toss. D is left uncovered by most of the groupings
# below on purpose: a deployment the grouping does not reach is the case the layout has to survive.
.pdGrouped <- function(vals = c(A = 40, B = 30, C = 20, D = 10), n = 8L) {
  do.call(rbind, lapply(names(vals), function(id) {
    data.frame(ID = id, dive_id = seq_len(n),
               amplitude_m = as.numeric(vals[[id]]), duration_s = 100,
               complete = TRUE, inter_dive_censored = FALSE,
               reference = "surface", direction = "down", stringsAsFactors = FALSE)
  }))
}

test_that("the three `group` spec forms resolve to the same per-deployment labels", {
  dm <- .pdGrouped()
  dm$site <- unname(c(A = "north", B = "south", C = "north", D = NA_character_)[dm$ID])
  truth <- c(A = "north", B = "south", C = "north", D = NA_character_)
  vec <- c(A = "north", B = "south", C = "north")
  df  <- data.frame(id = c("A", "B", "C"), grp = c("north", "south", "north"),
                    stringsAsFactors = FALSE)

  expect_equal(.pdGroups(dm, "ID", "site"), truth)     # a column of the table
  expect_equal(.pdGroups(dm, "ID", vec), truth)        # a named id -> group lookup
  expect_equal(.pdGroups(dm, "ID", df), truth)         # a two-column data.frame

  # ...and the plotter honours whichever form it was handed. A quiet call would prove nothing here, so
  # the assertion is on the label that came back attached to each deployment.
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f))
  g <- function(spec) {
    st <- plotDives(dm, metrics = "amplitude_m", group.by = spec, min.n = 1L,
                    plot = FALSE, plot.file = f, verbose = FALSE)
    stats::setNames(st$group, st$id)[c("A", "B", "C", "D")]
  }
  expect_equal(g("site"), truth)
  expect_equal(g(vec), truth)
  expect_equal(g(df), truth)
})

test_that("a deployment the grouping does not cover keeps its slot, in a trailing (ungrouped) block", {
  # Deliberately UNLIKE plotTimeAtDepth, which omits an ungrouped deployment from its facets. Here every
  # deployment owns a column, so dropping one would mean a figure that claims a cohort and shows part.
  dm  <- .pdGrouped()
  grp <- .pdGroups(dm, "ID", c(A = "north", B = "south", C = "north"))
  lay <- .pdLayout(sort(unique(dm$ID)), grp, plotTheme()$palette)

  expect_true("D" %in% lay$ids)                                   # not dropped
  expect_gt(lay$xpos[match("D", lay$ids)], max(lay$xpos[lay$ids != "D"]))   # ...and it comes last
  expect_equal(names(lay$blocks)[length(lay$blocks)], "(ungrouped)")
  expect_gt(min(lay$blocks[["(ungrouped)"]]),
            max(unlist(lay$blocks[names(lay$blocks) != "(ungrouped)"])))

  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f))
  st <- plotDives(dm, metrics = "amplitude_m", group.by = c(A = "north", B = "south", C = "north"),
                  min.n = 1L, plot = FALSE, plot.file = f, verbose = FALSE)
  expect_true("D" %in% st$id)
  expect_equal(nrow(st), 4L)                                      # all four still audited
  expect_true(is.na(st$group[st$id == "D"]))
  # "(ungrouped)" is an axis label for a block, not a group anyone belongs to
  expect_false(any(st$group %in% "(ungrouped)"))
})

test_that("blocks are contiguous inside and separated between", {
  # This is the whole visible content of the blocking: if the between-block step were 1 like every other
  # step, the figure would look identical to an ungrouped one.
  dm  <- .pdGrouped()
  grp <- .pdGroups(dm, "ID", c(A = "north", B = "south", C = "north", D = "south"))
  lay <- .pdLayout(sort(unique(dm$ID)), grp, plotTheme()$palette)   # default gap
  key <- unname(grp[lay$ids])
  steps <- function(l, xp, k) diff(sort(xp[k == l]))
  expect_equal(unlist(lapply(c("north", "south"), steps, lay$xpos, key)), c(1, 1))
  step <- min(lay$xpos[key == "south"]) - max(lay$xpos[key == "north"])
  expect_gt(step, 1)                                               # the break is wider than a slot...
  expect_equal(step, 1.75)                                         # ...by exactly the default gap

  # and the gap is a knob, not a constant: widening it moves the between-block step and nothing else
  wide <- .pdLayout(sort(unique(dm$ID)), grp, plotTheme()$palette, gap = 4)
  kw <- unname(grp[wide$ids])
  expect_equal(unlist(lapply(c("north", "south"), steps, wide$xpos, kw)), c(1, 1))
  expect_equal(min(wide$xpos[kw == "south"]) - max(wide$xpos[kw == "north"]), 5)
})

test_that("blank and whitespace-only group values become NA, not a group named \"\"", {
  # Spreadsheet exports deliver these constantly; a group called "" would get its own block, its own
  # colour and its own axis label.
  dm <- .pdGrouped()
  dm$site <- unname(c(A = "", B = "   ", C = "north", D = "south")[dm$ID])
  grp <- .pdGroups(dm, "ID", "site")
  expect_true(all(is.na(grp[c("A", "B")])))
  expect_equal(unname(grp[c("C", "D")]), c("north", "south"))      # the real labels are untouched
  expect_true(all(nzchar(stats::na.omit(grp))))

  lay <- .pdLayout(sort(unique(dm$ID)), grp, plotTheme()$palette)
  expect_equal(names(lay$blocks), c("north", "south", "(ungrouped)"))
  expect_false("" %in% names(lay$blocks))
  expect_false("   " %in% names(lay$blocks))

  # the named-vector form takes the same path, and a blank there is the same non-answer
  blanks <- .pdGroups(dm, "ID", c(A = "", B = " ", C = "north", D = "south"))
  expect_true(all(is.na(blanks[c("A", "B")])))
  expect_equal(unname(blanks[c("C", "D")]), c("north", "south"))
})

test_that("a grouping that covers no deployment warns and falls back to one ungrouped block", {
  dm <- .pdGrouped()
  expect_warning(grp <- .pdGroups(dm, "ID", c(Z = "north")), "ungrouped")
  expect_true(all(is.na(grp)))
  expect_equal(names(grp), c("A", "B", "C", "D"))                  # nobody is lost on the way

  lay <- .pdLayout(sort(unique(dm$ID)), grp, plotTheme()$palette)
  expect_equal(names(lay$blocks), "(ungrouped)")
  expect_equal(lay$ids, c("A", "B", "C", "D"))
  expect_equal(lay$xpos, c(1, 2, 3, 4))                            # one block, so no gap anywhere

  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f))
  expect_warning(st <- plotDives(dm, metrics = "amplitude_m", group.by = c(Z = "north"), min.n = 1L,
                                 plot = FALSE, plot.file = f, verbose = FALSE), "ungrouped")
  expect_equal(sort(st$id), c("A", "B", "C", "D"))
  expect_true(all(is.na(st$group)))
})

test_that("a single group level draws one block, with no phantom second one", {
  dm  <- .pdGrouped()
  spec <- c(A = "north", B = "north", C = "north", D = "north")
  grp <- .pdGroups(dm, "ID", spec)
  lay <- .pdLayout(sort(unique(dm$ID)), grp, plotTheme()$palette)
  expect_equal(names(lay$blocks), "north")
  expect_equal(lay$ids, c("A", "B", "C", "D"))
  expect_equal(lay$xpos, c(1, 2, 3, 4))                            # no gap: there is nothing to separate
  expect_equal(unname(lay$blocks[["north"]]), c(1, 4))
  expect_equal(length(unique(lay$cols)), 1L)                       # one level, one colour

  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f))
  st <- plotDives(dm, metrics = "amplitude_m", group.by = spec, min.n = 1L,
                  plot = FALSE, plot.file = f, verbose = FALSE)
  expect_equal(st$group, rep("north", 4L))
})

test_that("the returned group column is what was drawn, and NA never becomes the string \"NA\"", {
  dm   <- .pdGrouped()
  spec <- c(A = "north", B = "south", C = "north")
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f))
  st <- plotDives(dm, metrics = c("amplitude_m", "duration_s"), group.by = spec, min.n = 1L,
                  plot = FALSE, plot.file = f, verbose = FALSE)
  grp <- .pdGroups(dm, "ID", spec)
  lay <- .pdLayout(sort(unique(dm$ID)), grp, plotTheme()$palette)

  # every row carries the label its slot was drawn under, on BOTH panels
  expect_equal(nrow(st), 8L)
  expect_equal(st$group, unname(grp[st$id]))
  # and the table reads in the order the columns do, so it can be laid against the figure
  expect_equal(unique(st$id), lay$ids)
  expect_true(is.character(st$group))
  expect_identical(st$group[st$id == "D"], rep(NA_character_, 2L))
  expect_false(any(st$group %in% "NA"))
})

test_that("order.by orders WITHIN a block, never across blocks", {
  # Medians run A 40 > B 30 > C 20 > D 10 and the groups alternate straight down that ranking, so a
  # global ordering would interleave north and south - the one outcome blocking exists to prevent.
  dm   <- .pdGrouped(c(A = 40, B = 30, C = 20, D = 10))
  spec <- c(A = "north", B = "south", C = "north", D = "south")
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f))
  p <- function(...) plotDives(dm, metrics = "amplitude_m", min.n = 1L, plot = FALSE,
                               plot.file = f, verbose = FALSE, ...)
  expect_equal(p(order.by = "median")$id, c("A", "B", "C", "D"))            # ungrouped: the global order
  expect_equal(p(order.by = "median", group.by = spec)$id, c("A", "C", "B", "D"))
  expect_false(identical(p(order.by = "median", group.by = spec)$id, p(order.by = "median")$id))

  # and it is genuinely the median driving the within-block order, not the alphabetical id order it
  # happens to coincide with above: reverse the values and each block reverses while the blocks stay put
  dm <- .pdGrouped(c(A = 10, B = 20, C = 30, D = 40))
  expect_equal(p(order.by = "median", group.by = spec)$id, c("C", "A", "D", "B"))
  expect_equal(p(order.by = "id", group.by = spec)$id, c("A", "C", "B", "D"))
  expect_equal(p(order.by = "median")$id, c("D", "C", "B", "A"))            # ...global order did move
})


#######################################################################################################
# The render layer ####################################################################################
#
# .pdDrawPanel() returns a record of what it actually drew. Without it this whole layer is unverifiable:
# an adversarial pass planted 17 mutations here - trim deleted, the depth inversion deleted, the median
# drawn at q25, the IQR drawn as min..max - and every one of them passed a green suite.

.pdDraw <- function(dm, metric, min.n = 1L, trim = 0.95, theme = plotTheme()) {
  ids <- unique(as.character(dm$ID))
  st  <- .diveSummaryTable(dm, metric, min.n = min.n)
  grDevices::pdf(NULL); on.exit(grDevices::dev.off(), add = TRUE)
  graphics::par(mar = c(1.9, 4.4, 0.8, 1.2))
  .pdDrawPanel(dm, st, metric, ids, theme, trim, "ID")
}

.pdMk <- function(id, v, metric = "amplitude_m", ...) {
  d <- data.frame(ID = id, dive_id = seq_along(v), duration_s = 1, complete = TRUE,
                  stringsAsFactors = FALSE)
  d[[metric]] <- v
  o <- list(...); for (nm in names(o)) d[[nm]] <- o[[nm]]
  d
}

test_that("trim bounds the axis and pins the excess rather than dropping it", {
  d <- .pdMk("A", c(rep(10, 99), 5000))
  loose <- .pdDraw(d, "amplitude_m", trim = 1)
  tight <- .pdDraw(d, "amplitude_m", trim = 0.5)
  expect_equal(loose$hi, 5000)                    # trim = 1 shows everything
  expect_equal(unname(loose$trimmed["A"]), 0L)
  expect_lt(tight$hi, 5000)                       # ...and a tighter trim really does cut the axis
  expect_gt(unname(tight$trimmed["A"]), 0L)       # ...with the excess counted, not discarded
})

test_that("the axis always contains the markers the figure claims to draw", {
  # a deployment whose median sits above the POOLED trim quantile used to have its marker clipped to
  # nothing while the table still said drawn = TRUE - visually identical to a min.n suppression
  d <- rbind(.pdMk("SHALLOW1", rep(10, 60)), .pdMk("SHALLOW2", rep(10, 60)),
             .pdMk("DEEP", rep(1000, 6)))
  r <- .pdDraw(d, "amplitude_m", min.n = 5)
  expect_false(is.null(r$markers[["DEEP"]]))
  expect_lte(r$markers[["DEEP"]][["q75"]], r$hi)
  expect_gte(r$markers[["DEEP"]][["q25"]], r$lo)
})

test_that("an absolute depth inverts the axis and a magnitude does not", {
  d <- .pdMk("A", c(5, 10, 20, 30), metric = "max_depth_m")
  r <- .pdDraw(d, "max_depth_m")
  expect_true(r$inverted)
  expect_gt(r$ylim[1], r$ylim[2])                 # zero at the TOP
  d2 <- .pdMk("A", c(5, 10, 20, 30))
  r2 <- .pdDraw(d2, "amplitude_m")
  expect_false(r2$inverted)
  expect_lt(r2$ylim[1], r2$ylim[2])
})

test_that("the marker is the MEDIAN with the IQR, not some other pair of quantiles", {
  d <- .pdMk("A", c(1, 2, 3, 4, 100))            # median 3, q25 2, q75 4, min 1, max 100
  m <- .pdDraw(d, "amplitude_m", trim = 1)$markers[["A"]]
  expect_equal(unname(m[["median"]]), 3)
  expect_equal(unname(m[["q25"]]), 2)
  expect_equal(unname(m[["q75"]]), 4)
  expect_false(unname(m[["q75"]]) == 100)        # not min..max
  expect_false(unname(m[["median"]]) == 2)       # not q25 masquerading as the centre
})

test_that("min.n withholds the marker from the figure, not only from the table", {
  d <- .pdMk("A", c(1, 2, 3))
  expect_null(.pdDraw(d, "amplitude_m", min.n = 5)$markers[["A"]])
  expect_false(is.null(.pdDraw(d, "amplitude_m", min.n = 3)$markers[["A"]]))
})

test_that("excluded dives below the axis floor are pinned and counted, not clipped away", {
  # documented as "drawn in outline and counted"; a censored value below `lo` used to be neither
  d <- .pdMk("A", c(rep(0.5, 8), -9, -9), complete = c(rep(TRUE, 8), FALSE, FALSE))
  r <- .pdDraw(d, "amplitude_m", trim = 1)
  expect_lte(r$lo, -9)                            # the floor reaches them...
  expect_equal(unname(r$trimmed["A"]), 0L)        # ...so nothing needed pinning
  tight <- .pdDraw(d, "amplitude_m", trim = 0.5)
  expect_true(is.finite(tight$hi) && tight$hi >= tight$lo)
})

test_that("n_trimmed and axis_max reach the returned table so the figure is reproducible from it", {
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f))
  d <- .pdMk("A", c(rep(10, 99), 5000))
  st <- plotDives(d, metrics = "amplitude_m", trim = 0.5, plot = FALSE, plot.file = f, verbose = FALSE)
  expect_true(all(c("n_trimmed", "axis_max") %in% names(st)))
  expect_gt(st$n_trimmed, 0L)
  expect_lt(st$axis_max, 5000)
  expect_equal(st$max, 5000)                      # the true extreme is still reported
})

test_that("a censored dive with no value counts as censored, not as unsupported", {
  # the two exclusions are correlated - a truncated dive is exactly the one whose phases do not
  # resolve - so requiring a finite value before counting censoring made n_censored read 0 while the
  # censored dives inflated n_unsupported. Measured on real data at 18% of that bucket.
  dm <- data.frame(ID = "A", dive_id = 1:4, m = c(1, 2, NA, NA), duration_s = 1,
                   complete = c(TRUE, TRUE, TRUE, FALSE), stringsAsFactors = FALSE)
  st <- .diveSummaryTable(dm, "m", min.n = 1L)
  expect_equal(st$n_used, 2L)
  expect_equal(st$n_censored, 1L)                 # dive 4: censored AND valueless
  expect_equal(st$n_unsupported, 1L)              # dive 3: valueless only
  expect_equal(st$n_used + st$n_censored + st$n_unsupported, st$n_dives)
})

test_that("a factor metric is coerced by value, not by level code", {
  dm <- data.frame(ID = "A", dive_id = 1:5, m = factor(c(10, 20, 30, 40, 50)),
                   duration_s = 1, complete = TRUE, stringsAsFactors = FALSE)
  expect_equal(.diveSummaryTable(dm, "m", min.n = 1L)$median, 30)   # not 3
})

test_that("rows with no deployment id are dropped loudly, not turned into a phantom column", {
  f <- tempfile(fileext = ".pdf"); on.exit(unlink(f))
  d <- rbind(.pdMk("A", 1:5), .pdMk("B", 1:5)); d$ID[3] <- NA
  expect_warning(st <- plotDives(d, metrics = "amplitude_m", plot = FALSE, plot.file = f,
                                 verbose = FALSE), "no")
  expect_equal(st$id, c("A", "B"))
  expect_equal(st$n_dives, c(4L, 5L))             # A really has 4 rows left, and says so
})

test_that("a non-logical `complete` column does not silently produce NA counts", {
  dm <- data.frame(ID = "A", dive_id = 1:3, m = c(1, 2, 3), duration_s = 1,
                   complete = c("yes", "yes", "yes"), stringsAsFactors = FALSE)
  st <- .diveSummaryTable(dm, "m", min.n = 1L)
  expect_false(is.na(st$n_used))                  # as.logical("yes") is a silent NA
  expect_equal(st$n_used + st$n_censored + st$n_unsupported, st$n_dives)
})
