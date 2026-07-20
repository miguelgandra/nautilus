# Tests for the redesigned summarizeTagData():
#   - returns a typed (numeric/POSIXct) nautilus_summary, one row per deployment (no in-table mean row)
#   - deployment window comes from metadata, distinct from the recording span
#   - biological + kinematic metrics; graceful NA for absent columns
#   - print method renders + appends a display-only mean +/- error footer
#   - extra.metadata covariate join; summary.nautilus_tag reuse

# a processed-style fixture WITH consolidated metadata (deploy window, sampling, tag fields)
.mk <- function(id, n = 120, withpos = TRUE, withdepth = TRUE, withkin = TRUE, withtbf = FALSE, withpaddle = FALSE) {
  t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
  d <- data.table::data.table(ID = id, datetime = t0 + 0:(n - 1))
  if (withdepth) { d[, depth := seq(0, 50, length.out = n)]; d[, temp := seq(18, 22, length.out = n)] }
  if (withkin)   { d[, vedba := seq(0, 0.5, length.out = n)]; d[, odba := seq(0, 0.6, length.out = n)]
                   d[, vertical_velocity := sin(seq_len(n))] }     # spans descent (+) and ascent (-)
  if (withtbf)   { d[, tbf_hz := ifelse(seq_len(n) %% 2 == 0, 0.8, NA_real_)]   # beating (0.8 Hz) half the time
                   d[, tbf_swimming := as.integer(seq_len(n) %% 2 == 0)] }
  if (withpos)   d[, position_type := rep(c(NA, "FastGPS", "User"), length.out = n)]
  m <- nautilus:::.newNautilusMeta()
  m$id <- id
  m$tag$model <- "CATS"; m$tag$type <- "Camera"
  m$deployment$datetime          <- t0 - 3600                    # deployed 1 h before the data starts
  m$deployment$popup_datetime    <- t0 + (n - 1) + 3600          # popped 1 h after the data ends
  m$deployment$attachment_site   <- "dorsal"
  m$deployment$magnetic_declination <- -7.5
  m$tag$paddle_wheel              <- withpaddle
  m$sensors$sampling_hz_original  <- 50
  m$sensors$sampling_hz_processed <- 1
  nautilus:::new_nautilus_tag(d, m)
}

.run <- function(...) {
  res <- NULL
  invisible(capture.output(suppressWarnings(suppressMessages(res <- summarizeTagData(..., verbose = FALSE)))))
  res
}

# a MINIMAL tag for the malformed-input guards (no deploy metadata, controllable ID / datetime columns)
.mk_raw <- function(ids = "A", datetime = TRUE, id = "A", n = 50L) {
  t0 <- as.POSIXct("2020-01-01", tz = "UTC")
  d  <- data.table::data.table(ID = rep(ids, length.out = n), depth = as.numeric(seq_len(n)))
  if (datetime) d[, datetime := t0 + seq_len(n)]
  m <- nautilus:::.newNautilusMeta(); m$id <- id
  nautilus:::new_nautilus_tag(d, m)
}

# run an expression, capturing BOTH the value and every warning it emits (messages muffled)
.warns <- function(expr) {
  w <- character()
  r <- withCallingHandlers(suppressMessages(expr),
    warning = function(cnd) { w <<- c(w, conditionMessage(cnd)); invokeRestart("muffleWarning") })
  list(result = r, warnings = w)
}

test_that("returns a typed nautilus_summary, one row per deployment (no in-table mean row)", {
  out <- .run(list(A = .mk("A"), B = .mk("B")))
  expect_s3_class(out, "nautilus_summary")
  expect_equal(nrow(out), 2L)                                     # data has NO mean row
  expect_equal(out$id, c("A", "B"))
  # numeric columns stay numeric; datetimes stay POSIXct (not stringified)
  expect_type(out$depth_max, "double")
  expect_type(out$record_duration_h, "double")
  expect_s3_class(out$record_start, "POSIXct")
})

test_that("the recording window is the data's own span; deploy/popup window is NOT duplicated", {
  out <- .run(list(A = .mk("A", n = 120)))
  expect_s3_class(out$record_start, "POSIXct")
  expect_equal(out$record_start, as.POSIXct("2020-01-01 00:00:00", tz = "UTC"))
  expect_equal(out$n_samples, 120L)
  expect_false("deploy_start" %in% names(out))            # the broad metadata window is no longer duplicated
  expect_false("sampling_hz_processed" %in% names(out))   # processing constant dropped
  expect_false("declination_deg" %in% names(out))         # processing metadata dropped
  expect_false("status" %in% names(out))                  # status only appears with `deployments`
  expect_equal(out$sampling_hz, 50)                        # original rate (varies per tag) kept
  expect_equal(out$tag_model, "CATS")
})

test_that("paddle_wheel flag is reported (disambiguates the speed columns)", {
  out <- .run(list(A = .mk("A", withpaddle = TRUE), B = .mk("B", withpaddle = FALSE)))
  expect_type(out$paddle_wheel, "logical")
  expect_true(out$paddle_wheel[out$id == "A"])
  expect_false(out$paddle_wheel[out$id == "B"])
})

test_that("deployments= completes the study roster with an included/excluded status", {
  dep <- data.frame(id = c("A", "B", "C", "D"), tag_model = "CATS", tag_type = "Camera",
                    attachment_site = "dorsal", paddle_wheel = c(TRUE, FALSE, TRUE, FALSE),
                    stringsAsFactors = FALSE)
  class(dep) <- c("nautilus_deployments", "data.frame")
  out <- .run(list(A = .mk("A"), B = .mk("B")), deployments = dep)   # only A, B were processed
  expect_equal(nrow(out), 4L)                                        # all four deployments appear
  expect_equal(out$id, c("A", "B", "C", "D"))                        # ordered by the roster
  expect_equal(out$status, c("included", "included", "excluded", "excluded"))
  # excluded rows: identity from the roster, metrics NA
  expect_equal(out$tag_model[out$id == "C"], "CATS")
  expect_true(out$paddle_wheel[out$id == "C"])
  expect_true(all(is.na(out$depth_max[out$status == "excluded"])))
  # status sits right after the identity block
  expect_equal(which(names(out) == "status"), which(names(out) == "attachment_site") + 1L)
  # a non-deployments object is rejected
  expect_error(summarizeTagData(list(A = .mk("A")), deployments = data.frame(id = "A"), verbose = FALSE),
               "nautilus_deployments", ignore.case = TRUE)
})

test_that("accepts a character vector of .rds file paths (pipeline-consistent input)", {
  d <- tempfile(); dir.create(d); on.exit(unlink(d, recursive = TRUE), add = TRUE)
  pA <- file.path(d, "A.rds"); pB <- file.path(d, "B.rds")
  saveRDS(.mk("A"), pA); saveRDS(.mk("B"), pB)
  out <- .run(c(pA, pB))
  expect_s3_class(out, "nautilus_summary")
  expect_equal(out$id, c("A", "B")); expect_equal(nrow(out), 2L)
  expect_error(summarizeTagData(file.path(d, "nope.rds"), verbose = FALSE), "not found", ignore.case = TRUE)
})

test_that("tail-beat metrics are summarised when present, NA otherwise (fixed schema)", {
  out <- .run(list(A = .mk("A", withtbf = TRUE)))
  expect_equal(out$tbf_mean, 0.8, tolerance = 1e-6)       # mean over beating samples (NA samples excluded)
  expect_equal(out$pct_swimming, 50, tolerance = 1e-6)    # half the samples flagged swimming
  out0 <- .run(list(B = .mk("B")))                         # no tail-beat columns
  expect_true(is.na(out0$tbf_mean)); expect_true(is.na(out0$pct_swimming))
})

test_that("biological + kinematic metrics are computed", {
  out <- .run(list(A = .mk("A")))
  expect_equal(out$depth_max, 50, tolerance = 1e-6)
  expect_equal(out$temp_max, 22, tolerance = 1e-6)
  expect_gt(out$vedba_mean, 0); expect_lt(out$vedba_mean, 0.5)
  expect_true(is.finite(out$odba_mean))
  expect_gt(out$descent_rate_max, 0)                             # fastest descent (positive vv)
  expect_gt(out$ascent_rate_max, 0)                             # fastest ascent (reported positive)
})

test_that("absent columns yield NA, not errors (partial sensor sets)", {
  out <- .run(list(A = .mk("A", withdepth = FALSE, withkin = FALSE, withpos = FALSE),
                   B = .mk("B", withdepth = FALSE, withkin = FALSE, withpos = FALSE)))
  expect_equal(nrow(out), 2L)
  expect_true(all(is.na(out$depth_max)))
  expect_true(all(is.na(out$vedba_mean)))
  expect_true(all(is.na(out$n_positions)))
})

test_that("extra.metadata covariates are joined and kept typed", {
  meta <- data.table::data.table(ID = c("A", "B"), Size = c(5, 6), Sex = c("M", "F"))
  out <- .run(list(A = .mk("A"), B = .mk("B")), extra.metadata = meta)
  expect_true(all(c("Size", "Sex") %in% names(out)))
  expect_equal(out$Size[out$id == "A"], 5)                       # numeric covariate stays numeric
  expect_equal(out$Sex[out$id == "B"], "F")
  # id.col need not be first; merge is driven by ID, not column position
  meta2 <- data.frame(Sex = c("M", "F"), ID = c("A", "B"), stringsAsFactors = FALSE)
  out2 <- .run(list(A = .mk("A"), B = .mk("B")), extra.metadata = meta2)
  expect_equal(out2$Sex[out2$id == "A"], "M")
})

test_that("print appends a display-only mean +/- error footer (multi-deployment)", {
  res <- .run(list(A = .mk("A"), B = .mk("B")))
  lines <- capture.output(print(res))
  pm <- if (cli::is_utf8_output()) "\u00b1" else "+/-"           # locale-safe plus-minus marker
  expect_true(any(grepl(paste0("mean ", pm), lines, fixed = TRUE)))   # footer row present
  # the DATA object is unchanged by printing (still 2 rows, still numeric)
  expect_equal(nrow(res), 2L)
  expect_type(res$depth_max, "double")
})

test_that("format() exposes the formatted table for export (write.csv-ready)", {
  s <- .run(list(A = .mk("A"), B = .mk("B")))
  fmt <- format(s)
  expect_s3_class(fmt, "data.frame")
  expect_true(all(vapply(fmt, is.character, logical(1))))         # all character -> exports cleanly
  expect_equal(nrow(fmt), 3L)                                     # 2 deployments + the mean +/- footer
  expect_match(fmt$id[nrow(fmt)], "mean")                        # footer row
  expect_equal(nrow(format(s, include.summary.row = FALSE)), 2L) # footer is suppressible
  # the underlying typed object is untouched by formatting / printing
  expect_type(s$depth_max, "double")
  # round-trips to CSV without error
  f <- tempfile(fileext = ".csv"); on.exit(unlink(f), add = TRUE)
  expect_no_error(write.csv(format(s), f, row.names = FALSE))
  expect_gt(length(readLines(f)), 3L)
  # empty summary (built directly - empty INPUT now errors) -> empty formatted frame
  empty_s <- nautilus:::.newSummary(nautilus:::.summaryTemplate())
  expect_equal(nrow(format(empty_s)), 0L)
})

test_that("format(style = 'report') relabels columns with publication headers (values unchanged)", {
  s <- .run(list(A = .mk("A")),
            extra.metadata = data.frame(ID = "A", tagging_site = "Pico", stringsAsFactors = FALSE))
  internal <- format(s)
  report   <- format(s, style = "report")
  expect_equal(dim(internal), dim(report))                       # same shape, only names differ
  expect_equal(unname(unlist(internal[1, ])), unname(unlist(report[1, ])))   # identical values
  expect_true("Max depth (m)" %in% names(report))
  expect_true("Mean VeDBA (g)" %in% names(report))
  expect_true("Tagging site" %in% names(report))                 # covariate prettified, not in the dict
  expect_false(any(grepl("_", names(report))))                   # no snake_case left
  # the console / auto-print is unaffected by the report style
  expect_true(any(grepl("depth_max", capture.output(print(s)), fixed = TRUE)))
})

test_that("format(style = 'concise') abbreviates the publication headers (same values)", {
  s <- .run(list(A = .mk("A", withtbf = TRUE, withpaddle = TRUE)))
  concise <- format(s, style = "concise")
  expect_equal(dim(concise), dim(format(s)))
  expect_equal(unname(unlist(format(s)[1, ])), unname(unlist(concise[1, ])))    # identical values, only names differ
  expect_true(all(c("Start", "End", "Duration (h)", "Rate (Hz)", "Attach. site",
                    "Mean TBF (Hz)", "Swimming (%)", "Positions (n)") %in% names(concise)))
  expect_true("Mean speed (m s\u207b\u00b9)" %in% names(concise))               # superscript unit form
  expect_false(any(grepl("_", names(concise))))
})

test_that("datetime.format controls the record datetime columns", {
  s <- .run(list(A = .mk("A")))
  iso <- format(s, datetime.format = "%Y-%m-%d %H:%M:%S")
  expect_equal(iso$record_start[1], "2020-01-01 00:00:00")
  expect_match(format(s)$record_start[1], "01/Jan/2020")          # default unchanged
  expect_error(format(s, datetime.format = 1), "datetime.format")
})

test_that("single deployment prints with no footer and no '+/- NA'", {
  res <- .run(list(A = .mk("A")))
  lines <- capture.output(print(res))
  pm <- if (cli::is_utf8_output()) "\u00b1" else "+/-"
  expect_false(any(grepl(pm, lines, fixed = TRUE)))         # no aggregate row for n = 1
})

test_that("summary.nautilus_tag returns a one-row nautilus_summary", {
  s <- summary(.mk("A"))
  expect_s3_class(s, "nautilus_summary")
  expect_equal(nrow(s), 1L)
  expect_equal(s$id, "A")
})

test_that("detailed verbose reports metric coverage (and the roster split when completed)", {
  grab <- function(...) paste(cli::cli_fmt(suppressWarnings(summarizeTagData(...))), collapse = "\n")
  d2 <- grab(list(A = .mk("A", withtbf = TRUE), B = .mk("B")), verbose = 2)
  expect_match(d2, "metric coverage")
  expect_match(d2, "tail-beats 1")                          # only A has tail-beat data
  d1 <- grab(list(A = .mk("A")), verbose = 1)
  expect_false(grepl("metric coverage", d1))               # coverage is level-2 only
  # roster split surfaces at level 1 when deployments is supplied
  dep <- data.frame(id = c("A", "B", "C"), tag_model = "CATS", stringsAsFactors = FALSE)
  class(dep) <- c("nautilus_deployments", "data.frame")
  dr <- grab(list(A = .mk("A")), deployments = dep, verbose = 1)
  expect_match(dr, "1 included, 2 excluded")
})

test_that("verbose = FALSE is silent", {
  out <- capture.output(suppressWarnings(suppressMessages(
    res <- summarizeTagData(list(A = .mk("A"), B = .mk("B")), verbose = FALSE))))
  expect_length(out, 0)
})


# --- input-validation hardening (Tiers 1 + 2) ---------------------------------
# malformed deployments are skipped with an informative warning (never a silent bad row or a crash);
# reserved-column / ID-mismatch / multi-value covariate joins are guarded; the roster is always completed.

test_that("a deployment with no valid datetime is skipped with a warning, not a bad row", {
  wr <- .warns(summarizeTagData(list(A = .mk("A"), B = .mk_raw(id = "B", datetime = FALSE)), verbose = FALSE))
  expect_equal(nrow(wr$result), 1L)                          # only the valid deployment survives
  expect_equal(wr$result$id, "A")
  expect_true(any(grepl("datetime", wr$warnings)))
})

test_that("a deployment table with more than one animal ID is rejected (one animal per deployment)", {
  wr <- .warns(summarizeTagData(list(X = .mk_raw(ids = c("A", "B"), id = "X")), verbose = FALSE))
  expect_equal(nrow(wr$result), 0L)
  expect_true(any(grepl("ID", wr$warnings)))
})

test_that("NULL / malformed deployments are dropped and the warning reports the true input count", {
  wr <- .warns(summarizeTagData(list(A = .mk("A"), B = NULL, C = .mk("C")), verbose = FALSE))
  expect_equal(nrow(wr$result), 2L)
  expect_setequal(wr$result$id, c("A", "C"))
  expect_true(any(grepl("1 of 3", wr$warnings)))             # count reflects the full input, not the survivors
})

test_that("extra.metadata columns that clash with reserved summary fields abort clearly", {
  expect_error(
    summarizeTagData(list(A = .mk("A")), extra.metadata = data.frame(ID = "A", depth_max = 9), verbose = FALSE),
    "clash", ignore.case = TRUE)
})

test_that("extra.metadata whose IDs match nothing warns (covariates left NA, not silently absent)", {
  wr <- .warns(summarizeTagData(list(A = .mk("A")),
                                extra.metadata = data.frame(ID = "ZZ", tagger = "M"), verbose = FALSE))
  expect_true(any(grepl("no 'extra.metadata' ID", wr$warnings, fixed = TRUE)))
  expect_true("tagger" %in% names(wr$result))
  expect_true(is.na(wr$result$tagger))
})

test_that("a numeric covariate with multiple distinct values per animal warns before aggregating", {
  wr <- .warns(summarizeTagData(list(A = .mk("A")),
                                extra.metadata = data.frame(ID = c("A", "A"), length_cm = c(5, 7)), verbose = FALSE))
  expect_true(any(grepl("multiple values", wr$warnings)))
  expect_true(is.na(wr$result$length_cm))                    # ambiguous value collapses to NA
})

test_that("an all-excluded roster is produced when every processed deployment is malformed", {
  dep <- data.frame(id = c("A", "B", "C"), tag_model = c("mk10", "mk9", "mk10"),
                    paddle_wheel = c("yes", "no", "yes"), stringsAsFactors = FALSE)
  class(dep) <- c("nautilus_deployments", "data.frame")
  bad <- list(A = .mk_raw(id = "A", datetime = FALSE),           # non-empty but malformed (no datetime) -> all dropped
              B = .mk_raw(id = "B", datetime = FALSE),
              C = .mk_raw(id = "C", datetime = FALSE))
  wr <- .warns(summarizeTagData(bad, deployments = dep, verbose = FALSE))
  expect_equal(nrow(wr$result), 3L)
  expect_true(all(wr$result$status == "excluded"))
  expect_equal(wr$result$tag_model, c("mk10", "mk9", "mk10"))     # identity carried from the roster
  expect_equal(wr$result$paddle_wheel, c(TRUE, FALSE, TRUE))      # coerced to logical
  expect_true(all(is.na(wr$result$depth_max)))                   # metrics NA
})

test_that("empty data is a loud error - even with a deployments roster (catches a mistyped input path)", {
  dep <- data.frame(id = c("A", "B"), stringsAsFactors = FALSE)
  class(dep) <- c("nautilus_deployments", "data.frame")
  # the originally-reported bug: list.files() on a typo'd directory -> character(0) -> silent empty summary
  expect_error(summarizeTagData(character(0), verbose = FALSE), "empty", ignore.case = TRUE)
  expect_error(summarizeTagData(list(), deployments = dep, verbose = FALSE), "empty", ignore.case = TRUE)
})

test_that("a deployments roster missing its 'id' column aborts", {
  bad <- data.frame(animal = c("A", "B"), stringsAsFactors = FALSE)
  class(bad) <- c("nautilus_deployments", "data.frame")
  expect_error(summarizeTagData(list(A = .mk("A")), deployments = bad, verbose = FALSE),
               "id", ignore.case = TRUE)
})

test_that("a text roster paddle_wheel of NA stays NA (unknown), not silently coerced to FALSE", {
  dep <- data.frame(id = c("A", "B", "C"), paddle_wheel = c("yes", NA, "no"), stringsAsFactors = FALSE)
  class(dep) <- c("nautilus_deployments", "data.frame")
  out <- .run(list(A = .mk("A")), deployments = dep)          # B, C excluded -> identity from the roster
  expect_true(is.na(out$paddle_wheel[out$id == "B"]))         # NA text -> NA logical (not FALSE)
  expect_false(out$paddle_wheel[out$id == "C"])               # "no" -> FALSE
})

test_that("a covariate named 'status' is rejected even without a deployments roster", {
  # 'status' is a structural column; a covariate of that name would silently break the roster split
  expect_error(
    summarizeTagData(list(A = .mk("A"), B = .mk("B")),
                     extra.metadata = data.frame(ID = c("A", "B"), status = c("ok", "bad")), verbose = FALSE),
    "clash", ignore.case = TRUE)
})

test_that("a partial-coverage covariate join preserves the deployment/roster row order", {
  depo <- data.frame(id = c("zeb", "alpha", "mid"), stringsAsFactors = FALSE)
  class(depo) <- c("nautilus_deployments", "data.frame")
  out <- .run(list(zeb = .mk("zeb"), alpha = .mk("alpha"), mid = .mk("mid")),
              deployments = depo, extra.metadata = data.frame(ID = "mid", sex = "F"))   # covariate for 1 of 3
  expect_equal(out$id, c("zeb", "alpha", "mid"))              # merge() must not hoist the matched row to the front
  expect_equal(out$sex[out$id == "mid"], "F")                # covariate still correctly keyed
  expect_true(is.na(out$sex[out$id == "zeb"]))
})

test_that(".summaryTemplate() matches the .summarize() schema exactly (empty-path type-safety)", {
  # NOTE: this guard is deliberately blind to the dive block. .summaryTemplate() is dive-free because
  # the block only exists for deployments detectDives() has annotated, and .mk() is not one - so the
  # eight dive columns are pinned by the dive-block tests below, not here.
  tmpl <- nautilus:::.summaryTemplate()
  real <- nautilus:::.summarize(.mk("A"))
  expect_equal(names(tmpl), names(real))                          # same columns, same order
  expect_equal(nrow(tmpl), 0L)
  expect_equal(vapply(tmpl, function(x) class(x)[1], character(1)),   # same column classes -> rbind is type-safe
               vapply(real, function(x) class(x)[1], character(1)))
})


# --- the dive block -----------------------------------------------------------
# Eight columns appear once detectDives() has annotated a deployment. They are NOT recomputed here: the
# block calls the same reducer diveMetrics() calls, which is the only reason a dive count quoted from
# the summary and one quoted from the per-dive table cannot disagree in a paper.

.sumDiveCols <- c("n_dives", "dive_duration_median_min", "dive_duration_max_min",
                  "dive_depth_median_m", "dive_depth_max_m",
                  "dives_incomplete", "dives_truncated", "dives_gapped")

# six dives of deliberately different character, so no two of the eight numbers coincide by accident:
# three clean (299 s / 20 m, 599 s / 40 m, 199 s / 10 m), two left behind by a 99 s depth dropout, and
# one running off the end of the record
.sumDiveProfile <- c(rep(0, 60), rep(20, 300), rep(0, 60), rep(40, 600), rep(0, 60), rep(10, 200),
                     rep(0, 60), rep(30, 300), rep(NA_real_, 100), rep(30, 300),
                     rep(0, 60), rep(25, 300))

# a deployment carrying that profile, put through detectDives() exactly as a user would before summarising
.mk_dived <- function(id, depth = .sumDiveProfile) {
  n  <- length(depth)
  t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
  d  <- data.table::data.table(ID = id, datetime = t0 + 0:(n - 1), depth = as.numeric(depth))
  m  <- nautilus:::.newNautilusMeta(); m$id <- id
  m$tag$model <- "CATS"
  m$sensors$sampling_hz_original <- 50
  m <- nautilus:::.appendProcessing(m, "depth_drift", status = "applied",
                                    outcome = list(residual_m = 0.1))
  ctl <- diveControl(reference = "surface", depth.threshold = 5, surface.band = 2,
                     min.prominence = 5, min.duration = 10, max.gap = 60)
  detectDives(list(nautilus:::new_nautilus_tag(d, m)), control = ctl, verbose = FALSE)[[1]]
}

test_that("an annotated deployment gains the eight dive columns, correctly typed", {
  out <- .run(list(A = .mk_dived("A")))
  expect_true(all(.sumDiveCols %in% names(out)))
  # counts stay integer and statistics stay double, so a later rbind with an unannotated cohort is safe
  expect_type(out$n_dives, "integer")
  expect_type(out$dives_incomplete, "integer")
  expect_type(out$dives_truncated, "integer")
  expect_type(out$dives_gapped, "integer")
  for (cc in c("dive_duration_median_min", "dive_duration_max_min",
               "dive_depth_median_m", "dive_depth_max_m")) expect_type(out[[cc]], "double")
  # the block is appended after the kinematic metrics, in the documented order
  expect_equal(utils::tail(names(out), 8L), .sumDiveCols)
  # a deployment that never saw detectDives() carries none of them
  expect_false(any(.sumDiveCols %in% names(.run(list(B = .mk("B"))))))
})

test_that("a MIXED cohort binds: the unannotated deployments come back NA, not an error", {
  out <- .run(list(A = .mk_dived("A"), B = .mk("B")))
  expect_equal(nrow(out), 2L)
  expect_true(all(.sumDiveCols %in% names(out)))
  a <- out[out$id == "A", , drop = FALSE]
  b <- out[out$id == "B", , drop = FALSE]
  expect_equal(a$n_dives, 6L)
  expect_true(all(vapply(.sumDiveCols, function(cc) is.na(b[[cc]]), logical(1))))
  # NA of the RIGHT type - a fill that flipped the column to logical would break the next bind
  expect_type(out$n_dives, "integer")
  expect_type(out$dive_depth_max_m, "double")
  # and the schema does not depend on which deployment came first - identical, not merely the same SET,
  # since a column ORDER that tracked the input order is exactly the drift this is meant to catch
  expect_identical(names(.run(list(B = .mk("B"), A = .mk_dived("A")))), names(out))
})

test_that("a deployment annotated with no dives reports 0, and NA statistics rather than 0 statistics", {
  flat <- .mk_dived("FLAT", rep(0.2, 300))
  expect_true(all(flat$dive_id == 0L))                      # annotated, but nothing detected
  out <- .run(list(FLAT = flat))
  expect_equal(out$n_dives, 0L)
  expect_equal(out$dives_incomplete, 0L)
  expect_equal(out$dives_truncated, 0L)
  expect_equal(out$dives_gapped, 0L)
  # "no dives" is not "dives of length zero": counting over an empty table is 0, averaging over it is NA
  expect_true(is.na(out$dive_duration_median_min))
  expect_true(is.na(out$dive_duration_max_min))
  expect_true(is.na(out$dive_depth_median_m))
  expect_true(is.na(out$dive_depth_max_m))
})

test_that("the dive block is exactly a reduction of diveMetrics() on the same data", {
  # The test that catches the two functions drifting apart. Every column is checked against a hand
  # reduction of the per-dive table, on a cohort chosen so a plausible shortcut gets a different answer.
  tags <- list(RICH  = .mk_dived("RICH"),
               # a dive that occupies the WHOLE record: dive_id never takes the value 0 here, so a count
               # taken from the per-sample column rather than from the reducer reads 0 instead of 1
               WHOLE = .mk_dived("WHOLE", rep(20, 400)))
  out <- as.data.frame(.run(tags))
  for (k in seq_along(tags)) {
    dm <- diveMetrics(tags[[k]], verbose = FALSE)
    r  <- out[out$id == names(tags)[k], , drop = FALSE]
    expect_equal(r$n_dives, nrow(dm))
    expect_equal(r$dive_duration_median_min, stats::median(dm$duration_s) / 60, tolerance = 1e-10)
    expect_equal(r$dive_duration_max_min, max(dm$duration_s) / 60, tolerance = 1e-10)
    expect_equal(r$dive_depth_median_m, stats::median(dm$max_depth_m), tolerance = 1e-10)
    expect_equal(r$dive_depth_max_m, max(dm$max_depth_m), tolerance = 1e-10)
    expect_equal(r$dives_incomplete, sum(!dm$complete))
    expect_equal(r$dives_truncated, sum(dm$truncated_start | dm$truncated_end))
    expect_equal(r$dives_gapped, sum(dm$n_gaps > 0))
  }
  # the eight numbers really are distinguishable - a fixture where they coincided would prove nothing
  rich <- out[out$id == "RICH", , drop = FALSE]
  expect_equal(rich$n_dives, 6L)
  expect_equal(rich$dive_duration_median_min, 299 / 60, tolerance = 1e-10)
  expect_equal(rich$dive_duration_max_min, 599 / 60, tolerance = 1e-10)
  expect_equal(rich$dive_depth_median_m, 27.5, tolerance = 1e-10)
  expect_equal(rich$dive_depth_max_m, 40, tolerance = 1e-10)
  expect_equal(rich$dives_incomplete, 3L)                   # 2 gap-interrupted + 1 boundary-truncated
  expect_equal(rich$dives_truncated, 1L)
  expect_equal(rich$dives_gapped, 2L)
  # and the whole-record deployment is ONE dive, not none
  whole <- out[out$id == "WHOLE", , drop = FALSE]
  expect_equal(whole$n_dives, 1L)
  expect_equal(whole$dives_truncated, 1L)
  expect_equal(whole$dive_duration_max_min, 399 / 60, tolerance = 1e-10)
})

test_that("dive_id WITHOUT dive_phase yields no dive columns, matching diveMetrics' own guard", {
  x <- data.table::copy(.mk_dived("A"))
  x[, dive_phase := NULL]
  expect_true("dive_id" %in% names(x))
  out <- .run(list(A = x))
  expect_equal(nrow(out), 1L)
  expect_equal(out$id, "A")                                  # the deployment is still summarised...
  expect_equal(out$depth_max, 40, tolerance = 1e-10)
  expect_false(any(.sumDiveCols %in% names(out)))            # ...it just carries no dive block
  # diveMetrics() skips such a deployment too, so a silent summary is the MATCHING behaviour, not a loss
  expect_warning(dm <- diveMetrics(x, verbose = FALSE), "skipped")
  expect_equal(nrow(dm), 0L)
})
