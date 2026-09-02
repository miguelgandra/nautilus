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
  if (withtbf)   { d[, tbf_hz_peaks := ifelse(seq_len(n) %% 2 == 0, 0.8, NA_real_)]   # beating (0.8 Hz) half the time
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
  # status closes the record block, with its reason beside it: status_reason usually explains a short
  # or absent record, so the two read together rather than thirty columns apart
  expect_equal(which(names(out) == "status"), which(names(out) == "record_duration_h") + 1L)
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
  expect_true(all(c("Rec. start", "Rec. end", "Duration (h)", "Rate (Hz)", "Attach. site",
                    "Mean TBF (Hz)", "Swimming (%)", "Positions (n)") %in% names(concise)))
  expect_true("Max speed (m/s)" %in% names(concise))                           # ASCII by default
  expect_true("Mean speed (m s\u207b\u00b9)" %in%                               # superscript on request
                names(format(s, style = "concise", symbols = "unicode")))
  expect_false(any(grepl("_", names(concise))))
})

test_that("the formatted table is ASCII by default, in every style, whatever the locale", {
  # It is written to a CSV far more often than it is read in a terminal, and Excel has no BOM to go on:
  # a UTF-8 degree sign guessed as MacRoman renders as two mojibake characters. The old gate asked
  # cli::is_utf8_output(), which is a question about the TERMINAL - so the same script and data wrote
  # different bytes depending on the session's locale.
  s <- .run(list(A = .mk("A", withtbf = TRUE, withpaddle = TRUE), B = .mk("B")))
  nonascii <- function(z) sum(grepl("[^ -~]", c(names(z), unlist(lapply(z, as.character)))))
  for (st in c("internal", "report", "concise")) {
    expect_identical(nonascii(format(s, style = st)), 0L)
    expect_identical(nonascii(format(s, style = st, include.summary.row = FALSE)), 0L)
    expect_gt(nonascii(format(s, style = st, symbols = "unicode")), 0L)
  }
  expect_true(any(grepl("+/-", unlist(format(s)), fixed = TRUE)))              # the summary row too
  expect_true("Mean temp. (deg C)" %in% names(format(s, style = "report")))

  # locale independence: the bytes must not follow the terminal
  withr::with_options(list(cli.unicode = TRUE),  a <- format(s, style = "concise"))
  withr::with_options(list(cli.unicode = FALSE), b <- format(s, style = "concise"))
  expect_identical(a, b)

  # and every header the dictionaries can produce folds to ASCII, so a symbol added later cannot escape
  all_cols <- union(names(nautilus:::.summaryTemplate()), nautilus:::.summaryDiveCols())
  for (st in c("report", "concise"))
    expect_identical(sum(grepl("[^ -~]", nautilus:::.foldSymbols(
      nautilus:::.summaryHeaders(all_cols, st)))), 0L)
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

test_that("the SUMMARY block groups its content into titled sections", {
  grab <- function(...) paste(cli::cli_fmt(suppressWarnings(summarizeTagData(...))), collapse = "\n")

  d2 <- grab(list(A = .mk("A", withtbf = TRUE), B = .mk("B")), verbose = 2)
  expect_match(d2, "Data availability")                     # the section heading
  expect_match(d2, "Tail-beats:\\s+1")                       # only A has tail-beat data
  expect_match(d2, "Depth:\\s+2")

  # coverage stays level-2 only: the layout changed, the verbosity gating did not
  d1 <- grab(list(A = .mk("A")), verbose = 1)
  expect_false(grepl("Data availability", d1))
  expect_match(d1, "Deployments")                           # but the roster section is level-1

  # the roster split still surfaces at level 1, now as aligned rows rather than one packed line
  dep <- data.frame(id = c("A", "B", "C"), tag_model = "CATS", stringsAsFactors = FALSE)
  class(dep) <- c("nautilus_deployments", "data.frame")
  dr <- grab(list(A = .mk("A")), deployments = dep, verbose = 1)
  expect_match(dr, "Total:\\s+3")
  expect_match(dr, "Included:\\s+1")
  expect_match(dr, "Excluded:\\s+2")

  # and the excluded ids are named at level 2, under their own heading
  dr2 <- grab(list(A = .mk("A")), deployments = dep, verbose = 2)
  expect_match(dr2, "Excluded deployments")
  expect_match(dr2, "B, C")
})

test_that("the summary omits sections that have nothing to say", {
  grab <- function(...) paste(cli::cli_fmt(suppressWarnings(summarizeTagData(...))), collapse = "\n")
  # no roster -> no Roster/Excluded rows, and no excluded-ids section
  d <- grab(list(A = .mk("A"), B = .mk("B")), verbose = 2)
  expect_match(d, "Tags summarised:\\s+2")
  expect_false(grepl("Total:", d))
  expect_false(grepl("Excluded deployments", d))
  # no dive annotation -> no Dives section
  expect_false(grepl("^Dives", d))
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


test_that("the summary states which backend its tail-beat mean came from", {
  # the resolved backend travels with the value, so a cohort pooled from deployments that used
  # different backends is visible in the summary rather than silently blended
  a <- .mk("A", withtbf = TRUE)
  b <- data.table::copy(a); b[, ID := "B"]
  data.table::setnames(b, "tbf_hz_peaks", "tbf_hz_wavelet")
  out <- .run(list(A = a, B = b))
  expect_true("tbf_method" %in% names(out))
  expect_equal(out$tbf_method, c("peaks", "wavelet"))
  expect_equal(out$tbf_mean, c(0.8, 0.8), tolerance = 1e-6)
  # tbf.method forces one backend for the whole cohort
  forced <- .run(list(A = a), tbf.method = "peaks")
  expect_equal(forced$tbf_method, "peaks")
})

test_that("a deployment with no tail-beat columns reports NA for both value and backend", {
  out <- .run(list(B = .mk("B")))
  expect_true(is.na(out$tbf_mean)); expect_true(is.na(out$tbf_method))
})


# ---------------------------------------------------------------------------
# the metadata block, and what a deployment with no data still reports
# ---------------------------------------------------------------------------

.mkMeta <- function(id, traits = list(sex = "F", size_m = 9)) {
  n <- 400
  d <- data.table::data.table(ID = id, datetime = as.POSIXct("2020-01-01", tz = "UTC") + seq_len(n),
                              depth = abs(sin(seq_len(n) / 40)) * 30, temp = 18)
  m <- nautilus:::.newNautilusMeta(); m$id <- id
  m$biometrics <- traits
  m$deployment$datetime <- as.POSIXct("2019-12-31 08:00", tz = "UTC")
  m$deployment$lon <- -25.1234; m$deployment$lat <- 36.9876
  m$deployment$popup_lon <- -26.5; m$deployment$deployment_type <- "towed"
  m$tag$package_id <- "71"; m$tag$logger_id <- "29"; m$tag$axis_config <- "cfg1"
  nautilus:::new_nautilus_tag(d, m)
}
.mkRoster <- function(ids, ...) {
  r <- data.frame(id = ids, tag_model = "CATS", tag_type = "MS", attachment_site = "pectoral",
                  deploy_datetime = as.POSIXct("2019-12-31 08:00", tz = "UTC") + seq_along(ids) * 86400,
                  deploy_lon = -25 - seq_along(ids) / 10, deploy_lat = 36 + seq_along(ids) / 10,
                  sex = rep(c("F", "M"), length.out = length(ids)),
                  size_m = seq_along(ids) + 5, stringsAsFactors = FALSE, ...)
  class(r) <- c("nautilus_deployments", "data.frame"); r
}

test_that("metadata = 'standard' adds the traits and the tagging position by default", {
  s <- .run(list(A = .mkMeta("A")))
  expect_true(all(c("sex", "size_m", "deploy_datetime", "deploy_lon", "deploy_lat") %in% names(s)))
  expect_identical(s$sex, "F"); expect_equal(s$size_m, 9)
  expect_s3_class(s$deploy_datetime, "POSIXct")
  expect_equal(s$deploy_lon, -25.1234); expect_equal(s$deploy_lat, 36.9876)
  # traits first (who the animal was), then where and when it was tagged, then the record
  expect_equal(names(s)[1:11], c("id", "animal_id", "sex", "size_m", "tag_model", "tag_type",
                                 "attachment_site", "paddle_wheel",
                                 "deploy_datetime", "deploy_site", "deploy_lon"))
  expect_false(any(c("popup_lon", "package_id") %in% names(s)))
})

test_that("metadata keywords and explicit field lists select the block, and bad input aborts", {
  tags <- list(A = .mkMeta("A"))
  expect_false(any(c("sex", "deploy_lon") %in% names(.run(tags, metadata = "none"))))
  all_s <- .run(tags, metadata = "all")
  expect_true(all(c("popup_datetime", "popup_lon", "deployment_type", "package_id", "logger_id",
                    "axis_config") %in% names(all_s)))
  # explicit names come back in canonical order whatever order they were asked for
  a <- .run(tags, metadata = c("deploy_lat", "sex", "deploy_lon"))
  b <- .run(tags, metadata = c("sex", "deploy_lon", "deploy_lat"))
  expect_identical(names(a), names(b))

  expect_error(.run(tags, metadata = c("all", "sex")), "mixes the keyword")
  expect_error(.run(tags, metadata = 42), "character vector")
  expect_error(.run(tags, metadata = NA_character_), "character vector")
  # a name that is neither a field nor a trait any deployment carries is taken as a trait, and said so
  # (.run() muffles warnings, so this one goes through the function directly)
  expect_warning(summarizeTagData(tags, metadata = c("sex", "deploy_long"), verbose = FALSE),
                 "no deployment carries")
})

test_that("a deployment whose data never arrived still reports who, when and where", {
  # The complaint this exists for: a tag that was never recovered used to come back with an identifier
  # and nothing else, while the roster had been holding its tagging date and position all along.
  s <- .run(list(A = .mkMeta("A")), deployments = .mkRoster(c("A", "GHOST")))
  g <- s[s$id == "GHOST", ]
  expect_identical(g$status, "excluded")
  expect_identical(g$sex, "M"); expect_equal(g$size_m, 7)
  expect_s3_class(g$deploy_datetime, "POSIXct"); expect_false(is.na(g$deploy_datetime))
  expect_equal(g$deploy_lon, -25.2); expect_equal(g$deploy_lat, 36.2)
  expect_true(is.na(g$record_duration_h))                       # it has no record, and says so
  # the roster fill must not retype the column
  expect_s3_class(s$deploy_datetime, "POSIXct")
  expect_type(s$deploy_lon, "double")
})

test_that("a rejected window reaches the summary through the exclusions table", {
  ex <- data.frame(id = "SHORT", reason = "deployment too short",
                   window_start = as.POSIXct("2020-06-01 08:01", tz = "UTC"),
                   window_end   = as.POSIXct("2020-06-01 08:08", tz = "UTC"),
                   window_hours = 0.1164, stringsAsFactors = FALSE)
  s <- .run(list(A = .mkMeta("A")), deployments = .mkRoster(c("A", "SHORT")), exclusions = ex)
  sh <- s[s$id == "SHORT", ]
  expect_identical(sh$status, "excluded")
  expect_identical(sh$status_reason, "deployment too short")
  expect_equal(sh$record_duration_h, 0.1164)
  expect_false(is.na(sh$record_start))
  # a deployment that survived owns its own window - a stale table must not overwrite it
  expect_identical(s$status_reason[s$id == "A"], NA_character_)
  expect_equal(as.numeric(s$record_start[s$id == "A"]),
               as.numeric(as.POSIXct("2020-01-01 00:00:01", tz = "UTC")))

  expect_error(.run(list(A = .mkMeta("A")), exclusions = data.frame(id = "x")), "missing the column")
  expect_error(.run(list(A = .mkMeta("A")), exclusions = "no-such-file.csv"), "does not exist")
})

test_that("video.metadata totals the per-file table, and absence is NA rather than zero", {
  vm <- data.frame(ID = c("A", "A", "A"), duration = c(1200.5, 402.2, 421.0), stringsAsFactors = FALSE)
  s <- .run(list(A = .mkMeta("A"), B = .mkMeta("B")), video.metadata = vm)
  expect_equal(s$video_duration_h[s$id == "A"], sum(vm$duration) / 3600)
  expect_true(is.na(s$video_duration_h[s$id == "B"]))          # no footage found is not zero hours
  expect_false("video_duration_h" %in% names(.run(list(A = .mkMeta("A")))))

  expect_error(.run(list(A = .mkMeta("A")), video.metadata = data.frame(ID = "A")), "missing the column")
  expect_error(.run(list(A = .mkMeta("A")), video.metadata = data.frame(ID = "A", duration = "x")),
               "must be numeric")
  expect_warning(summarizeTagData(list(A = .mkMeta("A")), video.metadata = data.frame(ID = "ZZ", duration = 10),
                                  verbose = FALSE),
                 "no 'video.metadata' ID matches")
})


# ---------------------------------------------------------------------------
# column order, and the animal behind the deployment
# ---------------------------------------------------------------------------

test_that("columns are seated in declared blocks, whatever order they were produced in", {
  # `status_reason` is attached long after `status`, and `video_duration_h` after both. Seating each
  # block where it was appended is how `status_reason` ended up thirty columns from `status`.
  ros <- .mkRoster(c("A", "GHOST")); ros$animal_id <- c("WS_1", "WS_9")
  attr(ros, "nautilus.columns") <- list(traits = c("sex", "size_m"))
  ex <- data.frame(id = "GHOST", reason = "deployment too short",
                   window_start = as.POSIXct("2020-02-01 10:00", tz = "UTC"),
                   window_end = as.POSIXct("2020-02-01 10:06", tz = "UTC"),
                   window_hours = 0.1, stringsAsFactors = FALSE)
  s <- .run(list(A = .mkMeta("A")), deployments = ros, exclusions = ex,
            video.metadata = data.frame(ID = "A", duration = 3600),
            extra.metadata = data.frame(ID = "A", maturity = "adult", stringsAsFactors = FALSE))
  n <- names(s)
  expect_equal(n[1:2], c("id", "animal_id"))                       # deployment, then animal
  expect_true(match("sex", n) < match("tag_model", n))             # animal before tag
  expect_true(match("maturity", n) < match("tag_model", n))        # covariates sit with the traits
  expect_equal(match("status_reason", n), match("status", n) + 1L) # the pair, adjacent
  expect_equal(match("record_duration_h", n), match("status", n) - 1L)
  expect_true(match("deploy_lat", n) < match("record_start", n))   # tagging position before the record
  expect_true(match("video_duration_h", n) > match("n_positions", n))
  expect_true(match("depth_max", n) < match("vedba_mean", n))      # habitat before behaviour
  expect_true(match("paddle_wheel", n) < match("speed_mean", n))   # the qualifier before what it qualifies
})

test_that("animal_id is a role, and completes from the roster without overriding the tag", {
  expect_true("animal_id" %in% names(formals(metadataColumns)))
  expect_equal(metadataColumns(animal_id = "shark")$animal_id, "shark")
  expect_true("animal_id" %in% names(nautilus:::.newNautilusMeta()))

  # a tag that carries it keeps its own value; one that does not is completed from the roster
  own <- .mkMeta("A"); m <- nautilus:::.getMeta(own); m$animal_id <- "FROM_TAG"
  own <- nautilus:::.restoreMeta(own, m)
  ros <- .mkRoster(c("A", "B")); ros$animal_id <- c("FROM_ROSTER", "WS_2")
  attr(ros, "nautilus.columns") <- list(traits = c("sex", "size_m"))
  s <- .run(list(A = own, B = .mkMeta("B")), deployments = ros)
  expect_identical(s$animal_id[s$id == "A"], "FROM_TAG")           # the tag is authoritative
  expect_identical(s$animal_id[s$id == "B"], "WS_2")               # the gap is completed
})

test_that("only DECLARED traits ride in from the roster, not every column of the workbook", {
  # The workbook holds study columns nautilus knows nothing about. Sweeping them in populated them on
  # excluded rows and left them NA on every processed one - visible, asymmetric, and wrong.
  ros <- .mkRoster(c("A", "GHOST"))
  ros$site <- "SMA"; ros$argos_ptt <- c(1, 2); ros$deploy_year <- c(2019, 2019)
  attr(ros, "nautilus.columns") <- list(traits = c("sex", "size_m"))
  s <- .run(list(A = .mkMeta("A")), deployments = ros)
  expect_false(any(c("site", "argos_ptt", "deploy_year") %in% names(s)))
  expect_true(all(c("sex", "size_m") %in% names(s)))
  expect_identical(s$sex[s$id == "GHOST"], "M")                    # the declared ones still arrive
})


test_that("deploy_site is a role, sits beside the coordinates, and completes from the roster", {
  expect_true("deploy_site" %in% names(formals(metadataColumns)))
  expect_equal(metadataColumns(deploy_site = "locality")$deploy_site, "locality")
  expect_true("site" %in% names(nautilus:::.newNautilusMeta()$deployment))
  expect_true("deploy_site" %in% nautilus:::.summaryMetaSets()$standard)

  # the name and the coordinates answer different questions - one groups, the other locates - so the
  # name reads immediately before the pair rather than arriving as a study column of unknown provenance
  tg <- .mkMeta("A"); m <- nautilus:::.getMeta(tg); m$deployment$site <- "SMA"
  tg <- nautilus:::.restoreMeta(tg, m)
  s <- .run(list(A = tg))
  expect_identical(s$deploy_site, "SMA")
  n <- names(s)
  expect_equal(match("deploy_site", n), match("deploy_datetime", n) + 1L)
  expect_equal(match("deploy_lon", n),  match("deploy_site", n) + 1L)

  # and a deployment whose data never arrived takes it from the roster
  ros <- .mkRoster(c("A", "GHOST")); ros$deploy_site <- c("SMA", "FAI")
  attr(ros, "nautilus.columns") <- list(traits = c("sex", "size_m"))
  s2 <- .run(list(A = tg), deployments = ros)
  expect_identical(s2$deploy_site[s2$id == "GHOST"], "FAI")
  expect_identical(s2$deploy_site[s2$id == "A"], "SMA")            # the tag stays authoritative
  expect_false("deploy_site" %in% names(.run(list(A = tg), metadata = "none")))
})


# ---------------------------------------------------------------------------
# per-column display precision
# ---------------------------------------------------------------------------

test_that("decimals overrides one column's precision and leaves the rest alone", {
  s <- .run(list(A = .mk("A"), B = .mk("B"), C = .mk("C")))
  base <- format(s, style = "internal", include.summary.row = FALSE)
  one  <- format(s, style = "internal", include.summary.row = FALSE, decimals = c(depth_max = 0))
  expect_equal(base$depth_max, rep("50.0", 3))                         # the built-in 1 dp
  expect_equal(one$depth_max,  rep("50", 3))                           # overridden to 0 dp
  expect_identical(one$temp_max, base$temp_max)                        # untouched
  expect_identical(one$vedba_mean, base$vedba_mean)

  # several at once, each independent
  many <- format(s, style = "internal", include.summary.row = FALSE,
                 decimals = c(depth_max = 3, vedba_mean = 1, record_duration_h = 4))
  expect_match(many$depth_max[1], "^[0-9]+\\.[0-9]{3}$")
  expect_match(many$vedba_mean[1], "^[0-9]+\\.[0-9]{1}$")
  expect_match(many$record_duration_h[1], "^[0-9]+\\.[0-9]{4}$")
  expect_identical(many$temp_min, base$temp_min)
})

test_that("the mean +/- error row follows the override too", {
  s <- .run(list(A = .mk("A"), B = .mk("B")))
  f <- format(s, style = "internal", decimals = c(depth_max = 0))
  foot <- f$depth_max[nrow(f)]
  expect_match(foot, "^[0-9]+ \\+/- [0-9]+$")                          # no decimal point either side
  expect_false(grepl(".", foot, fixed = TRUE))
})

test_that("decimals is keyed on internal names, and a header is resolved back to one", {
  s <- .run(list(A = .mk("A"), B = .mk("B")))
  # the same override applies under every style, because the key does not depend on the style
  a <- format(s, style = "report",  decimals = c(depth_max = 0))
  b <- format(s, style = "concise", decimals = c(depth_max = 0))
  expect_identical(unname(unlist(a[1, ])), unname(unlist(b[1, ])))
  expect_error(format(s, decimals = c("Max depth (m)" = 0)), "display header")
  expect_error(format(s, decimals = c("Max depth (m)" = 0)), "depth_max")
})

test_that("decimals rejects what is certainly a mistake", {
  s <- .run(list(A = .mk("A"), B = .mk("B")))
  expect_error(format(s, decimals = c(nope = 1)), "not in this summary")
  expect_error(format(s, decimals = c(depth_max = 1.5)), "whole numbers")
  expect_error(format(s, decimals = c(depth_max = -1)), "whole numbers")
  expect_error(format(s, decimals = c(tag_model = 1)), "non-numeric")
  expect_error(format(s, decimals = 2), "named after a column")
  expect_error(format(s, decimals = "two"), "named numeric vector")
  expect_error(format(s, decimals = c(depth_max = 1, depth_max = 2)), "more than once")
  expect_identical(format(s, decimals = NULL), format(s))              # NULL is the default path
})

test_that("print() forwards to format(), so the display can be tuned in place", {
  s <- .run(list(A = .mk("A"), B = .mk("B")))
  out <- capture.output(print(s, decimals = c(depth_max = 0)))
  expect_false(any(grepl("50\\.0", out)))                              # the override reached the console
  expect_true(any(grepl("\\b50\\b", out)))
  expect_s3_class(withVisible(print(s))$value, "nautilus_summary")     # still returns x invisibly
  expect_false(withVisible(print(s))$visible)
})


# ---- format(group.by =) ----------------------------------------------------------------------------
# Grouping is a presentation concern: summarizeTagData() is untouched, the returned object keeps its
# class and row count, and only the rendering changes.

.grpSummary <- function(sex = c("F", "M", "F", NA, "M")) {
  df <- data.frame(id = paste0("A0", seq_along(sex)), sex = sex,
                   record_duration_h = c(30, 22, 41, 18, 12)[seq_along(sex)],
                   depth_max = c(120, 80, 140, 95, 60)[seq_along(sex)],
                   stringsAsFactors = FALSE)
  nautilus:::.newSummary(df, "sd")
}


test_that("group.by = FALSE reproduces the ungrouped output exactly", {
  s <- .grpSummary()
  expect_identical(format(s, group.by = FALSE), format(s))
  expect_null(attr(format(s), "summary.groups"))
})


test_that("grouping orders rows and gives each group its own footer", {
  f <- format(.grpSummary(), group.by = "sex")
  # 5 deployments + one footer per group (F, M, missing)
  expect_identical(nrow(f), 8L)
  foot <- which(f$id == "mean +/- sd")
  expect_length(foot, 3L)
  # rows are grouped, and each footer carries its own group value
  expect_identical(f$sex, c("F", "F", "F", "M", "M", "M", "-", "(missing)"))
  expect_identical(f$sex[foot], c("F", "M", "(missing)"))
  # the footer averages only its own group: F is (30 + 41) / 2
  expect_match(f$record_duration_h[foot[1]], "^35\\.5 ")
})


test_that("a group of one shows its mean without an error term", {
  f <- format(.grpSummary(sex = c("F", "M")), group.by = "sex")
  foot <- f[f$id == "mean +/- sd", ]
  expect_identical(nrow(foot), 2L)
  expect_false(any(grepl("+/-", foot$record_duration_h, fixed = TRUE)))   # sd of one value
  expect_identical(foot$record_duration_h, c("30.0", "22.0"))
})


test_that("missing values form their own trailing group and are never dropped", {
  f <- format(.grpSummary(), group.by = "sex")
  expect_true("(missing)" %in% f$sex)
  expect_identical(f$sex[nrow(f)], "(missing)")          # last
  expect_true("A04" %in% f$id)                            # the NA deployment still appears
})


test_that("factor levels set the group order, and unused levels are dropped", {
  s <- .grpSummary()
  s$sex <- factor(s$sex, levels = c("M", "F", "juvenile"))   # reversed, plus an unused level
  f <- format(s, group.by = "sex")
  expect_identical(unique(f$sex), c("M", "F", "-", "(missing)"))
  expect_false("juvenile" %in% f$sex)
})


test_that("include.summary.row = FALSE groups without footers", {
  f <- format(.grpSummary(), group.by = "sex", include.summary.row = FALSE)
  expect_identical(nrow(f), 5L)
  expect_false(any(f$id == "mean +/- sd"))
  expect_identical(f$sex, c("F", "F", "M", "M", "-"))       # still ordered
})


test_that("format() emits no blank rows, so the export path stays machine-readable", {
  f <- format(.grpSummary(), group.by = "sex")
  blank <- apply(f, 1, function(r) all(trimws(r) == ""))
  expect_false(any(blank))
  # and the grouping travels as an attribute, which write.csv() never sees
  expect_length(attr(f, "summary.groups"), nrow(f))
  csv <- tempfile(fileext = ".csv"); on.exit(unlink(csv), add = TRUE)
  utils::write.csv(f, csv, row.names = FALSE)
  back <- utils::read.csv(csv, check.names = FALSE)
  expect_identical(nrow(back), nrow(f))                    # no empty record on re-import
})


test_that("print() inserts the blank line between groups that format() withholds", {
  s <- .grpSummary()
  plain   <- utils::capture.output(print(s))
  grouped <- utils::capture.output(print(s, group.by = "sex"))
  expect_false(any(!nzchar(trimws(plain))))                # ungrouped: no blank lines
  expect_identical(sum(!nzchar(trimws(grouped))), 2L)      # three groups -> two breaks
  expect_match(grouped[1], "grouped by sex")
})


test_that("group.by validates against the summary's own columns", {
  s <- .grpSummary()
  expect_error(format(s, group.by = "nope"), "does not name a column")
  expect_error(format(s, group.by = "nope"), "Available columns")
  expect_error(format(s, group.by = c("sex", "id")), "group.by")
})


test_that("grouping keys on the internal column name, whatever the style renames it to", {
  f <- format(.grpSummary(), group.by = "sex", style = "report")
  expect_identical(nrow(f), 8L)                             # grouped despite the renamed header
  expect_false("sex" %in% names(f))                         # header was relabelled
})


# ---- format(order.by =) ----------------------------------------------------------------------------
# Ordering is a presentation concern too: the object is untouched, and the mean +/- error rows are
# computed after the rows reach their final order, so no ordering can move one off the foot of its block.

.ordSummary <- function() {
  df <- data.frame(id = c("A03", "A01", "A05", "A02", "A04"),
                   sex = c("F", "M", "F", NA, "M"),
                   status = c("included", "excluded", "included", "excluded", "included"),
                   record_start = as.POSIXct(c("2020-03-01", "2020-01-05", "2020-02-01",
                                               "2020-05-01", "2020-04-01"), tz = "UTC"),
                   depth_max = c(120, NA, 140, NA, 60),
                   record_duration_h = c(30, 22, 41, 18, 12), stringsAsFactors = FALSE)
  nautilus:::.newSummary(df, "sd")
}


test_that("order.by = NULL leaves the order the summary was built in", {
  s <- .ordSummary()
  expect_identical(format(s, order.by = NULL), format(s))
  expect_identical(format(s)$id, c(s$id, "mean +/- sd"))
})


test_that("order.by sorts on a column, ascending by default and descending with a leading dash", {
  s <- .ordSummary()
  expect_identical(format(s, order.by = "id")$id,
                   c("A01", "A02", "A03", "A04", "A05", "mean +/- sd"))
  expect_identical(format(s, order.by = "-record_duration_h")$id,
                   c("A05", "A03", "A01", "A02", "A04", "mean +/- sd"))
})


test_that("order.by works on every column type the summary carries", {
  s <- .ordSummary()
  expect_identical(format(s, order.by = "record_start")$id,           # POSIXct
                   c("A01", "A05", "A03", "A04", "A02", "mean +/- sd"))
  expect_identical(format(s, order.by = "sex")$id[1:2], c("A03", "A05"))          # character
  s$sex <- factor(s$sex, levels = c("M", "F"))
  expect_identical(format(s, order.by = "sex")$id[1:2], c("A01", "A04"))          # factor, by level
})


test_that("deployments missing the ordering value sort last in BOTH directions", {
  s <- .ordSummary()
  # A01 and A02 have no depth_max; they belong under the deployments that do, either way round
  expect_identical(utils::tail(format(s, order.by = "depth_max")$id, 3L),
                   c("A01", "A02", "mean +/- sd"))
  expect_identical(utils::tail(format(s, order.by = "-depth_max")$id, 3L),
                   c("A01", "A02", "mean +/- sd"))
})


test_that("several keys sort nested, and ties keep the incoming order", {
  s <- .ordSummary()
  f <- format(s, order.by = c("sex", "-record_duration_h"))
  expect_identical(f$id, c("A05", "A03", "A01", "A04", "A02", "mean +/- sd"))
  # a key that separates nothing must not reshuffle: the rendering has to be reproducible
  s$flat <- rep(1, nrow(s))
  expect_identical(format(s, order.by = "flat")$id, format(s)$id)
  expect_identical(format(s, order.by = "-flat")$id, format(s)$id)
})


test_that("a column whose own name starts with a dash sorts on itself, not descending", {
  df <- data.frame(id = c("A", "B", "C"), depth_max = c(1, 2, 3), check.names = FALSE)
  df[["-depth_max"]] <- c(2, 3, 1)             # unrelated values, so the two readings disagree
  s <- nautilus:::.newSummary(df, "sd")
  expect_identical(format(s, order.by = "-depth_max")$id, c("C", "A", "B", "mean +/- sd"))
})


test_that("the summary row stays at the foot of the table under any ordering", {
  s <- .ordSummary()
  # "mean +/- sd" sorts before every id alphabetically, so a footer built before the sort would move
  for (k in list("id", "-id", "record_start", "-depth_max", c("sex", "id")))
    expect_identical(utils::tail(format(s, order.by = k)$id, 1L), "mean +/- sd")
})


test_that("ordering applies within groups, and every footer stays at the foot of its own group", {
  s <- .ordSummary()
  f <- format(s, group.by = "sex", order.by = "-record_duration_h")
  expect_identical(f$id, c("A05", "A03", "mean +/- sd",       # F
                          "A01", "A04", "mean +/- sd",        # M
                          "A02", "mean +/- sd"))              # (missing)
  # each footer is the last row of its own block, and the blocks are contiguous
  g <- attr(f, "summary.groups")
  expect_identical(g[f$id == "mean +/- sd"], c("F", "M", "(missing)"))
  expect_identical(rle(g)$lengths, c(3L, 3L, 2L))
})


test_that("ordering never splits a group, even without summary rows to rebuild the blocks", {
  s <- .ordSummary()
  f <- format(s, group.by = "sex", order.by = "-depth_max", include.summary.row = FALSE)
  g <- attr(f, "summary.groups")
  expect_identical(g, c("F", "F", "M", "M", "(missing)"))
  expect_identical(length(rle(g)$lengths), 3L)               # contiguous: one run per group
})


test_that("order.by validates against the summary's own columns", {
  s <- .ordSummary()
  expect_error(format(s, order.by = "nope"), "not in this summary")
  expect_error(format(s, order.by = "Max depth (m)"), "display header")
  expect_error(format(s, order.by = "Max depth (m)"), "depth_max")
  expect_error(format(s, order.by = c("id", "-id")), "more than once")
  expect_error(format(s, order.by = 1), "character vector")
  expect_error(format(s, order.by = NA_character_), "character vector")
})


test_that("a formatting argument is validated even when no deployment survived", {
  # every metric NA and no rows to render is exactly when a silent no-op would go unnoticed
  s <- nautilus:::.newSummary(nautilus:::.summaryTemplate(), "sd")
  expect_error(format(s, order.by = "nope"), "not in this summary")
  expect_error(format(s, decimals = c(nope = 1)), "not in this summary")
  expect_error(format(s, group.by = "nope"), "does not name a column")
  expect_identical(format(s), data.frame())                  # still renders as empty
  expect_identical(format(nautilus:::.newSummary(data.frame(), "sd")), data.frame())
})


test_that("ordering the object itself survives, and format() renders it as it stands", {
  s <- .ordSummary()
  r <- s[order(s$record_duration_h), ]
  expect_s3_class(r, "nautilus_summary")
  expect_identical(attr(r, "error.stat"), "sd")              # the statistic travels with the rows
  expect_identical(format(r)$id, c(r$id, "mean +/- sd"))
})


# ---- format(group.order =) -------------------------------------------------------------------------

test_that("group.order sets the order the groups are rendered in", {
  s <- .ordSummary()
  expect_identical(attr(format(s, group.by = "sex", group.order = c("M", "F")), "summary.groups"),
                   c("M", "M", "M", "F", "F", "F", "(missing)", "(missing)"))
})


test_that("group.order may name only the groups that matter; the rest keep their usual order", {
  s <- .ordSummary()
  g <- attr(format(s, group.by = "sex", group.order = "M"), "summary.groups")
  expect_identical(unique(g), c("M", "F", "(missing)"))
})


test_that("group.order overrides a factor's own levels", {
  s <- .grpSummary()
  s$sex <- factor(s$sex, levels = c("M", "F"))
  expect_identical(unique(attr(format(s, group.by = "sex"), "summary.groups")),
                   c("M", "F", "(missing)"))               # the levels, as given
  expect_identical(unique(attr(format(s, group.by = "sex", group.order = "F"), "summary.groups")),
                   c("F", "M", "(missing)"))               # group.order wins
})


test_that("group.order refuses a value that names no group", {
  s <- .ordSummary()
  expect_error(format(s, group.by = "sex", group.order = "Z"), "not in this summary")
  expect_error(format(s, group.by = "sex", group.order = "Z"), "Available group")
  expect_error(format(s, group.by = "sex", group.order = c("F", "F")), "more than once")
  expect_error(format(s, group.by = "sex", group.order = 1), "character vector")
  expect_error(format(s, group.by = "sex", group.order = "(missing)"), "cannot place")
  expect_error(format(s, group.order = "F"), "without .*group.by")
})


test_that("grouping by status leads with the analysed deployments, not the alphabet", {
  s <- .ordSummary()
  expect_identical(unique(attr(format(s, group.by = "status"), "summary.groups")),
                   c("included", "excluded"))
  # still a caller's decision to make: both an explicit order and a factor override the convention
  expect_identical(unique(attr(format(s, group.by = "status", group.order = "excluded"),
                               "summary.groups")), c("excluded", "included"))
  s$status <- factor(s$status, levels = c("excluded", "included"))
  expect_identical(unique(attr(format(s, group.by = "status"), "summary.groups")),
                   c("excluded", "included"))
})


test_that("group.by = NULL is the ungrouped default, and FALSE still means the same", {
  s <- .ordSummary()
  expect_identical(format(s, group.by = NULL), format(s))
  expect_identical(format(s, group.by = FALSE), format(s))
  expect_null(attr(format(s, group.by = NULL), "summary.groups"))
})


test_that("summarizeTagData reports the reason whichever stage supplied it", {
  f <- file.path(withr::local_tempdir(), "exclusions.csv")
  nautilus:::.exclusionsWrite(nautilus:::.exclusionsBind(list(nautilus:::.exclusionsRow("GONE", "processTagData", "missing required columns: mx, my, mz"))),
    f, "processTagData")
  s <- .run(list(A = .mkMeta("A")), deployments = .mkRoster(c("A", "GONE")),
            exclusions = f)
  expect_true("GONE" %in% s$id)
  expect_identical(s$status[s$id == "GONE"], "excluded")
  expect_match(s$status_reason[s$id == "GONE"], "mx, my, mz")
})
