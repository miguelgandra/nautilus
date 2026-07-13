# Tests for reviewTagMapping(): the pure triage / footage-matching / clip-selection logic and argument
# validation run without video; one end-to-end render is skip-gated on av + ffmpeg.

# --- synthetic checkTagMapping-style mapping elements -------------------------------------------------
.surv <- function(nr = 1) {
  base <- data.frame(newX_col = "ax", newX_sign = 1, newY_col = "ay", newY_sign = 1,
                     newZ_col = "az", newZ_sign = 1, stringsAsFactors = FALSE)
  if (nr == 1) return(base)
  rbind(base, data.frame(newX_col = "ax", newX_sign = 1, newY_col = "az", newY_sign = -1,
                         newZ_col = "ay", newZ_sign = 1, stringsAsFactors = FALSE))
}
.mkmap <- function(conflicts = character(0), prior = "absent", nsurv = 1, gyro = "resolved",
                   vert = "resolved", surge = "resolved", coreg = "ok") {
  s1 <- .surv(nsurv)[1, ]                                          # a real checkTagMapping element carries a $proposal
  prop <- data.frame(from = c(s1$newX_col, s1$newY_col, s1$newZ_col),
                     to = c(paste0(if (s1$newX_sign < 0) "-" else "", "ax"),
                            paste0(if (s1$newY_sign < 0) "-" else "", "ay"),
                            paste0(if (s1$newZ_sign < 0) "-" else "", "az")), stringsAsFactors = FALSE)
  list(frame_state = list(conflicts = conflicts, prior = list(status = prior), survivors = .surv(nsurv),
                          vertical = list(status = vert), surge = list(status = surge),
                          coreg = list(status = coreg)),
       families = list(gyro = list(status = gyro)), proposal = prop)
}
.mapping <- list(
  D_conflict   = .mkmap(conflicts = "doc conflicts", prior = "conflict"),
  D_coreg      = .mkmap(coreg = "fail"),                                # co-die rejected, gyro resolved by fallback
  D_ambiguous  = .mkmap(prior = "absent", nsurv = 2),
  D_gyro       = .mkmap(coreg = "fail", gyro = "inconsistent"),         # estimators disagree: ALWAYS carries coreg fail
  D_lowconf    = .mkmap(prior = "confirmed", surge = "ambiguous"),
  D_clean      = .mkmap(prior = "confirmed"))
.inc <- c("conflict", "coreg_fail", "ambiguous", "gyro_inconsistent")


test_that("triage flags exactly the suspect deployments, by severity", {
  q <- nautilus:::.triageMappings(.mapping, names(.mapping), names(.mapping), .inc, NULL)
  # D_gyro's inconsistent estimators imply a co-die failure, so it triages as coreg_fail (priority 2),
  # not gyro_inconsistent (priority 4) - the honest production behaviour.
  expect_equal(q$id, c("D_conflict", "D_coreg", "D_gyro", "D_ambiguous"))   # sorted by priority then id
  expect_equal(q$review_reason, c("conflict", "coreg_fail", "coreg_fail", "ambiguous"))
  expect_equal(q$priority, c(1L, 2L, 2L, 3L))
  expect_false("D_clean" %in% q$id)
  expect_false("D_lowconf" %in% q$id)                                            # opt-in only
})

test_that("gyro_inconsistent is subsumed by coreg_fail, but reachable when coreg_fail is excluded", {
  one <- list(D = .mapping$D_gyro)
  # under the default include, coreg_fail (priority 2) shadows gyro_inconsistent (priority 4)
  expect_equal(nautilus:::.triageMappings(one, "D", "D", .inc, NULL)$review_reason, "coreg_fail")
  # excluding coreg_fail surfaces the underlying estimator disagreement
  expect_equal(nautilus:::.triageMappings(one, "D", "D", c("gyro_inconsistent"), NULL)$review_reason, "gyro_inconsistent")
})

test_that("low_confidence is opt-in; clean is never flagged", {
  q <- nautilus:::.triageMappings(.mapping, names(.mapping), names(.mapping), c(.inc, "low_confidence"), NULL)
  expect_true("D_lowconf" %in% q$id)
  expect_false("D_clean" %in% q$id)
})

test_that("unanalysed deployments (in data + video, absent from mapping) are flagged at priority 1", {
  q <- nautilus:::.triageMappings(.mapping, c(names(.mapping), "D_new"), c(names(.mapping), "D_new"), .inc, NULL)
  expect_equal(q$review_reason[q$id == "D_new"], "unanalysed")
  expect_equal(q$priority[q$id == "D_new"], 1L)
  # only when footage exists: not in video -> not flagged
  q2 <- nautilus:::.triageMappings(.mapping, c(names(.mapping), "D_new"), names(.mapping), .inc, NULL)
  expect_false("D_new" %in% q2$id)
})

test_that("explicit ids review exactly those (a suspect reason wins over the generic label)", {
  q <- nautilus:::.triageMappings(.mapping, names(.mapping), names(.mapping), .inc, ids.requested = c("D_clean", "D_conflict"))
  expect_setequal(q$id, c("D_clean", "D_conflict"))
  expect_equal(q$review_reason[q$id == "D_clean"], "user_requested")
  expect_equal(q$review_reason[q$id == "D_conflict"], "conflict")
})

.cfg <- list(TestCfg = data.frame(from = c("ax", "ay", "az"), to = c("-ax", "-ay", "-az"), stringsAsFactors = FALSE))

test_that("conflict compares Documented vs data-preferred Proposed; coreg_fail is a single indicator", {
  labels <- function(p) vapply(p$candidates, `[[`, "", "label")
  pc <- nautilus:::.candidatePlan("conflict", .mapping$D_conflict$frame_state, .cfg, 3)
  expect_equal(pc$dashboard, "validation-compare"); expect_equal(pc$mapping_source, "documented_vs_data")
  expect_equal(labels(pc), c("Documented", "Proposed"))
  # coreg_fail: the accel frame is fine (it is the gyro that mis-registers) -> single-indicator proposal,
  # not a Documented-vs-Proposed accel-frame compare
  pk <- nautilus:::.candidatePlan("coreg_fail", .mapping$D_coreg$frame_state, .cfg, 3)
  expect_equal(pk$dashboard, "validation"); expect_equal(pk$apply$type, "mapping")
  # without a configs dictionary there is no documented candidate -> single proposal dashboard
  pn <- nautilus:::.candidatePlan("conflict", .mapping$D_conflict$frame_state, NULL, 3)
  expect_equal(pn$dashboard, "validation"); expect_equal(pn$mapping_source, "proposal")
})

test_that("ambiguous compares the distinct surviving frames; other reasons show a single indicator", {
  pa <- nautilus:::.candidatePlan("ambiguous", .mapping$D_ambiguous$frame_state, NULL, 3)
  expect_equal(pa$dashboard, "validation-compare"); expect_length(pa$candidates, 2)
  expect_true(all(grepl("^Z=", vapply(pa$candidates, `[[`, "", "label"))))          # labelled by the Z axis
  pu <- nautilus:::.candidatePlan("unanalysed", NULL, NULL, 3)
  expect_equal(pu$dashboard, "validation"); expect_equal(pu$apply$type, "identity")
  pg <- nautilus:::.candidatePlan("gyro_inconsistent", .mapping$D_gyro$frame_state, NULL, 3)
  expect_equal(pg$dashboard, "validation"); expect_equal(pg$apply$type, "mapping")   # the resolved proposal
})

test_that(".survivorToAccelFt maps raw axes to signed body channels", {
  ft <- nautilus:::.survivorToAccelFt(.surv(2)[2, ])
  expect_equal(ft$from, c("ax", "az", "ay"))
  expect_equal(ft$to, c("ax", "-ay", "az"))                                      # newY_sign = -1 -> '-ay'
})

test_that(".matchClip finds the covering clip, else NULL", {
  t0 <- as.POSIXct("2020-01-01 12:00:00", tz = "UTC")
  vm <- data.frame(ID = c("A", "A"), file = c("/v/a1.mp4", "/v/a2.mp4"),
                   start = t0 + c(0, 600), end = t0 + c(300, 900), stringsAsFactors = FALSE)
  expect_equal(basename(nautilus:::.matchClip(vm, "A", t0 + 650)$file), "a2.mp4")
  expect_null(nautilus:::.matchClip(vm, "A", t0 + 450))                          # in the gap
  expect_null(nautilus:::.matchClip(vm, "B", t0 + 50))                           # no footage
})

test_that(".selectClips picks a diverse, bounded set round-robin across types", {
  t0 <- as.POSIXct("2020-01-01", tz = "UTC")
  segs <- do.call(rbind, lapply(c("turn", "roll", "dive"), function(ty)
    data.frame(id = "A", type = ty, rank = 1:3, start = t0, end = t0 + 20,
               peak_time = t0 + 10, value = 1, unit = "x", stringsAsFactors = FALSE)))
  sel <- nautilus:::.selectClips(segs, c("turn", "roll", "dive"), 3)
  expect_equal(nrow(sel), 3L)
  expect_setequal(sel$type, c("turn", "roll", "dive"))                           # one of each, not three of a kind
  expect_true(all(sel$rank == 1L))
})

test_that("argument validation aborts before any av / render work", {
  d <- tempfile(); dir.create(d); on.exit(unlink(d, recursive = TRUE), add = TRUE)
  vm <- data.frame(ID = "A", file = "x.mp4", start = Sys.time(), end = Sys.time())
  expect_error(reviewTagMapping(list(), .mapping, vm, "/no/such/dir", verbose = FALSE), "directory", ignore.case = TRUE)
  expect_error(reviewTagMapping(list(), list(), vm, d, verbose = FALSE), "mapping", ignore.case = TRUE)
  expect_error(reviewTagMapping(list(), .mapping, vm[, c("ID", "file")], d, verbose = FALSE), "column", ignore.case = TRUE)
  expect_error(reviewTagMapping(list(), .mapping, vm, d, clips.per.deployment = 0, verbose = FALSE),
               "clips.per.deployment", ignore.case = TRUE)
})

# --- the decision sheet -> applyAxisMapping overlay (no video needed) ---------------------------------
.mkReview <- function(decision = NA_character_, status = "rendered", n_clips = 1L) {
  base <- nautilus:::.asAxisMappingSet(list(
    A = list(proposal = data.frame(from = c("ax", "ay", "az"), to = c("ax", "ay", "az"), stringsAsFactors = FALSE)),
    B = list(proposal = data.frame(from = c("ax", "ay", "az"), to = c("-ax", "-ay", "az"), stringsAsFactors = FALSE))))
  cands <- list(B = list(
    list(label = "Documented", from_to = data.frame(from = c("ax", "ay", "az"), to = c("-ax", "-ay", "-az"), stringsAsFactors = FALSE)),
    list(label = "Proposed",   from_to = data.frame(from = c("ax", "ay", "az"), to = c("-ax", "-ay", "az"),  stringsAsFactors = FALSE))))
  sheet <- data.frame(id = "B", review_reason = "conflict", priority = 1L, n_clips = n_clips,
                      status = status, options = "Documented | Proposed", decision = decision, stringsAsFactors = FALSE)
  nautilus:::.newReview(sheet, candidates = cands, base_mapping = base, manifest = NULL, output.dir = NULL)
}
.mkTag <- function(id) data.frame(ID = id, ax = c(1, 2, 3), ay = c(10, 20, 30), az = c(100, 200, 300))

test_that("a nautilus_review overlays decisions onto the base and errors on undecided reviewable rows", {
  review <- .mkReview(decision = NA_character_)
  # a reviewable deployment left undecided is refused (never guess a handedness)
  expect_error(suppressWarnings(applyAxisMapping(list(B = .mkTag("B")), mapping = review, verbose = FALSE)),
               "decision", ignore.case = TRUE)
  # decide -> B overridden with the Documented config; the un-reviewed A keeps its base proposal
  review$decision[review$id == "B"] <- "Documented"
  out <- suppressWarnings(applyAxisMapping(list(A = .mkTag("A"), B = .mkTag("B")),
                                           mapping = review, return.data = TRUE, verbose = FALSE))
  expect_equal(out$B$ax, c(-1, -2, -3))                                          # Documented: ax -> -ax
  expect_equal(out$A$ax, c(1, 2, 3))                                            # base A: identity
  expect_equal(nautilus:::.getMeta(out$B)$axis_mapping$provenance[["accel"]], "review")
  expect_equal(nautilus:::.getMeta(out$A)$axis_mapping$provenance[["accel"]], "self")
})

test_that("a nautilus_review rejects an unknown decision label but tolerates undecided non-reviewable rows", {
  bad <- .mkReview(decision = "Bogus")
  expect_error(suppressWarnings(applyAxisMapping(list(B = .mkTag("B")), mapping = bad, verbose = FALSE)),
               "not one of|option", ignore.case = TRUE)
  # a flagged deployment with no clips (nothing to watch) is NOT forced: it falls through to the base
  noclip <- .mkReview(decision = NA_character_, status = "no_segment", n_clips = 0L)
  out <- suppressWarnings(applyAxisMapping(list(B = .mkTag("B")), mapping = noclip, return.data = TRUE, verbose = FALSE))
  expect_equal(out$B$ax, c(-1, -2, -3))                                          # base B proposal: ax -> -ax
})

test_that("a decision of 'Exclude' drops the deployment (case-insensitively; a valid decision)", {
  # NA on a reviewable row is an error; "Exclude" IS a decision, so it must not trip the undecided guard
  expect_error(suppressWarnings(applyAxisMapping(list(A = .mkTag("A"), B = .mkTag("B")),
               mapping = .mkReview(decision = NA_character_), verbose = FALSE)), "decision", ignore.case = TRUE)
  for (val in c("Exclude", "exclude", " EXCLUDE ")) {                           # canonical + case/whitespace-tolerant
    out <- suppressWarnings(applyAxisMapping(list(A = .mkTag("A"), B = .mkTag("B")),
                 mapping = .mkReview(decision = val), return.data = TRUE, verbose = FALSE))
    expect_setequal(names(out), "A")                                           # B excluded -> dropped; base A kept
    expect_equal(attr(out, "excluded"), "B")
  }
})

test_that("mapping decision labels are matched case-insensitively", {
  out <- suppressWarnings(applyAxisMapping(list(A = .mkTag("A"), B = .mkTag("B")),
               mapping = .mkReview(decision = "documented"), return.data = TRUE, verbose = FALSE))  # lowercase label
  expect_equal(out$B$ax, c(-1, -2, -3))                                         # still resolves "Documented" -> -ax
})

test_that("'exclude' is universal: it applies to a flagged row that has no candidate comparison", {
  base <- nautilus:::.asAxisMappingSet(list(
    A = list(proposal = data.frame(from = c("ax","ay","az"), to = c("ax","ay","az"), stringsAsFactors = FALSE)),
    Z = list(proposal = data.frame(from = c("ax","ay","az"), to = c("ax","ay","az"), stringsAsFactors = FALSE))))
  sheet <- data.frame(id = "A", review_reason = "gyro_inconsistent", priority = 4L, n_clips = 1L,
                      status = "rendered", options = "", decision = "Exclude", stringsAsFactors = FALSE)
  rev <- nautilus:::.newReview(sheet, candidates = list(), base_mapping = base, manifest = NULL, output.dir = NULL)
  out <- suppressWarnings(applyAxisMapping(list(A = .mkTag("A"), Z = .mkTag("Z")),
               mapping = rev, return.data = TRUE, verbose = FALSE))
  expect_false("A" %in% names(out))                                             # excluded even with no options
  expect_true("Z" %in% names(out))
})

test_that("end-to-end: an unanalysed deployment yields a single-path review sheet (no decision forced)", {
  skip_if_not_installed("av")
  skip_if(!nzchar(Sys.which("ffmpeg")), "ffmpeg not available")
  t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
  fs <- 5; n <- 150L; tt <- t0 + (seq_len(n) - 1) / fs                            # 30 s at 5 Hz
  rollev <- as.numeric(tt - t0) >= 12 & as.numeric(tt - t0) <= 18                 # a clear sustained roll
  raw <- data.table::data.table(
    ID = "SYN", datetime = tt,
    depth = 5 + 20 * pmin(pmax((as.numeric(tt - t0) - 5) / 20, 0), 1),            # a dive 5 -> 25 m
    ax = rnorm(n, 0, 0.05),
    ay = ifelse(rollev, 0.8, rnorm(n, 0, 0.05)),
    az = ifelse(rollev, 0.6, 1) + rnorm(n, 0, 0.02),
    gx = rnorm(n, 0, 0.05), gy = rnorm(n, 0, 0.05), gz = rnorm(n, 0, 0.05),
    mx = cos(seq_len(n) / 20), my = sin(seq_len(n) / 20), mz = rnorm(n, 0, 0.02))

  # a tall-enough synthetic source video spanning the deployment (filename carries no timestamp)
  src <- tempfile(fileext = ".mp4"); on.exit(unlink(src), add = TRUE)
  suppressWarnings(av::av_capture_graphics(
    { for (i in seq_len(n)) { par(mar = c(0, 0, 0, 0)); plot.new(); rect(0, 0, 1, 1, col = "steelblue", border = NA) } },
    output = src, width = 480, height = 720, framerate = fs, verbose = FALSE))
  vm <- data.frame(ID = "SYN", file = src, start = t0, end = t0 + n / fs, stringsAsFactors = FALSE)

  out <- tempfile(); dir.create(out); on.exit(unlink(out, recursive = TRUE), add = TRUE)
  review <- suppressWarnings(suppressMessages(
    reviewTagMapping(data = list(SYN = raw), mapping = list(D_clean = .mapping$D_clean),
                     video.metadata = vm, output.dir = out, types = c("roll", "dive"),
                     n = 2, window = 10, clips.per.deployment = 1, overlay.fps = 5, verbose = FALSE)))

  expect_s3_class(review, "nautilus_review")
  expect_equal(review$review_reason[review$id == "SYN"], "unanalysed")
  expect_identical(review$options[review$id == "SYN"], "")                       # single-path: no A/B choice
  man <- attr(review, "review_manifest")
  expect_equal(man$mapping_source[1], "identity_raw")
  expect_true(any(man$status == "rendered"))
  expect_true(file.exists(man$clip[man$status == "rendered"][1]))
})

test_that("end-to-end: a conflict renders a labelled Documented-vs-Proposed compare clip", {
  skip_if_not_installed("av")
  skip_if(!nzchar(Sys.which("ffmpeg")), "ffmpeg not available")
  t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
  fs <- 5; n <- 150L; tt <- t0 + (seq_len(n) - 1) / fs
  rollev <- as.numeric(tt - t0) >= 12 & as.numeric(tt - t0) <= 18
  raw <- data.table::data.table(
    ID = "REF", datetime = tt,
    depth = 5 + 20 * pmin(pmax((as.numeric(tt - t0) - 5) / 20, 0), 1),
    ax = rnorm(n, 0, 0.05),
    ay = ifelse(rollev, 0.8, rnorm(n, 0, 0.05)),
    az = ifelse(rollev, 0.6, 1) + rnorm(n, 0, 0.02),
    gx = rnorm(n, 0, 0.05), gy = rnorm(n, 0, 0.05), gz = rnorm(n, 0, 0.05),
    mx = cos(seq_len(n) / 20), my = sin(seq_len(n) / 20), mz = rnorm(n, 0, 0.02))
  # a conflict element with a sign-flipped Z survivor, so Proposed differs from Documented
  refl <- .mkmap(conflicts = "doc conflicts", prior = "conflict")
  refl$frame_state$survivors$newZ_sign <- -1

  src <- tempfile(fileext = ".mp4"); on.exit(unlink(src), add = TRUE)
  suppressWarnings(av::av_capture_graphics(
    { for (i in seq_len(n)) { par(mar = c(0, 0, 0, 0)); plot.new(); rect(0, 0, 1, 1, col = "steelblue", border = NA) } },
    output = src, width = 480, height = 720, framerate = fs, verbose = FALSE))
  vm <- data.frame(ID = "REF", file = src, start = t0, end = t0 + n / fs, stringsAsFactors = FALSE)

  out <- tempfile(); dir.create(out); on.exit(unlink(out, recursive = TRUE), add = TRUE)
  review <- suppressWarnings(suppressMessages(
    reviewTagMapping(data = list(REF = raw), mapping = list(REF = refl), configs = .cfg,
                     video.metadata = vm, output.dir = out, types = c("roll", "dive"),
                     n = 2, window = 10, clips.per.deployment = 1, overlay.fps = 5, verbose = FALSE)))

  expect_s3_class(review, "nautilus_review")
  expect_equal(review$review_reason[review$id == "REF"], "conflict")
  expect_match(review$options[review$id == "REF"], "Documented"); expect_match(review$options[review$id == "REF"], "Proposed")
  expect_true(is.na(review$decision[review$id == "REF"]))
  man <- attr(review, "review_manifest")
  expect_equal(man$mapping_source[1], "documented_vs_data")                         # the compare path
  expect_true(any(man$status == "rendered"))
  expect_true(file.exists(man$clip[man$status == "rendered"][1]))

  # undecided -> refused; decided -> applied with review provenance
  expect_error(suppressWarnings(applyAxisMapping(list(REF = raw), mapping = review, verbose = FALSE)),
               "decision", ignore.case = TRUE)
  review$decision[review$id == "REF"] <- "Proposed"
  applied <- suppressWarnings(applyAxisMapping(list(REF = raw), mapping = review, return.data = TRUE, verbose = FALSE))
  expect_equal(nautilus:::.getMeta(applied$REF)$axis_mapping$provenance[["accel"]], "review")
})
