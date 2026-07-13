# Tests for applyAxisMapping(): the post-import axis re-map engine (absolute/relative,
# idempotency, handedness, faulty-axis drops, metadata provenance).

# a raw, freshly-imported tag (no mapping applied) with distinguishable per-axis values
.raw_tag <- function(id = "A01", n = 3, sensors = c("accel", "gyro", "mag")) {
  dt <- data.table::data.table(ID = id,
                               datetime = as.POSIXct("2020-01-01", tz = "UTC") + 0:(n - 1),
                               depth = as.numeric(seq_len(n)))
  add <- function(prefix) {
    dt[[paste0(prefix, "x")]] <<- seq_len(n) * 1
    dt[[paste0(prefix, "y")]] <<- seq_len(n) * 10
    dt[[paste0(prefix, "z")]] <<- seq_len(n) * 100
  }
  if ("accel" %in% sensors) add("a")
  if ("gyro"  %in% sensors) add("g")
  if ("mag"   %in% sensors) add("m")
  m <- nautilus:::.newNautilusMeta(); m$id <- id
  nautilus:::new_nautilus_tag(dt, m)
}

run <- function(...) {
  res <- NULL
  invisible(capture.output(suppressMessages(res <- applyAxisMapping(..., verbose = FALSE))))
  res
}

# 90-degree rotation about z (proper, det +1): new_ax = -old_ay, new_ay = old_ax
.Mz <- data.frame(from = c("ay", "ax"), to = c("-ax", "ay"), stringsAsFactors = FALSE)
# 90-degree rotation about x (proper): new_ay = -old_az, new_az = old_ay
.Mx <- data.frame(from = c("az", "ay"), to = c("-ay", "az"), stringsAsFactors = FALSE)

# a longer, genuinely-rotating tag with a co-registered accel(gravity)+gyro pair (or the raw gyro
# sign-flipped = a genuine accel/gyro mis-registration). Enough rotation for the co-registration check.
.rotating_tag <- function(id = "R1", flip_gyro = FALSE, fs = 10, n = 1200) {
  t  <- (seq_len(n) - 1) / fs
  th <- 0.6 * sin(2 * pi * t / 10) + 0.9; ph <- 2 * pi * t / 13
  ghat <- cbind(sin(th) * cos(ph), sin(th) * sin(ph), cos(th))
  xp <- function(a, b) cbind(a[,2]*b[,3]-a[,3]*b[,2], a[,3]*b[,1]-a[,1]*b[,3], a[,1]*b[,2]-a[,2]*b[,1])
  m  <- rbind(ghat[2, ] - ghat[1, ], (ghat[3:n, ] - ghat[1:(n-2), ]) / 2, ghat[n, ] - ghat[n-1, ]) * fs
  om <- xp(m, ghat); if (flip_gyro) om <- -om
  dt <- data.table::data.table(ID = id,
    datetime = as.POSIXct("2020-01-01", tz = "UTC") + t,
    depth = as.numeric(20 + 0 * t),
    ax = ghat[, 1], ay = ghat[, 2], az = ghat[, 3],
    gx = om[, 1], gy = om[, 2], gz = om[, 3])
  m0 <- nautilus:::.newNautilusMeta(); m0$id <- id
  nautilus:::new_nautilus_tag(dt, m0)
}

test_that("a fresh mapping remaps the columns and records provenance", {
  x <- .raw_tag()
  out <- run(list(A01 = x), .Mz)[["A01"]]
  # new ax = -old ay ; new ay = old ax ; az unchanged
  expect_equal(out$ax, -(seq_len(3) * 10))
  expect_equal(out$ay,  seq_len(3) * 1)
  expect_equal(out$az,  seq_len(3) * 100)

  m <- tagMetadata(out)$axis_mapping
  expect_true(m$applied)
  expect_equal(m$source, "manual")                     # a bare from/to df -> producer "manual"
  expect_equal(unname(m$provenance["accel"]), "manual")
  expect_equal(unname(m$determinant["accel"]), 1L)     # proper rotation
  expect_equal(processingHistory(out)$step[1], "applyAxisMapping")
})

test_that("a checkTagMapping-shaped object routes per-deployment and records 'self'", {
  data <- list(A01 = .raw_tag("A01"), B02 = .raw_tag("B02"))
  qc <- structure(list(A01 = list(id = "A01", proposal = .Mz),
                       B02 = list(id = "B02", proposal = .Mx)),
                  nautilus.mapping.producer = "checkTagMapping")
  out <- run(data, qc)
  expect_equal(out$A01$ax, -(seq_len(3) * 10))          # A01 got Mz
  expect_equal(out$B02$ay, -(seq_len(3) * 100))         # B02 got Mx (new ay = -old az)
  expect_equal(tagMetadata(out$A01)$axis_mapping$source, "checkTagMapping")
  expect_equal(unname(tagMetadata(out$A01)$axis_mapping$provenance["accel"]), "self")
})

test_that("a consensusAxisMapping-shaped object routes per-deployment and records per-family origin", {
  data <- list(A01 = .raw_tag("A01"), B02 = .raw_tag("B02"))
  qc <- list(mappings = list(A01 = .Mz, B02 = .Mx),
             provenance = data.frame(id = c("A01", "B02"), accel = c("self", "consensus"),
                                     gyro = "none", mag = "none", stringsAsFactors = FALSE))
  out <- run(data, qc)
  expect_equal(out$A01$ax, -(seq_len(3) * 10))
  expect_equal(tagMetadata(out$A01)$axis_mapping$source, "consensusAxisMapping")
  expect_equal(unname(tagMetadata(out$A01)$axis_mapping$provenance["accel"]), "self")
  expect_equal(unname(tagMetadata(out$B02)$axis_mapping$provenance["accel"]), "consensus")  # group-rescued
})

test_that("a named list of from/to tables routes per-deployment (manual origin)", {
  data <- list(A01 = .raw_tag("A01"), B02 = .raw_tag("B02"))
  out <- run(data, list(A01 = .Mz, B02 = .Mx))
  expect_equal(out$A01$ax, -(seq_len(3) * 10))
  expect_equal(out$B02$ay, -(seq_len(3) * 100))
  expect_equal(tagMetadata(out$B02)$axis_mapping$source, "manual")
})

test_that("empty / unmatched mappings are handled: skip-and-leave-unchanged, warn on no-dataset", {
  data <- list(A01 = .raw_tag("A01"), B02 = .raw_tag("B02"))
  qc <- list(mappings = list(A01 = .Mz, B02 = .Mz[0, ], C03 = .Mx),    # B02 unresolved; C03 has no dataset
             provenance = data.frame(id = c("A01", "B02", "C03"), accel = c("self", "ambiguous", "self"),
                                     gyro = "none", mag = "none", stringsAsFactors = FALSE))
  expect_warning(res <- applyAxisMapping(data, qc, verbose = FALSE), "no matching dataset")
  expect_equal(res$A01$ax, -(seq_len(3) * 10))          # A01 remapped
  expect_equal(res$B02$ax,  seq_len(3) * 1)             # B02 left unchanged (empty mapping)
  expect_false(tagMetadata(res$B02)$axis_mapping$applied)
})

test_that("zero id-overlap between mapping and data is an error", {
  data <- list(A01 = .raw_tag("A01"))
  qc <- structure(list(X1 = list(id = "X1", proposal = .Mz)), nautilus.mapping.producer = "checkTagMapping")
  expect_error(suppressWarnings(applyAxisMapping(data, qc, verbose = FALSE)), "None of the mapping ids")
})

test_that("absolute re-application of the same mapping is a no-op (idempotent)", {
  x <- .raw_tag()
  once  <- run(list(A01 = x), .Mz)[["A01"]]
  twice <- run(list(A01 = once), .Mz)[["A01"]]            # absolute, same mapping
  expect_equal(twice$ax, once$ax)
  expect_equal(twice$ay, once$ay)
  expect_equal(twice$az, once$az)
})

test_that("absolute re-map composes from raw (M1 then M2 == M2 from raw)", {
  raw <- .raw_tag()
  d1   <- run(list(A01 = raw), .Mz)[["A01"]]
  d2   <- run(list(A01 = d1),  .Mx)[["A01"]]              # absolute: should equal Mx from raw
  dref <- run(list(A01 = .raw_tag()), .Mx)[["A01"]]
  expect_equal(d2$ax, dref$ax)
  expect_equal(d2$ay, dref$ay)
  expect_equal(d2$az, dref$az)
  expect_equal(unname(tagMetadata(d2)$axis_mapping$net$accel), unname(tagMetadata(dref)$axis_mapping$net$accel))
})

test_that("relative mode applies on top of the current state", {
  raw <- .raw_tag()
  d1  <- run(list(A01 = raw), .Mz)[["A01"]]
  drel <- run(list(A01 = d1), .Mz, relative = TRUE)[["A01"]]  # Mz applied twice
  # Mz twice == 180 about z: new_ax = -old_ax, new_ay = -old_ay
  expect_equal(drel$ax, -(seq_len(3) * 1))
  expect_equal(drel$ay, -(seq_len(3) * 10))
})

test_that("faulty-axis 'NA' drops are applied and recorded", {
  x <- .raw_tag()
  drop <- data.frame(from = c("mx", "my", "mz"), to = c("NA", "NA", "NA"), stringsAsFactors = FALSE)
  out <- run(list(A01 = x), drop)[["A01"]]
  expect_true(all(is.na(out$mx))); expect_true(all(is.na(out$my))); expect_true(all(is.na(out$mz)))
  expect_setequal(tagMetadata(out)$axis_mapping$dropped, c("mx", "my", "mz"))
})

test_that("a reflection accel mapping applies without warning and co-registers the gyro", {
  x <- .raw_tag()
  # the real CATS reflection: ax->-ay, ay->-ax (det -1). A per-family reflection is a benign device
  # convention now (no warning); the gyro is auto-derived as det(M)*M so it stays co-registered.
  refl <- data.frame(from = c("ax", "ay"), to = c("-ay", "-ax"), stringsAsFactors = FALSE)
  expect_no_warning(res <- applyAxisMapping(list(A01 = x), refl, verbose = FALSE))
  out <- res[["A01"]]
  # accel still applied (reflection), determinant recorded descriptively
  expect_equal(out$ax, -(seq_len(3) * 10))
  expect_equal(unname(tagMetadata(out)$axis_mapping$determinant["accel"]), -1L)
  # gyro was completed and co-registered: derived map gy->gx, gx->gy, gz->-gz (det(M)*M, det +1)
  expect_equal(out$gx,  seq_len(3) * 10)                  # new gx = old gy
  expect_equal(out$gy,  seq_len(3) * 1)                   # new gy = old gx
  expect_equal(out$gz, -(seq_len(3) * 100))               # new gz = -old gz
  expect_equal(unname(tagMetadata(out)$axis_mapping$determinant["gyro"]), 1L)  # proper (co-registered)
  # magnetometer keeps its own strategy: not completed here, left in the raw frame
  expect_equal(out$mx,  seq_len(3) * 1)
  expect_null(tagMetadata(out)$axis_mapping$net$mag)
})

test_that("co-registration: warns only on a genuine accel/gyro mis-registration, never a reflection", {
  # co-registered raw gyro -> derived gyro stays co-registered -> no warning, coreg_corr ~ +1 recorded
  expect_no_warning(res <- applyAxisMapping(list(R1 = .rotating_tag(flip_gyro = FALSE)), .Mz, verbose = FALSE))
  cc <- tagMetadata(res$R1)$axis_mapping$coreg_corr
  expect_gt(cc, 0.9)
  # raw gyro sign-flipped relative to accel -> decisive mismatch survives the mapping -> warns
  expect_warning(applyAxisMapping(list(R1 = .rotating_tag(flip_gyro = TRUE)), .Mz, verbose = FALSE),
                 "co-regist", ignore.case = TRUE)
})

test_that("co-registration is a no-op on the tiny 3-row fixture (NA, no warning)", {
  expect_no_warning(res <- applyAxisMapping(list(A01 = .raw_tag()), .Mz, verbose = FALSE))
  expect_true(is.na(tagMetadata(res$A01)$axis_mapping$coreg_corr))
})

test_that("check.handedness = FALSE skips the co-registration check entirely", {
  expect_no_warning(res <- applyAxisMapping(list(R1 = .rotating_tag(flip_gyro = TRUE)), .Mz,
                                            check.handedness = FALSE, verbose = FALSE))
  expect_true(is.na(tagMetadata(res$R1)$axis_mapping$coreg_corr))
})

test_that("families without channels are skipped; multi-family mapping works", {
  x <- .raw_tag(sensors = "accel")                        # accel-only tag
  map <- rbind(.Mz, data.frame(from = "gx", to = "gy", stringsAsFactors = FALSE))
  out <- run(list(A01 = x), map)[["A01"]]
  expect_equal(out$ax, -(seq_len(3) * 10))                # accel remapped
  expect_false("gx" %in% names(out))                      # gyro absent -> skipped, not created
  expect_null(tagMetadata(out)$axis_mapping$net$gyro)
})

test_that("file-path input round-trips and saves", {
  d <- file.path(tempdir(), paste0("aam_", as.integer(runif(1, 1, 1e7))))
  dir.create(d, showWarnings = FALSE); on.exit(unlink(d, recursive = TRUE), add = TRUE)
  saveRDS(.raw_tag("Z9"), file.path(d, "Z9.rds"))
  out <- applyAxisMapping(file.path(d, "Z9.rds"), .Mz,
                          output.dir = d, output.suffix = "-mapped", verbose = FALSE)
  expect_named(out, "Z9")
  saved <- readRDS(file.path(d, "Z9-mapped.rds"))
  expect_true(tagMetadata(saved)$axis_mapping$applied)
})

test_that("input validation errors are clear", {
  x <- .raw_tag()
  expect_error(applyAxisMapping(list(A01 = x), mapping = data.frame(a = 1)), "from.*to")
  expect_error(applyAxisMapping(list(A01 = x), mapping = .Mz, return.data = FALSE), "output.dir")
  expect_error(applyAxisMapping(list(A01 = x), mapping = .Mz[0, ]), "non-empty")
})

# det +1 accel rotation (M = .M) and a det -1 mag reflection, as full triplets
.acc3 <- data.frame(from = c("ay", "ax", "az"), to = c("ax", "-ay", "az"), stringsAsFactors = FALSE)
.magR <- data.frame(from = c("mx", "my", "mz"), to = c("-mx", "my", "mz"), stringsAsFactors = FALSE)

test_that("verbose = 2 is a unified cli dashboard: all three families, plain-language reflection, no raw prints", {
  qc <- list(mappings = list(A01 = .acc3, B02 = .magR),
             provenance = data.frame(id = c("A01", "B02"), accel = c("self", "none"),
                                     gyro = "none", mag = c("none", "self"), stringsAsFactors = FALSE))
  out <- paste(cli::cli_fmt(suppressWarnings(suppressMessages(
    applyAxisMapping(list(A01 = .raw_tag("A01"), B02 = .raw_tag("B02")), mapping = qc, verbose = 2)))),
    collapse = "\n")
  expect_match(out, "A01 \\(1/2\\)")                              # cli sub-header
  expect_match(out, "accel:")                                    # per-family lines present
  expect_match(out, "gyro:.*untouched \\(no mapping\\)")         # B02 has no gyro map -> untouched
  expect_match(out, "reflection \\(left-handed convention\\)")   # det -1 noted descriptively (B02 mag)
  expect_match(out, "reflection \\(left-handed\\) axis convention")  # neutral summary tally (info, not warning)
  expect_false(grepl("det +1", out, fixed = TRUE))               # healthy +1 determinant omitted
  expect_false(grepl("Saved:", out, fixed = TRUE))               # raw cat() print gone
  expect_false(grepl("remapped accel, mag", out, fixed = TRUE))  # terse outcome at level 2 (no family list)
})

test_that("verbose = 1 keeps a self-describing one-line outcome and no per-family block", {
  out <- paste(cli::cli_fmt(suppressWarnings(suppressMessages(
    applyAxisMapping(list(A01 = .raw_tag("A01")), mapping = .acc3, verbose = 1)))), collapse = "\n")
  expect_match(out, "A01  remapped accel")                       # full outcome carries id + families
  expect_false(grepl("untouched", out, fixed = TRUE))            # detail block is level-2 only
})


# ---- configs (apply documented orientation by axis_config) --------------------------------------

.tag_cfg <- function(cfg, id = "A01", ...) {
  x <- .raw_tag(id = id, ...); m <- nautilus:::.getMeta(x)
  m$tag$axis_config <- cfg; nautilus:::.setMeta(x, m); x
}

test_that("applyAxisMapping(configs=) looks up each tag's axis_config and applies it", {
  configs <- list("rot_z" = .Mz)                 # proper rotation (det +1)
  out <- run(list(A01 = .tag_cfg("rot_z")), configs = configs)[["A01"]]
  expect_equal(out$ax, -(seq_len(3) * 10))       # new ax = -old ay
  expect_equal(out$ay,  seq_len(3) * 1)          # new ay =  old ax
  m <- tagMetadata(out)$axis_mapping
  expect_true(m$applied)
  expect_equal(m$source, "axis_config")
  expect_equal(unname(m$provenance["accel"]), "axis_config")
})

test_that("a tag with blank/NA axis_config is left unchanged under configs", {
  out <- run(list(A01 = .tag_cfg(NA_character_)), configs = list("rot_z" = .Mz))[["A01"]]
  expect_equal(out$ax, seq_len(3) * 1)           # untouched
  expect_false(tagMetadata(out)$axis_mapping$applied)
})

test_that("a config name not in the dictionary is a clear error", {
  expect_error(run(list(A01 = .tag_cfg("CATS Camra")), configs = list("CATS Camera" = .Mz)),
               "not in")
})

test_that("mapping and configs are mutually exclusive and one is required", {
  x <- .raw_tag()
  expect_error(applyAxisMapping(list(A01 = x), mapping = .Mz, configs = list(a = .Mz), verbose = FALSE),
               "only one")
  expect_error(applyAxisMapping(list(A01 = x), verbose = FALSE), "either")
})

test_that(".validateConfigs rejects malformed dictionaries", {
  expect_error(nautilus:::.validateConfigs(list(.Mz)), "uniquely-named")          # unnamed
  expect_error(nautilus:::.validateConfigs(list(a = data.frame(x = 1))), "from")  # not a from/to df
  expect_error(nautilus:::.validateConfigs(list(a = data.frame(from = "depth", to = "depth"))), "non-IMU")
})
