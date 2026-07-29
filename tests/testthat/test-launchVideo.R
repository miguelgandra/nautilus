# Tests for launchVideo(): input validation + match logic (VLC is never actually launched here).

.vm <- function() data.frame(ID = "A01",
                             start = as.POSIXct("2020-01-01 00:00:00", tz = "UTC"),
                             end   = as.POSIXct("2020-01-01 00:10:00", tz = "UTC"),
                             video = "v1.mp4", file = "/tmp/v1.mp4", stringsAsFactors = FALSE)

test_that("input validation aborts clearly", {
  dt <- as.POSIXct("2020-01-01 00:05:00", tz = "UTC")
  expect_error(launchVideo("", dt, .vm()), "id", ignore.case = TRUE)                        # empty id
  expect_error(launchVideo("A01", "2020-01-01", .vm()), "POSIXct")                          # datetime not POSIXct
  expect_error(launchVideo("A01", dt, data.frame(ID = "A01")), "column", ignore.case = TRUE) # missing columns
  expect_error(launchVideo("ZZZ", dt, .vm()), "not present", ignore.case = TRUE)            # id absent
})

test_that("a datetime outside any video returns FALSE (no launch)", {
  out_after  <- suppressMessages(launchVideo("A01", as.POSIXct("2020-01-01 05:00:00", tz = "UTC"), .vm()))
  out_before <- suppressMessages(launchVideo("A01", as.POSIXct("2019-12-31 23:00:00", tz = "UTC"), .vm()))
  expect_false(out_after)
  expect_false(out_before)
})

test_that("a matching datetime aborts when VLC is not found (rather than launching)", {
  dt <- as.POSIXct("2020-01-01 00:05:00", tz = "UTC")               # within [00:00, 00:10]
  expect_error(launchVideo("A01", dt, .vm(), vlc.path = "/no/such/vlc", close.existing = FALSE),
               "VLC", ignore.case = TRUE)
})


# ---------------------------------------------------------------------------------------------------
# Segment selection and file resolution
# ---------------------------------------------------------------------------------------------------

test_that("a segment whose file is NA is never selected", {
  vm <- rbind(.vm(), .vm())
  vm$file[1] <- NA_character_                       # same window, but no usable file
  vm$video[2] <- "v2.mp4"; vm$file[2] <- tempfile(fileext = ".mp4")
  file.create(vm$file[2]); on.exit(unlink(vm$file[2]), add = TRUE)
  dt <- as.POSIXct("2020-01-01 00:05:00", tz = "UTC")
  # resolution reaches the VLC lookup, which means the NA row was skipped and row 2 was chosen
  expect_error(launchVideo("A01", dt, vm, vlc.path = "/no/such/vlc", close.existing = FALSE), "VLC")
})

test_that("a stale video path aborts rather than reporting a successful launch", {
  # the metadata is built once and often reused, so the file can be moved or the drive unmounted
  dt <- as.POSIXct("2020-01-01 00:05:00", tz = "UTC")
  expect_error(launchVideo("A01", dt, .vm(), close.existing = FALSE), "not found", ignore.case = TRUE)
})

test_that("segment matching is inclusive at both boundaries", {
  vm <- .vm(); vm$file <- tempfile(fileext = ".mp4")
  file.create(vm$file); on.exit(unlink(vm$file), add = TRUE)
  for (edge in c("2020-01-01 00:00:00", "2020-01-01 00:10:00")) {
    # reaching the VLC lookup proves the datetime matched a segment
    expect_error(launchVideo("A01", as.POSIXct(edge, tz = "UTC"), vm,
                             vlc.path = "/no/such/vlc", close.existing = FALSE), "VLC")
  }
})

test_that("each 'no covering segment' branch reports its own reason", {
  vm <- .vm()
  msg <- function(dt) {
    m <- character(0)
    withCallingHandlers(launchVideo("A01", as.POSIXct(dt, tz = "UTC"), vm),
                        message = function(c) { m <<- c(m, conditionMessage(c)); invokeRestart("muffleMessage") })
    paste(m, collapse = " ")
  }
  expect_match(msg("2020-01-01 09:00:00"), "later",   ignore.case = TRUE)
  expect_match(msg("2019-12-31 09:00:00"), "earlier", ignore.case = TRUE)
})

# ---------------------------------------------------------------------------------------------------
# Executable resolution
# ---------------------------------------------------------------------------------------------------

test_that(".vlcBin honours an explicit path and rejects a missing one", {
  f <- tempfile(); file.create(f); on.exit(unlink(f), add = TRUE)
  expect_identical(nautilus:::.vlcBin(f), f)
  expect_error(nautilus:::.vlcBin("/no/such/vlc"), "not found", ignore.case = TRUE)
})

test_that(".vlcBin searches the system path before the per-OS install locations", {
  # a fake 'vlc' early on PATH must win, proving lookup is not hardcoded to one absolute location
  dir <- tempfile(); dir.create(dir)
  bin <- file.path(dir, if (.Platform$OS.type == "windows") "vlc.exe" else "vlc")
  file.create(bin); Sys.chmod(bin, "0755")
  old <- Sys.getenv("PATH"); Sys.setenv(PATH = paste(dir, old, sep = .Platform$path.sep))
  on.exit({ Sys.setenv(PATH = old); unlink(dir, recursive = TRUE) }, add = TRUE)
  expect_identical(normalizePath(nautilus:::.vlcBin(NULL)), normalizePath(bin))
})

test_that("vlc.path is type-checked", {
  dt <- as.POSIXct("2020-01-01 00:05:00", tz = "UTC")
  expect_error(launchVideo("A01", dt, .vm(), vlc.path = 42), "vlc.path")
})

test_that(".closeVLC matches on process name, not the full command line", {
  # -x (exact name) rather than -f (full command line): `pkill -f vlc` would also match an unrelated
  # process whose arguments merely contain "vlc"
  body_src <- paste(deparse(body(nautilus:::.closeVLC)), collapse = " ")
  expect_match(body_src, '"-x"')
  expect_false(grepl('"-f"', body_src))
})
