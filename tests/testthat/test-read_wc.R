# Tests for the ANCILLARY axis of ingestion: read_wc() (a co-deployed Wildlife Computers tag's files ->
# typed ancillary records) and attachAncillary() (align that device's clock onto the primary tag's and
# hand back streams in the exact shape meta$ancillary stores).
#
# The clock-alignment MATHS is covered by test-alignment.R; what is locked here is the SEAM - the record
# shape, what reaches meta, and the provenance contract. Fixtures are synthetic WC exports (the real
# archives are research data and are not shipped), reproducing the format's quirks: the "%H:%M:%OS %d-%b-%Y"
# timestamps, a Dry column, and config/event rows with blank fields.

.wc_dives <- function(dur = 7200, fs = 1, base = 1.5e9) {
  t <- seq(0, dur, by = 1 / fs)
  list(t = base + t,
       depth = 50 * exp(-((t - 1500) / 200)^2) + 80 * exp(-((t - 3800) / 300)^2) +
               40 * exp(-((t - 6000) / 250)^2))
}
.wc_stamp <- function(x) format(as.POSIXct(x, origin = "1970-01-01", tz = "UTC"), "%H:%M:%OS %d-%b-%Y")

# a WC archive: the shared depth channel + the wet/dry signal, on the WC's own clock
.wc_archive <- function(file = tempfile(fileext = ".csv"), offset = 0, p = .wc_dives()) {
  d <- data.frame(Time = .wc_stamp(p$t + offset), Depth = p$depth, Dry = as.integer(p$depth < 1))
  utils::write.csv(d, file, row.names = FALSE, quote = FALSE)
  file
}
# a WC Locations export: the fix history, on the WC's own clock
.wc_locations <- function(file = tempfile(fileext = ".csv"), offset = 0, n = 5L, base = 1.5e9) {
  t <- base + seq(0, by = 1200, length.out = n)
  d <- data.frame(Date = .wc_stamp(t + offset), Type = "GPS",
                  Latitude = 38.6 + seq_len(n) / 1000, Longitude = -28.6, Quality = "3")
  utils::write.csv(d, file, row.names = FALSE, quote = FALSE)
  file
}
# the primary archival tag: the alignment reference, never moved
.wc_primary <- function(p = .wc_dives()) {
  data.table::data.table(datetime = as.POSIXct(p$t, origin = "1970-01-01", tz = "UTC"), depth = p$depth)
}


# ---- read_wc(): files -> typed records ------------------------------------------------------------

test_that("read_wc() reads both streams and tags each with its kind", {
  anc <- nautilus:::read_wc(.wc_locations(), .wc_archive())
  expect_named(anc, c("positions", "dry"))
  expect_identical(anc$positions$kind, "positions")
  expect_identical(anc$dry$kind, "events")
  expect_equal(nrow(anc$positions$data), 5L)
  expect_true(all(c("datetime", "lon", "lat") %in% names(anc$positions$data)))
  expect_true(all(c("datetime", "dry") %in% names(anc$dry$data)))
})

test_that("read_wc() returns an empty (but attributed) list when there is no co-deployed tag", {
  anc <- nautilus:::read_wc(NA_character_, NA_character_)
  expect_length(anc, 0L)
  expect_identical(attr(anc, "archive_file"), NA_character_)     # the aligner's route to the archive
  # a nonexistent path is the same as none: absent, not an error
  expect_length(nautilus:::read_wc(tempfile(), tempfile()), 0L)
})

test_that("read_wc() reads each stream independently of the other", {
  only_pos <- nautilus:::read_wc(.wc_locations(), NA_character_)
  expect_named(only_pos, "positions")
  only_dry <- nautilus:::read_wc(NA_character_, .wc_archive())
  expect_named(only_dry, "dry")
})

test_that("read_wc() preserves attributes on a reader's record", {
  # regression: building the record with `c(list(kind = ...), rec)` silently dropped every attribute the
  # reader had attached. Nothing carries one today, so this guards the seam, not current behaviour.
  rec <- list(source = "x", data = data.frame(datetime = Sys.time(), dry = TRUE))
  attr(rec, "provenance") <- "keep me"
  with_mocked_bindings(
    .readDryAncillary = function(...) rec,
    .readPositionsAncillary = function(...) NULL,
    .package = "nautilus",
    {
      anc <- nautilus:::read_wc(NA_character_, NA_character_)
      expect_identical(attr(anc$dry, "provenance"), "keep me")
      expect_identical(anc$dry$kind, "events")
    }
  )
})


# ---- .validateAncillary(): the record contract -----------------------------------------------------

test_that(".validateAncillary() rejects malformed records and unknown kinds", {
  expect_error(nautilus:::.validateAncillary(list(kind = "positions")), "kind.*data|data")
  expect_error(nautilus:::.validateAncillary(list(data = 1)), "kind")
  expect_error(nautilus:::.validateAncillary("nope"), "must be a list")
  expect_error(nautilus:::.validateAncillary(list(kind = "telepathy", data = 1)), "Unknown ancillary kind")
  expect_true(nautilus:::.validateAncillary(list(kind = "channel", data = 1)))
})


# ---- attachAncillary(): alignment + what reaches meta ----------------------------------------------

test_that("attachAncillary() shifts every WC stream onto the primary clock and records the offset", {
  delta <- 300                                    # the WC clock runs 300 s ahead of the primary tag
  p <- .wc_dives()
  anc <- nautilus:::read_wc(.wc_locations(offset = delta), .wc_archive(offset = delta, p = p))
  t_before <- anc$positions$data$datetime
  att <- nautilus:::attachAncillary(.wc_primary(p), anc, align = alignmentControl())

  expect_identical(att$info$status, "aligned")
  expect_equal(att$info$offset.seconds, -delta, tolerance = 1)    # shift ADDED to the WC timestamps
  expect_gt(att$info$correlation, 0.99)
  expect_equal(att$info$n_fixes_shifted, 5L)
  expect_equal(as.numeric(att$ancillary$positions$data$datetime - t_before), rep(-delta, 5), tolerance = 1)
})

test_that("attachAncillary() records an abstention even when there is nothing to align", {
  # regression: an early return on an empty `anc` skipped the aligner entirely, silently dropping
  # meta$ancillary$alignment from EVERY deployment with no co-deployed tag. The abstention is provenance -
  # "the aligner ran and declined", not "the aligner never ran" - and the two must stay distinguishable.
  anc <- nautilus:::read_wc(NA_character_, NA_character_)
  att <- nautilus:::attachAncillary(.wc_primary(), anc, align = alignmentControl())
  expect_named(att$ancillary, "alignment")
  expect_identical(att$ancillary$alignment$status, "abstained")
  expect_match(att$ancillary$alignment$reason, "no Wildlife Computers streams")
  expect_equal(att$ancillary$alignment$offset.seconds, 0)
})

test_that("attachAncillary() abstains without shifting when the evidence is weak", {
  # a flat primary depth trace has no dives to lock onto -> no trustworthy offset
  flat <- data.table::data.table(datetime = as.POSIXct(.wc_dives()$t, origin = "1970-01-01", tz = "UTC"),
                                 depth = 0)
  anc <- nautilus:::read_wc(.wc_locations(offset = 300), .wc_archive(offset = 300))
  t_before <- anc$positions$data$datetime
  att <- nautilus:::attachAncillary(flat, anc, align = alignmentControl())
  expect_identical(att$ancillary$alignment$status, "abstained")
  expect_equal(att$ancillary$alignment$offset.seconds, 0)
  expect_identical(att$ancillary$positions$data$datetime, t_before)   # stored, unshifted
})

test_that("attachAncillary() stores streams in the order meta expects, and never stores `kind`", {
  # `kind` is transport metadata for the dispatch; the stored record keeps the shape downstream reads
  # (processTagData()'s depth-drift correction consumes meta$ancillary$dry$data).
  anc <- nautilus:::read_wc(.wc_locations(), .wc_archive())
  att <- nautilus:::attachAncillary(.wc_primary(), anc, align = alignmentControl())
  expect_identical(names(att$ancillary), c("dry", "positions", "alignment"))
  expect_null(att$ancillary$dry$kind)
  expect_null(att$ancillary$positions$kind)
  expect_true(all(c("source", "data") %in% names(att$ancillary$positions)))
})

test_that("attachAncillary() honours alignmentControl(method = 'none') and says so", {
  delta <- 300
  anc <- nautilus:::read_wc(.wc_locations(offset = delta), .wc_archive(offset = delta))
  t_before <- anc$positions$data$datetime
  att <- nautilus:::attachAncillary(.wc_primary(), anc, align = alignmentControl(method = "none"))
  expect_identical(att$ancillary$alignment$status, "disabled")        # switched off, and recorded as such
  expect_identical(att$ancillary$positions$data$datetime, t_before)   # nothing moved
})

test_that("attachAncillary() rejects an unimplemented strategy rather than silently skipping alignment", {
  # a strategy that quietly stored streams with NO provenance record would repeat the bug above
  anc <- nautilus:::read_wc(.wc_locations(), .wc_archive())
  expect_error(nautilus:::attachAncillary(.wc_primary(), anc, align = alignmentControl(), strategy = "none"),
               "Unsupported alignment")
})
