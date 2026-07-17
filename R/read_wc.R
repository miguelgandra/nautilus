#######################################################################################################
# read_wc() / attachAncillary(): the ancillary axis ###################################################
#######################################################################################################
#
# The SECOND axis of ingestion, orthogonal to the primary-format readers (read_cats / read_little_leonardo).
#
# A co-deployed Wildlife Computers tag is not a "format" of the primary logger - it is a separate device,
# with its own clock, contributing supporting streams (position fixes, a wet/dry signal). Folding it into
# the primary reader would couple every format to every ancillary source (a CATS+WC reader, an LL+WC
# reader, a CATS+acoustic reader...), which is exactly the combinatorial explosion the reader seam exists
# to prevent. So it gets its own reader, and attaches to an ALREADY-BUILT tag:
#
#     read_wc(files)  ->  ancillary records  ->  attachAncillary(tag, anc, align = ...)  ->  tag
#
# attachAncillary() is the single home for clock alignment. Adding another ancillary source (an acoustic
# receiver, a light logger) means a new reader plus, usually, an existing align strategy - and no change
# to any primary reader, to buildTagData(), or to the alignment logic itself.
#
# Layout, matching read_cats(): this file holds the SEAM - the reader entry point and the attach - while the
# low-level parsing/alignment helpers it calls (.readPositionsAncillary(), .readDryAncillary(),
# .alignWCClocks(), .estimateClockOffset()) stay beside their CATS counterparts in importTagData.R.
#
# `kind` is TRANSPORT metadata used to dispatch the attach; it is deliberately NOT stored. What lands in
# `meta$ancillary$<name>` keeps the exact shape downstream already reads (processTagData()'s depth-drift
# correction consumes `meta$ancillary$dry$data`), so this extraction changes no stored contract.


#' Validate an in-flight ancillary record.
#'
#' A lightweight typed list rather than an S3 class: dispatch is a `switch` over a handful of kinds,
#' validation is this function, and the record is internal plumbing between a reader and the attach - so a
#' class would add ceremony to the object model without buying anything (see DESIGN_INGESTION.md 8).
#' @param x A list with `kind` ("positions" / "events" / "channel"), `source`, and `data`.
#' @keywords internal
#' @noRd
.validateAncillary <- function(x, arg = "ancillary") {
  if (!is.list(x) || is.null(x$kind) || is.null(x$data)) {
    .abort("{.arg {arg}} must be a list with at least {.field kind} and {.field data}.")
  }
  if (!x$kind %in% c("positions", "events", "channel")) {
    .abort(c("Unknown ancillary kind {.val {x$kind}}.",
             "i" = "Supported: {.val {c('positions', 'events', 'channel')}}."))
  }
  invisible(TRUE)
}


#' Read a co-deployed Wildlife Computers tag's ancillary streams
#'
#' @param locations_file Path to a WC `...Locations.csv` (or NA / nonexistent).
#' @param archive_file Path to a WC `...-Archive.csv` (or NA / nonexistent); supplies the wet/dry signal
#'   and the shared depth channel the clock alignment cross-correlates against.
#' @return A list of typed ancillary records (possibly empty): `positions` (kind "positions" - the COMPLETE
#'   fix history at its own cadence, never snapped to sensor rows) and `dry` (kind "events" -
#'   transition-encoded wet/dry). `archive_file` rides along for the aligner.
#' @keywords internal
#' @noRd
read_wc <- function(locations_file, archive_file) {
  # tag the record by assignment, not `c(list(kind = ...), rec)`: prepending via c() silently drops the
  # record's attributes (and class), so a reader that ever attaches one would lose it here without a word.
  tag_kind <- function(rec, kind) { if (is.null(rec)) return(NULL); rec$kind <- kind; rec }

  out <- list()
  pos <- .readPositionsAncillary(locations_file)
  if (!is.null(pos)) out$positions <- tag_kind(pos, "positions")
  dry <- .readDryAncillary(archive_file)
  if (!is.null(dry)) out$dry <- tag_kind(dry, "events")
  for (nm in names(out)) .validateAncillary(out[[nm]], nm)
  attr(out, "archive_file") <- archive_file
  out
}


#' Align a co-deployed device's streams onto the primary tag's clock and attach them
#'
#' The one place clock alignment lives. A co-deployed tag runs its own clock, offset from the primary's by
#' anything from seconds to many minutes; that offset is invisible without a shared signal, yet it silently
#' corrupts every step that combines the streams (the depth zero-offset correction, dead-reckoning anchors).
#'
#' One strategy exists today: `"depth-xcorr"` (WC) cross-correlates the two devices' depth traces - the same
#' physical quantity logged by both - and shifts the WC-clock streams onto the primary timeline. It is
#' conservative: it abstains and shifts nothing when the evidence is weak, recording why.
#'
#' The argument is the seam for future sources whose clock is trustworthy enough to join as-is (GPS/UTC), or
#' that share some other signal. Anything else is an error rather than a silent pass-through: a strategy that
#' quietly stored streams WITHOUT a provenance record would repeat the exact bug this attach was written to
#' avoid. Implementing a new one means emitting an `info` record too - not skipping it.
#'
#' To disable alignment for a WC deployment, use the control (`alignmentControl(method = "none")`), which
#' still records that the aligner was switched off.
#' @param data The primary sensor frame (needs `datetime` and `depth`) - the alignment reference.
#' @param anc A list of ancillary records from `read_wc()` (or empty).
#' @param align A `nautilus_alignment` control (see \code{alignmentControl}).
#' @param strategy Alignment strategy; see Details.
#' @return A list: `ancillary` (a named list ready to merge into `meta$ancillary`, in the exact shape
#'   downstream reads - `kind` stripped) and `info` (the alignment provenance, for reporting).
#' @keywords internal
#' @noRd
attachAncillary <- function(data, anc, align, strategy = "depth-xcorr") {
  # NOTE: no early return on an empty `anc`. The aligner is run even when there are no streams to shift,
  # because it still returns a provenance record saying why it abstained - and that record is stored for
  # EVERY deployment, including those with no co-deployed device. Short-circuiting here silently dropped
  # meta$ancillary$alignment from every WC-less tag.
  positions_anc <- anc$positions
  dry_anc       <- anc$dry
  archive_file  <- attr(anc, "archive_file")

  # `kind` is transport-only: the aligner and the stored record both use the original shape
  strip <- function(x) { if (is.null(x)) return(NULL); x$kind <- NULL; x }
  positions_anc <- strip(positions_anc); dry_anc <- strip(dry_anc)

  if (!identical(strategy, "depth-xcorr")) {
    .abort(c("Unsupported alignment {.arg strategy}: {.val {strategy}}.",
             "i" = "Supported: {.val {'depth-xcorr'}}.",
             "i" = "To disable alignment, use {.code alignmentControl(method = \"none\")}."))
  }
  aln <- .alignWCClocks(data, positions_anc, dry_anc, archive_file, align)
  positions_anc <- aln$positions_anc
  dry_anc       <- aln$dry_anc
  info          <- aln$info

  out <- list()
  if (!is.null(dry_anc))       out$dry       <- dry_anc
  if (!is.null(positions_anc)) out$positions <- positions_anc
  if (!is.null(info))          out$alignment <- info
  list(ancillary = out, info = info)
}
