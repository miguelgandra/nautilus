#' Cross-device clock-alignment settings for importTagData()
#'
#' @description
#' Bundles the settings for the temporal alignment [importTagData()] applies when a deployment pairs a
#' primary archival tag, recording depth and inertial data, with a separate Wildlife Computers tag
#' recording wet/dry state and Fastloc-GPS positions.
#'
#' The two devices keep independent clocks, which can disagree by anything from a few seconds to many
#' minutes. Nothing in either record reveals the offset on its own, yet it silently corrupts every step
#' that combines the streams: the depth zero-offset correction, and the position fixes that anchor a
#' dead-reckoned track.
#'
#' @details
#' The Wildlife Computers archive file records that tag's own depth, and often temperature, at a low
#' rate. Because depth is a physical quantity measured by *both* devices, cross-correlating the two
#' depth series recovers the offset directly: it is the lag at which they agree best. In real
#' deployments this peak is sharp, so the estimate is well determined. The streams carried on the
#' Wildlife Computers clock are then shifted onto the primary tag's timeline. The primary depth and
#' inertial stream is the reference and is never moved, and neither are the deployment and pop-up
#' positions, which come from the metadata table rather than from that clock.
#'
#' A single constant offset is estimated per deployment. Residual drift is negligible in practice - a
#' few seconds over a multi-day record - and dominated by the constant term.
#'
#' The correction abstains, shifting nothing and saying so, whenever the evidence is too weak to trust:
#' no shared depth channel, too little overlap between the records, a flat depth trace with no dives to
#' lock onto, or a peak correlation below `min.correlation`. The clock is never shifted silently, and
#' the estimated offset and its diagnostics are stored in the deployment's metadata.
#'
#' @param method How to align the clocks: `"depth-xcorr"` (default) cross-correlates the shared depth
#'   channel, and `"none"` disables alignment, keeping the Wildlife Computers streams on their own
#'   clock.
#' @param max.lag Largest absolute clock offset to search for, in seconds (default `3600`, one hour).
#'   This also acts as a sanity bound: a best lag landing on the edge of the search range is treated as
#'   unresolved and the correction abstains. Widen it only if you have reason to expect a larger
#'   disagreement.
#' @param min.overlap Minimum overlap between the two depth records, in minutes, required to attempt
#'   alignment (default `30`). Below this the correction abstains, because a short overlap can produce a
#'   convincing correlation peak at the wrong lag.
#' @param min.correlation Minimum peak correlation between the two depth traces, at the best lag, for
#'   the offset to be accepted (default `0.9`). Below this the profiles do not match well enough to trust
#'   the lag and the correction abstains. Lower it only for records whose depth traces are genuinely
#'   noisy, and check the stored diagnostics afterwards.
#'
#' @return A validated `nautilus_alignment` object for the `alignment` argument of [importTagData()].
#'
#' @seealso [importTagData()] for the function that consumes it; [depthDriftControl()] for the depth
#'   correction that depends on the alignment being right.
#'
#' @examples
#' alignmentControl(min.correlation = 0.95)   # stricter acceptance
#' alignmentControl(method = "none")          # disable clock alignment
#' @export
alignmentControl <- function(method = c("depth-xcorr", "none"),
                             max.lag = 3600,
                             min.overlap = 30,
                             min.correlation = 0.9) {
  method <- match.arg(method)
  .assert_number(max.lag, "alignment$max.lag", min = 1)
  .assert_number(min.overlap, "alignment$min.overlap", min = 0)
  .assert_number(min.correlation, "alignment$min.correlation", min = 0, max = 1)
  structure(list(method = method, max.lag = max.lag,
                 min.overlap = min.overlap, min.correlation = min.correlation),
            class = "nautilus_alignment")
}
