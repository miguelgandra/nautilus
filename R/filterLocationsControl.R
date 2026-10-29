#' Tuning for the speed check in filterLocations()
#'
#' @description
#' Groups the tuning of the neighbour-consistency speed test used by [filterLocations()] into one
#' validated object. The threshold that matters most - the fastest speed you would believe - stays the
#' top-level `max.speed.kmh` argument of that function; this object governs only how the test is
#' applied.
#'
#' @param min.time.mins The shortest separation, in minutes, between two fixes for the speed implied
#'   between them to be trusted. Closer pairs are not judged, because a sub-threshold gap inflates the
#'   apparent speed unreliably: a metre of positional jitter over a few seconds looks like a huge
#'   speed. Default \code{0}; non-finite speeds, including those from zero-time intervals, are not
#'   judged. This filter does not remove duplicate timestamps. Raise it if your tag reports bursts
#'   of near-simultaneous fixes.
#' @param max.iterations The most removal passes to make. Each pass removes the single most egregious
#'   spike and recomputes speeds against the new neighbours, and the loop stops early once no fix is
#'   implausible. Default `50`. It is a runaway guard rather than a tuning knob; reaching it usually
#'   means the threshold is too tight for the data.
#' @param spike.angle An optional direction-reversal test, in degrees between 90 and 180, that
#'   supplements the speed test: an interior fix is also treated as a spike when the track's heading
#'   reverses by at least this much there *and* at least one adjoining segment exceeds
#'   `max.speed.kmh`. It can therefore flag a reversal with only one over-threshold segment, but does
#'   not flag reversals whose adjacent speeds are both below the threshold. \code{NULL} (default)
#'   disables it; choose the angle after inspecting representative tracks.
#'
#' @return A validated `nautilus_filter_locations` object for the `control` argument of
#'   [filterLocations()].
#'
#' @seealso [filterLocations()] for the function that consumes it.
#'
#' @examples
#' filterLocationsControl(min.time.mins = 2)     # ignore fix pairs less than 2 min apart
#' filterLocationsControl(spike.angle = 160)     # also flag sharp out-and-back spikes
#' @export
filterLocationsControl <- function(min.time.mins = 0,
                                   max.iterations = 50,
                                   spike.angle = NULL) {
  .assert_number(min.time.mins, "filterLocations$min.time.mins", min = 0)
  .assert_count(max.iterations, "filterLocations$max.iterations", min = 1L)
  .assert_number(spike.angle, "filterLocations$spike.angle", min = 90, max = 180, null_ok = TRUE)
  structure(list(min.time.mins = min.time.mins, max.iterations = as.integer(max.iterations),
                 spike.angle = spike.angle),
            class = "nautilus_filter_locations")
}
