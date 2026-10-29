#' Depth zero-offset drift-correction settings for processTagData()
#'
#' @description
#' Bundles the settings for the depth zero-offset drift correction applied by \code{\link{processTagData}}.
#' Pressure sensors accumulate a slowly-varying zero offset over a deployment (mainly thermal), so an
#' animal at the surface gradually stops reading 0 m. The correction estimates that offset from
#' independent surface evidence and subtracts it; by default it never infers the surface from the depth
#' trace, and it abstains rather than invent a zero line when evidence is too sparse. An opt-in "shallow
#' mode" (`surface.evidence = "depth"`) can additionally infer surface intervals from the depth trace.
#'
#' @param method Correction method: `"surface"` (surface-anchored zero-offset correction, the default)
#'   or `"none"` (disable; depth is left untouched).
#' @param surface.evidence Character vector of the evidence sources used to locate surface references,
#'   any of `"dry"` (a wet/dry sensor's sustained dry intervals), `"gps"` (surface-implying position
#'   fixes - Fastloc-GPS or Argos - whose antenna must break the surface), and `"depth"` (an opt-in
#'   "shallow mode" that infers surface intervals from the depth trace itself; see the `surface.*`
#'   arguments). `"dry"` and `"gps"` are independent of the depth trace and are the safe default;
#'   `"depth"` is used only as a gap-filler where the independent sources are absent, and it assumes the
#'   shallowest sustained depth is the surface, so it is unsuitable for animals that rarely surface.
#'   Default uses `"dry"` and `"gps"`.
#' @param min.dry.duration Minimum duration (seconds) of a sustained dry interval for it to count as a
#'   surface anchor; briefer dry flips (spray, wave wash-over) are ignored. Default 3.
#' @param max.gap Maximum interval (hours) between consecutive surface anchors for the correction to be
#'   considered fully reliable. Samples inside a longer gap are still corrected (the offset is
#'   interpolated across it) but flagged low-confidence, and the step status becomes `"applied_with_gaps"`.
#'   Default 6.
#' @param min.anchors Minimum number of surface anchors for a time-varying correction. With exactly one
#'   anchor a single constant offset is applied; with none the correction abstains and depth is left
#'   untouched. Default 2.
#' @param surface.quantile,surface.band The surface level is estimated as the `surface.quantile` (0.05)
#'   quantile of depth over the deployment (the animal's shallowest sustained depth). Two uses: (1) in
#'   "shallow mode" (`surface.evidence = "depth"`), a sample counts as at-surface when its depth is within
#'   `surface.band` (2 m) of that estimate; (2) for ALL evidence types, an anchor is a valid zero-offset
#'   only when its depth reads within `surface.band` of the surface level - a "surface" fix that lands on a
#'   dive (reading tens of metres) is a mis-timed/mislabelled fix, not the sensor zero drift, and is
#'   rejected (else it would over-correct the depth above the surface). `surface.band` should exceed both
#'   the surface wave/noise amplitude and the expected drift magnitude.
#' @return A validated `nautilus_depth_drift` object for the `depth.drift` argument of \code{\link{processTagData}}.
#' @seealso \code{\link{processTagData}}, \code{\link{smoothingControl}}, \code{\link{calibrationControl}}
#' @examples
#' depthDriftControl(surface.evidence = "dry", max.gap = 12)
#' depthDriftControl(method = "none")   # disable drift correction
#' @export
depthDriftControl <- function(method = c("surface", "none"),
                              surface.evidence = c("dry", "gps"),
                              min.dry.duration = 3,
                              max.gap = 6,
                              min.anchors = 2,
                              surface.quantile = 0.05,
                              surface.band = 2) {
  method <- match.arg(method)
  valid_ev <- c("dry", "gps", "depth")
  bad <- setdiff(surface.evidence, valid_ev)
  if (length(bad))
    .abort(c("{.arg depth.drift$surface.evidence} has invalid value{?s} {.val {bad}}.",
             "i" = "Valid sources: {.val {valid_ev}}."))
  if (!length(surface.evidence)) .abort("{.arg depth.drift$surface.evidence} must name at least one source.")
  .assert_number(min.dry.duration, "depth.drift$min.dry.duration", min = 0)
  .assert_number(max.gap, "depth.drift$max.gap", min = 0)
  .assert_count(min.anchors, "depth.drift$min.anchors", min = 1L)
  .assert_number(surface.quantile, "depth.drift$surface.quantile", min = 0, max = 1)
  .assert_number(surface.band, "depth.drift$surface.band", min = 0)
  structure(list(method = method, surface.evidence = unique(surface.evidence),
                 min.dry.duration = min.dry.duration, max.gap = max.gap, min.anchors = min.anchors,
                 surface.quantile = surface.quantile, surface.band = surface.band),
            class = "nautilus_depth_drift")
}
