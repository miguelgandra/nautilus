#' Metric selection and window sizes for trackMetrics()
#'
#' @description
#' Selects which movement-path metrics [trackMetrics()] computes and the sizes of the successive windows
#' behind its temporal tortuosity columns, so the main call stays uncluttered.
#'
#' @param metrics Which metrics to compute: any of `"path_ratio"`, `"sinuosity"`, `"turning_angle"` and
#'   `"straightness"`, or `"all"` (the default). Narrow it when you only need one or two; the
#'   local-turning metrics are the more expensive to compute on a long track.
#' @param min.points The fewest valid positions a track needs before it is summarised at all; shorter
#'   tracks are skipped. Default `5`. Raise it if a handful of positions is not enough for the
#'   comparison you intend, since a two-point "path" is straight by construction.
#' @param hourly.window.h,daily.window.h The window lengths in hours behind the `Hourly_tortuosity` and
#'   `Daily_tortuosity` columns, each the mean path-to-displacement ratio over successive, non-overlapping
#'   windows of that length. Defaults `1` and `24`. Choose them to bracket the timescales your animal's behaviour
#'   actually switches on - a foraging bout and a diel cycle, say - rather than leaving them at values
#'   that fall between the two.
#'
#' @return A validated `nautilus_track_metrics` object for the `control` argument of [trackMetrics()].
#'
#' @seealso [trackMetrics()] for the function that consumes it.
#'
#' @examples
#' trackMetricsControl(metrics = c("path_ratio", "straightness"), min.points = 10)
#' @export
trackMetricsControl <- function(metrics = "all",
                                min.points = 5,
                                hourly.window.h = 1,
                                daily.window.h = 24) {
  available <- c("path_ratio", "sinuosity", "turning_angle", "straightness")
  if (!is.character(metrics) || !length(metrics))
    .abort("{.arg trackMetrics$metrics} must be a non-empty character vector.")
  bad <- setdiff(metrics, c(available, "all"))
  if (length(bad))
    .abort(c("{.arg trackMetrics$metrics} has invalid value{?s} {.val {bad}}.",
             "i" = "Valid values: {.val {c('all', available)}}."))
  .assert_count(min.points, "trackMetrics$min.points", min = 2L)
  .assert_number(hourly.window.h, "trackMetrics$hourly.window.h", min = 0)
  .assert_number(daily.window.h, "trackMetrics$daily.window.h", min = 0)
  if (hourly.window.h <= 0 || daily.window.h <= 0)
    .abort("{.arg trackMetrics$hourly.window.h} and {.arg trackMetrics$daily.window.h} must be > 0.")
  structure(list(metrics = metrics, min.points = as.integer(min.points),
                 hourly.window.h = hourly.window.h, daily.window.h = daily.window.h),
            class = "nautilus_track_metrics")
}
