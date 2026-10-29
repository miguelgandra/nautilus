#' Tile-fetch settings for a satellite basemap
#'
#' @description
#' Tunes how satellite imagery is fetched and cached when [plotTracks()] or [filterLocations()] is asked
#' for `basemap = "satellite"`.
#'
#' It governs the automatic fetch only. A raster you pre-fetched yourself with [getBasemap()] and passed
#' in as `basemap` is drawn as given, and ignores everything here.
#'
#' @param provider Which tile provider to draw from. Default `"Esri.WorldImagery"`, which is satellite
#'   imagery; any provider \pkg{maptiles} knows will work, such as `"Esri.WorldTopoMap"` for a
#'   topographic canvas or `"OpenStreetMap"` for a street map. Providers differ in coverage and licence,
#'   so check the terms before publishing a figure drawn on one.
#' @param cache Whether to keep fetched tiles for reuse: `TRUE` (default) uses a persistent per-user
#'   cache, `FALSE` keeps them only for the session, and a directory path caches them there. Leave it on
#'   unless disk space is tight - it makes a redrawn figure instant and works offline.
#'
#' @details
#' Tile zoom and grid resolution are deliberately not exposed. Both are derived from the map extent, and
#' where you genuinely need exact control the better route is to pre-fetch a raster with [getBasemap()]
#' and pass it in, which also makes the figure reproducible.
#'
#' @return A validated `nautilus_basemap` object for the `basemap.control` argument of [plotTracks()] and
#'   [filterLocations()].
#'
#' @seealso [plotTracks()] and [filterLocations()] for the functions that consume it; [getBasemap()] for
#'   pre-fetching a raster instead.
#'
#' @examples
#' basemapControl(provider = "Esri.WorldTopoMap")
#' @export
#' @param zoom Tile zoom level, or `NULL` (default) to choose one from the extent and the size the
#'   basemap will be drawn at. The automatic choice targets roughly 1200 pixels across the panel -
#'   about 280 dpi in a [plotTracks()] map - bounded by a tile budget so a wide extent cannot silently
#'   issue thousands of requests; when the budget binds rather than the target, the detailed log says so.
#'
#'   This exists because the tile library's own default optimises for a cheap request, not a figure: it
#'   takes the largest zoom that still covers the area in four tiles. On a 54 x 68 km extent that is
#'   zoom 9 - 244 m per pixel, about 60 dpi on the page - which renders islands as blurred blobs.
#'
#'   Higher is not always better. A provider's imagery runs out at some zoom and it then serves empty
#'   tiles rather than an error: `Esri.WorldImagery` over the open Atlantic is complete to zoom 11 and
#'   about 5% black at zoom 12, where the coastline gains detail but the ocean loses it. An automatic
#'   choice is therefore checked for coverage and steps back down if the imagery is missing. A zoom you
#'   set explicitly is treated as a decision and is used as given.
basemapControl <- function(provider = "Esri.WorldImagery", cache = TRUE, zoom = NULL) {
  .assert_string(provider, "provider")
  .assert_count(zoom, "zoom", min = 0, null_ok = TRUE)
  if (!is.null(zoom) && zoom > 19)
    .abort("{.arg zoom} ({zoom}) is above the maximum tile zoom (19).")
  if (!(isTRUE(cache) || isFALSE(cache) || (is.character(cache) && length(cache) == 1L && !is.na(cache))))
    .abort("{.arg cache} must be {.code TRUE}, {.code FALSE}, or a single directory path.")
  structure(list(provider = provider, cache = cache, zoom = zoom), class = "nautilus_basemap")
}
