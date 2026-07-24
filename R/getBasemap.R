#######################################################################################################
# getBasemap(): pre-fetch a raster basemap for reproducible / offline figures #########################
#######################################################################################################

#' Pre-fetch a raster basemap for a set of deployments
#'
#' @description Fetches a raster basemap (currently satellite imagery, via \pkg{maptiles}) covering the
#' geographic extent of `data`, reprojected to WGS84 lon/lat so it co-registers with the package's maps.
#' Pass the returned raster straight back to \code{\link{plotTracks}} or \code{\link{filterLocations}} as
#' `basemap = <raster>`: this turns a live network fetch into a saved, reproducible, offline asset - the
#' recommended pattern for publication figures (fetch once, `saveRDS()` it, reuse). It is the raster twin
#' of supplying your own `coastline`.
#'
#' @details The extent is the union of every deployment's surface fixes, dead-reckoned pseudo-track and
#' deploy/pop-up anchors (with a small margin), exactly as \code{\link{plotTracks}} frames its maps.
#' Tile zoom is derived from that extent; provider and caching are set via
#' \code{\link{basemapControl}}. Needs \pkg{maptiles}, \pkg{terra} and \pkg{sf}; a satellite basemap is a
#' network download.
#'
#' @param data A `nautilus_tag`, a list of them, an aggregated data.frame, or `.rds` file paths - the same
#'   `data` you would pass to \code{\link{plotTracks}}.
#' @param type Character. The basemap kind. Currently only `"satellite"` (imagery); `"bathymetry"` (a
#'   shaded-relief raster) is reserved for a later release.
#' @param control A \code{\link{basemapControl}} object (provider + cache).
#' @param id.col,datetime.col Column names, matching \code{\link{plotTracks}}. Defaults `"ID"`/`"datetime"`.
#' @param verbose Logical/character verbosity, as elsewhere. Default `TRUE`.
#' @return A \pkg{terra} `SpatRaster` (RGB, lon/lat) carrying a `nautilus.credit` attribute with the
#'   provider attribution. Ready to pass as `plotTracks(..., basemap = <this>)`.
#' @seealso \code{\link{plotTracks}}, \code{\link{basemapControl}}
#' @examples
#' \donttest{
#' # requires maptiles + internet:
#' # sat <- getBasemap(tag, type = "satellite")
#' # saveRDS(sat, "azores_imagery.rds")           # reuse offline
#' # plotTracks(tag, basemap = sat)
#' }
#' @export
getBasemap <- function(data, type = c("satellite", "bathymetry"), control = basemapControl(),
                       id.col = "ID", datetime.col = "datetime", verbose = TRUE) {
  lvl  <- .verbosity(verbose)
  type <- match.arg(type)
  if (identical(type, "bathymetry"))
    .abort(c("A shaded {.arg type = \"bathymetry\"} raster basemap is not available yet (reserved).",
             "i" = "For depth contours, set {.arg bathy.contours} on the map instead."))
  control <- .as_control(control, basemapControl, "nautilus_basemap", "control")
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")

  # gather lon/lat over every deployment: canonical fixes + pseudo-track + deploy/pop-up anchors
  src <- .resolveInput(data, id.col)
  xs <- numeric(0); ys <- numeric(0)
  for (i in seq_len(src$n)) {
    x   <- src$get(i)
    fx  <- .tagPositions(x)
    dep <- .getMeta(x)$deployment
    xs  <- c(xs, fx$lon, x[["pseudo_lon"]], dep$lon, dep$popup_lon)
    ys  <- c(ys, fx$lat, x[["pseudo_lat"]], dep$lat, dep$popup_lat)
  }
  ext <- .equalAspectExtent(xs[is.finite(xs)], ys[is.finite(ys)], f = 0.3)
  if (is.null(ext))
    .abort(c("No coordinates found in {.arg data} to size a basemap.",
             "i" = "A basemap needs surface fixes, a pseudo-track, or deploy/pop-up positions."))

  rast <- .fetchTiles(ext$xlim, ext$ylim, control, lvl)
  if (is.null(rast))
    .abort("Could not fetch a {type} basemap for the data extent (network or provider unavailable).")
  if (lvl >= 1L) .log_info(lvl, sprintf("fetched %s basemap (%s)", type,
                                        attr(rast, "nautilus.credit", exact = TRUE) %||% control$provider))
  rast
}
