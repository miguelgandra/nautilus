#######################################################################################################
# getBasemap(): pre-fetch a raster basemap for reproducible / offline figures #########################
#######################################################################################################

#' Pre-fetch a raster basemap for a set of deployments
#'
#' @description Fetches a raster basemap - satellite imagery (via \pkg{maptiles}, reprojected to WGS84
#' lon/lat so it co-registers with the package's maps) or a bathymetric depth grid (via \pkg{marmap}) -
#' covering the geographic extent of `data`.
#' Pass the returned raster straight back to \code{\link{plotTracks}} or \code{\link{filterLocations}} as
#' `basemap = <raster>`: this turns a live network fetch into a saved, reproducible, offline asset - the
#' recommended pattern for publication figures (fetch once, `saveRDS()` it, reuse). It is the raster twin
#' of supplying your own `coastline`.
#'
#' @details The extent is the union of every deployment's surface fixes, dead-reckoned pseudo-track and
#' deploy/pop-up anchors (with a small margin), exactly as \code{\link{plotTracks}} frames its maps.
#' Tile zoom and the bathymetry grid resolution are both derived from that extent; provider and caching
#' are set via \code{\link{basemapControl}} (imagery only). Satellite needs \pkg{maptiles}, \pkg{terra}
#' and \pkg{sf}; bathymetry needs \pkg{marmap}. Both are network downloads.
#'
#' @param data A `nautilus_tag`, a list of them, an aggregated data.frame, or `.rds` file paths - the same
#'   `data` you would pass to \code{\link{plotTracks}}.
#' @param type Character. The basemap kind: `"satellite"` (imagery tiles, via \pkg{maptiles}) or
#'   `"bathymetry"` (a depth grid, via \pkg{marmap}). Both are returned ready to pass straight back as
#'   `basemap =`.
#' @param control A \code{\link{basemapControl}} object (provider + cache).
#' @param id.col,datetime.col Column names, matching \code{\link{plotTracks}}. Defaults `"ID"`/`"datetime"`.
#' @param verbose Logical/character verbosity, as elsewhere. Default `TRUE`.
#' @return For `type = "satellite"`, a \pkg{terra} `SpatRaster` (RGB, lon/lat) carrying a
#'   `nautilus.credit` attribute with the provider attribution; for `type = "bathymetry"`, a \pkg{marmap}
#'   `bathy` depth grid. Either is ready to pass as `plotTracks(..., basemap = <this>)`.
#' @seealso \code{\link{plotTracks}}, \code{\link{basemapControl}}
#' @examples
#' \donttest{
#' # requires maptiles + internet:
#' # sat <- getBasemap(tag, type = "satellite")
#' # saveRDS(sat, "azores_imagery.rds")           # reuse offline
#' # plotTracks(tag, basemap = sat)
#' # bat <- getBasemap(tag, type = "bathymetry")  # a marmap depth grid
#' # plotTracks(tag, basemap = bat, bathy.contours = TRUE)
#' }
#' @export
getBasemap <- function(data, type = c("satellite", "bathymetry"), control = basemapControl(),
                       id.col = "ID", datetime.col = "datetime", verbose = TRUE) {
  lvl  <- .verbosity(verbose)
  type <- match.arg(type)
  if (identical(type, "bathymetry") && !requireNamespace("marmap", quietly = TRUE))
    .abort(c("{.arg type = \"bathymetry\"} needs the {.pkg marmap} package.",
             "i" = "Install it with {.code install.packages(\"marmap\")}."))
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

  # imagery -> a terra SpatRaster (reprojected to lon/lat); depth -> a marmap bathy grid. Both are
  # accepted straight back by `basemap =`, so either can be saved and reused offline.
  out <- if (identical(type, "satellite")) .fetchTiles(ext$xlim, ext$ylim, control, lvl)
         else .fetchBathy(ext$xlim, ext$ylim, lvl)
  if (is.null(out))
    .abort("Could not fetch a {type} basemap for the data extent (network or provider unavailable).")
  if (lvl >= 1L) .log_info(lvl, sprintf("fetched %s basemap (%s)", type,
                                        if (identical(type, "satellite"))
                                          attr(out, "nautilus.credit", exact = TRUE) %||% control$provider
                                        else "NOAA ETOPO via marmap"))
  out
}
