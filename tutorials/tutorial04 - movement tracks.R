###############################################################################################
## Miguel Gandra || CCMAR || m3gandra@gmail.com ###############################################
## Tutorial 04: Movement Track Reconstruction with the 'nautilus' R Package ###################
###############################################################################################

# The other branch from Tutorial 01: reconstructing where the animals actually went. Satellite fixes
# locate a marine animal only when the tag breaks the surface, which for a whale shark may be hours
# or days apart - too coarse to see the path that the behaviour happened on. Dead reckoning fills the
# gaps from the tag's own sensors, and this tutorial walks the full workflow:
#
#   1.  Screen the position fixes, since they are what the reconstruction is anchored to.
#   2.  Choose where swimming speed comes from - the single biggest decision in the branch.
#   3.  Reconstruct the pseudo-track and correct it onto the known positions.
#   4.  Measure how accurate the result actually is, by holding out real fixes.
#   5.  Feed that measurement back into the settings, and re-run.
#   6.  Summarise the paths, and produce publication-quality maps.
#   7.  Hand the track to a state-space model for formal uncertainty.
#
# A word on what a pseudo-track is. It is a plausible reconstruction, not an observation - which is
# why its columns are named pseudo_lon and pseudo_lat. Small, systematic heading or speed errors
# accumulate, so the path drifts; the correction step pulls it back onto every fix, which means
# accuracy is highest AT the fixes and worst midway between two of them. Read a reconstructed track
# with that shape in mind, and quantify it with STEP 4 rather than assuming it.
#
# Notes:
#  - Assumes Tutorial 01 has been run: this script reads "./data interim/06_tailbeats".
#  - Create "./data interim/07_tracks", "./plots" and "./outputs" before running.
#  - The mapping steps use optional packages: 'maps'/'mapdata' for coastlines, 'maptiles' for
#    satellite imagery, 'marmap' for bathymetry. Each degrades gracefully if absent.

################################################################################
# STEP 0. Load the package and the processed data                              #
################################################################################

library(nautilus)

# The reconstruction needs the derived heading, pitch and speed channels, so it starts from the
# processed files. Tutorial 01's final output works directly.
processed_files <- list.files("./data interim/06_tailbeats", full.names = TRUE)

# Heading is the input the track's SHAPE depends on, so it is worth knowing which flavour you have.
# processTagData() records this per deployment: where a position was available it applied the local
# magnetic declination and the heading is geographic; where none was, the heading is left magnetic
# and a warning was raised. A magnetic heading is fine for turning angles and circular variance -
# a constant offset cancels - but it ROTATES a dead-reckoned track, so it matters here.
tag <- readRDS(processed_files[1])
tagMetadata(tag)$deployment$heading_reference

################################################################################
# STEP 1. Screen the position fixes                                            #
################################################################################

# The fixes are the skeleton the whole reconstruction hangs on: a spurious position drags the
# corrected track out to a place the animal never was, and the surrounding path with it. Screen them
# first, before anything consumes them.
#
# The speed check is the principled one. Flagging every over-threshold step does not work, because a
# fast segment implicates two fixes and cannot say which is wrong. nautilus uses the
# neighbour-consistency ("root") test: a fix is implausible only when the implied speed to BOTH its
# neighbours exceeds the threshold - an isolated spike the track jumps out to and back from. A
# genuinely fast stretch, fast to one neighbour and normal to the other, is kept. (The first and last
# fix are the exception, necessarily: an endpoint has only one neighbour.)
#
# All the checks are opt-in - with the defaults nothing is removed. Set max.speed.kmh from your
# species' plausible sustained travel speed, not its burst speed, since fixes may be hours apart.

filterLocations(data           = processed_files,
                max.speed.kmh  = 5,      # sustained; whale sharks cruise well below this
                min.satellites = 4,      # geometric minimum for a Fastloc-GPS position
                control        = filterLocationsControl(min.time.mins = 2,
                                                        spike.angle   = 160),
                plot           = FALSE,
                plot.file      = "./plots/location-screening.pdf",
                basemap        = "land",
                return.data    = FALSE,
                output.dir     = "./data interim/07_tracks",
                verbose        = "detailed")

track_input <- list.files("./data interim/07_tracks", full.names = TRUE)

# The diagnostic map colours every fix by outcome (kept, or removed and by which check), so a glance
# confirms the filter removed spikes rather than genuine excursions. Only automatically-acquired
# Fastloc and Argos fixes are ever removed; hand-curated "User" positions and the deploy/pop-up
# anchors are never touched.

################################################################################
# STEP 2. Choose where speed comes from                                        #
################################################################################

# Speed sets the SCALE of the track; heading sets its SHAPE. They fail independently, which is useful:
# a speed error stretches or shrinks the path without distorting its geometry.
#
# Four sources, in rough order of preference when available:
#
#   "paddle"     a mechanical paddle-wheel count, calibrated per tag. The most direct measurement,
#                and the one to use if the tag carried a wheel (see Tutorial 01, STEP 10).
#   "vedba"      an activity model: speed regressed on dynamic body acceleration. Needs a
#                species-specific calibration - vedba.model = c(intercept, slope).
#   "depth_rate" derived from dive geometry: vertical speed divided by the sine of pitch, valid only
#                on steep segments (depth.rate.min.pitch). Free, but only informative while diving.
#   "constant"   a single assumed speed. Crude, but honest and often adequate for the SHAPE.
#
# The correction method decides how the accumulated drift is reconciled with the fixes:
#
#   "error_weighted" (default) each fix pulls as hard as its quality warrants, so a coarse Argos
#                    point bends the track less than a precise Fastloc one. The sensible default.
#   "linear"         the track passes exactly through every fix.
#   "scale_rotate"   each reckoned segment is rescaled and rotated onto its bracketing fixes; better
#                    where drift is systematic rather than random.
#   "none"           returns the raw reckoning, which is mainly useful for seeing how far it drifted.

track_settings <- reconstructTrackControl(
  speed.method   = "paddle",           # switch to "vedba" or "constant" for tags without a wheel
  max.speed      = 2.5,                # m/s ceiling, guards against a spurious speed spike
  vpc.method     = "error_weighted",
  vpc.weighting  = "distance",         # spread drift by distance travelled, not by elapsed time
  drift.rate     = 0.5,                # m/s of reckoning error growth; refined in STEP 4
  include.depth  = TRUE,               # attach depth as the vertical axis, giving a 3-D track
  reconstructability.min = 0.1)

################################################################################
# STEP 3. Reconstruct the pseudo-tracks                                        #
################################################################################

# reconstructTrack() integrates heading and speed into a horizontal displacement, steps it forward on
# a sphere, attaches depth as the vertical axis, and reconciles the accumulated drift with the known
# positions. It adds pseudo_lon, pseudo_lat, speed_dr (the speed actually used), pseudo_error (the
# per-sample 1-sigma uncertainty in metres) and, with include.depth, pseudo_depth.

reconstructTrack(data          = track_input,
                 control       = track_settings,
                 plot          = FALSE,
                 plot.file     = "./plots/track-reconstruction.pdf",
                 force.plots   = FALSE,
                 return.data   = FALSE,
                 output.dir    = "./data interim/08_reconstructed",
                 verbose       = "detailed")

track_files <- list.files("./data interim/08_reconstructed", full.names = TRUE)

# Watch the console for the reconstructability warning. Where a deployment has only its two endpoints
# - the deployment and the pop-up - nothing constrains a wandering interior, and two very different
# paths can end in the same place. nautilus measures how directed the reckoned path is and flags the
# ones too undirected to support their own interior geometry. Those tracks are still returned; treat
# their middles with great caution and lean on the endpoints.

################################################################################
# STEP 4. Measure how accurate the reconstruction actually is                  #
################################################################################

# This is the headline validation, and the step most often skipped. A marine pseudo-track has no
# underwater ground truth - between surfacings there is nothing to compare against. But one thing can
# be done: withhold a fix the animal genuinely produced, reconstruct from the remaining anchors
# alone, and see how close the reconstruction lands to the point it never saw.
#
# crossValidateTrack() does that leave-one-out across every fix, hiding the withheld point from both
# the correction and the speed calibration. The error at a fix `t` seconds from the nearest retained
# one is a direct estimate of accuracy over a `t`-second reckoning gap.

cv <- crossValidateTrack(data      = track_input,
                         control   = track_settings,
                         plot      = FALSE,
                         plot.file = "./plots/track-cross-validation.pdf",
                         verbose   = "detailed")

# The key diagnostic is error against gap: plot one against the other and the shape tells you how the
# reconstruction degrades. Where the relationship is roughly linear, its slope is an empirical drift
# rate - which is the number reconstructTrackControl(drift.rate = ) wants, in m/s.
head(cv)
summary(cv$error_m)
plot(cv$gap_h, cv$error_m, xlab = "Gap to nearest retained fix (h)", ylab = "Held-out error (m)",
     pch = 16, col = "#4272CB")

# Read the interpolated and extrapolated points separately. An interpolated fix has retained anchors
# on both sides and is the easier case; an extrapolated one (typically the pop-up) has anchors on one
# side only, and is the honest test of how far a reckoning runs before it wanders.
aggregate(error_m ~ interpolated, cv, median)

# Comparing settings on YOUR data beats accepting a default. Stack the results and see which helps:
methods <- c("none", "linear", "error_weighted", "scale_rotate")
cv_compare <- do.call(rbind, lapply(methods, function(m)
  crossValidateTrack(track_input,
                     control = reconstructTrackControl(speed.method = "paddle", vpc.method = m),
                     verbose = FALSE)))
aggregate(error_m ~ vpc_method, cv_compare, median)

# Honest limits: the held-out error conflates heading error, speed error and unmodelled current, so
# it measures the pipeline's net accuracy rather than which component failed. It needs deployments
# that surfaced often enough to produce fixes beyond the deployment origin, so pool across the fleet
# before reading a rate off the slope. And a withheld fix carries its own error - tens of metres for
# Fastloc, kilometres for coarse Argos - which sets a floor on the achievable error_m.

################################################################################
# STEP 5. Feed the measurement back and re-run                                 #
################################################################################

# With an empirical drift rate in hand, the uncertainty corridor stops being a guess. Re-running is
# cheap and makes pseudo_error mean something specific to this dataset and this tag configuration.
#
# The median of the per-fix ratio (error divided by gap) is the robust estimate, and it is the same
# quantity crossValidateTrack() prints in its summary line. Prefer it to a regression slope unless the
# scatter above is clean and well spread across gap lengths - with few fixes, one long extrapolated
# gap can dominate a fitted line.

empirical_drift <- median(cv$error_m / (cv$gap_h * 3600), na.rm = TRUE)   # metres per second
cat(sprintf("Empirical drift rate: %.3f m/s\n", empirical_drift))

# Sanity-check it before adopting it: a rate far above the animal's own swimming speed usually means
# the fixes are too sparse to constrain the reckoning, not that the tag drifted that fast.

track_settings_tuned <- reconstructTrackControl(
  speed.method  = "paddle",
  max.speed     = 2.5,
  vpc.method    = "error_weighted",
  vpc.weighting = "distance",
  drift.rate    = empirical_drift,
  include.depth = TRUE)

reconstructTrack(data        = track_input,
                 control     = track_settings_tuned,
                 plot        = FALSE,
                 plot.file   = "./plots/track-reconstruction-tuned.pdf",
                 return.data = FALSE,
                 output.dir  = "./data interim/08_reconstructed",
                 verbose     = "detailed")

track_files <- list.files("./data interim/08_reconstructed", full.names = TRUE)

################################################################################
# STEP 6. Summarise the paths                                                  #
################################################################################

# trackMetrics() reduces each path to one row: how far the animal travelled, how far it actually got,
# and how convoluted the route was between the two. Tortuosity is a common proxy for behavioural
# mode - a straight transit and a tightly looping search look quite different even when they cover
# the same distance.
#
# The measures are complementary. Path_ratio and Straightness are global, comparing the route to the
# straight line from start to end, so they say nothing about WHERE the wandering happened. Sinuosity
# and Mean_turning_angle are driven by local turning and will separate two paths that share endpoints
# but not behaviour. Hourly_tortuosity and Daily_tortuosity sit between the two.

track_metrics <- trackMetrics(data    = track_files,
                              control = trackMetricsControl(metrics         = "all",
                                                            min.points      = 10,
                                                            hourly.window.h = 1,
                                                            daily.window.h  = 24),
                              verbose = "detailed")

track_metrics[, c("ID", "Total_distance_km", "Net_displacement_km", "Straightness")]

write.csv2(track_metrics, file = "./outputs/track_metrics.csv",
           row.names = FALSE, fileEncoding = "UTF-8")

################################################################################
# STEP 7. Maps                                                                 #
################################################################################

fig_theme <- plotTheme(preset = "light", cex = 1.05)

## 7.1 Fetch the basemap once ----------------------------------------------------------------------

# Asking a plotting function for imagery means a network fetch every time the figure is drawn: slow,
# broken offline, and dependent on a tile server still serving the same tiles - a poor foundation for
# a published map. getBasemap() does the fetch once for the extent of your deployments and hands back
# the raster; save it and the figure becomes reproducible.

basemap_raster <- getBasemap(data    = track_files,
                             type    = "satellite",
                             control = basemapControl(provider = "Esri.WorldImagery", cache = TRUE),
                             verbose = TRUE)

saveRDS(basemap_raster, "./outputs/basemap.rds")
# basemap_raster <- readRDS("./outputs/basemap.rds")   # reuse it thereafter, offline

# Without a network, or without 'maptiles', skip this step and pass basemap = "land" below: a filled
# vector coastline over a flat sea, which needs no download and is often the cleaner figure anyway.

## 7.2 The main figure: tracks coloured by depth ---------------------------------------------------

# One map per deployment, showing the genuine surface fixes, the deployment and pop-up anchors, and
# the reconstructed track between them. Colouring by depth turns the map into a picture of where in
# the water column the animal was as it moved - the 3-D track seen from above.
#
# show.uncertainty draws the pseudo_error corridor around the path, so a stretch close to a fix is
# visibly tighter than one that has drifted mid-gap. It is the honest way to present a pseudo-track:
# the reconstruction never gets more visual weight than the fixes it interpolates between.

plotTracks(data             = track_files,
           color.by         = "depth",          # or "speed"; NULL for a single colour
           show.uncertainty = TRUE,
           basemap          = basemap_raster,   # the pre-fetched raster from 7.1
           coastline        = "high",           # needs 'mapdata'; "auto" picks the best installed
           theme            = fig_theme,
           max.points       = 5000,
           ncols            = 2,
           nrows            = 3,
           plot             = FALSE,
           plot.file        = "./plots/fig1-tracks-by-depth.pdf")

## 7.3 A bathymetric variant -----------------------------------------------------------------------

# For a habitat-use figure, the depth relief usually says more than imagery: it shows whether the
# animal followed the shelf break, crossed a canyon, or stayed over flat ground. The relief canvas
# and the isobath contours are separate layers over one grid, so they compose freely - and asking for
# both costs a single download.

plotTracks(data             = track_files,
           color.by         = "speed",
           show.uncertainty = FALSE,            # keep the canvas readable under the contours
           basemap          = "bathymetry",
           bathy.contours   = c(-50, -200, -1000, -2000),
           coastline        = "auto",
           theme            = fig_theme,
           ncols            = 2,
           nrows            = 3,
           plot             = FALSE,
           plot.file        = "./plots/fig2-tracks-bathymetry.pdf")

################################################################################
# STEP 8. Hand off to a state-space model                                      #
################################################################################

# A pseudo-track is dense and plausible, but it is a reconstruction rather than a set of observations
# with a formal error model. Analyses that need credible intervals - a utilisation distribution, a
# behavioural state estimate - require one, and the community-standard tools for that are aniMotum
# and crawl. nautilus deliberately does not re-implement a state-space smoother; it formats the track
# so those packages can take it.

ssm_input <- exportForSSM(data         = track_files,
                          lon.col      = "pseudo_lon",
                          lat.col      = "pseudo_lat",
                          error.col    = "pseudo_error",   # the per-position 1-sigma error, in metres
                          thin.minutes = 10,               # a dense track is heavy and redundant here
                          verbose      = "detailed")

head(ssm_input)
write.csv2(ssm_input, file = "./outputs/ssm_input.csv",
           row.names = FALSE, fileEncoding = "UTF-8")

# Then, with aniMotum installed:
# fit <- aniMotum::fit_ssm(ssm_input, model = "crw", time.step = 2)   # a 2-hour regularised track
# plot(fit, type = 2)


###############################################################################################
# That completes the movement branch: screened fixes, a speed source chosen deliberately, a
# reconstruction whose accuracy has been measured rather than assumed, path summaries, and two
# map figures ready for a manuscript.
#
# The habit worth keeping is STEP 4. A pseudo-track always looks convincing - it is smooth,
# dense and plausible - and the only way to know how far to trust it is to hold out a fix the
# animal really produced and see where the reconstruction puts it.
###############################################################################################
