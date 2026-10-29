# Overlay animation and custom geometry

`renderOverlayVideo()` prepares indexed geometry and interpolated frame state once
before drawing. Schematic motion is optional, and custom untextured geometry can be
validated with `overlayModel()`. Video rendering uses optional **av** and FFmpeg;
no new package dependencies are required. Shared 3D implementation and species
data live in `utils-3d-engine.R` and `utils-3d-models.R`, respectively. Model-file
import and browser telemetry export are outside this interface.

The production renderer uses the eleven species models in `overlaySpeciesCatalog()`.
The default is `"blue_shark"`; pass a species name such as `"whale_shark"` or an
`overlaySpeciesModel()` object. Named parts, pivots, countershading and rigs are
shared by both routes. Generic `"shark"`, `"cetacean"` and `"fish"` names have been
retired without aliases. See [the species guide](model-library.md).

```r
renderOverlayVideo(video, tag, "/path/to/whale-shark.mp4",
  video.start = camera_start, start = clip_start, end = clip_end,
  orientation.model = overlaySpeciesModel("whale_shark"),
  animate = TRUE, overlay.fps = 20)
```

## Render a short interval

Load the development package in a fresh R session and supply your own footage,
processed deployment and timing metadata.

```r
# From the nautilus package checkout:
pkgload::load_all(".")

# Set these to your actual footage, processed deployment and sensor-clock anchor.
video <- "/path/to/camera.mp4"
tag <- processed[["deployment_id"]]
video_start <- camera_metadata$start[1]
clip_start <- video_start + 30
clip_end <- clip_start + 15

# Prepared geometry and interpolated orientation, with motion still disabled.
renderOverlayVideo(
  video, tag, "/path/to/rigid.mp4",
  video.start = video_start, start = clip_start, end = clip_end,
  orientation.model = "blue_shark", overlay.fps = 30, codec = "h264"
)

# The same preparation with estimated-frequency schematic motion.
renderOverlayVideo(
  video, tag, "/path/to/animated.mp4",
  video.start = video_start, start = clip_start, end = clip_end,
  orientation.model = "blue_shark",
  motion = overlayMotionControl(method = "peaks"),
  codec = "h264"
)
```

When motion is enabled, the default `overlay.fps = NULL` uses the source video rate.
Otherwise it uses 5 fps. Use matching explicit rates for a controlled comparison.
The full dashboard currently redraws at that rate. Higher rates therefore take
longer and transparent composition can produce many temporary PNGs.

Built-in motion profiles are lateral bending for sharks, marlin and tuna,
vertical bending for dolphin and humpback whale, flexible wing motion for manta
and devil ray, and rigid shoulder-driven foreflipper motion for turtle. These
are schematic deformations of the selected silhouettes. Global attitude retains the production
body axes and pitch/roll signs; heading stays on its own compass.
The camera uses the original straight rear alignment, without an
oblique azimuth. A display-only pitch-zero rotation compensates the camera's
elevation before projection, preserving signed pitch cues without changing sensor
values or physical body rotations. Animated insets show `TBF: X.XX Hz` below H/P/R
using the interpolated frequency that drives the current pose, with matching font
size and weight. Unavailable or explicitly gliding intervals show `TBF: NA`.
The routine motion/backend annotation is omitted to keep the inset clear.

`animate = TRUE` is a shortcut for enabling the default motion control. It also
works alongside a custom `motion = overlayMotionControl(...)`. `animate = FALSE`
disables motion; omitting `animate` keeps the existing `motion` behaviour.

## Frequency, amplitude and missing estimates

`method = "auto"` chooses one backend for the entire supplied deployment, preferring
peaks with finite positive frequencies and otherwise using wavelet. Frequency and
amplitude always come from that same backend. Use `method = "wavelet"` for an
explicit comparison. The renderer does not combine methods or resolve disagreement.

The default amplitude reference is the 95th percentile of valid positive amplitudes
in the full supplied deployment, not the chosen video clip. An amplitude at that
reference maps to `max.angle = 20` degrees; larger values saturate. For reproducible
comparisons across deployments, pass `amplitude.reference` explicitly in the units
of the selected motion signal. This is a visual scaling convention, not a conversion
from sensor amplitude to anatomical angle or tail displacement.

Phase is the integral of the estimated frequency. It is not measured beat phase.
Keep the full deployment as `data` and restrict the render with `start`/`end` to
retain the same phase and amplitude reference across overlapping clips. Missing
frequency/amplitude and sensor intervals longer than `max.sensor.gap` (default one
second) break a motion segment. The model becomes neutral and is labelled as
unavailable; the next valid segment restarts at `phase.offset`. Only an explicit
`tbf_swimming = FALSE` is labelled as gliding. Unknown classification is allowed.

Motion is disabled in validation dashboards so it cannot obscure the axis cue.
Roll and heading follow their shortest angular arc during interpolation. Missing
orientation is labelled as unavailable. Scalar values retain the most recent sensor
observation, while the timestamp shows the actual frame time.

## Supply custom geometry

Use an N-by-3 numeric vertex matrix in the animal frame (x forward, y right, z down)
and a triangle matrix with one-based indices. Geometry is centred and uniformly
scaled once. Inputs are limited to 5000 vertices and 2000 triangles, with finite
coordinates, valid indices and non-degenerate faces. Consistent winding and simple
surfaces are expected; a painter's algorithm cannot resolve arbitrary intersections.

```r
vertices <- rbind(
  c( 1.0,  0.0,  0.0),
  c(-1.0,  0.3,  0.0),
  c(-1.0, -0.3,  0.0),
  c(-0.5,  0.0, -0.3)
)
faces <- rbind(c(1, 2, 3), c(1, 4, 2), c(1, 3, 4), c(2, 4, 3))

custom <- overlayModel(vertices, faces, name = "custom silhouette")

# This deliberately simple shape demonstrates the custom motion interface.
# Replace its geometry and weights with an appropriate species-specific schematic.
custom_moving <- overlayModel(
  vertices, faces, name = "custom lateral schematic",
  motion = "lateral", weights = c(0, 1, 1, .5), pivot = c(0, 0, 0)
)

# Pass either object as orientation.model to renderOverlayVideo().
# A moving model also needs motion = overlayMotionControl(...).
```

Weights are per vertex, between zero and one. Zero preserves the rest pose; larger
weights increase local bend and phase delay. Tail profiles rotate about the supplied
pivot. The paired-flipper profile uses a right-side pivot mirrored across y = 0 and
therefore requires geometry centred on that midline. These are bounded procedural
profiles, without textures, skeletal rigs or automatic anatomy inference.

Outputs refuse to overwrite existing files unless `overwrite = TRUE`.
The source video can never be the output path.
