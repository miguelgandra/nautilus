# Production orientation models

`renderOverlayVideo()` now uses the eleven approved species models directly.
The default is `orientation.model = "blue_shark"`. Each name selects the same
geometry, colours, markings, pivots and rig returned by `overlaySpeciesModel()`.

| Name | Species | Triangles | Motion |
| --- | --- | ---: | --- |
| `blue_shark` | Blue shark, *Prionace glauca* | 696 | Lateral |
| `whale_shark` | Whale shark, *Rhincodon typus* | 696 | Lateral |
| `mako` | Shortfin mako, *Isurus oxyrinchus* | 700 | Lateral |
| `hammerhead` | Smooth hammerhead, *Sphyrna zygaena* | 696 | Lateral |
| `manta` | Manta, *Mobula alfredi/birostris* | 696 | Flexible wings |
| `devil_ray` | Sicklefin devil ray, *Mobula tarapacana* | 696 | Flexible wings |
| `turtle` | Green sea turtle, *Chelonia mydas* | 690 | Rigid foreflippers |
| `dolphin` | Common dolphin, *Delphinus delphis* | 688 | Vertical |
| `whale` | Humpback whale, *Megaptera novaeangliae* | 696 | Vertical |
| `marlin` | Striped marlin, *Kajikia audax* | 696 | Lateral |
| `tuna` | Yellowfin tuna, *Thunnus albacares* | 700 | Lateral |

```r
overlaySpeciesCatalog()
renderOverlayVideo(video, data, output,
  video.start = camera_start,
  orientation.model = "whale_shark",
  animate = TRUE)
```

Animation remains optional. It uses synchronized frequency and amplitude
estimates, with the existing gliding and missing-frequency behavior. Rig
metadata determines deformation; paired caudal lobes/flukes move together.
Turtle foreflippers rotate rigidly at their shoulders. No telemetry column,
orientation convention or video timing is changed by species selection.

Custom `overlayModel()` geometry and `overlaySpeciesModel()` objects remain
supported. All species have indexed, closed components with named parts and
pivots. Countershading and sparse barycentric markings add no triangles; marks
follow deformed faces and are hidden when covered. No runtime GLB/image import
or new package dependency is required.

## Migration

The generic `shark`, `cetacean` and `fish` names are removed without aliases.
Use `blue_shark`, choose `dolphin` or `whale`, and choose an explicit fish species
such as `tuna` or `marlin`, respectively. Experimental wrapper functions are
removed; use `overlaySpeciesModel()` and `overlaySpeciesCatalog()`.

## Model interpretation

These are reference-guided schematic models. Approval records and component
metadata remain accessible through `model$metadata`. Regression fixtures retain
the approved geometry, appearance and complete rigs.

- Blue shark uses traced lateral and ventral outlines; anterior shape is inferred.
- Whale shark, manta and mako use neutral reference-asset sections, with mirrored
  paired parts. Whale-shark spots are illustrative, not an identification pattern.
- Smooth hammerhead adapts a scalloped-hammerhead reference with the median notch
  removed, retaining the approved smooth-hammerhead identity.
- Devil ray uses a dorsal drawing; depth, camber and lateral/anterior views are inferred.
- Turtle reconstructs the incomplete reference flipper by mirroring the intact
  side; its rigid flapping remains schematic.
- Common dolphin and humpback fit their supplied species assets. Mouth closure,
  surface lines and some embedded roots are simplified.
- Marlin is striped marlin, matching the supplied asset; the earlier blue-marlin
  identity is retired.
- Tuna fits a lateral illustration, retaining its long-finned form and 95.5%
  lateral silhouette overlap. Width is inferred as 0.76 times local body depth;
  dorsal and anterior views remain inferred. Small auxiliary keels are omitted.

The painter's algorithm retains visible facets and can show intersecting-root
seams. Motion is a frequency-driven schematic, not measured joint motion or
hydrodynamics. Sparse colour cells and markings prioritize recognition at inset size.

For equal-scale orthographic previews, source `preview-model-library.R` and call
`previewModelLibrary()`. For synchronized inset clips, source
`preview-overlay-model-animation.R` and supply your telemetry and model names.
