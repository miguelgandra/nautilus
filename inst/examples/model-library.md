# Production orientation models

`renderOverlayVideo()` uses eleven species models. The default is
`orientation.model = "blue_shark"`. Each name selects the same geometry,
colours, markings, pivots and rig returned by `overlaySpeciesModel()`.

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

Animation is optional. It uses synchronized frequency and amplitude estimates,
with the existing gliding and missing-frequency behavior. The rig determines
deformation; paired caudal lobes/flukes move together. Turtle foreflippers
rotate rigidly at their shoulders. Species selection does not change telemetry,
orientation conventions or video timing.

Custom `overlayModel()` geometry and `overlaySpeciesModel()` objects are also
supported. Species models have indexed, closed components with named parts and
pivots. Countershading and barycentric markings add no triangles; markings follow
deformed faces and are hidden when covered. No runtime GLB/image import or new
package dependency is required.

## Model objects

The model's top-level geometry, rig and appearance hold its operational state.
`model$metadata` contains its identifier, label, taxon, axes, named components,
and vertex/face region maps for inspection and customization. Geometry sizes
can be read directly from `nrow(model$vertices)` and `length(model$faces)`.

Development revisions, review records, fitting descriptions, duplicated authored
rig reports and normalization summaries are omitted. The catalog reports the
species, mesh sizes and motion profile without a development-phase column.

```r
model <- overlaySpeciesModel("tuna")
model$rig
model$metadata$components
```

## Code organization

`R/utils-3d-engine.R` contains shared mesh construction and validation, rigging,
appearance, projection and drawing functions. `R/utils-3d-models.R` contains
species definitions, coordinates, fin controls, colour tables and marking data.
The numerical model data and rendering calculations are unchanged by this layout.

For equal-scale orthographic previews, source `preview-model-library.R` and call
`previewModelLibrary()`. For synchronized inset clips, source
`preview-overlay-model-animation.R` and supply your telemetry and model names.

## Migration

Use explicit species identifiers from the table. The generic `shark`, `cetacean`
and `fish` names and experimental wrappers are removed without aliases. Use
`overlaySpeciesModel()` and `overlaySpeciesCatalog()` for the model interface.

The painter's algorithm retains visible facets and can show intersecting-root
seams. Motion is a frequency-driven schematic rather than measured joint motion
or hydrodynamics. Colours and markings prioritize recognition at inset size.
