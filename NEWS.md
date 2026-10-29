# nautilus 0.2.0

## Multiple vertical-velocity windows

* `smoothingControl(depth = c(5, 2.5, 10))` calculates multiple local depth slopes in one
  `processTagData()` run. The first window remains `vertical_velocity`, driving mounting corrections
  and downstream defaults; additional columns use names such as `vertical_velocity_2.5s` and
  `vertical_velocity_10s`. Scalar calls retain their existing columns and calculations.
* All estimates share the scalar velocity post-mean, native corrected depth, edge/gap handling,
  output binning and rounding. Other smoothing controls remain scalar. Multi-window vectors must
  contain unique finite positive numbers; scalar `NULL` and zero retain the minimum slope stencil.
* Processing records requested windows and nominal sample counts/spans by column. Cadence-limited
  requests warn once per batch while retaining the requested columns. Reprocessing replaces stale
  secondary velocity columns. Extra windows increase memory use and saved-file size.

## Check portability

* Orientation labels use ASCII Unicode escapes, retaining the same degree symbols in rendered output.
* Approved model fixtures permit at most 1e-12 absolute floating-point roundoff across platforms;
  topology, types, dimensions and labels remain exact, and approved fixture data are unchanged.
  Equivalent shared-edge markings are compared by their global vertex weights, preserving their
  position throughout deformation. Attribute storage order follows R's set comparison semantics.
  Gallery-axis checks use the same tight tolerance.
* The paddle-speed extrapolation column is declared for data.table static checks, and CI checkout
  uses the Node24 runtime.

## Production species models

* The renderer now defaults to `"blue_shark"` and resolves all eleven approved
  species names through the primary production registry. Named models and
  `overlaySpeciesModel()` objects use identical geometry, appearance and rigs.
* Generic `"shark"`, `"cetacean"` and `"fish"` identifiers and the experimental
  wrapper functions are removed without aliases. Choose explicit species names;
  `"marlin"` identifies striped marlin and `"whale"` identifies humpback whale.
* Approved regression fixtures protect 688--700 triangle models, named parts,
  pivots, countershading and mesh-attached markings. Animation stays optional;
  synchronization, custom geometry and existing motion controls are retained.
* Temporary review artifacts, intermediate meshes, rendered samples and benchmark
  logs are removed. Production examples and approved regression fixtures remain.

## Internal organization

* Shared 3D geometry, rigging and rendering functions are consolidated in
  `utils-3d-engine.R`; species definitions and numerical data are grouped in
  `utils-3d-models.R`. Other private helpers are grouped by their processing domain.
* Primary exported functions have function-named source files. The exported
  `tailBeatColumn()` helper now lives with the tail-beat utilities, retaining its
  arguments, help page and behavior.
* Sixteen control constructors and their shared coercion helper now live in
  `controls.R`. Circling, dive-shape, wavelet, deployment-metadata and exclusion
  helpers join the event, dive, tail-beat, metadata and pipeline modules respectively.
  This reduces `R/` from 101 files to 80, with unchanged exports, calculations,
  control defaults and 3D model data.
* Model objects retain species identity, axes, named components and region maps.
  Development revisions, review records, fitting descriptions, reporting-only
  geometry/rig copies and the catalog's development-phase column are removed.
  Preview labels no longer carry development annotations. Geometry, colours,
  animation calculations and production rendering are unchanged.

# nautilus 0.1.0

First public release. `nautilus` provides an end-to-end pipeline for processing and analysing
high-resolution multi-sensor archival tag data (depth, temperature, tri-axial acceleration,
magnetometer and gyroscope), with optional integration of onboard camera video.

## Vertical velocity

* Processing, axis mapping, validation overlays and dive analysis now share one centred local
  linear depth-rate estimator. `processTagData()` fits directly to drift-corrected depth instead of
  taking central differences of boxcar-smoothed depth; the stored depth channel remains unsmoothed.
* The default processing and mapping slope window is now 5 seconds (previously a 10-second depth
  boxcar), retaining the 1-second velocity post-mean. Existing explicit window settings are honoured.
  Dive analysis retains its duration/noise adaptations and omits velocity post-smoothing.
* Fits truncate at segment edges, require three observations and never cross missing depth/time,
  non-increasing timestamps or gaps longer than 1.5 sampling intervals. Missing centres remain `NA`;
  optional rate post-means require full windows. Overlapping calculation blocks preserve precision
  on long high-rate records. `NULL` or zero depth windows select the minimum three-point slope.
* Processing history identifies the `local_linear` estimator and records the existing window controls.
  Reprocess source records consistently before combining results from the old and new estimators.

## Behavioural feature extraction

* `extractFeatures()` supports independent `step.size` (seconds), explicit contiguous segments via
  `segment.col`, and scalar or named per-channel `min.coverage` thresholds. All scales share output
  anchors, even-width alignment and segment boundaries; timestamp gaps split segments automatically.
* Circular IQR is rotationally invariant; heading changes and circling use shortest angular steps.
  Angular locations use vector means during output downsampling. Roll continuity is respected in
  movement derivatives, posture stability and activity. Heading autocorrelation recovers after gaps
  and preserves missing samples in their lag positions.
* Empty or insufficiently covered channels return `NA` for every metric, including energy and extrema.
  Entirely unlabelled response windows stay `NA`, including with `response.aggregation = "any"` and
  downsampling. Surface shape defaults cannot overwrite missing streams or coverage failures.
* Kinematic derivatives use seconds: activity and mean depth-change rate are per second, movement jerk
  is the RMS first derivative, and movement smoothness is the RMS second derivative. Depth-change
  rate is now windowed. These numerical corrections require retraining models fitted to old features.
  Existing argument positions, default cadence, column naming and complete-case defaults are preserved.

## Deployment metadata and import

* Deployment identity is now checked across row IDs, constructor arguments, metadata aliases and stored
  tag metadata. Explicit IDs no longer silently relabel existing data; mixed, missing and conflicting
  row IDs fail construction. Character keys preserve case and leading zeros while normalizing whitespace.
* Constructors, deployment QC and imports share metadata value normalization. Factor coordinates use
  their labels; malformed numeric text, non-finite or out-of-range positions, incomplete coordinate pairs
  and impossible event ordering are rejected at ingestion or reported by QC. Named metadata lists require
  scalar values. Sensor-only construction still permits absent deployment metadata.
* Imports consume the normalized values and recheck current metadata even with a saved QC stamp.
  Recovery time is retained separately from pop-up time, survives saved objects and processing, and is
  available through `summarizeTagData(metadata = "recovery_datetime")` (also included in `"all"`).

* POSIXct timestamps and recording starts now preserve their absolute instants when the display
  timezone changes. This deliberately replaces the constructor's former implicit wall-clock
  reinterpretation. Use `buildTagData(time.mode = "clock")` for an explicit reinterpretation, with
  its input zone, source zone and shift range recorded separately in sensor clock metadata.
* Existing timestamp columns take precedence over `start` and `sampling.rate`, as documented;
  redundant generation arguments warn and are ignored. Fixed-rate samples are generated from one
  resolved start using elapsed seconds, preserving cadence and row order across DST transitions.
* Raw imports honor ISO timestamp offsets, accept fixed logger UTC offsets, and reject partial parses
  and unresolved DST clock readings. Repeated times can be resolved with `ambiguous`; nonexistent times
  fail import. The historical 0.0001-second timestamp adjustment has been removed. Invalid selected
  sensor fragments fail their deployment rather than silently discarding samples. Legacy saved times
  are not rewritten, and video-clock correction keeps source clocks separate from display zones.
* `checkDeploymentMetadata()` validates and normalises the deployment table before import, with
  `metadataColumns()` mapping arbitrary column names onto canonical roles and selecting passive traits.
* `getTagMetadata()` replaces `tagMetadata()` and returns an independent metadata list with a compact
  print method. It accepts objects, `.rds` paths, and collections. `updateTagMetadata()` replaces
  `updateBiometrics()`, merging safe descriptive edits or matching a corrections table by deployment ID.
  Processing state and computational inputs remain protected; manual edits do not alter processing history.
* `importTagData()` reads each animal's multi-sensor CSVs, standardises sensor names and units, folds
  in Wildlife Computers location files, and stores everything as a `nautilus_tag` object (a `data.table`
  carrying a single consolidated metadata record; inspect it with `getTagMetadata()` and
  `processingHistory()`). Metadata deployments without a matching raw-data folder are now reported and
  recorded as exclusions by default; `missing.deployments = "ignore"` supports intentional partial runs.
* Updates to the shared exclusions log are deployment-scoped. A partial rerun refreshes current-state
  rows only for the deployments it evaluated and cannot erase exclusions belonging to the rest of the
  cohort.
* `buildTagData()` constructs the same `nautilus_tag` from sensor data already in memory -- for tag
  makes `importTagData()` does not read (e.g. Little Leonardo loggers), data exported from another tool,
  or simulated data. It synthesises timestamps from a start time and rate when the raw has no clock, and
  fully supports accelerometer-only tags (no magnetometer/gyroscope required).

## Cleaning, quality control and orientation

* Public processing and diagnostic functions preserve caller-owned tables, classes and nested
  metadata. Mutable working tables are acquired per deployment, keeping aliased batch entries
  independent while avoiding redundant copies of fresh file reads. Returned datasets are independent
  of their inputs, including no-op results and video subsets with nested metadata.
* Report-only `checkSensorIntegrity()` and `checkSensorQuality()` runs leave processing history
  unchanged. `apply = TRUE` records curation only in the returned or explicitly saved dataset.
* `filterDeploymentData()` trims each record to the on-animal deployment period and
  `regularizeTimeSeries()` places the data on a regular time grid.
* `checkSensorIntegrity()` and `checkSensorQuality()` detect and optionally repair sensor faults
  (duplication, dead channels, spikes/anomalies), tuned via `integrityControl()` / `anomalyControl()`;
  `filterLocations()` then screens the location channel, removing implausible GPS/Argos fixes with a
  neighbour-consistency (root) speed test, satellite-count and gross-distance checks
  (`filterLocationsControl()`).
* `checkTagMapping()`, `consensusAxisMapping()`, `reviewTagMapping()` and `applyAxisMapping()` determine
  and apply the signed-permutation transform that rotates each tag's IMU axes into the animal's body
  frame, including a per-package consensus that can rescue weakly-observed deployments.

## Magnetometer calibration

* `calibrateMagnetometer()` estimates the hard-iron centre and soft-iron matrix from free-swimming data,
  with per-package pooling and support for external calibration recordings (`magCalibrationControl()`).
* Because a level-swimming animal samples only a thin band of orientations, the fit is under-determined:
  the calibration returns an explicit, honest heading-confidence flag and, for thin bands, a
  hard-iron-only correction whose trust is judged by in-plane rotational coverage rather than the
  (heading-irrelevant) vertical dip.

## Processing, kinematics and behaviour

* `detectCircling()` returns candidate sustained heading-rotation events with explicit net-rotation,
  directional-consistency, angular-rate and posture controls. It uses wrapped heading differences,
  elapsed-time smoothing and gap-separated unwrapping, without changing sensor data or excluding
  deployments. Results retain assessment intervals and distinguish unassessable records from zero events.
* `plotDepthProfiles(events = ...)` and `plotTracks(events = ...)` display generic event tables as
  exact-time bands and reconstructed path highlights. Short event paths are gathered before background
  thinning and are broken at missing coordinates or long gaps. A constant depth-plot colour channel
  now uses a uniform trace instead of failing in the colour legend.
* `annotateData()` accepts `.rds` paths and detector results, including valid zero-event tables.
  `assessed.intervals` (automatically read from detector provenance where available) provides `1` for
  detected events, `0` for assessed non-event samples and `NA` for unavailable evidence. Manual scoring
  retains its existing default convention. Interval unions avoid repeated full-record scans, and
  caller-owned data tables are copied before annotation.
* `calculatePaddleSpeed()` no longer censors speeds above 10 km/h by default. Its `max.speed` argument
  now defaults to `NULL`; an upper cutoff is applied only when supplied explicitly.
* `processTagData()` derives orientation (tilt-compensated compass or Madgwick fusion), kinematics,
  dynamic body acceleration and paddle-wheel swimming speed, applies the magnetometer calibration when
  trusted, and optionally down-samples; tuned via `orientationControl()`, `smoothingControl()`,
  `calibrationControl()` and `depthDriftControl()`. `imputePaddleCalibration()` fills gaps in paddle-wheel
  speed slopes.
* `calculateTailBeats()` estimates tail-beat frequency (peak-picking or wavelet), `extractFeatures()`
  computes windowed sensor features, and `getDielPhase()` classifies day / night / twilight.
* Tail-beat cross-check agreement now uses the larger of a 10% relative tolerance and a 0.03 Hz
  absolute tolerance. `agreement.rel.tol` and `agreement.abs.tol.Hz` make the QC convention explicit
  and configurable; the processing history records both tolerances and paired-estimate coverage.
* `summarizeTagData(tbf.method = ...)` now retains deployments where neither tail-beat backend produced
  an estimate, reporting `NA` tail-beat metrics without losing valid depth or temperature summaries.

## Dive analysis

* `detectDives()` annotates every sample with the dive it belongs to (`dive_id`, `dive_phase`,
  `depth_baseline`), detecting excursions by two-threshold hysteresis with a prominence criterion. One
  definition serves air-breathers, fish that never surface and benthic resters because `diveControl()`
  makes the reference level (`"surface"`, `"baseline"` or `"auto"`) and the excursion direction explicit
  choices rather than hidden assumptions.
* `diveMetrics()` reduces the annotated record to one row per dive -- timing, depth, phase structure and
  kinematics -- with a quality block saying what each row can support: `censoring` names why a dive is
  incomplete, and `inter_dive_censored` asks about the interval itself, so two otherwise complete dives
  separated by a sensor blackout are not read as one long surface interval. Any per-sample channel can be
  summarised per dive (optionally per phase) through `variables`, with circular handling for headings
  and roll.
* `diveMetrics(shape = diveShapeControl())` optionally classifies complete, resolved depth profiles as
  V, U, W or other using normalised profile broadness and prominent internal excursions. The output
  includes geometric descriptors and diagnostic statuses; censored or unsupported profiles remain
  unclassified. Shape controls are explicit, taxon-agnostic heuristics and do not change dive detection
  or phase annotations.
* `diveShapeControl(min.excursion.amplitude = ...)` optionally withholds shape labels below a
  study-specific prepared profile height in metres. These dives retain all non-shape metrics and
  receive `NA` shapes with status `"below_min_amplitude"`; the default `NULL` preserves existing
  classifications. Eligibility is separate from dive detection and internal-peak prominence.
* `diveShapeControl(peak.scope = "bottom")` optionally restricts W classification to significant
  peaks and intervening valleys in one labelled bottom interval. Whole-dive broadness and amplitude
  are unchanged; unresolved phases abstain explicitly. The default `"profile"` remains phase-independent.
  `peak.prominence.cap` optionally caps the proportional criterion in metres, without lowering absolute
  or instrument-resolution floors. Shape provenance records algorithm version 2 and the selected scope.
* Vertical-rate phases now independently validate candidate bottoms using net change relative to a
  smoothed, resolution-filtered vertical path. `diveControl(bottom.max.directionality = 0.60)` is the
  default heuristic; `NULL` restores the unchecked rule. Directional intervals are refined while
  preserving a real plateau after a slow approach. Detection boundaries and source depth are unchanged;
  phase-specific metrics must be regenerated after rerunning `detectDives()`.
* `plotDives()` compares deployments on those metrics: every dive is a point in its deployment's column,
  with a median and interquartile marker over it, one panel per metric. It deliberately does not draw a
  bar of per-individual maxima -- a bar reads magnitude from a zero that does not exist when dives are
  measured from a running baseline, and a maximum grows with the number of dives recorded. For the same
  reason the default metric is `amplitude_m` (measured from each dive's own baseline) rather than the
  absolute `max_depth_m`.
* Dives excluded from a panel's statistics are still drawn, in outline, and reported in two separate
  counts: `n_censored` (the record failed) and `n_unsupported` (the dive's phase structure did not
  support that metric). Points beyond the `trim` quantile are pinned to the axis edge as open triangles
  and noted on the panel, never silently dropped.

## Movement reconstruction

* `reconstructTrack()` dead-reckons a 3-D pseudo-track from heading, speed and depth, anchored to known
  fixes by Verified Position Correction (`reconstructTrackControl()`). `crossValidateTrack()` measures
  accuracy against held-out GPS fixes, `findValidationSegments()` surfaces informative manoeuvres,
  `trackMetrics()` computes movement metrics, `plotTracks()` maps the fixes and the dead-reckoned track
  (with its `pseudo_error` uncertainty corridor and optional depth/speed colouring), and `exportForSSM()`
  prepares input for state-space models.

## Summaries and plots

* `summarizeTagData()`, `processingSummary()` and `issues()` provide per-deployment overviews.
* `plotDepthProfiles()`, `plotTimeAtDepth()`, `plotDistributions()`, `plotDives()`, `plotTracks()` and
  the shared `plotTheme()` render the main diagnostic and result figures.

## Onboard camera video

* `getVideoMetadata()` extracts recording timestamps (from file names, with an OCR fallback for the
  on-screen clock via `ocrControl()`). Deployment-level corrections can be supplied through
  `clock.corrections`; the applied seconds and their source are retained in the returned table.
  `getVideoClockCorrections()` derives only the unambiguous CATS case where sensor logging is explicitly
  UTC, the sensor object was imported in UTC, and the device clock has a non-zero offset. Clock
  comparisons now retain a one-second numerical tolerance while correctly detecting half-hour offsets.
* `filterVideoPeriod()` restricts sensor data to filmed intervals, and `annotateData()` joins behavioural
  annotations. `launchVideo()`, `reencodeVideos()` and `renderOverlayVideo()` support review and
  sensor-overlay rendering. `renderOverlayVideo()` now builds presentation dashboards from reusable
  metric modules, with a combined 3-D orientation/compass complication by default and separate dials as
  an alternative. It supports opaque beside-video panels and transparent left/right overlays that retain
  the source dimensions, plus compact, expanded pseudo-trajectory, lower-ribbon and focus layouts. VeDBA
  and signed vertical velocity have dedicated displays with clip-stable scales; the pseudo-trajectory is
  projected and bounded once per clip rather than recomputed with a moving camera every frame.
* `renderOverlayVideo()` now prepares model geometry and interpolated orientation once per render,
  with symmetric positive/negative pitch cues in the original rear view. Optional `animate = TRUE`
  (or `motion = overlayMotionControl(...)`) adds schematic tail/flipper motion and a matching TBF
  readout. `overlayModel()` supplies validated custom triangle geometry and motion weights. Models
  remain rigid by default; the existing positional arguments are preserved. `overlay.fps = NULL`
  uses 5 fps for rigid models and the source rate for animation. Existing outputs now require
  `overwrite = TRUE`; the source video cannot be overwritten. The experimental entry point has been
  consolidated into the production function.

## Notes

* The fine-tuned camera-tag OCR model (~11 MB) is **not** bundled: it is downloaded on first use and
  cached locally (`installCamOcrModel()`), falling back gracefully to Tesseract's generic model offline.
* The package has a deliberately light dependency footprint and no geospatial system-library
  requirements (no GDAL/GEOS/PROJ).
