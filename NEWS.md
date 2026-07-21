# nautilus 0.1.0

First public release. `nautilus` provides an end-to-end pipeline for processing and analysing
high-resolution multi-sensor archival tag data (depth, temperature, tri-axial acceleration,
magnetometer and gyroscope), with optional integration of onboard camera video.

## Deployment metadata and import

* `qcDeploymentMetadata()` validates and normalises the deployment table before import, with
  `metadataColumns()` mapping arbitrary column names onto canonical roles and `updateBiometrics()`
  attaching passive animal traits.
* `importTagData()` reads each animal's multi-sensor CSVs, standardises sensor names and units, folds
  in Wildlife Computers location files, and stores everything as a `nautilus_tag` object (a `data.table`
  carrying a single consolidated metadata record; inspect it with `tagMetadata()` and
  `processingHistory()`).
* `buildTagData()` constructs the same `nautilus_tag` from sensor data already in memory -- for tag
  makes `importTagData()` does not read (e.g. Little Leonardo loggers), data exported from another tool,
  or simulated data. It synthesises timestamps from a start time and rate when the raw has no clock, and
  fully supports accelerometer-only tags (no magnetometer/gyroscope required).

## Cleaning, quality control and orientation

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

* `processTagData()` derives orientation (tilt-compensated compass or Madgwick fusion), kinematics,
  dynamic body acceleration and paddle-wheel swimming speed, applies the magnetometer calibration when
  trusted, and optionally down-samples; tuned via `orientationControl()`, `smoothingControl()`,
  `calibrationControl()` and `depthDriftControl()`. `imputePaddleCalibration()` fills gaps in paddle-wheel
  speed slopes.
* `calculateTailBeats()` estimates tail-beat frequency (peak-picking or wavelet), `extractFeatures()`
  computes windowed sensor features, and `getDielPhase()` classifies day / night / twilight.

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

* `summarizeTagData()`, `processingSummary()` and `qcIssues()` provide per-deployment overviews.
* `plotDepthProfiles()`, `plotTimeAtDepth()`, `plotDistributions()`, `plotDives()`, `plotTracks()` and
  the shared `plotTheme()` render the main diagnostic and result figures.

## Onboard camera video

* `getVideoMetadata()` extracts recording timestamps (from file names, with an OCR fallback for the
  on-screen clock via `ocrControl()`), `filterVideoPeriod()` restricts sensor data to filmed intervals,
  and `annotateData()` joins behavioural annotations. `launchVideo()`, `reencodeVideos()` and
  `renderOverlayVideo()` support review and sensor-overlay rendering.

## Notes

* The fine-tuned camera-tag OCR model (~11 MB) is **not** bundled: it is downloaded on first use and
  cached locally (`installCamOcrModel()`), falling back gracefully to Tesseract's generic model offline.
* The package has a deliberately light dependency footprint and no geospatial system-library
  requirements (no GDAL/GEOS/PROJ).
