# nautilus <img src="inst/resources/nautilus-logo.png" align="right" width="110" alt="nautilus logo" />

<!-- badges: start -->
[![Project Status: WIP](https://img.shields.io/badge/status-WIP-orange)](https://www.repostatus.org/#wip)
[![Lifecycle: experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
[![R-CMD-check](https://github.com/miguelgandra/nautilus/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/miguelgandra/nautilus/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->
<!-- CRAN badge omitted until the package is published; www.r-pkg.org 500s for packages not yet on CRAN.
     Re-add on acceptance:
     [![CRAN status](https://www.r-pkg.org/badges/version/nautilus)](https://CRAN.R-project.org/package=nautilus) -->

**nautilus** is an R package for processing, quality-controlling and analysing
high-resolution archival biologging data. It provides a reproducible workflow that
transforms raw recordings from multi-sensor animal-borne tags into analysis-ready datasets
for behavioural, ecological and movement analyses.

Modern archival tags record multiple sensors simultaneously, often at tens or hundreds of
samples per second. However, the path from raw recordings to ecological insight is long:
data must be trimmed to the period when the tag was on the animal, screened for sensor
issues, corrected for tag orientation, and prepared before derived metrics can be reliably
interpreted. nautilus standardises this workflow in one place, while documenting the
processing steps required to transform raw sensor streams into consistent and reproducible
datasets.

The package imports depth, temperature, tri-axial acceleration, magnetometer and gyroscope
data from **CATS**, **CEiiA** and **Little Leonardo** archival loggers, while remaining
compatible with datasets already available in R. It can integrate positions and wet/dry
records from co-deployed **Wildlife Computers** satellite tags, align onboard camera
footage with sensor recordings, and combine multiple data sources into a unified workflow
for downstream analyses. nautilus was originally developed around CATS Diary and Camera
tags deployed in towed **PILOT** packages
([Fontes et al., 2022](https://doi.org/10.1186/s40317-022-00310-1)), but the workflow is
designed to be applicable to a broad range of archival biologging datasets.

The package is intended for researchers working with high-frequency biologging data,
including ecologists, movement ecologists and behavioural scientists analysing
animal-borne sensor records.

> **Status.** nautilus is under active development ahead of its first release. The public
> API is stabilising but may still change.

<br/>

## The workflow

Every deployment follows the same **core sequence** &mdash; prepare, clean, orient &mdash;
which converges on a single call, `processTagData()`, returning one self-describing
`nautilus_tag` object. From there the analysis **branches** according to your scientific
question, so you only run the parts you need.

<p align="center">
  <img src="man/figures/nautilus-pipeline.png" width="820"
       alt="The nautilus workflow. Deployment metadata and raw tag data, optionally joined by a co-deployed satellite tag, flow through three core stages: 1, prepare and import; 2, clean and quality-control; 3, orient and calibrate. A parallel video track prepares onboard footage and feeds the orientation review. The core stages converge on processTagData(), which returns an analysis-ready nautilus_tag object and then branches into five optional analyses: summaries and figures, dive analysis, behaviour and kinematics, movement tracks, and video and annotation. A utilities strip provides metadata and audit-trail access throughout." />
</p>

The [Getting Started guide](#learning-more) walks through this diagram end to end on a real
deployment.

<br/>

## Organising a project

nautilus does not enforce a specific project layout, but `importTagData()` expects the
raw files from each deployment to be organised in a consistent way: each deployment
should have its own folder, with files grouped by tag or data source. The structure
below is the one used throughout the tutorials.

The `data/` folder name is not required; it is simply used here as the recommended
import root.

```
my-study/
├── deployments.csv          # deployment metadata table
├── data/
│   ├── PIN_01/              # one folder per deployment ID
│   │   ├── CMD/             # multi-sensor tag files
│   │   │   ├── xxxxx-Multisensor22Splash52.csv
│   │   │   └── ...
│   │   └── SPOT/            # satellite tag files
│   │       ├── xxxxx-Locations.csv
│   │       └── ...
│   ├── PIN_02/
│   │   ├── CMD/
│   │   │   ├── xxxxx-CameraCMD134Spot98.csv
│   │   │   └── ...
│   │   └── MiniPAT/
│   │       ├── xxxxx-Locations.csv
│   │       └── ...
│   └── ...
└── video/                   # optional: onboard footage
    └── PIN_02/
        ├── video_001.mp4
        └── ...
```

Each deployment folder must be named after the deployment ID used in your metadata
table. This allows nautilus to associate raw files with the corresponding deployment
information.

Inside each deployment folder, create one subfolder for each available data source.
The names shown above (CMD, SPOT, MiniPAT, etc.) are examples only and can be adapted
to your own file organisation: the multi-sensor folder is read from
`sensor.subdirectory` (default `"CMD"`), and a co-deployed satellite tag folder is
detected automatically, or named explicitly with `wc.subdirectory`.

Deployments do not need to contain the same data sources. For example, one deployment
may include a multi-sensor tag and satellite tag, while another may contain only the
multi-sensor data.

Onboard video sits under its own root, passed separately to `getVideoMetadata()`, since
footage is usually much larger than the sensor records and often kept on another drive.

The deployment table is the main metadata file linking your raw files to deployment
information. It should contain one row per deployment and can use your existing column
names. Use `metadataColumns()` to map your columns to the roles expected by nautilus;
biological attributes such as `size` are passed through its `traits` argument rather
than mapped to a role.

| ID | tag_model | attach_time | lon | lat | size | deployment_type |
|----|-----------|-------------|-----|-----|------|-----------------|
| PIN_01 | CATS | 2022-10-13 13:50 | -28.6 | 38.5 | 10 | towed |
| PIN_02 | CATS | 2022-10-14 09:12 | -28.5 | 38.6 | 9.5 | towed |

<br/>

## What nautilus does

### 1 &middot; Prepare and import

Checks your deployment table before any data is read &mdash; identifiers, dates,
coordinates, tag models and sensor configuration &mdash; so that problems surface while
they are still cheap to fix. Column names in your own spreadsheet can be mapped onto the
roles nautilus expects, rather than the other way round. It then reads each animal's raw
sensor files, standardises channel names and units, and merges in the positions and
wet/dry record of a co-deployed satellite tag where one exists. Each animal ends up as a
single object holding its data and its description.

`checkDeploymentMetadata()` &middot; `metadataColumns()` &middot; `importTagData()` &middot; `buildTagData()`

### 2 &middot; Clean and quality-control

Automatically identifies the period when the tag was attached to the animal, removes
handling and recovery periods, regularises sampling intervals, and performs extensive
quality-control checks before any downstream analysis. Every sensor channel is screened
for the faults that occur in practice &mdash; dead or duplicated channels, saturation,
spikes, dropouts, implausible readings and mechanical interference &mdash; and satellite
positions are screened for impossible speeds and distances.

Findings are reported first and acted on only if you ask. Nothing is quietly altered, and
everything that was found stays with the dataset, so you can inspect it later.

`filterDeploymentData()` &middot; `regularizeTimeSeries()` &middot; `checkSensorIntegrity()` &middot; `checkSensorQuality()` &middot; `filterLocations()` &middot; `issues()`

### 3 &middot; Orient and calibrate

Works out how the tag was actually oriented on the animal and rotates its X/Y/Z axes so
that they correspond to the animal's own body axes, then calibrates the magnetometer to
correct for distortion from the tag's electronics and housing, so that headings are
accurate.

A single deployment does not always contain enough varied movement to settle the
orientation on its own. When that happens, nautilus can pool the evidence across
deployments that shared the same physical tag package, and can confirm the result against
onboard video. Where the evidence genuinely is not sufficient, it declines to choose
rather than committing to a guess.

`checkTagMapping()` &middot; `consensusAxisMapping()` &middot; `reviewTagMapping()` &middot; `applyAxisMapping()` &middot; `calibrateMagnetometer()` &middot; `imputePaddleCalibration()`

### The single pivot &mdash; `processTagData()`

One call turns the cleaned, correctly-oriented record into the quantities most analyses
actually use: body pitch, roll and heading; dynamic body acceleration (ODBA and VeDBA) as
a proxy for activity and energy expenditure; swimming speed from a paddle wheel; and
vertical velocity, turning angle and jerk. Heading can be derived by either of two
established approaches, chosen to suit the deployment and compared in the orientation
vignette.

The result is an analysis-ready dataset that carries its own description and a complete
record of how it was made.

### Then branch by question

**Summaries and figures** &mdash; per-animal and cohort-level summary tables, depth
profiles, time-at-depth distributions and comparisons of kinematic variables across
deployments, with a single function to set a consistent visual style.<br/>
`summarizeTagData()` &middot; `plotDepthProfiles()` &middot; `plotTimeAtDepth()` &middot; `plotDistributions()` &middot; `plotTheme()`

**Dive analysis** &mdash; detects vertical excursions and reduces each to one row of
metrics. A dive must pass a deeper threshold to begin and return above a shallower one to
end, so noise around a single cut-off cannot split one dive into many. The reference depth
is yours to choose: the surface for air-breathers, a rolling baseline for fish that never
surface, or an inverted reference for species that rest on the bottom.<br/>
`diveControl()` &middot; `detectDives()` &middot; `diveMetrics()` &middot; `plotDives()`

**Behaviour and kinematics** &mdash; estimates tail-beat frequency and amplitude as a
measure of swimming effort, builds sliding-window feature sets ready for behavioural
classification, and assigns samples to day, night or twilight.<br/>
`calculateTailBeats()` &middot; `extractFeatures()` &middot; `getDielPhase()`

**Movement tracks** &mdash; reconstructs the animal's likely path from its heading and
speed, corrected towards satellite fixes where these exist, and tells you how far to trust
the result by holding fixes back and measuring the error against them. Tracks can be
summarised, mapped, and exported for state-space modelling.<br/>
`reconstructTrack()` &middot; `crossValidateTrack()` &middot; `trackMetrics()` &middot; `plotTracks()` &middot; `exportForSSM()`

**Video and annotation** &mdash; recovers each clip's start time (from the file name, or
by reading a burned-in clock when the file name does not carry one), aligns footage with
the sensor record, lets you score observed behaviours onto individual samples, and renders
video with sensor traces overlaid.<br/>
`getVideoMetadata()` &middot; `filterVideoPeriod()` &middot; `annotateData()` &middot; `renderOverlayVideo()`

<br/>

## Reproducibility

Every nautilus object carries its own metadata and an append-only processing history. Each
function that touches a deployment records what it did, when, with which version of the
package, and with which settings. A saved dataset therefore describes itself: you can
recover the exact parameters that produced it months later, and write a methods section
from the file rather than from memory.

Settings are grouped into named control objects (for example `orientationControl()`,
`integrityControl()`, `diveControl()`) rather than long argument lists, so a study's
processing choices can be defined once, stored, and reused across every deployment.

```r
tagMetadata(tag)          # what this deployment is: animal, tag, sensors, calibration
processingHistory(tag)    # every step applied, in order, with its settings
processingSummary(tags)   # a cohort-level overview
```

<br/>

## Installation

Install the development version from GitHub:

```r
# install.packages("remotes")
remotes::install_github("miguelgandra/nautilus", build_vignettes = TRUE)
```

nautilus has a deliberately light dependency footprint and needs **no geospatial system
libraries** (no GDAL/GEOS/PROJ). A few *optional* branches use external tools when you
reach for them:

| Feature | Optional tool |
|---|---|
| Video re-encoding / overlay rendering | [FFmpeg](https://ffmpeg.org/) |
| In-R video playback (`launchVideo()`) | [VLC](https://www.videolan.org/) |
| Reading a burned-in clock (`getVideoMetadata()`) | R package **tesseract** |

The fine-tuned camera-tag text-recognition model (~11 MB) is **not** bundled with the
package: it is downloaded on first use and cached locally, or you can fetch it ahead of
time with `installCamOcrModel()`. Offline, it falls back gracefully to the generic model.

<br/>

## A minimal pipeline

The core sequence is short and linear; each step reads a `nautilus_tag` and returns an
annotated one.

```r
library(nautilus)

# --- Core sequence: prepare, clean, orient ----------------------------------
deployments <- read.csv("deployments.csv")             # your deployment table
meta <- checkDeploymentMetadata(deployments)           # validate deployment metadata
tags <- importTagData(data.folders = "tag-data/",      # -> one nautilus_tag per animal
                      metadata = meta)

tags <- filterDeploymentData(tags)                     # trim to the on-animal period
tags <- regularizeTimeSeries(tags)                     # place on a regular time grid
tags <- checkSensorIntegrity(tags, apply = TRUE)       # screen every sensor channel

mapping <- consensusAxisMapping(checkTagMapping(tags)) # tag axes -> animal body axes
tags    <- applyAxisMapping(tags, mapping)
tags    <- calibrateMagnetometer(tags)                 # accurate headings

# --- The single pivot -------------------------------------------------------
tags <- processTagData(tags)                           # orientation, kinematics, speed

# --- Optional branches: choose by your question -----------------------------
summarizeTagData(tags)                                 # per-deployment overview
```

Large studies can run each stage straight to disk (`return.data = FALSE` plus an
`output.dir`) instead of holding every deployment in memory &mdash; each stage then returns
the file paths it wrote, which feed straight into the next. The Getting Started guide
covers both styles.

<br/>

<h2 id="learning-more">Learning more</h2>

- **Vignettes** &mdash; long-form guides shipped with the package. Start with *Getting
  Started with nautilus*, which introduces the pipeline, helps you choose a workflow, and
  runs a complete example. List them with:

  ```r
  browseVignettes("nautilus")
  vignette("getting-started", package = "nautilus")
  ```

- **Function reference** &mdash; every exported function is documented with runnable
  examples; open help with `?processTagData` (or any other function name).
- **Developer guide** &mdash; [`DEVELOPER_GUIDE.md`](DEVELOPER_GUIDE.md) records the implemented
  architecture, scientific invariants, coding conventions, known hazards and release workflow.
- **Changelog** &mdash; see [`NEWS.md`](NEWS.md) for what is new in each version.
- **Worked scripts** &mdash; the [`tutorials/`](tutorials/) directory holds two complete,
  runnable examples: [*data processing*](tutorials/tutorial01%20-%20data%20processing.R),
  which takes a set of deployments through the whole workflow, and
  [*video re-encoding*](tutorials/tutorial02%20-%20video%20reencoding.R), which prepares
  onboard footage for analysis.

<br/>

## Citing `nautilus`

If you use nautilus in your research, please cite it as:

```r
citation("nautilus")
```

Until the accompanying methods paper is published, the recommended citation is:

> Gandra, M., Saraiva, B. M., Macena, B. C. L., Afonso, P., &amp; Fontes, J. (2026).
> nautilus: An R package for biologging data processing and analysis. GitHub.
> <https://github.com/miguelgandra/nautilus>

No version is quoted above on purpose: `citation("nautilus")` reads it from the package you
actually have installed, so that call is the single source of truth and cannot drift from this
file. Please include the version it reports.

<br/>

## Related publications

Work associated with the package, the tagging system it was built around, or the datasets it was
developed on:

> Fontes, J., Macena, B., Solleliet-Ferreira, S., Buyle, F., Magalh&atilde;es, R.,
> Bartolomeu, T., Liebsch, N., Meyer, C. &amp; Afonso, P. (2022). The advantages and challenges
> of non-invasive towed PILOT tags for free-ranging deep-diving megafauna. *Animal
> Biotelemetry, 10*(1), 39. <https://doi.org/10.1186/s40317-022-00310-1>

<br/>

## Getting help

Found a bug, or something unclear? Please
[open an issue](https://github.com/miguelgandra/nautilus/issues) with a small reproducible
example.

<br/>

## License

GPL (>= 3). See [`LICENSE.md`](LICENSE.md) for the full license text.
