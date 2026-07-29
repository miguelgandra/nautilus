# nautilus <img src="inst/resources/nautilus-logo.png" align="right" width="110" alt="nautilus logo" />

<!-- badges: start -->
[![Project Status: WIP](https://img.shields.io/badge/status-WIP-orange)](https://www.repostatus.org/#wip)
[![Lifecycle: experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
[![R-CMD-check](https://github.com/miguelgandra/nautilus/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/miguelgandra/nautilus/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->
<!-- CRAN badge omitted until the package is published; www.r-pkg.org 500s for packages not yet on CRAN.
     Re-add on acceptance:
     [![CRAN status](https://www.r-pkg.org/badges/version/nautilus)](https://CRAN.R-project.org/package=nautilus) -->

**nautilus** is an R package for processing and analysing high-resolution archival
biologging data. It provides a reproducible workflow that transforms raw recordings from
multi-sensor animal-borne tags into analysis-ready datasets for behavioural, ecological
and movement analyses.

Modern archival tags record several sensors at once, often tens or hundreds of times per
second, and the distance between the raw file and the first ecological result is long:
the record has to be trimmed to the time on the animal, screened for sensor faults, and
corrected for how the tag happened to sit on the body before anything derived from it can
be trusted. nautilus carries a deployment through that whole sequence in one place, and
keeps a record of every decision it made along the way.

It reads depth, temperature, tri-axial acceleration, magnetometer and gyroscope data from
**CATS** and **CEiiA** multi-sensor loggers and **Little Leonardo** archival loggers, and
can take data from any other tag that is already in R. Positions and the wet/dry record
from a co-deployed **Wildlife Computers** satellite tag can be folded in, and footage from
onboard cameras can be aligned to the sensor record. It was developed around CATS Diary and
Camera tags carried in towed **PILOT** packages
([Fontes et al., 2022](https://doi.org/10.1186/s40317-022-00310-1)), but the workflow is
general.

<br/>

## Who is this package for?

nautilus is for researchers working with high-resolution archival biologging tags &mdash;
CATS, Little Leonardo and similar devices &mdash; who need to get from a raw download to
data they can analyse, without writing the intervening steps themselves.

It is likely to be useful if you want to:

- summarise and visualise a set of deployments consistently, and compare animals;
- describe diving behaviour, swimming effort or activity budgets;
- estimate body orientation and reconstruct fine-scale movement paths between satellite fixes;
- build labelled datasets for behavioural classification, including from onboard video;
- keep a defensible, reproducible record of how each dataset was processed.

You do not need every sensor. Deployments with only an accelerometer and a depth sensor
work fine; the analyses that need a magnetometer or a speed sensor simply become
unavailable, and nautilus says so rather than guessing.

> **Status.** nautilus is under active development ahead of its first release. The public
> API is stabilising but may still change.

<br/>

## The workflow

Every deployment follows the same **core sequence** &mdash; prepare, clean, orient &mdash;
which converges on a single call, `processTagData()`, returning one self-describing
`nautilus_tag` object. From there the analysis **branches** according to your scientific
question, so you only run the parts you need.

<p align="center">
  <img src="man/figures/nautilus-pipeline-v2.png" width="820"
       alt="The nautilus workflow. Deployment metadata and raw tag data, optionally joined by a co-deployed satellite tag, flow through three core stages: 1, prepare and import; 2, clean and quality-control; 3, orient and calibrate. A parallel video track prepares onboard footage and feeds the orientation review. The core stages converge on processTagData(), which returns an analysis-ready nautilus_tag object and then branches into five optional analyses: summaries and figures, dive analysis, behaviour and kinematics, movement tracks, and video and annotation. A utilities strip provides metadata and audit-trail access throughout." />
</p>

The [Getting Started guide](#learning-more) walks through this diagram end to end on a real
deployment.

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
remotes::install_github("miguelgandra/nautilus")
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
meta <- checkDeploymentMetadata("deployments.csv")     # validate deployment metadata
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
