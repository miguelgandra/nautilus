###############################################################################################
## Miguel Gandra || CCMAR || m3gandra@gmail.com ###############################################
## Tutorial 03: Dive Behaviour Analysis with the 'nautilus' R Package #########################
###############################################################################################

# Picking up where Tutorial 01 left off, this walkthrough covers the dive-analysis branch of
# 'nautilus': turning a continuous depth record into discrete dives, reducing those dives to a
# per-dive table, and reading the result honestly. It uses the same PINTADO whale-shark datasets,
# so it runs straight on the files Tutorial 01 wrote. The workflow covers:
#
#   1.  Decide what counts as a dive, and why that is a choice rather than a default.
#   2.  Detect dives and annotate every sample with the dive and phase it belongs to.
#   3.  Reduce the annotated data to one row per dive, with kinematics and per-dive covariates.
#   4.  Read the quality block: which dives the record can actually support.
#   5.  Summarize dive behaviour across the cohort.
#   6.  Produce publication-quality dive figures.
#
# A note on philosophy before starting. A depth trace is continuous; a "dive" is an interpretation
# imposed on it. nautilus makes that interpretation explicit and reproducible - the thresholds
# travel with every row of output - but it will not choose the biology for you. Two studies of the
# same animal with different thresholds are both correct and not comparable, which is exactly why
# the settings are written into the results.
#
# Notes:
#  - Assumes Tutorial 01 has been run: this script reads "./data interim/06_tailbeats".
#  - Defaults are tuned to whale-shark kinematics. Other species need other thresholds - see STEP 1.
#  - Create "./data interim/07_dives", "./plots" and "./outputs" before running; nautilus is
#    fail-fast about paths and will not create them for you.

################################################################################
# STEP 0. Load the package and the processed data                              #
################################################################################

library(nautilus)

# Tutorial 01 finished by writing tail-beat-annotated files. Those are the input here: dive detection
# needs only depth and time, but working from the final files keeps every later metric (temperature,
# activity, tail-beat frequency) available to summarise per dive in STEP 3.
processed_files <- list.files("./data interim/06_tailbeats", full.names = TRUE)

# A quick look at what we are starting from. Any nautilus_tag will print its deployment, its sensors
# and the full processing audit trail.
print(processed_files)
# summary(readRDS(processed_files[1]))
# processingHistory(readRDS(processed_files[1]))

################################################################################
# STEP 1. Decide what counts as a dive                                         #
################################################################################

# This is the step that matters most, and the one worth spending time on. Everything downstream -
# every count, every duration, every comparison between animals - inherits the definition set here.
#
# nautilus uses one definition for every taxon, and makes the taxonomy a choice rather than an
# assumption. A dive is a vertical excursion away from a REFERENCE level, detected by two-threshold
# hysteresis and ended by a return to within a band of that reference. Three settings adapt it:
#
#   reference  - where "not diving" sits. "surface" suits air-breathers, whose zero is anchored by
#                surfacing; "baseline" tracks a running level and suits fish that never come shallow,
#                or benthic animals resting at depth. "auto" (used here) decides per deployment from
#                the depth-drift provenance and whether the animal actually visits the surface band,
#                and reports which it picked - so a mixed cohort resolves sensibly.
#   direction  - "down" for animals excursing downward from a shallow level, "up" for benthic
#                resters leaving the bottom, "both" for either.
#   thresholds - depth.threshold (how deep an excursion must go) and surface.band (how close it must
#                return before the dive is over). The gap between them is the hysteresis, and it is
#                not optional: with a single threshold, sensor noise at the crossing splits one dive
#                into many and the dive count becomes a property of the pressure transducer.
#
# Whale sharks surface regularly, so "auto" will generally resolve to a surface reference. We set an
# explicit 5 m threshold rather than letting it derive: a derived threshold is the smallest excursion
# the RECORD can support, which is a property of the instrument, not of the animal. Choose yours from
# the study system - and choose it before looking at your response variable.

dive_settings <- diveControl(
  reference       = "auto",   # resolve per deployment; the choice is reported and stored
  direction       = "down",
  depth.threshold = 10,       # metres past the reference before an excursion counts as a dive
  surface.band    = 5,        # must return within 5 m of the reference to close the dive
  min.duration    = 20,       # seconds; excludes brief undulations that are not dives
  min.prominence  = NULL,     # NULL never splits a W-shaped excursion into two dives (see below)
  max.gap         = 300,      # a longer interruption splits the dive and marks both parts censored
  phase.method    = "vertical.rate")   # how descent/bottom/ascent are separated

# On min.prominence: left NULL, a deep excursion with a partial re-ascent in the middle is reported
# whole, however many sub-peaks it contains. That is deliberate - splitting is an interpretive act,
# and a 50 m dive with a 5 m re-ascent halfway is probably one dive. Set a number (in metres) to opt
# in. Either way the prominence is reported per dive as `prominence_m`, so you can see what splitting
# WOULD do before choosing to do it.

################################################################################
# STEP 2. Detect the dives                                                     #
################################################################################

# detectDives() annotates every sample with three columns - always all three, so the schema never
# varies between deployments:
#
#   dive_id         integer; 0 outside any dive (not NA, which would propagate silently and be
#                   deleted by any na.omit() in your own pipeline)
#   dive_phase      factor: descent / bottom / ascent / inter_dive
#   depth_baseline  the reference level the excursion was measured from
#
# The diagnostic PDF is worth opening the first time you run this on a new species: it shows each
# deployment's depth trace with the detected dives, the reference level and the thresholds marked,
# which is the quickest way to see whether the definition matches what you would have called a dive.

detectDives(data          = processed_files,
            control       = dive_settings,
            plot          = FALSE,
            plot.file     = "./plots/dive-detection.pdf",
            return.data   = FALSE,
            output.dir    = "./data interim/07_dives",
            verbose       = "detailed")

dive_files <- list.files("./data interim/07_dives", full.names = TRUE)

# Zero dives is a result, not a failure: it is reported with the threshold that produced it, the
# observed depth range and the reference used, and the threshold is never quietly relaxed until dives
# appear. If a deployment reports none, check its depth range before changing anything.

################################################################################
# STEP 3. Reduce to one row per dive                                           #
################################################################################

# diveMetrics() turns the per-sample annotation into a per-dive table: timing, depth, phase
# structure, kinematics, the detection settings that produced each dive, and a quality block.
#
# `variables` is what makes this a general reducer rather than a fixed list of depth statistics: name
# any per-sample channel and it is summarised over each dive. Here we ask for the thermal environment
# the dive happened in, the effort the animal was making, and its tail-beat frequency - so the table
# can answer "did it beat faster on deep dives?" without a second pass over the raw data.
#
# Angles need circular statistics (the mean of 350 and 10 degrees is 0, not 180), so heading and roll
# are handled separately and reported as a mean angle plus a mean resultant length.
#
# One habit worth adopting: calculateTailBeats() names its output after the backend that produced it
# (tbf_hz_peaks, tbf_hz_wavelet), so provenance travels with every value. Rather than hard-coding one,
# ask the data which is there. tailBeatColumn() resolves it from the column contents - so this script
# keeps working whichever backend Tutorial 01 was run with.
tbf_col <- tailBeatColumn(readRDS(dive_files[1]))
tbf_col

dive_metrics <- diveMetrics(data               = dive_files,
                            variables          = c("temp", "vedba", tbf_col, "heading"),
                            circular.variables = c("heading", "roll"),
                            statistics         = c("mean", "sd"),
                            by.phase           = TRUE,   # also summarise within descent/bottom/ascent
                            verbose            = "detailed")

# by.phase = TRUE is what lets you ask whether effort differs between descent and ascent - a natural
# question for a negatively buoyant animal, and one the whole-dive mean cannot answer. It costs
# columns: each variable adds 2 without it and 8 with it.

nrow(dive_metrics)
head(dive_metrics[, c("ID", "dive_id", "start", "duration_s", "max_depth_m", "amplitude_m")])

# The requested variables arrive as <variable>_<statistic> columns - vedba_mean, temp_sd - with the
# per-phase versions alongside them (vedba_descent_mean, vedba_bottom_mean, vedba_ascent_mean), and
# the circular ones as a mean angle plus a resultant length (heading_mean_angle, heading_mrl).
grep("^vedba|^heading", names(dive_metrics), value = TRUE)

################################################################################
# STEP 4. Read the quality block before analysing anything                     #
################################################################################

# Every row says how much of its own dive was actually recorded. This is not bookkeeping - it decides
# which dives may enter a statistic, and skipping it is the most common way to publish a number that
# describes the tag rather than the animal.
#
#   complete             TRUE only when nothing censored the dive. This is the filter to use.
#   censoring            why, exactly: "none", "boundary" (a record edge), "time_gap", "depth_gap"
#                        (the depth channel went dark), or "mixed".
#   truncated_start/end  the tag started or stopped mid-dive. Kept and flagged, never dropped -
#                        dropping them silently shortens the tail of the duration distribution.
#   depth_coverage       the fraction of the dive's samples carrying a finite depth. This is what
#                        tells a genuine long foray from a dropout: a long dive with low coverage is
#                        mostly absent record, and its duration describes the gap, not the animal.
#   depth_attenuation    how much of the dive's amplitude the processing could have removed. 1 means
#                        nothing did; 0.6 means up to 40% of amplitude_m and max_depth_m may be
#                        missing, because downsampling averages short excursions away.
#   inter_dive_censored  asks about the INTERVAL, not the dives bounding it. Two dives can each be
#                        complete and still be separated by an eight-hour blackout, which enters the
#                        table as an eight-hour surface interval describing the sensor.

# What the cohort looks like before filtering:
table(dive_metrics$censoring)
summary(dive_metrics$depth_coverage)
summary(dive_metrics$depth_attenuation)

# Keep the dives the record can support, and report how many that removed - always, in the paper.
dives_analysable <- subset(dive_metrics, complete & depth_coverage > 0.9 & depth_attenuation > 0.9)

cat(sprintf("Retained %d of %d dives (%.1f%%) after quality filtering\n",
            nrow(dives_analysable), nrow(dive_metrics),
            100 * nrow(dives_analysable) / nrow(dive_metrics)))

# Surface intervals need their own filter, for the reason above:
surface_intervals <- subset(dive_metrics, !inter_dive_censored & !is.na(inter_dive_s))

################################################################################
# STEP 5. Summarize dive behaviour across the cohort                           #
################################################################################

# summarizeTagData() gains a dive block automatically once the data carry a `dive_id` column. Its
# numbers are not recomputed: the deployment is reduced by the same engine behind diveMetrics(), so a
# dive count quoted from the summary is by construction the one the per-dive table gives.

dive_summary <- summarizeTagData(data        = dive_files,
                                 error.stat  = "sd",
                                 verbose     = "detailed")

# The dive columns: n_dives, dive_duration_median_min, dive_duration_max_min, dive_depth_median_m,
# dive_depth_max_m, and the censoring counts dives_incomplete / dives_truncated / dives_gapped.
#
# One figure to read carefully: the printed cohort line reports a median of per-deployment medians,
# not a pooled median over all dives. The per-deployment columns are exact; for a pooled figure use
# the per-dive table directly, e.g. median(dives_analysable$duration_s).

summary_table <- format(dive_summary, style = "concise", include.summary.row = TRUE)
write.csv2(summary_table, file = "./outputs/dive_summary.csv",
           row.names = FALSE, fileEncoding = "UTF-8")

# The per-dive table itself is usually the analysis unit, so export it too.
write.csv2(dive_metrics, file = "./outputs/dive_metrics.csv",
           row.names = FALSE, fileEncoding = "UTF-8")

################################################################################
# STEP 6. Figures                                                              #
################################################################################

# A shared theme keeps every figure in the set visually consistent. Bump `cex` for a presentation, or
# switch preset to "minimal" for a sparser look.
fig_theme <- plotTheme(preset = "light", cex = 1.05)

## 6.1 Compare deployments on per-dive metrics -----------------------------------------------------

# plotDives() draws every dive as a point in its deployment's column, with a median and interquartile
# marker over the top. Where a bar of means would hide both the spread and the individual dives,
# this shows the cohort comparison and the raw material behind it together - which matters because
# dive metrics are usually skewed enough that the mean is not the interesting number.
#
# Note it takes the per-dive TABLE, not the tags: it computes nothing that diveMetrics() has not.
# We pass the quality-filtered table, so the figure shows only dives the record supports.

plotDives(data      = dives_analysable,
          metrics   = c("max_depth_m", "duration_s", "bottom_duration_s", "descent_rate_q90"),
          order.by  = "median",        # rank deployments by the first metric; "id" keeps them alphabetical
          order.metric = "max_depth_m",
          trim      = 0.95,            # clip the top 5% so a few extreme dives do not flatten the panel
          min.n     = 5,
          theme     = fig_theme,
          plot      = FALSE,
          plot.file = "./plots/fig1-dive-metrics-by-deployment.pdf")

## 6.2 Depth profiles with the dives in context ----------------------------------------------------

# The depth trace is where dive structure is most legible. Colouring by tail-beat frequency rather
# than the default temperature turns the profile into a picture of effort through the water column -
# whether the animal worked hardest descending, on the bottom, or coming back up.

plotDepthProfiles(data             = dive_files,
                  color.by         = tbf_col,          # resolved in STEP 3, whichever backend ran
                  color.label      = "Tail-beat frequency (Hz)",
                  geom             = "line",
                  shade.diel       = TRUE,
                  same.depth.scale = FALSE,
                  downsample       = 5,
                  theme            = fig_theme,
                  plot             = FALSE,
                  plot.file        = "./plots/fig2-depth-profiles-effort.pdf",
                  ncols            = 2,
                  nrows            = 5)

## 6.3 How the cohort uses the water column --------------------------------------------------------

# Where the profiles show individual structure, plotTimeAtDepth() shows allocation: how much of the
# record each depth bin holds. Knowing an animal reached 300 m says little about how it used the
# column - it may have touched that depth once and lived at 20 m. Mirroring night against day is
# usually the first thing to look at in a diel migrator.

tad_summary <- plotTimeAtDepth(data      = dive_files,
                               variable  = c("depth", "temp"),
                               diel      = TRUE,          # night on the left, day on the right
                               style     = "profile",
                               theme     = fig_theme,
                               plot      = FALSE,
                               plot.file = "./plots/fig3-time-at-depth.pdf")

## 6.4 Distributions of the dive metrics themselves ------------------------------------------------

# plotDistributions() works on any per-row table, so it takes the per-dive table as readily as the
# per-sample one: a stack of per-deployment violins over a pooled population strip, showing the shape
# a mean would hide. Multimodal dive depths are common and ecologically meaningful - two modes often
# mean two behaviours.

dive_dist <- plotDistributions(data      = dives_analysable,
                               metrics   = c("max_depth_m", "duration_s", "inter_dive_s"),
                               order.by  = "median",
                               reference = "median",
                               trim      = 0.99,
                               min.n     = 10,
                               theme     = fig_theme,
                               plot      = FALSE,
                               plot.file = "./plots/fig4-dive-metric-distributions.pdf")


###############################################################################################
# That completes the dive branch: from a continuous depth record to a quality-screened per-dive
# table, cohort summaries, and four publication-ready figures. Two things to carry forward -
# the dive definition travels in every row of `dive_metrics` (reference, direction,
# depth_threshold_m, surface_band_m), so a published dive count is reproducible from the table
# alone; and the quality block is what separates a statement about the animal from a statement
# about the tag.
#
# Tutorial 04 takes the same processed data down the other branch: reconstructing where the
# animals actually went.
###############################################################################################
