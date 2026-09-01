#######################################################################################################
# Estimate tail-beat frequency and oscillation amplitude ##############################################
#######################################################################################################

#' Estimate tail-beat frequency and oscillation amplitude from tag motion data
#'
#' @description
#' Estimates the dominant periodic motion within a user-defined frequency band from archival tag data.
#' The resulting frequency may represent tail beats, wing beats or another periodic locomotor movement,
#' depending on the study species, tag placement and selected motion channel.
#'
#' The frequency can be estimated by peak detection, by a continuous wavelet transform, or by both. When
#' both are requested their estimates are kept separately and their agreement is reported as an
#' additional quality-control metric.
#'
#' The function expects processed data and should normally follow [processTagData()]. It can analyse one
#' or more candidate motion channels and, where several are supplied, selects the most suitable channel
#' separately for each deployment.
#'
#' @param data A `nautilus_tag` object, a list of tag datasets, a data frame containing multiple
#'   individuals identified by `id.col`, or a character vector of `.rds` file paths. File paths are
#'   processed sequentially, allowing large collections to be analysed without loading them all into
#'   memory. The output of [processTagData()] is recommended.
#' @param method One or both estimation methods, `"peaks"` and `"wavelet"` (default both). Where both
#'   are given, the first is the primary method and the second a cross-check.
#' @param id.col Column identifying individuals (default `"ID"`).
#' @param datetime.col Column containing timestamps (default `"datetime"`).
#' @param motion.col One or more candidate motion channels from which the periodic movement is estimated
#'   (default `"sway"`). Where several are supplied, the most suitable is selected separately for each
#'   deployment. See Details.
#' @param min.freq.Hz,max.freq.Hz Lower and upper limits of the frequency range analysed, in Hz
#'   (defaults `0.1` and `3`). Choose them from the locomotor frequencies expected of the study species.
#' @param bandpass.filter Whether the motion signal is band-pass filtered before analysis (default
#'   `TRUE`).
#' @param filter.low.freq,filter.high.freq Optional cut-off frequencies for the band-pass filter, in Hz.
#'   `NULL` (default) derives them from `min.freq.Hz` and `max.freq.Hz`, set slightly wider so that the
#'   filter does not attenuate the edges of the band being estimated.
#' @param filter.order Order of the Butterworth band-pass filter (default `4`).
#' @param min.amplitude Optional amplitude threshold, in the units of the selected motion channel, above
#'   which a sample is classified as active swimming. `NULL` (default) performs no classification. See
#'   Details.
#' @param smooth.window Width, in seconds, of the centred moving average applied to the frequency and
#'   amplitude estimates (default `10`). `0` disables smoothing.
#' @param max.interp.gap Longest gap, in seconds, between valid frequency estimates that may be filled
#'   by linear interpolation (default `10`). `NULL` disables interpolation.
#' @param ridge.prominence Minimum prominence of the dominant wavelet ridge relative to the surrounding
#'   spectral background (default `2`). Estimates with less support are returned as `NA`. Used only by
#'   `"wavelet"`.
#' @param min.periodicity Minimum autocorrelation-based periodicity required before an estimate is
#'   reported (default `0.15`). Estimates below it are returned as `NA`. Used only by `"peaks"`.
#' @param plot Whether to draw diagnostic plots to the active graphics device (default `FALSE`).
#' @param plot.file Path to a multi-page PDF holding the diagnostic plots, or `NULL` (default).
#' @param plot.wavelet Whether the wavelet power spectrum is included in the diagnostic plots (default
#'   `TRUE`).
#' @param plot.diagnostic Whether the time-series diagnostic panels are included (default `TRUE`).
#' @param return.data Whether to return the processed datasets in memory (default `TRUE`). When `FALSE`,
#'   the function returns the paths of the `.rds` files written to `output.dir`, which feed directly into
#'   the next step's `data` argument; this requires `output.dir` to be specified.
#' @param output.dir An existing directory in which to save one `.rds` file per deployment. Supplying a
#'   directory is what triggers saving; `NULL` (default) writes nothing.
#' @param output.suffix Optional string appended to each saved file name, before `.rds`, to label a
#'   processing run or avoid overwriting an earlier one. Only used when `output.dir` is specified.
#' @param compress Compression used when saving `.rds` files: `TRUE` (default, gzip), `FALSE`, or one of
#'   `"gzip"`, `"bzip2"` or `"xz"`. See [base::saveRDS()].
#' @param verbose How much detail to print: `0`/`"quiet"`, `1`/`"normal"`, or `2`/`"detailed"`
#'   (default), which adds per-deployment diagnostics.
#'
#' @details
#' ## Estimation methods
#'
#' \describe{
#'   \item{`"peaks"`}{Locates successive oscillatory cycles in the band-passed signal, each contributing
#'     a frequency from its peak-to-peak interval and an amplitude from its peak-to-trough excursion.
#'     Estimates rest on individual cycles.}
#'   \item{`"wavelet"`}{Follows the dominant frequency through time with a Morlet continuous wavelet
#'     transform (Torrence and Compo 1998). The estimate is continuous rather than cycle-by-cycle, and
#'     is often steadier on noisy or non-stationary signals.}
#' }
#'
#' Both can be requested at once. Their frequencies are stored in separate columns, and `tbf_agree`
#' records whether the two fall within 10% of each other at a given sample.
#'
#' Read agreement as a certificate rather than an error flag. Where the two methods concur they are
#' rarely both wrong; where they differ, nothing identifies which to distrust, so `FALSE` means
#' unresolved rather than bad. The certificate has one known blind spot: both methods establish a
#' dominant periodicity before anything else, so neither can vouch for the other against a harmonic
#' error, and both may settle on the same harmonic of the true locomotor frequency.
#'
#' ## Selecting the motion channel
#'
#' Where `motion.col` names several candidates, the channel is chosen by agreement in dominant frequency
#' across them, with in-band signal power breaking ties. Coordinated propulsion drives several axes at
#' one frequency, whereas a non-locomotor artefact is usually confined to one; choosing on raw in-band
#' power alone would favour the artefact whenever it is the stronger signal. The classic case is the
#' pendulum swing of a towed tag, which produces a sharp lateral-axis peak a little below the true beat.
#'
#' Where no two candidates agree, the channel with the strongest in-band signal is used and the estimate
#' is flagged as possibly reflecting an artefact. Flat or stuck channels are excluded before selection,
#' so they cannot form a spurious consensus. Where the deployment metadata marks a tag as towed, a
#' further warning notes that tow-pendulum motion may contaminate the lateral axis whichever channel is
#' finally used.
#'
#' ## Harmonic ambiguity
#'
#' A fish swimming by lateral undulation beats on a single axis, sway, while surge and heave carry its
#' second harmonic. A corroborated surge-and-heave pair can therefore sit at twice the true tail-beat
#' rate.
#'
#' That spectrum is identical to a genuine wingbeat consensus contaminated by an artefact near half the
#' beat, so **the ambiguity cannot be resolved from the signal alone**. Rather than guess, and risk
#' silently halving a correct estimate, the function keeps the channel it selected and warns whenever
#' another channel carries a comparable peak at about half the chosen frequency, naming it as the likely
#' fundamental for a single-axis swimmer. Resolving it needs independent information: video, or
#' species-specific knowledge of locomotor mechanics.
#'
#' ## Frequency band and filtering
#'
#' `min.freq.Hz` and `max.freq.Hz` bound the frequencies searched. The sampling rate must exceed twice
#' `max.freq.Hz` to satisfy the Nyquist criterion, and a deployment that fails this is reported; in
#' practice a substantially higher rate is needed before an estimate is reliable.
#'
#' The band-pass limits default to slightly outside the estimation band, so that the filter does not
#' attenuate the frequencies being estimated. Wavelet amplitudes are corrected for the filter's
#' attenuation, so `tbf_amplitude_peaks` and `tbf_amplitude_wavelet` measure the same quantity and are
#' directly comparable.
#'
#' ## When an estimate is withheld
#'
#' `min.periodicity` withholds a peak-detection estimate where the record shows too little periodic
#' structure to support one; `ridge.prominence` withholds a wavelet estimate where the dominant ridge is
#' not distinct enough from the spectral background.
#'
#' Both govern *whether* an estimate is reported, never *which* frequency is reported. Peak detection is
#' otherwise data-driven and not user-tunable, so the frequency returned does not depend on any
#' behavioural threshold.
#'
#' ## Oscillation amplitude
#'
#' Amplitude is the peak-to-trough excursion of the periodic motion, in the units of the selected motion
#' channel, and serves as a proxy for the magnitude of the oscillation.
#'
#' It should not be read as a direct measure of swimming effort without validation for the species, tag
#' placement and sensor configuration.
#'
#' ## Swimming classification
#'
#' Detecting a periodic oscillation does not by itself establish that an animal is actively swimming, so
#' no swimming-or-gliding classification is performed by default and `tbf_swimming` is returned as `NA`.
#'
#' Supplying `min.amplitude` enables classification against that threshold. It should be calibrated for
#' the species and tag configuration, ideally against independently validated behavioural observations.
#'
#' ## Smoothing and interpolation
#'
#' `smooth.window` applies a centred moving average to the frequency and amplitude series;
#' `max.interp.gap` optionally fills short gaps between valid estimates by linear interpolation.
#'
#' Interpolated values represent assumed continuity between estimates rather than measurements, so the
#' gap allowed should stay short relative to the timescale on which locomotor behaviour changes.
#'
#' ## Diagnostics and quality control
#'
#' The diagnostic plots show the band-passed signal with the estimated frequency and amplitude, and
#' optionally the wavelet power spectrum with the fitted ridge overlaid, which is the practical way to
#' judge whether the analysis band, the signal quality and the chosen channel are right.
#'
#' Quality-control findings cover estimates pressed against the edges of the analysis band, disagreement
#' between methods, uncertain channel selection and possible harmonic ambiguity. Each is raised once per
#' run, with the affected deployments named inline, rather than once per deployment, so a large batch
#' neither buries the console nor loses warnings past the limit R keeps.
#'
#' @return When `return.data = TRUE`, the input data with the tail-beat columns added. Which columns
#'   appear depends on the methods requested:
#'
#'   \describe{
#'     \item{`tbf_hz_peaks`, `tbf_hz_wavelet`}{Estimated frequency in Hz, one column per method that
#'       ran and named after it.}
#'     \item{`tbf_amplitude_peaks`, `tbf_amplitude_wavelet`}{Oscillation amplitude as a peak-to-trough
#'       excursion, in the units of the selected motion channel.}
#'     \item{`tbf_swimming`}{Whether the animal was actively swimming, where `min.amplitude` was
#'       supplied, and `NA` otherwise. Unsuffixed: it derives from the band-passed signal both methods
#'       share.}
#'     \item{`tbf_agree`}{Whether the two methods agree within 10%, where both ran. Also unsuffixed,
#'       being a property of the pair rather than of either one.}
#'   }
#'
#'   There is deliberately no method-agnostic `tbf_hz`, so a value's provenance always travels with it;
#'   use [tailBeatColumn()] to resolve which column to read without hard-coding a method.
#'
#'   The channel selected, the methods used, median frequency and amplitude, and the percentage of time
#'   classified as swimming are recorded in the deployment's processing history. When
#'   `return.data = FALSE`, a character vector of the written `.rds` file paths.
#'
#' @note Band-pass filtering requires the \pkg{signal} package; set `bandpass.filter = FALSE` to analyse
#'   the unfiltered signal instead.
#'
#' @references
#' Torrence C, Compo GP (1998) A practical guide to wavelet analysis. *Bulletin of the American
#' Meteorological Society* 79:61-78.
#' \doi{10.1175/1520-0477(1998)079<0061:APGTWA>2.0.CO;2}
#'
#' @seealso [processTagData()] for deriving the motion channels used as input; [tailBeatColumn()] for
#'   reading the result without hard-coding a method; [checkTagMapping()] for confirming the axis frame
#'   the channels are expressed in; [plotDives()] and [extractFeatures()] for what typically follows.
#'
#' @examples
#' \dontrun{
#' # Estimate tail-beat frequency by peak detection on a single channel.
#' tag <- calculateTailBeats(processed, method = "peaks", motion.col = "sway",
#'                           min.freq.Hz = 0.1, max.freq.Hz = 2.5)
#'
#' # Let the channel be chosen by consensus across three candidates, and cross-check both methods.
#' tag <- calculateTailBeats(processed, method = c("peaks", "wavelet"),
#'                           motion.col = c("sway", "surge", "heave"))
#'
#' # A batch too large to hold in memory: write a diagnostic PDF and pass the paths on.
#' calculateTailBeats(list.files("./processed", full.names = TRUE),
#'                    motion.col = c("sway", "surge", "heave"),
#'                    plot.file = "./qc/tail_beats.pdf",
#'                    return.data = FALSE, output.dir = "./tailbeats")
#' }
#' @export
calculateTailBeats <- function(data,
                               method = c("peaks", "wavelet"),
                               id.col = "ID",
                               datetime.col = "datetime",
                               motion.col = "sway",
                               min.freq.Hz = 0.1,
                               max.freq.Hz = 3,
                               bandpass.filter = TRUE,
                               filter.low.freq = NULL,
                               filter.high.freq = NULL,
                               filter.order = 4,
                               min.amplitude = NULL,
                               smooth.window = 10,
                               max.interp.gap = 10,
                               ridge.prominence = 2,
                               min.periodicity = 0.15,
                               plot = FALSE,
                               plot.file = NULL,
                               plot.wavelet = TRUE,
                               plot.diagnostic = TRUE,
                               return.data = TRUE,
                               output.dir = NULL,
                               output.suffix = NULL,
                               compress = TRUE,
                               verbose = "detailed") {

  ##############################################################################
  # Initial checks and setup ###################################################
  ##############################################################################

  # start the timer
  start.time <- Sys.time()

  # resolve the estimation method (peaks = default; wavelet = optional CWT)
  # `method` may name one backend or both. Naming both runs both: each fills its own tbf_hz_<backend>
  # column and their per-row agreement becomes tbf_agree. The two are methodologically
  # independent, so agreement between them is worth more than either estimate alone.
  # match.arg(several.ok = TRUE) DISCARDS anything it cannot match rather than complaining, so a typo in
  # one element would silently drop that backend and take the cross-check with it
  bad <- setdiff(method, c("peaks", "wavelet"))
  if (length(bad)) .abort(c("{.arg method} must be one or both of {.val peaks} and {.val wavelet}.",
                            "x" = "Unknown: {.val {bad}}."))
  methods <- match.arg(method, c("peaks", "wavelet"), several.ok = TRUE)
  if (anyDuplicated(methods)) .abort("{.arg method} must not name the same backend twice.")
  method <- methods[1]

  # both methods need 'signal' for the band-pass; the wavelet transform is now self-contained
  if (bandpass.filter && !requireNamespace("signal", quietly=TRUE)) {
    .abort("Band-pass filtering requires the {.pkg signal} package; install it or set {.arg bandpass.filter = FALSE}.")
  }

  # verbosity level (0 quiet / 1 normal / 2 detailed); detailed per-step output prints only at >= 2
  lvl <- .verbosity(verbose)

  # validate column specifications. `motion.col` may name several candidate axes; the best (highest
  # in-band power) is chosen per individual.
  .assert_string(id.col, "id.col"); .assert_string(datetime.col, "datetime.col")
  if (!is.character(motion.col) || length(motion.col) < 1) .abort("{.arg motion.col} must be one or more column names.")
  .assert_flag(plot, "plot"); .assert_flag(plot.wavelet, "plot.wavelet"); .assert_flag(plot.diagnostic, "plot.diagnostic")
  # a threshold of exactly 0 would classify every sample as swimming, including a motionless record
  if (!is.null(min.amplitude)) {
    .assert_number(min.amplitude, "min.amplitude", min = 0)
    if (min.amplitude <= 0) .abort("{.arg min.amplitude} must be a positive amplitude, or {.code NULL}.")
  }
  make_plots <- plot || !is.null(plot.file)

  # check if data is a character vector of RDS file paths
  is_filepaths <- is.character(data)
  .assert_nonempty(data, "data")             # loud failure on empty input (e.g. a typo'd list.files() -> character(0))
  if (is_filepaths) {
    # first, check all files exist
    missing_files <- data[!file.exists(data)]
    if (length(missing_files) > 0) {
      .abort(c("Some {.arg data} files were not found:", "x" = "{.path {missing_files}}"))
    }
  } else if (!is.list(data) || inherits(data, "data.frame")) {
    # if it's a single data.frame, convert it to a list
    .assert_columns(data, id.col, "data")
    data <- split(data, data[[id.col]])
  }

  # output method validation
  .assert_flag(return.data, "return.data")
  .assert_output(return.data, output.dir)

  # if data is already in memory (not file paths), validate each dataset up front
  if (!is_filepaths) {
    for (nm in names(data)) {
      .assert_columns(data[[nm]], c(id.col, datetime.col), sprintf("data[['%s']]", nm))
      # motion-column presence is handled per deployment by the skip path in the loop (see above)
      if (!inherits(data[[nm]][[datetime.col]], "POSIXct")) {
        .abort("{.arg datetime.col} ({.val {datetime.col}}) must be a POSIXct column in {.val {nm}}.")
      }
    }
    missing_attr <- vapply(data, function(x) is.null(attr(x, "nautilus.version")), logical(1))
    if (any(missing_attr)) {
      cli::cli_warn(c("Some datasets were likely not processed via {.fn importTagData}: {.val {names(data)[missing_attr]}}.",
                      "i" = "Run them through {.fn importTagData} first to ensure correct formatting."))
    }
  }

  # fail-fast directory / file checks
  .assert_dir(output.dir, "output.dir")
  .assert_string(output.suffix, "output.suffix", null_ok = TRUE)
  .assert_compress(compress)
  .assert_writable_file(plot.file, "plot.file", ext = "pdf")    # parent dir must exist; .pdf only

  # validate frequency parameters
  if (!is.numeric(min.freq.Hz) || length(min.freq.Hz) != 1 || min.freq.Hz <= 0) .abort("{.arg min.freq.Hz} must be a single positive number.")
  if (!is.numeric(max.freq.Hz) || length(max.freq.Hz) != 1 || max.freq.Hz <= 0) .abort("{.arg max.freq.Hz} must be a single positive number.")
  if (min.freq.Hz >= max.freq.Hz) .abort("{.arg min.freq.Hz} must be less than {.arg max.freq.Hz}.")

  # Warn only at values implausible for any swimmer. Tail-beat frequency scales inversely with body
  # length (roughly 2-20 Hz below 0.5 m, ~0.2 Hz for a 10 m shark), so a high max.freq.Hz is the correct
  # setting for a small-bodied species and must not be discouraged here; whether the sampling rate can
  # actually support it is a separate question, checked per individual against Nyquist further below.
  if (max.freq.Hz > 20) warning(paste("Specified max.freq.Hz of", max.freq.Hz, "Hz is above the tail-beat range reported even for small-bodied fish (~20 Hz). Check the units and the target species."))
  if (min.freq.Hz < 0.01) warning(paste("Specified min.freq.Hz of", min.freq.Hz, "Hz seems unusually low for tail beat frequencies. This may detect non-tail-beat movements."))

  # validate bandpass filter parameters
  .assert_flag(bandpass.filter, "bandpass.filter")
  .assert_number(ridge.prominence, "ridge.prominence", min = 0)
  # an autocorrelation is bounded by 1, so a floor at or above it would withhold every estimate
  .assert_number(min.periodicity, "min.periodicity", min = 0)
  if (min.periodicity >= 1) .abort("{.arg min.periodicity} must be below 1 (it is an autocorrelation, which cannot exceed 1).")

  # default band edges (also used for axis selection even when bandpass.filter = FALSE)
  if (is.null(filter.low.freq))  filter.low.freq  <- min.freq.Hz * 0.9
  if (is.null(filter.high.freq)) filter.high.freq <- max.freq.Hz * 1.1

  # NOTE: no axis is warned about up front. Which axis carries the beat depends on the species, the tag
  # placement and the gait (in this package's own manta validation the wingbeat sat in surge, not sway),
  # so a blanket "sway is best" warning would be wrong as often as right. The 2f concern is handled where
  # it can actually be measured: cross-axis consensus during selection, plus the per-individual harmonic
  # flag raised from the data further below.

  if (bandpass.filter) {
    # validate filter frequencies

    # validate filter frequencies
    if (!is.numeric(filter.low.freq) || length(filter.low.freq) != 1 || filter.low.freq <= 0) {
      .abort("{.arg filter.low.freq} must be a single positive number.")
    }
    if (!is.numeric(filter.high.freq) || length(filter.high.freq) != 1 || filter.high.freq <= 0) {
      .abort("{.arg filter.high.freq} must be a single positive number.")
    }
    if (filter.low.freq >= filter.high.freq) {
      .abort("{.arg filter.low.freq} must be less than {.arg filter.high.freq}.")
    }

    # validate filter order
    if (!is.numeric(filter.order) || length(filter.order) != 1 || filter.order <= 0 || filter.order != round(filter.order)) {
      .abort("{.arg filter.order} must be a single positive integer.")
    }

    # warn about filter settings
    if (filter.low.freq > min.freq.Hz) {
      warning(paste("Filter low cutoff (", filter.low.freq, "Hz) is higher than min.freq.Hz (", min.freq.Hz, "Hz). This may remove frequencies of interest."))
    }
    if (filter.high.freq < max.freq.Hz) {
      warning(paste("Filter high cutoff (", filter.high.freq, "Hz) is lower than max.freq.Hz (", max.freq.Hz, "Hz). This may remove frequencies of interest."))
    }
  }

  # validate smoothing window
  if (!is.numeric(smooth.window) || length(smooth.window) != 1 || smooth.window < 0) .abort("{.arg smooth.window} must be a single non-negative number.")
  if (smooth.window > 60) warning(paste("Large smoothing window of", smooth.window, "seconds may obscure true tail beat patterns"))

  # validate max interpolation gap
  if (!is.null(max.interp.gap)) {
    if (!is.numeric(max.interp.gap) || length(max.interp.gap) != 1 || max.interp.gap <= 0) {
      .abort("{.arg max.interp.gap} must be {.code NULL} or a single positive number.")
    }
    if (max.interp.gap > 30) {
      warning(paste("Large max.interp.gap of", max.interp.gap, "seconds may lead to over-interpolation of missing data"))
    }
  }

  ##############################################################################
  # Initialize variables #######################################################
  ##############################################################################

  # calculate number of animals
  n_animals <- length(data)

  # header
  hdr_bullets <- sprintf("Input: %d tag%s", n_animals, if (n_animals != 1) "s" else "")
  if (!is.null(output.dir)) hdr_bullets <- c(hdr_bullets, paste0("Output: ", output.dir))
  # header config, one fact per line: the method(s) - naming BOTH and which is primary when cross-checking -
  # then the fixed analysis band and smoothing. These are shown once here, never repeated per deployment.
  # The swimming line only appears when classification is OFF: it is a run-wide setting, so stating it once
  # here is clearer than repeating "not classified" under every deployment.
  .log_header(lvl, "calculateTailBeats",
              paste0("Estimating tail beats from ", paste(motion.col, collapse = " or ")),
              bullets = hdr_bullets,
              arrow = c(
                if (length(methods) > 1L)
                  sprintf("Methods: %s (primary) + %s (validation)", methods[1], methods[2])
                else paste0("Method: ", method),
                if (bandpass.filter) sprintf("Bandpass: %g \u2013 %g Hz", filter.low.freq, filter.high.freq)
                else "Bandpass: none",
                if (smooth.window > 0) sprintf("Smoothing: %g s moving average", smooth.window) else "Smoothing: none",
                if (is.null(min.amplitude)) "Swimming: not classified (no min.amplitude)"))

  # graphics setup (active device for `plot`, single multi-page PDF for `plot.file`)
  caller_dev <- grDevices::dev.cur()
  if (plot && caller_dev == 1L) { grDevices::dev.new(); caller_dev <- grDevices::dev.cur() }
  if (plot) oldpar <- graphics::par(no.readonly = TRUE)
  file_dev <- NULL
  if (!is.null(plot.file)) {
    grDevices::pdf(plot.file, width = 14, height = 6)
    file_dev <- grDevices::dev.cur()
    on.exit(grDevices::dev.off(file_dev), add = TRUE)
  }
  if (plot) on.exit({ if (caller_dev %in% grDevices::dev.list()) { grDevices::dev.set(caller_dev); graphics::par(oldpar) } }, add = TRUE)
  draw_devices <- c(if (!is.null(file_dev)) file_dev, if (plot) caller_dev)

  # initialize list to hold results
  data_list <- vector("list", n_animals)
  saved <- vector("list", n_animals)
  # axis/QC caveats collected across the loop and each warned ONCE at the end (see the end-of-run block).
  # `towed_ids` holds bare IDs (the caveat is the same sentence for all of them); the other two hold
  # pre-formatted bullets, because there the per-deployment detail is what makes the warning actionable.
  towed_ids      <- character(0)
  disagree_items <- character(0)
  harmonic_items <- character(0)
  # per-deployment stats, filled in the loop and rolled up into the final SUMMARY block (NA where a
  # deployment produced no estimate). Vectors, index-aligned with the deployment order.
  co_id     <- rep(NA_character_, n_animals)   # deployment ID (for the grouped end-of-run warnings)
  co_freq   <- rep(NA_real_,      n_animals)   # median tail-beat frequency (primary backend)
  co_freq2  <- rep(NA_real_,      n_animals)   # median tail-beat frequency (cross-check backend)
  co_diff   <- rep(NA_real_,      n_animals)   # median |primary - cross-check| per sample
  co_axis   <- rep(NA_character_, n_animals)   # selected motion axis
  co_reason <- rep(NA_character_, n_animals)   # how the axis was chosen ("consensus" / "power")
  co_harm   <- rep(NA_character_, n_animals)   # a flagged 2f-harmonic alternative, if any
  co_agree  <- rep(NA_real_,      n_animals)   # method agreement (two backends), as a fraction
  co_swim   <- rep(NA_real_,      n_animals)   # fraction swimming (only when classified)
  co_edge   <- rep(NA_real_,      n_animals)   # fraction of estimates sitting on a band edge
  co_unres  <- rep(NA_real_,      n_animals)   # wavelet: fraction withheld for lack of peak prominence


  ##############################################################################
  # Validate Sampling Frequency for Frequency Estimation #######################
  ##############################################################################

  # check if sampling frequency is sufficient for the requested max frequency
  # we want at least 4 samples per cycle for reliable frequency estimation
  # (Nyquist would be 2, but we're conservative). The per-individual sampling rate and its Nyquist
  # headroom are reported in each tag's diagnostic block; only genuinely insufficient/marginal data
  # raises an abort/warning below.

  # retrieve data sampling frequencies
  data_hz <- unlist(lapply(data, function(dt) {

    # if data is file paths, we need to load the data first
    if (is_filepaths) dt <- readRDS(dt)

    # sampling frequency from the consolidated metadata (set by processTagData), else from timestamps
    .tagFs(dt, datetime.col)
  }))

  # Timestamps must strictly increase. A duplicated or out-of-order record is not a nuisance here: the
  # sampling rate still reads correctly (a zero gap is discarded before the median), but the signal
  # itself is stretched, so every cycle spans more samples than it should and every frequency comes out
  # too low -- a duplicated record halves them. Both backends read the same clock, so they agree with
  # each other and tbf_agree certifies the wrong answer. Nothing downstream can detect it.
  bad_time <- vapply(seq_along(data), function(k) {
    dt_k <- if (is_filepaths) readRDS(data[[k]]) else data[[k]]
    tv <- as.numeric(dt_k[[datetime.col]])
    any(diff(tv) <= 0, na.rm = TRUE)
  }, logical(1))
  if (any(bad_time)) {
    who <- if (!is.null(names(data))) names(data)[bad_time] else paste("Dataset", which(bad_time))
    .abort(c("{.arg {datetime.col}} must increase strictly, but does not for {.val {who}}.",
             "i" = "Duplicated or out-of-order timestamps stretch the signal, so every tail-beat frequency comes out too low (a duplicated record halves them) with nothing to reveal it.",
             "i" = "De-duplicate and sort the data first; {.fn regularizeTimeSeries} does both."))
  }

  # calculate Nyquist criteria
  nyquist_crit <- max.freq.Hz * 2
  recommended_crit <- max.freq.Hz * 4

  # identify problematic datasets. The bound is exclusive: at data_hz == nyquist_crit the requested
  # max.freq.Hz sits exactly ON Nyquist, where a sinusoid's amplitude is phase-dependent and its
  # frequency is not recoverable, so that case is insufficient rather than merely marginal.
  insufficient_hz <- data_hz <= nyquist_crit & !is.na(data_hz)
  marginal_hz <- data_hz > nyquist_crit & data_hz < recommended_crit & !is.na(data_hz)

  # generate message only for the most severe issue
  if (any(insufficient_hz)) {
    offenders <- which(insufficient_hz)
    offender_names <- if (!is.null(names(data))) names(data)[offenders] else {
      if (is_filepaths) basename(data[offenders]) else paste("Dataset", offenders)
    }
    offender_freqs <- data_hz[offenders]
    .abort(c(
      "Insufficient sampling frequency for {length(offenders)} dataset(s): {.val {paste0(offender_names, ' (', round(offender_freqs, 2), ' Hz)')}}.",
      "i" = "Nyquist requires >= {nyquist_crit} Hz to detect {max.freq.Hz} Hz (recommended >= {recommended_crit} Hz, 4x Nyquist).",
      "i" = "Reduce {.arg max.freq.Hz} to <= {round(min(data_hz, na.rm = TRUE) / 4, 2)} Hz, or use higher-frequency data."
    ))
  } else if (any(marginal_hz)) {
    marginal_names <- if (!is.null(names(data))) names(data)[marginal_hz] else {
      if (is_filepaths) basename(data[marginal_hz]) else paste("Dataset", which(marginal_hz))
    }
    warning(paste0(
      "Marginal sampling frequency for ", length(marginal_names), " dataset(s):\n",
      paste0("- ", marginal_names, ": ", data_hz[marginal_hz], " Hz", collapse = "\n"), "\n",
      "While above Nyquist rate (", nyquist_crit, " Hz), frequencies near ", max.freq.Hz,
      " Hz may be unreliable.\n",
      "Recommended minimum is ", recommended_crit, " Hz for robust analysis.\n",
      "Interpret high frequencies with caution or reduce max.freq.Hz parameter."
    ), call. = FALSE)
  }


  ##############################################################################
  # Perform Continuous Wavelet Transform (CWT) #################################
  ##############################################################################

  # process each individual (sequentially)
  for (i in seq_along(data)) {

    ############################################################################
    # load data for the current individual if using file paths #################
    if (is_filepaths) {

      # get current file path
      file_path <- data[i]

      # load current file
      individual_data <- readRDS(file_path)

      # perform checks specific to loaded RDS files
      .assert_columns(individual_data, c(id.col, datetime.col), sprintf("file '%s'", basename(file_path)))
      # A deployment with no motion column is NOT an error: .selectMotionAxis() already returns
      # axis = NA for it, and the skip path below emits the full NA schema, records the reason in the
      # audit trail and still saves. Aborting here only pre-empted that graceful path - and took the
      # rest of the batch with it.
      if (!inherits(individual_data[[datetime.col]], "POSIXct")) .abort("The datetime column in {.file {basename(file_path)}} must be of class {.cls POSIXct}.")
      if (is.null(attr(individual_data, "nautilus.version"))) {
        cli::cli_warn(c("File {.file {basename(file_path)}} was likely not processed via {.fn importTagData}.",
                        "i" = "Run it through {.fn importTagData} first to ensure correct formatting."))
      }

      ############################################################################
      # data is already in memory (list of data frames/tables) ###################
    } else {
      # access the individual dataset
      individual_data <- data[[i]]
    }

    # get ID
    id <- unique(individual_data[[id.col]])[1]
    co_id[i] <- id
    alt_edge <- NULL; alt_unres <- NULL      # per-deployment; never inherited from the previous one

    # per-individual sub-header (detailed level only)
    .log_h2(lvl, sprintf("%s (%d/%d)", id, i, n_animals))

    # choose the motion axis by cross-axis frequency consensus (robust to a tow-pendulum artefact that
    # would outscore true locomotion on raw in-band power -- see .selectMotionAxis)
    fs_i  <- .tagFs(individual_data, datetime.col)
    sel   <- .selectMotionAxis(individual_data, motion.col, fs_i, filter.low.freq, filter.high.freq)
    axis  <- sel$axis

    # skip if no usable motion data. Still emit the FULL column schema (all NA) and an audit-trail entry,
    # and still save, so a skipped individual is not silently missing columns, missing from the metadata,
    # or -- under return.data = FALSE -- missing from the returned file paths.
    if (is.na(axis) || all(is.na(individual_data[[axis]]))) {
      .log_skip(lvl, id, "  no valid motion data ", cli::symbol$bullet, " skipped")
      if (!data.table::is.data.table(individual_data)) individual_data <- data.table::as.data.table(individual_data)
      # the full schema for the requested backends, so a skipped deployment is still row-bindable
      for (m in methods) for (q in c("tbf_hz_", "tbf_amplitude_"))
        data.table::set(individual_data, j = paste0(q, m), value = NA_real_)
      data.table::set(individual_data, j = "tbf_swimming", value = NA_real_)
      if (length(methods) > 1L) data.table::set(individual_data, j = "tbf_agree", value = NA_real_)
      res_i <- .ensureMeta(individual_data)
      meta <- .getMeta(res_i)
      if (!is.null(meta)) {
        meta <- .appendProcessing(meta, "calculateTailBeats", method = paste(methods, collapse = " + "),
                                  axis = NA_character_, axis_selection = sel$reason,
                                  axis_harmonic_alt = sel$harmonic_alt %||% NA_character_,
                                  median_tbf_hz = NA_real_, median_amplitude = NA_real_,
                                  pct_swimming = NA_real_, pct_edge = NA_real_, pct_agree = NA_real_,
                                  note = "no valid motion data")
        res_i <- .restoreMeta(res_i, meta)
      }
      data_list[[i]] <- res_i
      names(data_list)[i] <- id
      saved[i] <- list(.saveOutput(res_i, id, output.dir = output.dir, output.suffix = output.suffix, compress = compress))
      .log_gap(lvl)
      next
    }

    # Collect the three axis-related risks here and raise each ONCE after the loop (see the end-of-run
    # block). All three share a diagnosis and a recommendation that do not vary by deployment, so warning
    # per tag would print the same paragraphs 50 times on a large batch - and R discards warnings past the
    # first 50, which would drop the tail of a genuinely long list. What DOES vary per deployment (which
    # axes, which frequencies, which alternative) is kept, as one bullet each.
    # (1) A towed tag can carry a tow-pendulum swing that dominates and biases the lateral (sway/yaw) axis,
    #     whether or not that axis was auto-selected.
    dep_type <- .getMeta(individual_data)$deployment$deployment_type
    if (!is.null(dep_type) && length(dep_type) == 1L && !is.na(dep_type) && tolower(dep_type) == "towed") {
      towed_ids <- c(towed_ids, id)
    }
    axis_hz <- function(a) {                        # this deployment's dominant frequency on axis `a`
      v <- suppressWarnings(sel$freqs[[a]])
      if (length(v) == 1L && is.finite(v)) sprintf("%.2f Hz", v) else "n/a"
    }
    # (2) Several candidate axes were supplied but none corroborate each other's dominant frequency, so the
    #     power-based pick may be an artefact rather than locomotion.
    if (isFALSE(sel$agree)) {
      disagree_items <- c(disagree_items, sprintf("%s: chose %s (%s)", id, axis,
        paste(sprintf("%s %.2f Hz", names(sel$freqs), sel$freqs), collapse = ", ")))
    }
    # (3) A possible 2f-harmonic pick: an axis peaks at ~half the chosen frequency. Cannot be resolved from
    # the signal (a lateral swimmer's fundamental is single-axis with a 2f pair; a ray wingbeat's fundamental
    # is the pair) so we surface it rather than guess -- the chosen frequency stands, the user decides.
    if (!is.na(sel$harmonic_alt)) {
      harmonic_items <- c(harmonic_items, sprintf("%s: chose %s at %s; %s peaks at %s", id, axis,
                                                  axis_hz(axis), sel$harmonic_alt, axis_hz(sel$harmonic_alt)))
    }

    # diagnostics (detailed level): input shape, the selected axis (and why), and the sampling-rate
    # headroom over Nyquist. The method-specific findings (bandpass / detection / smoothing) are
    # emitted next from within the per-method engine, so the whole block reads in pipeline order.
    # what we are working with, and the axis decision. The sampling rate lives on the input line only (the
    # separate Nyquist-headroom line was dropped: an inadequate rate is caught and reported by the up-front
    # Nyquist guard, so the routine case needs no reassurance). The analysis band and method are fixed
    # config already shown once in the header, so they are not repeated per deployment.
    if (lvl >= 2L) {
      n_in <- nrow(individual_data)
      dur  <- as.numeric(difftime(max(individual_data[[datetime.col]]), min(individual_data[[datetime.col]]), units = "secs"))
      .log_detail(lvl, sprintf("input: %s rows \u00b7 %g Hz \u00b7 %s", .formatLargeNumber(n_in), fs_i, .fmt_duration(dur)))
      # selected axis on the headline line; the per-axis peak evidence that justified it on a subordinate
      # sub-line (only when there were several candidates to weigh)
      present <- motion.col[motion.col %in% names(individual_data)]
      if (length(present) > 1L) {
        reason_txt <- switch(sel$reason, consensus = "consensus",
                             power = "power, axes disagree", "power")
        freq_txt <- paste(sprintf("%s %.2f", names(sel$freqs), sel$freqs), collapse = " \u00b7 ")
        .log_detail(lvl, sprintf("axis: %s (%s)", axis, reason_txt))
        .log_subdetail(lvl, sprintf("peak frequencies: %s Hz", freq_txt))
      } else {
        .log_detail(lvl, sprintf("axis: %s", axis))
      }
    }

    # estimate tail beats with the chosen method
    if (method == "peaks") {
      data_list[[i]] <- .runPeaks(
        dt = individual_data, animal_id = id, datetime.col = datetime.col, motion.col = axis, fs = fs_i,
        min.freq = min.freq.Hz, max.freq = max.freq.Hz, bandpass = bandpass.filter,
        filter.low = filter.low.freq, filter.high = filter.high.freq, filter.order = filter.order,
        min.amplitude = min.amplitude, smooth.window = smooth.window,
        min.periodicity = min.periodicity, suffix = method,
        draw.devices = draw_devices, lvl = lvl)
    } else {
      data_list[[i]] <- .runCWT(
        dt = individual_data, animal_id = id, id.col = id.col, datetime.col = datetime.col, motion.col = axis,
        min.freq.Hz = min.freq.Hz, max.freq.Hz = max.freq.Hz, bandpass.filter = bandpass.filter,
        filter.low.freq = filter.low.freq, filter.high.freq = filter.high.freq, filter.order = filter.order,
        min.amplitude = min.amplitude, smooth.window = smooth.window, max.interp.gap = max.interp.gap,
        ridge.prominence = ridge.prominence,
        plot.wavelet = plot.wavelet, plot.diagnostic = plot.diagnostic,
        fs = fs_i, suffix = method, draw.devices = draw_devices, lvl = lvl)
    }
    col_hz  <- paste0("tbf_hz_", method)          # this deployment's primary columns, named per backend
    col_amp <- paste0("tbf_amplitude_", method)

    # Cross-check against the second backend. The two are methodologically independent -- one works in
    # the time domain, one in the frequency domain -- so they fail on different signals, and where they
    # agree that agreement is evidence.
    #
    # The backends write their columns into the table they are given, so the cross-check cannot be handed
    # the deployment itself: it would overwrite the primary's estimate. It used to receive a full copy,
    # which on the largest records meant duplicating ~800 MB to read two columns from it. It now receives
    # only the two columns either backend actually reads - the motion axis and the timestamps - which
    # holds peak memory flat and costs nothing in accuracy: the vectors are the same objects, and `fs`
    # comes from the driver, so nothing is re-derived from a table that no longer carries the metadata.
    if (length(methods) > 1L) {
      alt_dt <- data.table::data.table(individual_data[[datetime.col]], individual_data[[axis]])
      data.table::setnames(alt_dt, c(datetime.col, axis))
      # Each backend draws its own diagnostic panel when plotting is on -- the wavelet spectrogram comes
      # only from .runCWT, so under the default (peaks primary) it would otherwise never appear. The
      # secondary logs nothing (lvl = 0L) to avoid duplicating the per-step console block.
      alt <- if (methods[2] == "peaks")
        .runPeaks(dt = alt_dt, animal_id = id, datetime.col = datetime.col,
                  motion.col = axis, fs = fs_i, min.freq = min.freq.Hz, max.freq = max.freq.Hz,
                  bandpass = bandpass.filter, filter.low = filter.low.freq, filter.high = filter.high.freq,
                  filter.order = filter.order, min.amplitude = min.amplitude,
                  smooth.window = smooth.window, min.periodicity = min.periodicity,
                  suffix = methods[2], draw.devices = draw_devices, lvl = 0L)
      else
        .runCWT(dt = alt_dt, animal_id = id, id.col = id.col,
                datetime.col = datetime.col, motion.col = axis, min.freq.Hz = min.freq.Hz,
                max.freq.Hz = max.freq.Hz, bandpass.filter = bandpass.filter,
                filter.low.freq = filter.low.freq, filter.high.freq = filter.high.freq,
                filter.order = filter.order, min.amplitude = min.amplitude,
                smooth.window = smooth.window, max.interp.gap = max.interp.gap,
                ridge.prominence = ridge.prominence,
                plot.wavelet = plot.wavelet, plot.diagnostic = plot.diagnostic,
                fs = fs_i, suffix = methods[2], draw.devices = draw_devices, lvl = 0L)
      alt_edge <- attr(alt, "tb_ridge_edge", exact = TRUE)
      alt_unres <- attr(alt, "tb_unresolved", exact = TRUE)
      # Both backends' estimates travel under their own names, including the cross-check's amplitude,
      # which used to be computed and discarded. Now that the two report the same measurand they are
      # directly comparable, so keeping it costs one column and answers a question the user could not
      # previously ask.
      alt_hz  <- paste0("tbf_hz_", methods[2])
      alt_amp <- paste0("tbf_amplitude_", methods[2])
      data.table::set(data_list[[i]], j = alt_hz,  value = alt[[alt_hz]])
      data.table::set(data_list[[i]], j = alt_amp, value = alt[[alt_amp]])
      data.table::set(data_list[[i]], j = "tbf_agree",
                      value = .tbAgreement(data_list[[i]][[col_hz]], alt[[alt_hz]]))
    }
    names(data_list)[i] <- id

    # record QC stats in the metadata audit trail (ensure a meta object exists first)
    res_i <- .ensureMeta(data_list[[i]])
    # default to a numeric NA when a column is absent: stats::median(NULL) returns NULL, and round(NULL)
    # would error ("non-numeric argument to mathematical function"), so guard the summary up front.
    tbf_v <- res_i[[col_hz]] %||% NA_real_
    amp_v <- res_i[[col_amp]]
    # NA unless the caller supplied a min.amplitude to classify against: swimming is not inferred from
    # the signal by default (see .classifyActivity), so pct_swimming is genuinely "not determined", not
    # zero. Never fall back to mean(!is.na(frequency)) -- that is a detection mask, not a behavioural rate.
    swim  <- mean(res_i$tbf_swimming, na.rm = TRUE)
    if (!is.finite(swim)) swim <- NA_real_

    # Two cross-checks with disjoint blind spots, so both are recorded. Edge occupancy catches a truth
    # that lies outside the band -- the case where the backends agree with each other and are both
    # wrong. Agreement catches an in-band contaminant, which edge occupancy cannot see. Neither sees a
    # contaminant strong enough to capture both backends from inside the band.
    # Checked on BOTH tracks, because the two backends express an out-of-band beat differently: the
    # wavelet clamps it to the nearest edge, while peak detection discards any beat whose interval
    # implies an out-of-band frequency. Only the former leaves a pile-up to find, so looking at the
    # primary alone would miss it whenever peak detection is primary.
    edges <- c(.tbEdgeOccupancy(tbf_v, min.freq.Hz, max.freq.Hz),
               if (length(methods) > 1L) .tbEdgeOccupancy(res_i[[paste0("tbf_hz_", methods[2])]], min.freq.Hz, max.freq.Hz),
               # the wavelet's own pre-mask ridge, whichever run produced it (primary or cross-check)
               attr(data_list[[i]], "tb_ridge_edge", exact = TRUE), alt_edge)
    edges <- edges[is.finite(edges)]                  # neither track estimated anything: nothing to judge
    edge_occ <- if (length(edges)) max(edges) else NA_real_
    agree <- if (!is.null(res_i$tbf_agree)) mean(res_i$tbf_agree, na.rm = TRUE) else NA_real_
    # NOTE: edge occupancy is only RECORDED here. The diagnosis and the fix are identical for every
    # affected deployment, so they are raised once after the loop (see below) instead of once per tag.

    # record this deployment's stats for the cohort roll-up (median is NA when nothing was estimated).
    # The cross-check backend's own median and the per-sample gap are collected here rather than inside
    # the detailed-verbosity block, because the SUMMARY reports them at "normal" verbosity too.
    co_freq[i]   <- stats::median(tbf_v, na.rm = TRUE)
    if (length(methods) > 1L) {
      alt_v <- res_i[[paste0("tbf_hz_", methods[2])]]
      co_freq2[i] <- stats::median(alt_v, na.rm = TRUE)
      co_diff[i]  <- stats::median(abs(tbf_v - alt_v), na.rm = TRUE)
    }
    co_axis[i]   <- axis
    co_reason[i] <- sel$reason
    co_harm[i]   <- sel$harmonic_alt %||% NA_character_
    co_agree[i]  <- agree
    co_swim[i]   <- swim
    co_edge[i]   <- edge_occ
    prim_unres   <- attr(data_list[[i]], "tb_unresolved", exact = TRUE)
    co_unres[i]  <- prim_unres %||% alt_unres %||% NA_real_
    # Both backends publish an unresolved share, and they are not the same measurement: the wavelet
    # withholds where no scale stands above the local spectral background, peak detection where the
    # waveform does not repeat strongly enough to fix a period. So each is keyed to the backend that
    # measured it and reported under that backend's own heading, rather than collapsed into one line.
    # The metadata keeps the primary's (above), unchanged.
    unres_by <- stats::setNames(rep(NA_real_, length(methods)), methods)
    unres_by[[method]] <- prim_unres %||% NA_real_
    if (length(methods) > 1L) unres_by[[methods[2]]] <- alt_unres %||% NA_real_

    meta <- .getMeta(res_i)
    if (!is.null(meta)) {
      meta <- .appendProcessing(meta, "calculateTailBeats",
                                method = paste(methods, collapse = " + "), axis = axis,
                                axis_selection = sel$reason,
                                axis_harmonic_alt = sel$harmonic_alt %||% NA_character_,
                                median_tbf_hz = round(stats::median(tbf_v, na.rm = TRUE), 3),
                                median_amplitude = if (!is.null(amp_v)) round(stats::median(amp_v, na.rm = TRUE), 3) else NA_real_,
                                primary_method = method,
                                cross_check_method = if (length(methods) > 1L) methods[2] else NA_character_,
                                median_tbf_hz_crosscheck = if (is.na(co_freq2[i])) NA_real_ else round(co_freq2[i], 3),
                                pct_swimming = round(100 * swim, 1),
                                pct_edge = round(100 * edge_occ, 1),
                                pct_unresolved = if (is.na(co_unres[i])) NA_real_ else round(100 * co_unres[i], 1),
                                pct_agree = if (is.na(agree)) NA_real_ else round(100 * agree, 1))
      res_i <- .restoreMeta(res_i, meta)
    }
    data_list[[i]] <- res_i

    # save the processed data (with the audit-trail entry just appended) when requested. Done here, in
    # the driver, rather than inside the per-method engine so the saved file includes the QC metadata
    # and the destination can be reported on the outcome line.
    saved_to <- .saveOutput(res_i, id, output.dir = output.dir, output.suffix = output.suffix, compress = compress)
    saved[i] <- list(saved_to)

    # `return.data = FALSE` promises a fleet too large for memory can be processed without ever holding
    # it all. Reading the inputs one at a time bounds only the input side; without this the RESULTS
    # accumulate and peak memory still grows with the fleet. The table is on disk by now, and nothing
    # below reads it back - .collectOutput ignores `data_list` entirely on this branch.
    if (!return.data) data_list[i] <- list(NULL)

    # per-ID outcome. Detailed (level 2): a behaviour / frequency / amplitude block, then a minimal tick.
    # Standard (level 1): one compact summary line (axis, median frequency, swimming, save destination).
    if (length(tbf_v) && any(!is.na(tbf_v))) {
      b <- cli::symbol$bullet
      med_f <- stats::median(tbf_v, na.rm = TRUE)
      if (lvl >= 2L) {
        # Grouped by BACKEND, in the order the function reasons: the primary estimate, then the backend
        # that validates it, then their agreement. Interleaving them by statistic instead (one frequency
        # line per method, then one amplitude line) made the reader hop between methods to assemble
        # either answer. Only the primary retains an amplitude -- the cross-check keeps frequency only.
        has_alt <- length(methods) > 1L && any(!is.na(res_i[[paste0("tbf_hz_", methods[2])]]))
        # the unit follows the axis: hardcoding "g" mislabels a gyro channel as an accelerometer one
        u <- .tbAxisUnits(axis)
        sections <- list(list(name = method, freq = tbf_v, amp = amp_v,
                              unresolved = unres_by[[method]], units = u))
        if (has_alt)
          sections <- c(sections, list(list(name = methods[2],
                                            freq = res_i[[paste0("tbf_hz_", methods[2])]],
                                            amp  = res_i[[paste0("tbf_amplitude_", methods[2])]],
                                            unresolved = unres_by[[methods[2]]], units = u)))
        .logTailBeatMethods(lvl, sections,
                            agree = if (has_alt) agree else NA_real_,
                            agree.diff = if (has_alt) co_diff[i] else NA_real_)
        # `swim` is NA exactly when no classification ran (min.amplitude unset) - a run-wide fact already
        # stated once in the header, so nothing is reported per deployment in that case. It stays a
        # top-level line: both backends share one classifier, so it belongs to neither section.
        if (!is.na(swim)) .log_detail(lvl, sprintf("swimming: %.0f%%", 100 * swim))
        if (!is.null(saved_to)) .log_ok(lvl, "saved ", basename(saved_to)) else .log_ok(lvl, id, " processed")
      } else {
        .log_ok(lvl, id, " ", b, " ", axis, " ", b, " median ", round(med_f, 2), " Hz",
                if (!is.na(swim)) paste0(" ", b, " ", round(100 * swim, 0), "% swimming"),
                if (!is.null(saved_to)) paste0(" ", b, " saved ", basename(saved_to)))
      }
    } else {
      .log_skip(lvl, id, "  no tail-beat signal detected")
    }
    .log_gap(lvl)
  }

  # End-of-run caveats. All fire regardless of verbosity (they are warnings) and each is raised ONCE for
  # the whole batch: the diagnosis and the recommendation are identical for every affected deployment, so
  # repeating them per tag only floods the console - and past 50 warnings R starts discarding them. They
  # are ordered by how directly they undermine the number that was reported: an axis chosen without
  # corroboration, then a frequency that may be double the true one, then a band that may be misplaced,
  # then the standing tow caveat.

  # (1) No cross-axis corroboration: the pick rests on in-band power alone.
  .warn_grouped(
    "Candidate motion axes disagree on their dominant frequency in {length(disagree_items)} deployment{?s}.",
    items = disagree_items,
    hints = c("The axis with the most in-band power was chosen, but no other axis corroborates it, so the estimate may reflect an artefact (e.g. tag wobble) rather than locomotion.",
              "Compare the per-axis frequencies below against the species' expected range before using these values."))

  # (2) Possible 2f-harmonic pick. The alternative axis differs per deployment, so it is named in the
  # bullet and the hint stays generic - the ambiguity itself cannot be resolved from the signal.
  .warn_grouped(
    "The selected axis may be a 2f harmonic in {length(harmonic_items)} deployment{?s}.",
    items = harmonic_items,
    hints = c("Another axis peaks near half the chosen frequency; for an animal that beats on a single axis (a laterally-swimming fish) that axis is likely the true tail-beat axis.",
              "Re-run with {.arg motion.col} set to the alternative named below to compare."))

  # (3) Band-edge pile-up. Here the per-deployment magnitude does carry information (a tag at 60% is a
  # different problem from one at 6%), so each affected deployment gets a bullet, worst first.
  edge_hit <- which(!is.na(co_edge) & co_edge > 0.05)
  if (length(edge_hit)) {
    edge_hit <- edge_hit[order(co_edge[edge_hit], decreasing = TRUE)]
    .warn_grouped(
      "Tail-beat estimates in {length(edge_hit)} deployment{?s} pile up against the frequency band limits ({.val {min.freq.Hz}}-{.val {max.freq.Hz}} Hz).",
      items = sprintf("%s: %.1f%% of estimates at a band edge", co_id[edge_hit], 100 * co_edge[edge_hit]),
      hints = c("A pile-up at an edge suggests the true frequency lies outside the searched range.",
                "Widen the band via {.arg min.freq.Hz} / {.arg max.freq.Hz}, or check {.arg motion.col} and the species' expected range."))
  }

  # (4) Tow-pendulum caveat. Unlike the others there is no per-deployment detail to report - the caveat is
  # the same sentence for every towed tag - so the IDs are named only when being towed distinguishes a
  # SUBSET of the batch. When every deployment is towed the list identifies nothing and is dropped.
  if (length(towed_ids)) {
    subject <- if (n_animals == 1L) "Deployment {.val {towed_ids}} is towed"
               else if (length(towed_ids) == n_animals) "All {n_animals} deployments are towed"
               else "{length(towed_ids)} of {n_animals} deployments are towed ({.val {towed_ids}})"
    cli::cli_warn(c(
      paste0(subject, ": a tow-pendulum oscillation can dominate the lateral (sway/yaw) axis and bias the reported tail-beat frequency."),
      "i" = "Treat these frequencies as estimates and cross-check against the other motion axes where available."))
  }


  ############################################################################
  # Return processed data ####################################################
  ############################################################################

  # final summary: the outcome tally and a cohort roll-up of the results, then the output/runtime footer
  if (lvl >= 1L) {
    .log_summary(lvl)
    .reportTailBeatCohort(lvl, n_animals, co_freq, co_freq2, co_axis, co_reason, co_harm,
                          co_agree, co_diff, co_swim, co_edge, methods)
    if (!is.null(output.dir)) .log_arrow(lvl, "output: ", output.dir)
    if (!is.null(plot.file)) .log_arrow(lvl, "plots: ", plot.file)
    .log_runtime(lvl, start.time)
  }


  # return the processed data in memory, or the paths of the files written
  ids <- sapply(data_list, function(x) unique(x[[id.col]])[1])
  .collectOutput(data_list, saved, return.data, ids)

}


#' Report one deployment's tail-beat results, grouped by backend.
#'
#' One renderer for every method combination, so the single-backend and cross-checked layouts cannot
#' drift apart: each section prints only the statistics its backend actually produced, and a section
#' whose backend produced nothing is omitted entirely rather than left as an empty heading.
#'
#' Values are aligned in a column (the labels differ in width by one character), which is what makes a
#' 51-deployment log scannable down the page rather than only readable block by block.
#'
#' @param sections List of per-backend records, in report order (primary first). Each has `name`, `freq`,
#'   `amp` (may be `NULL` -- only the primary backend retains one), `unresolved` and `units`.
#' @param agree Fraction of samples where the two backends concur, or `NA` for a single-backend run.
#' @param agree.diff Median absolute per-sample difference between the two estimates (Hz).
#' @keywords internal
#' @noRd
.logTailBeatMethods <- function(lvl, sections, agree = NA_real_, agree.diff = NA_real_) {
  if (lvl < 2L) return(invisible(NULL))
  # pad label + colon to a common width so the values line up under each other
  lab <- function(x) sprintf("%-12s", paste0(x, ":"))

  for (s in sections) {
    rows <- character(0)
    if (!is.null(s$freq) && any(!is.na(s$freq))) {
      r <- range(s$freq, na.rm = TRUE)
      rows <- c(rows, paste0(lab("frequency"), sprintf("median %.2f Hz (%.2f \u2013 %.2f Hz)",
                                                       stats::median(s$freq, na.rm = TRUE), r[1], r[2])))
    }
    if (!is.null(s$amp) && any(is.finite(s$amp))) {
      r <- range(s$amp, na.rm = TRUE)
      rows <- c(rows, paste0(lab("amplitude"), sprintf("median %.2f %s (%.2f \u2013 %.2f %s)",
                                                       stats::median(s$amp, na.rm = TRUE), s$units,
                                                       r[1], r[2], s$units)))
    }
    # Reported whenever the backend measured it, including 0%: in a per-backend block a missing row reads
    # as "not measured" rather than "nothing withheld", and the withheld share is part of the estimate's
    # coverage, not an exception to it.
    if (isTRUE(is.finite(s$unresolved)))
      rows <- c(rows, paste0(lab("unresolved"), sprintf("%.0f%%", 100 * s$unresolved)))

    if (!length(rows)) next
    .log_detail(lvl, s$name)
    for (r in rows) .log_subdetail_aligned(lvl, r)
  }

  # the comparison itself, subordinate to neither backend: how often the two concur (samples within 10%)
  # and, beneath it, the typical per-sample gap
  if (isTRUE(is.finite(agree))) {
    .log_detail(lvl, sprintf("agreement: %.0f%%", 100 * agree))
    if (isTRUE(is.finite(agree.diff)))
      .log_subdetail_aligned(lvl, sprintf("typical difference: %.2f Hz", agree.diff))
  }
  invisible(NULL)
}


#' Roll up a batch of tail-beat results into the final SUMMARY block.
#'
#' The outcome tally and the cohort frequency distribution are always shown; the axis-usage tally,
#' swimming, and the QC-flag rollup appear only when they apply - so a clean run stays short and a messy
#' one surfaces exactly what needs a look. Counts, not IDs: the grouped end-of-run warnings already name
#' the affected tags, so this is the cohort overview, not a repeat.
#'
#' Grouped the same way as the per-deployment block: one frequency line per backend, then the
#' cross-check beneath them, so the summary reads as the same shape at cohort scale.
#'
#' @param n_total Number of deployments the run was asked to process.
#' @param freq,freq2 Per-deployment median frequency for the primary and the cross-check backend
#'   (`freq2` all-NA for a single-backend run), NA where a deployment produced nothing.
#' @param agree,diff,swim,edge Per-deployment numeric vectors (method-agreement fraction; median
#'   per-sample frequency gap; swimming fraction; band-edge-occupancy fraction).
#' @param axis,reason,harm Per-deployment character vectors: the selected axis, how it was chosen
#'   ("consensus"/"power"), and a flagged 2f-harmonic alternative (NA when none).
#' @param methods The requested method vector, primary first (its length decides whether the
#'   cross-check block shows).
#' @keywords internal
#' @noRd
.reportTailBeatCohort <- function(lvl, n_total, freq, freq2, axis, reason, harm, agree, diff,
                                  swim, edge, methods) {
  if (lvl < 1L) return(invisible(NULL))
  b <- "\u00b7"                                        # middot separator
  sep <- paste0(" ", b, " ")
  has_est <- is.finite(freq)
  n_est <- sum(has_est)

  # outcome: how many tags yielded a tail-beat estimate
  if (n_est == n_total) {
    .log_done(lvl, sprintf("%d of %d tag%s processed", n_total, n_total, if (n_total != 1) "s" else ""))
  } else {
    .log_done(lvl, sprintf("%d tag%s processed %s %d with a tail-beat estimate (%d no signal)",
                           n_total, if (n_total != 1) "s" else "", b, n_est, n_total - n_est))
  }

  # One cohort distribution per backend, labelled by the backend that produced it. Both lines are padded
  # to a common width so the medians line up under each other, which is the only reason they go through
  # the verbatim variant - cli_text would collapse the padding.
  labs <- paste0(methods, " frequency:")
  w <- max(nchar(labs))
  for (k in seq_along(methods)) {
    v <- if (k == 1L) freq else freq2
    ok <- is.finite(v)
    if (!any(ok)) next
    lab <- sprintf("%-*s ", w, labs[k])
    if (sum(ok) == 1L) {
      .log_arrow_aligned(lvl, lab, sprintf("%.2f Hz", v[ok]))
    } else {
      q   <- stats::quantile(v[ok], c(0.25, 0.5, 0.75), names = FALSE)
      rng <- range(v[ok])
      .log_arrow_aligned(lvl, lab, sprintf("median %.2f Hz (IQR %.2f\u2013%.2f, range %.2f\u2013%.2f Hz)",
                                           q[2], q[1], q[3], rng[1], rng[2]))
    }
  }

  # axis usage - only when the cohort genuinely used more than one axis
  ax <- axis[!is.na(axis)]
  if (length(unique(ax)) > 1L) {
    tb <- sort(table(ax), decreasing = TRUE)
    .log_arrow(lvl, "axis used: ", paste(sprintf("%s %d", names(tb), as.integer(tb)), collapse = sep))
  }

  # the cross-check, beneath the two estimates it compares - only when two backends ran
  if (length(methods) > 1L && (any(is.finite(agree)) || any(is.finite(diff)))) {
    .log_arrow(lvl, "cross-check")
    if (any(is.finite(agree)))
      .log_subdetail(lvl, sprintf("agreement: median %.0f%%", 100 * stats::median(agree, na.rm = TRUE)),
                     min_level = 1L)
    if (any(is.finite(diff)))
      .log_subdetail(lvl, sprintf("typical difference: median %.2f Hz", stats::median(diff, na.rm = TRUE)),
                     min_level = 1L)
  }

  # swimming - only when classification was enabled (a min.amplitude was supplied)
  if (any(is.finite(swim))) {
    .log_arrow(lvl, sprintf("swimming: median %.0f%% across tags", 100 * stats::median(swim, na.rm = TRUE)))
  }

  # QC-flag rollup - counts of concerns already warned per deployment; only the non-zero ones, and the
  # whole line is omitted on a clean batch
  flags <- character(0)
  n_edge <- sum(edge > 0.05, na.rm = TRUE)
  if (n_edge > 0L) flags <- c(flags, sprintf("%d near band edge", n_edge))
  n_harm <- sum(!is.na(harm))
  if (n_harm > 0L) flags <- c(flags, sprintf("%d possible harmonic%s", n_harm, if (n_harm != 1) "s" else ""))
  n_pow <- sum(reason == "power", na.rm = TRUE)
  if (n_pow > 0L) flags <- c(flags, sprintf("%d axis chosen without consensus", n_pow))
  if (length(flags)) .log_attention(lvl, "flags: ", paste(flags, collapse = sep))

  invisible(NULL)
}



#######################################################################################################
# Per-individual driver for the CWT method ############################################################
#######################################################################################################

#' Run the Morlet CWT tail-beat method on one individual; writes tbf_hz_<suffix> / tbf_amplitude_<suffix>, draws the
#' diagnostic pages (if requested), and returns the data.table for the driver to save.
#'
#' The transform itself lives in `.cwtRidge` (R/utils-cwt.R), which batches internally with guard bands
#' and is therefore invariant to its block size. This function is only the per-individual plumbing:
#' band-pass, transform, smooth, interpolate short gaps, write columns, draw.
#' @inheritParams calculateTailBeats
#' @param dt Input data.table for one individual. @param animal_id ID of the current individual.
#' @param draw.devices Open device numbers to draw on. @param lvl Verbosity level.
#' @param suffix Backend name appended to the columns this run writes.
#' @return The input data.table with `tbf_hz_<suffix>` and `tbf_amplitude_<suffix>` added.
#' @keywords internal
#' @noRd
.runCWT <- function(dt, animal_id, id.col, datetime.col, motion.col, ridge.prominence = 2,
                    min.freq.Hz, max.freq.Hz, bandpass.filter, filter.low.freq,
                    filter.high.freq, filter.order, min.amplitude, smooth.window, max.interp.gap,
                    plot.wavelet, plot.diagnostic, fs = NULL, suffix = "wavelet",
                    draw.devices = integer(0), lvl = 1L) {

  # `fs` is normally supplied by the driver, which has already resolved it from the deployment's
  # metadata. It matters when this runs as the cross-check, where the input carries only the two columns
  # the backend reads and so has no metadata to resolve it from.
  if (is.null(fs)) fs <- .tagFs(dt, datetime.col)
  motion <- dt[[motion.col]]

  if (!any(is.finite(motion))) {
    cli::cli_warn("No finite {.val {motion.col}} data for ID {.val {animal_id}}; returning NA.")
    data.table::set(dt, j = paste0("tbf_hz_", suffix), value = NA_real_)
    data.table::set(dt, j = paste0("tbf_amplitude_", suffix), value = NA_real_)
    return(dt)
  }

  # Same band-pass primitive as the peak method: it filters within each finite run rather than
  # interpolating gaps away first, and it carries the numerical-stability assert. The CWT is itself a
  # constant-Q filter bank, but its out-of-band rejection is only ~-10 dB just outside the band, so the
  # filter is not redundant: without it an out-of-band low-frequency component drags the ridge onto the
  # band floor, and an out-of-band tonal one pins it to the ceiling on gliding rows.
  # the analysis band (config, identical every deployment) is reported once in the header, and the wavelet
  # internals (omega0, scale count, batches, COI masking) are diagnostics, not user-facing findings - so
  # neither is echoed per deployment. The per-deployment findings (axis, frequency, amplitude, swimming)
  # are logged by the driver.
  bp <- if (bandpass.filter) .bandpassSegments(motion, fs, filter.low.freq, filter.high.freq, filter.order)
        else motion - mean(motion, na.rm = TRUE)

  want_spec <- isTRUE(plot.wavelet) && length(draw.devices) > 0
  r <- .cwtRidge(bp, fs, min.freq.Hz, max.freq.Hz, prominence = ridge.prominence, spectrogram = want_spec)

  # Band-placement QC travels on the UNMASKED ridge. An out-of-band beat leaks through the filter and
  # pins the ridge to an edge; the prominence floor then (correctly) refuses to report it, so reading
  # edge occupancy off the masked column would silently retire the band-placement check. Published as
  # attributes rather than columns: they are per-deployment scalars, not per-sample data.
  data.table::setattr(dt, "tb_ridge_edge", .tbEdgeOccupancy(r$freq_raw, min.freq.Hz, max.freq.Hz))
  data.table::setattr(dt, "tb_unresolved", r$meta$pct_masked_prominence / 100)

  # Undo the band-pass attenuation before smoothing, at each sample's own estimated frequency. The
  # ridge is confined to [min.freq.Hz, max.freq.Hz], over which this correction is bounded, so it
  # cannot amplify noise without limit.
  amp_raw <- r$amp
  if (bandpass.filter && any(!is.na(amp_raw))) {
    gain_at <- seq(min.freq.Hz, max.freq.Hz, length.out = 512L)
    gain <- .bandpassPowerGain(gain_at, fs, filter.low.freq, filter.high.freq, filter.order)
    amp_raw <- amp_raw / stats::approx(gain_at, gain, xout = r$freq, rule = 2)$y
  }
  # .cwtRidge reports the SEMI-amplitude of the dominant oscillation (A in A*cos(wt)), which is the
  # natural output of the transform. Peak detection reports the peak-to-trough excursion, which is 2A.
  # Doubling here puts both backends on the same measurand, so `tbf_amplitude_peaks` and
  # `tbf_amplitude_wavelet` are directly comparable. Measured before this line: a tone of semi-amplitude
  # 2 gave 4.00 from peaks and 2.00 from the wavelet, under one column name whose meaning depended only
  # on which backend the caller happened to name first. The conversion lives here rather than in
  # .cwtRidge so that primitive stays a faithful CWT and keeps reporting what the transform measures.
  amp_raw <- 2 * amp_raw

  freq <- .smoothSeries(r$freq, smooth.window, fs)
  amp <- .smoothSeries(amp_raw, smooth.window, fs)

  # fill short gaps in the frequency track (e.g. single COI-masked samples between valid ones)
  if (!is.null(max.interp.gap) && isTRUE(max.interp.gap * fs > 0) && any(!is.na(freq))) {
    freq <- zoo::na.approx(freq, maxgap = max.interp.gap * fs, na.rm = FALSE)
  }

  # same activity classifier as the peak method, so the two agree on what "swimming" means (and, by
  # default, both leave it unclassified -- see .classifyActivity). The outcome is reported once by the
  # driver as the merged "swimming:" line, not here.
  g <- .classifyActivity(bp, fs, min.amplitude = min.amplitude)

  # null the frequency on not-swimming rows only when classification actually ran (see .runPeaks)
  glide <- !is.na(g$swimming) & !g$swimming
  freq[glide] <- NA_real_
  amp[glide] <- NA_real_

  # Backend-specific quantities carry the backend's name; tbf_swimming does not, because
  # .classifyActivity() runs on the shared band-passed signal and is bit-identical whichever backend
  # computed it - a suffix there would assert a dependence that does not exist.
  data.table::set(dt, j = paste0("tbf_hz_", suffix), value = freq)
  data.table::set(dt, j = paste0("tbf_amplitude_", suffix), value = amp)
  data.table::set(dt, j = "tbf_swimming", value = g$swimming)

  if (length(draw.devices) > 0) {
    pd <- list(id = animal_id, t = dt[[datetime.col]], fs = fs, bandpassed = bp,
               freq = freq, amp = amp, spec = r$meta$spectrogram,
               min.freq = min.freq.Hz, max.freq = max.freq.Hz,
               show.spec = isTRUE(plot.wavelet), show.diag = isTRUE(plot.diagnostic))
    for (d in draw.devices) { grDevices::dev.set(d); .drawTailBeatsCWT(pd) }
  }

  dt
}


#' Draw the CWT diagnostic pages for one individual: a spectrogram with the estimated ridge overlaid,
#' and a time-series panel of the band-passed signal, frequency and amplitude.
#' @keywords internal
#' @noRd
.drawTailBeatsCWT <- function(pd) {
  op <- graphics::par(no.readonly = TRUE)
  on.exit(graphics::par(op), add = TRUE)

  t_num <- as.numeric(pd$t)
  t0 <- t_num[1]

  # ---- page 1: spectrogram + ridge -------------------------------------------------------------
  if (isTRUE(pd$show.spec) && !is.null(pd$spec)) {
    P <- pd$spec$power
    # Column-normalised power on a log scale, with FIXED breaks. Quantile-equalised breaks paint a
    # fixed 1% of pixels in each colour by construction, so a record with no beat at all renders as
    # vividly as a strong one -- the plot becomes unable to show absence of signal. Normalising each
    # column by its own mean measures how PEAKED that column's spectrum is: flat (no beat) -> 0,
    # sharply peaked -> large, and the scale means the same thing in every record.
    cm <- colMeans(P, na.rm = TRUE)
    cm[!is.finite(cm) | cm <= 0] <- NA_real_
    Z <- log10(P / rep(cm, each = nrow(P)))
    Z[!is.finite(Z)] <- NA_real_
    brk <- seq(-1, 2, length.out = 101)
    Z[] <- pmin(pmax(Z, brk[1]), brk[length(brk)])

    graphics::par(mar = c(4.2, 5.2, 3.2, 7.5))
    # Draw on seconds-from-start, not absolute time. Adding the POSIXct epoch (~1.6e9) to a grid spaced
    # by fractions of a second loses the spacing to floating point, and image() rejects the result as
    # irregular -- which errored on every real record while passing on a synthetic one starting at 0.
    x_img <- pd$spec$time_s
    y_img <- log2(1 / pd$spec$freqs)                    # log2 period: even spacing for a log freq axis
    ord <- order(y_img)
    graphics::image(x = x_img, y = y_img[ord], z = t(Z[ord, , drop = FALSE]),
                    col = .viridis_pal(100), breaks = brk, xlab = "", ylab = "Frequency (Hz)",
                    axes = FALSE, useRaster = TRUE)
    ticks <- pretty(range(y_img), 6)
    ticks <- ticks[ticks >= min(y_img) & ticks <= max(y_img)]
    graphics::axis(2, at = ticks, labels = sprintf("%.2f", 1 / 2^ticks), las = 1)
    .tbTimeAxis(t0 + x_img, offset = t0)                # labels stay absolute; the grid stays regular
    graphics::box()

    # The ridge is drawn on the SAME time base as the image, sampled at the image's own columns, so it
    # cannot drift out of register with the surface it is describing.
    if (any(!is.na(pd$freq))) {
      idx <- pmax(1L, pmin(length(pd$freq), round(pd$spec$time_s * pd$fs) + 1L))
      graphics::lines(x_img, log2(1 / pd$freq[idx]), col = "red", lwd = 1.4)
    }
    graphics::title(main = sprintf("%s \u00b7 wavelet power (column-normalised) and dominant frequency", pd$id))
    .colorlegend(col = .viridis_pal(100), zlim = c(brk[1], brk[length(brk)]),
                 zval = c(-1, 0, 1, 2), zlab = c("0.1", "1", "10", "100"),
                 main = "peak / mean\npower", xpd = NA, posx = c(0.90, 0.92))
  }

  # ---- page 2: band-passed signal, frequency, amplitude ------------------------------------------
  if (isTRUE(pd$show.diag)) {
    graphics::par(mfrow = c(3, 1), mar = c(0.6, 5.2, 0.6, 7.5), oma = c(4.2, 0, 3.2, 0))

    d1 <- .decimateForPlot(t_num, pd$bandpassed, 3000L)
    graphics::plot(d1$x, d1$y, type = "l", col = "grey35", xlab = "", ylab = "Band-passed",
                   xaxs = "i", axes = FALSE)
    graphics::axis(2, las = 1); graphics::box()

    d2 <- .decimateForPlot(t_num, pd$freq, 3000L)
    graphics::plot(d2$x, d2$y, type = "l", col = "#1f78b4", xlab = "", ylab = "Frequency (Hz)",
                   xaxs = "i", axes = FALSE, ylim = c(pd$min.freq, pd$max.freq))
    graphics::axis(2, las = 1); graphics::box()

    d3 <- .decimateForPlot(t_num, pd$amp, 3000L)
    graphics::plot(d3$x, d3$y, type = "l", col = "#33a02c", xlab = "", ylab = "Amplitude",
                   xaxs = "i", axes = FALSE)
    graphics::axis(2, las = 1); .tbTimeAxis(t_num); graphics::box()

    graphics::mtext(sprintf("%s \u00b7 CWT diagnostics", pd$id), outer = TRUE, line = 1, cex = 1.1)
  }
}


#' Shared date axis for the CWT panels.
#' @keywords internal
#' @noRd
.tbTimeAxis <- function(x, offset = 0) {
  # the panel's x runs in shifted seconds, so the ticks are placed at `at - offset` while the labels
  # name the instants `at`; the zone is stated rather than inherited (see .axisTime)
  at <- pretty(range(x, na.rm = TRUE), 6)
  .axisTime(.asPosix(at, "UTC"), at = at - offset, fmt = "%d/%b %H:%M", tz = "UTC", las = 1)
}
