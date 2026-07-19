#######################################################################################################
# Function to estimate tail beat frequencies ##########################################################
#######################################################################################################

#' Estimate tail-beat frequency and amplitude from motion data
#'
#' @description
#' Estimates the frequency and amplitude of the dominant in-band oscillation of a motion channel -- for
#' a swimming animal, its tail beat. Two backends are available and both run by default: peak detection,
#' which locates individual beats as peak/trough oscillations in the time domain, and a Morlet continuous
#' wavelet transform, which tracks the strongest in-band oscillation per sample. They fail on different
#' signals, so their agreement is reported alongside the estimate. Whether a given oscillation reflects
#' active swimming as opposed to gliding is treated as a separate, optional interpretation: it is
#' reported only when the caller supplies an amplitude reference (\code{min.amplitude}), and is
#' \code{NA} otherwise (see Details). One or many individuals can be processed
#' together, with optional diagnostic plots.
#'
#' @param data A list of data.tables/data.frames, one for each individual; a single aggregated data.table/data.frame
#' containing data from multiple animals (with an 'ID' column); or a character vector of file paths pointing to
#' `.rds` files, each containing data for a single individual. When a character vector is provided,
#' files are loaded sequentially to optimize memory use. The output of the \link{processTagData} function
#' is strongly recommended, as it formats the data appropriately for all downstream analysis.
#' @param method Character. One or both estimation backends. Naming both (the default) runs both and
#'   cross-checks them: the first fills \code{tbf_hz}, the second \code{tbf_hz_alt}, and their per-row
#'   agreement becomes \code{tbf_agree}. Name a single backend to skip the cross-check.
#'   \itemize{
#'     \item \code{"peaks"}: detects individual beats as peak/trough oscillations in the band-passed
#'       signal, yielding per-beat frequency, amplitude and timing. Fast and interpretable.
#'     \item \code{"wavelet"}: a Morlet continuous wavelet transform, tracking the strongest in-band
#'       oscillation per sample.
#'   }
#'   The two fail on different signals (see Details), so where they agree that agreement is evidence.
#' @param id.col Character. The name of the column identifying individuals (default: "ID").
#' @param datetime.col Character. The name of the column containing datetime information (default: "datetime").
#' @param motion.col Character. One or more candidate motion columns (default \code{"sway"}). When
#'   several are given, the axis is chosen per individual by \emph{cross-axis frequency consensus} (the
#'   axis whose dominant in-band frequency is corroborated by another axis, which rejects an axis-isolated
#'   artefact such as a tow-pendulum swing), falling back to plain in-band power -- with a warning -- when
#'   no two axes agree. Tail beats are cleanest on the lateral (\code{"sway"}) or yaw axis;
#'   \code{"surge"}/\code{"heave"} can carry energy at twice the beat frequency and trigger a warning.
#' @param min.amplitude Numeric or NULL (default). An absolute in-band amplitude-envelope threshold, in
#'   the units of \code{motion.col}, above which a sample is classified as actively swimming. Supplying
#'   it enables the swimming/gliding call (\code{tbf_swimming}); with \code{NULL} that call is withheld
#'   (\code{NA}) because it cannot be inferred reliably from the signal alone (see Details). It also sets
#'   peak-detection sensitivity for \code{method = "peaks"}.
#' @param min.freq.Hz Numeric. The lowest frequency of interest (in Hz). Default is 0.1 Hz.
#' @param max.freq.Hz Numeric. The highest frequency of interest (in Hz). Default is 3 Hz.
#' @param bandpass.filter Logical. Whether to band-pass filter the motion channel before detection.
#' Default is TRUE. Recommended, as it isolates the tail-beat band and improves signal quality.
#' @param filter.low.freq Numeric. Lower cutoff frequency for bandpass filter (in Hz).
#' If NULL (default), uses min.freq.Hz * 0.9 to allow some margin below the minimum frequency of interest.
#' @param filter.high.freq Numeric. Upper cutoff frequency for bandpass filter (in Hz).
#' If NULL (default), uses max.freq.Hz * 1.1 to allow some margin above the maximum frequency of interest.
#' @param filter.order Integer. Filter order for the Butterworth bandpass filter. Default is 4.
#' Higher orders provide steeper cutoffs but may introduce artifacts.
#' @param smooth.window Numeric. Window size (in seconds) for moving average smoothing of frequency estimates.
#' Set to 0 to disable smoothing. Default is 10 seconds.
#' @param max.interp.gap Numeric. Maximum gap (in seconds) for linear interpolation of missing frequency values.
#' Set to NULL to disable interpolation. Default is 10 seconds.
#' @param plot Logical. If `TRUE`, draw the diagnostic plots to the active graphics device. Default `FALSE`.
#' @param plot.file Character. Path to a single multi-page PDF in which to save the diagnostic plots
#'   (one page per individual per enabled panel). The parent directory must already exist; must end in
#'   `.pdf`. If `NULL` (default), no file is written. Independent of `plot`. The wavelet spectrogram is
#'   down-sampled to a page-friendly width so the PDF stays compact.
#' @param plot.wavelet Logical. Include the wavelet power-spectrum panel when plotting. Default TRUE.
#' @param plot.diagnostic Logical. Include the wavelet diagnostic panel (band-passed signal, frequency
#'   and amplitude through time) when plotting. Default TRUE.
#' @param verbose Verbosity level: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal" (header, per-ID
#'   outcome, summary), or `2`/"detailed" (default; adds low-level per-step diagnostics and progress
#'   bars). Defaults to `"detailed"`.
#' @param return.data Logical. Return the processed data in memory (default `TRUE`). When `FALSE`, the
#'   function instead returns the paths of the `.rds` files it wrote, which feed directly into the next
#'   step's `data` argument -- so a large fleet can be processed without ever holding it all in memory.
#'   `return.data = FALSE` therefore requires an `output.dir`.
#' @param output.dir Character. Directory in which to write one `<id>.rds` file per deployment. Providing
#'   a directory is what triggers saving; `NULL` (default) writes nothing. The directory must already exist.
#' @param output.suffix Character. Optional suffix appended to each saved file name (before `.rds`), e.g.
#'   to tag a processing run or avoid clashes. Only used when `output.dir` is set. Default `NULL`.
#' @param compress Compression for the saved `.rds` files (only used when `output.dir` is set): `TRUE`
#'   (default, gzip), `FALSE`, or one of `"gzip"`/`"bzip2"`/`"xz"`. See \code{\link[base]{saveRDS}}.
#'
#' @return If \code{return.data = TRUE} (default), the input data with added per-row columns:
#' \itemize{
#'   \item \code{tbf_hz}: estimated tail-beat frequency (Hz), NA where the animal is not beating.
#'   \item \code{tbf_amplitude}: an effort proxy in the units of \code{motion.col} -- the peak-to-trough
#'         excursion for \code{"peaks"}, the semi-amplitude of the dominant in-band oscillation for
#'         \code{"wavelet"}. Corrected for the band-pass attenuation.
#'   \item \code{tbf_swimming}: logical swimming/gliding flag when \code{min.amplitude} is supplied,
#'         otherwise \code{NA} (see Details).
#'   \item When both backends run: \code{tbf_hz_alt} (the second backend's frequency) and
#'         \code{tbf_agree} (whether the two agree within 10\%).
#' }
#' \code{tbf_agree} reads as a certificate, not an error flag: where the two backends agree they are
#' almost never both wrong, but where they disagree nothing identifies which one to distrust, so
#' \code{FALSE} means unresolved rather than bad.
#' For list/file inputs, returns a named list of modified data tables. Run-level QC (method, axis used,
#' median TBF and amplitude, percent swimming) is recorded in the metadata audit trail. If
#' \code{return.data = FALSE}, a character vector of the written \code{.rds} file paths.
#'
#' @details
#' \strong{Axis selection.} When several \code{motion.col} candidates are supplied, the axis is chosen per
#' individual by \emph{cross-axis frequency consensus}, not by raw in-band power. Coordinated propulsion
#' drives several axes at one frequency (a ray wingbeat appears on surge \emph{and} heave together),
#' whereas a non-locomotor artefact -- classically the tow-pendulum swing of a towed tag, a strong sharp
#' peak in the lateral axis a little below the true beat -- is confined to one axis. So the axis whose
#' dominant in-band frequency is corroborated by another axis is preferred (ties broken by in-band power);
#' if no two candidates agree, the choice falls back to plain in-band power and a warning is issued that
#' the estimate may reflect an artefact. On a video-ground-truthed reef-manta record, raw-power selection
#' picked the sway tow-pendulum (0.33 Hz) over the true surge wingbeat (0.44 Hz); the consensus rule
#' avoids it. Dead or near-dead candidate channels (a flat or stuck sensor axis) are excluded, so they
#' cannot form a spurious consensus. \strong{Harmonic ambiguity.} A laterally-swimming fish beats on a
#' single axis (sway) while surge and heave carry its \emph{second harmonic}, so a corroborated surge/heave
#' pair can be twice the true tail-beat rate. This spectrum is identical to a genuine ray-wingbeat
#' consensus contaminated by an artefact near half the beat, so it cannot be resolved from the signal
#' alone: rather than guess (and risk silently halving a correct estimate), the function keeps the chosen
#' axis and \emph{warns} whenever another axis carries a comparable peak at ~half the chosen frequency,
#' naming it as the likely fundamental for a single-axis swimmer. \strong{Towed tags:} when the deployment
#' metadata marks a tag as towed (\code{deployment_type == "towed"}), a warning flags that a tow-pendulum
#' oscillation may contaminate the lateral axis and bias the reported frequency, whichever axis is used.
#' The chosen axis, its selection basis (\code{single}/\code{consensus}/\code{power}), the per-axis peak
#' frequencies and any harmonic alternative are recorded in the processing metadata and the console log.
#'
#' \strong{Band-pass filtering.} A Butterworth band-pass of order \code{filter.order} is applied within
#' continuous data segments before detection, isolating the tail-beat band and removing low-frequency
#' posture drift and high-frequency noise. The band defaults to \code{min.freq.Hz} and \code{max.freq.Hz}
#' widened by a 10\% margin, or is set explicitly via \code{filter.low.freq} / \code{filter.high.freq}.
#' Disable it with \code{bandpass.filter = FALSE}.
#'
#' \strong{Peak detection} (\code{method = "peaks"}). Individual beats are located as successive
#' peak/trough pairs in the band-passed signal. Each beat contributes a frequency (from the peak-to-peak
#' interval) and an amplitude (the peak-to-trough excursion); every beat whose interval falls in
#' \code{[min.freq.Hz, max.freq.Hz]} is kept, and the values are mapped onto a per-row series. Detection
#' sensitivity (the minimum swing that counts as a peak) is data-driven and not user-tunable, so that the
#' frequency estimate does not depend on any behavioural threshold. Whether a beat counts as active
#' swimming is a separate step (see Swimming vs gliding), not part of detection.
#'
#' \strong{Wavelet transform} (\code{method = "wavelet"}). A Morlet continuous wavelet transform
#' (Torrence & Compo 1998) is computed over \code{[min.freq.Hz, max.freq.Hz]}, and the strongest scale at
#' each sample gives the dominant frequency, refined below the scale grid by a parabolic fit, together
#' with its absolute amplitude. \code{max.interp.gap} linearly fills short gaps. Estimates whose wavelet
#' support reaches a record end are masked (cone of influence). Long records are transformed in blocks
#' with guard bands, so the result does not depend on how the record is divided. Being a frequency-domain
#' method, it fails differently from peak detection: it is unbiased by harmonics, on which peak detection
#' can report a multiple of the true frequency, but it can be captured by a strong out-of-band or
#' low-frequency component, which peak detection shrugs off. Agreement between the two is therefore
#' informative.
#'
#' \strong{Swimming vs gliding.} This is deliberately \emph{not} inferred from the signal by default.
#' The function estimates the dominant in-band oscillation everywhere it can; deciding whether that
#' oscillation is a tail beat (the animal propelling) rather than tag motion during a glide requires
#' information a single sway axis does not carry. On a video-validated whale-shark towed-tag archive,
#' every self-referential rule tried -- a fraction of the beat amplitude, an out-of-band noise floor, an
#' Otsu split of the envelope, a spectral-flatness test -- failed: the tag oscillates in the tail-beat
#' band whether or not the animal propels, so human-annotated stationary feeding could not be separated
#' from active ram feeding, and swell, tag strum or a passing vessel were all reported as continuous
#' swimming. The failure held in clean, fully-submerged water, so it is not a removable surface artefact.
#'
#' Accordingly, with \code{min.amplitude = NULL} (the default) \code{tbf_swimming} is \code{NA}: the
#' honest answer. Supplying \code{min.amplitude} -- an absolute envelope threshold, ideally calibrated
#' for a species/tag combination or from validated footage -- turns the classification on: samples whose
#' band-passed amplitude envelope exceeds it are swimming (with hysteresis and a minimum-bout filter),
#' and \code{tbf_hz}/\code{tbf_amplitude} are then set to \code{NA} on gliding rows. \code{tbf_hz} and
#' \code{tbf_amplitude} are always reported when unclassified: a frequency is measurable even where a
#' behavioural call is not.
#'
#' \strong{Smoothing.} A centred moving average of \code{smooth.window} seconds is applied to the per-row
#' frequency (and, for peak detection, amplitude) series to suppress sample-to-sample jitter while
#' preserving sustained behaviour; set \code{smooth.window = 0} to disable. Both methods share the same
#' NA-aware smoother: gaps are averaged over rather than propagated across the window, and the leading and
#' trailing half-window of each record is left \code{NA} rather than padded with the end value.
#'
#' \strong{Output metrics.} For each individual the chosen axis, the median and range of tail-beat
#' frequency, the median and range of amplitude (peak detection), and the percentage of time classified
#' as swimming are reported in the console summary and recorded in the metadata audit trail.
#'
#' \strong{Sampling frequency.} The sampling rate must exceed twice \code{max.freq.Hz} (Nyquist); at
#' least four times is recommended for reliable estimates. For \code{max.freq.Hz = 3}, sample at >= 12 Hz.
#'
#' @note Both methods require the \pkg{signal} package for band-pass filtering.
#'
#' @references Torrence, C. & Compo, G.P. (1998) A Practical Guide to Wavelet Analysis.
#' \emph{Bulletin of the American Meteorological Society} 79(1):61-78.
#'
#' @examples
#' \dontrun{
#' # Processed tag data -> per-beat frequency, amplitude and a swimming/gliding flag.
#' tag <- processTagData(oriented)
#' tag <- calculateTailBeats(tag, method = "peaks", motion.col = "sway",
#'                           min.freq.Hz = 0.1, max.freq.Hz = 2.5)
#'
#' # Batch of saved deployments, with a diagnostic PDF.
#' calculateTailBeats(list.files("./processed", full.names = TRUE),
#'                    motion.col = "sway", plot.file = "./plots/tail_beats.pdf",
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
  # `method` may name one backend or both. Naming both runs both: the first fills tbf_hz, the second
  # tbf_hz_alt, and their per-row agreement becomes tbf_agree. The two are methodologically
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
      if (!any(motion.col %in% names(data[[nm]]))) {
        .abort("{.val {nm}}: none of the motion columns {.val {motion.col}} are present.")
      }
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

  # default band edges (also used for axis selection even when bandpass.filter = FALSE)
  if (is.null(filter.low.freq))  filter.low.freq  <- min.freq.Hz * 0.9
  if (is.null(filter.high.freq)) filter.high.freq <- max.freq.Hz * 1.1

  # tail beats appear most cleanly on the lateral (sway) / yaw axes; surge/heave often carry energy
  # at twice the beat frequency, which can bias the estimate
  if (any(motion.col %in% c("surge", "heave"))) {
    cli::cli_warn(c("{.arg motion.col} includes {.val {intersect(motion.col, c('surge','heave'))}}.",
                    "i" = "Tail beats are cleanest on {.val sway} (or yaw); surge/heave can report ~2x the true frequency."))
  }

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
  .log_header(lvl, "calculateTailBeats",
              paste0("Estimating tail beats from ", paste(motion.col, collapse = " or ")),
              bullets = hdr_bullets,
              arrow = c(
                if (length(methods) > 1L)
                  sprintf("Methods: %s (cross-checked, %s primary)", paste(methods, collapse = " + "), method)
                else paste0("Method: ", method),
                if (bandpass.filter) sprintf("Bandpass: %g \u2013 %g Hz", filter.low.freq, filter.high.freq)
                else "Bandpass: none",
                if (smooth.window > 0) sprintf("Smoothing: %g s moving average", smooth.window) else "Smoothing: none"))

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
  towed_ids <- character(0)   # collected across the loop, warned once at the end (see below)
  # per-deployment stats, filled in the loop and rolled up into the final SUMMARY block (NA where a
  # deployment produced no estimate). Vectors, index-aligned with the deployment order.
  co_freq   <- rep(NA_real_,      n_animals)   # median tail-beat frequency
  co_axis   <- rep(NA_character_, n_animals)   # selected motion axis
  co_reason <- rep(NA_character_, n_animals)   # how the axis was chosen ("consensus" / "power")
  co_harm   <- rep(NA_character_, n_animals)   # a flagged 2f-harmonic alternative, if any
  co_agree  <- rep(NA_real_,      n_animals)   # method agreement (two backends), as a fraction
  co_swim   <- rep(NA_real_,      n_animals)   # fraction swimming (only when classified)
  co_edge   <- rep(NA_real_,      n_animals)   # fraction of estimates sitting on a band edge


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
      if (!any(motion.col %in% names(individual_data))) .abort("None of the motion columns ({.val {motion.col}}) are present in {.file {basename(file_path)}}.")
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
      for (col in c("tbf_hz", "tbf_amplitude", "tbf_swimming")) data.table::set(individual_data, j = col, value = NA_real_)
      if (length(methods) > 1L) for (col in c("tbf_hz_alt", "tbf_agree")) data.table::set(individual_data, j = col, value = NA_real_)
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

    # Surface two axis-related risks (both are warnings, so they fire regardless of verbosity). (1) A towed
    # tag can carry a tow-pendulum swing that dominates and biases the lateral (sway/yaw) axis whether or
    # not it was auto-selected - the same caveat for every towed tag, so it is COLLECTED here and warned
    # once after the loop rather than repeated per deployment. (2) When several candidate axes were supplied
    # but none corroborate each other's dominant frequency, the power-based pick may be an artefact; that
    # one carries per-deployment specifics (which axes, which frequencies), so it stays inline.
    dep_type <- .getMeta(individual_data)$deployment$deployment_type
    if (!is.null(dep_type) && length(dep_type) == 1L && !is.na(dep_type) && tolower(dep_type) == "towed") {
      towed_ids <- c(towed_ids, id)
    }
    if (isFALSE(sel$agree)) {
      freq_str <- paste(sprintf("%s %.2f Hz", names(sel$freqs), sel$freqs), collapse = ", ")
      cli::cli_warn(c("{.val {id}}: candidate motion axes disagree on their dominant frequency ({freq_str}).",
        "i" = "Chose {.val {axis}} on in-band power alone, but no two axes corroborate one another, so the estimate may reflect an artefact (e.g. tag wobble) rather than locomotion."))
    }
    # A possible 2f-harmonic pick: an axis peaks at ~half the chosen frequency. Cannot be resolved from the
    # signal (a lateral swimmer's fundamental is single-axis with a 2f pair; a ray wingbeat's fundamental is
    # the pair) so we surface it rather than guess -- the chosen frequency stands, the user decides.
    if (!is.na(sel$harmonic_alt)) {
      cli::cli_warn(c("{.val {id}}: the chosen axis {.val {axis}} may be a 2f harmonic.",
        "i" = "Axis {.val {sel$harmonic_alt}} peaks near half its frequency; for an animal that beats on a single axis (a laterally-swimming fish) that is likely the true tail-beat axis. Re-run with {.code motion.col = \"{sel$harmonic_alt}\"} to compare."))
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
        draw.devices = draw_devices, lvl = lvl)
    } else {
      data_list[[i]] <- .runCWT(
        dt = individual_data, animal_id = id, id.col = id.col, datetime.col = datetime.col, motion.col = axis,
        min.freq.Hz = min.freq.Hz, max.freq.Hz = max.freq.Hz, bandpass.filter = bandpass.filter,
        filter.low.freq = filter.low.freq, filter.high.freq = filter.high.freq, filter.order = filter.order,
        min.amplitude = min.amplitude, smooth.window = smooth.window, max.interp.gap = max.interp.gap,
        plot.wavelet = plot.wavelet, plot.diagnostic = plot.diagnostic,
        draw.devices = draw_devices, lvl = lvl)
    }

    # Cross-check against the second backend. The two are methodologically independent -- one works in
    # the time domain, one in the frequency domain -- so they fail on different signals, and where they
    # agree that agreement is evidence. Runs on a copy: the backends write their columns into the table
    # they are given.
    if (length(methods) > 1L) {
      # Each backend draws its own diagnostic panel when plotting is on -- the wavelet spectrogram comes
      # only from .runCWT, so under the default (peaks primary) it would otherwise never appear. The
      # secondary logs nothing (lvl = 0L) to avoid duplicating the per-step console block.
      alt <- if (methods[2] == "peaks")
        .runPeaks(dt = data.table::copy(individual_data), animal_id = id, datetime.col = datetime.col,
                  motion.col = axis, fs = fs_i, min.freq = min.freq.Hz, max.freq = max.freq.Hz,
                  bandpass = bandpass.filter, filter.low = filter.low.freq, filter.high = filter.high.freq,
                  filter.order = filter.order, min.amplitude = min.amplitude,
                  smooth.window = smooth.window, draw.devices = draw_devices, lvl = 0L)
      else
        .runCWT(dt = data.table::copy(individual_data), animal_id = id, id.col = id.col,
                datetime.col = datetime.col, motion.col = axis, min.freq.Hz = min.freq.Hz,
                max.freq.Hz = max.freq.Hz, bandpass.filter = bandpass.filter,
                filter.low.freq = filter.low.freq, filter.high.freq = filter.high.freq,
                filter.order = filter.order, min.amplitude = min.amplitude,
                smooth.window = smooth.window, max.interp.gap = max.interp.gap,
                plot.wavelet = plot.wavelet, plot.diagnostic = plot.diagnostic,
                draw.devices = draw_devices, lvl = 0L)
      data.table::set(data_list[[i]], j = "tbf_hz_alt", value = alt$tbf_hz)
      data.table::set(data_list[[i]], j = "tbf_agree",
                      value = .tbAgreement(data_list[[i]]$tbf_hz, alt$tbf_hz))
    }
    names(data_list)[i] <- id

    # record QC stats in the metadata audit trail (ensure a meta object exists first)
    res_i <- .ensureMeta(data_list[[i]])
    # default to a numeric NA when a column is absent: stats::median(NULL) returns NULL, and round(NULL)
    # would error ("non-numeric argument to mathematical function"), so guard the summary up front.
    tbf_v <- res_i$tbf_hz %||% NA_real_
    amp_v <- res_i$tbf_amplitude
    # NA unless the caller supplied a min.amplitude to classify against: swimming is not inferred from
    # the signal by default (see .classifyActivity), so pct_swimming is genuinely "not determined", not
    # zero. Never fall back to mean(!is.na(tbf_hz)) -- that is a detection mask, not a behavioural rate.
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
               if (!is.null(res_i$tbf_hz_alt)) .tbEdgeOccupancy(res_i$tbf_hz_alt, min.freq.Hz, max.freq.Hz))
    edges <- edges[is.finite(edges)]                  # neither track estimated anything: nothing to judge
    edge_occ <- if (length(edges)) max(edges) else NA_real_
    agree <- if (!is.null(res_i$tbf_agree)) mean(res_i$tbf_agree, na.rm = TRUE) else NA_real_
    if (isTRUE(edge_occ > 0.05)) {
      cli::cli_warn(c("{round(100 * edge_occ, 1)}% of tail-beat estimates for ID {.val {id}} sit on a band edge.",
                      "i" = "Estimates piling up against an edge suggest the true frequency is outside {.val {min.freq.Hz}}-{.val {max.freq.Hz}} Hz.",
                      "i" = "Widen the band, or check {.arg motion.col} and the species' expected range."))
    }

    # record this deployment's stats for the cohort roll-up (median is NA when nothing was estimated)
    co_freq[i]   <- stats::median(tbf_v, na.rm = TRUE)
    co_axis[i]   <- axis
    co_reason[i] <- sel$reason
    co_harm[i]   <- sel$harmonic_alt %||% NA_character_
    co_agree[i]  <- agree
    co_swim[i]   <- swim
    co_edge[i]   <- edge_occ

    meta <- .getMeta(res_i)
    if (!is.null(meta)) {
      meta <- .appendProcessing(meta, "calculateTailBeats",
                                method = paste(methods, collapse = " + "), axis = axis,
                                axis_selection = sel$reason,
                                axis_harmonic_alt = sel$harmonic_alt %||% NA_character_,
                                median_tbf_hz = round(stats::median(tbf_v, na.rm = TRUE), 3),
                                median_amplitude = if (!is.null(amp_v)) round(stats::median(amp_v, na.rm = TRUE), 3) else NA_real_,
                                pct_swimming = round(100 * swim, 1),
                                pct_edge = round(100 * edge_occ, 1),
                                pct_agree = if (is.na(agree)) NA_real_ else round(100 * agree, 1))
      res_i <- .restoreMeta(res_i, meta)
    }
    data_list[[i]] <- res_i

    # save the processed data (with the audit-trail entry just appended) when requested. Done here, in
    # the driver, rather than inside the per-method engine so the saved file includes the QC metadata
    # and the destination can be reported on the outcome line.
    saved_to <- .saveOutput(res_i, id, output.dir = output.dir, output.suffix = output.suffix, compress = compress)
    saved[i] <- list(saved_to)

    # per-ID outcome. Detailed (level 2): a behaviour / frequency / amplitude block, then a minimal tick.
    # Standard (level 1): one compact summary line (axis, median frequency, swimming, save destination).
    if (length(tbf_v) && any(!is.na(tbf_v))) {
      b <- cli::symbol$bullet
      med_f <- stats::median(tbf_v, na.rm = TRUE)
      if (lvl >= 2L) {
        fr <- range(tbf_v, na.rm = TRUE)
        .log_detail(lvl, sprintf("frequency: median %.2f Hz (%.2f \u2013 %.2f Hz)", med_f, fr[1], fr[2]))
        # when both backends ran, surface the cross-check: how often they concur (samples within 10%), the
        # secondary method's own median, and the typical per-sample gap - the payoff of running both.
        if (length(methods) > 1L && !is.na(agree) && !is.null(res_i$tbf_hz_alt)) {
          alt_med  <- stats::median(res_i$tbf_hz_alt, na.rm = TRUE)
          diff_med <- stats::median(abs(res_i$tbf_hz - res_i$tbf_hz_alt), na.rm = TRUE)
          .log_detail(lvl, sprintf("agreement: %.0f%% \u00b7 %s median %.2f Hz (typical diff %.2f Hz)",
                                   100 * agree, methods[2], alt_med, diff_med))
        }
        if (!is.null(amp_v) && any(is.finite(amp_v))) {
          ar <- range(amp_v, na.rm = TRUE)
          # the unit follows the axis: hardcoding "g" mislabels a gyro channel as an accelerometer one
          u <- .tbAxisUnits(axis)
          .log_detail(lvl, sprintf("amplitude: median %.2f %s (%.2f \u2013 %.2f %s)",
                                   stats::median(amp_v, na.rm = TRUE), u, ar[1], ar[2], u))
        }
        # one line for swimming, merging the old "activity" (how it was classified) and "behaviour"
        # (the resulting %). `swim` is NA exactly when no classification ran (min.amplitude unset), so the
        # old, confusing "NA% swimming" becomes an explicit "not classified".
        .log_detail(lvl, if (is.na(swim)) "swimming: not classified (set min.amplitude)"
                         else sprintf("swimming: %.0f%%", 100 * swim))
        if (!is.null(saved_to)) .log_ok(lvl, "saved ", basename(saved_to)) else .log_ok(lvl, id, " processed")
      } else {
        swim_txt <- if (is.na(swim)) "swimming n/a" else paste0(round(100 * swim, 0), "% swimming")
        .log_ok(lvl, id, " ", b, " ", axis, " ", b, " median ", round(med_f, 2), " Hz ", b, " ", swim_txt,
                if (!is.null(saved_to)) paste0(" ", b, " saved ", basename(saved_to)))
      }
    } else {
      .log_skip(lvl, id, "  no tail-beat signal detected")
    }
    .log_gap(lvl)
  }

  # one consolidated tow-pendulum caveat for every towed deployment, rather than an identical warning per
  # tag (which floods the console on a large batch). Fires regardless of verbosity, like any warning.
  if (length(towed_ids)) {
    cli::cli_warn(c(
      "{length(towed_ids)} towed deployment{?s} ({.val {towed_ids}}): a tow-pendulum oscillation can dominate the lateral (sway/yaw) axis and bias the reported tail-beat frequency.",
      "i" = "Treat these as estimates and cross-check the axes."))
  }


  ############################################################################
  # Return processed data ####################################################
  ############################################################################

  # final summary: the outcome tally and a cohort roll-up of the results, then the output/runtime footer
  if (lvl >= 1L) {
    .log_summary(lvl)
    .reportTailBeatCohort(lvl, n_animals, co_freq, co_axis, co_reason, co_harm, co_agree, co_swim, co_edge, methods)
    if (!is.null(output.dir)) .log_arrow(lvl, "output: ", output.dir)
    if (!is.null(plot.file)) .log_arrow(lvl, "plots: ", plot.file)
    .log_runtime(lvl, start.time)
  }


  # return the processed data in memory, or the paths of the files written
  ids <- sapply(data_list, function(x) unique(x[[id.col]])[1])
  .collectOutput(data_list, saved, return.data, ids)

}


#' Roll up a batch of tail-beat results into the final SUMMARY block.
#'
#' The outcome tally and the cohort frequency distribution are always shown; the axis-usage tally, method
#' agreement, swimming, and the QC-flag rollup appear only when they apply - so a clean run stays short and
#' a messy one surfaces exactly what needs a look. Counts, not IDs: the per-deployment warnings already
#' name the individual tags (towed, harmonic, band-edge), so this is the cohort overview, not a repeat.
#'
#' @param n_total Number of deployments the run was asked to process.
#' @param freq,agree,swim,edge Per-deployment numeric vectors (median frequency; method-agreement fraction;
#'   swimming fraction; band-edge-occupancy fraction), NA where a deployment produced nothing.
#' @param axis,reason,harm Per-deployment character vectors: the selected axis, how it was chosen
#'   ("consensus"/"power"), and a flagged 2f-harmonic alternative (NA when none).
#' @param methods The requested method vector (its length decides whether the agreement line shows).
#' @keywords internal
#' @noRd
.reportTailBeatCohort <- function(lvl, n_total, freq, axis, reason, harm, agree, swim, edge, methods) {
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

  # cohort frequency distribution (across the tags that produced an estimate)
  if (n_est == 1L) {
    .log_arrow(lvl, sprintf("tail-beat frequency: %.2f Hz", freq[has_est]))
  } else if (n_est > 1L) {
    q   <- stats::quantile(freq[has_est], c(0.25, 0.5, 0.75), names = FALSE)
    rng <- range(freq[has_est])
    .log_arrow(lvl, sprintf("tail-beat frequency: median %.2f Hz (IQR %.2f\u2013%.2f, range %.2f\u2013%.2f Hz)",
                            q[2], q[1], q[3], rng[1], rng[2]))
  }

  # axis usage - only when the cohort genuinely used more than one axis
  ax <- axis[!is.na(axis)]
  if (length(unique(ax)) > 1L) {
    tb <- sort(table(ax), decreasing = TRUE)
    .log_arrow(lvl, "axis used: ", paste(sprintf("%s %d", names(tb), as.integer(tb)), collapse = sep))
  }

  # method agreement - only when two backends ran
  if (length(methods) > 1L && any(is.finite(agree))) {
    .log_arrow(lvl, sprintf("method agreement: median %.0f%% (%s vs %s)",
                            100 * stats::median(agree, na.rm = TRUE), methods[1], methods[2]))
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

#' Run the Morlet CWT tail-beat method on one individual; writes tbf_hz / tbf_amplitude, draws the
#' diagnostic pages (if requested), and returns the data.table for the driver to save.
#'
#' The transform itself lives in `.cwtRidge` (R/utils-cwt.R), which batches internally with guard bands
#' and is therefore invariant to its block size. This function is only the per-individual plumbing:
#' band-pass, transform, smooth, interpolate short gaps, write columns, draw.
#' @inheritParams calculateTailBeats
#' @param dt Input data.table for one individual. @param animal_id ID of the current individual.
#' @param draw.devices Open device numbers to draw on. @param lvl Verbosity level.
#' @return The input data.table with `tbf_hz` and `tbf_amplitude` added.
#' @keywords internal
#' @noRd
.runCWT <- function(dt, animal_id, id.col, datetime.col, motion.col,
                    min.freq.Hz, max.freq.Hz, bandpass.filter, filter.low.freq,
                    filter.high.freq, filter.order, min.amplitude, smooth.window, max.interp.gap,
                    plot.wavelet, plot.diagnostic, draw.devices = integer(0), lvl = 1L) {

  fs <- .tagFs(dt, datetime.col)
  motion <- dt[[motion.col]]

  if (!any(is.finite(motion))) {
    cli::cli_warn("No finite {.val {motion.col}} data for ID {.val {animal_id}}; returning NA.")
    data.table::set(dt, j = "tbf_hz", value = NA_real_)
    data.table::set(dt, j = "tbf_amplitude", value = NA_real_)
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
  r <- .cwtRidge(bp, fs, min.freq.Hz, max.freq.Hz, spectrogram = want_spec)

  # Undo the band-pass attenuation before smoothing, at each sample's own estimated frequency. The
  # ridge is confined to [min.freq.Hz, max.freq.Hz], over which this correction is bounded, so it
  # cannot amplify noise without limit.
  amp_raw <- r$amp
  if (bandpass.filter && any(!is.na(amp_raw))) {
    gain_at <- seq(min.freq.Hz, max.freq.Hz, length.out = 512L)
    gain <- .bandpassPowerGain(gain_at, fs, filter.low.freq, filter.high.freq, filter.order)
    amp_raw <- amp_raw / stats::approx(gain_at, gain, xout = r$freq, rule = 2)$y
  }

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

  data.table::set(dt, j = "tbf_hz", value = freq)
  data.table::set(dt, j = "tbf_amplitude", value = amp)
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
  at <- pretty(range(x, na.rm = TRUE), 6)
  graphics::axis(1, at = at - offset,                   # tick positions in the panel's own coordinates
                 labels = strftime(as.POSIXct(at, origin = "1970-01-01", tz = "UTC"),
                                   "%d/%b %H:%M", tz = "UTC"), las = 1)
}
