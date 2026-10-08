#######################################################################################################
# Re-encode camera-tag videos to HEVC #################################################################
#######################################################################################################

#' Re-encode camera-tag videos to HEVC
#'
#' @description
#' Batch re-encodes camera-tag video files using FFmpeg, with configurable software or hardware
#' encoding. Outputs are written as \code{.mp4} files with HEVC-compatible container tagging; audio
#' streams are discarded.
#'
#' Re-encoding can reduce storage requirements, but the default settings do not provide a lossless
#' archival copy or establish equivalence to the original footage. Retain source files and verify image
#' quality, timestamps and compatibility before using re-encoded videos for scientific annotation or analysis.
#'
#' @param mov.directory An existing directory containing \code{.mov} or \code{.mp4} source files.
#'   Extension matching is case-insensitive and subdirectories are not searched.
#' @param output.dir An existing directory in which output videos are written. Defaults to
#'   \code{mov.directory}; a separate output directory is recommended to avoid collisions and
#'   accidental re-encoding of previous outputs.
#' @param file.suffix String appended to each source-file stem before the output \code{.mp4} extension
#'   (default \code{""}). Use a suffix or a separate directory when processing \code{.mp4} sources,
#'   since a file is never re-encoded onto its own path.
#' @param encoder FFmpeg video encoder (default \code{"libx265"}, a software HEVC encoder). Recognised
#'   hardware choices are \code{"hevc_videotoolbox"}, \code{"hevc_nvenc"}, \code{"hevc_amf"} and
#'   \code{"hevc_qsv"}. The encoder must be available in the local FFmpeg build; hardware encoders
#'   additionally require compatible hardware and runtime support. Inspect \code{ffmpeg -encoders}
#'   before selecting an alternative.
#' @param crf Constant rate factor passed as \code{-crf} to encoders not recognised as hardware
#'   encoders. Must be between 0 and 51 (default \code{18}). For \code{libx265}, lower values generally
#'   retain more detail at the cost of larger files. Ignored for the four recognised hardware encoders.
#' @param video.quality Quality value passed as \code{-q:v} to the four recognised hardware encoders.
#'   Must be between 1 and 100 (default \code{50}). Its interpretation and effective range depend on
#'   the encoder; it is not a comparable quality scale across hardware backends. Ignored for other
#'   encoders.
#' @param preset Encoding preset: \code{"ultrafast"}, \code{"superfast"}, \code{"veryfast"},
#'   \code{"faster"}, \code{"fast"}, \code{"medium"} (default), \code{"slow"}, \code{"slower"} or
#'   \code{"veryslow"}. With \code{libx265}, this controls the encoding-effort and compression trade-off.
#'   The value is forwarded to every selected encoder without translation; hardware backends may
#'   ignore or reject these software-style presets.
#' @param overwrite Logical; whether to replace existing output files (default \code{FALSE}). Files
#'   whose output path equals the source path are always skipped, even with \code{TRUE}.
#' @param verbose How much detail to print: \code{0}/\code{"quiet"}, \code{1}/\code{"normal"}, or
#'   \code{2}/\code{"detailed"} (default).
#'
#' @details
#' ## Encoding and dependencies
#'
#' FFmpeg must be installed and discoverable on the system \code{PATH}. Encoding is sequential, with
#' one FFmpeg process per source file. The requested encoder is checked against the build's advertised
#' encoder list; this does not verify that a hardware encoder can run. There is no automatic encoder
#' fallback or translation of backend-specific quality controls.
#'
#' All outputs use the \code{hvc1} video tag and omit audio through \code{-an}. Select an HEVC encoder
#' compatible with the resulting MP4 container. The function does not request resizing or explicit
#' frame-rate conversion, and does not apply sensor/video clock corrections. It does not guarantee
#' preservation of all source container metadata.
#'
#' ## File naming and overwrite safeguards
#'
#' Output names are \code{<source stem><file.suffix>.mp4}. Existing outputs are skipped unless
#' \code{overwrite = TRUE}; skipped paths are not included in the returned vector. When input and
#' output directories are the same, an unsuffixed \code{.mp4} input is skipped because its destination
#' is identical to its source.
#'
#' Different source files sharing a stem, such as \code{clip.mov} and \code{clip.mp4}, can map to the
#' same output path. The same-path safeguard does not prevent overwriting another source with that
#' name. Use distinct source stems and preferably separate directories. Re-running in a directory
#' containing earlier outputs can also re-encode those outputs and append the suffix again.
#'
#' ## Failure handling and verification
#'
#' Missing FFmpeg, an unavailable encoder or a directory with no supported source files stops the
#' call. Individual encoding failures are reported in the console when verbosity permits, then the
#' batch continues. Failed attempts can leave partial output files; these are not removed or checked
#' for validity, and a later run with \code{overwrite = FALSE} skips any existing file.
#'
#' A successful result means FFmpeg returned success and the output file exists, not that image
#' quality or complete playback has been validated. Check outputs before replacing originals.
#' Re-run [getVideoMetadata()] for the new paths when integrating them into the video workflow.
#'
#' @return A character vector of newly encoded output paths, returned invisibly. Skipped and failed
#'   encodings are omitted; if none succeed, the result is \code{character(0)}. Writing video files is
#'   the principal side effect.
#'
#' @seealso [getVideoMetadata()], [launchVideo()], [renderOverlayVideo()].
#'
#' @examples
#' \dontrun{
#' # Both directories must already exist. Preserve the original footage.
#' encoded <- reencodeVideos(
#'   mov.directory = "./videos/raw",
#'   output.dir = "./videos/hevc",
#'   encoder = "libx265",
#'   crf = 18,
#'   preset = "medium")
#'
#' # Inspect outputs and retain the deployment ID when refreshing video metadata.
#' video.metadata <- getVideoMetadata(c(deployment_01 = "./videos/hevc"))
#'
#' # Alternatively, distinguish outputs written beside their sources.
#' reencodeVideos("./videos/raw", file.suffix = "_hevc")
#' }
#' @export

reencodeVideos <- function(mov.directory,
                           output.dir = mov.directory,
                           file.suffix = "",
                           encoder = "libx265",
                           crf = 18,
                           video.quality = 50,
                           preset = "medium",
                           overwrite = FALSE,
                           verbose = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  .assert_flag(overwrite, "overwrite")
  .assert_string(file.suffix, "file.suffix"); .assert_string(encoder, "encoder")
  .assert_number(crf, "crf", min = 0, max = 51)
  .assert_number(video.quality, "video.quality", min = 1, max = 100)
  .assert_choice(preset, "preset", c("ultrafast", "superfast", "veryfast", "faster", "fast", "medium", "slow", "slower", "veryslow"))
  .assert_dir(mov.directory, "mov.directory"); .assert_dir(output.dir, "output.dir")
  ffmpeg <- .ffmpegBin()
  if (!.hasEncoder(encoder)) .abort(c("Encoder {.val {encoder}} is not available in your ffmpeg build.",
                                      "i" = "List the available options with {.code ffmpeg -encoders}."))

  mov.directory <- path.expand(mov.directory); output.dir <- path.expand(output.dir)
  video_files <- list.files(mov.directory, pattern = "\\.(mov|mp4)$", full.names = TRUE, ignore.case = TRUE)
  if (!length(video_files)) .abort("No {.file .mov} or {.file .mp4} files found in {.file {mov.directory}}.")

  hardware <- encoder %in% c("hevc_videotoolbox", "hevc_nvenc", "hevc_amf", "hevc_qsv")
  .log_header(lvl, "reencodeVideos", "Re-encoding camera videos to HEVC",
              bullets = sprintf("Input: %d file%s in %s", length(video_files),
                                if (length(video_files) != 1) "s" else "", basename(mov.directory)),
              arrow = sprintf("Encoder: %s (%s) \u00b7 preset %s", encoder,
                              if (hardware) sprintf("quality %d", video.quality) else sprintf("crf %d", crf), preset))

  outputs <- character(0); n_done <- 0L; n_skip <- 0L
  for (i in seq_along(video_files)) {
    file <- video_files[i]
    out_file <- file.path(output.dir, paste0(tools::file_path_sans_ext(basename(file)), file.suffix, ".mp4"))
    .log_h2(lvl, sprintf("%s (%d/%d)", basename(file), i, length(video_files)))

    if (normalizePath(out_file, mustWork = FALSE) == normalizePath(file)) {
      .log_skip(lvl, "output path equals the source - set a {.arg file.suffix} or a different {.arg output.dir}")
      n_skip <- n_skip + 1L; next
    }
    if (file.exists(out_file) && !overwrite) {
      .log_skip(lvl, "output exists - skipping ({.code overwrite = TRUE} to replace)")
      n_skip <- n_skip + 1L; next
    }

    q_args <- if (hardware) c("-q:v", as.character(video.quality)) else c("-crf", as.character(crf))
    args <- c("-y", "-i", file, "-c:v", encoder, q_args, "-preset", preset, "-tag:v", "hvc1", "-an", out_file)
    if (lvl >= 2L) .log_detail(lvl, "encoding (this can take a while)")
    t0 <- Sys.time()
    status <- suppressWarnings(system2(ffmpeg, shQuote(args), stdout = FALSE, stderr = FALSE))
    if (status != 0 || !file.exists(out_file)) {
      .log_skip(lvl, "ffmpeg failed - left unencoded"); n_skip <- n_skip + 1L; next
    }
    .log_ok(lvl, basename(out_file), "  encoded ", cli::symbol$bullet, " ",
            sprintf("%.0f MB", file.size(out_file) / 1e6), " ", cli::symbol$bullet, " ",
            .fmt_duration(as.numeric(difftime(Sys.time(), t0, units = "secs"))))
    outputs <- c(outputs, out_file); n_done <- n_done + 1L
    .log_gap(lvl)
  }

  if (lvl >= 1L) {
    .log_summary(lvl)
    .log_done(lvl, n_done, " of ", length(video_files), " file", if (length(video_files) != 1) "s", " re-encoded",
              if (n_skip) sprintf(" (%d skipped)", n_skip))
    .log_runtime(lvl, start.time)
  }
  invisible(outputs)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
