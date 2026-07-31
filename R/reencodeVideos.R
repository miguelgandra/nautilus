#######################################################################################################
# Re-encode camera-tag videos to HEVC #################################################################
#######################################################################################################

#' Re-encode camera-tag videos to a more compact format
#'
#' @description
#' Camera tags produce very large files - a single deployment can fill a disk - and the original
#' encoding is chosen for the camera's convenience rather than for storage. Re-encoding to HEVC, also
#' known as H.265, typically cuts the size substantially at visually equivalent quality, which matters
#' when an archive has to be kept for the life of a study.
#'
#' This function batch-converts every video in a directory with FFmpeg, skipping any whose output
#' already exists.
#'
#' @param mov.directory The directory holding the `.mov` or `.mp4` files to re-encode.
#' @param output.dir Where to write the re-encoded `.mp4` files. Defaults to `mov.directory`.
#' @param file.suffix A string appended to each output name, before `.mp4`, to keep the re-encoded files
#'   distinct from the originals when writing to the same directory. Default `""`.
#' @param encoder Which FFmpeg encoder to use. Default `"libx265"`, the portable software encoder.
#'   Hardware encoders are far faster where supported: `"hevc_videotoolbox"` on macOS, `"hevc_nvenc"` on
#'   NVIDIA, `"hevc_amf"` on AMD, `"hevc_qsv"` on Intel. List what your build offers with
#'   `ffmpeg -encoders`.
#' @param crf The constant rate factor for the **software** encoder, from 0 to 51, where lower means
#'   higher quality and a larger file. Default `18`, which is visually near-lossless for this material.
#'   Ignored by the hardware encoders.
#' @param video.quality The quality setting for the **hardware** encoders, from 1 to 100, where higher
#'   is better. Default `50`. Ignored by the software encoder.
#' @param preset How hard the software encoder works for a given quality: one of `"ultrafast"`,
#'   `"superfast"`, `"veryfast"`, `"faster"`, `"fast"`, `"medium"` (default), `"slow"`, `"slower"` or
#'   `"veryslow"`. Slower presets buy smaller files at the same quality, at a cost in time that is
#'   substantial across a whole archive.
#' @param overwrite Whether to re-encode files whose output already exists (default `FALSE`, which skips
#'   them). Leave it off so an interrupted batch can simply be re-run.
#' @param verbose How much detail to print: `0`/`"quiet"`, `1`/`"normal"`, or `2`/`"detailed"`
#'   (default).
#'
#' @details
#' Audio is dropped, since a camera tag's audio track is rarely of interest and removing it saves space.
#' If you need the audio, re-encode outside this function.
#'
#' Requires FFmpeg on the system path.
#'
#' @return The output file paths, invisibly.
#'
#' @seealso [getVideoMetadata()] for reading the timestamps of the resulting files;
#'   [renderOverlayVideo()] for compositing them with a sensor dashboard.
#'
#' @examples
#' \dontrun{
#' # portable software encoder, written beside the originals with a suffix
#' reencodeVideos("./videos/raw", file.suffix = "_hevc")
#'
#' # the much faster macOS hardware encoder, into a separate folder
#' reencodeVideos("./videos/raw", output.dir = "./videos/hevc",
#'                encoder = "hevc_videotoolbox")
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
