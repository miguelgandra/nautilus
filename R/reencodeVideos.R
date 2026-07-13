#######################################################################################################
# Re-encode camera-tag videos to HEVC #################################################################
#######################################################################################################

#' Re-encode camera-tag videos to HEVC (H.265)
#'
#' @description
#' Batch re-encodes the `.mov`/`.mp4` video files in a directory to the more space-efficient HEVC
#' (H.265) format with FFmpeg, for archival or downstream processing. A hardware encoder
#' (`hevc_videotoolbox`, `h265_nvenc`, ...) is much faster when available; the software encoder
#' (`libx265`) is the portable default. Existing outputs are skipped unless `overwrite = TRUE`.
#'
#' @param mov.directory Directory containing the `.mov`/`.mp4` files to re-encode.
#' @param output.dir Directory for the re-encoded `.mp4` files. Defaults to `mov.directory`.
#' @param file.suffix Character string appended to each output base name (before `.mp4`), e.g. to keep
#'   the re-encoded files distinct from the originals when writing to the same directory. Default `""`.
#' @param encoder FFmpeg video encoder. Default `"libx265"` (software). Hardware options such as
#'   `"hevc_videotoolbox"` (macOS), `"h265_nvenc"` (NVIDIA), `"hevc_amf"` (AMD) or `"hevc_qsv"` (Intel)
#'   are faster when supported. List the available encoders with `ffmpeg -encoders`.
#' @param crf Constant Rate Factor (0-51; lower = higher quality / larger). Used by the **software**
#'   encoder (`libx265`). Default 18.
#' @param video.quality Quality (1-100; higher = better). Used by the **hardware** encoders. Default 50.
#' @param preset Encoding speed/compression preset, one of `"ultrafast"`, `"superfast"`, `"veryfast"`,
#'   `"faster"`, `"fast"`, `"medium"` (default), `"slow"`, `"slower"`, `"veryslow"`.
#' @param overwrite Logical. If `FALSE` (default), files whose output already exists are skipped; if
#'   `TRUE`, they are re-encoded and replaced.
#' @param verbose Verbosity: `FALSE`/`0`/"quiet", `TRUE`/`1`/"normal", or `2`/"detailed" (default).
#'
#' @details
#' Commonly used HEVC encoders: **libx265** (portable software), **hevc_videotoolbox** (macOS hardware),
#' **h265_nvenc** (NVIDIA), **hevc_amf** (AMD), **hevc_qsv** (Intel Quick Sync). Audio is dropped
#' (`-an`) for space-efficient archival.
#'
#' @return The vector of output file paths, invisibly.
#' @seealso \link{getVideoMetadata}, \link{renderOverlayVideo}.
#' @examples
#' \dontrun{
#' # Portable software HEVC, written beside the originals with a suffix
#' reencodeVideos("./videos/raw", file.suffix = "_hevc")
#'
#' # Faster macOS hardware encoder into a separate output folder
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

  hardware <- encoder %in% c("hevc_videotoolbox", "h265_nvenc", "hevc_amf", "hevc_qsv")
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
