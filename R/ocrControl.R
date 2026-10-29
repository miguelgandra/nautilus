#' Timestamp-recognition settings for getVideoMetadata()
#'
#' @description
#' Groups the settings [getVideoMetadata()] uses when it has to read a recording time off the picture,
#' from the clock a camera burns into its own footage.
#'
#' This is a fallback, not the normal path. The recording time is taken from the file name whenever a
#' camera writes one there, because that is exact, costs nothing and does not depend on the video at all.
#' Reading the screen is for cameras that write no such name, and for the optional cross-check. Nothing
#' here is consulted otherwise.
#'
#' @param model Which Tesseract model to use, trained on the overlay font. Default `"cam"`, the
#'   fine-tuned camera-tag model, downloaded on first use by [installCamOcrModel()]. Pass `"eng"`, or
#'   any other installed model, to skip that download at some cost in accuracy on this particular font.
#' @param box Where the timestamp sits in the frame, as `c(x, y, width, height)` in pixels, with `x` and
#'   `y` the top-left corner. The coordinates are read relative to `frame.height` and rescaled for
#'   videos of a different resolution, so one setting covers every resolution of the same camera. Default
#'   `c(3249, 2120, 325, 28)`, the bottom-right box of the 4K camera overlay. Change it for a camera that
#'   draws its clock somewhere else - grab a frame and read the pixel coordinates off it.
#' @param frame.height The frame height, in pixels, that `box` was measured against. Default `2160`.
#' @param search.radius How far, in pixels, to search around `box` for the bright timestamp panel
#'   (default `80`). This absorbs the small drift in overlay position between cameras and firmware
#'   versions, so a box measured on one unit still works on its siblings. Widen it if the clock moves
#'   more than that; too wide and the search can lock onto some other bright rectangle.
#' @param max.search.frames How many frames to try before giving up on a video (default `10`). The first
#'   frame of a clip is often black or half-exposed, which is what this exists for.
#' @param char.whitelist The characters the recogniser is allowed to return. `NULL` (default) uses the
#'   package's own alphabet - digits, the letters that spell the month abbreviations, and the few
#'   punctuation marks a timestamp needs - which is already restrictive enough for this job. Override
#'   it only for a camera whose clock uses a different format, and remember that the month
#'   abbreviation is read as letters, so a digits-only whitelist will break the parse.
#'
#' @return A validated `nautilus_ocr` object for the `ocr` argument of [getVideoMetadata()].
#'
#' @seealso [getVideoMetadata()] for the function that consumes it; [installCamOcrModel()] for
#'   pre-fetching the default model.
#'
#' @examples
#' ocrControl(box = c(120, 40, 300, 26), frame.height = 1080)   # 1080p camera, overlay top-left
#' @export
ocrControl <- function(model = "cam",
                       box = c(3249, 2120, 325, 28),
                       frame.height = 2160,
                       search.radius = 80,
                       max.search.frames = 10,
                       char.whitelist = NULL) {
  .assert_string(model, "ocr$model")
  if (!is.numeric(box) || length(box) != 4L || anyNA(box))
    .abort("{.arg ocr$box} must be a numeric vector {.code c(x, y, width, height)} of length 4.")
  if (any(box[1:2] < 0) || any(box[3:4] <= 0))
    .abort("{.arg ocr$box} must have non-negative {.code x}/{.code y} and positive {.code width}/{.code height}.")
  .assert_number(frame.height, "ocr$frame.height", min = 1)
  .assert_number(search.radius, "ocr$search.radius", min = 0)
  .assert_count(max.search.frames, "ocr$max.search.frames", min = 1L)
  .assert_string(char.whitelist, "ocr$char.whitelist", null_ok = TRUE)
  structure(list(model = model, box = as.numeric(box), frame.height = frame.height,
                 search.radius = search.radius, max.search.frames = max.search.frames,
                 char.whitelist = char.whitelist),
            class = "nautilus_ocr")
}
