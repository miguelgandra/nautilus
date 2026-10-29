#' Define custom geometry for the video renderer
#'
#' @description
#' Creates a validated, untextured schematic model for [renderOverlayVideo()].
#' This interface accepts geometry, not OBJ/glTF files or skeletal rigs.
#' Coordinates must use the animal's body frame: x forward, y right, z down.
#'
#' @param vertices Numeric matrix with one vertex per row and three columns (x, y, z).
#'   At most 5000 vertices are supported.
#' @param faces Numeric matrix with three vertex indices per row, or a list of index
#'   vectors of length three. Indices are one-based. At most 2000 triangles are supported.
#'   Use consistent winding; self-intersecting meshes are outside this schematic renderer's scope.
#' @param part Optional character vector, one label per face. `"fin"` receives a darker
#'   outline; other labels receive body styling. Default `NULL` uses `"body"`.
#' @param motion Optional deformation profile: `"none"` (default), `"lateral"`,
#'   `"vertical"`, or `"flippers"`. Lateral/vertical profiles bend around the local
#'   z/y axis. Flippers rotate around x with opposite rotation signs on the two sides.
#' @param weights Numeric vector, one value per vertex between zero and one. Required for an
#'   animated profile. Zero fixes a vertex in the rest pose; larger weights increase
#'   its rotation and phase delay. Supply weights explicitly to identify moving geometry.
#' @param pivot Numeric (x, y, z) rotation pivot in the input coordinates. For
#'   `"flippers"`, supply the right-side pivot; its y coordinate is mirrored for the
#'   left side. Flipper models must be centred on y = 0.
#' @param name Non-empty model name used for identification.
#'
#' @details
#' Custom geometry is centred on its bounding box and uniformly scaled to a maximum
#' span of two model units. The pivot is transformed with the vertices. Display scale
#' is fixed for the render and accommodates the configured maximum deformation.
#' The painter's algorithm has no z-buffer and cannot resolve arbitrary intersections.
#'
#' @return A validated `nautilus_overlay_model` object. Pass it as
#'   `orientation.model` to [renderOverlayVideo()].
#' @examples
#' vertices <- rbind(c(1, 0, 0), c(-1, .3, 0), c(-1, -.3, 0), c(-.5, 0, -.3))
#' faces <- rbind(c(1, 2, 3), c(1, 4, 2), c(1, 3, 4), c(2, 4, 3))
#' model <- overlayModel(vertices, faces, name = "schematic")
#' @seealso [overlaySpeciesModel()] for the production species catalog.
#' @export
overlayModel <- function(vertices, faces, part = NULL,
                         motion = c("none", "lateral", "vertical", "flippers"),
                         weights = NULL, pivot = c(0, 0, 0), name = "custom") {
  motion <- match.arg(motion)
  .assert_string(name, "name")
  if (!nzchar(trimws(name))) .abort("{.arg name} must not be empty.")
  .validateOverlayVertices(vertices)
  faces <- .validateOverlayFaces(faces, vertices, triangles.only = TRUE)
  if (is.null(part)) part <- rep("body", length(faces))
  .validateOverlayParts(part, length(faces))
  rig <- .validateOverlayRig(motion, weights, pivot, nrow(vertices))
  centre <- (apply(vertices, 2L, min) + apply(vertices, 2L, max)) / 2
  span <- max(apply(vertices, 2L, function(x) diff(range(x))))
  if (!is.finite(span) || span <= 0) .abort("{.arg vertices} must have a non-zero spatial extent.")
  if (motion == "flippers" && abs(centre[2L]) > span * 1e-8)
    .abort("Flipper geometry must be centred on y = 0 so its pivots can be mirrored.")
  vertices <- sweep(vertices, 2L, centre, "-") * (2 / span)
  rig$pivot <- (rig$pivot - centre) * (2 / span)
  structure(list(name = name, source = "custom", vertices = vertices, faces = faces, part = part,
                 rig = rig, details = list(), eyes = NULL), class = "nautilus_overlay_model")
}

.validateOverlayVertices <- function(vertices) {
  if (!is.matrix(vertices) || !is.numeric(vertices) || ncol(vertices) != 3L ||
      nrow(vertices) < 3L || nrow(vertices) > 5000L || any(!is.finite(vertices)))
    .abort("{.arg vertices} must be a finite numeric matrix with three columns and 3 to 5000 rows.")
  invisible(vertices)
}

.validateOverlayFaces <- function(faces, vertices, triangles.only = FALSE) {
  if (is.matrix(faces) && is.numeric(faces))
    faces <- lapply(seq_len(nrow(faces)), function(i) faces[i, ])
  if (!is.list(faces) || !length(faces) || length(faces) > 2000L)
    .abort("{.arg faces} must contain between 1 and 2000 indexed faces.")
  valid <- vapply(faces, function(f) {
    is.numeric(f) && is.null(dim(f)) && length(f) >= 3L &&
      (!triangles.only || length(f) == 3L) && all(is.finite(f)) &&
      all(f == floor(f)) && all(f >= 1 & f <= nrow(vertices)) && !anyDuplicated(f)
  }, logical(1))
  if (!all(valid))
    .abort("{.arg faces} must use distinct, valid one-based vertex indices; custom faces must be triangles.")
  # Check area relative to edge lengths so a tiny but well-formed input can be normalised.
  degenerate <- vapply(faces, function(f) {
    p <- vertices[f[1:3], , drop = FALSE]
    e1 <- p[2, ] - p[1, ]; e2 <- p[3, ] - p[1, ]
    lengths <- sqrt(sum(e1^2)) * sqrt(sum(e2^2))
    cross <- c(e1[2]*e2[3] - e1[3]*e2[2], e1[3]*e2[1] - e1[1]*e2[3],
               e1[1]*e2[2] - e1[2]*e2[1])
    !is.finite(lengths) || lengths == 0 || sqrt(sum(cross^2)) <= 1e-10 * lengths
  }, logical(1))
  if (any(degenerate)) .abort("{.arg faces} contains degenerate geometry (zero-area or collinear vertices).")
  lapply(faces, as.integer)
}

.validateOverlayParts <- function(part, n) {
  if (!is.character(part) || length(part) != n || anyNA(part) || any(!nzchar(part)))
    .abort("{.arg part} must contain one non-empty character label per face.")
  invisible(part)
}

.validateOverlayRig <- function(type, weights, pivot, n) {
  .assert_choice(type, "motion", c("none", "lateral", "vertical", "flippers"))
  if (!is.numeric(pivot) || length(pivot) != 3L || any(!is.finite(pivot)))
    .abort("{.arg pivot} must contain three finite coordinates.")
  if (type == "none" && is.null(weights)) weights <- rep(0, n)
  if (!is.numeric(weights) || length(weights) != n || any(!is.finite(weights)) ||
      any(weights < 0 | weights > 1))
    .abort("{.arg weights} must contain one finite value between zero and one per vertex.")
  if (type != "none" && !any(weights > 0))
    .abort("An animated model needs at least one positive motion weight.")
  list(type = type, weights = as.numeric(weights), pivot = as.numeric(pivot))
}

# Resolve production species once before validating and caching indexed geometry.
.prepareOverlayModel <- function(model = "blue_shark", max.angle = 0) {
  .assert_number(max.angle, "max.angle", min = 0, max = 45)
  if (is.character(model)) {
    .assert_choice(model, "orientation.model", names(.orientationModelRegistry()))
    model <- overlaySpeciesModel(model)
  }
  if (!inherits(model, "nautilus_overlay_model") || !is.list(model))
    .abort("{.arg orientation.model} must name a built-in model or be created by {.fn overlayModel}.")
  # Revalidate at the boundary: a caller can edit an R list after construction.
  .assert_string(model$name, "model$name")
  .assert_choice(model$source, "model$source", c("custom", "builtin"))
  .validateOverlayVertices(model$vertices)
  model$faces <- .validateOverlayFaces(model$faces, model$vertices, triangles.only = model$source == "custom")
  # Rebuild this cache after validation; callers may have edited a model's faces.
  # Legacy polygon meshes retain the established projection and drawing path.
  model$triangles <- if (model$source == "custom") do.call(rbind, model$faces) else NULL
  .validateOverlayParts(model$part, length(model$faces))
  if (!is.list(model$rig)) .abort("The model has no valid motion definition.")
  model$rig <- .validateOverlayRig(model$rig$type, model$rig$weights, model$rig$pivot, nrow(model$vertices))
  if (!is.list(model$details)) .abort("The model has invalid surface details.")
  for (line in model$details) {
    if (!is.matrix(line) || !is.numeric(line) || ncol(line) != 3L || any(!is.finite(line)))
      .abort("Model surface details must be finite matrices with three columns.")
  }
  if (!is.null(model$detail.weights)) {
    if (!is.list(model$detail.weights) || length(model$detail.weights)!=length(model$details))
      .abort("Detail motion weights must match the model details.")
    for (i in seq_along(model$details)) {
      w <- model$detail.weights[[i]]
      if (!is.numeric(w) || length(w)!=nrow(model$details[[i]]) ||
          any(!is.finite(w)) || any(w<0 | w>1))
        .abort("Detail motion weights must be finite values in [0,1], one per point.")
    }
  }
  model$appearance <- .validateOverlayAppearance(model$appearance,length(model$faces))
  if (!is.null(model$eyes) && (!is.matrix(model$eyes) || !is.numeric(model$eyes) ||
                              ncol(model$eyes) != 3L || any(!is.finite(model$eyes))))
    .abort("Model eyes must be a finite matrix with three columns.")
  # The norm bound is conservative for every phase and attitude, preventing frame-wise zoom.
  rig <- model$rig
  pivots <- matrix(rig$pivot, nrow(model$vertices), 3L, byrow = TRUE)
  if (rig$type == "flippers") pivots[, 2L] <- abs(rig$pivot[2L]) * sign(model$vertices[, 2L])
  lever <- sqrt(rowSums((model$vertices - pivots)^2))
  bound <- max(sqrt(rowSums(model$vertices^2)) +
                 2 * lever * sin(max.angle * pi / 360 * rig$weights))
  model$display.scale <- min(0.84, 1.08 / bound)
  if (!is.null(model$appearance) && model$appearance$fit=="tight")
    model$display.scale <- min(1.10,1.02/.overlayTightMotionBound(model,max.angle))
  model
}

.deformOverlayModel <- function(model, phase = 0, strength = 0, max.angle = 0, phase.lag = 0) {
  vertices <- model$vertices
  rig <- model$rig
  if (rig$type == "none" || strength <= 0 || max.angle == 0) return(vertices)
  theta <- max.angle * pi / 180 * strength * rig$weights * sin(phase - phase.lag * rig$weights)
  p <- matrix(rig$pivot, nrow(vertices), 3L, byrow = TRUE)
  if (rig$type == "flippers") {
    p[, 2L] <- abs(rig$pivot[2L]) * sign(vertices[, 2L])
    theta <- theta * sign(vertices[, 2L])
  }
  v <- vertices - p
  co <- cos(theta); si <- sin(theta)
  axes <- switch(rig$type, lateral = c(1L, 2L), vertical = c(3L, 1L), flippers = c(2L, 3L))
  a <- axes[1]; b <- axes[2]
  vertices[, a] <- co * v[, a] - si * v[, b] + p[, a]
  vertices[, b] <- si * v[, a] + co * v[, b] + p[, b]
  vertices
}

# The production registry shares the approved species descriptors; no legacy meshes.
.orientationModelRegistry <- function() .overlaySpeciesSpecs()

#' Outward-ish unit normal of a face (first three vertices), for flat shading.
#' @keywords internal
#' @noRd
.faceNormal <- function(w) {
  e1 <- w[, 2] - w[, 1]; e2 <- w[, 3] - w[, 1]
  n  <- c(e1[2] * e2[3] - e1[3] * e2[2], e1[3] * e2[1] - e1[1] * e2[3], e1[1] * e2[2] - e1[2] * e2[1])
  nn <- sqrt(sum(n^2)); if (is.finite(nn) && nn > 0) n / nn else c(0, 0, 1)
}

#' Multiply a base colour by a shading intensity (clamped to the 0-1 range).
#' @keywords internal
#' @noRd
.shadeColor <- function(col, s) {
  s <- max(0, min(1, s)); rgb <- grDevices::col2rgb(col)[, 1] / 255
  grDevices::rgb(rgb[1] * s, rgb[2] * s, rgb[3] * s)
}
