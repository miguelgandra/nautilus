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

# Build once, retaining the production silhouettes while indexing their shared vertices.
.prepareOverlayModel <- function(model = "shark", max.angle = 0) {
  .assert_number(max.angle, "max.angle", min = 0, max = 45)
  if (is.character(model)) {
    .assert_choice(model, "orientation.model", names(.orientationModelRegistry()))
    legacy <- .tagModel3D(model)
    points <- t(do.call(cbind, legacy$faces))
    vertices <- unique(points)
    key <- function(x) paste(sprintf("%.17g", x[, 1]), sprintf("%.17g", x[, 2]),
                             sprintf("%.17g", x[, 3]), sep = ":")
    indices <- match(key(points), key(vertices))
    faces <- unname(split(indices, rep(seq_along(legacy$faces),
                                       vapply(legacy$faces, ncol, integer(1)))))
    if (model %in% c("shark", "fish", "cetacean")) {
      type <- if (model == "cetacean") "vertical" else "lateral"
      pivot <- c(-0.15, 0, 0)
      weights <- pmin(1, pmax(0, (pivot[1] - vertices[, 1]) /
                                    (pivot[1] - min(vertices[, 1]))))^2
    } else {
      type <- "flippers"
      fins <- which(legacy$part == "fin")[1:2]
      moving <- unique(unlist(faces[fins], use.names = FALSE))
      root <- min(abs(vertices[moving, 2]))
      pivot <- c(0, root, 0)
      weights <- rep(0, nrow(vertices))
      weights[moving] <- pmin(1, pmax(0, (abs(vertices[moving, 2]) - root) /
                                           (max(abs(vertices[moving, 2])) - root)))
    }
    model <- structure(list(name = model, source = "builtin", vertices = vertices, faces = faces,
                            part = legacy$part, rig = list(type = type, weights = weights, pivot = pivot),
                            details = lapply(legacy$details, t),
                            eyes = if (is.null(legacy$eyes)) NULL else t(legacy$eyes)),
                       class = "nautilus_overlay_model")
  }
  if (!inherits(model, "nautilus_overlay_model") || !is.list(model))
    .abort("{.arg orientation.model} must name a built-in model or be created by {.fn overlayModel}.")
  # Revalidate at the boundary: a caller can edit an R list after construction.
  .assert_string(model$name, "model$name")
  .assert_choice(model$source, "model$source", c("custom", "builtin"))
  .validateOverlayVertices(model$vertices)
  model$faces <- .validateOverlayFaces(model$faces, model$vertices, triangles.only = model$source == "custom")
  .validateOverlayParts(model$part, length(model$faces))
  if (!is.list(model$rig)) .abort("The model has no valid motion definition.")
  model$rig <- .validateOverlayRig(model$rig$type, model$rig$weights, model$rig$pivot, nrow(model$vertices))
  if (!is.list(model$details)) .abort("The model has invalid surface details.")
  for (line in model$details) {
    if (!is.matrix(line) || !is.numeric(line) || ncol(line) != 3L || any(!is.finite(line)))
      .abort("Model surface details must be finite matrices with three columns.")
  }
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

#' Configuration registry for the schematic orientation models.
#'
#' Each entry defines the longitudinal body envelope, appendage polygons and optional surface-detail
#' lines in the common animal frame (x forward, y right, z down). The shark entry is deliberately kept
#' unchanged; the other entries are recognizable, generic representatives rather than species models.
#' @keywords internal
#' @noRd
.orientationModelRegistry <- local({
  appendage <- function(...) list(...)
  models <- list(
    shark = list(
      stations = c(-1.00, -0.55, -0.10, 0.35, 0.70, 0.95, 1.05),
      width = c(0.015, 0.16, 0.24, 0.26, 0.20, 0.10, 0.012),
      height = c(0.015, 0.13, 0.19, 0.21, 0.16, 0.09, 0.012),
      appendages = appendage(
        cbind(c(0.42, 0, -0.20), c(0.02, 0, -0.20), c(0.22, 0, -0.62)),
        cbind(c(0.42, 0.22, 0.05), c(0.16, 0.22, 0.09), c(0.34, 0.56, 0.20)),
        cbind(c(0.42, -0.22, 0.05), c(0.16, -0.22, 0.09), c(0.34, -0.56, 0.20)),
        cbind(c(-0.90, 0, -0.03), c(-1.34, 0, -0.34), c(-1.10, 0, 0.01)),
        cbind(c(-0.90, 0, 0.03), c(-1.30, 0, 0.26), c(-1.10, 0, 0.01))
      )),
    cetacean = list(
      # Dolphin-like odontocete: distinct melon and beak, swept dorsal/pectoral fins, horizontal flukes.
      sections = 12L,
      stations = c(-1.09, -0.88, -0.55, -0.14, 0.30, 0.67, 0.91, 1.06, 1.29),
      width = c(0.025, 0.07, 0.15, 0.27, 0.29, 0.23, 0.15, 0.065, 0.012),
      height = c(0.025, 0.06, 0.12, 0.21, 0.22, 0.18, 0.12, 0.055, 0.010),
      appendages = appendage(
        cbind(c(0.08, 0, -0.21), c(-0.26, 0, -0.18), c(-0.12, 0, -0.48), c(-0.02, 0, -0.43)),
        cbind(c(0.22, 0.26, 0.03), c(-0.16, 0.25, 0.04), c(-0.36, 0.64, 0.07),
              c(-0.08, 0.57, 0.04), c(0.15, 0.36, 0.02)),
        cbind(c(0.22, -0.26, 0.03), c(-0.16, -0.25, 0.04), c(-0.36, -0.64, 0.07),
              c(-0.08, -0.57, 0.04), c(0.15, -0.36, 0.02)),
        cbind(c(-1.07, 0.00, 0.00), c(-1.20, 0.30, 0.00), c(-1.38, 0.72, 0.01),
              c(-1.51, 0.40, 0.02), c(-1.38, 0.08, 0.02)),
        cbind(c(-1.07, 0.00, 0.00), c(-1.20, -0.30, 0.00), c(-1.38, -0.72, 0.01),
              c(-1.51, -0.40, 0.02), c(-1.38, -0.08, 0.02))
      ),
      details = appendage(
        rbind(c(0.89, 0.96, 1.05), c(-0.10, 0, 0.10), c(-0.105, -0.115, -0.07))
      ),
      eyes = rbind(c(0.87, 0.87), c(0.14, -0.14), c(-0.08, -0.08))),
    turtle = list(
      # Hard-shelled sea turtle: domed oval carapace, short neck/head, long foreflippers, small hind pair.
      sections = 12L,
      stations = c(-0.79, -0.66, -0.43, -0.08, 0.28, 0.52, 0.67, 0.80, 0.98, 1.10, 1.25),
      width = c(0.018, 0.23, 0.40, 0.48, 0.44, 0.31, 0.11, 0.13, 0.18, 0.16, 0.025),
      height = c(0.012, 0.10, 0.19, 0.26, 0.25, 0.16, 0.07, 0.07, 0.10, 0.09, 0.015),
      appendages = appendage(
        cbind(c(0.52, 0.30, 0.02), c(0.20, 0.46, 0.04), c(-0.06, 0.77, 0.05),
              c(-0.33, 1.04, 0.06), c(-0.08, 0.99, 0.07), c(0.27, 0.66, 0.03)),
        cbind(c(0.52, -0.30, 0.02), c(0.20, -0.46, 0.04), c(-0.06, -0.77, 0.05),
              c(-0.33, -1.04, 0.06), c(-0.08, -0.99, 0.07), c(0.27, -0.66, 0.03)),
        cbind(c(-0.49, 0.35, 0.04), c(-0.72, 0.27, 0.05), c(-0.90, 0.62, 0.05),
              c(-0.62, 0.55, 0.05)),
        cbind(c(-0.49, -0.35, 0.04), c(-0.72, -0.27, 0.05), c(-0.90, -0.62, 0.05),
              c(-0.62, -0.55, 0.05)),
        cbind(c(-0.76, 0, 0.02), c(-0.98, 0, 0.03), c(-0.79, 0, 0.08))
      ),
      details = appendage(
        # Midline vertebral scutes and paired costal sutures on the dorsal carapace.
        rbind(c(-0.65, -0.42, -0.10, 0.22, 0.49, 0.62),
              c(0, 0, 0, 0, 0, 0), c(-0.12, -0.205, -0.265, -0.255, -0.18, -0.10)),
        rbind(c(-0.44, -0.35, -0.22, -0.10), c(0, 0.14, 0.29, 0.39),
              c(-0.205, -0.19, -0.14, -0.07)),
        rbind(c(-0.44, -0.35, -0.22, -0.10), c(0, -0.14, -0.29, -0.39),
              c(-0.205, -0.19, -0.14, -0.07)),
        rbind(c(-0.08, -0.03, 0.01, 0.04), c(0, 0.17, 0.34, 0.46),
              c(-0.265, -0.24, -0.14, -0.05)),
        rbind(c(-0.08, -0.03, 0.01, 0.04), c(0, -0.17, -0.34, -0.46),
              c(-0.265, -0.24, -0.14, -0.05)),
        rbind(c(0.27, 0.29, 0.29, 0.29), c(0, 0.16, 0.31, 0.42),
              c(-0.255, -0.23, -0.14, -0.06)),
        rbind(c(0.27, 0.29, 0.29, 0.29), c(0, -0.16, -0.31, -0.42),
              c(-0.255, -0.23, -0.14, -0.06))
      ),
      eyes = rbind(c(1.07, 1.07), c(0.14, -0.14), c(-0.09, -0.09))),
    fish = list(
      # Generic ray-finned fish, not one species: deeper trunk, rounded head/operculum, symmetric tail.
      sections = 12L,
      stations = c(-1.02, -0.86, -0.60, -0.24, 0.18, 0.53, 0.80, 1.03, 1.15),
      width = c(0.020, 0.075, 0.14, 0.23, 0.28, 0.25, 0.18, 0.10, 0.025),
      height = c(0.020, 0.07, 0.19, 0.33, 0.36, 0.33, 0.25, 0.13, 0.025),
      appendages = appendage(
        cbind(c(0.33, 0, -0.34), c(-0.19, 0, -0.32), c(-0.52, 0, -0.22),
              c(-0.31, 0, -0.51), c(0.10, 0, -0.57)),
        cbind(c(-0.24, 0, 0.32), c(-0.69, 0, 0.16), c(-0.54, 0, 0.38), c(-0.38, 0, 0.45)),
        cbind(c(0.55, 0.22, 0.07), c(0.23, 0.26, 0.08), c(0.12, 0.54, 0.11),
              c(0.43, 0.46, 0.06)),
        cbind(c(0.55, -0.22, 0.07), c(0.23, -0.26, 0.08), c(0.12, -0.54, 0.11),
              c(0.43, -0.46, 0.06)),
        cbind(c(-0.22, 0.20, 0.24), c(-0.42, 0.18, 0.26), c(-0.35, 0.39, 0.31)),
        cbind(c(-0.22, -0.20, 0.24), c(-0.42, -0.18, 0.26), c(-0.35, -0.39, 0.31)),
        cbind(c(-1.01, 0, -0.01), c(-1.24, 0, -0.42), c(-1.52, 0, -0.53),
              c(-1.39, 0, 0), c(-1.52, 0, 0.53), c(-1.24, 0, 0.42), c(-1.01, 0, 0.01))
      ),
      details = appendage(
        rbind(c(0.58, 0.64, 0.62, 0.54), c(0.21, 0.22, 0.20, 0.17),
              c(-0.18, -0.11, -0.04, 0.02)),
        rbind(c(0.58, 0.64, 0.62, 0.54), c(-0.21, -0.22, -0.20, -0.17),
              c(-0.18, -0.11, -0.04, 0.02))
      ),
      eyes = rbind(c(0.92, 0.92), c(0.14, -0.14), c(-0.10, -0.10))),
    manta = list(
      # Mobulid disc: wing-like pectorals form the wide diamond; two cephalic lobes and a whip tail.
      sections = 12L,
      stations = c(-1.45, -0.77, -0.49, -0.17, 0.23, 0.57, 0.79, 0.88),
      width = c(0.005, 0.018, 0.15, 0.29, 0.38, 0.33, 0.19, 0.075),
      height = c(0.004, 0.014, 0.035, 0.06, 0.08, 0.07, 0.045, 0.012),
      appendages = appendage(
        cbind(c(0.75, 0.19, 0.015), c(0.49, 0.40, 0.02), c(0.14, 0.86, 0.03),
              c(-0.12, 1.29, 0.04), c(-0.22, 1.34, 0.04), c(-0.41, 0.79, 0.02),
              c(-0.53, 0.29, 0.01)),
        cbind(c(0.75, -0.19, 0.015), c(0.49, -0.40, 0.02), c(0.14, -0.86, 0.03),
              c(-0.12, -1.29, 0.04), c(-0.22, -1.34, 0.04), c(-0.41, -0.79, 0.02),
              c(-0.53, -0.29, 0.01)),
        cbind(c(0.78, 0.12, -0.015), c(1.15, 0.11, -0.005), c(1.13, 0.24, 0.005),
              c(0.85, 0.25, 0.01)),
        cbind(c(0.78, -0.12, -0.015), c(1.15, -0.11, -0.005), c(1.13, -0.24, 0.005),
              c(0.85, -0.25, 0.01)),
        cbind(c(-0.52, 0, -0.025), c(-0.73, 0, -0.025), c(-0.61, 0, -0.10))
      ),
      details = appendage(
        rbind(c(0.78, 0.86), c(-0.11, 0.11), c(-0.048, -0.048))
      ),
      eyes = rbind(c(0.75, 0.75), c(0.27, -0.27), c(-0.03, -0.03)))
  )
  function() models
})

#' Build a low-poly 3-D body model from the orientation-model registry.
#'
#' Returned as a list of faces (each a 3 x k matrix of vertex coordinates), matching part labels
#' (`"body"` or `"fin"`) for styling, optional surface-detail polylines and eye positions.
#' @keywords internal
#' @noRd
.tagModel3D <- function(model = "shark") {
  spec <- .orientationModelRegistry()[[model]]
  if (is.null(spec)) .abort("Unknown orientation model: {.val {model}}.")
  stn <- spec$stations
  ry <- spec$width
  rz <- spec$height
  nt  <- if (is.null(spec$sections)) 8L else spec$sections
  ang <- utils::head(seq(0, 2 * pi, length.out = nt + 1L), nt)
  rings <- lapply(seq_along(stn), function(i) rbind(rep(stn[i], nt), ry[i] * cos(ang), rz[i] * sin(ang)))
  faces <- list(); part <- character(0)
  add <- function(m, p) { faces[[length(faces) + 1L]] <<- m; part[length(part) + 1L] <<- p }
  for (i in seq_len(length(stn) - 1L)) {                      # tube: quad faces between adjacent rings
    A <- rings[[i]]; B <- rings[[i + 1L]]
    for (j in seq_len(nt)) { k <- if (j == nt) 1L else j + 1L; add(cbind(A[, j], A[, k], B[, k], B[, j]), "body") }
  }
  for (fin in spec$appendages) add(fin, "fin")
  list(faces = faces, part = part, details = spec$details, eyes = spec$eyes)
}

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
