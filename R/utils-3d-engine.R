# Shared 3D model construction, animation and rendering engine.

# Public constructors and catalog ---------------------------------------


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
overlayModel <- function(vertices, faces, part = NULL, motion = c("none", "lateral", "vertical", "flippers"),
    weights = NULL, pivot = c(0, 0, 0), name = "custom") {
    motion <- match.arg(motion)
    .assert_string(name, "name")
    if (!nzchar(trimws(name)))
        .abort("{.arg name} must not be empty.")
    .validateOverlayVertices(vertices)
    faces <- .validateOverlayFaces(faces, vertices, triangles.only = TRUE)
    if (is.null(part))
        part <- rep("body", length(faces))
    .validateOverlayParts(part, length(faces))
    rig <- .validateOverlayRig(motion, weights, pivot, nrow(vertices))
    centre <- (apply(vertices, 2L, min) + apply(vertices, 2L, max))/2
    span <- max(apply(vertices, 2L, function(x) diff(range(x))))
    if (!is.finite(span) || span <= 0)
        .abort("{.arg vertices} must have a non-zero spatial extent.")
    if (motion == "flippers" && abs(centre[2L]) > span * 1e-08)
        .abort("Flipper geometry must be centred on y = 0 so its pivots can be mirrored.")
    vertices <- sweep(vertices, 2L, centre, "-") * (2/span)
    rig$pivot <- (rig$pivot - centre) * (2/span)
    structure(list(name = name, source = "custom", vertices = vertices, faces = faces, part = part, rig = rig,
        details = list(), eyes = NULL), class = "nautilus_overlay_model")
}


#' Configure schematic motion for the video renderer
#'
#' @param method Frequency/amplitude backend: `"auto"` (default) selects peaks when
#'   it carries finite positive frequency estimates, otherwise wavelet. `"peaks"` or
#'   `"wavelet"` fixes the backend for the entire render; there is no per-frame switching.
#' @param amplitude.reference Positive peak-to-trough signal amplitude corresponding
#'   to full visual motion, in the selected motion channel's units. `NULL` uses the
#'   95th percentile of valid amplitudes across the full supplied deployment.
#'   Supply a calibrated reference for comparisons across deployments.
#' @param max.angle Maximum schematic bend angle in degrees, between 0 and 45
#'   (default 20). This is a display setting, not a measured anatomical angle.
#' @param phase.offset Initial phase in radians (default 0), reset at each valid segment.
#' @param phase.lag Phase delay in radians toward higher-weight vertices (default pi/3).
#'
#' @details
#' Pass this object as `motion` to [renderOverlayVideo()] to enable motion.
#' The default `motion = NULL` keeps models rigid. Phase is integrated from frequency;
#' it does not recover measured tail position. Peak-to-trough signal amplitude is
#' mapped linearly onto a fraction of `max.angle`, with saturation at `amplitude.reference`.
#' Unavailable estimates show a neutral model labelled as unavailable. An explicit
#' `tbf_swimming = FALSE` shows a neutral model labelled as gliding. Unknown swimming
#' classification does not suppress otherwise valid estimates.
#'
#' @return A validated `nautilus_overlay_motion` control list.
#' @export
overlayMotionControl <- function(method = c("auto", "peaks", "wavelet"), amplitude.reference = NULL,
    max.angle = 20, phase.offset = 0, phase.lag = pi/3) {
    method <- match.arg(method)
    .assert_number(amplitude.reference, "amplitude.reference", min = .Machine$double.eps, null_ok = TRUE)
    .assert_number(max.angle, "max.angle", min = 0, max = 45)
    .assert_number(phase.offset, "phase.offset")
    .assert_number(phase.lag, "phase.lag", min = 0, max = 2 * pi)
    structure(list(method = method, amplitude.reference = amplitude.reference, max.angle = max.angle,
        phase.offset = phase.offset, phase.lag = phase.lag), class = "nautilus_overlay_motion")
}


#' Build a species-specific orientation model
#'
#' @description
#' Builds the production animal geometry, appearance and motion profile.
#' @param model One of `blue_shark`, `whale_shark`, `mako`, `hammerhead`, `manta`,
#'   `devil_ray`, `turtle`, `dolphin`, `whale`, `marlin` or `tuna`.
#' @details
#' Coordinates use x forward, y right and z down. The returned model contains
#' indexed geometry, rig weights and pivots, colours and mesh-attached markings.
#' Named components and region maps support inspection and customization.
#' Pass a model name or this object to [renderOverlayVideo()]. Animation remains
#' optional through `animate = TRUE` or [overlayMotionControl()].
#' @return `overlaySpeciesModel()` returns a validated `nautilus_overlay_model`.
#'   `overlaySpeciesCatalog()` returns names, labels, taxa, mesh sizes and motion
#'   profiles. Model metadata contains identity, axes and component/region maps.
#' @examples
#' overlaySpeciesCatalog()
#' model <- overlaySpeciesModel("blue_shark")
#' model$rig
#' @seealso [overlayModel()], [overlayMotionControl()], [renderOverlayVideo()]
#' @export
overlaySpeciesModel <- function(model = "blue_shark") {
    specs <- .overlaySpeciesSpecs()
    .assert_choice(model, "model", names(specs))
    spec <- specs[[model]]
    .validateSpeciesSpec(spec)
    geometry <- .buildSpeciesMesh(spec)
    weights <- .speciesWeights(geometry, spec$rig)
    out <- overlayModel(geometry$vertices, geometry$faces, geometry$part, motion = spec$rig$type, weights = weights,
        pivot = spec$rig$pivot, name = model)
    out$part[out$part == "fin"] <- "appendage"
    triangles <- length(out$faces)
    if (triangles < 200L || triangles > 700L)
        .abort("Species model {.val {model}} exceeds its 200--700 triangle budget ({triangles}).")
    centre <- (apply(geometry$vertices, 2L, min) + apply(geometry$vertices, 2L, max))/2
    scale <- 2/max(apply(geometry$vertices, 2L, function(x) diff(range(x))))
    transform <- function(p) sweep(p, 2L, centre, "-") * scale
    components <- geometry$components
    pivots <- transform(as.matrix(components[, c("pivot.x", "pivot.y", "pivot.z")]))
    components[, c("normalised.pivot.x", "normalised.pivot.y", "normalised.pivot.z")] <- pivots
    out$details <- lapply(geometry$details, transform)
    if (isTRUE(spec$rig$deform.details))
        out$detail.weights <- lapply(geometry$details, function(p) pmin(1, pmax(0, (spec$rig$pivot[1] -
            p[, 1])/(spec$rig$pivot[1] - spec$rig$tip)))^spec$rig$exponent)
    out$eyes <- if (is.null(spec$eyes))
        NULL
    else transform(spec$eyes)
    if (!is.null(spec$appearance)) {
        out$appearance <- spec$appearance
        out$appearance$body.faces <- geometry$face.region == "body"
        p <- geometry$vertices
        w <- stats::approx(spec$body[, 1], spec$body[, 2], p[, 1], rule = 2)$y
        z <- stats::approx(spec$body[, 1], spec$body[, 5], p[, 1], rule = 2)$y
        h <- ifelse(p[, 3] < z, stats::approx(spec$body[, 1], spec$body[, 3], p[, 1], rule = 2)$y, stats::approx(spec$body[,
            1], spec$body[, 4], p[, 1], rule = 2)$y)
        inside <- (p[, 2]/w)^2 + ((p[, 3] - z)/h)^2 < 0.999 & p[, 1] > min(spec$body[, 1]) & p[, 1] <
            max(spec$body[, 1])
        out$appearance$hidden.faces <- !out$appearance$body.faces & vapply(geometry$faces, function(f) all(inside[f]),
            logical(1))
        u <- vapply(geometry$faces, function(f) mean((p[f, 3] - z[f])/h[f]), numeric(1))
        u <- pmin(1, pmax(0, (u + 0.3)/1.1))
        out$appearance$belly <- ifelse(out$appearance$body.faces, u * u * (3 - 2 * u), 0)
        if (identical(spec$surface, "manta")) {
            out$appearance$belly <- .mantaCountershading(geometry, out$appearance$belly)
            out$appearance$smooth.faces <- !geometry$face.region %in% c("tail", "dorsal")
            out$appearance$surface <- .compileMantaSurface(geometry)
        }
        else if (identical(spec$surface, "devil_ray")) {
            out$appearance$belly <- .devilRayCountershading(geometry, out$appearance$belly)
            out$appearance$smooth.faces <- !geometry$face.region %in% c("tail", "dorsal")
            out$appearance$surface <- .compileDevilRaySurface(geometry)
        }
        else if (identical(spec$surface, "mako")) {
            out$appearance$belly <- .makoCountershading(geometry, out$appearance$belly, spec$sections)
            out$appearance$smooth.faces <- !startsWith(geometry$face.region, "keel_")
            out$appearance$surface <- .compileMakoSurface(spec, geometry)
        }
        else if (identical(spec$surface, "hammerhead")) {
            out$appearance$belly <- .hammerheadCountershading(geometry, out$appearance$belly, spec$sections)
            out$appearance$surface <- .compileHammerheadSurface(spec, geometry)
        }
        else if (identical(spec$surface, "turtle")) {
            out$appearance$hidden.faces[geometry$face.region == "plastron"] <- FALSE
            out$appearance$belly <- .turtleCountershading(geometry)
            out$appearance$surface <- .compileTurtleSurface(spec, geometry)
        }
        else if (identical(spec$surface, "humpback")) {
            out$appearance$belly <- .humpbackCountershading(geometry, out$appearance$belly, spec$sections)
            out$appearance$surface <- .compileHumpbackSurface(spec, geometry)
        }
        else if (identical(spec$surface, "dolphin")) {
            out$appearance$face.rgb <- .dolphinReferenceFaceRGB()
            out$appearance$surface <- .compileDolphinSurface(spec, geometry)
        }
        else if (identical(spec$surface, "marlin")) {
            out$appearance$face.rgb <- .marlinReferenceFaceRGB()
            out$appearance$surface <- .compileMarlinSurface(spec, geometry)
        }
        else if (identical(spec$surface, "tuna")) {
            out$appearance$face.rgb <- .tunaReferenceFaceRGB(geometry)
            out$appearance$surface <- .compileTunaSurface(spec, geometry)
        }
        else if (!is.null(spec$surface)) {
            out$appearance$surface <- .compileSharkSurface(spec, geometry)
        }
    }
    out$metadata <- list(id = model, label = spec$label, taxon = spec$taxon, axes = c(x = "forward",
        y = "right", z = "down"), components = components, vertex.region = geometry$region, face.region = geometry$face.region)
    out
}


#' @rdname overlaySpeciesModel
#' @export
overlaySpeciesCatalog <- function() {
    specs <- .overlaySpeciesSpecs()
    rows <- lapply(names(specs), function(id) {
        m <- overlaySpeciesModel(id)
        data.frame(model = id, label = m$metadata$label, taxon = m$metadata$taxon, vertices = nrow(m$vertices),
            triangles = length(m$faces), motion = m$rig$type, stringsAsFactors = FALSE)
    })
    do.call(rbind, rows)
}


# .orientationModelRegistry
.orientationModelRegistry <- function() .overlaySpeciesSpecs()

# Geometry validation ---------------------------------------------------


# .validateOverlayVertices
.validateOverlayVertices <- function(vertices) {
    if (!is.matrix(vertices) || !is.numeric(vertices) || ncol(vertices) != 3L || nrow(vertices) < 3L ||
        nrow(vertices) > 5000L || any(!is.finite(vertices)))
        .abort("{.arg vertices} must be a finite numeric matrix with three columns and 3 to 5000 rows.")
    invisible(vertices)
}


# .validateOverlayFaces
.validateOverlayFaces <- function(faces, vertices, triangles.only = FALSE) {
    if (is.matrix(faces) && is.numeric(faces))
        faces <- lapply(seq_len(nrow(faces)), function(i) faces[i, ])
    if (!is.list(faces) || !length(faces) || length(faces) > 2000L)
        .abort("{.arg faces} must contain between 1 and 2000 indexed faces.")
    valid <- vapply(faces, function(f) {
        is.numeric(f) && is.null(dim(f)) && length(f) >= 3L && (!triangles.only || length(f) == 3L) &&
            all(is.finite(f)) && all(f == floor(f)) && all(f >= 1 & f <= nrow(vertices)) && !anyDuplicated(f)
    }, logical(1))
    if (!all(valid))
        .abort("{.arg faces} must use distinct, valid one-based vertex indices; custom faces must be triangles.")
    degenerate <- vapply(faces, function(f) {
        p <- vertices[f[1:3], , drop = FALSE]
        e1 <- p[2, ] - p[1, ]
        e2 <- p[3, ] - p[1, ]
        lengths <- sqrt(sum(e1^2)) * sqrt(sum(e2^2))
        cross <- c(e1[2] * e2[3] - e1[3] * e2[2], e1[3] * e2[1] - e1[1] * e2[3], e1[1] * e2[2] - e1[2] *
            e2[1])
        !is.finite(lengths) || lengths == 0 || sqrt(sum(cross^2)) <= 1e-10 * lengths
    }, logical(1))
    if (any(degenerate))
        .abort("{.arg faces} contains degenerate geometry (zero-area or collinear vertices).")
    lapply(faces, as.integer)
}


# .validateOverlayParts
.validateOverlayParts <- function(part, n) {
    if (!is.character(part) || length(part) != n || anyNA(part) || any(!nzchar(part)))
        .abort("{.arg part} must contain one non-empty character label per face.")
    invisible(part)
}


# .validateOverlayRig
.validateOverlayRig <- function(type, weights, pivot, n) {
    .assert_choice(type, "motion", c("none", "lateral", "vertical", "flippers"))
    if (!is.numeric(pivot) || length(pivot) != 3L || any(!is.finite(pivot)))
        .abort("{.arg pivot} must contain three finite coordinates.")
    if (type == "none" && is.null(weights))
        weights <- rep(0, n)
    if (!is.numeric(weights) || length(weights) != n || any(!is.finite(weights)) || any(weights < 0 |
        weights > 1))
        .abort("{.arg weights} must contain one finite value between zero and one per vertex.")
    if (type != "none" && !any(weights > 0))
        .abort("An animated model needs at least one positive motion weight.")
    list(type = type, weights = as.numeric(weights), pivot = as.numeric(pivot))
}


# .validateSpeciesSpec
.validateSpeciesSpec <- function(spec) {
    .assert_string(spec$label, "model label")
    .assert_string(spec$taxon, "model taxon")
    .assert_count(spec$sections, "body sections", min = 6L)
    .assert_choice(spec$body.role, "body motion role", c("axial", "rigid"))
    if (!is.list(spec$rig))
        .abort("A species model must declare its rig metadata.")
    rig <- spec$rig
    .assert_choice(rig$type, "rig type", c("lateral", "vertical", "flippers"))
    .assert_number(rig$tip, "rig tip")
    .assert_number(rig$exponent, "rig exponent", min = .Machine$double.eps)
    if (!is.numeric(rig$pivot) || length(rig$pivot) != 3L || any(!is.finite(rig$pivot)))
        .abort("Rig pivot must contain three finite coordinates.")
    if (rig$type == "flippers") {
        if (!is.null(rig$flipper.mode))
            .assert_choice(rig$flipper.mode, "flipper mode", c("flexible", "rigid"))
        if (spec$body.role != "rigid" || rig$tip <= abs(rig$pivot[2]))
            .abort("A flipper rig requires a rigid body and a tip beyond its pivot.")
    }
    else if (spec$body.role != "axial" || rig$tip >= rig$pivot[1])
        .abort("An axial rig requires an axial body and a tail tip behind its pivot.")
    validate.stations <- function(x) {
        if (!is.matrix(x) || !is.numeric(x) || ncol(x) != 5L || nrow(x) < 3L || any(!is.finite(x)) ||
            any(diff(x[, 1]) <= 0) || any(x[, 2:4] <= 0))
            .abort("Loft stations must have increasing x and positive width/dorsal/ventral radii.")
    }
    validate.stations(spec$body)
    if (!is.null(spec$body.angles) && (!is.numeric(spec$body.angles) || length(spec$body.angles) != spec$sections ||
        any(!is.finite(spec$body.angles)) || any(diff(spec$body.angles) <= 0) || min(spec$body.angles) <
        0 || max(spec$body.angles) >= 2 * pi))
        .abort("Body ring angles must increase within [0, 2*pi) and match body sections.")
    if (!is.list(spec$appendages) || !length(spec$appendages))
        .abort("A species model must declare its appendages.")
    ids <- character()
    for (component in spec$appendages) {
        .assert_string(component$id, "component id")
        .assert_choice(component$kind, "component kind", c("loft", "fin", "foil"))
        .assert_choice(component$part, "component part", c("body", "fin"))
        .assert_choice(component$role, "component motion role", c("rigid", "axial", "terminal", "attached",
            "flipper"))
        if (component$role == "attached")
            .assert_number(component$joint.x, "component joint.x")
        if (!is.null(component$offset) && (!is.numeric(component$offset) || length(component$offset) !=
            3L || any(!is.finite(component$offset))))
            .abort("Component offset must contain three finite coordinates.")
        if (component$kind == "loft") {
            validate.stations(component$stations)
            .assert_count(component$sections, "component sections", min = 6L)
            if (!is.null(component$axis))
                .assert_choice(component$axis, "loft axis", c("x", "y"))
            if (!is.null(component$centreline) && (!is.numeric(component$centreline) || length(component$centreline) !=
                nrow(component$stations) || any(!is.finite(component$centreline))))
                .abort("A curved loft needs one finite transverse centreline coordinate per station.")
            if (identical(component$axis, "y") && is.null(component$centreline))
                .abort("A transverse loft needs centreline coordinates.")
        }
        else if (component$kind == "foil") {
            .validateOverlayVertices(component$leading)
            .validateOverlayVertices(component$trailing)
            if (!identical(dim(component$leading), dim(component$trailing)) || length(component$thickness) !=
                nrow(component$leading) || any(!is.finite(component$thickness) | component$thickness <=
                0))
                .abort("Foil edges and positive thicknesses must have matching stations.")
            .assert_count(component$sections, "foil sections", min = 4L)
            if (!is.null(component$section.vertices)) {
                .validateOverlayVertices(component$section.vertices)
                if (nrow(component$section.vertices) != nrow(component$leading) * component$sections)
                  .abort("Measured foil sections must match the station and section counts.")
            }
            if (!is.numeric(component$normal) || length(component$normal) != 3L || any(!is.finite(component$normal)) ||
                sum(component$normal^2) == 0)
                .abort("A foil normal must be a finite non-zero vector.")
        }
        else {
            .validateOverlayVertices(component$outline)
            .assert_number(component$thickness, "fin thickness", min = .Machine$double.eps)
            if (!is.numeric(component$normal) || length(component$normal) != 3L || any(!is.finite(component$normal)) ||
                sum(component$normal^2) == 0)
                .abort("A fin normal must be a non-zero vector with three finite coordinates.")
            if (!is.null(component$centre) && (!is.numeric(component$centre) || length(component$centre) !=
                3L || any(!is.finite(component$centre))))
                .abort("Fin centre must contain three finite coordinates.")
        }
        ids <- c(ids, component$id)
    }
    if (anyDuplicated(c("body", ids)))
        .abort("Component ids must be unique, including the body.")
    invisible(spec)
}

# Mesh primitives and species assembly ----------------------------------


# .speciesBody
.speciesBody <- function(x, width, dorsal, ventral = dorsal * 0.8, z = 0) {
    cbind(x, width, dorsal, ventral, rep_len(z, length(x)))
}


# .speciesDescriptor
.speciesDescriptor <- function(label, taxon, body, rig, appendages, sections = 10L, body.role = "axial",
    eyes = NULL, details = list()) {
    list(label = label, taxon = taxon, body = body, sections = sections, body.role = body.role, rig = rig,
        appendages = appendages, eyes = eyes, details = details)
}


# .anatomyBody
.anatomyBody <- function(position, width, dorsal, ventral = dorsal, z = 0) {
    out <- .speciesBody(50 - position, width, dorsal, ventral, z)
    out[order(out[, 1]), , drop = FALSE]
}


# .anatomyFoil
.anatomyFoil <- function(id, stations, plane = "xy", role = "rigid", sections = 6L) {
    leading <- trailing <- matrix(0, nrow(stations), 3L)
    leading[, 1] <- stations[, 2]
    trailing[, 1] <- stations[, 3]
    span <- if (plane == "xy")
        2L
    else 3L
    normal.axis <- if (plane == "xy")
        3L
    else 2L
    leading[, span] <- trailing[, span] <- stations[, 1]
    leading[, normal.axis] <- trailing[, normal.axis] <- stations[, 4]
    pivot <- (leading[1, ] + trailing[1, ])/2
    list(id = id, kind = "foil", part = "fin", role = role, joint.x = pivot[1], leading = leading, trailing = trailing,
        normal = diag(3)[, normal.axis], thickness = stations[, 5], sections = sections, pivot = pivot)
}


# .anatomyPair
.anatomyPair <- function(fin, id = fin$id) {
    right <- fin
    right$id <- paste0(id, "_right")
    left <- right
    left$id <- paste0(id, "_left")
    left$leading[, 2] <- -left$leading[, 2]
    left$trailing[, 2] <- -left$trailing[, 2]
    left$normal[2] <- -left$normal[2]
    left$pivot[2] <- -left$pivot[2]
    if (!is.null(left$section.vertices))
        left$section.vertices[, 2] <- -left$section.vertices[, 2]
    list(right, left)
}


# .anatomyDorsal
.anatomyDorsal <- function(id, position, height, base, tip.sweep, surface, role = "attached", thickness = 1.1,
    down = FALSE, sections = 4L) {
    root.x <- 50 - position
    sign <- if (down)
        1
    else -1
    s <- rbind(c(surface * 0.45, root.x, root.x - base, 0, thickness), c(surface + height * 0.2, root.x -
        base * 0.13, root.x - base * 0.83, 0, thickness * 0.7), c(surface + height * 0.65, root.x - tip.sweep *
        0.65, root.x - tip.sweep - base * 0.14, 0, thickness * 0.3), c(surface + height * 0.94, root.x -
        tip.sweep + 0.45, root.x - tip.sweep - 0.55, 0, thickness * 0.12), c(surface + height, root.x -
        tip.sweep, root.x - tip.sweep - 0.12, 0, thickness * 0.035))
    s[, 1] <- sign * s[, 1]
    if (height < 4.5)
        s <- s[c(1, 3, 5), , drop = FALSE]
    .anatomyFoil(id, s, "xz", role, sections)
}


# .anatomyTail
.anatomyTail <- function(upper, lower, upper.sweep, lower.sweep, root = -33, chord = 6, thickness = 1.1,
    sections = 4L, horizontal = FALSE) {
    lobe <- function(height, sweep, id, side) {
        s <- rbind(c(0, root + 1, root - chord, 0, thickness), c(height * 0.18, root - 1, root - chord -
            1.2, 0, thickness * 0.8), c(height * 0.44, root - sweep * 0.25, root - chord - sweep * 0.25,
            0, thickness * 0.45), c(height * 0.77, root - sweep * 0.65, root - sweep * 0.65 - chord *
            0.42, 0, thickness * 0.2), c(height, root - sweep, root - sweep - 0.16, 0, thickness * 0.035))
        s[, 1] <- side * s[, 1]
        .anatomyFoil(id, s, if (horizontal)
            "xy"
        else "xz", "terminal", sections)
    }
    list(lobe(upper, upper.sweep, if (horizontal) "fluke_right" else "caudal_upper", if (horizontal) 1 else -1),
        lobe(lower, lower.sweep, if (horizontal) "fluke_left" else "caudal_lower", if (horizontal) -1 else 1))
}


# .anatomySpec
.anatomySpec <- function(label, taxon, body, appendages, type = "lateral", sections = 12L, eyes = NULL,
    details = list(), pivot = c(0, 0, 0), tip = -33) {
    spec <- .speciesDescriptor(label, taxon, body, list(type = type, pivot = pivot, tip = tip, exponent = if (type ==
        "flippers") 1.5 else 2), appendages, sections, if (type == "flippers")
        "rigid"
    else "axial", eyes, details)
    spec
}


# .anatomyKeels
.anatomyKeels <- function(x = -30, width = 3, z = 0) {
    keel <- .anatomyFoil("keel", rbind(c(0.6, x + 4, x - 5, z, 0.6), c(width * 0.7, x + 1, x - 4, z,
        0.32), c(width, x - 1, x - 2, z, 0.08)), sections = 4L, role = "attached")
    .anatomyPair(keel)
}


# .speciesLoft
.speciesLoft <- function(stations, sections = 12L, angles = NULL) {
    n <- nrow(stations)
    a <- if (is.null(angles))
        (seq_len(sections) - 1L) * 2 * pi/sections
    else angles
    vertices <- do.call(rbind, lapply(seq_len(n), function(i) {
        s <- stations[i, ]
        cbind(rep(s[1], sections), s[2] * cos(a), s[5] + ifelse(sin(a) < 0, s[3], s[4]) * sin(a))
    }))
    vertices <- rbind(vertices, c(stations[1, 1], 0, stations[1, 5]), c(stations[n, 1], 0, stations[n,
        5]))
    faces <- list()
    for (i in seq_len(n - 1L)) for (j in seq_len(sections)) {
        k <- j%%sections + 1L
        p <- (i - 1L) * sections
        q <- i * sections
        faces[[length(faces) + 1L]] <- c(p + j, p + k, q + k)
        faces[[length(faces) + 1L]] <- c(p + j, q + k, q + j)
    }
    for (j in seq_len(sections)) {
        k <- j%%sections + 1L
        faces[[length(faces) + 1L]] <- c(n * sections + 1L, k, j)
        faces[[length(faces) + 1L]] <- c(n * sections + 2L, (n - 1L) * sections + j, (n - 1L) * sections +
            k)
    }
    list(vertices = unname(vertices), faces = faces)
}


# .speciesFin
.speciesFin <- function(outline, normal, thickness, centre = NULL) {
    normal <- normal/sqrt(sum(normal^2))
    n <- nrow(outline)
    if (is.null(centre))
        centre <- colMeans(outline)
    area <- Reduce(`+`, lapply(seq_len(n), function(i) {
        a <- outline[i, ] - centre
        b <- outline[i%%n + 1L, ] - centre
        c(a[2] * b[3] - a[3] * b[2], a[3] * b[1] - a[1] * b[3], a[1] * b[2] - a[2] * b[1])
    }))
    if (sum(area * normal) < 0)
        outline <- outline[n:1L, , drop = FALSE]
    centre <- .speciesFinHub(outline, normal, centre)
    if (is.null(centre))
        return(.speciesExtrudedFin(outline, normal, thickness))
    vertices <- rbind(outline, centre + normal * thickness, centre - normal * thickness)
    faces <- unlist(lapply(seq_len(n), function(i) list(c(n + 1L, i, i%%n + 1L), c(n + 2L, i%%n + 1L,
        i))), recursive = FALSE)
    list(vertices = unname(vertices), faces = faces)
}


# .speciesFoil
.speciesFoil <- function(component) {
    n <- nrow(component$leading)
    k <- component$sections
    a <- 2 * pi * (seq_len(k) - 1L)/k
    normal <- component$normal/sqrt(sum(component$normal^2))
    vertices <- if (!is.null(component$section.vertices))
        component$section.vertices
    else do.call(rbind, lapply(seq_len(n), function(i) {
        middle <- (component$leading[i, ] + component$trailing[i, ])/2
        chord <- (component$leading[i, ] - component$trailing[i, ])/2
        matrix(middle, k, 3, byrow = TRUE) + outer(cos(a), chord) + outer(sin(a) * component$thickness[i],
            normal)
    }))
    vertices <- rbind(vertices, colMeans(rbind(component$leading[1, ], component$trailing[1, ])), colMeans(rbind(component$leading[n,
        ], component$trailing[n, ])))
    faces <- list()
    for (i in seq_len(n - 1L)) for (j in seq_len(k)) {
        next.j <- j%%k + 1L
        p <- (i - 1L) * k
        q <- i * k
        faces <- c(faces, list(c(p + j, p + next.j, q + next.j), c(p + j, q + next.j, q + j)))
    }
    for (j in seq_len(k)) {
        next.j <- j%%k + 1L
        faces <- c(faces, list(c(n * k + 1L, next.j, j), c(n * k + 2L, (n - 1L) * k + j, (n - 1L) * k +
            next.j)))
    }
    cross <- function(a, b) c(a[2] * b[3] - a[3] * b[2], a[3] * b[1] - a[1] * b[3], a[1] * b[2] - a[2] *
        b[1])
    volume <- sum(vapply(faces, function(f) sum(vertices[f[1], ] * cross(vertices[f[2], ], vertices[f[3],
        ])), numeric(1)))
    if (volume < 0)
        faces <- lapply(faces, rev)
    list(vertices = vertices, faces = faces)
}


# .speciesFinSpec
.speciesFinSpec <- function(id, outline, normal = c(0, 0, 1), thickness = 0.015, role = "rigid", joint.x = NA_real_,
    centre = NULL, part = "fin") {
    list(id = id, kind = "fin", outline = outline, normal = normal, thickness = thickness, centre = centre,
        role = role, joint.x = joint.x, part = part, pivot = c(if (is.finite(joint.x)) joint.x else mean(outline[c(1,
            nrow(outline)), 1]), colMeans(outline[c(1, nrow(outline)), 2:3, drop = FALSE])))
}


# .speciesPair
.speciesPair <- function(fin, right.id, left.id) {
    right <- fin
    right$id <- right.id
    left <- fin
    left$id <- left.id
    left$outline[, 2] <- -left$outline[, 2]
    left$normal[2] <- -left$normal[2]
    if (!is.null(left$centre))
        left$centre[2] <- -left$centre[2]
    if (!is.null(left$pivot))
        left$pivot[2] <- -left$pivot[2]
    list(right, left)
}


# .speciesBezier
.speciesBezier <- function(segments, steps = 4L) {
    pieces <- lapply(seq_along(segments), function(i) {
        p <- segments[[i]]
        t <- seq(0, 1, length.out = steps + 1L)
        if (i > 1L)
            t <- t[-1L]
        cbind((1 - t)^3, 3 * (1 - t)^2 * t, 3 * (1 - t) * t^2, t^3) %*% p
    })
    unname(do.call(rbind, pieces))
}


# .speciesFinHub
.speciesFinHub <- function(outline, normal, preferred) {
    seed <- diag(3)[, which.min(abs(normal))]
    u <- seed - sum(seed * normal) * normal
    u <- u/sqrt(sum(u^2))
    v <- c(normal[2] * u[3] - normal[3] * u[2], normal[3] * u[1] - normal[1] * u[3], normal[1] * u[2] -
        normal[2] * u[1])
    xy <- outline %*% cbind(u, v)
    cross <- function(a, b) a[1] * b[2] - a[2] * b[1]
    inside <- function(point) all(vapply(seq_len(nrow(xy)), function(i) {
        j <- i%%nrow(xy) + 1L
        cross(xy[j, ] - xy[i, ], point - xy[i, ]) > 1e-12
    }, logical(1)))
    point <- as.numeric(preferred %*% cbind(u, v))
    if (inside(point))
        return(preferred)
    lo <- apply(xy, 2, min)
    hi <- apply(xy, 2, max)
    kernel <- rbind(lo, c(hi[1], lo[2]), hi, c(lo[1], hi[2]))
    for (i in seq_len(nrow(xy))) {
        if (!nrow(kernel))
            break
        a <- xy[i, ]
        edge <- xy[i%%nrow(xy) + 1L, ] - a
        clipped <- list()
        for (k in seq_len(nrow(kernel))) {
            p <- kernel[k, ]
            q <- kernel[k%%nrow(kernel) + 1L, ]
            dp <- cross(edge, p - a)
            dq <- cross(edge, q - a)
            if (dp >= 0)
                clipped[[length(clipped) + 1L]] <- p
            if ((dp >= 0) != (dq >= 0))
                clipped[[length(clipped) + 1L]] <- p + (q - p) * dp/(dp - dq)
        }
        kernel <- if (length(clipped))
            do.call(rbind, clipped)
        else matrix(numeric(), 0L, 2L)
    }
    if (nrow(kernel) < 3L || !inside(colMeans(kernel)))
        return(NULL)
    point <- colMeans(kernel)
    as.numeric(point[1] * u + point[2] * v + mean(outline %*% normal) * normal)
}


# .speciesExtrudedFin
.speciesExtrudedFin <- function(outline, normal, thickness) {
    axis <- which.max(abs(normal))
    xy <- outline[, -axis, drop = FALSE]
    cross <- function(a, b) a[1] * b[2] - a[2] * b[1]
    area <- sum(xy[, 1] * xy[c(2:nrow(xy), 1), 2] - xy[, 2] * xy[c(2:nrow(xy), 1), 1])
    sense <- sign(area)
    ix <- seq_len(nrow(xy))
    triangles <- list()
    while (length(ix) > 3L) {
        found <- FALSE
        for (i in seq_along(ix)) {
            a <- ix[(i - 2L)%%length(ix) + 1L]
            b <- ix[i]
            c <- ix[i%%length(ix) + 1L]
            if (sense * cross(xy[b, ] - xy[a, ], xy[c, ] - xy[b, ]) <= 1e-12)
                next
            other <- setdiff(ix, c(a, b, c))
            inside <- vapply(other, function(k) {
                p <- xy[k, ]
                all(sense * c(cross(xy[b, ] - xy[a, ], p - xy[a, ]), cross(xy[c, ] - xy[b, ], p - xy[b,
                  ]), cross(xy[a, ] - xy[c, ], p - xy[c, ])) >= -1e-12)
            }, logical(1))
            if (any(inside))
                next
            triangles[[length(triangles) + 1L]] <- c(a, b, c)
            ix <- ix[-i]
            found <- TRUE
            break
        }
        if (!found)
            .abort("A fin contour cannot be triangulated; check for self-intersections.")
    }
    if (sense * cross(xy[ix[2], ] - xy[ix[1], ], xy[ix[3], ] - xy[ix[1], ]) <= 1e-12)
        .abort("A fin contour has a crossed or degenerate remainder.")
    triangles[[length(triangles) + 1L]] <- ix
    n <- nrow(outline)
    distance <- sqrt(rowSums(sweep(xy, 2L, (xy[1, ] + xy[n, ])/2, "-")^2))
    taper <- 0.15 + 0.85 * (1 - distance/max(distance))^0.65
    offset <- outer(taper * thickness, normal)
    vertices <- rbind(outline + offset, outline - offset)
    faces <- c(triangles, lapply(triangles, function(f) rev(f + n)))
    for (i in seq_len(n)) {
        j <- i%%n + 1L
        faces <- c(faces, list(c(i, i + n, j + n), c(i, j + n, j)))
    }
    list(vertices = unname(vertices), faces = faces)
}


# .buildSpeciesMesh
.buildSpeciesMesh <- function(spec) {
    vertices <- matrix(numeric(), 0L, 3L)
    faces <- list()
    parts <- character()
    region <- character()
    face.region <- character()
    components <- list()
    add <- function(mesh, id, part, role, joint.x = NA_real_, pivot = c(0, 0, 0)) {
        nv <- nrow(vertices)
        nf <- length(faces)
        vertices <<- rbind(vertices, mesh$vertices)
        faces <<- c(faces, lapply(mesh$faces, function(f) as.integer(f + nv)))
        parts <<- c(parts, rep(part, length(mesh$faces)))
        region <<- c(region, rep(id, nrow(mesh$vertices)))
        face.region <<- c(face.region, rep(id, length(mesh$faces)))
        components[[length(components) + 1L]] <<- data.frame(region = id, part = part, role = role, joint.x = joint.x,
            first.vertex = nv + 1L, vertices = nrow(mesh$vertices), first.face = nf + 1L, faces = length(mesh$faces),
            pivot.x = pivot[1], pivot.y = pivot[2], pivot.z = pivot[3], stringsAsFactors = FALSE)
    }
    body <- .speciesLoft(spec$body, spec$sections, spec$body.angles)
    if (!is.null(spec$body.transform))
        body$vertices <- spec$body.transform(body$vertices)
    add(body, "body", "body", spec$body.role, pivot = spec$rig$pivot)
    for (component in spec$appendages) {
        mesh <- if (identical(component$kind, "loft"))
            .speciesLoft(component$stations, component$sections)
        else if (component$kind == "foil")
            .speciesFoil(component)
        else .speciesFin(component$outline, component$normal, component$thickness, component$centre)
        if (identical(component$axis, "y")) {
            centreline <- c(rep(component$centreline, each = component$sections), component$centreline[c(1L,
                length(component$centreline))])
            mesh$vertices <- cbind(centreline + mesh$vertices[, 2], mesh$vertices[, 1], mesh$vertices[,
                3])
            mesh$faces <- lapply(mesh$faces, rev)
        }
        else if (!is.null(component$centreline)) {
            mesh$vertices[, 2] <- mesh$vertices[, 2] + c(rep(component$centreline, each = component$sections),
                component$centreline[c(1L, length(component$centreline))])
        }
        if (!is.null(component$offset))
            mesh$vertices <- sweep(mesh$vertices, 2L, component$offset, "+")
        pivot <- if (!is.null(component$pivot))
            component$pivot
        else c(if (is.finite(component$joint.x)) component$joint.x else mean(mesh$vertices[, 1]), 0,
            0)
        add(mesh, component$id, component$part, component$role, component$joint.x, pivot)
    }
    list(vertices = vertices, faces = faces, part = parts, region = region, face.region = face.region,
        components = do.call(rbind, components), details = spec$details)
}


# .speciesWeights
.speciesWeights <- function(geometry, rig) {
    .assert_choice(rig$type, "metadata rig$type", c("lateral", "vertical", "flippers"))
    weights <- numeric(nrow(geometry$vertices))
    for (i in seq_len(nrow(geometry$components))) {
        component <- geometry$components[i, ]
        ix <- component$first.vertex + seq_len(component$vertices) - 1L
        role <- component$role
        .assert_choice(role, "component motion role", c("rigid", "axial", "terminal", "attached", "flipper"))
        if (role == "rigid")
            next
        if (rig$type == "flippers") {
            if (role != "flipper")
                .abort("A flipper rig contains a non-flipper moving component.")
            if (identical(rig$flipper.mode, "rigid")) {
                weights[ix] <- 1
                next
            }
            span <- abs(geometry$vertices[ix, 2L])
            weights[ix] <- pmin(1, pmax(0, (span - abs(rig$pivot[2]))/(rig$tip - abs(rig$pivot[2]))))^rig$exponent
        }
        else {
            if (role == "flipper")
                .abort("An axial rig contains a flipper component.")
            x <- if (role == "attached")
                rep(component$joint.x, length(ix))
            else geometry$vertices[ix, 1L]
            weights[ix] <- if (role == "terminal")
                1
            else pmin(1, pmax(0, (rig$pivot[1] - x)/(rig$pivot[1] - rig$tip)))^rig$exponent
        }
    }
    weights
}


# .prepareOverlayModel
.prepareOverlayModel <- function(model = "blue_shark", max.angle = 0) {
    .assert_number(max.angle, "max.angle", min = 0, max = 45)
    if (is.character(model)) {
        .assert_choice(model, "orientation.model", names(.orientationModelRegistry()))
        model <- overlaySpeciesModel(model)
    }
    if (!inherits(model, "nautilus_overlay_model") || !is.list(model))
        .abort("{.arg orientation.model} must name a built-in model or be created by {.fn overlayModel}.")
    .assert_string(model$name, "model$name")
    .assert_choice(model$source, "model$source", c("custom", "builtin"))
    .validateOverlayVertices(model$vertices)
    model$faces <- .validateOverlayFaces(model$faces, model$vertices, triangles.only = model$source ==
        "custom")
    model$triangles <- if (model$source == "custom")
        do.call(rbind, model$faces)
    else NULL
    .validateOverlayParts(model$part, length(model$faces))
    if (!is.list(model$rig))
        .abort("The model has no valid motion definition.")
    model$rig <- .validateOverlayRig(model$rig$type, model$rig$weights, model$rig$pivot, nrow(model$vertices))
    if (!is.list(model$details))
        .abort("The model has invalid surface details.")
    for (line in model$details) {
        if (!is.matrix(line) || !is.numeric(line) || ncol(line) != 3L || any(!is.finite(line)))
            .abort("Model surface details must be finite matrices with three columns.")
    }
    if (!is.null(model$detail.weights)) {
        if (!is.list(model$detail.weights) || length(model$detail.weights) != length(model$details))
            .abort("Detail motion weights must match the model details.")
        for (i in seq_along(model$details)) {
            w <- model$detail.weights[[i]]
            if (!is.numeric(w) || length(w) != nrow(model$details[[i]]) || any(!is.finite(w)) || any(w <
                0 | w > 1))
                .abort("Detail motion weights must be finite values in [0,1], one per point.")
        }
    }
    model$appearance <- .validateOverlayAppearance(model$appearance, length(model$faces))
    if (!is.null(model$eyes) && (!is.matrix(model$eyes) || !is.numeric(model$eyes) || ncol(model$eyes) !=
        3L || any(!is.finite(model$eyes))))
        .abort("Model eyes must be a finite matrix with three columns.")
    rig <- model$rig
    pivots <- matrix(rig$pivot, nrow(model$vertices), 3L, byrow = TRUE)
    if (rig$type == "flippers")
        pivots[, 2L] <- abs(rig$pivot[2L]) * sign(model$vertices[, 2L])
    lever <- sqrt(rowSums((model$vertices - pivots)^2))
    bound <- max(sqrt(rowSums(model$vertices^2)) + 2 * lever * sin(max.angle * pi/360 * rig$weights))
    model$display.scale <- min(0.84, 1.08/bound)
    if (!is.null(model$appearance) && model$appearance$fit == "tight")
        model$display.scale <- min(1.1, 1.02/.overlayTightMotionBound(model, max.angle))
    model
}

# Motion ----------------------------------------------------------------


# .resolveOverlayMotion
.resolveOverlayMotion <- function(motion, data, model) {
    if (is.null(motion))
        return(NULL)
    if (!inherits(motion, "nautilus_overlay_motion") || !is.list(motion))
        .abort("{.arg motion} must be {.code NULL} or created by {.fn overlayMotionControl}.")
    motion <- overlayMotionControl(motion$method, motion$amplitude.reference, motion$max.angle, motion$phase.offset,
        motion$phase.lag)
    if (model$rig$type == "none")
        .abort("This model has no motion profile. Supply motion weights through {.fn overlayModel}.")
    method <- motion$method
    if (method == "auto") {
        candidates <- c("peaks", "wavelet")
        present <- vapply(candidates, function(m) paste0("tbf_hz_", m) %in% names(data), logical(1))
        positive <- vapply(candidates, function(m) {
            x <- data[[paste0("tbf_hz_", m)]]
            is.numeric(x) && any(is.finite(x) & x > 0)
        }, logical(1))
        if (!any(present))
            .abort("Schematic motion requires tail-beat frequency and amplitude columns.")
        method <- candidates[which(if (any(positive))
            positive
        else present)[1L]]
    }
    cols <- paste0(c("tbf_hz_", "tbf_amplitude_"), method)
    for (col in cols) {
        if (!col %in% names(data) || !is.numeric(data[[col]]))
            .abort("Schematic motion requires the numeric column {.val {col}} from the selected backend.")
    }
    if ("tbf_swimming" %in% names(data) && !is.logical(data$tbf_swimming))
        .abort("{.field tbf_swimming} must be logical when supplied.")
    valid <- is.finite(data[[cols[1]]]) & data[[cols[1]]] > 0 & is.finite(data[[cols[2]]]) & data[[cols[2]]] >=
        0
    if ("tbf_swimming" %in% names(data))
        valid <- valid & (is.na(data$tbf_swimming) | data$tbf_swimming)
    reference <- motion$amplitude.reference
    if (is.null(reference)) {
        amp <- data[[cols[2]]][valid & data[[cols[2]]] > 0]
        reference <- if (length(amp))
            unname(stats::quantile(amp, 0.95))
        else 1
    }
    motion$method <- method
    motion$amplitude.reference <- reference
    motion$columns <- cols
    motion
}


# .deformOverlayModel
.deformOverlayModel <- function(model, phase = 0, strength = 0, max.angle = 0, phase.lag = 0) {
    vertices <- model$vertices
    rig <- model$rig
    if (rig$type == "none" || strength <= 0 || max.angle == 0)
        return(vertices)
    theta <- max.angle * pi/180 * strength * rig$weights * sin(phase - phase.lag * rig$weights)
    p <- matrix(rig$pivot, nrow(vertices), 3L, byrow = TRUE)
    if (rig$type == "flippers") {
        p[, 2L] <- abs(rig$pivot[2L]) * sign(vertices[, 2L])
        theta <- theta * sign(vertices[, 2L])
    }
    v <- vertices - p
    co <- cos(theta)
    si <- sin(theta)
    axes <- switch(rig$type, lateral = c(1L, 2L), vertical = c(3L, 1L), flippers = c(2L, 3L))
    a <- axes[1]
    b <- axes[2]
    vertices[, a] <- co * v[, a] - si * v[, b] + p[, a]
    vertices[, b] <- si * v[, a] + co * v[, b] + p[, b]
    vertices
}


# .overlayTightMotionBound
.overlayTightMotionBound <- function(model, max.angle) {
    rig <- model$rig
    v <- model$vertices
    if (rig$type == "none")
        return(max(sqrt(rowSums(v^2))))
    p <- matrix(rig$pivot, nrow(v), 3L, byrow = TRUE)
    if (rig$type == "flippers")
        p[, 2] <- abs(rig$pivot[2]) * sign(v[, 2])
    axes <- switch(rig$type, lateral = c(1L, 2L), vertical = c(3L, 1L), flippers = c(2L, 3L))
    a <- axes[1]
    b <- axes[2]
    other <- setdiff(1:3, axes)
    d <- v - p
    A <- p[, a] * d[, a] + p[, b] * d[, b]
    B <- -p[, a] * d[, b] + p[, b] * d[, a]
    theta <- max.angle * pi/180 * rig$weights
    maximum <- pmax(A * cos(theta) + B * sin(theta), A * cos(theta) - B * sin(theta))
    inside <- abs(atan2(B, A)) <= theta
    maximum[inside] <- sqrt(A[inside]^2 + B[inside]^2)
    sqrt(max(p[, a]^2 + p[, b]^2 + d[, a]^2 + d[, b]^2 + v[, other]^2 + 2 * maximum))
}

# Appearance and attached markings --------------------------------------


# .validateOverlayAppearance
.validateOverlayAppearance <- function(appearance, n) {
    if (is.null(appearance))
        return(NULL)
    if (!is.list(appearance))
        .abort("Model appearance must be a list.")
    for (field in c("smooth", "cull")) .assert_flag(appearance[[field]], paste0("appearance$", field))
    .assert_choice(appearance$fit, "appearance$fit", c("legacy", "tight"))
    .assert_number(appearance$eye.cex, "appearance$eye.cex", min = 0.1, max = 2)
    for (field in c("body.faces", "hidden.faces")) {
        x <- appearance[[field]]
        if (!is.logical(x) || length(x) != n || anyNA(x))
            .abort("Appearance face masks must match the model faces.")
    }
    if (!is.null(appearance$smooth.faces) && (!is.logical(appearance$smooth.faces) || length(appearance$smooth.faces) !=
        n || anyNA(appearance$smooth.faces)))
        .abort("Appearance smooth face mask must match the model faces.")
    x <- appearance$belly
    if (!is.numeric(x) || length(x) != n || any(!is.finite(x)) || any(x < 0 | x > 1))
        .abort("Appearance countershading weights must match the faces and lie in [0,1].")
    x <- appearance$face.rgb
    if (!is.null(x) && (!is.matrix(x) || !is.numeric(x) || !identical(dim(x), c(as.integer(n), 3L)) ||
        any(!is.finite(x)) || any(x < 0 | x > 1)))
        .abort("Appearance face RGB colours must be a finite face-by-three matrix in [0,1].")
    x <- appearance$compass
    if (!is.numeric(x) || length(x) != 3L || any(!is.finite(x)) || x[3] <= 0 || x[3] > 0.5 || abs(x[1]) +
        x[3] > 1.35 || abs(x[2]) + x[3] > 1.55)
        .abort("Appearance compass must fit inside the inset viewport.")
    for (field in c("dorsal", "ventral")) {
        .assert_string(appearance[[field]], paste0("appearance$", field))
        tryCatch(grDevices::col2rgb(appearance[[field]]), error = function(e) .abort("Appearance colours must be valid R colours."))
    }
    if (!is.null(appearance$surface))
        appearance$surface <- .validateOverlaySurface(appearance$surface, n)
    appearance
}


# .overlayFaceColours
.overlayFaceColours <- function(model, intensity, body.col = "#ff453a", use.appearance = TRUE) {
    a <- model$appearance
    if (!is.null(a) && use.appearance) {
        dorsal <- grDevices::col2rgb(a$dorsal)[, 1]/255
        ventral <- grDevices::col2rgb(a$ventral)[, 1]/255
        rgb <- if (is.null(a$face.rgb))
            outer(1 - a$belly, dorsal) + outer(a$belly, ventral)
        else a$face.rgb
    }
    else rgb <- matrix(grDevices::col2rgb(body.col)[, 1]/255, length(intensity), 3L, byrow = TRUE)
    rgb <- rgb * intensity
    grDevices::rgb(rgb[, 1], rgb[, 2], rgb[, 3])
}


# .validateOverlaySurface
.validateOverlaySurface <- function(surface, nfaces) {
    if (is.null(surface))
        return(NULL)
    if (!is.list(surface))
        .abort("Surface markings must be a list.")
    f <- surface$face
    b <- surface$barycentric
    n <- length(f)
    if (!is.numeric(f) || !n || any(!is.finite(f)) || any(f != as.integer(f) | f < 1 | f > nfaces))
        .abort("Surface marking face indices must identify model faces.")
    if (!is.matrix(b) || !is.numeric(b) || !identical(dim(b), c(n, 3L)) || any(!is.finite(b)) || any(b <
        0 | b > 1) || any(abs(rowSums(b) - 1) > 1e-10))
        .abort("Surface marking barycentric weights must sum to one within each face.")
    if (!is.character(surface$kind) || length(surface$kind) != n || anyNA(surface$kind) || any(!surface$kind %in%
        c("spot", "eye", "line")))
        .abort("Surface marking kinds must be spot, eye or line.")
    g <- surface$group
    if (!is.numeric(g) || length(g) != n || any(!is.finite(g)) || any(g < 0 | g != as.integer(g)))
        .abort("Surface marking groups must be non-negative integers.")
    colours <- surface$line.colour
    if (!is.null(colours)) {
        if (!is.character(colours) || length(colours) != n || anyNA(colours))
            .abort("Surface line colours must contain one valid R colour per anchor.")
        tryCatch(grDevices::col2rgb(colours), error = function(e) .abort("Surface line colours must contain one valid R colour per anchor."))
    }
    surface
}


# .attachOverlaySurfaceRays
.attachOverlaySurfaceRays <- function(geometry, requests) {
    f <- do.call(rbind, geometry$faces)
    v <- geometry$vertices
    cross <- function(x, y) cbind(x[, 2] * y[, 3] - x[, 3] * y[, 2], x[, 3] * y[, 1] - x[, 1] * y[, 3],
        x[, 1] * y[, 2] - x[, 2] * y[, 1])
    attach <- function(request) {
        ids <- which(geometry$face.region %in% request$regions)
        a <- v[f[ids, 1], ]
        e <- v[f[ids, 2], ] - a
        g <- v[f[ids, 3], ] - a
        t(vapply(seq_len(nrow(request$origin)), function(i) {
            d <- matrix(request$direction[i, ], nrow(a), 3, byrow = TRUE)
            p <- cross(d, g)
            det <- rowSums(e * p)
            det[abs(det) < 1e-12] <- NA_real_
            t <- matrix(request$origin[i, ], nrow(a), 3, byrow = TRUE) - a
            u <- rowSums(t * p)/det
            q <- cross(t, e)
            w <- rowSums(d * q)/det
            distance <- rowSums(g * q)/det
            ok <- which(is.finite(distance) & distance >= 0 & u >= -1e-09 & w >= -1e-09 & u + w <= 1 +
                1e-09)
            if (!length(ok))
                .abort("A surface marking could not be attached to the geometry (group {request$group[i]}, point {i}).")
            k <- ok[which.min(distance[ok])]
            b <- pmax(0, c(1 - u[k] - w[k], u[k], w[k]))
            c(ids[k], b/sum(b))
        }, numeric(4)))
    }
    anchors <- do.call(rbind, lapply(requests, attach))
    list(face = as.integer(anchors[, 1]), barycentric = unname(anchors[, 2:4]), kind = unlist(lapply(requests,
        `[[`, "kind")), group = unlist(lapply(requests, `[[`, "group")))
}


# .overlaySurfacePoints
.overlaySurfacePoints <- function(model, vertices) {
    s <- model$appearance$surface
    triangles <- if (is.null(model$triangles))
        do.call(rbind, model$faces)
    else model$triangles
    f <- triangles[s$face, , drop = FALSE]
    vertices[f[, 1], , drop = FALSE] * s$barycentric[, 1] + vertices[f[, 2], , drop = FALSE] * s$barycentric[,
        2] + vertices[f[, 3], , drop = FALSE] * s$barycentric[, 3]
}


# .overlaySurfaceProjection
.overlaySurfaceProjection <- function(model, vertices, rotation, camera, projection) {
    s <- model$appearance$surface
    project <- function(v) v %*% t(rotation) %*% t(camera$view.rotation) %*% cbind(camera$right, camera$up,
        camera$depth) * model$display.scale
    p <- project(.overlaySurfacePoints(model, vertices))
    visible <- s$face %in% projection$order
    candidates <- which(visible)
    if (length(candidates) && length(projection$order)) {
        triangles <- if (is.null(model$triangles))
            do.call(rbind, model$faces)
        else model$triangles
        v <- project(vertices)
        f <- triangles[projection$order, , drop = FALSE]
        a <- v[f[, 1], , drop = FALSE]
        e <- v[f[, 2], , drop = FALSE] - a
        g <- v[f[, 3], , drop = FALSE] - a
        det <- e[, 1] * g[, 2] - e[, 2] * g[, 1]
        det[abs(det) < 1e-12] <- NA_real_
        dx <- outer(p[candidates, 1], a[, 1], `-`)
        dy <- outer(p[candidates, 2], a[, 2], `-`)
        u <- sweep(sweep(dx, 2, g[, 2], `*`) - sweep(dy, 2, g[, 1], `*`), 2, det, `/`)
        w <- sweep(sweep(dy, 2, e[, 1], `*`) - sweep(dx, 2, e[, 2], `*`), 2, det, `/`)
        depth <- sweep(sweep(u, 2, e[, 3], `*`) + sweep(w, 2, g[, 3], `*`), 2, a[, 3], `+`)
        covered <- u >= -1e-08 & w >= -1e-08 & u + w <= 1 + 1e-08 & depth < p[candidates, 3] - 1e-08
        covered[is.na(covered)] <- FALSE
        visible[candidates] <- rowSums(covered) == 0
    }
    list(points = p[, 1:2, drop = FALSE], visible = visible)
}


# .drawOverlaySurface
.drawOverlaySurface <- function(model, vertices, rotation, camera, projection) {
    s <- model$appearance$surface
    if (is.null(s))
        return(invisible(NULL))
    projected <- .overlaySurfaceProjection(model, vertices, rotation, camera, projection)
    p <- projected$points
    visible <- projected$visible
    spots <- visible & s$kind == "spot"
    if (any(spots))
        graphics::points(p[spots, , drop = FALSE], pch = 16, cex = 0.27 * model$display.scale, col = grDevices::adjustcolor("#d8e4dc",
            alpha.f = 0.65))
    eyes <- visible & s$kind == "eye"
    if (any(eyes))
        graphics::points(p[eyes, , drop = FALSE], pch = 21, cex = model$appearance$eye.cex, bg = "#101820",
            col = "#92a6ab", lwd = 0.6)
    next.i <- seq_len(nrow(p) - 1L)
    j <- next.i + 1L
    lines <- visible[next.i] & visible[j] & s$kind[next.i] == "line" & s$kind[j] == "line" & s$group[next.i] ==
        s$group[j]
    if (any(lines))
        graphics::segments(p[next.i[lines], 1], p[next.i[lines], 2], p[j[lines], 1], p[j[lines], 2],
            col = grDevices::adjustcolor(if (is.null(s$line.colour))
                "#1e3540"
            else s$line.colour[next.i[lines]], 0.75), lwd = 0.8)
    invisible(NULL)
}

# Projection, lighting and drawing --------------------------------------


# .faceNormal
.faceNormal <- function(w) {
    e1 <- w[, 2] - w[, 1]
    e2 <- w[, 3] - w[, 1]
    n <- c(e1[2] * e2[3] - e1[3] * e2[2], e1[3] * e2[1] - e1[1] * e2[3], e1[1] * e2[2] - e1[2] * e2[1])
    nn <- sqrt(sum(n^2))
    if (is.finite(nn) && nn > 0)
        n/nn
    else c(0, 0, 1)
}


# .shadeColor
.shadeColor <- function(col, s) {
    s <- max(0, min(1, s))
    rgb <- grDevices::col2rgb(col)[, 1]/255
    grDevices::rgb(rgb[1] * s, rgb[2] * s, rgb[3] * s)
}


# .overlayAttitudeRotation
.overlayAttitudeRotation <- function(pitch, roll) {
    th <- (if (is.finite(pitch))
        pitch
    else 0) * pi/180
    ro <- (if (is.finite(roll))
        roll
    else 0) * pi/180
    rx <- matrix(c(1, 0, 0, 0, cos(ro), -sin(ro), 0, sin(ro), cos(ro)), 3L, byrow = TRUE)
    ry <- matrix(c(cos(th), 0, sin(th), 0, 1, 0, -sin(th), 0, cos(th)), 3L, byrow = TRUE)
    ry %*% rx
}


# .overlayAttitudeCamera
.overlayAttitudeCamera <- function() {
    elevation <- 24 * pi/180
    co <- cos(elevation)
    si <- sin(elevation)
    list(right = c(0, 1, 0), up = c(si, 0, -co), depth = c(co, 0, si), view.rotation = matrix(c(co, 0,
        -si, 0, 1, 0, si, 0, co), 3L, byrow = TRUE))
}


# .projectPreparedOverlayModel
.projectPreparedOverlayModel <- function(model, vertices, rotation, camera = .overlayAttitudeCamera()) {
    world <- vertices %*% t(rotation)
    light <- c(0.25, -0.3, -0.92)
    light <- light/sqrt(sum(light^2))
    view <- world %*% t(camera$view.rotation)
    projected <- view %*% cbind(camera$right, camera$up, camera$depth)
    face.depth <- vapply(model$faces, function(f) mean(projected[f, 3L]), numeric(1))
    if (!is.null(model$triangles)) {
        f <- model$triangles
        e1 <- world[f[, 2L], , drop = FALSE] - world[f[, 1L], , drop = FALSE]
        e2 <- world[f[, 3L], , drop = FALSE] - world[f[, 1L], , drop = FALSE]
        normals <- cbind(e1[, 2L] * e2[, 3L] - e1[, 3L] * e2[, 2L], e1[, 3L] * e2[, 1L] - e1[, 1L] *
            e2[, 3L], e1[, 1L] * e2[, 2L] - e1[, 2L] * e2[, 1L])
        lengths <- sqrt(rowSums(normals^2))
        valid <- is.finite(lengths) & lengths > 0
        normals[valid, ] <- normals[valid, , drop = FALSE]/lengths[valid]
        normals[!valid, ] <- rep(c(0, 0, 1), each = sum(!valid))
        intensity <- 0.4 + 0.6 * abs(as.vector(normals %*% light))
    }
    else {
        intensity <- vapply(model$faces, function(f) {
            0.4 + 0.6 * abs(sum(.faceNormal(t(world[f, , drop = FALSE])) * light))
        }, numeric(1))
        if (!is.null(model$appearance))
            normals <- t(vapply(model$faces, function(f) .faceNormal(t(world[f, , drop = FALSE])), numeric(3)))
    }
    a <- model$appearance
    visible <- rep(TRUE, length(model$faces))
    if (!is.null(a)) {
        if (a$smooth) {
            f <- do.call(rbind, model$faces)
            sums <- rowsum(normals[rep(seq_len(nrow(f)), 3), , drop = FALSE], as.vector(f), reorder = FALSE)
            vertex.normals <- matrix(0, nrow(world), 3L)
            vertex.normals[as.integer(rownames(sums)), ] <- sums
            smooth <- vertex.normals[f[, 1], , drop = FALSE] + vertex.normals[f[, 2], , drop = FALSE] +
                vertex.normals[f[, 3], , drop = FALSE]
            lengths <- sqrt(rowSums(smooth^2))
            smooth.faces <- if (is.null(a$smooth.faces))
                a$body.faces
            else a$smooth.faces
            ok <- smooth.faces & is.finite(lengths) & lengths > 0
            smooth[ok, ] <- smooth[ok, , drop = FALSE]/lengths[ok]
            intensity[ok] <- 0.65 + 0.35 * abs(as.vector(smooth[ok, , drop = FALSE] %*% light))
        }
        visible <- !a$hidden.faces
        if (a$cull)
            visible <- visible & as.vector(normals %*% t(camera$view.rotation) %*% camera$depth) < -1e-10
    }
    range.depth <- diff(range(face.depth))
    dimming <- if (range.depth > 0)
        (face.depth - min(face.depth))/range.depth
    else face.depth * 0
    order <- order(face.depth, decreasing = TRUE)
    list(points = projected[, 1:2, drop = FALSE] * model$display.scale, order = order[visible[order]],
        intensity = intensity * (0.72 + 0.28 * (1 - dimming)))
}


# .drawPreparedAttitudeModel
.drawPreparedAttitudeModel <- function(fd, theme, model, motion, label = NULL, body.col = "#ff453a",
    show.heading = TRUE, pitch.col = "pitch", roll.col = "roll") {
    pitch <- .scalarValue(fd, pitch.col)
    roll <- .scalarValue(fd, roll.col)
    heading <- .scalarValue(fd, "heading")
    valid.attitude <- is.finite(pitch) && is.finite(roll)
    graphics::plot(0, 0, type = "n", xlim = c(-1.35, 1.35), ylim = c(-1.55, 1.55), axes = FALSE, ann = FALSE,
        asp = 1)
    .drawPanelBackground(theme)
    graphics::symbols(0, 0, circles = 1.05, inches = FALSE, add = TRUE, bg = grDevices::adjustcolor("black",
        0.58), fg = theme$border)
    graphics::segments(-0.94, 0, 0.94, 0, col = grDevices::adjustcolor("#64d2ff", 0.35), lwd = 1)
    rotation <- .overlayAttitudeRotation(pitch, roll)
    vertices <- if (is.null(motion) || !valid.attitude)
        model$vertices
    else .deformOverlayModel(model, fd$.motion.phase, fd$.motion.strength, motion$max.angle, motion$phase.lag)
    camera <- .overlayAttitudeCamera()
    projection <- .projectPreparedOverlayModel(model, vertices, rotation, camera)
    use.appearance <- missing(body.col) && valid.attitude
    if (use.appearance && !is.null(model$appearance))
        body.col <- model$appearance$dorsal
    if (!valid.attitude)
        body.col <- theme$muted
    colours <- .overlayFaceColours(model, projection$intensity, body.col, use.appearance)
    borders <- grDevices::adjustcolor(colours, alpha.f = 0.5)
    borders[model$part == "fin"] <- grDevices::adjustcolor("grey10", 0.85)
    if (!is.null(model$appearance))
        borders <- colours
    if (!is.null(model$triangles)) {
        indices <- as.vector(t(cbind(model$triangles[projection$order, , drop = FALSE], NA_integer_)))
        graphics::polygon(projection$points[indices, 1L], projection$points[indices, 2L], col = colours[projection$order],
            border = borders[projection$order], lwd = 0.6)
    }
    else {
        for (i in projection$order) {
            p <- projection$points[model$faces[[i]], , drop = FALSE]
            graphics::polygon(p[, 1], p[, 2], col = colours[i], border = borders[i], lwd = 0.6)
        }
    }
    if (use.appearance && !is.null(model$appearance$surface))
        .drawOverlaySurface(model, vertices, rotation, camera, projection)
    dorsal.visible <- sum(camera$depth * (camera$view.rotation %*% rotation %*% c(0, 0, -1))) < 0
    project.detail <- function(p, weights = NULL) {
        if (!is.null(weights) && !is.null(motion) && valid.attitude) {
            rig <- model$rig
            rig$weights <- weights
            p <- .deformOverlayModel(list(vertices = p, rig = rig), fd$.motion.phase, fd$.motion.strength,
                motion$max.angle, motion$phase.lag)
        }
        view <- p %*% t(rotation) %*% t(camera$view.rotation)
        view %*% cbind(camera$right, camera$up) * model$display.scale
    }
    if (dorsal.visible) {
        for (i in seq_along(model$details)) {
            weights <- if (is.null(model$detail.weights))
                NULL
            else model$detail.weights[[i]]
            p <- project.detail(model$details[[i]], weights)
            graphics::lines(p[, 1], p[, 2], col = .shadeColor(body.col, 0.42), lwd = 1.1)
        }
        if (!is.null(model$eyes)) {
            p <- project.detail(model$eyes)
            eye.cex <- if (is.null(model$appearance))
                0.62
            else model$appearance$eye.cex
            graphics::points(p[, 1], p[, 2], pch = 21, cex = eye.cex, bg = "#11161a", col = .shadeColor(body.col,
                0.48), lwd = 0.7)
        }
    }
    if (isTRUE(show.heading)) {
        cx <- 0.91
        cy <- 0.93
        radius <- 0.25
        if (!is.null(model$appearance)) {
            cx <- model$appearance$compass[1]
            cy <- model$appearance$compass[2]
            radius <- model$appearance$compass[3]
        }
        graphics::symbols(cx, cy, circles = radius, inches = FALSE, add = TRUE, bg = grDevices::adjustcolor("black",
            0.72), fg = theme$muted)
        graphics::text(cx, cy + radius + 0.08, "N", cex = 0.55, font = 2, col = theme$text)
        if (is.finite(heading)) {
            rad <- (90 - heading) * pi/180
            graphics::arrows(cx, cy, cx + cos(rad) * radius * 0.72, cy + sin(rad) * radius * 0.72, length = 0.08,
                lwd = 2, col = "#ff453a")
        }
    }
    if (!is.null(label))
        graphics::text(0, 1.38, label, cex = 1.35, font = 2, col = theme$text, xpd = NA)
    status <- if (!valid.attitude)
        "Orientation unavailable"
    else if (!is.null(motion)) {
        switch(fd$.motion.status, estimated = NULL, gliding = "Gliding", "Motion unavailable")
    }
    else NULL
    animated <- !is.null(motion)
    value.cex <- if (animated)
        0.98
    else 1.05
    if (!is.null(status))
        graphics::text(0, -1.12, status, cex = if (animated)
            0.6
        else 0.65, col = theme$muted)
    values <- if (show.heading)
        sprintf("H %s°   P %s°   R %s°", .formatOverlayValue(heading, 0), .formatOverlayValue(pitch,
            0), .formatOverlayValue(roll, 0))
    else sprintf("Roll %s°  Pitch %s°", .formatOverlayValue(roll, 0, signed = TRUE), .formatOverlayValue(pitch,
        0, signed = TRUE))
    graphics::text(0, if (animated)
        -1.29
    else -1.38, values, cex = value.cex, col = theme$value, xpd = NA, font = 2)
    if (animated) {
        frequency <- .scalarValue(fd, ".motion.frequency")
        supported <- identical(fd$.motion.status, "estimated") && is.finite(frequency) && frequency >
            0
        tbf <- if (supported)
            sprintf("TBF: %.2f Hz", frequency)
        else "TBF: NA"
        graphics::text(0, -1.49, tbf, cex = value.cex, col = theme$value, font = 2, xpd = NA)
    }
    invisible(NULL)
}
