# Species mesh construction for the opt-in library.
# Components are closed surfaces with consistent winding. Fin roots are embedded in
# the body, rather than coincident sheets; the existing painter still has no z-buffer.

.validateSpeciesSpec <- function(spec) {
  .assert_string(spec$label, "model label")
  .assert_string(spec$taxon, "model taxon")
  .assert_count(spec$phase, "model phase")
  if (!spec$phase %in% 1:2) .abort("Model phase must be 1 or 2.")
  .assert_count(spec$sections, "body sections", min = 6L)
  .assert_choice(spec$body.role, "body motion role", c("axial", "rigid"))
  for (field in c("features", "references")) {
    x <- spec[[field]]
    if (!is.character(x) || (field == "features" && !length(x)) || anyNA(x) || any(!nzchar(x)))
      .abort("Model {.val {field}} must contain non-empty strings.")
  }
  if (!is.list(spec$rig)) .abort("A species model must declare its rig metadata.")
  rig <- spec$rig
  .assert_choice(rig$type, "rig type", c("lateral", "vertical", "flippers"))
  .assert_number(rig$tip, "rig tip")
  .assert_number(rig$exponent, "rig exponent", min = .Machine$double.eps)
  .assert_string(rig$weighting, "rig weighting")
  if (!is.numeric(rig$pivot) || length(rig$pivot) != 3L || any(!is.finite(rig$pivot)))
    .abort("Rig pivot must contain three finite coordinates.")
  if (rig$type == "flippers") {
    if (!is.null(rig$flipper.mode))
      .assert_choice(rig$flipper.mode, "flipper mode", c("flexible", "rigid"))
    if (spec$body.role != "rigid" || rig$tip <= abs(rig$pivot[2]))
      .abort("A flipper rig requires a rigid body and a tip beyond its pivot.")
  } else if (spec$body.role != "axial" || rig$tip >= rig$pivot[1])
    .abort("An axial rig requires an axial body and a tail tip behind its pivot.")
  validate.stations <- function(x) {
    if (!is.matrix(x) || !is.numeric(x) || ncol(x) != 5L || nrow(x) < 3L ||
        any(!is.finite(x)) || any(diff(x[, 1]) <= 0) || any(x[, 2:4] <= 0))
      .abort("Loft stations must have increasing x and positive width/dorsal/ventral radii.")
  }
  validate.stations(spec$body)
  if (!is.null(spec$body.angles) && (!is.numeric(spec$body.angles) ||
      length(spec$body.angles) != spec$sections || any(!is.finite(spec$body.angles)) ||
      any(diff(spec$body.angles) <= 0) || min(spec$body.angles) < 0 || max(spec$body.angles) >= 2*pi))
    .abort("Body ring angles must increase within [0, 2*pi) and match body sections.")
  if (!is.list(spec$appendages) || !length(spec$appendages))
    .abort("A species model must declare its appendages.")
  ids <- character()
  for (component in spec$appendages) {
    .assert_string(component$id, "component id")
    .assert_choice(component$kind, "component kind", c("loft", "fin", "foil"))
    .assert_choice(component$part, "component part", c("body", "fin"))
    .assert_choice(component$role, "component motion role",
                    c("rigid", "axial", "terminal", "attached", "flipper"))
    if (component$role == "attached") .assert_number(component$joint.x, "component joint.x")
    if (!is.null(component$offset) && (!is.numeric(component$offset) ||
        length(component$offset) != 3L || any(!is.finite(component$offset))))
      .abort("Component offset must contain three finite coordinates.")
    if (component$kind == "loft") {
      validate.stations(component$stations)
      .assert_count(component$sections, "component sections", min = 6L)
      if (!is.null(component$axis)) .assert_choice(component$axis, "loft axis", c("x", "y"))
      if (!is.null(component$centreline) && (!is.numeric(component$centreline) ||
          length(component$centreline) != nrow(component$stations) || any(!is.finite(component$centreline))))
        .abort("A curved loft needs one finite transverse centreline coordinate per station.")
      if (identical(component$axis,"y") && is.null(component$centreline))
        .abort("A transverse loft needs centreline coordinates.")
    } else if (component$kind == "foil") {
      .validateOverlayVertices(component$leading)
      .validateOverlayVertices(component$trailing)
      if (!identical(dim(component$leading), dim(component$trailing)) ||
          length(component$thickness) != nrow(component$leading) ||
          any(!is.finite(component$thickness) | component$thickness <= 0))
        .abort("Foil edges and positive thicknesses must have matching stations.")
      .assert_count(component$sections, "foil sections", min = 4L)
      if (!is.null(component$section.vertices)) {
        .validateOverlayVertices(component$section.vertices)
        if (nrow(component$section.vertices)!=nrow(component$leading)*component$sections)
          .abort("Measured foil sections must match the station and section counts.")
      }
      if (!is.numeric(component$normal) || length(component$normal) != 3L ||
          any(!is.finite(component$normal)) || sum(component$normal^2) == 0)
        .abort("A foil normal must be a finite non-zero vector.")
    } else {
      .validateOverlayVertices(component$outline)
      .assert_number(component$thickness, "fin thickness", min = .Machine$double.eps)
      if (!is.numeric(component$normal) || length(component$normal) != 3L ||
          any(!is.finite(component$normal)) || sum(component$normal^2) == 0)
        .abort("A fin normal must be a non-zero vector with three finite coordinates.")
      if (!is.null(component$centre) && (!is.numeric(component$centre) ||
          length(component$centre) != 3L || any(!is.finite(component$centre))))
        .abort("Fin centre must contain three finite coordinates.")
    }
    ids <- c(ids, component$id)
  }
  if (anyDuplicated(c("body", ids))) .abort("Component ids must be unique, including the body.")
  invisible(spec)
}

.speciesLoft <- function(stations, sections = 12L, angles = NULL) {
  # Rows: x, half-width, dorsal height, ventral height, centre-z.
  n <- nrow(stations); a <- if (is.null(angles)) (seq_len(sections) - 1L) * 2*pi/sections else angles
  vertices <- do.call(rbind, lapply(seq_len(n), function(i) {
    s <- stations[i, ]
    cbind(rep(s[1], sections), s[2]*cos(a),
          s[5] + ifelse(sin(a) < 0, s[3], s[4])*sin(a))
  }))
  vertices <- rbind(vertices, c(stations[1, 1], 0, stations[1, 5]),
                    c(stations[n, 1], 0, stations[n, 5]))
  faces <- list()
  for (i in seq_len(n - 1L)) for (j in seq_len(sections)) {
    k <- j %% sections + 1L
    p <- (i - 1L)*sections; q <- i*sections
    faces[[length(faces) + 1L]] <- c(p + j, p + k, q + k)
    faces[[length(faces) + 1L]] <- c(p + j, q + k, q + j)
  }
  for (j in seq_len(sections)) {
    k <- j %% sections + 1L
    faces[[length(faces) + 1L]] <- c(n*sections + 1L, k, j)
    faces[[length(faces) + 1L]] <- c(n*sections + 2L,
                                    (n - 1L)*sections + j, (n - 1L)*sections + k)
  }
  list(vertices = unname(vertices), faces = faces)
}

.speciesFin <- function(outline, normal, thickness, centre = NULL) {
  # A tapered, double-sided fin volume; the perimeter is shared, not duplicated.
  # The kernel locates a valid interior hub for curved/falcate contours, avoiding
  # inverted triangles when a vertex-average falls outside a concave trailing edge.
  normal <- normal / sqrt(sum(normal^2))
  n <- nrow(outline)
  if (is.null(centre)) centre <- colMeans(outline)
  area <- Reduce(`+`, lapply(seq_len(n), function(i) {
    a <- outline[i, ] - centre; b <- outline[i %% n + 1L, ] - centre
    c(a[2]*b[3] - a[3]*b[2], a[3]*b[1] - a[1]*b[3], a[1]*b[2] - a[2]*b[1])
  }))
  if (sum(area * normal) < 0) outline <- outline[n:1L, , drop = FALSE]
  centre <- .speciesFinHub(outline, normal, centre)
  if (is.null(centre)) return(.speciesExtrudedFin(outline, normal, thickness))
  vertices <- rbind(outline, centre + normal*thickness, centre - normal*thickness)
  faces <- unlist(lapply(seq_len(n), function(i)
    list(c(n + 1L, i, i %% n + 1L), c(n + 2L, i %% n + 1L, i))), recursive = FALSE)
  list(vertices = unname(vertices), faces = faces)
}

.buildSpeciesMesh <- function(spec) {
  vertices <- matrix(numeric(), 0L, 3L); faces <- list(); parts <- character()
  region <- character(); face.region <- character(); components <- list()
  add <- function(mesh, id, part, role, joint.x = NA_real_, pivot = c(0, 0, 0),
                  allocation = "silhouette") {
    nv <- nrow(vertices); nf <- length(faces)
    vertices <<- rbind(vertices, mesh$vertices)
    faces <<- c(faces, lapply(mesh$faces, function(f) as.integer(f + nv)))
    parts <<- c(parts, rep(part, length(mesh$faces)))
    region <<- c(region, rep(id, nrow(mesh$vertices)))
    face.region <<- c(face.region, rep(id, length(mesh$faces)))
    components[[length(components) + 1L]] <<- data.frame(region = id, part = part,
      role = role, joint.x = joint.x, first.vertex = nv + 1L,
      vertices = nrow(mesh$vertices), first.face = nf + 1L,
      faces = length(mesh$faces), pivot.x = pivot[1], pivot.y = pivot[2],
      pivot.z = pivot[3], allocation = allocation, stringsAsFactors = FALSE)
  }
  body <- .speciesLoft(spec$body, spec$sections, spec$body.angles)
  if (!is.null(spec$body.transform)) body$vertices <- spec$body.transform(body$vertices)
  add(body, "body", "body", spec$body.role, pivot = spec$rig$pivot,
      allocation = "head / trunk / peduncle")
  for (component in spec$appendages) {
    mesh <- if (identical(component$kind, "loft"))
      .speciesLoft(component$stations, component$sections) else if (component$kind == "foil")
      .speciesFoil(component) else
      .speciesFin(component$outline, component$normal,
                              component$thickness, component$centre)
    if (identical(component$axis, "y")) {
      centreline <- c(rep(component$centreline, each = component$sections),
                      component$centreline[c(1L, length(component$centreline))])
      mesh$vertices <- cbind(centreline + mesh$vertices[, 2],
                             mesh$vertices[, 1], mesh$vertices[, 3])
      # Swapping x/y reflects the mesh. Restore outward face winding.
      mesh$faces <- lapply(mesh$faces, rev)
    } else if (!is.null(component$centreline)) {
      mesh$vertices[,2] <- mesh$vertices[,2] + c(rep(component$centreline,each=component$sections),
        component$centreline[c(1L,length(component$centreline))])
    }
    if (!is.null(component$offset))
      mesh$vertices <- sweep(mesh$vertices, 2L, component$offset, "+")
    pivot <- if (!is.null(component$pivot)) component$pivot else
      c(if (is.finite(component$joint.x)) component$joint.x else mean(mesh$vertices[, 1]), 0, 0)
    add(mesh, component$id, component$part, component$role, component$joint.x, pivot,
        if (is.null(component$allocation)) "silhouette" else component$allocation)
  }
  list(vertices = vertices, faces = faces, part = parts, region = region,
       face.region = face.region, components = do.call(rbind, components),
       details = spec$details)
}

.speciesWeights <- function(geometry, rig) {
  .assert_choice(rig$type, "metadata rig$type", c("lateral", "vertical", "flippers"))
  weights <- numeric(nrow(geometry$vertices))
  for (i in seq_len(nrow(geometry$components))) {
    component <- geometry$components[i, ]
    ix <- component$first.vertex + seq_len(component$vertices) - 1L
    role <- component$role
    .assert_choice(role, "component motion role", c("rigid", "axial", "terminal", "attached", "flipper"))
    if (role == "rigid") next
    if (rig$type == "flippers") {
      if (role != "flipper") .abort("A flipper rig contains a non-flipper moving component.")
      if (identical(rig$flipper.mode, "rigid")) {
        weights[ix] <- 1
        next
      }
      span <- abs(geometry$vertices[ix, 2L])
      weights[ix] <- pmin(1, pmax(0, (span - abs(rig$pivot[2])) /
                                     (rig$tip - abs(rig$pivot[2]))))^rig$exponent
    } else {
      if (role == "flipper") .abort("An axial rig contains a flipper component.")
      x <- if (role == "attached") rep(component$joint.x, length(ix)) else geometry$vertices[ix, 1L]
      weights[ix] <- if (role == "terminal") 1 else
        pmin(1, pmax(0, (rig$pivot[1] - x) / (rig$pivot[1] - rig$tip)))^rig$exponent
    }
  }
  weights
}

# Descriptor constructors keep geometry and motion declarations together. The
# compiler dispatches only on rig$type and component roles, never species names.
.speciesFinSpec <- function(id, outline, normal = c(0, 0, 1),
                                        thickness = .015, role = "rigid",
                                        joint.x = NA_real_, centre = NULL, part = "fin") {
  list(id = id, kind = "fin", outline = outline, normal = normal,
       thickness = thickness, centre = centre, role = role, joint.x = joint.x, part = part,
       pivot = c(if (is.finite(joint.x)) joint.x else mean(outline[c(1,nrow(outline)),1]),
         colMeans(outline[c(1,nrow(outline)),2:3,drop=FALSE])))
}

.speciesPair <- function(fin, right.id, left.id) {
  right <- fin; right$id <- right.id
  left <- fin; left$id <- left.id
  left$outline[, 2] <- -left$outline[, 2]
  left$normal[2] <- -left$normal[2]
  if (!is.null(left$centre)) left$centre[2] <- -left$centre[2]
  if (!is.null(left$pivot)) left$pivot[2] <- -left$pivot[2]
  list(right, left)
}

.speciesBezier <- function(segments, steps = 4L) {
  pieces <- lapply(seq_along(segments), function(i) {
    p <- segments[[i]]; t <- seq(0, 1, length.out = steps + 1L)
    if (i > 1L) t <- t[-1L]
    cbind((1 - t)^3, 3*(1 - t)^2*t, 3*(1 - t)*t^2, t^3) %*% p
  })
  unname(do.call(rbind, pieces))
}

.speciesFinHub <- function(outline, normal, preferred) {
  # Orthonormal plane coordinates; the authored perimeter may also carry camber.
  seed <- diag(3)[, which.min(abs(normal))]
  u <- seed - sum(seed*normal)*normal; u <- u/sqrt(sum(u^2))
  v <- c(normal[2]*u[3] - normal[3]*u[2], normal[3]*u[1] - normal[1]*u[3],
         normal[1]*u[2] - normal[2]*u[1])
  xy <- outline %*% cbind(u, v)
  cross <- function(a, b) a[1]*b[2] - a[2]*b[1]
  inside <- function(point) all(vapply(seq_len(nrow(xy)), function(i) {
    j <- i %% nrow(xy) + 1L
    cross(xy[j, ] - xy[i, ], point - xy[i, ]) > 1e-12
  }, logical(1)))
  point <- as.numeric(preferred %*% cbind(u, v))
  if (inside(point)) return(preferred)
  lo <- apply(xy, 2, min); hi <- apply(xy, 2, max)
  kernel <- rbind(lo, c(hi[1], lo[2]), hi, c(lo[1], hi[2]))
  for (i in seq_len(nrow(xy))) {
    if (!nrow(kernel)) break
    a <- xy[i, ]; edge <- xy[i %% nrow(xy) + 1L, ] - a
    clipped <- list()
    for (k in seq_len(nrow(kernel))) {
      p <- kernel[k, ]; q <- kernel[k %% nrow(kernel) + 1L, ]
      dp <- cross(edge, p - a); dq <- cross(edge, q - a)
      if (dp >= 0) clipped[[length(clipped) + 1L]] <- p
      if ((dp >= 0) != (dq >= 0))
        clipped[[length(clipped) + 1L]] <- p + (q - p)*dp/(dp - dq)
    }
    kernel <- if (length(clipped)) do.call(rbind, clipped) else matrix(numeric(), 0L, 2L)
  }
  if (nrow(kernel) < 3L || !inside(colMeans(kernel))) return(NULL)
  point <- colMeans(kernel)
  as.numeric(point[1]*u + point[2]*v + mean(outline %*% normal)*normal)
}

.speciesExtrudedFin <- function(outline, normal, thickness) {
  # Deep sickles need concave triangulation, not a fan across the missing area.
  # Distinct tapered upper/lower perimeters and a thin rim give a closed volume.
  axis <- which.max(abs(normal))
  xy <- outline[, -axis, drop = FALSE]
  cross <- function(a, b) a[1]*b[2] - a[2]*b[1]
  area <- sum(xy[, 1]*xy[c(2:nrow(xy), 1), 2] - xy[, 2]*xy[c(2:nrow(xy), 1), 1])
  sense <- sign(area)
  ix <- seq_len(nrow(xy)); triangles <- list()
  while (length(ix) > 3L) {
    found <- FALSE
    for (i in seq_along(ix)) {
      a <- ix[(i - 2L) %% length(ix) + 1L]; b <- ix[i]; c <- ix[i %% length(ix) + 1L]
      if (sense*cross(xy[b, ] - xy[a, ], xy[c, ] - xy[b, ]) <= 1e-12) next
      other <- setdiff(ix, c(a, b, c))
      inside <- vapply(other, function(k) {
        p <- xy[k, ]
        all(sense*c(cross(xy[b, ] - xy[a, ], p - xy[a, ]),
                    cross(xy[c, ] - xy[b, ], p - xy[b, ]),
                    cross(xy[a, ] - xy[c, ], p - xy[c, ])) >= -1e-12)
      }, logical(1))
      if (any(inside)) next
      triangles[[length(triangles) + 1L]] <- c(a, b, c)
      ix <- ix[-i]; found <- TRUE; break
    }
    if (!found) .abort("A fin contour cannot be triangulated; check for self-intersections.")
  }
  if (sense*cross(xy[ix[2], ] - xy[ix[1], ], xy[ix[3], ] - xy[ix[1], ]) <= 1e-12)
    .abort("A fin contour has a crossed or degenerate remainder.")
  triangles[[length(triangles) + 1L]] <- ix
  n <- nrow(outline)
  distance <- sqrt(rowSums(sweep(xy, 2L, (xy[1, ] + xy[n, ])/2, "-")^2))
  taper <- .15 + .85*(1 - distance/max(distance))^.65
  offset <- outer(taper*thickness, normal)
  vertices <- rbind(outline + offset, outline - offset)
  faces <- c(triangles, lapply(triangles, function(f) rev(f + n)))
  for (i in seq_len(n)) {
    j <- i %% n + 1L
    faces <- c(faces, list(c(i, i + n, j + n), c(i, j + n, j)))
  }
  list(vertices = unname(vertices), faces = faces)
}
