# Reference-proportioned species anatomy. Authoring coordinates are percent
# of nominal TL: x = 50 - distance from snout, y right, z down. Ray dimensions
# additionally declare disc length/width; no view can change mesh proportions.

.speciesFoil <- function(component) {
  # Cross sections concentrate geometry at a thick root and a curved, tapered
  # leading edge. Unlike a fan, no long triangle runs from tip through the body.
  n <- nrow(component$leading); k <- component$sections
  a <- 2*pi*(seq_len(k) - 1L)/k
  normal <- component$normal/sqrt(sum(component$normal^2))
  vertices <- if (!is.null(component$section.vertices)) component$section.vertices else do.call(rbind, lapply(seq_len(n), function(i) {
    middle <- (component$leading[i, ] + component$trailing[i, ])/2
    chord <- (component$leading[i, ] - component$trailing[i, ])/2
    matrix(middle, k, 3, byrow = TRUE) + outer(cos(a), chord) +
      outer(sin(a)*component$thickness[i], normal)
  }))
  vertices <- rbind(vertices, colMeans(rbind(component$leading[1, ], component$trailing[1, ])),
    colMeans(rbind(component$leading[n, ], component$trailing[n, ])))
  faces <- list()
  for (i in seq_len(n - 1L)) for (j in seq_len(k)) {
    next.j <- j %% k + 1L; p <- (i - 1L)*k; q <- i*k
    faces <- c(faces, list(c(p + j, p + next.j, q + next.j), c(p + j, q + next.j, q + j)))
  }
  for (j in seq_len(k)) {
    next.j <- j %% k + 1L
    faces <- c(faces, list(c(n*k + 1L, next.j, j), c(n*k + 2L, (n - 1L)*k + j, (n - 1L)*k + next.j)))
  }
  cross <- function(a, b) c(a[2]*b[3]-a[3]*b[2], a[3]*b[1]-a[1]*b[3], a[1]*b[2]-a[2]*b[1])
  volume <- sum(vapply(faces, function(f) sum(vertices[f[1], ] *
    cross(vertices[f[2], ], vertices[f[3], ])), numeric(1)))
  if (volume < 0) faces <- lapply(faces, rev)
  list(vertices = vertices, faces = faces)
}

.anatomyBody <- function(position, width, dorsal, ventral = dorsal, z = 0) {
  out <- .speciesBody(50 - position, width, dorsal, ventral, z)
  out[order(out[, 1]), , drop = FALSE]
}

.anatomyFoil <- function(id, stations, plane = "xy", role = "rigid", sections = 6L) {
  # Each row: span coordinate, leading x, trailing x, normal-axis centre, half-thickness.
  leading <- trailing <- matrix(0, nrow(stations), 3L)
  leading[, 1] <- stations[, 2]; trailing[, 1] <- stations[, 3]
  span <- if (plane == "xy") 2L else 3L
  normal.axis <- if (plane == "xy") 3L else 2L
  leading[, span] <- trailing[, span] <- stations[, 1]
  leading[, normal.axis] <- trailing[, normal.axis] <- stations[, 4]
  pivot <- (leading[1, ] + trailing[1, ])/2
  list(id = id, kind = "foil", part = "fin", role = role, joint.x = pivot[1],
    leading = leading, trailing = trailing, normal = diag(3)[, normal.axis],
    thickness = stations[, 5], sections = sections, pivot = pivot,
    allocation = "curved planform / thick root / tapered tip")
}

.anatomyPair <- function(fin, id = fin$id) {
  right <- fin; right$id <- paste0(id, "_right")
  left <- right; left$id <- paste0(id, "_left")
  left$leading[, 2] <- -left$leading[, 2]; left$trailing[, 2] <- -left$trailing[, 2]
  left$normal[2] <- -left$normal[2]; left$pivot[2] <- -left$pivot[2]
  if (!is.null(left$section.vertices)) left$section.vertices[,2] <- -left$section.vertices[,2]
  list(right, left)
}

.anatomyDorsal <- function(id, position, height, base, tip.sweep, surface,
                            role = "attached", thickness = 1.1, down = FALSE, sections = 4L) {
  root.x <- 50 - position; sign <- if (down) 1 else -1
  s <- rbind(c(surface*.45, root.x, root.x - base, 0, thickness),
    c(surface + height*.20, root.x - base*.13, root.x - base*.83, 0, thickness*.70),
    c(surface + height*.65, root.x - tip.sweep*.65, root.x - tip.sweep - base*.14, 0, thickness*.30),
    c(surface + height*.94, root.x - tip.sweep + .45, root.x - tip.sweep - .55, 0, thickness*.12),
    c(surface + height, root.x - tip.sweep, root.x - tip.sweep - .12, 0, thickness*.035))
  s[, 1] <- sign*s[, 1]
  if (height < 4.5) s <- s[c(1, 3, 5), , drop = FALSE]
  .anatomyFoil(id, s, "xz", role, sections)
}

.anatomyTail <- function(upper, lower, upper.sweep, lower.sweep, root = -33,
                         chord = 6, thickness = 1.1, sections = 4L, horizontal = FALSE) {
  lobe <- function(height, sweep, id, side) {
    s <- rbind(c(0, root + 1, root - chord, 0, thickness),
      c(height*.18, root - 1, root - chord - 1.2, 0, thickness*.8),
      c(height*.44, root - sweep*.25, root - chord - sweep*.25, 0, thickness*.45),
      c(height*.77, root - sweep*.65, root - sweep*.65 - chord*.42, 0, thickness*.20),
      c(height, root - sweep, root - sweep - .16, 0, thickness*.035))
    s[, 1] <- side*s[, 1]
    .anatomyFoil(id, s, if (horizontal) "xy" else "xz", "terminal", sections)
  }
  list(lobe(upper, upper.sweep, if (horizontal) "fluke_right" else "caudal_upper", if (horizontal) 1 else -1),
       lobe(lower, lower.sweep, if (horizontal) "fluke_left" else "caudal_lower", if (horizontal) -1 else 1))
}

.anatomySpec <- function(label, taxon, phase, body, appendages, features, references,
                         type = "lateral", sections = 12L, eyes = NULL, details = list(),
                         pivot = c(0, 0, 0), tip = -33, landmarks = list()) {
  spec <- .speciesDescriptor(label, taxon, phase, body,
    list(type = type, pivot = pivot, tip = tip, exponent = if (type == "flippers") 1.5 else 2,
      weighting = if (type == "flippers") "continuous spanwise bend" else "continuous axial bend; named attachment pivots"),
    appendages, features, references, sections,
    if (type == "flippers") "rigid" else "axial", eyes, details)
  spec$landmarks <- landmarks
  spec
}

.anatomyKeels <- function(x = -30, width = 3, z = 0) {
  keel <- .anatomyFoil("keel", rbind(c(.6, x + 4, x - 5, z, .6),
    c(width*.7, x + 1, x - 4, z, .32), c(width, x - 1, x - 2, z, .08)), sections = 4L,
    role = "attached")
  .anatomyPair(keel)
}

.anatomyWhaleShark <- function() .referenceSharkSpec(TRUE)

.anatomyHammerhead <- function() .referenceHammerheadSpec()

.anatomyManta <- function() .referenceMantaSpec()

.anatomyBlueShark <- function() .referenceSharkSpec(FALSE)

.anatomyMako <- function() .referenceMakoSpec()

.anatomyDevilRay <- function() .referenceDevilRaySpec()

.anatomyTurtle <- function() .referenceTurtleSpec()

.anatomyDolphin <- function() .referenceDolphinSpec()

.anatomyHumpback <- function() .referenceHumpbackSpec()

.anatomyMarlin <- function() .referenceMarlinSpec()

.anatomyTuna <- function() .referenceTunaSpec()
