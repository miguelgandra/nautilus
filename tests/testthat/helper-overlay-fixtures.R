# Construction-space geometry supports anatomical assertions without exposing
# authoring transforms or development history in public model objects.

# Trigonometric mesh construction varies by a few ulps between platform math
# libraries. Allow at most 1e-12 absolute roundoff in double values, while keeping
# types, attributes, dimensions, topology, labels and non-finite values exact.
.overlay_test_allow_roundoff <- function(actual, expected, tolerance = 1e-12) {
  # R compares attributes as a set; their storage order can differ by R version.
  a <- attributes(actual)
  e <- attributes(expected)
  if (!identical(typeof(actual), typeof(expected)) ||
      !identical(a[sort(names(a))], e[sort(names(e))]) || length(actual) != length(expected))
    return(actual)
  if (is.double(actual)) {
    close <- is.finite(actual) & is.finite(expected) & abs(actual - expected) <= tolerance
    actual[close] <- expected[close]
  } else if (is.list(actual)) {
    for (i in seq_along(actual))
      actual[i] <- list(.overlay_test_allow_roundoff(actual[[i]], expected[[i]], tolerance))
  }
  actual
}

# A point exactly on a mesh edge can be assigned to either adjacent triangle.
# Accept that choice only when its weights on global vertices are the same,
# which also preserves the point under every vertex deformation. Mesh faces
# themselves remain exact; this does not permit moving or reordering markings.
.overlay_test_align_surface_edges <- function(actual, expected, tolerance = 1e-12) {
  if (!is.list(actual) || !is.list(expected) || !identical(actual$faces, expected$faces))
    return(actual)
  a <- actual$appearance$surface
  e <- expected$appearance$surface
  if (is.null(a) || is.null(e) || !identical(typeof(a$face), typeof(e$face)) ||
      !identical(typeof(a$barycentric), typeof(e$barycentric)) ||
      length(a$face) != length(e$face) || !identical(dim(a$barycentric), dim(e$barycentric)))
    return(actual)
  if (any(!is.finite(a$face)) || any(a$face != as.integer(a$face)) ||
      any(a$face < 1 | a$face > length(actual$faces)))
    return(actual)
  for (i in which(a$face != e$face)) {
    av <- actual$faces[[a$face[i]]]
    ev <- expected$faces[[e$face[i]]]
    vertices <- unique(c(av, ev))
    aw <- ew <- numeric(length(vertices))
    aw[match(av, vertices)] <- a$barycentric[i, ]
    ew[match(ev, vertices)] <- e$barycentric[i, ]
    if (all(is.finite(c(aw, ew))) && all(abs(aw - ew) <= tolerance)) {
      a$face[i] <- e$face[i]
      a$barycentric[i, ] <- e$barycentric[i, ]
    }
  }
  actual$appearance$surface <- a
  actual
}

.expect_overlay_fixture <- function(actual, expected) {
  actual <- .overlay_test_align_surface_edges(actual, expected)
  testthat::expect_identical(.overlay_test_allow_roundoff(actual, expected), expected)
}

.overlay_test_construction <- function(model) {
  spec <- nautilus:::.overlaySpeciesSpecs()[[model$name]]
  mesh <- nautilus:::.buildSpeciesMesh(spec)
  centre <- (apply(mesh$vertices,2L,min)+apply(mesh$vertices,2L,max))/2
  scale <- 2/max(apply(mesh$vertices,2L,function(x)diff(range(x))))
  normalized <- sweep(mesh$vertices,2L,centre,"-")*scale
  testthat::expect_equal(normalized,model$vertices,tolerance=1e-13)
  list(spec=spec,vertices=mesh$vertices,centre=centre,scale=scale)
}

.overlay_test_raw_vertices <- function(model) {
  .overlay_test_construction(model)$vertices
}

.overlay_test_authored_rig <- function(model) {
  rig <- nautilus:::.overlaySpeciesSpecs()[[model$name]]$rig
  c(rig,list(normalised.pivot=model$rig$pivot))
}

.expect_overlay_production_metadata <- function(model) {
  testthat::expect_named(model$metadata,
    c("id","label","taxon","axes","components","vertex.region","face.region"))
  testthat::expect_false("allocation" %in% names(model$metadata$components))
  spec <- nautilus:::.overlaySpeciesSpecs()[[model$name]]
  testthat::expect_false(any(c("revision","review","phase","features","references",
    "anatomy","landmarks") %in% names(spec)))
  testthat::expect_null(spec$rig$weighting)
}
