# Construction-space geometry supports anatomical assertions without exposing
# authoring transforms or development history in public model objects.
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
