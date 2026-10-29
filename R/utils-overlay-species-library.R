# Original species-guided meshes. Dimensionless proportions are authored from
# identification references, not measured specimens. Curves are sampled once.

.speciesBody <- function(x, width, dorsal, ventral = dorsal*.8, z = 0) {
  cbind(x, width, dorsal, ventral, rep_len(z, length(x)))
}

.speciesDescriptor <- function(label, taxon, phase, body, rig,
                                            appendages, features, references,
                                            sections = 10L, body.role = "axial",
                                            eyes = NULL, details = list()) {
  list(label = label, taxon = taxon, phase = phase, body = body,
    sections = sections, body.role = body.role, rig = rig, appendages = appendages,
    features = features, references = references, eyes = eyes, details = details)
}

.overlaySpeciesSpecs <- function() {
  list(blue_shark = .anatomyBlueShark(), whale_shark = .anatomyWhaleShark(),
    mako = .anatomyMako(), hammerhead = .anatomyHammerhead(),
    manta = .anatomyManta(), devil_ray = .anatomyDevilRay(),
    turtle = .anatomyTurtle(), dolphin = .anatomyDolphin(),
    whale = .anatomyHumpback(), marlin = .anatomyMarlin(),
    tuna = .anatomyTuna())
}
