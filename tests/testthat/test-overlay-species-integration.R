test_that("production names and constructors resolve identical approved models", {
  ids <- overlaySpeciesCatalog()$model
  for(id in ids) {
    m <- overlaySpeciesModel(id)
    expect_identical(nautilus:::.prepareOverlayModel(id),nautilus:::.prepareOverlayModel(m))
  }
  expect_identical(nautilus:::.prepareOverlayModel(),nautilus:::.prepareOverlayModel("blue_shark"))
  expect_identical(formals(renderOverlayVideo)$orientation.model,"blue_shark")
  expect_identical(names(nautilus:::.orientationModelRegistry()),ids)
  expect_false(any(c("overlayModelExperimental","overlayModelCatalogExperimental") %in%
    getNamespaceExports("nautilus")))
})

test_that("batched triangle projection preserves painter order and face lighting", {
  for (id in overlaySpeciesCatalog()$model) {
    m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel(id), 20)
    slow <- m; slow$triangles <- NULL
    for (p in c(-35, 0, 25)) for (r in c(-30, 0, 40)) for (phase in c(0, .7, 2.4)) {
      v <- nautilus:::.deformOverlayModel(m, phase, .8, 20, pi/3)
      rot <- nautilus:::.overlayAttitudeRotation(p, r)
      a <- nautilus:::.projectPreparedOverlayModel(m, v, rot)
      b <- nautilus:::.projectPreparedOverlayModel(slow, v, rot)
      expect_identical(a$points, b$points, info = id)
      expect_identical(a$order, b$order, info = id)
      expect_equal(a$intensity, b$intensity, tolerance = 1e-14, info = id)
    }
  }
  # The cache is derived from the validated faces, never trusted from an edited list.
  m <- overlaySpeciesModel("whale_shark")
  m$triangles <- matrix(1L, 1L, 3L)
  prepared <- nautilus:::.prepareOverlayModel(m)
  expect_identical(prepared$triangles, do.call(rbind, m$faces))
  for (id in names(nautilus:::.orientationModelRegistry()))
    expect_identical(nautilus:::.prepareOverlayModel(id)$triangles,
      do.call(rbind,overlaySpeciesModel(id)$faces))
})

test_that("batched polygons produce the same raster as individual painter calls", {
  skip_if_not(capabilities("cairo"))
  paths <- c(tempfile(fileext = ".png"), tempfile(fileext = ".png"))
  on.exit(unlink(paths), add = TRUE)
  fd <- list(heading = 270, pitch = -18, roll = 12, .motion.phase = .7,
    .motion.strength = .8, .motion.frequency = .9, .motion.status = "estimated")
  for (id in overlaySpeciesCatalog()$model) {
    m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel(id), 20)
    for (i in 1:2) {
      if (i == 2L) m$triangles <- NULL
      grDevices::png(paths[i], width = 420, height = 420, type = "cairo", bg = "#101820")
      tryCatch({
        graphics::par(mar = c(1, 1, 1, 1))
        nautilus:::.drawPreparedAttitudeModel(fd, nautilus:::.overlayTheme(), m,
          overlayMotionControl(), label = id)
      }, finally = grDevices::dev.off())
    }
    expect_identical(unname(tools::md5sum(paths[1])), unname(tools::md5sum(paths[2])), info = id)
  }
})
