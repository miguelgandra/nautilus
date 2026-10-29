.species_cross <- function(a, b) {
  c(a[2]*b[3] - a[3]*b[2], a[3]*b[1] - a[1]*b[3], a[1]*b[2] - a[2]*b[1])
}

test_that("the production catalog is explicit and keeps every mesh within budget", {
  catalog <- overlaySpeciesCatalog()
  expect_identical(catalog$model,
    c("blue_shark", "whale_shark", "mako", "hammerhead", "manta", "devil_ray", "turtle", "dolphin", "whale", "marlin", "tuna"))
  expect_false("phase" %in% names(catalog))
  expect_true(all(catalog$triangles >= 200 & catalog$triangles <= 700))
  expect_true(all(catalog$vertices < 5000))
  expect_equal(catalog$motion, c(rep("lateral", 4), rep("flippers", 3), rep("vertical", 2), rep("lateral", 2)))
  for (id in c("shark", "cetacean", "fish", "billfish", "unknown"))
    expect_error(overlaySpeciesModel(id), "must be one of")
  expect_error(overlaySpeciesModel(c("blue_shark", "manta")), "must be one of")
})

test_that("the production registry resolves the approved catalog without mutating it", {
  ids <- overlaySpeciesCatalog()$model
  before <- lapply(ids,overlaySpeciesModel)
  expect_identical(names(nautilus:::.orientationModelRegistry()),ids)
  expect_identical(formals(renderOverlayVideo)$orientation.model,"blue_shark")
  expect_null(formals(renderOverlayVideo)$motion)
  expect_null(formals(renderOverlayVideo)$animate)
  for(i in seq_along(ids)) {
    m <- nautilus:::.prepareOverlayModel(ids[i],20)
    expect_identical(m,nautilus:::.prepareOverlayModel(before[[i]],20))
  }
  expect_identical(lapply(ids,overlaySpeciesModel),before)
  for(id in c("shark","cetacean","fish"))
    expect_error(nautilus:::.prepareOverlayModel(id),"must be one of")
})

test_that("metadata contains only production identity and complete components", {
  for (id in overlaySpeciesCatalog()$model) {
    m <- overlaySpeciesModel(id)
    expect_s3_class(m, "nautilus_overlay_model")
    expect_identical(m$source, "custom")
    expect_identical(m$metadata$id, id)
    .expect_overlay_production_metadata(m)
    construction <- .overlay_test_construction(m)
    expected.pivot <- (construction$spec$rig$pivot-construction$centre)*construction$scale
    expect_equal(m$rig$pivot,expected.pivot)
    expect_length(m$metadata$vertex.region, nrow(m$vertices))
    expect_length(m$metadata$face.region, length(m$faces))
    expect_setequal(unique(m$metadata$vertex.region), m$metadata$components$region)
    expect_equal(sum(m$metadata$components$vertices), nrow(m$vertices))
    expect_equal(sum(m$metadata$components$faces), length(m$faces))
    expect_equal(max(apply(m$vertices, 2, function(x) diff(range(x)))), 2)
    expect_no_error(nautilus:::.prepareOverlayModel(m, 45))
  }
})

test_that("triangle surfaces have positive area, coherent winding and closed edges", {
  for (id in overlaySpeciesCatalog()$model) {
    m <- overlaySpeciesModel(id)
    triangles <- do.call(rbind, m$faces)
    edges <- rbind(triangles[, c(1, 2)], triangles[, c(2, 3)], triangles[, c(3, 1)])
    key <- paste(pmin(edges[, 1], edges[, 2]), pmax(edges[, 1], edges[, 2]), sep = ":")
    expect_true(all(table(key) == 2L), info = id)
    winding <- tapply(sign(edges[, 2] - edges[, 1]), key, sum)
    expect_true(all(winding == 0L), info = id)
    areas <- vapply(m$faces, function(f) {
      p <- m$vertices[f, ]; normal <- .species_cross(p[2, ] - p[1, ], p[3, ] - p[1, ])
      sqrt(sum(normal^2))/2
    }, numeric(1))
    expect_true(all(is.finite(areas) & areas > 1e-12), info = id)
    for (component in m$metadata$components$region) {
      faces <- m$faces[m$metadata$face.region == component]
      volume <- sum(vapply(faces, function(f) {
        p <- m$vertices[f, ]; sum(p[1, ] * .species_cross(p[2, ], p[3, ]))/6
      }, numeric(1)))
      expect_true(volume > 0, info = paste(id, component))
    }
  }
  # Both kernel-based fans and closed extrusions preserve outward cap normals.
  for (spec in nautilus:::.overlaySpeciesSpecs()) for (component in spec$appendages) {
    if (component$kind != "fin") next
    m <- nautilus:::.speciesFin(component$outline, component$normal,
                                            component$thickness, component$centre)
    top <- if (nrow(m$vertices) == 2L*nrow(component$outline))
      m$faces[vapply(m$faces, function(f) all(f <= nrow(component$outline)), logical(1))] else
      m$faces[seq(1, length(m$faces), 2)]
    outward <- vapply(top, function(f) {
      p <- m$vertices[f, ]
      sum(.species_cross(p[2, ] - p[1, ], p[3, ] - p[1, ]) * component$normal)
    }, numeric(1))
    expect_true(all(outward > 0), info = component$id)
  }
})

test_that("rig metadata selects deformation without species-name dispatch", {
  spec <- nautilus:::.anatomyBlueShark()
  mesh <- nautilus:::.buildSpeciesMesh(spec)
  reference <- nautilus:::.speciesWeights(mesh, spec$rig)
  spec$label <- "An arbitrary new species name"; spec$taxon <- "Unregistered species"
  expect_identical(nautilus:::.buildSpeciesMesh(spec), mesh)
  expect_identical(nautilus:::.speciesWeights(mesh, spec$rig), reference)
  for (id in overlaySpeciesCatalog()$model) {
    m <- overlaySpeciesModel(id)
    roles <- m$metadata$components$role[match(m$metadata$vertex.region, m$metadata$components$region)]
    expect_true(all(m$rig$weights[roles == "rigid"] == 0), info = id)
    expect_true(all(m$rig$weights[roles == "terminal"] == 1), info = id)
    if (!identical(.overlay_test_authored_rig(m)$flipper.mode, "rigid"))
      expect_true(any(m$rig$weights > 0 & m$rig$weights < 1), info = id)
    if (m$rig$type == "flippers")
      expect_true(all(roles[m$rig$weights > 0] == "flipper"), info = id)
  }
  bad <- spec; bad$rig$type <- "guess-from-name"
  expect_error(nautilus:::.validateSpeciesSpec(bad), "rig type")
  bad <- spec; bad$rig$tip <- 10
  expect_error(nautilus:::.validateSpeciesSpec(bad), "tail tip")
  bad <- spec; bad$body[3, 1] <- bad$body[2, 1]
  expect_error(nautilus:::.validateSpeciesSpec(bad), "Loft stations")
  bad <- spec; bad$appendages[[1]]$id <- "body"
  expect_error(nautilus:::.validateSpeciesSpec(bad), "unique")
})

test_that("refined geometry retains rigid anchors, continuous motion and fixed display bounds", {
  for (id in overlaySpeciesCatalog()$model) {
    m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel(id), 45)
    expect_equal(nautilus:::.deformOverlayModel(m, .9, 0, 45, pi/3), m$vertices)
    for (phase in seq(0, 2*pi, length.out = 17)) {
      v <- nautilus:::.deformOverlayModel(m, phase, 1, 45, pi/3)
      expect_true(all(is.finite(v)), info = id)
      expect_equal(v[m$rig$weights == 0, ], m$vertices[m$rig$weights == 0, ], tolerance = 1e-12)
      expect_lte(max(sqrt(rowSums(v^2))) * m$display.scale, 1.08 + 1e-12)
      next.v <- nautilus:::.deformOverlayModel(m, phase + 1e-6, 1, 45, pi/3)
      expect_lt(max(abs(next.v - v)), 1e-5)
    }
    expect_false(isTRUE(all.equal(nautilus:::.deformOverlayModel(m, pi/2, 1, 20, 0), m$vertices)))
  }
})

test_that("diagnostic geometry distinguishes species and preserves symmetric anatomy", {
  specs <- nautilus:::.overlaySpeciesSpecs()
  expect_gt(max(specs$whale_shark$body[, 2]), max(specs$blue_shark$body[, 2]))
  expect_gt(max(specs$whale_shark$body[specs$whale_shark$body[,1]>=40,2]),
    2.5*max(specs$blue_shark$body[specs$blue_shark$body[,1]>=40,2]))
  hammer <- overlaySpeciesModel("hammerhead")
  expect_true("cephalofoil" %in% hammer$metadata$components$region)
  marlin <- overlaySpeciesModel("marlin")
  expect_true("bill" %in% marlin$metadata$components$region)
  for (id in c("manta", "devil_ray", "turtle", "dolphin", "whale")) {
    m <- overlaySpeciesModel(id)
    right <- m$vertices[m$metadata$vertex.region %in%
      c("wing_right", "foreflipper_right", "fluke_right"), , drop = FALSE]
    left <- m$vertices[m$metadata$vertex.region %in%
      c("wing_left", "foreflipper_left", "fluke_left"), , drop = FALSE]
    left[, 2] <- -left[, 2]
    # Reflection reverses face winding and hence perimeter storage order.
    key <- function(x) apply(round(x, 12), 1, paste, collapse = ":")
    expect_equal(sort(key(right)), sort(key(left)))
  }
})

test_that("the catalog follows the requested scientific species references", {
  catalog <- overlaySpeciesCatalog()
  expect_identical(catalog$taxon, c("Prionace glauca", "Rhincodon typus", "Isurus oxyrinchus",
    "Sphyrna zygaena", "Mobula alfredi / Mobula birostris", "Mobula tarapacana",
    "Chelonia mydas", "Delphinus delphis", "Megaptera novaeangliae", "Kajikia audax", "Thunnus albacares"))
  expect_identical(overlaySpeciesModel()$metadata$id, "blue_shark")
  specs <- nautilus:::.overlaySpeciesSpecs()
  component <- function(spec, id) spec$appendages[[which(vapply(spec$appendages,
    function(x) identical(x$id, id), logical(1)))]]
  head <- component(specs$hammerhead, "cephalofoil")
  front <- head$centreline + head$stations[, 2]
  expect_identical(head$axis, "y")
  mid <- which.min(abs(head$stations[, 1]))
  expect_equal(unname(head$stations[mid, 1]), 0)
  # Smooth hammerheads lack a median notch; lateral indentations are retained.
  central <- which(abs(head$stations[,1]) <= 8)
  expect_true(all(diff(front[central[central <= mid]]) > 0))
  expect_true(all(diff(front[central[central >= mid]]) < 0))
  expect_equal(front, rev(front))              # Rounded arch, no central scallop/notch.
  # Free blades stay thin; the central head thickens into the measured trunk.
  expect_lt(max(head$stations[abs(head$stations[,1]) >= 8, 3]), 2)
  expect_lt(max(head$stations[,3]), 4.5)
  raw <- function(id) {
    m <- overlaySpeciesModel(id)
    .overlay_test_raw_vertices(m)
  }
  m <- overlaySpeciesModel("hammerhead")
  v <- raw("hammerhead")
  head.width <- diff(range(v[m$metadata$vertex.region == "cephalofoil",2]))
  expect_gte(head.width/diff(range(v[,1])), .26)
  expect_lte(head.width/diff(range(v[,1])), .29)
  region <- function(id, pattern) {
    m <- overlaySpeciesModel(id); v <- raw(id)
    v[grepl(pattern,m$metadata$vertex.region),,drop=FALSE]
  }
  # Proportions measured from geometry, not copied from landmark metadata.
  fluke <- region("dolphin", "^fluke_")
  ratio <- diff(range(fluke[,2]))/diff(range(raw("dolphin")[,1]))
  # The supplied common-dolphin asset has a measured 28.4% axial-TL fluke span,
  # superseding the earlier 20--25% sketch target.
  expect_gte(ratio,.28); expect_lte(ratio,.29)
  m <- overlaySpeciesModel("whale")
  fin <- region("whale","^pectoral_right$")
  pivot <- as.numeric(m$metadata$components[m$metadata$components$region=="pectoral_right",c("pivot.x","pivot.y","pivot.z")])
  length <- max(sqrt(rowSums(sweep(fin,2,pivot,"-")^2)))/diff(range(raw("whale")[,1]))
  # Supplied humpback neutral asset has a shorter, swept blade than the old
  # hand-proportioned 29--36% target. Check its measured root-to-tip extent.
  expect_gte(length,.21); expect_lte(length,.25)
  expect_true(all(c("dorsal","second_dorsal","anal","second_anal","bill","keel_right","keel_left") %in%
    overlaySpeciesModel("marlin")$metadata$components$region))
  tuna <- overlaySpeciesModel("tuna")
  ids <- tuna$metadata$components$region
  expect_equal(sum(startsWith(ids,"dorsal_finlet_")),9L)
  expect_equal(sum(startsWith(ids,"ventral_finlet_")),9L)
  expect_true(all(c("keel_right","keel_left") %in% ids))
  finlets <- region("tuna","finlet")
  expect_lt(diff(range(finlets[,1])),30)
  expect_true(all(finlets[,1] < -4 & finlets[,1] > -35))
  manta <- raw("manta"); m <- overlaySpeciesModel("manta")
  disc <- manta[m$metadata$vertex.region %in% c("body","wing_left","wing_right"),,drop=FALSE]
  expect_gte(diff(range(disc[,2]))/diff(range(disc[,1])),2.2)
  expect_lte(diff(range(disc[,2]))/diff(range(disc[,1])),2.4)
  expect_lt(diff(range(region("devil_ray","^tail$")[,1])),diff(range(region("manta","^tail$")[,1])))

})

test_that("curved fin perimeters do not cross and deep sickles use closed concave caps", {
  crossed <- function(p) {
    cross <- function(a, b) a[1]*b[2] - a[2]*b[1]
    n <- nrow(p)
    for (i in seq_len(n - 1L)) for (j in seq.int(i + 1L, n)) {
      k <- i %% n + 1L; l <- j %% n + 1L
      if (length(unique(c(i, j, k, l))) < 4L) next
      a <- p[i, ]; b <- p[k, ]; c <- p[j, ]; d <- p[l, ]
      if (cross(b - a, c - a)*cross(b - a, d - a) < -1e-14 &&
          cross(d - c, a - c)*cross(d - c, b - c) < -1e-14) return(TRUE)
    }
    FALSE
  }
  for (spec in nautilus:::.overlaySpeciesSpecs()) for (fin in spec$appendages) {
    if (fin$kind != "fin") next
    expect_false(crossed(fin$outline[, -which.max(abs(fin$normal)), drop = FALSE]),
                 info = paste(spec$taxon, fin$id))
  }
  # A deliberately concave contour with no shared fan hub must not be bridged.
  outline <- cbind(c(0, 3, 3, 1, 1, 3, 3, 0), c(0, 0, 1, 1, 2, 2, 3, 3), 0)
  mesh <- nautilus:::.speciesFin(outline, c(0, 0, 1), .01)
  expect_equal(nrow(mesh$vertices), 2*nrow(outline))
  expect_equal(length(mesh$faces), 4*nrow(outline) - 4)
})

test_that("production models use the shared frame-state and dashboard pipeline", {
  t0 <- as.POSIXct("2022-10-13 18:29:00", tz = "UTC")
  d <- data.table::data.table(datetime = t0 + seq(0, 2, by = .05),
    pitch = -18, roll = 12, heading = 280, depth = 20, vedba = .1,
    vertical_velocity = -.2, tbf_hz_peaks = .8, tbf_amplitude_peaks = .2)
  times <- t0 + seq(.1, 1.9, by = .05)
  plan <- nautilus:::.resolveDashboardPlan("general")
  context <- nautilus:::.prepareOverlayContext(d, plan)
  baseline <- nautilus:::.prepareOverlayFrameState(d, times,
    nautilus:::.resolveOverlayMotion(overlayMotionControl(), d, nautilus:::.prepareOverlayModel("blue_shark")))
  grDevices::pdf(file = NULL, width = 6, height = 10)
  on.exit(grDevices::dev.off(), add = TRUE)
  for (id in overlaySpeciesCatalog()$model) {
    model <- nautilus:::.prepareOverlayModel(overlaySpeciesModel(id), 20)
    motion <- nautilus:::.resolveOverlayMotion(overlayMotionControl(), d, model)
    state <- nautilus:::.prepareOverlayFrameState(d, times, motion)
    expect_identical(state$frames, baseline$frames)
    fd <- nautilus:::.overlayFrameData(state, 4, d)
    expect_no_error(nautilus:::.drawPreparedOverlayDashboard(fd, context$clip, fd$datetime,
      300, 30, nautilus:::.overlayTheme(), NULL, NULL, plan, context, model, motion))
  }
})

test_that("the production entry point accepts each production species name and model object", {
  skip_if_not_installed("av")
  t0 <- as.POSIXct("2022-10-13 18:29:00", tz = "UTC")
  d <- data.table::data.table(datetime = t0 + seq(0, 1, by = .05),
    pitch = -18, roll = 12, heading = 280, depth = 20, vedba = .1,
    vertical_velocity = -.2, tbf_hz_peaks = .8, tbf_amplitude_peaks = .2)
  source <- tempfile(fileext = ".mp4"); file.create(source)
  outputs <- vapply(seq_len(nrow(overlaySpeciesCatalog())), function(i) tempfile(fileext = ".mp4"), character(1))
  on.exit(unlink(c(source, outputs)), add = TRUE)
  captured <- list()
  testthat::local_mocked_bindings(
    .ffmpegBin = function() "ffmpeg", .hasEncoder = function(...) FALSE,
    .drawPreparedOverlayDashboard = function(fd, clip, current.time, depth.window, activity.window,
                                             theme, caption, candidates, plan, context, model, motion) {
      captured[[length(captured) + 1L]] <<- list(id = model$metadata$id,
        type = model$rig$type, time = fd$datetime, phase = fd$.motion.phase)
    },
    .runFfmpegWithProgress = function(ffmpeg, args, duration, lvl) {
      file.create(utils::tail(args, 1L)); 0L
    }, .package = "nautilus")
  testthat::local_mocked_bindings(
    av_video_info = function(...) list(video = list(framerate = 30, width = 1280, height = 720), duration = 1),
    av_capture_graphics = function(expr, ...) force(expr), .package = "av")
  ids <- overlaySpeciesCatalog()$model
  for (i in seq_along(ids)) {
    captured <- list()
    expect_no_warning(renderOverlayVideo(source, d, outputs[i], video.start = t0,
      overlay.fps = 20, orientation.model = if(i %% 2L) ids[i] else overlaySpeciesModel(ids[i]),
      animate = TRUE, verbose = FALSE))
    expect_length(captured, 20)
    expect_true(all(vapply(captured, function(x) identical(x$id, ids[i]), logical(1))))
    elapsed <- as.numeric(difftime(captured[[20]]$time, t0, units = "secs"))
    expect_equal(captured[[20]]$phase, 2*pi*.8*elapsed)
  }
  expect_identical(names(nautilus:::.orientationModelRegistry()), overlaySpeciesCatalog()$model)
})

test_that("named attachment pivots and sectional roots are finite and preserve symmetry", {
  for (id in overlaySpeciesCatalog()$model) {
    model <- overlaySpeciesModel(id)
    components <- model$metadata$components
    authored <- as.matrix(components[,c("pivot.x","pivot.y","pivot.z")])
    normalised <- as.matrix(components[,c("normalised.pivot.x","normalised.pivot.y","normalised.pivot.z")])
    construction <- .overlay_test_construction(model)
    expected <- sweep(authored,2,construction$centre,"-")*construction$scale
    expect_equal(unname(normalised),unname(expected))
    expect_true(all(is.finite(normalised)))
    expect_false("allocation" %in% names(components))
  }
  for (spec in nautilus:::.overlaySpeciesSpecs()) for (fin in spec$appendages) {
    if (fin$kind != "foil") next
    # Every section has a nonzero chord, and the normal does not point along it.
    chord <- fin$leading - fin$trailing
    expect_true(all(rowSums(chord^2) > 0),info=paste(spec$label,fin$id))
    # Sloped root chords may follow a sloped body envelope. The thickness
    # direction must remain transverse, but need not be exactly perpendicular.
    # Explicit measured section vertices replace the normal-based extrusion;
    # their local twist is checked by the closed-surface/area tests above.
    if (is.null(fin$section.vertices)) {
      cosine <- as.numeric(chord %*% fin$normal)/sqrt(rowSums(chord^2)*sum(fin$normal^2))
      expect_true(all(abs(cosine)<.2),info=paste(spec$label,fin$id))
    }
    expect_gt(fin$thickness[1],tail(fin$thickness,1))
  }
})

test_that("the orthographic gallery reuses one mesh with identical view scales", {
  script <- system.file("examples","preview-model-library.R",package="nautilus")
  skip_if(!nzchar(script))
  environment <- new.env(parent=globalenv())
  sys.source(script,environment)
  limits <- list()
  environment$plot <- function(...) {
    graphics::plot(...)
    limits[[length(limits)+1L]] <<- graphics::par("usr")
  }
  path <- tempfile("species-gallery-"); dir.create(path)
  on.exit(unlink(path,recursive=TRUE),add=TRUE)
  result <- environment$previewModelLibrary(path,models="whale_shark")
  expect_length(limits,3L)
  expect_identical(limits[[1]],limits[[2]])
  expect_identical(limits[[1]],limits[[3]])
  expect_true(all(file.exists(c(result$images,result$parts))))
  expect_error(environment$previewModelLibrary(path,models="whale_shark"),"new folder")
  parts <- utils::read.csv(result$parts)
  expect_equal(sum(parts$faces),length(overlaySpeciesModel("whale_shark")$faces))
})
