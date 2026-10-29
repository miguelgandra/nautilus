test_that("dolphin neutral reference fit retains species, parts and proportions", {
  m <- overlaySpeciesModel("dolphin")
  expect_identical(m$metadata$taxon,"Delphinus delphis")
  expect_identical(m$rig$type,"vertical")
  expect_equal(length(m$faces),688L)
  expect_equal(nrow(m$vertices),356L)
  expect_equal(m$metadata$components$faces,c(320,84,84,56,72,72))
  expect_setequal(m$metadata$components$region,c("body","pectoral_right","pectoral_left",
    "dorsal","fluke_right","fluke_left"))
  expect_equal(length(m$appearance$surface$face),111L)
  expect_length(m$details,0)
  raw <- .overlay_test_raw_vertices(m)
  expect_gt(diff(range(raw[,1])),99.5)
  expect_lt(diff(range(raw[,1])),100.1)
  body <- raw[m$metadata$vertex.region=="body",,drop=FALSE]
  expect_gt(diff(range(body[,2])),17)
  expect_lt(diff(range(body[,2])),19)
  expect_gt(diff(range(body[,3])),17)
  expect_lt(diff(range(body[,3])),20)
  tail <- raw[startsWith(m$metadata$vertex.region,"fluke_"),,drop=FALSE]
  expect_equal(diff(range(tail[,2])),28.39,tolerance=1e-8)
  # The central trailing notch is anterior to the outer lobe tips.
  centre <- tail[abs(tail[,2])<1e-8,,drop=FALSE]
  expect_gt(min(centre[,1])-min(tail[,1]),3.5)
  for(part in c("pectoral","fluke")) {
    right <- raw[m$metadata$vertex.region==paste0(part,"_right"),,drop=FALSE]
    left <- raw[m$metadata$vertex.region==paste0(part,"_left"),,drop=FALSE];left[,2] <- -left[,2]
    expect_equal(right,left,tolerance=1e-12)
  }
  pectorals <- raw[startsWith(m$metadata$vertex.region,"pectoral_"),,drop=FALSE]
  expect_equal(diff(range(pectorals[,2])),31.67,tolerance=1e-8)
  expect_gt(max(pectorals[,3]),9)
  expect_identical(formals(renderOverlayVideo)$orientation.model,"blue_shark")
})

test_that("dolphin keeps anchored pectorals and rigid whole flukes through vertical bends", {
  m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel("dolphin"),45)
  raw <- .overlay_test_raw_vertices(m)
  fixed <- startsWith(m$metadata$vertex.region,"pectoral_") |
    (m$metadata$vertex.region=="body" & raw[,1]>=0)
  flukes <- startsWith(m$metadata$vertex.region,"fluke_")
  expect_true(all(m$rig$weights[fixed]==0))
  expect_true(all(m$rig$weights[flukes]==1))
  expect_true(any(m$rig$weights>0 & m$rig$weights<1))
  for(phase in seq(0,2*pi,length.out=17)) {
    v <- nautilus:::.deformOverlayModel(m,phase,1,45,pi/3)
    expect_true(all(is.finite(v)))
    expect_equal(v[fixed,],m$vertices[fixed,],tolerance=1e-12)
    expect_equal(as.vector(stats::dist(v[flukes,])),as.vector(stats::dist(m$vertices[flukes,])),tolerance=1e-10)
    expect_equal(v[,2],m$vertices[,2],tolerance=1e-12)
    expect_lte(max(sqrt(rowSums(v^2)))*m$display.scale,1.08+1e-10)
  }
})


test_that("dolphin appearance preserves the approved geometry and complete rig", {
  approved <- readRDS(testthat::test_path("fixtures","approved-dolphin-geometry-v19.rds"))
  m <- overlaySpeciesModel("dolphin")
  .expect_overlay_fixture(c(m[c("vertices","faces","part","rig","details")],
    list(components=m$metadata$components,authored.rig=.overlay_test_authored_rig(m))),approved)
  rgb <- m$appearance$face.rgb
  expect_identical(dim(rgb),c(688L,3L))
  expect_true(all(is.finite(rgb) & rgb>=0 & rgb<=1))
  expect_equal(rgb[seq(1,299,2),],rgb[seq(2,300,2),])
  raw <- .overlay_test_raw_vertices(m)
  f <- do.call(rbind,m$faces)
  p <- (raw[f[,1],]+raw[f[,2],]+raw[f[,3],])/3
  spec <- nautilus:::.referenceDolphinSpec()
  z <- stats::approx(spec$body[,1],spec$body[,5],p[,1],rule=2)$y
  h <- ifelse(p[,3]<z,stats::approx(spec$body[,1],spec$body[,3],p[,1],rule=2)$y,
    stats::approx(spec$body[,1],spec$body[,4],p[,1],rule=2)$y)
  angle <- (p[,3]-z)/h;body <- m$metadata$face.region=="body"
  expect_gt(mean(rgb[body & angle>.6,]),mean(rgb[body & angle< -.6,])+.25)
  flank <- body & p[,1]>5 & p[,1]<35 & abs(angle)<.5
  expect_true(any(rgb[flank,1]-rgb[flank,3]>.06))
})

test_that("optional face RGB palettes validate once and respect lighting and colour override", {
  model <- overlaySpeciesModel("dolphin");n <- length(model$faces)
  palette <- model$appearance$face.rgb
  for(value in list(matrix(0,n-1L,3),matrix(0,n,2),matrix("red",n,3),
      replace(palette,1,NA_real_),replace(palette,1,Inf),replace(palette,1,-.1),replace(palette,1,1.1))) {
    bad <- model;bad$appearance$face.rgb <- value
    expect_error(nautilus:::.prepareOverlayModel(bad,20),"face RGB colours")
  }
  m <- nautilus:::.prepareOverlayModel(model,20)
  expected <- grDevices::rgb(palette[,1]*.5,palette[,2]*.5,palette[,3]*.5)
  expect_equal(nautilus:::.overlayFaceColours(m,rep(.5,n)),expected)
  expect_equal(nautilus:::.overlayFaceColours(m,rep(1,n),"#ffffff",use.appearance=FALSE),rep("#FFFFFF",n))
})

test_that("dolphin facial markings remain on their fixed head triangles", {
  m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel("dolphin"),45)
  s <- m$appearance$surface
  expect_equal(sum(s$kind=="eye"),2L)
  expect_equal(rowSums(s$barycentric),rep(1,111L))
  expect_setequal(unique(s$group[s$kind=="line"]),1:7)
  expect_true(all(m$metadata$face.region[s$face]=="body"))
  expect_false(any(m$appearance$hidden.faces[s$face]))
  resting <- nautilus:::.overlaySurfacePoints(m,m$vertices)
  for(phase in seq(0,2*pi,length.out=9)) {
    v <- nautilus:::.deformOverlayModel(m,phase,1,45,pi/3)
    points <- nautilus:::.overlaySurfacePoints(m,v)
    expect_true(all(is.finite(points)))
    expect_equal(points,resting,tolerance=1e-12)
    f <- m$triangles[s$face,,drop=FALSE]
    for(axis in 1:3) {
      bounds <- cbind(v[f[,1],axis],v[f[,2],axis],v[f[,3],axis])
      expect_true(all(points[,axis]>=apply(bounds,1,min)-1e-12))
      expect_true(all(points[,axis]<=apply(bounds,1,max)+1e-12))
    }
  }
})

test_that("dolphin eyes and facial lines respect body occlusion", {
  m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel("dolphin"),45)
  cameras <- list(
    dorsal=list(right=c(-1,0,0),up=c(0,1,0),depth=c(0,0,1),view.rotation=diag(3)),
    ventral=list(right=c(-1,0,0),up=c(0,-1,0),depth=c(0,0,-1),view.rotation=diag(3)),
    lateral=list(right=c(-1,0,0),up=c(0,0,-1),depth=c(0,1,0),view.rotation=diag(3)),
    opposite=list(right=c(1,0,0),up=c(0,0,-1),depth=c(0,-1,0),view.rotation=diag(3)))
  visible <- lapply(cameras,function(camera) {
    p <- nautilus:::.projectPreparedOverlayModel(m,m$vertices,diag(3),camera)
    nautilus:::.overlaySurfaceProjection(m,m$vertices,diag(3),camera,p)$visible
  })
  s <- m$appearance$surface;eyes <- s$kind=="eye"
  expect_identical(visible$lateral[eyes],c(FALSE,TRUE))
  expect_identical(visible$opposite[eyes],c(TRUE,FALSE))
  expect_false(any(visible$ventral[s$group==7]))
  expect_true(all(visible$dorsal[s$group==7]))
  expect_false(any(visible$dorsal[s$group %in% 3:4]))
  for(roll in seq(0,360,by=30)) {
    v <- nautilus:::.deformOverlayModel(m,roll*pi/180,1,45,pi/3)
    r <- nautilus:::.overlayAttitudeRotation(-18,roll)
    p <- nautilus:::.projectPreparedOverlayModel(m,v,r)
    surface <- nautilus:::.overlaySurfaceProjection(m,v,r,nautilus:::.overlayAttitudeCamera(),p)
    expect_true(all(is.finite(surface$points)))
    expect_false(anyNA(surface$visible))
  }
})


test_that("approved dolphin v20 preserves reviewed colour, markings and articulation", {
  approved <- readRDS(testthat::test_path("fixtures","approved-dolphin-v20.rds"))
  m <- overlaySpeciesModel("dolphin")
  .expect_overlay_fixture(c(m[c("vertices","faces","part","rig","details","appearance")],
    list(components=m$metadata$components,authored.rig=.overlay_test_authored_rig(m))),approved)
})
