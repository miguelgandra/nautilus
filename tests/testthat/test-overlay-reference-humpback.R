test_that("humpback neutral reference fit retains species, parts and proportions", {
  m <- overlaySpeciesModel("whale")
  expect_identical(m$metadata$taxon,"Megaptera novaeangliae")
  expect_identical(m$rig$type,"vertical")
  expect_equal(length(m$faces),696L)
  expect_equal(nrow(m$vertices),360L)
  expect_equal(m$metadata$components$faces,c(280,132,132,24,64,64))
  expect_setequal(m$metadata$components$region,c("body","pectoral_right","pectoral_left",
    "dorsal","fluke_right","fluke_left"))
  expect_equal(length(m$appearance$surface$face),225L)
  expect_length(m$details,0)
  raw <- .overlay_test_raw_vertices(m)
  expect_gt(diff(range(raw[,1])),99.5)
  expect_lt(diff(range(raw[,1])),100.1)
  body <- raw[m$metadata$vertex.region=="body",,drop=FALSE]
  expect_gt(diff(range(body[,2])),16)
  expect_lt(diff(range(body[,2])),18)
  expect_gt(diff(range(body[,3])),17)
  expect_lt(diff(range(body[,3])),19)
  tail <- raw[startsWith(m$metadata$vertex.region,"fluke_"),,drop=FALSE]
  expect_equal(diff(range(tail[,2])),28.84,tolerance=1e-8)
  # The central trailing notch is anterior to the outer lobe tips.
  centre <- tail[abs(tail[,2])<1e-8,,drop=FALSE]
  expect_gt(min(centre[,1])-min(tail[,1]),3.5)
  for(part in c("pectoral","fluke")) {
    right <- raw[m$metadata$vertex.region==paste0(part,"_right"),,drop=FALSE]
    left <- raw[m$metadata$vertex.region==paste0(part,"_left"),,drop=FALSE];left[,2] <- -left[,2]
    expect_equal(right,left,tolerance=1e-12)
  }
  pectorals <- raw[startsWith(m$metadata$vertex.region,"pectoral_"),,drop=FALSE]
  expect_equal(diff(range(pectorals[,2])),38.16,tolerance=1e-8)
  expect_gt(max(pectorals[,3]),10)
  expect_identical(formals(renderOverlayVideo)$orientation.model,"blue_shark")
})

test_that("humpback keeps anchored pectorals and rigid whole flukes through vertical bends", {
  m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel("whale"),45)
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

test_that("humpback appearance preserves the approved geometry and complete rig", {
  approved <- readRDS(testthat::test_path("fixtures","approved-humpback-geometry-v17.rds"))
  m <- overlaySpeciesModel("whale")
  .expect_overlay_fixture(c(m[c("vertices","faces","part","rig","details")],
    list(components=m$metadata$components,authored.rig=.overlay_test_authored_rig(m))),approved)
  expect_equal(range(m$appearance$belly),c(0,1))
  body <- which(m$metadata$face.region=="body")
  pairs <- matrix(m$appearance$belly[head(body,-20L)],ncol=2,byrow=TRUE)
  expect_equal(pairs[,1],pairs[,2])
  expect_true(all(m$appearance$belly[m$metadata$face.region=="dorsal"]==0))
})

test_that("humpback markings remain attached to the actual deforming triangles", {
  m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel("whale"),45)
  s <- m$appearance$surface
  expect_equal(sum(s$kind=="eye"),2L)
  expect_equal(sum(s$kind=="spot"),14L)
  expect_setequal(unique(s$group[s$kind=="line"]),1:13)
  expect_equal(rowSums(s$barycentric),rep(1,225L))
  expect_true(all(m$metadata$face.region[s$face]=="body"))
  expect_false(any(m$appearance$hidden.faces[s$face]))
  resting <- nautilus:::.overlaySurfacePoints(m,m$vertices)
  fixed <- s$kind %in% c("eye","spot") | s$group %in% c(1,2,12,13)
  for(phase in seq(0,2*pi,length.out=9)) {
    v <- nautilus:::.deformOverlayModel(m,phase,1,45,pi/3)
    points <- nautilus:::.overlaySurfacePoints(m,v)
    expect_true(all(is.finite(points)))
    expect_equal(points[fixed,],resting[fixed,],tolerance=1e-12)
    f <- m$triangles[s$face,,drop=FALSE]
    for(axis in 1:3) {
      bounds <- cbind(v[f[,1],axis],v[f[,2],axis],v[f[,3],axis])
      expect_true(all(points[,axis]>=apply(bounds,1,min)-1e-12))
      expect_true(all(points[,axis]<=apply(bounds,1,max)+1e-12))
    }
  }
  moved <- nautilus:::.overlaySurfacePoints(m,nautilus:::.deformOverlayModel(m,pi/2,1,45,pi/3))
  expect_gt(max(abs(moved[!fixed,]-resting[!fixed,])),1e-6)
})

test_that("humpback pleats and head markings respect body occlusion", {
  m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel("whale"),45)
  cameras <- list(
    dorsal=list(right=c(-1,0,0),up=c(0,1,0),depth=c(0,0,1),view.rotation=diag(3)),
    ventral=list(right=c(-1,0,0),up=c(0,-1,0),depth=c(0,0,-1),view.rotation=diag(3)),
    lateral=list(right=c(-1,0,0),up=c(0,0,-1),depth=c(0,1,0),view.rotation=diag(3)),
    opposite=list(right=c(1,0,0),up=c(0,0,-1),depth=c(0,-1,0),view.rotation=diag(3)))
  visible <- lapply(cameras,function(camera) {
    p <- nautilus:::.projectPreparedOverlayModel(m,m$vertices,diag(3),camera)
    nautilus:::.overlaySurfaceProjection(m,m$vertices,diag(3),camera,p)$visible
  })
  s <- m$appearance$surface;eyes <- s$kind=="eye";pleats <- s$group %in% 3:11
  head <- s$kind=="spot" | s$group %in% 12:13
  expect_identical(visible$lateral[eyes],c(FALSE,TRUE))
  expect_identical(visible$opposite[eyes],c(TRUE,FALSE))
  expect_false(any(visible$dorsal[pleats]))
  expect_true(all(visible$ventral[pleats]))
  expect_false(any(visible$ventral[head]))
  expect_true(all(visible$dorsal[head]))
  for(roll in seq(0,360,by=30)) {
    v <- nautilus:::.deformOverlayModel(m,roll*pi/180,1,45,pi/3)
    r <- nautilus:::.overlayAttitudeRotation(-18,roll)
    p <- nautilus:::.projectPreparedOverlayModel(m,v,r)
    surface <- nautilus:::.overlaySurfaceProjection(m,v,r,nautilus:::.overlayAttitudeCamera(),p)
    expect_true(all(is.finite(surface$points)))
    expect_false(anyNA(surface$visible))
  }
})


test_that("approved humpback v18 preserves reviewed appearance and articulation", {
  approved <- readRDS(testthat::test_path("fixtures","approved-humpback-v18.rds"))
  m <- overlaySpeciesModel("whale")
  .expect_overlay_fixture(c(m[c("vertices","faces","part","rig","details","appearance")],
    list(components=m$metadata$components,authored.rig=.overlay_test_authored_rig(m))),approved)
})
