test_that("reference turtle records reconstruction and a bounded named-part budget", {
  m <- overlaySpeciesModel("turtle")
  expect_identical(m$metadata$taxon,"Chelonia mydas")
  expect_identical(m$metadata$revision,"turtle-surface-v16")
  expect_identical(m$metadata$review$status,"approved")
  expect_match(m$metadata$review$reconstruction,"intact positive lateral half mirrored")
  expect_setequal(m$metadata$review$measured.views,c("dorsal","lateral","anterior"))
  expect_equal(length(m$faces),690L)
  expect_equal(nrow(m$vertices),361L)
  expect_equal(m$metadata$components$faces,c(260,84,84,48,48,112,48,6))
  expect_setequal(m$metadata$components$region,c("body","foreflipper_right","foreflipper_left",
    "hindflipper_right","hindflipper_left","head_neck","plastron","tail"))
  expect_equal(length(m$appearance$surface$face),293L)
  expect_length(m$details,0)
  expect_identical(m$metadata$rig$flipper.mode,"rigid")
  raw <- sweep(m$vertices/m$metadata$geometry$scale,2,m$metadata$geometry$centre,"+")
  body <- raw[m$metadata$vertex.region=="body",,drop=FALSE]
  expect_gt(diff(range(body[,1])),80)
  expect_lt(diff(range(body[,1])),83)
  expect_gt(diff(range(body[,2])),59)
  expect_lt(diff(range(body[,2])),61)
  expect_gt(diff(range(body[,3])),25)
  expect_lt(diff(range(body[,3])),28)
  # Flippers extend past the short tail; TL is explicitly snout-to-tail,
  # not the full posed axial bounding box or a view-specific fit.
  expect_equal(m$metadata$landmarks$TL,100)
  expect_gt(diff(range(raw[,1])),106)
  expect_lt(diff(range(raw[,1])),109)
  expect_equal(diff(range(raw[,2])),93.2,tolerance=1e-7)
  for (part in c("foreflipper","hindflipper")) {
    right <- raw[m$metadata$vertex.region==paste0(part,"_right"),,drop=FALSE]
    left <- raw[m$metadata$vertex.region==paste0(part,"_left"),,drop=FALSE]
    left[,2] <- -left[,2]
    expect_equal(right,left,tolerance=1e-12)
  }
  expect_identical(formals(renderOverlayVideo)$orientation.model,"blue_shark")
})

test_that("turtle rigid stroke preserves each blade and leaves shell and hind paddles fixed", {
  m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel("turtle"),45)
  right <- m$metadata$vertex.region=="foreflipper_right"
  left <- m$metadata$vertex.region=="foreflipper_left"
  still <- !(right|left)
  expect_true(all(m$rig$weights[right|left]==1))
  expect_true(all(m$rig$weights[still]==0))
  for (phase in seq(0,2*pi,length.out=17)) {
    v <- nautilus:::.deformOverlayModel(m,phase,1,45,pi/3)
    expect_true(all(is.finite(v)))
    expect_equal(v[still,],m$vertices[still,],tolerance=1e-12)
    for (part in list(right,left))
      expect_equal(as.vector(stats::dist(v[part,])),as.vector(stats::dist(m$vertices[part,])),tolerance=1e-10)
    mirrored <- v[left,];mirrored[,2] <- -mirrored[,2]
    expect_equal(v[right,],mirrored,tolerance=1e-12)
    expect_lte(max(sqrt(rowSums(v^2)))*m$display.scale,1.08+1e-10)
  }
})

test_that("turtle appearance leaves the approved v15 geometry and rigid rig identical", {
  approved <- readRDS(testthat::test_path("fixtures","approved-turtle-geometry-v15.rds"))
  m <- overlaySpeciesModel("turtle")
  expect_identical(c(m[setdiff(names(approved),"components")],list(components=m$metadata$components)),approved)
  expect_identical(m$metadata$review$geometry$status,"approved")
  expect_identical(m$metadata$review$geometry$checkpoint,"v15")
  expect_equal(range(m$appearance$belly),c(0,1))
  expect_identical(m$appearance$dorsal,"#68583b")
  body <- which(m$metadata$face.region=="body")
  pairs <- matrix(m$appearance$belly[head(body,-20L)],ncol=2,byrow=TRUE)
  expect_equal(pairs[,1],pairs[,2])
})

test_that("turtle seams and scale plates follow their actual triangles", {
  m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel("turtle"),45)
  s <- m$appearance$surface
  expect_equal(sum(s$kind=="eye"),2L)
  expect_equal(rowSums(s$barycentric),rep(1,293L))
  expect_setequal(unique(s$group[s$kind=="line"]),1:26)
  expect_true(all(m$metadata$face.region[s$face[s$kind=="eye"]]=="head_neck"))
  expect_true(all(m$metadata$face.region[s$face[s$group %in% 1:13]]=="body"))
  moving <- s$group %in% 15:20
  expect_equal(sum(moving),54L)
  expect_true(all(startsWith(m$metadata$face.region[s$face[moving]],"foreflipper_")))
  resting <- nautilus:::.overlaySurfacePoints(m,m$vertices)
  for(phase in seq(0,2*pi,length.out=9)) {
    v <- nautilus:::.deformOverlayModel(m,phase,1,45,pi/3)
    points <- nautilus:::.overlaySurfacePoints(m,v)
    expect_true(all(is.finite(points)))
    expect_equal(points[!moving,],resting[!moving,],tolerance=1e-12)
    for(groups in list(15:17,18:20)) {
      i <- s$group %in% groups
      expect_equal(as.vector(stats::dist(points[i,])),as.vector(stats::dist(resting[i,])),tolerance=1e-10)
    }
    f <- m$triangles[s$face,,drop=FALSE]
    for(axis in 1:3) {
      bounds <- cbind(v[f[,1],axis],v[f[,2],axis],v[f[,3],axis])
      expect_true(all(points[,axis]>=apply(bounds,1,min)-1e-12))
      expect_true(all(points[,axis]<=apply(bounds,1,max)+1e-12))
    }
  }
  displaced <- nautilus:::.overlaySurfacePoints(m,nautilus:::.deformOverlayModel(m,pi/2,1,45,pi/3))
  expect_gt(max(abs(displaced[moving,]-resting[moving,])),.01)
})

test_that("turtle markings respect shell occlusion through rolls", {
  m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel("turtle"),45)
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
  expect_false(any(visible$ventral[s$group %in% 1:13]))
  expect_false(any(visible$dorsal[s$group %in% 21:26]))
  expect_true(any(visible$dorsal[s$group %in% 1:13]))
  expect_true(all(visible$ventral[s$group %in% 21:25]))
  # The hind paddles legitimately cover the outer ends of the last seam.
  expect_equal(sum(!visible$ventral[s$group==26]),4L)
  expect_false(any(m$appearance$hidden.faces[s$face]))
  for(roll in seq(0,360,by=30)) {
    v <- nautilus:::.deformOverlayModel(m,roll*pi/180,1,45,pi/3)
    r <- nautilus:::.overlayAttitudeRotation(-18,roll)
    p <- nautilus:::.projectPreparedOverlayModel(m,v,r)
    surface <- nautilus:::.overlaySurfaceProjection(m,v,r,nautilus:::.overlayAttitudeCamera(),p)
    expect_true(all(is.finite(surface$points)))
    expect_false(anyNA(surface$visible))
  }
})


test_that("approved turtle v16 retains reviewed appearance and articulation", {
  approved <- readRDS(testthat::test_path("fixtures","approved-turtle-v16.rds"))
  m <- overlaySpeciesModel("turtle")
  expect_identical(c(m[setdiff(names(approved),"components")],list(components=m$metadata$components)),approved)
  expect_identical(m$metadata$review$status,"approved")
  expect_identical(m$metadata$review$checkpoint,"v16")
})
