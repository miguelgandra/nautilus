test_that("reference hammerhead retains smooth species identity and named anatomy", {
  m<-overlaySpeciesModel("hammerhead")
  expect_identical(m$metadata$taxon,"Sphyrna zygaena")
  expect_equal(length(m$faces),696L)
  expect_equal(nrow(m$vertices),370L)
  expect_equal(m$metadata$components$faces,c(220,156,40,40,48,24,24,24,24,56,40))
  expect_setequal(m$metadata$components$region,c("body","cephalofoil",
    "pectoral_right","pectoral_left","dorsal","second_dorsal",
    "pelvic_right","pelvic_left","anal","caudal_upper","caudal_lower"))
  raw<-.overlay_test_raw_vertices(m)
  expect_gt(diff(range(raw[,1])),99.9)
  expect_equal(diff(range(raw[m$metadata$vertex.region=="cephalofoil",2])),26.88)
  for(part in c("pectoral","pelvic")) {
    right<-raw[m$metadata$vertex.region==paste0(part,"_right"),]
    left<-raw[m$metadata$vertex.region==paste0(part,"_left"),];left[,2]<- -left[,2]
    key<-function(p) p[do.call(order,as.data.frame(round(p,10))),,drop=FALSE]
    expect_equal(key(right),key(left),tolerance=1e-12)
  }
})

test_that("hammerhead head and pectorals stay anchored while whole tail rotates", {
  m<-nautilus:::.prepareOverlayModel(overlaySpeciesModel("hammerhead"),45)
  stationary<-m$metadata$vertex.region %in% c("cephalofoil","pectoral_right","pectoral_left")
  tail<-m$metadata$vertex.region %in% c("caudal_upper","caudal_lower")
  expect_true(all(m$rig$weights[stationary]==0))
  expect_true(all(m$rig$weights[tail]==1))
  expect_true(any(m$rig$weights>0 & m$rig$weights<1))
  for(phase in seq(0,2*pi,length.out=17)) {
    v<-nautilus:::.deformOverlayModel(m,phase,1,45,pi/3)
    expect_true(all(is.finite(v)))
    expect_equal(v[stationary,],m$vertices[stationary,],tolerance=1e-12)
    expect_equal(as.vector(stats::dist(v[tail,])),as.vector(stats::dist(m$vertices[tail,])),tolerance=1e-10)
    expect_lte(max(sqrt(rowSums(v^2)))*m$display.scale,1.08+1e-10)
  }
  expect_identical(formals(renderOverlayVideo)$orientation.model,"blue_shark")
})

test_that("hammerhead appearance preserves approved geometry, pivots and rig", {
  approved<-readRDS(testthat::test_path("fixtures","approved-hammerhead-geometry-v13.rds"))
  m<-overlaySpeciesModel("hammerhead")
  .expect_overlay_fixture(c(m[setdiff(names(approved),"components")],
    list(components=m$metadata$components)),approved)
  # Adjacent triangles within each trunk quad must not form a diagonal
  # countershade checkerboard. End caps are excluded from the quad sequence.
  body<-which(m$metadata$face.region=="body")
  pairs<-matrix(m$appearance$belly[head(body,-20)],ncol=2,byrow=TRUE)
  expect_equal(pairs[,1],pairs[,2])
  expect_equal(range(m$appearance$belly),c(0,1))
})

test_that("hammerhead markings stay on the head and gill triangles during tail beats", {
  m<-nautilus:::.prepareOverlayModel(overlaySpeciesModel("hammerhead"),45)
  s<-m$appearance$surface
  expect_equal(length(s$face),117L)
  expect_equal(rowSums(s$barycentric),rep(1,117))
  expect_equal(sum(s$kind=="eye"),2L)
  expect_setequal(unique(s$group[s$kind=="line"]),1:11)
  expect_true(all(m$metadata$face.region[s$face] %in% c("body","cephalofoil")))
  resting<-nautilus:::.overlaySurfacePoints(m,m$vertices)
  for(phase in seq(0,2*pi,length.out=9)) {
    v<-nautilus:::.deformOverlayModel(m,phase,1,45,pi/3)
    points<-nautilus:::.overlaySurfacePoints(m,v)
    expect_true(all(is.finite(points)))
    expect_equal(points,resting,tolerance=1e-12)
    f<-m$triangles[s$face,,drop=FALSE]
    for(axis in 1:3) {
      bounds<-cbind(v[f[,1],axis],v[f[,2],axis],v[f[,3],axis])
      expect_true(all(points[,axis]>=apply(bounds,1,min)-1e-12))
      expect_true(all(points[,axis]<=apply(bounds,1,max)+1e-12))
    }
  }
})

test_that("hammerhead markings respect near-side visibility through full rolls", {
  m<-nautilus:::.prepareOverlayModel(overlaySpeciesModel("hammerhead"),45)
  cameras<-list(
    dorsal=list(right=c(-1,0,0),up=c(0,1,0),depth=c(0,0,1),view.rotation=diag(3)),
    ventral=list(right=c(-1,0,0),up=c(0,-1,0),depth=c(0,0,-1),view.rotation=diag(3)),
    lateral=list(right=c(-1,0,0),up=c(0,0,-1),depth=c(0,1,0),view.rotation=diag(3)),
    opposite=list(right=c(1,0,0),up=c(0,0,-1),depth=c(0,-1,0),view.rotation=diag(3)))
  visible<-lapply(cameras,function(camera) {
    p<-nautilus:::.projectPreparedOverlayModel(m,m$vertices,diag(3),camera)
    nautilus:::.overlaySurfaceProjection(m,m$vertices,diag(3),camera,p)$visible
  })
  s<-m$appearance$surface;eyes<-s$kind=="eye";mouth<-s$group==11
  expect_identical(visible$lateral[eyes],c(FALSE,TRUE))
  expect_identical(visible$opposite[eyes],c(TRUE,FALSE))
  expect_false(any(visible$dorsal[eyes]))
  expect_true(all(visible$ventral[eyes]))
  expect_false(any(visible$dorsal[mouth]))
  expect_true(all(visible$ventral[mouth]))
  expect_false(any(visible$lateral[s$group %in% 1:5]))
  expect_false(any(visible$opposite[s$group %in% 6:10]))
  for(roll in seq(0,360,by=30)) {
    v<-nautilus:::.deformOverlayModel(m,roll*pi/180,1,45,pi/3)
    r<-nautilus:::.overlayAttitudeRotation(-18,roll)
    p<-nautilus:::.projectPreparedOverlayModel(m,v,r)
    surface<-nautilus:::.overlaySurfaceProjection(m,v,r,nautilus:::.overlayAttitudeCamera(),p)
    expect_true(all(is.finite(surface$points)))
    expect_false(anyNA(surface$visible))
  }
})




test_that("approved hammerhead v14 retains reviewed geometry and appearance", {
  approved<-readRDS(testthat::test_path("fixtures","approved-hammerhead-v14.rds"))
  m<-overlaySpeciesModel("hammerhead")
  .expect_overlay_fixture(c(m[setdiff(names(approved),"components")],
    list(components=m$metadata$components)),approved)
})
