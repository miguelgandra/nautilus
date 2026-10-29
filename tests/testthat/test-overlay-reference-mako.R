test_that("reference mako retains measured proportions and separate fin components", {
  m<-overlaySpeciesModel("mako")
  expect_equal(length(m$faces),700L)
  expect_equal(nrow(m$vertices),374L)
  expect_equal(m$metadata$components$faces,c(260,84,84,48,24,24,24,24,56,56,8,8))
  expect_setequal(m$metadata$components$region,c("body","pectoral_right","pectoral_left",
    "dorsal","second_dorsal","pelvic_right","pelvic_left","anal",
    "caudal_upper","caudal_lower","keel_right","keel_left"))
  raw<-.overlay_test_raw_vertices(m)
  expect_equal(diff(range(raw[,1])),99.98,tolerance=1e-6)
  expect_equal(diff(range(raw[,2])),35.82,tolerance=1e-8)
  body<-raw[m$metadata$vertex.region=="body",]
  expect_gt(diff(range(body[,2])),15)
  expect_lt(diff(range(body[,2])),15.5)
  for(part in c("pectoral","pelvic","keel")) {
    right<-raw[m$metadata$vertex.region==paste0(part,"_right"),]
    left<-raw[m$metadata$vertex.region==paste0(part,"_left"),];left[,2]<- -left[,2]
    sort.points<-function(p) p[do.call(order,as.data.frame(round(p,10))),,drop=FALSE]
    expect_equal(sort.points(right),sort.points(left),tolerance=1e-12)
  }
})

test_that("mako full caudal blade follows the terminal rig and pectorals stay anchored", {
  m<-nautilus:::.prepareOverlayModel(overlaySpeciesModel("mako"),45)
  tail<-m$metadata$vertex.region %in% c("caudal_upper","caudal_lower")
  pectoral<-startsWith(m$metadata$vertex.region,"pectoral")
  expect_true(all(m$rig$weights[tail]==1))
  expect_true(all(m$rig$weights[pectoral]==0))
  expect_true(any(m$rig$weights>0 & m$rig$weights<1))
  for(phase in seq(0,2*pi,length.out=17)) {
    v<-nautilus:::.deformOverlayModel(m,phase,1,45,pi/3)
    expect_true(all(is.finite(v)))
    expect_equal(v[pectoral,],m$vertices[pectoral,],tolerance=1e-12)
    # The entire caudal assembly rotates as one blade, preserving pairwise
    # distances across both lobes instead of flexing only a distal tip.
    expect_equal(as.vector(stats::dist(v[tail,])),as.vector(stats::dist(m$vertices[tail,])),tolerance=1e-10)
    expect_lte(max(sqrt(rowSums(v^2)))*m$display.scale,1.08+1e-10)
  }
  expect_identical(formals(renderOverlayVideo)$orientation.model,"blue_shark")
})

test_that("mako appearance preserves approved geometry, pivots and rig", {
  approved<-readRDS(testthat::test_path("fixtures","approved-mako-geometry-v11.rds"))
  m<-overlaySpeciesModel("mako")
  expect_identical(c(m[setdiff(names(approved),"components")],
    list(components=m$metadata$components)),approved)
  # Adjacent triangles within each trunk quad must not form a diagonal
  # countershade checkerboard. End caps are excluded from the quad sequence.
  body<-which(m$metadata$face.region=="body")
  pairs<-matrix(m$appearance$belly[head(body,-20)],ncol=2,byrow=TRUE)
  expect_equal(pairs[,1],pairs[,2])
  expect_equal(range(m$appearance$belly),c(0,1))
})

test_that("mako markings stay on the head and gill triangles during tail beats", {
  m<-nautilus:::.prepareOverlayModel(overlaySpeciesModel("mako"),45)
  s<-m$appearance$surface
  expect_equal(length(s$face),117L)
  expect_equal(rowSums(s$barycentric),rep(1,117))
  expect_equal(sum(s$kind=="eye"),2L)
  expect_setequal(unique(s$group[s$kind=="line"]),1:11)
  expect_true(all(m$metadata$face.region[s$face]=="body"))
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

test_that("mako markings respect near-side visibility through full rolls", {
  m<-nautilus:::.prepareOverlayModel(overlaySpeciesModel("mako"),45)
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
  expect_true(all(visible$dorsal[eyes]))
  expect_false(any(visible$ventral[eyes]))
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


test_that("approved mako v12 retains its reviewed appearance and geometry", {
  approved<-readRDS(testthat::test_path("fixtures","approved-mako-v12.rds"))
  m<-overlaySpeciesModel("mako")
  expect_identical(c(m[setdiff(names(approved),"components")],
    list(components=m$metadata$components)),approved)
})
