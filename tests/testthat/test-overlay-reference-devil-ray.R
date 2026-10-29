test_that("devil ray declares the measured view and retains a named closed mesh", {
  m<-overlaySpeciesModel("devil_ray")
  expect_equal(length(m$faces),696L)
  expect_equal(nrow(m$vertices),366L)
  expect_equal(m$metadata$components$faces,c(144,144,144,60,60,72,24,24,24))
  expect_setequal(m$metadata$components$region,c("body","wing_right","wing_left",
    "cephalic_right","cephalic_left","tail","dorsal","pelvic_right","pelvic_left"))
  raw<-.overlay_test_raw_vertices(m)
  expect_equal(diff(range(raw[,1])),100,tolerance=1e-10)
  expect_gt(diff(range(raw[,2])),76)
  expect_lt(diff(range(raw[,2])),77)
  tail<-raw[m$metadata$vertex.region=="tail",]
  expect_equal(diff(range(tail[,1])),52.1077005378,tolerance=1e-8)
  expect_gt(diff(range(tail[,1])),50) # Visible reference tail retained, not the old stub.
  expect_lt(diff(range(tail[,2])),1)
  for(part in c("wing","cephalic","pelvic")) {
    right<-raw[m$metadata$vertex.region==paste0(part,"_right"),]
    left<-raw[m$metadata$vertex.region==paste0(part,"_left"),];left[,2]<- -left[,2]
    sort.points<-function(p) p[do.call(order,as.data.frame(round(p,10))),,drop=FALSE]
    expect_equal(sort.points(right),sort.points(left),tolerance=1e-12)
  }
})

test_that("devil-ray wings retain symmetric bounded deformation with stationary roots", {
  m<-nautilus:::.prepareOverlayModel(overlaySpeciesModel("devil_ray"),45)
  right<-m$metadata$vertex.region=="wing_right";left<-m$metadata$vertex.region=="wing_left"
  stationary<-!(right|left) | m$rig$weights==0
  expect_true(all(m$rig$weights[!(right|left)]==0))
  expect_true(any(m$rig$weights[right]>0 & m$rig$weights[right]<1))
  for(phase in seq(0,2*pi,length.out=25)) {
    v<-nautilus:::.deformOverlayModel(m,phase,1,45,pi/3)
    expect_true(all(is.finite(v)))
    expect_equal(v[stationary,],m$vertices[stationary,],tolerance=1e-12)
    expect_equal(v[right,1],v[left,1],tolerance=1e-12)
    expect_equal(v[right,2],-v[left,2],tolerance=1e-12)
    expect_equal(v[right,3],v[left,3],tolerance=1e-12)
    expect_lte(max(sqrt(rowSums(v^2)))*m$display.scale,1.08+1e-10)
  }
  expect_identical(formals(renderOverlayVideo)$orientation.model,"blue_shark")
})

test_that("devil ray surface refinement preserves the approved v9 geometry and rig", {
  approved<-readRDS(testthat::test_path("fixtures","approved-devil-ray-geometry-v9.rds"))
  m<-overlaySpeciesModel("devil_ray")
  fields<-setdiff(names(approved),"components")
  expect_identical(c(m[fields],list(components=m$metadata$components)),approved)
  expect_identical(formals(renderOverlayVideo)$orientation.model,"blue_shark")
})

test_that("devil ray surface marks follow the deformed disc without extra geometry", {
  m<-nautilus:::.prepareOverlayModel(overlaySpeciesModel("devil_ray"),45)
  s<-m$appearance$surface
  expect_equal(length(s$face),117L) # Two eyes, 90 gill points, 25 mouth points.
  expect_equal(rowSums(s$barycentric),rep(1,117))
  expect_equal(sum(s$kind=="eye"),2L)
  expect_setequal(unique(s$group[s$kind=="line"]),1:11)
  moving<-m$metadata$face.region[s$face] %in% c("wing_left","wing_right")
  expect_true(any(moving))
  resting<-nautilus:::.overlaySurfacePoints(m,m$vertices)
  for(phase in seq(0,2*pi,length.out=9)) {
    v<-nautilus:::.deformOverlayModel(m,phase,1,45,pi/3)
    points<-nautilus:::.overlaySurfacePoints(m,v)
    expect_true(all(is.finite(points)))
    expect_equal(points[!moving,],resting[!moving,],tolerance=1e-12)
    f<-m$triangles[s$face,,drop=FALSE]
    for(axis in 1:3) {
      bounds<-cbind(v[f[,1],axis],v[f[,2],axis],v[f[,3],axis])
      expect_true(all(points[,axis]>=apply(bounds,1,min)-1e-12))
      expect_true(all(points[,axis]<=apply(bounds,1,max)+1e-12))
    }
  }
  expect_gt(max(abs(points[moving,]-resting[moving,])),1e-5)
  expect_true(any(m$appearance$belly[m$metadata$face.region=="wing_right"]>.9))
  expect_true(any(m$appearance$belly[m$metadata$face.region=="wing_right"]==0))
  expect_error({bad<-m;bad$appearance$smooth.faces<-TRUE;
    nautilus:::.prepareOverlayModel(bad)},"smooth face mask")
})

test_that("devil ray gills stay ventral and eyes and mouth obey visibility", {
  m<-nautilus:::.prepareOverlayModel(overlaySpeciesModel("devil_ray"),20)
  cameras<-list(
    dorsal=list(right=c(-1,0,0),up=c(0,1,0),depth=c(0,0,1),view.rotation=diag(3)),
    ventral=list(right=c(-1,0,0),up=c(0,-1,0),depth=c(0,0,-1),view.rotation=diag(3)),
    anterior=list(right=c(0,1,0),up=c(0,0,-1),depth=c(-1,0,0),view.rotation=diag(3)))
  visible<-lapply(cameras,function(camera) {
    p<-nautilus:::.projectPreparedOverlayModel(m,m$vertices,diag(3),camera)
    nautilus:::.overlaySurfaceProjection(m,m$vertices,diag(3),camera,p)$visible
  })
  s<-m$appearance$surface;gills<-s$group %in% 1:10;eyes<-s$kind=="eye"
  expect_false(any(visible$dorsal[gills]))
  expect_true(all(visible$ventral[gills]))
  expect_true(all(visible$dorsal[eyes]))
  expect_true(all(visible$anterior[eyes]))
  expect_false(any(visible$ventral[eyes]))
  expect_false(any(visible$dorsal[s$group==11]))
  expect_true(all(visible$ventral[s$group==11]))
  expect_true(any(visible$anterior[s$group==11]))
  # Full rolls and strong strokes exercise moving wing occlusion and backfaces.
  for(roll in seq(0,360,by=30)) {
    v<-nautilus:::.deformOverlayModel(m,roll*pi/180,1,45,pi/3)
    r<-nautilus:::.overlayAttitudeRotation(-18,roll)
    p<-nautilus:::.projectPreparedOverlayModel(m,v,r)
    s<-nautilus:::.overlaySurfaceProjection(m,v,r,nautilus:::.overlayAttitudeCamera(),p)
    expect_true(all(is.finite(s$points)))
    expect_false(anyNA(s$visible))
  }
})


test_that("approved v10 devil ray retains its reviewed appearance, geometry and motion", {
  approved<-readRDS(testthat::test_path("fixtures","approved-devil-ray-v10.rds"))
  m<-overlaySpeciesModel("devil_ray")
  fields<-setdiff(names(approved),"components")
  expect_identical(c(m[fields],list(components=m$metadata$components)),approved)
})
