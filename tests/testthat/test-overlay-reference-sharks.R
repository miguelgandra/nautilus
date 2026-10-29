test_that("surface anchors remain on the same mesh throughout motion", {
  for (id in c("whale_shark","blue_shark")) {
    m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel(id),45)
    s<-m$appearance$surface
    expect_length(s$face,nrow(s$barycentric))
    expect_true(all(m$metadata$face.region[s$face]=="body"))
    expect_equal(rowSums(s$barycentric),rep(1,length(s$face)))
    expect_equal(sum(s$kind=="eye"),2)
    for(phase in seq(0,2*pi,length.out=9)) {
      v<-nautilus:::.deformOverlayModel(m,phase,1,45,pi/3)
      points<-nautilus:::.overlaySurfacePoints(m,v)
      f<-m$triangles[s$face,,drop=FALSE]
      for(axis in 1:3) {
        bounds<-cbind(v[f[,1],axis],v[f[,2],axis],v[f[,3],axis])
        expect_true(all(points[,axis]>=apply(bounds,1,min)-1e-12))
        expect_true(all(points[,axis]<=apply(bounds,1,max)+1e-12))
      }
      eyes<-s$kind=="eye"
      expect_equal(points[eyes,],nautilus:::.overlaySurfacePoints(m,m$vertices)[eyes,])
    }
    expect_error({bad<-m;bad$appearance$surface$face[1]<-9999;
      nautilus:::.prepareOverlayModel(bad)},"face indices")
    expect_error({bad<-m;bad$appearance$surface$barycentric[1,]<-c(1,1,1);
      nautilus:::.prepareOverlayModel(bad)},"barycentric")
    expect_error({bad<-m;bad$appearance$surface$kind[1]<-"invalid";
      nautilus:::.prepareOverlayModel(bad)},"kinds")
  }
})

test_that("surface depth tests hide far-side eyes and occluded marks", {
  m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel("whale_shark"),20)
  camera <- list(right=c(-1,0,0),up=c(0,0,-1),depth=c(0,-1,0),view.rotation=diag(3))
  p<-nautilus:::.projectPreparedOverlayModel(m,m$vertices,diag(3),camera)
  s<-nautilus:::.overlaySurfaceProjection(m,m$vertices,diag(3),camera,p)
  expect_equal(sum(s$visible & m$appearance$surface$kind=="eye"),1)
  for(roll in seq(0,360,by=30)) {
    r<-nautilus:::.overlayAttitudeRotation(0,roll)
    p<-nautilus:::.projectPreparedOverlayModel(m,m$vertices,r,camera)
    s<-nautilus:::.overlaySurfaceProjection(m,m$vertices,r,camera,p)
    expect_true(all(is.finite(s$points)))
    expect_false(anyNA(s$visible))
  }
  # Two parallel triangles: a mark on the farther triangle must be hidden,
  # even when its own triangle belongs to the visible/painter face list.
  toy <- list(vertices=rbind(c(0,0,0),c(1,0,0),c(0,1,0),c(0,0,1),c(1,0,1),c(0,1,1)),
    triangles=rbind(1:3,4:6),display.scale=1,
    appearance=list(surface=list(face=c(1L,2L),barycentric=matrix(1/3,2,3))))
  camera<-list(right=c(1,0,0),up=c(0,1,0),depth=c(0,0,1),view.rotation=diag(3))
  result<-nautilus:::.overlaySurfaceProjection(toy,toy$vertices,diag(3),camera,list(order=2:1))
  expect_identical(result$visible,c(TRUE,FALSE))
})

test_that("reference-fitted shaders do not alter geometry or use random state", {
  set.seed(51);before<-.Random.seed
  for(id in c("blue_shark","whale_shark")) {
    m<-overlaySpeciesModel(id)
    expect_equal(length(m$faces),696L)
    expect_equal(nrow(m$vertices),372L)
    expect_true(any(m$appearance$belly>0 & m$appearance$belly<1))
    expect_equal(sort(m$metadata$components$region),sort(c("body","pectoral_left","pectoral_right",
      "dorsal","second_dorsal","pelvic_left","pelvic_right","anal","caudal_upper","caudal_lower","keel_left","keel_right")))
  }
  expect_identical(.Random.seed,before)
  expect_identical(formals(renderOverlayVideo)$orientation.model,"blue_shark")
})

test_that("approved v6 sharks retain their reviewed geometry, rigs and appearance", {
  approved<-readRDS(testthat::test_path("fixtures","approved-sharks-v6.rds"))
  fields<-c("vertices","faces","part","rig","appearance","details","eyes")
  for(id in names(approved)) {
    m<-overlaySpeciesModel(id)
    expect_equal(c(m[fields],list(components=m$metadata$components)),approved[[id]],tolerance=1e-13)
  }
})
