test_that("striped marlin keeps reference proportions within the named-part budget", {
  m <- overlaySpeciesModel("marlin")
  expect_identical(m$metadata$label,"Striped marlin")
  expect_identical(m$metadata$taxon,"Kajikia audax")
  expect_identical(m$metadata$revision,"marlin-surface-v22")
  expect_identical(m$metadata$review$status,"approved")
  expect_identical(m$rig$type,"lateral")
  expect_equal(length(m$faces),696L)
  expect_equal(nrow(m$vertices),376L)
  expect_equal(m$metadata$components$faces,c(224,36,64,24,24,24,72,72,24,24,48,48,6,6))
  expect_setequal(m$metadata$components$region,c("body","bill","dorsal","second_dorsal",
    "anal","second_anal","pectoral_right","pectoral_left","pelvic_right","pelvic_left",
    "caudal_upper","caudal_lower","keel_right","keel_left"))
  expect_equal(length(m$appearance$surface$face),282L)
  expect_length(m$details,0)
  raw <- sweep(m$vertices/m$metadata$geometry$scale,2,m$metadata$geometry$centre,"+")
  expect_gt(diff(range(raw[,1])),99)
  expect_lt(diff(range(raw[,1])),100.1)
  bill <- raw[m$metadata$vertex.region=="bill",,drop=FALSE]
  expect_gt(diff(range(bill[,1])),19)
  expect_lt(diff(range(bill[,1])),20)
  tail <- raw[startsWith(m$metadata$vertex.region,"caudal_"),,drop=FALSE]
  expect_equal(diff(range(tail[,3])),25.3,tolerance=1e-7)
  pectoral <- raw[startsWith(m$metadata$vertex.region,"pectoral_"),,drop=FALSE]
  expect_gt(diff(range(pectoral[,2])),21)
  expect_lt(diff(range(pectoral[,2])),22)
  dorsal <- raw[m$metadata$vertex.region=="dorsal",,drop=FALSE]
  expect_equal(diff(range(dorsal[,1])),35.7,tolerance=1e-7)
  expect_lt(min(dorsal[,3]),-15.5)
  for(part in c("pectoral","pelvic","keel")) {
    right <- raw[m$metadata$vertex.region==paste0(part,"_right"),,drop=FALSE]
    left <- raw[m$metadata$vertex.region==paste0(part,"_left"),,drop=FALSE];left[,2] <- -left[,2]
    key <- function(x)apply(round(x,12),1,paste,collapse=":")
    expect_equal(sort(key(right)),sort(key(left)))
  }
  expect_identical(formals(renderOverlayVideo)$orientation.model,"blue_shark")
})

test_that("marlin keeps bill and paired fins fixed and moves the whole caudal blade rigidly", {
  m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel("marlin"),45)
  raw <- sweep(m$vertices/m$metadata$geometry$scale,2,m$metadata$geometry$centre,"+")
  fixed <- m$metadata$vertex.region=="bill" | startsWith(m$metadata$vertex.region,"pectoral_") |
    startsWith(m$metadata$vertex.region,"pelvic_") | (m$metadata$vertex.region=="body" & raw[,1]>=0)
  tail <- startsWith(m$metadata$vertex.region,"caudal_")
  expect_true(all(m$rig$weights[fixed]==0))
  expect_true(all(m$rig$weights[tail]==1))
  expect_true(any(m$rig$weights>0 & m$rig$weights<1))
  for(phase in seq(0,2*pi,length.out=17)) {
    v <- nautilus:::.deformOverlayModel(m,phase,1,45,pi/3)
    expect_true(all(is.finite(v)))
    expect_equal(v[fixed,],m$vertices[fixed,],tolerance=1e-12)
    expect_equal(as.vector(stats::dist(v[tail,])),as.vector(stats::dist(m$vertices[tail,])),tolerance=1e-10)
    expect_equal(v[,3],m$vertices[,3],tolerance=1e-12)
    expect_lte(max(sqrt(rowSums(v^2)))*m$display.scale,1.08+1e-10)
  }
})

test_that("marlin appearance preserves approved geometry and the complete lateral rig", {
  approved <- readRDS(testthat::test_path("fixtures","approved-marlin-geometry-v21.rds"))
  m <- overlaySpeciesModel("marlin")
  expect_identical(c(m[c("vertices","faces","part","rig","details")],
    list(components=m$metadata$components,authored.rig=m$metadata$rig)),approved)
  expect_identical(m$metadata$review$geometry$status,"approved")
  expect_identical(m$metadata$review$geometry$checkpoint,"v21")
  rgb <- m$appearance$face.rgb
  expect_identical(dim(rgb),c(696L,3L))
  expect_true(all(is.finite(rgb) & rgb>=0 & rgb<=1))
  expect_equal(rgb[seq(1,207,2),],rgb[seq(2,208,2),])
  expect_equal(sum(m$appearance$surface$kind=="eye"),2L)
  expect_equal(sum(m$appearance$surface$group>=5),216L)
})

test_that("surface line colours validate and reach the drawing call without changing the fallback", {
  model <- overlaySpeciesModel("marlin");n <- length(model$appearance$surface$face)
  for(colour in list(rep("red",n-1L),rep(1,n),replace(rep("red",n),1,NA_character_),
      replace(rep("red",n),1,"not-a-colour"))) {
    bad <- model;bad$appearance$surface$line.colour <- colour
    expect_error(nautilus:::.prepareOverlayModel(bad,20),"Surface line colours")
  }
  m <- nautilus:::.prepareOverlayModel(model,20)
  camera <- list(right=c(-1,0,0),up=c(0,0,-1),depth=c(0,1,0),view.rotation=diag(3))
  p <- nautilus:::.projectPreparedOverlayModel(m,m$vertices,diag(3),camera)
  painted <- NULL
  testthat::local_mocked_bindings(segments=function(...,col,lwd) painted<<-col,.package="graphics")
  grDevices::pdf(file=NULL);on.exit(grDevices::dev.off(),add=TRUE)
  graphics::plot(0,0,type="n")
  nautilus:::.drawOverlaySurface(m,m$vertices,diag(3),camera,p)
  expect_true(any(painted==grDevices::adjustcolor("#aac4cf",.75)))
  expect_true(any(painted==grDevices::adjustcolor("#1e3540",.75)))
  m$appearance$surface$line.colour <- NULL
  nautilus:::.drawOverlaySurface(m,m$vertices,diag(3),camera,p)
  expect_identical(painted,grDevices::adjustcolor("#1e3540",.75))
})

test_that("marlin bars stay on their deforming triangles while forward facial lines remain fixed", {
  m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel("marlin"),45)
  s <- m$appearance$surface
  expect_equal(rowSums(s$barycentric),rep(1,282L))
  expect_setequal(unique(s$group[s$kind=="line"]),1:28)
  expect_true(all(m$metadata$face.region[s$face]=="body"))
  expect_false(any(m$appearance$hidden.faces[s$face]))
  resting <- nautilus:::.overlaySurfacePoints(m,m$vertices)
  fixed <- s$group<=4
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
  expect_gt(max(abs(moved[!fixed,]-resting[!fixed,])),.01)
})

test_that("marlin far-side bars and eyes are occluded through turns and rolls", {
  m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel("marlin"),45)
  cameras <- list(
    lateral=list(right=c(-1,0,0),up=c(0,0,-1),depth=c(0,1,0),view.rotation=diag(3)),
    opposite=list(right=c(1,0,0),up=c(0,0,-1),depth=c(0,-1,0),view.rotation=diag(3)))
  visible <- lapply(cameras,function(camera) {
    p <- nautilus:::.projectPreparedOverlayModel(m,m$vertices,diag(3),camera)
    nautilus:::.overlaySurfaceProjection(m,m$vertices,diag(3),camera,p)$visible
  })
  s <- m$appearance$surface;eyes <- s$kind=="eye"
  expect_identical(visible$lateral[eyes],c(FALSE,TRUE))
  expect_identical(visible$opposite[eyes],c(TRUE,FALSE))
  expect_false(any(visible$lateral[s$group>=5 & s$group%%2==1]))
  expect_false(any(visible$opposite[s$group>=5 & s$group%%2==0]))
  expect_true(any(visible$lateral[s$group>=5]))
  expect_true(any(visible$opposite[s$group>=5]))
  for(roll in seq(0,360,by=30)) {
    v <- nautilus:::.deformOverlayModel(m,roll*pi/180,1,45,pi/3)
    r <- nautilus:::.overlayAttitudeRotation(-18,roll)
    p <- nautilus:::.projectPreparedOverlayModel(m,v,r)
    surface <- nautilus:::.overlaySurfaceProjection(m,v,r,nautilus:::.overlayAttitudeCamera(),p)
    expect_true(all(is.finite(surface$points)))
    expect_false(anyNA(surface$visible))
  }
})


test_that("approved marlin v22 preserves reviewed colour, markings and articulation", {
  approved <- readRDS(testthat::test_path("fixtures","approved-marlin-v22.rds"))
  m <- overlaySpeciesModel("marlin")
  expect_identical(c(m[c("vertices","faces","part","rig","details","appearance")],
    list(components=m$metadata$components,authored.rig=m$metadata$rig)),approved)
  expect_identical(m$metadata$review$status,"approved")
  expect_identical(m$metadata$review$checkpoint,"v22")
})
