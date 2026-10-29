test_that("yellowfin uses a traced lateral profile and explicitly inferred transverse geometry", {
  m <- overlaySpeciesModel("tuna")
  expect_identical(m$metadata$taxon,"Thunnus albacares")
  expect_identical(m$metadata$revision,"tuna-surface-v24")
  expect_identical(m$metadata$review$status,"approved")
  expect_identical(m$metadata$review$measured.views,"lateral")
  expect_setequal(m$metadata$review$inferred.views,c("dorsal","anterior"))
  expect_equal(length(m$faces),700L)
  expect_equal(nrow(m$vertices),410L)
  expect_equal(nrow(m$metadata$components),30L)
  expect_equal(m$metadata$components$faces,c(240,56,48,48,40,40,6,6,6,6,48,48,rep(6,18)))
  expect_length(m$appearance$surface$face,104L)
  expect_length(m$details,0)
  ids <- m$metadata$components$region
  expect_equal(sum(startsWith(ids,"dorsal_finlet_")),9L)
  expect_equal(sum(startsWith(ids,"ventral_finlet_")),9L)
  raw <- sweep(m$vertices/m$metadata$geometry$scale,2,m$metadata$geometry$centre,"+")
  body <- raw[m$metadata$vertex.region=="body",,drop=FALSE]
  expect_gt(diff(range(body[,3])),25)
  expect_lt(diff(range(body[,3])),27)
  expect_gt(diff(range(body[,2])),19)
  expect_lt(diff(range(body[,2])),21)
  expect_equal(m$metadata$landmarks$width.depth.ratio,.76)
  tail <- raw[startsWith(m$metadata$vertex.region,"caudal_"),,drop=FALSE]
  expect_equal(diff(range(tail[,3])),613/1587*100,tolerance=1e-7)
  for(part in c("pectoral","pelvic","keel")) {
    right <- raw[m$metadata$vertex.region==paste0(part,"_right"),,drop=FALSE]
    left <- raw[m$metadata$vertex.region==paste0(part,"_left"),,drop=FALSE];left[,2] <- -left[,2]
    key <- function(x)apply(round(x,12),1,paste,collapse=":")
    expect_equal(sort(key(right)),sort(key(left)))
  }
  expect_identical(formals(renderOverlayVideo)$orientation.model,"blue_shark")
})

test_that("yellowfin preserves rigid sickles and whole caudal blade under bounded lateral motion", {
  m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel("tuna"),45)
  tail <- startsWith(m$metadata$vertex.region,"caudal_")
  pectorals <- startsWith(m$metadata$vertex.region,"pectoral_")
  expect_true(all(m$rig$weights[tail]==1))
  expect_true(all(m$rig$weights[pectorals]==0))
  expect_true(any(m$rig$weights>0 & m$rig$weights<1))
  for(phase in seq(0,2*pi,length.out=17)) {
    v <- nautilus:::.deformOverlayModel(m,phase,1,45,pi/3)
    expect_true(all(is.finite(v)))
    expect_equal(v[pectorals,],m$vertices[pectorals,],tolerance=1e-12)
    for(keep in list(tail,m$metadata$vertex.region=="second_dorsal",m$metadata$vertex.region=="anal"))
      expect_equal(as.vector(stats::dist(v[keep,])),as.vector(stats::dist(m$vertices[keep,])),tolerance=1e-10)
    expect_equal(v[,3],m$vertices[,3],tolerance=1e-12)
    expect_lte(max(sqrt(rowSums(v^2)))*m$display.scale,1.08+1e-10)
  }
})

test_that("yellowfin appearance preserves approved geometry and a bilateral quad palette", {
  approved <- readRDS(testthat::test_path("fixtures","approved-tuna-geometry-v23.rds"))
  m <- overlaySpeciesModel("tuna")
  expect_identical(c(m[c("vertices","faces","part","rig","details")],
    list(components=m$metadata$components,authored.rig=m$metadata$rig)),approved)
  expect_identical(m$metadata$review$geometry$status,"approved")
  expect_identical(m$metadata$review$geometry$checkpoint,"v23")
  rgb <- m$appearance$face.rgb
  expect_identical(dim(rgb),c(700L,3L))
  expect_true(all(is.finite(rgb) & rgb>=0 & rgb<=1))
  expect_equal(rgb[seq(1,219,2),],rgb[seq(2,220,2),])
  for(i in 0:10) for(pair in list(c(1,4),c(2,3),c(5,10),c(6,9),c(7,8)))
    expect_identical(rgb[(i*10+pair[1])*2-1,],rgb[(i*10+pair[2])*2-1,])
  gold <- rgb[m$metadata$face.region %in% c("second_dorsal","anal","dorsal_finlet_1"),,drop=FALSE]
  expect_true(all(gold[,1]>gold[,2] & gold[,2]>gold[,3]))
  expect_true(all(gold[,2]>.6 & gold[,3]<.35))
  expect_true(all(m$metadata$face.region[m$appearance$surface$face]=="body"))
  expect_false(any(m$appearance$hidden.faces[m$appearance$surface$face]))
})

test_that("yellowfin posterior markings follow the mesh while facial anchors remain fixed", {
  m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel("tuna"),45)
  s <- m$appearance$surface
  expect_equal(sum(s$kind=="eye"),2L)
  expect_equal(sum(s$kind=="line"),102L)
  expect_setequal(s$group[s$kind=="line"],1:6)
  expect_equal(rowSums(s$barycentric),rep(1,104L))
  resting <- nautilus:::.overlaySurfacePoints(m,m$vertices)
  fixed <- s$group<=4
  moved <- FALSE
  for(phase in seq(0,2*pi,length.out=9)) {
    v <- nautilus:::.deformOverlayModel(m,phase,1,45,pi/3)
    points <- nautilus:::.overlaySurfacePoints(m,v)
    expect_true(all(is.finite(points)))
    expect_equal(points[fixed,],resting[fixed,],tolerance=1e-12)
    moved <- moved || any(abs(points[!fixed,]-resting[!fixed,])>1e-6)
    f <- m$triangles[s$face,,drop=FALSE]
    for(axis in 1:3) {
      bounds <- cbind(v[f[,1],axis],v[f[,2],axis],v[f[,3],axis])
      expect_true(all(points[,axis]>=apply(bounds,1,min)-1e-12))
      expect_true(all(points[,axis]<=apply(bounds,1,max)+1e-12))
    }
  }
  expect_true(moved)
})

test_that("yellowfin markings are hidden on the far side and remain finite through rolls", {
  m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel("tuna"),45)
  s <- m$appearance$surface
  cameras <- list(lateral=list(right=c(-1,0,0),up=c(0,0,-1),depth=c(0,1,0),view.rotation=diag(3)),
    opposite=list(right=c(1,0,0),up=c(0,0,-1),depth=c(0,-1,0),view.rotation=diag(3)))
  visible <- lapply(cameras,function(camera) {
    p <- nautilus:::.projectPreparedOverlayModel(m,m$vertices,diag(3),camera)
    nautilus:::.overlaySurfaceProjection(m,m$vertices,diag(3),camera,p)$visible
  })
  expect_identical(visible$lateral[s$kind=="eye"],c(FALSE,TRUE))
  expect_identical(visible$opposite[s$kind=="eye"],c(TRUE,FALSE))
  expect_false(any(visible$lateral[s$kind=="line" & s$group%%2==1]))
  expect_false(any(visible$opposite[s$kind=="line" & s$group%%2==0]))
  expect_true(any(visible$lateral[s$group>=5]))
  expect_true(any(visible$opposite[s$group>=5]))
  for(roll in seq(0,360,by=30)) {
    v <- nautilus:::.deformOverlayModel(m,roll*pi/180,1,45,pi/3)
    r <- nautilus:::.overlayAttitudeRotation(-18,roll)
    p <- nautilus:::.projectPreparedOverlayModel(m,v,r)
    projected <- nautilus:::.overlaySurfaceProjection(m,v,r,nautilus:::.overlayAttitudeCamera(),p)
    expect_true(all(is.finite(projected$points)))
    expect_false(anyNA(projected$visible))
  }
})

test_that("approved tuna v24 preserves reviewed colour, markings and articulation", {
  approved <- readRDS(testthat::test_path("fixtures","approved-tuna-v24.rds"))
  m <- overlaySpeciesModel("tuna")
  expect_identical(c(m[c("vertices","faces","part","rig","details","appearance")],
    list(components=m$metadata$components,authored.rig=m$metadata$rig)),approved)
  expect_identical(m$metadata$review$status,"approved")
  expect_identical(m$metadata$review$checkpoint,"v24")
})
