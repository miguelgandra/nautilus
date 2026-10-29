.targetedRawVertices <- function(m) {
  .overlay_test_raw_vertices(m)
}

test_that("whale shark retains approved reference-fit proportions and part allocation", {
  m <- overlaySpeciesModel("whale_shark"); v <- .targetedRawVertices(m)
  region <- function(name) v[m$metadata$vertex.region==name,,drop=FALSE]
  body <- region("body")
  expect_equal(body[which.max(abs(body[,2])),1],38)
  expect_equal(diff(range(body[,3])),15.26)
  expect_equal(max(abs(body[body[,1]>49.5,2])),4.8)
  expect_gt(max(abs(body[body[,1]< -29,2])),3)
  pectoral <- region("pectoral_right")
  expect_equal(max(pectoral[,1]),31.575)
  expect_gt(diff(range(pectoral[,2])),13)
  expect_gt(diff(range(pectoral[,1])),20)
  expect_equal(length(m$faces),696L)
  expect_equal(m$metadata$components$faces,c(288,84,84,40,24,24,24,24,48,40,8,8))
})

test_that("whale shark bends progressively through the trunk and moves both tail lobes", {
  m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel("whale_shark"),20)
  raw <- .targetedRawVertices(m)
  body <- m$metadata$vertex.region=="body"
  stations <- c(2,-12,-23)
  weights <- vapply(stations,function(x) mean(m$rig$weights[body & abs(raw[,1]-x)<1e-8]),numeric(1))
  expect_true(all(is.finite(weights)) && all(diff(weights)>0))
  expect_gt(weights[1],.1)
  tail <- startsWith(m$metadata$vertex.region,"caudal_")
  expect_true(all(m$rig$weights[tail]==1))
  for(phase in c(.4,1.3,2.7,4.1,5.3)) {
    v <- nautilus:::.deformOverlayModel(m,phase,1,20,pi/3)
    for(part in c("caudal_upper","caudal_lower")) {
      ix<-m$metadata$vertex.region==part
      expect_gt(max(abs(v[ix,2]-m$vertices[ix,2])),.02)
    }
    # Both lobes form one rigid terminal blade, not separately flexing tips.
    expect_equal(as.vector(stats::dist(v[tail,])),as.vector(stats::dist(m$vertices[tail,])),tolerance=1e-12)
  }
  # Markings are now bound to triangles, so they follow the deformed mesh
  # without a second, potentially divergent set of motion weights.
  expect_true(any(m$appearance$surface$kind=="spot"))
})

test_that("turtle foreflippers rotate rigidly around mirrored shoulder pivots", {
  m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel("turtle"),45)
  right <- m$metadata$vertex.region=="foreflipper_right"
  left <- m$metadata$vertex.region=="foreflipper_left"
  expect_true(all(m$rig$weights[right|left]==1))
  expect_true(all(m$rig$weights[!(right|left)]==0))
  pivot <- m$metadata$components[m$metadata$components$region=="foreflipper_right",
    c("normalised.pivot.x","normalised.pivot.y","normalised.pivot.z")]
  expect_equal(unname(unlist(pivot)),m$rig$pivot)
  for(phase in seq(0,2*pi,length.out=17)) {
    v <- nautilus:::.deformOverlayModel(m,phase,1,45,pi/3)
    for(ix in list(right,left))
      expect_equal(as.vector(stats::dist(v[ix,])),as.vector(stats::dist(m$vertices[ix,])),tolerance=1e-12)
    expect_equal(v[!(right|left),],m$vertices[!(right|left),],tolerance=1e-12)
    expect_equal(v[right,1],v[left,1],tolerance=1e-12)
    expect_equal(v[right,2],-v[left,2],tolerance=1e-12)
    expect_equal(v[right,3],v[left,3],tolerance=1e-12)
  }
  for(id in c("manta","devil_ray"))
    expect_true(any(overlaySpeciesModel(id)$rig$weights>0 & overlaySpeciesModel(id)$rig$weights<1))
})

test_that("blue shark tight fit remains bounded and compass clears the circle", {
  m <- nautilus:::.prepareOverlayModel(overlaySpeciesModel("blue_shark"),45)
  legacy.fit <- m; legacy.fit$appearance <- NULL
  legacy.fit <- nautilus:::.prepareOverlayModel(legacy.fit,45)
  expect_gt(m$display.scale,legacy.fit$display.scale*1.2)
  for(phase in seq(0,2*pi,length.out=73)) {
    v <- nautilus:::.deformOverlayModel(m,phase,1,45,pi/3)
    expect_lte(max(sqrt(rowSums(v^2)))*m$display.scale,1.02+1e-12)
  }
  c <- m$appearance$compass
  expect_gt(sqrt(sum(c[1:2]^2))-c[3],1.05)
  expect_gt(m$appearance$eye.cex,.62)
  expect_true(any(m$appearance$hidden.faces))
  expect_true(any(m$appearance$belly>0 & m$appearance$belly<1))
  expect_true(all(c("second_dorsal","pelvic_left","pelvic_right") %in% m$metadata$components$region))
  bad <- m;bad$appearance$belly <- 2
  expect_error(nautilus:::.prepareOverlayModel(bad),"countershading")
  bad <- m;bad$appearance$compass <- c(10,1,.2)
  expect_error(nautilus:::.prepareOverlayModel(bad),"compass")
})
