# Appearance only: approved v17 vertices, component pivots and vertical rig stay fixed.
.humpbackCountershading <- function(geometry,belly,sections) {
  f <- do.call(rbind,geometry$faces);v <- geometry$vertices
  centroid <- (v[f[,1],]+v[f[,2],]+v[f[,3],])/3
  smooth <- function(u) {u <- pmin(1,pmax(0,u));u*u*(3-2*u)}
  # Retain a dark rostrum and soften the transition into the pale throat.
  body <- which(geometry$face.region=="body")
  belly[body] <- belly[body]*smooth((50-centroid[body,1]-8)/16)
  side <- head(body,length(body)-2L*sections)
  pairs <- matrix(side,ncol=2,byrow=TRUE)
  weight <- rowMeans(matrix(belly[side],ncol=2,byrow=TRUE))
  belly[pairs[,1]] <- weight;belly[pairs[,2]] <- weight
  e <- v[f[,2],]-v[f[,1],];g <- v[f[,3],]-v[f[,1],]
  n <- cbind(e[,2]*g[,3]-e[,3]*g[,2],e[,3]*g[,1]-e[,1]*g[,3],
    e[,1]*g[,2]-e[,2]*g[,1])
  nz <- n[,3]/pmax(sqrt(rowSums(n^2)),1e-12)
  pectoral <- startsWith(geometry$face.region,"pectoral_")
  span <- abs(centroid[,2])
  # Both surfaces of the supplied pectoral blades are pale; their roots and
  # extreme tips remain dusky. These bind-pose values travel with the mesh.
  belly[pectoral] <- (.84+.13*smooth(nz[pectoral]))*
    smooth((span[pectoral]-6.5)/3)*(1-.65*smooth((span[pectoral]-17.5)/1.5))
  fluke <- startsWith(geometry$face.region,"fluke_")
  belly[fluke] <- .92*smooth((nz[fluke]-.05)/.4)
  belly
}

.compileHumpbackSurface <- function(spec,geometry) {
  requests <- list()
  add <- function(origin,direction,kind="line",group=0L) {
    n <- nrow(origin)
    if(is.null(dim(direction))) direction <- matrix(direction,n,3,byrow=TRUE)
    requests[[length(requests)+1L]] <<- list(origin=origin,direction=direction,
      kind=rep(kind,n),group=rep(group,n),regions="body")
  }
  # Eye centre mapped through the supplied texture's UV triangle; mirrored to
  # match the approved bilateral head. Ray attachment uses the actual low-poly hull.
  for(side in c(1,-1)) add(matrix(c(50-22.5138513,side*30,-4.81005707),1,3),
    c(0,-side,0),"eye")
  # Closed mouth seams are schematic, following the widening head below the eyes.
  # Origins are inside the trunk, with radial rays to each side of the lower jaw.
  s <- seq(1.6,23,length.out=23)
  centre <- stats::approx(spec$body[,1],spec$body[,5],50-s,rule=2)$y
  for(side in c(1,-1)) {
    angle <- seq(12,4,length.out=length(s))*pi/180
    add(cbind(50-s,0,centre),cbind(0,side*cos(angle),sin(angle)),group=if(side==1)1L else 2L)
  }
  # A sparse suggestion of the source's longitudinal throat pleats. They stop
  # before the peduncle and are curves on the ventral hull, not extra geometry.
  for(i in seq_len(9L)) {
    lateral <- (i-5)/4
    s <- seq(6+8*abs(lateral),54-7*abs(lateral),length.out=17)
    width <- stats::approx(spec$body[,1],spec$body[,2],50-s,rule=2)$y
    y <- lateral*.72*width*sin(pi*(s-2)/65)^.5
    add(cbind(50-s,y,40),c(0,0,-1),group=2L+i)
  }
  # Sparse head tubercle highlights: schematic surface marks, not raised bumps.
  for(side in c(1,-1)) {
    s <- c(3.5,6.5,10,14,18)
    width <- stats::approx(spec$body[,1],spec$body[,2],50-s,rule=2)$y
    add(cbind(50-s,side*.63*width,-40),c(0,0,1),"spot")
  }
  s <- c(4.5,8,12,16)
  add(cbind(50-s,0,-40),c(0,0,1),"spot")
  # A small paired blowhole indication, intentionally simplified for inset size.
  for(side in c(1,-1)) {
    s <- seq(23.5,25.5,length.out=5)
    add(cbind(50-s,side*(.35+.15*sin(seq(0,pi,length.out=5))),-40),
      c(0,0,1),group=if(side==1)12L else 13L)
  }
  .attachOverlaySurfaceRays(geometry,requests)
}
