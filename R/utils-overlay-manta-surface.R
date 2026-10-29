# Appearance only: the approved v7 geometry, component pivots and weights are fixed.
.mantaCountershading <- function(geometry, belly) {
  f <- do.call(rbind,geometry$faces);v <- geometry$vertices
  e <- v[f[,2],]-v[f[,1],];g <- v[f[,3],]-v[f[,1],]
  n <- cbind(e[,2]*g[,3]-e[,3]*g[,2],e[,3]*g[,1]-e[,1]*g[,3],
    e[,1]*g[,2]-e[,2]*g[,1])
  nz <- n[,3]/pmax(sqrt(rowSums(n^2)),1e-12)
  # Bind-space weights stay attached during a roll or wing stroke.
  u <- pmin(1,pmax(0,(nz+.03)/.28));u <- u*u*(3-2*u)
  body <- geometry$face.region=="body"
  belly[body] <- pmax(belly[body],u[body])
  wing <- geometry$face.region %in% c("wing_right","wing_left")
  span <- abs(rowMeans(cbind(v[f[,1],2],v[f[,2],2],v[f[,3],2])))
  rim <- pmin(1,pmax(0,(span-38)/10.15))
  belly[wing] <- u[wing]*(1-.65*rim[wing]^2)
  pelvic <- geometry$face.region %in% c("pelvic_right","pelvic_left")
  belly[pelvic] <- u[pelvic]
  # The supplied asset has pale cephalic lobes on both surfaces.
  cephalic <- geometry$face.region %in% c("cephalic_right","cephalic_left")
  belly[cephalic] <- .80+.20*u[cephalic]
  belly
}

.compileMantaSurface <- function(geometry) {
  requests <- list()
  add <- function(origin,direction,kind,group=0L,regions) {
    n<-nrow(origin)
    requests[[length(requests)+1L]] <<- list(origin=origin,
      direction=matrix(direction,n,3,byrow=TRUE),kind=rep(kind,n),
      group=rep(group,n),regions=regions)
  }
  # Eyes sit behind the cephalic bases, on the lateral head surface.
  for(side in c(1,-1)) add(cbind(50-8.0,side*20,.1),c(0,-side,0),
    "eye",regions="body")
  # Five curved ventral slits per side, guided by the reference texture/UV
  # layout (approximately 12--24% axial TL). Attach to the exposed disc surface, which
  # may be trunk or wing at a root; both follow their actual deformed triangles.
  for(side in c(1,-1)) for(i in seq_len(5L)) {
    y <- seq(c(6.4,6.0,5.4,4.7,3.9)[i],c(11.0,10.6,10.0,9.0,7.9)[i],length.out=9)
    s <- c(12,15,18,21,24)[i]+.5*sin(seq(0,pi,length.out=9))
    add(cbind(50-s,side*y,30),c(0,0,-1),"line",i+if(side==1)0L else 5L,
      c("body","wing_right","wing_left"))
  }
  # Terminal mouth across the front cap, behind the curled cephalic lobes.
  y <- seq(-7.8,7.8,length.out=25)
  add(cbind(60,y,2.8-.45*(1-(y/7.8)^2)),c(-1,0,0),"line",11L,"body")
  .attachOverlaySurfaceRays(geometry, requests)
}
