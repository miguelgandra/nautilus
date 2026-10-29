# Appearance only: the approved dorsal-fit geometry and wing rig stay fixed.
.devilRayCountershading <- function(geometry, belly) {
  f <- do.call(rbind,geometry$faces);v <- geometry$vertices
  e <- v[f[,2],]-v[f[,1],];g <- v[f[,3],]-v[f[,1],]
  n <- cbind(e[,2]*g[,3]-e[,3]*g[,2],e[,3]*g[,1]-e[,1]*g[,3],
    e[,1]*g[,2]-e[,2]*g[,1])
  nz <- n[,3]/pmax(sqrt(rowSums(n^2)),1e-12)
  u <- pmin(1,pmax(0,(nz+.03)/.28));u <- u*u*(3-2*u)
  body <- geometry$face.region=="body"
  belly[body] <- pmax(belly[body],u[body])
  # Pale underside with a subtle dark wing margin; no individual pattern is
  # inferred from the supplied dorsal drawing. Weights live in the bind pose.
  wing <- geometry$face.region %in% c("wing_right","wing_left")
  span <- abs(rowMeans(cbind(v[f[,1],2],v[f[,2],2],v[f[,3],2])))
  tip <- max(abs(v[geometry$region %in% c("wing_right","wing_left"),2]))
  rim <- pmin(1,pmax(0,(span/tip-.75)/.25))
  belly[wing] <- u[wing]*(1-.35*rim[wing]^2)
  paired <- geometry$face.region %in% c("pelvic_right","pelvic_left",
    "cephalic_right","cephalic_left")
  belly[paired] <- u[paired]
  belly
}

.compileDevilRaySurface <- function(geometry) {
  requests <- list()
  add <- function(origin,direction,kind,group=0L,regions) {
    n<-nrow(origin)
    requests[[length(requests)+1L]] <<- list(origin=origin,
      direction=matrix(direction,n,3,byrow=TRUE),kind=rep(kind,n),
      group=rep(group,n),regions=regions)
  }
  # Bilateral eyes behind the horn bases. Exact marking placement and all
  # underside detail are schematic: only a dorsal outline was provided.
  for(side in c(1,-1)) add(cbind(50-9.8,side*20,-1.3),c(0,-side,0),
    "eye",regions="body")
  for(side in c(1,-1)) for(i in seq_len(5L)) {
    y <- seq(c(4,3.8,3.6,3.3,3)[i],c(8.2,8,7.6,7.2,6.8)[i],length.out=9)
    s <- c(13,16,19,22,25)[i]+.4*sin(seq(0,pi,length.out=9))
    add(cbind(50-s,side*y,30),c(0,0,-1),"line",i+if(side==1)0L else 5L,
      c("body","wing_right","wing_left"))
  }
  # Subterminal mouth on the underside, behind the anterior head edge.
  y <- seq(-4.6,4.6,length.out=25)
  s <- 7.3+.6*(1-(y/4.6)^2)
  add(cbind(50-s,y,30),c(0,0,-1),"line",11L,"body")
  .attachOverlaySurfaceRays(geometry, requests)
}
