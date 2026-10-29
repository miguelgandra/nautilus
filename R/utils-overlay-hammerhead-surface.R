# Appearance only: approved v13 geometry, component pivots and rig stay fixed.
.hammerheadCountershading <- function(geometry, belly, sections) {
  body <- which(geometry$face.region=="body")
  side <- head(body,length(body)-2L*sections)
  pairs <- matrix(side,ncol=2,byrow=TRUE)
  weight <- rowMeans(matrix(belly[side],ncol=2,byrow=TRUE))
  belly[pairs[,1]] <- weight;belly[pairs[,2]] <- weight
  # Bind-pose normals identify underside faces of the transverse head and fins.
  f <- do.call(rbind,geometry$faces);v <- geometry$vertices
  e <- v[f[,2],]-v[f[,1],];g <- v[f[,3],]-v[f[,1],]
  n <- cbind(e[,2]*g[,3]-e[,3]*g[,2],e[,3]*g[,1]-e[,1]*g[,3],
    e[,1]*g[,2]-e[,2]*g[,1])
  nz <- n[,3]/pmax(sqrt(rowSums(n^2)),1e-12)
  u <- pmin(1,pmax(0,(nz+.03)/.35));u <- u*u*(3-2*u)
  head <- geometry$face.region=="cephalofoil"
  belly[head] <- u[head]
  paired <- geometry$face.region %in% c("pectoral_right","pectoral_left",
    "pelvic_right","pelvic_left")
  belly[paired] <- u[paired]*.92
  # Subtle dusky pectoral tips, fixed to the mesh rather than the camera.
  pectoral <- startsWith(geometry$face.region,"pectoral_")
  span <- abs(rowMeans(cbind(v[f[,1],2],v[f[,2],2],v[f[,3],2])))
  tip <- max(abs(v[startsWith(geometry$region,"pectoral_"),2]))
  rim <- pmin(1,pmax(0,(span/tip-.7)/.3))
  belly[pectoral] <- belly[pectoral]*(1-.6*rim[pectoral]^2)
  belly
}

.compileHammerheadSurface <- function(spec, geometry) {
  requests <- list()
  add <- function(origin,direction,kind,group=0L,regions) {
    n<-nrow(origin)
    if(is.null(dim(direction))) direction<-matrix(direction,n,3,byrow=TRUE)
    requests[[length(requests)+1L]] <<- list(origin=origin,direction=direction,
      kind=rep(kind,n),group=rep(group,n),regions=regions)
  }
  # UV eye centre in the supplied neutral asset, attached to the adapted blade.
  for(side in c(1,-1)) add(cbind(50-7.283,side*20,.451),c(0,-side,0),
    "eye",regions="cephalofoil")
  traces <- .hammerheadGillTraces()
  for(side in c(1,-1)) for(i in seq_along(traces)) {
    p<-apply(traces[[i]],2,function(x) stats::approx(1:3,x,seq(1,3,length.out=9))$y)
    centre<-stats::approx(spec$body[,1],spec$body[,5],50-p[,1],rule=2)$y
    add(cbind(50-p[,1],0,centre),cbind(0,side*p[,2],p[,3]-centre),
      "line",i+if(side==1)0L else 5L,"body")
  }
  # Closed ventral mouth: schematic because the cephalofoil is species-adapted.
  y<-seq(-3,3,length.out=25);s<-5.5+2.3*(y/3)^2
  add(cbind(50-s,y,20),c(0,0,-1),"line",11L,c("body","cephalofoil"))
  .attachOverlaySurfaceRays(geometry,requests)
}

# Approximate three-point texture traces mapped through source UV triangles.
.hammerheadGillTraces <- function() list(
  rbind(c(20.948570,4.985497,1.414005),c(21.008181,4.294963,3.524216),c(21.019500,3.553762,5.687164)),
  rbind(c(19.852003,4.893056,0.586973),c(19.730036,4.425237,3.147314),c(19.728595,3.484827,5.231869)),
  rbind(c(18.798300,5.007904,0.262322),c(18.530278,4.513196,2.864326),c(18.377217,3.630046,5.102770)),
  rbind(c(17.674515,5.063619,0.064356),c(17.341870,4.656158,2.560228),c(16.991369,3.996907,4.952402)),
  rbind(c(16.414496,5.268836,0.845593),c(16.121667,4.913925,2.530272),c(15.753687,4.288156,4.332738)))
