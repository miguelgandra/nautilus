# Appearance only. Approved v11 vertices, topology, pivots and weights are fixed.
.makoCountershading <- function(geometry, belly, sections) {
  # The two triangles of each trunk quad share a countershade weight. Using
  # separate triangle centroids makes a pale/dark diagonal checkerboard across
  # the large side panels, even though they represent one continuous surface.
  body <- which(geometry$face.region=="body")
  side <- head(body,length(body)-2L*sections)
  pairs <- matrix(side,ncol=2,byrow=TRUE)
  weight <- rowMeans(matrix(belly[side],ncol=2,byrow=TRUE))
  belly[pairs[,1]] <- weight;belly[pairs[,2]] <- weight
  f <- do.call(rbind,geometry$faces);v <- geometry$vertices
  e <- v[f[,2],]-v[f[,1],];g <- v[f[,3],]-v[f[,1],]
  n <- cbind(e[,2]*g[,3]-e[,3]*g[,2],e[,3]*g[,1]-e[,1]*g[,3],
    e[,1]*g[,2]-e[,2]*g[,1])
  nz <- n[,3]/pmax(sqrt(rowSums(n^2)),1e-12)
  # Pale ventral faces on paired fins, dark dorsal faces; fixed in the bind
  # pose so colours roll and bend with the animal rather than the camera.
  u <- pmin(1,pmax(0,(nz+.03)/.35));u <- u*u*(3-2*u)
  paired <- geometry$face.region %in% c("pectoral_right","pectoral_left",
    "pelvic_right","pelvic_left")
  belly[paired] <- u[paired]*.88
  belly
}

.compileMakoSurface <- function(spec, geometry) {
  requests <- list()
  add <- function(s,y,z,kind,group=0L) {
    n <- max(length(s),length(y),length(z))
    s<-rep_len(s,n);y<-rep_len(y,n);z<-rep_len(z,n)
    centre <- stats::approx(spec$body[,1],spec$body[,5],50-s,rule=2)$y
    requests[[length(requests)+1L]] <<- list(origin=cbind(50-s,0,centre),
      direction=cbind(0,y,z-centre),kind=rep(kind,n),group=rep(group,n),regions="body")
  }
  # The separate eye lenses were transformed through the head's inverse bind
  # matrix into the same neutral coordinate system as the approved body.
  add(7.411,c(4.094,-4.094),-.894,"eye")
  # Three-point traces per gill from the supplied base-colour texture, mapped
  # back through the source UVs. Interpolate and mirror onto the faceted trunk.
  traces <- list(
    rbind(c(21.583,4.405,4.285),c(21.369,6.174,1.500),c(20.426,6.118,-2.314)),
    rbind(c(23.340,4.400,4.153),c(22.942,6.292,1.315),c(22.406,6.383,-2.622)),
    rbind(c(24.736,4.634,4.224),c(24.486,6.226,1.337),c(24.394,6.620,-2.851)),
    rbind(c(26.335,5.039,4.344),c(26.144,6.311,1.355),c(26.450,6.938,-2.585)),
    rbind(c(27.623,6.038,3.002),c(27.867,6.644,.612),c(28.196,6.960,-2.056)))
  for(side in c(1,-1)) for(i in seq_along(traces)) {
    p<-apply(traces[[i]],2,function(x) stats::approx(1:3,x,seq(1,3,length.out=9))$y)
    add(p[,1],side*p[,2],p[,3],"line",i+if(side==1)0L else 5L)
  }
  # A schematic closed-jaw seam replaces the source's small open-jaw gap.
  a<-seq(20,160,length.out=25)*pi/180
  s<-6.5+7*abs(cos(a));z<-stats::approx(spec$body[,1],spec$body[,5],50-s,rule=2)$y
  add(s,cos(a),z+sin(a),"line",11L)
  .attachOverlaySurfaceRays(geometry,requests)
}
