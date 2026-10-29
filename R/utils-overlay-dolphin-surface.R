# Sparse appearance anchors on the approved v19 hull; no geometry/rig changes.
.compileDolphinSurface <- function(spec,geometry) {
  requests <- list()
  add <- function(origin,direction,kind="line",group=0L) {
    n <- nrow(origin)
    if(is.null(dim(direction))) direction <- matrix(direction,n,3,byrow=TRUE)
    requests[[length(requests)+1L]] <<- list(origin=origin,direction=direction,
      kind=rep(kind,n),group=rep(group,n),regions="body")
  }
  # Source UV eye-socket edge mean: the lens itself occupies a UV hole.
  for(side in c(1,-1)) add(matrix(c(50-16.13860295,side*30,1.1700419),1,3),
    c(0,-side,0),"eye")
  radial <- function(s,angle,side,group) {
    z <- stats::approx(spec$body[,1],spec$body[,5],50-s,rule=2)$y
    a <- angle*pi/180
    add(cbind(50-s,0,z),cbind(0,side*cos(a),sin(a)),group=group)
  }
  for(side in c(1,-1)) {
    offset <- if(side==1)0L else 1L
    # Closed beak/mouth seam and chin-to-flipper stripe are schematic curves.
    radial(seq(.7,19,length.out=21),seq(2,25,length.out=21),side,1L+offset)
    radial(seq(5,26,length.out=17),seq(60,30,length.out=17),side,3L+offset)
    # The source's dark beak-to-eye boundary is simplified to one thin curve.
    radial(seq(4,16,length.out=13),seq(3,10,length.out=13),side,5L+offset)
  }
  a <- seq(0,pi,length.out=7)
  add(cbind(50-(20.5+.4*sin(a)),.35*cos(a),-40),c(0,0,1),group=7L)
  .attachOverlaySurfaceRays(geometry,requests)
}
