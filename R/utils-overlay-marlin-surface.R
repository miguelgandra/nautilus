# Appearance only: approved v21 mesh, named pivots and complete rig stay fixed.
.compileMarlinSurface <- function(spec,geometry) {
  requests <- list();colours <- character()
  add <- function(origin,direction,kind="line",group=0L,colour="#1e3540") {
    n <- nrow(origin)
    if(is.null(dim(direction))) direction <- matrix(direction,n,3,byrow=TRUE)
    requests[[length(requests)+1L]] <<- list(origin=origin,direction=direction,
      kind=rep(kind,n),group=rep(group,n),regions="body")
    colours <<- c(colours,rep(colour,n))
  }
  # Eye centre mapped through the positive-side source texture UV triangle.
  for(side in c(1,-1)) add(matrix(c(50-25.00091507,side*30,2.19374332),1,3),
    c(0,-side,0),"eye")
  radial <- function(s,angle,side,group,colour="#1e3540") {
    z <- stats::approx(spec$body[,1],spec$body[,5],50-s,rule=2)$y;a <- angle*pi/180
    add(cbind(50-s,0,z),cbind(0,side*cos(a),sin(a)),group=group,colour=colour)
  }
  for(side in c(1,-1)) {
    offset <- if(side==1)0L else 1L
    # The approved mouth is closed. These lip and opercular curves are schematic.
    radial(seq(18.8,29.2,length.out=19),seq(0,25,length.out=19),side,1L+offset)
    a <- seq(-38,53,length.out=13)
    radial(31.2+.55*sin(a*pi/180)^2,a,side,3L+offset)
  }
  # Bar axial centres are UV-mapped source brightness peaks. The curves are
  # simplified radial arcs, mirrored across the approved bilateral trunk.
  bars <- c(33.91003017, 37.72900427, 39.55820734, 45.32606849, 48.27771140, 50.66005694, 55.32264039, 59.78549519, 63.29513551, 68.13709972, 71.92772589, 74.58673390)
  a <- seq(-65,30,length.out=9)
  for(i in seq_along(bars)) for(side in c(1,-1))
    radial(bars[i]+.14*sin(a*pi/180),a,side,
      4L+(i-1L)*2L+if(side==1)1L else 2L,colour="#aac4cf")
  surface <- .attachOverlaySurfaceRays(geometry,requests)
  surface$line.colour <- colours
  surface
}
