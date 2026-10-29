# Appearance only: the approved v23 geometry, pivots and complete rig stay fixed.
.compileTunaSurface <- function(spec,geometry) {
  requests <- list();colours <- character()
  add <- function(origin,direction,kind="line",group=0L,colour="#35413e") {
    n <- nrow(origin)
    if(is.null(dim(direction))) direction <- matrix(direction,n,3,byrow=TRUE)
    requests[[length(requests)+1L]] <<- list(origin=origin,direction=direction,
      kind=rep(kind,n),group=rep(group,n),regions="body")
    colours <<- c(colours,rep(colour,n))
  }
  # The lateral drawing gives x/z, not 3D depth. Project onto the approved hull.
  eye <- c(1435,668)
  for(side in c(1,-1)) add(matrix(c(50-(1594-eye[1])/1587*100,
    side*30,(eye[2]-700)/1587*100),1,3),c(0,-side,0),"eye")
  radial <- function(s,angle,side,group) {
    z <- stats::approx(spec$body[,1],spec$body[,5],50-s,rule=2)$y;a <- angle*pi/180
    add(cbind(50-s,0,z),cbind(0,side*cos(a),sin(a)),group=group)
  }
  # The mesh closes the source's open jaw; the closed lip seam is schematic.
  for(side in c(1,-1)) radial(seq(.4,9.6,length.out=17),
    seq(5,32,length.out=17),side,if(side==1)1L else 2L)
  project.profile <- function(points,n,side,group,colour="#35413e") {
    at <- seq(1,nrow(points),length.out=n)
    xy <- cbind(stats::approx(seq_len(nrow(points)),points[,1],at)$y,
      stats::approx(seq_len(nrow(points)),points[,2],at)$y)
    add(cbind(50-(1594-xy[,1])/1587*100,side*30,(xy[,2]-700)/1587*100),
      c(0,-side,0),group=group,colour=colour)
  }
  operculum <- rbind(c(1240,568),c(1221,591),c(1206,620),c(1195,654),c(1193,694),
    c(1195,739),c(1190,786),c(1183,831),c(1174,874))
  lateral <- rbind(c(1140,675),c(1060,656),c(960,642),c(860,639),c(760,643),
    c(650,656),c(530,677),c(420,692),c(320,698),c(225,699))
  for(side in c(1,-1)) {
    project.profile(operculum,13L,side,if(side==1)3L else 4L)
    project.profile(lateral,21L,side,if(side==1)5L else 6L,colour="#34414a")
  }
  surface <- .attachOverlaySurfaceRays(geometry,requests)
  surface$line.colour <- colours
  surface
}
