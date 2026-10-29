# Appearance only: approved v15 geometry, shoulder pivots and rigid blades stay fixed.
.turtleCountershading <- function(geometry) {
  f <- do.call(rbind,geometry$faces);v <- geometry$vertices
  e <- v[f[,2],]-v[f[,1],];g <- v[f[,3],]-v[f[,1],]
  n <- cbind(e[,2]*g[,3]-e[,3]*g[,2],e[,3]*g[,1]-e[,1]*g[,3],e[,1]*g[,2]-e[,2]*g[,1])
  nz <- n[,3]/pmax(sqrt(rowSums(n^2)),1e-12)
  u <- pmin(1,pmax(0,(nz-.05)/.35));u <- u*u*(3-2*u)
  # Dark carapace above the rim; cream ventral shell and appendage undersides.
  # Fixed bind-pose weights rotate with the animal, never with the camera.
  body <- which(geometry$face.region=="body")
  paired <- matrix(head(body,-20L),ncol=2,byrow=TRUE)
  shade <- rowMeans(matrix(u[head(body,-20L)],ncol=2,byrow=TRUE))
  u[paired[,1]] <- shade;u[paired[,2]] <- shade
  skin <- geometry$face.region %in% c("head_neck","foreflipper_right","foreflipper_left",
    "hindflipper_right","hindflipper_left","tail")
  u[skin] <- .18+.78*u[skin]
  u[geometry$face.region=="plastron"] <- 1
  u
}

.compileTurtleSurface <- function(spec,geometry) {
  requests <- list()
  add <- function(origin,direction,kind="line",group=0L,regions) {
    n <- nrow(origin)
    if(is.null(dim(direction))) direction <- matrix(direction,n,3,byrow=TRUE)
    requests[[length(requests)+1L]] <<- list(origin=origin,direction=direction,
      kind=rep(kind,n),group=rep(group,n),regions=regions)
  }
  interpolate <- function(p,steps=3L) {
    do.call(rbind,lapply(seq_len(nrow(p)-1L),function(i) {
      t <- seq(0,1,length.out=steps+1L)
      if(i<nrow(p)-1L)t <- head(t,-1L)
      outer(1-t,p[i,])+outer(t,p[i+1L,])
    }))
  }
  # Large shell seams traced on the source diffuse texture and mapped through
  # its UV triangles. Costal boundaries use the intact half and are mirrored.
  traces <- .turtleScuteTraces()
  for(i in seq_len(5L)) {
    p <- interpolate(traces[[i]])
    add(cbind(50-p[,1],p[,2],-80),c(0,0,1),group=i,regions="body")
  }
  for(side in c(1,-1)) for(i in 6:9) {
    p <- interpolate(traces[[i]])
    add(cbind(50-p[,1],side*abs(p[,2])*.97,-80),c(0,0,1),
      group=i+if(side==1)0L else 4L,regions="body")
  }
  # Lens centre estimated from the surrounding source UV socket boundary.
  for(side in c(1,-1)) add(matrix(c(50-4.36452,side*20,-14.61973),1,3),
    c(0,-side,0),kind="eye",regions="head_neck")
  # A small closed beak seam is schematic; the approved head closes the socket
  # and jaw rather than reproducing the texture's fine scales and tiny gaps.
  a <- seq(0,pi,length.out=21)
  s <- 1.8+7*abs(cos(a));h <- .referenceTurtleControls()$head
  z <- stats::approx(h[,1],(h[,3]+h[,4])/2,s,rule=2)$y
  add(cbind(50-s,0,z),cbind(0,cos(a),.5*sin(a)),group=14L,regions="head_neck")
  # A few elongated scale plates on each rigid blade retain an inset-readable
  # suggestion of the reference texture; these are schematic, not UV traces.
  fore <- .referenceTurtleControls()$foreflipper
  for(side in c(1,-1)) for(i in 1:3) {
    span <- c(28,35,41)[i]
    y <- span+c(-1.7,0,1.7,0,-1.7)
    front <- stats::approx(fore[,1],fore[,2],y)$y
    back <- stats::approx(fore[,1],fore[,3],y)$y
    x <- (front+back)/2+c(0,.22,0,-.22,0)*(front-back)
    p <- interpolate(cbind(x,side*y),steps=2L)
    add(cbind(p,-80),c(0,0,1),group=14L+i+if(side==1)0L else 3L,
      regions=paste0("foreflipper_",if(side==1)"right" else "left"))
  }
  # Simple plastron seams, projected onto whichever ventral shell surface is
  # exposed. They stay attached to the same mesh during the roll diagnostic.
  add(cbind(50-seq(30,90,length.out=25),0,40),c(0,0,-1),group=21L,
    regions=c("body","plastron"))
  for(i in 1:5) {
    y <- seq(-1,1,length.out=11)*c(12,19,22,20,12)[i]
    s <- c(34,45,58,72,85)[i]+1.5*abs(y)/max(abs(y))
    add(cbind(50-s,y,40),c(0,0,-1),group=21L+i,regions=c("body","plastron"))
  }
  .attachOverlaySurfaceRays(geometry,requests)
}

.turtleScuteTraces <- function() list(
  matrix(c(23.059724,-1.910454,-22.729569,
    30.484797,8.664905,-21.252637,
    38.233097,6.653178,-24.305408,
    41.161207,-0.294511,-25.411250,
    38.008707,-5.499210,-24.371496,
    30.237535,-6.995731,-22.790101,
    23.059724,-1.910454,-22.729569),ncol=3,byrow=TRUE),
  matrix(c(38.233097,6.653178,-24.305408,
    45.952726,8.213331,-23.745237,
    55.942951,4.006572,-24.163292,
    56.844295,-2.297391,-24.298009,
    50.846998,-7.085172,-24.177816,
    38.008707,-5.499210,-24.371496),ncol=3,byrow=TRUE),
  matrix(c(55.942951,4.006572,-24.163292,
    63.171978,7.093735,-21.752961,
    72.760988,3.750534,-20.833080,
    73.047219,-3.519461,-21.021292,
    65.718789,-6.958425,-21.412163,
    56.844295,-2.297391,-24.298009),ncol=3,byrow=TRUE),
  matrix(c(72.760988,3.750534,-20.833080,
    76.792332,6.170928,-18.322817,
    83.478530,3.013475,-16.589515,
    84.135295,-2.543195,-16.623336,
    79.017995,-5.992642,-17.657822,
    73.047219,-3.519461,-21.021292),ncol=3,byrow=TRUE),
  matrix(c(83.478530,3.013475,-16.589515,
    88.116338,2.445157,-15.396949,
    91.074533,-0.650913,-13.949463,
    88.011133,-3.998421,-14.509351,
    84.135295,-2.543195,-16.623336),ncol=3,byrow=TRUE),
  matrix(c(38.233097,6.653178,-24.305408,
    42.785106,15.379080,-19.553092,
    39.831402,23.419050,-13.470774),ncol=3,byrow=TRUE),
  matrix(c(55.942951,4.006572,-24.163292,
    57.798826,13.871578,-19.259353,
    58.138331,26.955013,-9.774723),ncol=3,byrow=TRUE),
  matrix(c(72.760988,3.750534,-20.833080,
    74.997890,11.438441,-15.577060,
    76.255319,22.103160,-8.899113),ncol=3,byrow=TRUE),
  matrix(c(83.478530,3.013475,-16.589515,
    88.036859,6.591598,-13.770552,
    92.479549,12.564982,-9.603450),ncol=3,byrow=TRUE)
)
