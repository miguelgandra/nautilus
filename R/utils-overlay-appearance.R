# Optional species appearance. Absent metadata keeps the original renderer path.
.validateOverlayAppearance <- function(appearance, n) {
  if (is.null(appearance)) return(NULL)
  if (!is.list(appearance)) .abort("Model appearance must be a list.")
  for (field in c("smooth","cull")) .assert_flag(appearance[[field]],paste0("appearance$",field))
  .assert_choice(appearance$fit,"appearance$fit",c("legacy","tight"))
  .assert_number(appearance$eye.cex,"appearance$eye.cex",min=.1,max=2)
  for (field in c("body.faces","hidden.faces")) {
    x <- appearance[[field]]
    if (!is.logical(x) || length(x)!=n || anyNA(x))
      .abort("Appearance face masks must match the model faces.")
  }
  if (!is.null(appearance$smooth.faces) && (!is.logical(appearance$smooth.faces) ||
      length(appearance$smooth.faces)!=n || anyNA(appearance$smooth.faces)))
    .abort("Appearance smooth face mask must match the model faces.")
  x <- appearance$belly
  if (!is.numeric(x) || length(x)!=n || any(!is.finite(x)) || any(x<0 | x>1))
    .abort("Appearance countershading weights must match the faces and lie in [0,1].")
  x <- appearance$face.rgb
  if (!is.null(x) && (!is.matrix(x) || !is.numeric(x) ||
      !identical(dim(x),c(as.integer(n),3L)) || any(!is.finite(x)) || any(x<0 | x>1)))
    .abort("Appearance face RGB colours must be a finite face-by-three matrix in [0,1].")
  x <- appearance$compass
  if (!is.numeric(x) || length(x)!=3L || any(!is.finite(x)) ||
      x[3]<=0 || x[3]>.5 || abs(x[1])+x[3]>1.35 || abs(x[2])+x[3]>1.55)
    .abort("Appearance compass must fit inside the inset viewport.")
  for (field in c("dorsal","ventral")) {
    .assert_string(appearance[[field]],paste0("appearance$",field))
    tryCatch(grDevices::col2rgb(appearance[[field]]),error=function(e)
      .abort("Appearance colours must be valid R colours."))
  }
  if (!is.null(appearance$surface)) appearance$surface <- .validateOverlaySurface(appearance$surface,n)
  appearance
}

.overlayFaceColours <- function(model, intensity, body.col = "#ff453a", use.appearance = TRUE) {
  a <- model$appearance
  if (!is.null(a) && use.appearance) {
    dorsal <- grDevices::col2rgb(a$dorsal)[,1]/255
    ventral <- grDevices::col2rgb(a$ventral)[,1]/255
    rgb <- if (is.null(a$face.rgb)) outer(1-a$belly,dorsal) + outer(a$belly,ventral) else a$face.rgb
  } else rgb <- matrix(grDevices::col2rgb(body.col)[,1]/255,length(intensity),3L,byrow=TRUE)
  rgb <- rgb*intensity
  grDevices::rgb(rgb[,1],rgb[,2],rgb[,3])
}

.overlayTightMotionBound <- function(model, max.angle) {
  # Exact maximum distance over each vertex's allowed rotation interval. Unlike
  # sampling poses, this bound cannot miss an extreme between sampled phases.
  rig <- model$rig; v <- model$vertices
  if (rig$type=="none") return(max(sqrt(rowSums(v^2))))
  p <- matrix(rig$pivot,nrow(v),3L,byrow=TRUE)
  if (rig$type=="flippers") p[,2] <- abs(rig$pivot[2])*sign(v[,2])
  axes <- switch(rig$type,lateral=c(1L,2L),vertical=c(3L,1L),flippers=c(2L,3L))
  a<-axes[1]; b<-axes[2]; other<-setdiff(1:3,axes); d<-v-p
  A<-p[,a]*d[,a]+p[,b]*d[,b]; B<- -p[,a]*d[,b]+p[,b]*d[,a]
  theta<-max.angle*pi/180*rig$weights
  maximum<-pmax(A*cos(theta)+B*sin(theta),A*cos(theta)-B*sin(theta))
  inside<-abs(atan2(B,A))<=theta
  maximum[inside]<-sqrt(A[inside]^2+B[inside]^2)
  sqrt(max(p[,a]^2+p[,b]^2+d[,a]^2+d[,b]^2+v[,other]^2+2*maximum))
}
