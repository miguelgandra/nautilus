# Sparse, mesh-attached markings. Barycentric anchors follow the *deformed*
# triangles exactly, without extra polygons or independent animation weights.
.compileSharkSurface <- function(spec, geometry) {
  whale <- spec$surface == "whale_shark"
  requests <- list()
  add <- function(s, angle, kind, group=0L) {
    n <- max(length(s),length(angle));s<-rep_len(s,n);angle<-rep_len(angle,n)*pi/180
    z <- stats::approx(spec$body[,1],spec$body[,5],50-s,rule=2)$y
    requests[[length(requests)+1L]] <<- list(origin=cbind(50-s,0,z),
      direction=cbind(0,cos(angle),sin(angle)),kind=rep(kind,n),group=rep(group,n))
  }
  if (whale) {
    # Deterministic, illustrative dapple rows; no RNG changes in the caller.
    for (i in seq_len(17L)) {
      a <- seq(-162,-18,length.out=7) + 3*sin(i*1.7)
      add(5+i*3.8+1.1*sin(seq_along(a)*2+i),a,"spot")
    }
  }
  add(if (whale) 3.3 else 7.9,c(-8,188),"eye")
  for (side in c(1,-1)) for (i in seq_len(5L)) {
    a <- seq(-35,38,length.out=9)
    add((if (whale) 14.5+i*1.15 else 16.2+i*1.18)+.35*(a/40)^2,
      if (side==1) a else 180-a,"line",i+if(side==1) 0L else 5L)
  }
  if (whale) {
    y <- seq(-4.2,4.2,length.out=25)
    requests[[length(requests)+1L]] <- list(origin=cbind(49.90,y,.22+.10*(y/4.2)^2),
      direction=cbind(rep(1,length(y)),0,0),kind=rep("line",length(y)),group=rep(11L,length(y)))
  } else {
    a <- seq(18,162,length.out=25)
    add(4.1+4.5*abs(cos(a*pi/180)),a,"line",11L)
  }
  origins <- do.call(rbind,lapply(requests,`[[`,"origin"))
  directions <- do.call(rbind,lapply(requests,`[[`,"direction"))
  # Ray/triangle intersection is preparation-only, on the actual faceted trunk.
  ids <- which(geometry$face.region=="body")
  triangles <- do.call(rbind,geometry$faces[ids]);v <- geometry$vertices
  a <- v[triangles[,1],];e1<-v[triangles[,2],]-a;e2<-v[triangles[,3],]-a
  cross <- function(x,y) cbind(x[,2]*y[,3]-x[,3]*y[,2],
    x[,3]*y[,1]-x[,1]*y[,3],x[,1]*y[,2]-x[,2]*y[,1])
  anchors <- t(vapply(seq_len(nrow(origins)),function(i) {
    d <- matrix(directions[i,],nrow(a),3,byrow=TRUE)
    p <- cross(d,e2);det<-rowSums(e1*p)
    det[abs(det)<1e-12] <- NA_real_
    t <- matrix(origins[i,],nrow(a),3,byrow=TRUE)-a
    u<-rowSums(t*p)/det;q<-cross(t,e1);w<-rowSums(d*q)/det
    distance<-rowSums(e2*q)/det
    ok <- which(is.finite(distance) & distance>=0 & u>=-1e-9 & w>=-1e-9 & u+w<=1+1e-9)
    if (!length(ok)) .abort("A shark surface marking could not be attached to the trunk.")
    j<-ok[which.min(distance[ok])];b<-pmax(0,c(1-u[j]-w[j],u[j],w[j]));b<-b/sum(b)
    c(ids[j],b)
  },numeric(4)))
  list(face=as.integer(anchors[,1]),barycentric=unname(anchors[,2:4]),
    kind=unlist(lapply(requests,`[[`,"kind")),group=unlist(lapply(requests,`[[`,"group")))
}

.validateOverlaySurface <- function(surface, nfaces) {
  if (is.null(surface)) return(NULL)
  if (!is.list(surface)) .abort("Surface markings must be a list.")
  f <- surface$face;b<-surface$barycentric;n<-length(f)
  if (!is.numeric(f) || !n || any(!is.finite(f)) || any(f!=as.integer(f) | f<1 | f>nfaces))
    .abort("Surface marking face indices must identify model faces.")
  if (!is.matrix(b) || !is.numeric(b) || !identical(dim(b),c(n,3L)) ||
      any(!is.finite(b)) || any(b<0 | b>1) || any(abs(rowSums(b)-1)>1e-10))
    .abort("Surface marking barycentric weights must sum to one within each face.")
  if (!is.character(surface$kind) || length(surface$kind)!=n ||
      anyNA(surface$kind) || any(!surface$kind %in% c("spot","eye","line")))
    .abort("Surface marking kinds must be spot, eye or line.")
  g <- surface$group
  if (!is.numeric(g) || length(g)!=n || any(!is.finite(g)) || any(g<0 | g!=as.integer(g)))
    .abort("Surface marking groups must be non-negative integers.")
  colours <- surface$line.colour
  if (!is.null(colours)) {
    if (!is.character(colours) || length(colours)!=n || anyNA(colours))
      .abort("Surface line colours must contain one valid R colour per anchor.")
    tryCatch(grDevices::col2rgb(colours),error=function(e)
      .abort("Surface line colours must contain one valid R colour per anchor."))
  }
  surface
}

.overlaySurfacePoints <- function(model, vertices) {
  s <- model$appearance$surface
  triangles <- if (is.null(model$triangles)) do.call(rbind,model$faces) else model$triangles
  f <- triangles[s$face,,drop=FALSE]
  vertices[f[,1],,drop=FALSE]*s$barycentric[,1] +
    vertices[f[,2],,drop=FALSE]*s$barycentric[,2] +
    vertices[f[,3],,drop=FALSE]*s$barycentric[,3]
}

.overlaySurfaceProjection <- function(model, vertices, rotation, camera, projection) {
  s <- model$appearance$surface
  project <- function(v) v %*% t(rotation) %*% t(camera$view.rotation) %*%
    cbind(camera$right,camera$up,camera$depth)*model$display.scale
  p <- project(.overlaySurfacePoints(model,vertices))
  visible <- s$face %in% projection$order
  candidates <- which(visible)
  # Depth-test sparse marks against all painted triangles. This catches far-side
  # eyes, spots and gills and marks covered by a fin, including when rolling.
  if (length(candidates) && length(projection$order)) {
    triangles <- if (is.null(model$triangles)) do.call(rbind,model$faces) else model$triangles
    v <- project(vertices);f<-triangles[projection$order,,drop=FALSE]
    a<-v[f[,1],,drop=FALSE];e<-v[f[,2],,drop=FALSE]-a;g<-v[f[,3],,drop=FALSE]-a
    det<-e[,1]*g[,2]-e[,2]*g[,1];det[abs(det)<1e-12]<-NA_real_
    dx<-outer(p[candidates,1],a[,1],`-`);dy<-outer(p[candidates,2],a[,2],`-`)
    u<-sweep(sweep(dx,2,g[,2],`*`)-sweep(dy,2,g[,1],`*`),2,det,`/`)
    w<-sweep(sweep(dy,2,e[,1],`*`)-sweep(dx,2,e[,2],`*`),2,det,`/`)
    depth<-sweep(sweep(u,2,e[,3],`*`)+sweep(w,2,g[,3],`*`),2,a[,3],`+`)
    covered<-u>=-1e-8 & w>=-1e-8 & u+w<=1+1e-8 &
      depth < p[candidates,3]-1e-8
    covered[is.na(covered)]<-FALSE
    visible[candidates]<-rowSums(covered)==0
  }
  list(points=p[,1:2,drop=FALSE],visible=visible)
}

.drawOverlaySurface <- function(model, vertices, rotation, camera, projection) {
  s <- model$appearance$surface
  if (is.null(s)) return(invisible(NULL))
  projected <- .overlaySurfaceProjection(model,vertices,rotation,camera,projection)
  p<-projected$points;visible<-projected$visible
  spots<-visible & s$kind=="spot"
  if (any(spots)) graphics::points(p[spots,,drop=FALSE],pch=16,cex=.27*model$display.scale,
    col=grDevices::adjustcolor("#d8e4dc",alpha.f=.65))
  eyes<-visible & s$kind=="eye"
  if (any(eyes)) graphics::points(p[eyes,,drop=FALSE],pch=21,cex=model$appearance$eye.cex,
    bg="#101820",col="#92a6ab",lwd=.6)
  next.i<-seq_len(nrow(p)-1L);j<-next.i+1L
  lines<-visible[next.i] & visible[j] & s$kind[next.i]=="line" &
    s$kind[j]=="line" & s$group[next.i]==s$group[j]
  if (any(lines)) graphics::segments(p[next.i[lines],1],p[next.i[lines],2],
    p[j[lines],1],p[j[lines],2],col=grDevices::adjustcolor(if(is.null(s$line.colour)) "#1e3540" else
      s$line.colour[next.i[lines]],.75),lwd=.8)
  invisible(NULL)
}

# One-time ray intersections attach markings to actual triangles, including
# moving wing roots. No ray casting is needed during frame rendering.
.attachOverlaySurfaceRays <- function(geometry, requests) {
  f <- do.call(rbind,geometry$faces);v <- geometry$vertices
  cross <- function(x,y) cbind(x[,2]*y[,3]-x[,3]*y[,2],
    x[,3]*y[,1]-x[,1]*y[,3],x[,1]*y[,2]-x[,2]*y[,1])
  attach <- function(request) {
    ids <- which(geometry$face.region %in% request$regions)
    a <- v[f[ids,1],];e <- v[f[ids,2],]-a;g <- v[f[ids,3],]-a
    t(vapply(seq_len(nrow(request$origin)),function(i) {
      d <- matrix(request$direction[i,],nrow(a),3,byrow=TRUE)
      p <- cross(d,g);det <- rowSums(e*p);det[abs(det)<1e-12] <- NA_real_
      t <- matrix(request$origin[i,],nrow(a),3,byrow=TRUE)-a
      u <- rowSums(t*p)/det;q <- cross(t,e);w <- rowSums(d*q)/det
      distance <- rowSums(g*q)/det
      ok <- which(is.finite(distance) & distance>=0 & u>=-1e-9 & w>=-1e-9 & u+w<=1+1e-9)
      if(!length(ok)) .abort("A surface marking could not be attached to the geometry (group {request$group[i]}, point {i}).")
      k <- ok[which.min(distance[ok])]
      b <- pmax(0,c(1-u[k]-w[k],u[k],w[k]));c(ids[k],b/sum(b))
    },numeric(4)))
  }
  anchors <- do.call(rbind,lapply(requests,attach))
  list(face=as.integer(anchors[,1]),barycentric=unname(anchors[,2:4]),
    kind=unlist(lapply(requests,`[[`,"kind")),group=unlist(lapply(requests,`[[`,"group")))
}
