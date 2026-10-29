# Draw dashboard layouts and attitude models from geometry and frame state prepared once per render.
.drawPreparedOverlayDashboard <- function(fd, clip, current.time, depth.window, activity.window,
                                           theme, caption, candidates, plan, context, model, motion) {
  theme <- .completeOverlayTheme(theme)
  if (plan$dashboard == "validation") {
    graphics::layout(matrix(1:4, ncol = 1L), heights = c(2.8, 1.3, 1.2, .5))
    op <- graphics::par(bg = theme$canvas, mar = c(1, 1, 2.2, 1), oma = c(0, 0, .3, 0))
    on.exit(graphics::par(op), add = TRUE)
    .drawPreparedAttitudeModel(fd, theme, model, NULL, show.heading = FALSE)
    graphics::par(mar = c(1.6, 3.2, 2, 1))
    .drawTriTrace(clip, current.time, activity.window, c("gx", "gy", "gz"),
                  c("roll-rate", "pitch-rate", "yaw-rate"), "Gyroscope (body)", theme, fd)
    .drawSeriesPanel(clip, current.time, depth.window, "depth", "Depth", "m", fd$depth,
                     theme, invert = TRUE, fill = "#1f4e8c")
    graphics::par(mar = c(0, 0, 0, 0))
    .drawCaption(fd$datetime, caption, theme)
    return(invisible(NULL))
  }
  if (plan$dashboard == "validation-compare") {
    n <- nrow(candidates)
    pal <- .candidatePalette(n)
    graphics::layout(rbind(rep(1L, n), 1L + seq_len(n), rep(n + 2L, n), rep(n + 3L, n)),
                      heights = c(.55, 2.7, 1, .32))
    op <- graphics::par(bg = theme$canvas, oma = c(0, 0, .3, 0))
    on.exit(graphics::par(op), add = TRUE)
    graphics::par(mar = c(0, 1, 0, 1)); .drawHeader(caption, theme)
    graphics::par(mar = c(1, 1, 2.4, 1))
    for (i in seq_len(n))
      .drawPreparedAttitudeModel(fd, theme, model, NULL, label = candidates$label[i],
                                  body.col = pal[i], show.heading = FALSE,
                                  pitch.col = candidates$pitch[i], roll.col = candidates$roll[i])
    graphics::par(mar = c(1.6, 3.2, 2, 1))
    .drawSeriesPanel(clip, current.time, depth.window, "depth", "Depth", "m", fd$depth,
                     theme, invert = TRUE, fill = "#1f4e8c")
    graphics::par(mar = c(0, 0, 0, 0)); .drawTimestamp(fd$datetime, theme)
    return(invisible(NULL))
  }

  draw.module <- function(metric)
    .drawMetricModule(metric, fd, clip, current.time, theme, plan, context, model, motion)
  metrics <- plan$metrics
  if (plan$dashboard == "ribbon") {
    n <- length(metrics)
    graphics::layout(rbind(seq_len(n), rep(n + 1L, n)), heights = c(1, .22))
    op <- graphics::par(bg = theme$canvas, oma = c(0, 0, 0, 0))
    on.exit(graphics::par(op), add = TRUE)
    for (metric in metrics) draw.module(metric)
    graphics::par(mar = c(0, 0, 0, 0)); .drawTimestamp(fd$datetime, theme)
    return(invisible(NULL))
  }
  groups <- .presentationGroups(metrics, plan$dashboard)
  mats <- list(); heights <- numeric(); next.id <- 1L
  for (i in seq_along(groups$metrics)) {
    group <- groups$metrics[[i]]
    ids <- next.id + seq_along(group) - 1L
    next.id <- next.id + length(group)
    mats[[i]] <- rep(ids, each = 6L / length(ids))
    heights[i] <- if (groups$tiles[i]) .72 else max(vapply(group, .moduleHeight, numeric(1)))
  }
  mats[[length(mats) + 1L]] <- rep(next.id, 6L)
  heights <- c(heights, .22)
  gap <- isTRUE(theme$overlay) && plan$dashboard == "general"
  if (gap) {
    mats[[length(mats) + 1L]] <- rep(next.id + 1L, 6L)
    heights <- c(heights, .34)
  }
  graphics::layout(do.call(rbind, mats), heights = heights)
  op <- graphics::par(bg = theme$canvas, oma = c(0, 0, .3, 0))
  on.exit(graphics::par(op), add = TRUE)
  for (i in seq_along(groups$metrics)) for (metric in groups$metrics[[i]]) {
    if (groups$tiles[i]) .drawMetricTile(metric, fd, theme) else draw.module(metric)
  }
  graphics::par(mar = c(0, 3.1, 0, .8)); .drawTimestamp(fd$datetime, theme)
  if (gap) {
    graphics::par(mar = c(0, 0, 0, 0)); .drawTransparentSpacer()
  }
  invisible(NULL)
}

.overlayAttitudeRotation <- function(pitch, roll) {
  th <- (if (is.finite(pitch)) pitch else 0) * pi / 180
  ro <- (if (is.finite(roll)) roll else 0) * pi / 180
  rx <- matrix(c(1, 0, 0, 0, cos(ro), -sin(ro), 0, sin(ro), cos(ro)), 3L, byrow = TRUE)
  ry <- matrix(c(cos(th), 0, sin(th), 0, 1, 0, -sin(th), 0, cos(th)), 3L, byrow = TRUE)
  ry %*% rx
}

.overlayAttitudeCamera <- function() {
  # Restore the original rear camera (no azimuth/yaw). Zero the pitch reference
  # with a display-only Ry(-elevation), before projection: up * Ry(-a) = (0, 0, -1).
  # Otherwise the image shows sin(pitch + a), masking descents near -24 degrees.
  # This calibration leaves the physical body -> world rotation and sensor values intact.
  elevation <- 24 * pi / 180
  co <- cos(elevation); si <- sin(elevation)
  list(right = c(0, 1, 0), up = c(si, 0, -co), depth = c(co, 0, si),
        view.rotation = matrix(c(co, 0, -si, 0, 1, 0, si, 0, co), 3L, byrow = TRUE))
}

.projectPreparedOverlayModel <- function(model, vertices, rotation, camera = .overlayAttitudeCamera()) {
  world <- vertices %*% t(rotation)
  light <- c(.25, -.30, -.92); light <- light / sqrt(sum(light^2))
  view <- world %*% t(camera$view.rotation)
  projected <- view %*% cbind(camera$right, camera$up, camera$depth)
  # Retain mean() exactly: changing summation can reorder coplanar faces on ties.
  face.depth <- vapply(model$faces, function(f) mean(projected[f, 3L]), numeric(1))
  if (!is.null(model$triangles)) {
    f <- model$triangles
    e1 <- world[f[, 2L], , drop = FALSE] - world[f[, 1L], , drop = FALSE]
    e2 <- world[f[, 3L], , drop = FALSE] - world[f[, 1L], , drop = FALSE]
    normals <- cbind(e1[, 2L]*e2[, 3L] - e1[, 3L]*e2[, 2L],
                     e1[, 3L]*e2[, 1L] - e1[, 1L]*e2[, 3L],
                     e1[, 1L]*e2[, 2L] - e1[, 2L]*e2[, 1L])
    lengths <- sqrt(rowSums(normals^2))
    valid <- is.finite(lengths) & lengths > 0
    normals[valid, ] <- normals[valid, , drop = FALSE] / lengths[valid]
    normals[!valid, ] <- rep(c(0, 0, 1), each = sum(!valid))
    intensity <- .40 + .60 * abs(as.vector(normals %*% light))
  } else {
    intensity <- vapply(model$faces, function(f) {
      .40 + .60 * abs(sum(.faceNormal(t(world[f, , drop = FALSE])) * light))
    }, numeric(1))
    if (!is.null(model$appearance))
      normals <- t(vapply(model$faces,function(f) .faceNormal(t(world[f,,drop=FALSE])),numeric(3)))
  }
  a <- model$appearance
  visible <- rep(TRUE,length(model$faces))
  if (!is.null(a)) {
    if (a$smooth) {
      f <- do.call(rbind,model$faces)
      sums <- rowsum(normals[rep(seq_len(nrow(f)),3),,drop=FALSE],as.vector(f),reorder=FALSE)
      vertex.normals <- matrix(0,nrow(world),3L)
      vertex.normals[as.integer(rownames(sums)),] <- sums
      smooth <- vertex.normals[f[,1],,drop=FALSE]+vertex.normals[f[,2],,drop=FALSE]+vertex.normals[f[,3],,drop=FALSE]
      lengths <- sqrt(rowSums(smooth^2))
      smooth.faces <- if (is.null(a$smooth.faces)) a$body.faces else a$smooth.faces
      ok <- smooth.faces & is.finite(lengths) & lengths>0
      smooth[ok,] <- smooth[ok,,drop=FALSE]/lengths[ok]
      intensity[ok] <- .65+.35*abs(as.vector(smooth[ok,,drop=FALSE] %*% light))
    }
    visible <- !a$hidden.faces
    if (a$cull) visible <- visible &
      as.vector(normals %*% t(camera$view.rotation) %*% camera$depth) < -1e-10
  }
  range.depth <- diff(range(face.depth))
  dimming <- if (range.depth > 0) (face.depth - min(face.depth)) / range.depth else face.depth * 0
  order <- order(face.depth,decreasing=TRUE)
  list(points = projected[, 1:2, drop = FALSE] * model$display.scale,
        order = order[visible[order]], intensity = intensity * (.72 + .28 * (1 - dimming)))
}

.drawPreparedAttitudeModel <- function(fd, theme, model, motion, label = NULL,
                                        body.col = "#ff453a", show.heading = TRUE,
                                        pitch.col = "pitch", roll.col = "roll") {
  pitch <- .scalarValue(fd, pitch.col); roll <- .scalarValue(fd, roll.col)
  heading <- .scalarValue(fd, "heading")
  valid.attitude <- is.finite(pitch) && is.finite(roll)
  graphics::plot(0, 0, type = "n", xlim = c(-1.35, 1.35), ylim = c(-1.55, 1.55),
                   axes = FALSE, ann = FALSE, asp = 1)
  .drawPanelBackground(theme)
  graphics::symbols(0, 0, circles = 1.05, inches = FALSE, add = TRUE,
                      bg = grDevices::adjustcolor("black", .58), fg = theme$border)
  graphics::segments(-.94, 0, .94, 0, col = grDevices::adjustcolor("#64d2ff", .35), lwd = 1)
  rotation <- .overlayAttitudeRotation(pitch, roll)
  vertices <- if (is.null(motion) || !valid.attitude) model$vertices else
    .deformOverlayModel(model, fd$.motion.phase, fd$.motion.strength, motion$max.angle, motion$phase.lag)
  camera <- .overlayAttitudeCamera()
  projection <- .projectPreparedOverlayModel(model, vertices, rotation, camera)
  use.appearance <- missing(body.col) && valid.attitude
  if (use.appearance && !is.null(model$appearance)) body.col <- model$appearance$dorsal
  if (!valid.attitude) body.col <- theme$muted
  colours <- .overlayFaceColours(model,projection$intensity,body.col,use.appearance)
  borders <- grDevices::adjustcolor(colours, alpha.f = .5)
  borders[model$part == "fin"] <- grDevices::adjustcolor("grey10", .85)
  if (!is.null(model$appearance)) borders <- colours
  if (!is.null(model$triangles)) {
    # NA separates polygons. One graphics call preserves far-to-near painter order
    # and supplies the same fill/border for each triangle without an R call per face.
    indices <- as.vector(t(cbind(model$triangles[projection$order, , drop = FALSE], NA_integer_)))
    graphics::polygon(projection$points[indices, 1L], projection$points[indices, 2L],
      col = colours[projection$order], border = borders[projection$order], lwd = .6)
  } else {
    for (i in projection$order) {
      p <- projection$points[model$faces[[i]], , drop = FALSE]
      graphics::polygon(p[, 1], p[, 2], col = colours[i], border = borders[i], lwd = .6)
    }
  }
  if (use.appearance && !is.null(model$appearance$surface))
    .drawOverlaySurface(model,vertices,rotation,camera,projection)
  dorsal.visible <- sum(camera$depth * (camera$view.rotation %*% rotation %*% c(0, 0, -1))) < 0
  project.detail <- function(p, weights = NULL) {
    if (!is.null(weights) && !is.null(motion) && valid.attitude) {
      rig <- model$rig; rig$weights <- weights
      p <- .deformOverlayModel(list(vertices=p,rig=rig),fd$.motion.phase,
        fd$.motion.strength,motion$max.angle,motion$phase.lag)
    }
    view <- p %*% t(rotation) %*% t(camera$view.rotation)
    view %*% cbind(camera$right, camera$up) * model$display.scale
  }
  if (dorsal.visible) {
    for (i in seq_along(model$details)) {
      weights <- if (is.null(model$detail.weights)) NULL else model$detail.weights[[i]]
      p <- project.detail(model$details[[i]],weights)
      graphics::lines(p[, 1], p[, 2], col = .shadeColor(body.col, .42), lwd = 1.1)
    }
    if (!is.null(model$eyes)) {
      p <- project.detail(model$eyes)
      eye.cex <- if (is.null(model$appearance)) .62 else model$appearance$eye.cex
      graphics::points(p[, 1], p[, 2], pch = 21, cex = eye.cex, bg = "#11161a",
                         col = .shadeColor(body.col, .48), lwd = .7)
    }
  }
  if (isTRUE(show.heading)) {
    cx <- .91; cy <- .93; radius <- .25
    if (!is.null(model$appearance)) {
      cx<-model$appearance$compass[1];cy<-model$appearance$compass[2];radius<-model$appearance$compass[3]
    }
    graphics::symbols(cx, cy, circles = radius, inches = FALSE, add = TRUE,
                       bg = grDevices::adjustcolor("black", .72), fg = theme$muted)
    graphics::text(cx, cy + radius + .08, "N", cex = .55, font = 2, col = theme$text)
    if (is.finite(heading)) {
      rad <- (90 - heading)*pi/180
      graphics::arrows(cx, cy, cx + cos(rad)*radius*.72, cy + sin(rad)*radius*.72,
                         length = .08, lwd = 2, col = "#ff453a")
    }
  }
  if (!is.null(label)) graphics::text(0, 1.38, label, cex = 1.35, font = 2, col = theme$text, xpd = NA)
  status <- if (!valid.attitude) "Orientation unavailable" else if (!is.null(motion)) {
    switch(fd$.motion.status,
           estimated = NULL, gliding = "Gliding", "Motion unavailable")
  } else NULL
  animated <- !is.null(motion)
  value.cex <- if (animated) .98 else 1.05
  if (!is.null(status)) graphics::text(0, -1.12, status,
                                        cex = if (animated) .60 else .65, col = theme$muted)
  values <- if (show.heading)
    sprintf("H %s\u00b0   P %s\u00b0   R %s\u00b0", .formatOverlayValue(heading, 0),
             .formatOverlayValue(pitch, 0), .formatOverlayValue(roll, 0)) else
    sprintf("Roll %s\u00b0  Pitch %s\u00b0", .formatOverlayValue(roll, 0, signed = TRUE),
             .formatOverlayValue(pitch, 0, signed = TRUE))
  graphics::text(0, if (animated) -1.29 else -1.38, values,
                   cex = value.cex, col = theme$value, xpd = NA, font = 2)
  if (animated) {
    frequency <- .scalarValue(fd, ".motion.frequency")
    supported <- identical(fd$.motion.status, "estimated") && is.finite(frequency) && frequency > 0
    tbf <- if (supported) sprintf("TBF: %.2f Hz", frequency) else "TBF: NA"
    graphics::text(0, -1.49, tbf, cex = value.cex, col = theme$value, font = 2, xpd = NA)
  }
  invisible(NULL)
}
