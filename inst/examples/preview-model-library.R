# One prepared mesh per species, identical orthographic limits and pixel scale.
# Load nautilus, source this script, then call previewModelLibrary().
previewModelLibrary <- function(output.dir, overwrite = FALSE,
                                            models = nautilus::overlaySpeciesCatalog()$model,
                                            prefix = "species-models") {
  stopifnot(dir.exists(output.dir), is.logical(overwrite), length(overwrite) == 1L)
  stopifnot(is.character(prefix),length(prefix)==1L,grepl("^[A-Za-z0-9_-]+$",prefix))
  prepared <- lapply(models, function(id)
    nautilus:::.prepareOverlayModel(nautilus::overlaySpeciesModel(id), 20))
  names(prepared) <- models
  paths <- file.path(output.dir, paste0(prefix,"-", models, ".png"))
  if (!overwrite && any(file.exists(paths))) stop("Choose a new folder or set overwrite = TRUE.")
  cameras <- list(
    Dorsal = list(right = c(-1,0,0), up = c(0,1,0), depth = c(0,0,1), view.rotation = diag(3)),
    Lateral = list(right = c(-1,0,0), up = c(0,0,-1), depth = c(0,-1,0), view.rotation = diag(3)),
    Anterior = list(right = c(0,1,0), up = c(0,0,-1), depth = c(-1,0,0), view.rotation = diag(3)))
  reports <- list()
  for (index in seq_along(prepared)) {
    model <- prepared[[index]]
    # Identical square viewports, limits and scale. Only camera matrices differ.
    png(paths[index], width = 2000, height = 600, res = 125, bg = "#101820")
    tryCatch({
      layout(matrix(1:4, 1, 4))
      par(mar = c(2.6, .5, 4.6, .5), oma = c(1.4, 0, 0, 0), bg = "#101820", fg = "#deebf3")
      for (view in names(cameras)) {
        camera <- cameras[[view]]
        projection <- nautilus:::.projectPreparedOverlayModel(model, model$vertices, diag(3), camera)
        plot(0, 0, type = "n", asp = 1, xlim = c(-1.05,1.05), ylim = c(-1.05,1.05),
          xaxs = "i", yaxs = "i", axes = FALSE, ann = FALSE)
        abline(h = 0, v = 0, col = "#22313b", lwd = .5)
        colours <- nautilus:::.overlayFaceColours(model,projection$intensity)
        for (i in projection$order) polygon(projection$points[model$faces[[i]], ],
          col = colours[i], border = colours[i], lwd = .25)
        nautilus:::.drawOverlaySurface(model,model$vertices,diag(3),camera,projection)
        project <- function(p) p %*% cbind(camera$right, camera$up)*model$display.scale
        # Geometric visibility of the same 3D features in each view.
        visible <- function(p) {
          xy <- project(p); depths <- as.numeric(p %*% camera$depth)
          vdepth <- as.numeric(model$vertices %*% camera$depth)
          vapply(seq_len(nrow(p)), function(k) {
            for (f in model$faces) {
              q <- projection$points[f, ]; e <- q[2,]-q[1,]; g <- q[3,]-q[1,]
              det <- e[1]*g[2]-e[2]*g[1]
              if (abs(det) < 1e-10) next
              d <- xy[k,]-q[1,]; a <- (d[1]*g[2]-d[2]*g[1])/det
              b <- (e[1]*d[2]-e[2]*d[1])/det
              if (min(a,b,1-a-b) >= -1e-7 && sum(c(1-a-b,a,b)*vdepth[f]) < depths[k]-.012)
                return(FALSE)
            }
            TRUE
          }, logical(1))
        }
        for (line in model$details) {
          p <- project(line); p[!visible(line), ] <- NA
          lines(p, col = if(is.null(model$appearance)) "#782622" else "#25495b", lwd = .85)
        }
        if (!is.null(model$eyes)) {
          eyes <- model$eyes[visible(model$eyes), , drop = FALSE]
          if (nrow(eyes)) points(project(eyes), pch = 21,
            cex = if(is.null(model$appearance)) .65 else model$appearance$eye.cex,
            bg = "#101820", col = "#71838e", lwd = .4)
        }
        unit <- diff(range(model$vertices[,1]))*.2*model$display.scale
        segments(-.9,-.94,-.9+unit,-.94,col="#c8d9e3",lwd=2)
        text(-.9+unit/2,-1.01,"20% axial TL",cex=.65,col="#9bb2c0")
        title(main = view, line = 1, cex.main = 1.1, col.main = "#deebf3")
      }
      # Actual production drawer at inset size; no alternative camera or shader.
      par(mar = c(5, 2.2, 5, 2.2))
      nautilus:::.drawPreparedAttitudeModel(list(pitch=-18,roll=12,heading=0),
        nautilus:::.overlayTheme(),model,NULL,label=NULL)
      mtext("Production inset",side=3,line=2.5,cex=.95,col="#deebf3")
      mtext(paste(model$metadata$label, "|", model$metadata$taxon, "|", length(model$faces), "triangles"),
        outer=TRUE,side=1,line=.2,cex=.85,col="#deebf3")
    }, finally = dev.off())
    reports[[index]] <- cbind(model = models[index], model$metadata$components)
  }
  report <- do.call(rbind,reports)
  report.path <- file.path(output.dir,paste0(prefix,"-parts.csv"))
  if (file.exists(report.path) && !overwrite) report.path <- tempfile(paste0(prefix,"-parts-"), output.dir, ".csv")
  write.csv(report,report.path,row.names=FALSE)
  invisible(list(images = stats::setNames(paths,models), parts = report.path))
}
