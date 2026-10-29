# Render the real inset drawer for a named list of models on one shared clock.
# Supply synchronized telemetry with the same columns used by renderOverlayVideo().
previewOverlayModelAnimation <- function(output, data, models, fps = 20,
                                           duration = 12, start = data$datetime[1]) {
  stopifnot(!file.exists(output), dir.exists(dirname(output)),
    length(models) >= 1L, length(models) <= 11L, !is.null(names(models)),
    tolower(tools::file_ext(output)) == "mp4", is.finite(fps), fps > 0,
    is.finite(duration), duration > 0)
  times <- start + (seq_len(ceiling(duration*fps))-1)/fps
  control <- nautilus::overlayMotionControl(amplitude.reference = .2)
  prepared <- lapply(models, nautilus:::.prepareOverlayModel, max.angle = control$max.angle)
  motions <- lapply(prepared, function(m) nautilus:::.resolveOverlayMotion(control,data,m))
  states <- lapply(motions, function(m) nautilus:::.prepareOverlayFrameState(data,times,m))
  theme <- nautilus:::.overlayTheme()
  audit <- lapply(seq_along(prepared),function(j) {
    m <- prepared[[j]]; state <- states[[j]]; previous <- NULL
    steps <- radius <- anchor.error <- numeric(length(times))
    for(i in seq_along(times)) {
      fd <- nautilus:::.overlayFrameData(state,i,data)
      v <- nautilus:::.deformOverlayModel(m,fd$.motion.phase,fd$.motion.strength,
        control$max.angle,control$phase.lag)
      anchor.error[i] <- max(abs(v[m$rig$weights==0,,drop=FALSE] -
        m$vertices[m$rig$weights==0,,drop=FALSE]),0)
      stopifnot(all(is.finite(v)), anchor.error[i] <= 1e-12)
      radius[i] <- max(sqrt(rowSums(v^2)))*m$display.scale
      if(!is.null(previous)) steps[i] <- max(sqrt(rowSums((v-previous)^2)))
      previous <- v
    }
    valid <- state$frames$.motion.status == "estimated"
    continuous <- valid & c(FALSE,head(valid,-1))
    data.frame(model=names(models)[j],frames=length(times),
      maximum.display.radius=max(radius),
      maximum.rigid.anchor.error=max(anchor.error),
      max.step.valid.motion=max(steps[continuous]),
      estimated.frames=sum(valid),gliding.frames=sum(state$frames$.motion.status=="gliding"),
      unavailable.frames=sum(state$frames$.motion.status=="unavailable"),
      min.samples.per.cycle=fps/max(state$frames$.motion.frequency))
  })
  audit <- do.call(rbind,audit)
  stopifnot(all(audit$maximum.display.radius <= 1.08+1e-10))
  timing <- system.time(av::av_capture_graphics({
    for(i in seq_along(times)) {
      graphics::layout(matrix(seq_len(12),nrow=4,byrow=TRUE))
      graphics::par(mar=c(.8,1,1.8,1),oma=c(0,0,0,0),bg=theme$canvas)
      for(j in seq_along(prepared)) {
        fd <- nautilus:::.overlayFrameData(states[[j]],i,data)
        label <- prepared[[j]]$metadata$label
        if(is.null(label)) label <- names(prepared)[j]
        nautilus:::.drawPreparedAttitudeModel(fd,theme,prepared[[j]],motions[[j]],label=label)
      }
      for(j in seq_len(12-length(prepared))) {
        graphics::plot.new()
        if(j==1L) {
          fd <- nautilus:::.overlayFrameData(states[[1]],i,data)
          graphics::text(.5,.75,"Overlay animation",cex=1.5,col=theme$text,font=2)
          graphics::text(.5,.55,sprintf("%.2f / %.0f seconds\n%d frames/s\n%s",
            as.numeric(times[i]-start,units="secs"),duration,fps,fd$.motion.status),col=theme$text)
          graphics::text(.5,.27,"Shared telemetry and phase\nOriginal production camera\nGliding and missing-estimate transitions",
            cex=.85,col=theme$muted)
        }
      }
    }
  },output=output,width=1600,height=1280,framerate=fps,verbose=FALSE,pointsize=13))[['elapsed']]
  info <- av::av_video_info(output)
  stopifnot(abs(info$duration-duration) < 1/fps+.01,
    info$video$width==1600,info$video$height==1280)
  utils::write.csv(audit,sub("\\.mp4$","-audit.csv",output),row.names=FALSE)
  invisible(list(video=output,audit=audit,seconds=timing))
}
