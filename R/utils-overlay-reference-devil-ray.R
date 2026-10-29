# Dorsal tracing in nominal TL units; depth and camber remain explicit inferences.
.referenceDevilRayControls <- function() list(
  body=matrix(c(
    5.776892153, 2.484684797, 1.552927998, 1.242342398,
    6.429121912, 5.590540793, 3.261148796, 1.708220798,
    9.224392309, 7.267703031, 5.435247993, 2.080923517,
    12.33024831, 7.547230071, 5.901126393, 2.143040637,
    20.0948883, 6.957117432, 4.969369594, 1.801396478,
    29.10187068, 6.522297592, 3.727027195, 1.521869438,
    37.17709628, 6.211711992, 2.329391997, 1.211283839,
    43.69939387, 5.745833593, 1.304459518, 0.8696396789,
    47.89229946, 2.484684797, 0.6211711992, 0.4969369594
  ),ncol=4,byrow=TRUE),
  wing=matrix(c(
    6.12226334, 37.0485805, 4.126506935, 0, 3.2,
    8.111998925, 37.56104674, 8.56788101, 0.15, 2.5,
    10.71396084, 35.4956525, 10.66433381, 0.35, 1.95,
    14.54037543, 33.46131682, 13.02478436, 0.55, 1.5,
    19.13207294, 31.10086626, 14.88829796, 0.75, 1.15,
    23.72377044, 28.22794947, 16.08405252, 0.95, 0.8,
    28.31546795, 24.87362499, 16.2548746, 1.1, 0.55,
    32.14188253, 21.44165411, 15.68029124, 1.25, 0.32,
    35.2030142, 18.28921028, 14.51559524, 1.35, 0.18,
    36.96316491, 16.13064036, 13.52172132, 1.4, 0.11,
    37.88150442, 14.51559524, 13.1490186, 1.43, 0.06,
    38.14935344, 13.97207044, 13.25772356, 1.45, 0.025
  ),ncol=5,byrow=TRUE),
  cephalic=matrix(c(
    0, 4.348198395, 0.07764639991, 0, 0.1,
    1.615045118, 4.588902234, 0.5978772793, 0, 0.65,
    3.944437115, 4.845135354, 0.9938739188, 0, 1.2,
    5.342072314, 4.487961915, 1.521869438, 0, 1.6,
    8.13734271, 6.646531832, 0.6832883192, 0, 1.25
  ),ncol=5,byrow=TRUE),
  tail.root=47.8922994622,
  disc.width=76.5282917476,
  tail.length=52.1077005378)

.referenceDevilRaySpec <- function() {
  c <- .referenceDevilRayControls()
  body <- .anatomyBody(c$body[,1],c$body[,2],c$body[,3],c$body[,4])
  wing <- .anatomyFoil("wing",c$wing,role="flipper",sections=6L)
  wing$pivot <- c(50-13, c$wing[1,1],0)
  wing$allocation <- "traced leading and concave trailing edges; thick buried root; tapered tip"
  # The root section narrows towards the tail, remaining embedded in the trunk.
  mesh <- .speciesFoil(wing)
  wing$section.vertices <- mesh$vertices[seq_len(nrow(c$wing)*6L),]
  root <- 1:6
  chord <- (wing$section.vertices[root,1]-wing$trailing[1,1]) /
    (wing$leading[1,1]-wing$trailing[1,1])
  wing$section.vertices[root,2] <- .6+chord*(c$wing[1,1]-.6)
  wing$leading <- wing$section.vertices[seq(1,nrow(wing$section.vertices),6),,drop=FALSE]
  wing$trailing <- wing$section.vertices[seq(4,nrow(wing$section.vertices),6),,drop=FALSE]
  h <- c$cephalic[nrow(c$cephalic):1,,drop=FALSE]
  horn <- list(id="cephalic_right",kind="loft",axis="x",part="body",role="rigid",joint.x=NA_real_,
    sections=6L,centreline=h[,2],pivot=c(50-h[1,1],h[1,2],0),
    allocation="traced narrow cephalic horn and broad head attachment",
    stations=.speciesBody(50-h[,1],h[,3],h[,5],z=h[,4]))
  left<-horn;left$id<-"cephalic_left";left$centreline<- -left$centreline;left$pivot[2]<- -left$pivot[2]
  root <- c$tail.root
  tail <- list(id="tail",kind="loft",part="body",role="rigid",joint.x=NA_real_,sections=6L,
    pivot=c(50-root,0,.2),allocation="traced visible tail length; neutral straightening and thin taper",
    stations=.speciesBody(50-c(100,90,76,63,53,root),c(.012,.04,.075,.14,.24,.45),
      c(.012,.04,.075,.14,.24,.45),z=.2))
  dorsal <- .anatomyFoil("dorsal",rbind(c(-.35,6.9,2.1,0,.52),
    c(-2.5,6.0,2.4,0,.30),c(-3.7,3.2,3.0,0,.025)),plane="xz",sections=4L)
  pelvic <- .anatomyFoil("pelvic",rbind(c(.55,3.9,-.15,.4,.60),
    c(1.7,3.6,-.05,.6,.32),c(2.9,1.9,.80,.65,.035)),sections=4L)
  spec <- .anatomySpec("Sicklefin devil ray","Mobula tarapacana",2L,body,
    c(.anatomyPair(wing),list(horn,left,tail,dorsal),.anatomyPair(pelvic)),
    c("paired traced dorsal disc outline and concave wing trailing margins",
      "narrow cephalic horns; prominent projecting head; separately pivoted fins",
      "visible tail length retained but straightened; depth and camber inferred"),
    character(),type="flippers",sections=8L,pivot=wing$pivot,tip=max(c$wing[,1]),
    landmarks=list(TL=100,disc.width=c$disc.width,disc.length=41.52,
      body.depth=max(c$body[,3]+c$body[,4]),tail.length=c$tail.length,
      basis="supplied dorsal drawing; symmetric disc trace; reconstructed TL includes tail arc; depths inferred"))
  spec$revision <- "devil-ray-surface-v10"
  # Shallow concave front edge between the traced cephalic horns; one mesh for
  # every view. Only the foremost body ring changes its fore-aft coordinate.
  spec$body.transform <- function(p) {
    front <- abs(p[,1]-max(body[,1])) < 1e-8
    p[front,1] <- p[front,1]+.27*(p[front,2]/body[nrow(body),2])^2
    p
  }
  spec$appearance <- list(smooth=TRUE,cull=TRUE,fit="legacy",eye.cex=.75,
    compass=c(1.12,1.10,.18),dorsal="#696c4a",ventral="#e5e5d3")
  spec$surface <- "devil_ray"
  spec$review <- list(status="approved",checkpoint="v10",date="2026-10-27",
    geometry=list(status="approved",checkpoint="v9",date="2026-10-27"),
    measured.views="dorsal",inferred.views=c("lateral","anterior"),
    surface.basis="dorsal palette guided by drawing; underside and marking positions schematic")
  spec$anatomy <- "dorsal-reference fit; bilateral average; neutral tail straightening; depth and camber inferred"
  spec
}
