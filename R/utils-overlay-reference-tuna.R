# Lateral profile traced in percent axial TL; transverse shape and paired-fin opening inferred.
.referenceTunaControls <- function() list(
body=matrix(c(
    0.06301197227, 0.07183364839, 0.4410838059, 0.6301197227,
    3.402646503, 2.873345936, -2.835538752, 4.725897921,
    10.33396345, 6.129804663, -7.309388784, 8.821676118,
    18.52551985, 8.50031506, -10.9010712, 11.46817895,
    27.34719597, 9.793320731, -13.23251418, 12.53938248,
    37.42911153, 9.81726528, -13.04347826, 12.79143037,
    48.14114682, 9.098928796, -12.79143037, 11.15311909,
    59.48330183, 6.441083806, -9.388783869, 7.561436673,
    70.82545684, 3.256458727, -4.788909893, 3.780718336,
    80.27725268, 1.34089477, -2.142407057, 1.38626339,
    86.57844991, 0.9338374291, -1.449275362, 1.008191556,
    88.78386894, 1.005671078, -1.260239445, 1.38626339
  ),ncol=4,byrow=TRUE),
dorsal=matrix(c(
    23.91304348, -12.66540643, -12.45836709, 0.38,
    20.76244486, -23.88153749, -12.75598614, 0.16,
    17.61184625, -20.2268431, -12.69691241, 0.16,
    13.83112791, -16.95022054, -12.62602394, 0.16,
    9.420289855, -15.56395715, -12.52826272, 0.16,
    5.324511657, -14.68178954, -12.43189147, 0.16,
    3.119092628, -13.10649023, -12.37999926, 0.16
  ),ncol=4,byrow=TRUE),
second_dorsal=matrix(c(
    1.228733459, -12.79143037, -12.16131065, 0.42,
    -4.442344045, -19.91178324, -14.42974165, 0.3436363636,
    -10.11342155, -25.39382483, -21.48708255, 0.2672727273,
    -15.78449905, -29.42659105, -27.41020794, 0.1909090909,
    -21.45557656, -32.51417769, -31.1279143, 0.1145454545,
    -27.06364209, -35.60176434, -35.47574039, 0.03818181818
  ),ncol=4,byrow=TRUE),
anal=matrix(c(
    -1.921865154, 9.514807813, 10.33396345, 0.42,
    -7.592942659, 7.718966604, 14.5557656, 0.3436363636,
    -13.89413989, 15.62696912, 19.0926276, 0.2672727273,
    -20.19533711, 20.2268431, 21.86515438, 0.1909090909,
    -27.12665406, 22.18021424, 22.99936988, 0.1145454545,
    -34.37303088, 22.87334594, 22.93635791, 0.03818181818
  ),ncol=4,byrow=TRUE),
pectoral=matrix(c(
    8, 23.15689981, 20.76244486, -0.6301197227, 0, 0.65,
    9.5, 21.07750473, 14.46124764, -1.197227473, 0.5671077505, 0.55,
    11, 16.35160681, 8.16005041, -1.323251418, 0.3780718336, 0.35,
    12.1, 8.16005041, 1.858853182, -1.260239445, -0.3780718336, 0.18,
    12.4, -0.9766855703, -1.291745432, -1.197227473, -1.071203529, 0.025
  ),ncol=6,byrow=TRUE),
caudal_upper=matrix(c(
    0, -35.0031506, -41.30434783, 0, 0, 0.95,
    -3.150598614, -35.31821046, -41.55639572, 0, 0, 0.7,
    -7.561436673, -38.4057971, -43.32073094, 0, 0, 0.45,
    -12.60239445, -41.68241966, -45.71518589, 0, 0, 0.25,
    -17.01323251, -45.58916194, -48.42470069, 0, 0, 0.1,
    -18.90359168, -48.99180844, -50, 0, 0, 0.025
  ),ncol=6,byrow=TRUE),
caudal_lower=matrix(c(
    0, -35.0031506, -41.30434783, 0, 0, 0.95,
    3.780718336, -36.01134216, -41.05229994, 0, 0, 0.7,
    8.821676118, -38.46880907, -42.37555135, 0, 0, 0.45,
    13.8626339, -41.49338374, -44.83301827, 0, 0, 0.25,
    18.27347196, -45.58916194, -47.66855703, 0, 0, 0.1,
    19.72274732, -48.23566478, -48.99180844, 0, 0, 0.025
  ),ncol=6,byrow=TRUE),
dorsal_finlets=list(
matrix(c(
    -5.261499685, 0, -10.52930057,
    -6.647763075, 0, -11.34215501,
    -8.034026465, 0, -9.697542533
  ),ncol=3,byrow=TRUE),
matrix(c(
    -9.294265911, 0, -9.319470699,
    -10.6805293, 0, -10.39697543,
    -12.06679269, 0, -8.215010852
  ),ncol=3,byrow=TRUE),
matrix(c(
    -12.88594833, 0, -7.882797732,
    -14.14618778, 0, -8.821676118,
    -15.40642722, 0, -6.860603515
  ),ncol=3,byrow=TRUE),
matrix(c(
    -16.79269061, 0, -6.298396695,
    -17.92690611, 0, -7.561436673,
    -19.06112161, 0, -5.3784219
  ),ncol=3,byrow=TRUE),
matrix(c(
    -20.0063012, 0, -4.995099069,
    -21.07750473, 0, -6.175173283,
    -22.14870825, 0, -4.292375551
  ),ncol=3,byrow=TRUE),
matrix(c(
    -23.21991178, 0, -3.992438563,
    -24.22810334, 0, -5.229993699,
    -25.2362949, 0, -3.427851292
  ),ncol=3,byrow=TRUE),
matrix(c(
    -26.11846251, 0, -3.18084436,
    -27.00063012, 0, -4.032766226,
    -27.88279773, 0, -2.686830498
  ),ncol=3,byrow=TRUE),
matrix(c(
    -28.44990548, 0, -2.528040328,
    -29.20604915, 0, -3.087586641,
    -29.96219282, 0, -2.104599874
  ),ncol=3,byrow=TRUE),
matrix(c(
    -31.78954001, 0, -1.850031506,
    -32.41965974, 0, -2.221802142,
    -33.04977946, 0, -1.711405167
  ),ncol=3,byrow=TRUE)),
ventral_finlets=list(
matrix(c(
    -10.23944549, 0, 7.183364839,
    -11.373661, 0, 7.813484562,
    -12.5078765, 0, 6.427221172
  ),ncol=3,byrow=TRUE),
matrix(c(
    -12.75992439, 0, 6.343205209,
    -13.89413989, 0, 6.93131695,
    -15.02835539, 0, 5.587061542
  ),ncol=3,byrow=TRUE),
matrix(c(
    -15.34341525, 0, 5.482041588,
    -16.41461878, 0, 6.301197227,
    -17.48582231, 0, 4.767905902
  ),ncol=3,byrow=TRUE),
matrix(c(
    -18.11594203, 0, 4.557865995,
    -19.25015753, 0, 5.734089477,
    -20.38437303, 0, 3.801722327
  ),ncol=3,byrow=TRUE),
matrix(c(
    -20.95148078, 0, 3.622768326,
    -22.08569628, 0, 4.347826087,
    -23.21991178, 0, 3.048099139
  ),ncol=3,byrow=TRUE),
matrix(c(
    -23.91304348, 0, 2.872505776,
    -25.04725898, 0, 3.843730309,
    -26.18147448, 0, 2.297836589
  ),ncol=3,byrow=TRUE),
matrix(c(
    -27.06364209, 0, 2.074354127,
    -28.07183365, 0, 2.898550725,
    -29.0800252, 0, 1.563537072
  ),ncol=3,byrow=TRUE),
matrix(c(
    -29.52110901, 0, 1.451795841,
    -30.27725268, 0, 2.268431002,
    -31.03339635, 0, 1.214870825
  ),ncol=3,byrow=TRUE),
matrix(c(
    -31.97857593, 0, 1.15816005,
    -32.60869565, 0, 1.890359168,
    -33.23881537, 0, 1.082545684
  ),ncol=3,byrow=TRUE)))

.referenceTunaSpec <- function() {
  controls <- .referenceTunaControls();b <- controls$body;z <- (b[,3]+b[,4])/2
  body <- .anatomyBody(b[,1],b[,2],z-b[,3],b[,4]-z,z)
  median <- function(id,role="attached") {
    p <- controls[[id]]
    list(id=id,kind="foil",part="fin",role=role,joint.x=p[1,1],sections=4L,
      pivot=c(p[1,1],0,mean(p[1,2:3])),normal=c(0,1,0),thickness=p[,4],
      leading=cbind(p[,1],0,p[,2]),trailing=cbind(p[,1],0,p[,3]),
      allocation="traced axial blade sections / embedded root / long curved tip")
  }
  p <- controls$pectoral
  pectoral <- .anatomyFoil("pectoral",cbind(p[,1:3],(p[,4]+p[,5])/2,p[,6]),sections=4L)
  pectoral$leading[,3] <- p[,4];pectoral$trailing[,3] <- p[,5]
  pectoral$pivot <- (pectoral$leading[1,]+pectoral$trailing[1,])/2
  pectoral$allocation <- "profile-guided blade; inferred slight lateral opening; local transverse thickness"
  a <- (0:3)*pi/2
  pectoral$section.vertices <- do.call(rbind,lapply(seq_len(nrow(p)),function(i) {
    d <- pectoral$leading[i,]-pectoral$trailing[i,];n <- c(-d[3],0,d[1]);n <- n/sqrt(sum(n^2))
    centre <- (pectoral$leading[i,]+pectoral$trailing[i,])/2
    matrix(centre,4,3,byrow=TRUE)+outer(cos(a),d/2)+outer(sin(a),n*p[i,6])
  }))
  pelvic <- .speciesFinSpec("pelvic",rbind(c(20.3,4.5,11),c(13.8,6,19.3),c(15.6,4.5,12)),
    normal=c(0,1,0),thickness=.3,role="rigid")
  keel <- .speciesFinSpec("keel",rbind(c(-33.5,.6,.25),c(-39.5,.5,.35),c(-37.5,2.8,.3)),
    normal=c(0,0,1),thickness=.2,role="axial")
  tail <- lapply(c("caudal_upper","caudal_lower"),function(id) {
    p <- controls[[id]];fin <- .anatomyFoil(id,cbind(p[,1:3],0,p[,6]),"xz","terminal",4L)
    fin$allocation <- "lateral caudal outline / thick root / tapered stiff lobe";fin
  })
  finlets <- unlist(lapply(c("dorsal","ventral"),function(edge)
    lapply(seq_len(9),function(i) {
      outline <- controls[[paste0(edge,"_finlets")]][[i]]
      out <- .speciesFinSpec(paste0(edge,"_finlet_",i),outline,normal=c(0,1,0),
        thickness=.12-(i-1)*.009,role="attached",joint.x=mean(outline[c(1,3),1]))
      out$allocation <- "small traced finlet; finite thickness; local axial attachment";out
    })),recursive=FALSE)
  spec <- .anatomySpec("Yellowfin tuna","Thunnus albacares",2L,body,
    c(list(median("dorsal"),median("second_dorsal"),median("anal")),.anatomyPair(pectoral),
      .speciesPair(pelvic,"pelvic_right","pelvic_left"),.speciesPair(keel,"keel_right","keel_left"),tail,finlets),
    c("profile-fitted thick torpedo trunk; inferred mildly compressed transverse sections",
      "long illustrated adult second dorsal and anal sickles; separate pectoral blades",
      "nine small finlets per edge; strong lateral keels and a lunate rigid caudal blade"),
    character(),type="lateral",sections=10L,pivot=c(0,0,0),tip=-35.3,
    landmarks=list(TL=100,body.depth=max(b[,4]-b[,3]),body.width=2*max(b[,2]),
      width.depth.ratio=.76,pectoral.origin=26.84,first.dorsal.origin=26.09,
      second.dorsal.origin=48.77,caudal.span=613/1587*100,
      basis="lateral illustration trace; width and paired-fin opening inferred; oblique drawing qualitative"))
  spec$body.angles <- c(0,pi/4,pi/2,3*pi/4,pi,7*pi/6,4*pi/3,3*pi/2,5*pi/3,11*pi/6)
  spec$appearance <- list(smooth=TRUE,smooth.faces=rep(TRUE,700L),cull=TRUE,
    fit="legacy",eye.cex=.7,compass=c(1.12,1.10,.18),dorsal="#233d53",ventral="#d6dcde")
  spec$surface <- "tuna"
  spec$revision <- "tuna-surface-v24"
  spec$review <- list(status="approved",checkpoint="v24",date="2026-10-29",
    geometry=list(status="approved",checkpoint="v23",date="2026-10-29"),
    measured.views="lateral",inferred.views=c("dorsal","anterior"),
    interpretation="long-finned illustrated form; oblique view qualitative; mouth closed; inferred width and paired-fin pose; auxiliary keels omitted")
  spec$anatomy <- "lateral-reference fitted schematic; inferred transverse geometry; approved lateral rig; profile-averaged colour, projected eyes and opercular/lateral lines; schematic closed lip seam"
  spec
}
