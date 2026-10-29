# Neutral GLB sections in axial percent TL. Geometry is embedded; the source
# asset and extraction tools are not needed by the package at runtime.
.referenceMakoControls <- function() list(
body=matrix(c(
    0.02, 0.1402183388, 0.3386863487, 0.6455732925,
    2, 1.901070076, -0.6197197701, 1.992032643,
    7, 3.872216476, -2.510816437, 4.930439446,
    13, 5.318786383, -4.685629329, 6.35526143,
    23, 6.789508505, -6.950294322, 6.555783027,
    33, 7.45259247, -8.278799157, 6.724177086,
    43, 7.643187785, -7.265697077, 6.822823163,
    55, 6.216853775, -6.178565627, 5.309131623,
    65, 4.495657401, -3.877287145, 3.607984478,
    73, 3.671739731, -2.207030636, 2.987744499,
    79, 2.984032209, -0.1893299461, 2.651555911,
    83, 2.087537193, 0.6235145699, 1.762691307,
    86, 1.576809981, -1.181206504, 3.9937803
  ),ncol=4,byrow=TRUE),
  pectoral=matrix(c(
    6, 21.97396459, 11.68654437, 4.764696482, 4.548954087, 0.8813519225,
    7.7, 20.99671647, 12.07497261, 4.346381945, 4.631091135, 1.80932636,
    10.5, 19.06625525, 12.19237742, 4.706464759, 4.342115641, 0.9877220571,
    14, 15.9437091, 10.16500305, 4.935769746, 5.024739696, 0.7581247657,
    16.9, 12.12333261, 8.725345394, 5.37810729, 5.394855492, 0.4643883386,
    17.91, 9.351551567, 9.180180665, 5.480715107, 5.480715107, 0.07300166008
  ),ncol=6,byrow=TRUE),
  dorsal=matrix(c(
    -6.2, 15, 1.8, 0, 1.65,
    -8.7, 13.86127629, 2.944526523, 0, 1.263593013,
    -9.7, 11.71987077, 3.737917034, 0, 0.9284921723,
    -13, 9.440792008, 4.393785676, 0, 0.5751961008,
    -15.4, 7.059651733, 4.173790206, 0, 0.3494097367,
    -16.19, 5.461372755, 4.689423473, 0, 0.018
  ),ncol=5,byrow=TRUE),
  second_dorsal=matrix(c(
    -1.2, -22, -27, 0, 0.8,
    -2.2, -21.46288583, -24.57836372, 0, 0.7222337327,
    -2.92, -24.19405257, -24.304301, 0, 0.05339853129
  ),ncol=5,byrow=TRUE),
  anal=matrix(c(
    2.6, -22.03602197, -29.39041054, 0, 1.860319646,
    4, -23.97140875, -25.43571136, 0, 0.3185790372,
    4.76, -24.82768934, -24.91296982, 0, 0.02244401076
  ),ncol=5,byrow=TRUE),
  caudal_upper=matrix(c(
    1.4, -30.34387858, -40.85296281, 0, 3.435116059,
    -0.3, -32.42021031, -41.93084539, 0, 1.297303068,
    -1.6, -33.9252492, -43.063917, 0, 1.003343275,
    -3.3, -36.24936402, -44.63827611, 0, 0.7131889206,
    -6.2, -40.96736223, -46.93100761, 0, 0.4530833781,
    -8.3, -45.96422574, -49.29384663, 0, 0.3630485608,
    -9.34706389, -48.81976166, -50, 0, 0.018
  ),ncol=5,byrow=TRUE),
  caudal_lower=matrix(c(
    1.4, -30.34387858, -40.85296281, 0, 3.435116059,
    4, -32.72444939, -39.8185845, 0, 1.116439347,
    5.8, -33.53414431, -39.72751543, 0, 0.8829704489,
    7.5, -34.20638016, -39.83125023, 0, 0.7713316266,
    10.8, -36.40086202, -39.80283945, 0, 0.4043531916,
    12.5, -38.00340862, -39.55361663, 0, 0.1510135189,
    13.18, -39.04473577, -39.21613741, 0, 0.018
  ),ncol=5,byrow=TRUE),
  pelvic=matrix(c(
    3.2, -7.322288423, -12.40824897, 3.622834628, 0.7284962421,
    4.8, -8.523914618, -10.47043974, 3.728386886, 0.6681795695,
    5.45, -9.657076924, -9.794159179, 4.522719043, 0.055073875
  ),ncol=5,byrow=TRUE))

.referenceMakoSpec <- function() {
  c <- .referenceMakoControls();b <- c$body;z <- (b[,3]+b[,4])/2
  body <- .anatomyBody(b[,1],b[,2],z-b[,3],b[,4]-z,z)
  p <- rbind(c(4.4,22,11.5,4.2,4.4,1.15),c$pectoral)
  pectoral <- .anatomyFoil("pectoral",cbind(p[,1:3],(p[,4]+p[,5])/2,p[,6]),sections=6L)
  pectoral$leading[,3] <- p[,4];pectoral$trailing[,3] <- p[,5]
  pectoral$pivot <- (pectoral$leading[1,]+pectoral$trailing[1,])/2
  pectoral$allocation <- "measured free blade planform, tilted sections and thick root"
  foil <- function(id,role="attached") {
    fin <- .anatomyFoil(id,c[[id]],"xz",role,4L)
    fin$allocation <- "measured source sections; tapered edge and embedded root"
    fin
  }
  pelvic <- .anatomyFoil("pelvic",c$pelvic,plane="xz",role="attached",sections=4L)
  keel <- .speciesFinSpec("keel",rbind(c(-20,2.9,1.5),c(-26,4.3,1.5),
    c(-35,1.8,1.6),c(-30,2,1.6)),thickness=.16,role="attached",joint.x=-28)
  spec <- .anatomySpec("Shortfin mako","Isurus oxyrinchus",2L,body,
    c(.anatomyPair(pectoral),list(foil("dorsal"),foil("second_dorsal")),
      .anatomyPair(pelvic),list(foil("anal"),foil("caudal_upper","terminal"),
        foil("caudal_lower","terminal")),.speciesPair(keel,"keel_right","keel_left")),
    c("neutral-reference conical head and muscular trunk",
      "paired curved pectoral sections and tall first dorsal",
      "small second dorsal and anal; keeled peduncle; full caudal lobes"),
    character(),sections=10L,pivot=c(0,0,0),tip=-33,
    landmarks=list(TL=100,body.width=2*max(b[,2]),body.depth=max(b[,4]-b[,3]),
      pectoral.origin=50-pectoral$leading[1,1],pectoral.span=2*max(p[,1]),
      basis="supplied GLB neutral mesh sections; bilateral fin average; simplified closed jaw and keel"))
  # More dorsal arc samples resolve the shoulder and peduncle curvature.
  spec$body.angles <- c(0,pi/4,pi/2,3*pi/4,pi,7*pi/6,4*pi/3,3*pi/2,5*pi/3,11*pi/6)
  spec$appearance <- list(smooth=TRUE,cull=TRUE,fit="legacy",eye.cex=.85,
    compass=c(1.12,1.10,.18),dorsal="#456379",ventral="#e0e7e8")
  spec$surface <- "mako"
  spec$revision <- "mako-surface-v12"
  spec$review <- list(status="approved",checkpoint="v12",date="2026-10-28",
    geometry=list(status="approved",checkpoint="v11",date="2026-10-28"),
    measured.views=c("dorsal","lateral","anterior"),
    surface.basis="neutral asset eye positions and UV-traced gills; simplified countershading and closed-jaw seam")
  spec$anatomy <- "approved neutral-reference geometry and rig; reference-guided surface marks; simplified closed jaw and keel"
  spec
}
