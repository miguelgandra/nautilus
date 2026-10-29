# Neutral asset sections in percent axial TL; cephalofoil adapted to smooth hammerhead.
.referenceHammerheadControls <- function() list(
body=matrix(c(
    7, 5.089686926, -1.599956517, 4.474870232,
    11, 5.067744964, -3.304233707, 5.577602454,
    16, 5.12920895, -5.254880426, 5.962960365,
    23, 4.736321502, -7.079242335, 6.750300487,
    30, 4.628028693, -9.36528065, 6.897789341,
    40, 4.301284419, -9.302133243, 6.453644149,
    49, 3.624140528, -5.930815857, 4.881606026,
    58, 2.402409408, -4.416984952, 3.632233727,
    65, 1.808416997, -3.717928521, 2.617043049,
    69, 1.374478907, -3.223461942, 2.422138159,
    71, 1.212110078, -3.368809067, 2.345575547
  ),ncol=4,byrow=TRUE),
head=matrix(c(
    -13.44, 42.74386303, 42.71601984, 0.334561389, 0.01673774123,
    -13, 44.18371038, 38.09371103, 0.4638960212, 1.33066152,
    -12.5, 45.97372431, 37.01286559, 0.3061378143, 1.452374406,
    -11.5, 45.79445, 37.05806608, 0.4176527822, 0.9276658715,
    -8, 47.9648, 38.55518423, 0.7010008455, 0.7539905919,
    -4, 49.4912, 37.36666039, 1.453179356, 2.161896784,
    0, 50, 37.3461624, 1.37658412, 4.039746011,
    4, 49.4912, 37.36666039, 1.453179356, 2.161896784,
    8, 47.9648, 38.55518423, 0.7010008455, 0.7539905919,
    11.5, 45.79445, 37.05806608, 0.4176527822, 0.9276658715,
    12.5, 45.97372431, 37.01286559, 0.3061378143, 1.452374406,
    13, 44.18371038, 38.09371103, 0.4638960212, 1.33066152,
    13.44, 42.74386303, 42.71601984, 0.334561389, 0.01673774123
  ),ncol=5,byrow=TRUE),
pectoral=matrix(c(
    3.7, 28.5, 16.5, 6, 6.5, 0.55,
    6.5, 25.47847235, 16.94185927, 8.848607624, 9.007474999, 0.4579333098,
    9.5, 22.33773939, 16.20033596, 11.15908476, 11.21668256, 0.3100342361,
    11.7, 19.86174428, 16.08767297, 12.87066046, 12.88479105, 0.1468460486,
    12.68, 16.39678191, 16.37223111, 13.6308454, 13.6308454, 0.03
  ),ncol=6,byrow=TRUE),
dorsal=matrix(c(
    -6, 23, 10, 0, 1,
    -9, 20.9866192, 9.543547868, 0, 1.8039612,
    -12, 18.6234668, 11.01277292, 0, 1.197712911,
    -16, 15.71027121, 10.70442742, 0, 0.5521509696,
    -18.5, 12.88064971, 11.04692591, 0, 0.1599951875,
    -19.52, 11.22760277, 11.21626769, 0, 0.025
  ),ncol=5,byrow=TRUE),
second_dorsal=matrix(c(
    -3.5, -9.5, -18.8, 0, 0.6,
    -5.3, -11.05906189, -15.63180404, 0, 0.4457177783,
    -6.5, -14.41676043, -14.44621751, 0, 0.025
  ),ncol=5,byrow=TRUE),
anal=matrix(c(
    2, -9, -17, 0, 0.65,
    4, -8.388622618, -14.11135993, 0, 0.3943937032,
    5.48, -13.80532729, -13.85957331, 0, 0.025
  ),ncol=5,byrow=TRUE),
caudal_upper=matrix(c(
    0, -15.3417939, -31.12646999, 0, 1.711068342,
    -2.5, -17.10952875, -34.61204564, 0, 1.086661072,
    -5, -24.44348845, -38.30255704, 0, 0.8989495477,
    -8, -31.66610916, -42.66345748, 0, 0.6543265878,
    -11, -39.03668756, -47.94926328, 0, 0.5291053611,
    -13.8048707, -46.71619891, -49.99999925, 0, 0.4358675527,
    -14.05, -47.60941345, -47.74748967, 0, 0.1060188198
  ),ncol=5,byrow=TRUE),
caudal_lower=matrix(c(
    0, -15.3417939, -31.12646999, 0, 1.711068342,
    2.5, -21.43491737, -28.45389203, 0, 0.4885747993,
    5, -23.55823161, -29.0383291, 0, 0.2579301263,
    7.2, -26.46433098, -29.79349436, 0, 0.1724556693,
    8.47, -29.63569008, -29.66993768, 0, 0.025
  ),ncol=5,byrow=TRUE),
pelvic=matrix(c(
    5.1, -0.001367389297, -5.331639594, 1.131618289, 0.8288423192,
    6.6, 1.172161617, -3.845868832, 2.055752372, 0.4891787774,
    8.16, -2.251183448, -2.300667238, 3.148298512, 0.035
  ),ncol=5,byrow=TRUE))

.referenceHammerheadSpec <- function() {
  c <- .referenceHammerheadControls();b <- c$body;z <- (b[,3]+b[,4])/2
  body <- .anatomyBody(b[,1],b[,2],z-b[,3],b[,4]-z,z)
  h <- c$head
  head <- list(id="cephalofoil",kind="loft",axis="y",sections=6L,
    centreline=(h[,2]+h[,3])/2,part="body",role="rigid",joint.x=NA_real_,
    pivot=c(43,0,.8),allocation="measured transverse head sections; unnotched central arc adaptation",
    stations=.speciesBody(h[,1],(h[,2]-h[,3])/2,h[,5],h[,5],h[,4]))
  p <- c$pectoral
  pectoral <- .anatomyFoil("pectoral",cbind(p[,1:3],(p[,4]+p[,5])/2,p[,6]),sections=4L)
  pectoral$leading[,3] <- p[,4];pectoral$trailing[,3] <- p[,5]
  pectoral$pivot <- (pectoral$leading[1,]+pectoral$trailing[1,])/2
  pectoral$allocation <- "measured swept blade, droop and root thickness"
  foil <- function(id,role="attached") {
    out <- .anatomyFoil(id,c[[id]],"xz",role,4L)
    out$allocation <- "measured fin sections and embedded root"
    out
  }
  pelvic <- .anatomyFoil("pelvic",c$pelvic,"xz","attached",4L)
  spec <- .anatomySpec("Smooth hammerhead","Sphyrna zygaena",2L,body,
    c(list(head),.anatomyPair(pectoral),list(foil("dorsal"),foil("second_dorsal")),
      .anatomyPair(pelvic),list(foil("anal"),foil("caudal_upper","terminal"),
        foil("caudal_lower","terminal"))),
    c("reference-fitted trunk and fins; smooth-hammerhead head adaptation",
      "broad symmetric cephalofoil without a median notch",
      "separate dorsal, pectoral, pelvic, anal and full caudal components"),
    character(),sections=10L,pivot=c(0,0,0),tip=-21,
    landmarks=list(TL=100,head.width=diff(range(h[,1])),body.width=2*max(b[,2]),
      body.depth=max(b[,4]-b[,3]),pectoral.origin=50-pectoral$leading[1,1],
      pectoral.span=2*max(p[,1]),basis="neutral scalloped asset sections; intentionally unnotched smooth-hammerhead cephalofoil"))
  spec$body.angles <- c(0,pi/4,pi/2,3*pi/4,pi,7*pi/6,4*pi/3,3*pi/2,5*pi/3,11*pi/6)
  spec$appearance <- list(smooth=TRUE,cull=TRUE,fit="legacy",eye.cex=.85,
    compass=c(1.12,1.10,.18),dorsal="#65695e",ventral="#e2e5dc")
  spec$surface <- "hammerhead"
  spec$revision <- "hammerhead-surface-v14"
  spec$review <- list(status="approved",checkpoint="v14",date="2026-10-28",
    geometry=list(status="approved",checkpoint="v13",date="2026-10-28"),
    measured.views=c("dorsal","lateral","anterior"),
    adaptation="scalloped reference used for proportions; median cephalofoil notch removed for Sphyrna zygaena")
  spec$anatomy <- "neutral asset fit with explicit cross-species head adaptation; reference-guided eyes and gills; schematic closed mouth"
  spec
}
