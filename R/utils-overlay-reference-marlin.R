# Stored neutral striped-marlin sections in percent axial TL, including bill.
.referenceMarlinControls <- function() list(
body=matrix(c(
    18.6, 0.9780506334, 1.766440391, 3.701715149,
    21.5, 2.61292122, 1.035367137, 5.577560714,
    25, 4.037592212, 0.04276090941, 7.388827782,
    30, 4.903575885, -1.940234659, 9.009159166,
    35, 4.476798384, -4.003753766, 9.767688007,
    40, 4.103483307, -4.594673713, 9.422572666,
    45, 3.85699975, -4.132744915, 9.103033826,
    52, 3.291782765, -2.834432456, 8.618985919,
    60, 2.514729405, -1.594043742, 7.059321814,
    68, 1.625544148, -0.1970879644, 5.301036795,
    74, 0.8453637561, 0.9153032658, 4.445940554,
    78, 0.4545818086, 0.9341618801, 4.409289982,
    80.5, 0.4354951608, 0.9893766226, 4.382144242,
    82, 0.22, 1.9, 3.5
  ),ncol=4,byrow=TRUE),
body_vertices=matrix(c(
    31.4, 0.9780506334, 2.73407777,
    31.4, 0.3082492083, 3.039045065,
    31.4, 5.988832888e-17, 3.701715149,
    31.4, -0.3082492083, 3.039045065,
    31.4, -0.9780506334, 2.73407777,
    31.4, -0.7358613503, 2.006051097,
    31.4, -1.796649866e-16, 1.766440391,
    31.4, 0.7358613503, 2.006051097,
    28.5, 2.61292122, 3.306463926,
    28.5, 1.850557256, 4.91492981,
    28.5, 1.599952804e-16, 5.577560714,
    28.5, -1.850557256, 4.91492981,
    28.5, -2.61292122, 3.306463926,
    28.5, -1.650012866, 1.872307055,
    28.5, -4.799858412e-16, 1.035367137,
    28.5, 1.650012866, 1.872307055,
    25, 4.037592212, 3.715794345,
    25, 2.803829318, 6.266462737,
    25, 2.472312189e-16, 7.388827782,
    25, -2.803829318, 6.266462737,
    25, -4.037592212, 3.715794345,
    25, -3.564249192, 0.4733652521,
    25, -7.416936568e-16, 0.04276090941,
    25, 3.564249192, 0.4733652521,
    20, 4.903575885, 3.534462254,
    20, 3.496524705, 7.438228292,
    20, 3.002574256e-16, 9.009159166,
    20, -3.496524705, 7.438228292,
    20, -4.903575885, 3.534462254,
    20, -3.362411465, -0.2195703337,
    20, -9.007722768e-16, -1.940234659,
    20, 3.362411465, -0.2195703337,
    15, 4.476798384, 2.88196712,
    15, 3.355047879, 8.042334256,
    15, 2.741248406e-16, 9.767688007,
    15, -3.355047879, 8.042334256,
    15, -4.476798384, 2.88196712,
    15, -2.88585945, -1.556745769,
    15, -8.223745218e-16, -4.003753766,
    15, 2.88585945, -1.556745769,
    10, 4.103483307, 2.413949476,
    10, 3.070000585, 7.657415985,
    10, 2.512658849e-16, 9.422572666,
    10, -3.070000585, 7.657415985,
    10, -4.103483307, 2.413949476,
    10, -2.652107475, -2.115768463,
    10, -7.537976547e-16, -4.594673713,
    10, 2.652107475, -2.115768463,
    5, 3.85699975, 2.485144455,
    5, 2.867415155, 7.405091955,
    5, 2.361731199e-16, 9.103033826,
    5, -2.867415155, 7.405091955,
    5, -3.85699975, 2.485144455,
    5, -2.414335535, -1.657403252,
    5, -7.085193597e-16, -4.132744915,
    5, 2.414335535, -1.657403252,
    -2, 3.291782765, 2.892276731,
    -2, 2.461769951, 7.175013959,
    -2, 2.015635614e-16, 8.618985919,
    -2, -2.461769951, 7.175013959,
    -2, -3.291782765, 2.892276731,
    -2, -2.052369937, -0.6782279499,
    -2, -6.046906841e-16, -2.834432456,
    -2, 2.052369937, -0.6782279499,
    -10, 2.514729405, 2.732639036,
    -10, 1.960007057, 6.104901978,
    -10, 1.539827658e-16, 7.059321814,
    -10, -1.960007057, 6.104901978,
    -10, -2.514729405, 2.732639036,
    -10, -1.533763462, 0.0937435976,
    -10, -4.619482974e-16, -1.594043742,
    -10, 1.533763462, 0.0937435976,
    -18, 1.625544148, 2.551974416,
    -18, 1.317778323, 4.780554191,
    -18, 9.953587186e-17, 5.301036795,
    -18, -1.317778323, 4.780554191,
    -18, -1.625544148, 2.551974416,
    -18, -1.034927531, 0.801741826,
    -18, -2.986076156e-16, -0.1970879644,
    -18, 1.034927531, 0.801741826,
    -24, 0.8453637561, 2.68062191,
    -24, 0.6565560154, 4.051665519,
    -24, 5.17636009e-17, 4.445940554,
    -24, -0.6565560154, 4.051665519,
    -24, -0.8453637561, 2.68062191,
    -24, -0.6009907322, 1.425611701,
    -24, -1.552908027e-16, 0.9153032658,
    -24, 0.6009907322, 1.425611701,
    -28, 0.4545818086, 2.671725931,
    -28, 0.3322403895, 3.941660069,
    -28, 2.783510784e-17, 4.409289982,
    -28, -0.3322403895, 3.941660069,
    -28, -0.4545818086, 2.671725931,
    -28, -0.3158774461, 1.464336448,
    -28, -8.350532353e-17, 0.9341618801,
    -28, 0.3158774461, 1.464336448,
    -30.5, 0.4354951608, 2.685760432,
    -30.5, 0.3168455117, 3.919968398,
    -30.5, 2.666638774e-17, 4.382144242,
    -30.5, -0.3168455117, 3.919968398,
    -30.5, -0.4354951608, 2.685760432,
    -30.5, -0.3035312582, 1.503415464,
    -30.5, -7.999916321e-17, 0.9893766226,
    -30.5, 0.3035312582, 1.503415464,
    -32, 0.275, 2.7,
    -32, 0.1944543648, 3.407106781,
    -32, 1.347111479e-17, 3.5,
    -32, -0.1944543648, 3.407106781,
    -32, -0.275, 2.7,
    -32, -0.1944543648, 1.992893219,
    -32, -4.041334437e-17, 1.9,
    -32, 0.1944543648, 1.992893219
  ),ncol=3,byrow=TRUE),
bill=matrix(c(
    0.03, 0.015, 2.257568257, 2.339422324,
    10, 0.4062001758, 2.111490259, 2.986721235,
    19.4, 0.66, 1.9, 3.25
  ),ncol=4,byrow=TRUE),
pectoral=matrix(c(
    3.7, 17.8, 13.6, 3.1, 4.8, 0.5,
    5.3, 18.60504765, 15.01495086, 4.20364484, 6.562472047, 3.430241299,
    6.8, 15.51099765, 12.26153921, 3.732057517, 5.770786803, 2.650086538,
    8.5, 11.38819558, 9.826984614, 3.179217559, 4.207201008, 1.278272568,
    10, 7.934492007, 7.065452094, 3.069284792, 3.400506949, 0.4503802911,
    10.7, 6.025189573, 5.721983108, 3.083777146, 3.281104012, 0.1810699172
  ),ncol=6,byrow=TRUE),
pelvic=matrix(c(
    1.1, 15.7, 12.5, 9.1, 10.4, 0.22,
    3.5, 11.04997309, 6.990219745, 11.78929071, 13.64143538, 0.448179224,
    5.99, 2.470652897, 2.19036634, 15.01570074, 15.01441411, 0.03518623286
  ),ncol=6,byrow=TRUE),
dorsal=matrix(c(
    31.8, -2.868086139, -2.333101538, 0.32,
    35.5, -5.874384238, -3.712845761, 0.2904,
    38.5, -13.04896749, -4.067397729, 0.2664,
    40.83289119, -15.59548465, -4.167726428, 0.2477368705,
    42.2, -11.71552201, -4.041425042, 0.2368,
    46, -6.594907578, -3.597271706, 0.2064,
    54, -4.816531892, -2.174335277, 0.1424,
    67.5, -2.003780722, 0.06560229951, 0.045
  ),ncol=4,byrow=TRUE),
second_dorsal=matrix(c(
    0.5, -18.6, -25.5, 0, 0, 0.32,
    -0.8, -17.97133286, -24.5740622, 0, 0, 0.169677394,
    -1.82, -18.06168451, -18.18110451, 0, 0, 0.01266690017
  ),ncol=6,byrow=TRUE),
anal=matrix(c(
    8.3, -4.2, -11.4, 0, 0, 0.38,
    11, -7.493082209, -10.40062329, 0, 0, 0.2440261286,
    13.72, -11.19217421, -11.28733032, 0, 0, 0.012
  ),ncol=6,byrow=TRUE),
second_anal=matrix(c(
    4.7, -18.2, -23.8, 0, 0, 0.23,
    5.9, -19.99075182, -23.36020432, 0, 0, 0.08768229417,
    6.83, -22.01799905, -22.34078109, 0, 0, 0.012
  ),ncol=6,byrow=TRUE),
caudal_upper=matrix(c(
    2.7, -30.9, -36.1, 0, 0, 0.32,
    0, -32.14369661, -37.08613183, 0, 0, 0.3233029027,
    -3.5, -35.53490108, -39.48639445, 0, 0, 0.3275853432,
    -6.7, -39.52929858, -43.59750689, 0, 0, 0.2482371991,
    -9.2, -44.86902563, -49.59105021, 0, 0, 0.1130886495,
    -9.64, -48.04385836, -49.8411183, 0, 0, 0.02823363599
  ),ncol=6,byrow=TRUE),
caudal_lower=matrix(c(
    2.7, -30.9, -36.1, 0, 0, 0.32,
    6, -31.83941191, -37.00438202, 0, 0, 0.3279808663,
    10, -35.18712773, -39.29863977, 0, 0, 0.2930259776,
    13, -38.56703792, -42.20439536, 0, 0, 0.2434425823,
    15.25, -43.83374752, -46.72177783, 0, 0, 0.06622346082,
    15.66, -46.26757951, -46.73161006, 0, 0, 0.012
  ),ncol=6,byrow=TRUE))

.referenceMarlinSpec <- function() {
  controls <- .referenceMarlinControls();b <- controls$body;z <- (b[,3]+b[,4])/2
  body <- .anatomyBody(b[,1],b[,2],z-b[,3],b[,4]-z,z)
  foil <- function(id,plane="xy",role="rigid") {
    p <- controls[[id]]
    fin <- .anatomyFoil(id,cbind(p[,1:3],(p[,4]+p[,5])/2,p[,6]),plane,role,if(id=="pectoral")6L else 4L)
    axis <- if(plane=="xy")3L else 2L
    fin$leading[,axis] <- p[,4];fin$trailing[,axis] <- p[,5]
    if(plane=="xy") {
      d <- colSums(fin$leading-fin$trailing)
      fin$normal <- c(-d[3]/d[1],0,1);fin$normal <- fin$normal/sqrt(sum(fin$normal^2))
    }
    if(plane=="xy") {
      nsection <- fin$sections;a <- (seq_len(nsection)-1L)*2*pi/nsection
      fin$section.vertices <- do.call(rbind,lapply(seq_len(nrow(p)),function(i) {
        d <- fin$leading[i,]-fin$trailing[i,];n <- c(-d[3],0,d[1]);n <- n/sqrt(sum(n^2))
        centre <- (fin$leading[i,]+fin$trailing[i,])/2
        matrix(centre,nsection,3,byrow=TRUE)+outer(cos(a),d/2)+outer(sin(a),n*p[i,6])
      }))
    }
    fin$pivot <- (fin$leading[1,]+fin$trailing[1,])/2
    fin$allocation <- "measured sectional planform; embedded root and finite tapered tip"
    fin
  }
  bill <- controls$bill;bz <- (bill[,3]+bill[,4])/2
  bill <- list(id="bill",kind="loft",part="body",role="rigid",joint.x=NA_real_,sections=6L,
    pivot=c(50-tail(bill[,1],1),0,tail(bz,1)),allocation="measured bill taper / overlapping closed head root",
    stations=.anatomyBody(bill[,1],bill[,2],bz-bill[,3],bill[,4]-bz,bz))
  d <- controls$dorsal
  dorsal <- list(id="dorsal",kind="foil",part="fin",role="axial",joint.x=NA_real_,sections=4L,
    pivot=c(50-d[1,1],0,d[1,3]),normal=c(0,1,0),thickness=d[,4],
    leading=cbind(50-d[,1],0,d[,2]),trailing=cbind(50-d[,1],0,d[,3]),
    allocation="measured long dorsal membrane / anterior crest / embedded base")
  # The reference's very small keel is retained as a simple closed ridge.
  keel <- .speciesFinSpec("keel",rbind(c(-26.5,.25,2.7),c(-32.3,.22,2.7),c(-30,.67,2.7)),
    normal=c(0,0,1),thickness=.11,role="axial")
  spec <- .anatomySpec("Striped marlin","Kajikia audax",2L,body,
    c(list(bill,dorsal,foil("second_dorsal","xz","attached"),foil("anal","xz","attached"),
      foil("second_anal","xz","attached")),.anatomyPair(foil("pectoral")),
      .anatomyPair(foil("pelvic")),list(foil("caudal_upper","xz","terminal"),
      foil("caudal_lower","xz","terminal")),.speciesPair(keel,"keel_right","keel_left")),
    c("reference-fitted round tapered bill and laterally compressed trunk",
      "tall anterior dorsal crest and long low membrane; second dorsal and two anal fins",
      "swept pectorals, narrow pelvic ribbons and a crescent caudal with small keel ridges"),
    character(),type="lateral",sections=8L,pivot=c(0,0,2.7),tip=-31,
    landmarks=list(TL=100,bill.length=19.4,body.width=2*max(b[,2]),body.depth=max(b[,4]-b[,3]),
      pectoral.origin=32.2,pectoral.span=21.4,first.dorsal.origin=31.8,first.dorsal.end=67.5,
      caudal.span=25.3,basis="stored striped-marlin GLB; paired fins mirrored; axial TL includes bill"))
  spec$body.angles <- (0:7)*pi/4
  rings <- controls$body_vertices
  ix <- unlist(lapply(rev(seq_len(nrow(b))),function(i)(i-1L)*8L+seq_len(8L)))
  rings <- rings[ix,,drop=FALSE]
  spec$body.transform <- function(vertices) {
    n <- nrow(rings);vertices[seq_len(n),] <- rings
    vertices[n+1L,] <- colMeans(rings[1:8,,drop=FALSE]);vertices[n+2L,] <- colMeans(tail(rings,8));vertices
  }
  spec$appearance <- list(smooth=TRUE,smooth.faces=rep(TRUE,696L),cull=TRUE,
    fit="legacy",eye.cex=.7,compass=c(1.12,1.10,.18),dorsal="#2b456d",ventral="#d2d9d6")
  spec$surface <- "marlin"
  spec$revision <- "marlin-surface-v22"
  spec$review <- list(status="approved",checkpoint="v22",date="2026-10-29",
    geometry=list(status="approved",checkpoint="v21",date="2026-10-29"),
    measured.views=c("dorsal","lateral","anterior"),
    interpretation="catalog identity changed to striped marlin by user; paired parts made bilateral; closed mouth, simplified embedded roots and small keel ridges")
  spec$anatomy <- "approved striped-marlin geometry; schematic lateral rig; source-averaged colours, UV-guided eyes and bar positions, schematic mouth and opercular seam"
  spec
}
