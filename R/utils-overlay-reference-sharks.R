# Approved v5 controls in percent TL. All views and motion use these same
# vertices. Geometry is embedded so no reference files or import dependencies
# are needed at runtime. Surface marks are attached after mesh compilation.

.referenceSharkControls <- function(whale) {
  if (whale) {
    list(body = structure(c(0.02, 1, 5, 12, 23, 29, 35, 48, 62, 73,
    79.5, 82.5, 4.8, 8.11, 10.3, 11.12, 10.8, 10.1, 9.15, 6.16, 4.78,
    3.88, 3.33, 2.45, -0.85, -1.94, -3.46, -5.57, -7.81, -8.16, -7.69,
    -4.61, -1.81, 0.31, 1.18, 1.7, 1, 1.96, 3.59, 5.48, 6.99, 7.1,
    6.89, 5.78, 5.26, 4.95, 5.03, 5), dim = c(12L, 4L), dimnames = list(
        NULL, c("s", "half_width", "dorsal_z", "ventral_z"))), fins = list(
        pectoral = structure(c(10.5, 12.5, 16, 19, 21.2, 21.77, 29.1114634086482,
        23.309397607106, 20.3660839163008, 17.764004320594, 14.446061560696,
        11.6185701024696, 12.4011416782819, 13.1388336916317, 13.2353096393472,
        12.040653893512, 10.5047785605722, 10.7119606538624, 1.71438153700219,
        3.00472647430113, 3.6656932719523, 4.29532202803771, 5.28099508453721,
        5.73572132258197, 2.46154695180009, 1.3912732101774, 0.924151041423352,
        0.698226913323083, 0.508033096868228, 0.100489873515913), dim = 6:5),
        dorsal = structure(c(-5.7, -7, -8.7, -10.3, -11, 6.17813379193878,
        3.96781098035699, 1.46600864527382, -1.09329017556058, -2.97896665411916,
        -4.57240992107178, -4.48032644755752, -4.57240222127128,
        -4.32561612447781, -3.0675562382405, 0, 0, 0, 0, 0, 0.948587392199047,
        0.527261510773737, 0.388193624922894, 0.220740548715855,
        0.02), dim = c(5L, 5L)), caudal_upper = structure(c(4, 2,
        -1, -4, -7, -8.56, -31.0179813081008, -30.5212049000261,
        -33.9846041353554, -39.2936221265735, -45.3999946037448,
        -49.5806118337508, -38.5726146853082, -40.2968030585235,
        -43.1589659927373, -46.121949988254, -49.0676684520824, -49.6586017202146,
        0, 0, 0, 0, 0, 0, 2.87749153297717, 1.70657571145348, 0.977048709337255,
        0.661699752792205, 0.300642546184638, 0.02), dim = 6:5),
        caudal_lower = structure(c(4.9, 8, 12, 15, 16.75, -30.1226698504913,
        -31.5444953773236, -33.7026159132811, -35.6929673130535,
        -37.4261863108261, -32.7456956723488, -37.5803846074182,
        -37.8199695515242, -38.0503175563017, -37.6333774168533,
        0, 0, 0, 0, 0, 1.83854693333964, 0.877293943394327, 0.552501716572463,
        0.378451177349461, 0.02), dim = c(5L, 5L)), second_dorsal = structure(c(-0.9,
        -2.3, -3.29, -14.8, -17, -18.8, -21, -20.5, -19.8, 0, 0,
        0, 0.55, 0.2, 0.025), dim = c(3L, 5L)), anal = structure(c(4.8,
        6.2, 7.1, -14.7, -16.6, -18, -20.5, -19.8, -18.7, 0, 0, 0,
        0.5, 0.2, 0.025), dim = c(3L, 5L)), pelvic = structure(c(2.3,
        3.2, 3.9, -0.5, -2.7, -5, -8, -7.3, -5.6, 5.7, 6.7, 7.6,
        0.65, 0.35, 0.08), dim = c(3L, 5L))))
  } else {
    list(body = structure(c(0, 0.8, 3, 7, 13, 20, 30, 40, 52, 64,
    72, 75.5, 0.6426735218509, 1.26465154382892, 2.47497857754927,
    3.63018400272733, 4.63375757436651, 4.80486094881982, 4.28466928811778,
    4.15655526992288, 3.66638103399029, 2.79627249357326, 2.17418288652222,
    1.62147093311685, -0.386100386100386, -0.614671814671815, -1.2287136760821,
    -2.17608209713473, -3.1816731016731, -4.32338832338832, -5.52252252252252,
    -6.16731016731017, -5.46932646932647, -4.17956241956242, -2.75383955383955,
    -2.06642906642907, 0.386100386100386, 0.765047754521439, 1.70288656002942,
    2.84144144144144, 3.98129558129558, 4.88648648648649, 5.27339584482442,
    4.94105534105534, 3.81724581724582, 2.25328185328185, 0.971428571428571,
    0.518414018414018), dim = c(12L, 4L), dimnames = list(NULL, c("s",
    "half_width", "dorsal_z", "ventral_z"))), fins = list(dorsal = structure(c(-5.8,
    -8, -11, -13.5, -14.39, 5.5096525096525, 12.5836550836551, 10.3639942463472,
    7.11062848562849, 4.51746675246675, 1.08513513513513, 4.73359073359073,
    3.45448305448305, 3.09840197340198, 4.31808236808237, 0, 0, 0,
    0, 0, 0.45, 0.35, 0.25, 0.15, 0.05), dim = c(5L, 5L)), second_dorsal = structure(c(-3.8,
    -5, -6.16, -17.6401115401115, -17.1645752895753, -18.9887258687259,
    -20.8681145431145, -19.8069498069498, -19.1237065637065, 0, 0,
    0, 0.45, 0.27, 0.09), dim = c(3L, 5L)), anal = structure(c(2,
    3.5, 4.8, -16.9853281853282, -16.3056628056628, -18.9616473616474,
    -21.0553410553411, -19.4132882882883, -19.8223938223938, 0, 0,
    0, 0.45, 0.27, 0.09), dim = c(3L, 5L)), pectoral = structure(c(4.5,
    7.5, 11, 15, 19, 20.8, 29.0484147386461, 28.0484147386461, 26.6210299361473,
    24.559476513204, 22.2246786632391, 19.4728363324764, 21.5925449871465,
    21.0655526992288, 20.3604595115681, 19.5065046350393, 18.4247000856898,
    19.3691516709512, 3.8, 4.34, 4.97, 5.69, 6.41, 6.734, 0.75, 0.613636363636364,
    0.477272727272727, 0.340909090909091, 0.204545454545455, 0.0681818181818182
    ), dim = 6:5), pelvic = structure(c(2.8, 4.5, 5.95, -7.86478149100257,
    -4.6417737789203, -6.9175235646958, -10.2480205655527, -9.09286632390746,
    -7.34280205655527, 3.4, 5.7, 7, 0.75, 0.45, 0.15), dim = c(3L,
    5L)), caudal_upper = structure(c(-0.3, -3, -6, -8.5, -10, -11.54,
    -24.4831145431145, -25.8038038038038, -29.96996996997, -34.9984984984985,
    -39.3307593307593, -47.9612998712999, -31.3810810810811, -34.2338665195808,
    -38.6061776061776, -44.3471226328369, -47.4370288656003, -48.563462033462,
    0, 0, 0, 0, 0, 0, 0.7, 0.572727272727273, 0.445454545454545,
    0.318181818181818, 0.190909090909091, 0.0636363636363637), dim = 6:5),
        caudal_lower = structure(c(0.3, 2, 4.5, 6.5, 7.56, -24.6031145431145,
        -26.984438984439, -29.2852462852463, -32.1706885456885, -34.3473462033462,
        -30.8739811239811, -31.5162389280036, -33.2841514984372,
        -34.5884813384813, -34.4716945516946, 0, 0, 0, 0, 0, 0.7,
        0.544444444444444, 0.388888888888889, 0.233333333333333,
        0.0777777777777778), dim = c(5L, 5L))))
  }
}

.referenceSharkSpec <- function(whale) {
  controls <- .referenceSharkControls(whale)
  body <- controls$body; fins <- controls$fins
  z <- (body[,3]+body[,4])/2
  body <- .anatomyBody(body[,1],body[,2],z-body[,3],body[,4]-z,z)
  # Preserve the approved buried attachment stations and free blade outline.
  fins$pectoral <- rbind(if (whale) c(8,50-18.425,50-38.484,1.35,3.9)
    else c(3.2,29,22,3.4,.9),fins$pectoral)
  foil <- function(name,plane="xz",role="attached",sections=4L) {
    fin <- .anatomyFoil(name,fins[[name]],plane,role,sections)
    if (!whale && name %in% c("dorsal","second_dorsal","anal")) {
      root <- switch(name,dorsal=rbind(c(294,70),c(399,73)),
        second_dorsal=rbind(c(531,86),c(574,92)),anal=rbind(c(521,134),c(574,129)))
      points <- cbind(50-(root[,1]-16)/777*100,0,(root[,2]-117)/777*100)
      fin$leading[1,] <- points[1,];fin$trailing[1,] <- points[2,]
    }
    if (!whale && name=="pelvic") {
      fin$leading[1,] <- c(50-(432-16)/778*100,(292-264)/778*100,(146-117)/777*100)
      fin$trailing[1,] <- c(50-(487-16)/778*100,(282-264)/778*100,(141-117)/777*100)
    }
    fin$pivot <- (fin$leading[1,]+fin$trailing[1,])/2
    fin$joint.x <- fin$pivot[1]
    fin
  }
  paired <- function(name,role="rigid",sections=6L)
    .anatomyPair(foil(name,"xy",role,sections))
  appendages <- c(paired("pectoral"),list(foil("dorsal"),foil("second_dorsal")),
    paired("pelvic","attached",4L),list(foil("anal"),
      foil("caudal_upper",role="terminal"),foil("caudal_lower",role="terminal")))
  keel <- .speciesFinSpec("keel",
    if (whale) rbind(c(-23,2.8,3.1),c(-28,3.55,3.2),c(-33,2.9,3.4),c(-30,2.4,3.3))
    else rbind(c(-20,1,1),c(-24,1.5,1),c(-27,1,1),c(-24,.7,1)),
    thickness=if (whale) .15 else .08,role="attached",joint.x=if (whale) -28 else -24)
  appendages <- c(appendages,.speciesPair(keel,"keel_right","keel_left"))
  spec <- .anatomySpec(if (whale) "Whale shark" else "Blue shark",
    if (whale) "Rhincodon typus" else "Prionace glauca",if (whale) 2L else 1L,
    body,appendages,
    if (whale) c("approved reference-fit head, trunk and broad pectoral planforms",
      "thick keeled peduncle; full asymmetrical caudal blade",
      "progressive mid-trunk wave; pale countershading and subtle dorsal spots")
    else c("approved drawing-traced snout, body and long scythe pectorals",
      "first dorsal at 35.8 percent TL; distinct second dorsal and small pelvics",
      "smooth blue countershading and prominent lateral eyes"),
    character(),pivot=if (whale) c(13,0,0) else c(0,0,0),tip=if (whale) -30 else -25,
    landmarks=list(TL=100,body.width=2*max(body[,2]),body.depth=max(body[,3]+body[,4]),
      basis=if (whale) "approved v5 neutral GLB section and fin fit"
        else "approved v5 lateral and ventral drawing traces; anterior section inferred"))
  if (whale) {
    spec$rig$exponent <- 1.1
    spec$rig$weighting <- "progressive mid-trunk lateral bend; full caudal blade shares terminal rotation"
  }
  spec$appearance <- list(smooth=TRUE,cull=TRUE,fit="tight",eye.cex=if (whale) .65 else .95,
    compass=c(1.12,1.10,.18),dorsal=if (whale) "#536e7b" else "#39748e",ventral="#d6e2e5")
  spec$surface <- if (whale) "whale_shark" else "blue_shark"
  spec$revision <- "reference-fit-v6"
  spec$review <- list(status="approved",checkpoint="v6",date="2026-10-27")
  spec$anatomy <- "reference-fitted schematic; approved v5 geometry, illustrative surface markings"
  spec
}
