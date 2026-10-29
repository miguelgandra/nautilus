# Canonical species definitions and numerical appearance data.
# Coordinates use the animal frame: x forward, y right, z down.

# Blue shark and whale shark --------------------------------------------


# .referenceSharkControls
.referenceSharkControls <- function(whale) {
    if (whale) {
        list(body = structure(c(0.02, 1, 5, 12, 23, 29, 35, 48, 62, 73, 79.5, 82.5, 4.8, 8.11, 10.3,
            11.12, 10.8, 10.1, 9.15, 6.16, 4.78, 3.88, 3.33, 2.45, -0.85, -1.94, -3.46, -5.57, -7.81,
            -8.16, -7.69, -4.61, -1.81, 0.31, 1.18, 1.7, 1, 1.96, 3.59, 5.48, 6.99, 7.1, 6.89, 5.78,
            5.26, 4.95, 5.03, 5), dim = c(12L, 4L), dimnames = list(NULL, c("s", "half_width", "dorsal_z",
            "ventral_z"))), fins = list(pectoral = structure(c(10.5, 12.5, 16, 19, 21.2, 21.77, 29.1114634086482,
            23.309397607106, 20.3660839163008, 17.764004320594, 14.446061560696, 11.6185701024696, 12.4011416782819,
            13.1388336916317, 13.2353096393472, 12.040653893512, 10.5047785605722, 10.7119606538624,
            1.71438153700219, 3.00472647430113, 3.6656932719523, 4.29532202803771, 5.28099508453721,
            5.73572132258197, 2.46154695180009, 1.3912732101774, 0.924151041423352, 0.698226913323083,
            0.508033096868228, 0.100489873515913), dim = 6:5), dorsal = structure(c(-5.7, -7, -8.7, -10.3,
            -11, 6.17813379193878, 3.96781098035699, 1.46600864527382, -1.09329017556058, -2.97896665411916,
            -4.57240992107178, -4.48032644755752, -4.57240222127128, -4.32561612447781, -3.0675562382405,
            0, 0, 0, 0, 0, 0.948587392199047, 0.527261510773737, 0.388193624922894, 0.220740548715855,
            0.02), dim = c(5L, 5L)), caudal_upper = structure(c(4, 2, -1, -4, -7, -8.56, -31.0179813081008,
            -30.5212049000261, -33.9846041353554, -39.2936221265735, -45.3999946037448, -49.5806118337508,
            -38.5726146853082, -40.2968030585235, -43.1589659927373, -46.121949988254, -49.0676684520824,
            -49.6586017202146, 0, 0, 0, 0, 0, 0, 2.87749153297717, 1.70657571145348, 0.977048709337255,
            0.661699752792205, 0.300642546184638, 0.02), dim = 6:5), caudal_lower = structure(c(4.9,
            8, 12, 15, 16.75, -30.1226698504913, -31.5444953773236, -33.7026159132811, -35.6929673130535,
            -37.4261863108261, -32.7456956723488, -37.5803846074182, -37.8199695515242, -38.0503175563017,
            -37.6333774168533, 0, 0, 0, 0, 0, 1.83854693333964, 0.877293943394327, 0.552501716572463,
            0.378451177349461, 0.02), dim = c(5L, 5L)), second_dorsal = structure(c(-0.9, -2.3, -3.29,
            -14.8, -17, -18.8, -21, -20.5, -19.8, 0, 0, 0, 0.55, 0.2, 0.025), dim = c(3L, 5L)), anal = structure(c(4.8,
            6.2, 7.1, -14.7, -16.6, -18, -20.5, -19.8, -18.7, 0, 0, 0, 0.5, 0.2, 0.025), dim = c(3L,
            5L)), pelvic = structure(c(2.3, 3.2, 3.9, -0.5, -2.7, -5, -8, -7.3, -5.6, 5.7, 6.7, 7.6,
            0.65, 0.35, 0.08), dim = c(3L, 5L))))
    }
    else {
        list(body = structure(c(0, 0.8, 3, 7, 13, 20, 30, 40, 52, 64, 72, 75.5, 0.6426735218509, 1.26465154382892,
            2.47497857754927, 3.63018400272733, 4.63375757436651, 4.80486094881982, 4.28466928811778,
            4.15655526992288, 3.66638103399029, 2.79627249357326, 2.17418288652222, 1.62147093311685,
            -0.386100386100386, -0.614671814671815, -1.2287136760821, -2.17608209713473, -3.1816731016731,
            -4.32338832338832, -5.52252252252252, -6.16731016731017, -5.46932646932647, -4.17956241956242,
            -2.75383955383955, -2.06642906642907, 0.386100386100386, 0.765047754521439, 1.70288656002942,
            2.84144144144144, 3.98129558129558, 4.88648648648649, 5.27339584482442, 4.94105534105534,
            3.81724581724582, 2.25328185328185, 0.971428571428571, 0.518414018414018), dim = c(12L, 4L),
            dimnames = list(NULL, c("s", "half_width", "dorsal_z", "ventral_z"))), fins = list(dorsal = structure(c(-5.8,
            -8, -11, -13.5, -14.39, 5.5096525096525, 12.5836550836551, 10.3639942463472, 7.11062848562849,
            4.51746675246675, 1.08513513513513, 4.73359073359073, 3.45448305448305, 3.09840197340198,
            4.31808236808237, 0, 0, 0, 0, 0, 0.45, 0.35, 0.25, 0.15, 0.05), dim = c(5L, 5L)), second_dorsal = structure(c(-3.8,
            -5, -6.16, -17.6401115401115, -17.1645752895753, -18.9887258687259, -20.8681145431145, -19.8069498069498,
            -19.1237065637065, 0, 0, 0, 0.45, 0.27, 0.09), dim = c(3L, 5L)), anal = structure(c(2, 3.5,
            4.8, -16.9853281853282, -16.3056628056628, -18.9616473616474, -21.0553410553411, -19.4132882882883,
            -19.8223938223938, 0, 0, 0, 0.45, 0.27, 0.09), dim = c(3L, 5L)), pectoral = structure(c(4.5,
            7.5, 11, 15, 19, 20.8, 29.0484147386461, 28.0484147386461, 26.6210299361473, 24.559476513204,
            22.2246786632391, 19.4728363324764, 21.5925449871465, 21.0655526992288, 20.3604595115681,
            19.5065046350393, 18.4247000856898, 19.3691516709512, 3.8, 4.34, 4.97, 5.69, 6.41, 6.734,
            0.75, 0.613636363636364, 0.477272727272727, 0.340909090909091, 0.204545454545455, 0.0681818181818182),
            dim = 6:5), pelvic = structure(c(2.8, 4.5, 5.95, -7.86478149100257, -4.6417737789203, -6.9175235646958,
            -10.2480205655527, -9.09286632390746, -7.34280205655527, 3.4, 5.7, 7, 0.75, 0.45, 0.15),
            dim = c(3L, 5L)), caudal_upper = structure(c(-0.3, -3, -6, -8.5, -10, -11.54, -24.4831145431145,
            -25.8038038038038, -29.96996996997, -34.9984984984985, -39.3307593307593, -47.9612998712999,
            -31.3810810810811, -34.2338665195808, -38.6061776061776, -44.3471226328369, -47.4370288656003,
            -48.563462033462, 0, 0, 0, 0, 0, 0, 0.7, 0.572727272727273, 0.445454545454545, 0.318181818181818,
            0.190909090909091, 0.0636363636363637), dim = 6:5), caudal_lower = structure(c(0.3, 2, 4.5,
            6.5, 7.56, -24.6031145431145, -26.984438984439, -29.2852462852463, -32.1706885456885, -34.3473462033462,
            -30.8739811239811, -31.5162389280036, -33.2841514984372, -34.5884813384813, -34.4716945516946,
            0, 0, 0, 0, 0, 0.7, 0.544444444444444, 0.388888888888889, 0.233333333333333, 0.0777777777777778),
            dim = c(5L, 5L))))
    }
}


# .referenceSharkSpec
.referenceSharkSpec <- function(whale) {
    controls <- .referenceSharkControls(whale)
    body <- controls$body
    fins <- controls$fins
    z <- (body[, 3] + body[, 4])/2
    body <- .anatomyBody(body[, 1], body[, 2], z - body[, 3], body[, 4] - z, z)
    fins$pectoral <- rbind(if (whale)
        c(8, 50 - 18.425, 50 - 38.484, 1.35, 3.9)
    else c(3.2, 29, 22, 3.4, 0.9), fins$pectoral)
    foil <- function(name, plane = "xz", role = "attached", sections = 4L) {
        fin <- .anatomyFoil(name, fins[[name]], plane, role, sections)
        if (!whale && name %in% c("dorsal", "second_dorsal", "anal")) {
            root <- switch(name, dorsal = rbind(c(294, 70), c(399, 73)), second_dorsal = rbind(c(531,
                86), c(574, 92)), anal = rbind(c(521, 134), c(574, 129)))
            points <- cbind(50 - (root[, 1] - 16)/777 * 100, 0, (root[, 2] - 117)/777 * 100)
            fin$leading[1, ] <- points[1, ]
            fin$trailing[1, ] <- points[2, ]
        }
        if (!whale && name == "pelvic") {
            fin$leading[1, ] <- c(50 - (432 - 16)/778 * 100, (292 - 264)/778 * 100, (146 - 117)/777 *
                100)
            fin$trailing[1, ] <- c(50 - (487 - 16)/778 * 100, (282 - 264)/778 * 100, (141 - 117)/777 *
                100)
        }
        fin$pivot <- (fin$leading[1, ] + fin$trailing[1, ])/2
        fin$joint.x <- fin$pivot[1]
        fin
    }
    paired <- function(name, role = "rigid", sections = 6L) .anatomyPair(foil(name, "xy", role, sections))
    appendages <- c(paired("pectoral"), list(foil("dorsal"), foil("second_dorsal")), paired("pelvic",
        "attached", 4L), list(foil("anal"), foil("caudal_upper", role = "terminal"), foil("caudal_lower",
        role = "terminal")))
    keel <- .speciesFinSpec("keel", if (whale)
        rbind(c(-23, 2.8, 3.1), c(-28, 3.55, 3.2), c(-33, 2.9, 3.4), c(-30, 2.4, 3.3))
    else rbind(c(-20, 1, 1), c(-24, 1.5, 1), c(-27, 1, 1), c(-24, 0.7, 1)), thickness = if (whale)
        0.15
    else 0.08, role = "attached", joint.x = if (whale)
        -28
    else -24)
    appendages <- c(appendages, .speciesPair(keel, "keel_right", "keel_left"))
    spec <- .anatomySpec(if (whale)
        "Whale shark"
    else "Blue shark", if (whale)
        "Rhincodon typus"
    else "Prionace glauca", body, appendages, pivot = if (whale)
        c(13, 0, 0)
    else c(0, 0, 0), tip = if (whale)
        -30
    else -25)
    if (whale) {
        spec$rig$exponent <- 1.1
    }
    spec$appearance <- list(smooth = TRUE, cull = TRUE, fit = "tight", eye.cex = if (whale) 0.65 else 0.95,
        compass = c(1.12, 1.1, 0.18), dorsal = if (whale) "#536e7b" else "#39748e", ventral = "#d6e2e5")
    spec$surface <- if (whale)
        "whale_shark"
    else "blue_shark"
    spec
}


# .anatomyWhaleShark
.anatomyWhaleShark <- function() .referenceSharkSpec(TRUE)


# .anatomyBlueShark
.anatomyBlueShark <- function() .referenceSharkSpec(FALSE)


# .compileSharkSurface
.compileSharkSurface <- function(spec, geometry) {
    whale <- spec$surface == "whale_shark"
    requests <- list()
    add <- function(s, angle, kind, group = 0L) {
        n <- max(length(s), length(angle))
        s <- rep_len(s, n)
        angle <- rep_len(angle, n) * pi/180
        z <- stats::approx(spec$body[, 1], spec$body[, 5], 50 - s, rule = 2)$y
        requests[[length(requests) + 1L]] <<- list(origin = cbind(50 - s, 0, z), direction = cbind(0,
            cos(angle), sin(angle)), kind = rep(kind, n), group = rep(group, n))
    }
    if (whale) {
        for (i in seq_len(17L)) {
            a <- seq(-162, -18, length.out = 7) + 3 * sin(i * 1.7)
            add(5 + i * 3.8 + 1.1 * sin(seq_along(a) * 2 + i), a, "spot")
        }
    }
    add(if (whale)
        3.3
    else 7.9, c(-8, 188), "eye")
    for (side in c(1, -1)) for (i in seq_len(5L)) {
        a <- seq(-35, 38, length.out = 9)
        add((if (whale)
            14.5 + i * 1.15
        else 16.2 + i * 1.18) + 0.35 * (a/40)^2, if (side == 1)
            a
        else 180 - a, "line", i + if (side == 1)
            0L
        else 5L)
    }
    if (whale) {
        y <- seq(-4.2, 4.2, length.out = 25)
        requests[[length(requests) + 1L]] <- list(origin = cbind(49.9, y, 0.22 + 0.1 * (y/4.2)^2), direction = cbind(rep(1,
            length(y)), 0, 0), kind = rep("line", length(y)), group = rep(11L, length(y)))
    }
    else {
        a <- seq(18, 162, length.out = 25)
        add(4.1 + 4.5 * abs(cos(a * pi/180)), a, "line", 11L)
    }
    origins <- do.call(rbind, lapply(requests, `[[`, "origin"))
    directions <- do.call(rbind, lapply(requests, `[[`, "direction"))
    ids <- which(geometry$face.region == "body")
    triangles <- do.call(rbind, geometry$faces[ids])
    v <- geometry$vertices
    a <- v[triangles[, 1], ]
    e1 <- v[triangles[, 2], ] - a
    e2 <- v[triangles[, 3], ] - a
    cross <- function(x, y) cbind(x[, 2] * y[, 3] - x[, 3] * y[, 2], x[, 3] * y[, 1] - x[, 1] * y[, 3],
        x[, 1] * y[, 2] - x[, 2] * y[, 1])
    anchors <- t(vapply(seq_len(nrow(origins)), function(i) {
        d <- matrix(directions[i, ], nrow(a), 3, byrow = TRUE)
        p <- cross(d, e2)
        det <- rowSums(e1 * p)
        det[abs(det) < 1e-12] <- NA_real_
        t <- matrix(origins[i, ], nrow(a), 3, byrow = TRUE) - a
        u <- rowSums(t * p)/det
        q <- cross(t, e1)
        w <- rowSums(d * q)/det
        distance <- rowSums(e2 * q)/det
        ok <- which(is.finite(distance) & distance >= 0 & u >= -1e-09 & w >= -1e-09 & u + w <= 1 + 1e-09)
        if (!length(ok))
            .abort("A shark surface marking could not be attached to the trunk.")
        j <- ok[which.min(distance[ok])]
        b <- pmax(0, c(1 - u[j] - w[j], u[j], w[j]))
        b <- b/sum(b)
        c(ids[j], b)
    }, numeric(4)))
    list(face = as.integer(anchors[, 1]), barycentric = unname(anchors[, 2:4]), kind = unlist(lapply(requests,
        `[[`, "kind")), group = unlist(lapply(requests, `[[`, "group")))
}

# Shortfin mako ---------------------------------------------------------


# .makoCountershading
.makoCountershading <- function(geometry, belly, sections) {
    body <- which(geometry$face.region == "body")
    side <- head(body, length(body) - 2L * sections)
    pairs <- matrix(side, ncol = 2, byrow = TRUE)
    weight <- rowMeans(matrix(belly[side], ncol = 2, byrow = TRUE))
    belly[pairs[, 1]] <- weight
    belly[pairs[, 2]] <- weight
    f <- do.call(rbind, geometry$faces)
    v <- geometry$vertices
    e <- v[f[, 2], ] - v[f[, 1], ]
    g <- v[f[, 3], ] - v[f[, 1], ]
    n <- cbind(e[, 2] * g[, 3] - e[, 3] * g[, 2], e[, 3] * g[, 1] - e[, 1] * g[, 3], e[, 1] * g[, 2] -
        e[, 2] * g[, 1])
    nz <- n[, 3]/pmax(sqrt(rowSums(n^2)), 1e-12)
    u <- pmin(1, pmax(0, (nz + 0.03)/0.35))
    u <- u * u * (3 - 2 * u)
    paired <- geometry$face.region %in% c("pectoral_right", "pectoral_left", "pelvic_right", "pelvic_left")
    belly[paired] <- u[paired] * 0.88
    belly
}


# .compileMakoSurface
.compileMakoSurface <- function(spec, geometry) {
    requests <- list()
    add <- function(s, y, z, kind, group = 0L) {
        n <- max(length(s), length(y), length(z))
        s <- rep_len(s, n)
        y <- rep_len(y, n)
        z <- rep_len(z, n)
        centre <- stats::approx(spec$body[, 1], spec$body[, 5], 50 - s, rule = 2)$y
        requests[[length(requests) + 1L]] <<- list(origin = cbind(50 - s, 0, centre), direction = cbind(0,
            y, z - centre), kind = rep(kind, n), group = rep(group, n), regions = "body")
    }
    add(7.411, c(4.094, -4.094), -0.894, "eye")
    traces <- list(rbind(c(21.583, 4.405, 4.285), c(21.369, 6.174, 1.5), c(20.426, 6.118, -2.314)), rbind(c(23.34,
        4.4, 4.153), c(22.942, 6.292, 1.315), c(22.406, 6.383, -2.622)), rbind(c(24.736, 4.634, 4.224),
        c(24.486, 6.226, 1.337), c(24.394, 6.62, -2.851)), rbind(c(26.335, 5.039, 4.344), c(26.144, 6.311,
        1.355), c(26.45, 6.938, -2.585)), rbind(c(27.623, 6.038, 3.002), c(27.867, 6.644, 0.612), c(28.196,
        6.96, -2.056)))
    for (side in c(1, -1)) for (i in seq_along(traces)) {
        p <- apply(traces[[i]], 2, function(x) stats::approx(1:3, x, seq(1, 3, length.out = 9))$y)
        add(p[, 1], side * p[, 2], p[, 3], "line", i + if (side == 1)
            0L
        else 5L)
    }
    a <- seq(20, 160, length.out = 25) * pi/180
    s <- 6.5 + 7 * abs(cos(a))
    z <- stats::approx(spec$body[, 1], spec$body[, 5], 50 - s, rule = 2)$y
    add(s, cos(a), z + sin(a), "line", 11L)
    .attachOverlaySurfaceRays(geometry, requests)
}


# .referenceMakoControls
.referenceMakoControls <- function() list(body = matrix(c(0.02, 0.1402183388, 0.3386863487, 0.6455732925,
    2, 1.901070076, -0.6197197701, 1.992032643, 7, 3.872216476, -2.510816437, 4.930439446, 13, 5.318786383,
    -4.685629329, 6.35526143, 23, 6.789508505, -6.950294322, 6.555783027, 33, 7.45259247, -8.278799157,
    6.724177086, 43, 7.643187785, -7.265697077, 6.822823163, 55, 6.216853775, -6.178565627, 5.309131623,
    65, 4.495657401, -3.877287145, 3.607984478, 73, 3.671739731, -2.207030636, 2.987744499, 79, 2.984032209,
    -0.1893299461, 2.651555911, 83, 2.087537193, 0.6235145699, 1.762691307, 86, 1.576809981, -1.181206504,
    3.9937803), ncol = 4, byrow = TRUE), pectoral = matrix(c(6, 21.97396459, 11.68654437, 4.764696482,
    4.548954087, 0.8813519225, 7.7, 20.99671647, 12.07497261, 4.346381945, 4.631091135, 1.80932636, 10.5,
    19.06625525, 12.19237742, 4.706464759, 4.342115641, 0.9877220571, 14, 15.9437091, 10.16500305, 4.935769746,
    5.024739696, 0.7581247657, 16.9, 12.12333261, 8.725345394, 5.37810729, 5.394855492, 0.4643883386,
    17.91, 9.351551567, 9.180180665, 5.480715107, 5.480715107, 0.07300166008), ncol = 6, byrow = TRUE),
    dorsal = matrix(c(-6.2, 15, 1.8, 0, 1.65, -8.7, 13.86127629, 2.944526523, 0, 1.263593013, -9.7, 11.71987077,
        3.737917034, 0, 0.9284921723, -13, 9.440792008, 4.393785676, 0, 0.5751961008, -15.4, 7.059651733,
        4.173790206, 0, 0.3494097367, -16.19, 5.461372755, 4.689423473, 0, 0.018), ncol = 5, byrow = TRUE),
    second_dorsal = matrix(c(-1.2, -22, -27, 0, 0.8, -2.2, -21.46288583, -24.57836372, 0, 0.7222337327,
        -2.92, -24.19405257, -24.304301, 0, 0.05339853129), ncol = 5, byrow = TRUE), anal = matrix(c(2.6,
        -22.03602197, -29.39041054, 0, 1.860319646, 4, -23.97140875, -25.43571136, 0, 0.3185790372, 4.76,
        -24.82768934, -24.91296982, 0, 0.02244401076), ncol = 5, byrow = TRUE), caudal_upper = matrix(c(1.4,
        -30.34387858, -40.85296281, 0, 3.435116059, -0.3, -32.42021031, -41.93084539, 0, 1.297303068,
        -1.6, -33.9252492, -43.063917, 0, 1.003343275, -3.3, -36.24936402, -44.63827611, 0, 0.7131889206,
        -6.2, -40.96736223, -46.93100761, 0, 0.4530833781, -8.3, -45.96422574, -49.29384663, 0, 0.3630485608,
        -9.34706389, -48.81976166, -50, 0, 0.018), ncol = 5, byrow = TRUE), caudal_lower = matrix(c(1.4,
        -30.34387858, -40.85296281, 0, 3.435116059, 4, -32.72444939, -39.8185845, 0, 1.116439347, 5.8,
        -33.53414431, -39.72751543, 0, 0.8829704489, 7.5, -34.20638016, -39.83125023, 0, 0.7713316266,
        10.8, -36.40086202, -39.80283945, 0, 0.4043531916, 12.5, -38.00340862, -39.55361663, 0, 0.1510135189,
        13.18, -39.04473577, -39.21613741, 0, 0.018), ncol = 5, byrow = TRUE), pelvic = matrix(c(3.2,
        -7.322288423, -12.40824897, 3.622834628, 0.7284962421, 4.8, -8.523914618, -10.47043974, 3.728386886,
        0.6681795695, 5.45, -9.657076924, -9.794159179, 4.522719043, 0.055073875), ncol = 5, byrow = TRUE))


# .referenceMakoSpec
.referenceMakoSpec <- function() {
    c <- .referenceMakoControls()
    b <- c$body
    z <- (b[, 3] + b[, 4])/2
    body <- .anatomyBody(b[, 1], b[, 2], z - b[, 3], b[, 4] - z, z)
    p <- rbind(c(4.4, 22, 11.5, 4.2, 4.4, 1.15), c$pectoral)
    pectoral <- .anatomyFoil("pectoral", cbind(p[, 1:3], (p[, 4] + p[, 5])/2, p[, 6]), sections = 6L)
    pectoral$leading[, 3] <- p[, 4]
    pectoral$trailing[, 3] <- p[, 5]
    pectoral$pivot <- (pectoral$leading[1, ] + pectoral$trailing[1, ])/2
    foil <- function(id, role = "attached") {
        fin <- .anatomyFoil(id, c[[id]], "xz", role, 4L)
        fin
    }
    pelvic <- .anatomyFoil("pelvic", c$pelvic, plane = "xz", role = "attached", sections = 4L)
    keel <- .speciesFinSpec("keel", rbind(c(-20, 2.9, 1.5), c(-26, 4.3, 1.5), c(-35, 1.8, 1.6), c(-30,
        2, 1.6)), thickness = 0.16, role = "attached", joint.x = -28)
    spec <- .anatomySpec("Shortfin mako", "Isurus oxyrinchus", body, c(.anatomyPair(pectoral), list(foil("dorsal"),
        foil("second_dorsal")), .anatomyPair(pelvic), list(foil("anal"), foil("caudal_upper", "terminal"),
        foil("caudal_lower", "terminal")), .speciesPair(keel, "keel_right", "keel_left")), sections = 10L,
        pivot = c(0, 0, 0), tip = -33)
    spec$body.angles <- c(0, pi/4, pi/2, 3 * pi/4, pi, 7 * pi/6, 4 * pi/3, 3 * pi/2, 5 * pi/3, 11 * pi/6)
    spec$appearance <- list(smooth = TRUE, cull = TRUE, fit = "legacy", eye.cex = 0.85, compass = c(1.12,
        1.1, 0.18), dorsal = "#456379", ventral = "#e0e7e8")
    spec$surface <- "mako"
    spec
}


# .anatomyMako
.anatomyMako <- function() .referenceMakoSpec()

# Smooth hammerhead -----------------------------------------------------


# .hammerheadCountershading
.hammerheadCountershading <- function(geometry, belly, sections) {
    body <- which(geometry$face.region == "body")
    side <- head(body, length(body) - 2L * sections)
    pairs <- matrix(side, ncol = 2, byrow = TRUE)
    weight <- rowMeans(matrix(belly[side], ncol = 2, byrow = TRUE))
    belly[pairs[, 1]] <- weight
    belly[pairs[, 2]] <- weight
    f <- do.call(rbind, geometry$faces)
    v <- geometry$vertices
    e <- v[f[, 2], ] - v[f[, 1], ]
    g <- v[f[, 3], ] - v[f[, 1], ]
    n <- cbind(e[, 2] * g[, 3] - e[, 3] * g[, 2], e[, 3] * g[, 1] - e[, 1] * g[, 3], e[, 1] * g[, 2] -
        e[, 2] * g[, 1])
    nz <- n[, 3]/pmax(sqrt(rowSums(n^2)), 1e-12)
    u <- pmin(1, pmax(0, (nz + 0.03)/0.35))
    u <- u * u * (3 - 2 * u)
    head <- geometry$face.region == "cephalofoil"
    belly[head] <- u[head]
    paired <- geometry$face.region %in% c("pectoral_right", "pectoral_left", "pelvic_right", "pelvic_left")
    belly[paired] <- u[paired] * 0.92
    pectoral <- startsWith(geometry$face.region, "pectoral_")
    span <- abs(rowMeans(cbind(v[f[, 1], 2], v[f[, 2], 2], v[f[, 3], 2])))
    tip <- max(abs(v[startsWith(geometry$region, "pectoral_"), 2]))
    rim <- pmin(1, pmax(0, (span/tip - 0.7)/0.3))
    belly[pectoral] <- belly[pectoral] * (1 - 0.6 * rim[pectoral]^2)
    belly
}


# .compileHammerheadSurface
.compileHammerheadSurface <- function(spec, geometry) {
    requests <- list()
    add <- function(origin, direction, kind, group = 0L, regions) {
        n <- nrow(origin)
        if (is.null(dim(direction)))
            direction <- matrix(direction, n, 3, byrow = TRUE)
        requests[[length(requests) + 1L]] <<- list(origin = origin, direction = direction, kind = rep(kind,
            n), group = rep(group, n), regions = regions)
    }
    for (side in c(1, -1)) add(cbind(50 - 7.283, side * 20, 0.451), c(0, -side, 0), "eye", regions = "cephalofoil")
    traces <- .hammerheadGillTraces()
    for (side in c(1, -1)) for (i in seq_along(traces)) {
        p <- apply(traces[[i]], 2, function(x) stats::approx(1:3, x, seq(1, 3, length.out = 9))$y)
        centre <- stats::approx(spec$body[, 1], spec$body[, 5], 50 - p[, 1], rule = 2)$y
        add(cbind(50 - p[, 1], 0, centre), cbind(0, side * p[, 2], p[, 3] - centre), "line", i + if (side ==
            1)
            0L
        else 5L, "body")
    }
    y <- seq(-3, 3, length.out = 25)
    s <- 5.5 + 2.3 * (y/3)^2
    add(cbind(50 - s, y, 20), c(0, 0, -1), "line", 11L, c("body", "cephalofoil"))
    .attachOverlaySurfaceRays(geometry, requests)
}


# .hammerheadGillTraces
.hammerheadGillTraces <- function() list(rbind(c(20.94857, 4.985497, 1.414005), c(21.008181, 4.294963,
    3.524216), c(21.0195, 3.553762, 5.687164)), rbind(c(19.852003, 4.893056, 0.586973), c(19.730036,
    4.425237, 3.147314), c(19.728595, 3.484827, 5.231869)), rbind(c(18.7983, 5.007904, 0.262322), c(18.530278,
    4.513196, 2.864326), c(18.377217, 3.630046, 5.10277)), rbind(c(17.674515, 5.063619, 0.064356), c(17.34187,
    4.656158, 2.560228), c(16.991369, 3.996907, 4.952402)), rbind(c(16.414496, 5.268836, 0.845593), c(16.121667,
    4.913925, 2.530272), c(15.753687, 4.288156, 4.332738)))


# .referenceHammerheadControls
.referenceHammerheadControls <- function() list(body = matrix(c(7, 5.089686926, -1.599956517, 4.474870232,
    11, 5.067744964, -3.304233707, 5.577602454, 16, 5.12920895, -5.254880426, 5.962960365, 23, 4.736321502,
    -7.079242335, 6.750300487, 30, 4.628028693, -9.36528065, 6.897789341, 40, 4.301284419, -9.302133243,
    6.453644149, 49, 3.624140528, -5.930815857, 4.881606026, 58, 2.402409408, -4.416984952, 3.632233727,
    65, 1.808416997, -3.717928521, 2.617043049, 69, 1.374478907, -3.223461942, 2.422138159, 71, 1.212110078,
    -3.368809067, 2.345575547), ncol = 4, byrow = TRUE), head = matrix(c(-13.44, 42.74386303, 42.71601984,
    0.334561389, 0.01673774123, -13, 44.18371038, 38.09371103, 0.4638960212, 1.33066152, -12.5, 45.97372431,
    37.01286559, 0.3061378143, 1.452374406, -11.5, 45.79445, 37.05806608, 0.4176527822, 0.9276658715,
    -8, 47.9648, 38.55518423, 0.7010008455, 0.7539905919, -4, 49.4912, 37.36666039, 1.453179356, 2.161896784,
    0, 50, 37.3461624, 1.37658412, 4.039746011, 4, 49.4912, 37.36666039, 1.453179356, 2.161896784, 8,
    47.9648, 38.55518423, 0.7010008455, 0.7539905919, 11.5, 45.79445, 37.05806608, 0.4176527822, 0.9276658715,
    12.5, 45.97372431, 37.01286559, 0.3061378143, 1.452374406, 13, 44.18371038, 38.09371103, 0.4638960212,
    1.33066152, 13.44, 42.74386303, 42.71601984, 0.334561389, 0.01673774123), ncol = 5, byrow = TRUE),
    pectoral = matrix(c(3.7, 28.5, 16.5, 6, 6.5, 0.55, 6.5, 25.47847235, 16.94185927, 8.848607624, 9.007474999,
        0.4579333098, 9.5, 22.33773939, 16.20033596, 11.15908476, 11.21668256, 0.3100342361, 11.7, 19.86174428,
        16.08767297, 12.87066046, 12.88479105, 0.1468460486, 12.68, 16.39678191, 16.37223111, 13.6308454,
        13.6308454, 0.03), ncol = 6, byrow = TRUE), dorsal = matrix(c(-6, 23, 10, 0, 1, -9, 20.9866192,
        9.543547868, 0, 1.8039612, -12, 18.6234668, 11.01277292, 0, 1.197712911, -16, 15.71027121, 10.70442742,
        0, 0.5521509696, -18.5, 12.88064971, 11.04692591, 0, 0.1599951875, -19.52, 11.22760277, 11.21626769,
        0, 0.025), ncol = 5, byrow = TRUE), second_dorsal = matrix(c(-3.5, -9.5, -18.8, 0, 0.6, -5.3,
        -11.05906189, -15.63180404, 0, 0.4457177783, -6.5, -14.41676043, -14.44621751, 0, 0.025), ncol = 5,
        byrow = TRUE), anal = matrix(c(2, -9, -17, 0, 0.65, 4, -8.388622618, -14.11135993, 0, 0.3943937032,
        5.48, -13.80532729, -13.85957331, 0, 0.025), ncol = 5, byrow = TRUE), caudal_upper = matrix(c(0,
        -15.3417939, -31.12646999, 0, 1.711068342, -2.5, -17.10952875, -34.61204564, 0, 1.086661072,
        -5, -24.44348845, -38.30255704, 0, 0.8989495477, -8, -31.66610916, -42.66345748, 0, 0.6543265878,
        -11, -39.03668756, -47.94926328, 0, 0.5291053611, -13.8048707, -46.71619891, -49.99999925, 0,
        0.4358675527, -14.05, -47.60941345, -47.74748967, 0, 0.1060188198), ncol = 5, byrow = TRUE),
    caudal_lower = matrix(c(0, -15.3417939, -31.12646999, 0, 1.711068342, 2.5, -21.43491737, -28.45389203,
        0, 0.4885747993, 5, -23.55823161, -29.0383291, 0, 0.2579301263, 7.2, -26.46433098, -29.79349436,
        0, 0.1724556693, 8.47, -29.63569008, -29.66993768, 0, 0.025), ncol = 5, byrow = TRUE), pelvic = matrix(c(5.1,
        -0.001367389297, -5.331639594, 1.131618289, 0.8288423192, 6.6, 1.172161617, -3.845868832, 2.055752372,
        0.4891787774, 8.16, -2.251183448, -2.300667238, 3.148298512, 0.035), ncol = 5, byrow = TRUE))


# .referenceHammerheadSpec
.referenceHammerheadSpec <- function() {
    c <- .referenceHammerheadControls()
    b <- c$body
    z <- (b[, 3] + b[, 4])/2
    body <- .anatomyBody(b[, 1], b[, 2], z - b[, 3], b[, 4] - z, z)
    h <- c$head
    head <- list(id = "cephalofoil", kind = "loft", axis = "y", sections = 6L, centreline = (h[, 2] +
        h[, 3])/2, part = "body", role = "rigid", joint.x = NA_real_, pivot = c(43, 0, 0.8), stations = .speciesBody(h[,
        1], (h[, 2] - h[, 3])/2, h[, 5], h[, 5], h[, 4]))
    p <- c$pectoral
    pectoral <- .anatomyFoil("pectoral", cbind(p[, 1:3], (p[, 4] + p[, 5])/2, p[, 6]), sections = 4L)
    pectoral$leading[, 3] <- p[, 4]
    pectoral$trailing[, 3] <- p[, 5]
    pectoral$pivot <- (pectoral$leading[1, ] + pectoral$trailing[1, ])/2
    foil <- function(id, role = "attached") {
        out <- .anatomyFoil(id, c[[id]], "xz", role, 4L)
        out
    }
    pelvic <- .anatomyFoil("pelvic", c$pelvic, "xz", "attached", 4L)
    spec <- .anatomySpec("Smooth hammerhead", "Sphyrna zygaena", body, c(list(head), .anatomyPair(pectoral),
        list(foil("dorsal"), foil("second_dorsal")), .anatomyPair(pelvic), list(foil("anal"), foil("caudal_upper",
            "terminal"), foil("caudal_lower", "terminal"))), sections = 10L, pivot = c(0, 0, 0), tip = -21)
    spec$body.angles <- c(0, pi/4, pi/2, 3 * pi/4, pi, 7 * pi/6, 4 * pi/3, 3 * pi/2, 5 * pi/3, 11 * pi/6)
    spec$appearance <- list(smooth = TRUE, cull = TRUE, fit = "legacy", eye.cex = 0.85, compass = c(1.12,
        1.1, 0.18), dorsal = "#65695e", ventral = "#e2e5dc")
    spec$surface <- "hammerhead"
    spec
}


# .anatomyHammerhead
.anatomyHammerhead <- function() .referenceHammerheadSpec()

# Manta -----------------------------------------------------------------


# .mantaCountershading
.mantaCountershading <- function(geometry, belly) {
    f <- do.call(rbind, geometry$faces)
    v <- geometry$vertices
    e <- v[f[, 2], ] - v[f[, 1], ]
    g <- v[f[, 3], ] - v[f[, 1], ]
    n <- cbind(e[, 2] * g[, 3] - e[, 3] * g[, 2], e[, 3] * g[, 1] - e[, 1] * g[, 3], e[, 1] * g[, 2] -
        e[, 2] * g[, 1])
    nz <- n[, 3]/pmax(sqrt(rowSums(n^2)), 1e-12)
    u <- pmin(1, pmax(0, (nz + 0.03)/0.28))
    u <- u * u * (3 - 2 * u)
    body <- geometry$face.region == "body"
    belly[body] <- pmax(belly[body], u[body])
    wing <- geometry$face.region %in% c("wing_right", "wing_left")
    span <- abs(rowMeans(cbind(v[f[, 1], 2], v[f[, 2], 2], v[f[, 3], 2])))
    rim <- pmin(1, pmax(0, (span - 38)/10.15))
    belly[wing] <- u[wing] * (1 - 0.65 * rim[wing]^2)
    pelvic <- geometry$face.region %in% c("pelvic_right", "pelvic_left")
    belly[pelvic] <- u[pelvic]
    cephalic <- geometry$face.region %in% c("cephalic_right", "cephalic_left")
    belly[cephalic] <- 0.8 + 0.2 * u[cephalic]
    belly
}


# .compileMantaSurface
.compileMantaSurface <- function(geometry) {
    requests <- list()
    add <- function(origin, direction, kind, group = 0L, regions) {
        n <- nrow(origin)
        requests[[length(requests) + 1L]] <<- list(origin = origin, direction = matrix(direction, n,
            3, byrow = TRUE), kind = rep(kind, n), group = rep(group, n), regions = regions)
    }
    for (side in c(1, -1)) add(cbind(50 - 8, side * 20, 0.1), c(0, -side, 0), "eye", regions = "body")
    for (side in c(1, -1)) for (i in seq_len(5L)) {
        y <- seq(c(6.4, 6, 5.4, 4.7, 3.9)[i], c(11, 10.6, 10, 9, 7.9)[i], length.out = 9)
        s <- c(12, 15, 18, 21, 24)[i] + 0.5 * sin(seq(0, pi, length.out = 9))
        add(cbind(50 - s, side * y, 30), c(0, 0, -1), "line", i + if (side == 1)
            0L
        else 5L, c("body", "wing_right", "wing_left"))
    }
    y <- seq(-7.8, 7.8, length.out = 25)
    add(cbind(60, y, 2.8 - 0.45 * (1 - (y/7.8)^2)), c(-1, 0, 0), "line", 11L, "body")
    .attachOverlaySurfaceRays(geometry, requests)
}


# .referenceMantaControls
.referenceMantaControls <- function() {
    list(body = structure(c(5.7, 8.5, 14, 22, 30, 37, 43, 47, 9.5, 10.7, 10, 8, 6.4, 4, 1.7, 1.1, -0.664055802022558,
        -2.19728414253175, -3.79030547432293, -3.89325822207709, -2.5357739291739, -0.505185030638435,
        1.7918105231566, 0.458127641389515, 4.57193413576624, 3.31011408624253, 4.62844772906065, 5.9389245119406,
        5.45410762978483, 4.78466629507253, 4.97780865440851, 5.43455951300959), dim = c(8L, 4L)), wing = structure(c(5.5,
        8, 11, 14, 18, 22, 27, 32, 37, 42, 46, 48.15, 40.8231322447702, 42.9342738435116, 40.5776183466592,
        38.5583501175325, 34.7610871035066, 31.8989471195861, 28.8555810287836, 25.5855684283523, 22.2556529411468,
        18.8748422644357, 15.4626927834264, 12.0897010629095, 1.22128304385939, 3.13756485872541, 4.9386343153791,
        6.58261197408394, 8.57889835515128, 9.90836551901543, 11.0955634347362, 11.827382613402, 12.3731053106409,
        12.4768400669828, 11.9914948994659, 11.4629529262213, -0.782655663740737, 0.182273573888782,
        1.03368795772648, 0.320809231102519, 0.124072013142872, -0.163871831045509, -0.536161393860132,
        -0.687381795139589, -0.683818466666106, -0.435036684146724, -0.158485547419857, 0.0611341984484317,
        5.11736142213575, 4.81610249727743, 4.39304930131013, 3.94149031472582, 2.95005040550614, 2.16898856443825,
        1.04343427929475, 0.2958401438935, -0.0601737210066506, -0.0773653658805896, 0.0825008981620784,
        0.111987081998958, 5.26971465045404, 5.15162822061787, 4.53099556127958, 3.32660491583386, 2.80582207812509,
        2.16577136580149, 1.78990247624435, 1.38647229874857, 1.07186050940424, 0.805178277119473, 0.500508666087363,
        0.0831433838278772), dim = c(12L, 6L)), cephalic = structure(c(0.06, 1.2, 3, 5.2, 7.2, 2.03031723579954,
        3.49349555472237, 6.02535335516836, 8.21178782206716, 7.85354191172936, 0.523711170582591, 2.6814340757554,
        2.8619944416757, 1.39146245236931, 0.846288821359684, 2.61824377891994, 1.92748255447224, 0.647924908748249,
        -0.296579647542738, 2.49364972005591, 3.01587124329962, 3.95995143477265, 4.97223239538076, 4.66161949628053,
        4.00318269325172), dim = c(5L, 5L)), tail = structure(c(45, 50, 58, 72, 88, 99.98, 1.04956471907023,
        0.60844621261177, 0.297097997444426, 0.139919202843488, 0.0982176661152895, 0.0178928047900267,
        1.41368004091765, 3.15407570356697, 4.65888740487215, 5.07656969110795, 5.30348216912557, 5.57023670256646,
        4.36878181030899, 4.83722940038823, 5.32913757205721, 5.43994536474592, 5.54979144395925, 5.57549615252539),
        dim = c(6L, 4L)), dorsal = structure(c(2.6, 1, -0.72, 3.96038942130372, 4.0558728567007, 0.650961148389271,
        1.05068896129431, 0.494771563471673, 0.3411992360532, 0, 0, 0, 0.542705065418586, 0.247559304693862,
        0.0440710253427455), dim = c(3L, 5L)), pelvic = structure(c(0.65, 2.1, 3.55, 3.68, 7.34097471071526,
        7.64985298824944, 7.59332230703071, 4.00590822585587, -0.367153310067604, -0.846735727581166,
        0.172772895337516, 3.10063244786184, 4.81961035990219, 4.31401417598966, 4.31416439664583, 4.49967225160567,
        1.11265026749026, 1.67564072095335, 1.67777856710555, 0.25489938406667), dim = 4:5), wing_vertices = structure(c(40.8231322447702,
        30.9226699445425, 11.1217453440871, 1.22128304385939, 11.1217453440871, 30.9226699445425, 42.9342738435116,
        32.9850965973151, 13.086742104922, 3.13756485872541, 13.086742104922, 32.9850965973151, 40.5776183466592,
        31.6678723388392, 13.8483803231991, 4.9386343153791, 13.8483803231991, 31.6678723388392, 38.5583501175325,
        30.5644155816704, 14.5765465099461, 6.58261197408395, 14.5765465099461, 30.5644155816704, 34.7610871035066,
        28.2155399164178, 15.1244455422401, 8.57889835515128, 15.1244455422401, 28.2155399164178, 31.8989471195861,
        26.4013017194434, 15.4060109191581, 9.90836551901543, 15.4060109191581, 26.4013017194434, 28.8555810287836,
        24.4155766302718, 15.535567833248, 11.0955634347362, 15.535567833248, 24.4155766302718, 25.5855684283523,
        22.1460219746148, 15.2669290671396, 11.827382613402, 15.2669290671396, 22.1460219746148, 22.2556529411468,
        19.7850160335204, 14.8437422182674, 12.3731053106409, 14.8437422182674, 19.7850160335204, 18.8748422644357,
        17.2753417150725, 14.076340616346, 12.4768400669828, 14.076340616346, 17.2753417150725, 15.4626927834264,
        14.5948933124363, 12.8592943704561, 11.9914948994659, 12.8592943704561, 14.5948933124363, 12.0897010629095,
        11.9330140287375, 11.6196399603934, 11.4629529262213, 11.6196399603934, 11.9330140287375, 5.5,
        4.2875, 1.8625, 0.65, 1.8625, 4.2875, 8, 8, 8, 8, 8, 8, 11, 11, 11, 11, 11, 11, 14, 14, 14, 14,
        14, 14, 18, 18, 18, 18, 18, 18, 22, 22, 22, 22, 22, 22, 27, 27, 27, 27, 27, 27, 32, 32, 32, 32,
        32, 32, 37, 37, 37, 37, 37, 37, 42, 42, 42, 42, 42, 42, 46, 46, 46, 46, 46, 46, 48.15, 48.15,
        48.15, 48.15, 48.15, 48.15, -1.98506275330083, 3.88836911248254, 4.3885693319987, 5.41466641213239,
        1.67065302558044, -3.15173962037356, -0.931056747141503, 1.96573303724649, 4.29424543389787,
        5.18674279085146, 1.32071485122204, -2.73458412273076, 2.65658209473899, 3.03060399525208, 4.06829131636449,
        4.76964056186013, 1.0789719840167, -1.98241458040591, 0.364270641390669, 4.06621517981826, 3.80286721904024,
        4.22552509364528, 0.874245163489321, -1.24361148676909, -0.0694952829246894, 3.2488648463114,
        3.32312410111648, 3.21344634101096, 0.701773860594279, -0.704183530890149, -0.298299357275851,
        2.23057137538022, 2.63555138542301, 2.15795591578511, 0.327269587818406, -0.697231281527166,
        -0.79540505034303, 1.33531866949638, 1.70179522852673, 0.99740813958427, -0.290664835889169,
        -0.914300404348138, -0.861492541977791, 0.665427977333798, 1.00220760299313, 0.169167317742693,
        -0.76462407269303, -1.1212625250844, -0.656836942775335, 0.284088830828533, 0.610111730909485,
        -0.101921275112072, -0.855980790572351, -1.17007149388203, -0.420892721771824, 0.242028826874051,
        0.425375340245303, -0.0975423836354045, -0.586804512309328, -0.868008045578863, -0.170698291398905,
        0.248711407324165, 0.301493622906515, 0.0989942287891734, -0.232516518851648, -0.416151066056292,
        0.0653440150464767, 0.185122582152972, 0.213731253247955, 0.113140697414343, -0.00696945554596368,
        -0.0341311142518508), dim = c(72L, 3L)))
}


# .referenceMantaSpec
.referenceMantaSpec <- function() {
    c <- .referenceMantaControls()
    body <- c$body
    z <- (body[, 3] + body[, 4])/2
    body <- .anatomyBody(body[, 1], body[, 2], z - body[, 3], body[, 4] - z, z)
    w <- c$wing
    wing <- .anatomyFoil("wing", cbind(w[, 1:3], (w[, 4] + w[, 5])/2, w[, 6]), role = "flipper", sections = 6L)
    wing$section.vertices <- c$wing_vertices
    wing$leading <- c$wing_vertices[seq(1, nrow(c$wing_vertices), 6), , drop = FALSE]
    wing$trailing <- c$wing_vertices[seq(4, nrow(c$wing_vertices), 6), , drop = FALSE]
    wing$pivot <- c(22, 5.5, 1)
    h <- c$cephalic[order(c$cephalic[, 1], decreasing = TRUE), ]
    mid <- (h[, 4] + h[, 5])/2
    horn <- list(id = "cephalic_right", kind = "loft", axis = "x", part = "body", role = "rigid", joint.x = NA_real_,
        sections = 6L, centreline = h[, 2], pivot = c(50 - h[1, 1], h[1, 2], mid[1]), stations = .speciesBody(50 -
            h[, 1], h[, 3], mid - h[, 4], h[, 5] - mid, mid))
    left <- horn
    left$id <- "cephalic_left"
    left$centreline <- -left$centreline
    left$pivot[2] <- -left$pivot[2]
    t <- c$tail[order(c$tail[, 1], decreasing = TRUE), ]
    mid <- (t[, 3] + t[, 4])/2
    tail <- list(id = "tail", kind = "loft", part = "body", role = "rigid", joint.x = NA_real_, sections = 6L,
        pivot = c(5, 0, tail(mid, 1)), stations = .speciesBody(50 - t[, 1], t[, 2], pmax(0.003, mid -
            t[, 3]), pmax(0.003, t[, 4] - mid), mid))
    dorsal <- .anatomyFoil("dorsal", c$dorsal, "xz", "rigid", 4L)
    pelvic <- .anatomyFoil("pelvic", c$pelvic, "xy", "rigid", 4L)
    spec <- .anatomySpec("Manta ray", "Mobula alfredi / Mobula birostris", body, c(.anatomyPair(wing),
        list(horn, left, tail, dorsal), .anatomyPair(pelvic)), type = "flippers", sections = 8L, pivot = wing$pivot,
        tip = max(w[, 1]))
    spec$appearance <- list(smooth = TRUE, cull = TRUE, fit = "legacy", eye.cex = 0.65, compass = c(1.12,
        1.1, 0.18), dorsal = "#4a616b", ventral = "#e4e7df")
    spec$surface <- "manta"
    spec
}


# .anatomyManta
.anatomyManta <- function() .referenceMantaSpec()

# Devil ray -------------------------------------------------------------


# .devilRayCountershading
.devilRayCountershading <- function(geometry, belly) {
    f <- do.call(rbind, geometry$faces)
    v <- geometry$vertices
    e <- v[f[, 2], ] - v[f[, 1], ]
    g <- v[f[, 3], ] - v[f[, 1], ]
    n <- cbind(e[, 2] * g[, 3] - e[, 3] * g[, 2], e[, 3] * g[, 1] - e[, 1] * g[, 3], e[, 1] * g[, 2] -
        e[, 2] * g[, 1])
    nz <- n[, 3]/pmax(sqrt(rowSums(n^2)), 1e-12)
    u <- pmin(1, pmax(0, (nz + 0.03)/0.28))
    u <- u * u * (3 - 2 * u)
    body <- geometry$face.region == "body"
    belly[body] <- pmax(belly[body], u[body])
    wing <- geometry$face.region %in% c("wing_right", "wing_left")
    span <- abs(rowMeans(cbind(v[f[, 1], 2], v[f[, 2], 2], v[f[, 3], 2])))
    tip <- max(abs(v[geometry$region %in% c("wing_right", "wing_left"), 2]))
    rim <- pmin(1, pmax(0, (span/tip - 0.75)/0.25))
    belly[wing] <- u[wing] * (1 - 0.35 * rim[wing]^2)
    paired <- geometry$face.region %in% c("pelvic_right", "pelvic_left", "cephalic_right", "cephalic_left")
    belly[paired] <- u[paired]
    belly
}


# .compileDevilRaySurface
.compileDevilRaySurface <- function(geometry) {
    requests <- list()
    add <- function(origin, direction, kind, group = 0L, regions) {
        n <- nrow(origin)
        requests[[length(requests) + 1L]] <<- list(origin = origin, direction = matrix(direction, n,
            3, byrow = TRUE), kind = rep(kind, n), group = rep(group, n), regions = regions)
    }
    for (side in c(1, -1)) add(cbind(50 - 9.8, side * 20, -1.3), c(0, -side, 0), "eye", regions = "body")
    for (side in c(1, -1)) for (i in seq_len(5L)) {
        y <- seq(c(4, 3.8, 3.6, 3.3, 3)[i], c(8.2, 8, 7.6, 7.2, 6.8)[i], length.out = 9)
        s <- c(13, 16, 19, 22, 25)[i] + 0.4 * sin(seq(0, pi, length.out = 9))
        add(cbind(50 - s, side * y, 30), c(0, 0, -1), "line", i + if (side == 1)
            0L
        else 5L, c("body", "wing_right", "wing_left"))
    }
    y <- seq(-4.6, 4.6, length.out = 25)
    s <- 7.3 + 0.6 * (1 - (y/4.6)^2)
    add(cbind(50 - s, y, 30), c(0, 0, -1), "line", 11L, "body")
    .attachOverlaySurfaceRays(geometry, requests)
}


# .referenceDevilRayControls
.referenceDevilRayControls <- function() list(body = matrix(c(5.776892153, 2.484684797, 1.552927998,
    1.242342398, 6.429121912, 5.590540793, 3.261148796, 1.708220798, 9.224392309, 7.267703031, 5.435247993,
    2.080923517, 12.33024831, 7.547230071, 5.901126393, 2.143040637, 20.0948883, 6.957117432, 4.969369594,
    1.801396478, 29.10187068, 6.522297592, 3.727027195, 1.521869438, 37.17709628, 6.211711992, 2.329391997,
    1.211283839, 43.69939387, 5.745833593, 1.304459518, 0.8696396789, 47.89229946, 2.484684797, 0.6211711992,
    0.4969369594), ncol = 4, byrow = TRUE), wing = matrix(c(6.12226334, 37.0485805, 4.126506935, 0, 3.2,
    8.111998925, 37.56104674, 8.56788101, 0.15, 2.5, 10.71396084, 35.4956525, 10.66433381, 0.35, 1.95,
    14.54037543, 33.46131682, 13.02478436, 0.55, 1.5, 19.13207294, 31.10086626, 14.88829796, 0.75, 1.15,
    23.72377044, 28.22794947, 16.08405252, 0.95, 0.8, 28.31546795, 24.87362499, 16.2548746, 1.1, 0.55,
    32.14188253, 21.44165411, 15.68029124, 1.25, 0.32, 35.2030142, 18.28921028, 14.51559524, 1.35, 0.18,
    36.96316491, 16.13064036, 13.52172132, 1.4, 0.11, 37.88150442, 14.51559524, 13.1490186, 1.43, 0.06,
    38.14935344, 13.97207044, 13.25772356, 1.45, 0.025), ncol = 5, byrow = TRUE), cephalic = matrix(c(0,
    4.348198395, 0.07764639991, 0, 0.1, 1.615045118, 4.588902234, 0.5978772793, 0, 0.65, 3.944437115,
    4.845135354, 0.9938739188, 0, 1.2, 5.342072314, 4.487961915, 1.521869438, 0, 1.6, 8.13734271, 6.646531832,
    0.6832883192, 0, 1.25), ncol = 5, byrow = TRUE), tail.root = 47.8922994622, disc.width = 76.5282917476,
    tail.length = 52.1077005378)


# .referenceDevilRaySpec
.referenceDevilRaySpec <- function() {
    c <- .referenceDevilRayControls()
    body <- .anatomyBody(c$body[, 1], c$body[, 2], c$body[, 3], c$body[, 4])
    wing <- .anatomyFoil("wing", c$wing, role = "flipper", sections = 6L)
    wing$pivot <- c(50 - 13, c$wing[1, 1], 0)
    mesh <- .speciesFoil(wing)
    wing$section.vertices <- mesh$vertices[seq_len(nrow(c$wing) * 6L), ]
    root <- 1:6
    chord <- (wing$section.vertices[root, 1] - wing$trailing[1, 1])/(wing$leading[1, 1] - wing$trailing[1,
        1])
    wing$section.vertices[root, 2] <- 0.6 + chord * (c$wing[1, 1] - 0.6)
    wing$leading <- wing$section.vertices[seq(1, nrow(wing$section.vertices), 6), , drop = FALSE]
    wing$trailing <- wing$section.vertices[seq(4, nrow(wing$section.vertices), 6), , drop = FALSE]
    h <- c$cephalic[nrow(c$cephalic):1, , drop = FALSE]
    horn <- list(id = "cephalic_right", kind = "loft", axis = "x", part = "body", role = "rigid", joint.x = NA_real_,
        sections = 6L, centreline = h[, 2], pivot = c(50 - h[1, 1], h[1, 2], 0), stations = .speciesBody(50 -
            h[, 1], h[, 3], h[, 5], z = h[, 4]))
    left <- horn
    left$id <- "cephalic_left"
    left$centreline <- -left$centreline
    left$pivot[2] <- -left$pivot[2]
    root <- c$tail.root
    tail <- list(id = "tail", kind = "loft", part = "body", role = "rigid", joint.x = NA_real_, sections = 6L,
        pivot = c(50 - root, 0, 0.2), stations = .speciesBody(50 - c(100, 90, 76, 63, 53, root), c(0.012,
            0.04, 0.075, 0.14, 0.24, 0.45), c(0.012, 0.04, 0.075, 0.14, 0.24, 0.45), z = 0.2))
    dorsal <- .anatomyFoil("dorsal", rbind(c(-0.35, 6.9, 2.1, 0, 0.52), c(-2.5, 6, 2.4, 0, 0.3), c(-3.7,
        3.2, 3, 0, 0.025)), plane = "xz", sections = 4L)
    pelvic <- .anatomyFoil("pelvic", rbind(c(0.55, 3.9, -0.15, 0.4, 0.6), c(1.7, 3.6, -0.05, 0.6, 0.32),
        c(2.9, 1.9, 0.8, 0.65, 0.035)), sections = 4L)
    spec <- .anatomySpec("Sicklefin devil ray", "Mobula tarapacana", body, c(.anatomyPair(wing), list(horn,
        left, tail, dorsal), .anatomyPair(pelvic)), type = "flippers", sections = 8L, pivot = wing$pivot,
        tip = max(c$wing[, 1]))
    spec$body.transform <- function(p) {
        front <- abs(p[, 1] - max(body[, 1])) < 1e-08
        p[front, 1] <- p[front, 1] + 0.27 * (p[front, 2]/body[nrow(body), 2])^2
        p
    }
    spec$appearance <- list(smooth = TRUE, cull = TRUE, fit = "legacy", eye.cex = 0.75, compass = c(1.12,
        1.1, 0.18), dorsal = "#696c4a", ventral = "#e5e5d3")
    spec$surface <- "devil_ray"
    spec
}


# .anatomyDevilRay
.anatomyDevilRay <- function() .referenceDevilRaySpec()

# Green sea turtle ------------------------------------------------------


# .referenceTurtleControls
.referenceTurtleControls <- function() list(body = matrix(c(20, 4.25648728, -5.500508156, -3.829682975,
    -4.330930529, 22, 8.014097215, -21.52069094, -1.149129293, -7.260597787, 28, 19.01125168, -24.43777069,
    0.3502234367, -7.086174802, 36, 25.33586509, -24.12175248, 0.8008642186, -6.675920792, 46, 29.0797779,
    -25.07143967, 1.099675462, -6.751659076, 56, 30.07371631, -24.39873805, 0.4387485454, -7.012497433,
    66, 29.09827593, -22.86309614, 0.9713280319, -6.178999219, 76, 25.47575474, -20.60484372, 0.9917000686,
    -5.487263068, 85, 20.74031912, -16.70544192, 0.8682194885, -4.403878933, 91, 15.53199804, -14.2731675,
    -2.030916472, -5.703591779, 96, 7.952552633, -11.02582126, -5.598612741, -7.226775298, 99, 5.347664961,
    -10.11670695, -8.7814853, -9.182051796, 101.05, 2.361428842, -10.24427192, -10.01480833, -10.08364741),
    ncol = 5, byrow = TRUE), body_vertices = matrix(c(30, 4.25648728, -5.500508156, 30, 0.9789423916,
    -3.925439314, 30, 3.063885508e-17, -3.830560053, 30, -0.9789423916, -3.925439314, 30, -4.25648728,
    -5.500508156, 30, -3.686226116, -4.915719343, 30, -2.12824364, -5.343814466, 30, -7.819040285e-16,
    -5.500508156, 30, 2.12824364, -5.343814466, 30, 3.686226116, -4.915719343, 28, 8.014097215, -16.1072292,
    28, 6.542114724, -4.550765141, 28, 3.751593518e-16, -1.133780616, 28, -6.542114724, -4.550765141,
    28, -8.014097215, -16.1072292, 28, -6.940411777, -14.39064436, 28, -6.597196926, -18.68727805, 28,
    -2.600197604e-15, -21.41541427, 28, 6.597196926, -18.68727805, 28, 6.940411777, -14.39064436, 22,
    19.01125168, -11.30198704, 22, 17.56410632, -4.2403884, 22, 4.462353892e-16, 0.2014021471, 22, -17.56410632,
    -4.2403884, 22, -19.01125168, -11.30198704, 22, -15.59226084, -16.0883708, 22, -8.077778337, -21.07729729,
    22, -3.187480526e-15, -24.43801061, 22, 8.077778337, -21.07729729, 22, 15.59226084, -16.0883708,
    14, 25.33586509, -9.682265306, 14, 23.4072872, -3.814679041, 14, 4.548658754e-16, 0.752602692, 14,
    -23.4072872, -3.814679041, 14, -25.33586509, -9.682265306, 14, -17.91806144, -17.02091839, 14, -9.3330885,
    -22.84130427, 14, -3.357854653e-15, -24.95523014, 14, 9.3330885, -22.84130427, 14, 17.91806144, -17.02091839,
    4, 29.0797779, -8.554404768, 4, 15.7968009, -0.2084099006, 4, 4.505606882e-16, 0.6065553657, 4, -15.7968009,
    -0.2084099006, 4, -29.0797779, -8.554404768, 4, -18.85387396, -17.63694828, 4, -9.523949991, -23.24762435,
    4, -3.437010509e-15, -25.46187284, 4, 9.523949991, -23.24762435, 4, 18.85387396, -17.63694828, -6,
    30.07371631, -7.2510798, -6, 14.72747071, -0.9121793255, -6, 4.291393405e-16, -0.004120147964, -6,
    -14.72747071, -0.9121793255, -6, -30.07371631, -7.2510798, -6, -17.70910793, -17.23685566, -6, -8.913732461,
    -22.45153494, -6, -3.217537106e-15, -24.52795364, -6, 8.913732461, -22.45153494, -6, 17.70910793,
    -17.23685566, -16, 29.09827593, -8.337629539, -16, 13.79474988, -0.4650267278, -16, 3.639486176e-16,
    -0.2352672329, -16, -13.79474988, -0.4650267278, -16, -29.09827593, -8.337629539, -16, -16.23416947,
    -15.55180133, -16, -8.242276091, -20.45504018, -16, -3.093080933e-15, -23.01694757, -16, 8.242276091,
    -20.45504018, -16, 16.23416947, -15.55180133, -26, 25.47575474, -8.182859707, -26, 11.22590204, -0.8373421923,
    -26, 3.7276307e-16, 0.6004198507, -26, -11.22590204, -0.8373421923, -26, -25.47575474, -8.182859707,
    -26, -14.08008542, -13.61640417, -26, -7.201365338, -17.96039372, -26, -2.79615716e-15, -20.70883368,
    -26, 7.201365338, -17.96039372, -26, 14.08008542, -13.61640417, -35, 20.74031912, -8.707837543, -35,
    19.16155633, -2.386334213, -35, 2.850394125e-16, 0.2511679376, -35, -19.16155633, -2.386334213, -35,
    -20.74031912, -8.707837543, -35, -12.44562065, -11.58936137, -35, -5.785231047, -14.42419304, -35,
    -2.379851449e-15, -17.35918681, -35, 5.785231047, -14.42419304, -35, 12.44562065, -11.58936137, -41,
    15.53199804, -9.357413108, -41, 14.34969508, -4.298119786, -41, 2.28024692e-16, -1.979665954, -41,
    -14.34969508, -4.298119786, -41, -15.53199804, -9.357413108, -41, -9.150777272, -10.9867955, -41,
    -4.367957468, -13.26911604, -41, -1.572541414e-15, -14.26410852, -41, 4.367957468, -13.26911604,
    -41, 9.150777272, -10.9867955, -46, 7.952552633, -9.20922931, -46, 7.347200609, -6.603704462, -46,
    4.869534064e-16, -5.598612741, -46, -7.347200609, -6.603704462, -46, -7.952552633, -9.20922931, -46,
    -4.879720209, -10.04408307, -46, -1.990116515, -10.67375822, -46, -6.860764225e-16, -10.96160139,
    -46, 1.990116515, -10.67375822, -46, 4.879720209, -10.04408307, -49, 5.347664961, -9.43281905, -49,
    4.940598204, -9.028761635, -49, 3.274500389e-16, -8.7814853, -49, -4.940598204, -9.028761635, -49,
    -5.347664961, -9.43281905, -49, -1.514415916, -10.05640023, -49, -0.4987404337, -10.04589557, -49,
    -1.481219012e-16, -9.98838987, -49, 0.4987404337, -10.04589557, -49, 1.514415916, -10.05640023, -51.05,
    2.361428842, -10.17604597, -51.05, 2.181675774, -10.05730383, -51.05, 1.445958136e-16, -10.01480833,
    -51.05, -2.181675774, -10.05730383, -51.05, -2.361428842, -10.17604597, -51.05, -2.045057366, -10.16395966,
    -51.05, -1.180714421, -10.22275231, -51.05, -4.337874408e-16, -10.24427192, -51.05, 1.180714421,
    -10.22275231, -51.05, 2.045057366, -10.16395966), ncol = 3, byrow = TRUE), head = matrix(c(0.04,
    0.156259747, -12.1182665, -11.8400192, 1.5, 2.228143177, -15.83949568, -8.854894771, 4.5, 4.544648319,
    -17.94092451, -7.471270952, 8, 5.349375817, -18.72225225, -6.341319451, 12, 5.881671879, -18.52075847,
    -6.41276736, 16, 6.070662254, -17.46824449, -5.966348998, 21, 8.683740928, -17.88996148, -5.494591857),
    ncol = 4, byrow = TRUE), foreflipper = matrix(c(10, 29, 17, -5, -4, 1.847520861, 15, 30.00805808,
    11.41413084, -4.682538913, 0.4753126902, 11.16976667, 22, 33.78515112, 10.84041, -1.446762203, -3.996006119,
    6.121164934, 29, 32.03130243, 18.69439092, -0.8336884444, -3.744754344, 3.360099657, 36, 28.46546217,
    15.90424564, -0.557942919, -1.654480019, 2.386172796, 43, 21.40743768, 11.55393204, -0.2576020083,
    0.3868479928, 2.240560223, 46.6, 15.18168202, 14.67354286, 0.3092553699, 0.3152300966, 0.6386226943),
    ncol = 6, byrow = TRUE), hindflipper = matrix(c(7, -34, -45, -4, -3, 1.2, 10, -30.74037474, -52.59285152,
    -0.2518121936, -1.780878904, 6.980393773, 14, -31.5099433, -55.50198324, -4.332217561, -0.9196485658,
    4.313305448, 18, -41.48726144, -56.55698853, -0.3060240427, -0.5928057251, 2.013454363, 21, -48.11406038,
    -56.16225265, 0.2209664687, -0.3089582686, 0.8173500427, 22.3, -52.90745886, -54.45684536, 0.0857387252,
    -0.08285249919, 0.05295460901), ncol = 6, byrow = TRUE), fore_vertices = matrix(c(29, 10, -5, 26,
    10, -3.15, 20, 10, -2.65, 17, 10, -4, 20, 10, -5.85, 26, 10, -6.35, 30.00805808, 15, -4.682538913,
    24.39262556, 15, -1.512610239, 15.5245845, 15, 0.3494266997, 11.41413084, 15, 0.4753126902, 16.06261265,
    15, -11.81892894, 24.55667352, 15, -10.82553652, 33.78515112, 22, -1.446762203, 28.93479591, 22,
    1.583109367, 16.57659528, 22, 0.8981571588, 10.84041, 22, -3.996006119, 16.39856997, 22, -8.110302389,
    26.76494563, 22, -6.979801928, 32.03130243, 29, -0.8336884444, 28.7330979, 29, 1.217743595, 24.23637751,
    29, -0.7515604275, 18.69439092, 29, -3.744754344, 21.79573422, 29, -4.870984201, 27.92241124, 29,
    -3.986687923, 28.46546217, 36, -0.557942919, 23.83789034, 36, -0.6982974795, 21.37093615, 36, -1.106365383,
    15.90424564, 36, -1.654480019, 19.47970417, 36, -2.817706752, 24.79936979, 36, -2.773631018, 21.40743768,
    43, -0.2576020083, 17.52450756, 43, 0.04932500216, 14.65469908, 43, 0.4506034736, 11.55393204, 43,
    0.3868479928, 14.8841639, 43, -1.305268548, 18.4654187, 43, -1.504436059, 15.18168202, 46.6, 0.3092553699,
    14.95057366, 46.6, 0.6314788221, 14.81157262, 46.6, 0.8082686666, 14.67354286, 46.6, 0.3152300966,
    14.78305302, 46.6, 0.3132966858, 15.07354701, 46.6, 0.3106847646), ncol = 3, byrow = TRUE), hind_vertices = matrix(c(-34,
    7, -4, -39.5, 7, -2.3, -45, 7, -3, -39.5, 7, -4.7, -30.74037474, 10, -0.2518121936, -41.66661313,
    10, -0.289779941, -52.59285152, 10, -1.780878904, -41.66661313, 10, -6.079431488, -31.5099433, 14,
    -4.332217561, -43.50596327, 14, 1.101727667, -55.50198324, 14, -0.9196485658, -43.50596327, 14, -3.720765044,
    -41.48726144, 18, -0.3060240427, -49.02212499, 18, 0.1261954709, -56.55698853, 18, -0.5928057251,
    -49.02212499, 18, -2.047256771, -48.11406038, 21, 0.2209664687, -52.13815651, 21, 0.1591665759, -56.16225265,
    21, -0.3089582686, -52.13815651, 21, -0.6339719497, -52.90745886, 22.3, 0.0857387252, -53.68215211,
    22.3, 0.002386779265, -54.45684536, 22.3, -0.08285249919, -53.68215211, 22.3, -0.04808847215), ncol = 3,
    byrow = TRUE))


# .referenceTurtleSpec
.referenceTurtleSpec <- function() {
    controls <- .referenceTurtleControls()
    b <- controls$body
    body <- .anatomyBody(b[, 1], b[, 2], b[, 5] - b[, 3], b[, 4] - b[, 5], b[, 5])
    foil <- function(id, sections, role) {
        p <- controls[[id]]
        out <- .anatomyFoil(id, cbind(p[, 1:3], (p[, 4] + p[, 5])/2, p[, 6]), role = role, sections = sections)
        out$leading[, 3] <- p[, 4]
        out$trailing[, 3] <- p[, 5]
        out$pivot <- (out$leading[1, ] + out$trailing[1, ])/2
        out$section.vertices <- controls[[if (id == "foreflipper")
            "fore_vertices"
        else "hind_vertices"]]
        out
    }
    fore <- foil("foreflipper", 6L, "flipper")
    hind <- foil("hindflipper", 4L, "rigid")
    h <- controls$head
    z <- (h[, 3] + h[, 4])/2
    head <- list(id = "head_neck", kind = "loft", part = "body", role = "rigid", joint.x = NA_real_,
        sections = 8L, pivot = c(29, 0, -11), stations = .anatomyBody(h[, 1], h[, 2], z - h[, 3], h[,
            4] - z, z))
    plastron <- list(id = "plastron", kind = "loft", part = "body", role = "rigid", joint.x = NA_real_,
        sections = 6L, pivot = c(0, 0, 0), stations = .anatomyBody(c(30, 50, 70, 89), c(12, 21, 20, 10),
            rep(0.5, 4), rep(0.4, 4), c(-1, -0.2, -0.3, -2.5)))
    tail <- .speciesFinSpec("tail", rbind(c(-45, -1, -7), c(-50, 0, -7), c(-45, 1, -7)), thickness = 0.5,
        part = "body")
    spec <- .anatomySpec("Green sea turtle", "Chelonia mydas", body, c(.anatomyPair(fore), .anatomyPair(hind),
        list(head, plastron, tail)), type = "flippers", sections = 10L, pivot = fore$pivot, tip = 46.6)
    spec$body.angles <- c(0, pi/8, pi/2, 7 * pi/8, pi, 7 * pi/6, 4 * pi/3, 3 * pi/2, 5 * pi/3, 11 * pi/6)
    rings <- controls$body_vertices
    order <- unlist(lapply(rev(seq_len(nrow(b))), function(i) (i - 1L) * 10L + seq_len(10L)))
    rings <- rings[order, , drop = FALSE]
    spec$body.transform <- function(vertices) {
        n <- nrow(rings)
        vertices[seq_len(n), ] <- rings
        vertices[n + 1L, ] <- colMeans(rings[1:10, , drop = FALSE])
        vertices[n + 2L, ] <- colMeans(tail(rings, 10))
        vertices
    }
    spec$rig$flipper.mode <- "rigid"
    spec$appearance <- list(smooth = TRUE, smooth.faces = rep(TRUE, 690L), cull = TRUE, fit = "legacy",
        eye.cex = 0.85, compass = c(1.12, 1.1, 0.18), dorsal = "#68583b", ventral = "#dfd5a4")
    spec$surface <- "turtle"
    spec
}


# .anatomyTurtle
.anatomyTurtle <- function() .referenceTurtleSpec()


# .turtleCountershading
.turtleCountershading <- function(geometry) {
    f <- do.call(rbind, geometry$faces)
    v <- geometry$vertices
    e <- v[f[, 2], ] - v[f[, 1], ]
    g <- v[f[, 3], ] - v[f[, 1], ]
    n <- cbind(e[, 2] * g[, 3] - e[, 3] * g[, 2], e[, 3] * g[, 1] - e[, 1] * g[, 3], e[, 1] * g[, 2] -
        e[, 2] * g[, 1])
    nz <- n[, 3]/pmax(sqrt(rowSums(n^2)), 1e-12)
    u <- pmin(1, pmax(0, (nz - 0.05)/0.35))
    u <- u * u * (3 - 2 * u)
    body <- which(geometry$face.region == "body")
    paired <- matrix(head(body, -20L), ncol = 2, byrow = TRUE)
    shade <- rowMeans(matrix(u[head(body, -20L)], ncol = 2, byrow = TRUE))
    u[paired[, 1]] <- shade
    u[paired[, 2]] <- shade
    skin <- geometry$face.region %in% c("head_neck", "foreflipper_right", "foreflipper_left", "hindflipper_right",
        "hindflipper_left", "tail")
    u[skin] <- 0.18 + 0.78 * u[skin]
    u[geometry$face.region == "plastron"] <- 1
    u
}


# .compileTurtleSurface
.compileTurtleSurface <- function(spec, geometry) {
    requests <- list()
    add <- function(origin, direction, kind = "line", group = 0L, regions) {
        n <- nrow(origin)
        if (is.null(dim(direction)))
            direction <- matrix(direction, n, 3, byrow = TRUE)
        requests[[length(requests) + 1L]] <<- list(origin = origin, direction = direction, kind = rep(kind,
            n), group = rep(group, n), regions = regions)
    }
    interpolate <- function(p, steps = 3L) {
        do.call(rbind, lapply(seq_len(nrow(p) - 1L), function(i) {
            t <- seq(0, 1, length.out = steps + 1L)
            if (i < nrow(p) - 1L)
                t <- head(t, -1L)
            outer(1 - t, p[i, ]) + outer(t, p[i + 1L, ])
        }))
    }
    traces <- .turtleScuteTraces()
    for (i in seq_len(5L)) {
        p <- interpolate(traces[[i]])
        add(cbind(50 - p[, 1], p[, 2], -80), c(0, 0, 1), group = i, regions = "body")
    }
    for (side in c(1, -1)) for (i in 6:9) {
        p <- interpolate(traces[[i]])
        add(cbind(50 - p[, 1], side * abs(p[, 2]) * 0.97, -80), c(0, 0, 1), group = i + if (side == 1)
            0L
        else 4L, regions = "body")
    }
    for (side in c(1, -1)) add(matrix(c(50 - 4.36452, side * 20, -14.61973), 1, 3), c(0, -side, 0), kind = "eye",
        regions = "head_neck")
    a <- seq(0, pi, length.out = 21)
    s <- 1.8 + 7 * abs(cos(a))
    h <- .referenceTurtleControls()$head
    z <- stats::approx(h[, 1], (h[, 3] + h[, 4])/2, s, rule = 2)$y
    add(cbind(50 - s, 0, z), cbind(0, cos(a), 0.5 * sin(a)), group = 14L, regions = "head_neck")
    fore <- .referenceTurtleControls()$foreflipper
    for (side in c(1, -1)) for (i in 1:3) {
        span <- c(28, 35, 41)[i]
        y <- span + c(-1.7, 0, 1.7, 0, -1.7)
        front <- stats::approx(fore[, 1], fore[, 2], y)$y
        back <- stats::approx(fore[, 1], fore[, 3], y)$y
        x <- (front + back)/2 + c(0, 0.22, 0, -0.22, 0) * (front - back)
        p <- interpolate(cbind(x, side * y), steps = 2L)
        add(cbind(p, -80), c(0, 0, 1), group = 14L + i + if (side == 1)
            0L
        else 3L, regions = paste0("foreflipper_", if (side == 1)
            "right"
        else "left"))
    }
    add(cbind(50 - seq(30, 90, length.out = 25), 0, 40), c(0, 0, -1), group = 21L, regions = c("body",
        "plastron"))
    for (i in 1:5) {
        y <- seq(-1, 1, length.out = 11) * c(12, 19, 22, 20, 12)[i]
        s <- c(34, 45, 58, 72, 85)[i] + 1.5 * abs(y)/max(abs(y))
        add(cbind(50 - s, y, 40), c(0, 0, -1), group = 21L + i, regions = c("body", "plastron"))
    }
    .attachOverlaySurfaceRays(geometry, requests)
}


# .turtleScuteTraces
.turtleScuteTraces <- function() list(matrix(c(23.059724, -1.910454, -22.729569, 30.484797, 8.664905,
    -21.252637, 38.233097, 6.653178, -24.305408, 41.161207, -0.294511, -25.41125, 38.008707, -5.49921,
    -24.371496, 30.237535, -6.995731, -22.790101, 23.059724, -1.910454, -22.729569), ncol = 3, byrow = TRUE),
    matrix(c(38.233097, 6.653178, -24.305408, 45.952726, 8.213331, -23.745237, 55.942951, 4.006572, -24.163292,
        56.844295, -2.297391, -24.298009, 50.846998, -7.085172, -24.177816, 38.008707, -5.49921, -24.371496),
        ncol = 3, byrow = TRUE), matrix(c(55.942951, 4.006572, -24.163292, 63.171978, 7.093735, -21.752961,
        72.760988, 3.750534, -20.83308, 73.047219, -3.519461, -21.021292, 65.718789, -6.958425, -21.412163,
        56.844295, -2.297391, -24.298009), ncol = 3, byrow = TRUE), matrix(c(72.760988, 3.750534, -20.83308,
        76.792332, 6.170928, -18.322817, 83.47853, 3.013475, -16.589515, 84.135295, -2.543195, -16.623336,
        79.017995, -5.992642, -17.657822, 73.047219, -3.519461, -21.021292), ncol = 3, byrow = TRUE),
    matrix(c(83.47853, 3.013475, -16.589515, 88.116338, 2.445157, -15.396949, 91.074533, -0.650913, -13.949463,
        88.011133, -3.998421, -14.509351, 84.135295, -2.543195, -16.623336), ncol = 3, byrow = TRUE),
    matrix(c(38.233097, 6.653178, -24.305408, 42.785106, 15.37908, -19.553092, 39.831402, 23.41905, -13.470774),
        ncol = 3, byrow = TRUE), matrix(c(55.942951, 4.006572, -24.163292, 57.798826, 13.871578, -19.259353,
        58.138331, 26.955013, -9.774723), ncol = 3, byrow = TRUE), matrix(c(72.760988, 3.750534, -20.83308,
        74.99789, 11.438441, -15.57706, 76.255319, 22.10316, -8.899113), ncol = 3, byrow = TRUE), matrix(c(83.47853,
        3.013475, -16.589515, 88.036859, 6.591598, -13.770552, 92.479549, 12.564982, -9.60345), ncol = 3,
        byrow = TRUE))

# Common dolphin --------------------------------------------------------


# .dolphinReferenceFaceRGB
.dolphinReferenceFaceRGB <- function() matrix(c(0.363408, 0.355389, 0.396795, 0.363408, 0.355389, 0.396795,
    0.324932, 0.317459, 0.355355, 0.324932, 0.317459, 0.355355, 0.324932, 0.317459, 0.355355, 0.324932,
    0.317459, 0.355355, 0.363408, 0.355389, 0.396795, 0.363408, 0.355389, 0.396795, 0.281942, 0.267835,
    0.272638, 0.281942, 0.267835, 0.272638, 0.296549, 0.284647, 0.300361, 0.296549, 0.284647, 0.300361,
    0.310348, 0.300025, 0.323546, 0.310348, 0.300025, 0.323546, 0.310348, 0.300025, 0.323546, 0.310348,
    0.300025, 0.323546, 0.296549, 0.284647, 0.300361, 0.296549, 0.284647, 0.300361, 0.281942, 0.267835,
    0.272638, 0.281942, 0.267835, 0.272638, 0.299486, 0.292297, 0.32743, 0.299486, 0.292297, 0.32743,
    0.300578, 0.293564, 0.329356, 0.300578, 0.293564, 0.329356, 0.300578, 0.293564, 0.329356, 0.300578,
    0.293564, 0.329356, 0.299486, 0.292297, 0.32743, 0.299486, 0.292297, 0.32743, 0.272219, 0.262598,
    0.280614, 0.272219, 0.262598, 0.280614, 0.270612, 0.261688, 0.280691, 0.270612, 0.261688, 0.280691,
    0.285136, 0.274864, 0.291362, 0.285136, 0.274864, 0.291362, 0.285136, 0.274864, 0.291362, 0.285136,
    0.274864, 0.291362, 0.270612, 0.261688, 0.280691, 0.270612, 0.261688, 0.280691, 0.272219, 0.262598,
    0.280614, 0.272219, 0.262598, 0.280614, 0.367411, 0.35516, 0.386287, 0.367411, 0.35516, 0.386287,
    0.314832, 0.307163, 0.342047, 0.314832, 0.307163, 0.342047, 0.314832, 0.307163, 0.342047, 0.314832,
    0.307163, 0.342047, 0.367411, 0.35516, 0.386287, 0.367411, 0.35516, 0.386287, 0.364175, 0.347169,
    0.361895, 0.364175, 0.347169, 0.361895, 0.325588, 0.306645, 0.306504, 0.325588, 0.306645, 0.306504,
    0.321017, 0.302695, 0.301931, 0.321017, 0.302695, 0.301931, 0.321017, 0.302695, 0.301931, 0.321017,
    0.302695, 0.301931, 0.325588, 0.306645, 0.306504, 0.325588, 0.306645, 0.306504, 0.364175, 0.347169,
    0.361895, 0.364175, 0.347169, 0.361895, 0.507423, 0.491133, 0.515887, 0.507423, 0.491133, 0.515887,
    0.657311, 0.650342, 0.676249, 0.657311, 0.650342, 0.676249, 0.657311, 0.650342, 0.676249, 0.657311,
    0.650342, 0.676249, 0.507423, 0.491133, 0.515887, 0.507423, 0.491133, 0.515887, 0.421112, 0.396318,
    0.394045, 0.421112, 0.396318, 0.394045, 0.364133, 0.339062, 0.325956, 0.364133, 0.339062, 0.325956,
    0.299584, 0.284974, 0.286053, 0.299584, 0.284974, 0.286053, 0.299584, 0.284974, 0.286053, 0.299584,
    0.284974, 0.286053, 0.364133, 0.339062, 0.325956, 0.364133, 0.339062, 0.325956, 0.421112, 0.396318,
    0.394045, 0.421112, 0.396318, 0.394045, 0.57508, 0.5598, 0.565724, 0.57508, 0.5598, 0.565724, 0.790652,
    0.785712, 0.804933, 0.790652, 0.785712, 0.804933, 0.790652, 0.785712, 0.804933, 0.790652, 0.785712,
    0.804933, 0.57508, 0.5598, 0.565724, 0.57508, 0.5598, 0.565724, 0.332279, 0.314349, 0.313817, 0.332279,
    0.314349, 0.313817, 0.242655, 0.232597, 0.240484, 0.242655, 0.232597, 0.240484, 0.255366, 0.244391,
    0.251379, 0.255366, 0.244391, 0.251379, 0.255366, 0.244391, 0.251379, 0.255366, 0.244391, 0.251379,
    0.242655, 0.232597, 0.240484, 0.242655, 0.232597, 0.240484, 0.332279, 0.314349, 0.313817, 0.332279,
    0.314349, 0.313817, 0.505712, 0.487199, 0.481011, 0.505712, 0.487199, 0.481011, 0.843882, 0.839253,
    0.848888, 0.843882, 0.839253, 0.848888, 0.843882, 0.839253, 0.848888, 0.843882, 0.839253, 0.848888,
    0.505712, 0.487199, 0.481011, 0.505712, 0.487199, 0.481011, 0.218818, 0.21134, 0.223073, 0.218818,
    0.21134, 0.223073, 0.221377, 0.210034, 0.21052, 0.221377, 0.210034, 0.21052, 0.243086, 0.231374,
    0.236051, 0.243086, 0.231374, 0.236051, 0.243086, 0.231374, 0.236051, 0.243086, 0.231374, 0.236051,
    0.221377, 0.210034, 0.21052, 0.221377, 0.210034, 0.21052, 0.218818, 0.21134, 0.223073, 0.218818,
    0.21134, 0.223073, 0.708679, 0.669341, 0.622312, 0.708679, 0.669341, 0.622312, 0.822346, 0.817864,
    0.834901, 0.822346, 0.817864, 0.834901, 0.822346, 0.817864, 0.834901, 0.822346, 0.817864, 0.834901,
    0.708679, 0.669341, 0.622312, 0.708679, 0.669341, 0.622312, 0.54188, 0.50344, 0.457973, 0.54188,
    0.50344, 0.457973, 0.16371, 0.15463, 0.15176, 0.16371, 0.15463, 0.15176, 0.168838, 0.162428, 0.168579,
    0.168838, 0.162428, 0.168579, 0.168838, 0.162428, 0.168579, 0.168838, 0.162428, 0.168579, 0.16371,
    0.15463, 0.15176, 0.16371, 0.15463, 0.15176, 0.54188, 0.50344, 0.457973, 0.54188, 0.50344, 0.457973,
    0.708693, 0.676791, 0.643565, 0.708693, 0.676791, 0.643565, 0.801098, 0.797214, 0.813515, 0.801098,
    0.797214, 0.813515, 0.801098, 0.797214, 0.813515, 0.801098, 0.797214, 0.813515, 0.708693, 0.676791,
    0.643565, 0.708693, 0.676791, 0.643565, 0.685009, 0.633972, 0.565555, 0.685009, 0.633972, 0.565555,
    0.239395, 0.221432, 0.205422, 0.239395, 0.221432, 0.205422, 0.161553, 0.154175, 0.153273, 0.161553,
    0.154175, 0.153273, 0.161553, 0.154175, 0.153273, 0.161553, 0.154175, 0.153273, 0.239395, 0.221432,
    0.205422, 0.239395, 0.221432, 0.205422, 0.685009, 0.633972, 0.565555, 0.685009, 0.633972, 0.565555,
    0.743986, 0.696607, 0.622155, 0.743986, 0.696607, 0.622155, 0.825854, 0.822425, 0.838679, 0.825854,
    0.822425, 0.838679, 0.825854, 0.822425, 0.838679, 0.825854, 0.822425, 0.838679, 0.743986, 0.696607,
    0.622155, 0.743986, 0.696607, 0.622155, 0.62266, 0.575452, 0.517764, 0.62266, 0.575452, 0.517764,
    0.187547, 0.179393, 0.186976, 0.187547, 0.179393, 0.186976, 0.163406, 0.156718, 0.160975, 0.163406,
    0.156718, 0.160975, 0.163406, 0.156718, 0.160975, 0.163406, 0.156718, 0.160975, 0.187547, 0.179393,
    0.186976, 0.187547, 0.179393, 0.186976, 0.62266, 0.575452, 0.517764, 0.62266, 0.575452, 0.517764,
    0.615668, 0.585942, 0.545469, 0.615668, 0.585942, 0.545469, 0.768235, 0.762333, 0.77198, 0.768235,
    0.762333, 0.77198, 0.768235, 0.762333, 0.77198, 0.768235, 0.762333, 0.77198, 0.615668, 0.585942,
    0.545469, 0.615668, 0.585942, 0.545469, 0.383946, 0.368507, 0.360585, 0.383946, 0.368507, 0.360585,
    0.194181, 0.185529, 0.190452, 0.194181, 0.185529, 0.190452, 0.196159, 0.187886, 0.195011, 0.196159,
    0.187886, 0.195011, 0.196159, 0.187886, 0.195011, 0.196159, 0.187886, 0.195011, 0.194181, 0.185529,
    0.190452, 0.194181, 0.185529, 0.190452, 0.383946, 0.368507, 0.360585, 0.383946, 0.368507, 0.360585,
    0.473736, 0.444189, 0.433389, 0.473736, 0.444189, 0.433389, 0.73029, 0.636903, 0.643725, 0.73029,
    0.636903, 0.643725, 0.73029, 0.636903, 0.643725, 0.73029, 0.636903, 0.643725, 0.473736, 0.444189,
    0.433389, 0.473736, 0.444189, 0.433389, 0.369474, 0.359878, 0.367387, 0.369474, 0.359878, 0.367387,
    0.251849, 0.241067, 0.247093, 0.251849, 0.241067, 0.247093, 0.23734, 0.226283, 0.230843, 0.23734,
    0.226283, 0.230843, 0.23734, 0.226283, 0.230843, 0.23734, 0.226283, 0.230843, 0.251849, 0.241067,
    0.247093, 0.251849, 0.241067, 0.247093, 0.369474, 0.359878, 0.367387, 0.369474, 0.359878, 0.367387,
    0.372714, 0.34962, 0.352731, 0.372714, 0.34962, 0.352731, 0.705571, 0.643389, 0.655489, 0.705571,
    0.643389, 0.655489, 0.705571, 0.643389, 0.655489, 0.705571, 0.643389, 0.655489, 0.372714, 0.34962,
    0.352731, 0.372714, 0.34962, 0.352731, 0.348327, 0.341061, 0.35683, 0.348327, 0.341061, 0.35683,
    0.327517, 0.318928, 0.331937, 0.327517, 0.318928, 0.331937, 0.305838, 0.294571, 0.304067, 0.305838,
    0.294571, 0.304067, 0.305838, 0.294571, 0.304067, 0.305838, 0.294571, 0.304067, 0.327517, 0.318928,
    0.331937, 0.327517, 0.318928, 0.331937, 0.348327, 0.341061, 0.35683, 0.348327, 0.341061, 0.35683,
    0.375075, 0.368036, 0.383512, 0.375075, 0.368036, 0.383512, 0.713703, 0.706819, 0.71191, 0.713703,
    0.706819, 0.71191, 0.713703, 0.706819, 0.71191, 0.713703, 0.706819, 0.71191, 0.375075, 0.368036,
    0.383512, 0.375075, 0.368036, 0.383512, 0.263302, 0.257705, 0.283114, 0.263302, 0.257705, 0.283114,
    0.299523, 0.293609, 0.315262, 0.299523, 0.293609, 0.315262, 0.344477, 0.337591, 0.353763, 0.344477,
    0.337591, 0.353763, 0.344477, 0.337591, 0.353763, 0.344477, 0.337591, 0.353763, 0.299523, 0.293609,
    0.315262, 0.299523, 0.293609, 0.315262, 0.263302, 0.257705, 0.283114, 0.263302, 0.257705, 0.283114,
    0.399189, 0.385989, 0.378039, 0.399189, 0.385989, 0.378039, 0.666526, 0.642989, 0.605843, 0.666526,
    0.642989, 0.605843, 0.666526, 0.642989, 0.605843, 0.666526, 0.642989, 0.605843, 0.399189, 0.385989,
    0.378039, 0.399189, 0.385989, 0.378039, 0.29831, 0.292002, 0.319934, 0.29831, 0.292002, 0.319934,
    0.308538, 0.301727, 0.334716, 0.308538, 0.301727, 0.334716, 0.305264, 0.298469, 0.331236, 0.305264,
    0.298469, 0.331236, 0.305264, 0.298469, 0.331236, 0.305264, 0.298469, 0.331236, 0.308538, 0.301727,
    0.334716, 0.308538, 0.301727, 0.334716, 0.29831, 0.292002, 0.319934, 0.29831, 0.292002, 0.319934,
    0.508125, 0.481127, 0.450226, 0.508125, 0.481127, 0.450226, 0.662521, 0.621191, 0.560007, 0.662521,
    0.621191, 0.560007, 0.662521, 0.621191, 0.560007, 0.662521, 0.621191, 0.560007, 0.508125, 0.481127,
    0.450226, 0.508125, 0.481127, 0.450226, 0.356871, 0.341521, 0.348824, 0.356871, 0.341521, 0.348824,
    0.356944, 0.340206, 0.364782, 0.356944, 0.340206, 0.364782, 0.375506, 0.361961, 0.397714, 0.375506,
    0.361961, 0.397714, 0.375506, 0.361961, 0.397714, 0.375506, 0.361961, 0.397714, 0.356944, 0.340206,
    0.364782, 0.356944, 0.340206, 0.364782, 0.356871, 0.341521, 0.348824, 0.356871, 0.341521, 0.348824,
    0.315436, 0.305071, 0.329739, 0.315436, 0.305071, 0.329739, 0.315436, 0.305071, 0.329739, 0.315436,
    0.305071, 0.329739, 0.315436, 0.305071, 0.329739, 0.315436, 0.305071, 0.329739, 0.315436, 0.305071,
    0.329739, 0.315436, 0.305071, 0.329739, 0.315436, 0.305071, 0.329739, 0.315436, 0.305071, 0.329739,
    0.451993, 0.429201, 0.424311, 0.451993, 0.429201, 0.424311, 0.451993, 0.429201, 0.424311, 0.451993,
    0.429201, 0.424311, 0.451993, 0.429201, 0.424311, 0.451993, 0.429201, 0.424311, 0.451993, 0.429201,
    0.424311, 0.451993, 0.429201, 0.424311, 0.451993, 0.429201, 0.424311, 0.451993, 0.429201, 0.424311,
    0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671,
    0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.292592, 0.282806,
    0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157,
    0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.629671, 0.620266, 0.638092, 0.629671,
    0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266,
    0.638092, 0.629671, 0.620266, 0.638092, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157,
    0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592,
    0.282806, 0.305157, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266,
    0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092,
    0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592,
    0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.629671, 0.620266,
    0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092,
    0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.292592, 0.282806, 0.305157, 0.292592,
    0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806,
    0.305157, 0.292592, 0.282806, 0.305157, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092,
    0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671,
    0.620266, 0.638092, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806,
    0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157,
    0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671,
    0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.292592, 0.282806,
    0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157,
    0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592,
    0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806,
    0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157,
    0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592,
    0.282806, 0.305157, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266,
    0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092,
    0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592,
    0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.629671, 0.620266,
    0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092,
    0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.292592, 0.282806, 0.305157, 0.292592,
    0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806,
    0.305157, 0.292592, 0.282806, 0.305157, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092,
    0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671,
    0.620266, 0.638092, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806,
    0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157,
    0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671,
    0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.292592, 0.282806,
    0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157,
    0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.629671, 0.620266, 0.638092, 0.629671,
    0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266,
    0.638092, 0.629671, 0.620266, 0.638092, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157,
    0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592,
    0.282806, 0.305157, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266,
    0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092, 0.629671, 0.620266, 0.638092,
    0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592,
    0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806,
    0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157,
    0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592,
    0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806, 0.305157, 0.292592, 0.282806,
    0.305157, 0.292592, 0.282806, 0.305157, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603,
    0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212,
    0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858,
    0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212,
    0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858,
    0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212,
    0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858,
    0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212,
    0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858,
    0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212,
    0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858,
    0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212,
    0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858,
    0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212,
    0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858,
    0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.276212,
    0.269858, 0.29603, 0.276212, 0.269858, 0.29603, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487,
    0.400081, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081,
    0.367604, 0.359487, 0.400081, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396,
    0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961,
    0.256435, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081,
    0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.254396,
    0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961,
    0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.367604, 0.359487, 0.400081,
    0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.367604,
    0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961,
    0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435,
    0.254396, 0.243961, 0.256435, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.367604,
    0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487,
    0.400081, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435,
    0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.367604,
    0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487,
    0.400081, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.254396, 0.243961, 0.256435,
    0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396,
    0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961,
    0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435,
    0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396,
    0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961,
    0.256435, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081,
    0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.254396,
    0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961,
    0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.367604, 0.359487, 0.400081,
    0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.367604,
    0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961,
    0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435,
    0.254396, 0.243961, 0.256435, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.367604,
    0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487,
    0.400081, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435,
    0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.367604,
    0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487,
    0.400081, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.254396, 0.243961, 0.256435,
    0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396,
    0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487,
    0.400081, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081, 0.367604, 0.359487, 0.400081,
    0.367604, 0.359487, 0.400081, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396,
    0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961,
    0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435,
    0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396,
    0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961,
    0.256435, 0.254396, 0.243961, 0.256435, 0.254396, 0.243961, 0.256435), ncol = 3L, byrow = TRUE)


# .compileDolphinSurface
.compileDolphinSurface <- function(spec, geometry) {
    requests <- list()
    add <- function(origin, direction, kind = "line", group = 0L) {
        n <- nrow(origin)
        if (is.null(dim(direction)))
            direction <- matrix(direction, n, 3, byrow = TRUE)
        requests[[length(requests) + 1L]] <<- list(origin = origin, direction = direction, kind = rep(kind,
            n), group = rep(group, n), regions = "body")
    }
    for (side in c(1, -1)) add(matrix(c(50 - 16.13860295, side * 30, 1.1700419), 1, 3), c(0, -side, 0),
        "eye")
    radial <- function(s, angle, side, group) {
        z <- stats::approx(spec$body[, 1], spec$body[, 5], 50 - s, rule = 2)$y
        a <- angle * pi/180
        add(cbind(50 - s, 0, z), cbind(0, side * cos(a), sin(a)), group = group)
    }
    for (side in c(1, -1)) {
        offset <- if (side == 1)
            0L
        else 1L
        radial(seq(0.7, 19, length.out = 21), seq(2, 25, length.out = 21), side, 1L + offset)
        radial(seq(5, 26, length.out = 17), seq(60, 30, length.out = 17), side, 3L + offset)
        radial(seq(4, 16, length.out = 13), seq(3, 10, length.out = 13), side, 5L + offset)
    }
    a <- seq(0, pi, length.out = 7)
    add(cbind(50 - (20.5 + 0.4 * sin(a)), 0.35 * cos(a), -40), c(0, 0, 1), group = 7L)
    .attachOverlaySurfaceRays(geometry, requests)
}


# .referenceDolphinControls
.referenceDolphinControls <- function() list(body = matrix(c(0.03, 0.1716390532, 4.178211561, 4.497630334,
    2, 1.216413198, 2.176328082, 5.109791161, 5.5, 1.652412691, 1.188294483, 4.803954908, 7.3, 1.988280964,
    -0.3912568275, 4.656275795, 9.5, 2.855704699, -2.713458738, 4.678722718, 13, 4.435455151, -4.729176652,
    5.254094856, 20, 7.076639033, -7.331873751, 7.042141206, 28, 8.24522078, -9.055875247, 8.28208306,
    38, 8.903602896, -9.689851938, 8.347285744, 47, 8.447536928, -9.444855855, 7.931042782, 57, 7.333405661,
    -8.875046533, 7.000278625, 69, 5.034642387, -5.971815073, 6.239900085, 80, 2.433695176, -2.906486695,
    5.965701764, 88, 1.55961366, 0.286199394, 6.479312624, 93, 1, 3.918506415, 6.583214052, 96.18, 0.13,
    6, 6.55), ncol = 4, byrow = TRUE), pectoral = matrix(c(6.5, 27.1, 19.5, 4.8, 5.7, 1.05, 8.4, 26.5421362,
    19.55729217, 6.122086284, 5.765125669, 1.387654826, 10.5, 25.55388591, 18.73618447, 6.957590551,
    6.471926228, 0.9201726948, 12.5, 24.00909703, 18.01234849, 7.836223658, 7.537146797, 0.6073251311,
    14.2, 21.726536, 16.65569253, 8.66335824, 8.553662408, 0.4037376277, 15.4, 19.13219141, 15.43422331,
    9.238722848, 9.34275527, 0.2719846589, 15.835, 16.828634, 16.44830039, 9.439864014, 9.46786531, 0.03253678546),
    ncol = 6, byrow = TRUE), fluke = matrix(c(0, -42.5, -46.217, 5.4, 6.4, 0.38, 3, -40.05051908, -47.76387083,
    4.521690847, 6.337537518, 0.7453490979, 6.5, -41.32750035, -47.7369001, 5.002973012, 6.369451578,
    0.5916291044, 10, -42.87634103, -48.40859282, 5.312543809, 6.519402152, 0.5276036747, 13, -45.74645517,
    -49.90059168, 5.876788148, 6.715573521, 0.3778490551, 14.195, -48.78067401, -49.37954979, 6.551089555,
    6.682905631, 0.04477724534), ncol = 6, byrow = TRUE), dorsal = matrix(c(-9, 7.5, -7.3, 0, 1.55, -11,
    5.002901386, -5.292942303, 0, 1.367201515, -13.5, 1.914788333, -5.276434421, 0, 0.7595404185, -15.5,
    -0.7555783074, -6.34446588, 0, 0.5145320929, -17, -3.103233154, -7.602777856, 0, 0.3134353008, -18,
    -5.013653712, -7.67052598, 0, 0.1902588571, -18.46, -6.425778801, -6.826695847, 0, 0.04940229604),
    ncol = 5, byrow = TRUE), body_vertices = matrix(c(49.97, 0.1716390532, 4.320120292, 49.97, 0.1207320549,
    4.450261596, 49.97, 1.035721907e-17, 4.495310768, 49.97, -0.1207320549, 4.450261596, 49.97, -0.1716390532,
    4.320120292, 49.97, -0.1533898878, 4.255516542, 49.97, -0.08471730466, 4.201384975, 49.97, -3.11276689e-17,
    4.180247408, 49.97, 0.08471730466, 4.201384975, 49.97, 0.1533898878, 4.255516542, 48, 1.216413198,
    3.12740006, 48, 0.8826582889, 4.707354841, 48, 7.447221619e-17, 5.109562533, 48, -0.8826582889, 4.707354841,
    48, -1.216413198, 3.12740006, 48, -1.160351856, 2.835269419, 48, -0.6236549536, 2.340569282, 48,
    -2.234015909e-16, 2.176655549, 48, 0.6236549536, 2.340569282, 48, 1.160351856, 2.835269419, 44.5,
    1.652412691, 2.531094828, 44.5, 1.13867943, 4.241902513, 44.5, 1.011614152e-16, 4.803603272, 44.5,
    -1.13867943, 4.241902513, 44.5, -1.652412691, 2.531094828, 44.5, -1.548164852, 2.018221885, 44.5,
    -0.8650806121, 1.356832676, 44.5, -3.035071577e-16, 1.188509659, 44.5, 0.8650806121, 1.356832676,
    44.5, 1.548164852, 2.018221885, 42.7, 1.988280964, 2.129254548, 42.7, 1.440308193, 3.96072258, 42.7,
    1.217304672e-16, 4.65593109, 42.7, -1.440308193, 3.96072258, 42.7, -1.988280964, 2.129254548, 42.7,
    -1.489719733, 1.040779276, 42.7, -0.8893305316, 0.1772913608, 42.7, -3.650193127e-16, -0.3897230113,
    42.7, 0.8893305316, 0.1772913608, 42.7, 1.489719733, 1.040779276, 40.5, 2.855704699, 1.223196522,
    40.5, 2.092176204, 3.690500584, 40.5, 1.748461956e-16, 4.678399628, 40.5, -2.092176204, 3.690500584,
    40.5, -2.855704699, 1.223196522, 40.5, -2.53101363, -0.9086800344, 40.5, -1.438312529, -2.241725732,
    40.5, -5.244976338e-16, -2.712847103, 40.5, 1.438312529, -2.241725732, 40.5, 2.53101363, -0.9086800344,
    37, 4.435455151, -0.1386288954, 37, 3.114881093, 3.767928396, 37, 2.715745648e-16, 5.253750563, 37,
    -3.114881093, 3.767928396, 37, -4.435455151, -0.1386288954, 37, -3.796639045, -2.204394502, 37, -2.252639181,
    -4.128475367, 37, -8.14737368e-16, -4.728916128, 37, 2.252639181, -4.128475367, 37, 3.796639045,
    -2.204394502, 30, 7.076639033, 1.482001207, 30, 5.003939448, 4.937115452, 30, 4.333130006e-16, 7.04203893,
    30, -5.003939448, 4.937115452, 30, -7.076639033, 1.482001207, 30, -5.687947362, -3.480021058, 30,
    -3.380494642, -6.091373244, 30, -1.293891761e-15, -7.298338407, 30, 3.380494642, -6.091373244, 30,
    5.687947362, -3.480021058, 22, 8.24522078, 1.569421474, 22, 5.830251526, 5.742997852, 22, 5.048717323e-16,
    8.282041344, 22, -5.830251526, 5.742997852, 22, -8.24522078, 1.569421474, 22, -6.281400048, -4.199849476,
    22, -3.736711116, -7.191703229, 22, -1.514429578e-15, -9.054771135, 22, 3.736711116, -7.191703229,
    22, 6.281400048, -4.199849476, 12, 8.903602896, 1.130340458, 12, 6.79538196, 6.211842837, 12, 5.451858532e-16,
    8.347242963, 12, -6.79538196, 6.211842837, 12, -8.903602896, 1.130340458, 12, -6.941137132, -4.730496089,
    12, -4.024733945, -7.732338946, 12, -1.635373685e-15, -9.688795266, 12, 4.024733945, -7.732338946,
    12, 6.941137132, -4.730496089, 3, 8.447536928, 0.3724961544, 3, 6.257012543, 5.678177243, 3, 5.172592204e-16,
    7.930988488, 3, -6.257012543, 5.678177243, 3, -8.447536928, 0.3724961544, 3, -6.774584407, -4.779528409,
    3, -4.019324371, -7.916706085, 3, -1.551787359e-15, -9.444855855, 3, 4.019324371, -7.916706085, 3,
    6.774584407, -4.779528409, -7, 7.333405661, 0.3159993662, -7, 5.356077476, 4.860022833, -7, 4.490358915e-16,
    7.00017792, -7, -5.356077476, 4.860022833, -7, -7.333405661, 0.3159993662, -7, -5.560082588, -4.412005748,
    -7, -3.176348844, -6.89230139, -7, -1.346770554e-15, -8.872959415, -7, 3.176348844, -6.89230139,
    -7, 5.560082588, -4.412005748, -19, 5.034642387, 0.9758152954, -19, 3.689924663, 4.609068285, -19,
    3.082749429e-16, 6.23974181, -19, -3.689924663, 4.609068285, -19, -5.034642387, 0.9758152954, -19,
    -3.91410536, -2.606584461, -19, -2.230264397, -4.550800277, -19, -9.24618853e-16, -5.970296945, -19,
    2.230264397, -4.550800277, -19, 3.91410536, -2.606584461, -30, 2.433695176, 1.493834436, -30, 1.650958181,
    4.538943351, -30, 1.489941284e-16, 5.964906297, -30, -1.650958181, 4.538943351, -30, -2.433695176,
    1.493834436, -30, -2.038985528, -0.6161868147, -30, -1.139774509, -2.068831494, -30, -4.469251554e-16,
    -2.905123351, -30, 1.139774509, -2.068831494, -30, 2.038985528, -0.6161868147, -38, 1.55961366, 3.52726144,
    -38, 0.9841646214, 5.336779332, -38, 9.548056085e-17, 6.478721417, -38, -0.9841646214, 5.336779332,
    -38, -1.55961366, 3.52726144, -38, -1.201619014, 2.005330382, -38, -0.7241279427, 0.8925347978, -38,
    -2.86391678e-16, 0.2873310682, -38, 0.7241279427, 0.8925347978, -38, 1.201619014, 2.005330382, -43,
    1, 5.250860234, -43, 0.7071067812, 6.192976654, -43, 6.123233996e-17, 6.583214052, -43, -0.7071067812,
    6.192976654, -43, -1, 5.250860234, -43, -0.8660254038, 4.584683324, -43, -0.5, 4.09700798, -43, -1.836970199e-16,
    3.918506415, -43, 0.5, 4.09700798, -43, 0.8660254038, 4.584683324, -46.18, 0.13, 6.275, -46.18, 0.09192388155,
    6.469454365, -46.18, 7.960204194e-18, 6.55, -46.18, -0.09192388155, 6.469454365, -46.18, -0.13, 6.275,
    -46.18, -0.1125833025, 6.1375, -46.18, -0.065, 6.036843014, -46.18, -2.388061258e-17, 6, -46.18,
    0.065, 6.036843014, -46.18, 0.1125833025, 6.1375), ncol = 3, byrow = TRUE))


# .referenceDolphinSpec
.referenceDolphinSpec <- function() {
    controls <- .referenceDolphinControls()
    b <- controls$body
    z <- (b[, 3] + b[, 4])/2
    body <- .anatomyBody(b[, 1], b[, 2], z - b[, 3], b[, 4] - z, z)
    foil <- function(id, section.id, sections, role) {
        p <- controls[[section.id]]
        out <- .anatomyFoil(id, cbind(p[, 1:3], (p[, 4] + p[, 5])/2, p[, 6]), role = role, sections = sections)
        out$leading[, 3] <- p[, 4]
        out$trailing[, 3] <- p[, 5]
        chord <- colSums(out$leading - out$trailing)
        out$normal <- c(-chord[3]/chord[1], 0, 1)
        out$normal <- out$normal/sqrt(sum(out$normal^2))
        out$pivot <- (out$leading[1, ] + out$trailing[1, ])/2
        out
    }
    pectoral <- foil("pectoral", "pectoral", 6L, "rigid")
    chord <- colSums(pectoral$leading - pectoral$trailing)
    pectoral$normal <- c(-chord[3]/chord[1], 0, 1)
    pectoral$normal <- pectoral$normal/sqrt(sum(pectoral$normal^2))
    fluke <- foil("fluke", "fluke", 6L, "terminal")
    dorsal <- .anatomyFoil("dorsal", controls$dorsal, "xz", "attached", 4L)
    spec <- .anatomySpec("Common dolphin", "Delphinus delphis", body, c(.anatomyPair(pectoral), list(dorsal),
        .anatomyPair(fluke)), type = "vertical", sections = 10L, pivot = c(0, 0, 0), tip = -40)
    spec$body.angles <- c(0, pi/4, pi/2, 3 * pi/4, pi, 7 * pi/6, 4 * pi/3, 3 * pi/2, 5 * pi/3, 11 * pi/6)
    rings <- controls$body_vertices
    order <- unlist(lapply(rev(seq_len(nrow(b))), function(i) (i - 1L) * 10L + seq_len(10L)))
    rings <- rings[order, , drop = FALSE]
    spec$body.transform <- function(vertices) {
        n <- nrow(rings)
        vertices[seq_len(n), ] <- rings
        vertices[n + 1L, ] <- colMeans(rings[1:10, , drop = FALSE])
        vertices[n + 2L, ] <- colMeans(tail(rings, 10))
        vertices
    }
    spec$appearance <- list(smooth = TRUE, smooth.faces = rep(TRUE, 688L), cull = TRUE, fit = "legacy",
        eye.cex = 0.65, compass = c(1.12, 1.1, 0.18), dorsal = "#343c42", ventral = "#dce2df")
    spec$surface <- "dolphin"
    spec
}


# .anatomyDolphin
.anatomyDolphin <- function() .referenceDolphinSpec()

# Humpback whale --------------------------------------------------------


# .humpbackCountershading
.humpbackCountershading <- function(geometry, belly, sections) {
    f <- do.call(rbind, geometry$faces)
    v <- geometry$vertices
    centroid <- (v[f[, 1], ] + v[f[, 2], ] + v[f[, 3], ])/3
    smooth <- function(u) {
        u <- pmin(1, pmax(0, u))
        u * u * (3 - 2 * u)
    }
    body <- which(geometry$face.region == "body")
    belly[body] <- belly[body] * smooth((50 - centroid[body, 1] - 8)/16)
    side <- head(body, length(body) - 2L * sections)
    pairs <- matrix(side, ncol = 2, byrow = TRUE)
    weight <- rowMeans(matrix(belly[side], ncol = 2, byrow = TRUE))
    belly[pairs[, 1]] <- weight
    belly[pairs[, 2]] <- weight
    e <- v[f[, 2], ] - v[f[, 1], ]
    g <- v[f[, 3], ] - v[f[, 1], ]
    n <- cbind(e[, 2] * g[, 3] - e[, 3] * g[, 2], e[, 3] * g[, 1] - e[, 1] * g[, 3], e[, 1] * g[, 2] -
        e[, 2] * g[, 1])
    nz <- n[, 3]/pmax(sqrt(rowSums(n^2)), 1e-12)
    pectoral <- startsWith(geometry$face.region, "pectoral_")
    span <- abs(centroid[, 2])
    belly[pectoral] <- (0.84 + 0.13 * smooth(nz[pectoral])) * smooth((span[pectoral] - 6.5)/3) * (1 -
        0.65 * smooth((span[pectoral] - 17.5)/1.5))
    fluke <- startsWith(geometry$face.region, "fluke_")
    belly[fluke] <- 0.92 * smooth((nz[fluke] - 0.05)/0.4)
    belly
}


# .compileHumpbackSurface
.compileHumpbackSurface <- function(spec, geometry) {
    requests <- list()
    add <- function(origin, direction, kind = "line", group = 0L) {
        n <- nrow(origin)
        if (is.null(dim(direction)))
            direction <- matrix(direction, n, 3, byrow = TRUE)
        requests[[length(requests) + 1L]] <<- list(origin = origin, direction = direction, kind = rep(kind,
            n), group = rep(group, n), regions = "body")
    }
    for (side in c(1, -1)) add(matrix(c(50 - 22.5138513, side * 30, -4.81005707), 1, 3), c(0, -side,
        0), "eye")
    s <- seq(1.6, 23, length.out = 23)
    centre <- stats::approx(spec$body[, 1], spec$body[, 5], 50 - s, rule = 2)$y
    for (side in c(1, -1)) {
        angle <- seq(12, 4, length.out = length(s)) * pi/180
        add(cbind(50 - s, 0, centre), cbind(0, side * cos(angle), sin(angle)), group = if (side == 1)
            1L
        else 2L)
    }
    for (i in seq_len(9L)) {
        lateral <- (i - 5)/4
        s <- seq(6 + 8 * abs(lateral), 54 - 7 * abs(lateral), length.out = 17)
        width <- stats::approx(spec$body[, 1], spec$body[, 2], 50 - s, rule = 2)$y
        y <- lateral * 0.72 * width * sin(pi * (s - 2)/65)^0.5
        add(cbind(50 - s, y, 40), c(0, 0, -1), group = 2L + i)
    }
    for (side in c(1, -1)) {
        s <- c(3.5, 6.5, 10, 14, 18)
        width <- stats::approx(spec$body[, 1], spec$body[, 2], 50 - s, rule = 2)$y
        add(cbind(50 - s, side * 0.63 * width, -40), c(0, 0, 1), "spot")
    }
    s <- c(4.5, 8, 12, 16)
    add(cbind(50 - s, 0, -40), c(0, 0, 1), "spot")
    for (side in c(1, -1)) {
        s <- seq(23.5, 25.5, length.out = 5)
        add(cbind(50 - s, side * (0.35 + 0.15 * sin(seq(0, pi, length.out = 5))), -40), c(0, 0, 1), group = if (side ==
            1)
            12L
        else 13L)
    }
    .attachOverlaySurfaceRays(geometry, requests)
}


# .referenceHumpbackControls
.referenceHumpbackControls <- function() list(body = matrix(c(0.03, 0.1366663586, -3.077586575, -2.822670387,
    1.8, 2.499398817, -4.848991004, -0.2513364994, 6, 4.223962419, -6.611983629, -0.4147450763, 12, 5.818781342,
    -8.924239877, 0.01171284705, 20, 6.939883748, -10.74075116, 1.814534185, 28, 8.067723265, -12.16465577,
    3.508500626, 38, 8.661517572, -13.13624107, 4.406909802, 50, 7.969871418, -13.54657607, 4.29242044,
    60, 6.655598515, -13.50043068, 3.859501745, 69.4, 4.519624493, -11.65788416, 3.746320587, 77, 2.935040861,
    -8.304205768, 2.920968771, 85, 2.054870408, -4.504349895, 0.8278463038, 90, 1.3, -2.4, -0.12, 95.2,
    0.18, -1, -0.52), ncol = 4, byrow = TRUE), pectoral = matrix(c(6.5, 20, 11, 0, 1, 1.2, 8.5, 19.5,
    11.4, 0.1, 1.6, 1.15, 9.5, 18.27230317, 10.60535972, 1.430640259, 2.416057326, 1.415156218, 10.5,
    17.68395731, 6.090946317, 2.328196521, 2.979482692, 1.545721357, 11.8, 16.5967535, 4.932523829, 3.42413406,
    4.102724865, 1.626669277, 13, 14.03759936, 3.964354437, 5.041639287, 5.145328146, 1.459480219, 14.2,
    12.34352592, 2.29728873, 6.140878383, 6.542518128, 1.752199926, 15.5, 9.904292667, 0.2237337754,
    7.181171036, 7.677408285, 1.602255753, 17, 6.384425498, -2.154922364, 8.422027451, 8.796552489, 1.528000701,
    18.2, 4.146305582, -2.143550046, 9.616370179, 9.656572676, 1.323756953, 19.08, 0.1214111375, 0.009855760219,
    10.28677282, 10.31159795, 0.07542171975), ncol = 6, byrow = TRUE), fluke = matrix(c(0, -41.22003937,
    -45.34809545, -0.5564770463, -0.5760000515, 0.6821129263, 2, -37.06461508, -46.23771734, -1.368631751,
    -0.5336059525, 1.02007269, 4, -38.59506124, -46.59846218, -0.8528339307, -0.5349506547, 0.8053154548,
    6.5, -39.90258048, -47.44332925, -0.9444787313, -0.4412216447, 0.9498582796, 9, -41.22738895, -47.91015056,
    -0.8952082376, -0.4049544223, 0.8933132083, 11.5, -42.86700123, -48.22068416, -0.7132027886, -0.5011124507,
    0.7694694536, 13.75, -45.63232789, -49.88401172, -0.7730638107, -0.7585901422, 0.6466887382, 14.42,
    -48.00185856, -48.19446776, -0.6760581449, -0.6752236457, 0.025), ncol = 6, byrow = TRUE), dorsal = matrix(c(-11.5,
    -12.2, -19.4, 0, 0.75, -13.85, -13.5, -17.8, 0, 0.3, -14.66, -17.8, -18.24, 0, 0.035), ncol = 5,
    byrow = TRUE), body_vertices = matrix(c(49.97, 0.1366663586, -2.947590778, 49.97, 0.06902027957,
    -2.885758629, 49.97, 8.368400919e-18, -2.822670387, 49.97, -0.06902027957, -2.885758629, 49.97, -0.1366663586,
    -2.947590778, 49.97, -0.08556298111, -2.996199846, 49.97, -0.04966144627, -3.030349059, 49.97, -2.510520276e-17,
    -3.077586575, 49.97, 0.04966144627, -3.030349059, 49.97, 0.08556298111, -2.996199846, 48.2, 2.499398817,
    -2.770959013, 48.2, 1.492035439, -1.177861058, 48.2, 1.530440381e-16, -0.2513364995, 48.2, -1.492035439,
    -1.177861058, 48.2, -2.499398817, -2.770959013, 48.2, -2.099812394, -3.665204258, 48.2, -1.15066818,
    -4.383244126, 48.2, -4.591321142e-16, -4.848991004, 48.2, 1.15066818, -4.383244126, 48.2, 2.099812394,
    -3.665204258, 44, 4.223962419, -4.170821908, 44, 3.002055445, -1.311113019, 44, 2.586431028e-16,
    -0.4147450763, 44, -3.002055445, -1.311113019, 44, -4.223962419, -4.170821908, 44, -3.488123445,
    -4.990700729, 44, -2.142070274, -6.235078524, 44, -7.759293084e-16, -6.611983629, 44, 2.142070274,
    -6.235078524, 44, 3.488123445, -4.990700729, 38, 5.818781342, -4.86650399, 38, 3.757911141, -1.57073523,
    38, 3.562975973e-16, 0.01171284693, 38, -3.757911141, -1.57073523, 38, -5.818781342, -4.86650399,
    38, -4.801940378, -6.585064024, 38, -2.761834694, -8.129400605, 38, -1.068892792e-15, -8.924239877,
    38, 2.761834694, -8.129400605, 38, 4.801940378, -6.585064024, 30, 6.939883748, -5.500911667, 30,
    4.548623937, -0.3485387383, 30, 4.249453209e-16, 1.814534185, 30, -4.548623937, -0.3485387383, 30,
    -6.939883748, -5.500911667, 30, -5.668219536, -7.423372401, 30, -3.309842169, -9.648867228, 30, -1.274835963e-15,
    -10.74075116, 30, 3.309842169, -9.648867228, 30, 5.668219536, -7.423372401, 22, 8.067723265, -5.504482888,
    22, 5.926017472, 1.428155978, 22, 4.868239409e-16, 3.394575947, 22, -5.926017472, 1.428155978, 22,
    -8.067723265, -5.504482888, 22, -6.580568169, -8.018518375, 22, -3.850347724, -10.8060047, 22, -1.482016721e-15,
    -12.16465576, 22, 3.850347724, -10.8060047, 22, 6.580568169, -8.018518375, 12, 8.661517572, -4.580657959,
    12, 6.488446192, 2.206226217, 12, 5.303649885e-16, 4.406909802, 12, -6.488446192, 2.206226217, 12,
    -8.661517572, -4.580657959, 12, -7.268210091, -8.614289155, 12, -4.1764768, -11.69045306, 12, -1.591094966e-15,
    -13.13624107, 12, 4.1764768, -11.69045306, 12, 7.268210091, -8.614289155, 0, 7.969871418, -1.529987614,
    0, 6.141224737, 2.245886684, 0, 4.871764815e-16, 4.277115262, 0, -6.141224737, 2.245886684, 0, -7.969871418,
    -1.529987614, 0, -6.57247928, -8.873837721, 0, -3.799224508, -11.99160205, 0, -1.464041628e-15, -13.54657607,
    0, 3.799224508, -11.99160205, 0, 6.57247928, -8.873837721, -10, 6.655598515, -1.831225289, -10, 5.2107963,
    1.975248304, -10, 4.075378709e-16, 3.859501745, -10, -5.2107963, 1.975248304, -10, -6.655598515,
    -1.831225289, -10, -5.060035662, -8.630454754, -10, -3.132774796, -11.89700049, -10, -1.222613613e-15,
    -13.50043068, -10, 3.132774796, -11.89700049, -10, 5.060035662, -8.630454754, -19.4, 4.519624493,
    -1.863223619, -19.4, 3.288423148, 1.648173093, -19.4, 2.767471834e-16, 3.746320586, -19.4, -3.288423148,
    1.648173093, -19.4, -4.519624493, -1.863223619, -19.4, -3.311457823, -7.213890218, -19.4, -2.093243629,
    -10.13434278, -19.4, -8.302415501e-16, -11.65788416, -19.4, 2.093243629, -10.13434278, -19.4, 3.311457823,
    -7.213890218, -27, 2.935040861, -1.565785882, -27, 1.879859291, 0.9031779039, -27, 1.797194197e-16,
    2.920968769, -27, -1.879859291, 0.9031779039, -27, -2.935040861, -1.565785882, -27, -2.354596851,
    -5.291208671, -27, -1.397948333, -7.321828651, -27, -5.391582592e-16, -8.304205766, -27, 1.397948333,
    -7.321828651, -27, 2.354596851, -5.291208671, -35, 2.054870408, -2.234925215, -35, 1.361997106, -0.07112434904,
    -35, 1.258245234e-16, 0.8278463035, -35, -1.361997106, -0.07112434904, -35, -2.054870408, -2.234925215,
    -35, -1.623446316, -3.054350965, -35, -0.8964266799, -3.852750945, -35, -3.774735699e-16, -4.504349893,
    -35, 0.8964266799, -3.852750945, -35, 1.623446316, -3.054350965, -40, 1.3, -1.26, -40, 0.9192388155,
    -0.4538982694, -40, 7.960204194e-17, -0.12, -40, -0.9192388155, -0.4538982694, -40, -1.3, -1.26,
    -40, -1.125833025, -1.83, -40, -0.65, -2.24726896, -40, -2.388061258e-16, -2.4, -40, 0.65, -2.24726896,
    -40, 1.125833025, -1.83, -45.2, 0.18, -0.76, -45.2, 0.1272792206, -0.5902943725, -45.2, 1.102182119e-17,
    -0.52, -45.2, -0.1272792206, -0.5902943725, -45.2, -0.18, -0.76, -45.2, -0.1558845727, -0.88, -45.2,
    -0.09, -0.9678460969, -45.2, -3.306546358e-17, -1, -45.2, 0.09, -0.9678460969, -45.2, 0.1558845727,
    -0.88), ncol = 3, byrow = TRUE))


# .referenceHumpbackSpec
.referenceHumpbackSpec <- function() {
    controls <- .referenceHumpbackControls()
    b <- controls$body
    z <- (b[, 3] + b[, 4])/2
    body <- .anatomyBody(b[, 1], b[, 2], z - b[, 3], b[, 4] - z, z)
    foil <- function(id, section.id, sections, role) {
        p <- controls[[section.id]]
        out <- .anatomyFoil(id, cbind(p[, 1:3], (p[, 4] + p[, 5])/2, p[, 6]), role = role, sections = sections)
        out$leading[, 3] <- p[, 4]
        out$trailing[, 3] <- p[, 5]
        out$pivot <- (out$leading[1, ] + out$trailing[1, ])/2
        out
    }
    pectoral <- foil("pectoral", "pectoral", 6L, "rigid")
    chord <- colSums(pectoral$leading - pectoral$trailing)
    pectoral$normal <- c(-chord[3]/chord[1], 0, 1)
    pectoral$normal <- pectoral$normal/sqrt(sum(pectoral$normal^2))
    fluke <- foil("fluke", "fluke", 4L, "terminal")
    dorsal <- .anatomyFoil("dorsal", controls$dorsal, "xz", "attached", 4L)
    spec <- .anatomySpec("Humpback whale", "Megaptera novaeangliae", body, c(.anatomyPair(pectoral),
        list(dorsal), .anatomyPair(fluke)), type = "vertical", sections = 10L, pivot = c(0, 0, 0), tip = -37)
    spec$body.angles <- c(0, pi/4, pi/2, 3 * pi/4, pi, 7 * pi/6, 4 * pi/3, 3 * pi/2, 5 * pi/3, 11 * pi/6)
    rings <- controls$body_vertices
    order <- unlist(lapply(rev(seq_len(nrow(b))), function(i) (i - 1L) * 10L + seq_len(10L)))
    rings <- rings[order, , drop = FALSE]
    spec$body.transform <- function(vertices) {
        n <- nrow(rings)
        vertices[seq_len(n), ] <- rings
        vertices[n + 1L, ] <- colMeans(rings[1:10, , drop = FALSE])
        vertices[n + 2L, ] <- colMeans(tail(rings, 10))
        vertices
    }
    spec$appearance <- list(smooth = TRUE, smooth.faces = rep(TRUE, 696L), cull = TRUE, fit = "legacy",
        eye.cex = 0.65, compass = c(1.12, 1.1, 0.18), dorsal = "#35434d", ventral = "#d8ddd8")
    spec$surface <- "humpback"
    spec
}


# .anatomyHumpback
.anatomyHumpback <- function() .referenceHumpbackSpec()

# Striped marlin --------------------------------------------------------


# .marlinReferenceFaceRGB
.marlinReferenceFaceRGB <- function() matrix(c(0.469351, 0.471445, 0.470467, 0.469351, 0.471445, 0.470467,
    0.438324, 0.452246, 0.495726, 0.438324, 0.452246, 0.495726, 0.438324, 0.452246, 0.495726, 0.438324,
    0.452246, 0.495726, 0.469351, 0.471445, 0.470467, 0.469351, 0.471445, 0.470467, 0.430621, 0.430088,
    0.425475, 0.430621, 0.430088, 0.425475, 0.344517, 0.355899, 0.395106, 0.344517, 0.355899, 0.395106,
    0.344517, 0.355899, 0.395106, 0.344517, 0.355899, 0.395106, 0.430621, 0.430088, 0.425475, 0.430621,
    0.430088, 0.425475, 0.573247, 0.575828, 0.575305, 0.573247, 0.575828, 0.575305, 0.454179, 0.464057,
    0.505892, 0.454179, 0.464057, 0.505892, 0.454179, 0.464057, 0.505892, 0.454179, 0.464057, 0.505892,
    0.573247, 0.575828, 0.575305, 0.573247, 0.575828, 0.575305, 0.465454, 0.465875, 0.459229, 0.465454,
    0.465875, 0.459229, 0.332769, 0.348028, 0.393202, 0.332769, 0.348028, 0.393202, 0.332769, 0.348028,
    0.393202, 0.332769, 0.348028, 0.393202, 0.465454, 0.465875, 0.459229, 0.465454, 0.465875, 0.459229,
    0.621114, 0.623236, 0.616942, 0.621114, 0.623236, 0.616942, 0.431748, 0.436056, 0.459697, 0.431748,
    0.436056, 0.459697, 0.431748, 0.436056, 0.459697, 0.431748, 0.436056, 0.459697, 0.621114, 0.623236,
    0.616942, 0.621114, 0.623236, 0.616942, 0.467251, 0.468613, 0.456406, 0.467251, 0.468613, 0.456406,
    0.286762, 0.306853, 0.348314, 0.286762, 0.306853, 0.348314, 0.286762, 0.306853, 0.348314, 0.286762,
    0.306853, 0.348314, 0.467251, 0.468613, 0.456406, 0.467251, 0.468613, 0.456406, 0.675573, 0.677397,
    0.665118, 0.675573, 0.677397, 0.665118, 0.542365, 0.547285, 0.559614, 0.542365, 0.547285, 0.559614,
    0.542365, 0.547285, 0.559614, 0.542365, 0.547285, 0.559614, 0.675573, 0.677397, 0.665118, 0.675573,
    0.677397, 0.665118, 0.473483, 0.476117, 0.449359, 0.473483, 0.476117, 0.449359, 0.266836, 0.299363,
    0.354437, 0.266836, 0.299363, 0.354437, 0.266836, 0.299363, 0.354437, 0.266836, 0.299363, 0.354437,
    0.473483, 0.476117, 0.449359, 0.473483, 0.476117, 0.449359, 0.771048, 0.773412, 0.760431, 0.771048,
    0.773412, 0.760431, 0.685049, 0.687997, 0.700655, 0.685049, 0.687997, 0.700655, 0.685049, 0.687997,
    0.700655, 0.685049, 0.687997, 0.700655, 0.771048, 0.773412, 0.760431, 0.771048, 0.773412, 0.760431,
    0.503135, 0.503617, 0.451452, 0.503135, 0.503617, 0.451452, 0.238887, 0.283869, 0.390174, 0.238887,
    0.283869, 0.390174, 0.238887, 0.283869, 0.390174, 0.238887, 0.283869, 0.390174, 0.503135, 0.503617,
    0.451452, 0.503135, 0.503617, 0.451452, 0.802286, 0.803952, 0.796945, 0.802286, 0.803952, 0.796945,
    0.690766, 0.698694, 0.714937, 0.690766, 0.698694, 0.714937, 0.690766, 0.698694, 0.714937, 0.690766,
    0.698694, 0.714937, 0.802286, 0.803952, 0.796945, 0.802286, 0.803952, 0.796945, 0.53982, 0.55534,
    0.530444, 0.53982, 0.55534, 0.530444, 0.259009, 0.309682, 0.446356, 0.259009, 0.309682, 0.446356,
    0.259009, 0.309682, 0.446356, 0.259009, 0.309682, 0.446356, 0.53982, 0.55534, 0.530444, 0.53982,
    0.55534, 0.530444, 0.81612, 0.816238, 0.809371, 0.81612, 0.816238, 0.809371, 0.814969, 0.820371,
    0.836377, 0.814969, 0.820371, 0.836377, 0.814969, 0.820371, 0.836377, 0.814969, 0.820371, 0.836377,
    0.81612, 0.816238, 0.809371, 0.81612, 0.816238, 0.809371, 0.505913, 0.539747, 0.555826, 0.505913,
    0.539747, 0.555826, 0.236174, 0.28603, 0.437399, 0.236174, 0.28603, 0.437399, 0.236174, 0.28603,
    0.437399, 0.236174, 0.28603, 0.437399, 0.505913, 0.539747, 0.555826, 0.505913, 0.539747, 0.555826,
    0.736368, 0.739443, 0.743106, 0.736368, 0.739443, 0.743106, 0.702845, 0.707294, 0.726772, 0.702845,
    0.707294, 0.726772, 0.702845, 0.707294, 0.726772, 0.702845, 0.707294, 0.726772, 0.736368, 0.739443,
    0.743106, 0.736368, 0.739443, 0.743106, 0.458495, 0.496896, 0.542355, 0.458495, 0.496896, 0.542355,
    0.243291, 0.292556, 0.445402, 0.243291, 0.292556, 0.445402, 0.243291, 0.292556, 0.445402, 0.243291,
    0.292556, 0.445402, 0.458495, 0.496896, 0.542355, 0.458495, 0.496896, 0.542355, 0.617653, 0.622159,
    0.635454, 0.617653, 0.622159, 0.635454, 0.607784, 0.618437, 0.639136, 0.607784, 0.618437, 0.639136,
    0.607784, 0.618437, 0.639136, 0.607784, 0.618437, 0.639136, 0.617653, 0.622159, 0.635454, 0.617653,
    0.622159, 0.635454, 0.459054, 0.493399, 0.54462, 0.459054, 0.493399, 0.54462, 0.252663, 0.300402,
    0.450615, 0.252663, 0.300402, 0.450615, 0.252663, 0.300402, 0.450615, 0.252663, 0.300402, 0.450615,
    0.459054, 0.493399, 0.54462, 0.459054, 0.493399, 0.54462, 0.691428, 0.697791, 0.707213, 0.691428,
    0.697791, 0.707213, 0.765356, 0.769741, 0.783127, 0.765356, 0.769741, 0.783127, 0.765356, 0.769741,
    0.783127, 0.765356, 0.769741, 0.783127, 0.691428, 0.697791, 0.707213, 0.691428, 0.697791, 0.707213,
    0.491887, 0.509517, 0.51842, 0.491887, 0.509517, 0.51842, 0.27321, 0.32321, 0.465422, 0.27321, 0.32321,
    0.465422, 0.27321, 0.32321, 0.465422, 0.27321, 0.32321, 0.465422, 0.491887, 0.509517, 0.51842, 0.491887,
    0.509517, 0.51842, 0.749465, 0.751475, 0.753733, 0.749465, 0.751475, 0.753733, 0.719978, 0.725438,
    0.74008, 0.719978, 0.725438, 0.74008, 0.719978, 0.725438, 0.74008, 0.719978, 0.725438, 0.74008, 0.749465,
    0.751475, 0.753733, 0.749465, 0.751475, 0.753733, 0.535005, 0.543736, 0.55436, 0.535005, 0.543736,
    0.55436, 0.270615, 0.325677, 0.453294, 0.270615, 0.325677, 0.453294, 0.270615, 0.325677, 0.453294,
    0.270615, 0.325677, 0.453294, 0.535005, 0.543736, 0.55436, 0.535005, 0.543736, 0.55436, 0.685773,
    0.686597, 0.701706, 0.685773, 0.686597, 0.701706, 0.483727, 0.493322, 0.526494, 0.483727, 0.493322,
    0.526494, 0.483727, 0.493322, 0.526494, 0.483727, 0.493322, 0.526494, 0.685773, 0.686597, 0.701706,
    0.685773, 0.686597, 0.701706, 0.581135, 0.600154, 0.640983, 0.581135, 0.600154, 0.640983, 0.2607,
    0.306452, 0.419642, 0.2607, 0.306452, 0.419642, 0.2607, 0.306452, 0.419642, 0.2607, 0.306452, 0.419642,
    0.581135, 0.600154, 0.640983, 0.581135, 0.600154, 0.640983, 0.683626, 0.691855, 0.708584, 0.683626,
    0.691855, 0.708584, 0.486274, 0.496841, 0.531534, 0.486274, 0.496841, 0.531534, 0.486274, 0.496841,
    0.531534, 0.486274, 0.496841, 0.531534, 0.683626, 0.691855, 0.708584, 0.683626, 0.691855, 0.708584,
    0.637374, 0.6699, 0.729415, 0.637374, 0.6699, 0.729415, 0.335422, 0.37128, 0.461984, 0.335422, 0.37128,
    0.461984, 0.335422, 0.37128, 0.461984, 0.335422, 0.37128, 0.461984, 0.637374, 0.6699, 0.729415, 0.637374,
    0.6699, 0.729415, 0.420703, 0.42742, 0.446694, 0.420703, 0.42742, 0.446694, 0.420703, 0.42742, 0.446694,
    0.420703, 0.42742, 0.446694, 0.420703, 0.42742, 0.446694, 0.420703, 0.42742, 0.446694, 0.420703,
    0.42742, 0.446694, 0.420703, 0.42742, 0.446694, 0.535674, 0.557469, 0.607879, 0.535674, 0.557469,
    0.607879, 0.535674, 0.557469, 0.607879, 0.535674, 0.557469, 0.607879, 0.535674, 0.557469, 0.607879,
    0.535674, 0.557469, 0.607879, 0.535674, 0.557469, 0.607879, 0.535674, 0.557469, 0.607879, 0.499366,
    0.520523, 0.56815, 0.499366, 0.520523, 0.56815, 0.499366, 0.520523, 0.56815, 0.499366, 0.520523,
    0.56815, 0.499366, 0.520523, 0.56815, 0.499366, 0.520523, 0.56815, 0.514929, 0.546846, 0.612126,
    0.514929, 0.546846, 0.612126, 0.514929, 0.546846, 0.612126, 0.514929, 0.546846, 0.612126, 0.514929,
    0.546846, 0.612126, 0.514929, 0.546846, 0.612126, 0.499366, 0.520523, 0.56815, 0.499366, 0.520523,
    0.56815, 0.499366, 0.520523, 0.56815, 0.499366, 0.520523, 0.56815, 0.499366, 0.520523, 0.56815, 0.499366,
    0.520523, 0.56815, 0.514929, 0.546846, 0.612126, 0.514929, 0.546846, 0.612126, 0.514929, 0.546846,
    0.612126, 0.514929, 0.546846, 0.612126, 0.514929, 0.546846, 0.612126, 0.514929, 0.546846, 0.612126,
    0.514929, 0.546846, 0.612126, 0.514929, 0.546846, 0.612126, 0.514929, 0.546846, 0.612126, 0.514929,
    0.546846, 0.612126, 0.514929, 0.546846, 0.612126, 0.514929, 0.546846, 0.612126, 0.514929, 0.546846,
    0.612126, 0.514929, 0.546846, 0.612126, 0.514929, 0.546846, 0.612126, 0.514929, 0.546846, 0.612126,
    0.514929, 0.546846, 0.612126, 0.514929, 0.546846, 0.612126, 0.211063, 0.244038, 0.355745, 0.211063,
    0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038,
    0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745,
    0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063,
    0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038,
    0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745,
    0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063,
    0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038,
    0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745,
    0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063,
    0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038,
    0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745,
    0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063,
    0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038,
    0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745,
    0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063,
    0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038,
    0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745,
    0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063,
    0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.211063, 0.244038, 0.355745, 0.216508, 0.244262,
    0.322647, 0.216508, 0.244262, 0.322647, 0.216508, 0.244262, 0.322647, 0.216508, 0.244262, 0.322647,
    0.216508, 0.244262, 0.322647, 0.216508, 0.244262, 0.322647, 0.216508, 0.244262, 0.322647, 0.216508,
    0.244262, 0.322647, 0.216508, 0.244262, 0.322647, 0.216508, 0.244262, 0.322647, 0.216508, 0.244262,
    0.322647, 0.216508, 0.244262, 0.322647, 0.216508, 0.244262, 0.322647, 0.216508, 0.244262, 0.322647,
    0.216508, 0.244262, 0.322647, 0.216508, 0.244262, 0.322647, 0.216508, 0.244262, 0.322647, 0.216508,
    0.244262, 0.322647, 0.216508, 0.244262, 0.322647, 0.216508, 0.244262, 0.322647, 0.216508, 0.244262,
    0.322647, 0.216508, 0.244262, 0.322647, 0.216508, 0.244262, 0.322647, 0.216508, 0.244262, 0.322647,
    0.627141, 0.638122, 0.654281, 0.627141, 0.638122, 0.654281, 0.627141, 0.638122, 0.654281, 0.627141,
    0.638122, 0.654281, 0.627141, 0.638122, 0.654281, 0.627141, 0.638122, 0.654281, 0.627141, 0.638122,
    0.654281, 0.627141, 0.638122, 0.654281, 0.627141, 0.638122, 0.654281, 0.627141, 0.638122, 0.654281,
    0.627141, 0.638122, 0.654281, 0.627141, 0.638122, 0.654281, 0.627141, 0.638122, 0.654281, 0.627141,
    0.638122, 0.654281, 0.627141, 0.638122, 0.654281, 0.627141, 0.638122, 0.654281, 0.627141, 0.638122,
    0.654281, 0.627141, 0.638122, 0.654281, 0.627141, 0.638122, 0.654281, 0.627141, 0.638122, 0.654281,
    0.627141, 0.638122, 0.654281, 0.627141, 0.638122, 0.654281, 0.627141, 0.638122, 0.654281, 0.627141,
    0.638122, 0.654281, 0.536051, 0.545758, 0.560582, 0.536051, 0.545758, 0.560582, 0.536051, 0.545758,
    0.560582, 0.536051, 0.545758, 0.560582, 0.536051, 0.545758, 0.560582, 0.536051, 0.545758, 0.560582,
    0.536051, 0.545758, 0.560582, 0.536051, 0.545758, 0.560582, 0.536051, 0.545758, 0.560582, 0.536051,
    0.545758, 0.560582, 0.536051, 0.545758, 0.560582, 0.536051, 0.545758, 0.560582, 0.536051, 0.545758,
    0.560582, 0.536051, 0.545758, 0.560582, 0.536051, 0.545758, 0.560582, 0.536051, 0.545758, 0.560582,
    0.536051, 0.545758, 0.560582, 0.536051, 0.545758, 0.560582, 0.536051, 0.545758, 0.560582, 0.536051,
    0.545758, 0.560582, 0.536051, 0.545758, 0.560582, 0.536051, 0.545758, 0.560582, 0.536051, 0.545758,
    0.560582, 0.536051, 0.545758, 0.560582, 0.538918, 0.550587, 0.571243, 0.418305, 0.457273, 0.566093,
    0.538918, 0.550587, 0.571243, 0.538918, 0.550587, 0.571243, 0.538918, 0.550587, 0.571243, 0.538918,
    0.550587, 0.571243, 0.418305, 0.457273, 0.566093, 0.538918, 0.550587, 0.571243, 0.418305, 0.457273,
    0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093,
    0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.538918, 0.550587, 0.571243, 0.538918,
    0.550587, 0.571243, 0.538918, 0.550587, 0.571243, 0.538918, 0.550587, 0.571243, 0.418305, 0.457273,
    0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093,
    0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305,
    0.457273, 0.566093, 0.538918, 0.550587, 0.571243, 0.538918, 0.550587, 0.571243, 0.538918, 0.550587,
    0.571243, 0.538918, 0.550587, 0.571243, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093,
    0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305,
    0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.538918, 0.550587,
    0.571243, 0.538918, 0.550587, 0.571243, 0.538918, 0.550587, 0.571243, 0.538918, 0.550587, 0.571243,
    0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305,
    0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273,
    0.566093, 0.418305, 0.457273, 0.566093, 0.538918, 0.550587, 0.571243, 0.538918, 0.550587, 0.571243,
    0.538918, 0.550587, 0.571243, 0.538918, 0.550587, 0.571243, 0.418305, 0.457273, 0.566093, 0.418305,
    0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273,
    0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093,
    0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305,
    0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273,
    0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093,
    0.538918, 0.550587, 0.571243, 0.418305, 0.457273, 0.566093, 0.538918, 0.550587, 0.571243, 0.538918,
    0.550587, 0.571243, 0.538918, 0.550587, 0.571243, 0.538918, 0.550587, 0.571243, 0.418305, 0.457273,
    0.566093, 0.538918, 0.550587, 0.571243, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093,
    0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305,
    0.457273, 0.566093, 0.538918, 0.550587, 0.571243, 0.538918, 0.550587, 0.571243, 0.538918, 0.550587,
    0.571243, 0.538918, 0.550587, 0.571243, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093,
    0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305,
    0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.538918, 0.550587,
    0.571243, 0.538918, 0.550587, 0.571243, 0.538918, 0.550587, 0.571243, 0.538918, 0.550587, 0.571243,
    0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305,
    0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273,
    0.566093, 0.418305, 0.457273, 0.566093, 0.538918, 0.550587, 0.571243, 0.538918, 0.550587, 0.571243,
    0.538918, 0.550587, 0.571243, 0.538918, 0.550587, 0.571243, 0.418305, 0.457273, 0.566093, 0.418305,
    0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273,
    0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093,
    0.538918, 0.550587, 0.571243, 0.538918, 0.550587, 0.571243, 0.538918, 0.550587, 0.571243, 0.538918,
    0.550587, 0.571243, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273,
    0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093,
    0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305,
    0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273,
    0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093,
    0.418305, 0.457273, 0.566093, 0.418305, 0.457273, 0.566093, 0.500665, 0.516412, 0.543939, 0.500665,
    0.516412, 0.543939, 0.500665, 0.516412, 0.543939, 0.500665, 0.516412, 0.543939, 0.592131, 0.607201,
    0.637922, 0.592131, 0.607201, 0.637922, 0.592131, 0.607201, 0.637922, 0.592131, 0.607201, 0.637922,
    0.500665, 0.516412, 0.543939, 0.500665, 0.516412, 0.543939, 0.500665, 0.516412, 0.543939, 0.500665,
    0.516412, 0.543939, 0.592131, 0.607201, 0.637922, 0.592131, 0.607201, 0.637922, 0.592131, 0.607201,
    0.637922, 0.592131, 0.607201, 0.637922, 0.592131, 0.607201, 0.637922, 0.592131, 0.607201, 0.637922,
    0.592131, 0.607201, 0.637922, 0.592131, 0.607201, 0.637922, 0.592131, 0.607201, 0.637922, 0.592131,
    0.607201, 0.637922, 0.592131, 0.607201, 0.637922, 0.592131, 0.607201, 0.637922, 0.500665, 0.516412,
    0.543939, 0.500665, 0.516412, 0.543939, 0.500665, 0.516412, 0.543939, 0.500665, 0.516412, 0.543939,
    0.592131, 0.607201, 0.637922, 0.592131, 0.607201, 0.637922, 0.592131, 0.607201, 0.637922, 0.592131,
    0.607201, 0.637922, 0.500665, 0.516412, 0.543939, 0.500665, 0.516412, 0.543939, 0.500665, 0.516412,
    0.543939, 0.500665, 0.516412, 0.543939, 0.592131, 0.607201, 0.637922, 0.592131, 0.607201, 0.637922,
    0.592131, 0.607201, 0.637922, 0.592131, 0.607201, 0.637922, 0.592131, 0.607201, 0.637922, 0.592131,
    0.607201, 0.637922, 0.592131, 0.607201, 0.637922, 0.592131, 0.607201, 0.637922, 0.592131, 0.607201,
    0.637922, 0.592131, 0.607201, 0.637922, 0.592131, 0.607201, 0.637922, 0.592131, 0.607201, 0.637922,
    0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324,
    0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164,
    0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324,
    0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164,
    0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324,
    0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164,
    0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324,
    0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164,
    0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324,
    0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164,
    0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324,
    0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164,
    0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324,
    0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.247324, 0.25208, 0.258164, 0.288681, 0.292447,
    0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728,
    0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681,
    0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447,
    0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728,
    0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681,
    0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447,
    0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728,
    0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681,
    0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447,
    0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728,
    0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681,
    0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447,
    0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728,
    0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681, 0.292447, 0.292728, 0.288681,
    0.292447, 0.292728, 0.479346, 0.48362, 0.490791, 0.479346, 0.48362, 0.490791, 0.479346, 0.48362,
    0.490791, 0.479346, 0.48362, 0.490791, 0.479346, 0.48362, 0.490791, 0.479346, 0.48362, 0.490791,
    0.479346, 0.48362, 0.490791, 0.479346, 0.48362, 0.490791, 0.479346, 0.48362, 0.490791, 0.479346,
    0.48362, 0.490791, 0.479346, 0.48362, 0.490791, 0.479346, 0.48362, 0.490791), ncol = 3L, byrow = TRUE)


# .compileMarlinSurface
.compileMarlinSurface <- function(spec, geometry) {
    requests <- list()
    colours <- character()
    add <- function(origin, direction, kind = "line", group = 0L, colour = "#1e3540") {
        n <- nrow(origin)
        if (is.null(dim(direction)))
            direction <- matrix(direction, n, 3, byrow = TRUE)
        requests[[length(requests) + 1L]] <<- list(origin = origin, direction = direction, kind = rep(kind,
            n), group = rep(group, n), regions = "body")
        colours <<- c(colours, rep(colour, n))
    }
    for (side in c(1, -1)) add(matrix(c(50 - 25.00091507, side * 30, 2.19374332), 1, 3), c(0, -side,
        0), "eye")
    radial <- function(s, angle, side, group, colour = "#1e3540") {
        z <- stats::approx(spec$body[, 1], spec$body[, 5], 50 - s, rule = 2)$y
        a <- angle * pi/180
        add(cbind(50 - s, 0, z), cbind(0, side * cos(a), sin(a)), group = group, colour = colour)
    }
    for (side in c(1, -1)) {
        offset <- if (side == 1)
            0L
        else 1L
        radial(seq(18.8, 29.2, length.out = 19), seq(0, 25, length.out = 19), side, 1L + offset)
        a <- seq(-38, 53, length.out = 13)
        radial(31.2 + 0.55 * sin(a * pi/180)^2, a, side, 3L + offset)
    }
    bars <- c(33.91003017, 37.72900427, 39.55820734, 45.32606849, 48.2777114, 50.66005694, 55.32264039,
        59.78549519, 63.29513551, 68.13709972, 71.92772589, 74.5867339)
    a <- seq(-65, 30, length.out = 9)
    for (i in seq_along(bars)) for (side in c(1, -1)) radial(bars[i] + 0.14 * sin(a * pi/180), a, side,
        4L + (i - 1L) * 2L + if (side == 1)
            1L
        else 2L, colour = "#aac4cf")
    surface <- .attachOverlaySurfaceRays(geometry, requests)
    surface$line.colour <- colours
    surface
}


# .referenceMarlinControls
.referenceMarlinControls <- function() list(body = matrix(c(18.6, 0.9780506334, 1.766440391, 3.701715149,
    21.5, 2.61292122, 1.035367137, 5.577560714, 25, 4.037592212, 0.04276090941, 7.388827782, 30, 4.903575885,
    -1.940234659, 9.009159166, 35, 4.476798384, -4.003753766, 9.767688007, 40, 4.103483307, -4.594673713,
    9.422572666, 45, 3.85699975, -4.132744915, 9.103033826, 52, 3.291782765, -2.834432456, 8.618985919,
    60, 2.514729405, -1.594043742, 7.059321814, 68, 1.625544148, -0.1970879644, 5.301036795, 74, 0.8453637561,
    0.9153032658, 4.445940554, 78, 0.4545818086, 0.9341618801, 4.409289982, 80.5, 0.4354951608, 0.9893766226,
    4.382144242, 82, 0.22, 1.9, 3.5), ncol = 4, byrow = TRUE), body_vertices = matrix(c(31.4, 0.9780506334,
    2.73407777, 31.4, 0.3082492083, 3.039045065, 31.4, 5.988832888e-17, 3.701715149, 31.4, -0.3082492083,
    3.039045065, 31.4, -0.9780506334, 2.73407777, 31.4, -0.7358613503, 2.006051097, 31.4, -1.796649866e-16,
    1.766440391, 31.4, 0.7358613503, 2.006051097, 28.5, 2.61292122, 3.306463926, 28.5, 1.850557256, 4.91492981,
    28.5, 1.599952804e-16, 5.577560714, 28.5, -1.850557256, 4.91492981, 28.5, -2.61292122, 3.306463926,
    28.5, -1.650012866, 1.872307055, 28.5, -4.799858412e-16, 1.035367137, 28.5, 1.650012866, 1.872307055,
    25, 4.037592212, 3.715794345, 25, 2.803829318, 6.266462737, 25, 2.472312189e-16, 7.388827782, 25,
    -2.803829318, 6.266462737, 25, -4.037592212, 3.715794345, 25, -3.564249192, 0.4733652521, 25, -7.416936568e-16,
    0.04276090941, 25, 3.564249192, 0.4733652521, 20, 4.903575885, 3.534462254, 20, 3.496524705, 7.438228292,
    20, 3.002574256e-16, 9.009159166, 20, -3.496524705, 7.438228292, 20, -4.903575885, 3.534462254, 20,
    -3.362411465, -0.2195703337, 20, -9.007722768e-16, -1.940234659, 20, 3.362411465, -0.2195703337,
    15, 4.476798384, 2.88196712, 15, 3.355047879, 8.042334256, 15, 2.741248406e-16, 9.767688007, 15,
    -3.355047879, 8.042334256, 15, -4.476798384, 2.88196712, 15, -2.88585945, -1.556745769, 15, -8.223745218e-16,
    -4.003753766, 15, 2.88585945, -1.556745769, 10, 4.103483307, 2.413949476, 10, 3.070000585, 7.657415985,
    10, 2.512658849e-16, 9.422572666, 10, -3.070000585, 7.657415985, 10, -4.103483307, 2.413949476, 10,
    -2.652107475, -2.115768463, 10, -7.537976547e-16, -4.594673713, 10, 2.652107475, -2.115768463, 5,
    3.85699975, 2.485144455, 5, 2.867415155, 7.405091955, 5, 2.361731199e-16, 9.103033826, 5, -2.867415155,
    7.405091955, 5, -3.85699975, 2.485144455, 5, -2.414335535, -1.657403252, 5, -7.085193597e-16, -4.132744915,
    5, 2.414335535, -1.657403252, -2, 3.291782765, 2.892276731, -2, 2.461769951, 7.175013959, -2, 2.015635614e-16,
    8.618985919, -2, -2.461769951, 7.175013959, -2, -3.291782765, 2.892276731, -2, -2.052369937, -0.6782279499,
    -2, -6.046906841e-16, -2.834432456, -2, 2.052369937, -0.6782279499, -10, 2.514729405, 2.732639036,
    -10, 1.960007057, 6.104901978, -10, 1.539827658e-16, 7.059321814, -10, -1.960007057, 6.104901978,
    -10, -2.514729405, 2.732639036, -10, -1.533763462, 0.0937435976, -10, -4.619482974e-16, -1.594043742,
    -10, 1.533763462, 0.0937435976, -18, 1.625544148, 2.551974416, -18, 1.317778323, 4.780554191, -18,
    9.953587186e-17, 5.301036795, -18, -1.317778323, 4.780554191, -18, -1.625544148, 2.551974416, -18,
    -1.034927531, 0.801741826, -18, -2.986076156e-16, -0.1970879644, -18, 1.034927531, 0.801741826, -24,
    0.8453637561, 2.68062191, -24, 0.6565560154, 4.051665519, -24, 5.17636009e-17, 4.445940554, -24,
    -0.6565560154, 4.051665519, -24, -0.8453637561, 2.68062191, -24, -0.6009907322, 1.425611701, -24,
    -1.552908027e-16, 0.9153032658, -24, 0.6009907322, 1.425611701, -28, 0.4545818086, 2.671725931, -28,
    0.3322403895, 3.941660069, -28, 2.783510784e-17, 4.409289982, -28, -0.3322403895, 3.941660069, -28,
    -0.4545818086, 2.671725931, -28, -0.3158774461, 1.464336448, -28, -8.350532353e-17, 0.9341618801,
    -28, 0.3158774461, 1.464336448, -30.5, 0.4354951608, 2.685760432, -30.5, 0.3168455117, 3.919968398,
    -30.5, 2.666638774e-17, 4.382144242, -30.5, -0.3168455117, 3.919968398, -30.5, -0.4354951608, 2.685760432,
    -30.5, -0.3035312582, 1.503415464, -30.5, -7.999916321e-17, 0.9893766226, -30.5, 0.3035312582, 1.503415464,
    -32, 0.275, 2.7, -32, 0.1944543648, 3.407106781, -32, 1.347111479e-17, 3.5, -32, -0.1944543648, 3.407106781,
    -32, -0.275, 2.7, -32, -0.1944543648, 1.992893219, -32, -4.041334437e-17, 1.9, -32, 0.1944543648,
    1.992893219), ncol = 3, byrow = TRUE), bill = matrix(c(0.03, 0.015, 2.257568257, 2.339422324, 10,
    0.4062001758, 2.111490259, 2.986721235, 19.4, 0.66, 1.9, 3.25), ncol = 4, byrow = TRUE), pectoral = matrix(c(3.7,
    17.8, 13.6, 3.1, 4.8, 0.5, 5.3, 18.60504765, 15.01495086, 4.20364484, 6.562472047, 3.430241299, 6.8,
    15.51099765, 12.26153921, 3.732057517, 5.770786803, 2.650086538, 8.5, 11.38819558, 9.826984614, 3.179217559,
    4.207201008, 1.278272568, 10, 7.934492007, 7.065452094, 3.069284792, 3.400506949, 0.4503802911, 10.7,
    6.025189573, 5.721983108, 3.083777146, 3.281104012, 0.1810699172), ncol = 6, byrow = TRUE), pelvic = matrix(c(1.1,
    15.7, 12.5, 9.1, 10.4, 0.22, 3.5, 11.04997309, 6.990219745, 11.78929071, 13.64143538, 0.448179224,
    5.99, 2.470652897, 2.19036634, 15.01570074, 15.01441411, 0.03518623286), ncol = 6, byrow = TRUE),
    dorsal = matrix(c(31.8, -2.868086139, -2.333101538, 0.32, 35.5, -5.874384238, -3.712845761, 0.2904,
        38.5, -13.04896749, -4.067397729, 0.2664, 40.83289119, -15.59548465, -4.167726428, 0.2477368705,
        42.2, -11.71552201, -4.041425042, 0.2368, 46, -6.594907578, -3.597271706, 0.2064, 54, -4.816531892,
        -2.174335277, 0.1424, 67.5, -2.003780722, 0.06560229951, 0.045), ncol = 4, byrow = TRUE), second_dorsal = matrix(c(0.5,
        -18.6, -25.5, 0, 0, 0.32, -0.8, -17.97133286, -24.5740622, 0, 0, 0.169677394, -1.82, -18.06168451,
        -18.18110451, 0, 0, 0.01266690017), ncol = 6, byrow = TRUE), anal = matrix(c(8.3, -4.2, -11.4,
        0, 0, 0.38, 11, -7.493082209, -10.40062329, 0, 0, 0.2440261286, 13.72, -11.19217421, -11.28733032,
        0, 0, 0.012), ncol = 6, byrow = TRUE), second_anal = matrix(c(4.7, -18.2, -23.8, 0, 0, 0.23,
        5.9, -19.99075182, -23.36020432, 0, 0, 0.08768229417, 6.83, -22.01799905, -22.34078109, 0, 0,
        0.012), ncol = 6, byrow = TRUE), caudal_upper = matrix(c(2.7, -30.9, -36.1, 0, 0, 0.32, 0, -32.14369661,
        -37.08613183, 0, 0, 0.3233029027, -3.5, -35.53490108, -39.48639445, 0, 0, 0.3275853432, -6.7,
        -39.52929858, -43.59750689, 0, 0, 0.2482371991, -9.2, -44.86902563, -49.59105021, 0, 0, 0.1130886495,
        -9.64, -48.04385836, -49.8411183, 0, 0, 0.02823363599), ncol = 6, byrow = TRUE), caudal_lower = matrix(c(2.7,
        -30.9, -36.1, 0, 0, 0.32, 6, -31.83941191, -37.00438202, 0, 0, 0.3279808663, 10, -35.18712773,
        -39.29863977, 0, 0, 0.2930259776, 13, -38.56703792, -42.20439536, 0, 0, 0.2434425823, 15.25,
        -43.83374752, -46.72177783, 0, 0, 0.06622346082, 15.66, -46.26757951, -46.73161006, 0, 0, 0.012),
        ncol = 6, byrow = TRUE))


# .referenceMarlinSpec
.referenceMarlinSpec <- function() {
    controls <- .referenceMarlinControls()
    b <- controls$body
    z <- (b[, 3] + b[, 4])/2
    body <- .anatomyBody(b[, 1], b[, 2], z - b[, 3], b[, 4] - z, z)
    foil <- function(id, plane = "xy", role = "rigid") {
        p <- controls[[id]]
        fin <- .anatomyFoil(id, cbind(p[, 1:3], (p[, 4] + p[, 5])/2, p[, 6]), plane, role, if (id ==
            "pectoral")
            6L
        else 4L)
        axis <- if (plane == "xy")
            3L
        else 2L
        fin$leading[, axis] <- p[, 4]
        fin$trailing[, axis] <- p[, 5]
        if (plane == "xy") {
            d <- colSums(fin$leading - fin$trailing)
            fin$normal <- c(-d[3]/d[1], 0, 1)
            fin$normal <- fin$normal/sqrt(sum(fin$normal^2))
        }
        if (plane == "xy") {
            nsection <- fin$sections
            a <- (seq_len(nsection) - 1L) * 2 * pi/nsection
            fin$section.vertices <- do.call(rbind, lapply(seq_len(nrow(p)), function(i) {
                d <- fin$leading[i, ] - fin$trailing[i, ]
                n <- c(-d[3], 0, d[1])
                n <- n/sqrt(sum(n^2))
                centre <- (fin$leading[i, ] + fin$trailing[i, ])/2
                matrix(centre, nsection, 3, byrow = TRUE) + outer(cos(a), d/2) + outer(sin(a), n * p[i,
                  6])
            }))
        }
        fin$pivot <- (fin$leading[1, ] + fin$trailing[1, ])/2
        fin
    }
    bill <- controls$bill
    bz <- (bill[, 3] + bill[, 4])/2
    bill <- list(id = "bill", kind = "loft", part = "body", role = "rigid", joint.x = NA_real_, sections = 6L,
        pivot = c(50 - tail(bill[, 1], 1), 0, tail(bz, 1)), stations = .anatomyBody(bill[, 1], bill[,
            2], bz - bill[, 3], bill[, 4] - bz, bz))
    d <- controls$dorsal
    dorsal <- list(id = "dorsal", kind = "foil", part = "fin", role = "axial", joint.x = NA_real_, sections = 4L,
        pivot = c(50 - d[1, 1], 0, d[1, 3]), normal = c(0, 1, 0), thickness = d[, 4], leading = cbind(50 -
            d[, 1], 0, d[, 2]), trailing = cbind(50 - d[, 1], 0, d[, 3]))
    keel <- .speciesFinSpec("keel", rbind(c(-26.5, 0.25, 2.7), c(-32.3, 0.22, 2.7), c(-30, 0.67, 2.7)),
        normal = c(0, 0, 1), thickness = 0.11, role = "axial")
    spec <- .anatomySpec("Striped marlin", "Kajikia audax", body, c(list(bill, dorsal, foil("second_dorsal",
        "xz", "attached"), foil("anal", "xz", "attached"), foil("second_anal", "xz", "attached")), .anatomyPair(foil("pectoral")),
        .anatomyPair(foil("pelvic")), list(foil("caudal_upper", "xz", "terminal"), foil("caudal_lower",
            "xz", "terminal")), .speciesPair(keel, "keel_right", "keel_left")), type = "lateral", sections = 8L,
        pivot = c(0, 0, 2.7), tip = -31)
    spec$body.angles <- (0:7) * pi/4
    rings <- controls$body_vertices
    ix <- unlist(lapply(rev(seq_len(nrow(b))), function(i) (i - 1L) * 8L + seq_len(8L)))
    rings <- rings[ix, , drop = FALSE]
    spec$body.transform <- function(vertices) {
        n <- nrow(rings)
        vertices[seq_len(n), ] <- rings
        vertices[n + 1L, ] <- colMeans(rings[1:8, , drop = FALSE])
        vertices[n + 2L, ] <- colMeans(tail(rings, 8))
        vertices
    }
    spec$appearance <- list(smooth = TRUE, smooth.faces = rep(TRUE, 696L), cull = TRUE, fit = "legacy",
        eye.cex = 0.7, compass = c(1.12, 1.1, 0.18), dorsal = "#2b456d", ventral = "#d2d9d6")
    spec$surface <- "marlin"
    spec
}


# .anatomyMarlin
.anatomyMarlin <- function() .referenceMarlinSpec()

# Yellowfin tuna --------------------------------------------------------


# .referenceTunaControls
.referenceTunaControls <- function() list(body = matrix(c(0.06301197227, 0.07183364839, 0.4410838059,
    0.6301197227, 3.402646503, 2.873345936, -2.835538752, 4.725897921, 10.33396345, 6.129804663, -7.309388784,
    8.821676118, 18.52551985, 8.50031506, -10.9010712, 11.46817895, 27.34719597, 9.793320731, -13.23251418,
    12.53938248, 37.42911153, 9.81726528, -13.04347826, 12.79143037, 48.14114682, 9.098928796, -12.79143037,
    11.15311909, 59.48330183, 6.441083806, -9.388783869, 7.561436673, 70.82545684, 3.256458727, -4.788909893,
    3.780718336, 80.27725268, 1.34089477, -2.142407057, 1.38626339, 86.57844991, 0.9338374291, -1.449275362,
    1.008191556, 88.78386894, 1.005671078, -1.260239445, 1.38626339), ncol = 4, byrow = TRUE), dorsal = matrix(c(23.91304348,
    -12.66540643, -12.45836709, 0.38, 20.76244486, -23.88153749, -12.75598614, 0.16, 17.61184625, -20.2268431,
    -12.69691241, 0.16, 13.83112791, -16.95022054, -12.62602394, 0.16, 9.420289855, -15.56395715, -12.52826272,
    0.16, 5.324511657, -14.68178954, -12.43189147, 0.16, 3.119092628, -13.10649023, -12.37999926, 0.16),
    ncol = 4, byrow = TRUE), second_dorsal = matrix(c(1.228733459, -12.79143037, -12.16131065, 0.42,
    -4.442344045, -19.91178324, -14.42974165, 0.3436363636, -10.11342155, -25.39382483, -21.48708255,
    0.2672727273, -15.78449905, -29.42659105, -27.41020794, 0.1909090909, -21.45557656, -32.51417769,
    -31.1279143, 0.1145454545, -27.06364209, -35.60176434, -35.47574039, 0.03818181818), ncol = 4, byrow = TRUE),
    anal = matrix(c(-1.921865154, 9.514807813, 10.33396345, 0.42, -7.592942659, 7.718966604, 14.5557656,
        0.3436363636, -13.89413989, 15.62696912, 19.0926276, 0.2672727273, -20.19533711, 20.2268431,
        21.86515438, 0.1909090909, -27.12665406, 22.18021424, 22.99936988, 0.1145454545, -34.37303088,
        22.87334594, 22.93635791, 0.03818181818), ncol = 4, byrow = TRUE), pectoral = matrix(c(8, 23.15689981,
        20.76244486, -0.6301197227, 0, 0.65, 9.5, 21.07750473, 14.46124764, -1.197227473, 0.5671077505,
        0.55, 11, 16.35160681, 8.16005041, -1.323251418, 0.3780718336, 0.35, 12.1, 8.16005041, 1.858853182,
        -1.260239445, -0.3780718336, 0.18, 12.4, -0.9766855703, -1.291745432, -1.197227473, -1.071203529,
        0.025), ncol = 6, byrow = TRUE), caudal_upper = matrix(c(0, -35.0031506, -41.30434783, 0, 0,
        0.95, -3.150598614, -35.31821046, -41.55639572, 0, 0, 0.7, -7.561436673, -38.4057971, -43.32073094,
        0, 0, 0.45, -12.60239445, -41.68241966, -45.71518589, 0, 0, 0.25, -17.01323251, -45.58916194,
        -48.42470069, 0, 0, 0.1, -18.90359168, -48.99180844, -50, 0, 0, 0.025), ncol = 6, byrow = TRUE),
    caudal_lower = matrix(c(0, -35.0031506, -41.30434783, 0, 0, 0.95, 3.780718336, -36.01134216, -41.05229994,
        0, 0, 0.7, 8.821676118, -38.46880907, -42.37555135, 0, 0, 0.45, 13.8626339, -41.49338374, -44.83301827,
        0, 0, 0.25, 18.27347196, -45.58916194, -47.66855703, 0, 0, 0.1, 19.72274732, -48.23566478, -48.99180844,
        0, 0, 0.025), ncol = 6, byrow = TRUE), dorsal_finlets = list(matrix(c(-5.261499685, 0, -10.52930057,
        -6.647763075, 0, -11.34215501, -8.034026465, 0, -9.697542533), ncol = 3, byrow = TRUE), matrix(c(-9.294265911,
        0, -9.319470699, -10.6805293, 0, -10.39697543, -12.06679269, 0, -8.215010852), ncol = 3, byrow = TRUE),
        matrix(c(-12.88594833, 0, -7.882797732, -14.14618778, 0, -8.821676118, -15.40642722, 0, -6.860603515),
            ncol = 3, byrow = TRUE), matrix(c(-16.79269061, 0, -6.298396695, -17.92690611, 0, -7.561436673,
            -19.06112161, 0, -5.3784219), ncol = 3, byrow = TRUE), matrix(c(-20.0063012, 0, -4.995099069,
            -21.07750473, 0, -6.175173283, -22.14870825, 0, -4.292375551), ncol = 3, byrow = TRUE), matrix(c(-23.21991178,
            0, -3.992438563, -24.22810334, 0, -5.229993699, -25.2362949, 0, -3.427851292), ncol = 3,
            byrow = TRUE), matrix(c(-26.11846251, 0, -3.18084436, -27.00063012, 0, -4.032766226, -27.88279773,
            0, -2.686830498), ncol = 3, byrow = TRUE), matrix(c(-28.44990548, 0, -2.528040328, -29.20604915,
            0, -3.087586641, -29.96219282, 0, -2.104599874), ncol = 3, byrow = TRUE), matrix(c(-31.78954001,
            0, -1.850031506, -32.41965974, 0, -2.221802142, -33.04977946, 0, -1.711405167), ncol = 3,
            byrow = TRUE)), ventral_finlets = list(matrix(c(-10.23944549, 0, 7.183364839, -11.373661,
        0, 7.813484562, -12.5078765, 0, 6.427221172), ncol = 3, byrow = TRUE), matrix(c(-12.75992439,
        0, 6.343205209, -13.89413989, 0, 6.93131695, -15.02835539, 0, 5.587061542), ncol = 3, byrow = TRUE),
        matrix(c(-15.34341525, 0, 5.482041588, -16.41461878, 0, 6.301197227, -17.48582231, 0, 4.767905902),
            ncol = 3, byrow = TRUE), matrix(c(-18.11594203, 0, 4.557865995, -19.25015753, 0, 5.734089477,
            -20.38437303, 0, 3.801722327), ncol = 3, byrow = TRUE), matrix(c(-20.95148078, 0, 3.622768326,
            -22.08569628, 0, 4.347826087, -23.21991178, 0, 3.048099139), ncol = 3, byrow = TRUE), matrix(c(-23.91304348,
            0, 2.872505776, -25.04725898, 0, 3.843730309, -26.18147448, 0, 2.297836589), ncol = 3, byrow = TRUE),
        matrix(c(-27.06364209, 0, 2.074354127, -28.07183365, 0, 2.898550725, -29.0800252, 0, 1.563537072),
            ncol = 3, byrow = TRUE), matrix(c(-29.52110901, 0, 1.451795841, -30.27725268, 0, 2.268431002,
            -31.03339635, 0, 1.214870825), ncol = 3, byrow = TRUE), matrix(c(-31.97857593, 0, 1.15816005,
            -32.60869565, 0, 1.890359168, -33.23881537, 0, 1.082545684), ncol = 3, byrow = TRUE)))


# .referenceTunaSpec
.referenceTunaSpec <- function() {
    controls <- .referenceTunaControls()
    b <- controls$body
    z <- (b[, 3] + b[, 4])/2
    body <- .anatomyBody(b[, 1], b[, 2], z - b[, 3], b[, 4] - z, z)
    median <- function(id, role = "attached") {
        p <- controls[[id]]
        list(id = id, kind = "foil", part = "fin", role = role, joint.x = p[1, 1], sections = 4L, pivot = c(p[1,
            1], 0, mean(p[1, 2:3])), normal = c(0, 1, 0), thickness = p[, 4], leading = cbind(p[, 1],
            0, p[, 2]), trailing = cbind(p[, 1], 0, p[, 3]))
    }
    p <- controls$pectoral
    pectoral <- .anatomyFoil("pectoral", cbind(p[, 1:3], (p[, 4] + p[, 5])/2, p[, 6]), sections = 4L)
    pectoral$leading[, 3] <- p[, 4]
    pectoral$trailing[, 3] <- p[, 5]
    pectoral$pivot <- (pectoral$leading[1, ] + pectoral$trailing[1, ])/2
    a <- (0:3) * pi/2
    pectoral$section.vertices <- do.call(rbind, lapply(seq_len(nrow(p)), function(i) {
        d <- pectoral$leading[i, ] - pectoral$trailing[i, ]
        n <- c(-d[3], 0, d[1])
        n <- n/sqrt(sum(n^2))
        centre <- (pectoral$leading[i, ] + pectoral$trailing[i, ])/2
        matrix(centre, 4, 3, byrow = TRUE) + outer(cos(a), d/2) + outer(sin(a), n * p[i, 6])
    }))
    pelvic <- .speciesFinSpec("pelvic", rbind(c(20.3, 4.5, 11), c(13.8, 6, 19.3), c(15.6, 4.5, 12)),
        normal = c(0, 1, 0), thickness = 0.3, role = "rigid")
    keel <- .speciesFinSpec("keel", rbind(c(-33.5, 0.6, 0.25), c(-39.5, 0.5, 0.35), c(-37.5, 2.8, 0.3)),
        normal = c(0, 0, 1), thickness = 0.2, role = "axial")
    tail <- lapply(c("caudal_upper", "caudal_lower"), function(id) {
        p <- controls[[id]]
        fin <- .anatomyFoil(id, cbind(p[, 1:3], 0, p[, 6]), "xz", "terminal", 4L)
        fin
    })
    finlets <- unlist(lapply(c("dorsal", "ventral"), function(edge) lapply(seq_len(9), function(i) {
        outline <- controls[[paste0(edge, "_finlets")]][[i]]
        out <- .speciesFinSpec(paste0(edge, "_finlet_", i), outline, normal = c(0, 1, 0), thickness = 0.12 -
            (i - 1) * 0.009, role = "attached", joint.x = mean(outline[c(1, 3), 1]))
        out
    })), recursive = FALSE)
    spec <- .anatomySpec("Yellowfin tuna", "Thunnus albacares", body, c(list(median("dorsal"), median("second_dorsal"),
        median("anal")), .anatomyPair(pectoral), .speciesPair(pelvic, "pelvic_right", "pelvic_left"),
        .speciesPair(keel, "keel_right", "keel_left"), tail, finlets), type = "lateral", sections = 10L,
        pivot = c(0, 0, 0), tip = -35.3)
    spec$body.angles <- c(0, pi/4, pi/2, 3 * pi/4, pi, 7 * pi/6, 4 * pi/3, 3 * pi/2, 5 * pi/3, 11 * pi/6)
    spec$appearance <- list(smooth = TRUE, smooth.faces = rep(TRUE, 700L), cull = TRUE, fit = "legacy",
        eye.cex = 0.7, compass = c(1.12, 1.1, 0.18), dorsal = "#233d53", ventral = "#d6dcde")
    spec$surface <- "tuna"
    spec
}


# .anatomyTuna
.anatomyTuna <- function() .referenceTunaSpec()


# .tunaReferenceFaceRGB
.tunaReferenceFaceRGB <- function(geometry) {
    quad <- matrix(c(0.279301, 0.331583, 0.393855, 0.28042, 0.334125, 0.396018, 0.28042, 0.334125, 0.396018,
        0.279301, 0.331583, 0.393855, 0.301629, 0.341042, 0.392101, 0.189582, 0.228089, 0.293683, 0.181399,
        0.221229, 0.290763, 0.181399, 0.221229, 0.290763, 0.189582, 0.228089, 0.293683, 0.301629, 0.341042,
        0.392101, 0.470256, 0.501417, 0.537984, 0.322849, 0.360416, 0.399581, 0.322849, 0.360416, 0.399581,
        0.470256, 0.501417, 0.537984, 0.408016, 0.440123, 0.481235, 0.142963, 0.176266, 0.232168, 0.217303,
        0.229886, 0.253652, 0.217303, 0.229886, 0.253652, 0.142963, 0.176266, 0.232168, 0.408016, 0.440123,
        0.481235, 0.789016, 0.809333, 0.821174, 0.697124, 0.718139, 0.741056, 0.697124, 0.718139, 0.741056,
        0.789016, 0.809333, 0.821174, 0.337994, 0.379389, 0.433684, 0.111299, 0.133956, 0.184451, 0.279914,
        0.271033, 0.237032, 0.279914, 0.271033, 0.237032, 0.111299, 0.133956, 0.184451, 0.337994, 0.379389,
        0.433684, 0.818102, 0.837218, 0.839548, 0.672509, 0.699682, 0.745448, 0.672509, 0.699682, 0.745448,
        0.818102, 0.837218, 0.839548, 0.32184, 0.405937, 0.463958, 0.113628, 0.139561, 0.192494, 0.163015,
        0.170584, 0.188333, 0.163015, 0.170584, 0.188333, 0.113628, 0.139561, 0.192494, 0.32184, 0.405937,
        0.463958, 0.858051, 0.868704, 0.842809, 0.724343, 0.754937, 0.800325, 0.724343, 0.754937, 0.800325,
        0.858051, 0.868704, 0.842809, 0.483434, 0.535303, 0.47166, 0.121303, 0.15038, 0.205416, 0.183508,
        0.203526, 0.239749, 0.183508, 0.203526, 0.239749, 0.121303, 0.15038, 0.205416, 0.483434, 0.535303,
        0.47166, 0.829639, 0.839613, 0.817588, 0.761308, 0.790287, 0.824499, 0.761308, 0.790287, 0.824499,
        0.829639, 0.839613, 0.817588, 0.641933, 0.620845, 0.399747, 0.195544, 0.224151, 0.247354, 0.182811,
        0.214256, 0.266531, 0.182811, 0.214256, 0.266531, 0.195544, 0.224151, 0.247354, 0.641933, 0.620845,
        0.399747, 0.820971, 0.831121, 0.812013, 0.741594, 0.768638, 0.801627, 0.741594, 0.768638, 0.801627,
        0.820971, 0.831121, 0.812013, 0.604619, 0.575987, 0.330043, 0.202019, 0.237241, 0.258462, 0.242156,
        0.273295, 0.318286, 0.242156, 0.273295, 0.318286, 0.202019, 0.237241, 0.258462, 0.604619, 0.575987,
        0.330043, 0.765531, 0.769331, 0.726964, 0.749765, 0.767741, 0.778579, 0.749765, 0.767741, 0.778579,
        0.765531, 0.769331, 0.726964, 0.69461, 0.675447, 0.456968, 0.218123, 0.255465, 0.29991, 0.303145,
        0.336214, 0.37521, 0.303145, 0.336214, 0.37521, 0.218123, 0.255465, 0.29991, 0.69461, 0.675447,
        0.456968, 0.858029, 0.852375, 0.762908, 0.795786, 0.808221, 0.799018, 0.795786, 0.808221, 0.799018,
        0.858029, 0.852375, 0.762908, 0.683103, 0.654391, 0.430846, 0.288761, 0.312492, 0.331658, 0.372408,
        0.395588, 0.412928, 0.372408, 0.395588, 0.412928, 0.288761, 0.312492, 0.331658, 0.683103, 0.654391,
        0.430846, 0.576226, 0.574041, 0.505451, 0.540738, 0.558254, 0.571556, 0.540738, 0.558254, 0.571556,
        0.576226, 0.574041, 0.505451, 0.430667, 0.427484, 0.352614, 0.293992, 0.308244, 0.311009, 0.224696,
        0.235529, 0.249888, 0.224696, 0.235529, 0.249888, 0.293992, 0.308244, 0.311009, 0.430667, 0.427484,
        0.352614, 0.28886, 0.310225, 0.326635, 0.215599, 0.226754, 0.249327, 0.215599, 0.226754, 0.249327,
        0.28886, 0.310225, 0.326635, 0.365084, 0.382228, 0.380584, 0.2823, 0.296903, 0.309224, 0.191991,
        0.204695, 0.23241, 0.191991, 0.204695, 0.23241, 0.2823, 0.296903, 0.309224, 0.365084, 0.382228,
        0.380584), ncol = 3L, byrow = TRUE)
    rgb <- matrix(0, length(geometry$faces), 3L)
    rgb[1:220, ] <- quad[rep(seq_len(110L), each = 2L), ]
    rgb[seq(221, 239, 2), ] <- matrix(colMeans(quad[1:10, ]), 10, 3, byrow = TRUE)
    rgb[seq(222, 240, 2), ] <- matrix(colMeans(tail(quad, 10)), 10, 3, byrow = TRUE)
    family <- list(dorsal = c(0.27, 0.34, 0.39), second_dorsal = c(0.795353, 0.707824, 0.266529), anal = c(0.795353,
        0.707824, 0.266529), pectoral = c(0.34, 0.39, 0.38), pelvic = c(0.7, 0.66, 0.42), keel = c(0.23,
        0.32, 0.4), caudal_upper = c(0.19, 0.27, 0.36), caudal_lower = c(0.19, 0.27, 0.36), dorsal_finlet = c(0.795353,
        0.707824, 0.266529), ventral_finlet = c(0.795353, 0.707824, 0.266529))
    for (id in unique(geometry$face.region[geometry$face.region != "body"])) {
        key <- sub("_(right|left|[0-9]+)$", "", id)
        keep <- geometry$face.region == id
        rgb[keep, ] <- matrix(family[[key]], sum(keep), 3, byrow = TRUE)
    }
    rgb
}


# .compileTunaSurface
.compileTunaSurface <- function(spec, geometry) {
    requests <- list()
    colours <- character()
    add <- function(origin, direction, kind = "line", group = 0L, colour = "#35413e") {
        n <- nrow(origin)
        if (is.null(dim(direction)))
            direction <- matrix(direction, n, 3, byrow = TRUE)
        requests[[length(requests) + 1L]] <<- list(origin = origin, direction = direction, kind = rep(kind,
            n), group = rep(group, n), regions = "body")
        colours <<- c(colours, rep(colour, n))
    }
    eye <- c(1435, 668)
    for (side in c(1, -1)) add(matrix(c(50 - (1594 - eye[1])/1587 * 100, side * 30, (eye[2] - 700)/1587 *
        100), 1, 3), c(0, -side, 0), "eye")
    radial <- function(s, angle, side, group) {
        z <- stats::approx(spec$body[, 1], spec$body[, 5], 50 - s, rule = 2)$y
        a <- angle * pi/180
        add(cbind(50 - s, 0, z), cbind(0, side * cos(a), sin(a)), group = group)
    }
    for (side in c(1, -1)) radial(seq(0.4, 9.6, length.out = 17), seq(5, 32, length.out = 17), side,
        if (side == 1)
            1L
        else 2L)
    project.profile <- function(points, n, side, group, colour = "#35413e") {
        at <- seq(1, nrow(points), length.out = n)
        xy <- cbind(stats::approx(seq_len(nrow(points)), points[, 1], at)$y, stats::approx(seq_len(nrow(points)),
            points[, 2], at)$y)
        add(cbind(50 - (1594 - xy[, 1])/1587 * 100, side * 30, (xy[, 2] - 700)/1587 * 100), c(0, -side,
            0), group = group, colour = colour)
    }
    operculum <- rbind(c(1240, 568), c(1221, 591), c(1206, 620), c(1195, 654), c(1193, 694), c(1195,
        739), c(1190, 786), c(1183, 831), c(1174, 874))
    lateral <- rbind(c(1140, 675), c(1060, 656), c(960, 642), c(860, 639), c(760, 643), c(650, 656),
        c(530, 677), c(420, 692), c(320, 698), c(225, 699))
    for (side in c(1, -1)) {
        project.profile(operculum, 13L, side, if (side == 1)
            3L
        else 4L)
        project.profile(lateral, 21L, side, if (side == 1)
            5L
        else 6L, colour = "#34414a")
    }
    surface <- .attachOverlaySurfaceRays(geometry, requests)
    surface$line.colour <- colours
    surface
}

# Species registry ------------------------------------------------------


# .overlaySpeciesSpecs
.overlaySpeciesSpecs <- function() {
    list(blue_shark = .anatomyBlueShark(), whale_shark = .anatomyWhaleShark(), mako = .anatomyMako(),
        hammerhead = .anatomyHammerhead(), manta = .anatomyManta(), devil_ray = .anatomyDevilRay(), turtle = .anatomyTurtle(),
        dolphin = .anatomyDolphin(), whale = .anatomyHumpback(), marlin = .anatomyMarlin(), tuna = .anatomyTuna())
}
