# Solar event times (.solarEventTime) reproduce suntools::sunriset/crepuscule WITHOUT the sf/GDAL stack.
# The reference values below are FROZEN from suntools 1.1.0 (24 location/date cases); the internal NOAA
# implementation must reproduce them (sub-second for sun rise/set, ~3 s for twilight). Regenerated via
# scratchpad/solar_golden.R if ever needed. This test carries no dependency on suntools.

test_that(".solarEventTime matches the frozen suntools reference (sunrise/sunset/dawn/dusk)", {
  ref <- data.frame(
    lon = c(-25, 73, 0, -80, 100, -25, -25, 73, 0, -80, 100, -25, -25, 73, 0, -80, 100, -25, -25, 73, 0, -80, 100, -25),
    lat = c(37, 12, 0, 25, -10, 37, 37, 12, 0, 25, -10, 37, 37, 12, 0, 25, -10, 37, 37, 12, 0, 25, -10, 37),
    date = c("2020-01-15", "2020-01-15", "2020-01-15", "2020-01-15", "2020-01-15", "2020-01-15", "2020-04-15", "2020-04-15", "2020-04-15", "2020-04-15", "2020-04-15", "2020-04-15", "2020-07-15", "2020-07-15", "2020-07-15", "2020-07-15", "2020-07-15", "2020-07-15", "2020-10-15", "2020-10-15", "2020-10-15", "2020-10-15", "2020-10-15", "2020-10-15"),
    sunrise = c(1579078348, 1579051942, 1579068334, 1579090010, 1579043379, 1579078348, 1586934316, 1586912165, 1586930196, 1586948237, 1586906617, 1586934316, 1594794749, 1594774268, 1594792946, 1594809593, 1594769896, 1594794749, 1602748081, 1602723462, 1602740541, 1602760707, 1602716175, 1602748081),
    sunset = c(1579113979, 1579093322, 1579111973, 1579128717, 1579088911, 1579113979, 1586981709, 1586956800, 1586973794, 1586994169, 1586949373, 1586981709, 1594846756, 1594820210, 1594836579, 1594858323, 1594811630, 1594846756, 1602788564, 1602766173, 1602784139, 1602802346, 1602760520, 1602788564),
    dawn6 = c(1579076646, 1579050584, 1579067003, 1579088536, 1579042018, 1579076646, 1586932707, 1586910874, 1586928936, 1586946834, 1586905341, 1586932707, 1594792950, 1594772893, 1594791614, 1594808076, 1594768545, 1594792950, 1602746511, 1602722181, 1602739286, 1602759325, 1602714899, 1602746511),
    dusk12 = c(1579117595, 1579096244, 1579114850, 1579131873, 1579091867, 1579117595, 1586985240, 1586959599, 1586976518, 1586997224, 1586952129, 1586985240, 1594850770, 1594823199, 1594839460, 1594861651, 1594814539, 1594850770, 1602791944, 1602768938, 1602786852, 1602805326, 1602763285, 1602791944),
    stringsAsFactors = FALSE)
  dt  <- as.POSIXct(ref$date, tz = "UTC")
  se  <- nautilus:::.solarEventTime
  tol <- 10  # seconds; measured max error vs suntools 1.1.0 is ~3 s (twilight), <0.1 s (rise/set)
  expect_lt(max(abs(as.numeric(se(ref$lon, ref$lat, dt, 0.833, TRUE))  - ref$sunrise)), tol)
  expect_lt(max(abs(as.numeric(se(ref$lon, ref$lat, dt, 0.833, FALSE)) - ref$sunset)),  tol)
  expect_lt(max(abs(as.numeric(se(ref$lon, ref$lat, dt, 6, TRUE))      - ref$dawn6)),   tol)
  expect_lt(max(abs(as.numeric(se(ref$lon, ref$lat, dt, 12, FALSE))    - ref$dusk12)),  tol)
})

test_that("getDielPhase classifies day/night/dawn/dusk and returns the requested factor levels", {
  crds <- matrix(c(-25, 37), ncol = 2)   # Azores
  d    <- as.POSIXct("2020-07-15", tz = "UTC") + c(2, 6, 13, 20) * 3600  # night, dawn-ish, day, dusk-ish
  p4 <- getDielPhase(d, crds, phases = 4)
  expect_s3_class(p4, "factor")
  expect_identical(levels(p4), c("dawn", "day", "dusk", "night"))
  expect_equal(as.character(p4[3]), "day")     # 13:00 UTC is daytime at the Azores in July
  expect_equal(as.character(p4[1]), "night")   # 02:00 UTC is night
  expect_identical(levels(getDielPhase(d, crds, phases = 2)), c("day", "night"))
  expect_identical(levels(getDielPhase(d, crds, phases = 3)), c("day", "crepuscule", "night"))
})
