# Tests for encoding/confusable-tolerant header matching (Step 0).
# Real tag exporters vary the degree sign (CATS camera firmware writes the
# masculine ordinal U+00BA instead of U+00B0), the micro sign vs Greek mu, the
# superscript two, encoding (UTF-8 vs Latin-1), whitespace and case. The
# importer must match all of these against its canonical default mappings.

deg   <- intToUtf8(0x00B0)  # degree sign
ord   <- intToUtf8(0x00BA)  # masculine ordinal indicator
micro <- intToUtf8(0x00B5)  # micro sign
mu    <- intToUtf8(0x03BC)  # Greek small letter mu
sup2  <- intToUtf8(0x00B2)  # superscript two

test_that(".normalizeHeader folds confusable degree/ordinal/micro symbols", {
  nh <- nautilus:::.normalizeHeader
  expect_identical(nh(paste0("Temperature (", ord, "C)")),
                   nh(paste0("Temperature (", deg, "C)")))
  expect_identical(nh(paste0("Mx (", micro, "T)")),
                   nh(paste0("Mx (", mu, "T)")))
  expect_identical(nh(paste0("Accelerometer X [m/s", sup2, "]")),
                   nh("Accelerometer X [m/s2]"))
})

test_that(".normalizeHeader is tolerant to whitespace, case and Latin-1 encoding", {
  nh <- nautilus:::.normalizeHeader
  expect_identical(nh("  Depth (M) "), nh("depth (m)"))
  latin1 <- iconv(paste0("Temperature (depth) [", deg, "C]"), "UTF-8", "latin1")
  expect_identical(Encoding(latin1), "latin1")
  expect_identical(nh(latin1), nh(paste0("Temperature (depth) [", deg, "C]")))
})

test_that(".normalizeHeader does not collapse genuinely distinct headers", {
  nh <- nautilus:::.normalizeHeader
  expect_false(nh("Date (UTC)") == nh("Date (local)"))
  expect_false(nh("Ax (g)") == nh("Ay (g)"))
})

test_that("importTagData matches real CATS camera headers via default mappings (Step 0 regression)", {
  # Header as written by CATS camera firmware: UTF-8, ordinal degree in temperature,
  # degree-per-second gyro, micro-sign magnetometer, single 'Date' datetime column.
  hdr <- paste(c(
    "Date", "Battery (V)", "Battery (%)",
    "Ax (g)", "Ay (g)", "Az (g)",
    paste0("Gx (", deg, "/s)"), paste0("Gy (", deg, "/s)"), paste0("Gz (", deg, "/s)"),
    paste0("Mx (", micro, "T)"), paste0("My (", micro, "T)"), paste0("Mz (", micro, "T)"),
    "Light (lux)", paste0("Temperature (", ord, "C)"),
    "Pressure (bar)", "Depth (m)", "Velocity (m/s)", "Camera", "External LED"
  ), collapse = ",")

  n <- 30
  dt <- seq(as.POSIXct("2020-01-01 00:00:00", tz = "UTC"), by = 1, length.out = n)
  rows <- vapply(seq_len(n), function(i) paste(c(
    format(dt[i], "%Y-%m-%d %H:%M:%OS3"), 4.0, 90,                 # Date, Battery
    0.4, 1.0, 0.2, 0.5, -0.3, 0.1, 20, -15, 30,                   # Ax..Mz
    500, 21.5, 1.03, runif(1, 0, 40), 0.2, "Disabled", "Disabled" # Light, Temp, Pressure, Depth, Vel, Camera, LED
  ), collapse = ","), character(1))

  root <- file.path(tempdir(), paste0("cats_cam_", as.integer(runif(1, 1, 1e7))))
  dir.create(file.path(root, "ID_01", "CMD"), recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  writeLines(c(hdr, rows), file.path(root, "ID_01", "CMD", "rec.csv"), useBytes = TRUE)

  meta <- data.frame(
    ID = "ID_01", tag = "CATS", type = "MS",
    deploy_date = as.POSIXct("2020-01-01 00:00:00", tz = "UTC"),
    deploy_lon = -25, deploy_lat = 38, stringsAsFactors = FALSE
  )

  res <- NULL
  invisible(capture.output(suppressWarnings(suppressMessages(
    res <- importTagData(
      data.folders = file.path(root, "ID_01"),
      import.mapping = NULL,   # exercise DEFAULT mappings
      id.metadata = meta, columns = metadataColumns(deploy_datetime = "deploy_date"),
      return.data = TRUE, verbose = FALSE
    )
  ))))

  # Before the fix the ordinal temperature header failed to match, 'temp' was
  # reported missing, and the whole file was dropped (empty list).
  expect_named(res, "ID_01")
  dt_out <- res[["ID_01"]]
  expect_true(all(c("ax", "ay", "az", "gx", "gy", "gz",
                    "mx", "my", "mz", "depth", "temp") %in% names(dt_out)))
  expect_equal(nrow(dt_out), n)
  expect_false(all(is.na(dt_out$temp)))
  # gyro should have been standardized from deg/s to rad/s (0.5 deg/s -> ~0.0087)
  expect_lt(median(abs(dt_out$gx), na.rm = TRUE), 0.05)
})
