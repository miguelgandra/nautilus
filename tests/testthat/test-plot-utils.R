# Tests for the shared plotting infrastructure: plotTheme() (the visual-style control) and the
# .deploymentGroup() grouping resolver used by the grouped plotters.

test_that("plotTheme() builds a themed object from presets with field overrides", {
  th <- plotTheme()
  expect_s3_class(th, "nautilus_theme")
  expect_equal(th$preset, "light")
  expect_true(nautilus:::.isColour(th$day) && nautilus:::.isColour(th$night))

  th2 <- plotTheme("classic", cex = 1.4, day = "#EEF5FC")
  expect_equal(th2$preset, "classic")
  expect_equal(th2$cex, 1.4)
  expect_equal(th2$day, "#EEF5FC")                              # override applied
  expect_equal(th2$panel, "#EDEDED")                            # rest from the preset
})

test_that("plotTheme() validates colours and numeric fields", {
  expect_error(plotTheme(day = "notacolour"), "colour", ignore.case = TRUE)
  expect_error(plotTheme(bar.alpha = 2), "bar.alpha", ignore.case = TRUE)
  expect_error(plotTheme(sequential = "#000000"), "sequential", ignore.case = TRUE)  # needs >= 2
})

test_that(".as_control coerces a list / NULL / object to a theme", {
  expect_s3_class(nautilus:::.as_control(list(preset = "minimal"), plotTheme, "nautilus_theme", "theme"), "nautilus_theme")
  expect_equal(nautilus:::.as_control(NULL, plotTheme, "nautilus_theme", "theme")$preset, "light")
  th <- plotTheme(); expect_identical(nautilus:::.as_control(th, plotTheme, "nautilus_theme", "theme"), th)
})

test_that(".themePalette resolves a colour vector or a named hcl.colors palette to n colours", {
  expect_length(nautilus:::.themePalette("viridis", 4), 4)
  expect_equal(nautilus:::.themePalette(c("#111111", "#222222"), 3), c("#111111", "#222222", "#111111"))
  expect_error(nautilus:::.themePalette("not_a_palette", 3), "palette", ignore.case = TRUE)
})

test_that(".deploymentGroup resolves from a data column, a named vector, a data.frame, or NULL", {
  x <- data.table::data.table(ID = "A01", depth = 1:3, species = "R. typus")
  expect_equal(nautilus:::.deploymentGroup(x, "A01", "species"), "R. typus")
  expect_equal(nautilus:::.deploymentGroup(x, "A01", c(A01 = "Filter feeders", B02 = "Coastal")), "Filter feeders")
  df <- data.frame(id = c("A01", "B02"), grp = c("g1", "g2"), stringsAsFactors = FALSE)
  expect_equal(nautilus:::.deploymentGroup(x, "B02", df), "g2")
  expect_true(is.na(nautilus:::.deploymentGroup(x, "A01", NULL)))
  expect_true(is.na(nautilus:::.deploymentGroup(x, "Z99", c(A01 = "g1"))))       # unmatched id
})


#######################################################################################################
# Time axes are labelled in the DATA's timezone ########################################################
#
# graphics::axis.POSIXct(side, at = ) without `x` sets tz <- "" internally and then OVERWRITES the tick
# vector's own tzone with it, so the labels come out in the analyst's session zone. On a Europe/Lisbon
# machine that silently put every depth profile of a UTC record one hour late: a real dive at 12:26 UTC
# was drawn at 13:26 and could not be reconciled with the camera footage of it. Nothing about the figure
# looked wrong. These tests are the only thing standing between that and a repeat.

test_that("the time axis is labelled in the data's timezone, not the session's", {
  old <- Sys.getenv("TZ", unset = NA)
  on.exit({ if (is.na(old)) Sys.unsetenv("TZ") else Sys.setenv(TZ = old) }, add = TRUE)
  Sys.setenv(TZ = "America/New_York")                       # -4 h in September: a guaranteed mismatch

  t <- seq(as.POSIXct("2019-09-27 10:25:00", tz = "UTC"), by = "10 min", length.out = 14)
  grDevices::pdf(NULL); on.exit(grDevices::dev.off(), add = TRUE)
  plot(t, seq_along(t), xaxt = "n")
  r <- .axisTime(t, n = 5)

  expect_equal(r$tz, "UTC")
  expect_equal(r$labels, format(r$at, "%H:%M", tz = "UTC"))
  # and the regression itself: the session zone must NOT be what got printed
  expect_false(identical(r$labels, format(r$at, "%H:%M", tz = "America/New_York")))
})

test_that("a time vector carrying no zone falls back to UTC, never to the session", {
  old <- Sys.getenv("TZ", unset = NA)
  on.exit({ if (is.na(old)) Sys.unsetenv("TZ") else Sys.setenv(TZ = old) }, add = TRUE)
  Sys.setenv(TZ = "America/New_York")

  t <- seq(as.POSIXct("2019-09-27 10:25:00", tz = "UTC"), by = "10 min", length.out = 14)
  attr(t, "tzone") <- NULL                                  # storage contract says UTC; honour it
  grDevices::pdf(NULL); on.exit(grDevices::dev.off(), add = TRUE)
  plot(t, seq_along(t), xaxt = "n")
  r <- .axisTime(t, n = 5)

  expect_equal(r$tz, "UTC")
  expect_equal(r$labels, format(r$at, "%H:%M", tz = "UTC"))
})

test_that("an explicit tz argument wins, and the format follows the record length", {
  t <- seq(as.POSIXct("2019-09-27 10:00:00", tz = "UTC"), by = "10 min", length.out = 14)
  grDevices::pdf(NULL); on.exit(grDevices::dev.off(), add = TRUE)
  plot(t, seq_along(t), xaxt = "n")
  r <- .axisTime(t, n = 5, tz = "Pacific/Auckland")
  expect_equal(r$tz, "Pacific/Auckland")
  expect_equal(r$labels, format(r$at, "%H:%M", tz = "Pacific/Auckland"))

  # a record longer than a day switches from clock time to dates
  long <- seq(as.POSIXct("2019-09-27 10:00:00", tz = "UTC"), by = "6 hours", length.out = 20)
  plot(long, seq_along(long), xaxt = "n")
  expect_true(all(grepl("^[0-9]{2}/", .axisTime(long, n = 5)$labels)))
  expect_true(all(grepl("^[0-9]{2}:[0-9]{2}$", r$labels)))
})

test_that("an explicit `at` labels a non-time axis in the data's zone", {
  # coverage bars, spectrogram panels and gap zooms plot in bin/sample/offset coordinates, so the tick
  # POSITIONS are not instants. The labels still have to name instants in the data's zone.
  old <- Sys.getenv("TZ", unset = NA)
  on.exit({ if (is.na(old)) Sys.unsetenv("TZ") else Sys.setenv(TZ = old) }, add = TRUE)
  Sys.setenv(TZ = "America/New_York")

  t3 <- as.POSIXct(c("2020-01-01 00:00:00", "2020-01-01 01:00:00", "2020-01-01 02:00:00"), tz = "UTC")
  grDevices::pdf(NULL); on.exit(grDevices::dev.off(), add = TRUE)
  plot(c(0, 100), c(0, 1), type = "n", xaxt = "n")
  r <- .axisTime(t3, at = c(0, 50, 100), fmt = "%d-%b %H:%M")

  expect_equal(r$labels, c("01-Jan 00:00", "01-Jan 01:00", "01-Jan 02:00"))
  expect_false(any(grepl("31-Dec", r$labels)))          # the session zone must not appear
  expect_equal(r$tz, "UTC")
})

test_that("a mismatch between tick positions and timestamps is refused, not silently recycled", {
  t3 <- as.POSIXct(c("2020-01-01 00:00:00", "2020-01-01 01:00:00"), tz = "UTC")
  grDevices::pdf(NULL); on.exit(grDevices::dev.off(), add = TRUE)
  plot(c(0, 100), c(0, 1), type = "n", xaxt = "n")
  expect_error(.axisTime(t3, at = c(0, 50, 100)), "same length")
})

test_that("epoch seconds are accepted, so a panel in shifted coordinates can still be labelled", {
  secs <- as.numeric(as.POSIXct(c("2020-01-01 00:00:00", "2020-01-01 06:00:00"), tz = "UTC"))
  grDevices::pdf(NULL); on.exit(grDevices::dev.off(), add = TRUE)
  plot(c(0, 1), c(0, 1), type = "n", xaxt = "n")
  r <- .axisTime(secs, at = c(0, 1), fmt = "%H:%M", tz = "UTC")
  expect_equal(r$labels, c("00:00", "06:00"))
})

test_that("no function builds a time axis outside .axisTime", {
  # TWO ways to leak the session zone into a figure, both silent:
  #   1. axis.POSIXct(at=) without x= - it OVERWRITES the tick vector's tzone with ""
  #   2. axis(labels = format(<timestamps>)) - correct only while something upstream happens to have
  #      preserved the tzone attribute; c() on POSIXct has not always done so
  # .axisTime() is the sanctioned path for both: it resolves the zone and formats the labels itself.
  ns <- asNamespace("nautilus")
  bodies <- vapply(ls(ns, all.names = TRUE), function(nm) {
    f <- get(nm, envir = ns)
    if (!is.function(f)) "" else paste(deparse(body(f)), collapse = " ")
  }, character(1))
  expect_false(any(grepl("axis.POSIXct", bodies, fixed = TRUE)))
  expect_false(any(grepl("labels = format(",   bodies, fixed = TRUE)))
  expect_false(any(grepl("labels = strftime(", bodies, fixed = TRUE)))
})
