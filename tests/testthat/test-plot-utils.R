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
