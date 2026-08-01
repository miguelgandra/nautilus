# Tests for plotTheme(): the shared look-and-feel object for the plot family.
#
# plotTheme() had no tests of its own until a palette regression made the case for them. Its fields are
# read by every plotting function, so a change here is a change to every figure the package draws - and
# a default that no test describes is a default that can move silently.

test_that("the default continuous ramp spans real hue contrast, not one colour", {
  # REGRESSION LOCK. The theme migration replaced this ramp with a three-stop blue sequential palette,
  # on the reasonable-sounding grounds that it was moving only the SOURCE of the colours. But for a
  # trace whose colour IS the data, the base colours are the whole palette: a page of depth profiles
  # went monochrome and temperature variation became unreadable. Nothing caught it, because no test
  # asserted anything about what the default looks like.
  #
  # Deliberately a property, not a fixed vector of hex codes: the ramp may be restyled, but it must
  # stay discriminable. A monochrome ramp of any hue fails this.
  ramp <- grDevices::colorRampPalette(plotTheme()$sequential)(100)
  rgb  <- grDevices::col2rgb(ramp)

  # every channel has to do real work; a blue-only ramp leaves red and green nearly flat
  spread <- apply(rgb, 1L, function(ch) diff(range(ch)))
  expect_true(all(spread > 150), label = "per-channel spread across the default ramp")

  # and the two ends must be opposite in hue, not merely light and dark versions of one colour
  expect_gt(rgb["blue", 1] - rgb["red", 1], 80)            # cold end is blue-dominant
  expect_gt(rgb["red", 100] - rgb["blue", 100], 80)        # warm end is red-dominant
})

test_that("the continuous ramp remains overridable through plotTheme", {
  th <- plotTheme(sequential = c("#FFFFFF", "#000000"))
  expect_equal(th$sequential, c("#FFFFFF", "#000000"))
  expect_error(plotTheme(sequential = "#FFFFFF"), "sequential")   # a ramp needs >= 2 colours
})


# ---- diel fills and their derived outlines ------------------------------------------------

test_that("the day fill is warm and separates from both the panel and the night bar", {
  th <- plotTheme()
  lum <- function(col) {
    v <- grDevices::col2rgb(col)[, 1] / 255
    v <- ifelse(v <= 0.03928, v / 12.92, ((v + 0.055) / 1.055)^2.4)
    sum(v * c(0.2126, 0.7152, 0.0722))
  }
  contrast <- function(a, b) (max(lum(a), lum(b)) + 0.05) / (min(lum(a), lum(b)) + 0.05)

  # the old pale-blue day sat at 1.14 against the panel and was hard to see; keep a floor under that
  expect_gt(contrast(th$day, th$panel), 1.25)
  # and it must stay clearly distinct from the night bar it is mirrored against
  expect_gt(contrast(th$day, th$night), 4)

  # warm day, cool night: the pairing is what makes the two sides read as separate categories
  rgb_day <- grDevices::col2rgb(th$day)[, 1]
  expect_gt(rgb_day[1], rgb_day[3])                       # more red than blue
  rgb_night <- grDevices::col2rgb(th$night)[, 1]
  expect_gt(rgb_night[3], rgb_night[1])                   # more blue than red
})

test_that("day.border is derived from day, and an explicit value still wins", {
  # the default pair
  th <- plotTheme()
  expect_identical(th$day.border, nautilus:::.darkenColor(th$day))

  # restyling the fill carries the outline with it, so the two never drift apart
  th2 <- plotTheme(day = "#C3DBF0")
  expect_identical(th2$day.border, nautilus:::.darkenColor("#C3DBF0"))

  # naming both keeps the explicit value
  th3 <- plotTheme(day = "#C3DBF0", day.border = "#000000")
  expect_identical(th3$day.border, "#000000")
})

test_that(".darkenColor darkens without changing hue, and is idempotent in direction", {
  d <- nautilus:::.darkenColor("#F6D2AE")
  expect_match(d, "^#[0-9A-Fa-f]{6}$")
  orig <- grDevices::col2rgb("#F6D2AE")[, 1]
  dark <- grDevices::col2rgb(d)[, 1]
  expect_true(all(dark < orig))                           # every channel darker
  # hue preserved: the channel ordering is unchanged
  expect_identical(order(dark), order(orig))
  # darker still with a smaller factor
  expect_true(all(grDevices::col2rgb(nautilus:::.darkenColor("#F6D2AE", 0.5))[, 1] < dark))
})
