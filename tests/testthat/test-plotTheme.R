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
