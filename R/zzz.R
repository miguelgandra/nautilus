
# data.table uses non-standard evaluation (unquoted column names inside j / by / :=), which R CMD check
# flags as "no visible binding for global variable". Declaring those names here silences the NOTE; "."
# is data.table's .() helper, likewise used unquoted.
utils::globalVariables(c(
  ".", ".bucket", "accel", "ax", "ay", "az", "corr_e", "corr_n", "datetime", "depth",
  "disp_e", "disp_n", "gx", "gy", "gz", "heading", "heave", "horizontal_sum_in_window", "ID", "idx",
  "is_horizontal", "is_stable_horizontal", "mx", "my", "mz", "obs_datetime", "odba", "paddle_freq",
  "paddle_speed", "pitch", "pitch_rad", "pseudo_depth", "pseudo_error", "quality",
  "roll", "speed_dr", "surge", "sway", "temp", "time_diff", "turning_angle", "vedba",
  "vertical_speed", "vertical_velocity", "vv_smooth"
))

# runs when the package's namespace is loaded (before attach)
.onLoad <- function(libname, pkgname) {
  # nautilus errors are concise by design (thrown via .abort() with call = NULL). rlang otherwise
  # appends an interactive "Run `rlang::last_trace()`" reminder; silence it so errors stay one-liners.
  # Only set it when the user has expressed no preference of their own.
  if (is.null(getOption("rlang_backtrace_on_error"))) {
    options(rlang_backtrace_on_error = "none")
  }
  invisible()
}

# this will run automatically when package is attached
.onAttach <- function(libname, pkgname){
  W    <- 54L
  rule <- function(ch) strrep(ch, W)
  ansi <- interactive() && cli::num_ansi_colors() > 1
  bold <- function(x) if (ansi) paste0("\033[1m", x, "\033[0m") else x
  dim2 <- function(x) if (ansi) paste0("\033[2m", x, "\033[0m") else x
  ptr  <- function(label, target) dim2(paste0("  ", formatC(label, width = -17L), " ", target))

  packageStartupMessage(paste(c(
    dim2(rule("=")),
    bold(sprintf(" nautilus v%s", utils::packageVersion("nautilus"))),
    dim2(rule("-")),
    "  - process tag and video data",
    "  - analyse movement and behaviour",
    dim2(rule("-")),
    ptr("smooth sailing:", "help(package = \"nautilus\")"),
    ptr("set sail:",       "https://github.com/miguelgandra/nautilus"),
    ptr("cite the voyage:", "citation(\"nautilus\")"),
    dim2(rule("="))
  ), collapse = "\n"))
}

