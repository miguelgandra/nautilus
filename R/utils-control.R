#' Internal method parameters for the integrity checks (NOT user-facing).
#'
#' These govern HOW a metric is computed, not how it is interpreted: the paddle spectral search band and
#' the gyro-bias absolute floor. They are implementation details of the detectors - deliberately kept out
#' of `integrityControl()` so the algorithms can be improved without an API change - whereas everything a
#' user should reasonably tune (the metric -> severity thresholds) is public there.
#'   \itemize{
#'     \item `gyro.bias.min` - absolute floor (rad/s) a median offset must also clear, so a negligible
#'       offset is not flagged merely because the animal barely rotated (a tiny MAD inflates the ratio).
#'     \item `paddle.min.freq`, `paddle.harmonic.guard` - the search floor is
#'       `max(paddle.min.freq, paddle.harmonic.guard * f_tailbeat)` Hz, keeping it clear of the tail-beat
#'       fundamental and its harmonics (the main source of false positives).
#'     \item `paddle.max.freq.frac` - ceiling as a fraction of Nyquist, avoiding aliasing artefacts.
#'     \item `mag.break.window` - window DURATION (s) the field magnitude is summarised over. A duration
#'       rather than a count, so the statistic means the same thing on a 5 h and a 50 h record.
#'     \item `mag.break.min.frac` - each side of a candidate break must be at least this fraction of the
#'       record. This is what "persistent" means operationally, and it sets the blind spot: a break in
#'       the first or last `mag.break.min.frac` of a record cannot be seen.
#'     \item `mag.break.min.windows` - fewer windows than this and the check abstains rather than guess.
#'     \item `mag.break.min.rel` - the step must also be at least this fraction of the field magnitude,
#'       so perfect rank separation across a negligible shift (a stable sensor drifting) is not flagged.
#'   }
#' @keywords internal
#' @noRd
.integrityMethod <- function() {
  list(gyro.bias.min = 0.02, paddle.min.freq = 3.5, paddle.harmonic.guard = 6, paddle.max.freq.frac = 0.85,
       mag.break.window = 600, mag.break.min.frac = 0.15, mag.break.min.windows = 30L,
       mag.break.min.rel = 0.05)
}


#' Coerce a control argument (object, named list, or NULL) to its validated control object.
#' @keywords internal
#' @noRd
.as_control <- function(x, constructor, cls, arg) {
  if (is.null(x)) return(constructor())
  if (inherits(x, cls)) return(x)
  if (is.list(x)) {
    unknown <- setdiff(names(x), names(formals(constructor)))
    if (length(unknown)) .abort(c("{.arg {arg}} has unknown field{?s} {.val {unknown}}.",
                                          "i" = "Valid fields: {.val {names(formals(constructor))}}."))
    return(do.call(constructor, x))
  }
  .abort("{.arg {arg}} must be created with {.fn {deparse(substitute(constructor))}} (or a named list of its fields).")
}
