# The immutable logging reference was captured from commit 6000ad52 before the
# parallel refactor. Parallel tests use actual PSOCK workers, not a mocked map.
.p_tag <- function(id = "A", n = 1200L, fs = 20) {
  t <- (seq_len(n) - 1) / fs
  d <- data.table::data.table(ID = id, datetime = as.POSIXct("2020-01-01", tz = "UTC") + t,
    depth = 10 + 2 * sin(t / 10), ax = 0.02 * sin(t), ay = 0.1 * sin(2 * pi * 0.8 * t),
    az = 1 + 0.01 * cos(t), temp = 20)
  data.table::setattr(d, "nautilus.version", "test")
  m <- nautilus:::.newNautilusMeta(); m$id <- id; m$axis_mapping$applied <- TRUE
  nautilus:::new_nautilus_tag(d, m)
}
.p_capture <- function(expr) {
  warnings <- list(); value <- NULL; text <- character()
  stdout <- capture.output(text <- cli::cli_fmt(withCallingHandlers(
    { value <- force(expr); invisible(NULL) }, warning = function(w) {
      warnings[[length(warnings) + 1L]] <<- list(message = conditionMessage(w),
                                               class = class(w), call = deparse(conditionCall(w)))
      invokeRestart("muffleWarning")
    })))
  list(text = sub("(?<=runtime: ).*$", "<elapsed>", text, perl = TRUE),
       stdout = stdout, warnings = warnings, value = value)
}
.p_normalize <- function(x) {
  if (is.list(x) && !is.data.frame(x)) return(lapply(x, .p_normalize))
  a <- attributes(x); a$.internal.selfref <- NULL
  if (!is.null(a$nautilus$processing)) a$nautilus$processing <- lapply(a$nautilus$processing,
    function(p) { p$time <- NULL; p })
  list(columns = lapply(seq_along(x), function(i) x[[i]]), attributes = a)
}
.p_workers <- function() {
  for (p in c("foreach", "doSNOW", "parallelly")) skip_if_not_installed(p)
  skip_if(parallelly::availableCores() < 2L, "requires two available CPU slots")
}
