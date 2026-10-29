#######################################################################################################
# Deployment-metadata quality control #################################################################
#######################################################################################################
#
# A metadata-only QC + normalisation step that runs BEFORE importTagData(). It maps arbitrary metadata
# columns to nautilus ROLES (via metadataColumns()), normalises them into a canonical table, and runs a
# set of role-gated checks: a check only runs when the roles it needs are mapped, so the same engine
# serves any lab's metadata without hard-coding column names. The check engine (.runMetadataQC) is
# shared with importTagData()'s inline guard, so the two never diverge.


#' Quality-control deployment metadata before importing
#'
#' @description
#' A deployment table is small, hand-assembled, and usually the least-checked part of a study - yet
#' almost every downstream result depends on it. A transposed latitude puts an animal on land, a
#' mistyped year places a deployment in the future, a duplicated identifier silently merges two
#' animals, and a recovery date before the deployment date makes a record impossible. These errors are
#' cheap to find now and expensive to find later, once they have propagated into tracks, dive
#' statistics and diel analyses.
#'
#' `checkDeploymentMetadata()` screens the table before any sensor data are read, and returns a cleaned
#' version carrying its own quality verdict. Run it once, at the start, and pass the result straight to
#' [importTagData()].
#'
#' @details
#' ## Roles, and why they matter
#' Your columns are matched to nautilus *roles* using [metadataColumns()]. A role is what a column
#' means, not merely what it is called, so mapping one both renames it and switches on the checks and
#' features that depend on it. Roles you do not map are skipped and their checks are silently disabled -
#' so if a check you expected is missing, confirm the role is mapped. The roles that were recognised are
#' listed in the console output.
#'
#' ## What is checked
#' Each check runs only when the roles it needs are available.
#'
#' \describe{
#'   \item{Identifiers}{Missing or duplicated deployment identifiers, and whitespace normalization of
#'     deployment, animal, package and logger identifiers. Case and leading zeros are preserved.}
#'   \item{Deployment records}{Missing deployment dates or positions; positions outside plausible
#'     geographic bounds; malformed numeric text, non-finite coordinates and incomplete position pairs;
#'     deployment dates further into the future than `future.tolerance` allows. Factor coordinates use
#'     their labels rather than their integer codes.}
#'   \item{Chronology}{Non-finite event times, or recovery or pop-up datetimes falling before deployment.
#'     Event columns must already be POSIXct; their absolute instants are preserved.}
#'   \item{Hardware reuse}{The same tag package deployed on overlapping dates, which would make its
#'     axis orientation ambiguous; and the same logger appearing on more than one deployment.}
#'   \item{Declared sensor faults}{Sensor channels listed as known-bad, checked for spelling against the
#'     recognised channel names so that a typo does not silently fail to exclude anything.}
#' }
#'
#' The package-overlap check additionally needs `recovery_datetime`, since it requires a deployment
#' window to intersect.
#'
#' ## Reporting, not stopping
#' Problems are reported and attached to the returned object rather than raised as errors, so you can
#' see the whole picture at once and decide what to fix. Identifier whitespace and numeric coordinate
#' representations are normalized; invalid numeric text is reported before coercion to missing values.
#' Structural problems such as a missing mapped column or a non-POSIXct event column stop the call. It is
#' [importTagData()] that refuses to proceed on unresolved errors, which keeps inspection and
#' enforcement as separate steps.
#'
#' At `verbose = 0` the console is silent and any errors are raised as a single aggregated R warning
#' instead, which is the channel a scripted caller can capture.
#'
#' @param metadata A table with one row per deployment.
#' @param columns How your columns map onto nautilus roles, built with [metadataColumns()]. Map the
#'   optional roles you have - recovery datetime, package and logger identifiers, and so on - to enable
#'   the checks that depend on them.
#' @param future.tolerance How many days beyond today a deployment datetime may fall before it is
#'   treated as implausible (default `1`). The allowance exists because tag clocks drift and time zones
#'   are easy to misread; raise it if your records legitimately run slightly ahead.
#' @param verbose How much detail to print: `0`/`"quiet"`, `1`/`"normal"`, or `2`/`"detailed"`
#'   (default).
#'
#' @return A `nautilus_deployments` object: the cleaned metadata table, carrying its quality verdict and
#'   the list of problems found. Inspect the problems with [issues()], and pass the object directly to
#'   [importTagData()], which reads its verdict and needs no separate `columns` argument.
#'
#' @seealso [metadataColumns()] to describe your columns; [issues()] to inspect what was found;
#'   [importTagData()] for the next step.
#'
#' @examples
#' \dontrun{
#' meta <- checkDeploymentMetadata(
#'   raw_metadata,
#'   columns = metadataColumns(id = "id", tag_model = "tag", deploy_datetime = "dateTime",
#'                             deploy_lon = "longitudeD", deploy_lat = "latitudeD",
#'                             package_id = "PackageID", logger_id = "ID_CMD",
#'                             recovery_datetime = "recoveryDate"))
#' issues(meta)                                    # inspect what was flagged
#' tags <- importTagData(folders, metadata = meta) # consumes the cleaned table directly
#' }
#' @export

checkDeploymentMetadata <- function(metadata,
                                 columns = metadataColumns(),
                                 future.tolerance = 1,
                                 verbose = "detailed") {

  start.time <- Sys.time()
  lvl <- .verbosity(verbose)
  .assert_number(future.tolerance, "future.tolerance", min = 0)

  columns <- .as_metadata_columns(columns)
  if (!is.data.frame(metadata)) .abort("{.arg metadata} must be a data.frame or data.table.")
  if (inherits(metadata, "data.table")) metadata <- as.data.frame(metadata)

  # normalise into a canonical table (renames mapped roles, trims identifiers, sorts chronologically);
  # also returns the realised schema (role -> canonical name) and any normalisation notes.
  norm <- .qcNormalizeMetadata(metadata, columns)
  d        <- norm$data
  roles    <- norm$roles                 # character vector of mapped, present roles
  schema   <- norm$schema                # nautilus_metadata_columns mapping each present role to itself

  # parse the `exclude_sensors` metadata column (if mapped) into known-unusable channels per deployment:
  # a data-quality fact kept separate from axis orientation. The column itself rides on `d`, so
  # importTagData reads it directly and drops those channels - nothing extra is attached here.
  parsed <- .parseExcludeSensors(d)

  # run the role-gated checks; fold the known-bad sensors in as info findings (+ any token errors)
  issues <- .runMetadataQC(d, roles, future.tolerance = future.tolerance)
  issues <- rbind(norm$notes, .validityIssues(parsed$excluded), parsed$issues, issues)
  rownames(issues) <- NULL

  n_err  <- sum(issues$severity == "error")
  n_warn <- sum(issues$severity == "warning")
  status <- if (n_err > 0L) "failed" else if (n_warn > 0L) "passed_with_warnings" else "passed"

  stamp <- list(passed = (n_err == 0L), status = status,
                n_errors = n_err, n_warnings = n_warn,
                timestamp = Sys.time(),
                version = tryCatch(as.character(utils::packageVersion("nautilus")),
                                   error = function(e) NA_character_))

  out <- .new_nautilus_deployments(d, schema = schema, stamp = stamp, issues = issues, roles = roles)

  # console summary (lvl >= 1); deferred aggregated warning at lvl == 0 when errors exist
  .qcReportConsole(out, lvl, start.time)
  if (lvl == 0L && n_err > 0L) {
    msgs <- issues$message[issues$severity == "error"]
    warning(sprintf("checkDeploymentMetadata: %d metadata error%s found:\n  %s",
                    n_err, if (n_err != 1L) "s" else "", paste(msgs, collapse = "\n  ")), call. = FALSE)
  }

  out
}
