#######################################################################################################
# Metadata column schema ##############################################################################
#######################################################################################################

#' Map deployment-metadata columns for importTagData()
#'
#' @description
#' Describes which columns of your `id.metadata` table hold each piece of deployment information.
#' It replaces the long list of individual `*.col` arguments with a single, self-documenting object:
#' fields default to the canonical nautilus names, so metadata that already uses those names needs no
#' configuration, and only non-standard columns have to be named explicitly.
#'
#' @param id Column holding the animal/deployment ID (default `"ID"`). Required.
#' @param tag_model Column holding the tag model (default `"tag"`). Required.
#' @param tag_type Column holding the tag type (e.g. "Camera", "MS"), or `NULL` to infer it from the
#'   ID (`"CAM"` in the ID -> "Camera", otherwise "MS"). Default `NULL`.
#' @param deploy_datetime Column holding the deployment (tagging) datetime, POSIXct (default
#'   `"tagging_date"`). Required.
#' @param deploy_lon,deploy_lat Columns holding the deployment longitude / latitude. Required
#'   (defaults `"deploy_lon"` / `"deploy_lat"`).
#' @param recovery_datetime Column holding the recovery (or detachment) datetime, POSIXct, or `NULL`.
#'   Mapping it enables the recovery-before-deployment and deployment-duration checks in
#'   \code{\link{qcDeploymentMetadata}}. Default `NULL`.
#' @param popup_datetime,popup_lon,popup_lat Columns holding the pop-up (detachment) datetime /
#'   longitude / latitude. Supply all three to enable pop-up location integration. Default `NULL`.
#' @param package_id Column holding the physical tag-package ID (the housing whose axis orientation is
#'   constant), or `NULL`. Enables paddle-wheel speed calibration, per-package axis-orientation
#'   consensus (\code{\link{consensusAxisMapping}}), and the package-overlap check in
#'   \code{\link{qcDeploymentMetadata}}. Default `NULL`.
#' @param logger_id Column holding the logger / data-recorder ID (the unit that owns the raw recording
#'   file, which may be reused across packages), or `NULL`. Enables logger-reuse / board-swap detection
#'   and continuous-recording-session detection in \code{\link{qcDeploymentMetadata}}. Default `NULL`.
#' @param exclude_sensors Column listing sensor channels known to be unusable on a deployment (a
#'   data-quality fact kept separate from axis orientation, e.g. a firmware bug), or `NULL`. Each value
#'   is a comma-separated list of families (`"accel"`, `"gyro"`, `"mag"`) and/or channels (e.g.
#'   `"mx"`, `"gz"`), blank for none. Reported by \code{\link{qcDeploymentMetadata}} and dropped by
#'   \code{\link{importTagData}} (so the channel is simply absent downstream). Default `NULL`.
#' @param axis_config Column naming the IMU orientation configuration of each deployment (e.g.
#'   `"CATS Camera"`), or `NULL`. The name is looked up in a `configs` dictionary by
#'   \code{\link{checkTagMapping}} (to validate the documented mapping against the data) and
#'   \code{\link{applyAxisMapping}} (to apply it); blank means "no documented config" (resolve from the
#'   data instead). Carried onto each tag's metadata at import. Default `NULL`.
#' @param paddle_wheel Column flagging whether the tag carried a paddle wheel (logical or 0/1), or
#'   `NULL`. Default `NULL`.
#' @param attachment_site Column recording where the tag was attached (e.g. "dorsal",
#'   "left_pectoral", "right_pectoral"); informs the magnetometer/sway-sign cross-checks. Default
#'   `NULL`.
#' @param deployment_type Column recording the mount type, `"rigid"` or `"towed"`; used by
#'   \code{\link{checkTagMapping}} to choose the posture scorer (rigid rewards near-zero resting
#'   roll/pitch; towed tolerates resting offsets). Default `NULL`.
#' @param traits Character *vector* of column names holding passive biological / ecological attributes of
#'   the animal (e.g. `c("sex", "length", "maturity", "species")`), or `NULL`. Unlike the roles above,
#'   traits drive no processing - they are carried through VERBATIM by \code{\link{importTagData}} into
#'   each object's metadata (`tagMetadata(x)$biometrics`), so they are consistently available for later
#'   grouping, filtering, and plotting (e.g. `group = "sex"`). Default `NULL`.
#'
#' @details
#' Each field maps one column of your metadata table to a *role* that nautilus understands. Mapping a
#' role does more than rename a column: it tells the package what the column *means*, and so unlocks the
#' checks and features that depend on it. Roles you do not map are simply skipped, and the checks that
#' need them are silently disabled (and reported as skipped in the QC summary).
#'
#' \tabular{lll}{
#'   \strong{Role} \tab \strong{Required?} \tab \strong{Enables}\cr
#'   `id` \tab required \tab the deployment key; duplicate-ID and ordering checks\cr
#'   `deploy_datetime` \tab required \tab deployment window; temporal-validity and future-date checks\cr
#'   `deploy_lon`/`deploy_lat` \tab required \tab declination correction; location-plausibility check\cr
#'   `tag_model` \tab required \tab posture scorer; axis-mapping keying\cr
#'   `tag_type` \tab optional (inferred) \tab Camera/MS-specific handling\cr
#'   `package_id` \tab optional \tab per-package axis consensus; package-overlap check; paddle calibration\cr
#'   `logger_id` \tab optional \tab logger-reuse / board-swap and recording-session detection\cr
#'   `recovery_datetime` \tab optional \tab recovery-before-deploy and deployment-duration checks\cr
#'   `exclude_sensors` \tab optional \tab drops known-unusable sensor channels (validity, e.g. a firmware bug)\cr
#'   `axis_config` \tab optional \tab names the IMU orientation config (looked up in a `configs` dictionary)\cr
#'   `popup_datetime`/`popup_lon`/`popup_lat` \tab optional \tab pop-up location integration\cr
#'   `paddle_wheel` \tab optional \tab paddle-wheel speed estimation\cr
#'   `attachment_site` \tab optional \tab magnetometer / sway-sign cross-checks\cr
#'   `deployment_type` \tab optional \tab rigid-vs-towed posture scorer
#' }
#'
#' `traits` is the exception to the table above: it is NOT a role. It lists passive attribute columns
#' (sex, length, maturity, species, ...) carried onto each object's metadata verbatim (they drive no
#' check), for later grouping / filtering / plotting - see the `traits` argument.
#'
#' @return A validated `nautilus_metadata_columns` object (a named list) for the `columns` argument of
#'   \code{\link{importTagData}} and \code{\link{qcDeploymentMetadata}}.
#' @seealso \code{\link{importTagData}}, \code{\link{qcDeploymentMetadata}}
#' @examples
#' # metadata already uses the canonical names:
#' metadataColumns()
#' # override only the columns that differ:
#' metadataColumns(deploy_datetime = "tagging_date", package_id = "PackageID")
#' @export

metadataColumns <- function(id = "ID",
                        tag_model = "tag",
                        tag_type = NULL,
                        deploy_datetime = "tagging_date",
                        deploy_lon = "deploy_lon",
                        deploy_lat = "deploy_lat",
                        recovery_datetime = NULL,
                        popup_datetime = NULL,
                        popup_lon = NULL,
                        popup_lat = NULL,
                        package_id = NULL,
                        logger_id = NULL,
                        exclude_sensors = NULL,
                        axis_config = NULL,
                        paddle_wheel = NULL,
                        attachment_site = NULL,
                        deployment_type = NULL,
                        traits = NULL) {

  # role fields (each maps ONE column, so each is a single string); `traits` is the one vector-valued
  # field (a set of passive attribute columns) and is validated separately below.
  fields <- list(id = id, tag_model = tag_model, tag_type = tag_type,
                 deploy_datetime = deploy_datetime, deploy_lon = deploy_lon, deploy_lat = deploy_lat,
                 recovery_datetime = recovery_datetime,
                 popup_datetime = popup_datetime, popup_lon = popup_lon, popup_lat = popup_lat,
                 package_id = package_id, logger_id = logger_id, exclude_sensors = exclude_sensors,
                 axis_config = axis_config, paddle_wheel = paddle_wheel, attachment_site = attachment_site,
                 deployment_type = deployment_type)

  required <- c("id", "tag_model", "deploy_datetime", "deploy_lon", "deploy_lat")
  for (f in names(fields)) .assert_string(fields[[f]], paste0("columns$", f), null_ok = !(f %in% required))

  if (!is.null(traits)) {
    if (!is.character(traits) || !length(traits) || anyNA(traits) || any(!nzchar(traits)))
      .abort("{.arg columns$traits} must be a non-empty character vector of column names (or {.code NULL}).")
    traits <- unique(traits)
  }

  structure(c(fields, list(traits = traits)), class = "nautilus_metadata_columns")
}


#' Coerce a `columns` argument to a validated schema (accepts a schema, a plain named list, or NULL).
#' @keywords internal
#' @noRd
.as_metadata_columns <- function(columns) {
  if (is.null(columns)) return(metadataColumns())
  if (inherits(columns, "nautilus_metadata_columns")) return(columns)
  if (is.list(columns) && !is.null(names(columns))) {
    unknown <- setdiff(names(columns), names(formals(metadataColumns)))
    if (length(unknown)) {
      .abort(c("{.arg columns} has unknown field{?s} {.val {unknown}}.",
                       "i" = "Valid fields: {.val {names(formals(metadataColumns))}}."))
    }
    return(do.call(metadataColumns, columns))
  }
  .abort("{.arg columns} must be created with {.fn metadataColumns} (or a named list of its fields).")
}
