#######################################################################################################
# Function to identify and extract deployment periods #################################################
#######################################################################################################

#' Filter deployment periods from a given dataset
#'
#' This function identifies deployment periods by analyzing changes in depth data using binary segmentation
#' to detect change points in both mean and variance. It then extracts the periods between the pre-deployment
#' and post-deployment phases based on the specified datetime range.
#'
#' The function uses a binary segmentation method (`cpt.meanvar`) to detect changes in depth and variance,
#' and it allows users to specify the maximum number of changepoints detected. The function also provides options
#' to visualize the data with custom plot options.
#'
#' @param data A data frame containing the dataset, including a column for 'depth' and 'datetime'.
#' @param first.datetime A datetime object indicating the start of the first deployment period.
#' @param last.datetime A datetime object indicating the end of the last deployment period.
#' @param psat.data An optional data frame containing PSAT data for additional plotting (default is NULL).
#' @param depth.threshold A numeric value for the threshold depth to distinguish deployment periods (default is 3.5).
#' @param variance.threshold A numeric value for the threshold variance to distinguish deployment periods (default is 6).
#' @param max.changepoints An integer specifying the maximum number of changepoints to detect (default is 6).
#' @param id.col A string representing the column name for the ID field (default is "ID").
#' @param datetime.col A string representing the column name for the datetime field (default is "datetime").
#' @param depth.col A string representing the column name for the depth field (default is "depth").
#' @param plot.vars A character vector specifying the variables to plot, such as acceleration ("ax") or pitch (default is c("ax", "pitch")).
#' @param plot A boolean value indicating whether to generate the plot (default is TRUE).
#'
#' @return A filtered data frame containing the deployment period between the detected attachment and popup times.
#' @export
#' @export


filterDeploymentData <- function(data,
                                 first.datetime,
                                 last.datetime,
                                 psat.data = NULL,
                                 depth.threshold = 3.5,
                                 variance.threshold = 6,
                                 max.changepoints = 6,
                                 id.col = "ID",
                                 datetime.col = "datetime",
                                 depth.col = "depth",
                                 plot.vars = c("ax", "pitch"),
                                 plot = TRUE) {

  ##############################################################################
  # initial checks #############################################################
  ##############################################################################



  id <- unique(data[[id.col]])


  ##############################################################################
  # filter out pre and post-deployment periods #################################
  ##############################################################################

  # run binary segmentation to detect change points in both mean and variance
  cp_depth <- suppressWarnings(changepoint::cpt.meanvar(data[[depth.col]], method="BinSeg", Q=max.changepoints, test.stat="Normal"))

  # extract changepoints
  changepoints <- changepoint::cpts(cp_depth)

  # add start and end indices
  changepoints <- c(1, changepoints, nrow(data))

  # calculate mean and variance for each segment
  segment_stats <- lapply(seq_along(changepoints[-1]), function(s) {
    start_idx <- changepoints[s]
    end_idx <- changepoints[s + 1] - 1
    segment <- data[[depth.col]][start_idx:end_idx]
    list(start = start_idx, end = end_idx, mean = mean(segment), variance = var(segment))
  })

  # convert to a data frame for easier manipulation
  segment_stats <- do.call(rbind, lapply(segment_stats, as.data.frame))

  # identify deployments
  deployment_segments <- which(segment_stats$mean >= depth.threshold | segment_stats$variance >= variance.threshold)
  spurious_segments <- which(segment_stats$mean < depth.threshold | segment_stats$variance < variance.threshold)

  # pre-deployment
  pre_deployment <- max(spurious_segments[spurious_segments < min(deployment_segments)])
  pre_segment_end <- segment_stats$end[pre_deployment]
  post_deployment <- min(spurious_segments[spurious_segments > max(deployment_segments)])
  post_segment_start <- segment_stats$start[post_deployment]

  # assign deploy_index and popup_index
  deploy_index <- pre_segment_end + 1
  popup_index <- post_segment_start - 1

  ##############################################################################
  # print to console ###########################################################
  ##############################################################################

  attachtime <- data[[datetime.col]][deploy_index]
  poptime <- data[[datetime.col]][popup_index]
  pre_deploy <- as.numeric(difftime(attachtime, first.datetime, units="hours"))
  pre_deploy <- sprintf("%dh:%02dm", floor(pre_deploy), round((pre_deploy - floor(pre_deploy)) * 60))
  post_deploy <- as.numeric(difftime(last.datetime, poptime, units="hours"))
  post_deploy <- sprintf("%dh:%02dm", floor(post_deploy), round((post_deploy - floor(post_deploy)) * 60))
  cat(sprintf("Attach time: %s (+%s)\n", strftime(attachtime, "%d/%b/%Y %H:%M:%S", tz="UTC"), pre_deploy))
  cat(sprintf("Popup time: %s (-%s)\n", strftime(poptime, "%d/%b/%Y %H:%M:%S", tz="UTC"), post_deploy))


  ##############################################################################
  # plot results ###############################################################
  ##############################################################################

  if(plot){

    # plot depth and temperature patterns and highlight discarded data
    #par(mfrow=c(3,1), mar=c(0.6, 3.4, 2, 1), mgp=c(2.2,0.6,0))
    layout(matrix(1:3, ncol=1), heights=c(2,1,1))
    par(mar=c(0.6, 3.4, 2, 1), mgp=c(2.2,0.6,0))
    plot(y=data[[depth.col]], x=data[[datetime.col]], type="n", ylim=c(max(data[[depth.col]], na.rm=T), -5),
         main=id, xlab="", ylab="Depth (m)", las=1, xaxt="n", cex.axis=0.8, cex.lab=0.9, cex.main=1)
    rect(par("usr")[1], par("usr")[3], par("usr")[2], par("usr")[4], col="grey97", border=NA)
    lines(y=data[[depth.col]], x=data[[datetime.col]], lwd=0.8)
    abline(v=data[[datetime.col]][c(deploy_index, popup_index)], col="red", lty=3)
    lines(x=data[[datetime.col]][1:deploy_index], y=data[[depth.col]][1:deploy_index], col="red1", lwd=0.8)
    lines(x=data[[datetime.col]][popup_index:nrow(data)], y=data[[depth.col]][popup_index:nrow(data)], col="red1", lwd=0.8)
    box()
    if(!is.na(psat.data)){
      # split PSAT positions for plotting
      fastloc_pos <- psat.data[psat.data$type=="FastGPS",]
      user_pos <- psat.data[psat.data$type=="User",]
      points(x=fastloc_pos[[datetime.col]], y=rep(par("usr")[4], nrow(fastloc_pos)), pch=16, col="blue", cex=0.8, xpd=T)
      points(x=user_pos[[datetime.col]], y=rep(par("usr")[4], nrow(user_pos)), pch=17, col="green2", cex=0.8, xpd=T)
      legend("bottomright", legend=c("Fastloc GPS", "User"), pch=c(16,17), col=c("blue","green2"),
             bty="n", horiz=T, cex=0.6, pt.cex=0.8, inset=c(-0.04, 0))
    }
    par(mar=c(0.6, 3.4, 0.6, 1))
    plot(y=data$ax, x=data[[datetime.col]], type="n", main="", xlab="",
         ylab="Surge (m/s2)", xaxt="n", las=1, cex.axis=0.8,  cex.lab=0.9)
    rect(par("usr")[1], par("usr")[3], par("usr")[2], par("usr")[4], col="grey97", border=NA)
    lines(y=data$ax, x=data[[datetime.col]], lwd=0.8)
    abline(v=data[[datetime.col]][c(deploy_index, popup_index)], col="red", lty=3)
    abline(h=quantile(data$ax, probs=0.99, na.rm=T), lty=2)
    lines(x=data[[datetime.col]][1:deploy_index], y=data$ax[1:deploy_index], col="red1", lwd=0.8)
    lines(x=data[[datetime.col]][popup_index:nrow(data)], y=data$ax[popup_index:nrow(data)], col="red1", lwd=0.8)
    box()
    if(!is.na(psat.files[i])){
      points(x=fastloc_pos[[datetime.col]], y=rep(par("usr")[4], nrow(fastloc_pos)), pch=16, col="blue", cex=0.8, xpd=T)
      points(x=user_pos[[datetime.col]], y=rep(par("usr")[4], nrow(user_pos)), pch=17, col="green2", cex=0.8, xpd=T)
    }
    par(mar=c(2, 3.4, 0.6, 1))
    plot(y=data$pitch, x=data[[datetime.col]], type="n", main="", xlab="",
         ylab="Pitch", las=1, cex.axis=0.8,  cex.lab=0.9)
    rect(par("usr")[1], par("usr")[3], par("usr")[2], par("usr")[4], col="grey97", border=NA)
    lines(y=data$pitch, x=data[[datetime.col]], lwd=0.8)
    abline(v=data[[datetime.col]][c(deploy_index, popup_index)], col="red", lty=3)
    abline(h=0, lty=2)
    lines(x=data[[datetime.col]][1:deploy_index], y=data$pitch[1:deploy_index], col="red1", lwd=0.8)
    lines(x=data[[datetime.col]][popup_index:nrow(data)], y=data$pitch[popup_index:nrow(data)], col="red1", lwd=0.8)
    box()
    if(!is.na(psat.files[i])){
      points(x=fastloc_pos[[datetime.col]], y=rep(par("usr")[4], nrow(fastloc_pos)), pch=16, col="blue", cex=0.8, xpd=T)
      points(x=user_pos[[datetime.col]], y=rep(par("usr")[4], nrow(user_pos)), pch=17, col="green2", cex=0.8, xpd=T)
    }
  }

  ##############################################################################
  # save updated data ##########################################################
  ##############################################################################

  # return the filtered data
  data <- data[deploy_index:popup_index, ]
  data <- data[, -which(colnames(data) %in% c("depth_diff", "surface"))]
  return(data)
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
