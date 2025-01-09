#######################################################################################################
# Function to create model diagnostic plots ###########################################################
#######################################################################################################

#' Plot Model Diagnostics
#'
#' Generates a series of diagnostic plots for a machine learning model, including feature importance, ROC curve,
#' confusion matrix heatmap, and performance metrics.
#'
#' @param model A trained random forest model object. This model is expected to be trained on a dataset where the dependent variable is a factor.
#' @param test.data A data frame representing the testing dataset used to evaluate the model. Should include the dependent variable as the first column.
#'
#' @details
#' The function produces the following four plots:
#' \itemize{
#'   \item \strong{Feature Importance:} Displays a horizontal barplot of the feature importance values based on Mean Decrease Gini.
#'   \item \strong{ROC Curve:} Plots the Receiver Operating Characteristic (ROC) curve with the Area Under Curve (AUC) displayed in the title.
#'   \item \strong{Confusion Matrix Heatmap:} A heatmap representation of the confusion matrix, showing proportions for predicted vs. actual classifications.
#'   \item \strong{Model Performance Metrics:} A barplot summarizing precision, recall, and F1 score calculated from the confusion matrix.
#' }
#' The function assumes the response variable in `test.data` is a factor.
#' @export


plotModelDiagnostics <- function(model, test.data) {

  ################################################################################
  # Set up diagnostics ###########################################################

  # set layout for 4 diagnostic plots
  par(mfrow = c(2, 2))

  # extract the formula from the model
  formula <- model$call$formula

  # get the name of the response variable
  response.col <- as.character(formula[[2]])

  # ensure response column is of factor class
  test.data[[response.col]] <- factor(test.data[[response.col]], levels = c(0, 1))

  # generate predictions and confusion matrix
  predictions <- predict(model, newdata = test.data[, !(names(test.data) %in% response.col)])
  confusion_matrix <- table(Predicted = predictions, Actual = test.data[[response.col]])

  ################################################################################
  # Plot feature importance ######################################################

  # extract feature importance values from the random forest model
  importance_vals <- randomForest::importance(model)[, "MeanDecreaseGini"]
  importance_order <- order(importance_vals, decreasing = FALSE)

  # plot a horizontal barplot for feature importance
  par(mar = c(4, 6, 3, 1))
  barplot_vals <- barplot(
    importance_vals[importance_order], names.arg = FALSE, axes = FALSE,
    las = 1, horiz = TRUE, main = "", xlab = "", ylab = "", border = NA,
    xlim = c(0, max(importance_vals) * 1.1), xaxs = "i"
  )
  rect(par("usr")[1], par("usr")[3], par("usr")[2], par("usr")[4], col = "grey97", border = NA)
  barplot(
    importance_vals[importance_order],
    names.arg = rownames(importance_vals)[importance_order],
    xlim = c(0, max(importance_vals) * 1.1), xaxs = "i", col = "aquamarine4",
    horiz = TRUE, las = 1, cex.axis = 0.8, cex.names = 0.8, xlab = "", ylab = "", add = TRUE
  )
  title(main = "Feature Importance", cex.main = 1, line = 1)
  title(xlab = "Importance (Mean Decrease Gini)", line = 2, cex.lab = 0.9)
  title(ylab = "Feature", line = 4.5, cex.lab = 0.9)
  box()

  ################################################################################
  # Plot ROC Curve ###############################################################

  # compute predicted probabilities for the positive class
  test.data$predicted_prob <- predict(model, newdata = test.data[, !(names(test.data) %in% response.col)], type = "prob")[, 2]
  # calculate ROC curve
  roc_obj <- pROC::roc(test.data[[response.col]], test.data$predicted_prob,
                       levels = c(0,1), direction = "<")

  # plot the ROC curve
  plot(
    roc_obj, col = "blue", lwd = 2,
    main = paste("ROC Curve (AUC =", round(pROC::auc(roc_obj), 2), ")")
  )
  abline(a = 0, b = 1, col = "red", lty = 2)  # Add a diagonal reference line

  ################################################################################
  # Confusion Matrix Heatmap #####################################################

  # convert confusion matrix to proportions for visualization
  confusion_matrix_prop <- prop.table(confusion_matrix)

  # create color gradients
  correct_palette <- colorRampPalette(c("lightblue", "steelblue"))(100)
  incorrect_palette <- colorRampPalette(c("#f7dfe3", "red3"))(100)

  # create a color matrix for heatmap
  color_matrix <- ifelse(
    row(confusion_matrix_prop) == col(confusion_matrix_prop),
    # correct predictions
    correct_palette[floor(confusion_matrix_prop * 99) + 1],
    # incorrect predictions
    incorrect_palette[floor(confusion_matrix_prop * 99) + 1]
  )

  # generate the heatmap
  graphics::image(1:ncol(confusion_matrix_prop), 1:nrow(confusion_matrix_prop), t(confusion_matrix_prop),
        col=NA, axes = FALSE,
        main = "Confusion Matrix Heatmap", xlab = "Actual", ylab = "Predicted"
  )
  # add rectangles
  rect(xleft=0.5, xright=1.5, ybottom=1.5, ytop=2.5, col=color_matrix[1,2])
  rect(xleft=1.5, xright=2.5, ybottom=0.5, ytop=1.5, col=color_matrix[2,1])
  rect(xleft=0.5, xright=1.5, ybottom=0.5, ytop=1.5, col=color_matrix[1,1])
  rect(xleft=1.5, xright=2.5, ybottom=1.5, ytop=2.5, col=color_matrix[2,2])
  # add axis labels
  axis(1, at = 1:ncol(confusion_matrix_prop), labels = colnames(confusion_matrix))
  axis(2, at = 1:nrow(confusion_matrix_prop), labels = rownames(confusion_matrix))
  # add text labels
  text(
    rep(1:ncol(confusion_matrix_prop), each = nrow(confusion_matrix_prop)),
    rep(1:nrow(confusion_matrix_prop), ncol(confusion_matrix_prop)),
    labels = as.numeric(confusion_matrix), col = "black")
  # add box
  box()



  ################################################################################
  # Accuracy Metrics Barplot #####################################################

  # calculate precision, recall, and F1 score from confusion matrix
  true_positive <- confusion_matrix[2, 2]
  false_positive <- confusion_matrix[1, 2]
  false_negative <- confusion_matrix[2, 1]
  precision <- true_positive / (true_positive + false_positive)
  recall <- true_positive / (true_positive + false_negative)
  f1_score <- 2 * (precision * recall) / (precision + recall)
  metrics <- c(Precision = precision, Recall = recall, `F1 Score` = f1_score)

  # plot a barplot for the calculated metrics
  par(mar = c(4, 4, 3, 1), mgp = c(3, 0.8, 0))
  barplot_vals <- barplot(
    metrics, names.arg = FALSE, axes = FALSE, ylim = c(0, 1.1),
    las = 1, main = "", xlab = "", ylab = "", border = NA
  )
  rect(par("usr")[1], par("usr")[3], par("usr")[2], par("usr")[4], col = "grey97", border = NA)
  barplot(
    metrics, names.arg = FALSE, col = "steelblue", ylim = c(0, 1), las = 1,
    add = TRUE, main = "", xlab = "", ylab = "", cex.names = 0.8, cex.axis = 0.8
  )
  par(mgp = c(3, 0.5, 0))  # Adjust axis label positions
  axis(1, at = barplot_vals, labels = names(metrics), cex.axis = 0.8)  # X-axis labels
  title(main = "Model Performance Metrics", cex.main = 1, line = 1)
  title(xlab = "Metric", line = 2, cex.lab = 1)
  title(ylab = "Value", line = 2.5, cex.lab = 1)
  text(
    x = barplot_vals, y = metrics + 0.05, labels = sprintf("%.2f", metrics),
    cex = 0.7, col = "black"  # Add metric values as text labels
  )
  box()
}

#######################################################################################################
#######################################################################################################
#######################################################################################################
