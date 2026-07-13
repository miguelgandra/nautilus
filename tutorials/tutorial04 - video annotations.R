###############################################################################################
## Miguel Gandra || CCMAR || m3gandra@gmail.com || July 2025 ##################################
## Tutorial: Annotating and Processing Video-Sensor Data with the 'nautilus' R Package ########
###############################################################################################

# This script provides a step-by-step tutorial for annotating and processing multi-sensor
# biologging data from whale sharks using the 'nautilus' R package. It demonstrates how to:
#
# ---> Import and clean behavioral video annotations.
# ---> Automatically extract embedded timestamps from video frames using OCR.
# ---> Annotate sensor data using video-derived behavioral labels.
# ---> Extract summary features from annotated datasets for classification tasks.

################################################################################
# Install and load required packages ###########################################
################################################################################

# Install (if missing) and load CRAN packages
if(!require(readxl)){install.packages("readxl"); library(readxl)}
if(!require(data.table)){install.packages("data.table"); library(data.table)}
if(!require(zoo)){install.packages("zoo"); library(zoo)}
if(!require(circular)){install.packages("circular"); library(circular)}
if(!require(ranger)){install.packages("ranger"); library(ranger)}
if(!require(xgboost)){install.packages("xgboost"); library(xgboost)}
if(!require(caret)){install.packages("caret"); library(caret)}
if(!require(PRROC)){install.packages("PRROC"); library(PRROC)}
if(!require(corrplot)){install.packages("corrplot"); library(corrplot)}

# Load machine learning helper funcitons
files <- list.files(path = "./code/machine learning", pattern = "\\.R$", full.names = TRUE)
invisible(lapply(files, source))

# Load 'nautilus' package (install from GitHub if needed)
# devtools::install_github("miguelgandra/nautilus")
library(nautilus)


################################################################################
# Import and clean animal metadata #############################################
################################################################################

animal_metadata <- read_excel("./PINTADO_metadata_multisensor.xlsx")

# Select and rename relevant columns
selected_cols <- c("id", "dateTime", "site", "longitudeD", "latitudeD",
                   "sex", "size", "Nmax", "typeCMD", "PakageID", "satPtt",
                   "recoveryDate", "recoveryTime", "lonRecov", "latRecov",
                   "popupDatetime", "latPop", "lonPop")

animal_metadata <- as.data.frame(animal_metadata)[, selected_cols]

colnames(animal_metadata) <- c("ID", "deploy_date", "deploy_site", "deploy_lon", "deploy_lat",
                               "sex", "size", "n_animals", "tag", "packageID", "satPtt",
                               "recover_date", "recover_time", "recover_lon", "recover_lat",
                               "popup_date", "popup_lat", "popup_lon")

# Standardize tag labels
animal_metadata$tag[animal_metadata$tag == "4k"] <- "4K"
animal_metadata$tag[animal_metadata$tag == "Ceiia"] <- "CEIIA"


################################################################################
# Import and clean video annotation data #######################################
################################################################################

# Read Excel file containing manually annotated behavior events
annotations <- read_excel("~/Desktop/Whale Sharks/annotated_events_final4.xlsx")
annotations <- as.data.frame(annotations)

# Convert column names to lowercase
colnames(annotations)[-1] <- tolower(colnames(annotations)[-1])

# Convert start and end timestamps to POSIXct format with UTC timezone
annotations$start <- as.POSIXct(annotations$start, tz = "UTC")
annotations$end <- as.POSIXct(annotations$end, tz = "UTC")

# Standardize event category labels to lowercase
annotations$category <- tolower(annotations$category)

# Exclude specific ID if required (e.g., incomplete annotations)
annotations <- annotations[annotations$ID != "PIN_CAM_06", ]
annotations <- annotations[annotations$ID != "PIN_CAM_40", ]
annotations <- annotations[!is.na(annotations$ID),]

# Standardize feeding behaviour labels
annotations$category[annotations$category=="ram feeding"] <- "passive_feeding"
annotations$category[annotations$category=="vertical feeding"] <- "passive_feeding"
annotations$category[annotations$category=="strike feeding"] <- "lounge_feeding"

# Remove annotations with missing ID values
annotations <- annotations[!is.na(annotations$ID), ]

# Extract the unique animal IDs for which annotations are available
annotated_ids <- unique(annotations$ID)
annotated_ids <- annotated_ids[order(annotated_ids)]


# Subset relevant annotations
feeding_annots <- annotations[annotations$category %in% c("passive_feeding", "lounge_feeding"), ]
feeding_annots$duration <- as.numeric(difftime(feeding_annots$end, feeding_annots$start, units = "secs"))

# Create summary per category
categories <- unique(feeding_annots$category)
feeding_stats <- do.call(rbind, lapply(categories, function(cat) {
  df <- feeding_annots[feeding_annots$category == cat, ]
  total_duration <- sum(df$duration)
  mean_duration <- mean(df$duration)
  sd_duration <- sd(df$duration)
  min_duration <- min(df$duration)
  max_duration <- max(df$duration)
  mean_sd <- sprintf("%.2f ± %.2f", mean_duration, sd_duration)
  range_str <- sprintf("[%.2f - %.2f]", min_duration, max_duration)
  events <- nrow(df)
  n_individuals <- length(unique(df$ID))
  data.frame(category = cat,
             total_duration = total_duration,
             mean_duration_sd = mean_sd,
             duration_range = range_str,
             events = events,
             n_individuals = n_individuals,
             stringsAsFactors = FALSE)
}))
feeding_stats


################################################################################
# Extract summary features for classification ##################################
################################################################################

# Define the variables (sensor-derived features) to summarize
feature_vars <- c("depth", "vedba", "heading", "roll", "pitch",
                  "surge", "sway", "heave", "vertical_speed")

# Define metrics differently for circular vs linear variables
linear_metrics <- c("mean", "sd", "min", "max", "iqr", "rate", "entropy")
circular_metrics <- c("mean", "sd", "range", "iqr", "rate", "mrl")

# Create separate grids for circular and linear variables
linear_vars <- setdiff(feature_vars, c("heading", "roll"))
circular_vars <- c("heading", "roll")
linear_grid <- expand.grid(variable = linear_vars, metric = linear_metrics, stringsAsFactors = FALSE)
circular_grid <- expand.grid(variable = circular_vars, metric = circular_metrics, stringsAsFactors = FALSE)

# Set default sliding window size
default_window <- 10

# Combine the grids and add default 10-second window
basic_parameter_grid <- rbind(linear_grid, circular_grid)
basic_parameter_grid <- basic_parameter_grid[order(basic_parameter_grid$variable), ]
basic_parameter_grid$window_seconds <- default_window

# Exclude specific variable-metric combinations that are not meaningful or redundant
features_to_exclude <- c("depth_rate", "heave_entropy", "surge_entropy",
                         "sway_entropy", "vertical_speed_min", "vertical_speed_max",
                          "vertical_speed_entropy")
basic_parameter_grid$feature_name <- paste(basic_parameter_grid$variable,
                                           basic_parameter_grid$metric, sep = "_")
basic_parameter_grid <- basic_parameter_grid[!basic_parameter_grid$feature_name %in% features_to_exclude, ]
basic_parameter_grid$feature_name <- NULL
rownames(basic_parameter_grid) <- NULL

# Define enhanced ecological features
enhanced_features <- data.frame(variable = character(), metric = character(), window_seconds = numeric(), stringsAsFactors = FALSE)
# Heading change metrics (60s window)
enhanced_features[1, ] <- c("heading", "net_heading_change", "60")
enhanced_features[2, ] <- c("heading", "cumulative_heading_change", "60")
enhanced_features[3, ] <- c("heading", "circular_variance_heading", "60")
enhanced_features[4, ] <- c("heading", "uturn_flag", "60")
# Heading autocorrelation (60s window)
enhanced_features[5, ] <- c("heading", "heading_autocorr_avg", "60")
# Rolling autocorrelation of VEDBA (60s window)
enhanced_features[6, ] <- c("vedba", "rolling_autocorrelation", "60")
# Zero-crossing rate (10s window)
enhanced_features[7, ] <- c("pitch", "zero_crossing_rate", "15")
enhanced_features[8, ] <- c("roll", "zero_crossing_rate", "15")
# Oscillation regularity (15s window)
enhanced_features[9, ] <- c("pitch", "oscillation_regularity", "15")
enhanced_features[10, ] <- c("roll", "oscillation_regularity", "15")
# Movement smoothness and jerk (15s window)
enhanced_features[11, ] <- c("pitch", "movement_jerk", "15")
enhanced_features[12, ] <- c("roll", "movement_jerk", "15")
enhanced_features[13, ] <- c("pitch", "movement_smoothness", "15")
enhanced_features[14, ] <- c("roll", "movement_smoothness", "15")
# Posture stability (15s window)
enhanced_features[15, ] <- c("posture", "posture_stability", "15")
# Turning behavior variability (15s window)
enhanced_features[16, ] <- c("heading", "turning_rate_variability", "15")
# Activity indices (15s window)
enhanced_features[17, ] <- c("activity", "activity_index", "15")
# Movement predictability and consistency (15s window)
enhanced_features[18, ] <- c("pitch", "movement_predictability", "15")
enhanced_features[19, ] <- c("roll", "movement_predictability", "15")
enhanced_features[20, ] <- c("pitch", "movement_consistency", "15")
enhanced_features[21, ] <- c("roll", "movement_consistency", "15")
# Depth and vertical movement patterns (30s window)
enhanced_features[22, ] <- c("depth", "depth_change_rate", "30")
enhanced_features[23, ] <- c("depth", "depth_change_consistency", "30")
# Circling behavior detection (60s window)
enhanced_features[24, ] <- c("heading", "circling_behavior", "60")

# Convert window_seconds to numeric
enhanced_features$window_seconds <- as.numeric(enhanced_features$window_seconds)
# Combine basic and enhanced parameter grids
complete_parameter_grid <- rbind(basic_parameter_grid, enhanced_features)

# Add output suffix for enhanced features
enhanced_rows <- (nrow(basic_parameter_grid) + 1):nrow(complete_parameter_grid)
complete_parameter_grid$output_suffix <- ""
complete_parameter_grid$output_suffix[enhanced_rows] <- paste0("_", complete_parameter_grid$window_seconds[enhanced_rows])

# Create final feature names for reference
complete_parameter_grid$feature_name <- paste0(
  complete_parameter_grid$variable, "_",
  complete_parameter_grid$metric,
  ifelse(complete_parameter_grid$window_seconds != default_window,
         paste0("_", complete_parameter_grid$window_seconds), "")
)

# Select the previously processed files
processed_files <- list.files("./data processed/processed/20Hz", full.names = TRUE)
processed_files <- processed_files[gsub("-20Hz.rds", "", basename(processed_files)) %in% annotated_ids]

# Extract feature summaries from processed multi-sensor data
features_list <- extractFeatures(data = processed_files,
                                 parameter.grid = complete_parameter_grid,
                                 enhanced.features = TRUE,
                                 id.col = "ID",
                                 datetime.col = "datetime",
                                 window.size = 10,
                                 aggregate = FALSE,
                                 downsample.to = 1,
                                 circular.variables = c("heading", "roll"),
                                 output.dir = "./data processed/features/enhanced",
                                 output.suffix = "-features",
                                 return.data = FALSE,
                                 n.cores = 6)


################################################################################
# Annotate sensor data with video events #######################################
################################################################################

# Load previously processed features data
features_files <- list.files("./data processed/features/enhanced", full.names = TRUE)
features_list <- vector("list", length(features_files))
for (i in seq_along(features_files)) {
  features_list[[i]] <- readRDS(features_files[i])
  names(features_list)[i] <- attributes(features_list[[i]])$id
}

# Split features data into annotated and non-annotated IDs
annotated_list <- features_list[names(features_list) %in% annotated_ids]

# Match annotated feeding events to the features datasets
annotated_list <- annotateData(data = annotated_list,
                               annotations = annotations,
                               event.col = "category",
                               selected.events = c("lounge_feeding", "passive_feeding"))


################################################################################
# Extract and validate embedded video timestamps ###############################
################################################################################

# List all subdirectories in the main video folder
video_folders <- list.dirs("~/Desktop/Whale Sharks/CAMS", recursive = FALSE)
# Filter to include only folders corresponding to annotated video IDs
video_folders <- video_folders[basename(video_folders) %in% annotated_ids]

# Extract metadata (start/end/duration/frame rate) from each video. Start times are taken from the
# video file names where possible (exact and camera-agnostic); cross.check = TRUE additionally reads the
# on-screen timestamp by OCR and flags any file-name/overlay disagreements (see the ocr_flag column).
video_metadata <- getVideoMetadata(video.folders = video_folders,
                                   video.format = "mp4",
                                   timestamp.source = "auto",
                                   cross.check = TRUE,
                                   use.parallel = TRUE,
                                   n.cores = 7)

# Optionally save frames for videos whose timestamp is uncertain (OCR-sourced, missing, or flagged by
# the cross-check) to a directory for manual inspection
video_metadata <- saveUncertainTimestampFrames(video.metadata = video_metadata,
                                               output.dir = "~/Desktop/review frames")

# Manually correct a missing or failed timestamp for a specific video
missing_idx1 <- which(video_metadata$video=="CameraCMD71Spot17-20201006-172957-009-00005.mp4")
video_metadata$start[missing_idx1] <- as.POSIXct("2020-09-06 17:29:57", tz="UTC")
video_metadata$end[missing_idx1] <- video_metadata$start[missing_idx1] + video_metadata$duration[missing_idx1]

missing_idx2 <- which(video_metadata$video=="230831-161949_CAM0bc99448_30.mp4")
video_metadata$start[missing_idx2] <- as.POSIXct("2023-08-31 16:19:49", tz="UTC")
video_metadata$end[missing_idx2] <- video_metadata$start[missing_idx2] + video_metadata$duration[missing_idx2]

missing_idx3 <- which(video_metadata$video=="230831-171758_CAM0bc99448_30.mp4")
video_metadata$start[missing_idx3] <- as.POSIXct("2023-08-31 17:17:59", tz="UTC")
video_metadata$end[missing_idx3] <- video_metadata$start[missing_idx3] + video_metadata$duration[missing_idx3]


# Save final video metadata to a CSV file for future reference
write.csv(video_metadata, file = "./video_metadata2.csv", row.names = FALSE)



################################################################################
## Filter annotated data to match the reviewed video deployment periods ########
################################################################################

# Load video deployment metadata
video_metadata <- read.csv("./video_metadata.csv", stringsAsFactors = FALSE)
video_metadata$start <- as.POSIXct(video_metadata$start, tz = "UTC")
video_metadata$end <- as.POSIXct(video_metadata$end, tz = "UTC")

# Build annotation intervals to limit analysis to periods that were actually reviewed.
# Intervals are based on "annotation resume" and "annotation end" tags and exclude
# unannotated segments (e.g. nighttime or poor visibility segments).

# Convert annotations to data.table
annotations_dt <- data.table::as.data.table(annotations)
annotations_dt[, start := as.POSIXct(start, tz = "UTC")]

# Extract annotation resume and end events
annot_events <- annotations_dt[category %in% c("annotation end", "annotation resume"), .(ID, category, time = start)]
data.table::setorder(annot_events, ID, time)

# Initialise result list
annotation_intervals_list <- list()
# Process each individual
for (curr_id in unique(annotations_dt$ID)) {
  # Get annotation events for this ID
  events_id <- annot_events[ID == curr_id]
  # Initialise start and end vectors
  interval_starts <- as.POSIXct(character(0), tz = "UTC")
  interval_ends <- as.POSIXct(character(0), tz = "UTC")
  current_start <- NA
  for (i in seq_len(nrow(events_id))) {
    event <- events_id[i]
    if (event$category == "annotation end") {
      # close interval
      interval_starts <- c(interval_starts, current_start)
      interval_ends <- c(interval_ends, event$time)
      current_start <- NA  # reset
    } else if (event$category == "annotation resume") {
      current_start <- event$time
    }
  }
  # If the last event was a "resume" with no "end", close it as open interval
  if (!is.na(current_start)) {
    interval_starts <- c(interval_starts, current_start)
    interval_ends <- c(interval_ends, as.POSIXct(NA, tz = "UTC"))
  }
  # Build interval table for this ID
  dt_id <- data.table::data.table(ID = curr_id, start = interval_starts, end = interval_ends)
  annotation_intervals_list[[curr_id]] <- dt_id
}
# Combine all
annotation_intervals <- data.table::rbindlist(annotation_intervals_list)


# Filter sensor data to match the final reviewed periods
# We'll pass both video_metadata and annotation_intervals to the function.
data_model <- filterVideoPeriod(data = annotated_list,
                                video.metadata = video_metadata,
                                annotation.intervals = annotation_intervals)



################################################################################
## Data Preparation and Feature Selection ######################################
################################################################################

# Combine all individual data frames into a single data frame
data_model <- data.table::rbindlist(data_model, fill=TRUE)
data_model <- as.data.frame(data_model)
data_model$lounge_feeding[is.na(data_model$lounge_feeding)] <- 0
data_model$passive_feeding[is.na(data_model$passive_feeding)] <- 0

# Exclude rows with NAs
original_rows <- nrow(data_model)
data_model <- data_model[complete.cases(data_model), ]
excluded_rows <- original_rows - nrow(data_model)
excluded_pct <- 100 * excluded_rows / original_rows
cat("Rows excluded:", excluded_rows, "(", round(excluded_pct, 1), "% )\n")

# Create aggregated feeding label
data_model$feeding <- 0
data_model$feeding[data_model$lounge_feeding==1] <- 1
data_model$feeding[data_model$passive_feeding==1] <- 1


################################################################################
################################################################################
## FEEDING CLASSIFICATION WORKFLOW #############################################
################################################################################
################################################################################

# Step 1. Set grid for lounge feeding type
hyperparameter_grid  <- expand.grid(
  # Feature engineering (domain specific)
  n_features = c(30, 35, 40),
  smooth_run_length = c(3, 5),
  event_window = c(15, 30),
  # Evaluation strategy
  splitting_method = c("bout", "individual"),
  # XGBoost core parameters (expanded learning rate)
  eta = c(0.01, 0.05, 0.1),
  min_child_weight = c(1, 3, 5),
  max_depth = c(6, 8),
  # Regularization
  alpha = c(0.1, 1),  # L1 regularization - crucial for feature selection
  lambda = c(5, 10),  # L2 regularization - crucial for overfitting
  # Sampling
  subsample = c(0.7, 0.9, 1.0),
  colsample_bytree = c(0.7, 1.0),
  # Regularization tree splits (reduced)
  gamma = c(0, 1),
  # Imbalanced data handling (keep as is - your domain expertise)
  balancing_method = c("undersample", "undersample_smote", "undersample_lounge_smote"),
  # General SMOTE parameters
  dup_size = c(1, 2, 5),
  # Lounge-specific SMOTE parameters
  lounge_dup_size = c(3, 5, 10),
  negative_ratio = c(5, 10),
  scale_pos_weight_type = c("neg_pos_ratio", "neg_pos_ratio_x3", "neg_pos_ratio_x6"),
  # Boosting rounds
  nrounds = c(500, 1000),
  stringsAsFactors = FALSE
)
# Filter grid to remove invalid combinations
# Remove lounge_dup_size for non-lounge methods
hyperparameter_grid <- hyperparameter_grid[
  !(hyperparameter_grid$balancing_method %in% c("undersample") &
      hyperparameter_grid$dup_size != 2 &
      hyperparameter_grid$lounge_dup_size != 3),]
# Remove general dup_size for lounge-only methods
hyperparameter_grid <- hyperparameter_grid[
  !(hyperparameter_grid$balancing_method %in% c("undersample_lounge_smote") &
      hyperparameter_grid$dup_size != 1),]

# Check grid size
cat("Grid size:", nrow(hyperparameter_grid), "combinations\n")


################################################################################
################################################################################
################################################################################

# Select feature columns excluding identifiers and target variables
feature_cols <- setdiff(colnames(data_model), c("ID", "datetime", "feeding", "lounge_feeding", "passive_feeding", "split_individual"))

# Step 2: Summarise the behaviour class distribution
# Useful for understanding class balance and prevalence.
createPositiveClassSummary(data_model, "feeding")

# Step 3: Hyperparameter optimisation for the classification model
optimization_results <- runModelOptimization(data = data_model,
                                             hyperparameter.grid = hyperparameter_grid,
                                             response.col =  "feeding",
                                             feature.cols = feature_cols,
                                             test.ratio = 0.3,
                                             dominance.threshold = 0.3,
                                             optimize.for = "sample",
                                             n.configs = 500,
                                             n.seeds = 5,
                                             return.models = FALSE,
                                             return.predictions = FALSE,
                                             parallel = TRUE,
                                             n.cores = 7)


# Step 4: Save the optimisation results to disk
saveRDS(optimization_results, "./hyperparameter-optim-feeding-500.rds")
# Reload result (in case of script resume)
optimization_results <- readRDS("./hyperparameter-optim-feeding-500.rds")

# Step 5: Analyse optimisation results
# Extract insights from hyperparameter tuning
optim_analysis <- analyzeOptimizationResults(optimization_results,
                                             target.metric = "sample_f1",
                                             top.n = 20,
                                             create.plots = TRUE)

# Display diagnostic plots from optimisation analysis if available
for(i in 1:length(optim_analysis$plots)){
  print(optim_analysis$plots[[i]])
  Sys.sleep(3)
}

# Step 6: Train the final model using the best hyperparameter configuration
# The model is trained on the full dataset (or training subset) using the optimal parameters.
final_model<- trainFinalModel(data_model,
                              optimization.results = optimization_results,
                              response.col = "feeding",
                              feature.cols = feature_cols,
                              optimize.for = "agg_sample")


cv_model <- list()
for(i in 1:length(optimization_results$random_seeds_used)){
  current_seed <- optimization_results$random_seeds_used[i]
  cv_model[[i]] <- trainFinalModel(data_model,
                                   optimization.results = optimization_results,
                                   response.col = "feeding",
                                   feature.cols = feature_cols,
                                   random.seed = current_seed,
                                   optimize.for = "sample")
}


f1s <- unlist(lapply(cv_model, function(x) x$sample_level$f1))
sprintf("%.3f ± %.3f", mean(f1s), sd(f1s))

importance_matrix <- xgboost::xgb.importance(model = cv_model[[4]]$model)
head(importance_matrix, 10)
xgboost::xgb.plot.importance(importance_matrix, top_n = 10)



pdp_plot <- partial(final_model$model, pred.var = "depth_mean", train = final_model$, type = "classification")
plot(pdp_plot)

# Calculate PR curve and AUC-PR

library(PRROC)
model_predictions <- final_model$predictions
pr <- pr.curve(scores.class0 = scores[labels == 1],
               scores.class1 = scores[labels == 0],
               curve = TRUE)









################################################################################
## Train XGBoost ###############################################################
################################################################################

# Define non-feature columns to exclude
non_feature_cols <- c("ID", "datetime", "split", "feeding", "lounge_feeding", "passive_feeding",  "feeding_type")
# Get potential predictor columns
feature_cols <- setdiff(names(lounge_feeding), non_feature_cols)

# Prepare data for modeling
X_train <- as.matrix(balanced_train[,feature_cols])
y_train <- balanced_train$feeding

X_val <- as.matrix(val_data[,feature_cols])
y_val <- val_data$feeding

X_test <- as.matrix(test_data[,feature_cols])
y_test <- test_data$feeding


# Create DMatrix objects
dtrain <- xgb.DMatrix(data = X_train, label = y_train)
dval <- xgb.DMatrix(data = X_val, label = y_val)

# Set parameters optimized for imbalanced classification
params <- list(
  objective = "binary:logistic",
  eval_metric = c("auc", "logloss"),
  max_depth = 6,
  eta = 0.1,
  subsample = 0.8,
  colsample_bytree = 0.8,
  min_child_weight = 5,
  scale_pos_weight = sum(y_train == 0) / sum(y_train == 1)  # Handle remaining imbalance
)

# Train model with early stopping
model <- xgb.train(
  params = params,
  data = dtrain,
  nrounds = 1000,
  watchlist = list(train = dtrain, validation = dval),
  early_stopping_rounds = 50,
  verbose = 1
)


# Model evaluation function
evaluate_model <- function(model, X, y, data_name) {

  dtest <- xgb.DMatrix(data = X, label = y)
  predictions <- predict(model, dtest)
  pred_binary <- ifelse(predictions > 0.5, 1, 0)

  # Calculate metrics
  conf_matrix <- table(Predicted = pred_binary, Actual = y)
  accuracy <- sum(diag(conf_matrix)) / sum(conf_matrix)

  if (length(unique(y)) > 1) {
    roc_obj <- pROC::roc(y, predictions)
    auc <- as.numeric(pROC::auc(roc_obj))
  } else {
    auc <- NA
  }

  # Calculate precision, recall, F1 for feeding class
  if (1 %in% y && 1 %in% pred_binary) {
    precision <- conf_matrix[2,2] / sum(conf_matrix[2,])
    recall <- conf_matrix[2,2] / sum(conf_matrix[,2])
    f1 <- 2 * (precision * recall) / (precision + recall)
  } else {
    precision <- recall <- f1 <- NA
  }

  cat(sprintf("\n=== %s RESULTS ===\n", toupper(data_name)))
  cat(sprintf("Accuracy: %.3f\n", accuracy))
  cat(sprintf("AUC: %.3f\n", auc))
  cat(sprintf("Precision: %.3f\n", precision))
  cat(sprintf("Recall: %.3f\n", recall))
  cat(sprintf("F1-Score: %.3f\n", f1))
  cat("Confusion Matrix:\n")
  print(conf_matrix)

  return(list(
    predictions = predictions,
    accuracy = accuracy,
    auc = auc,
    precision = precision,
    recall = recall,
    f1 = f1,
    confusion_matrix = conf_matrix
  ))
}

# Evaluate on all sets
val_results <- evaluate_model(model, X_val, y_val, "validation")
test_results <- evaluate_model(model, X_test, y_test, "test")

# Feature importance
cat("\n=== FEATURE IMPORTANCE ===\n")
importance <- xgb.importance(model = xgb_model)
print(head(importance, 15))

################################################################################
## Apply SMOTE to balance lounge vs. passive feeding ###########################
################################################################################

# Create a 3-level feeding type variable: lounge, passive, non-feeding
data_model$feeding_type <- with(data_model,
                                ifelse(lounge_feeding == 1, "lounge",
                                       ifelse(passive_feeding == 1, "passive", "non-feeding")))
data_model$feeding_type <- as.factor(data_model$feeding_type)

# Filter only feeding events (exclude non-feeding observations)
feeding_events <- subset(data_model, feeding_type != "non-feeding")

# Save metadata (e.g. ID, datetime) for later use
feeding_meta <- feeding_events[, c("ID", "datetime")]

# Step 3: Prepare feature matrix (only numeric columns, excluding labels)
X <- feeding_events[, sapply(feeding_events, is.numeric) &
                      !names(feeding_events) %in% c("lounge_feeding", "passive_feeding", "feeding")]

# Step 4: Extract feeding_type as class label (lounge vs passive)
y <- droplevels(feeding_events$feeding_type)

# Step 5: Apply SMOTE to balance feeding types
set.seed(123)
smote_output <- smotefamily::SMOTE(X, y, K = 5, dup_size = 0)

# Step 6: Format balanced feeding data (real + synthetic)
balanced_feeding <- smote_output$data
balanced_feeding$feeding_type <- as.factor(balanced_feeding$class)
balanced_feeding$class <- NULL
balanced_feeding$feeding <- 1  # binary target

# Identify how many were original vs synthetic
n_original <- nrow(feeding_events)
n_total <- nrow(balanced_feeding)
n_synth <- n_total - n_original

# Add back ID and datetime for original rows, leave synthetic as NA
balanced_feeding$ID <- c(feeding_meta$ID, rep(NA, n_synth))
balanced_feeding$datetime <- c(feeding_meta$datetime, rep(NA, n_synth))

# Step 7: Extract non-feeding observations from full dataset
nonfeeding <- subset(data_model, feeding_type == "non-feeding")
nonfeeding$feeding <- 0  # binary target

# Step 8: Match columns before merging
common_cols <- intersect(names(balanced_feeding), names(nonfeeding))
train_data <- rbind(balanced_feeding[, common_cols], nonfeeding[, common_cols])

# Reorder train_data columns
front_cols <- c("ID", "datetime")
train_data <- train_data[, c(front_cols, setdiff(colnames(train_data), front_cols))]


################################################################################
## Undersample non-feeding #####################################################
################################################################################

# Separate feeding and non-feeding
feeding_data <- subset(train_data, feeding == 1)
nonfeeding_data <- subset(train_data, feeding == 0)

# Decide your undersampling fraction or target size:
# For example, match the number of feeding rows:
n_feed <- nrow(feeding_data)
n_nonfeed <- nrow(nonfeeding_data)

# Undersample non-feeding to size = n_feed (or a fraction of that)
undersample_size <- n_feed

# Randomly sample from non-feeding
nonfeeding_sampled <- nonfeeding_data[sample(n_nonfeed, undersample_size), ]

# Combine back
train_data_undersampled <- rbind(feeding_data, nonfeeding_sampled)

# Optional: shuffle rows (if model is not time-aware)
set.seed(42)
train_data_undersampled <- train_data_undersampled[sample(nrow(train_data_undersampled)), ]


################################################################################
################################################################################
################################################################################

# Convert target to factor
train_data$feeding <- as.factor(train_data[[response.col]])
test_data[[response.col]] <- as.factor(test_data[[response.col]])

# Prepare initial matrices for feature selection
train_matrix_full <- as.matrix(sapply(train_data[, feature.cols], as.numeric))
train_label <- as.numeric(as.character(train_data[[response.col]]))
dtrain_full <- xgb.DMatrix(data = train_matrix_full, label = train_label)

# Calculate scale_pos_weight based on type
neg <- sum(train_label == 0)
pos <- sum(train_label == 1)

# Define XGBoost parameters
params_initial <- list(
  objective = "binary:logistic",
  eval_metric = "auc",
  scale_pos_weight = scale_pos_weight,
  max_depth = config$max_depth,
  eta = config$eta,
  subsample = config$subsample,
  colsample_bytree = config$colsample_bytree,
  min_child_weight = config$min_child_weight,
  gamma = config$gamma,
  alpha = config$alpha,        # L1 regularization
  lambda = config$lambda,      # L2 regularization
  nthread = 1
)

# Train initial model for feature importance
set.seed(random.seed)
xgb_initial <- xgb.train(
  params = params_initial,
  data = dtrain_full,
  nrounds = nrounds_initial,
  verbose = 0
)



################################################################################
################################################################################
## FEEDING CLASSIFICATION WORKFLOW #############################################
################################################################################
################################################################################

# Step 1. Set grid for lounge feeding type
hyperparameter_grid  <- expand.grid(
  # Feature engineering (domain specific)
  n_features = c(30, 35, 40),
  smooth_run_length = c(3, 5),
  event_window = c(15, 30),
  # Evaluation strategy
  splitting_method = c("bout", "individual"),
  # XGBoost core parameters (expanded learning rate)
  eta = c(0.01, 0.05, 0.1),
  min_child_weight = c(1, 3, 5),
  max_depth = c(6, 8),
  # Regularization
  alpha = c(0.1, 1),  # L1 regularization - crucial for feature selection
  lambda = c(5, 10),  # L2 regularization - crucial for overfitting
  # Sampling
  subsample = c(0.7, 0.9, 1.0),
  colsample_bytree = c(0.7, 1.0),
  # Regularization tree splits (reduced)
  gamma = c(0, 1),
  # Imbalanced data handling (keep as is - your domain expertise)
  balancing_method = c("undersample", "undersample_smote"),
  dup_size = c(1, 2),
  negative_ratio = c(5, 10),
  scale_pos_weight_type = c("neg_pos_ratio", "neg_pos_ratio_x3", "neg_pos_ratio_x6", "neg_pos_ratio_x10"),
  # Boosting rounds
  nrounds = c(500, 1000),
  stringsAsFactors = FALSE
)
# Check grid size
cat("Grid size:", nrow(hyperparameter_grid), "combinations\n")


# Step 2: Summarise the behaviour class distribution
# Useful for understanding class balance and prevalence.
createPositiveClassSummary(data_model, "feeding")

# Step 3: Hyperparameter optimisation for the classification model
optimization_results <- runModelOptimization(data = data_model,
                                             hyperparameter.grid = hyperparameter_grid,
                                             response.col =  "feeding",
                                             feature.cols = feature_cols,
                                             test.ratio = 0.3,
                                             dominance.threshold = 0.3,
                                             optimize.for = "event",
                                             n.configs = 200,
                                             n.seeds = 5,
                                             return.models = FALSE,
                                             return.predictions = FALSE,
                                             parallel = TRUE,
                                             n.cores = 7)


# Step 4: Save the optimisation results to disk
saveRDS(optimization_results, "./hyperparameter-optim-feeding.rds")
# Reload result (in case of script resume)
optimization_results <- readRDS("./hyperparameter-optim-feeding.rds")

# Step 5: Analyse optimisation results
# Extract insights from hyperparameter tuning
optim_analysis <- analyzeOptimizationResults(optimization_results,
                                             target.metric = "sample_f1",
                                             top.n = 20,
                                             create.plots = TRUE)

# Display diagnostic plots from optimisation analysis if available
for(i in 1:length(optim_analysis$plots)){
  print(optim_analysis$plots[[12]])
  Sys.sleep(3)
}

# Step 6: Train the final model using the best hyperparameter configuration
# The model is trained on the full dataset (or training subset) using the optimal parameters.
final_model<- trainFinalModel(data_model,
                              optimization.results = optimization_results,
                              response.col = "feeding",
                              feature.cols = feature_cols,
                              optimize.for = "agg_sample")


cv_model <- list()
for(i in 1:length(optimization_results$random_seeds_used)){
  current_seed <- optimization_results$random_seeds_used[i]
  cv_model[[i]] <- trainFinalModel(data_model,
                                   optimization.results = optimization_results,
                                   response.col = "feeding",
                                   feature.cols = feature_cols,
                                   random.seed = current_seed,
                                   optimize.for = "agg_sample")
}


f1s <- unlist(lapply(cv_model, function(x) x$sample_level$f1))
sprintf("%.3f ± %.3f", mean(f1s), sd(f1s))

importance_matrix <- xgboost::xgb.importance(model = cv_model[[4]]$model)
head(importance_matrix, 10)
xgboost::xgb.plot.importance(importance_matrix, top_n = 10)



pdp_plot <- partial(final_model$model, pred.var = "depth_mean", train = final_model$, type = "classification")
plot(pdp_plot)

# Calculate PR curve and AUC-PR

library(PRROC)
model_predictions <- final_model$predictions
pr <- pr.curve(scores.class0 = scores[labels == 1],
               scores.class1 = scores[labels == 0],
               curve = TRUE)






################################################################################
## Check true events vs predicted events #######################################
################################################################################

df <- final_model$predictions

# Helper function to get event bouts
.getBouts <- function(labels, timestamps) {
  # coerce factor to numeric (0/1)
  labels <- as.integer(as.character(labels))
  rle_labels <- rle(labels)
  ends <- cumsum(rle_labels$lengths)
  starts <- c(1, head(ends, -1) + 1)
  bouts <- data.frame(start = timestamps[starts][rle_labels$values == 1],
                      end = timestamps[ends][rle_labels$values == 1])
  return(bouts)
}
true_bouts <- .getBouts(df$true_label, df$datetime)
pred_bouts <- .getBouts(df$predicted_label, df$datetime)


################################################################################
## Predict feeding events on remaining data ####################################
################################################################################

# Load model
mod <- readRDS("./xgboost_model.rds")
final_model <- mod$model
top_features <- mod$selected_features

# Threshold and smoothing settings from your final model
best_thresh <- mod$optimal_threshold
smooth_run_length <- 5

# Remove data.tables with no features
features_list <- Filter(function(dt) nrow(dt) > 0, features_list)

# Loop over each individual
for (i in seq_along(features_list)) {

  # Extract individual data.table
  dt <- features_list[[i]]

  # Ensure predictors match the ones used in model (top 20 features)
  dt_matrix <- as.matrix(sapply(dt[, top_features, with = FALSE], as.numeric))

  # Predict probabilities
  probs <- predict(final_model, newdata = dt_matrix)

  # Apply threshold
  preds_raw <- ifelse(probs > best_thresh, 1, 0)

  # Apply run-length smoothing: keep only 1s that are part of runs >=5
  rle_preds <- rle(preds_raw)
  rle_preds$values[rle_preds$lengths < smooth_run_length & rle_preds$values == 1] <- 0
  preds_smoothed <- inverse.rle(rle_preds)

  # Add to original data.table
  dt[, feeding := preds_smoothed]
  dt[, feeding_prob := probs]

  # Save back to the list
  features_list[[i]] <- dt
}


# Combine individual predictions
predicted_data <- data.table::rbindlist(features_list)

# Extract predicted feeding events
feeding_events <- predicted_data[feeding == 1]

# Total predicted feeding duration and number of bouts per individual (assuming 1 Hz data)
feeding_summary_individual <- feeding_events[, .(total_duration_seconds = .N,
                                                 feeding_bouts = sum(c(1, diff(as.numeric(datetime))) > 1)),
                                             by = ID]

################################################################################
## Plot hourly feeding events ##################################################
################################################################################

# Extract hour from datetime
predicted_data[, hour := lubridate::hour(datetime)]

# Compute total observations and feeding events per individual per hour
individual_hourly <- predicted_data[, .(total_obs = .N,
                                        feeding_events = sum(feeding == 1)),
                                    by = .(ID, hour)]

# Calculate per-individual feeding rate
individual_hourly[, feeding_rate := feeding_events / total_obs]

# Aggregate across individuals
summary_by_hour <- individual_hourly[, .(mean_feeding_rate = mean(feeding_rate, na.rm = TRUE),
                                         se_feeding_rate = sd(feeding_rate, na.rm = TRUE) / sqrt(.N)),
                                     by = hour]

summary_by_hour <- summary_by_hour[order(summary_by_hour$hour),]


# Basic plot (proportion of time - between 0 and 1 - spent feeding during each hour)
par(mgp=c(3,0.7,0))
x <- summary_by_hour$hour
y <- summary_by_hour$mean_feeding_rate
se <- summary_by_hour$se_feeding_rate
plot(x, y, type = "l", lwd = 2, col = "cyan4", ylim = c(0, max(y + se, na.rm = TRUE)),
     main = "Hourly predicted feeding rate (± SE)",  xlab = "Hour of day", ylab = "Feeding rate",
     axes = FALSE, cex.main = 1, cex.lab = 0.9)
# Add background
rect(par("usr")[1], par("usr")[3], par("usr")[2], par("usr")[4], col="grey97", border=NA)
# Add shaded ribbon for SE
polygon(c(x, rev(x)), c(y - se, rev(y + se)),
        col = adjustcolor("cyan4", alpha.f = 0.2), border = NA)
# Add the mean line again on top
lines(x, y, type = "l", lwd = 2, col = "cyan4")
points(x, y, pch = 19, col = "cyan4")
# Add axes
axis(1, at = seq(0, 23, by = 2), labels = paste0(seq(0, 23, by = 2), "h"), cex.axis = 0.8)
axis(2, las = 1, cex.axis = 0.8)
# Add box()
box()



################################################################################
## Feeding Bout Duration #######################################################
################################################################################

# Store results
feeding_bout_durations <- list()

for (i in seq_along(features_list)) {
  dt <- features_list[[i]]
  rle_out <- rle(dt$feeding)
  # Get the indices of feeding bouts (durations in seconds - assuming 1Hz)
  bout_lengths <- rle_out$lengths[rle_out$values == 1]
  if (length(bout_lengths) > 0) feeding_bout_durations[[i]] <- data.table::data.table(ID = unique(dt$ID), duration = bout_lengths)
}

# Combine into one table
feeding_bout_durations_dt <- data.table::rbindlist(feeding_bout_durations)

# Extract durations
dur <- feeding_bout_durations_dt$duration
# Compute density on log-transformed data
dens <- density(log10(dur))

# Density plot (feeding bout durations)
par(mgp=c(3,0.7,0))
# Set up empty plot with log-scaled x-axis and appropriate labels
plot(10^dens$x, dens$y, type = "n", log = "x", ylim = c(0, max(dens$y)*1.1),
     xlab = "Duration (seconds, log scale)", ylab = "Density", axes = FALSE,
     main = "Feeding Bout Durations", cex.main = 1, cex.lab = 0.9, yaxs = "i")
# Add background
rect(10^par("usr")[1], par("usr")[3], 10^par("usr")[2], par("usr")[4], col="grey97", border=NA)
# Draw shaded polygon under the density curve with semi-transparent blue
polygon(x = c(10^dens$x, rev(10^dens$x)), y = c(dens$y, rep(0, length(dens$y))),
        col = rgb(0, 0, 1, alpha = 0.3), border = NA)
# Add the density line on top of the shaded area
lines(10^dens$x, dens$y, col = "blue", lwd = 2)
# Add custom x-axis ticks and labels for clarity
x_ticks <- c(5, 10, 30, 60, 120, 300, 600, 780)
axis(1, at = x_ticks, labels = x_ticks)
axis(2, las = 1, cex.axis = 0.8)
# Add box()
box()


################################################################################
## Feeding Depths ##############################################################
################################################################################

# Separate depths
depths_feeding <- predicted_data[feeding == 1, depth_mean]
depths_nonfeeding <- predicted_data[feeding == 0, depth_mean]

# Create density estimates
dens_feed <- density(depths_feeding, na.rm = TRUE)
dens_nonfeed <- density(depths_nonfeeding, na.rm = TRUE)

# Histogram of predictedfeeding depths
par(mgp=c(3, 0.7, 0))
# Calculate log10-transformed depths (adding +1 to avoid log(0))
hist_depths <- log10(feeding_events$depth_mean + 1)
# Create histogram object first (without plotting)
h <- hist(hist_depths, breaks = 50, plot = FALSE)
# Set up empty plot with correct limits
plot(h, col = NA, border = NA, main = "", xlab = "", ylab = "", axes = FALSE, yaxs = "i")
# Add grey background across the full plotting area
usr <- par("usr")  # get current plot dimensions
rect(usr[1], usr[3], usr[2], usr[4], col = "grey97", border = NA)
# Plot the histogram again, now on top
plot(h, col = adjustcolor("cyan4", alpha = 0.75), border = "white", add = TRUE, axes = FALSE)
# Add custom x-axis in **metres** (10^x)
tick_log <- pretty(h$breaks, n=7)  # log10 values
tick_labels <- round(10^tick_log)  # original units
axis(1, at = tick_log, labels = tick_labels, cex.axis = 0.9)
# Add y-axis and labels
axis(2, las = 1, cex.axis = 0.9)
title(main = "Depth Distribution of Predicted Foraging Behaviour",
      xlab = "Mean depth (m)", ylab = "Frequency",
      cex.main = 1, cex.lab = 0.9)
box()









# Example density estimate (replace dens$x and dens$y with your data)
dens <- density(log10(feeding_bout_durations_dt$mean_depth), na.rm = TRUE)

# Convert back to original units (metres) for plotting on log scale
x_vals <- 10^dens$x  # back-transform
y_vals <- dens$y

# Save current plot parameters
old_par <- par(no.readonly = TRUE)

# Set up plot margins
par(mar = c(5, 5, 4, 2))

# Create empty plot with log x-axis
plot(x_vals, y_vals, type = "n", log = "x", ylim = c(0, max(y_vals, na.rm = TRUE) * 1.1),
     xlab = "Mean Depth (metres, log scale)", ylab = "Density",
     main = "Density of Mean Depths During Feeding Events")

# Add grey background over entire plot area
usr <- par("usr")  # get plot limits AFTER setting up the plot
rect(usr[1], usr[3], usr[2], usr[4], col = "grey97", border = NA)

# Plot density line and points
lines(x_vals, y_vals, col = "steelblue", lwd = 2)
points(x_vals, y_vals, col = "steelblue", pch = 16)

# Add custom x-axis with base-10 labels
log_ticks <- 10^(0:3)  # for example: 1, 10, 100, 1000
axis(side = 1, at = log_ticks, labels = log_ticks)

# Add y-axis
axis(side = 2)

# Add box around the plot
box()

# Restore original par settings
par(old_par)





# Plot non-feeding first (background)
plot(dens_nonfeed, col = "gray60", lwd = 2, main = "Depth distribution: Feeding vs. Non-feeding",
     xlab = "Depth (m)", ylab = "Density", ylim = c(0, max(dens_feed$y, dens_nonfeed$y)*1.1))

# Add feeding density
lines(dens_feed, col = "cyan4", lwd = 2)

# Add legend
legend("topright", legend = c("Feeding", "Non-feeding"),
       col = c("cyan4", "gray60"), lwd = 2, bty = "n")


