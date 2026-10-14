# Common utilities for R analytics
# Shared functions for data loading, preprocessing, and visualization

library(tidyverse)
library(lubridate)

# Load congestion data from CSV or the Spark Parquet lake.
load_congestion_data <- function(filepath = "/data/export/congestion.csv") {
  if (!file.exists(filepath) && !dir.exists(filepath)) {
    warning(paste("Data file not found:", filepath))
    return(NULL)
  }

  if (dir.exists(filepath) || grepl("\\.parquet$", filepath, ignore.case = TRUE)) {
    if (!requireNamespace("arrow", quietly = TRUE)) {
      stop("Reading Parquet requires the arrow R package")
    }
    data <- as_tibble(arrow::read_parquet(filepath))
  } else {
    data <- read_csv(filepath, show_col_types = FALSE)
  }

  # Normalize the current Spark output to the older report column names.  The
  # report can therefore read both historical CSVs and MapYourWay batches.
  if ("segment_id" %in% names(data) && !"road_id" %in% names(data)) {
    data$road_id <- data$segment_id
  }
  if ("current_speed_kmph" %in% names(data) && !"avg_speed" %in% names(data)) {
    data$avg_speed <- data$current_speed_kmph
  }
  if ("occupancy_pct" %in% names(data) && !"avg_occupancy" %in% names(data)) {
    data$avg_occupancy <- data$occupancy_pct / 100
  }
  if ("estimated_vehicle_count" %in% names(data)) {
    data$vehicle_count <- data$estimated_vehicle_count
  } else if (!"vehicle_count" %in% names(data)) {
    # record_count is the number of contributing events in overlapping Spark
    # windows, not a unique-vehicle measurement.
    data$vehicle_count <- NA_real_
  }
  if ("congestion_level" %in% names(data)) {
    level <- as.numeric(data$congestion_level)
    if ("congestion_label" %in% names(data)) {
      data$congestion_level <- case_when(
        data$congestion_label == "free_flow" ~ 0,
        data$congestion_label == "moderate" ~ 1 / 3,
        data$congestion_label == "heavy" ~ 2 / 3,
        data$congestion_label == "severe" ~ 1,
        TRUE ~ level
      )
    } else if (all(level %in% 0:3, na.rm = TRUE)) {
      data$congestion_level <- level / 3
    } else {
      data$congestion_level <- pmax(0, pmin(1, level))
    }
  }
  
  # Parse timestamp and add Mumbai-local time features. Spark exports ISO
  # timestamps; support epoch milliseconds for hand-built historical files.
  if (is.numeric(data$timestamp) && any(data$timestamp > 1e11, na.rm = TRUE)) {
    data$timestamp <- data$timestamp / 1000
  }
  source_values <- if ("source" %in% names(data)) as.character(data$source) else rep(NA_character_, nrow(data))
  prediction_values <- if ("prediction_source" %in% names(data)) as.character(data$prediction_source) else rep(NA_character_, nrow(data))
  data <- data %>%
    mutate(
       timestamp = with_tz(as_datetime(timestamp, tz = "UTC"), "Asia/Kolkata"),
       hour = hour(timestamp),
       day_of_week = wday(timestamp, label = TRUE),
       date = as_date(timestamp),
       data_source = coalesce(source_values, prediction_values, "unknown")
    )
  
  return(data)
}

# Calculate summary statistics
calculate_summary_stats <- function(data) {
  if (is.null(data) || nrow(data) == 0) {
    return(NULL)
  }
  
  summary <- data %>%
    group_by(road_id) %>%
    summarise(
      avg_speed = mean(avg_speed, na.rm = TRUE),
      avg_occupancy = mean(avg_occupancy, na.rm = TRUE),
      avg_congestion = mean(congestion_level, na.rm = TRUE),
      max_congestion = max(congestion_level, na.rm = TRUE),
      n_readings = n()
    )
  
  return(summary)
}

# Time-based aggregation
aggregate_by_hour <- function(data) {
  if (is.null(data) || nrow(data) == 0) {
    return(NULL)
  }
  
  hourly <- data %>%
    group_by(road_id, hour) %>%
    summarise(
      avg_speed = mean(avg_speed, na.rm = TRUE),
      avg_congestion = mean(congestion_level, na.rm = TRUE),
      vehicle_count = sum(vehicle_count, na.rm = TRUE),
      .groups = "drop"
    )
  
  return(hourly)
}

# Format congestion level as category
format_congestion_level <- function(level) {
  case_when(
    level < 0.3 ~ "Low",
    level < 0.6 ~ "Medium",
    level < 0.8 ~ "High",
    TRUE ~ "Severe"
  )
}

# Color palette for congestion levels
congestion_colors <- function() {
  c("Low" = "#2ecc71", "Medium" = "#f39c12", "High" = "#e74c3c", "Severe" = "#8e44ad")
}
