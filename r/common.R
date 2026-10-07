# Common utilities for R analytics
# Shared functions for data loading, preprocessing, and visualization

library(tidyverse)
library(lubridate)

# Load congestion data from CSV
load_congestion_data <- function(filepath = "/app/data/export/congestion.csv") {
  if (!file.exists(filepath)) {
    warning(paste("Data file not found:", filepath))
    return(NULL)
  }
  
  data <- read_csv(filepath, show_col_types = FALSE)
  
  # Parse timestamp and add time features
  data <- data %>%
    mutate(
      timestamp = as_datetime(timestamp),
      hour = hour(timestamp),
      day_of_week = wday(timestamp, label = TRUE),
      date = as_date(timestamp)
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
