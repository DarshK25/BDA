# Statistical analysis script for CGLS data
# Run this script to generate reports and analysis

source("/app/r/common.R")

library(tidyverse)
library(lubridate)
library(jsonlite)

# Main analysis function
main <- function() {
  cat("Loading congestion data...\n")
  data <- load_congestion_data()
  
  if (is.null(data)) {
    cat("No data available for analysis.\n")
    return()
  }
  
  cat(sprintf("Loaded %d records\n", nrow(data)))
  
  # Summary statistics
  cat("\n=== Summary Statistics ===\n")
  summary_stats <- calculate_summary_stats(data)
  print(summary_stats)
  
  # Peak hour analysis
  cat("\n=== Peak Hour Analysis ===\n")
  hourly_data <- aggregate_by_hour(data)
  peak_hours <- hourly_data %>%
    group_by(hour) %>%
    summarise(
      avg_congestion = mean(avg_congestion, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    arrange(desc(avg_congestion)) %>%
    head(5)
  
  cat("Top 5 congested hours:\n")
  print(peak_hours)
  
  # Most congested roads
  cat("\n=== Most Congested Roads ===\n")
  congested_roads <- data %>%
    group_by(road_id) %>%
    summarise(
      avg_congestion = mean(congestion_level, na.rm = TRUE),
      max_congestion = max(congestion_level, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    arrange(desc(avg_congestion)) %>%
    head(10)
  
  print(congested_roads)
  
  # Day of week patterns
  cat("\n=== Day of Week Patterns ===\n")
  dow_pattern <- data %>%
    group_by(day_of_week) %>%
    summarise(
      avg_congestion = mean(congestion_level, na.rm = TRUE),
      avg_speed = mean(avg_speed, na.rm = TRUE),
      .groups = "drop"
    )
  
  print(dow_pattern)
  
  # Export results
  results <- list(
    summary = summary_stats,
    peak_hours = peak_hours,
    congested_roads = congested_roads,
    dow_pattern = dow_pattern,
    analysis_timestamp = Sys.time()
  )
  
  output_path <- "/app/data/reports/analysis_results.json"
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  write_json(results, output_path, pretty = TRUE, auto_unbox = TRUE)
  
  cat(sprintf("\nAnalysis results saved to: %s\n", output_path))
}

# Run analysis
if (!interactive()) {
  main()
}
