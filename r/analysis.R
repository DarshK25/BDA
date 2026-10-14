# Batch report over the Parquet lake that Spark writes every micro-batch.
# Optional: docker compose --profile r run --rm r-report

source("/app/common.R")

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

  # Produce portable PNG reports alongside the machine-readable JSON output.
  report_dir <- "/data/reports/r"
  dir.create(report_dir, recursive = TRUE, showWarnings = FALSE)
  if (nrow(data) > 0) {
    timeline_plot <- data %>%
      group_by(timestamp) %>%
      summarise(avg_congestion = mean(congestion_level, na.rm = TRUE), .groups = "drop") %>%
      ggplot(aes(timestamp, avg_congestion)) +
      geom_line(color = "#3498db") +
      labs(title = "Mumbai Congestion Over Time", x = "Time (Asia/Kolkata)", y = "Congestion") +
      theme_minimal()
    ggsave(file.path(report_dir, "congestion_timeline.png"), timeline_plot, width = 10, height = 5, dpi = 120)
  }
  road_plot <- congested_roads %>%
    ggplot(aes(reorder(road_id, avg_congestion), avg_congestion)) +
    geom_col(fill = "#e74c3c") + coord_flip() +
    labs(title = "Most Congested Mumbai Road Links", x = "Road", y = "Average congestion") +
    theme_minimal()
  ggsave(file.path(report_dir, "top_congested_roads.png"), road_plot, width = 9, height = 6, dpi = 120)
  hourly_plot <- peak_hours %>%
    ggplot(aes(hour, avg_congestion)) + geom_col(fill = "#9b59b6") +
    labs(title = "Peak Congestion Hours", x = "Hour (Asia/Kolkata)", y = "Average congestion") +
    theme_minimal()
  ggsave(file.path(report_dir, "peak_hours.png"), hourly_plot, width = 8, height = 5, dpi = 120)
  write_csv(summary_stats, file.path(report_dir, "road_summary.csv"))
  
  # Export results
  results <- list(
    summary = summary_stats,
    peak_hours = peak_hours,
    congested_roads = congested_roads,
    dow_pattern = dow_pattern,
    analysis_timestamp = Sys.time()
  )
  
  output_path <- "/data/reports/analysis_results.json"
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  write_json(results, output_path, pretty = TRUE, auto_unbox = TRUE)
  
  cat(sprintf("\nAnalysis results saved to: %s\n", output_path))
}

# Run analysis
if (!interactive()) {
  main()
}
