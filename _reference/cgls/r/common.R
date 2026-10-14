# Shared helpers for the R analytics (ggplot2 report + Shiny app).
# Data comes from the Spark job's flat CSV export and the simulator's ground-truth file:
#   <data>/export/congestion.csv        one row per road and 5-second window (Spark output)
#   <data>/ground_truth/gt.csv          hidden true congestion c (only the simulator knows it)
suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(ggplot2)
  library(scales)
})

DATA_DIR  <- Sys.getenv("CGLS_DATA_DIR", "/data")
CSV_PATH  <- file.path(DATA_DIR, "export", "congestion.csv")
GT_PATH   <- file.path(DATA_DIR, "ground_truth", "gt.csv")
LEVELS    <- c("FREE", "MODERATE", "HEAVY", "SEVERE")
LEVEL_COL <- c(FREE = "#2e9d4f", MODERATE = "#f2c200", HEAVY = "#f07f1e", SEVERE = "#d62828")

theme_cgls <- function() {
  theme_minimal(base_size = 12) +
    theme(plot.title = element_text(face = "bold"), legend.position = "bottom",
          panel.grid.minor = element_blank(), legend.key.width = unit(1.6, "cm"))
}

level_from_c <- function(c) {
  as.integer(cut(c, breaks = c(-Inf, 0.25, 0.5, 0.75, Inf), labels = 0:3, right = FALSE)) - 1L
}

# Read the Spark export. Safe to call while Spark is still appending (a half-written last line is dropped).
read_congestion <- function(path = CSV_PATH) {
  if (!file.exists(path)) return(NULL)
  df <- tryCatch(read.csv(path, stringsAsFactors = FALSE), error = function(e) NULL)
  if (is.null(df) || nrow(df) == 0) return(NULL)
  df <- df[!is.na(df$window_end_ms) & !is.na(df$segment_id), ]
  df <- df %>%
    arrange(emitted_ms) %>%
    distinct(segment_id, window_end_ms, .keep_all = TRUE) %>%       # replays after a restart: keep one row per window
    mutate(time = as.POSIXct(window_end_ms / 1000, origin = "1970-01-01", tz = "UTC"),
           level_name = factor(level_name, levels = LEVELS),
           forecast_level_name = factor(forecast_level_name, levels = LEVELS),
           rain_band = cut(rain_mm_hr, c(-Inf, 0.5, 8, Inf), labels = c("dry", "light rain", "heavy rain")),
           freshness_s = (emitted_ms - window_end_ms) / 1000,
           incident_suspected = as.logical(incident_suspected))
  df
}

read_truth <- function(path = GT_PATH) {
  if (!file.exists(path)) return(NULL)
  read.csv(path, stringsAsFactors = FALSE)
}

# Mean true congestion over the 30 s window that ends at (window_end + shift_ms)
truth_windows <- function(gt, shift_ms = 0) {
  bind_rows(lapply(0:5, function(k) {
    transmute(gt, segment_id, window_end_ms = ts_ms - shift_ms + k * 5000, c = c)
  })) %>%
    group_by(segment_id, window_end_ms) %>%
    summarise(c = mean(c), n = n(), .groups = "drop") %>%
    filter(n == 6) %>% select(-n)
}

# Add true level now and 60 s later; NA when ground truth is missing.
add_truth <- function(df, gt) {
  if (is.null(gt)) return(df)
  now <- truth_windows(gt, 0)    %>% rename(true_c_now = c)
  fut <- truth_windows(gt, 60000) %>% rename(true_c_future = c)
  df %>%
    left_join(now, by = c("segment_id", "window_end_ms")) %>%
    left_join(fut, by = c("segment_id", "window_end_ms")) %>%
    mutate(true_level_now = level_from_c(true_c_now), true_level_future = level_from_c(true_c_future))
}
