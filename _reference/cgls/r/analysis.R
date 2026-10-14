# Static analytics report with ggplot2.   Run:  Rscript r/analysis.R
# Writes PNG charts + summary.csv into <data>/reports/r/
script_dir <- tryCatch(dirname(normalizePath(sub("--file=", "", grep("--file=", commandArgs(FALSE), value = TRUE)[1]))),
                       error = function(e) ".")
source(file.path(script_dir, "common.R"))

out_dir <- file.path(DATA_DIR, "reports", "r")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

df <- read_congestion()
if (is.null(df)) stop("No data in ", CSV_PATH, " - start the pipeline first (docker compose up -d) and wait a few minutes.")
gt <- read_truth()
df <- add_truth(df, gt)
cat(sprintf("Loaded %d window rows, %d roads, %s to %s UTC\n", nrow(df), n_distinct(df$segment_id),
            format(min(df$time), "%H:%M:%S"), format(max(df$time), "%H:%M:%S")))

save <- function(p, name, w = 10, h = 5.5) {
  ggsave(file.path(out_dir, name), p, width = w, height = h, dpi = 150, bg = "white")
  cat("  wrote", name, "\n")
}

# 1. Heat map: every road over time ----------------------------------------------------------
road_order <- df %>% group_by(name) %>% summarise(m = mean(score)) %>% arrange(m) %>% pull(name)
p1 <- ggplot(df, aes(time, factor(name, levels = road_order), fill = score)) +
  geom_tile() +
  scale_fill_gradientn(colours = unname(LEVEL_COL), limits = c(0, 1), name = "congestion score",
                       values = rescale(c(0, 0.25, 0.5, 0.75))) +
  labs(title = "Congestion heat map", subtitle = "each tile = one 30 s window, one road", x = "time (UTC)", y = NULL) +
  theme_cgls()
save(p1, "01_heatmap.png")

# 2. Speed versus sensor occupancy, coloured by the model's level ---------------------------------
p2 <- ggplot(df, aes(occupancy_pct, avg_speed_kmph / free_flow_kmph, colour = level_name)) +
  geom_point(alpha = 0.55, size = 1.4) +
  scale_colour_manual(values = LEVEL_COL, name = "estimated level", drop = FALSE) +
  scale_y_continuous(labels = percent) +
  labs(title = "Two independent sensors agree", subtitle = "GPS speed (as % of free-flow) falls as loop-sensor occupancy rises",
       x = "sensor occupancy (%)", y = "GPS speed / free-flow speed") + theme_cgls()
save(p2, "02_speed_vs_occupancy.png")

# 3. Rain effect ---------------------------------------------------------------------------------
p3 <- ggplot(df, aes(rain_band, score, fill = rain_band)) +
  geom_boxplot(show.legend = FALSE, outlier.alpha = 0.3) +
  scale_fill_manual(values = c("dry" = "#9ecae1", "light rain" = "#4292c6", "heavy rain" = "#08519c")) +
  labs(title = "Weather impact on congestion", subtitle = "congestion score of all roads, by rain intensity",
       x = NULL, y = "congestion score (0 = free, 1 = gridlock)") + theme_cgls()
save(p3, "03_rain_effect.png", h = 4.5)

# 4. Incident timeline: the busiest road and its neighbours ------------------------------------------
worst <- df %>% group_by(segment_id) %>% summarise(m = max(score)) %>% slice_max(m, n = 1) %>% pull(segment_id)
focus <- df %>% filter(segment_id == worst[1])
inc <- focus %>% filter(incident_suspected) %>% summarise(t0 = min(time), t1 = max(time))
p4 <- ggplot(focus, aes(time)) +
  { if (nrow(inc) && is.finite(as.numeric(inc$t0))) annotate("rect", xmin = inc$t0, xmax = inc$t1, ymin = -Inf, ymax = Inf, fill = "#d62828", alpha = 0.12) } +
  geom_line(aes(y = score, colour = "estimated now"), linewidth = 1) +
  geom_line(aes(y = forecast_score, colour = "forecast (60 s ahead, shifted to when it was made)"), linewidth = 0.8, linetype = "dashed") +
  { if (!is.null(gt)) geom_line(aes(y = true_c_now, colour = "true (simulator)"), linewidth = 0.8, na.rm = TRUE) } +
  scale_colour_manual(values = c("estimated now" = "#1d4ed8", "forecast (60 s ahead, shifted to when it was made)" = "#7c3aed", "true (simulator)" = "#111111"), name = NULL) +
  labs(title = paste("Busiest road:", focus$name[1]), subtitle = "shaded = period flagged as a suspected incident",
       x = "time (UTC)", y = "congestion score") + theme_cgls()
save(p4, "04_incident_timeline.png")

# 5. Forecast quality ---------------------------------------------------------------------------
summary_rows <- list()
if (!is.null(gt) && any(!is.na(df$true_c_future))) {
  fq <- df %>% filter(!is.na(true_c_future))
  rmse <- function(a, b) sqrt(mean((a - b)^2))
  r_model <- rmse(fq$forecast_score, fq$true_c_future)
  r_persist <- rmse(fq$score, fq$true_c_future)
  acc_model <- mean(fq$forecast_level == fq$true_level_future)
  acc_persist <- mean(fq$level == fq$true_level_future)
  cat(sprintf("  forecast RMSE: model %.3f vs persistence %.3f | level accuracy %.1f%% vs %.1f%%\n",
              r_model, r_persist, 100 * acc_model, 100 * acc_persist))
  long <- fq %>% transmute(true = true_c_future, `learned forecast` = forecast_score, `assume nothing changes` = score) %>%
    pivot_longer(-true, names_to = "method", values_to = "predicted")
  p5 <- ggplot(long, aes(true, predicted)) +
    geom_point(alpha = 0.25, size = 0.9, colour = "#1d4ed8") +
    geom_abline(slope = 1, intercept = 0, colour = "#d62828") +
    facet_wrap(~method) + coord_equal(xlim = c(0, 1), ylim = c(0, 1)) +
    labs(title = "60-second forecast vs what really happened", subtitle = sprintf("RMSE: learned %.3f, naive %.3f   (red line = perfect)", r_model, r_persist),
         x = "true congestion 60 s later", y = "predicted") + theme_cgls()
  save(p5, "05_forecast_quality.png", h = 5)
  summary_rows$forecast_rmse_model <- r_model
  summary_rows$forecast_rmse_persistence <- r_persist
  summary_rows$forecast_level_acc_model <- acc_model
  summary_rows$forecast_level_acc_persistence <- acc_persist
}

# 6. Confusion matrix of the "now" estimate ---------------------------------------------------------
if (!is.null(gt) && any(!is.na(df$true_level_now))) {
  cm <- df %>% filter(!is.na(true_level_now)) %>%
    count(true = factor(LEVELS[true_level_now + 1], LEVELS), estimated = level_name, .drop = FALSE) %>%
    group_by(true) %>% mutate(share = ifelse(sum(n) == 0, 0, n / sum(n))) %>% ungroup()
  acc_now <- mean(df$level == df$true_level_now, na.rm = TRUE)
  p6 <- ggplot(cm, aes(estimated, true, fill = share)) + geom_tile(colour = "white") +
    geom_text(aes(label = ifelse(n > 0, n, "")), size = 4) +
    scale_fill_gradient(low = "white", high = "#1d4ed8", labels = percent, name = "row share") +
    scale_y_discrete(limits = rev(LEVELS)) +
    labs(title = "Estimated vs true congestion level", subtitle = sprintf("accuracy %.1f%%   (counts shown in tiles)", 100 * acc_now),
         x = "estimated by Spark + MLlib", y = "true (simulator)") + theme_cgls()
  save(p6, "06_confusion.png", h = 4.8, w = 7)
  summary_rows$accuracy_now <- acc_now
}

# 7. How old is a result when it is published? ---------------------------------------------------
p7 <- ggplot(df %>% filter(freshness_s < 60), aes(freshness_s)) +
  geom_histogram(binwidth = 1, fill = "#1d4ed8", colour = "white") +
  geom_vline(xintercept = median(df$freshness_s), colour = "#d62828") +
  labs(title = "Result freshness", subtitle = sprintf("median %.1f s after the end of the 30 s window (red line)", median(df$freshness_s)),
       x = "seconds between window end and publication", y = "windows") + theme_cgls()
save(p7, "07_freshness.png", h = 4)
summary_rows$freshness_median_s <- median(df$freshness_s)
summary_rows$freshness_p95_s <- unname(quantile(df$freshness_s, 0.95))

# 8. Road ranking -----------------------------------------------------------------------------
rank <- df %>% group_by(name) %>%
  summarise(mean_score = mean(score), share_heavy_or_severe = mean(level >= 2), mean_speed = mean(avg_speed_kmph, na.rm = TRUE), .groups = "drop") %>%
  arrange(desc(mean_score))
p8 <- ggplot(rank, aes(reorder(name, mean_score), share_heavy_or_severe)) +
  geom_col(fill = "#f07f1e") + coord_flip() + scale_y_continuous(labels = percent) +
  labs(title = "Where is time lost?", subtitle = "share of windows with HEAVY or SEVERE congestion", x = NULL, y = NULL) + theme_cgls()
save(p8, "08_road_ranking.png", h = 6)

summary <- data.frame(metric = names(summary_rows), value = round(unlist(summary_rows), 4))
write.csv(summary, file.path(out_dir, "summary.csv"), row.names = FALSE)
print(summary, row.names = FALSE)
cat("\nAll charts are in", out_dir, "\n")
