# Shiny analytics dashboard (R).  Run:  Rscript -e "shiny::runApp('r/app.R', port = 3838)"
# Reads the CSV that the Spark job appends to and refreshes itself every few seconds.
app_dir <- Find(function(d) nzchar(d) && file.exists(file.path(d, "common.R")),
                c(Sys.getenv("CGLS_R_DIR", ""), "r", ".", "/app"))
if (is.null(app_dir)) stop("common.R not found - run from the project folder or set CGLS_R_DIR")
source(file.path(app_dir, "common.R"))
library(shiny)

ui <- fluidPage(
  titlePanel("Smart City Traffic - R analytics (Shiny + ggplot2)"),
  sidebarLayout(
    sidebarPanel(width = 3,
      sliderInput("minutes", "Show the last N minutes", min = 2, max = 60, value = 15, step = 1),
      uiOutput("segment_picker"),
      sliderInput("refresh", "Refresh every (seconds)", min = 3, max = 30, value = 5, step = 1),
      hr(), textOutput("status")),
    mainPanel(width = 9,
      fluidRow(column(4, strong("Roads HEAVY / SEVERE now"), textOutput("n_heavy")),
               column(4, strong("Suspected incidents"), textOutput("n_incident")),
               column(4, strong("Median result age"), textOutput("fresh"))),
      tabsetPanel(
        tabPanel("Heat map", plotOutput("heat", height = 520)),
        tabPanel("Road trend", plotOutput("trend", height = 520)),
        tabPanel("Weather impact", plotOutput("weather", height = 460)),
        tabPanel("Forecast quality", plotOutput("forecast", height = 480), textOutput("forecast_note")),
        tabPanel("Data", tableOutput("table")))))
)

server <- function(input, output, session) {
  raw <- reactive({
    invalidateLater(input$refresh * 1000, session)
    d <- read_congestion()
    if (is.null(d)) return(NULL)
    add_truth(d, read_truth())
  })
  recent <- reactive({
    d <- raw(); req(d)
    d %>% filter(time >= max(time) - input$minutes * 60)
  })
  output$segment_picker <- renderUI({
    d <- raw(); req(d)
    sel <- isolate(input$segment)
    busiest <- d %>% filter(window_end_ms == max(window_end_ms)) %>% slice_max(score, n = 1, with_ties = FALSE) %>% pull(name)
    selectInput("segment", "Road", choices = sort(unique(d$name)), selected = if (is.null(sel)) busiest else sel)
  })
  output$status <- renderText({
    d <- raw()
    if (is.null(d)) paste("Waiting for", CSV_PATH) else sprintf("%d windows loaded; newest %s UTC", nrow(d), format(max(d$time), "%H:%M:%S"))
  })
  latest <- reactive({ d <- recent(); d %>% filter(window_end_ms == max(window_end_ms)) })
  output$n_heavy <- renderText({ as.character(sum(latest()$level >= 2)) })
  output$n_incident <- renderText({ l <- latest(); if (any(l$incident_suspected)) paste(l$name[l$incident_suspected], collapse = ", ") else "none" })
  output$fresh <- renderText({ sprintf("%.1f s", median(recent()$freshness_s)) })

  output$heat <- renderPlot({
    d <- recent()
    ord <- d %>% group_by(name) %>% summarise(m = mean(score)) %>% arrange(m) %>% pull(name)
    ggplot(d, aes(time, factor(name, levels = ord), fill = score)) + geom_tile() +
      scale_fill_gradientn(colours = unname(LEVEL_COL), limits = c(0, 1), values = rescale(c(0, 0.25, 0.5, 0.75)), name = "score") +
      labs(x = "time (UTC)", y = NULL) + theme_cgls()
  })
  output$trend <- renderPlot({
    d <- recent() %>% filter(name == input$segment); req(nrow(d) > 0)
    long <- d %>% transmute(time, `GPS speed (km/h)` = avg_speed_kmph, `sensor occupancy (%)` = occupancy_pct,
                            `congestion score x100` = 100 * score, `forecast x100 (60 s ahead)` = 100 * forecast_score) %>%
      pivot_longer(-time, names_to = "series", values_to = "value")
    inc <- d %>% filter(incident_suspected)
    ggplot(long, aes(time, value, colour = series)) +
      { if (nrow(inc)) annotate("rect", xmin = min(inc$time), xmax = max(inc$time), ymin = -Inf, ymax = Inf, fill = "#d62828", alpha = 0.12) } +
      geom_line(linewidth = 0.9, na.rm = TRUE) + labs(title = input$segment, subtitle = "red band = suspected incident", x = "time (UTC)", y = NULL) + theme_cgls()
  })
  output$weather <- renderPlot({
    ggplot(recent(), aes(rain_band, score, fill = rain_band)) + geom_boxplot(show.legend = FALSE) +
      labs(x = NULL, y = "congestion score") + theme_cgls()
  })
  fq <- reactive({ d <- recent(); d %>% filter(!is.na(true_c_future)) })
  output$forecast <- renderPlot({
    d <- fq(); validate(need(nrow(d) > 10, "Needs ground truth from the simulator and at least a minute of data."))
    long <- d %>% transmute(true = true_c_future, `learned forecast` = forecast_score, `assume nothing changes` = score) %>%
      pivot_longer(-true, names_to = "method", values_to = "predicted")
    ggplot(long, aes(true, predicted)) + geom_point(alpha = 0.3, colour = "#1d4ed8") + geom_abline(colour = "#d62828") +
      facet_wrap(~method) + coord_equal(xlim = c(0, 1), ylim = c(0, 1)) + labs(x = "true congestion 60 s later", y = "predicted") + theme_cgls()
  })
  output$forecast_note <- renderText({
    d <- fq(); if (nrow(d) < 10) return("")
    sprintf("RMSE: learned forecast %.3f vs naive %.3f (lower is better)", sqrt(mean((d$forecast_score - d$true_c_future)^2)), sqrt(mean((d$score - d$true_c_future)^2)))
  })
  output$table <- renderTable({
    latest() %>% arrange(desc(score)) %>%
      transmute(road = name, now = as.character(level_name), `speed km/h` = avg_speed_kmph, `occupancy %` = occupancy_pct,
                `in 60 s` = as.character(forecast_level_name), `forecast by` = forecast_source, incident = ifelse(incident_suspected, "YES", ""))
  })
}

shinyApp(ui, server)
