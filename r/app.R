# Shiny Dashboard for CGLS Traffic Analytics
# Real-time visualization of traffic congestion and patterns

library(shiny)
library(shinydashboard)
library(tidyverse)
library(plotly)
library(DT)

source("/app/r/common.R")

# UI Definition
ui <- dashboardPage(
  dashboardHeader(title = "CGLS Traffic Analytics"),
  
  dashboardSidebar(
    sidebarMenu(
      menuItem("Overview", tabName = "overview", icon = icon("dashboard")),
      menuItem("Road Analysis", tabName = "roads", icon = icon("road")),
      menuItem("Time Patterns", tabName = "time", icon = icon("clock")),
      menuItem("Data Table", tabName = "data", icon = icon("table"))
    ),
    hr(),
    actionButton("refresh", "Refresh Data", icon = icon("refresh"), width = "90%")
  ),
  
  dashboardBody(
    tabItems(
      # Overview tab
      tabItem(
        tabName = "overview",
        fluidRow(
          valueBoxOutput("total_roads"),
          valueBoxOutput("avg_congestion"),
          valueBoxOutput("total_vehicles")
        ),
        fluidRow(
          box(
            title = "Congestion Over Time",
            status = "primary",
            solidHeader = TRUE,
            width = 12,
            plotlyOutput("congestion_timeline")
          )
        ),
        fluidRow(
          box(
            title = "Speed Distribution",
            status = "info",
            solidHeader = TRUE,
            width = 6,
            plotlyOutput("speed_histogram")
          ),
          box(
            title = "Congestion by Level",
            status = "warning",
            solidHeader = TRUE,
            width = 6,
            plotlyOutput("congestion_pie")
          )
        )
      ),
      
      # Road Analysis tab
      tabItem(
        tabName = "roads",
        fluidRow(
          box(
            title = "Top 10 Congested Roads",
            status = "danger",
            solidHeader = TRUE,
            width = 12,
            plotlyOutput("top_congested_roads")
          )
        ),
        fluidRow(
          box(
            title = "Road Performance Comparison",
            status = "primary",
            solidHeader = TRUE,
            width = 12,
            plotlyOutput("road_comparison")
          )
        )
      ),
      
      # Time Patterns tab
      tabItem(
        tabName = "time",
        fluidRow(
          box(
            title = "Hourly Congestion Pattern",
            status = "primary",
            solidHeader = TRUE,
            width = 12,
            plotlyOutput("hourly_pattern")
          )
        ),
        fluidRow(
          box(
            title = "Day of Week Pattern",
            status = "info",
            solidHeader = TRUE,
            width = 6,
            plotlyOutput("dow_pattern")
          ),
          box(
            title = "Peak Hours Heatmap",
            status = "warning",
            solidHeader = TRUE,
            width = 6,
            plotlyOutput("peak_heatmap")
          )
        )
      ),
      
      # Data Table tab
      tabItem(
        tabName = "data",
        fluidRow(
          box(
            title = "Raw Data",
            status = "primary",
            solidHeader = TRUE,
            width = 12,
            DTOutput("data_table")
          )
        )
      )
    )
  )
)

# Server Logic
server <- function(input, output, session) {
  
  # Reactive data loading
  data <- reactiveVal(NULL)
  
  # Load data on startup
  observe({
    data(load_congestion_data())
  })
  
  # Refresh button
  observeEvent(input$refresh, {
    showNotification("Refreshing data...", type = "message")
    data(load_congestion_data())
    showNotification("Data refreshed!", type = "message", duration = 2)
  })
  
  # Value boxes
  output$total_roads <- renderValueBox({
    df <- data()
    count <- if (!is.null(df)) n_distinct(df$road_id) else 0
    valueBox(count, "Active Roads", icon = icon("road"), color = "blue")
  })
  
  output$avg_congestion <- renderValueBox({
    df <- data()
    avg <- if (!is.null(df)) round(mean(df$congestion_level, na.rm = TRUE), 2) else 0
    color <- if (avg < 0.5) "green" else if (avg < 0.7) "yellow" else "red"
    valueBox(avg, "Avg Congestion", icon = icon("traffic-light"), color = color)
  })
  
  output$total_vehicles <- renderValueBox({
    df <- data()
    total <- if (!is.null(df)) sum(df$vehicle_count, na.rm = TRUE) else 0
    valueBox(total, "Total Vehicles", icon = icon("car"), color = "purple")
  })
  
  # Congestion timeline
  output$congestion_timeline <- renderPlotly({
    df <- data()
    if (is.null(df)) return(NULL)
    
    timeline <- df %>%
      group_by(timestamp) %>%
      summarise(avg_congestion = mean(congestion_level, na.rm = TRUE), .groups = "drop")
    
    plot_ly(timeline, x = ~timestamp, y = ~avg_congestion, type = "scatter", mode = "lines",
            line = list(color = "#3498db", width = 2)) %>%
      layout(xaxis = list(title = "Time"), yaxis = list(title = "Avg Congestion Level"))
  })
  
  # Speed histogram
  output$speed_histogram <- renderPlotly({
    df <- data()
    if (is.null(df)) return(NULL)
    
    plot_ly(df, x = ~avg_speed, type = "histogram", marker = list(color = "#2ecc71")) %>%
      layout(xaxis = list(title = "Speed (km/h)"), yaxis = list(title = "Frequency"))
  })
  
  # Congestion pie chart
  output$congestion_pie <- renderPlotly({
    df <- data()
    if (is.null(df)) return(NULL)
    
    df <- df %>%
      mutate(level_cat = format_congestion_level(congestion_level))
    
    congestion_counts <- df %>%
      count(level_cat)
    
    plot_ly(congestion_counts, labels = ~level_cat, values = ~n, type = "pie",
            marker = list(colors = congestion_colors()))
  })
  
  # Top congested roads
  output$top_congested_roads <- renderPlotly({
    df <- data()
    if (is.null(df)) return(NULL)
    
    top_roads <- df %>%
      group_by(road_id) %>%
      summarise(avg_congestion = mean(congestion_level, na.rm = TRUE), .groups = "drop") %>%
      arrange(desc(avg_congestion)) %>%
      head(10)
    
    plot_ly(top_roads, x = ~reorder(road_id, avg_congestion), y = ~avg_congestion,
            type = "bar", marker = list(color = "#e74c3c")) %>%
      layout(xaxis = list(title = "Road ID"), yaxis = list(title = "Avg Congestion"))
  })
  
  # Road comparison
  output$road_comparison <- renderPlotly({
    df <- data()
    if (is.null(df)) return(NULL)
    
    comparison <- df %>%
      group_by(road_id, hour) %>%
      summarise(avg_congestion = mean(congestion_level, na.rm = TRUE), .groups = "drop")
    
    plot_ly(comparison, x = ~hour, y = ~avg_congestion, color = ~road_id,
            type = "scatter", mode = "lines") %>%
      layout(xaxis = list(title = "Hour of Day"), yaxis = list(title = "Congestion Level"))
  })
  
  # Hourly pattern
  output$hourly_pattern <- renderPlotly({
    df <- data()
    if (is.null(df)) return(NULL)
    
    hourly <- aggregate_by_hour(df) %>%
      group_by(hour) %>%
      summarise(avg_congestion = mean(avg_congestion, na.rm = TRUE), .groups = "drop")
    
    plot_ly(hourly, x = ~hour, y = ~avg_congestion, type = "bar",
            marker = list(color = "#9b59b6")) %>%
      layout(xaxis = list(title = "Hour"), yaxis = list(title = "Avg Congestion"))
  })
  
  # Day of week pattern
  output$dow_pattern <- renderPlotly({
    df <- data()
    if (is.null(df)) return(NULL)
    
    dow <- df %>%
      group_by(day_of_week) %>%
      summarise(avg_congestion = mean(congestion_level, na.rm = TRUE), .groups = "drop")
    
    plot_ly(dow, x = ~day_of_week, y = ~avg_congestion, type = "bar",
            marker = list(color = "#f39c12")) %>%
      layout(xaxis = list(title = "Day of Week"), yaxis = list(title = "Avg Congestion"))
  })
  
  # Peak heatmap
  output$peak_heatmap <- renderPlotly({
    df <- data()
    if (is.null(df)) return(NULL)
    
    heatmap_data <- df %>%
      group_by(day_of_week, hour) %>%
      summarise(avg_congestion = mean(congestion_level, na.rm = TRUE), .groups = "drop")
    
    plot_ly(heatmap_data, x = ~hour, y = ~day_of_week, z = ~avg_congestion,
            type = "heatmap", colors = colorRamp(c("green", "yellow", "red"))) %>%
      layout(xaxis = list(title = "Hour"), yaxis = list(title = "Day"))
  })
  
  # Data table
  output$data_table <- renderDT({
    df <- data()
    if (is.null(df)) return(NULL)
    
    datatable(df, options = list(pageLength = 25, scrollX = TRUE),
              filter = "top", rownames = FALSE)
  })
}

# Run the application
shinyApp(ui = ui, server = server)
