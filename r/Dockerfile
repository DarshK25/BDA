# R Analytics and Shiny Dashboard
FROM rocker/shiny:4.3.2

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    && rm -rf /var/lib/apt/lists/*

# Install R packages
RUN R -e "install.packages(c('tidyverse', 'plotly', 'shiny', 'shinydashboard', 'DT', 'jsonlite', 'lubridate', 'ggplot2', 'dplyr', 'readr'), repos='https://cloud.r-project.org/')"

# Copy R scripts
COPY common.R /app/r/
COPY analysis.R /app/r/
COPY app.R /srv/shiny-server/

# Expose Shiny port
EXPOSE 3838

# Run Shiny server
CMD ["/usr/bin/shiny-server"]
