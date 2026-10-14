# R analytics image: ggplot2 report + Shiny app. Packages come from Debian (fast binary install, no compiling).
FROM r-base:4.4.1
RUN apt-get update \
 && apt-get install -y --no-install-recommends r-cran-shiny r-cran-ggplot2 r-cran-dplyr r-cran-tidyr \
    r-cran-scales r-cran-jsonlite \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ENV CGLS_DATA_DIR=/data CGLS_R_DIR=/app
