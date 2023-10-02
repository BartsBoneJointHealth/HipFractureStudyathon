# A. Meta Info -----------------------

# Task: Share Results


# B. Dependencies ----------------------

library(tidyverse, quietly = TRUE)
library(readr)
library(here)
source("analysis/private/_utilities.R")


# C. Connection ----------------------

# Set database block
configBlock <- "nhfd"


#bindAndShareResults()

#debug(bindFilesCat)
bindFilesCat(outputPath = here::here("report"),
             database = configBlock,
             filename = "catCov")


#debug(bindFilesCont)
bindFilesCont(outputPath = here::here("report"),
             database = configBlock,
             filename = "contChar")


allCohorts <- readr::read_csv(here::here("results/nhfd/02_buildStrata/allCohorts.csv"),
                              show_col_types = FALSE)


