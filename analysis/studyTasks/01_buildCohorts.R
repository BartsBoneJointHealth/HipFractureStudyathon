# A. Meta Info -----------------------

# Task: Build Cohorts
# Description: The purpose of this script is to build the cohorts needed for the HIPSTER study.

# B. Dependencies ----------------------

library(tidyverse, quietly = TRUE)
library(DatabaseConnector)
library(config)
source("analysis/private/_utilities.R")
source("analysis/private/_buildCohorts.R")
source("analysis/private/_buildStrata.R")


# C. Connection ----------------------

# Set database block
configBlock <- "nhfd"

# Provide connection details
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = config::get("dbms", config = configBlock),
  connectionString = config::get("connectionString", config = configBlock)
)

# Connect to database
con <- DatabaseConnector::connect(connectionDetails)


# D. Study Variables -----------------------

# Administrative Variables
executionSettings <- config::get(config = configBlock) %>%
  purrr::discard_at(c("user", "password", "connectionString"))

outputFolder <- here::here("results") %>%
  fs::path(executionSettings$databaseName, "01_buildCohorts") %>%
  fs::dir_create()

# Add study variables
cohortManifest <- getCohortManifest()


# E. Script --------------------

### RUN ONCE - Initialize cohort tables ###
#initializeCohortTables(executionSettings = executionSettings, con = con)
# 
# # Generate cohorts
# generatedCohorts <- generateCohorts(
#   executionSettings = executionSettings,
#   con = con,
#   cohortManifest = cohortManifest,
#   outputFolder = outputFolder
# )

# Build stratas

debug(buildStrata)
buildStrata(con = con,
            executionSettings = executionSettings)


outputFolder <- here::here("results") %>%
  fs::path(executionSettings$databaseName, "02_buildStrata") %>%
  fs::dir_create()

# Count cohorts
countedCohorts <- countCohorts(
  executionSettings = executionSettings,
  con = con,
  cohortManifest = cohortManifest,
  outputFolder = outputFolder
)


# F. Disconnect from server --------------------

DatabaseConnector::disconnect(con)

