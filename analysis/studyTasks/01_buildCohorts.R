# A. Meta Info -----------------------

# Study: HIPSTER
# Name: Build Cohorts
# Date: 2023-09-14
# Description: The purpose of this script is to build the cohorts needed for the HIPSTER study.

# B. Dependencies ----------------------

library(tidyverse, quietly = TRUE)
library(DatabaseConnector)
library(config)

source("analysis/private/_utilities.R")
source("analysis/private/_buildCohorts.R")
source("analysis/private/_buildStrata.R")


# C. Connection ----------------------

# set connection Block
configBlock <- "optum"

# provide connection details
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = config::get("dbms", config = configBlock),
  user = config::get("user", config = configBlock),
  password = config::get("password", config = configBlock),
  connectionString = config::get("connectionString", config = configBlock)
)


# connect to database
con <- DatabaseConnector::connect(connectionDetails)


# D. Study Variables -----------------------

### Administrative Variables
executionSettings <- config::get(config = configBlock) %>%
  purrr::discard_at(c("dbms", "user", "password", "connectionString"))

outputFolder <- here::here("results") %>%
  fs::path(executionSettings$databaseName, "01_buildCohorts") %>%
  fs::dir_create()


### Add study variables or load from settings
cohortManifest <- getCohortManifest()


# E. Script --------------------

startSnowflakeSession(con, executionSettings)

### RUN ONCE - Initialize cohort tables #########

#debug(dropCohortTables)
dropCohortTables(executionSettings = executionSettings, con = con)

#debug(initializeCohortTables)
initializeCohortTables(executionSettings = executionSettings, con = con)


# Generate cohorts
generatedCohorts <- generateCohorts(
  executionSettings = executionSettings,
  con = con,
  cohortManifest = cohortManifest,
  outputFolder = outputFolder
)


# Build stratas

# debug(buildStrata)
# buildStrata(con = con,
#             executionSettings = executionSettings)

# F. Session Info ------------------------
#DatabaseConnector::disconnect(con)

