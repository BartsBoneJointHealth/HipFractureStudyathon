# A. Meta Info -----------------------

# Task: Post-Index Characteristics


# B. Dependencies ----------------------

library(tidyverse, quietly = TRUE)
library(readr)
library(SqlRender)
source("analysis/private/_utilities.R")
source("analysis/private/_postIndexCharacteristics.R")


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


# D. Variables -----------------------

# Administrative Variables
executionSettings <- config::get(config = configBlock) %>%
  purrr::discard_at(c("dbms", "user", "password", "connectionString"))


# E. Script --------------------

#debug(executePostIndex)
executePostIndex(con = con,
                 executionSettings = executionSettings)


# F. Disconnect from server --------------------

DatabaseConnector::disconnect(con)

