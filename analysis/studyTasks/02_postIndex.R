# A. Meta Info -----------------------

# Task: Post Index Characteristics
# Date: 2023-05-03


# B. Dependencies ----------------------

library(tidyverse, quietly = TRUE)
library(readr)
library(SqlRender)
source("analysis/private/_utilities.R")
source("analysis/private/_postIndexCharacteristics.R")


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


#connect to database
con <- DatabaseConnector::connect(connectionDetails)

# D. Variables -----------------------
### Administrative Variables
executionSettings <- config::get(config = configBlock) %>%
  purrr::discard_at(c("dbms", "user", "password", "connectionString"))


# E. Script --------------------
#######if BAYER uncomment this line#################
startSnowflakeSession(con, executionSettings)

## Get Baseline Covariates

#debug(executePostIndex)
executePostIndex(con = con,
                 executionSettings = executionSettings)



