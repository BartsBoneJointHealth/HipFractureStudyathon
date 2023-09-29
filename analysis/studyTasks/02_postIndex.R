# A. Meta Info -----------------------

# Task: Post-Index Characteristics

# B. Dependencies ----------------------

library(tidyverse, quietly = TRUE)
library(readr)
library(SqlRender)
source("analysis/private/_utilities.R")
source("analysis/private/_postIndexCharacteristics.R")


# C. Connection ----------------------

# set connection Block
configBlock <- "nhfd"

# provide connection details
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = config::get("dbms", config = configBlock),
  connectionString = config::get("connectionString", config = configBlock)
)


#connect to database
con <- DatabaseConnector::connect(connectionDetails)

# D. Variables -----------------------
### Administrative Variables
executionSettings <- config::get(config = configBlock) %>%
  purrr::discard_at(c("dbms", "user", "password", "connectionString"))


# E. Script --------------------

#debug(executePostIndex)
executePostIndex(con = con,
                 executionSettings = executionSettings)


# debug(bindFilesCat)
# bindFilesCat(outputPath = here::here("report"),
#              database = configBlock,
#              filename = "catCov")
# 
# 
# debug(bindFilesCont)
# bindFilesCont(outputPath = here::here("report"),
#              database = configBlock,
#              filename = "contChar")


# F. Disconnect from server --------------------
DatabaseConnector::disconnect(con)
