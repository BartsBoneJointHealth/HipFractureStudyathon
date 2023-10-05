### A. Install packages --------------

install.packages("DatabaseConnector")

library(DatabaseConnector)


### B. Connect to server --------------

connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = 'sqlite',
    #user = '<username>',
    #password = '<password>'
    server = '<server string or IP>'
  )

con <- DatabaseConnector::connect(connectionDetails = connectionDetails)


### C. Test quering SQL database

 sql <- "SELECT count(*) from @cdmDatabaseSchema.person;"

 sqlRendered <- SqlRender::render(sql = sql, cdmDatabaseSchema = '<databaseName.schemaName>') 
 
 sqlTranslated <- SqlRender::translate(sqlRendered, connectionDetails$dbms)

 result <-  DatabaseConnector::querySql(connection = con, sql = sqlTranslated)
 result
 


### D. Test building cohorts with CohortGenerator 
 
 install.packages("remotes")
 remotes::install_github("OHDSI/CohortGenerator")
 
 cohortsToCreate <- readRDS(file = "cohortToCreate.rds") # Add the directory of file cohortToCreate.rds (added in zip file along with this script)
 
 name <- "test"
 
 cohortTableNames <- list(cohortTable = paste0(name),
                          cohortInclusionTable = paste0(name, "_inclusion"),
                          cohortInclusionResultTable = paste0(name, "_inclusion_result"),
                          cohortInclusionStatsTable = paste0(name, "_inclusion_stats"),
                          cohortSummaryStatsTable = paste0(name, "_summary_stats"),
                          cohortCensorStatsTable = paste0(name, "_censor_stats"))
 
 # Build cohort tables
 CohortGenerator::createCohortTables(
   connection = con,
   cohortDatabaseSchema = 'databaseName.schemaName', # The name of the database+schema where results can be saved (Note that you will need write permissions in this schema)
   cohortTableNames = cohortTableNames,
   incremental = FALSE
   )
 
 # Generate cohorts
 CohortGenerator::generateCohortSet(
   connection = con,
   cdmDatabaseSchema = 'databaseName.schemaName',    # The name of the database+schema where OMOP data are stored
   cohortDatabaseSchema = 'databaseName.schemaName', # The name of the database+schema where results can be saved (Note that you will need write permissions in this schema)
   cohortTableNames = cohortTableNames,
   cohortDefinitionSet = cohortsToCreate,
   incremental = FALSE
 )
 
 # Generate cohort counts
 cohortCounts <- CohortGenerator::getCohortCounts(
   connection = con,
   cohortDatabaseSchema = 'databaseName.schemaName', # The name of the database+schema where results can be saved (Note that you will need write permissions in this schema)
   cohortTable = cohortTableNames$cohortTable
 )
 
 cohortCounts[1:4]

 
 ### F. Disconnect from server
 DatabaseConnector::disconnect(con)
