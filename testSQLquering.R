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
 
 
 DatabaseConnector::disconnect(con)

