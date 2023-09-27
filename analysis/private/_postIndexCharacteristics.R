# A. Meta Info -----------------------

# Task: Post Index Analysis
# Date: 2023-07-26
# Description: The purpose of the _postIndexAnalysis.R script is to

source("analysis/private/_utilities.R")

# B. Functions ------------------------

# Post index module -------------
executePostIndex<- function(con,
                            executionSettings) {
  
  ## Remove scientific notation
  #options(scipen=999)
  
  ## Prep
  ## get schema vars
  cdmDatabaseSchema <- executionSettings$cdmDatabaseSchema
  workDatabaseSchema <- executionSettings$workDatabaseSchema
  cohortTable <- executionSettings$cohortTable
  databaseId <- executionSettings$databaseName
  
  outputFolder <- fs::path(here::here("results"), databaseId, "03_postIndex") %>%
    fs::dir_create()

  
  #Start execution talk
  cli::cat_boxx("Building Post-Index Covariates")
  cli::cat_line()
  
  tik <- Sys.time()
  
  # Run post-index
  cohortCovariatesFra(con = con,
                      cohortDatabaseSchema = workDatabaseSchema,
                      cdmDatabaseSchema = cdmDatabaseSchema,
                      cohortTable = cohortTable,
                      database = databaseId,
                      outputFolder = outputFolder)
  
  cohortCovariatesCogn(con = con,
                      cohortDatabaseSchema = workDatabaseSchema,
                      cdmDatabaseSchema = cdmDatabaseSchema,
                      cohortTable = cohortTable,
                      database = databaseId,
                      outputFolder = outputFolder)
  
  cohortCovariatesMob(con = con,
                       cohortDatabaseSchema = workDatabaseSchema,
                       cdmDatabaseSchema = cdmDatabaseSchema,
                       cohortTable = cohortTable,
                       database = databaseId,
                       outputFolder = outputFolder)
  
  cohortCovariatesPathFra(con = con,
                      cohortDatabaseSchema = workDatabaseSchema,
                      cdmDatabaseSchema = cdmDatabaseSchema,
                      cohortTable = cohortTable,
                      database = databaseId,
                      outputFolder = outputFolder)
  
  cohortCovariatesAna(con = con,
                          cohortDatabaseSchema = workDatabaseSchema,
                          cdmDatabaseSchema = cdmDatabaseSchema,
                          cohortTable = cohortTable,
                          database = databaseId,
                          outputFolder = outputFolder)
  
  cohortCovariatesRes(con = con,
                      cohortDatabaseSchema = workDatabaseSchema,
                      cdmDatabaseSchema = cdmDatabaseSchema,
                      cohortTable = cohortTable,
                      database = databaseId,
                      outputFolder = outputFolder)
  
  cohortCovariatesASA(con = con,
                      cohortDatabaseSchema = workDatabaseSchema,
                      cdmDatabaseSchema = cdmDatabaseSchema,
                      cohortTable = cohortTable,
                      database = databaseId,
                      outputFolder = outputFolder)
  
  tok <- Sys.time()
  cli::cat_bullet("Execution Completed at: ", crayon::red(tok),
                  bullet = "info", bullet_col = "blue")
  tdif <- tok - tik
  tok_format <- paste(scales::label_number(0.01)(as.numeric(tdif)), attr(tdif, "units"))
  cli::cat_bullet("Execution took: ", crayon::red(tok_format),
                  bullet = "info", bullet_col = "blue")
  
  invisible(tok)
}


# Cohort Covariates -----------
cohortCovariatesFra <- function(con,
                             cohortDatabaseSchema,
                             cdmDatabaseSchema,
                             cohortTable,
                             database,
                             type = "fracture",
                             outputFolder) {
  
  cli::cat_rule("Build Cohort Covariates: Fracture Type")
  
  
  # sql to get cohort covariates
  sql <- "
   -- Fracture Type
   
   with cts as (
      select subject_id, cohort_start_date, cohort_end_date, condition_start_date, cohort_definition_id, condition_concept_id
       ,row_number()over(partition by subject_id, cohort_definition_id order by condition_start_date) as rnk
      from @cohortDatabaseSchema.@cohortTable a
      left join @cdmDatabaseSchema.CONDITION_OCCURRENCE b
      on a.subject_id = b.person_id and a.cohort_start_date = b.condition_start_date
      where condition_concept_id in (433856,433856,4133012,4135748,4138412)
    )
    select 
      count(subject_id) as nn, 
      cohort_definition_id, 
      concept_name
    from cts a
    left join @cdmDatabaseSchema.CONCEPT b on a.condition_concept_id =  b.concept_id
    where rnk=1
    group by cohort_definition_id, concept_name;
"
  
  # Render and translate sql
  cohortCovariateSql <- SqlRender::render(
    sql,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = cohortTable
  ) %>%
    SqlRender::translate(targetDialect = con@dbms)
  
  
  tb <- DatabaseConnector::querySql(connection = con, sql = cohortCovariateSql)
  names(tb) <- tolower(names(tb))
  
  cohortManifest <- readr::read_csv(file = here::here("results", database, "01_buildCohorts", "cohortManifest.csv"), 
                                    show_col_types = FALSE)
  
  tb <- tb %>%
    dplyr::inner_join(cohortManifest, by = c("cohort_definition_id" = "id")) %>%
    dplyr::rename(cohortId = cohort_definition_id,
                  totalEntries = entries,
                  totalSubjects = subjects) %>%
    dplyr::select(nn, cohortId, concept_name, totalEntries, totalSubjects) %>%
    dplyr::mutate(pct = nn/totalSubjects) 

  
  verboseSave(
    object = tb,
    saveName = paste("cohortCovariates", type, sep = "_"),
    saveLocation = outputFolder
  )
  
  
  invisible(cohortCovariateSql)
}



cohortCovariatesCogn <- function(con,
                                cohortDatabaseSchema,
                                cdmDatabaseSchema,
                                cohortTable,
                                database,
                                type = "cognStatus",
                                outputFolder) {
  
  cli::cat_rule("Build Cohort Covariates: Cognitive Status")
  
  
  # sql to get cohort covariates
  sql <- "
   -- Cognitive Status
   
   with cts as (
      select subject_id, cohort_start_date, cohort_end_date, condition_start_date, cohort_definition_id, condition_concept_id
       ,row_number()over(partition by subject_id, cohort_definition_id order by condition_start_date) as rnk
      from @cohortDatabaseSchema.@cohortTable a
      left join @cdmDatabaseSchema.CONDITION_OCCURRENCE b
      on a.subject_id = b.person_id and a.cohort_start_date = b.condition_start_date
      where condition_concept_id in (42710016,4182210,443432)
    )
    select 
      count(subject_id) as nn, 
      cohort_definition_id, 
      concept_name
    from cts a
    left join @cdmDatabaseSchema.CONCEPT b on a.condition_concept_id =  b.concept_id
    where rnk=1
    group by cohort_definition_id, concept_name;
"
  
  # Render and translate sql
  cohortCovariateSql <- SqlRender::render(
    sql,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = cohortTable
  ) %>%
    SqlRender::translate(targetDialect = con@dbms)
  
  
  tb <- DatabaseConnector::querySql(connection = con, sql = cohortCovariateSql)
  names(tb) <- tolower(names(tb))
  
  cohortManifest <- readr::read_csv(file = here::here("results", database, "01_buildCohorts", "cohortManifest.csv"), 
                                    show_col_types = FALSE)
  
  tb <- tb %>%
    dplyr::inner_join(cohortManifest, by = c("cohort_definition_id" = "id")) %>%
    dplyr::rename(cohortId = cohort_definition_id,
                  totalEntries = entries,
                  totalSubjects = subjects) %>%
    dplyr::select(nn, cohortId, concept_name, totalEntries, totalSubjects) %>%
    dplyr::mutate(pct = nn/totalSubjects) 
  
  
  verboseSave(
    object = tb,
    saveName = paste("cohortCovariates", type, sep = "_"),
    saveLocation = outputFolder
  )
  
  
  invisible(cohortCovariateSql)
}



cohortCovariatesMob <- function(con,
                                 cohortDatabaseSchema,
                                 cdmDatabaseSchema,
                                 cohortTable,
                                 database,
                                 type = "mobility",
                                 outputFolder) {
  
  cli::cat_rule("Build Cohort Covariates: Pre-fracture mobility")
  
  
  # sql to get cohort covariates
  sql <- "
   -- Pre-fracture mobility
   
   with cts as (
      select subject_id, cohort_start_date, cohort_end_date, condition_start_date, cohort_definition_id, condition_concept_id
       ,row_number()over(partition by subject_id, cohort_definition_id order by condition_start_date) as rnk
      from @cohortDatabaseSchema.@cohortTable a
      left join @cdmDatabaseSchema.CONDITION_OCCURRENCE b
      on a.subject_id = b.person_id and a.cohort_start_date = b.condition_start_date
      where condition_concept_id in (4052958,4200353,4200818,4200353,4200818,4199109,4199092,4009877)
    )
    select 
      count(subject_id) as nn, 
      cohort_definition_id, 
      concept_name
    from cts a
    left join @cdmDatabaseSchema.CONCEPT b on a.condition_concept_id =  b.concept_id
    where rnk=1
    group by cohort_definition_id, concept_name;
"
  
  # Render and translate sql
  cohortCovariateSql <- SqlRender::render(
    sql,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = cohortTable
  ) %>%
    SqlRender::translate(targetDialect = con@dbms)
  
  
  tb <- DatabaseConnector::querySql(connection = con, sql = cohortCovariateSql)
  names(tb) <- tolower(names(tb))
  
  cohortManifest <- readr::read_csv(file = here::here("results", database, "01_buildCohorts", "cohortManifest.csv"), 
                                    show_col_types = FALSE)
  
  tb <- tb %>%
    dplyr::inner_join(cohortManifest, by = c("cohort_definition_id" = "id")) %>%
    dplyr::rename(cohortId = cohort_definition_id,
                  totalEntries = entries,
                  totalSubjects = subjects) %>%
    dplyr::select(nn, cohortId, concept_name, totalEntries, totalSubjects) %>%
    dplyr::mutate(pct = nn/totalSubjects) 
  
  
  verboseSave(
    object = tb,
    saveName = paste("cohortCovariates", type, sep = "_"),
    saveLocation = outputFolder
  )
  
  
  invisible(cohortCovariateSql)
}



cohortCovariatesPathFra <- function(con,
                                cohortDatabaseSchema,
                                cdmDatabaseSchema,
                                cohortTable,
                                database,
                                type = "pathFra",
                                outputFolder) {
  
  cli::cat_rule("Build Cohort Covariates: Pathological fracture")
  
  
  # sql to get cohort covariates
  sql <- "
   -- Pathological fracture
   
   with cts as (
      select subject_id, cohort_start_date, cohort_end_date, condition_start_date, cohort_definition_id, condition_concept_id
       ,row_number()over(partition by subject_id, cohort_definition_id order by condition_start_date) as rnk
      from @cohortDatabaseSchema.@cohortTable a
      left join @cdmDatabaseSchema.CONDITION_OCCURRENCE b
      on a.subject_id = b.person_id and a.cohort_start_date = b.condition_start_date
      where condition_concept_id in (45772710,45766906)
    )
    select 
      count(subject_id) as nn, 
      cohort_definition_id, 
      concept_name
    from cts a
    left join @cdmDatabaseSchema.CONCEPT b on a.condition_concept_id =  b.concept_id
    where rnk=1
    group by cohort_definition_id, concept_name;
"
  
  # Render and translate sql
  cohortCovariateSql <- SqlRender::render(
    sql,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = cohortTable
  ) %>%
    SqlRender::translate(targetDialect = con@dbms)
  
  
  tb <- DatabaseConnector::querySql(connection = con, sql = cohortCovariateSql)
  names(tb) <- tolower(names(tb))
  
  cohortManifest <- readr::read_csv(file = here::here("results", database, "01_buildCohorts", "cohortManifest.csv"), 
                                    show_col_types = FALSE)
  
  tb <- tb %>%
    dplyr::inner_join(cohortManifest, by = c("cohort_definition_id" = "id")) %>%
    dplyr::rename(cohortId = cohort_definition_id,
                  totalEntries = entries,
                  totalSubjects = subjects) %>%
    dplyr::select(nn, cohortId, concept_name, totalEntries, totalSubjects) %>%
    dplyr::mutate(pct = nn/totalSubjects) 
  
  
  verboseSave(
    object = tb,
    saveName = paste("cohortCovariates", type, sep = "_"),
    saveLocation = outputFolder
  )
  
  
  invisible(cohortCovariateSql)
}


cohortCovariatesAna <- function(con,
                                    cohortDatabaseSchema,
                                    cdmDatabaseSchema,
                                    cohortTable,
                                    database,
                                    type = "anasthesia",
                                    outputFolder) {
  
  cli::cat_rule("Build Cohort Covariates: Anasthesia Type")
  
  
  # sql to get cohort covariates
  sql <- "
   -- Anasthesia Type
   
with cts as (
    select subject_id, cohort_start_date, cohort_end_date, procedure_date, cohort_definition_id, procedure_concept_id
     ,row_number()over(partition by subject_id, cohort_definition_id order by procedure_date) as rnk
    from @cohortDatabaseSchema.@cohortTable a
    left join @cdmDatabaseSchema.PROCEDURE_OCCURRENCE b
    on a.subject_id = b.person_id and a.cohort_start_date = b.procedure_date
    where procedure_concept_id in (44174669,4332593,4100052)
  )
  select 
    count(subject_id) as nn, 
    cohort_definition_id, 
    concept_name
  from cts a
  left join @cdmDatabaseSchema.CONCEPT b
  on a.procedure_concept_id =  b.concept_id
  where rnk=1
  group by cohort_definition_id, concept_name;
"
  
  # Render and translate sql
  cohortCovariateSql <- SqlRender::render(
    sql,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = cohortTable
  ) %>%
    SqlRender::translate(targetDialect = con@dbms)
  
  
  tb <- DatabaseConnector::querySql(connection = con, sql = cohortCovariateSql)
  names(tb) <- tolower(names(tb))
  
  cohortManifest <- readr::read_csv(file = here::here("results", database, "01_buildCohorts", "cohortManifest.csv"), 
                                    show_col_types = FALSE)
  
  tb <- tb %>%
    dplyr::inner_join(cohortManifest, by = c("cohort_definition_id" = "id")) %>%
    dplyr::rename(cohortId = cohort_definition_id,
                  totalEntries = entries,
                  totalSubjects = subjects) %>%
    dplyr::select(nn, cohortId, concept_name, totalEntries, totalSubjects) %>%
    dplyr::mutate(pct = nn/totalSubjects) 
  
  
  verboseSave(
    object = tb,
    saveName = paste("cohortCovariates", type, sep = "_"),
    saveLocation = outputFolder
  )
  
  
  invisible(cohortCovariateSql)
}


cohortCovariatesRes <- function(con,
                                    cohortDatabaseSchema,
                                    cdmDatabaseSchema,
                                    cohortTable,
                                    database,
                                    type = "residence",
                                    outputFolder) {
  
  cli::cat_rule("Build Cohort Covariates: Pre-fracture residence")
  
  
  # sql to get cohort covariates
  sql <- "
   -- Pre-fracture residence
   
with cts as (
    select subject_id, cohort_start_date, cohort_end_date, visit_start_date, cohort_definition_id, admitting_source_concept_id
     ,row_number()over(partition by subject_id, cohort_definition_id order by visit_start_date) as rnk
    from @cohortDatabaseSchema.@cohortTable a
    left join @cdmDatabaseSchema.VISIT_OCCURRENCE b
    on a.subject_id = b.person_id and a.cohort_start_date = b.visit_start_date
    where admitting_source_concept_id in (42898160,38004279)
    order by subject_id, cohort_definition_id, rnk
  )
  select count(subject_id) as nn, cohort_definition_id, concept_name
  from cts a
  left join @cdmDatabaseSchema.CONCEPT b
  on a.admitting_source_concept_id =  b.concept_id
  where rnk=1
  group by cohort_definition_id, concept_name;
"
  
  # Render and translate sql
  cohortCovariateSql <- SqlRender::render(
    sql,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = cohortTable
  ) %>%
    SqlRender::translate(targetDialect = con@dbms)
  
  
  tb <- DatabaseConnector::querySql(connection = con, sql = cohortCovariateSql)
  names(tb) <- tolower(names(tb))
  
  cohortManifest <- readr::read_csv(file = here::here("results", database, "01_buildCohorts", "cohortManifest.csv"), 
                                    show_col_types = FALSE)
  
  tb <- tb %>%
    dplyr::inner_join(cohortManifest, by = c("cohort_definition_id" = "id")) %>%
    dplyr::rename(cohortId = cohort_definition_id,
                  totalEntries = entries,
                  totalSubjects = subjects) %>%
    dplyr::select(nn, cohortId, concept_name, totalEntries, totalSubjects) %>%
    dplyr::mutate(pct = nn/totalSubjects) 
  
  
  verboseSave(
    object = tb,
    saveName = paste("cohortCovariates", type, sep = "_"),
    saveLocation = outputFolder
  )
  
  
  invisible(cohortCovariateSql)
}


cohortCovariatesASA <- function(con,
                                cohortDatabaseSchema,
                                cdmDatabaseSchema,
                                cohortTable,
                                database,
                                type = "ASA",
                                outputFolder) {
  
  cli::cat_rule("Build Cohort Covariates: ASA grade")
  
  
  # sql to get cohort covariates
  sql <- "
   -- ASA grade
   
    select subject_id, cohort_start_date, cohort_end_date, measurement_date, cohort_definition_id, measurement_concept_id, value_as_number
     ,row_number()over(partition by subject_id, cohort_definition_id order by measurement_date) as rnk
    from @cohortDatabaseSchema.@cohortTable a
    left join @cdmDatabaseSchema.MEASUREMENT b
    on a.subject_id = b.person_id and a.cohort_start_date = b.measurement_date
    where measurement_concept_id in (42898160,38004279)
    order by subject_id, cohort_definition_id, rnk
"
  
  # Render and translate sql
  cohortCovariateSql <- SqlRender::render(
    sql,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = cohortTable
  ) %>%
    SqlRender::translate(targetDialect = con@dbms)
  
  
  tb <- DatabaseConnector::querySql(connection = con, sql = cohortCovariateSql)
  names(tb) <- tolower(names(tb))
  
  cohortManifest <- readr::read_csv(file = here::here("results", database, "01_buildCohorts", "cohortManifest.csv"), 
                                    show_col_types = FALSE)
  
  tb <- tb %>%
    dplyr::inner_join(cohortManifest, by = c("cohort_definition_id" = "id")) %>%
    dplyr::rename(cohortId = cohort_definition_id,
                  totalEntries = entries,
                  totalSubjects = subjects) %>%
    dplyr::select(nn, cohortId, concept_name, totalEntries, totalSubjects) %>%
    dplyr::mutate(pct = nn/totalSubjects) 
  
  
  verboseSave(
    object = tb,
    saveName = paste("cohortCovariates", type, sep = "_"),
    saveLocation = outputFolder
  )
  
  
  invisible(cohortCovariateSql)
}



