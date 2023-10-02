# A. Meta Info -----------------------

# Task: Post-Index Analysis

source("analysis/private/_utilities.R")

# B. Functions ------------------------

# Post-Index module -------------
executePostIndex<- function(con,
                            executionSettings) {
  
  ## Remove scientific notation
  #options(scipen=999)
  
  ## Get variables
  cdmDatabaseSchema <- executionSettings$cdmDatabaseSchema
  workDatabaseSchema <- executionSettings$workDatabaseSchema
  cohortTable <- executionSettings$cohortTable
  databaseId <- executionSettings$databaseName
  outputFolder <- fs::path(here::here("results"), databaseId, "03_postIndex") %>%
    fs::dir_create()

  ## Start execution
  cli::cat_boxx("Building Post-Index Covariates (on index date)")
  cli::cat_line()
  
  tik <- Sys.time()
  
  catCovFra(con = con,
            cohortDatabaseSchema = workDatabaseSchema,
            cdmDatabaseSchema = cdmDatabaseSchema,
            cohortTable = cohortTable,
            database = databaseId,
            outputFolder = outputFolder)

  catCovCogn(con = con,
             cohortDatabaseSchema = workDatabaseSchema,
             cdmDatabaseSchema = cdmDatabaseSchema,
             cohortTable = cohortTable,
             database = databaseId,
             outputFolder = outputFolder)

  catCovMob(con = con,
            cohortDatabaseSchema = workDatabaseSchema,
            cdmDatabaseSchema = cdmDatabaseSchema,
            cohortTable = cohortTable,
            database = databaseId,
            outputFolder = outputFolder)

  catCovPathFra(con = con,
                cohortDatabaseSchema = workDatabaseSchema,
                cdmDatabaseSchema = cdmDatabaseSchema,
                cohortTable = cohortTable,
                database = databaseId,
                outputFolder = outputFolder)

  catCovAna(con = con,
            cohortDatabaseSchema = workDatabaseSchema,
            cdmDatabaseSchema = cdmDatabaseSchema,
            cohortTable = cohortTable,
            database = databaseId,
            outputFolder = outputFolder)

  catCovRes(con = con,
            cohortDatabaseSchema = workDatabaseSchema,
            cdmDatabaseSchema = cdmDatabaseSchema,
            cohortTable = cohortTable,
            database = databaseId,
            outputFolder = outputFolder)

  catCovASA(con = con,
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
  
  
  cli::cat_boxx("Building Post-Index Covariates (any time after and on index date)")
  cli::cat_line()
  
  tik <- Sys.time()
  
  catCovBoneMed(con = con,
                cohortDatabaseSchema = workDatabaseSchema,
                cdmDatabaseSchema = cdmDatabaseSchema,
                cohortTable = cohortTable,
                database = databaseId,
                outputFolder = outputFolder)

  contCovTimeToSurgery(con = con,
                       cohortDatabaseSchema = workDatabaseSchema,
                       cdmDatabaseSchema = cdmDatabaseSchema,
                       cohortTable = cohortTable,
                       database = databaseId,
                       outputFolder = outputFolder)

  catCovDeath(con = con,
              cohortDatabaseSchema = workDatabaseSchema,
              cdmDatabaseSchema = cdmDatabaseSchema,
              cohortTable = cohortTable,
              database = databaseId,
              outputFolder = outputFolder)

  catCovSex(con = con,
            cohortDatabaseSchema = workDatabaseSchema,
            cdmDatabaseSchema = cdmDatabaseSchema,
            cohortTable = cohortTable,
            database = databaseId,
            outputFolder = outputFolder)

  contCovAge(con = con,
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
catCovFra <- function(con,
                      cohortDatabaseSchema,
                      cdmDatabaseSchema,
                      cohortTable,
                      database,
                      type = "fracture",
                      outputFolder) {
  
  cli::cat_rule("Build Cohort Covariates: Fracture Type")
  
  
  # SQL code to get cohort covariates
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
      b.concept_id,
      concept_name
    from cts a
    left join @cdmDatabaseSchema.CONCEPT b on a.condition_concept_id =  b.concept_id
    where rnk=1
    group by cohort_definition_id, b.concept_id, concept_name;
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
    dplyr::mutate(pct = nn/totalSubjects,
                  database = database)

  
  verboseSave(
    object = tb,
    saveName = paste("catCov", type, sep = "_"),
    saveLocation = outputFolder
  )
  
  
  invisible(cohortCovariateSql)
}



catCovCogn <- function(con,
                       cohortDatabaseSchema,
                       cdmDatabaseSchema,
                       cohortTable,
                       database,
                       type = "cognStatus",
                       outputFolder) {
  
  cli::cat_rule("Build Cohort Covariates: Cognitive Status")
  
  
  # SQL code to get cohort covariates
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
      b.concept_id,
      concept_name
    from cts a
    left join @cdmDatabaseSchema.CONCEPT b on a.condition_concept_id =  b.concept_id
    where rnk=1
    group by cohort_definition_id,b.concept_id, concept_name;
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
    dplyr::mutate(pct = nn/totalSubjects,
                  database = database)
  
  
  verboseSave(
    object = tb,
    saveName = paste("catCov", type, sep = "_"),
    saveLocation = outputFolder
  )
  
  
  invisible(cohortCovariateSql)
}



catCovMob <- function(con,
                      cohortDatabaseSchema,
                      cdmDatabaseSchema,
                      cohortTable,
                      database,
                      type = "mobility",
                      outputFolder) {
  
  cli::cat_rule("Build Cohort Covariates: Pre-fracture mobility")
  
  
  # SQL code to get cohort covariates
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
      b.concept_id,
      concept_name
    from cts a
    left join @cdmDatabaseSchema.CONCEPT b on a.condition_concept_id =  b.concept_id
    where rnk=1
    group by cohort_definition_id,b.concept_id, concept_name;
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
    dplyr::mutate(pct = nn/totalSubjects,
                  database = database)
  
  
  verboseSave(
    object = tb,
    saveName = paste("catCov", type, sep = "_"),
    saveLocation = outputFolder
  )
  
  
  invisible(cohortCovariateSql)
}



catCovPathFra <- function(con,
                          cohortDatabaseSchema,
                          cdmDatabaseSchema,
                          cohortTable,
                          database,
                          type = "pathFra",
                          outputFolder) {
  
  cli::cat_rule("Build Cohort Covariates: Pathological fracture")
  
  
  # SQL code to get cohort covariates
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
      b.concept_id,
      concept_name
    from cts a
    left join @cdmDatabaseSchema.CONCEPT b on a.condition_concept_id =  b.concept_id
    where rnk=1
    group by cohort_definition_id,b.concept_id, concept_name;
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
    dplyr::mutate(pct = nn/totalSubjects,
                  database = database) 
  
  
  verboseSave(
    object = tb,
    saveName = paste("catCov", type, sep = "_"),
    saveLocation = outputFolder
  )
  
  
  invisible(cohortCovariateSql)
}


catCovAna <- function(con,
                      cohortDatabaseSchema,
                      cdmDatabaseSchema,
                      cohortTable,
                      database,
                      type = "anasthesia",
                      outputFolder) {
  
  cli::cat_rule("Build Cohort Covariates: Anasthesia Type")
  
  
  # SQL code to get cohort covariates
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
    b.concept_id,
    concept_name
  from cts a
  left join @cdmDatabaseSchema.CONCEPT b
  on a.procedure_concept_id =  b.concept_id
  where rnk=1
  group by cohort_definition_id,b.concept_id, concept_name;
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
    dplyr::mutate(pct = nn/totalSubjects,
                  database = database)
  
  
  verboseSave(
    object = tb,
    saveName = paste("catCov", type, sep = "_"),
    saveLocation = outputFolder
  )
  
  
  invisible(cohortCovariateSql)
}


catCovRes <- function(con,
                      cohortDatabaseSchema,
                      cdmDatabaseSchema,
                      cohortTable,
                      database,
                      type = "residence",
                      outputFolder) {
  
  cli::cat_rule("Build Cohort Covariates: Pre-fracture residence")
  
  
  # SQL code to get cohort covariates
  sql <- "
   -- Pre-fracture residence
   
with cts as (
    select subject_id, cohort_start_date, cohort_end_date, visit_start_date, cohort_definition_id, admitting_source_concept_id
     ,row_number()over(partition by subject_id, cohort_definition_id order by visit_start_date) as rnk
    from @cohortDatabaseSchema.@cohortTable a
    left join @cdmDatabaseSchema.VISIT_OCCURRENCE b
    on a.subject_id = b.person_id and a.cohort_start_date = b.visit_start_date
    where admitting_source_concept_id in (42898160,38004279)
  )
  select 
    count(subject_id) as nn, 
    cohort_definition_id,
    b.concept_id,
    concept_name
  from cts a
  left join @cdmDatabaseSchema.CONCEPT b
  on a.admitting_source_concept_id =  b.concept_id
  where rnk=1
  group by cohort_definition_id,b.concept_id, concept_name;
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
    dplyr::mutate(pct = nn/totalSubjects,
                  database = database)
  
  
  verboseSave(
    object = tb,
    saveName = paste("catCov", type, sep = "_"),
    saveLocation = outputFolder
  )
  
  
  invisible(cohortCovariateSql)
}


catCovASA <- function(con,
                      cohortDatabaseSchema,
                      cdmDatabaseSchema,
                      cohortTable,
                      database,
                      type = "ASA",
                      outputFolder) {
  
  cli::cat_rule("Build Cohort Covariates: ASA grade")
  
  
  # SQL code to get cohort covariates
  sql <- "
   -- ASA grade

      with cts as (
        select top 1000 subject_id, cohort_start_date, cohort_end_date, measurement_date, cohort_definition_id, measurement_concept_id, value_as_number
         ,row_number()over(partition by subject_id, cohort_definition_id order by measurement_date) as rnk
        from @cohortDatabaseSchema.@cohortTable a
        left join @cdmDatabaseSchema.MEASUREMENT b
        on a.subject_id = b.person_id and a.cohort_start_date = b.measurement_date
        where measurement_concept_id in (4159411)
    )
    select 
      count(subject_id) as nn, 
      cohort_definition_id,
      value_as_number,
      b.concept_id,
      concept_name
    from cts a
    left join @cdmDatabaseSchema.CONCEPT b
    on a.measurement_concept_id =  b.concept_id
    where rnk=1
    group by cohort_definition_id, b.concept_id, value_as_number, concept_name;
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
    dplyr::mutate(concept_name = paste0("ASA grade: ", value_as_number)) %>%
    dplyr::select(nn, cohortId, concept_name, totalEntries, totalSubjects) %>%
    dplyr::mutate(pct = nn/totalSubjects,
                  database = database) 
  
  
  verboseSave(
    object = tb,
    saveName = paste("catCov", type, sep = "_"),
    saveLocation = outputFolder
  )
  
  
  invisible(cohortCovariateSql)
}



catCovBoneMed <- function(con,
                          cohortDatabaseSchema,
                          cdmDatabaseSchema,
                          cohortTable,
                          database,
                          type = "boneMed",
                          outputFolder) {
  
  cli::cat_rule("Build Cohort Covariates: Bone medication")
  
  
  # SQL code to get cohort covariates
  sql <- "
   -- Bone medication

    with cts as (
      select subject_id, cohort_start_date, cohort_end_date, observation_date, cohort_definition_id, observation_concept_id
       ,row_number()over(partition by subject_id, cohort_definition_id order by observation_date) as rnk
      from @cohortDatabaseSchema.@cohortTable a
      left join @cdmDatabaseSchema.observation b
      on a.subject_id = b.person_id and a.cohort_start_date <= b.observation_date
      where observation_concept_id in (765842, 762564, 4164526)
    )
    select 
      count(subject_id) as nn, 
      cohort_definition_id,
      b.concept_id,
      concept_name
    from cts a
    left join @cdmDatabaseSchema.CONCEPT b
    on a.observation_concept_id =  b.concept_id
    where rnk=1
    group by cohort_definition_id, b.concept_id, concept_name;
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
    dplyr::mutate(pct = nn/totalSubjects,
                  database = database) 
  
  
  verboseSave(
    object = tb,
    saveName = paste("catCov", type, sep = "_"),
    saveLocation = outputFolder
  )
  
  
  invisible(cohortCovariateSql)
}



catCovDischarge <- function(con,
                            cohortDatabaseSchema,
                            cdmDatabaseSchema,
                            cohortTable,
                            database,
                            type = "discharge",
                            outputFolder) {
  
  cli::cat_rule("Build Cohort Covariates: Discharge location")
  
  
  # SQL code to get cohort covariates
  sql <- "
   -- Discharge location

    with cts as (
      select subject_id, cohort_start_date, cohort_end_date, visit_start_date, cohort_definition_id, visit_concept_id
       ,row_number()over(partition by subject_id, cohort_definition_id order by visit_start_date) as rnk
      from @cohortDatabaseSchema.@cohortTable a
      left join @cdmDatabaseSchema.visit_occurrence b
      on a.subject_id = b.person_id and a.cohort_start_date <= b.visit_start_date
      where discharge_to_concept_id in (42898160, 38004279, 38004285)
    )
    select 
      count(subject_id) as nn, 
      cohort_definition_id,
      b.concept_id,
      concept_name
    from cts a
    left join @cdmDatabaseSchema.CONCEPT b
    on a.discharge_to_concept_id = b.concept_id
    where rnk=1
    group by cohort_definition_id, b.concept_id, concept_name;
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
    dplyr::mutate(pct = nn/totalSubjects,
                  database = database) 
  
  
  verboseSave(
    object = tb,
    saveName = paste("catCov", type, sep = "_"),
    saveLocation = outputFolder
  )
  
  
  invisible(cohortCovariateSql)
}



catCovPostMob <- function(con,
                          cohortDatabaseSchema,
                          cdmDatabaseSchema,
                          cohortTable,
                          database,
                          type = "postMobility",
                          outputFolder) {
  
  cli::cat_rule("Build Cohort Covariates: Post-op mobility")
  
  
  # SQL code to get cohort covariates
  sql <- "
   -- Post-op mobility

    with cts as (
      select subject_id, cohort_start_date, cohort_end_date, procedure_date, cohort_definition_id, procedure_concept_id
       ,row_number()over(partition by subject_id, cohort_definition_id order by procedure_date) as rnk
      from @cohortDatabaseSchema.@cohortTable a
      left join @cdmDatabaseSchema.procedure_occurrence b
      on a.subject_id = b.person_id and a.cohort_start_date <= b.procedure_date
      where b.procedure_concept_id in (4040076)
    )
    select 
      count(subject_id) as nn, 
      cohort_definition_id,
      b.concept_id,
      concept_name
    from cts a
    left join @cdmDatabaseSchema.CONCEPT b
    on a.procedure_concept_id =  b.concept_id
    where rnk=1
    group by cohort_definition_id, b.concept_id, concept_name;
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
    dplyr::mutate(pct = nn/totalSubjects,
                  database = database) 
  
  
  verboseSave(
    object = tb,
    saveName = paste("catCov", type, sep = "_"),
    saveLocation = outputFolder
  )
  
  
  invisible(cohortCovariateSql)
}



catCovDeath <- function(con,
                        cohortDatabaseSchema,
                        cdmDatabaseSchema,
                        cohortTable,
                        database,
                        type = "death",
                        outputFolder) {
  
  cli::cat_rule("Build Cohort Covariates: Death")
  
  
  # SQL code to get cohort covariates
  sql <- "
   -- Death

    with cts as (
      select subject_id, cohort_start_date, cohort_end_date, death_date, cohort_definition_id, death_type_concept_id
       ,row_number()over(partition by subject_id, cohort_definition_id order by death_date) as rnk
      from @cohortDatabaseSchema.@cohortTable a
      left join @cdmDatabaseSchema.death b
      on a.subject_id = b.person_id and a.cohort_start_date <= b.death_date
      where b.death_type_concept_id in (32817)
    )
    select 
      count(subject_id) as nn, 
      cohort_definition_id,
      b.concept_id,
      concept_name
    from cts a
    left join @cdmDatabaseSchema.CONCEPT b
    on a.death_type_concept_id =  b.concept_id
    where rnk=1
    group by cohort_definition_id, b.concept_id, concept_name;
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
    dplyr::mutate(pct = nn/totalSubjects,
                  database = database) 
  
  
  verboseSave(
    object = tb,
    saveName = paste("catCov", type, sep = "_"),
    saveLocation = outputFolder
  )
  
  
  invisible(cohortCovariateSql)
}



catCovSex <- function(con,
                      cohortDatabaseSchema,
                      cdmDatabaseSchema,
                      cohortTable,
                      database,
                      type = "sex",
                      outputFolder) {
  
  cli::cat_rule("Build Cohort Covariates: Sex")
  
  
  # SQL code to get cohort covariates
  sql <- "
   -- Sex

    with cts as (
      select 
        subject_id, 
        cohort_start_date, 
        cohort_end_date, 
        cohort_definition_id, 
        b.gender_concept_id
      from @cohortDatabaseSchema.@cohortTable a
      left join @cdmDatabaseSchema.person b
      on a.subject_id = b.person_id 
    )
    select 
      count(subject_id) as nn, 
      cohort_definition_id,
      b.concept_id,
      concept_name
    from cts a
    left join @cdmDatabaseSchema.CONCEPT b
    on a.gender_concept_id =  b.concept_id
    group by cohort_definition_id, b.concept_id, concept_name;
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
    dplyr::mutate(pct = nn/totalSubjects,
                  database = database) 
  
  
  verboseSave(
    object = tb,
    saveName = paste("catCov", type, sep = "_"),
    saveLocation = outputFolder
  )
  
  
  invisible(cohortCovariateSql)
}



contCovAge <- function(con,
                       cohortDatabaseSchema,
                       cdmDatabaseSchema,
                       cohortTable,
                       database,
                       type = "age",
                       outputFolder) {
  
  cli::cat_rule("Build Cohort Covariates: Age")
  
  
  # SQL code to get percentiles (0.25, 0.5, 0.75)
  sql <- "
   -- Age

    with cts as (
      select 
        subject_id, 
        cohort_start_date, 
        cohort_end_date, 
        cohort_definition_id, 
        abs(YEAR(a.cohort_start_date) - b.year_of_birth) as age
      from @cohortDatabaseSchema.@cohortTable a
      left join @cdmDatabaseSchema.person b
      on a.subject_id = b.person_id 
    )
    select distinct
      percentile_cont(0.25) within group (order by age) over (partition by cohort_definition_id) as p25,
      percentile_cont(0.5) within group (order by age) over (partition by cohort_definition_id) as medianValue,
      percentile_cont(0.75) within group (order by age) over (partition by cohort_definition_id) as p75,
      cohort_definition_id
    from cts
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
  
  
  # SQL code to get min, max, mean and proportion
  sql2 <- "
   -- Age

    with cts as (
      select 
        subject_id, 
        cohort_start_date, 
        cohort_end_date, 
        cohort_definition_id, 
        abs(YEAR(a.cohort_start_date) - b.year_of_birth) as age
      from @cohortDatabaseSchema.@cohortTable a
      left join @cdmDatabaseSchema.person b
      on a.subject_id = b.person_id 
    )
    select distinct
      count(subject_id) as nn,
      avg(age) as meanValue,
      min(age) as minValue,
      max(age) as maxValue,
      cohort_definition_id
    from cts
    group by cohort_definition_id;
  "
  
  # Render and translate sql
  cohortCovariateSql2 <- SqlRender::render(
    sql2,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = cohortTable
  ) %>%
    SqlRender::translate(targetDialect = con@dbms)
  
  tb2 <- DatabaseConnector::querySql(connection = con, sql = cohortCovariateSql2)
  names(tb2) <- tolower(names(tb2))
  
  tbAll <- tb %>% 
    dplyr::left_join(tb2, by = c("cohort_definition_id"))
  
  cohortManifest <- readr::read_csv(file = here::here("results", database, "01_buildCohorts", "cohortManifest.csv"),
                                    show_col_types = FALSE)

  tb <- tbAll %>%
    dplyr::inner_join(cohortManifest, by = c("cohort_definition_id" = "id")) %>%
    dplyr::rename(cohortId = cohort_definition_id,
                  totalEntries = entries,
                  totalSubjects = subjects) %>%
    dplyr::select(-name, -type, -file) %>%
    dplyr::mutate(database = database,
                  type = type)
  
  
  verboseSave(
    object = tb,
    saveName = paste("contChar", type, sep = "_"),
    saveLocation = outputFolder
  )
  
  
  invisible(cohortCovariateSql)
}



contCovTimeToSurgery <- function(con,
                                 cohortDatabaseSchema,
                                 cdmDatabaseSchema,
                                 cohortTable,
                                 database,
                                 type = "timeToSurgery",
                                 outputFolder) {
  
  cli::cat_rule("Build Cohort Covariates: Time to surgery")
  
  
  # SQL code to get percentiles (0.25, 0.5, 0.75)
  sql <- "
   -- Time to surgery

     with cts as (
        select subject_id, cohort_start_date, cohort_end_date, observation_date, cohort_definition_id, observation_concept_id, value_as_number
         ,row_number()over(partition by subject_id, cohort_definition_id order by observation_date) as rnk
        from @cohortDatabaseSchema.@cohortTable a
        left join @cdmDatabaseSchema.observation b
        on a.subject_id = b.person_id and a.cohort_start_date <= b.observation_date
        where value_as_concept_id in (4078490)
    )
    select distinct
      percentile_cont(0.25) within group (order by value_as_number) over (partition by cohort_definition_id) as p25,
      percentile_cont(0.5) within group (order by value_as_number) over (partition by cohort_definition_id)  as medianValue,
      percentile_cont(0.75) within group (order by value_as_number) over (partition by cohort_definition_id)  as p75,
      cohort_definition_id
  from cts 
  where rnk=1
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
  
  
  # SQL code to get min, max, mean and proportion
  sql2 <- "
   -- Time to surgery

     with cts as (
        select subject_id, cohort_start_date, cohort_end_date, observation_date, cohort_definition_id, observation_concept_id, value_as_number
         ,row_number()over(partition by subject_id, cohort_definition_id order by observation_date) as rnk
        from @cohortDatabaseSchema.@cohortTable a
        left join @cdmDatabaseSchema.observation b
        on a.subject_id = b.person_id and a.cohort_start_date <= b.observation_date
        where value_as_concept_id in (4078490)
    )
    select
      count(subject_id) as nn,
      avg(value_as_number) as meanValue,
      min(value_as_number) as minValue,
      max(value_as_number) as maxValue,
      cohort_definition_id
  from cts 
  where rnk=1
  group by cohort_definition_id;
  "
  
  # Render and translate sql
  cohortCovariateSql2 <- SqlRender::render(
    sql2,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = cohortTable
  ) %>%
    SqlRender::translate(targetDialect = con@dbms)
  
  tb2 <- DatabaseConnector::querySql(connection = con, sql = cohortCovariateSql2)
  names(tb2) <- tolower(names(tb2))
  
  tbAll <- tb %>% 
    dplyr::left_join(tb2, by = c("cohort_definition_id"))
  
  cohortManifest <- readr::read_csv(file = here::here("results", database, "01_buildCohorts", "cohortManifest.csv"), 
                                    show_col_types = FALSE)
  
  tb <- tbAll %>%
    dplyr::inner_join(cohortManifest, by = c("cohort_definition_id" = "id")) %>%
    dplyr::rename(cohortId = cohort_definition_id,
                  totalEntries = entries,
                  totalSubjects = subjects) %>%
    dplyr::select(-name, -type, -file) %>%
    dplyr::mutate(database = database,
                  type = type)
  
  
  verboseSave(
    object = tb,
    saveName = paste("contChar", type, sep = "_"),
    saveLocation = outputFolder
  )
  
  
  invisible(cohortCovariateSql)
}
