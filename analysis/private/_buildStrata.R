# A. Meta Info -----------------------

# Task: Build Strata
# Author: Martin Lavallee
# Date: 2023-05-03
# Description: The purpose of the _buildStrata.R script is to create stratas in the cohort table

# B. Functions ------------------------



## Age Strata ---------------
ageStrata <- function(con,
                      cohortDatabaseSchema,
                      cohortTable,
                      cdmDatabaseSchema,
                      targetId,
                      strataId,
                      ageMin,
                      ageMax) {
  
  cli::cat_bullet("Building age strata between ", ageMin, "-" ,ageMax,
                  bullet = "checkbox_on", bullet_col = "green")
  
  sql <- "
    SELECT
    t2.cohort_definition_id * 1000 + @strataId AS cohort_definition_id,
    t2.subject_id,
    t2.cohort_start_date,
    t2.cohort_end_date
  INTO #age
  FROM (
    SELECT
      t1.cohort_definition_id,
      t1.subject_id,
      t1.cohort_start_date,
      t1.cohort_end_date,
      CASE
        WHEN age between @ageMin and @ageMax THEN 1
        ELSE 0
      END AS ageStrata
    FROM (
      SELECT c.cohort_definition_id,
             c.subject_id,
             c.cohort_start_date,
             c.cohort_end_date,
             p.year_of_birth,
             abs(p.year_of_birth - EXTRACT(YEAR FROM c.cohort_start_date)) AS age
      FROM @cohortDatabaseSchema.@cohortTable c
      JOIN @cdmDatabaseSchema.person p
        ON p.person_id = c.subject_id
      WHERE c.cohort_definition_id IN (@targetId)
      ) t1
    ) t2
  WHERE t2.ageStrata = 1;

  DELETE FROM @cohortDatabaseSchema.@cohortTable
  WHERE cohort_definition_id in (select cohort_definition_id from #age);

  INSERT INTO @cohortDatabaseSchema.@cohortTable (
        	cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date
  )
  select * from #age;

  DROP TABLE #age;
"
  
  ageStrataSql <- SqlRender::render(
    sql,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cdmDatabaseSchema = cdmDatabaseSchema,
    targetId = targetId,
    strataId = strataId,
    ageMin = ageMin,
    ageMax = ageMax) %>%
    SqlRender::translate(targetDialect = con@dbms)
  
  DatabaseConnector::executeSql(connection = con, ageStrataSql, progressBar = FALSE)
  
  #TODO Add timing
  cohortStrataId <- targetId * 1000 + strataId
  cohortSchemaTable <- paste(cohortDatabaseSchema, cohortTable, sep = ".")
  cli::cat_bullet("Age strata written to ", cohortSchemaTable,
                  "using ids: ", paste(cohortStrataId, collapse = ", "),
                  bullet = "tick", bullet_col = "green")
  
  invisible(ageStrataSql)
}



cohortStrata <- function(con,
                         cohortDatabaseSchema,
                         cohortTable,
                         targetId,
                         strataId) {
  
  cli::cat_bullet("Building strata for target cohort id ", crayon::magenta(targetId),
                  " using strata cohort id ", crayon::magenta(strataId),
                  bullet = "checkbox_on", bullet_col = "green")
  
  # create ids for cohort with and without strata
  cohortIdWithStrata <- as.integer(paste0(as.character(targetId), "001", strataId))
  cohortIdWithoutStrata <- as.integer(paste0(as.character(targetId), "000", strataId))
  
  cohortStrataSql <- "
        select @cohortIdWithStrata as cohort_definition_id,
              tar_cohort.subject_id,
              tar_cohort.cohort_start_date,
              tar_cohort.cohort_end_date
        into #t_w_s_cohort
              from (
                select * from @cohortDatabaseSchema.@cohortTable
                where cohort_definition_id IN (@targetId)
              ) tar_cohort
        join (
              select * from @cohortDatabaseSchema.@cohortTable
              where cohort_definition_id IN (@strataId)
          ) strata_cohort
        ON tar_cohort.subject_id = strata_cohort.subject_id
        and strata_cohort.cohort_start_date <= tar_cohort.cohort_start_date
        and strata_cohort.cohort_end_date >= tar_cohort.cohort_start_date
        ;


        select @cohortIdWithoutStrata as cohort_definition_id,
              tar_cohort.subject_id,
              tar_cohort.cohort_start_date,
              tar_cohort.cohort_end_date

        into #t_wo_s_cohort
        from (
            select *
            from @cohortDatabaseSchema.@cohortTable
            where cohort_definition_id IN (@targetId)
          ) tar_cohort
        left join (
            select *
            from @cohortDatabaseSchema.@cohortTable
            where cohort_definition_id IN (@strataId)
          ) strata_cohort
        ON tar_cohort.subject_id = strata_cohort.subject_id
        and strata_cohort.cohort_start_date <= tar_cohort.cohort_start_date
        and strata_cohort.cohort_end_date >= tar_cohort.cohort_start_date
        where strata_cohort.subject_id is null
        ;

        delete from @cohortDatabaseSchema.@cohortTable where cohort_definition_id = @cohortIdWithStrata;

        delete from @cohortDatabaseSchema.@cohortTable where cohort_definition_id = @cohortIdWithoutStrata;

        INSERT INTO @cohortDatabaseSchema.@cohortTable (
        	cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date
        )
        -- T with S
        select cohort_definition_id, subject_id, cohort_start_date, cohort_end_date
        from #t_w_s_cohort

        union all
        -- T without S
        select cohort_definition_id, subject_id, cohort_start_date, cohort_end_date
        from #t_wo_s_cohort
        ;


        TRUNCATE TABLE #t_w_s_cohort;
        DROP TABLE #t_w_s_cohort;

        TRUNCATE TABLE #t_wo_s_cohort;
        DROP TABLE #t_wo_s_cohort;
  "
  
  cohortStrataSql <- SqlRender::render(
    cohortStrataSql,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    targetId = targetId,
    strataId = strataId,
    cohortIdWithStrata = cohortIdWithStrata,
    cohortIdWithoutStrata = cohortIdWithoutStrata) %>%
    SqlRender::translate(targetDialect = con@dbms)
  
  DatabaseConnector::executeSql(connection = con, cohortStrataSql, progressBar = FALSE)
  
  #TODO Add timing
  cohortSchemaTable <- paste(cohortDatabaseSchema, cohortTable, sep = ".")
  cli::cat_bullet("Cohort strata written to ", cohortSchemaTable,
                  bullet = "tick", bullet_col = "green")
  cli::cat_bullet("Age strata written to ", cohortSchemaTable,
                  "\n-cohort without strata ", cohortIdWithoutStrata,
                  "\n-cohort with strata ", cohortIdWithStrata,
                  bullet = "tick", bullet_col = "green")
  
  invisible(cohortStrataSql)
  
}


buildStrata <- function(con,
                        executionSettings,
                        analysisSettings) {
  
  #Step 0: Prep
  
  ## get schema vars
  cdmDatabaseSchema <- executionSettings$cdmDatabaseSchema
  workDatabaseSchema <- executionSettings$workDatabaseSchema
  cohortTable <- executionSettings$cohortTable
  databaseId <- executionSettings$databaseName
  
  outputFolder <- fs::path(here::here("results"), databaseId, analysisSettings$strata$outputFolder) %>%
    fs::dir_create()
  
  
  ## get cohort Ids
  targetCohorts <- analysisSettings$strata$cohorts$targetCohort
  cohortStrata <- analysisSettings$strata$cohorts$strataCohorts %>%
    dplyr::rename(
      strataId = id,
      strataName = name
    )
  demoStrata <- analysisSettings$strata$demographics
  
  cli::cat_rule("Building Demographic Strata")
  
  tb1 <- expand_grid(targetCohorts, demoStrata) %>%
    dplyr::mutate(
      strataId = id * 1000 + strataId,
      strataName = paste(name, strataName)
    ) %>%
    select(strataId, strataName)
  
  
  ageStrata(con,
            cohortDatabaseSchema = workDatabaseSchema,
            cohortTable = cohortTable,
            cdmDatabaseSchema = cdmDatabaseSchema,
            targetId = targetCohorts$id,
            strataId = demoStrata$strataId[1],
            ageMin = 18,
            ageMax = 39)
  
  ageStrata(con,
            cohortDatabaseSchema = workDatabaseSchema,
            cohortTable = cohortTable,
            cdmDatabaseSchema = cdmDatabaseSchema,
            targetId = targetCohorts$id,
            strataId = demoStrata$strataId[2],
            ageMin = 40,
            ageMax = 49)
  
  ageStrata(con,
            cohortDatabaseSchema = workDatabaseSchema,
            cohortTable = cohortTable,
            cdmDatabaseSchema = cdmDatabaseSchema,
            targetId = targetCohorts$id,
            strataId = demoStrata$strataId[3],
            ageMin = 50,
            ageMax = 59)
  
  ageStrata(con,
            cohortDatabaseSchema = workDatabaseSchema,
            cohortTable = cohortTable,
            cdmDatabaseSchema = cdmDatabaseSchema,
            targetId = demoStrata$strataId[4],
            strataId = 4,
            ageMin = 60,
            ageMax = 65)
  
  
  # cli::cat_rule("Building Cohort Strata")
  # strataMap <- tidyr::expand_grid(targetCohorts$id, cohortStrata$strataId)
  # 
  # purrr::pwalk(strataMap,
  #              ~cohortStrata(con = con,
  #                            cohortDatabaseSchema = workDatabaseSchema,
  #                            cohortTable = cohortTable,
  #                            targetId = ..1,
  #                            strataId = ..2))
  # 
  # 
  # cohortStrataTbl <- tibble::tibble(
  #   strataId = c(paste0("000", cohortStrata$strataId), paste0("001", cohortStrata$strataId)),
  #   strataName = c(paste("without", cohortStrata$strataName), paste("with", cohortStrata$strataName))
  # )
  # tb2 <- tidyr::expand_grid(targetCohorts, cohortStrataTbl) %>%
  #   dplyr::mutate(
  #     strataId = as.integer(paste0(as.character(id), strataId)),
  #     strataName = paste(name, strataName)
  #   )
  # 
  # 
  # strataKey <- dplyr::bind_rows(
  #   tb1,
  #   tb2 %>% select(strataId, strataName)
  # )
  # 
  # 
  # strataSummary <- dplyr::tbl(con, dbplyr::in_schema(workDatabaseSchema, cohortTable)) %>%
  #   dplyr::count(cohort_definition_id) %>%
  #   dplyr::collect() %>%
  #   dplyr::filter(cohort_definition_id > 1000)
  # 
  # 
  # dt <- strataKey %>%
  #   dplyr::left_join(strataSummary, by = c("strataId" = "cohort_definition_id")) %>%
  #   dplyr::rename(cohort_definition_id = strataId,
  #                 name = strataName) %>%
  #   dplyr::select(cohort_definition_id, name, n)
  # 
  # verboseSave(
  #   object = dt,
  #   saveName = "strata_table",
  #   saveLocation = outputFolder
  # )
  
  dt <- 0
  invisible(dt)
  
  
}
