# A. Meta Info -----------------------

# Task: Build stratas

# B. Functions ------------------------



## Initial Stratas ---------------


## Additional Stratas ---------------
ageStrata <- function(con,
                      cohortDatabaseSchema,
                      cohortTable,
                      cdmDatabaseSchema,
                      targetId,
                      strataId,
                      ageMin,
                      ageMax) {
  
  cli::cat_bullet("Building Age strata: ", ageMin, "-" ,ageMax, " for target cohort id: ", targetId, bullet = "checkbox_on", bullet_col = "green")
  
  sql <- "
    SELECT
    t2.cohort_definition_id * 100 + @strataId AS cohort_definition_id,
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
             abs(p.year_of_birth - YEAR(c.cohort_start_date)) AS age
      FROM @cohortDatabaseSchema.@cohortTable c
      JOIN @cdmDatabaseSchema.person p
        ON p.person_id = c.subject_id
      WHERE c.cohort_definition_id IN (@targetId)
      ) t1
    ) t2
  WHERE t2.ageStrata = 1;

  DELETE FROM @cohortDatabaseSchema.@cohortTable WHERE cohort_definition_id in (select cohort_definition_id from #age);

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
  
  DatabaseConnector::executeSql(connection = con, ageStrataSql, progressBar = TRUE)
  

  invisible(ageStrataSql)
}



sexStrata <- function(con,
                      cohortDatabaseSchema,
                      cohortTable,
                      cdmDatabaseSchema,
                      targetId) {
  
  cli::cat_bullet("Building Sex strata for target cohort id: ", targetId, bullet = "checkbox_on", bullet_col = "green")
  
  sql <- "
    SELECT
      cohort_definition_id,
      t2.subject_id,
      t2.cohort_start_date,
      t2.cohort_end_date,
      t2.sexStrata
  INTO #sex
  FROM (
    SELECT
      t1.cohort_definition_id,
      t1.subject_id,
      t1.cohort_start_date,
      t1.cohort_end_date,
      CASE
        WHEN sex in (8532) THEN 1
        WHEN sex in (8507) THEN 2
        ELSE 0
      END AS sexStrata
    FROM (
      SELECT c.cohort_definition_id,
             c.subject_id,
             c.cohort_start_date,
             c.cohort_end_date,
             p.gender_concept_id AS sex
      FROM @cohortDatabaseSchema.@cohortTable c
      JOIN @cdmDatabaseSchema.person p ON p.person_id = c.subject_id
      WHERE c.cohort_definition_id IN (@targetId)
      ) t1
    ) t2;

  DELETE FROM @cohortDatabaseSchema.@cohortTable 
  --WHERE cohort_definition_id in (select cohort_definition_id from #sex where cohort_definition_id between 1000 and 44002);
  WHERE cohort_definition_id between 1000 and 44002;

  INSERT INTO @cohortDatabaseSchema.@cohortTable (
        	cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date
  )
  select  CAST(cohort_definition_id * 1000 + 1 AS bigint) AS cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date 
  from #sex
  where sexStrata = 1;
  
  
  INSERT INTO @cohortDatabaseSchema.@cohortTable (
        	cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date
  )
  select  cohort_definition_id * 1000 + 2 AS cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date 
  from #sex
  where sexStrata = 2;
  
  INSERT INTO @cohortDatabaseSchema.@cohortTable (
        	cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date
  )
  select  cohort_definition_id * 1000 + 0 AS cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date 
  from #sex
  where sexStrata = 0;

  DROP TABLE #sex;
"
  
  sexStrataSql <- SqlRender::render(
    sql,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cdmDatabaseSchema = cdmDatabaseSchema,
    targetId = targetId) %>%
    SqlRender::translate(targetDialect = con@dbms)
  
  DatabaseConnector::executeSql(connection = con, sexStrataSql, progressBar = TRUE)
  
  
  invisible(sexStrataSql)
}



cognStrata <- function(con,
                       cohortDatabaseSchema,
                       cohortTable,
                       cdmDatabaseSchema,
                       targetId) {
  
  cli::cat_bullet("Building Cognitive Status strata for target cohort id: ", targetId, bullet = "checkbox_on", bullet_col = "green")
  
  sql <- "
  select
      cohort_definition_id,
      t2.subject_id,
      t2.cohort_start_date,
      t2.cohort_end_date,
      t2.cat
  INTO #concept
  FROM
  (
  select subject_id, cohort_start_date, cohort_end_date, cohort_definition_id, condition_concept_id,
         case 
           when condition_concept_id in (4182210) then 1
           when condition_concept_id in (42710016) then 2
           when condition_concept_id in (443432) then 3
           else 0 end as cat
  FROM (
        select subject_id, cohort_start_date, cohort_end_date, condition_start_date, condition_concept_id, cohort_definition_id,
            row_number()over(partition by subject_id, cohort_definition_id order by condition_start_date) as rnk
        from @cohortDatabaseSchema.@cohortTable a
        left join  @cdmDatabaseSchema.CONDITION_OCCURRENCE b
         on a.subject_id = b.person_id and a.cohort_start_date = b.condition_start_date 
        where cohort_definition_id IN (@targetId) and b.condition_concept_id in (42710016, 4182210, 443432)
  )t1 
  WHERE rnk =1
    )t2;

  DELETE FROM @cohortDatabaseSchema.@cohortTable WHERE cohort_definition_id between 1000000 and 44000003;


  -- CAT1: Dementia
  INSERT INTO @cohortDatabaseSchema.@cohortTable (
        	cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date
  )
  select  CAST(cohort_definition_id * 1000000 + 1 AS bigint) AS cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date 
  from #concept
  where cat = 1;
  
  
  -- CAT2: Normal Cognition
  INSERT INTO @cohortDatabaseSchema.@cohortTable (
        	cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date
  )
  select  cohort_definition_id * 1000000 + 2 AS cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date 
  from #concept
  where cat = 2;
  
  
  -- CAT3: Impaired Cognition
  INSERT INTO @cohortDatabaseSchema.@cohortTable (
        	cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date
  )
  select  cohort_definition_id * 1000000 + 3 AS cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date 
  from #concept
  where cat = 3;
  
  
  -- CAT4: Unknown
  INSERT INTO @cohortDatabaseSchema.@cohortTable (
        	cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date
  )
  select  cohort_definition_id * 1000000 + 0 AS cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date 
  from #concept
  where cat = 0;

  DROP TABLE #concept;
"
  
  cognStrataSql <- SqlRender::render(
    sql,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cdmDatabaseSchema = cdmDatabaseSchema,
    targetId = targetId) %>%
    SqlRender::translate(targetDialect = con@dbms)
  
  DatabaseConnector::executeSql(connection = con, cognStrataSql, progressBar = TRUE)
  
  invisible(cognStrataSql)
}



fracStrata <- function(con,
                       cohortDatabaseSchema,
                       cohortTable,
                       cdmDatabaseSchema,
                       targetId) {
  
  cli::cat_bullet("Building Fracture Type strata for target cohort id: ", targetId, bullet = "checkbox_on", bullet_col = "green")
  
  sql <- "
  select
      cohort_definition_id,
      t2.subject_id,
      t2.cohort_start_date,
      t2.cohort_end_date,
      t2.cat
  INTO #concept
  FROM
  (
  select subject_id, cohort_start_date, cohort_end_date, cohort_definition_id, condition_concept_id,
         case 
           when condition_concept_id in (433856) then 1   -- Intracapsular displaced/non-displaced (Fracture of neck of femur)
           when condition_concept_id in (4133012) then 2  -- Intertrochanteric
           when condition_concept_id in (4135748) then 3  -- Subtrochanteric
           when condition_concept_id in (4138412) then 4  -- Other
           else 5 end as cat                              
  FROM (
        select subject_id, cohort_start_date, cohort_end_date, condition_start_date, condition_concept_id, cohort_definition_id,
            row_number()over(partition by subject_id, cohort_definition_id order by condition_start_date) as rnk
        from @cohortDatabaseSchema.@cohortTable a
        left join  @cdmDatabaseSchema.CONDITION_OCCURRENCE b
         on a.subject_id = b.person_id and a.cohort_start_date = b.condition_start_date 
        where cohort_definition_id IN (@targetId) and b.condition_concept_id in (45772710, 45766906)
  )t1 
  WHERE rnk =1
    )t2;

  DELETE FROM @cohortDatabaseSchema.@cohortTable WHERE cohort_definition_id between 10000 and 44003;


  -- CAT1: Intracapsular displaced/non-displaced
  INSERT INTO @cohortDatabaseSchema.@cohortTable (
        	cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date
  )
  select  CAST(cohort_definition_id * 10000 + 1 AS bigint) AS cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date 
  from #concept
  where cat = 1;
  
  
  -- CAT2: Intertrochanteric
  INSERT INTO @cohortDatabaseSchema.@cohortTable (
        	cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date
  )
  select  cohort_definition_id * 10000 + 2 AS cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date 
  from #concept
  where cat = 2;
  
  
  -- CAT3: Subtrochanteric
  INSERT INTO @cohortDatabaseSchema.@cohortTable (
        	cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date
  )
  select  cohort_definition_id * 10000 + 3 AS cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date 
  from #concept
  where cat = 3;
  
  
  -- CAT4: Other
  INSERT INTO @cohortDatabaseSchema.@cohortTable (
        	cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date
  )
  select  cohort_definition_id * 10000 + 4 AS cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date 
  from #concept
  where cat = 4;
  
  
    -- CAT5: Unknown
  INSERT INTO @cohortDatabaseSchema.@cohortTable (
        	cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date
  )
  select  cohort_definition_id * 10000 + 0 AS cohort_definition_id,
        	subject_id,
        	cohort_start_date,
        	cohort_end_date 
  from #concept
  where cat = 0;

  DROP TABLE #concept;
"
  
  cognStrataSql <- SqlRender::render(
    sql,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cdmDatabaseSchema = cdmDatabaseSchema,
    targetId = targetId) %>%
    SqlRender::translate(targetDialect = con@dbms)
  
  DatabaseConnector::executeSql(connection = con, cognStrataSql, progressBar = TRUE)
  
  invisible(cognStrataSql)
}



## Master function ----------
buildStrata <- function(con,
                        executionSettings) {
  
  ## Get variables
  cdmDatabaseSchema <- executionSettings$cdmDatabaseSchema
  workDatabaseSchema <- executionSettings$workDatabaseSchema
  cohortTable <- executionSettings$cohortTable
  databaseId <- executionSettings$databaseName
  
  outputFolder <- fs::path(here::here("results", databaseId, "02_buildStrata")) %>%
    fs::dir_create()
  
  
  ## Get cohort ids
  targetCohortsIds <- getCohortManifest() %>% dplyr::select(id)

  
  ## Initial stratas ----------
  
  
  ## Additional stratas ----------
  cli::cat_rule("Building Demographic Stratas")
  
  
  # ### Age ----------
  # purrrObj <- data.frame(ageMin = as.integer(c(0, 60, 65, 70, 75, 80, 85, 90, 95, 100, 105, 110)),
  #                        ageMax = as.integer(c(59, 64, 69, 74, 79, 84, 89, 94, 99, 104, 109, 9999)),
  #                        strataId = as.integer(c(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)))
  # 
  # inputDF <- tidyr::expand_grid(purrrObj, targetCohortsIds)
  # 
  # #debug(ageStrata)
  # purrr::pwalk(inputDF,
  #              ~ageStrata(con = con,
  #                         cohortDatabaseSchema = workDatabaseSchema,
  #                         cohortTable = cohortTable,
  #                         cdmDatabaseSchema = cdmDatabaseSchema,
  #                         targetId = ..4,
  #                         strataId = ..3,
  #                         ageMin = ..1,
  #                         ageMax = ..2))
  # 
  # cli::cat_bullet("Age strata written to table: ", paste0(workDatabaseSchema,".",cohortTable), bullet = "tick", bullet_col = "green")
# 
#   ### Sex ----------
# 
#   #debug(sexStrata)
#   purrr::pwalk(targetCohortsIds,
#                ~sexStrata(con = con,
#                           cohortDatabaseSchema = workDatabaseSchema,
#                           cohortTable = cohortTable,
#                           cdmDatabaseSchema = cdmDatabaseSchema,
#                           targetId = ..1))
# 
#   cli::cat_bullet("Sex strata written to table: ",  paste0(workDatabaseSchema,".",cohortTable), bullet = "tick", bullet_col = "green")
  

  ### Cognitive Status ----------
  
  #debug(cognStrata)
  purrr::pwalk(targetCohortsIds,
               ~ cognStrata(con = con,
                            cohortDatabaseSchema = workDatabaseSchema,
                            cohortTable = cohortTable,
                            cdmDatabaseSchema = cdmDatabaseSchema,
                            targetId = ..1)
  )
  
  cli::cat_bullet("Cognitive Status strata written to table: ",  paste0(workDatabaseSchema,".",cohortTable), bullet = "tick", bullet_col = "green")

  
  ### Fracture type ----------
  
  #debug(fracStrata)
  purrr::pwalk(targetCohortsIds,
               ~ fracStrata(con = con,
                            cohortDatabaseSchema = workDatabaseSchema,
                            cohortTable = cohortTable,
                            cdmDatabaseSchema = cdmDatabaseSchema,
                            targetId = ..1)
  )
  
  cli::cat_bullet("Fracture type strata written to table: ",  paste0(workDatabaseSchema,".",cohortTable), bullet = "tick", bullet_col = "green")
  
  
  sql <- "
      select distinct cohort_definition_id
      from @cohortDatabaseSchema.@cohortTable;
  "
  
  countSql <- SqlRender::render(
    sql,
    cohortDatabaseSchema = workDatabaseSchema,
    cohortTable = cohortTable) %>%
    SqlRender::translate(targetDialect = con@dbms)
  
  counts <- DatabaseConnector::querySql(connection = con, sql = countSql)
  counts
  
  
  countedCohorts <- countCohorts(
    executionSettings = executionSettings,
    con = con,
    cohortManifest = cohortManifest,
    outputFolder = outputFolder
  )

  # verboseSave(
  #   object = dt,
  #   saveName = "strata_table",
  #   saveLocation = outputFolder
  # )
  
  dt<-0
  invisible(dt)
}
