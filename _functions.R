
generateCohorts <- function(executionSettings,
                            con,
                            cohortManifest,
                            outputFolder,
                            type = "analysis") {
  
  
  # Prepare cohorts for Cohort Generator
  cohortsToCreate <- prepManifestForCohortGenerator(cohortManifest)
  incrementalFolder <- fs::path(outputFolder)
  
  
  name <- executionSettings$cohortTable
  
  cohortTableNames <- list(cohortTable = paste0(name),
                           cohortInclusionTable = paste0(name, "_inclusion"),
                           cohortInclusionResultTable = paste0(name, "_inclusion_result"),
                           cohortInclusionStatsTable = paste0(name, "_inclusion_stats"),
                           cohortSummaryStatsTable = paste0(name, "_summary_stats"),
                           cohortCensorStatsTable = paste0(name, "_censor_stats"))
  
  # Generate cohorts
  CohortGenerator::generateCohortSet(
    connection = con,
    cdmDatabaseSchema = executionSettings$cdmDatabaseSchema,
    cohortDatabaseSchema =  executionSettings$workDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortsToCreate,
    incremental = TRUE,
    incrementalFolder = incrementalFolder
  )
  
  # Get cohort counts
  cohortCounts <- CohortGenerator::getCohortCounts(
    connection = con,
    cohortDatabaseSchema = executionSettings$workDatabaseSchema,
    cohortTable = cohortTableNames$cohortTable,
    cohortDefinitionSet = cohortsToCreate
  ) %>%
    dplyr::select(cohortId, cohortName, cohortEntries, cohortSubjects)
  
  # Save cohort counts
  tb <- cohortManifest %>%
    dplyr::left_join(cohortCounts %>%
                       dplyr::select(cohortId, cohortEntries, cohortSubjects),
                     by = c("id" = "cohortId")) %>%
    dplyr::rename(
      entries = cohortEntries,
      subjects = cohortSubjects) %>%
    dplyr::select(
      id, name, type, entries, subjects, file
    )
  
  savePath <- fs::path(outputFolder, "cohortManifest.csv")
  readr::write_csv(x = tb, file = savePath)
  cli::cat_bullet("Saving Generated Cohorts to ", crayon::cyan(savePath),
                  bullet = "tick", bullet_col = "green")
  
  return(cohortCounts)
}



prepManifestForCohortGenerator <- function(cohortManifest) {
  
  cohortsToCreate <- cohortManifest %>%
    dplyr::mutate(
      json = purrr::map_chr(file, ~readr::read_file(.x))
    ) %>%
    dplyr::select(id, name, json) %>%
    dplyr::rename(cohortId = id, cohortName = name)
  
  cohortsToCreate$sql <- purrr::map_chr(
    cohortsToCreate$json,
    ~CirceR::buildCohortQuery(CirceR::cohortExpressionFromJson(.x),
                              CirceR::createGenerateOptions(generateStats = TRUE)))
  
  return(cohortsToCreate)
}