# Create Analysis settings ----------------

# A. File Info -----------------------

# Study: HIPSTER
# Name: Analysis Settings
# Date: 2023-09-14
# Description: Build the analysis settings for the study

# B. Dependencies ----------------------

## include R libraries
library(tidyverse, quietly = TRUE)
library(DatabaseConnector)
library(yaml)

source("analysis/private/_utilities.R")

configBlock <- "cprdGold"

### Administrative Variables
executionSettings <- config::get(config = configBlock) %>%
  purrr::discard_at(c("dbms", "user", "password", "connectionString"))

# C. Script --------------------

cohortManifest <- getCohortManifest()


## Get all target cohorts for analysis

targetCohorts <- cohortManifest %>%
  dplyr::filter(type == "target") %>%
  dplyr::mutate(id = as.integer(id)) %>%
  dplyr::select(name, id)


## 1. Build Strata settings -------------------
#strataCohorts <- ""


ll <- list(
  'strata' = list(
    'cohorts' = list(
      'targetCohort' = targetCohorts,
      'strataCohorts' = strataCohorts
    ),
    'demographics' = tibble::tibble(
      strataId = 1L:4L,
      strataName = c("age 18 to 39",
                     "age 40 to 49",
                     "age 50 to 59",
                     "age 60 to 65")
    ),
    'outputFolder' = fs::path("02_buildStrata")
  )
)

#write_yaml(ll, file = here::here("analysis/settings/strata.yml"), column.major = FALSE)


## Make the strata manifest ----------------

demoStrata <- ll$strata$demographics


tb1 <- expand_grid(targetCohorts, demoStrata) %>%
  dplyr::mutate(
    strataId = id * 1000 + strataId,
    strataName = paste(name, strataName)
  ) %>%
  select(strataId, strataName)


cohortStrataTbl <- tibble::tibble(
  strataId = c(paste0("000", strataCohorts$id), paste0("001", strataCohorts$id)),
  strataName = c(paste("without", strataCohorts$name), paste("with", strataCohorts$name))
)

tb2 <- tidyr::expand_grid(targetCohorts, cohortStrataTbl) %>%
  dplyr::mutate(
    strataId = as.integer(paste0(as.character(id), strataId)),
    strataName = paste(name, strataName)
  )

`%notin%` <- Negate("%in%")

strataKey <- dplyr::bind_rows(
  tb1,
  tb2 %>% select(strataId, strataName)
) %>%
  dplyr::rename(
    id = strataId,
    name = strataName
  ) %>%
  dplyr::filter(
    id %notin% c(1001, 2001, 2004, 3001, 3004, 5001, 5004, 6001, 6004)
  )

allCohorts <- dplyr::bind_rows(
  targetCohorts, strataKey
) %>%
  mutate(
    id = as.integer(id)
  )

## 2. Build settings for baseline characteristics------------------

covariateCohorts <- cohortManifest %>%
  dplyr::filter(type == "covariates") %>%
  dplyr::mutate(id = as.integer(id)) %>%
  dplyr::select(name, id)


ll <- list(
  'baselineCharacteristics' = list(
    'cohorts' = list(
      'targetCohort' = allCohorts,
      'covariateCohorts' = covariateCohorts
    ),
    'timeWindow' = tibble::tibble(startDay = -365L, endDay = -1L),
    'outputFolder' = fs::path("03_baselineCharacteristics")
  )
)

##write_yaml(ll, file = here::here("analysis/settings/baselineCharacteristics.yml"), column.major = FALSE)



## 3. build settings for postIndexUtilization----------------------

drugCohorts <- cohortManifest %>%
  dplyr::filter(type %in% c("class3", "ingredient")) %>%
  dplyr::mutate(id = as.integer(id)) %>%
  dplyr::select(name, id)


ll <- list(
  'postIndexPrevalence' = list(
    'cohorts' = list(
      'targetCohort' = allCohorts,
      'drugCohorts' = drugCohorts
    ),
    'prevalenceTimeWindow' = tibble::tibble(
      startDays = c(0, 184, 366, 731),
      endDays = c(183, 365, 730, 1825)
    ),
    'outputFolder' = fs::path("04_postIndexPrevalence")
  )
)

#write_yaml(ll, file = here::here("analysis/settings/postIndexPrevalence.yml"), column.major = FALSE)



## 4. build settings for incidence analysis---------------------------

outcomeCohorts <- cohortManifest %>%
  #dplyr::filter(id %in% c(8)) %>%
  dplyr::filter(type == "outcome" | id %in% c(8,9,12,13)) %>%
  dplyr::mutate(id = as.integer(id)) %>%
  dplyr::select(name, id)

#outcomeCohorts <- outcomeCohorts %>% dplyr::filter(id == 54)

ll <- list(
  'incidenceAnalysis' = list(
    'cohorts' = list(
      'denominator' = allCohorts,
      'outcomes' = outcomeCohorts
    ),
    'incidenceSettings' = list(
      'cleanWindow' = 0L,
      'startWith' = 'start',
      'startOffset' = c(0L, 184L, 366L, 731L),
      'endsWith' = 'start',
      'endOffset' = c(183L, 365L, 730L, 1825L)
    ),
    # 'incidenceSettings' = list(
    #   'cleanWindow' = 0L,
    #   'startWith' = 'start',
    #   'startOffset' = c(0L),
    #   'endsWith' = 'start',
    #   'endOffset' = c(183L)
    # ),
    'outputFolder' = fs::path("05_incidenceAnalysis")
  )
)

#write_yaml(ll, file = here::here("analysis/settings/incidenceAnalysis_vms.yml"), column.major = FALSE)


## 5. Treatment Pattern Analysis -------------------

`%notin%` <- Negate("%in%")

txCohorts <- cohortManifest %>%
  dplyr::filter(type %notin% c("studyPop", "covariates", "outcome")) %>%
  dplyr::mutate(id = as.integer(id),
                class = dplyr::case_when(
                  name %in% c("anticonvulsants_lvl3", "antidepressants_lvl3",
                              "antihypertensives_lvl3", "benzodiazepines_lvl3",
                              "hormoneTherapy_lvl3") ~ "class3",
                  TRUE ~ "ingredient"
                ),
                era = c(rep(30L, 22), rep(60L, 22), rep(90L, 22), rep(180L, 22))) %>%
  dplyr::select(name, id, class, era)

allCohorts <- allCohorts %>%
  # dplyr::filter(
  #   id %notin% c(1001L, 2001L, 2004L, 3001L, 3004L, 5001L, 5004L, 6001L, 6004L)
  # ) %>%
  dplyr::mutate(
    idx = as.integer(substr(id,1,1)),
    name = snakecase::to_lower_camel_case(name)
  )

ll <- list(
  'treatmentLandscape' = list(
    'cohorts' = list(
      'targetCohorts' = targetCohorts,
      'txCohorts' = txCohorts,
      'strataCohorts' = allCohorts
    ),
    'treatmentHistorySettings' = list(
      minEraDuration = 0L,
      eraCollapseSize = c(30L, 60L, 90L, 180L),
      combinationWindow = 30L,
      minPostCombinationDuration = 30L,
      filterTreatments = "Changes",
      periodPriorToIndex = 0L,
      includeTreatments = "startDate",
      maxPathLength = 5L,
      minCellCount = 30L,
      minCellMethod = "Remove",
      groupCombinations = 10L,
      addNoPaths = FALSE
    ),
    'outputFolder' = list(
      fs::path("06_treatmentHistory"),
      fs::path("07_treatmentPatterns"),
      fs::path("08_timeToEvent")
    )
  )
)

write_yaml(ll, file = here::here("analysis/settings/treatmentPatterns_analysis1.yml"), column.major = FALSE)
