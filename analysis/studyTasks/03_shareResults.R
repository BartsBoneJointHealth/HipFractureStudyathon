# A. Meta Info -----------------------

# Task: Share Results


# B. Dependencies ----------------------

library(tidyverse, quietly = TRUE)
library(readr)
library(here)
source("analysis/private/_utilities.R")


# C. Connection ----------------------

# Set database block
configBlock <- "nhfd"


#bindAndShareResults()

#debug(bindFilesCat)
bindFilesCat(outputPath = here::here("report"),
             database = configBlock,
             filename = "catCov")


#debug(bindFilesCont)
bindFilesCont(outputPath = here::here("report"),
              database = configBlock,
              filename = "contChar")


allCohorts <- readr::read_csv(here::here("results/nhfd/02_buildStrata/allCohorts.csv"),
                              show_col_types = FALSE)

allCohortsF <- allCohorts %>%
  dplyr::mutate(targetCohortId = as.integer(substr(allCohorts$cohortId,1,1)),
                strataCohortId = substr(allCohorts$cohortId,2,3),
                targetCohortName = dplyr::case_when(
                  targetCohortId == 1 ~ "Cohort1",
                  targetCohortId == 2 ~ "Cohort2",
                  targetCohortId == 3 ~ "Cohort3",
                  targetCohortId == 4 ~ "Cohort4"),
                strataCohortName = dplyr::case_when(
                  strataCohortId == "01" ~ "SHS",
                  strataCohortId == "02" ~ "No SHS",
                  strataCohortId == "03" ~ "CHS",
                  strataCohortId == "04" ~ "No CHS",
                  strataCohortId == "05" ~ "THR",
                  strataCohortId == "06" ~ "No THR",
                  strataCohortId == "07" ~ "IM nail",
                  strataCohortId == "08" ~ "No IM nail",
                  strataCohortId == "09" ~ "Hemiarthroplasty",
                  strataCohortId == "10" ~ "No Hemiarthroplasty",
                  strataCohortId == "11" ~ "Operation",
                  strataCohortId == "12" ~ "No Operation",
                  strataCohortId == "13" ~ "Pathology",
                  strataCohortId == "14" ~ "No pathology",
                  strataCohortId == "15" ~ "Malignancy",
                  strataCohortId == "16" ~ "No Malignancy",
                  strataCohortId == "17" ~ "Atypical",
                  strataCohortId == "18" ~ "No Atypical",
                  TRUE ~ ""
                 ),
                fullName = paste0(targetCohortName, ": ", strataCohortName)
                )


readr::write_csv(allCohortsF, file = here::here("misc", "report", "allCohorts.csv"))
