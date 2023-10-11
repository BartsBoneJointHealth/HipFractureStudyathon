# A. Meta Info -----------------------

# Task: Execution Settings

# B. Functions ------------------------
`%notin%` <- Negate("%in%")

getCohortManifest <- function(inputPath = here::here("cohortsToCreate")) {

  #get cohort file paths
  cohortFiles <- fs::dir_ls(inputPath, recurse = TRUE, type = "file", glob = "*.json")
  #get cohort names
  cohortNames <- fs::path_file(cohortFiles) %>%
    fs::path_ext_remove()
  #get cohort type
  cohortType <- fs::path_dir(cohortFiles) %>%
    basename() %>%
    gsub(".*_", "", .)

  #future addition of hash
  hash <- purrr::map(cohortFiles, ~readr::read_file(.x)) %>%
    purrr::map_chr(~digest::digest(.x, algo = "sha1")) %>%
    unname()

  #return tibble with info
  tb <- tibble::tibble(
    name = cohortNames,
    type = cohortType,
    hash = hash,
    file = cohortFiles %>% as.character()
  ) %>%
    dplyr::mutate(
      id = dplyr::row_number(), .before = 1
    )
  return(tb)
}


startSnowflakeSession <- function(con, executionSettings) {
  sql <- "
  ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT='JSON';
    USE ROLE @user_role;
    USE SECONDARY ROLES ALL;
    USE DATABASE @write_database;
    USE SCHEMA @write_schema;
  "
  crd <- stringr::str_split_1(string = executionSettings$workDatabaseSchema, pattern = "\\.")

  sessionSql <- SqlRender::render(
    sql = sql,
    user_role = executionSettings$role,
    write_database = crd[1],
    write_schema = crd[2]
  ) %>%
    SqlRender::translate(targetDialect = con@dbms)

  DatabaseConnector::executeSql(connection = con, sql = sessionSql)
  cli::cat_line("Setting up Snowflake session")

  invisible(sessionSql)
}


readSettingsFile <- function(settingsFile) {

  tt <- yaml::read_yaml(file = settingsFile)

  # convert cohorts into dataframes
  for (i in seq_along(tt[[1]][[1]])) {
    tt[[1]][[1]][[i]] <- listToTibble(tt[[1]][[1]][[i]])
  }

  #convert unnamed lists into dataframes
  ss <- seq_along(tt[[1]])
  for (j in ss[-1]) {
    check <- is.list(tt[[1]][[j]]) && is.null(names(tt[[1]][[j]]))
    if (check) {
      tt[[1]][[j]] <- listToTibble(tt[[1]][[j]])
    } else {
      next
    }
  }

  return(tt)
}

listToTibble <- function(ll) {
  df <- do.call(rbind.data.frame, ll) |>
    tibble::as_tibble()
  return(df)
}

verboseSave <- function(object, saveName, saveLocation) {

  savePath <- fs::path(saveLocation, saveName, ext = "csv")
  readr::write_csv(object, file = savePath)
  cli::cat_line()
  cli::cat_bullet("Saved file ", crayon::green(basename(savePath)), " to:",
                  bullet = "info", bullet_col = "blue")
  cli::cat_bullet(crayon::cyan(saveLocation), bullet = "pointer", bullet_col = "yellow")
  cli::cat_line()
  invisible(savePath)
}


bindFilesCat <- function(outputPath,
                         database = NULL,
                         filename = NULL)  {
  
  inputPath <- here::here("results", database, "03_postIndex")
  
  ## List all csv files in folder
  filepath <- list.files(inputPath, full.names = TRUE, pattern = filename, recursive = TRUE)
  
  ## Read all files and save in list
  listed_files <- lapply(filepath, 
                         readr::read_csv, 
                         show_col_types = FALSE, 
                         col_types = list(
                           nn = "d",
                           cohortId = "d",
                           conceptId = "d",
                           conceptName = "c",
                           type = "c",
                           year = "d"
                         )
  )

  
  ## Created binded data frame with all data frames of list
  binded_df <- dplyr::bind_rows(listed_files)
  
  cohortManifest <- readr::read_csv(file = here::here("results", database, "02_buildStrata", "allCohorts.csv"), 
                                    show_col_types = FALSE)
  
  binded_df_F <- binded_df %>%
    dplyr::left_join(cohortManifest, by = c("cohortId")) %>%
    dplyr::rename(totalEntries = cohortEntries,
                  totalSubjects = cohortSubjects) %>%
    dplyr::mutate(pct = nn/totalSubjects)
    
  
  ## Create output directory
  fs::dir_create(outputPath)
  
  ## Save output
  readr::write_csv(
    x = binded_df_F,
    file = file.path(outputPath, paste0(filename, "_", database, ".csv")),
    append = FALSE
  )
  
  invisible(binded_df)
}


bindFilesCont <- function(outputPath,
                          database = NULL,
                          filename = NULL)  {
  
  inputPath <- here::here("results", database, "03_postIndex")
  
  ## List all csv files in folder
  filepath <- list.files(inputPath, full.names = TRUE, pattern = filename, recursive = TRUE)
  
  ## Read all files and save in list
  listed_files <- lapply(filepath, 
                         readr::read_csv, 
                         show_col_types = FALSE, 
                         col_types = list(
                           nn = "d",
                           meanvalue = "d",
                           minvalue = "d",
                           maxvalue = "d",
                           sd = "d",
                           cohortId = "d",
                           totalSubjects = "d",
                           totalEntries = "d"
                         )
  )
  
  
  ## Created binded data frame with all data frames of list
  binded_df <- dplyr::bind_rows(listed_files)
  
  ## Create output directory
  fs::dir_create(outputPath)
  
  ## Save output
  readr::write_csv(
    x = binded_df,
    file = file.path(outputPath, paste0(filename, "_", database, ".csv")),
    append = FALSE
  )
  
  invisible(binded_df)
}


bindAndShareResults <- function(
    
    )
  
{
  
  
  
  
}