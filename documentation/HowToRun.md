# How to Run Hip Fracture study

## Table of Contents

1.  Technical Requirements
2.  Setup the Hip Fracture study locally
3.  Setup Credentials
4.  Execute Scripts

## 1. Technical Requirements

To run this study you must setup the following software: 

-   [R](https://cloud.r-project.org/) (version 4.2.3 or greater)
-   [R Studio](https://posit.co/download/rstudio-desktop/)
-   On Windows: [RTools](https://cran.r-project.org/bin/windows/Rtools/)
-   [Java](https://www.java.com/en/)

You also require your site data to be mapped to the [OMOP CDM](https://ohdsi.github.io/CommonDataModel/) and administered on one of the following supported database platforms:

-   Microsoft SQL Server
-   Oracle
-   PostgreSQL
-   Google BigQuery
-   Amazon RedShift
-   Snowflake
-   DuckDb
-   SQLite
-   DatBricks


## 2. Setup the Hip Fracture study locally

### Get Study Package

1)  Go to URL: <https://github.com/BartsBoneJointHealth/HipFractureStudyathon>
2)  Select the green code button named `Code`, revealing a dropdown menu
3)  Select `Download Zip`
4)  Unzip the folder in a location on your local computer that is easily accessible by R Studio
5)  Open the unzipped folder and click on the `HipFractureStudyathon.Rproj` file to open the study package in R Studio


### Final steps
To run the study package user must install the following packages:

``` r
install.packages('usethis')
install.packages('remotes')
remotes::install_github("ohdsi/Ulysses", ref = "develop")
```

## 3. Setup Credentials

Have your database credentials ready, preferably in a text file.

The credentials that you will need are the following:

1)  **Database dialect**: The database SQL dialect e.g. redshift
2)  **Username**: The user's username
3)  **Password**: The user's password
4)  **Connection string**: The JDBC connection string for the database
5)  **CDM schema**: The name of the database and schema where OMOP data are located e.g. SYNPUF.CDM
6)  **Vocabulary schema**: The name of the database and schema where the vocabulary tables are located (usually the same as CDM schema) e.g. SYNPUF.CDM
7)  **Work schema**: The name of the database and schema where study results will be saved e.g. OHDSI.SCRATCH_GA (Note that you will need to have write permissions on this schema)

If you don't know what your credentials are please contact your database administrator. Once you have your credentials ready, open file `extras/KeyringSetup.R`.


**1. Load Dependencies**

Run section `A. Dependencies` to load the packages. If they are not already installed install them using `install.packages` for CRAN packages or using the installation directions for the `Ulysses` package provided in section `Final Steps` above.


**2. Assign Variables**

Run section `B. Assign Variables`. The following variables will be created:

1) **configBlock**: Shorthand reference of the database. This should be a single string without spaces e.g. synpuf
2) **database**:    The database name. This should be a single string without spaces  e.g. synpuf
3) **keyringName**: The name of the keyring where the credentials are going to be saved. No need to fill in. This variable will be pre-registered.
4) **keyringPassword**: The password of the keyring. **Keep this password handy as you will need it when accessing the keyring for the study.**

The keyring is the group that stores the credentials that pertain to the study. More on that later.


**3. Create file config.yml**

Once variables have been assigned, you can now setup the `config.yml` file.
Run section `C. Set config.yml` to check if the config.yml file exists. If it does not already exist run the following function to create:

``` r
Ulysses::makeConfig(block = configBlock, database = database)
```

Variables `configBlock` and `database` are defined in section `B. Assign Variables`. Running this function will open the `config.yml` file in Rstudio.


If the `config.yml` already exists open it and review. Make sure the block contains the following structure, where [block] is the value of the `configBlock` variable:

  ```
# Config block example

[block]:
databaseName: [database]
cohortTable: ehden_hmb_[database]
dbms: !expr keyring::key_get('[block]_dbms', keyring = 'ehden_hmb')
user: !expr keyring::key_get('[block]_user', keyring = 'ehden_hmb')
password: !expr keyring::key_get('[block]_password', keyring = 'ehden_hmb')
connectionString: !expr keyring::key_get('[block]_connectionString', keyring = 'ehden_hmb')
cdmDatabaseSchema: !expr keyring::key_get('[block]_cdmDatabaseSchema', keyring = 'ehden_hmb')
vocabDatabaseSchema: !expr keyring::key_get('[block]_vocabDatabaseSchema', keyring = 'ehden_hmb')
workDatabaseSchema: !expr keyring::key_get('[block]_workDatabaseSchema', keyring = 'ehden_hmb')
```

**4. Setup keyring**

Next we need to setup the keyring for the  study. Run section `D. Setup Keyring`.
The function checks if the keyring for the study already exists. If it does, it is being deleted and recreated.


**5. Set Credentials**

Once we have setup the keyring we can add credentials in the keyring. Run section `E. Set Credentials`.
You will be prompted to place your credentials. Type in the credentials into the dialog box as they appear.


**6. Check credentials**

Once you have finished adding the credentials run function `checkCreds` in section `F. Check credentials` to check if they are correct.
If there is a mistake, uncomment and run function `setSingleCred` for each incorrect credential.
As one final check, run function `testCreds` to ensure that the connection details have been established successfully.
If they have, you should see the name of the `dbms` variable i.e. database dialect, in your console.


**7. Adding more databases**

With one set of credentials confirmed we must do the same for the remaining databases in the study. To add another config block to the `config.yml` file run the following function:

  ```
Ulysses::addConfig(block = "[next_configBlock]", database = "[next_database]")
```

Replace the bracketed variables with the next database. This function will open the `config.yml` file with a new config block added at the bottom of the script.

We now need to add the credentials of this configBlock to the keyring. Reassign variables `configBlock` and `database` in section `B) Set Parameters` of the `KeyringSetup.R` file to the new block and database. Next, rerun section `E) Set Credentials` to set the credentials in the keyring for your other databases. Optionally, rerun section `F) Check Credentials` to check the credentials and test connection to the database as demonstrated above. Rerun these steps until all databases are added to the `config.yml` and keyring.

Lastly, run section `G. Session Info` to clear your R session. You are now ready to execute the study package.


### Support

If you encounter any issues, please contact [george.argyriou\@odysseusinc.com](mailto:george.argyriou@odysseusinc.com){.email}.
