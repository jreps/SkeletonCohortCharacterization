Instructions To Run Study
===================
- Execute the study by running the code in (extras/CodeToRun.R) :
```r
  library(SkeletonCohortCharacterization)
  # USER INPUTS
#=======================
# The folder where the study intermediate and result files will be written:
outputFolder <- "C:/SkeletonCohortCharacterizationResults"


# Details for connecting to the server:
dbms <- "you dbms"
user <- 'your username'
pw <- 'your password'
server <- 'your server'
port <- 'your port'

connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                server = server,
                                                                user = user,
                                                                password = pw,
                                                                port = port)

# Add the database containing the OMOP CDM data
cdmDatabaseSchema <- 'cdm database schema'
# Add a database with read/write access as this is where the cohorts will be generated
resultDatabaseSchema <- 'work database schema'

oracleTempSchema <- NULL

vocabularySchema <- 'vocab database schema'

# table name where the cohorts will be generated
cohortDatabaseSchema <- resultDatabaseSchema
cohortTable <- 'SkeletonCohortCharacterizationCohort'

#======================
# PICK OPTIONS
#=======================
sessionId <- '1'
jobId <- NULL
saveSql <- T
createCohorts <- T
customCovariates <- NULL
runCharacterization <- T
viewShiny <- T

#=======================

runAnalysis(connectionDetails = connectionDetails,
                                            cohortDatabaseSchema = cohortDatabaseSchema,
                                            cohortTable = cohortTable,
                                            sessionId = sessionId, #?
                                            cdmDatabaseSchema = cdmDatabaseSchema,
                                            resultsSchema = resultDatabaseSchema,
                                            vocabularySchema = vocabularySchema, #?
                                            tempSchema = oracleTempSchema,
                                            jobId = jobId,
                                            outputFolder = outputFolder,
                                            customCovariates = customCovariates,
                                            createCohorts = createCohorts, 
                                            runCharacterization = runCharacterization,
                                            saveSql = saveSql, 
                                            viewShiny = viewShiny)
```

The 'createCohorts' option will create all the cohorts into cohortDatabaseSchema.cohortTable if set to T.  The 'runCharacterization' option will run the characterization for each cohort if set to T.  The results of each Analysis are saved in the resultSchema.  After running execute with 'runCharacterization' set to T, you can view the results via a shiny app by running execute with viewShiny set to T.