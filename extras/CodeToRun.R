library(SkeletonCohortCharacterization)
#=======================
# USER INPUTS
#=======================
# The folder where the study intermediate and result files will be written:
outputFolder <- "./SkeletonCohortCharacterization"
analysisId <- 1
sessionId <- '1'

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
saveSql <- T
createCohorts <- T
customCovariates <- NULL

#=======================

SkeletonCohortCharacterization::runAnalysis(connectionDetails = connectionDetails,
                                            cohortDatabaseSchema = cohortDatabaseSchema,
                                            cohortTable = cohortTable,
                                            sessionId = sessionId, #?
                                            cdmDatabaseSchema = cdmDatabaseSchema,
                                            resultsSchema = resultDatabaseSchema,
                                            vocabularySchema = vocabularySchema, #?
                                            tempSchema = oracleTempSchema,
                                            analysisId = analysisId, #?
                                            outputFolder = outputFolder,
                                            customCovariates = customCovariates,
                                            createCohorts = createCohorts,
                                            saveSql = saveSql)
