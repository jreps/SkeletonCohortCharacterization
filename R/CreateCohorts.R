# Copyright 2021 Observational Health Data Sciences and Informatics
#
# This file is part of SkeletonCohortCharacterization
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

createAllCohorts <- function(connection =  NULL,
                          connectionDetails,
                          cdmDatabaseSchema,
                          vocabularyDatabaseSchema = cdmDatabaseSchema,
                          cohortDatabaseSchema,
                          cohortTable,
                          oracleTempSchema,
                          outputFolder) {
  
  if(is.null(connection)){
    connection <- DatabaseConnector::connect(connectionDetails)
  }
  
  # if tables do not exist:
  tblNames <- DatabaseConnector::getTableNames(connection = connection, databaseSchema = cohortDatabaseSchema)
  if(length(tblNames)>0){
    if(!tolower(cohortTable)%in%tolower(tblNames)){
      ParallelLogger::logInfo("Creating cohortTable")
      createCohortTable(connection = connection,
                        cohortDatabaseSchema = cohortDatabaseSchema, 
                        cohortTable = cohortTable,
                        oracleTempSchema = oracleTempSchema)
    }
  } else{
    ParallelLogger::logInfo("Creating cohortTable")
    createCohortTable(connection = connection,
                      cohortDatabaseSchema = cohortDatabaseSchema, 
                      cohortTable = cohortTable,
                      oracleTempSchema = oracleTempSchema)
  }
  
 
  # Instantiate cohorts:
  pathToCsv <- system.file("settings", "CohortsToCreate.csv", package = "SkeletonCohortCharacterization")
  cohortsToCreate <- utils::read.csv(pathToCsv)
  for (i in 1:nrow(cohortsToCreate)) {
    writeLines(paste("Creating cohort:", cohortsToCreate$name[i]))
    sql <- SqlRender::loadRenderTranslateSql(sqlFilename = paste0(cohortsToCreate$name[i], ".sql"),
                                             packageName = "SkeletonCohortCharacterization",
                                             dbms = attr(connection, "dbms"),
                                             oracleTempSchema = oracleTempSchema,
                                             cdm_database_schema = cdmDatabaseSchema,
                                             vocabulary_database_schema = vocabularyDatabaseSchema,
                                             
                                             target_database_schema = cohortDatabaseSchema,
                                             target_cohort_table = cohortTable,
                                             target_cohort_id = cohortsToCreate$cohortId[i])
    DatabaseConnector::executeSql(connection, sql)
  }
}

#' Builds SQL to create cohort table
#'
#' @param connection The connection to the OMOP CDM
#' @param cohortDatabaseSchema The name of schema where cohort table will be placed
#' @param cohortTable The name of cohort table
#' @param oracleTempSchema The temp schema
#'
#'
createCohortTable <- function(connection,
                              cohortDatabaseSchema, 
                              cohortTable,
                              oracleTempSchema) {
  
  # Create study cohort table structure:
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "CreateCohortTable.sql",
                                           packageName = "SkeletonCohortCharacterization",
                                           dbms = attr(connection, "dbms"),
                                           oracleTempSchema = oracleTempSchema,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
  return(invisible(NULL))
}


# old code no longer used:
#==========================

#' Builds SQLs to construct cohorts for analysis
#'
#' @param cdmDatabaseSchema The name of schema containing data in CDM format
#' @param vocabularySchema The name of schema with vocabularies
#' @param cohortDatabaseSchema The name of schema where cohorts will be placed
#' @param cohortTable The name of table where cohorts will be placed
#'
getCohortSqls <- function(cdmDatabaseSchema, 
                          vocabularySchema, 
                          cohortDatabaseSchema, 
                          cohortTable) {
  
  studySpec <- system.file("settings", "StudySpecification.json", package = 'SkeletonCohortCharacterization')
  

  cohortExpression <- new(J("org.ohdsi.circe.cohortdefinition.CohortExpression"))
  queryBuilder <- new(J("org.ohdsi.circe.cohortdefinition.CohortExpressionQueryBuilder"))

  cohorts <- fromJSON(studySpec)$cohorts

  dbOptions <- list(
  cdmSchema = cdmDatabaseSchema,
  targetTable = paste(cohortDatabaseSchema, ".", cohortTable, sep = ""),
  resultSchema = cohortDatabaseSchema,
  vocabularySchema = vocabularySchema,
  generateStats = FALSE
  )

  sqls <- c()
  for (c in cohorts) {
    options <- list(dbOptions)

    options[[1]]$cohortId <- c$id

    optionsJSON <- toJSON(options, container = FALSE)[[1]]
    queryExpressionOptions <- queryBuilder$BuildExpressionQueryOptions$fromJson(optionsJSON)

    expressionJSON <- toJSON(c$expression, digits = 32)[[1]]
    expression <- cohortExpression$fromJson(expressionJSON)

    sqls <- c(sqls, queryBuilder$buildExpressionQuery(expression, queryExpressionOptions))
  }

  return(sqls)
}


#' Constructs cohorts used in CC analysis
#'
#' @param connectionDetails An object of type \code{connectionDetails} as created using the
#'                             \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                             DatabaseConnector package.
#' @param cdmDatabaseSchema The schema containing the cdm data  
#' @param vocabularySchema  The schema containing the vocab
#' @param cohortDatabaseSchema The schema that will contain the cohortTable
#' @param cohortTable  The name of the cohortTable that gets created
#' @param tempSchema  The temp schema if needed           
#' @export
generateCohortsFromJson <- function(connectionDetails, 
                          cdmDatabaseSchema, 
                          vocabularySchema, 
                          cohortDatabaseSchema, cohortTable,
                          tempSchema = cohortDatabaseSchema) {

  cohortSqls <- getCohortSqls(cdmDatabaseSchema = cdmDatabaseSchema,
                              vocabularySchema = vocabularySchema,
                              cohortDatabaseSchema = cohortDatabaseSchema,
                              cohortTable = cohortTable
                              )
  
  i <- 1
  for (sql in cohortSqls) {
    renderedSql <- SqlRender::render(sql)
    
    translatedSql <- SqlRender::translate(renderedSql, connectionDetails$dbms, tempSchema)
    
    ParallelLogger::logInfo(paste0("Building cohort ",i))
    con <- DatabaseConnector::connect(connectionDetails)
    DatabaseConnector::executeSql(con, translatedSql, runAsBatch = TRUE)
    DatabaseConnector::disconnect(con)
    i <- 1+1
  }

  return(paste0(cohortDatabaseSchema, '.', cohortTable))
}
