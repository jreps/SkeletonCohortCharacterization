#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Builds SQL to create cohort table
#'
#' @param connectionDetails The connection details to the OMOP CDM
#' @param cohortDatabaseSchema The name of schema where cohort table will be placed
#' @param cohortTable The name of cohort table
#' @param oracleTempSchema The temp schema
#'
#'
createCohortTable <- function(connectionDetails,
                              cohortDatabaseSchema, 
                              cohortTable,
                              oracleTempSchema) {
  
  sqlPath <- system.file("sql", "sql_server", "CreateCohortTable.sql", package = 'SkeletonCohortCharacterization')
  
  createCohortTableSql <- SqlRender::readSql(sqlPath)
  createCohortTableSql <- SqlRender::render(createCohortTableSql, 
                                            cohort_database_schema = cohortDatabaseSchema, 
                                            cohort_table = cohortTable)
  
  createCohortTableSql <- SqlRender::translate(createCohortTableSql, 
                                               targetDialect = connectionDetails$dbms,
                                               oracleTempSchema = oracleTempSchema)
  
  con <- DatabaseConnector::connect(connectionDetails)
  DatabaseConnector::executeSql(con, createCohortTableSql)
  DatabaseConnector::disconnect(con)
  
  return(invisible(NULL))
}

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

#' Drops cohort table
#'
#' @param connectionDetails An object of type \code{connectionDetails} as created using the
#'                             \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                             DatabaseConnector package.
#' @param cohortDatabaseSchema The name of schema where cohort table will be placed
#' @param cohortTable The name of cohort table
#' @param tempSchema The temp schema if needed
#' @export
cleanupCohortTable <- function(connectionDetails, 
                               cohortDatabaseSchema, 
                               cohortTable, 
                               tempSchema = cohortDatabaseSchema) {
  sql <- "IF OBJECT_ID('@cohort_database_schema.@cohort_table', 'U') IS NOT NULL DROP TABLE @cohort_database_schema.@cohort_table;"
  sql <- SqlRender::render(sql, cohort_database_schema = cohortDatabaseSchema, cohort_table = cohortTable)
  sql <- SqlRender::translate(sql, connectionDetails$dbms, tempSchema)
  con <- DatabaseConnector::connect(connectionDetails)
  DatabaseConnector::executeSql(con, sql, runAsBatch = TRUE)
  DatabaseConnector::disconnect(con)
}