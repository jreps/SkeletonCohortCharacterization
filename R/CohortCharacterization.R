#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Builds query, run analyses and save results to the result folder.
#' 
#' @param connectionDetails An object of type \code{connectionDetails} as created using the
#'                             \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                             DatabaseConnector package.
#' @param sessionId session identifier using to build temporary tables
#' @param cdmDatabaseSchema the name of schema containing data in CDM format
#' @param cohortDatabaseSchema  The name of the schema containing the cohort table
#' @param cohortTable The name of table with cohorts
#' @param resultsSchema the name of schema where results would be placed
#' @param vocabularySchema the name of schema with vocabularies
#' @param tempSchema the name of database temp schema
#' @param jobId analysis identifier
#' @param outputFolder The location to save the results to
#' @param customCovariates A list of lists with objects: function (a string) and settings and list of inputs to the function
#' @param createCohorts Whether to run the code to create the cohort
#' @param runCharacterization Whether to run the code to charcterize the cohorts
#' @param saveSql Whether to save the sql used into the outputFolder
#' @param saveCvsResults Whether to save the results as csv files
#' @param viewShiny Whether to view the results in a shiny app
#' 
#' @export
runAnalysis <- function(connectionDetails,
                  sessionId, # how to define this?
                  cdmDatabaseSchema,
                  cohortDatabaseSchema,
                  cohortTable = "cohort",
                  resultsSchema,
                  vocabularySchema,
                  tempSchema = resultsSchema,
                  jobId = NULL,
                  outputFolder = "SkeletonCohortCharacterization",
                  customCovariates = NULL,
                  createCohorts = T,
                  runCharacterization =T,
                  saveSql = T,
                  saveCvsResults = T,
                  viewShiny = T
) {
  if (!file.exists(outputFolder))
    dir.create(outputFolder, recursive = TRUE)
  ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))
  
  if(is.null(jobId)){
    ParallelLogger::logInfo('Extracting jobId from json')
    filename <- system.file("settings", "StudySpecification.json", package = "SkeletonCohortCharacterization")
    jobId <- jsonlite::fromJSON(filename)$generationId
    ParallelLogger::logInfo(paste0('jobId is ', jobId))
  } else{
    ParallelLogger::logInfo(paste0('Using input jobId of ', jobId))
  }
  
  
  con <- DatabaseConnector::connect(connectionDetails)
  
  
  if(createCohorts){
    
    # new code
    createAllCohorts(connection =  con,
                  connectionDetails = NULL,
                  cdmDatabaseSchema = cdmDatabaseSchema,
                  vocabularyDatabaseSchema = cdmDatabaseSchema,
                  cohortDatabaseSchema = cohortDatabaseSchema,
                  cohortTable = cohortTable,
                  oracleTempSchema = tempSchema,
                  outputFolder = outputFolder)
    
  } # end cohort creation
  
  
  if(runCharacterization){
    
    # create results table is not exists
    tblNames <- DatabaseConnector::getTableNames(connection = con, databaseSchema = resultsSchema)
    if(length(tblNames)>0){
      if(!'cc_results'%in%tolower(tblNames)){
        ParallelLogger::logInfo(paste0('Creating cc_results table in ', resultsSchema))
        sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "CreateResultTable.sql",
                                                 packageName = "SkeletonCohortCharacterization",
                                                 dbms = attr(con, "dbms"),
                                                 oracleTempSchema = tempSchema,
                                                 results_database_schema = resultsSchema)
        DatabaseConnector::executeSql(con, sql, progressBar = FALSE, reportOverallTime = FALSE)
      }
    }
    
    filename <- system.file("settings", "StudySpecification.json", package = "SkeletonCohortCharacterization")
    if(file.exists(filename)){
      ##cohortCharacterization <- read_file(filename)  # testing line below
      cohortCharacterization <- readChar(filename, file.info(filename)$size)
      
      ParallelLogger::logInfo("Building Cohort Characterization queries to run")
      
      tempSchema <- ifelse(is.null(tempSchema),resultsSchema,tempSchema)
      
      sql <- buildQuery(cohortCharacterization, paste(cohortDatabaseSchema,cohortTable, sep='.'), sessionId, cdmDatabaseSchema, resultsSchema, vocabularySchema, tempSchema, jobId)
      dbms <- connectionDetails$dbms
      ParallelLogger::logInfo(paste("Translate SQL for", dbms))
      translatedSql <- SqlRender::translate(sql = sql, 
                                            targetDialect = dbms, 
                                            oracleTempSchema = tempSchema)
      
      if(saveSql){
        if(!dir.exists(paste0(outputFolder,"/tmp"))){
          dir.create(paste0(outputFolder,"/tmp"), recursive = T)
        }
        sqlFile <- paste0(outputFolder,"/tmp/sql-cc-", dbms, "-", jobId, ".sql")
        ParallelLogger::logInfo("Saving sql to: ", sqlFile )
        writeLines(translatedSql, sqlFile)
      }
      
      ParallelLogger::logInfo("Running analysis")
      sql <- SqlRender::render("DELETE FROM @results_database_schema.cc_results WHERE cc_generation_id = @generation_id", 
                               results_database_schema = resultsSchema,  
                               generation_id = jobId)
      deleteSql <- SqlRender::translate(sql, dbms, tempSchema)
      DatabaseConnector::executeSql(con, deleteSql)
      
      DatabaseConnector::executeSql(con, translatedSql, runAsBatch = TRUE)
      DatabaseConnector::disconnect(con)
      
      if(!is.null(customCovariates)){
        # add custom covariate bit here...
        customCovariates <- lapply(customCovariates, 
                                   function(x){addSettings(x, 
                                                           connectionDetails,
                                                           resultsSchema,
                                                           jobId,
                                                           cohortTable,
                                                           cdmDatabaseSchema
                                   )})
        
        lapply(customCovariates, 
               function(x){do.call(x$funct, x$settings)})
        
      }
      
      
      if(saveCvsResults){
        ParallelLogger::logInfo("Collecting results")
        saveResults(connectionDetails, cohortCharacterization, jobId, resultsSchema, file.path(outputFolder,'results'))
      }
      
    } else{
      ParallelLogger::logInfo("Missing settings json")
    }
    
  }
    
  if(viewShiny){
    viewShinyChar(connectionDetails, jobId, resultsSchema)
  }

  invisible(NULL)
}