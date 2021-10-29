library(dplyr)

getResults <- function(con, mySchema, jobId){
  
  sql <- "select * from @results_database_schema.cc_results r where r.cc_generation_id = @cohort_characterization_generation_id"
  
  sql <- SqlRender::render(sql, 
                           results_database_schema = mySchema, 
                           cohort_characterization_generation_id = jobId)
  
  sql <- SqlRender::translate(sql = sql, 
                              targetDialect = attr(con, "dbms"))
  
  results <- DatabaseConnector::querySql(con, sql, snakeCaseToCamelCase = T)
  
  
  results <-results[,!colnames(results) %in% c('ccGenerationId')]
  
  results$type <- as.factor(results$type)
  results$faType  <- as.factor(results$faType)
  results$analysisName <- as.factor(results$faType)
  
  return(results)
}

# EDIT FOR REPO OR DATABASE
connectionDetails <- .GlobalEnv$shinySettings$connectionDetails
jobId <- .GlobalEnv$shinySettings$jobId
resultsSchema <- .GlobalEnv$shinySettings$resultsSchema


ParallelLogger::logInfo('Connecting to database')
con <- DatabaseConnector::connect(connectionDetails)

ParallelLogger::logInfo('Extracting results from database')
allResults <- getResults(con = con, 
                         mySchema = resultsSchema, 
                         jobId = jobId)


onStop(function() {
  DatabaseConnector::disconnect(con)
})



