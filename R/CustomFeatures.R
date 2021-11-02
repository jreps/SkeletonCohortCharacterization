# custom feature code
timeBetweenDrugs <- function(connectionDetails,
                             resultsSchema,
                              generationId,
                              cohortTable,
                              cdmDatabaseSchema,
                             includeConceptParents = c(19117912),
                             excludeConceptParents = c(43730114),
                              startDate = -30, 
                              endDate = 365, 
                             endType = 'start'){
  
  ParallelLogger::logInfo('Adding custom features...')
  
  sql <- "select person_id, row_number() over (partition by person_id order by DRUG_EXPOSURE_START_DATE) as rn,
  datediff(day, DRUG_EXPOSURE_@end_type_DATE, lead(DRUG_EXPOSURE_START_DATE, 1) over (partition by person_id order by DRUG_EXPOSURE_START_DATE)) as time_to_next 
  
  from 
  (select drugs.person_id, 
  drugs.DRUG_EXPOSURE_START_DATE, 
  max(drugs.DRUG_EXPOSURE_END_DATE) DRUG_EXPOSURE_END_DATE 
  
  from @cohort_database_schema.@cohort_table ct inner join 
  (select * from @cdm_database_schema.drug_exposure where drug_concept_id in  
  (select DESCENDANT_CONCEPT_ID 
  from @cdm_database_schema.CONCEPT_ANCESTOR 
  where ANCESTOR_CONCEPT_ID = 19117912
  and DESCENDANT_CONCEPT_ID not in (select DESCENDANT_CONCEPT_ID from @cdm_database_schema.CONCEPT_ANCESTOR where ANCESTOR_CONCEPT_ID = 43730114) 
  )) drugs 
  on ct.subject_id = drugs.person_id and 
  drugs.DRUG_EXPOSURE_@end_type_DATE >= dateadd(day, @start_date, ct.cohort_start_date) and 
  drugs.DRUG_EXPOSURE_START_DATE <= dateadd(day, @end_date, ct.cohort_start_date) 
  GROUP BY drugs.person_id, drugs.DRUG_EXPOSURE_START_DATE) valid_drugs;"
  
  sql <- SqlRender::render(sql, 
                           cohort_database_schema = resultsSchema,
                           cohort_table = cohortTable,
                           cdm_database_schema = cdmDatabaseSchema,
                           start_date = startDate,
                           includes = paste0(includeConceptParents, collapse = ',', sep = ','),
                           excludes = paste0(excludeConceptParents, collapse = ',', sep = ','),
                           end_date = endDate,
                           end_type = endType)
  
  sql <- SqlRender::translate(sql, targetDialect = connectionDetails$dbms)
  con <- DatabaseConnector::connect(connectionDetails)
  results <- DatabaseConnector::querySql(con, sql)
  DatabaseConnector::disconnect(con)
  
  resultsAll <- results %>% dplyr::filter(!is.na(.data$TIME_TO_NEXT)) %>%
    dplyr::summarise(type = "'DISTRIBUTION'",
                     fa_type = "'CUSTOM'",
                     cc_generation_id = .data$generationId,
                     concept_id = 0,
                     analysis_id = 879,
                     analysis_name = "'Custom time to next drug'",
                     count = length(.data$TIME_TO_NEXT),
                     avg_value = mean(.data$TIME_TO_NEXT),
                  stdev_value = stats::sd(.data$TIME_TO_NEXT),
                  min_value = min(.data$TIME_TO_NEXT),
                  p10_value = stats::quantile(.data$TIME_TO_NEXT, 0.1),
                  p25_value = stats::quantile(.data$TIME_TO_NEXT, 0.25),
                  median_value = stats::quantile(.data$TIME_TO_NEXT, 0.5),
                  p75_value = stats::quantile(.data$TIME_TO_NEXT, 0.75),
                  p90_value = stats::quantile(.data$TIME_TO_NEXT, 0.90),
                  max_value = max(.data$TIME_TO_NEXT)
                  )
  
  
  resultsSecond <- results %>% dplyr::filter(.data$RN == 1 & !is.na(.data$TIME_TO_NEXT)) %>%
    dplyr::summarise(type = "'DISTRIBUTION'",
                     fa_type = "'CUSTOM'",
                     cc_generation_id = .data$generationId,
                     concept_id = 0,
                     analysis_id = 880,
                     analysis_name = "'Custom time to second drug'",
                     count_value = length(.data$TIME_TO_NEXT),
                     avg_value = mean(.data$TIME_TO_NEXT),
                     stdev_value = stats::sd(.data$TIME_TO_NEXT),
                     min_value = min(.data$TIME_TO_NEXT),
                     p10_value = stats::quantile(.data$TIME_TO_NEXT, 0.1),
                     p25_value = stats::quantile(.data$TIME_TO_NEXT, 0.25),
                     median_value = stats::quantile(.data$TIME_TO_NEXT, 0.5),
                     p75_value = stats::quantile(.data$TIME_TO_NEXT, 0.75),
                     p90_value = stats::quantile(.data$TIME_TO_NEXT, 0.90),
                     max_value = max(.data$TIME_TO_NEXT)
    )
  
  # add these into resultsSchema.cc_results:
  ParallelLogger::logInfo('Loading custom features into resultsSchema')
  
  con <- DatabaseConnector::connect(connectionDetails)
  sql <- 'DELETE from @result_database_schema.cc_results where ANALYSIS_ID = @analysis_id
          and CC_GENERATION_id = @generation_id;'
  sql <- SqlRender::render(sql, 
                           result_database_schema = resultsSchema,
                           analysis_id = 879,
                           generation_id = generationId)
  sql <- SqlRender::translate(sql, targetDialect = connectionDetails$dbms)
  DatabaseConnector::executeSql(con, sql)
  

  sql <- 'insert into @result_database_schema.cc_results(TYPE,FA_TYPE, CC_GENERATION_id,
    CONCEPT_ID, ANALYSIS_ID, ANALYSIS_NAME, COUNT_VALUE, AVG_VALUE, STDEV_VALUE,
   MIN_VALUE, P10_VALUE, P25_VALUE, MEDIAN_VALUE, P75_VALUE, P90_VALUE, MAX_VALUE) VALUES 
  (@values)'
  sql <- SqlRender::render(sql, 
                           result_database_schema = resultsSchema,
                           values = paste(c(resultsAll[1:10], as.double(resultsAll[11:16])), collapse=',')
                           )
  sql <- SqlRender::translate(sql, targetDialect = connectionDetails$dbms)
  DatabaseConnector::executeSql(con, sql)
  
  
  # second var
  sql <- 'DELETE from @result_database_schema.cc_results where ANALYSIS_ID = @analysis_id
          and CC_GENERATION_id = @generation_id;'
  sql <- SqlRender::render(sql, 
                           result_database_schema = resultsSchema,
                           analysis_id = 880,
                           generation_id = generationId)
  sql <- SqlRender::translate(sql, targetDialect = connectionDetails$dbms)
  DatabaseConnector::executeSql(con, sql)
  
  
  sql <- 'insert into @result_database_schema.cc_results(TYPE,FA_TYPE, CC_GENERATION_id,
    CONCEPT_ID, ANALYSIS_ID, ANALYSIS_NAME, COUNT_VALUE, AVG_VALUE, STDEV_VALUE,
   MIN_VALUE, P10_VALUE, P25_VALUE, MEDIAN_VALUE, P75_VALUE, P90_VALUE, MAX_VALUE) VALUES 
  (@values)'
  sql <- SqlRender::render(sql, 
                           result_database_schema = resultsSchema,
                           values = paste(c(resultsSecond[1:10], as.double(resultsSecond[11:16])), collapse=',')
  )
  sql <- SqlRender::translate(sql, targetDialect = connectionDetails$dbms)
  DatabaseConnector::executeSql(con, sql)
  
  DatabaseConnector::disconnect(con)
  
  
}


addSettings <- function(x,
                        connectionDetails,
                        resultsSchema,
                        generationId,
                        cohortTable,
                        cdmDatabaseSchema){
  
  x$settings$connectionDetails <- connectionDetails
  x$settings$resultsSchema <- resultsSchema
  x$settings$generationId <- generationId
  x$settings$cohortTable <- cohortTable
  x$settings$cdmDatabaseSchema <- cdmDatabaseSchema
  
  return(x)
}
