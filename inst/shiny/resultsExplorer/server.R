prettyHr <- function(x) {
  result <- sprintf("%.2f", x)
  result[is.na(x)] <- "NA"
  result <- suppressWarnings(format(as.numeric(result), big.mark=",")) # add thousands separator
  return(result)
}

addThousandsSeparator<-function(table){
  if(is.data.frame(table)){
    is.num <- sapply(table, is.numeric)
    table[is.num] <- lapply(table[is.num], function(x) format(as.numeric(x), big.mark=","))
    return(table)
  } else {
    is.not.na<- !sapply(suppressWarnings(as.numeric(table)), is.na)
    table[is.not.na] <- format(as.numeric(table[is.not.na]), big.mark=",")
    return(table)
  }
  
}

getHoveroverStyle <- function(left_px, top_px) {
  style <- paste0("position:absolute; z-index:100; background-color: rgba(245, 245, 245, 0.85); ",
                   "left:",
                   left_px - 200,
                   "px; top:",
                   top_px - 130,
                   "px; width:400px;")
}



shinyServer(function(input, output, session) {
  
  
  # Tables
  output$resultTable <- DT::renderDataTable(DT::datatable(allResults,
                                                          
                                                          rownames= FALSE, 
                                                          selection = 'single', 
                                                          filter = 'top',
                                                          extensions = 'Buttons', 
                                                          options = list(
                                                            dom = 'Blfrtip' , 
                                                            buttons = c(I('colvis'), 'copy', 'excel', 'pdf' ),
                                                            scrollX = TRUE
                                                            #pageLength = 100, lengthMenu=c(10, 50, 100,200)
                                                          )##,
                                                          
                                                          ##container = htmltools::withTags(table(
                                                            ##class = 'display',
                                                            ##thead(
                                                            ##  tr(apply(data.frame(colnames=c('Dev', 'Val', 'T','O', 'Model','Covariate setting',
                                                            ##                                 'TAR', 'AUC', 'AUPRC', 
                                                            ##                                 'T Size', 'O Count','Val (%)', 'O Incidence (%)', 'timeStamp'), 
                                                            ##                      labels=c('Database used to develop the model', 'Database used to evaluate model', 'Target population - the patients you want to predict risk for','Outcome - what you want to predict', 
                                                            ##                               'Model type','Id for the covariate/settings used','Time-at-risk period', 'Area under the reciever operating characteristics (test or validation)', 'Area under the precision recall curve (test or validation)',
                                                            ##                               'Target population size in the data', 'Outcome count in the data','The percentage of data used to evaluate the model', 'Percentage of target population that have outcome during time-at-risk','date and time of execution')), 1,
                                                            ##           function(x) th(title=x[2], x[1])))
                                                            ##)
                                                          ##))
                                            ))
 
})

