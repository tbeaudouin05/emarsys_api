
run_query_wo_error_bi_f <- function(formatted_query){
  
  db_access <- yaml.load_file('access.yaml')
  start_t <- Sys.time()

  conn <- NULL
  j <- 0
  while (is.null(conn)) {
    
    writeLines('Connecting to BI database...')
  # connect to sc database
  tryCatch({conn <- odbcConnect("BI_replica"
                                ,uid = db_access$bi_access[[1]][[1]]
                                ,pwd = db_access$bi_access[[2]][[1]])}
           ,error = function(cond){conn <- NULL})
    
    # if connection fails more than 10 times then give up
    if (j > 10) {conn <- 1}
  }
  
  i <- 1
  j <- 0
  
  while (i == 1) {
    
    j <- j+1

  writeLines('Running query and handling potential errors, please wait...')
  writeLines('FYI: query takes around 30 minutes')
  writeLines(paste('Start time of query:',Sys.time()))
  
  query_output <- withCallingHandlers(sqlQuery(conn, formatted_query),
                                      warning = function(w){
                                        if(grepl("error while fetching rows", w$message)){
                                          writeLines('Error while fetching rows - fetching rows again...')
                                          
                                        } else if (grepl("server has gone away", w$message)) {
                                          writeLines('Server has gone away (on vacation?) - connecting again...')
                                          
                                        } else {
                                          writeLines('All good on my side - moving on and retrieving data! :)')
                                    
                                        }
                                      })
  
# break the loop when all known errors have been handled 
if(is.data.frame(query_output)){i <- 2}

# if the loop has failed more than 10 times then give up
if(j > 10) {i <- 2}
  
# close all open DB connections
odbcCloseAll()
  
  }
  
  end_t <- Sys.time()
  
  writeLines(paste('Query time:',as.character(round(difftime(end_t,start_t,units = 'mins' ),digits = 0)),'minute(s)'))
  
  return(query_output)
  
}