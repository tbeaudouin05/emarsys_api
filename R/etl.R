library(RODBC)
library(rjson)
library(yaml)

Sys.time()

# run sql query ---------------------------------------------------------------------------------

# fetch the text of the query 
unformatted_query <- readLines('SQL/emarsys_query.txt')

# format query to be one string readable in R
# THERE SHOULD BE NO COMMENTS IN THE ORIGINAL QUERY!
formatted_query <- gsub("\t","", paste(unformatted_query, collapse=" "))

# create connection to BI database (ids should be in Yaml file)
db_access <- yaml.load_file('access.yaml')
conn <- odbcConnect("BI_replica", uid = db_access$bi_access[[1]][[1]], pwd = db_access$bi_access[[2]][[1]])

# run the query on SQL Server database
query_output <- sqlQuery(conn, formatted_query)

Sys.time()

# feed api 1000 rows at a time ---------------------------------------------------------------------

# initialize total_row with query output
total_row <- query_output

while (nrow(total_row)>0) {
  
  # take top 1000 rows of total_row
  row_to_feed <- head(total_row,1000)
  
  # replace NA of dates by 0000-00-00  
  row_to_feed[,c('18125', '18138','18131','18135','18136','18139','18140','18142','18144','18149','18152')] <- 
    apply(row_to_feed[,c('18125', '18138','18131','18135','18136','18139','18140','18142','18144','18149','18152')]
          , 2, function(x){replace(x, is.na(x), '0000-00-00')})
    
  
  # format rows in JSON format
  JSON_to_feed <- toJSON(unname(split(row_to_feed, 1:nrow(row_to_feed))))
  
  # add JSON headers and format exactly for upload
  JSON_to_feed <- paste0('{ "key_id": "4819", "contacts": ',JSON_to_feed,' }')
  
  # replace NA left by ''
  JSON_to_feed <- gsub('NA','null',JSON_to_feed)
  
  # save JSON to disk
  writeLines(JSON_to_feed,'temp/json_to_feed.txt')
  
  # create flag_row_fed to flag rows already fed in total_row
  flag_row_fed <- data.frame('4819' = row_to_feed$`4819`
                             ,'flag' = 1)
  # erase the bloody X which appears in name of column
  names(flag_row_fed)[1] <- sub('X','',names(flag_row_fed)[1])
  
  # flag rows already fed in total_row
  total_row <- merge(total_row
                     ,flag_row_fed
                     ,by = '4819'
                     ,all.x = T)
  
  # filter out rows already fed from total_row
  total_row <- total_row[is.na(total_row$flag),]
  
  # erase flag
  total_row$flag <- NULL
  
  Sys.sleep(1)
  
  # run nodejs etl.js to upload rows to Emarsys
  shell.exec('etl_nodejs_launcher.bat')
  
  # wait until nodejs finished uploading rows 
  # NB: nodejs will erase temp/json_to_feed.txt once it is finished
  # loop makes sure R does not crash in case nodejs is accessing json_to_feed.txt exactly at the same time when R is
  i <- 1
  start_time <- Sys.time()
  while (i <- 1) {
    
    # wait 2 second NB: this time should be different from nodejs loop time! (nodejs = 1 second)
    Sys.sleep(2)
    # check if json_to_feed.txt still exists
    file_exist_test <- tryCatch({
    test <- file.exists('temp/json_to_feed.txt')
    return(test)}
             ,error = function(test){
             test<- T
             return(test)})
    
    # if nodejs has erased the file, then stop waiting and perform next loop
    if (file_exist_test == F) { i <- 0 }
    
    # if the loop runs more than 10 times then stop the program
    loop_minute_spent <- as.numeric(difftime(Sys.time(), start_time, units ="mins"))
    if (loop_minute_spent > 10) { stop('ETL took more than 10 minutes to upload 1000 rows to Emarsys: program was stopped! ERROR!')}
    }
}
