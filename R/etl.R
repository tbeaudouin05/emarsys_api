library(RODBC)
library(rjson)
library(yaml)

source('R/function/run_query_wo_error_bi.R')

# run sql query ---------------------------------------------------------------------------------

start_t <- Sys.time()

# fetch the text of the query 
unformatted_query <- readLines('SQL/emarsys_query.txt')

# format query to be one string readable in R
# THERE SHOULD BE NO COMMENTS IN THE ORIGINAL QUERY!
formatted_query <- gsub("\t","", paste(unformatted_query, collapse=" "))

# run the query on SQL Server database
query_output <- run_query_wo_error_bi_f(formatted_query)


# feed api 1000 rows at a time ---------------------------------------------------------------------

# initialize total_row with query output
total_row <- query_output
i <- 1

start_t_loop <- Sys.time()

while (nrow(total_row)>0) {
  
  nrow_processed <- i * 1000
  writeLines(paste('Formatting rows in JSON to upload via Nodejs API. Time:',  Sys.time()))
  writeLines(paste('Number of rows processed so far:',nrow_processed))
  
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
  
  # make sure temp folder exists otherwise create it
  dir.create(file.path(getwd(), 'nodejs/temp'),showWarnings = FALSE)
  
  # save JSON to disk
  writeLines(JSON_to_feed,'nodejs/temp/json_to_feed.txt')
  
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
  
  writeLines(paste('Starting Nodejs process. Time:',Sys.time()))

  # run nodejs etl.js to upload rows to Emarsys
  shell.exec(paste0(getwd(),'/nodejs/etl_nodejs_launcher.bat'))
  
  # wait until nodejs finishes uploading JSON 
  # FYI: when nodejs finishes, it erases json_to_feed.txt
  while (file.exists('nodejs/temp/json_to_feed.txt')) {Sys.sleep(2)}
  
  writeLines(paste('Nodejs process finished, starting another loop. Time:',Sys.time()))
  
  i <- i+1
}

end_t <- Sys.time()

writeLines(paste('Total process time:',as.character(round(difftime(end_t,start_t,units = 'mins' ),digits = 0)),'minute(s)'))
writeLines(paste('Loop process time:',as.character(round(difftime(end_t,start_t_loop,units = 'mins' ),digits = 0)),'minute(s)'))
