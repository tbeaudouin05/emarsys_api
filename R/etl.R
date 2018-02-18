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
  
  # run nodejs etl.js to upload rows to Emarsys
  shell.exec('nodejs/etl_nodejs_launcher.bat')
  
  # wait until nodejs finished uploading JSON 
  while (file.exists('nodejs/temp/json_to_feed.txt')) {Sys.sleep(2)}
  
}
