# 
# update exposure tables
# 
require(digest)
require(stringi)
require(readxl)
require(scales)
require(data.table)
require(Matrix)
require(Matrix.utils)
require(Rblpapi)
require(RSQLite)
require(DBI)
require(gsubfn)
require(stringi)
require(fasttime)

source("https://raw.githubusercontent.com/satrapade/utility/master/utility_functions.R")

# mapply for matrices
source("https://raw.githubusercontent.com/satrapade/utility/master/with_columns.R")

# dMcast is slow and fails on large tables
source("https://raw.githubusercontent.com/satrapade/utility/master/nn_cast.R")

source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/append2log.R")

# some matrices are too big for data.table::melt, so we melt them to a file in chunks
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/melt2file.R")

# memoized bloomberg bdp
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/memo_bdp.R")

# get current ticker from historical ticker
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/get_current_equity_isin.R")

# create dense vector of dates from start, end date
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/make_date_range.R")

# fetch historical data and make it into a matrix
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/populate_history_matrix.R")

# memoized historical data matrix
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/memo_populate_history_matrix.R")

# macro expansion for dynamic SQL queries
source("https://raw.githubusercontent.com/satrapade/pairs/master/sql_tools/make_query.R")

# some queries are too large, so we chunk them by splitting on comments
source("https://raw.githubusercontent.com/satrapade/pairs/master/sql_tools/chunk_query.R")


#
# start workflow
#

append2log("create_database_temp_tables.R: fetching config")
config<-new.env()

source(
  file="https://raw.githubusercontent.com/satrapade/pairs/master/configuration/workflow_config.R",
  local=config
)

required_config<-c(
  "database_end_date","database_product_id",
  "database_results_directory","database_root_bucket_name",
  "database_source_id","database_start_date"
)

config_not_found<-which(!required_config %in% ls(config))
if(length(config_not_found)>0){
  append2log(paste0(
    "create_database_temp_tables.R: not found ",
    paste0(required_config[config_not_found],collapse = " ")
  ))
  stop()
}

append2log("create_database_temp_tables.R:opening db conncections")


# bloomberg
con<-Rblpapi::blpConnect()

# copy of production position db
if(exists("db"))dbDisconnect(db)
db<-dbConnect(
  odbc::odbc(), 
  .connection_string = paste0(
    "driver={SQL Server};",
    "server=SQLS071FP\\QST;",
    "database=PRDQSTFundPerformance;",
    "trusted_connection=true"
  )
)

# bloomberg cache
if(exists("db_bbg_cache"))dbDisconnect(db_bbg_cache)
db_bbg_cache<-dbConnect(
  SQLite(), 
  dbname=paste0(config$database_results_directory,"bbg_cache.sqlite")
)


# get latest date
append2log("create_database_temp_tables.R: determining latest date ")
res<-try(end_date<-paste0("'",stri_sub(query(make_query(
  product_id=config$database_product_id,
  position_data_source_id=config$database_source_id,
  query_string="
    SELECT MAX(HistoricalDate)
    FROM tHistoricalBucketHolding
    WHERE tHistoricalBucketHolding.ProductId = --R{product_id}--
    AND   tHistoricalBucketHolding.DataSourceId = --R{position_data_source_id}--
  "
))[[1]],1,10),"'"),silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error determining latest date ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
}

append2log("create_database_temp_tables.R:checking 'HistoricalEquityIsins' table ")

# check if  "HistoricalEquityIsins" exists,
# if the table does not exist, create is

if(!all("HistoricalEquityIsins" %in% dbListTables(db_bbg_cache))){
  append2log("create_database_temp_tables.R: 'HistoricalEquityIsins' does not exist ")
  dbWriteTable(
    conn=db_bbg_cache,
    name="HistoricalEquityIsins",
    value=data.table(
      risk_isin=character(0),
      current_risk_isin=character(0),
      update_date=character(0)
    ),
    overwrite=TRUE,
    append=FALSE
  )
}

required_tables<-c(
  "ttISIN_MARKET_STATUS", # market status of exposure ISINs 
  "ttHISTORICAL_EQUITY_ISIN",
  "ttDATES","ttBUCKETS",
  "ttBUCKET_EXPOSURES","ttBUCKET_PNL",
  "ttBUID_MARKET_STATUS","ttCURRENT_ISIN_MARKET_STATUS", "ttEXPOSURE_SECURITIES", "ttHISTORICAL_BUCKET_EXPOSURES",
  "ttHISTORICAL_BUCKET_HOLDINGS", "ttHISTORICAL_BUCKETS", "ttHISTORICAL_EQUITY_ISIN",
  "ttTICKER_MARKET_STATUS"       
)

if(!all(required_tables %in% dbListTables(db))){
  append2log("create_database_temp_tables.R: temp tables dont exist, probably overwritten on daily copy ")
}else{
  append2log("create_database_temp_tables.R: temp tables do exist, this has run once before today ")
}
  
# get ticker universe and static
append2log("create_database_temp_tables.R: relevant_equity_ticker_table.sql ")
res<-try(the_query<-make_query(
  product_id=config$database_product_id,
  position_data_source_id=config$database_source_id,
  rates_data_source_id=config$database_source_id,
  start_date=config$database_start_date,
  start_date_search=config$database_start_date,
  end_date=end_date,
  end_date_search=end_date,
  file="https://raw.githubusercontent.com/satrapade/pairs/master/sql/relevant_equity_ticker_table.sql"
),silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error making query relevant_equity_ticker_table.sql ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
}
res<-try(equity_ticker_table<-query(db=db,the_query),silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error executing relevant_equity_ticker_table.sql ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
}

# get market status for tickers
append2log("create_database_temp_tables.R: ttISIN_MARKET_STATUS ") 
try(res<-dbWriteTable(
    conn=db,
    name="ttISIN_MARKET_STATUS",
    value=data.table(
      Rblpapi::bdp(equity_ticker_table[!is.na(security_isin),paste0("/isin/",security_isin)],"MARKET_STATUS"),
      keep.rownames=TRUE
    )[,.(isin=gsub("^/isin/","",rn),MARKET_STATUS=MARKET_STATUS)],
    overwrite=TRUE,
    append=FALSE
),silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error writing ttISIN_MARKET_STATUS  ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
}
  
# get current ISINs for historical positions
res<-try({
  historical_equity_isins<-query(
    "SELECT * FROM HistoricalEquityIsins",
    db=db_bbg_cache
  )[,.SD,keyby=risk_isin]
  current_exposure_isins<-historical_equity_isins[
    equity_ticker_table[
      !is.na(security_isin),
      .(risk_isin=security_isin)
    ],
    on="risk_isin"
  ]
  the_isins<-current_exposure_isins[is.na(current_risk_isin)]
},silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error retrieving current ISINs  ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
}

#
# if there are any isins not in the table, 
# fetch the isins from BBG
# append them to 'HistoricalEquityIsins
#
res<-try(if(nrow(the_isins)>0){
    
    append2log("create_database_temp_tables.R: update 'HistoricalEquityIsins' ") 
  
    con<-Rblpapi::blpConnect()
    
    current_isin<-mapply(
      get_current_equity_isin,
      date=as.character(Sys.Date(),format="%Y-%m-%d"),
      isin=the_isins$risk_isin
    )
    
    historical_equity_isins_update_table<-data.table(
      risk_isin=the_isins$risk_isin,
      current_risk_isin=current_isin,
      update_date=as.character(Sys.Date(),format="%Y-%m-%d")
    )
    
    dbWriteTable(
      conn=db_bbg_cache,
      name="HistoricalEquityIsins",
      value=historical_equity_isins_update_table,
      overwrite=FALSE,
      append=TRUE
    )
},silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error updating 'HistoricalEquityIsins' ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
}  

#
# now, re-fetch the active ISINs from table that is quaranteed
# to contain new ones and write to table in DB
#
res<-try({
  new_historical_equity_isins<-query("SELECT * FROM HistoricalEquityIsins",db=db_bbg_cache)[,.SD,keyby=risk_isin]
  append2log("create_database_temp_tables.R: update 'ttHISTORICAL_EQUITY_ISIN' ") 
  dbWriteTable(
    conn=db,
    name="ttHISTORICAL_EQUITY_ISIN",
    value=new_historical_equity_isins[,.(
      risk_isin=risk_isin,
      current_risk_isin=current_risk_isin,
      update_date=update_date
    )],
    overwrite=TRUE,
    append=FALSE
  )
},silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error fetching ISINs from updated 'HistoricalEquityIsins' ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
}  

#
#
#
res<-try({
  append2log("create_database_temp_tables.R: update 'ttCURRENT_ISIN_MARKET_STATUS' ") 
  dbWriteTable(
    conn=db,
    name="ttCURRENT_ISIN_MARKET_STATUS",
    value=data.table(
      Rblpapi::bdp(new_historical_equity_isins[!is.na(current_risk_isin),unique(paste0("/isin/",current_risk_isin))],"MARKET_STATUS"),
      keep.rownames=TRUE
    )[,.(isin=gsub("^/isin/","",rn),MARKET_STATUS=MARKET_STATUS)],
    overwrite=TRUE,
    append=FALSE
  )
},silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error updating 'ttCURRENT_ISIN_MARKET_STATUS' ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
}  

#
#
#
res<-try({
  append2log("create_database_temp_tables.R: update 'ttBUID_MARKET_STATUS' ") 
  dbWriteTable(
    conn=db,
    name="ttBUID_MARKET_STATUS",
    value=data.table(
      Rblpapi::bdp(equity_ticker_table[!is.na(security_buid),paste0("/buid/",security_buid)],"MARKET_STATUS"),
      keep.rownames=TRUE
    )[,.(buid=gsub("^/buid/","",rn),MARKET_STATUS=MARKET_STATUS)],
    overwrite=TRUE,
    append=FALSE
  )
},silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error updating 'ttBUID_MARKET_STATUS'  ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
}  

#
#
#
res<-try({
  append2log("create_database_temp_tables.R: update 'ttTICKER_MARKET_STATUS' ") 
  dbWriteTable(
    conn=db,
    name="ttTICKER_MARKET_STATUS",
    value=data.table(
      Rblpapi::bdp(
        equity_ticker_table[!is.na(security_ticker) & grepl("Equity$",security_ticker),unique(security_ticker)],
        "MARKET_STATUS"
      ),
      keep.rownames=TRUE
    )[,.(ticker=rn,MARKET_STATUS=MARKET_STATUS)],
    overwrite=TRUE,
    append=FALSE
  )
},silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error updating  'ttTICKER_MARKET_STATUS'  ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
} 

#
# create_ttDATES.sql
#
res<-try({
  append2log("create_database_temp_tables.R: create_ttDATES.sql ") 
  q<-dbSendStatement(conn=db,make_query(
    valuation_dates=local({
      vdr1<-make_date_range(
        config$database_start_date,
        end_date
      )
      vdr2<-paste0("SELECT '",vdr1,"' AS date")
      paste(vdr2,collapse=" UNION ALL \n")
    }),
    file="https://raw.githubusercontent.com/satrapade/pairs/master/sql/create_ttDATES.sql"
  ))
  dbClearResult(q)
},silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error in create_ttDATES.sql  ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
}

#
# create_ttBUCKETS.sql
#
res<-try({
  append2log("create_database_temp_tables.R: create_ttBUCKETS.sql ") 
  q<-dbSendStatement(conn=db,make_query(
    root_bucket_name=config$database_root_bucket_name,
    file="https://raw.githubusercontent.com/satrapade/pairs/master/sql/create_ttBUCKETS.sql"
  ))
  dbClearResult(q)
},silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error in create_ttBUCKETS.sql  ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
}  

#
# create_ttHISTORICAL_BUCKETS.sql 
#
res<-try({
  append2log("create_database_temp_tables.R: create_ttHISTORICAL_BUCKETS.sql ") 
  q<-dbSendStatement(conn=db,make_query(
    product_id=config$database_product_id,
    position_data_source_id=config$database_source_id,
    file="https://raw.githubusercontent.com/satrapade/pairs/master/sql/create_ttHISTORICAL_BUCKETS.sql"
  ))
  dbClearResult(q)
},silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error in create_ttHISTORICAL_BUCKETS.sql  ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
}

#
# create_ttBUCKET_PNL.sql
#
res<-try({
  append2log("create_database_temp_tables.R: create_ttBUCKET_PNL.sql ") 
  q<-dbSendStatement(conn=db,make_query(
    file="https://raw.githubusercontent.com/satrapade/pairs/master/sql/create_ttBUCKET_PNL.sql"
  ))
  dbClearResult(q)
},silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error in create_ttBUCKET_PNL.sql  ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
}

#
# create_ttHISTORICAL_BUCKET_HOLDINGS.sql
#
res<-try({
  append2log("create_database_temp_tables.R: create_ttHISTORICAL_BUCKET_HOLDINGS.sql ") 
  q<-dbSendStatement(conn=db,make_query(
    product_id=config$database_product_id,
    position_data_source_id=config$database_source_id,
    file="https://raw.githubusercontent.com/satrapade/pairs/master/sql/create_ttHISTORICAL_BUCKET_HOLDINGS.sql"
  ))
  dbClearResult(q)
},silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error in create_ttHISTORICAL_BUCKET_HOLDINGS.sql  ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
}

#
# create_ttHISTORICAL_BUCKET_EXPOSURES.sql
#
res<-try({
  append2log("create_database_temp_tables.R: create_ttHISTORICAL_BUCKET_EXPOSURES.sql ") 
  q<-dbSendStatement(conn=db,make_query(
    file="https://raw.githubusercontent.com/satrapade/pairs/master/sql/create_ttHISTORICAL_BUCKET_EXPOSURES.sql"
  ))
  dbClearResult(q)
},silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error in create_ttHISTORICAL_BUCKET_EXPOSURES.sql  ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
}

#
# create_ttEXPOSURE_SECURITIES.sql
#
res<-try({
  append2log("create_database_temp_tables.R: create_ttEXPOSURE_SECURITIES.sql ") 
  q<-dbSendStatement(conn=db,make_query(
    file="https://raw.githubusercontent.com/satrapade/pairs/master/sql/create_ttEXPOSURE_SECURITIES.sql"
  ))
  dbClearResult(q)
},silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error in create_ttEXPOSURE_SECURITIES.sql ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
}  
  
#
# create_ttBUCKET_EXPOSURES.sql
#
res<-try({
  append2log("create_database_temp_tables.R: create_ttBUCKET_EXPOSURES.sql ") 
  q<-dbSendStatement(conn=db,make_query(
    file="https://raw.githubusercontent.com/satrapade/pairs/master/sql/create_ttBUCKET_EXPOSURES.sql"
  ))
  dbClearResult(q)
},silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error in create_ttBUCKET_EXPOSURES.sql ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
}   
  
  required_tables<-c(
    "ttBUCKET_EXPOSURES","ttBUCKET_PNL","ttBUCKETS","ttBUID_MARKET_STATUS",
    "ttCURRENT_ISIN_MARKET_STATUS","ttDATES", "ttEXPOSURE_SECURITIES", "ttHISTORICAL_BUCKET_EXPOSURES",
    "ttHISTORICAL_BUCKET_HOLDINGS", "ttHISTORICAL_BUCKETS", "ttHISTORICAL_EQUITY_ISIN", "ttISIN_MARKET_STATUS",
    "ttTICKER_MARKET_STATUS"       
  )
  
  if(all(required_tables %in% dbListTables(db))){
    append2log("create_database_temp_tables.R: temp tables successfully created ")
  }else{
    append2log("create_database_temp_tables.R: temp tables creation failed ")
  }
  
  #
  # save results for bucket reports
  #
  
  all_days<-query("SELECT * FROM ttDATES WHERE date>'2011-01-01' ORDER BY date ")
  fwrite(all_days,paste0(config$database_results_directory,"all_days.csv"))
  append2log("create_database_temp_tables.R: save 'all_days' in 'db_cache' directory ")
  
  all_buckets<-query("SELECT * FROM ttBUCKETS ")
  fwrite(all_buckets,paste0(config$database_results_directory,"all_buckets.csv"))
  append2log("create_database_temp_tables.R: save 'all_buckets' in 'db_cache' directory ")
  
  exposure_securities<-query("SELECT * FROM ttEXPOSURE_SECURITIES")[
    exposure_security_type %in% c("Equity Index","Equity Etd","Fund Etd")
    ]
  fwrite(exposure_securities,paste0(config$database_results_directory,"exposure_securities.csv"))
  append2log("create_database_temp_tables.R: save 'exposure_securities' in 'db_cache' directory ")
  
  bucket_exposures<-query("SELECT * FROM ttBUCKET_EXPOSURES WHERE date > '2011-01-01'")[
    exposure_security_type %in% c("Equity Index","Equity Etd","Fund Etd")
    ]
  

  ref_matrix<-matrix(
    0,
    nrow=nrow(all_days),
    ncol=nrow(exposure_securities),
    dimnames=list(all_days$date,exposure_securities$exposure_security_external_id)
  )
  
#
# tret
#
res<-try({
  tret<-melt2file(memo_populate_history_matrix(
    ref.matrix=ref_matrix,
    field="DAY_TO_DAY_TOT_RETURN_GROSS_DVDS",
    post.function = function(x)scrub(x)/100,
    force=FALSE
  ),paste0(config$database_results_directory,"tret.csv"))
  append2log("create_database_temp_tables.R: save 'tret' in 'db_cache' directory ")
},silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error fetching tret ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
}  

#
# px_last
#
res<-try({
  px_last<-melt2file(memo_populate_history_matrix(
    ref.matrix=ref_matrix,
    field="PX_LAST",
    overrides=c(EQY_FUND_CRNCY="GBP"),
    post.function = function(x)replace_zero_with_last(scrub(x)),
    force=FALSE
  ),paste0(config$database_results_directory,"px_last.csv"))
  append2log("create_database_temp_tables.R: save 'px_last' in 'db_cache' directory ")
},silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error fetching px_last ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
} 

#
# px_high
#
res<-try({
  px_high<-melt2file(memo_populate_history_matrix(
    ref.matrix=ref_matrix,
    field="HIGH",
    overrides=c(EQY_FUND_CRNCY="GBP"),
    post.function = function(x)replace_zero_with_last(scrub(x)),
    force=FALSE
  ),paste0(config$database_results_directory,"px_high.csv"))
  append2log("create_database_temp_tables.R: save 'px_high' in 'db_cache' directory ")
},silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error fetching px_high ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
} 

#
# px_low
#
res<-try({
  px_low<-melt2file(memo_populate_history_matrix(
    ref.matrix=ref_matrix,
    field="LOW",
    overrides=c(EQY_FUND_CRNCY="GBP"),
    post.function = function(x)replace_zero_with_last(scrub(x)),
    force=FALSE
  ),paste0(config$database_results_directory,"px_low.csv"))
  append2log("create_database_temp_tables.R: save 'all_days' in 'px_low' directory ")
},silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error fetching px_low ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
} 

#
# px_open
#
res<-try({
  px_open<-melt2file(memo_populate_history_matrix(
    ref.matrix=ref_matrix,
    field="OPEN",
    overrides=c(EQY_FUND_CRNCY="GBP"),
    post.function = function(x)replace_zero_with_last(scrub(x)),
    force=FALSE
  ),paste0(config$database_results_directory,"px_open.csv"))
  append2log("create_database_temp_tables.R: save 'px_low' in 'db_cache' directory ")
},silent=TRUE)
if(any(class(res) %in% "try-error")){
  append2log("create_database_temp_tables.R: error fetching px_open ")
  append2log(paste0("create_database_temp_tables.R:",as.character(res),collapse=" "))
  stop()
} 


  fund<-query(make_query(
      product_id=config$database_product_id,
      source_id=config$database_source_id,
      query_string = c("
        SELECT * 
        FROM tHistoricalProduct 
        WHERE ProductId=--R{product_id}--
        AND DataSourceId=--R{source_id}--
      ")
  ))[
    TRUE,
    "date":=as.Date(fastPOSIXct(HistoricalDate))
  ]
  
  
  ptf<-bucket_exposures[
    TRUE,
    .(
      date=date,
      nav=local({
        ndx<-replace_zero_with_last(scrub(match(date,as.character(fund$date))))
        replace_zero_with_last(scrub(fund[ndx,NetAssetValue]))
      }),
      bucket=bucket,
      ticker=local({
        i<-match(
          exposure_security_external_id,
          exposure_securities$exposure_security_external_id
        )
        gsub(" Equity","",exposure_securities$security_ticker[i])
      }),
      security=exposure_security_external_id,
      security_units=security_units,
      market_value=market_value,
      open=px_open[cbind(
        match(date,rownames(px_open)),
        match(exposure_security_external_id,colnames(px_open))
      )],
      close=px_last[cbind(
        match(date,rownames(px_last)),
        match(exposure_security_external_id,colnames(px_last))
      )],
      high=px_high[cbind(
        match(date,rownames(px_high)),
        match(exposure_security_external_id,colnames(px_high))
      )],
      low=px_low[cbind(
        match(date,rownames(px_low)),
        match(exposure_security_external_id,colnames(px_low))
      )],
      tret=tret[cbind(
        match(date,rownames(tret)),
        match(exposure_security_external_id,colnames(tret))
      )]
    )
  ]
  
  fwrite(ptf,paste0(config$database_results_directory,"ptf.csv"))
  append2log("create_database_temp_tables.R: save 'ptf' in 'db_cache' directory ")
  
  
  append2log("create_database_temp_tables.R: closing connections ") 
  if(exists("db"))dbDisconnect(db)
  if(exists("db_bbg_cache"))dbDisconnect(db_bbg_cache)

