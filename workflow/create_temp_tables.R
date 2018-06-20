
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


source("https://raw.githubusercontent.com/satrapade/latex_utils/master/utility_functions.R")

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

config<-new.env()

source(
  file="https://raw.githubusercontent.com/satrapade/pairs/master/configuration/workflow_config.R",
  local=config
)



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
  dbname="N:/Depts/Share/UK Alpha Team/Analytics/db_cache/bbg_cache.sqlite"
)

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
  "ttBUCKET_EXPOSURES","ttBUCKET_PNL","ttBUCKETS","ttBUID_MARKET_STATUS",
  "ttCURRENT_ISIN_MARKET_STATUS","ttDATES", "ttEXPOSURE_SECURITIES", "ttHISTORICAL_BUCKET_EXPOSURES",
  "ttHISTORICAL_BUCKET_HOLDINGS", "ttHISTORICAL_BUCKETS", "ttHISTORICAL_EQUITY_ISIN", "ttISIN_MARKET_STATUS",
  "ttTICKER_MARKET_STATUS"       
)

if(!all(required_tables %in% dbListTables(db)))append2log("create_database_temp_tables.R: temp tables dont exist ")
  
# get ticker universe and static
append2log("create_database_temp_tables.R: relevant_equity_ticker_table.sql ")
the_query<-make_query(
  product_id=config$database_product_id,
  position_data_source_id=config$database_source_id,
  rates_data_source_id=config$database_source_id,
  start_date=config$database_start_date,
  start_date_search=config$database_start_date,
  file="https://raw.githubusercontent.com/satrapade/pairs/master/sql/relevant_equity_ticker_table.sql"
)
equity_ticker_table<-query(db=db,the_query)

# get market status for tickers
append2log("create_database_temp_tables.R: ttISIN_MARKET_STATUS ") 
dbWriteTable(
    conn=db,
    name="ttISIN_MARKET_STATUS",
    value=data.table(
      Rblpapi::bdp(equity_ticker_table[!is.na(security_isin),paste0("/isin/",security_isin)],"MARKET_STATUS"),
      keep.rownames=TRUE
    )[,.(isin=gsub("^/isin/","",rn),MARKET_STATUS=MARKET_STATUS)],
    overwrite=TRUE,
    append=FALSE
)
  
# get current ISINs for historical positions
historical_equity_isins<-query("SELECT * FROM HistoricalEquityIsins",db=db_bbg_cache)[,.SD,keyby=risk_isin]
current_exposure_isins<-historical_equity_isins[
    equity_ticker_table[!is.na(security_isin),.(risk_isin=security_isin)],
    on="risk_isin"
]
the_isins<-current_exposure_isins[is.na(current_risk_isin)]
  
  # if there are any isins not in the table, run an update
if(nrow(the_isins)>0){
    
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
}
  
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
  
  #1
  append2log("create_database_temp_tables.R: create_ttDATES.sql ") 
  q<-dbSendStatement(conn=db,make_query(
    valuation_dates=local({
      vdr1<-make_date_range(
        config$database_start_date,
        config$database_end_date
      )
      vdr2<-paste0("SELECT '",vdr1,"' AS date")
      paste(vdr2,collapse=" UNION ALL \n")
    }),
    file="https://raw.githubusercontent.com/satrapade/pairs/master/sql/create_ttDATES.sql"
  ))
  dbClearResult(q)
  
  #2
  append2log("create_database_temp_tables.R: create_ttBUCKETS.sql ") 
  q<-dbSendStatement(conn=db,make_query(
    root_bucket_name=config$database_root_bucket_name,
    file="https://raw.githubusercontent.com/satrapade/pairs/master/sql/create_ttBUCKETS.sql"
  ))
  dbClearResult(q)
  
  #3
  append2log("create_database_temp_tables.R: create_ttHISTORICAL_BUCKETS.sql ") 
  q<-dbSendStatement(conn=db,make_query(
    product_id=config$database_product_id,
    position_data_source_id=config$database_source_id,
    file="https://raw.githubusercontent.com/satrapade/pairs/master/sql/create_ttHISTORICAL_BUCKETS.sql"
  ))
  dbClearResult(q)
  
  #4
  append2log("create_database_temp_tables.R: create_ttBUCKET_PNL.sql ") 
  q<-dbSendStatement(conn=db,make_query(
    file="https://raw.githubusercontent.com/satrapade/pairs/master/sql/create_ttBUCKET_PNL.sql"
  ))
  dbClearResult(q)
  
  #5
  append2log("create_database_temp_tables.R: create_ttHISTORICAL_BUCKET_HOLDINGS.sql ") 
  q<-dbSendStatement(conn=db,make_query(
    product_id=config$database_product_id,
    position_data_source_id=config$database_source_id,
    file="https://raw.githubusercontent.com/satrapade/pairs/master/sql/create_ttHISTORICAL_BUCKET_HOLDINGS.sql"
  ))
  dbClearResult(q)
  
  #6
  append2log("create_database_temp_tables.R: create_ttHISTORICAL_BUCKET_EXPOSURES.sql ") 
  q<-dbSendStatement(conn=db,make_query(
    file="https://raw.githubusercontent.com/satrapade/pairs/master/sql/create_ttHISTORICAL_BUCKET_EXPOSURES.sql"
  ))
  dbClearResult(q)
  
  #7
  append2log("create_database_temp_tables.R: create_ttEXPOSURE_SECURITIES.sql ") 
  q<-dbSendStatement(conn=db,make_query(
    file="https://raw.githubusercontent.com/satrapade/pairs/master/sql/create_ttEXPOSURE_SECURITIES.sql"
  ))
  dbClearResult(q)
  
  
  #8
  append2log("create_database_temp_tables.R: create_ttBUCKET_EXPOSURES.sql ") 
  q<-dbSendStatement(conn=db,make_query(
    file="https://raw.githubusercontent.com/satrapade/pairs/master/sql/create_ttBUCKET_EXPOSURES.sql"
  ))
  dbClearResult(q)
  
  
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
  

  ref_matrix<-matrix(
    0,
    nrow=nrow(all_days),
    ncol=nrow(exposure_securities),
    dimnames=list(all_days$date,exposure_securities$exposure_security_external_id)
  )
  
  tret<-melt2file(memo_populate_history_matrix(
    ref.matrix=ref_matrix,
    field="DAY_TO_DAY_TOT_RETURN_GROSS_DVDS",
    post.function = function(x)scrub(x)/100,
    force=FALSE
  ),"N:/Depts/Share/UK Alpha Team/Analytics/db_cache/tret.csv")
  append2log("create_database_temp_tables.R: save 'tret' in 'db_cache' directory ")

  px_last<-melt2file(memo_populate_history_matrix(
    ref.matrix=ref_matrix,
    field="PX_LAST",
    overrides=c(EQY_FUND_CRNCY="GBP"),
    post.function = function(x)replace_zero_with_last(scrub(x)),
    force=FALSE
  ),"N:/Depts/Share/UK Alpha Team/Analytics/db_cache/px_last.csv")
  append2log("create_database_temp_tables.R: save 'px_last' in 'db_cache' directory ")
  
  px_high<-melt2file(memo_populate_history_matrix(
    ref.matrix=ref_matrix,
    field="HIGH",
    overrides=c(EQY_FUND_CRNCY="GBP"),
    post.function = function(x)replace_zero_with_last(scrub(x)),
    force=FALSE
  ),"N:/Depts/Share/UK Alpha Team/Analytics/db_cache/px_high.csv")
  append2log("create_database_temp_tables.R: save 'px_high' in 'db_cache' directory ")
  
  px_low<-melt2file(memo_populate_history_matrix(
    ref.matrix=ref_matrix,
    field="LOW",
    overrides=c(EQY_FUND_CRNCY="GBP"),
    post.function = function(x)replace_zero_with_last(scrub(x)),
    force=FALSE
  ),"N:/Depts/Share/UK Alpha Team/Analytics/db_cache/px_low.csv")
  append2log("create_database_temp_tables.R: save 'all_days' in 'px_low' directory ")
  
  px_open<-melt2file(memo_populate_history_matrix(
    ref.matrix=ref_matrix,
    field="OPEN",
    overrides=c(EQY_FUND_CRNCY="GBP"),
    post.function = function(x)replace_zero_with_last(scrub(x)),
    force=FALSE
  ),"N:/Depts/Share/UK Alpha Team/Analytics/db_cache/px_open.csv")
  append2log("create_database_temp_tables.R: save 'px_low' in 'db_cache' directory ")
 
  #
  # save results for bucket reports
  #
  
  all_days<-query("SELECT * FROM ttDATES WHERE date>'2011-01-01' ORDER BY date ")
  fwrite(all_days,"N:/Depts/Share/UK Alpha Team/Analytics/db_cache/all_days.csv")
  append2log("create_database_temp_tables.R: save 'all_days' in 'db_cache' directory ")
  
  all_buckets<-query("SELECT * FROM ttBUCKETS ")
  fwrite(all_days,"N:/Depts/Share/UK Alpha Team/Analytics/db_cache/all_buckets.csv")
  append2log("create_database_temp_tables.R: save 'all_buckets' in 'db_cache' directory ")
  
  exposure_securities<-query("SELECT * FROM ttEXPOSURE_SECURITIES")[
    exposure_security_type %in% c("Equity Index","Equity Etd","Fund Etd")
  ]
  fwrite(exposure_securities,"N:/Depts/Share/UK Alpha Team/Analytics/db_cache/exposure_securities.csv")
  append2log("create_database_temp_tables.R: save 'exposure_securities' in 'db_cache' directory ")
  
  bucket_exposures<-query("SELECT * FROM ttBUCKET_EXPOSURES WHERE date > '2011-01-01'")[
    exposure_security_type %in% c("Equity Index","Equity Etd","Fund Etd")
  ]
  
  ptf<-bucket_exposures[
    TRUE,
    .(
      date=date,
      bucket=bucket,
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
  
  fwrite(ptf,"N:/Depts/Share/UK Alpha Team/Analytics/db_cache/ptf.csv")
  append2log("create_database_temp_tables.R: save 'ptf' in 'db_cache' directory ")
  
  
  append2log("create_database_temp_tables.R: closing connections ") 
  if(exists("db"))dbDisconnect(db)
  if(exists("db_bbg_cache"))dbDisconnect(db_bbg_cache)


