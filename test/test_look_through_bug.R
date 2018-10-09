require(Hmisc)
require(stringi)
require(digest)
require(scales)
require(data.table)
require(Matrix)
require(Matrix.utils)
require(clue)
require(magick)
require(readxl)
require(Rtsne)
require(knitr)
require(magrittr)
require(gsubfn)
require(FRAPO)
require(ggplot2)

config<-new.env()
source(
  file="https://raw.githubusercontent.com/satrapade/pairs/master/configuration/workflow_config.R",
  local=config
)
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/fetch_risk_report.R")
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/append2log.R")
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/log_code.R")
source("https://raw.githubusercontent.com/satrapade/utility/master/ticker_class.R")
source("https://raw.githubusercontent.com/satrapade/utility/master/scrub.R")

source("https://raw.githubusercontent.com/satrapade/latex_utils/master/latex_helpers_v2.R")
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/volatility_trajectory.R")
source("https://raw.githubusercontent.com/satrapade/utility/master/nn_cast.R")

off_site<-if(Sys.info()["sysname"]=="Windows"){FALSE}else{TRUE}

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


function(
  equity_index_ticker="UKX Index",
  db=get("db",parent.frame())
){
  
  product_types<-query("SELECT * FROM tProductType",db=db)[,.SD,keyby=Name]
  
  equity_index_products<-query(make_query(
    product_type=product_types["Equity Index",ProductTypeId],
    query_string = "SELECT * FROM tProduct WHERE ProductTypeId=--R{product_type}--"
  ),db=db)[,.SD,keyby=PrimaryDataSourceProductCode]
  
  equity_index_update<-query(make_query(
    product_id=equity_index_products[equity_index_ticker,ProductId],
    query_string = "SELECT MAX(HistoricalDate) FROM tHistoricalProductHolding WHERE ProductId=--R{product_id}--"
  ),db=db)[[1]]
  
  equity_index_weights<-query(make_query(
    product_id=equity_index_products[equity_index_ticker,ProductId],
    update_date=equity_index_update,
    query_string = "
      SELECT 
        tProduct.PrimaryDataSourceProductCode AS IndexTicker,
        tSecurity.Ticker AS Ticker,
        tSecurity.UniqueId AS UniqueId,
        tHistoricalProductHolding.SecurityUnits AS Weight
      FROM tHistoricalProductHolding 
      LEFT JOIN tProduct ON tProduct.ProductId=tHistoricalProductHolding.ProductId
      LEFT JOIN tSecurity ON tSecurity.SecurityId=tHistoricalProductHolding.SecurityId
      WHERE tHistoricalProductHolding.ProductId=--R{product_id}-- 
      AND tHistoricalProductHolding.HistoricalDate='--R{update_date}--'
    "
  ),db=db)
  
  equity_index_weights
  
}

equity_index_ticker="UKX Index"

product_types<-query("SELECT * FROM tProductType",db=db)[,.SD,keyby=Name]

equity_index_products<-query(make_query(
    product_type=product_types["Equity Index",ProductTypeId],
    query_string = "SELECT * FROM tProduct WHERE ProductTypeId=--R{product_type}--"
),db=db)[,.SD,keyby=PrimaryDataSourceProductCode]

duke_exposure<-fread(
  "N:/Depts/Share/UK Alpha Team/Analytics/duke_summary/duke_exposure.csv",
  col.names = c("Ticker","Exposure")
)


duke_index_exposure<-duke_exposure[ticker_class(Ticker)=="index|nomatch"]
duke_equity_exposure<-duke_exposure[ticker_class(Ticker)=="equity|nomatch"]


mapply(
  function(ndx){
    cat(ndx,"\n")
    fetch_index_weights(ndx)
  },
  duke_index_exposure$Ticker
)




