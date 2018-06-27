
require(DBI)
require(data.table)

if(!exists("query")){
 source("https://raw.githubusercontent.com/satrapade/utility/master/make_query.R")
}

fetch_index_weights<-function(
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

