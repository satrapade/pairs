#
# scrape DUKE positions of
# spreadsheet
#
# convert futures exposures
# into index exposures
#

require(stringi)
require(data.table)
require(readxl)
require(ggplot2)
require(magrittr)
library(Rblpapi)

scrape_duke<-function(
  fname="N:/Depts/Global/Absolute Insight/UK Equity/AbsoluteUK xp final.xlsm"
){
  
  rcon<-Rblpapi::blpConnect()
  # DUKE AUM 
  duke_summary_scrape<-data.table(read_xlsx(
    path=fname,
    sheet="DUKE Summary",
    range="A4:B11",
    col_names = c("A","B"),
    col_types = "text"
  ))[,.(
    parameter=make.names(A),
    value=scrub(as.numeric(B))
  )][,.SD,keyby=parameter]
  
  # DUKE open positions
  duke_position_scrape<-data.table(read_xlsx(
    path=fname, 
    sheet = "DUKE Open Trades", 
    range = "B10:P2000", 
    col_names = c("B","C","D","E","F","G","H","I","J","K","L","M","N","O","P"), 
    col_types = "text"
  ))[grepl("^[A-Z]{2,4}[0-9]{1,3}$",stri_trim(toupper(B)))][,.(
    row=seq_along(C),
    manager=toupper(C),
    pair=toupper(B[B!=""][findInterval(1:length(B),which(B!=""),all.inside = TRUE,rightmost.closed = TRUE)]),
    direction=toupper(D),
    ticker=local({
      x0<-gsub("[ ]{2,10}"," ",F)
      x1<-toupper(x0)
      x2<-gsub("EQUITY$","Equity",x1)
      x3<-gsub("INDEX$","Index",x2)
      ifelse(is.na(x3),"",x3)
    }),
    units=scrub(as.integer(I)),
    multiplier=scrub(as.integer(H)),
    quantity=scrub(as.integer(H))*scrub(as.integer(I)),
    price=scrub(as.numeric(G)),
    cash=(-1)*scrub(as.numeric(J)),
    asset_value=scrub(as.numeric(N)),
    pnl=scrub(as.numeric(O)),
    bps=scrub(as.numeric(P)),
    fx=scrub(as.numeric(M)),
    local_asset_value=scrub(as.numeric(L)),
    class=ticker_class(gsub("INDEX$","Index",gsub("EQUITY$","Equity",toupper(F))))
  )][,c(.SD,list(
    risk_ticker=local({
      res<-Rblpapi::bdp(unique(ticker[grepl("future",class)]),"UNDL_SPOT_TICKER")
      res1<-data.table(
        sheet_ticker=rownames(res),
        risk_ticker=paste0(res$UNDL_SPOT_TICKER," Index")
      )[,.SD,keyby=sheet_ticker]
      res2<-res1[J(ticker),risk_ticker]
      ifelse(is.na(res2),ticker,res2)
    })
  ))][ticker!=""][,.(
    manager=unique(manager),
    exposure=sum(asset_value)/duke_summary_scrape["Current.fund",value]
  ),keyby=c("pair","risk_ticker")][,.(
    manager,
    pair,
    ticker=risk_ticker,
    exposure=round(exposure*10000,digits=1)
  )]
  
  duke_position_scrape
  
}

