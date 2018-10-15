
#
# fetch sheet positions, simplified version
#
require(stringi)
require(data.table)
require(readxl)
library(Rblpapi)
rcon<-Rblpapi::blpConnect()
        
#
scrub<-function(x)
{
  if(length(x)==0)return(0)
  x[which(!is.finite(x))]<-0
  x
}

#
ticker_class<-function(x)
{
  
  x_trim<-stri_trim(x)
  x_upper<-toupper(x_trim)
  x_lower<-tolower(x_trim)
  
  all_matches<-list(
    list(class="date",match=grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$",x_trim)),
    list(class="equity",match=grepl("^[A-Z0-9/]+\\s+[A-Z]{2,2}$",x_trim)),
    list(class="equity",match=grepl("^[A-Z0-9/]+\\s+[A-Z]{2,2}\\sEquity$",x_trim)),
    list(class="equity",match=grepl("^[a-z0-9/]+\\s+[a-z]{2,2}\\sequity$",x_lower)),
    list(class="index",match=grepl("^[A-Z0-9]{3,20}$",x_trim)),
    list(class="index",match=grepl("^[A-Z0-9]{3,20}\\sIndex$",x_trim)),
    list(class="cix",match=grepl("^\\.[A-Z0-9]{3,20}\\sIndex$",x_trim)),
    list(class="index",match=grepl("^[a-z0-9]{3,20}\\sindex$",x_lower)),
    list(class="index",match=grepl("^[A-Z0-9]+\\s+[A-Z]{2}\\s+Index$",x_trim)),
    list(class="future",match=grepl("^([A-Z]{2,4}|[A-Z]\\s)([FGHJKMNQUVXZ]\\d{1,2})$",x_trim)),
    list(class="future",match=grepl("^([A-Z]{2,4}|[A-Z]\\s)([FGHJKMNQUVXZ]\\d{1,2}) Index$",x_trim)),
    list(class="future",match=grepl("^([A-Z]{2,4}|[A-Z]\\s)([FGHJKMNQUVXZ]\\d{1,2})$",x_upper)),
    list(class="future",match=grepl("^([A-Z]{2,4}|[A-Z]\\s)([FGHJKMNQUVXZ]\\d{1,2}) INDEX$",x_upper)),
    list(class="bbgisin",match=grepl("^/isin/[A-Z]{2}[A-Z0-9]{10}$",x_trim)),
    list(class="isin",match=grepl("^[A-Z]{2}[A-Z0-9]{10}$",x_trim)),
    list(class="equity_option",match=grepl("^[A-Z0-9]{1,10} [A-Z0-9]{2} [0-9]{2}/[0-9]{2}/[0-9]{2} [CP]{1}[\\.0-9]{1,5}$",x_trim)),
    list(class="otc_equity_option",match=grepl("^OTC-[A-Z0-9]{1,10} [A-Z0-9]{2} [0-9]{2}/[0-9]{2}/[0-9]{2} [CP]{1}[\\.0-9]{1,5}$",x_trim)),
    list(class="index_option",match=grepl("^[A-Z0-9]{1,10} [0-9]{2}/[0-9]{2}/[0-9]{2} [CP]{1}[\\.0-9]{1,5}$",x_trim)),
    list(class="index_option",match=grepl("^[A-Z0-9]{1,10} [0-9]{1,2} [CP]{1}[\\.0-9]{1,5}$",x_trim)),
    list(class="otc_index_option",match=grepl("^OTC-[A-Z0-9]{1,10} [0-9]{2}/[0-9]{2}/[0-9]{2} [CP]{1}[\\.0-9]{1,5}$",x_trim)),
    list(class="forex",match=grepl("^[A-Z]{3,20}\\sCurncy$",x_trim))
  )
  all_classes<-do.call(cbind,mapply(function(m)ifelse(m$match,m$class,"nomatch"),all_matches,SIMPLIFY=FALSE))
  ticker_patterns<-apply(all_classes,1,function(a)paste(sort(unique(a)),collapse="|"))
  
  ticker_patterns
}

fname<-"N:/Depts/Global/Absolute Insight/UK Equity/AbsoluteUK xp final.xlsm"

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

duke_position_scrape<-data.table(read_xlsx(
  path="N:/Depts/Global/Absolute Insight/UK Equity/AbsoluteUK xp final.xlsm", 
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

