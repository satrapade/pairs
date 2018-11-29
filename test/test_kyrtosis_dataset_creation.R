
require(data.table)
require(PerformanceAnalytics)
require(RcppRoll)
require(Rblpapi)
rcon<-Rblpapi::blpConnect()

index_snapshot<-function(
  ndx,
  snapshot_date=as.character(Sys.Date(),"%Y%m%d")
){

  x<-Rblpapi::bds(
    security=ndx, 
    field="INDX_MWEIGHT_HIST",
    overrides=c(END_DATE_OVERRIDE=snapshot_date)
  )
  c(paste0(x[["Index Member"]]," Equity"),ndx)
}

x<-as.character(seq(from=as.Date("2001-01-01"),to=as.Date("2018-11-01"),length.out=72),format="%Y%m%d")

y<-mapply(index_snapshot,snapshot_date=tail(x,-1),MoreArgs=list(ndx="SXXP Index"),SIMPLIFY=FALSE)

z<-do.call(rbind,mapply(
  function(tickers,initial_date,final_date){
    bdh_input=data.table(
      ticker_count=length(tickers),
      tickers=list(tickers),
      start_date=initial_date,
      end_date=final_date
    )
  },
  tickers=y,
  initial_date=head(x,-1),
  final_date=tail(x,-1),
  SIMPLIFY=FALSE
))


w<-mapply(
  function(tickers,start,end){
    res<-Rblpapi::bdh(
      securities=tickers,
      fields=c(
        "DAY_TO_DAY_TOT_RETURN_GROSS_DVDS",
        "VOLATILITY_30D"
      ),
      start.date=as.Date(start,format="%Y%m%d"),
      end.date=as.Date(end,format="%Y%m%d"),
      include.non.trading.days=FALSE
    )
    res
  },
  tickers=z$tickers,
  start=z$start_date,
  end=z$end_date,
  SIMPLIFY=FALSE
)

w1<-w[mapply(class,w)=="list"]

w2<-do.call(c,w1)

w3<-w2[mapply(nrow,w2)>50]

w4<-do.call(rbind,mapply(function(df,ticker){
      data.table(
        ticker=ticker,
        date=df$date,
        tret=df$DAY_TO_DAY_TOT_RETURN_GROSS_DVDS,
        vol_30d=df$VOLATILITY_30D
      )
},df=w3,ticker=names(w3),SIMPLIFY=FALSE))

fwrite(w4,"N:/Depts/Share/UK Alpha Team/Analytics/test/hist_stock_data.csv")

