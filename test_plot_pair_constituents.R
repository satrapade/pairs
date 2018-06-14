

# 
# A <- data.table(date = rep(1:10,each=10),  ticker=rep(1:10,times=10))
# B <- data.table(date = 1:4, ticker= rep(1,4), value1 = 1:4, value2=rep(6,4), value4=rep("tt",4))
# B[A,on=c("ticker","date")][ticker==1]
# 
# dates<-ptf[bucket=="ABC_PAIR_06",.(
#   date=rep(sort(unique(date)),times=length(unique(ticker))),
#   bucket=rep(unique(bucket),times=length(unique(ticker))*length(unique(date))),
#   ticker=rep(sort(unique(ticker)),each=length(unique(date)))
# )]
# 
# x<-ptf[bucket=="ABC_PAIR_06"][dates,on=c("bucket","ticker","date")]
# 
# 
source("https://raw.githubusercontent.com/satrapade/utility/master/nn_cast.R")

securities<-fread(
  "N:/Depts/Share/UK Alpha Team/Analytics/db_cache/exposure_securities.csv"
)[TRUE,.SD,keyby=exposure_security_external_id]


ptf<-fread("N:/Depts/Share/UK Alpha Team/Analytics/db_cache/ptf.csv")[date>"2017-01-01"]
ptf$date<-as.Date(ptf$date,format="%Y-%m-%d")
i<-match(ptf$security,securities$exposure_security_external_id)
ptf$ticker<-gsub(" Equity","",securities$security_ticker[i])


px_close<-NNcast(
  data=fread("N:/Depts/Share/UK Alpha Team/Analytics/db_cache/px_last.csv")[date>"2017-01-01",.(
    date=date,
    ticker=gsub(
      " Equity",
      "",
      securities$security_ticker[match(stock,securities$exposure_security_external_id)]
    ),
    value=value
  )],
  i_name="date",
  j_name="ticker",
  v_name="value"
)

scale2unit<-function(x)rescale(x,from=c(-1,1)*max(1,max(abs(scrub(x)))),to=c(-1,1))

#
# plot pair constituents, price action
#
plot_pair_constituents<-function(
  the_pair="ABC_PAIR_06",
  the_ptf=get("ptf",parent.frame()),
  line_width=0.25
){
  
  the_dates<-ptf[bucket==the_pair,.(
    date=rep(sort(unique(date)),times=length(unique(ticker))),
    bucket=rep(unique(bucket),times=length(unique(ticker))*length(unique(date))),
    ticker=rep(sort(unique(ticker)),each=length(unique(date)))
  )]
  
  df<-ptf[
    bucket==the_pair
  ][
    the_dates,
    on=c("bucket","ticker","date")
  ][TRUE,.(
    bucket=bucket,
    date=date,
    ticker=ticker,
    security_units=scrub(security_units),
    market_value=scrub(market_value),
    close=px_close[cbind(
      match(as.character(date,format="%Y-%m-%d"),rownames(px_close)),
      match(ticker,colnames(px_close))
    )],
    tret=scrub(tret)
  )][TRUE,.(
    date=sort(date),
    bucket=bucket,
    security_units=scale2unit(scrub(security_units))[order(date)],
    market_value=scale2unit(scrub(market_value))[order(date)],
    close=rescale(close[order(date)],to=c(-1,1)),
    tret=local({
      i<-order(date)
      scale2unit(cumsum((scrub(market_value[i])*scrub(tret[i])/100)))
    })
  ),keyby=ticker]
  
  dfm <- melt(
    df,
    id.vars=c("date","bucket","ticker"),
    measure.vars=list(
      what=c("market_value","close","tret"),
      pos=rep("security_units",3)
    ),
    variable.name="select"
  )
  
  g1<-ggplot(
    data=dfm,
    mapping=aes(
      x=date,
      y=what,
      group=interaction(ticker,select),
      color=ticker
    )
  ) +
    geom_hline(aes(yintercept=0),size=0.25,color=rgb(0,0,0,0.5),show.legend=FALSE) +
    geom_line(size=line_width)+
    scale_colour_discrete(name=the_pair) +
    scale_x_date(
      labels = date_format("%Y-%m-%d"),
      breaks = pretty(sort(unique(expanded_ptf$date)), n = 15)) +
    theme(axis.text.x=element_text(angle=50,size=6,vjust=0.5))
  
  g2 <-   g1+facet_grid(
    cols=vars(select),
    rows=vars(ticker),
    labeller=labeller(select=c("1"="market_value","2"="close","3"="tret"))
  )
  
  g2
}




