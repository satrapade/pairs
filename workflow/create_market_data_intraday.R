# 
# Open sheet, scrape positions + upload to bloomberg
# 
library(digest)
library(stringi)
library(readxl)
library(scales)
library(data.table)
library(Matrix)
library(Matrix.utils)
library(blpapi)
library(Rblpapi)
conn <- new(BlpApiConnection)
rcon<-Rblpapi::blpConnect()
append2log<-function(log_text,append=TRUE)
{
  cat(
    paste0(stri_trim(gsub("##|-","",capture.output(timestamp())))," : ",log_text,"\n"),
    file="N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/workflow.log",
    append=append
  )
}

append2log("create_market_data_intraday: source utility_functions, sheet_bbg_functions")
source("https://raw.githubusercontent.com/satrapade/utility/master/utility_functions.R")
source("https://raw.githubusercontent.com/satrapade/utility/master/sheet_bbg_functions.R")

data_loaded<-fread("N:/Depts/Share/UK Alpha Team/Analytics/market_data/data_loaded.csv")

sheet_ref_matrix<-load_matrix(data_loaded[name=="sheet_ref_matrix",fn],row_names=TRUE)
factor_ref_matrix<-load_matrix(data_loaded[name=="factor_ref_matrix",fn],row_names=TRUE)

all_tickers<-sort(unique(c(colnames(sheet_ref_matrix),colnames(factor_ref_matrix))))

append2log("create_market_data_intraday: fetch intraday for union of factor, ticker matrices")
bars<-mapply(function(ticker){
  res<-Rblpapi::getBars(
    ticker,
    barInterval = 10,
    startTime = Sys.time()-60*60*24*14,
    endTime = Sys.time()
  )
  data.table(ticker=rep(ticker,nrow(res)),res)
},all_tickers,SIMPLIFY=FALSE)

append2log("create_market_data_intraday: save market_data/intraday.csv")
intraday<-do.call(rbind,bars[mapply(nrow,bars)>0])
fwrite(intraday,"N:/Depts/Share/UK Alpha Team/Analytics/market_data/intraday.csv")
intraday<-fread("N:/Depts/Share/UK Alpha Team/Analytics/market_data/intraday.csv")
write(compress(intraday),"N:/Depts/Share/UK Alpha Team/Analytics/market_data/compressed_intraday.txt")

row_size2universe<-function(x,u){
  m<-matrix(0,nrow=length(u),ncol=ncol(x),dimnames=list(u,colnames(x)))
  i<-match(rownames(x),u)
  j<-match(colnames(x),colnames(m))
  m[i,j]<-as.matrix(x)
  m
}

mc<-function(x,p="*",rows=1:nrow(x),cols=which(grepl(p,colnames(x))))x[rows,cols,drop=FALSE]

make_bar_matrix<-function(x,minute_interval=10,extrapolate=TRUE){
  time_string<-stri_sub(gsub("T"," ",rownames(x)),1,-2)
  minutes<-rev(c(0,cumsum(as.numeric(diff(as.POSIXct(time_string))))))
  intervally<-seq(from=min(minutes),to=max(minutes),by=minute_interval)
  all_times<-as.character(rev(sort(unique(c(minutes,intervally)))))
  x0<-if(extrapolate){
    apply(x,2,replace_zero_with_last)
  }else{as.matrix(x)}
  x1<-structure(x0,dimnames=list(as.character(rev(minutes)),colnames(x0)))
  x2<-row_size2universe(x1,all_times)
  x3<-if(extrapolate){
    structure(apply(x2,2,replace_zero_with_last),dimnames=dimnames(x2))
  }else{x2}
  x4<-x3[as.character(intervally),]
  attributes(x4)$datetime<-as.character(as.POSIXct(min(time_string))+intervally*60)
  x4
}

append2log("create_market_data_intraday: compute intraday_open")
intraday_open<-local({
  res<-rename_colnames(dMcast(intraday,times~ticker,value.var="open"),"^ticker","")
  make_bar_matrix(res)
})

append2log("create_market_data_intraday: compute intraday_close")
intraday_close<-local({
  res<-rename_colnames(dMcast(intraday,times~ticker,value.var="close"),"^ticker","")
  make_bar_matrix(res)
})

append2log("create_market_data_intraday: compute intraday_high")
intraday_high<-local({
  res<-rename_colnames(dMcast(intraday,times~ticker,value.var="high"),"^ticker","")
  make_bar_matrix(res)
})

append2log("create_market_data_intraday: compute intraday_low")
intraday_low<-local({
  res<-rename_colnames(dMcast(intraday,times~ticker,value.var="low"),"^ticker","")
  make_bar_matrix(res)
})

append2log("create_market_data_intraday: compute intraday_perf")
intraday_perf<-structure(
  (tail(intraday_close,-1)-head(intraday_close,-1))/head(intraday_close,-1),
  dimnames=list(tail(rownames(intraday_close),-1),colnames(intraday_close)),
  datetime=tail(attributes(intraday_close)$datetime,-1)
)

append2log("create_market_data_intraday: compute bar_intervals")
bar_moves<-rowSums(abs(intraday_perf)>0)
bar_moves<-c(0,roll_mean(rowSums(abs(intraday_perf)>0),2))
bar_open<-bar_moves>ncol(intraday_perf)/4
bar_edge<-sign(diff(c(0,bar_open)))
bar_day<-data.table(
  open=seq_along(bar_edge)[bar_edge>0],
  close=seq_along(bar_edge)[bar_edge<0],
  day=seq_along(seq_along(bar_edge)[bar_edge>0])
)
bar_intervals<-data.table(
  date=stri_sub(attributes(intraday_perf)$datetime,1,10),
  time=stri_sub(attributes(intraday_perf)$datetime,12,-1),
  bar=seq_along(bar_moves),
  moves=bar_moves,
  edge=bar_edge,
  open=findInterval(seq_along(bar_moves),which(bar_edge>0)),
  close=findInterval(seq_along(bar_moves),which(bar_edge<0))
)[,c(.SD,.(
  day=ifelse(open>close,open,0)
))]

append2log("create_market_data_intraday: save market_data/intraday_perf.csv")
fwrite(data.table(intraday_perf),"N:/Depts/Share/UK Alpha Team/Analytics/market_data/intraday_perf.csv")
write(
  compress(intraday_perf),
  "N:/Depts/Share/UK Alpha Team/Analytics/market_data/compressed_intraday_perf.txt"
)

append2log("create_market_data_intraday: save market_data/bar_intervals.csv")
fwrite(data.table(bar_intervals),"N:/Depts/Share/UK Alpha Team/Analytics/market_data/bar_intervals.csv")

make_pair<-function(
  w,
  intraday_perf=get("intraday_perf",envir=.GlobalEnv)
){
  new_pair<-matrix(
    0,
    nrow=ncol(intraday_perf),
    ncol=1,
    dimnames=list(colnames(intraday_perf),NULL)
  )
  res<-do.call(rbind,mapply(function(n,w){
    i<-which(grepl(paste0("^",n),rownames(new_pair)))
    j<-rep(1,length(i))
    ij<-structure(
      cbind(i,j,rep(w,length(i))),
      dimnames=list(rownames(new_pair)[i],c("i","j","w"))
    )
    ij
  },n=names(w),w=w,SIMPLIFY=FALSE))
  new_pair[res[,c(1,2)]]<-res[,3]
  pnl<-structure(
    intraday_perf%*%new_pair,
    dimnames=list(rownames(intraday_perf),"new_pair")
  )
  structure(pnl,.Names=attributes(intraday_perf)$datetime)
}


pair_exposure<-local({
  res<-load_matrix(
    "N:/Depts/Share/UK Alpha Team/Analytics/duke_summary/duke_pair_exposure.csv",
    row_names=TRUE
  )
  res[rownames(res) %in% colnames(intraday_perf),]
})

manager_exposure<-local({
  res<-load_matrix(
    "N:/Depts/Share/UK Alpha Team/Analytics/duke_summary/duke_manager_exposure.csv",
    row_names=TRUE
  )
  res[rownames(res) %in% colnames(intraday_perf),]
})

append2log("create_market_data_intraday: compute intraday_pair")
intraday_pair<-structure(
  intraday_perf[,rownames(pair_exposure)]%*%pair_exposure,
  datetime=attributes(intraday_perf)$datetime
)
append2log("create_market_data_intraday: compute intraday_pair_longs")
intraday_pair_longs<-structure(
  intraday_perf[,rownames(pair_exposure)]%*%pmax(pair_exposure,0),
  datetime=attributes(intraday_perf)$datetime
)
append2log("create_market_data_intraday: compute intraday_pair_shorts")
intraday_pair_shorts<-structure(
  intraday_perf[,rownames(pair_exposure)]%*%pmin(pair_exposure,0),
  datetime=attributes(intraday_perf)$datetime
)
append2log("create_market_data_intraday: save market_data/intraday_pair.csv")
fwrite(data.table(intraday_pair),"N:/Depts/Share/UK Alpha Team/Analytics/market_data/intraday_pair.csv")
write(
  compress(intraday_pair),
  "N:/Depts/Share/UK Alpha Team/Analytics/market_data/compressed_intraday_pair.txt"
)


append2log("create_market_data_intraday: market_data/intraday_pair_longs.csv")
fwrite(data.table(intraday_pair_longs),"N:/Depts/Share/UK Alpha Team/Analytics/market_data/intraday_pair_longs.csv")
write(
  compress(intraday_pair_longs),
  "N:/Depts/Share/UK Alpha Team/Analytics/market_data/compressed_intraday_pair_longs.txt"
)

append2log("create_market_data_intraday: market_data/intraday_pair_shorts.csv")
fwrite(data.table(intraday_pair_shorts),"N:/Depts/Share/UK Alpha Team/Analytics/market_data/intraday_pair_shorts.csv")
write(
  compress(intraday_pair_shorts),
  "N:/Depts/Share/UK Alpha Team/Analytics/market_data/compressed_intraday_pair_shorts.txt"
)



