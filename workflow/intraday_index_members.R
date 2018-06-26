# 
# Intraday tickers
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

source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/append2log.R")

append2log("intraday_index_members: source utility_functions, sheet_bbg_functions")
source("https://raw.githubusercontent.com/satrapade/utility/master/utility_functions.R")
source("https://raw.githubusercontent.com/satrapade/utility/master/sheet_bbg_functions.R")


append2log("intraday_index_members: fetch index memebers")
indices<-c("SX8P Index","SX5E Index","SX7E Index","SX7P Index","SXIP Index","SXIE Index")

get_index_members<-function(ndx){
  ndx<-stri_trim(ndx)
  if(!grepl("Index$",ndx))ndx<-paste0(ndx," Index")
  bbg_res<-Rblpapi::bds(ndx,"INDX_MWEIGHT")
  data.table(
    index=rep(ndx,nrow(bbg_res)),
    ticker=paste(bbg_res[["Member Ticker and Exchange Code"]],"Equity")
  )
}

index_membership<-do.call(rbind,mapply(get_index_members,indices,SIMPLIFY = FALSE))

append2log("intraday_index_members: save market_data/index_membership.csv ")
fwrite(index_membership,"N:/Depts/Share/UK Alpha Team/Analytics/market_data/index_membership.csv")


all_tickers<-sort(unique(index_membership$ticker))

append2log("intraday_index_members: fetch intraday for index members")
bars<-mapply(function(ticker){
  res<-Rblpapi::getBars(
    ticker,
    barInterval = 10,
    startTime = Sys.time()-60*60*24*21,
    endTime = Sys.time()
  )
  data.table(ticker=rep(ticker,nrow(res)),res)
},all_tickers,SIMPLIFY=FALSE)

append2log("intraday_index_members: save market_data/intraday_index_members.csv")
intraday<-do.call(rbind,bars[mapply(nrow,bars)>0])
fwrite(intraday,"N:/Depts/Share/UK Alpha Team/Analytics/market_data/intraday_index_members.csv")

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

append2log("intraday_index_members: compute intraday_open")
intraday_open<-local({
  res<-rename_colnames(dMcast(intraday,times~ticker,value.var="open"),"^ticker","")
  make_bar_matrix(res)
})

append2log("intraday_bank_pairs: compute intraday_close")
intraday_close<-local({
  res<-rename_colnames(dMcast(intraday,times~ticker,value.var="close"),"^ticker","")
  make_bar_matrix(res)
})

append2log("intraday_index_members: compute intraday_high")
intraday_high<-local({
  res<-rename_colnames(dMcast(intraday,times~ticker,value.var="high"),"^ticker","")
  make_bar_matrix(res)
})

append2log("intraday_index_members: compute intraday_low")
intraday_low<-local({
  res<-rename_colnames(dMcast(intraday,times~ticker,value.var="low"),"^ticker","")
  make_bar_matrix(res)
})

append2log("intraday_index_members: compute intraday_perf")
intraday_perf<-structure(
  (tail(intraday_close,-1)-head(intraday_close,-1))/head(intraday_close,-1),
  dimnames=list(tail(rownames(intraday_close),-1),colnames(intraday_close)),
  datetime=tail(attributes(intraday_close)$datetime,-1)
)

append2log("intraday_index_members: compute bar_intervals")
bar_moves<-rowSums(abs(intraday_perf)>0)
bar_moves<-c(0,roll_mean(rowSums(abs(intraday_perf)>0),2))
bar_open<-c(head(bar_moves>ncol(intraday_perf)/4,-1),FALSE)
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

append2log("intraday_index_members: save market_data/intraday_perf_index_members.csv")
fwrite(data.table(intraday_perf),"N:/Depts/Share/UK Alpha Team/Analytics/market_data/intraday_perf_index_members.csv")

append2log("intraday_index_members: save market_data/intraday_open_index_members.csv")
fwrite(data.table(intraday_open),"N:/Depts/Share/UK Alpha Team/Analytics/market_data/intraday_open_index_members.csv")

append2log("intraday_index_members: save market_data/intraday_close_index_members.csv")
fwrite(data.table(intraday_close),"N:/Depts/Share/UK Alpha Team/Analytics/market_data/intraday_close_index_members.csv")

append2log("intraday_index_members: save market_data/bar_intervals_index_members.csv")
fwrite(data.table(bar_intervals),"N:/Depts/Share/UK Alpha Team/Analytics/market_data/bar_intervals_index_members.csv")

