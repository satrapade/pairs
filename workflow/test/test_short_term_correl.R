require(digest)
require(stringi)
require(readxl)
require(scales)
require(data.table)
require(Matrix)
require(Matrix.utils)
require(clue)
require(Rtsne)

source("https://raw.githubusercontent.com/satrapade/utility/master/utility_functions.R")


intraday<-fread("N:/Depts/Share/UK Alpha Team/Analytics/market_data/intraday.csv")

row_size2universe<-function(x,u){
  m<-matrix(0,nrow=length(u),ncol=ncol(x),dimnames=list(u,colnames(x)))
  i<-match(rownames(x),u)
  j<-match(colnames(x),colnames(m))
  m[i,j]<-as.matrix(x)
  m
}

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


intraday_open<-local({
  res<-rename_colnames(dMcast(intraday,times~ticker,value.var="open"),"^ticker","")
  make_bar_matrix(res)
})

intraday_close<-local({
  res<-rename_colnames(dMcast(intraday,times~ticker,value.var="close"),"^ticker","")
  make_bar_matrix(res)
})

intraday_high<-local({
  res<-rename_colnames(dMcast(intraday,times~ticker,value.var="high"),"^ticker","")
  make_bar_matrix(res)
})

intraday_low<-local({
  res<-rename_colnames(dMcast(intraday,times~ticker,value.var="low"),"^ticker","")
  make_bar_matrix(res)
})


intraday_perf<-structure(
  (tail(intraday_close,-1)-head(intraday_close,-1))/head(intraday_close,-1),
  dimnames=list(tail(rownames(intraday_close),-1),colnames(intraday_close)),
  datetime=tail(attributes(intraday_close)$datetime,-1)
)

bar_moves<-c(0,roll_mean(rowSums(abs(intraday_perf)>0),2))
bar_open<-bar_moves>ncol(intraday_perf)/4
bar_edge<-sign(diff(c(0,bar_open)))
bar_day<-data.table(
  open=seq_along(bar_edge)[bar_edge>0],
  close=seq_along(bar_edge)[bar_edge<0],
  day=seq_along(seq_along(bar_edge)[bar_edge>0])
)
bar_intervals<-data.table(
  bar=seq_along(bar_moves),
  moves=bar_moves,
  edge=bar_edge,
  open=findInterval(seq_along(bar_moves),which(bar_edge>0)),
  close=findInterval(seq_along(bar_moves),which(bar_edge<0))
)[,c(.SD,.(day=ifelse(open>close,open,0)))]



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

intraday_pair<-structure(
  intraday_perf[,rownames(pair_exposure)]%*%pair_exposure,
  datetime=attributes(intraday_perf)$datetime
)

intraday_pair_longs<-structure(
  intraday_perf[,rownames(pair_exposure)]%*%pmax(pair_exposure,0),
  datetime=attributes(intraday_perf)$datetime
)

intraday_pair_shorts<-structure(
  intraday_perf[,rownames(pair_exposure)]%*%pmin(pair_exposure,0),
  datetime=attributes(intraday_perf)$datetime
)

intraday_pair_correlation<-mapply(function(i){
  bars<-sort(bar_intervals$bar[bar_intervals$day==i])
  live_pairs<-which(apply(intraday_pair[bars,,drop=FALSE],2,sd)>0)
  cm<-cor(intraday_pair[bars,live_pairs])
  correlations<-cm[which(row(cm)>col(cm),arr.ind = TRUE)]
  correlations
},1:max(bar_intervals$day),SIMPLIFY=FALSE)

intraday_pair_pnl<-mapply(function(i){
  bars<-sort(bar_intervals$bar[bar_intervals$day==i])
  live_pairs<-which(apply(intraday_pair[bars,,drop=FALSE],2,sd)>0)
  intraday_pair[bars,live_pairs]
},1:max(bar_intervals$day),SIMPLIFY=FALSE)



