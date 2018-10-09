require(data.table)
require(magick)
require(Rtsne)
require(clue)
require(stringi)
require(ggplot2)
require(lars)

duke_pair_local_pnl<-load_matrix(
  "N:/Depts/Share/UK Alpha Team/Analytics/duke_summary/duke_pair_local_pnl.csv",
  row_names=TRUE
)

bar_intervals<-fread("N:/Depts/Share/UK Alpha Team/Analytics/market_data/bar_intervals.csv")

intraday_pair<-load_matrix(
  "N:/Depts/Share/UK Alpha Team/Analytics/market_data/intraday_pair.csv",
  row_names=TRUE
)[bar_intervals$day>0,]

intraday_pair_longs<-load_matrix(
  "N:/Depts/Share/UK Alpha Team/Analytics/market_data/intraday_pair_longs.csv",
  row_names=TRUE
)[bar_intervals$day>0,]

intraday_pair_shorts<-load_matrix(
  "N:/Depts/Share/UK Alpha Team/Analytics/market_data/intraday_pair_shorts.csv",
  row_names=TRUE
)[bar_intervals$day>0,]

intraday_perf<-load_matrix(
  "N:/Depts/Share/UK Alpha Team/Analytics/market_data/intraday_perf.csv",
  row_names=TRUE
)[bar_intervals$day>0,]

index_tickers<-colnames(intraday_perf) %>% 
{.[grepl("Index$",.)]} %>% 
  {setdiff(.,c("SGBVPHVE Index","SGBVPMEU Index","LYXRLSMN Index"))}

intraday_factor<-intraday_perf[,index_tickers]

cm<-cor(intraday_pair,intraday_factor)

duke<-unname(rowSums(intraday_pair))

mean(cor(intraday_pair))

x <- intraday_factor
y <- cbind(unname(rowSums(intraday_pair)))

multivar_reg <- t(cov(y, x) %*% solve(cov(x)))

explain<-x%*%multivar_reg

unexplained<-(y-explain)


x1<-intraday_factor[,c("MSEEGRW Index","MSEEVAL Index","MSEEMOMO Index")]

betas1 <- t(cov(y, x1) %*% solve(cov(x1)))

explain1<-x1%*%betas1

unexplained1<-(y-explain1)

betas2<-lars(x,y,"lasso")


