
require(data.table)
require(magrittr)

pair_subset<-function(
  manager="*",
  cutoff="2018-06-01",
  filter=grepl(manager,bucket),
  dataset=duke
)(
dataset[filter] %>%
  {.$pair<-gsub("_PAIR_","",.$bucket);.} %>%
  {.$bucket<-NULL;.} %>% 
  {.[!is.na(gross),.(
    initial_date=min(date),
    final_date=max(date),
    duration=date%>%{c(min(.),max(.))}%>%as.Date(format="%Y-%m-%d")%>%as.integer%>%diff,
    peak_date=date[which.max(cumprod(1+scrub(pnl)))],
    peak_pnl=round(10000*max(cumprod(1+scrub(pnl))-1),digits=1),
    last_pnl=round(10000*(cumprod(1+scrub(pnl))-1)[which(date==max(date))],digits=1),
    start_date=as.Date(min(date),format="%Y-%m-%d"),
    end_date=as.Date(max(date),format="%Y-%m-%d"),
    final_gross=round(10000*gross[which(date==max(date))],digits=1),
    final_net=round(10000*net[which(date==max(date))],digits=1),
    final_pnl_draw=round(10000*pnl_draw[which(date==max(date))],digits=1),
    final_pnl_ltd=round(10000*pnl_ltd[which(date==max(date))],digits=1),
    final_pnl_ytd=round(10000*pnl_ytd[which(date==max(date))],digits=1),
    final_pnl_mtd=round(10000*pnl_mtd[which(date==max(date))],digits=1),
    final_pnl_rolling=round(10000*pnl_rolling[which(date==max(date))],digits=1)
  ),keyby=pair]}
)[final_date>=cutoff]




