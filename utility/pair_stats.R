
require(data.table)
require(magrittr)

pair_stats<-function(manager="*",cutoff="2018-06-01"){
  res<-pair_subset(manager=manager,cutoff=cutoff)
  res0<-res[,c("pair","final_pnl_mtd","final_pnl_ytd","final_pnl_ltd")]
  res1<-mapply(function(x)data.table(
    SUM=sum(x),
    HITRATIO=round(100*sum(pmax(sign(x),0))/max(length(x),1),digits=1),
    WINLOSSRATIO=round(100*scrub(sum(pmax(x,0))/sum(pmax(-x,0))),digits=1)
  ),res0[,c("final_pnl_mtd","final_pnl_ytd","final_pnl_ltd")]) %>%
  {data.table(pair=rownames(.),.)}
  res2<-rbind(res0,res1)
  res2$status<-c(
    ifelse(
      res$final_date<max(res$final_date),
      paste0("{\\tt CLOSE} ",res$final_date),
      ifelse(
        res$initial_date>=cutoff,
        paste0("{\\tt OPEN} ",res$initial_date),
        paste0("{\\tt LIVE} ",res$initial_date)
      )
    ),
    rep("",nrow(res1))
  )
  res2$gross<-c(as.character(round(res$final_gross,digits=1)),rep("",nrow(res1)))
  res2$net<-c(as.character(round(res$final_net,digits=1)),rep("",nrow(res1)))
  attributes(res2[["final_pnl_mtd"]])$format<-quote(sign_color(n_fmt(this),this))
  attributes(res2[["final_pnl_ytd"]])$format<-quote(sign_color(n_fmt(this),this))
  attributes(res2[["final_pnl_ltd"]])$format<-quote(sign_color(n_fmt(this),this))
  attributes(res2[["gross"]])$format<-quote(sign_color(n_fmt(this),this))
  attributes(res2[["net"]])$format<-quote(sign_color(n_fmt(this),this))
  res2
}
