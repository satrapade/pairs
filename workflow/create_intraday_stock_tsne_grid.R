require(data.table)
require(magick)
require(Rtsne)
require(clue)
require(stringi)
require(magrittr)

append2log<-function(log_text,append=TRUE)
{
  cat(
    paste0(stri_trim(gsub("##|-","",capture.output(timestamp())))," : ",log_text,"\n"),
    file="N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/workflow.log",
    append=append
  )
}

append2log("create_tsne_grid: source utility_functions, plot_utility_functions")
source("https://raw.githubusercontent.com/satrapade/latex_utils/master/utility_functions.R")
source("https://raw.githubusercontent.com/satrapade/utility/master/plot_utility_functions.R")


bar_intervals<-fread("N:/Depts/Share/UK Alpha Team/Analytics/market_data/bar_intervals.csv")

intraday_perf<-load_matrix(
  "N:/Depts/Share/UK Alpha Team/Analytics/market_data/intraday_perf.csv",
  row_names=TRUE
)[bar_intervals$day>0,]

equity_tickers<-colnames(intraday_perf) %>% {.[grepl("Equity$",.)]}
index_tickers<-colnames(intraday_perf) %>% {.[grepl("Index$",.)]}

equity_matrix<-apply(intraday_perf[,equity_tickers],2,function(x)cumsum(x/sd(x)))
index_matrix<-intraday_perf[,index_tickers]

pair_tsne<-Rtsne(
  X=t(equity_matrix),
  parplexity=300,
  check_duplicates = FALSE
)

pair_tsne_grid<-force2grid(
  data.table(x=pair_tsne$Y[,1],y=pair_tsne$Y[,2]),
  col_slack = 10,
  row_slack = 10
)


pair_tsne_grid_df<-data.table(
    instrument=equity_tickers,
    x=pair_tsne_grid$x,
    y=pair_tsne_grid$y
  )[,.SD,keyby=instrument]


plot_fraction<-function(a){
  par(mai=c(0,0,0,0))
  plot(
    x=c(1-0.5,max(pair_tsne_grid_df$x)+0.5),
    y=c(1-0.5,max(pair_tsne_grid_df$y)+0.5),
    type="n",
    axes=FALSE,
    xlab="",
    ylab=""
  )
  text(
    x=pair_tsne_grid_df$x,
    y=pair_tsne_grid_df$y,
    labels=gsub(" Equity","",pair_tsne_grid_df$instrument),
    cex=0.20,
    col=ifelse(apply(head(equity_matrix,nrow(equity_matrix)*a),2,sum)>0,"green","red")
  )
}




