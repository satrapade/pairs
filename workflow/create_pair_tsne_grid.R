require(data.table)
require(magick)
require(Rtsne)
require(clue)
require(stringi)
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

duke_pair_local_pnl<-load_matrix(
  "N:/Depts/Share/UK Alpha Team/Analytics/duke_summary/duke_pair_local_pnl.csv",
  row_names=TRUE
)

bar_intervals<-fread("N:/Depts/Share/UK Alpha Team/Analytics/market_data/bar_intervals.csv")

intraday_pair<-load_matrix(
  "N:/Depts/Share/UK Alpha Team/Analytics/market_data/intraday_pair.csv",
  row_names=TRUE
)[bar_intervals$day>0,]


append2log("create_pair_tsne_grid: compute price_plots bitmaps")
pair_price_plots<-mapply(
    function(i){
      x<-seq_along(intraday_pair[,i])
      fig_plot <- image_graph(width=256, height=256, res = 300,bg=rgb(1,1,1,0),antialias = FALSE)
      par(mai=c(0,0,0,0))
      plot(x=range(x),y=c(0,1),type="n",axes=FALSE,main="",xlab="",ylab="")
      #points(x=x,y=rescale(pmax(intraday_pair[,i],0),c(0,0.5)),pch=19,col=rgb(0.66,1.00,0.66),cex=0.1)
      #points(x=x,y=rescale(pmax(-intraday_pair[,i],0),c(1,0.5)),pch=19,col=rgb(1.00,0.66,0.66),cex=0.1)
      lines(x=x,y=rescale(cumsum(intraday_pair[,i]),c(0,1)),col="blue",lwd=3)
      dev.off()
      as.raster(fig_plot)
  },
  colnames(intraday_pair),
  SIMPLIFY=FALSE
)

append2log("create_pair_tsne_grid: compute price_plot_feature")
pair_price_plot_feature<-t(mapply(function(p){
  as.vector(col2rgb(p))
},pair_price_plots))

append2log("create_pair_tsne_grid: compute tSNE layout ")
pair_tsne<-Rtsne(pair_price_plot_feature,check_duplicates = FALSE)
append2log("create_pair_tsne_grid: compute grid layout ")
pair_tsne_grid<-force2grid(
  data.table(x=pair_tsne$Y[,1],y=pair_tsne$Y[,2]),
  col_slack = 10,
  row_slack = 10
)

append2log("create_pair_tsne_grid: save app_data/pair_tsne.csv ")
fwrite(
  data.table(
    instrument=names(pair_price_plots),
    x=pair_tsne$Y[,1],
    y=pair_tsne$Y[,2]
  ),
  "N:/Depts/Share/UK Alpha Team/Analytics/app_data/pair_tsne.csv"
)


pair_tsne_grid_df<-data.table(
    instrument=names(pair_price_plots),
    x=pair_tsne_grid$x,
    y=pair_tsne_grid$y
  )[,.SD,keyby=instrument]

append2log("create_pair_tsne_grid: save app_data/pair_tsne_grid.csv ")
fwrite(
  pair_tsne_grid_df,
  "N:/Depts/Share/UK Alpha Team/Analytics/app_data/pair_tsne_grid.csv"
)

append2log("create_pair_tsne_grid: save app_data/pair_price_plots.bin ")
fdump(pair_price_plots,"N:/Depts/Share/UK Alpha Team/Analytics/app_data/pair_price_plots.bin")


par(mai=c(0,0,0,0))

plot(
  x=c(1-0.5,max(pair_tsne_grid_df$x)+0.5),
  y=c(1-0.5,max(pair_tsne_grid_df$y)+0.5),
  type="n",
  axes=FALSE,
  xlab="",
  ylab=""
)

abline(v=seq(
  from=min(pair_tsne_grid_df$x)-0.5,
  to=max(pair_tsne_grid_df$x)+0.5,
  by=1
),col=rgb(0.5,0.5,0.5,1))

abline(h=seq(
  from=min(pair_tsne_grid_df$y)-0.5,
  to=max(pair_tsne_grid_df$y)+0.5,
  by=1
),col=rgb(0.5,0.5,0.5,1))

for(i in pair_tsne_grid_df$instrument){
  x<-pair_tsne_grid_df[i,x]
  y<-pair_tsne_grid_df[i,y]
  rasterImage(pair_price_plots[[i]],x-0.5,y-0.45,x+0.5,y+0.4)
}

text(
  x=pair_tsne_grid_df$x,
  y=pair_tsne_grid_df$y+0.45,
  labels=pair_tsne_grid_df$instrument,
  cex=0.33,
  col="black"
)






