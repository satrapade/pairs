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

portfolio_local_tret<-load_matrix("N:/Depts/Share/UK Alpha Team/Analytics/market_data/portfolio_local_tret.csv",row_names=TRUE)

append2log("create_tsne_grid: compute price_plots bitmaps")
price_plots<-mapply(
    function(i){
      x<-seq_along(portfolio_local_tret[,i])
      fig_plot <- image_graph(width=256, height=256, res = 300,bg=rgb(1,1,1,0),antialias = FALSE)
      par(mai=c(0,0,0,0))
      plot(x=range(x),y=c(0,1),type="n",axes=FALSE,main="",xlab="",ylab="")
      points(x=x,y=rescale(pmax(portfolio_local_tret[,i],0),c(0,0.5)),pch=19,col=rgb(0.66,1.00,0.66),cex=0.1)
      points(x=x,y=rescale(pmax(-portfolio_local_tret[,i],0),c(1,0.5)),pch=19,col=rgb(1.00,0.66,0.66),cex=0.1)
      lines(x=x,y=rescale(cumsum(portfolio_local_tret[,i]),c(0,1)),col="black",lwd=2)
      dev.off()
      as.raster(fig_plot)
  },
  colnames(portfolio_local_tret),
  SIMPLIFY=FALSE
)

append2log("create_tsne_grid: compute price_plot_feature")
price_plot_feature<-t(mapply(function(p){
  as.vector(col2rgb(p))
},price_plots))

append2log("create_tsne_grid: compute tSNE layout ")
tsne<-Rtsne(price_plot_feature,check_duplicates = FALSE)
append2log("create_tsne_grid: compute grid layout ")
tsne_grid<-force2grid(data.table(x=tsne$Y[,1],y=tsne$Y[,2]),col_slack = 5,row_slack = 5)

append2log("create_tsne_grid: save app_data/tsne.csv ")
fwrite(
  data.table(
    instrument=names(price_plots),
    x=tsne$Y[,1],
    y=tsne$Y[,2]
  ),
  "N:/Depts/Share/UK Alpha Team/Analytics/app_data/tsne.csv"
)

append2log("create_tsne_grid: save app_data/tsne_grid.csv ")
fwrite(
  data.table(
    instrument=names(price_plots),
    x=tsne_grid$x,
    y=tsne_grid$y
  ),
  "N:/Depts/Share/UK Alpha Team/Analytics/app_data/tsne_grid.csv"
)

append2log("create_tsne_grid: save app_data/price_plots.bin ")
fdump(price_plots,"N:/Depts/Share/UK Alpha Team/Analytics/app_data/price_plots.bin")




