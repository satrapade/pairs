require(ggplot2)

get_facet_row_titles<-function(g1){
  g2<-ggplot_gtable(ggplot_build(g1))
  stripr <- which(grepl('strip-r', g2$layout$name))
  facet_titles<-rep("",length(stripr))
  for (ndx in seq_along(stripr)) {
    i <-stripr[ndx]
    j <- which(grepl('rect', g2$grobs[[i]]$grobs[[1]]$childrenOrder))
    k <- which(grepl('title', g2$grobs[[i]]$grobs[[1]]$childrenOrder))
    facet_titles[ndx]<-pair<-g2$grobs[[i]]$grobs[[1]]$children[[k]]$children[[1]][[1]]
  }
  facet_titles
}

