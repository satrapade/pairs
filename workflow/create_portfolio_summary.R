# 
# Open sheed, scrape positions + upload to bloomberg
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

append2log("create_portfolio_summary: sourcing utility_functions, sheet_bbg_functions") 
source("https://raw.githubusercontent.com/satrapade/utility/master/utility_functions.R")
source("https://raw.githubusercontent.com/satrapade/utility/master/sheet_bbg_functions.R")


the_luke_portfolio<-fread("N:/Depts/Share/UK Alpha Team/Analytics/sheet_scrape/luke_portfolio.csv")
the_duke_portfolio<-fread("N:/Depts/Share/UK Alpha Team/Analytics/sheet_scrape/duke_portfolio.csv")
scrape_details<-fread("N:/Depts/Share/UK Alpha Team/Analytics/sheet_scrape/scrape_details.csv")
data_loaded<-fread("N:/Depts/Share/UK Alpha Team/Analytics/market_data/data_loaded.csv")


for(i in 1:nrow(data_loaded)){
  if(data_loaded$class[i]!="matrix")next;
  append2log(paste0("create_portfolio_summary: loading ",data_loaded$name[i]))
  assign(data_loaded$name[i],load_matrix(data_loaded$fn[i],row_names = TRUE))
}

row_size2universe<-function(x,u){
  m<-matrix(0,nrow=length(u),ncol=ncol(x),dimnames=list(u,colnames(x)))
  i<-match(rownames(x),u)
  j<-match(colnames(x),colnames(m))
  m[i,j]<-as.matrix(x)
  m
}

portfolio_30d_volume<-apply(tail(portfolio_gbp_volume,30),2,function(x){
  mean(nz(x))
})

the_luke_portfolio$days_volume<-abs(the_luke_portfolio$Exposure/portfolio_30d_volume[the_luke_portfolio$Ticker])
the_duke_portfolio$days_volume<-abs(the_duke_portfolio$Exposure/portfolio_30d_volume[the_duke_portfolio$Ticker])

the_luke_AUM<-as.numeric(scrape_details[,.SD,keyby=names]["luke_aum",values])
the_duke_AUM<-as.numeric(scrape_details[,.SD,keyby=names]["duke_aum",values])


e1<-new.env()

#
# portfolio summary calculations
#

res<-eval(expression({
  
  append2log("create_portfolio_summary: compute LUKE luke_pair_exposure matrix") 
  luke_pair_exposure<-as.matrix(rename_colnames(
    row_size2universe(
      dMcast(the_luke_portfolio,Ticker~Pair,value.var="Exposure"),
      colnames(portfolio_local_tret)
    )/as.numeric(scrape_details[,.SD,keyby=names]["luke_aum",values]),
    "^Pair",
    ""
  ))
  append2log("create_portfolio_summary: compute DUKE luke_pair_exposure matrix") 
  duke_pair_exposure<-as.matrix(rename_colnames(
    row_size2universe(
      dMcast(the_duke_portfolio,Ticker~Pair,value.var="Exposure"),
      colnames(portfolio_local_tret)
    )/as.numeric(scrape_details[,.SD,keyby=names]["duke_aum",values]),
    "^Pair",
    ""
  ))
  
  append2log("create_portfolio_summary: compute LUKE luke_pair_days (volume) matrix") 
  luke_pair_days<-as.matrix(rename_colnames(
    row_size2universe(dMcast(the_luke_portfolio,Ticker~Pair,value.var="days_volume"),colnames(portfolio_local_tret)),
    "^Pair",
    ""
  ))
  
  append2log("create_portfolio_summary: compute DUKE luke_pair_days (volume) matrix") 
  duke_pair_days<-as.matrix(rename_colnames(
    row_size2universe(dMcast(the_duke_portfolio,Ticker~Pair,value.var="days_volume"),colnames(portfolio_local_tret)),
    "^Pair",
    ""
  ))
  
  append2log("create_portfolio_summary: compute LUKE manager exposure matrix") 
  luke_manager_exposure<-as.matrix(rename_colnames(
    row_size2universe(dMcast(the_luke_portfolio,Ticker~Manager,value.var="Exposure"),colnames(portfolio_local_tret))/the_luke_AUM,
    "^Manager",
    ""
  ))
  
  append2log("create_portfolio_summary: compute DUKE manager exposure matrix") 
  duke_manager_exposure<-as.matrix(rename_colnames(
    row_size2universe(dMcast(the_duke_portfolio,Ticker~Manager,value.var="Exposure"),colnames(portfolio_local_tret))/the_duke_AUM,
    "^Manager",
    ""
  ))
  
  luke_exposure <- luke_manager_exposure%*%matrix(1,ncol=1,nrow=ncol(luke_manager_exposure))
  luke_long_exposure <- pmax(luke_manager_exposure,0)%*%matrix(1,ncol=1,nrow=ncol(luke_manager_exposure))
  luke_short_exposure <- pmax(-luke_manager_exposure,0)%*%matrix(1,ncol=1,nrow=ncol(luke_manager_exposure))
  
  duke_exposure<-duke_manager_exposure%*%matrix(1,ncol=1,nrow=ncol(duke_manager_exposure))
  duke_long_exposure <- pmax(duke_manager_exposure,0)%*%matrix(1,ncol=1,nrow=ncol(duke_manager_exposure))
  duke_short_exposure <- pmax(-duke_manager_exposure,0)%*%matrix(1,ncol=1,nrow=ncol(duke_manager_exposure))
  
  
  append2log("create_portfolio_summary: compute DUKE drop_one_pair_pnl matrix") 
  duke_drop_one_pair_pnl<-portfolio_local_tret%*%(duke_pair_exposure%*%(1-diag(ncol(duke_pair_exposure))))
  colnames(duke_drop_one_pair_pnl)<-colnames(duke_pair_exposure)

  append2log("create_portfolio_summary: compute LUKE drop_one_pair_pnl matrix") 
  luke_drop_one_pair_pnl<-portfolio_local_tret%*%(luke_pair_exposure%*%(1-diag(ncol(luke_pair_exposure))))
  colnames(luke_drop_one_pair_pnl)<-colnames(luke_pair_exposure)
  
  append2log("create_portfolio_summary: compute DUKE drop_one_manager_pnl matrix") 
  duke_drop_one_manager_pnl<-portfolio_local_tret%*%(duke_manager_exposure%*%(1-diag(ncol(duke_manager_exposure))))
  luke_drop_one_manager_pnl<-portfolio_local_tret%*%(luke_manager_exposure%*%(1-diag(ncol(luke_manager_exposure))))
  
  
  luke_local_pnl<-portfolio_local_tret%*%(luke_pair_exposure%*%matrix(1,ncol=1,nrow=ncol(luke_pair_exposure)))
  duke_local_pnl<-portfolio_local_tret%*%(duke_pair_exposure%*%matrix(1,ncol=1,nrow=ncol(duke_pair_exposure)))
  
  append2log("create_portfolio_summary: compute LUKE pair_local_pnl matrix") 
  luke_pair_local_pnl<-portfolio_local_tret%*%luke_pair_exposure
  append2log("create_portfolio_summary: compute LUKE pair_long_pnl matrix") 
  luke_pair_long_pnl<-portfolio_local_tret%*%pmax(luke_pair_exposure,0)
  append2log("create_portfolio_summary: compute LUKE pair_short_pnl matrix") 
  luke_pair_short_pnl<-portfolio_local_tret%*%pmax(-luke_pair_exposure,0)
  
  append2log("create_portfolio_summary: compute DUKE pair_local_pnl matrix") 
  duke_pair_local_pnl<-portfolio_local_tret%*%duke_pair_exposure
  append2log("create_portfolio_summary: compute DUKE pair_long_pnl matrix") 
  duke_pair_long_pnl<-portfolio_local_tret%*%pmax(duke_pair_exposure,0)
  append2log("create_portfolio_summary: compute DUKE pair_short_pnl matrix") 
  duke_pair_short_pnl<-portfolio_local_tret%*%pmax(-duke_pair_exposure,0)
  
  luke_manager_local_pnl<-portfolio_local_tret%*%luke_manager_exposure
  duke_manager_local_pnl<-portfolio_local_tret%*%duke_manager_exposure
  
  luke_manager_drop_one_pair_pnl<-structure(
    luke_manager_local_pnl[,pair2pm(colnames(luke_pair_local_pnl))]-luke_pair_local_pnl,
    dimnames=dimnames(luke_pair_local_pnl)
  )
  duke_manager_drop_one_pair_pnl<-structure(
    duke_manager_local_pnl[,pair2pm(colnames(duke_pair_local_pnl))]-duke_pair_local_pnl,
    dimnames=dimnames(duke_pair_local_pnl)
  )
  
  luke_gbp_pnl<-as.matrix(portfolio_gbp_tret%*%t(t(apply(luke_pair_exposure,1,sum))))
  luke_pair_gbp_pnl<-as.matrix(portfolio_gbp_tret%*%luke_pair_exposure)
  luke_manager_gbp_pnl<-as.matrix(portfolio_gbp_tret%*%luke_manager_exposure)
  
  manager_col<-assign_color(colnames(e1$luke_manager_local_pnl),col_alpha=1)[,.SD,keyby=item]
  pair_col<-assign_color(colnames(e1$luke_pair_local_pnl),col_alpha=1)[,.SD,keyby=item]
  
  
}),envir=e1)

data_loaded<-data.table(
  name=ls(e1),
  class=mapply(function(v)class(e1[[v]]),ls(e1)),
  rows=ifelse(mapply(function(v)class(e1[[v]]),ls(e1))=="matrix",mapply(function(v)nrow(e1[[v]]),ls(e1)),0),
  cols=ifelse(mapply(function(v)class(e1[[v]]),ls(e1))=="matrix",mapply(function(v)ncol(e1[[v]]),ls(e1)),0),
  na_count=mapply(function(v)sum(is.na(e1[[v]])),ls(e1)),
  fn=local({
    directory<-ifelse(grepl("^luke",ls(e1)),"luke_summary","duke_summary")
    paste0("N:/Depts/Share/UK Alpha Team/Analytics/",directory,"/",ls(e1),".csv")
  }),
  timestamp=rep(as.character(Sys.time()),length(ls(e1)))
)

for(i in 1:nrow(data_loaded)){
  if(data_loaded$class[i]!="matrix")next;
  v<-e1[[data_loaded$name[i]]]
  dt<-data.table(date=rownames(v),v)
  append2log(paste0("create_portfolio_summary: saving ",data_loaded$name[i]))
  fwrite(dt,file=data_loaded$fn[i])
}
append2log("create_portfolio_summary: saving luke_summary/manager_col.csv") 
fwrite(e1$manager_col,"N:/Depts/Share/UK Alpha Team/Analytics/luke_summary/manager_col.csv")

append2log("create_portfolio_summary: saving luke_summary/pair_col.csv") 
fwrite(e1$pair_col,"N:/Depts/Share/UK Alpha Team/Analytics/luke_summary/pair_col.csv")

append2log("create_portfolio_summary: saving duke_summary/manager_col.csv") 
fwrite(e1$manager_col,"N:/Depts/Share/UK Alpha Team/Analytics/duke_summary/manager_col.csv")

append2log("create_portfolio_summary: saving duke_summary/pair_col.csv") 
fwrite(e1$pair_col,"N:/Depts/Share/UK Alpha Team/Analytics/duke_summary/pair_col.csv")

append2log("create_portfolio_summary: saving luke_summary/data_loaded.csv") 
fwrite(data_loaded,"N:/Depts/Share/UK Alpha Team/Analytics/luke_summary/data_loaded.csv")

append2log("create_portfolio_summary: saving duke_summary/data_loaded.csv") 
fwrite(data_loaded,"N:/Depts/Share/UK Alpha Team/Analytics/duke_summary/data_loaded.csv")





