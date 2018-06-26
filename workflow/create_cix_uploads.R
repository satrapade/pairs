# 
# Open sheet, scrape positions, make formuli + and create upload files
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
Rblpapi::blpConnect()

config<-new.env()
source(
  file="https://raw.githubusercontent.com/satrapade/pairs/master/configuration/workflow_config.R",
  local=config
)

source("https://raw.githubusercontent.com/satrapade/utility/master/utility_functions.R")
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/append2log.R")
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/futures_historical_tickers.R")
#source("https://raw.githubusercontent.com/satrapade/latex_utils/master/utility_functions.R")
source("https://raw.githubusercontent.com/satrapade/utility/master/sheet_scraping_functions.R")

append2log("create_cix_uploads: sourced utility function")

cix_results_directory<-config$workflow$create_cix_uploads$cix_results_directory
luke_results_directory<-config$workflow$create_cix_uploads$luke_results_directory
duke_results_directory<-config$workflow$create_cix_uploads$duke_results_directory


memo_calc_futures_static<-function(
  futures
){
  futures_static<-loadCache(key=list(futures))
  if(is.null(futures_static)){
    futures_bbg<-futures_historical_tickers(futures)
    res<-Rblpapi::bdp(
      futures_bbg,
      c(
        "SECURITY_TYP",
        "NAME",
        "CRNCY",
        "UNDL_SPOT_TICKER",
        "FUT_CONT_SIZE"
      )
    )[futures_bbg,]
    futures_static<-data.table(
      ticker=futures,
      bbg=futures_bbg,
      res
    )
    saveCache(futures_static,key=list(futures))
  }
  return(futures_static)
}


the_date<-as.character(Sys.Date(),format="%Y-%m-%d")
the_file<-make_sheet_name(the_date)
# sheet_fn is like "N:/Depts/Global/Absolute Insight/UK Equity/AbsoluteUK xp final.xlsm"
the_file<-config$workflow$create_cix_uploads$sheet_fn 
the_type<-determine_excel_filetype(the_file)

#
# fetch sheet ranges
#
append2log("create_cix_uploads : scraping DUKE position range")
the_duke_position_range<-get_sheet_position_range(
  date=the_date,
  fn=the_file,
  file_type = the_type,
  sheet="DUKE Open Trades",
  summary_sheet="DUKE Summary"
)

append2log("create_cix_uploads : scraping LUKE position range")
the_luke_position_range<-get_sheet_position_range(
  date=the_date,
  fn=the_file,
  file_type = the_type,
  sheet="AIL Open Trades",
  summary_sheet="AIL Summary"
)

append2log("create_cix_uploads : scraping DUKE unwind range")
the_duke_unwind_range<-get_sheet_unwind_range(
  date=the_date,
  fn=the_file,
  file_type = the_type,
  sheet="DUKE Closed Trades",
  summary_sheet="DUKE Summary"
)

append2log("create_cix_uploads : scraping LUKE unwind range")
the_luke_unwind_range<-get_sheet_unwind_range(
  date=the_date,
  fn=the_file,
  file_type = the_type,
  sheet="AIL Closed Trades",
  summary_sheet="AIL Summary"
)

append2log("create_cix_uploads : computing DUKE positions")
the_duke_position<-get_sheet_positions(
  date=the_date,
  fn=the_file,
  file_type=the_type,
  position_range=the_duke_position_range
)

append2log("create_cix_uploads : computing DUKE unwinds")
the_duke_unwinds<- get_sheet_unwinds(
  date=the_date,
  fn=the_file,
  file_type=the_type,
  unwind_range=the_duke_unwind_range
)

append2log("create_cix_uploads : computing LUKE positions")
the_luke_position<-get_sheet_positions(
  date=the_date,
  fn=the_file,
  file_type=the_type,
  position_range=the_luke_position_range
)

append2log("create_cix_uploads : computing LUKE unwinds")
the_luke_unwinds<- get_sheet_unwinds(
  date=the_date,
  fn=the_file,
  file_type=the_type,
  unwind_range=the_luke_unwind_range
)


all_tickers<-setdiff(Filter(function(x)ticker_class(x)!="nomatch",sort(unique(c(
  the_luke_position$ticker,
  the_duke_position$ticker
)))),"")

append2log("create_cix_uploads : computing futures static")
futures<-all_tickers[grepl("^future",ticker_class(all_tickers))]
futures_static<-memo_calc_futures_static(futures)

append2log("create_cix_uploads : fetching position, unwinds, again (todo:check this)")
the_position_range<-get_sheet_position_range(date=the_date,fn=the_file,file_type = the_type)
the_unwind_range<-get_sheet_unwind_range(date=the_date,fn=the_file,file_type = the_type)
the_position<-get_sheet_positions(date=the_date,fn=the_file,file_type=the_type,position_range=the_position_range)
the_unwinds<- get_sheet_unwinds(date=the_date,fn=the_file,file_type=the_type,unwind_range=the_unwind_range)



append2log("create_cix_uploads : compute ticker static")
all_ticker_static<-data.table(
  ticker=all_tickers,
  class=ticker_class(all_tickers),
  undl=ifelse(
    all_tickers %in% futures_static$ticker,
    paste(futures_static[,.SD,keyby=ticker][all_tickers,UNDL_SPOT_TICKER],"Index"),
    all_tickers
  ),
  mult=ifelse(
    all_tickers %in% futures_static$ticker,
    futures_static[,.SD,keyby=ticker][all_tickers,FUT_CONT_SIZE],
    1
  )
)[,c(
  .SD,
  list(
    crncy=blpapi::bdp(conn,ticker,"CRNCY")[ticker,"CRNCY"]
  )
)][,c(
  .SD,
  list(
    mult_px=ifelse(crncy=="GBp",100,1),
    fx=paste0(toupper(crncy),"GBP Curncy")
  )
)]


ticker2undl<-function(x){
  setkey(all_ticker_static,ticker)
  all_ticker_static[x,undl]
}

ticker2mult<-function(x){
  setkey(all_ticker_static,ticker)
  all_ticker_static[x,mult]
}

ticker2crncy<-function(x){
  setkey(all_ticker_static,ticker)
  fx<-all_ticker_static[x,fx]
  mult_px<-all_ticker_static[x,mult_px]
  if(fx=="GBPGBP Curncy")return(paste0("/",mult_px))
  paste0("*(",fx,")")
}


# if(sum(grepl("^future",all_tickers$class))>0){
#   i<-which(grepl("^future",all_tickers$class))
#   fut_static<-blpapi::bdp(
#     conn,
#     all_tickers$ticker[i],
#     c("UNDL_SPOT_TICKER","FUT_CONT_SIZE")
#   )[all_tickers$ticker[i],c("UNDL_SPOT_TICKER","FUT_CONT_SIZE")]
#   all_tickers$mult[i]<-fut_static$FUT_CONT_SIZE
#   all_tickers$undl[i]<-paste0(fut_static$UNDL_SPOT_TICKER," Index")
# }


append2log("create_cix_uploads : compute DUKE cix formuli")
t1<-the_duke_position[
  ticker_class(ticker)!="nomatch",.(
    Ticker=ticker2undl(ticker[1]),
    Quantity=sum(units)*ticker2mult(ticker[1]),
    Curncy=ticker2crncy(ticker[1]),
    Date=the_date
  ),keyby="ticker,pair"]
the_duke_CIX_formuli<-t1[,.(
    Formula=local({
      if(sum(abs(Quantity)>0)<1)return("")
      i<-which(Quantity>0)
      numerator<-paste0("(",Ticker[i],"*(",format_format(scientific=FALSE)(Quantity[i]),")",Curncy[i],")",collapse = "+")
      j<-which(Quantity<0)
      denominator<-paste0("(",Ticker[j],"*(",format_format(scientific=FALSE)(abs(Quantity[j])),")",Curncy[j],")",collapse = "+")
      if(length(j)==0)return(numerator)
      if(length(i)==0)return(paste0("1/(",denominator,")"))
      paste0("(",numerator,")/(",denominator,")")
    }),
    Name=local({
      if(sum(abs(Quantity)>0)<1)return("")
      i<-which(Quantity>0)
      numerator<-paste0(gsub("(/)|( [A-Z]{2} Equity$)|( Index$)","",Ticker[i]),collapse = "+")
      j<-which(Quantity<0)
      denominator<-paste0(gsub("(/)|( [A-Z]{2} Equity$)|( Index$)","",Ticker[j]),collapse = "+")
      if(length(j)==0)return(numerator)
      if(length(i)==0)return(denominator)
      stri_sub(paste0(numerator," vs ",denominator),1,20)
    })
  ),keyby="pair"][Formula!=""]

append2log("create_cix_uploads : compute LUKE cix formuli")
the_luke_CIX_formuli<-the_luke_position[
  ticker_class(ticker)!="nomatch",.(
    Ticker=ticker2undl(ticker[1]),
    Quantity=sum(units)*ticker2mult(ticker[1]),
    Curncy=ticker2crncy(ticker[1]),
    Date=the_date
  ),keyby="ticker,pair"][,.(
    Formula=local({
      if(sum(abs(Quantity)>0)<1)return("")
      i<-which(Quantity>0)
      numerator<-paste0("(",Ticker[i],"*(",format_format(scientific=FALSE)(Quantity[i]),")",Curncy[i],")",collapse = "+")
      j<-which(Quantity<0)
      denominator<-paste0("(",Ticker[j],"*(",format_format(scientific=FALSE)(abs(Quantity[j])),")",Curncy[j],")",collapse = "+")
      if(length(j)==0)return(numerator)
      if(length(i)==0)return(paste0("1/(",denominator,")"))
      paste0("(",numerator,")/(",denominator,")")
    }),
    Name=local({
      if(sum(abs(Quantity)>0)<1)return("")
      i<-which(Quantity>0)
      numerator<-paste0(gsub("(/)|( [A-Z]{2} Equity$)|( Index$)","",Ticker[i]),collapse = "+")
      j<-which(Quantity<0)
      denominator<-paste0(gsub("(/)|( [A-Z]{2} Equity$)|( Index$)","",Ticker[j]),collapse = "+")
      if(length(j)==0)return(numerator)
      if(length(i)==0)return(denominator)
      stri_sub(paste0(numerator," vs ",denominator),1,20)
    })
  ),keyby="pair"][Formula!=""]

#
# save 
#

# one file for all pairs
append2log("create_cix_uploads : compute save DUKE cix formuli for all pairs")
cat(c(
  "\\Target, CIX, Description, Permission",
  paste(the_duke_CIX_formuli$pair,",",the_duke_CIX_formuli$Formula,",",the_duke_CIX_formuli$Name,",")
),file=paste0(config$home_directory,"/CIX/duke_all_cix.txt"),sep="\n")
append2log("create_cix_uploads : compute save LUKE cix formuli for all pairs")
cat(c(
  "\\Target, CIX, Description, Permission",
  paste(the_luke_CIX_formuli$pair,",",the_luke_CIX_formuli$Formula,",",the_luke_CIX_formuli$Name,",")
),file=paste0(config$home_directory,"/CIX/luke_all_cix.txt"),sep="\n")


# one file per portfolio manager
append2log("create_cix_uploads : compute save DUKE cix formuli for each manager")
for(i in sort(unique(gsub("[0-9]{1,9}$","",the_duke_CIX_formuli$pair)))){
  n<-grepl(paste0("^",i,"[0-9]{1,9}$"),the_duke_CIX_formuli$pair)
  cat(c(
  "\\Target, CIX, Description, Permission",
  paste(the_duke_CIX_formuli[n]$pair,",",the_duke_CIX_formuli[n]$Formula,",",the_duke_CIX_formuli[n]$Name,",")
  ),file=paste0(config$home_directory,"/CIX/duke_",i,"_cix.txt"),sep="\n")
}
append2log("create_cix_uploads : compute save LUKE cix formuli for each manager")
for(i in sort(unique(gsub("[0-9]{1,9}$","",the_luke_CIX_formuli$pair)))){
  n<-grepl(paste0("^",i,"[0-9]{1,9}$"),the_luke_CIX_formuli$pair)
  cat(c(
    "\\Target, CIX, Description, Permission",
    paste(the_luke_CIX_formuli[n]$pair,",",the_luke_CIX_formuli[n]$Formula,",",the_luke_CIX_formuli[n]$Name,",")
  ),file=paste0(config$home_directory,"/CIX/luke_",i,"_cix.txt"),sep="\n")
}



