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
rconn<-blpConnect()
append2log<-function(log_text,append=TRUE)
{
  cat(
    paste0(stri_trim(gsub("##|-","",capture.output(timestamp())))," : ",log_text,"\n"),
    file="N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/workflow.log",
    append=append
  )
}

source("https://raw.githubusercontent.com/satrapade/utility/master/utility_functions.R")
source("https://raw.githubusercontent.com/satrapade/utility/master/sheet_scraping_functions.R")
source("https://raw.githubusercontent.com/satrapade/utility/master/eq_excel_scraping_functions.R")

bbu<-function(fn){
  cmdline<-paste0("C:/blp/Wintrv/openfl -P @profile.bbu ",gsub("/","\\\\",fn))
  res<-system(cmdline,show.output.on.console = FALSE,intern=TRUE)
  res
}


#
# fetch futures historical tickers
#
futures_historical_tickers<-function(futures_tickers){
  fmc<-c("F"="Jan","G"="Feb","H"="Mar","J"="Apr","K"="May","M"="Jun","N"="Jul","Q"="Aug","U"="Sep","V"="Oct","X"="Nov","Z"="Dec")
  futures<-sort(unique(futures_tickers))  
  f2d<-function(f){
    date_string<-paste0("01-",fmc[stri_sub(f,3,3)],"-201",stri_sub(f,4,4))
    as.character(as.Date(date_string,format="%d-%b-%Y"),format="%Y-%m-%d")
  }
  lookup_table<-do.call(rbind,mapply(function(f){
    ffc<-paste0(stri_sub(f,1,2),"1 Index")
    res<-bds(ffc,"FUT_CHAIN", override=c(CHAIN_DATE=gsub("-","",f2d(f))))
    data.frame(contract=f,historical=res[1,1],row.names=NULL,stringsAsFactors=FALSE)
  },futures,SIMPLIFY=FALSE))
  lookup_table[futures_tickers,"historical"]
}

memo_calc_futures_static<-function(
  futures
){
  futures_static<-loadCache(key=list(futures))
  if(is.null(futures_static)){
    futures_bbg<-futures_historical_tickers(futures)
    res<-bdp(
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
      id=securities$id[match(futures,securities$ticker)],
      ticker=futures,
      bbg=futures_bbg,
      res
    )
    saveCache(futures_static,key=list(futures))
  }
  return(futures_static)
}


the_date<-as.character(Sys.Date(),format="%Y-%m-%d")
the_file<-"N:/Depts/Global/Absolute Insight/UK Equity/AbsoluteUK xp final.xlsm"
the_type<-determine_excel_filetype(the_file)


#
# fetch sheet ranges
#
append2log("create_portfolio upload: scrape DUKE position range")
the_duke_position_range<-get_sheet_position_range(
  date=the_date,
  fn=the_file,
  file_type = the_type,
  sheet="DUKE Open Trades",
  summary_sheet="DUKE Summary"
)
append2log("create_portfolio upload: scrape LUKE position range")
the_luke_position_range<-get_sheet_position_range(
  date=the_date,
  fn=the_file,
  file_type = the_type,
  sheet="AIL Open Trades",
  summary_sheet="AIL Summary"
)
append2log("create_portfolio upload: scrape DUKE unwind range")
the_duke_unwind_range<-get_sheet_unwind_range(
  date=the_date,
  fn=the_file,
  file_type = the_type,
  sheet="DUKE Closed Trades",
  summary_sheet="DUKE Summary"
)
append2log("create_portfolio upload: scrape LUKE unwind range")
the_luke_unwind_range<-get_sheet_unwind_range(
  date=the_date,
  fn=the_file,
  file_type = the_type,
  sheet="AIL Closed Trades",
  summary_sheet="AIL Summary"
)

replace_blank_with_last<-function(x){
  x_upper<-toupper(x)
  x_edges<-c(
    x_upper[1]!="",
    head(x_upper,-1)!=tail(x_upper,-1) & nchar(tail(x_upper,-1))>0
  )
  x_all<-x_upper[x_edges]
  x_i<-findInterval(seq_along(x_edges),which(x_edges))
  x_all[pmax(x_i,1)]
}

#
# parse ranges into postions, unwinds
# attributes(attributes(get_sheet_positions)$srcref)$srcfile
append2log("create_portfolio upload: compute DUKE position")
the_duke_position<-get_sheet_positions(
  date=the_date,
  fn=the_file,
  file_type=the_type,
  position_range=the_duke_position_range
)
append2log("create_portfolio upload: compute DUKE unwinds")
the_duke_unwinds<- get_sheet_unwinds(
  date=the_date,
  fn=the_file,
  file_type=the_type,
  unwind_range=the_duke_unwind_range
)
append2log("create_portfolio upload: compute LUKE position")
the_luke_position<-get_sheet_positions(
  date=the_date,
  fn=the_file,
  file_type=the_type,
  position_range=the_luke_position_range
)
append2log("create_portfolio upload: compute LUKE unwinds")
the_luke_unwinds<- get_sheet_unwinds(
  date=the_date,
  fn=the_file,
  file_type=the_type,
  unwind_range=the_luke_unwind_range
)

#fwrite(the_duke_position,file="N:/Depts/Share/UK Alpha Team/Analytics/DUKE/duke_position.csv")
#fwrite(the_luke_position,file="N:/Depts/Share/UK Alpha Team/Analytics/LUKE/luke_position.csv")



# setup a mapping, valid column types:
# PID Portfolio Name
# PNUM Portfolio Number
# BENCH Benchmark Name
# QUANTITY Quantity/Positions?
# FWEIGHT Fixed Weight for Portfolios and Benchmarks
# DWEIGHT Drifting Weight for Portfolios and Benchmarks
# ID_TYPE Numeric ID Type
# DATE Position Date, For Transactions it's the Trade date
# EXCHANGE Two digit Exchange Code
# ID Security Id
# COST Cost Price
# COST_XRATE Cost Exchange Rate
# USER_PX User Price
# USER_MKT_VALUE User Market Value 
luke_portfolio_spec<-c(
  "\\TARGET, MAPPING",
  "Name,LUKE Uploads",
  "Delimiter,\",\"",
  "Start Row,2",
  "Ignore Lines Starting,\"\"",
  "Upload To,Portfolio",
  "Apply To,luke_bbg_upload_portfolio.csv",
  "Default Date,today",
  "Enterprise Upload,No",
  "Desktop Upload,Yes",
  "Has Custom Data,Yes",
  "Ignore CDE Blank,No",
  "XSLT Seed1,0",
  "XSLT Seed2,0",
  "#Columns",
  "Column,1,PID",
  "Column,2,ID",
  "Column,3,QUANTITY",
  "Column,4,DATE",
  "Column,5,UD-LUKE_ABC",
  "Column,6,UD-LUKE_AC",
  "Column,7,UD-LUKE_ACRW",
  "Column,8,UD-LUKE_DH",
  "Column,9,UD-LUKE_GJ",
  "Column,10,UD-LUKE_MC",
  "Column,11,UD-LUKE_IB",
  "Column,11,UD-LUKE_JR",
  "#File Defaults",
  "Asset", 
  "Class,Equity",
  "Currency,GBP",
  "Is Current Face,No",
  "Is Swap Units,No",
  "Has OVML As Notional,No",
  "Is Current Face for Bonds,No",
  "Number Format,\"nnn,nnn.dd\"",
  "Date Format,YY/MM/DD or YYYY/MM/DD",
  "Bypass Blank Quantity,Yes",
  "Create New ",
  "Portfolio,Yes",
  "Divide by 1000,Yes",
  "Excel Worksheet Name,",
  "Excel Worksheet Number,0"
)
duke_portfolio_spec<-c(
  "\\TARGET, MAPPING",
  "Name,DUKE Uploads",
  "Delimiter,\",\"",
  "Start Row,2",
  "Ignore Lines Starting,\"\"",
  "Upload To,Portfolio",
  "Apply To,duke_bbg_upload_portfolio.csv",
  "Default Date,today",
  "Enterprise Upload,No",
  "Desktop Upload,Yes",
  "Has Custom Data,Yes",
  "Ignore CDE Blank,No",
  "XSLT Seed1,0",
  "XSLT Seed2,0",
  "#Columns",
  "Column,1,PID",
  "Column,2,ID",
  "Column,3,QUANTITY",
  "Column,4,DATE",
  "Column,5,UD-DUKE_ABC",
  "Column,6,UD-DUKE_AC",
  "Column,7,UD-DUKE_ACRW",
  "Column,8,UD-DUKE_DH",
  "Column,9,UD-DUKE_GJ",
  "Column,10,UD-DUKE_MC",
  "Column,11,UD-DUKE_IB",
  "Column,11,UD-DUKE_JR",
  "#File Defaults",
  "Asset", 
  "Class,Equity",
  "Currency,GBP",
  "Is Current Face,No",
  "Is Swap Units,No",
  "Has OVML As Notional,No",
  "Is Current Face for Bonds,No",
  "Number Format,\"nnn,nnn.dd\"",
  "Date Format,YY/MM/DD or YYYY/MM/DD",
  "Bypass Blank Quantity,Yes",
  "Create New ",
  "Portfolio,Yes",
  "Divide by 1000,Yes",
  "Excel Worksheet Name,",
  "Excel Worksheet Number,0"
)
append2log("create_portfolio upload: save LUKE, DUKE porfolio spec")
cat(luke_portfolio_spec,file="N:/Depts/Share/UK Alpha Team/Analytics/LUKE/luke_portfolio_spec.txt",sep="\n")
cat(duke_portfolio_spec,file="N:/Depts/Share/UK Alpha Team/Analytics/DUKE/duke_portfolio_spec.txt",sep="\n")



#bbu("uploads/duke_portfolio_spec.txt")
#Portfolio,Ticker,Quantity,Date,LUKE_ABC,LUKE_AC,LUKE_ACRW,LUKE_DH,LUKE_GJ,LUKE_MC

# the_date<-as.character(Sys.Date(),format="%Y-%m-%d")
# #the_file<-make_sheet_name(the_date)
# the_file<-"N:/Depts/Global/Absolute Insight/UK Equity/AbsoluteUK xp final.xlsm"
# the_type<-determine_excel_filetype(the_file)
# the_position_range<-get_sheet_position_range(date=the_date,fn=the_file,file_type = the_type)
# the_unwind_range<-get_sheet_unwind_range(date=the_date,fn=the_file,file_type = the_type)
# the_position<-get_sheet_positions(date=the_date,fn=the_file,file_type=the_type,position_range=the_position_range)
# the_unwinds<- get_sheet_unwinds(date=the_date,fn=the_file,file_type=the_type,unwind_range=the_unwind_range)



append2log("create_portfolio upload: compute all tickers")
all_tickers<-data.table(
  ticker=setdiff(sort(unique(c(
    the_duke_position$ticker[ticker_class(the_duke_position$ticker)!="nomatch"],
    the_luke_position$ticker[ticker_class(the_luke_position$ticker)!="nomatch"] 
  ))),"")
)[,
  "class":=list(ticker_class(ticker))
][,
    "crncy":=list(paste0(toupper(blpapi::bdp(conn,ticker,"CRNCY")[ticker,"CRNCY"]),"GBP Curncy"))
][,
    c("undl","mult"):=list(ticker,rep(1,length(ticker)))
]

ticker2undl<-function(x){
  setkey(all_tickers,ticker)
  all_tickers[x,undl]
}

ticker2mult<-function(x){
  setkey(all_tickers,ticker)
  all_tickers[x,mult]
}

ticker2crncy<-function(x){
  setkey(all_tickers,ticker)
  res<-all_tickers[x,crncy]
  if(res=="GBPGBP Curncy")return("/100")
  paste0("*(",res,")")
}


if(sum(grepl("^future",all_tickers$class))>0){
  i<-which(grepl("^future",all_tickers$class))
  fut_static<-blpapi::bdp(
    conn,
    all_tickers$ticker[i],
    c("UNDL_SPOT_TICKER","FUT_CONT_SIZE")
  )[all_tickers$ticker[i],c("UNDL_SPOT_TICKER","FUT_CONT_SIZE")]
  all_tickers$mult[i]<-fut_static$FUT_CONT_SIZE
  all_tickers$undl[i]<-paste0(fut_static$UNDL_SPOT_TICKER," Index")
}


# compute cash holdings for LUKE
append2log("create_portfolio upload: compute LUKE cash holdings")
the_luke_cash<-the_luke_position[ticker_class(ticker)!="nomatch",.(
  Portfolio=paste0("LUKE_",unique(manager)[1]),
  Curncy=local({
    setkey(all_tickers,ticker)
    the_ticker<-ticker
    res<-stri_sub(all_tickers[the_ticker,crncy],1,3)
    paste0(toupper(res)," Curncy")
  }),
  Cash=sum(cash*local_asset_value/asset_value)
),keyby="ticker,manager"][,.(
  Ticker=Curncy[1],
  Quantity=format_format(scientific=FALSE,digits=0)(sum(Cash)),
  Date=the_date,
  LUKE_ABC=0,
  LUKE_AC=0,
  LUKE_ACRW=0,
  LUKE_DH=0,
  LUKE_GJ=0,
  LUKE_MC=0,
  LUKE_IB=0,
  LUKE_JR=0
),
keyby="Portfolio,Curncy"
][,.(Portfolio,Ticker,Quantity,Date,LUKE_ABC,LUKE_AC,LUKE_ACRW,LUKE_DH,LUKE_GJ,LUKE_MC,LUKE_IB,LUKE_JR)]

# compute cash holdings for DUKE
append2log("create_portfolio upload: compute DUKE cash holdings")
the_duke_cash<-the_duke_position[ticker_class(ticker)!="nomatch",.(
  Portfolio=paste0("DUKE_",unique(manager)[1]),
  Curncy=local({
    setkey(all_tickers,ticker)
    the_ticker<-ticker
    res<-stri_sub(all_tickers[the_ticker,crncy],1,3)
    paste0(toupper(res)," Curncy")
  }),
  Cash=sum(cash*local_asset_value/asset_value)
),keyby="ticker,manager"][,.(
  Ticker=Curncy[1],
  Quantity=format_format(scientific=FALSE,digits=0)(sum(Cash)),
  Date=the_date,
  DUKE_ABC=0,
  DUKE_AC=0,
  DUKE_ACRW=0,
  DUKE_DH=0,
  DUKE_GJ=0,
  DUKE_MC=0,
  DUKE_IB=0,
  DUKE_JR=0
),
keyby="Portfolio,Curncy"
][,.(Portfolio,Ticker,Quantity,Date,DUKE_ABC,DUKE_AC,DUKE_ACRW,DUKE_DH,DUKE_GJ,DUKE_MC,DUKE_IB,DUKE_JR)]

# bbg portfolio
append2log("create_portfolio upload: compute LUKE bloomberg portfolio")
the_luke_bbg_portfolio<-rbind(the_luke_position[ticker_class(ticker)!="nomatch",.(
  Ticker=ticker[1],
  Quantity=sum(units),
  Portfolio=paste0("LUKE_",unique(manager)[1]),
  Date=the_date
),keyby="ticker,manager"][,.(
  Ticker=c(Ticker,"PORTFOLIO VALUE"),
  Quantity=format_format(scientific=FALSE)(c(Quantity,attributes(the_luke_position_range)$AUM)),
  Date=c(Date,the_date)
),keyby=Portfolio][,.(
  Portfolio,
  Ticker,
  Quantity,
  Date,
  LUKE_ABC=mapply(function(x)sum(as.numeric(Quantity)[Portfolio=="LUKE_ABC" & Ticker==x]),Ticker),
  LUKE_AC=mapply(function(x)sum(as.numeric(Quantity)[Portfolio=="LUKE_AC" & Ticker==x]),Ticker),
  LUKE_ACRW=mapply(function(x)sum(as.numeric(Quantity)[Portfolio=="LUKE_ACRW" & Ticker==x]),Ticker),
  LUKE_DH=mapply(function(x)sum(as.numeric(Quantity)[Portfolio=="LUKE_DH" & Ticker==x]),Ticker),
  LUKE_GJ=mapply(function(x)sum(as.numeric(Quantity)[Portfolio=="LUKE_GJ" & Ticker==x]),Ticker),
  LUKE_MC=mapply(function(x)sum(as.numeric(Quantity)[Portfolio=="LUKE_MC" & Ticker==x]),Ticker),
  LUKE_IB=mapply(function(x)sum(as.numeric(Quantity)[Portfolio=="LUKE_IB" & Ticker==x]),Ticker),
  LUKE_JR=mapply(function(x)sum(as.numeric(Quantity)[Portfolio=="LUKE_JR" & Ticker==x]),Ticker)
 )],the_luke_cash)

# bbg portfolio
append2log("create_portfolio upload: compute DUKE bloomberg portfolio")
the_duke_bbg_portfolio<-rbind(the_duke_position[ticker_class(ticker)!="nomatch",.(
  Ticker=ticker[1],
  Quantity=sum(units),
  Portfolio=paste0("DUKE_",unique(manager)[1]),
  Date=the_date
),keyby="ticker,manager"][,.(
  Ticker=c(Ticker,"PORTFOLIO VALUE"),
  Quantity=format_format(scientific=FALSE)(c(Quantity,attributes(the_duke_position_range)$AUM)),
  Date=c(Date,the_date)
),keyby=Portfolio][,.(
  Portfolio,
  Ticker,
  Quantity,
  Date,
  DUKE_ABC=mapply(function(x)sum(as.numeric(Quantity)[Portfolio=="DUKE_ABC" & Ticker==x]),Ticker),
  DUKE_AC=mapply(function(x)sum(as.numeric(Quantity)[Portfolio=="DUKE_AC" & Ticker==x]),Ticker),
  DUKE_ACRW=mapply(function(x)sum(as.numeric(Quantity)[Portfolio=="DUKE_ACRW" & Ticker==x]),Ticker),
  DUKE_DH=mapply(function(x)sum(as.numeric(Quantity)[Portfolio=="DUKE_DH" & Ticker==x]),Ticker),
  DUKE_GJ=mapply(function(x)sum(as.numeric(Quantity)[Portfolio=="DUKE_GJ" & Ticker==x]),Ticker),
  DUKE_MC=mapply(function(x)sum(as.numeric(Quantity)[Portfolio=="DUKE_MC" & Ticker==x]),Ticker),
  DUKE_IB=mapply(function(x)sum(as.numeric(Quantity)[Portfolio=="DUKE_IB" & Ticker==x]),Ticker),
  DUKE_JR=mapply(function(x)sum(as.numeric(Quantity)[Portfolio=="DUKE_JR" & Ticker==x]),Ticker)
)],the_duke_cash)


append2log("create_portfolio upload: save LUKE, DUKE unwind range")
fwrite(data.table(the_luke_unwind_range),"N:/Depts/Share/UK Alpha Team/Analytics/sheet_scrape/luke_unwind_range.csv")
fwrite(data.table(the_duke_unwind_range),"N:/Depts/Share/UK Alpha Team/Analytics/sheet_scrape/duke_unwind_range.csv")

append2log("create_portfolio upload: save LUKE, DUKE position range")
fwrite(data.table(the_luke_position_range),"N:/Depts/Share/UK Alpha Team/Analytics/sheet_scrape/luke_position_range.csv")
fwrite(data.table(the_duke_position_range),"N:/Depts/Share/UK Alpha Team/Analytics/sheet_scrape/duke_position_range.csv")

append2log("create_portfolio upload: save LUKE, DUKE unwinds")
fwrite(the_luke_unwinds,"N:/Depts/Share/UK Alpha Team/Analytics/sheet_scrape/luke_unwinds.csv")
fwrite(the_duke_unwinds,"N:/Depts/Share/UK Alpha Team/Analytics/sheet_scrape/duke_unwinds.csv")

append2log("create_portfolio upload: save sheet_scrape/luke_position.csv, sheet_scrape/duke_position.csv ")
fwrite(the_luke_position,"N:/Depts/Share/UK Alpha Team/Analytics/sheet_scrape/luke_position.csv")
fwrite(the_duke_position,"N:/Depts/Share/UK Alpha Team/Analytics/sheet_scrape/duke_position.csv")

append2log("create_portfolio upload: save sheet_scrape/scrape_details.csv")
fwrite(data.table(list2data.frame(list(
 date=attributes(the_luke_position_range)$date,
 luke_date=attributes(the_luke_position_range)$date,
 duke_date=attributes(the_duke_position_range)$date,
 luke_filename=attributes(the_luke_position_range)$filename,
 duke_filename=attributes(the_duke_position_range)$filename,
 luke_filetype=attributes(the_luke_position_range)$filetype,
 duke_filetype=attributes(the_duke_position_range)$filetype,
 luke_aum=attributes(the_luke_position_range)$AUM,
 duke_aum=attributes(the_duke_position_range)$AUM,
 luke_position_sheet=attributes(the_luke_position_range)$sheet,
 duke_position_sheet=attributes(the_duke_position_range)$sheet,
 luke_unwind_sheet=attributes(the_luke_unwind_range)$sheet,
 duke_unwind_sheet=attributes(the_duke_unwind_range)$sheet,
 luke_summary_sheet=attributes(the_luke_position_range)$summary_sheet,
 duke_summary_sheet=attributes(the_duke_position_range)$summary_sheet,
 timestamp=as.character(Sys.timeDate(),format="%Y-%m-%d")
))),"N:/Depts/Share/UK Alpha Team/Analytics/sheet_scrape/scrape_details.csv")

append2log("create_portfolio upload: save LUKE/luke_bbg_upload_portfolio.csv, DUKE/duke_bbg_upload_portfolio.csv")
fwrite(the_luke_bbg_portfolio,"N:/Depts/Share/UK Alpha Team/Analytics/LUKE/luke_bbg_upload_portfolio.csv")
fwrite(the_duke_bbg_portfolio,"N:/Depts/Share/UK Alpha Team/Analytics/DUKE/duke_bbg_upload_portfolio.csv")



append2log("create_portfolio upload: compute DUKE portfolio")
the_luke_portfolio<-the_luke_position[ticker_class(ticker)!="nomatch",.(
  Ticker=ticker2undl(ticker[1]),
  Quantity=sum(units)*ticker2mult(ticker[1]),
  Portfolio=paste0("LUKE_",unique(manager)[1]),
  Pair=unique(pair)[1],
  Date=the_date
),keyby="ticker,pair,manager"][,.(
  Manager=gsub("[0-9]{1,3}$","",Pair),
  Ticker=Ticker,
  Quantity=Quantity,
  Date=Date
),keyby="Portfolio,Pair"][,.(
  Manager,
  Portfolio,
  Pair,
  Ticker,
  Quantity,
  Date
)][
  !Pair %in% c(
    "DH20",
    names(which(mapply(sum,split(abs(Quantity),Pair))==0))
  )
  ]

append2log("create_portfolio upload: compute DUKE portfolio")
the_duke_portfolio<-the_duke_position[ticker_class(ticker)!="nomatch",.(
  Ticker=ticker2undl(ticker[1]),
  Quantity=sum(units)*ticker2mult(ticker[1]),
  Portfolio=paste0("DUKE_",unique(manager)[1]),
  Pair=unique(pair)[1],
  Date=the_date
),keyby="ticker,pair,manager"][,.(
  Manager=gsub("[0-9]{1,3}$","",Pair),
  Ticker=Ticker,
  Quantity=Quantity,
  Date=Date
),keyby="Portfolio,Pair"][,.(
  Manager,
  Portfolio,
  Pair,
  Ticker,
  Quantity,
  Date
)][
  !Pair %in% c(
    "DH20",
    names(which(mapply(sum,split(abs(Quantity),Pair))==0))
  )
  ]


all_days<-make_date_range(
  as.character(as.Date(the_date,format="%Y-%m-%d")-365*2,format="%Y-%m-%d"),
  the_date,
  leading_days=0
)

the_tickers<-sort(unique(c(the_duke_portfolio$Ticker,the_luke_portfolio$Ticker)))

append2log("create_portfolio upload: fetch PX_LAST for all tickers")
the_bbg_res<-Rblpapi::bdh(
  the_tickers,
  "PX_LAST",
  start.date= as.Date(tail(all_days,30)[1],format="%Y-%m-%d"),
  end.date= as.Date(tail(all_days,30)[30],format="%Y-%m-%d"),
  overrides=c(EQY_FUND_CRNCY="GBP")
)

the_prices<-data.table(
  ticker=rep(names(the_bbg_res),mapply(nrow,the_bbg_res)),
  do.call(rbind,the_bbg_res)
)[,.(
  date=date[max(which(!is.na(PX_LAST)))],
  PX_LAST=PX_LAST[max(which(!is.na(PX_LAST)))]
),keyby="ticker"]

append2log("create_portfolio upload: update price, exposure in LUKE portfolio")
the_luke_portfolio$PX_LAST<-the_prices[,.SD,keyby=ticker][the_luke_portfolio$Ticker,PX_LAST]
the_luke_portfolio$Exposure<-the_luke_portfolio$Quantity*the_luke_portfolio$PX_LAST

append2log("create_portfolio upload: update price, exposure in DUKE portfolio")
the_duke_portfolio$PX_LAST<-the_prices[,.SD,keyby=ticker][the_duke_portfolio$Ticker,PX_LAST]
the_duke_portfolio$Exposure<-the_duke_portfolio$Quantity*the_duke_portfolio$PX_LAST

append2log("create_portfolio upload: save sheet_scrape/luke_portfolio.csv, sheet_scrape/duke_portfolio.csv")
fwrite(the_luke_portfolio,"N:/Depts/Share/UK Alpha Team/Analytics/sheet_scrape/luke_portfolio.csv")
fwrite(the_duke_portfolio,"N:/Depts/Share/UK Alpha Team/Analytics/sheet_scrape/duke_portfolio.csv")


