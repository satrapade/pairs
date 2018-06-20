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

source("https://raw.githubusercontent.com/satrapade/utility/master/utility_functions.R")
source("https://raw.githubusercontent.com/satrapade/utility/master/sheet_bbg_functions.R")


the_luke_portfolio<-fread("N:/Depts/Share/UK Alpha Team/Analytics/sheet_scrape/luke_portfolio.csv")
the_duke_portfolio<-fread("N:/Depts/Share/UK Alpha Team/Analytics/sheet_scrape/duke_portfolio.csv")
scrape_details<-fread("N:/Depts/Share/UK Alpha Team/Analytics/sheet_scrape/scrape_details.csv")

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

append2log("create_market_data: fetch SXXP, UKX constituents")
sxxp_constituents<-paste(Rblpapi::bds("SXXP Index","INDX_MWEIGHT")[[1]],"Equity")
ukx_constituents<-paste(Rblpapi::bds("UKX Index","INDX_MWEIGHT")[[1]],"Equity")

append2log("create_market_data: for ticker universe (LUKE, DUKE, SXXP, UKX)")
all_tickers<-data.table(
  ticker=setdiff(sort(unique(c(
    the_duke_portfolio$Ticker[ticker_class(the_duke_portfolio$Ticker)!="nomatch"],
    the_luke_portfolio$Ticker[ticker_class(the_luke_portfolio$Ticker)!="nomatch"],
    sxxp_constituents,
    ukx_constituents
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

the_tickers<-all_tickers$ticker

all_days<-make_date_range(
  as.character(as.Date(scrape_details[,.SD,keyby=names]["date",values],format="%Y-%m-%d")-365*2,format="%Y-%m-%d"),
  scrape_details[,.SD,keyby=names]["date",values],
  leading_days=0
)


factor_tickers<-c(
  "UKX Index",
  "MCX Index",
  "SMX Index",
  "DAX Index",
  "CAC Index",
  ################## bloomberg indices
  "BEUIPO Index",
  "PMOMENUS Index",
  "LBUSTRUU Index",
  ################## SG factors
  "SGSLVAW Index",
  "SGSLQAW Index",
  "SGSLPAW Index",
  "SGSLMAW Index",
  "SGSLVQAW Index",
  "SGSLVAU Index",
  "SGSLQAU Index",
  "SGSLPAU Index",
  "SGSLMAU Index",
  "SGSLVQAU Index",
  "SGSLVAE Index",
  "SGSLQAE Index",
  "SGSLPAE Index",
  "SGSLMAE Index",
  "SGSLVQAE Index",
  "SGBVPMEU Index",
  "SGBVPHVE Index",
  "SGIXTFEQ Index",
  "SGIXTFFX Index",
  "SGIXTFIR Index",
  "SGIXGCM Index",
  "SGIXTFCY Index",
  ##################
  "LYXRLSMN Index",
  ################## RW picks
  "SXPARO Index",
  ################## GS themes
  "GSTHVISP Index",
  "GSTHHVIP Index",
  "GSTHSHRP Index",
  "GSTHSBAL Index",
  "GSTHWBAL Index",
  "GSTHHTAX Index",
  "GSTHLTAX Index",
  "GSTHMFOW Index",
  "GSTHMFUW Index",
  "GSTHDIVG Index",
  "GSTHQUAL Index",
  "GSTHCASH Index",
  ################## JPM themes
  "JPEUBATL Index",
  "JPEUBATW Index",
  #################
  # MS quant pairs
  "MSEEMOMO Index",
  "MSZZMOMO Index",
  "MSEEGRW Index",
  "MSEEVAL Index",
  "MSSTERSI Index",
  "MSSTPERI Index",
  "MSSTSTUS Index",
  "MSSTHYDS Index",
  ################# MW TOPS
  "FXB US Equity",
  "MLISMBC LX Equity",
  "GLD US Equity",
  "EEM US Equity",
  "VXX US Equity",
  "TLT US Equity",
  "USO US Equity",
  "COINXBE SS Equity",
  "DXY Index",
  "SXXE Index","SXXP Index", # all stocks
  "SX3E Index","SX3P Index", # food and beverage
  "SX4E Index","SX4P Index", # chemicals
  "SX6E Index","SX6P Index", # utilities
  "SX7E Index","SX7P Index", # banks
  "SX8E Index","SX8P Index", # tech
  "SXAE Index","SXAP Index", # auto
  "SXDE Index","SXDP Index", # health
  "SXEE Index","SXEP Index", # energy
  "SXFE Index","SXFP Index", # financial services
  "SXIE Index","SXIP Index", # insurance
  "SXKE Index","SXKP Index", # telecoms
  "SXME Index","SXMP Index", # media
  "SXNE Index","SXNP Index", # industrial goods and services
  "SXOE Index","SXOP Index", # construction and materials
  "SXPE Index","SXPP Index", # basic resource
  "SXQE Index","SXQP Index", # personal and household goods
  "SXRE Index","SXRP Index", # retail
  "SXTE Index","SXTP Index", # travel and leisure
  "SX86E Index","SX86P Index" # real estate
)


the_equity_tickers<-the_tickers[ticker_class(the_tickers)=="equity|nomatch"]

# static
comps_inputs<-list(
c("NAME"),
c("TICKER"),
c("COUNTRY"),
c("CRNCY"),
c("PARSEKYABLE_DES"),
c("GICS_SECTOR_NAME"),
c("INDUSTRY_GROUP"), 
c("INDUSTRY_SECTOR"),
c("MARKET_STATUS"),
# equity market
c("EQY_SH_OUT_ACTUAL"),
c("CUR_MKT_CAP"),
c("EQY_FREE_FLOAT_PCT"),
c("PX_LAST"),
c("CHG_PCT_5D"),
c("BEST_SALES"),
c("CHG_PCT_1D"),
c("CHG_PCT_3M"),
c("CHG_PCT_YTD"),
c("EQY_BETA"),
c("RSI_14D"),
c("REL_INDEX"),
c("DVD_PAYOUT_RATIO"),
c("TOT_ANALYST_REC"),
c("TOT_BUY_REC"),
c("TOT_SELL_REC"),
c("BEST_ANALYST_RATING"),
c("ESG_DISCLOSURE_SCORE"),
c("EXPECTED_REPORT_DT"),
c("EBITDA"),
c("EBIT"),
c("NET_INCOME"),
c("NET_DEBT"),
c("PENSION_LIABILITIES"),
c("MINORITY_NONCONTROLLING_INTEREST"),
c("BS_OTHER_ST_LIAB"), # some people exclude ST liab from EV
c("BS_OTHER_CUR_LIAB"),
c("CF_FREE_CASH_FLOW"),
c("CAPITAL_EXPEND"),
c("DILUTED_EV"),
c("CURR_ENTP_VAL"),
c("WACC"),
c("EBIT_TO_NET_SALES"),
c("IS_EPS"),
c("PE_RATIO"),
c("FCF_YIELD_WITH_CUR_ENTP_VAL"),
c("FCF_YIELD_WITH_CUR_MKT_CAP"),
c("CRNCY_ADJ_MKT_CAP", "EQY_FUND_CRNCY", "USD"),
c("BEST_EPS_4WK_PCT_CHG"),
c("BEST_EPS_6MO_PCT_CHG"),
c("PX_TO_BOOK_RATIO"),
c("BEST_SALES_STDDEV"),
c("BEST_SALES_MEDIAN"),
c("BEST_SALES_HI"),
c("BEST_SALES_LO"),
c("BEST_SALES_NUMEST"),
# operating income
# net income
c("BEST_NET_INCOME"),
# 
c("BEST_TARGET_PRICE","BEST_FPERIOD_OVERRIDE","1FY"),
c("BEST_TARGET_PRICE","BEST_FPERIOD_OVERRIDE","2FY"),
c("BEST_TARGET_PRICE","BEST_FPERIOD_OVERRIDE","3FY"),
#
c("BEST_TARGET_MEDIAN","BEST_FPERIOD_OVERRIDE","1FY"),
c("BEST_TARGET_MEDIAN","BEST_FPERIOD_OVERRIDE","2FY"),
c("BEST_TARGET_MEDIAN","BEST_FPERIOD_OVERRIDE","3FY"),
#
c("FREE_CASH_FLOW_YIELD","BEST_FPERIOD_OVERRIDE","1FY"),
c("FREE_CASH_FLOW_YIELD","BEST_FPERIOD_OVERRIDE","2FY"),
c("FREE_CASH_FLOW_YIELD","BEST_FPERIOD_OVERRIDE","3FY"),
#
c("BEST_ESTIMATE_FCF","BEST_FPERIOD_OVERRIDE","1FY"),
c("BEST_ESTIMATE_FCF","BEST_FPERIOD_OVERRIDE","2FY"),
c("BEST_ESTIMATE_FCF","BEST_FPERIOD_OVERRIDE","3FY"),
#
c("BEST_NET_DEBT","BEST_FPERIOD_OVERRIDE","1FY"),
c("BEST_NET_DEBT","BEST_FPERIOD_OVERRIDE","2FY"),
c("BEST_NET_DEBT","BEST_FPERIOD_OVERRIDE","3FY"),
#
c("BEST_NET_INCOME","BEST_FPERIOD_OVERRIDE","1FY"),
c("BEST_NET_INCOME","BEST_FPERIOD_OVERRIDE","2FY"),
c("BEST_NET_INCOME","BEST_FPERIOD_OVERRIDE","3FY"),
#
c("BEST_CAPEX","BEST_FPERIOD_OVERRIDE","1FY"),
c("BEST_CAPEX","BEST_FPERIOD_OVERRIDE","2FY"),
c("BEST_CAPEX","BEST_FPERIOD_OVERRIDE","3FY"),
#
c("BEST_SALES","BEST_FPERIOD_OVERRIDE","1FY"),
c("BEST_SALES","BEST_FPERIOD_OVERRIDE","2FY"),
c("BEST_SALES","BEST_FPERIOD_OVERRIDE","3FY"),
#
c("BEST_EBITDA","BEST_FPERIOD_OVERRIDE","1FY"),
c("BEST_EBITDA","BEST_FPERIOD_OVERRIDE","2FY"),
c("BEST_EBITDA","BEST_FPERIOD_OVERRIDE","3FY"),
#
c("BEST_EBIT","BEST_FPERIOD_OVERRIDE","1FY"),
c("BEST_EBIT","BEST_FPERIOD_OVERRIDE","2FY"),
c("BEST_EBIT","BEST_FPERIOD_OVERRIDE","3FY"),
#
c("BEST_EV","BEST_FPERIOD_OVERRIDE","1FY"),
c("BEST_EV","BEST_FPERIOD_OVERRIDE","2FY"),
c("BEST_EV","BEST_FPERIOD_OVERRIDE","3FY"),
#
c("BEST_PE_RATIO","BEST_FPERIOD_OVERRIDE","1FY"),
c("BEST_PE_RATIO","BEST_FPERIOD_OVERRIDE","2FY"),
c("BEST_PE_RATIO","BEST_FPERIOD_OVERRIDE","3FY"),
#
c("BEST_PEG_RATIO","BEST_FPERIOD_OVERRIDE","1FY"),
c("BEST_PEG_RATIO","BEST_FPERIOD_OVERRIDE","2FY"),
c("BEST_PEG_RATIO","BEST_FPERIOD_OVERRIDE","3FY"),
#
c("BEST_DIV_YLD","BEST_FPERIOD_OVERRIDE","1FY"),
c("BEST_DIV_YLD","BEST_FPERIOD_OVERRIDE","2FY"),
c("BEST_DIV_YLD","BEST_FPERIOD_OVERRIDE","3FY"),
#
c("BEST_EV_TO_BEST_EBITDA","BEST_FPERIOD_OVERRIDE","1FY"),
c("BEST_EV_TO_BEST_EBITDA","BEST_FPERIOD_OVERRIDE","2FY"),
c("BEST_EV_TO_BEST_EBITDA","BEST_FPERIOD_OVERRIDE","3FY"),
#
c("BEST_EV_TO_BEST_EBIT","BEST_FPERIOD_OVERRIDE","1FY"),
c("BEST_EV_TO_BEST_EBIT","BEST_FPERIOD_OVERRIDE","2FY"),
c("BEST_EV_TO_BEST_EBIT","BEST_FPERIOD_OVERRIDE","3FY"),
#
c("BEST_CURRENT_EV_BEST_SALES","BEST_FPERIOD_OVERRIDE","1FY"),
c("BEST_CURRENT_EV_BEST_SALES","BEST_FPERIOD_OVERRIDE","2FY"),
c("BEST_CURRENT_EV_BEST_SALES","BEST_FPERIOD_OVERRIDE","3FY"),
#
c("BEST_EBIT_TO_SALES","BEST_FPERIOD_OVERRIDE","1FY"),
c("BEST_EBIT_TO_SALES","BEST_FPERIOD_OVERRIDE","2FY"),
c("BEST_EBIT_TO_SALES","BEST_FPERIOD_OVERRIDE","3FY")
)

get_fundamentals<-function(ticker,fields=comps_inputs){
  # group bbg calls by overrides used, 
  x0<-function(i)ifelse(length(i)==3,paste(i[2:3],collapse="="),"")
  x1<-mapply(x0,fields)
  x2<-mapply(head,fields,1)
  x3<-split(x2,x1)
  x4<-mapply(function(f,o)c(list(ticker,f),as.list(o)),x3,strsplit(names(x3),"="))
  x5<-mapply(function(a)do.call(blpapi::bdp,c(list(conn),a)),x4) # do.call(bdp,c(list(conn),a)),x4)
  x6<-mapply(function(b,o){
    i<-as.matrix(expand.grid(dimnames(b),stringsAsFactors = FALSE))
    cbind(i,value=b[i],override=rep(o,nrow(i)))
  },x5,names(x5),SIMPLIFY = FALSE)
  x7<-do.call(rbind,x6)
  colnames(x7)<-c("ticker","field","value","override")
  as.data.frame(x7,strigsAsFactors=FALSE)
}

append2log("create_market_data: fetch fundamentals for ticker universe")
fundamentals<-data.table(get_fundamentals(the_equity_tickers,comps_inputs))
append2log("create_market_data: save market_data/fundamentals.csv")
fwrite(fundamentals,"N:/Depts/Share/UK Alpha Team/Analytics/market_data/fundamentals.csv")

e1<-new.env()

res<-eval(expression({
  
  factor_ref_matrix<-matrix(
    0,
    ncol=length(sort(unique(factor_tickers))),
    nrow=length(all_days),
    dimnames=list(all_days,sort(unique(factor_tickers)))
  )
  
  append2log("create_market_data: fetch factor DAY_TO_DAY_TOT_RETURN_GROSS_DVDS ")
  factor_local_tret<-as.matrix(memo_populate_sheet_history_matrix(
    ref_matrix=factor_ref_matrix,
    bbg_field="DAY_TO_DAY_TOT_RETURN_GROSS_DVDS",
    bbg_overrides=NULL,
    post_fetch_fun=function(x)scrub(as.numeric(x))/100,
    verbose=TRUE,
    force=FALSE
  ))
  
  sheet_ref_matrix<-matrix(
    0,
    ncol=length(the_tickers),
    nrow=length(all_days),
    dimnames=list(all_days,the_tickers)
  )
  append2log("create_market_data: fetch ticker DAY_TO_DAY_TOT_RETURN_GROSS_DVDS ")
  portfolio_local_tret<-as.matrix(memo_populate_sheet_history_matrix(
    ref_matrix=sheet_ref_matrix,
    bbg_field="DAY_TO_DAY_TOT_RETURN_GROSS_DVDS",
    bbg_overrides=NULL,
    post_fetch_fun=function(x)scrub(as.numeric(x))/100,
    verbose=TRUE,
    force=FALSE
  ))
  append2log("create_market_data: fetch ticker VOLUME ")
  portfolio_volume<-as.matrix(memo_populate_sheet_history_matrix(
    ref_matrix=sheet_ref_matrix,
    bbg_field="VOLUME",
    bbg_overrides=NULL,
    post_fetch_fun=function(x)scrub(as.numeric(x)),
    verbose=TRUE,
    force=FALSE
  ))
  append2log("create_market_data: fetch ticker GBP DAY_TO_DAY_TOT_RETURN_GROSS_DVDS ")
  portfolio_gbp_tret<-as.matrix(memo_populate_sheet_history_matrix(
    ref_matrix=sheet_ref_matrix,
    bbg_field="DAY_TO_DAY_TOT_RETURN_GROSS_DVDS",
    bbg_overrides=c(EQY_FUND_CRNCY="GBP"),
    post_fetch_fun=function(x)scrub(as.numeric(x))/100,
    verbose=TRUE,
    force=FALSE
  ))
  append2log("create_market_data: fetch ticker EUR DAY_TO_DAY_TOT_RETURN_GROSS_DVDS ")
  portfolio_eur_tret<-as.matrix(memo_populate_sheet_history_matrix(
    ref_matrix=sheet_ref_matrix,
    bbg_field="DAY_TO_DAY_TOT_RETURN_GROSS_DVDS",
    bbg_overrides=c(EQY_FUND_CRNCY="EUR"),
    post_fetch_fun=function(x)scrub(as.numeric(x))/100,
    verbose=TRUE,
    force=FALSE
  ))
  append2log("create_market_data: fetch ticker PX_LAST ")
  portfolio_local_px<-as.matrix(memo_populate_sheet_history_matrix(
    ref_matrix=sheet_ref_matrix,
    bbg_field="PX_LAST",
    bbg_overrides=NULL,
    post_fetch_fun=function(x)replace_zero_with_last(scrub(as.numeric(x))),
    verbose=TRUE,
    force=FALSE
  ))
  
  append2log("create_market_data: fetch ticker EUR PX_LAST ")
  portfolio_eur_px<-as.matrix(memo_populate_sheet_history_matrix(
    ref_matrix=sheet_ref_matrix,
    bbg_field="PX_LAST",
    bbg_overrides=c(EQY_FUND_CRNCY="EUR"),
    post_fetch_fun=function(x)replace_zero_with_last(scrub(as.numeric(x))),
    verbose=TRUE,
    force=FALSE
  ))
  
  append2log("create_market_data: fetch ticker GBP PX_LAST ")
  portfolio_gbp_px<-as.matrix(memo_populate_sheet_history_matrix(
    ref_matrix=sheet_ref_matrix,
    bbg_field="PX_LAST",
    bbg_overrides=c(EQY_FUND_CRNCY="GBP"),
    post_fetch_fun=function(x)replace_zero_with_last(scrub(as.numeric(x))),
    verbose=TRUE,
    force=FALSE
  ))
  
  portfolio_gbp_volume<-portfolio_volume*portfolio_gbp_px
  
  portfolio_30d_volume<-apply(tail(portfolio_gbp_volume,30),2,function(x){
    mean(nz(x))
  })
  
}),envir=e1)

data_loaded<-data.table(
  name=ls(e1),
  class=mapply(function(v)class(e1[[v]]),ls(e1)),
  rows=ifelse(mapply(function(v)class(e1[[v]]),ls(e1))=="matrix",mapply(function(v)nrow(e1[[v]]),ls(e1)),0),
  cols=ifelse(mapply(function(v)class(e1[[v]]),ls(e1))=="matrix",mapply(function(v)ncol(e1[[v]]),ls(e1)),0),
  na_count=mapply(function(v)sum(is.na(e1[[v]])),ls(e1)),
  fn=paste0("N:/Depts/Share/UK Alpha Team/Analytics/market_data/",ls(e1),".csv"),
  timestamp=rep(as.character(Sys.time()),length(ls(e1)))
)


for(i in 1:nrow(data_loaded)){
  if(data_loaded$class[i]!="matrix")next;
  v<-e1[[data_loaded$name[i]]]
  dt<-data.table(date=rownames(v),v)
  fwrite(dt,file=data_loaded$fn[i])
  append2log(paste0("create_market_data: save ",data_loaded$name[i]))
}
append2log("create_market_data: save market_data/data_loaded.csv")
fwrite(data_loaded,"N:/Depts/Share/UK Alpha Team/Analytics/market_data/data_loaded.csv")



