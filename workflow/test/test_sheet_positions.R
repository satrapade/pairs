#
# fetch sheet positions, take 2
#
require(R.cache)
require(stringi)
require(data.table)
require(readxl)
require(ggplot2)
require(magrittr)
library(Rblpapi)
rcon<-Rblpapi::blpConnect()

#
replace_zero_with_last<-function(x,a=x!=0)x[which(a)[c(1,1:sum(a))][cumsum(a)+1]]

#
scrub<-function(x)
{
  if(length(x)==0)return(0)
  x[which(!is.finite(x))]<-0
  x
}

make_date_range<-function(
  start="2017-06-01",
  end="2017-06-30",
  leading_days=0,
  trailing_days=0
){
  fmt="%Y-%m-%d"
  date_seq<-seq(from=as.Date(start,format=fmt)-leading_days,to=as.Date(end,format=fmt)+trailing_days,by=1)
  as.character(date_seq,format=fmt)
}

#
ticker_class<-function(x)
{
  
  x_trim<-stri_trim(x)
  x_upper<-toupper(x_trim)
  x_lower<-tolower(x_trim)
  
  all_matches<-list(
    list(class="date",match=grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$",x_trim)),
    list(class="equity",match=grepl("^[A-Z0-9/]+\\s+[A-Z]{2,2}$",x_trim)),
    list(class="equity",match=grepl("^[A-Z0-9/]+\\s+[A-Z]{2,2}\\sEquity$",x_trim)),
    list(class="equity",match=grepl("^[a-z0-9/]+\\s+[a-z]{2,2}\\sequity$",x_lower)),
    list(class="index",match=grepl("^[A-Z0-9]{3,20}$",x_trim)),
    list(class="index",match=grepl("^[A-Z0-9]{3,20}\\sIndex$",x_trim)),
    list(class="cix",match=grepl("^\\.[A-Z0-9]{3,20}\\sIndex$",x_trim)),
    list(class="index",match=grepl("^[a-z0-9]{3,20}\\sindex$",x_lower)),
    list(class="index",match=grepl("^[A-Z0-9]+\\s+[A-Z]{2}\\s+Index$",x_trim)),
    list(class="future",match=grepl("^([A-Z]{2,4}|[A-Z]\\s)([FGHJKMNQUVXZ]\\d{1,2})$",x_trim)),
    list(class="future",match=grepl("^([A-Z]{2,4}|[A-Z]\\s)([FGHJKMNQUVXZ]\\d{1,2}) Index$",x_trim)),
    list(class="future",match=grepl("^([A-Z]{2,4}|[A-Z]\\s)([FGHJKMNQUVXZ]\\d{1,2})$",x_upper)),
    list(class="future",match=grepl("^([A-Z]{2,4}|[A-Z]\\s)([FGHJKMNQUVXZ]\\d{1,2}) INDEX$",x_upper)),
    list(class="bbgisin",match=grepl("^/isin/[A-Z]{2}[A-Z0-9]{10}$",x_trim)),
    list(class="isin",match=grepl("^[A-Z]{2}[A-Z0-9]{10}$",x_trim)),
    list(class="equity_option",match=grepl("^[A-Z0-9]{1,10} [A-Z0-9]{2} [0-9]{2}/[0-9]{2}/[0-9]{2} [CP]{1}[\\.0-9]{1,5}$",x_trim)),
    list(class="otc_equity_option",match=grepl("^OTC-[A-Z0-9]{1,10} [A-Z0-9]{2} [0-9]{2}/[0-9]{2}/[0-9]{2} [CP]{1}[\\.0-9]{1,5}$",x_trim)),
    list(class="index_option",match=grepl("^[A-Z0-9]{1,10} [0-9]{2}/[0-9]{2}/[0-9]{2} [CP]{1}[\\.0-9]{1,5}$",x_trim)),
    list(class="index_option",match=grepl("^[A-Z0-9]{1,10} [0-9]{1,2} [CP]{1}[\\.0-9]{1,5}$",x_trim)),
    list(class="otc_index_option",match=grepl("^OTC-[A-Z0-9]{1,10} [0-9]{2}/[0-9]{2}/[0-9]{2} [CP]{1}[\\.0-9]{1,5}$",x_trim)),
    list(class="forex",match=grepl("^[A-Z]{3,20}\\sCurncy$",x_trim))
  )
  all_classes<-do.call(cbind,mapply(function(m)ifelse(m$match,m$class,"nomatch"),all_matches,SIMPLIFY=FALSE))
  ticker_patterns<-apply(all_classes,1,function(a)paste(sort(unique(a)),collapse="|"))
  
  ticker_patterns
}

#
populate_history_matrix<-function(
  tickers,
  field,
  start,
  end,
  overrides=NULL
){
  all_dates<-as.character(seq(
    min(as.Date(start),as.Date(end)),
    max(as.Date(start),as.Date(end)),
    by=1
  ),format="%Y-%m-%d")
  bbg_res<-Rblpapi::bdh(
    tickers,
    field,
    as.Date(min(all_dates)),
    as.Date(max(all_dates)),
    overrides=overrides
  )
  df<-data.frame(
    ticker=do.call(c,mapply(rep,names(bbg_res),mapply(nrow,bbg_res),SIMPLIFY=FALSE)),
    date=as.character(do.call(c,mapply("[[",bbg_res,MoreArgs=list("date"),SIMPLIFY=FALSE)),format="%Y-%m-%d"),
    value=do.call(c,mapply("[[",bbg_res,MoreArgs=list(field),SIMPLIFY=FALSE)),
    row.names=NULL,
    stringsAsFactors=FALSE
  )
  #return(df)
  m<-sparseMatrix(#
    i=match(df$date,all_dates),
    j=match(df$ticker,tickers),   
    x=df$value,
    dims=c(length(all_dates),length(tickers)),
    dimnames=list(all_dates,tickers)
  )
  return(m)
}


memo_populate_history_matrix<-function(
  ref_matrix=local({
    warning("memo_populate_history_matrix: \"ref_matrix\" argument not supplied, stopping",call.=FALSE)
    stop
  }),
  bbg_field="PX_LAST",
  bbg_overrides=NULL,
  post_fetch_fun=function(x)scrub(as.numeric(x))/100,
  force=FALSE,
  verbose=FALSE
){
  if(any(ticker_class(colnames(ref_matrix))=="nomatch")){
    warning("memo_populate_history_matrix: invalid \"ref_matrix\" argument",call.=FALSE)
    return(NULL)
  }
  the_key<-list(dimnames(ref_matrix),bbg_field,bbg_overrides,"memo_populate_sheet_history_matrix")
  cached_value<-loadCache(key=the_key)
  if(!is.null(cached_value))if(!force){
    if(verbose)warning(
      "memo_populate_history_matrix: using cached value for ",paste(c(bbg_field,bbg_overrides),collapse=", "),".",call.=FALSE
    )
    return(cached_value)
  }
  if(verbose)warning(
    "memo_populate_history_matrix: accessing bloomberg for ",paste(c(bbg_field,bbg_overrides),collapse=", "),".",call.=FALSE
  )
  tickers<-colnames(ref_matrix)
  res<-populate_history_matrix(
    tickers,
    bbg_field,
    min(rownames(ref_matrix)),
    max(rownames(ref_matrix)),
    overrides=bbg_overrides
  )
  cached_value<-apply(res,2,post_fetch_fun)
  dimnames(cached_value)<-dimnames(ref_matrix)
  attributes(cached_value)$bbg_overrides=bbg_overrides
  attributes(cached_value)$bbg_field=bbg_field
  attributes(cached_value)$post_fetch_fun=post_fetch_fun
  saveCache(cached_value,key=the_key)
  cached_value
}

#
NNcast<-function(
  data,
  i_name="date",
  j_name="id",
  v_name="value",
  fun=sum,
  scrub_fun=function(x)scrub(x,default=0),
  scrub=function(x, default = 0){
    if(length(x) == 0) return(default)
    x[which(!is.finite(x))] <- default
    return(x)
  },
  default_value=NA
){
  i_expr<-parse(text=as.character(i_name))
  j_expr<-parse(text=as.character(j_name))
  v_expr<-parse(text=as.character(v_name))
  i<-as.character(eval(i_expr,envir=data))
  j<-as.character(eval(j_expr,envir=data))
  x<-eval(v_expr,envir=data)
  df<-data.table(i=i,j=j,x=x)[,.(x=fun(x)),keyby="i,j"]
  is<-sort(unique(df$i))
  js<-sort(unique(df$j))
  res<-matrix(
    default_value,
    nrow=length(is),
    ncol=length(js),
    dimnames = list(is,js)
  )
  i<-match(df$i,rownames(res))
  j<-match(df$j,colnames(res))
  res[cbind(i,j)[!is.na(df$x),]]<-df$x[!is.na(df$x)]
  scrub_fun(res)
}

require(stringi)
require(data.table)
require(readxl)
require(ggplot2)
require(magrittr)
library(Rblpapi)

scrape_duke<-function(
  fname="N:/Depts/Global/Absolute Insight/UK Equity/AbsoluteUK xp final.xlsm"
){
  
  rcon<-Rblpapi::blpConnect()
  # DUKE AUM 
  duke_summary_scrape<-data.table(read_xlsx(
    path=fname,
    sheet="DUKE Summary",
    range="A4:B11",
    col_names = c("A","B"),
    col_types = "text"
  ))[,.(
    parameter=make.names(A),
    value=scrub(as.numeric(B))
  )][,.SD,keyby=parameter]
  
  # DUKE open positions
  duke_position_scrape<-data.table(read_xlsx(
    path=fname, 
    sheet = "DUKE Open Trades", 
    range = "B10:P2000", 
    col_names = c("B","C","D","E","F","G","H","I","J","K","L","M","N","O","P"), 
    col_types = "text"
  ))[grepl("^[A-Z]{2,4}[0-9]{1,3}$",stri_trim(toupper(B)))][,.(
    row=seq_along(C),
    manager=toupper(C),
    pair=toupper(B[B!=""][findInterval(1:length(B),which(B!=""),all.inside = TRUE,rightmost.closed = TRUE)]),
    direction=toupper(D),
    ticker=local({
      x0<-gsub("[ ]{2,10}"," ",F)
      x1<-toupper(x0)
      x2<-gsub("EQUITY$","Equity",x1)
      x3<-gsub("INDEX$","Index",x2)
      ifelse(is.na(x3),"",x3)
    }),
    units=scrub(as.integer(I)),
    multiplier=scrub(as.integer(H)),
    quantity=scrub(as.integer(H))*scrub(as.integer(I)),
    price=scrub(as.numeric(G)),
    cash=(-1)*scrub(as.numeric(J)),
    asset_value=scrub(as.numeric(N)),
    pnl=scrub(as.numeric(O)),
    bps=scrub(as.numeric(P)),
    fx=scrub(as.numeric(M)),
    local_asset_value=scrub(as.numeric(L)),
    class=ticker_class(gsub("INDEX$","Index",gsub("EQUITY$","Equity",toupper(F))))
  )][,c(.SD,list(
    risk_ticker=local({
      res<-Rblpapi::bdp(unique(ticker[grepl("future",class)]),"UNDL_SPOT_TICKER")
      res1<-data.table(
        sheet_ticker=rownames(res),
        risk_ticker=paste0(res$UNDL_SPOT_TICKER," Index")
      )[,.SD,keyby=sheet_ticker]
      res2<-res1[J(ticker),risk_ticker]
      ifelse(is.na(res2),ticker,res2)
    })
  ))][ticker!=""][,.(
    manager=unique(manager),
    exposure=sum(asset_value)/duke_summary_scrape["Current.fund",value]
  ),keyby=c("pair","risk_ticker")][,.(
    manager,
    pair,
    ticker=risk_ticker,
    exposure=round(exposure*10000,digits=1)
  )]
  
  duke_position_scrape
  
}

#
# form reference matrix
#

duke_position_scrape<-scrape_duke()

all_tickers<-sort(unique(duke_position_scrape$ticker))

all_days<-make_date_range(
  start = as.character(Sys.Date()-365*2,format="%Y-%m-%d"),
  end = as.character(Sys.Date(),format="%Y-%m-%d"),
  leading_days = 0,
  trailing_days = 0
)

ref_matrix<-matrix(
    0,
    ncol=length(all_tickers),
    nrow=length(all_days),
    dimnames=list(all_days,all_tickers)
)

# load total returns

local_tret<-as.matrix(memo_populate_history_matrix(
    ref_matrix=ref_matrix,
    bbg_field="DAY_TO_DAY_TOT_RETURN_GROSS_DVDS",
    bbg_overrides=NULL,
    post_fetch_fun=function(x)scrub(as.numeric(x))/100,
    verbose=TRUE,
    force=FALSE
))



  
factor_tickers<-c(
  "UKX Index",
  "MCX Index",
  "SMX Index",
  "DAX Index",
  "CAC Index",
  ################## bloomberg indices
  "PMOMENUS Index",
  ################## SG factors
  "SGBVPMEU Index",
  "SGBVPHVE Index",
  "SGIXTFEQ Index",
  "SGIXTFFX Index",
  "SGIXTFIR Index",
  "SGIXGCM Index",
  "SGIXTFCY Index",
  ################## RW picks
  "SXPARO Index",
  ################## GS themes
  "GSTHVISP Index",
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
  "MSSTHYDS Index",
  ################# MW TOPS
  "FXB US Equity",
  "GLD US Equity",
  "EEM US Equity",
  "VXX US Equity",
  "TLT US Equity",
  "USO US Equity",
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

factor_ref_matrix<-matrix(
    0,
    ncol=length(factor_tickers),
    nrow=length(all_days),
    dimnames=list(all_days,factor_tickers)
)

# load total returns

factor_local_tret<-as.matrix(memo_populate_history_matrix(
    ref_matrix=factor_ref_matrix,
    bbg_field="DAY_TO_DAY_TOT_RETURN_GROSS_DVDS",
    bbg_overrides=NULL,
    post_fetch_fun=function(x)scrub(as.numeric(x))/100,
    verbose=TRUE,
    force=FALSE
))

#
#
#


get_single_bar<-function(
  ticker,
  start=Sys.time()-60*60*24*14,
  end=Sys.time(),
  force=FALSE
){
  the_key<-list("get_single_bar",ticker,start,end)
  cached_value<-loadCache(key=the_key)
  if(!is.null(cached_value))if(!force)return(cached_value)
  res<-Rblpapi::getBars(
    ticker,
    barInterval = 10,
    startTime = start,
    endTime = end
  )
  cached_value<-data.table(ticker=rep(ticker,nrow(res)),res)
  saveCache(cached_value,key=the_key)
  cached_value
}

bars<-do.call(
  rbind,
  mapply(
    get_single_bar,
    sort(unique(c(all_tickers,factor_tickers))),
    MoreArgs=list( 
      start=as.POSIXct.Date(Sys.Date())-60*60*24*14,
      end=as.POSIXct.Date(Sys.Date())
    ),
    SIMPLIFY=FALSE
  )
)


bar_matrix<-apply(NNcast(
  bars,
  i_name="times",
  j_name="ticker",
  v_name="close",
  fun=sum
),2,function(x)x%>%replace_zero_with_last%>%{diff(.)/head(.,-1)})



#
#
#

manager_ptf<-mapply(
  function(the_manager)structure(cbind(
    duke_position_scrape[,.(exposure=sum(ifelse(grepl(the_manager,manager),exposure,0))),keyby=ticker][,exposure]
  ),dimnames=list(sort(unique(duke_position_scrape$ticker)),the_manager)),
  c(paste0("^",sort(unique(duke_position_scrape$manager)),"$"),"*")
)

duke<-local_tret%*%manager_ptf

intraday_duke<-bar_matrix[,all_tickers]%*%manager_ptf

inv_factor_cov<-solve(cov(factor_local_tret))
intraday_inv_factor_cov<-solve(cov(bar_matrix[,factor_tickers]))
  
duke_explain_all<- factor_local_tret %*% t(cov(duke,factor_local_tret) %*% inv_factor_cov)
intraday_duke_explain_all<- bar_matrix[,factor_tickers] %*% t(cov(intraday_duke,bar_matrix[,factor_tickers]) %*% intraday_inv_factor_cov)

duke_specific_all <- duke - duke_explain_all  
intraday_duke_specific_all <- intraday_duke - intraday_duke_explain_all  

g1<-rbind(
  data.table(
    date=as.Date(rownames(duke_specific_all),format="%Y-%m-%d"),
    pnl=cumsum(duke_specific_all[,"*"]),
    what="specific"
  ),
  data.table(
     date=as.Date(rownames(duke),format="%Y-%m-%d"),
     pnl=cumsum(duke[,"*"]),
     what="all"
  ),
  data.table(
     date=as.Date(rownames(duke_explain_all),format="%Y-%m-%d"),
     pnl=cumsum(duke_explain_all[,"*"]),
     what="explained"
  )
) %>% 
  ggplot() +
  geom_line(aes(x=date,y=pnl,col=what),size=2,alpha=0.75)
  

plot(g1)


g2<-rbind(
  data.table(
    bar=1:nrow(intraday_duke_specific_all),
    pnl=cumsum(intraday_duke_specific_all[,"*"]),
    what="specific"
  ),
  data.table(
     bar=1:nrow(intraday_duke),
     pnl=cumsum(intraday_duke[,"*"]),
     what="all"
  ),
  data.table(
     bar=1:nrow(intraday_duke_explain_all),
     pnl=cumsum(intraday_duke_explain_all[,"*"]),
     what="explained"
  )
) %>% 
  ggplot() +
  geom_line(aes(x=bar,y=pnl,col=what),size=2,alpha=0.75)
  

plot(g2)



