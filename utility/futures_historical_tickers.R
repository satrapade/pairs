require(Rblpapi)


#
# fetch futures historical tickers
#
futures_historical_tickers<-function(futures_tickers){
  fmc<-c(
    "F"="Jan","G"="Feb","H"="Mar","J"="Apr","K"="May","M"="Jun",
    "N"="Jul","Q"="Aug","U"="Sep","V"="Oct","X"="Nov","Z"="Dec"
  )
  futures<-sort(unique(futures_tickers))  
  f2d<-function(f){
    date_string<-paste0("01-",fmc[stri_sub(f,3,3)],"-201",stri_sub(f,4,4))
    as.character(as.Date(date_string,format="%d-%b-%Y"),format="%Y-%m-%d")
  }
  lookup_table<-do.call(rbind,mapply(function(f){
    ffc<-paste0(stri_sub(f,1,2),"1 Index")
    res<-Rblpapi::bds(
      security=ffc,
      field="FUT_CHAIN", 
      options=NULL
      overrides=c(CHAIN_DATE=gsub("-","",f2d(f))),
      verbose=FALSE,
      identity=NULL
    )
    data.frame(contract=f,historical=res[1,1],row.names=NULL,stringsAsFactors=FALSE)
  },futures,SIMPLIFY=FALSE))
  lookup_table[futures_tickers,"historical"]
}

