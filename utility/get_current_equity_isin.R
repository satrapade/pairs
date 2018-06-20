

get_current_equity_isin<-function(date,isin){
  current_isin<-memo_bdp(
    securities="JNJ US Equity", # placeholder, not used
    fields="HISTORICAL_ID_POINT_TIME",
    overrides=c(
      "HISTORICAL_IDS_INPUT_TYPE"="isin",
      "HISTORICAL_STARTING_IDENTIFIER"=isin,
      "HISTORICAL_ID_TM_RANGE_START_DT"=gsub("-","",date)
    ),
    verbose=TRUE,
    force = FALSE,
    cache_control_value="static"
  )
  return(current_isin$HISTORICAL_ID_POINT_TIME[1])
}


