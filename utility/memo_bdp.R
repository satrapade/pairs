require(R.cache)
require(Rblpapi)

memo_bdp <- function(
  securities, 
  fields = "PX_LAST", 
  overrides = NULL, 
  force = FALSE, 
  verbose = FALSE,
  ticker_xlat_fun=function(ticker)gsub(
    pattern=ticker_modifier$pattern,
    ticker_modifier$replacement,
    ticker
  ),
  ticker_modifier=list(pattern="",replacement=""),
  cache_control_value=as.character(Sys.Date())
){
  key <- list(securities, fields, overrides, cache_control_value,"memo_bdp")
  
  cached.value <- loadCache(key = key)
  
  if(!is.null(cached.value) & !force)
  {
    if(verbose)warning(
      "memo_bdp: using cached value for ", 
      paste(c(fields, overrides), collapse = ", "), 
      ".", 
      call. = FALSE
    )
    return(cached.value)
  }
  if(verbose)warning(
    "memo_bdp: accessing bloomberg for ", 
    paste(c(fields, overrides), collapse = ", "), 
    ".", 
    call. = FALSE
  )
  
  modified_securities <- gsub(
    pattern=ticker_modifier$pattern,
    replacement=ticker_modifier$replacement,
    securities
  )
  
  res <- Rblpapi::bdp(
    securities=modified_securities, 
    fields=fields, 
    overrides=overrides
  )
  
  saveCache(res, key = key)
  
  return(res)
}

