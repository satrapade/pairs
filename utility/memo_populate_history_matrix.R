require(R.cache)

memo_populate_history_matrix <- function(
  ref.matrix, 
  post.function = replace_zero_with_last, 
  field = "PX_LAST", 
  overrides = NULL, 
  force = FALSE, 
  verbose = FALSE,
  ticker_xlat_fun=function(ticker)gsub(
    pattern=ticker_modifier$pattern,
    ticker_modifier$replacement,
    ticker
  ),
  ticker_modifier=list(pattern="",replacement="")
){
  key <- list(dimnames(ref.matrix), field, overrides, "memo_populate_history_matrix")
  
  cached.value <- loadCache(key = key)
  
  if(!is.null(cached.value) & !force)
  {
    if(verbose)warning(
      "memo_populate_history_matrix: using cached value for ", 
      paste(c(field, overrides), collapse = ", "), 
      ".", 
      call. = FALSE
    )
    return(cached.value)
  }
  if(verbose)warning(
    "memo_populate_history_matrix: accessing bloomberg for ", 
    paste(c(field, overrides), collapse = ", "), 
    ".", 
    call. = FALSE
  )
  
  tickers <- ticker_xlat_fun(colnames(ref.matrix))
  
  res <- populate_history_matrix(
    tickers, 
    field, 
    min(rownames(ref.matrix)), 
    max(rownames(ref.matrix)), 
    overrides
  )
  
  cached.value <- apply(res, 2, post.function)
  
  dimnames(cached.value) <- dimnames(ref.matrix)
  
  saveCache(cached.value, key = key)
  
  return(cached.value)
}


