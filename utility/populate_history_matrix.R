require(Rblpapi)


populate_history_matrix <- function(tickers, field, start, end, overrides)
{
  all.dates <- make_date_range(start, end)
  
  res <- Rblpapi::bdh(
    tickers, 
    field, 
    as.Date(min(all.dates),format="%Y-%m-%d"), 
    as.Date(max(all.dates),format="%Y-%m-%d"), 
    overrides=overrides,
    include.non.trading.days=TRUE
  )
  
  df <- data.frame(
    ticker = do.call(c, mapply(rep, names(res), mapply(nrow, res), SIMPLIFY = FALSE)),
    date = as.character(do.call(
      c, 
      mapply("[[", res, MoreArgs = list("date"), SIMPLIFY = FALSE)
    ), format = "%Y-%m-%d"),
    value = do.call(c, mapply("[[", res, MoreArgs = list(field), SIMPLIFY = FALSE)),
    row.names = NULL,
    stringsAsFactors = FALSE)
  
  sm <- sparseMatrix(
    i = match(df$date, all.dates),
    j = match(df$ticker, tickers),
    x = df$value,
    dims = c(length(all.dates), length(tickers)),
    dimnames = list(all.dates, tickers))
  
  return(sm)
}


