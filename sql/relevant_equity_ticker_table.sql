--
--  get relevant equity tickers
--
-- +' 12:00:00.0000000'
--

--include{N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/query_parameters.sql}--


SELECT DISTINCT
  CASE tSecurityType.Name
    WHEN 'Equity Swap' THEN tUnderlyingSecurity.SecurityId
    WHEN 'Fund Swap' THEN tUnderlyingSecurity.SecurityId
    WHEN 'Equity Etd' THEN tSecurity.SecurityId
    WHEN 'Fund Etd' THEN tSecurity.SecurityId
    ELSE NULL
  END AS security_id,
  CASE tSecurityType.Name
    WHEN 'Equity Swap' THEN tUnderlyingSecurity.Name
    WHEN 'Fund Swap' THEN tUnderlyingSecurity.Name
    WHEN 'Equity Etd' THEN tSecurity.Name
    WHEN 'Fund Etd' THEN tSecurity.Name
    ELSE NULL
  END AS security_name,
  CASE tSecurityType.Name
    WHEN 'Equity Swap' THEN tUnderlyingSecurity.Ticker
    WHEN 'Fund Swap' THEN tUnderlyingSecurity.Ticker
    WHEN 'Equity Etd' THEN tSecurity.Ticker
    WHEN 'Fund Etd' THEN tSecurity.Ticker
    ELSE NULL
  END AS security_ticker,
  CASE tSecurityType.Name
    WHEN 'Equity Swap' THEN tUnderlyingSecurity.UniqueId
    WHEN 'Fund Swap' THEN tUnderlyingSecurity.UniqueId
    WHEN 'Equity Etd' THEN tSecurity.UniqueId
    WHEN 'Fund Etd' THEN tSecurity.UniqueId
    ELSE NULL
  END AS security_buid,
  CASE tSecurityType.Name
    WHEN 'Equity Swap' THEN tUnderlyingSecurity.Isin
    WHEN 'Fund Swap' THEN tUnderlyingSecurity.Isin
    WHEN 'Equity Etd' THEN tSecurity.Isin
    WHEN 'Fund Etd' THEN tSecurity.Isin
    ELSE NULL
  END AS security_isin,
  CASE tSecurityType.Name
    WHEN 'Equity Swap' THEN tUnderlyingSecurity.Sedol
    WHEN 'Fund Swap' THEN tUnderlyingSecurity.Sedol
    WHEN 'Equity Etd' THEN tSecurity.Sedol
    WHEN 'Fund Etd' THEN tSecurity.Sedol
    ELSE NULL
  END AS security_sedol,
  CASE tSecurityType.Name
    WHEN 'Equity Swap' THEN tUnderlyingSecurity.Cusip
    WHEN 'Fund Swap' THEN tUnderlyingSecurity.Cusip
    WHEN 'Equity Etd' THEN tSecurity.Cusip
    WHEN 'Fund Etd' THEN tSecurity.Cusip
    ELSE NULL
  END AS security_cusip,
  MIN(SUBSTRING(CONVERT(varchar,tHistoricalBucketHolding.HistoricalDate),1,10)) 
  OVER(
    PARTITION BY CASE tSecurityType.Name
      WHEN 'Equity Swap' THEN tUnderlyingSecurity.SecurityId
      WHEN 'Fund Swap' THEN tUnderlyingSecurity.SecurityId
      WHEN 'Equity Etd' THEN tSecurity.SecurityId
      WHEN 'Fund Etd' THEN tSecurity.SecurityId
      ELSE NULL
    END
  ) AS date
FROM  tHistoricalBucketHolding
LEFT JOIN tSecurity
ON tSecurity.SecurityId = tHistoricalBucketHolding.SecurityId
LEFT JOIN tSecurityType
ON tSecurityType.SecurityTypeId=tSecurity.SecurityTypeId
LEFT JOIN tSecurity AS tUnderlyingSecurity
ON tUnderlyingSecurity.SecurityId = tSecurity.UnderlyingSecurityId
LEFT JOIN tSecurityType AS tUnderlyingSecurityType
ON tUnderlyingSecurityType.SecurityTypeId = tUnderlyingSecurity.SecurityTypeId
WHERE tHistoricalBucketHolding.ProductId = --R{product_id}--
AND   tHistoricalBucketHolding.DataSourceId = --R{position_data_source_id}--
AND   tHistoricalBucketHolding.HistoricalDate >= --R{start_date}-- +' 12:00:00.0000000'
AND   tHistoricalBucketHolding.HistoricalDate <= --R{end_date}-- +' 12:00:00.0000000'
AND   tSecurityType.Name IN ('Equity Swap', 'Equity Etd', 'Fund Swap', 'Fund Etd')


