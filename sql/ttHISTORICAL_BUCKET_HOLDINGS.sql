
--
--
-- HISTORICAL_BUCKET_HOLDINGS : historical bucket holdings
--
--
IF EXISTS (
  SELECT * from sys.tables WHERE name='ttHISTORICAL_BUCKET_HOLDINGS'
) BEGIN DROP TABLE ttHISTORICAL_BUCKET_HOLDINGS END;

SELECT 
  *
INTO ttHISTORICAL_BUCKET_HOLDINGS
FROM (
    SELECT
      SUBSTRING(CONVERT(varchar,tHistoricalBucketHolding.HistoricalDate),1,10) AS date,
      tBucket.Name AS bucket,
      tHistoricalBucketHolding.SecurityId AS security_id,
      tUnderlyingSecurity.SecurityId AS underlying_security_id,
      tSecurityType.Name AS security_type,
      tSecurity.Name AS security_name,
      tHistoricalBucketHolding.SecurityUnits AS security_units,
      tHistoricalProductHolding.SecurityUnits AS product_security_units,
      tCurrency.CurrencyCode AS currency,
      tHistoricalFxSecurity.UnitPrice AS fx_rate,
      tHistoricalSecurity.UnitPrice AS unit_price,
      tHistoricalSecurity.UnitAccruedIncome AS unit_accrued_income,
      (
        CASE
          WHEN tSecurityType.Name='Cash'
          THEN 1
          WHEN tHistoricalSecurity.UnitPrice IS NULL
          THEN 0
          ELSE tHistoricalSecurity.UnitPrice
        END +
        CASE
          WHEN tHistoricalSecurity.UnitAccruedIncome IS NULL
          THEN 0
          ELSE tHistoricalSecurity.UnitAccruedIncome
        END
      ) * (
        CASE 
          WHEN tCurrency.CurrencyCode = 'GBP'
          THEN 1
          ELSE 1/tHistoricalFxSecurity.UnitPrice
        END
      ) * tHistoricalBucketHolding.SecurityUnits AS market_value,  
      CASE
        WHEN tSecurityType.Name IN (
          'Equity Swap', 'Equity Index Swap', 'Equity Index Future',
          'Fx Forward', 'Fund Swap'
        ) 
        THEN tUnderlyingSecurity.SecurityId
        ELSE tHistoricalBucketHolding.SecurityId
      END AS exposure_security_id,
      CASE
        WHEN tSecurityType.Name IN ('Equity Swap', 'Fund Swap')
        AND  tUnderlyingSecurity.Isin IS NOT NULL
        THEN '/isin/'+tUnderlyingSecurity.Isin
        WHEN tSecurityType.Name IN ('Equity Swap', 'Fund Swap')
        AND  tUnderlyingSecurity.UniqueId IS NOT NULL
        THEN '/buid/'+tUnderlyingSecurity.UniqueId
        WHEN tSecurityType.Name IN ('Equity Index Swap', 'Equity Index Future') 
        AND  tUnderlyingSecurity.UniqueId IS NOT NULL
        THEN '/buid/'+tUnderlyingSecurity.UniqueId
        WHEN tSecurityType.Name IN ('Fx Forward') 
        AND  tUnderlyingSecurity.Ticker IS NOT NULL
        THEN tUnderlyingSecurity.Ticker+'CR Curncy'
        WHEN tSecurityType.Name IN ('Equity Etd', 'Fund Etd')
        AND  tSecurity.Isin IS NOT NULL
        THEN '/isin/'+tSecurity.Isin
        WHEN tSecurityType.Name IN ('Equity Etd', 'Fund Etd')
        AND  tSecurity.UniqueId IS NOT NULL
        THEN '/buid/'+tSecurity.UniqueId
        ELSE '/name/'+tSecurity.Name
      END AS exposure_security_external_id
    FROM tHistoricalBucketHolding
    LEFT JOIN tHistoricalProductHolding
    ON tHistoricalProductHolding.ProductId = tHistoricalBucketHolding.ProductId
    AND tHistoricalProductHolding.DataSourceId = tHistoricalBucketHolding.DataSourceId
    AND tHistoricalProductHolding.HistoricalDate = tHistoricalBucketHolding.HistoricalDate
    AND tHistoricalProductHolding.SecurityId = tHistoricalBucketHolding.SecurityId
    LEFT JOIN tBucket
    ON tBucket.BucketId = tHistoricalBucketHolding.BucketId
    LEFT JOIN tSecurity 
    ON tSecurity.SecurityId = tHistoricalBucketHolding.SecurityId
    LEFT JOIN tSecurityType
    ON tSecurityType.SecurityTypeId = tSecurity.SecurityTypeId
    LEFT JOIN tCurrency
    ON tCurrency.CurrencyId = tSecurity.CurrencyId
    LEFT JOIN tSecurity AS tUnderlyingSecurity
    ON tUnderlyingSecurity.SecurityId = tSecurity.UnderlyingSecurityId
    LEFT JOIN tSecurity as tFxSecurity 
    ON tFxSecurity.Ticker = 'GBP' + tCurrency.CurrencyCode 
    AND tFxSecurity.SecurityTypeId = 1
    LEFT JOIN tHistoricalSecurity 
    ON tHistoricalSecurity.SecurityId = tSecurity.SecurityId 
    AND tHistoricalSecurity.DataSourceId = tHistoricalBucketHolding.DataSourceId
    AND tHistoricalSecurity.HistoricalDate = tHistoricalBucketHolding.HistoricalDate
    LEFT JOIN tHistoricalSecurity AS tHistoricalFxSecurity
    ON tHistoricalFxSecurity.SecurityId = tFxSecurity.SecurityId
    AND tHistoricalFxSecurity.DataSourceId = tHistoricalBucketHolding.DataSourceId
    AND tHistoricalFxSecurity.HistoricalDate = tHistoricalBucketHolding.HistoricalDate
    WHERE tHistoricalBucketHolding.ProductId = --R{product_id}--
    AND  tHistoricalBucketHolding.DataSourceId = --R{position_data_source_id}--
    AND tBucket.Name IN ( SELECT bucket FROM ttBuckets )
) AS tHISTORICAL_BUCKET_HOLDINGS

