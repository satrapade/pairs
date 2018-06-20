
--include{N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/query_parameters.sql}--

--
-- tables required to compute P&L by bucket and perform
-- exposure-based "what-if" analysis
--
-- ttDATES                  : all dates in the period
-- ttBUCKETS                : all buckets in the period
-- ttHISTORICAL_BUCKETS     : saved down historical bucket pnl
-- ttBUCKET_PNL             : dense date-bucket pnl over all dates, buckets
-- ttEXPOSURE_SECURITIES    : all securities contributing to exposure-based 
--                            p&l calculation
--

--
--
-- DATES : valuation dates
--
--
IF EXISTS (
  SELECT * from sys.tables WHERE name='ttDATES'
) BEGIN DROP TABLE ttDATES END;

SELECT 
  tDATES.date,
  ROW_NUMBER() OVER(ORDER BY DATE) AS seqno
INTO ttDATES
FROM (
  --R{valuation_dates}--
) AS tDATES;

CREATE INDEX ix_dates_date ON ttDATES (date);


--
--
-- BUCKETS : all buckets
--
--
IF EXISTS (
  SELECT * from sys.tables WHERE name='ttBUCKETS'
) BEGIN DROP TABLE ttBUCKETS END;

SELECT 
  tBUCKETS.bucket AS bucket,
  ROW_NUMBER() OVER(ORDER BY bucket) AS seqno
INTO ttBUCKETS
FROM (
 SELECT 
      tBucket.Name AS bucket
    FROM tBucket
    LEFT JOIN tBucket AS tParentBucket ON tParentBucket.BucketId=tBucket.ParentBucketId
    LEFT JOIN tBucket AS tRootBucket ON tRootBucket.BucketId=tParentBucket.ParentBucketId
    WHERE tRootBucket.Name = --R{root_bucket_name}--
    AND tBucket.Name NOT IN (
      'BondEtd','Cash','CASH','CashCollateral','CertificateDeposit','CommercialPaper'
    )
) AS tBUCKETS

CREATE INDEX ix_buckets_bucket ON ttBUCKETS (bucket);

--
--
-- HISTORICAL_BUCKETS : historical buckets
--
--
IF EXISTS (
  SELECT * from sys.tables WHERE name='ttHISTORICAL_BUCKETS'
) BEGIN DROP TABLE ttHISTORICAL_BUCKETS END;

SELECT 
  *
INTO ttHISTORICAL_BUCKETS
FROM (
 SELECT
       SUBSTRING(CONVERT(varchar,tHistoricalBucket.HistoricalDate),1,10) AS date,
       tBucket.Name AS bucket,
       tHistoricalBucket.BucketPricePlItd AS bucket_pnl
  FROM tHistoricalBucket
  LEFT JOIN tBucket
  ON tBucket.BucketId =  tHistoricalBucket.BucketId
  WHERE tHistoricalBucket.ProductId = --R{product_id}--
  AND tHistoricalBucket.DataSourceId = --R{position_data_source_id}--
  AND tBucket.Name IN ( SELECT bucket FROM ttBuckets )
) AS tHISTORICAL_BUCKETS

CREATE INDEX ix_historical_buckets_date ON ttHISTORICAL_BUCKETS (date);
CREATE INDEX ix_historical_buckets_bucket ON ttHISTORICAL_BUCKETS (bucket);

--
--
-- BUCKET_PNL 
--
-- date
-- bucket
-- bucket_pnl
--
--
IF EXISTS (
  SELECT * FROM sys.tables WHERE name='ttBUCKET_PNL'
) BEGIN DROP TABLE ttBUCKET_PNL END;

SELECT 
  *
INTO ttBUCKET_PNL
FROM (
  SELECT
    tT1.date AS date,
    tT1.bucket AS bucket,
    CASE 
      WHEN ttHISTORICAL_BUCKETS.bucket_pnl IS NULL
      THEN 0
      ELSE ttHISTORICAL_BUCKETS.bucket_pnl
    END AS bucket_pnl
  FROM (
    SELECT
      ttDATES.date AS date,
      ttBUCKETS.bucket AS bucket,
      MAX(ttHISTORICAL_BUCKETS.date)
      OVER (
        PARTITION BY ttBUCKETS.bucket 
        ORDER BY ttDATES.date
      )   AS last_good_date
    FROM ttDATES
    CROSS JOIN ttBUCKETS
    LEFT JOIN ttHISTORICAL_BUCKETS
    ON ttHISTORICAL_BUCKETS.date = ttDATES.date
    AND ttHISTORICAL_BUCKETS.bucket = ttBUCKETS.bucket
  ) AS tT1
  LEFT JOIN ttHISTORICAL_BUCKETS
  ON ttHISTORICAL_BUCKETS.date = tT1.last_good_date
  AND ttHISTORICAL_BUCKETS.bucket = tT1.bucket
) AS tBUCKET_PNL

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



--
--
-- HISTORICAL_BUCKET_EXPOSURES : historical bucket exposures
--
--
IF EXISTS (
  SELECT * from sys.tables WHERE name='ttHISTORICAL_BUCKET_EXPOSURES'
) BEGIN DROP TABLE ttHISTORICAL_BUCKET_EXPOSURES END;

SELECT 
  *
INTO ttHISTORICAL_BUCKET_EXPOSURES
FROM (
  SELECT
    ttHISTORICAL_BUCKET_HOLDINGS.date AS date,
    ttHISTORICAL_BUCKET_HOLDINGS.bucket AS bucket,
    ttHISTORICAL_BUCKET_HOLDINGS.exposure_security_id AS exposure_security_id,
    MAX(ttHISTORICAL_BUCKET_HOLDINGS.exposure_security_external_id) AS exposure_security_external_id,
    MAX(ttHISTORICAL_BUCKET_HOLDINGS.security_type) AS security_type,
    SUM(ttHISTORICAL_BUCKET_HOLDINGS.security_units) AS security_units,
    SUM(ttHISTORICAL_BUCKET_HOLDINGS.market_value) AS market_value
  FROM ttHISTORICAL_BUCKET_HOLDINGS
  WHERE 'cost leg' <> CASE
    WHEN security_type IN ('Equity Swap','Equity Index Swap', 'Fund Swap')
    AND security_name LIKE '% COST %'
    THEN 'cost leg'
    WHEN security_type IN ('Equity Swap','Equity Index Swap', 'Fund Swap')
    AND security_name LIKE '% PRICE %'
    THEN 'price leg'
    ELSE 'not swap'
  END 
  GROUP BY 
    ttHISTORICAL_BUCKET_HOLDINGS.date, 
    ttHISTORICAL_BUCKET_HOLDINGS.bucket,
    ttHISTORICAL_BUCKET_HOLDINGS.exposure_security_id

) AS tHISTORICAL_BUCKET_EXPOSURES


--
--
-- EXPOSURE_SECURITIES : securities contributing to exposures
-- which require additional historical data to perfrom 
-- what-if analysis
--
-- exposure_security_id
--
IF EXISTS (
  SELECT * FROM sys.tables WHERE name='ttEXPOSURE_SECURITIES'
) BEGIN DROP TABLE ttEXPOSURE_SECURITIES END;


SELECT 
  ROW_NUMBER() OVER(ORDER BY tEXPOSURE_SECURITIES.exposure_security_external_id) as seqno,
  tEXPOSURE_SECURITIES.exposure_security_id AS exposure_security_id,
  tEXPOSURE_SECURITIES.security_type AS exposure_security_type,
  /* */
  CASE
    -- non cash equity
    WHEN tEXPOSURE_SECURITIES.security_type NOT IN ('Equity Etd', 'Fund Etd')
    THEN tEXPOSURE_SECURITIES.exposure_security_external_id 
    -- the bbg ticker is active
    WHEN ttTICKER_MARKET_STATUS.MARKET_STATUS='ACTV'
    AND tEXPOSURE_SECURITIES.security_ticker IS NOT NULL
    THEN tEXPOSURE_SECURITIES.security_ticker
    -- the buid is active
    WHEN ttBUID_MARKET_STATUS.MARKET_STATUS='ACTV'
    AND tEXPOSURE_SECURITIES.security_unique_id IS NOT NULL
    THEN '/buid/'+tEXPOSURE_SECURITIES.security_unique_id
    --
    WHEN ttISIN_MARKET_STATUS.MARKET_STATUS='ACTV'
    AND tEXPOSURE_SECURITIES.security_isin IS NOT NULL
    THEN '/isin/'+tEXPOSURE_SECURITIES.security_isin
    --
    WHEN ttHISTORICAL_EQUITY_ISIN.current_risk_isin IS NOT NULL
    THEN '/isin/'+ttHISTORICAL_EQUITY_ISIN.current_risk_isin
    --
    ELSE tEXPOSURE_SECURITIES.exposure_security_external_id 
  END AS exposure_security_external_id,
  /* */
  tEXPOSURE_SECURITIES.external_count AS external_count,
  tEXPOSURE_SECURITIES.first_date AS first_date,
  tEXPOSURE_SECURITIES.last_date AS last_date,
  --
  tEXPOSURE_SECURITIES.security_ticker AS security_ticker,
  ttTICKER_MARKET_STATUS.MARKET_STATUS AS ticker_market_status,
  --
  tEXPOSURE_SECURITIES.security_isin AS security_isin,
  ttISIN_MARKET_STATUS.MARKET_STATUS AS isin_market_status,
  --
  ttHISTORICAL_EQUITY_ISIN.current_risk_isin AS current_security_isin,
  ttCURRENT_ISIN_MARKET_STATUS.MARKET_STATUS AS current_isin_market_status,
  --
  tEXPOSURE_SECURITIES.security_unique_id AS security_unique_id,
  ttBUID_MARKET_STATUS.MARKET_STATUS AS buid_market_status,
  --
  tEXPOSURE_SECURITIES.security_sedol AS security_sedol,
  tEXPOSURE_SECURITIES.security_cusip AS security_cusip,
  tEXPOSURE_SECURITIES.security_name AS security_name
  --
INTO ttEXPOSURE_SECURITIES 
FROM (
    SELECT DISTINCT
      ttHISTORICAL_BUCKET_EXPOSURES.exposure_security_id AS exposure_security_id,
      MAX(tSecurityType.Name) AS security_type,
      MAX(ttHISTORICAL_BUCKET_EXPOSURES.exposure_security_external_id) AS exposure_security_external_id,
      COUNT(DISTINCT ttHISTORICAL_BUCKET_EXPOSURES.exposure_security_external_id) AS external_count,
      MIN(ttHISTORICAL_BUCKET_EXPOSURES.date) AS first_date,
      MAX(ttHISTORICAL_BUCKET_EXPOSURES.date) AS last_date,
      MAX(tSecurity.Ticker) AS security_ticker,
      MAX(tSecurity.Isin) AS security_isin,
      MAX(tSecurity.UniqueId) AS security_unique_id,
      MAX(tSecurity.Sedol) AS security_sedol,
      MAX(tSecurity.Cusip) AS security_cusip,
      MAX(tSecurity.Name) AS security_name
    FROM ttHISTORICAL_BUCKET_EXPOSURES
    LEFT JOIN tSecurity
    ON tSecurity.SecurityId=ttHISTORICAL_BUCKET_EXPOSURES.exposure_security_id
    LEFT JOIN tSecurityType
    ON tSecurityType.SecurityTypeId=tSecurity.SecurityTypeId
    GROUP BY ttHISTORICAL_BUCKET_EXPOSURES.exposure_security_id
) AS tEXPOSURE_SECURITIES
LEFT JOIN ttISIN_MARKET_STATUS
ON ttISIN_MARKET_STATUS.isin = tEXPOSURE_SECURITIES.security_isin
LEFT JOIN ttBUID_MARKET_STATUS
ON ttBUID_MARKET_STATUS.buid = tEXPOSURE_SECURITIES.security_unique_id
LEFT JOIN ttTICKER_MARKET_STATUS
ON ttTICKER_MARKET_STATUS.ticker = tEXPOSURE_SECURITIES.security_ticker
LEFT JOIN ttHISTORICAL_EQUITY_ISIN
ON ttHISTORICAL_EQUITY_ISIN.risk_isin = tEXPOSURE_SECURITIES.security_isin
LEFT JOIN ttCURRENT_ISIN_MARKET_STATUS 
ON ttCURRENT_ISIN_MARKET_STATUS.isin = ttHISTORICAL_EQUITY_ISIN.current_risk_isin
;


--
--
--  BUCKET_EXPOSURES : securities contributing to exposures
--
--  rolls HISTORICAL_BUCKET_EXPOSURES to fill gaps in reported bucket exposures
--  so that reported days are continous
--
--

IF EXISTS (
  SELECT * FROM sys.tables WHERE name='ttBUCKET_EXPOSURES'
) BEGIN DROP TABLE ttBUCKET_EXPOSURES END;


WITH
tT1 AS (
  SELECT
    ttDATES.date AS date,
    ttHISTORICAL_BUCKET_EXPOSURES.bucket AS bucket,
    ttHISTORICAL_BUCKET_EXPOSURES.exposure_security_id AS exposure_security_id,
    ttHISTORICAL_BUCKET_EXPOSURES.security_units AS security_units,
    ttHISTORICAL_BUCKET_EXPOSURES.market_value AS market_value,
    ttEXPOSURE_SECURITIES.first_date AS first_date,
    ttEXPOSURE_SECURITIES.last_date AS last_date
  FROM ttDATES
  LEFT JOIN ttHISTORICAL_BUCKET_EXPOSURES
  ON ttHISTORICAL_BUCKET_EXPOSURES.date = ttDATES.date
  LEFT JOIN ttEXPOSURE_SECURITIES
  ON ttEXPOSURE_SECURITIES.exposure_security_id=ttHISTORICAL_BUCKET_EXPOSURES.exposure_security_id
  WHERE ttHISTORICAL_BUCKET_EXPOSURES.bucket IN (SELECT bucket FROM ttBUCKETS)
),
tT2 AS (
  SELECT
    tT1.date AS date,
    tT1.bucket AS bucket,
    tT1.exposure_security_id AS exposure_security_id,
    ttDATES.seqno - ROW_NUMBER() OVER(PARTITION BY tT1.bucket, tT1.exposure_security_id ORDER BY tT1.date) AS seq_id
  FROM tT1
  LEFT JOIN ttDATES
  ON ttDATES.date = tT1.date
),
tT3 AS (
  SELECT
    tT2.bucket,
    tT2.exposure_security_id AS exposure_security_id,
    MIN(tT2.date) AS seq_start,
    MAX(tT2.date) AS seq_end
  FROM tT2
  GROUP BY tT2.bucket,tT2.exposure_security_id, tT2.seq_id
),
tT4 AS (
  SELECT 
    tT3.bucket AS bucket,
    tT3.exposure_security_id AS exposure_security_id,
    tT3.seq_start AS seq_start,
    tT3.seq_end AS seq_end,
    ROW_NUMBER() OVER(PARTITION BY tT3.bucket, tT3.exposure_security_id ORDER BY tT3.seq_end) AS seqno
  FROM tT3
),
tT5 AS (
  SELECT
    tT4.bucket AS bucket,
    tT4.exposure_security_id AS exposure_security_id,
    tT4.seq_end AS last_good_day,
    nxt_tT4.seq_start AS first_good_day,
    ttHISTORICAL_BUCKET_EXPOSURES.security_units AS last_good_security_units,
    ttHISTORICAL_BUCKET_EXPOSURES.market_value AS last_good_market_value
  FROM tT4
  INNER JOIN tT4 AS nxt_tT4
  ON nxt_tT4.seqno = tT4.seqno+1
  AND nxt_tT4.bucket = tT4.bucket
  AND nxt_tT4.exposure_security_id = tT4.exposure_security_id
  LEFT JOIN ttHISTORICAL_BUCKET_EXPOSURES 
  ON ttHISTORICAL_BUCKET_EXPOSURES.bucket = tT4.bucket
  AND ttHISTORICAL_BUCKET_EXPOSURES.exposure_security_id = tT4.exposure_security_id
  AND ttHISTORICAL_BUCKET_EXPOSURES.date = tT4.seq_end
),
tT6 AS (
  SELECT
    ttDATES.date AS date,
    tT5.bucket AS bucket,
    tT5.exposure_security_id AS exposure_security_id,
    ttEXPOSURE_SECURITIES.exposure_security_external_id AS exposure_security_external_id,
    ttEXPOSURE_SECURITIES.exposure_security_type AS exposure_security_type,
    tT5.last_good_security_units AS security_units,
    tT5.last_good_market_value AS market_value,
    'rolled' AS source_type
  FROM ttDATES
  INNER JOIN tT5
  ON  tT5.last_good_day   < ttDATES.date
  AND tT5.first_good_day  > ttDATES.date
  LEFT JOIN ttEXPOSURE_SECURITIES
  ON ttEXPOSURE_SECURITIES.exposure_security_id=tT5.exposure_security_id
  UNION
  SELECT
    ttHISTORICAL_BUCKET_EXPOSURES.date AS date,
    ttHISTORICAL_BUCKET_EXPOSURES.bucket AS bucket,
    ttHISTORICAL_BUCKET_EXPOSURES.exposure_security_id AS exposure_security_id,
    ttEXPOSURE_SECURITIES.exposure_security_external_id AS exposure_security_external_id,
    ttEXPOSURE_SECURITIES.exposure_security_type AS exposure_security_type,
    ttHISTORICAL_BUCKET_EXPOSURES.security_units AS security_units,
    ttHISTORICAL_BUCKET_EXPOSURES.market_value AS market_value,
    'stored' AS source_type
  FROM ttHISTORICAL_BUCKET_EXPOSURES
  LEFT JOIN ttEXPOSURE_SECURITIES
  ON ttEXPOSURE_SECURITIES.exposure_security_id=ttHISTORICAL_BUCKET_EXPOSURES.exposure_security_id
  WHERE ttHISTORICAL_BUCKET_EXPOSURES.bucket IN (SELECT bucket FROM ttBUCKETS)
  AND ttHISTORICAL_BUCKET_EXPOSURES.date IN (SELECT date FROM ttDATES)
)
SELECT 
  *
INTO ttBUCKET_EXPOSURES 
FROM  tT6
;





