

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
