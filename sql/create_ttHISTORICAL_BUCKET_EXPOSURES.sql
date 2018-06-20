
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


