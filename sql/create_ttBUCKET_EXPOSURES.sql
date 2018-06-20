

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

