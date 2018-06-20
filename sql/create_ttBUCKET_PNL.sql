
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

