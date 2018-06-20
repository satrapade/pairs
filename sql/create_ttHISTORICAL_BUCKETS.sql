
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


