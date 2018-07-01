require(RSQLite)
require(DBI)
require(data.table)
require(gsubfn)
require(magrittr)


fetch_bucket_pnl<-function(
  db,
  product_id="8",
  data_source_id="2"
 ){
 
  bucket_pnl_query_string="
    SELECT 
    tBucket.Name AS bucket,
    SUBSTRING(CONVERT(varchar,tHistoricalBucket.HistoricalDate),1,10) AS date,
    tHistoricalBucket.BucketPricePl AS pnl,
    tHistoricalBucket.BucketPricePlItd AS pnl_ltd,
    tHistoricalBucket.BucketPricePlMtd AS pnl_mtd,
    tHistoricalBucket.BucketPricePlYtd AS pnl_ytd,
    tHistoricalBucket.BucketPricePlRolling AS pnl_rolling,
    tHistoricalBucket.BucketPricePlDrawdown AS pnl_draw,
    tHistoricalBucket.NetExposure AS net,
    tHistoricalBucket.GrossExposure AS gross
    FROM tHistoricalBucket
    LEFT JOIN tBucket
    ON tBucket.BucketId = tHistoricalBucket.BucketId
    LEFT JOIN tBucket AS tParentBucket 
    ON tParentBucket.BucketId=tBucket.ParentBucketId
    LEFT JOIN tBucket AS tRootBucket 
    ON tRootBucket.BucketId=tParentBucket.ParentBucketId
    WHERE tRootBucket.Name = 'EqyBucket'
    AND tHistoricalBucket.ProductId=--R{product_id}--
    AND tHistoricalBucket.DataSourceId=--R{data_source_id}--
"

  query(make_query(
    query_string = bucket_pnl_query_string,
    product_id=product_id,
    data_source_id=data_source_id
  ),db=db)[
    grepl("[A-Z]{2,3}_PAIR_[0-9]{2,9}",bucket) 
  ]
  
 }
