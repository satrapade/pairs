
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

