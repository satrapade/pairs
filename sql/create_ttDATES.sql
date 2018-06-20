

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



