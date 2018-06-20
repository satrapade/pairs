# query parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| start_date | only lines with ``HistoricalDate`` greater than this are selected | ``'2015-05-28'`` |           
|  end_date  |  only lines with ``HistoricalDate`` less than this are selected  | ``'2018-05-01'`` |           
|  valuation_dates | full, explicit, vector of all dates | ``make_date_range(start_date,end_date)`` |
|  root_bucket_name | start of recursive bucket traversal | ``'EqyBucket'`` |       
|  product_id | only lines with  ``ProductId`` equal to this are selected | 8 |             
|  data_source_id | only lines with  ``DataSourceId`` equal to this are selected | 2 |  


the ``make_query`` function takes an SQL query containing pre-processor directives and expands
the directives into concrete values. The resulting text is a valid SQL query that is passed to
the database.

Example call:

```r
make_query(
  product_id="7",
  data_source_id="1",
  start_date="'2011-05-28'",
  start_date_search="'2011-05-28'",
  file="N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/relevant_equity_ticker_table.sql"
)
```

The ``.sql`` file with macros looks like this:

```
WHERE tHistoricalBucketHolding.ProductId = --R{product_id}--
AND   tHistoricalBucketHolding.DataSourceId = --R{position_data_source_id}--
AND   tHistoricalBucketHolding.HistoricalDate >= --R{start_date}-- +' 12:00:00.0000000'
AND   tHistoricalBucketHolding.HistoricalDate <= --R{end_date}-- +' 12:00:00.0000000'
AND   tSecurityType.Name IN ('Equity Swap', 'Equity Etd', 'Fund Swap', 'Fund Etd')
```

There are 2 macro commands: ``--R{<expr>}--`` and ``--include{<filename>}--``, where
``<expr>`` is a valid R expression and ``<filename>>`` is avalid filename.

``--R{ expr }--`` just evaluates the expression inside the curly braces and replaces the macro text
with whatever value we get from ``as.character(eval(expr))``.

``--include{filename}--`` just replaces the macro text witht the contents of the file name.




