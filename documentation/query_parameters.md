# query parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| start_date | only lines with ``HistoricalDate`` greater than this are selected | ``'2015-05-28'`` |           
|  end_date  |  only lines with ``HistoricalDate`` less than this are selected  | ``'2018-05-01'`` |           
|  valuation_dates | full, explicit, vector of all dates | ``make_date_range(start_date,end_date)`` |
|  root_bucket_name | start of recursive bucket traversal | ``'EqyBucket'`` |       
|  product_id | only lines with  ``ProductId`` equal to this are selected | 8 |             
|  data_source_id | only lines with  ``DataSourceId`` equal to this are selected | 2 |  



