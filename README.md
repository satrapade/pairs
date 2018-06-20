# pair_reporting
data, workflows for pair performance reporting

Scripts and results are in th ``N:\Depts\Share\UK Alpha Team\Analytics`` directory.

A task is setup on the Windows Task Manager to run every day at 6:30 in the morning:

![image](https://user-images.githubusercontent.com/1358190/41651469-8a890876-7478-11e8-9341-8c5563304c76.png)

The task is setup to run Rscript.exe:

``"C:\Program Files\R\R-3.4.3\bin\Rscript.exe"``

with this parameter:

``-e source('N:/Depts/Share/UK\x20Alpha\x20Team/Analytics/Rscripts/workflow.R')``

``workflow.R`` then executes individual workflow steps.

The  ``N:\Depts\Share\UK Alpha Team\Analytics\Rscripts`` directory contains 
all workflow scripts.


| File | Description |
|----------|----------|
| ``workflow.R`` | the start script  |
| ``workflow.log`` | results log |
| ``query_parameters.sql`` | set query parameters |
| ``relevant_equity_ticker_table.sql`` | construct date, ticker, bucket tables |
| ``create_temp_tables.sql`` |  create tables |
| ``initialize_scrape_db.R`` |  make sure the sheet scrape DB exists |
| ``perform_sheet_scrape_to_db.R`` |  scrape latest sheet |
| ``create_cix_uploads.R`` |  create ``BLOOMBERG`` CIX formuli |
| ``create_database_temp_tables.R`` | call ``SQL`` scripts  |
| ``create_market_data.R`` | fetch relevant market data from ``BLOOMBERG`` |
| ``create_market_data_intraday.R`` | fetch relevant intraday data from ``BLOOMBERG`` |
| ``create_pair_icons.R`` | create icons for live pairs |
| ``create_portfolio_summary.R`` | create portfolio summery from sheet scrapes |
| ``create_portfolio_upload.R`` | create portfolio upload files |
| ``create_risk_reports.R`` |  run .Rnw files to create risk reports |
| ``create_tsne_grid.R`` | compute 2D layout of pairs, live stocks |
| ``intraday_bank_pairs.R`` |  fetch intraday price action for SX7P components |
| ``intraday_fx.R`` |  fetch intraday price action for 30 currencies |


