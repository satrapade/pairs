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


| File | Description | Documentation |
|----------|----------|----------|
| ``workflow.R`` | the start script  |[here](documentation/workflow.md)|
| ``workflow.log`` | results log |[here](documentation/workflow.md)|
| ``query_parameters.sql`` | set query parameters |[here](documentation/query_parameters.md)|
| ``relevant_equity_ticker_table.sql`` | construct date, ticker, bucket tables |[here](documentation/relevant_equity_ticker_table.md)|
| ``create_temp_tables.sql`` |  create tables |[here](documentation/create_temp_tables.md)|
| ``initialize_scrape_db.R`` |  make sure the sheet scrape DB exists |[here](documentation/initialize_scrape_db.md)|
| ``perform_sheet_scrape_to_db.R`` |  scrape latest sheet |[here](documentation/perform_sheet_scrape_to_db.md)|
| ``create_cix_uploads.R`` |  create ``BLOOMBERG`` CIX formuli |[here](documentation/create_cix_uploads.md)|
| ``create_database_temp_tables.R`` | call ``SQL`` scripts  |[here](documentation/create_database_temp_tables.md)|
| ``create_market_data.R`` | fetch relevant market data from ``BLOOMBERG`` |[here](documentation/create_market_data.md)|
| ``create_market_data_intraday.R`` | fetch relevant intraday data from ``BLOOMBERG`` |[here](documentation/create_market_data_intraday.md)|
| ``create_pair_icons.R`` | create icons for live pairs |[here](documentation/create_pair_icons.md)|
| ``create_portfolio_summary.R`` | create portfolio summery from sheet scrapes |[here](documentation/create_portfolio_summary.md)|
| ``create_portfolio_upload.R`` | create portfolio upload files |[here](documentation/create_portfolio_upload.md)|
| ``create_risk_reports.R`` |  run .Rnw files to create risk reports |[here](documentation/create_risk_reports.md)|
| ``create_tsne_grid.R`` | compute 2D layout of pairs, live stocks |[here](documentation/create_tsne_grid.md)|
| ``intraday_bank_pairs.R`` |  fetch intraday price action for SX7P components |[here](documentation/intraday_bank_pairs.md)|
| ``intraday_fx.R`` |  fetch intraday price action for 30 currencies |[here](documentation/intraday_fx.md)|


The  ``N:\Depts\Share\UK Alpha Team\Analytics\risk_reports`` directory contains 
all knitr Rnw files used to generate reports.


| File | Description | Documentation | Example |
| ``bank_pair_report.Rnw`` ||||
| ``custom_ABC_report.Rnw`` ||||
| ``custom_AC_report.Rnw`` ||||
| ``custom_DH_report.Rnw`` ||||
| ``custom_GJ_report.Rnw`` ||||
| ``custom_JR_report.Rnw`` ||||
| ``custom_MC_report.Rnw`` ||||
| ``fx_trend_report.Rnw`` ||||
| ``market_data_status.Rnw`` ||||
| ``pair_risk_contribution.Rnw`` ||||
| ``portfolio_summary.Rnw`` ||||
| ``risk_plots.Rnw`` ||||
| ``scrape_status.Rnw`` ||||
| ``sheet_scrape_report.Rnw`` ||||
| ``tech_pair_report.Rnw||||
| ``trailing_stop_report.Rnw``||||
| ``what_happened_last_week.Rnw``||||



