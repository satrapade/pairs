# pair_reporting
data, workflows for pair performance reporting

Scripts and results are in th ``N:\Depts\Share\UK Alpha Team\Analytics`` directory.

----

A task is setup on the Windows Task Manager to run every day at 6:30 in the morning:

![image](https://user-images.githubusercontent.com/1358190/41651469-8a890876-7478-11e8-9341-8c5563304c76.png)

The task is setup to run Rscript.exe:

``"C:\Program Files\R\R-3.4.3\bin\Rscript.exe"``

with this parameter:

``-e source('N:/Depts/Share/UK\x20Alpha\x20Team/Analytics/Rscripts/workflow.R')``

``workflow.R`` then executes individual workflow steps.

----

The  ``N:\Depts\Share\UK Alpha Team\Analytics\Rscripts`` directory contains 
all workflow scripts:


| File | Description | Documentation |
|----------|----------|----------|
| ``workflow.R`` | the start script  |[here](documentation/workflow.md)|
| ``workflow.log`` | results log |[here](documentation/workflow.md)|
| ``relevant_equity_ticker_table.sql`` | construct date, ticker, bucket tables |[here](documentation/relevant_equity_ticker_table.md)|
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


----

The  ``N:\Depts\Share\UK Alpha Team\Analytics\risk_reports`` directory contains 
all knitr Rnw files used to generate reports:

| File | Description | Documentation | Example |
|----------|----------|----------|----------|
| ``bank_pair_report.Rnw`` | search for trending bank pairs | here | here |
| ``custom_ABC_report.Rnw`` | analysis of ABC's pairs | here | here |
| ``custom_AC_report.Rnw`` | analysis of ABC's pairs | here | here |
| ``custom_DH_report.Rnw`` | analysis of ABC's pairs | here | here |
| ``custom_GJ_report.Rnw`` | analysis of ABC's pairs | here | here |
| ``custom_JR_report.Rnw`` | analysis of ABC's pairs | here | here |
| ``custom_MC_report.Rnw`` | analysis of ABC's pairs | here | here |
| ``fx_trend_report.Rnw`` | search for trending FX pairs| here | here |
| ``market_data_status.Rnw`` | market data fetch | here | here |
| ``pair_risk_contribution.Rnw`` | pair risk contribution | here | here |
| ``portfolio_summary.Rnw`` | portfolio summary statistics | here | here |
| ``risk_plots.Rnw`` | risk plot | here | here |
| ``scrape_status.Rnw`` | sheet scrape status | here | here |
| ``sheet_scrape_report.Rnw`` | sheet scrape report | here | here |
| ``tech_pair_report.Rnw``| search for trending technology pairs | here | here |
| ``trailing_stop_report.Rnw``| trailing stop-loss report | here | here |
| ``what_happened_last_week.Rnw``| what happened last week report | here | here |


----

The production database contains product and bucket history which is used to
create the above reports. some amount of pre-processing is done
on the SQL server, which results in the following intermediate tables.
The SQL scripts used to create these tables are in the ``sql`` folder.


| Table                        | Description                         | Script         |
|------------------------------|-------------------------------------|----------------|
|ttBUCKET_EXPOSURES            | exposures by bucket                 | [create_ttBUCKET_EXPOSURES.sql](sql/create_ttBUCKET_EXPOSURES.sql) |
|ttBUCKET_PNL                  | pnl by bucket                       | [create_ttDATES.sql](sql/create_ttDATES.sql) |
|ttBUCKETS                     | relevant buckets                    | [create_ttBUCKET_PNL.sql](sql/create_ttBUCKET_PNL.sql) |
|ttBUID_MARKET_STATUS          | market status of relevant BUIDs     | [create_ttBUCKETS.sql](sql/create_ttBUCKETS.sql) |
|ttCURRENT_ISIN_MARKET_STATUS  | market status of current ISINs      | [create_ttBUID_MARKET_STATUS.sql](sql/create_ttBUID_MARKET_STATUS.sql) |
|ttDATES                       | relevant dates (including weekends) | [create_ttDATES.sql](sql/create_ttDATES.sql) |
|ttEXPOSURE_SECURITIES         | relevant instrument table           | [create_ttEXPOSURE_SECURITIES.sql](sql/create_ttEXPOSURE_SECURITIES.sql) |
|ttHISTORICAL_BUCKET_EXPOSURES | exposures over time                 | [create_ttHISTORICAL_BUCKET_EXPOSURES.sql](sql/create_ttHISTORICAL_BUCKET_EXPOSURES.sql) |
|ttHISTORICAL_BUCKET_HOLDINGS  | holdings                            | [create_ttHISTORICAL_BUCKET_HOLDINGS.sql](sql/create_ttHISTORICAL_BUCKET_HOLDINGS.sql) |
|ttHISTORICAL_BUCKETS          | all buckets                         | [create_ttHISTORICAL_BUCKETS.sql](sql/create_ttHISTORICAL_BUCKETS.sql) |
|ttHISTORICAL_EQUITY_ISIN      | map historical to current ISINs     | [create_ttHISTORICAL_EQUITY_ISIN.sql](sql/create_ttHISTORICAL_EQUITY_ISIN.sql) |
|ttISIN_MARKET_STATUS          | old ISIN status                     | [create_ttISIN_MARKET_STATUS.sql](sql/create_ttISIN_MARKET_STATUS.sql) |
|ttTICKER_MARKET_STATUS        | ticker status                       | [create_ttTICKER_MARKET_STATUS.sql](sql/create_ttTICKER_MARKET_STATUS.sql) |






