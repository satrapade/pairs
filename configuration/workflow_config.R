#
# workflow configuration file
#
#

# data preparation steps
workflow = c(
  "create_database_temp_tables"=list(),
  "create_cix_uploads"=list(),
  "create_portfolio_upload"=list(),
  "create_market_data"=list(),
  "create_portfolio_summary"=list(),
  "create_market_data_intraday"=list(),
  "intraday_fx"=list(),
  "intraday_index_members"=list(),
  "intraday_bank_pairs"=list(),
  "create_tsne_grid"=list(),
  "perform_sheet_scrape_to_db"=list(),
  "create_pair_icons"=list()
)

# risk reports prepared using knitr
risk_report=list(
  "scrape_status"=list(),
  "market_data_status"=list(),
  "portfolio_summary"=list(),
  "risk_plots"=list(),
  "pair_risk_contribution"=list(),
  "what_happened_last_week"=list(),
  "sheet_scrape_report"=list(),
  "custom_AC_report"=list(),
  "custom_JR_report"=list(),
  "custom_DH_report"=list(),
  "custom_GJ_report"=list(),
  "custom_ABC_report"=list(),
  "custom_MC_report"=list(),
  "fx_trend_report"=list(push_to_directory ="N:/Depts/FI Currency/Quant/"),
  "bank_pair_report"=list(),
  "trailing_stop_report"=list(),
  "duke_luke_drawdown"=list()
)

database_product_id="7"
database_source_id="1"
database_start_date="'2015-05-28'"
database_end_date="'2018-05-01'"
database_root_bucket_name="'EqyBucket'"
database_results_directory="N:/Depts/Share/UK Alpha Team/Analytics/db_cache/"


