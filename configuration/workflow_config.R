#
# workflow configuration file
#
#

script_directory="N:/Depts/Share/UK Alpha Team/Analytics/Rscripts"
risk_report_directory="N:/Depts/Share/UK Alpha Team/Analytics/risk_reports"

# data preparation steps
workflow = list(
  "create_database_temp_tables"=list(
    database_product_id="7",
    database_source_id="1",
    database_start_date="'2015-05-28'",
    database_end_date="'2018-05-01'",
    database_root_bucket_name="'EqyBucket'",
    database_results_directory="N:/Depts/Share/UK Alpha Team/Analytics/db_cache/"
  ),
  "create_cix_uploads"=list(
    cix_results_directory="N:/Depts/Share/UK Alpha Team/Analytics/CIX",
    luke_results_directory="N:/Depts/Share/UK Alpha Team/Analytics/LUKE",
    duke_results_directory="N:/Depts/Share/UK Alpha Team/Analytics/DUKE",
    sheet_fn="N:/Depts/Global/Absolute Insight/UK Equity/AbsoluteUK xp final.xlsm"
  ),
  "create_portfolio_upload"=list(),
  "create_market_data"=list(),
  "create_portfolio_summary"=list(),
  "create_market_data_intraday"=list(),
  "intraday_fx"=list(),
  "intraday_index_members"=list(
    indices=c("SX8P Index","SX5E Index","SX7E Index","SX7P Index","SXIP Index","SXIE Index"),
    index_membership_fn="N:/Depts/Share/UK Alpha Team/Analytics/market_data/index_membership.csv",
    intraday_fn="N:/Depts/Share/UK Alpha Team/Analytics/market_data/intraday_index_members.csv",
    intraday_perf_fn="N:/Depts/Share/UK Alpha Team/Analytics/market_data/intraday_perf_index_members.csv",
    intraday_open_fn="N:/Depts/Share/UK Alpha Team/Analytics/market_data/intraday_open_index_members.csv",
    intraday_close_fn="N:/Depts/Share/UK Alpha Team/Analytics/market_data/intraday_close_index_members.csv",
    bar_intervals_fn="N:/Depts/Share/UK Alpha Team/Analytics/market_data/bar_intervals_index_members.csv"
  ),
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
  "fx_trend_report"=list(
    push_to_directory ="N:/Depts/FI Currency/Quant/"
  ),
  "bank_pair_report"=list(),
  "tech_pair_report"=list(),
  "trailing_stop_report"=list(),
  "duke_luke_drawdown"=list()
)

# create_database_temp_tables constants
database_product_id="7"
database_source_id="1"
database_start_date="'2015-05-28'"
database_end_date="'2018-05-01'"
database_root_bucket_name="'EqyBucket'"
database_results_directory="N:/Depts/Share/UK Alpha Team/Analytics/db_cache/"


