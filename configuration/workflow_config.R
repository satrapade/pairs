#
# workflow configuration file
#
#

workflow = c(
  "create_database_temp_tables",
  "create_cix_uploads",
  "create_portfolio_upload",
  "create_market_data",
  "create_portfolio_summary",
  "create_market_data_intraday",
  "intraday_fx",
  "intraday_index_members",
  "intraday_bank_pairs",
  "create_tsne_grid",
  "perform_sheet_scrape_to_db",
  "create_pair_icons"
)


database_product_id="7"
database_source_id="1"
database_start_date="'2015-05-28'"
database_end_date="'2018-05-01'"
database_root_bucket_name="'EqyBucket'"
database_results_directory="N:/Depts/Share/UK Alpha Team/Analytics/db_cache/"


