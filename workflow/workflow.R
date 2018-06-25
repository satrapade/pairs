#
# daily workflow
#
require(stringi)
require(knitr)

source("https://raw.githubusercontent.com/satrapade/utility/master/utility_functions.R")
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/append2log.R")
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/create_report.R")
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/fetch_risk_report.R")
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/source_workflow_step.R")

append2log("workflow: start",append=FALSE)

#
source_workflow_step("create_database_temp_tables")
source_workflow_step("create_cix_uploads")
source_workflow_step("create_portfolio_upload")
source_workflow_step("create_market_data")
source_workflow_step("create_portfolio_summary")
source_workflow_step("create_market_data_intraday")
source_workflow_step("intraday_fx")
source_workflow_step("intraday_index_members")
source_workflow_step("intraday_bank_pairs")
source_workflow_step("create_tsne_grid")
source_workflow_step("perform_sheet_scrape_to_db")
source_workflow_step("create_pair_icons")
source_workflow_step("create_pair_icons")

#
append2log("workflow: create_risk_reports")
setwd("N:/Depts/Share/UK Alpha Team/Analytics/risk_reports")
create_report("scrape_status")
create_report("market_data_status")
create_report("portfolio_summary")
create_report("risk_plots")
create_report("pair_risk_contribution")
create_report("what_happened_last_week")
create_report("sheet_scrape_report")
create_report("custom_AC_report")
create_report("custom_JR_report")
create_report("custom_DH_report")
create_report("custom_GJ_report")
create_report("custom_ABC_report")
create_report("custom_MC_report")
create_report("fx_trend_report",push_to_directory ="N:/Depts/FI Currency/Quant/" )
create_report("bank_pair_report")
create_report("trailing_stop_report")
create_report("duke_luke_drawdown")




