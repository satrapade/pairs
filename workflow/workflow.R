#
# daily workflow
#
require(stringi)
require(knitr)

source("https://raw.githubusercontent.com/satrapade/utility/master/utility_functions.R")
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/create_report.R")
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/fetch_risk_report.R")
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/append2log.R")

append2log("workflow: start",append=FALSE)

#
append2log("workflow: create_database_temp_tables.R")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/create_database_temp_tables.R\"")

#
append2log("workflow: create_cix_uploads")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/create_cix_uploads.R\"")

#
append2log("workflow: create_portfolio_upload")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/create_portfolio_upload.R\"")

#
append2log("workflow: create_market_data")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/create_market_data.R\"")


#
append2log("workflow: create_portfolio_summary")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/create_portfolio_summary.R\"")

#
append2log("workflow: create_market_data_intraday")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/create_market_data_intraday.R\"")

#
append2log("workflow: intraday_fx")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/intraday_fx.R\"")

#
append2log("workflow: intraday_index_members")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/intraday_bank_pairs.R\"")


#
append2log("workflow: create_tsne_grid")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/create_tsne_grid.R\"")

#
append2log("workflow: perform_sheet_scrape_to_db")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/perform_sheet_scrape_to_db.R\"")


#
append2log("workflow: create_pair_icons")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/create_pair_icons.R\"")


#
# data has been produced, now make .PDF reports
#

#system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/create_risk_reports.R\"")


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




