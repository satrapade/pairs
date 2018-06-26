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

config<-new.env()
source(
  file="https://raw.githubusercontent.com/satrapade/pairs/master/configuration/workflow_config.R",
  local=config
)

setwd(config$home_directory)

append2log("workflow: start",append=FALSE)

# workflow steps
mapply(function(w)try(source_workflow_step(w),silent=TRUE),names(config$workflow))


# risk report steps
append2log("workflow: create_risk_reports")
# setwd("N:/Depts/Share/UK Alpha Team/Analytics/risk_reports")
setwd(config$risk_report_directory)
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




