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
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/move_risk_reports_to_month_folder.R")

config<-new.env()
source(
  file="https://raw.githubusercontent.com/satrapade/pairs/master/configuration/workflow_config.R",
  local=config
)

setwd(config$home_directory)

# workflow steps
append2log("workflow: start",append=FALSE)
mapply(function(w){
  append2log(paste0("workflow: sourcing ",w),append=TRUE)
  try(source_workflow_step(w),silent=TRUE)
  append2log(paste0("workflow: finished ",w),append=TRUE)
},names(config$workflow))


# risk report steps
append2log("workflow: create_risk_reports")
# setwd("N:/Depts/Share/UK Alpha Team/Analytics/risk_reports")
setwd(config$risk_report_directory)
move_risk_reports_to_month_folder(config)

create_report(report_name="scrape_status")
create_report(report_name="market_data_status")
create_report(report_name="portfolio_summary")
create_report(report_name="risk_plots")
create_report(report_name="pair_risk_contribution")
create_report(report_name="pair_risk_contribution_new")
create_report(report_name="what_happened_last_week")
create_report(report_name="sheet_scrape_report")
create_report(report_name="custom_AC_report")
create_report(report_name="sizing_report",output_suffix="_AC",envir=list2env(list(the_manager="AC")))
create_report(report_name="custom_JR_report")
create_report(report_name="sizing_report",output_suffix="_JR",envir=list2env(list(the_manager="JR")))
create_report(report_name="custom_DH_report")
create_report(report_name="sizing_report",output_suffix="_DH",envir=list2env(list(the_manager="DH")))
create_report(report_name="custom_GJ_report")
create_report(report_name="sizing_report",output_suffix="_GJ",envir=list2env(list(the_manager="GJ")))
create_report(report_name="custom_ABC_report")
create_report(report_name="sizing_report",output_suffix="_ABC",envir=list2env(list(the_manager="ABC")))
create_report(report_name="custom_MC_report")
create_report(report_name="sizing_report",output_suffix="_MC",envir=list2env(list(the_manager="MC")))
create_report(report_name="sizing_report",output_suffix="_ACTW",envir=list2env(list(the_manager="ACTW")))
create_report("fx_trend_report",push_to_directory ="N:/Depts/FI Currency/Quant/" )
create_report(report_name="bank_pair_report")
create_report(report_name="trailing_stop_report")
create_report(report_name="duke_luke_drawdown")
create_report(report_name="duke_pair_performance")

