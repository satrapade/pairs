
fetch_risk_report<-function(
  report_name="what_happened_last_week",
  target_directory=config$risk_report_directory
){

  report_url<-paste0(
    "https://raw.githubusercontent.com/satrapade/pairs/master/risk_reports/",
    report_name,
    ".Rnw"
  )
  
  report_fname<-paste0(
    target_directory,
    "/",
    report_name,
    ".Rnw"
  )
  
  download.file(report_url,report_fname) 
  
}


