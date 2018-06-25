

fetch_risk_report<-function(
  workflow_step="create_cix_uploads",
  target_directory="N:/Depts/Share/UK Alpha Team/Analytics/Rscripts"
){
  
  report_url<-paste0(
    "https://raw.githubusercontent.com/satrapade/pairs/master/Rscripts/",
    report_name,
    ".R"
  )
  
  report_fname<-paste0(
    target_directory,
    "/",
    report_name,
    ".R"
  )
  
  download.file(report_url,report_fname) 
  
}



