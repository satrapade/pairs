#
# getch workflow step from repo to local dire for execution
#

fetch_workflow_step<-function(
  workflow_step="create_cix_uploads",
  target_directory="N:/Depts/Share/UK Alpha Team/Analytics/Rscripts"
){
  
  report_url<-paste0(
    "https://raw.githubusercontent.com/satrapade/pairs/master/Rscripts/",
    workflow_step,
    ".R"
  )
  
  report_fname<-paste0(
    target_directory,
    "/",
    workflow_step,
    ".R"
  )
  
  download.file(report_url,report_fname) 
  
}



