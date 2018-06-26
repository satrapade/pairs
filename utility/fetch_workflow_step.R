#
# get workflow step from repo to local directory for execution
#

fetch_workflow_step<-function(
  workflow_step,
  target_directory="N:/Depts/Share/UK Alpha Team/Analytics/Rscripts"
){
  
  workflow_url<-paste0(
    "https://raw.githubusercontent.com/satrapade/pairs/master/workflow/",
    workflow_step,
    ".R"
  )
  
  workflow_fname<-paste0(
    target_directory,
    "/",
    workflow_step,
    ".R"
  )
  
  download.file(workflow_url,workflow_fname) 
  
}



