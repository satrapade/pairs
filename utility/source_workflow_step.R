
#
#
#

source_workflow_step<-function(
  workflow_step="create_cix_uploads"
){
  
  workflow_url<-paste0(
    "https://raw.githubusercontent.com/satrapade/pairs/master/Rscripts/",
    workflow_step,
    ".R"
  )
  
 source(workflow_url)
  
}

