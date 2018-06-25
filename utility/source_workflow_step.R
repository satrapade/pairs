
#
#
#

source_workflow_step<-function(
  workflow_step="create_cix_uploads"
){
  
  workflow_url<-paste0(
    "https://raw.githubusercontent.com/satrapade/pairs/master/workflow/",
    workflow_step,
    ".R"
  )
  
 append2log(paste0(workflow_step,": starting"))
 source(workflow_url)
  
}

