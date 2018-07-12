require(stringi)

append2log<-function(log_text,append=TRUE)
{
  if(Sys.info()["sysname"]!="Windows")return()
  cat(
    paste0(stri_trim(gsub("##|-","",capture.output(timestamp())))," : ",log_text,"\n"),
    file="N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/workflow.log",
    append=append
  )
}
