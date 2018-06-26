#
create_report<-function(
  report_name,
  push_to_directory=NULL,
  config=get("config",parent.frame())
)
{
  wd<-getwd()
  setwd(config$risk_report_directory)
  res<-try(fetch_risk_report(report_name),silent=TRUE)
  if(any(class(res) %in% "try-error")){
    append2log(paste0("!!!>ERROR<!!! fetch:",report_name))
    setwd(wd)
    return(paste0(report_name,": error: cant fetch report"))
  }
  if(!file.exists(paste0(report_name,".Rnw"))){
    append2log(paste0("!!!>ERROR<!!! report does not exist:",report_name))
    setwd(wd)
    return(paste0(report_name,": error: report does not exist"))
  }
  append2log(paste0(report_name,": creating report"))
  
  # remove figures 
  append2log(paste0(report_name,": deleting old figures, old .tex and pdf files"))
  temp_files<-list.files(
    path=paste0(config$risk_report_directory,"/figure"),
    pattern="pdf$",
    recursive = FALSE,
    full.names = TRUE
  )
  file.remove(temp_files)
  file.remove(paste0(report_name,".tex"))
  file.remove(paste0(report_name,".pdf"))
  
  # knit .Rnw file
  append2log(paste0(report_name,": knitting Rnw:"))
  res<-try(knit(
    input=paste0(config$risk_report_directory,"/",report_name,".Rnw"),
    output=paste0(config$risk_report_directory,"/",report_name,".tex"),
    envir=.GlobalEnv
  ),silent = TRUE)
  if(class(res) %in% "try-error"){
    append2log(paste0(report_name,": !!!>ERROR<!!! :",gsub("\n","",as.character(res))))
    setwd(wd)
    return(paste0(report_name,": error: tex file not generated"))
  }
  if(!file.exists(paste0(report_name,".tex"))){
    append2log(paste0(report_name,": !!!>ERROR<!!! :.tex file not generated"))
    setwd(wd)
    return(paste0(report_name,": error: tex file not generated"))
  }
  # pdf latex, append date-time
  append2log(paste0(report_name,": pdflatex on"))  
  outfn<-paste0(report_name,"_",gsub("\\s","x",gsub(":","",as.character(Sys.timeDate()))))
  pdf_latex_cmd<-paste0(
    "pdflatex  -jobname=\"",
    outfn,
    "\" \"",
    config$risk_report_directory,
    "/",
    report_name,
    ".tex\""
  )
  system(pdf_latex_cmd)
  
  # delete temporary latex files
  append2log(paste0(report_name,": remove temporary latex files"))  
  temp_files<-list.files(
    path="N:/Depts/Share/UK Alpha Team/Analytics/risk_reports",
    pattern="(dpth$)|(log$)|(md5$)|(aux$)|(gz$)|(toc$)|(out$)",
    recursive = FALSE,
    full.names = TRUE
  )
  file.remove(temp_files)
  
  # make on-the run copy
  append2log(paste0(report_name,": copying to latest"))  
  file.copy(paste0(outfn,".pdf"),paste0(report_name,".pdf"))
  if(!file.exists(paste0(report_name,".pdf"))){
    append2log(paste0(report_name,": !!!>ERROR<!!! :.pdf file not generated:"))
    setwd(wd)
    return(paste0(report_name," error: pdf file not generated"))
  }
  if(!is.null(push_to_directory)){
    file.copy(paste0(outfn,".pdf"),paste0(push_to_directory,outfn,".pdf"))
  }
  if(file.exists(paste0(report_name,".tex")))file.remove(paste0(report_name,".tex"))
  if(file.exists(paste0(report_name,".toc")))file.remove(paste0(report_name,".toc"))
  if(file.exists(paste0(report_name,".out")))file.remove(paste0(report_name,".out"))
  if(file.exists(paste0(report_name,"-concordance.tex")))file.remove(paste0(report_name,"-concordance.tex"))
  if(file.exists(paste0(outfn,".tex")))file.remove(paste0(outfn,".tex"))
  if(file.exists(paste0(outfn,".toc")))file.remove(paste0(outfn,".toc"))
  if(file.exists(paste0(outfn,".out")))file.remove(paste0(outfn,".out"))
  if(file.exists(paste0(outfn,"-concordance.tex")))file.remove(paste0(outfn,"-concordance.tex"))
  
  setwd(wd)
  return(paste0(report_name,": success"))
}
