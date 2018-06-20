require(knitr)
require(stringi)

source("https://raw.githubusercontent.com/satrapade/utility/master/utility_functions.R")

append2log<-function(log_text)
{
  cat(
    paste0(stri_trim(gsub("##|-","",capture.output(timestamp())))," : ",log_text,"\n"),
    file="N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/workflow.log",
    append=TRUE
  )
}
#
create_report<-function(
  report_name,
  push_to_directory=NULL
)
{
  wd<-getwd()
  setwd("N:/Depts/Share/UK Alpha Team/Analytics/risk_reports")
  if(!file.exists(paste0(report_name,".Rnw"))){
    append2log(paste0("!!!>ERROR<!!! report does not exist:",report_name))
    setwd(wd)
    return(paste0(report_name,": error: report does not exist"))
  }
  append2log(paste0(report_name,": creating report"))
  
  # remove figures 
  append2log(paste0(report_name,": deleting old figures, old .tex and pdf files"))
  temp_files<-list.files(
    path="N:/Depts/Share/UK Alpha Team/Analytics/risk_reports/figure",
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
    input=paste0("N:/Depts/Share/UK Alpha Team/Analytics/risk_reports/",report_name,".Rnw"),
    output=paste0("N:/Depts/Share/UK Alpha Team/Analytics/risk_reports/",report_name,".tex"),
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
    "pdflatex  -jobname=\"",outfn,"\" \"N:/Depts/Share/UK Alpha Team/Analytics/risk_reports/",report_name,".tex\""
  )
  system(pdf_latex_cmd)
  
  # delete temporary latex files
  append2log(paste0(report_name,": remove temporary latex files"))  
  temp_files<-list.files(
    path="N:/Depts/Share/UK Alpha Team/Analytics/risk_reports",
    pattern="(dpth$)|(log$)|(md5$)|(aux$)|(gz$)",
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
  file.remove(paste0(report_name,".tex"))
  file.remove(paste0(report_name,"-concordance.tex"))
  setwd(wd)
  return(paste0(report_name,": success"))
}



setwd("N:/Depts/Share/UK Alpha Team/Analytics/risk_reports")

# create pdf's with timestamp and root name only
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






