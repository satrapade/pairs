#
# risk reports with a time-date component in the file name accumulate over time
# to avoid filling up the risk reports directory, we copy these to 
# folders named after months
#

move_risk_reports_to_month_folder<-function(config){
  month_folder<-fread("
                      month,  folder     
                      01,     january
                      02,     february
                      03,     march
                      04,     april
                      05,     may
                      06,     june
                      07,     july
                      08,     august
                      09,     september
                      10,     october
                      11,     november
                      12,     december
                      ",
colClasses=c(month="character",folder="character")
)[,.SD,keyby=month]
  
  for(i in month_folder$month){
    file_pattern<-paste0("^[A-Za-z_]{1,30}_20[0-9]{2}-",i,"-*")
    the_files<-list.files(config$risk_report_directory,file_pattern)
    the_directory<-paste0(config$risk_report_directory,"/",month_folder[i,folder])
    if(length(the_files)>0)for(f in the_files){
      file.copy(
        paste0(config$risk_report_directory,"/",f),
        paste0(the_directory,"/",f)
      )
      file.remove(paste0(config$risk_report_directory,"/",f))
    }
  }  
}
