library(knitr)
require(stringi)
append2log<-function(log_text,append=TRUE)
{
  cat(
    paste0(stri_trim(gsub("##|-","",capture.output(timestamp())))," : ",log_text,"\n"),
    file="N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/workflow.log",
    append=append
  )
}
append2log("create_pair_icons: remove existing icons from Analytics/pair_icons")
plot_files<-list.files(
  path="N:/Depts/Share/UK Alpha Team/Analytics/pair_icons",
  pattern="(^[A-Za-z0-9]+\\.dpth$)|(^[A-Za-z0-9]+\\.log$)|(^[A-Za-z0-9]+\\.md5$)|(^[A-Za-z0-9]+\\.pdf$)",
  recursive = FALSE,
  full.names = TRUE
)
file.remove(plot_files)

append2log("create_pair_icons: knit create_pair_icons")
knit(
  input="N:/Depts/Share/UK Alpha Team/Analytics/pair_icons/create_pair_icons.Rnw",
  output="N:/Depts/Share/UK Alpha Team/Analytics/pair_icons/create_pair_icons.tex"
)


setwd("N:/Depts/Share/UK Alpha Team/Analytics/pair_icons")

append2log("create_pair_icons: pdflatex create_pair_icons")
system("pdflatex -shell-escape \"N:/Depts/Share/UK Alpha Team/Analytics/pair_icons/create_pair_icons.tex\"")

append2log("create_pair_icons: delete temp files")
temp_files<-list.files(
  path="N:/Depts/Share/UK Alpha Team/Analytics/pair_icons",
  pattern="(^[A-Za-z0-9]+\\.dpth$)|(^[A-Za-z0-9]+\\.log$)|(^[A-Za-z0-9]+\\.md5$)",
  recursive = FALSE,
  full.names = TRUE
)
file.remove(temp_files)

append2log("create_pair_icons: create image directory")
images<-data.table(
  file=list.files(
    path="N:/Depts/Share/UK Alpha Team/Analytics/pair_icons",
    pattern="^[A-Za-z0-9]+\\.pdf$",
    recursive = FALSE,
    full.names = TRUE
  ),
  name=gsub("\\.pdf$","",list.files(
    path="N:/Depts/Share/UK Alpha Team/Analytics/pair_icons",
    pattern="^[A-Za-z0-9]+\\.pdf$",
    recursive = FALSE,
    full.names = FALSE
  ))
)[!grepl("^ICON",name)]

keys<-data.table(
  file=list.files(
    path="N:/Depts/Share/UK Alpha Team/Analytics/pair_icons",
    pattern="^[A-Za-z0-9]+\\.pdf$",
    recursive = FALSE,
    full.names = TRUE
  ),
  name=gsub("\\.pdf$","",list.files(
    path="N:/Depts/Share/UK Alpha Team/Analytics/pair_icons",
    pattern="^[A-Za-z0-9]+\\.pdf$",
    recursive = FALSE,
    full.names = FALSE
  ))
)[grepl("^ICON",name)]

append2log("create_pair_icons: save image directory to pair_icons/images.csv")
fwrite(images,"N:/Depts/Share/UK Alpha Team/Analytics/pair_icons/images.csv")
append2log("create_pair_icons: save key directory to pair_icons/keys.csv")
fwrite(keys,"N:/Depts/Share/UK Alpha Team/Analytics/pair_icons/keys.csv")




