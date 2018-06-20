#
# daily workflow
#
require(stringi)
append2log<-function(log_text,append=TRUE)
{
  cat(
    paste0(stri_trim(gsub("##|-","",capture.output(timestamp())))," : ",log_text,"\n"),
    file="N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/workflow.log",
    append=append
  )
}

append2log("workflow: start",append=FALSE)

#
append2log("workflow: create_database_temp_tables.R")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/create_database_temp_tables.R\"")

#
append2log("workflow: create_cix_uploads")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/create_cix_uploads.R\"")

#
append2log("workflow: create_portfolio_upload")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/create_portfolio_upload.R\"")

#
append2log("workflow: create_market_data")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/create_market_data.R\"")


#
append2log("workflow: create_portfolio_summary")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/create_portfolio_summary.R\"")

#
append2log("workflow: create_market_data_intraday")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/create_market_data_intraday.R\"")

#
append2log("workflow: intraday_fx")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/intraday_fx.R\"")

#
append2log("workflow: intraday_index_members")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/intraday_bank_pairs.R\"")


#
append2log("workflow: create_tsne_grid")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/create_tsne_grid.R\"")

#
append2log("workflow: perform_sheet_scrape_to_db")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/perform_sheet_scrape_to_db.R\"")


#
append2log("workflow: create_pair_icons")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/create_pair_icons.R\"")


#
append2log("workflow: create_risk_reports")
system("Rscript \"N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/create_risk_reports.R\"")





