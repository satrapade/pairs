#
# sheet scraping:
#
# all sheets are kept in sheet_scrapes.sqlite
#
# this contains the following tables:
#
#  scrape_directories    the directories containing the sheet snapshots
#  sheet_directory       one line per worksheet to screape, with location, date, 
#                        sheet inventory
#  sheet_scrape_location the ranges, sheey names we want to scrape 
#  sheet_scrape          the actual scrape
#
# every day, we compute valid sheet names for all dates starting from 2010-01-01
# we then scrape the sheets that corresponds to dates not found in the sheet_scrape table
#
# sheet contents are read using readxl and compressed using brotli
#
# we then compute portfolio lines from the sheets
#
#
require(feather)
require(data.table)
require(RSQLite)
require(readxl)
require(digest)
require(base64enc)
require(brotli)
require(stringi)
append2log<-function(log_text,append=TRUE)
{
  cat(
    paste0(stri_trim(gsub("##|-","",capture.output(timestamp())))," : ",log_text,"\n"),
    file="N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/workflow.log",
    append=append
  )
}
append2log("perform_sheet_scrape_to_db: source utility_functions")
source("https://raw.githubusercontent.com/satrapade/utility/master/utility_functions.R")

append2log("perform_sheet_scrape_to_db: source /Rscripts/initialize_scrape_db")
source("N:/Depts/Share/UK Alpha Team/Analytics/Rscripts/initialize_scrape_db.R")



verbose<-TRUE

snapshot_directory<-"N:/Depts/Global/Absolute Insight/UK Equity/Daily Snap Shot UK Fund/"

append2log("perform_sheet_scrape_to_db: connect to sheet_scrape/sheet_scrapes.sqlite")
db<-dbConnect(
  SQLite(), 
  dbname="N:/Depts/Share/UK Alpha Team/Analytics/sheet_scrape/sheet_scrapes.sqlite"
)

#
# locate files that need updating:
#
# 1. get all dates in period
# 2. filter out weekends and dates already in the database
# 3. translate remaining dates to files
# 4. if resulting file list is not empty, scrape these files
#

append2log("perform_sheet_scrape_to_db: compute all relevant dates: all_dates")
all_dates<-setdiff(
  Filter(
    function(x){!weekdays(as.Date(x,format="%Y-%m-%d")) %in% c("Saturday","Sunday")},
    make_date_range(
      start="2010-01-04",
      end=as.character(Sys.Date(),format="%Y-%m-%d")
    )
  ),
  query("SELECT date FROM sheet_directory")$date
)

append2log("perform_sheet_scrape_to_db: compute all relevant files: all_valid_files")
all_valid_files<-Filter(
  function(x)nchar(x)>0,
  mapply(make_sheet_name,all_dates)
)

#
# if there are any files to add then we update the "sheet_directory" table
# to include these new files.
#
# "sheet_directory" contains date, year, month, file, type, sheets 
#
#  this table contains the inventory of the sheets we wish to scrape
#
append2log("perform_sheet_scrape_to_db: compute sheets not in database: valid_sheets")
valid_sheets<-sort(unique(query("SELECT sheet FROM sheet_scrape_location")$sheet))
append2log("perform_sheet_scrape_to_db: examine valid sheets, update 'sheet_directory' tabel with sheet details")
if(length(all_valid_files)>0)for(i in names(all_valid_files)){
  if(verbose)cat("Adding file to 'sheet_directory' table:",i,"\n")
  append2log(paste0("perform_sheet_scrape_to_db: adding file ",i))
  data<-data.table(
    date=i,
    year=stri_sub(i,1,4),
    month=stri_sub(i,6,7),
    file=all_valid_files[i],
    type=local({
      x0<-ifelse(nchar(all_valid_files[i])>3,stri_sub(all_valid_files[i],-3,-1),"")
      x1<-ifelse(x0=="lsx","xlsx","")
      x2<-ifelse(x0=="lsm","xlsx","")
      x3<-ifelse(x0=="xls","xls","")
      paste0(x1,x2,x3)
    }),
    sheets=paste0(intersect(valid_sheets,excel_sheets(all_valid_files[i])),collapse="|")
  )
  statement(insert("sheet_directory",data))
}
# make sure no empty files are in the "sheet_directory" table
statement("DELETE FROM sheet_directory WHERE file IS ''")



#
# for any date in the 'sheet_directory' table that is 
# not in the 'sheet_scrape' table, we have to do a scrape
#
# add position for any sheets we dont have already
append2log("perform_sheet_scrape_to_db: scrape sheets in 'sheet_directory' table not in 'sheet_scrape' table")
for(i in setdiff(
  query("SELECT date FROM sheet_directory")$date,
  query("SELECT date FROM sheet_scrape")$date
)){
  if(verbose)cat(i,"\n")
  append2log(paste0("Scraping file to 'sheet_scrape' table:",i))
  scrape_single_sheet(i)
}


# sheets prior to 2012 do not have pair numbers, so cant be used 
append2log("perform_sheet_scrape_to_db: add scraped data to 'portfolio_lines' table")
if(TRUE)if(length(r<-Filter(function(x)stri_sub(x,1,4)>"2011",setdiff(
  query("SELECT date FROM sheet_scrape")$date,
  query("SELECT date FROM portfolio_lines")$date
))))for(i in r){
  if(verbose)cat("Adding portfolio lines:",i)
  append2log(paste0("Adding portfolio lines to 'portfolio_lines' table:",i))
  x<-query(paste0(
    "SELECT fname, sheet, contents, digest ",
    "FROM sheet_scrape ",
    "WHERE date='",i,"' "
  ))
  if(all(c("AIL Closed Trades","AIL Open Trades") %in% x$sheet)){
    y<-make_portfolio(
      compressed_live_range=x$contents[x$sheet=="AIL Open Trades"][1],
      compressed_realized_range=x$contents[x$sheet=="AIL Closed Trades"][1],
      date=i,
      fund="LUKE"
    )
    if(verbose)cat(".")
    statement(insert("portfolio_lines",y))
  }
  if(all(c("DUKE Closed Trades","DUKE Open Trades") %in% x$sheet)){
    y<-make_portfolio(
      compressed_live_range=x$contents[x$sheet=="DUKE Open Trades"][1],
      compressed_realized_range=x$contents[x$sheet=="DUKE Closed Trades"][1],
      date=i,
      fund="DUKE"
    )
    if(verbose)cat(".")
    statement(insert("portfolio_lines",y))
  }
  if(verbose)cat("\n")
}

#
#
#
# accumulate closed positions
append2log("perform_sheet_scrape_to_db: accumulate closed lines")
accumulated_closed_lines<-function(x){
  res<-Reduce(function(a,b){
    removed<-setdiff(a$digest[a$type=="closed"],b$digest[b$type=="closed"])
    if(length(removed)<1)return(b)
    removed_lines<-which(a$digest %in% removed)
    res<-rbind(b,a[removed_lines])
    res$date<-rep(max(b$date[1]),nrow(res))
    res
  },x,accumulate = TRUE)
  structure(res,.Names=names(x))
}

append2log("perform_sheet_scrape_to_db: compute accumulated_duke_lines")
accumulated_duke_lines<-local({
  last_date<-query(paste0(
    "SELECT max(date) AS date ",
    "FROM cummulative_closed_portfolio_lines ",
    "WHERE fund='DUKE'"
  ))$date[1]
  if(!is.null(last_date)){
    last_accumulation<-data.table(query(paste0(
      "SELECT * ",
      "FROM cummulative_closed_portfolio_lines ",
      "WHERE fund='DUKE' ",
      "AND date='",last_date,"'"
    ))) 
  } else { 
    last_accumulation<-data.table(query("SELECT * FROM cummulative_closed_portfolio_lines WHERE fund='none'"))
  }
  a<-rbind(a0<-data.table(query(paste0(
    "SELECT ",
    "* ",
    "FROM portfolio_lines ",
    "WHERE type='closed' ",
    "AND fund='DUKE' ",
    "AND date>'",last_date,"'"
  ))),last_accumulation)
  if(nrow(a)<1)return(last_accumulation[integer(0),])
  x<-split(a,a$date)[sort(unique(a$date))]
  if(length(x)<1)return(last_accumulation[integer(0),])
  y<-accumulated_closed_lines(x)
  if(length(y)<1)return(last_accumulation[integer(0),])
  if(is.null(last_date)){
    z<-do.call(rbind,y)
  } else {
    z<-do.call(rbind,y)[date>last_date,.SD]
  }
  z
})

append2log("perform_sheet_scrape_to_db: append accumulated_duke_lines to 'cummulative_closed_portfolio_lines' table")
if(nrow(accumulated_duke_lines)>0){
  dbWriteTable(conn=db,name="cummulative_closed_portfolio_lines",value=accumulated_duke_lines,append=TRUE)
}

append2log("perform_sheet_scrape_to_db: compute accumulated_luke_lines")
accumulated_luke_lines<-local({
  last_date<-query(paste0(
    "SELECT max(date) AS date ",
    "FROM cummulative_closed_portfolio_lines ",
    "WHERE fund='LUKE'"
  ))$date[1]
  if(!is.null(last_date)){
    last_accumulation<-data.table(query(paste0(
      "SELECT * ",
      "FROM cummulative_closed_portfolio_lines ",
      "WHERE fund='LUKE' ",
      "AND date='",last_date,"'"
    ))) 
  } else { 
    last_accumulation<-data.table(query("SELECT * FROM cummulative_closed_portfolio_lines WHERE fund='none'"))
  }
  a<-rbind(data.table(query(paste0(
    "SELECT ",
    "* ",
    "FROM portfolio_lines ",
    "WHERE type='closed' ",
    "AND fund='LUKE' ",
    "AND date>'",last_date,"'"
  ))),last_accumulation)
  if(nrow(a)<1)return(last_accumulation[integer(0),])
  x<-split(a,a$date)[sort(unique(a$date))]
  y<-accumulated_closed_lines(x)
  if(is.null(last_date)){
    z<-do.call(rbind,y)
  } else {
    z<-do.call(rbind,y)[date>last_date,.SD]
  }
  z
})

append2log("perform_sheet_scrape_to_db: append accumulated_luke_lines to 'cummulative_closed_portfolio_lines' table")
if(nrow(accumulated_luke_lines)>0){
  dbWriteTable(conn=db,name="cummulative_closed_portfolio_lines",value=accumulated_luke_lines,append=TRUE)
}

# a test
append2log("perform_sheet_scrape_to_db: test that scrape lines and portfolio lines are consistent")
p_scrape<-make_portfolio(
  compressed_live_range=data.table(query(paste0(
    "SELECT ",
    "sheet, contents ",
    "FROM sheet_scrape ",
    "WHERE date='2018-01-17' ",
    "AND sheet='AIL Open Trades' "
  )))$contents[1],
  compressed_realized_range=data.table(query(paste0(
    "SELECT ",
    "sheet, contents ",
    "FROM sheet_scrape ",
    "WHERE date='2018-01-17' ",
    "AND sheet='AIL Closed Trades' "
  )))$contents[1],
  date="2018-01-17",
  fund="LUKE"
)

append2log("perform_sheet_scrape_to_db: test")
p_lines<-query(paste0(
  "SELECT type, (CAST(bps AS REAL)) AS bps ",
  "FROM portfolio_lines ",
  "WHERE date='2018-01-17' ",
  "AND fund='LUKE'"
))

stopifnot(
  abs(
    sum(p_scrape$bps[p_scrape$type=="closed"])-
    sum(p_lines$bps[p_lines$type=="closed"])
  )<1e-10
)

append2log("perform_sheet_scrape_to_db: compute all_dates")
all_dates<-sort(unique(query("SELECT date FROM sheet_scrape")$date))

append2log("perform_sheet_scrape_to_db: compute open_duke_pnl")
open_duke_pnl<-data.table(query(paste0(
  "SELECT date, pair, sum(cast(bps as real)) AS bps ",
  "FROM portfolio_lines ",
  "WHERE fund='DUKE' ",
  "AND type='live'",
  "GROUP BY date, pair"
)))

append2log("perform_sheet_scrape_to_db: compute closed_duke_pnl")
closed_duke_pnl<-data.table(query(paste0(
  "SELECT date, pair, sum(cast(bps as real)) AS bps ",
  "FROM cummulative_closed_portfolio_lines ",
  "WHERE fund='DUKE' ",
  "AND type='closed'",
  "GROUP BY date, pair"
)))

append2log("perform_sheet_scrape_to_db: compute pair_pnl")
pair_pnl<-apply(rename_colnames(
  dMcast(rbind(open_duke_pnl,closed_duke_pnl),date~pair,value.var = "bps"),
  "^pair",
  ""
),2,function(x)x-x[1])

append2log("perform_sheet_scrape_to_db: save pair_pnl to sheet_scrape/actual_pair_pnl.csv")
dt<-data.table(date=rownames(pair_pnl),pair_pnl)
fwrite(x=dt,file="N:/Depts/Share/UK Alpha Team/Analytics/sheet_scrape/actual_pair_pnl.csv")

#plot_with_dates(cumprod(diff(tail(rowSums(pair_pnl),100))/10000+1),divisor=0.001)

# matplot(
#   x=matrix(1:nrow(pair_pnl),ncol=1)[,rep(1,ncol(pair_pnl)-1)],
#   y=pair_pnl[,-640],
#   type="l",
#   lty=1,
#   lwd=1,
#   col=rgb(0,0,1,0.33)
# )


dbDisconnect(conn=db)

# DB maintenance 










