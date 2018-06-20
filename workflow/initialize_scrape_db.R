#
# initialize scrape database 
# define access functions
#

source("https://raw.githubusercontent.com/satrapade/utility/master/utility_functions.R")


require(RSQLite)
require(data.table)
require(readxl)
require(digest)
require(base64enc)
require(brotli)
require(stringi)

verbose<-TRUE

snapshot_directory<-"N:/Depts/Global/Absolute Insight/UK Equity/Daily Snap Shot UK Fund/"

db<-dbConnect(
  SQLite(), 
  dbname="N:/Depts/Share/UK Alpha Team/Analytics/sheet_scrape/sheet_scrapes.sqlite"
)

seq_char<-function(from="A",to="Z"){
  LETTERS[seq(from=match(from,LETTERS),to=match(to,LETTERS),by=1)]
}
#


# convert data frame into text fror SQLite "INSERT" statement
insert<-function(tbl,x){
  a<-mapply(function(i,x){
    mapply(function(s)paste0("'",s,"'"),as.list(x[i,]),SIMPLIFY=FALSE)
  },i=1:nrow(x),MoreArgs = list(x=x),SIMPLIFY=FALSE)
  h<-paste("(",paste(names(x),collapse=", "),")")
  v<-mapply(function(...)paste0("(",do.call(paste,list(...,collapse=",")),")"),a)
  paste("INSERT INTO",tbl,"\n",h,"\n","VALUES \n",paste(v,collapse=", \n"),";")
}

# create UPDATE statement for table
update_tbl<-function(tbl,date,x){
  a<-paste0(names(x),"='",x,"'",collapse=", ")
  paste("UPDATE",tbl,"SET",a,"WHERE date IS",paste0("'",date[1],"'"))
}


# create directories table if it does not exist
if(!dbExistsTable(conn=db,"scrape_directories")){
  # subdirectory naming convention is inconsistent, names recoreded here
  dir_schema1<-"Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec"
  dir_schema11<-"Jan|Feb|Mar|Apr|May|June|Jul|Aug|Sep|Oct|Nov|Dec"
  dir_schema2<-"Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|oct|Nov|Dec"
  dir_schema3<-"Jan|Feb|Mar|Apr|May|June|July|Aug|Sep|Oct|Nov|Dec"
  dir_schema4<-"Jan|Feb|Mar|Apr|May|Jun|July|Aug|Sep|Oct|Nov|Dec"
  dir_schema5<-"Jan|Feb|march|Apr|May|June|July|Aug|sep|Oct|Nov|Dec"
  
  # year -> directory map, with month directories
  scrape_directories<-rbind(
    data.table(year="2019",dir="2019",dirs=dir_schema1),
    data.table(year="2018",dir="2018",dirs=dir_schema1),
    data.table(year="2017",dir="2017",dirs=dir_schema1),
    data.table(year="2016",dir="2016",dirs=dir_schema11),
    data.table(year="2015",dir="2015",dirs=dir_schema1),
    data.table(year="2014",dir="2014",dirs=dir_schema2),
    data.table(year="2013",dir="2013",dirs=dir_schema3),
    data.table(year="2012",dir="2012",dirs=dir_schema4),
    data.table(year="2011",dir="2011",dirs=dir_schema3),
    data.table(year="2010",dir="2010",dirs=dir_schema5)
  )[,.SD,keyby=year]
  dbWriteTable(conn=db,name="scrape_directories",value=scrape_directories,overwrite=TRUE)
}


# create sheet scrape location table
if(!dbExistsTable(conn=db,"sheet_scrape_location")){
  dbWriteTable(conn=db,name="sheet_scrape_location",value=data.table(
    sheet=c(
      "AIL Summary","AIL Open Trades","AIL Closed Trades",
      "DUKE Summary","DUKE Open Trades","DUKE Closed Trades"
    ),
    range=c(
      "A4:B11","B10:P2000","A10:O5000",
      "A4:B11","B10:P2000","A10:O5000"
    ),
    colname=c(
      paste(seq_char("A","B"),collapse="|"),
      paste(seq_char("B","P"),collapse="|"),
      paste(seq_char("A","O"),collapse="|"),
      paste(seq_char("A","B"),collapse="|"),
      paste(seq_char("B","P"),collapse="|"),
      paste(seq_char("A","O"),collapse="|")
    )
  ))
}

# create sheet directory table if it does not exist
if(!dbExistsTable(conn=db,"sheet_directory")){
  dbWriteTable(conn=db,name="sheet_directory",value=data.table(
    date=character(0),
    year=character(0),
    month=character(0),
    file=character(0),
    type=character(0),
    sheets=character(0)
  ),overwrite=TRUE)
}


# create sheet scrape table
if(!dbExistsTable(conn=db,"sheet_scrape")){
  dbWriteTable(conn=db,name="sheet_scrape",value=data.table(
    date=character(0),
    fname=character(0),
    sheet=character(0),
    range=character(0),
    contents=character(0),
    timestamp=character(0),
    digest=character(0)
  ),overwrite=TRUE)
}


# create duke_position if it does not exist
if(!dbExistsTable(conn=db,"portfolio_lines")){
  dbWriteTable(conn=db,name="portfolio_lines",value=data.table(
    line=character(0),
    date=character(0),
    type=character(0),
    fund=character(0),
    manager=character(0),
    pair=character(0),
    ticker=character(0),
    direction=character(0),
    quantity=character(0),
    cash=character(0),
    asset_value=character(0),
    pnl=character(0),
    bps=character(0),
    timestamp=character(0),
    digest=character(0)
  ),overwrite=TRUE)
}


if(!dbExistsTable(conn=db,"cummulative_closed_portfolio_lines")){
  dbWriteTable(conn=db,name="cummulative_closed_portfolio_lines",value=data.table(
    line=character(0),
    date=character(0),
    type=character(0),
    fund=character(0),
    manager=character(0),
    pair=character(0),
    ticker=character(0),
    direction=character(0),
    quantity=character(0),
    cash=character(0),
    asset_value=character(0),
    pnl=character(0),
    bps=character(0),
    timestamp=character(0),
    digest=character(0)
  ),overwrite=TRUE)
}

# map date to sheet name
#
# constants neded to compute the mapping
# are stored as formals of the function
#
make_sheet_name<-with_formals(function(date){
  date_components<-strsplit(date,"-")[[1]]
  the_dir<-paste0(snapshot_directory,scrape_directories[date_components[1],dir])
  the_subdir<-strsplit(
    scrape_directories[date_components[1],dirs],"\\|"
  )[[1]][as.integer(date_components[2])]
  location<-paste0(
    the_dir,"/",
    the_subdir,"/",
    "AbsoluteUK xp final ",
    date_components[3],
    "(\\.| )",
    paste0("(",date_components[2],"|",the_subdir,")"),
    "\\.(xls|xlsm)"
  )
  i<-which(grepl(location,all_files))
  if(length(i)<1)return("")
  all_files[head(i,1)]
},list(
  scrape_directories=data.table(
    dbReadTable(conn=db,name="scrape_directories")
  )[,.SD,keyby=year],
  all_files=local({
    scrape_directories<-data.table(
      dbReadTable(conn=db,name="scrape_directories")
    )[,.SD,keyby=year]
    list.files(
      path=paste0(snapshot_directory,scrape_directories$dir),
      recursive=TRUE,
      full.names = TRUE,
      include.dirs = FALSE
    )
  })
))





#
#
#
scrape_single_sheet<-with_formals(function(
  date,
  overwrite=TRUE
){
  info<-query(paste0(
    "SELECT * ",
    "FROM sheet_directory ",
    "WHERE date = '",date,"' ",
    "LIMIT 1"
  ))
  if(nrow(info)<1)return("no date")
  if(info$date[1]!=date)stop("Bad sheet_directory for ",date)
  current<-query(paste0(
    "SELECT * ",
    "FROM sheet_scrape ",
    "WHERE date = '",date,"'"
  ))
  if(nrow(current)>0 & overwrite){
    statement(paste0(
      "DELETE FROM sheet_scrape ",
      "WHERE date = '",date,"'"
    ))
  }
  res<-mapply(
    function(sheet,range,colname,file,type,sheets){
      if(!any(strsplit(sheets,"\\|")[[1]]==sheet))return("no sheet")
      res1<-"error"
      col_names<-strsplit(colname,"\\|")[[1]]
      if(type=="xls")res1<-try(
        read_xls(file,sheet=sheet,range=range,col_names=col_names,col_types="text"),
        silent=TRUE
      )
      if(type=="xlsx")res1<-try(
        read_xlsx(file,sheet=sheet,range=range,col_names=col_names,col_types="text"),
        silent=TRUE
      )
      data<-data.table(
        date=date,
        fname=file,
        sheet=sheet,
        range=range,
        contents=compress(res1),
        timestamp=as.character(Sys.timeDate()),
        digest=digest(res1)
      )
      statement(insert("sheet_scrape",data))
      if(nrow(current)<1)return("scrape saved")
      if(overwrite)return("scrape overwritten")
      return("scrape amended")
    },
    sheet=sheet_scrape_location$sheet,
    range=sheet_scrape_location$range,
    colname=sheet_scrape_location$colname,
    MoreArgs=list(
      file=info$file,
      type=info$type,
      sheets=info$sheets
    ),
    SIMPLIFY=FALSE
  )
  do.call(paste,c(res,sep="|"))
},list(
  sheet_scrape_location=query("SELECT * FROM sheet_scrape_location")
))


#
make_portfolio<-function(
  compressed_live_range,
  compressed_realized_range,
  date,
  fund
){
  if(nchar(compressed_live_range)<1 & nchar(compressed_realized_range)<1)return(NULL)
  live_range<-decompress(compressed_live_range)
  realized_range<-decompress(compressed_realized_range)
  live_line_ndx<-which(grepl("^[A-Z]{2,4}[0-9]{1,3}$",stri_trim(toupper(as.matrix(live_range)[,"B"]))))
  live_lines<-as.matrix(live_range)[live_line_ndx,]
  realized_line_ndx<-which(grepl("^[A-Z]{2,4}[0-9]{1,3}$",stri_trim(toupper(as.matrix(realized_range)[,"A"]))))
  realized_lines<-as.matrix(realized_range)[realized_line_ndx,]
  pair<-toupper(live_lines[,"B"])
  live<-data.table(
    date=rep(date,nrow(live_lines)),
    line=live_line_ndx,
    fund=rep(fund,nrow(live_lines)),
    type=rep("live",nrow(live_lines)),
    ticker=gsub("'","",gsub("INDEX$","Index",gsub("EQUITY$","Equity",toupper(live_lines[,"F"])))),
    manager=gsub("'","",toupper(live_lines[,"C"])),
    pair=gsub("'","",pair[pair!=""][findInterval(1:nrow(live_lines),which(pair!=""),all.inside = TRUE,rightmost.closed = TRUE)]),
    direction=gsub("'","",toupper(live_lines[,"D"])),
    quantity=scrub(as.integer(live_lines[,"H"]))*scrub(as.integer(live_lines[,"I"])),
    cash=(-1)*scrub(as.numeric(live_lines[,"J"])),
    asset_value=scrub(as.numeric(live_lines[,"N"])),
    pnl=scrub(as.numeric(live_lines[,"O"])),
    bps=scrub(as.numeric(live_lines[,"P"]))
  )
  realized<-data.table(
    date=rep(date,nrow(realized_lines)),
    line=realized_line_ndx,
    fund=rep(fund,nrow(realized_lines)),
    type=rep("closed",nrow(realized_lines)),
    ticker=gsub("'","",gsub("INDEX$","Index",gsub("EQUITY$","Equity",toupper(realized_lines[,"E"])))), 
    manager=gsub("'","",stri_trim(toupper(realized_lines[,"B"]))),
    pair=gsub("'","",stri_trim(toupper(realized_lines[,"A"]))),
    direction=gsub("'","",toupper(realized_lines[,"C"])),
    quantity=scrub(as.integer(realized_lines[,"G"]))*scrub(as.integer(realized_lines[,"H"])),
    cash=(-1)*scrub(as.numeric(realized_lines[,"I"])),
    asset_value=scrub(as.numeric(realized_lines[,"M"])),
    pnl=scrub(as.numeric(realized_lines[,"N"])),
    bps=scrub(as.numeric(realized_lines[,"O"]))
  )
  ptf<-rbind(live,realized)[,.SD,keyby=pair]
  x0<-paste0(ptf$pair,ptf$fund,ptf$type,ptf$ticker,ptf$manager,ptf$direction,ptf$quantity,ptf$cash)
  ptf$digest<-sapply(x0,digest)
  ptf
}


remove_scrape<-function(date){
  statement(paste0(
    "DELETE FROM sheet_directory ",
    "WHERE date = '",date,"'"
  ))
  statement(paste0(
    "DELETE FROM sheet_scrape ",
    "WHERE date = '",date,"'"
  ))
  statement(paste0(
    "DELETE FROM portfolio_lines ",
    "WHERE date = '",date,"'"
  ))
}

dbDisconnect(conn=db)



