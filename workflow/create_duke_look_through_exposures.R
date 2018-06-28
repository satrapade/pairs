
require(RSQLite)
require(DBI)
require(Matrix)
require(Matrix.utils)
require(Rblpapi)
require(R.cache)
require(RcppRoll)
require(data.table)
require(Hmisc)
require(clue)
require(gsubfn)
require(magrittr)

source("https://raw.githubusercontent.com/satrapade/utility/master/scrub.R")
source("https://raw.githubusercontent.com/satrapade/utility/master/make_query.R")
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/make_date_range.R")
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/fetch_index_weights.R")


if(exists("db"))dbDisconnect(db)
db<-dbConnect(
  odbc::odbc(), 
  .connection_string = paste0(
    "driver={SQL Server};",
    "server=SQLS071FP\\QST;",
    "database=PRDQSTFundPerformance;",
    "trusted_connection=true"
  )
)

rconn<-Rblpapi::blpConnect()

fundamentals<-fread(
  "N:/Depts/Share/UK Alpha Team/Analytics/market_data/fundamentals.csv"
)

super_sector_index_map<-fread(
'
  NAME,                           INDEX
  "Automobiles & Parts",          "SXA"
  "Banks",                        "SX7"
  "Basic Resources",              "SXP"
  "Chemicals",                    "SX4"
  "Construction & Materials",     "SXO"
  "Financial Services",           "SXF"
  "Food & Beverage",              "SX3"
  "Health Care",                  "SXD"
  "Industrial Goods & Services",  "SXN"
  "Insurance",                    "SXI"
  "Media",                        "SXM"
  "Oil & Gas",                    "SXE"
  "Personal & Household Goods",   "SXQ"
  "Real Estate",                  "SX86"
  "Retail",                       "SXR"
  "Technology",                   "SX8"
  "Telecommunications",           "SXK"
  "Travel & Leisure",             "SXT"
  "Utilities",                    "SX6"         
'
)[,
  c(
    "P_Index",
    "E_Index"
  ):=list(
    P_index=local({
      Rblpapi::bdp(unique(paste0(INDEX,"P Index")),"DAY_TO_DAY_TOT_RETURN_GROSS_DVDS")[
        paste0(INDEX,"P Index")  , "DAY_TO_DAY_TOT_RETURN_GROSS_DVDS"
      ]
    }),
    E_index=local({
      Rblpapi::bdp(unique(paste0(INDEX,"E Index")),"DAY_TO_DAY_TOT_RETURN_GROSS_DVDS")[
        paste0(INDEX,"E Index")  , "DAY_TO_DAY_TOT_RETURN_GROSS_DVDS"
        ]
    })
  )
]


#
# all of duke look-through
#

duke_exposure<-fread(
  "N:/Depts/Share/UK Alpha Team/Analytics/duke_summary/duke_exposure.csv",
  col.names = c("Ticker","Exposure")
)


duke_index_exposure<-duke_exposure[ticker_class(Ticker)=="index|nomatch"]
duke_equity_exposure<-duke_exposure[ticker_class(Ticker)=="equity|nomatch"]

duke_index_look_through<-do.call(rbind,mapply(function(ndx,exposure){
  res<-fetch_index_weights(ndx)[,"Exposure":=exposure*Weight/sum(Weight)] 
  bbg_ticker<-Rblpapi::bdp(paste0("/buid/",res$UniqueId),"PARSEKYABLE_DES")
  res$Ticker<-bbg_ticker[paste0("/buid/",res$UniqueId),"PARSEKYABLE_DES"]
  res
},duke_index_exposure$Ticker,duke_index_exposure$Exposure,SIMPLIFY = FALSE))

duke_look_through_exposure<-rbind(
  duke_equity_exposure[,c("Source","IndexTicker"):=list("Outright","None")],
  duke_index_look_through[,"Source":="LookThrough"][,.(Ticker,Exposure,Source,IndexTicker)]
)[,.(
  Ticker=Ticker,
  Exposure=round(10000*Exposure,digits=1),
  Source=Source,
  IndexTicker=IndexTicker
)][abs(Exposure)>0]

duke_aggregated_look_through_exposure<-duke_look_through_exposure[,.(
  Exposure=sum(Exposure)
),keyby=c("Ticker","Source")]

duke_look_vs_outright<-duke_aggregated_look_through_exposure[,.(
  Outright=sum(Exposure[Source=="Outright"]),
  LookThrough=sum(Exposure[Source=="LookThrough"])
),keyby=Ticker][,
  "Sector":=local({
    res<-Rblpapi::bdp(Ticker,"ICB_SECTOR_NAME")
    res[Ticker,"ICB_SECTOR_NAME"]
  })
]

#
# manager look-through
#

duke_manager_exposure<-fread(
  "N:/Depts/Share/UK Alpha Team/Analytics/duke_summary/duke_manager_exposure.csv"
) %>% {
  names(.)[1]<-"Ticker"
  .
} %>% data.table::melt(
  id.vars="Ticker",
  measure.vars=tail(colnames(.),-1),
  variable.name="Manager",
  value.name="Exposure"
) %>% {
  .[abs(Exposure)>0]
} %>% {
  .[,.(Manager,Ticker,Exposure)]
}

duke_manager_index_exposure<-duke_manager_exposure[ticker_class(Ticker)=="index|nomatch"]
duke_manager_equity_exposure<-duke_manager_exposure[ticker_class(Ticker)=="equity|nomatch"]

duke_manager_index_look_through<-do.call(rbind,mapply(function(ndx,manager,exposure){
  res<-fetch_index_weights(ndx)[,"Exposure":=exposure*Weight/sum(Weight)] 
  bbg_ticker<-Rblpapi::bdp(paste0("/buid/",res$UniqueId),"PARSEKYABLE_DES")
  res$Ticker<-bbg_ticker[paste0("/buid/",res$UniqueId),"PARSEKYABLE_DES"]
  res$Manager<-manager
  res[,.(IndexTicker, Manager,Ticker,Exposure)]
},
duke_manager_index_exposure$Ticker,
duke_manager_index_exposure$Manager,
duke_manager_index_exposure$Exposure,
SIMPLIFY = FALSE
))

duke_manager_look_through_exposure<-rbind(
  duke_manager_equity_exposure[,c("Source","IndexTicker"):=list("Outright","None")],
  duke_manager_index_look_through[,"Source":="LookThrough"][,.(Manager,Ticker,Exposure,Source,IndexTicker)]
)[,.(
  Manager=Manager,
  Ticker=Ticker,
  Exposure=round(10000*Exposure,digits=1),
  Source=Source,
  IndexTicker=IndexTicker
)][abs(Exposure)>0]

duke_manager_aggregated_look_through_exposure<-duke_manager_look_through_exposure[,.(
  Exposure=sum(Exposure)
),keyby=c("Manager","Ticker","Source")]

duke_manager_look_vs_outright<-duke_manager_aggregated_look_through_exposure[,.(
  Outright=sum(Exposure[Source=="Outright"]),
  LookThrough=sum(Exposure[Source=="LookThrough"])
),keyby=c("Manager","Ticker")][,
 c(
   "Sector",
   "SuperSector",
   "Industry",
   "Return"
  ):=list(
    Sector=local({
      res<-Rblpapi::bdp(unique(Ticker),"ICB_SECTOR_NAME")
      res[Ticker,"ICB_SECTOR_NAME"]
    }),
    SuperSector=local({
      res<-Rblpapi::bdp(unique(Ticker),"ICB_SUPERSECTOR_NAME")
      res[Ticker,"ICB_SUPERSECTOR_NAME"]
    }),
    Industry=local({
      res<-Rblpapi::bdp(unique(Ticker),"ICB_INDUSTRY_NAME")
      res[Ticker,"ICB_INDUSTRY_NAME"]
    }),
    Return=local({
      res<-Rblpapi::bdp(unique(Ticker),"DAY_TO_DAY_TOT_RETURN_GROSS_DVDS")
      scrub(res[Ticker,"DAY_TO_DAY_TOT_RETURN_GROSS_DVDS"])
    })
 )
][,
  c(
    "SuperSectorIndex",
    "SuperSectorIndexReturn"
  ):=list(
    SuperSectorIndex=local({
      i<-match(SuperSector,super_sector_index_map$NAME)
      res<-ifelse(
        is.na(i),
        "",
        paste0(super_sector_index_map$INDEX[i],"P Index")  
      )
    }),
    SuperSectorIndexReturn=local({
      i<-match(SuperSector,super_sector_index_map$NAME)
      res<-ifelse(
        is.na(i),
        0,
        super_sector_index_map$P_Index[i]
      )
    })
  )
]

fwrite(
  duke_look_vs_outright,
  "N:/Depts/Share/UK Alpha Team/Analytics/duke_summary/duke_look_vs_outright.csv"
)

fwrite(
  duke_index_look_through,
  "N:/Depts/Share/UK Alpha Team/Analytics/duke_summary/duke_index_look_through.csv"
)

fwrite(
  duke_manager_look_vs_outright,
  "N:/Depts/Share/UK Alpha Team/Analytics/duke_summary/duke_manager_look_vs_outright.csv"
)

