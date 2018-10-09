
require(magrittr)
require(RcppRoll)
require(data.table)

x<-as.matrix(fread("market_data/factor_local_tret.csv")[,-1])
y<-as.matrix(fread("duke_summary/duke_manager_local_pnl.csv")%>%{.[
  ,
  !grepl("(date)|(DXY Index)|(GSTHHVIP Index)|(SX[A-Z0-9]{2} Index)",names(.)),
  with=FALSE
]})

cm<-cor(apply(x,2,roll_sum,7),apply(y,2,roll_sum,7))

