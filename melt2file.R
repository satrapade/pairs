
require(data.table)

melt2file<-function(m,fn="N:/Depts/Share/UK Alpha Team/Analytics/db_cache/tret.csv"){
    file.remove(fn)
    cn<-colnames(m)
    if(ncol(m)>100){
      cns<-split(cn,seq_along(cn)%%as.integer(ncol(m)/100))
    } else { 
      cns<-list(cn)
    }
    for(i in cns){
      m_dt<-data.table::melt(
        data=data.table(date=rownames(m[,i]),data.table(m[,i])), 
        id.vars="date",measure.vars=i,
        variable.name = "stock"
      )
      fwrite(m_dt,file=fn,append=TRUE)
    }
}
  


