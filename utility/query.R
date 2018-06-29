
require(DBI)

query<-function(statement,db=get("db",parent.frame())){
  q<-try(dbSendQuery(conn=db,statement),silent = TRUE)
  if(any(class(q)=="try-error")){
    cat(as.character(attributes(q)$condition))
    stop(attributes(q)$condition)
  }
  r<-try(dbFetch(q,n=-1),silent=TRUE)
  if(any(class(r)=="try-error")){
    cat(as.character(attributes(r)$condition))
    stop(attributes(r)$condition)
  }
  dbClearResult(q)
  data.table(r)
}


