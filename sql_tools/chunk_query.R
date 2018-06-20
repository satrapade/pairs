require(DBI)

chunk_query<-function(the_query,db=get("db",parent.frame())){

 x<-capture.output(cat(the_query))
 
  y<-data.table(
    is_comment=grepl("^--",x),
    chunk=cumsum(pmax(c(0,diff(grepl("^--",x))),0)),
    text=x
  )
  
  z<-split(y$text,y$chunk)
  
  for(i in names(z)){
    the_chunk<-paste0(z[[i]],collapse="\n")
    if(the_chunk=="")next;
    q<-dbSendStatement(conn=db,the_chunk)
    dbClearResult(q)
  }
  
}
  
