require(stringi)
col2pair<-function(cn,sep=":"){
  if(class(cn)=="matrix")cn<-colnames(cn)
  if(is.null(cn))return(NULL)
  stri_sub(cn,1,stri_locate_first(cn,fixed=sep)[,1]-1)
}


