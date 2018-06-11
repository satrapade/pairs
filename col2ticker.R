require(stringi)

col2ticker<-function(cn,sep=":"){
  if(class(cn)=="matrix")cn<-colnames(cn)
  if(is.null(cn))return(NULL)
  stri_sub(cn,stri_locate_first(cn,fixed=sep)[,1]+1,-1)
}



