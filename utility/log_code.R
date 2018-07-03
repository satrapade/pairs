
log_code<-function(code){
  if(!exists("config",parent.frame())){
    append2log(paste0("!!!>ERROR<!!! log_code called without config"))
    stop("no config")
  }
  config<-get("config",parent.frame())
  the_code<-substitute(code)
  if(class(the_code)!="<-"){
    append2log(paste0("!!!>ERROR<!!! log_code called with invalid expression:",class(the_code)))
    stop(paste0("invalid expression:",class(the_code)))          
  }  
  code_action<-as.character(the_code[[1]])
  if(code_action!="<-"){
    append2log(paste0("!!!>ERROR<!!! log_code called non-assigment expression"))
    stop(paste0("expression is not assigment:",code_action))          
  }
  code_lhs<-as.character(the_code[[2]])
  res<-try(eval(the_code,parent.frame()),silent = TRUE)
  if(class(res)=="try-error"){
    append2log(paste0("log_code !!!>ERROR<!!! :",code_lhs))
    stop(paste0("error:",code_lhs))
  }
  return(res)
}

