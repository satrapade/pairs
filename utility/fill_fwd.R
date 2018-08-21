


fill_fwd<-function(x,b,fill=0){
  p<-which(b)
  i<-diff(c(1,p,length(x)+1))
  j<-rep(c(1,p+1),times=i)
  y<-c(fill,x)[j]
  y
}



