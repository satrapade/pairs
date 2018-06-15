require(scales)
scale2unit<-function(x)rescale(x,from=c(-1,1)*max(1,max(abs(scrub(x)))),to=c(-1,1))

