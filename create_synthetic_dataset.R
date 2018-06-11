# 
#
# create synthetic dataset for testing
#
#
#
#
require(data.table)
require(stringi)


#
# synthetic data 
#
date_count<-100
all_dates<-as.character(seq(Sys.Date()-date_count+1,Sys.Date(),by=1),format="%Y-%m-%d")

# shape elements have a length in days and are concatenated
flat<-function(n,x=0)c(x,rep(0,n))
up<-function(n,x=0)c(x,sin(seq(0,(pi+1)/2,length.out=n))+tail(x,1))
down<-function(n,x=0)c(x,sin(seq((pi-1)/2,pi,length.out=n))-sin((pi-1)/2)+tail(x,1))
const<-function(n,x=0)c(x,rep(tail(x,1),n))

# shapes are: 1.named, 2.per cent of total, 3.interval lengths, 4.adding up to 1
shapes<-list( 
  c(flat=0.1,up=0.3,const=0.1,down=0.3,flat=0.1),
  c(flat=0.1,up=0.2,up=0.1,const=0.1,down=0.2,down=0.1,flat=0.1)
)

# a profile is a concatenation of shapes applied to a period of a number of days
make_profile<-function(shape){ # multiply a shape by a day count and get an exposure profile
  funcs<-mapply(get,names(shape))
  concretized_funcs<-mapply(function(f,n)with_formals(f,list(n=n)),f=funcs,n=shape)
  tail(Reduce(function(a,b)b(x=a),concretized_funcs,init=0),-1)
}

# given a sequence of elements, select random weights that add up to 1
# and create a shape
create_shape<-function(s=c("flat","up","const","down","flat")){ # a shape from elements
  setNames(diff(c(0,sort(sample(1:99,length(s)-1)/100),1)),s)
}

# select a random sequence of elements
random_shape_elements<-function(n){
  c("flat",c(rep("up",n),rep("down",n),rep("const",n))[rank(runif(3*n),t="f")],"flat")
}

# 5 pms, 100 pairs, 1000 stock universe, 80 stocks
all_pms<-c("AC","ABC","MC","GJ","DH","IB","JR")
all_pairs<-1:70
all_stocks<-1:1000

all_positions<-1:200
live_stocks<-sample(all_stocks,80)

all_fund<-data.table(
  position=all_positions,
  pm=rep(
    all_pms,
    times=round(diff(c(0,sort(sample(1:99,length(all_pms)-1))/100,1))*length(all_positions))
  ),
  pair=rep(
    all_pairs,
    times=round(diff(c(0,sort(sample(1:99,length(all_pairs)-1))/100,1))*length(all_positions))
  ),
  stock=sample(all_stocks,length(all_positions),replace=TRUE)
)

positions<-structure(
  mapply(
    function(x,n)make_profile(round(create_shape(x)*n)),
    x=mapply(random_shape_elements,sample(1:5,length(all_positions),replace=TRUE)),
    MoreArgs=list(n=date_count)
  )%*%diag(runif(length(all_positions))),
  dimnames=list(all_dates,all_fund[,paste0(pm,"_PAIR_",pair,":STOCK_",stock)])
)

ref_matrix<-matrix(
  rep(0,length(positions)),
  ncol=ncol(positions),
  nrow=nrow(positions),
  dimnames=dimnames(positions)
)

tret<-apply(matrix(
  rnorm(length(positions),mean=0.1)/100,
  ncol=ncol(positions),
  nrow=nrow(positions),
  dimnames=dimnames(positions)
),2,function(x)x*sign(rnorm(1)))

px_last<-apply(tret,2,function(x)cumprod(1+x)/(1+x[1]))

pnl<-positions*tret

all<-data.table(
  date=as.Date(rownames(positions),format="%Y-%m-%d"),
  e=apply(positions,2,function(x)rescale(x,from=c(-1,1)*max(abs(x)),c(-1,1))),
  p=apply(pnl,2,function(x)rescale(cumsum(x),from=c(-1,1)*max(abs(cumsum(x))),c(-1,1))),
  r=apply(tret,2,function(x)rescale(cumprod(1+x),c(-1,1)))
)

all_groups<-list(
  position=paste0("e.",colnames(positions)),
  pnl=paste0("p.",colnames(pnl)),
  price=paste0("r.",colnames(tret))
)

ptf<-melt(
  melt(data=all,id.vars="date",measure.vars=all_groups,variable.name = "stock"),
  id.vars=c("date","stock"),measure.vars=names(all_groups),variable.name = "stat"
)[,.(
  date=date,
  stock=colnames(positions)[as.integer(stock)],
  stat=stat,
  value=value
)]


