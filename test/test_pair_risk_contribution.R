


source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/volatility_trajectory.R")
source("https://raw.githubusercontent.com/satrapade/utility/master/nn_cast.R")

pair_pnl<-readLines(
  "https://raw.githubusercontent.com/satrapade/pairs/master/data/duke_pair_local_pnl.csv"
) %>% paste0(collapse="\n") %>% fread

m<-structure(
  do.call(cbind,pair_pnl[,-1,with=FALSE]),
  dimnames=list(pair_pnl$date,names(pair_pnl)[-1])
)[,grepl(paste0("^",the_manager),names(pair_pnl)[-1])]

pair_exposure<- readLines(
  "https://raw.githubusercontent.com/satrapade/pairs/master/data/duke_pair_exposure.csv"
) %>% paste0(collapse="\n") %>% fread %>%
{ melt(
  data=.,
  id.vars = "date",
  measure.vars = names(.)[-1],
  variable.name="pair",
  value.name="exposure"
)} %>% 
{.$manager<-gsub("[0-9]+$","",.$pair); .}



total_gross<-round(pair_exposure[,sum(abs(exposure))]*10000,digits=1)


