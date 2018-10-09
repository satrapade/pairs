
source("https://raw.githubusercontent.com/satrapade/pairs/master/utility/volatility_trajectory.R")
source("https://raw.githubusercontent.com/satrapade/utility/master/nn_cast.R")

pair_pnl<-readLines(
  "https://raw.githubusercontent.com/satrapade/pairs/master/data/duke_pair_local_pnl.csv"
) %>% paste0(collapse="\n") %>% fread

pair_pnl_matrix<-structure(
  do.call(cbind,pair_pnl[,-1,with=FALSE]),
  dimnames=list(pair_pnl$date,names(pair_pnl)[-1])
)

pair_exposure<- readLines(
  "https://raw.githubusercontent.com/satrapade/pairs/master/data/duke_pair_exposure.csv"
) %>% 
  paste0(collapse="\n") %>% 
  fread %>% 
  {names(.)<-gsub("date","ticker",names(.),fixed=TRUE);.} %>% # mislabeling 
  {melt(
      data=.,
      id.vars = "ticker", 
      measure.vars = names(.)[-1],
      variable.name="pair",
      value.name="exposure"
  )} %>% 
  {.$manager<-gsub("[0-9]+$","",.$pair); .} %>%
  {.[,.(manager,pair,ticker,exposure)]}

total_gross<-round(sum(abs(pair_exposure$exposure))*10000,digits=1)

gross_fraction <- pair_exposure[,.(gross=sum(abs(exposure))),keyby=pair] %>% 
  {setNames(.$gross,.$pair)} %>%
  {./sum(.)}

stopifnot(all(names(gross_fraction)==colnames(pair_pnl_matrix)))

pair_tret <- pair_pnl_matrix%*%diag(10000/(total_gross*gross_fraction)) %>%
{dimnames(.)<-dimnames(pair_pnl_matrix);.}

plot(volatility_trajectory(pair_tret,total_gross*gross_fraction))

plot(volatility_trajectory_mrc(pair_tret,total_gross*gross_fraction))

pair_stats<-data.table(
  pm=gsub("[0-9]+$","",names(gross_fraction)),
  pair=names(gross_fraction),
  gross_fraction=100*gross_fraction,
  gross=total_gross*gross_fraction,
  marginal_risk_contribution=mrc(total_gross*gross_fraction,cov(pair_tret)),
  volatility=apply(pair_pnl_matrix,2,sd)*10000
)


duke_lt<-fread(
  "N:/Depts/Share/UK Alpha Team/Analytics/duke_summary/duke_manager_look_vs_outright.csv"
)

duke_lt_matrix<-NNcast(
  data=duke_lt,
  i_name="SuperSectorIndex",
  i_name="Manager",
  v_name="LookThrough+Outright",
  fun=function(x)round(sum(x),digits=1)
) %>%
{cbind(.,DUKE=rowSums(.))} %>%
{rbind(.,NET=colSums(.),GROSS=colSums(abs(.)))} %>%
round(digits=1)

log_code(sector_exposure<- duke_lt_matrix %>% 
{ data.table(sector=rownames(.),.)})

for(i in setdiff(names(sector_exposure),"sector")){
  attributes(sector_exposure[[i]])$format<-quote(sign_color(n_fmt(this),this))
}







