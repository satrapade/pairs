
require(magrittr)
require(PerformanceAnalytics)
require(RcppRoll)
require(ggplot2)
require(scales)

tret<- fread("N:/Depts/Share/UK Alpha Team/Analytics/test/hist_stock_data.csv") %>%
{.$date<-as.Date(.$date,format="%Y-%m-%d");.} %>%
{.[,.(tret=tret[1],vol_30d=vol_30d[1]),keyby=c("date","ticker")]} %>%
{.[!is.na(vol_30d)]}

countcurt<-function(x){
  maxx<-max(x)
  minx<-min(x)
  a<-mean(x)+(maxx-minx)/24
  b<-mean(x)-(maxx-minx)/24
  2*(sum(x>b)-sum(x>a))/length(x)-1
}

count_disasters<-function(x,q=0.1){
  the_range<-quantile(x,c(q,1-q))
  thresh<-min(the_range)+diff(the_range)*q
  sum(x<thresh)/length(x)
}

count_home_runs<-function(x,q=0.1){
  the_range<-quantile(x,c(q,1-q))
  thresh<-max(the_range)-diff(the_range)*q
  sum(x>thresh)/length(x)
}

fatness<-function(x,q=0.25){
  q_points<-c(q,1-q)
  diff(quantile(x,q_points))
}

fatness_ratio<-function(x,q1=0.25,q2=0.01){
  q1_points<-c(q1,1-q1)
  q1_diff<-diff(quantile(x,q1_points))
  q2_points<-c(q2,1-q2)
  q2_diff<-diff(quantile(x,q2_points))
  q2_diff/q1_diff
}

wsize<-180

v<-tret[,.(
  cross_sectional_sd=sd(tret),
  mean_move=mean(abs(tret)),
  mean_lovol_move=mean(abs(tret[vol_30d<quantile(vol_30d,0.10)])),
  mean_hivol_move=mean(abs(tret[vol_30d>quantile(vol_30d,0.90)])),
  lovol_breadth=mean(sign(tret[vol_30d<quantile(vol_30d,0.10)])),
  hivol_breadth=mean(sign(tret[vol_30d>quantile(vol_30d,0.90)])),
  count_disasters=count_disasters(tret,q=0.1),
  count_home_runs=count_home_runs(tret,q=0.1),
  dispersion_01=fatness(tret,q=0.01),
  dispersion_05=fatness(tret,q=0.05),
  dispersion_25=fatness(tret,q=0.25),
  breadth=mean(sign(tret)),
  upmove=if(sum(tret>0)>0){mean(tret[tret>0])}else{0},
  dnmove=if(sum(tret<0)>0){mean(abs(tret[tret<0]))}else{0},
  count=length(tret)
),keyby=date][cross_sectional_sd>0 & count>500][,.SD,keyby=date][,.(
  date=tail(date,-(wsize-1)),
  dispersion_01=roll_mean(dispersion_01,wsize),
  dispersion_05=roll_mean(dispersion_05,wsize),
  dispersion_25=roll_mean(dispersion_25,wsize),
  mean_lovol_move=roll_mean(mean_lovol_move,wsize),
  mean_hivol_move=roll_mean(mean_hivol_move,wsize),
  lovol_breadth=roll_mean(lovol_breadth,wsize),
  hivol_breadth=roll_mean(hivol_breadth,wsize),
  hilovol_breadth=roll_mean(hivol_breadth-lovol_breadth,wsize),
  hivol_lovol_ratio=roll_mean(mean_hivol_move/mean_lovol_move,wsize),
  cross_sectional_sd=roll_mean(cross_sectional_sd,wsize),
  count_disasters=roll_mean(count_disasters,wsize),
  count_home_runs=roll_mean(count_home_runs,wsize),
  count_skew=roll_mean(count_home_runs-count_disasters,wsize),
  breadth=roll_mean(breadth,wsize),
  upmove=roll_mean(upmove,wsize),
  dnmove=roll_mean(dnmove,wsize),
  updn_diff=roll_mean(upmove-dnmove,wsize)
)]

data1<-c("dispersion_01","dispersion_05","dispersion_25","cross_sectional_sd")
data2<-c("count_disasters","count_home_runs","count_skew")
data3<-c("count_skew")
data4<-c("mean_lovol_move","mean_hivol_move")
data5<-c("hivol_lovol_ratio")
data6<-c("upmove","dnmove")
data7<-c("updn_diff")
data8<-c("lovol_breadth","hivol_breadth")

v[date>as.Date("2010-01-01")] %>% 
  melt(
    id.vars="date",
    measure.vars="hilovol_breadth"
  ) %>%
  ggplot() + 
  geom_line(aes(x=date,y=value,col=variable)) + 
  scale_x_date(date_breaks = "1 years" , date_labels = "%y")+
  ggtitle("20-trading day rolling means")

  





