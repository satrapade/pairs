

require(ggplot2)
require(grid)
require(dplyr)
require(lubridate)

securities<-fread("N:/Depts/Share/UK Alpha Team/Analytics/db_cache/exposure_securities.csv")

ptf<-fread("N:/Depts/Share/UK Alpha Team/Analytics/db_cache/ptf.csv")[date>"2017-01-01"]
ptf$date<-as.Date(ptf$date,format="%Y-%m-%d")
i<-match(ptf$security,securities$exposure_security_external_id)
ptf$ticker<-gsub(" Equity","",securities$security_ticker[i])

scale2unit<-function(x)rescale(x,from=c(-1,1)*max(1,max(abs(scrub(x)))),to=c(-1,1))

df<-ptf[bucket=="DH_PAIR_42"][TRUE,.(
  date=sort(date),
  bucket=bucket,
  security_units=scale2unit(scrub(security_units))[order(date)],
  market_value=scale2unit(scrub(market_value))[order(date)],
  close=rescale(close[order(date)],to=c(-1,1)),
  tret=local({
    i<-order(date)
    scale2unit(cumsum((scrub(market_value[i])*scrub(tret[i])/100)))
  })
),keyby=ticker]

dfm <- melt(
        df,
        id.vars=c("date","bucket","ticker"),
        measure.vars=list(
          what=c("market_value","close","tret"),
          pos=rep("security_units",3)
        ),
        variable.name="select"
)

g1 <-  dfm %>% ggplot() +
      ylim(-1.5, 1.5) +
      geom_hline(aes(yintercept=0),size=0.25,color=rgb(0,0,0,0.5),show.legend=FALSE)+
      geom_point(
        mapping=aes(
          x=date,
          y=what,
          group=interaction(ticker,select),
          color=pos, #as.character(sign(position)),
          size=1
        ),
        show.legend=FALSE
      ) +  
      scale_colour_gradient2(
        low = rgb(1,0,0,0.5), 
        mid = rgb(1,1,1,0.5), 
        high = rgb(0,1,0,0.5)
      )+
      geom_line(
        mapping=aes(
          x=date,
          y=what,
          group=interaction(ticker,select)
        ),
        size=1,
        color="black",
        show.legend=FALSE
      )+
      scale_x_date(labels = date_format("%Y-%m-%d")) +
      theme(
        axis.text.x=element_text(angle=50,size=5,vjust=0.5),
        axis.ticks.y = element_blank(),    
        axis.text.y = element_blank(),
        axis.title.x = element_blank(),
        axis.title.y = element_blank(),
        strip.text.y = element_text(size = 6,angle=90)
      )

g2 <-   g1+facet_grid(
          cols=vars(select),
          rows=vars(ticker),
          labeller=labeller(select=c("1"="market_value","2"="close","3"="tret"))
        )
      






