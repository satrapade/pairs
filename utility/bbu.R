#
# bloomberg upload
#

bbu<-function(fn){
  cmdline<-paste0("C:/blp/Wintrv/openfl -P @profile.bbu ",gsub("/","\\\\",fn))
  res<-system(cmdline,show.output.on.console = FALSE,intern=TRUE)
  res
}



