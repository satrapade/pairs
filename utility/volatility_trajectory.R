
# volatility of a sequence of portfolios, adding assets starting with the most volatile
# and continuing until the least volatile asset has been added
# constant total exposure, equal to sum of exposures in input
# weighted by ratios in input ptf
# the resulting curve is a visualization of a portfolio's diversification
#
require(FRAPO)

tri<-function(n,d=1,s=1)(s*row(diag(n))<s*col(diag(n)))+d*diag(n)

# order constituantes by volatility
volatility_trajectory<-function(
  returns,
  ptf
){
  vol=ptf*apply(returns[,names(ptf)],2,sd)
  vol_rank=rank(vol,ties.method="first")
  weight_matrix <- cbind(ptf=ptf)[,rep(1,length(ptf))]
  mask_matrix <- diag(length(vol))[vol_rank,] %*% tri(length(vol))
  trajectory_matrix <- weight_matrix * mask_matrix
  normalized_trajectory_matrix <- trajectory_matrix %*% diag(sum(ptf)/colSums(trajectory_matrix))
  apply(returns[,names(ptf)]%*%normalized_trajectory_matrix,2,sd)
}

# order constituents by marginal risk contribution 
volatility_trajectory_mrc<-function(
  returns,
  ptf
){
  vol<-setNames(mrc(ptf,cov(returns[,names(ptf)])),names(ptf))
  vol_rank=rank(vol,ties.method="first")
  weight_matrix <- cbind(ptf=ptf)[,rep(1,length(ptf))]
  mask_matrix <- diag(length(vol))[vol_rank,] %*% tri(length(vol))
  trajectory_matrix <- weight_matrix * mask_matrix
  normalized_trajectory_matrix <- trajectory_matrix %*% diag(sum(ptf)/colSums(trajectory_matrix))
  apply(returns[,names(ptf)]%*%normalized_trajectory_matrix,2,sd)
}

