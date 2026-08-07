suppressMessages(devtools::load_all("/Users/Mig/Desktop/ClaudeCode/nautilus-claude", quiet=TRUE))
static_of <- function(A, fs, keep=0.4) {
  k <- max(3L, 2L*round(fs)+1L)
  S <- apply(A,2,function(v) as.numeric(stats::filter(v, rep(1/k,k), sides=2)))
  ok <- stats::complete.cases(S)
  d <- sqrt(rowSums((A[ok,,drop=FALSE]-S[ok,,drop=FALSE])^2))
  St <- S[ok,,drop=FALSE][d < stats::quantile(d,keep,na.rm=TRUE),,drop=FALSE]
  St[stats::complete.cases(St),,drop=FALSE]
}
diag_of <- function(A) {
  n2 <- rowSums(A^2); u <- A/sqrt(n2)
  X <- cbind(u[,1]^2,u[,2]^2,u[,3]^2,u[,1],u[,2],u[,3])
  sv <- svd(X)$d; kap <- max(sv)/min(sv)                       # conditioning of the design
  b <- stats::lm.fit(X,n2)$coefficients
  if (any(!is.finite(b))||any(b[1:3]<=0)) return(NULL)
  g <- sqrt(b[1:3]); cc <- b[4:6]/(2*g)
  r <- n2 - X%*%b
  # implied tilt error: worst-case angular effect of the offset, ~atan(|c_perp|) near level
  tilt <- atan(sqrt(sum(cc[1:2]^2)))*180/pi
  list(kappa=kap, gain=g, offset=cc, tilt=tilt, resid_sd=sd(r), medg=median(sqrt(n2)))
}
dir<-"/Users/Mig/Desktop/ClaudeCode/Whale Sharks Claude/data interim/03_checked"
cat(sprintf("%-9s %7s %8s %8s %7s %7s  %s\n","id","med|A|","kappa","resid_sd","offs_n","tilt_d","gains"))
for (f in sort(list.files(dir,"\\.rds$",full.names=TRUE))[1:11]) {
  x<-readRDS(f); if(!all(c("ax","ay","az")%in%names(x))) next
  step<-max(1,floor(nrow(x)/200000)); fs<-1/as.numeric(median(diff(as.numeric(x$datetime[1:2000]))))/step
  A<-as.matrix(x[seq(1,nrow(x),by=step),c("ax","ay","az")]); A<-A[complete.cases(A),,drop=FALSE]
  St<-static_of(A,fs); if(nrow(St)<5000) next
  d<-diag_of(St); if(is.null(d)) next
  cat(sprintf("%-9s %7.3f %8.1f %8.4f %7.3f %7.2f  %.3f %.3f %.3f\n",
     sub("\\.rds$","",basename(f)), d$medg, d$kappa, d$resid_sd,
     sqrt(sum(d$offset^2)), d$tilt, d$gain[1],d$gain[2],d$gain[3]))
}
