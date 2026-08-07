suppressMessages(devtools::load_all("/Users/Mig/Desktop/ClaudeCode/nautilus-claude", quiet=TRUE))
sim <- function(n=60000, off=c(0,0,0), gain=c(1,1,1), sd_deg=20, noise=0.004, fs=10, seed=11) {
  set.seed(seed)
  sm <- function(k){ w<-max(3L,round(fs*20)); z<-as.numeric(stats::filter(rnorm(n+2*w),rep(1/w,w),sides=2))
                     z<-z[!is.na(z)][seq_len(n)]; z/sd(z)*k*pi/180 }
  p<-sm(sd_deg); r<-sm(sd_deg)
  A<-cbind(-sin(p),cos(p)*sin(r),cos(p)*cos(r))
  A<-sweep(sweep(A,2,gain,"*"),2,off,"+")+matrix(rnorm(3*n,0,noise),ncol=3)
  d<-data.table::data.table(ID="A01", datetime=as.POSIXct("2020-01-01",tz="UTC")+seq_len(n)/fs,
                            ax=A[,1],ay=A[,2],az=A[,3],depth=0)
  data.table::setattr(d,"nautilus.version","test"); d
}
probe <- function(d, fs=10) {
  win<-max(3L,2L*round(fs)+1L)
  s<-nautilus:::.staticDynamicAccel(d$ax,d$ay,d$az,win)
  S<-cbind(s$static$x,s$static$y,s$static$z); dyn<-sqrt(s$dynamic$x^2+s$dynamic$y^2+s$dynamic$z^2)
  k<-complete.cases(S)&is.finite(dyn); S<-S[k,,drop=FALSE]; dyn<-dyn[k]
  S<-S[dyn<=quantile(dyn,0.4,na.rm=TRUE),,drop=FALSE]
  n2<-rowSums(S^2); u<-S/sqrt(n2); X<-cbind(u[,1]^2,u[,2]^2,u[,3]^2,u[,1],u[,2],u[,3])
  b<-stats::lm.fit(X,n2)$coefficients; sv<-svd(X)$d
  list(gain=sqrt(b[1:3]), off=b[4:6]/(2*sqrt(b[1:3])),
       resid=sd(n2-as.numeric(X%*%b)), kappa=max(sv)/min(sv))
}
cat("PERFECT sensor (offset 0, gain 1) - does narrow posture create a FALSE finding?\n")
cat(sprintf("%8s %26s %8s %10s  %s\n","post_sd","gain (true 1,1,1)","resid","kappa","check says"))
for (sd_deg in c(20,10,5,3,2,1)) {
  d<-sim(sd_deg=sd_deg); pr<-probe(d)
  r<-suppressWarnings(suppressMessages(checkSensorIntegrity(list(A01=d),checks="accel.calibration",
        apply=FALSE,return.data=FALSE,verbose=FALSE)))
  iss<-if(is.data.frame(r)) r else r$issues
  says<-if(!nrow(iss)) "clean" else paste0(iss$severity[1]," ",sprintf("%.2f",iss$metric[1]),"deg")
  cat(sprintf("%8.1f %26s %8.4f %10.0f  %s\n", sd_deg,
      sprintf("%.3f %.3f %.3f",pr$gain[1],pr$gain[2],pr$gain[3]), pr$resid, pr$kappa, says))
}
