suppressMessages(devtools::load_all("/Users/Mig/Desktop/ClaudeCode/nautilus-claude", quiet=TRUE))
dir<-"/Users/Mig/Desktop/ClaudeCode/Whale Sharks Claude/data interim/03_checked"
ids<-c("PIN_01","PIN_02","PIN_05","PIN_08","PIN_09","PIN_12","PIN_03","PIN_06D")
cat("=== SENSITIVITY of `quiet` (fraction of quietest samples used) ===\n")
cat(sprintf("%-8s %s\n","id",paste(sprintf("%12s",paste0("q=",c(0.2,0.3,0.4,0.5,0.6))),collapse="")))
for (id in ids) {
  f<-file.path(dir,paste0(id,".rds")); if(!file.exists(f)) next
  x<-readRDS(f)
  out<-sapply(c(0.2,0.3,0.4,0.5,0.6), function(q){
    r<-suppressWarnings(suppressMessages(checkSensorIntegrity(setNames(list(x),id),
       checks="accel.calibration", control=integrityControl(accel.calibration.quiet=q),
       apply=FALSE, return.data=FALSE, verbose=FALSE)))
    i<-if(is.data.frame(r)) r else r$issues
    if(!nrow(i)) "clean" else if(i$severity[1]=="info") "declined" else
      sprintf("%s %.2f", substr(i$severity[1],1,4), i$metric[1])
  })
  cat(sprintf("%-8s %s\n", id, paste(sprintf("%12s",out),collapse="")))
}
cat("\n=== what does min.n = 5000 SAMPLES mean across the fleet? ===\n")
for (id in c("PIN_01","PIN_05","PIN_08")) {
  x<-readRDS(file.path(dir,paste0(id,".rds")))
  fs<-1/as.numeric(median(diff(as.numeric(x$datetime[1:5000]))))
  cat(sprintf("  %-8s fs = %5.1f Hz -> 5000 samples = %6.1f s of static data (record %.1f h)\n",
      id, fs, 5000/fs, as.numeric(difftime(max(x$datetime),min(x$datetime),units="hours"))))
}
