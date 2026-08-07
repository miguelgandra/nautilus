# Does accel.scale see the errors that corrupt tilt?  Simulate a realistic posture distribution.
set.seed(1)
n <- 200000
# whale-shark-like: mostly near level, occasional steep dives
pitch <- rnorm(n, 0, 12*pi/180); pitch[runif(n) < .08] <- rnorm(sum(runif(n) < .08), 0, 45*pi/180)
roll  <- rnorm(n, 0, 10*pi/180)
A <- cbind(-sin(pitch), cos(pitch)*sin(roll), cos(pitch)*cos(roll))   # true static gravity, g
tilt <- function(M) list(p = atan2(-M[,1], sqrt(M[,2]^2 + M[,3]^2)) * 180/pi,
                         r = atan2(M[,2], M[,3]) * 180/pi)
t0 <- tilt(A)
row <- function(lab, gain, off) {
  M <- sweep(sweep(A, 2, gain, "*"), 2, off, "+")
  t1 <- tilt(M)
  g  <- sqrt(rowSums(M^2))
  data.frame(error = lab,
             accel.scale_metric = round(abs(median(g) - 1), 4),   # exactly what .icheckAccelScale grades
             pitch_rmse_deg = round(sqrt(mean((t1$p - t0$p)^2)), 2),
             roll_rmse_deg  = round(sqrt(mean((t1$r - t0$r)^2)), 2))
}
res <- rbind(
  row("x offset +0.02 g",       c(1,1,1),      c(0.02,0,0)),
  row("y offset +0.02 g",       c(1,1,1),      c(0,0.02,0)),
  row("z offset +0.02 g",       c(1,1,1),      c(0,0,0.02)),
  row("x offset +0.05 g",       c(1,1,1),      c(0.05,0,0)),
  row("x gain +3%",             c(1.03,1,1),   c(0,0,0)),
  row("x gain +10%",            c(1.10,1,1),   c(0,0,0)),
  row("y gain +10%",            c(1,1.10,1),   c(0,0,0)),
  row("all gains +10% (unit err)", c(1.1,1.1,1.1), c(0,0,0)),
  row("x off .03 + y gain 1.06", c(1,1.06,1),  c(0.03,0,0)))
print(res, row.names = FALSE)
cat("\nintegrityControl defaults: accel.scale.warning =",
    nautilus:::integrityControl()$accel.scale.warning, " error =",
    nautilus:::integrityControl()$accel.scale.error, "\n")
